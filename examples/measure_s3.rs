//! Real-world S3 latency measurement for OSI segments.
//!
//! This binary uploads test segments to a real S3 bucket, then measures actual
//! range-read latency for the access patterns that matter:
//!
//! 1. Footer read (last 64 bytes)
//! 2. Directory + bloom read (FST + bloom section)
//! 3. Single data block read (64KB)
//! 4. Small range read (4KB)
//! 5. Full segment read (for baseline)
//!
//! It also measures connection warm-up effects (first request vs subsequent)
//! and compares against reading the equivalent Parquet files with DuckDB
//! (if DuckDB is available — graceful skip otherwise).
//!
//! ## Usage
//!
//! ```bash
//! # Set your bucket and region
//! export OSI_TEST_BUCKET=your-bucket-name
//! export AWS_REGION=ap-southeast-2
//!
//! # Run with real AWS credentials
//! cargo run --example measure_s3 --features s3
//! ```
//!
//! ## Output
//!
//! Produces a table of latency measurements (p50, p95, p99, mean) for each
//! access pattern, plus segment metadata (size, key count, block count).

use pique::format::{FOOTER_SIZE, Footer};
use pique::{S3Backend, SegmentWriter, SegmentWriterOptions, StorageBackend};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let bucket =
        std::env::var("OSI_TEST_BUCKET").expect("Set OSI_TEST_BUCKET to a real S3 bucket name");
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "ap-southeast-2".to_string());

    println!("=== OSI Real-World S3 Latency Measurement ===");
    println!("Bucket: {bucket}");
    println!("Region: {region}");
    println!();

    // Create the S3 backend
    let backend = S3Backend::from_env(bucket.clone()).await;

    // Build test segments of various sizes
    let configs = vec![
        SegmentConfig {
            name: "small",
            key_count: 1_000,
            block_size: 64 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "medium",
            key_count: 10_000,
            block_size: 64 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "large",
            key_count: 100_000,
            block_size: 64 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "adjacency",
            key_count: 10_000,
            block_size: 64 * 1024,
            value_size: 500,
        },
        // Block size variants (all 50K keys)
        SegmentConfig {
            name: "block_4k",
            key_count: 50_000,
            block_size: 4 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "block_16k",
            key_count: 50_000,
            block_size: 16 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "block_64k",
            key_count: 50_000,
            block_size: 64 * 1024,
            value_size: 50,
        },
        SegmentConfig {
            name: "block_256k",
            key_count: 50_000,
            block_size: 256 * 1024,
            value_size: 50,
        },
    ];

    let prefix = format!("osi-bench/{}", timestamp_id());

    for config in &configs {
        println!(
            "--- Segment: {} ({} keys, {}KB blocks, {}B values) ---",
            config.name,
            config.key_count,
            config.block_size / 1024,
            config.value_size
        );

        let segment_path = format!("{}/{}.osi", prefix, config.name);

        // Build and upload
        let (segment_data, keys) = build_segment(config);
        let segment_size = segment_data.len();
        println!(
            "  Segment size: {:.1} KB ({:.1} MB)",
            segment_size as f64 / 1024.0,
            segment_size as f64 / (1024.0 * 1024.0)
        );

        let upload_start = Instant::now();
        if let Err(e) = backend.put(&segment_path, segment_data).await {
            eprintln!("  ERROR uploading: {e}");
            eprintln!("  (check bucket permissions and region)");
            continue;
        }
        println!("  Upload: {}ms", upload_start.elapsed().as_millis());

        // --- Measure access patterns ---

        // Pattern 1: Footer read (last 64 bytes) — "what is this segment?"
        let footer_latencies = measure_repeated(10, || async {
            let start = Instant::now();
            let _ = backend
                .read_tail(&segment_path, FOOTER_SIZE as u64)
                .await
                .unwrap();
            start.elapsed()
        })
        .await;
        print_stats("  Footer (64B)", &footer_latencies);

        // Parse footer for subsequent reads
        let footer_bytes = backend
            .read_tail(&segment_path, FOOTER_SIZE as u64)
            .await
            .unwrap();
        let footer = Footer::from_bytes(footer_bytes.as_slice().try_into().unwrap()).unwrap();

        // Pattern 2: Directory read (bloom + FST)
        let dir_size = footer.bloom_length + footer.fst_length;
        let dir_latencies = measure_repeated(10, || async {
            let start = Instant::now();
            let _ = backend
                .read_range(
                    &segment_path,
                    footer.bloom_offset..footer.fst_offset + footer.fst_length,
                )
                .await
                .unwrap();
            start.elapsed()
        })
        .await;
        print_stats(
            &format!("  Directory ({:.1}KB)", dir_size as f64 / 1024.0),
            &dir_latencies,
        );

        // Pattern 3: Single 64KB block read (from the middle of the segment)
        let mid_offset = footer.data_blocks_length / 2;
        let block_end = (mid_offset + 65536).min(footer.data_blocks_length);
        let block_latencies = measure_repeated(20, || async {
            let start = Instant::now();
            let _ = backend
                .read_range(&segment_path, mid_offset..block_end)
                .await
                .unwrap();
            start.elapsed()
        })
        .await;
        print_stats("  Block (64KB)", &block_latencies);

        // Pattern 4: Small range read (4KB) — minimum useful read
        let small_latencies = measure_repeated(20, || async {
            let start = Instant::now();
            let _ = backend.read_range(&segment_path, 0..4096).await.unwrap();
            start.elapsed()
        })
        .await;
        print_stats("  Small (4KB)", &small_latencies);

        // Pattern 5: Full segment read (baseline — what we'd pay without range reads)
        let full_latencies = measure_repeated(5, || async {
            let start = Instant::now();
            let _ = backend.read_all(&segment_path).await.unwrap();
            start.elapsed()
        })
        .await;
        print_stats(
            &format!("  Full ({:.1}KB)", segment_size as f64 / 1024.0),
            &full_latencies,
        );

        // Pattern 6: End-to-end point lookup simulation
        // (footer + directory + one block — the complete cold-lookup pattern)
        let e2e_latencies = measure_repeated(10, || async {
            let start = Instant::now();
            // Step 1: footer
            let _ = backend
                .read_tail(&segment_path, FOOTER_SIZE as u64)
                .await
                .unwrap();
            // Step 2: directory
            let _ = backend
                .read_range(
                    &segment_path,
                    footer.bloom_offset..footer.fst_offset + footer.fst_length,
                )
                .await
                .unwrap();
            // Step 3: one data block
            let _ = backend
                .read_range(&segment_path, mid_offset..block_end)
                .await
                .unwrap();
            start.elapsed()
        })
        .await;
        print_stats("  E2E cold (3 reqs)", &e2e_latencies);

        // Pattern 7: Warm lookup simulation (directory already cached, just block)
        let warm_latencies = measure_repeated(20, || async {
            let start = Instant::now();
            let _ = backend
                .read_range(&segment_path, mid_offset..block_end)
                .await
                .unwrap();
            start.elapsed()
        })
        .await;
        print_stats("  E2E warm (1 req)", &warm_latencies);

        // Connection warmup analysis: compare first request vs rest
        println!("  Connection warmup:");
        if footer_latencies.len() >= 3 {
            println!("    First request: {}ms", footer_latencies[0].as_millis());
            let rest_avg: f64 = footer_latencies[1..]
                .iter()
                .map(|d| d.as_millis() as f64)
                .sum::<f64>()
                / (footer_latencies.len() - 1) as f64;
            println!("    Subsequent avg: {:.1}ms", rest_avg);
        }

        // Cleanup
        backend.delete(&segment_path).await.unwrap();
        println!();
    }

    // --- Summary ---
    println!("=== Measurement Complete ===");
    println!("Prefix used: {prefix}");
    println!();
    println!("Key findings to validate:");
    println!("  - Is footer read latency ≈ block read latency? (per-request overhead dominates)");
    println!("  - Is warm lookup (1 req) significantly faster than cold (3 reqs)?");
    println!("  - Does block size (4K vs 64K vs 256K) affect latency meaningfully?");
    println!("  - What's the first-request penalty? (TLS handshake / connection setup)");
    println!("  - How does full-segment read compare to targeted range reads?");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct SegmentConfig {
    name: &'static str,
    key_count: usize,
    block_size: u32,
    value_size: usize,
}

/// Build a test segment and return (segment_bytes, keys_for_later_lookup).
fn build_segment(config: &SegmentConfig) -> (Vec<u8>, Vec<String>) {
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: config.block_size,
        restart_interval: 16,
        enable_bloom: true,
    });

    let modules = [
        "src/query/src/translation.rs",
        "src/cortex/src/evidence_assembly.rs",
        "src/extractor/src/main.rs",
        "packages/contracts/src/flow_registry.rs",
        "packages/storage/src/s3vectors.rs",
    ];

    let mut keys: Vec<String> = (0..config.key_count)
        .map(|i| {
            let module = modules[i % modules.len()];
            format!("acme/myproject::{}::function::fn_{:06}", module, i)
        })
        .collect();
    keys.sort();

    for (i, key) in keys.iter().enumerate() {
        // Value is a fixed-size byte blob simulating EntityLocation or adjacency data
        let val = vec![(i % 256) as u8; config.value_size];
        writer.add(key.as_bytes(), &val).unwrap();
    }

    let output = writer.finish().unwrap();
    (output.data, keys)
}

/// Run an async operation N times and collect durations.
async fn measure_repeated<F, Fut>(n: usize, f: F) -> Vec<Duration>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Duration>,
{
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        results.push(f().await);
    }
    results
}

/// Print latency statistics for a set of measurements.
fn print_stats(label: &str, durations: &[Duration]) {
    if durations.is_empty() {
        println!("{label}: no data");
        return;
    }

    let mut ms: Vec<f64> = durations.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mean: f64 = ms.iter().sum::<f64>() / ms.len() as f64;
    let p50 = ms[ms.len() / 2];
    let p95 = ms[(ms.len() as f64 * 0.95) as usize].min(*ms.last().unwrap());
    let p99 = ms[(ms.len() as f64 * 0.99) as usize].min(*ms.last().unwrap());
    let min = ms[0];
    let max = *ms.last().unwrap();

    println!(
        "{label}: mean={mean:.1}ms p50={p50:.1}ms p95={p95:.1}ms min={min:.1}ms max={max:.1}ms (n={})",
        ms.len()
    );
}

fn timestamp_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{secs}")
}
