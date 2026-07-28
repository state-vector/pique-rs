//! S3/MinIO integration test.
//!
//! This test validates the full flow against a real S3-compatible endpoint.
//!
//! ## Running
//!
//! ### Against MinIO (local)
//! ```bash
//! docker run -d -p 9000:9000 -p 9001:9001 \
//!   -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin \
//!   minio/minio server /data --console-address ":9001"
//!
//! # Create the test bucket
//! aws --endpoint-url http://localhost:9000 s3 mb s3://s3-index-test
//!
//! # Run the test
//! S3_INDEX_TEST_BUCKET=s3-index-test \
//! S3_INDEX_TEST_ENDPOINT=http://localhost:9000 \
//! AWS_ACCESS_KEY_ID=minioadmin \
//! AWS_SECRET_ACCESS_KEY=minioadmin \
//! AWS_REGION=us-east-1 \
//! cargo test --test s3_integration --features s3
//! ```
//!
//! ### Against real S3
//! ```bash
//! S3_INDEX_TEST_BUCKET=your-test-bucket \
//! cargo test --test s3_integration --features s3
//! ```
//!
//! If `S3_INDEX_TEST_BUCKET` is not set, the test is skipped.

#[cfg(feature = "s3")]
mod s3_tests {
    use osi::format::{FOOTER_SIZE, Footer};
    use osi::values::entity_location::EntityLocation;
    use osi::{S3Backend, SegmentReader, SegmentWriter, SegmentWriterOptions, StorageBackend};
    use std::time::Instant;

    fn get_test_config() -> Option<(String, Option<String>)> {
        let bucket = std::env::var("S3_INDEX_TEST_BUCKET").ok()?;
        let endpoint = std::env::var("S3_INDEX_TEST_ENDPOINT").ok();
        Some((bucket, endpoint))
    }

    async fn make_backend() -> Option<S3Backend> {
        let (bucket, endpoint) = get_test_config()?;
        let backend = if let Some(ep) = endpoint {
            S3Backend::with_endpoint(bucket, &ep).await
        } else {
            S3Backend::from_env(bucket).await
        };
        Some(backend)
    }

    #[tokio::test]
    async fn s3_write_read_round_trip() {
        let backend = match make_backend().await {
            Some(b) => b,
            None => {
                eprintln!("Skipping S3 integration test: S3_INDEX_TEST_BUCKET not set");
                return;
            }
        };

        let prefix = format!("s3-index-test/{}", uuid_v4());
        let segment_path = format!("{}/entities-gen001.idx", prefix);

        // Build a segment
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 16 * 1024,
            restart_interval: 16,
            enable_bloom: true,
        });

        let count = 5000;
        let mut keys: Vec<String> = (0..count)
            .map(|i| {
                format!(
                    "acme/myproject::packages/mod_{:03}/src/file_{:03}.rs::function::fn_{:05}",
                    i / 100,
                    i / 10,
                    i
                )
            })
            .collect();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            let loc = EntityLocation {
                file_key: format!("data/org1/entities/part-{:05}.parquet", i / 500),
                row_group: (i / 100) as u32,
                row_offset: (i % 100) as u32,
            };
            writer.add(key.as_bytes(), &loc.encode()).unwrap();
        }

        let output = writer.finish().unwrap();
        let segment_size = output.data.len();

        // --- Upload ---
        let upload_start = Instant::now();
        backend.put(&segment_path, output.data).await.unwrap();
        let upload_ms = upload_start.elapsed().as_millis();
        eprintln!("Upload: {} bytes in {}ms", segment_size, upload_ms);

        // --- Verify size ---
        let size = backend.object_size(&segment_path).await.unwrap();
        assert_eq!(size, segment_size as u64);

        // --- Range read pattern ---

        // Request 1: Footer
        let footer_start = Instant::now();
        let footer_bytes = backend
            .read_tail(&segment_path, FOOTER_SIZE as u64)
            .await
            .unwrap();
        let footer_ms = footer_start.elapsed().as_millis();
        let footer = Footer::from_bytes(footer_bytes.as_slice().try_into().unwrap()).unwrap();
        assert_eq!(footer.key_count, count as u32);
        eprintln!("Footer read: {}ms", footer_ms);

        // Request 2: FST + bloom (the "directory" — cached in Lambda memory)
        let dir_start = Instant::now();
        let dir_bytes = backend
            .read_range(
                &segment_path,
                footer.bloom_offset..footer.fst_offset + footer.fst_length,
            )
            .await
            .unwrap();
        let dir_ms = dir_start.elapsed().as_millis();
        eprintln!("Directory read: {} bytes in {}ms", dir_bytes.len(), dir_ms);

        // Request 3: A single data block (simulating a point lookup)
        let block_start = Instant::now();
        // Read first 16KB of data (first block)
        let block_bytes = backend
            .read_range(&segment_path, 0..16 * 1024)
            .await
            .unwrap();
        let block_ms = block_start.elapsed().as_millis();
        eprintln!("Block read: {} bytes in {}ms", block_bytes.len(), block_ms);

        // --- Full read for correctness verification ---
        let full_start = Instant::now();
        let full_data = backend.read_all(&segment_path).await.unwrap();
        let full_ms = full_start.elapsed().as_millis();
        eprintln!("Full read: {} bytes in {}ms", full_data.len(), full_ms);

        let reader = SegmentReader::open(full_data).unwrap();

        // Spot-check lookups
        for i in (0..count).step_by(100) {
            let key = &keys[i];
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key not found: {}", key);
            let loc = EntityLocation::decode(&result.unwrap()).unwrap();
            assert_eq!(loc.row_group, (i / 100) as u32);
            assert_eq!(loc.row_offset, (i % 100) as u32);
        }

        // Non-existent key
        assert_eq!(reader.get(b"nonexistent_key").unwrap(), None);

        // --- Cleanup ---
        backend.delete(&segment_path).await.unwrap();

        // --- Summary ---
        eprintln!("\n=== S3 Integration Results ===");
        eprintln!(
            "Segment: {} keys, {} bytes ({:.1} KB)",
            count,
            segment_size,
            segment_size as f64 / 1024.0
        );
        eprintln!("Footer read:    {}ms (64 bytes)", footer_ms);
        eprintln!("Directory read: {}ms ({} bytes)", dir_ms, dir_bytes.len());
        eprintln!("Block read:     {}ms (16 KB)", block_ms);
        eprintln!(
            "Total cold lookup: ~{}ms (footer + dir + block)",
            footer_ms + dir_ms + block_ms
        );
        eprintln!("Warm lookup:       ~{}ms (block only)", block_ms);
        eprintln!("==============================");
    }

    #[tokio::test]
    async fn s3_latency_benchmark() {
        let backend = match make_backend().await {
            Some(b) => b,
            None => {
                eprintln!("Skipping S3 latency benchmark: S3_INDEX_TEST_BUCKET not set");
                return;
            }
        };

        let prefix = format!("s3-index-bench/{}", uuid_v4());
        let segment_path = format!("{}/bench.idx", prefix);

        // Build a 100K-entry segment (realistic size)
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 64 * 1024,
            restart_interval: 16,
            enable_bloom: true,
        });

        let count = 100_000;
        let mut keys: Vec<String> = (0..count)
            .map(|i| {
                format!(
                    "repo::src/mod_{:04}/file_{:04}.rs::fn_{:06}",
                    i / 1000,
                    i / 100,
                    i
                )
            })
            .collect();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            let val = format!(
                "{{\"f\":\"p-{:03}.pq\",\"rg\":{},\"ro\":{}}}",
                i / 1000,
                i / 100,
                i % 100
            );
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let output = writer.finish().unwrap();
        let segment_size = output.data.len();
        eprintln!(
            "Segment: {} keys, {:.1} MB",
            count,
            segment_size as f64 / (1024.0 * 1024.0)
        );

        backend.put(&segment_path, output.data).await.unwrap();

        // Measure individual range read latencies
        let mut footer_times = Vec::new();
        let mut block_times = Vec::new();

        for _ in 0..10 {
            let start = Instant::now();
            let _ = backend.read_tail(&segment_path, 64).await.unwrap();
            footer_times.push(start.elapsed().as_millis());
        }

        // Read various 64KB blocks at different offsets
        let size = backend.object_size(&segment_path).await.unwrap();
        let block_offsets: Vec<u64> = (0..10).map(|i| (i * size / 10).min(size - 65536)).collect();

        for &offset in &block_offsets {
            let start = Instant::now();
            let _ = backend
                .read_range(&segment_path, offset..offset + 65536)
                .await
                .unwrap();
            block_times.push(start.elapsed().as_millis());
        }

        // Cleanup
        backend.delete(&segment_path).await.unwrap();

        // Report
        footer_times.sort();
        block_times.sort();

        eprintln!("\n=== S3 Range Read Latencies ===");
        eprintln!(
            "Footer (64B): p50={}ms p99={}ms",
            footer_times[footer_times.len() / 2],
            footer_times[footer_times.len() - 1],
        );
        eprintln!(
            "Block (64KB): p50={}ms p99={}ms",
            block_times[block_times.len() / 2],
            block_times[block_times.len() - 1],
        );
        eprintln!("================================");
    }

    /// Simple UUID v4 generation without pulling in a UUID crate.
    fn uuid_v4() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        format!("{:08x}-{:04x}", nanos, std::process::id() as u16)
    }
}

/// Placeholder test that always passes (so the test file compiles without the s3 feature).
#[test]
fn s3_integration_placeholder() {
    // Real tests are behind #[cfg(feature = "s3")].
    // This test exists so `cargo test` doesn't skip the file entirely.
}
