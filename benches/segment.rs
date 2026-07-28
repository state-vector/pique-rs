//! Benchmark harness for segment operations.
//!
//! Measures:
//! - Segment build time for various key counts
//! - Point lookup latency (in-memory)
//! - Bloom filter rejection rate
//! - Segment sizes at different block sizes
//!
//! Run with: cargo bench

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pique::{SegmentReader, SegmentWriter, SegmentWriterOptions};

/// Generate synthetic entity IDs that match our real workload pattern.
fn generate_entity_keys(count: usize) -> Vec<String> {
    let modules = [
        "src/query/src/translation.rs",
        "src/cortex/src/evidence_assembly.rs",
        "src/extractor/src/main.rs",
        "packages/contracts/src/flow_registry.rs",
        "packages/storage/src/s3vectors.rs",
        "packages/lambda-chat/src/main.rs",
        "packages/tools/src/system_context.rs",
        "src/writer/src/main.rs",
    ];

    let mut keys: Vec<String> = (0..count)
        .map(|i| {
            let module = modules[i % modules.len()];
            format!("acme/myproject::{}::function::fn_{:06}", module, i)
        })
        .collect();

    keys.sort();
    keys
}

/// Generate synthetic adjacency list values (variable size).
fn generate_adjacency_value(edge_count: usize) -> Vec<u8> {
    // Simulate a compressed adjacency list: each edge is ~60 bytes (entity_id + rel_kind)
    let mut val = Vec::with_capacity(edge_count * 60);
    for i in 0..edge_count {
        let edge = format!("edge_to_entity_{:06}:calls,", i);
        val.extend_from_slice(edge.as_bytes());
    }
    val
}

fn bench_build_segment(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_segment");

    for &count in &[1_000, 10_000, 100_000] {
        let keys = generate_entity_keys(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("entity_lookup", count),
            &keys,
            |b, keys| {
                b.iter(|| {
                    let mut writer = SegmentWriter::new(SegmentWriterOptions {
                        block_size: 64 * 1024,
                        restart_interval: 16,
                        enable_bloom: true,
                    });

                    for (i, key) in keys.iter().enumerate() {
                        let val = format!(
                            "{{\"file\":\"part-{:04}.parquet\",\"rg\":{}}}",
                            i / 1000,
                            i % 100
                        );
                        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
                    }

                    writer.finish().unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");

    for &count in &[1_000, 10_000, 100_000] {
        let keys = generate_entity_keys(count);

        // Build the segment
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 64 * 1024,
            restart_interval: 16,
            enable_bloom: true,
        });
        for (i, key) in keys.iter().enumerate() {
            let val = format!(
                "{{\"file\":\"part-{:04}.parquet\",\"rg\":{}}}",
                i / 1000,
                i % 100
            );
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        // Benchmark lookups at various positions
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("existing_key", count),
            &(keys.clone(), reader),
            |b, (keys, reader)| {
                let mut i = 0;
                b.iter(|| {
                    let key = &keys[i % keys.len()];
                    let result = reader.get(key.as_bytes()).unwrap();
                    assert!(result.is_some());
                    i += 1;
                });
            },
        );
    }

    group.finish();
}

fn bench_bloom_rejection(c: &mut Criterion) {
    let keys = generate_entity_keys(100_000);

    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 64 * 1024,
        restart_interval: 16,
        enable_bloom: true,
    });
    for (i, key) in keys.iter().enumerate() {
        let val = format!("{}", i);
        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    let output = writer.finish().unwrap();
    let reader = SegmentReader::open(output.data).unwrap();

    c.bench_function("bloom_reject_missing_key", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("nonexistent_entity_{:08}", i);
            let result = reader.might_contain(key.as_bytes());
            // Most should be false (rejected by bloom)
            let _ = result;
            i += 1;
        });
    });
}

fn bench_block_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_size_impact");
    let keys = generate_entity_keys(50_000);

    for &block_size in &[4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024] {
        // Build segment with this block size
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size,
            restart_interval: 16,
            enable_bloom: true,
        });
        for (i, key) in keys.iter().enumerate() {
            let val = format!(
                "{{\"file\":\"part-{:04}.parquet\",\"rg\":{}}}",
                i / 1000,
                i % 100
            );
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let output = writer.finish().unwrap();
        let segment_size = output.data.len();
        let reader = SegmentReader::open(output.data).unwrap();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new(
                format!(
                    "lookup_{}KB_seg{}KB",
                    block_size / 1024,
                    segment_size / 1024
                ),
                block_size,
            ),
            &(keys.clone(), reader),
            |b, (keys, reader)| {
                let mut i = 0;
                b.iter(|| {
                    let key = &keys[i % keys.len()];
                    reader.get(key.as_bytes()).unwrap();
                    i += 1;
                });
            },
        );
    }

    group.finish();
}

fn bench_adjacency_segment(c: &mut Criterion) {
    let mut group = c.benchmark_group("adjacency");

    // Simulate edge adjacency index: entity_id → compressed adjacency list
    for &edges_per_entity in &[10, 50, 200] {
        let count = 10_000;
        let keys = generate_entity_keys(count);

        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 64 * 1024,
            restart_interval: 16,
            enable_bloom: true,
        });
        for key in &keys {
            let val = generate_adjacency_value(edges_per_entity);
            writer.add(key.as_bytes(), &val).unwrap();
        }
        let output = writer.finish().unwrap();
        let segment_size = output.data.len();

        eprintln!(
            "Adjacency segment: {} entities × {} edges = {} KB",
            count,
            edges_per_entity,
            segment_size / 1024
        );

        let reader = SegmentReader::open(output.data).unwrap();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("lookup", edges_per_entity),
            &(keys.clone(), reader),
            |b, (keys, reader)| {
                let mut i = 0;
                b.iter(|| {
                    let key = &keys[i % keys.len()];
                    let result = reader.get(key.as_bytes()).unwrap();
                    assert!(result.is_some());
                    i += 1;
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_build_segment,
    bench_point_lookup,
    bench_bloom_rejection,
    bench_block_sizes,
    bench_adjacency_segment,
);
criterion_main!(benches, layered_benches);

// ===========================================================================
// Layered segment benchmarks
// ===========================================================================

use pique::{LayeredReader, merge_segments, tombstone_value};

/// Build a base segment (100K keys) + N delta segments (50 keys each).
/// Returns (base_reader, delta_readers, base_keys, delta_keys).
fn build_layered_test_data(
    base_count: usize,
    delta_count: usize,
    keys_per_delta: usize,
) -> (
    SegmentReader,
    Vec<SegmentReader>,
    Vec<String>,
    Vec<Vec<String>>,
) {
    let base_keys = generate_entity_keys(base_count);

    // Build base segment
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 64 * 1024,
        restart_interval: 16,
        enable_bloom: true,
    });
    for (i, key) in base_keys.iter().enumerate() {
        let val = format!("base_value_{:06}", i);
        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    let base_reader = SegmentReader::open(writer.finish().unwrap().data).unwrap();

    // Build delta segments — each updates some keys from the base + adds new ones
    let mut delta_readers = Vec::new();
    let mut delta_keys_all = Vec::new();

    for d in 0..delta_count {
        let mut delta_keys = Vec::new();
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 4096,
            restart_interval: 4,
            enable_bloom: true,
        });

        // Half the delta keys are updates to existing base keys
        let update_count = keys_per_delta / 2;
        // Pick keys from different parts of the base to spread them out
        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();

        for i in 0..update_count {
            let base_idx = (d * keys_per_delta + i * (base_count / keys_per_delta)) % base_count;
            let key = base_keys[base_idx].clone();
            let val = format!("delta_{}_updated_{:06}", d, i).into_bytes();
            entries.push((key.clone(), val));
            delta_keys.push(key);
        }

        // Other half are new keys (not in base)
        for i in update_count..keys_per_delta {
            let key = format!("acme/myproject::src/new_module/delta_{}/fn_{:06}", d, i);
            let val = format!("delta_{}_new_{:06}", d, i).into_bytes();
            entries.push((key.clone(), val));
            delta_keys.push(key);
        }

        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (key, val) in &entries {
            writer.add(key.as_bytes(), val).unwrap();
        }

        delta_readers.push(SegmentReader::open(writer.finish().unwrap().data).unwrap());
        delta_keys_all.push(delta_keys);
    }

    (base_reader, delta_readers, base_keys, delta_keys_all)
}

fn bench_layered_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("layered_lookup");

    let (base_reader, delta_readers, base_keys, delta_keys_all) =
        build_layered_test_data(100_000, 5, 50);

    // Assemble the layered reader: deltas (newest first) + base
    let mut segments: Vec<SegmentReader> = delta_readers.into_iter().rev().collect();
    // We need to rebuild the base since we moved it — just rebuild
    let (base_reader2, _, _, _) = build_layered_test_data(100_000, 0, 0);
    // Actually let's just rebuild properly:
    drop(base_reader);
    let base_keys2 = generate_entity_keys(100_000);
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 64 * 1024,
        restart_interval: 16,
        enable_bloom: true,
    });
    for (i, key) in base_keys2.iter().enumerate() {
        let val = format!("base_value_{:06}", i);
        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    let base = SegmentReader::open(writer.finish().unwrap().data).unwrap();
    segments.push(base);

    let layered = LayeredReader::new(segments);

    eprintln!(
        "Layered: {} segments ({} base + {} deltas), total logical keys ~{}",
        layered.segment_count(),
        1,
        layered.segment_count() - 1,
        100_000 + 5 * 50
    );

    // --- Benchmark: key exists only in base (bloom rejects all deltas) ---
    group.throughput(Throughput::Elements(1));
    group.bench_function("key_in_base_only", |b| {
        let mut i = 0;
        b.iter(|| {
            // Pick a key that's in the base but NOT in any delta
            // Use keys from the middle that are unlikely to be in deltas
            let key = &base_keys[50_000 + (i % 1000)];
            let result = layered.get(key.as_bytes()).unwrap();
            assert!(result.is_some());
            i += 1;
        });
    });

    // --- Benchmark: key exists in newest delta (first hit) ---
    let newest_delta_keys = &delta_keys_all[delta_keys_all.len() - 1];
    group.bench_function("key_in_newest_delta", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = &newest_delta_keys[i % newest_delta_keys.len()];
            let result = layered.get(key.as_bytes()).unwrap();
            assert!(result.is_some());
            i += 1;
        });
    });

    // --- Benchmark: key doesn't exist anywhere (all blooms reject) ---
    group.bench_function("key_nonexistent", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("zzz_definitely_not_here_{:08}", i);
            let result = layered.get(key.as_bytes()).unwrap();
            assert!(result.is_none());
            i += 1;
        });
    });

    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");

    // Build base (100K) + 5 deltas (50 keys each, some with tombstones)
    let base_keys = generate_entity_keys(100_000);
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 64 * 1024,
        restart_interval: 16,
        enable_bloom: true,
    });
    for (i, key) in base_keys.iter().enumerate() {
        let val = format!("v{:06}", i);
        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    let base_data = writer.finish().unwrap().data;

    // Build 5 deltas
    let mut delta_datas: Vec<Vec<u8>> = Vec::new();
    for d in 0..5 {
        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..50 {
            let base_idx = d * 50 + i;
            let key = base_keys[base_idx * 100 % base_keys.len()].clone();
            if i < 5 {
                // 5 tombstones per delta
                entries.push((key, tombstone_value()));
            } else {
                entries.push((key, format!("delta{}_{}", d, i).into_bytes()));
            }
        }
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries.dedup_by(|(a, _), (b, _)| a == b);

        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 4096,
            restart_interval: 4,
            enable_bloom: true,
        });
        for (key, val) in &entries {
            writer.add(key.as_bytes(), val).unwrap();
        }
        delta_datas.push(writer.finish().unwrap().data);
    }

    let base_size = base_data.len();
    let delta_total: usize = delta_datas.iter().map(|d| d.len()).sum();
    eprintln!(
        "Merge input: base={:.1}KB + {} deltas ({:.1}KB total)",
        base_size as f64 / 1024.0,
        delta_datas.len(),
        delta_total as f64 / 1024.0
    );

    group.bench_function("merge_100k_base_5_deltas", |b| {
        b.iter(|| {
            // Reconstruct readers each iteration (merge consumes via iter())
            let base_r = SegmentReader::open(base_data.clone()).unwrap();
            let delta_rs: Vec<SegmentReader> = delta_datas
                .iter()
                .map(|d| SegmentReader::open(d.clone()).unwrap())
                .collect();

            // Assemble segments newest-first for merge
            let mut segments: Vec<SegmentReader> = delta_rs.into_iter().rev().collect();
            segments.push(base_r);

            let _merged = merge_segments(&segments).unwrap();
        });
    });

    group.finish();
}

criterion_group!(layered_benches, bench_layered_lookup, bench_merge,);
