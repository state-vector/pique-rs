//! Integration test: build a segment, write to local filesystem via StorageBackend,
//! read it back using range reads, and verify all lookups.
//!
//! This validates the full write → store → range-read → lookup flow without
//! needing S3 or MinIO.

use pique::format::{FOOTER_SIZE, Footer};
use pique::values::adjacency::{AdjacencyList, Edge, RelKind};
use pique::values::entity_location::EntityLocation;
use pique::{LocalBackend, SegmentReader, SegmentWriter, SegmentWriterOptions, StorageBackend};
use tempfile::TempDir;

/// End-to-end: build segment → write via backend → read via range reads → lookup.
#[tokio::test]
async fn write_and_read_via_local_backend() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalBackend::new(tmp.path());
    let segment_path = "indexes/entities-gen001.idx";

    // --- Build a segment with realistic entity lookup data ---
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 4096, // Small blocks to exercise multi-block reads
        restart_interval: 8,
        enable_bloom: true,
    });

    let mut entries: Vec<(String, EntityLocation)> = (0..500)
        .map(|i| {
            let key = format!(
                "acme/myproject::src/query/src/mod_{:03}.rs::function::fn_{:04}",
                i / 10,
                i
            );
            let loc = EntityLocation {
                file_key: format!("data/org1/entities/part-{:05}.parquet", i / 100),
                row_group: (i / 50) as u32,
                row_offset: (i % 50) as u32,
            };
            (key, loc)
        })
        .collect();

    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (key, loc) in &entries {
        writer.add(key.as_bytes(), &loc.encode()).unwrap();
    }

    let output = writer.finish().unwrap();

    // --- Write to local backend ---
    backend
        .put(segment_path, output.data.clone())
        .await
        .unwrap();

    // --- Read back via range reads (simulating S3 access pattern) ---

    // Step 1: Read footer (last 64 bytes)
    let footer_bytes = backend
        .read_tail(segment_path, FOOTER_SIZE as u64)
        .await
        .unwrap();
    let footer = Footer::from_bytes(footer_bytes.as_slice().try_into().unwrap()).unwrap();
    assert_eq!(footer.key_count, 500);

    // Step 2: Read FST + bloom (would be cached in Lambda memory)
    let directory_bytes = backend
        .read_range(
            segment_path,
            footer.bloom_offset..footer.fst_offset + footer.fst_length,
        )
        .await
        .unwrap();
    assert!(!directory_bytes.is_empty());

    // Step 3: Full in-memory read for verification
    let full_data = backend.read_all(segment_path).await.unwrap();
    let reader = SegmentReader::open(full_data).unwrap();

    // --- Verify lookups ---
    for (key, expected_loc) in &entries {
        let result = reader.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key not found: {}", key);
        let decoded_loc = EntityLocation::decode(&result.unwrap()).unwrap();
        assert_eq!(
            &decoded_loc, expected_loc,
            "Wrong location for key: {}",
            key
        );
    }

    // Verify non-existent keys
    assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    assert_eq!(reader.get(b"zzz_after_all").unwrap(), None);
}

/// End-to-end with adjacency list values (larger values, variable size).
#[tokio::test]
async fn adjacency_index_via_local_backend() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalBackend::new(tmp.path());
    let segment_path = "indexes/adjacency-gen001.idx";

    // Build adjacency data
    let mut entries: Vec<(String, AdjacencyList)> = (0..200)
        .map(|i| {
            let key = format!("repo::src/module_{:03}.rs::function::fn_{:04}", i / 10, i);
            let outgoing: Vec<Edge> = (0..((i % 20) + 1))
                .map(|j| Edge {
                    entity_id: format!("repo::src/dep_{:03}.rs::function::helper_{:04}", j, j),
                    rel_kind: if j % 3 == 0 {
                        RelKind::Calls
                    } else {
                        RelKind::Imports
                    },
                })
                .collect();
            let incoming: Vec<Edge> = (0..((i % 5) + 1))
                .map(|j| Edge {
                    entity_id: format!("repo::src/caller_{:03}.rs::function::caller_{:04}", j, j),
                    rel_kind: RelKind::Calls,
                })
                .collect();
            (key, AdjacencyList { outgoing, incoming })
        })
        .collect();

    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 64 * 1024,
        restart_interval: 16,
        enable_bloom: true,
    });

    for (key, adj) in &entries {
        writer.add(key.as_bytes(), &adj.encode()).unwrap();
    }

    let output = writer.finish().unwrap();
    backend.put(segment_path, output.data).await.unwrap();

    // Read back and verify
    let full_data = backend.read_all(segment_path).await.unwrap();
    let reader = SegmentReader::open(full_data).unwrap();

    // Spot-check several entries
    for (key, expected_adj) in entries.iter().step_by(10) {
        let result = reader.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key not found: {}", key);
        let decoded_adj = AdjacencyList::decode(&result.unwrap()).unwrap();
        assert_eq!(
            decoded_adj.outgoing.len(),
            expected_adj.outgoing.len(),
            "Wrong outgoing count for {}",
            key
        );
        assert_eq!(
            decoded_adj.incoming.len(),
            expected_adj.incoming.len(),
            "Wrong incoming count for {}",
            key
        );
        assert_eq!(&decoded_adj, expected_adj);
    }
}

/// Verify object_size and delete operations.
#[tokio::test]
async fn backend_metadata_and_delete() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalBackend::new(tmp.path());

    let data = vec![1, 2, 3, 4, 5];
    backend.put("test/obj.bin", data.clone()).await.unwrap();

    let size = backend.object_size("test/obj.bin").await.unwrap();
    assert_eq!(size, 5);

    let read_back = backend.read_all("test/obj.bin").await.unwrap();
    assert_eq!(read_back, data);

    backend.delete("test/obj.bin").await.unwrap();
    let result = backend.read_all("test/obj.bin").await;
    assert!(result.is_err());
}

/// Simulate the S3 range-read pattern: only read footer + directory once,
/// then individual blocks per lookup.
#[tokio::test]
async fn simulated_range_read_pattern() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalBackend::new(tmp.path());
    let segment_path = "indexes/test-range.idx";

    // Build a segment
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size: 1024, // 1KB blocks — many blocks to exercise range reads
        restart_interval: 4,
        enable_bloom: true,
    });

    for i in 0..1000u32 {
        let key = format!("entity_{:06}", i);
        let val = format!("location_{:06}", i);
        writer.add(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let output = writer.finish().unwrap();
    backend.put(segment_path, output.data).await.unwrap();

    // Simulate S3 access pattern:
    // Request 1: Read footer (last 64 bytes)
    let size = backend.object_size(segment_path).await.unwrap();
    let footer_data = backend
        .read_range(segment_path, size - 64..size)
        .await
        .unwrap();
    let footer = Footer::from_bytes(footer_data.as_slice().try_into().unwrap()).unwrap();

    // Request 2: Read FST + bloom into memory (cached for all subsequent lookups)
    let meta_start = footer.bloom_offset;
    let meta_end = footer.fst_offset + footer.fst_length;
    let _meta_bytes = backend
        .read_range(segment_path, meta_start..meta_end)
        .await
        .unwrap();

    // Request 3+: Individual block reads per lookup
    // (In this test we just verify the pattern works — actual block-level reads
    // require the reader to be refactored for lazy loading, which is a production concern)

    // For now, verify via full read
    let full_data = backend.read_all(segment_path).await.unwrap();
    let reader = SegmentReader::open(full_data).unwrap();

    assert_eq!(
        reader.get(b"entity_000000").unwrap(),
        Some(b"location_000000".to_vec())
    );
    assert_eq!(
        reader.get(b"entity_000500").unwrap(),
        Some(b"location_000500".to_vec())
    );
    assert_eq!(
        reader.get(b"entity_000999").unwrap(),
        Some(b"location_000999".to_vec())
    );
    assert_eq!(reader.get(b"entity_001000").unwrap(), None);
}
