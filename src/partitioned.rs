//! Partitioned index — routes lookups across multiple segments by key range.
//!
//! At scale (millions to billions of keys), a single segment becomes too large
//! for serverless constraints (Lambda memory, S3 multipart upload limits).
//! Partitioned indexes solve this by splitting the keyspace into ranges, each
//! backed by an independent segment.
//!
//! ## Architecture
//!
//! ```text
//! PartitionManifest (tiny — <1KB for 1000 partitions):
//!   partition[0]: keys [aaa... → bbb...]  → seg_000.osi
//!   partition[1]: keys [bbb... → ccc...]  → seg_001.osi
//!   ...
//!   partition[N]: keys [zzz... → end]     → seg_N.osi
//!
//! Lookup:
//!   key → binary search manifest → partition_idx → open segment → point lookup
//! ```
//!
//! ## Build (serverless-friendly)
//!
//! Each segment builds independently (parallelisable across Lambdas):
//! 1. Coordinator partitions sorted keys into ranges
//! 2. Each builder Lambda receives one partition's keys → builds one .osi segment
//! 3. Coordinator writes the manifest listing all segments + key ranges
//!
//! Memory per builder: O(keys_per_partition) — typically ~1M keys = ~100MB.
//! Total index size: unlimited (just add more partitions).
//!
//! ## Read (serverless-friendly)
//!
//! 1. Load manifest (cached, <1KB)
//! 2. Binary search key ranges → target segment
//! 3. Load segment metadata (tail read, cached per segment)
//! 4. Block read for the answer
//!
//! Memory per query: manifest + one segment's FST+bloom (~50KB for 1M keys).

use serde::{Deserialize, Serialize};

/// The partition manifest — routes keys to segments by range.
///
/// Stored as a single small JSON file alongside the segments.
/// Binary search on `partitions[i].max_key` finds the target segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionManifest {
    /// Version for format evolution.
    pub version: u32,
    /// Total key count across all partitions.
    pub total_keys: u64,
    /// Number of partitions (= number of segment files).
    pub partition_count: u32,
    /// Ordered list of partitions. `partitions[i].max_key` is the upper bound
    /// (inclusive) for that partition. Keys > max_key go to partition[i+1].
    pub partitions: Vec<PartitionEntry>,
}

/// A single partition within the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionEntry {
    /// Maximum key in this partition (inclusive). Used for binary search routing.
    /// The first partition has an implicit min of "". Subsequent partitions have
    /// min = previous partition's max_key + 1 byte (lexicographic).
    pub max_key: String,
    /// S3 path to this partition's segment file.
    pub segment_path: String,
    /// Number of keys in this partition.
    pub key_count: u32,
    /// Segment file size in bytes.
    pub size_bytes: u64,
}

impl PartitionManifest {
    /// Find the partition index that should contain the given key.
    /// Uses binary search on max_key values.
    ///
    /// Returns `None` if the key is beyond all partitions (shouldn't happen
    /// if the manifest covers the full keyspace — the last partition's max_key
    /// should be >= any possible key).
    pub fn find_partition(&self, key: &[u8]) -> Option<usize> {
        // Binary search: find the first partition whose max_key >= key
        let key_str = std::str::from_utf8(key).unwrap_or("");
        match self
            .partitions
            .binary_search_by(|p| p.max_key.as_str().cmp(key_str))
        {
            Ok(idx) => Some(idx), // Exact match on max_key
            Err(idx) => {
                if idx < self.partitions.len() {
                    Some(idx) // First partition with max_key > key
                } else {
                    None // Beyond all partitions
                }
            }
        }
    }

    /// Total size across all segments.
    pub fn total_size_bytes(&self) -> u64 {
        self.partitions.iter().map(|p| p.size_bytes).sum()
    }

    /// Average keys per partition.
    pub fn avg_keys_per_partition(&self) -> u64 {
        if self.partition_count == 0 {
            return 0;
        }
        self.total_keys / self.partition_count as u64
    }
}

/// Target keys per partition. Balances:
/// - Memory usage (FST + bloom must fit in Lambda)
/// - Segment size (upload speed, tail read budget)
/// - Build time (each partition should complete in <10s)
///
/// At 1M keys with 60-byte keys + 32-byte values:
/// - Segment size: ~60MB
/// - FST size: ~2MB  
/// - Bloom size: ~1.2MB
/// - Build time: ~200ms
/// - Memory during build: ~120MB
///
/// Comfortably within Lambda's 10GB / 15min limits.
pub const DEFAULT_KEYS_PER_PARTITION: usize = 1_000_000;

/// Partition a sorted key stream into ranges of approximately `keys_per_partition` keys.
///
/// Returns the list of partition boundaries (max_key for each partition).
/// The last partition captures all remaining keys.
pub fn compute_partition_boundaries(total_keys: usize, keys_per_partition: usize) -> usize {
    let count = total_keys.div_ceil(keys_per_partition);
    count.max(1)
}

// ---------------------------------------------------------------------------
// Builder — produces a PartitionManifest from a sorted key iterator
// ---------------------------------------------------------------------------

use crate::writer::{SegmentWriter, SegmentWriterOptions};

/// Result of building one partition.
pub struct BuiltPartition {
    /// The segment bytes (ready to upload to S3).
    pub data: Vec<u8>,
    /// Maximum key in this partition.
    pub max_key: String,
    /// Number of keys.
    pub key_count: u32,
}

/// Build a single partition from a sorted slice of (key, value) pairs.
///
/// This is the unit of work that can run independently in a Lambda.
/// Memory usage: O(keys.len() * avg_key_size) for the segment builder.
pub fn build_partition(
    keys_and_values: &[(&[u8], &[u8])],
    block_size: u32,
) -> Result<BuiltPartition, crate::writer::WriterError> {
    let mut writer = SegmentWriter::new(SegmentWriterOptions {
        block_size,
        restart_interval: 16,
        enable_bloom: true,
    });

    for (key, value) in keys_and_values {
        writer.add(key, value)?;
    }

    let output = writer.finish()?;
    let max_key = keys_and_values
        .last()
        .map(|(k, _)| String::from_utf8_lossy(k).to_string())
        .unwrap_or_default();
    let key_count = keys_and_values.len() as u32;

    Ok(BuiltPartition {
        data: output.data,
        max_key,
        key_count,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_partition_binary_search() {
        let manifest = PartitionManifest {
            version: 1,
            total_keys: 3000,
            partition_count: 3,
            partitions: vec![
                PartitionEntry {
                    max_key: "ddd".into(),
                    segment_path: "seg_0.osi".into(),
                    key_count: 1000,
                    size_bytes: 50_000,
                },
                PartitionEntry {
                    max_key: "mmm".into(),
                    segment_path: "seg_1.osi".into(),
                    key_count: 1000,
                    size_bytes: 50_000,
                },
                PartitionEntry {
                    max_key: "zzz".into(),
                    segment_path: "seg_2.osi".into(),
                    key_count: 1000,
                    size_bytes: 50_000,
                },
            ],
        };

        // Key in first partition
        assert_eq!(manifest.find_partition(b"aaa"), Some(0));
        assert_eq!(manifest.find_partition(b"ddd"), Some(0)); // exact max_key

        // Key in second partition
        assert_eq!(manifest.find_partition(b"eee"), Some(1));
        assert_eq!(manifest.find_partition(b"mmm"), Some(1));

        // Key in third partition
        assert_eq!(manifest.find_partition(b"nnn"), Some(2));
        assert_eq!(manifest.find_partition(b"zzz"), Some(2));

        // Key beyond all (shouldn't happen with proper manifest)
        assert_eq!(manifest.find_partition(b"zzzz"), None);
    }

    #[test]
    fn compute_boundaries() {
        assert_eq!(compute_partition_boundaries(1_000_000, 1_000_000), 1);
        assert_eq!(compute_partition_boundaries(10_000_000, 1_000_000), 10);
        assert_eq!(compute_partition_boundaries(1_000_000_000, 1_000_000), 1000);
        assert_eq!(compute_partition_boundaries(1_500_000, 1_000_000), 2); // rounds up
    }

    #[test]
    fn build_partition_produces_valid_segment() {
        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"key_001", b"val_001"),
            (b"key_002", b"val_002"),
            (b"key_003", b"val_003"),
        ];

        let result = build_partition(&pairs, 4096).unwrap();
        assert_eq!(result.key_count, 3);
        assert_eq!(result.max_key, "key_003");
        assert!(!result.data.is_empty());

        // Verify the segment is readable
        let reader = crate::reader::SegmentReader::open(result.data).unwrap();
        assert_eq!(reader.get(b"key_002").unwrap(), Some(b"val_002".to_vec()));
    }

    #[test]
    fn manifest_serialization_round_trip() {
        let manifest = PartitionManifest {
            version: 1,
            total_keys: 1_000_000_000,
            partition_count: 1000,
            partitions: (0..1000)
                .map(|i| PartitionEntry {
                    max_key: format!("key_{:010}", (i + 1) * 1_000_000),
                    segment_path: format!("segments/seg_{:04}.osi", i),
                    key_count: 1_000_000,
                    size_bytes: 55_000_000,
                })
                .collect(),
        };

        let json = serde_json::to_string(&manifest).unwrap();
        // Manifest for 1B keys across 1000 segments should be < 200KB
        assert!(
            json.len() < 200_000,
            "Manifest too large: {} bytes",
            json.len()
        );

        let decoded: PartitionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.total_keys, 1_000_000_000);
        assert_eq!(decoded.partition_count, 1000);
        assert_eq!(decoded.find_partition(b"key_0000500000"), Some(0));
    }
}
