//! Secondary index writer — accumulates multiple locations per key.
//!
//! For secondary indexes (one column value → many file locations), the caller
//! shouldn't need to pre-aggregate locations before writing. This module
//! provides `SecondaryIndexWriter` which accepts `add_location(key, loc)` calls
//! in any order and coalesces them into a sorted segment at `finish()`.
//!
//! ## Usage
//!
//! ```rust
//! use pique::secondary::SecondaryIndexWriter;
//! use pique::values::entity_location::EntityLocation;
//!
//! let mut writer = SecondaryIndexWriter::new();
//!
//! // Add locations in any order — duplicates per key are accumulated
//! writer.add_location(b"subject_001", EntityLocation {
//!     file_key: "data/part-000.parquet".into(),
//!     row_group: 0,
//!     row_offset: 42,
//! });
//! writer.add_location(b"subject_001", EntityLocation {
//!     file_key: "data/part-005.parquet".into(),
//!     row_group: 2,
//!     row_offset: 7,
//! });
//! writer.add_location(b"subject_002", EntityLocation {
//!     file_key: "data/part-001.parquet".into(),
//!     row_group: 1,
//!     row_offset: 0,
//! });
//!
//! // Finish: sorts keys, encodes LocationSet per key, builds segment
//! let output = writer.finish().unwrap();
//! // output.data is a standard Pique segment — use SegmentReader to query
//! ```
//!
//! The resulting segment's values are `LocationSet`-encoded. Decode with
//! `LocationSet::decode(value_bytes)` after a `reader.get(key)` call.

use std::collections::BTreeMap;

use crate::values::entity_location::EntityLocation;
use crate::values::location_set::LocationSet;
use crate::writer::{SegmentOutput, SegmentWriter, SegmentWriterOptions, WriterError};

/// Options for the secondary index writer.
#[derive(Debug, Clone)]
pub struct SecondaryIndexOptions {
    /// Segment writer options (block size, restart interval, bloom).
    pub segment_options: SegmentWriterOptions,
}

impl Default for SecondaryIndexOptions {
    fn default() -> Self {
        Self {
            segment_options: SegmentWriterOptions::default(),
        }
    }
}

/// Accumulating writer for secondary indexes.
///
/// Accepts `add_location(key, loc)` calls in any order. At `finish()`, sorts
/// keys lexicographically, encodes each key's locations as a `LocationSet`,
/// and builds a standard Pique segment.
pub struct SecondaryIndexWriter {
    opts: SecondaryIndexOptions,
    /// BTreeMap gives us sorted keys automatically.
    entries: BTreeMap<Vec<u8>, Vec<EntityLocation>>,
}

impl SecondaryIndexWriter {
    /// Create a new secondary index writer with default options.
    pub fn new() -> Self {
        Self {
            opts: SecondaryIndexOptions::default(),
            entries: BTreeMap::new(),
        }
    }

    /// Create with custom options.
    pub fn with_options(opts: SecondaryIndexOptions) -> Self {
        Self {
            opts,
            entries: BTreeMap::new(),
        }
    }

    /// Add a location for a key. Multiple calls with the same key accumulate.
    ///
    /// Keys can be added in any order — sorting happens at `finish()`.
    pub fn add_location(&mut self, key: &[u8], location: EntityLocation) {
        self.entries
            .entry(key.to_vec())
            .or_default()
            .push(location);
    }

    /// Number of distinct keys accumulated so far.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Total number of locations accumulated across all keys.
    pub fn total_locations(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    /// Finish building the segment. Sorts keys, encodes LocationSets, and
    /// produces a standard Pique segment.
    ///
    /// Returns an error if no keys were added.
    pub fn finish(self) -> Result<SegmentOutput, WriterError> {
        if self.entries.is_empty() {
            return Err(WriterError::EmptySegment);
        }

        let mut writer = SegmentWriter::new(self.opts.segment_options);

        // BTreeMap iteration is already sorted
        for (key, locations) in &self.entries {
            let set = LocationSet {
                locations: locations.clone(),
            };
            let encoded = set.encode();
            writer.add(key, &encoded)?;
        }

        writer.finish()
    }
}

impl Default for SecondaryIndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::SegmentReader;

    #[test]
    fn single_key_multiple_locations() {
        let mut writer = SecondaryIndexWriter::new();

        writer.add_location(
            b"subject_001",
            EntityLocation {
                file_key: "part-000.parquet".into(),
                row_group: 0,
                row_offset: 10,
            },
        );
        writer.add_location(
            b"subject_001",
            EntityLocation {
                file_key: "part-003.parquet".into(),
                row_group: 1,
                row_offset: 55,
            },
        );

        assert_eq!(writer.key_count(), 1);
        assert_eq!(writer.total_locations(), 2);

        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        let value = reader.get(b"subject_001").unwrap().unwrap();
        let set = LocationSet::decode(&value).unwrap();
        assert_eq!(set.len(), 2);
        assert_eq!(set.locations[0].file_key, "part-000.parquet");
        assert_eq!(set.locations[1].file_key, "part-003.parquet");
    }

    #[test]
    fn multiple_keys_out_of_order() {
        let mut writer = SecondaryIndexWriter::new();

        // Add keys out of order — writer handles sorting
        writer.add_location(
            b"zzz",
            EntityLocation {
                file_key: "f.parquet".into(),
                row_group: 0,
                row_offset: 0,
            },
        );
        writer.add_location(
            b"aaa",
            EntityLocation {
                file_key: "f.parquet".into(),
                row_group: 1,
                row_offset: 0,
            },
        );
        writer.add_location(
            b"mmm",
            EntityLocation {
                file_key: "f.parquet".into(),
                row_group: 2,
                row_offset: 0,
            },
        );

        assert_eq!(writer.key_count(), 3);

        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        // All keys findable
        assert!(reader.get(b"aaa").unwrap().is_some());
        assert!(reader.get(b"mmm").unwrap().is_some());
        assert!(reader.get(b"zzz").unwrap().is_some());
        assert!(reader.get(b"bbb").unwrap().is_none());
    }

    #[test]
    fn interleaved_adds_accumulate() {
        let mut writer = SecondaryIndexWriter::new();

        // Interleave adds for different keys
        writer.add_location(
            b"key_a",
            EntityLocation {
                file_key: "f1.parquet".into(),
                row_group: 0,
                row_offset: 0,
            },
        );
        writer.add_location(
            b"key_b",
            EntityLocation {
                file_key: "f1.parquet".into(),
                row_group: 0,
                row_offset: 1,
            },
        );
        writer.add_location(
            b"key_a",
            EntityLocation {
                file_key: "f2.parquet".into(),
                row_group: 0,
                row_offset: 0,
            },
        );
        writer.add_location(
            b"key_b",
            EntityLocation {
                file_key: "f2.parquet".into(),
                row_group: 1,
                row_offset: 5,
            },
        );

        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        let val_a = reader.get(b"key_a").unwrap().unwrap();
        let set_a = LocationSet::decode(&val_a).unwrap();
        assert_eq!(set_a.len(), 2);

        let val_b = reader.get(b"key_b").unwrap().unwrap();
        let set_b = LocationSet::decode(&val_b).unwrap();
        assert_eq!(set_b.len(), 2);
    }

    #[test]
    fn empty_writer_returns_error() {
        let writer = SecondaryIndexWriter::new();
        assert!(writer.finish().is_err());
    }

    #[test]
    fn bloom_rejects_missing_keys() {
        let mut writer = SecondaryIndexWriter::new();
        for i in 0..100 {
            writer.add_location(
                format!("key_{:04}", i).as_bytes(),
                EntityLocation {
                    file_key: "f.parquet".into(),
                    row_group: 0,
                    row_offset: i,
                },
            );
        }

        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        // Bloom should reject most non-existent keys
        let mut rejections = 0;
        for i in 200..300 {
            if !reader.might_contain(format!("key_{:04}", i).as_bytes()) {
                rejections += 1;
            }
        }
        assert!(rejections > 95);
    }
}
