//! Stats manifest — per-(file, row_group) column statistics for range-predicate pruning.
//!
//! The stats manifest stores min/max/null_count per column per row group across
//! all Parquet files in a dataset. This enables interval-overlap queries: given a
//! predicate like `WHERE ts BETWEEN a AND b`, the manifest identifies which
//! (file, row_group) pairs *might* contain matching rows — without opening any
//! Parquet footers.
//!
//! ## Design
//!
//! - **Built externally** from Parquet footer metadata (no data scan required).
//! - **Stored as a single compact file** alongside the Pique segments.
//! - **Query = interval overlap**: predicate range [lo, hi] overlaps row-group
//!   range [min, max] iff `min <= hi && max >= lo`.
//! - **Multi-column conjunction**: multiple predicates intersect their candidate sets.
//!
//! ## Relationship to the segment index
//!
//! The segment index (bloom + FST) handles equality lookups. The stats manifest
//! handles range predicates. They are complementary — a query planner checks the
//! bloom for exact matches and the stats manifest for range scans, then takes the
//! intersection of candidate files.
//!
//! ## Usage
//!
//! ```rust
//! use pique::stats::{StatsManifest, ColumnStats, RowGroupStats, RangePredicate};
//!
//! // Build manifest from Parquet footer metadata (no data scan)
//! let mut manifest = StatsManifest::new();
//! manifest.add_row_group("data/2024-01-15.parquet", 0, vec![
//!     ("timestamp".into(), ColumnStats {
//!         min: b"2024-01-15T00:00:00Z".to_vec(),
//!         max: b"2024-01-15T23:59:59Z".to_vec(),
//!         null_count: 0,
//!     }),
//! ]);
//!
//! // Query: which (file, row_group) pairs overlap [10:00, 14:00]?
//! let candidates = manifest.query_overlap(&[
//!     RangePredicate {
//!         column: "timestamp".into(),
//!         min: b"2024-01-15T10:00:00Z".to_vec(),
//!         max: b"2024-01-15T14:00:00Z".to_vec(),
//!     },
//! ]);
//! // Returns: [("data/2024-01-15.parquet", 0)]
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ===========================================================================
// Core types
// ===========================================================================

/// Per-column statistics for a single row group.
///
/// `min` and `max` are raw bytes — the consumer is responsible for interpreting
/// them according to the column's logical type. This keeps the manifest
/// type-agnostic (timestamps, integers, strings all work the same way via
/// lexicographic or custom comparison).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value in this row group for this column (raw bytes, Parquet encoding).
    pub min: Vec<u8>,
    /// Maximum value in this row group for this column (raw bytes, Parquet encoding).
    pub max: Vec<u8>,
    /// Number of null values in this row group for this column.
    pub null_count: u64,
}

/// Statistics for a single row group within a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupStats {
    /// Row group index within the file (0-based).
    pub row_group: u32,
    /// Per-column statistics. Key = column name.
    pub columns: HashMap<String, ColumnStats>,
}

/// Statistics for a single Parquet file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStats {
    /// Path to the Parquet file (relative to dataset root).
    pub path: String,
    /// Per-row-group statistics.
    pub row_groups: Vec<RowGroupStats>,
}

/// A range predicate for interval-overlap queries.
///
/// Represents `column BETWEEN min AND max` (inclusive on both ends).
/// The comparison is lexicographic on raw bytes by default. For numeric
/// or timestamp columns, callers should encode values in a byte-comparable
/// format (big-endian integers, ISO-8601 strings, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePredicate {
    /// Column name to filter on.
    pub column: String,
    /// Lower bound (inclusive) of the predicate range.
    pub min: Vec<u8>,
    /// Upper bound (inclusive) of the predicate range.
    pub max: Vec<u8>,
}

/// A reference to a specific row group in a specific file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowGroupRef {
    /// File path.
    pub file: String,
    /// Row group index within the file.
    pub row_group: u32,
}

// ===========================================================================
// StatsManifest — the top-level structure
// ===========================================================================

/// The stats manifest — per-(file, row_group) column statistics for an entire dataset.
///
/// Serialized as a single JSON file. Typical size for 1000 files × 10 row groups
/// × 5 columns ≈ 2–5MB (compresses well with gzip to ~500KB).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsManifest {
    /// Format version for forward compatibility.
    pub version: u32,
    /// Per-file statistics.
    pub files: Vec<FileStats>,
}

impl StatsManifest {
    /// Create an empty manifest.
    pub fn new() -> Self {
        Self {
            version: 1,
            files: Vec::new(),
        }
    }

    /// Add row-group statistics for a file.
    ///
    /// `columns` is a list of (column_name, stats) pairs for this row group.
    pub fn add_row_group(
        &mut self,
        file_path: &str,
        row_group: u32,
        columns: Vec<(String, ColumnStats)>,
    ) {
        let rg_stats = RowGroupStats {
            row_group,
            columns: columns.into_iter().collect(),
        };

        // Find or create the file entry
        if let Some(file) = self.files.iter_mut().find(|f| f.path == file_path) {
            file.row_groups.push(rg_stats);
        } else {
            self.files.push(FileStats {
                path: file_path.to_string(),
                row_groups: vec![rg_stats],
            });
        }
    }

    /// Query the manifest for row groups whose column statistics overlap ALL
    /// given predicates (conjunction / AND semantics).
    ///
    /// Returns the set of (file, row_group) references that *might* contain
    /// matching rows. False positives are possible (a row group's min/max range
    /// overlaps but no actual row matches); false negatives are not.
    ///
    /// ## Interval overlap condition
    ///
    /// For a single predicate `column BETWEEN pred.min AND pred.max`:
    ///   overlaps iff `col_stats.min <= pred.max AND col_stats.max >= pred.min`
    ///
    /// For multiple predicates (conjunction): a row group is a candidate only
    /// if ALL predicates overlap.
    ///
    /// Row groups that lack statistics for a predicate column are conservatively
    /// included (we cannot rule them out).
    pub fn query_overlap(&self, predicates: &[RangePredicate]) -> Vec<RowGroupRef> {
        if predicates.is_empty() {
            // No predicates → all row groups are candidates
            return self.all_row_group_refs();
        }

        let mut results = Vec::new();

        for file in &self.files {
            for rg in &file.row_groups {
                let mut all_match = true;

                for pred in predicates {
                    if let Some(col_stats) = rg.columns.get(&pred.column) {
                        // Interval overlap: min <= pred.max AND max >= pred.min
                        if col_stats.min.as_slice() > pred.max.as_slice()
                            || col_stats.max.as_slice() < pred.min.as_slice()
                        {
                            all_match = false;
                            break;
                        }
                    }
                    // Column not in stats → conservatively include (can't prune)
                }

                if all_match {
                    results.push(RowGroupRef {
                        file: file.path.clone(),
                        row_group: rg.row_group,
                    });
                }
            }
        }

        results
    }

    /// Query with a single predicate (convenience method).
    pub fn query_overlap_single(&self, predicate: &RangePredicate) -> Vec<RowGroupRef> {
        self.query_overlap(&[predicate.clone()])
    }

    /// Total number of row groups across all files.
    pub fn total_row_groups(&self) -> usize {
        self.files.iter().map(|f| f.row_groups.len()).sum()
    }

    /// Total number of files.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// All row group references (no filtering).
    fn all_row_group_refs(&self) -> Vec<RowGroupRef> {
        let mut refs = Vec::new();
        for file in &self.files {
            for rg in &file.row_groups {
                refs.push(RowGroupRef {
                    file: file.path.clone(),
                    row_group: rg.row_group,
                });
            }
        }
        refs
    }

    /// Serialize the manifest to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize a manifest from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

impl Default for StatsManifest {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Builder — ergonomic construction from Parquet footer metadata
// ===========================================================================

/// Builder for constructing a StatsManifest incrementally.
///
/// Typical usage: iterate Parquet file footers, extract row-group column
/// statistics, feed them to the builder.
pub struct StatsManifestBuilder {
    manifest: StatsManifest,
}

impl StatsManifestBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            manifest: StatsManifest::new(),
        }
    }

    /// Add statistics for a single row group in a file.
    pub fn add_row_group(
        mut self,
        file_path: &str,
        row_group: u32,
        columns: Vec<(String, ColumnStats)>,
    ) -> Self {
        self.manifest.add_row_group(file_path, row_group, columns);
        self
    }

    /// Consume the builder and return the manifest.
    pub fn build(self) -> StatsManifest {
        self.manifest
    }
}

impl Default for StatsManifestBuilder {
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

    fn build_time_series_manifest() -> StatsManifest {
        let mut manifest = StatsManifest::new();

        // File with 3 row groups, hourly data
        for rg in 0..3 {
            let hour_start = rg * 8;
            let hour_end = (rg + 1) * 8 - 1;
            manifest.add_row_group(
                "data/2024-01-15.parquet",
                rg,
                vec![
                    (
                        "timestamp".into(),
                        ColumnStats {
                            min: format!("2024-01-15T{:02}:00:00Z", hour_start).into_bytes(),
                            max: format!("2024-01-15T{:02}:59:59Z", hour_end).into_bytes(),
                            null_count: 0,
                        },
                    ),
                    (
                        "category".into(),
                        ColumnStats {
                            min: b"A".to_vec(),
                            max: b"Z".to_vec(),
                            null_count: 5,
                        },
                    ),
                ],
            );
        }

        // Second file — next day
        for rg in 0..3 {
            let hour_start = rg * 8;
            let hour_end = (rg + 1) * 8 - 1;
            manifest.add_row_group(
                "data/2024-01-16.parquet",
                rg,
                vec![(
                    "timestamp".into(),
                    ColumnStats {
                        min: format!("2024-01-16T{:02}:00:00Z", hour_start).into_bytes(),
                        max: format!("2024-01-16T{:02}:59:59Z", hour_end).into_bytes(),
                        null_count: 0,
                    },
                )],
            );
        }

        manifest
    }

    #[test]
    fn empty_predicates_returns_all() {
        let manifest = build_time_series_manifest();
        let results = manifest.query_overlap(&[]);
        assert_eq!(results.len(), 6); // 3 + 3 row groups
    }

    #[test]
    fn single_predicate_prunes_non_overlapping() {
        let manifest = build_time_series_manifest();

        // Query: 10:00–14:00 on Jan 15
        // RG0: 00:00–07:59 → no overlap
        // RG1: 08:00–15:59 → overlaps
        // RG2: 16:00–23:59 → no overlap
        // Jan 16 RGs: all before → no overlap
        let results = manifest.query_overlap(&[RangePredicate {
            column: "timestamp".into(),
            min: b"2024-01-15T10:00:00Z".to_vec(),
            max: b"2024-01-15T14:00:00Z".to_vec(),
        }]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file, "data/2024-01-15.parquet");
        assert_eq!(results[0].row_group, 1);
    }

    #[test]
    fn predicate_spanning_multiple_row_groups() {
        let manifest = build_time_series_manifest();

        // Query: 06:00–20:00 on Jan 15 — spans RG0 (partially), RG1, RG2 (partially)
        let results = manifest.query_overlap(&[RangePredicate {
            column: "timestamp".into(),
            min: b"2024-01-15T06:00:00Z".to_vec(),
            max: b"2024-01-15T20:00:00Z".to_vec(),
        }]);

        assert_eq!(results.len(), 3); // All 3 RGs of Jan 15
        assert!(results.iter().all(|r| r.file == "data/2024-01-15.parquet"));
    }

    #[test]
    fn predicate_spanning_multiple_files() {
        let manifest = build_time_series_manifest();

        // Query: Jan 15 20:00 – Jan 16 04:00
        let results = manifest.query_overlap(&[RangePredicate {
            column: "timestamp".into(),
            min: b"2024-01-15T20:00:00Z".to_vec(),
            max: b"2024-01-16T04:00:00Z".to_vec(),
        }]);

        // Jan 15 RG2 (16:00–23:59) overlaps
        // Jan 16 RG0 (00:00–07:59) overlaps
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn multi_column_conjunction() {
        let mut manifest = StatsManifest::new();

        // RG with timestamp 00:00–12:00 and category A–M
        manifest.add_row_group(
            "f.parquet",
            0,
            vec![
                (
                    "ts".into(),
                    ColumnStats {
                        min: b"00:00".to_vec(),
                        max: b"12:00".to_vec(),
                        null_count: 0,
                    },
                ),
                (
                    "cat".into(),
                    ColumnStats {
                        min: b"A".to_vec(),
                        max: b"M".to_vec(),
                        null_count: 0,
                    },
                ),
            ],
        );

        // RG with timestamp 12:00–23:59 and category N–Z
        manifest.add_row_group(
            "f.parquet",
            1,
            vec![
                (
                    "ts".into(),
                    ColumnStats {
                        min: b"12:00".to_vec(),
                        max: b"23:59".to_vec(),
                        null_count: 0,
                    },
                ),
                (
                    "cat".into(),
                    ColumnStats {
                        min: b"N".to_vec(),
                        max: b"Z".to_vec(),
                        null_count: 0,
                    },
                ),
            ],
        );

        // Query: ts in [10:00, 14:00] AND cat in [A, F]
        // RG0: ts overlaps (00:00–12:00 vs 10:00–14:00 ✓), cat overlaps (A–M vs A–F ✓) → match
        // RG1: ts overlaps (12:00–23:59 vs 10:00–14:00 ✓), cat NO overlap (N–Z vs A–F ✗) → pruned
        let results = manifest.query_overlap(&[
            RangePredicate {
                column: "ts".into(),
                min: b"10:00".to_vec(),
                max: b"14:00".to_vec(),
            },
            RangePredicate {
                column: "cat".into(),
                min: b"A".to_vec(),
                max: b"F".to_vec(),
            },
        ]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_group, 0);
    }

    #[test]
    fn missing_column_conservatively_includes() {
        let mut manifest = StatsManifest::new();
        manifest.add_row_group(
            "f.parquet",
            0,
            vec![(
                "ts".into(),
                ColumnStats {
                    min: b"00:00".to_vec(),
                    max: b"12:00".to_vec(),
                    null_count: 0,
                },
            )],
        );

        // Query on a column that has no stats → cannot prune → included
        let results = manifest.query_overlap(&[RangePredicate {
            column: "unknown_col".into(),
            min: b"X".to_vec(),
            max: b"Y".to_vec(),
        }]);

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn no_overlap_returns_empty() {
        let manifest = build_time_series_manifest();

        // Query for Jan 20 — no data exists for that date
        let results = manifest.query_overlap(&[RangePredicate {
            column: "timestamp".into(),
            min: b"2024-01-20T00:00:00Z".to_vec(),
            max: b"2024-01-20T23:59:59Z".to_vec(),
        }]);

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn serialization_round_trip() {
        let manifest = build_time_series_manifest();

        let json = manifest.to_json().unwrap();
        let decoded = StatsManifest::from_json(&json).unwrap();

        assert_eq!(manifest, decoded);
        assert_eq!(decoded.file_count(), 2);
        assert_eq!(decoded.total_row_groups(), 6);
    }

    #[test]
    fn builder_ergonomics() {
        let manifest = StatsManifestBuilder::new()
            .add_row_group(
                "a.parquet",
                0,
                vec![(
                    "id".into(),
                    ColumnStats {
                        min: b"000".to_vec(),
                        max: b"499".to_vec(),
                        null_count: 0,
                    },
                )],
            )
            .add_row_group(
                "a.parquet",
                1,
                vec![(
                    "id".into(),
                    ColumnStats {
                        min: b"500".to_vec(),
                        max: b"999".to_vec(),
                        null_count: 0,
                    },
                )],
            )
            .build();

        assert_eq!(manifest.file_count(), 1);
        assert_eq!(manifest.total_row_groups(), 2);

        // Point-ish range query: 300–600
        let results = manifest.query_overlap(&[RangePredicate {
            column: "id".into(),
            min: b"300".to_vec(),
            max: b"600".to_vec(),
        }]);
        assert_eq!(results.len(), 2); // Both RGs overlap
    }

    #[test]
    fn exact_boundary_overlap() {
        let mut manifest = StatsManifest::new();
        manifest.add_row_group(
            "f.parquet",
            0,
            vec![(
                "x".into(),
                ColumnStats {
                    min: b"100".to_vec(),
                    max: b"200".to_vec(),
                    null_count: 0,
                },
            )],
        );

        // Predicate max == col min → overlaps (inclusive)
        let results = manifest.query_overlap(&[RangePredicate {
            column: "x".into(),
            min: b"050".to_vec(),
            max: b"100".to_vec(),
        }]);
        assert_eq!(results.len(), 1);

        // Predicate min == col max → overlaps (inclusive)
        let results = manifest.query_overlap(&[RangePredicate {
            column: "x".into(),
            min: b"200".to_vec(),
            max: b"300".to_vec(),
        }]);
        assert_eq!(results.len(), 1);

        // Predicate entirely below → no overlap
        let results = manifest.query_overlap(&[RangePredicate {
            column: "x".into(),
            min: b"001".to_vec(),
            max: b"099".to_vec(),
        }]);
        assert_eq!(results.len(), 0);
    }
}
