#![doc = include_str!("../README.md")]

pub mod block;
pub mod bloom;
pub mod format;
pub mod layered;
pub mod partitioned;
pub mod reader;
pub mod stats;
pub mod storage;
pub mod values;
pub mod writer;

// Re-export key public types
pub use format::{FORMAT_VERSION, Footer, MAGIC, SegmentMeta};
pub use layered::{
    LayeredIndexManifest, LayeredReader, MAX_DELTA_COUNT, MAX_DELTA_RATIO, RemoteLayeredReader,
    SegmentRef, TOMBSTONE_MARKER, is_tombstone, merge_segments, tombstone_value,
};
pub use reader::{ReaderError, RemoteSegmentReader, SegmentMetadata, SegmentReader};
pub use storage::{LocalBackend, StorageBackend, StorageError};
pub use stats::{
    ColumnStats, RangePredicate, RowGroupRef, RowGroupStats, StatsManifest, StatsManifestBuilder,
};
pub use writer::{SegmentOutput, SegmentWriter, SegmentWriterOptions};

#[cfg(feature = "s3")]
pub use storage::S3Backend;
