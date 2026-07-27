//! # pique
//!
//! Immutable, S3-native secondary index segments for Parquet acceleration.
//!
//! This library provides a compact, immutable key→value index format designed
//! to be stored as single S3 objects and queried with minimal range reads (1–2
//! per point lookup). It is NOT a database — it's a derived, read-only index
//! that accelerates lookups into authoritative Parquet storage.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  Segment File (single S3 object)        │
//! ├─────────────────────────────────────────┤
//! │  Data blocks (prefix-compressed KV)     │
//! │  Block 0, Block 1, ..., Block N         │
//! ├─────────────────────────────────────────┤
//! │  Bloom filter (XOR8 filter)             │
//! ├─────────────────────────────────────────┤
//! │  FST directory (key → block offset)     │
//! ├─────────────────────────────────────────┤
//! │  Footer (fixed 64 bytes)                │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## Lookup flow
//!
//! 1. Read footer (last 64 bytes) → learn section offsets
//! 2. Read FST + bloom (cached after first access)
//! 3. Check bloom → early exit on definite miss
//! 4. FST lookup → block offset
//! 5. Range-read single data block → binary search → return value
//!
//! Cold lookup: 2 S3 range requests. Warm (directory cached): 1 request.

pub mod block;
pub mod bloom;
pub mod format;
pub mod layered;
pub mod partitioned;
pub mod reader;
pub mod storage;
pub mod values;
pub mod writer;

// Re-export key public types
pub use format::{Footer, SegmentMeta, MAGIC, FORMAT_VERSION};
pub use reader::{SegmentReader, SegmentMetadata, RemoteSegmentReader, ReaderError};
pub use writer::{SegmentWriter, SegmentWriterOptions, SegmentOutput};
pub use storage::{StorageBackend, LocalBackend, StorageError};
pub use layered::{
    LayeredReader, RemoteLayeredReader, LayeredIndexManifest, SegmentRef,
    merge_segments, tombstone_value, is_tombstone,
    TOMBSTONE_MARKER, MAX_DELTA_COUNT, MAX_DELTA_RATIO,
};

#[cfg(feature = "s3")]
pub use storage::S3Backend;
