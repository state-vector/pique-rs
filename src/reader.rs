//! Segment reader — performs point lookups and prefix scans on a segment.
//!
//! Two reader types:
//!
//! 1. **`SegmentReader`** — in-memory, entire segment loaded. For tests and
//!    small segments.
//!
//! 2. **`RemoteSegmentReader`** — backend-aware, uses the 2-request pattern:
//!    - Request 1 (cold start): tail read (last 256KB) → footer + bloom + FST
//!    - Request 2 (per lookup): single block range read
//!    Caches the metadata across lookups so steady-state is 1 request per lookup.
//!
//! ## Lookup flow
//!
//! ```text
//! key → bloom check → FST lookup → block offset → read block → binary search
//! ```

use crate::block::{BlockReader, BlockError};
use crate::bloom;
use crate::format::{Footer, FormatError, FOOTER_SIZE};
use crate::storage::{StorageBackend, StorageError};
use fst::{IntoStreamer, Streamer};

/// Default tail read budget — 256KB covers footer + bloom + FST for up to
/// ~200K keys. The measurement proves reading 256KB costs the same latency
/// as reading 64 bytes from S3.
pub const DEFAULT_TAIL_READ_BUDGET: u64 = 256 * 1024;

// ===========================================================================
// SegmentReader — in-memory (for tests and small segments)
// ===========================================================================

/// A fully in-memory segment reader. Loads everything at construction time.
pub struct SegmentReader {
    /// Complete segment data.
    data: Vec<u8>,
    /// Parsed footer.
    footer: Footer,
    /// FST map (key → block_offset).
    fst: fst::Map<Vec<u8>>,
    /// Bloom filter bytes (empty if not built).
    bloom_bytes: Vec<u8>,
}

impl SegmentReader {
    /// Open a segment from a complete byte buffer (in-memory mode).
    pub fn open(data: Vec<u8>) -> Result<Self, ReaderError> {
        if data.len() < FOOTER_SIZE {
            return Err(ReaderError::Format(FormatError::TooSmall {
                size: data.len() as u64,
            }));
        }

        // Parse footer (last 64 bytes)
        let footer_start = data.len() - FOOTER_SIZE;
        let footer_bytes: &[u8; FOOTER_SIZE] = data[footer_start..footer_start + FOOTER_SIZE]
            .try_into()
            .unwrap();
        let footer = Footer::from_bytes(footer_bytes).map_err(ReaderError::Format)?;

        // Load bloom filter
        let bloom_start = footer.bloom_offset as usize;
        let bloom_end = bloom_start + footer.bloom_length as usize;
        let bloom_bytes = if footer.bloom_length > 0 {
            data[bloom_start..bloom_end].to_vec()
        } else {
            Vec::new()
        };

        // Load FST
        let fst_start = footer.fst_offset as usize;
        let fst_end = fst_start + footer.fst_length as usize;
        let fst_data = data[fst_start..fst_end].to_vec();
        let fst = fst::Map::new(fst_data).map_err(|e| ReaderError::FstError(e.to_string()))?;

        Ok(Self {
            data,
            footer,
            fst,
            bloom_bytes,
        })
    }

    /// Open from a tail buffer — the last N bytes of the segment, plus the
    /// total segment size. Parses footer + bloom + FST from the tail buffer.
    /// Block reads will require the full data or a backend.
    ///
    /// This is the constructor used by `RemoteSegmentReader` internally.
    pub fn open_from_tail(tail: &[u8], segment_size: u64) -> Result<SegmentMetadata, ReaderError> {
        if tail.len() < FOOTER_SIZE {
            return Err(ReaderError::Format(FormatError::TooSmall {
                size: tail.len() as u64,
            }));
        }

        // Footer is at the END of the tail buffer
        let footer_start = tail.len() - FOOTER_SIZE;
        let footer_bytes: &[u8; FOOTER_SIZE] = tail[footer_start..footer_start + FOOTER_SIZE]
            .try_into()
            .unwrap();
        let footer = Footer::from_bytes(footer_bytes).map_err(ReaderError::Format)?;

        // Calculate where in the tail buffer the bloom and FST live.
        // The tail buffer contains the last `tail.len()` bytes of the segment.
        // Offsets in the footer are from segment start.
        let tail_start_offset = segment_size - tail.len() as u64;

        // Check that bloom + FST are within the tail buffer
        if footer.bloom_offset < tail_start_offset {
            return Err(ReaderError::TailTooSmall {
                needed: (segment_size - footer.bloom_offset) as usize,
                got: tail.len(),
            });
        }

        // Bloom filter
        let bloom_local_start = (footer.bloom_offset - tail_start_offset) as usize;
        let bloom_local_end = bloom_local_start + footer.bloom_length as usize;
        let bloom_bytes = if footer.bloom_length > 0 {
            tail[bloom_local_start..bloom_local_end].to_vec()
        } else {
            Vec::new()
        };

        // FST
        let fst_local_start = (footer.fst_offset - tail_start_offset) as usize;
        let fst_local_end = fst_local_start + footer.fst_length as usize;
        let fst_data = tail[fst_local_start..fst_local_end].to_vec();
        let fst = fst::Map::new(fst_data).map_err(|e| ReaderError::FstError(e.to_string()))?;

        Ok(SegmentMetadata {
            footer,
            fst,
            bloom_bytes,
        })
    }

    /// Look up a key in the segment. Returns the value if found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ReaderError> {
        // Step 1: Bloom filter check
        if !bloom::might_contain(&self.bloom_bytes, key) {
            return Ok(None);
        }

        // Step 2: FST lookup
        let block_offset = match find_block_for_key(&self.fst, key) {
            Some(offset) => offset,
            None => return Ok(None),
        };

        // Step 3: Read and search the block
        let block_data = self.read_block(block_offset)?;
        let reader = BlockReader::open(block_data).map_err(ReaderError::Block)?;

        match reader.get(key).map_err(ReaderError::Block)? {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    /// Check if a key might exist (bloom filter only). No I/O.
    pub fn might_contain(&self, key: &[u8]) -> bool {
        bloom::might_contain(&self.bloom_bytes, key)
    }

    /// Number of keys in the segment.
    pub fn key_count(&self) -> u32 {
        self.footer.key_count
    }

    /// Total segment size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.data.len() as u64
    }

    /// Iterate all entries in sorted order.
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ReaderError> {
        let mut results = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut stream = self.fst.stream();
        while let Some((_key, offset)) = stream.next() {
            offsets.push(offset);
        }
        for &offset in &offsets {
            let block_data = self.read_block(offset)?;
            let reader = BlockReader::open(block_data).map_err(ReaderError::Block)?;
            for entry in reader.iter() {
                let (key, value) = entry.map_err(ReaderError::Block)?;
                results.push((key, value.to_vec()));
            }
        }
        Ok(results)
    }

    fn read_block(&self, offset: u64) -> Result<&[u8], ReaderError> {
        let start = offset as usize;
        let data_end = self.footer.data_blocks_length as usize;
        if start >= data_end {
            return Err(ReaderError::InvalidBlockOffset(offset));
        }
        let end = find_block_end(&self.fst, offset, data_end);
        Ok(&self.data[start..end])
    }
}

// ===========================================================================
// SegmentMetadata — the cached portion (bloom + FST + footer)
// ===========================================================================

/// Cached segment metadata — loaded once from a single tail read,
/// reused for all subsequent lookups against this segment.
pub struct SegmentMetadata {
    pub footer: Footer,
    pub fst: fst::Map<Vec<u8>>,
    pub bloom_bytes: Vec<u8>,
}

impl SegmentMetadata {
    /// Check bloom filter (no I/O).
    pub fn might_contain(&self, key: &[u8]) -> bool {
        bloom::might_contain(&self.bloom_bytes, key)
    }

    /// Find the block offset for a key. Returns None if key > all keys.
    pub fn block_offset_for_key(&self, key: &[u8]) -> Option<u64> {
        find_block_for_key(&self.fst, key)
    }

    /// Calculate the byte range for a block given its offset.
    /// Returns (start, end) as absolute offsets within the segment.
    pub fn block_byte_range(&self, block_offset: u64) -> Option<(u64, u64)> {
        let data_end = self.footer.data_blocks_length as usize;
        if block_offset as usize >= data_end {
            return None;
        }
        let end = find_block_end(&self.fst, block_offset, data_end);
        Some((block_offset, end as u64))
    }

    /// Number of keys.
    pub fn key_count(&self) -> u32 {
        self.footer.key_count
    }

    /// Size of the metadata (bloom + FST + footer) in bytes.
    pub fn metadata_size(&self) -> usize {
        self.bloom_bytes.len() + self.fst.as_fst().as_bytes().len() + FOOTER_SIZE
    }
}

// ===========================================================================
// RemoteSegmentReader — 2-request pattern for object storage
// ===========================================================================

/// A segment reader that operates against a storage backend (S3, local, etc).
///
/// Uses the 2-request access pattern:
/// - **Initialization** (1 request): tail read → load footer + bloom + FST
/// - **Each lookup** (1 request): range-read a single data block
///
/// The metadata is cached for the reader's lifetime. Construct once per
/// segment, reuse for many lookups.
pub struct RemoteSegmentReader {
    backend: Box<dyn StorageBackend>,
    path: String,
    meta: SegmentMetadata,
}

impl RemoteSegmentReader {
    /// Open a remote segment. Performs one tail read to load metadata.
    ///
    /// The `tail_budget` controls how many bytes to read from the end of the
    /// object. Defaults to 256KB which covers segments up to ~200K keys.
    /// If the bloom+FST exceeds this budget, returns `TailTooSmall` error.
    pub async fn open(
        backend: Box<dyn StorageBackend>,
        path: String,
        tail_budget: Option<u64>,
    ) -> Result<Self, ReaderError> {
        let budget = tail_budget.unwrap_or(DEFAULT_TAIL_READ_BUDGET);

        // Get segment size
        let segment_size = backend
            .object_size(&path)
            .await
            .map_err(ReaderError::Storage)?;

        // Read the tail (one request)
        let read_size = budget.min(segment_size);
        let tail = backend
            .read_tail(&path, read_size)
            .await
            .map_err(ReaderError::Storage)?;

        // Parse metadata from tail
        let meta = SegmentReader::open_from_tail(&tail, segment_size)?;

        Ok(Self { backend, path, meta })
    }

    /// Point lookup. Returns the value for the given key, or None.
    ///
    /// Cost: 0 requests if bloom rejects, 1 range-read request otherwise.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ReaderError> {
        // Bloom check (no I/O)
        if !self.meta.might_contain(key) {
            return Ok(None);
        }

        // FST lookup (no I/O)
        let block_offset = match self.meta.block_offset_for_key(key) {
            Some(offset) => offset,
            None => return Ok(None),
        };

        // Block range read (1 request)
        let (start, end) = self
            .meta
            .block_byte_range(block_offset)
            .ok_or(ReaderError::InvalidBlockOffset(block_offset))?;

        let block_data = self
            .backend
            .read_range(&self.path, start..end)
            .await
            .map_err(ReaderError::Storage)?;

        // Decode block and binary search
        let reader = BlockReader::open(&block_data).map_err(ReaderError::Block)?;
        match reader.get(key).map_err(ReaderError::Block)? {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    /// Bloom-only check (no I/O).
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.meta.might_contain(key)
    }

    /// Access the cached metadata.
    pub fn metadata(&self) -> &SegmentMetadata {
        &self.meta
    }

    /// Number of keys in this segment.
    pub fn key_count(&self) -> u32 {
        self.meta.key_count()
    }

    /// Size of in-memory metadata cache (bloom + FST + footer).
    pub fn memory_usage(&self) -> usize {
        self.meta.metadata_size()
    }
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Find the block offset for a key using the FST.
/// The FST maps last_key_in_block → block_offset. We find the first entry
/// whose key >= target.
fn find_block_for_key(fst: &fst::Map<Vec<u8>>, key: &[u8]) -> Option<u64> {
    let mut stream = fst.range().ge(key).into_stream();
    stream.next().map(|(_, offset)| offset)
}

/// Find where a block ends (next block's start or end of data section).
fn find_block_end(fst: &fst::Map<Vec<u8>>, block_offset: u64, data_end: usize) -> usize {
    let mut stream = fst.stream();
    let mut found_current = false;
    while let Some((_key, offset)) = stream.next() {
        if found_current {
            return offset as usize;
        }
        if offset == block_offset {
            found_current = true;
        }
    }
    data_end
}

// ===========================================================================
// Errors
// ===========================================================================

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("Format error: {0}")]
    Format(#[from] FormatError),

    #[error("Block error: {0}")]
    Block(#[from] BlockError),

    #[error("FST error: {0}")]
    FstError(String),

    #[error("Invalid block offset: {0}")]
    InvalidBlockOffset(u64),

    #[error("Tail read too small: needed {needed} bytes but only read {got}")]
    TailTooSmall { needed: usize, got: usize },

    #[error("Storage error: {0}")]
    Storage(StorageError),
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{SegmentWriter, SegmentWriterOptions};

    fn build_test_segment(count: u32, block_size: u32) -> Vec<u8> {
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size,
            restart_interval: 4,
            enable_bloom: true,
        });

        for i in 0..count {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:06}", i);
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }

        writer.finish().unwrap().data
    }

    #[test]
    fn open_and_query_segment() {
        let data = build_test_segment(100, 512);
        let reader = SegmentReader::open(data).unwrap();

        assert_eq!(reader.key_count(), 100);

        let result = reader.get(b"key_000000").unwrap();
        assert_eq!(result, Some(b"value_000000".to_vec()));

        let result = reader.get(b"key_000050").unwrap();
        assert_eq!(result, Some(b"value_000050".to_vec()));

        let result = reader.get(b"key_000099").unwrap();
        assert_eq!(result, Some(b"value_000099".to_vec()));
    }

    #[test]
    fn query_missing_keys() {
        let data = build_test_segment(100, 512);
        let reader = SegmentReader::open(data).unwrap();

        assert_eq!(reader.get(b"key_000100").unwrap(), None);
        assert_eq!(reader.get(b"aaa_before").unwrap(), None);
        assert_eq!(reader.get(b"key_000050x").unwrap(), None);
    }

    #[test]
    fn bloom_filter_rejects_missing_keys() {
        let data = build_test_segment(1000, 4096);
        let reader = SegmentReader::open(data).unwrap();

        let mut bloom_rejections = 0;
        for i in 2000..3000 {
            let key = format!("nonexistent_{:06}", i);
            if !reader.might_contain(key.as_bytes()) {
                bloom_rejections += 1;
            }
        }

        assert!(
            bloom_rejections > 990,
            "Bloom should reject most missing keys, only rejected {}",
            bloom_rejections
        );
    }

    #[test]
    fn large_segment_correctness() {
        let count = 10_000;
        let data = build_test_segment(count, 64 * 1024);
        let reader = SegmentReader::open(data).unwrap();

        assert_eq!(reader.key_count(), count);

        for i in (0..count).step_by(100) {
            let key = format!("key_{:06}", i);
            let expected_val = format!("value_{:06}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(expected_val.into_bytes()),
                "Failed for key {}",
                key
            );
        }
    }

    #[test]
    fn single_entry_segment() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
        writer.add(b"only_key", b"only_value").unwrap();
        let output = writer.finish().unwrap();

        let reader = SegmentReader::open(output.data).unwrap();
        assert_eq!(reader.key_count(), 1);
        assert_eq!(reader.get(b"only_key").unwrap(), Some(b"only_value".to_vec()));
        assert_eq!(reader.get(b"other").unwrap(), None);
    }

    #[test]
    fn segment_with_long_keys() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 64 * 1024,
            restart_interval: 16,
            enable_bloom: true,
        });

        let keys: Vec<String> = (0..500)
            .map(|i| {
                format!(
                    "acme/myproject::src/query/src/translation.rs::function::execute_query_{:04}",
                    i
                )
            })
            .collect();

        for (i, key) in keys.iter().enumerate() {
            let val = format!("{{\"file\": \"part-{}.parquet\", \"row_group\": {}}}", i / 100, i % 100);
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let output = writer.finish().unwrap();
        let reader = SegmentReader::open(output.data).unwrap();

        assert_eq!(reader.key_count(), 500);

        let result = reader.get(keys[0].as_bytes()).unwrap();
        assert!(result.is_some());

        let result = reader.get(keys[250].as_bytes()).unwrap();
        assert!(result.is_some());
        let val_str = String::from_utf8(result.unwrap()).unwrap();
        assert!(val_str.contains("\"row_group\": 50"));

        let result = reader.get(b"acme/myproject::nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn open_from_tail_parses_metadata() {
        let data = build_test_segment(1000, 4096);
        let segment_size = data.len() as u64;

        // Simulate a tail read — last 256KB (or the whole thing if smaller)
        let tail_size = (256 * 1024).min(data.len());
        let tail = &data[data.len() - tail_size..];

        let meta = SegmentReader::open_from_tail(tail, segment_size).unwrap();
        assert_eq!(meta.key_count(), 1000);

        // Verify bloom works
        assert!(meta.might_contain(b"key_000500"));

        // Verify FST lookup works
        let offset = meta.block_offset_for_key(b"key_000500");
        assert!(offset.is_some());
    }

    #[tokio::test]
    async fn remote_reader_via_local_backend() {
        use crate::storage::LocalBackend;

        let tmp = tempfile::TempDir::new().unwrap();
        let backend = LocalBackend::new(tmp.path());
        let path = "test/segment.osi";

        // Build and store a segment
        let data = build_test_segment(500, 2048);
        backend.put(path, data).await.unwrap();

        // Open via remote reader
        let reader = RemoteSegmentReader::open(
            Box::new(LocalBackend::new(tmp.path())),
            path.to_string(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(reader.key_count(), 500);
        assert!(reader.memory_usage() > 0);

        // Point lookups
        let result = reader.get(b"key_000000").await.unwrap();
        assert_eq!(result, Some(b"value_000000".to_vec()));

        let result = reader.get(b"key_000250").await.unwrap();
        assert_eq!(result, Some(b"value_000250".to_vec()));

        let result = reader.get(b"key_000499").await.unwrap();
        assert_eq!(result, Some(b"value_000499".to_vec()));

        // Missing key
        let result = reader.get(b"nonexistent").await.unwrap();
        assert_eq!(result, None);

        // Bloom rejection
        assert!(!reader.might_contain(b"definitely_not_here_xyz"));
    }
}
