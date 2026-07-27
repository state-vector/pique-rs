//! Segment writer — builds a complete segment from sorted key→value pairs.
//!
//! Usage:
//! ```ignore
//! let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
//!
//! // Keys MUST be added in sorted order
//! writer.add(b"key_001", b"value_001")?;
//! writer.add(b"key_002", b"value_002")?;
//! // ...
//!
//! let output = writer.finish()?;
//! // output.data contains the complete segment bytes
//! // output.meta contains segment metadata for the manifest
//! ```

use crate::block::TrackedBlockBuilder;
use crate::bloom;
use crate::format::{Footer, SegmentMeta, DEFAULT_BLOCK_SIZE, DEFAULT_RESTART_INTERVAL, FORMAT_VERSION};

/// Options for the segment writer.
#[derive(Debug, Clone)]
pub struct SegmentWriterOptions {
    /// Target block size in bytes.
    pub block_size: u32,
    /// Number of entries between restart points.
    pub restart_interval: u32,
    /// Whether to build a bloom filter.
    pub enable_bloom: bool,
}

impl Default for SegmentWriterOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            enable_bloom: true,
        }
    }
}

/// The output of finishing a segment build.
pub struct SegmentOutput {
    /// Complete segment bytes (ready to upload to S3 as a single object).
    pub data: Vec<u8>,
    /// Metadata for the manifest.
    pub meta: SegmentMeta,
}

/// Builds an immutable segment from sorted key→value pairs.
pub struct SegmentWriter {
    opts: SegmentWriterOptions,
    /// Finished data blocks (concatenated bytes).
    data_blocks: Vec<u8>,
    /// FST builder entries: (last_key_in_block, block_offset).
    /// We store the LAST key of each block as the FST key, mapping to the
    /// block's byte offset. During lookup, we find the first FST entry >= target.
    fst_entries: Vec<(Vec<u8>, u64)>,
    /// All keys seen (for bloom filter construction).
    all_keys: Vec<Vec<u8>>,
    /// Current in-progress block builder.
    current_block: TrackedBlockBuilder,
    /// Byte offset where the next block will start.
    current_offset: u64,
    /// Total entries written.
    key_count: u32,
    /// First key across all blocks (for segment metadata).
    first_key: Option<Vec<u8>>,
    /// Last key across all blocks (for segment metadata).
    last_key: Option<Vec<u8>>,
    /// Whether any key has been added.
    has_entries: bool,
    /// Previous key (for sort-order validation).
    prev_key: Option<Vec<u8>>,
}

impl SegmentWriter {
    /// Create a new segment writer with the given options.
    pub fn new(opts: SegmentWriterOptions) -> Self {
        let block_size = opts.block_size;
        let restart_interval = opts.restart_interval;
        Self {
            opts,
            data_blocks: Vec::new(),
            fst_entries: Vec::new(),
            all_keys: Vec::new(),
            current_block: TrackedBlockBuilder::new(block_size, restart_interval),
            current_offset: 0,
            key_count: 0,
            first_key: None,
            last_key: None,
            has_entries: false,
            prev_key: None,
        }
    }

    /// Add a key-value pair. Keys MUST be added in strictly ascending sorted order.
    ///
    /// Returns an error if keys are out of order or if a duplicate is detected.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), WriterError> {
        // Validate sort order
        if let Some(ref prev) = self.prev_key {
            match key.cmp(prev.as_slice()) {
                std::cmp::Ordering::Less => return Err(WriterError::KeysOutOfOrder),
                std::cmp::Ordering::Equal => return Err(WriterError::DuplicateKey),
                std::cmp::Ordering::Greater => {}
            }
        }

        // Track first/last key
        if !self.has_entries {
            self.first_key = Some(key.to_vec());
            self.has_entries = true;
        }
        self.last_key = Some(key.to_vec());
        self.prev_key = Some(key.to_vec());

        // Collect for bloom filter
        if self.opts.enable_bloom {
            self.all_keys.push(key.to_vec());
        }

        // Try to add to the current block
        if !self.current_block.add(key, value) {
            // Block is full — flush it and start a new one
            self.flush_block();
            // Add the entry to the new block (guaranteed to succeed — first entry)
            let added = self.current_block.add(key, value);
            debug_assert!(added, "First entry in a fresh block must succeed");
        }

        self.key_count += 1;
        Ok(())
    }

    /// Finish building the segment. Returns the complete segment bytes and metadata.
    pub fn finish(mut self) -> Result<SegmentOutput, WriterError> {
        if !self.has_entries {
            return Err(WriterError::EmptySegment);
        }

        // Flush the last in-progress block
        if !self.current_block.is_empty() {
            self.flush_block();
        }

        // --- Build bloom filter ---
        let bloom_bytes = if self.opts.enable_bloom {
            let key_refs: Vec<&[u8]> = self.all_keys.iter().map(|k| k.as_slice()).collect();
            bloom::build_filter(&key_refs)
        } else {
            Vec::new()
        };

        // --- Build FST directory ---
        let fst_bytes = self.build_fst()?;

        // --- Assemble the final segment ---
        let data_blocks_len = self.data_blocks.len() as u64;
        let bloom_offset = data_blocks_len;
        let bloom_len = bloom_bytes.len() as u64;
        let fst_offset = bloom_offset + bloom_len;
        let fst_len = fst_bytes.len() as u64;

        let footer = Footer {
            format_version: FORMAT_VERSION,
            data_blocks_offset: 0,
            data_blocks_length: data_blocks_len,
            bloom_offset,
            bloom_length: bloom_len,
            fst_offset,
            fst_length: fst_len,
            key_count: self.key_count,
            checksum: 0, // Computed by to_bytes()
        };

        let footer_bytes = footer.to_bytes();

        // Concatenate: data_blocks + bloom + fst + footer
        let total_size = self.data_blocks.len() + bloom_bytes.len() + fst_bytes.len() + footer_bytes.len();
        let mut output = Vec::with_capacity(total_size);
        output.extend_from_slice(&self.data_blocks);
        output.extend_from_slice(&bloom_bytes);
        output.extend_from_slice(&fst_bytes);
        output.extend_from_slice(&footer_bytes);

        let meta = SegmentMeta {
            generation: 0, // Caller sets this
            path: String::new(), // Caller sets this
            size_bytes: output.len() as u64,
            key_count: self.key_count,
            min_key: self.first_key.unwrap_or_default(),
            max_key: self.last_key.unwrap_or_default(),
        };

        Ok(SegmentOutput { data: output, meta })
    }

    /// Flush the current block to the data_blocks buffer.
    fn flush_block(&mut self) {
        let block_offset = self.current_offset;

        // Take the current block and finish it
        let old_block = std::mem::replace(
            &mut self.current_block,
            TrackedBlockBuilder::new(self.opts.block_size, self.opts.restart_interval),
        );

        let (block_bytes, _first_key, last_key) = old_block.finish();

        // Record for FST: last_key → block_offset
        // During lookup: FST.range().ge(target_key) gives us the block whose
        // last_key >= target_key, which is the block that might contain it.
        self.fst_entries.push((last_key, block_offset));

        self.current_offset += block_bytes.len() as u64;
        self.data_blocks.extend_from_slice(&block_bytes);
    }

    /// Build the FST from accumulated (last_key, block_offset) entries.
    fn build_fst(&self) -> Result<Vec<u8>, WriterError> {
        use fst::MapBuilder;

        let mut builder = MapBuilder::memory();

        for (key, offset) in &self.fst_entries {
            builder
                .insert(key, *offset)
                .map_err(|e| WriterError::FstBuildError(e.to_string()))?;
        }

        builder.into_inner().map_err(|e| WriterError::FstBuildError(e.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("Keys must be added in strictly ascending sorted order")]
    KeysOutOfOrder,

    #[error("Duplicate key detected")]
    DuplicateKey,

    #[error("Cannot finish an empty segment (no entries added)")]
    EmptySegment,

    #[error("FST build error: {0}")]
    FstBuildError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Footer, FOOTER_SIZE};

    #[test]
    fn build_simple_segment() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 256, // Small blocks to force multiple blocks
            restart_interval: 4,
            enable_bloom: true,
        });

        for i in 0..100u32 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:06}", i);
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let output = writer.finish().unwrap();

        // Verify metadata
        assert_eq!(output.meta.key_count, 100);
        assert_eq!(output.meta.min_key, b"key_000000");
        assert_eq!(output.meta.max_key, b"key_000099");
        assert!(output.meta.size_bytes > 0);
        assert_eq!(output.data.len(), output.meta.size_bytes as usize);

        // Verify footer can be read back
        let footer_start = output.data.len() - FOOTER_SIZE;
        let footer_bytes: &[u8; FOOTER_SIZE] =
            output.data[footer_start..].try_into().unwrap();
        let footer = Footer::from_bytes(footer_bytes).unwrap();

        assert_eq!(footer.key_count, 100);
        assert!(footer.bloom_length > 0);
        assert!(footer.fst_length > 0);
        assert_eq!(footer.data_blocks_offset, 0);
        assert!(footer.data_blocks_length > 0);
    }

    #[test]
    fn rejects_unsorted_keys() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
        writer.add(b"bbb", b"1").unwrap();
        let result = writer.add(b"aaa", b"2");
        assert!(matches!(result, Err(WriterError::KeysOutOfOrder)));
    }

    #[test]
    fn rejects_duplicate_keys() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
        writer.add(b"aaa", b"1").unwrap();
        let result = writer.add(b"aaa", b"2");
        assert!(matches!(result, Err(WriterError::DuplicateKey)));
    }

    #[test]
    fn rejects_empty_segment() {
        let writer = SegmentWriter::new(SegmentWriterOptions::default());
        let result = writer.finish();
        assert!(matches!(result, Err(WriterError::EmptySegment)));
    }

    #[test]
    fn segment_with_large_values() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: 4096,
            restart_interval: 8,
            enable_bloom: true,
        });

        // Simulate adjacency lists (large values)
        for i in 0..50u32 {
            let key = format!("entity_{:06}", i);
            // Value is a "compressed adjacency list" — just random bytes for now
            let val = vec![i as u8; 500]; // 500-byte values
            writer.add(key.as_bytes(), &val).unwrap();
        }

        let output = writer.finish().unwrap();
        assert_eq!(output.meta.key_count, 50);
        // With 500-byte values and 4KB blocks, expect ~6-8 entries per block
        // So ~7-9 blocks
        assert!(output.data.len() > 50 * 500); // At least the raw values
    }

    #[test]
    fn no_bloom_filter_option() {
        let mut writer = SegmentWriter::new(SegmentWriterOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            enable_bloom: false,
        });

        writer.add(b"key1", b"val1").unwrap();
        writer.add(b"key2", b"val2").unwrap();

        let output = writer.finish().unwrap();

        let footer_start = output.data.len() - FOOTER_SIZE;
        let footer_bytes: &[u8; FOOTER_SIZE] =
            output.data[footer_start..].try_into().unwrap();
        let footer = Footer::from_bytes(footer_bytes).unwrap();

        // Bloom section should be empty
        assert_eq!(footer.bloom_length, 0);
    }
}
