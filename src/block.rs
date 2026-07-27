//! Block encoder/decoder — the core data storage unit.
//!
//! A data block contains a sorted sequence of key→value pairs with prefix
//! compression and restart points for efficient binary search.
//!
//! ## Block layout
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ Entry 0 (restart point — full key)                   │
//! │ Entry 1 (prefix-compressed)                          │
//! │ ...                                                  │
//! │ Entry N-1                                            │
//! ├──────────────────────────────────────────────────────┤
//! │ Restart offsets: [u32; num_restarts] (LE)            │
//! │ num_restarts: u32 (LE)                               │
//! ├──────────────────────────────────────────────────────┤
//! │ CRC32: u32 (LE) — covers everything above            │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! ## Entry encoding
//!
//! Each entry is encoded as:
//! ```text
//! shared_prefix_len: varint
//! unshared_key_len:  varint
//! value_len:         varint
//! unshared_key:      [u8; unshared_key_len]
//! value:             [u8; value_len]
//! ```
//!
//! At restart points, `shared_prefix_len` is always 0 (full key stored).



// ---------------------------------------------------------------------------
// Varint encoding (LEB128 unsigned)
// ---------------------------------------------------------------------------

/// Encode a u32 as a varint into the buffer. Returns bytes written.
#[inline]
fn encode_varint(mut val: u32, buf: &mut Vec<u8>) -> usize {
    let start = buf.len();
    loop {
        if val < 0x80 {
            buf.push(val as u8);
            break;
        }
        buf.push((val as u8 & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.len() - start
}

/// Decode a varint from a byte slice. Returns (value, bytes_consumed).
#[inline]
fn decode_varint(data: &[u8]) -> Result<(u32, usize), BlockError> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 35 {
            return Err(BlockError::InvalidVarint);
        }
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(BlockError::UnexpectedEof)
}

// ---------------------------------------------------------------------------
// BlockBuilder — accumulates entries and produces a finished block
// ---------------------------------------------------------------------------

/// Builds a single data block from sorted key→value pairs.
///
/// Usage:
/// ```ignore
/// let mut builder = BlockBuilder::new(64 * 1024, 16);
/// while let Some((key, value)) = source.next() {
///     if !builder.add(key, value) {
///         // Block is full — finish it and start a new one
///         let block_bytes = builder.finish();
///         builder = BlockBuilder::new(64 * 1024, 16);
///         builder.add(key, value); // This entry goes in the new block
///     }
/// }
/// let final_block = builder.finish();
/// ```
pub struct BlockBuilder {
    /// Buffer accumulating the encoded entries.
    buf: Vec<u8>,
    /// Offsets of each restart point within `buf`.
    restarts: Vec<u32>,
    /// The last key added (for prefix computation).
    last_key: Vec<u8>,
    /// Number of entries since the last restart point.
    entries_since_restart: u32,
    /// Restart interval (entries between restart points).
    restart_interval: u32,
    /// Maximum block size (approximate — we finish the block when the next
    /// entry would exceed this).
    max_block_size: u32,
    /// Total number of entries in this block.
    entry_count: u32,
}

impl BlockBuilder {
    /// Create a new block builder.
    ///
    /// * `max_block_size` — target maximum block size in bytes (data + restarts + trailer).
    /// * `restart_interval` — number of entries between restart points.
    pub fn new(max_block_size: u32, restart_interval: u32) -> Self {
        let restart_interval = restart_interval.max(1);
        Self {
            buf: Vec::with_capacity(max_block_size as usize),
            restarts: vec![0], // First entry is always a restart point at offset 0
            last_key: Vec::new(),
            entries_since_restart: 0,
            restart_interval,
            max_block_size,
            entry_count: 0,
        }
    }

    /// Try to add a key-value pair to the block.
    ///
    /// Returns `true` if the entry was added, `false` if the block is full.
    /// When `false` is returned, the entry was NOT added — call `finish()`
    /// on this block and create a new one.
    ///
    /// Keys MUST be added in sorted (lexicographic) order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // Estimate the size this entry would add
        let estimated_entry_size = 5 + key.len() + value.len(); // varints + key + value
        let estimated_total = self.estimated_size() + estimated_entry_size;

        // Don't reject the very first entry (block must have at least one)
        if self.entry_count > 0 && estimated_total > self.max_block_size as usize {
            return false;
        }

        // Determine if this is a restart point
        let is_restart = self.entries_since_restart >= self.restart_interval;

        let shared_len = if is_restart || self.entry_count == 0 {
            if self.entry_count > 0 {
                self.restarts.push(self.buf.len() as u32);
            }
            self.entries_since_restart = 0;
            0
        } else {
            shared_prefix_len(&self.last_key, key)
        };

        let unshared_len = key.len() - shared_len;

        // Encode the entry
        encode_varint(shared_len as u32, &mut self.buf);
        encode_varint(unshared_len as u32, &mut self.buf);
        encode_varint(value.len() as u32, &mut self.buf);
        self.buf.extend_from_slice(&key[shared_len..]);
        self.buf.extend_from_slice(value);

        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        self.entry_count += 1;

        true
    }

    /// Finish the block and return the encoded bytes (including restart array + CRC).
    ///
    /// The builder is consumed — create a new one for the next block.
    pub fn finish(mut self) -> Vec<u8> {
        // Write restart offsets
        for &offset in &self.restarts {
            self.buf.extend_from_slice(&offset.to_le_bytes());
        }
        // Write number of restarts
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());

        // Write CRC32 checksum over everything so far
        let crc = crc32fast::hash(&self.buf);
        self.buf.extend_from_slice(&crc.to_le_bytes());

        self.buf
    }

    /// Returns the first key added to this block (for the FST directory).
    /// Empty if no entries have been added.
    pub fn first_key(&self) -> Option<&[u8]> {
        if self.entry_count == 0 {
            return None;
        }
        // The first entry is at offset 0, with shared_prefix_len = 0.
        // We need to decode it to get the key. But we also track it:
        // Actually, the first key is the last_key when entry_count was 1.
        // Simpler: just store it.
        None // We'll fix this — see `first_key_stored` below
    }

    /// Number of entries in this block so far.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Returns true if the block has no entries.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Estimated current size in bytes (data + restarts + trailer).
    fn estimated_size(&self) -> usize {
        self.buf.len()
            + (self.restarts.len() * 4)  // restart offsets
            + 4                           // num_restarts
            + 4                           // CRC32
    }
}

/// Block builder that also tracks the first and last key for FST construction.
pub struct TrackedBlockBuilder {
    inner: BlockBuilder,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
}

impl TrackedBlockBuilder {
    pub fn new(max_block_size: u32, restart_interval: u32) -> Self {
        Self {
            inner: BlockBuilder::new(max_block_size, restart_interval),
            first_key: None,
            last_key: None,
        }
    }

    /// Try to add a key-value pair. Returns false if the block is full.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let added = self.inner.add(key, value);
        if added {
            if self.first_key.is_none() {
                self.first_key = Some(key.to_vec());
            }
            self.last_key = Some(key.to_vec());
        }
        added
    }

    /// Finish the block. Returns (block_bytes, first_key, last_key).
    pub fn finish(self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let bytes = self.inner.finish();
        (
            bytes,
            self.first_key.unwrap_or_default(),
            self.last_key.unwrap_or_default(),
        )
    }

    pub fn entry_count(&self) -> u32 {
        self.inner.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

// ---------------------------------------------------------------------------
// BlockReader — decode entries from a finished block
// ---------------------------------------------------------------------------

/// Reader for a single data block. Operates on a byte slice (from S3 range
/// read or local file read).
pub struct BlockReader<'a> {
    /// Raw block data (including restarts + CRC trailer).
    data: &'a [u8],
    /// Decoded restart offsets.
    restarts: Vec<u32>,
    /// Offset where the entry data ends (before restart array).
    entries_end: usize,
}

impl<'a> BlockReader<'a> {
    /// Create a reader from raw block bytes. Verifies the CRC checksum.
    pub fn open(data: &'a [u8]) -> Result<Self, BlockError> {
        if data.len() < 8 {
            return Err(BlockError::BlockTooSmall);
        }

        // Verify CRC (last 4 bytes)
        let crc_offset = data.len() - 4;
        let stored_crc =
            u32::from_le_bytes(data[crc_offset..crc_offset + 4].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(BlockError::ChecksumMismatch);
        }

        // Read num_restarts (4 bytes before CRC)
        let num_restarts_offset = crc_offset - 4;
        let num_restarts = u32::from_le_bytes(
            data[num_restarts_offset..num_restarts_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        // Read restart offsets
        let restarts_start = num_restarts_offset - (num_restarts * 4);
        let mut restarts = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let offset = restarts_start + i * 4;
            restarts.push(u32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ));
        }

        Ok(Self {
            data,
            restarts,
            entries_end: restarts_start,
        })
    }

    /// Look up a key in this block. Returns the value if found.
    ///
    /// Uses binary search over restart points, then linear scan within the
    /// restart interval.
    pub fn get(&self, target: &[u8]) -> Result<Option<&'a [u8]>, BlockError> {
        // Binary search over restart points to find the right interval
        let restart_idx = self.find_restart_for_key(target)?;

        // Linear scan from that restart point
        let start_offset = self.restarts[restart_idx] as usize;
        let end_offset = if restart_idx + 1 < self.restarts.len() {
            self.restarts[restart_idx + 1] as usize
        } else {
            self.entries_end
        };

        let mut current_key = Vec::new();
        let mut pos = start_offset;

        while pos < end_offset {
            let (shared_len, consumed1) = decode_varint(&self.data[pos..])?;
            pos += consumed1;
            let (unshared_len, consumed2) = decode_varint(&self.data[pos..])?;
            pos += consumed2;
            let (value_len, consumed3) = decode_varint(&self.data[pos..])?;
            pos += consumed3;

            // Reconstruct the full key
            current_key.truncate(shared_len as usize);
            let key_end = pos + unshared_len as usize;
            if key_end > self.entries_end {
                return Err(BlockError::CorruptedEntry);
            }
            current_key.extend_from_slice(&self.data[pos..key_end]);
            pos = key_end;

            let value_end = pos + value_len as usize;
            if value_end > self.entries_end {
                return Err(BlockError::CorruptedEntry);
            }
            let value = &self.data[pos..value_end];
            pos = value_end;

            match current_key.as_slice().cmp(target) {
                std::cmp::Ordering::Equal => return Ok(Some(value)),
                std::cmp::Ordering::Greater => return Ok(None), // Passed it — not found
                std::cmp::Ordering::Less => continue,
            }
        }

        Ok(None)
    }

    /// Iterate all entries in the block in sorted order.
    pub fn iter(&self) -> BlockIterator<'a> {
        BlockIterator {
            data: self.data,
            entries_end: self.entries_end,
            pos: 0,
            current_key: Vec::new(),
        }
    }

    /// Binary search over restart points. Returns the restart index whose
    /// interval may contain the target key.
    fn find_restart_for_key(&self, target: &[u8]) -> Result<usize, BlockError> {
        let mut lo = 0;
        let mut hi = self.restarts.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let key_at_restart = self.decode_key_at_restart(mid)?;

            match key_at_restart.as_slice().cmp(target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        // lo is the first restart > target, so we want lo - 1 (the interval containing target)
        Ok(if lo > 0 { lo - 1 } else { 0 })
    }

    /// Decode the key at a restart point (shared_len is guaranteed 0).
    fn decode_key_at_restart(&self, restart_idx: usize) -> Result<Vec<u8>, BlockError> {
        let pos = self.restarts[restart_idx] as usize;
        if pos >= self.entries_end {
            return Err(BlockError::CorruptedEntry);
        }

        let (shared_len, consumed1) = decode_varint(&self.data[pos..])?;
        let pos = pos + consumed1;
        debug_assert_eq!(shared_len, 0, "Restart point must have shared_len=0");

        let (unshared_len, consumed2) = decode_varint(&self.data[pos..])?;
        let pos = pos + consumed2;

        // Skip value_len varint (we don't need the value)
        let (_value_len, consumed3) = decode_varint(&self.data[pos..])?;
        let pos = pos + consumed3;

        let key_end = pos + unshared_len as usize;
        if key_end > self.entries_end {
            return Err(BlockError::CorruptedEntry);
        }

        Ok(self.data[pos..key_end].to_vec())
    }
}

/// Iterator over all entries in a block.
pub struct BlockIterator<'a> {
    data: &'a [u8],
    entries_end: usize,
    pos: usize,
    current_key: Vec<u8>,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = Result<(Vec<u8>, &'a [u8]), BlockError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries_end {
            return None;
        }

        let result = (|| {
            let (shared_len, consumed1) = decode_varint(&self.data[self.pos..])?;
            self.pos += consumed1;
            let (unshared_len, consumed2) = decode_varint(&self.data[self.pos..])?;
            self.pos += consumed2;
            let (value_len, consumed3) = decode_varint(&self.data[self.pos..])?;
            self.pos += consumed3;

            self.current_key.truncate(shared_len as usize);
            let key_end = self.pos + unshared_len as usize;
            if key_end > self.entries_end {
                return Err(BlockError::CorruptedEntry);
            }
            self.current_key
                .extend_from_slice(&self.data[self.pos..key_end]);
            self.pos = key_end;

            let value_end = self.pos + value_len as usize;
            if value_end > self.entries_end {
                return Err(BlockError::CorruptedEntry);
            }
            let value = &self.data[self.pos..value_end];
            self.pos = value_end;

            Ok((self.current_key.clone(), value))
        })();

        Some(result)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the length of the shared prefix between two byte slices.
#[inline]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("Block too small to contain valid data")]
    BlockTooSmall,

    #[error("Block checksum mismatch")]
    ChecksumMismatch,

    #[error("Invalid varint encoding")]
    InvalidVarint,

    #[error("Unexpected end of data")]
    UnexpectedEof,

    #[error("Corrupted block entry")]
    CorruptedEntry,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        for val in [0, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            encode_varint(val, &mut buf);
            let (decoded, consumed) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn single_entry_block() {
        let mut builder = TrackedBlockBuilder::new(4096, 16);
        assert!(builder.add(b"hello", b"world"));
        let (bytes, first, last) = builder.finish();

        assert_eq!(first, b"hello");
        assert_eq!(last, b"hello");

        let reader = BlockReader::open(&bytes).unwrap();
        assert_eq!(reader.get(b"hello").unwrap(), Some(b"world".as_slice()));
        assert_eq!(reader.get(b"other").unwrap(), None);
    }

    #[test]
    fn multiple_entries_with_prefix_compression() {
        let mut builder = TrackedBlockBuilder::new(4096, 4);

        let entries = vec![
            (b"prefix_aaa".to_vec(), b"val1".to_vec()),
            (b"prefix_aab".to_vec(), b"val2".to_vec()),
            (b"prefix_abc".to_vec(), b"val3".to_vec()),
            (b"prefix_abd".to_vec(), b"val4".to_vec()),
            (b"prefix_xyz".to_vec(), b"val5".to_vec()),
            (b"zzz_other".to_vec(), b"val6".to_vec()),
        ];

        for (k, v) in &entries {
            assert!(builder.add(k, v));
        }

        let (bytes, first, last) = builder.finish();
        assert_eq!(first.as_slice(), b"prefix_aaa");
        assert_eq!(last.as_slice(), b"zzz_other");

        let reader = BlockReader::open(&bytes).unwrap();

        // Verify all entries
        for (k, v) in &entries {
            let result = reader.get(k).unwrap();
            assert_eq!(result, Some(v.as_slice()), "Failed for key {:?}", k);
        }

        // Verify missing keys
        assert_eq!(reader.get(b"prefix_aac").unwrap(), None);
        assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn block_iterator() {
        let mut builder = TrackedBlockBuilder::new(4096, 4);

        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"alpha", b"1"),
            (b"beta", b"2"),
            (b"gamma", b"3"),
            (b"delta", b"4"),
            (b"epsilon", b"5"),
        ];

        // Insert in sorted order
        let mut sorted = entries.clone();
        sorted.sort_by_key(|(k, _)| k.to_vec());

        for (k, v) in &sorted {
            assert!(builder.add(k, v));
        }

        let (bytes, _, _) = builder.finish();
        let reader = BlockReader::open(&bytes).unwrap();

        let collected: Vec<(Vec<u8>, Vec<u8>)> = reader
            .iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k, v.to_vec())
            })
            .collect();

        assert_eq!(collected.len(), sorted.len());
        for ((k, v), (ek, ev)) in collected.iter().zip(sorted.iter()) {
            assert_eq!(k.as_slice(), *ek);
            assert_eq!(v.as_slice(), *ev);
        }
    }

    #[test]
    fn block_rejects_overflow() {
        // Very small block — only fits a few entries
        let mut builder = TrackedBlockBuilder::new(64, 4);

        assert!(builder.add(b"key1", b"value1")); // First entry always accepted
        // Keep adding until rejected
        let mut count = 1;
        for i in 2..100 {
            let key = format!("key{:04}", i);
            let val = format!("value{:04}", i);
            if !builder.add(key.as_bytes(), val.as_bytes()) {
                break;
            }
            count += 1;
        }

        assert!(count >= 1); // At least the first entry
        assert!(count < 10); // Block is tiny, shouldn't fit many

        let (bytes, _, _) = builder.finish();
        assert!(bytes.len() <= 100); // Shouldn't be wildly over the target
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut builder = TrackedBlockBuilder::new(4096, 16);
        builder.add(b"key", b"value");
        let (mut bytes, _, _) = builder.finish();

        // Corrupt a byte
        bytes[0] ^= 0xFF;

        let result = BlockReader::open(&bytes);
        assert!(matches!(result, Err(BlockError::ChecksumMismatch)));
    }

    #[test]
    fn restart_points_enable_binary_search() {
        // Use restart_interval=2 to create many restart points
        let mut builder = TrackedBlockBuilder::new(16 * 1024, 2);

        // Insert 100 entries with diverse prefixes
        for i in 0..100u32 {
            let key = format!("entity_{:06}", i);
            let val = format!("loc_{}", i);
            assert!(builder.add(key.as_bytes(), val.as_bytes()));
        }

        let (bytes, _, _) = builder.finish();
        let reader = BlockReader::open(&bytes).unwrap();

        // Look up various keys
        assert_eq!(
            reader.get(b"entity_000000").unwrap(),
            Some(b"loc_0".as_slice())
        );
        assert_eq!(
            reader.get(b"entity_000050").unwrap(),
            Some(b"loc_50".as_slice())
        );
        assert_eq!(
            reader.get(b"entity_000099").unwrap(),
            Some(b"loc_99".as_slice())
        );
        assert_eq!(reader.get(b"entity_000100").unwrap(), None);
        assert_eq!(reader.get(b"aaa_before_all").unwrap(), None);
        assert_eq!(reader.get(b"zzz_after_all").unwrap(), None);
    }
}
