//! Segment format definitions — header, footer, and metadata types.
//!
//! The segment is a single contiguous byte sequence (stored as one S3 object):
//!
//! ```text
//! [data_blocks...] [bloom_filter] [fst_directory] [footer: 64 bytes]
//! ```
//!
//! The footer is always the last 64 bytes. Readers start by reading the footer
//! to learn where each section begins.

use serde::{Deserialize, Serialize};

/// Magic bytes identifying an s3-index segment file.
/// ASCII "S3IX" followed by version-specific padding.
pub const MAGIC: [u8; 4] = [0x53, 0x33, 0x49, 0x58]; // "S3IX"

/// Current format version. Increment on breaking format changes.
pub const FORMAT_VERSION: u16 = 1;

/// Fixed footer size in bytes.
pub const FOOTER_SIZE: usize = 64;

/// Default data block size (64 KB) — optimised for S3 range read overhead.
pub const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024;

/// Number of entries between restart points within a block.
/// Restart points store full keys for binary search entry points.
pub const DEFAULT_RESTART_INTERVAL: u32 = 16;

/// The footer sits at the end of the segment and points to all sections.
///
/// Layout (64 bytes, little-endian):
/// ```text
/// [0..4]    magic: "S3IX"
/// [4..6]    format_version: u16
/// [6..8]    reserved: u16
/// [8..16]   data_blocks_offset: u64     (always 0 — data starts at byte 0)
/// [16..24]  data_blocks_length: u64
/// [24..32]  bloom_offset: u64
/// [32..40]  bloom_length: u64
/// [40..48]  fst_offset: u64
/// [48..56]  fst_length: u64
/// [56..60]  key_count: u32
/// [60..64]  checksum: u32 (CRC32 of bytes [0..60])
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub format_version: u16,
    pub data_blocks_offset: u64,
    pub data_blocks_length: u64,
    pub bloom_offset: u64,
    pub bloom_length: u64,
    pub fst_offset: u64,
    pub fst_length: u64,
    pub key_count: u32,
    pub checksum: u32,
}

impl Footer {
    /// Serialize the footer to exactly 64 bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];

        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        // [6..8] reserved
        buf[8..16].copy_from_slice(&self.data_blocks_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.data_blocks_length.to_le_bytes());
        buf[24..32].copy_from_slice(&self.bloom_offset.to_le_bytes());
        buf[32..40].copy_from_slice(&self.bloom_length.to_le_bytes());
        buf[40..48].copy_from_slice(&self.fst_offset.to_le_bytes());
        buf[48..56].copy_from_slice(&self.fst_length.to_le_bytes());
        buf[56..60].copy_from_slice(&self.key_count.to_le_bytes());

        // Checksum covers bytes [0..60]
        let crc = crc32fast::hash(&buf[0..60]);
        buf[60..64].copy_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a footer from exactly 64 bytes.
    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> Result<Self, FormatError> {
        // Verify magic
        if buf[0..4] != MAGIC {
            return Err(FormatError::InvalidMagic);
        }

        let format_version = u16::from_le_bytes([buf[4], buf[5]]);
        if format_version != FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(format_version));
        }

        // Verify checksum
        let stored_crc = u32::from_le_bytes([buf[60], buf[61], buf[62], buf[63]]);
        let computed_crc = crc32fast::hash(&buf[0..60]);
        if stored_crc != computed_crc {
            return Err(FormatError::ChecksumMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        Ok(Footer {
            format_version,
            data_blocks_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            data_blocks_length: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            bloom_offset: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            bloom_length: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            fst_offset: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            fst_length: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
            key_count: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
            checksum: stored_crc,
        })
    }
}

/// Segment metadata (stored separately, e.g., in a manifest file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Monotonically increasing generation number.
    pub generation: u64,
    /// S3 key (or file path) of the segment.
    pub path: String,
    /// Total segment size in bytes.
    pub size_bytes: u64,
    /// Number of keys in the segment.
    pub key_count: u32,
    /// Smallest key in the segment (for segment-level routing).
    pub min_key: Vec<u8>,
    /// Largest key in the segment (for segment-level routing).
    pub max_key: Vec<u8>,
}

/// Errors during format parsing.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("Invalid magic bytes — not an s3-index segment")]
    InvalidMagic,

    #[error("Unsupported format version: {0} (expected {FORMAT_VERSION})")]
    UnsupportedVersion(u16),

    #[error("Footer checksum mismatch: stored={stored:#x}, computed={computed:#x}")]
    ChecksumMismatch { stored: u32, computed: u32 },

    #[error("Segment too small: {size} bytes (minimum is {FOOTER_SIZE})")]
    TooSmall { size: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_round_trip() {
        let footer = Footer {
            format_version: FORMAT_VERSION,
            data_blocks_offset: 0,
            data_blocks_length: 1024 * 1024,
            bloom_offset: 1024 * 1024,
            bloom_length: 4096,
            fst_offset: 1024 * 1024 + 4096,
            fst_length: 32768,
            key_count: 50000,
            checksum: 0, // Will be computed by to_bytes
        };

        let bytes = footer.to_bytes();
        let decoded = Footer::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.format_version, FORMAT_VERSION);
        assert_eq!(decoded.data_blocks_length, 1024 * 1024);
        assert_eq!(decoded.bloom_offset, 1024 * 1024);
        assert_eq!(decoded.bloom_length, 4096);
        assert_eq!(decoded.fst_offset, 1024 * 1024 + 4096);
        assert_eq!(decoded.fst_length, 32768);
        assert_eq!(decoded.key_count, 50000);
    }

    #[test]
    fn footer_detects_bad_magic() {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..4].copy_from_slice(b"NOPE");
        let result = Footer::from_bytes(&bytes);
        assert!(matches!(result, Err(FormatError::InvalidMagic)));
    }

    #[test]
    fn footer_detects_checksum_corruption() {
        let footer = Footer {
            format_version: FORMAT_VERSION,
            data_blocks_offset: 0,
            data_blocks_length: 100,
            bloom_offset: 100,
            bloom_length: 50,
            fst_offset: 150,
            fst_length: 200,
            key_count: 10,
            checksum: 0,
        };

        let mut bytes = footer.to_bytes();
        // Corrupt one byte in the data region
        bytes[10] ^= 0xFF;

        let result = Footer::from_bytes(&bytes);
        assert!(matches!(result, Err(FormatError::ChecksumMismatch { .. })));
    }
}
