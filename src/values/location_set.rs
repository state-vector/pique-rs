//! LocationSet — compact encoding for multiple entity locations per key.
//!
//! Used for secondary indexes where one key maps to many (file, row_group, row_offset)
//! locations. For example, a subject_id that appears across multiple Parquet files.
//!
//! ## Binary format
//!
//! ```text
//! count:    u32 LE                        (number of locations)
//! entries:  [LocationEntry; count]        (packed, no padding)
//!
//! LocationEntry:
//!   file_key_len: u16 LE
//!   file_key:     [u8; file_key_len]
//!   row_group:    u32 LE
//!   row_offset:   u32 LE
//! ```
//!
//! This is the canonical multi-location value encoding for Pique secondary indexes.
//! Consumers should use this instead of hand-rolling join/concat logic.

use super::entity_location::{EntityLocation, ValueDecodeError};

/// A set of locations for a single key (secondary index value).
///
/// Represents all (file, row_group, row_offset) positions where a given
/// key value appears across the dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationSet {
    pub locations: Vec<EntityLocation>,
}

impl LocationSet {
    /// Create a new empty location set.
    pub fn new() -> Self {
        Self {
            locations: Vec::new(),
        }
    }

    /// Create from a single location.
    pub fn single(loc: EntityLocation) -> Self {
        Self {
            locations: vec![loc],
        }
    }

    /// Add a location to the set.
    pub fn push(&mut self, loc: EntityLocation) {
        self.locations.push(loc);
    }

    /// Number of locations.
    pub fn len(&self) -> usize {
        self.locations.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// Encode to compact binary representation.
    ///
    /// Format: `count:u32 LE` followed by `count` packed `EntityLocation` entries.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.locations.len() * 50);
        buf.extend_from_slice(&(self.locations.len() as u32).to_le_bytes());
        for loc in &self.locations {
            let file_bytes = loc.file_key.as_bytes();
            buf.extend_from_slice(&(file_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(file_bytes);
            buf.extend_from_slice(&loc.row_group.to_le_bytes());
            buf.extend_from_slice(&loc.row_offset.to_le_bytes());
        }
        buf
    }

    /// Decode from binary representation.
    pub fn decode(data: &[u8]) -> Result<Self, ValueDecodeError> {
        if data.len() < 4 {
            return Err(ValueDecodeError::TooShort);
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut locations = Vec::with_capacity(count);

        for _ in 0..count {
            if offset + 2 > data.len() {
                return Err(ValueDecodeError::TooShort);
            }
            let file_key_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if offset + file_key_len + 8 > data.len() {
                return Err(ValueDecodeError::TooShort);
            }
            let file_key = std::str::from_utf8(&data[offset..offset + file_key_len])
                .map_err(|_| ValueDecodeError::InvalidUtf8)?
                .to_string();
            offset += file_key_len;

            let row_group =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let row_offset =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            locations.push(EntityLocation {
                file_key,
                row_group,
                row_offset,
            });
        }

        Ok(Self { locations })
    }
}

impl Default for LocationSet {
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

    #[test]
    fn round_trip_single() {
        let set = LocationSet::single(EntityLocation {
            file_key: "data/part-001.parquet".into(),
            row_group: 3,
            row_offset: 42,
        });

        let encoded = set.encode();
        let decoded = LocationSet::decode(&encoded).unwrap();
        assert_eq!(set, decoded);
    }

    #[test]
    fn round_trip_multiple() {
        let set = LocationSet {
            locations: vec![
                EntityLocation {
                    file_key: "data/2024-01/part-000.parquet".into(),
                    row_group: 0,
                    row_offset: 100,
                },
                EntityLocation {
                    file_key: "data/2024-01/part-001.parquet".into(),
                    row_group: 2,
                    row_offset: 55,
                },
                EntityLocation {
                    file_key: "data/2024-02/part-000.parquet".into(),
                    row_group: 1,
                    row_offset: 0,
                },
            ],
        };

        let encoded = set.encode();
        let decoded = LocationSet::decode(&encoded).unwrap();
        assert_eq!(set, decoded);
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn empty_set() {
        let set = LocationSet::new();
        assert!(set.is_empty());

        let encoded = set.encode();
        assert_eq!(encoded.len(), 4); // just the count
        let decoded = LocationSet::decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn rejects_truncated() {
        assert!(LocationSet::decode(&[0x02, 0x00, 0x00, 0x00, 0x05]).is_err());
    }

    #[test]
    fn compact_size() {
        // 3 locations with ~30-byte file keys
        let set = LocationSet {
            locations: (0..3)
                .map(|i| EntityLocation {
                    file_key: format!("data/part-{:03}.parquet", i),
                    row_group: i,
                    row_offset: i * 100,
                })
                .collect(),
        };

        let encoded = set.encode();
        // 4 (count) + 3 * (2 + 22 + 4 + 4) = 4 + 96 = 100 bytes
        let per_entry = 2 + "data/part-000.parquet".len() + 8;
        let expected = 4 + 3 * per_entry;
        assert_eq!(encoded.len(), expected);
    }
}
