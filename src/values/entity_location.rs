//! EntityLocation — encodes where an entity lives in Parquet storage.
//!
//! Used for the entity lookup index:
//!   entity_id → (parquet_file, row_group, row_offset)
//!
//! ## Binary format
//!
//! ```text
//! file_key_len: u16 LE
//! file_key:     [u8; file_key_len]
//! row_group:    u32 LE
//! row_offset:   u32 LE
//! ```
//!
//! Typical size: 50–150 bytes (dominated by file_key which is an S3 key).

/// Where an entity lives in Parquet storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityLocation {
    /// S3 key of the Parquet file containing this entity.
    pub file_key: String,
    /// Row group index within the Parquet file.
    pub row_group: u32,
    /// Row offset within the row group.
    pub row_offset: u32,
}

impl EntityLocation {
    /// Encode to compact binary representation.
    pub fn encode(&self) -> Vec<u8> {
        let file_bytes = self.file_key.as_bytes();
        let mut buf = Vec::with_capacity(2 + file_bytes.len() + 8);

        buf.extend_from_slice(&(file_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(file_bytes);
        buf.extend_from_slice(&self.row_group.to_le_bytes());
        buf.extend_from_slice(&self.row_offset.to_le_bytes());

        buf
    }

    /// Decode from binary representation.
    pub fn decode(data: &[u8]) -> Result<Self, ValueDecodeError> {
        if data.len() < 2 {
            return Err(ValueDecodeError::TooShort);
        }

        let file_key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let expected_len = 2 + file_key_len + 8;

        if data.len() < expected_len {
            return Err(ValueDecodeError::TooShort);
        }

        let file_key = std::str::from_utf8(&data[2..2 + file_key_len])
            .map_err(|_| ValueDecodeError::InvalidUtf8)?
            .to_string();

        let rg_offset = 2 + file_key_len;
        let row_group = u32::from_le_bytes(data[rg_offset..rg_offset + 4].try_into().unwrap());
        let row_offset = u32::from_le_bytes(data[rg_offset + 4..rg_offset + 8].try_into().unwrap());

        Ok(EntityLocation {
            file_key,
            row_group,
            row_offset,
        })
    }
}

/// Errors when decoding a value.
#[derive(Debug, thiserror::Error)]
pub enum ValueDecodeError {
    #[error("Data too short for value type")]
    TooShort,

    #[error("Invalid UTF-8 in string field")]
    InvalidUtf8,

    #[error("Invalid format")]
    InvalidFormat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let loc = EntityLocation {
            file_key: "data/org123/entities/repo=owner%2Frepo/ref=default/kind=function/part-00042.parquet".to_string(),
            row_group: 7,
            row_offset: 142,
        };

        let encoded = loc.encode();
        let decoded = EntityLocation::decode(&encoded).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn compact_size() {
        let loc = EntityLocation {
            file_key: "data/org/entities/part-00001.parquet".to_string(),
            row_group: 0,
            row_offset: 0,
        };

        let encoded = loc.encode();
        // 2 (len) + 39 (key) + 4 (rg) + 4 (offset) = 49 bytes
        assert_eq!(encoded.len(), 2 + loc.file_key.len() + 8);
    }

    #[test]
    fn rejects_truncated() {
        let result = EntityLocation::decode(&[0x05, 0x00, b'h', b'e']);
        assert!(matches!(result, Err(ValueDecodeError::TooShort)));
    }
}
