//! PathEntries — list of entity IDs under a file path.
//!
//! Used for the path prefix index:
//!   file_path → [entity_id, entity_id, ...]
//!
//! ## Binary format
//!
//! ```text
//! num_entries: u16 LE
//! [entries...]
//! ```
//!
//! Each entry:
//! ```text
//! entity_id_len: u16 LE
//! entity_id:     [u8; entity_id_len]
//! entity_kind:   u8 (dictionary-coded: 0=function, 1=class, 2=method, ...)
//! ```

/// List of entities at a given file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntries {
    pub entries: Vec<PathEntry>,
}

/// A single entity at a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    pub entity_id: String,
    pub kind: EntityKind,
}

/// Entity kinds — dictionary-coded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntityKind {
    Function = 0,
    Class = 1,
    Method = 2,
    Interface = 3,
    Module = 4,
    File = 5,
    Struct = 6,
    Enum = 7,
    Trait = 8,
    Constant = 9,
    Variable = 10,
    Other = 255,
}

impl EntityKind {
    fn from_byte(b: u8) -> Self {
        match b {
            0 => EntityKind::Function,
            1 => EntityKind::Class,
            2 => EntityKind::Method,
            3 => EntityKind::Interface,
            4 => EntityKind::Module,
            5 => EntityKind::File,
            6 => EntityKind::Struct,
            7 => EntityKind::Enum,
            8 => EntityKind::Trait,
            9 => EntityKind::Constant,
            10 => EntityKind::Variable,
            _ => EntityKind::Other,
        }
    }
}

impl PathEntries {
    /// Encode to compact binary.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.entries.len() * 50);
        buf.extend_from_slice(&(self.entries.len() as u16).to_le_bytes());

        for entry in &self.entries {
            let id_bytes = entry.entity_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(id_bytes);
            buf.push(entry.kind as u8);
        }

        buf
    }

    /// Decode from binary.
    pub fn decode(data: &[u8]) -> Result<Self, super::entity_location::ValueDecodeError> {
        use super::entity_location::ValueDecodeError;

        if data.len() < 2 {
            return Err(ValueDecodeError::TooShort);
        }

        let num_entries = u16::from_le_bytes([data[0], data[1]]) as usize;
        let mut pos = 2;
        let mut entries = Vec::with_capacity(num_entries);

        for _ in 0..num_entries {
            if pos + 2 > data.len() {
                return Err(ValueDecodeError::TooShort);
            }
            let id_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + id_len + 1 > data.len() {
                return Err(ValueDecodeError::TooShort);
            }
            let entity_id = std::str::from_utf8(&data[pos..pos + id_len])
                .map_err(|_| ValueDecodeError::InvalidUtf8)?
                .to_string();
            pos += id_len;

            let kind = EntityKind::from_byte(data[pos]);
            pos += 1;

            entries.push(PathEntry { entity_id, kind });
        }

        Ok(PathEntries { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let entries = PathEntries {
            entries: vec![
                PathEntry {
                    entity_id: "repo::src/api/auth/handler.rs::function::authenticate".into(),
                    kind: EntityKind::Function,
                },
                PathEntry {
                    entity_id: "repo::src/api/auth/handler.rs::class::AuthHandler".into(),
                    kind: EntityKind::Class,
                },
                PathEntry {
                    entity_id: "repo::src/api/auth/handler.rs::method::validate_token".into(),
                    kind: EntityKind::Method,
                },
            ],
        };

        let encoded = entries.encode();
        let decoded = PathEntries::decode(&encoded).unwrap();
        assert_eq!(entries, decoded);
    }

    #[test]
    fn empty() {
        let entries = PathEntries { entries: vec![] };
        let encoded = entries.encode();
        assert_eq!(encoded.len(), 2);
        let decoded = PathEntries::decode(&encoded).unwrap();
        assert_eq!(entries, decoded);
    }
}
