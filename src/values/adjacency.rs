//! AdjacencyList — compressed edge list for graph traversal.
//!
//! Used for the edge adjacency index:
//!   entity_id → (outgoing_edges, incoming_edges)
//!
//! ## Binary format
//!
//! ```text
//! num_outgoing: u16 LE
//! num_incoming: u16 LE
//! [outgoing edges...]
//! [incoming edges...]
//! ```
//!
//! Each edge:
//! ```text
//! target_id_len: u16 LE
//! target_id:     [u8; target_id_len]
//! rel_kind:      u8 (dictionary-coded)
//! ```
//!
//! ## Relation kind dictionary
//!
//! Common relation kinds are encoded as a single byte to save space:
//! 0 = calls, 1 = imports, 2 = extends, 3 = implements,
//! 4 = contains, 5 = uses, 6 = type_of, 255 = custom (followed by string)

/// Compressed adjacency list for an entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyList {
    /// Outgoing edges (this entity → target).
    pub outgoing: Vec<Edge>,
    /// Incoming edges (source → this entity).
    pub incoming: Vec<Edge>,
}

/// A single edge in the adjacency list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// The entity at the other end of the edge.
    pub entity_id: String,
    /// The kind of relationship.
    pub rel_kind: RelKind,
}

/// Relation kinds — dictionary-coded for compression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelKind {
    Calls,
    Imports,
    Extends,
    Implements,
    Contains,
    Uses,
    TypeOf,
    /// Custom relation kind (stored as string).
    Custom(String),
}

impl RelKind {
    fn to_byte(&self) -> u8 {
        match self {
            RelKind::Calls => 0,
            RelKind::Imports => 1,
            RelKind::Extends => 2,
            RelKind::Implements => 3,
            RelKind::Contains => 4,
            RelKind::Uses => 5,
            RelKind::TypeOf => 6,
            RelKind::Custom(_) => 255,
        }
    }

    fn from_byte(byte: u8, data: &[u8], pos: &mut usize) -> Result<Self, super::entity_location::ValueDecodeError> {
        use super::entity_location::ValueDecodeError;
        match byte {
            0 => Ok(RelKind::Calls),
            1 => Ok(RelKind::Imports),
            2 => Ok(RelKind::Extends),
            3 => Ok(RelKind::Implements),
            4 => Ok(RelKind::Contains),
            5 => Ok(RelKind::Uses),
            6 => Ok(RelKind::TypeOf),
            255 => {
                // Read custom string
                if *pos + 2 > data.len() {
                    return Err(ValueDecodeError::TooShort);
                }
                let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
                *pos += 2;
                if *pos + len > data.len() {
                    return Err(ValueDecodeError::TooShort);
                }
                let s = std::str::from_utf8(&data[*pos..*pos + len])
                    .map_err(|_| ValueDecodeError::InvalidUtf8)?
                    .to_string();
                *pos += len;
                Ok(RelKind::Custom(s))
            }
            _ => Err(ValueDecodeError::InvalidFormat),
        }
    }
}

impl AdjacencyList {
    /// Encode to compact binary representation.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.outgoing.len() * 40 + self.incoming.len() * 40);

        buf.extend_from_slice(&(self.outgoing.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(self.incoming.len() as u16).to_le_bytes());

        for edge in &self.outgoing {
            encode_edge(&mut buf, edge);
        }
        for edge in &self.incoming {
            encode_edge(&mut buf, edge);
        }

        buf
    }

    /// Decode from binary representation.
    pub fn decode(data: &[u8]) -> Result<Self, super::entity_location::ValueDecodeError> {
        use super::entity_location::ValueDecodeError;

        if data.len() < 4 {
            return Err(ValueDecodeError::TooShort);
        }

        let num_outgoing = u16::from_le_bytes([data[0], data[1]]) as usize;
        let num_incoming = u16::from_le_bytes([data[2], data[3]]) as usize;

        let mut pos = 4;
        let mut outgoing = Vec::with_capacity(num_outgoing);
        for _ in 0..num_outgoing {
            outgoing.push(decode_edge(data, &mut pos)?);
        }

        let mut incoming = Vec::with_capacity(num_incoming);
        for _ in 0..num_incoming {
            incoming.push(decode_edge(data, &mut pos)?);
        }

        Ok(AdjacencyList { outgoing, incoming })
    }

    /// Total number of edges.
    pub fn edge_count(&self) -> usize {
        self.outgoing.len() + self.incoming.len()
    }
}

fn encode_edge(buf: &mut Vec<u8>, edge: &Edge) {
    let id_bytes = edge.entity_id.as_bytes();
    buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(id_bytes);
    buf.push(edge.rel_kind.to_byte());

    // If custom, also write the string
    if let RelKind::Custom(ref s) = edge.rel_kind {
        let s_bytes = s.as_bytes();
        buf.extend_from_slice(&(s_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(s_bytes);
    }
}

fn decode_edge(data: &[u8], pos: &mut usize) -> Result<Edge, super::entity_location::ValueDecodeError> {
    use super::entity_location::ValueDecodeError;

    if *pos + 2 > data.len() {
        return Err(ValueDecodeError::TooShort);
    }

    let id_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;

    if *pos + id_len + 1 > data.len() {
        return Err(ValueDecodeError::TooShort);
    }

    let entity_id = std::str::from_utf8(&data[*pos..*pos + id_len])
        .map_err(|_| ValueDecodeError::InvalidUtf8)?
        .to_string();
    *pos += id_len;

    let kind_byte = data[*pos];
    *pos += 1;

    let rel_kind = RelKind::from_byte(kind_byte, data, pos)?;

    Ok(Edge { entity_id, rel_kind })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple() {
        let adj = AdjacencyList {
            outgoing: vec![
                Edge { entity_id: "pkg::mod_a::fn_foo".into(), rel_kind: RelKind::Calls },
                Edge { entity_id: "pkg::mod_b::struct_Bar".into(), rel_kind: RelKind::Uses },
            ],
            incoming: vec![
                Edge { entity_id: "pkg::mod_c::fn_main".into(), rel_kind: RelKind::Calls },
            ],
        };

        let encoded = adj.encode();
        let decoded = AdjacencyList::decode(&encoded).unwrap();
        assert_eq!(adj, decoded);
    }

    #[test]
    fn round_trip_custom_kind() {
        let adj = AdjacencyList {
            outgoing: vec![
                Edge {
                    entity_id: "some::entity".into(),
                    rel_kind: RelKind::Custom("delegates_to".into()),
                },
            ],
            incoming: vec![],
        };

        let encoded = adj.encode();
        let decoded = AdjacencyList::decode(&encoded).unwrap();
        assert_eq!(adj, decoded);
    }

    #[test]
    fn empty_adjacency() {
        let adj = AdjacencyList {
            outgoing: vec![],
            incoming: vec![],
        };

        let encoded = adj.encode();
        assert_eq!(encoded.len(), 4); // Just the two u16 counts
        let decoded = AdjacencyList::decode(&encoded).unwrap();
        assert_eq!(adj, decoded);
    }

    #[test]
    fn large_adjacency_encoding_size() {
        // Simulate a heavily-connected entity (200 outgoing edges)
        let outgoing: Vec<Edge> = (0..200)
            .map(|i| Edge {
                entity_id: format!("acme/myproject::src/query/src/translation.rs::function::helper_{:04}", i),
                rel_kind: RelKind::Calls,
            })
            .collect();

        let adj = AdjacencyList {
            outgoing,
            incoming: vec![],
        };

        let encoded = adj.encode();
        // Each edge: 2 (id_len) + ~100 (id) + 1 (kind) ≈ 103 bytes
        // 200 edges × 103 ≈ 20,600 bytes + 4 header
        assert!(encoded.len() < 30_000, "Encoded size: {}", encoded.len());
        assert!(encoded.len() > 5_000, "Encoded size: {}", encoded.len());

        let decoded = AdjacencyList::decode(&encoded).unwrap();
        assert_eq!(decoded.outgoing.len(), 200);
    }
}
