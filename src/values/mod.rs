//! Domain-specific value encodings for the three target use cases.
//!
//! The segment format stores raw `&[u8]` values. These modules provide
//! structured encode/decode for the domain types we care about:
//!
//! 1. **EntityLocation** — where an entity lives in Parquet storage (single location)
//! 2. **LocationSet** — multiple locations per key (secondary index: key → N files)
//! 3. **AdjacencyList** — compressed edge list for graph traversal
//! 4. **PathEntries** — list of entity IDs matching a file path
//!
//! All encodings use a compact binary format (varint lengths, no JSON overhead)
//! for minimal block space usage.

pub mod adjacency;
pub mod entity_location;
pub mod location_set;
pub mod path_entries;
