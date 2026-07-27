//! Bloom filter layer using XOR8 filters.
//!
//! XOR filters provide better space efficiency than classic Bloom filters
//! (fewer bits per key for the same false positive rate) and have zero
//! false-negative rate. They're ideal for immutable indexes because they
//! require all keys upfront at build time (no incremental insertion).
//!
//! At 8 bits per key, the false positive rate is ~0.39%.
//! At 16 bits per key, the false positive rate is ~0.0015%.

use xorf::{Filter as XorFilter, Xor8};

/// Build a bloom filter from a set of keys.
///
/// Uses XOR8 filter (8 bits per entry, ~0.39% FP rate).
/// Returns the serialized filter bytes.
pub fn build_filter(keys: &[&[u8]]) -> Vec<u8> {
    if keys.is_empty() {
        return Vec::new();
    }

    // Hash keys to u64 using xxh3
    let hashes: Vec<u64> = keys.iter().map(|k| xxhash_rust::xxh3::xxh3_64(k)).collect();

    // Build the XOR8 filter
    let filter = Xor8::from(&hashes);

    // Serialize: fingerprints + metadata
    serialize_xor8(&filter)
}

/// Check if a key might be in the filter.
///
/// Returns:
/// - `true` — key might be present (check the data blocks)
/// - `false` — key is definitely NOT present (skip the segment)
pub fn might_contain(filter_bytes: &[u8], key: &[u8]) -> bool {
    if filter_bytes.is_empty() {
        return true; // No filter → always check
    }

    let filter = match deserialize_xor8(filter_bytes) {
        Some(f) => f,
        None => return true, // Corrupted filter → be conservative
    };

    let hash = xxhash_rust::xxh3::xxh3_64(key);
    filter.contains(&hash)
}

// ---------------------------------------------------------------------------
// Serialization — compact binary format for the XOR8 filter
// ---------------------------------------------------------------------------

/// Serialize an Xor8 filter to bytes.
///
/// Format:
/// ```text
/// seed: u64 (LE)
/// block_length: u64 (LE) — length of each of the 3 blocks
/// fingerprints: [u8; block_length * 3]
/// ```
fn serialize_xor8(filter: &Xor8) -> Vec<u8> {
    let block_length = filter.fingerprints.len() / 3;
    let mut buf = Vec::with_capacity(16 + filter.fingerprints.len());

    buf.extend_from_slice(&filter.seed.to_le_bytes());
    buf.extend_from_slice(&(block_length as u64).to_le_bytes());
    buf.extend_from_slice(&filter.fingerprints);

    buf
}

/// Deserialize an Xor8 filter from bytes.
fn deserialize_xor8(data: &[u8]) -> Option<Xor8> {
    if data.len() < 16 {
        return None;
    }

    let seed = u64::from_le_bytes(data[0..8].try_into().ok()?);
    let block_length = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;

    let expected_len = 16 + block_length * 3;
    if data.len() < expected_len {
        return None;
    }

    let fingerprints = data[16..16 + block_length * 3].to_vec();

    Some(Xor8 {
        seed,
        block_length,
        fingerprints: fingerprints.into_boxed_slice(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_query_filter() {
        let keys: Vec<&[u8]> = vec![
            b"entity_001",
            b"entity_002",
            b"entity_003",
            b"entity_100",
            b"some/path/file.rs",
        ];

        let filter_bytes = build_filter(&keys);
        assert!(!filter_bytes.is_empty());

        // All inserted keys should be found
        for key in &keys {
            assert!(
                might_contain(&filter_bytes, key),
                "Key {:?} should be found",
                key
            );
        }

        // Non-existent keys should mostly NOT be found (probabilistic)
        let mut false_positives = 0;
        for i in 0..1000 {
            let fake_key = format!("nonexistent_key_{}", i);
            if might_contain(&filter_bytes, fake_key.as_bytes()) {
                false_positives += 1;
            }
        }
        // XOR8 has ~0.39% FP rate. With 1000 queries, expect ~4 false positives.
        // Allow up to 20 to account for variance.
        assert!(
            false_positives < 20,
            "Too many false positives: {} out of 1000",
            false_positives
        );
    }

    #[test]
    fn empty_filter_always_returns_true() {
        let filter_bytes = build_filter(&[]);
        assert!(might_contain(&filter_bytes, b"anything"));
    }

    #[test]
    fn large_key_set() {
        let keys: Vec<Vec<u8>> = (0..10_000)
            .map(|i| format!("acme/myproject::src/query/src/entity_{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let filter_bytes = build_filter(&key_refs);

        // Size should be roughly 10000 * 1.23 bytes (XOR8 overhead)
        // Plus 16 bytes metadata
        assert!(filter_bytes.len() < 15_000, "Filter too large: {}", filter_bytes.len());
        assert!(filter_bytes.len() > 10_000, "Filter suspiciously small: {}", filter_bytes.len());

        // Spot-check membership
        assert!(might_contain(&filter_bytes, keys[0].as_slice()));
        assert!(might_contain(&filter_bytes, keys[5000].as_slice()));
        assert!(might_contain(&filter_bytes, keys[9999].as_slice()));
    }
}
