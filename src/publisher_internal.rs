use std::collections::HashMap;

use crate::error::{Result, ShoveError};

/// Header prefixes reserved for internal use. Callers may not set headers
/// that start with any of these via `Publisher::publish_with_headers`.
#[allow(dead_code)] // Used by feature-gated backend publishers.
const RESERVED_HEADER_PREFIXES: &[&str] =
    &["x-retry-count", "x-message-id", "x-death", "x-sequence-key"];

/// Rejects user-supplied headers that collide with internal header keys.
#[allow(dead_code)] // Used by feature-gated backend publishers.
pub(crate) fn validate_headers(headers: &HashMap<String, String>) -> Result<()> {
    for key in headers.keys() {
        let key_lower = key.to_ascii_lowercase();
        if RESERVED_HEADER_PREFIXES
            .iter()
            .any(|prefix| key_lower.starts_with(*prefix))
        {
            return Err(ShoveError::Validation(format!(
                "header '{key}' uses a reserved prefix"
            )));
        }
    }
    Ok(())
}

/// FNV-1a 64-bit hash. Stable across versions — used for sequence-key shard
/// routing and content-dedup IDs. The single canonical implementation shared by
/// every backend's publisher so a given key routes identically everywhere.
#[allow(dead_code)] // Used by feature-gated backend publishers.
pub(crate) fn fnv1a_64(data: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Map a sequence key to a shard index in `0..routing_shards` via [`fnv1a_64`].
/// Shared by all backends so a key lands on the same shard regardless of
/// backend.
#[allow(dead_code)] // Used by feature-gated backend publishers.
pub(crate) fn shard_for_key(key: &str, routing_shards: u16) -> u16 {
    assert!(routing_shards > 0, "routing_shards must be > 0");
    (fnv1a_64(key.as_bytes()) % u64::from(routing_shards)) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_safe_headers() {
        let mut h = HashMap::new();
        h.insert("x-trace-id".into(), "abc".into());
        h.insert("x-custom".into(), "value".into());
        assert!(validate_headers(&h).is_ok());
    }

    #[test]
    fn rejects_retry_count_header() {
        let mut h = HashMap::new();
        h.insert("x-retry-count".into(), "5".into());
        assert!(validate_headers(&h).is_err());
    }

    #[test]
    fn rejects_message_id_header() {
        let mut h = HashMap::new();
        h.insert("X-Message-Id".into(), "fake".into());
        assert!(validate_headers(&h).is_err());
    }

    #[test]
    fn rejects_death_header() {
        let mut h = HashMap::new();
        h.insert("x-death-reason".into(), "evil".into());
        assert!(validate_headers(&h).is_err());
    }

    #[test]
    fn empty_headers_ok() {
        assert!(validate_headers(&HashMap::new()).is_ok());
    }

    #[test]
    fn fnv1a_64_is_deterministic_and_distinguishes_inputs() {
        assert_eq!(fnv1a_64(b"hello"), fnv1a_64(b"hello"));
        assert_ne!(fnv1a_64(b"hello"), fnv1a_64(b"world"));
    }

    #[test]
    fn shard_for_key_is_bounded_and_stable() {
        assert_eq!(shard_for_key("acct-1", 8), shard_for_key("acct-1", 8));
        for i in 0..200u32 {
            assert!(shard_for_key(&format!("acct-{i}"), 8) < 8);
        }
    }

    #[test]
    fn shard_for_key_single_shard_always_zero() {
        for key in ["", "a", "hello-world", "acct-9999"] {
            assert_eq!(shard_for_key(key, 1), 0);
        }
    }

    #[test]
    fn shard_for_key_distributes_across_shards() {
        let shards = 8u16;
        let mut buckets = vec![0u32; shards as usize];
        for i in 0..1000u32 {
            buckets[shard_for_key(&format!("account-{i}"), shards) as usize] += 1;
        }
        assert!(
            buckets.iter().filter(|&&c| c > 0).count() >= 6,
            "poor distribution: {buckets:?}"
        );
    }
}
