use std::collections::HashMap;

use crate::error::{Result, ShoveError};

/// How a reserved header name is matched, ASCII case-insensitively.
#[allow(dead_code)] // Used by feature-gated backend publishers.
enum Reserved {
    /// Every name that starts with this string.
    Prefix(&'static str),
    /// This name and no other, so a name that only resembles it stays free.
    Exact(&'static str),
}

/// Header names callers may not set via `Publisher::publish_with_headers`: the
/// names the backends stamp themselves or read back as their own. Every backend
/// checks this one list, and a test in each backend checks that every name it
/// writes is covered, so a new internal name is added here.
#[allow(dead_code)] // Used by feature-gated backend publishers.
const RESERVED_HEADERS: &[Reserved] = &[
    Reserved::Prefix("shove-"),
    Reserved::Prefix("x-retry-count"),
    Reserved::Prefix("x-message-id"),
    Reserved::Prefix("x-death"),
    Reserved::Prefix("x-sequence-key"),
    Reserved::Exact("x-original-queue"),
    Reserved::Exact("payload"),
    Reserved::Exact("shard"),
];

/// Rejects user-supplied headers that collide with internal header keys.
#[allow(dead_code)] // Used by feature-gated backend publishers.
pub(crate) fn validate_headers(headers: &HashMap<String, String>) -> Result<()> {
    for key in headers.keys() {
        let key_lower = key.to_ascii_lowercase();
        for reserved in RESERVED_HEADERS {
            let reason = match reserved {
                Reserved::Prefix(prefix) if key_lower.starts_with(*prefix) => {
                    "uses a reserved prefix"
                }
                Reserved::Exact(name) if key_lower == *name => "is a reserved name",
                _ => continue,
            };
            return Err(ShoveError::Validation(format!("header '{key}' {reason}")));
        }
    }
    Ok(())
}

/// Panics unless [`validate_headers`] rejects every name in `keys`. Each
/// backend's tests call it with the names that backend writes itself.
#[cfg(test)]
#[allow(dead_code)] // Used by feature-gated backend tests.
pub(crate) fn assert_internal_keys_reserved(backend: &str, keys: &[&str]) {
    assert!(!keys.is_empty(), "{backend} lists no internal names");
    for key in keys {
        let headers = HashMap::from([((*key).to_string(), "1".to_string())]);
        assert!(
            validate_headers(&headers).is_err(),
            "{backend} writes '{key}' itself but validate_headers lets a caller set it"
        );
    }
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
    fn shove_prefixed_headers_are_rejected() {
        for name in [
            "Shove-Retry-Count",
            "Shove-Message-Id",
            "Shove-Base-Message-Id",
            "Shove-Death-Reason",
            "Shove-Death-Count",
            "Shove-Original-Queue",
            "Shove-Sequence-Key",
            "SHOVE-retry-count",
            "shove-anything",
        ] {
            let headers = HashMap::from([(name.to_string(), "1".to_string())]);
            match validate_headers(&headers) {
                Err(ShoveError::Validation(message)) => {
                    assert!(message.contains(name), "{message:?} does not name {name:?}");
                }
                other => panic!("{name:?} was not rejected: {other:?}"),
            }
        }
    }

    #[test]
    fn names_that_only_resemble_the_shove_prefix_are_accepted() {
        for name in [
            "shove",
            "shove_retry_count",
            "shoves-retry",
            "x-shove-trace",
        ] {
            let headers = HashMap::from([(name.to_string(), "1".to_string())]);
            assert!(validate_headers(&headers).is_ok(), "{name:?} was rejected");
        }
    }

    #[test]
    fn reserved_exact_names_are_rejected() {
        for name in [
            "payload",
            "Payload",
            "PAYLOAD",
            "x-original-queue",
            "X-Original-Queue",
            "shard",
            "Shard",
        ] {
            let headers = HashMap::from([(name.to_string(), "1".to_string())]);
            match validate_headers(&headers) {
                Err(ShoveError::Validation(message)) => {
                    assert!(message.contains(name), "{message:?} does not name {name:?}");
                }
                other => panic!("{name:?} was not rejected: {other:?}"),
            }
        }
    }

    #[test]
    fn names_that_only_resemble_a_reserved_exact_name_are_accepted() {
        for name in [
            "payload-type",
            "payloads",
            "x-payload",
            "shard-key",
            "shards",
            "x-original-queues",
        ] {
            let headers = HashMap::from([(name.to_string(), "1".to_string())]);
            assert!(validate_headers(&headers).is_ok(), "{name:?} was rejected");
        }
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
