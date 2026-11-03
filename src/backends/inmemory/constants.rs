pub const DEFAULT_QUEUE_CAPACITY: usize = 10_000;

pub(super) const X_RETRY_COUNT: &str = "x-retry-count";
pub(super) const X_MESSAGE_ID: &str = "x-message-id";
pub(super) const X_SEQUENCE_KEY: &str = "x-sequence-key";
pub(super) const X_DEATH_REASON: &str = "x-death-reason";
pub(super) const X_ORIGINAL_QUEUE: &str = "x-original-queue";
pub(super) const X_DEATH_COUNT: &str = "x-death-count";

/// Header names this backend writes itself. A new one belongs here: a test
/// checks that `validate_headers` reserves every name in this list.
#[cfg(test)]
const INTERNAL_KEYS: &[&str] = &[
    X_RETRY_COUNT,
    X_MESSAGE_ID,
    X_SEQUENCE_KEY,
    X_DEATH_REASON,
    X_ORIGINAL_QUEUE,
    X_DEATH_COUNT,
];

#[cfg(test)]
mod tests {
    use super::INTERNAL_KEYS;
    use crate::publisher_internal::assert_internal_keys_reserved;

    #[test]
    fn every_internal_key_is_reserved_by_validate_headers() {
        assert_internal_keys_reserved("InMemory", INTERNAL_KEYS, &[]);
    }
}
