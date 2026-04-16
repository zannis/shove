use std::collections::HashMap;

use crate::error::{Result, ShoveError};
use crate::topic::Topic;

/// Header prefixes reserved for internal use. Callers may not set headers
/// that start with any of these via [`Publisher::publish_with_headers`].
const RESERVED_HEADER_PREFIXES: &[&str] = &["x-retry-count", "x-message-id", "x-death"];

/// Rejects user-supplied headers that collide with internal header keys.
pub(crate) fn validate_headers(headers: &HashMap<String, String>) -> Result<()> {
    for key in headers.keys() {
        if RESERVED_HEADER_PREFIXES.iter().any(|prefix| {
            key.len() >= prefix.len()
                && key.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        }) {
            return Err(ShoveError::Validation(format!(
                "header '{key}' uses a reserved prefix"
            )));
        }
    }
    Ok(())
}

/// Publish messages to their topic's main queue.
///
/// The destination is derived from `T::topology().queue()` — callers never
/// specify queue names directly. For sequenced topics, the publisher
/// automatically routes via `T::SEQUENCE_KEY_FN` when `topology.sequencing()`
/// is configured.
///
/// This trait is intentionally **not object-safe** — methods are generic
/// over `T: Topic`. Backends are always concrete types.
pub trait Publisher: Send + Sync + Clone + 'static {
    /// Publish a single message to the topic's queue.
    fn publish<T: Topic>(&self, message: &T::Message) -> impl Future<Output = Result<()>> + Send;

    /// Publish a single message with additional string headers/attributes.
    fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Publish a batch of messages to the topic's queue.
    ///
    /// Implementations should send all messages before awaiting confirmations.
    fn publish_batch<T: Topic>(
        &self,
        messages: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send;
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
}
