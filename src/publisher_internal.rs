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
