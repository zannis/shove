/// Errors that can occur during pub/sub operations.
#[derive(Debug, thiserror::Error)]
pub enum ShoveError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Connection-level failure (channel closed, timeout, network error).
    #[error("connection error: {0}")]
    Connection(String),

    /// Topology declaration or validation failed.
    #[error("topology error: {0}")]
    Topology(String),

    /// Input validation failed (e.g. message too large, reserved header).
    #[error("validation error: {0}")]
    Validation(String),

    /// A `Codec` failed to encode or decode a payload.
    ///
    /// `codec` is the codec's stable `NAME`. `source` is the codec-specific
    /// error (e.g. `prost::EncodeError`, `prost::DecodeError`).
    #[error("codec error in {codec}: {source}")]
    Codec {
        codec: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An error from an external SDK or backend that doesn't map to a known category.
    /// Treated as non-retryable so it surfaces immediately to the operator.
    #[error("unknown backend error: {0}")]
    Unknown(String),
}

impl ShoveError {
    /// Returns `true` for transient errors that may succeed on retry (connection
    /// failures). Non-transient errors (topology, serialization) are returned
    /// immediately so callers don't waste time retrying.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ShoveError::Connection(_))
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, ShoveError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_serialization_error() {
        let json_err = serde_json::from_str::<String>("not json").unwrap_err();
        let err = ShoveError::Serialization(json_err);
        let msg = err.to_string();
        assert!(msg.starts_with("serialization error:"), "got: {msg}");
    }

    #[test]
    fn display_connection_error() {
        let err = ShoveError::Connection("channel closed".into());
        assert_eq!(err.to_string(), "connection error: channel closed");
    }

    #[test]
    fn display_topology_error() {
        let err = ShoveError::Topology("missing exchange".into());
        assert_eq!(err.to_string(), "topology error: missing exchange");
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<String>("{}").unwrap_err();
        let err: ShoveError = json_err.into();
        assert!(matches!(err, ShoveError::Serialization(_)));
    }

    #[test]
    fn display_codec_error() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "protobuf",
            source: inner,
        };
        let msg = err.to_string();
        assert!(msg.contains("codec error in protobuf"), "got: {msg}");
    }

    #[test]
    fn codec_error_is_not_retryable() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "json",
            source: inner,
        };
        assert!(!err.is_retryable());
    }
}
