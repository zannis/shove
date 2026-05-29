//! Structured errors for the schema registry client.

/// Errors from resolving a schema against the registry.
#[derive(Debug, thiserror::Error)]
pub enum SchemaRegistryError {
    /// The schema id is unknown to the registry (HTTP 404 / 40403).
    #[error("schema id {0} not found in registry")]
    NotFound(u32),

    /// The resolved schema's subject is not in the consumer's accepted set.
    #[error("schema subject `{got}` not accepted (expected one of {accepted:?})")]
    Incompatible { got: String, accepted: Vec<String> },

    /// A transport/HTTP failure. `retriable` is true for timeouts and 5xx.
    #[error("registry transport error (retriable={retriable}): {message}")]
    Transport { retriable: bool, message: String },

    /// The registry response could not be parsed.
    #[error("failed to decode registry response: {0}")]
    Decode(String),
}

impl SchemaRegistryError {
    /// Whether retrying the registry call might succeed.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            SchemaRegistryError::Transport {
                retriable: true,
                ..
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_5xx_is_retriable() {
        let e = SchemaRegistryError::Transport {
            retriable: true,
            message: "503".into(),
        };
        assert!(e.is_retriable());
    }

    #[test]
    fn not_found_is_not_retriable() {
        assert!(!SchemaRegistryError::NotFound(7).is_retriable());
    }
}
