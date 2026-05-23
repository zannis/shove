//! Pluggable encoding of `Topic::Message` payloads.
//!
//! Every backend publisher/consumer routes through the topic's `Codec`
//! associated type instead of calling `serde_json::*` directly. The default
//! codec ([`JsonCodec`]) preserves the historical JSON-on-the-wire behavior;
//! users opt into other encodings (Protobuf, raw bytes, custom) by setting
//! `type Codec = ...` on their topic.

use crate::error::Result;

/// Encodes and decodes the byte representation of a topic message.
///
/// Implementations must be deterministic and round-trip-safe:
/// `decode(encode(m).unwrap()).unwrap() == m` for every `m` the codec
/// claims to support.
///
/// The trait is type-erased: `encode` and `decode` are associated functions,
/// not methods on a value. This matches `Topic::topology()` and lets the
/// macros inject a codec without changing publisher/consumer constructors.
pub trait Codec<M>: Send + Sync + 'static {
    /// Stable identifier used in logs and metrics. Examples: `"json"`,
    /// `"protobuf"`, `"raw"`.
    const NAME: &'static str;

    /// Encode `value` to its byte representation.
    fn encode(value: &M) -> Result<Vec<u8>>;

    /// Decode `bytes` into a value of type `M`.
    fn decode(bytes: &[u8]) -> Result<M>;
}

use serde::{Serialize, de::DeserializeOwned};

/// JSON codec — the default. Routes through `serde_json` and surfaces
/// failures via [`ShoveError::Serialization`] for back-compat with code that
/// pattern-matches on that variant.
pub struct JsonCodec;

impl<M> Codec<M> for JsonCodec
where
    M: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    const NAME: &'static str = "json";

    fn encode(value: &M) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode(bytes: &[u8]) -> Result<M> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        id: String,
        n: u64,
    }

    #[test]
    fn json_codec_round_trip() {
        let sample = Sample {
            id: "abc".into(),
            n: 42,
        };
        let bytes = <JsonCodec as Codec<Sample>>::encode(&sample).unwrap();
        let decoded = <JsonCodec as Codec<Sample>>::decode(&bytes).unwrap();
        assert_eq!(decoded, sample);
    }

    #[test]
    fn json_codec_name_is_json() {
        assert_eq!(<JsonCodec as Codec<Sample>>::NAME, "json");
    }
}
