//! Pluggable encoding of `Topic::Message` payloads.
//!
//! Every backend publisher/consumer routes through the topic's `Codec`
//! associated type instead of calling `serde_json::*` directly. The default
//! codec ([`JsonCodec`]) preserves the historical JSON-on-the-wire behavior;
//! users opt into other encodings (Protobuf, raw bytes, custom) by setting
//! `type Codec = ...` on their topic.

use crate::error::{Result, ShoveError};

use serde::{Serialize, de::DeserializeOwned};

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

    /// Encode `value` to a UTF-8 `String` for string-API backends (SNS/SQS,
    /// Redis Streams). The default implementation calls [`Self::encode`] and
    /// then validates the bytes as UTF-8; non-UTF-8 codec output surfaces as
    /// [`ShoveError::Codec`] with the codec's [`Self::NAME`].
    ///
    /// `String::from_utf8` is zero-copy (it takes ownership of the `Vec<u8>`
    /// returned by `encode` and validates in place), so JSON output incurs no
    /// extra allocation. Binary codecs (Protobuf, raw bytes containing
    /// non-UTF-8) will fail here at publish time, which is the right failure
    /// mode for those backends.
    fn encode_to_string(value: &M) -> Result<String> {
        let bytes = Self::encode(value)?;
        String::from_utf8(bytes).map_err(|e| ShoveError::Codec {
            codec: Self::NAME,
            source: Box::new(e),
        })
    }
}

/// JSON codec — the default. Routes through `serde_json` and surfaces
/// failures via [`crate::error::ShoveError::Serialization`] for back-compat
/// with code that pattern-matches on that variant.
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

/// Pass-through codec for topics whose payloads are already encoded
/// (Schema Registry framings, opaque blobs, third-party formats).
/// The handler owns the wire-format details.
pub struct RawBytesCodec;

impl Codec<Vec<u8>> for RawBytesCodec {
    const NAME: &'static str = "raw";

    fn encode(value: &Vec<u8>) -> Result<Vec<u8>> {
        Ok(value.clone())
    }

    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(bytes.to_vec())
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

    #[test]
    fn raw_bytes_codec_passthrough() {
        let payload: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let bytes = <RawBytesCodec as Codec<Vec<u8>>>::encode(&payload).unwrap();
        assert_eq!(bytes, payload);
        let decoded = <RawBytesCodec as Codec<Vec<u8>>>::decode(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn raw_bytes_codec_name_is_raw() {
        assert_eq!(<RawBytesCodec as Codec<Vec<u8>>>::NAME, "raw");
    }

    #[test]
    fn json_codec_encode_to_string_round_trips() {
        let sample = Sample {
            id: "abc".into(),
            n: 42,
        };
        let s = <JsonCodec as Codec<Sample>>::encode_to_string(&sample).unwrap();
        assert!(s.contains("\"abc\""));
        let decoded: Sample = <JsonCodec as Codec<Sample>>::decode(s.as_bytes()).unwrap();
        assert_eq!(decoded, sample);
    }

    #[test]
    fn raw_bytes_codec_encode_to_string_surfaces_non_utf8_as_codec_error() {
        let payload: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        let err = <RawBytesCodec as Codec<Vec<u8>>>::encode_to_string(&payload).unwrap_err();
        match err {
            crate::ShoveError::Codec { codec, .. } => assert_eq!(codec, "raw"),
            other => panic!("expected Codec variant, got {other:?}"),
        }
    }
}
