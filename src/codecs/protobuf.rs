//! Protobuf codec backed by [`prost`].
//!
//! Enable via the `protobuf` cargo feature. Any type implementing
//! [`prost::Message`] + `Default` can ride through `Topic::Codec =
//! ProtobufCodec`.

use prost::Message;

use crate::codec::Codec;
use crate::error::{Result, ShoveError};

/// Encodes/decodes messages via [`prost`].
///
/// A single zero-sized marker that implements `Codec<M>` for every
/// `M: prost::Message + Default`. One topic can use `codec = ProtobufCodec`
/// for `OrderEvent`, another for `UserCreated`, and so on — each call site
/// picks the matching impl through `<ProtobufCodec as Codec<T::Message>>`.
///
/// Encoding pre-sizes the buffer via [`Message::encoded_len`] so a single
/// allocation suffices for the common case.
pub struct ProtobufCodec;

impl<M> Codec<M> for ProtobufCodec
where
    M: Message + Default + Send + Sync + 'static,
{
    const NAME: &'static str = "protobuf";

    fn encode(value: &M) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(value.encoded_len());
        value.encode(&mut buf).map_err(|e| ShoveError::Codec {
            codec: "protobuf",
            source: Box::new(e),
        })?;
        Ok(buf)
    }

    fn decode(bytes: &[u8]) -> Result<M> {
        M::decode(bytes).map_err(|e| ShoveError::Codec {
            codec: "protobuf",
            source: Box::new(e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct TestMsg {
        #[prost(string, tag = "1")]
        id: String,
        #[prost(uint64, tag = "2")]
        n: u64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct OtherMsg {
        #[prost(string, tag = "1")]
        name: String,
    }

    #[test]
    fn protobuf_codec_round_trip() {
        let m = TestMsg {
            id: "abc".into(),
            n: 42,
        };
        let bytes = <ProtobufCodec as Codec<TestMsg>>::encode(&m).unwrap();
        let back = <ProtobufCodec as Codec<TestMsg>>::decode(&bytes).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn protobuf_codec_decode_garbage_surfaces_codec_variant() {
        let bad: &[u8] = &[0xff; 32];
        let err = <ProtobufCodec as Codec<TestMsg>>::decode(bad).unwrap_err();
        match err {
            crate::ShoveError::Codec { codec, .. } => assert_eq!(codec, "protobuf"),
            other => panic!("expected Codec variant, got {other:?}"),
        }
    }

    #[test]
    fn protobuf_codec_name_is_protobuf() {
        assert_eq!(<ProtobufCodec as Codec<TestMsg>>::NAME, "protobuf");
    }

    #[test]
    fn protobuf_codec_handles_multiple_distinct_message_types() {
        let a = TestMsg {
            id: "x".into(),
            n: 1,
        };
        let b = OtherMsg { name: "y".into() };

        let a_bytes = <ProtobufCodec as Codec<TestMsg>>::encode(&a).unwrap();
        let b_bytes = <ProtobufCodec as Codec<OtherMsg>>::encode(&b).unwrap();

        let a_back = <ProtobufCodec as Codec<TestMsg>>::decode(&a_bytes).unwrap();
        let b_back = <ProtobufCodec as Codec<OtherMsg>>::decode(&b_bytes).unwrap();

        assert_eq!(a_back, a);
        assert_eq!(b_back, b);
    }
}
