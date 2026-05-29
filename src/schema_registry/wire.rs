//! Confluent wire-frame parsing. Pure — no I/O.

/// Confluent schema id (the 4-byte big-endian value after the magic byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaId(pub u32);

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which wire framing the payload uses after the 5-byte header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    /// Protobuf: a message-index varint array precedes the proto bytes.
    Protobuf,
    /// JSON Schema: the payload is plain JSON.
    Json,
}

impl WireFormat {
    /// Derive the wire format from a codec's [`crate::codec::Codec::NAME`].
    /// Returns `None` for codecs that don't correspond to a registry format.
    /// Avro is intentionally unsupported in this decode-only phase, so `"avro"`
    /// (and any other unrecognised name) maps to `None` by design.
    pub fn from_codec_name(name: &str) -> Option<Self> {
        match name {
            "protobuf" => Some(WireFormat::Protobuf),
            "json" => Some(WireFormat::Json),
            _ => None,
        }
    }
}

/// Result of parsing the Confluent frame.
#[derive(Debug, PartialEq)]
pub enum FrameResult<'a> {
    /// Empty/`None` Kafka payload (tombstone).
    Null,
    /// No `0x00` magic byte — not a registry-framed message.
    Unframed(&'a [u8]),
    /// A valid frame: schema id plus the post-header inner payload.
    Framed { id: SchemaId, payload: &'a [u8] },
}

/// Read a base-128 varint, returning (value, bytes_consumed). `None` if truncated.
#[allow(dead_code)] // Consumed by the consumer decode stage (added in a later task); allow until then.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for (i, b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        // At the last legal shift (63), only bit 63 may be set; anything above overflows u64.
        if shift == 63 && (b & 0x7e) != 0 {
            return None;
        }
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    None
}

/// Parse the Confluent frame for the given wire format.
#[allow(dead_code)] // Consumed by the consumer decode stage (added in a later task); allow until then.
pub fn parse_frame(format: WireFormat, bytes: &[u8]) -> FrameResult<'_> {
    if bytes.is_empty() {
        return FrameResult::Null;
    }
    if bytes[0] != 0x00 {
        return FrameResult::Unframed(bytes);
    }
    if bytes.len() < 5 {
        // Has the magic byte but is too short to carry an id — treat as unframed
        // so the caller's enforcement policy decides (never silently decode).
        return FrameResult::Unframed(bytes);
    }
    let id = SchemaId(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]));
    let rest = &bytes[5..];
    let payload = match format {
        WireFormat::Json => rest,
        WireFormat::Protobuf => match skip_message_indexes(rest) {
            Some(p) => p,
            None => return FrameResult::Unframed(bytes),
        },
    };
    FrameResult::Framed { id, payload }
}

/// Skip the protobuf message-index array, returning the proto bytes after it.
#[allow(dead_code)] // Consumed by the consumer decode stage (added in a later task); allow until then.
fn skip_message_indexes(bytes: &[u8]) -> Option<&[u8]> {
    let (count, mut off) = read_varint(bytes)?;
    if count == 0 {
        // Optimization: a single 0 means index [0]; no further varints.
        return Some(&bytes[off..]);
    }
    for _ in 0..count {
        let (_idx, n) = read_varint(&bytes[off..])?;
        off += n;
    }
    Some(&bytes[off..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_null() {
        assert_eq!(parse_frame(WireFormat::Json, &[]), FrameResult::Null);
    }

    #[test]
    fn no_magic_byte_is_unframed() {
        let bytes = [0x7b, 0x22]; // '{"'
        assert_eq!(
            parse_frame(WireFormat::Json, &bytes),
            FrameResult::Unframed(&bytes)
        );
    }

    #[test]
    fn json_frame_extracts_be_id_and_payload() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x01, b'{', b'}'];
        assert_eq!(
            parse_frame(WireFormat::Json, &bytes),
            FrameResult::Framed {
                id: SchemaId(1),
                payload: b"{}",
            }
        );
    }

    #[test]
    fn json_frame_large_id() {
        let bytes = [0x00, 0x00, 0x01, 0x86, 0xa0, 0xAB]; // id = 100_000
        assert_eq!(
            parse_frame(WireFormat::Json, &bytes),
            FrameResult::Framed {
                id: SchemaId(100_000),
                payload: &[0xAB]
            }
        );
    }

    #[test]
    fn protobuf_single_zero_index_optimization() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0xDE, 0xAD];
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xDE, 0xAD]
            }
        );
    }

    #[test]
    fn protobuf_explicit_index_array_is_skipped() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x05, 0x02, 0x01, 0x03, 0xBE, 0xEF];
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xBE, 0xEF]
            }
        );
    }

    #[test]
    fn magic_byte_but_too_short_is_unframed() {
        let bytes = [0x00, 0x00, 0x01];
        assert_eq!(
            parse_frame(WireFormat::Json, &bytes),
            FrameResult::Unframed(&bytes)
        );
    }

    #[test]
    fn wire_format_from_codec_name() {
        assert_eq!(
            WireFormat::from_codec_name("protobuf"),
            Some(WireFormat::Protobuf)
        );
        assert_eq!(WireFormat::from_codec_name("json"), Some(WireFormat::Json));
        assert_eq!(WireFormat::from_codec_name("raw"), None);
        assert_eq!(WireFormat::from_codec_name("avro"), None);
    }

    #[test]
    fn varint_overflow_returns_none_not_truncated_value() {
        // 10-byte overflowing varint for the message-index count: [0x80; 9] ++ [0x04]
        // Magic + id=7, then the overflow varint — parse_frame must return Unframed.
        let mut frame = vec![0x00, 0x00, 0x00, 0x00, 0x07];
        frame.extend_from_slice(&[0x80u8; 9]);
        frame.push(0x04);
        assert!(matches!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Unframed(_)
        ));
    }

    #[test]
    fn protobuf_truncated_index_array_is_unframed() {
        // count=3 but only 2 index varints present, then EOF.
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x07, 0x03, 0x01, 0x02];
        assert!(matches!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Unframed(_)
        ));
    }

    #[test]
    fn protobuf_multibyte_count_varint() {
        // count encoded as 2-byte varint [0x80, 0x01] = 128,
        // followed by 128 zero-byte index varints (value 0), then payload [0xAA].
        let mut frame = vec![0x00, 0x00, 0x00, 0x00, 0x07];
        frame.push(0x80); // low 7 bits of 128, continuation bit set
        frame.push(0x01); // high bit of 128
        frame.extend_from_slice(&[0x00u8; 128]); // 128 index varints, each 1 byte
        frame.push(0xAA); // payload
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Framed {
                id: SchemaId(7),
                payload: &[0xAA],
            }
        );
    }
}
