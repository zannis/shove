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
    /// A valid frame: schema id, the post-header inner payload, and for
    /// [`WireFormat::Protobuf`] the message-index array naming which message
    /// of the schema file the payload encodes (`[0]` is the first top-level
    /// message). `None` for JSON, which carries no index.
    Framed {
        id: SchemaId,
        payload: &'a [u8],
        message_index: Option<Vec<i32>>,
    },
}

/// The most message indexes a frame may carry before it is treated as
/// garbage rather than as a path into an implausibly deep schema file.
const MAX_MESSAGE_INDEXES: i64 = 1024;

/// Confluent writes the message-index count and every index as zigzag
/// varints (Kafka's `ByteUtils.writeVarint`), so `1` is on the wire as `0x02`.
fn zigzag_decode(raw: u64) -> i64 {
    ((raw >> 1) as i64) ^ -((raw & 1) as i64)
}

fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Read a base-128 varint, returning (value, bytes_consumed). `None` if truncated.
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

/// Append `value` to `out` as a base-128 varint. Inverse of [`read_varint`].
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Build a Confluent wire frame — the inverse of [`parse_frame`].
///
/// Layout: magic byte `0x00`, big-endian schema id, then for [`WireFormat::Protobuf`]
/// the message-index array followed by the proto `payload`; for [`WireFormat::Json`]
/// the `payload` directly (`msg_index` is ignored). The common protobuf index
/// `[0]` is encoded as a single `0x00` byte, matching the count==0 fast path in
/// [`read_message_indexes`]; any other array is encoded as Confluent writes
/// it, a zigzag varint count followed by each index as a zigzag varint.
pub fn build_frame(format: WireFormat, id: SchemaId, msg_index: &[u32], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0x00);
    out.extend_from_slice(&id.0.to_be_bytes());
    if format == WireFormat::Protobuf {
        if msg_index == [0] {
            out.push(0x00);
        } else {
            write_varint(&mut out, zigzag_encode(msg_index.len() as i64));
            for &idx in msg_index {
                write_varint(&mut out, zigzag_encode(i64::from(idx)));
            }
        }
    }
    out.extend_from_slice(payload);
    out
}

/// Parse the Confluent frame for the given wire format.
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
    let (payload, message_index) = match format {
        WireFormat::Json => (rest, None),
        WireFormat::Protobuf => match read_message_indexes(rest) {
            Some((indexes, p)) => (p, Some(indexes)),
            None => return FrameResult::Unframed(bytes),
        },
    };
    FrameResult::Framed {
        id,
        payload,
        message_index,
    }
}

/// Read the protobuf message-index array, returning it with the proto bytes
/// after it. `None` when the array is truncated, negative, or implausibly
/// long.
///
/// The encoding is Confluent's: a zigzag varint count followed by one zigzag
/// varint per index, with the single byte `0x00` standing for the common
/// `[0]`. A frame written by a Confluent serializer with index `[1]` is on the
/// wire as `0x02 0x02`; reading the values as plain varints would take that
/// for a two-element array and swallow the first byte of the payload.
fn read_message_indexes(bytes: &[u8]) -> Option<(Vec<i32>, &[u8])> {
    let (raw_count, mut off) = read_varint(bytes)?;
    if raw_count == 0 {
        // Optimization: a single 0 means index [0]; no further varints.
        return Some((vec![0], &bytes[off..]));
    }
    let count = zigzag_decode(raw_count);
    if count <= 0 || count > MAX_MESSAGE_INDEXES {
        return None;
    }
    let mut indexes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (raw, n) = read_varint(&bytes[off..])?;
        let index = zigzag_decode(raw);
        if index < 0 || index > i64::from(i32::MAX) {
            return None;
        }
        indexes.push(index as i32);
        off += n;
    }
    Some((indexes, &bytes[off..]))
}

/// The one check every `require_schema_message_index` setter applies, on
/// `ConsumerOptions::<Kafka>`, `KafkaConsumerGroupConfig` and
/// `BatchConsumerOptions::<Kafka>`, at the same fail-fast point as
/// `validate_commit_interval`.
///
/// [`read_message_indexes`] only ever yields a non-empty array of
/// non-negative indexes, so an empty or negative requirement can never equal
/// a parsed index: every protobuf frame would go to the DLQ as
/// `schema_message_index_rejected`, and nothing at startup would point at
/// the cause. Refused at configuration time instead.
pub(crate) fn validate_message_index(index: &[i32]) {
    assert!(
        !index.is_empty(),
        "schema_message_index must not be empty: a Confluent protobuf frame always carries at \
         least one index"
    );
    assert!(
        index.iter().all(|i| *i >= 0),
        "schema_message_index must not contain a negative index, got {index:?}"
    );
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
                message_index: None,
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
                payload: &[0xAB],
                message_index: None,
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
                payload: &[0xDE, 0xAD],
                message_index: Some(vec![0]),
            }
        );
    }

    /// Confluent's encoding: count 2 is `0x04`, index 1 is `0x02`, index 3 is
    /// `0x06`, all zigzag varints.
    #[test]
    fn protobuf_explicit_index_array_is_read() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x05, 0x04, 0x02, 0x06, 0xBE, 0xEF];
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xBE, 0xEF],
                message_index: Some(vec![1, 3]),
            }
        );
    }

    /// The path `[2, 1, 3]`, the third top-level message's second nested
    /// message's fourth nested message, as a Confluent serializer frames it.
    #[test]
    fn protobuf_nested_index_path_is_read() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x09, 0x06, 0x04, 0x02, 0x06, 0xAA];
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Framed {
                id: SchemaId(9),
                payload: &[0xAA],
                message_index: Some(vec![2, 1, 3]),
            }
        );
    }

    /// An explicit `[0]` (count 1 as `0x02`, index 0 as `0x00`) reads the same
    /// as the single-byte shorthand.
    #[test]
    fn protobuf_explicit_zero_index_equals_the_shorthand() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x05, 0x02, 0x00, 0xAA];
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xAA],
                message_index: Some(vec![0]),
            }
        );
    }

    /// A zigzag count that decodes negative (`0x01` is -1) is not a frame.
    #[test]
    fn protobuf_negative_index_count_is_unframed() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0xAA];
        assert!(matches!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Unframed(_)
        ));
    }

    #[test]
    fn zigzag_round_trips() {
        for value in [0i64, 1, -1, 2, -2, 127, 128, i64::from(i32::MAX)] {
            assert_eq!(zigzag_decode(zigzag_encode(value)), value);
        }
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_decode(2), 1);
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
    fn build_json_frame_exact_bytes_and_round_trip() {
        let frame = build_frame(WireFormat::Json, SchemaId(1), &[], b"{}");
        assert_eq!(frame, [0x00, 0x00, 0x00, 0x00, 0x01, b'{', b'}']);
        assert_eq!(
            parse_frame(WireFormat::Json, &frame),
            FrameResult::Framed {
                id: SchemaId(1),
                payload: b"{}",
                message_index: None,
            }
        );
    }

    #[test]
    fn build_protobuf_zero_index_uses_single_byte_optimization() {
        // msg_index [0] must encode as a single 0x00 byte (matching
        // skip_message_indexes' count==0 fast path), not [0x01, 0x00].
        let frame = build_frame(WireFormat::Protobuf, SchemaId(5), &[0], &[0xDE, 0xAD]);
        assert_eq!(frame, [0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0xDE, 0xAD]);
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xDE, 0xAD],
                message_index: Some(vec![0]),
            }
        );
    }

    #[test]
    fn build_protobuf_explicit_index_array_round_trips() {
        // A non-[0] index array is encoded as a zigzag count varint + each
        // index as a zigzag varint: count 2 -> 0x04, index 1 -> 0x02, 3 -> 0x06.
        let frame = build_frame(WireFormat::Protobuf, SchemaId(5), &[1, 3], &[0xBE, 0xEF]);
        assert_eq!(
            frame,
            [0x00, 0x00, 0x00, 0x00, 0x05, 0x04, 0x02, 0x06, 0xBE, 0xEF]
        );
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Framed {
                id: SchemaId(5),
                payload: &[0xBE, 0xEF],
                message_index: Some(vec![1, 3]),
            }
        );
    }

    #[test]
    fn build_frame_large_id_round_trips() {
        let frame = build_frame(WireFormat::Json, SchemaId(100_000), &[], &[0xAB]);
        assert_eq!(
            parse_frame(WireFormat::Json, &frame),
            FrameResult::Framed {
                id: SchemaId(100_000),
                payload: &[0xAB],
                message_index: None,
            }
        );
    }

    #[test]
    fn build_protobuf_multibyte_index_varint_round_trips() {
        // An index of 128 is zigzag 256, a 2-byte varint ([0x80, 0x02]), which
        // exercises write_varint's continuation-bit path symmetrically with
        // read_varint.
        let frame = build_frame(WireFormat::Protobuf, SchemaId(7), &[128], &[0xAA]);
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Framed {
                id: SchemaId(7),
                payload: &[0xAA],
                message_index: Some(vec![128]),
            }
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
        // count=3 (zigzag 0x06) but only 2 index varints present, then EOF.
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x07, 0x06, 0x02, 0x04];
        assert!(matches!(
            parse_frame(WireFormat::Protobuf, &bytes),
            FrameResult::Unframed(_)
        ));
    }

    #[test]
    fn protobuf_multibyte_count_varint() {
        // count 128 is zigzag 256, a 2-byte varint [0x80, 0x02], followed by
        // 128 zero-byte index varints (value 0), then payload [0xAA].
        let mut frame = vec![0x00, 0x00, 0x00, 0x00, 0x07];
        frame.push(0x80); // low 7 bits of 256, continuation bit set
        frame.push(0x02); // high bits of 256
        frame.extend_from_slice(&[0x00u8; 128]); // 128 index varints, each 1 byte
        frame.push(0xAA); // payload
        assert_eq!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Framed {
                id: SchemaId(7),
                payload: &[0xAA],
                message_index: Some(vec![0; 128]),
            }
        );
    }

    /// A count above the cap is garbage, not a schema file a thousand
    /// messages deep.
    #[test]
    fn protobuf_implausible_index_count_is_unframed() {
        let mut frame = vec![0x00, 0x00, 0x00, 0x00, 0x07];
        write_varint(&mut frame, zigzag_encode(MAX_MESSAGE_INDEXES + 1));
        frame.extend_from_slice(&[0x00u8; 8]);
        assert!(matches!(
            parse_frame(WireFormat::Protobuf, &frame),
            FrameResult::Unframed(_)
        ));
    }

    /// The validator admits exactly the shapes the parser yields: the
    /// shorthand, an explicit array and a nested path.
    #[test]
    fn validate_message_index_accepts_what_the_parser_yields() {
        for index in [vec![0], vec![1], vec![0, 2], vec![2, 1, 3]] {
            let frame = build_frame(
                WireFormat::Protobuf,
                SchemaId(7),
                &index.iter().map(|i| *i as u32).collect::<Vec<_>>(),
                &[0xAA],
            );
            let FrameResult::Framed { message_index, .. } =
                parse_frame(WireFormat::Protobuf, &frame)
            else {
                panic!("frame must parse");
            };
            let parsed = message_index.expect("protobuf frames carry an index");
            validate_message_index(&parsed);
            assert_eq!(parsed, index);
        }
    }

    #[test]
    #[should_panic(expected = "schema_message_index must not be empty")]
    fn validate_message_index_rejects_an_empty_requirement() {
        validate_message_index(&[]);
    }

    #[test]
    #[should_panic(
        expected = "schema_message_index must not contain a negative index, got [0, -1]"
    )]
    fn validate_message_index_rejects_a_negative_requirement() {
        validate_message_index(&[0, -1]);
    }
}
