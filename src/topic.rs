use crate::codec::Codec;
use crate::topology::QueueTopology;

/// A logical message topic that binds a message type, its codec, and its
/// queue topology together.
///
/// Implement on a unit struct per topic. Prefer the `define_topic!` /
/// `define_sequenced_topic!` macros — they generate the `OnceLock` for the
/// static topology and default the codec to `JsonCodec`.
///
/// Hand-rolled `impl Topic for X` blocks must set `type Codec` explicitly;
/// associated type defaults are unstable, so there is no built-in fallback.
pub trait Topic: Send + Sync + 'static {
    /// The message type that flows through this topic.
    type Message: Send + Sync + 'static;

    /// The codec used to encode and decode `Self::Message` on every backend.
    type Codec: Codec<Self::Message>;

    /// Returns the queue topology for this topic.
    ///
    /// Must return the same `&'static` reference every time. Use `OnceLock`
    /// internally (the `define_topic!` macro does this automatically).
    fn topology() -> &'static QueueTopology;

    /// Optional sequence key extractor for publisher routing.
    ///
    /// `None` for unsequenced topics (default). Set to `Some(Self::sequence_key)`
    /// when implementing `SequencedTopic`. The `define_sequenced_topic!` macro
    /// wires this automatically.
    ///
    /// The publisher uses this to route sequenced messages to the correct
    /// broker primitive without requiring a `SequencedTopic` bound.
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = None;
}

/// Extension trait for topics that require strict message ordering.
///
/// Messages sharing the same sequence key are consumed in strict order:
/// a message is only delivered after all preceding messages in the same
/// sequence have been successfully acked.
///
/// The failure behavior when a message in a sequence is permanently
/// rejected is configured via `SequenceFailure` in the topology.
pub trait SequencedTopic: Topic {
    /// Extract the sequence key from a message.
    ///
    /// All messages returning the same key form an ordered sequence.
    /// Different keys are independent and can be consumed concurrently.
    fn sequence_key(message: &Self::Message) -> String;
}
