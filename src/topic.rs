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

/// Capability: this topic is safe to consume in batches.
///
/// Implemented automatically by `define_topic!` and deliberately **not** by
/// `define_sequenced_topic!`. Batch consumption
/// (`KafkaConsumer::run_batch`) is bound on this trait, so passing a
/// sequenced topic to it is a compile error rather than a silent
/// ordering violation.
///
/// # Why the exclusion exists
///
/// Batching and sequencing are mutually exclusive by design. A batch-wide
/// [`Outcome`](crate::Outcome) carries no sequence key, so it cannot express
/// the per-key poison set that [`SequenceFailure::FailAll`] implements: a
/// batch-wide `Reject` over a batch spanning many keys could only poison
/// every key (failing keys that succeeded) or none (silently downgrading
/// `FailAll`). Neither is correct. Use `run_fifo` for sequenced topics.
///
/// See `docs/design/batch-and-sequencing.md` for the full rationale.
///
/// # Hand-rolled topics
///
/// A hand-written `impl Topic for X` does not get this automatically. Add
/// `impl NotSequenced for X {}` if the topic is genuinely unsequenced. The
/// batch consumer additionally re-checks `topology().sequencing()` at
/// runtime, so a topic that claims `NotSequenced` while carrying sequencing
/// config is rejected rather than silently consumed out of order.
///
/// # Pinning the exclusion
///
/// A sequenced topic does not satisfy this bound:
///
/// ```compile_fail
/// use std::time::Duration;
/// use shove::{
///     NotSequenced, SequenceFailure, SequencedTopic, TopologyBuilder,
///     define_sequenced_topic,
/// };
///
/// define_sequenced_topic!(Ledger, String, |m: &String| m.clone(),
///     TopologyBuilder::new("ledger")
///         .sequenced(SequenceFailure::FailAll)
///         .hold_queue(Duration::from_secs(5))
///         .dlq()
///         .build());
///
/// fn batch_only<T: NotSequenced>() {}
/// // error: the trait bound `Ledger: NotSequenced` is not satisfied
/// batch_only::<Ledger>();
/// ```
///
/// A `compile_fail` doctest passes on *any* compile error, so it needs a
/// control: the same definition, bound on [`Topic`] instead, must compile.
/// If the macro invocation above ever breaks for an unrelated reason, this
/// one fails loudly instead of the exclusion silently going untested.
///
/// ```
/// use std::time::Duration;
/// use shove::{
///     SequenceFailure, SequencedTopic, Topic, TopologyBuilder,
///     define_sequenced_topic,
/// };
///
/// define_sequenced_topic!(Ledger, String, |m: &String| m.clone(),
///     TopologyBuilder::new("ledger")
///         .sequenced(SequenceFailure::FailAll)
///         .hold_queue(Duration::from_secs(5))
///         .dlq()
///         .build());
///
/// fn any_topic<T: Topic>() {}
/// any_topic::<Ledger>();
/// ```
///
/// And an unsequenced topic does satisfy the bound:
///
/// ```
/// use shove::{NotSequenced, TopologyBuilder, define_topic};
///
/// define_topic!(Orders, String, TopologyBuilder::new("orders").build());
///
/// fn batch_only<T: NotSequenced>() {}
/// batch_only::<Orders>();
/// ```
///
/// [`SequenceFailure::FailAll`]: crate::SequenceFailure::FailAll
#[diagnostic::on_unimplemented(
    message = "`{Self}` is a sequenced topic; batch consumption is unavailable.",
    label = "sequenced topic",
    note = "Batching and sequencing are mutually exclusive: a batch-wide `Outcome` carries no sequence key, so it cannot express the per-key poison set that `SequenceFailure::FailAll` implements. Use `run_fifo` for sequenced topics.",
    note = "If `{Self}` is a hand-rolled unsequenced topic, write an empty `impl NotSequenced for {Self}` block."
)]
pub trait NotSequenced: Topic {}

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
