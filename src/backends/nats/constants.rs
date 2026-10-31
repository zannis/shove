pub(super) const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";
pub(super) const DEATH_REASON_HEADER: &str = "Shove-Death-Reason";
pub(super) const ORIGINAL_QUEUE_HEADER: &str = "Shove-Original-Queue";
pub(super) const DEATH_COUNT_HEADER: &str = "Shove-Death-Count";
/// The delivery a retry copy or dead letter was made from, as
/// `{stream}.{consumer}.{stream_sequence}`. A duplicate PubAck for a copy is
/// trusted only when the message stored under the copy's id carries the same
/// value and payload.
pub(super) const COPY_OF_HEADER: &str = "Shove-Copy-Of";
/// Sequence key of a message on a sequenced topic.
///
/// The shard is already encoded in the subject (`{queue}.shard.{n}`), but a
/// shard carries many keys, so `SequenceFailure::FailAll` needs the key itself
/// to poison one sequence without taking out the whole shard. Messages
/// published before this header existed simply carry no key and are therefore
/// never poisoned — `FailAll` degrades to `Skip` for them rather than
/// mis-poisoning.
pub(super) const SEQUENCE_KEY_HEADER: &str = "Shove-Sequence-Key";

/// The `max_ack_pending` of a durable shove creates when no budget was set:
/// unbounded, because every worker bounds its own in-flight messages.
pub(super) const UNBOUNDED_ACK_PENDING: i64 = -1;

/// Derives the durable consumer name from a queue name.
///
/// JetStream WorkQueue retention permits only one non-filtered consumer per
/// stream, so every task in a consumer group binds to the *same* durable
/// consumer and the server load-balances messages across them. The group
/// registry creates that durable once, and each task bounds its own
/// in-flight messages, so the durable needs no budget of its own.
pub(super) fn consumer_name(queue: &str) -> String {
    format!("{queue}-consumer")
}

/// Derives the DLQ consumer name from a DLQ stream name.
pub(super) fn dlq_consumer_name(dlq: &str) -> String {
    format!("{dlq}-consumer")
}
