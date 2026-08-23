use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata about a consumed message, extracted from broker headers/properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// How many times this message has been retried (0 on first delivery).
    pub retry_count: u32,
    /// Opaque delivery identifier (AMQP tag, SQS receipt handle, etc).
    pub delivery_id: String,
    /// Whether the broker flagged this as a redelivery.
    pub redelivered: bool,
    /// How many times the **broker** has delivered this message, including the
    /// current delivery, or `None` when the backend cannot supply a count.
    ///
    /// This is the broker's own attempt counter, not `shove`'s, so it sees
    /// deliveries [`retry_count`](Self::retry_count) cannot: a consumer crash,
    /// an ack that never landed, a visibility timeout expiring, and — on NATS —
    /// [`Outcome::Defer`](crate::Outcome::Defer) hops.
    ///
    /// That last one is the reason this field exists. `Defer` deliberately
    /// leaves `retry_count` untouched, so a handler that defers forever bounces
    /// between the main queue and the first hold queue with `retry_count`
    /// pinned at its original value and nothing to alert on beyond
    /// [`redelivered`](Self::redelivered), a bare boolean. `delivery_count`
    /// makes "stuck at N attempts" expressible:
    ///
    /// ```ignore
    /// // NATS: Defer naks in place, so this climbs with every deferred hop.
    /// if metadata.delivery_count.is_some_and(|n| n > 20) {
    ///     tracing::error!(delivery_count = ?metadata.delivery_count, "message is stuck");
    ///     return Outcome::Reject; // to the DLQ instead of deferring again
    /// }
    /// ```
    ///
    /// ## Per-backend availability
    ///
    /// | Backend | Value | Source |
    /// |---|---|---|
    /// | NATS JetStream | `Some(n)` | `num_delivered` from the message's stream metadata |
    /// | AWS SQS | `Some(n)` | the `ApproximateReceiveCount` system attribute |
    /// | In-process | `Some(n)` | counted by the in-process broker |
    /// | RabbitMQ | `None` | AMQP 0-9-1 carries only the `redelivered` flag, no counter |
    /// | Apache Kafka | `None` | delivery is offset-based; brokers keep no per-message attempt counter |
    /// | Redis Streams | `None` | the count lives in the group's PEL; reading it would cost an `XPENDING` per message |
    ///
    /// Treat `None` as "unknown", never as zero. Where it is reported, a first
    /// delivery is `Some(1)`.
    ///
    /// ## What resets it
    ///
    /// The count belongs to a *broker-level* message, so anything that creates a
    /// new one starts it over at 1. [`Outcome::Retry`](crate::Outcome::Retry)
    /// publishes an incremented copy on every backend, so it always resets;
    /// `retry_count` is the field that survives a retry. `Defer` resets it on
    /// the backends where deferring re-sends the message (SQS) and preserves it
    /// on the backends where deferring naks in place (NATS, and the in-process
    /// broker, which models NATS here).
    ///
    /// So: use `retry_count` to reason about `shove`'s retry budget, and
    /// `delivery_count` to reason about attempts the broker repeated on its own.
    /// Neither is a total-attempts-ever counter, and this field does not try to
    /// be one — it reports what the broker actually knows.
    ///
    /// SQS's `ApproximateReceiveCount` is approximate by AWS's own definition:
    /// it can over-count when a receive is not followed by a delete. Use it for
    /// thresholds, not for exact accounting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_count: Option<u32>,
    /// String-valued headers attached to the delivery (e.g. `x-trace-id`).
    ///
    /// ## Deduplication (`x-message-id`, RabbitMQ)
    ///
    /// For RabbitMQ, the `x-message-id` header (see [`shove::rabbitmq::MESSAGE_ID_KEY`]) is a
    /// stable per-message UUID stamped by [`RabbitMqPublisher`] on every outgoing
    /// message and preserved through all hold-queue hops.
    ///
    /// Under normal operation each delivery of the same logical message has the
    /// same `x-message-id`. Handlers can store this value (e.g. in Redis or a
    /// database) and skip processing if they have already seen it:
    ///
    /// ```ignore
    /// if let Some(mid) = metadata.headers.get(shove::rabbitmq::MESSAGE_ID_KEY) {
    ///     if store.already_processed(mid).await? {
    ///         return Outcome::Ack;
    ///     }
    ///     store.mark_processed(mid).await?;
    /// }
    /// // ... business logic ...
    /// ```
    ///
    /// **When this matters:** if the broker requeues an unacked original while
    /// the hold-queue copy is also in flight (the publish-then-ack race), both
    /// deliveries carry the same `x-message-id`. Deduplicating on it prevents
    /// the handler running twice for the same logical message.
    ///
    /// **Limitation for external producers:** messages published outside of
    /// `RabbitMqPublisher` will not have `x-message-id` on their first delivery.
    /// The header is stamped when the message first enters a hold queue, so
    /// deduplication becomes available from the *second* retry onward for those
    /// messages.
    ///
    /// Stored as `Arc<HashMap>` so cloning the metadata only bumps the
    /// refcount; the underlying map is shared across delivery, handler, and
    /// outcome-routing paths.
    pub headers: Arc<HashMap<String, String>>,
}

/// Metadata about a dead-lettered message.
#[derive(Debug, Clone)]
pub struct DeadMessageMetadata {
    /// Base message metadata.
    pub message: MessageMetadata,
    /// Why the message was dead-lettered (e.g., "rejected", "expired").
    pub reason: Option<String>,
    /// The original queue the message was in before being dead-lettered.
    pub original_queue: Option<String>,
    /// How many times this message has been dead-lettered.
    pub death_count: u32,
}
