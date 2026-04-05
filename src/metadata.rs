use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata about a consumed message, extracted from broker headers/properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// How many times this message has been retried (0 on first delivery).
    pub retry_count: u32,
    /// Opaque delivery identifier (AMQP tag, SQS receipt handle, etc).
    pub delivery_id: String,
    /// Whether the broker flagged this as a redelivery.
    pub redelivered: bool,
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
    pub headers: HashMap<String, String>,
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
