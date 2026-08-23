use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata about a consumed message, extracted from broker headers/properties.
///
/// `shove` builds this for you on every delivery; you normally only read it.
/// It is [`#[non_exhaustive]`][non_exhaustive] so that future backends can
/// report more about a delivery without that being a breaking change, which
/// means downstream code cannot build one with a struct literal. To construct
/// one — in handler unit tests, say — use [`MessageMetadata::builder`]:
///
/// ```
/// use shove::MessageMetadata;
///
/// let metadata = MessageMetadata::builder().retry_count(3).redelivered(true).build();
/// assert_eq!(metadata.retry_count, 3);
/// ```
///
/// Reading every field stays exactly as it was. Only these two shapes changed,
/// and both are rejected at compile time:
///
/// ```compile_fail,E0639
/// # use shove::MessageMetadata;
/// # use std::{collections::HashMap, sync::Arc};
/// // error[E0639]: cannot create non-exhaustive struct using struct expression
/// let metadata = MessageMetadata {
///     retry_count: 0,
///     delivery_id: String::new(),
///     redelivered: false,
///     delivery_count: None,
///     headers: Arc::new(HashMap::new()),
/// };
/// ```
///
/// ```compile_fail,E0638
/// # use shove::MessageMetadata;
/// # fn f(metadata: MessageMetadata) {
/// // error[E0638]: `..` required with non-exhaustive struct pattern
/// let MessageMetadata { retry_count, delivery_id, redelivered, delivery_count, headers } = metadata;
/// # }
/// ```
///
/// Add `..` to the pattern and the destructuring compiles again.
///
/// [non_exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute
#[non_exhaustive]
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
///
/// Like [`MessageMetadata`], this is `#[non_exhaustive]`; construct one with
/// [`DeadMessageMetadata::builder`].
#[non_exhaustive]
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

impl MessageMetadata {
    /// Starts building a [`MessageMetadata`].
    ///
    /// Every field defaults to its empty value — no retries, no headers, an
    /// empty `delivery_id`, and an unknown `delivery_count` — so you only set
    /// what your test actually asserts on.
    ///
    /// ```
    /// use shove::MessageMetadata;
    ///
    /// let metadata = MessageMetadata::builder()
    ///     .delivery_id("amqp-tag-7")
    ///     .retry_count(2)
    ///     .header("x-trace-id", "abc123")
    ///     .build();
    ///
    /// assert_eq!(metadata.headers.get("x-trace-id").map(String::as_str), Some("abc123"));
    /// ```
    pub fn builder() -> MessageMetadataBuilder {
        MessageMetadataBuilder::default()
    }
}

/// Builder for [`MessageMetadata`]. Created by [`MessageMetadata::builder`].
#[derive(Debug, Clone, Default)]
pub struct MessageMetadataBuilder {
    retry_count: u32,
    delivery_id: String,
    redelivered: bool,
    delivery_count: Option<u32>,
    headers: HashMap<String, String>,
}

impl MessageMetadataBuilder {
    /// Sets [`MessageMetadata::retry_count`]. Defaults to `0`.
    pub fn retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    /// Sets [`MessageMetadata::delivery_id`]. Defaults to an empty string.
    pub fn delivery_id(mut self, delivery_id: impl Into<String>) -> Self {
        self.delivery_id = delivery_id.into();
        self
    }

    /// Sets [`MessageMetadata::redelivered`]. Defaults to `false`.
    pub fn redelivered(mut self, redelivered: bool) -> Self {
        self.redelivered = redelivered;
        self
    }

    /// Sets [`MessageMetadata::delivery_count`]. Defaults to `None`, which
    /// means "the backend cannot supply a count" — not zero.
    pub fn delivery_count(mut self, delivery_count: impl Into<Option<u32>>) -> Self {
        self.delivery_count = delivery_count.into();
        self
    }

    /// Adds a single header, overwriting any previous value for `key`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Replaces all headers set so far.
    pub fn headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
        self.headers = headers.into_iter().collect();
        self
    }

    /// Finishes the build.
    pub fn build(self) -> MessageMetadata {
        MessageMetadata {
            retry_count: self.retry_count,
            delivery_id: self.delivery_id,
            redelivered: self.redelivered,
            delivery_count: self.delivery_count,
            headers: Arc::new(self.headers),
        }
    }
}

impl DeadMessageMetadata {
    /// Starts building a [`DeadMessageMetadata`] around the message that died.
    ///
    /// ```
    /// use shove::{DeadMessageMetadata, MessageMetadata};
    ///
    /// let dead = DeadMessageMetadata::builder(MessageMetadata::builder().build())
    ///     .reason("rejected")
    ///     .death_count(1)
    ///     .build();
    ///
    /// assert_eq!(dead.reason.as_deref(), Some("rejected"));
    /// ```
    pub fn builder(message: MessageMetadata) -> DeadMessageMetadataBuilder {
        DeadMessageMetadataBuilder {
            message,
            reason: None,
            original_queue: None,
            death_count: 0,
        }
    }
}

/// Builder for [`DeadMessageMetadata`]. Created by [`DeadMessageMetadata::builder`].
#[derive(Debug, Clone)]
pub struct DeadMessageMetadataBuilder {
    message: MessageMetadata,
    reason: Option<String>,
    original_queue: Option<String>,
    death_count: u32,
}

impl DeadMessageMetadataBuilder {
    /// Sets [`DeadMessageMetadata::reason`]. Defaults to `None`.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Sets [`DeadMessageMetadata::original_queue`]. Defaults to `None`.
    pub fn original_queue(mut self, original_queue: impl Into<String>) -> Self {
        self.original_queue = Some(original_queue.into());
        self
    }

    /// Sets [`DeadMessageMetadata::death_count`]. Defaults to `0`.
    pub fn death_count(mut self, death_count: u32) -> Self {
        self.death_count = death_count;
        self
    }

    /// Finishes the build.
    pub fn build(self) -> DeadMessageMetadata {
        DeadMessageMetadata {
            message: self.message,
            reason: self.reason,
            original_queue: self.original_queue,
            death_count: self.death_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_builder_defaults_every_field_to_empty() {
        let metadata = MessageMetadata::builder().build();

        assert_eq!(metadata.retry_count, 0);
        assert_eq!(metadata.delivery_id, "");
        assert!(!metadata.redelivered);
        // `None` means "unknown", never zero — see the field docs.
        assert_eq!(metadata.delivery_count, None);
        assert!(metadata.headers.is_empty());
    }

    #[test]
    fn message_builder_sets_every_field() {
        let metadata = MessageMetadata::builder()
            .retry_count(3)
            .delivery_id("amqp-tag-7")
            .redelivered(true)
            .delivery_count(5)
            .header("x-trace-id", "abc123")
            .build();

        assert_eq!(metadata.retry_count, 3);
        assert_eq!(metadata.delivery_id, "amqp-tag-7");
        assert!(metadata.redelivered);
        assert_eq!(metadata.delivery_count, Some(5));
        assert_eq!(
            metadata.headers.get("x-trace-id").map(String::as_str),
            Some("abc123")
        );
    }

    #[test]
    fn delivery_count_accepts_both_a_bare_count_and_an_option() {
        assert_eq!(
            MessageMetadata::builder()
                .delivery_count(2)
                .build()
                .delivery_count,
            Some(2)
        );
        assert_eq!(
            MessageMetadata::builder()
                .delivery_count(Some(2))
                .build()
                .delivery_count,
            Some(2)
        );
        assert_eq!(
            MessageMetadata::builder()
                .delivery_count(None)
                .build()
                .delivery_count,
            None
        );
    }

    #[test]
    fn header_overwrites_and_headers_replaces() {
        let metadata = MessageMetadata::builder()
            .header("k", "first")
            .header("k", "second")
            .build();
        assert_eq!(
            metadata.headers.get("k").map(String::as_str),
            Some("second")
        );

        let metadata = MessageMetadata::builder()
            .header("dropped", "v")
            .headers([("kept".to_string(), "v".to_string())])
            .build();
        assert!(!metadata.headers.contains_key("dropped"));
        assert!(metadata.headers.contains_key("kept"));
    }

    #[test]
    fn dead_message_builder_keeps_the_message_and_defaults_the_rest() {
        let dead =
            DeadMessageMetadata::builder(MessageMetadata::builder().delivery_id("d-1").build())
                .build();

        assert_eq!(dead.message.delivery_id, "d-1");
        assert_eq!(dead.reason, None);
        assert_eq!(dead.original_queue, None);
        assert_eq!(dead.death_count, 0);
    }

    #[test]
    fn dead_message_builder_sets_every_field() {
        let dead = DeadMessageMetadata::builder(MessageMetadata::builder().build())
            .reason("rejected")
            .original_queue("orders")
            .death_count(2)
            .build();

        assert_eq!(dead.reason.as_deref(), Some("rejected"));
        assert_eq!(dead.original_queue.as_deref(), Some("orders"));
        assert_eq!(dead.death_count, 2);
    }

    #[test]
    fn metadata_still_round_trips_through_serde() {
        let metadata = MessageMetadata::builder()
            .retry_count(1)
            .delivery_id("d-9")
            .delivery_count(4)
            .header("h", "v")
            .build();

        let json = serde_json::to_string(&metadata).expect("serialize");
        let back: MessageMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.retry_count, 1);
        assert_eq!(back.delivery_id, "d-9");
        assert_eq!(back.delivery_count, Some(4));
        assert_eq!(back.headers.get("h").map(String::as_str), Some("v"));
    }
}
