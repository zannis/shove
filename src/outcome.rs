use serde::{Deserialize, Serialize};

/// What a handler instructs the consumer to do with a message after processing.
///
/// # Without hold queues
///
/// [`Retry`](Outcome::Retry) and [`Defer`](Outcome::Defer) take their delay
/// from the topology's hold queues. A topology that declares none gives them
/// no delay to pick. For the single-message consumers of a shared queue, each
/// backend then redelivers in its own way:
///
/// - **Kafka**: after a fixed 1 s the record is republished into the topic,
///   with the retry count incremented for a `Retry`. Under
///   `RetryStrategy::InPlace` the handler's task waits 1 s and hands the same
///   record back. No warning is logged.
/// - **NATS JetStream**: after a fixed 1 s. On a shove-owned stream a `Retry`
///   holds the original un-acked and then publishes a copy with the count
///   incremented. A `Defer`, and both outcomes on an external stream, nak with
///   a 1 s delay. No warning is logged.
/// - **RabbitMQ**: no delay; the delivery is nacked with requeue. It keeps its
///   retry count, so a `Retry` never spends `max_retries`. A warning is logged
///   for a `Retry` and for a `Defer`.
/// - **Redis Streams**: no delay; the entry is added back to its stream and the
///   original is acknowledged. A warning is logged for a `Retry` and for a
///   `Defer`.
/// - **AWS SQS**: no delay; the message is re-sent with a delay of 0 seconds
///   and the original is deleted. A FIFO queue sets the visibility timeout to 0
///   instead. A warning is logged for a `Retry` and for a `Defer`.
/// - **In-process**: no delay. A warning is logged for a `Defer` and none for
///   a `Retry`.
///
/// Those warnings are logged each time the fallback is taken. Building a
/// topology that declares a DLQ and no hold queues also logs one warning at
/// build time, whichever backend it runs on. The warning is skipped for a
/// sequenced topology and for one that calls `allow_message_loss()`.
///
/// A `Retry` still increments the retry count on every backend except
/// RabbitMQ. The 1 s wait on Kafka and NATS has no setting; declare a hold
/// queue to choose the delay.
///
/// Two other consumers never take this fallback. A
/// [`BroadcastSubscriber`](crate::BroadcastSubscriber) discards a `Retry` and
/// waits 1 s before it redelivers a `Defer`, on every backend that supports
/// broadcast. A [`BatchConsumer`](crate::BatchConsumer) redelivers the whole
/// batch for a `Retry` or a `Defer`, with no per-batch retry budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    /// Processed successfully. Remove from queue.
    Ack,

    /// Transient failure. Route to a hold queue for delayed retry.
    /// Increments the retry counter.
    ///
    /// The consumer picks the hold queue matching the current retry count,
    /// giving automatic escalating backoff when multiple hold queues are
    /// defined. Once the retry count exceeds the number of hold queues,
    /// messages keep going to the last (longest-delay) hold queue until
    /// [`ConsumerOptions::max_retries`](crate::ConsumerOptions::max_retries)
    /// is exhausted, at which point the message is routed to the DLQ.
    ///
    /// If no hold queues are configured, Kafka and NATS redeliver after a
    /// fixed 1 s and the other backends redeliver with no delay. The
    /// mechanism, the retry-count effect and the warnings are listed per
    /// backend under "Without hold queues" on [`Outcome`].
    Retry,

    /// Permanent failure. Route to DLQ without retrying.
    ///
    /// If the topic has no DLQ configured, the message is discarded (nack
    /// without requeue) and a warning is logged.
    Reject,

    /// Delay and re-deliver via hold queue WITHOUT incrementing the retry
    /// counter. Always routes to `hold_queues[0]` (shortest delay),
    /// regardless of retry count — no escalating backoff.
    ///
    /// Unlike [`Retry`], deferred messages never exhaust `max_retries` and
    /// will never be routed to the DLQ as a result of deferral alone.
    ///
    /// # When to use
    ///
    /// - **Scheduled delivery** — the message carries a "not-before" timestamp
    ///   and the time hasn't arrived yet.
    /// - **Missing dependency** — a prerequisite event or resource isn't
    ///   available yet (e.g. waiting for a payment to clear before fulfilling
    ///   an order).
    /// - **External rate-limiting** — an upstream API returned 429 / "retry
    ///   after"; the message itself is valid, you just can't act on it now.
    /// - **Temporal ordering without FIFO** — the message arrived before a
    ///   prerequisite; hold it without penalty until the dependency appears.
    ///
    /// In all these cases the message is not *failing* — it is simply *not
    /// ready*. Using `Retry` would burn retry budget and eventually send a
    /// perfectly valid message to the DLQ.
    ///
    /// # Caution: infinite defer loops
    ///
    /// Because `Defer` never increments the retry counter, a handler that
    /// always returns `Defer` will bounce the message between the main queue
    /// and `hold_queues[0]` **indefinitely**. There is no built-in circuit
    /// breaker. Ensure your handler has a condition that eventually resolves
    /// to `Ack`, `Retry`, or `Reject`.
    ///
    /// If no hold queues are configured, `Defer` falls back the way
    /// [`Retry`](Outcome::Retry) does. Kafka and NATS redeliver after a fixed
    /// 1 s and the other backends redeliver with no delay. Which backends log
    /// a warning is listed under "Without hold queues" on [`Outcome`].
    ///
    /// # Sequenced (FIFO) consumers
    ///
    /// `Defer` is not supported on sequenced consumers because it violates
    /// ordering guarantees. If a handler returns `Defer` on a sequenced
    /// consumer, it is treated as [`Retry`] and a warning is logged.
    Defer,
}
