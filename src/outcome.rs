use serde::{Deserialize, Serialize};

/// What a handler instructs the consumer to do with a message after processing.
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
    /// If no hold queues are configured, the message is nacked with requeue
    /// (broker-level redelivery with no delay).
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
    /// If no hold queues are configured, falls back to nack-with-requeue
    /// (broker-level redelivery with no delay) and a warning is logged.
    Defer,
}
