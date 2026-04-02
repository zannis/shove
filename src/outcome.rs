use serde::{Deserialize, Serialize};

/// What a handler instructs the consumer to do with a message after processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    /// Processed successfully. Remove from queue.
    Ack,

    /// Transient failure. Route to a hold queue for delayed retry.
    /// Increments the retry counter.
    ///
    /// The consumer picks the hold queue based on retry count:
    /// `hold_queues[min(retry_count, len - 1)]` — this gives automatic
    /// escalating backoff when multiple hold queues are defined.
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
    /// Use for messages that cannot be processed yet (e.g., scheduled for
    /// the future) but should not count against `max_retries`.
    ///
    /// If no hold queues are configured, falls back to nack-with-requeue
    /// (broker-level redelivery with no delay) and a warning is logged.
    Defer,
}
