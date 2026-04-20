use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};

/// Default maximum message payload size: 10 MiB.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Default handler timeout: 30 seconds.
pub const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(30);

/// Default per-key pending buffer limit for sequenced consumers.
pub const DEFAULT_MAX_PENDING_PER_KEY: usize = 1_000;

/// Validates that `len` does not exceed the optional `max` limit.
///
/// Used by [`ConsumerOptions::validate_payload_message_size`] and directly by
/// backends that destructure options before entering reconnect closures.
pub(crate) fn validate_message_size(len: usize, max: Option<usize>) -> Result<()> {
    match max {
        Some(max) if len > max => Err(ShoveError::Validation(format!(
            "message size {len} exceeds max_message_size {max}"
        ))),
        _ => Ok(()),
    }
}

/// Options for consumer behavior.
#[derive(Clone)]
pub struct ConsumerOptions {
    /// Maximum retries before automatically rejecting to DLQ.
    ///
    /// Each time a handler returns [`Outcome::Retry`](crate::Outcome::Retry),
    /// the retry counter increments and the message is routed to a hold queue
    /// selected by `hold_queues[min(retry_count, len - 1)]`. This gives
    /// escalating backoff when multiple hold queues are defined — once the
    /// counter exceeds the number of hold queues, retries keep using the
    /// last (longest-delay) hold queue.
    ///
    /// When `retry_count >= max_retries`, the message is sent to the DLQ
    /// instead of another hold queue.
    ///
    /// # Example
    ///
    /// With `max_retries = 5` and hold queues `[1s, 5s, 30s]`:
    ///
    /// | retry | hold queue | delay |
    /// |-------|-----------|-------|
    /// | 0     | `[0]`     | 1s    |
    /// | 1     | `[1]`     | 5s    |
    /// | 2     | `[2]`     | 30s   |
    /// | 3     | `[2]`     | 30s   |
    /// | 4     | `[2]`     | 30s   |
    /// | 5     | — DLQ —   |       |
    pub max_retries: u32,
    /// Prefetch count (number of unacked messages the broker will deliver).
    pub prefetch_count: u16,
    /// Cancellation token for graceful shutdown. When triggered, the consumer
    /// finishes processing the current message, acks it, and returns `Ok(())`.
    pub shutdown: CancellationToken,
    /// Flag that indicates the consumer is currently processing a message.
    /// Used by the autoscaler to avoid scaling down busy consumers.
    pub processing: Arc<AtomicBool>,
    /// Maximum time a handler may spend processing a single message.
    /// If the handler exceeds this duration the message is retried.
    ///
    /// Default: [`DEFAULT_HANDLER_TIMEOUT`] (30 s). Set to `None` to disable.
    pub handler_timeout: Option<Duration>,
    /// Maximum number of locally buffered messages per sequence key in
    /// concurrent-sequenced consumers. When the limit is reached, new
    /// deliveries for that key are rejected to the DLQ.
    ///
    /// Default: [`DEFAULT_MAX_PENDING_PER_KEY`] (1 000). Set to `None` to
    /// disable.
    pub max_pending_per_key: Option<usize>,
    /// Maximum allowed message payload size in bytes. Messages exceeding this
    /// limit are rejected to the DLQ (or discarded in DLQ consumers) **before**
    /// deserialization, preventing JSON-bomb OOM attacks.
    ///
    /// Default: [`DEFAULT_MAX_MESSAGE_SIZE`] (10 MiB). Set to `None` to
    /// disable the check.
    pub max_message_size: Option<usize>,
    /// Enable exactly-once delivery via AMQP transactions (RabbitMQ only).
    ///
    /// Requires the `rabbitmq-transactional` Cargo feature. When enabled, the
    /// consumer channel is put in AMQP transaction mode (`tx_select`). Every
    /// routing decision (retry, defer, ack, reject) is wrapped in a `tx_commit`,
    /// making publish-to-hold-queue and ack/nack of the original delivery
    /// **atomic**. This eliminates the publish-then-ack race that can produce a
    /// duplicate delivery under at-least-once semantics.
    ///
    /// **Trade-off**: AMQP transactions disable publisher confirms and add a
    /// round-trip per message. Expect roughly 10–15× lower throughput per channel
    /// compared to the default confirm mode. Use [`ConsumerOptions::with_exactly_once`]
    /// to opt in.
    #[cfg(feature = "rabbitmq-transactional")]
    pub exactly_once: bool,
    /// Number of messages to request per SQS `ReceiveMessage` poll, independent
    /// of how many handlers may run concurrently (`prefetch_count`).
    ///
    /// When non-zero, the SQS consumer fetches this many messages per API call
    /// and buffers them locally, dispatching them to handlers one-by-one (serial
    /// mode) or in parallel (concurrent mode) up to `prefetch_count` at a time.
    /// This allows batching SQS receives even in non-concurrent mode, reducing
    /// API call overhead significantly when multiple consumers share the same queue.
    ///
    /// Zero means "use `prefetch_count`" (the default).
    #[cfg(feature = "aws-sns-sqs")]
    pub(crate) receive_batch_size: u16,
    /// Override for JetStream `max_ack_pending` on the durable consumer.
    ///
    /// When multiple consumer tasks share a single JetStream durable consumer
    /// (as in consumer groups), `max_ack_pending` must account for the total
    /// in-flight capacity across all tasks — not just the per-task prefetch.
    /// `None` means use `prefetch_count` (the default for standalone consumers).
    #[cfg(feature = "nats")]
    pub(crate) max_ack_pending: Option<i64>,
}

impl ConsumerOptions {
    /// Create consumer options with the given shutdown token.
    /// Uses defaults: `max_retries = 10`, `prefetch_count = 10`.
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            shutdown,
            processing: Arc::new(AtomicBool::new(false)),
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        }
    }

    /// Set the maximum number of retries before rejecting to DLQ.
    ///
    /// This controls the total retry budget, independent of how many hold
    /// queues are configured. See [`max_retries`](Self::max_retries) for
    /// details on how retries escalate through hold queues.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the prefetch count (number of unacked messages the broker will deliver).
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Set the handler timeout. If a handler exceeds this duration the message
    /// is retried automatically.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }

    /// Disable the handler timeout entirely (handlers may run indefinitely).
    pub fn without_handler_timeout(mut self) -> Self {
        self.handler_timeout = None;
        self
    }

    /// Set the maximum number of locally buffered messages per sequence key.
    /// When exceeded, new deliveries for that key are rejected to the DLQ.
    pub fn with_max_pending_per_key(mut self, limit: usize) -> Self {
        self.max_pending_per_key = Some(limit);
        self
    }

    /// Disable the per-key pending buffer limit entirely (unbounded).
    pub fn without_max_pending_per_key(mut self) -> Self {
        self.max_pending_per_key = None;
        self
    }

    /// Set the maximum allowed message payload size in bytes.
    /// Messages exceeding this limit are rejected before deserialization.
    pub fn with_max_message_size(mut self, max: usize) -> Self {
        self.max_message_size = Some(max);
        self
    }

    /// Disable the message size limit entirely.
    pub fn without_message_size_limit(mut self) -> Self {
        self.max_message_size = None;
        self
    }

    /// Returns `Ok(())` if the payload is within the configured
    /// [`max_message_size`](Self::max_message_size), or an error if it
    /// exceeds the limit. Always succeeds when no limit is set.
    pub(crate) fn validate_payload_message_size(&self, len: usize) -> Result<()> {
        validate_message_size(len, self.max_message_size)
    }

    /// Attach an external shutdown token. Harnesses call this during `.register()`.
    pub(crate) fn with_shutdown_token(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Lower to the internal options struct for passing across the Backend trait boundary.
    pub(crate) fn into_inner(self) -> crate::backend::ConsumerOptionsInner {
        crate::backend::ConsumerOptionsInner {
            max_retries: self.max_retries,
            prefetch_count: self.prefetch_count,
            concurrent_processing: true, // Phase 12 adds the field to ConsumerOptions
            handler_timeout: self.handler_timeout,
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            shutdown: self.shutdown.clone(),
            processing: self.processing.clone(),
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: self.exactly_once,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: self.receive_batch_size,
            #[cfg(feature = "nats")]
            max_ack_pending: self.max_ack_pending,
        }
    }

    /// Enable exactly-once delivery via AMQP transactions.
    ///
    /// Requires the `rabbitmq-transactional` Cargo feature. See
    /// [`ConsumerOptions::exactly_once`] for the full trade-off description.
    ///
    /// # Example
    /// ```rust,ignore
    /// let options = ConsumerOptions::new(shutdown)
    ///     .with_exactly_once();
    /// ```
    #[cfg(feature = "rabbitmq-transactional")]
    pub fn with_exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_correct() {
        let opts = ConsumerOptions::new(CancellationToken::new());
        assert_eq!(opts.max_retries, 10);
        assert_eq!(opts.prefetch_count, 10);
        assert_eq!(opts.handler_timeout, Some(DEFAULT_HANDLER_TIMEOUT));
        assert_eq!(opts.max_pending_per_key, Some(DEFAULT_MAX_PENDING_PER_KEY));
        assert_eq!(opts.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        assert!(!opts.processing.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn with_max_retries_overrides() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_max_retries(5);
        assert_eq!(opts.max_retries, 5);
    }

    #[test]
    fn with_prefetch_count_overrides() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_prefetch_count(50);
        assert_eq!(opts.prefetch_count, 50);
    }

    #[test]
    fn with_handler_timeout_sets_timeout() {
        let opts = ConsumerOptions::new(CancellationToken::new())
            .with_handler_timeout(Duration::from_secs(30));
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn builder_chains() {
        let opts = ConsumerOptions::new(CancellationToken::new())
            .with_max_retries(3)
            .with_prefetch_count(20)
            .with_handler_timeout(Duration::from_secs(5))
            .with_max_pending_per_key(100)
            .with_max_message_size(5 * 1024 * 1024);
        assert_eq!(opts.max_retries, 3);
        assert_eq!(opts.prefetch_count, 20);
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(5)));
        assert_eq!(opts.max_pending_per_key, Some(100));
        assert_eq!(opts.max_message_size, Some(5 * 1024 * 1024));
    }

    #[test]
    fn shutdown_token_propagated() {
        let token = CancellationToken::new();
        let opts = ConsumerOptions::new(token.clone());
        assert!(!opts.shutdown.is_cancelled());
        token.cancel();
        assert!(opts.shutdown.is_cancelled());
    }

    #[test]
    fn with_max_pending_per_key_sets_value() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_max_pending_per_key(50);
        assert_eq!(opts.max_pending_per_key, Some(50));
    }

    #[test]
    fn with_max_message_size_overrides_default() {
        let opts =
            ConsumerOptions::new(CancellationToken::new()).with_max_message_size(1024 * 1024);
        assert_eq!(opts.max_message_size, Some(1024 * 1024));
    }

    #[test]
    fn without_message_size_limit_disables_check() {
        let opts = ConsumerOptions::new(CancellationToken::new()).without_message_size_limit();
        assert_eq!(opts.max_message_size, None);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn exactly_once_defaults_to_false() {
        let opts = ConsumerOptions::new(CancellationToken::new());
        assert!(!opts.exactly_once);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn with_exactly_once_sets_flag() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_exactly_once();
        assert!(opts.exactly_once);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn exactly_once_chains_with_other_builders() {
        let opts = ConsumerOptions::new(CancellationToken::new())
            .with_max_retries(5)
            .with_exactly_once()
            .with_prefetch_count(1);
        assert!(opts.exactly_once);
        assert_eq!(opts.max_retries, 5);
        assert_eq!(opts.prefetch_count, 1);
    }
}
