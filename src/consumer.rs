use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, ConsumerOptionsInner};
use crate::error::{Result, ShoveError};
#[cfg(feature = "nats")]
use crate::markers::Nats;
#[cfg(feature = "rabbitmq-transactional")]
use crate::markers::RabbitMq;
#[cfg(feature = "aws-sns-sqs")]
use crate::markers::Sqs;

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

/// Options for consumer behavior, parameterised by backend marker `B`.
///
/// Shared knobs (retries, prefetch, timeouts, size limits) live on the generic
/// struct; backend-specific knobs (`exactly_once` for RabbitMQ,
/// `receive_batch_size` for SQS, `max_ack_pending` for NATS) are still stored
/// on the struct but set via feature-gated inherent-impl blocks so users can
/// only reach them on the appropriate backend.
pub struct ConsumerOptions<B: Backend> {
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
    pub max_retries: u32,
    /// Prefetch count (number of unacked messages the broker will deliver).
    ///
    /// Note: when [`concurrent_processing`](Self::concurrent_processing) is
    /// `false`, [`into_inner`](Self::into_inner) clamps the effective
    /// prefetch to `1` regardless of this value, so the consumer processes
    /// one message at a time.
    pub prefetch_count: u16,
    /// Process prefetched messages concurrently (`true`, default) or one at
    /// a time (`false`). When `false`, [`into_inner`](Self::into_inner)
    /// clamps [`prefetch_count`](Self::prefetch_count) to `1` on the way
    /// across the backend trait boundary.
    ///
    /// Semantically equivalent to the per-backend
    /// `ConsumerGroupConfig::with_concurrent_processing` setter; set this
    /// flag via [`ConsumerOptions::with_concurrent_processing`] when
    /// registering a handler through [`ConsumerSupervisor`] rather than a
    /// coordinated group.
    ///
    /// [`ConsumerSupervisor`]: crate::consumer_supervisor::ConsumerSupervisor
    pub concurrent_processing: bool,
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
    /// Opt in via the feature-gated
    /// [`ConsumerOptions::<RabbitMq>::with_exactly_once`] builder.
    #[cfg(feature = "rabbitmq-transactional")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq-transactional")))]
    pub exactly_once: bool,
    /// Number of messages to request per SQS `ReceiveMessage` poll, independent
    /// of how many handlers may run concurrently (`prefetch_count`).
    ///
    /// Zero means "use `prefetch_count`" (the default).
    #[cfg(feature = "aws-sns-sqs")]
    #[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
    pub receive_batch_size: u16,
    /// Override for JetStream `max_ack_pending` on the durable consumer.
    ///
    /// `None` means use `prefetch_count` (the default for standalone consumers).
    #[cfg(feature = "nats")]
    #[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
    pub max_ack_pending: Option<i64>,

    // Runtime coordination — crate-private.
    pub(crate) shutdown: Option<CancellationToken>,
    pub(crate) processing: Arc<AtomicBool>,
    pub(crate) consumer_group: Option<Arc<str>>,

    _backend: PhantomData<fn() -> B>,
}

impl<B: Backend> ConsumerOptions<B> {
    /// Create consumer options with library-wide defaults.
    pub fn new() -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            concurrent_processing: true,
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
            shutdown: None,
            processing: Arc::new(AtomicBool::new(false)),
            consumer_group: None,
            _backend: PhantomData,
        }
    }

    /// Shorthand for `ConsumerOptions::new().with_prefetch_count(prefetch)`.
    pub fn preset(prefetch: u16) -> Self {
        Self::new().with_prefetch_count(prefetch)
    }

    /// Set the maximum number of retries before rejecting to DLQ.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the prefetch count (number of unacked messages the broker will deliver).
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Process prefetched messages concurrently (`true`) or one at a time
    /// (`false`). When `false`, `prefetch_count` is clamped to `1` on the
    /// way to the backend, matching the semantic of the per-backend
    /// `ConsumerGroupConfig::with_concurrent_processing` setter.
    pub fn with_concurrent_processing(mut self, on: bool) -> Self {
        self.concurrent_processing = on;
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

    /// Attach a shutdown token. Supervisors/groups call this at `.register()`
    /// time so application code does not have to thread tokens manually.
    /// Direct `consumer.run()` call sites (primarily tests that bypass the
    /// supervisor) may use this explicitly.
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Tag this consumer with a group name for metrics labelling. Group
    /// registries set this automatically; `ConsumerSupervisor` leaves it
    /// unset (which surfaces as `consumer_group="default"` in metrics).
    pub fn with_consumer_group(mut self, name: impl Into<Arc<str>>) -> Self {
        self.consumer_group = Some(name.into());
        self
    }

    /// Clone of the internal "is currently processing" flag. Primarily useful
    /// for tests that want to block until a consumer starts handling a
    /// message; production code observes this through the autoscaler.
    pub fn processing_handle(&self) -> Arc<AtomicBool> {
        self.processing.clone()
    }

    /// Lower to the internal options struct for passing across the Backend
    /// trait boundary. When [`concurrent_processing`](Self::concurrent_processing)
    /// is `false`, the effective prefetch is clamped to `1` so the consumer
    /// processes one message at a time — matching the semantic of the
    /// per-backend `ConsumerGroupConfig::with_concurrent_processing`.
    pub(crate) fn into_inner(self) -> ConsumerOptionsInner {
        let effective_prefetch = if self.concurrent_processing {
            self.prefetch_count
        } else {
            1
        };
        ConsumerOptionsInner {
            max_retries: self.max_retries,
            prefetch_count: effective_prefetch,
            handler_timeout: self.handler_timeout,
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            shutdown: self.shutdown.unwrap_or_default(),
            processing: self.processing,
            consumer_group: self.consumer_group,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: self.exactly_once,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: self.receive_batch_size,
            #[cfg(feature = "nats")]
            max_ack_pending: self.max_ack_pending,
        }
    }
}

impl<B: Backend> Default for ConsumerOptions<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Backend> Clone for ConsumerOptions<B> {
    fn clone(&self) -> Self {
        Self {
            max_retries: self.max_retries,
            prefetch_count: self.prefetch_count,
            concurrent_processing: self.concurrent_processing,
            handler_timeout: self.handler_timeout,
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: self.exactly_once,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: self.receive_batch_size,
            #[cfg(feature = "nats")]
            max_ack_pending: self.max_ack_pending,
            shutdown: self.shutdown.clone(),
            processing: self.processing.clone(),
            consumer_group: self.consumer_group.clone(),
            _backend: PhantomData,
        }
    }
}

// -- Backend-specific builders ---------------------------------------------

#[cfg(feature = "aws-sns-sqs")]
#[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
impl ConsumerOptions<Sqs> {
    /// Number of messages requested per SQS `ReceiveMessage` poll.
    ///
    /// Zero (the default) means "use `prefetch_count`".
    pub fn with_receive_batch_size(mut self, n: u16) -> Self {
        self.receive_batch_size = n;
        self
    }
}

#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
impl ConsumerOptions<Nats> {
    /// Override the durable consumer's `max_ack_pending`.
    pub fn with_max_ack_pending(mut self, n: i64) -> Self {
        self.max_ack_pending = Some(n);
        self
    }
}

#[cfg(feature = "rabbitmq-transactional")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq-transactional")))]
impl ConsumerOptions<RabbitMq> {
    /// Enable exactly-once delivery via AMQP transactions.
    ///
    /// See [`ConsumerOptions::exactly_once`] for the full trade-off description.
    pub fn with_exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }
}

#[cfg(test)]
#[allow(clippy::absolute_paths)]
mod tests {
    use super::*;
    use crate::markers::*;

    // Tests use the InMemory marker when available; otherwise fall back to
    // any enabled backend marker.
    #[cfg(feature = "inmemory")]
    type TestBackend = InMemory;

    #[cfg(all(not(feature = "inmemory"), feature = "kafka"))]
    type TestBackend = Kafka;

    #[cfg(all(not(feature = "inmemory"), not(feature = "kafka"), feature = "nats"))]
    type TestBackend = Nats;

    #[cfg(all(
        not(feature = "inmemory"),
        not(feature = "kafka"),
        not(feature = "nats"),
        feature = "rabbitmq"
    ))]
    type TestBackend = RabbitMq;

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn defaults_are_correct() {
        let opts = ConsumerOptions::<TestBackend>::new();
        assert_eq!(opts.max_retries, 10);
        assert_eq!(opts.prefetch_count, 10);
        assert!(opts.concurrent_processing);
        assert_eq!(opts.handler_timeout, Some(DEFAULT_HANDLER_TIMEOUT));
        assert_eq!(opts.max_pending_per_key, Some(DEFAULT_MAX_PENDING_PER_KEY));
        assert_eq!(opts.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        assert!(!opts.processing.load(std::sync::atomic::Ordering::Acquire));
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_concurrent_processing_toggles_flag() {
        let opts = ConsumerOptions::<TestBackend>::new().with_concurrent_processing(false);
        assert!(!opts.concurrent_processing);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn into_inner_clamps_prefetch_when_non_concurrent() {
        let inner = ConsumerOptions::<TestBackend>::new()
            .with_prefetch_count(32)
            .with_concurrent_processing(false)
            .into_inner();
        assert_eq!(
            inner.prefetch_count, 1,
            "prefetch must clamp to 1 when concurrent_processing=false"
        );
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn into_inner_preserves_prefetch_when_concurrent() {
        let inner = ConsumerOptions::<TestBackend>::new()
            .with_prefetch_count(32)
            .into_inner();
        assert_eq!(inner.prefetch_count, 32);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_max_retries_overrides() {
        let opts = ConsumerOptions::<TestBackend>::new().with_max_retries(5);
        assert_eq!(opts.max_retries, 5);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_prefetch_count_overrides() {
        let opts = ConsumerOptions::<TestBackend>::new().with_prefetch_count(50);
        assert_eq!(opts.prefetch_count, 50);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_handler_timeout_sets_timeout() {
        let opts =
            ConsumerOptions::<TestBackend>::new().with_handler_timeout(Duration::from_secs(30));
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(30)));
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn builder_chains() {
        let opts = ConsumerOptions::<TestBackend>::new()
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

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn preset_sets_prefetch() {
        let opts = ConsumerOptions::<TestBackend>::preset(42);
        assert_eq!(opts.prefetch_count, 42);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_max_pending_per_key_sets_value() {
        let opts = ConsumerOptions::<TestBackend>::new().with_max_pending_per_key(50);
        assert_eq!(opts.max_pending_per_key, Some(50));
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_max_message_size_overrides_default() {
        let opts = ConsumerOptions::<TestBackend>::new().with_max_message_size(1024 * 1024);
        assert_eq!(opts.max_message_size, Some(1024 * 1024));
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn without_message_size_limit_disables_check() {
        let opts = ConsumerOptions::<TestBackend>::new().without_message_size_limit();
        assert_eq!(opts.max_message_size, None);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn exactly_once_defaults_to_false() {
        let opts = ConsumerOptions::<RabbitMq>::new();
        assert!(!opts.exactly_once);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn with_exactly_once_sets_flag() {
        let opts = ConsumerOptions::<RabbitMq>::new().with_exactly_once();
        assert!(opts.exactly_once);
    }

    #[cfg(feature = "rabbitmq-transactional")]
    #[test]
    fn exactly_once_chains_with_other_builders() {
        let opts = ConsumerOptions::<RabbitMq>::new()
            .with_max_retries(5)
            .with_exactly_once()
            .with_prefetch_count(1);
        assert!(opts.exactly_once);
        assert_eq!(opts.max_retries, 5);
        assert_eq!(opts.prefetch_count, 1);
    }

    #[test]
    fn validate_message_size_accepts_payload_at_limit() {
        assert!(validate_message_size(100, Some(100)).is_ok());
    }

    #[test]
    fn validate_message_size_accepts_payload_under_limit() {
        assert!(validate_message_size(99, Some(100)).is_ok());
    }

    #[test]
    fn validate_message_size_rejects_oversize_payload() {
        let err = validate_message_size(101, Some(100)).unwrap_err();
        let ShoveError::Validation(msg) = err else {
            panic!("expected Validation variant");
        };
        assert!(msg.contains("101"));
        assert!(msg.contains("100"));
    }

    #[test]
    fn validate_message_size_skips_check_when_limit_absent() {
        assert!(validate_message_size(usize::MAX, None).is_ok());
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_shutdown_stores_token() {
        let token = CancellationToken::new();
        let opts = ConsumerOptions::<TestBackend>::new().with_shutdown(token.clone());
        let inner = opts.into_inner();
        // into_inner picks up the provided token instead of a fresh default.
        token.cancel();
        assert!(inner.shutdown.is_cancelled());
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn into_inner_without_shutdown_yields_fresh_token() {
        let inner = ConsumerOptions::<TestBackend>::new().into_inner();
        assert!(!inner.shutdown.is_cancelled());
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn processing_handle_is_a_shared_view() {
        use std::sync::atomic::Ordering;
        let opts = ConsumerOptions::<TestBackend>::new();
        let handle = opts.processing_handle();
        handle.store(true, Ordering::Release);
        let inner = opts.into_inner();
        assert!(inner.processing.load(Ordering::Acquire));
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn clone_preserves_all_settings() {
        let opts = ConsumerOptions::<TestBackend>::new()
            .with_max_retries(7)
            .with_prefetch_count(13)
            .with_concurrent_processing(false)
            .with_handler_timeout(Duration::from_secs(11))
            .with_max_pending_per_key(99)
            .with_max_message_size(4096);
        let copy = opts.clone();
        assert_eq!(copy.max_retries, 7);
        assert_eq!(copy.prefetch_count, 13);
        assert!(!copy.concurrent_processing);
        assert_eq!(copy.handler_timeout, Some(Duration::from_secs(11)));
        assert_eq!(copy.max_pending_per_key, Some(99));
        assert_eq!(copy.max_message_size, Some(4096));
    }

    #[cfg(feature = "aws-sns-sqs")]
    #[test]
    fn sqs_with_receive_batch_size_sets_value() {
        let opts = ConsumerOptions::<Sqs>::new().with_receive_batch_size(7);
        assert_eq!(opts.receive_batch_size, 7);
    }

    #[cfg(feature = "nats")]
    #[test]
    fn nats_with_max_ack_pending_sets_value() {
        let opts = ConsumerOptions::<Nats>::new().with_max_ack_pending(128);
        assert_eq!(opts.max_ack_pending, Some(128));
    }
}
