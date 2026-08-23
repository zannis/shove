use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, ConsumerOptionsInner};
use crate::error::{Result, ShoveError};
#[cfg(feature = "kafka")]
use crate::markers::Kafka;
#[cfg(feature = "nats")]
use crate::markers::Nats;
#[cfg(feature = "rabbitmq")]
use crate::markers::RabbitMq;
#[cfg(feature = "aws-sns-sqs")]
use crate::markers::Sqs;
use crate::outcome::Outcome;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};

/// Default maximum message payload size: 10 MiB.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Default handler timeout: 30 seconds.
///
/// Applied when [`ConsumerOptions::handler_timeout`] is left unset. This is the
/// value downstream code should reference — not a hardcoded `30`— when it needs
/// to keep its own internal deadline below shove's, which is the usual way to
/// make a stalled handler resolve to a deliberate [`Outcome`] instead of being
/// cancelled mid-flight:
///
/// ```
/// use shove::DEFAULT_HANDLER_TIMEOUT;
/// const SUBMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
/// const _: () = assert!(SUBMIT_TIMEOUT.as_secs() < DEFAULT_HANDLER_TIMEOUT.as_secs());
/// ```
///
/// Treat the constant as stable: it is part of the public API and a change to
/// it is a breaking change, so an assertion like the one above will fail at
/// compile time rather than silently invert the ordering.
///
/// If you are wrapping handlers in an internal timeout purely to avoid shove
/// turning a slow handler into a budget-burning retry, prefer
/// [`ConsumerOptions::with_handler_timeout_outcome`] instead.
pub const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(30);

/// Two-state used by each backend's `ConsumerGroupConfig` so the
/// registry can tell "user explicitly set a timeout" from "config
/// left at default" and apply a registry-level default only in the
/// latter case. Resolved into a `Duration` at registration time by
/// [`resolve_handler_timeout`].
#[allow(dead_code)] // every consumer is feature-gated; dead under --no-default-features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum HandlerTimeoutConfig {
    /// Use the registry default if set; otherwise [`DEFAULT_HANDLER_TIMEOUT`].
    #[default]
    Inherit,
    /// Use this exact duration.
    Set(Duration),
}

/// Resolve a per-config [`HandlerTimeoutConfig`] against an optional
/// registry-level default, producing the effective `Duration` that is
/// plumbed into `ConsumerOptionsInner.handler_timeout` (wrapped in
/// `Some` by the caller).
#[allow(dead_code)] // every consumer is feature-gated; dead under --no-default-features
pub(crate) fn resolve_handler_timeout(
    config: HandlerTimeoutConfig,
    registry_default: Option<Duration>,
) -> Duration {
    match config {
        HandlerTimeoutConfig::Set(d) => d,
        HandlerTimeoutConfig::Inherit => registry_default.unwrap_or(DEFAULT_HANDLER_TIMEOUT),
    }
}

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
    /// What a handler timeout resolves to. `None` keeps each backend's
    /// historical default — see
    /// [`with_handler_timeout_outcome`](Self::with_handler_timeout_outcome).
    pub handler_timeout_outcome: Option<Outcome>,
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
    /// Maximum number of reconnect attempts before the consumer gives up and
    /// returns an error.
    ///
    /// Each connection-level failure (broker unreachable, channel error, etc.)
    /// increments the attempt counter. Once the counter reaches this limit the
    /// consumer emits a `tracing::error!` and propagates
    /// `ShoveError::Connection` to the caller (typically the supervisor, which
    /// may then restart the consumer or surface the error).
    ///
    /// `None` (the default) means unlimited retries — the consumer will keep
    /// backing off and retrying until it succeeds or is shut down. Use a
    /// finite value in environments where a permanently-unreachable broker
    /// should be surfaced rather than silently spinning.
    ///
    /// # Example
    ///
    /// ```ignore
    /// ConsumerOptions::<MyBackend>::new().with_max_reconnect_attempts(50)
    /// ```
    pub max_reconnect_attempts: Option<u32>,
    /// Maximum time a sequence key may remain in the `AwaitingRetry` state
    /// before its pending deliveries are dead-lettered (RabbitMQ sequenced
    /// consumers only).
    ///
    /// When a handler returns `Retry` or `Defer`, the key is blocked until
    /// the hold-queue message comes back. If the hold queue has a longer TTL
    /// than expected (or is misconfigured), the key stays blocked indefinitely.
    /// Setting this timeout causes the consumer to dead-letter any buffered
    /// deliveries for the key and unblock it, preventing consumer stalls.
    ///
    /// `None` (the default) disables the eviction — keys stay in
    /// `AwaitingRetry` until the retry message arrives.
    #[cfg(feature = "rabbitmq")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
    pub hold_queue_timeout: Option<Duration>,

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

    /// Kafka-only: Schema Registry client for decoding Confluent wire-framed
    /// messages. `None` disables registry-based decoding.
    #[cfg(feature = "kafka-schema-registry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "kafka-schema-registry")))]
    pub schema_registry: Option<Arc<SchemaRegistry>>,
    /// Kafka-only: how subject mismatches are handled. Default `Enforce`.
    #[cfg(feature = "kafka-schema-registry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "kafka-schema-registry")))]
    pub schema_enforcement: SchemaEnforcement,
    /// Kafka-only: accepted subject set. `None` derives `["{queue}-value"]` at
    /// decode time.
    #[cfg(feature = "kafka-schema-registry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "kafka-schema-registry")))]
    pub schema_accepted_subjects: Option<Vec<Arc<str>>>,

    /// Kafka-only: base consumer `group.id` override. `None` (the default)
    /// keeps the topic-derived ids. Set via
    /// [`ConsumerOptions::<Kafka>::with_group_id`].
    #[cfg(feature = "kafka")]
    #[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
    pub kafka_group_id: Option<Arc<str>>,

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
            handler_timeout_outcome: None,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            max_reconnect_attempts: None,
            #[cfg(feature = "rabbitmq")]
            hold_queue_timeout: None,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: None,
            #[cfg(feature = "kafka")]
            kafka_group_id: None,
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
    /// is retried automatically. Panics if `timeout` is zero.
    ///
    /// On the Redis backend the timeout also drives crash recovery: a
    /// background sidecar reclaims entries that have been pending longer
    /// than the timeout. Reclaim applies to the whole consumer group, so
    /// configure the same timeout for every consumer of a given stream and
    /// group, including consumers in other processes. Within one process,
    /// mixed settings are reconciled conservatively (the longest timeout
    /// wins; a disabled timeout disables reclaim); across processes each
    /// process acts on its own setting.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = Some(timeout);
        self
    }

    /// Choose what a handler timeout resolves to, instead of each backend's
    /// default.
    ///
    /// A timeout is the library's verdict on a *slow consumer*, not evidence
    /// that the message itself is bad, but the two are indistinguishable from
    /// the outside — so the choice belongs to the caller:
    ///
    /// - [`Outcome::Retry`] — the historical default on every backend except
    ///   Redis. Consumes retry budget, so a persistently slow handler
    ///   eventually dead-letters (or, with no DLQ declared, is discarded).
    /// - [`Outcome::Defer`] — redeliver via `hold_queues[0]` **without**
    ///   consuming retry budget, so a slow handler never dead-letters a valid
    ///   message. Note [`Outcome::Defer`]'s infinite-loop caveat: nothing
    ///   bounds the redeliveries.
    ///
    ///   On a **sequenced** consumer this inherits `Defer`'s existing
    ///   backend-specific handling, which is not uniform: Kafka, NATS,
    ///   SNS/SQS and the in-memory backend downgrade `Defer` to
    ///   [`Outcome::Retry`] with a warning, so a persistently slow handler
    ///   still exhausts its budget there; RabbitMQ's sharded consumer and
    ///   Redis Streams route it to a hold queue without incrementing the
    ///   retry count, so it can defer indefinitely. Choose `Defer` for a
    ///   sequenced consumer only if that split is acceptable — on the four
    ///   backends that downgrade, this option buys you nothing.
    /// - [`Outcome::Reject`] — treat a timeout as terminal and dead-letter on
    ///   the first occurrence, consuming no retry budget.
    /// - [`Outcome::Ack`] — drop the message. The handler is cancelled
    ///   mid-flight, so any work it had not yet committed is lost.
    ///
    /// Leaving this unset preserves current behaviour exactly. On Redis
    /// Streams the default is not an outcome at all: a timed-out entry is left
    /// in the PEL and reclaimed by `XAUTOCLAIM` after the idle deadline, which
    /// redelivers without consuming retry budget. Setting this option makes
    /// Redis route the given outcome instead, which for [`Outcome::Defer`]
    /// means the configured hold-queue delay rather than the idle deadline.
    ///
    /// Combined with [`without_handler_timeout`](Self::without_handler_timeout)
    /// this setting has nothing to act on and is inert: no handler timeout ever
    /// fires. It is deliberately *not* borrowed by the shutdown drain's own
    /// backstop on RabbitMQ and SNS/SQS either — that backstop bounds shutdown,
    /// not the handler, and with deadlines disabled it can expire while the
    /// handler is still running, so it stays [`Outcome::Retry`] and lets the
    /// broker redeliver rather than retiring live work.
    ///
    /// # Redis: this narrows a race, it does not remove one
    ///
    /// With the option unset, a timed-out Redis entry has exactly one actor —
    /// the reaper. Setting it makes the consumer act at that same deadline, so
    /// the consumer and any `XAUTOCLAIM` sweep can both touch the entry.
    ///
    /// Two things keep them apart. Within a process, the maintenance registry
    /// backs its sweep threshold off to twice the handler timeout, which is a
    /// real guarantee because the registry sees every consumer there. Across
    /// processes, the consumer holds a *lease* on the entry it is working on —
    /// renewed at half the handler timeout, which keeps the entry's idle clock
    /// below a foreign sweep threshold — and re-checks that lease before
    /// applying any outcome, dropping the outcome if a reaper got there first.
    ///
    /// What that buys you, stated without varnish:
    ///
    /// - It requires handler timeouts configured **consistently** across every
    ///   consumer of a stream and group — already the rule for
    ///   [`with_handler_timeout`](Self::with_handler_timeout), and not relaxed
    ///   here. A process sweeping at 30 s against a 60 s handler elsewhere is
    ///   racing the renewal, not beaten by it.
    /// - The re-check is check-then-act, and this backend does not currently
    ///   serialize the two halves: applying an outcome takes further round
    ///   trips (`XADD` then `XACK`) issued separately from the check, so a
    ///   consumer stalled between them can still be overtaken. That is a
    ///   present limitation rather than something Redis forbids — `Ack`, a
    ///   no-DLQ `Reject` and an immediate `Retry`/`Defer` are single-key and
    ///   would fit in the check's script, and DLQ/hold routing is scriptable
    ///   too wherever the destination shares a hash slot with the queue. What
    ///   has no single-script form is arbitrary cross-slot destinations on a
    ///   clustered deployment, and the same at-least-once fallback is applied
    ///   uniformly rather than only there.
    /// - The renewal interval has a 100 ms floor, so below a 200 ms handler
    ///   timeout the margin narrows from half the timeout to whatever is left
    ///   over that floor — and at 100 ms or under there is none: the first
    ///   renewal would fall due after the deadline has already passed.
    ///
    /// So the window shrinks from "every handler that reaches its deadline" to
    /// "a consumer that loses its lease and does not notice in time", but a
    /// reclaim-induced duplicate remains possible — as it already is for any
    /// Redis Streams consumer group, which is at-least-once. Where ownership
    /// cannot be established at all, the outcome is applied anyway rather than
    /// leaving an entry that may have no reaper to recover it.
    ///
    /// Handler *panics* are unaffected and always resolve to
    /// [`Outcome::Retry`] — a panic is a failed attempt, not a slow one.
    pub fn with_handler_timeout_outcome(mut self, outcome: Outcome) -> Self {
        self.handler_timeout_outcome = Some(outcome);
        self
    }

    /// Disable the handler timeout entirely (handlers may run indefinitely).
    ///
    /// On the Redis backend this also disables crash-recovery reclaim for
    /// the stream's maintenance sidecar: with no deadline, in-flight work is
    /// never presumed dead. See [`with_handler_timeout`](Self::with_handler_timeout)
    /// for why the setting should be consistent across every consumer of a
    /// stream and group, including other processes.
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

    /// Set the maximum number of reconnect attempts before the consumer gives
    /// up and surfaces an error. See [`max_reconnect_attempts`](Self::max_reconnect_attempts)
    /// for full semantics.
    pub fn with_max_reconnect_attempts(mut self, n: u32) -> Self {
        self.max_reconnect_attempts = Some(n);
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
            handler_timeout_outcome: self.handler_timeout_outcome,
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            max_reconnect_attempts: self.max_reconnect_attempts,
            #[cfg(feature = "rabbitmq")]
            hold_queue_timeout: self.hold_queue_timeout,
            shutdown: self.shutdown.unwrap_or_default(),
            processing: self.processing,
            consumer_group: self.consumer_group,
            #[cfg(feature = "kafka")]
            kafka_group_id: self.kafka_group_id,
            #[cfg(feature = "kafka")]
            kafka_auto_offset_reset: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: self.schema_registry,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: self.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: self.schema_accepted_subjects,
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
            handler_timeout_outcome: self.handler_timeout_outcome.clone(),
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            max_reconnect_attempts: self.max_reconnect_attempts,
            #[cfg(feature = "rabbitmq")]
            hold_queue_timeout: self.hold_queue_timeout,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: self.exactly_once,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: self.receive_batch_size,
            #[cfg(feature = "nats")]
            max_ack_pending: self.max_ack_pending,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: self.schema_registry.clone(),
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: self.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: self.schema_accepted_subjects.clone(),
            #[cfg(feature = "kafka")]
            kafka_group_id: self.kafka_group_id.clone(),
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
    ///
    /// Pass `-1` for an unbounded budget.
    ///
    /// # Panics
    ///
    /// Panics unless `n` is positive or exactly `-1`, matching
    /// [`NatsConsumerGroupConfig::with_max_ack_pending`](
    /// crate::backends::nats::NatsConsumerGroupConfig::with_max_ack_pending).
    /// `0` is rejected rather than forwarded: JetStream treats a zero
    /// `max_ack_pending` as "unset" and silently substitutes the server
    /// default (1000), which looks like a working override while quietly
    /// ignoring it.
    pub fn with_max_ack_pending(mut self, n: i64) -> Self {
        assert!(
            n > 0 || n == -1,
            "max_ack_pending ({n}) must be positive, or -1 for unbounded"
        );
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

#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
impl ConsumerOptions<RabbitMq> {
    /// Set the maximum time a sequence key may remain in `AwaitingRetry`
    /// before its pending deliveries are dead-lettered.
    ///
    /// See [`hold_queue_timeout`](Self::hold_queue_timeout) for semantics.
    /// Panics if `timeout` is zero.
    pub fn with_hold_queue_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "hold_queue_timeout must be positive");
        self.hold_queue_timeout = Some(timeout);
        self
    }
}

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
impl ConsumerOptions<Kafka> {
    /// Override the base Kafka consumer `group.id` for this consumer.
    ///
    /// The standard consumer joins this value verbatim; a DLQ drain joins
    /// `"{group}-dlq"`; a FIFO consumer joins `"{group}-fifo"`. `None` (the
    /// default) keeps the topic-derived ids: `"{queue}-consumer"`,
    /// `"{dlq}-consumer"`, and `"{queue}-fifo"`.
    ///
    /// Use this when two independent services consume the same topic and each
    /// must receive every message (fan-out): without distinct group IDs they
    /// share one consumer group and compete for partitions. For the coordinated
    /// registry path the equivalent is
    /// [`KafkaConsumerGroupConfig::with_group_id`](crate::kafka::KafkaConsumerGroupConfig::with_group_id).
    pub fn with_group_id(mut self, group_id: impl Into<Arc<str>>) -> Self {
        self.kafka_group_id = Some(group_id.into());
        self
    }
}

#[cfg(feature = "kafka-schema-registry")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka-schema-registry")))]
impl ConsumerOptions<Kafka> {
    /// Decode registry-framed messages via the given registry (shared cache).
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Set how subject mismatches are handled. Default `Enforce`.
    pub fn with_schema_enforcement(mut self, enforcement: SchemaEnforcement) -> Self {
        self.schema_enforcement = enforcement;
        self
    }

    /// Set the accepted subject set. Defaults to `["{queue}-value"]` at decode
    /// time when not set.
    pub fn accept_schema_subjects<I, S>(mut self, subjects: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.schema_accepted_subjects = Some(subjects.into_iter().map(Into::into).collect());
        self
    }
}

#[cfg(test)]
#[allow(clippy::absolute_paths)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
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
        not(feature = "redis-streams"),
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
        assert_eq!(opts.max_reconnect_attempts, None);
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
            .with_max_message_size(5 * 1024 * 1024)
            .with_max_reconnect_attempts(50);
        assert_eq!(opts.max_retries, 3);
        assert_eq!(opts.prefetch_count, 20);
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(5)));
        assert_eq!(opts.max_pending_per_key, Some(100));
        assert_eq!(opts.max_message_size, Some(5 * 1024 * 1024));
        assert_eq!(opts.max_reconnect_attempts, Some(50));
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
    fn with_max_reconnect_attempts_sets_value() {
        let opts = ConsumerOptions::<TestBackend>::new().with_max_reconnect_attempts(25);
        assert_eq!(opts.max_reconnect_attempts, Some(25));
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
    fn without_handler_timeout_clears_default() {
        let opts = ConsumerOptions::<TestBackend>::new().without_handler_timeout();
        assert_eq!(opts.handler_timeout, None);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn without_max_pending_per_key_clears_default() {
        let opts = ConsumerOptions::<TestBackend>::new().without_max_pending_per_key();
        assert_eq!(opts.max_pending_per_key, None);
    }

    #[cfg(any(
        feature = "inmemory",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq"
    ))]
    #[test]
    fn with_consumer_group_sets_label() {
        let opts = ConsumerOptions::<TestBackend>::new().with_consumer_group("orders-worker");
        let inner = opts.into_inner();
        assert_eq!(inner.consumer_group.as_deref(), Some("orders-worker"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_with_group_id_propagates_through_into_inner() {
        use crate::markers::Kafka;
        let inner = ConsumerOptions::<Kafka>::new()
            .with_group_id("price-latest-sink")
            .into_inner();
        assert_eq!(inner.kafka_group_id.as_deref(), Some("price-latest-sink"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_group_id_defaults_to_none() {
        use crate::markers::Kafka;
        let inner = ConsumerOptions::<Kafka>::new().into_inner();
        assert_eq!(inner.kafka_group_id, None);
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
            .with_max_message_size(4096)
            .with_max_reconnect_attempts(42);
        let copy = opts.clone();
        assert_eq!(copy.max_retries, 7);
        assert_eq!(copy.prefetch_count, 13);
        assert!(!copy.concurrent_processing);
        assert_eq!(copy.handler_timeout, Some(Duration::from_secs(11)));
        assert_eq!(copy.max_pending_per_key, Some(99));
        assert_eq!(copy.max_message_size, Some(4096));
        assert_eq!(copy.max_reconnect_attempts, Some(42));
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

    #[cfg(feature = "nats")]
    #[test]
    fn nats_with_max_ack_pending_allows_unbounded_sentinel() {
        let opts = ConsumerOptions::<Nats>::new().with_max_ack_pending(-1);
        assert_eq!(opts.max_ack_pending, Some(-1));
    }

    #[cfg(feature = "nats")]
    #[test]
    #[should_panic(expected = "max_ack_pending (0) must be positive")]
    fn nats_with_max_ack_pending_panics_on_zero() {
        let _ = ConsumerOptions::<Nats>::new().with_max_ack_pending(0);
    }

    #[cfg(feature = "nats")]
    #[test]
    #[should_panic(expected = "max_ack_pending (-2) must be positive")]
    fn nats_with_max_ack_pending_panics_on_negative_other_than_unbounded() {
        let _ = ConsumerOptions::<Nats>::new().with_max_ack_pending(-2);
    }

    #[test]
    fn resolve_set_returns_explicit_duration() {
        assert_eq!(
            resolve_handler_timeout(
                HandlerTimeoutConfig::Set(Duration::from_secs(5)),
                Some(Duration::from_secs(60)),
            ),
            Duration::from_secs(5),
        );
        assert_eq!(
            resolve_handler_timeout(HandlerTimeoutConfig::Set(Duration::from_secs(5)), None),
            Duration::from_secs(5),
        );
    }

    #[test]
    fn resolve_inherit_uses_registry_default_then_library_default() {
        assert_eq!(
            resolve_handler_timeout(HandlerTimeoutConfig::Inherit, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
        assert_eq!(
            resolve_handler_timeout(HandlerTimeoutConfig::Inherit, None),
            DEFAULT_HANDLER_TIMEOUT,
        );
    }

    #[test]
    fn handler_timeout_config_default_is_inherit() {
        assert_eq!(
            HandlerTimeoutConfig::default(),
            HandlerTimeoutConfig::Inherit
        );
    }

    #[cfg(feature = "kafka-schema-registry")]
    mod kafka_schema_registry_options {
        use std::sync::Arc;

        use crate::consumer::ConsumerOptions;
        use crate::markers::Kafka;
        use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};

        #[test]
        fn schema_enforcement_default_is_enforce() {
            let inner = ConsumerOptions::<Kafka>::new().into_inner();
            assert_eq!(inner.schema_enforcement, SchemaEnforcement::Enforce);
            assert!(inner.schema_registry.is_none());
            assert!(inner.schema_accepted_subjects.is_none());
        }

        #[test]
        fn with_schema_enforcement_permissive_propagates() {
            let inner = ConsumerOptions::<Kafka>::new()
                .with_schema_enforcement(SchemaEnforcement::Permissive)
                .into_inner();
            assert_eq!(inner.schema_enforcement, SchemaEnforcement::Permissive);
        }

        #[test]
        fn accept_schema_subjects_propagates() {
            let inner = ConsumerOptions::<Kafka>::new()
                .accept_schema_subjects(["orders-value", "orders-key"])
                .into_inner();
            assert_eq!(
                inner.schema_accepted_subjects,
                Some(vec![Arc::from("orders-value"), Arc::from("orders-key")])
            );
        }

        #[test]
        fn with_schema_registry_propagates() {
            let registry = SchemaRegistry::builder("http://localhost:8081").build();
            let inner = ConsumerOptions::<Kafka>::new()
                .with_schema_registry(registry)
                .into_inner();
            assert!(inner.schema_registry.is_some());
        }
    }
}
