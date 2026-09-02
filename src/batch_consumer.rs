//! Public [`BatchConsumer<B>`] — the entry point for batch consumption.
//! Gated on `B: `[`HasBatchConsumption`].
//!
//! The counterpart to [`ConsumerSupervisor`](crate::consumer_supervisor::ConsumerSupervisor)
//! / [`ConsumerGroup`](crate::consumer_group::ConsumerGroup) for handlers that
//! process many messages per call instead of one: buffering up to
//! [`BatchConsumerOptions::with_max_batch_size`] messages (or
//! [`BatchConsumerOptions::with_max_batch_age`], whichever comes first) before
//! invoking the handler once with the whole batch. The primitive exists for
//! **handler amortisation** — one flush per N messages, so a sink pays its
//! per-flush cost (one DB transaction, one HTTP request) once instead of once
//! per message — and for nothing else; see [`HasBatchConsumption`] for which
//! backends implement it.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::batch_consumer::validate_batch_topic;
use crate::backend::capability::HasBatchConsumption;
use crate::backend::{Backend, BatchConsumerImpl, BatchConsumerOptionsInner};
use crate::consumer::{
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_BATCH_AGE, DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_MESSAGE_SIZE,
};
use crate::error::Result;
use crate::handler::BatchMessageHandler;
use crate::outcome::Outcome;
use crate::topic::NotSequenced;

#[cfg(feature = "kafka")]
use crate::backends::kafka::KafkaAutoOffsetReset;
#[cfg(feature = "kafka")]
use crate::markers::Kafka;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};

/// Options for [`BatchConsumer::run`], parameterised by backend marker `B`.
///
/// Fields are private and set through builders (the same shape as Kafka's own
/// `shove::kafka::BatchConsumerOptions`, not [`ConsumerOptions`](crate::ConsumerOptions)'s
/// public fields): the size/age invariants are enforced in the setters, so a
/// constructed value is always internally consistent.
///
/// # No `Clone`
///
/// Deliberately: a [`CancellationToken`] plus, on Kafka, a Schema Registry
/// `Arc`, make blind cloning a footgun on a one-consumer-per-call API — the
/// same reason `shove::kafka::BatchConsumerOptions` is not `Clone` today.
/// Parity with that sibling type wins over parity with `ConsumerOptions`.
///
/// # Field visibility
///
/// Fields are `pub(crate)`, not private: `shove::kafka::BatchConsumerOptions`
/// is a `pub type` alias of `BatchConsumerOptions<Kafka>` (see
/// `backends::kafka::consumer`), and that backend's own white-box unit tests
/// — defined in `backends::kafka::consumer`, a sibling module rather than a
/// descendant of this one — read these fields directly rather than through
/// the builders' getters. Plain private would hide them from that sibling
/// module; `pub(crate)` stops at the crate boundary either way, so nothing
/// external gains access that a private field would have hidden from it.
pub struct BatchConsumerOptions<B: Backend> {
    pub(crate) max_batch_size: usize,
    pub(crate) max_batch_age: Duration,
    pub(crate) handler_timeout: Option<Duration>,
    pub(crate) handler_timeout_outcome: Option<Outcome>,
    pub(crate) consumer_group: Option<Arc<str>>,
    pub(crate) max_message_size: Option<usize>,
    pub(crate) max_reconnect_attempts: Option<u32>,
    pub(crate) shutdown: CancellationToken,

    #[cfg(feature = "kafka")]
    pub(crate) kafka_group_id: Option<Arc<str>>,
    #[cfg(feature = "kafka")]
    pub(crate) kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,

    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_registry: Option<Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_accepted_subjects: Option<Vec<Arc<str>>>,

    _backend: PhantomData<fn() -> B>,
}

impl<B: Backend> BatchConsumerOptions<B> {
    /// Create batch consumer options with library-wide defaults.
    pub fn new() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_batch_age: DEFAULT_MAX_BATCH_AGE,
            // Same default as `ConsumerOptions`. A batch flush is one DB
            // transaction rather than one row, so this is a different amount
            // of headroom than it is on the single-message path — a sink
            // whose flush legitimately takes longer should raise it
            // deliberately rather than discover the default by having
            // batches retried.
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            handler_timeout_outcome: None,
            consumer_group: None,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            max_reconnect_attempts: None,
            shutdown: CancellationToken::new(),
            #[cfg(feature = "kafka")]
            kafka_group_id: None,
            #[cfg(feature = "kafka")]
            kafka_auto_offset_reset: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            // Matches `ConsumerOptions`: enforcement is opt-out, not opt-in.
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: None,
            _backend: PhantomData,
        }
    }

    /// Flush once the batch reaches this many messages. Default
    /// [`DEFAULT_MAX_BATCH_SIZE`](crate::DEFAULT_MAX_BATCH_SIZE).
    ///
    /// # Panics
    ///
    /// Panics if `n == 0`.
    pub fn with_max_batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "max_batch_size must be > 0");
        self.max_batch_size = n;
        self
    }

    /// Flush once this long has elapsed since the first message in the
    /// current batch, even if `max_batch_size` hasn't been reached. Default
    /// [`DEFAULT_MAX_BATCH_AGE`](crate::DEFAULT_MAX_BATCH_AGE).
    ///
    /// # Panics
    ///
    /// Panics if `d` is zero.
    pub fn with_max_batch_age(mut self, d: Duration) -> Self {
        assert!(!d.is_zero(), "max_batch_age must be positive");
        self.max_batch_age = d;
        self
    }

    /// Abandon a `handle_batch` call that runs longer than this and treat the
    /// batch as [`Outcome::Retry`]. Default
    /// [`DEFAULT_HANDLER_TIMEOUT`](crate::DEFAULT_HANDLER_TIMEOUT).
    ///
    /// # Panics
    ///
    /// Panics if `timeout` is zero.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = Some(timeout);
        self
    }

    /// Let `handle_batch` run for as long as it likes.
    ///
    /// For sinks whose flush has no meaningful upper bound. The cost is that
    /// a genuinely hung flush wedges the consumer with no recovery —
    /// including `shutdown.cancel()`, which cannot interrupt an in-flight
    /// flush.
    pub fn without_handler_timeout(mut self) -> Self {
        self.handler_timeout = None;
        self
    }

    /// What a batch handler timeout resolves to, instead of the default
    /// [`Outcome::Retry`].
    ///
    /// Applies batch-wide, not per message — see
    /// [`BatchConsumer::run`] for the full outcome table.
    pub fn with_handler_timeout_outcome(mut self, outcome: Outcome) -> Self {
        self.handler_timeout_outcome = Some(outcome);
        self
    }

    /// Tag this consumer with a group name for metrics labelling. Left unset
    /// it surfaces as `consumer_group="default"`.
    pub fn with_consumer_group(mut self, name: impl Into<Arc<str>>) -> Self {
        self.consumer_group = Some(name.into());
        self
    }

    /// Set the maximum allowed message payload size in bytes. Messages
    /// exceeding this limit are dropped before the handler sees the batch.
    pub fn with_max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    /// Set the maximum number of reconnect attempts before the consumer
    /// gives up and returns an error. `None` (the default) means unlimited.
    pub fn with_max_reconnect_attempts(mut self, n: u32) -> Self {
        self.max_reconnect_attempts = Some(n);
        self
    }

    /// Attach a shutdown token.
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// The configured flush size — what [`with_max_batch_size`](Self::with_max_batch_size)
    /// set, or [`DEFAULT_MAX_BATCH_SIZE`](crate::DEFAULT_MAX_BATCH_SIZE).
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    /// The configured flush age — what [`with_max_batch_age`](Self::with_max_batch_age)
    /// set, or [`DEFAULT_MAX_BATCH_AGE`](crate::DEFAULT_MAX_BATCH_AGE).
    pub fn max_batch_age(&self) -> Duration {
        self.max_batch_age
    }

    /// Lower to the internal options struct for passing across the
    /// [`BatchConsumerImpl`] trait boundary.
    pub(crate) fn into_inner(self) -> BatchConsumerOptionsInner {
        BatchConsumerOptionsInner {
            max_batch_size: self.max_batch_size,
            max_batch_age: self.max_batch_age,
            handler_timeout: self.handler_timeout,
            handler_timeout_outcome: self.handler_timeout_outcome,
            shutdown: self.shutdown,
            consumer_group: self.consumer_group,
            max_message_size: self.max_message_size,
            max_reconnect_attempts: self.max_reconnect_attempts,
            #[cfg(feature = "kafka")]
            kafka_group_id: self.kafka_group_id,
            #[cfg(feature = "kafka")]
            kafka_auto_offset_reset: self.kafka_auto_offset_reset,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: self.schema_registry,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: self.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: self.schema_accepted_subjects,
        }
    }
}

// Manual `Default`, not `#[derive]`: `PhantomData<fn() -> B>` needs no
// `B: Default` bound, and deriving would add one. Mirrors
// `ConsumerOptions<B>`'s `Default` impl.
impl<B: Backend> Default for BatchConsumerOptions<B> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "kafka")]
impl BatchConsumerOptions<Kafka> {
    /// Kafka-only: explicit `group.id` override. See
    /// `shove::kafka::BatchConsumerOptions::with_group_id` for full
    /// semantics — this is the same knob, generalised.
    pub fn with_group_id(mut self, group_id: impl Into<Arc<str>>) -> Self {
        self.kafka_group_id = Some(group_id.into());
        self
    }

    /// Kafka-only: `auto.offset.reset` override.
    pub fn with_auto_offset_reset(mut self, reset: KafkaAutoOffsetReset) -> Self {
        self.kafka_auto_offset_reset = Some(reset);
        self
    }
}

#[cfg(feature = "kafka-schema-registry")]
impl BatchConsumerOptions<Kafka> {
    /// Decode batch messages through the Confluent Schema Registry.
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Whether a message whose schema subject is not accepted is routed to
    /// the DLQ (`Enforce`, the default) or decoded anyway with a warning
    /// (`Permissive`).
    pub fn with_schema_enforcement(mut self, enforcement: SchemaEnforcement) -> Self {
        self.schema_enforcement = enforcement;
        self
    }

    /// Subjects this consumer accepts. Defaults to `{queue}-value`.
    pub fn accept_schema_subjects<I, S>(mut self, subjects: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.schema_accepted_subjects = Some(subjects.into_iter().map(Into::into).collect());
        self
    }
}

/// Runs batch consumption for one topic. Obtained from
/// [`Broker::batch_consumer`](crate::Broker::batch_consumer), and only for
/// backends implementing [`HasBatchConsumption`].
pub struct BatchConsumer<B: HasBatchConsumption> {
    inner: B::BatchConsumerImpl,
}

impl<B: HasBatchConsumption> BatchConsumer<B> {
    pub(crate) fn new(client: &B::Client) -> Self {
        Self {
            inner: B::make_batch_consumer(client),
        }
    }

    /// Consume `T` in batches of up to [`BatchConsumerOptions::max_batch_size`]
    /// messages, flushing whichever of size or
    /// [`BatchConsumerOptions::max_batch_age`] is reached first.
    ///
    /// `handler` is invoked once per flush with the *whole* batch, and
    /// returns a single [`Outcome`] that applies to every message in it:
    ///
    /// | `Outcome` (whole batch) | Effect |
    /// |---|---|
    /// | `Ack` | Every message in the batch retires. |
    /// | `Reject` | Terminal: every message is dead-lettered (or discarded, with no DLQ configured) and retires. |
    /// | `Retry` | The whole batch is returned to the backend's redelivery mechanism. Shove itself imposes no per-batch retry budget, but a backend-declared delivery cap (NATS `MaxDeliver`, RabbitMQ's quorum delivery-limit, SQS's `maxReceiveCount`) may terminate redelivery per that backend's own semantics regardless — see `messages_consumed_total{outcome="retry"}` climbing with no matching `outcome="ack"` as the alertable signal on a backend without such a cap. |
    /// | `Defer` | **Identical to `Retry` here.** A batch-wide outcome carries no sequence key, so the `Retry`/`Defer` distinction that matters on the single-message path has no meaning for a batch. |
    ///
    /// `T` is bound by [`NotSequenced`] at compile time — a topic from
    /// `define_sequenced_topic!` is a compile error here, since a batch-wide
    /// `Outcome` cannot express the per-key poison set a sequenced topic's
    /// failure policy needs (see [`NotSequenced`]'s own doctest for this
    /// pinned as a compile failure). `NotSequenced` is hand-implementable,
    /// though, so this also re-checks the topology at runtime and returns
    /// [`crate::ShoveError::Topology`] if it declares sequencing config
    /// anyway.
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptions<B>,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        validate_batch_topic::<T>()?;
        self.inner
            .run_batch::<T, H>(handler, ctx, options.into_inner())
            .await
    }
}
