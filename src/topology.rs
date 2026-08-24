use std::time::Duration;

#[cfg(all(feature = "nats", feature = "env-config"))]
use crate::env::EnvVars;
#[cfg(all(feature = "nats", feature = "env-config"))]
use crate::error::Result;

// ---------------------------------------------------------------------------
// NATS stream config
// ---------------------------------------------------------------------------

/// JetStream retention policy for a shove-managed stream. Mirrors
/// `async_nats::jetstream::stream::RetentionPolicy` without coupling shove's
/// public API to the async-nats version. NATS-specific.
#[cfg(feature = "nats")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsRetention {
    /// Messages are removed once acknowledged by a consumer. Implies a single
    /// logical consumer over a given subject. shove's historical default.
    WorkQueue,
    /// Messages are retained until a limit (age/bytes/count) is hit, regardless
    /// of acknowledgement. Allows multiple consumers and replay within the window.
    Limits,
    /// Messages are retained while any consumer has interest.
    Interest,
}

/// Explicit JetStream stream configuration for a shove-*managed* stream, set via
/// [`TopologyBuilder::nats_stream_config`]. Lets shove create a stream with the
/// retention, size/age bounds, and replication that the defaults don't express
/// (the defaults are WorkQueue / unbounded / single-replica). NATS-specific.
///
/// Use this when shove should own the stream but you need it bounded (so a stalled
/// consumer can't grow the file store without limit) or replicated (R3). Use
/// [`TopologyBuilder::nats_external_stream`] instead when infra owns the stream.
#[cfg(feature = "nats")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsStreamConfig {
    /// Retention policy. Default [`NatsRetention::WorkQueue`].
    pub retention: NatsRetention,
    /// Maximum age of a message before it is discarded. `None` ⇒ unlimited.
    pub max_age: Option<Duration>,
    /// Maximum total stream size in bytes. `None` ⇒ unlimited.
    pub max_bytes: Option<i64>,
    /// Maximum number of messages retained. `None` ⇒ unlimited.
    pub max_messages: Option<i64>,
    /// Replica count (e.g. 3 for prod durability). Default 1.
    pub num_replicas: usize,
}

#[cfg(feature = "nats")]
impl Default for NatsStreamConfig {
    fn default() -> Self {
        Self {
            retention: NatsRetention::WorkQueue,
            max_age: None,
            max_bytes: None,
            max_messages: None,
            num_replicas: 1,
        }
    }
}

#[cfg(all(feature = "nats", feature = "env-config"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "nats", feature = "env-config"))))]
impl NatsStreamConfig {
    /// Read the stream knobs from the environment under `prefix`.
    ///
    /// | Variable | Type | Default |
    /// |---|---|---|
    /// | `{PREFIX}_RETENTION` | `work_queue` \| `limits` \| `interest` | `work_queue` |
    /// | `{PREFIX}_MAX_AGE_SECS` | `u64`, `>= 1` | unlimited |
    /// | `{PREFIX}_MAX_BYTES` | `i64`, `>= 1` | unlimited |
    /// | `{PREFIX}_MAX_MESSAGES` | `i64`, `>= 1` | unlimited |
    /// | `{PREFIX}_NUM_REPLICAS` | `usize`, `1..=5` | `1` |
    ///
    /// `RETENTION` matching ignores case and treats `-` and `_` alike, so
    /// `work_queue`, `work-queue`, and `WorkQueue` all resolve to
    /// [`NatsRetention::WorkQueue`].
    ///
    /// ```
    /// use shove::NatsStreamConfig;
    ///
    /// let config = NatsStreamConfig::from_env("EVENTS")?;
    /// # let _ = config.num_replicas;
    /// # Ok::<_, shove::ShoveError>(())
    /// ```
    pub fn from_env(prefix: impl Into<String>) -> Result<Self> {
        Self::from_vars(&EnvVars::with_prefix(prefix))
    }

    /// Read from an existing [`EnvVars`], so one reader can populate several
    /// config structs.
    pub fn from_vars(vars: &EnvVars) -> Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            retention: vars.choice(
                "RETENTION",
                defaults.retention,
                &[
                    ("work_queue", NatsRetention::WorkQueue),
                    ("limits", NatsRetention::Limits),
                    ("interest", NatsRetention::Interest),
                ],
            )?,
            max_age: vars
                .opt_parse_in("MAX_AGE_SECS", 1..=u64::MAX)?
                .map(Duration::from_secs),
            max_bytes: vars.opt_parse_in("MAX_BYTES", 1..=i64::MAX)?,
            max_messages: vars.opt_parse_in("MAX_MESSAGES", 1..=i64::MAX)?,
            // 5 is JetStream's own ceiling: reject a typo at startup rather
            // than at stream-creation time.
            num_replicas: vars.parse_in("NUM_REPLICAS", defaults.num_replicas, 1..=5)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Kafka topic config
// ---------------------------------------------------------------------------

/// Kafka `cleanup.policy` for a shove-managed topic, set via
/// [`TopologyBuilder::with_cleanup_policy`]. Kafka-specific.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaCleanupPolicy {
    /// Discard segments past the retention limits (`delete`, Kafka's default).
    Delete,
    /// Retain the latest value per message key (`compact`).
    Compact,
    /// Compact, and also discard segments past the retention limits
    /// (`compact,delete`).
    CompactDelete,
}

#[cfg(feature = "kafka")]
impl KafkaCleanupPolicy {
    /// The `cleanup.policy` value string Kafka expects.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Compact => "compact",
            Self::CompactDelete => "compact,delete",
        }
    }
}

// ---------------------------------------------------------------------------
// HoldQueue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HoldQueue {
    pub(crate) name: String,
    pub(crate) delay: Duration,
}

impl HoldQueue {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn delay(&self) -> Duration {
        self.delay
    }
}

// ---------------------------------------------------------------------------
// SequenceFailure
// ---------------------------------------------------------------------------

/// Controls what happens to remaining messages in a sequence when one message
/// fails permanently (exceeds max retries or returns [`Reject`](crate::Outcome::Reject)).
///
/// Both policies dead-letter the failed message itself. They differ in how
/// they treat *subsequent* messages that share the same sequence key.
///
/// # Choosing a policy
///
/// - Use [`Skip`](Self::Skip) when messages are **independently valid** but
///   happen to need ordered delivery (e.g. audit-log entries, analytics events).
///   A single bad event should not block the rest of the stream.
///
/// - Use [`FailAll`](Self::FailAll) when messages are **causally dependent** —
///   each message assumes every prior message in the sequence was processed
///   successfully (e.g. financial ledger entries, state-machine transitions).
///   Processing later messages after an earlier one failed would leave the
///   system in an inconsistent state.
///
/// # Example
///
/// Given a sequence for key `ACC-A` with messages `[1, 2, 3, 4, 5]` where
/// message 3 is permanently rejected:
///
/// | Policy   | DLQ'd messages | Ack'd messages |
/// |----------|---------------|----------------|
/// | `Skip`   | 3             | 1, 2, 4, 5    |
/// | `FailAll`| 3, 4, 5       | 1, 2           |
///
/// Messages for *other* sequence keys (e.g. `ACC-B`) are unaffected by either
/// policy — poisoning is scoped to the failing key only.
///
/// # Backend support
///
/// Both policies are honoured on all six backends, with identical semantics.
/// See `docs/design/sequence-failure-parity.md` for the per-backend key source
/// and the rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFailure {
    /// Dead-letter the failed message, skip it, and continue processing
    /// subsequent messages in the sequence as normal.
    Skip,
    /// Dead-letter the failed message and automatically dead-letter all
    /// remaining messages for the same sequence key ("poison" the key).
    ///
    /// A key is poisoned by any permanent failure of one of its messages:
    /// [`Outcome::Reject`](crate::Outcome::Reject), an exhausted retry budget,
    /// or a pre-handler rejection (oversize or undeserializable payload).
    /// Poisoned messages are dead-lettered without invoking the handler.
    ///
    /// # Scope and limits
    ///
    /// - The key stays poisoned for the **lifetime of the consumer task**, and
    ///   survives a broker reconnect.
    /// - The poison set is **process-local**. A second consumer of the same
    ///   topic keeps its own set and will still invoke the handler for a key
    ///   the first consumer poisoned. On Kafka, a partition moved by a
    ///   rebalance likewise arrives at its new owner unpoisoned. Ordering is
    ///   preserved in both cases; the poison record is not shared.
    /// - Messages with no sequence key are never poisoned — an unkeyed message
    ///   belongs to no sequence.
    FailAll,
}

// ---------------------------------------------------------------------------
// SequenceConfig
// ---------------------------------------------------------------------------

/// Configuration for sequenced (strictly ordered) delivery on a topic.
///
/// Created automatically by [`TopologyBuilder::sequenced`]. The config
/// determines:
///
/// - **`on_failure`** — the [`SequenceFailure`] policy (`Skip` or `FailAll`).
/// - **`routing_shards`** — how many sub-queues the consistent-hash exchange
///   fans out to. More shards allow higher parallelism while preserving
///   per-key ordering. Default: **8**.
/// - **`exchange`** — the name of the consistent-hash exchange (derived from
///   the queue name as `{queue}-seq-hash`).
#[derive(Debug, Clone)]
pub struct SequenceConfig {
    pub(crate) on_failure: SequenceFailure,
    pub(crate) routing_shards: u16,
    pub(crate) exchange: String, // pre-computed "{queue}-seq-hash"
}

impl SequenceConfig {
    pub fn on_failure(&self) -> SequenceFailure {
        self.on_failure
    }

    pub fn routing_shards(&self) -> u16 {
        self.routing_shards
    }

    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

// ---------------------------------------------------------------------------
// QueueTopology
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct QueueTopology {
    pub(crate) queue: String,
    /// Fan-out group this topology belongs to (see
    /// [`TopologyBuilder::for_consumer_group`]). `None` is the historical
    /// single-reader shape, where every derived name is topic-derived only.
    pub(crate) consumer_group: Option<String>,
    /// Ephemeral per-instance fan-out (see [`TopologyBuilder::broadcast`]).
    /// Mutually exclusive with every retry-chain option, so when this is true
    /// `dlq`, `hold_queues` and `sequencing` are all guaranteed empty.
    pub(crate) broadcast: bool,
    pub(crate) dlq: Option<String>,
    pub(crate) hold_queues: Vec<HoldQueue>,
    pub(crate) sequencing: Option<SequenceConfig>,
    #[cfg(feature = "nats")]
    pub(crate) nats_stream_subjects: Option<Vec<String>>,
    /// When true, shove binds to an externally-provisioned stream instead of
    /// creating it (see [`TopologyBuilder::nats_external_stream`]).
    #[cfg(feature = "nats")]
    pub(crate) nats_external_stream: bool,
    /// Explicit config for a shove-managed stream (see
    /// [`TopologyBuilder::nats_stream_config`]); `None` keeps the defaults.
    #[cfg(feature = "nats")]
    pub(crate) nats_stream_config: Option<NatsStreamConfig>,
    /// Kafka topic-level config entries (e.g. `retention.ms`) applied to the
    /// main topic when shove creates or reconciles it (see
    /// [`TopologyBuilder::with_topic_config`]). Kafka-specific.
    #[cfg(feature = "kafka")]
    pub(crate) kafka_topic_config: Vec<(String, String)>,
}

impl QueueTopology {
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The fan-out group this topology was declared for, if set via
    /// [`TopologyBuilder::for_consumer_group`].
    ///
    /// `None` means the topology is the historical single-reader shape: the
    /// DLQ, hold queues and sequencing exchange are derived from the queue
    /// name alone, so two readers of the same topic would share them.
    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    /// Whether this topology is an ephemeral per-instance broadcast
    /// subscription (see [`TopologyBuilder::broadcast`]).
    ///
    /// A broadcast topology has no DLQ, no hold queues and no sequencing —
    /// `build()` rejects those combinations — so consumers can treat it as
    /// best-effort delivery with no retry chain.
    pub fn broadcast(&self) -> bool {
        self.broadcast
    }

    pub fn dlq(&self) -> Option<&str> {
        self.dlq.as_deref()
    }

    pub fn hold_queues(&self) -> &[HoldQueue] {
        &self.hold_queues
    }

    pub fn sequencing(&self) -> Option<&SequenceConfig> {
        self.sequencing.as_ref()
    }

    /// NATS JetStream subjects this stream is created over, if overridden via
    /// [`TopologyBuilder::nats_subjects`]. `None` keeps the default behavior
    /// (the stream is created over a single subject equal to the queue name).
    ///
    /// Lets shove create and own a WorkQueue stream over an externally-owned
    /// subject (e.g. `markets-feed.market.EVENT_TYPE_PRICE_CHANGE.>`) rather
    /// than only the queue name. NATS-specific; ignored by other backends.
    #[cfg(feature = "nats")]
    pub fn nats_stream_subjects(&self) -> Option<&[String]> {
        self.nats_stream_subjects.as_deref()
    }

    /// Whether shove binds to an externally-provisioned JetStream stream rather
    /// than creating it. When true, `declare()` verifies the stream exists and
    /// fails fast if it doesn't (no silent fallback). NATS-specific.
    #[cfg(feature = "nats")]
    pub fn nats_external_stream(&self) -> bool {
        self.nats_external_stream
    }

    /// Explicit config for a shove-managed stream, if set via
    /// [`TopologyBuilder::nats_stream_config`]. `None` keeps shove's defaults
    /// (WorkQueue / unbounded / single-replica). NATS-specific.
    #[cfg(feature = "nats")]
    pub fn nats_stream_config(&self) -> Option<&NatsStreamConfig> {
        self.nats_stream_config.as_ref()
    }

    /// Kafka topic-level config entries set via
    /// [`TopologyBuilder::with_topic_config`], in call order (repeated keys
    /// are kept; the declarer resolves last-write-wins at merge time).
    /// Applied to the **main topic only**; the DLQ keeps cluster defaults.
    /// Kafka-specific; ignored by other backends.
    #[cfg(feature = "kafka")]
    pub fn kafka_topic_config(&self) -> &[(String, String)] {
        &self.kafka_topic_config
    }

    pub fn shard_hold_queue_names(&self, shard_index: u16) -> Vec<HoldQueue> {
        let prefix = aux_prefix(&self.queue, self.consumer_group.as_deref());
        self.hold_queues
            .iter()
            .map(|hq| HoldQueue {
                name: format!(
                    "{prefix}-seq-{shard_index}-hold-{}",
                    hold_delay_suffix(hq.delay)
                ),
                delay: hq.delay,
            })
            .collect()
    }
}

/// The prefix every *derived* auxiliary name (DLQ, hold queues, sequencing
/// exchange) is built from.
///
/// Without a fan-out group this is the queue name, which is what shove has
/// always used. With one it is `{queue}-{group}`, so two independent readers
/// of the same topic get disjoint retry/DLQ chains instead of draining each
/// other's held and dead messages. The main queue name is never rewritten —
/// the whole point is that the topic stays shared.
fn aux_prefix(queue: &str, consumer_group: Option<&str>) -> String {
    match consumer_group {
        Some(group) => format!("{queue}-{group}"),
        None => queue.to_owned(),
    }
}

/// Renders a hold-queue delay into the suffix used in its queue name.
///
/// Whole-second delays keep the historical `{secs}s` form so existing deployed
/// topologies are unchanged. Sub-second or fractional delays use `{ms}ms` so the
/// name stays injective at millisecond granularity — the precision backends use
/// for the queue TTL (e.g. RabbitMQ's `x-message-ttl`), so two delays sharing a
/// name with different TTLs would fail re-declaration with PRECONDITION_FAILED.
/// The two forms never overlap: `{secs}s` is only emitted for whole seconds, and
/// `{ms}ms` only for non-whole-second values. (Delays differing only below 1ms
/// still collide, but sub-millisecond backoff tiers are not a real use case.)
fn hold_delay_suffix(delay: Duration) -> String {
    if delay.subsec_millis() == 0 {
        format!("{}s", delay.as_secs())
    } else {
        format!("{}ms", delay.as_millis())
    }
}

// ---------------------------------------------------------------------------
// TopologyBuilder
// ---------------------------------------------------------------------------

/// How the DLQ name was requested. Resolved at `build()` rather than at call
/// time so `for_consumer_group()` can be called in any order relative to
/// `dlq()`; last write still wins between `dlq()` and `dlq_named()`.
enum DlqName {
    /// `.dlq()` — derive `{queue}[-{group}]-dlq` at build time.
    Derived,
    /// `.dlq_named(..)` — use this name verbatim, group or no group.
    Explicit(String),
}

#[must_use]
pub struct TopologyBuilder {
    queue: String,
    consumer_group: Option<String>,
    broadcast: bool,
    dlq: Option<DlqName>,
    hold_queues: Vec<Duration>,
    sequencing: Option<SequenceConfig>,
    allow_message_loss: bool,
    #[cfg(feature = "nats")]
    nats_stream_subjects: Option<Vec<String>>,
    #[cfg(feature = "nats")]
    nats_external_stream: bool,
    #[cfg(feature = "nats")]
    nats_stream_config: Option<NatsStreamConfig>,
    #[cfg(feature = "kafka")]
    kafka_topic_config: Vec<(String, String)>,
    #[cfg(feature = "kafka")]
    kafka_retention_finite: bool,
    #[cfg(feature = "kafka")]
    kafka_retention_forever: bool,
}

impl TopologyBuilder {
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            consumer_group: None,
            broadcast: false,
            dlq: None,
            hold_queues: Vec::new(),
            sequencing: None,
            allow_message_loss: false,
            #[cfg(feature = "nats")]
            nats_stream_subjects: None,
            #[cfg(feature = "nats")]
            nats_external_stream: false,
            #[cfg(feature = "nats")]
            nats_stream_config: None,
            #[cfg(feature = "kafka")]
            kafka_topic_config: Vec::new(),
            #[cfg(feature = "kafka")]
            kafka_retention_finite: false,
            #[cfg(feature = "kafka")]
            kafka_retention_forever: false,
        }
    }

    /// Declare this topology on behalf of a named fan-out group, so a second
    /// independent reader of the same topic gets its **own** retry/DLQ chain.
    ///
    /// shove derives every auxiliary name from the queue name: `{queue}-dlq`,
    /// `{queue}-hold-5s`, `{queue}-seq-hash`. That is right for one reader and
    /// wrong for two — two services consuming the same topic would share one
    /// DLQ and one set of hold queues, so each would drain the other's held
    /// and dead messages. The workaround was to declare a *bare* topology
    /// (no `.dlq()`, no `.hold_queue()`) on the second reader, which gives up
    /// [`Outcome::Retry`](crate::Outcome::Retry) and
    /// [`Outcome::Reject`](crate::Outcome::Reject) entirely — both silently
    /// discard without a DLQ.
    ///
    /// With a group set, the derived names become `{queue}-{group}-dlq`,
    /// `{queue}-{group}-hold-5s` and `{queue}-{group}-seq-hash`. The queue
    /// name itself is untouched — the topic stays shared, which is the point.
    /// A name given explicitly to [`dlq_named`](Self::dlq_named) is used
    /// verbatim; the group only affects *derived* names.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Service A — the original reader, unchanged.
    /// define_topic!(pub PriceEvents, PriceEvent,
    ///     TopologyBuilder::new("price-events")
    ///         .hold_queue(Duration::from_secs(5))
    ///         .dlq()                       // price-events-dlq
    ///         .build()
    /// );
    ///
    /// // Service B — same topic, its own failure-handling chain.
    /// define_topic!(pub PriceEventsLatest, PriceEvent,
    ///     TopologyBuilder::new("price-events")
    ///         .for_consumer_group("price-latest")
    ///         .hold_queue(Duration::from_secs(5))  // price-events-price-latest-hold-5s
    ///         .dlq()                               // price-events-price-latest-dlq
    ///         .build()
    /// );
    /// ```
    ///
    /// # Backend notes
    ///
    /// Namespacing the retry chain is necessary for fan-out but not by itself
    /// sufficient — the two readers must also be *in different consumer
    /// groups*, which is a backend-level notion:
    ///
    /// - **Kafka** — the group is wired through: the `group.id` defaults to
    ///   `{queue}-{group}` instead of `{queue}-consumer`, so the two readers
    ///   get independent partition assignments rather than splitting one set.
    ///   An explicit
    ///   [`ConsumerOptions::with_group_id`](crate::ConsumerOptions::with_group_id)
    ///   or [`KafkaConsumerGroupConfig::with_group_id`](crate::kafka::KafkaConsumerGroupConfig::with_group_id)
    ///   still wins.
    /// - **Redis** — the XGROUP name is a client-level setting; give the second
    ///   reader a client with a different group and this namespaces its chain.
    /// - **NATS / RabbitMQ / SQS** — the group namespaces the retry chain, but
    ///   the readers still share one durable consumer / queue and therefore
    ///   compete for messages. Real fan-out needs a second stream consumer or a
    ///   fan-out exchange, which shove does not derive for you.
    ///
    /// # Panics
    ///
    /// `build()` panics if the group name is empty or blank.
    pub fn for_consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Subscribe **every instance** of a service to every message, rather than
    /// competing for one shared queue.
    ///
    /// Where [`for_consumer_group`](Self::for_consumer_group) gives a second
    /// *service* its own retry chain, `broadcast()` gives every *process* its
    /// own ephemeral subscription. It is the same group notion taken to its
    /// limit: the group is per-process identity, created at start and gone the
    /// moment the process is. Written for cache invalidation and similar
    /// fan-out signals, where each replica must act on the message itself.
    ///
    /// ```rust
    /// # use shove::topology::TopologyBuilder;
    /// let topology = TopologyBuilder::new("cache-invalidations").broadcast().build();
    /// assert!(topology.broadcast());
    /// ```
    ///
    /// # Best-effort by construction
    ///
    /// Broadcast is deliberately lossy, and the type system says so:
    ///
    /// - **Deliver-new only.** A subscriber receives what is published while it
    ///   is subscribed. Nothing is replayed for an instance that was down —
    ///   which is correct for invalidation, because a cold process has a cold
    ///   cache.
    /// - **No retry chain.** [`dlq`](Self::dlq), [`dlq_named`](Self::dlq_named),
    ///   [`hold_queue`](Self::hold_queue) and [`sequenced`](Self::sequenced)
    ///   each panic in `build()` when combined with this. Redelivery to *one*
    ///   subscriber of a fan-out is not expressible on most brokers, and a
    ///   shared DLQ would collect N copies of every failure. `broadcast()`
    ///   therefore implies [`allow_message_loss`](Self::allow_message_loss):
    ///   [`Outcome::Retry`](crate::Outcome::Retry) and
    ///   [`Outcome::Reject`](crate::Outcome::Reject) discard with a warning.
    /// - **One consumer per instance.** A second consumer in the same process
    ///   would *split* the broadcast rather than duplicate it, so the broadcast
    ///   entry point has no autoscaling knob at all.
    ///
    /// # Backend support
    ///
    /// Declaring a broadcast topology is backend-independent; *consuming* one
    /// is gated on [`HasBroadcast`](crate::backend::capability::HasBroadcast),
    /// which carries the authoritative list of backends that implement it.
    /// Not every backend does yet, and `Broker<Sqs>` never will — per-instance
    /// SQS fan-out needs per-pod queue and subscription lifecycle management,
    /// and a leaked queue costs real money. A backend without the impl fails to
    /// compile at `broadcast_subscriber()` rather than silently degrading.
    ///
    /// # Panics
    ///
    /// `build()` panics if this is combined with `dlq()`, `dlq_named()`,
    /// `hold_queue()`, `sequenced()` or `for_consumer_group()`.
    pub fn broadcast(mut self) -> Self {
        self.broadcast = true;
        self
    }

    /// Create and own a NATS JetStream WorkQueue stream over the given
    /// subjects, instead of the default single subject equal to the queue name.
    ///
    /// Use this to bridge an externally-owned subject (e.g.
    /// `markets-feed.market.EVENT_TYPE_PRICE_CHANGE.>`): the stream **name**
    /// stays the queue name; only the subjects it captures change, and the
    /// durable pull consumer filters on the configured subject.
    ///
    /// NATS-specific — other backends ignore it. Cannot be combined with
    /// [`sequenced`](Self::sequenced), which owns the subject space via its
    /// per-shard subjects; `build()` panics if both are set.
    #[cfg(feature = "nats")]
    pub fn nats_subjects(mut self, subjects: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.nats_stream_subjects = Some(subjects.into_iter().map(Into::into).collect());
        self
    }

    /// Bind to an externally-provisioned JetStream stream instead of creating it.
    ///
    /// `declare()` will verify the stream (named after the queue) already exists
    /// and **fail fast** if it doesn't — it never falls back to creating one. Use
    /// this when infra owns the stream config (retention bounds, replication)
    /// that shove can't express; shove then only manages the durable consumer and
    /// its own DLQ/hold queues.
    ///
    /// NATS-specific. Mutually exclusive with [`nats_subjects`](Self::nats_subjects)
    /// and [`nats_stream_config`](Self::nats_stream_config) (both configure stream
    /// *creation*, which external mode skips) and with [`sequenced`](Self::sequenced);
    /// `build()` panics if combined.
    #[cfg(feature = "nats")]
    pub fn nats_external_stream(mut self) -> Self {
        self.nats_external_stream = true;
        self
    }

    /// Create the shove-managed stream with explicit config (retention, size/age
    /// bounds, replicas) instead of the defaults (WorkQueue / unbounded / R1).
    ///
    /// Use this when shove should own the stream but you need it bounded (so a
    /// stalled consumer can't grow the file store without limit) or replicated.
    /// For an infra-owned stream use [`nats_external_stream`](Self::nats_external_stream)
    /// instead.
    ///
    /// NATS-specific. Mutually exclusive with
    /// [`nats_external_stream`](Self::nats_external_stream); `build()` panics if both are set.
    #[cfg(feature = "nats")]
    pub fn nats_stream_config(mut self, config: NatsStreamConfig) -> Self {
        self.nats_stream_config = Some(config);
        self
    }

    /// Sets a Kafka topic-level config entry (e.g. `retention.ms`,
    /// `retention.bytes`, `cleanup.policy`) on the **main topic**. Repeatable;
    /// later calls for the same key win. The DLQ topic is never touched.
    ///
    /// Entries apply when shove creates the topic, and are **reconciled** on
    /// already-existing topics: `declare()` compares the declared keys against
    /// the live values and issues an alter when they drift (preserving the
    /// topic's other dynamic config entries).
    ///
    /// Values pass through to the broker verbatim; an invalid key or value
    /// surfaces as a `ShoveError::Topology` from `declare()`.
    ///
    /// Kafka-specific; other backends ignore it. For a declarer-wide default
    /// see `TopologyDeclarer::<Kafka>::with_topic_config`.
    ///
    /// # Panics
    ///
    /// `build()` panics if a key is empty or blank.
    #[cfg(feature = "kafka")]
    pub fn with_topic_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.kafka_topic_config.push((key.into(), value.into()));
        self
    }

    /// Sets `retention.ms` from a [`Duration`]. Sugar for
    /// [`with_topic_config`](Self::with_topic_config); the same scope and
    /// reconcile semantics apply. Sub-millisecond precision is truncated.
    ///
    /// # Panics
    ///
    /// Panics if combined with
    /// [`with_retention_forever`](Self::with_retention_forever).
    #[cfg(feature = "kafka")]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        assert!(
            !self.kafka_retention_forever,
            "with_retention() cannot be combined with with_retention_forever() — both set retention.ms"
        );
        self.kafka_retention_finite = true;
        self.with_topic_config("retention.ms", retention.as_millis().to_string())
    }

    /// Sets `retention.ms = -1`, retaining messages forever (typically paired
    /// with [`with_cleanup_policy`](Self::with_cleanup_policy) and
    /// [`KafkaCleanupPolicy::Compact`]). Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    ///
    /// # Panics
    ///
    /// Panics if combined with [`with_retention`](Self::with_retention).
    #[cfg(feature = "kafka")]
    pub fn with_retention_forever(mut self) -> Self {
        assert!(
            !self.kafka_retention_finite,
            "with_retention() cannot be combined with with_retention_forever() — both set retention.ms"
        );
        self.kafka_retention_forever = true;
        self.with_topic_config("retention.ms", "-1")
    }

    /// Sets `retention.bytes`, the maximum partition size before old segments
    /// are discarded. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    #[cfg(feature = "kafka")]
    pub fn with_retention_bytes(self, bytes: u64) -> Self {
        self.with_topic_config("retention.bytes", bytes.to_string())
    }

    /// Sets `cleanup.policy`. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    #[cfg(feature = "kafka")]
    pub fn with_cleanup_policy(self, policy: KafkaCleanupPolicy) -> Self {
        self.with_topic_config("cleanup.policy", policy.as_str())
    }

    /// Sets `max.message.bytes`, the largest record batch the topic accepts.
    /// Sugar for [`with_topic_config`](Self::with_topic_config).
    #[cfg(feature = "kafka")]
    pub fn with_max_message_bytes(self, bytes: u32) -> Self {
        self.with_topic_config("max.message.bytes", bytes.to_string())
    }

    /// Enables strict per-key ordered delivery for this topic.
    ///
    /// Messages are routed through a consistent-hash exchange so that all
    /// messages sharing the same sequence key land on the same sub-queue and
    /// are processed in publish order.
    ///
    /// The `on_failure` policy determines what happens when a message is
    /// permanently rejected — see [`SequenceFailure`] for details.
    ///
    /// Defaults to **8** routing shards (override with [`routing_shards`](Self::routing_shards)).
    pub fn sequenced(mut self, on_failure: SequenceFailure) -> Self {
        // The exchange name is re-derived in `build()` so it picks up a
        // `for_consumer_group()` call made after this one.
        self.sequencing = Some(SequenceConfig {
            on_failure,
            routing_shards: 8,
            exchange: String::new(),
        });
        self
    }

    /// Overrides the routing shard count.
    /// Panics if called before `sequenced()`.
    pub fn routing_shards(mut self, count: u16) -> Self {
        let seq = self
            .sequencing
            .as_mut()
            .expect("routing_shards() called before sequenced()");
        seq.routing_shards = count;
        self
    }

    /// Adds a hold queue with the given delay for retry backoff.
    ///
    /// Hold queues are selected in order by retry count. Define multiple hold
    /// queues with increasing delays to get escalating backoff. Once the retry
    /// count exceeds the number of hold queues, messages keep going to the last
    /// (longest-delay) hold queue on every subsequent retry until
    /// [`ConsumerOptions::max_retries`](crate::ConsumerOptions::max_retries) is exhausted, at which point the
    /// message is routed to the DLQ.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // With max_retries = 5:
    /// // retry 0 → hold-1s, retry 1 → hold-5s,
    /// // retries 2..4 → hold-30s (clamped to last),
    /// // retry 5 → DLQ
    /// TopologyBuilder::new("orders")
    ///     .hold_queue(Duration::from_secs(1))   // 1st retry: 1s
    ///     .hold_queue(Duration::from_secs(5))   // 2nd retry: 5s
    ///     .hold_queue(Duration::from_secs(30))  // 3rd+ retries: 30s until DLQ
    ///     .dlq()
    ///     .build();
    /// ```
    pub fn hold_queue(mut self, delay: Duration) -> Self {
        self.hold_queues.push(delay);
        self
    }

    /// Enables a dead-letter queue with the default name `{queue}-dlq`, or
    /// `{queue}-{group}-dlq` under
    /// [`for_consumer_group`](Self::for_consumer_group).
    pub fn dlq(mut self) -> Self {
        self.dlq = Some(DlqName::Derived);
        self
    }

    /// Enables a dead-letter queue with a custom name.
    ///
    /// Use this when the default `{queue}-dlq` suffix doesn't match your
    /// naming convention or when the DLQ is shared across topics. An explicit
    /// name is used verbatim — [`for_consumer_group`](Self::for_consumer_group)
    /// does not namespace it, so two readers naming the same DLQ share it on
    /// purpose.
    ///
    /// # Example
    ///
    /// ```ignore
    /// TopologyBuilder::new("orders")
    ///     .dlq_named("orders-dead-letters")
    ///     .build();
    /// ```
    pub fn dlq_named(mut self, name: impl Into<String>) -> Self {
        self.dlq = Some(DlqName::Explicit(name.into()));
        self
    }

    /// Acknowledge that failed messages in this sequenced topic may be
    /// permanently lost.
    ///
    /// By default, `build()` panics if a sequenced topic is missing a DLQ or
    /// hold queues, because that means rejected messages are silently
    /// discarded. Call this method to suppress those guards when message loss
    /// is acceptable (e.g. ephemeral metrics, best-effort notifications).
    ///
    /// Has no effect on non-sequenced topics.
    pub fn allow_message_loss(mut self) -> Self {
        self.allow_message_loss = true;
        self
    }

    /// Builds the `QueueTopology`.
    ///
    /// # Panics
    ///
    /// Panics when the configuration is invalid:
    /// - Sequencing enabled with `routing_shards = 0`.
    /// - Sequencing enabled without a DLQ (unless
    ///   [`allow_message_loss`](Self::allow_message_loss) is set).
    /// - Sequencing enabled without at least one hold queue (unless
    ///   [`allow_message_loss`](Self::allow_message_loss) is set).
    /// - [`for_consumer_group`](Self::for_consumer_group) given an empty or
    ///   blank group name.
    /// - [`broadcast`](Self::broadcast) combined with `dlq()`, `dlq_named()`,
    ///   `hold_queue()`, `sequenced()` or `for_consumer_group()`.
    pub fn build(mut self) -> QueueTopology {
        if let Some(ref group) = self.consumer_group {
            assert!(
                !group.trim().is_empty(),
                "for_consumer_group() requires a non-empty group name"
            );
        }
        if self.broadcast {
            // Each of these declares a retry chain, and a broadcast topology
            // has nowhere coherent to put one: a redelivery would go to one
            // subscriber of a fan-out, and a shared DLQ would collect N copies
            // of every failure. Reject at build() rather than silently
            // declaring auxiliary queues nothing will ever read.
            assert!(
                self.dlq.is_none(),
                "broadcast() cannot be combined with dlq()/dlq_named() — a broadcast \
                 subscription is best-effort with no retry chain, and a shared DLQ would \
                 collect one copy of every failure per subscriber"
            );
            assert!(
                self.hold_queues.is_empty(),
                "broadcast() cannot be combined with hold_queue() — a broadcast \
                 subscription is best-effort with no retry chain, and a held message \
                 would be redelivered to one subscriber of the fan-out"
            );
            assert!(
                self.sequencing.is_none(),
                "broadcast() cannot be combined with sequenced() — sequencing orders one \
                 shared consumer group across shard queues, which is the opposite of \
                 giving every instance its own ephemeral subscription"
            );
            assert!(
                self.consumer_group.is_none(),
                "broadcast() cannot be combined with for_consumer_group() — broadcast is \
                 already a per-process group, so the named group would name nothing and \
                 silently have no effect"
            );
            // Broadcast is best-effort by definition; the sequencing/retry
            // guards below never fire for it, but stating this here keeps the
            // flag truthful for anything reading the built topology.
            self.allow_message_loss = true;
        }
        #[cfg(feature = "nats")]
        {
            assert!(
                !(self.nats_stream_subjects.is_some() && self.sequencing.is_some()),
                "nats_subjects() cannot be combined with sequenced() — sequencing owns the subject space via its per-shard subjects"
            );
            if let Some(ref subjects) = self.nats_stream_subjects {
                assert!(
                    !subjects.is_empty(),
                    "nats_subjects() requires at least one subject"
                );
                assert!(
                    subjects.iter().all(|s| !s.trim().is_empty()),
                    "nats_subjects() subjects must be non-empty"
                );
            }
            // External mode skips stream creation, so it can't be combined with
            // options that configure how the stream is created.
            assert!(
                !(self.nats_external_stream && self.nats_stream_config.is_some()),
                "nats_external_stream() cannot be combined with nats_stream_config() — an external stream's config is owned by whoever provisions it"
            );
            assert!(
                !(self.nats_external_stream && self.nats_stream_subjects.is_some()),
                "nats_external_stream() cannot be combined with nats_subjects() — an external stream's subjects are owned by whoever provisions it"
            );
            assert!(
                !(self.nats_external_stream && self.sequencing.is_some()),
                "nats_external_stream() cannot be combined with sequenced() — sequencing requires shove to own the stream/subject space"
            );
        }
        #[cfg(feature = "kafka")]
        assert!(
            self.kafka_topic_config
                .iter()
                .all(|(k, _)| !k.trim().is_empty()),
            "with_topic_config() keys must be non-empty"
        );
        if let Some(ref seq) = self.sequencing {
            assert!(
                seq.routing_shards > 0,
                "routing_shards must be greater than 0 when sequencing is enabled"
            );
            if !self.allow_message_loss {
                assert!(
                    self.dlq.is_some(),
                    "sequenced topics require a DLQ — call .dlq() or .dlq_named() or .allow_message_loss() before .build()"
                );
                assert!(
                    !self.hold_queues.is_empty(),
                    "sequenced topics require at least one hold queue — call .hold_queue() or .allow_message_loss() before .build()"
                );
            }
        }

        // Warn about non-sequenced topics with incomplete retry infrastructure.
        if self.sequencing.is_none() && !self.allow_message_loss {
            if !self.hold_queues.is_empty() && self.dlq.is_none() {
                tracing::warn!(
                    queue = self.queue,
                    "topic has hold queues but no DLQ — messages exhausting max_retries will be silently discarded"
                );
            }
            if self.dlq.is_some() && self.hold_queues.is_empty() {
                tracing::warn!(
                    queue = self.queue,
                    "topic has a DLQ but no hold queues — retries will use broker redelivery with no delay"
                );
            }
        }

        // Every derived name resolves here, off one prefix, so a
        // `for_consumer_group()` call anywhere in the chain is honoured.
        let prefix = aux_prefix(&self.queue, self.consumer_group.as_deref());

        let dlq = self.dlq.map(|name| match name {
            DlqName::Derived => format!("{prefix}-dlq"),
            DlqName::Explicit(name) => name,
        });

        let hold_queues = self
            .hold_queues
            .into_iter()
            .map(|delay| HoldQueue {
                name: format!("{prefix}-hold-{}", hold_delay_suffix(delay)),
                delay,
            })
            .collect();

        if let Some(ref mut seq) = self.sequencing {
            seq.exchange = format!("{prefix}-seq-hash");
        }

        QueueTopology {
            queue: self.queue,
            consumer_group: self.consumer_group,
            broadcast: self.broadcast,
            dlq,
            hold_queues,
            sequencing: self.sequencing,
            #[cfg(feature = "nats")]
            nats_stream_subjects: self.nats_stream_subjects,
            #[cfg(feature = "nats")]
            nats_external_stream: self.nats_external_stream,
            #[cfg(feature = "nats")]
            nats_stream_config: self.nats_stream_config,
            #[cfg(feature = "kafka")]
            kafka_topic_config: self.kafka_topic_config,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "nats", feature = "env-config"))]
mod nats_env_config_tests {
    use super::{NatsRetention, NatsStreamConfig};
    use crate::env::EnvVars;
    use std::time::Duration;

    fn vars(pairs: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_pairs("EVENTS", pairs.to_vec())
    }

    #[test]
    fn all_unset_matches_default() {
        assert_eq!(
            NatsStreamConfig::from_vars(&vars(&[])).unwrap(),
            NatsStreamConfig::default()
        );
    }

    #[test]
    fn reads_every_knob() {
        let config = NatsStreamConfig::from_vars(&vars(&[
            ("EVENTS_RETENTION", "limits"),
            ("EVENTS_MAX_AGE_SECS", "3600"),
            ("EVENTS_MAX_BYTES", "1048576"),
            ("EVENTS_MAX_MESSAGES", "1000"),
            ("EVENTS_NUM_REPLICAS", "3"),
        ]))
        .unwrap();
        assert_eq!(config.retention, NatsRetention::Limits);
        assert_eq!(config.max_age, Some(Duration::from_secs(3600)));
        assert_eq!(config.max_bytes, Some(1_048_576));
        assert_eq!(config.max_messages, Some(1000));
        assert_eq!(config.num_replicas, 3);
    }

    #[test]
    fn retention_spelling_is_forgiving() {
        for spelling in ["work_queue", "work-queue", "WorkQueue"] {
            let config = NatsStreamConfig::from_vars(&vars(&[("EVENTS_RETENTION", spelling)]))
                .unwrap_or_else(|e| panic!("rejected {spelling}: {e}"));
            assert_eq!(config.retention, NatsRetention::WorkQueue);
        }
        let err =
            NatsStreamConfig::from_vars(&vars(&[("EVENTS_RETENTION", "forever")])).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("EVENTS_RETENTION"), "got: {msg}");
        assert!(msg.contains("work_queue"), "got: {msg}");
    }

    #[test]
    fn replicas_are_capped_at_jetstreams_own_maximum() {
        assert!(NatsStreamConfig::from_vars(&vars(&[("EVENTS_NUM_REPLICAS", "6")])).is_err());
        assert!(NatsStreamConfig::from_vars(&vars(&[("EVENTS_NUM_REPLICAS", "0")])).is_err());
        assert_eq!(
            NatsStreamConfig::from_vars(&vars(&[("EVENTS_NUM_REPLICAS", "5")]))
                .unwrap()
                .num_replicas,
            5
        );
    }

    #[test]
    fn zero_bounds_are_rejected_rather_than_read_as_unlimited() {
        // `max_bytes: 0` would mean "unlimited" to JetStream, which is the
        // opposite of what an operator writing `0` intends.
        assert!(NatsStreamConfig::from_vars(&vars(&[("EVENTS_MAX_BYTES", "0")])).is_err());
        assert!(NatsStreamConfig::from_vars(&vars(&[("EVENTS_MAX_MESSAGES", "0")])).is_err());
        assert!(NatsStreamConfig::from_vars(&vars(&[("EVENTS_MAX_AGE_SECS", "0")])).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn builder_main_queue_name() {
        let topology = TopologyBuilder::new("orders").build();
        assert_eq!(topology.queue(), "orders");
    }

    #[test]
    fn builder_dlq_named() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        assert_eq!(topology.dlq(), Some("orders-dlq"));
    }

    #[test]
    fn builder_no_dlq() {
        let topology = TopologyBuilder::new("orders").build();
        assert_eq!(topology.dlq(), None);
    }

    #[test]
    fn builder_hold_queues() {
        let topology = TopologyBuilder::new("orders")
            .hold_queue(Duration::from_secs(30))
            .hold_queue(Duration::from_secs(300))
            .build();

        let hqs = topology.hold_queues();
        assert_eq!(hqs.len(), 2);

        assert_eq!(hqs[0].name(), "orders-hold-30s");
        assert_eq!(hqs[0].delay(), Duration::from_secs(30));

        assert_eq!(hqs[1].name(), "orders-hold-300s");
        assert_eq!(hqs[1].delay(), Duration::from_secs(300));
    }

    #[test]
    fn builder_no_hold_queues() {
        let topology = TopologyBuilder::new("orders").build();
        assert!(topology.hold_queues().is_empty());
    }

    #[test]
    fn builder_whole_second_hold_names_unchanged() {
        // Whole-second delays keep the historical `{secs}s` suffix.
        let topology = TopologyBuilder::new("orders")
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(30))
            .build();

        let hqs = topology.hold_queues();
        assert_eq!(hqs[0].name(), "orders-hold-5s");
        assert_eq!(hqs[1].name(), "orders-hold-30s");
    }

    #[test]
    fn builder_sub_second_hold_names_use_millis_and_stay_distinct() {
        // Delays that share a whole second (or are sub-second) render as `{ms}ms`
        // so their names — and the TTLs backends derive from them — stay distinct.
        let topology = TopologyBuilder::new("orders")
            .hold_queue(Duration::from_millis(200))
            .hold_queue(Duration::from_millis(500))
            .hold_queue(Duration::from_millis(1500))
            .hold_queue(Duration::from_millis(1800))
            .build();

        let hqs = topology.hold_queues();
        assert_eq!(hqs[0].name(), "orders-hold-200ms");
        assert_eq!(hqs[1].name(), "orders-hold-500ms");
        assert_eq!(hqs[2].name(), "orders-hold-1500ms");
        assert_eq!(hqs[3].name(), "orders-hold-1800ms");
    }

    #[test]
    fn builder_sequenced_defaults() {
        let topology = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();

        let seq = topology.sequencing().expect("sequencing should be set");
        assert_eq!(seq.routing_shards(), 8);
        assert_eq!(seq.exchange(), "orders-seq-hash");
        assert_eq!(seq.on_failure(), SequenceFailure::Skip);
    }

    #[test]
    fn builder_sequenced_custom_shards() {
        let topology = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(16)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();

        let seq = topology.sequencing().expect("sequencing should be set");
        assert_eq!(seq.routing_shards(), 16);
        assert_eq!(seq.on_failure(), SequenceFailure::FailAll);
    }

    #[test]
    #[should_panic(expected = "routing_shards() called before sequenced()")]
    fn builder_routing_shards_before_sequenced_panics() {
        let _ = TopologyBuilder::new("orders").routing_shards(4).build();
    }

    #[test]
    #[should_panic(expected = "routing_shards must be greater than 0")]
    fn builder_zero_shards_panics() {
        let _ = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::Skip)
            .routing_shards(0)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
    }

    #[test]
    fn builder_no_sequencing() {
        let topology = TopologyBuilder::new("orders").build();
        assert!(topology.sequencing().is_none());
    }

    #[test]
    fn builder_full_topology() {
        let topology = TopologyBuilder::new("payments")
            .dlq()
            .hold_queue(Duration::from_secs(60))
            .hold_queue(Duration::from_secs(600))
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(32)
            .build();

        assert_eq!(topology.queue(), "payments");

        assert_eq!(topology.dlq(), Some("payments-dlq"));

        let hqs = topology.hold_queues();
        assert_eq!(hqs.len(), 2);
        assert_eq!(hqs[0].name(), "payments-hold-60s");
        assert_eq!(hqs[0].delay(), Duration::from_secs(60));
        assert_eq!(hqs[1].name(), "payments-hold-600s");
        assert_eq!(hqs[1].delay(), Duration::from_secs(600));

        let seq = topology.sequencing().expect("sequencing should be set");
        assert_eq!(seq.on_failure(), SequenceFailure::FailAll);
        assert_eq!(seq.routing_shards(), 32);
        assert_eq!(seq.exchange(), "payments-seq-hash");
    }

    #[test]
    #[should_panic(expected = "sequenced topics require a DLQ")]
    fn builder_sequenced_without_dlq_panics() {
        let _ = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .build();
    }

    #[test]
    #[should_panic(expected = "sequenced topics require at least one hold queue")]
    fn builder_sequenced_without_hold_queue_panics() {
        let _ = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::FailAll)
            .dlq()
            .build();
    }

    #[test]
    fn builder_allow_message_loss_suppresses_dlq_guard() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .allow_message_loss()
            .build();
        assert!(topology.dlq().is_none());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn builder_allow_message_loss_suppresses_hold_queue_guard() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::FailAll)
            .dlq()
            .allow_message_loss()
            .build();
        assert!(topology.hold_queues().is_empty());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn shard_hold_queue_names() {
        let topology = TopologyBuilder::new("payments")
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(60))
            .dlq()
            .build();

        let names = topology.shard_hold_queue_names(2);
        assert_eq!(names.len(), 2);
        assert_eq!(names[0].name(), "payments-seq-2-hold-5s");
        assert_eq!(names[0].delay(), Duration::from_secs(5));
        assert_eq!(names[1].name(), "payments-seq-2-hold-60s");
        assert_eq!(names[1].delay(), Duration::from_secs(60));
    }

    #[test]
    fn builder_allow_message_loss_suppresses_both_guards() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::Skip)
            .allow_message_loss()
            .build();
        assert!(topology.dlq().is_none());
        assert!(topology.hold_queues().is_empty());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn builder_dlq_custom_name() {
        let topology = TopologyBuilder::new("orders")
            .dlq_named("orders-dead-letters")
            .build();
        assert_eq!(topology.dlq(), Some("orders-dead-letters"));
    }

    #[test]
    fn builder_dlq_default_suffix_unchanged() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        assert_eq!(topology.dlq(), Some("orders-dlq"));
    }

    #[test]
    fn builder_dlq_name_overrides_dlq() {
        let topology = TopologyBuilder::new("orders")
            .dlq()
            .dlq_named("custom-dead")
            .build();
        assert_eq!(topology.dlq(), Some("custom-dead"));
    }

    #[test]
    fn builder_dlq_after_dlq_name_uses_default() {
        let topology = TopologyBuilder::new("orders")
            .dlq_named("custom-dead")
            .dlq()
            .build();
        assert_eq!(topology.dlq(), Some("orders-dlq"));
    }

    // -- fan-out groups --

    #[test]
    fn fan_out_group_is_none_by_default() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        assert_eq!(topology.consumer_group(), None);
    }

    #[test]
    fn fan_out_group_leaves_the_main_queue_shared() {
        // The topic is the thing both readers share; namespacing it would
        // defeat the purpose.
        let topology = TopologyBuilder::new("price-events")
            .for_consumer_group("price-latest")
            .build();
        assert_eq!(topology.queue(), "price-events");
        assert_eq!(topology.consumer_group(), Some("price-latest"));
    }

    #[test]
    fn fan_out_group_namespaces_the_derived_dlq() {
        let topology = TopologyBuilder::new("price-events")
            .for_consumer_group("price-latest")
            .dlq()
            .build();
        assert_eq!(topology.dlq(), Some("price-events-price-latest-dlq"));
    }

    #[test]
    fn fan_out_group_namespaces_hold_queues() {
        let topology = TopologyBuilder::new("price-events")
            .for_consumer_group("price-latest")
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_millis(250))
            .dlq()
            .build();
        let names: Vec<_> = topology
            .hold_queues()
            .iter()
            .map(|hq| hq.name().to_owned())
            .collect();
        assert_eq!(
            names,
            vec![
                "price-events-price-latest-hold-5s",
                "price-events-price-latest-hold-250ms",
            ]
        );
    }

    #[test]
    fn fan_out_group_namespaces_the_sequencing_exchange_and_shard_hold_queues() {
        let topology = TopologyBuilder::new("payments")
            .for_consumer_group("ledger-mirror")
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(
            topology.sequencing().unwrap().exchange(),
            "payments-ledger-mirror-seq-hash"
        );
        assert_eq!(
            topology.shard_hold_queue_names(2)[0].name(),
            "payments-ledger-mirror-seq-2-hold-5s"
        );
    }

    // Call order must not matter: the builder is a set of independent knobs
    // everywhere else, and a group applied only to the calls that follow it
    // would produce a half-namespaced chain — the exact silent-overlap bug the
    // feature exists to prevent.
    #[test]
    fn fan_out_group_applies_regardless_of_call_order() {
        let before = TopologyBuilder::new("price-events")
            .for_consumer_group("price-latest")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        let after = TopologyBuilder::new("price-events")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .for_consumer_group("price-latest")
            .build();
        assert_eq!(before.dlq(), after.dlq());
        assert_eq!(
            before.hold_queues()[0].name(),
            after.hold_queues()[0].name()
        );
        assert_eq!(
            before.sequencing().unwrap().exchange(),
            after.sequencing().unwrap().exchange()
        );
    }

    // An explicitly named DLQ is a deliberate choice — two readers naming the
    // same one are asking to share it.
    #[test]
    fn fan_out_group_leaves_an_explicit_dlq_name_alone() {
        let topology = TopologyBuilder::new("price-events")
            .for_consumer_group("price-latest")
            .dlq_named("shared-dead-letters")
            .build();
        assert_eq!(topology.dlq(), Some("shared-dead-letters"));
    }

    #[test]
    fn two_fan_out_groups_share_the_topic_and_nothing_else() {
        let a = TopologyBuilder::new("price-events")
            .for_consumer_group("book")
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        let b = TopologyBuilder::new("price-events")
            .for_consumer_group("latest")
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(a.queue(), b.queue());
        assert_ne!(a.dlq(), b.dlq());
        assert_ne!(a.hold_queues()[0].name(), b.hold_queues()[0].name());
    }

    // Every derived name keeps its historical spelling when no group is set,
    // so upgrading shove never silently re-points a deployed topology at a
    // fresh, empty DLQ.
    #[test]
    fn no_fan_out_group_keeps_every_historical_name() {
        let topology = TopologyBuilder::new("payments")
            .sequenced(SequenceFailure::FailAll)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(topology.dlq(), Some("payments-dlq"));
        assert_eq!(topology.hold_queues()[0].name(), "payments-hold-5s");
        assert_eq!(
            topology.sequencing().unwrap().exchange(),
            "payments-seq-hash"
        );
        assert_eq!(
            topology.shard_hold_queue_names(1)[0].name(),
            "payments-seq-1-hold-5s"
        );
    }

    #[test]
    #[should_panic(expected = "for_consumer_group() requires a non-empty group name")]
    fn fan_out_group_rejects_a_blank_name() {
        // A blank group would produce `orders--dlq` and read as a typo'd
        // deployment rather than a shared one.
        let _ = TopologyBuilder::new("orders")
            .for_consumer_group("  ")
            .dlq()
            .build();
    }

    #[test]
    fn builder_dlq_name_with_sequenced() {
        let topology = TopologyBuilder::new("events")
            .sequenced(SequenceFailure::Skip)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .dlq_named("events-failed")
            .build();
        assert_eq!(topology.dlq(), Some("events-failed"));
        assert!(topology.sequencing().is_some());
    }

    // -- Broadcast --

    #[test]
    fn broadcast_is_off_by_default() {
        assert!(!TopologyBuilder::new("orders").build().broadcast());
    }

    #[test]
    fn broadcast_declares_no_auxiliary_queues() {
        let topology = TopologyBuilder::new("cache-invalidations")
            .broadcast()
            .build();
        assert!(topology.broadcast());
        assert_eq!(topology.queue(), "cache-invalidations");
        assert_eq!(topology.dlq(), None);
        assert!(topology.hold_queues().is_empty());
        assert!(topology.sequencing().is_none());
        assert_eq!(topology.consumer_group(), None);
    }

    // AC1: each conflicting option panics in build(), naming the conflict.
    // Both orderings, because the guard runs at build() and must not depend on
    // which call came first.

    #[test]
    #[should_panic(expected = "broadcast() cannot be combined with dlq()/dlq_named()")]
    fn broadcast_with_dlq_panics() {
        let _ = TopologyBuilder::new("cache-invalidations")
            .broadcast()
            .dlq()
            .build();
    }

    #[test]
    #[should_panic(expected = "broadcast() cannot be combined with dlq()/dlq_named()")]
    fn dlq_named_before_broadcast_panics() {
        let _ = TopologyBuilder::new("cache-invalidations")
            .dlq_named("shared-dead-letters")
            .broadcast()
            .build();
    }

    #[test]
    #[should_panic(expected = "broadcast() cannot be combined with hold_queue()")]
    fn broadcast_with_hold_queue_panics() {
        let _ = TopologyBuilder::new("cache-invalidations")
            .broadcast()
            .hold_queue(Duration::from_secs(5))
            .build();
    }

    #[test]
    #[should_panic(expected = "broadcast() cannot be combined with sequenced()")]
    fn broadcast_with_sequenced_panics() {
        let _ = TopologyBuilder::new("cache-invalidations")
            .broadcast()
            .sequenced(SequenceFailure::Skip)
            .build();
    }

    // Not in the ticket's list, but the same class of defect: under broadcast
    // the group namespaces nothing (there are no derived names) and no backend
    // reads it, so accepting it would be a silent no-op.
    #[test]
    #[should_panic(expected = "broadcast() cannot be combined with for_consumer_group()")]
    fn broadcast_with_consumer_group_panics() {
        let _ = TopologyBuilder::new("cache-invalidations")
            .broadcast()
            .for_consumer_group("price-latest")
            .build();
    }

    // AC8: every derived name for a topology that does not call `.broadcast()`
    // is byte-identical to what it was before broadcast existed.
    #[test]
    fn broadcast_changes_nothing_for_a_topology_without_it() {
        let plain = TopologyBuilder::new("orders")
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(30))
            .dlq()
            .build();
        assert_eq!(plain.queue(), "orders");
        assert_eq!(plain.dlq(), Some("orders-dlq"));
        assert_eq!(plain.hold_queues()[0].name(), "orders-hold-5s");
        assert_eq!(plain.hold_queues()[1].name(), "orders-hold-30s");
        assert!(!plain.broadcast());

        let grouped = TopologyBuilder::new("orders")
            .for_consumer_group("latest")
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(grouped.queue(), "orders");
        assert_eq!(grouped.dlq(), Some("orders-latest-dlq"));
        assert_eq!(grouped.hold_queues()[0].name(), "orders-latest-hold-5s");
        assert!(!grouped.broadcast());

        let sequenced = TopologyBuilder::new("ledger")
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(
            sequenced.sequencing().map(|s| s.exchange()),
            Some("ledger-seq-hash")
        );
        assert_eq!(
            sequenced.shard_hold_queue_names(2)[0].name(),
            "ledger-seq-2-hold-5s"
        );
        assert!(!sequenced.broadcast());
    }

    // -- NATS stream subjects --

    #[cfg(feature = "nats")]
    mod nats_subjects {
        use super::*;

        #[test]
        fn builder_no_nats_subjects_is_none() {
            let topology = TopologyBuilder::new("orders").build();
            assert!(topology.nats_stream_subjects().is_none());
        }

        #[test]
        fn builder_nats_subjects_sets_stream_subjects() {
            let topology = TopologyBuilder::new("price-bridge")
                .nats_subjects(["markets-feed.market.EVENT_TYPE_PRICE_CHANGE.>"])
                .build();
            assert_eq!(
                topology.nats_stream_subjects(),
                Some(["markets-feed.market.EVENT_TYPE_PRICE_CHANGE.>".to_string()].as_slice())
            );
        }

        #[test]
        fn builder_nats_subjects_accepts_multiple() {
            let topology = TopologyBuilder::new("q")
                .nats_subjects(["a.b.>", "c.d"])
                .build();
            assert_eq!(
                topology.nats_stream_subjects(),
                Some(["a.b.>".to_string(), "c.d".to_string()].as_slice())
            );
        }

        #[test]
        #[should_panic(expected = "nats_subjects() requires at least one subject")]
        fn builder_nats_subjects_empty_panics() {
            let _ = TopologyBuilder::new("q")
                .nats_subjects(Vec::<String>::new())
                .build();
        }

        #[test]
        #[should_panic(expected = "nats_subjects() subjects must be non-empty")]
        fn builder_nats_subjects_blank_panics() {
            let _ = TopologyBuilder::new("q").nats_subjects(["  "]).build();
        }

        #[test]
        #[should_panic(expected = "nats_subjects() cannot be combined with sequenced()")]
        fn builder_nats_subjects_with_sequenced_panics() {
            let _ = TopologyBuilder::new("q")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
                .hold_queue(Duration::from_secs(5))
                .dlq()
                .nats_subjects(["a.b.>"])
                .build();
        }
    }

    // -- NATS stream management (external bind / managed-with-config) --

    #[cfg(feature = "nats")]
    mod nats_stream_management {
        use super::*;

        #[test]
        fn builder_defaults_are_managed_unbounded() {
            let topology = TopologyBuilder::new("orders").build();
            assert!(!topology.nats_external_stream());
            assert!(topology.nats_stream_config().is_none());
        }

        #[test]
        fn builder_external_stream_sets_flag() {
            let topology = TopologyBuilder::new("CLOB_PRICE_CHANGES")
                .nats_external_stream()
                .build();
            assert!(topology.nats_external_stream());
            assert!(topology.nats_stream_config().is_none());
        }

        #[test]
        fn builder_stream_config_is_stored() {
            let cfg = NatsStreamConfig {
                retention: NatsRetention::Limits,
                max_age: Some(Duration::from_secs(600)),
                max_bytes: Some(1_000_000),
                max_messages: None,
                num_replicas: 3,
            };
            let topology = TopologyBuilder::new("orders")
                .nats_stream_config(cfg.clone())
                .build();
            assert_eq!(topology.nats_stream_config(), Some(&cfg));
            assert!(!topology.nats_external_stream());
        }

        #[test]
        fn stream_config_default_matches_historical_behavior() {
            let cfg = NatsStreamConfig::default();
            assert_eq!(cfg.retention, NatsRetention::WorkQueue);
            assert_eq!(cfg.num_replicas, 1);
            assert!(cfg.max_age.is_none());
            assert!(cfg.max_bytes.is_none());
            assert!(cfg.max_messages.is_none());
        }

        #[test]
        #[should_panic(
            expected = "nats_external_stream() cannot be combined with nats_stream_config()"
        )]
        fn external_with_stream_config_panics() {
            let _ = TopologyBuilder::new("q")
                .nats_external_stream()
                .nats_stream_config(NatsStreamConfig::default())
                .build();
        }

        #[test]
        #[should_panic(expected = "nats_external_stream() cannot be combined with nats_subjects()")]
        fn external_with_subjects_panics() {
            let _ = TopologyBuilder::new("q")
                .nats_external_stream()
                .nats_subjects(["a.b.>"])
                .build();
        }

        #[test]
        #[should_panic(expected = "nats_external_stream() cannot be combined with sequenced()")]
        fn external_with_sequenced_panics() {
            let _ = TopologyBuilder::new("q")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
                .hold_queue(Duration::from_secs(5))
                .dlq()
                .nats_external_stream()
                .build();
        }
    }

    // -- Kafka topic config --

    #[cfg(feature = "kafka")]
    mod kafka_topic_config {
        use super::*;

        #[test]
        fn builder_no_kafka_config_is_empty() {
            let topology = TopologyBuilder::new("orders").build();
            assert!(topology.kafka_topic_config().is_empty());
        }

        #[test]
        fn builder_kafka_config_is_stored_in_order() {
            let topology = TopologyBuilder::new("orders")
                .with_topic_config("retention.ms", "3600000")
                .with_topic_config("cleanup.policy", "delete")
                .build();
            assert_eq!(
                topology.kafka_topic_config(),
                [
                    ("retention.ms".to_string(), "3600000".to_string()),
                    ("cleanup.policy".to_string(), "delete".to_string()),
                ]
                .as_slice()
            );
        }

        #[test]
        fn builder_kafka_config_keeps_repeated_keys() {
            // Last-write-wins is resolved at merge time in the declarer;
            // the builder stores entries verbatim, in call order.
            let topology = TopologyBuilder::new("orders")
                .with_topic_config("retention.ms", "1000")
                .with_topic_config("retention.ms", "2000")
                .build();
            assert_eq!(topology.kafka_topic_config().len(), 2);
        }

        #[test]
        #[should_panic(expected = "with_topic_config() keys must be non-empty")]
        fn builder_kafka_config_blank_key_panics() {
            let _ = TopologyBuilder::new("orders")
                .with_topic_config("  ", "3600000")
                .build();
        }

        #[test]
        fn builder_kafka_config_with_sequenced() {
            let topology = TopologyBuilder::new("orders")
                .sequenced(SequenceFailure::Skip)
                .hold_queue(Duration::from_secs(5))
                .dlq()
                .with_topic_config("retention.ms", "3600000")
                .build();
            assert_eq!(topology.kafka_topic_config().len(), 1);
        }

        #[test]
        fn builder_named_helpers_map_to_config_entries() {
            let topology = TopologyBuilder::new("orders")
                .with_retention(Duration::from_secs(3600))
                .with_retention_bytes(1_073_741_824)
                .with_cleanup_policy(KafkaCleanupPolicy::CompactDelete)
                .with_max_message_bytes(1_048_576)
                .build();
            assert_eq!(
                topology.kafka_topic_config(),
                [
                    ("retention.ms".to_string(), "3600000".to_string()),
                    ("retention.bytes".to_string(), "1073741824".to_string()),
                    ("cleanup.policy".to_string(), "compact,delete".to_string()),
                    ("max.message.bytes".to_string(), "1048576".to_string()),
                ]
                .as_slice()
            );
        }

        #[test]
        fn builder_with_retention_forever_is_minus_one() {
            let topology = TopologyBuilder::new("orders")
                .with_retention_forever()
                .build();
            assert_eq!(
                topology.kafka_topic_config(),
                [("retention.ms".to_string(), "-1".to_string())].as_slice()
            );
        }

        #[test]
        fn builder_named_helper_overrides_via_last_write_wins() {
            // Named helpers go through the same generic entry list, so the
            // declarer-side merge resolves repeated keys to the last call.
            let topology = TopologyBuilder::new("orders")
                .with_topic_config("retention.ms", "1000")
                .with_retention(Duration::from_secs(2))
                .build();
            let entries = topology.kafka_topic_config();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1], ("retention.ms".to_string(), "2000".to_string()));
        }

        #[test]
        fn cleanup_policy_value_strings() {
            assert_eq!(KafkaCleanupPolicy::Delete.as_str(), "delete");
            assert_eq!(KafkaCleanupPolicy::Compact.as_str(), "compact");
            assert_eq!(KafkaCleanupPolicy::CompactDelete.as_str(), "compact,delete");
        }

        #[test]
        #[should_panic(
            expected = "with_retention() cannot be combined with with_retention_forever()"
        )]
        fn builder_retention_then_forever_panics() {
            let _ = TopologyBuilder::new("orders")
                .with_retention(Duration::from_secs(1))
                .with_retention_forever();
        }

        #[test]
        #[should_panic(
            expected = "with_retention() cannot be combined with with_retention_forever()"
        )]
        fn builder_forever_then_retention_panics() {
            let _ = TopologyBuilder::new("orders")
                .with_retention_forever()
                .with_retention(Duration::from_secs(1));
        }

        #[test]
        fn builder_retention_combines_with_retention_bytes() {
            // retention.bytes is a complementary size bound, kept combinable
            // with either retention.ms form.
            let topology = TopologyBuilder::new("orders")
                .with_retention_forever()
                .with_retention_bytes(1_000_000)
                .build();
            assert_eq!(topology.kafka_topic_config().len(), 2);
        }
    }
}
