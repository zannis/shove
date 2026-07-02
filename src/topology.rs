use std::time::Duration;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFailure {
    /// Dead-letter the failed message, skip it, and continue processing
    /// subsequent messages in the sequence as normal.
    Skip,
    /// Dead-letter the failed message and automatically dead-letter all
    /// remaining messages for the same sequence key ("poison" the key).
    /// The key stays poisoned for the lifetime of the consumer process.
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
        self.hold_queues
            .iter()
            .map(|hq| HoldQueue {
                name: format!(
                    "{}-seq-{shard_index}-hold-{}",
                    self.queue,
                    hold_delay_suffix(hq.delay)
                ),
                delay: hq.delay,
            })
            .collect()
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

pub struct TopologyBuilder {
    queue: String,
    dlq: Option<String>,
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
        let exchange = format!("{}-seq-hash", self.queue);
        self.sequencing = Some(SequenceConfig {
            on_failure,
            routing_shards: 8,
            exchange,
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

    /// Enables a dead-letter queue with the default name `{queue}-dlq`.
    pub fn dlq(mut self) -> Self {
        self.dlq = Some(format!("{}-dlq", self.queue));
        self
    }

    /// Enables a dead-letter queue with a custom name.
    ///
    /// Use this when the default `{queue}-dlq` suffix doesn't match your
    /// naming convention or when the DLQ is shared across topics.
    ///
    /// # Example
    ///
    /// ```ignore
    /// TopologyBuilder::new("orders")
    ///     .dlq_named("orders-dead-letters")
    ///     .build();
    /// ```
    pub fn dlq_named(mut self, name: impl Into<String>) -> Self {
        self.dlq = Some(name.into());
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
    pub fn build(self) -> QueueTopology {
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

        let dlq = self.dlq;

        let hold_queues = self
            .hold_queues
            .into_iter()
            .map(|delay| HoldQueue {
                name: format!("{}-hold-{}", self.queue, hold_delay_suffix(delay)),
                delay,
            })
            .collect();

        QueueTopology {
            queue: self.queue,
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
