use std::time::Duration;

use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::context::GetStreamErrorKind;
use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};

use crate::ShoveError;
use crate::error::Result;
use crate::topology::{NatsRetention, NatsStreamConfig, QueueTopology, SequenceConfig};

use super::client::NatsClient;
use super::consumer::map_get_stream_error;

/// JetStream's default `duplicate_window`.
const DEFAULT_DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

/// The `duplicate_window` of a stream that keeps its messages for `max_age`,
/// zero for no limit. JetStream refuses a window longer than the age, so the
/// window is the default or the age, whichever is shorter.
fn duplicate_window(max_age: Duration) -> Duration {
    if max_age.is_zero() {
        DEFAULT_DUPLICATE_WINDOW
    } else {
        max_age.min(DEFAULT_DUPLICATE_WINDOW)
    }
}

fn retention_policy(retention: NatsRetention) -> RetentionPolicy {
    match retention {
        NatsRetention::WorkQueue => RetentionPolicy::WorkQueue,
        NatsRetention::Limits => RetentionPolicy::Limits,
        NatsRetention::Interest => RetentionPolicy::Interest,
    }
}

/// The stream shove declares for `name` over `subjects` from an explicit
/// `cfg`.
fn declared_config(name: &str, subjects: Vec<String>, cfg: &NatsStreamConfig) -> StreamConfig {
    // `None` maps to JetStream's "unlimited" sentinels (Duration::ZERO / -1),
    // preserving the historical unbounded default.
    let max_age = cfg.max_age.unwrap_or_default();
    StreamConfig {
        name: name.to_string(),
        subjects,
        retention: retention_policy(cfg.retention),
        storage: StorageType::File,
        discard: DiscardPolicy::New,
        duplicate_window: duplicate_window(max_age),
        max_age,
        max_bytes: cfg.max_bytes.unwrap_or(-1),
        max_messages: cfg.max_messages.unwrap_or(-1),
        num_replicas: cfg.num_replicas.max(1),
        ..Default::default()
    }
}

/// `existing` with the fields shove owns set again from `owned`: subjects,
/// retention, storage, the discard policy and the duplicate window. Every other
/// field stays as it is, the limits and replicas an operator tuned among them.
fn with_owned_fields(mut existing: StreamConfig, owned: StreamConfig) -> StreamConfig {
    existing.subjects = owned.subjects;
    existing.retention = owned.retention;
    existing.storage = owned.storage;
    existing.discard = owned.discard;
    existing.duplicate_window = duplicate_window(existing.max_age);
    existing
}

/// The stream config a `.broadcast()` topology must be declared with.
///
/// shove's historical default is [`NatsRetention::WorkQueue`], and JetStream
/// rejects *both* halves of the ephemeral broadcast consumer on such a stream:
/// `AckPolicy::None` fails with "consumer in pull mode requires ack policy"
/// (10084) and `DeliverPolicy::New` with "consumer must be deliver all on
/// workqueue stream" (10101). So broadcast cannot use the default at all.
///
/// [`NatsRetention::Interest`] is used instead. It accepts the ephemeral
/// consumer, delivers every message to every subscriber, and retains nothing —
/// with `AckPolicy::None` interest is satisfied on delivery, so the stream stays
/// empty both while subscribers are live and when there are none. That is the
/// documented broadcast contract, and it is why NATS needs no "bound the stream"
/// caveat of the sort Redis carries.
///
/// An explicit [`nats_stream_config`](crate::TopologyBuilder::nats_stream_config)
/// is honoured — except for `WorkQueue`, which is refused here rather than left
/// to fail later as an opaque consumer-create error.
fn broadcast_stream_config(topology: &QueueTopology) -> Result<NatsStreamConfig> {
    let explicit = topology.nats_stream_config();
    let mut cfg = explicit.cloned().unwrap_or_default();
    if cfg.retention == NatsRetention::WorkQueue {
        if explicit.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' combines `.broadcast()` with \
                 `nats_stream_config(NatsRetention::WorkQueue)`, which JetStream cannot \
                 serve: a WorkQueue stream rejects both the `AckPolicy::None` and the \
                 `DeliverPolicy::New` that an ephemeral broadcast consumer requires. Use \
                 `NatsRetention::Interest` (retains nothing — the broadcast default) or \
                 `NatsRetention::Limits` (bounded replay).",
                topology.queue()
            )));
        }
        // Not a choice the caller made for this topology — just the crate-wide
        // default, which broadcast overrides.
        cfg.retention = NatsRetention::Interest;
    }
    Ok(cfg)
}

pub struct NatsTopologyDeclarer {
    client: NatsClient,
}

impl NatsTopologyDeclarer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    /// Create or **reconcile** a shove-managed stream. `config` is the caller's
    /// explicit [`NatsStreamConfig`] (retention/bounds/replicas) and is applied
    /// in full.
    ///
    /// With `None` a stream that does not exist yet is created with shove's
    /// historical defaults (WorkQueue / unbounded / single-replica). One that
    /// exists keeps the limits, replicas and other tuning it has: only the
    /// fields shove owns are set again (subjects, retention, storage, the
    /// discard policy and the duplicate window), so re-declaring a dead-letter
    /// or sequenced stream, whose config the topology cannot express, never
    /// undoes what an operator tuned.
    ///
    /// Uses `create_or_update_stream` rather than `get_or_create_stream` so that
    /// a config change actually takes effect on an already-existing stream:
    /// `get_or_create` returns the existing stream verbatim and would silently
    /// ignore the supplied config. Mutable changes (`max_age`/`max_bytes`/
    /// `max_messages`/`num_replicas`) are applied; immutable ones (`retention`,
    /// `storage`) make the JetStream UPDATE fail loud rather than no-op silently —
    /// the operator must recreate the stream (or use
    /// [`external`](crate::TopologyBuilder::external) to let infra own it).
    async fn create_stream(
        &self,
        name: &str,
        subjects: Vec<String>,
        config: Option<&NatsStreamConfig>,
    ) -> Result<()> {
        let desired = match config {
            Some(cfg) => declared_config(name, subjects, cfg),
            None => {
                let owned = declared_config(name, subjects, &NatsStreamConfig::default());
                match self.existing_stream_config(name).await? {
                    Some(existing) => with_owned_fields(existing, owned),
                    None => owned,
                }
            }
        };
        self.client
            .jetstream()
            .create_or_update_stream(desired)
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "create_or_update_stream({name}) failed (retention/storage are immutable on an existing stream): {e}"
                ))
            })?;

        Ok(())
    }

    /// The config of stream `name`, `None` when the server has no such stream.
    async fn existing_stream_config(&self, name: &str) -> Result<Option<StreamConfig>> {
        match self.client.jetstream().get_stream(name).await {
            Ok(stream) => Ok(Some(stream.cached_info().config.clone())),
            Err(e) => match e.kind() {
                GetStreamErrorKind::JetStream(error)
                    if error.error_code() == ErrorCode::STREAM_NOT_FOUND =>
                {
                    Ok(None)
                }
                _ => Err(map_get_stream_error(name, e)),
            },
        }
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();

        if topology.external() {
            // Bind to an infra-provisioned stream: verify it exists and fail fast
            // rather than silently creating a (differently-configured) fallback.
            self.client
                .jetstream()
                .get_stream(queue)
                .await
                .map_err(|e| {
                    ShoveError::Topology(format!(
                        "external(): stream `{queue}` must be provisioned before the consumer starts, but get_stream failed: {e}"
                    ))
                })?;
        } else {
            // The stream NAME stays the queue; only the subjects it captures
            // change. When `nats_subjects` is set, shove creates and owns a stream
            // over the externally-owned subject(s) rather than the queue name.
            let subjects = match topology.nats_stream_subjects() {
                Some(subjects) => subjects.to_vec(),
                None => vec![queue.to_string()],
            };
            // A broadcast topology cannot be served by shove's default
            // WorkQueue retention — see `broadcast_stream_config`.
            if topology.broadcast() {
                let cfg = broadcast_stream_config(topology)?;
                self.create_stream(queue, subjects, Some(&cfg)).await?;
            } else {
                self.create_stream(queue, subjects, topology.nats_stream_config())
                    .await?;
            }
        }

        // shove always owns its own dead-letter stream (its dead-letter mechanism,
        // not the infra-provided source stream), so create it in both modes.
        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()], None).await?;
        }

        Ok(())
    }

    async fn declare_sequenced(
        &self,
        topology: &QueueTopology,
        seq: &SequenceConfig,
    ) -> Result<()> {
        let queue = topology.queue();

        let subjects: Vec<String> = (0..seq.routing_shards())
            .map(|i| format!("{queue}.shard.{i}"))
            .collect();
        self.create_stream(queue, subjects, None).await?;

        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()], None).await?;
        }

        Ok(())
    }
}

impl NatsTopologyDeclarer {
    pub async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        match topology.sequencing() {
            Some(seq) => self.declare_sequenced(topology, seq).await,
            None => self.declare_standard(topology).await,
        }
    }

    /// Establish (or upsert) the durable pull consumer on `stream` with the
    /// given `max_ack_pending` budget.
    ///
    /// This is the single point where `CONSUMER.CREATE` is issued for a
    /// consumer group. Previously every consumer task in the group called
    /// `create_consumer` on its own reconnect, producing N redundant
    /// upserts of the same durable object on every reconnect storm. The
    /// per-consumer path now uses `get_consumer` instead (read-only).
    ///
    /// `max_ack_pending` is `-1`, unbounded, unless the group set a cap
    /// ([`durable_max_ack_pending`](super::consumer_group::durable_max_ack_pending)):
    /// each member bounds its own in-flight messages, and a cap here is
    /// shared by every member attached to the durable, in every process.
    ///
    /// `ack_wait` must leave headroom above the group's handler timeout so a
    /// handler running to its limit never has its message redelivered
    /// mid-flight — derive it with
    /// [`derive_ack_wait`](super::consumer::derive_ack_wait).
    pub(crate) async fn declare_pull_consumer(
        &self,
        stream: &str,
        consumer_name: &str,
        max_ack_pending: i64,
        ack_wait: Duration,
        filter_subjects: &[String],
    ) -> Result<()> {
        let stream = self
            .client
            .jetstream()
            .get_stream(stream)
            .await
            .map_err(|e| ShoveError::Topology(format!("get_stream({stream}) failed: {e}")))?;
        // When the stream is created over external subject(s), filter the durable
        // pull consumer to those subjects. A single subject uses `filter_subject`
        // (the common case); multiple use `filter_subjects`. With no override the
        // consumer reads the whole stream (unchanged default behavior).
        let (filter_subject, filter_subjects) = match filter_subjects {
            [] => (String::new(), Vec::new()),
            [one] => (one.clone(), Vec::new()),
            many => (String::new(), many.to_vec()),
        };
        stream
            .create_consumer(PullConsumerConfig {
                durable_name: Some(consumer_name.to_string()),
                ack_policy: AckPolicy::Explicit,
                max_ack_pending,
                ack_wait,
                filter_subject,
                filter_subjects,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "create_consumer({consumer_name}) on stream {} failed: {e}",
                    stream.cached_info().config.name
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_duplicate_window_is_the_shorter_of_the_default_and_max_age() {
        assert_eq!(duplicate_window(Duration::ZERO), DEFAULT_DUPLICATE_WINDOW);
        assert_eq!(
            duplicate_window(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            duplicate_window(Duration::from_secs(119)),
            Duration::from_secs(119)
        );
        assert_eq!(
            duplicate_window(Duration::from_secs(120)),
            DEFAULT_DUPLICATE_WINDOW
        );
        assert_eq!(
            duplicate_window(Duration::from_secs(3600)),
            DEFAULT_DUPLICATE_WINDOW
        );
    }

    /// A single-node server takes one replica only, so the replica count an
    /// operator sets is checked here, on the config the declaration builds.
    #[test]
    fn re_declaring_sets_the_owned_fields_and_keeps_replicas_and_limits() {
        let existing = StreamConfig {
            name: "orders-dlq".to_string(),
            subjects: vec!["orders-dlq".to_string(), "extra".to_string()],
            retention: RetentionPolicy::WorkQueue,
            storage: StorageType::File,
            discard: DiscardPolicy::Old,
            duplicate_window: Duration::from_secs(120),
            num_replicas: 3,
            max_bytes: 5_000_000,
            max_messages: 1_000,
            max_age: Duration::from_secs(60),
            description: Some("tuned by the operator".to_string()),
            ..Default::default()
        };
        let owned = declared_config(
            "orders-dlq",
            vec!["orders-dlq".to_string()],
            &NatsStreamConfig::default(),
        );

        let declared = with_owned_fields(existing, owned);

        assert_eq!(declared.subjects, ["orders-dlq"]);
        assert_eq!(declared.discard, DiscardPolicy::New);
        assert_eq!(declared.num_replicas, 3);
        assert_eq!(declared.max_bytes, 5_000_000);
        assert_eq!(declared.max_messages, 1_000);
        assert_eq!(declared.max_age, Duration::from_secs(60));
        assert_eq!(
            declared.description.as_deref(),
            Some("tuned by the operator")
        );
        assert_eq!(
            declared.duplicate_window,
            Duration::from_secs(60),
            "the window follows the max_age the operator set"
        );
    }

    #[test]
    fn an_explicit_config_is_applied_in_full() {
        let declared = declared_config(
            "orders",
            vec!["orders".to_string()],
            &NatsStreamConfig {
                retention: NatsRetention::Limits,
                max_age: Some(Duration::from_secs(30)),
                max_bytes: Some(1_000_000),
                max_messages: None,
                num_replicas: 3,
            },
        );

        assert_eq!(declared.retention, RetentionPolicy::Limits);
        assert_eq!(declared.max_age, Duration::from_secs(30));
        assert_eq!(declared.duplicate_window, Duration::from_secs(30));
        assert_eq!(declared.max_bytes, 1_000_000);
        assert_eq!(declared.max_messages, -1);
        assert_eq!(declared.num_replicas, 3);
    }
}
