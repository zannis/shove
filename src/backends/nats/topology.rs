use std::time::Duration;

use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};

use crate::ShoveError;
use crate::error::Result;
use crate::topology::{NatsRetention, NatsStreamConfig, QueueTopology};

use super::client::NatsClient;

pub struct NatsTopologyDeclarer {
    client: NatsClient,
}

impl NatsTopologyDeclarer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    /// Create or **reconcile** a shove-managed stream. `config` is the caller's
    /// explicit [`NatsStreamConfig`] (retention/bounds/replicas); `None` keeps
    /// shove's historical defaults (WorkQueue / unbounded / single-replica).
    ///
    /// Uses `create_or_update_stream` rather than `get_or_create_stream` so that
    /// a config change actually takes effect on an already-existing stream:
    /// `get_or_create` returns the existing stream verbatim and would silently
    /// ignore the supplied config. Mutable changes (`max_age`/`max_bytes`/
    /// `max_messages`/`num_replicas`) are applied; immutable ones (`retention`,
    /// `storage`) make the JetStream UPDATE fail loud rather than no-op silently —
    /// the operator must recreate the stream (or use
    /// [`nats_external_stream`](crate::TopologyBuilder::nats_external_stream) to
    /// let infra own it).
    async fn create_stream(
        &self,
        name: &str,
        subjects: Vec<String>,
        config: Option<&NatsStreamConfig>,
    ) -> Result<()> {
        let cfg = config.cloned().unwrap_or_default();
        let retention = match cfg.retention {
            NatsRetention::WorkQueue => RetentionPolicy::WorkQueue,
            NatsRetention::Limits => RetentionPolicy::Limits,
            NatsRetention::Interest => RetentionPolicy::Interest,
        };
        self.client
            .jetstream()
            .create_or_update_stream(StreamConfig {
                name: name.to_string(),
                subjects,
                retention,
                storage: StorageType::File,
                discard: DiscardPolicy::New,
                duplicate_window: Duration::from_secs(120),
                // `None` maps to JetStream's "unlimited" sentinels (Duration::ZERO
                // / -1), preserving the historical unbounded default.
                max_age: cfg.max_age.unwrap_or_default(),
                max_bytes: cfg.max_bytes.unwrap_or(-1),
                max_messages: cfg.max_messages.unwrap_or(-1),
                num_replicas: cfg.num_replicas.max(1),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "create_or_update_stream({name}) failed (retention/storage are immutable on an existing stream): {e}"
                ))
            })?;

        Ok(())
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();

        if topology.nats_external_stream() {
            // Bind to an infra-provisioned stream: verify it exists and fail fast
            // rather than silently creating a (differently-configured) fallback.
            self.client
                .jetstream()
                .get_stream(queue)
                .await
                .map_err(|e| {
                    ShoveError::Topology(format!(
                        "nats_external_stream: stream `{queue}` must be provisioned before the consumer starts, but get_stream failed: {e}"
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
            self.create_stream(queue, subjects, topology.nats_stream_config())
                .await?;
        }

        // shove always owns its own dead-letter stream (its dead-letter mechanism,
        // not the infra-provided source stream), so create it in both modes.
        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()], None).await?;
        }

        Ok(())
    }

    async fn declare_sequenced(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        let seq = topology
            .sequencing()
            .expect("sequenced topology must have sequencing config");

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
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_standard(topology).await
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
    /// `max_ack_pending` must reflect the **aggregate** in-flight budget
    /// for the whole group (typically `prefetch_count × max_consumers`).
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
