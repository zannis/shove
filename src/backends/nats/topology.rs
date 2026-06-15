use std::time::Duration;

use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};

use crate::ShoveError;
use crate::error::Result;
use crate::topology::QueueTopology;

use super::client::NatsClient;

pub struct NatsTopologyDeclarer {
    client: NatsClient,
}

impl NatsTopologyDeclarer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    async fn create_stream(&self, name: &str, subjects: Vec<String>) -> Result<()> {
        self.client
            .jetstream()
            .get_or_create_stream(StreamConfig {
                name: name.to_string(),
                subjects,
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                discard: DiscardPolicy::New,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .map_err(|e| ShoveError::Topology(e.to_string()))?;

        Ok(())
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();

        // The stream NAME stays the queue; only the subjects it captures change.
        // When `nats_subjects` is set, shove creates and owns a WorkQueue stream
        // over the externally-owned subject(s) rather than the queue name.
        let subjects = match topology.nats_stream_subjects() {
            Some(subjects) => subjects.to_vec(),
            None => vec![queue.to_string()],
        };
        self.create_stream(queue, subjects).await?;

        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()]).await?;
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
        self.create_stream(queue, subjects).await?;

        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()]).await?;
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
    pub(crate) async fn declare_pull_consumer(
        &self,
        stream: &str,
        consumer_name: &str,
        max_ack_pending: i64,
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
