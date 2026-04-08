use std::time::Duration;

use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};

use crate::ShoveError;
use crate::error::Result;
use crate::topology::{QueueTopology, TopologyDeclarer};

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

        // Main stream
        self.create_stream(queue, vec![queue.to_string()]).await?;

        // DLQ stream
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

        // Main stream with shard subjects
        let subjects: Vec<String> = (0..seq.routing_shards())
            .map(|i| format!("{queue}.shard.{i}"))
            .collect();
        self.create_stream(queue, subjects).await?;

        // DLQ stream
        if let Some(dlq) = topology.dlq() {
            self.create_stream(dlq, vec![dlq.to_string()]).await?;
        }

        Ok(())
    }
}

impl TopologyDeclarer for NatsTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_standard(topology).await
        }
    }
}
