use crate::error::Result;
use crate::topology::{QueueTopology, TopologyDeclarer};

use super::client::KafkaClient;

/// Default number of partitions for standard (non-sequenced) topics.
const DEFAULT_PARTITIONS: i32 = 8;
/// Default replication factor.
const DEFAULT_REPLICATION: i32 = 1;

pub struct KafkaTopologyDeclarer {
    client: KafkaClient,
    /// Minimum number of partitions for the main topic.
    /// When set (e.g. by consumer group registration), the partition count
    /// will be `max(default, min_partitions)` so that Kafka can distribute
    /// load across all consumers.
    min_partitions: Option<i32>,
}

impl KafkaTopologyDeclarer {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            client,
            min_partitions: None,
        }
    }

    /// Ensure the main topic has at least `n` partitions.
    pub fn with_min_partitions(mut self, n: i32) -> Self {
        self.min_partitions = Some(n);
        self
    }

    fn effective_partitions(&self, base: i32) -> i32 {
        match self.min_partitions {
            Some(min) => base.max(min),
            None => base,
        }
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        let partitions = self.effective_partitions(DEFAULT_PARTITIONS);
        self.client
            .create_topic(queue, partitions, DEFAULT_REPLICATION)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, DEFAULT_REPLICATION)
                .await?;
        }

        Ok(())
    }

    async fn declare_sequenced(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        let seq = topology
            .sequencing()
            .expect("sequenced topology must have sequencing config");

        let num_partitions = self.effective_partitions(seq.routing_shards() as i32);
        self.client
            .create_topic(queue, num_partitions, DEFAULT_REPLICATION)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, DEFAULT_REPLICATION)
                .await?;
        }

        Ok(())
    }
}

impl TopologyDeclarer for KafkaTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_standard(topology).await
        }
    }
}
