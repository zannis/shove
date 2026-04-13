use crate::error::Result;
use crate::topology::{QueueTopology, TopologyDeclarer};

use super::client::KafkaClient;

/// Default number of partitions for standard (non-sequenced) topics.
const DEFAULT_PARTITIONS: i32 = 8;
/// Default replication factor.
const DEFAULT_REPLICATION: i32 = 1;

pub struct KafkaTopologyDeclarer {
    client: KafkaClient,
}

impl KafkaTopologyDeclarer {
    pub fn new(client: KafkaClient) -> Self {
        Self { client }
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        self.client
            .create_topic(queue, DEFAULT_PARTITIONS, DEFAULT_REPLICATION)
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

        let num_partitions = seq.routing_shards() as i32;
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
