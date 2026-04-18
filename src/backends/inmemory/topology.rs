use crate::error::Result;
use crate::topology::{QueueTopology, TopologyDeclarer};

use super::client::InMemoryBroker;

/// Creates the in-memory queues backing a [`QueueTopology`].
///
/// Idempotent — calling `declare` twice for the same topology is a no-op.
pub struct InMemoryTopologyDeclarer {
    broker: InMemoryBroker,
}

impl InMemoryTopologyDeclarer {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self { broker }
    }

    pub fn shard_queue_name(main_queue: &str, shard: u16) -> String {
        format!("{main_queue}-seq-{shard}")
    }
}

impl TopologyDeclarer for InMemoryTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        if let Some(seq) = topology.sequencing() {
            for shard in 0..seq.routing_shards() {
                let shard_name = Self::shard_queue_name(topology.queue(), shard);
                self.broker.declare(&shard_name);
                for hq in topology.shard_hold_queue_names(shard) {
                    self.broker.declare(hq.name());
                }
            }
        } else {
            self.broker.declare(topology.queue());
            for hq in topology.hold_queues() {
                self.broker.declare(hq.name());
            }
        }

        if let Some(dlq) = topology.dlq() {
            self.broker.declare(dlq);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::topology::{SequenceFailure, TopologyBuilder};

    #[tokio::test]
    async fn declares_main_dlq_and_hold_queues() {
        let broker = InMemoryBroker::new();
        let topology = TopologyBuilder::new("orders")
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(30))
            .dlq()
            .build();

        let declarer = InMemoryTopologyDeclarer::new(broker.clone());
        declarer.declare(&topology).await.unwrap();

        assert!(broker.lookup("orders").is_ok());
        assert!(broker.lookup("orders-hold-5s").is_ok());
        assert!(broker.lookup("orders-hold-30s").is_ok());
        assert!(broker.lookup("orders-dlq").is_ok());
    }

    #[tokio::test]
    async fn declare_is_idempotent() {
        let broker = InMemoryBroker::new();
        let topology = TopologyBuilder::new("idemp").dlq().build();
        let declarer = InMemoryTopologyDeclarer::new(broker.clone());
        declarer.declare(&topology).await.unwrap();
        declarer.declare(&topology).await.unwrap();
        assert!(broker.lookup("idemp").is_ok());
        assert!(broker.lookup("idemp-dlq").is_ok());
    }

    #[tokio::test]
    async fn declares_sequenced_shard_queues() {
        let broker = InMemoryBroker::new();
        let topology = TopologyBuilder::new("ledger")
            .sequenced(SequenceFailure::Skip)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();

        let declarer = InMemoryTopologyDeclarer::new(broker.clone());
        declarer.declare(&topology).await.unwrap();

        for shard in 0..4 {
            assert!(broker.lookup(&format!("ledger-seq-{shard}")).is_ok());
            assert!(
                broker
                    .lookup(&format!("ledger-seq-{shard}-hold-5s"))
                    .is_ok()
            );
        }
        assert!(broker.lookup("ledger-dlq").is_ok());
    }
}
