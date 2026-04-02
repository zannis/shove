use std::future::Future;
use std::time::Duration;

use crate::error::ShoveError;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFailure {
    /// Dead-letter the failed message, skip it, sequence continues.
    Skip,
    /// Dead-letter the failed message AND all remaining messages in the sequence.
    FailAll,
}

// ---------------------------------------------------------------------------
// SequenceConfig
// ---------------------------------------------------------------------------

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
}

// ---------------------------------------------------------------------------
// TopologyBuilder
// ---------------------------------------------------------------------------

pub struct TopologyBuilder {
    queue: String,
    dlq: bool,
    hold_queues: Vec<Duration>,
    sequencing: Option<SequenceConfig>,
}

impl TopologyBuilder {
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            dlq: false,
            hold_queues: Vec::new(),
            sequencing: None,
        }
    }

    /// Enables sequencing with default 8 routing shards.
    /// Pre-computes the exchange name as `{queue}-seq-hash`.
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

    /// Adds a hold queue with the given delay.
    pub fn hold_queue(mut self, delay: Duration) -> Self {
        self.hold_queues.push(delay);
        self
    }

    /// Enables a dead-letter queue.
    pub fn dlq(mut self) -> Self {
        self.dlq = true;
        self
    }

    /// Builds the `QueueTopology`.
    /// Panics if sequencing is enabled with `routing_shards = 0`.
    pub fn build(self) -> QueueTopology {
        if let Some(ref seq) = self.sequencing {
            assert!(
                seq.routing_shards > 0,
                "routing_shards must be greater than 0 when sequencing is enabled"
            );
        }

        let dlq = if self.dlq {
            Some(format!("{}-dlq", self.queue))
        } else {
            None
        };

        let hold_queues = self
            .hold_queues
            .into_iter()
            .map(|delay| HoldQueue {
                name: format!("{}-hold-{}s", self.queue, delay.as_secs()),
                delay,
            })
            .collect();

        QueueTopology {
            queue: self.queue,
            dlq,
            hold_queues,
            sequencing: self.sequencing,
        }
    }
}

// ---------------------------------------------------------------------------
// TopologyDeclarer trait
// ---------------------------------------------------------------------------

pub trait TopologyDeclarer: Send + Sync {
    fn declare(
        &self,
        topology: &QueueTopology,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;
}

// ---------------------------------------------------------------------------
// Free function — uncomment when topic.rs is wired into lib.rs
// ---------------------------------------------------------------------------

pub async fn declare_topic<T: crate::topic::Topic>(
    declarer: &impl TopologyDeclarer,
) -> Result<(), ShoveError> {
    declarer.declare(T::topology()).await
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
    fn builder_dlq_name() {
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
    fn builder_sequenced_defaults() {
        let topology = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::Skip)
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
}
