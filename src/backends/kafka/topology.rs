use crate::error::Result;
use crate::topology::QueueTopology;

use super::client::KafkaClient;
use super::constants::{DEFAULT_PARTITIONS, DEFAULT_REPLICATION};

pub struct KafkaTopologyDeclarer {
    client: KafkaClient,
    /// Minimum number of partitions for the main topic.
    /// When set (e.g. by consumer group registration), the partition count
    /// will be `max(default, min_partitions)` so that Kafka can distribute
    /// load across all consumers.
    min_partitions: Option<i32>,
    /// Replication factor applied to every auto-created topic (main, DLQ).
    /// `None` keeps the default of `1` (single-broker dev). Production
    /// clusters should set `3` (or whatever quorum the cluster sizes for).
    replication_factor: Option<i32>,
}

impl KafkaTopologyDeclarer {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            client,
            min_partitions: None,
            replication_factor: None,
        }
    }

    /// Ensure the main topic has at least `n` partitions.
    pub fn with_min_partitions(mut self, n: i32) -> Self {
        self.min_partitions = Some(n);
        self
    }

    /// Replication factor for auto-created topics. The default is `1` for
    /// single-broker development clusters; **set this to ≥ 3 in production**
    /// or pre-create topics out-of-band (Terraform, MSK console, etc.) —
    /// `create_topic` is idempotent and will not lower an existing topic's
    /// replication.
    ///
    /// # Panics
    ///
    /// Panics if `n < 1`.
    pub fn with_replication_factor(mut self, n: i32) -> Self {
        assert!(n >= 1, "replication_factor must be >= 1 (got {n})");
        self.replication_factor = Some(n);
        self
    }

    fn effective_partitions(&self, base: i32) -> i32 {
        match self.min_partitions {
            Some(min) => base.max(min),
            None => base,
        }
    }

    fn effective_replication(&self) -> i32 {
        self.replication_factor.unwrap_or(DEFAULT_REPLICATION)
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        let partitions = self.effective_partitions(DEFAULT_PARTITIONS);
        let replication = self.effective_replication();
        self.client
            .create_topic(queue, partitions, replication)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, replication)
                .await?;
        }

        Ok(())
    }

    async fn declare_sequenced(&self, topology: &QueueTopology) -> Result<()> {
        let queue = topology.queue();
        // sec-K-9: surface misuse as a typed error instead of panicking. The
        // caller path is gated by `topology.sequencing().is_some()` in
        // `declare`, so this branch is unreachable under correct callers —
        // but a Result keeps misuse from this internal helper recoverable
        // (vs. process abort) if a future caller wires it up wrong.
        let seq = topology.sequencing().ok_or_else(|| {
            crate::ShoveError::Topology(format!(
                "declare_sequenced called for {queue} without sequencing config"
            ))
        })?;

        let num_partitions = self.effective_partitions(seq.routing_shards() as i32);
        let replication = self.effective_replication();
        self.client
            .create_topic(queue, num_partitions, replication)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, replication)
                .await?;
        }

        Ok(())
    }
}

impl KafkaTopologyDeclarer {
    pub async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        // arch-K-9: Kafka simulates retry delays via deferred republish to
        // the same topic — no broker-side hold-queue topics are created.
        // Document the intentional omission so operators searching for
        // "where's my `{queue}-hold-{n}s` topic?" find the answer.
        let hold_count = topology.hold_queues().len();
        if hold_count > 0 {
            tracing::debug!(
                queue = topology.queue(),
                hold_queues = hold_count,
                "Kafka simulates retry delays via deferred republish — no broker-side \
                 hold-queue topics declared"
            );
        }
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_standard(topology).await
        }
    }
}
