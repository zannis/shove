use std::collections::BTreeMap;

use crate::error::Result;
use crate::topology::QueueTopology;

use super::client::KafkaClient;
use super::constants::{DEFAULT_PARTITIONS, DEFAULT_REPLICATION};

/// Merge declarer-level and per-topic config entries into one map: per-topic
/// wins key-by-key; within a layer, the later entry wins. BTreeMap keeps the
/// result deterministically ordered.
fn merge_topic_config(
    declarer: &[(String, String)],
    topic: &[(String, String)],
) -> Vec<(String, String)> {
    let mut merged: BTreeMap<&str, &str> = BTreeMap::new();
    for (k, v) in declarer {
        merged.insert(k, v);
    }
    for (k, v) in topic {
        merged.insert(k, v);
    }
    merged
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

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
    /// Declarer-level topic config entries (e.g. `retention.ms`) applied to
    /// every **main** topic this declarer creates. Per-topic entries from
    /// `TopologyBuilder::with_topic_config` override these key-by-key.
    /// DLQ topics are never touched.
    topic_config: Vec<(String, String)>,
    /// Guards for the mutually exclusive named retention helpers:
    /// `with_retention` sets the first, `with_retention_forever` the second.
    retention_finite: bool,
    retention_forever: bool,
}

impl KafkaTopologyDeclarer {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            client,
            min_partitions: None,
            replication_factor: None,
            topic_config: Vec::new(),
            retention_finite: false,
            retention_forever: false,
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

    /// Sets a Kafka topic-level config entry (e.g. `retention.ms`) applied to
    /// every **main** topic this declarer creates or reconciles. Repeatable;
    /// later calls for the same key win. Per-topic entries set via
    /// `TopologyBuilder::with_topic_config` override these key-by-key.
    /// DLQ topics keep cluster defaults.
    pub fn with_topic_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.topic_config.push((key.into(), value.into()));
        self
    }

    /// Sets `retention.ms` from a [`Duration`]. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    ///
    /// # Panics
    ///
    /// Panics if combined with
    /// [`with_retention_forever`](Self::with_retention_forever).
    pub fn with_retention(mut self, retention: std::time::Duration) -> Self {
        assert!(
            !self.retention_forever,
            "with_retention() cannot be combined with with_retention_forever() — both set retention.ms"
        );
        self.retention_finite = true;
        self.with_topic_config("retention.ms", retention.as_millis().to_string())
    }

    /// Sets `retention.ms = -1`, retaining messages forever. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    ///
    /// # Panics
    ///
    /// Panics if combined with [`with_retention`](Self::with_retention).
    pub fn with_retention_forever(mut self) -> Self {
        assert!(
            !self.retention_finite,
            "with_retention() cannot be combined with with_retention_forever() — both set retention.ms"
        );
        self.retention_forever = true;
        self.with_topic_config("retention.ms", "-1")
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
        let config = merge_topic_config(&self.topic_config, topology.kafka_topic_config());
        self.client
            .create_topic(queue, partitions, replication, &config)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, replication, &[])
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
        let config = merge_topic_config(&self.topic_config, topology.kafka_topic_config());
        self.client
            .create_topic(queue, num_partitions, replication, &config)
            .await?;

        if let Some(dlq) = topology.dlq() {
            self.client
                .create_topic(dlq, DEFAULT_PARTITIONS, replication, &[])
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

#[cfg(test)]
mod tests {
    use super::merge_topic_config;

    fn cfg(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn topic_entries_override_declarer_entries_per_key() {
        let declarer = cfg(&[("retention.ms", "604800000"), ("cleanup.policy", "delete")]);
        let topic = cfg(&[("retention.ms", "3600000")]);
        let merged = merge_topic_config(&declarer, &topic);
        assert_eq!(
            merged,
            cfg(&[("cleanup.policy", "delete"), ("retention.ms", "3600000")])
        );
    }

    #[test]
    fn last_write_wins_within_a_layer() {
        let declarer = cfg(&[("retention.ms", "1000"), ("retention.ms", "2000")]);
        let merged = merge_topic_config(&declarer, &[]);
        assert_eq!(merged, cfg(&[("retention.ms", "2000")]));
    }

    #[test]
    fn empty_layers_merge_to_empty() {
        assert!(merge_topic_config(&[], &[]).is_empty());
    }
}
