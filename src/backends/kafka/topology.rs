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

/// `true` once `replication` is too low to ever satisfy the producer's
/// hardcoded `acks=all` on a cluster with a non-default (≥ 2)
/// `min.insync.replicas` — see `warn_if_under_replicated_for_acks_all`.
fn under_replicated_for_acks_all(replication: i32) -> bool {
    replication < 2
}

/// Warn once per `declare()` call when auto-creating topic(s) at the RF=1
/// default while the producer hardcodes `acks=all` (see
/// `KafkaClient::connect`'s `sec-K-10` comment) — the pairing is a footgun:
/// on any cluster whose `min.insync.replicas` is >= 2 (a common production
/// default), a produce to an RF=1 topic can never satisfy `acks=all` and
/// surfaces only as a bare `MessageTimedOut`, with no hint that the actual
/// cause is under-replication. `create_topic` is also idempotent and won't
/// raise an existing topic's replication once created, so this is easy to
/// overlook locally (RF=1 works fine on a single-broker dev cluster) and
/// only bites in production.
///
/// Deliberately does not query the cluster's actual `min.insync.replicas`
/// (an extra admin round trip) — this is a blunt, unconditional nudge
/// toward `with_replication_factor(n ≥ 2)`.
///
/// A free function rather than a method so it is reachable from unit tests
/// without standing up a `KafkaClient`.
fn warn_if_under_replicated_for_acks_all(queue: &str, replication: i32) {
    if under_replicated_for_acks_all(replication) {
        tracing::warn!(
            queue,
            replication_factor = replication,
            "auto-creating Kafka topic(s) at replication_factor=1 while the producer \
             requires acks=all — on any cluster with min.insync.replicas >= 2 this topic \
             can never satisfy a produce and every send will time out as a bare \
             MessageTimedOut with no hint of the real cause. Call \
             KafkaTopologyDeclarer::with_replication_factor(n) with n >= 2 to match your \
             cluster's min.insync.replicas, or ignore this on a single-broker dev cluster."
        );
    }
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
        warn_if_under_replicated_for_acks_all(topology.queue(), self.effective_replication());
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_standard(topology).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        merge_topic_config, under_replicated_for_acks_all, warn_if_under_replicated_for_acks_all,
    };

    #[test]
    fn under_replicated_for_acks_all_flags_rf1() {
        assert!(under_replicated_for_acks_all(1));
    }

    #[test]
    fn under_replicated_for_acks_all_accepts_rf2_and_above() {
        assert!(!under_replicated_for_acks_all(2));
        assert!(!under_replicated_for_acks_all(3));
    }

    /// RF=1 takes the warning arm. There is no subscriber installed here, so
    /// this asserts the call is total (does not panic) and covers the
    /// `tracing::warn!` body — the emitted event itself is asserted in
    /// `warn_if_under_replicated_for_acks_all_emits_event_for_rf1`.
    #[test]
    fn warn_if_under_replicated_for_acks_all_warns_on_rf1() {
        warn_if_under_replicated_for_acks_all("kafka-under-replicated", 1);
    }

    /// RF >= 2 takes the silent arm.
    #[test]
    fn warn_if_under_replicated_for_acks_all_is_silent_from_rf2() {
        warn_if_under_replicated_for_acks_all("kafka-replicated", 2);
        warn_if_under_replicated_for_acks_all("kafka-replicated", 3);
    }

    /// The RF=1 arm actually emits a WARN carrying the queue and the
    /// offending replication factor, and the RF>=2 arm emits nothing — the
    /// warning is the entire deliverable of this code path, so assert on the
    /// event rather than just on non-panicking.
    #[test]
    fn warn_if_under_replicated_for_acks_all_emits_event_for_rf1() {
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};
        use tracing::subscriber::with_default;

        #[derive(Default)]
        struct Captured {
            queues: Vec<String>,
            replication_factors: Vec<i64>,
        }

        struct CaptureVisitor<'a>(&'a mut Captured);
        impl Visit for CaptureVisitor<'_> {
            fn record_i64(&mut self, field: &Field, value: i64) {
                if field.name() == "replication_factor" {
                    self.0.replication_factors.push(value);
                }
            }
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "queue" {
                    self.0.queues.push(value.to_string());
                }
            }
            fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
        }

        struct CaptureSubscriber(Arc<Mutex<Captured>>);
        impl tracing::Subscriber for CaptureSubscriber {
            fn enabled(&self, meta: &tracing::Metadata<'_>) -> bool {
                *meta.level() == tracing::Level::WARN
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                let mut captured = self.0.lock().unwrap();
                event.record(&mut CaptureVisitor(&mut captured));
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }

        let captured = Arc::new(Mutex::new(Captured::default()));
        with_default(CaptureSubscriber(captured.clone()), || {
            warn_if_under_replicated_for_acks_all("kafka-rf1", 1);
            warn_if_under_replicated_for_acks_all("kafka-rf3", 3);
        });

        let captured = captured.lock().unwrap();
        assert_eq!(captured.queues, vec!["kafka-rf1".to_string()]);
        assert_eq!(captured.replication_factors, vec![1]);
    }

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
