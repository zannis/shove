use std::time::Duration;
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

/// Controls what happens to remaining messages in a sequence when one message
/// fails permanently (exceeds max retries or returns [`Reject`](crate::Outcome::Reject)).
///
/// Both policies dead-letter the failed message itself. They differ in how
/// they treat *subsequent* messages that share the same sequence key.
///
/// # Choosing a policy
///
/// - Use [`Skip`](Self::Skip) when messages are **independently valid** but
///   happen to need ordered delivery (e.g. audit-log entries, analytics events).
///   A single bad event should not block the rest of the stream.
///
/// - Use [`FailAll`](Self::FailAll) when messages are **causally dependent** —
///   each message assumes every prior message in the sequence was processed
///   successfully (e.g. financial ledger entries, state-machine transitions).
///   Processing later messages after an earlier one failed would leave the
///   system in an inconsistent state.
///
/// # Example
///
/// Given a sequence for key `ACC-A` with messages `[1, 2, 3, 4, 5]` where
/// message 3 is permanently rejected:
///
/// | Policy   | DLQ'd messages | Ack'd messages |
/// |----------|---------------|----------------|
/// | `Skip`   | 3             | 1, 2, 4, 5    |
/// | `FailAll`| 3, 4, 5       | 1, 2           |
///
/// Messages for *other* sequence keys (e.g. `ACC-B`) are unaffected by either
/// policy — poisoning is scoped to the failing key only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFailure {
    /// Dead-letter the failed message, skip it, and continue processing
    /// subsequent messages in the sequence as normal.
    Skip,
    /// Dead-letter the failed message and automatically dead-letter all
    /// remaining messages for the same sequence key ("poison" the key).
    /// The key stays poisoned for the lifetime of the consumer process.
    FailAll,
}

// ---------------------------------------------------------------------------
// SequenceConfig
// ---------------------------------------------------------------------------

/// Configuration for sequenced (strictly ordered) delivery on a topic.
///
/// Created automatically by [`TopologyBuilder::sequenced`]. The config
/// determines:
///
/// - **`on_failure`** — the [`SequenceFailure`] policy (`Skip` or `FailAll`).
/// - **`routing_shards`** — how many sub-queues the consistent-hash exchange
///   fans out to. More shards allow higher parallelism while preserving
///   per-key ordering. Default: **8**.
/// - **`exchange`** — the name of the consistent-hash exchange (derived from
///   the queue name as `{queue}-seq-hash`).
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

    pub fn shard_hold_queue_names(&self, shard_index: u16) -> Vec<HoldQueue> {
        self.hold_queues
            .iter()
            .map(|hq| HoldQueue {
                name: format!(
                    "{}-seq-{shard_index}-hold-{}s",
                    self.queue,
                    hq.delay.as_secs()
                ),
                delay: hq.delay,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// TopologyBuilder
// ---------------------------------------------------------------------------

pub struct TopologyBuilder {
    queue: String,
    dlq: Option<String>,
    hold_queues: Vec<Duration>,
    sequencing: Option<SequenceConfig>,
    allow_message_loss: bool,
}

impl TopologyBuilder {
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            dlq: None,
            hold_queues: Vec::new(),
            sequencing: None,
            allow_message_loss: false,
        }
    }

    /// Enables strict per-key ordered delivery for this topic.
    ///
    /// Messages are routed through a consistent-hash exchange so that all
    /// messages sharing the same sequence key land on the same sub-queue and
    /// are processed in publish order.
    ///
    /// The `on_failure` policy determines what happens when a message is
    /// permanently rejected — see [`SequenceFailure`] for details.
    ///
    /// Defaults to **8** routing shards (override with [`routing_shards`](Self::routing_shards)).
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

    /// Adds a hold queue with the given delay for retry backoff.
    ///
    /// Hold queues are selected in order by retry count. Define multiple hold
    /// queues with increasing delays to get escalating backoff. Once the retry
    /// count exceeds the number of hold queues, messages keep going to the last
    /// (longest-delay) hold queue on every subsequent retry until
    /// [`ConsumerOptions::max_retries`](crate::ConsumerOptions::max_retries) is exhausted, at which point the
    /// message is routed to the DLQ.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // With max_retries = 5:
    /// // retry 0 → hold-1s, retry 1 → hold-5s,
    /// // retries 2..4 → hold-30s (clamped to last),
    /// // retry 5 → DLQ
    /// TopologyBuilder::new("orders")
    ///     .hold_queue(Duration::from_secs(1))   // 1st retry: 1s
    ///     .hold_queue(Duration::from_secs(5))   // 2nd retry: 5s
    ///     .hold_queue(Duration::from_secs(30))  // 3rd+ retries: 30s until DLQ
    ///     .dlq()
    ///     .build();
    /// ```
    pub fn hold_queue(mut self, delay: Duration) -> Self {
        self.hold_queues.push(delay);
        self
    }

    /// Enables a dead-letter queue with the default name `{queue}-dlq`.
    pub fn dlq(mut self) -> Self {
        self.dlq = Some(format!("{}-dlq", self.queue));
        self
    }

    /// Enables a dead-letter queue with a custom name.
    ///
    /// Use this when the default `{queue}-dlq` suffix doesn't match your
    /// naming convention or when the DLQ is shared across topics.
    ///
    /// # Example
    ///
    /// ```ignore
    /// TopologyBuilder::new("orders")
    ///     .dlq_named("orders-dead-letters")
    ///     .build();
    /// ```
    pub fn dlq_named(mut self, name: impl Into<String>) -> Self {
        self.dlq = Some(name.into());
        self
    }

    /// Acknowledge that failed messages in this sequenced topic may be
    /// permanently lost.
    ///
    /// By default, `build()` panics if a sequenced topic is missing a DLQ or
    /// hold queues, because that means rejected messages are silently
    /// discarded. Call this method to suppress those guards when message loss
    /// is acceptable (e.g. ephemeral metrics, best-effort notifications).
    ///
    /// Has no effect on non-sequenced topics.
    pub fn allow_message_loss(mut self) -> Self {
        self.allow_message_loss = true;
        self
    }

    /// Builds the `QueueTopology`.
    ///
    /// # Panics
    ///
    /// Panics when the configuration is invalid:
    /// - Sequencing enabled with `routing_shards = 0`.
    /// - Sequencing enabled without a DLQ (unless
    ///   [`allow_message_loss`](Self::allow_message_loss) is set).
    /// - Sequencing enabled without at least one hold queue (unless
    ///   [`allow_message_loss`](Self::allow_message_loss) is set).
    pub fn build(self) -> QueueTopology {
        if let Some(ref seq) = self.sequencing {
            assert!(
                seq.routing_shards > 0,
                "routing_shards must be greater than 0 when sequencing is enabled"
            );
            if !self.allow_message_loss {
                assert!(
                    self.dlq.is_some(),
                    "sequenced topics require a DLQ — call .dlq() or .dlq_named() or .allow_message_loss() before .build()"
                );
                assert!(
                    !self.hold_queues.is_empty(),
                    "sequenced topics require at least one hold queue — call .hold_queue() or .allow_message_loss() before .build()"
                );
            }
        }

        // Warn about non-sequenced topics with incomplete retry infrastructure.
        if self.sequencing.is_none() && !self.allow_message_loss {
            if !self.hold_queues.is_empty() && self.dlq.is_none() {
                tracing::warn!(
                    queue = self.queue,
                    "topic has hold queues but no DLQ — messages exhausting max_retries will be silently discarded"
                );
            }
            if self.dlq.is_some() && self.hold_queues.is_empty() {
                tracing::warn!(
                    queue = self.queue,
                    "topic has a DLQ but no hold queues — retries will use broker redelivery with no delay"
                );
            }
        }

        let dlq = self.dlq;

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
    fn builder_dlq_named() {
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
            .hold_queue(Duration::from_secs(5))
            .dlq()
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
            .hold_queue(Duration::from_secs(5))
            .dlq()
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
            .hold_queue(Duration::from_secs(5))
            .dlq()
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

    #[test]
    #[should_panic(expected = "sequenced topics require a DLQ")]
    fn builder_sequenced_without_dlq_panics() {
        let _ = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .build();
    }

    #[test]
    #[should_panic(expected = "sequenced topics require at least one hold queue")]
    fn builder_sequenced_without_hold_queue_panics() {
        let _ = TopologyBuilder::new("orders")
            .sequenced(SequenceFailure::FailAll)
            .dlq()
            .build();
    }

    #[test]
    fn builder_allow_message_loss_suppresses_dlq_guard() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .allow_message_loss()
            .build();
        assert!(topology.dlq().is_none());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn builder_allow_message_loss_suppresses_hold_queue_guard() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::FailAll)
            .dlq()
            .allow_message_loss()
            .build();
        assert!(topology.hold_queues().is_empty());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn shard_hold_queue_names() {
        let topology = TopologyBuilder::new("payments")
            .sequenced(SequenceFailure::FailAll)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(60))
            .dlq()
            .build();

        let names = topology.shard_hold_queue_names(2);
        assert_eq!(names.len(), 2);
        assert_eq!(names[0].name(), "payments-seq-2-hold-5s");
        assert_eq!(names[0].delay(), Duration::from_secs(5));
        assert_eq!(names[1].name(), "payments-seq-2-hold-60s");
        assert_eq!(names[1].delay(), Duration::from_secs(60));
    }

    #[test]
    fn builder_allow_message_loss_suppresses_both_guards() {
        let topology = TopologyBuilder::new("ephemeral")
            .sequenced(SequenceFailure::Skip)
            .allow_message_loss()
            .build();
        assert!(topology.dlq().is_none());
        assert!(topology.hold_queues().is_empty());
        assert!(topology.sequencing().is_some());
    }

    #[test]
    fn builder_dlq_custom_name() {
        let topology = TopologyBuilder::new("orders")
            .dlq_named("orders-dead-letters")
            .build();
        assert_eq!(topology.dlq(), Some("orders-dead-letters"));
    }

    #[test]
    fn builder_dlq_default_suffix_unchanged() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        assert_eq!(topology.dlq(), Some("orders-dlq"));
    }

    #[test]
    fn builder_dlq_name_overrides_dlq() {
        let topology = TopologyBuilder::new("orders")
            .dlq()
            .dlq_named("custom-dead")
            .build();
        assert_eq!(topology.dlq(), Some("custom-dead"));
    }

    #[test]
    fn builder_dlq_after_dlq_name_uses_default() {
        let topology = TopologyBuilder::new("orders")
            .dlq_named("custom-dead")
            .dlq()
            .build();
        assert_eq!(topology.dlq(), Some("orders-dlq"));
    }

    #[test]
    fn builder_dlq_name_with_sequenced() {
        let topology = TopologyBuilder::new("events")
            .sequenced(SequenceFailure::Skip)
            .routing_shards(4)
            .hold_queue(Duration::from_secs(5))
            .dlq_named("events-failed")
            .build();
        assert_eq!(topology.dlq(), Some("events-failed"));
        assert!(topology.sequencing().is_some());
    }
}
