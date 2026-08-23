//! Operator-initiated offset re-anchoring for an existing Kafka consumer
//! group.
//!
//! `auto.offset.reset` only decides where a group starts when it has *no*
//! usable committed offset. Once a group has committed, the only way to move
//! it — short of minting a throwaway group ID and stranding the old one — is
//! to rewrite its committed offsets directly. That is what this module does:
//! the library-side equivalent of
//! `kafka-consumer-groups.sh --reset-offsets --execute`.
//!
//! Like that tool, a reset is only accepted while the group is **inactive**
//! (no live members). Kafka's group coordinator enforces this: a commit that
//! carries no member ID is only honoured for an `Empty` group. We check the
//! group's member list first so the failure is an actionable shove error
//! rather than a bare `UNKNOWN_MEMBER_ID` from librdkafka.

#[cfg(feature = "kafka-msk-iam")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer as RdkafkaConsumer, ConsumerContext};
use rdkafka::{Offset, TopicPartitionList};

#[cfg(feature = "kafka-msk-iam")]
use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};
use crate::topology::QueueTopology;

use super::client::KafkaClient;
use super::consumer_group::KafkaConsumerGroupConfig;
#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;

/// Timeout for each broker RPC issued during a reset. Generous: a reset is a
/// one-shot operator action, not a hot path, and the group coordinator may
/// still be electing when it runs.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Where to re-anchor a consumer group's committed offsets.
///
/// Mirrors the `--to-*` flags of `kafka-consumer-groups.sh --reset-offsets`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaOffsetReset {
    /// The low watermark of every partition: replay all retained history.
    Earliest,
    /// The high watermark of every partition: skip everything currently on
    /// the topic and tail from here. This is the "seek to end" case — a
    /// latest-value sink that must serve fresh data now, not after crawling
    /// days of backlog.
    Latest,
    /// The first offset whose record timestamp is at or after the given point,
    /// in **milliseconds since the Unix epoch** (Kafka's own timestamp unit,
    /// as taken by `--to-datetime`).
    ///
    /// Partitions with no record at or after that point re-anchor at their
    /// high watermark, matching Kafka's `offsetsForTimes` contract.
    Timestamp(i64),
}

impl KafkaOffsetReset {
    /// Short label used in the tracing span and in error text.
    fn label(self) -> &'static str {
        match self {
            KafkaOffsetReset::Earliest => "earliest",
            KafkaOffsetReset::Latest => "latest",
            KafkaOffsetReset::Timestamp(_) => "timestamp",
        }
    }
}

/// What one partition's committed offset moved from and to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KafkaPartitionOffsetReset {
    partition: i32,
    previous: Option<i64>,
    new: i64,
}

impl KafkaPartitionOffsetReset {
    /// The partition this entry describes.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The group's committed offset before the reset, or `None` if it had
    /// never committed this partition.
    pub fn previous(&self) -> Option<i64> {
        self.previous
    }

    /// The offset now committed for this partition — the next record the
    /// group will read.
    pub fn new_offset(&self) -> i64 {
        self.new
    }

    /// Signed distance the group moved, or `None` if it had no prior commit
    /// to move from. Positive means records were skipped, negative means
    /// history will be replayed.
    pub fn delta(&self) -> Option<i64> {
        self.previous.map(|p| self.new.saturating_sub(p))
    }
}

/// Outcome of a completed offset reset — one entry per partition, ascending.
///
/// Worth logging verbatim: it is the audit trail for a destructive operator
/// action, and the `previous` offsets are the only record of where the group
/// was before it moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaOffsetResetReport {
    queue: String,
    group_id: String,
    partitions: Vec<KafkaPartitionOffsetReset>,
}

impl KafkaOffsetResetReport {
    /// The topic whose offsets were rewritten.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The Kafka consumer group whose offsets were rewritten.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Per-partition detail, ordered by partition ID.
    pub fn partitions(&self) -> &[KafkaPartitionOffsetReset] {
        &self.partitions
    }

    /// `true` when every partition already sat at its target — the reset
    /// committed the offsets the group was already on and changed nothing.
    pub fn is_noop(&self) -> bool {
        self.partitions.iter().all(|p| p.previous == Some(p.new))
    }
}

/// The group ID a reset must target for `topology`.
///
/// Kept next to the reset itself, and derived from the same two inputs
/// `register` / `register_fifo` use, so the two cannot drift: a reset that
/// resolves a different group ID than the consumers join rewrites offsets
/// nothing reads, and reports success while doing it.
pub(crate) fn resolved_reset_group_id(
    config: &KafkaConsumerGroupConfig,
    topology: &QueueTopology,
) -> String {
    let queue = topology.queue();
    let fan_out = topology.consumer_group();
    if topology.sequencing().is_some() {
        config.resolved_fifo_group_id(queue, fan_out)
    } else {
        config.resolved_group_id(queue, fan_out)
    }
}

/// Target offset for one partition given its watermarks. `Timestamp` is not
/// answerable from watermarks alone and returns `None` — it needs the
/// broker-side `offsets_for_times` lookup instead.
fn target_from_watermarks(to: KafkaOffsetReset, low: i64, high: i64) -> Option<i64> {
    match to {
        KafkaOffsetReset::Earliest => Some(low),
        KafkaOffsetReset::Latest => Some(high),
        KafkaOffsetReset::Timestamp(_) => None,
    }
}

/// Target offset for one partition from an `offsets_for_times` result.
///
/// Kafka answers "no record at or after that timestamp" with a null offset,
/// which librdkafka surfaces as `Offset::End` (and, for a partition it could
/// not resolve at all, `Offset::Invalid`). Both mean the same thing for a
/// re-anchor: there is nothing left to read from that point, so the group
/// belongs at the tail. Anything else is a concrete offset.
///
/// Every branch resolves to a real offset inside `low..=high`, never a
/// symbolic one: a committed offset outside the retained range would be
/// silently reinterpreted by `auto.offset.reset` on the next join, quietly
/// undoing the reset.
fn target_from_timestamp_lookup(looked_up: Offset, low: i64, high: i64) -> i64 {
    match looked_up {
        Offset::Offset(n) if n >= 0 => n.clamp(low, high),
        Offset::Beginning => low,
        _ => high,
    }
}

/// Committed offset for one partition as reported by `committed_offsets`.
/// Anything that is not a concrete non-negative offset means "this group has
/// never committed here".
fn previous_committed(offset: Offset) -> Option<i64> {
    match offset {
        Offset::Offset(n) if n >= 0 => Some(n),
        _ => None,
    }
}

/// Rewrite `group_id`'s committed offsets for every partition of `queue`.
///
/// Runs the whole sequence on a blocking thread: every librdkafka call below
/// is synchronous, and a reset is a one-shot operation, so one `spawn_blocking`
/// for the lot is simpler than per-RPC hops.
pub(crate) async fn reset_group_offsets(
    client: &KafkaClient,
    queue: &str,
    group_id: &str,
    to: KafkaOffsetReset,
) -> Result<KafkaOffsetResetReport> {
    let mut cfg = client.base_config();
    cfg.set("group.id", group_id);
    // This client only rewrites offsets; it must never join the group (that
    // would make the group active and disqualify its own commit) and must
    // never move offsets on its own.
    cfg.set("enable.auto.commit", "false");

    #[cfg(feature = "kafka-msk-iam")]
    let msk_ctx = client.msk_context();
    #[cfg(feature = "kafka-msk-iam")]
    let shutdown = client.shutdown_token();

    let queue = queue.to_string();
    let group_id = group_id.to_string();

    tokio::task::spawn_blocking(move || {
        reset_group_offsets_blocking(
            cfg,
            &queue,
            &group_id,
            to,
            #[cfg(feature = "kafka-msk-iam")]
            msk_ctx,
            #[cfg(feature = "kafka-msk-iam")]
            shutdown,
        )
    })
    .await
    .map_err(|e| ShoveError::Topology(format!("offset reset task failed: {e}")))?
}

fn reset_group_offsets_blocking(
    cfg: ClientConfig,
    queue: &str,
    group_id: &str,
    to: KafkaOffsetReset,
    #[cfg(feature = "kafka-msk-iam")] msk_ctx: Option<MskIamContext>,
    #[cfg(feature = "kafka-msk-iam")] shutdown: CancellationToken,
) -> Result<KafkaOffsetResetReport> {
    #[cfg(feature = "kafka-msk-iam")]
    if let Some(ctx) = msk_ctx {
        let consumer: BaseConsumer<MskIamContext> = cfg.create_with_context(ctx).map_err(|e| {
            ShoveError::Topology(format!("failed to create MSK offset-reset consumer: {e}"))
        })?;
        // This consumer never subscribes, so nothing services librdkafka's
        // event queue and the initial OAUTHBEARER token would never be
        // delivered. Pump it on a scoped thread for the duration of the
        // blocking work; the thread is joined before this block returns.
        // Same shape as `fetch_topic_partition_count_blocking`.
        let done = AtomicBool::new(false);
        return std::thread::scope(|s| {
            s.spawn(|| {
                while !done.load(Ordering::Relaxed) && !shutdown.is_cancelled() {
                    let _ = consumer.poll(Duration::from_millis(100));
                }
            });
            let out = run_reset(&consumer, queue, group_id, to);
            done.store(true, Ordering::Relaxed);
            out
        });
    }

    let consumer: BaseConsumer = cfg.create().map_err(|e| {
        ShoveError::Topology(format!("failed to create offset-reset consumer: {e}"))
    })?;
    run_reset(&consumer, queue, group_id, to)
}

/// The reset itself, generic over whichever `BaseConsumer` flavour the auth
/// mode produced. `Ctx` must stay generic: rdkafka's `Consumer` trait is
/// parameterised by the client context, so pinning it to the default would
/// exclude `BaseConsumer<MskIamContext>` — see `context_generic_smoke`.
fn run_reset<C, Ctx>(
    consumer: &C,
    queue: &str,
    group_id: &str,
    to: KafkaOffsetReset,
) -> Result<KafkaOffsetResetReport>
where
    Ctx: ConsumerContext,
    C: RdkafkaConsumer<Ctx> + Sized,
{
    ensure_group_inactive(consumer, queue, group_id)?;

    let metadata = consumer
        .fetch_metadata(Some(queue), RPC_TIMEOUT)
        .map_err(|e| {
            ShoveError::Connection(format!("failed to fetch metadata for {queue}: {e}"))
        })?;
    let topic = metadata
        .topics()
        .first()
        .ok_or_else(|| ShoveError::Topology(format!("no metadata for topic {queue}")))?;
    let mut partitions: Vec<i32> = topic.partitions().iter().map(|p| p.id()).collect();
    partitions.sort_unstable();
    if partitions.is_empty() {
        return Err(ShoveError::Topology(format!(
            "cannot reset offsets: topic '{queue}' has no partitions (does it exist?)"
        )));
    }

    // Watermarks answer Earliest/Latest outright and bound the Timestamp case.
    let mut watermarks = Vec::with_capacity(partitions.len());
    for &pid in &partitions {
        let (low, high) = consumer
            .fetch_watermarks(queue, pid, RPC_TIMEOUT)
            .map_err(|e| {
                ShoveError::Connection(format!(
                    "failed to fetch watermarks for {queue}[{pid}]: {e}"
                ))
            })?;
        watermarks.push((pid, low, high));
    }

    let mut targets = TopicPartitionList::new();
    match to {
        KafkaOffsetReset::Timestamp(ms) => {
            let mut query = TopicPartitionList::new();
            for &(pid, _, _) in &watermarks {
                query
                    .add_partition_offset(queue, pid, Offset::Offset(ms))
                    .map_err(|e| {
                        ShoveError::Topology(format!("failed to build timestamp query: {e}"))
                    })?;
            }
            let looked_up = consumer
                .offsets_for_times(query, RPC_TIMEOUT)
                .map_err(|e| {
                    ShoveError::Connection(format!(
                        "failed to look up offsets for timestamp {ms} on {queue}: {e}"
                    ))
                })?;
            let by_partition = looked_up.to_topic_map();
            for &(pid, low, high) in &watermarks {
                let found = by_partition
                    .get(&(queue.to_string(), pid))
                    .copied()
                    .unwrap_or(Offset::End);
                let target = target_from_timestamp_lookup(found, low, high);
                targets
                    .add_partition_offset(queue, pid, Offset::Offset(target))
                    .map_err(|e| {
                        ShoveError::Topology(format!("failed to build target offsets: {e}"))
                    })?;
            }
        }
        _ => {
            for &(pid, low, high) in &watermarks {
                let target = target_from_watermarks(to, low, high)
                    .expect("Timestamp is handled in the arm above");
                targets
                    .add_partition_offset(queue, pid, Offset::Offset(target))
                    .map_err(|e| {
                        ShoveError::Topology(format!("failed to build target offsets: {e}"))
                    })?;
            }
        }
    }

    // Read the pre-reset position before overwriting it — the report is the
    // only record of where the group was.
    let mut probe = TopicPartitionList::new();
    for &(pid, _, _) in &watermarks {
        probe.add_partition(queue, pid);
    }
    let committed = consumer
        .committed_offsets(probe, RPC_TIMEOUT)
        .map_err(|e| {
            ShoveError::Connection(format!(
                "failed to read committed offsets for group '{group_id}': {e}"
            ))
        })?;
    let previous = committed.to_topic_map();

    // Bounded independently of `RPC_TIMEOUT`: librdkafka defers a commit issued
    // before the group coordinator is reachable and gives up after
    // `session.timeout.ms`, so this cannot block forever.
    consumer.commit(&targets, CommitMode::Sync).map_err(|e| {
        ShoveError::Connection(format!(
            "failed to commit {} offsets for group '{group_id}' on '{queue}': {e}. \
             Kafka only accepts an offset reset while the group is inactive — \
             stop every consumer in the group first.",
            to.label(),
        ))
    })?;

    let target_map = targets.to_topic_map();
    let mut entries = Vec::with_capacity(watermarks.len());
    for &(pid, _, _) in &watermarks {
        let key = (queue.to_string(), pid);
        let new = match target_map.get(&key).copied() {
            Some(Offset::Offset(n)) => n,
            // Unreachable: every entry was inserted as `Offset::Offset`.
            _ => continue,
        };
        entries.push(KafkaPartitionOffsetReset {
            partition: pid,
            previous: previous.get(&key).copied().and_then(previous_committed),
            new,
        });
    }

    tracing::info!(
        queue = %queue,
        group = %group_id,
        to = to.label(),
        partitions = entries.len(),
        "consumer group offsets re-anchored"
    );

    Ok(KafkaOffsetResetReport {
        queue: queue.to_string(),
        group_id: group_id.to_string(),
        partitions: entries,
    })
}

/// Refuse the reset if the group still has live members.
///
/// The broker enforces this too (an offset commit with no member ID is only
/// honoured for an `Empty` group), but the resulting `UNKNOWN_MEMBER_ID` is
/// opaque. Checking first turns it into an error that names the problem. The
/// check is advisory — a member could join between here and the commit — so
/// the broker stays the real guard and the commit error above repeats the
/// requirement.
fn ensure_group_inactive<C, Ctx>(consumer: &C, queue: &str, group_id: &str) -> Result<()>
where
    Ctx: ConsumerContext,
    C: RdkafkaConsumer<Ctx> + Sized,
{
    let groups = consumer
        .fetch_group_list(Some(group_id), RPC_TIMEOUT)
        .map_err(|e| {
            ShoveError::Connection(format!("failed to fetch group list for '{group_id}': {e}"))
        })?;
    let members = groups
        .groups()
        .iter()
        .filter(|g| g.name() == group_id)
        .map(|g| g.members().len())
        .max()
        .unwrap_or(0);
    if members > 0 {
        return Err(ShoveError::Validation(format!(
            "cannot reset offsets for group '{group_id}' on topic '{queue}': \
             the group has {members} active member(s). Kafka only accepts an \
             offset reset while the group is inactive — stop every consumer in \
             the group, then reset before starting them again."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod context_generic_smoke {
    //! Compile-only assertion that `run_reset` accepts a `BaseConsumer`
    //! carrying a **non-default** `ConsumerContext`.
    //!
    //! That is exactly the shape the `kafka-msk-iam` path uses
    //! (`BaseConsumer<MskIamContext>`), and rdkafka's `Consumer` trait is
    //! parameterised by the context — so a signature pinned to the default
    //! context compiles fine under plain `--features kafka` and only breaks
    //! once `kafka-msk-iam` is on. Instantiating the generics against a
    //! stand-in context keeps that path type-checked in every feature set that
    //! builds Kafka at all. Never called.

    use super::*;
    use rdkafka::ClientContext;

    struct StandInContext;
    impl ClientContext for StandInContext {}
    impl ConsumerContext for StandInContext {}

    #[allow(dead_code)]
    fn run_reset_accepts_a_custom_context(consumer: &BaseConsumer<StandInContext>) {
        let _ = run_reset(consumer, "queue", "group", KafkaOffsetReset::Latest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::topology::{SequenceFailure, TopologyBuilder};

    /// The group ID a reset targets must track every input `register` uses to
    /// pick a group. Each arm below is a way the two can silently diverge —
    /// and a divergence is invisible at runtime: the reset rewrites a group
    /// nothing reads and reports success for it.
    mod reset_targets_the_group_register_joins {
        use super::*;

        fn group_for(topology: &QueueTopology, config: &KafkaConsumerGroupConfig) -> String {
            resolved_reset_group_id(config, topology)
        }

        fn any_config() -> KafkaConsumerGroupConfig {
            KafkaConsumerGroupConfig::new(1..=1)
        }

        #[test]
        fn plain_topic_uses_the_default_derivation() {
            let t = TopologyBuilder::new("orders").build();
            assert_eq!(group_for(&t, &any_config()), "orders-consumer");
        }

        #[test]
        fn sequenced_topic_uses_the_fifo_group() {
            // `allow_message_loss` only waives the DLQ/hold-queue requirement a
            // sequenced topology otherwise carries; neither feeds group-ID
            // resolution, which is all this test is about.
            let t = TopologyBuilder::new("orders")
                .sequenced(SequenceFailure::FailAll)
                .allow_message_loss()
                .build();
            assert_eq!(group_for(&t, &any_config()), "orders-fifo");
        }

        #[test]
        fn fan_out_topology_uses_the_scoped_group() {
            // Without this the reset would move `orders-consumer` while the
            // fan-out reader sits on `orders-price-latest-consumer`.
            let t = TopologyBuilder::new("orders")
                .for_consumer_group("price-latest")
                .build();
            assert_eq!(group_for(&t, &any_config()), "orders-price-latest-consumer");
        }

        #[test]
        fn fan_out_and_sequencing_compose() {
            let t = TopologyBuilder::new("orders")
                .for_consumer_group("price-latest")
                .sequenced(SequenceFailure::FailAll)
                .allow_message_loss()
                .build();
            assert_eq!(group_for(&t, &any_config()), "orders-price-latest-fifo");
        }

        #[test]
        fn an_explicit_override_outranks_the_fan_out_group() {
            let t = TopologyBuilder::new("orders")
                .for_consumer_group("price-latest")
                .build();
            let config = any_config().with_group_id("legacy-prices");
            assert_eq!(group_for(&t, &config), "legacy-prices");
        }
    }

    #[test]
    fn earliest_targets_the_low_watermark() {
        assert_eq!(
            target_from_watermarks(KafkaOffsetReset::Earliest, 40, 100),
            Some(40)
        );
    }

    #[test]
    fn latest_targets_the_high_watermark() {
        assert_eq!(
            target_from_watermarks(KafkaOffsetReset::Latest, 40, 100),
            Some(100)
        );
    }

    #[test]
    fn timestamp_is_not_answerable_from_watermarks() {
        assert_eq!(
            target_from_watermarks(KafkaOffsetReset::Timestamp(1), 40, 100),
            None
        );
    }

    #[test]
    fn empty_partition_reanchors_at_the_same_offset_either_way() {
        // low == high: nothing retained, so Earliest and Latest agree.
        assert_eq!(
            target_from_watermarks(KafkaOffsetReset::Earliest, 7, 7),
            target_from_watermarks(KafkaOffsetReset::Latest, 7, 7)
        );
    }

    #[test]
    fn timestamp_lookup_uses_the_resolved_offset() {
        assert_eq!(
            target_from_timestamp_lookup(Offset::Offset(55), 40, 100),
            55
        );
    }

    #[test]
    fn timestamp_past_the_last_record_falls_back_to_the_tail() {
        // Kafka returns a null offset ("no record at or after that time"),
        // which librdkafka surfaces as `End`.
        assert_eq!(target_from_timestamp_lookup(Offset::End, 40, 100), 100);
    }

    #[test]
    fn unresolvable_timestamp_partition_falls_back_to_the_tail() {
        assert_eq!(target_from_timestamp_lookup(Offset::Invalid, 40, 100), 100);
        assert_eq!(
            target_from_timestamp_lookup(Offset::Offset(-1), 40, 100),
            100
        );
    }

    #[test]
    fn timestamp_before_the_first_record_resolves_to_the_head() {
        // The head is the *low watermark*, not literal 0: committing 0 on a
        // partition whose history has aged out would fall outside the retained
        // range and be reinterpreted by `auto.offset.reset`.
        assert_eq!(target_from_timestamp_lookup(Offset::Beginning, 40, 100), 40);
    }

    #[test]
    fn a_resolved_offset_is_clamped_into_the_retained_range() {
        assert_eq!(target_from_timestamp_lookup(Offset::Offset(5), 40, 100), 40);
        assert_eq!(
            target_from_timestamp_lookup(Offset::Offset(500), 40, 100),
            100
        );
    }

    #[test]
    fn only_a_concrete_offset_counts_as_a_previous_commit() {
        assert_eq!(previous_committed(Offset::Offset(12)), Some(12));
        assert_eq!(previous_committed(Offset::Invalid), None);
        // librdkafka reports "never committed" as -1001 via `Offset::Offset`.
        assert_eq!(previous_committed(Offset::Offset(-1001)), None);
        assert_eq!(previous_committed(Offset::End), None);
    }

    fn report(entries: Vec<(i32, Option<i64>, i64)>) -> KafkaOffsetResetReport {
        KafkaOffsetResetReport {
            queue: "orders".into(),
            group_id: "orders-consumer".into(),
            partitions: entries
                .into_iter()
                .map(|(partition, previous, new)| KafkaPartitionOffsetReset {
                    partition,
                    previous,
                    new,
                })
                .collect(),
        }
    }

    #[test]
    fn delta_is_signed_and_absent_without_a_prior_commit() {
        let r = report(vec![(0, Some(10), 100), (1, Some(90), 40), (2, None, 5)]);
        assert_eq!(r.partitions()[0].delta(), Some(90));
        assert_eq!(r.partitions()[1].delta(), Some(-50));
        assert_eq!(r.partitions()[2].delta(), None);
    }

    #[test]
    fn a_group_already_at_the_target_reports_a_noop() {
        assert!(report(vec![(0, Some(100), 100), (1, Some(7), 7)]).is_noop());
    }

    #[test]
    fn a_group_that_never_committed_is_not_a_noop() {
        // `None` previous means the group had nothing there; the reset really
        // did write an offset, so operators should see it as a change.
        assert!(!report(vec![(0, None, 0)]).is_noop());
    }

    #[test]
    fn any_moved_partition_defeats_the_noop() {
        assert!(!report(vec![(0, Some(100), 100), (1, Some(7), 9)]).is_noop());
    }
}
