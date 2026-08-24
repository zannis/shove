//! Kafka backend constants.
//!
//! Centralizes magic numbers (timeouts, attempt counts, rdkafka tuning) so
//! a future "expose as `KafkaConfig` knobs" change has a single source of
//! truth to wire up. Typed `Duration` / `u32` constants only — strings are
//! formatted at the rdkafka call site.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Message header keys
// ---------------------------------------------------------------------------

pub(super) const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";
pub(super) const DEATH_REASON_HEADER: &str = "Shove-Death-Reason";
pub(super) const ORIGINAL_QUEUE_HEADER: &str = "Shove-Original-Queue";
pub(super) const DEATH_COUNT_HEADER: &str = "Shove-Death-Count";
pub(super) const MESSAGE_ID_HEADER: &str = "Shove-Message-Id";

// ---------------------------------------------------------------------------
// Group ID derivations
// ---------------------------------------------------------------------------

/// Derives the consumer group ID from a queue name.
/// Used by both the consumer and autoscaler to ensure consistency.
pub(super) fn consumer_group_id(queue: &str) -> String {
    format!("{queue}-consumer")
}

/// Derives the FIFO consumer group ID from a queue name.
/// FIFO consumers use a distinct group so the autoscaler can query committed
/// offsets under the correct group — not `{queue}-consumer`.
pub(super) fn consumer_group_id_fifo(queue: &str) -> String {
    format!("{queue}-fifo")
}

/// Derives the consumer group ID for a topology declared with
/// [`TopologyBuilder::for_consumer_group`](crate::TopologyBuilder::for_consumer_group).
///
/// Without a fan-out group this is the historical `{queue}-consumer`. With one
/// it is `{queue}-{group}-consumer`, so a second service reading the same topic
/// is assigned its own partition set instead of splitting the first service's.
/// An explicit `with_group_id` override outranks both — see
/// `KafkaConsumerGroupConfig::resolved_group_id`.
pub(super) fn consumer_group_id_scoped(queue: &str, fan_out_group: Option<&str>) -> String {
    match fan_out_group {
        Some(group) => format!("{queue}-{group}-consumer"),
        None => consumer_group_id(queue),
    }
}

/// FIFO counterpart of [`consumer_group_id_scoped`]: `{queue}-{group}-fifo`,
/// mirroring how the unscoped FIFO group drops the `-consumer` suffix.
pub(super) fn consumer_group_id_fifo_scoped(queue: &str, fan_out_group: Option<&str>) -> String {
    match fan_out_group {
        Some(group) => format!("{queue}-{group}-fifo"),
        None => consumer_group_id_fifo(queue),
    }
}

/// The `group.id` a broadcast subscription's consumer handle is configured
/// with — inert, and deliberately not a per-process value.
///
/// The design for this feature says "no `group.id`", and librdkafka will not
/// allow that literally: `rd_kafka_assign()` returns `_UNKNOWN_GROUP`
/// ("Requires a consumer with group.id configured") on a handle whose group id
/// is empty, because the assignment machinery hangs off the consumer-group
/// object. What the design is actually asking for — no broker-side group, no
/// `__consumer_offsets` churn, no rebalance on boot — comes from never calling
/// `subscribe()` and never committing, not from the string being absent. A
/// manually assigned consumer sends no JoinGroup and no OffsetCommit, so the
/// group is never created and never appears in `kafka-consumer-groups --list`.
///
/// It is a fixed function of the topic rather than a UUID for exactly the
/// reason the design rules a per-process group out: a per-restart identifier is
/// a per-restart name in every tool that enumerates group ids, which is the
/// residue this feature exists to avoid. Every instance sharing one inert
/// string leaves nothing to accumulate.
pub(super) fn broadcast_group_id(queue: &str) -> String {
    format!("{queue}-broadcast")
}

/// Derives the DLQ consumer group ID from a DLQ topic name.
///
/// Takes the *resolved* DLQ topic name, so a fan-out group needs no special
/// case here: its DLQ is already `{queue}-{group}-dlq`, giving the drain group
/// `{queue}-{group}-dlq-consumer`.
pub(super) fn dlq_consumer_group_id(dlq: &str) -> String {
    format!("{dlq}-consumer")
}

/// Rebases the FIFO group ID onto an explicit `group.id` override `base`.
/// Single source for the `{base}-fifo` rule so the autoscaler-stored group
/// (via `KafkaConsumerGroupConfig::resolved_fifo_group_id`) and the broker-side
/// FIFO consumer (via `spawn_fifo_shards`) cannot drift apart.
pub(super) fn fifo_group_id_from_base(base: &str) -> String {
    format!("{base}-fifo")
}

/// Rebases the DLQ group ID onto an explicit `group.id` override `base`, so a
/// custom group does not re-collide on the default `{dlq}-consumer` group.
pub(super) fn dlq_group_id_from_base(base: &str) -> String {
    format!("{base}-dlq")
}

// ---------------------------------------------------------------------------
// Publisher tuning
// ---------------------------------------------------------------------------

/// How many times `publish_with_retry` attempts a send before surfacing a
/// `Connection` error. Same value is used for the DLQ publish and for the
/// delayed Retry/Defer republish — all three are "best-effort with bounded
/// retries" paths.
pub(super) const MAX_PUBLISH_ATTEMPTS: u32 = 3;

/// Per-send timeout passed to `FutureProducer::send`. Bounds how long a
/// single attempt blocks waiting for broker ack before the `publish_with_retry`
/// loop counts it as a failure and either retries or surfaces an error.
pub(super) const PRODUCE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Topology defaults
// ---------------------------------------------------------------------------

/// Default partition count for standard (non-sequenced) topics.
pub(super) const DEFAULT_PARTITIONS: i32 = 8;

/// Default replication factor for auto-created topics. `1` keeps the
/// no-config test/dev path working with a single-broker cluster; production
/// deployments override via `KafkaConsumerGroupRegistry::with_default_replication_factor`
/// or `KafkaTopologyDeclarer::with_replication_factor`.
pub(super) const DEFAULT_REPLICATION: i32 = 1;

// ---------------------------------------------------------------------------
// Client lifecycle
// ---------------------------------------------------------------------------

/// Grace period the producer flush is given on shutdown before it is
/// dropped. Picked to be long enough for in-flight produces to drain on a
/// healthy cluster but short enough that a hung broker cannot stall
/// shutdown indefinitely.
pub(super) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// rdkafka producer / consumer tuning
//
// These are the librdkafka client-config values shove sets on every
// `ClientConfig::set("foo.ms", ...)` call. Kept here as `u32` so callers
// format the string once at the call site instead of leaving stringly-typed
// literals scattered through the backend.
// ---------------------------------------------------------------------------

/// Producer per-send timeout passed to librdkafka via `message.timeout.ms`.
/// Used by `KafkaClient::connect`'s default and MSK-IAM producer builders.
pub(super) const MESSAGE_TIMEOUT_MS: u32 = 5_000;

/// Consumer `session.timeout.ms`. The broker considers a group member dead
/// after this many ms without a heartbeat, triggering a rebalance. Must be
/// less than `MAX_POLL_INTERVAL_MS` (librdkafka enforces this).
pub(super) const SESSION_TIMEOUT_MS: u32 = 10_000;

/// Consumer `max.poll.interval.ms`. Upper bound on time between poll calls
/// before the broker boots the consumer from the group. Currently 5 minutes
/// — handlers that legitimately take longer should bump this via a future
/// `KafkaConsumerGroupConfig::with_max_poll_interval` knob (see review
/// follow-up: producer/consumer tuning knobs).
pub(super) const MAX_POLL_INTERVAL_MS: u32 = 300_000;

/// Consumer `fetch.min.bytes`. `1` returns from a fetch as soon as any data
/// is available, minimising latency for small-payload workloads.
pub(super) const FETCH_MIN_BYTES: u32 = 1;

/// Consumer `fetch.wait.max.ms`. Caps how long the broker holds a fetch
/// waiting for `fetch.min.bytes` to accumulate. Set to `50` so that
/// low-volume topics don't pay librdkafka's default 500 ms dwell.
pub(super) const FETCH_WAIT_MAX_MS: u32 = 50;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_id_derivations_are_canonical() {
        assert_eq!(consumer_group_id("orders"), "orders-consumer");
        assert_eq!(consumer_group_id_fifo("orders"), "orders-fifo");
        assert_eq!(dlq_consumer_group_id("orders-dlq"), "orders-dlq-consumer");
    }

    #[test]
    fn scoped_group_ids_match_the_unscoped_ones_without_a_fan_out_group() {
        // `None` must be byte-identical to the historical derivation: an
        // existing deployment upgrading to this version keeps its group and
        // its committed offsets.
        assert_eq!(
            consumer_group_id_scoped("orders", None),
            consumer_group_id("orders")
        );
        assert_eq!(
            consumer_group_id_fifo_scoped("orders", None),
            consumer_group_id_fifo("orders")
        );
    }

    #[test]
    fn scoped_group_ids_namespace_by_fan_out_group() {
        assert_eq!(
            consumer_group_id_scoped("orders", Some("price-latest")),
            "orders-price-latest-consumer"
        );
        assert_eq!(
            consumer_group_id_fifo_scoped("orders", Some("price-latest")),
            "orders-price-latest-fifo"
        );
        // Two fan-out groups on one topic never collide — the property the
        // whole feature rests on.
        assert_ne!(
            consumer_group_id_scoped("orders", Some("a")),
            consumer_group_id_scoped("orders", Some("b"))
        );
    }

    #[test]
    fn dlq_drain_group_follows_the_namespaced_dlq_topic() {
        // The DLQ group needs no fan-out case of its own: it is derived from
        // the resolved DLQ topic, which `for_consumer_group` already
        // namespaced.
        assert_eq!(
            dlq_consumer_group_id("orders-price-latest-dlq"),
            "orders-price-latest-dlq-consumer"
        );
    }

    #[test]
    fn override_rebasing_appends_role_suffix() {
        // A `with_group_id` override is the base; FIFO/DLQ append their role
        // suffix so a custom group cannot re-collide on the auxiliary groups.
        assert_eq!(fifo_group_id_from_base("price-sink"), "price-sink-fifo");
        assert_eq!(dlq_group_id_from_base("price-sink"), "price-sink-dlq");
    }
}
