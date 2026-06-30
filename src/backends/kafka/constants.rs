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

/// Derives the DLQ consumer group ID from a DLQ topic name.
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
    fn override_rebasing_appends_role_suffix() {
        // A `with_group_id` override is the base; FIFO/DLQ append their role
        // suffix so a custom group cannot re-collide on the auxiliary groups.
        assert_eq!(fifo_group_id_from_base("price-sink"), "price-sink-fifo");
        assert_eq!(dlq_group_id_from_base("price-sink"), "price-sink-dlq");
    }
}
