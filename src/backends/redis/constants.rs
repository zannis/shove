//! Redis stream entry field name constants for shove metadata.

#![allow(dead_code)]

pub(super) const PAYLOAD_FIELD: &str = "payload";
pub(super) const X_RETRY_COUNT: &str = "x-retry-count";
/// Reserved for future distributed-tracing support; not yet written or read.
pub(super) const X_TRACE_ID: &str = "x-trace-id";
pub(super) const X_MESSAGE_ID: &str = "x-message-id";
pub(super) const X_SEQUENCE_KEY: &str = "x-sequence-key";
pub(super) const X_DEATH_REASON: &str = "x-death-reason";
pub(super) const X_DEATH_COUNT: &str = "x-death-count";
pub(super) const X_ORIGINAL_QUEUE: &str = "x-original-queue";

/// Default number of FIFO routing shards when routing_shards() is not overridden.
pub(super) const DEFAULT_ROUTING_SHARDS: u16 = 8;
/// Default Redis consumer group name used when none is provided in ConsumerOptions.
pub(super) const DEFAULT_GROUP: &str = "shove";
/// BLOCK timeout for XREADGROUP calls (milliseconds). Short enough to check
/// the shutdown token regularly; long enough to avoid busy-polling.
pub(super) const BLOCK_MS: u64 = 2_000;
/// Number of pending entries reclaimed per XAUTOCLAIM call.
pub(super) const AUTOCLAIM_COUNT: usize = 100;
/// Poll interval for the hold queue requeuer (milliseconds).
pub(super) const REQUEUE_POLL_MS: u64 = 500;
/// Maximum number of entries to fetch per ZRANGEBYSCORE call in the requeuer.
pub(super) const REQUEUE_BATCH_SIZE: i64 = 200;
