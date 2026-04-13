pub(super) const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";
pub(super) const DEATH_REASON_HEADER: &str = "Shove-Death-Reason";
pub(super) const ORIGINAL_QUEUE_HEADER: &str = "Shove-Original-Queue";
pub(super) const DEATH_COUNT_HEADER: &str = "Shove-Death-Count";
pub(super) const MESSAGE_ID_HEADER: &str = "Shove-Message-Id";

/// Max consecutive connection-level failures before giving up.
/// Distinct from `ConsumerOptions::max_retries` which is per-message.
pub(super) const CONNECTION_RETRIES: u32 = 10;

/// Derives the consumer group ID from a queue name.
/// Used by both the consumer and autoscaler to ensure consistency.
pub(super) fn consumer_group_id(queue: &str) -> String {
    format!("{queue}-consumer")
}

/// Derives the DLQ consumer group ID from a DLQ topic name.
pub(super) fn dlq_consumer_group_id(dlq: &str) -> String {
    format!("{dlq}-consumer")
}
