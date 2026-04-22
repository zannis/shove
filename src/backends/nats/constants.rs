pub(super) const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";
pub(super) const DEATH_REASON_HEADER: &str = "Shove-Death-Reason";
pub(super) const ORIGINAL_QUEUE_HEADER: &str = "Shove-Original-Queue";
pub(super) const DEATH_COUNT_HEADER: &str = "Shove-Death-Count";

/// Max consecutive connection-level failures before giving up.
/// Distinct from `ConsumerOptions::max_retries` which is per-message.
pub(super) const CONNECTION_RETRIES: u32 = 10;

/// Derives the durable consumer name from a queue name.
///
/// JetStream WorkQueue retention permits only one non-filtered consumer per
/// stream, so every task in a consumer group binds to the *same* durable
/// consumer and the server load-balances messages across them. The group
/// registry is responsible for configuring the consumer once (with a
/// sufficient `max_ack_pending`) so all tasks inherit the right in-flight
/// budget.
pub(super) fn consumer_name(queue: &str) -> String {
    format!("{queue}-consumer")
}

/// Derives the DLQ consumer name from a DLQ stream name.
pub(super) fn dlq_consumer_name(dlq: &str) -> String {
    format!("{dlq}-consumer")
}
