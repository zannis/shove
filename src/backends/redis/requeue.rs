//! Hold queue requeuer for delayed message redelivery.
//!
//! This module implements a background task that polls Redis sorted sets
//! (hold queues) for entries with due redelivery timestamps, deserializes them,
//! re-adds them to their origin streams, and removes them from the hold queue.

use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};
use super::client::{RedisClient, RedisConnection};
use super::topology::RedisTopologyDeclarer;
use super::constants::{REQUEUE_POLL_MS, REQUEUE_BATCH_SIZE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Interval between requeue polling ticks (milliseconds).
const POLL_INTERVAL: Duration = Duration::from_millis(REQUEUE_POLL_MS);

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// A message entry in a hold sorted set, awaiting delayed redelivery.
///
/// Serialized as JSON and stored in a Redis Sorted Set with a score
/// equal to the Unix timestamp (in milliseconds) when the message should
/// be redelivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HoldEntry {
    /// Target stream to XADD back into (the main stream or shard stream).
    pub stream: String,
    /// All fields (payload + metadata) to restore on redeliver, as key-value pairs.
    pub fields: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Push a message to a hold sorted set for delayed redelivery.
///
/// Stores the entry as JSON in the sorted set keyed by `hold_queue_name`,
/// with a score equal to the current time plus `delay`.
///
/// # Arguments
///
/// * `conn` - Active Redis connection
/// * `hold_queue_name` - Name of the hold queue (e.g., "orders-hold-5s")
/// * `entry` - The message entry containing stream and fields
/// * `delay` - How long to wait before redelivery
///
/// # Errors
///
/// Returns `ShoveError::Serialization` if JSON encoding fails,
/// or `ShoveError::Connection` if Redis operations fail.
pub async fn enqueue_hold(
    conn: &mut RedisConnection,
    hold_queue_name: &str,
    entry: HoldEntry,
    delay: Duration,
) -> Result<()> {
    let set_key = RedisTopologyDeclarer::hold_set_name(hold_queue_name);
    let redeliver_at_ms = now_ms() + delay.as_millis() as u64;
    let value = serde_json::to_string(&entry)?;

    // ZADD key redeliver_at_ms value
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(&set_key)
        .arg(redeliver_at_ms as f64)
        .arg(&value);

    let _: i64 = conn.query(&mut cmd).await?;
    Ok(())
}

/// Spawn a background requeuer task that periodically drains due entries
/// from all hold sets back to their target streams.
///
/// The task polls all hold queues on a 500ms interval, looking for entries
/// with scores (timestamps) <= current time. For each due entry, it:
/// 1. Deserializes the JSON
/// 2. XADDs the message back to the origin stream
/// 3. Removes the entry from the hold set (only on successful XADD)
///
/// The task runs until `shutdown` is cancelled. Deserialization and XADD
/// failures are logged as warnings but do not stop the requeuer.
///
/// # Arguments
///
/// * `client` - Redis client (cloned for background task)
/// * `hold_queue_names` - List of hold queue names to monitor
/// * `shutdown` - Token to signal task cancellation
///
/// # Returns
///
/// A `tokio::task::JoinHandle` that completes when the task exits.
/// Abort the handle to stop the requeuer.
pub fn spawn_requeuer(
    client: RedisClient,
    hold_queue_names: Vec<String>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {
                    // Poll all hold sets on this tick
                    for hold_queue_name in &hold_queue_names {
                        if let Err(e) = poll_hold_set(&client, hold_queue_name).await {
                            tracing::warn!("failed to poll hold set {}: {}", hold_queue_name, e);
                        }
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Current Unix timestamp in milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Poll a single hold set for due entries and requeue them.
///
/// Fetches up to 200 entries with score <= now_ms, deserializes each,
/// XADDs to the origin stream, and removes from the hold set.
/// Logs warnings for deserialization and XADD failures but continues.
async fn poll_hold_set(client: &RedisClient, hold_queue_name: &str) -> Result<()> {
    let mut conn = client.multiplexed_conn().await?;
    let set_key = RedisTopologyDeclarer::hold_set_name(hold_queue_name);
    let now = now_ms();

    // ZRANGEBYSCORE key 0 {now} LIMIT 0 200
    let entries: Vec<String> = conn
        .query(&mut redis::cmd("ZRANGEBYSCORE")
            .arg(&set_key)
            .arg(0f64)
            .arg(now as f64)
            .arg("LIMIT")
            .arg(0i64)
            .arg(REQUEUE_BATCH_SIZE))
        .await
        .unwrap_or_default();

    for raw_json in entries {
        // Deserialize the HoldEntry
        let entry: HoldEntry = match serde_json::from_str(&raw_json) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    "failed to deserialize hold entry in {}: {}",
                    hold_queue_name,
                    e
                );
                continue;
            }
        };

        // XADD {entry.stream} * field1 val1 field2 val2 ...
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&entry.stream).arg("*");
        for (k, v) in &entry.fields {
            cmd.arg(k).arg(v);
        }

        match conn.query::<String>(&mut cmd).await {
            Ok(_) => {
                // Only remove from hold set on successful XADD
                let mut del_cmd = redis::cmd("ZREM");
                del_cmd.arg(&set_key).arg(&raw_json);
                if let Err(e) = conn.query::<i64>(&mut del_cmd).await {
                    tracing::warn!(
                        "failed to remove hold entry from {}: {}",
                        hold_queue_name,
                        e
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "failed to XADD to {} during requeue: {}",
                    entry.stream,
                    e
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_entry_roundtrips() {
        let entry = HoldEntry {
            stream: "orders".into(),
            fields: vec![
                ("payload".into(), "{}".into()),
                ("x-retry-count".into(), "1".into()),
            ],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.fields.len(), 2);
    }

    #[test]
    fn now_ms_is_nonzero() {
        assert!(now_ms() > 0);
    }
}
