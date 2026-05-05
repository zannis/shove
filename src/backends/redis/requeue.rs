//! Hold queue requeuer for delayed message redelivery.
//!
//! Hold queues are modelled as Redis Sorted Sets where:
//!   key   = `{hold_queue_name}:pending`  (see `RedisTopologyDeclarer::hold_set_name`)
//!   score = Unix timestamp in milliseconds at which the entry should be redelivered
//!   value = JSON-serialized `HoldEntry`
//!
//! This task polls each hold set every `POLL_INTERVAL`, moves all entries
//! whose score ≤ now_ms back to the appropriate stream via XADD, and removes
//! them from the set only after successful XADD (at-least-once delivery).
//!
//! ## Concurrent requeuer instances
//!
//! This module provides **at-least-once** redelivery semantics. If two requeuer
//! instances run concurrently (e.g., during a rolling restart), both may XADD
//! the same entry before either ZREM completes, resulting in duplicate delivery.
//! This is expected behaviour: consumers must be idempotent, which is already
//! required by the broader shove at-least-once contract.

use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use super::client::{RedisClient, RedisConnection};
use super::topology::RedisTopologyDeclarer;
use super::constants::{REQUEUE_POLL_MS, REQUEUE_BATCH_SIZE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Interval between requeue polling ticks.
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
pub(crate) struct HoldEntry {
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
/// # Errors
///
/// Returns an error if JSON encoding fails or the Redis ZADD fails.
pub(crate) async fn enqueue_hold(
    conn: &mut RedisConnection,
    hold_queue_name: &str,
    entry: HoldEntry,
    delay: Duration,
) -> Result<()> {
    let set_key = RedisTopologyDeclarer::hold_set_name(hold_queue_name);
    let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    let redeliver_at_ms = now_ms().saturating_add(delay_ms);
    let value = serde_json::to_string(&entry)?;

    let mut cmd = redis::cmd("ZADD");
    cmd.arg(set_key)
        .arg(redeliver_at_ms as f64)
        .arg(&value);

    let _: i64 = conn.query(&mut cmd).await?;
    Ok(())
}

/// Spawn a background requeuer task that periodically drains due entries
/// from all hold sets back to their target streams.
///
/// The task acquires one connection and reuses it across poll ticks. On
/// connection error it attempts to reconnect before the next tick. The task
/// runs until `shutdown` is cancelled.
///
/// **At-least-once semantics:** see module-level documentation for the
/// concurrent-instance duplicate-delivery note.
pub(crate) fn spawn_requeuer(
    client: RedisClient,
    hold_queue_names: Vec<String>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    // Pre-compute the sorted-set key for each hold queue once.
    let hold_set_keys: Vec<String> = hold_queue_names
        .iter()
        .map(|n| RedisTopologyDeclarer::hold_set_name(n))
        .collect();

    tokio::spawn(async move {
        let mut conn = match client.multiplexed_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("requeuer: failed to acquire initial connection: {}", e);
                return;
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(POLL_INTERVAL) => {
                    let mut needs_reconnect = false;
                    for (hold_queue_name, set_key) in hold_queue_names.iter().zip(hold_set_keys.iter()) {
                        if let Err(e) = poll_hold_set(&mut conn, hold_queue_name, set_key).await {
                            tracing::warn!("requeuer: poll failed for {}: {}", hold_queue_name, e);
                            needs_reconnect = true;
                        }
                    }
                    if needs_reconnect {
                        match client.multiplexed_conn().await {
                            Ok(new_conn) => conn = new_conn,
                            Err(e) => tracing::warn!("requeuer: reconnect failed: {}", e),
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

/// Current Unix timestamp in milliseconds, saturating at `u64::MAX`.
fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Poll a single hold set for due entries and requeue them.
///
/// Fetches up to `REQUEUE_BATCH_SIZE` entries with score ≤ now_ms,
/// XADDs each to its origin stream, and removes it from the hold set only on
/// success. Corrupt entries (JSON parse failures) are removed from the set to
/// avoid perpetual re-fetch and logged as warnings.
async fn poll_hold_set(conn: &mut RedisConnection, hold_queue_name: &str, set_key: &str) -> Result<()> {
    let now = now_ms();

    let entries: Vec<String> = conn
        .query(
            redis::cmd("ZRANGEBYSCORE")
                .arg(set_key)
                .arg(0f64)
                .arg(now as f64)
                .arg("LIMIT")
                .arg(0i64)
                .arg(REQUEUE_BATCH_SIZE),
        )
        .await
        .unwrap_or_default();

    for raw_json in entries {
        let entry: HoldEntry = match serde_json::from_str(&raw_json) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    "requeuer: corrupt hold entry in {} (removing): {}",
                    hold_queue_name,
                    e
                );
                let _: i64 = conn
                    .query(redis::cmd("ZREM").arg(set_key).arg(&raw_json))
                    .await
                    .unwrap_or(0);
                continue;
            }
        };

        let mut cmd = redis::cmd("XADD");
        cmd.arg(&entry.stream).arg("*");
        for (k, v) in &entry.fields {
            cmd.arg(k).arg(v);
        }

        match conn.query::<String>(&mut cmd).await {
            Ok(_) => {
                if let Err(e) = conn
                    .query::<i64>(redis::cmd("ZREM").arg(set_key).arg(&raw_json))
                    .await
                {
                    tracing::warn!(
                        "requeuer: ZREM failed for entry in {}: {}",
                        hold_queue_name,
                        e
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "requeuer: XADD failed for stream {}: {}",
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
        use super::super::constants::{PAYLOAD_FIELD, X_RETRY_COUNT};
        let entry = HoldEntry {
            stream: "orders".into(),
            fields: vec![
                (PAYLOAD_FIELD.into(), "{}".into()),
                (X_RETRY_COUNT.into(), "1".into()),
            ],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.fields[0], (PAYLOAD_FIELD.into(), "{}".into()));
        assert_eq!(decoded.fields[1], (X_RETRY_COUNT.into(), "1".into()));
    }

    #[test]
    fn now_ms_is_nonzero() {
        assert!(now_ms() > 0);
    }

    #[test]
    fn hold_entry_with_empty_fields_roundtrips() {
        let entry = HoldEntry {
            stream: "my-stream".into(),
            fields: vec![],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.stream, "my-stream");
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn hold_entry_fields_order_preserved() {
        // JSON round-trip must preserve insertion order of fields.
        let entry = HoldEntry {
            stream: "order-stream".into(),
            fields: vec![
                ("alpha".into(), "1".into()),
                ("beta".into(), "2".into()),
                ("gamma".into(), "3".into()),
            ],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.fields[0], ("alpha".into(), "1".into()));
        assert_eq!(decoded.fields[1], ("beta".into(), "2".into()));
        assert_eq!(decoded.fields[2], ("gamma".into(), "3".into()));
    }

    #[test]
    fn now_ms_is_positive_and_recent() {
        let before = now_ms();
        let after = now_ms();
        // Monotonically non-decreasing between two consecutive calls.
        assert!(after >= before);
        // Should be in the range of a plausible Unix timestamp (year 2020+).
        // 2020-01-01 00:00:00 UTC in ms = 1_577_836_800_000
        assert!(before > 1_577_836_800_000u64, "timestamp too small: {before}");
    }
}
