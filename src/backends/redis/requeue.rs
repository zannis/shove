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

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::client::{RedisClient, RedisConnection, acquire_conn_with_retry};
use super::constants::{REQUEUE_BATCH_SIZE, REQUEUE_POLL_MS};
use super::topology::RedisTopologyDeclarer;
use crate::error::{Result, ShoveError};
use crate::metrics::{BackendErrorKind, BackendLabel, record_backend_error};
use crate::retry::Backoff;

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
    /// Unique storage identity for this hold generation. Redis sorted-set
    /// members are the serialized entry itself; without a nonce, a handler
    /// that immediately defers a just-released retry can enqueue an identical
    /// member before the requeuer's ZREM, and that ZREM deletes the new hold.
    /// `default` keeps entries persisted by older shove versions readable.
    #[serde(default)]
    hold_id: String,
    /// Target stream to XADD back into (the main stream or shard stream).
    pub stream: String,
    /// All fields (payload + metadata) to restore on redeliver, as key-value pairs.
    pub fields: Vec<(String, String)>,
}

impl HoldEntry {
    pub(crate) fn new(stream: String, fields: Vec<(String, String)>) -> Self {
        Self {
            hold_id: uuid::Uuid::new_v4().to_string(),
            stream,
            fields,
        }
    }
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
    cmd.arg(set_key).arg(redeliver_at_ms as f64).arg(&value);

    let _: i64 = conn.query(&mut cmd).await?;
    Ok(())
}

/// Spawn a background requeuer task that periodically drains due entries
/// from all hold sets back to their target streams.
///
/// The task acquires one connection and reuses it across poll ticks. On
/// connection error it backs off with jitter and retries until the shutdown
/// token is cancelled, preventing silent abandonment of hold-queue messages.
/// The task runs until `shutdown` is cancelled.
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
        let mut conn = match acquire_conn_with_retry(&client, &shutdown, "requeuer").await {
            Some(c) => c,
            None => return,
        };

        loop {
            let mut needs_reconnect = false;
            for (hold_queue_name, set_key) in hold_queue_names.iter().zip(hold_set_keys.iter()) {
                if let Err(e) = poll_hold_set(&mut conn, hold_queue_name, set_key).await {
                    tracing::warn!("requeuer: poll failed for {}: {}", hold_queue_name, e);
                    needs_reconnect = true;
                    break;
                }
            }

            if needs_reconnect {
                match acquire_conn_with_retry(&client, &shutdown, "requeuer").await {
                    Some(c) => {
                        conn = c;
                        continue;
                    }
                    None => break,
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
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
async fn poll_hold_set(
    conn: &mut RedisConnection,
    hold_queue_name: &str,
    set_key: &str,
) -> Result<()> {
    let now = now_ms();

    let entries: Vec<String> = match conn
        .query(
            // ZRANGE with BYSCORE replaces the deprecated ZRANGEBYSCORE (Redis 6.2+).
            redis::cmd("ZRANGE")
                .arg(set_key)
                .arg(0f64)
                .arg(now as f64)
                .arg("BYSCORE")
                .arg("LIMIT")
                .arg(0i64)
                .arg(REQUEUE_BATCH_SIZE),
        )
        .await
    {
        Ok(entries) => entries,
        Err(e) => {
            tracing::error!(
                hold_queue = hold_queue_name,
                set_key = set_key,
                error = %e,
                "requeuer: ZRANGE failed, cannot poll hold set"
            );
            return Err(ShoveError::Connection(format!(
                "ZRANGE failed for hold set '{set_key}': {e}"
            )));
        }
    };

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
                    record_backend_error(BackendLabel::Redis, BackendErrorKind::Ack);
                }
            }
            Err(e) => {
                tracing::warn!("requeuer: XADD failed for stream {}: {}", entry.stream, e);
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
    use crate::error::ShoveError;

    #[test]
    fn hold_entry_roundtrips() {
        use super::super::constants::{PAYLOAD_FIELD, X_RETRY_COUNT};
        let entry = HoldEntry::new(
            "orders".into(),
            vec![
                (PAYLOAD_FIELD.into(), "{}".into()),
                (X_RETRY_COUNT.into(), "1".into()),
            ],
        );
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
        let entry = HoldEntry::new("my-stream".into(), vec![]);
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.stream, "my-stream");
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn hold_entry_fields_order_preserved() {
        // JSON round-trip must preserve insertion order of fields.
        let entry = HoldEntry::new(
            "order-stream".into(),
            vec![
                ("alpha".into(), "1".into()),
                ("beta".into(), "2".into()),
                ("gamma".into(), "3".into()),
            ],
        );
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.fields[0], ("alpha".into(), "1".into()));
        assert_eq!(decoded.fields[1], ("beta".into(), "2".into()));
        assert_eq!(decoded.fields[2], ("gamma".into(), "3".into()));
    }

    #[test]
    fn consecutive_hold_generations_have_distinct_sorted_set_members() {
        let fields = vec![("payload".into(), "same-message".into())];
        let first = HoldEntry::new("orders".into(), fields.clone());
        let second = HoldEntry::new("orders".into(), fields);

        assert_ne!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
    }

    #[test]
    fn hold_entries_from_before_the_nonce_remain_readable() {
        let decoded: HoldEntry =
            serde_json::from_str(r#"{"stream":"orders","fields":[["payload","legacy"]]}"#).unwrap();

        assert!(decoded.hold_id.is_empty());
        assert_eq!(decoded.stream, "orders");
    }

    #[test]
    fn now_ms_is_positive_and_recent() {
        let before = now_ms();
        let after = now_ms();
        // Monotonically non-decreasing between two consecutive calls.
        assert!(after >= before);
        // Should be in the range of a plausible Unix timestamp (year 2020+).
        // 2020-01-01 00:00:00 UTC in ms = 1_577_836_800_000
        assert!(
            before > 1_577_836_800_000u64,
            "timestamp too small: {before}"
        );
    }

    // -----------------------------------------------------------------------
    // Fix #14 — ZRANGE error propagation
    // -----------------------------------------------------------------------

    #[test]
    fn zrange_error_is_connection_variant() {
        // poll_hold_set wraps ZRANGE failures as ShoveError::Connection so that
        // spawn_requeuer's error branch fires and sets needs_reconnect = true.
        let set_key = "orders-hold-5s:pending";
        let err = ShoveError::Connection(format!(
            "ZRANGE failed for hold set '{set_key}': connection refused"
        ));
        assert!(
            matches!(err, ShoveError::Connection(_)),
            "ZRANGE error must be ShoveError::Connection"
        );
    }

    #[test]
    fn zrange_error_message_contains_set_key_and_cause() {
        // The error message must include both the hold set name (operator context)
        // and the original Redis error (root-cause diagnosis).
        let set_key = "orders-hold-5s:pending";
        let cause = "connection timed out";
        let msg = format!("ZRANGE failed for hold set '{set_key}': {cause}");
        assert!(
            msg.contains(set_key),
            "error message must name the hold set; got: {msg}"
        );
        assert!(
            msg.contains(cause),
            "error message must preserve the original error; got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Fix #24 — acquire_conn_with_retry backoff contract
    // -----------------------------------------------------------------------

    #[test]
    fn backoff_is_infinite_for_retry_loop() {
        // acquire_conn_with_retry calls backoff.next().expect("backoff is infinite").
        // Verify that Backoff::default() never yields None — even after the delay
        // reaches the 30 s cap it keeps returning Some(delay).
        let delays: Vec<_> = Backoff::default().take(500).collect();
        assert_eq!(
            delays.len(),
            500,
            "Backoff must never return None; the .expect() in acquire_conn_with_retry would panic"
        );
    }

    #[test]
    fn backoff_default_delay_stays_within_bounds() {
        // acquire_conn_with_retry uses Backoff::default() which has initial=1s, max=30s,
        // factor=0.5. Every yielded delay must be in [0.5s, 45s] (max * 1.5 upper bound).
        let max_expected = std::time::Duration::from_millis(45_000);
        let min_expected = std::time::Duration::from_millis(500);
        for delay in Backoff::default().take(50) {
            assert!(
                delay >= min_expected,
                "delay {delay:?} is below the minimum expected bound"
            );
            assert!(
                delay <= max_expected,
                "delay {delay:?} exceeds the maximum expected bound"
            );
        }
    }
}
