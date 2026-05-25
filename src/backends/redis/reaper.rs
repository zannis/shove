//! Background reaper task that runs XAUTOCLAIM for an entire consumer group.
//!
//! Before this existed, every consumer task ran `autoclaim_all` itself — both
//! at startup and on a periodic timer. With N consumers in a group, that
//! produces N redundant XAUTOCLAIM passes per interval against the **same**
//! stream. XAUTOCLAIM is O(PEL_size / COUNT) Redis commands and roughly
//! 1.5 ms per command on localhost; at 1024 consumers and a moderately
//! populated PEL this serialised ~17 s of single-thread Redis CPU during a
//! ~17 s consume window (measured via `INFO commandstats`).
//!
//! The reaper consolidates the work: one task per `(stream, group)` pair
//! calls `autoclaim_all` on a fixed interval using a single, well-known
//! consumer name (`shove-reaper-{group}`). Regular consumers no longer do
//! any XAUTOCLAIM themselves.
//!
//! **At-least-once semantics**: if shove runs in multiple processes against
//! the same Redis, each spawns its own reaper. They race on XAUTOCLAIM, which
//! is safe — only one of them wins ownership of each reclaimed entry. Some
//! duplicate scanning happens but no entry is delivered twice.
//!
//! ## Redelivery
//!
//! XAUTOCLAIM transfers PEL ownership but does **not** re-deliver the entry
//! — the new owner sees it only by issuing `XREADGROUP ... 0` (its own PEL).
//! The current implementation pairs with the existing consumer-loop
//! behaviour, which is `XREADGROUP ... >` (new deliveries only). That means
//! claimed entries sit in the reaper's PEL until either (a) the operator
//! drains them manually or (b) a follow-up change implements PEL drain in
//! the reaper. This module deliberately stops at the XAUTOCLAIM step so the
//! diff stays focused on removing the thundering herd; PEL-drain redelivery
//! is tracked as follow-up work.

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::backends::redis::client::{RedisClient, RedisConnection};
use crate::error::Result;
use crate::retry::Backoff;

use super::constants::AUTOCLAIM_COUNT;

/// Reaper consumer name used as the XAUTOCLAIM target. Stable per group so
/// reclaimed entries accumulate under one PEL slot regardless of process
/// restarts.
fn reaper_consumer_name(group: &str) -> String {
    format!("shove-reaper-{group}")
}

/// Spawn a background reaper task for the given streams + group.
///
/// `streams` is plural to support FIFO consumer groups that own one stream
/// per shard — a single reaper handles them all.
///
/// `interval` is the gap between XAUTOCLAIM passes. Defaults at the call
/// site to `max(handler_timeout, 30s)` so the reaper never fires more
/// frequently than entries can plausibly time out.
///
/// `min_idle_ms` is the XAUTOCLAIM idle threshold — entries whose PEL idle
/// time is shorter than this are not reclaimed.
/// `pub` (with `#[doc(hidden)]`) rather than `pub(crate)` so integration
/// tests in `tests/` — which are compiled as separate crates — can spawn the
/// reaper directly with arbitrary timing. Production code should not call
/// this; the consumer-group registry owns reaper lifecycle.
#[doc(hidden)]
pub fn spawn_reaper(
    client: RedisClient,
    streams: Vec<String>,
    group: String,
    interval: Duration,
    min_idle_ms: u64,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let reaper = reaper_consumer_name(&group);
    tokio::spawn(async move {
        let mut conn = match acquire_conn_with_retry(&client, &shutdown).await {
            Some(c) => c,
            None => return,
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }

            let mut needs_reconnect = false;
            for stream in &streams {
                if shutdown.is_cancelled() {
                    return;
                }
                if let Err(e) =
                    autoclaim_all(&mut conn, stream, &group, &reaper, min_idle_ms, &shutdown).await
                {
                    tracing::warn!(
                        stream,
                        error = %e,
                        "reaper: XAUTOCLAIM failed, reconnecting",
                    );
                    needs_reconnect = true;
                    break;
                }
            }

            if needs_reconnect {
                match acquire_conn_with_retry(&client, &shutdown).await {
                    Some(c) => conn = c,
                    None => return,
                }
            }
        }
    })
}

/// Walk the PEL via XAUTOCLAIM cursor pagination until no more entries
/// match. Equivalent semantics to the helper that used to live inside each
/// consumer's loop in `consumer.rs`, just relocated so a single task can run
/// it on behalf of the whole group.
async fn autoclaim_all(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
    shutdown: &CancellationToken,
) -> Result<()> {
    use crate::error::ShoveError;
    use redis::streams::StreamAutoClaimReply;

    let mut cursor = "0-0".to_owned();
    loop {
        // Bail out between pages so a shutdown signal doesn't have to
        // wait for the entire PEL walk to finish. Worst-case shutdown
        // delay is now one in-flight XAUTOCLAIM round-trip rather than
        // `ceil(PEL_size / AUTOCLAIM_COUNT)` round-trips per shard.
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let reply: StreamAutoClaimReply = conn
            .query(
                redis::cmd("XAUTOCLAIM")
                    .arg(stream)
                    .arg(group)
                    .arg(consumer)
                    .arg(min_idle_ms)
                    .arg(&cursor)
                    .arg("COUNT")
                    .arg(AUTOCLAIM_COUNT),
            )
            .await
            .map_err(|e| ShoveError::Connection(format!("XAUTOCLAIM failed: {e}")))?;

        if reply.next_stream_id == "0-0" || reply.next_stream_id.is_empty() {
            break;
        }
        cursor = reply.next_stream_id;
    }
    Ok(())
}

/// Acquire a multiplexed Redis connection, retrying with exponential backoff
/// (1 s → 30 s, full jitter) until the shutdown token is cancelled. Mirrors
/// `requeue::acquire_conn_with_retry` — they're kept separate so each
/// sidecar can evolve independently.
async fn acquire_conn_with_retry(
    client: &RedisClient,
    shutdown: &CancellationToken,
) -> Option<RedisConnection> {
    let mut backoff = Backoff::default();
    loop {
        match client.multiplexed_conn().await {
            Ok(c) => return Some(c),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return None;
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    "reaper: connection failed ({}), retrying in {:.1}s",
                    e,
                    delay.as_secs_f64()
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reaper_consumer_name_is_stable_per_group() {
        assert_eq!(reaper_consumer_name("orders"), "shove-reaper-orders");
        assert_eq!(reaper_consumer_name("shove"), "shove-reaper-shove");
        // Same input → same name (regression guard for any future randomisation).
        assert_eq!(reaper_consumer_name("g"), reaper_consumer_name("g"));
    }
}
