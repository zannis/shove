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
//! Each sweep also trims fully-acknowledged entries (`XTRIM MINID`, see
//! [`trim_acked`]): XACK clears the PEL but leaves entries in the stream,
//! so without trimming, stream memory grows without bound and `XLEN`-based
//! backlog estimates drift ever upward.
//!
//! **At-least-once semantics**: if shove runs in multiple processes against
//! the same Redis, each spawns its own reaper. They race on XAUTOCLAIM, which
//! is safe — only one of them wins ownership of each reclaimed entry. Some
//! duplicate scanning happens but no entry is delivered twice.
//!
//! ## Redelivery
//!
//! After each XAUTOCLAIM page the reaper immediately re-delivers every
//! claimed entry: it XADDs the entry data back to the stream (making it
//! visible to regular consumers via `XREADGROUP ... >`) and then XACKs the
//! original ID to clear its own PEL. If XADD fails the entry stays in the
//! reaper's PEL and will be re-attempted on the next autoclaim cycle. If XACK
//! fails after a successful XADD the entry also stays in the PEL; the next
//! cycle will re-XADD it, producing an at-most-one-extra delivery — consistent
//! with the at-least-once guarantee this module provides.

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::backends::redis::client::{RedisClient, RedisConnection};
use crate::error::Result;

use super::client::acquire_conn_with_retry;
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
        let mut conn = match acquire_conn_with_retry(&client, &shutdown, "reaper").await {
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
                let result =
                    match autoclaim_all(&mut conn, stream, &group, &reaper, min_idle_ms, &shutdown)
                        .await
                    {
                        Ok(()) => trim_acked(&mut conn, stream, &group).await,
                        err => err,
                    };
                if let Err(e) = result {
                    tracing::warn!(
                        stream,
                        error = %e,
                        "reaper: sweep failed, reconnecting",
                    );
                    needs_reconnect = true;
                    break;
                }
            }

            if needs_reconnect {
                match acquire_conn_with_retry(&client, &shutdown, "reaper").await {
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

        // Redeliver every claimed entry: XADD it back so regular consumers
        // see it via `XREADGROUP ... >`, then XACK the original to clear it
        // from the reaper's PEL. If XADD fails the entry stays in the PEL
        // for the next cycle; if XACK fails after a successful XADD the
        // next cycle will re-XADD (at-least-once duplicate), which is
        // acceptable under the at-least-once delivery guarantee.
        for entry in &reply.claimed {
            let mut xadd_cmd = redis::cmd("XADD");
            xadd_cmd.arg(stream).arg("*");
            for (field, value) in &entry.map {
                match value {
                    redis::Value::BulkString(bytes) => {
                        xadd_cmd.arg(field.as_str()).arg(bytes.as_slice());
                    }
                    redis::Value::SimpleString(s) => {
                        xadd_cmd.arg(field.as_str()).arg(s.as_str());
                    }
                    // Integer or other types are not expected in stream
                    // entries written by shove; skip silently.
                    _ => {}
                }
            }

            match conn.query::<redis::Value>(&mut xadd_cmd).await {
                Ok(_) => {
                    if let Err(e) = conn
                        .query::<i64>(redis::cmd("XACK").arg(stream).arg(group).arg(&entry.id))
                        .await
                    {
                        tracing::warn!(
                            stream,
                            entry_id = %entry.id,
                            error = %e,
                            "reaper: XACK failed after redeliver — entry stays in PEL",
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        stream,
                        entry_id = %entry.id,
                        error = %e,
                        "reaper: XADD redeliver failed — entry stays in PEL for next cycle",
                    );
                }
            }
        }

        if reply.next_stream_id == "0-0" || reply.next_stream_id.is_empty() {
            break;
        }
        cursor = reply.next_stream_id;
    }
    Ok(())
}

/// Trim entries that every consumer in `group` has already acknowledged.
///
/// XACK removes an entry from the PEL but not from the stream, so without
/// trimming every stream grows without bound. The safe `XTRIM MINID`
/// threshold:
///
/// * PEL non-empty → the oldest pending entry id. Delivery is in id order,
///   so every entry below the oldest pending one was delivered and is no
///   longer pending — i.e. acknowledged.
/// * PEL empty → the group's `last-delivered-id`. Everything at or below it
///   is acked; using the id itself (MINID keeps entries `>=` threshold)
///   retains the last delivered entry, trading one kept entry for not
///   having to do stream-id arithmetic.
///
/// shove owns exactly one consumer group per stream, so the group's own
/// progress is the stream's global progress — there is no other group whose
/// unread entries could be trimmed away. DLQ streams are not in the
/// reaper's stream list and are never trimmed.
async fn trim_acked(conn: &mut RedisConnection, stream: &str, group: &str) -> Result<()> {
    use redis::streams::{StreamInfoGroupsReply, StreamPendingReply};

    let pending: StreamPendingReply = conn
        .query(redis::cmd("XPENDING").arg(stream).arg(group))
        .await?;

    let threshold = match pending {
        StreamPendingReply::Data(data) => data.start_id,
        StreamPendingReply::Empty => {
            let info: StreamInfoGroupsReply = conn
                .query(redis::cmd("XINFO").arg("GROUPS").arg(stream))
                .await?;
            let Some(g) = info.groups.into_iter().find(|g| g.name == group) else {
                // Group not declared on this stream (yet) — nothing to trim.
                return Ok(());
            };
            if g.last_delivered_id == "0-0" {
                // Nothing delivered yet — trimming would race fresh entries.
                return Ok(());
            }
            g.last_delivered_id
        }
        // `StreamPendingReply` is non_exhaustive; an unknown variant gives
        // no safe threshold, so skip the trim for this sweep.
        _ => return Ok(()),
    };

    let trimmed: i64 = conn
        .query(redis::cmd("XTRIM").arg(stream).arg("MINID").arg(&threshold))
        .await?;
    if trimmed > 0 {
        tracing::debug!(stream, trimmed, threshold, "reaper: trimmed acked entries");
    }
    Ok(())
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
