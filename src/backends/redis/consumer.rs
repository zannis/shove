//! Redis Streams consumer — XREADGROUP loop with outcome routing, hold-queue
//! scheduling via ZADD, DLQ routing via XADD, and XAUTOCLAIM crash recovery.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::consumer::ConsumerImpl;
use crate::backend::ConsumerOptionsInner;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;

use super::client::{RedisClient, RedisConnection};
use super::constants::{
    AUTOCLAIM_COUNT, BLOCK_MS, PAYLOAD_FIELD, X_DEATH_COUNT, X_DEATH_REASON, X_MESSAGE_ID,
    X_ORIGINAL_QUEUE, X_RETRY_COUNT, X_SEQUENCE_KEY,
};
use super::requeue::{HoldEntry, enqueue_hold, spawn_requeuer};
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// RedisConsumer
// ---------------------------------------------------------------------------

/// Consumer backed by Redis Streams via XREADGROUP.
#[derive(Clone)]
pub struct RedisConsumer {
    client: RedisClient,
}

impl RedisConsumer {
    /// Create a new consumer backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Generate a unique consumer name for this process instance.
    ///
    /// Format: `{hostname}-{uuid4}`. Unique per task so XAUTOCLAIM can
    /// differentiate between dead and active consumers.
    fn consumer_name() -> String {
        // Try HOSTNAME env var first (set in most Unix environments), fall back
        // to "unknown" — the uuid suffix guarantees uniqueness regardless.
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
        let uid = uuid::Uuid::new_v4();
        format!("{hostname}-{uid}")
    }
}

impl ConsumerImpl for RedisConsumer {
    fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let stream = topology.queue();
            run_stream_loop::<T, H>(client, handler, ctx, options, topology, stream).await
        }
    }

    fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let consumer = self.clone();
        async move {
            let handles = consumer
                .spawn_fifo_shards::<T, H>(handler, ctx, options)
                .await?;
            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!("sequenced shard task failed: {e}"),
                    Err(e) => tracing::error!("sequenced shard task panicked: {e}"),
                }
            }
            Ok(())
        }
    }

    fn run_dlq<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let dlq_name = topology.dlq().ok_or_else(|| {
                ShoveError::Topology(format!(
                    "run_dlq called on topic {} without DLQ",
                    topology.queue()
                ))
            })?;
            let shutdown = CancellationToken::new();
            let options = ConsumerOptionsInner::defaults_with_shutdown(shutdown);
            run_stream_loop::<T, H>(client, handler, ctx, options, topology, dlq_name).await
        }
    }

    fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<Vec<tokio::task::JoinHandle<Result<()>>>>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let seq = topology.sequencing().ok_or_else(|| {
                ShoveError::Topology(format!(
                    "spawn_fifo_shards called on topic {} without sequencing config",
                    topology.queue()
                ))
            })?;

            let n_shards = seq.routing_shards();
            let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> =
                Vec::with_capacity(n_shards as usize);

            // Wrap handler/ctx in Arc so each shard task can share without
            // requiring H: Clone. The inner loop runs sequentially per shard,
            // so there's no concurrent access to the handler within a shard.
            let handler = Arc::new(handler);
            let ctx = Arc::new(ctx);

            for shard_idx in 0..n_shards {
                let stream_name =
                    RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard_idx);

                // Per-shard hold queue names use the shard-specific naming from topology.
                let shard_hold_queues = topology.shard_hold_queue_names(shard_idx);

                let client = client.clone();
                // Arc::clone is cheap — each shard gets its own Arc handle.
                let handler = Arc::clone(&handler);
                let ctx = Arc::clone(&ctx);
                let options = options.clone();

                handles.push(tokio::spawn(async move {
                    let hold_names: Vec<String> =
                        shard_hold_queues.iter().map(|hq| hq.name().to_owned()).collect();

                    let shutdown = options.shutdown.clone();
                    let requeue_handle = if !hold_names.is_empty() {
                        Some(spawn_requeuer(client.clone(), hold_names, shutdown.clone()))
                    } else {
                        None
                    };

                    let result = run_stream_loop_arc::<T, H>(
                        client,
                        handler,
                        ctx,
                        options,
                        topology,
                        &stream_name,
                        &shard_hold_queues,
                    )
                    .await;

                    if let Some(h) = requeue_handle {
                        h.abort();
                    }
                    result
                }));
            }

            Ok(handles)
        }
    }
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------

async fn run_stream_loop<T, H>(
    client: RedisClient,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let hold_queues = topology.hold_queues();
    let shutdown = options.shutdown.clone();

    let hold_names: Vec<String> = hold_queues.iter().map(|hq| hq.name().to_owned()).collect();
    let requeue_handle = if !hold_names.is_empty() {
        Some(spawn_requeuer(client.clone(), hold_names, shutdown.clone()))
    } else {
        None
    };

    let result = run_stream_loop_arc::<T, H>(
        client,
        Arc::new(handler),
        Arc::new(ctx),
        options,
        topology,
        stream,
        hold_queues,
    )
    .await;

    if let Some(h) = requeue_handle {
        h.abort();
    }
    result
}

/// Core consumer loop that takes `Arc<H>` and `Arc<H::Context>` so it can be
/// shared across shard tasks without requiring `H: Clone`.
async fn run_stream_loop_arc<T, H>(
    client: RedisClient,
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
    hold_queues: &[crate::topology::HoldQueue],
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let group = client.group().to_owned();
    let consumer = RedisConsumer::consumer_name();
    let shutdown = options.shutdown.clone();
    let topic_name = topology.queue();
    let consumer_group = options.consumer_group.as_deref();

    // Idle threshold for XAUTOCLAIM — use handler_timeout as the basis so that
    // messages claimed by a crashed consumer are reclaimed after roughly the
    // same interval as a handler timeout.
    let idle_ms = options
        .handler_timeout
        .unwrap_or(Duration::from_secs(30))
        .as_millis() as u64;

    // Reclaim stale pending entries from prior crashed consumers on startup.
    if let Ok(mut conn) = client.dedicated_conn().await {
        let _ = autoclaim_all(&mut conn, stream, &group, &consumer, idle_ms).await;
    }

    let mut conn = client.dedicated_conn().await?;
    let prefetch = options.prefetch_count.max(1) as usize;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // XREADGROUP GROUP {group} {consumer} COUNT {prefetch} BLOCK {BLOCK_MS} STREAMS {stream} >
        let raw_reply: redis::Value = match conn
            .query(
                redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(prefetch)
                    .arg("BLOCK")
                    .arg(BLOCK_MS)
                    .arg("STREAMS")
                    .arg(stream)
                    .arg(">"),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, stream, "XREADGROUP failed, retrying after 500ms");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        let entries = parse_xreadgroup_reply(raw_reply);

        for (entry_id, fields_vec) in entries {
            let fields: HashMap<String, String> = fields_vec.into_iter().collect();

            // Extract payload.
            let payload_raw = match fields.get(PAYLOAD_FIELD) {
                Some(s) => s.clone(),
                None => {
                    tracing::warn!(entry_id, "missing payload field — acking and skipping");
                    let _ = xack(&mut conn, stream, &group, &entry_id).await;
                    continue;
                }
            };

            let retry_count = fields
                .get(X_RETRY_COUNT)
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);

            // Size check.
            if let Some(max) = options.max_message_size {
                if payload_raw.len() > max {
                    tracing::warn!(
                        entry_id,
                        size = payload_raw.len(),
                        limit = max,
                        "message exceeds size limit — sending to DLQ"
                    );
                    metrics::record_failed(topic_name, consumer_group, metrics::FailReason::Oversize);
                    route_to_dlq(
                        &mut conn,
                        topology,
                        stream,
                        &group,
                        &entry_id,
                        &fields,
                        "oversize",
                        retry_count,
                    )
                    .await;
                    continue;
                }
            }

            // Deserialize.
            let msg: T::Message = match serde_json::from_str(&payload_raw) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, entry_id, "deserialization failed — sending to DLQ");
                    metrics::record_failed(topic_name, consumer_group, metrics::FailReason::Deserialize);
                    route_to_dlq(
                        &mut conn,
                        topology,
                        stream,
                        &group,
                        &entry_id,
                        &fields,
                        "deserialize",
                        retry_count,
                    )
                    .await;
                    continue;
                }
            };

            let delivery_id = fields
                .get(X_MESSAGE_ID)
                .cloned()
                .unwrap_or_else(|| entry_id.clone());

            let meta = MessageMetadata {
                retry_count,
                delivery_id,
                redelivered: retry_count > 0,
                headers: build_headers(&fields),
            };

            options.processing.store(true, std::sync::atomic::Ordering::Release);

            let handler_clone = Arc::clone(&handler);
            let ctx_clone = Arc::clone(&ctx);
            let topic_arc: Arc<str> = Arc::from(topic_name);
            let group_arc: Option<Arc<str>> = consumer_group.map(Arc::from);

            let _inflight = metrics::InflightGuard::new(topic_arc.clone(), group_arc.clone());
            let start = std::time::Instant::now();

            let outcome_opt = match options.handler_timeout {
                Some(timeout_dur) => {
                    match tokio::time::timeout(
                        timeout_dur,
                        handler_clone.handle(msg, meta, &ctx_clone),
                    )
                    .await
                    {
                        Ok(o) => Some(o),
                        Err(_) => {
                            tracing::warn!(
                                entry_id,
                                timeout = ?timeout_dur,
                                "handler timed out — leaving in PEL for XAUTOCLAIM"
                            );
                            metrics::record_failed(
                                &topic_arc,
                                group_arc.as_deref(),
                                metrics::FailReason::Timeout,
                            );
                            // Do NOT ack — XAUTOCLAIM will reclaim it after idle_ms.
                            None
                        }
                    }
                }
                None => Some(handler_clone.handle(msg, meta, &ctx_clone).await),
            };

            let elapsed = start.elapsed().as_secs_f64();

            let Some(outcome) = outcome_opt else {
                options
                    .processing
                    .store(false, std::sync::atomic::Ordering::Release);
                continue;
            };

            metrics::record_consumed(&topic_arc, group_arc.as_deref(), &outcome);
            metrics::record_processing_duration(&topic_arc, group_arc.as_deref(), &outcome, elapsed);
            options
                .processing
                .store(false, std::sync::atomic::Ordering::Release);

            route_outcome(
                &mut conn,
                topology,
                stream,
                &group,
                &entry_id,
                &fields,
                outcome,
                retry_count,
                options.max_retries,
                hold_queues,
            )
            .await;
        }

        // Periodically reclaim stale PEL entries.
        let _ = autoclaim_all(&mut conn, stream, &group, &consumer, idle_ms).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Outcome routing
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    conn: &mut RedisConnection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    fields: &HashMap<String, String>,
    outcome: Outcome,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[crate::topology::HoldQueue],
) {
    match outcome {
        Outcome::Ack => {
            let _ = xack(conn, stream, group, entry_id).await;
        }
        Outcome::Retry => {
            let new_retry = retry_count + 1;
            if new_retry >= max_retries {
                route_to_dlq(
                    conn,
                    topology,
                    stream,
                    group,
                    entry_id,
                    fields,
                    "max-retries",
                    new_retry,
                )
                .await;
            } else if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Retry but no hold queues — re-queueing immediately"
                );
                requeue_to_stream(conn, stream, fields, new_retry).await;
                let _ = xack(conn, stream, group, entry_id).await;
            } else {
                let level = (new_retry as usize).min(hold_queues.len() - 1);
                let hq = &hold_queues[level];
                route_to_hold(conn, stream, group, entry_id, fields, hq.name(), hq.delay(), new_retry)
                    .await;
            }
        }
        Outcome::Reject => {
            route_to_dlq(conn, topology, stream, group, entry_id, fields, "rejected", retry_count)
                .await;
        }
        Outcome::Defer => {
            if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Defer but no hold queues — re-queueing immediately"
                );
                requeue_to_stream(conn, stream, fields, retry_count).await;
                let _ = xack(conn, stream, group, entry_id).await;
            } else {
                let hq = &hold_queues[0];
                // Defer does NOT increment retry count.
                route_to_hold(conn, stream, group, entry_id, fields, hq.name(), hq.delay(), retry_count)
                    .await;
            }
        }
    }
}

async fn route_to_hold(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    entry_id: &str,
    fields: &HashMap<String, String>,
    hold_name: &str,
    delay: Duration,
    new_retry_count: u32,
) {
    let mut hold_fields: Vec<(String, String)> = fields
        .iter()
        .filter(|(k, _)| k.as_str() != X_RETRY_COUNT)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    hold_fields.push((X_RETRY_COUNT.into(), new_retry_count.to_string()));

    let entry = HoldEntry {
        stream: stream.to_owned(),
        fields: hold_fields,
    };

    if let Err(e) = enqueue_hold(conn, hold_name, entry, delay).await {
        tracing::warn!(error = %e, hold_name, "enqueue_hold failed — message may be lost");
        return;
    }
    let _ = xack(conn, stream, group, entry_id).await;
}

async fn route_to_dlq(
    conn: &mut RedisConnection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    fields: &HashMap<String, String>,
    reason: &str,
    death_count: u32,
) {
    let dlq = match topology.dlq() {
        Some(d) => d,
        None => {
            tracing::warn!(stream, entry_id, reason, "no DLQ configured — discarding");
            let _ = xack(conn, stream, group, entry_id).await;
            return;
        }
    };

    let mut cmd = redis::cmd("XADD");
    cmd.arg(dlq).arg("*");
    for (k, v) in fields {
        cmd.arg(k.as_str()).arg(v.as_str());
    }
    cmd.arg(X_DEATH_REASON).arg(reason);
    cmd.arg(X_DEATH_COUNT).arg(death_count.to_string());
    cmd.arg(X_ORIGINAL_QUEUE).arg(stream);

    if let Err(e) = conn.query::<redis::Value>(&mut cmd).await {
        tracing::warn!(error = %e, dlq, "XADD to DLQ failed");
    }
    let _ = xack(conn, stream, group, entry_id).await;
}

async fn requeue_to_stream(
    conn: &mut RedisConnection,
    stream: &str,
    fields: &HashMap<String, String>,
    retry_count: u32,
) {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("*");
    for (k, v) in fields {
        if k.as_str() != X_RETRY_COUNT {
            cmd.arg(k.as_str()).arg(v.as_str());
        }
    }
    cmd.arg(X_RETRY_COUNT).arg(retry_count.to_string());
    let _ = conn.query::<redis::Value>(&mut cmd).await;
}

async fn xack(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    entry_id: &str,
) -> Result<()> {
    conn.query::<i64>(
        redis::cmd("XACK")
            .arg(stream)
            .arg(group)
            .arg(entry_id),
    )
    .await
    .map(|_| ())
    .map_err(|e| ShoveError::Connection(format!("XACK failed: {e}")))
}

async fn autoclaim_all(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
) -> Result<()> {
    conn.query::<redis::Value>(
        redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(AUTOCLAIM_COUNT),
    )
    .await
    .map(|_| ())
    .map_err(|e| ShoveError::Connection(format!("XAUTOCLAIM failed: {e}")))
}

// ---------------------------------------------------------------------------
// XREADGROUP reply parser
// ---------------------------------------------------------------------------

/// Parse the raw `redis::Value` reply from XREADGROUP into a flat list of
/// `(entry_id, fields)` pairs. Returns an empty vec on nil reply (timeout)
/// or any parse error.
///
/// Expected structure:
/// ```text
/// Bulk array [
///   Bulk array [        // per stream key
///     stream_name: BulkString,
///     entries: Bulk array [
///       entry: Bulk array [
///         id: BulkString,
///         fields: Bulk array [field, value, field, value, ...]
///       ]
///     ]
///   ]
/// ]
/// ```
pub(super) fn parse_xreadgroup_reply(
    value: redis::Value,
) -> Vec<(String, Vec<(String, String)>)> {
    let streams = match value {
        redis::Value::Nil => return vec![],
        redis::Value::Array(arr) => arr,
        _ => return vec![],
    };

    let mut result = Vec::new();

    for stream_item in streams {
        let stream_pair = match stream_item {
            redis::Value::Array(arr) if arr.len() >= 2 => arr,
            _ => continue,
        };

        // stream_pair[1] is the list of entries
        let entry_list = match &stream_pair[1] {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        for entry_item in entry_list {
            let entry_pair = match entry_item {
                redis::Value::Array(arr) if arr.len() >= 2 => arr,
                _ => continue,
            };

            let entry_id = match &entry_pair[0] {
                redis::Value::BulkString(b) => match std::str::from_utf8(b) {
                    Ok(s) => s.to_owned(),
                    Err(_) => continue,
                },
                redis::Value::SimpleString(s) => s.clone(),
                _ => continue,
            };

            let field_list = match &entry_pair[1] {
                redis::Value::Array(arr) => arr,
                _ => continue,
            };

            let mut fields: Vec<(String, String)> = Vec::new();
            let mut iter = field_list.iter();
            loop {
                let key = match iter.next() {
                    Some(redis::Value::BulkString(b)) => match std::str::from_utf8(b) {
                        Ok(s) => s.to_owned(),
                        Err(_) => break,
                    },
                    Some(redis::Value::SimpleString(s)) => s.clone(),
                    Some(_) => break,
                    None => break,
                };
                let val = match iter.next() {
                    Some(redis::Value::BulkString(b)) => {
                        String::from_utf8_lossy(b).into_owned()
                    }
                    Some(redis::Value::SimpleString(s)) => s.clone(),
                    Some(redis::Value::Nil) => String::new(),
                    Some(_) => break,
                    None => break,
                };
                fields.push((key, val));
            }

            result.push((entry_id, fields));
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the `headers` map for `MessageMetadata` from stream entry fields,
/// excluding internal shove fields that are exposed via dedicated metadata
/// fields.
fn build_headers(fields: &HashMap<String, String>) -> HashMap<String, String> {
    const SKIP: &[&str] = &[PAYLOAD_FIELD, X_RETRY_COUNT, X_SEQUENCE_KEY];
    fields
        .iter()
        .filter(|(k, _)| !SKIP.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// hold_level utility
// ---------------------------------------------------------------------------

/// Map a `retry_count` to a hold-queue index, clamped to the last element.
///
/// Returns `None` if the slice is empty (no hold queues configured).
pub(super) fn hold_level<T>(retry_count: u32, hold_queues: &[T]) -> Option<usize> {
    if hold_queues.is_empty() {
        None
    } else {
        Some((retry_count as usize).min(hold_queues.len() - 1))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_count_routing_to_hold_level() {
        let hold_queues = vec!["orders-hold-5s", "orders-hold-30s"];
        assert_eq!(hold_level(0, &hold_queues), Some(0));
        assert_eq!(hold_level(1, &hold_queues), Some(1));
        assert_eq!(hold_level(2, &hold_queues), Some(1)); // clamped to last
    }

    #[test]
    fn hold_level_empty_returns_none() {
        assert_eq!(hold_level(0, &[""]), Some(0));
        let empty: Vec<&str> = vec![];
        assert_eq!(hold_level(0, &empty), None);
    }

    #[test]
    fn parse_xreadgroup_nil_returns_empty() {
        let result = parse_xreadgroup_reply(redis::Value::Nil);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_empty_array_returns_empty() {
        let result = parse_xreadgroup_reply(redis::Value::Array(vec![]));
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_valid_entry() {
        // Simulate:
        // [
        //   [ "mystream", [
        //     [ "1234-0", ["payload", "{}", "x-retry-count", "0"] ]
        //   ]]
        // ]
        let entry = redis::Value::Array(vec![
            redis::Value::BulkString(b"1234-0".to_vec()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::BulkString(b"{}".to_vec()),
                redis::Value::BulkString(b"x-retry-count".to_vec()),
                redis::Value::BulkString(b"0".to_vec()),
            ]),
        ]);
        let stream = redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![entry]),
        ]);
        let reply = redis::Value::Array(vec![stream]);

        let result = parse_xreadgroup_reply(reply);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "1234-0");
        assert_eq!(result[0].1.len(), 2);
        assert_eq!(result[0].1[0], ("payload".to_string(), "{}".to_string()));
        assert_eq!(
            result[0].1[1],
            ("x-retry-count".to_string(), "0".to_string())
        );
    }
}
