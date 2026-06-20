//! Redis Streams consumer — XREADGROUP loop with outcome routing, hold-queue
//! scheduling via ZADD, DLQ routing via XADD, and XAUTOCLAIM crash recovery.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::ConsumerOptions;
use crate::backend::ConsumerOptionsInner;
use crate::backend::consumer::ConsumerImpl;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::markers::Redis;
use crate::metadata::MessageMetadata;
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::hold_index;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::{HoldQueue, QueueTopology};

use super::client::{RedisClient, RedisConnection};
use super::constants::{
    BLOCK_MS, PAYLOAD_FIELD, X_DEATH_COUNT, X_DEATH_REASON, X_MESSAGE_ID, X_ORIGINAL_QUEUE,
    X_RETRY_COUNT, X_SEQUENCE_KEY,
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

    /// Run the consumer with concurrent in-flight handlers.
    ///
    /// Each XREADGROUP-returned entry is dispatched to a fresh tokio task
    /// that owns its own multiplexed connection for outcome routing
    /// (XACK / hold / DLQ). A semaphore caps in-flight handlers at
    /// `options.prefetch_count`. On shutdown the main loop drains by
    /// reacquiring all permits before returning.
    ///
    /// Sequential dispatch (the [`ConsumerImpl::run`] path) is preserved
    /// untouched for groups that opt out of `concurrent_processing`.
    pub(super) async fn run_concurrent<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T> + 'static,
        H::Context: 'static,
    {
        let topology = T::topology();
        let stream = topology.queue();
        let hold_queues = topology.hold_queues();
        let shutdown = options.shutdown.clone();

        let hold_names: Vec<String> = hold_queues.iter().map(|hq| hq.name().to_owned()).collect();
        let requeue_handle = if !hold_names.is_empty() {
            Some(spawn_requeuer(
                self.client.clone(),
                hold_names,
                shutdown.clone(),
            ))
        } else {
            None
        };

        let result = run_stream_loop_concurrent::<T, H>(
            self.client.clone(),
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
}

// ---------------------------------------------------------------------------
// Inherent public API — mirrors the NatsConsumer/KafkaConsumer surface so
// users who hold a RedisConsumer directly can drive it without going
// through the generic ConsumerSupervisor<B>.
// ---------------------------------------------------------------------------

impl RedisConsumer {
    /// Run the non-FIFO consumer loop until `options.shutdown` is cancelled.
    ///
    /// Always uses the sequential single-message XREADGROUP dispatch path.
    /// To opt into concurrent in-flight dispatch (semaphore-gated), register
    /// the topic through [`RedisConsumerGroupRegistry`] with
    /// [`RedisConsumerGroupConfig::with_concurrent_processing(true)`].
    ///
    /// [`RedisConsumerGroupRegistry`]: super::consumer_group::RedisConsumerGroupRegistry
    /// [`RedisConsumerGroupConfig::with_concurrent_processing(true)`]: super::consumer_group::RedisConsumerGroupConfig::with_concurrent_processing
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<Redis>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        <Self as ConsumerImpl>::run::<T, H>(self, handler, ctx, options.into_inner()).await
    }

    /// Run a FIFO (sequenced) consumer loop until `options.shutdown` is
    /// cancelled. Spawns one shard worker per `routing_shards` and awaits
    /// every handle.
    pub async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<Redis>,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        <Self as ConsumerImpl>::run_fifo::<T, H>(self, handler, ctx, options.into_inner()).await
    }

    /// Drive `run_fifo` until `signal` fires, then drain shard tasks with
    /// `drain_timeout`. Aborted shards are counted in the returned outcome.
    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<Redis>,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        let inner = options.into_inner();
        let shutdown = inner.shutdown.clone();
        let handles = match <Self as ConsumerImpl>::spawn_fifo_shards::<T, H>(
            self, handler, ctx, inner,
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(error = %e, "run_fifo_until_timeout: shard spawn failed");
                return SupervisorOutcome {
                    errors: 1,
                    panics: 0,
                    timed_out: false,
                };
            }
        };
        drive_fifo_until_timeout(handles, shutdown, signal, drain_timeout).await
    }

    /// Drain the DLQ stream of topic `T` with the supplied handler.
    ///
    /// The loop runs until the underlying JoinHandle is aborted by the caller
    /// — the DLQ consumer does not accept an external shutdown token (matches
    /// the [`ConsumerImpl::run_dlq`] contract).
    /// Public DLQ entrypoint with default options. The DLQ loop spins until
    /// the underlying JoinHandle is aborted by the caller (matches the
    /// [`ConsumerImpl::run_dlq`] contract).
    pub async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let options = crate::ConsumerOptions::<crate::Redis>::new().into_inner();
        <Self as ConsumerImpl>::run_dlq::<T, H>(self, handler, ctx, options).await
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
            run_stream_loop::<T, H>(
                client,
                handler,
                ctx,
                options,
                topology,
                stream,
                Maintain::Stream,
            )
            .await
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
        options: ConsumerOptionsInner,
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
            // Maintain::None: DLQ streams are an operator audit record —
            // they get neither autoclaim redelivery nor acked-entry
            // trimming from the maintenance sidecar.
            run_stream_loop::<T, H>(
                client,
                handler,
                ctx,
                options,
                topology,
                dlq_name,
                Maintain::None,
            )
            .await
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
                    let hold_names: Vec<String> = shard_hold_queues
                        .iter()
                        .map(|hq| hq.name().to_owned())
                        .collect();

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
                        Maintain::Stream,
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
// Reconnect wrapper
// ---------------------------------------------------------------------------

/// Run `f` in a reconnect loop, retrying on transient errors until shutdown.
///
/// Acquires a fresh connection on each attempt and applies exponential backoff
/// with jitter (1 s → 30 s). Non-retryable errors are propagated immediately.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    stream: &str,
    max_reconnect_attempts: Option<u32>,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = Backoff::default();
    let mut attempts = 0u32;
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !e.is_retryable() {
                    return Err(e);
                }
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                attempts += 1;
                if let Some(max) = max_reconnect_attempts
                    && attempts >= max
                {
                    tracing::error!(
                        stream,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{stream}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    stream,
                    attempt = attempts,
                    ?max_reconnect_attempts,
                    error = %e,
                    "consumer error, reconnecting in {delay:?}"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------

/// Whether a consumer loop enrols its stream in background maintenance
/// (XAUTOCLAIM crash recovery + acked-entry trimming via the per-process
/// registry in [`super::maintenance`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Maintain {
    /// Regular work stream — acquire a maintenance interest.
    Stream,
    /// No maintenance. Used for DLQ streams, which are an operator audit
    /// record: dead entries must never be reclaimed or trimmed.
    None,
}

async fn run_stream_loop<T, H>(
    client: RedisClient,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
    maintain: Maintain,
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
        maintain,
    )
    .await;

    if let Some(h) = requeue_handle {
        h.abort();
    }
    result
}

/// Core consumer loop that takes `Arc<H>` and `Arc<H::Context>` so it can be
/// shared across shard tasks without requiring `H: Clone`.
#[allow(clippy::too_many_arguments)]
async fn run_stream_loop_arc<T, H>(
    client: RedisClient,
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
    hold_queues: &[HoldQueue],
    maintain: Maintain,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let group = client.group().to_owned();
    let shutdown = options.shutdown.clone();
    let topic_name = topology.queue();
    let consumer_group = options.consumer_group.as_deref();

    // Hold a maintenance interest (reaper: XAUTOCLAIM recovery + acked-entry
    // trimming) for this stream while the consumer runs. The registry dedupes
    // per (client, stream, group), so N consumers still share one sidecar.
    let _maintenance = (maintain == Maintain::Stream)
        .then(|| super::maintenance::acquire(&client, stream, options.handler_timeout));

    // Pre-compute metric label arcs once — reused cheaply for every message.
    let topic_arc: Arc<str> = Arc::from(topic_name);
    let group_arc: Option<Arc<str>> = consumer_group.map(Arc::from);

    let prefetch = options.prefetch_count.max(1) as usize;

    run_with_reconnect(&shutdown, stream, options.max_reconnect_attempts, || {
        let client = client.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let options = options.clone();
        let group = group.clone();
        let consumer = RedisConsumer::consumer_name();
        tracing::debug!(
            consumer,
            stream,
            "new consumer name registered; previous name left as stale entry in group until XGROUP DELCONSUMER is called"
        );
        let topic_arc = Arc::clone(&topic_arc);
        let group_arc = group_arc.clone();
        let shutdown = shutdown.clone();

        async move {
            let mut conn = client.dedicated_conn().await?;
            // XAUTOCLAIM has been hoisted out of the per-consumer hot path —
            // see `reaper.rs` for the consolidated sidecar that runs it on
            // behalf of the whole group.

            loop {
                if shutdown.is_cancelled() {
                    return Ok(());
                }

                let mut xreadgroup_cmd = redis::cmd("XREADGROUP");
                xreadgroup_cmd
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(prefetch)
                    .arg("BLOCK")
                    .arg(BLOCK_MS)
                    .arg("STREAMS")
                    .arg(stream)
                    .arg(">");
                let xreadgroup_fut = conn.query(&mut xreadgroup_cmd);

                let raw_reply: redis::Value = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    result = xreadgroup_fut => match result {
                        Ok(v) => v,
                        Err(e) => {
                            // NOGROUP means the consumer group does not exist on the stream.
                            // This is transient after a Redis restart with data loss while the
                            // application re-declares topology. Return a retryable Connection
                            // error so run_with_reconnect backs off and retries.
                            if e.to_string().contains("NOGROUP") {
                                tracing::warn!(
                                    stream,
                                    error = %e,
                                    "consumer group does not exist — topology may not be declared yet; will retry"
                                );
                                return Err(ShoveError::Connection(format!(
                                    "consumer group does not exist on stream '{stream}': {e}"
                                )));
                            }
                            tracing::warn!(error = %e, stream, "XREADGROUP failed");
                            return Err(e);
                        }
                    }
                };

                let entries = parse_xreadgroup_reply(raw_reply, prefetch);

                for (entry_id, fields_vec) in entries {
                    let (mut fields, user_headers) = partition_entry_fields(fields_vec);

                    // Extract payload — take ownership to avoid cloning on the hot path.
                    let payload_raw = match fields.remove(PAYLOAD_FIELD) {
                        Some(s) => s,
                        None => {
                            tracing::warn!(entry_id, "missing payload field — acking and skipping");
                            if let Err(e) = xack(&mut conn, stream, &group, &entry_id).await {
                                tracing::warn!(entry_id, error = %e, "XACK failed after skipping corrupt entry");
                                metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
                            }
                            continue;
                        }
                    };

                    let retry_count = fields
                        .get(X_RETRY_COUNT)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);

                    // Size check.
                    if let Some(max) = options.max_message_size
                        && payload_raw.len() > max
                    {
                        tracing::warn!(
                            entry_id,
                            size = payload_raw.len(),
                            limit = max,
                            "message exceeds size limit — sending to DLQ"
                        );
                        metrics::record_failed(
                            topic_name,
                            consumer_group,
                            metrics::FailReason::Oversize,
                        );
                        fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
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
                        .await?;
                        continue;
                    }

                    // Deserialize.
                    let msg: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(
                        payload_raw.as_bytes(),
                    ) {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                entry_id,
                                "deserialization failed — sending to DLQ"
                            );
                            metrics::record_failed(
                                topic_name,
                                consumer_group,
                                metrics::FailReason::Deserialize,
                            );
                            fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
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
                            .await?;
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
                        headers: Arc::new(user_headers),
                    };

                    options
                        .processing
                        .store(true, std::sync::atomic::Ordering::Release);

                    let handler_clone = Arc::clone(&handler);
                    let ctx_clone = Arc::clone(&ctx);

                    let _inflight =
                        metrics::InflightGuard::new(topic_arc.clone(), group_arc.clone());
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
                    metrics::record_processing_duration(
                        &topic_arc,
                        group_arc.as_deref(),
                        &outcome,
                        elapsed,
                    );
                    options
                        .processing
                        .store(false, std::sync::atomic::Ordering::Release);

                    fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
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
                    .await?;
                }
                // Periodic XAUTOCLAIM removed — handled by the group-wide
                // reaper sidecar in `reaper.rs`.
            }
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Concurrent core loop
// ---------------------------------------------------------------------------

/// Concurrent variant of [`run_stream_loop_arc`].
///
/// Differences vs. the sequential loop:
///
/// * A `tokio::sync::Semaphore` initialised with `options.prefetch_count`
///   permits caps in-flight handlers. The main loop blocks on
///   `acquire_owned()` before spawning, providing natural backpressure.
/// * Each dispatched message gets its own tokio task that:
///   1. runs the handler under its existing timeout,
///   2. acquires a fresh `multiplexed_conn` (cheap — multiplexed clients
///      share an underlying socket), and
///   3. routes the outcome (XACK / hold / DLQ) using that connection,
///   4. drops the permit so the next fetch can proceed.
/// * On shutdown the main loop calls `acquire_many(prefetch_count)` to wait
///   for every in-flight task to complete before returning.
///
/// XACK / hold-queue / DLQ routing are unchanged; they execute inside the
/// spawned task instead of the polling task.
#[allow(clippy::too_many_arguments)]
async fn run_stream_loop_concurrent<T, H>(
    client: RedisClient,
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
    hold_queues: &'static [HoldQueue],
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T> + 'static,
    H::Context: 'static,
{
    use tokio::sync::Semaphore;

    let group = client.group().to_owned();
    let shutdown = options.shutdown.clone();
    let topic_name = topology.queue();
    let consumer_group = options.consumer_group.as_deref();

    // Same per-(client, stream, group) maintenance interest as the
    // sequential loop — see run_stream_loop_arc.
    let _maintenance = super::maintenance::acquire(&client, stream, options.handler_timeout);

    let topic_arc: Arc<str> = Arc::from(topic_name);
    let group_arc: Option<Arc<str>> = consumer_group.map(Arc::from);

    let prefetch = options.prefetch_count.max(1) as usize;

    let semaphore = Arc::new(Semaphore::new(prefetch));
    let max_retries = options.max_retries;
    let max_message_size = options.max_message_size;
    let handler_timeout = options.handler_timeout;
    let processing = options.processing.clone();

    run_with_reconnect(&shutdown, stream, options.max_reconnect_attempts, || {
        let client = client.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let consumer = RedisConsumer::consumer_name();
        let topic_arc = Arc::clone(&topic_arc);
        let group_arc = group_arc.clone();
        let shutdown = shutdown.clone();
        let semaphore = Arc::clone(&semaphore);
        let processing = Arc::clone(&processing);
        let group = group.clone();

        async move {
            let mut conn = client.dedicated_conn().await?;
            // Acquire ONE multiplexed connection per reconnect cycle and hand
            // `.clone()`s to each spawned handler. MultiplexedConnection clones
            // share the underlying socket and multiplexer task, so this caps
            // socket creation at one-per-consumer-task instead of
            // one-per-message — the old per-spawn
            // `task_client.multiplexed_conn().await` pattern exhausts the
            // macOS ephemeral port range under fast handler workloads. On
            // reconnect (this closure re-runs) the outcome connection is
            // dialed afresh, recovering from a dead socket without further
            // plumbing.
            let outcome_conn = client.multiplexed_conn().await?;
            // XAUTOCLAIM has been hoisted out of the per-consumer hot path —
            // see `reaper.rs` for the consolidated sidecar that runs it on
            // behalf of the whole group.

            loop {
                if shutdown.is_cancelled() {
                    // Drain in-flight handlers before returning.
                    let _ = semaphore.acquire_many(prefetch as u32).await;
                    return Ok(());
                }

                let mut xreadgroup_cmd = redis::cmd("XREADGROUP");
                xreadgroup_cmd
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(prefetch)
                    .arg("BLOCK")
                    .arg(BLOCK_MS)
                    .arg("STREAMS")
                    .arg(stream)
                    .arg(">");
                let xreadgroup_fut = conn.query(&mut xreadgroup_cmd);

                let raw_reply: redis::Value = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        let _ = semaphore.acquire_many(prefetch as u32).await;
                        return Ok(());
                    }
                    result = xreadgroup_fut => match result {
                        Ok(v) => v,
                        Err(e) => {
                            if e.to_string().contains("NOGROUP") {
                                tracing::warn!(
                                    stream,
                                    error = %e,
                                    "consumer group does not exist — topology may not be declared yet; will retry"
                                );
                                return Err(ShoveError::Connection(format!(
                                    "consumer group does not exist on stream '{stream}': {e}"
                                )));
                            }
                            tracing::warn!(error = %e, stream, "XREADGROUP failed");
                            return Err(e);
                        }
                    }
                };

                let entries = parse_xreadgroup_reply(raw_reply, prefetch);

                for (entry_id, fields_vec) in entries {
                    let (mut fields, user_headers) = partition_entry_fields(fields_vec);

                    // Extract payload — take ownership to avoid cloning on the hot path.
                    let payload_raw = match fields.remove(PAYLOAD_FIELD) {
                        Some(s) => s,
                        None => {
                            tracing::warn!(entry_id, "missing payload field — acking and skipping");
                            if let Err(e) = xack(&mut conn, stream, &group, &entry_id).await {
                                tracing::warn!(entry_id, error = %e, "XACK failed after skipping corrupt entry");
                                metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
                            }
                            continue;
                        }
                    };

                    let retry_count = fields
                        .get(X_RETRY_COUNT)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);

                    if let Some(max) = max_message_size
                        && payload_raw.len() > max
                    {
                        tracing::warn!(
                            entry_id,
                            size = payload_raw.len(),
                            limit = max,
                            "message exceeds size limit — sending to DLQ"
                        );
                        metrics::record_failed(
                            topic_name,
                            consumer_group,
                            metrics::FailReason::Oversize,
                        );
                        fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
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
                        .await?;
                        continue;
                    }

                    let msg: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(
                        payload_raw.as_bytes(),
                    ) {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                entry_id,
                                "deserialization failed — sending to DLQ"
                            );
                            metrics::record_failed(
                                topic_name,
                                consumer_group,
                                metrics::FailReason::Deserialize,
                            );
                            fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
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
                            .await?;
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
                        headers: Arc::new(user_headers),
                    };

                    // Block here once `prefetch` handlers are in-flight; the
                    // permit is dropped when the spawned task finishes.
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            return Err(ShoveError::Connection(
                                "concurrent consumer semaphore closed".to_string(),
                            ));
                        }
                    };

                    processing.store(true, std::sync::atomic::Ordering::Release);

                    let task_handler = Arc::clone(&handler);
                    let task_ctx = Arc::clone(&ctx);
                    // Clone the hoisted outcome connection — cheap, shares the
                    // multiplexer/socket from the parent task.
                    let mut task_conn = outcome_conn.clone();
                    let task_topic = Arc::clone(&topic_arc);
                    let task_group_metric = group_arc.clone();
                    let task_group = group.clone();
                    let task_stream = stream.to_owned();
                    let task_processing = Arc::clone(&processing);
                    let task_semaphore = Arc::clone(&semaphore);

                    fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                    tokio::spawn(async move {
                        let _inflight =
                            metrics::InflightGuard::new(task_topic.clone(), task_group_metric.clone());
                        let start = std::time::Instant::now();

                        let outcome_opt = match handler_timeout {
                            Some(timeout_dur) => {
                                match tokio::time::timeout(
                                    timeout_dur,
                                    task_handler.handle(msg, meta, &task_ctx),
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
                                            &task_topic,
                                            task_group_metric.as_deref(),
                                            metrics::FailReason::Timeout,
                                        );
                                        None
                                    }
                                }
                            }
                            None => Some(task_handler.handle(msg, meta, &task_ctx).await),
                        };

                        let elapsed = start.elapsed().as_secs_f64();

                        if let Some(outcome) = outcome_opt {
                            metrics::record_consumed(
                                &task_topic,
                                task_group_metric.as_deref(),
                                &outcome,
                            );
                            metrics::record_processing_duration(
                                &task_topic,
                                task_group_metric.as_deref(),
                                &outcome,
                                elapsed,
                            );

                            // `task_conn` was cloned from the parent's hoisted
                            // outcome connection — no per-message socket churn.
                            if let Err(e) = route_outcome(
                                &mut task_conn,
                                topology,
                                &task_stream,
                                &task_group,
                                &entry_id,
                                &fields,
                                outcome,
                                retry_count,
                                max_retries,
                                hold_queues,
                            )
                            .await
                            {
                                tracing::warn!(
                                    error = %e,
                                    entry_id,
                                    "outcome routing failed; message left in PEL"
                                );
                            }
                        }

                        // Release the prefetch permit only AFTER outcome routing
                        // (XACK / hold-enqueue / DLQ) has landed, so the shutdown
                        // drain (`acquire_many(prefetch)`) waits for in-flight
                        // routing to complete — mirroring the NATS consumer.
                        // Releasing before routing let the drain return while a
                        // detached task still owed an XACK/hold/DLQ write, which
                        // could then be lost if the process exited.
                        drop(permit);
                        if task_semaphore.available_permits() == prefetch {
                            task_processing
                                .store(false, std::sync::atomic::Ordering::Release);
                        }
                    });
                }

                // Periodic XAUTOCLAIM removed — handled by the group-wide
                // reaper sidecar in `reaper.rs`.
            }
        }
    })
    .await
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
    hold_queues: &[HoldQueue],
) -> Result<()> {
    match outcome {
        Outcome::Ack => {
            if let Err(e) = xack(conn, stream, group, entry_id).await {
                tracing::warn!(stream, entry_id, error = %e, "XACK failed on Ack");
                metrics::record_backend_error(
                    metrics::BackendLabel::Redis,
                    metrics::BackendErrorKind::Ack,
                );
            }
        }
        Outcome::Retry => {
            let new_retry = retry_count + 1;
            // DLQ when the incoming retry count has reached the budget, so
            // `max_retries = N` permits 1 initial attempt + N retries. Matches
            // the documented contract and the other backends.
            if retry_count >= max_retries {
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
                .await?;
            } else if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Retry but no hold queues — re-queueing immediately"
                );
                requeue_to_stream(conn, stream, fields, new_retry).await;
                if let Err(e) = xack(conn, stream, group, entry_id).await {
                    tracing::warn!(stream, entry_id, error = %e, "XACK failed after immediate requeue");
                    metrics::record_backend_error(
                        metrics::BackendLabel::Redis,
                        metrics::BackendErrorKind::Ack,
                    );
                }
            } else if let Some(level) = hold_level(retry_count, hold_queues) {
                // Select the backoff tier from the *incoming* retry count
                // (retry 0 -> tier 0), matching the documented contract in
                // `topology.rs` and every other backend. `new_retry` is still
                // what gets written into the held entry's retry-count header.
                let hq = &hold_queues[level];
                route_to_hold(
                    conn,
                    stream,
                    group,
                    entry_id,
                    fields,
                    hq.name(),
                    hq.delay(),
                    new_retry,
                )
                .await;
            }
        }
        Outcome::Reject => {
            route_to_dlq(
                conn,
                topology,
                stream,
                group,
                entry_id,
                fields,
                "rejected",
                retry_count,
            )
            .await?;
        }
        Outcome::Defer => {
            if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Defer but no hold queues — re-queueing immediately"
                );
                requeue_to_stream(conn, stream, fields, retry_count).await;
                if let Err(e) = xack(conn, stream, group, entry_id).await {
                    tracing::warn!(stream, entry_id, error = %e, "XACK failed after defer requeue");
                    metrics::record_backend_error(
                        metrics::BackendLabel::Redis,
                        metrics::BackendErrorKind::Ack,
                    );
                }
            } else {
                let hq = &hold_queues[0];
                // Defer does NOT increment retry count.
                route_to_hold(
                    conn,
                    stream,
                    group,
                    entry_id,
                    fields,
                    hq.name(),
                    hq.delay(),
                    retry_count,
                )
                .await;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
    if let Err(e) = xack(conn, stream, group, entry_id).await {
        tracing::warn!(stream, entry_id, error = %e, "XACK failed after enqueue_hold");
        metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
    }
}

#[allow(clippy::too_many_arguments)]
async fn route_to_dlq(
    conn: &mut RedisConnection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    fields: &HashMap<String, String>,
    reason: &str,
    death_count: u32,
) -> Result<()> {
    let dlq = match topology.dlq() {
        Some(d) => d,
        None => {
            tracing::warn!(stream, entry_id, reason, "no DLQ configured — discarding");
            if let Err(e) = xack(conn, stream, group, entry_id).await {
                tracing::warn!(stream, entry_id, error = %e, "XACK failed while discarding (no DLQ)");
                metrics::record_backend_error(
                    metrics::BackendLabel::Redis,
                    metrics::BackendErrorKind::Ack,
                );
            }
            return Ok(());
        }
    };

    // Pre-size: "XADD", dlq, "*", all field pairs, 3 extra k/v pairs (reason, count, original).
    let arg_count = fields.len() * 2 + 9;
    let mut cmd = redis::Cmd::with_capacity(arg_count, arg_count * 16);
    cmd.arg("XADD").arg(dlq).arg("*");
    for (k, v) in fields {
        cmd.arg(k.as_str()).arg(v.as_str());
    }
    cmd.arg(X_DEATH_REASON).arg(reason);
    cmd.arg(X_DEATH_COUNT).arg(death_count.to_string());
    cmd.arg(X_ORIGINAL_QUEUE).arg(stream);

    conn.query::<redis::Value>(&mut cmd).await.map_err(|e| {
        tracing::warn!(error = %e, dlq, "XADD to DLQ failed — message stays in PEL");
        ShoveError::Connection(format!("XADD to DLQ failed: {e}"))
    })?;

    if let Err(e) = xack(conn, stream, group, entry_id).await {
        tracing::warn!(stream, entry_id, error = %e, "XACK failed after DLQ enqueue");
        metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
    }
    Ok(())
}

async fn requeue_to_stream(
    conn: &mut RedisConnection,
    stream: &str,
    fields: &HashMap<String, String>,
    retry_count: u32,
) {
    // Pre-size: "XADD", stream, "*", all field pairs (one key filtered at runtime), 1 extra k/v pair.
    let arg_count = fields.len() * 2 + 4;
    let mut cmd = redis::Cmd::with_capacity(arg_count, arg_count * 16);
    cmd.arg("XADD").arg(stream).arg("*");
    for (k, v) in fields {
        if k.as_str() != X_RETRY_COUNT {
            cmd.arg(k.as_str()).arg(v.as_str());
        }
    }
    cmd.arg(X_RETRY_COUNT).arg(retry_count.to_string());
    if let Err(e) = conn.query::<redis::Value>(&mut cmd).await {
        tracing::warn!(error = %e, stream, "XADD on immediate requeue failed — message may be lost");
    }
}

async fn xack(conn: &mut RedisConnection, stream: &str, group: &str, entry_id: &str) -> Result<()> {
    conn.query::<i64>(redis::cmd("XACK").arg(stream).arg(group).arg(entry_id))
        .await
        .map(|_| ())
        .map_err(|e| ShoveError::Connection(format!("XACK failed: {e}")))
}

// `autoclaim_all` moved to `reaper.rs` — see module docs there for why.

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
    capacity_hint: usize,
) -> Vec<(String, Vec<(String, String)>)> {
    let streams = match value {
        redis::Value::Nil => return Vec::new(),
        redis::Value::Array(arr) => arr,
        _ => return Vec::new(),
    };

    let mut result = Vec::with_capacity(capacity_hint);

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
                    Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
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

/// Shove-internal field names that are exposed via dedicated `MessageMetadata`
/// fields and must be excluded from the user-visible `headers` map.
const INTERNAL_KEYS: &[&str] = &[
    PAYLOAD_FIELD,
    X_RETRY_COUNT,
    X_SEQUENCE_KEY,
    X_MESSAGE_ID,
    X_DEATH_REASON,
    X_DEATH_COUNT,
    X_ORIGINAL_QUEUE,
];

/// Partition raw XREADGROUP entry fields into `(internal_fields, user_headers)`
/// in a single pass, consuming `fields_vec` without cloning any values.
///
/// `internal_fields` contains the shove-internal keys (routing, metadata);
/// `user_headers` contains everything else and is moved directly into
/// [`MessageMetadata::headers`].
fn partition_entry_fields(
    fields_vec: Vec<(String, String)>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut internal = HashMap::with_capacity(INTERNAL_KEYS.len());
    let mut user = HashMap::new();
    for (k, v) in fields_vec {
        if INTERNAL_KEYS.contains(&k.as_str()) {
            internal.insert(k, v);
        } else {
            user.insert(k, v);
        }
    }
    (internal, user)
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
        Some(hold_index(retry_count, hold_queues.len()))
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
        let result = parse_xreadgroup_reply(redis::Value::Nil, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_empty_array_returns_empty() {
        let result = parse_xreadgroup_reply(redis::Value::Array(vec![]), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_valid_entry() {
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

        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "1234-0");
        assert_eq!(result[0].1.len(), 2);
        assert_eq!(result[0].1[0], ("payload".to_string(), "{}".to_string()));
        assert_eq!(
            result[0].1[1],
            ("x-retry-count".to_string(), "0".to_string())
        );
    }

    #[test]
    fn parse_xreadgroup_simple_string_id() {
        // Some Redis versions return SimpleString for the entry ID.
        let entry = redis::Value::Array(vec![
            redis::Value::SimpleString("9999-1".to_string()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::BulkString(b"hello".to_vec()),
            ]),
        ]);
        let stream = redis::Value::Array(vec![
            redis::Value::BulkString(b"s".to_vec()),
            redis::Value::Array(vec![entry]),
        ]);
        let result = parse_xreadgroup_reply(redis::Value::Array(vec![stream]), 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "9999-1");
    }

    #[test]
    fn parse_xreadgroup_nil_field_value_becomes_empty_string() {
        // Redis may return Nil for a field value in some edge cases.
        let entry = redis::Value::Array(vec![
            redis::Value::BulkString(b"1-0".to_vec()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::Nil,
            ]),
        ]);
        let stream = redis::Value::Array(vec![
            redis::Value::BulkString(b"s".to_vec()),
            redis::Value::Array(vec![entry]),
        ]);
        let result = parse_xreadgroup_reply(redis::Value::Array(vec![stream]), 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1[0], ("payload".to_string(), String::new()));
    }

    #[test]
    fn parse_xreadgroup_odd_field_count_stops_at_last_key() {
        // Odd number of field values — the trailing key is dropped (no value follows).
        let entry = redis::Value::Array(vec![
            redis::Value::BulkString(b"2-0".to_vec()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::BulkString(b"{}".to_vec()),
                redis::Value::BulkString(b"dangling-key".to_vec()),
                // no value — loop breaks on None
            ]),
        ]);
        let stream = redis::Value::Array(vec![
            redis::Value::BulkString(b"s".to_vec()),
            redis::Value::Array(vec![entry]),
        ]);
        let result = parse_xreadgroup_reply(redis::Value::Array(vec![stream]), 0);
        assert_eq!(result.len(), 1);
        // Only the complete pair should be present.
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, "payload");
    }

    #[test]
    fn parse_xreadgroup_wrong_root_type_returns_empty() {
        let result = parse_xreadgroup_reply(redis::Value::Int(0), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn partition_entry_fields_separates_user_headers() {
        let fields_vec = vec![
            (PAYLOAD_FIELD.to_string(), "data".to_string()),
            (X_RETRY_COUNT.to_string(), "2".to_string()),
            (X_SEQUENCE_KEY.to_string(), "acct-1".to_string()),
            ("x-custom".to_string(), "val".to_string()),
        ];
        let (internal, user) = partition_entry_fields(fields_vec);
        assert_eq!(user.len(), 1);
        assert_eq!(user.get("x-custom").map(String::as_str), Some("val"));
        assert!(internal.contains_key(PAYLOAD_FIELD));
        assert!(internal.contains_key(X_RETRY_COUNT));
        assert!(internal.contains_key(X_SEQUENCE_KEY));
    }

    #[test]
    fn partition_entry_fields_all_internal_keys_go_to_internal() {
        let fields_vec = vec![
            (PAYLOAD_FIELD.to_string(), "data".to_string()),
            (X_RETRY_COUNT.to_string(), "2".to_string()),
            (X_SEQUENCE_KEY.to_string(), "acct-1".to_string()),
            (X_MESSAGE_ID.to_string(), "msg-abc".to_string()),
            (X_DEATH_REASON.to_string(), "max-retries".to_string()),
            (X_DEATH_COUNT.to_string(), "5".to_string()),
            (X_ORIGINAL_QUEUE.to_string(), "orders".to_string()),
            ("x-custom".to_string(), "val".to_string()),
        ];
        let (internal, user) = partition_entry_fields(fields_vec);
        // Only x-custom must appear in user headers.
        assert_eq!(user.len(), 1);
        assert_eq!(user.get("x-custom").map(String::as_str), Some("val"));
        // All internal keys must be in the internal map, not the user map.
        for key in INTERNAL_KEYS {
            assert!(
                !user.contains_key(*key),
                "internal key {key:?} leaked into user headers"
            );
            assert!(
                internal.contains_key(*key),
                "internal key {key:?} missing from internal map"
            );
        }
    }

    #[test]
    fn partition_entry_fields_empty_input_returns_empty_maps() {
        let (internal, user) = partition_entry_fields(vec![]);
        assert!(internal.is_empty());
        assert!(user.is_empty());
    }

    #[test]
    fn consumer_name_is_unique() {
        let a = RedisConsumer::consumer_name();
        let b = RedisConsumer::consumer_name();
        assert_ne!(a, b, "consumer names must be unique per call");
    }

    // --- Additional branch coverage for parse_xreadgroup_reply ---

    #[test]
    fn parse_xreadgroup_non_array_stream_item_skipped() {
        // A non-array element at the stream level is skipped via `_ => continue`.
        let reply = redis::Value::Array(vec![
            redis::Value::Int(42), // not an array — should be skipped
        ]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_stream_pair_too_short_skipped() {
        // An array with len < 2 at the stream level is skipped.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::BulkString(
            b"only-one".to_vec(),
        )])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_non_array_entry_list_skipped() {
        // stream_pair[1] is not an array — the whole stream is skipped.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Int(99), // entries list is not an array
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_entry_pair_too_short_skipped() {
        // An entry array with len < 2 is skipped.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![
                // entry with only one element
                redis::Value::Array(vec![redis::Value::BulkString(b"1-0".to_vec())]),
            ]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_int_entry_id_skipped() {
        // Entry ID is an Int — entry is skipped via `_ => continue`.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::Int(12345), // not a valid ID type
                redis::Value::Array(vec![]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_non_array_field_list_skipped() {
        // entry_pair[1] is not an array — entry is skipped via `_ => continue`.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Int(0), // field list is not an array
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_xreadgroup_simple_string_field_key() {
        // Field key is a SimpleString — should be accepted.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::SimpleString("myfieldkey".to_string()),
                    redis::Value::BulkString(b"myvalue".to_vec()),
                ]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, "myfieldkey");
        assert_eq!(result[0].1[0].1, "myvalue");
    }

    #[test]
    fn parse_xreadgroup_int_field_key_breaks_loop() {
        // An Int field key triggers the `Some(_) => break` branch.
        // Fields collected before the int key are kept; the int terminates the loop.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"good-key".to_vec()),
                    redis::Value::BulkString(b"good-val".to_vec()),
                    redis::Value::Int(42), // triggers break
                    redis::Value::BulkString(b"after-break".to_vec()),
                ]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        // The entry IS emitted (the break only ends field collection, not the entry).
        assert_eq!(result.len(), 1);
        // Only the pair before the Int key should be present.
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, "good-key");
    }

    #[test]
    fn parse_xreadgroup_simple_string_field_value() {
        // Field value is a SimpleString — should be accepted.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"key".to_vec()),
                    redis::Value::SimpleString("simplevalue".to_string()),
                ]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1[0].1, "simplevalue");
    }

    #[test]
    fn parse_xreadgroup_int_field_value_breaks_loop() {
        // An Int field value triggers the `Some(_) => break` branch.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"k1".to_vec()),
                    redis::Value::BulkString(b"v1".to_vec()),
                    redis::Value::BulkString(b"k2".to_vec()),
                    redis::Value::Int(99), // Int value triggers break
                ]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 1);
        // Only the pair before the Int value should be present.
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, "k1");
    }

    #[test]
    fn parse_xreadgroup_multiple_streams_merged_flat() {
        // Multiple streams in one reply produce a flat list of entries.
        fn make_stream(name: &str, id: &str, val: &str) -> redis::Value {
            redis::Value::Array(vec![
                redis::Value::BulkString(name.as_bytes().to_vec()),
                redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::BulkString(id.as_bytes().to_vec()),
                    redis::Value::Array(vec![
                        redis::Value::BulkString(b"payload".to_vec()),
                        redis::Value::BulkString(val.as_bytes().to_vec()),
                    ]),
                ])]),
            ])
        }
        let reply = redis::Value::Array(vec![
            make_stream("stream-a", "1-0", "msg-a"),
            make_stream("stream-b", "2-0", "msg-b"),
            make_stream("stream-c", "3-0", "msg-c"),
        ]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "1-0");
        assert_eq!(result[1].0, "2-0");
        assert_eq!(result[2].0, "3-0");
    }

    #[test]
    fn hold_level_single_element_always_returns_zero() {
        let single = vec!["only-queue"];
        // Any retry count on a single-element slice must return Some(0).
        assert_eq!(hold_level(0, &single), Some(0));
        assert_eq!(hold_level(1, &single), Some(0));
        assert_eq!(hold_level(100, &single), Some(0));
        assert_eq!(hold_level(u32::MAX, &single), Some(0));
    }

    #[test]
    fn nogroup_error_string_is_detected() {
        // Verify the substring we check for matches what Redis actually returns.
        // Redis error format: "NOGROUP No such consumer group 'grp' for key name 'stream'"
        let err_str = "NOGROUP No such consumer group 'grp' for key name 'stream'";
        assert!(err_str.contains("NOGROUP"));
        // The ShoveError wrapping must preserve the NOGROUP text for the check to work.
        let err = ShoveError::Connection(err_str.to_string());
        assert!(err.to_string().contains("NOGROUP"));
    }

    #[test]
    fn nogroup_error_is_retryable() {
        // NOGROUP is wrapped as Connection so run_with_reconnect retries after Redis
        // restart with data loss, giving the application time to re-declare topology.
        let err = ShoveError::Connection(
            "consumer group does not exist on stream 'foo': NOGROUP ...".into(),
        );
        assert!(
            err.is_retryable(),
            "NOGROUP error must be retryable so consumers survive Redis restart"
        );
    }

    #[test]
    fn nogroup_error_is_connection_not_topology() {
        // Verifies the variant: NOGROUP must NOT be ShoveError::Topology (non-retryable).
        let err = ShoveError::Connection(
            "consumer group does not exist on stream 'foo': NOGROUP ...".into(),
        );
        assert!(
            matches!(err, ShoveError::Connection(_)),
            "NOGROUP must be ShoveError::Connection, not Topology"
        );
        assert!(
            !matches!(err, ShoveError::Topology(_)),
            "NOGROUP must not be ShoveError::Topology"
        );
    }

    // -----------------------------------------------------------------------
    // run_with_reconnect — max_reconnect_attempts exhaustion
    // -----------------------------------------------------------------------

    #[test]
    fn exhausted_reconnect_error_message_format() {
        let stream = "orders";
        let max: u32 = 3;
        let cause = "connection refused";
        let msg = format!("consumer on '{stream}' exhausted {max} reconnect attempt(s): {cause}");
        assert!(msg.contains(stream), "stream name must appear in error");
        assert!(
            msg.contains(&max.to_string()),
            "attempt count must appear in error"
        );
        assert!(msg.contains(cause), "root cause must appear in error");
    }

    #[tokio::test]
    async fn run_with_reconnect_stops_when_limit_reached() {
        use tokio_util::sync::CancellationToken;
        let shutdown = CancellationToken::new();
        let mut calls = 0u32;
        let result = run_with_reconnect(&shutdown, "test-stream", Some(2), || {
            calls += 1;
            async { Err(ShoveError::Connection("transient".into())) }
        })
        .await;
        assert!(
            result.is_err(),
            "must propagate error after exhausting attempts"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("test-stream"),
            "error must name the stream; got: {msg}"
        );
        assert_eq!(calls, 2, "must attempt exactly max times before giving up");
    }

    #[tokio::test]
    async fn run_with_reconnect_unlimited_can_succeed_after_retries() {
        use tokio_util::sync::CancellationToken;
        let shutdown = CancellationToken::new();
        let mut calls = 0u32;
        let result = run_with_reconnect(&shutdown, "test-stream", None, || {
            calls += 1;
            async move {
                if calls < 3 {
                    Err(ShoveError::Connection("transient".into()))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(result.is_ok(), "must succeed once the closure returns Ok");
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn run_with_reconnect_non_retryable_error_propagates_immediately() {
        use tokio_util::sync::CancellationToken;
        let shutdown = CancellationToken::new();
        let mut calls = 0u32;
        let result = run_with_reconnect(&shutdown, "test-stream", None, || {
            calls += 1;
            async { Err(ShoveError::Topology("bad topology".into())) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 1, "non-retryable error must not trigger reconnect");
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_reconnect_shutdown_during_sleep_returns_ok() {
        // After a retryable error the function backs off in a select! that races
        // tokio::time::sleep against shutdown.cancelled(). With paused time the
        // sleep never elapses, so the cancellation arm is the only way the select
        // can return — proving the cancellation-during-sleep branch is taken
        // without depending on real wall-clock timing.
        use std::sync::atomic::{AtomicU32, Ordering};
        use tokio_util::sync::CancellationToken;

        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let canceller = shutdown.clone();
        let calls_clone = Arc::clone(&calls);

        // Cancel after yielding so run_with_reconnect has already:
        //   1. invoked the closure (calls -> 1),
        //   2. passed the is_cancelled() check after the error, and
        //   3. entered the select!. With time paused, the sleep arm cannot
        //      complete, so the cancellation arm must be what returns Ok.
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            canceller.cancel();
        });

        let result = run_with_reconnect(&shutdown, "test-stream", None, || {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            async { Err(ShoveError::Connection("transient".into())) }
        })
        .await;

        assert!(
            result.is_ok(),
            "shutdown during backoff sleep must short-circuit to Ok"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "closure must not be re-invoked after cancellation"
        );
    }

    #[tokio::test]
    async fn run_with_reconnect_shutdown_between_error_and_sleep_returns_ok() {
        // The `if shutdown.is_cancelled() { return Ok(()); }` check sits between the
        // is_retryable check and the backoff sleep. Cancel the token before the closure
        // even runs so that the first error returns immediately via that branch.
        use tokio_util::sync::CancellationToken;

        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let mut calls = 0u32;
        let result = run_with_reconnect(&shutdown, "test-stream", None, || {
            calls += 1;
            async { Err(ShoveError::Connection("transient".into())) }
        })
        .await;

        assert!(
            result.is_ok(),
            "cancellation observed after a retryable error must yield Ok"
        );
        assert_eq!(
            calls, 1,
            "closure runs exactly once before the cancellation check"
        );
    }

    // --- parse_xreadgroup_reply: non-UTF-8 and multi-entry branches ---

    #[test]
    fn parse_xreadgroup_non_utf8_entry_id_skipped() {
        // BulkString entry ID with invalid UTF-8 hits `Err(_) => continue` and skips
        // the entry. The surrounding stream/reply structure stays well-formed so we
        // can prove the skip is per-entry, not a structural failure.
        let bad_id_entry = redis::Value::Array(vec![
            redis::Value::BulkString(vec![0xff, 0xfe, 0xfd]), // invalid UTF-8
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::BulkString(b"x".to_vec()),
            ]),
        ]);
        let good_entry = redis::Value::Array(vec![
            redis::Value::BulkString(b"2-0".to_vec()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"payload".to_vec()),
                redis::Value::BulkString(b"y".to_vec()),
            ]),
        ]);
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![bad_id_entry, good_entry]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(
            result.len(),
            1,
            "non-UTF-8 entry ID must be skipped, leaving only the good entry"
        );
        assert_eq!(result[0].0, "2-0");
    }

    #[test]
    fn parse_xreadgroup_non_utf8_field_key_breaks_loop() {
        // A BulkString field key with invalid UTF-8 hits `Err(_) => break` in the key
        // arm, terminating field collection but still emitting the entry with the
        // fields gathered so far.
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"1-0".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"good-key".to_vec()),
                    redis::Value::BulkString(b"good-val".to_vec()),
                    redis::Value::BulkString(vec![0xff, 0xfe]), // invalid UTF-8 key
                    redis::Value::BulkString(b"never-reached".to_vec()),
                ]),
            ])]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].1.len(),
            1,
            "only the pair before the bad key survives"
        );
        assert_eq!(result[0].1[0].0, "good-key");
    }

    #[test]
    fn parse_xreadgroup_multiple_entries_within_single_stream() {
        // prefetch_count > 1 produces multiple entries under one stream key. The
        // parser must emit them in order in the flat result list.
        fn entry(id: &str, val: &str) -> redis::Value {
            redis::Value::Array(vec![
                redis::Value::BulkString(id.as_bytes().to_vec()),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(val.as_bytes().to_vec()),
                ]),
            ])
        }
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"mystream".to_vec()),
            redis::Value::Array(vec![
                entry("1-0", "a"),
                entry("2-0", "b"),
                entry("3-0", "c"),
            ]),
        ])]);
        let result = parse_xreadgroup_reply(reply, 0);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "1-0");
        assert_eq!(result[1].0, "2-0");
        assert_eq!(result[2].0, "3-0");
        assert_eq!(result[0].1[0].1, "a");
        assert_eq!(result[1].1[0].1, "b");
        assert_eq!(result[2].1[0].1, "c");
    }
}
