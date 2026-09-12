//! Redis Streams consumer — XREADGROUP loop with outcome routing, hold-queue
//! scheduling via ZADD, DLQ routing via XADD, and XAUTOCLAIM crash recovery.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::ConsumerOptions;
use crate::backend::BatchConsumerOptionsInner;
use crate::backend::ConsumerOptionsInner;
use crate::backend::batch_consumer::settling::{
    PREALLOC_CAP, batch_redelivery_backoff, invoke_batch_handler, next_redelivery_delay,
};
use crate::backend::batch_consumer::{BatchSettlement, settle_batch_outcome};
use crate::backend::consumer::ConsumerImpl;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::markers::Redis;
use crate::metadata::MessageMetadata;
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{PoisonedKeys, RetryDecision, decide_retry, hold_index};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::{HoldQueue, QueueTopology};

use super::client::{RedisClient, RedisConnection};
use super::constants::{
    BLOCK_MS, PAYLOAD_FIELD, X_DEATH_COUNT, X_DEATH_REASON, X_MESSAGE_ID, X_ORIGINAL_QUEUE,
    X_RETRY_COUNT, X_SEQUENCE_KEY,
};
use super::lease;
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

    /// The underlying client, for the sibling broadcast module — which issues a
    /// bare `XREAD` on its own connection rather than going through any of the
    /// group-aware helpers here.
    pub(super) fn client_ref(&self) -> &RedisClient {
        &self.client
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
    /// Honours [`ConsumerOptions::with_concurrent_processing`]: with it on,
    /// each XREADGROUP-returned entry is dispatched to its own task with
    /// in-flight handlers capped at `prefetch_count` (the semaphore-gated
    /// path a [`RedisConsumerGroupRegistry`] member takes); with it off,
    /// `prefetch_count` reaches this backend as `1` and entries are handled
    /// one at a time.
    ///
    /// [`ConsumerOptions::with_concurrent_processing`]: crate::consumer::ConsumerOptions::with_concurrent_processing
    /// [`RedisConsumerGroupRegistry`]: super::consumer_group::RedisConsumerGroupRegistry
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

    /// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl)'s
    /// delegate. No `validate_batch_topic` call here — mirrors InMemory's
    /// `run_batch_with_inner`: this has exactly one caller,
    /// `BatchConsumerImpl::run_batch`, itself only reachable through the
    /// generic `BatchConsumer::run` wrapper, which already ran the guard.
    /// Redis has no separate public inherent `run_batch` bypassing that
    /// wrapper the way Kafka's does, so a second check here would only ever
    /// repeat the first.
    pub(super) fn run_batch_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        run_batch_impl::<T, H>(self.client.clone(), handler, ctx, options)
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
            // `ConsumerOptions::into_inner` pins `prefetch_count` to 1 when
            // `concurrent_processing` is off, so a prefetch above 1 is the
            // caller asking for concurrent dispatch: the same rule the group
            // registry applies when it picks a member's loop. Until this
            // dispatch existed a direct consumer ignored the flag and the
            // benchmark's consume_parallel flow sat 10 to 30x below
            // consumer_group on this backend.
            if options.prefetch_count > 1 {
                return RedisConsumer::new(client)
                    .run_concurrent::<T, H>(handler, ctx, options)
                    .await;
            }
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
            let on_failure = seq.on_failure();
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

                    // One poison set per shard task. A sequence key always
                    // hashes to the same shard, so per-shard tracking sees
                    // every message the key will ever produce on this consumer.
                    let result = run_stream_loop_arc::<T, H>(
                        client,
                        handler,
                        ctx,
                        options,
                        topology,
                        &stream_name,
                        &shard_hold_queues,
                        Maintain::Stream,
                        PoisonedKeys::new(on_failure),
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
pub(super) async fn run_with_reconnect<F, Fut>(
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
        // Unsequenced path — no sequence keys, so an inert tracker.
        PoisonedKeys::default(),
    )
    .await;

    if let Some(h) = requeue_handle {
        h.abort();
    }
    result
}

/// Record a `FailAll` poisoning, logging only the first transition per key.
/// A no-op under `SequenceFailure::Skip` and for unkeyed messages.
fn poison_key(poisoned: &PoisonedKeys, key: &str, stream: &str) {
    if poisoned.poison(key) {
        tracing::info!(
            stream,
            sequence_key = %key,
            "poisoning sequence key (FailAll)"
        );
    }
}

/// Core consumer loop that takes `Arc<H>` and `Arc<H::Context>` so it can be
/// shared across shard tasks without requiring `H: Clone`.
///
/// `poisoned` carries `SequenceFailure::FailAll` state on the sequenced path
/// and is inert everywhere else. It is passed in (rather than created here)
/// because it must outlive the reconnect loop below.
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
    poisoned: PoisonedKeys,
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
    let _maintenance = (maintain == Maintain::Stream).then(|| {
        super::maintenance::acquire(
            &client,
            stream,
            options.handler_timeout,
            options.handler_timeout_outcome.is_some(),
        )
    });

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
        // Shares the set with the outer tracker: a reconnect must not
        // un-poison keys that already failed.
        let poisoned = poisoned.clone();

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
                    // Shared with `MessageMetadata::headers` rather than moved
                    // into it: every write-back below must republish the user's
                    // headers alongside the internal fields.
                    let user_headers = Arc::new(user_headers);

                    // Built before the pre-handler checks, not just around the
                    // handler: every write below has to prove we still own the
                    // entry first. With `prefetch > 1` the whole batch entered
                    // our PEL on one XREADGROUP but is inspected serially, so a
                    // late entry can already have been idle long enough for a
                    // foreign reaper to reclaim and re-add it by the time we
                    // look at it — and these paths dead-letter or ack without
                    // any handler running, so nothing else would catch that.
                    let pre_lease = lease::Lease {
                        stream,
                        group: &group,
                        consumer: &consumer,
                        entry_id: &entry_id,
                    };
                    let leased = options.handler_timeout_outcome.is_some();

                    // Extract payload — take ownership to avoid cloning on the hot path.
                    let payload_raw = match fields.remove(PAYLOAD_FIELD) {
                        Some(s) => s,
                        None => {
                            if !may_act_on_entry(&mut conn, &pre_lease, leased, &"missing-payload")
                                .await
                            {
                                continue;
                            }
                            tracing::warn!(entry_id, "missing payload field — acking and skipping");
                            // Counted only once the XACK lands, and only when
                            // it is *this* call that retired the entry. A
                            // failed XACK leaves the entry in the PEL for a
                            // reclaim to redeliver, and `Ok(false)` means a
                            // reaper already retired it and a live copy
                            // exists; this arm runs again in both cases, so
                            // counting here too would double-count one entry.
                            match xack(&mut conn, stream, &group, &entry_id).await {
                                Ok(true) => metrics::record_failed(
                                    topic_name,
                                    consumer_group,
                                    metrics::FailReason::Malformed,
                                ),
                                Ok(false) => {
                                    tracing::debug!(entry_id, "corrupt entry was already retired by a reaper — not counting");
                                }
                                Err(e) => {
                                    tracing::warn!(entry_id, error = %e, "XACK failed after skipping corrupt entry");
                                    metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
                                }
                            }
                            continue;
                        }
                    };

                    // Recorded before every pre-handler drop below — the
                    // FailAll cascade, the size check, the decode — and not
                    // after. `shove_message_size_bytes` describes what arrived
                    // on the wire, so an oversize payload is precisely the
                    // sample an operator sizing `max_message_size` needs to
                    // see; sizing only what survives would hide it. Every
                    // other backend places the call the same way (RabbitMQ
                    // `try_deserialize_or_reject`, Kafka/NATS/SQS immediately
                    // after the payload is in hand, InMemory
                    // `prepare_message`), and a cross-backend split here would
                    // be worse than either choice.
                    metrics::record_message_size(topic_name, consumer_group, payload_raw.len());

                    let retry_count = fields
                        .get(X_RETRY_COUNT)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);

                    // ── FailAll: skip poisoned keys ──
                    // Inert unless this is a sequenced consumer configured
                    // `SequenceFailure::FailAll`.
                    let seq_key = fields.get(X_SEQUENCE_KEY).cloned().unwrap_or_default();
                    if poisoned.is_poisoned(&seq_key) {
                        tracing::warn!(
                            stream,
                            entry_id,
                            sequence_key = %seq_key,
                            "sequence key poisoned (FailAll) — sending to DLQ without invoking handler"
                        );
                        // Collateral of an already-counted failure, so the
                        // failure half is deliberately not counted again — see
                        // `metrics::FailReason`. The discard half still
                        // applies: a cascaded message dropped with no DLQ is
                        // just as gone as any other.
                        let pending = metrics::pending_discard(
                            topic_name,
                            consumer_group,
                            metrics::FailReason::Rejected,
                            topology.dlq().is_some(),
                        );
                        fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                        let retired = match route_to_dlq(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &entry_id,
                            &fields,
                            &user_headers,
                            "rejected",
                            retry_count,
                        )
                        .await
                        {
                            Ok(retired) => retired,
                            Err(e) => {
                                // XADD to the DLQ failed; the entry stays in
                                // the PEL for the reaper to redeliver, so
                                // nothing was discarded.
                                pending.survived();
                                return Err(e);
                            }
                        };
                        // `route_to_dlq` reports whether the XACK actually
                        // acknowledged the entry — a lost lease means someone
                        // else owns it and it is not retired here.
                        if retired {
                            pending.confirm();
                        } else {
                            pending.survived();
                        }
                        continue;
                    }

                    // Size check.
                    if let Some(max) = options.max_message_size
                        && payload_raw.len() > max
                    {
                        // Skip the whole entry, not just the DLQ write: a
                        // reclaimed entry belongs to the reaper, and falling
                        // through would hand an oversize payload to the
                        // handler.
                        if !may_act_on_entry(&mut conn, &pre_lease, leased, &"oversize").await {
                            continue;
                        }
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
                        poison_key(&poisoned, &seq_key, stream);
                        fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                        route_to_dlq(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &entry_id,
                            &fields,
                            &user_headers,
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
                            if !may_act_on_entry(&mut conn, &pre_lease, leased, &"deserialize")
                                .await
                            {
                                continue;
                            }
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
                            poison_key(&poisoned, &seq_key, stream);
                            fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                            route_to_dlq(
                                &mut conn,
                                topology,
                                stream,
                                &group,
                                &entry_id,
                                &fields,
                                &user_headers,
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
                        // Redis tracks deliveries in the group's PEL, but
                        // XREADGROUP does not return the counter — surfacing it
                        // would cost an XPENDING round-trip per message.
                        delivery_count: None,
                        headers: Arc::clone(&user_headers),
                    };

                    options
                        .processing
                        .store(true, std::sync::atomic::Ordering::Release);

                    let handler_clone = Arc::clone(&handler);
                    let ctx_clone = Arc::clone(&ctx);

                    let _inflight =
                        metrics::InflightGuard::new(topic_arc.clone(), group_arc.clone());
                    let start = std::time::Instant::now();

                    // Resolving a timeout to an outcome makes this consumer an
                    // actor at the deadline, racing any reaper — including one
                    // in another process, which `maintenance` cannot reconcile
                    // with — that sweeps at the same idle threshold. Hold the
                    // entry's lease while the handler runs so no reaper reaches
                    // that threshold, and re-check it before routing.
                    let lease = lease::Lease {
                        stream,
                        group: &group,
                        consumer: &consumer,
                        entry_id: &entry_id,
                    };
                    let leased = options.handler_timeout_outcome.is_some();

                    let outcome_opt = match options.handler_timeout {
                        Some(timeout_dur) => {
                            match lease::run_under_lease(
                                &mut conn,
                                leased.then_some(&lease),
                                timeout_dur,
                                handler_clone.handle(msg, meta, &ctx_clone),
                            )
                            .await
                            {
                                // A lease can be lost while the handler is
                                // still running, so normal completion is
                                // guarded exactly like a timeout: routing an
                                // outcome onto an entry a reaper already
                                // re-added is what produces the duplicate.
                                Ok(o) => {
                                    resolve_under_lease(&mut conn, &lease, leased, Some(o)).await
                                }
                                Err(_) => {
                                    // With no override, do NOT ack: XAUTOCLAIM
                                    // reclaims the entry after idle_ms, which
                                    // redelivers without touching retry_count.
                                    let resolved = options.handler_timeout_outcome.clone();
                                    match resolved.as_ref() {
                                        Some(o) => tracing::warn!(
                                            entry_id,
                                            timeout = ?timeout_dur,
                                            outcome = ?o,
                                            "handler timed out"
                                        ),
                                        None => tracing::warn!(
                                            entry_id,
                                            timeout = ?timeout_dur,
                                            "handler timed out — leaving in PEL for XAUTOCLAIM"
                                        ),
                                    }
                                    metrics::record_failed(
                                        &topic_arc,
                                        group_arc.as_deref(),
                                        metrics::FailReason::Timeout,
                                    );
                                    resolve_under_lease(&mut conn, &lease, leased, resolved).await
                                }
                            }
                        }
                        None => Some(
                            lease::catch_handler_panic(handler_clone.handle(
                                msg,
                                meta,
                                &ctx_clone,
                            ))
                            .await,
                        ),
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

                    // FailAll: a DLQ-terminal outcome poisons the key, so every
                    // later message for it is dead-lettered instead of handled.
                    if matches!(
                        decide_retry(&outcome, retry_count, options.max_retries),
                        RetryDecision::Dlq { .. }
                    ) {
                        poison_key(&poisoned, &seq_key, stream);
                    }

                    fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                    route_outcome(
                        &mut conn,
                        topology,
                        stream,
                        &group,
                        &entry_id,
                        &fields,
                        &user_headers,
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
    let _maintenance = super::maintenance::acquire(
        &client,
        stream,
        options.handler_timeout,
        options.handler_timeout_outcome.is_some(),
    );

    let topic_arc: Arc<str> = Arc::from(topic_name);
    let group_arc: Option<Arc<str>> = consumer_group.map(Arc::from);

    let prefetch = options.prefetch_count.max(1) as usize;

    let semaphore = Arc::new(Semaphore::new(prefetch));
    let max_retries = options.max_retries;
    let max_message_size = options.max_message_size;
    let handler_timeout = options.handler_timeout;
    let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
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
        let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();

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
                    // Shared with `MessageMetadata::headers` rather than moved
                    // into it: every write-back below must republish the user's
                    // headers alongside the internal fields.
                    let user_headers = Arc::new(user_headers);

                    // Built before the pre-handler checks, not just around the
                    // handler: every write below has to prove we still own the
                    // entry first. With `prefetch > 1` the whole batch entered
                    // our PEL on one XREADGROUP but is inspected serially, so a
                    // late entry can already have been idle long enough for a
                    // foreign reaper to reclaim and re-add it by the time we
                    // look at it — and these paths dead-letter or ack without
                    // any handler running, so nothing else would catch that.
                    let pre_lease = lease::Lease {
                        stream,
                        group: &group,
                        consumer: &consumer,
                        entry_id: &entry_id,
                    };
                    let leased = handler_timeout_outcome_cfg.is_some();

                    // Extract payload — take ownership to avoid cloning on the hot path.
                    let payload_raw = match fields.remove(PAYLOAD_FIELD) {
                        Some(s) => s,
                        None => {
                            if !may_act_on_entry(&mut conn, &pre_lease, leased, &"missing-payload")
                                .await
                            {
                                continue;
                            }
                            tracing::warn!(entry_id, "missing payload field — acking and skipping");
                            // Counted only once the XACK lands, and only when
                            // it is *this* call that retired the entry. A
                            // failed XACK leaves the entry in the PEL for a
                            // reclaim to redeliver, and `Ok(false)` means a
                            // reaper already retired it and a live copy
                            // exists; this arm runs again in both cases, so
                            // counting here too would double-count one entry.
                            match xack(&mut conn, stream, &group, &entry_id).await {
                                Ok(true) => metrics::record_failed(
                                    topic_name,
                                    consumer_group,
                                    metrics::FailReason::Malformed,
                                ),
                                Ok(false) => {
                                    tracing::debug!(entry_id, "corrupt entry was already retired by a reaper — not counting");
                                }
                                Err(e) => {
                                    tracing::warn!(entry_id, error = %e, "XACK failed after skipping corrupt entry");
                                    metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
                                }
                            }
                            continue;
                        }
                    };

                    // Same placement as the sequential loop above: before the
                    // size check, so an oversize payload still lands in the
                    // histogram.
                    metrics::record_message_size(topic_name, consumer_group, payload_raw.len());

                    let retry_count = fields
                        .get(X_RETRY_COUNT)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);

                    if let Some(max) = max_message_size
                        && payload_raw.len() > max
                    {
                        // Skip the whole entry, not just the DLQ write: a
                        // reclaimed entry belongs to the reaper, and falling
                        // through would hand an oversize payload to the
                        // handler.
                        if !may_act_on_entry(&mut conn, &pre_lease, leased, &"oversize").await {
                            continue;
                        }
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
                            &user_headers,
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
                            if !may_act_on_entry(&mut conn, &pre_lease, leased, &"deserialize")
                                .await
                            {
                                continue;
                            }
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
                                &user_headers,
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
                        // Redis tracks deliveries in the group's PEL, but
                        // XREADGROUP does not return the counter — surfacing it
                        // would cost an XPENDING round-trip per message.
                        delivery_count: None,
                        headers: Arc::clone(&user_headers),
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
                    let task_timeout_outcome = handler_timeout_outcome_cfg.clone();
                    // Every handler spawned in this reconnect cycle was read
                    // under the same XREADGROUP identity, so that is the PEL
                    // owner each task must renew its lease as.
                    let task_consumer = consumer.clone();

                    fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                    tokio::spawn(async move {
                        let _inflight =
                            metrics::InflightGuard::new(task_topic.clone(), task_group_metric.clone());
                        let start = std::time::Instant::now();

                        // See the non-concurrent path: an override makes this
                        // task an actor at the deadline, so it must hold the
                        // entry's lease against every reaper while it works.
                        let lease = lease::Lease {
                            stream: &task_stream,
                            group: &task_group,
                            consumer: &task_consumer,
                            entry_id: &entry_id,
                        };
                        let leased = task_timeout_outcome.is_some();

                        let outcome_opt = match handler_timeout {
                            Some(timeout_dur) => {
                                match lease::run_under_lease(
                                    &mut task_conn,
                                    leased.then_some(&lease),
                                    timeout_dur,
                                    task_handler.handle(msg, meta, &task_ctx),
                                )
                                .await
                                {
                                    // See the non-concurrent path: a lease lost
                                    // mid-handler makes normal completion just
                                    // as much of a race as a timeout.
                                    Ok(o) => {
                                        resolve_under_lease(
                                            &mut task_conn,
                                            &lease,
                                            leased,
                                            Some(o),
                                        )
                                        .await
                                    }
                                    Err(_) => {
                                        // See the non-concurrent path: `None`
                                        // leaves the entry in the PEL for
                                        // XAUTOCLAIM to reclaim.
                                        let resolved = task_timeout_outcome.clone();
                                        match resolved.as_ref() {
                                            Some(o) => tracing::warn!(
                                                entry_id,
                                                timeout = ?timeout_dur,
                                                outcome = ?o,
                                                "handler timed out"
                                            ),
                                            None => tracing::warn!(
                                                entry_id,
                                                timeout = ?timeout_dur,
                                                "handler timed out — leaving in PEL for XAUTOCLAIM"
                                            ),
                                        }
                                        metrics::record_failed(
                                            &task_topic,
                                            task_group_metric.as_deref(),
                                            metrics::FailReason::Timeout,
                                        );
                                        resolve_under_lease(
                                            &mut task_conn,
                                            &lease,
                                            leased,
                                            resolved,
                                        )
                                        .await
                                    }
                                }
                            }
                            None => Some(
                                lease::catch_handler_panic(task_handler.handle(
                                    msg,
                                    meta,
                                    &task_ctx,
                                ))
                                .await,
                            ),
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
                                &user_headers,
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
    user_headers: &HashMap<String, String>,
    outcome: Outcome,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
) -> Result<()> {
    match decide_retry(&outcome, retry_count, max_retries) {
        RetryDecision::Ack => {
            if let Err(e) = xack(conn, stream, group, entry_id).await {
                tracing::warn!(stream, entry_id, error = %e, "XACK failed on Ack");
                metrics::record_backend_error(
                    metrics::BackendLabel::Redis,
                    metrics::BackendErrorKind::Ack,
                );
            }
        }
        RetryDecision::Dlq { reason } => {
            let fail_reason = match reason {
                "rejected" => metrics::FailReason::Rejected,
                _ => metrics::FailReason::MaxRetriesExceeded,
            };
            let pending = metrics::record_terminal(
                topology.queue(),
                Some(group),
                fail_reason,
                topology.dlq().is_some(),
            );
            // Preserve the pre-refactor death counts: max-retries recorded
            // `retry_count + 1`, reject recorded `retry_count`.
            let death_count = if reason == "rejected" {
                retry_count
            } else {
                retry_count.saturating_add(1)
            };
            let retired = match route_to_dlq(
                conn,
                topology,
                stream,
                group,
                entry_id,
                fields,
                user_headers,
                reason,
                death_count,
            )
            .await
            {
                Ok(retired) => retired,
                Err(e) => {
                    // XADD to the DLQ failed; the entry stays in the PEL for
                    // the reaper to redeliver, so nothing was discarded.
                    pending.survived();
                    return Err(e);
                }
            };
            if retired {
                pending.confirm();
            } else {
                pending.survived();
            }
        }
        RetryDecision::Hold { increment: true } => {
            let new_retry = retry_count.saturating_add(1);
            if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Retry but no hold queues — re-queueing immediately"
                );
                // Only ack once the replacement copy exists — see
                // `requeue_to_stream`. On failure the entry stays in the PEL
                // and the reaper redelivers it.
                if requeue_to_stream(conn, stream, fields, user_headers, new_retry)
                    .await
                    .is_ok()
                    && let Err(e) = xack(conn, stream, group, entry_id).await
                {
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
                    user_headers,
                    hq.name(),
                    hq.delay(),
                    new_retry,
                )
                .await;
            }
        }
        RetryDecision::Hold { increment: false } => {
            if hold_queues.is_empty() {
                tracing::warn!(
                    stream,
                    entry_id,
                    "Defer but no hold queues — re-queueing immediately"
                );
                // Same ordering as the Retry arm above: the XADD is the copy,
                // so a failed re-add must not be followed by an ack.
                if requeue_to_stream(conn, stream, fields, user_headers, retry_count)
                    .await
                    .is_ok()
                    && let Err(e) = xack(conn, stream, group, entry_id).await
                {
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
                    user_headers,
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
    user_headers: &HashMap<String, String>,
    hold_name: &str,
    delay: Duration,
    new_retry_count: u32,
) {
    let mut hold_fields: Vec<(String, String)> =
        merged_entry_fields(fields, user_headers, Some(X_RETRY_COUNT))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
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
/// Dead-letter `entry_id`, or drop it when the topology declares no DLQ.
///
/// Returns whether the entry was actually retired from the group — that is,
/// whether the `XACK` landed. A failed `XACK` leaves the entry in the PEL, so
/// the reaper reclaims and redelivers it and the message still exists; callers
/// holding a [`metrics::PendingDiscard`] must not confirm it in that case.
async fn route_to_dlq(
    conn: &mut RedisConnection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    fields: &HashMap<String, String>,
    user_headers: &HashMap<String, String>,
    reason: &str,
    death_count: u32,
) -> Result<bool> {
    let dlq = match topology.dlq() {
        Some(d) => d,
        None => {
            tracing::warn!(stream, entry_id, reason, "no DLQ configured — discarding");
            match xack(conn, stream, group, entry_id).await {
                Ok(true) => return Ok(true),
                Ok(false) => {
                    // Nothing was acknowledged, so this call did not drop the
                    // message — a reaper had already reclaimed the entry and
                    // its re-added copy is live. Reporting a discard here
                    // would count a message that still exists.
                    tracing::warn!(
                        stream,
                        entry_id,
                        "XACK acknowledged nothing while discarding (no DLQ) — \
                         a reaper reclaimed the entry and owns its redelivery"
                    );
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!(stream, entry_id, error = %e, "XACK failed while discarding (no DLQ)");
                    metrics::record_backend_error(
                        metrics::BackendLabel::Redis,
                        metrics::BackendErrorKind::Ack,
                    );
                    return Ok(false);
                }
            }
        }
    };

    // Pre-size: "XADD", dlq, "*", all field pairs (internal + user headers),
    // 3 extra k/v pairs (reason, count, original).
    let arg_count = fields
        .len()
        .saturating_add(user_headers.len())
        .saturating_mul(2)
        .saturating_add(9);
    let mut cmd = redis::Cmd::with_capacity(arg_count, arg_count.saturating_mul(16));
    cmd.arg("XADD").arg(dlq).arg("*");
    for (k, v) in merged_entry_fields(fields, user_headers, None) {
        cmd.arg(k).arg(v);
    }
    cmd.arg(X_DEATH_REASON).arg(reason);
    cmd.arg(X_DEATH_COUNT).arg(death_count.to_string());
    cmd.arg(X_ORIGINAL_QUEUE).arg(stream);

    conn.query::<redis::Value>(&mut cmd).await.map_err(|e| {
        tracing::warn!(error = %e, dlq, "XADD to DLQ failed — message stays in PEL");
        ShoveError::Connection(format!("XADD to DLQ failed: {e}"))
    })?;

    match xack(conn, stream, group, entry_id).await {
        Ok(true) => Ok(true),
        Ok(false) => {
            // The DLQ copy is in place, but a reaper had already reclaimed and
            // re-added the original, so its replacement is live too. The
            // delivery was duplicated rather than retired by us.
            tracing::warn!(
                stream,
                entry_id,
                "XACK acknowledged nothing after DLQ enqueue — a reaper \
                 reclaimed the entry, so a re-added copy survives alongside \
                 the dead-lettered one"
            );
            Ok(false)
        }
        Err(e) => {
            tracing::warn!(stream, entry_id, error = %e, "XACK failed after DLQ enqueue");
            metrics::record_backend_error(
                metrics::BackendLabel::Redis,
                metrics::BackendErrorKind::Ack,
            );
            Ok(false)
        }
    }
}

/// Re-add the entry to its own stream, for a Retry/Defer with no hold queue.
///
/// Reports whether the replacement copy actually landed. The caller XACKs the
/// original only on `Ok(())`: the XADD is what preserves the message, so
/// acking after a failed XADD deletes the sole copy. That combination is
/// reachable in practice — a Redis ACL that grants `XACK` but denies `XADD`
/// (or a stream at `MAXLEN` with `NOMKSTREAM` semantics upstream) fails the
/// re-add while the ack still succeeds — so the failure has to propagate
/// instead of being logged and swallowed.
async fn requeue_to_stream(
    conn: &mut RedisConnection,
    stream: &str,
    fields: &HashMap<String, String>,
    user_headers: &HashMap<String, String>,
    retry_count: u32,
) -> Result<()> {
    // Pre-size: "XADD", stream, "*", all field pairs (internal + user headers,
    // one key filtered at runtime), 1 extra k/v pair.
    let arg_count = fields
        .len()
        .saturating_add(user_headers.len())
        .saturating_mul(2)
        .saturating_add(4);
    let mut cmd = redis::Cmd::with_capacity(arg_count, arg_count.saturating_mul(16));
    cmd.arg("XADD").arg(stream).arg("*");
    for (k, v) in merged_entry_fields(fields, user_headers, Some(X_RETRY_COUNT)) {
        cmd.arg(k).arg(v);
    }
    cmd.arg(X_RETRY_COUNT).arg(retry_count.to_string());
    conn.query::<redis::Value>(&mut cmd).await.map(|_| ()).map_err(|e| {
        tracing::warn!(error = %e, stream, "XADD on immediate requeue failed — leaving the entry in the PEL");
        ShoveError::Connection(format!("XADD on immediate requeue failed: {e}"))
    })
}

/// How many times a *failed* ownership check is retried before the outcome is
/// routed anyway.
///
/// A check that answers "not yours" is believed immediately. A check that
/// cannot answer at all is retried, because both readings of it are bad: see
/// [`resolve_under_lease`].
const OWNERSHIP_CHECK_ATTEMPTS: u32 = 3;

/// Pause between [`OWNERSHIP_CHECK_ATTEMPTS`]. Short — the whole retry budget
/// has to fit inside the margin the lease bought us, which is one renewal
/// interval.
const OWNERSHIP_CHECK_BACKOFF: Duration = Duration::from_millis(50);

/// Decide whether this consumer may apply `outcome` to the entry it holds.
///
/// Called on both paths that produce an outcome under a lease — a handler that
/// returned normally and one that hit its deadline — because a lease can be
/// lost either way. `leased` is false for consumers without
/// `handler_timeout_outcome` set: they hold no lease, so there is nothing to
/// check and `outcome` passes straight through (as does `None`, the "leave the
/// timed-out entry to the reaper" case).
///
/// When a lease *is* held, the outcome is only returned if we still own the
/// entry. Having lost it, a reaper already owns the entry's redelivery, and
/// routing anyway would put our copy — a DLQ entry, a hold-queue entry, a
/// requeue — on the stream *alongside* the reaper's re-add. Declining leaves
/// exactly the reaper's copy, which is the behaviour this consumer would have
/// had with no override at all.
///
/// ## Why an errored check is not treated as a loss
///
/// It used to be, on the reasoning that an entry left in the PEL is
/// redelivered rather than dropped. That is only true when something is
/// actually reclaiming: [`super::maintenance`] disables XAUTOCLAIM for the
/// whole `(client, stream, group)` key as soon as one
/// `without_handler_timeout()` consumer joins it, and a deployment may have no
/// other process sweeping. Declining on an error can therefore strand the
/// entry in the PEL indefinitely — a worse failure than the duplicate it
/// avoids, and a silent one.
///
/// So the check is retried, and if it still cannot be answered the outcome is
/// applied. Redis Streams delivery is at-least-once; a duplicate is within
/// contract, a permanently stuck message is not.
async fn resolve_under_lease(
    conn: &mut RedisConnection,
    lease: &lease::Lease<'_>,
    leased: bool,
    outcome: Option<Outcome>,
) -> Option<Outcome> {
    let outcome = outcome?;
    may_act_on_entry(conn, lease, leased, &outcome)
        .await
        .then_some(outcome)
}

/// The ownership check behind [`resolve_under_lease`], as a plain predicate.
///
/// Split out because handler outcomes are not the only writes this consumer
/// makes to an entry it may no longer own. A batch read with
/// `XREADGROUP COUNT prefetch` puts every entry in our PEL at once, but they
/// are inspected one at a time; an entry near the end of the batch can sit
/// idle for most of the batch's processing time before it is even looked at.
/// The pre-handler terminal paths — missing payload, oversize, undecodable —
/// then dead-letter or ack it directly, without a handler ever running, so
/// without this check they would write alongside a reaper's re-add exactly
/// like an unguarded outcome would.
///
/// `action` is only used for logging; it is whatever the caller was about to
/// do (an [`Outcome`], or a reason string on the pre-handler paths).
async fn may_act_on_entry(
    conn: &mut RedisConnection,
    lease: &lease::Lease<'_>,
    leased: bool,
    // `+ Sync` so the `&dyn` stays `Send`: this is awaited inside the spawned
    // per-key tasks of the sequenced path, whose futures must be `Send`.
    action: &(dyn std::fmt::Debug + Sync),
) -> bool {
    if !leased {
        return true;
    }

    let mut last_error = None;
    for attempt in 0..OWNERSHIP_CHECK_ATTEMPTS {
        match lease::touch(conn, lease).await {
            Ok(true) => return true,
            Ok(false) => {
                tracing::warn!(
                    stream = lease.stream,
                    entry_id = lease.entry_id,
                    ?action,
                    "not routed — a reaper reclaimed the entry and now owns \
                     its redelivery",
                );
                metrics::record_backend_error(
                    metrics::BackendLabel::Redis,
                    metrics::BackendErrorKind::Ack,
                );
                return false;
            }
            Err(e) => {
                last_error = Some(e);
                if attempt + 1 < OWNERSHIP_CHECK_ATTEMPTS {
                    tokio::time::sleep(OWNERSHIP_CHECK_BACKOFF).await;
                }
            }
        }
    }

    tracing::warn!(
        stream = lease.stream,
        entry_id = lease.entry_id,
        ?action,
        error = ?last_error.map(|e| e.to_string()),
        attempts = OWNERSHIP_CHECK_ATTEMPTS,
        "could not confirm entry ownership — acting anyway rather than risk \
         stranding the entry; it may be delivered again",
    );
    metrics::record_backend_error(metrics::BackendLabel::Redis, metrics::BackendErrorKind::Ack);
    true
}

/// Acknowledge `entry_id`, reporting whether **this** call is what retired it.
///
/// `XACK` replies with the number of entries it actually removed from the
/// group's PEL, and Redis documents `0` as "nothing was acknowledged" — the
/// entry was already gone. In this backend that is not a benign no-op: it is
/// the signature of a reaper that reclaimed the entry, XADDed a replacement
/// and XACKed the original while we were resolving an outcome for it. A live
/// copy therefore still exists, so callers that treat an ack as "the message
/// is gone" (the no-DLQ discard accounting) must not do so on `false`.
///
/// `Err` keeps its existing meaning: the command did not complete, so
/// ownership is unknown and the entry is assumed to survive.
async fn xack(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    entry_id: &str,
) -> Result<bool> {
    conn.query::<i64>(redis::cmd("XACK").arg(stream).arg(group).arg(entry_id))
        .await
        .map(|acked| acked > 0)
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
/// `user_headers` contains everything else and is shared into
/// [`MessageMetadata::headers`].
///
/// The two maps are disjoint by construction, so [`merged_entry_fields`] can
/// re-join them for a write-back without any key colliding.
pub(super) fn partition_entry_fields(
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

/// Re-join the two halves of a partitioned entry for a write-back (hold queue,
/// immediate requeue, DLQ), optionally dropping one key the caller is about to
/// rewrite.
///
/// Every path that re-publishes an entry must go through this: writing back
/// only `internal_fields` silently strips the publisher's headers, so a handler
/// sees them on the first delivery and not on the redelivery, and a dead letter
/// arrives without the context needed to triage it.
fn merged_entry_fields<'a>(
    internal_fields: &'a HashMap<String, String>,
    user_headers: &'a HashMap<String, String>,
    exclude: Option<&'a str>,
) -> impl Iterator<Item = (&'a str, &'a str)> {
    internal_fields
        .iter()
        .chain(user_headers.iter())
        .filter(move |(k, _)| Some(k.as_str()) != exclude)
        .map(|(k, v)| (k.as_str(), v.as_str()))
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
// Batch consumption
// ---------------------------------------------------------------------------
//
// `run_batch_impl` is a separate accumulation loop over its own
// `XREADGROUP`, never the single-message loop above and never the
// `RedisConsumerGroupRegistry::register` spawner — the non-concurrent
// `COUNT 1` clamp (`consumer_group.rs::register`'s `effective_prefetch`)
// lives in that spawner, so a loop that computes its own COUNT never passes
// through it.
//
// # Why no per-entry lease during accumulation or flush
//
// The single-message path holds a lease (`super::lease`) for the duration of
// one handler call so a foreign reaper cannot race the owner at the handler
// timeout. Renewing N per-entry leases for a whole batch cycle would be N
// `EVAL`s per renewal tick — instead this loop widens the *reclaim policy*
// itself to cover the whole batch cycle (see `run_batch_impl`'s maintenance
// interest, below) so this process's own sidecar cannot steal a live batch;
// the ownership guard on every terminal pre-handler write and every
// `DeadLetter` entry (`may_act_on_entry`, `leased: true` unconditionally)
// covers foreign sidecars; and `Commit`'s `XACK` / `Redeliver`'s no-write are
// intrinsically safe against a stolen entry (an `XACK` of a non-pending id is
// a silent no-op). The reclaim-collision window this leaves is wider than
// the leased single-message path's — accepted, and at-least-once holds
// throughout exactly as it does everywhere else in this backend.
//
// # PEL replay metadata
//
// A history read (`XREADGROUP … STREAMS stream <id>` with `<id> != ">"`)
// resets the entry's PEL idle clock and increments its server-side delivery
// counter (visible via `XPENDING`), but leaves `x-retry-count` and
// `redelivered` untouched — this backend has no `MaxDeliver` concept and
// shove's Redis `MessageMetadata::delivery_count` is always `None` (see
// `run_stream_loop_arc`'s comment on why). A `Redeliver` settlement is
// therefore a re-buffer, matching Kafka's batch seek-back, not InMemory's
// `mark_redelivery` — the same divergence
// `src/backend/batch_consumer.rs`'s module doc already states for the
// generic `Retry`/`Defer` table.
//
// # The no-lease design's wider reclaim window, restated precisely
//
// See `run_batch_impl`'s maintenance-interest doc for the exact policy this
// loop registers, and its "without `handler_timeout`" caveat for the one
// configuration where a dead or reconnected batch consumer's pending entries
// can strand rather than reclaim.

/// One buffered stream entry's metadata, kept index-parallel with the
/// decoded message so a `DeadLetter` settlement (or the debug log on a
/// partial `Commit` ack) can act on the exact bytes/headers/retry-count that
/// arrived, without re-fetching anything.
struct RedisBatchEntry {
    entry_id: String,
    /// Internal fields, WITH the payload re-inserted (mirrors every
    /// single-message write-back site above) — `route_to_dlq` needs it to
    /// write the DLQ copy.
    fields: HashMap<String, String>,
    user_headers: Arc<HashMap<String, String>>,
    retry_count: u32,
}

/// One in-flight batch. Unlike InMemory's `InMemoryBatch`, there is no
/// `dropped`/`parked` bookkeeping: a pre-handler drop on this backend settles
/// immediately (acked or dead-lettered at drop time, never joining the
/// batch — see the module doc), so `entries`/`messages` only ever hold
/// entries the handler will actually see, and the size trigger is exactly
/// `messages.len()`.
struct RedisBatch<T: Topic> {
    entries: Vec<RedisBatchEntry>,
    /// Index-parallel with `entries`.
    messages: Vec<(T::Message, MessageMetadata)>,
}

impl<T: Topic> RedisBatch<T> {
    fn new(max_batch_size: usize) -> Self {
        let cap = max_batch_size.min(PREALLOC_CAP);
        Self {
            entries: Vec::with_capacity(cap),
            messages: Vec::with_capacity(cap),
        }
    }

    fn len(&self) -> usize {
        self.messages.len()
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        entry_id: String,
        fields: HashMap<String, String>,
        user_headers: Arc<HashMap<String, String>>,
        retry_count: u32,
        message: T::Message,
        metadata: MessageMetadata,
    ) {
        self.entries.push(RedisBatchEntry {
            entry_id,
            fields,
            user_headers,
            retry_count,
        });
        self.messages.push((message, metadata));
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.messages.clear();
    }
}

/// Pure headroom computation for the batch's next `XREADGROUP COUNT`: the
/// remaining room in the batch. Never zero in practice — the caller only
/// reads when `buffered < max_batch_size` (the size trigger fires first
/// otherwise) — but the subtraction is saturating regardless, so a caller
/// error here can never wrap instead of clamping to zero.
fn batch_headroom(max_batch_size: usize, buffered: usize) -> usize {
    max_batch_size.saturating_sub(buffered)
}

/// Pure `BLOCK` computation for a live (`>`) `XREADGROUP` read: `BLOCK_MS`
/// while no age deadline is armed (an empty buffer — nothing to time out
/// yet), otherwise the smaller of `BLOCK_MS` and the time left until the
/// deadline, floored at 1ms. `BLOCK 0` means "block forever" in Redis, so the
/// floor is load-bearing, not a tuning nicety: a deadline that has *already*
/// elapsed (this call is merely late reaching the server) must still block
/// for a positive, if tiny, duration rather than hang the read indefinitely.
fn batch_block_ms(deadline: Option<Instant>, now: Instant) -> u64 {
    match deadline {
        None => BLOCK_MS,
        Some(d) => {
            let remaining_ms = d.saturating_duration_since(now).as_millis();
            let remaining_ms = u64::try_from(remaining_ms).unwrap_or(u64::MAX);
            remaining_ms.clamp(1, BLOCK_MS)
        }
    }
}

/// One `XREADGROUP` reply's worth of entries, as
/// [`parse_xreadgroup_reply`] hands them back: `(entry_id, fields)`.
type BatchEntries = Vec<(String, Vec<(String, String)>)>;

/// What one read-ahead came back with. `Nothing` covers both "not armed this
/// cycle" and "the stream had nothing to give", which the loop treats
/// identically; `Failed` is distinct because it is the only one that says
/// something about the *connection* — see [`read_ahead_batch`].
enum ReadAhead {
    Entries(BatchEntries),
    Nothing,
    Failed,
}

/// One **non-blocking** live (`>`) `XREADGROUP` for the *next* batch, issued
/// to run concurrently with the current batch's flush (see
/// [`run_batch_impl`]'s "Read-ahead across the flush").
///
/// `BLOCK` is deliberately absent, and that is a correctness requirement
/// rather than a tuning choice: this future is joined with the flush, so a
/// blocking read on an idle stream would keep the join pending for up to
/// [`BLOCK_MS`] after the flush had returned, delaying the loop and shutdown
/// with it — where the loop's own read is the one that may wait, raced
/// `biased` against the shutdown token. Without `BLOCK` an empty stream
/// answers immediately with nil, which is `None` here and costs the flush
/// nothing.
///
/// A second reason applied while this read rode a clone of the loop's socket
/// (Redis does not serve a blocked client's subsequent commands until it
/// unblocks, so a blocking read-ahead would have held the flush's `XACK`
/// hostage) and no longer does: the read-ahead has a connection of its own —
/// see [`run_batch_impl`] for what that costs and why it is still the choice.
/// The join reason above is sufficient on its own.
///
/// A failure is **reported to the caller** rather than propagated out of the
/// loop, and the distinction is load-bearing now that this read has a socket
/// of its own. While it rode a clone of the loop's connection, swallowing the
/// error was enough: the same dead socket made the loop's own next read fail,
/// which reported it with the error taxonomy `run_with_reconnect` expects. On
/// a separate connection nothing else ever touches this socket, so a swallowed
/// error would leave the read-ahead silently dead for the rest of the
/// reconnect cycle while the loop ran on, degraded and quiet. `run_batch_impl`
/// therefore drops the connection on `Err` and re-dials it at the next arming
/// — best effort, because the loop's own read covers every entry either way.
/// Failing the whole cycle instead was rejected: a transient read-ahead error
/// would then spend a `max_reconnect_attempts` budget that exists for real
/// broker loss.
///
/// Entries this read delivered but whose reply never arrived stay pending,
/// recovered exactly as a lost reply to the loop's own read already is — by
/// the reaper, or by the next `Redeliver` replay.
async fn read_ahead_batch(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    count: usize,
) -> ReadAhead {
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(count)
        .arg("STREAMS")
        .arg(stream)
        .arg(">");
    match conn.query::<redis::Value>(&mut cmd).await {
        Ok(value) => {
            let entries = parse_xreadgroup_reply(value, count);
            if entries.is_empty() {
                ReadAhead::Nothing
            } else {
                ReadAhead::Entries(entries)
            }
        }
        Err(e) => {
            tracing::debug!(
                stream,
                error = %e,
                "batch read-ahead XREADGROUP failed; its connection is dropped \
                 and re-dialed at the next flush"
            );
            ReadAhead::Failed
        }
    }
}

/// Ingest one read's entries into `batch`, arming the age deadline on the
/// first message to land in an empty buffer.
///
/// Shared by all three sites that ingest — the loop's own read, the
/// read-ahead, and the shutdown drain — so that none of them can arm the
/// deadline differently from the others. The deadline means "age of the
/// oldest message buffered", and it is never pushed back.
///
/// `read_at` is when these entries left the stream, and the deadline is armed
/// `max_batch_age` from it rather than from the ingest. For the loop's own
/// read the two are the same instant. For the read-ahead they are not: its
/// reply waits in `prefetched` for as long as the flush that issued it runs,
/// and `max_batch_age` bounds how long this loop may sit on an entry it has
/// already taken out of the stream — the entry is in the PEL and counting
/// against its own reclaim from the moment it is read, so the age clock may
/// not restart just because the entry is parked in a local variable rather
/// than in `batch`.
#[allow(clippy::too_many_arguments)]
async fn ingest_entries<T: Topic>(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    entries: BatchEntries,
    topic_name: &str,
    consumer_group: Option<&str>,
    max_message_size: Option<usize>,
    batch: &mut RedisBatch<T>,
    deadline: &mut Option<Instant>,
    max_batch_age: Duration,
    read_at: Instant,
) -> Result<()> {
    for (entry_id, fields_vec) in entries {
        ingest_batch_entry::<T>(
            conn,
            stream,
            group,
            consumer,
            entry_id,
            fields_vec,
            topic_name,
            consumer_group,
            max_message_size,
            batch,
        )
        .await?;
        if deadline.is_none() && !batch.is_empty() {
            // `checked_add` rather than `+`: this loop's contract is no
            // unchecked arithmetic on a runtime path, and
            // `with_max_batch_age` bounds its argument below (non-zero) but
            // not above. An age too large for the clock to represent leaves
            // the deadline unarmed, which is what such an age means — no age
            // trigger, with the size trigger still bounding the batch.
            *deadline = read_at.checked_add(max_batch_age);
        }
    }
    Ok(())
}

/// Advance a PEL-replay cursor after a non-blocking history read. History
/// reads are exclusive of the start id, so the next read must begin just
/// past the last entry actually returned; an empty reply means the PEL is
/// drained for now, so the caller switches back to live (`>`) reads.
fn next_replay_cursor(entries: &[(String, Vec<(String, String)>)]) -> Option<String> {
    entries.last().map(|(id, _)| id.clone())
}

/// Decode one XREADGROUP-returned entry and either push it onto `batch` or
/// settle it immediately (acked or dead-lettered) before it ever joins one —
/// the same pre-handler drop paths as `run_stream_loop_arc` (missing
/// payload / oversize / undecodable), with two differences: `leased: true`
/// is passed to `may_act_on_entry` unconditionally (a batch enters the PEL
/// all at once but is inspected serially — see `may_act_on_entry`'s own doc
/// — and copying the single-message `handler_timeout_outcome.is_some()`
/// expression here would make the guard vacuous whenever no override is
/// configured, the default), and there is no `FailAll`/poisoned-key handling
/// (the batch path only accepts `NotSequenced` topics).
#[allow(clippy::too_many_arguments)]
async fn ingest_batch_entry<T: Topic>(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    entry_id: String,
    fields_vec: Vec<(String, String)>,
    topic_name: &str,
    consumer_group: Option<&str>,
    max_message_size: Option<usize>,
    batch: &mut RedisBatch<T>,
) -> Result<()> {
    let (mut fields, user_headers) = partition_entry_fields(fields_vec);
    let user_headers = Arc::new(user_headers);
    let lease = lease::Lease {
        stream,
        group,
        consumer,
        entry_id: &entry_id,
    };

    let payload_raw = match fields.remove(PAYLOAD_FIELD) {
        Some(s) => s,
        None => {
            if !may_act_on_entry(conn, &lease, true, &"missing-payload").await {
                return Ok(());
            }
            tracing::warn!(entry_id, "missing payload field — acking and skipping");
            match xack(conn, stream, group, &entry_id).await {
                Ok(true) => metrics::record_failed(
                    topic_name,
                    consumer_group,
                    metrics::FailReason::Malformed,
                ),
                Ok(false) => {
                    tracing::debug!(
                        entry_id,
                        "corrupt entry was already retired by a reaper — not counting"
                    );
                }
                Err(e) => {
                    tracing::warn!(entry_id, error = %e, "XACK failed after skipping corrupt entry");
                    metrics::record_backend_error(
                        metrics::BackendLabel::Redis,
                        metrics::BackendErrorKind::Ack,
                    );
                }
            }
            return Ok(());
        }
    };

    // Same placement as every single-message site: before the size check, so
    // an oversize payload still lands in the histogram.
    metrics::record_message_size(topic_name, consumer_group, payload_raw.len());

    let retry_count = fields
        .get(X_RETRY_COUNT)
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    if let Some(max) = max_message_size
        && payload_raw.len() > max
    {
        if !may_act_on_entry(conn, &lease, true, &"oversize").await {
            return Ok(());
        }
        tracing::warn!(
            entry_id,
            size = payload_raw.len(),
            limit = max,
            "message exceeds size limit — sending to DLQ"
        );
        metrics::record_failed(topic_name, consumer_group, metrics::FailReason::Oversize);
        fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
        let topology = T::topology();
        route_to_dlq(
            conn,
            topology,
            stream,
            group,
            &entry_id,
            &fields,
            &user_headers,
            "oversize",
            retry_count,
        )
        .await?;
        return Ok(());
    }

    let msg: T::Message =
        match <T::Codec as crate::Codec<T::Message>>::decode(payload_raw.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                if !may_act_on_entry(conn, &lease, true, &"deserialize").await {
                    return Ok(());
                }
                tracing::warn!(error = %e, entry_id, "deserialization failed — sending to DLQ");
                metrics::record_failed(
                    topic_name,
                    consumer_group,
                    metrics::FailReason::Deserialize,
                );
                fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
                let topology = T::topology();
                route_to_dlq(
                    conn,
                    topology,
                    stream,
                    group,
                    &entry_id,
                    &fields,
                    &user_headers,
                    "deserialize",
                    retry_count,
                )
                .await?;
                return Ok(());
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
        delivery_count: None,
        headers: Arc::clone(&user_headers),
    };

    fields.insert(PAYLOAD_FIELD.to_owned(), payload_raw);
    batch.push(entry_id, fields, user_headers, retry_count, msg, meta);
    Ok(())
}

/// Acknowledge a batch of entry ids in one round trip, reporting how many
/// were actually removed from the PEL. Fewer than `entry_ids.len()` means a
/// reaper already reclaimed and redelivered some of them before this call
/// landed — the batch counterpart of `xack`'s single-entry signal — logged,
/// never treated as a discard: every message here already ran through the
/// handler successfully, so a partial ack is a reclaim race, not data loss.
async fn xack_many(
    conn: &mut RedisConnection,
    stream: &str,
    group: &str,
    entry_ids: &[&str],
) -> Result<i64> {
    if entry_ids.is_empty() {
        return Ok(0);
    }
    let mut cmd = redis::cmd("XACK");
    cmd.arg(stream).arg(group);
    for id in entry_ids {
        cmd.arg(*id);
    }
    conn.query::<i64>(&mut cmd)
        .await
        .map_err(|e| ShoveError::Connection(format!("XACK (batch) failed: {e}")))
}

/// What [`flush_redis_batch`] tells its caller about the flush that just ran.
enum FlushOutcome {
    /// The batch settled (however: `Commit`, `DeadLetter` or `Redeliver`) and
    /// is now empty; the read loop may continue.
    Flushed,
    /// The batch resolved to `Redeliver` and, while backing off before the
    /// next replay read, the shutdown token fired. The caller must return
    /// `Ok(())` immediately rather than loop back — the un-acked batch stays
    /// pending in the PEL for the reaper, exactly as a graceful shutdown
    /// leaves it.
    ShutdownDuringBackoff,
}

/// Hand the buffered batch to the handler and apply the single returned
/// [`Outcome`] via the shared [`settle_batch_outcome`] classifier.
///
/// - **Commit**: one variadic [`xack_many`]. Resets `redelivery_backoff` and
///   clears the batch.
/// - **DeadLetter**: per message, in order — the ownership guard FIRST
///   (`may_act_on_entry`, `leased: true` unconditionally; `false` skips that
///   one entry, it is the reaper's now), then
///   [`metrics::record_terminal`] and [`route_to_dlq`] with this entry's own
///   `retry_count` as the death count. An `Err` from `route_to_dlq`
///   propagates immediately (after `pending.survived()`) rather than
///   continuing the batch — exactly like the single-message `Dlq` arm:
///   continuing would hammer a dead connection, and the caller's
///   `run_with_reconnect` wrapper is what recovers. Messages not yet
///   processed at that point stay in the old consumer name's PEL for the
///   reaper. On full success: resets backoff, clears the batch.
/// - **Redeliver** (`Retry`/`Defer`): no ack at all — the whole batch stays
///   pending. Draws the shared redelivery backoff, `select!`s the delay
///   against shutdown (shutdown wins ⇒ [`FlushOutcome::ShutdownDuringBackoff`],
///   entries stay pending — the reaper is the crash/stop safety net), then
///   arms `*replay_cursor = Some("0")` and clears the batch so the next read
///   drains the PEL from the start instead of hoping the live `>` cursor
///   happens to redeliver it. Backoff is deliberately NOT reset here.
#[allow(clippy::too_many_arguments)]
async fn flush_redis_batch<T, H>(
    conn: &mut RedisConnection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    consumer: &str,
    handler: &H,
    ctx: &H::Context,
    batch: &mut RedisBatch<T>,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
    topic_name: &str,
    consumer_group: Option<&str>,
    redelivery_backoff: &mut Backoff,
    replay_cursor: &mut Option<String>,
    shutdown: &CancellationToken,
) -> Result<FlushOutcome>
where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    if batch.is_empty() {
        return Ok(FlushOutcome::Flushed);
    }
    let batch_size = batch.len();
    let messages = std::mem::take(&mut batch.messages);

    let outcome = invoke_batch_handler(
        || handler.handle_batch(messages, ctx),
        handler_timeout,
        handler_timeout_outcome,
        topic_name,
        consumer_group,
        batch_size as u64,
    )
    .await;

    match settle_batch_outcome(&outcome) {
        BatchSettlement::Commit => {
            let ids: Vec<&str> = batch.entries.iter().map(|e| e.entry_id.as_str()).collect();
            match xack_many(conn, stream, group, &ids).await {
                Ok(acked) if (acked as usize) < ids.len() => {
                    tracing::debug!(
                        stream,
                        acked,
                        expected = ids.len(),
                        "batch XACK acked fewer entries than requested — a reaper already retired some"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(stream, error = %e, "batch XACK failed");
                    metrics::record_backend_error(
                        metrics::BackendLabel::Redis,
                        metrics::BackendErrorKind::Ack,
                    );
                }
            }
            *redelivery_backoff = batch_redelivery_backoff();
            batch.clear();
            Ok(FlushOutcome::Flushed)
        }
        BatchSettlement::DeadLetter => {
            let has_dlq = topology.dlq().is_some();
            for entry in batch.entries.drain(..) {
                let lease = lease::Lease {
                    stream,
                    group,
                    consumer,
                    entry_id: &entry.entry_id,
                };
                if !may_act_on_entry(conn, &lease, true, &"batch-reject").await {
                    continue;
                }
                let pending = metrics::record_terminal(
                    topic_name,
                    consumer_group,
                    metrics::FailReason::Rejected,
                    has_dlq,
                );
                let retired = match route_to_dlq(
                    conn,
                    topology,
                    stream,
                    group,
                    &entry.entry_id,
                    &entry.fields,
                    &entry.user_headers,
                    "rejected",
                    entry.retry_count,
                )
                .await
                {
                    Ok(retired) => retired,
                    Err(e) => {
                        pending.survived();
                        return Err(e);
                    }
                };
                if retired {
                    pending.confirm();
                } else {
                    pending.survived();
                }
            }
            *redelivery_backoff = batch_redelivery_backoff();
            batch.clear();
            Ok(FlushOutcome::Flushed)
        }
        BatchSettlement::Redeliver => {
            let delay = next_redelivery_delay(redelivery_backoff);
            tracing::warn!(
                queue = stream,
                batch_size,
                ?outcome,
                delay_ms = delay.as_millis() as u64,
                "batch handler returned a non-Ack outcome, leaving the batch pending for redelivery"
            );
            batch.clear();
            *replay_cursor = Some("0".to_owned());
            tokio::select! {
                () = tokio::time::sleep(delay) => Ok(FlushOutcome::Flushed),
                () = shutdown.cancelled() => Ok(FlushOutcome::ShutdownDuringBackoff),
            }
        }
    }
}

/// A spawned task that is aborted when its handle is dropped.
///
/// `tokio::task::JoinHandle` detaches on drop; [`run_batch_impl`]'s read-ahead
/// dial wants the opposite, so a loop that exits — shutdown, or an error on its
/// way to `run_with_reconnect` — does not leave a dial running against a broker
/// it has stopped using.
struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    fn is_finished(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }

    /// The finished task's output, without suspending the caller.
    ///
    /// Only call once [`is_finished`](Self::is_finished) is true: a finished
    /// `JoinHandle` resolves on its first poll, so this `await` cannot yield —
    /// which is what lets the caller collect a dial from a path that must
    /// never wait on one. `None` if the task panicked or was aborted.
    async fn join_now(mut self) -> Option<T> {
        match self.0.take() {
            Some(handle) => handle.await.ok(),
            None => None,
        }
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.as_ref() {
            handle.abort();
        }
    }
}

/// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl) for
/// Redis. See the module doc above for the no-lease design and PEL-replay
/// metadata notes this loop depends on.
///
/// # Loop shape
///
/// Each iteration: flush if the size trigger has fired
/// (`batch.len() >= max_batch_size`); otherwise, if shutdown is already
/// cancelled, flush whatever is buffered and return; otherwise issue one
/// `XREADGROUP` — `>` (live) or the replay cursor (history), `COUNT`
/// [`batch_headroom`], `BLOCK` [`batch_block_ms`] on the live path only
/// (history reads never block) — raced `biased` against shutdown exactly
/// like `run_stream_loop_arc`, including the `NOGROUP` → retryable mapping.
/// Every returned entry is decoded via [`ingest_batch_entry`]; the age
/// deadline arms on the first message pushed into an empty batch, measured
/// from when that message left the stream (see [`ingest_entries`]), and is
/// never pushed back. After ingesting, the deadline is re-checked and a
/// flush fires if it has elapsed. `replay_cursor` advances via
/// [`next_replay_cursor`] after every history read; an empty history reply
/// switches back to live (`>`) reads. The age/size triggers apply
/// unchanged during replay, so a PEL larger than `max_batch_size` flushes in
/// size-capped batches rather than in one read.
///
/// A single `XREADGROUP COUNT max BLOCK max_age` per batch was rejected: it
/// returns as soon as ANY entry exists, so it cannot fill a batch, and it
/// holds shutdown hostage for up to `max_batch_age`.
///
/// # Read-ahead across the flush
///
/// A size-triggered flush issues the next read ([`read_ahead_batch`]) to run
/// concurrently with itself, and the reply seeds the next batch. Without it
/// the cycle is strictly serial — read, then handle and ack with nothing in
/// flight, then read again — so one consumer spends the whole flush idle on
/// its connection. Measured on this loop at 64 B / `max_batch_size` 500
/// against a Docker Redis 7.0 on an aarch64 Linux host: 3.88 ms per
/// 500-message cycle, of which the read phase alone is 57% and the process
/// holds one core only half busy.
///
/// What this does **not** settle is the sub-parity single-consumer batch row
/// in `benches/results/bench-results.json` — 64 B, one consumer, drain:
/// 132 389 msg/s against `consume_parallel`'s 289 728 at the same cell. That
/// row's cause has since been measured on the host that produced it (Apple
/// M4 Max / macOS), and it is outside this loop: a single consumer there
/// spends most of each cycle waiting on the Docker VM, never sustains the
/// utilisation that keeps the host's performance cores clocked up, and runs
/// every phase of the cycle slow — the decode included, which is client-side
/// CPU that nothing on the wire or inside the server can reach. Any
/// unrelated busy process on that host removes the deficit, and so does a
/// second consumer, which is why the multi-consumer rows are 1.3x-2.3x. The
/// measurements, and how to read the affected rows, are under *A lone
/// consumer on the macOS host clocks down* in `benches/README.md`. The
/// read-ahead is neither that row's cause nor its fix, and is justified here
/// only by what it measures where it was measured.
///
/// **Where the gain comes from, and where it is largest.** On a socket of its
/// own the read-ahead does not make the loop's round trips *fewer* — a
/// transparent proxy in front of the broker counts 803 consumer-path requests
/// per 200 000 messages at one consumer with or without it — it stops them
/// being *serial*: the next read travels on `read_conn` while the flush's
/// `XACK` travels on `conn`, where the serial loop issues each into the
/// silence the previous one left. The proxy's own accounting is the sharpest
/// form of that: it charges 800 of the split's 803 requests against 790 of
/// the serial loop's 803, so the faster arm is the one paying *more* charges,
/// concurrently.
///
/// The gain therefore scales with what one round trip costs. Behind that
/// proxy charging 1.2 ms on any consumer-path request issued after >= 0.3 ms
/// of quiet (64 B, 200 000-message drains, 3 reps, medians, arms interleaved)
/// it is **1.37x at one consumer** and 1.16x at two; with the same proxy in
/// measure-only mode — no charge, but its own hop still in the path — 1.15x
/// and 1.20x; and against a loopback Docker broker, where a round trip is
/// nearly free, it is the 0.96x and 1.11x measured below. A broker a network
/// hop away is the deployment this helps most, and a loopback broker at one
/// consumer is the one it costs.
///
/// # What a socket of its own costs, and why it is still the choice
///
/// The read-ahead reads on its own connection. The loop's own read and the
/// flush's `XACK` stay on `conn`, so no two replies this loop waits for are
/// queued behind each other on one client. The cheaper-looking structure is a
/// `conn.clone()`: a `MultiplexedConnection` clone shares the socket and the
/// multiplexer task, so it opens nothing (`connected_clients` does not move)
/// and the read-ahead's `XREADGROUP` pipelines with the flush's `XACK`
/// instead of taking turns with it. Three structures were built and measured
/// on the host above — no read-ahead; the clone; the split. 64 B,
/// `--concurrent --handler zero`, 600 000-message drains, `max_batch_size`
/// 500, every arm built from one `CARGO_TARGET_DIR`, medians of three reps
/// (four at eight consumers) with the arm order **rotated per rep** so no arm
/// is systematically measured into the host state another arm left behind:
///
/// | consumers | no read-ahead | shared socket | own socket (shipped) |
/// |---|---|---|---|
/// | 1 | 108 515 | 116 516 (1.074x) | 104 392 (**0.962x**) |
/// | 2 | 171 074 | 247 154 (1.445x) | 189 490 (1.108x) |
/// | 8 | 370 189 | 349 741 (**0.945x**) | 376 923 (1.018x) |
///
/// Read the crossover rather than any one column. The clone is worth more
/// wherever the server has idle to fill (+7% at one consumer, +45% at two)
/// and *regresses* the eight-consumer cell against no read-ahead at all,
/// where the split gives part of that gain back (+11% at two) and leaves the
/// eight-consumer cell alone. The split's own one-consumer cell is ~4%
/// **below** the serial loop here — small, but its runs do not overlap the
/// baseline's, so it is a cost rather than noise. This loop takes the
/// structure that regresses nothing at eight consumers: a multi-consumer
/// regression is what the published rows cannot afford, and the
/// single-consumer gain given up is the gain this host has least of (a
/// loopback round trip is nearly free — the paragraph above prices the same
/// read-ahead behind a proxy that charges for one).
///
/// The crossover is stable across campaigns rather than an artefact of one
/// hour: an earlier pass on this host with a fixed (unrotated) arm order put
/// the same three arms at 1.074x / 1.475x / 0.950x for the clone and
/// 0.999x / 1.188x / 1.077x for the split, and it also measured the mirror
/// split — settlement moved off instead of the read-ahead, which is what
/// `run_stream_loop_concurrent` does — within 4% of the split column (n=1).
/// The cost is having two sockets at all, not which half moves.
///
/// What the clone's eight-consumer cost is **not**, measured off the broker
/// rather than argued: the
/// command mix is identical (1 209 vs 1 214 `XREADGROUP`s at ~495 entries
/// each and 1 200 `XACK`s per 600 000 messages, so nothing is fragmented,
/// clamped or double-read), time inside Redis commands is within 4%
/// (2.163 vs 2.246 us/msg), the server is not command-saturated on either arm
/// (command execution fills 71% and 66% of the drain window), socket events
/// per cycle actually *fall* (2.0 -> 1.15 reads, 3.0 -> 2.1 writes: the
/// pipelining works), and `connected_clients` is unchanged at 11, proving the
/// clone opens no socket. What rises is Redis's non-command CPU per message,
/// +10 to 18% — the event-loop cost of serving one client whose 32 KB read
/// reply and whose ack reply are queued together, the ack's behind the
/// read's. Redis writes a client's replies in arrival order, so the flush
/// cannot finish until the read it was hiding has been drained: at eight
/// consumers that lands on the critical path, at one or two it does not.
/// Splitting the sockets is what removes it, and a second socket per consumer
/// is what that costs.
///
/// Five properties keep it from changing what a handler sees:
///
/// - **No surplus, so `max_batch_size` still bounds a handler call.** The
///   read-ahead is armed only where a flush is about to empty the batch, so
///   its at-most-`max_batch_size` entries are always ingested into an empty
///   buffer.
/// - **`max_batch_age` still bounds the wait, flush time included.** The
///   deadline for a prefetched entry is armed from the instant its read was
///   issued, not from the ingest that follows the flush, so parking a reply
///   in `prefetched` cannot extend how long this loop sits on an entry it
///   has already taken out of the stream. A flush that outlasts
///   `max_batch_age` therefore hands its read-ahead to a handler as soon as
///   the loop re-enters, rather than starting a fresh window.
/// - **Never during a replay.** A live `>` read would interleave
///   never-delivered entries into a PEL drain.
/// - **Dropped on `Redeliver`.** That settlement arms `replay_cursor = "0"`,
///   and the replay returns the read-ahead's entries too; ingesting them
///   here as well would deliver them twice in one process.
/// - **Settled on a graceful stop, not stranded.** The shutdown arm ingests
///   a pending read-ahead before its final flush, so a clean stop still
///   delivers what this loop fetched rather than leaving it for the reaper.
///
/// The reclaim policy below still covers the widened buffering phase: a
/// read-ahead entry waits out the flush that fetched it (bounded by
/// `handler_timeout`) before its own batch's age window even starts, so its
/// worst-case idle is `2 * handler_timeout + max_batch_age`, inside the
/// `2 * (handler_timeout + max_batch_age)` threshold this loop registers.
///
/// The deadline is **disarmed** (`deadline = None`) on every flush — the
/// size trigger, the age trigger, and the shutdown-triggered partial flush —
/// so an elapsed deadline is never left armed over an empty buffer (which
/// would either fire immediately on nothing or spin hot).
///
/// # Maintenance interest covers the whole batch cycle
///
/// The single-message policy (`handler_timeout`, or 2x it when the consumer
/// resolves its own timeout) assumes read→settle is bounded by one handler
/// run. This loop adds an unbounded-by-timeout buffering phase — an early
/// entry can idle up to `max_batch_age` before the flush even starts, and
/// nothing resets its idle clock while buffered — so the reclaim policy this
/// loop registers is the EFFECTIVE timeout `handler_timeout.map(|t|
/// t.saturating_add(max_batch_age))`, always with `resolves_own_timeout:
/// true` (this loop always acts at its own deadline: `invoke_batch_handler`'s
/// timeout arm resolves to `Retry` or the configured
/// `handler_timeout_outcome`, never "leave it for the reaper" the way the
/// single-message no-override path does). That doubles again inside
/// `reclaim_policy`, so the sidecar's threshold is
/// `2 * (handler_timeout + max_batch_age)` — comfortably past buffer-wait
/// plus flush. A replay read resets idle on every entry it returns
/// (empirically confirmed — see the plan's R1), so consecutive `Redeliver`
/// cycles do not accumulate idle time against this threshold.
///
/// The registry's dedup is max-wins (see `super::maintenance`'s module doc),
/// so this policy cannot be undercut by a co-located single-message consumer
/// on the same `(stream, group)` — but the converse holds too: a batch guard
/// on the key delays crash recovery for every co-located single-message
/// consumer to this same, wider threshold.
///
/// `without_handler_timeout()` ⇒ `effective_timeout` is `None` ⇒ reclaim is
/// disabled while this consumer runs, exactly mirroring the single-message
/// no-timeout semantics. Entries a dead or reconnected no-timeout batch
/// consumer leaves pending are then recovered only if some OTHER guard on
/// the same `(stream, group)` carries a reclaim policy — otherwise they
/// strand, the same shape as the single-message no-timeout path, wider
/// because a batch is bigger. (Test 15's reaper is spawned directly, not
/// through this registry, precisely to make that recovery path provable
/// without depending on a second consumer being present.)
///
/// # No panics, no unchecked arithmetic
///
/// No `unwrap`/`expect`/indexing on runtime paths; all arithmetic here is
/// `saturating`/`checked` — see [`batch_headroom`], [`batch_block_ms`].
pub(crate) async fn run_batch_impl<T, H>(
    client: RedisClient,
    handler: H,
    ctx: H::Context,
    options: BatchConsumerOptionsInner,
) -> Result<()>
where
    T: NotSequenced,
    H: BatchMessageHandler<T>,
{
    let topology = T::topology();
    let stream = topology.queue();
    let group = client.group().to_owned();
    let shutdown = options.shutdown.clone();
    let consumer_group = options.consumer_group.clone();
    let max_batch_size = options.max_batch_size.max(1);
    let max_batch_age = options.max_batch_age;
    let max_message_size = options.max_message_size;
    let handler_timeout = options.handler_timeout;
    let handler_timeout_outcome = options.handler_timeout_outcome.clone();
    let max_reconnect_attempts = options.max_reconnect_attempts;

    let topic_arc: Arc<str> = Arc::from(stream);
    let group_arc: Option<Arc<str>> = consumer_group.as_deref().map(Arc::from);

    // See the doc above: the effective policy covers buffer-wait + flush,
    // not just one handler call, and this loop always resolves its own
    // timeout (never "leave it for the reaper").
    let effective_timeout = handler_timeout.map(|t| t.saturating_add(max_batch_age));
    let _maintenance = super::maintenance::acquire(&client, stream, effective_timeout, true);

    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);

    run_with_reconnect(&shutdown, stream, max_reconnect_attempts, || {
        let client = client.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let group = group.clone();
        let consumer = RedisConsumer::consumer_name();
        let shutdown = shutdown.clone();
        let topic_arc = Arc::clone(&topic_arc);
        let group_arc = group_arc.clone();
        let handler_timeout_outcome = handler_timeout_outcome.clone();

        async move {
            let mut conn = client.dedicated_conn().await?;
            // A second connection rather than a `conn.clone()`, and that is a
            // **measured** choice rather than the tidier-looking one. A
            // `MultiplexedConnection` clone shares the socket and the
            // multiplexer task, so the read-ahead's `XREADGROUP` and the
            // flush's `XACK` would pipeline over one socket instead of taking
            // turns on it — which buys the 1- and 2-consumer cells, and costs
            // the 8-consumer one, because Redis writes a client's replies in
            // arrival order and the ack's then waits behind a 32 KB read reply.
            // This loop takes the structure that regresses no multi-consumer
            // cell and pays about 4% at one consumer for it; both halves of the
            // trade, in both directions, are in `run_batch_impl`'s "What a
            // socket of its own costs, and why it is still the choice".
            // `multiplexed_conn` rather than
            // `dedicated_conn` because the read-ahead never blocks (see
            // [`read_ahead_batch`]), the same call
            // `run_stream_loop_concurrent` makes for its `outcome_conn`.
            // Re-dialed when this closure re-runs, alongside `conn`, and also
            // on its own whenever a read-ahead fails on it — `None` means
            // "dial one before the next read-ahead", see [`read_ahead_batch`].
            let mut read_conn = Some(client.multiplexed_conn().await?);
            // A re-dial in flight for `read_conn`, spawned rather than
            // awaited. See the recovery comment in the size-triggered arm:
            // the flush must never wait on a dial, so the dial runs off the
            // loop and a later cycle collects it. Aborted on drop, so a loop
            // that exits (shutdown, or an error into `run_with_reconnect`)
            // does not leave a dial running against a broker it has stopped
            // using.
            let mut dialing: Option<AbortOnDrop<Result<RedisConnection>>> = None;
            let mut batch: RedisBatch<T> = RedisBatch::new(max_batch_size);
            let mut deadline: Option<Instant> = None;
            let mut redelivery_backoff = batch_redelivery_backoff();
            let mut replay_cursor: Option<String> = None;
            // The read-ahead's reply, waiting to seed the next batch, paired
            // with the instant its read was issued. Only ever `Some` between
            // a size-triggered flush and the ingest that immediately follows
            // it. The instant rides along because the age deadline is armed
            // from it, not from the ingest — see `ingest_entries`.
            let mut prefetched: Option<(Instant, BatchEntries)> = None;

            loop {
                if batch.len() >= max_batch_size {
                    // Keep the next read in flight for the duration of the
                    // flush. Serialized read → flush → read leaves this
                    // consumer idle on the socket for the whole flush, with
                    // nothing of its own to fill that window with. This is
                    // not the published single-consumer deficit — see the
                    // arithmetic in `run_batch_impl`'s doc comment — it is
                    // just a gap worth not having.
                    //
                    // No surplus buffer is needed to make this safe: the
                    // flush below empties `batch`, so a
                    // `COUNT max_batch_size` reply can only ever be ingested
                    // into an empty buffer and `max_batch_size` still bounds
                    // what one handler call sees.
                    //
                    // `read_conn` is a connection of its own, not a clone of
                    // `conn`: this read and the flush's `XACK` do not queue
                    // their replies on one client, which is what keeps the
                    // eight-consumer cell from regressing. See this function's
                    // "What a socket of its own costs, and why it is still the
                    // choice" for the whole trade, and [`read_ahead_batch`] for
                    // why the read-ahead omits `BLOCK` either way.
                    //
                    // A replay cycle takes no read-ahead: a live `>` read
                    // would interleave never-delivered entries into a PEL
                    // drain, and `next_replay_cursor` only describes the
                    // history read it advanced from.
                    let read_ahead_armed = replay_cursor.is_none();
                    if read_ahead_armed && read_conn.is_none() {
                        // A previous read-ahead failed on that socket and it
                        // was dropped; get a fresh one — **without the flush
                        // ever waiting for it**.
                        //
                        // Awaiting the dial here instead is what this does
                        // not do, and the difference is not a micro-
                        // optimisation. A dial is bounded only by
                        // `connection_timeout` (10 s by default), and it can
                        // run to that bound while this loop's own connection
                        // is perfectly healthy: a middlebox that accepts a
                        // TCP connection and answers nothing stalls the
                        // handshake, not the socket already established. An
                        // awaited dial therefore parks a `max_batch_size`-
                        // full, PEL-owned batch in memory for up to that
                        // timeout before its handler sees it — and since a
                        // failed dial leaves `read_conn` at `None`, the next
                        // size-triggered cycle pays it again. That turns an
                        // optional read-ahead into a per-batch stall and
                        // breaks the size/age flush contract on precisely the
                        // recovery path (`a_stalled_read_ahead_re_dial_does_
                        // not_delay_the_flush` measures it: ~1 batch per
                        // `connection_timeout` instead of the whole drain).
                        //
                        // So the dial runs as its own task and a later cycle
                        // collects it. Polling `is_finished` rather than
                        // awaiting is the whole point — this arm never yields
                        // on the dial. One dial is in flight at a time, and
                        // a failed one is simply re-spawned at the next
                        // cycle; the loop's own read covers every entry while
                        // the read-ahead is off, so there is nothing to
                        // recover urgently and nothing to escalate to
                        // `run_with_reconnect` (a transient read-ahead error
                        // must not spend a `max_reconnect_attempts` budget
                        // that exists for real broker loss).
                        match dialing.take() {
                            Some(handle) if handle.is_finished() => {
                                read_conn = handle.join_now().await.and_then(Result::ok);
                            }
                            // Still dialling: leave it running, carry on
                            // without a read-ahead this cycle.
                            Some(handle) => dialing = Some(handle),
                            None => {
                                let client = client.clone();
                                dialing = Some(AbortOnDrop::new(tokio::spawn(async move {
                                    client.multiplexed_conn().await
                                })));
                            }
                        }
                    }
                    // Taken before the read is issued rather than when its
                    // reply lands: the age bound this arms is an upper bound,
                    // so the conservative end is the earlier instant, and the
                    // reply is parked in `prefetched` for however long the
                    // flush it travels with runs.
                    let read_at = Instant::now();
                    let (ahead, flushed) = tokio::join!(
                        async {
                            match read_conn.as_mut() {
                                Some(read_conn) if read_ahead_armed => {
                                    read_ahead_batch(
                                        read_conn,
                                        stream,
                                        &group,
                                        &consumer,
                                        max_batch_size,
                                    )
                                    .await
                                }
                                _ => ReadAhead::Nothing,
                            }
                        },
                        flush_redis_batch(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &consumer,
                            handler.as_ref(),
                            ctx.as_ref(),
                            &mut batch,
                            handler_timeout,
                            handler_timeout_outcome.clone(),
                            &topic_arc,
                            group_arc.as_deref(),
                            &mut redelivery_backoff,
                            &mut replay_cursor,
                            &shutdown,
                        ),
                    );
                    match flushed? {
                        FlushOutcome::ShutdownDuringBackoff => return Ok(()),
                        FlushOutcome::Flushed => {}
                    }
                    // A failed read-ahead takes its connection with it: nothing
                    // else uses that socket, so a dead one would otherwise
                    // leave this loop quietly un-pipelined for the rest of the
                    // reconnect cycle. The next arming dials a fresh one.
                    if matches!(ahead, ReadAhead::Failed) {
                        read_conn = None;
                    }
                    // Entries are dropped rather than ingested when the flush
                    // armed a replay: `Redeliver` leaves the whole batch
                    // pending and sets the cursor to "0", so the replay read
                    // returns these entries as well — ingesting them here too
                    // would hand the same entries to the handler twice within
                    // one process.
                    prefetched = match ahead {
                        ReadAhead::Entries(entries) if replay_cursor.is_none() => {
                            Some((read_at, entries))
                        }
                        _ => None,
                    };
                    deadline = None;
                    continue;
                }

                if shutdown.is_cancelled() {
                    // Settle what the read-ahead already took out of the
                    // stream instead of leaving it pending for the reaper: a
                    // graceful stop flushes what this loop holds, and the
                    // read-ahead makes "holds" include entries fetched while
                    // the previous flush ran.
                    if let Some((read_at, entries)) = prefetched.take() {
                        ingest_entries::<T>(
                            &mut conn,
                            stream,
                            &group,
                            &consumer,
                            entries,
                            &topic_arc,
                            group_arc.as_deref(),
                            max_message_size,
                            &mut batch,
                            &mut deadline,
                            max_batch_age,
                            read_at,
                        )
                        .await?;
                    }
                    let _ = flush_redis_batch(
                        &mut conn,
                        topology,
                        stream,
                        &group,
                        &consumer,
                        handler.as_ref(),
                        ctx.as_ref(),
                        &mut batch,
                        handler_timeout,
                        handler_timeout_outcome.clone(),
                        &topic_arc,
                        group_arc.as_deref(),
                        &mut redelivery_backoff,
                        &mut replay_cursor,
                        &shutdown,
                    )
                    .await?;
                    return Ok(());
                }

                // The read-ahead's reply stands in for this iteration's read.
                // Always a live (`>`) read of at most `max_batch_size`
                // entries into a buffer the flush that issued it emptied, so
                // it needs no headroom recomputation and never touches
                // `replay_cursor`.
                if let Some((read_at, entries)) = prefetched.take() {
                    ingest_entries::<T>(
                        &mut conn,
                        stream,
                        &group,
                        &consumer,
                        entries,
                        &topic_arc,
                        group_arc.as_deref(),
                        max_message_size,
                        &mut batch,
                        &mut deadline,
                        max_batch_age,
                        read_at,
                    )
                    .await?;
                    continue;
                }

                let headroom = batch_headroom(max_batch_size, batch.len());
                let is_replay = replay_cursor.is_some();

                let mut cmd = redis::cmd("XREADGROUP");
                cmd.arg("GROUP").arg(&group).arg(&consumer).arg("COUNT").arg(headroom);
                if !is_replay {
                    let block_ms = batch_block_ms(deadline, Instant::now());
                    cmd.arg("BLOCK").arg(block_ms);
                }
                cmd.arg("STREAMS").arg(stream);
                cmd.arg(replay_cursor.as_deref().unwrap_or(">"));

                let read_fut = conn.query(&mut cmd);
                let raw_reply: redis::Value = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        let _ = flush_redis_batch(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &consumer,
                            handler.as_ref(),
                            ctx.as_ref(),
                            &mut batch,
                            handler_timeout,
                            handler_timeout_outcome.clone(),
                            &topic_arc,
                            group_arc.as_deref(),
                            &mut redelivery_backoff,
                            &mut replay_cursor,
                            &shutdown,
                        ).await?;
                        return Ok(());
                    }
                    result = read_fut => match result {
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
                            tracing::warn!(error = %e, stream, "batch XREADGROUP failed");
                            return Err(e);
                        }
                    }
                };

                // When these entries left the stream. Unlike the read-ahead's
                // instant this is taken when the reply lands, not when the
                // read was issued: this read `BLOCK`s, so the entries need
                // not have existed when it went out.
                let read_at = Instant::now();
                let entries = parse_xreadgroup_reply(raw_reply, headroom);

                if is_replay {
                    replay_cursor = next_replay_cursor(&entries);
                }

                ingest_entries::<T>(
                    &mut conn,
                    stream,
                    &group,
                    &consumer,
                    entries,
                    &topic_arc,
                    group_arc.as_deref(),
                    max_message_size,
                    &mut batch,
                    &mut deadline,
                    max_batch_age,
                    read_at,
                )
                .await?;

                if let Some(d) = deadline
                    && Instant::now() >= d
                {
                    match flush_redis_batch(
                        &mut conn,
                        topology,
                        stream,
                        &group,
                        &consumer,
                        handler.as_ref(),
                        ctx.as_ref(),
                        &mut batch,
                        handler_timeout,
                        handler_timeout_outcome.clone(),
                        &topic_arc,
                        group_arc.as_deref(),
                        &mut redelivery_backoff,
                        &mut replay_cursor,
                        &shutdown,
                    )
                    .await?
                    {
                        FlushOutcome::ShutdownDuringBackoff => return Ok(()),
                        FlushOutcome::Flushed => {}
                    }
                    deadline = None;
                }
            }
        }
    })
    .await
}

#[cfg(test)]
mod batch_impl_tests {
    use super::*;

    #[test]
    fn headroom_is_the_remaining_room_in_the_batch() {
        assert_eq!(batch_headroom(10, 0), 10);
        assert_eq!(batch_headroom(10, 7), 3);
        assert_eq!(batch_headroom(10, 10), 0);
        // Saturating: a caller bug must clamp to zero, never wrap.
        assert_eq!(batch_headroom(10, 11), 0);
    }

    #[test]
    fn block_ms_is_block_ms_when_no_deadline_is_armed() {
        assert_eq!(batch_block_ms(None, Instant::now()), BLOCK_MS);
    }

    #[test]
    fn block_ms_is_the_smaller_of_block_ms_and_remaining_time() {
        let now = Instant::now();
        let deadline = now + Duration::from_millis(500);
        assert_eq!(batch_block_ms(Some(deadline), now), 500);

        // Remaining time above BLOCK_MS clamps down to BLOCK_MS.
        let far_deadline = now + Duration::from_secs(3600);
        assert_eq!(batch_block_ms(Some(far_deadline), now), BLOCK_MS);
    }

    #[test]
    fn block_ms_is_floored_at_one_when_the_deadline_has_already_elapsed() {
        let now = Instant::now();
        let past_deadline = now - Duration::from_millis(50);
        // BLOCK 0 means "block forever" in Redis — must never be emitted.
        assert_eq!(batch_block_ms(Some(past_deadline), now), 1);
    }

    #[test]
    fn replay_cursor_advances_to_the_last_returned_entry_id() {
        let entries = vec![
            ("1-1".to_owned(), vec![]),
            ("1-2".to_owned(), vec![]),
            ("1-3".to_owned(), vec![]),
        ];
        assert_eq!(next_replay_cursor(&entries), Some("1-3".to_owned()));
    }

    #[test]
    fn replay_cursor_is_none_on_an_empty_reply() {
        assert_eq!(next_replay_cursor(&[]), None);
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
            (
                X_DEATH_REASON.to_string(),
                "max_retries_exceeded".to_string(),
            ),
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

    /// `partition_entry_fields` -> `merged_entry_fields` must round-trip every
    /// field of the original entry. Losing the user half here is what stripped
    /// headers from retried and dead-lettered messages.
    #[test]
    fn merged_entry_fields_round_trips_a_partitioned_entry() {
        let fields_vec = vec![
            (PAYLOAD_FIELD.to_string(), "data".to_string()),
            (X_RETRY_COUNT.to_string(), "2".to_string()),
            ("x-trace-id".to_string(), "trace-1".to_string()),
            ("tenant".to_string(), "acme".to_string()),
        ];
        let (internal, user) = partition_entry_fields(fields_vec.clone());

        let mut merged: Vec<(String, String)> = merged_entry_fields(&internal, &user, None)
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        merged.sort();
        let mut expected = fields_vec;
        expected.sort();
        assert_eq!(merged, expected);
    }

    #[test]
    fn merged_entry_fields_drops_only_the_excluded_key() {
        let (internal, user) = partition_entry_fields(vec![
            (PAYLOAD_FIELD.to_string(), "data".to_string()),
            (X_RETRY_COUNT.to_string(), "2".to_string()),
            ("x-trace-id".to_string(), "trace-1".to_string()),
        ]);

        let merged: HashMap<&str, &str> =
            merged_entry_fields(&internal, &user, Some(X_RETRY_COUNT)).collect();
        assert_eq!(merged.len(), 2);
        assert!(!merged.contains_key(X_RETRY_COUNT));
        assert_eq!(merged.get(PAYLOAD_FIELD), Some(&"data"));
        assert_eq!(merged.get("x-trace-id"), Some(&"trace-1"));
    }

    #[test]
    fn merged_entry_fields_preserves_user_headers_when_internal_is_empty() {
        let internal = HashMap::new();
        let user = HashMap::from([("x-trace-id".to_string(), "trace-1".to_string())]);
        let merged: Vec<(&str, &str)> = merged_entry_fields(&internal, &user, None).collect();
        assert_eq!(merged, vec![("x-trace-id", "trace-1")]);
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
