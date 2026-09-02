use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::client::{Envelope, InMemoryBroker, QueueState};
use super::constants::{
    X_DEATH_COUNT, X_DEATH_REASON, X_MESSAGE_ID, X_ORIGINAL_QUEUE, X_RETRY_COUNT, X_SEQUENCE_KEY,
};
use super::topology::InMemoryTopologyDeclarer;
use crate::backend::ConsumerOptionsInner;
use crate::backend::batch_consumer::{
    BatchConsumerOptionsInner, BatchSettlement, RejectSettlement, TerminalDiscard,
    batch_redelivery_backoff, invoke_batch_handler, reject_settlement, settle_batch_outcome,
    validate_batch_topic,
};
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{
    PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome, hold_index,
};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::{QueueTopology, SequenceFailure};
use crate::{ConsumerOptions, InMemory};

/// Consumes messages from an [`InMemoryBroker`] queue.
#[derive(Clone)]
pub struct InMemoryConsumer {
    broker: InMemoryBroker,
}

impl InMemoryConsumer {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self { broker }
    }
}

impl InMemoryConsumer {
    pub fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<InMemory>,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_concurrent::<T, H>(self.broker.clone(), handler, ctx, options.into_inner())
    }

    pub fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<InMemory>,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        self.run_fifo_with_inner::<T, H>(handler, ctx, options.into_inner())
    }

    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions<InMemory>,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        run_fifo_until_timeout_impl::<T, H, S>(
            self.broker.clone(),
            handler,
            ctx,
            options.into_inner(),
            signal,
            drain_timeout,
        )
        .await
    }

    pub(crate) fn run_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_concurrent::<T, H>(self.broker.clone(), handler, ctx, options)
    }

    pub(crate) fn run_fifo_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        run_fifo_impl::<T, H>(self.broker.clone(), handler, ctx, options)
    }

    pub(crate) fn spawn_fifo_shards_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        spawn_fifo_shards::<T, H>(self.broker.clone(), handler, ctx, options)
    }

    pub fn run_dlq<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_dlq_impl::<T, H>(self.broker.clone(), handler, ctx)
    }

    pub(crate) fn run_broadcast_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_broadcast_impl::<T, H>(self.broker.clone(), handler, ctx, options)
    }

    pub(crate) fn run_batch_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        run_batch_impl::<T, H>(self.broker.clone(), handler, ctx, options)
    }
}

// ---------------------------------------------------------------------------
// Broadcast — one ephemeral subscription per call
// ---------------------------------------------------------------------------

/// Register this call's own subscription to `T`'s topic and run the ordinary
/// concurrent loop against its private buffer.
///
/// Deliver-new and "nothing survives" are both structural here rather than
/// enforced: the subscription is created after this future starts, so it cannot
/// see earlier publishes, and its registry entry is owned by a guard on this
/// future's stack, so it is gone whether the loop returns, is cancelled, or is
/// aborted outright.
async fn run_broadcast_impl<T, H>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    if !topology.broadcast() {
        return Err(ShoveError::Topology(format!(
            "topic '{}' is not a broadcast topology; a broadcast subscription to it \
             would receive nothing, because publishes go to the shared queue",
            topology.queue()
        )));
    }

    let subscription = broker.broadcast_subscribe(topology.queue());
    let queue = Arc::clone(subscription.queue());
    let subscription_closed = subscription.closed_token().clone();
    let result = run_concurrent_on::<T, H>(
        broker,
        queue,
        handler,
        ctx,
        options,
        Some(subscription_closed),
    )
    .await;
    // Explicit rather than implicit: dropping the guard is what deregisters
    // this subscriber, and it must happen after the loop, not before it.
    drop(subscription);
    result
}

// ---------------------------------------------------------------------------
// Main loop — concurrent with in-order routing
// ---------------------------------------------------------------------------

async fn run_concurrent<T, H>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let queue = broker.lookup(T::topology().queue())?;
    run_concurrent_on::<T, H>(broker, queue, handler, ctx, options, None).await
}

/// The concurrent delivery loop, against an already-resolved queue.
///
/// Split out from [`run_concurrent`] so the broadcast path can run the very
/// same loop against a subscription's private buffer — which has no name to
/// look up — instead of the shared, declared queue.
async fn run_concurrent_on<T, H>(
    broker: InMemoryBroker,
    queue: Arc<QueueState>,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
    subscription_closed: Option<CancellationToken>,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);

    let prefetch = options.prefetch_count.max(1) as usize;
    let shutdown = options.shutdown.clone();
    let broker_shutdown = broker.shutdown_token().clone();

    // The JoinSet task itself is the panic boundary — `join_next_with_id`
    // surfaces panics as `JoinError` without killing the consumer loop. The
    // envelope lives in a sidecar map keyed by `tokio::task::Id` so we can
    // recover it (for retry routing) when a handler panics.
    let mut inflight: JoinSet<(u64, Handled)> = JoinSet::new();
    let mut envelopes: HashMap<tokio::task::Id, (u64, Envelope)> = HashMap::new();
    let mut pending: BTreeMap<u64, (Envelope, Handled)> = BTreeMap::new();
    let mut next_ticket: u64 = 0;
    let mut next_route: u64 = 0;

    loop {
        // Pull messages up to prefetch.
        while inflight.len() < prefetch
            && !shutdown.is_cancelled()
            && !broker_shutdown.is_cancelled()
        {
            let env_opt = queue.buffer.lock().await.pop_front();
            let Some(env) = env_opt else { break };

            queue.space.notify_one();
            queue.in_flight.fetch_add(1, Ordering::Release);
            options.processing.store(true, Ordering::Release);

            let ticket = next_ticket;
            next_ticket += 1;

            let handler_clone = Arc::clone(&handler);
            let ctx_clone = Arc::clone(&ctx);
            let max_size = options.max_message_size;
            let timeout_opt = options.handler_timeout;
            let timeout_outcome = options.handler_timeout_outcome.clone();
            let env_for_task = env.clone();
            let group = options.consumer_group.clone();

            let abort = inflight.spawn(async move {
                let handled = invoke_handler::<T, H>(
                    handler_clone,
                    ctx_clone,
                    &env_for_task,
                    max_size,
                    timeout_opt,
                    timeout_outcome,
                    T::topology().queue(),
                    group.as_deref(),
                )
                .await;
                (ticket, handled)
            });
            envelopes.insert(abort.id(), (ticket, env));
        }

        if shutdown.is_cancelled() || broker_shutdown.is_cancelled() {
            break;
        }

        let ready_notified = queue.ready.notified();
        tokio::pin!(ready_notified);

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = broker_shutdown.cancelled() => break,
            join = inflight.join_next_with_id(), if !inflight.is_empty() => {
                match join {
                    Some(Ok((task_id, (ticket, handled)))) => {
                        if let Some((_, env)) = envelopes.remove(&task_id) {
                            pending.insert(ticket, (env, handled));
                            drain_pending(
                                &broker,
                                topology,
                                &queue,
                                &mut pending,
                                &mut next_route,
                                &options,
                                subscription_closed.as_ref(),
                            )
                            .await;
                            if inflight.is_empty() {
                                options.processing.store(false, Ordering::Release);
                            }
                        }
                    }
                    Some(Err(join_err)) => {
                        let task_id = join_err.id();
                        if let Some((ticket, env)) = envelopes.remove(&task_id) {
                            tracing::warn!(error = ?join_err, ticket, "handler task panicked — retrying message");
                            pending.insert(ticket, (env, (Outcome::Retry, None)));
                            drain_pending(
                                &broker,
                                topology,
                                &queue,
                                &mut pending,
                                &mut next_route,
                                &options,
                                subscription_closed.as_ref(),
                            )
                            .await;
                            if inflight.is_empty() {
                                options.processing.store(false, Ordering::Release);
                            }
                        } else {
                            tracing::error!(error = ?join_err, "consumer task join error without tracked envelope");
                        }
                    }
                    None => {}
                }
            }
            _ = &mut ready_notified, if inflight.len() < prefetch => continue,
        }
    }

    // Graceful drain.
    while let Some(res) = inflight.join_next_with_id().await {
        match res {
            Ok((task_id, (ticket, handled))) => {
                if let Some((_, env)) = envelopes.remove(&task_id) {
                    pending.insert(ticket, (env, handled));
                }
            }
            Err(join_err) => {
                let task_id = join_err.id();
                if let Some((ticket, env)) = envelopes.remove(&task_id) {
                    tracing::warn!(error = ?join_err, ticket, "handler task panicked during drain — retrying message");
                    pending.insert(ticket, (env, (Outcome::Retry, None)));
                }
            }
        }
    }
    drain_pending(
        &broker,
        topology,
        &queue,
        &mut pending,
        &mut next_route,
        &options,
        subscription_closed.as_ref(),
    )
    .await;
    options.processing.store(false, Ordering::Release);
    Ok(())
}

async fn drain_pending(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    queue: &Arc<QueueState>,
    pending: &mut BTreeMap<u64, (Envelope, Handled)>,
    next_route: &mut u64,
    options: &ConsumerOptionsInner,
    subscription_closed: Option<&CancellationToken>,
) {
    while let Some((env, (outcome, pre_handler_reason))) = pending.remove(next_route) {
        route_outcome(
            broker,
            topology,
            DeliverySource {
                queue,
                subscription_closed,
            },
            env,
            outcome,
            pre_handler_reason,
            options,
        )
        .await;
        queue.in_flight.fetch_sub(1, Ordering::Release);
        *next_route += 1;
    }
}

// ---------------------------------------------------------------------------
// Sequenced (FIFO) loop
// ---------------------------------------------------------------------------

/// Spawn one task per shard and return the join handles.
///
/// The `pub(crate)` visibility is required because the consumer group
/// (Phase 2, Task 14) will call this from `consumer_group.rs`.
///
/// InMemory shard tasks return `()` internally (errors are handled inside
/// `run_fifo_shard`), so each task is wrapped to produce `Result<()>` so
/// the handle type is uniform with other backends.
pub(crate) fn spawn_fifo_shards<T, H>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
where
    T: SequencedTopic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    let seq = topology.sequencing().ok_or_else(|| {
        ShoveError::Topology(format!(
            "run_fifo called on topic {} without sequencing config",
            topology.queue()
        ))
    })?;

    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);
    let n_shards = seq.routing_shards();
    let on_failure = seq.on_failure();

    let mut shards: Vec<(String, Arc<QueueState>)> = Vec::with_capacity(n_shards as usize);
    for shard in 0..n_shards {
        let name = InMemoryTopologyDeclarer::shard_queue_name(topology.queue(), shard);
        let state = broker.lookup(&name)?;
        shards.push((name, state));
    }

    // Shared busy counter across all shards. `options.processing` reflects
    // "any shard is currently invoking a handler", so the autoscaler can't
    // shrink a pool while a sibling shard is in the middle of a message.
    let busy = Arc::new(AtomicUsize::new(0));

    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> =
        Vec::with_capacity(n_shards as usize);
    for (shard_name, shard) in shards {
        let broker = broker.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let options = options.clone();
        let busy = Arc::clone(&busy);
        handles.push(tokio::spawn(async move {
            run_fifo_shard::<T, H>(
                broker, shard_name, shard, topology, on_failure, handler, ctx, options, busy,
            )
            .await;
            Result::<()>::Ok(())
        }));
    }

    Ok(handles)
}

async fn run_fifo_impl<T, H>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
) -> Result<()>
where
    T: SequencedTopic,
    H: MessageHandler<T>,
{
    let handles = spawn_fifo_shards::<T, H>(broker, handler, ctx, options)?;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!("sequenced shard task failed: {e}"),
            Err(e) => tracing::error!("sequenced shard task panicked: {e}"),
        }
    }
    Ok(())
}

async fn run_fifo_until_timeout_impl<T, H, S>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
    signal: S,
    drain_timeout: Duration,
) -> SupervisorOutcome
where
    T: SequencedTopic,
    H: MessageHandler<T>,
    S: Future<Output = ()> + Send + 'static,
{
    let shutdown = options.shutdown.clone();
    let handles = match spawn_fifo_shards::<T, H>(broker, handler, ctx, options) {
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

#[allow(clippy::too_many_arguments)]
async fn run_fifo_shard<T, H>(
    broker: InMemoryBroker,
    shard_name: String,
    shard: Arc<QueueState>,
    topology: &'static QueueTopology,
    on_failure: SequenceFailure,
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    options: ConsumerOptionsInner,
    busy: Arc<AtomicUsize>,
) where
    T: SequencedTopic,
    H: MessageHandler<T>,
{
    let poisoned = PoisonedKeys::new(on_failure);
    let shutdown = options.shutdown.clone();
    let broker_shutdown = broker.shutdown_token().clone();

    'outer: loop {
        if shutdown.is_cancelled() || broker_shutdown.is_cancelled() {
            return;
        }

        // Acquire SAC permit.
        let _permit = tokio::select! {
            permit = shard.sac.lock() => permit,
            _ = shutdown.cancelled() => return,
            _ = broker_shutdown.cancelled() => return,
        };

        loop {
            if shutdown.is_cancelled() || broker_shutdown.is_cancelled() {
                return;
            }

            // Pop or await.
            let env = match pop_or_wait(&shard, &shutdown, &broker_shutdown).await {
                Some(e) => e,
                None => continue 'outer,
            };

            shard.in_flight.fetch_add(1, Ordering::Release);
            let finish =
                |shard: &QueueState, busy: &AtomicUsize, options: &ConsumerOptionsInner| {
                    shard.in_flight.fetch_sub(1, Ordering::Release);
                    if busy.fetch_sub(1, Ordering::AcqRel) == 1 {
                        options.processing.store(false, Ordering::Release);
                    }
                };
            busy.fetch_add(1, Ordering::AcqRel);
            options.processing.store(true, Ordering::Release);

            let key = env.headers.get(X_SEQUENCE_KEY).cloned().unwrap_or_default();

            let skip_handler = poisoned.is_poisoned(&key);

            let outcome = if skip_handler {
                tracing::debug!(
                    shard = %shard_name,
                    %key,
                    "sequence key poisoned — routing message to DLQ without invoking handler"
                );
                (Outcome::Reject, None)
            } else {
                let raw = invoke_handler_caught::<T, H>(
                    Arc::clone(&handler),
                    Arc::clone(&ctx),
                    &env,
                    options.max_message_size,
                    options.handler_timeout,
                    options.handler_timeout_outcome.clone(),
                    &shutdown,
                    &broker_shutdown,
                    T::topology().queue(),
                    options.consumer_group.as_deref(),
                )
                .await;
                match raw {
                    None => {
                        // Shutdown drop: intentionally not counted — see
                        // `metrics::FailReason`.
                        warn_shutdown_drop(
                            &shard_name,
                            &key,
                            message_id_of(&env.headers),
                            "shutdown cancelled an in-flight sequenced handler — message dropped",
                        );
                        finish(&shard, &busy, &options);
                        return;
                    }
                    Some((Outcome::Defer, _)) => {
                        tracing::warn!(
                            shard = %shard_name,
                            "Defer is not supported on sequenced consumers — treating as Retry"
                        );
                        (Outcome::Retry, None)
                    }
                    Some(other) => other,
                }
            };

            let (outcome, pre_handler_reason) = outcome;
            match outcome {
                Outcome::Ack => {}
                Outcome::Retry => {
                    let retry_count = get_retry_count(&env.headers);
                    if retry_count >= options.max_retries {
                        // Burning the last retry is an independent failure: this
                        // message reached the handler on its own merits.
                        let pending = metrics::record_terminal(
                            topology.queue(),
                            options.consumer_group.as_deref(),
                            metrics::FailReason::MaxRetriesExceeded,
                            topology.dlq().is_some(),
                        );
                        route_reject_sequenced(&broker, topology, env, &key, &poisoned, pending)
                            .await;
                    } else {
                        // Inline sleep (blocks this shard until republish completes).
                        let hold_queues = topology.hold_queues();
                        let delay = if hold_queues.is_empty() {
                            Duration::ZERO
                        } else {
                            hold_queues[hold_index(retry_count, hold_queues.len())].delay()
                        };

                        let mut new_env = env;
                        set_retry_count(&mut new_env.headers, retry_count + 1);
                        new_env.reset_delivery_count();

                        let cancelled = tokio::select! {
                            _ = tokio::time::sleep(delay) => false,
                            _ = shutdown.cancelled() => true,
                            _ = broker_shutdown.cancelled() => true,
                        };
                        if cancelled {
                            // Shutdown drop: intentionally not counted — see
                            // `metrics::FailReason`.
                            warn_shutdown_drop(
                                &shard_name,
                                &key,
                                message_id_of(&new_env.headers),
                                "shutdown cancelled a pending sequenced retry — message dropped",
                            );
                            finish(&shard, &busy, &options);
                            return;
                        }

                        // `enqueue` only fails once the broker's shutdown token
                        // is cancelled, so this is the same class as the two
                        // arms above — but it dropped the message without even
                        // a log line, which is what made it invisible.
                        // Shutdown drop: intentionally not counted — see
                        // `metrics::FailReason`.
                        let message_id = message_id_of(&new_env.headers).to_owned();
                        if broker.enqueue(&shard, new_env).await.is_err() {
                            warn_shutdown_drop(
                                &shard_name,
                                &key,
                                &message_id,
                                "broker shutdown rejected a sequenced retry re-enqueue — message dropped",
                            );
                            finish(&shard, &busy, &options);
                            return;
                        }
                    }
                }
                Outcome::Reject => {
                    // An oversize or undecodable message never reached the
                    // handler, so it is not a handler reject; report what
                    // actually happened to it.
                    let reason = pre_handler_reason.unwrap_or(metrics::FailReason::Rejected);
                    let queue = topology.queue();
                    let group = options.consumer_group.as_deref();
                    let has_dlq = topology.dlq().is_some();
                    // `skip_handler` synthesises this `Reject` for a delivery
                    // whose key an earlier failure already poisoned.
                    // Cascade: intentionally not counted — see `metrics::FailReason`.
                    let pending = if skip_handler {
                        metrics::pending_discard(queue, group, reason, has_dlq)
                    } else {
                        metrics::record_terminal(queue, group, reason, has_dlq)
                    };
                    route_reject_sequenced(&broker, topology, env, &key, &poisoned, pending).await;
                }
                Outcome::Defer => unreachable!("Defer normalized to Retry above"),
            }

            finish(&shard, &busy, &options);
        }
    }
}

async fn pop_or_wait(
    queue: &QueueState,
    shutdown: &CancellationToken,
    broker_shutdown: &CancellationToken,
) -> Option<Envelope> {
    loop {
        let notified = queue.ready.notified();
        tokio::pin!(notified);
        {
            let mut buf = queue.buffer.lock().await;
            if let Some(env) = buf.pop_front() {
                queue.space.notify_one();
                return Some(env);
            }
        }
        tokio::select! {
            _ = &mut notified => continue,
            _ = shutdown.cancelled() => return None,
            _ = broker_shutdown.cancelled() => return None,
        }
    }
}

/// Terminal routing for the sequenced loop. Every sequenced path that gives up
/// on a message — retry-budget exhaustion, an explicit `Reject`, and the
/// FailAll cascade onto a poisoned key — funnels through here. The unsequenced
/// loop's equivalent is the `RetryDecision::Dlq` arm of `route_outcome`.
///
/// The caller supplies `pending` because only the caller knows whether this
/// retirement is an independent failure ([`metrics::record_terminal`]) or a
/// cascade that must not be counted as one ([`metrics::pending_discard`]).
async fn route_reject_sequenced(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    env: Envelope,
    key: &str,
    poisoned: &PoisonedKeys,
    pending: metrics::PendingDiscard,
) {
    resolve_reject(
        route_reject(broker, topology, env, "rejected").await,
        pending,
    );
    poisoned.poison(key);
}

// ---------------------------------------------------------------------------
// DLQ loop
// ---------------------------------------------------------------------------

async fn run_dlq_impl<T, H>(broker: InMemoryBroker, handler: H, ctx: H::Context) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    let dlq_name = topology.dlq().ok_or_else(|| {
        ShoveError::Topology(format!(
            "run_dlq called on topic {} without DLQ",
            topology.queue()
        ))
    })?;
    let dlq = broker.lookup(dlq_name)?;
    let shutdown = broker.shutdown_token().clone();
    // DLQ consumer uses default options for the payload-size validator. Same
    // pattern as the RabbitMQ DLQ loop (`run_dlq`).
    let options = ConsumerOptionsInner::defaults_with_shutdown(shutdown.clone());

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let env = match pop_or_wait(&dlq, &shutdown, &shutdown).await {
            Some(e) => e,
            None => return Ok(()),
        };

        dlq.in_flight.fetch_add(1, Ordering::Release);

        // Before the size gate, exactly as `prepare_message` places it on the
        // main loop. Labelled with the SOURCE topic rather than the DLQ name,
        // and with no consumer group (the DLQ loop builds default options):
        // Redis already drains its DLQ through `run_stream_loop`, which labels
        // every metric `topology.queue()` whichever stream it reads, so a DLQ
        // name here would make `topic` mean two different things depending on
        // the backend and would stop a per-topic size profile summing across
        // the main and DLQ paths.
        metrics::record_message_size(
            topology.queue(),
            options.consumer_group.as_deref(),
            env.payload.len(),
        );

        if let Err(e) = options.validate_payload_message_size(env.payload.len()) {
            tracing::warn!(
                error = %e,
                queue = dlq_name,
                "oversized DLQ message — discarding"
            );
            dlq.in_flight.fetch_sub(1, Ordering::Release);
            continue;
        }

        let message: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode_owned(
            env.payload.clone(),
        ) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, queue = dlq_name, "failed to deserialize DLQ message, discarding");
                dlq.in_flight.fetch_sub(1, Ordering::Release);
                continue;
            }
        };

        let dead = dead_metadata_from(&env);
        handler.handle_dead(message, dead, &ctx).await;
        dlq.in_flight.fetch_sub(1, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Outcome routing
// ---------------------------------------------------------------------------

async fn route_outcome(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    source: DeliverySource<'_>,
    env: Envelope,
    outcome: Outcome,
    // `Some` when the message was rejected before the handler ran; overrides
    // the reason derived below, which cannot tell that case from a handler
    // `Reject`. See [`prepare_message`].
    pre_handler_reason: Option<metrics::FailReason>,
    options: &ConsumerOptionsInner,
) {
    let retry_count = get_retry_count(&env.headers);
    match decide_retry(&outcome, retry_count, options.max_retries) {
        RetryDecision::Ack => {}
        RetryDecision::Dlq { reason } => {
            let fail_reason = pre_handler_reason.unwrap_or(match reason {
                "rejected" => metrics::FailReason::Rejected,
                _ => metrics::FailReason::MaxRetriesExceeded,
            });
            let pending = metrics::record_terminal(
                topology.queue(),
                options.consumer_group.as_deref(),
                fail_reason,
                topology.dlq().is_some(),
            );
            // In-memory's DLQ enqueue path is `route_reject` for both
            // `Reject` and `max_retries_exceeded`; it does not differentiate
            // the reason beyond the metric recorded above.
            resolve_reject(
                route_reject(broker, topology, env, "rejected").await,
                pending,
            );
        }
        RetryDecision::Hold { increment } => {
            schedule_redelivery(
                broker,
                topology,
                source.queue,
                env,
                increment,
                source.subscription_closed,
            );
        }
    }
}

/// The queue a delivery came from and, for a private broadcast queue, the
/// lifecycle signal that makes detached redelivery stop retaining it.
struct DeliverySource<'a> {
    queue: &'a Arc<QueueState>,
    subscription_closed: Option<&'a CancellationToken>,
}

/// Apply the pending discard given where [`route_reject`] actually put the
/// message.
///
/// The in-memory backend owns the envelope outright, so a rejected message is
/// retired the moment this returns — there is no broker ack that can fail
/// afterwards. What can fail is the DLQ hand-off, and a message that was
/// supposed to be dead-lettered but never arrived is data loss regardless of
/// what the topology declares.
fn resolve_reject(reached_dlq: bool, pending: metrics::PendingDiscard) {
    if reached_dlq {
        pending.confirm();
    } else {
        pending.confirm_lost();
    }
}

fn schedule_redelivery(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    source: &Arc<QueueState>,
    env: Envelope,
    increment: bool,
    subscription_closed: Option<&CancellationToken>,
) {
    let retry_count = get_retry_count(&env.headers);
    let hold_queues = topology.hold_queues();
    let delay = if hold_queues.is_empty() {
        if !increment {
            tracing::warn!(
                queue = topology.queue(),
                "Defer with no hold queues configured — falling back to zero delay"
            );
        }
        Duration::ZERO
    } else if increment {
        hold_queues[hold_index(retry_count, hold_queues.len())].delay()
    } else {
        hold_queues[0].delay()
    };

    let mut env = env;
    if increment {
        // `Retry` is a republish on every real backend, which resets the
        // broker's own attempt counter. Model that here so handlers written
        // against the in-process broker behave the same in production.
        set_retry_count(&mut env.headers, retry_count + 1);
        env.reset_delivery_count();
    } else {
        // `Defer` is a nak-in-place on JetStream — same message, one more
        // attempt. This is the case `retry_count` deliberately cannot express.
        env.mark_redelivery();
    }

    let broker_clone = broker.clone();
    let broker_shutdown = broker.shutdown_token().clone();
    let main_queue = topology.queue().to_string();
    // A broadcast subscription's buffer is private and unnamed, so redelivery
    // resolves it by holding the `Arc` rather than by name. Redelivering to the
    // topic would be wrong even if it were addressable: a `Defer` belongs to
    // the one subscriber that deferred, not to the whole fan-out. Named
    // topologies keep the historical lookup, whose failure case (queue removed
    // mid-backoff) still applies to them.
    let target: Option<Arc<QueueState>> = topology.broadcast().then(|| Arc::clone(source));
    let subscription_closed = subscription_closed.cloned();

    // Every `return` below destroys the message. None is counted — see
    // `metrics::FailReason` for why in-process broker drops stay log-only.
    tokio::spawn(async move {
        let message_id = message_id_of(&env.headers).to_owned();
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = broker_shutdown.cancelled() => {
                tracing::warn!(
                    queue = %main_queue,
                    %message_id,
                    "broker shutdown cancelled a pending redelivery — message dropped"
                );
                return;
            }
            _ = subscription_cancelled(&subscription_closed) => {
                tracing::debug!(
                    queue = %main_queue,
                    %message_id,
                    "subscription closed during redelivery backoff"
                );
                return;
            }
        }
        // Re-check shutdown after sleep — the broker may have shut down while
        // awaiting capacity.
        if broker_shutdown.is_cancelled() {
            tracing::warn!(
                queue = %main_queue,
                %message_id,
                "broker shut down during redelivery backoff — message dropped"
            );
            return;
        }
        let q = match target {
            Some(q) => q,
            None => {
                let Ok(q) = broker_clone.lookup(&main_queue) else {
                    tracing::warn!(
                        queue = %main_queue,
                        %message_id,
                        "redelivery target queue no longer exists — message dropped"
                    );
                    return;
                };
                q
            }
        };
        let redelivery_timeout = Duration::from_secs(30);
        let enqueue = tokio::time::timeout(redelivery_timeout, broker_clone.enqueue(&q, env));
        tokio::pin!(enqueue);
        let result = tokio::select! {
            result = &mut enqueue => result,
            _ = subscription_cancelled(&subscription_closed) => {
                tracing::debug!(
                    queue = %main_queue,
                    %message_id,
                    "subscription closed during redelivery enqueue"
                );
                return;
            }
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(
                    queue = %main_queue,
                    %message_id,
                    error = %e,
                    "redelivery enqueue failed — message dropped"
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    queue = %main_queue,
                    %message_id,
                    "redelivery enqueue timed out after {}s — message dropped",
                    redelivery_timeout.as_secs()
                );
            }
        }
    });
}

async fn subscription_cancelled(token: &Option<CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Dead-letter a rejected message, or drop it when there is nowhere to put it.
///
/// `reason` is stamped into [`X_DEATH_REASON`] verbatim — the single-message
/// and sequenced paths always pass `"rejected"`, but the batch path also
/// dead-letters a pre-handler drop (`"oversize"`, `"deserialize"`) under its
/// own reason, so the DLQ record says what actually happened rather than a
/// blanket "rejected".
///
/// Returns whether the message reached the DLQ. `false` covers all three ways
/// it can end up gone: no DLQ declared, a DLQ declared but absent from the
/// broker, and an enqueue that failed. Callers use this to decide whether the
/// message still exists somewhere before asserting a discard.
async fn route_reject(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    mut env: Envelope,
    reason: &str,
) -> bool {
    let Some(dlq_name) = topology.dlq() else {
        tracing::warn!(
            queue = topology.queue(),
            reason,
            "message rejected but no DLQ configured — discarding"
        );
        return false;
    };
    let Ok(dlq) = broker.lookup(dlq_name) else {
        tracing::error!(queue = dlq_name, "DLQ declared but not found in broker");
        return false;
    };

    env.headers
        .insert(X_DEATH_REASON.to_string(), reason.to_string());
    env.headers
        .insert(X_ORIGINAL_QUEUE.to_string(), topology.queue().to_string());
    let count = env
        .headers
        .get(X_DEATH_COUNT)
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    env.headers
        .insert(X_DEATH_COUNT.to_string(), count.to_string());

    if let Err(e) = broker.enqueue(&dlq, env).await {
        tracing::warn!(queue = dlq_name, error = %e, "DLQ enqueue failed — message lost");
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Batch consumption
// ---------------------------------------------------------------------------

/// Pop exactly one envelope, blocking until one is available.
///
/// Re-checks the buffer *before* awaiting `queue.ready.notified()` — a
/// `Notify` holds at most one permit, so N publishes that land while nothing
/// is waiting collapse to a single wake-up; checking first (then only
/// awaiting when genuinely empty) is what makes that collapsing harmless
/// instead of a missed wake-up.
///
/// The tail after acquiring the lock — pop, `space.notify_one`,
/// `in_flight.fetch_add` — is entirely synchronous, with no `.await` between
/// the pop and the return. That matters because this runs as one arm of
/// `run_batch_impl`'s outer `tokio::select!`: a losing arm can only cancel
/// this future at an `.await` point, so once the lock is held there is no
/// window where a pop is torn down half-applied (buffer emptied but
/// `in_flight` never incremented, or vice versa).
async fn pop_one(queue: &QueueState) -> Envelope {
    loop {
        let notified = queue.ready.notified();
        tokio::pin!(notified);
        {
            let mut buf = queue.buffer.lock().await;
            if let Some(env) = buf.pop_front() {
                queue.space.notify_one();
                queue.in_flight.fetch_add(1, Ordering::Release);
                return env;
            }
        }
        notified.await;
    }
}

/// One in-flight batch, built up across `run_batch_impl`'s pops and consumed
/// by `flush_inmemory_batch`. Mirrors Kafka's `BatchBuffer` (same concept, no
/// offsets): `messages`/`envelopes` are index-parallel — the envelope for
/// `messages[i]` is `envelopes[i]`, kept so a `Redeliver` settlement can put
/// the exact same envelope back rather than reconstructing one.
struct InMemoryBatch<T: Topic> {
    messages: Vec<(T::Message, MessageMetadata)>,
    envelopes: Vec<Envelope>,
    /// Pre-handler drops with an envelope worth keeping — only when the
    /// topology declares a DLQ, mirroring Kafka's `retain_raw` gate
    /// (`BatchBuffer::drop_message`): with nowhere to publish it, holding the
    /// envelope until the flush buys nothing, and the drop is as final as it
    /// will ever be the moment it happens.
    parked: Vec<(Envelope, metrics::FailReason)>,
    /// Every pre-handler drop, DLQ or not — `flush_len` has to count these so
    /// a flush window of nothing but poison still hits `max_batch_size`
    /// instead of growing for the whole `max_batch_age`.
    dropped: usize,
}

impl<T: Topic> InMemoryBatch<T> {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            envelopes: Vec::new(),
            parked: Vec::new(),
            dropped: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.dropped == 0
    }

    /// Messages plus every pre-handler drop — the quantity `max_batch_size`
    /// bounds, so an all-poison window still trips the size trigger.
    fn flush_len(&self) -> usize {
        self.messages.len() + self.dropped
    }

    fn push(&mut self, message: T::Message, metadata: MessageMetadata, envelope: Envelope) {
        self.messages.push((message, metadata));
        self.envelopes.push(envelope);
    }

    fn drop_message(&mut self, envelope: Option<Envelope>, reason: metrics::FailReason) {
        self.dropped += 1;
        if let Some(envelope) = envelope {
            self.parked.push((envelope, reason));
        }
    }

    fn clear(&mut self) {
        self.messages.clear();
        self.envelopes.clear();
        self.parked.clear();
        self.dropped = 0;
    }
}

/// Fields `flush_inmemory_batch` needs that do not change across flushes —
/// split out so the flush function's signature does not grow every time a new
/// one is needed, mirroring Kafka's `BatchFlushCtx`.
struct InMemoryFlushCtx<'a> {
    broker: &'a InMemoryBroker,
    topology: &'static QueueTopology,
    queue: &'a Arc<QueueState>,
    topic: &'a str,
    group: Option<&'a str>,
    shutdown: &'a CancellationToken,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
}

/// Publish every parked pre-handler drop to the DLQ (a no-op per entry when
/// the topology has none — `route_reject` logs and returns `false`), then
/// drop the list. Pre-handler drops are not part of the terminal-accounting
/// contract on this backend's batch path — mirroring Kafka's
/// `publish_pending_dlq` — so there is no discard to settle either way.
async fn publish_parked(
    flush: &InMemoryFlushCtx<'_>,
    parked: &mut Vec<(Envelope, metrics::FailReason)>,
) {
    for (envelope, reason) in parked.drain(..) {
        let _reached_dlq =
            route_reject(flush.broker, flush.topology, envelope, reason.as_label()).await;
    }
}

/// Release `n` popped-but-unsettled envelopes back to the queue's in-flight
/// count. Called exactly once per flush, for `flush_len()` envelopes — see
/// [`run_batch_impl`]'s doc for the balance invariant this maintains.
fn release_in_flight(queue: &QueueState, n: u64) {
    if n > 0 {
        queue.in_flight.fetch_sub(n, Ordering::Release);
    }
}

/// Hands the buffered batch to the handler and applies the single returned
/// [`Outcome`] via the shared [`settle_batch_outcome`] classifier — the same
/// three-way split Kafka's `flush_batch` uses, with InMemory's own mechanics
/// in each arm:
///
/// - `Commit`: every parked pre-handler drop is published to the DLQ (or
///   discarded, with none declared) and the batch's envelopes are gone for
///   good — there is no broker ack that can still fail afterwards, so
///   retirement is immediate. See `route_reject`'s doc and
///   [`crate::backend::broadcast::settle_broadcast_outcome`]'s sibling
///   rationale (`src/backend/broadcast.rs:134-142`) for why an in-process
///   backend can settle without waiting on a commit result.
/// - `DeadLetter`: terminal, exactly as every other reject path — every
///   *handled* message is individually routed to the DLQ (or discarded) via
///   [`crate::backend::batch_consumer::reject_settlement`], then the parked
///   pre-handler drops are published the same way `Commit` publishes them.
/// - `Redeliver`: the whole batch — handled envelopes plus parked ones —
///   goes back to the front of the queue via `InMemoryBroker::requeue_front`,
///   so the next pop sees it again instead of it being silently skipped.
///   Retry counters are **not** incremented: this models Kafka's seek-back,
///   not a republish, and there is no per-batch retry budget either way (see
///   [`crate::backend::batch_consumer::BatchSettlement`]'s doc).
///
/// Every branch — including the empty-batch one below — releases exactly
/// `flush_len()` envelopes back to `queue.in_flight`, whether or not each one
/// had a surviving envelope to redeliver: a no-DLQ pre-handler drop was
/// already destroyed the moment it was popped (nothing retains it — see
/// [`InMemoryBatch::drop_message`]), but its `in_flight` increment from
/// `pop_one` is still outstanding until this flush resolves it.
async fn flush_inmemory_batch<T, H>(
    flush: &InMemoryFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    batch: &mut InMemoryBatch<T>,
    redelivery_backoff: &mut Backoff,
) where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    let flush_len = batch.flush_len();
    if flush_len == 0 {
        return;
    }
    let batch_size = batch.messages.len();

    // Every message in this flush was dropped pre-handler. Nothing to hand
    // the handler, but the parked ones (if any survived to be parked) still
    // need publishing, and every popped envelope still needs its in-flight
    // slot released.
    if batch_size == 0 {
        publish_parked(flush, &mut batch.parked).await;
        release_in_flight(flush.queue, flush_len as u64);
        batch.clear();
        return;
    }

    let messages = std::mem::take(&mut batch.messages);
    let outcome = invoke_batch_handler(
        || handler.handle_batch(messages, ctx),
        flush.handler_timeout,
        flush.handler_timeout_outcome.clone(),
        flush.topic,
        flush.group,
        batch_size as u64,
    )
    .await;

    match settle_batch_outcome(&outcome) {
        BatchSettlement::Commit => {
            publish_parked(flush, &mut batch.parked).await;
            release_in_flight(flush.queue, flush_len as u64);
            *redelivery_backoff = batch_redelivery_backoff();
            batch.clear();
        }
        BatchSettlement::DeadLetter => {
            let has_dlq = flush.topology.dlq().is_some();
            let envelopes = std::mem::take(&mut batch.envelopes);
            for envelope in envelopes {
                let pending = metrics::record_terminal(
                    flush.topic,
                    flush.group,
                    metrics::FailReason::Rejected,
                    has_dlq,
                );
                let reached_dlq =
                    route_reject(flush.broker, flush.topology, envelope, "rejected").await;
                // InMemory owns the envelope outright and retires it the
                // instant `route_reject` returns — there is no separate
                // broker ack that could still fail, so the discard settles
                // now instead of waiting on one. Same rationale as
                // `settle_broadcast_outcome`'s immediate confirm
                // (`src/backend/broadcast.rs:134-142`).
                match reject_settlement(has_dlq, reached_dlq) {
                    RejectSettlement::InDlq => pending.survived(),
                    RejectSettlement::Retired => TerminalDiscard::Retired(pending).confirm(),
                    RejectSettlement::Lost => TerminalDiscard::Lost(pending).confirm(),
                }
            }
            publish_parked(flush, &mut batch.parked).await;
            release_in_flight(flush.queue, flush_len as u64);
            *redelivery_backoff = batch_redelivery_backoff();
            batch.clear();
        }
        BatchSettlement::Redeliver => {
            // `Backoff::next()` never returns `None` (see its doc) — the
            // fallback exists only so this cannot panic if that ever changed.
            let delay = redelivery_backoff.next().unwrap_or(Duration::from_secs(30));
            tracing::warn!(
                queue = flush.topology.queue(),
                batch_size,
                ?outcome,
                delay_ms = delay.as_millis() as u64,
                "batch handler returned a non-Ack outcome, redelivering the whole batch"
            );

            let mut to_requeue = std::mem::take(&mut batch.envelopes);
            to_requeue.extend(batch.parked.drain(..).map(|(env, _)| env));
            batch.clear();

            // Shutdown cuts only the delay, not the redelivery itself: a
            // wedged handler must not add the full backoff to every stop, but
            // the batch must still make it back to the queue rather than
            // being silently dropped.
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = flush.shutdown.cancelled() => {}
            }

            release_in_flight(flush.queue, flush_len as u64);
            flush.broker.requeue_front(flush.queue, to_requeue).await;
        }
    }
}

/// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl) for
/// InMemory. Mirrors Kafka's `run_batch_inner`'s *semantics* — size/age
/// flush triggers, pre-handler oversize/undecodable drops, one DLQ copy per
/// commit, whole-batch redelivery — over InMemory's own mechanics: a
/// `VecDeque` buffer instead of partition offsets, `requeue_front` instead of
/// a seek, immediate settlement instead of a commit round trip.
///
/// # Loop shape
///
/// Each iteration first flushes if `flush_len() >= max_batch_size` (checked
/// *before* selecting, so a size-triggered flush doesn't wait on the next
/// event) and otherwise `select!`s over shutdown, the broker's own shutdown,
/// the armed age deadline, and [`pop_one`] — which itself awaits only when
/// the buffer is genuinely empty (see its doc). At most one envelope is
/// popped per iteration, so the deadline and size cap are both re-checked
/// between pops rather than racing a batch of unbounded size past either.
///
/// # `in_flight` balance
///
/// Every envelope [`pop_one`] pops increments `queue.in_flight` exactly
/// once. It is decremented exactly once, by [`release_in_flight`], on
/// whichever settlement resolves it: `Commit` and `DeadLetter` release the
/// whole flush once their DLQ work is done, `Redeliver` releases it at
/// `requeue_front` (the envelope is back in the buffer, no longer "in
/// flight"). A pre-handler drop with no DLQ is destroyed immediately, before
/// any of these — see [`InMemoryBatch::drop_message`] — but its `in_flight`
/// slot is still only released when its batch's flush resolves, keeping the
/// increments and decrements paired 1:1 regardless of which path retired the
/// envelope.
///
/// Unlike the single-message consumer path, there is no `processing` flag to
/// mirror here: `BatchConsumerOptionsInner` carries none — batch consumption
/// is not registered with a coordinated-group registry, so nothing reads it.
/// `in_flight` is the signal an autoscaler or queue-depth sampler has for
/// this consumer.
///
/// `options.max_reconnect_attempts` is a documented no-op here: it exists on
/// `BatchConsumerOptionsInner` for trait-shape parity with Kafka, but there is
/// nothing to reconnect to for an in-process broker, exactly as on every
/// other InMemory consumer path.
pub(crate) async fn run_batch_impl<T, H>(
    broker: InMemoryBroker,
    handler: H,
    ctx: H::Context,
    options: BatchConsumerOptionsInner,
) -> Result<()>
where
    T: NotSequenced,
    H: BatchMessageHandler<T>,
{
    validate_batch_topic::<T>()?;
    let topology = T::topology();
    let queue_name = topology.queue();
    let queue = broker.lookup(queue_name)?;
    let broker_shutdown = broker.shutdown_token().clone();
    let shutdown = options.shutdown.clone();
    let group = options.consumer_group.clone();
    let topic: Arc<str> = Arc::from(queue_name);
    let max_batch_size = options.max_batch_size.max(1);
    let max_batch_age = options.max_batch_age;
    let max_message_size = options.max_message_size;
    let handler_timeout = options.handler_timeout;
    let handler_timeout_outcome = options.handler_timeout_outcome.clone();

    let flush_ctx = InMemoryFlushCtx {
        broker: &broker,
        topology,
        queue: &queue,
        topic: topic.as_ref(),
        group: group.as_deref(),
        shutdown: &shutdown,
        handler_timeout,
        handler_timeout_outcome,
    };

    let has_dlq = topology.dlq().is_some();
    let mut batch: InMemoryBatch<T> = InMemoryBatch::new();
    let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    let mut redelivery_backoff = batch_redelivery_backoff();

    tracing::info!(
        queue = queue_name,
        max_batch_size,
        ?max_batch_age,
        "InMemory batch consumer started"
    );

    loop {
        if batch.flush_len() >= max_batch_size {
            flush_inmemory_batch(
                &flush_ctx,
                &handler,
                &ctx,
                &mut batch,
                &mut redelivery_backoff,
            )
            .await;
            deadline = None;
            continue;
        }

        let sleep_until_deadline = async {
            match deadline.as_mut() {
                Some(d) => d.await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                if !batch.is_empty() {
                    flush_inmemory_batch(&flush_ctx, &handler, &ctx, &mut batch, &mut redelivery_backoff).await;
                }
                tracing::info!(queue = queue_name, "shutdown signal received, InMemory batch consumer stopped");
                return Ok(());
            }
            () = broker_shutdown.cancelled() => {
                if !batch.is_empty() {
                    flush_inmemory_batch(&flush_ctx, &handler, &ctx, &mut batch, &mut redelivery_backoff).await;
                }
                return Ok(());
            }
            () = sleep_until_deadline => {
                flush_inmemory_batch(&flush_ctx, &handler, &ctx, &mut batch, &mut redelivery_backoff).await;
                deadline = None;
            }
            env = pop_one(&queue) => {
                if deadline.is_none() {
                    deadline = Some(Box::pin(tokio::time::sleep(max_batch_age)));
                }
                match prepare_message::<T>(&env, max_message_size, &topic, group.as_deref()) {
                    Ok((message, metadata)) => {
                        batch.push(message, metadata, env);
                    }
                    Err(reason) => {
                        metrics::record_failed(&topic, group.as_deref(), reason);
                        batch.drop_message(has_dlq.then_some(env), reason);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

/// Prepare a message for handling: validate size and deserialize.
///
/// Returns `Err(reason)` when the message must be rejected before the handler
/// runs. The failure is deliberately *not* recorded here. Both rejections are
/// terminal — a message that is too large or does not decode will be no
/// smaller and no more decodable on redelivery — so they land in the same
/// reject funnel as a handler `Reject`, and that funnel records the terminal
/// metric. Recording here as well is how the same message came to increment
/// `messages_failed_total` twice, once as `oversize`/`deserialize` and again
/// as `rejected`, and to be discarded under `reason="rejected"` — which hides
/// the actual cause from exactly the alert that needs it.
fn prepare_message<T: Topic>(
    env: &Envelope,
    max_size: Option<usize>,
    topic: &str,
    group: Option<&str>,
) -> std::result::Result<(T::Message, MessageMetadata), metrics::FailReason> {
    metrics::record_message_size(topic, group, env.payload.len());

    if let Err(e) = validate_message_size(env.payload.len(), max_size) {
        tracing::warn!(error = %e, "rejecting oversized message");
        return Err(metrics::FailReason::Oversize);
    }

    let message: T::Message =
        match <T::Codec as crate::Codec<T::Message>>::decode_owned(env.payload.clone()) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "failed to deserialize message — rejecting");
                return Err(metrics::FailReason::Deserialize);
            }
        };

    Ok((message, metadata_from(env)))
}

/// A handler outcome plus, when the message never reached the handler, the
/// reason it was rejected.
///
/// `Some(reason)` overrides the reason the terminal funnel would otherwise
/// derive from [`decide_retry`], which only knows that the outcome was
/// `Reject` and would label an oversize or undecodable message `rejected`.
type Handled = (Outcome, Option<metrics::FailReason>);

/// Await the handler with an optional timeout, resolving a timeout through
/// [`handler_timeout_outcome`].
#[allow(clippy::too_many_arguments)]
async fn run_handler<T, H>(
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout_opt: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
) -> Outcome
where
    T: Topic,
    H: MessageHandler<T>,
{
    match timeout_opt {
        Some(timeout_dur) => {
            match tokio::time::timeout(timeout_dur, handler.handle(message, metadata, &ctx)).await {
                Ok(o) => o,
                Err(_) => {
                    let resolved = handler_timeout_outcome(timeout_outcome);
                    tracing::warn!(timeout = ?timeout_dur, outcome = ?resolved, "handler timed out");
                    metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                    resolved
                }
            }
        }
        None => handler.handle(message, metadata, &ctx).await,
    }
}

/// Direct handler invocation. The caller is responsible for catching panics
/// (the concurrent path relies on `JoinSet::join_next_with_id` for this).
#[allow(clippy::too_many_arguments)]
async fn invoke_handler<T, H>(
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    env: &Envelope,
    max_size: Option<usize>,
    timeout_opt: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
) -> Handled
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (message, metadata) = match prepare_message::<T>(env, max_size, topic, group) {
        Ok(pair) => pair,
        Err(reason) => return (Outcome::Reject, Some(reason)),
    };

    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let outcome = run_handler::<T, H>(
        handler,
        ctx,
        message,
        metadata,
        timeout_opt,
        timeout_outcome,
        topic,
        group,
    )
    .await;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, group, &outcome);
    metrics::record_processing_duration(topic, group, &outcome, elapsed);
    (outcome, None)
}

/// Runs the handler with panic-catching and shutdown-awareness for paths
/// without an outer `JoinSet` (the FIFO shard loop). Returns `None` when
/// shutdown aborts the in-flight handler — the caller must drop the message.
#[allow(clippy::too_many_arguments)]
async fn invoke_handler_caught<T, H>(
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    env: &Envelope,
    max_size: Option<usize>,
    timeout_opt: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    shutdown: &CancellationToken,
    broker_shutdown: &CancellationToken,
    topic: &str,
    group: Option<&str>,
) -> Option<Handled>
where
    T: Topic,
    H: MessageHandler<T>,
{
    // Deserialize on the caller task so the spawned task only owns the
    // already-decoded message + metadata — avoids cloning the full Envelope
    // on every FIFO message.
    let (message, metadata) = match prepare_message::<T>(env, max_size, topic, group) {
        Ok(pair) => pair,
        Err(reason) => return Some((Outcome::Reject, Some(reason))),
    };

    let topic_owned: std::sync::Arc<str> = std::sync::Arc::from(topic);
    let group_owned: Option<std::sync::Arc<str>> = group.map(std::sync::Arc::from);
    let _inflight = metrics::InflightGuard::new(topic_owned.clone(), group_owned.clone());
    let start = std::time::Instant::now();

    let mut join = tokio::spawn({
        let topic_owned = topic_owned.clone();
        let group_owned = group_owned.clone();
        async move {
            run_handler::<T, H>(
                handler,
                ctx,
                message,
                metadata,
                timeout_opt,
                timeout_outcome,
                &topic_owned,
                group_owned.as_deref(),
            )
            .await
        }
    });

    let outcome_opt = tokio::select! {
        biased;
        _ = shutdown.cancelled() => { join.abort(); None }
        _ = broker_shutdown.cancelled() => { join.abort(); None }
        res = &mut join => Some(res.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "handler task panicked — retrying message");
            Outcome::Retry
        })),
    };

    let elapsed = start.elapsed().as_secs_f64();
    if let Some(ref o) = outcome_opt {
        metrics::record_consumed(&topic_owned, group_owned.as_deref(), o);
        metrics::record_processing_duration(&topic_owned, group_owned.as_deref(), o, elapsed);
    }
    outcome_opt.map(|o| (o, None))
}

// ---------------------------------------------------------------------------
// Metadata helpers
// ---------------------------------------------------------------------------

fn message_id_of(headers: &HashMap<String, String>) -> &str {
    headers.get(X_MESSAGE_ID).map(String::as_str).unwrap_or("")
}

/// Log a message the in-process broker destroyed at shutdown.
///
/// These drops are deliberately not counted in `messages_failed_total` (see
/// [`metrics::FailReason`]), so this log line is the *only* evidence they
/// happened — which is why every arm that destroys the sole envelope has to
/// call it, and has to carry the message id.
///
/// Takes the id as `&str` rather than the headers because the arm that needs
/// it most fires *after* the envelope was moved into `enqueue`; the caller
/// hoists the id before the move.
fn warn_shutdown_drop(shard_name: &str, key: &str, message_id: &str, reason: &'static str) {
    tracing::warn!(
        shard = %shard_name,
        %key,
        %message_id,
        "{reason}"
    );
}

fn metadata_from(env: &Envelope) -> MessageMetadata {
    let retry_count = get_retry_count(&env.headers);
    let delivery_id = env
        .headers
        .get(X_MESSAGE_ID)
        .cloned()
        .unwrap_or_else(String::new);
    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered: retry_count > 0,
        delivery_count: Some(env.delivery_count),
        headers: Arc::new(env.headers.clone()),
    }
}

fn dead_metadata_from(env: &Envelope) -> DeadMessageMetadata {
    let message = metadata_from(env);
    DeadMessageMetadata {
        reason: env.headers.get(X_DEATH_REASON).cloned(),
        original_queue: env.headers.get(X_ORIGINAL_QUEUE).cloned(),
        death_count: env
            .headers
            .get(X_DEATH_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(1),
        message,
    }
}

fn get_retry_count(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(X_RETRY_COUNT)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn set_retry_count(headers: &mut HashMap<String, String>, count: u32) {
    headers.insert(X_RETRY_COUNT.to_string(), count.to_string());
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::backends::inmemory::client::{Envelope, InMemoryConfig};
    use crate::topology::TopologyBuilder;

    fn broadcast_topology() -> &'static QueueTopology {
        static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
        TOPOLOGY.get_or_init(|| {
            TopologyBuilder::new("deferred-teardown-broadcast")
                .broadcast()
                .build()
        })
    }

    fn envelope(body: &'static [u8]) -> Envelope {
        Envelope::new(Bytes::from_static(body), HashMap::new())
    }

    /// A deferred delivery parked on a full private broadcast buffer must stop
    /// owning that buffer when the subscriber leaves. Broker shutdown is not a
    /// substitute: the broker may continue serving unrelated subscriptions.
    #[tokio::test]
    async fn unsubscribe_cancels_a_blocked_broadcast_redelivery() {
        let broker = InMemoryBroker::with_config(InMemoryConfig {
            default_capacity: 1,
        });
        let subscription = broker.broadcast_subscribe("deferred-teardown-broadcast");
        let source = Arc::clone(subscription.queue());

        broker
            .enqueue(&source, envelope(b"fills-capacity"))
            .await
            .expect("fill private buffer");
        let source_weak = Arc::downgrade(&source);

        schedule_redelivery(
            &broker,
            broadcast_topology(),
            &source,
            envelope(b"deferred"),
            false,
            Some(subscription.closed_token()),
        );

        // Let the detached task reach the full-buffer capacity wait, then
        // remove every owner except that task.
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(source);
        drop(subscription);
        drop(broker);

        tokio::time::timeout(Duration::from_secs(2), async {
            while source_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deferred task retained a departed subscriber's private queue");
    }
}
