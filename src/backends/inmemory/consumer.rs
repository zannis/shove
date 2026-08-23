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
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::routing::{
    PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome, hold_index,
};
use crate::topic::{SequencedTopic, Topic};
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
        run_fifo_impl::<T, H>(self.broker.clone(), handler, ctx, options.into_inner())
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
    let topology = T::topology();
    let queue = broker.lookup(topology.queue())?;
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
    )
    .await;
    options.processing.store(false, Ordering::Release);
    Ok(())
}

async fn drain_pending(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    queue: &QueueState,
    pending: &mut BTreeMap<u64, (Envelope, Handled)>,
    next_route: &mut u64,
    options: &ConsumerOptionsInner,
) {
    while let Some((env, (outcome, pre_handler_reason))) = pending.remove(next_route) {
        route_outcome(broker, topology, env, outcome, pre_handler_reason, options).await;
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
                        warn_shutdown_drop(
                            &shard_name,
                            &key,
                            &env.headers,
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
                            warn_shutdown_drop(
                                &shard_name,
                                &key,
                                &new_env.headers,
                                "shutdown cancelled a pending sequenced retry — message dropped",
                            );
                            finish(&shard, &busy, &options);
                            return;
                        }

                        if broker.enqueue(&shard, new_env).await.is_err() {
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
    resolve_reject(route_reject(broker, topology, env).await, pending);
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
            resolve_reject(route_reject(broker, topology, env).await, pending);
        }
        RetryDecision::Hold { increment } => {
            schedule_redelivery(broker, topology, env, increment);
        }
    }
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
    env: Envelope,
    increment: bool,
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

    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = broker_shutdown.cancelled() => {
                let message_id = env.headers.get(X_MESSAGE_ID).map(String::as_str).unwrap_or("");
                tracing::warn!(
                    queue = %main_queue,
                    %message_id,
                    "broker shutdown cancelled a pending redelivery — message dropped"
                );
                return;
            }
        }
        // Re-check shutdown after sleep — the broker may have shut down while
        // awaiting capacity.
        if broker_shutdown.is_cancelled() {
            return;
        }
        let Ok(q) = broker_clone.lookup(&main_queue) else {
            tracing::warn!(queue = %main_queue, "redelivery target queue no longer exists");
            return;
        };
        let redelivery_timeout = Duration::from_secs(30);
        match tokio::time::timeout(redelivery_timeout, broker_clone.enqueue(&q, env)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(queue = %main_queue, error = %e, "redelivery enqueue failed");
            }
            Err(_elapsed) => {
                tracing::warn!(
                    queue = %main_queue,
                    "redelivery enqueue timed out after {}s — message dropped",
                    redelivery_timeout.as_secs()
                );
            }
        }
    });
}

/// Dead-letter a rejected message, or drop it when there is nowhere to put it.
///
/// Returns whether the message reached the DLQ. `false` covers all three ways
/// it can end up gone: no DLQ declared, a DLQ declared but absent from the
/// broker, and an enqueue that failed. Callers use this to decide whether the
/// message still exists somewhere before asserting a discard.
async fn route_reject(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    mut env: Envelope,
) -> bool {
    let Some(dlq_name) = topology.dlq() else {
        tracing::warn!(
            queue = topology.queue(),
            "message rejected but no DLQ configured — discarding"
        );
        return false;
    };
    let Ok(dlq) = broker.lookup(dlq_name) else {
        tracing::error!(queue = dlq_name, "DLQ declared but not found in broker");
        return false;
    };

    env.headers
        .insert(X_DEATH_REASON.to_string(), "rejected".to_string());
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

fn warn_shutdown_drop(
    shard_name: &str,
    key: &str,
    headers: &HashMap<String, String>,
    reason: &'static str,
) {
    let message_id = headers.get(X_MESSAGE_ID).map(String::as_str).unwrap_or("");
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
