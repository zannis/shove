//! NATS broadcast — one **ephemeral pull consumer** per subscription.
//!
//! # Why this is not a parameter change to the consumer-group path
//!
//! [`NatsTopologyDeclarer::declare_pull_consumer`] exists so the registry
//! upserts one *named durable* consumer and every task in the group then does a
//! read-only `get_consumer` **by name**. An ephemeral consumer has no name
//! until the server assigns one, so there is nothing to look up: the subscriber
//! has to create and own its handle. That ownership is not incidental — it is
//! also what makes the explicit teardown below possible.
//!
//! # Why the stream must not be `WorkQueue`
//!
//! shove's default retention is [`RetentionPolicy::WorkQueue`], and JetStream
//! refuses both halves of the broadcast consumer config on such a stream:
//!
//! ```text
//! AckPolicy::None      -> "consumer in pull mode requires ack policy" (10084)
//! DeliverPolicy::New   -> "consumer must be deliver all on workqueue stream" (10101)
//! ```
//!
//! So a broadcast topology is declared [`RetentionPolicy::Interest`] instead
//! (see [`super::topology`]). `Interest` accepts the ephemeral consumer, fans
//! every message to every subscriber, and — because `AckPolicy::None` means
//! interest is satisfied on delivery — retains **nothing**: neither while
//! subscribers are live nor when none exist. That is the documented broadcast
//! contract exactly, and it is why NATS needs no "bound your stream" caveat of
//! the sort Redis carries.
//!
//! # Teardown
//!
//! An ephemeral consumer does **not** disappear when the client handle drops —
//! the server keeps it until `inactive_threshold` elapses. So teardown is two
//! mechanisms, not one:
//!
//! 1. An awaited `delete_consumer` on the way out of the loop, so nothing
//!    survives the future resolving — the [`BroadcastImpl`] contract's wording.
//! 2. [`BROADCAST_INACTIVE_THRESHOLD`] as the backstop for the paths where no
//!    code of ours gets to run to completion: `JoinSet::abort_all` after a
//!    drain timeout, and the process dying outright. A live-but-idle subscriber
//!    is not at risk from it, because `messages()` keeps issuing pull requests
//!    and those count as activity.
//!
//! [`NatsTopologyDeclarer::declare_pull_consumer`]: super::topology::NatsTopologyDeclarer
//! [`RetentionPolicy::WorkQueue`]: async_nats::jetstream::stream::RetentionPolicy::WorkQueue
//! [`RetentionPolicy::Interest`]: async_nats::jetstream::stream::RetentionPolicy::Interest

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::jetstream::Message;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::broadcast::{BROADCAST_DEFER_DELAY, BroadcastAction, settle_broadcast_outcome};
use crate::consumer::validate_message_size;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::MessageMetadata;
use crate::metrics;
use crate::outcome::Outcome;
use crate::routing::handler_timeout_outcome;
use crate::topic::Topic;

use super::client::NatsClient;
use super::consumer::{
    NatsConsumer, extract_message_metadata, map_get_stream_error, run_with_reconnect,
};

/// How long the server keeps an abandoned ephemeral broadcast consumer.
///
/// Only ever reached when the awaited `delete_consumer` did not run: an
/// `abort_all()` after a drain timeout, or the process dying. Long enough that
/// a subscriber stalled behind a slow handler is never GC'd out from under
/// itself, short enough that a pod killed hard does not leave a visible
/// consumer around for long. A live subscriber is unaffected regardless — its
/// pull requests count as activity.
const BROADCAST_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

/// Deletes this subscription's ephemeral consumer when it goes out of scope.
///
/// The awaited delete on the happy path is the primary mechanism; this guard
/// covers the abort path, where the loop future is dropped mid-poll and no
/// `.await` after that point ever runs. `Drop` cannot await, so the delete is
/// spawned — and if there is no runtime left to spawn onto,
/// [`BROADCAST_INACTIVE_THRESHOLD`] is what finally reaps it.
struct EphemeralConsumerGuard {
    client: NatsClient,
    stream: String,
    consumer: String,
    armed: bool,
}

impl EphemeralConsumerGuard {
    fn new(client: NatsClient, stream: &str, consumer: String) -> Self {
        Self {
            client,
            stream: stream.to_owned(),
            consumer,
            armed: true,
        }
    }

    /// The caller has taken responsibility for the delete and awaited it.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EphemeralConsumerGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let client = self.client.clone();
        let stream = std::mem::take(&mut self.stream);
        let consumer = std::mem::take(&mut self.consumer);
        // `try_current` rather than `Handle::current`: no panics on a runtime
        // path, and a missing runtime is a real case here (the loop is being
        // dropped, which can happen during runtime teardown).
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    delete_ephemeral(&client, &stream, &consumer).await;
                });
            }
            Err(_) => {
                tracing::debug!(
                    stream,
                    consumer,
                    threshold_secs = BROADCAST_INACTIVE_THRESHOLD.as_secs(),
                    "no tokio runtime to delete the ephemeral broadcast consumer on; \
                     leaving it to the server's inactive_threshold"
                );
            }
        }
    }
}

/// Best-effort delete of an ephemeral consumer. Failure is logged, never
/// propagated: the subscription is already over, and the server's
/// `inactive_threshold` removes the consumer regardless.
async fn delete_ephemeral(client: &NatsClient, stream: &str, consumer: &str) {
    let js_stream = match client.jetstream().get_stream(stream).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                stream,
                consumer,
                error = %e,
                "could not reach the stream to delete the ephemeral broadcast consumer; \
                 the server's inactive_threshold will reap it"
            );
            return;
        }
    };
    match js_stream.delete_consumer(consumer).await {
        Ok(_) => tracing::debug!(stream, consumer, "ephemeral broadcast consumer deleted"),
        Err(e) => tracing::warn!(
            stream,
            consumer,
            error = %e,
            "failed to delete the ephemeral broadcast consumer; the server's \
             inactive_threshold will reap it"
        ),
    }
}

impl NatsConsumer {
    pub(crate) async fn run_broadcast_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_broadcast_impl::<T, H>(self.client_ref().clone(), handler, ctx, options).await
    }
}

async fn run_broadcast_impl<T, H>(
    client: NatsClient,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    let queue = topology.queue();
    if !topology.broadcast() {
        return Err(ShoveError::Topology(format!(
            "topic '{queue}' is not a broadcast topology; an ephemeral subscription to it \
             would receive nothing, because publishes go to the shared queue"
        )));
    }

    let shutdown = options.shutdown.clone();
    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);
    let topic: Arc<str> = Arc::from(queue);

    tracing::info!(queue, "NATS broadcast subscription starting");

    // Reconnects, like every other consumer in this crate. The alternative —
    // resolving the subscription with the error — reads as the more honest
    // option and is not: `BroadcastSubscriber` only surfaces a task's error
    // through `SupervisorOutcome` when `run_until_timeout` returns, i.e. at
    // shutdown. So a subscriber killed by a broker blip would go on looking
    // healthy for the rest of the process's life while receiving nothing, and
    // a cache-invalidation reader would serve stale data indefinitely. A gap in
    // delivery is explicitly within the broadcast contract; silently receiving
    // nothing forever is not.
    //
    // Each attempt creates its own ephemeral consumer at `New`, so messages
    // published during the outage are not replayed — the gap is real, logged by
    // `run_with_reconnect` at WARN, and documented on the concepts page. There
    // is no way around it here: an ephemeral consumer cannot resume from where
    // a previous one stopped, and under `Interest` retention the messages are
    // gone the moment the last interested consumer saw them anyway.
    run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
        let client = client.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let options = options.clone();
        let topic = Arc::clone(&topic);
        let shutdown = shutdown.clone();
        async move {
            let stream = client
                .jetstream()
                .get_stream(queue)
                .await
                .map_err(|e| map_get_stream_error(queue, e))?;

            let consumer = stream
                .create_consumer(PullConsumerConfig {
                    // The three properties that make this a broadcast
                    // subscription rather than one more member of a competing
                    // group.
                    durable_name: None,
                    ack_policy: AckPolicy::None,
                    deliver_policy: DeliverPolicy::New,
                    inactive_threshold: BROADCAST_INACTIVE_THRESHOLD,
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    ShoveError::Connection(format!(
                        "failed to create the ephemeral broadcast consumer on stream \
                         '{queue}' (a `.broadcast()` topology must be declared with \
                         Interest retention — WorkQueue rejects both AckPolicy::None and \
                         DeliverPolicy::New): {e}"
                    ))
                })?;

            // Server-assigned, because an ephemeral consumer has no durable name.
            let consumer_name = consumer.cached_info().name.clone();
            let mut guard =
                EphemeralConsumerGuard::new(client.clone(), queue, consumer_name.clone());
            tracing::debug!(
                queue,
                consumer = consumer_name,
                "ephemeral broadcast consumer created"
            );

            let result =
                broadcast_loop::<T, H>(consumer, handler, ctx, options, topic, shutdown).await;

            // Awaited, so "nothing survives the future resolving" is literally
            // true on this path rather than merely scheduled — and it runs per
            // attempt, so a reconnect does not accumulate consumers. The guard
            // covers the abort path, where nothing below this line runs at all.
            guard.disarm();
            delete_ephemeral(&client, queue, &consumer_name).await;

            result
        }
    })
    .await
}

async fn broadcast_loop<T, H>(
    consumer: Consumer<PullConsumerConfig>,
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    options: ConsumerOptions,
    topic: Arc<str>,
    shutdown: CancellationToken,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let mut messages = consumer
        .messages()
        .await
        .map_err(|e| ShoveError::Connection(format!("failed to get message stream: {e}")))?;

    // Broadcast is one delivery loop, and messages are handled inline rather
    // than spawned. A fan-out subscriber is reading a signal stream, so
    // in-order, one-at-a-time delivery is both cheaper and less surprising than
    // a prefetch window — and `Defer`'s in-loop redelivery needs the message to
    // still be here, which a spawned task would not give us.
    loop {
        let msg = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(topic = %topic, "broadcast subscription cancelled");
                return Ok(());
            }
            item = messages.next() => match item {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Consume,
                    );
                    return Err(ShoveError::Connection(format!(
                        "broadcast stream error on {topic}: {e}"
                    )));
                }
                None => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Consume,
                    );
                    return Err(ShoveError::Connection(format!(
                        "broadcast stream closed for {topic}"
                    )));
                }
            },
        };

        metrics::record_message_size(&topic, None, msg.payload.len());

        // No DLQ to route a malformed message to — `build()` rejects
        // `.broadcast()` with `.dlq()`. So the pre-handler rejections discard
        // through the same counters the terminal arm uses, and the loop moves
        // on; with `AckPolicy::None` there is nothing to ack either way.
        if let Err(e) = validate_message_size(msg.payload.len(), options.max_message_size) {
            tracing::warn!(
                topic = %topic,
                error = %e,
                "oversized message on a broadcast subscription — discarding (no DLQ)"
            );
            metrics::record_terminal(&topic, None, metrics::FailReason::Oversize, false).confirm();
            continue;
        }

        options.processing.store(true, Ordering::Release);
        deliver_until_settled::<T, H>(&handler, &ctx, &msg, &options, &topic, &shutdown).await;
        options.processing.store(false, Ordering::Release);
    }
}

/// Hand one message to the handler, honouring `Defer` by redelivering it to
/// this subscription only, until the outcome is terminal or shutdown fires.
///
/// Delivery is inline, so a handler that keeps returning `Defer` holds up the
/// messages behind it. That is a consequence of broadcast being one loop at
/// concurrency 1, and it is documented on the concepts page rather than worked
/// around: there is no second consumer to hand the backlog to, and no DLQ or
/// hold queue to park the deferred message in.
async fn deliver_until_settled<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    msg: &Message,
    options: &ConsumerOptions,
    topic: &Arc<str>,
    shutdown: &CancellationToken,
) where
    T: Topic,
    H: MessageHandler<T>,
{
    loop {
        // Decoded per attempt rather than once. `T::Message` carries no `Clone`
        // bound and the handler takes it by value, so a `Defer` needs a fresh
        // one; `msg.payload` is a `Bytes`, so the clone is a refcount bump.
        let payload: T::Message =
            match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        topic = %topic,
                        error = %e,
                        "undeserializable message on a broadcast subscription — discarding (no DLQ)"
                    );
                    metrics::record_terminal(topic, None, metrics::FailReason::Deserialize, false)
                        .confirm();
                    return;
                }
            };

        let metadata = extract_message_metadata(msg);
        let outcome = invoke_broadcast_handler(
            handler.clone(),
            ctx.clone(),
            payload,
            metadata,
            options,
            topic,
        )
        .await;

        match settle_broadcast_outcome(&outcome, topic, None) {
            BroadcastAction::Done => return,
            BroadcastAction::Redeliver => {
                tokio::select! {
                    _ = tokio::time::sleep(BROADCAST_DEFER_DELAY) => {}
                    // A deferred message is not worth holding shutdown open
                    // for: it has no DLQ and no durable home, so waiting out
                    // the backoff would only delay the drain.
                    _ = shutdown.cancelled() => return,
                }
            }
        }
    }
}

/// The handler-invocation wrapper, minus the retry machinery a broadcast
/// subscription has no use for. Keeps the timeout, panic containment and the
/// inflight/consumed/duration metrics identical to the competing-consumer path.
async fn invoke_broadcast_handler<T, H>(
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    payload: T::Message,
    metadata: MessageMetadata,
    options: &ConsumerOptions,
    topic: &Arc<str>,
) -> Outcome
where
    T: Topic,
    H: MessageHandler<T>,
{
    let _inflight = metrics::InflightGuard::new(topic.clone(), None);
    let start = std::time::Instant::now();
    // Spawned so a panicking handler surfaces as a `JoinError` here instead of
    // taking the delivery loop down with it.
    let mut join =
        tokio::spawn(async move { handler.handle(payload, metadata, ctx.as_ref()).await });

    let outcome = match options.handler_timeout {
        Some(duration) => match tokio::time::timeout(duration, &mut join).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "broadcast handler panicked");
                Outcome::Retry
            }
            Err(_) => {
                join.abort();
                let resolved = handler_timeout_outcome(options.handler_timeout_outcome.clone());
                tracing::warn!(outcome = ?resolved, "broadcast handler timed out after {duration:?}");
                metrics::record_failed(topic, None, metrics::FailReason::Timeout);
                resolved
            }
        },
        None => match join.await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "broadcast handler panicked");
                Outcome::Retry
            }
        },
    };

    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, None, &outcome);
    metrics::record_processing_duration(topic, None, &outcome, elapsed);
    outcome
}
