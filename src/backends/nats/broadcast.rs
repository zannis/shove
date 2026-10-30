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
//!    drain timeout, and the process dying outright. A live subscriber is not
//!    at risk from it, because `messages()` keeps issuing pull requests and
//!    those count as activity. That holds while a handler runs as well: the
//!    delivery loop goes on reading the stream behind the message it is
//!    handling ([`Intake::reading_during`]), so a slow handler or a `Defer`
//!    that keeps the loop for minutes still leaves the consumer pulling.
//!
//! [`NatsTopologyDeclarer::declare_pull_consumer`]: super::topology::NatsTopologyDeclarer
//! [`RetentionPolicy::WorkQueue`]: async_nats::jetstream::stream::RetentionPolicy::WorkQueue
//! [`RetentionPolicy::Interest`]: async_nats::jetstream::stream::RetentionPolicy::Interest

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::jetstream::Message;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy};
use futures_util::{Stream, StreamExt};
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
/// `abort_all()` after a drain timeout, or the process dying. Short enough
/// that a pod killed hard does not leave a visible consumer around for long. A
/// live subscriber is unaffected regardless: its pull requests count as
/// activity, and it keeps issuing them while a handler runs.
const BROADCAST_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

/// How many messages the delivery loop sets aside while a handler runs. The
/// loop reads the stream behind the message it handles so the consumer keeps
/// pulling, and it stops at this many, or at [`MAX_READ_AHEAD_BYTES`] of
/// message bytes, whichever comes first: from then on the consumer pulls again
/// only once the handler is done, as it does with no read-ahead at all.
const MAX_READ_AHEAD: usize = 1_024;

/// How many bytes of messages the delivery loop sets aside while a handler
/// runs, counted at their length on the wire: headers, payload, subject and
/// reply. Reading stops once the messages set aside add up to this, so a few
/// large messages, or messages with large headers and no payload, cannot fill
/// the memory that [`MAX_READ_AHEAD`] small ones would leave alone. The message
/// that takes the total past it is kept, so the loop holds this and at most one
/// message more.
const MAX_READ_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// How many times the awaited teardown asks the server to delete the consumer
/// before leaving it to the guard and `BROADCAST_INACTIVE_THRESHOLD`.
const BROADCAST_DELETE_ATTEMPTS: u32 = 3;

/// Gap between those attempts. Short: this sits on the shutdown path, and the
/// failure it covers is a blip rather than an outage.
const BROADCAST_DELETE_RETRY_DELAY: Duration = Duration::from_millis(200);

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
                    let _ = delete_ephemeral(&client, &stream, &consumer).await;
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

/// Delete an ephemeral consumer. Returns whether the consumer is confirmed
/// gone, so the caller can keep its `Drop` guard armed for another attempt
/// instead of leaving the consumer to `inactive_threshold`. Failure is logged,
/// never propagated: the subscription is already over either way.
async fn delete_ephemeral(client: &NatsClient, stream: &str, consumer: &str) -> bool {
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
            return false;
        }
    };
    match js_stream.delete_consumer(consumer).await {
        // `DeleteStatus` carries the server's own verdict. `Ok` means the
        // request round-tripped, not that the consumer went, so a `success:
        // false` reply has to keep the guard armed like any other failure.
        Ok(status) if status.success => {
            tracing::debug!(stream, consumer, "ephemeral broadcast consumer deleted");
            true
        }
        Ok(_) => {
            tracing::warn!(
                stream,
                consumer,
                "the server declined to delete the ephemeral broadcast consumer"
            );
            false
        }
        Err(e) => {
            tracing::warn!(
                stream,
                consumer,
                error = %e,
                "failed to delete the ephemeral broadcast consumer"
            );
            false
        }
    }
}

/// Attempts a delete until the server confirms it, or the budget runs out.
///
/// The awaited delete is what makes "nothing survives the future resolving"
/// literally true, so a single failed attempt handing off to a detached `Drop`
/// spawn would give that up for a transient blip. Bounded rather than
/// open-ended: this runs on the shutdown path, where a drain is already racing
/// a deadline, and `inactive_threshold` is the backstop that makes giving up
/// safe.
async fn delete_ephemeral_with_retry(client: &NatsClient, stream: &str, consumer: &str) -> bool {
    for attempt in 0..BROADCAST_DELETE_ATTEMPTS {
        if delete_ephemeral(client, stream, consumer).await {
            return true;
        }
        if attempt.saturating_add(1) < BROADCAST_DELETE_ATTEMPTS {
            tokio::time::sleep(BROADCAST_DELETE_RETRY_DELAY).await;
        }
    }
    tracing::warn!(
        stream,
        consumer,
        attempts = BROADCAST_DELETE_ATTEMPTS,
        threshold_secs = BROADCAST_INACTIVE_THRESHOLD.as_secs(),
        "giving up on deleting the ephemeral broadcast consumer; the guard's \
         detached attempt and the server's inactive_threshold are what remain"
    );
    false
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
            //
            // Disarmed only once the delete is confirmed: disarming first would
            // drop the guard's second chance if this await were cancelled
            // mid-delete, or if the delete simply failed.
            if delete_ephemeral_with_retry(&client, queue, &consumer_name).await {
                guard.disarm();
            }

            result
        }
    })
    .await
}

/// A subscription's pull stream, read ahead of its handler.
///
/// The delivery loop handles one message at a time, and a consumer that nobody
/// reads stops pulling: its last pull request expires after 30 s, and the
/// server drops the consumer, and everything the stream held for it, once
/// [`BROADCAST_INACTIVE_THRESHOLD`] passes with no request. So while a handler
/// runs, or a `Defer` waits, the loop goes on reading the stream and sets the
/// messages aside, up to [`MAX_READ_AHEAD`] of them and [`MAX_READ_AHEAD_BYTES`]
/// of message bytes, for the loop to handle in order afterwards.
///
/// Every message is gated by `max_message_size` as it is read, before it can
/// be set aside, so a message over the limit is never held.
struct Intake<S> {
    messages: S,
    topic: Arc<str>,
    group: Option<Arc<str>>,
    max_message_size: Option<usize>,
    /// Taken off the stream while a handler ran, oldest first.
    ahead: VecDeque<Message>,
    /// The wire bytes of `ahead`: headers, payload, subject and reply.
    ahead_bytes: usize,
    /// How the stream ended while a handler ran, reported once `ahead` is
    /// handled.
    ended: Option<ShoveError>,
}

impl<S, E> Intake<S>
where
    S: Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: std::fmt::Display,
{
    fn new(
        messages: S,
        topic: Arc<str>,
        group: Option<Arc<str>>,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            messages,
            topic,
            group,
            max_message_size,
            ahead: VecDeque::new(),
            ahead_bytes: 0,
            ended: None,
        }
    }

    /// The next message in arrival order: one set aside during a handler, or
    /// the next off the stream. The error is how the stream ended.
    async fn next(&mut self) -> Result<Message> {
        if let Some(msg) = self.take_aside() {
            return Ok(msg);
        }
        if let Some(ended) = self.ended.take() {
            return Err(ended);
        }
        self.read().await
    }

    /// The oldest message set aside, with its bytes no longer counted.
    fn take_aside(&mut self) -> Option<Message> {
        let msg = self.ahead.pop_front()?;
        self.ahead_bytes = self.ahead_bytes.saturating_sub(msg.length);
        Some(msg)
    }

    fn set_aside(&mut self, msg: Message) {
        self.ahead_bytes = self.ahead_bytes.saturating_add(msg.length);
        self.ahead.push_back(msg);
    }

    /// Whether the loop may set another message aside: the stream is open, and
    /// the messages set aside are under both limits. The one that takes them
    /// past [`MAX_READ_AHEAD_BYTES`] is kept, so they never hold more than the
    /// limit and one message.
    fn has_room(&self) -> bool {
        self.ended.is_none()
            && self.ahead.len() < MAX_READ_AHEAD
            && self.ahead_bytes < MAX_READ_AHEAD_BYTES
    }

    /// The next message off the stream that passes the size gate, or the error
    /// that ends the subscription.
    async fn read(&mut self) -> Result<Message> {
        loop {
            let ended = match self.messages.next().await {
                Some(Ok(msg)) => {
                    if self.admit(&msg) {
                        return Ok(msg);
                    }
                    continue;
                }
                Some(Err(e)) => format!("broadcast stream error on {}: {e}", self.topic),
                None => format!("broadcast stream closed for {}", self.topic),
            };
            metrics::record_backend_error(
                metrics::BackendLabel::Nats,
                metrics::BackendErrorKind::Consume,
            );
            return Err(ShoveError::Connection(ended));
        }
    }

    /// Records the size of `msg` and gates it by `max_message_size`.
    ///
    /// There is no DLQ to route a message over the limit to, since `build()`
    /// rejects `.broadcast()` with `.dlq()`. So it discards through the same
    /// counters the terminal arm uses and is gone; with `AckPolicy::None` there
    /// is nothing to ack either way.
    fn admit(&self, msg: &Message) -> bool {
        let group = self.group.as_deref();
        metrics::record_message_size(&self.topic, group, msg.payload.len());
        let Err(e) = validate_message_size(msg.payload.len(), self.max_message_size) else {
            return true;
        };
        tracing::warn!(
            topic = %self.topic,
            error = %e,
            "oversized message on a broadcast subscription — discarding (no DLQ)"
        );
        metrics::record_terminal(&self.topic, group, metrics::FailReason::Oversize, false)
            .confirm();
        false
    }

    /// Runs `work`, reading the stream meanwhile so the consumer keeps pulling
    /// for as long as it runs. Stops reading when [`MAX_READ_AHEAD`] messages
    /// or [`MAX_READ_AHEAD_BYTES`] of message bytes are set aside, or the stream
    /// has ended, and finishes `work` all the same.
    async fn reading_during<F: Future>(&mut self, work: F) -> F::Output {
        let mut work = std::pin::pin!(work);
        while self.has_room() {
            tokio::select! {
                biased;
                output = &mut work => return output,
                read = self.read() => match read {
                    Ok(msg) => self.set_aside(msg),
                    Err(ended) => self.ended = Some(ended),
                },
            }
        }
        work.await
    }
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
    let messages = consumer
        .messages()
        .await
        .map_err(|e| ShoveError::Connection(format!("failed to get message stream: {e}")))?;
    let mut intake = Intake::new(
        messages,
        Arc::clone(&topic),
        options.consumer_group.clone(),
        options.max_message_size,
    );

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
            next = intake.next() => next?,
        };

        options.processing.store(true, Ordering::Release);
        intake
            .reading_during(deliver_until_settled::<T, H>(
                &handler, &ctx, &msg, &options, &topic, &shutdown,
            ))
            .await;
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
                    metrics::record_terminal(
                        topic,
                        options.consumer_group.as_deref(),
                        metrics::FailReason::Deserialize,
                        false,
                    )
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

        match settle_broadcast_outcome(&outcome, topic, options.consumer_group.as_deref()) {
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
    let _inflight = metrics::InflightGuard::new(topic.clone(), options.consumer_group.clone());
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
                metrics::record_failed(
                    topic,
                    options.consumer_group.as_deref(),
                    metrics::FailReason::Timeout,
                );
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
    metrics::record_consumed(topic, options.consumer_group.as_deref(), &outcome);
    metrics::record_processing_duration(
        topic,
        options.consumer_group.as_deref(),
        &outcome,
        elapsed,
    );
    outcome
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Poll;

    use async_nats::jetstream::{self, Context};
    use bytes::Bytes;
    use futures_util::stream;

    use super::*;

    /// A JetStream context on a client that never connects. The messages built
    /// below are only read and counted, never acked.
    async fn lazy_context() -> Context {
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .expect("a lazily connecting client is built without a server");
        jetstream::new(client)
    }

    fn message(context: &Context, size: usize) -> Message {
        Message {
            message: async_nats::Message {
                subject: "orders".into(),
                reply: None,
                payload: Bytes::from(vec![0; size]),
                headers: None,
                status: None,
                description: None,
                length: size,
            },
            context: context.clone(),
        }
    }

    /// A message with no payload and `header_bytes` of headers.
    fn header_heavy(context: &Context, header_bytes: usize) -> Message {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Bulk", "x".repeat(header_bytes));
        Message {
            message: async_nats::Message {
                subject: "orders".into(),
                reply: None,
                payload: Bytes::new(),
                headers: Some(headers),
                status: None,
                description: None,
                length: "orders".len() + header_bytes,
            },
            context: context.clone(),
        }
    }

    /// A stream of messages that never ends, each made by `make`, and the
    /// count of those it has handed out.
    fn endless_of(
        make: impl Fn() -> Message,
    ) -> (
        impl Stream<Item = std::result::Result<Message, String>> + Unpin,
        Arc<AtomicUsize>,
    ) {
        let produced = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&produced);
        let messages = stream::iter(std::iter::from_fn(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Some(Ok(make()))
        }));
        (messages, produced)
    }

    /// A stream of messages that never ends, each of `size` bytes, and the
    /// count of those it has handed out.
    fn endless(
        context: Context,
        size: usize,
    ) -> (
        impl Stream<Item = std::result::Result<Message, String>> + Unpin,
        Arc<AtomicUsize>,
    ) {
        endless_of(move || message(&context, size))
    }

    /// A future that stays pending for `polls` polls. The loop polls it once
    /// per message it reads, so it outlasts the reading a test looks at.
    fn pending_for(polls: usize) -> impl Future<Output = ()> {
        let mut left = polls;
        std::future::poll_fn(move |cx| {
            if left == 0 {
                return Poll::Ready(());
            }
            left = left.saturating_sub(1);
            cx.waker().wake_by_ref();
            Poll::Pending
        })
    }

    fn intake_of<S, E>(messages: S, max_message_size: Option<usize>) -> Intake<S>
    where
        S: Stream<Item = std::result::Result<Message, E>> + Unpin,
        E: std::fmt::Display,
    {
        Intake::new(messages, "orders".into(), None, max_message_size)
    }

    fn sizes(intake: &Intake<impl Sized>) -> Vec<usize> {
        intake.ahead.iter().map(|msg| msg.payload.len()).collect()
    }

    #[tokio::test]
    async fn an_oversized_message_is_dropped_at_read_time_and_never_buffered() {
        let context = lazy_context().await;
        let source = stream::iter(vec![
            Ok::<_, String>(message(&context, 10)),
            Ok(message(&context, 101)),
            Ok(message(&context, 20)),
        ]);
        let mut intake = intake_of(source, Some(100));

        intake.reading_during(pending_for(100)).await;

        assert!(
            intake.ended.is_some(),
            "the whole stream was read, the oversized message included"
        );
        assert_eq!(
            sizes(&intake),
            [10, 20],
            "the oversized message is not set aside"
        );
        assert_eq!(intake.ahead_bytes, 30);
        assert_eq!(intake.next().await.expect("a message").payload.len(), 10);
        assert_eq!(intake.next().await.expect("a message").payload.len(), 20);
        assert!(intake.next().await.is_err(), "and none is handed out later");
    }

    #[tokio::test]
    async fn an_oversized_message_is_dropped_when_no_handler_is_running() {
        let context = lazy_context().await;
        let source = stream::iter(vec![
            Ok::<_, String>(message(&context, 101)),
            Ok(message(&context, 10)),
        ]);
        let mut intake = intake_of(source, Some(100));

        let next = intake.next().await.expect("a message");

        assert_eq!(next.payload.len(), 10, "the oversized message is skipped");
    }

    #[tokio::test]
    async fn a_message_at_the_size_limit_is_kept() {
        let context = lazy_context().await;
        let source = stream::iter(vec![Ok::<_, String>(message(&context, 100))]);
        let mut intake = intake_of(source, Some(100));

        intake.reading_during(pending_for(10)).await;

        assert_eq!(sizes(&intake), [100]);
    }

    #[tokio::test]
    async fn read_ahead_stops_at_the_byte_budget() {
        let size = MAX_READ_AHEAD_BYTES / 4;
        let (source, produced) = endless(lazy_context().await, size);
        let mut intake = intake_of(source, None);

        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        assert_eq!(
            produced.load(Ordering::SeqCst),
            4,
            "no message is read once four of them reach the budget"
        );
        assert_eq!(sizes(&intake), [size; 4]);
        assert_eq!(intake.ahead_bytes, MAX_READ_AHEAD_BYTES);
    }

    #[tokio::test]
    async fn the_message_that_crosses_the_byte_budget_is_kept() {
        let size = MAX_READ_AHEAD_BYTES / 4 * 3;
        let (source, produced) = endless(lazy_context().await, size);
        let mut intake = intake_of(source, None);

        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        assert_eq!(
            produced.load(Ordering::SeqCst),
            2,
            "the second message takes the total past the budget, and ends the reading"
        );
        assert_eq!(sizes(&intake), [size; 2]);
        assert_eq!(intake.ahead_bytes, size * 2);
    }

    #[tokio::test]
    async fn taking_a_message_aside_frees_its_bytes_for_more_read_ahead() {
        let size = MAX_READ_AHEAD_BYTES / 4;
        let (source, produced) = endless(lazy_context().await, size);
        let mut intake = intake_of(source, None);
        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        intake.next().await.expect("a message");
        intake.next().await.expect("a message");
        assert_eq!(intake.ahead_bytes, size * 2);
        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        assert_eq!(
            produced.load(Ordering::SeqCst),
            6,
            "two more fill the budget again"
        );
        assert_eq!(intake.ahead_bytes, MAX_READ_AHEAD_BYTES);
    }

    /// Header bytes of a message with no payload, few enough that the byte
    /// budget and not the message limit ends the reading.
    const HEADER_BYTES: usize = 16 * 1024;

    #[tokio::test]
    async fn read_ahead_counts_the_header_bytes_of_messages_without_a_payload() {
        let context = lazy_context().await;
        let (source, produced) = endless_of(|| header_heavy(&context, HEADER_BYTES));
        let mut intake = intake_of(source, None);
        let wire_bytes = HEADER_BYTES + "orders".len();

        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        let read = produced.load(Ordering::SeqCst);
        assert_eq!(
            read,
            MAX_READ_AHEAD_BYTES.div_ceil(wire_bytes),
            "no message is read once the headers alone reach the budget"
        );
        assert_eq!(intake.ahead.len(), read);
        assert_eq!(intake.ahead_bytes, read * wire_bytes);
        assert!(
            intake.ahead.iter().all(|msg| msg.payload.is_empty()),
            "none of the bytes counted is payload"
        );
    }

    #[tokio::test]
    async fn taking_a_message_aside_frees_its_header_bytes_for_more_read_ahead() {
        let context = lazy_context().await;
        let (source, produced) = endless_of(|| header_heavy(&context, HEADER_BYTES));
        let mut intake = intake_of(source, None);
        let wire_bytes = HEADER_BYTES + "orders".len();
        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;
        let filled = produced.load(Ordering::SeqCst);

        intake.next().await.expect("a message");
        intake.next().await.expect("a message");
        assert_eq!(intake.ahead_bytes, (filled - 2) * wire_bytes);
        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        assert_eq!(
            produced.load(Ordering::SeqCst),
            filled + 2,
            "two more fill the budget again"
        );
        assert_eq!(intake.ahead_bytes, filled * wire_bytes);
    }

    #[tokio::test]
    async fn read_ahead_stops_at_the_message_limit() {
        let (source, produced) = endless(lazy_context().await, 1);
        let mut intake = intake_of(source, None);

        intake.reading_during(pending_for(MAX_READ_AHEAD * 2)).await;

        assert_eq!(produced.load(Ordering::SeqCst), MAX_READ_AHEAD);
        assert_eq!(intake.ahead.len(), MAX_READ_AHEAD);
        assert_eq!(intake.ahead_bytes, MAX_READ_AHEAD);
    }

    #[tokio::test]
    async fn a_stream_error_during_work_is_reported_after_the_messages_set_aside() {
        let context = lazy_context().await;
        let source = stream::iter(vec![
            Ok(message(&context, 5)),
            Ok(message(&context, 6)),
            Err("boom".to_string()),
        ]);
        let mut intake = intake_of(source, None);

        intake.reading_during(pending_for(100)).await;

        assert_eq!(intake.next().await.expect("a message").payload.len(), 5);
        assert_eq!(intake.next().await.expect("a message").payload.len(), 6);
        let Err(ShoveError::Connection(ended)) = intake.next().await else {
            panic!("the stream's end follows the messages set aside");
        };
        assert!(
            ended.contains("boom") && ended.contains("orders"),
            "the error names the stream error and the topic: {ended}"
        );
    }
}
