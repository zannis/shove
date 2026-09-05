use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::Message;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::context::{
    ConsumerInfoError, ConsumerInfoErrorKind, GetStreamError, GetStreamErrorKind,
};
use async_nats::jetstream::message::AckKind;
use futures_util::FutureExt;
use futures_util::stream::{self, StreamExt};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::batch_consumer::{
    BatchConsumerOptionsInner, BatchSettlement, PREALLOC_CAP, batch_redelivery_backoff,
    invoke_batch_handler, next_redelivery_delay, settle_batch_outcome,
};
use crate::consumer::{DEFAULT_HANDLER_TIMEOUT, validate_message_size};
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::Result;
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{
    PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome, hold_index,
};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, HoldQueue, Nats, ShoveError};

use super::client::NatsClient;
use super::constants::{
    DEATH_COUNT_HEADER, DEATH_REASON_HEADER, ORIGINAL_QUEUE_HEADER, RETRY_COUNT_HEADER,
    SEQUENCE_KEY_HEADER,
};
use super::publisher::publish_with_retry;

// ---------------------------------------------------------------------------
// ack_wait derivation
// ---------------------------------------------------------------------------

/// JetStream redelivers any message not acked within `ack_wait`. Derive it
/// from the handler timeout with a 3x margin so a handler running to its
/// limit — plus queue-wait behind a full prefetch buffer (the `ack_wait`
/// clock ticks from delivery, and delivered messages wait on the prefetch
/// semaphore before their handler starts) — never has its message
/// redelivered mid-flight. Floor at the JetStream default (30s) so short
/// handler timeouts don't tighten redelivery below the server default.
pub(super) fn derive_ack_wait(handler_timeout: Duration) -> Duration {
    (handler_timeout * 3).max(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Metadata extraction functions
// ---------------------------------------------------------------------------

/// Converts all header name/value pairs to a HashMap.
fn extract_string_headers(headers: &Option<HeaderMap>) -> HashMap<String, String> {
    let Some(hm) = headers.as_ref() else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for (name, values) in hm.iter() {
        if let Some(first) = values.first() {
            out.insert(name.to_string(), first.as_str().to_string());
        }
    }
    out
}

/// Reads `Shove-Retry-Count` from headers, defaults to 0.
fn get_retry_count(headers: &Option<HeaderMap>) -> u32 {
    headers
        .as_ref()
        .and_then(|hm| hm.get(RETRY_COUNT_HEADER))
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0)
}

/// Reads `Shove-Sequence-Key` from headers.
///
/// Empty when the message carries no key — either it predates the header or it
/// was published by something other than shove's publisher. `PoisonedKeys`
/// never poisons the empty key, so such a message falls back to `Skip`
/// behaviour rather than poisoning every unkeyed message on the shard.
fn get_sequence_key(headers: &Option<HeaderMap>) -> String {
    headers
        .as_ref()
        .and_then(|hm| hm.get(SEQUENCE_KEY_HEADER))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default()
}

/// Record a `FailAll` poisoning, logging only the first transition per key.
/// A no-op under `SequenceFailure::Skip` and for unkeyed messages.
fn poison_key(poisoned: &PoisonedKeys, key: &str, queue: &str) {
    if poisoned.poison(key) {
        tracing::info!(queue, sequence_key = %key, "poisoning sequence key (FailAll)");
    }
}

/// Extracts message metadata from a JetStream message.
pub(super) fn extract_message_metadata(msg: &Message) -> MessageMetadata {
    let retry_count = get_retry_count(&msg.headers);

    let delivery_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();

    // `info.delivered` is JetStream's `num_delivered`: attempts so far including
    // this one, so the first delivery reports 1. It is absent for messages that
    // did not come from a stream (`info()` errors), which is why the count is
    // optional rather than defaulted to 1.
    let delivery_count = msg
        .info()
        .ok()
        .map(|info| u32::try_from(info.delivered).unwrap_or(u32::MAX));
    let redelivered = delivery_count.is_some_and(|n| n > 1);

    let headers = extract_string_headers(&msg.headers);

    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        delivery_count,
        headers: Arc::new(headers),
    }
}

/// Extracts dead message metadata from a JetStream message.
fn extract_dead_metadata(msg: &Message) -> DeadMessageMetadata {
    let message = extract_message_metadata(msg);

    let reason = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(DEATH_REASON_HEADER))
        .map(|v| v.as_str().to_string());

    let original_queue = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(ORIGINAL_QUEUE_HEADER))
        .map(|v| v.as_str().to_string());

    let death_count = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(DEATH_COUNT_HEADER))
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0);

    DeadMessageMetadata {
        message,
        reason,
        original_queue,
        death_count,
    }
}

// ---------------------------------------------------------------------------
// Outcome routing functions
// ---------------------------------------------------------------------------

/// In FIFO (sequenced) mode, Defer is not supported because it violates
/// ordering guarantees. Convert Defer → Retry with a warning.
fn adjust_outcome_for_fifo(outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Defer => {
            tracing::warn!("Defer is not supported on sequenced consumers — treating as Retry");
            Outcome::Retry
        }
        other => other,
    }
}

/// Publishes a message to the DLQ stream with death headers.
async fn publish_to_dlq(
    client: &NatsClient,
    topology: &QueueTopology,
    msg: &Message,
    reason: &str,
) -> Result<()> {
    let dlq_subject = match topology.dlq() {
        Some(dlq) => dlq.to_string(),
        None => {
            tracing::warn!(
                queue = topology.queue(),
                "no DLQ configured, message will be discarded"
            );
            return Ok(());
        }
    };

    let mut headers = msg.headers.clone().unwrap_or_default();
    headers.insert(DEATH_REASON_HEADER, reason);
    headers.insert(ORIGINAL_QUEUE_HEADER, topology.queue());

    let current_death_count = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(DEATH_COUNT_HEADER))
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0);
    headers.insert(
        DEATH_COUNT_HEADER,
        (current_death_count + 1).to_string().as_str(),
    );

    // Generate a new message ID for the DLQ publish to avoid dedup rejection
    let original_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    headers.insert(NATS_MESSAGE_ID, format!("{original_id}-dlq").as_str());

    publish_with_retry(
        client.jetstream(),
        dlq_subject,
        headers,
        msg.payload.clone(),
        3,
        "DLQ publish",
    )
    .await
}

/// Retries a message after `delay` without the at-least-once gap the old
/// ack-then-republish path had.
///
/// The original message is held *un-acked* for the whole backoff, so a crash or
/// a failed republish can never drop it. JetStream redelivers any un-acked
/// message once `ack_wait` elapses, so WIP progress acks are sent during the
/// wait to keep extending that timer (the same enqueue/republish-before-ack
/// ordering the Redis and Kafka paths use). Once `delay` has passed, an
/// incremented copy is published durably (the PubAck is awaited) and only then
/// is the original acked. On shutdown the hold is abandoned with the message
/// still un-acked so it is redelivered on restart; on republish failure the
/// original is nak'd for immediate redelivery.
///
/// Retry counting stays header-based — the republished copy carries an
/// incremented `RETRY_COUNT_HEADER` — so, unlike a `Nak`-driven redelivery, a
/// `Defer` (which uses `Nak(Some(delay))` and leaves the header untouched) does
/// not consume the retry budget. That keeps parity with the other backends.
async fn hold_then_republish(
    client: &NatsClient,
    msg: &Message,
    delay: Duration,
    new_count: u32,
    ack_wait: Duration,
    shutdown: &CancellationToken,
) {
    // Heartbeat well inside ack_wait so the server never redelivers mid-hold.
    // `ack_wait` of zero means "unknown"; fall back to a value safely under
    // JetStream's 30s default.
    let heartbeat = if ack_wait.is_zero() {
        Duration::from_secs(15)
    } else {
        (ack_wait / 2).max(Duration::from_secs(1))
    };

    let deadline = tokio::time::Instant::now() + delay;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let tick = (deadline - now).min(heartbeat);
        tokio::select! {
            _ = tokio::time::sleep(tick) => {}
            _ = shutdown.cancelled() => {
                // Leave the message un-acked — it redelivers on restart.
                return;
            }
        }
        if tokio::time::Instant::now() < deadline
            && let Err(e) = msg.ack_with(AckKind::Progress).await
        {
            tracing::warn!(error = %e, "failed to send progress ack during retry hold");
        }
    }

    let mut hdrs = msg.headers.clone().unwrap_or_default();
    hdrs.insert(RETRY_COUNT_HEADER, new_count.to_string().as_str());

    // New message ID so JetStream dedup doesn't reject the republished copy.
    // Our publisher always sets a UUID message ID, so `original_id` is normally
    // present; the empty fallback only applies to externally-published messages
    // and at worst weakens dedup within the short dedup window.
    let original_id = hdrs
        .get(NATS_MESSAGE_ID)
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    hdrs.insert(
        NATS_MESSAGE_ID,
        format!("{original_id}-r{new_count}").as_str(),
    );

    // Publish the copy durably BEFORE acking the original: a failure leaves the
    // original for redelivery instead of silently dropping the message.
    match publish_with_retry(
        client.jetstream(),
        msg.subject.to_string(),
        hdrs,
        msg.payload.clone(),
        3,
        "retry republish",
    )
    .await
    {
        Ok(()) => {
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack after retry republish");
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "retry republish failed, nak-ing original for redelivery");
            if let Err(nak_err) = msg.ack_with(AckKind::Nak(None)).await {
                tracing::error!(error = %nak_err, "failed to nak after retry republish failure");
            }
        }
    }
}

/// Dispatches message routing based on the handler's outcome.
#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    client: &NatsClient,
    // Consumer-group label propagated to `metrics::record_failed` on
    // DLQ-terminal outcomes (max_retries_exceeded, Rejected). Matches the
    // shape `invoke_handler` already uses, and Kafka's `route_outcome`.
    group: Option<&str>,
    msg: &Message,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
    ack_wait: Duration,
    shutdown: &CancellationToken,
) {
    let result: Result<()> = match decide_retry(&outcome, retry_count, max_retries) {
        RetryDecision::Ack => {
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack message");
            }
            return;
        }
        RetryDecision::Dlq { reason } => {
            let fail_reason = match reason {
                "rejected" => metrics::FailReason::Rejected,
                _ => metrics::FailReason::MaxRetriesExceeded,
            };
            let pending = metrics::record_terminal(
                topology.queue(),
                group,
                fail_reason,
                topology.dlq().is_some(),
            );
            match publish_to_dlq(client, topology, msg, reason).await {
                Ok(()) => {
                    // The ack is what retires the message; until it lands
                    // JetStream still owns the delivery and will redeliver on
                    // ack-wait expiry, so a failed ack is not a discard.
                    //
                    // `double_ack` rather than `ack` because this is the arm
                    // that decides whether a discard actually happened. `ack`
                    // only publishes `+ACK` to the client connection and
                    // returns as soon as it is written; a connection lost
                    // before JetStream applies it redelivers the message while
                    // `confirm` has already counted it gone. `double_ack`
                    // waits for the server's reply, so `Ok` means the stream
                    // really did retire the delivery. The extra round trip is
                    // paid only on terminal outcomes (rejected or retries
                    // exhausted), not on the happy path above.
                    match msg.double_ack().await {
                        Ok(()) => pending.confirm(),
                        Err(e) => {
                            tracing::error!(error = %e, "failed to ack after DLQ publish");
                            pending.survived();
                        }
                    }
                    return;
                }
                Err(e) => {
                    // Not acked, so JetStream redelivers.
                    pending.survived();
                    Err(e)
                }
            }
        }
        RetryDecision::Hold { increment: true } => {
            let new_count = retry_count + 1;
            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                let idx = hold_index(retry_count, hold_queues.len());
                hold_queues[idx].delay()
            };

            // Hold the original un-acked through the backoff, then durably
            // republish an incremented copy and ack — see
            // `hold_then_republish`. Keeps at-least-once across crashes and
            // republish failures while leaving the retry count header-based.
            hold_then_republish(client, msg, delay, new_count, ack_wait, shutdown).await;
            return;
        }
        RetryDecision::Hold { increment: false } => {
            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                hold_queues[0].delay()
            };
            if let Err(e) = msg.ack_with(AckKind::Nak(Some(delay))).await {
                tracing::error!(error = %e, "failed to nak-with-delay for defer");
            }
            return;
        }
    };

    // If we reach here, a routing operation failed — nak the message for redelivery.
    if let Err(e) = result {
        tracing::error!(error = %e, "routing failed, nak-ing message for redelivery");
        if let Err(nak_err) = msg.ack_with(AckKind::Nak(None)).await {
            tracing::error!(error = %nak_err, "failed to nak message after routing failure");
        }
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

/// Wraps a handler future with optional timeout, emitting inflight/consumed/duration metrics.
/// Returns `Outcome::Retry` on timeout or panic.
///
/// The future is run inside a child `tokio::spawn` so a panic inside the
/// user's handler is caught here (as `JoinError::is_panic`) and surfaced as
/// `Outcome::Retry` with metrics recorded — without this, the spawned task
/// aborts before the metric calls and panicked handlers disappear from the
/// consumed/latency series even though the caller still requeues them.
async fn invoke_handler<F>(
    fut: F,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
) -> Outcome
where
    F: Future<Output = Outcome> + Send + 'static,
{
    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let mut join = tokio::spawn(fut);
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, &mut join).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
            Err(_) => {
                join.abort();
                let resolved = handler_timeout_outcome(timeout_outcome);
                tracing::warn!(outcome = ?resolved, "handler timed out after {duration:?}");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                resolved
            }
        },
        None => match join.await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
        },
    };
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, group, &outcome);
    metrics::record_processing_duration(topic, group, &outcome, elapsed);
    outcome
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps a NATS `GetStreamError` to the appropriate `ShoveError` variant based
/// on the underlying error kind. Only `Request` errors (transient network
/// failures) become `Connection`; everything else is a `Topology` error.
pub(super) fn map_get_stream_error(queue: &str, e: GetStreamError) -> ShoveError {
    match e.kind() {
        GetStreamErrorKind::Request => {
            ShoveError::Connection(format!("failed to get stream {queue}: {e}"))
        }
        _ => ShoveError::Topology(format!("failed to get stream {queue}: {e}")),
    }
}

// Reconnect loop
// ---------------------------------------------------------------------------

/// A consumer that stayed up at least this long before erroring is considered
/// to have had a healthy connection: the reconnect budget and backoff reset,
/// so `max_reconnect_attempts` bounds *consecutive* failures, not lifetime.
const RECONNECT_RESET_AFTER: Duration = Duration::from_secs(60);

/// Runs `f` in a reconnect loop, retrying on transient errors until shutdown
/// or `max_retries` consecutive failures.
pub(super) async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
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
        let started = tokio::time::Instant::now();
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if started.elapsed() >= RECONNECT_RESET_AFTER {
                    attempts = 0;
                    backoff = Backoff::default();
                }
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
                        label,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{label}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    label,
                    attempt = attempts,
                    ?max_reconnect_attempts,
                    error = %e,
                    delay_ms = delay.as_millis() as u64,
                    "consumer error, reconnecting"
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
// NatsConsumer
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct NatsConsumer {
    client: NatsClient,
}

impl NatsConsumer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    /// The underlying client, for the sibling broadcast module — which needs
    /// the raw JetStream context to create and delete its own ephemeral
    /// consumer rather than reading a pre-declared durable one by name.
    pub(super) fn client_ref(&self) -> &NatsClient {
        &self.client
    }
}

impl NatsConsumer {
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Nats>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_with_inner<T, H>(
        &self,
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
        // All tasks in a consumer group bind to the same durable consumer name;
        // the JetStream server load-balances messages across them. The registry
        // pre-configures this consumer with an aggregate `max_ack_pending` so
        // N pullers can actually have N × prefetch messages in flight.
        let consumer_name = super::constants::consumer_name(queue);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let hold_queues = topology.hold_queues();

        let max_message_size = options.max_message_size;
        let max_ack_pending = options.max_ack_pending.unwrap_or(prefetch_count as i64);
        let derived_ack_wait = derive_ack_wait(handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT));

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();

        tracing::info!(
            queue,
            consumer = consumer_name,
            prefetch_count,
            max_ack_pending,
            max_retries,
            "NATS consumer started"
        );

        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let consumer_name = consumer_name.clone();
            let semaphore = semaphore.clone();
            let topic = topic.clone();
            let group = group.clone();
            let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
            async move {
                let stream = client
                    .jetstream()
                    .get_stream(queue)
                    .await
                    .map_err(|e| map_get_stream_error(queue, e))?;

                // Fast path: attach to the pre-declared durable consumer.
                // `NatsTopologyDeclarer::declare_pull_consumer` does the
                // upsert once at consumer-group registration, so the hot
                // path stays read-only and there is no `CONSUMER.CREATE`
                // storm on reconnect — every consumer just reads.
                //
                // Fallback (NotFound only): callers that use
                // `NatsConsumer::run` directly (e.g. tests via
                // `ConsumerSupervisor`) bypass the registry and don't
                // pre-declare. In that case `get_consumer` returns
                // `ConsumerInfoErrorKind::NotFound` and we one-shot
                // `create_consumer` to bootstrap. Any other error
                // (TimedOut, NoResponders, JetStream, …) means the
                // server is in trouble, not that the consumer is
                // missing — propagate so the reconnect loop retries
                // instead of silently misreporting as a create failure.
                let pull_consumer = match stream
                    .get_consumer::<PullConsumerConfig>(&consumer_name)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        let is_not_found = e
                            .downcast_ref::<ConsumerInfoError>()
                            .is_some_and(|ce| {
                                matches!(ce.kind(), ConsumerInfoErrorKind::NotFound)
                            });
                        if !is_not_found {
                            return Err(ShoveError::Connection(format!(
                                "get_consumer({consumer_name}) failed: {e}"
                            )));
                        }
                        stream
                            .create_consumer(PullConsumerConfig {
                                durable_name: Some(consumer_name.clone()),
                                ack_policy: AckPolicy::Explicit,
                                max_ack_pending,
                                ack_wait: derived_ack_wait,
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| {
                                ShoveError::Connection(format!(
                                    "create_consumer({consumer_name}) fallback failed: {e}"
                                ))
                            })?
                    }
                };

                // Effective server-side ack_wait, used to pace the WIP progress
                // acks that keep retried messages alive during their backoff.
                let ack_wait = pull_consumer.cached_info().config.ack_wait;

                let mut messages = pull_consumer.messages().await.map_err(|e| {
                    ShoveError::Connection(format!("failed to get message stream: {e}"))
                })?;

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            return Ok(());
                        }
                        item = messages.next() => {
                            let msg = match item {
                                Some(Ok(msg)) => msg,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, queue, "consumer stream error");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(
                                        format!("consumer stream error on {queue}: {e}"),
                                    ));
                                }
                                None => {
                                    tracing::warn!(queue, "consumer stream closed");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(
                                        format!("consumer stream closed for {queue}"),
                                    ));
                                }
                            };

                            metrics::record_message_size(
                                &topic,
                                group.as_deref(),
                                msg.payload.len(),
                            );

                            // Reject oversized messages before deserialization
                            if let Err(e) = validate_message_size(msg.payload.len(), max_message_size) {
                                tracing::warn!(
                                    error = %e,
                                    queue,
                                    "rejecting oversized message to DLQ"
                                );
                                metrics::record_failed(
                                    &topic,
                                    group.as_deref(),
                                    metrics::FailReason::Oversize,
                                );
                                if let Err(dlq_err) = publish_to_dlq(
                                    &client,
                                    topology,
                                    &msg,
                                    &e.to_string(),
                                ).await {
                                    tracing::error!(
                                        error = %dlq_err,
                                        "failed to publish oversized message to DLQ, nak-ing"
                                    );
                                    let _ = msg.ack_with(AckKind::Nak(None)).await;
                                    continue;
                                }
                                let _ = msg.ack().await;
                                continue;
                            }

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        queue,
                                        "failed to deserialize message, sending to DLQ"
                                    );
                                    metrics::record_failed(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Deserialize,
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        &msg,
                                        &format!("deserialization_error: {e}"),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish bad message to DLQ, nak-ing"
                                        );
                                        let _ = msg.ack_with(AckKind::Nak(None)).await;
                                        continue;
                                    }
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            };

                            let metadata = extract_message_metadata(&msg);
                            let retry_count = metadata.retry_count;

                            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;

                            let task_handler = handler.clone();
                            let task_ctx = ctx.clone();
                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;
                            let task_topic = topic.clone();
                            let task_group = group.clone();
                            let task_shutdown = shutdown.clone();
                            let task_timeout_outcome = handler_timeout_outcome_cfg.clone();

                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = invoke_handler(
                                    async move {
                                        task_handler.handle(payload, metadata, task_ctx.as_ref()).await
                                    },
                                    handler_timeout,
                                    task_timeout_outcome,
                                    &task_topic,
                                    task_group.as_deref(),
                                )
                                .await;

                                route_outcome(
                                    &task_client,
                                    task_group.as_deref(),
                                    &msg,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    ack_wait,
                                    &task_shutdown,
                                )
                                .await;

                                drop(permit);
                                // Only report idle when ALL permits are available (no other tasks in-flight)
                                if task_semaphore.available_permits() == task_prefetch as usize {
                                    task_processing.store(false, Ordering::Release);
                                }
                            });
                        }
                    }
                }
            }
        })
        .await
    }

    pub async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Nats>,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        self.run_fifo_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Nats>,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        self.run_fifo_until_timeout_with_inner::<T, H, S>(
            handler,
            ctx,
            options.into_inner(),
            signal,
            drain_timeout,
        )
        .await
    }

    pub(crate) async fn run_fifo_until_timeout_with_inner<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        let shutdown = options.shutdown.clone();
        let handles = match self.spawn_fifo_shards::<T, H>(handler, ctx, options).await {
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

    pub(crate) async fn run_fifo_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let handles = self
            .spawn_fifo_shards::<T, H>(handler, ctx, options)
            .await?;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!("NATS sequenced shard task failed: {e}"),
                Err(e) => tracing::error!("NATS sequenced shard task panicked: {e}"),
            }
        }
        Ok(())
    }

    /// Spawn one task per routing shard and return the join handles.
    ///
    /// Each shard task internally returns `()` (errors are logged within the
    /// task), so each handle is wrapped to produce `Result<()>` for a uniform
    /// handle type across backends.
    ///
    /// The `pub(crate)` visibility is required for Phase 2 (Task 16), which
    /// calls this from the consumer-group module.
    pub(crate) async fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        let seq_config = topology
            .sequencing()
            .expect("run_fifo requires a sequenced topology");
        let routing_shards = seq_config.routing_shards();
        let on_failure = seq_config.on_failure();

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();
        let derived_ack_wait = derive_ack_wait(handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT));

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        let max_reconnect_attempts = options.max_reconnect_attempts;

        tracing::info!(
            queue,
            routing_shards,
            max_retries,
            "NATS FIFO consumer started"
        );

        let mut shard_tasks: Vec<tokio::task::JoinHandle<Result<()>>> =
            Vec::with_capacity(routing_shards as usize);

        for shard in 0..routing_shards {
            let consumer_name = format!("{queue}-shard-{shard}");
            let filter_subject = format!("{queue}.shard.{shard}");

            let shard_handler = handler.clone();
            let shard_ctx = ctx.clone();
            let shard_client = client.clone();
            let shard_shutdown = shutdown.clone();
            let shard_processing = processing.clone();
            let shard_topic = topic.clone();
            let shard_group = group.clone();
            let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
            // One poison set per shard task, created outside the reconnect
            // wrapper so a broker blip cannot un-poison a failed key. A
            // sequence key always hashes to the same shard, so per-shard
            // tracking sees every message that key will produce here.
            let shard_poisoned = PoisonedKeys::new(on_failure);

            let task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                run_with_reconnect(&shard_shutdown, &consumer_name, max_reconnect_attempts, || {
                    let shard_client = shard_client.clone();
                    let shard_handler = shard_handler.clone();
                    let shard_ctx = shard_ctx.clone();
                    let shard_shutdown = shard_shutdown.clone();
                    let shard_processing = shard_processing.clone();
                    let shard_topic = shard_topic.clone();
                    let shard_group = shard_group.clone();
                    let consumer_name = consumer_name.clone();
                    let filter_subject = filter_subject.clone();
                    let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
                    let shard_poisoned = shard_poisoned.clone();
                    async move {
                        let stream = shard_client
                            .jetstream()
                            .get_stream(queue)
                            .await
                            .map_err(|e| map_get_stream_error(queue, e))?;

                        // Note: `get_or_create_consumer` returns an existing
                        // durable verbatim — only newly created shard
                        // consumers get the derived ack_wait; pre-existing
                        // ones keep their stored value until recreated.
                        let pull_consumer = stream
                            .get_or_create_consumer(
                                &consumer_name,
                                PullConsumerConfig {
                                    durable_name: Some(consumer_name.clone()),
                                    filter_subject: filter_subject.clone(),
                                    ack_policy: AckPolicy::Explicit,
                                    max_ack_pending: 1,
                                    ack_wait: derived_ack_wait,
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(|e| {
                                ShoveError::Connection(format!(
                                    "failed to create shard consumer {consumer_name}: {e}"
                                ))
                            })?;

                        let ack_wait = pull_consumer.cached_info().config.ack_wait;

                        let mut messages = pull_consumer.messages().await.map_err(|e| {
                            ShoveError::Connection(format!(
                                "failed to get message stream for shard {shard}: {e}"
                            ))
                        })?;

                        loop {
                            tokio::select! {
                                _ = shard_shutdown.cancelled() => {
                                    tracing::info!(shard, "shutdown signal received, stopping shard consumer");
                                    return Ok(());
                                }
                                item = messages.next() => {
                                    let msg = match item {
                                        Some(Ok(msg)) => msg,
                                        Some(Err(e)) => {
                                            tracing::error!(error = %e, shard, "shard consumer stream error");
                                            metrics::record_backend_error(
                                                metrics::BackendLabel::Nats,
                                                metrics::BackendErrorKind::Consume,
                                            );
                                            return Err(ShoveError::Connection(
                                                format!("shard {shard} stream error: {e}"),
                                            ));
                                        }
                                        None => {
                                            tracing::warn!(shard, "shard consumer stream closed");
                                            metrics::record_backend_error(
                                                metrics::BackendLabel::Nats,
                                                metrics::BackendErrorKind::Consume,
                                            );
                                            return Err(ShoveError::Connection(
                                                format!("shard {shard} stream closed"),
                                            ));
                                        }
                                    };

                                    metrics::record_message_size(
                                        &shard_topic,
                                        shard_group.as_deref(),
                                        msg.payload.len(),
                                    );

                                    // ── FailAll: skip poisoned keys ──
                                    // Inert unless this topic is configured
                                    // `SequenceFailure::FailAll`.
                                    let seq_key = get_sequence_key(&msg.headers);
                                    if shard_poisoned.is_poisoned(&seq_key) {
                                        tracing::warn!(
                                            shard,
                                            sequence_key = %seq_key,
                                            "sequence key poisoned (FailAll) — sending to DLQ without invoking handler"
                                        );
                                        // Collateral of an already-counted
                                        // failure, so the failure half is
                                        // deliberately not counted again — see
                                        // `metrics::FailReason`. The discard
                                        // half still applies: a cascaded
                                        // message dropped with no DLQ is just
                                        // as gone as any other.
                                        let pending = metrics::pending_discard(
                                            &shard_topic,
                                            shard_group.as_deref(),
                                            metrics::FailReason::Rejected,
                                            topology.dlq().is_some(),
                                        );
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &shard_client,
                                            topology,
                                            &msg,
                                            "rejected",
                                        ).await {
                                            tracing::error!(
                                                error = %dlq_err,
                                                "failed to publish poisoned-key message to DLQ, nak-ing"
                                            );
                                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                                            // Nak-ed, so JetStream redelivers.
                                            pending.survived();
                                            continue;
                                        }
                                        if topology.dlq().is_some() {
                                            // The message is in the DLQ, so it
                                            // exists whatever the ack does and
                                            // `confirm` could never count it.
                                            // Settling now keeps the
                                            // dead-lettered cascade off the
                                            // `double_ack` round trip the live
                                            // pending record below needs — and
                                            // a poisoned key drains its whole
                                            // backlog through this branch, so
                                            // that tax is per message.
                                            pending.survived();
                                            let _ = msg.ack().await;
                                            continue;
                                        }
                                        // No DLQ, so this ack is what drops the
                                        // message and it decides the discard
                                        // accounting. `double_ack` rather than
                                        // `ack`: `ack` returns as soon as
                                        // `+ACK` is written to the connection,
                                        // so a connection lost before JetStream
                                        // applies it would redeliver a message
                                        // already counted gone. Same reasoning
                                        // as `route_outcome`'s Dlq arm.
                                        match msg.double_ack().await {
                                            Ok(()) => pending.confirm(),
                                            Err(e) => {
                                                tracing::error!(
                                                    error = %e,
                                                    "failed to ack poisoned-key message after DLQ publish"
                                                );
                                                pending.survived();
                                            }
                                        }
                                        continue;
                                    }

                                    // Reject oversized messages before deserialization
                                    if let Err(e) = validate_message_size(msg.payload.len(), max_message_size) {
                                        tracing::warn!(
                                            error = %e,
                                            shard,
                                            "rejecting oversized message to DLQ"
                                        );
                                        metrics::record_failed(
                                            &shard_topic,
                                            shard_group.as_deref(),
                                            metrics::FailReason::Oversize,
                                        );
                                        poison_key(&shard_poisoned, &seq_key, queue);
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &shard_client,
                                            topology,
                                            &msg,
                                            &e.to_string(),
                                        ).await {
                                            tracing::error!(
                                                error = %dlq_err,
                                                "failed to publish oversized message to DLQ, nak-ing"
                                            );
                                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                                            continue;
                                        }
                                        let _ = msg.ack().await;
                                        continue;
                                    }

                                    // Deserialize payload; reject to DLQ on failure
                                    let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                shard,
                                                "failed to deserialize message, sending to DLQ"
                                            );
                                            metrics::record_failed(
                                                &shard_topic,
                                                shard_group.as_deref(),
                                                metrics::FailReason::Deserialize,
                                            );
                                            poison_key(&shard_poisoned, &seq_key, queue);
                                            if let Err(dlq_err) = publish_to_dlq(
                                                &shard_client,
                                                topology,
                                                &msg,
                                                &format!("deserialization_error: {e}"),
                                            ).await {
                                                tracing::error!(
                                                    error = %dlq_err,
                                                    "failed to publish bad message to DLQ, nak-ing"
                                                );
                                                let _ = msg.ack_with(AckKind::Nak(None)).await;
                                                continue;
                                            }
                                            let _ = msg.ack().await;
                                            continue;
                                        }
                                    };

                                    let metadata = extract_message_metadata(&msg);
                                    let retry_count = metadata.retry_count;

                                    shard_processing.store(true, Ordering::Release);

                                    let outcome = {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        let h = shard_handler.clone();
                                        let c = shard_ctx.clone();
                                        let spawn_topic = shard_topic.clone();
                                        let spawn_group = shard_group.clone();
                                        let spawn_timeout_outcome = handler_timeout_outcome_cfg.clone();
                                        tokio::spawn(async move {
                                            let o = invoke_handler(
                                                async move { h.handle(payload, metadata, c.as_ref()).await },
                                                handler_timeout,
                                                spawn_timeout_outcome,
                                                &spawn_topic,
                                                spawn_group.as_deref(),
                                            ).await;
                                            let _ = tx.send(o);
                                        });
                                        rx.await.unwrap_or_else(|_| {
                                            tracing::warn!(shard, "handler task panicked, retrying message");
                                            Outcome::Retry
                                        })
                                    };
                                    let outcome = adjust_outcome_for_fifo(outcome);

                                    // FailAll: a DLQ-terminal outcome poisons
                                    // the key, so every later message for it is
                                    // dead-lettered instead of handled.
                                    if matches!(
                                        decide_retry(&outcome, retry_count, max_retries),
                                        RetryDecision::Dlq { .. }
                                    ) {
                                        poison_key(&shard_poisoned, &seq_key, queue);
                                    }

                                    route_outcome(
                                        &shard_client,
                                        shard_group.as_deref(),
                                        &msg,
                                        outcome,
                                        topology,
                                        retry_count,
                                        max_retries,
                                        hold_queues,
                                        ack_wait,
                                        &shard_shutdown,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                })
                .await
            });

            shard_tasks.push(task);
        }

        Ok(shard_tasks)
    }

    pub async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let dlq = topology.dlq().ok_or_else(|| {
            ShoveError::Topology("run_dlq requires a DLQ to be configured".into())
        })?;

        let dlq_consumer_name = super::constants::dlq_consumer_name(dlq);
        let shutdown = self.client.shutdown_token();
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        // `shove_message_size_bytes` labels a DLQ drain with the SOURCE topic,
        // not the DLQ stream name, and with no consumer group — `run_dlq`
        // takes no `ConsumerOptions`, so there is no group to carry, and the
        // internal `{dlq}-consumer` durable is not one. Redis already drains
        // its DLQ through `run_stream_loop`, which labels every metric
        // `topology.queue()` whichever stream it reads; a DLQ name here would
        // make `topic` mean two different things depending on the backend, and
        // would stop a per-topic size profile summing across the main and DLQ
        // paths. The DLQ drain stays distinguishable through the metrics that
        // exist to name it, not by overloading `topic`.
        let topic: Arc<str> = Arc::from(topology.queue());

        tracing::info!(
            dlq,
            consumer = dlq_consumer_name,
            "NATS DLQ consumer started"
        );

        run_with_reconnect(&shutdown, dlq, None, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let dlq_consumer_name = dlq_consumer_name.clone();
            let topic = topic.clone();
            async move {
                let stream = client
                    .jetstream()
                    .get_stream(dlq)
                    .await
                    .map_err(|e| map_get_stream_error(dlq, e))?;

                let pull_consumer = stream
                    .get_or_create_consumer(
                        &dlq_consumer_name,
                        PullConsumerConfig {
                            durable_name: Some(dlq_consumer_name.clone()),
                            ack_policy: AckPolicy::Explicit,
                            // run_dlq has no handler-timeout knob; give the
                            // DLQ durable the same margin a default-timeout
                            // consumer gets.
                            ack_wait: derive_ack_wait(DEFAULT_HANDLER_TIMEOUT),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!(
                            "failed to create DLQ consumer {dlq_consumer_name}: {e}"
                        ))
                    })?;

                let mut messages = pull_consumer.messages().await.map_err(|e| {
                    ShoveError::Connection(format!("failed to get DLQ message stream: {e}"))
                })?;

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(dlq, "shutdown signal received, stopping DLQ consumer");
                            return Ok(());
                        }
                        item = messages.next() => {
                            let msg = match item {
                                Some(Ok(msg)) => msg,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, dlq, "DLQ consumer stream error");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(
                                        format!("DLQ consumer stream error on {dlq}: {e}"),
                                    ));
                                }
                                None => {
                                    tracing::warn!(dlq, "DLQ consumer stream closed");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(
                                        format!("DLQ consumer stream closed for {dlq}"),
                                    ));
                                }
                            };

                            // Before the size gate, exactly as on the main loop: the
                            // histogram describes what arrived on the wire, so the
                            // payload the gate is about to discard is precisely the
                            // sample an operator sizing `max_message_size` needs.
                            metrics::record_message_size(&topic, None, msg.payload.len());

                            // Discard oversized DLQ messages
                            if msg.payload.len() > DEFAULT_MAX_MESSAGE_SIZE {
                                tracing::warn!(
                                    bytes = msg.payload.len(),
                                    max = DEFAULT_MAX_MESSAGE_SIZE,
                                    dlq,
                                    "oversized DLQ message — discarding"
                                );
                                let _ = msg.ack().await;
                                continue;
                            }

                            // Deserialize payload; on failure, log and ack anyway
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        dlq,
                                        "failed to deserialize DLQ message, acking anyway"
                                    );
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            };

                            let metadata = extract_dead_metadata(&msg);

                            handler.handle_dead(payload, metadata, ctx.as_ref()).await;

                            // Always ack after handle_dead completes
                            if let Err(e) = msg.ack().await {
                                tracing::error!(error = %e, dlq, "failed to ack DLQ message");
                            }
                        }
                    }
                }
            }
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// Batch consumption
// ---------------------------------------------------------------------------

/// `ack_wait` for the batch consumer's fallback-created durable, and the
/// threshold the pre-declared-consumer warning compares against.
///
/// A message can be delivered at the very start of a pull window and then
/// wait the whole `max_batch_age` before its flush even begins, so the
/// single-path derivation ([`derive_ack_wait`]: 3x the handler timeout,
/// floored at the JetStream default) under-covers a batch: the pull window is
/// added on top.
///
/// `handler_timeout: None`
/// ([`BatchConsumerOptions::without_handler_timeout`](crate::BatchConsumerOptions::without_handler_timeout))
/// still derives from [`DEFAULT_HANDLER_TIMEOUT`]: an unbounded flush has no
/// number to derive from, so a sink that legitimately flushes longer than
/// that margin will see mid-flight redelivery (duplicates, never loss) unless
/// its durable is pre-declared with a larger `ack_wait`. Same wart as the
/// single-message fallback, which also derives from the default in that case.
pub(super) fn derive_batch_ack_wait(
    max_batch_age: Duration,
    handler_timeout: Option<Duration>,
) -> Duration {
    max_batch_age.saturating_add(derive_ack_wait(
        handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT),
    ))
}

/// How much consecutive zero-message pull time the batch loop rides out
/// before probing the server with a `consumer.info()` round trip — see
/// [`empty_windows_before_probe`].
const BATCH_LIVENESS_INTERVAL: Duration = Duration::from_secs(30);

/// Consecutive zero-message pull windows before a liveness probe.
///
/// A dead connection ends a batch pull stream exactly like an idle topic
/// does: async-nats 0.49.1 terminates the stream cleanly on its client-side
/// backstop timer rather than yielding an error, and `idle_heartbeat` stays
/// unset because that version's heartbeat arm returns `Pending` without
/// registering a waker (a lost wakeup that stalls the stream). Without a
/// probe, the loop would ride out an outage as an endless run of "idle"
/// windows and `max_reconnect_attempts` could never fire. Once
/// [`BATCH_LIVENESS_INTERVAL`] of consecutive emptiness accumulates, one
/// `consumer.info()` round trip either proves the server is reachable or
/// converts the silent outage into a counted `Connection` error. Any received
/// message resets the count.
fn empty_windows_before_probe(max_batch_age: Duration) -> u32 {
    let age_ms = max_batch_age.as_millis().max(1);
    let interval_ms = BATCH_LIVENESS_INTERVAL.as_millis();
    u32::try_from(interval_ms.div_ceil(age_ms))
        .unwrap_or(u32::MAX)
        .max(1)
}

/// Bounded concurrency for the per-message reject settlement in
/// [`settle_reject_batch`]. A rejected batch settles with one DLQ publish
/// plus one server-confirmed ack per message; running them strictly
/// sequentially would put a `max_batch_size`-sized tail at risk of hitting
/// `ack_wait` mid-settle (a duplicate-DLQ window, never loss). Sixteen keeps
/// that window small without turning a reject into a thundering herd.
const REJECT_SETTLE_CONCURRENCY: usize = 16;

/// Everything a batch settle arm needs, bundled so the helpers stay at sane
/// arities — the same shape as Kafka's `BatchFlushCtx`.
struct NatsBatchCtx<'a> {
    client: &'a NatsClient,
    topology: &'static QueueTopology,
    topic: &'a str,
    group: Option<&'a str>,
}

/// Pre-handler intake for one pulled message: size-gate, decode, extract
/// metadata. Poison (oversized or undecodable) is settled immediately —
/// `record_failed` + DLQ publish + ack, with a Nak if the publish fails —
/// exactly like the single-message path's pre-handler arms. Immediate
/// settlement is safe here where Kafka has to defer its poison DLQ publishes
/// to the commit: acks are per message, so a later batch redelivery can never
/// replay a message that was individually acked, and the DLQ cannot collect
/// duplicate copies of it.
async fn ingest_batch_message<T: Topic>(
    ctx: &NatsBatchCtx<'_>,
    msg: Message,
    max_message_size: Option<usize>,
) -> Option<(T::Message, MessageMetadata, Message)> {
    metrics::record_message_size(ctx.topic, ctx.group, msg.payload.len());

    if let Err(e) = validate_message_size(msg.payload.len(), max_message_size) {
        tracing::warn!(
            error = %e,
            queue = ctx.topology.queue(),
            "oversized message dropped from the batch, rejecting to DLQ"
        );
        metrics::record_failed(ctx.topic, ctx.group, metrics::FailReason::Oversize);
        settle_poison(ctx, &msg, &e.to_string()).await;
        return None;
    }

    let payload: T::Message =
        match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    queue = ctx.topology.queue(),
                    "failed to deserialize message, dropped from the batch and sent to DLQ"
                );
                metrics::record_failed(ctx.topic, ctx.group, metrics::FailReason::Deserialize);
                settle_poison(ctx, &msg, &format!("deserialization_error: {e}")).await;
                return None;
            }
        };

    let metadata = extract_message_metadata(&msg);
    Some((payload, metadata, msg))
}

/// Retire one pre-handler drop: DLQ publish then ack, Nak for redelivery if
/// the publish fails. Mirrors the single-message path's oversize/deserialize
/// arms; like them it does no terminal accounting (`record_failed` already
/// counted the drop, and pre-handler drops are not part of the
/// terminal-discard contract on any backend).
async fn settle_poison(ctx: &NatsBatchCtx<'_>, msg: &Message, reason: &str) {
    if let Err(dlq_err) = publish_to_dlq(ctx.client, ctx.topology, msg, reason).await {
        tracing::error!(
            error = %dlq_err,
            "failed to publish poison batch message to DLQ, nak-ing"
        );
        if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
            tracing::error!(error = %e, "failed to nak poison batch message");
        }
        return;
    }
    if let Err(e) = msg.ack().await {
        tracing::error!(error = %e, "failed to ack poison batch message after DLQ publish");
    }
}

/// `Ack`: retire every message. Plain per-message acks, the same guarantee
/// the single-message happy path gives — a failed ack logs and the message
/// redelivers on `ack_wait` expiry (a duplicate, never loss).
async fn ack_batch(acks: &[Message]) {
    for msg in acks {
        if let Err(e) = msg.ack().await {
            tracing::error!(error = %e, "failed to ack batch message");
        }
    }
}

/// `Reject`: per message, the exact terminal contract of the single-message
/// path's DLQ arm — `record_terminal`, publish to the DLQ, then `double_ack`
/// (the server-confirmed ack is what decides whether a discard really
/// happened, so `confirm` waits for it and a failed ack `survived`s instead).
/// A failed DLQ publish leaves the message un-acked and Naks it for immediate
/// redelivery, so nothing is ever discarded without a copy landing first.
///
/// Each message settles independently: a flush interrupted partway leaves the
/// remainder un-acked, and JetStream redelivers them on `ack_wait` expiry —
/// no message can be left in limbo. Settlement runs at
/// [`REJECT_SETTLE_CONCURRENCY`] to bound how long a large batch's tail
/// stays un-acked behind the sequential publish+confirm round trips.
async fn settle_reject_batch(ctx: &NatsBatchCtx<'_>, acks: &[Message]) {
    let has_dlq = ctx.topology.dlq().is_some();
    stream::iter(acks.iter())
        .for_each_concurrent(REJECT_SETTLE_CONCURRENCY, |msg| async move {
            let pending = metrics::record_terminal(
                ctx.topic,
                ctx.group,
                metrics::FailReason::Rejected,
                has_dlq,
            );
            match publish_to_dlq(ctx.client, ctx.topology, msg, "rejected").await {
                Ok(()) => match msg.double_ack().await {
                    Ok(()) => pending.confirm(),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "failed to ack rejected batch message after DLQ publish"
                        );
                        pending.survived();
                    }
                },
                Err(e) => {
                    // Not acked, so JetStream redelivers.
                    pending.survived();
                    tracing::error!(
                        error = %e,
                        "DLQ publish failed for rejected batch message, nak-ing"
                    );
                    if let Err(nak_err) = msg.ack_with(AckKind::Nak(None)).await {
                        tracing::error!(
                            error = %nak_err,
                            "failed to nak rejected batch message"
                        );
                    }
                }
            }
        })
        .await;
}

/// `Retry`/`Defer`: Nak every message so the server redelivers the whole
/// batch after `delay` (`None` = immediately). A batch redelivery is a
/// re-buffer, never a republish: retry counts stay untouched and no hold
/// queue is involved — the backend-declared `MaxDeliver` cap is the only
/// bound, per the shared settlement table.
async fn nak_batch(acks: &[Message], delay: Option<Duration>) {
    for msg in acks {
        if let Err(e) = msg.ack_with(AckKind::Nak(delay)).await {
            tracing::error!(error = %e, "failed to nak batch message for redelivery");
        }
    }
}

impl NatsConsumer {
    /// The NATS body of
    /// [`BatchConsumer::run`](crate::batch_consumer::BatchConsumer::run),
    /// reachable only through that wrapper — which runs the sequencing guard
    /// (`validate_batch_topic`), so per the
    /// [`BatchConsumerImpl`](crate::backend::BatchConsumerImpl) contract it
    /// is not repeated here (the InMemory model; Kafka differs only because
    /// it also exposes its own public entry point).
    ///
    /// # Batching maps onto the pull-batch wire primitive
    ///
    /// One JetStream pull request per batch: up to `max_batch_size` messages,
    /// expiring after `max_batch_age`. The server ends the request at
    /// whichever comes first, so the size/age flush pair needs no local
    /// accumulator or timer — a flush happens at most `max_batch_age` after
    /// the *request* was issued, which is always at or before "age since the
    /// first message in the batch". The cost is one pull request per
    /// `max_batch_age` on an idle topic (250ms windows by default, against
    /// the single path's long-lived pull) — cheap, but visible in server
    /// request counters.
    ///
    /// # The durable consumer and `max_ack_pending`
    ///
    /// Binds the same durable (`{queue}-consumer`) as the single-message
    /// path. The fallback `create_consumer` — which applies **only** when
    /// nothing pre-declared that durable — sizes `max_ack_pending` to
    /// `max_batch_size` and `ack_wait` to [`derive_batch_ack_wait`]. A
    /// pre-declared durable keeps whatever the registry gave it; since the
    /// server stops delivering at the unacked budget and this loop acks only
    /// at flush, the per-pull size is **clamped to the durable's
    /// `max_ack_pending`** (when positive and smaller) with a startup warning
    /// — the clamp plus the request expiry guarantees forward progress in
    /// ≤budget-sized batches rather than a silent deadlock. A second warning
    /// fires when the durable's `ack_wait` is below the batch window plus the
    /// handler-timeout margin, where a slow flush risks mid-flight redelivery
    /// (duplicates, never loss).
    ///
    /// A batch consumer and a single-message group can legally share the
    /// durable: JetStream splits deliveries arbitrarily across their pull
    /// requests and both draw on one ack budget. During a migration that
    /// means some messages are handled per-message while others arrive in
    /// batches — run one shape at a time unless that is intended.
    pub(crate) async fn run_batch_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        let consumer_name = super::constants::consumer_name(queue);

        let shutdown = options.shutdown.clone();
        let max_batch_size = options.max_batch_size;
        let max_batch_age = options.max_batch_age;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;
        let batch_ack_wait = derive_batch_ack_wait(max_batch_age, handler_timeout);
        let probe_after = empty_windows_before_probe(max_batch_age);

        let handler = Arc::new(handler);
        let hctx = Arc::new(ctx);
        let client = self.client.clone();
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();

        tracing::info!(
            queue,
            consumer = consumer_name,
            max_batch_size,
            ?max_batch_age,
            "NATS batch consumer started"
        );

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
            let handler = handler.clone();
            let hctx = hctx.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let consumer_name = consumer_name.clone();
            let topic = topic.clone();
            let group = group.clone();
            let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
            async move {
                let stream = client
                    .jetstream()
                    .get_stream(queue)
                    .await
                    .map_err(|e| map_get_stream_error(queue, e))?;

                // Same fast-path/fallback split as `run_with_inner`: attach to
                // the pre-declared durable, and only bootstrap one (NotFound
                // alone) with batch-derived config when nothing declared it.
                let mut pull_consumer = match stream
                    .get_consumer::<PullConsumerConfig>(&consumer_name)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        let is_not_found = e
                            .downcast_ref::<ConsumerInfoError>()
                            .is_some_and(|ce| matches!(ce.kind(), ConsumerInfoErrorKind::NotFound));
                        if !is_not_found {
                            return Err(ShoveError::Connection(format!(
                                "get_consumer({consumer_name}) failed: {e}"
                            )));
                        }
                        stream
                            .create_consumer(PullConsumerConfig {
                                durable_name: Some(consumer_name.clone()),
                                ack_policy: AckPolicy::Explicit,
                                max_ack_pending: i64::try_from(max_batch_size).unwrap_or(i64::MAX),
                                ack_wait: batch_ack_wait,
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| {
                                ShoveError::Connection(format!(
                                    "create_consumer({consumer_name}) fallback failed: {e}"
                                ))
                            })?
                    }
                };

                let (budget, ack_wait) = {
                    let config = &pull_consumer.cached_info().config;
                    (config.max_ack_pending, config.ack_wait)
                };
                let effective_batch_size = match usize::try_from(budget) {
                    Ok(b) if b > 0 && b < max_batch_size => {
                        tracing::warn!(
                            queue,
                            max_batch_size,
                            max_ack_pending = budget,
                            "clamping the per-pull batch size to the consumer's \
                             max_ack_pending — a batch larger than the unacked budget \
                             can never fill, and every flush would wait out the full \
                             window"
                        );
                        b
                    }
                    // Zero/negative budget means unbounded, and a budget at or
                    // above the batch size needs no clamp.
                    _ => max_batch_size,
                };
                if !ack_wait.is_zero() && ack_wait < batch_ack_wait {
                    tracing::warn!(
                        queue,
                        ?ack_wait,
                        required = ?batch_ack_wait,
                        "consumer ack_wait is below the batch window plus the \
                         handler-timeout margin; a slow flush risks mid-flight \
                         redelivery (duplicates, never loss)"
                    );
                }

                let flush_ctx = NatsBatchCtx {
                    client: &client,
                    topology,
                    topic: topic.as_ref(),
                    group: group.as_deref(),
                };
                let mut redelivery_backoff = batch_redelivery_backoff();
                let mut empty_windows: u32 = 0;

                loop {
                    if shutdown.is_cancelled() {
                        tracing::info!(queue, "shutdown signal received, batch consumer stopped");
                        return Ok(());
                    }

                    let mut batch_stream = pull_consumer
                        .batch()
                        .max_messages(effective_batch_size)
                        .expires(max_batch_age)
                        .messages()
                        .await
                        .map_err(|e| {
                            ShoveError::Connection(format!(
                                "batch pull request on {queue} failed: {e}"
                            ))
                        })?;

                    let prealloc = effective_batch_size.min(PREALLOC_CAP);
                    let mut buffer: Vec<(T::Message, MessageMetadata)> =
                        Vec::with_capacity(prealloc);
                    let mut acks: Vec<Message> = Vec::with_capacity(prealloc);
                    let mut saw_messages = false;
                    let mut interrupted = false;

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                interrupted = true;
                                break;
                            }
                            item = batch_stream.next() => match item {
                                None => break,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, queue, "batch pull stream error");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(format!(
                                        "batch pull stream error on {queue}: {e}"
                                    )));
                                }
                                Some(Ok(msg)) => {
                                    saw_messages = true;
                                    if let Some((payload, metadata, msg)) =
                                        ingest_batch_message::<T>(
                                            &flush_ctx,
                                            msg,
                                            max_message_size,
                                        )
                                        .await
                                    {
                                        buffer.push((payload, metadata));
                                        acks.push(msg);
                                    }
                                }
                            }
                        }
                    }

                    if interrupted {
                        // Best-effort: Nak whatever the open pull request
                        // already delivered beyond the buffer, so a restarted
                        // process sees it immediately instead of after
                        // ack_wait. Anything still in flight from the server
                        // redelivers on ack_wait expiry — at-least-once holds
                        // either way.
                        while let Some(Some(Ok(msg))) = batch_stream.next().now_or_never() {
                            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                tracing::error!(
                                    error = %e,
                                    "failed to nak undelivered batch message at shutdown"
                                );
                            }
                        }
                    } else if saw_messages {
                        empty_windows = 0;
                    } else {
                        // A silent outage ends windows exactly like an idle
                        // topic — see `empty_windows_before_probe`.
                        empty_windows += 1;
                        if empty_windows >= probe_after {
                            pull_consumer.info().await.map_err(|e| {
                                ShoveError::Connection(format!(
                                    "batch liveness probe on {queue} failed: {e}"
                                ))
                            })?;
                            empty_windows = 0;
                        }
                    }

                    if !buffer.is_empty() {
                        let batch_size = buffer.len();
                        let messages = std::mem::take(&mut buffer);
                        let outcome = invoke_batch_handler(
                            // Closure, not a ready-made future: `handle_batch`
                            // may panic while building it, and that has to
                            // happen inside the guard.
                            || handler.handle_batch(messages, hctx.as_ref()),
                            handler_timeout,
                            handler_timeout_outcome_cfg.clone(),
                            &topic,
                            group.as_deref(),
                            batch_size as u64,
                        )
                        .await;

                        match settle_batch_outcome(&outcome) {
                            BatchSettlement::Commit => {
                                ack_batch(&acks).await;
                                redelivery_backoff = batch_redelivery_backoff();
                                tracing::debug!(queue, batch_size, "batch acked");
                            }
                            BatchSettlement::DeadLetter => {
                                if topology.dlq().is_some() {
                                    tracing::warn!(
                                        queue,
                                        batch_size,
                                        "batch rejected, routed to the DLQ"
                                    );
                                } else {
                                    tracing::warn!(
                                        queue,
                                        batch_size,
                                        "batch rejected but no DLQ is configured, the \
                                         messages are discarded"
                                    );
                                }
                                settle_reject_batch(&flush_ctx, &acks).await;
                                redelivery_backoff = batch_redelivery_backoff();
                            }
                            BatchSettlement::Redeliver => {
                                if shutdown.is_cancelled() {
                                    // No delay and no local sleep at shutdown:
                                    // a restarted process should see the batch
                                    // immediately.
                                    nak_batch(&acks, None).await;
                                } else {
                                    let delay = next_redelivery_delay(&mut redelivery_backoff);
                                    tracing::warn!(
                                        queue,
                                        batch_size,
                                        ?outcome,
                                        delay_ms = delay.as_millis() as u64,
                                        "batch handler returned a non-Ack outcome, \
                                         redelivering the whole batch"
                                    );
                                    nak_batch(&acks, Some(delay)).await;
                                    // Pace the next pull to the same delay the
                                    // Naks carry: the batch becomes eligible
                                    // right as the loop resumes, and a
                                    // wedged-sink loop cannot hammer fresh
                                    // messages in the meantime.
                                    tokio::select! {
                                        _ = tokio::time::sleep(delay) => {}
                                        _ = shutdown.cancelled() => {}
                                    }
                                }
                            }
                        }
                    }

                    if interrupted {
                        tracing::info!(queue, "shutdown signal received, batch consumer stopped");
                        return Ok(());
                    }
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod ack_wait_tests {
    use super::*;

    #[test]
    fn default_handler_timeout_gets_triple_margin() {
        assert_eq!(
            derive_ack_wait(Duration::from_secs(30)),
            Duration::from_secs(90)
        );
    }

    #[test]
    fn short_handler_timeout_floors_at_server_default() {
        assert_eq!(
            derive_ack_wait(Duration::from_secs(10)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn long_handler_timeout_scales_linearly() {
        assert_eq!(
            derive_ack_wait(Duration::from_secs(120)),
            Duration::from_secs(360)
        );
    }
}

/// [`derive_batch_ack_wait`] and [`empty_windows_before_probe`]: the two
/// batch-only derivations layered on the single-path `derive_ack_wait`.
#[cfg(test)]
mod batch_derivation_tests {
    use super::*;

    /// The batch window is added on top of the single-path margin: a message
    /// can wait the whole window before its flush even starts.
    #[test]
    fn batch_ack_wait_adds_the_window_to_the_single_path_margin() {
        assert_eq!(
            derive_batch_ack_wait(Duration::from_secs(2), Some(Duration::from_secs(30))),
            Duration::from_secs(92)
        );
    }

    /// A short handler timeout still floors at the server default before the
    /// window is added — same floor as the single path.
    #[test]
    fn batch_ack_wait_floors_the_margin_at_the_server_default() {
        assert_eq!(
            derive_batch_ack_wait(Duration::from_millis(250), Some(Duration::from_secs(1))),
            Duration::from_millis(30_250)
        );
    }

    /// `without_handler_timeout` has no number to derive from, so the default
    /// stands in — the documented wart for slow unbounded sinks.
    #[test]
    fn no_handler_timeout_derives_from_the_default() {
        assert_eq!(
            derive_batch_ack_wait(Duration::from_secs(1), None),
            Duration::from_secs(1) + derive_ack_wait(DEFAULT_HANDLER_TIMEOUT)
        );
    }

    /// The default 250ms window probes after 120 consecutive empty windows —
    /// 30s of accumulated silence.
    #[test]
    fn probe_threshold_accumulates_the_liveness_interval() {
        assert_eq!(empty_windows_before_probe(Duration::from_millis(250)), 120);
    }

    /// A window at or above the interval probes on every empty window rather
    /// than never.
    #[test]
    fn probe_threshold_floors_at_one_window() {
        assert_eq!(empty_windows_before_probe(Duration::from_secs(30)), 1);
        assert_eq!(empty_windows_before_probe(Duration::from_secs(300)), 1);
    }

    /// Partial windows round up, so the probe never waits a full extra
    /// window beyond the interval.
    #[test]
    fn probe_threshold_rounds_up() {
        assert_eq!(empty_windows_before_probe(Duration::from_secs(7)), 5);
    }
}

#[cfg(test)]
mod reconnect_tests {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    use super::*;

    /// A closure that keeps a connection "up" for at least
    /// `RECONNECT_RESET_AFTER` before failing must have its reconnect budget
    /// reset each time, so `max_reconnect_attempts` never trips even though
    /// the closure fails more times than the configured max.
    #[tokio::test(start_paused = true)]
    async fn resets_budget_after_healthy_run() {
        let shutdown = CancellationToken::new();
        let calls = AtomicU32::new(0);
        let result = run_with_reconnect(&shutdown, "test", Some(2), || {
            let n = calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            async move {
                if n <= 5 {
                    tokio::time::advance(RECONNECT_RESET_AFTER + Duration::from_secs(1)).await;
                    Err(ShoveError::Connection("boom".to_string()))
                } else {
                    Ok(())
                }
            }
        })
        .await;

        assert!(result.is_ok(), "expected success, got {result:?}");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 6);
    }

    /// Without an intervening healthy period, consecutive fast failures must
    /// still exhaust the configured reconnect budget.
    #[tokio::test(start_paused = true)]
    async fn exhausts_budget_on_consecutive_fast_failures() {
        let shutdown = CancellationToken::new();
        let calls = AtomicU32::new(0);
        let result = run_with_reconnect(&shutdown, "test", Some(2), || {
            calls.fetch_add(1, AtomicOrdering::SeqCst);
            async move { Err(ShoveError::Connection("boom".to_string())) }
        })
        .await;

        assert!(result.is_err(), "expected exhaustion error, got {result:?}");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }
}
