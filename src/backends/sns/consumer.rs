use aws_sdk_sqs::config::http::HttpResponse;
use aws_sdk_sqs::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::backend::BatchConsumerOptionsInner;
use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::batch_consumer::{
    BatchSettlement, batch_redelivery_backoff, invoke_batch_handler, next_redelivery_delay,
    settle_batch_outcome,
};
use crate::backends::sns::client::SnsClient;
use crate::backends::sns::router;
use crate::backends::sns::topology::QueueRegistry;
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{drain_timeout_outcome, handler_timeout_outcome, shutdown_drain_timeout};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::{QueueTopology, SequenceFailure};
use crate::{DEFAULT_MAX_MESSAGE_SIZE, Sqs};

/// Maps an SQS `SdkError` to the appropriate `ShoveError` variant.
///
/// Transport-level errors (timeout, dispatch failure, response parse) are
/// transient → `Connection`.  Service-level errors are classified by the
/// specific SQS error code: throttling and over-limit are transient, while
/// queue-not-found, auth, and config errors are permanent → `Topology`.
fn map_sqs_error<E>(context: &str, e: SdkError<E, HttpResponse>) -> ShoveError
where
    E: std::fmt::Debug + std::fmt::Display + ProvideErrorMetadata,
{
    match &e {
        // Transient transport-level errors
        SdkError::TimeoutError(_) => ShoveError::Connection(format!("{context}: {e}")),
        SdkError::DispatchFailure(_) => ShoveError::Connection(format!("{context}: {e}")),
        SdkError::ResponseError(_) => ShoveError::Connection(format!("{context}: {e}")),
        // Construction failures are config/code bugs — permanent
        SdkError::ConstructionFailure(_) => ShoveError::Topology(format!("{context}: {e}")),
        // Service errors — classify by AWS error code
        SdkError::ServiceError(se) => {
            let code = ProvideErrorMetadata::code(se.err());
            let is_transient = matches!(
                code,
                Some("RequestThrottled" | "Throttling" | "KMS.ThrottlingException" | "OverLimit")
            );
            if is_transient {
                ShoveError::Connection(format!("{context}: {e}"))
            } else {
                ShoveError::Topology(format!("{context}: {e}"))
            }
        }
        // Required: SdkError is #[non_exhaustive]. All current variants are
        // handled above; this only fires if the AWS SDK adds new ones.
        _ => ShoveError::Unknown(format!("unrecognized AWS SDK error in {context}: {e}")),
    }
}

#[derive(Clone)]
pub struct SqsConsumer {
    client: SnsClient,
    queue_registry: Arc<QueueRegistry>,
}

impl SqsConsumer {
    pub fn new(client: SnsClient, queue_registry: Arc<QueueRegistry>) -> Self {
        Self {
            client,
            queue_registry,
        }
    }

    async fn resolve_queue_url(&self, queue_name: &str) -> Result<String> {
        self.queue_registry.get(queue_name).await.ok_or_else(|| {
            ShoveError::Topology(format!(
                "no SQS queue URL registered for '{queue_name}'. Declare topology first."
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_metadata(msg: &Message) -> MessageMetadata {
    let retry_count = router::get_retry_count(msg);
    MessageMetadata {
        retry_count,
        delivery_id: msg.message_id().unwrap_or_default().to_string(),
        redelivered: retry_count > 0,
        delivery_count: approximate_receive_count(msg),
        headers: Arc::new(router::extract_message_attributes(msg)),
    }
}

/// Reads SQS's `ApproximateReceiveCount`, which counts receives including this
/// one (so a first delivery reports 1). Every `receive_message` call in this
/// backend requests the attribute; it is `None` only when SQS omitted it.
fn approximate_receive_count(msg: &Message) -> Option<u32> {
    msg.attributes()
        .and_then(|attrs| attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|v| v.parse::<u32>().ok())
}

/// SNS notification envelope that wraps message payloads when `RawMessageDelivery`
/// is not enabled (or is ignored by the broker emulator).
///
/// Real AWS with `RawMessageDelivery=true`: body is the raw payload → this struct
/// is never used. Emulators that ignore the attribute always wrap
/// messages; we unwrap them here so the consumer can deserialize normally.
#[derive(serde::Deserialize)]
struct SnsEnvelope {
    #[serde(rename = "Type")]
    notification_type: String,
    /// The actual message payload, serialized as a JSON string inside the envelope.
    #[serde(rename = "Message")]
    message: String,
}

/// Return the raw payload string from an SQS message body.
///
/// Tries to parse the body as an [`SnsEnvelope`]. If the `Type` field is
/// `"Notification"` the inner `Message` string is returned (owned); otherwise
/// the original body is returned as a borrow (no allocation).
fn extract_payload(body: &str) -> std::borrow::Cow<'_, str> {
    if let Ok(envelope) = serde_json::from_str::<SnsEnvelope>(body)
        && envelope.notification_type == "Notification"
    {
        return std::borrow::Cow::Owned(envelope.message);
    }
    std::borrow::Cow::Borrowed(body)
}

fn extract_dead_metadata(msg: &Message, queue_name: &str) -> DeadMessageMetadata {
    let metadata = extract_metadata(msg);
    let death_count = metadata.retry_count;
    DeadMessageMetadata {
        message: metadata,
        reason: Some("max_receives_exceeded".into()),
        original_queue: Some(queue_name.to_string()),
        death_count,
    }
}

/// Run `f` in a reconnect loop, retrying on transient errors until shutdown.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    queue: &str,
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
                        queue,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{queue}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                warn!(
                    queue,
                    attempt = attempts,
                    ?max_reconnect_attempts,
                    "consumer error, reconnecting in {delay:?}: {e}"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

/// Run the handler future with optional timeout, emitting inflight/consumed/duration metrics.
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
    F: std::future::Future<Output = Outcome> + Send + 'static,
{
    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let mut join = tokio::spawn(fut);
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, &mut join).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
            Err(_) => {
                join.abort();
                let resolved = handler_timeout_outcome(timeout_outcome);
                warn!(outcome = ?resolved, "handler exceeded timeout ({duration:?})");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                resolved
            }
        },
        None => match join.await {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
        },
    };
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, group, &outcome);
    metrics::record_processing_duration(topic, group, &outcome, elapsed);
    outcome
}

/// Spawns a handler task for a deserialized message.
/// Returns the oneshot receiver that will resolve with the handler's outcome.
#[allow(clippy::too_many_arguments)]
fn spawn_handler<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    notify: &Arc<Notify>,
    topic: Arc<str>,
    group: Option<Arc<str>>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let c = ctx.clone();
    let n = notify.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(
            async move { h.handle(message, metadata, c.as_ref()).await },
            timeout,
            timeout_outcome,
            &topic,
            group.as_deref(),
        )
        .await;
        let _ = tx.send(outcome);
        n.notify_one();
    });
    rx
}

// ---------------------------------------------------------------------------
// Concurrent consumption loop
// ---------------------------------------------------------------------------

struct PendingMessage {
    receipt_handle: String,
    /// The original SQS message, kept for lazy access on retry/defer paths.
    /// Arc avoids cloning the body and attributes for the common Ack outcome.
    msg: Arc<Message>,
    retry_count: u32,
    outcome_rx: oneshot::Receiver<Outcome>,
}

async fn consume_loop_concurrent<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    topology: &'static QueueTopology,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let notify = Arc::new(Notify::new());
    let topic: Arc<str> = Arc::from(topology.queue());
    let group: Option<Arc<str>> = options.consumer_group.as_deref().map(Arc::from);

    // Max number of handlers running concurrently (1 = serial / non-concurrent mode).
    let max_in_flight = options.prefetch_count as usize;

    // How many messages to request per SQS poll. Defaults to SQS's hard cap
    // (10) to amortise `ReceiveMessage` round-trips across as many messages
    // as possible — critical in serial / low-prefetch mode where `max_in_flight`
    // would otherwise pin the batch to 1 and bottleneck throughput on poll
    // RTT. Users can override via `ConsumerOptions::with_receive_batch_size`.
    let receive_batch: usize = {
        let configured = if options.receive_batch_size > 0 {
            options.receive_batch_size as usize
        } else {
            10
        };
        configured.min(10)
    };

    let mut in_flight: VecDeque<PendingMessage> = VecDeque::with_capacity(max_in_flight);
    // Received from SQS but not yet dispatched to a handler.  Populated when
    // the receive batch is larger than `max_in_flight`.
    let mut local_buffer: VecDeque<Message> = VecDeque::with_capacity(receive_batch);

    // Ack receipt handles accumulated across loop iterations.
    //
    // Rather than flushing a DeleteMessageBatch after every drained handler
    // (which would produce 1-item batches in serial / non-concurrent mode),
    // we collect receipt handles here and only send the batch when:
    //   (a) we have a full batch of 10, or
    //   (b) we are about to call ReceiveMessage, or
    //   (c) we are shutting down.
    //
    // In concurrent mode (max_in_flight > 1) the drain loop typically fills
    // the batch in a single iteration, so behaviour is unchanged.  In serial
    // mode (max_in_flight = 1) this collapses N individual DeleteMessageBatch
    // calls into a single call per receive batch, reducing API pressure by ~5×.
    let mut pending_acks: Vec<String> = Vec::with_capacity(10);

    info!(
        queue_url,
        max_in_flight, receive_batch, "SQS consumer started"
    );

    loop {
        // ── Drain completed messages from the front, preserving order ──
        //
        // Accumulate consecutive Ack receipts in `pending_acks`.  They will be
        // flushed as a single DeleteMessageBatch call before the next
        // ReceiveMessage poll.  Non-Ack outcomes (Retry/Reject/Defer) are
        // routed individually because they need separate SQS operations.
        while let Some(front) = in_flight.front_mut() {
            match front.outcome_rx.try_recv() {
                Ok(Outcome::Ack) => {
                    let msg = in_flight.pop_front().expect("in_flight was just peeked");
                    debug!(queue_url, receipt_handle = %msg.receipt_handle, "message acked (pending flush)");
                    pending_acks.push(msg.receipt_handle);
                    // Flush immediately once we have a full batch.
                    if pending_acks.len() >= 10 {
                        let batch_size = pending_acks.len();
                        debug!(queue_url, batch_size, "flushing full ack batch");
                        router::route_ack_batch(sqs, queue_url, std::mem::take(&mut pending_acks))
                            .await;
                    }
                }
                Ok(outcome) => {
                    // Flush accumulated acks before routing a non-ack outcome.
                    if !pending_acks.is_empty() {
                        let batch_size = pending_acks.len();
                        debug!(
                            queue_url,
                            batch_size,
                            ?outcome,
                            "flushing ack batch before non-ack outcome"
                        );
                        router::route_ack_batch(sqs, queue_url, std::mem::take(&mut pending_acks))
                            .await;
                    }
                    let msg = in_flight.pop_front().expect("in_flight was just peeked");
                    debug!(queue_url, ?outcome, "message handled");
                    route_outcome(
                        sqs,
                        queue_url,
                        &msg.receipt_handle,
                        &msg.msg,
                        outcome,
                        topology,
                        msg.retry_count,
                        group.as_deref(),
                    )
                    .await;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    // Handler task panicked — treat as retry.
                    if !pending_acks.is_empty() {
                        let batch_size = pending_acks.len();
                        debug!(
                            queue_url,
                            batch_size, "flushing ack batch after handler panic"
                        );
                        router::route_ack_batch(sqs, queue_url, std::mem::take(&mut pending_acks))
                            .await;
                    }
                    let msg = in_flight.pop_front().expect("in_flight was just peeked");
                    warn!(queue_url, "handler task panicked, retrying message");
                    route_outcome(
                        sqs,
                        queue_url,
                        &msg.receipt_handle,
                        &msg.msg,
                        Outcome::Retry,
                        topology,
                        msg.retry_count,
                        group.as_deref(),
                    )
                    .await;
                }
            }
        }
        // Do NOT flush pending_acks here — let them accumulate across
        // iterations until the pre-poll flush or a full-batch flush above.

        options.processing.store(
            !in_flight.is_empty() || !local_buffer.is_empty(),
            Ordering::Release,
        );

        // ── Shutdown: requeue buffered messages, drain in-flight handlers ──
        if options.shutdown.is_cancelled() {
            debug!(
                "shutdown signal, requeueing {} buffered, draining {} in-flight on {queue_url}",
                local_buffer.len(),
                in_flight.len()
            );
            // Flush any deferred acks before requeuing.
            if !pending_acks.is_empty() {
                let batch_size = pending_acks.len();
                debug!(queue_url, batch_size, "flushing ack batch on shutdown");
                router::route_ack_batch(sqs, queue_url, std::mem::take(&mut pending_acks)).await;
            }
            // Messages in the local buffer were received (invisible in SQS) but
            // never dispatched.  Make them visible again so they can be redelivered.
            for msg in local_buffer.drain(..) {
                if let Some(rh) = msg.receipt_handle() {
                    router::route_requeue(sqs, queue_url, rh).await;
                }
            }
            // When a `handler_timeout` is set the handler is already bounded by
            // it and resolves its own timeout; this wait is only a backstop so
            // shutdown cannot hang on a channel that never delivers. Hence the
            // grace, and hence resolving to the *configured* timeout outcome
            // rather than assuming Retry. With deadlines disabled the handler is
            // still running when this fires, so `drain_timeout_outcome` keeps
            // the backstop at Retry — see its docs.
            let drain_timeout = shutdown_drain_timeout(options.handler_timeout);
            // Collect Acks into a batch; non-Ack outcomes still need
            // per-message routing (Retry/Reject/Defer touch distinct queues).
            let mut drain_acks: Vec<String> = Vec::with_capacity(in_flight.len());
            for pending in in_flight {
                let outcome = tokio::time::timeout(drain_timeout, pending.outcome_rx)
                    .await
                    .unwrap_or_else(|_| {
                        let resolved = drain_timeout_outcome(
                            options.handler_timeout,
                            options.handler_timeout_outcome.clone(),
                        );
                        warn!(
                            queue_url,
                            outcome = ?resolved,
                            "handler outcome did not arrive within the shutdown drain"
                        );
                        Ok(resolved)
                    })
                    // A closed channel means the handler task panicked or was
                    // aborted: no outcome exists, so redeliver.
                    .unwrap_or(Outcome::Retry);
                if matches!(outcome, Outcome::Ack) {
                    drain_acks.push(pending.receipt_handle);
                } else {
                    route_outcome(
                        sqs,
                        queue_url,
                        &pending.receipt_handle,
                        &pending.msg,
                        outcome,
                        topology,
                        pending.retry_count,
                        group.as_deref(),
                    )
                    .await;
                }
            }
            if !drain_acks.is_empty() {
                let batch_size = drain_acks.len();
                debug!(
                    queue_url,
                    batch_size, "flushing ack batch on drain completion"
                );
                router::route_ack_batch(sqs, queue_url, drain_acks).await;
            }
            return Ok(());
        }

        // ── Dispatch buffered messages to handlers ──
        //
        // Move messages from the local buffer into in-flight handler slots.
        // In concurrent mode (max_in_flight > 1) this fills all available slots.
        // In serial mode (max_in_flight = 1) this dispatches one message at a time.
        while in_flight.len() < max_in_flight {
            let Some(msg) = local_buffer.pop_front() else {
                break;
            };

            let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
            let retry_count = router::get_retry_count(&msg);

            if retry_count >= options.max_retries {
                warn!(
                    queue_url,
                    retry_count,
                    max_retries = options.max_retries,
                    "message exceeded max retries, rejecting"
                );
                router::route_reject(
                    sqs,
                    queue_url,
                    &receipt_handle,
                    topology,
                    group.as_deref(),
                    metrics::FailReason::MaxRetriesExceeded,
                )
                .await;
                continue;
            }

            let body = extract_payload(msg.body().unwrap_or_default());

            metrics::record_message_size(&topic, group.as_deref(), body.len());

            // Reject oversized messages before deserialization
            if let Err(e) = options.validate_payload_message_size(body.len()) {
                warn!(error = %e, queue_url, "rejecting oversized message");
                router::route_reject(
                    sqs,
                    queue_url,
                    &receipt_handle,
                    topology,
                    group.as_deref(),
                    metrics::FailReason::Oversize,
                )
                .await;
                continue;
            }

            let message: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(
                body.as_bytes(),
            ) {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, queue_url, "failed to deserialize SQS message, rejecting");
                    router::route_reject(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        group.as_deref(),
                        metrics::FailReason::Deserialize,
                    )
                    .await;
                    continue;
                }
            };

            let metadata = extract_metadata(&msg);
            debug!(
                queue_url,
                message_id = %metadata.delivery_id,
                retry_count = metadata.retry_count,
                "dispatching message to handler"
            );
            let rx = spawn_handler::<T, H>(
                handler,
                ctx,
                message,
                metadata,
                options.handler_timeout,
                options.handler_timeout_outcome.clone(),
                &notify,
                Arc::clone(&topic),
                group.clone(),
            );
            in_flight.push_back(PendingMessage {
                receipt_handle,
                msg: Arc::new(msg),
                retry_count,
                outcome_rx: rx,
            });
            options.processing.store(true, Ordering::Relaxed);
        }

        // ── Poll SQS when the buffer needs refilling ──
        //
        // Fetch a full batch whenever the buffer is empty AND we have handler
        // slots available.  This keeps the pipeline full while preventing us
        // from pulling messages we cannot process for a long time (which would
        // exhaust SQS visibility timeouts).
        if local_buffer.is_empty() && in_flight.len() < max_in_flight {
            // Flush deferred acks before blocking on ReceiveMessage.
            // In serial mode this is the point where all N messages from the
            // previous receive batch have been processed — send their receipt
            // handles in a single DeleteMessageBatch instead of N individual calls.
            if !pending_acks.is_empty() {
                let batch_size = pending_acks.len();
                debug!(queue_url, batch_size, "flushing ack batch before poll");
                router::route_ack_batch(sqs, queue_url, std::mem::take(&mut pending_acks)).await;
            }

            let max_messages = receive_batch as i32;

            // Always use wait_time_seconds=0 (short poll, returns immediately).
            //
            // Server-side long polling (wait_time_seconds > 0) holds an open
            // HTTP connection inside the broker for the full wait duration.  On
            // LocalStack — which handles ReceiveMessage requests serially — N
            // consumers all sleeping in a long poll stacks up to N × wait_time
            // of blocking (e.g. 4 consumers × 5 s = 20 s stall).
            //
            // Instead we return immediately and do client-side backoff: if the
            // queue is empty we yield for 500 ms via tokio::time::sleep, which
            // is async and does not hold any broker connection.  All N consumers
            // sleep concurrently so the total stall is ≈ 500 ms regardless of N.
            let receive_result = sqs
                .receive_message()
                .queue_url(queue_url)
                .wait_time_seconds(0)
                .max_number_of_messages(max_messages)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names("All")
                .send()
                .await
                .map_err(|e| {
                    metrics::record_backend_error(
                        metrics::BackendLabel::SnsSqs,
                        metrics::BackendErrorKind::Consume,
                    );
                    map_sqs_error(&format!("SQS ReceiveMessage failed on {queue_url}"), e)
                })?;

            let msgs = receive_result.messages.unwrap_or_default();
            if msgs.is_empty() {
                debug!(queue_url, "queue empty, backing off 500ms");
                // Queue appears empty — sleep briefly before re-polling so we
                // don't spin and so multiple consumers naturally stagger.
                tokio::select! {
                    biased;
                    _ = options.shutdown.cancelled() => {}
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            } else {
                debug!(
                    queue_url,
                    received = msgs.len(),
                    "received messages from SQS"
                );
            }
            local_buffer.extend(msgs);

            // Loop immediately to dispatch newly buffered messages.
            continue;
        }

        // ── Wait for progress ──
        if in_flight.len() >= max_in_flight {
            // All handler slots occupied — wait for the next completion.
            notify.notified().await;
        }
        // If in_flight is empty and local_buffer is also empty, the outer loop
        // will fall through to the poll branch on the next iteration.
    }
}

/// Route a completed message based on its outcome.
///
/// `body` and `message_attributes` are accessed lazily: only the Retry and
/// Defer arms need them, so the common Ack path pays no extraction cost.
#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    msg: &Message,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    // Consumer-group label propagated to `metrics::record_failed` on
    // `Outcome::Reject` — matches Kafka/NATS/Redis/RabbitMQ `route_outcome`.
    group: Option<&str>,
) {
    match outcome {
        Outcome::Ack => router::route_ack(sqs, queue_url, receipt_handle).await,
        Outcome::Retry => {
            let body = msg.body().unwrap_or_default();
            let empty_attrs = HashMap::new();
            let attrs = msg.message_attributes().unwrap_or(&empty_attrs);
            router::route_retry(
                sqs,
                queue_url,
                receipt_handle,
                body,
                attrs,
                topology,
                retry_count,
            )
            .await;
        }
        Outcome::Reject => {
            router::route_reject(
                sqs,
                queue_url,
                receipt_handle,
                topology,
                group,
                metrics::FailReason::Rejected,
            )
            .await
        }
        Outcome::Defer => {
            let body = msg.body().unwrap_or_default();
            let empty_attrs = HashMap::new();
            let attrs = msg.message_attributes().unwrap_or(&empty_attrs);
            router::route_defer(
                sqs,
                queue_url,
                receipt_handle,
                body,
                attrs,
                topology,
                retry_count,
            )
            .await;
        }
    }
}

// ---------------------------------------------------------------------------
// KeyState — per-key state machine for sequenced consumers
// ---------------------------------------------------------------------------

/// Tracks the processing state of a single sequence key within a
/// sequenced shard consumer.
enum KeyState {
    /// A handler is currently running for this key.
    InFlight {
        receipt_handle: String,
        /// Original SQS message kept for lazy body/attribute access on retry/defer.
        msg: Arc<Message>,
        retry_count: u32,
        outcome_rx: oneshot::Receiver<Outcome>,
    },
    /// The handler returned Retry/Defer and the message visibility has been
    /// changed. The key is blocked until the retry comes back.
    AwaitingRetry,
}

/// Extract the sequence key from SQS MessageGroupId system attribute.
fn extract_sequence_key(msg: &Message) -> Option<String> {
    msg.attributes()
        .and_then(|attrs| attrs.get(&MessageSystemAttributeName::MessageGroupId))
        .map(|s| s.to_string())
}

/// Spawns a handler task for a sequenced message, signalling completion via an
/// mpsc channel with the sequence key.
#[allow(clippy::too_many_arguments)]
fn spawn_handler_keyed<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key: String,
    topic: Arc<str>,
    group: Option<Arc<str>>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let c = ctx.clone();
    let completed = completed_tx.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(
            async move { h.handle(message, metadata, c.as_ref()).await },
            timeout,
            timeout_outcome,
            &topic,
            group.as_deref(),
        )
        .await;
        let _ = tx.send(outcome);
        let _ = completed.send(key);
    });
    rx
}

// ---------------------------------------------------------------------------
// Sequenced consumption loop
// ---------------------------------------------------------------------------

/// Runs a single shard consumer with reconnect handling, owning the per-shard
/// mutable state (poisoned_keys, pending_deliveries) across reconnects.
#[allow(clippy::too_many_arguments)]
async fn run_sequenced_shard<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    queue_name: &str,
    topology: &'static QueueTopology,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let mut poisoned_keys = HashSet::new();
    let mut pending_deliveries: HashMap<String, VecDeque<Message>> = HashMap::new();
    let mut backoff = Backoff::default();
    let mut attempts = 0u32;

    loop {
        match consume_loop_sequenced::<T, H>(
            sqs,
            queue_url,
            topology,
            handler,
            ctx,
            options,
            on_failure,
            &mut poisoned_keys,
            &mut pending_deliveries,
        )
        .await
        {
            Ok(()) => {
                // Graceful shutdown — release buffered-but-unprocessed messages
                // back to the queue so another consumer can pick them up.
                for (_key, msgs) in pending_deliveries.drain() {
                    for msg in msgs {
                        let rh = msg.receipt_handle().unwrap_or_default();
                        router::route_requeue(sqs, queue_url, rh).await;
                    }
                }
                return Ok(());
            }
            Err(e) => {
                if options.shutdown.is_cancelled() {
                    pending_deliveries.clear();
                    return Ok(());
                }
                // On reconnect, clear pending — visibility will expire and SQS
                // will redeliver them.
                pending_deliveries.clear();
                attempts += 1;
                if let Some(max) = options.max_reconnect_attempts
                    && attempts >= max
                {
                    tracing::error!(
                        queue = queue_name,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{queue_name}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                warn!(
                    queue = queue_name,
                    attempt = attempts,
                    max_reconnect_attempts = ?options.max_reconnect_attempts,
                    "consumer error, reconnecting in {delay:?}: {e}"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = options.shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn consume_loop_sequenced<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    topology: &'static QueueTopology,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
    poisoned_keys: &mut HashSet<String>,
    pending_deliveries: &mut HashMap<String, VecDeque<Message>>,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let prefetch = options.prefetch_count as usize;
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<String>();
    let topic: Arc<str> = Arc::from(topology.queue());
    let group: Option<Arc<str>> = options.consumer_group.as_deref().map(Arc::from);

    let mut key_states: HashMap<String, KeyState> = HashMap::new();
    let mut in_flight_count: usize = 0;

    info!(queue_url, prefetch, "sequenced SQS consumer started");

    loop {
        // ── Drain completed handlers ──
        while let Ok(key) = completed_rx.try_recv() {
            let Some(state) = key_states.remove(&key) else {
                continue;
            };
            let KeyState::InFlight {
                receipt_handle,
                msg,
                retry_count,
                mut outcome_rx,
            } = state
            else {
                // AwaitingRetry — shouldn't happen, but put it back.
                key_states.insert(key, state);
                continue;
            };

            let outcome = match outcome_rx.try_recv() {
                Ok(o) => o,
                Err(TryRecvError::Closed) => {
                    warn!(queue_url, sequence_key = %key, "handler task panicked, retrying");
                    Outcome::Retry
                }
                Err(TryRecvError::Empty) => {
                    key_states.insert(
                        key,
                        KeyState::InFlight {
                            receipt_handle,
                            msg,
                            retry_count,
                            outcome_rx,
                        },
                    );
                    continue;
                }
            };
            debug!(queue_url, sequence_key = %key, ?outcome, "message handled (sequenced)");

            match outcome {
                Outcome::Ack => {
                    router::route_ack(sqs, queue_url, &receipt_handle).await;
                    in_flight_count -= 1;
                    drain_pending_for_key::<T, H>(
                        sqs,
                        queue_url,
                        &key,
                        handler,
                        ctx,
                        options,
                        on_failure,
                        topology,
                        poisoned_keys,
                        &completed_tx,
                        &mut key_states,
                        &mut in_flight_count,
                        pending_deliveries,
                        &topic,
                        &group,
                    )
                    .await;
                }
                Outcome::Reject => {
                    if on_failure == SequenceFailure::FailAll {
                        info!(
                            sequence_key = %key,
                            queue_url,
                            "poisoning sequence key (FailAll)"
                        );
                        poisoned_keys.insert(key.clone());
                    }
                    router::route_reject(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        group.as_deref(),
                        metrics::FailReason::Rejected,
                    )
                    .await;
                    in_flight_count -= 1;
                    drain_pending_for_key::<T, H>(
                        sqs,
                        queue_url,
                        &key,
                        handler,
                        ctx,
                        options,
                        on_failure,
                        topology,
                        poisoned_keys,
                        &completed_tx,
                        &mut key_states,
                        &mut in_flight_count,
                        pending_deliveries,
                        &topic,
                        &group,
                    )
                    .await;
                }
                Outcome::Retry => {
                    router::route_retry_fifo(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        retry_count,
                    )
                    .await;
                    in_flight_count -= 1;
                    key_states.insert(key, KeyState::AwaitingRetry);
                }
                Outcome::Defer => {
                    warn!(
                        queue_url,
                        sequence_key = %key,
                        "Defer is not supported on sequenced (FIFO) consumers, treating as Retry"
                    );
                    router::route_retry_fifo(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        retry_count,
                    )
                    .await;
                    in_flight_count -= 1;
                    key_states.insert(key, KeyState::AwaitingRetry);
                }
            }
        }

        options
            .processing
            .store(in_flight_count > 0, Ordering::Relaxed);

        let can_accept = in_flight_count < prefetch;

        tokio::select! {
            biased;

            _ = options.shutdown.cancelled() => {
                debug!(
                    "shutdown signal, draining {} in-flight messages on {queue_url}",
                    in_flight_count
                );
                // Wait for all in-flight handlers to complete. Same backstop as
                // the standard consumer's drain: the handler is already bounded
                // by `handler_timeout` and resolves its own timeout, so this
                // only stops shutdown hanging on a channel that never delivers.
                // With deadlines disabled the handler is still running when it
                // fires, so `drain_timeout_outcome` keeps it at Retry.
                let drain_timeout = shutdown_drain_timeout(options.handler_timeout);
                for (key, state) in key_states.drain() {
                    if let KeyState::InFlight { receipt_handle, msg: _, retry_count, outcome_rx } = state {
                        let outcome = tokio::time::timeout(drain_timeout, outcome_rx)
                            .await
                            .unwrap_or_else(|_| {
                                let resolved = drain_timeout_outcome(
                                    options.handler_timeout,
                                    options.handler_timeout_outcome.clone(),
                                );
                                warn!(queue_url, sequence_key = %key, outcome = ?resolved, "handler outcome did not arrive within the shutdown drain");
                                Ok(resolved)
                            })
                            // A closed channel means the handler task panicked
                            // or was aborted: no outcome exists, so redeliver.
                            .unwrap_or(Outcome::Retry);
                        debug!(
                            queue_url,
                            sequence_key = %key,
                            ?outcome,
                            "draining in-flight message on shutdown"
                        );
                        match outcome {
                            Outcome::Ack => {
                                router::route_ack(sqs, queue_url, &receipt_handle).await;
                            }
                            Outcome::Reject => {
                                router::route_reject(
                                    sqs,
                                    queue_url,
                                    &receipt_handle,
                                    topology,
                                    group.as_deref(),
                                    metrics::FailReason::Rejected,
                                )
                                .await;
                            }
                            Outcome::Retry => {
                                router::route_retry_fifo(
                                    sqs,
                                    queue_url,
                                    &receipt_handle,
                                    topology,
                                    retry_count,
                                )
                                .await;
                            }
                            Outcome::Defer => {
                                warn!(
                                    queue_url,
                                    sequence_key = %key,
                                    "Defer is not supported on sequenced (FIFO) consumers, treating as Retry"
                                );
                                router::route_retry_fifo(
                                    sqs,
                                    queue_url,
                                    &receipt_handle,
                                    topology,
                                    retry_count,
                                )
                                .await;
                            }
                        }
                    }
                }
                // Pending deliveries: change visibility to 0 so they are redelivered.
                // These were buffered but never dispatched, so this is a requeue,
                // not a rejection — matching the standard and concurrent loops.
                for (_key, msgs) in pending_deliveries.drain() {
                    for msg in msgs {
                        let rh = msg.receipt_handle().unwrap_or_default();
                        router::route_requeue(sqs, queue_url, rh).await;
                    }
                }
                return Ok(());
            }

            Some(key) = completed_rx.recv() => {
                // Re-inject the key so the drain loop at the top picks it up.
                let _ = completed_tx.send(key);
            }

            result = async {
                sqs.receive_message()
                    .queue_url(queue_url)
                    .wait_time_seconds(5)
                    .max_number_of_messages(prefetch.saturating_sub(in_flight_count).min(10) as i32)
                    .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                    .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
                    .message_attribute_names("All")
                    .send()
                    .await
            }, if can_accept => {
                let messages = result
                    .map_err(|e| {
                        metrics::record_backend_error(
                            metrics::BackendLabel::SnsSqs,
                            metrics::BackendErrorKind::Consume,
                        );
                        map_sqs_error(
                            &format!("SQS ReceiveMessage failed on {queue_url}"),
                            e,
                        )
                    })?
                    .messages
                    .unwrap_or_default();

                debug!(queue_url, received = messages.len(), "received messages from SQS (sequenced)");

                for msg in messages {
                    let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
                    let retry_count = router::get_retry_count(&msg);
                    // Unreachable on the supported topology, and deliberately kept.
                    // `declare` creates every sequenced shard queue as FIFO
                    // (`topology.rs`, `create_sqs_queue(.., fifo = true, ..)`), and SQS
                    // itself rejects a `SendMessage` to a FIFO queue that carries no
                    // `MessageGroupId` — so a message in this shape cannot reach the
                    // queue. The fan-in topic is FIFO too, and `run_fifo` is bound
                    // `T: SequencedTopic` with shard URLs resolved from the registry
                    // `declare` populates, so there is no way to aim it at a non-FIFO
                    // queue. It is not a "we forgot to request the attribute" bug
                    // either: the receive call above asks for `MessageGroupId`
                    // explicitly.
                    //
                    // This arm would start firing if sequenced topics ever ran over a
                    // non-FIFO transport (standard queues plus an application-level
                    // ordering key), which is why the guard is worth its ten lines.
                    // Until then `messages_failed_total{reason="malformed"}` is
                    // Redis-only — see `docs/pages/guides/observability.mdx`. Do not
                    // write a test that "covers" it by synthesising a `Message`
                    // in-crate; that asserts the harness, not the backend.
                    let seq_key = match extract_sequence_key(&msg) {
                        Some(k) => k,
                        None => {
                            warn!(
                                queue_url,
                                "message missing MessageGroupId, rejecting"
                            );
                            router::route_reject(
                                sqs,
                                queue_url,
                                &receipt_handle,
                                topology,
                                group.as_deref(),
                                metrics::FailReason::Malformed,
                            )
                            .await;
                            continue;
                        }
                    };

                    // ── FailAll: skip poisoned keys ──
                    if on_failure == SequenceFailure::FailAll
                        && poisoned_keys.contains(&seq_key)
                    {
                        warn!(
                            sequence_key = %seq_key,
                            queue_url,
                            "message with poisoned sequence key, rejecting"
                        );
                        // Cascade: intentionally not counted — see `metrics::FailReason`.
                        router::route_reject_cascade(sqs, queue_url, &receipt_handle, topology)
                            .await;
                        continue;
                    }

                    // ── Max retries check ──
                    if retry_count >= options.max_retries {
                        warn!(
                            queue_url,
                            retry_count,
                            max_retries = options.max_retries,
                            "message exceeded max retries, rejecting"
                        );
                        if on_failure == SequenceFailure::FailAll {
                            info!(
                                sequence_key = %seq_key,
                                queue_url,
                                "poisoning sequence key (FailAll)"
                            );
                            poisoned_keys.insert(seq_key.clone());
                            // Reject all pending deliveries for this key.
                            // Cascade: intentionally not counted — see `metrics::FailReason`.
                            if let Some(pending) = pending_deliveries.remove(&seq_key) {
                                for pd in pending {
                                    let rh = pd.receipt_handle().unwrap_or_default();
                                    router::route_reject_cascade(sqs, queue_url, rh, topology)
                                        .await;
                                }
                            }
                        }
                        router::route_reject(
                            sqs,
                            queue_url,
                            &receipt_handle,
                            topology,
                            group.as_deref(),
                            metrics::FailReason::MaxRetriesExceeded,
                        )
                        .await;
                        continue;
                    }

                    // ── Check if key is busy ──
                    match key_states.get(&seq_key) {
                        Some(KeyState::InFlight { .. }) => {
                            // Handler running — buffer locally if within limit.
                            if let Some(limit) = options.max_pending_per_key {
                                let current_len = pending_deliveries
                                    .get(&seq_key)
                                    .map_or(0, |q| q.len());
                                if current_len >= limit {
                                    warn!(
                                        sequence_key = %seq_key,
                                        queue_url,
                                        limit,
                                        "per-key pending buffer full, rejecting"
                                    );
                                    router::route_reject(
                                        sqs,
                                        queue_url,
                                        &receipt_handle,
                                        topology,
                                        group.as_deref(),
                                        metrics::FailReason::PendingFull,
                                    )
                                    .await;
                                    continue;
                                }
                            }
                            debug!(
                                sequence_key = %seq_key,
                                queue_url,
                                "key in-flight, buffering delivery locally"
                            );
                            pending_deliveries
                                .entry(seq_key)
                                .or_insert_with(|| VecDeque::with_capacity(4))
                                .push_back(msg);
                            continue;
                        }
                        Some(KeyState::AwaitingRetry) => {
                            if retry_count > 0 {
                                // This is the returning retry — clear AwaitingRetry
                                // and fall through to spawn a handler below.
                                debug!(
                                    sequence_key = %seq_key,
                                    queue_url,
                                    retry_count,
                                    "returning retry clears AwaitingRetry"
                                );
                                key_states.remove(&seq_key);
                            } else {
                                // New message while awaiting retry — buffer if within limit.
                                if let Some(limit) = options.max_pending_per_key {
                                    let current_len = pending_deliveries
                                        .get(&seq_key)
                                        .map_or(0, |q| q.len());
                                    if current_len >= limit {
                                        warn!(
                                            sequence_key = %seq_key,
                                            queue_url,
                                            limit,
                                            "per-key pending buffer full, rejecting"
                                        );
                                        router::route_reject(
                                            sqs,
                                            queue_url,
                                            &receipt_handle,
                                            topology,
                                            group.as_deref(),
                                            metrics::FailReason::PendingFull,
                                        )
                                        .await;
                                        continue;
                                    }
                                }
                                debug!(
                                    sequence_key = %seq_key,
                                    queue_url,
                                    "key awaiting retry, buffering new delivery locally"
                                );
                                pending_deliveries
                                    .entry(seq_key)
                                    .or_default()
                                    .push_back(msg);
                                continue;
                            }
                        }
                        None => {}
                    }

                    // ── Spawn handler for this key ──
                    let body = extract_payload(msg.body().unwrap_or_default());

                    metrics::record_message_size(&topic, group.as_deref(), body.len());

                    // Reject oversized messages before deserialization
                    if let Err(e) = options.validate_payload_message_size(body.len()) {
                        warn!(
                            error = %e,
                            queue_url,
                            sequence_key = %seq_key,
                            "rejecting oversized message"
                        );
                        if on_failure == SequenceFailure::FailAll {
                            poisoned_keys.insert(seq_key.clone());
                        }
                        router::route_reject(
                            sqs,
                            queue_url,
                            &receipt_handle,
                            topology,
                            group.as_deref(),
                            metrics::FailReason::Oversize,
                        )
                        .await;
                        continue;
                    }

                    let message: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(
                        body.as_bytes(),
                    ) {
                        Ok(m) => m,
                        Err(err) => {
                            error!(
                                error = %err,
                                queue_url,
                                sequence_key = %seq_key,
                                "failed to deserialize SQS message, rejecting"
                            );
                            if on_failure == SequenceFailure::FailAll {
                                poisoned_keys.insert(seq_key.clone());
                            }
                            router::route_reject(
                                sqs,
                                queue_url,
                                &receipt_handle,
                                topology,
                                group.as_deref(),
                                metrics::FailReason::Deserialize,
                            )
                            .await;
                            continue;
                        }
                    };

                    let metadata = extract_metadata(&msg);

                    debug!(
                        queue_url,
                        sequence_key = %seq_key,
                        retry_count,
                        "dispatching sequenced message to handler"
                    );
                    let rx = spawn_handler_keyed::<T, H>(
                        handler,
                        ctx,
                        message,
                        metadata,
                        options.handler_timeout,
                        options.handler_timeout_outcome.clone(),
                        &completed_tx,
                        seq_key.clone(),
                        Arc::clone(&topic),
                        group.clone(),
                    );

                    key_states.insert(
                        seq_key,
                        KeyState::InFlight {
                            receipt_handle,
                            msg: Arc::new(msg),
                            retry_count,
                            outcome_rx: rx,
                        },
                    );
                    in_flight_count += 1;
                    options.processing.store(true, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Pop the next pending delivery for `key` and spawn a handler for it.
/// Called after a terminal outcome (Ack/Reject) to drain buffered messages.
#[allow(clippy::too_many_arguments)]
async fn drain_pending_for_key<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    key: &str,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
    topology: &'static QueueTopology,
    poisoned_keys: &mut HashSet<String>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key_states: &mut HashMap<String, KeyState>,
    in_flight_count: &mut usize,
    pending_deliveries: &mut HashMap<String, VecDeque<Message>>,
    topic: &Arc<str>,
    group: &Option<Arc<str>>,
) where
    T: Topic,
    H: MessageHandler<T>,
{
    // If the key is poisoned, reject all pending deliveries for it.
    // Cascade: intentionally not counted — see `metrics::FailReason`.
    if on_failure == SequenceFailure::FailAll && poisoned_keys.contains(key) {
        if let Some(pending) = pending_deliveries.remove(key) {
            for pd in pending {
                let rh = pd.receipt_handle().unwrap_or_default();
                router::route_reject_cascade(sqs, queue_url, rh, topology).await;
            }
        }
        return;
    }

    let Some(pending) = pending_deliveries.get_mut(key) else {
        return;
    };

    // Pop the next delivery and try to spawn it.
    while let Some(msg) = pending.pop_front() {
        let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
        let retry_count = router::get_retry_count(&msg);

        // Max retries check on buffered delivery.
        if retry_count >= options.max_retries {
            warn!(
                queue_url,
                sequence_key = %key,
                retry_count,
                "buffered message exceeded max retries, rejecting"
            );
            if on_failure == SequenceFailure::FailAll {
                poisoned_keys.insert(key.to_string());
                // Reject remaining pending for this key too.
                router::route_reject(
                    sqs,
                    queue_url,
                    &receipt_handle,
                    topology,
                    group.as_deref(),
                    metrics::FailReason::MaxRetriesExceeded,
                )
                .await;
                // Cascade: intentionally not counted — see `metrics::FailReason`.
                while let Some(pd) = pending.pop_front() {
                    let rh = pd.receipt_handle().unwrap_or_default();
                    router::route_reject_cascade(sqs, queue_url, rh, topology).await;
                }
                pending_deliveries.remove(key);
                return;
            }
            router::route_reject(
                sqs,
                queue_url,
                &receipt_handle,
                topology,
                group.as_deref(),
                metrics::FailReason::MaxRetriesExceeded,
            )
            .await;
            continue;
        }

        let body = extract_payload(msg.body().unwrap_or_default());

        metrics::record_message_size(topic, group.as_deref(), body.len());

        // Reject oversized messages before deserialization
        if let Err(e) = options.validate_payload_message_size(body.len()) {
            warn!(
                error = %e,
                queue_url,
                sequence_key = %key,
                "rejecting oversized buffered message"
            );
            // The offending delivery is rejected first, on both branches. The
            // FailAll branch used to drain only the *rest* of the buffer and
            // return, leaving this message invisible until its visibility
            // timeout lapsed — at which point it redelivered, was still
            // oversized, and looped. The max-retries branch above always got
            // this right; this now matches it.
            router::route_reject(
                sqs,
                queue_url,
                &receipt_handle,
                topology,
                group.as_deref(),
                metrics::FailReason::Oversize,
            )
            .await;
            if on_failure == SequenceFailure::FailAll {
                poisoned_keys.insert(key.to_string());
                // Cascade: intentionally not counted — see `metrics::FailReason`.
                while let Some(pd) = pending.pop_front() {
                    let rh = pd.receipt_handle().unwrap_or_default();
                    router::route_reject_cascade(sqs, queue_url, rh, topology).await;
                }
                pending_deliveries.remove(key);
                return;
            }
            continue;
        }

        let message: T::Message =
            match <T::Codec as crate::Codec<T::Message>>::decode(body.as_bytes()) {
                Ok(m) => m,
                Err(err) => {
                    error!(
                        error = %err,
                        queue_url,
                        sequence_key = %key,
                        "failed to deserialize buffered SQS message, rejecting"
                    );
                    // Rejected first on both branches — see the note on the
                    // oversize check above.
                    router::route_reject(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        group.as_deref(),
                        metrics::FailReason::Deserialize,
                    )
                    .await;
                    if on_failure == SequenceFailure::FailAll {
                        poisoned_keys.insert(key.to_string());
                        // Cascade: intentionally not counted — see `metrics::FailReason`.
                        while let Some(pd) = pending.pop_front() {
                            let rh = pd.receipt_handle().unwrap_or_default();
                            router::route_reject_cascade(sqs, queue_url, rh, topology).await;
                        }
                        pending_deliveries.remove(key);
                        return;
                    }
                    continue;
                }
            };

        let metadata = extract_metadata(&msg);

        let rx = spawn_handler_keyed::<T, H>(
            handler,
            ctx,
            message,
            metadata,
            options.handler_timeout,
            options.handler_timeout_outcome.clone(),
            completed_tx,
            key.to_string(),
            Arc::clone(topic),
            group.clone(),
        );

        key_states.insert(
            key.to_string(),
            KeyState::InFlight {
                receipt_handle,
                msg: Arc::new(msg),
                retry_count,
                outcome_rx: rx,
            },
        );
        *in_flight_count += 1;

        // Clean up empty deque.
        if pending.is_empty() {
            pending_deliveries.remove(key);
        }
        return;
    }

    // All pending drained without spawning (all rejected).
    pending_deliveries.remove(key);
}

// ---------------------------------------------------------------------------
// DLQ consumption loop
// ---------------------------------------------------------------------------

async fn consume_dlq_loop<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    original_queue: &str,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    shutdown: &CancellationToken,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    info!(queue_url, "DLQ consumer started");

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                debug!("shutdown signal received, stopping DLQ consumer on {queue_url}");
                return Ok(());
            }
            result = sqs
                .receive_message()
                .queue_url(queue_url)
                .wait_time_seconds(5)
                .max_number_of_messages(10)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names("All")
                .send() => {
                let output = result.map_err(|e| {
                    metrics::record_backend_error(
                        metrics::BackendLabel::SnsSqs,
                        metrics::BackendErrorKind::Consume,
                    );
                    map_sqs_error(&format!("SQS ReceiveMessage failed on DLQ {queue_url}"), e)
                })?;

                let messages = output.messages.unwrap_or_default();
                debug!(queue_url, received = messages.len(), "received messages from DLQ");

                for msg in messages {
                    let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
                    let body = extract_payload(msg.body().unwrap_or_default());
                    let metadata = extract_dead_metadata(&msg, original_queue);

                    // Before the size gate, exactly as the main loop places it
                    // after its own `extract_payload`. `original_queue` is
                    // `topology.queue()` — the SOURCE topic, not the DLQ queue
                    // name — and there is no consumer group, since `run_dlq`
                    // takes no `ConsumerOptions`. Redis already drains its DLQ
                    // through `run_stream_loop`, which labels every metric
                    // `topology.queue()` whichever stream it reads, so a DLQ
                    // name here would make `topic` mean two different things
                    // depending on the backend and would stop a per-topic size
                    // profile summing across the main and DLQ paths.
                    metrics::record_message_size(original_queue, None, body.len());

                    if body.len() > DEFAULT_MAX_MESSAGE_SIZE {
                        warn!(
                            bytes = body.len(),
                            max = DEFAULT_MAX_MESSAGE_SIZE,
                            delivery_id = %metadata.message.delivery_id,
                            "oversized DLQ message — discarding"
                        );
                    } else {
                        match <T::Codec as crate::Codec<T::Message>>::decode(body.as_bytes()) {
                            Err(err) => {
                                error!(
                                    error = %err,
                                    delivery_id = %metadata.message.delivery_id,
                                    "failed to deserialize message from DLQ — discarding"
                                );
                            }
                            Ok(message) => {
                                debug!(
                                    queue_url,
                                    delivery_id = %metadata.message.delivery_id,
                                    death_count = metadata.death_count,
                                    "dispatching DLQ message to handle_dead"
                                );
                                handler.handle_dead(message, metadata, ctx.as_ref()).await;
                            }
                        }
                    }

                    // Always ack DLQ messages.
                    debug!(queue_url, "acking DLQ message");
                    router::route_ack(sqs, queue_url, &receipt_handle).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Batch consumption
// ---------------------------------------------------------------------------
//
// [`BatchConsumerImpl`](crate::backend::BatchConsumerImpl) for SQS. SQS is
// the only batch-consuming backend whose wire primitive is already
// request-response and already returns up to a fixed cap per call
// (`ReceiveMessage`), so this loop is **poll-shaped** rather than the
// select-over-one-envelope-at-a-time shape Kafka's and InMemory's batch
// loops share: those two accumulate a batch one message at a time because
// their underlying primitive (a partition poll, an in-process queue pop)
// hands back one message at a time; SQS's `ReceiveMessage` already hands
// back up to 10 in one round trip, so accumulating here means looping
// `ReceiveMessage` calls with shrinking headroom, not selecting over a
// per-message stream. This is a genuinely different loop shape, not a
// clause-for-clause port of either existing one — noted for whoever reviews
// this, since no third copy of the shared select-loop skeleton was
// extracted; only the flush-invoking/backoff machinery
// (`invoke_batch_handler`, `batch_redelivery_backoff`,
// `next_redelivery_delay`) is shared, via `crate::backend::batch_consumer`.
//
// # The 10-message cap
//
// SQS hard-caps both `ReceiveMessage` (`MaxNumberOfMessages`) and
// `DeleteMessageBatch`/`ChangeMessageVisibilityBatch` at 10 entries per call.
// [`validate_sqs_batch_size`] rejects any `max_batch_size > 10` at consumer
// startup — including the crate-wide default of 500
// ([`DEFAULT_MAX_BATCH_SIZE`](crate::DEFAULT_MAX_BATCH_SIZE)), which is why
// this is a startup error rather than a silent clamp: clamping would turn
// every default-configured SQS batch consumer into a silent 10-message
// consumer, the "performance regression nobody can see" a clamp would cause
// to fire for every caller, not just the ones who typed 500 by mistake.
// Reject forces one explicit, checked line
// (`with_max_batch_size(10)` or less) that doubles as the caller
// acknowledging the cap.
//
// # Amortisation is bounded, not eliminated
//
// The whole point of batch consumption is amortising a sink's per-flush
// cost (one DB transaction, one HTTP request) across many messages instead
// of paying it per message. On SQS that amortisation is over **at most 10**
// messages — a fraction of what Kafka's or InMemory's defaults amortise
// over. This module makes no throughput or CPU claim about what that cap
// buys; it only guarantees the mechanics (one `ReceiveMessage`,
// one flush, one settlement call) stay batched up to that ceiling.
//
// # The visibility bound covers receive-to-settle, not batch age alone
//
// A message must be accumulated (bounded by `max_batch_age`), flushed
// (bounded by `handler_timeout`) *and* settled (one bounded API call) before
// the queue's `VisibilityTimeout` elapses, or it turns visible again
// mid-flush: a sibling consumer picks it up and processes it a second time,
// and the eventual settlement call (`DeleteMessageBatch` or
// `ChangeMessageVisibilityBatch`) partially fails on the now-stale receipt
// handle — a systematic duplicate-processing hazard on slow flushes, not an
// occasional one. The crate's own defaults (`max_batch_age` 250ms,
// `handler_timeout` 30s) already brush SQS's own default `VisibilityTimeout`
// (30s): raise `VisibilityTimeout` on the queue, or lower `handler_timeout`,
// so that `max_batch_age + handler_timeout` sits well under whatever
// `VisibilityTimeout` the queue declares.
//
// # `Redeliver`: visibility reset, not a republish
//
// A batch-wide `Outcome::Retry`/`Outcome::Defer` resolves to
// [`BatchSettlement::Redeliver`], settled by [`router::route_requeue_batch`]:
// one `ChangeMessageVisibilityBatch` call resetting every buffered handle's
// visibility to the shared backoff's next delay (`next_redelivery_delay`).
// This is deliberately **not** the single-message `route_retry`'s
// delete+re-send: a batch-wide outcome carries no sequence key and no
// per-message retry budget (see
// [`BatchSettlement`](crate::backend::batch_consumer::BatchSettlement)'s
// doc), so there is nothing for a retry count to track, and topology
// `hold_queues` — which key single-message retry delay off a per-message
// retry count — **do not apply to batch redelivery**; the shared escalating
// backoff paces it instead. A message that was previously re-sent by the
// *single-message* retry path carries a frozen `x-retry-count` attribute
// that batch redeliveries never move (though `ApproximateReceiveCount`,
// surfaced as `delivery_count`, keeps incrementing on every SQS-side
// redelivery regardless of which path is redelivering it).
//
// # `DeadLetter` is not process-terminal on SQS
//
// A batch-wide `Outcome::Reject` resolves to `BatchSettlement::DeadLetter`,
// settled by [`router::route_reject_batch`]: `messages_failed_total` records
// once per message, then every handle's visibility resets to 0 immediately
// — same mechanics as the single-message [`router::route_reject`]. Unlike
// Kafka's or InMemory's `DeadLetter` arm, this is **not terminal**: shove
// never publishes to a DLQ on this backend, so whether the batch ever
// reaches one is entirely up to the queue's *AWS-side* redrive policy
// (`maxReceiveCount`) — and until redrive fires, a handler that rejects a
// batch will **re-see the same terminally-rejected batch** on the very next
// flush, because nothing removed it from the queue. This is why the
// redelivery backoff resets only on `Commit`, never on `DeadLetter` (see
// [`flush_sqs_batch`]): an always-Reject handler would otherwise spin
// receives at full API rate forever. The escalating sleep this loop takes
// after a `DeadLetter` flush is head-of-line blocking for this consumer —
// healthy messages sitting behind a rejecting batch wait out each 1→30s
// delay too, a stall Kafka's genuinely-terminal reject arm cannot produce.
// With no redrive policy configured, the cycle never ends short of the
// queue's retention period; [`router::route_reject_batch`] carries the same
// loud "no DLQ configured" warning [`router::route_reject`] does.
//
// # Poison hot-loop
//
// An oversized or undecodable message is rejected (visibility → 0)
// immediately, outside the batch — see "Pre-handler drops" below — and
// therefore returns on the very next `ReceiveMessage`, still oversized or
// undecodable, and is rejected again: a tight per-message reject loop, one
// `record_failed` per round, that only stops once AWS redrive moves it (or,
// with no redrive policy, never). This is the same property the
// single-message SQS consumer already has; this doc names it rather than
// leaving it implied by "settled once".
//
// # Pre-handler drops settle immediately, uncounted toward the batch
//
// An oversized or undecodable message never enters the batch: it is
// rejected via the existing single-message [`router::route_reject`] the
// moment it is decoded, with its true [`metrics::FailReason`] (`Oversize` or
// `Deserialize`), and does not count toward `flush_len`/`max_batch_size` or
// arm the age deadline. Kafka and InMemory park an equivalent drop until
// their batch's flush, because their drops must ride the same commit their
// batch's other messages retire through (Kafka's offsets must commit past
// them; InMemory owns the envelope outright until the flush resolves it).
// Neither reason exists here: every SQS message settles independently by
// receipt handle, and shove never publishes to a DLQ on this backend, so a
// drop settled at receive time is exactly as final as one settled at flush
// time — parking it would only delay a settlement that is already as final
// as it will ever be. The discard counter (`messages_discarded_total`)
// stays opted out here, same as every other SQS reject path (see
// [`router::route_reject`]'s doc) — `messages_failed_total` is the signal.
//
// # No separate sequencing-guard call
//
// Unlike Kafka, SQS exposes no public inherent `run_batch` entry point that
// could bypass [`crate::batch_consumer::BatchConsumer::run`]'s
// `validate_batch_topic` call — [`BatchConsumerImpl::run_batch`] is reached
// only through that generic wrapper, which already ran the guard. A second
// check here would only ever repeat the first, exactly as InMemory's
// `run_batch_impl` documents for its own case.
//
// # No `in_flight`/`processing` bookkeeping
//
// Every message this loop receives is already `messages_not_visible`
// broker-side the moment `ReceiveMessage` returns it — SQS itself tracks
// that, unlike InMemory's in-process queue, which needs its own
// `in_flight` counter for exactly this. So the backlog-0/in-flight-0 window
// an autoscaler could otherwise mistake for "nothing outstanding" cannot
// open here without any extra bookkeeping.

/// SQS's hard per-call cap on `ReceiveMessage`'s `MaxNumberOfMessages` and on
/// `DeleteMessageBatch`/`ChangeMessageVisibilityBatch`'s entry count.
const SQS_MAX_BATCH: usize = 10;

/// Reject a batch consumer's configured `max_batch_size` outright when it
/// exceeds SQS's hard 10-message cap on `ReceiveMessage` and on
/// `DeleteMessageBatch`/`ChangeMessageVisibilityBatch` — see the module
/// doc's "The 10-message cap" section for why this rejects instead of
/// clamping. Pure and unit-tested directly; called before any AWS call and
/// before the batch buffer's own allocation, mirroring the sequencing
/// guard's (`validate_batch_topic`) fail-fast shape.
fn validate_sqs_batch_size(max_batch_size: usize) -> Result<()> {
    if max_batch_size > SQS_MAX_BATCH {
        return Err(ShoveError::Validation(format!(
            "SQS batch consumer max_batch_size ({max_batch_size}) exceeds SQS's hard \
             {SQS_MAX_BATCH}-message cap on ReceiveMessage and on \
             DeleteMessageBatch/ChangeMessageVisibilityBatch. This crate's default \
             max_batch_size (500) also exceeds it — set \
             `BatchConsumerOptions::with_max_batch_size({SQS_MAX_BATCH})` or less explicitly."
        )));
    }
    Ok(())
}

/// One in-flight SQS batch: `messages`/`handles` are index-parallel — the
/// receipt handle for `messages[i]` is `handles[i]` — kept separate (rather
/// than one `Vec` of a combined struct) because a flush needs to move
/// `messages` into the handler by value while `handles` survives the flush
/// to settle afterward.
///
/// No `cap`/`PREALLOC_CAP` clamp, unlike Kafka's `BatchBuffer` and
/// InMemory's `InMemoryBatch`: [`validate_sqs_batch_size`] already bounds
/// `max_batch_size` to at most [`SQS_MAX_BATCH`] (10) before this is ever
/// constructed, so sizing the initial allocation to the real cap can never
/// overflow the way an unclamped `usize::MAX` could on those two backends.
struct SqsBatch<T: Topic> {
    messages: Vec<(T::Message, MessageMetadata)>,
    handles: Vec<String>,
}

impl<T: Topic> SqsBatch<T> {
    fn new(max_batch_size: usize) -> Self {
        Self {
            messages: Vec::with_capacity(max_batch_size),
            handles: Vec::with_capacity(max_batch_size),
        }
    }

    fn len(&self) -> usize {
        self.messages.len()
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    fn push(&mut self, message: T::Message, metadata: MessageMetadata, receipt_handle: String) {
        self.messages.push((message, metadata));
        self.handles.push(receipt_handle);
    }

    /// Take both lists for a flush, leaving both empty (capacity retained by
    /// `Vec::drain`/`mem::take`'s allocation is not preserved here — a fresh
    /// batch is small and short-lived enough that re-growing to at most 10
    /// entries costs nothing worth avoiding a second allocation for).
    fn take(&mut self) -> (Vec<(T::Message, MessageMetadata)>, Vec<String>) {
        (
            std::mem::take(&mut self.messages),
            std::mem::take(&mut self.handles),
        )
    }
}

/// Fields [`flush_sqs_batch`] needs that do not change across flushes —
/// split out so the flush function's signature does not grow every time a
/// new one is needed, mirroring Kafka's `BatchFlushCtx` and InMemory's
/// `InMemoryFlushCtx`.
struct SqsBatchFlushCtx<'a> {
    sqs: &'a aws_sdk_sqs::Client,
    queue_url: &'a str,
    topology: &'static QueueTopology,
    topic: &'a str,
    group: Option<&'a str>,
    shutdown: &'a CancellationToken,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
}

/// Hands the buffered batch to the handler and settles the single returned
/// [`Outcome`] via the shared [`settle_batch_outcome`] classifier. See the
/// module doc's "`Redeliver`" and "`DeadLetter` is not process-terminal"
/// sections for the mechanics each arm below performs.
async fn flush_sqs_batch<T, H>(
    flush: &SqsBatchFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    batch: &mut SqsBatch<T>,
    redelivery_backoff: &mut Backoff,
) where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    if batch.is_empty() {
        return;
    }
    let batch_size = batch.len();
    let (messages, handles) = batch.take();

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
            router::route_ack_batch(flush.sqs, flush.queue_url, handles).await;
            // This flush retired cleanly, so the next `DeadLetter`/`Redeliver`
            // starts escalating from the beginning again.
            *redelivery_backoff = batch_redelivery_backoff();
        }
        BatchSettlement::DeadLetter => {
            router::route_reject_batch(
                flush.sqs,
                flush.queue_url,
                &handles,
                flush.topology,
                flush.group,
                metrics::FailReason::Rejected,
            )
            .await;
            // Deliberately NOT reset — see the module doc's "`DeadLetter` is
            // not process-terminal" section. SQS's DeadLetter arm re-sees
            // the same batch until AWS redrive moves it, so an always-Reject
            // handler must still escalate 1s -> 30s instead of spinning
            // receives at full API rate.
            let delay = next_redelivery_delay(redelivery_backoff);
            tracing::warn!(
                queue = flush.topology.queue(),
                batch_size,
                ?outcome,
                delay_ms = delay.as_millis() as u64,
                "batch handler rejected; redelivery is paced by AWS redrive, pacing this consumer's re-receive"
            );
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = flush.shutdown.cancelled() => {}
            }
        }
        BatchSettlement::Redeliver => {
            // A shutdown-time Retry/Defer must not strand the batch invisible
            // for the escalated delay: release it NOW, exactly like the
            // single-message drain's `route_requeue` on shutdown.
            let delay = if flush.shutdown.is_cancelled() {
                Duration::ZERO
            } else {
                next_redelivery_delay(redelivery_backoff)
            };
            tracing::warn!(
                queue = flush.topology.queue(),
                batch_size,
                ?outcome,
                delay_ms = delay.as_millis() as u64,
                "batch handler returned a non-Ack outcome, redelivering the whole batch"
            );
            // Requeue (the visibility-change call itself) IS the point of no
            // return: after it, the batch lives broker-side with its delay,
            // so aborting this task mid-sleep strands nothing — the same
            // requeue-before-sleep invariant InMemory's `Redeliver` arm
            // documents.
            router::route_requeue_batch(flush.sqs, flush.queue_url, &handles, delay).await;
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = flush.shutdown.cancelled() => {}
            }
        }
    }
}

/// The accumulation loop: poll-shaped, not select-shaped — see the module
/// doc's opening section for why. Runs one attempt; [`SqsConsumer::run_batch_with_inner`]
/// wraps this in [`run_with_reconnect`] the same way
/// [`SqsConsumer::run_with_inner`] wraps [`consume_loop_concurrent`], so a
/// transient error here restarts with a fresh, empty batch rather than
/// resuming a partial one — any handles already buffered when the error
/// happened are made visible immediately (see the `ReceiveMessage`-error arm
/// below) rather than carried across the reconnect.
async fn run_batch_loop<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    topology: &'static QueueTopology,
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &BatchConsumerOptionsInner,
) -> Result<()>
where
    T: NotSequenced,
    H: BatchMessageHandler<T>,
{
    let topic: Arc<str> = Arc::from(topology.queue());
    let group: Option<Arc<str>> = options.consumer_group.as_deref().map(Arc::from);
    // Validated `<= SQS_MAX_BATCH` by `validate_sqs_batch_size` before this
    // loop is ever entered (see `run_batch_with_inner`), and
    // `BatchConsumerOptions::with_max_batch_size` asserts `n > 0` at the
    // builder, so `max_batch_size` is already in `1..=SQS_MAX_BATCH` here.
    let max_batch_size = options.max_batch_size;
    let max_batch_age = options.max_batch_age;
    let max_message_size = options.max_message_size;
    let handler_timeout = options.handler_timeout;
    let handler_timeout_outcome = options.handler_timeout_outcome.clone();

    let flush_ctx = SqsBatchFlushCtx {
        sqs,
        queue_url,
        topology,
        topic: &topic,
        group: group.as_deref(),
        shutdown: &options.shutdown,
        handler_timeout,
        handler_timeout_outcome,
    };

    let mut batch: SqsBatch<T> = SqsBatch::new(max_batch_size);
    let mut deadline: Option<tokio::time::Instant> = None;
    let mut redelivery_backoff = batch_redelivery_backoff();

    tracing::info!(
        queue_url,
        max_batch_size,
        ?max_batch_age,
        "SQS batch consumer started"
    );

    loop {
        if batch.len() >= max_batch_size {
            flush_sqs_batch(
                &flush_ctx,
                handler.as_ref(),
                ctx.as_ref(),
                &mut batch,
                &mut redelivery_backoff,
            )
            .await;
            deadline = None;
            continue;
        }

        if let Some(d) = deadline
            && tokio::time::Instant::now() >= d
        {
            flush_sqs_batch(
                &flush_ctx,
                handler.as_ref(),
                ctx.as_ref(),
                &mut batch,
                &mut redelivery_backoff,
            )
            .await;
            deadline = None;
            continue;
        }

        if options.shutdown.is_cancelled() {
            if !batch.is_empty() {
                flush_sqs_batch(
                    &flush_ctx,
                    handler.as_ref(),
                    ctx.as_ref(),
                    &mut batch,
                    &mut redelivery_backoff,
                )
                .await;
            }
            debug!(
                queue_url,
                "shutdown signal received, SQS batch consumer stopped"
            );
            return Ok(());
        }

        // Headroom is always > 0 here: the size-trigger check above already
        // returned/continued when `batch.len() >= max_batch_size`.
        let headroom = max_batch_size - batch.len();

        // Same short-poll + client-side backoff rationale as
        // `consume_loop_concurrent` (see its own comment): a server-side long
        // poll would hold an open connection on LocalStack's serial request
        // handling for the full wait duration, stacking up across consumers.
        let receive_result = sqs
            .receive_message()
            .queue_url(queue_url)
            .wait_time_seconds(0)
            .max_number_of_messages(headroom as i32)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .message_attribute_names("All")
            .send()
            .await;

        let msgs = match receive_result {
            Ok(output) => output.messages.unwrap_or_default(),
            Err(e) => {
                metrics::record_backend_error(
                    metrics::BackendLabel::SnsSqs,
                    metrics::BackendErrorKind::Consume,
                );
                // Buffered handles did nothing wrong — they are released to
                // be visible NOW, never the escalated redelivery backoff
                // delay, so a transient receive error does not additionally
                // delay messages that were already safely buffered.
                if !batch.handles.is_empty() {
                    router::route_requeue_batch(sqs, queue_url, &batch.handles, Duration::ZERO)
                        .await;
                }
                return Err(map_sqs_error(
                    &format!("SQS ReceiveMessage failed on {queue_url}"),
                    e,
                ));
            }
        };

        if msgs.is_empty() {
            let sleep_for = match deadline {
                Some(d) => {
                    let now = tokio::time::Instant::now();
                    if d <= now {
                        Duration::ZERO
                    } else {
                        (d - now).min(Duration::from_millis(500))
                    }
                }
                None => Duration::from_millis(500),
            };
            if !sleep_for.is_zero() {
                tokio::select! {
                    biased;
                    _ = options.shutdown.cancelled() => {}
                    _ = tokio::time::sleep(sleep_for) => {}
                }
            }
            continue;
        }

        for msg in msgs {
            let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
            let body = extract_payload(msg.body().unwrap_or_default());

            metrics::record_message_size(&topic, group.as_deref(), body.len());

            // Pre-handler drop: settles immediately via the single-message
            // `route_reject`, outside the batch — see the module doc's
            // "Pre-handler drops" section for why this does not park like
            // Kafka/InMemory.
            if let Err(e) = validate_message_size(body.len(), max_message_size) {
                warn!(error = %e, queue_url, "rejecting oversized message (pre-handler drop)");
                router::route_reject(
                    sqs,
                    queue_url,
                    &receipt_handle,
                    topology,
                    group.as_deref(),
                    metrics::FailReason::Oversize,
                )
                .await;
                continue;
            }

            let message: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(
                body.as_bytes(),
            ) {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, queue_url, "failed to deserialize SQS message, rejecting (pre-handler drop)");
                    router::route_reject(
                        sqs,
                        queue_url,
                        &receipt_handle,
                        topology,
                        group.as_deref(),
                        metrics::FailReason::Deserialize,
                    )
                    .await;
                    continue;
                }
            };

            let metadata = extract_metadata(&msg);
            if batch.is_empty() {
                deadline = Some(tokio::time::Instant::now() + max_batch_age);
            }
            batch.push(message, metadata, receipt_handle);
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer trait implementation
// ---------------------------------------------------------------------------

impl SqsConsumer {
    pub fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Sqs>,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_with_inner::<T, H>(handler, ctx, options.into_inner())
    }

    pub(crate) fn run_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let client = self.client.clone();
        let queue_registry = self.queue_registry.clone();
        async move {
            let topology = T::topology();
            let consumer = SqsConsumer::new(client, queue_registry);
            let queue_url = consumer.resolve_queue_url(topology.queue()).await?;
            let handler = Arc::new(handler);
            let ctx = Arc::new(ctx);
            let sqs = consumer.client.sqs().clone();

            run_with_reconnect(
                &options.shutdown,
                topology.queue(),
                options.max_reconnect_attempts,
                || {
                    consume_loop_concurrent::<T, H>(
                        &sqs, &queue_url, topology, &handler, &ctx, &options,
                    )
                },
            )
            .await
        }
    }

    pub fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Sqs>,
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
        options: crate::ConsumerOptions<Sqs>,
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
                error!(error = %e, "run_fifo_until_timeout: shard spawn failed");
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
                Ok(Err(e)) => error!("SQS sequenced shard task failed: {e}"),
                Err(e) => error!("SQS sequenced shard task panicked: {e}"),
            }
        }
        Ok(())
    }

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
        let seq = topology
            .sequencing()
            .ok_or_else(|| ShoveError::Topology("run_fifo requires a sequenced topic".into()))?;

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let consumer = SqsConsumer::new(self.client.clone(), self.queue_registry.clone());
        let on_failure = seq.on_failure();
        let mut handles = Vec::new();

        for i in 0..seq.routing_shards() {
            let shard_queue_name = format!("{}-seq-{i}", topology.queue());
            let shard_queue_url = consumer.resolve_queue_url(&shard_queue_name).await?;

            let sqs = consumer.client.sqs().clone();
            let h = handler.clone();
            let c = ctx.clone();
            let opts = options.clone();

            handles.push(tokio::spawn(async move {
                run_sequenced_shard::<T, H>(
                    &sqs,
                    &shard_queue_url,
                    &shard_queue_name,
                    topology,
                    &h,
                    &c,
                    &opts,
                    on_failure,
                )
                .await
            }));
        }

        Ok(handles)
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
        let client = self.client.clone();
        let queue_registry = self.queue_registry.clone();
        async move {
            let topology = T::topology();
            let dlq = topology.dlq().ok_or_else(|| {
                ShoveError::Topology(format!(
                    "topic '{}' has no DLQ configured",
                    topology.queue()
                ))
            })?;
            let consumer = SqsConsumer::new(client, queue_registry);
            let queue_url = consumer.resolve_queue_url(dlq).await?;
            let handler = Arc::new(handler);
            let ctx = Arc::new(ctx);
            let sqs = consumer.client.sqs().clone();
            let shutdown = consumer.client.shutdown_token();

            run_with_reconnect(&shutdown, dlq, None, || {
                consume_dlq_loop::<T, H>(
                    &sqs,
                    &queue_url,
                    topology.queue(),
                    &handler,
                    &ctx,
                    &shutdown,
                )
            })
            .await
        }
    }

    /// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl) for
    /// SQS. See the "Batch consumption" module doc above [`run_batch_loop`]
    /// for the loop's full contract (the 10-message cap, amortisation bound,
    /// visibility timing, redelivery/reject mechanics).
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
        let client = self.client.clone();
        let queue_registry = self.queue_registry.clone();
        async move {
            // Before any AWS call and before `SqsBatch`'s own allocation —
            // mirrors the sequencing guard's fail-fast shape.
            validate_sqs_batch_size(options.max_batch_size)?;

            let topology = T::topology();
            let consumer = SqsConsumer::new(client, queue_registry);
            let queue_url = consumer.resolve_queue_url(topology.queue()).await?;
            let handler = Arc::new(handler);
            let ctx = Arc::new(ctx);
            let sqs = consumer.client.sqs().clone();

            run_with_reconnect(
                &options.shutdown,
                topology.queue(),
                options.max_reconnect_attempts,
                || run_batch_loop::<T, H>(&sqs, &queue_url, topology, &handler, &ctx, &options),
            )
            .await
        }
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn approximate_receive_count_is_read_from_system_attributes() {
        let msg = Message::builder()
            .body("{}")
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "4")
            .build();
        assert_eq!(approximate_receive_count(&msg), Some(4));
    }

    #[test]
    fn approximate_receive_count_is_none_when_sqs_omits_it() {
        let msg = Message::builder().body("{}").build();
        assert_eq!(approximate_receive_count(&msg), None);
    }

    #[test]
    fn approximate_receive_count_is_none_when_unparseable() {
        let msg = Message::builder()
            .body("{}")
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "many")
            .build();
        assert_eq!(approximate_receive_count(&msg), None);
    }
}

#[cfg(test)]
mod batch_cap_tests {
    use super::*;
    use crate::consumer::DEFAULT_MAX_BATCH_SIZE;

    #[test]
    fn one_is_ok() {
        assert!(validate_sqs_batch_size(1).is_ok());
    }

    #[test]
    fn ten_is_ok() {
        assert!(validate_sqs_batch_size(10).is_ok());
    }

    #[test]
    fn eleven_is_rejected() {
        let err = validate_sqs_batch_size(11).expect_err("11 exceeds the SQS cap");
        assert!(matches!(err, ShoveError::Validation(_)));
    }

    #[test]
    fn usize_max_is_rejected() {
        let err = validate_sqs_batch_size(usize::MAX).expect_err("usize::MAX exceeds the cap");
        assert!(matches!(err, ShoveError::Validation(_)));
    }

    /// The crate-wide default (`DEFAULT_MAX_BATCH_SIZE = 500`) is itself
    /// over the cap — the decisive fact behind rejecting instead of
    /// clamping (see the module doc's cap-rule rationale): a caller who
    /// never touches `with_max_batch_size` would otherwise silently get a
    /// 10-message consumer.
    #[test]
    fn the_crate_default_exceeds_the_cap() {
        let err = validate_sqs_batch_size(DEFAULT_MAX_BATCH_SIZE)
            .expect_err("the crate default of 500 exceeds the SQS cap of 10");
        assert!(matches!(err, ShoveError::Validation(_)));
    }

    #[test]
    fn error_text_names_both_aws_apis_and_the_cap() {
        let err = validate_sqs_batch_size(11).expect_err("11 exceeds the cap");
        let ShoveError::Validation(msg) = err else {
            panic!("expected ShoveError::Validation, got {err:?}");
        };
        assert!(msg.contains("ReceiveMessage"), "message: {msg}");
        assert!(msg.contains("DeleteMessageBatch"), "message: {msg}");
        assert!(msg.contains("10"), "message: {msg}");
        assert!(msg.contains("500"), "message: {msg}");
    }
}
