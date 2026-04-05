use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::RECONNECT_DELAY;
use crate::backends::sns::client::SnsClient;
use crate::backends::sns::router;
use crate::backends::sns::topology::QueueRegistry;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::{QueueTopology, SequenceFailure};

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

fn extract_metadata(msg: &aws_sdk_sqs::types::Message) -> MessageMetadata {
    let retry_count = router::get_retry_count(msg);
    MessageMetadata {
        retry_count,
        delivery_id: msg.message_id().unwrap_or_default().to_string(),
        redelivered: retry_count > 0,
        headers: router::extract_message_attributes(msg),
    }
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

fn extract_dead_metadata(
    msg: &aws_sdk_sqs::types::Message,
    queue_name: &str,
) -> DeadMessageMetadata {
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
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                warn!("consumer error on {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                tokio::select! {
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

async fn invoke_handler<T: Topic, H: MessageHandler<T>>(
    handler: &H,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
) -> Outcome {
    match timeout {
        Some(duration) => tokio::time::timeout(duration, handler.handle(message, metadata))
            .await
            .unwrap_or_else(|_| {
                warn!("handler exceeded timeout ({duration:?}), retrying message");
                Outcome::Retry
            }),
        None => handler.handle(message, metadata).await,
    }
}

/// Spawns a handler task for a deserialized message.
/// Returns the oneshot receiver that will resolve with the handler's outcome.
fn spawn_handler<T, H>(
    handler: &Arc<H>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    notify: &Arc<Notify>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let n = notify.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler::<T, H>(&h, message, metadata, timeout).await;
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
    outcome_rx: oneshot::Receiver<Outcome>,
}

async fn consume_loop_concurrent<T, H>(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    topology: &'static QueueTopology,
    handler: &Arc<H>,
    options: &ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let notify = Arc::new(Notify::new());

    // Max number of handlers running concurrently (1 = serial / non-concurrent mode).
    let max_in_flight = options.prefetch_count as usize;

    // How many messages to request per SQS poll.  When `receive_batch_size` is
    // set (non-concurrent mode), we fetch a full batch even though we only run
    // one handler at a time.  This amortises the `ReceiveMessage` API call
    // overhead across multiple messages, which is critical for throughput when
    // several consumers share the same queue.
    let receive_batch: usize = {
        let configured = if options.receive_batch_size > 0 {
            options.receive_batch_size as usize
        } else {
            max_in_flight
        };
        configured.min(10)
    };

    let mut in_flight: VecDeque<PendingMessage> = VecDeque::with_capacity(max_in_flight);
    // Received from SQS but not yet dispatched to a handler.  Populated when
    // the receive batch is larger than `max_in_flight`.
    let mut local_buffer: VecDeque<aws_sdk_sqs::types::Message> =
        VecDeque::with_capacity(receive_batch);

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
                    let msg = in_flight.pop_front().unwrap();
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
                    let msg = in_flight.pop_front().unwrap();
                    debug!(queue_url, ?outcome, "message handled");
                    route_outcome(sqs, queue_url, &msg.receipt_handle, outcome, topology, 0).await;
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
                    let msg = in_flight.pop_front().unwrap();
                    warn!(queue_url, "handler task panicked, retrying message");
                    route_outcome(
                        sqs,
                        queue_url,
                        &msg.receipt_handle,
                        Outcome::Retry,
                        topology,
                        0,
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
            for pending in in_flight {
                let outcome = pending.outcome_rx.await.unwrap_or(Outcome::Retry);
                route_outcome(
                    sqs,
                    queue_url,
                    &pending.receipt_handle,
                    outcome,
                    topology,
                    0,
                )
                .await;
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
                router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
                continue;
            }

            let body = extract_payload(msg.body().unwrap_or_default());
            let message: T::Message = match serde_json::from_str(&body) {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, queue_url, "failed to deserialize SQS message, rejecting");
                    router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
            let rx =
                spawn_handler::<T, H>(handler, message, metadata, options.handler_timeout, &notify);
            in_flight.push_back(PendingMessage {
                receipt_handle,
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
                .message_system_attribute_names(
                    aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                )
                .message_attribute_names("All")
                .send()
                .await
                .map_err(|e| {
                    ShoveError::Connection(format!("SQS ReceiveMessage failed on {queue_url}: {e}"))
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
async fn route_outcome(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
) {
    match outcome {
        Outcome::Ack => router::route_ack(sqs, queue_url, receipt_handle).await,
        Outcome::Retry => {
            router::route_retry(sqs, queue_url, receipt_handle, topology, retry_count).await;
        }
        Outcome::Reject => router::route_reject(sqs, queue_url, receipt_handle, topology).await,
        Outcome::Defer => router::route_defer(sqs, queue_url, receipt_handle, topology).await,
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
        retry_count: u32,
        outcome_rx: oneshot::Receiver<Outcome>,
    },
    /// The handler returned Retry/Defer and the message visibility has been
    /// changed. The key is blocked until the retry comes back.
    AwaitingRetry,
}

/// Extract the sequence key from SQS MessageGroupId system attribute.
fn extract_sequence_key(msg: &aws_sdk_sqs::types::Message) -> Option<String> {
    msg.attributes()
        .and_then(|attrs| {
            attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::MessageGroupId)
        })
        .map(|s| s.to_string())
}

/// Spawns a handler task for a sequenced message, signalling completion via an
/// mpsc channel with the sequence key.
fn spawn_handler_keyed<T, H>(
    handler: &Arc<H>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key: String,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let ctx = completed_tx.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler::<T, H>(&h, message, metadata, timeout).await;
        let _ = tx.send(outcome);
        let _ = ctx.send(key);
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
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let mut poisoned_keys = HashSet::new();
    let mut pending_deliveries: HashMap<String, VecDeque<aws_sdk_sqs::types::Message>> =
        HashMap::new();

    loop {
        match consume_loop_sequenced::<T, H>(
            sqs,
            queue_url,
            topology,
            handler,
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
                warn!("consumer error on {queue_name}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                tokio::select! {
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
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
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
    poisoned_keys: &mut HashSet<String>,
    pending_deliveries: &mut HashMap<String, VecDeque<aws_sdk_sqs::types::Message>>,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let prefetch = options.prefetch_count as usize;
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<String>();

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
                    // Notified but not ready yet (shouldn't happen in practice).
                    key_states.insert(
                        key,
                        KeyState::InFlight {
                            receipt_handle,
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
                        options,
                        on_failure,
                        topology,
                        poisoned_keys,
                        &completed_tx,
                        &mut key_states,
                        &mut in_flight_count,
                        pending_deliveries,
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
                    router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
                    in_flight_count -= 1;
                    drain_pending_for_key::<T, H>(
                        sqs,
                        queue_url,
                        &key,
                        handler,
                        options,
                        on_failure,
                        topology,
                        poisoned_keys,
                        &completed_tx,
                        &mut key_states,
                        &mut in_flight_count,
                        pending_deliveries,
                    )
                    .await;
                }
                Outcome::Retry => {
                    router::route_retry(sqs, queue_url, &receipt_handle, topology, retry_count)
                        .await;
                    in_flight_count -= 1;
                    key_states.insert(key, KeyState::AwaitingRetry);
                }
                Outcome::Defer => {
                    router::route_defer(sqs, queue_url, &receipt_handle, topology).await;
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
                // Wait for all in-flight handlers to complete.
                for (key, state) in key_states.drain() {
                    if let KeyState::InFlight { receipt_handle, retry_count, outcome_rx } = state {
                        let outcome = outcome_rx.await.unwrap_or(Outcome::Retry);
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
                                router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
                            }
                            Outcome::Retry => {
                                router::route_retry(sqs, queue_url, &receipt_handle, topology, retry_count)
                                    .await;
                            }
                            Outcome::Defer => {
                                router::route_defer(sqs, queue_url, &receipt_handle, topology)
                                    .await;
                            }
                        }
                    }
                }
                // Pending deliveries: change visibility to 0 so they are redelivered.
                for (_key, msgs) in pending_deliveries.drain() {
                    for msg in msgs {
                        let rh = msg.receipt_handle().unwrap_or_default();
                        router::route_reject(sqs, queue_url, rh, topology).await;
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
                    .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                    .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::MessageGroupId)
                    .message_attribute_names("All")
                    .send()
                    .await
            }, if can_accept => {
                let messages = result
                    .map_err(|e| {
                        ShoveError::Connection(format!(
                            "SQS ReceiveMessage failed on {queue_url}: {e}"
                        ))
                    })?
                    .messages
                    .unwrap_or_default();

                debug!(queue_url, received = messages.len(), "received messages from SQS (sequenced)");

                for msg in messages {
                    let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
                    let retry_count = router::get_retry_count(&msg);
                    let seq_key = match extract_sequence_key(&msg) {
                        Some(k) => k,
                        None => {
                            warn!(
                                queue_url,
                                "message missing MessageGroupId, rejecting"
                            );
                            router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                        router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                            if let Some(pending) = pending_deliveries.remove(&seq_key) {
                                for pd in pending {
                                    let rh = pd.receipt_handle().unwrap_or_default();
                                    router::route_reject(sqs, queue_url, rh, topology).await;
                                }
                            }
                        }
                        router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                                    router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                                        router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                    let message: T::Message = match serde_json::from_str(&body) {
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
                            router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
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
                        message,
                        metadata,
                        options.handler_timeout,
                        &completed_tx,
                        seq_key.clone(),
                    );

                    key_states.insert(
                        seq_key,
                        KeyState::InFlight {
                            receipt_handle,
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
    options: &ConsumerOptions,
    on_failure: SequenceFailure,
    topology: &'static QueueTopology,
    poisoned_keys: &mut HashSet<String>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key_states: &mut HashMap<String, KeyState>,
    in_flight_count: &mut usize,
    pending_deliveries: &mut HashMap<String, VecDeque<aws_sdk_sqs::types::Message>>,
) where
    T: Topic,
    H: MessageHandler<T>,
{
    // If the key is poisoned, reject all pending deliveries for it.
    if on_failure == SequenceFailure::FailAll && poisoned_keys.contains(key) {
        if let Some(pending) = pending_deliveries.remove(key) {
            for pd in pending {
                let rh = pd.receipt_handle().unwrap_or_default();
                router::route_reject(sqs, queue_url, rh, topology).await;
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
                router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
                while let Some(pd) = pending.pop_front() {
                    let rh = pd.receipt_handle().unwrap_or_default();
                    router::route_reject(sqs, queue_url, rh, topology).await;
                }
                pending_deliveries.remove(key);
                return;
            }
            router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
            continue;
        }

        let body = extract_payload(msg.body().unwrap_or_default());
        let message: T::Message = match serde_json::from_str(&body) {
            Ok(m) => m,
            Err(err) => {
                error!(
                    error = %err,
                    queue_url,
                    sequence_key = %key,
                    "failed to deserialize buffered SQS message, rejecting"
                );
                if on_failure == SequenceFailure::FailAll {
                    poisoned_keys.insert(key.to_string());
                    while let Some(pd) = pending.pop_front() {
                        let rh = pd.receipt_handle().unwrap_or_default();
                        router::route_reject(sqs, queue_url, rh, topology).await;
                    }
                    pending_deliveries.remove(key);
                    return;
                }
                router::route_reject(sqs, queue_url, &receipt_handle, topology).await;
                continue;
            }
        };

        let metadata = extract_metadata(&msg);

        let rx = spawn_handler_keyed::<T, H>(
            handler,
            message,
            metadata,
            options.handler_timeout,
            completed_tx,
            key.to_string(),
        );

        key_states.insert(
            key.to_string(),
            KeyState::InFlight {
                receipt_handle,
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
                .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names("All")
                .send() => {
                let output = result.map_err(|e| {
                    ShoveError::Connection(format!("SQS ReceiveMessage failed on DLQ {queue_url}: {e}"))
                })?;

                let messages = output.messages.unwrap_or_default();
                debug!(queue_url, received = messages.len(), "received messages from DLQ");

                for msg in messages {
                    let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();
                    let body = extract_payload(msg.body().unwrap_or_default());
                    let metadata = extract_dead_metadata(&msg, original_queue);

                    match serde_json::from_str::<T::Message>(&body) {
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
                            handler.handle_dead(message, metadata).await;
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
// Consumer trait implementation
// ---------------------------------------------------------------------------

impl Consumer for SqsConsumer {
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        let queue_registry = self.queue_registry.clone();
        async move {
            let topology = T::topology();
            let consumer = SqsConsumer::new(client, queue_registry);
            let queue_url = consumer.resolve_queue_url(topology.queue()).await?;
            let handler = Arc::new(handler);
            let sqs = consumer.client.sqs().clone();

            run_with_reconnect(&options.shutdown, topology.queue(), || {
                consume_loop_concurrent::<T, _>(&sqs, &queue_url, topology, &handler, &options)
            })
            .await
        }
    }

    fn run_fifo<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        let queue_registry = self.queue_registry.clone();
        async move {
            let topology = T::topology();
            let seq = topology.sequencing().ok_or_else(|| {
                ShoveError::Topology("run_fifo requires a sequenced topic".into())
            })?;

            let handler = Arc::new(handler);
            let consumer = SqsConsumer::new(client, queue_registry);
            let on_failure = seq.on_failure();
            let mut handles = Vec::new();

            for i in 0..seq.routing_shards() {
                let shard_queue_name = format!("{}-seq-{i}", topology.queue());
                let shard_queue_url = consumer.resolve_queue_url(&shard_queue_name).await?;

                let sqs = consumer.client.sqs().clone();
                let h = handler.clone();
                let opts = options.clone();

                handles.push(tokio::spawn(async move {
                    run_sequenced_shard::<T, _>(
                        &sqs,
                        &shard_queue_url,
                        &shard_queue_name,
                        topology,
                        &h,
                        &opts,
                        on_failure,
                    )
                    .await
                }));
            }

            // Wait for all shards
            for handle in handles {
                if let Err(e) = handle.await {
                    error!("shard consumer task panicked: {e}");
                }
            }

            Ok(())
        }
    }

    fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> impl Future<Output = Result<()>> + Send {
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
            let sqs = consumer.client.sqs().clone();
            let shutdown = consumer.client.shutdown_token();

            run_with_reconnect(&shutdown, dlq, || {
                consume_dlq_loop::<T, _>(&sqs, &queue_url, topology.queue(), &handler, &shutdown)
            })
            .await
        }
    }
}
