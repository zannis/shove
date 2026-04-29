use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::Message;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind};
use async_nats::jetstream::message::AckKind;
use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::consumer::validate_message_size;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, HoldQueue, Nats, ShoveError};
use std::future::Future;

use super::client::NatsClient;
use super::constants::{
    CONNECTION_RETRIES, DEATH_COUNT_HEADER, DEATH_REASON_HEADER, ORIGINAL_QUEUE_HEADER,
    RETRY_COUNT_HEADER,
};
use super::publisher::publish_with_retry;

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

/// Extracts message metadata from a JetStream message.
fn extract_message_metadata(msg: &Message) -> MessageMetadata {
    let retry_count = get_retry_count(&msg.headers);

    let delivery_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();

    let redelivered = msg.info().map(|info| info.delivered > 1).unwrap_or(false);

    let headers = extract_string_headers(&msg.headers);

    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        headers,
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

/// Dispatches message routing based on the handler's outcome.
async fn route_outcome(
    client: &NatsClient,
    msg: &Message,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
) {
    let result: Result<()> = match outcome {
        Outcome::Ack => {
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack message");
            }
            return;
        }
        Outcome::Retry => {
            let new_count = retry_count + 1;
            if new_count >= max_retries {
                // Exhausted retries — send to DLQ
                match publish_to_dlq(client, topology, msg, "max_retries_exceeded").await {
                    Ok(()) => {
                        if let Err(e) = msg.ack().await {
                            tracing::error!(error = %e, "failed to ack after DLQ publish");
                        }
                        return;
                    }
                    Err(e) => Err(e),
                }
            } else {
                let delay = if hold_queues.is_empty() {
                    Duration::from_secs(1)
                } else {
                    let idx = (retry_count as usize).min(hold_queues.len() - 1);
                    hold_queues[idx].delay()
                };

                // Ack first, then spawn delayed republish independently
                // so the semaphore permit is not held during sleep.
                if let Err(e) = msg.ack().await {
                    tracing::error!(error = %e, "failed to ack before retry republish");
                    return;
                }

                let client = client.clone();
                let subject = msg.subject.to_string();
                let headers = msg.headers.clone();
                let payload = msg.payload.clone();

                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;

                    let mut hdrs = headers.unwrap_or_default();
                    hdrs.insert(RETRY_COUNT_HEADER, new_count.to_string().as_str());

                    let original_id = hdrs
                        .get(NATS_MESSAGE_ID)
                        .map(|v| v.as_str().to_string())
                        .unwrap_or_default();
                    hdrs.insert(
                        NATS_MESSAGE_ID,
                        format!("{original_id}-r{new_count}").as_str(),
                    );

                    if let Err(e) = client
                        .jetstream()
                        .publish_with_headers(subject, hdrs, payload)
                        .await
                    {
                        tracing::error!(error = %e, "delayed retry republish send failed");
                    }
                });
                return;
            }
        }
        Outcome::Reject => match publish_to_dlq(client, topology, msg, "rejected").await {
            Ok(()) => {
                if let Err(e) = msg.ack().await {
                    tracing::error!(error = %e, "failed to ack after reject DLQ publish");
                }
                return;
            }
            Err(e) => Err(e),
        },
        Outcome::Defer => {
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
/// Returns `Outcome::Retry` on timeout.
async fn invoke_handler(
    fut: impl Future<Output = Outcome>,
    timeout: Option<Duration>,
    topic: &str,
    group: Option<&str>,
) -> Outcome {
    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let outcome = match timeout {
        Some(duration) => tokio::time::timeout(duration, fut)
            .await
            .unwrap_or_else(|_| {
                tracing::warn!("handler timed out after {duration:?}, retrying");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                Outcome::Retry
            }),
        None => fut.await,
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
fn map_get_stream_error(queue: &str, e: GetStreamError) -> ShoveError {
    match e.kind() {
        GetStreamErrorKind::Request => {
            ShoveError::Connection(format!("failed to get stream {queue}: {e}"))
        }
        _ => ShoveError::Topology(format!("failed to get stream {queue}: {e}")),
    }
}

// Reconnect loop
// ---------------------------------------------------------------------------

/// Runs `f` in a reconnect loop, retrying on transient errors until shutdown
/// or `max_retries` consecutive failures.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
    max_retries: u32,
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
                if attempts >= max_retries {
                    return Err(e);
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    error = %e,
                    label,
                    attempt = attempts,
                    max_retries,
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
        let hold_queues = topology.hold_queues();

        let max_message_size = options.max_message_size;
        let max_ack_pending = options.max_ack_pending.unwrap_or(prefetch_count as i64);

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

        run_with_reconnect(&shutdown, queue, CONNECTION_RETRIES, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let consumer_name = consumer_name.clone();
            let semaphore = semaphore.clone();
            let topic = topic.clone();
            let group = group.clone();
            async move {
                let stream = client
                    .jetstream()
                    .get_stream(queue)
                    .await
                    .map_err(|e| map_get_stream_error(queue, e))?;

                // `create_consumer` upserts the durable consumer — unlike
                // `get_or_create_consumer`, which returns the pre-existing
                // config verbatim and silently ignores the caller's config
                // (including `max_ack_pending`). That path caused N-way
                // parallel pullers to inherit whatever ack-budget the first
                // registrant set, bottlenecking the whole group.
                let pull_consumer = stream
                    .create_consumer(PullConsumerConfig {
                        durable_name: Some(consumer_name.clone()),
                        ack_policy: AckPolicy::Explicit,
                        max_ack_pending,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!(
                            "failed to create consumer {consumer_name}: {e}"
                        ))
                    })?;

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
                            let payload: T::Message = match serde_json::from_slice(&msg.payload) {
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

                            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                            tokio::spawn(async move {
                                let outcome = invoke_handler(
                                    async move {
                                        task_handler.handle(payload, metadata, task_ctx.as_ref()).await
                                    },
                                    handler_timeout,
                                    &task_topic,
                                    task_group.as_deref(),
                                )
                                .await;
                                let _ = outcome_tx.send(outcome);
                            });

                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = outcome_rx.await.unwrap_or_else(|_| {
                                    tracing::warn!(queue = topology.queue(), "handler task panicked, retrying message");
                                    Outcome::Retry
                                });

                                route_outcome(
                                    &task_client,
                                    &msg,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
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
        let topology = T::topology();
        let queue = topology.queue();
        let seq_config = topology
            .sequencing()
            .expect("run_fifo requires a sequenced topology");
        let routing_shards = seq_config.routing_shards();

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();

        let stream = self
            .client
            .jetstream()
            .get_stream(queue)
            .await
            .map_err(|e| map_get_stream_error(queue, e))?;

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        tracing::info!(
            queue,
            routing_shards,
            max_retries,
            "NATS FIFO consumer started"
        );

        let mut shard_tasks = Vec::with_capacity(routing_shards as usize);

        for shard in 0..routing_shards {
            let consumer_name = format!("{queue}-shard-{shard}");
            let filter_subject = format!("{queue}.shard.{shard}");

            let pull_consumer = stream
                .get_or_create_consumer(
                    &consumer_name,
                    PullConsumerConfig {
                        durable_name: Some(consumer_name.clone()),
                        filter_subject: filter_subject.clone(),
                        ack_policy: AckPolicy::Explicit,
                        max_ack_pending: 1,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| {
                    ShoveError::Connection(format!(
                        "failed to create shard consumer {consumer_name}: {e}"
                    ))
                })?;

            let shard_handler = handler.clone();
            let shard_ctx = ctx.clone();
            let shard_client = client.clone();
            let shard_shutdown = shutdown.clone();
            let shard_processing = processing.clone();
            let shard_topic = topic.clone();
            let shard_group = group.clone();

            let task = tokio::spawn(async move {
                let mut messages = match pull_consumer.messages().await {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            shard,
                            "failed to get message stream for shard"
                        );
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        _ = shard_shutdown.cancelled() => {
                            tracing::info!(shard, "shutdown signal received, stopping shard consumer");
                            return;
                        }
                        item = messages.next() => {
                            let msg = match item {
                                Some(Ok(msg)) => msg,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, shard, "shard consumer stream error");
                                    return;
                                }
                                None => {
                                    tracing::warn!(shard, "shard consumer stream closed");
                                    return;
                                }
                            };

                            metrics::record_message_size(
                                &shard_topic,
                                shard_group.as_deref(),
                                msg.payload.len(),
                            );

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
                            let payload: T::Message = match serde_json::from_slice(&msg.payload) {
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
                                tokio::spawn(async move {
                                    let o = invoke_handler(
                                        async move { h.handle(payload, metadata, c.as_ref()).await },
                                        handler_timeout,
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

                            route_outcome(
                                &shard_client,
                                &msg,
                                outcome,
                                topology,
                                retry_count,
                                max_retries,
                                hold_queues,
                            )
                            .await;
                        }
                    }
                }
            });

            shard_tasks.push(task);
        }

        // Wait for all shard tasks to complete
        for task in shard_tasks {
            let _ = task.await;
        }

        Ok(())
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

        tracing::info!(
            dlq,
            consumer = dlq_consumer_name,
            "NATS DLQ consumer started"
        );

        run_with_reconnect(&shutdown, dlq, CONNECTION_RETRIES, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let dlq_consumer_name = dlq_consumer_name.clone();
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
                            let payload: T::Message = match serde_json::from_slice(&msg.payload) {
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
