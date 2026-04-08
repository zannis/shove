use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::message::AckKind;
use async_nats::HeaderMap;
use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::ShoveError;

use super::client::NatsClient;

// ---------------------------------------------------------------------------
// Header constants
// ---------------------------------------------------------------------------

const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";
const DEATH_REASON_HEADER: &str = "Shove-Death-Reason";
const ORIGINAL_QUEUE_HEADER: &str = "Shove-Original-Queue";
const DEATH_COUNT_HEADER: &str = "Shove-Death-Count";

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
fn extract_message_metadata(msg: &async_nats::jetstream::Message) -> MessageMetadata {
    let retry_count = get_retry_count(&msg.headers);

    let delivery_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();

    let redelivered = msg
        .info()
        .map(|info| info.delivered > 1)
        .unwrap_or(false);

    let headers = extract_string_headers(&msg.headers);

    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        headers,
    }
}

/// Extracts dead message metadata from a JetStream message.
fn extract_dead_metadata(msg: &async_nats::jetstream::Message) -> DeadMessageMetadata {
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

/// Publishes a message to the DLQ stream with death headers.
async fn publish_to_dlq(
    client: &NatsClient,
    topology: &QueueTopology,
    msg: &async_nats::jetstream::Message,
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

    // Increment death count
    let current_death_count = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(DEATH_COUNT_HEADER))
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0);
    headers.insert(DEATH_COUNT_HEADER, (current_death_count + 1).to_string().as_str());

    // Generate a new message ID for the DLQ publish to avoid dedup rejection
    let original_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    headers.insert(NATS_MESSAGE_ID, format!("{original_id}-dlq").as_str());

    let mut backoff = crate::retry::Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
    let max_attempts = 3u32;

    for attempt in 1..=max_attempts {
        match client
            .jetstream()
            .publish_with_headers(dlq_subject.clone(), headers.clone(), msg.payload.clone())
            .await
        {
            Ok(ack_future) => match ack_future.await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == max_attempts {
                        return Err(ShoveError::Connection(format!(
                            "DLQ publish ack failed after {max_attempts} attempts: {e}"
                        )));
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                    tracing::warn!(attempt, error = %e, "DLQ publish ack failed, retrying");
                    tokio::time::sleep(delay).await;
                }
            },
            Err(e) => {
                if attempt == max_attempts {
                    return Err(ShoveError::Connection(format!(
                        "DLQ publish failed after {max_attempts} attempts: {e}"
                    )));
                }
                let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                tracing::warn!(attempt, error = %e, "DLQ publish failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }

    unreachable!()
}

/// Dispatches message routing based on the handler's outcome.
async fn route_outcome(
    client: &NatsClient,
    msg: &async_nats::jetstream::Message,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[crate::topology::HoldQueue],
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
        Outcome::Reject => {
            match publish_to_dlq(client, topology, msg, "rejected").await {
                Ok(()) => {
                    if let Err(e) = msg.ack().await {
                        tracing::error!(error = %e, "failed to ack after reject DLQ publish");
                    }
                    return;
                }
                Err(e) => Err(e),
            }
        }
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

/// Wraps handler.handle() with optional timeout, returns Outcome::Retry on timeout.
async fn invoke_handler<T: Topic>(
    handler: &(impl MessageHandler<T> + ?Sized),
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
) -> Outcome {
    match timeout {
        Some(duration) => match tokio::time::timeout(duration, handler.handle(message, metadata)).await {
            Ok(outcome) => outcome,
            Err(_) => {
                tracing::warn!("handler timed out after {duration:?}, retrying");
                Outcome::Retry
            }
        },
        None => handler.handle(message, metadata).await,
    }
}

// ---------------------------------------------------------------------------
// Reconnect loop
// ---------------------------------------------------------------------------

/// Runs `f` in a reconnect loop, retrying on transient errors until shutdown.
/// Matches the pattern used by RabbitMqConsumer.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff = crate::retry::Backoff::default();
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    error = %e,
                    label,
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

pub struct NatsConsumer {
    client: NatsClient,
}

impl NatsConsumer {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }
}

impl Consumer for NatsConsumer {
    async fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> Result<()> {
        let topology = T::topology();
        let queue = topology.queue();
        let consumer_name = format!("{queue}-consumer");

        // Extract needed values from options before moving into the reconnect closure.
        // ConsumerOptions does not implement Clone in all configurations.
        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let hold_queues = topology.hold_queues();

        // Wrap handler in Arc for sharing across tasks
        let handler = Arc::new(handler);
        let client = self.client.clone();

        tracing::info!(
            queue,
            consumer = consumer_name,
            prefetch_count,
            max_retries,
            "NATS consumer started"
        );

        run_with_reconnect(&shutdown, queue, || {
            let handler = handler.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let consumer_name = consumer_name.clone();
            async move {
                // Get the stream
                let stream = client
                    .jetstream()
                    .get_stream(queue)
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!("failed to get stream {queue}: {e}"))
                    })?;

                // Create durable pull consumer
                let pull_consumer = stream
                    .get_or_create_consumer(
                        &consumer_name,
                        PullConsumerConfig {
                            durable_name: Some(consumer_name.clone()),
                            ack_policy: AckPolicy::Explicit,
                            max_ack_pending: prefetch_count as i64,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!(
                            "failed to create consumer {consumer_name}: {e}"
                        ))
                    })?;

                // Get continuous message stream
                let mut messages = pull_consumer.messages().await.map_err(|e| {
                    ShoveError::Connection(format!("failed to get message stream: {e}"))
                })?;

                let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, stopping consumer");
                            return Ok(());
                        }
                        item = messages.next() => {
                            let msg = match item {
                                Some(Ok(msg)) => msg,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, queue, "consumer stream error");
                                    return Err(ShoveError::Connection(
                                        format!("consumer stream error on {queue}: {e}"),
                                    ));
                                }
                                None => {
                                    tracing::warn!(queue, "consumer stream closed");
                                    return Err(ShoveError::Connection(
                                        format!("consumer stream closed for {queue}"),
                                    ));
                                }
                            };

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match serde_json::from_slice(&msg.payload) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        queue,
                                        "failed to deserialize message, sending to DLQ"
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

                            // Acquire semaphore permit to limit concurrency
                            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;

                            let task_handler = handler.clone();
                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;

                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = invoke_handler(
                                    task_handler.as_ref(),
                                    payload,
                                    metadata,
                                    handler_timeout,
                                )
                                .await;

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

    async fn run_fifo<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> Result<()> {
        let topology = T::topology();
        let queue = topology.queue();
        let seq_config = topology
            .sequencing()
            .expect("run_fifo requires a sequenced topology");
        let routing_shards = seq_config.routing_shards();

        // Extract needed values from options before spawning tasks.
        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let hold_queues = topology.hold_queues();

        // Get the stream
        let stream = self
            .client
            .jetstream()
            .get_stream(queue)
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to get stream {queue}: {e}")))?;

        // Wrap handler in Arc for sharing across shard tasks
        let handler = Arc::new(handler);
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
            let shard_client = client.clone();
            let shard_shutdown = shutdown.clone();
            let shard_processing = processing.clone();

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

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match serde_json::from_slice(&msg.payload) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        shard,
                                        "failed to deserialize message, sending to DLQ"
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

                            let outcome = invoke_handler(
                                shard_handler.as_ref(),
                                payload,
                                metadata,
                                handler_timeout,
                            )
                            .await;

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

    async fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> Result<()> {
        let topology = T::topology();
        let dlq = topology
            .dlq()
            .ok_or_else(|| ShoveError::Topology("run_dlq requires a DLQ to be configured".into()))?;

        let dlq_consumer_name = format!("{dlq}-consumer");
        let shutdown = self.client.shutdown_token();
        let handler = Arc::new(handler);
        let client = self.client.clone();

        tracing::info!(
            dlq,
            consumer = dlq_consumer_name,
            "NATS DLQ consumer started"
        );

        run_with_reconnect(&shutdown, dlq, || {
            let handler = handler.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let dlq_consumer_name = dlq_consumer_name.clone();
            async move {
                // Get the DLQ stream
                let stream = client
                    .jetstream()
                    .get_stream(dlq)
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!("failed to get DLQ stream {dlq}: {e}"))
                    })?;

                // Create durable pull consumer for the DLQ
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
                                    return Err(ShoveError::Connection(
                                        format!("DLQ consumer stream error on {dlq}: {e}"),
                                    ));
                                }
                                None => {
                                    tracing::warn!(dlq, "DLQ consumer stream closed");
                                    return Err(ShoveError::Connection(
                                        format!("DLQ consumer stream closed for {dlq}"),
                                    ));
                                }
                            };

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

                            handler.handle_dead(payload, metadata).await;

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