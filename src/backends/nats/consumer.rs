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

    client
        .jetstream()
        .publish_with_headers(dlq_subject, headers, msg.payload.clone())
        .await
        .map_err(|e| ShoveError::Connection(format!("DLQ publish failed: {e}")))?
        .await
        .map_err(|e| ShoveError::Connection(format!("DLQ publish ack failed: {e}")))?;

    Ok(())
}

/// Sleeps for the delay duration, then publishes a new message to the same
/// subject with an incremented retry count and a new message ID.
async fn republish_with_retry(
    client: &NatsClient,
    subject: String,
    msg: &async_nats::jetstream::Message,
    new_retry_count: u32,
    delay: Duration,
) -> Result<()> {
    tokio::time::sleep(delay).await;

    let mut headers = msg.headers.clone().unwrap_or_default();
    headers.insert(RETRY_COUNT_HEADER, new_retry_count.to_string().as_str());

    // Generate a new Nats-Msg-Id to avoid dedup rejection
    let original_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    headers.insert(
        NATS_MESSAGE_ID,
        format!("{original_id}-r{new_retry_count}").as_str(),
    );

    client
        .jetstream()
        .publish_with_headers(subject, headers, msg.payload.clone())
        .await
        .map_err(|e| ShoveError::Connection(format!("retry republish failed: {e}")))?
        .await
        .map_err(|e| ShoveError::Connection(format!("retry republish ack failed: {e}")))?;

    Ok(())
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
                match republish_with_retry(
                    client,
                    topology.queue().to_string(),
                    msg,
                    new_count,
                    delay,
                )
                .await
                {
                    Ok(()) => {
                        if let Err(e) = msg.ack().await {
                            tracing::error!(error = %e, "failed to ack after retry republish");
                        }
                        return;
                    }
                    Err(e) => Err(e),
                }
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

        // Get the stream
        let stream = self
            .client
            .jetstream()
            .get_stream(queue)
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to get stream {queue}: {e}")))?;

        // Create durable pull consumer
        let pull_consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConsumerConfig {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: AckPolicy::Explicit,
                    max_ack_pending: options.prefetch_count as i64,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                ShoveError::Connection(format!("failed to create consumer {consumer_name}: {e}"))
            })?;

        // Get continuous message stream
        let mut messages = pull_consumer
            .messages()
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to get message stream: {e}")))?;

        // Extract needed values from options before moving into spawned tasks.
        // ConsumerOptions does not implement Clone in all configurations.
        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let hold_queues = topology.hold_queues();

        // Wrap handler in Arc for sharing across tasks
        let handler = Arc::new(handler);
        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));
        let client = self.client.clone();

        tracing::info!(
            queue,
            consumer = consumer_name,
            prefetch_count,
            max_retries,
            "NATS consumer started"
        );

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

                        task_processing.store(false, Ordering::Release);
                        drop(permit);
                    });
                }
            }
        }
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

                            shard_processing.store(false, Ordering::Release);
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

        // Get the DLQ stream
        let stream = self
            .client
            .jetstream()
            .get_stream(dlq)
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to get DLQ stream {dlq}: {e}")))?;

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

        let mut messages = pull_consumer
            .messages()
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to get DLQ message stream: {e}")))?;

        let shutdown = self.client.shutdown_token();

        tracing::info!(
            dlq,
            consumer = dlq_consumer_name,
            "NATS DLQ consumer started"
        );

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
                            return Ok(());
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
}