use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer as RdkafkaConsumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::ShoveError;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;

use super::client::KafkaClient;
use super::constants::{
    CONNECTION_RETRIES, DEATH_COUNT_HEADER, DEATH_REASON_HEADER, MESSAGE_ID_HEADER,
    ORIGINAL_QUEUE_HEADER, RETRY_COUNT_HEADER,
};
use super::publisher::publish_with_retry;

// ---------------------------------------------------------------------------
// Offset tracking for concurrent consumption
// ---------------------------------------------------------------------------

struct PartitionTracker {
    /// Next offset to commit (exclusive — Kafka convention).
    next_to_commit: i64,
    /// Offsets that have been processed but not yet committable
    /// (because earlier offsets are still in-flight).
    completed: BTreeSet<i64>,
}

impl PartitionTracker {
    fn new(first_offset: i64) -> Self {
        Self {
            next_to_commit: first_offset,
            completed: BTreeSet::new(),
        }
    }

    fn mark_complete(&mut self, offset: i64) {
        self.completed.insert(offset);
    }

    /// Returns the new commit offset if progress was made, or None.
    fn drain_committable(&mut self) -> Option<i64> {
        let mut next = self.next_to_commit;
        while self.completed.remove(&next) {
            next += 1;
        }
        if next > self.next_to_commit {
            self.next_to_commit = next;
            Some(next)
        } else {
            None
        }
    }
}

struct OffsetTracker {
    topic: String,
    partitions: HashMap<i32, PartitionTracker>,
}

impl OffsetTracker {
    fn new(topic: String) -> Self {
        Self {
            topic,
            partitions: HashMap::new(),
        }
    }

    fn track_received(&mut self, partition: i32, offset: i64) {
        self.partitions
            .entry(partition)
            .or_insert_with(|| PartitionTracker::new(offset));
    }

    fn mark_complete(&mut self, partition: i32, offset: i64) {
        if let Some(tracker) = self.partitions.get_mut(&partition) {
            tracker.mark_complete(offset);
        }
    }

    fn drain_committable(&mut self) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for (&partition, tracker) in &mut self.partitions {
            if let Some(commit_offset) = tracker.drain_committable() {
                tpl.add_partition_offset(&self.topic, partition, Offset::Offset(commit_offset))
                    .ok();
            }
        }
        tpl
    }
}

// ---------------------------------------------------------------------------
// Metadata extraction functions
// ---------------------------------------------------------------------------

fn extract_string_headers(msg: &BorrowedMessage<'_>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(headers) = msg.headers() {
        for idx in 0..headers.count() {
            let header = headers.get(idx);
            if let Some(value) = header.value
                && let Ok(s) = std::str::from_utf8(value)
            {
                out.insert(header.key.to_string(), s.to_string());
            }
        }
    }
    out
}

fn get_retry_count(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

fn build_message_metadata(headers: &HashMap<String, String>, redelivered: bool) -> MessageMetadata {
    let retry_count = get_retry_count(headers);
    let delivery_id = headers.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        headers: headers.clone(),
    }
}

fn build_dead_metadata(headers: &HashMap<String, String>) -> DeadMessageMetadata {
    let message = build_message_metadata(headers, false);
    let reason = headers.get(DEATH_REASON_HEADER).cloned();
    let original_queue = headers.get(ORIGINAL_QUEUE_HEADER).cloned();
    let death_count = headers
        .get(DEATH_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    DeadMessageMetadata {
        message,
        reason,
        original_queue,
        death_count,
    }
}

// ---------------------------------------------------------------------------
// Header building helpers
// ---------------------------------------------------------------------------

fn headers_with_retry_count(
    original: &HashMap<String, String>,
    retry_count: u32,
    message_id_suffix: &str,
) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new();
    for (k, v) in original {
        if k == RETRY_COUNT_HEADER || k == MESSAGE_ID_HEADER {
            continue;
        }
        headers = headers.insert(Header {
            key: k.as_str(),
            value: Some(v.as_bytes()),
        });
    }
    headers = headers.insert(Header {
        key: RETRY_COUNT_HEADER,
        value: Some(retry_count.to_string().as_bytes()),
    });

    let original_id = original.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    let new_id = format!("{original_id}{message_id_suffix}");
    headers = headers.insert(Header {
        key: MESSAGE_ID_HEADER,
        value: Some(new_id.as_bytes()),
    });
    headers
}

fn headers_for_dlq(
    original: &HashMap<String, String>,
    reason: &str,
    original_queue: &str,
) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new();
    for (k, v) in original {
        if k == DEATH_REASON_HEADER
            || k == ORIGINAL_QUEUE_HEADER
            || k == DEATH_COUNT_HEADER
            || k == MESSAGE_ID_HEADER
        {
            continue;
        }
        headers = headers.insert(Header {
            key: k.as_str(),
            value: Some(v.as_bytes()),
        });
    }
    headers = headers.insert(Header {
        key: DEATH_REASON_HEADER,
        value: Some(reason.as_bytes()),
    });
    headers = headers.insert(Header {
        key: ORIGINAL_QUEUE_HEADER,
        value: Some(original_queue.as_bytes()),
    });

    let current_death_count = original
        .get(DEATH_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    headers = headers.insert(Header {
        key: DEATH_COUNT_HEADER,
        value: Some((current_death_count + 1).to_string().as_bytes()),
    });

    let original_id = original.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    headers = headers.insert(Header {
        key: MESSAGE_ID_HEADER,
        value: Some(format!("{original_id}-dlq").as_bytes()),
    });
    headers
}

// ---------------------------------------------------------------------------
// Outcome routing functions
// ---------------------------------------------------------------------------

fn adjust_outcome_for_fifo(outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Defer => {
            tracing::warn!("Defer is not supported on sequenced consumers — treating as Retry");
            Outcome::Retry
        }
        other => other,
    }
}

async fn publish_to_dlq(
    client: &KafkaClient,
    topology: &QueueTopology,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &HashMap<String, String>,
    reason: &str,
) -> Result<()> {
    let dlq_topic = match topology.dlq() {
        Some(dlq) => dlq.to_string(),
        None => {
            tracing::warn!(
                queue = topology.queue(),
                "no DLQ configured, message will be discarded"
            );
            return Ok(());
        }
    };

    let dlq_headers = headers_for_dlq(headers, reason, topology.queue());
    publish_with_retry(
        client.producer(),
        &dlq_topic,
        key,
        dlq_headers,
        payload,
        3,
        "DLQ publish",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    client: &KafkaClient,
    topic: &str,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &HashMap<String, String>,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[crate::topology::HoldQueue],
) -> bool {
    match outcome {
        Outcome::Ack => true,
        Outcome::Retry => {
            let new_count = retry_count + 1;
            if new_count >= max_retries {
                match publish_to_dlq(
                    client,
                    topology,
                    payload,
                    key,
                    headers,
                    "max_retries_exceeded",
                )
                .await
                {
                    Ok(()) => return true,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to publish to DLQ after exhausting retries");
                        return false;
                    }
                }
            }

            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                let idx = (retry_count as usize).min(hold_queues.len() - 1);
                hold_queues[idx].delay()
            };

            let client = client.clone();
            let topic = topic.to_string();
            let payload = payload.to_vec();
            let key = key.map(|k| k.to_vec());
            let retry_headers =
                headers_with_retry_count(headers, new_count, &format!("-r{new_count}"));

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Err(e) = publish_with_retry(
                    client.producer(),
                    &topic,
                    key.as_deref(),
                    retry_headers,
                    &payload,
                    3,
                    "retry republish",
                )
                .await
                {
                    tracing::error!(error = %e, "delayed retry republish failed");
                }
            });
            true
        }
        Outcome::Reject => {
            match publish_to_dlq(client, topology, payload, key, headers, "rejected").await {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(error = %e, "failed to publish rejected message to DLQ");
                    false
                }
            }
        }
        Outcome::Defer => {
            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                hold_queues[0].delay()
            };

            let client = client.clone();
            let topic = topic.to_string();
            let payload = payload.to_vec();
            let key = key.map(|k| k.to_vec());
            // Defer does NOT increment retry count
            let defer_headers = headers_with_retry_count(
                headers,
                retry_count,
                &format!("-d{}", uuid::Uuid::new_v4()),
            );

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Err(e) = publish_with_retry(
                    client.producer(),
                    &topic,
                    key.as_deref(),
                    defer_headers,
                    &payload,
                    3,
                    "defer republish",
                )
                .await
                {
                    tracing::error!(error = %e, "deferred republish failed");
                }
            });
            true
        }
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

async fn invoke_handler<T: Topic>(
    handler: &(impl MessageHandler<T> + ?Sized),
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
) -> Outcome {
    match timeout {
        Some(duration) => {
            match tokio::time::timeout(duration, handler.handle(message, metadata)).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    tracing::warn!("handler timed out after {duration:?}, retrying");
                    Outcome::Retry
                }
            }
        }
        None => handler.handle(message, metadata).await,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps a rdkafka `KafkaError` to the appropriate `ShoveError` variant.
/// Permanent errors (bad config, fatal consumption, cancelled) become
/// `Topology`; transient errors (broker down, network) become `Connection`.
fn map_kafka_error(context: &str, e: KafkaError) -> ShoveError {
    let is_permanent = matches!(
        &e,
        KafkaError::ClientConfig(..)
            | KafkaError::ClientCreation(_)
            | KafkaError::MessageConsumptionFatal(_)
            | KafkaError::Canceled
            | KafkaError::Nul(_)
    );
    if is_permanent {
        ShoveError::Topology(format!("{context}: {e}"))
    } else {
        ShoveError::Connection(format!("{context}: {e}"))
    }
}

// Consumer helper
// ---------------------------------------------------------------------------

fn create_stream_consumer(brokers: &str, group_id: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("max.poll.interval.ms", "300000")
        .create()
        .map_err(|e| map_kafka_error("failed to create consumer", e))?;
    Ok(consumer)
}

// ---------------------------------------------------------------------------
// Reconnect loop
// ---------------------------------------------------------------------------

async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
    max_retries: u32,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff = crate::retry::Backoff::default();
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
// KafkaConsumer
// ---------------------------------------------------------------------------

pub struct KafkaConsumer {
    client: KafkaClient,
}

impl KafkaConsumer {
    pub fn new(client: KafkaClient) -> Self {
        Self { client }
    }
}

impl Consumer for KafkaConsumer {
    async fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> Result<()> {
        let topology = T::topology();
        let queue = topology.queue();
        let group_id = super::constants::consumer_group_id(queue);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let client = self.client.clone();

        tracing::info!(
            queue,
            group_id,
            prefetch_count,
            max_retries,
            "Kafka consumer started"
        );

        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));

        run_with_reconnect(&shutdown, queue, CONNECTION_RETRIES, || {
            let handler = handler.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let group_id = group_id.clone();
            let semaphore = semaphore.clone();
            async move {
                let consumer = create_stream_consumer(client.brokers(), &group_id)?;
                consumer
                    .subscribe(&[queue])
                    .map_err(|e| map_kafka_error("failed to subscribe", e))?;

                let queue_owned = queue.to_string();
                let tracker = Arc::new(Mutex::new(OffsetTracker::new(queue_owned.clone())));
                let consumer = Arc::new(consumer);
                let (completion_tx, mut completion_rx) = mpsc::unbounded_channel::<(i32, i64)>();

                loop {
                    // Drain completed offsets and commit
                    {
                        let mut t = tracker.lock().await;
                        while let Ok((partition, offset)) = completion_rx.try_recv() {
                            t.mark_complete(partition, offset);
                        }
                        let tpl = t.drain_committable();
                        if tpl.count() > 0 {
                            consumer.commit(&tpl, CommitMode::Async).map_err(|e| {
                                map_kafka_error("commit failed", e)
                            })?;
                        }
                    }

                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            // Final commit
                            {
                                let mut t = tracker.lock().await;
                                while let Ok((partition, offset)) = completion_rx.try_recv() {
                                    t.mark_complete(partition, offset);
                                }
                                let tpl = t.drain_committable();
                                if tpl.count() > 0 {
                                    consumer.commit(&tpl, CommitMode::Async).ok();
                                }
                            }
                            return Ok(());
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("consumer recv error on {queue}"),
                                        e,
                                    ));
                                }
                            };

                            let payload_bytes = msg.payload().unwrap_or_default().to_vec();
                            let headers = extract_string_headers(&msg);
                            let partition = msg.partition();
                            let offset = msg.offset();
                            let key = msg.key().map(|k| k.to_vec());

                            {
                                tracker.lock().await.track_received(partition, offset);
                            }

                            // Reject oversized messages before deserialization
                            if let Some(max) = max_message_size {
                                if payload_bytes.len() > max {
                                    tracing::warn!(
                                        bytes = payload_bytes.len(),
                                        max,
                                        queue,
                                        "rejecting oversized message to DLQ"
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        &payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &format!("message size {} exceeds max_message_size {max}", payload_bytes.len()),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish oversized message to DLQ"
                                        );
                                    }
                                    completion_tx.send((partition, offset)).ok();
                                    continue;
                                }
                            }

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match serde_json::from_slice(&payload_bytes) {
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
                                        &payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &format!("deserialization_error: {e}"),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish bad message to DLQ"
                                        );
                                    }
                                    completion_tx.send((partition, offset)).ok();
                                    continue;
                                }
                            };

                            let metadata = build_message_metadata(&headers, false);
                            let retry_count = metadata.retry_count;

                            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;

                            let task_handler = handler.clone();
                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;
                            let task_tx = completion_tx.clone();
                            let task_topic = queue_owned.clone();

                            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                            tokio::spawn(async move {
                                let outcome = invoke_handler(
                                    task_handler.as_ref(),
                                    payload,
                                    metadata,
                                    handler_timeout,
                                )
                                .await;
                                let _ = outcome_tx.send(outcome);
                            });

                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = outcome_rx.await.unwrap_or_else(|_| {
                                    tracing::warn!(queue = task_topic.as_str(), "handler task panicked, retrying message");
                                    Outcome::Retry
                                });

                                route_outcome(
                                    &task_client,
                                    &task_topic,
                                    &payload_bytes,
                                    key.as_deref(),
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                )
                                .await;

                                task_tx.send((partition, offset)).ok();
                                drop(permit);

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
        let _seq_config = topology
            .sequencing()
            .expect("run_fifo requires a sequenced topology");

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let client = self.client.clone();

        // Kafka naturally provides per-partition ordering. A single consumer
        // processing one message at a time guarantees FIFO per key (all
        // messages for the same key land in the same partition).
        let group_id = format!("{queue}-fifo");

        tracing::info!(queue, group_id, max_retries, "Kafka FIFO consumer started");

        run_with_reconnect(&shutdown, queue, CONNECTION_RETRIES, || {
            let handler = handler.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let processing = processing.clone();
            let group_id = group_id.clone();
            async move {
                let consumer = create_stream_consumer(client.brokers(), &group_id)?;
                consumer
                    .subscribe(&[queue])
                    .map_err(|e| {
                        map_kafka_error("failed to subscribe", e)
                    })?;

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, stopping FIFO consumer");
                            return Ok(());
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "FIFO consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("FIFO consumer recv error on {queue}"),
                                        e,
                                    ));
                                }
                            };

                            let payload_bytes = msg.payload().unwrap_or_default().to_vec();
                            let headers = extract_string_headers(&msg);
                            let key = msg.key().map(|k| k.to_vec());

                            // Reject oversized messages before deserialization
                            if let Some(max) = max_message_size {
                                if payload_bytes.len() > max {
                                    tracing::warn!(
                                        bytes = payload_bytes.len(),
                                        max,
                                        queue,
                                        "rejecting oversized FIFO message to DLQ"
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        &payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &format!("message size {} exceeds max_message_size {max}", payload_bytes.len()),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish oversized message to DLQ"
                                        );
                                    }
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }
                            }

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match serde_json::from_slice(&payload_bytes) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        queue,
                                        "failed to deserialize FIFO message, sending to DLQ"
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        &payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &format!("deserialization_error: {e}"),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish bad message to DLQ"
                                        );
                                    }
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }
                            };

                            let metadata = build_message_metadata(&headers, false);
                            let retry_count = metadata.retry_count;

                            processing.store(true, Ordering::Release);

                            let outcome = {
                                let (tx, rx) = tokio::sync::oneshot::channel();
                                let h = handler.clone();
                                tokio::spawn(async move {
                                    let o = invoke_handler(h.as_ref(), payload, metadata, handler_timeout).await;
                                    let _ = tx.send(o);
                                });
                                rx.await.unwrap_or_else(|_| {
                                    tracing::warn!(queue, "handler task panicked, retrying message");
                                    Outcome::Retry
                                })
                            };
                            let outcome = adjust_outcome_for_fifo(outcome);

                            route_outcome(
                                &client,
                                queue,
                                &payload_bytes,
                                key.as_deref(),
                                &headers,
                                outcome,
                                topology,
                                retry_count,
                                max_retries,
                                hold_queues,
                            )
                            .await;

                            consumer.commit_message(&msg, CommitMode::Async).ok();
                            processing.store(false, Ordering::Release);
                        }
                    }
                }
            }
        })
        .await
    }

    async fn run_dlq<T: Topic>(&self, handler: impl MessageHandler<T>) -> Result<()> {
        let topology = T::topology();
        let dlq = topology.dlq().ok_or_else(|| {
            ShoveError::Topology("run_dlq requires a DLQ to be configured".into())
        })?;

        let dlq_group_id = super::constants::dlq_consumer_group_id(dlq);
        let shutdown = self.client.shutdown_token();
        let handler = Arc::new(handler);
        let client = self.client.clone();

        tracing::info!(dlq, group_id = dlq_group_id, "Kafka DLQ consumer started");

        run_with_reconnect(&shutdown, dlq, CONNECTION_RETRIES, || {
            let handler = handler.clone();
            let _client = client.clone();
            let shutdown = shutdown.clone();
            let dlq_group_id = dlq_group_id.clone();
            async move {
                let consumer = create_stream_consumer(_client.brokers(), &dlq_group_id)?;
                consumer
                    .subscribe(&[dlq])
                    .map_err(|e| map_kafka_error("failed to subscribe to DLQ", e))?;

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(dlq, "shutdown signal received, stopping DLQ consumer");
                            return Ok(());
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, dlq, "DLQ consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("DLQ consumer recv error on {dlq}"),
                                        e,
                                    ));
                                }
                            };

                            let payload_bytes = msg.payload().unwrap_or_default().to_vec();
                            let headers = extract_string_headers(&msg);

                            // Discard oversized DLQ messages
                            if payload_bytes.len() > crate::consumer::DEFAULT_MAX_MESSAGE_SIZE {
                                tracing::warn!(
                                    bytes = payload_bytes.len(),
                                    max = crate::consumer::DEFAULT_MAX_MESSAGE_SIZE,
                                    dlq,
                                    "oversized DLQ message — discarding"
                                );
                                consumer.commit_message(&msg, CommitMode::Async).ok();
                                continue;
                            }

                            // Deserialize payload; on failure, log and ack anyway
                            let payload: T::Message = match serde_json::from_slice(&payload_bytes) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        dlq,
                                        "failed to deserialize DLQ message, acking anyway"
                                    );
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }
                            };

                            let metadata = build_dead_metadata(&headers);
                            handler.handle_dead(payload, metadata).await;

                            if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                                tracing::error!(error = %e, dlq, "failed to commit DLQ message");
                            }
                        }
                    }
                }
            }
        })
        .await
    }
}
