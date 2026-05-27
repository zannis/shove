use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer as RdkafkaConsumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{HoldQueue, Kafka, ShoveError};

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;

use super::client::KafkaClient;
use super::constants::{
    DEATH_COUNT_HEADER, DEATH_REASON_HEADER, FETCH_MIN_BYTES, FETCH_WAIT_MAX_MS,
    MAX_POLL_INTERVAL_MS, MAX_PUBLISH_ATTEMPTS, MESSAGE_ID_HEADER, ORIGINAL_QUEUE_HEADER,
    RETRY_COUNT_HEADER, SESSION_TIMEOUT_MS,
};
use super::consumer_group::KafkaAutoOffsetReset;

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

    /// Returns the partitions that have new contiguous-from-start offsets to
    /// commit, or `None` if nothing has advanced since the last call.
    ///
    /// perf-K-16: the previous impl allocated a fresh `TopicPartitionList` on
    /// every receive-loop iteration even when no partition had progress to
    /// commit (the common case). Returning `Option` skips the C-heap (librdkafka
    /// FFI) allocation when there's nothing to do.
    fn drain_committable(&mut self) -> Option<TopicPartitionList> {
        let mut tpl: Option<TopicPartitionList> = None;
        for (&partition, tracker) in &mut self.partitions {
            if let Some(commit_offset) = tracker.drain_committable() {
                tpl.get_or_insert_with(TopicPartitionList::new)
                    .add_partition_offset(&self.topic, partition, Offset::Offset(commit_offset))
                    .ok();
            }
        }
        tpl
    }
}

// ---------------------------------------------------------------------------
// Metadata extraction functions
// ---------------------------------------------------------------------------

fn extract_string_headers(msg: &BorrowedMessage<'_>) -> Arc<HashMap<String, String>> {
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
    Arc::new(out)
}

fn get_retry_count(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

fn build_message_metadata(
    headers: &Arc<HashMap<String, String>>,
    redelivered: bool,
) -> MessageMetadata {
    let retry_count = get_retry_count(headers);
    let delivery_id = headers.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        headers: Arc::clone(headers),
    }
}

fn build_dead_metadata(headers: &Arc<HashMap<String, String>>) -> DeadMessageMetadata {
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
    // perf-K-8: original.len() bounds the carried-over headers; +2 for the
    // RETRY_COUNT_HEADER and MESSAGE_ID_HEADER we always re-insert.
    let mut headers = OwnedHeaders::new_with_capacity(original.len() + 2);
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
    // perf-K-8: original.len() bounds the carried-over headers; +4 for the
    // DEATH_REASON / ORIGINAL_QUEUE / DEATH_COUNT / MESSAGE_ID we re-insert.
    let mut headers = OwnedHeaders::new_with_capacity(original.len() + 4);
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
    client
        .publish_with_retry(
            &dlq_topic,
            key,
            dlq_headers,
            payload,
            MAX_PUBLISH_ATTEMPTS,
            "DLQ publish",
        )
        .await
}

#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    client: &KafkaClient,
    topic: &str,
    // Optional consumer-group label propagated to `metrics::record_failed`
    // on DLQ-terminal outcomes (max_retries_exceeded, Rejected). Matches the
    // shape `invoke_handler` already uses.
    group: Option<&str>,
    payload: &[u8],
    // perf-K-9: take key as Option<Bytes> by value. Each match arm uses it
    // once, so we move it instead of cloning. The receive loop's Bytes
    // refcount machinery makes any further sharing a refcount bump.
    key: Option<Bytes>,
    headers: &HashMap<String, String>,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
    // sec-K-8: retry/defer arms move this permit into the delayed-republish
    // spawn so the prefetch semaphore stays bounded across delayed work.
    // None on the FIFO path (no semaphore in play there) and on the
    // outer-task's Ack/Reject arms (permit drops at end of scope).
    retry_permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> bool {
    match outcome {
        Outcome::Ack => true,
        Outcome::Retry => {
            let new_count = retry_count + 1;
            if new_count >= max_retries {
                // Emit before the DLQ publish so the metric fires regardless
                // of DLQ outcome — silent loss on DLQ failure is what the
                // counter has to surface to alerting.
                metrics::record_failed(topic, group, metrics::FailReason::MaxRetriesExceeded);
                return match publish_to_dlq(
                    client,
                    topology,
                    payload,
                    key.as_deref(),
                    headers,
                    "max_retries_exceeded",
                )
                .await
                {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to publish to DLQ after exhausting retries");
                        false
                    }
                };
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
            let retry_headers =
                headers_with_retry_count(headers, new_count, &format!("-r{new_count}"));

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Err(e) = client
                    .publish_with_retry(
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
                // sec-K-8: hold the prefetch permit until the delayed retry
                // republish finishes so the inflight-task count stays
                // bounded by the prefetch limit.
                drop(retry_permit);
            });
            true
        }
        Outcome::Reject => {
            // Emit before the DLQ publish — see the symmetric note in the
            // max_retries_exceeded arm above.
            metrics::record_failed(topic, group, metrics::FailReason::Rejected);
            match publish_to_dlq(
                client,
                topology,
                payload,
                key.as_deref(),
                headers,
                "rejected",
            )
            .await
            {
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
            // Defer does NOT increment retry count
            let defer_headers = headers_with_retry_count(
                headers,
                retry_count,
                &format!("-d{}", uuid::Uuid::new_v4()),
            );

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Err(e) = client
                    .publish_with_retry(
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
                // sec-K-8: same permit-lifetime contract as Retry.
                drop(retry_permit);
            });
            true
        }
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

/// Invoke the handler future with an optional timeout, emitting inflight /
/// consumed / duration metrics. Returns `Outcome::Retry` on timeout or panic.
///
/// Awaits the handler future with timeout + panic isolation, recording
/// per-outcome metrics. A panic inside the user's handler is caught via
/// `AssertUnwindSafe(...).catch_unwind()` and surfaced as `Outcome::Retry`.
///
/// perf-K-7: this previously spawned an inner `tokio::spawn` to catch panics
/// via JoinError. The spawn allocated a task struct + scheduler enqueue per
/// message — combined with the wrapper spawn (now removed) and outer outcome
/// spawn, that was 3 spawns per message. catch_unwind achieves the same
/// panic-isolation outcome without the task alloc.
async fn invoke_handler<F>(
    fut: F,
    timeout: Option<Duration>,
    topic: &str,
    group: Option<&str>,
) -> Outcome
where
    F: std::future::Future<Output = Outcome> + Send,
{
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let safe_fut = AssertUnwindSafe(fut).catch_unwind();
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, safe_fut).await {
            Ok(Ok(o)) => o,
            Ok(Err(_panic)) => {
                tracing::warn!("handler panicked, retrying message");
                Outcome::Retry
            }
            Err(_) => {
                tracing::warn!("handler timed out after {duration:?}, retrying");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                Outcome::Retry
            }
        },
        None => match safe_fut.await {
            Ok(o) => o,
            Err(_panic) => {
                tracing::warn!("handler panicked, retrying message");
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

// ---------------------------------------------------------------------------
// KafkaStreamConsumer — context-agnostic wrapper
// ---------------------------------------------------------------------------

pub(super) enum KafkaStreamConsumer {
    Default(StreamConsumer),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(StreamConsumer<MskIamContext>),
}

impl KafkaStreamConsumer {
    pub(super) fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.subscribe(topics),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.subscribe(topics),
        }
    }

    pub(super) async fn recv(&self) -> KafkaResult<BorrowedMessage<'_>> {
        match self {
            Self::Default(c) => c.recv().await,
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.recv().await,
        }
    }

    pub(super) fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.commit(tpl, mode),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.commit(tpl, mode),
        }
    }

    pub(super) fn commit_message(
        &self,
        msg: &BorrowedMessage<'_>,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.commit_message(msg, mode),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.commit_message(msg, mode),
        }
    }
}

// Consumer helper
// ---------------------------------------------------------------------------

fn create_stream_consumer(
    mut base: ClientConfig,
    group_id: &str,
    auto_offset_reset: KafkaAutoOffsetReset,
    #[cfg(feature = "kafka-msk-iam")] msk_context: Option<MskIamContext>,
) -> Result<KafkaStreamConsumer> {
    // Each consumer task within a group gets a distinct `client.id` so
    // librdkafka treats them as separate members. Without this, group
    // rebalances across repeated join attempts can produce stale
    // "group generation id is not valid" commit errors.
    let client_id = format!("shove-{}", uuid::Uuid::new_v4().simple());
    base.set("group.id", group_id)
        .set("client.id", client_id)
        // Cooperative-sticky assignment performs incremental rebalance so that
        // adding/removing a consumer only reassigns the delta — without this,
        // every join triggers an eager (stop-the-world) rebalance that
        // freezes the entire group for the heartbeat window.
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", auto_offset_reset.as_rdkafka_str())
        .set("session.timeout.ms", SESSION_TIMEOUT_MS.to_string())
        .set("max.poll.interval.ms", MAX_POLL_INTERVAL_MS.to_string())
        // Minimise fetch-latency so small-payload workloads aren't bottlenecked
        // by the default 500 ms broker dwell. `FETCH_MIN_BYTES=1` returns as
        // soon as any data is available; `FETCH_WAIT_MAX_MS=50` caps the
        // blocking dwell so the broker doesn't hold the connection open.
        .set("fetch.min.bytes", FETCH_MIN_BYTES.to_string())
        .set("fetch.wait.max.ms", FETCH_WAIT_MAX_MS.to_string());

    #[cfg(feature = "kafka-msk-iam")]
    if let Some(ctx) = msk_context {
        let consumer: StreamConsumer<MskIamContext> = base
            .create_with_context(ctx)
            .map_err(|e| map_kafka_error("failed to create MSK consumer", e))?;
        return Ok(KafkaStreamConsumer::MskIam(consumer));
    }

    let consumer: StreamConsumer = base
        .create()
        .map_err(|e| map_kafka_error("failed to create consumer", e))?;
    Ok(KafkaStreamConsumer::Default(consumer))
}

// ---------------------------------------------------------------------------
// Reconnect loop
// ---------------------------------------------------------------------------

async fn run_with_reconnect<F, Fut>(
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
// KafkaConsumer
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct KafkaConsumer {
    client: KafkaClient,
}

impl KafkaConsumer {
    pub fn new(client: KafkaClient) -> Self {
        Self { client }
    }
}

impl KafkaConsumer {
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
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
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| super::constants::consumer_group_id(queue));
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        tracing::info!(
            queue,
            group_id,
            prefetch_count,
            max_retries,
            "Kafka consumer started"
        );

        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let group_id = group_id.clone();
            let semaphore = semaphore.clone();
            let topic = topic.clone();
            let group = group.clone();
            async move {
                let consumer = create_stream_consumer(
                    client.base_config(),
                    &group_id,
                    auto_offset_reset,
                    #[cfg(feature = "kafka-msk-iam")]
                    client.msk_context(),
                )?;
                consumer
                    .subscribe(&[queue])
                    .map_err(|e| map_kafka_error("failed to subscribe", e))?;

                let queue_owned = queue.to_string();
                // perf-K-6: OffsetTracker is touched only by this receive loop; handler
                // completions arrive via completion_tx/_rx. Drop the Mutex so the loop
                // owns the tracker directly — saves two async-lock acquisitions per
                // message (drain at top + track_received in the message branch).
                let mut tracker = OffsetTracker::new(queue_owned.clone());
                let consumer = Arc::new(consumer);
                // Bounded to prefetch_count: the semaphore already limits in-flight
                // handler tasks to this count, so the channel can never grow beyond
                // it under correct operation. An Err from try_send would indicate a
                // logic bug (handler completing without holding a permit) and is
                // surfaced immediately rather than silently accumulating (sec-K-4).
                let (completion_tx, mut completion_rx) =
                    mpsc::channel::<(i32, i64)>(prefetch_count as usize);

                loop {
                    // Drain completed offsets and commit
                    while let Ok((partition, offset)) = completion_rx.try_recv() {
                        tracker.mark_complete(partition, offset);
                    }
                    if let Some(tpl) = tracker.drain_committable() {
                        consumer
                            .commit(&tpl, CommitMode::Async)
                            .map_err(|e| map_kafka_error("commit failed", e))?;
                    }

                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            // Final commit
                            while let Ok((partition, offset)) = completion_rx.try_recv() {
                                tracker.mark_complete(partition, offset);
                            }
                            if let Some(tpl) = tracker.drain_committable() {
                                consumer.commit(&tpl, CommitMode::Async).ok();
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

                            // perf-K-5: defer Vec<u8> allocation until after decode succeeds.
                            // Oversize and decode-fail paths use msg.payload() directly for
                            // their DLQ publish (no copy). The happy path owns the bytes
                            // only because the handler runs in a spawned task that outlives
                            // this loop iteration.
                            let payload_slice = msg.payload().unwrap_or_default();
                            let headers = extract_string_headers(&msg);
                            let partition = msg.partition();
                            let offset = msg.offset();
                            // perf-K-9: store key as bytes::Bytes — cloning into spawned
                            // delay tasks becomes a refcount bump instead of a memcpy.
                            let key = msg.key().map(Bytes::copy_from_slice);

                            tracker.track_received(partition, offset);

                            metrics::record_message_size(&topic, group.as_deref(), payload_slice.len());

                            // Reject oversized messages before deserialization
                            if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
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
                                    payload_slice,
                                    key.as_deref(),
                                    &headers,
                                    &e.to_string(),
                                ).await {
                                    tracing::error!(
                                        error = %dlq_err,
                                        "failed to publish oversized message to DLQ"
                                    );
                                }
                                if completion_tx.try_send((partition, offset)).is_err() {
                                    tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                }
                                continue;
                            }

                            // Deserialize payload; reject to DLQ on failure
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
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
                                        payload_slice,
                                        key.as_deref(),
                                        &headers,
                                        // sec-K-5: do NOT append the codec error message to
                                        // the DLQ death-reason header — serde_json errors can
                                        // carry fragments of attacker-controlled payload bytes.
                                        // The full error is recorded via tracing above.
                                        "deserialization_error",
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish bad message to DLQ"
                                        );
                                    }
                                    if completion_tx.try_send((partition, offset)).is_err() {
                                        tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                    }
                                    continue;
                                }
                            };

                            // Decode succeeded — copy bytes for the spawned task's
                            // route_outcome (msg goes out of scope after this loop iteration).
                            let payload_bytes = payload_slice.to_vec();

                            let metadata = build_message_metadata(&headers, false);
                            let retry_count = metadata.retry_count;

                            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;

                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;
                            let task_tx = completion_tx.clone();
                            let task_topic = topic.clone();
                            let task_handler = handler.clone();
                            let task_ctx = ctx.clone();
                            let task_group = group.clone();

                            // perf-K-7: single spawn per message (was three).
                            // invoke_handler awaits the handler with catch_unwind +
                            // timeout in-place, then route_outcome runs in the same
                            // task — no inner spawn, no oneshot relay.
                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = invoke_handler(
                                    async move {
                                        task_handler
                                            .handle(payload, metadata, task_ctx.as_ref())
                                            .await
                                    },
                                    handler_timeout,
                                    &task_topic,
                                    task_group.as_deref(),
                                )
                                .await;

                                // sec-K-8: hand the prefetch permit to route_outcome
                                // so Retry/Defer's delayed republish spawn stays
                                // bounded by the prefetch limit instead of running
                                // outside the cap.
                                route_outcome(
                                    &task_client,
                                    &task_topic,
                                    task_group.as_deref(),
                                    &payload_bytes,
                                    key,
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    Some(permit),
                                )
                                .await;

                                if task_tx.try_send((partition, offset)).is_err() {
                                    tracing::error!(
                                        queue = task_topic.as_ref(),
                                        partition,
                                        offset,
                                        "completion channel full — logic bug in offset tracker"
                                    );
                                }
                                // sec-K-8: permit was passed to route_outcome —
                                // either dropped at end of Ack/Reject arms, or
                                // moved into the Retry/Defer republish spawn.

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
        options: crate::ConsumerOptions<Kafka>,
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
        let handles = self.spawn_fifo_shards::<T, H>(handler, ctx, options)?;
        // Kafka has exactly one FIFO task per call (single consumer, partition ordering).
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!("Kafka FIFO consumer task failed: {e}"),
                Err(e) => tracing::error!("Kafka FIFO consumer task panicked: {e}"),
            }
        }
        Ok(())
    }

    /// Spawn the Kafka FIFO consumer task and return its join handle.
    ///
    /// Kafka relies on partition-level ordering, so a single consumer task is
    /// sufficient — `routing_shards` is a no-op for Kafka FIFO. The returned
    /// `Vec` always contains exactly one element.
    ///
    /// `pub(crate)` visibility is required for Phase 2 (Task 16), which calls
    /// this from the consumer-group module.
    pub(crate) fn spawn_fifo_shards<T, H>(
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
        let queue = topology.queue().to_string();
        // sec-K-9: T is bound by SequencedTopic at the trait level, so this
        // is unreachable under correct callers. Returning an error instead
        // of expect()-panicking keeps misuse (e.g. from a future caller
        // path) recoverable.
        let _seq_config = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology(format!(
                "run_fifo called on {queue} without sequencing config"
            ))
        })?;

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        // Kafka naturally provides per-partition ordering. A single consumer
        // processing one message at a time guarantees FIFO per key (all
        // messages for the same key land in the same partition).
        let group_id = format!("{queue}-fifo");
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);
        let topic: Arc<str> = Arc::from(queue.as_str());
        let group: Option<Arc<str>> = options.consumer_group.clone();

        tracing::info!(queue, group_id, max_retries, "Kafka FIFO consumer started");

        let shard_task = tokio::spawn(async move {
            run_with_reconnect(&shutdown, &queue, options.max_reconnect_attempts, || {
                let handler = handler.clone();
                let ctx = ctx.clone();
                let client = client.clone();
                let shutdown = shutdown.clone();
                let processing = processing.clone();
                let group_id = group_id.clone();
                let queue = queue.clone();
                let topic = topic.clone();
                let group = group.clone();
                async move {
                    let consumer = create_stream_consumer(
                        client.base_config(),
                        &group_id,
                        auto_offset_reset,
                        #[cfg(feature = "kafka-msk-iam")]
                        client.msk_context(),
                    )?;
                    consumer
                        .subscribe(&[queue.as_str()])
                        .map_err(|e| map_kafka_error("failed to subscribe", e))?;

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

                                // perf-K-5: FIFO is sequential — msg lives through this whole
                                // iteration (commit_message at the end), so use msg.payload()
                                // directly instead of allocating a Vec<u8> copy.
                                let payload_bytes = msg.payload().unwrap_or_default();
                                let headers = extract_string_headers(&msg);
                                // perf-K-9: Bytes for cheap refcount-clone semantics.
                                let key = msg.key().map(Bytes::copy_from_slice);

                                metrics::record_message_size(&topic, group.as_deref(), payload_bytes.len());

                                // Reject oversized messages before deserialization
                                if let Err(e) = validate_message_size(payload_bytes.len(), max_message_size) {
                                    tracing::warn!(
                                        error = %e,
                                        queue,
                                        "rejecting oversized FIFO message to DLQ"
                                    );
                                    metrics::record_failed(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Oversize,
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &e.to_string(),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish oversized message to DLQ"
                                        );
                                    }
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }

                                // Deserialize payload; reject to DLQ on failure
                                let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            queue,
                                            "failed to deserialize FIFO message, sending to DLQ"
                                        );
                                        metrics::record_failed(
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::Deserialize,
                                        );
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &client,
                                            topology,
                                            payload_bytes,
                                            key.as_deref(),
                                            &headers,
                                            // sec-K-5: do NOT append the codec error message to
                                        // the DLQ death-reason header — serde_json errors can
                                        // carry fragments of attacker-controlled payload bytes.
                                        // The full error is recorded via tracing above.
                                        "deserialization_error",
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

                                // perf-K-7: call invoke_handler directly (no inner spawn).
                                // FIFO awaits the outcome inline anyway, so no task alloc
                                // is needed for panic isolation — catch_unwind covers it.
                                let handler_clone = handler.clone();
                                let ctx_clone = ctx.clone();
                                let outcome = invoke_handler(
                                    async move {
                                        handler_clone
                                            .handle(payload, metadata, ctx_clone.as_ref())
                                            .await
                                    },
                                    handler_timeout,
                                    &topic,
                                    group.as_deref(),
                                )
                                .await;
                                let outcome = adjust_outcome_for_fifo(outcome);

                                route_outcome(
                                    &client,
                                    &queue,
                                    group.as_deref(),
                                    payload_bytes,
                                    key,
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    // FIFO is sequential — no prefetch semaphore in play.
                                    None,
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
        });

        Ok(vec![shard_task])
    }

    /// Drain a Kafka FIFO consumer with a timeout, mirroring
    /// [`ConsumerSupervisor::run_until_timeout`] for sequenced topics.
    ///
    /// Spawns a single FIFO task (Kafka uses partition ordering rather than
    /// routing shards). Races `signal` against the task exiting on its own.
    /// When `signal` resolves, cancels `options.shutdown` and waits up to
    /// `drain_timeout` for the task to finish; a surviving task is aborted
    /// and reflected in `timed_out`.
    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
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
        let handles = match self.spawn_fifo_shards::<T, H>(handler, ctx, options) {
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

    /// Public DLQ entrypoint with default options (no max_message_size cap).
    /// Equivalent to `run_dlq_with_inner` with `ConsumerOptions::default()`
    /// inner; kept for backward compatibility with users who don't need to
    /// thread per-consumer options into the DLQ loop.
    pub async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let options = crate::ConsumerOptions::<Kafka>::new().into_inner();
        self.run_dlq_with_inner::<T, H>(handler, ctx, options).await
    }

    pub(crate) async fn run_dlq_with_inner<T, H>(
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
        let dlq = topology.dlq().ok_or_else(|| {
            ShoveError::Topology("run_dlq requires a DLQ to be configured".into())
        })?;

        let dlq_group_id = super::constants::dlq_consumer_group_id(dlq);
        let shutdown = self.client.shutdown_token();
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        // sec-K-7: respect the same max_message_size the main consumer uses
        // rather than the DEFAULT_MAX_MESSAGE_SIZE constant.
        let max_message_size = options.max_message_size;

        tracing::info!(dlq, group_id = dlq_group_id, "Kafka DLQ consumer started");

        run_with_reconnect(&shutdown, dlq, None, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client_clone = client.clone();
            let shutdown = shutdown.clone();
            let dlq_group_id = dlq_group_id.clone();
            async move {
                // DLQ consumers always drain from the earliest available
                // offset — skipping dead messages on a tail-only join would
                // silently lose audit data the operator explicitly opted in
                // to. Keep the policy fixed regardless of the user's main
                // consumer `auto_offset_reset` override.
                let consumer = create_stream_consumer(
                    client_clone.base_config(),
                    &dlq_group_id,
                    KafkaAutoOffsetReset::Earliest,
                    #[cfg(feature = "kafka-msk-iam")]
                    client_clone.msk_context(),
                )?;
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

                            // perf-K-5: msg lives through commit_message at the end of this
                            // iteration and we never spawn — decode from msg.payload() directly
                            // instead of allocating a Vec<u8> copy.
                            let payload_bytes = msg.payload().unwrap_or_default();
                            let headers = extract_string_headers(&msg);

                            // sec-K-7: honor options.max_message_size (same as the main
                            // consumer) instead of the DEFAULT_MAX_MESSAGE_SIZE constant.
                            // None means no limit.
                            if let Some(max) = max_message_size
                                && payload_bytes.len() > max
                            {
                                tracing::warn!(
                                    bytes = payload_bytes.len(),
                                    max,
                                    dlq,
                                    "oversized DLQ message — discarding"
                                );
                                consumer.commit_message(&msg, CommitMode::Async).ok();
                                continue;
                            }

                            // Deserialize payload; on failure, log and ack anyway
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
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
                            handler.handle_dead(payload, metadata, ctx.as_ref()).await;

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
