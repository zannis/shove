use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::{HeaderName, HeaderValue, NATS_MESSAGE_ID};
use async_nats::jetstream::Message;
use async_nats::jetstream::consumer::pull::{Batch, Config as PullConsumerConfig};
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer};
use async_nats::jetstream::context::{
    ConsumerInfoError, ConsumerInfoErrorKind, GetStreamError, GetStreamErrorKind,
};
use async_nats::jetstream::message::AckKind;
use futures_util::FutureExt;
use futures_util::stream::{self, StreamExt};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::batch_consumer::settling::{
    PREALLOC_CAP, batch_redelivery_backoff, invoke_batch_handler, next_redelivery_delay,
};
use crate::backend::batch_consumer::{
    BatchConsumerOptionsInner, BatchSettlement, settle_batch_outcome,
};
use crate::consumer::{DEFAULT_HANDLER_TIMEOUT, RetryStrategy, validate_message_size};
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::Result;
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{
    PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome, hold_delay,
};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, Nats, ShoveError};

use super::client::NatsClient;
use super::constants::{
    COPY_OF_HEADER, DEATH_COUNT_HEADER, DEATH_REASON_HEADER, ORIGINAL_QUEUE_HEADER,
    RETRY_COUNT_HEADER, SEQUENCE_KEY_HEADER, UNBOUNDED_ACK_PENDING,
};
use super::publisher::{PublishResult, header_value, publish_with_retry};

// ---------------------------------------------------------------------------
// ack_wait derivation
// ---------------------------------------------------------------------------

/// JetStream redelivers any message not acked within `ack_wait`, a clock that
/// starts at delivery. A consumer takes delivery only of what it can start at
/// once: a worker pulls no more messages than it has free slots, and a FIFO
/// shard's durable delivers one at a time. So the clock measures the handler
/// and its settlement, and the 3x margin over the handler timeout lets a
/// handler run to its limit with room to settle. A copy's publish and a
/// retry's hold renew the clock with progress acks instead of drawing on the
/// margin. Floor at the JetStream default (30s) so short handler timeouts
/// don't tighten redelivery below the server default.
pub(super) fn derive_ack_wait(handler_timeout: Duration) -> Duration {
    handler_timeout
        .saturating_mul(3)
        .max(Duration::from_secs(30))
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

/// The retry strategy a NATS consumer runs with, from the topology's
/// ownership.
///
/// An external stream implies [`RetryStrategy::InPlace`]: the consumer never
/// writes into a stream infra owns, so a `Retry` is the same delayed negative
/// acknowledgement a `Defer` uses, and the count is read from JetStream's
/// delivery info. A shove-owned stream keeps [`RetryStrategy::Republish`],
/// the hold-then-republish path with the header-based count. The setter on
/// the consumer options reaches NATS with plan 019, when an owned stream can
/// opt into `InPlace` as well.
fn resolve_retry_strategy(topology: &QueueTopology) -> RetryStrategy {
    if topology.external() {
        RetryStrategy::InPlace
    } else {
        RetryStrategy::Republish
    }
}

/// The retry count an in-place consumer settles with.
///
/// A republish carries the count in the `Shove-Retry-Count` header of every
/// copy it publishes. An in-place retry publishes nothing, so the count is the
/// number of redeliveries JetStream itself reports: `num_delivered - 1`, 0 on
/// the first delivery. An `ack_wait` expiry and a `Defer` nak are redeliveries
/// too, so on an external stream both consume the retry budget; the
/// `MessageMetadata::delivery_count` doc states the same.
fn in_place_retry_count(metadata: &MessageMetadata) -> u32 {
    metadata.delivery_count.map_or(0, |n| n.saturating_sub(1))
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

    // The stream metadata is absent for messages that did not come from a
    // stream (`info()` errors), which is why every field read from it is
    // optional rather than defaulted.
    let info = msg.info().ok();
    // `info.delivered` is JetStream's `num_delivered`: attempts so far including
    // this one, so the first delivery reports 1.
    let delivery_count = info
        .as_ref()
        .map(|info| u32::try_from(info.delivered).unwrap_or(u32::MAX));
    let redelivered = delivery_count.is_some_and(|n| n > 1);
    // The stream sequence is a log position with no partition, and
    // `published` is when the server received the message from its publisher;
    // see the field docs on `MessageMetadata::offset` and `timestamp_ms`.
    let (offset, timestamp_ms) = info
        .as_ref()
        .map(|info| {
            (
                sequence_to_offset(info.stream_sequence),
                published_to_millis(info.published.unix_timestamp_nanos()),
            )
        })
        .unwrap_or((None, None));

    let headers = extract_string_headers(&msg.headers);

    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        delivery_count,
        // No partitions on JetStream; the sequence goes in `offset`.
        partition: None,
        offset,
        timestamp_ms,
        headers: Arc::new(headers),
    }
}

/// A JetStream stream sequence as [`MessageMetadata::offset`]. Checked: the
/// field is `i64` and the sequence is `u64`, so a value past `i64::MAX` reads
/// as `None` rather than wrapping.
fn sequence_to_offset(sequence: u64) -> Option<i64> {
    i64::try_from(sequence).ok()
}

/// A stream message's `published` instant, given as nanoseconds since the
/// Unix epoch, as [`MessageMetadata::timestamp_ms`]. Floor division keeps a
/// pre-epoch instant on the millisecond it falls in, and the checked
/// narrowing reads a value outside `i64` as `None`.
fn published_to_millis(unix_nanos: i128) -> Option<i64> {
    i64::try_from(unix_nanos.div_euclid(1_000_000)).ok()
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

/// Header-name prefixes of the JetStream publish directives a copy never
/// replays. Each instructs the server about the publish that carries it, and
/// none describes the copy: a `Nats-Expected-*` check fails against another
/// stream or a later sequence, and a stream that does not enable rollups,
/// per-message TTLs, atomic batches or schedules refuses the copy, while one
/// that does would roll it up, expire it, batch it or schedule it.
const PUBLISH_DIRECTIVE_PREFIXES: &[&str] = &[
    "Nats-Expected-",
    "Nats-Rollup",
    "Nats-TTL",
    "Nats-Batch-",
    "Nats-Schedule",
    "Nats-Required-Api-Level",
];

/// Whether `name` starts with one of the [`PUBLISH_DIRECTIVE_PREFIXES`], in
/// any letter case: dropping a `nats-*` header that is no directive costs a
/// copy nothing, and replaying one that is blocks it for good.
fn is_publish_directive(name: &HeaderName) -> bool {
    let name: &str = name.as_ref();
    PUBLISH_DIRECTIVE_PREFIXES.iter().any(|prefix| {
        name.get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    })
}

/// The original's headers less its publish directives: what every retry and
/// dead-letter copy starts from. Every value of a repeated header is kept.
fn copy_headers(original: Option<&HeaderMap>) -> HeaderMap {
    let mut copy = HeaderMap::new();
    for (name, values) in original.into_iter().flat_map(HeaderMap::iter) {
        if is_publish_directive(name) {
            continue;
        }
        for value in values {
            copy.append(name.clone(), value.clone());
        }
    }
    copy
}

/// The delivery a copy is made from, `{stream}.{consumer}.{stream_sequence}`:
/// stamped on every copy as `Shove-Copy-Of`, and the root of the id of a copy
/// whose original carries no `Nats-Msg-Id`. The consumer is part of it, so two
/// consumers copying one record into a shared DLQ never pass for each other.
fn copy_origin_of(stream: &str, consumer: &str, stream_sequence: u64) -> String {
    format!("{stream}.{consumer}.{stream_sequence}")
}

/// The [`copy_origin_of`] the delivery `msg` arrived on.
fn copy_origin(msg: &Message) -> Result<String> {
    let info = msg.info().map_err(|e| {
        ShoveError::Validation(format!("a copy needs the delivery's stream position: {e}"))
    })?;
    Ok(copy_origin_of(
        info.stream,
        info.consumer,
        info.stream_sequence,
    ))
}

/// The `Nats-Msg-Id` of a copy: the original's own id when it has one, so a
/// handler deduplicating on `delivery_id` keeps the lineage, or else the
/// copy's origin; then `-{suffix}`.
///
/// Either way the id can be predicted, which is why a duplicate PubAck for a
/// copy is trusted only once the stored message proves to be the same copy
/// (`publish_with_retry`). A redelivery of the same record, after a crash
/// between the copy's publish and the original's ack, rebuilds the same id and
/// passes that check while the copy is still stored, so the duplicate window
/// still keeps one copy.
fn copy_message_id(original_id: Option<&str>, origin: &str, suffix: &str) -> String {
    let root = original_id.filter(|id| !id.is_empty()).unwrap_or(origin);
    format!("{root}-{suffix}")
}

/// Stamps a copy of `msg` with its origin and its id.
fn stamp_copy(headers: &mut HeaderMap, msg: &Message, origin: &str, suffix: &str) -> Result<()> {
    let original_id = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(NATS_MESSAGE_ID))
        .map(HeaderValue::as_str);
    headers.insert(COPY_OF_HEADER, header_value(COPY_OF_HEADER, origin)?);
    headers.insert(
        NATS_MESSAGE_ID,
        header_value("Nats-Msg-Id", &copy_message_id(original_id, origin, suffix))?,
    );
    Ok(())
}

/// Publishes a message to the DLQ stream with death headers, the message's
/// own `Shove-Retry-Count` header kept as it is.
async fn publish_to_dlq(
    client: &NatsClient,
    topology: &QueueTopology,
    msg: &Message,
    reason: &str,
) -> PublishResult {
    publish_dead_letter(client, topology, msg, reason, None).await
}

/// Publishes a message to the DLQ stream with death headers.
///
/// `reason` is written to `Shove-Death-Reason` as given. A codec error can
/// quote the payload, line breaks included, so a decode failure passes the
/// fixed `deserialization_error` and leaves the error itself to the log.
///
/// `retry_count` is `Some` on the in-place path, where the count lives in the
/// delivery info and not in a header: the dead letter is stamped with it, so a
/// DLQ handler reads the same `retry_count` a republish would have carried.
/// `None` keeps the message's own header.
async fn publish_dead_letter(
    client: &NatsClient,
    topology: &QueueTopology,
    msg: &Message,
    reason: &str,
    retry_count: Option<u32>,
) -> PublishResult {
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

    let origin = copy_origin(msg)?;
    let mut headers = copy_headers(msg.headers.as_ref());
    headers.insert(
        DEATH_REASON_HEADER,
        header_value(DEATH_REASON_HEADER, reason)?,
    );
    headers.insert(
        ORIGINAL_QUEUE_HEADER,
        header_value(ORIGINAL_QUEUE_HEADER, topology.queue())?,
    );
    if let Some(count) = retry_count {
        headers.insert(RETRY_COUNT_HEADER, HeaderValue::from(count));
    }

    let current_death_count = msg
        .headers
        .as_ref()
        .and_then(|hm| hm.get(DEATH_COUNT_HEADER))
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0);
    headers.insert(
        DEATH_COUNT_HEADER,
        HeaderValue::from(current_death_count.saturating_add(1)),
    );
    stamp_copy(&mut headers, msg, &origin, "dlq")?;

    publish_with_retry(
        client.jetstream(),
        dlq_subject,
        headers,
        msg.payload.clone(),
        Some(&origin),
        3,
        "DLQ publish",
    )
    .await
}

/// Retries a message after `delay` without the at-least-once gap the old
/// ack-then-republish path had.
///
/// The original message is held *un-acked* for the whole backoff, so a crash or
/// a failed republish can never drop it (the same enqueue/republish-before-ack
/// ordering the Redis and Kafka paths use). JetStream redelivers any un-acked
/// message once `ack_wait` elapses, so the hold renews that timer with progress
/// acks ([`hold_with_progress`]), and so does the copy's publish
/// ([`renewing`]). Once `delay` has passed, an incremented copy is published
/// durably (the PubAck is awaited) and only then is the original acked. On
/// shutdown the hold is abandoned with the message still un-acked so it is
/// redelivered on restart; on republish failure the original is nak'd for
/// redelivery, at once or, when the copy's id is held by a message that is not
/// this copy, after a second.
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
    if !hold_with_progress(msg, delay, ack_wait, shutdown).await {
        return;
    }

    // Publish the copy durably BEFORE acking the original: a failure leaves the
    // original for redelivery instead of silently dropping the message.
    match renewing(msg, ack_wait, publish_retry_copy(client, msg, new_count)).await {
        Ok(()) => {
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack after retry republish");
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "retry republish failed, nak-ing original for redelivery");
            if let Err(nak_err) = msg.ack_with(AckKind::Nak(e.redelivery_delay())).await {
                tracing::error!(error = %nak_err, "failed to nak after retry republish failure");
            }
        }
    }
}

/// Publishes the retry copy of `msg`: the original's headers less its publish
/// directives, with the incremented count, its origin and its id.
async fn publish_retry_copy(client: &NatsClient, msg: &Message, new_count: u32) -> PublishResult {
    let origin = copy_origin(msg)?;
    let mut headers = copy_headers(msg.headers.as_ref());
    headers.insert(RETRY_COUNT_HEADER, HeaderValue::from(new_count));
    stamp_copy(&mut headers, msg, &origin, &format!("r{new_count}"))?;
    publish_with_retry(
        client.jetstream(),
        msg.subject.to_string(),
        headers,
        msg.payload.clone(),
        Some(&origin),
        3,
        "retry republish",
    )
    .await
}

/// Runs `work` while renewing `msg`'s `ack_wait` with a progress ack every
/// [`progress_interval`], so a wait that can outlast `ack_wait`, such as a
/// copy's publish with its retries and its duplicate read-back, never gets
/// `msg` redelivered while it runs.
async fn renewing<F: Future>(msg: &Message, ack_wait: Duration, work: F) -> F::Output {
    let interval = progress_interval(ack_wait);
    let mut work = std::pin::pin!(work);
    loop {
        // A publish can block while the client reconnects.
        let renewed = tokio::select! {
            biased;
            out = &mut work => return out,
            renewed = async {
                tokio::time::sleep(interval).await;
                msg.ack_with(AckKind::Progress).await
            } => renewed,
        };
        if let Err(e) = renewed {
            tracing::warn!(error = %e, "failed to renew the ack_wait of a waiting message");
        }
    }
}

/// The failure a dead letter is counted under, from the reason
/// [`decide_retry`] gave it.
fn dlq_fail_reason(reason: &str) -> metrics::FailReason {
    match reason {
        "rejected" => metrics::FailReason::Rejected,
        _ => metrics::FailReason::MaxRetriesExceeded,
    }
}

// ---------------------------------------------------------------------------
// Sequenced settlement
// ---------------------------------------------------------------------------

/// The `ack_wait` JetStream applies to a consumer whose config leaves it at
/// zero.
const JETSTREAM_DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);

/// The count a sequenced record first reaches its handler with: its
/// `Shove-Retry-Count` plus one for every earlier JetStream delivery, so each
/// hand-off to a new delivery spends a retry, whether a stop, a crash or an
/// `ack_wait` expiry made it.
fn sequenced_retry_count(metadata: &MessageMetadata) -> u32 {
    metadata
        .retry_count
        .saturating_add(in_place_retry_count(metadata))
}

/// How often a held record renews its `ack_wait` with a progress ack: a third
/// of it, so one renewal can be lost and the next still lands in time. Zero
/// stands for JetStream's default.
fn progress_interval(ack_wait: Duration) -> Duration {
    let ack_wait = if ack_wait.is_zero() {
        JETSTREAM_DEFAULT_ACK_WAIT
    } else {
        ack_wait
    };
    ack_wait / 3
}

/// Holds `msg` un-acked for `delay`, renewing its `ack_wait` with a progress
/// ack every [`progress_interval`] and once more as the wait ends, so what
/// follows the hold, the handler on a FIFO shard or the copy's publish on a
/// concurrent worker, has a whole `ack_wait` ahead of it. `false` when
/// shutdown fired first, the record still un-acked.
async fn hold_with_progress(
    msg: &Message,
    delay: Duration,
    ack_wait: Duration,
    shutdown: &CancellationToken,
) -> bool {
    let interval = progress_interval(ack_wait);
    let started = tokio::time::Instant::now();
    loop {
        let remaining = delay.saturating_sub(started.elapsed());
        // A publish can block while the client reconnects.
        let renewed = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return false,
            renewed = async {
                tokio::time::sleep(remaining.min(interval)).await;
                msg.ack_with(AckKind::Progress).await
            } => renewed,
        };
        if let Err(e) = renewed {
            tracing::warn!(error = %e, "failed to renew the ack_wait of a held message");
        }
        if remaining <= interval {
            return true;
        }
    }
}

/// What a FIFO shard settles each of its records with.
struct FifoShard<'a, H, C> {
    client: &'a NatsClient,
    topology: &'static QueueTopology,
    handler: &'a Arc<H>,
    ctx: &'a Arc<C>,
    poisoned: &'a PoisonedKeys,
    processing: &'a AtomicBool,
    topic: &'a str,
    group: Option<&'a str>,
    shard: u16,
    max_retries: u32,
    max_message_size: Option<usize>,
    handler_timeout: Option<Duration>,
    timeout_outcome: &'a Option<Outcome>,
    ack_wait: Duration,
    shutdown: &'a CancellationToken,
}

/// How a FIFO shard left the record it was handed.
enum ShardStep {
    /// Acked or dead-lettered: the shard takes its next record.
    Settled,
    /// Shutdown fired while the record was held. It is still un-acked, and the
    /// shard releases it with this redelivery delay once its pull
    /// subscription is gone ([`release_held`]).
    Stopped { redelivery: Option<Duration> },
}

/// How [`dead_letter_in_place`] left the record.
enum DeadLetter {
    /// The dead letter is stored, or the topology has no DLQ to store it in.
    Landed,
    /// Shutdown fired between two attempts. The record is still un-acked, and
    /// `redelivery` is the delay the last failed publish asks for.
    Stopped { redelivery: Option<Duration> },
}

/// Publishes the dead letter of a record a FIFO shard holds. A failed publish
/// keeps the record un-acked, its `ack_wait` renewed, and is retried in place
/// after a capped exponential backoff for as long as it fails, so nothing
/// behind the record on its shard is delivered before the copy lands.
async fn dead_letter_in_place<H, C>(
    shard: &FifoShard<'_, H, C>,
    msg: &Message,
    reason: &str,
    retry_count: Option<u32>,
) -> DeadLetter {
    let mut backoff = Backoff::default();
    loop {
        let failure =
            match publish_dead_letter(shard.client, shard.topology, msg, reason, retry_count).await
            {
                Ok(()) => return DeadLetter::Landed,
                Err(e) => e,
            };
        let delay = next_redelivery_delay(&mut backoff);
        tracing::error!(
            error = %failure,
            shard = shard.shard,
            retry_in = ?delay,
            "DLQ publish failed, keeping the sequenced message and retrying"
        );
        if !hold_with_progress(msg, delay, shard.ack_wait, shard.shutdown).await {
            return DeadLetter::Stopped {
                redelivery: failure.redelivery_delay(),
            };
        }
    }
}

/// Dead-letters a record the handler never saw, oversized or undecodable,
/// then acks it. Like the other NATS pre-handler drops it counts only the
/// failure the caller already recorded, no discard.
async fn dead_letter_unhandled<H, C>(
    shard: &FifoShard<'_, H, C>,
    msg: &Message,
    reason: &str,
    retry_count: Option<u32>,
) -> ShardStep {
    match dead_letter_in_place(shard, msg, reason, retry_count).await {
        DeadLetter::Landed => {
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack message after DLQ publish");
            }
            ShardStep::Settled
        }
        DeadLetter::Stopped { redelivery } => ShardStep::Stopped { redelivery },
    }
}

/// Dead-letters a record whose outcome is terminal, a `Reject` or an exhausted
/// `Retry`, stamped with the count it was settled at, and with the accounting
/// of `route_outcome`'s DLQ arm: the failure counts at once, the discard only
/// once `double_ack` confirms the record is retired.
async fn dead_letter_terminal<H, C>(
    shard: &FifoShard<'_, H, C>,
    msg: &Message,
    reason: &'static str,
    retry_count: u32,
) -> ShardStep {
    let pending = metrics::record_terminal(
        shard.topic,
        shard.group,
        dlq_fail_reason(reason),
        shard.topology.dlq().is_some(),
    );
    match dead_letter_in_place(shard, msg, reason, Some(retry_count)).await {
        DeadLetter::Landed => {
            // Only a server-confirmed ack proves the record is retired.
            match msg.double_ack().await {
                Ok(()) => pending.confirm(),
                Err(e) => {
                    tracing::error!(error = %e, "failed to ack after DLQ publish");
                    pending.survived();
                }
            }
            ShardStep::Settled
        }
        DeadLetter::Stopped { redelivery } => {
            pending.survived();
            ShardStep::Stopped { redelivery }
        }
    }
}

/// Settles one record a FIFO shard was handed, keeping it at its position on
/// the shard until it is acked or dead-lettered.
///
/// A `Retry`, which on a sequenced consumer also covers a `Defer`, a handler
/// timeout and a handler panic, holds the record un-acked through the
/// attempt's hold delay ([`hold_with_progress`]) and then hands it back to the
/// handler in process, decoded afresh from its bytes. The retry count lives in
/// memory, seeded by [`sequenced_retry_count`], and nothing is republished.
/// The shard durable's `max_ack_pending: 1` keeps every later record on the
/// shard undelivered meanwhile. A `Reject` or an exhausted retry budget
/// dead-letters the record under the topic's `SequenceFailure`, with the
/// count it was settled at, and a failed dead-letter publish is retried in
/// place ([`dead_letter_in_place`]).
async fn settle_sequenced<T, H>(shard: &FifoShard<'_, H, H::Context>, msg: &Message) -> ShardStep
where
    T: SequencedTopic,
    H: MessageHandler<T>,
{
    let topology = shard.topology;
    let queue = topology.queue();

    // ── FailAll: skip poisoned keys ──
    // Inert unless this topic is configured `SequenceFailure::FailAll`.
    let seq_key = get_sequence_key(&msg.headers);
    if shard.poisoned.is_poisoned(&seq_key) {
        tracing::warn!(
            shard = shard.shard,
            sequence_key = %seq_key,
            "sequence key poisoned (FailAll) — sending to DLQ without invoking handler"
        );
        // Collateral of an already-counted failure, so the failure half is
        // deliberately not counted again — see `metrics::FailReason`. The
        // discard half still applies: a cascaded message dropped with no DLQ
        // is just as gone as any other.
        let has_dlq = topology.dlq().is_some();
        let pending = metrics::pending_discard(
            shard.topic,
            shard.group,
            metrics::FailReason::Rejected,
            has_dlq,
        );
        if let DeadLetter::Stopped { redelivery } =
            dead_letter_in_place(shard, msg, "rejected", None).await
        {
            pending.survived();
            return ShardStep::Stopped { redelivery };
        }
        if has_dlq {
            // The message is in the DLQ, so it exists whatever the ack does
            // and `confirm` could never count it. Settling now keeps the
            // dead-lettered cascade off the `double_ack` round trip the live
            // pending record below needs — and a poisoned key drains its whole
            // backlog through this branch, so that tax is per message.
            pending.survived();
            if let Err(e) = msg.ack().await {
                tracing::error!(error = %e, "failed to ack poisoned-key message after DLQ publish");
            }
            return ShardStep::Settled;
        }
        // No DLQ, so this ack is what drops the message and it decides the
        // discard accounting. `double_ack` rather than `ack`: `ack` returns as
        // soon as `+ACK` is written to the connection, so a connection lost
        // before JetStream applies it would redeliver a message already
        // counted gone. Same reasoning as `route_outcome`'s Dlq arm.
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
        return ShardStep::Settled;
    }

    if let Err(e) = validate_message_size(msg.payload.len(), shard.max_message_size) {
        tracing::warn!(error = %e, shard = shard.shard, "rejecting oversized message to DLQ");
        metrics::record_failed(shard.topic, shard.group, metrics::FailReason::Oversize);
        poison_key(shard.poisoned, &seq_key, queue);
        return dead_letter_unhandled(shard, msg, &e.to_string(), None).await;
    }

    let mut metadata = extract_message_metadata(msg);
    metadata.retry_count = sequenced_retry_count(&metadata);
    loop {
        let payload: T::Message =
            match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        shard = shard.shard,
                        "failed to deserialize message, sending to DLQ"
                    );
                    metrics::record_failed(
                        shard.topic,
                        shard.group,
                        metrics::FailReason::Deserialize,
                    );
                    poison_key(shard.poisoned, &seq_key, queue);
                    let count = Some(metadata.retry_count);
                    return dead_letter_unhandled(shard, msg, "deserialization_error", count).await;
                }
            };

        shard.processing.store(true, Ordering::Release);
        let retry_count = metadata.retry_count;
        let handler = Arc::clone(shard.handler);
        let ctx = Arc::clone(shard.ctx);
        let attempt = metadata.clone();
        let outcome = invoke_handler(
            async move { handler.handle(payload, attempt, ctx.as_ref()).await },
            shard.handler_timeout,
            shard.timeout_outcome.clone(),
            shard.topic,
            shard.group,
        )
        .await;

        match decide_retry(
            &adjust_outcome_for_fifo(outcome),
            retry_count,
            shard.max_retries,
        ) {
            RetryDecision::Ack => {
                if let Err(e) = msg.ack().await {
                    tracing::error!(error = %e, "failed to ack message");
                }
                return ShardStep::Settled;
            }
            RetryDecision::Dlq { reason } => {
                // FailAll: a DLQ-terminal outcome poisons the key, so every
                // later message for it is dead-lettered instead of handled.
                poison_key(shard.poisoned, &seq_key, queue);
                return dead_letter_terminal(shard, msg, reason, retry_count).await;
            }
            RetryDecision::Hold { increment } => {
                let delay = hold_delay(topology.hold_queues(), retry_count);
                if !hold_with_progress(msg, delay, shard.ack_wait, shard.shutdown).await {
                    return ShardStep::Stopped { redelivery: None };
                }
                if increment {
                    metadata.retry_count = retry_count.saturating_add(1);
                }
                metadata.redelivered = true;
            }
        }
    }
}

/// How long a stopping consumer spends releasing the messages it holds. A
/// release that cannot finish in time, on a connection that is down, leaves
/// them to be redelivered once their `ack_wait` expires.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// How much of [`RELEASE_TIMEOUT`] the consumer-info sweep in
/// [`sweep_waiting_list`] may take, so a sweep that times out still leaves
/// time for the naks.
const SWEEP_TIMEOUT: Duration = Duration::from_secs(2);

/// Has the server sweep the durable's waiting list once a stopping
/// consumer's pull subscription is dropped, so neither a nak that follows
/// nor a newly published message is spent on that subscription's request.
///
/// The server keeps the request on the waiting list until it next sweeps the
/// list, and a delivery made before then goes to an inbox nobody reads, or,
/// on nats-server 2.10.14, is dropped: the message waits out its `ack_wait`.
/// A consumer-info request sweeps the list. The sweep only finds the request
/// dead once the subscription's UNSUB has reached the server, and that UNSUB
/// is sent from a task the subscription's drop spawned: the yield gives that
/// task its turn, with no guarantee that it has sent the UNSUB by then.
async fn sweep_waiting_list(consumer: &PullConsumer) {
    tokio::task::yield_now().await;
    match tokio::time::timeout(SWEEP_TIMEOUT, consumer.get_info()).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(
            error = %e,
            "failed to clear a stopped consumer's pull request before releasing its messages"
        ),
        Err(_) => tracing::warn!(
            timeout = ?SWEEP_TIMEOUT,
            "timed out clearing a stopped consumer's pull request before releasing its messages"
        ),
    }
}

/// Naks the record a stopping shard still holds, so the server can hand it to
/// the next consumer of the shard at once, ahead of every record behind it.
///
/// Best effort: whatever does not happen within [`RELEASE_TIMEOUT`], and every
/// release the server spends elsewhere, leaves the record to be redelivered
/// once its `ack_wait` expires. Nothing is lost either way, and nothing behind
/// the record is delivered first. Called once the shard's pull subscription
/// is dropped, and sweeps the waiting list first ([`sweep_waiting_list`]).
async fn release_held(consumer: &PullConsumer, msg: &Message, redelivery: Option<Duration>) {
    let release = async {
        sweep_waiting_list(consumer).await;
        msg.ack_with(AckKind::Nak(redelivery)).await
    };
    match tokio::time::timeout(RELEASE_TIMEOUT, release).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(
            error = %e,
            "failed to release a held sequenced message; it is redelivered once ack_wait expires"
        ),
        Err(_) => tracing::error!(
            timeout = ?RELEASE_TIMEOUT,
            "timed out releasing a held sequenced message; it is redelivered once ack_wait expires"
        ),
    }
}

/// Naks, without delay, the messages a stopping worker pulled and never
/// started, so the server can hand them to another consumer at once. Called
/// once the worker's pull is dropped, and sweeps the waiting list first
/// ([`sweep_waiting_list`]), which it does even with nothing to nak, so no
/// message published from then on goes to the dropped pull's request.
///
/// Best effort within [`RELEASE_TIMEOUT`], like [`release_held`]: a message it
/// cannot release is redelivered once its `ack_wait` expires.
async fn release_pulled(consumer: &PullConsumer, unstarted: &[Message]) {
    let release = async {
        sweep_waiting_list(consumer).await;
        for msg in unstarted {
            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                tracing::error!(
                    error = %e,
                    "failed to release a pulled message; it is redelivered once ack_wait expires"
                );
            }
        }
    };
    if tokio::time::timeout(RELEASE_TIMEOUT, release)
        .await
        .is_err()
    {
        tracing::error!(
            timeout = ?RELEASE_TIMEOUT,
            unstarted = unstarted.len(),
            "timed out releasing pulled messages; they are redelivered once ack_wait expires"
        );
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
// Concurrent worker
// ---------------------------------------------------------------------------

/// How long a worker's pull request waits on the server for messages. A pull
/// that ends is replaced as soon as the worker has a free slot, so this only
/// paces the pulls an idle topic costs, and it is how long an idle worker
/// goes before it probes its connection.
const PULL_EXPIRES: Duration = Duration::from_secs(30);

/// How many pulls a worker keeps open at once. A second pull asks for the
/// slots the worker's handlers free while the first pull's messages are still
/// arriving or running, so the pipe does not empty between pulls. Each pull
/// asks only for slots that are free, so the bound on the worker's in-flight
/// messages is unchanged, and a worker with a single slot keeps a single
/// pull.
const MAX_OPEN_PULLS: usize = 2;

/// One pull's deliveries, each paired with the processing slot reserved for
/// it when the pull was sized. The pairing ends with whichever side runs out
/// first, so no message is started without a slot.
type Deliveries = stream::Zip<Batch, stream::Iter<std::vec::IntoIter<OwnedSemaphorePermit>>>;

/// The first pause before a worker opens another pull after the server ended
/// one with a status. The pause doubles, up to [`ENDED_PULL_PAUSE_MAX`], while
/// that keeps happening, and a delivery resets it. The pulls already open go
/// on meanwhile.
const ENDED_PULL_PAUSE: Duration = Duration::from_millis(100);

/// The longest pause [`ENDED_PULL_PAUSE`] grows to.
const ENDED_PULL_PAUSE_MAX: Duration = Duration::from_secs(5);

fn ended_pull_backoff() -> Backoff {
    Backoff::new(ENDED_PULL_PAUSE, ENDED_PULL_PAUSE_MAX)
}

/// The next pause from `backoff`, whose iterator never ends.
fn next_pause(backoff: &mut Backoff) -> Duration {
    backoff.next().unwrap_or(ENDED_PULL_PAUSE_MAX)
}

/// One open pull of a worker.
struct Pull {
    deliveries: Deliveries,
    /// Whether it has delivered a message.
    delivered: bool,
    /// Whether it has ended, so [`next_event`] drops it.
    ended: bool,
}

impl Pull {
    fn new(batch: Batch, reserved: Vec<OwnedSemaphorePermit>) -> Self {
        Self {
            deliveries: batch.zip(stream::iter(reserved)),
            delivered: false,
            ended: false,
        }
    }
}

/// What [`next_event`] reports from a worker's open pulls.
// Returned once per event and never stored, so boxing would only add an
// allocation per message.
#[allow(clippy::large_enum_variant)]
enum PullEvent {
    /// A message, or the status the server ended its pull with, with the
    /// slot reserved for it.
    Delivery(
        std::result::Result<Message, async_nats::Error>,
        OwnedSemaphorePermit,
    ),
    /// A pull ended. It is already dropped, so the slots it reserved and did
    /// not fill are free again. `delivered` is whether it delivered a message.
    Ended { delivered: bool },
}

/// The next event of a worker's open pulls, never ready while there are
/// none. A pull that ends is dropped before its event is returned.
fn next_event(pulls: &mut Vec<Pull>) -> impl Future<Output = PullEvent> + '_ {
    std::future::poll_fn(move |cx| {
        let mut ended = None;
        for pull in pulls.iter_mut() {
            match pull.deliveries.poll_next_unpin(cx) {
                Poll::Ready(Some((delivery, slot))) => {
                    pull.delivered |= delivery.is_ok();
                    return Poll::Ready(PullEvent::Delivery(delivery, slot));
                }
                Poll::Ready(None) => {
                    pull.ended = true;
                    ended = Some(PullEvent::Ended {
                        delivered: pull.delivered,
                    });
                    break;
                }
                Poll::Pending => {}
            }
        }
        match ended {
            Some(event) => {
                pulls.retain(|pull| !pull.ended);
                Poll::Ready(event)
            }
            None => Poll::Pending,
        }
    })
}

/// What a worker's pull loop does next.
// Returned once per loop turn and never stored, like `PullEvent`.
#[allow(clippy::large_enum_variant)]
enum Step {
    Stop,
    Resume,
    Reserved(Result<Vec<OwnedSemaphorePermit>>),
    Event(PullEvent),
}

/// Waits out the pause before a worker's next pull, never ready without one.
async fn pause_over(pause: &mut Option<Pin<Box<tokio::time::Sleep>>>) {
    match pause {
        Some(sleep) => sleep.as_mut().await,
        None => std::future::pending().await,
    }
}

/// The message tasks of one worker that have started and not finished, held
/// retries included. The worker's `processing` flag, which tells a group's
/// scale-down which members are idle, is set while there is one. The flag is
/// written under the lock that guards the count, so the last task to finish
/// always leaves it clear.
struct BusyTasks {
    count: Mutex<usize>,
    processing: Arc<AtomicBool>,
    finished: Notify,
}

impl BusyTasks {
    fn new(processing: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            count: Mutex::new(0),
            processing,
            finished: Notify::new(),
        })
    }

    /// Counts a task until the returned guard is dropped.
    fn enter(self: &Arc<Self>) -> BusyTask {
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        *count = count.saturating_add(1);
        self.processing.store(true, Ordering::Release);
        BusyTask(Arc::clone(self))
    }

    fn running(&self) -> usize {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until no counted task is running.
    async fn idle(&self) {
        loop {
            // Registered before the count is read, so the last task's
            // notification cannot slip in between.
            let mut finished = std::pin::pin!(self.finished.notified());
            finished.as_mut().enable();
            if self.running() == 0 {
                return;
            }
            finished.await;
        }
    }
}

/// One task counted in [`BusyTasks`].
struct BusyTask(Arc<BusyTasks>);

impl Drop for BusyTask {
    fn drop(&mut self) {
        let mut count = self.0.count.lock().unwrap_or_else(PoisonError::into_inner);
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.0.processing.store(false, Ordering::Release);
            self.0.finished.notify_waiters();
        }
    }
}

/// Waits for a free processing slot, then takes every other free one: the
/// slots the next pull is sized to and reserves for its messages.
async fn reserve_free_slots(slots: &Arc<Semaphore>) -> Result<Vec<OwnedSemaphorePermit>> {
    let first = Arc::clone(slots)
        .acquire_owned()
        .await
        .map_err(|_| ShoveError::Connection("semaphore closed".to_string()))?;
    let mut reserved = Vec::with_capacity(slots.available_permits().saturating_add(1));
    reserved.push(first);
    while let Ok(slot) = Arc::clone(slots).try_acquire_owned() {
        reserved.push(slot);
    }
    Ok(reserved)
}

/// The messages a worker's open pulls have delivered and the worker has not
/// started, taken without waiting for more.
fn take_ready(pulls: &mut [Pull]) -> Vec<Message> {
    let mut unstarted = Vec::new();
    for pull in pulls.iter_mut() {
        while let Some(Some((delivery, _slot))) = pull.deliveries.next().now_or_never() {
            if let Ok(msg) = delivery {
                unstarted.push(msg);
            }
        }
    }
    unstarted
}

/// A concurrent consumer's worker: what every message task it starts shares.
///
/// The worker bounds its own in-flight work. It holds `prefetch` processing
/// slots, and each pull asks the server for as many messages as the worker
/// has free slots, reserving a slot for every message the pull may deliver.
/// So each delivered message starts at once, and none waits on the client
/// while its `ack_wait` runs. Up to [`MAX_OPEN_PULLS`] pulls are open at once,
/// each holding its own reserved slots. The durable needs no budget of its
/// own for that, and a durable shove creates has none (`max_ack_pending: -1`)
/// unless one is set: workers attached to one durable, from any number of
/// processes, each keep up to their own `prefetch` in flight.
///
/// A retry on a shove-owned stream is held un-acked for its delay and gives
/// up its processing slot for the hold, so the worker goes on with other
/// messages meanwhile. Holds are bounded on their own, at `prefetch` hold
/// slots: a retry that finds none free waits for one, keeping its processing
/// slot and renewing its `ack_wait`.
struct Worker<T: Topic, H: MessageHandler<T>> {
    client: NatsClient,
    topology: &'static QueueTopology,
    handler: H,
    ctx: H::Context,
    group: Option<Arc<str>>,
    max_retries: u32,
    retry_strategy: RetryStrategy,
    max_message_size: Option<usize>,
    handler_timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    slots: Arc<Semaphore>,
    holds: Semaphore,
    busy: Arc<BusyTasks>,
    shutdown: CancellationToken,
    _topic: PhantomData<fn() -> T>,
}

impl<T: Topic, H: MessageHandler<T>> Worker<T, H> {
    /// `None` when shutdown fires before `work` finishes.
    async fn unless_stopped<F: Future>(&self, work: F) -> Option<F::Output> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => None,
            out = work => Some(out),
        }
    }

    /// Pulls and starts messages on `consumer` until shutdown, which releases
    /// what the worker pulled and never started ([`release_pulled`]) and waits
    /// for its tasks, or until the connection to the server fails.
    async fn pull_until_stopped(
        self: &Arc<Self>,
        consumer: &PullConsumer,
        ack_wait: Duration,
    ) -> Result<()> {
        let queue = self.topology.queue();
        let mut pulls: Vec<Pull> = Vec::with_capacity(MAX_OPEN_PULLS);
        let mut quiet_since = tokio::time::Instant::now();
        let mut pause: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let mut pause_backoff = ended_pull_backoff();
        loop {
            let may_pull = pulls.len() < MAX_OPEN_PULLS && pause.is_none();
            let step = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => Step::Stop,
                () = pause_over(&mut pause), if pause.is_some() => Step::Resume,
                reserved = reserve_free_slots(&self.slots), if may_pull => Step::Reserved(reserved),
                event = next_event(&mut pulls), if !pulls.is_empty() => Step::Event(event),
            };
            match step {
                Step::Stop => return self.stop(consumer, pulls).await,
                Step::Resume => pause = None,
                Step::Reserved(reserved) => {
                    let reserved = reserved?;
                    let request = consumer
                        .batch()
                        .max_messages(reserved.len())
                        .expires(PULL_EXPIRES)
                        .messages();
                    let Some(batch) = self.unless_stopped(request).await else {
                        drop(reserved);
                        return self.stop(consumer, pulls).await;
                    };
                    let batch = batch.map_err(|e| {
                        ShoveError::Connection(format!("pull request on {queue} failed: {e}"))
                    })?;
                    pulls.push(Pull::new(batch, reserved));
                }
                Step::Event(PullEvent::Delivery(Ok(msg), slot)) => {
                    quiet_since = tokio::time::Instant::now();
                    pause_backoff = ended_pull_backoff();
                    self.start(msg, slot, ack_wait);
                }
                Step::Event(PullEvent::Delivery(Err(e), _)) => {
                    // The server ended the pull with a status: it refused it
                    // (`Exceeded MaxWaiting`), lost its cluster leader, or the
                    // durable is gone. Only a probe that fails makes that a
                    // connection error; otherwise the worker pulls again
                    // after a pause.
                    tracing::warn!(error = %e, queue, "the server ended a pull, probing the connection");
                    let Some(probe) = self.unless_stopped(consumer.get_info()).await else {
                        return self.stop(consumer, pulls).await;
                    };
                    if let Err(probe_err) = probe {
                        metrics::record_backend_error(
                            metrics::BackendLabel::Nats,
                            metrics::BackendErrorKind::Consume,
                        );
                        return Err(ShoveError::Connection(format!(
                            "a pull on {queue} ended with `{e}`, and the connection probe failed: {probe_err}"
                        )));
                    }
                    quiet_since = tokio::time::Instant::now();
                    pause = Some(Box::pin(tokio::time::sleep(next_pause(&mut pause_backoff))));
                }
                Step::Event(PullEvent::Ended { delivered }) => {
                    if delivered {
                        continue;
                    }
                    // A silent outage ends pulls exactly like an idle topic
                    // does, so a worker that has seen nothing for a whole
                    // pull's expiry probes its connection.
                    if quiet_since.elapsed() < PULL_EXPIRES {
                        continue;
                    }
                    let Some(probe) = self.unless_stopped(consumer.get_info()).await else {
                        return self.stop(consumer, pulls).await;
                    };
                    probe.map_err(|e| {
                        ShoveError::Connection(format!("liveness probe on {queue} failed: {e}"))
                    })?;
                    quiet_since = tokio::time::Instant::now();
                }
            }
        }
    }

    /// Starts `msg` in a task of its own, on the processing slot its pull
    /// reserved for it.
    fn start(self: &Arc<Self>, msg: Message, slot: OwnedSemaphorePermit, ack_wait: Duration) {
        let busy = self.busy.enter();
        let worker = Arc::clone(self);
        tokio::spawn(async move {
            worker.process(msg, slot, ack_wait).await;
            drop(busy);
        });
    }

    /// Releases what the worker's open pulls delivered and it never started,
    /// then waits for the tasks it did start. The pulls are dropped first, so
    /// the slots they reserved are free before the wait.
    async fn stop(&self, consumer: &PullConsumer, mut pulls: Vec<Pull>) -> Result<()> {
        let unstarted = take_ready(&mut pulls);
        drop(pulls);
        release_pulled(consumer, &unstarted).await;
        self.drain().await;
        Ok(())
    }

    /// Waits until every task the worker started has finished, held retries
    /// included. It waits on the tasks themselves, so a slot the caller still
    /// holds cannot hold it up.
    async fn drain(&self) {
        tracing::info!(
            queue = self.topology.queue(),
            "shutdown signal received, draining in-flight tasks"
        );
        self.busy.idle().await;
    }

    /// Size-checks and decodes `msg`, runs the handler on it and settles its
    /// outcome.
    async fn process(
        self: Arc<Self>,
        msg: Message,
        slot: OwnedSemaphorePermit,
        ack_wait: Duration,
    ) {
        let queue = self.topology.queue();
        let group = self.group.as_deref();
        metrics::record_message_size(queue, group, msg.payload.len());

        if let Err(e) = validate_message_size(msg.payload.len(), self.max_message_size) {
            tracing::warn!(error = %e, queue, "rejecting oversized message to DLQ");
            metrics::record_failed(queue, group, metrics::FailReason::Oversize);
            self.settle_unhandled(&msg, &e.to_string(), ack_wait).await;
            return;
        }

        let payload: T::Message =
            match <T::Codec as crate::Codec<T::Message>>::decode_owned(msg.payload.clone()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        queue,
                        "failed to deserialize message, sending to DLQ"
                    );
                    metrics::record_failed(queue, group, metrics::FailReason::Deserialize);
                    self.settle_unhandled(&msg, "deserialization_error", ack_wait)
                        .await;
                    return;
                }
            };

        let mut metadata = extract_message_metadata(&msg);
        if self.retry_strategy == RetryStrategy::InPlace {
            metadata.retry_count = in_place_retry_count(&metadata);
        }
        let retry_count = metadata.retry_count;

        let worker = Arc::clone(&self);
        let outcome = invoke_handler(
            async move { worker.handler.handle(payload, metadata, &worker.ctx).await },
            self.handler_timeout,
            self.timeout_outcome.clone(),
            queue,
            group,
        )
        .await;

        self.route_outcome(&msg, outcome, retry_count, slot, ack_wait)
            .await;
    }

    /// Dead-letters a message the handler never saw, oversized or
    /// undecodable, and acks it, or naks it for redelivery when the dead
    /// letter cannot be published. Like the other NATS pre-handler drops it
    /// counts only the failure the caller recorded, no discard.
    async fn settle_unhandled(&self, msg: &Message, reason: &str, ack_wait: Duration) {
        let dead_letter = publish_to_dlq(&self.client, self.topology, msg, reason);
        match renewing(msg, ack_wait, dead_letter).await {
            Ok(()) => {
                if let Err(e) = msg.ack().await {
                    tracing::error!(error = %e, "failed to ack message after DLQ publish");
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to publish a message the handler never saw to the DLQ, nak-ing"
                );
                if let Err(nak_err) = msg.ack_with(AckKind::Nak(e.redelivery_delay())).await {
                    tracing::error!(error = %nak_err, "failed to nak message after DLQ publish failure");
                }
            }
        }
    }

    /// Settles `msg` by the outcome its handler returned. `slot` is the
    /// message's processing slot: a held retry gives it up for the hold, and
    /// every other outcome keeps it until the message is settled.
    async fn route_outcome(
        &self,
        msg: &Message,
        outcome: Outcome,
        retry_count: u32,
        slot: OwnedSemaphorePermit,
        ack_wait: Duration,
    ) {
        let topology = self.topology;
        let in_place = self.retry_strategy == RetryStrategy::InPlace;
        let result: PublishResult = match decide_retry(&outcome, retry_count, self.max_retries) {
            RetryDecision::Ack => {
                if let Err(e) = msg.ack().await {
                    tracing::error!(error = %e, "failed to ack message");
                }
                return;
            }
            RetryDecision::Dlq { reason } => {
                let pending = metrics::record_terminal(
                    topology.queue(),
                    self.group.as_deref(),
                    dlq_fail_reason(reason),
                    topology.dlq().is_some(),
                );
                let stamped = in_place.then_some(retry_count);
                let dead_letter = publish_dead_letter(&self.client, topology, msg, reason, stamped);
                match renewing(msg, ack_wait, dead_letter).await {
                    Ok(()) => {
                        // The ack is what retires the message; until it lands
                        // JetStream still owns the delivery and will redeliver
                        // on ack-wait expiry, so a failed ack is not a discard.
                        //
                        // `double_ack` rather than `ack` because this is the
                        // arm that decides whether a discard actually
                        // happened. `ack` only publishes `+ACK` to the client
                        // connection and returns as soon as it is written; a
                        // connection lost before JetStream applies it
                        // redelivers the message while `confirm` has already
                        // counted it gone. `double_ack` waits for the server's
                        // reply, so `Ok` means the stream really did retire
                        // the delivery. The extra round trip is paid only on
                        // terminal outcomes (rejected or retries exhausted),
                        // not on the happy path above.
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
                let delay = hold_delay(topology.hold_queues(), retry_count);

                if in_place {
                    // An external stream is never written by its consumer:
                    // the retry is the same delayed negative acknowledgement a
                    // `Defer` uses, and the redelivery it triggers increments
                    // the `num_delivered` that `in_place_retry_count` reads.
                    if let Err(e) = msg.ack_with(AckKind::Nak(Some(delay))).await {
                        tracing::error!(error = %e, "failed to nak-with-delay for in-place retry");
                    }
                    return;
                }

                // Hold the original un-acked through the backoff, then durably
                // republish an incremented copy and ack — see
                // `hold_then_republish`. Keeps at-least-once across crashes and
                // republish failures while leaving the retry count header-based.
                let Some(_hold) = self.take_hold_slot(msg, ack_wait).await else {
                    return;
                };
                drop(slot);
                hold_then_republish(
                    &self.client,
                    msg,
                    delay,
                    retry_count.saturating_add(1),
                    ack_wait,
                    &self.shutdown,
                )
                .await;
                return;
            }
            RetryDecision::Hold { increment: false } => {
                let delay = hold_delay(topology.hold_queues(), 0);
                if let Err(e) = msg.ack_with(AckKind::Nak(Some(delay))).await {
                    tracing::error!(error = %e, "failed to nak-with-delay for defer");
                }
                return;
            }
        };

        // If we reach here, a routing operation failed — nak the message for redelivery.
        if let Err(e) = result {
            tracing::error!(error = %e, "routing failed, nak-ing message for redelivery");
            if let Err(nak_err) = msg.ack_with(AckKind::Nak(e.redelivery_delay())).await {
                tracing::error!(error = %nak_err, "failed to nak message after routing failure");
            }
        }
    }

    /// A hold slot for `msg`'s retry delay. When every slot is taken it waits
    /// for one, renewing `msg`'s `ack_wait` meanwhile and once more on taking
    /// it. `None` when shutdown fired first, `msg` still un-acked.
    async fn take_hold_slot(
        &self,
        msg: &Message,
        ack_wait: Duration,
    ) -> Option<SemaphorePermit<'_>> {
        if let Ok(hold) = self.holds.try_acquire() {
            return Some(hold);
        }
        let hold = self
            .unless_stopped(renewing(msg, ack_wait, self.holds.acquire()))
            .await?
            .ok()?;
        if let Err(e) = msg.ack_with(AckKind::Progress).await {
            tracing::warn!(error = %e, "failed to renew the ack_wait of a retry that waited for a hold slot");
        }
        Some(hold)
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
        options.refuse_broadcast_start(queue, "NatsConsumer::run")?;
        // Every worker of a topic, in any process and in any group member,
        // binds to the same durable consumer name; the JetStream server
        // load-balances messages across their pulls, and each worker bounds
        // its own in-flight work (`Worker`).
        let consumer_name = super::constants::consumer_name(queue);

        // Zero is lowered to one, as `ConsumerOptions::with_prefetch_count`
        // documents.
        let prefetch = options.prefetch_count.max(1);
        let retry_strategy = resolve_retry_strategy(topology);
        let max_ack_pending = options.max_ack_pending.unwrap_or(UNBOUNDED_ACK_PENDING);
        let derived_ack_wait =
            derive_ack_wait(options.handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT));

        tracing::info!(
            queue,
            consumer = consumer_name,
            prefetch_count = prefetch,
            max_ack_pending,
            max_retries = options.max_retries,
            retry_strategy = ?retry_strategy,
            "NATS consumer started"
        );

        let slots = usize::from(prefetch);
        let worker = Arc::new(Worker::<T, H> {
            client: self.client.clone(),
            topology,
            handler,
            ctx,
            group: options.consumer_group.clone(),
            max_retries: options.max_retries,
            retry_strategy,
            max_message_size: options.max_message_size,
            handler_timeout: options.handler_timeout,
            timeout_outcome: options.handler_timeout_outcome.clone(),
            slots: Arc::new(Semaphore::new(slots)),
            holds: Semaphore::new(slots),
            busy: BusyTasks::new(options.processing.clone()),
            shutdown: options.shutdown.clone(),
            _topic: PhantomData,
        });

        run_with_reconnect(
            &options.shutdown,
            queue,
            options.max_reconnect_attempts,
            || {
                let worker = Arc::clone(&worker);
                let consumer_name = consumer_name.clone();
                async move {
                    let stream = worker
                        .client
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
                            let is_not_found =
                                e.downcast_ref::<ConsumerInfoError>().is_some_and(|ce| {
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

                    let config = &pull_consumer.cached_info().config;
                    // Effective server-side ack_wait, used to pace the progress acks
                    // that keep a waiting message from being redelivered.
                    let ack_wait = config.ack_wait;
                    // The fallback only ever creates the durable, so a budget an
                    // earlier version gave it stays until it is changed.
                    let in_force = config.max_ack_pending;
                    if in_force > 0 && max_ack_pending == UNBOUNDED_ACK_PENDING {
                        tracing::warn!(
                            queue,
                            consumer = consumer_name,
                            max_ack_pending = in_force,
                            "the durable caps the un-acked messages of every worker attached \
                             to it at its max_ack_pending, a budget this consumer did not ask \
                             for, such as one an earlier version created the durable with; \
                             lift it in place with `nats consumer edit {queue} {consumer_name} \
                             --max-pending=-1`"
                        );
                    }

                    worker.pull_until_stopped(&pull_consumer, ack_wait).await
                }
            },
        )
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
        options.refuse_broadcast_start(queue, "NatsConsumer::run_fifo")?;
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

                        let fifo_shard = FifoShard {
                            client: &shard_client,
                            topology,
                            handler: &shard_handler,
                            ctx: &shard_ctx,
                            poisoned: &shard_poisoned,
                            processing: &shard_processing,
                            topic: &shard_topic,
                            group: shard_group.as_deref(),
                            shard,
                            max_retries,
                            max_message_size,
                            handler_timeout,
                            timeout_outcome: &handler_timeout_outcome_cfg,
                            ack_wait,
                            shutdown: &shard_shutdown,
                        };

                        loop {
                            // Biased, so no record is taken once shutdown fired.
                            let item = tokio::select! {
                                biased;
                                _ = shard_shutdown.cancelled() => {
                                    tracing::info!(shard, "shutdown signal received, stopping shard consumer");
                                    return Ok(());
                                }
                                item = messages.next() => item,
                            };
                            let msg = match item {
                                Some(Ok(msg)) => msg,
                                Some(Err(e)) => {
                                    tracing::error!(error = %e, shard, "shard consumer stream error");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(format!(
                                        "shard {shard} stream error: {e}"
                                    )));
                                }
                                None => {
                                    tracing::warn!(shard, "shard consumer stream closed");
                                    metrics::record_backend_error(
                                        metrics::BackendLabel::Nats,
                                        metrics::BackendErrorKind::Consume,
                                    );
                                    return Err(ShoveError::Connection(format!(
                                        "shard {shard} stream closed"
                                    )));
                                }
                            };

                            metrics::record_message_size(
                                &shard_topic,
                                shard_group.as_deref(),
                                msg.payload.len(),
                            );

                            if let ShardStep::Stopped { redelivery } =
                                settle_sequenced::<T, H>(&fifo_shard, &msg).await
                            {
                                tracing::info!(
                                    shard,
                                    "shutdown signal received while a message was held, releasing it"
                                );
                                drop(messages);
                                release_held(&pull_consumer, &msg, redelivery).await;
                                return Ok(());
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
                settle_poison(ctx, &msg, "deserialization_error").await;
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
        if let Err(e) = msg.ack_with(AckKind::Nak(dlq_err.redelivery_delay())).await {
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
/// A failed DLQ publish leaves the message un-acked and Naks it for
/// redelivery, at once or, when the dead letter's id is held by a message
/// that is not this copy, after a second; nothing is ever discarded without a
/// copy landing first.
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
                    if let Err(nak_err) = msg.ack_with(AckKind::Nak(e.redelivery_delay())).await {
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
    /// nothing pre-declared that durable — leaves `max_ack_pending`
    /// **unbounded** (`-1`) and sizes `ack_wait` to [`derive_batch_ack_wait`].
    /// Unbounded is deliberate: JetStream serves every concurrent pull request
    /// on the durable from that one budget, and any finite value invites the
    /// pathology where N attached workers split it, no request can fill to
    /// `max_messages`, and every partial pull sits open until `expires` —
    /// collapsing throughput to `budget / max_batch_age` while the stream is
    /// full. In-flight exposure stays bounded without the budget's help: in
    /// steady state each worker keeps at most one outstanding pull of at most
    /// `max_batch_size` messages and settles it at flush. The exception is a
    /// flapping connection — the server keeps serving an abandoned pull
    /// request until its `expires`, and with no budget to cap it those
    /// messages stay ack-pending for the full `ack_wait`. The failed cycle
    /// Naks everything it actually received (see the `stream_error` arm) and
    /// `run_with_reconnect` backs off between cycles, so the accumulation is
    /// bounded by that rate rather than unbounded — but it is more than one
    /// batch per worker while a server is flapping.
    ///
    /// A pre-declared durable keeps whatever the registry gave it; since the
    /// server stops delivering at the unacked budget and this loop acks only
    /// at flush, the per-pull size is **clamped to the durable's
    /// `max_ack_pending`** (when positive and smaller) with a startup warning
    /// — the clamp plus the request expiry guarantees forward progress in
    /// ≤budget-sized batches rather than a silent deadlock. A budget that
    /// covers only a single batch (this includes durables created by the
    /// pre-unbounded bootstrap, which persist server-side across an upgrade)
    /// draws its own warning: one worker runs fine, but concurrent workers
    /// split it and flushes go expiry-bound — recreate the durable or raise
    /// its budget for multi-worker use. A further warning fires when the
    /// durable's `ack_wait` is below the batch window plus the
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
                                // Unbounded on purpose — see the doc block: a
                                // finite budget is split across every attached
                                // worker's pull request, and a request that
                                // cannot fill to `max_messages` waits out the
                                // whole `expires` window even when the stream
                                // has supply. Flight stays bounded by the pull
                                // requests themselves (≤ one batch per worker).
                                max_ack_pending: UNBOUNDED_ACK_PENDING,
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
                             window; the budget is shared by every attached worker, so \
                             size it at max_batch_size x workers to keep flushes \
                             supply-bound rather than expiry-bound"
                        );
                        b
                    }
                    Ok(b) if b >= max_batch_size && b < max_batch_size.saturating_mul(2) => {
                        // The band is `[size, 2*size)` because that is where a
                        // *second* worker provably cannot fill: two concurrent
                        // pulls need 2*size of budget between them. It also
                        // catches the durable this bootstrap wrote before it
                        // went unbounded (exactly one batch of budget, which
                        // persists server-side across an upgrade). A budget at
                        // or above 2*size is left alone even though it is still
                        // too small for enough workers — the consumer cannot
                        // know how many will attach, so the warning names the
                        // rule instead of guessing the count.
                        tracing::warn!(
                            queue,
                            max_batch_size,
                            max_ack_pending = budget,
                            "the durable's max_ack_pending covers only a single \
                             batch, and it is shared by every attached worker: \
                             fine for one, but a second worker's concurrent pull \
                             splits the budget so neither can fill and both \
                             flushes go expiry-bound. Size the budget at \
                             max_batch_size x workers (or recreate the durable to \
                             take shove's unbounded default) before running more \
                             than one batch worker"
                        );
                        max_batch_size
                    }
                    // Zero/negative budget means unbounded, and a budget at or
                    // above twice the batch size fits at least two concurrent
                    // pulls, so it needs no clamp and no warning.
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
                    let mut stream_error: Option<ShoveError> = None;

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
                                    stream_error = Some(ShoveError::Connection(format!(
                                        "batch pull stream error on {queue}: {e}"
                                    )));
                                    break;
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

                    if let Some(err) = stream_error {
                        // This cycle took delivery of `acks` but never reached
                        // the flush, so nothing handled them and nothing
                        // settled them. `run_with_reconnect` retries, and
                        // without the budget that used to cap it (the durable
                        // is unbounded now) every failed cycle inside one
                        // `ack_wait` window would leave another batch parked
                        // ack-pending until that window expired. Nak instead:
                        // best-effort, at-least-once holds either way, and the
                        // retry sees them immediately. Same shape as the
                        // shutdown arm below.
                        for msg in acks {
                            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                tracing::error!(
                                    error = %e,
                                    queue,
                                    "failed to nak an unhandled batch message after a \
                                     pull stream error; it redelivers on ack_wait"
                                );
                            }
                        }
                        return Err(err);
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
mod retry_strategy_tests {
    use super::*;
    use crate::TopologyBuilder;

    fn metadata(delivery_count: Option<u32>) -> MessageMetadata {
        MessageMetadata {
            retry_count: 0,
            delivery_id: String::new(),
            redelivered: false,
            delivery_count,
            partition: None,
            offset: None,
            timestamp_ms: None,
            headers: Arc::new(HashMap::new()),
        }
    }

    #[test]
    fn ownership_decides_the_strategy() {
        let owned = TopologyBuilder::new("nats-owned").build();
        let external = TopologyBuilder::new("nats-external").external().build();
        assert_eq!(resolve_retry_strategy(&owned), RetryStrategy::Republish);
        assert_eq!(resolve_retry_strategy(&external), RetryStrategy::InPlace);
    }

    #[test]
    fn in_place_count_is_the_redelivery_count() {
        assert_eq!(in_place_retry_count(&metadata(None)), 0);
        assert_eq!(in_place_retry_count(&metadata(Some(1))), 0);
        assert_eq!(in_place_retry_count(&metadata(Some(4))), 3);
        assert_eq!(
            in_place_retry_count(&metadata(Some(u32::MAX))),
            u32::MAX - 1
        );
    }

    fn sequenced(header_count: u32, delivery_count: Option<u32>) -> u32 {
        let mut metadata = metadata(delivery_count);
        metadata.retry_count = header_count;
        sequenced_retry_count(&metadata)
    }

    #[test]
    fn a_sequenced_count_spends_a_retry_per_earlier_delivery() {
        assert_eq!(
            sequenced(0, Some(1)),
            0,
            "a first delivery keeps the header's count"
        );
        assert_eq!(sequenced(2, Some(1)), 2);
        assert_eq!(sequenced(0, Some(3)), 2, "two hand-offs spend two retries");
        assert_eq!(sequenced(2, Some(4)), 5);
        assert_eq!(
            sequenced(3, None),
            3,
            "no delivery info keeps the header's count"
        );
    }

    #[test]
    fn a_sequenced_count_saturates() {
        assert_eq!(sequenced(u32::MAX, Some(5)), u32::MAX);
        assert_eq!(sequenced(1, Some(u32::MAX)), u32::MAX);
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

    #[test]
    fn a_huge_handler_timeout_saturates() {
        assert_eq!(derive_ack_wait(Duration::MAX), Duration::MAX);
    }
}

/// [`progress_interval`]: how often a held sequenced record renews its
/// `ack_wait`.
#[cfg(test)]
mod sequenced_hold_tests {
    use super::*;

    #[test]
    fn a_held_record_is_renewed_three_times_per_ack_wait() {
        assert_eq!(
            progress_interval(Duration::from_secs(30)),
            Duration::from_secs(10)
        );
        assert_eq!(
            progress_interval(Duration::from_secs(90)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn a_zero_ack_wait_is_the_server_default() {
        assert_eq!(progress_interval(Duration::ZERO), Duration::from_secs(10));
    }
}

/// [`reserve_free_slots`] and [`BusyTasks`]: how a concurrent worker sizes its
/// pulls and reports itself busy.
#[cfg(test)]
mod worker_flow_tests {
    use super::*;

    #[tokio::test]
    async fn a_pull_reserves_every_free_slot() {
        let slots = Arc::new(Semaphore::new(4));
        let running = Arc::clone(&slots).try_acquire_owned().unwrap();
        let reserved = reserve_free_slots(&slots).await.unwrap();
        assert_eq!(reserved.len(), 3, "the three free slots");
        assert_eq!(slots.available_permits(), 0);
        drop(reserved);
        assert_eq!(
            slots.available_permits(),
            3,
            "the slots a pull did not fill go back"
        );
        drop(running);
    }

    #[tokio::test]
    async fn a_pull_waits_for_a_free_slot() {
        let slots = Arc::new(Semaphore::new(1));
        let running = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut reserve = std::pin::pin!(reserve_free_slots(&slots));
        assert!(
            reserve.as_mut().now_or_never().is_none(),
            "no free slot, no pull"
        );
        drop(running);
        assert_eq!(reserve.await.unwrap().len(), 1);
    }

    #[test]
    fn the_processing_flag_is_set_until_the_last_task_finishes() {
        let processing = Arc::new(AtomicBool::new(false));
        let busy = BusyTasks::new(Arc::clone(&processing));
        let first = busy.enter();
        let second = busy.enter();
        assert!(processing.load(Ordering::Acquire));
        drop(first);
        assert!(
            processing.load(Ordering::Acquire),
            "one task is still running"
        );
        drop(second);
        assert!(!processing.load(Ordering::Acquire));
        let third = busy.enter();
        assert!(
            processing.load(Ordering::Acquire),
            "a new task sets it again"
        );
        drop(third);
        assert!(!processing.load(Ordering::Acquire));
    }

    /// A drain waits on the tasks themselves, so it ends once the last one
    /// finishes whatever slots are still held elsewhere.
    #[tokio::test]
    async fn idle_waits_for_the_last_task() {
        let busy = BusyTasks::new(Arc::new(AtomicBool::new(false)));
        assert!(busy.idle().now_or_never().is_some(), "nothing is running");
        let first = busy.enter();
        let second = busy.enter();
        let mut idle = std::pin::pin!(busy.idle());
        assert!(idle.as_mut().now_or_never().is_none());
        drop(first);
        assert!(
            idle.as_mut().now_or_never().is_none(),
            "one task is still running"
        );
        drop(second);
        assert!(idle.as_mut().now_or_never().is_some());
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

#[cfg(test)]
mod metadata_coordinates_tests {
    use super::*;

    /// The stream sequence is `u64` and `offset` is `i64`: the whole
    /// representable range passes through unchanged, and anything past it
    /// reads as unknown instead of wrapping negative.
    #[test]
    fn a_stream_sequence_becomes_a_checked_offset() {
        assert_eq!(sequence_to_offset(1), Some(1));
        assert_eq!(sequence_to_offset(i64::MAX as u64), Some(i64::MAX));
        assert_eq!(sequence_to_offset(i64::MAX as u64 + 1), None);
        assert_eq!(sequence_to_offset(u64::MAX), None);
    }

    /// `published` arrives as nanoseconds; the field is milliseconds, floored,
    /// and checked into an `i64`.
    #[test]
    fn a_published_instant_becomes_unix_milliseconds() {
        assert_eq!(published_to_millis(0), Some(0));
        assert_eq!(
            published_to_millis(1_700_000_000_123_456_789),
            Some(1_700_000_000_123)
        );
        assert_eq!(
            published_to_millis(-1),
            Some(-1),
            "floor, not truncation toward zero"
        );
        assert_eq!(
            published_to_millis(i128::from(i64::MAX) * 1_000_000),
            Some(i64::MAX)
        );
        assert_eq!(
            published_to_millis((i128::from(i64::MAX) + 1) * 1_000_000),
            None
        );
    }
}

#[cfg(test)]
mod copy_header_tests {
    use super::*;

    fn values<'a>(headers: &'a HeaderMap, name: &str) -> Vec<&'a str> {
        headers.get_all(name).map(HeaderValue::as_str).collect()
    }

    /// A copy replays none of the JetStream directives aimed at the
    /// original's own publish, and keeps every other header, every value of a
    /// repeated one included.
    #[test]
    fn a_copy_drops_publish_directives_and_keeps_the_rest() {
        let mut original = HeaderMap::new();
        original.insert("Nats-Expected-Stream", "orders");
        original.insert("Nats-Expected-Last-Sequence", "7");
        original.insert("Nats-Expected-Last-Subject-Sequence", "3");
        original.insert("Nats-Expected-Last-Msg-Id", "previous");
        original.insert("Nats-Rollup", "sub");
        original.insert("Nats-TTL", "1s");
        original.insert("Nats-Batch-Id", "batch-1");
        original.insert("Nats-Batch-Sequence", "1");
        original.insert("Nats-Batch-Commit", "1");
        original.insert("Nats-Schedule", "@hourly");
        original.insert("Nats-Schedule-Target", "orders");
        original.insert("Nats-Scheduler", "orders.schedule");
        original.insert("Nats-Required-Api-Level", "2");
        original.insert("nats-expected-stream", "orders");
        original.insert("NATS-ROLLUP", "sub");
        original.insert("nats-ttl", "1s");
        original.insert(NATS_MESSAGE_ID, "original");
        original.insert(RETRY_COUNT_HEADER, "2");
        original.insert("x-trace-id", "trace-1");
        original.append("x-multi", "one");
        original.append("x-multi", "two");

        let copy = copy_headers(Some(&original));

        for directive in [
            "Nats-Expected-Stream",
            "Nats-Expected-Last-Sequence",
            "Nats-Expected-Last-Subject-Sequence",
            "Nats-Expected-Last-Msg-Id",
            "Nats-Rollup",
            "Nats-TTL",
            "Nats-Batch-Id",
            "Nats-Batch-Sequence",
            "Nats-Batch-Commit",
            "Nats-Schedule",
            "Nats-Schedule-Target",
            "Nats-Scheduler",
            "Nats-Required-Api-Level",
            "nats-expected-stream",
            "NATS-ROLLUP",
            "nats-ttl",
        ] {
            assert!(
                copy.get(directive).is_none(),
                "{directive} is a directive for the original's publish"
            );
        }
        assert_eq!(values(&copy, "Nats-Msg-Id"), ["original"]);
        assert_eq!(values(&copy, RETRY_COUNT_HEADER), ["2"]);
        assert_eq!(values(&copy, "x-trace-id"), ["trace-1"]);
        assert_eq!(values(&copy, "x-multi"), ["one", "two"]);
        assert_eq!(copy.len(), 4);
    }

    #[test]
    fn a_message_without_headers_copies_to_none() {
        assert!(copy_headers(None).is_empty());
    }
}

#[cfg(test)]
mod copy_id_tests {
    use super::*;

    /// A copy of a message with a `Nats-Msg-Id` builds on that id, so
    /// `delivery_id` keeps its lineage.
    #[test]
    fn a_copy_extends_the_original_id() {
        let origin = copy_origin_of("orders", "orders-consumer", 7);
        assert_eq!(copy_message_id(Some("abc"), &origin, "dlq"), "abc-dlq");
        assert_eq!(copy_message_id(Some("abc"), &origin, "r2"), "abc-r2");
    }

    /// Without an id of its own a copy is named after the delivery it was
    /// made from, and an empty id counts as none.
    #[test]
    fn a_copy_of_a_message_without_an_id_is_named_after_its_delivery() {
        let origin = copy_origin_of("orders", "orders-consumer", 7);
        assert_eq!(origin, "orders.orders-consumer.7");
        assert_eq!(
            copy_message_id(None, &origin, "dlq"),
            "orders.orders-consumer.7-dlq"
        );
        assert_eq!(
            copy_message_id(Some(""), &origin, "r1"),
            "orders.orders-consumer.7-r1"
        );
    }

    /// Two consumers dead-lettering one record into a shared DLQ. Without an
    /// original id their dead letters get different ids. With one they share
    /// the id, and their different origins are what stop the second from
    /// being taken for the first when JetStream reports it a duplicate.
    #[test]
    fn two_consumers_of_one_record_make_distinct_copies() {
        let billing = copy_origin_of("orders", "billing-consumer", 7);
        let audit = copy_origin_of("orders", "audit-consumer", 7);
        assert_ne!(
            copy_message_id(None, &billing, "dlq"),
            copy_message_id(None, &audit, "dlq")
        );
        assert_eq!(
            copy_message_id(Some("abc"), &billing, "dlq"),
            copy_message_id(Some("abc"), &audit, "dlq")
        );
        assert_ne!(billing, audit);
    }
}
