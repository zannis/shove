use lapin::Channel;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::{AMQPValue, FieldTable};
use tracing::{debug, error, warn};

use uuid::Uuid;

use crate::backends::rabbitmq::headers::{MESSAGE_ID_KEY, RETRY_COUNT_KEY};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::routing::hold_index;
use crate::topology::QueueTopology;

pub(crate) async fn route_ack(delivery: &Delivery, publisher: &ChannelPublisher) -> Result<()> {
    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
        error!("failed to ack delivery: {e}");
    }
    if let Err(e) = publisher.commit_if_tx().await {
        error!("tx_commit failed after ack: {e}");
        return Err(ShoveError::Connection(format!(
            "tx_commit failed after ack: {e}"
        )));
    }
    Ok(())
}

pub(crate) async fn route_retry(
    delivery: &Delivery,
    payload: &[u8],
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    retry_count: u32,
) -> Result<()> {
    let new_retry_count = retry_count + 1;
    let hold_queues = topology.hold_queues();

    if !hold_queues.is_empty() {
        let index = hold_index(retry_count, hold_queues.len());
        let hold_queue = &hold_queues[index];
        let headers = clone_headers_with_retry(delivery, new_retry_count);

        match publisher
            .publish_to_queue(hold_queue.name(), payload, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to hold queue: {e}");
                    // Ack failed while publish is buffered in tx — roll back the
                    // buffered publish so no duplicate ends up in the hold queue.
                    publisher.rollback_if_tx().await;
                    nack_requeue(delivery, publisher).await.ok();
                    return Ok(());
                }
                if let Err(e) = publisher.commit_if_tx().await {
                    error!("tx_commit failed for retry (attempt {new_retry_count}): {e}");
                    // tx_commit failure means neither publish nor ack happened;
                    // delivery remains unacked and will be redelivered by the broker.
                    return Err(ShoveError::Connection(format!(
                        "tx_commit failed for retry: {e}"
                    )));
                }
                debug!(
                    "retrying message via hold queue {} (attempt {})",
                    hold_queue.name(),
                    new_retry_count
                );
            }
            Err(e) => {
                warn!(
                    "failed to publish to hold queue {}, requeuing: {e}",
                    hold_queue.name()
                );
                nack_requeue(delivery, publisher).await.ok();
            }
        }
    } else {
        warn!(
            queue = topology.queue(),
            retry_count, "retrying message but no hold queues configured — requeuing with no delay"
        );
        nack_requeue(delivery, publisher).await.ok();
    }
    Ok(())
}

pub(crate) async fn route_defer(
    delivery: &Delivery,
    payload: &[u8],
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
) -> Result<()> {
    let hold_queues = topology.hold_queues();

    if !hold_queues.is_empty() {
        let hold_queue = &hold_queues[0];
        let headers = clone_headers(delivery);

        match publisher
            .publish_to_queue(hold_queue.name(), payload, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after deferring to hold queue: {e}");
                    publisher.rollback_if_tx().await;
                    nack_requeue(delivery, publisher).await.ok();
                    return Ok(());
                }
                if let Err(e) = publisher.commit_if_tx().await {
                    error!("tx_commit failed for defer: {e}");
                    return Err(ShoveError::Connection(format!(
                        "tx_commit failed for defer: {e}"
                    )));
                }
                debug!("deferring message to hold queue {}", hold_queue.name());
            }
            Err(e) => {
                warn!(
                    "failed to publish to hold queue {} for defer, requeuing: {e}",
                    hold_queue.name()
                );
                nack_requeue(delivery, publisher).await.ok();
            }
        }
    } else {
        warn!(
            queue = topology.queue(),
            "deferring message but no hold queues configured — requeuing with no delay"
        );
        nack_requeue(delivery, publisher).await.ok();
    }
    Ok(())
}

/// Terminal routing: nack without requeue, so the broker moves the delivery to
/// the queue's dead-letter exchange — or, with no DLX bound, drops it.
///
/// The terminal metric is recorded here rather than at the call sites. This
/// consumer hand-rolls its retry-budget checks at a dozen-odd places (standard,
/// sharded, concurrent-sequenced, buffered-pending, AwaitingRetry timeout,
/// FailAll cascade, pre-handler rejects), and instrumenting them individually
/// is how the discard counter came to miss half of them. Every path that gives
/// up on a delivery ends here, so `reason` is a required argument: a new
/// terminal path cannot be added without accounting for it.
///
/// The discard half of that metric is held until the broker has accepted the
/// nack — and, on a transactional channel, until the commit that makes it
/// durable. A nack that fails on a closing channel leaves the delivery
/// unacknowledged, so the broker requeues it on close and the message is very
/// much still alive; counting a discard there would report data loss during
/// precisely the connection failure an operator is trying to diagnose.
pub(crate) async fn route_reject(
    delivery: &Delivery,
    topology: &QueueTopology,
    publisher: &ChannelPublisher,
    group: Option<&str>,
    reason: metrics::FailReason,
) -> Result<()> {
    let pending =
        metrics::record_terminal(topology.queue(), group, reason, topology.dlq().is_some());
    reject_with(delivery, topology, publisher, pending).await
}

/// [`route_reject`] for a delivery being dead-lettered as collateral of a
/// failure that has already been counted — a [`SequenceFailure::FailAll`]
/// cascade behind a poisoned key.
///
/// Identical routing; the only difference is that it does not increment
/// `messages_failed_total`, because the cascade's size is queue depth rather
/// than a count of things that went wrong. See [`metrics::FailReason`].
///
/// It is a distinct function rather than a flag on `route_reject` so the
/// call-site choice stays explicit, in the same spirit as `reason` being
/// mandatory: a cascade site cannot be added by forgetting an argument.
///
/// [`SequenceFailure::FailAll`]: crate::topology::SequenceFailure
pub(crate) async fn route_reject_cascade(
    delivery: &Delivery,
    topology: &QueueTopology,
    publisher: &ChannelPublisher,
    group: Option<&str>,
    reason: metrics::FailReason,
) -> Result<()> {
    let pending =
        metrics::pending_discard(topology.queue(), group, reason, topology.dlq().is_some());
    reject_with(delivery, topology, publisher, pending).await
}

/// Shared nack/commit mechanics for both reject entry points. Takes the
/// already-decided discard so the accounting choice stays with the caller.
async fn reject_with(
    delivery: &Delivery,
    topology: &QueueTopology,
    publisher: &ChannelPublisher,
    pending: metrics::PendingDiscard,
) -> Result<()> {
    if topology.dlq().is_none() {
        warn!(
            queue = topology.queue(),
            "rejecting message on queue with no DLQ configured — message will be discarded"
        );
    }
    if let Err(e) = delivery
        .nack(BasicNackOptions {
            requeue: false,
            ..BasicNackOptions::default()
        })
        .await
    {
        error!("failed to nack-reject delivery: {e}");
        // Unacknowledged: the broker requeues it when the channel closes.
        pending.survived();
        return Ok(());
    }
    if let Err(e) = publisher.commit_if_tx().await {
        error!("tx_commit failed after reject: {e}");
        // On a transactional channel the nack only takes effect at commit, so
        // a failed commit rolls it back and the delivery is redelivered.
        pending.survived();
        return Err(ShoveError::Connection(format!(
            "tx_commit failed after reject: {e}"
        )));
    }
    pending.confirm();
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch settling — `multiple: true` frames over one channel's delivery tags.
//
// Used only by the batch consumer (`consumer.rs::run_batch_with_inner`),
// which always runs on a plain confirm-mode channel: there is no
// `ChannelPublisher` here because the batch path never publishes (RabbitMQ
// dead-letters broker-side via the DLX bound at declare time) and never opens
// a transactional channel, so there is no `commit_if_tx` to run either.
//
// Error contract, deliberately different from the single-message helpers
// above: every function here returns `Err(ShoveError::Connection)` on a
// failed frame instead of logging and continuing. After a failed or partial
// settle the channel's outstanding-tag set is unknown, so a later
// `multiple: true` frame could silently cover leftovers the handler never
// acked — the only sound recovery is to abandon the channel and reconnect
// (dropping it makes the broker requeue everything unsettled). The error is
// constructed directly rather than through `map_lapin_error` so it is always
// retryable and can never be classified `ShoveError::Topology`, which would
// kill the consumer instead of reconnecting it.
//
// Tag-ordering constraint every caller must hold: a `multiple: true` frame
// settles all *outstanding* tags up to and including the target, and RabbitMQ
// raises a 406 channel error if the target tag itself is already settled —
// so the target must be the highest still-unsettled tag of the set being
// retired. Already-settled tags *below* the target are skipped harmlessly.
// ---------------------------------------------------------------------------

/// `basic_ack(multiple: true)` on the highest still-unsettled handled tag —
/// the one-frame settle that is the batch consumer's payoff on this backend.
///
/// On the Commit arm the parked pre-handler drops must be individually
/// settled *first* ([`settle_parked_batch`]): parked tags interleave with
/// handled ones, and a multi-ack targeting a handled tag above an unsettled
/// parked tag would silently ack the poison instead of dead-lettering it.
pub(crate) async fn ack_batch_multiple(channel: &Channel, highest_handled_tag: u64) -> Result<()> {
    channel
        .basic_ack(highest_handled_tag, BasicAckOptions { multiple: true })
        .await
        .map_err(|e| {
            error!("batch multi-ack failed: {e}");
            ShoveError::Connection(format!("batch multi-ack failed: {e}"))
        })
}

/// Individually nack (`requeue: false`) each parked pre-handler drop so the
/// broker dead-letters it via the DLX (or drops it, with none bound), before
/// the Commit arm's multi-ack.
///
/// The failure was already counted at ingest (`record_failed`), so this
/// settles a [`metrics::pending_discard`] per entry — the same
/// counted-once split `record_terminal` bundles (see `metrics.rs`) and the
/// same discard-held-until-the-broker-accepted-the-nack contract as
/// [`route_reject`]. Runs to completion with no shutdown-token checks: a
/// nack cannot block on DLQ capacity (the DLX move is broker-side), and
/// stopping between these nacks and the multi-ack would strand the whole
/// handled batch unacked — guaranteed duplicate processing bought for
/// nothing.
pub(crate) async fn settle_parked_batch(
    channel: &Channel,
    topology: &QueueTopology,
    group: Option<&str>,
    parked: &[(u64, metrics::FailReason)],
) -> Result<()> {
    let has_dlq = topology.dlq().is_some();
    for &(tag, reason) in parked {
        if !has_dlq {
            warn!(
                queue = topology.queue(),
                "dropping pre-handler-rejected message on queue with no DLQ configured"
            );
        }
        let pending = metrics::pending_discard(topology.queue(), group, reason, has_dlq);
        match channel
            .basic_nack(
                tag,
                BasicNackOptions {
                    requeue: false,
                    ..BasicNackOptions::default()
                },
            )
            .await
        {
            Ok(()) => pending.confirm(),
            Err(e) => {
                error!("batch parked-drop nack failed: {e}");
                pending.survived();
                return Err(ShoveError::Connection(format!(
                    "batch parked-drop nack failed: {e}"
                )));
            }
        }
    }
    Ok(())
}

/// Dead-letter an entire batch — handled messages and parked pre-handler
/// drops alike — with one `basic_nack(multiple: true, requeue: false)` on the
/// highest tag. Every tag in the batch is still unsettled when this runs, so
/// the prefix frame covers exactly the batch (deliveries the broker has
/// already handed lapin but this loop has not ingested carry higher tags).
///
/// Serves both the handler-`Reject` arm and the all-poison window (an empty
/// `handled_count` with parked entries): the accounting differs per message —
/// a handled reject is a fresh terminal failure (`record_terminal`), a parked
/// drop's failure was already counted at ingest (`pending_discard` only) —
/// but the frame is the same, and every discard settles on the broker
/// accepting the nack, `survived()` on a failed frame (the delivery stays
/// unacked and redelivers, so counting it would be a false data-loss alert).
pub(crate) async fn reject_batch_multiple(
    channel: &Channel,
    topology: &QueueTopology,
    group: Option<&str>,
    handled_count: usize,
    parked: &[(u64, metrics::FailReason)],
    highest_tag: u64,
) -> Result<()> {
    let has_dlq = topology.dlq().is_some();
    if !has_dlq {
        warn!(
            queue = topology.queue(),
            batch = handled_count + parked.len(),
            "rejecting batch on queue with no DLQ configured — messages will be discarded"
        );
    }
    let mut pendings = Vec::with_capacity(handled_count + parked.len());
    for _ in 0..handled_count {
        pendings.push(metrics::record_terminal(
            topology.queue(),
            group,
            metrics::FailReason::Rejected,
            has_dlq,
        ));
    }
    for &(_, reason) in parked {
        pendings.push(metrics::pending_discard(
            topology.queue(),
            group,
            reason,
            has_dlq,
        ));
    }
    match channel
        .basic_nack(
            highest_tag,
            BasicNackOptions {
                multiple: true,
                requeue: false,
            },
        )
        .await
    {
        Ok(()) => {
            pendings
                .into_iter()
                .for_each(metrics::PendingDiscard::confirm);
            Ok(())
        }
        Err(e) => {
            error!("batch multi-nack (reject) failed: {e}");
            pendings
                .into_iter()
                .for_each(metrics::PendingDiscard::survived);
            Err(ShoveError::Connection(format!(
                "batch multi-nack (reject) failed: {e}"
            )))
        }
    }
}

/// Redeliver an entire batch with one `basic_nack(multiple: true,
/// requeue: true)` on its highest tag. The broker requeues every delivery
/// and flags the re-deliveries `redelivered` — this backend's equivalent of
/// InMemory's `mark_redelivery()` (AMQP 0-9-1 carries no delivery counter,
/// so `MessageMetadata::delivery_count` stays `None`, as its table
/// documents). No retry counters move: per the shared
/// [`BatchSettlement`](crate::backend::batch_consumer::BatchSettlement)
/// table this is a re-buffer, not a republish — and since shove declares
/// classic queues (no `x-queue-type`, so no quorum delivery-limit), a
/// handler stuck returning `Retry` redelivers indefinitely here, exactly as
/// on Kafka and InMemory.
pub(crate) async fn redeliver_batch_multiple(channel: &Channel, highest_tag: u64) -> Result<()> {
    channel
        .basic_nack(
            highest_tag,
            BasicNackOptions {
                multiple: true,
                requeue: true,
            },
        )
        .await
        .map_err(|e| {
            error!("batch multi-nack (redeliver) failed: {e}");
            ShoveError::Connection(format!("batch multi-nack (redeliver) failed: {e}"))
        })
}

pub(crate) async fn nack_requeue(delivery: &Delivery, publisher: &ChannelPublisher) -> Result<()> {
    if let Err(e) = delivery
        .nack(BasicNackOptions {
            requeue: true,
            ..BasicNackOptions::default()
        })
        .await
    {
        error!("failed to nack delivery for requeue: {e}");
    }
    if let Err(e) = publisher.commit_if_tx().await {
        error!("tx_commit failed after nack-requeue: {e}");
        return Err(ShoveError::Connection(format!(
            "tx_commit failed after nack-requeue: {e}"
        )));
    }
    Ok(())
}

pub(crate) fn clone_headers_with_retry(delivery: &Delivery, retry_count: u32) -> FieldTable {
    let mut table = copy_preserved_headers(delivery);
    table.insert(RETRY_COUNT_KEY.into(), AMQPValue::LongUInt(retry_count));
    ensure_message_id(&mut table);
    table
}

pub(crate) fn clone_headers(delivery: &Delivery) -> FieldTable {
    let mut table = copy_preserved_headers(delivery);
    ensure_message_id(&mut table);
    table
}

/// Insert a fresh `x-message-id` if one is not already present.
///
/// Called when routing a message to a hold queue so that the hold-queue copy
/// and any broker-requeued original share the same stable identifier. Handlers
/// can compare `metadata.headers["x-message-id"]` across deliveries to detect
/// the duplicate introduced by the publish-then-ack race.
fn ensure_message_id(table: &mut FieldTable) {
    if !table.inner().contains_key(MESSAGE_ID_KEY) {
        table.insert(
            MESSAGE_ID_KEY.into(),
            AMQPValue::LongString(Uuid::new_v4().to_string().into()),
        );
    }
}

/// Headers that must be preserved across retries and defers.
const PRESERVED_HEADER_PREFIXES: &[&str] = &["x-trace-", "x-request-"];

/// Build a minimal `FieldTable` by copying only headers that need to survive
/// retries/defers, instead of deep-cloning the entire table.
fn copy_preserved_headers(delivery: &Delivery) -> FieldTable {
    let Some(orig) = delivery.properties.headers().as_ref() else {
        return FieldTable::default();
    };

    let inner = orig.inner();
    let mut table = FieldTable::default();

    for (k, v) in inner.iter() {
        let key_str = k.as_str();
        // Always preserve retry count (will be overwritten by caller if needed)
        // and message ID (stable deduplication key across hold-queue hops).
        if key_str == RETRY_COUNT_KEY || key_str == MESSAGE_ID_KEY {
            table.insert(k.clone(), v.clone());
            continue;
        }
        // Preserve headers matching known prefixes.
        if PRESERVED_HEADER_PREFIXES
            .iter()
            .any(|prefix| key_str.starts_with(prefix))
        {
            table.insert(k.clone(), v.clone());
        }
    }

    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::BasicProperties;
    use lapin::message::Delivery;
    use lapin::types::{AMQPValue, FieldTable, ShortString};

    fn make_delivery(headers: Option<FieldTable>) -> Delivery {
        let mut delivery = Delivery::mock(
            1,
            ShortString::from(""),
            ShortString::from(""),
            false,
            vec![],
        );
        if let Some(h) = headers {
            delivery.properties = BasicProperties::default().with_headers(h);
        }
        delivery
    }

    #[test]
    fn clone_headers_with_no_headers_adds_message_id() {
        let delivery = make_delivery(None);
        let result = clone_headers(&delivery);
        assert!(result.inner().contains_key(MESSAGE_ID_KEY));
        assert_eq!(result.inner().len(), 1);
    }

    #[test]
    fn clone_headers_preserves_trace_headers() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString("abc123".into()),
        );
        let delivery = make_delivery(Some(table));
        let result = clone_headers(&delivery);
        assert!(result.inner().contains_key("x-trace-id"));
        assert!(result.inner().contains_key(MESSAGE_ID_KEY));
        assert_eq!(result.inner().len(), 2);
    }

    #[test]
    fn clone_headers_drops_non_preserved_headers() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-custom"),
            AMQPValue::LongString("value".into()),
        );
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString("tid".into()),
        );
        let delivery = make_delivery(Some(table));
        let result = clone_headers(&delivery);
        assert!(!result.inner().contains_key("x-custom"));
        assert!(result.inner().contains_key("x-trace-id"));
        assert!(result.inner().contains_key(MESSAGE_ID_KEY));
        assert_eq!(result.inner().len(), 2);
    }

    #[test]
    fn clone_headers_with_retry_no_existing_headers_inserts_retry_count() {
        let delivery = make_delivery(None);
        let result = clone_headers_with_retry(&delivery, 3);
        assert!(result.inner().contains_key(MESSAGE_ID_KEY));
        assert_eq!(result.inner().len(), 2);
        assert_eq!(
            result.inner().get(RETRY_COUNT_KEY),
            Some(&AMQPValue::LongUInt(3))
        );
    }

    #[test]
    fn clone_headers_with_retry_preserves_trace_headers_and_adds_retry_count() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString("tid".into()),
        );
        let delivery = make_delivery(Some(table));
        let result = clone_headers_with_retry(&delivery, 2);
        assert!(result.inner().contains_key(MESSAGE_ID_KEY));
        assert_eq!(result.inner().len(), 3);
        assert!(result.inner().contains_key("x-trace-id"));
        assert_eq!(
            result.inner().get(RETRY_COUNT_KEY),
            Some(&AMQPValue::LongUInt(2))
        );
    }

    #[test]
    fn clone_headers_with_retry_overwrites_existing_retry_count() {
        let mut table = FieldTable::default();
        table.insert(ShortString::from(RETRY_COUNT_KEY), AMQPValue::LongUInt(1));
        let delivery = make_delivery(Some(table));
        let result = clone_headers_with_retry(&delivery, 5);
        assert_eq!(
            result.inner().get(RETRY_COUNT_KEY),
            Some(&AMQPValue::LongUInt(5))
        );
    }

    #[test]
    fn clone_headers_preserves_request_headers() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-request-id"),
            AMQPValue::LongString("req-1".into()),
        );
        table.insert(
            ShortString::from("content-encoding"),
            AMQPValue::LongString("gzip".into()),
        );
        let delivery = make_delivery(Some(table));
        let result = clone_headers(&delivery);
        assert!(result.inner().contains_key("x-request-id"));
        assert!(!result.inner().contains_key("content-encoding"));
    }
}
