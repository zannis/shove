use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry, Message,
    MessageAttributeValue, MessageSystemAttributeName,
};
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::metrics;
use crate::metrics::{BackendErrorKind, BackendLabel, record_backend_error};
use crate::topology::QueueTopology;

/// Custom SQS message attribute used to track retry count across
/// delete+re-send cycles. Takes precedence over `ApproximateReceiveCount`.
pub(crate) const RETRY_COUNT_ATTR: &str = "x-retry-count";

/// Delete a message from SQS (acknowledge).
pub(crate) async fn route_ack(sqs: &aws_sdk_sqs::Client, queue_url: &str, receipt_handle: &str) {
    if let Err(e) = sqs
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await
    {
        error!(queue_url, error = %e, "failed to delete (ack) SQS message");
    }
}

/// Delete up to 10 messages from SQS in a single `DeleteMessageBatch` call.
///
/// Using this instead of individual `DeleteMessage` calls settles a full
/// batch in one API round-trip instead of up to ten.
pub(crate) async fn route_ack_batch(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handles: Vec<String>,
) {
    debug!(
        queue_url,
        batch_size = receipt_handles.len(),
        "acking message batch (DeleteMessageBatch)"
    );
    for chunk in receipt_handles.chunks(10) {
        let entries: Vec<_> = chunk
            .iter()
            .enumerate()
            .filter_map(|(i, rh)| {
                DeleteMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(rh)
                    .build()
                    .ok()
            })
            .collect();

        if entries.is_empty() {
            continue;
        }

        match sqs
            .delete_message_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
        {
            Err(e) => error!(queue_url, error = %e, "failed to batch delete (ack) SQS messages"),
            Ok(out) => {
                for failure in out.failed() {
                    error!(
                        queue_url,
                        id = failure.id(),
                        code = failure.code(),
                        "batch ack: individual message delete failed"
                    );
                }
            }
        }
    }
}

/// The API's per-`ChangeMessageVisibility[Batch]` cap on `VisibilityTimeout`,
/// in seconds (12 hours).
pub(crate) const SQS_MAX_VISIBILITY_TIMEOUT_SECS: i32 = 43200;

/// Convert a redelivery/reject delay into an SQS `VisibilityTimeout`, in
/// whole seconds.
///
/// Ceiling-rounded rather than truncated: the shared batch redelivery
/// backoff (`batch_redelivery_backoff`) jitters ±50%, so its first draw is
/// often sub-second, and a plain `as_secs()` would floor that to `0` —
/// reopening the instant-cross-replica-redelivery hole a non-zero delay
/// exists to close (a sibling consumer on the same queue would re-receive
/// the batch before the backoff has done anything). Any non-zero input is
/// therefore floored at 1 second, and the result is capped at the API's
/// [`SQS_MAX_VISIBILITY_TIMEOUT_SECS`] maximum.
///
/// `Duration::ZERO` passes through as `0` unchanged — the deliberate
/// make-visible-now case both `route_requeue_batch` (a `ReceiveMessage`
/// error stranding already-buffered handles — those messages did nothing
/// wrong) and `route_reject_batch` (the terminal reject arm, which always
/// wants visibility 0) rely on.
pub(crate) fn visibility_seconds_for_delay(delay: Duration) -> i32 {
    if delay.is_zero() {
        return 0;
    }
    let whole_secs = delay.as_secs();
    let ceiled = if delay.subsec_nanos() > 0 {
        whole_secs.saturating_add(1)
    } else {
        whole_secs
    };
    ceiled.max(1).min(SQS_MAX_VISIBILITY_TIMEOUT_SECS as u64) as i32
}

/// Shared mechanics behind [`route_requeue_batch`] and [`route_reject_batch`]:
/// one `ChangeMessageVisibilityBatch` call per chunk of up to 10 receipt
/// handles, all set to the same `visibility_timeout`. Failures are logged
/// and not accounted, mirroring `route_ack_batch` — a failed entry simply
/// returns via its original visibility timeout instead of the requested one,
/// which is "delayed, not lost".
async fn change_visibility_batch(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handles: &[String],
    visibility_timeout: i32,
) {
    for chunk in receipt_handles.chunks(10) {
        let entries: Vec<_> = chunk
            .iter()
            .enumerate()
            .filter_map(|(i, rh)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(rh)
                    .visibility_timeout(visibility_timeout)
                    .build()
                    .ok()
            })
            .collect();

        if entries.is_empty() {
            continue;
        }

        match sqs
            .change_message_visibility_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
        {
            Err(e) => warn!(
                queue_url,
                error = %e,
                "failed to batch change visibility for SQS messages"
            ),
            Ok(out) => {
                for failure in out.failed() {
                    warn!(
                        queue_url,
                        id = failure.id(),
                        code = failure.code(),
                        "batch visibility change: individual entry failed"
                    );
                }
            }
        }
    }
}

/// Batch-wide `Redeliver` mechanic (see
/// [`BatchSettlement`](crate::backend::batch_consumer::BatchSettlement)):
/// reset the visibility timeout of up to N buffered receipt handles to
/// `delay` in one `ChangeMessageVisibilityBatch` call, rather than the
/// single-message `route_retry`'s delete+re-send. A batch-wide outcome is a
/// seek-back/re-buffer, not a republish — delete+re-send would increment
/// `x-retry-count` (which the shared contract says a `Redeliver` must not
/// touch) and cost two API calls per message instead of one call for the
/// whole batch.
///
/// `delay` should come from the shared `next_redelivery_delay`; see
/// [`visibility_seconds_for_delay`] for the rounding this applies to it.
/// Chunked to 10 entries per call defensively — the caller already validates
/// `max_batch_size <= 10` (`validate_sqs_batch_size`), so a second chunk
/// should never actually happen.
pub(crate) async fn route_requeue_batch(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handles: &[String],
    delay: Duration,
) {
    if receipt_handles.is_empty() {
        return;
    }
    let visibility_timeout = visibility_seconds_for_delay(delay);
    debug!(
        queue_url,
        batch_size = receipt_handles.len(),
        visibility_timeout,
        "requeueing batch (ChangeMessageVisibilityBatch)"
    );
    change_visibility_batch(sqs, queue_url, receipt_handles, visibility_timeout).await;
}

/// Batch-wide `DeadLetter` mechanic: `record_failed` once per message (so
/// `messages_failed_total` stays comparable to the single-message
/// `route_reject`, which records one increment per rejected delivery) plus a
/// single `ChangeMessageVisibilityBatch` call resetting every handle's
/// visibility to 0 immediately, redelivering the whole batch until the
/// queue's `maxReceiveCount` redrive moves it to a DLQ (or, with no redrive
/// policy, until SQS's retention period expires — same as the single-message
/// `route_reject`, which this mirrors exactly). Every reject path must name
/// its reason; nothing here decides one on its own.
pub(crate) async fn route_reject_batch(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handles: &[String],
    topology: &QueueTopology,
    group: Option<&str>,
    reason: metrics::FailReason,
) {
    if receipt_handles.is_empty() {
        return;
    }
    metrics::record_failed_n(
        topology.queue(),
        group,
        reason,
        receipt_handles.len() as u64,
    );
    if topology.dlq().is_none() {
        warn!(
            queue_url,
            "rejecting batch on queue with no DLQ configured — messages will cycle until SQS retention expires"
        );
    }
    debug!(
        queue_url,
        batch_size = receipt_handles.len(),
        "rejecting batch (ChangeMessageVisibilityBatch, visibility=0)"
    );
    change_visibility_batch(sqs, queue_url, receipt_handles, 0).await;
}

/// Delete + re-send the message with an incremented `x-retry-count`
/// attribute and a delay based on the hold queue configuration.
pub(crate) async fn route_retry(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    body: &str,
    message_attributes: &std::collections::HashMap<String, MessageAttributeValue>,
    topology: &QueueTopology,
    retry_count: u32,
) {
    let new_retry_count = retry_count + 1;

    let delay = if topology.hold_queues().is_empty() {
        warn!(
            queue_url,
            "retrying message but no hold queues configured — re-sending with no delay"
        );
        Duration::ZERO
    } else {
        let index = (retry_count as usize).min(topology.hold_queues().len() - 1);
        topology.hold_queues()[index].delay()
    };

    let delay_seconds = delay.as_secs().min(900) as i32;

    debug!(
        queue_url,
        retry_count = new_retry_count,
        delay_seconds,
        "re-sending message for retry"
    );

    resend_to_queue(
        sqs,
        queue_url,
        receipt_handle,
        body,
        message_attributes,
        new_retry_count,
        delay_seconds,
    )
    .await;
}

/// Change message visibility timeout for retry with escalating delay.
///
/// Used by sequenced (FIFO) consumers where delete+re-send is not viable
/// (FIFO queues require `MessageGroupId` and don't support per-message
/// `DelaySeconds`). Retry count is tracked via `ApproximateReceiveCount`.
pub(crate) async fn route_retry_fifo(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    topology: &QueueTopology,
    retry_count: u32,
) {
    let delay = if topology.hold_queues().is_empty() {
        warn!(
            queue_url,
            "retrying message but no hold queues configured — visibility timeout set to 0"
        );
        Duration::ZERO
    } else {
        let index = (retry_count as usize).min(topology.hold_queues().len() - 1);
        topology.hold_queues()[index].delay()
    };

    let timeout_secs = delay.as_secs() as i32;

    debug!(
        queue_url,
        retry_count, timeout_secs, "changing visibility for retry (FIFO)"
    );

    if let Err(e) = sqs
        .change_message_visibility()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .visibility_timeout(timeout_secs)
        .send()
        .await
    {
        warn!(queue_url, error = %e, "failed to change visibility for retry");
    }
}

/// Reject a message. Sets visibility to 0 so SQS redelivers it immediately,
/// incrementing ApproximateReceiveCount. Once maxReceiveCount is exceeded,
/// SQS native redrive moves it to the DLQ.
///
/// ## Why SQS never records a discard
///
/// This is the terminal path on SQS, and it deletes nothing: the message stays
/// on the queue and becomes visible again. Whether it eventually reaches a DLQ
/// is decided by the queue's *AWS-side* redrive policy, which `shove` does not
/// own and cannot read from `topology`. So neither branch here is a discard —
/// with a redrive policy the message is dead-lettered by SQS, and without one
/// it cycles until the retention period expires.
///
/// So this records `record_failed`, never `record_terminal`.
/// `shove_messages_discarded_total` promises that every increment is a message
/// that no longer exists, and incrementing it here would break that promise
/// twice over: the message is still live, and because its receive count stays
/// above the budget, every later receive would increment the counter again for
/// the same message. The repeated `WARN` below is the intended signal.
///
/// ## Why `reason` is a required argument
///
/// Because SQS opts out of the discard counter, `messages_failed_total` is the
/// only signal an SQS operator has — and it is what the observability guide now
/// tells them to alert on. The metric is therefore recorded *here* rather than
/// at the call sites: this consumer hand-rolls its retry-budget and validation
/// checks in a dozen-odd places across the standard, concurrent, sequenced and
/// buffered-pending loops, and instrumenting them individually is exactly how
/// the sequenced and buffered paths came to record nothing at all. Every path
/// that rejects a delivery ends here, so a new one cannot be added without
/// naming its reason.
///
/// Releasing a message that was never dispatched — the graceful-shutdown drain
/// — is not a failure and must use [`route_requeue`] instead.
pub(crate) async fn route_reject(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    topology: &QueueTopology,
    group: Option<&str>,
    reason: metrics::FailReason,
) {
    metrics::record_failed(topology.queue(), group, reason);
    reject_visibility(sqs, queue_url, receipt_handle, topology).await;
}

/// [`route_reject`] for a delivery released as collateral of a failure that has
/// already been counted — a [`SequenceFailure::FailAll`] cascade behind a
/// poisoned key.
///
/// Identical routing; it simply does not increment `messages_failed_total`,
/// because the cascade's size tracks queue depth rather than a count of things
/// that went wrong. See [`metrics::FailReason`].
///
/// This is deliberately a second function rather than a flag, so that the
/// "every reject path must name its reason" property above extends to the
/// accounting choice: a cascade cannot be introduced by omitting an argument.
///
/// [`SequenceFailure::FailAll`]: crate::topology::SequenceFailure
pub(crate) async fn route_reject_cascade(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    topology: &QueueTopology,
) {
    reject_visibility(sqs, queue_url, receipt_handle, topology).await;
}

/// Shared visibility-reset mechanics behind both reject entry points.
async fn reject_visibility(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    topology: &QueueTopology,
) {
    if topology.dlq().is_none() {
        warn!(
            queue_url,
            "rejecting message on queue with no DLQ configured — message will cycle until SQS retention expires"
        );
    }
    if let Err(e) = sqs
        .change_message_visibility()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .visibility_timeout(0)
        .send()
        .await
    {
        warn!(queue_url, error = %e, "failed to change visibility for reject");
    }
}

/// Requeue an unprocessed message by making it immediately visible again.
///
/// Used during graceful shutdown to release buffered-but-never-dispatched
/// messages back to the queue without the DLQ semantics of `route_reject`.
/// Does not imply the message was processed or rejected.
pub(crate) async fn route_requeue(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
) {
    if let Err(e) = sqs
        .change_message_visibility()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .visibility_timeout(0)
        .send()
        .await
    {
        warn!(queue_url, error = %e, "failed to change visibility for requeue");
    }
}

/// Delete the original message and re-send it to the same queue with
/// updated message attributes and an optional delay.
///
/// On send failure the original is NOT deleted — it will return via
/// its visibility timeout.
async fn resend_to_queue(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    body: &str,
    existing_attrs: &std::collections::HashMap<String, MessageAttributeValue>,
    retry_count: u32,
    delay_seconds: i32,
) {
    // Clone existing attributes and set/overwrite x-retry-count.
    let retry_attr = MessageAttributeValue::builder()
        .data_type("String")
        .string_value(retry_count.to_string())
        .build()
        .expect("building MessageAttributeValue should not fail");

    let mut req = sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .delay_seconds(delay_seconds)
        .message_attributes(RETRY_COUNT_ATTR, retry_attr);

    for (k, v) in existing_attrs {
        if k != RETRY_COUNT_ATTR {
            req = req.message_attributes(k, v.clone());
        }
    }

    match req.send().await {
        Ok(_) => {
            // Send succeeded — delete the original.
            if let Err(e) = sqs
                .delete_message()
                .queue_url(queue_url)
                .receipt_handle(receipt_handle)
                .send()
                .await
            {
                error!(
                    queue_url,
                    error = %e,
                    "failed to delete original after re-send (possible duplicate)"
                );
                record_backend_error(BackendLabel::SnsSqs, BackendErrorKind::Ack);
            }
        }
        Err(e) => {
            // Send failed — do NOT delete, original returns via visibility timeout.
            error!(
                queue_url,
                error = %e,
                retry_count,
                delay_seconds,
                "failed to re-send message to queue"
            );
        }
    }
}

/// Delete + re-send the message with the SAME `x-retry-count` attribute
/// (no increment) and a delay based on `hold_queues[0]`.
pub(crate) async fn route_defer(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    body: &str,
    message_attributes: &std::collections::HashMap<String, MessageAttributeValue>,
    topology: &QueueTopology,
    retry_count: u32,
) {
    let delay = if topology.hold_queues().is_empty() {
        warn!(
            queue_url,
            "deferring message but no hold queues configured — re-sending with no delay"
        );
        Duration::ZERO
    } else {
        topology.hold_queues()[0].delay()
    };

    let delay_seconds = delay.as_secs().min(900) as i32;

    debug!(
        queue_url,
        retry_count, delay_seconds, "re-sending message for defer"
    );

    resend_to_queue(
        sqs,
        queue_url,
        receipt_handle,
        body,
        message_attributes,
        retry_count,
        delay_seconds,
    )
    .await;
}

/// Extract retry count from SQS message attributes.
///
/// Prefers the explicit `x-retry-count` message attribute (set by our
/// retry/defer re-send path). Falls back to `ApproximateReceiveCount - 1`
/// for first-delivery or legacy messages that lack the attribute.
pub(crate) fn get_retry_count(message: &Message) -> u32 {
    // Prefer explicit x-retry-count attribute (set by our retry/defer resend).
    if let Some(count) = message
        .message_attributes()
        .and_then(|a| a.get(RETRY_COUNT_ATTR))
        .and_then(|v| v.string_value())
        .and_then(|s| s.parse::<u32>().ok())
    {
        return count;
    }
    // Fallback: ApproximateReceiveCount - 1 (first delivery or legacy messages).
    message
        .attributes()
        .and_then(|attrs| attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|v| v.parse::<u32>().ok())
        .map(|count| count.saturating_sub(1))
        .unwrap_or(0)
}

/// Extract string message attributes from an SQS message.
pub(crate) fn extract_message_attributes(
    message: &Message,
) -> std::collections::HashMap<String, String> {
    message
        .message_attributes()
        .map(|attrs| {
            attrs
                .iter()
                .filter_map(|(k, v)| v.string_value().map(|sv| (k.clone(), sv.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TopologyBuilder;

    #[test]
    fn retry_delay_escalates_with_hold_queues() {
        let topology = TopologyBuilder::new("test")
            .hold_queue(Duration::from_secs(5))
            .hold_queue(Duration::from_secs(30))
            .hold_queue(Duration::from_secs(120))
            .build();

        let hqs = topology.hold_queues();
        // retry_count 0 → hold_queues[0] = 5s
        assert_eq!(hqs[0].delay(), Duration::from_secs(5));
        // retry_count 1 → hold_queues[1] = 30s
        assert_eq!(hqs[1.min(hqs.len() - 1)].delay(), Duration::from_secs(30));
        // retry_count 2 → hold_queues[2] = 120s
        assert_eq!(hqs[2.min(hqs.len() - 1)].delay(), Duration::from_secs(120));
        // retry_count 5 → hold_queues[2] = 120s (capped)
        assert_eq!(hqs[5.min(hqs.len() - 1)].delay(), Duration::from_secs(120));
    }

    #[test]
    fn retry_delay_no_hold_queues() {
        let topology = TopologyBuilder::new("test").build();
        assert!(topology.hold_queues().is_empty());
    }

    #[test]
    fn get_retry_count_from_message() {
        let msg = Message::builder()
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "3")
            .build();
        assert_eq!(get_retry_count(&msg), 2);
    }

    #[test]
    fn get_retry_count_first_receive() {
        let msg = Message::builder()
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "1")
            .build();
        assert_eq!(get_retry_count(&msg), 0);
    }

    #[test]
    fn get_retry_count_missing() {
        let msg = Message::builder().build();
        assert_eq!(get_retry_count(&msg), 0);
    }

    #[test]
    fn extract_message_attributes_works() {
        let attr = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("trace-123")
            .build()
            .unwrap();
        let msg = Message::builder()
            .message_attributes("x-trace-id", attr)
            .build();
        let attrs = extract_message_attributes(&msg);
        assert_eq!(attrs.get("x-trace-id"), Some(&"trace-123".to_string()));
    }

    #[test]
    fn extract_message_attributes_empty() {
        let msg = Message::builder().build();
        let attrs = extract_message_attributes(&msg);
        assert!(attrs.is_empty());
    }

    #[test]
    fn retry_delay_is_zero_without_hold_queues() {
        let topology = TopologyBuilder::new("test").build();
        assert!(topology.hold_queues().is_empty());
    }

    #[test]
    fn defer_delay_is_zero_without_hold_queues() {
        let topology = TopologyBuilder::new("test").build();
        assert!(topology.hold_queues().is_empty());
    }

    #[test]
    fn reject_topology_without_dlq() {
        let topology = TopologyBuilder::new("test").build();
        assert!(topology.dlq().is_none());
    }

    #[test]
    fn reject_topology_with_dlq() {
        let topology = TopologyBuilder::new("test").dlq().build();
        assert!(topology.dlq().is_some());
    }

    #[test]
    fn get_retry_count_prefers_custom_attribute() {
        let attr = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("3")
            .build()
            .unwrap();
        let msg = Message::builder()
            .attributes(
                MessageSystemAttributeName::ApproximateReceiveCount,
                "10", // ARC says 9, but x-retry-count says 3
            )
            .message_attributes(RETRY_COUNT_ATTR, attr)
            .build();
        assert_eq!(get_retry_count(&msg), 3);
    }

    #[test]
    fn get_retry_count_falls_back_to_arc_without_custom_attribute() {
        let msg = Message::builder()
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "4")
            .build();
        assert_eq!(get_retry_count(&msg), 3);
    }

    // -----------------------------------------------------------------------
    // visibility_seconds_for_delay — batch redelivery/reject visibility math
    // -----------------------------------------------------------------------

    #[test]
    fn zero_delay_passes_through_as_zero() {
        assert_eq!(visibility_seconds_for_delay(Duration::ZERO), 0);
    }

    #[test]
    fn sub_second_delay_ceils_to_one() {
        // The shared backoff jitters ±50%, so the first draw is often
        // sub-second — truncating via `as_secs()` would yield 0, reopening
        // the instant-cross-replica-redelivery hole a non-zero delay is
        // supposed to close.
        assert_eq!(visibility_seconds_for_delay(Duration::from_millis(1)), 1);
        assert_eq!(visibility_seconds_for_delay(Duration::from_millis(500)), 1);
        assert_eq!(visibility_seconds_for_delay(Duration::from_millis(999)), 1);
    }

    #[test]
    fn whole_second_delay_is_unchanged() {
        assert_eq!(visibility_seconds_for_delay(Duration::from_secs(1)), 1);
        assert_eq!(visibility_seconds_for_delay(Duration::from_secs(30)), 30);
    }

    #[test]
    fn fractional_second_delay_ceils_up() {
        assert_eq!(visibility_seconds_for_delay(Duration::from_millis(1500)), 2);
        assert_eq!(
            visibility_seconds_for_delay(Duration::from_millis(30001)),
            31
        );
    }

    #[test]
    fn delay_is_capped_at_the_api_maximum() {
        assert_eq!(
            visibility_seconds_for_delay(Duration::from_secs(999_999)),
            SQS_MAX_VISIBILITY_TIMEOUT_SECS
        );
    }
}
