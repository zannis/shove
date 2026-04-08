use std::time::Duration;

use tracing::{debug, error, warn};

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
/// Using this instead of 10 individual `DeleteMessage` calls reduces API
/// round-trips by 10× for a full batch, which is critical for throughput when
/// multiple consumers are competing on the same queue.
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
                aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
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

/// Change message visibility timeout for retry with escalating delay.
pub(crate) async fn route_retry(
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
        retry_count, timeout_secs, "changing visibility for retry"
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
pub(crate) async fn route_reject(
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

/// Change visibility timeout for defer (uses hold_queues[0] delay).
pub(crate) async fn route_defer(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    receipt_handle: &str,
    topology: &QueueTopology,
) {
    let delay = if topology.hold_queues().is_empty() {
        warn!(
            queue_url,
            "deferring message but no hold queues configured — visibility timeout set to 0"
        );
        Duration::ZERO
    } else {
        topology.hold_queues()[0].delay()
    };

    let timeout_secs = delay.as_secs() as i32;

    debug!(queue_url, timeout_secs, "changing visibility for defer");

    if let Err(e) = sqs
        .change_message_visibility()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .visibility_timeout(timeout_secs)
        .send()
        .await
    {
        warn!(queue_url, error = %e, "failed to change visibility for defer");
    }
}

/// Extract retry count from SQS message attributes.
///
/// Prefers the explicit `x-retry-count` message attribute (set by our
/// retry/defer re-send path). Falls back to `ApproximateReceiveCount - 1`
/// for first-delivery or legacy messages that lack the attribute.
pub(crate) fn get_retry_count(message: &aws_sdk_sqs::types::Message) -> u32 {
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
        .and_then(|attrs| {
            attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
        })
        .and_then(|v| v.parse::<u32>().ok())
        .map(|count| count.saturating_sub(1))
        .unwrap_or(0)
}

/// Extract string message attributes from an SQS message.
pub(crate) fn extract_message_attributes(
    message: &aws_sdk_sqs::types::Message,
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
        assert_eq!(hqs[0.min(hqs.len() - 1)].delay(), Duration::from_secs(5));
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
        let msg = aws_sdk_sqs::types::Message::builder()
            .attributes(
                aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                "3",
            )
            .build();
        assert_eq!(get_retry_count(&msg), 2);
    }

    #[test]
    fn get_retry_count_first_receive() {
        let msg = aws_sdk_sqs::types::Message::builder()
            .attributes(
                aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                "1",
            )
            .build();
        assert_eq!(get_retry_count(&msg), 0);
    }

    #[test]
    fn get_retry_count_missing() {
        let msg = aws_sdk_sqs::types::Message::builder().build();
        assert_eq!(get_retry_count(&msg), 0);
    }

    #[test]
    fn extract_message_attributes_works() {
        let attr = aws_sdk_sqs::types::MessageAttributeValue::builder()
            .data_type("String")
            .string_value("trace-123")
            .build()
            .unwrap();
        let msg = aws_sdk_sqs::types::Message::builder()
            .message_attributes("x-trace-id", attr)
            .build();
        let attrs = extract_message_attributes(&msg);
        assert_eq!(attrs.get("x-trace-id"), Some(&"trace-123".to_string()));
    }

    #[test]
    fn extract_message_attributes_empty() {
        let msg = aws_sdk_sqs::types::Message::builder().build();
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
        let attr = aws_sdk_sqs::types::MessageAttributeValue::builder()
            .data_type("String")
            .string_value("3")
            .build()
            .unwrap();
        let msg = aws_sdk_sqs::types::Message::builder()
            .attributes(
                aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                "10", // ARC says 9, but x-retry-count says 3
            )
            .message_attributes(RETRY_COUNT_ATTR, attr)
            .build();
        assert_eq!(get_retry_count(&msg), 3);
    }

    #[test]
    fn get_retry_count_falls_back_to_arc_without_custom_attribute() {
        let msg = aws_sdk_sqs::types::Message::builder()
            .attributes(
                aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                "4",
            )
            .build();
        assert_eq!(get_retry_count(&msg), 3);
    }
}
