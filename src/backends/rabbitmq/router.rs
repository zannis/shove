use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::{AMQPValue, FieldTable};
use tracing::{debug, error, warn};

use uuid::Uuid;

use crate::backends::rabbitmq::headers::{MESSAGE_ID_KEY, RETRY_COUNT_KEY};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::topology::QueueTopology;

pub(crate) async fn route_ack(delivery: &Delivery, publisher: &ChannelPublisher) {
    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
        error!("failed to ack delivery: {e}");
    }
    if let Err(e) = publisher.commit_if_tx().await {
        error!("tx_commit failed after ack: {e}");
    }
}

pub(crate) async fn route_retry(
    delivery: &Delivery,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    retry_count: u32,
) {
    let new_retry_count = retry_count + 1;
    let hold_queues = topology.hold_queues();

    if !hold_queues.is_empty() {
        let index = (retry_count as usize).min(hold_queues.len() - 1);
        let hold_queue = &hold_queues[index];
        let headers = clone_headers_with_retry(delivery, new_retry_count);

        match publisher
            .publish_to_queue(hold_queue.name(), &delivery.data, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to hold queue: {e}");
                    // Ack failed while publish is buffered in tx — roll back the
                    // buffered publish so no duplicate ends up in the hold queue.
                    publisher.rollback_if_tx().await;
                    nack_requeue(delivery, publisher).await;
                    return;
                }
                if let Err(e) = publisher.commit_if_tx().await {
                    error!("tx_commit failed for retry (attempt {new_retry_count}): {e}");
                    // tx_commit failure means neither publish nor ack happened;
                    // delivery remains unacked and will be redelivered by the broker.
                    return;
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
                nack_requeue(delivery, publisher).await;
            }
        }
    } else {
        warn!(
            queue = topology.queue(),
            retry_count, "retrying message but no hold queues configured — requeuing with no delay"
        );
        nack_requeue(delivery, publisher).await;
    }
}

pub(crate) async fn route_defer(
    delivery: &Delivery,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
) {
    let hold_queues = topology.hold_queues();

    if !hold_queues.is_empty() {
        let hold_queue = &hold_queues[0];
        let headers = clone_headers(delivery);

        match publisher
            .publish_to_queue(hold_queue.name(), &delivery.data, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after deferring to hold queue: {e}");
                    publisher.rollback_if_tx().await;
                    nack_requeue(delivery, publisher).await;
                    return;
                }
                if let Err(e) = publisher.commit_if_tx().await {
                    error!("tx_commit failed for defer: {e}");
                    return;
                }
                debug!("deferring message to hold queue {}", hold_queue.name());
            }
            Err(e) => {
                warn!(
                    "failed to publish to hold queue {} for defer, requeuing: {e}",
                    hold_queue.name()
                );
                nack_requeue(delivery, publisher).await;
            }
        }
    } else {
        warn!(
            queue = topology.queue(),
            "deferring message but no hold queues configured — requeuing with no delay"
        );
        nack_requeue(delivery, publisher).await;
    }
}

pub(crate) async fn route_reject(
    delivery: &Delivery,
    topology: &QueueTopology,
    publisher: &ChannelPublisher,
) {
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
    }
    if let Err(e) = publisher.commit_if_tx().await {
        error!("tx_commit failed after reject: {e}");
    }
}

pub(crate) async fn nack_requeue(delivery: &Delivery, publisher: &ChannelPublisher) {
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
    }
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
