use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::{AMQPValue, FieldTable};
use tracing::{debug, error, warn};

use crate::backends::rabbitmq::headers::RETRY_COUNT_KEY;
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::topology::QueueTopology;

pub(crate) async fn route_ack(delivery: &Delivery) {
    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
        error!("failed to ack delivery: {e}");
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
                debug!(
                    "retrying message via hold queue {} (attempt {})",
                    hold_queue.name(),
                    new_retry_count
                );
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to hold queue: {e}");
                }
            }
            Err(e) => {
                warn!(
                    "failed to publish to hold queue {}, requeuing: {e}",
                    hold_queue.name()
                );
                nack_requeue(delivery).await;
            }
        }
    } else {
        nack_requeue(delivery).await;
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
                debug!("deferring message to hold queue {}", hold_queue.name());
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after deferring to hold queue: {e}");
                }
            }
            Err(e) => {
                warn!(
                    "failed to publish to hold queue {} for defer, requeuing: {e}",
                    hold_queue.name()
                );
                nack_requeue(delivery).await;
            }
        }
    } else {
        debug!(
            "Defer outcome on queue {} but no hold queues configured, requeuing via nack",
            topology.queue()
        );
        nack_requeue(delivery).await;
    }
}

pub(crate) async fn route_reject(delivery: &Delivery) {
    if let Err(e) = delivery
        .nack(BasicNackOptions {
            requeue: false,
            ..BasicNackOptions::default()
        })
        .await
    {
        error!("failed to nack-reject delivery: {e}");
    }
}

async fn nack_requeue(delivery: &Delivery) {
    if let Err(e) = delivery
        .nack(BasicNackOptions {
            requeue: true,
            ..BasicNackOptions::default()
        })
        .await
    {
        error!("failed to nack delivery for requeue: {e}");
    }
}

fn clone_headers_with_retry(delivery: &Delivery, retry_count: u32) -> FieldTable {
    let mut table = clone_headers(delivery);
    table.insert(RETRY_COUNT_KEY.into(), AMQPValue::LongUInt(retry_count));
    table
}

fn clone_headers(delivery: &Delivery) -> FieldTable {
    delivery
        .properties
        .headers()
        .as_ref()
        .cloned()
        .unwrap_or_default()
}
