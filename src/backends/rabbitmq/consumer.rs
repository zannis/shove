use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions};
use lapin::types::FieldTable;
use lapin::{Channel, Error as LapinError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::RECONNECT_DELAY;
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::{
    extract_dead_metadata, extract_message_metadata, get_retry_count,
};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::backends::rabbitmq::router;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::SequenceFailure;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Opens a channel with QoS and starts consuming from `queue`.
async fn open_consumer(
    client: &RabbitMqClient,
    queue: &str,
    prefetch_count: u16,
) -> Result<(Channel, lapin::Consumer)> {
    let channel = client.create_confirm_channel().await?;
    channel
        .basic_qos(prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| ShoveError::Connection(format!("failed to set QoS: {e}")))?;
    let consumer = channel
        .basic_consume(
            lapin::types::ShortString::from(queue),
            lapin::types::ShortString::from(""),
            BasicConsumeOptions {
                no_ack: false,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| ShoveError::Connection(format!("failed to start consumer on {queue}: {e}")))?;
    Ok((channel, consumer))
}

/// Unwrap a delivery from the consumer stream.
fn unwrap_delivery(
    item: Option<std::result::Result<Delivery, LapinError>>,
    queue: &str,
) -> Result<Delivery> {
    match item {
        Some(Ok(d)) => Ok(d),
        Some(Err(e)) => Err(ShoveError::Connection(format!(
            "consumer stream error on {queue}: {e}"
        ))),
        None => Err(ShoveError::Connection(format!(
            "consumer stream closed for {queue}"
        ))),
    }
}

/// Run `f` in a reconnect loop, retrying on transient errors until shutdown.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    queue: &str,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                warn!("consumer error on {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                tokio::select! {
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RabbitMqConsumer
// ---------------------------------------------------------------------------

pub struct RabbitMqConsumer {
    client: RabbitMqClient,
}

impl RabbitMqConsumer {
    pub fn new(client: RabbitMqClient) -> Self {
        Self { client }
    }

    async fn run_internal<T, H>(
        &self,
        handler: &H,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        run_with_reconnect(&options.shutdown, queue, || {
            self.consume_loop::<T, H>(handler, queue, topology, &options)
        })
        .await
    }

    /// Like `run_internal` but maintains a per-sub-queue poisoned-key set
    /// across reconnections for `SequenceFailure::FailAll` enforcement.
    async fn run_internal_sequenced<T, H>(
        &self,
        handler: &H,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: ConsumerOptions,
        on_failure: SequenceFailure,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        let mut poisoned_keys = HashSet::new();
        loop {
            match self
                .consume_loop_sequenced::<T, H>(
                    handler,
                    queue,
                    topology,
                    &options,
                    on_failure,
                    &mut poisoned_keys,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if options.shutdown.is_cancelled() {
                        return Ok(());
                    }
                    warn!("consumer error on {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                    tokio::select! {
                        _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                        _ = options.shutdown.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    async fn consume_loop_sequenced<T, H>(
        &self,
        handler: &H,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        let (channel, mut stream) = open_consumer(&self.client, queue, 1).await?;
        let publisher = ChannelPublisher::new(channel);

        info!("sequenced consumer started on sub-queue {queue}");

        loop {
            tokio::select! {
                _ = options.shutdown.cancelled() => {
                    debug!("shutdown signal received, stopping sequenced consumer on {queue}");
                    return Ok(());
                }
                item = stream.next() => {
                    let delivery = unwrap_delivery(item, queue)?;

                    let retry_count = get_retry_count(&delivery);

                    // FailAll: skip messages whose sequence key has been poisoned.
                    if on_failure == SequenceFailure::FailAll
                        && poisoned_keys.contains(delivery.routing_key.as_str())
                    {
                        warn!(
                            sequence_key = %delivery.routing_key,
                            queue = %queue,
                            "message with poisoned sequence key, sending to DLQ"
                        );
                        router::route_reject(&delivery).await;
                        continue;
                    }

                    if retry_count >= options.max_retries {
                        warn!(
                            queue = %queue,
                            retry_count,
                            max_retries = options.max_retries,
                            "message exceeded max retries, sending to DLQ"
                        );
                        if on_failure == SequenceFailure::FailAll {
                            info!(
                                sequence_key = %delivery.routing_key,
                                queue = %queue,
                                "poisoning sequence key (FailAll)"
                            );
                            poisoned_keys.insert(delivery.routing_key.to_string());
                        }
                        router::route_reject(&delivery).await;
                        continue;
                    }

                    let metadata = extract_message_metadata(&delivery);
                    let outcome = match serde_json::from_slice::<T::Message>(&delivery.data) {
                        Err(err) => {
                            error!(
                                error = %err,
                                delivery_id = %metadata.delivery_id,
                                "Failed to deserialize message from sequenced queue"
                            );
                            Outcome::Reject
                        }
                        Ok(message) => {
                            options.processing.store(true, Ordering::Release);
                            invoke_handler(handler.handle(message, metadata), options.handler_timeout).await
                        }
                    };

                    match outcome {
                        Outcome::Ack => {
                            router::route_ack(&delivery).await;
                        }
                        Outcome::Retry => {
                            router::route_retry(&delivery, topology, &publisher, retry_count).await;
                        }
                        Outcome::Reject => {
                            if on_failure == SequenceFailure::FailAll {
                                info!(
                                    sequence_key = %delivery.routing_key,
                                    queue = %queue,
                                    "poisoning sequence key (FailAll)"
                                );
                                poisoned_keys.insert(delivery.routing_key.to_string());
                            }
                            router::route_reject(&delivery).await;
                        }
                        Outcome::Defer => {
                            router::route_defer(&delivery, topology, &publisher).await;
                        }
                    }
                    options.processing.store(false, Ordering::Release);
                }
            }
        }
    }

    async fn consume_loop<T, H>(
        &self,
        handler: &H,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: &ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        let (channel, mut stream) =
            open_consumer(&self.client, queue, options.prefetch_count).await?;
        let publisher = ChannelPublisher::new(channel);

        info!("consumer started on queue {queue}");

        loop {
            tokio::select! {
                _ = options.shutdown.cancelled() => {
                    debug!("shutdown signal received, stopping consumer on {queue}");
                    return Ok(());
                }
                item = stream.next() => {
                    let delivery = unwrap_delivery(item, queue)?;

                    let retry_count = get_retry_count(&delivery);

                    if retry_count >= options.max_retries {
                        warn!(
                            "message on {queue} exceeded max retries ({}/{}), sending to DLQ",
                            retry_count, options.max_retries
                        );
                        router::route_reject(&delivery).await;
                        continue;
                    }

                    let metadata = extract_message_metadata(&delivery);
                    let outcome = match serde_json::from_slice::<T::Message>(&delivery.data) {
                        Err(err) => {
                            error!(
                                error = %err,
                                delivery_id = %metadata.delivery_id,
                                "Failed to deserialize message from main queue"
                            );
                            Outcome::Reject
                        }
                        Ok(message) => {
                            options.processing.store(true, Ordering::Release);
                            let outcome = invoke_handler(handler.handle(message, metadata), options.handler_timeout).await;
                            debug!(queue, ?outcome, retry_count, "message handled");
                            outcome
                        }
                    };

                    match outcome {
                        Outcome::Ack => {
                            router::route_ack(&delivery).await;
                        }
                        Outcome::Retry => {
                            router::route_retry(&delivery, topology, &publisher, retry_count).await;
                        }
                        Outcome::Reject => {
                            router::route_reject(&delivery).await;
                        }
                        Outcome::Defer => {
                            router::route_defer(&delivery, topology, &publisher).await;
                        }
                    }
                    options.processing.store(false, Ordering::Release);
                }
            }
        }
    }
}

/// Consume a DLQ, deserializing each message inline and calling `handler.handle_dead`.
/// Always acks after handling (or on deserialization failure).
async fn consume_dlq_loop<T, H>(
    client: &RabbitMqClient,
    handler: &H,
    dlq: &str,
    options: &ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    T::Message: for<'de> serde::Deserialize<'de>,
    H: MessageHandler<T>,
{
    let (_channel, mut stream) = open_consumer(client, dlq, options.prefetch_count).await?;

    info!("DLQ consumer started on queue {dlq}");

    loop {
        tokio::select! {
            _ = options.shutdown.cancelled() => {
                debug!("shutdown signal received, stopping DLQ consumer on {dlq}");
                return Ok(());
            }
            item = stream.next() => {
                let delivery = unwrap_delivery(item, dlq)?;

                let metadata = extract_dead_metadata(&delivery);

                match serde_json::from_slice::<T::Message>(&delivery.data) {
                    Err(err) => {
                        error!(
                            error = %err,
                            delivery_id = %metadata.message.delivery_id,
                            "Failed to deserialize message from dead letter queue — discarding"
                        );
                    }
                    Ok(message) => {
                        handler.handle_dead(message, metadata).await;
                    }
                }

                // Always ack DLQ messages.
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack DLQ delivery: {e}");
                }
            }
        }
    }
}

/// Run the handler future with an optional timeout.
/// Returns `Outcome::Retry` if the timeout is exceeded.
async fn invoke_handler(fut: impl Future<Output = Outcome>, timeout: Option<Duration>) -> Outcome {
    match timeout {
        Some(duration) => tokio::time::timeout(duration, fut)
            .await
            .unwrap_or_else(|_elapsed| {
                warn!("handler exceeded timeout ({duration:?}), retrying message");
                Outcome::Retry
            }),
        None => fut.await,
    }
}

impl Consumer for RabbitMqConsumer {
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let consumer = RabbitMqConsumer::new(client);
            consumer
                .run_internal::<T, _>(&handler, topology.queue(), topology, options)
                .await
        }
    }

    fn run_sequenced<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let seq = topology.sequencing().ok_or_else(|| {
                ShoveError::Topology(
                    "run_sequenced called on topic without sequencing config".into(),
                )
            })?;

            let on_failure = seq.on_failure();
            let handler = Arc::new(handler);
            let shutdown = options.shutdown.clone();
            let mut handles = Vec::with_capacity(seq.routing_shards() as usize);

            for i in 0..seq.routing_shards() {
                let sub_queue = format!("{}-seq-{i}", topology.queue());
                let h = handler.clone();
                let inner_client = client.clone();
                let opts = ConsumerOptions::new(shutdown.clone())
                    .with_max_retries(options.max_retries)
                    .with_prefetch_count(1);
                handles.push(tokio::spawn(async move {
                    let consumer = RabbitMqConsumer::new(inner_client);
                    consumer
                        .run_internal_sequenced::<T, _>(&*h, &sub_queue, topology, opts, on_failure)
                        .await
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!("sequenced consumer sub-task failed: {e}"),
                    Err(e) => error!("sequenced consumer task panicked: {e}"),
                }
            }

            Ok(())
        }
    }

    fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let dlq = topology.dlq().ok_or_else(|| {
                ShoveError::Topology("run_dlq called on topic without DLQ".into())
            })?;
            let shutdown = client.shutdown_token();
            let options = ConsumerOptions::new(shutdown);

            run_with_reconnect(&options.shutdown, dlq, || {
                consume_dlq_loop::<T, _>(&client, &handler, dlq, &options)
            })
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invoke_handler_returns_outcome_without_timeout() {
        let outcome = invoke_handler(async { Outcome::Ack }, None).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn invoke_handler_returns_outcome_within_timeout() {
        let timeout = Some(Duration::from_secs(1));
        let outcome = invoke_handler(async { Outcome::Reject }, timeout).await;
        assert!(matches!(outcome, Outcome::Reject));
    }

    #[tokio::test]
    async fn invoke_handler_returns_retry_on_timeout() {
        let timeout = Some(Duration::from_millis(10));
        let outcome = invoke_handler(
            async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Outcome::Ack
            },
            timeout,
        )
        .await;
        assert!(matches!(outcome, Outcome::Retry));
    }
}
