use std::collections::HashSet;
use std::sync::Arc;

use futures_lite::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions};
use lapin::types::{AMQPValue, FieldTable};
use tracing::{debug, error, info, warn};

use crate::RECONNECT_DELAY;
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::backends::rabbitmq::typed::{
    DlqAdapter, RETRY_COUNT_KEY, RawHandler, TypedAdapter, get_retry_count,
};
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::ShoveError;
use crate::handler::MessageHandler;
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::SequenceFailure;

pub struct RabbitMqConsumer {
    client: RabbitMqClient,
}

impl RabbitMqConsumer {
    pub fn new(client: RabbitMqClient) -> Self {
        Self { client }
    }

    async fn run_internal(
        &self,
        handler: impl RawHandler,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: ConsumerOptions,
        is_dlq: bool,
    ) -> Result<(), ShoveError> {
        loop {
            match self
                .consume_loop(&handler, queue, topology, &options, is_dlq)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if options.shutdown.is_cancelled() {
                        return Ok(());
                    }
                    warn!(
                        "consumer error on queue {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                        _ = options.shutdown.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    /// Like `run_internal` but maintains a per-sub-queue poisoned-key set
    /// across reconnections for `SequenceFailure::FailAll` enforcement.
    async fn run_internal_sequenced(
        &self,
        handler: impl RawHandler,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: ConsumerOptions,
        on_failure: SequenceFailure,
    ) -> Result<(), ShoveError> {
        let mut poisoned_keys = HashSet::new();
        loop {
            match self
                .consume_loop_sequenced(
                    &handler,
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
                    warn!(
                        "sequenced consumer error on queue {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                        _ = options.shutdown.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    async fn consume_loop_sequenced(
        &self,
        handler: &impl RawHandler,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
    ) -> Result<(), ShoveError> {
        let channel = self.client.create_confirm_channel().await?;

        channel
            .basic_qos(1, BasicQosOptions::default())
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to set QoS: {e}")))?;

        let publisher = ChannelPublisher::new(channel.clone());

        let mut consumer = channel
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
            .map_err(|e| {
                ShoveError::Connection(format!("failed to start consumer on {queue}: {e}"))
            })?;

        info!("sequenced consumer started on sub-queue {queue}");

        loop {
            tokio::select! {
                _ = options.shutdown.cancelled() => {
                    debug!("shutdown signal received, stopping sequenced consumer on {queue}");
                    return Ok(());
                }
                item = consumer.next() => {
                    let delivery = match item {
                        Some(Ok(d)) => d,
                        Some(Err(e)) => {
                            return Err(ShoveError::Connection(format!("consumer stream error on {queue}: {e}")));
                        }
                        None => {
                            return Err(ShoveError::Connection(format!("consumer stream closed for {queue}")));
                        }
                    };

                    let sequence_key = delivery.routing_key.to_string();
                    let retry_count = get_retry_count(&delivery);

                    // FailAll: skip messages whose sequence key has been poisoned.
                    if poisoned_keys.contains(&sequence_key) {
                        warn!(
                            sequence_key = %sequence_key,
                            queue = %queue,
                            "message with poisoned sequence key, sending to DLQ"
                        );
                        nack_reject(&delivery).await;
                        continue;
                    }

                    // Max retries exceeded → treat as permanent rejection.
                    if retry_count >= options.max_retries {
                        warn!(
                            queue = %queue,
                            retry_count,
                            max_retries = options.max_retries,
                            "message exceeded max retries, sending to DLQ"
                        );
                        if on_failure == SequenceFailure::FailAll {
                            info!(
                                sequence_key = %sequence_key,
                                queue = %queue,
                                "poisoning sequence key (FailAll)"
                            );
                            poisoned_keys.insert(sequence_key);
                        }
                        nack_reject(&delivery).await;
                        continue;
                    }

                    let outcome = handler.handle(&delivery).await;

                    match outcome {
                        Outcome::Ack => {
                            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                error!("failed to ack delivery: {e}");
                            }
                        }
                        Outcome::Retry => {
                            handle_retry(&delivery, topology, &publisher, retry_count).await;
                        }
                        Outcome::Reject => {
                            if on_failure == SequenceFailure::FailAll {
                                info!(
                                    sequence_key = %sequence_key,
                                    queue = %queue,
                                    "poisoning sequence key (FailAll)"
                                );
                                poisoned_keys.insert(sequence_key);
                            }
                            nack_reject(&delivery).await;
                        }
                        Outcome::Defer => {
                            handle_defer(&delivery, topology, &publisher, retry_count).await;
                        }
                    }
                }
            }
        }
    }

    async fn consume_loop(
        &self,
        handler: &impl RawHandler,
        queue: &str,
        topology: &'static crate::topology::QueueTopology,
        options: &ConsumerOptions,
        is_dlq: bool,
    ) -> Result<(), ShoveError> {
        let channel = self.client.create_confirm_channel().await?;

        channel
            .basic_qos(options.prefetch_count, BasicQosOptions::default())
            .await
            .map_err(|e| ShoveError::Connection(format!("failed to set QoS: {e}")))?;

        let publisher = ChannelPublisher::new(channel.clone());

        let mut consumer = channel
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
            .map_err(|e| {
                ShoveError::Connection(format!("failed to start consumer on {queue}: {e}"))
            })?;

        info!("consumer started on queue {queue}");

        loop {
            tokio::select! {
                _ = options.shutdown.cancelled() => {
                    debug!("shutdown signal received, stopping consumer on {queue}");
                    return Ok(());
                }
                item = consumer.next() => {
                    let delivery = match item {
                        Some(Ok(d)) => d,
                        Some(Err(e)) => {
                            return Err(ShoveError::Connection(format!("consumer stream error on {queue}: {e}")));
                        }
                        None => {
                            return Err(ShoveError::Connection(format!("consumer stream closed for {queue}")));
                        }
                    };

                    let retry_count = get_retry_count(&delivery);

                    if !is_dlq && retry_count >= options.max_retries {
                        warn!(
                            "message on {queue} exceeded max retries ({}/{}), sending to DLQ",
                            retry_count, options.max_retries
                        );
                        nack_reject(&delivery).await;
                        continue;
                    }

                    let outcome = handler.handle(&delivery).await;

                    match outcome {
                        Outcome::Ack => {
                            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                error!("failed to ack delivery: {e}");
                            }
                        }
                        Outcome::Retry => {
                            handle_retry(&delivery, topology, &publisher, retry_count).await;
                        }
                        Outcome::Reject => {
                            nack_reject(&delivery).await;
                        }
                        Outcome::Defer => {
                            handle_defer(&delivery, topology, &publisher, retry_count).await;
                        }
                    }
                }
            }
        }
    }
}

async fn handle_retry(
    delivery: &lapin::message::Delivery,
    topology: &'static crate::topology::QueueTopology,
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
                if let Err(ne) = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..BasicNackOptions::default()
                    })
                    .await
                {
                    error!("failed to nack delivery: {ne}");
                }
            }
        }
    } else {
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
}

async fn handle_defer(
    delivery: &lapin::message::Delivery,
    topology: &'static crate::topology::QueueTopology,
    publisher: &ChannelPublisher,
    _retry_count: u32,
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
                if let Err(ne) = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..BasicNackOptions::default()
                    })
                    .await
                {
                    error!("failed to nack delivery: {ne}");
                }
            }
        }
    } else {
        warn!(
            "Defer outcome on queue {} but no hold queues configured, requeuing",
            topology.queue()
        );
        if let Err(e) = delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..BasicNackOptions::default()
            })
            .await
        {
            error!("failed to nack delivery for defer fallback: {e}");
        }
    }
}

fn clone_headers_with_retry(delivery: &lapin::message::Delivery, retry_count: u32) -> FieldTable {
    let mut table = clone_headers(delivery);
    table.insert(RETRY_COUNT_KEY.into(), AMQPValue::LongUInt(retry_count));
    table
}

fn clone_headers(delivery: &lapin::message::Delivery) -> FieldTable {
    delivery
        .properties
        .headers()
        .as_ref()
        .cloned()
        .unwrap_or_default()
}

async fn nack_reject(delivery: &lapin::message::Delivery) {
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

impl Consumer for RabbitMqConsumer {
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl std::future::Future<Output = Result<(), ShoveError>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let adapter = TypedAdapter::<_, T>::new(handler);
            let consumer = RabbitMqConsumer::new(client);
            consumer
                .run_internal(adapter, topology.queue(), topology, options, false)
                .await
        }
    }

    fn run_sequenced<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl std::future::Future<Output = Result<(), ShoveError>> + Send {
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
            let mut handles = Vec::new();

            for i in 0..seq.routing_shards() {
                let sub_queue = format!("{}-seq-{i}", topology.queue());
                let h = handler.clone();
                let inner_client = client.clone();
                let opts = ConsumerOptions {
                    max_retries: options.max_retries,
                    prefetch_count: 1,
                    shutdown: shutdown.clone(),
                };
                handles.push(tokio::spawn(async move {
                    let adapter = TypedAdapter::<_, T>::new(h);
                    let consumer = RabbitMqConsumer::new(inner_client);
                    consumer
                        .run_internal_sequenced(adapter, &sub_queue, topology, opts, on_failure)
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
    ) -> impl std::future::Future<Output = Result<(), ShoveError>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let dlq = topology.dlq().ok_or_else(|| {
                ShoveError::Topology("run_dlq called on topic without DLQ".into())
            })?;
            let adapter = DlqAdapter::<_, T>::new(handler);
            let shutdown = client.shutdown_token();
            let options = ConsumerOptions::new(shutdown);
            let consumer = RabbitMqConsumer::new(client);
            consumer
                .run_internal(adapter, dlq, topology, options, true)
                .await
        }
    }
}
