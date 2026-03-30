use std::collections::HashMap;
use std::sync::Arc;

use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel};
use tokio::sync::Mutex;

use tracing::{debug, warn};

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::error::{Result, ShoveError};
use crate::publisher::Publisher;
use crate::topic::Topic;

const DELIVERY_MODE_PERSISTENT: u8 = 2;

#[derive(Clone)]
pub struct RabbitMqPublisher {
    client: RabbitMqClient,
    channel: Arc<Mutex<Channel>>,
}

impl RabbitMqPublisher {
    pub async fn new(client: RabbitMqClient) -> Result<Self> {
        let channel = client.create_confirm_channel().await?;
        Ok(Self {
            client,
            channel: Arc::new(Mutex::new(channel)),
        })
    }

    async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        headers: Option<FieldTable>,
    ) -> Result<()> {
        let mut channel_guard = self.channel.lock().await;

        debug!(
            exchange,
            routing_key,
            bytes = payload.len(),
            "publishing message"
        );

        let result = Self::do_publish(
            &channel_guard,
            exchange,
            routing_key,
            payload,
            headers.clone(),
        )
        .await;

        match result {
            Ok(()) => {
                debug!(exchange, routing_key, "message published and confirmed");
                Ok(())
            }
            Err(e) => {
                warn!(exchange, routing_key, error = %e, "publish failed, recovering channel and retrying");
                let fresh = self.client.create_confirm_channel().await?;
                *channel_guard = fresh;

                Self::do_publish(&channel_guard, exchange, routing_key, payload, headers).await
            }
        }
    }

    async fn do_publish(
        channel: &Channel,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        headers: Option<FieldTable>,
    ) -> Result<()> {
        let mut props = BasicProperties::default()
            .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
            .with_content_type("application/json".into());

        if let Some(h) = headers {
            props = props.with_headers(h);
        }

        let confirm = channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                payload,
                props,
            )
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        if confirm.is_nack() {
            return Err(ShoveError::Connection(
                "broker NACKed the published message".to_string(),
            ));
        }

        Ok(())
    }

    async fn publish_batch_raw(&self, exchange: &str, items: &[(&str, Vec<u8>)]) -> Result<()> {
        let channel_guard = self.channel.lock().await;

        debug!(exchange, count = items.len(), "publishing batch");

        let result = Self::do_publish_batch(&channel_guard, exchange, items).await;

        match result {
            Ok(()) => {
                debug!(
                    exchange,
                    count = items.len(),
                    "batch published and confirmed"
                );
                Ok(())
            }
            Err(e) => {
                warn!(exchange, error = %e, "batch publish failed, recovering channel and retrying");
                drop(channel_guard);
                let mut channel_guard = self.channel.lock().await;
                let fresh = self.client.create_confirm_channel().await?;
                *channel_guard = fresh;

                Self::do_publish_batch(&channel_guard, exchange, items).await
            }
        }
    }

    async fn do_publish_batch(
        channel: &Channel,
        exchange: &str,
        items: &[(&str, Vec<u8>)],
    ) -> Result<()> {
        // Phase 1: Send all messages, collecting confirmation futures.
        let mut confirms = Vec::with_capacity(items.len());
        for (routing_key, payload) in items {
            let props = BasicProperties::default()
                .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
                .with_content_type("application/json".into());

            let confirm = channel
                .basic_publish(
                    exchange.into(),
                    (*routing_key).into(),
                    BasicPublishOptions::default(),
                    payload,
                    props,
                )
                .await
                .map_err(|e| ShoveError::Connection(e.to_string()))?;

            confirms.push(confirm);
        }

        // Phase 2: Await all confirmations.
        for confirm in confirms {
            let result = confirm
                .await
                .map_err(|e| ShoveError::Connection(e.to_string()))?;

            if result.is_nack() {
                return Err(ShoveError::Connection(
                    "broker NACKed a batch message".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Publisher for RabbitMqPublisher {
    fn publish<T: Topic>(
        &self,
        message: &T::Message,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let payload = serde_json::to_vec(message).map_err(ShoveError::Serialization);

        let topology = T::topology();
        let sequencing = topology.sequencing();
        let key_fn = T::SEQUENCE_KEY_FN;

        async move {
            let payload = payload?;

            match (sequencing, key_fn) {
                (Some(seq), Some(kf)) => {
                    let routing_key = kf(message);
                    self.publish_raw(seq.exchange(), &routing_key, &payload, None)
                        .await
                }
                (Some(_), None) => Err(ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                )),
                (None, _) => self.publish_raw("", topology.queue(), &payload, None).await,
            }
        }
    }

    fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let payload = serde_json::to_vec(message).map_err(ShoveError::Serialization);
        let field_table = hashmap_to_field_table(headers);

        let topology = T::topology();
        let sequencing = topology.sequencing();
        let key_fn = T::SEQUENCE_KEY_FN;

        async move {
            let payload = payload?;

            match (sequencing, key_fn) {
                (Some(seq), Some(kf)) => {
                    let routing_key = kf(message);
                    self.publish_raw(seq.exchange(), &routing_key, &payload, Some(field_table))
                        .await
                }
                (Some(_), None) => Err(ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                )),
                (None, _) => {
                    self.publish_raw("", topology.queue(), &payload, Some(field_table))
                        .await
                }
            }
        }
    }

    fn publish_batch<T: Topic>(
        &self,
        messages: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send {
        let topology = T::topology();
        let sequencing = topology.sequencing();
        let key_fn = T::SEQUENCE_KEY_FN;

        // Serialize all messages up front for fail-fast behaviour.
        let serialized: Result<Vec<Vec<u8>>> = messages
            .iter()
            .map(|m| serde_json::to_vec(m).map_err(ShoveError::Serialization))
            .collect();

        // Pre-compute routing keys while we still have access to messages.
        let routing_keys: Option<Vec<String>> = key_fn.map(|kf| messages.iter().map(kf).collect());

        async move {
            let payloads = serialized?;

            match (sequencing, routing_keys) {
                (Some(seq), Some(keys)) => {
                    let items: Vec<(&str, Vec<u8>)> = keys
                        .iter()
                        .zip(payloads)
                        .map(|(k, p)| (k.as_str(), p))
                        .collect();
                    self.publish_batch_raw(seq.exchange(), &items).await
                }
                (Some(_), None) => Err(ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                )),
                (None, _) => {
                    let queue = topology.queue();
                    let items: Vec<(&str, Vec<u8>)> =
                        payloads.into_iter().map(|p| (queue, p)).collect();
                    self.publish_batch_raw("", &items).await
                }
            }
        }
    }
}

fn hashmap_to_field_table(headers: HashMap<String, String>) -> FieldTable {
    let mut table = FieldTable::default();
    for (k, v) in headers {
        table.insert(k.into(), AMQPValue::LongString(v.into()));
    }
    table
}

pub(crate) struct ChannelPublisher {
    channel: Channel,
}

impl ChannelPublisher {
    pub(crate) fn new(channel: Channel) -> Self {
        Self { channel }
    }

    pub(crate) async fn publish_to_queue(
        &self,
        queue: &str,
        payload: &[u8],
        headers: FieldTable,
    ) -> Result<()> {
        let props = BasicProperties::default()
            .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
            .with_content_type("application/json".into())
            .with_headers(headers);

        self.channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                payload,
                props,
            )
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        Ok(())
    }
}
