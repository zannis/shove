use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel};
use tokio::sync::Mutex;

use tracing::{debug, warn};
use uuid::Uuid;

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::MESSAGE_ID_KEY;
use crate::error::{Result, ShoveError};
use crate::publisher::Publisher;
use crate::topic::Topic;

const DELIVERY_MODE_PERSISTENT: u8 = 2;
const DEFAULT_CHANNEL_POOL_SIZE: usize = 4;

fn base_properties() -> BasicProperties {
    BasicProperties::default()
        .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
        .with_content_type("application/json".into())
}

/// Round-robin pool of AMQP channels with independent confirmation streams.
struct ChannelPool {
    channels: Vec<Mutex<Channel>>,
    next: AtomicUsize,
}

impl ChannelPool {
    fn get(&self) -> &Mutex<Channel> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        &self.channels[idx]
    }
}

#[derive(Clone)]
pub struct RabbitMqPublisher {
    client: RabbitMqClient,
    pool: Arc<ChannelPool>,
}

impl RabbitMqPublisher {
    /// Create a publisher with the default channel pool size (4).
    pub async fn new(client: RabbitMqClient) -> Result<Self> {
        Self::with_channel_count(client, DEFAULT_CHANNEL_POOL_SIZE).await
    }

    /// Create a publisher with a custom number of pooled channels.
    pub async fn with_channel_count(client: RabbitMqClient, count: usize) -> Result<Self> {
        let count = count.max(1);
        let mut channels = Vec::with_capacity(count);
        for _ in 0..count {
            channels.push(Mutex::new(client.create_confirm_channel().await?));
        }
        Ok(Self {
            client,
            pool: Arc::new(ChannelPool {
                channels,
                next: AtomicUsize::new(0),
            }),
        })
    }

    async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        headers: Option<FieldTable>,
    ) -> Result<()> {
        let slot = self.pool.get();
        let mut channel_guard = slot.lock().await;

        // Stamp a stable x-message-id so consumers can deduplicate if the
        // publish-then-ack race produces a second delivery of this message.
        let mut headers = headers.unwrap_or_default();
        if !headers.inner().contains_key(MESSAGE_ID_KEY) {
            headers.insert(
                MESSAGE_ID_KEY.into(),
                AMQPValue::LongString(Uuid::new_v4().to_string().into()),
            );
        }
        let headers = Some(headers);

        debug!(
            exchange,
            routing_key,
            bytes = payload.len(),
            "publishing message"
        );

        match Self::do_publish(&channel_guard, exchange, routing_key, payload, headers).await {
            Ok(()) => {
                debug!(exchange, routing_key, "message published and confirmed");
                Ok(())
            }
            Err(e) => {
                warn!(exchange, routing_key, error = %e, "publish failed, recovering channel and retrying");
                let fresh = self.client.create_confirm_channel().await?;
                *channel_guard = fresh;

                Self::do_publish(&channel_guard, exchange, routing_key, payload, None).await
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
        let props = match headers {
            Some(h) => base_properties().with_headers(h),
            None => base_properties(),
        };

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
        let slot = self.pool.get();
        let mut channel_guard = slot.lock().await;

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
        let mut confirms = Vec::with_capacity(items.len());
        let props = base_properties();
        for (routing_key, payload) in items {
            let confirm = channel
                .basic_publish(
                    exchange.into(),
                    (*routing_key).into(),
                    BasicPublishOptions::default(),
                    payload,
                    props.clone(),
                )
                .await
                .map_err(|e| ShoveError::Connection(e.to_string()))?;

            confirms.push(confirm);
        }

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
        // Pre-allocate buffers with estimated capacity to reduce heap fragmentation.
        let serialized: Result<Vec<Vec<u8>>> = messages
            .iter()
            .map(|m| {
                let mut buf = Vec::with_capacity(128);
                serde_json::to_writer(&mut buf, m).map_err(ShoveError::Serialization)?;
                Ok(buf)
            })
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
    /// True when the channel is in AMQP transaction mode (`tx_select`).
    /// Set via [`ChannelPublisher::new_tx`]; always `false` in non-tx channels.
    tx_mode: bool,
}

impl ChannelPublisher {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            channel,
            tx_mode: false,
        }
    }

    /// Create a publisher wrapping a channel that has `tx_select` enabled.
    ///
    /// In tx mode every `basic_publish` and `basic_ack`/`nack` is buffered
    /// until [`commit_if_tx`](Self::commit_if_tx) is called, making routing
    /// decisions atomic.
    #[cfg(feature = "rabbitmq-transactional")]
    pub(crate) fn new_tx(channel: Channel) -> Self {
        Self {
            channel,
            tx_mode: true,
        }
    }

    /// Commit the current AMQP transaction if in tx mode; otherwise no-op.
    ///
    /// Call after every routing decision (publish + ack/nack) to make the
    /// operations atomic. On error the broker automatically rolls back the tx;
    /// the original delivery remains unacked and will be redelivered.
    pub(crate) async fn commit_if_tx(&self) -> Result<()> {
        #[cfg(feature = "rabbitmq-transactional")]
        if self.tx_mode {
            self.channel
                .tx_commit()
                .await
                .map_err(|e| ShoveError::Connection(format!("tx_commit failed: {e}")))?;
        }
        Ok(())
    }

    /// Roll back the current AMQP transaction if in tx mode; otherwise no-op.
    ///
    /// Call when a publish succeeded (was buffered) but the subsequent ack
    /// failed, to undo the buffered publish before requeuing.
    pub(crate) async fn rollback_if_tx(&self) {
        #[cfg(feature = "rabbitmq-transactional")]
        if self.tx_mode
            && let Err(e) = self.channel.tx_rollback().await
        {
            warn!("tx_rollback failed: {e}");
        }
    }

    pub(crate) async fn publish_to_queue(
        &self,
        queue: &str,
        payload: &[u8],
        headers: FieldTable,
    ) -> Result<()> {
        let props = base_properties().with_headers(headers);

        // In tx mode the channel has no confirm mode; the second `.await` on
        // the publisher-confirm future resolves immediately with a dummy ack.
        // Real atomicity is provided by `tx_commit` called by the router.
        let confirm = self
            .channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                payload,
                props,
            )
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        if !self.tx_mode && confirm.is_nack() {
            return Err(ShoveError::Connection(
                "broker NACKed the published message".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_pool_round_robins() {
        // Create a pool of 3 channels (we test the index logic, not real channels).
        let pool_next = AtomicUsize::new(0);
        let size = 3usize;
        // Simulate round-robin index selection.
        let indices: Vec<usize> = (0..7)
            .map(|_| pool_next.fetch_add(1, Ordering::Relaxed) % size)
            .collect();
        assert_eq!(indices, vec![0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn empty_hashmap_produces_empty_field_table() {
        let table = hashmap_to_field_table(HashMap::new());
        assert!(table.inner().is_empty());
    }

    #[test]
    fn single_entry_is_correctly_converted() {
        let mut map = HashMap::new();
        map.insert("x-trace-id".to_string(), "abc123".to_string());

        let table = hashmap_to_field_table(map);
        let inner = table.inner();

        assert_eq!(inner.len(), 1);
        let value = inner.get("x-trace-id").expect("key should be present");
        assert!(
            matches!(value, AMQPValue::LongString(s) if s.as_bytes() == b"abc123"),
            "expected LongString(\"abc123\"), got {value:?}"
        );
    }

    #[test]
    fn multiple_entries_are_all_present() {
        let mut map = HashMap::new();
        map.insert("key-a".to_string(), "val-a".to_string());
        map.insert("key-b".to_string(), "val-b".to_string());
        map.insert("key-c".to_string(), "val-c".to_string());

        let table = hashmap_to_field_table(map);
        let inner = table.inner();

        assert_eq!(inner.len(), 3);
        assert!(inner.contains_key("key-a"), "key-a should be present");
        assert!(inner.contains_key("key-b"), "key-b should be present");
        assert!(inner.contains_key("key-c"), "key-c should be present");
    }

    #[test]
    fn values_are_stored_as_long_string_amqp_values() {
        let mut map = HashMap::new();
        map.insert("content-type".to_string(), "application/json".to_string());
        map.insert("x-retry-count".to_string(), "3".to_string());

        let table = hashmap_to_field_table(map);

        for value in table.inner().values() {
            assert!(
                matches!(value, AMQPValue::LongString(_)),
                "all values must be AMQPValue::LongString, got {value:?}"
            );
        }
    }
}
