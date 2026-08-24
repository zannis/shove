use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel};
use tokio::sync::Mutex;

use tracing::{debug, warn};
use uuid::Uuid;

use crate::backend::PublisherImpl;
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::MESSAGE_ID_KEY;
use crate::backends::rabbitmq::map_lapin_error;
use crate::backends::rabbitmq::topology::broadcast_exchange;
use crate::batch::BatchReport;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
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

        // Stamp a stable x-message-id so consumers can deduplicate if the
        // publish-then-ack race produces a second delivery of this message.
        let mut headers = headers.unwrap_or_default();
        if !headers.inner().contains_key(MESSAGE_ID_KEY) {
            let mut buf = [0u8; 36];
            let id = Uuid::new_v4().hyphenated().encode_lower(&mut buf);
            headers.insert(
                MESSAGE_ID_KEY.into(),
                AMQPValue::LongString(id.as_bytes().into()),
            );
        }
        let headers = Some(headers);

        debug!(
            exchange,
            routing_key,
            bytes = payload.len(),
            "publishing message"
        );

        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let mut last_err = None;

        for attempt in 0..3u32 {
            // Clone the channel while holding the lock, then release
            // immediately. `lapin::Channel` is Arc-backed — cloning is
            // O(1) and the clone shares the same underlying AMQP channel.
            // Releasing before `do_publish` means the mutex is not held
            // during `basic_publish` or the confirm round-trip, so
            // concurrent callers on the same slot can pipeline their
            // publishes instead of serializing for the full RTT.
            let channel = slot.lock().await.clone();
            match Self::do_publish(&channel, exchange, routing_key, payload, &headers).await {
                Ok(()) => {
                    debug!(exchange, routing_key, "message published and confirmed");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        exchange, routing_key, attempt, error = %e,
                        "publish failed, recovering channel"
                    );
                    last_err = Some(e);
                    if attempt < 2 {
                        let delay = backoff.next().expect("backoff is infinite");
                        tokio::time::sleep(delay).await;
                        let fresh = self.client.create_confirm_channel().await?;
                        *slot.lock().await = fresh;
                    }
                }
            }
        }

        Err(last_err.expect("loop ran at least once"))
    }

    async fn do_publish(
        channel: &Channel,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        headers: &Option<FieldTable>,
    ) -> Result<()> {
        let props = match headers {
            Some(h) => base_properties().with_headers(h.clone()),
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
            .map_err(|e| map_lapin_error("publish failed", e))?
            .await
            .map_err(|e| map_lapin_error("publish confirm failed", e))?;

        if confirm.is_nack() {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Publish,
            );
            return Err(ShoveError::Connection(
                "broker NACKed the published message".to_string(),
            ));
        }

        Ok(())
    }

    /// Returns the per-index report for the batch. On success nothing is
    /// outstanding; on failure the report reflects the **final** attempt
    /// (retries replay the full batch on a fresh channel, so the previous
    /// attempts' confirmations are no longer attributable to a single
    /// publish call).
    async fn publish_batch_raw(
        &self,
        exchange: &str,
        items: &[(&str, bytes::Bytes)],
    ) -> BatchReport {
        let slot = self.pool.get();

        debug!(exchange, count = items.len(), "publishing batch");

        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let mut last = BatchReport::all_succeeded();

        for attempt in 0..3u32 {
            // Clone the channel before releasing the lock — same rationale as
            // `publish_raw`: the mutex is held only for the Arc clone, not for
            // the network round-trips inside `do_publish_batch`.
            let channel = slot.lock().await.clone();
            let report = Self::do_publish_batch(&channel, exchange, items).await;
            match report.first_err() {
                None => {
                    debug!(
                        exchange,
                        count = items.len(),
                        "batch published and confirmed"
                    );
                    return report;
                }
                Some(e) => {
                    warn!(exchange, attempt, error = %e, "batch publish failed, recovering channel");
                    last = report;
                    if attempt < 2 {
                        let delay = backoff.next().expect("backoff is infinite");
                        tokio::time::sleep(delay).await;
                        match self.client.create_confirm_channel().await {
                            Ok(fresh) => *slot.lock().await = fresh,
                            // The last attempt's indices are still what needs
                            // re-publishing; the channel error is what the
                            // caller should see.
                            Err(e) => return last.with_error(e, items.len()),
                        }
                    }
                }
            }
        }

        last
    }

    /// Submit every message and await each publisher confirm, reporting which
    /// indices were rejected and which are outstanding.
    ///
    /// Only confirmations actually observed count as successes: messages
    /// submitted to the channel but unconfirmed when a later `basic_publish`
    /// fails are reported as **unattempted**, since we don't know if the
    /// broker ever received them. Re-publishing them risks a duplicate;
    /// not re-publishing them risks a loss, and this crate always picks the
    /// duplicate.
    async fn do_publish_batch(
        channel: &Channel,
        exchange: &str,
        items: &[(&str, bytes::Bytes)],
    ) -> BatchReport {
        let total = items.len();
        let mut confirms = Vec::with_capacity(total);
        let props = base_properties();
        for (i, (routing_key, payload)) in items.iter().enumerate() {
            match channel
                .basic_publish(
                    exchange.into(),
                    (*routing_key).into(),
                    BasicPublishOptions::default(),
                    payload,
                    props.clone(),
                )
                .await
            {
                Ok(confirm) => confirms.push(confirm),
                Err(e) => {
                    // Index `i` was attempted and rejected. Indices before it
                    // were submitted but will never be confirmed now, and
                    // indices after it were never submitted — both are
                    // outstanding.
                    let unattempted = (0..i).chain(i.saturating_add(1)..total).collect();
                    return BatchReport::sparse(
                        vec![i],
                        unattempted,
                        Some(map_lapin_error("batch publish failed", e)),
                    );
                }
            }
        }

        for (i, confirm) in confirms.into_iter().enumerate() {
            match confirm.await {
                Ok(result) => {
                    if result.is_nack() {
                        metrics::record_backend_error(
                            metrics::BackendLabel::RabbitMq,
                            metrics::BackendErrorKind::Publish,
                        );
                        return BatchReport::prefix(
                            i,
                            total,
                            ShoveError::Connection("broker NACKed a batch message".to_string()),
                        );
                    }
                }
                Err(e) => {
                    return BatchReport::prefix(
                        i,
                        total,
                        map_lapin_error("batch confirm failed", e),
                    );
                }
            }
        }

        BatchReport::all_succeeded()
    }
}

impl RabbitMqPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();

        // RabbitMQ is the one backend where broadcast changes the *publish*
        // side too. There is no shared queue to address, so the message goes to
        // the fanout exchange with an empty routing key and the broker copies
        // it into every instance's own ephemeral queue — or discards it, if no
        // instance is subscribed.
        //
        // This is what makes the deployment caveat in
        // `docs/pages/concepts/broadcast.mdx` real: a publisher on an older
        // build still takes the `("", queue)` route below, and nothing is bound
        // there. Publisher and subscribers deploy together.
        //
        // `build()` rejects `.broadcast()` combined with `.sequenced()`, so
        // this branch never competes with the sequencing match below.
        if topology.broadcast() {
            let exchange = broadcast_exchange(topology.queue());
            return self.publish_raw(&exchange, "", &payload, None).await;
        }

        match (topology.sequencing(), T::SEQUENCE_KEY_FN) {
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

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&headers)?;
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let field_table = hashmap_to_field_table(headers);
        let topology = T::topology();

        // Fanout exchange, empty routing key — see the note in `publish`.
        if topology.broadcast() {
            let exchange = broadcast_exchange(topology.queue());
            return self
                .publish_raw(&exchange, "", &payload, Some(field_table))
                .await;
        }

        match (topology.sequencing(), T::SEQUENCE_KEY_FN) {
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

    /// Prefix semantics: confirms are awaited in order and the call stops at
    /// the first nack, so that index is the failure and the tail behind it is
    /// outstanding. In the submit-failure case, already-submitted-but-
    /// unconfirmed records are outstanding too — their fate is unknown, and
    /// this crate resolves that toward re-publishing.
    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        self.publish_batch_report::<T>(messages)
            .await
            .resolve(messages.len())
            .result
    }

    pub(crate) async fn publish_batch_report<T: Topic>(
        &self,
        messages: &[T::Message],
    ) -> BatchReport {
        let topology = T::topology();
        let queue = topology.queue();
        let sequencing = topology.sequencing();
        let key_fn = T::SEQUENCE_KEY_FN;

        let payloads: Result<Vec<bytes::Bytes>> = messages
            .iter()
            .map(<T::Codec as crate::Codec<T::Message>>::encode_bytes)
            .collect();
        let payloads = match payloads {
            Ok(v) => v,
            Err(e) => return BatchReport::wholly_unattempted(messages.len(), e),
        };

        // Fanout exchange, empty routing key per record — see the note in
        // `publish`. The per-index reporting is unchanged: a fanout publish is
        // confirmed the same way any other publish on a confirm channel is.
        if topology.broadcast() {
            let exchange = broadcast_exchange(queue);
            let items: Vec<(&str, bytes::Bytes)> = payloads.into_iter().map(|p| ("", p)).collect();
            return self.publish_batch_raw(&exchange, &items).await;
        }

        let routing_keys: Option<Vec<String>> = key_fn.map(|kf| messages.iter().map(kf).collect());

        match (sequencing, routing_keys) {
            (Some(seq), Some(keys)) => {
                let items: Vec<(&str, bytes::Bytes)> = keys
                    .iter()
                    .zip(payloads)
                    .map(|(k, p)| (k.as_str(), p))
                    .collect();
                self.publish_batch_raw(seq.exchange(), &items).await
            }
            (Some(_), None) => BatchReport::wholly_unattempted(
                messages.len(),
                ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                ),
            ),
            (None, _) => {
                let items: Vec<(&str, bytes::Bytes)> =
                    payloads.into_iter().map(|p| (queue, p)).collect();
                self.publish_batch_raw("", &items).await
            }
        }
    }
}

impl PublisherImpl for RabbitMqPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        RabbitMqPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        RabbitMqPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = BatchReport> + Send {
        RabbitMqPublisher::publish_batch_report::<T>(self, msgs)
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
                .map_err(|e| map_lapin_error("tx_commit failed", e))?;
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
            .map_err(|e| map_lapin_error("publish to queue failed", e))?
            .await
            .map_err(|e| map_lapin_error("publish to queue confirm failed", e))?;

        if !self.tx_mode && confirm.is_nack() {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Publish,
            );
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
