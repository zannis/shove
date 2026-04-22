use std::collections::HashMap;
use std::time::Duration;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use uuid::Uuid;

use crate::backend::PublisherImpl;
use crate::error::Result;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

use super::client::KafkaClient;
use super::constants::{MESSAGE_ID_HEADER, RETRY_COUNT_HEADER};

const MAX_PUBLISH_ATTEMPTS: u32 = 3;
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(5);

/// Publish a message to Kafka with retry on transient failures.
/// Shared by both the publisher and consumer (for DLQ / retry publishes).
pub(super) async fn publish_with_retry(
    producer: &FutureProducer,
    topic: &str,
    key: Option<&[u8]>,
    headers: OwnedHeaders,
    payload: &[u8],
    max_attempts: u32,
    label: &str,
) -> Result<()> {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));

    for attempt in 1..=max_attempts {
        let mut record = FutureRecord::to(topic)
            .payload(payload)
            .headers(headers.clone());
        if let Some(k) = key {
            record = record.key(k);
        }

        match producer.send(record, PRODUCE_TIMEOUT).await {
            Ok(_) => return Ok(()),
            Err((e, _)) => {
                if attempt == max_attempts {
                    return Err(ShoveError::Connection(format!(
                        "{label} failed after {max_attempts} attempts: {e}"
                    )));
                }
                let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                tracing::warn!(attempt, error = %e, "{label} failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }

    unreachable!()
}

#[derive(Clone)]
pub struct KafkaPublisher {
    client: KafkaClient,
}

impl KafkaPublisher {
    pub async fn new(client: KafkaClient) -> Result<Self> {
        Ok(Self { client })
    }

    fn resolve_topic_and_key<T: Topic>(
        topology: &'static QueueTopology,
        message: &T::Message,
    ) -> (String, Option<Vec<u8>>) {
        let topic = topology.queue().to_string();
        let key = T::SEQUENCE_KEY_FN.map(|key_fn| key_fn(message).into_bytes());
        (topic, key)
    }

    fn build_headers(extra: Option<&HashMap<String, String>>) -> OwnedHeaders {
        let mut headers = OwnedHeaders::new()
            .insert(Header {
                key: MESSAGE_ID_HEADER,
                value: Some(Uuid::new_v4().to_string().as_bytes()),
            })
            .insert(Header {
                key: RETRY_COUNT_HEADER,
                value: Some(b"0"),
            });

        if let Some(extra) = extra {
            for (k, v) in extra {
                headers = headers.insert(Header {
                    key: k.as_str(),
                    value: Some(v.as_bytes()),
                });
            }
        }
        headers
    }
}

impl KafkaPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = serde_json::to_vec(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        let headers = Self::build_headers(None);
        publish_with_retry(
            self.client.producer(),
            &topic,
            key.as_deref(),
            headers,
            &payload,
            MAX_PUBLISH_ATTEMPTS,
            "publish",
        )
        .await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        extra_headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&extra_headers)?;
        let payload = serde_json::to_vec(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        let headers = Self::build_headers(Some(&extra_headers));
        publish_with_retry(
            self.client.producer(),
            &topic,
            key.as_deref(),
            headers,
            &payload,
            MAX_PUBLISH_ATTEMPTS,
            "publish_with_headers",
        )
        .await
    }

    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        use futures_util::future::try_join_all;

        let topology = T::topology();
        #[allow(clippy::type_complexity)]
        let prepared: Vec<(String, Option<Vec<u8>>, OwnedHeaders, Vec<u8>)> = messages
            .iter()
            .map(|msg| {
                let payload = serde_json::to_vec(msg)?;
                let (topic, key) = Self::resolve_topic_and_key::<T>(topology, msg);
                let headers = Self::build_headers(None);
                Ok((topic, key, headers, payload))
            })
            .collect::<Result<Vec<_>>>()?;

        // Submit every record concurrently so librdkafka's internal batching
        // (`batch.size` / `linger.ms`) can coalesce them into broker-side
        // batches. Awaiting each `send` serially forced one round-trip per
        // message and pinned publish throughput at ~200 msg/s on localhost.
        let producer = self.client.producer();
        try_join_all(
            prepared
                .into_iter()
                .map(|(topic, key, headers, payload)| async move {
                    let mut record = FutureRecord::to(&topic)
                        .payload(payload.as_slice())
                        .headers(headers);
                    if let Some(k) = key.as_deref() {
                        record = record.key(k);
                    }
                    producer
                        .send(record, PRODUCE_TIMEOUT)
                        .await
                        .map_err(|(e, _)| {
                            ShoveError::Connection(format!("batch publish failed: {e}"))
                        })?;
                    Ok::<(), ShoveError>(())
                }),
        )
        .await?;

        Ok(())
    }
}

impl PublisherImpl for KafkaPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        KafkaPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        KafkaPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send {
        KafkaPublisher::publish_batch::<T>(self, msgs)
    }
}
