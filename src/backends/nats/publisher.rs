use std::collections::HashMap;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;
use bytes::Bytes;
use uuid::Uuid;

use crate::ShoveError;
use crate::backend::PublisherImpl;
use crate::error::Result;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
use crate::topic::Topic;

use super::client::NatsClient;
use super::constants::RETRY_COUNT_HEADER;

const MAX_PUBLISH_ATTEMPTS: u32 = 3;

/// Publish a message to JetStream with retry on transient failures.
/// Shared by both the publisher and consumer (for DLQ publishes).
pub(super) async fn publish_with_retry(
    js: &jetstream::Context,
    subject: String,
    headers: HeaderMap,
    payload: Bytes,
    max_attempts: u32,
    label: &str,
) -> Result<()> {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));

    for attempt in 1..=max_attempts {
        match js
            .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
            .await
        {
            Ok(ack_future) => match ack_future.await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == max_attempts {
                        return Err(ShoveError::Connection(format!(
                            "{label} ack failed after {max_attempts} attempts: {e}"
                        )));
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                    tracing::warn!(attempt, error = %e, "{label} ack failed, retrying");
                    tokio::time::sleep(delay).await;
                }
            },
            Err(e) => {
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
pub struct NatsPublisher {
    client: NatsClient,
}

impl NatsPublisher {
    pub async fn new(client: NatsClient) -> Result<Self> {
        Ok(Self { client })
    }

    fn resolve_subject<T: Topic>(
        topology: &'static crate::topology::QueueTopology,
        message: &T::Message,
    ) -> String {
        if let Some(seq) = topology.sequencing()
            && let Some(key_fn) = T::SEQUENCE_KEY_FN
        {
            let key = key_fn(message);
            let shard = fnv1a_hash(&key) % seq.routing_shards() as u64;
            return format!("{}.shard.{shard}", topology.queue());
        }
        topology.queue().to_string()
    }

    fn build_headers(extra: Option<&HashMap<String, String>>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, Uuid::new_v4().to_string().as_str());
        headers.insert(RETRY_COUNT_HEADER, "0");

        if let Some(extra) = extra {
            for (k, v) in extra {
                headers.insert(k.as_str(), v.as_str());
            }
        }
        headers
    }

    async fn publish_raw(&self, subject: String, headers: HeaderMap, payload: Bytes) -> Result<()> {
        publish_with_retry(
            self.client.jetstream(),
            subject,
            headers,
            payload,
            MAX_PUBLISH_ATTEMPTS,
            "publish",
        )
        .await
    }
}

impl NatsPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = serde_json::to_vec(message)?;
        let topology = T::topology();
        let subject = Self::resolve_subject::<T>(topology, message);
        let headers = Self::build_headers(None);
        self.publish_raw(subject, headers, Bytes::from(payload))
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
        let subject = Self::resolve_subject::<T>(topology, message);
        let headers = Self::build_headers(Some(&extra_headers));
        self.publish_raw(subject, headers, Bytes::from(payload))
            .await
    }

    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        let topology = T::topology();
        let prepared: Vec<(String, HeaderMap, Bytes)> = messages
            .iter()
            .map(|msg| {
                let payload = serde_json::to_vec(msg)?;
                let subject = Self::resolve_subject::<T>(topology, msg);
                let headers = Self::build_headers(None);
                Ok((subject, headers, Bytes::from(payload)))
            })
            .collect::<Result<Vec<_>>>()?;

        // Fire all publishes, then await all acks — O(1 RTT) instead of O(N RTT)
        let mut ack_futures = Vec::with_capacity(prepared.len());
        for (subject, headers, payload) in prepared {
            let ack = self
                .client
                .jetstream()
                .publish_with_headers(subject, headers, payload)
                .await
                .map_err(|e| ShoveError::Connection(format!("batch publish failed: {e}")))?;
            ack_futures.push(ack);
        }

        for ack in ack_futures {
            ack.await
                .map_err(|e| ShoveError::Connection(format!("batch publish ack failed: {e}")))?;
        }

        Ok(())
    }
}

impl PublisherImpl for NatsPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        NatsPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        NatsPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send {
        NatsPublisher::publish_batch::<T>(self, msgs)
    }
}

fn fnv1a_hash(key: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
