use std::collections::HashMap;
use std::time::Duration;

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::HeaderMap;
use bytes::Bytes;
use uuid::Uuid;

use crate::error::Result;
use crate::publisher::Publisher;
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::ShoveError;

use super::client::NatsClient;

const MAX_PUBLISH_ATTEMPTS: u32 = 3;
const RETRY_COUNT_HEADER: &str = "Shove-Retry-Count";

#[derive(Clone)]
pub struct NatsPublisher {
    client: NatsClient,
}

impl NatsPublisher {
    pub async fn new(client: NatsClient) -> Result<Self> {
        Ok(Self { client })
    }

    fn resolve_subject<T: Topic>(topology: &'static crate::topology::QueueTopology, message: &T::Message) -> String {
        if let Some(seq) = topology.sequencing() {
            if let Some(key_fn) = T::SEQUENCE_KEY_FN {
                let key = key_fn(message);
                let shard = fnv1a_hash(&key) % seq.routing_shards() as u64;
                return format!("{}.shard.{shard}", topology.queue());
            }
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
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));

        for attempt in 1..=MAX_PUBLISH_ATTEMPTS {
            match self
                .client
                .jetstream()
                .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
                .await
            {
                Ok(ack_future) => match ack_future.await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if attempt == MAX_PUBLISH_ATTEMPTS {
                            return Err(ShoveError::Connection(format!(
                                "publish ack failed after {MAX_PUBLISH_ATTEMPTS} attempts: {e}"
                            )));
                        }
                        let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                        tracing::warn!(
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "NATS publish ack failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                },
                Err(e) => {
                    if attempt == MAX_PUBLISH_ATTEMPTS {
                        return Err(ShoveError::Connection(format!(
                            "publish failed after {MAX_PUBLISH_ATTEMPTS} attempts: {e}"
                        )));
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                    tracing::warn!(
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "NATS publish failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        unreachable!()
    }
}

impl Publisher for NatsPublisher {
    async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = serde_json::to_vec(message)?;
        let topology = T::topology();
        let subject = Self::resolve_subject::<T>(topology, message);
        let headers = Self::build_headers(None);
        self.publish_raw(subject, headers, Bytes::from(payload)).await
    }

    async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        extra_headers: HashMap<String, String>,
    ) -> Result<()> {
        let payload = serde_json::to_vec(message)?;
        let topology = T::topology();
        let subject = Self::resolve_subject::<T>(topology, message);
        let headers = Self::build_headers(Some(&extra_headers));
        self.publish_raw(subject, headers, Bytes::from(payload)).await
    }

    async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        // Serialize all upfront for fail-fast
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

        for (subject, headers, payload) in prepared {
            self.publish_raw(subject, headers, payload).await?;
        }

        Ok(())
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