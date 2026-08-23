use std::collections::HashMap;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;
use bytes::Bytes;
use uuid::Uuid;

use crate::backend::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::Result;
use crate::metrics;
use crate::publisher_internal::{shard_for_key, validate_headers};
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

use super::client::NatsClient;
use super::constants::{RETRY_COUNT_HEADER, SEQUENCE_KEY_HEADER};

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
                        metrics::record_backend_error(
                            metrics::BackendLabel::Nats,
                            metrics::BackendErrorKind::Publish,
                        );
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
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
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

    /// Resolve the publish subject and, on a sequenced topic, the sequence key.
    ///
    /// The key is returned (not just hashed into the subject) because the
    /// consumer needs it to implement `SequenceFailure::FailAll` — the subject
    /// only identifies the shard, and a shard holds many keys.
    fn resolve_subject_and_key<T: Topic>(
        topology: &'static QueueTopology,
        message: &T::Message,
    ) -> (String, Option<String>) {
        if let Some(seq) = topology.sequencing()
            && let Some(key_fn) = T::SEQUENCE_KEY_FN
        {
            let key = key_fn(message);
            let shard = shard_for_key(&key, seq.routing_shards());
            return (format!("{}.shard.{shard}", topology.queue()), Some(key));
        }
        (topology.queue().to_string(), None)
    }

    fn build_headers(extra: Option<&HashMap<String, String>>, seq_key: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, Uuid::new_v4().to_string().as_str());
        headers.insert(RETRY_COUNT_HEADER, "0");

        if let Some(extra) = extra {
            for (k, v) in extra {
                headers.insert(k.as_str(), v.as_str());
            }
        }

        // After the user headers: the sequence key drives FailAll poisoning, so
        // a caller must not be able to overwrite it. (`validate_headers` guards
        // the `x-` internal names shared with the other backends, but not the
        // `Shove-` names this backend uses.)
        if let Some(key) = seq_key {
            headers.insert(SEQUENCE_KEY_HEADER, key);
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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, message);
        let headers = Self::build_headers(None, seq_key.as_deref());
        self.publish_raw(subject, headers, payload).await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        extra_headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&extra_headers)?;
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, message);
        let headers = Self::build_headers(Some(&extra_headers), seq_key.as_deref());
        self.publish_raw(subject, headers, payload).await
    }

    /// Mixed semantics: ack failures are exact over the submitted prefix, and
    /// a submission break leaves an unattempted tail behind it.
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
        let prepared: Result<Vec<(String, HeaderMap, Bytes)>> = messages
            .iter()
            .map(|msg| {
                let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(msg)?;
                let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, msg);
                let headers = Self::build_headers(None, seq_key.as_deref());
                Ok((subject, headers, payload))
            })
            .collect();
        let prepared = match prepared {
            Ok(v) => v,
            Err(e) => return BatchReport::wholly_unattempted(messages.len(), e),
        };
        let total = prepared.len();

        // Fire all publishes, then await all acks — O(1 RTT) instead of O(N RTT).
        // Submission and ack are tracked separately so the wrapper can
        // attribute partial-failure counters to what NATS actually accepted
        // before we surface the first error. Each ack carries its own index so
        // a sparse ack failure is reported at the record it belongs to rather
        // than collapsed into a count.
        let mut ack_futures = Vec::with_capacity(total);
        let mut failed: Vec<usize> = Vec::new();
        let mut unattempted: Vec<usize> = Vec::new();
        let mut first_err: Option<ShoveError> = None;
        for (i, (subject, headers, payload)) in prepared.into_iter().enumerate() {
            match self
                .client
                .jetstream()
                .publish_with_headers(subject, headers, payload)
                .await
            {
                Ok(ack) => ack_futures.push((i, ack)),
                Err(e) => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
                    first_err = Some(ShoveError::Connection(format!("batch publish failed: {e}")));
                    // This record was attempted and rejected; everything after
                    // it never left the process.
                    failed.push(i);
                    unattempted.extend(i.saturating_add(1)..total);
                    break;
                }
            }
        }

        // Drain every already-submitted ack even if submission broke early:
        // those messages were accepted by NATS and must be counted, not
        // abandoned. A submission error takes precedence in `first_err`; an
        // ack error only replaces it when nothing has failed yet.
        let mut ack_failed: Vec<usize> = Vec::new();
        for (i, ack) in ack_futures {
            if let Err(e) = ack.await {
                metrics::record_backend_error(
                    metrics::BackendLabel::Nats,
                    metrics::BackendErrorKind::Publish,
                );
                ack_failed.push(i);
                if first_err.is_none() {
                    first_err = Some(ShoveError::Connection(format!(
                        "batch publish ack failed: {e}"
                    )));
                }
            }
        }
        if first_err.is_none() {
            return BatchReport::all_succeeded();
        }
        // Ack failures all sit below the submission break, so prepending them
        // keeps `failed` ascending.
        ack_failed.extend(failed);
        BatchReport::sparse(ack_failed, unattempted, first_err)
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
    ) -> impl Future<Output = BatchReport> + Send {
        NatsPublisher::publish_batch_report::<T>(self, msgs)
    }
}
