use std::collections::HashMap;
#[cfg(feature = "kafka-schema-registry")]
use std::sync::Arc;
use std::time::Duration;

use rdkafka::client::ClientContext;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use uuid::Uuid;

use crate::backend::PublisherImpl;
use crate::error::Result;
use crate::metrics;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::SchemaRegistry;

use super::client::KafkaClient;
use super::constants::{
    MAX_PUBLISH_ATTEMPTS, MESSAGE_ID_HEADER, PRODUCE_TIMEOUT, RETRY_COUNT_HEADER,
};

/// Publish a message to Kafka with retry on transient failures.
/// Shared by both the publisher and consumer (for DLQ / retry publishes).
pub(super) async fn publish_with_retry<C>(
    producer: &FutureProducer<C>,
    topic: &str,
    key: Option<&[u8]>,
    headers: OwnedHeaders,
    payload: &[u8],
    max_attempts: u32,
    label: &str,
) -> Result<()>
where
    C: ClientContext + 'static,
{
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
    // perf-K-1: avoid cloning headers on the last (or only) attempt.
    // Take from the Option on the final attempt to move instead of clone;
    // earlier attempts still clone because we may need them for retries.
    let mut headers_opt = Some(headers);

    for attempt in 1..=max_attempts {
        let h = if attempt < max_attempts {
            // Safety: headers_opt is Some until the final attempt.
            headers_opt.as_ref().unwrap().clone()
        } else {
            headers_opt.take().unwrap()
        };
        let mut record = FutureRecord::to(topic).payload(payload).headers(h);
        if let Some(k) = key {
            record = record.key(k);
        }

        match producer.send(record, PRODUCE_TIMEOUT).await {
            Ok(_) => return Ok(()),
            Err((e, _)) => {
                if attempt == max_attempts {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Kafka,
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

/// Publisher-side configuration for the Kafka backend.
///
/// The symmetric counterpart to [`KafkaConsumerGroupConfig`] on the consume
/// side: it carries optional Schema Registry settings used to Confluent-frame
/// outgoing payloads. The default (no schema registry) leaves payloads as the
/// topic codec encodes them — identical to [`Broker::publisher`].
///
/// [`KafkaConsumerGroupConfig`]: super::consumer_group::KafkaConsumerGroupConfig
/// [`Broker::publisher`]: crate::broker::Broker::publisher
#[derive(Clone, Default)]
pub struct KafkaPublisherConfig {
    /// Schema Registry client used to resolve subject → latest id and frame
    /// outgoing payloads. `None` disables producer-side framing.
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<Arc<SchemaRegistry>>,
    /// Override the Confluent subject. Defaults to TopicNameStrategy
    /// (`"{topic}-value"`) when unset.
    #[cfg(feature = "kafka-schema-registry")]
    subject_override: Option<Arc<str>>,
}

impl KafkaPublisherConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Schema Registry client for producer-side Confluent framing.
    ///
    /// When set, payloads for protobuf/JSON codecs are wrapped in the Confluent
    /// wire frame (magic byte + big-endian schema id [+ protobuf message-index])
    /// using the latest registered id for the resolved subject. Symmetric with
    /// [`KafkaConsumerGroupConfig::with_schema_registry`] on the consume side.
    ///
    /// [`KafkaConsumerGroupConfig::with_schema_registry`]: super::consumer_group::KafkaConsumerGroupConfig::with_schema_registry
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Override the Confluent subject used for the schema-id lookup. Defaults to
    /// `"{topic}-value"` (TopicNameStrategy).
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_subject(mut self, subject: impl Into<Arc<str>>) -> Self {
        self.subject_override = Some(subject.into());
        self
    }
}

/// Resolved producer-side Schema Registry encoder held by the publisher.
#[cfg(feature = "kafka-schema-registry")]
#[derive(Clone)]
struct PublisherSrEncoder {
    registry: Arc<SchemaRegistry>,
    subject_override: Option<Arc<str>>,
}

#[derive(Clone)]
pub struct KafkaPublisher {
    client: KafkaClient,
    #[cfg(feature = "kafka-schema-registry")]
    sr: Option<PublisherSrEncoder>,
}

impl KafkaPublisher {
    pub async fn new(client: KafkaClient) -> Result<Self> {
        Ok(Self {
            client,
            #[cfg(feature = "kafka-schema-registry")]
            sr: None,
        })
    }

    /// Build a publisher from a [`KafkaPublisherConfig`]. The config's Schema
    /// Registry settings (if any) enable producer-side Confluent framing.
    #[cfg_attr(
        not(feature = "kafka-schema-registry"),
        allow(unused_variables, clippy::unused_async)
    )]
    pub async fn with_config(client: KafkaClient, config: KafkaPublisherConfig) -> Result<Self> {
        Ok(Self {
            client,
            #[cfg(feature = "kafka-schema-registry")]
            sr: config.schema_registry.map(|registry| PublisherSrEncoder {
                registry,
                subject_override: config.subject_override,
            }),
        })
    }

    /// Frame `payload` in the Confluent wire format when producer-side SR is
    /// configured and the codec maps to a registry wire format; otherwise return
    /// it unchanged.
    #[cfg(feature = "kafka-schema-registry")]
    async fn frame_payload(
        enc: Option<&PublisherSrEncoder>,
        codec_name: &str,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>> {
        use crate::schema_registry::{WireFormat, build_frame, default_subject};

        let Some(enc) = enc else {
            return Ok(payload);
        };
        let Some(fmt) = WireFormat::from_codec_name(codec_name) else {
            return Ok(payload);
        };
        let subject = enc
            .subject_override
            .clone()
            .unwrap_or_else(|| default_subject(topic));
        let id = enc.registry.latest_id(&subject).await.map_err(|e| {
            let msg = format!("schema registry latest_id({subject}) failed: {e}");
            if e.is_retriable() {
                ShoveError::Connection(msg)
            } else {
                ShoveError::Unknown(msg)
            }
        })?;
        // The publisher always frames with message index [0] (the first message
        // type) — the only index byte-identical across Confluent's zig-zag and
        // shove's plain varint encoding, and all the bridge needs.
        Ok(build_frame(fmt, id, &[0], &payload))
    }

    fn resolve_topic_and_key<T: Topic>(
        topology: &'static QueueTopology,
        message: &T::Message,
    ) -> (&'static str, Option<Vec<u8>>) {
        // perf-K-3: topology is &'static so queue() is &'static str —
        // no allocation needed; the topic string lives forever.
        let topic = topology.queue();
        let key = T::SEQUENCE_KEY_FN.map(|key_fn| key_fn(message).into_bytes());
        (topic, key)
    }

    fn build_headers(extra: Option<&HashMap<String, String>>) -> OwnedHeaders {
        // perf-K-2: encode UUID into a stack buffer — no heap alloc
        let mut uuid_buf = Uuid::encode_buffer();
        let uuid_str = Uuid::new_v4().hyphenated().encode_lower(&mut uuid_buf);
        let mut headers = OwnedHeaders::new()
            .insert(Header {
                key: MESSAGE_ID_HEADER,
                value: Some(uuid_str.as_bytes()),
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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        #[cfg(feature = "kafka-schema-registry")]
        let payload = Self::frame_payload(
            self.sr.as_ref(),
            <T::Codec as crate::Codec<T::Message>>::NAME,
            topic,
            payload,
        )
        .await?;
        let headers = Self::build_headers(None);
        self.client
            .publish_with_retry(
                topic,
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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        #[cfg(feature = "kafka-schema-registry")]
        let payload = Self::frame_payload(
            self.sr.as_ref(),
            <T::Codec as crate::Codec<T::Message>>::NAME,
            topic,
            payload,
        )
        .await?;
        let headers = Self::build_headers(Some(&extra_headers));
        self.client
            .publish_with_retry(
                topic,
                key.as_deref(),
                headers,
                &payload,
                MAX_PUBLISH_ATTEMPTS,
                "publish_with_headers",
            )
            .await
    }

    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> (u64, Result<()>) {
        use futures_util::future::join_all;

        let topology = T::topology();
        #[allow(clippy::type_complexity)]
        let prepared: Result<Vec<(&'static str, Option<Vec<u8>>, OwnedHeaders, Vec<u8>)>> =
            messages
                .iter()
                .map(|msg| {
                    let payload = <T::Codec as crate::Codec<T::Message>>::encode(msg)?;
                    let (topic, key) = Self::resolve_topic_and_key::<T>(topology, msg);
                    let headers = Self::build_headers(None);
                    Ok((topic, key, headers, payload))
                })
                .collect();
        let prepared = match prepared {
            Ok(v) => v,
            Err(e) => return (0, Err(e)),
        };

        // Submit every record concurrently so librdkafka's internal batching
        // (`batch.size` / `linger.ms`) can coalesce them into broker-side
        // batches. Awaiting each `send` serially forced one round-trip per
        // message and pinned publish throughput at ~200 msg/s on localhost.
        // We use `join_all` (not `try_join_all`) so an error on one message
        // doesn't cancel the others — that lets the wrapper attribute
        // accurate per-message success/failure counters even on partial
        // failure, and we still surface the first error to the caller.
        #[cfg(feature = "kafka-schema-registry")]
        let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
        #[cfg(feature = "kafka-schema-registry")]
        let sr = self.sr.as_ref();
        let results: Vec<Result<()>> =
            join_all(prepared.into_iter().map(|(topic, key, headers, payload)| {
                let client = &self.client;
                async move {
                    #[cfg(feature = "kafka-schema-registry")]
                    let payload = Self::frame_payload(sr, codec_name, topic, payload).await?;
                    client
                        .publish_with_retry(
                            topic,
                            key.as_deref(),
                            headers,
                            &payload,
                            1,
                            "batch publish",
                        )
                        .await
                }
            }))
            .await;

        let mut succeeded: u64 = 0;
        let mut first_err: Option<ShoveError> = None;
        for r in results {
            match r {
                Ok(()) => succeeded += 1,
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => (succeeded, Err(e)),
            None => (succeeded, Ok(())),
        }
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
    ) -> impl Future<Output = (u64, Result<()>)> + Send {
        KafkaPublisher::publish_batch::<T>(self, msgs)
    }
}
