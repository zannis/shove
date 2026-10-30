use std::collections::HashMap;
use std::num::NonZeroU32;
#[cfg(feature = "kafka-schema-registry")]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer as _};
use uuid::{Builder, Uuid};

use crate::backend::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::Result;
use crate::metrics;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaId, SchemaRegistry, WireFormat, build_frame, default_subject};

use super::client::KafkaClient;
use super::constants::{
    MAX_PUBLISH_ATTEMPTS, MESSAGE_ID_HEADER, PRODUCE_TIMEOUT, RETRY_COUNT_HEADER,
};

/// How long an in-order batch waits before it enqueues a record again that
/// librdkafka's full queue refused, as `FutureProducer::send` waits.
const QUEUE_FULL_WAIT: Duration = Duration::from_millis(100);

/// What a failed send says about sending the record again.
enum SendFailure {
    /// librdkafka marked the producer fatal, so it refuses every record from
    /// then on: the code and reason of that error.
    Fatal(RDKafkaErrorCode, String),
    /// The broker refused the principal, and refuses it again.
    Unauthorized,
    /// Another send may get past it.
    Transient,
}

impl SendFailure {
    fn of<C>(producer: &FutureProducer<C>, error: &KafkaError) -> Self
    where
        C: ClientContext + 'static,
    {
        if let Some((code, reason)) = producer.client().fatal_error() {
            return Self::Fatal(code, reason);
        }
        match error.rdkafka_error_code() {
            Some(
                RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed,
            ) => Self::Unauthorized,
            _ => Self::Transient,
        }
    }

    /// The error for a record that is not sent again, after `attempts`
    /// sends. Only a transient failure is retryable.
    fn into_error(self, label: &str, attempts: u32, error: &KafkaError) -> ShoveError {
        match self {
            Self::Fatal(code, reason) => ShoveError::Unknown(format!(
                "{label} failed: {error}; the producer hit a fatal error, {code}: {reason}, \
                 and the next publish builds a new one"
            )),
            Self::Unauthorized => ShoveError::Topology(format!("{label} was refused: {error}")),
            Self::Transient => {
                ShoveError::Connection(format!("{label} failed after {attempts} attempts: {error}"))
            }
        }
    }
}

/// Publish a message to Kafka, sending it again after a transient failure,
/// at most `max_attempts` sends in all. Shared by the publisher and the
/// consumer (for DLQ and retry publishes).
///
/// `headers` move into the first send and come back only with a failed
/// one. An authorization error or a fatal producer error is not sent
/// again; see [`KafkaClient::publish_with_retry`] for the errors returned.
pub(super) async fn publish_with_retry<C>(
    producer: &FutureProducer<C>,
    topic: &str,
    key: Option<&[u8]>,
    headers: OwnedHeaders,
    payload: &[u8],
    max_attempts: NonZeroU32,
    label: &str,
) -> Result<()>
where
    C: ClientContext + 'static,
{
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
    let mut headers = headers;
    let mut attempt: u32 = 1;
    loop {
        let mut record = FutureRecord::to(topic).payload(payload).headers(headers);
        if let Some(k) = key {
            record = record.key(k);
        }
        let (error, mut returned) = match producer.send(record, PRODUCE_TIMEOUT).await {
            Ok(_) => return Ok(()),
            Err(failed) => failed,
        };
        let failure = SendFailure::of(producer, &error);
        let again = matches!(failure, SendFailure::Transient) && attempt < max_attempts.get();
        let recovered = if again {
            returned.detach_headers()
        } else {
            None
        };
        let Some(recovered) = recovered else {
            record_publish_error();
            return Err(failure.into_error(label, attempt, &error));
        };
        headers = recovered;
        let delay = backoff.next_delay();
        tracing::warn!(attempt, error = %error, "{label} failed, retrying");
        tokio::time::sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

/// One record of a batch, encoded, framed and ready to send.
pub(super) struct BatchRecord {
    key: Option<Vec<u8>>,
    headers: OwnedHeaders,
    payload: Bytes,
}

/// Enqueues `record` without waiting for its delivery. While librdkafka's
/// queue is full the same record is offered again, for up to
/// [`PRODUCE_TIMEOUT`], so no later record can be enqueued ahead of it.
async fn enqueue<C>(
    producer: &FutureProducer<C>,
    topic: &str,
    record: BatchRecord,
) -> KafkaResult<DeliveryFuture>
where
    C: ClientContext + 'static,
{
    let BatchRecord {
        key,
        headers,
        payload,
    } = record;
    let mut pending = FutureRecord::to(topic)
        .payload(payload.as_ref())
        .headers(headers);
    if let Some(k) = key.as_deref() {
        pending = pending.key(k);
    }
    let started = tokio::time::Instant::now();
    loop {
        match producer.send_result(pending) {
            Ok(delivery) => return Ok(delivery),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), refused))
                if started.elapsed() < PRODUCE_TIMEOUT =>
            {
                pending = refused;
                tokio::time::sleep(QUEUE_FULL_WAIT).await;
            }
            Err((error, _)) => return Err(error),
        }
    }
}

/// Sends `records` to `topic` in slice order and reports a prefix, and sets
/// `delivered` when the broker acknowledges any of them.
///
/// Each record is enqueued before the next, and the delivery reports are
/// awaited once the enqueueing is done, every one of them, so no record is
/// in flight when the report returns. The first record that fails, as it is
/// enqueued or on delivery, is the report's failure, and every record after
/// it is unattempted whatever became of it: a republish from the failure
/// stores copies of those records in slice order, after the stored records
/// that came before the failure. A record that cannot be enqueued ends the
/// enqueueing, so nothing after it is sent.
pub(super) async fn publish_in_order<C>(
    producer: &FutureProducer<C>,
    topic: &str,
    records: Vec<BatchRecord>,
    label: &str,
    delivered: &AtomicBool,
) -> BatchReport
where
    C: ClientContext + 'static,
{
    let total = records.len();
    let mut deliveries = Vec::with_capacity(total);
    let mut refused = None;
    for record in records {
        match enqueue(producer, topic, record).await {
            Ok(delivery) => deliveries.push(delivery),
            Err(error) => {
                record_publish_error();
                refused = Some(SendFailure::of(producer, &error).into_error(label, 1, &error));
                break;
            }
        }
    }
    let enqueued = deliveries.len();
    let mut first = None;
    for (index, delivery) in deliveries.into_iter().enumerate() {
        let error = match delivery.await {
            Ok(Ok(_)) => {
                delivered.store(true, Ordering::Release);
                continue;
            }
            Ok(Err((error, _))) => SendFailure::of(producer, &error).into_error(label, 1, &error),
            Err(_canceled) => {
                ShoveError::Connection(format!("{label}: the producer dropped the delivery report"))
            }
        };
        record_publish_error();
        if first.is_none() {
            first = Some((index, error));
        }
    }
    match first.or_else(|| refused.map(|error| (enqueued, error))) {
        Some((index, error)) => BatchReport::prefix(index, total, error),
        None => BatchReport::all_succeeded(),
    }
}

fn record_publish_error() {
    metrics::record_backend_error(
        metrics::BackendLabel::Kafka,
        metrics::BackendErrorKind::Publish,
    );
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

/// The Confluent frame one publish wraps its payloads in: the wire format
/// and the schema id its subject resolved to.
#[cfg(feature = "kafka-schema-registry")]
struct Frame {
    format: WireFormat,
    id: SchemaId,
}

#[cfg(feature = "kafka-schema-registry")]
impl Frame {
    fn wrap(&self, payload: &[u8]) -> Bytes {
        // The publisher always frames with message index [0] (the first message
        // type), which is all the bridge needs.
        Bytes::from(build_frame(self.format, self.id, &[0], payload))
    }
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

    /// The frame for `topic`'s payloads, with the subject's latest schema id,
    /// when producer-side SR is configured and the codec maps to a registry
    /// wire format; `None` leaves payloads as the codec encoded them.
    #[cfg(feature = "kafka-schema-registry")]
    async fn resolve_frame(&self, codec_name: &str, topic: &str) -> Result<Option<Frame>> {
        let Some(enc) = self.sr.as_ref() else {
            return Ok(None);
        };
        let Some(format) = WireFormat::from_codec_name(codec_name) else {
            return Ok(None);
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
        Ok(Some(Frame { format, id }))
    }

    /// `payload` in the frame [`Self::resolve_frame`] returns, or unchanged
    /// when it returns none.
    #[cfg(feature = "kafka-schema-registry")]
    async fn frame_payload(&self, codec_name: &str, topic: &str, payload: Bytes) -> Result<Bytes> {
        Ok(match self.resolve_frame(codec_name, topic).await? {
            Some(frame) => frame.wrap(&payload),
            None => payload,
        })
    }

    /// Every payload of a batch in one frame, so the schema id is looked up
    /// once for the batch, before any record is sent.
    #[cfg(feature = "kafka-schema-registry")]
    async fn frame_batch(
        &self,
        codec_name: &str,
        topic: &str,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<Bytes>> {
        Ok(match self.resolve_frame(codec_name, topic).await? {
            Some(frame) => payloads.iter().map(|payload| frame.wrap(payload)).collect(),
            None => payloads,
        })
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
        // rand's thread RNG calls the OS only to reseed, once per 64 KiB of output.
        let id = Builder::from_random_bytes(rand::random()).into_uuid();
        // perf-K-2: encode UUID into a stack buffer — no heap alloc
        let mut uuid_buf = Uuid::encode_buffer();
        let uuid_str = id.hyphenated().encode_lower(&mut uuid_buf);
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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        #[cfg(feature = "kafka-schema-registry")]
        let payload = self
            .frame_payload(<T::Codec as crate::Codec<T::Message>>::NAME, topic, payload)
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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (topic, key) = Self::resolve_topic_and_key::<T>(topology, message);
        #[cfg(feature = "kafka-schema-registry")]
        let payload = self
            .frame_payload(<T::Codec as crate::Codec<T::Message>>::NAME, topic, payload)
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

    /// Every record is encoded before any is sent, and framed with one schema
    /// id looked up for the whole batch when the publisher frames for a
    /// Schema Registry; a failure there rejects the batch before anything is
    /// submitted, which is wholly unattempted rather than partial.
    ///
    /// On a topic without a sequence key every record is submitted
    /// independently, so the report names exactly the rejected indices and
    /// nothing is left unattempted. On a sequenced topic the records are
    /// sent in slice order and the report is a prefix: the first record that
    /// failed, with every record after it unattempted, so republishing the
    /// report's records stores copies of the failed record and every record
    /// after it in slice order.
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
        use futures_util::future::join_all;

        if messages.is_empty() {
            return BatchReport::all_succeeded();
        }
        let total = messages.len();
        let topology = T::topology();
        let topic = topology.queue();
        let payloads: Result<Vec<Bytes>> = messages
            .iter()
            .map(<T::Codec as crate::Codec<T::Message>>::encode_bytes)
            .collect();
        let payloads = match payloads {
            Ok(payloads) => payloads,
            Err(e) => return BatchReport::wholly_unattempted(total, e),
        };
        #[cfg(feature = "kafka-schema-registry")]
        let payloads = match self
            .frame_batch(
                <T::Codec as crate::Codec<T::Message>>::NAME,
                topic,
                payloads,
            )
            .await
        {
            Ok(payloads) => payloads,
            Err(e) => return BatchReport::wholly_unattempted(total, e),
        };
        let records: Vec<BatchRecord> = messages
            .iter()
            .zip(payloads)
            .map(|(message, payload)| BatchRecord {
                key: Self::resolve_topic_and_key::<T>(topology, message).1,
                headers: Self::build_headers(None),
                payload,
            })
            .collect();

        if T::SEQUENCE_KEY_FN.is_some() {
            return self
                .client
                .publish_in_order(topic, records, "batch publish")
                .await;
        }

        // Submit every record concurrently so librdkafka's internal batching
        // (`batch.size` / `linger.ms`) can coalesce them into broker-side
        // batches. Awaiting each `send` serially forced one round-trip per
        // message and pinned publish throughput at ~200 msg/s on localhost.
        // We use `join_all` (not `try_join_all`) so an error on one message
        // doesn't cancel the others — that lets the wrapper attribute
        // accurate per-message success/failure counters even on partial
        // failure, and we still surface the first error to the caller.
        let results: Vec<Result<()>> = join_all(records.into_iter().map(|record| {
            let client = &self.client;
            async move {
                client
                    .publish_with_retry(
                        topic,
                        record.key.as_deref(),
                        record.headers,
                        &record.payload,
                        1,
                        "batch publish",
                    )
                    .await
            }
        }))
        .await;

        // `results` is positional — `join_all` preserves input order — so the
        // index of an `Err` is the index of the record that was rejected.
        let mut failed: Vec<usize> = Vec::new();
        let mut first_err: Option<ShoveError> = None;
        for (i, r) in results.into_iter().enumerate() {
            if let Err(e) = r {
                failed.push(i);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if first_err.is_none() {
            return BatchReport::all_succeeded();
        }
        BatchReport::sparse(failed, Vec::new(), first_err)
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
    ) -> impl Future<Output = BatchReport> + Send {
        KafkaPublisher::publish_batch_report::<T>(self, msgs)
    }
}
