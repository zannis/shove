use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
#[cfg(feature = "kafka-ssl")]
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::{KafkaResult, RDKafkaErrorCode};
use rdkafka::message::OwnedHeaders;
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, Producer};

use super::constants::{
    MESSAGE_TIMEOUT_MS, PARTITION_METADATA_ATTEMPTS, PARTITION_METADATA_RETRY_INITIAL,
    PARTITION_METADATA_RETRY_MAX, PING_PROBE_TOPIC, PRODUCER_BUILD_SHUTDOWN_WAIT,
    PRODUCER_FLUSH_TIMEOUT, PRODUCER_REBUILD_WAIT, PRODUCER_REBUILD_WAIT_MAX, SHUTDOWN_GRACE,
};
use super::publisher::{
    BatchRecord, publish_in_order as publisher_publish_in_order,
    publish_with_retry as publisher_publish_with_retry,
};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::{MskIamContext, MskIamTokenProvider};
#[cfg(feature = "kafka-msk-iam")]
use rdkafka::bindings::{
    rd_kafka_oauthbearer_set_token, rd_kafka_oauthbearer_set_token_failure, rd_kafka_t,
};
#[cfg(feature = "kafka-msk-iam")]
use rdkafka::client::OAuthToken;
use rdkafka::types::RDKafkaRespErr;

use crate::ShoveError;
use crate::batch::BatchReport;
use crate::error::Result;
use crate::metrics;
use crate::retry::Backoff;

/// TLS material for Kafka connections. Client cert/key are only needed for mTLS.
///
/// For each pair (CA, cert, key), set **either** the `*_location` path **or**
/// the `*_pem` string — not both. If both are set, librdkafka prefers the PEM
/// value and silently ignores the path.
///
/// If no CA fields are set, librdkafka falls back to the OS trust store. This
/// is the right default for managed brokers with publicly-signed certs
/// (AWS MSK + ACM, Confluent Cloud); set `ca_location`/`ca_pem` for private CAs.
#[cfg(feature = "kafka-ssl")]
#[derive(Clone, Default)]
#[must_use]
pub struct KafkaTls {
    /// Path to CA certificate (PEM). Maps to `ssl.ca.location`.
    pub ca_location: Option<PathBuf>,
    /// CA certificate as an in-memory PEM string. Maps to `ssl.ca.pem`.
    pub ca_pem: Option<String>,
    /// Path to client certificate (PEM). Maps to `ssl.certificate.location`.
    pub certificate_location: Option<PathBuf>,
    /// Client certificate as an in-memory PEM string. Maps to `ssl.certificate.pem`.
    pub certificate_pem: Option<String>,
    /// Path to client private key (PEM). Maps to `ssl.key.location`.
    pub key_location: Option<PathBuf>,
    /// Client private key as an in-memory PEM string. Maps to `ssl.key.pem`.
    pub key_pem: Option<String>,
    /// Passphrase for the client private key. Maps to `ssl.key.password`.
    pub key_password: Option<String>,
    /// If true, set `ssl.endpoint.identification.algorithm=none`. Use only for
    /// test clusters — disables hostname verification.
    pub skip_hostname_verification: bool,
}

#[cfg(feature = "kafka-ssl")]
impl fmt::Debug for KafkaTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTls")
            .field("ca_location", &self.ca_location)
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<redacted>"))
            .field("certificate_location", &self.certificate_location)
            .field(
                "certificate_pem",
                &self.certificate_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("key_location", &self.key_location)
            .field("key_pem", &self.key_pem.as_ref().map(|_| "<redacted>"))
            .field(
                "key_password",
                &self.key_password.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "skip_hostname_verification",
                &self.skip_hostname_verification,
            )
            .finish()
    }
}

/// SASL credentials for Kafka. Combine with [`KafkaTls`] on the same
/// [`KafkaConfig`] to get `SASL_SSL`; without TLS this is `SASL_PLAINTEXT`.
#[cfg(feature = "kafka-ssl")]
#[derive(Clone)]
#[must_use]
pub enum KafkaSasl {
    Plain {
        username: String,
        password: String,
    },
    ScramSha256 {
        username: String,
        password: String,
    },
    ScramSha512 {
        username: String,
        password: String,
    },

    /// AWS MSK IAM authentication. Tokens expire after ~15 minutes;
    /// librdkafka invokes the refresh callback at ~80% of token lifetime
    /// (so roughly every ~12 minutes), signing a fresh presigned URL via
    /// `MskIamTokenProvider`.
    #[cfg(feature = "kafka-msk-iam")]
    MskIam {
        /// AWS region the MSK cluster lives in (e.g. `"eu-west-2"`).
        region: String,
        /// Optional named profile to load credentials from. If `None`,
        /// the default credential provider chain is used (env → profile
        /// → IMDS → IRSA → SSO).
        profile: Option<String>,
    },
}

#[cfg(feature = "kafka-ssl")]
impl KafkaSasl {
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Plain {
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn scram_sha_256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::ScramSha256 {
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn scram_sha_512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::ScramSha512 {
            username: username.into(),
            password: password.into(),
        }
    }

    #[cfg(feature = "kafka-msk-iam")]
    pub fn msk_iam(region: impl Into<String>) -> Self {
        Self::MskIam {
            region: region.into(),
            profile: None,
        }
    }

    #[cfg(feature = "kafka-msk-iam")]
    pub fn msk_iam_with_profile(region: impl Into<String>, profile: impl Into<String>) -> Self {
        Self::MskIam {
            region: region.into(),
            profile: Some(profile.into()),
        }
    }

    /// librdkafka mechanism string for this variant (e.g. `"PLAIN"`).
    pub(super) fn mechanism(&self) -> &'static str {
        match self {
            Self::Plain { .. } => "PLAIN",
            Self::ScramSha256 { .. } => "SCRAM-SHA-256",
            Self::ScramSha512 { .. } => "SCRAM-SHA-512",
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam { .. } => "OAUTHBEARER",
        }
    }

    /// Username/password pair to feed into `sasl.username` / `sasl.password`.
    /// Returns `None` for variants that don't use a static password.
    pub(super) fn credentials(&self) -> Option<(&str, &str)> {
        match self {
            Self::Plain { username, password }
            | Self::ScramSha256 { username, password }
            | Self::ScramSha512 { username, password } => {
                Some((username.as_str(), password.as_str()))
            }
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam { .. } => None,
        }
    }
}

#[cfg(feature = "kafka-ssl")]
impl fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain { username, .. } => f
                .debug_struct("KafkaSasl::Plain")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::ScramSha256 { username, .. } => f
                .debug_struct("KafkaSasl::ScramSha256")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::ScramSha512 { username, .. } => f
                .debug_struct("KafkaSasl::ScramSha512")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam { region, profile } => f
                .debug_struct("KafkaSasl::MskIam")
                .field("region", region)
                .field("profile", profile)
                .finish(),
        }
    }
}

/// Producer compression codec. Maps to librdkafka's `compression.type`.
///
/// Compresses each batch on the client, so it cuts producer→broker bytes as
/// well as broker storage and replication traffic.
///
/// `#[non_exhaustive]` because librdkafka's codec list grows — it gained
/// `zstd` after the other four — so a downstream `match` needs a `_` arm and a
/// future codec stays an additive change. Naming a variant is unaffected,
/// which is the only thing callers do with this type. Applied now because the
/// type is unreleased: adding the attribute after 0.12.3 would itself be
/// breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaCompression {
    /// No compression (librdkafka's default).
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl KafkaCompression {
    pub(crate) fn as_rdkafka_str(self) -> &'static str {
        match self {
            KafkaCompression::None => "none",
            KafkaCompression::Gzip => "gzip",
            KafkaCompression::Snappy => "snappy",
            KafkaCompression::Lz4 => "lz4",
            KafkaCompression::Zstd => "zstd",
        }
    }
}

#[must_use]
pub struct KafkaConfig {
    pub brokers: String,
    #[cfg(feature = "kafka-ssl")]
    pub tls: Option<KafkaTls>,
    #[cfg(feature = "kafka-ssl")]
    pub sasl: Option<KafkaSasl>,
    /// When `true`, bypass the SASL-without-TLS refusal for development
    /// environments. **Never set this in production.** Default: `false`.
    #[cfg(feature = "kafka-ssl")]
    pub(crate) allow_plaintext_credentials: bool,
    /// Optional producer throughput knobs; `None` = librdkafka's default.
    pub(crate) producer_compression: Option<KafkaCompression>,
    pub(crate) producer_linger_ms: Option<u32>,
    pub(crate) producer_batch_size: Option<u32>,
    /// See [`Self::with_producer_auto_create_topics`].
    pub(crate) producer_auto_create_topics: bool,
}

impl KafkaConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            #[cfg(feature = "kafka-ssl")]
            tls: None,
            #[cfg(feature = "kafka-ssl")]
            sasl: None,
            #[cfg(feature = "kafka-ssl")]
            allow_plaintext_credentials: false,
            producer_compression: None,
            producer_linger_ms: None,
            producer_batch_size: None,
            producer_auto_create_topics: false,
        }
    }

    /// Bootstrap brokers string this config was built with.
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Producer `compression.type`. Unset = librdkafka's default (no
    /// compression). See the Kafka backend docs' "Producer tuning" section
    /// for when and why to tune this.
    pub fn with_producer_compression(mut self, codec: KafkaCompression) -> Self {
        self.producer_compression = Some(codec);
        self
    }

    /// Producer `linger.ms` — how long the producer waits to accumulate a
    /// batch. Unset = librdkafka's default (5 ms).
    ///
    /// Must be below the pinned `message.timeout.ms` (5000): linger time
    /// counts toward the message timeout, so values at or above it would make
    /// every publish expire in the accumulator. Rejected at `connect`.
    pub fn with_producer_linger_ms(mut self, ms: u32) -> Self {
        self.producer_linger_ms = Some(ms);
        self
    }

    /// Producer `batch.size` — maximum bytes accumulated per batch. Unset =
    /// librdkafka's default (1 MB). Accepted range `1..=i32::MAX`, checked at
    /// `connect`.
    ///
    /// librdkafka caps the effective batch at
    /// `min(batch.size, message.max.bytes)` and `message.max.bytes` (default
    /// 1 MB) is not exposed, so this knob can only lower the cap, not raise it.
    pub fn with_producer_batch_size(mut self, bytes: u32) -> Self {
        self.producer_batch_size = Some(bytes);
        self
    }

    /// Control producer topic auto-creation (`allow.auto.create.topics`).
    ///
    /// Defaults to `false`: publishing cannot create a missing topic.
    /// If the topic stays absent, publishing returns [`ShoveError::Connection`]
    /// once `message.timeout.ms` elapses.
    ///
    /// Set `true` to let the producer request topic creation on first publish.
    /// The broker must also enable `auto.create.topics.enable=true`.
    /// Created topics use broker defaults, outside shove's topology config.
    ///
    /// This controls the client's shared producer, including consumer retry,
    /// defer, and DLQ publishes.
    /// It does not change `declare()` or consumer topic auto-creation settings.
    /// Consumers keep librdkafka's default, `false`.
    pub fn with_producer_auto_create_topics(mut self, allow: bool) -> Self {
        self.producer_auto_create_topics = allow;
        self
    }

    /// Validate and apply producer throughput settings to a `ClientConfig`.
    /// Leave unset throughput properties unchanged.
    fn apply_producer_tuning(&self, cfg: &mut ClientConfig) -> Result<()> {
        if let Some(codec) = self.producer_compression {
            cfg.set("compression.type", codec.as_rdkafka_str());
        }
        if let Some(ms) = self.producer_linger_ms {
            if ms >= MESSAGE_TIMEOUT_MS {
                return Err(ShoveError::Validation(format!(
                    "producer linger.ms ({ms}) must be below the pinned \
                     message.timeout.ms ({MESSAGE_TIMEOUT_MS}); linger time counts \
                     toward the message timeout, so every publish would expire \
                     before a batch is sent"
                )));
            }
            cfg.set("linger.ms", ms.to_string());
        }
        if let Some(bytes) = self.producer_batch_size {
            if bytes == 0 || bytes > i32::MAX as u32 {
                return Err(ShoveError::Validation(format!(
                    "producer batch.size ({bytes}) must be in 1..={}",
                    i32::MAX
                )));
            }
            cfg.set("batch.size", bytes.to_string());
        }
        Ok(())
    }

    #[cfg(feature = "kafka-ssl")]
    pub fn with_tls(mut self, tls: KafkaTls) -> Self {
        self.tls = Some(tls);
        self
    }

    #[cfg(feature = "kafka-ssl")]
    pub fn with_sasl(mut self, sasl: KafkaSasl) -> Self {
        self.sasl = Some(sasl);
        self
    }

    /// Allow SASL credentials to be sent over a plaintext (non-TLS) connection.
    ///
    /// **For development use only.** Sending a static username/password over
    /// plaintext exposes credentials to any network observer. In production,
    /// always pair SASL with TLS via `with_tls(...)`.
    #[cfg(feature = "kafka-ssl")]
    pub fn allow_plaintext_credentials(mut self) -> Self {
        self.allow_plaintext_credentials = true;
        self
    }
}

impl Default for KafkaConfig {
    /// Default Kafka bootstrap endpoint for local development.
    fn default() -> Self {
        Self::new("localhost:9092")
    }
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("KafkaConfig");
        d.field("brokers", &self.brokers);
        #[cfg(feature = "kafka-ssl")]
        {
            d.field("tls", &self.tls);
            d.field("sasl", &self.sasl);
        }
        d.field("producer_compression", &self.producer_compression);
        d.field("producer_linger_ms", &self.producer_linger_ms);
        d.field("producer_batch_size", &self.producer_batch_size);
        d.field(
            "producer_auto_create_topics",
            &self.producer_auto_create_topics,
        );
        d.finish()
    }
}

#[derive(Clone)]
pub struct KafkaClient {
    brokers: String,
    /// Pre-populated ClientConfig containing `bootstrap.servers` plus any
    /// TLS/SASL settings. Every consumer/admin/metadata call clones from
    /// this so security settings never have to be re-applied at call sites.
    ///
    /// perf-K-14: stored as `Arc<ClientConfig>` so `KafkaClient::clone()` —
    /// done for every per-consumer, per-publisher, per-autoscaler-poll
    /// handle — is a refcount bump instead of a multi-KB copy of the inner
    /// HashMap (which can include large PEM blobs when TLS is configured).
    base_config: Arc<ClientConfig>,
    /// Validated at `connect`; the producer is built from it at the first
    /// publish, see [`Self::producer`].
    producer_config: Arc<ClientConfig>,
    /// The one producer every clone shares, built at the first publish and
    /// built again after a fatal error.
    producer: Arc<ProducerCell>,
    /// The consumer-type client `ping` probes with, built at `connect`.
    metadata: Arc<MetadataConsumer>,
    #[cfg(feature = "kafka-msk-iam")]
    msk_context: Option<MskIamContext>,
    shutdown_token: CancellationToken,
}

enum ProducerKind {
    Default(FutureProducer<DefaultClientContext>),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(FutureProducer<MskIamContext>),
}

/// A built producer, and whether the broker has acknowledged a record it
/// sent.
struct KafkaProducerInner {
    kind: ProducerKind,
    delivered: AtomicBool,
}

impl KafkaProducerInner {
    fn new(kind: ProducerKind) -> Self {
        Self {
            kind,
            delivered: AtomicBool::new(false),
        }
    }

    fn has_delivered(&self) -> bool {
        self.delivered.load(Ordering::Acquire)
    }

    /// Blocks until the queue is empty or `timeout` passes; the caller runs
    /// this on a blocking thread.
    fn flush(&self, timeout: Duration) -> KafkaResult<()> {
        match &self.kind {
            ProducerKind::Default(p) => p.flush(timeout),
            #[cfg(feature = "kafka-msk-iam")]
            ProducerKind::MskIam(p) => p.flush(timeout),
        }
    }

    /// The fatal error librdkafka raised on this producer, if it raised one.
    fn fatal_error(&self) -> Option<(RDKafkaErrorCode, String)> {
        match &self.kind {
            ProducerKind::Default(p) => p.client().fatal_error(),
            #[cfg(feature = "kafka-msk-iam")]
            ProducerKind::MskIam(p) => p.client().fatal_error(),
        }
    }

    async fn publish_with_retry(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        headers: OwnedHeaders,
        payload: &[u8],
        max_attempts: NonZeroU32,
        label: &str,
    ) -> Result<()> {
        let result = match &self.kind {
            ProducerKind::Default(p) => {
                publisher_publish_with_retry(p, topic, key, headers, payload, max_attempts, label)
                    .await
            }
            #[cfg(feature = "kafka-msk-iam")]
            ProducerKind::MskIam(p) => {
                publisher_publish_with_retry(p, topic, key, headers, payload, max_attempts, label)
                    .await
            }
        };
        if result.is_ok() {
            self.delivered.store(true, Ordering::Release);
        }
        result
    }

    async fn publish_in_order(
        &self,
        topic: &str,
        records: Vec<BatchRecord>,
        label: &str,
    ) -> BatchReport {
        let delivered = &self.delivered;
        match &self.kind {
            ProducerKind::Default(p) => {
                publisher_publish_in_order(p, topic, records, label, delivered).await
            }
            #[cfg(feature = "kafka-msk-iam")]
            ProducerKind::MskIam(p) => {
                publisher_publish_in_order(p, topic, records, label, delivered).await
            }
        }
    }
}

/// The slot a producer is built into, once. A fatal error on the producer
/// swaps a new, empty slot in, rather than resetting this one under a
/// publish that is still reading it.
type ProducerSlot = Arc<OnceCell<Arc<KafkaProducerInner>>>;

/// The client's producer, shared by every clone of the client.
///
/// Locked only to read or swap small values, which cannot panic, so a
/// poisoned lock still holds a valid state.
struct ProducerCell {
    state: RwLock<CellState>,
    /// Producer builds started, for tests that bound them.
    #[cfg(test)]
    builds: std::sync::atomic::AtomicUsize,
}

struct CellState {
    slot: ProducerSlot,
    /// Set when the last producer died of a fatal error before it delivered
    /// a record: no producer is built until the hold has passed.
    hold: Option<RebuildHold>,
    /// The wait the next producer that dies before delivering is given.
    next_wait: Duration,
}

/// A fatal error that holds every producer build back for `wait` from
/// `since`.
struct RebuildHold {
    since: tokio::time::Instant,
    wait: Duration,
    cause: String,
}

impl ProducerCell {
    fn new() -> Self {
        Self {
            state: RwLock::new(CellState {
                slot: Arc::new(OnceCell::new()),
                hold: None,
                next_wait: PRODUCER_REBUILD_WAIT,
            }),
            #[cfg(test)]
            builds: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn builds(&self) -> usize {
        self.builds.load(Ordering::Relaxed)
    }

    fn read(&self) -> RwLockReadGuard<'_, CellState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, CellState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// The slot in use now.
    fn current(&self) -> ProducerSlot {
        Arc::clone(&self.read().slot)
    }

    /// The error a producer build is refused with while a hold lasts.
    fn held(&self) -> Option<ShoveError> {
        let state = self.read();
        let hold = state.hold.as_ref()?;
        let left = hold.wait.saturating_sub(hold.since.elapsed());
        if left.is_zero() {
            return None;
        }
        Some(ShoveError::Unknown(format!(
            "no Kafka producer is built for another {left:?}: the last one was marked fatal \
             before it delivered a record: {}",
            hold.cause
        )))
    }

    /// Swaps an empty slot in when the slot in use holds `dead`, and
    /// returns the slot it replaced with the wait before a new producer is
    /// built. A producer that delivered a record is replaced at once and
    /// resets the wait; one that never did holds every build back for the
    /// wait, which doubles for the next such producer. A slot with another
    /// producer, or a build in flight, came after `dead` and is left alone.
    fn discard(
        &self,
        dead: &Arc<KafkaProducerInner>,
        cause: String,
    ) -> Option<(ProducerSlot, Duration)> {
        let mut state = self.write();
        if !state
            .slot
            .get()
            .is_some_and(|current| Arc::ptr_eq(current, dead))
        {
            return None;
        }
        let wait = if dead.has_delivered() {
            state.next_wait = PRODUCER_REBUILD_WAIT;
            state.hold = None;
            Duration::ZERO
        } else {
            let wait = state.next_wait;
            state.next_wait = wait.saturating_mul(2).min(PRODUCER_REBUILD_WAIT_MAX);
            state.hold = Some(RebuildHold {
                since: tokio::time::Instant::now(),
                wait,
                cause,
            });
            wait
        };
        let replaced = std::mem::replace(&mut state.slot, Arc::new(OnceCell::new()));
        Some((replaced, wait))
    }

    /// The producer in the slot in use, if one is built.
    #[cfg(test)]
    fn built(&self) -> Option<Arc<KafkaProducerInner>> {
        self.current().get().cloned()
    }
}

/// A publish's hold on the client's producer. However the publish ends,
/// finished or cancelled mid-send, its reference goes through
/// [`release_producer`].
struct ProducerLease {
    // Fields drop in order, so `_release` holds the possibly last reference.
    producer: Arc<KafkaProducerInner>,
    _release: ReleaseOnDrop,
}

impl ProducerLease {
    fn new(producer: Arc<KafkaProducerInner>) -> Self {
        Self {
            _release: ReleaseOnDrop(Some(Arc::clone(&producer))),
            producer,
        }
    }
}

impl std::ops::Deref for ProducerLease {
    type Target = KafkaProducerInner;

    fn deref(&self) -> &KafkaProducerInner {
        &self.producer
    }
}

/// Hands its reference to [`release_producer`] when dropped.
struct ReleaseOnDrop(Option<Arc<KafkaProducerInner>>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(producer) = self.0.take() {
            release_producer(producer);
        }
    }
}

#[cfg(test)]
thread_local! {
    static RELEASED_PRODUCERS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Drops a reference to a producer. Dropping the last one destroys the
/// producer, which joins its threads and waits on its queue in librdkafka,
/// so that drop runs on a blocking thread.
fn release_producer(producer: Arc<KafkaProducerInner>) {
    #[cfg(test)]
    RELEASED_PRODUCERS.with(|released| released.set(released.get().saturating_add(1)));
    let Some(last) = Arc::into_inner(producer) else {
        return;
    };
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => drop(runtime.spawn_blocking(move || drop(last))),
        Err(_) => drop(last),
    }
}

/// librdkafka's constructor is synchronous and may block on TLS material, so
/// the caller runs this on a blocking thread.
fn build_producer(
    config: &ClientConfig,
    #[cfg(feature = "kafka-msk-iam")] msk_context: Option<MskIamContext>,
) -> Result<KafkaProducerInner> {
    #[cfg(feature = "kafka-msk-iam")]
    if let Some(ctx) = msk_context {
        let p: FutureProducer<MskIamContext> = config
            .create_with_context(ctx)
            .map_err(|e| ShoveError::Topology(format!("failed to create MSK IAM producer: {e}")))?;
        return Ok(KafkaProducerInner::new(ProducerKind::MskIam(p)));
    }
    let p: FutureProducer<DefaultClientContext> = config
        .create()
        .map_err(|e| ShoveError::Topology(format!("failed to create Kafka producer: {e}")))?;
    Ok(KafkaProducerInner::new(ProducerKind::Default(p)))
}

/// The producer's client config: the connection settings of `base`, the
/// pinned correctness settings, and the tuning `config` opts into.
///
/// `enable.idempotence=true` because `publish_with_retry` retries a timed-out
/// send, and the broker then deduplicates by producer id and sequence instead
/// of storing the record twice. It needs Kafka 0.11 or later and caps
/// in-flight requests per connection at five.
///
/// librdkafka enables producer topic auto-creation by default.
/// Set the option explicitly to preserve shove's topic ownership policy.
/// See [`KafkaConfig::with_producer_auto_create_topics`] for the public contract.
fn producer_config(
    base: &ClientConfig,
    client_name: &str,
    config: &KafkaConfig,
) -> Result<ClientConfig> {
    let mut producer_config = base.clone();
    producer_config
        .set("client.id", client_name)
        .set("message.timeout.ms", MESSAGE_TIMEOUT_MS.to_string())
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set(
            "allow.auto.create.topics",
            if config.producer_auto_create_topics {
                "true"
            } else {
                "false"
            },
        );
    config.apply_producer_tuning(&mut producer_config)?;
    Ok(producer_config)
}

impl KafkaClient {
    pub async fn connect(config: &KafkaConfig) -> Result<Self> {
        let client_name = format!("shove-rs-{}", process::id());

        let mut base_config = ClientConfig::new();
        base_config.set("bootstrap.servers", &config.brokers);

        #[cfg(feature = "kafka-ssl")]
        {
            let protocol = match (config.tls.is_some(), config.sasl.is_some()) {
                (true, true) => Some("SASL_SSL"),
                (true, false) => Some("SSL"),
                (false, true) => Some("SASL_PLAINTEXT"),
                (false, false) => None,
            };
            // Only warn for mechanisms that actually transmit a static
            // username/password. OAUTHBEARER (MSK IAM) sends a signed token
            // instead, and MSK IAM is rejected outright below if TLS is off —
            // suppressing the warn here keeps the eventual Topology error
            // the only thing the operator sees.
            //
            // sec-K-2: static credentials over plaintext is refused outright.
            // A warning is too easy to miss in production logs; the password
            // would already be on the wire by the time anyone reads it.
            // Use KafkaConfig::allow_plaintext_credentials() to opt in
            // explicitly for development environments.
            if config
                .sasl
                .as_ref()
                .is_some_and(|s| s.credentials().is_some())
                && config.tls.is_none()
                && !config.allow_plaintext_credentials
            {
                return Err(ShoveError::Topology(
                    "Kafka SASL credentials require TLS: set KafkaConfig::with_tls(...) before \
                     connecting. Sending a static username/password over plaintext exposes \
                     credentials to any network observer. To allow this for development, call \
                     KafkaConfig::allow_plaintext_credentials()."
                        .into(),
                ));
            }
            if let Some(p) = protocol {
                base_config.set("security.protocol", p);
            }

            if let Some(tls) = &config.tls {
                if let Some(v) = tls.ca_location.as_ref().and_then(|p| p.to_str()) {
                    base_config.set("ssl.ca.location", v);
                }
                if let Some(v) = &tls.ca_pem {
                    base_config.set("ssl.ca.pem", v);
                }
                if let Some(v) = tls.certificate_location.as_ref().and_then(|p| p.to_str()) {
                    base_config.set("ssl.certificate.location", v);
                }
                if let Some(v) = &tls.certificate_pem {
                    base_config.set("ssl.certificate.pem", v);
                }
                if let Some(v) = tls.key_location.as_ref().and_then(|p| p.to_str()) {
                    base_config.set("ssl.key.location", v);
                }
                if let Some(v) = &tls.key_pem {
                    base_config.set("ssl.key.pem", v);
                }
                if let Some(v) = &tls.key_password {
                    base_config.set("ssl.key.password", v);
                }
                if tls.skip_hostname_verification {
                    base_config.set("ssl.endpoint.identification.algorithm", "none");
                }
            }

            if let Some(sasl) = &config.sasl {
                base_config.set("sasl.mechanism", sasl.mechanism());
                if let Some((username, password)) = sasl.credentials() {
                    base_config.set("sasl.username", username);
                    base_config.set("sasl.password", password);
                }
            }
        }

        // Phase A — compute optional MskIamContext.
        #[cfg(feature = "kafka-msk-iam")]
        let msk_context: Option<MskIamContext> = match &config.sasl {
            Some(KafkaSasl::MskIam { region, profile }) => {
                if config.tls.is_none() {
                    return Err(ShoveError::Topology(
                        "MSK IAM auth requires TLS; set KafkaConfig::with_tls(...) before connect"
                            .into(),
                    ));
                }
                let provider =
                    Arc::new(MskIamTokenProvider::new(region.clone(), profile.clone()).await?);
                Some(MskIamContext::new(provider))
            }
            _ => None,
        };

        // Phase B — overlay SASL_SSL/OAUTHBEARER when MSK IAM is active (wins
        // over the protocol-matrix block above).
        #[cfg(feature = "kafka-msk-iam")]
        if msk_context.is_some() {
            base_config.set("security.protocol", "SASL_SSL");
            base_config.set("sasl.mechanism", "OAUTHBEARER");
        }

        // Phase C: validate the producer's config, and build the client
        // `ping` probes with. The producer itself is built at the first
        // publish, see `Self::producer`.
        let producer_config = producer_config(&base_config, &client_name, config)?;
        let mut metadata_config = base_config.clone();
        // `ping` asks about a topic that does not exist; the broker must not create it.
        metadata_config.set("allow.auto.create.topics", "false");
        #[cfg(feature = "kafka-msk-iam")]
        let metadata_context = msk_context.clone();
        let metadata = tokio::task::spawn_blocking(move || {
            MetadataConsumer::create(
                metadata_config,
                #[cfg(feature = "kafka-msk-iam")]
                metadata_context,
            )
        })
        .await
        .map_err(|e| ShoveError::Topology(format!("metadata consumer task failed: {e}")))??;

        Ok(Self {
            brokers: config.brokers.clone(),
            base_config: Arc::new(base_config),
            producer_config: Arc::new(producer_config),
            producer: Arc::new(ProducerCell::new()),
            metadata: Arc::new(metadata),
            #[cfg(feature = "kafka-msk-iam")]
            msk_context,
            shutdown_token: CancellationToken::new(),
        })
    }

    /// The client's one producer, built on the first call and shared by
    /// every clone. A client that never publishes never builds it, and so
    /// never requests a producer id. After `shutdown` the build is refused.
    /// A producer that librdkafka marked fatal is dropped by the publish that
    /// met the error, and a later call builds a new one, held back for a
    /// while when the dead one never delivered a record; see
    /// [`ProducerCell::discard`].
    async fn producer(&self) -> Result<ProducerLease> {
        let slot = self.producer.current();
        let producer = slot
            .get_or_try_init(|| async {
                if self.shutdown_token.is_cancelled() {
                    return Err(ShoveError::Connection("client is shut down".into()));
                }
                if let Some(held) = self.producer.held() {
                    return Err(held);
                }
                #[cfg(test)]
                self.producer.builds.fetch_add(1, Ordering::Relaxed);
                let config = Arc::clone(&self.producer_config);
                #[cfg(feature = "kafka-msk-iam")]
                let msk_context = self.msk_context.clone();
                tokio::task::spawn_blocking(move || {
                    build_producer(
                        &config,
                        #[cfg(feature = "kafka-msk-iam")]
                        msk_context,
                    )
                })
                .await
                .map_err(|e| ShoveError::Topology(format!("producer build task failed: {e}")))?
                .map(Arc::new)
            })
            .await?;
        Ok(ProducerLease::new(Arc::clone(producer)))
    }

    /// Drops `producer` from the client when librdkafka marked it fatal, so
    /// a later publish builds a new one.
    fn discard_if_fatal(&self, producer: &Arc<KafkaProducerInner>) {
        let Some((code, reason)) = producer.fatal_error() else {
            return;
        };
        let cause = format!("{code}: {reason}");
        if let Some((_replaced, wait)) = self.producer.discard(producer, cause) {
            tracing::warn!(
                error = %code,
                reason,
                ?wait,
                "the Kafka producer hit a fatal error; a new one is built once the wait has passed"
            );
        }
    }

    pub async fn connect_with_retry(config: &KafkaConfig, max_attempts: u32) -> Result<Self> {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
        let mut attempts = 0u32;

        loop {
            attempts = attempts.saturating_add(1);
            match Self::connect(config).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        attempt = attempts,
                        max_attempts,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        error = %e,
                        "Kafka connection failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Publishes one record to `topic`, sending it again after a transient
    /// failure, at most `max_attempts` sends in all.
    ///
    /// `max_attempts` of zero is `ShoveError::Validation`, returned before
    /// anything is built or sent. An authorization error ends the call at
    /// the first refusal with `ShoveError::Topology`. A producer that
    /// librdkafka marked fatal ends it with `ShoveError::Unknown`, whatever
    /// the cause, including a principal that may not request a producer id;
    /// neither error is retryable. The client then builds a new producer: at
    /// the next publish when the dead one had delivered a record, and
    /// otherwise after a wait of 1 s that doubles for each such producer in a
    /// row, up to 60 s, while every publish fails at once with the same
    /// `ShoveError::Unknown`. Any other failure is sent again, and the last
    /// one is returned as `ShoveError::Connection`.
    pub async fn publish_with_retry(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        headers: OwnedHeaders,
        payload: &[u8],
        max_attempts: u32,
        label: &str,
    ) -> Result<()> {
        let Some(max_attempts) = NonZeroU32::new(max_attempts) else {
            return Err(ShoveError::Validation(format!(
                "{label}: max_attempts must be at least 1"
            )));
        };
        let producer = self.producer().await?;
        let result = producer
            .publish_with_retry(topic, key, headers, payload, max_attempts, label)
            .await;
        if result.is_err() {
            self.discard_if_fatal(&producer.producer);
        }
        result
    }

    /// Sends a batch to `topic` in slice order and reports a prefix, so a
    /// republish of what the report names stores copies of the failed record
    /// and every record after it in slice order. Nothing is retried.
    pub(super) async fn publish_in_order(
        &self,
        topic: &str,
        records: Vec<BatchRecord>,
        label: &str,
    ) -> BatchReport {
        let producer = match self.producer().await {
            Ok(producer) => producer,
            Err(e) => return BatchReport::wholly_unattempted(records.len(), e),
        };
        let report = producer.publish_in_order(topic, records, label).await;
        if report.first_err().is_some() {
            self.discard_if_fatal(&producer.producer);
        }
        report
    }

    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Base `ClientConfig` with `bootstrap.servers` and any TLS/SASL settings
    /// already applied. Clone this, then layer per-client settings (group.id,
    /// client.id, ...) before `.create()`.
    ///
    /// `pub(super)` — intentionally not `pub`. rdkafka's `ClientConfig` Debug
    /// impl does **not** redact `ssl.ca.pem`, `ssl.certificate.pem`, or
    /// `ssl.key.pem`; any external caller that logs this value via `{:?}`
    /// would dump raw PEM including private keys. Internal call sites only
    /// use the returned config to create consumers/admins — they do not log it.
    pub(super) fn base_config(&self) -> ClientConfig {
        (*self.base_config).clone()
    }

    /// Look up a single non-sensitive rdkafka configuration entry.
    ///
    /// Sensitive keys (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`,
    /// `ssl.key.password`, `sasl.password`) always return `None` — callers
    /// cannot reach raw PEM or credentials through this method.
    ///
    /// Intended for integration tests that verify the client's rdkafka config
    /// without exposing the full `ClientConfig` (which includes raw PEM in its
    /// `Debug` representation).
    pub fn config_entry(&self, key: &str) -> Option<String> {
        const SENSITIVE: &[&str] = &[
            "ssl.ca.pem",
            "ssl.certificate.pem",
            "ssl.key.pem",
            "ssl.key.password",
            "sasl.password",
        ];
        if SENSITIVE.contains(&key) {
            return None;
        }
        self.base_config
            .config_map()
            .get(key)
            .map(|v| v.to_string())
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Liveness check. Issues a single metadata request that names one probe
    /// topic, on the metadata consumer `connect` built. Any answer from a
    /// broker shows the cluster is reachable, so the probe topic's own error
    /// is not looked at. No new socket, no consumer-group churn, no producer,
    /// and no topic created.
    ///
    /// Returns `Err(ShoveError::Connection)` if the client is shut down, the
    /// metadata fetch fails, or `spawn_blocking` itself fails.
    pub(super) async fn ping(&self, timeout: Duration) -> Result<()> {
        self.ping_metadata(timeout).await.map(|_| ())
    }

    async fn ping_metadata(&self, timeout: Duration) -> Result<Metadata> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection("client is shut down".into()));
        }
        let consumer = Arc::clone(&self.metadata);
        #[cfg(feature = "kafka-msk-iam")]
        let shutdown = self.shutdown_token();
        let join = tokio::task::spawn_blocking(move || {
            consumer.fetch(
                PING_PROBE_TOPIC,
                timeout,
                #[cfg(feature = "kafka-msk-iam")]
                &shutdown,
            )
        });

        let metadata_result = tokio::time::timeout(timeout, join)
            .await
            .map_err(|_| ShoveError::Connection(format!("kafka ping timed out after {timeout:?}")))?
            .map_err(|e| ShoveError::Connection(format!("kafka ping task failed: {e}")))?;

        metadata_result.map_err(|e| ShoveError::Connection(format!("kafka ping failed: {e}")))
    }

    /// Test-only seam (see the `test-support` feature): each topic the
    /// metadata answer to a `ping` lists, with the error the broker gave it.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn ping_topics_for_test(
        &self,
        timeout: Duration,
    ) -> Result<Vec<(String, Option<RDKafkaRespErr>)>> {
        let metadata = self.ping_metadata(timeout).await?;
        Ok(metadata
            .topics()
            .iter()
            .map(|topic| (topic.name().to_string(), topic.error()))
            .collect())
    }

    /// What the broker says of one topic, creating nothing; see
    /// `probe_topic_blocking`. `Err` is a metadata fetch that failed, so
    /// nothing was learned about the topic.
    pub(super) async fn probe_topic(&self, name: &str) -> Result<TopicProbe> {
        let base = (*self.base_config).clone();
        let topic_name = name.to_string();
        #[cfg(feature = "kafka-msk-iam")]
        let msk_ctx = self.msk_context();
        #[cfg(feature = "kafka-msk-iam")]
        let shutdown = self.shutdown_token();
        tokio::task::spawn_blocking(move || {
            probe_topic_blocking(
                base,
                &topic_name,
                #[cfg(feature = "kafka-msk-iam")]
                msk_ctx,
                #[cfg(feature = "kafka-msk-iam")]
                shutdown,
            )
        })
        .await
        .map_err(|e| ShoveError::Topology(format!("metadata task failed: {e}")))?
    }

    /// Whether the producer would create a topic it publishes to.
    pub(super) fn producer_creates_topics(&self) -> bool {
        self.producer_config
            .config_map()
            .get("allow.auto.create.topics")
            .is_some_and(|v| *v == "true")
    }

    /// Confirm an infra-owned topic exists and return its partition count,
    /// creating nothing. `what` names the topic's role in the refusal.
    pub(super) async fn verify_external_topic(&self, name: &str, what: &str) -> Result<i32> {
        match self.probe_topic(name).await? {
            TopicProbe::Present(partitions) => Ok(partitions),
            TopicProbe::Missing => Err(external_topic_missing(what, name)),
            TopicProbe::Failed(code) => Err(ShoveError::Topology(format!(
                "metadata for topic {name} returned {code:?}"
            ))),
        }
    }

    /// librdkafka's constructor may block on TLS material, so the admin client
    /// is built on a blocking thread.
    pub(super) async fn create_admin_default(&self) -> Result<AdminClient<DefaultClientContext>> {
        let config = self.base_config();
        tokio::task::spawn_blocking(move || config.create::<AdminClient<DefaultClientContext>>())
            .await
            .map_err(|e| ShoveError::Topology(format!("admin client task failed: {e}")))?
            .map_err(|e| ShoveError::Topology(format!("failed to create admin client: {e}")))
    }

    #[cfg(feature = "kafka-msk-iam")]
    pub(super) async fn create_admin_msk(
        &self,
        ctx: MskIamContext,
    ) -> Result<AdminClient<MskIamContext>> {
        let config = self.base_config();
        let admin_ctx = ctx.clone();
        let admin = tokio::task::spawn_blocking(move || {
            config.create_with_context::<_, AdminClient<MskIamContext>>(admin_ctx)
        })
        .await
        .map_err(|e| ShoveError::Topology(format!("MSK admin client task failed: {e}")))?
        .map_err(|e| ShoveError::Topology(format!("failed to create MSK admin client: {e}")))?;
        // Unlike the producer and stream/base consumers, rdkafka's admin client
        // never polls its main queue, so it never services the OAUTHBEARER
        // token-refresh event and would fail authentication under MSK IAM. Admin
        // operations are short-lived (well under a token lifetime), so we set the
        // token once here and return a ready-to-use client — no refresh thread,
        // and thus nothing that could outlive `admin`.
        prime_admin_oauth_token(&admin, ctx).await?;
        Ok(admin)
    }

    /// Create a topic, or bring an existing one up to spec: treat its
    /// partition count as `existing` says, and reconcile the given
    /// topic-level config entries if they drift from the live values.
    /// Returns the growth when partitions were added to an existing topic.
    pub(super) async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        config: &[(String, String)],
        existing: ExistingTopic,
    ) -> Result<Option<PartitionGrowth>> {
        #[cfg(feature = "kafka-msk-iam")]
        if let Some(ctx) = self.msk_context() {
            let admin = self.create_admin_msk(ctx).await?;
            return self
                .create_topic_with_admin(
                    &admin,
                    name,
                    num_partitions,
                    replication_factor,
                    config,
                    existing,
                )
                .await;
        }
        let admin = self.create_admin_default().await?;
        self.create_topic_with_admin(
            &admin,
            name,
            num_partitions,
            replication_factor,
            config,
            existing,
        )
        .await
    }

    async fn create_topic_with_admin<C>(
        &self,
        admin: &AdminClient<C>,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        config: &[(String, String)],
        existing: ExistingTopic,
    ) -> Result<Option<PartitionGrowth>>
    where
        C: ClientContext + 'static,
    {
        let mut new_topic = NewTopic::new(
            name,
            num_partitions,
            TopicReplication::Fixed(replication_factor),
        );
        for (key, value) in config {
            new_topic = new_topic.set(key, value);
        }
        let results = admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await
            .map_err(|e| ShoveError::Topology(format!("failed to create topic {name}: {e}")))?;

        let mut growth = None;
        for result in results {
            match result {
                Ok(_) => {}
                Err((topic, code)) => {
                    if code == RDKafkaErrorCode::TopicAlreadyExists {
                        tracing::debug!(topic, "topic already exists, checking partition count");
                        // Settle the count before the configs, so a refused topic stays untouched.
                        growth = match existing {
                            ExistingTopic::Grow => {
                                self.ensure_partitions(admin, name, num_partitions).await?
                            }
                            ExistingTopic::Keep => {
                                let live = self.live_partition_count(name).await?;
                                check_kept_partitions(name, live, num_partitions)?;
                                None
                            }
                        };
                        self.ensure_topic_configs(admin, name, config).await?;
                    } else {
                        metrics::record_backend_error(
                            metrics::BackendLabel::Kafka,
                            metrics::BackendErrorKind::Topology,
                        );
                        return Err(ShoveError::Topology(format!(
                            "failed to create topic {topic}: {code:?}"
                        )));
                    }
                }
            }
        }

        Ok(growth)
    }

    /// If the existing topic has fewer partitions than `desired`, expand it.
    /// `Some` when partitions were added.
    async fn ensure_partitions<C>(
        &self,
        admin: &AdminClient<C>,
        name: &str,
        desired: i32,
    ) -> Result<Option<PartitionGrowth>>
    where
        C: ClientContext + 'static,
    {
        use rdkafka::admin::NewPartitions;

        let current = self.live_partition_count(name).await?;
        if current >= desired {
            tracing::debug!(
                topic = name,
                current,
                desired,
                "partition count already sufficient"
            );
            return Ok(None);
        }

        tracing::info!(topic = name, current, desired, "expanding partition count");
        let target = usize::try_from(desired).map_err(|_| {
            ShoveError::Topology(format!(
                "cannot expand {name} to {desired} partitions: not a partition count"
            ))
        })?;
        let new_parts = NewPartitions::new(name, target);
        let results = admin
            .create_partitions(&[new_parts], &AdminOptions::new())
            .await
            .map_err(|e| {
                ShoveError::Topology(format!("failed to expand partitions for {name}: {e}"))
            })?;

        for result in results {
            if let Err((topic, code)) = result {
                // Lost the expansion race: the broker refuses a count the topic already has.
                if code == RDKafkaErrorCode::InvalidPartitions
                    && self.partition_count_reaching(name, desired).await? >= desired
                {
                    tracing::debug!(topic, desired, "another declarer expanded the topic first");
                    return Ok(None);
                }
                metrics::record_backend_error(
                    metrics::BackendLabel::Kafka,
                    metrics::BackendErrorKind::Topology,
                );
                return Err(ShoveError::Topology(format!(
                    "failed to expand partitions for {topic}: {code:?}"
                )));
            }
        }

        Ok(Some(PartitionGrowth {
            from: current,
            to: desired,
        }))
    }

    /// The partition count the broker reports for a topic that `CreateTopics`
    /// said exists, read as `read_partition_count` reads it.
    async fn live_partition_count(&self, name: &str) -> Result<i32> {
        self.partition_count_reaching(name, 0).await
    }

    /// The partition count of a topic, read until it is at least `at_least`.
    async fn partition_count_reaching(&self, name: &str, at_least: i32) -> Result<i32> {
        read_partition_count(name, at_least, || self.probe_topic(name)).await
    }

    /// If any declared config entry differs from the topic's live value,
    /// realign the topic via AlterConfigs. No drift ⇒ no alter call, so
    /// idempotent redeclares stay quiet.
    async fn ensure_topic_configs<C>(
        &self,
        admin: &AdminClient<C>,
        name: &str,
        desired: &[(String, String)],
    ) -> Result<()>
    where
        C: ClientContext + 'static,
    {
        if desired.is_empty() {
            return Ok(());
        }

        let specifier = ResourceSpecifier::Topic(name);
        let described = admin
            .describe_configs([&specifier], &AdminOptions::new())
            .await
            .map_err(|e| {
                ShoveError::Topology(format!("failed to describe configs for {name}: {e}"))
            })?;
        let resource = described
            .into_iter()
            .next()
            .ok_or_else(|| {
                ShoveError::Topology(format!("no config resource returned for topic {name}"))
            })?
            .map_err(|code| {
                ShoveError::Topology(format!("failed to describe configs for {name}: {code:?}"))
            })?;

        let live = resource.entry_map();
        let drift: Vec<&str> = desired
            .iter()
            .filter(|(k, v)| {
                live.get(k.as_str()).and_then(|e| e.value.as_deref()) != Some(v.as_str())
            })
            .map(|(k, _)| k.as_str())
            .collect();
        if drift.is_empty() {
            tracing::debug!(topic = name, "topic configs already match declared values");
            return Ok(());
        }

        tracing::info!(topic = name, keys = ?drift, "reconciling topic configs");
        let merged = overlay_dynamic_entries(&resource.entries, desired);
        let mut alter = AlterConfig::new(ResourceSpecifier::Topic(name));
        for (key, value) in &merged {
            alter = alter.set(key, value);
        }
        let results = admin
            .alter_configs([&alter], &AdminOptions::new())
            .await
            .map_err(|e| {
                ShoveError::Topology(format!("failed to alter configs for {name}: {e}"))
            })?;
        for result in results {
            if let Err((spec, code)) = result {
                metrics::record_backend_error(
                    metrics::BackendLabel::Kafka,
                    metrics::BackendErrorKind::Topology,
                );
                return Err(ShoveError::Topology(format!(
                    "failed to alter configs for {spec:?}: {code:?}"
                )));
            }
        }
        Ok(())
    }

    /// Cancels the client's shutdown token, gives in-flight work a moment,
    /// and flushes the producer if one was built.
    ///
    /// A producer build still in flight is waited for a bounded time, and the
    /// flush, which blocks until the queue is empty or its timeout passes,
    /// runs on a blocking thread, so neither holds the runtime.
    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        // A failing initializer waits for a build in flight and starts none,
        // and the cancelled token refuses every build from here on.
        let slot = self.producer.current();
        let producer = match tokio::time::timeout(
            PRODUCER_BUILD_SHUTDOWN_WAIT,
            slot.get_or_try_init(|| async { Err::<Arc<KafkaProducerInner>, ()>(()) }),
        )
        .await
        {
            Ok(Ok(producer)) => Arc::clone(producer),
            Ok(Err(())) => return,
            Err(_elapsed) => {
                tracing::warn!(
                    wait = ?PRODUCER_BUILD_SHUTDOWN_WAIT,
                    "a Kafka producer build was still running at shutdown; its records are not flushed"
                );
                return;
            }
        };
        // A discard during the flush must not leave the last reference here.
        drop(slot);
        match tokio::task::spawn_blocking(move || producer.flush(PRODUCER_FLUSH_TIMEOUT)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(
                error = %e,
                "the Kafka producer flush at shutdown did not finish; unsent records are dropped"
            ),
            Err(e) => tracing::warn!(error = %e, "the Kafka producer flush task failed"),
        }
    }

    #[cfg(feature = "kafka-msk-iam")]
    #[allow(dead_code)]
    pub(super) fn msk_context(&self) -> Option<MskIamContext> {
        self.msk_context.clone()
    }
}

/// Merge set for a legacy `AlterConfigs` call: the topic's current
/// dynamic-topic entries overlaid with the declared `desired` keys.
///
/// Legacy AlterConfigs (the only variant rdkafka 0.39 wraps) **replaces** the
/// topic's entire dynamic config set — submitting only the declared keys would
/// silently reset every other dynamic entry to cluster defaults. Only entries
/// with `source == DynamicTopic` are carried over: default/broker-sourced
/// values are not part of the topic's dynamic set and must not be pinned onto
/// it. Valueless entries (sensitive) are skipped — they cannot be read back,
/// so they cannot be preserved.
fn overlay_dynamic_entries<'a>(
    entries: &'a [ConfigEntry],
    desired: &'a [(String, String)],
) -> BTreeMap<&'a str, &'a str> {
    let mut merged: BTreeMap<&str, &str> = BTreeMap::new();
    for e in entries {
        if e.source == ConfigSource::DynamicTopic
            && let Some(v) = e.value.as_deref()
        {
            merged.insert(e.name.as_str(), v);
        }
    }
    for (k, v) in desired {
        merged.insert(k.as_str(), v.as_str());
    }
    merged
}

/// Reads a topic's partition count through `probe` until it is at least
/// `at_least`, and returns the last count read.
///
/// A topic whose metadata is `UNKNOWN_TOPIC_OR_PART` (the broker metadata comes
/// from is behind the one that created it) or `LEADER_NOT_AVAILABLE` (its
/// leaders are still being elected) is read again, a bounded number of times.
/// So is a count below `at_least`; the last such count is returned once the
/// reads run out, for the caller to judge. Any other topic-level error is final.
async fn read_partition_count<F, Fut>(name: &str, at_least: i32, mut probe: F) -> Result<i32>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<TopicProbe>>,
{
    let mut waits = Backoff::new(
        PARTITION_METADATA_RETRY_INITIAL,
        PARTITION_METADATA_RETRY_MAX,
    );
    let mut last = "no metadata";
    let mut behind = None;
    for attempt in 1..=PARTITION_METADATA_ATTEMPTS {
        match probe().await? {
            TopicProbe::Present(partitions) if partitions >= at_least => return Ok(partitions),
            TopicProbe::Present(partitions) => behind = Some(partitions),
            TopicProbe::Missing => {
                behind = None;
                last = "no such topic";
            }
            TopicProbe::Failed(RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE) => {
                behind = None;
                last = "no leaders";
            }
            TopicProbe::Failed(code) => {
                metrics::record_backend_error(
                    metrics::BackendLabel::Kafka,
                    metrics::BackendErrorKind::Topology,
                );
                return Err(ShoveError::Topology(format!(
                    "metadata for topic {name} returned {code:?}"
                )));
            }
        }
        if attempt < PARTITION_METADATA_ATTEMPTS
            && let Some(wait) = waits.next()
        {
            tokio::time::sleep(wait).await;
        }
    }
    if let Some(partitions) = behind {
        return Ok(partitions);
    }
    metrics::record_backend_error(
        metrics::BackendLabel::Kafka,
        metrics::BackendErrorKind::Topology,
    );
    Err(ShoveError::Topology(format!(
        "topic {name} exists, but its metadata still shows {last} after \
         {PARTITION_METADATA_ATTEMPTS} reads"
    )))
}

/// What `create_topic` does when the topic it is asked to create exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExistingTopic {
    /// Raise the partition count to the requested one when it is lower.
    Grow,
    /// Leave the partition count alone. A topic whose keys are routed over
    /// its partitions remaps them when one is added, so a count below the
    /// requested one is refused and a count above it is kept, with a warning.
    Keep,
}

/// A partition count `create_topic` raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PartitionGrowth {
    pub(super) from: i32,
    pub(super) to: i32,
}

/// The `ExistingTopic::Keep` verdict on a topic with `live` partitions, for a
/// topology whose keys are routed over `required` of them.
fn check_kept_partitions(name: &str, live: i32, required: i32) -> Result<()> {
    use std::cmp::Ordering;

    match live.cmp(&required) {
        Ordering::Less => {
            metrics::record_backend_error(
                metrics::BackendLabel::Kafka,
                metrics::BackendErrorKind::Topology,
            );
            Err(ShoveError::Topology(format!(
                "sequenced topic `{name}` has {live} partitions, fewer than its routing_shards \
                 ({required}). Keys are routed over the partitions, so adding one would remap \
                 keys whose backlog sits in the partition they used to map to, and shove never \
                 adds partitions to a sequenced topic. Provision the topic with {required} \
                 partitions, or lower routing_shards to {live}"
            )))
        }
        Ordering::Equal => Ok(()),
        Ordering::Greater => {
            tracing::warn!(
                topic = name,
                partitions = live,
                routing_shards = required,
                "sequenced topic has more partitions than its routing_shards; shove leaves the \
                 count alone, since adding or removing partitions remaps keys"
            );
            Ok(())
        }
    }
}

/// What the broker's metadata says of one topic.
pub(super) enum TopicProbe {
    /// The broker has no topic by that name.
    Missing,
    /// The topic exists, with this many partitions.
    Present(i32),
    /// The broker answered, but with an error for the topic that is not
    /// "unknown topic": nothing says whether it exists.
    Failed(RDKafkaRespErr),
}

/// The `Topology` refusal for an external topic the broker does not have.
/// `what` is the topic's role: "topic" or "dead-letter topic".
pub(super) fn external_topic_missing(what: &str, name: &str) -> ShoveError {
    metrics::record_backend_error(
        metrics::BackendLabel::Kafka,
        metrics::BackendErrorKind::Topology,
    );
    ShoveError::Topology(format!(
        "external(): {what} `{name}` must be provisioned before the consumer starts, \
         but the broker has no topic by that name; shove never creates or alters an \
         external topic"
    ))
}

/// What the broker says of a topic, without creating it. `Err` is a metadata
/// fetch that failed (`Connection`: the broker is unreachable or timed out),
/// which says nothing about the topic; `TopicProbe::Missing` is the broker's
/// answer that no such topic exists.
///
/// The probe is a consumer-type client with no `group.id`, and both halves
/// matter. librdkafka defaults `allow.auto.create.topics` to true for a
/// producer and sends it on a topic-specific metadata request, which is how
/// a topic gets created behind `external()`'s back on a broker running
/// `auto.create.topics.enable=true`; shove's own producer pins the flag to
/// false (`producer_config`), and the probe does not lean on that: a
/// consumer-type client defaults it to false, and it is set explicitly here
/// anyway. Without a `group.id` there is no coordinator lookup, so the probe
/// needs no group permission under a group-scoped ACL.
fn probe_topic_blocking(
    base: ClientConfig,
    topic_name: &str,
    #[cfg(feature = "kafka-msk-iam")] msk_ctx: Option<MskIamContext>,
    #[cfg(feature = "kafka-msk-iam")] shutdown: CancellationToken,
) -> Result<TopicProbe> {
    let mut cfg = base;
    cfg.set("allow.auto.create.topics", "false");
    let md = fetch_topic_metadata_blocking(
        cfg,
        topic_name,
        #[cfg(feature = "kafka-msk-iam")]
        msk_ctx,
        #[cfg(feature = "kafka-msk-iam")]
        shutdown,
    )?;
    let Some(topic) = md.topics().iter().find(|t| t.name() == topic_name) else {
        return Ok(TopicProbe::Missing);
    };
    Ok(match topic.error() {
        Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) => TopicProbe::Missing,
        Some(other) => TopicProbe::Failed(other),
        None if topic.partitions().is_empty() => TopicProbe::Missing,
        None => TopicProbe::Present(i32::try_from(topic.partitions().len()).map_err(|_| {
            ShoveError::Topology(format!("topic {topic_name} reports too many partitions"))
        })?),
    })
}

/// A consumer-type librdkafka client with no `group.id`, used for metadata
/// requests only: no coordinator lookup, so no group permission, and no
/// producer id.
enum MetadataConsumer {
    Default(BaseConsumer),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(BaseConsumer<MskIamContext>),
}

impl MetadataConsumer {
    fn create(
        cfg: ClientConfig,
        #[cfg(feature = "kafka-msk-iam")] msk_ctx: Option<MskIamContext>,
    ) -> Result<Self> {
        #[cfg(feature = "kafka-msk-iam")]
        if let Some(ctx) = msk_ctx {
            let consumer: BaseConsumer<MskIamContext> =
                cfg.create_with_context(ctx).map_err(|e| {
                    ShoveError::Topology(format!("failed to create MSK metadata consumer: {e}"))
                })?;
            return Ok(Self::MskIam(consumer));
        }
        let consumer: BaseConsumer = cfg.create().map_err(|e| {
            ShoveError::Topology(format!("failed to create metadata consumer: {e}"))
        })?;
        Ok(Self::Default(consumer))
    }

    /// Fetch the metadata of one topic. There is no way to ask for the whole
    /// cluster: that answer lists every topic and grows with the cluster.
    /// Blocking: the caller runs this inside `spawn_blocking`.
    fn fetch(
        &self,
        topic_name: &str,
        timeout: Duration,
        #[cfg(feature = "kafka-msk-iam")] shutdown: &CancellationToken,
    ) -> KafkaResult<Metadata> {
        match self {
            Self::Default(consumer) => consumer.fetch_metadata(Some(topic_name), timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(consumer) => {
                // A metadata consumer never polls, so nothing would deliver
                // the OAUTHBEARER token (rdkafka services it via the event
                // queue, which fetch_metadata does not pump). Pump it on a
                // scoped thread for the duration of the blocking fetch; the
                // thread is joined before this block returns.
                use std::sync::atomic::{AtomicBool, Ordering};
                let done = AtomicBool::new(false);
                std::thread::scope(|s| {
                    s.spawn(|| {
                        while !done.load(Ordering::Relaxed) && !shutdown.is_cancelled() {
                            let _ = consumer.poll(Duration::from_millis(100));
                        }
                    });
                    let md = consumer.fetch_metadata(Some(topic_name), timeout);
                    done.store(true, Ordering::Relaxed);
                    md
                })
            }
        }
    }
}

/// Build a one-shot [`MetadataConsumer`] from `cfg` and fetch one topic's
/// metadata synchronously. Runs inside `spawn_blocking` because librdkafka's
/// `fetch_metadata` is blocking; owns all the cfg-gated context selection.
fn fetch_topic_metadata_blocking(
    cfg: ClientConfig,
    topic_name: &str,
    #[cfg(feature = "kafka-msk-iam")] msk_ctx: Option<MskIamContext>,
    #[cfg(feature = "kafka-msk-iam")] shutdown: CancellationToken,
) -> Result<Metadata> {
    let consumer = MetadataConsumer::create(
        cfg,
        #[cfg(feature = "kafka-msk-iam")]
        msk_ctx,
    )?;
    let metadata = consumer.fetch(
        topic_name,
        Duration::from_secs(10),
        #[cfg(feature = "kafka-msk-iam")]
        &shutdown,
    );

    metadata.map_err(|e| {
        ShoveError::Connection(format!("failed to fetch metadata for {topic_name}: {e}"))
    })
}

/// Generate an OAUTHBEARER token from `ctx` and set it on the admin client's
/// native handle.
///
/// rdkafka services the OAUTHBEARER refresh event by polling the client's main
/// queue; the admin client never polls it, so we generate and set the token
/// ourselves. `generate_oauth_token` blocks its thread until the token is
/// signed, so it runs on a blocking thread rather than a Tokio worker.
#[cfg(feature = "kafka-msk-iam")]
async fn prime_admin_oauth_token<C>(admin: &AdminClient<C>, ctx: C) -> Result<()>
where
    C: ClientContext + Send + 'static,
{
    let token = tokio::task::spawn_blocking(move || {
        // The `Box<dyn Error>` from generate_oauth_token is not `Send`; reduce
        // it inside the blocking thread so only a `String` crosses the boundary.
        // A typed `Connection` error gives up its prefix; the wrapper below adds it.
        ctx.generate_oauth_token(None)
            .map_err(|e| match e.downcast::<ShoveError>() {
                Ok(typed) => match *typed {
                    ShoveError::Connection(message) => message,
                    other => other.to_string(),
                },
                Err(other) => other.to_string(),
            })
    })
    .await
    .map_err(|e| ShoveError::Topology(format!("oauth token task join failed: {e}")))?
    .map_err(|e| {
        ShoveError::Connection(format!(
            "failed to generate OAUTHBEARER token for admin client: {e}"
        ))
    })?;

    set_admin_oauth_token(admin.inner().native_ptr(), &token)
}

/// Test-only seam: run the exact admin OAUTHBEARER priming path
/// (`prime_admin_oauth_token`) against a caller-supplied admin client and
/// context. Exists because MSK IAM's presigned-URL tokens cannot be validated
/// without real AWS, so the integration test substitutes an unsecured-JWT
/// context to exercise this code path against a local OAUTHBEARER broker.
#[cfg(all(feature = "kafka-msk-iam", feature = "test-support"))]
pub async fn prime_admin_oauth_token_for_test<C>(admin: &AdminClient<C>, ctx: C) -> Result<()>
where
    C: ClientContext + Send + 'static,
{
    prime_admin_oauth_token(admin, ctx).await
}

/// Hand a generated token to librdkafka via `rd_kafka_oauthbearer_set_token`.
///
/// `rk` must be a live `rd_kafka_t` (it is: `admin` outlives this call, and we
/// set the token synchronously — no thread captures the pointer). On failure we
/// call `rd_kafka_oauthbearer_set_token_failure` so librdkafka stops waiting on
/// a token that will never arrive.
#[cfg(feature = "kafka-msk-iam")]
fn set_admin_oauth_token(rk: *mut rd_kafka_t, token: &OAuthToken) -> Result<()> {
    use std::ffi::{CStr, CString};
    use std::os::raw::c_char;

    let token_c = CString::new(token.token.as_str())
        .map_err(|_| ShoveError::Connection("OAuth token contains an interior NUL".into()))?;
    let principal_c = CString::new(token.principal_name.as_str())
        .map_err(|_| ShoveError::Connection("OAuth principal contains an interior NUL".into()))?;
    let mut errbuf = [0 as c_char; 512];

    // SAFETY: `rk` is a valid rd_kafka_t for the duration of the call; the
    // CString pointers outlive it; `errbuf` matches the errstr_size argument.
    let code = unsafe {
        rd_kafka_oauthbearer_set_token(
            rk,
            token_c.as_ptr(),
            token.lifetime_ms,
            principal_c.as_ptr(),
            std::ptr::null_mut(),
            0,
            errbuf.as_mut_ptr(),
            errbuf.len(),
        )
    };

    if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
        Ok(())
    } else {
        // SAFETY: same validity contract on `rk`; `errbuf` is a NUL-terminated
        // C string populated by the failed call above.
        let msg = unsafe {
            rd_kafka_oauthbearer_set_token_failure(rk, errbuf.as_ptr());
            CStr::from_ptr(errbuf.as_ptr())
                .to_string_lossy()
                .into_owned()
        };
        Err(ShoveError::Connection(format!(
            "rd_kafka_oauthbearer_set_token failed: {msg}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::bindings::rd_kafka_test_fatal_error;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::types::{RDKafka, RDKafkaApiKey};
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::future::{Ready, ready};
    use std::rc::Rc;

    #[test]
    fn default_config_is_localhost() {
        let cfg = KafkaConfig::default();
        assert!(cfg.brokers().contains("localhost:9092"));
    }

    #[cfg(feature = "kafka-ssl")]
    #[test]
    fn sasl_constructors_yield_expected_variants() {
        let plain = KafkaSasl::plain("alice", "pw");
        assert!(matches!(plain, KafkaSasl::Plain { .. }));

        let s256 = KafkaSasl::scram_sha_256("alice", "pw");
        assert!(matches!(s256, KafkaSasl::ScramSha256 { .. }));

        let s512 = KafkaSasl::scram_sha_512("alice", "pw");
        assert!(matches!(s512, KafkaSasl::ScramSha512 { .. }));
    }

    #[cfg(feature = "kafka-ssl")]
    #[test]
    fn sasl_debug_redacts_password() {
        let sasl = KafkaSasl::plain("alice", "s3cr3t-p@ssw0rd");
        let rendered = format!("{sasl:?}");
        assert!(
            !rendered.contains("s3cr3t-p@ssw0rd"),
            "password leaked in Debug output: {rendered}"
        );
        assert!(rendered.contains("alice"), "username should be visible");
        assert!(rendered.contains("<redacted>"));
    }

    #[cfg(feature = "kafka-ssl")]
    #[test]
    fn tls_debug_redacts_pem_and_key_password() {
        let tls = KafkaTls {
            ca_pem: Some("-----BEGIN CERTIFICATE-----CA-SECRET-----".into()),
            certificate_pem: Some("-----BEGIN CERTIFICATE-----CERT-SECRET-----".into()),
            key_pem: Some("-----BEGIN PRIVATE KEY-----KEY-SECRET-----".into()),
            key_password: Some("key-pass-s3cret".into()),
            ..KafkaTls::default()
        };
        let rendered = format!("{tls:?}");
        for secret in ["CA-SECRET", "CERT-SECRET", "KEY-SECRET", "key-pass-s3cret"] {
            assert!(
                !rendered.contains(secret),
                "secret `{secret}` leaked in Debug output: {rendered}"
            );
        }
    }

    #[cfg(feature = "kafka-ssl")]
    #[test]
    fn kafka_config_debug_redacts_nested_secrets() {
        let cfg = KafkaConfig::new("broker:9093")
            .with_tls(KafkaTls {
                ca_pem: Some("NESTED-CA-SECRET".into()),
                ..KafkaTls::default()
            })
            .with_sasl(KafkaSasl::scram_sha_512("bob", "NESTED-PASSWORD"));
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("NESTED-CA-SECRET"));
        assert!(!rendered.contains("NESTED-PASSWORD"));
        assert!(rendered.contains("broker:9093"));
        assert!(rendered.contains("bob"));
    }

    #[test]
    fn producer_tuning_sets_exactly_the_configured_properties() {
        let cfg = KafkaConfig::new("broker:9092")
            .with_producer_compression(KafkaCompression::Lz4)
            .with_producer_linger_ms(25)
            .with_producer_batch_size(500_000);
        let mut client_cfg = ClientConfig::new();
        cfg.apply_producer_tuning(&mut client_cfg).unwrap();
        assert_eq!(client_cfg.get("compression.type"), Some("lz4"));
        assert_eq!(client_cfg.get("linger.ms"), Some("25"));
        assert_eq!(client_cfg.get("batch.size"), Some("500000"));
    }

    #[test]
    fn unset_producer_tuning_is_a_noop() {
        let cfg = KafkaConfig::new("broker:9092");
        let mut client_cfg = ClientConfig::new();
        cfg.apply_producer_tuning(&mut client_cfg).unwrap();
        assert_eq!(client_cfg.get("compression.type"), None);
        assert_eq!(client_cfg.get("linger.ms"), None);
        assert_eq!(client_cfg.get("batch.size"), None);
    }

    #[test]
    fn partial_producer_tuning_sets_only_the_given_knobs() {
        let cfg = KafkaConfig::new("broker:9092").with_producer_linger_ms(50);
        let mut client_cfg = ClientConfig::new();
        cfg.apply_producer_tuning(&mut client_cfg).unwrap();
        assert_eq!(client_cfg.get("linger.ms"), Some("50"));
        assert_eq!(client_cfg.get("compression.type"), None);
        assert_eq!(client_cfg.get("batch.size"), None);
    }

    #[test]
    fn linger_at_or_above_message_timeout_is_rejected() {
        let cfg = KafkaConfig::new("broker:9092").with_producer_linger_ms(MESSAGE_TIMEOUT_MS);
        let mut client_cfg = ClientConfig::new();
        let err = cfg.apply_producer_tuning(&mut client_cfg).unwrap_err();
        assert!(matches!(err, ShoveError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn linger_just_below_message_timeout_is_accepted() {
        let cfg = KafkaConfig::new("broker:9092").with_producer_linger_ms(MESSAGE_TIMEOUT_MS - 1);
        let mut client_cfg = ClientConfig::new();
        cfg.apply_producer_tuning(&mut client_cfg).unwrap();
        assert_eq!(
            client_cfg.get("linger.ms"),
            Some((MESSAGE_TIMEOUT_MS - 1).to_string().as_str())
        );
    }

    #[test]
    fn zero_batch_size_is_rejected() {
        let cfg = KafkaConfig::new("broker:9092").with_producer_batch_size(0);
        let mut client_cfg = ClientConfig::new();
        let err = cfg.apply_producer_tuning(&mut client_cfg).unwrap_err();
        assert!(matches!(err, ShoveError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn batch_size_above_i32_max_is_rejected() {
        let cfg = KafkaConfig::new("broker:9092").with_producer_batch_size(i32::MAX as u32 + 1);
        let mut client_cfg = ClientConfig::new();
        let err = cfg.apply_producer_tuning(&mut client_cfg).unwrap_err();
        assert!(matches!(err, ShoveError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn every_advertised_compression_codec_is_supported() {
        for codec in [
            KafkaCompression::None,
            KafkaCompression::Gzip,
            KafkaCompression::Snappy,
            KafkaCompression::Lz4,
            KafkaCompression::Zstd,
        ] {
            let producer: std::result::Result<FutureProducer<DefaultClientContext>, _> =
                ClientConfig::new()
                    .set("compression.type", codec.as_rdkafka_str())
                    .create();
            assert!(
                producer.is_ok(),
                "codec {codec:?} rejected by librdkafka: {:?}",
                producer.err()
            );
        }
    }

    #[test]
    fn kafka_config_debug_shows_producer_tuning() {
        let cfg = KafkaConfig::new("broker:9092")
            .with_producer_compression(KafkaCompression::Zstd)
            .with_producer_linger_ms(25);
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("producer_compression"));
        assert!(rendered.contains("Zstd"));
        assert!(rendered.contains("producer_linger_ms"));
        assert!(rendered.contains("25"));
        assert!(rendered.contains("producer_auto_create_topics: false"));
    }

    /// By default the producer never creates the topic it publishes to, and
    /// the pinned correctness settings travel with that flag.
    #[test]
    fn producer_config_never_auto_creates_topics_by_default() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "broker:9092");
        let cfg = producer_config(&base, "shove-rs-test", &KafkaConfig::new("broker:9092"))
            .expect("the default tuning is valid");
        assert_eq!(cfg.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(cfg.get("enable.idempotence"), Some("true"));
        assert_eq!(cfg.get("acks"), Some("all"));
        assert_eq!(cfg.get("client.id"), Some("shove-rs-test"));
        assert_eq!(cfg.get("bootstrap.servers"), Some("broker:9092"));
    }

    /// The switch flips only `allow.auto.create.topics`; the pinned
    /// correctness settings stay.
    #[test]
    fn producer_config_auto_creates_topics_when_switched_on() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "broker:9092");
        let config = KafkaConfig::new("broker:9092").with_producer_auto_create_topics(true);
        let cfg =
            producer_config(&base, "shove-rs-test", &config).expect("the default tuning is valid");
        assert_eq!(cfg.get("allow.auto.create.topics"), Some("true"));
        assert_eq!(cfg.get("enable.idempotence"), Some("true"));
        assert_eq!(cfg.get("acks"), Some("all"));
    }

    /// Passing `false` explicitly is the default, spelled out.
    #[test]
    fn producer_config_switched_off_explicitly_matches_the_default() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "broker:9092");
        let config = KafkaConfig::new("broker:9092").with_producer_auto_create_topics(false);
        let cfg =
            producer_config(&base, "shove-rs-test", &config).expect("the default tuning is valid");
        assert_eq!(cfg.get("allow.auto.create.topics"), Some("false"));
    }

    // -- overlay_dynamic_entries: legacy AlterConfigs merge correctness --

    mod overlay {
        use super::super::overlay_dynamic_entries;
        use rdkafka::admin::{ConfigEntry, ConfigSource};

        fn entry(name: &str, value: &str, source: ConfigSource) -> ConfigEntry {
            ConfigEntry {
                name: name.to_string(),
                value: Some(value.to_string()),
                source,
                is_read_only: false,
                is_default: false,
                is_sensitive: false,
            }
        }

        #[test]
        fn preserves_dynamic_topic_entries_not_being_overridden() {
            let entries = vec![
                entry("segment.bytes", "123456789", ConfigSource::DynamicTopic),
                entry("retention.ms", "604800000", ConfigSource::Default),
            ];
            let desired = vec![("retention.ms".to_string(), "3600000".to_string())];
            let merged = overlay_dynamic_entries(&entries, &desired);
            assert_eq!(merged.get("segment.bytes"), Some(&"123456789"));
            assert_eq!(merged.get("retention.ms"), Some(&"3600000"));
            assert_eq!(merged.len(), 2);
        }

        #[test]
        fn excludes_non_dynamic_topic_sources() {
            // Broker-level / default entries must NOT be echoed into a topic
            // alter — only dynamic *topic* config belongs to the topic's set.
            let entries = vec![
                entry("retention.ms", "604800000", ConfigSource::Default),
                entry("compression.type", "producer", ConfigSource::DynamicBroker),
                entry("min.insync.replicas", "2", ConfigSource::StaticBroker),
            ];
            let desired = vec![("retention.ms".to_string(), "1000".to_string())];
            let merged = overlay_dynamic_entries(&entries, &desired);
            assert_eq!(merged.get("retention.ms"), Some(&"1000"));
            assert_eq!(merged.len(), 1);
        }

        #[test]
        fn desired_overrides_dynamic_topic_entry() {
            let entries = vec![entry("retention.ms", "1000", ConfigSource::DynamicTopic)];
            let desired = vec![("retention.ms".to_string(), "2000".to_string())];
            let merged = overlay_dynamic_entries(&entries, &desired);
            assert_eq!(merged.get("retention.ms"), Some(&"2000"));
            assert_eq!(merged.len(), 1);
        }

        #[test]
        fn later_desired_entry_wins_for_repeated_key() {
            let entries: Vec<ConfigEntry> = vec![];
            let desired = vec![
                ("retention.ms".to_string(), "1000".to_string()),
                ("retention.ms".to_string(), "2000".to_string()),
            ];
            let merged = overlay_dynamic_entries(&entries, &desired);
            assert_eq!(merged.get("retention.ms"), Some(&"2000"));
        }

        #[test]
        fn skips_valueless_dynamic_entries() {
            let entries = vec![ConfigEntry {
                name: "some.sensitive".to_string(),
                value: None,
                source: ConfigSource::DynamicTopic,
                is_read_only: false,
                is_default: false,
                is_sensitive: true,
            }];
            let desired = vec![("retention.ms".to_string(), "1000".to_string())];
            let merged = overlay_dynamic_entries(&entries, &desired);
            assert_eq!(merged.len(), 1);
            assert_eq!(merged.get("retention.ms"), Some(&"1000"));
        }
    }

    // -- sec-K-2: SASL over plaintext must be refused, not warned --

    #[cfg(feature = "kafka-ssl")]
    #[tokio::test]
    async fn sasl_plaintext_without_tls_is_rejected() {
        // Before the fix this produced a warn! and continued; it must now Err.
        let cfg =
            KafkaConfig::new("localhost:9092").with_sasl(KafkaSasl::plain("alice", "password"));
        // No TLS → should be a topology error, not a warn.
        let result = KafkaClient::connect(&cfg).await.map(|_| ());
        assert!(
            result.is_err(),
            "SASL over plaintext must be refused at connect() time, not just warned"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("TLS") || msg.contains("plaintext") || msg.contains("credentials"),
            "error message should describe the plaintext-credentials risk, got: {msg}"
        );
    }
    // -- the producer is built at the first publish, on a blocking thread --

    /// A client against a port nothing listens on. librdkafka connects
    /// lazily, so nothing here needs a broker.
    async fn client() -> KafkaClient {
        KafkaClient::connect(&KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("connect needs no broker")
    }

    /// `client()` with producer properties replaced after `connect`, so the
    /// producer build fails or stalls where the metadata consumer did not.
    async fn client_whose_producer_config_sets(entries: &[(&str, &str)]) -> KafkaClient {
        let mut client = client().await;
        let mut producer_config = (*client.producer_config).clone();
        for (key, value) in entries {
            producer_config.set(*key, *value);
        }
        client.producer_config = Arc::new(producer_config);
        client
    }

    /// `connect` leaves the cell empty, and concurrent first publishes from
    /// four clones end with one producer: tokio's `OnceCell` runs one
    /// initializer at a time.
    #[tokio::test]
    async fn connect_builds_no_producer_and_concurrent_clones_share_one() {
        use futures_util::future::join_all;

        let client = client().await;
        assert!(
            client.producer.built().is_none(),
            "connect built a producer"
        );
        let clones: Vec<KafkaClient> = (0..4).map(|_| client.clone()).collect();
        let built = join_all(clones.iter().map(|c| c.producer())).await;
        let first: &KafkaProducerInner = built[0].as_ref().expect("the first clone builds");
        for producer in &built {
            let producer: &KafkaProducerInner = producer.as_ref().expect("every clone gets it");
            assert!(
                std::ptr::eq(first, producer),
                "two clones hold two producers"
            );
        }
        assert!(
            client.producer.built().is_some(),
            "the cell is empty after the builds"
        );
    }

    /// After `shutdown` a publish that finds no producer is refused and
    /// builds none, so nothing is left unflushed behind the close.
    #[tokio::test]
    async fn a_publish_after_shutdown_builds_no_producer() {
        let client = client().await;
        client.shutdown().await;
        let err = client
            .publish_with_retry("orders", None, OwnedHeaders::new(), b"{}", 1, "test")
            .await
            .expect_err("a publish after shutdown is refused");
        assert!(matches!(err, ShoveError::Connection(_)), "got {err:?}");
        assert!(
            client.producer.built().is_none(),
            "the refused publish built a producer"
        );
    }

    /// `shutdown` lets go of the producer slot before it flushes. Holding it
    /// through the flush, it could own the last reference to a producer a
    /// fatal discard swapped out meanwhile, and destroy it on the runtime
    /// thread. A record to an address nothing listens on keeps the flush
    /// open for the 5 s message timeout, and the slot is counted 2 s in.
    #[tokio::test]
    async fn shutdown_holds_no_producer_slot_while_it_flushes() {
        let client = client().await;
        let publish = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .publish_with_retry("orders", None, OwnedHeaders::new(), b"{}", 1, "test")
                    .await
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while client.producer.built().is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the publish built no producer"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let close = tokio::spawn({
            let client = client.clone();
            async move { client.shutdown().await }
        });

        tokio::time::sleep(Duration::from_secs(2)).await;
        let holders = Arc::strong_count(&client.producer.current());

        close.await.expect("shutdown joins");
        publish
            .await
            .expect("the publish joins")
            .expect_err("nothing answers the record");
        assert_eq!(
            holders, 2,
            "besides the cell and this count, shutdown held the slot through its flush"
        );
    }

    /// A producer build librdkafka refuses fails the publish with the error
    /// `connect` used to return, and leaves the cell empty, so the next
    /// publish builds again instead of finding a poisoned cell.
    #[tokio::test]
    async fn a_failed_producer_build_leaves_the_cell_empty_for_the_next_publish() {
        let client = client_whose_producer_config_sets(&[("no.such.property", "1")]).await;
        for attempt in 1..=2 {
            let err = client
                .publish_with_retry("orders", None, OwnedHeaders::new(), b"{}", 1, "test")
                .await
                .expect_err("librdkafka refuses an unknown property");
            let msg = err.to_string();
            assert!(
                msg.contains("failed to create Kafka producer"),
                "attempt {attempt}: {msg}"
            );
            assert!(
                matches!(err, ShoveError::Topology(_)),
                "attempt {attempt}: {err:?}"
            );
            assert!(
                client.producer.built().is_none(),
                "attempt {attempt}: the cell is not empty"
            );
        }
    }

    fn released_producers() -> usize {
        RELEASED_PRODUCERS.with(std::cell::Cell::get)
    }

    /// A publish cancelled mid-send still hands its producer reference to
    /// `release_producer`, so a producer the client already discarded is
    /// never destroyed on the runtime thread that dropped the publish.
    #[tokio::test]
    async fn a_cancelled_publish_releases_its_producer() {
        let client = client().await;
        drop(client.producer().await.expect("nothing is needed to build"));
        let before = released_producers();

        let publish =
            client.publish_with_retry("orders", None, OwnedHeaders::new(), b"{}", 1, "test");
        tokio::time::timeout(Duration::from_millis(200), publish)
            .await
            .expect_err("no broker answers, so the publish is still waiting");

        assert_eq!(released_producers().saturating_sub(before), 1);
    }

    /// Zero attempts is a caller error, refused before anything is sent.
    #[tokio::test]
    async fn zero_attempts_is_validation() {
        let client = client().await;
        let err = client
            .publish_with_retry("orders", None, OwnedHeaders::new(), b"{}", 0, "test")
            .await
            .expect_err("no attempt is no publish");
        assert!(matches!(err, ShoveError::Validation(_)), "got {err:?}");
        assert!(
            client.producer.built().is_none(),
            "the refused publish built a producer"
        );
    }

    // -- a fatal producer error --

    fn native_handle(producer: &KafkaProducerInner) -> *mut RDKafka {
        match &producer.kind {
            ProducerKind::Default(p) => p.client().native_ptr(),
            #[cfg(feature = "kafka-msk-iam")]
            ProducerKind::MskIam(p) => p.client().native_ptr(),
        }
    }

    /// A producer that librdkafka marked fatal refuses every record from
    /// then on. The publish that meets it fails with an error that is not
    /// retryable, and since the dead producer had delivered a record, the
    /// next publish builds a new one instead of failing on the dead one for
    /// the client's lifetime.
    #[tokio::test]
    async fn fatal_producer_error_rebuilds_the_producer() {
        use super::super::constants::MAX_PUBLISH_ATTEMPTS;

        let (_mock, client) = client_on_a_mock_with(Some("orders")).await;
        let publish = || {
            client.publish_with_retry(
                "orders",
                None,
                OwnedHeaders::new(),
                b"{}",
                MAX_PUBLISH_ATTEMPTS,
                "test",
            )
        };
        publish().await.expect("a healthy producer publishes");
        let injected = {
            let producer = client.producer().await.expect("the publish built it");
            // SAFETY: the handle belongs to a live producer, held for the call.
            unsafe {
                rd_kafka_test_fatal_error(
                    native_handle(&producer),
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
                    c"injected by the test".as_ptr(),
                )
            }
        };
        assert_eq!(injected, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);

        let err = publish()
            .await
            .expect_err("a fatal producer refuses the record");
        assert!(
            !err.is_retryable(),
            "a fatal producer error reads as retryable: {err:?}"
        );

        publish()
            .await
            .expect("the next publish builds a new producer");
        let producer = client.producer().await.expect("the rebuilt producer");
        assert_eq!(
            producer.fatal_error(),
            None,
            "the client still holds the dead producer"
        );
    }

    /// A fatal error before a producer delivered anything is a fault the
    /// next producer meets as well, such as a principal that may not request
    /// a producer id. The client waits before it builds again, 1 s after the
    /// first such producer and twice as long after each one in a row, and a
    /// publish inside the wait fails at once without building.
    #[tokio::test(start_paused = true)]
    async fn a_producer_refused_its_id_is_rebuilt_after_a_growing_wait() {
        use super::super::constants::MAX_PUBLISH_ATTEMPTS;

        let (mock, client) = client_on_a_mock_with(Some("orders")).await;
        mock.request_errors(
            RDKafkaApiKey::InitProducerId,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED; 8],
        );
        let publish = || {
            client.publish_with_retry(
                "orders",
                None,
                OwnedHeaders::new(),
                b"{}",
                MAX_PUBLISH_ATTEMPTS,
                "test",
            )
        };

        let refused = publish().await.expect_err("the producer id is refused");
        assert!(!refused.is_retryable(), "got {refused:?}");
        assert_eq!(client.producer.builds(), 1);
        for _ in 0..5 {
            let err = publish()
                .await
                .expect_err("a publish inside the wait fails");
            assert!(matches!(err, ShoveError::Unknown(_)), "got {err:?}");
        }
        assert_eq!(
            client.producer.builds(),
            1,
            "a publish inside the 1 s wait built a producer"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        publish()
            .await
            .expect_err("the second producer is refused its id too");
        assert_eq!(client.producer.builds(), 2, "no build after the first wait");

        tokio::time::advance(Duration::from_secs(1)).await;
        publish().await.expect_err("still inside the second wait");
        assert_eq!(client.producer.builds(), 2, "the second wait is 2 s");

        tokio::time::advance(Duration::from_secs(1)).await;
        publish()
            .await
            .expect_err("the third producer is refused its id too");
        assert_eq!(
            client.producer.builds(),
            3,
            "no build after the second wait"
        );
    }

    /// A producer that delivered a record and then died is replaced at
    /// once, and the wait for one that never delivers starts again at 1 s.
    #[tokio::test(start_paused = true)]
    async fn a_delivery_resets_the_rebuild_wait() {
        use super::super::constants::MAX_PUBLISH_ATTEMPTS;

        let (mock, client) = client_on_a_mock_with(Some("orders")).await;
        let refuse_the_next_id = || {
            mock.request_errors(
                RDKafkaApiKey::InitProducerId,
                &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED],
            );
        };
        let publish = || {
            client.publish_with_retry(
                "orders",
                None,
                OwnedHeaders::new(),
                b"{}",
                MAX_PUBLISH_ATTEMPTS,
                "test",
            )
        };

        refuse_the_next_id();
        publish()
            .await
            .expect_err("the first producer is refused its id");
        tokio::time::advance(Duration::from_secs(1)).await;
        publish().await.expect("the second producer delivers");
        let injected = {
            let producer = client.producer().await.expect("the second producer");
            // SAFETY: the handle belongs to a live producer, held for the call.
            unsafe {
                rd_kafka_test_fatal_error(
                    native_handle(&producer),
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
                    c"injected by the test".as_ptr(),
                )
            }
        };
        assert_eq!(injected, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
        publish()
            .await
            .expect_err("the second producer is marked fatal");

        refuse_the_next_id();
        publish()
            .await
            .expect_err("the third producer is refused its id");
        assert_eq!(
            client.producer.builds(),
            3,
            "a producer that delivered is replaced at once"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        publish()
            .await
            .expect("after a 1 s wait the fourth producer delivers");
        assert_eq!(client.producer.builds(), 4);
    }

    /// TLS material librdkafka rejects fails `connect`, which builds the
    /// metadata consumer from the connection settings the producer would use.
    #[cfg(feature = "kafka-ssl")]
    #[tokio::test]
    async fn rejected_tls_material_fails_connect() {
        let cfg = KafkaConfig::new("127.0.0.1:1").with_tls(KafkaTls {
            ca_pem: Some("not a certificate".into()),
            ..KafkaTls::default()
        });
        let err = KafkaClient::connect(&cfg)
            .await
            .err()
            .expect("librdkafka rejects the CA");
        let msg = err.to_string();
        assert!(msg.contains("failed to create metadata consumer"), "{msg}");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
    }

    /// The one-shot probe reports a configuration librdkafka refuses as
    /// `Topology`, as `connect` does.
    #[test]
    fn a_probe_reports_a_refused_configuration() {
        let mut cfg = ClientConfig::new();
        cfg.set("no.such.property", "1");
        let err = fetch_topic_metadata_blocking(
            cfg,
            "orders",
            #[cfg(feature = "kafka-msk-iam")]
            None,
            #[cfg(feature = "kafka-msk-iam")]
            CancellationToken::new(),
        )
        .expect_err("librdkafka refuses an unknown property");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
    }

    /// A CA file that is a FIFO in a fresh temp dir. Opening the FIFO for
    /// writing blocks until librdkafka's constructor opens it for reading,
    /// so the writer wakes exactly when a native constructor is inside the
    /// read, holds it there for `hold` or until `release`, then hands it
    /// bytes OpenSSL rejects. One constructor reads it: a second one would
    /// wait for a writer that never comes.
    #[cfg(all(feature = "kafka-ssl", unix))]
    struct StalledCa {
        path: String,
        dir: std::path::PathBuf,
        writer: std::thread::JoinHandle<()>,
        release: std::sync::mpsc::Sender<()>,
    }

    #[cfg(all(feature = "kafka-ssl", unix))]
    impl StalledCa {
        fn new(tag: &str, hold: Duration) -> Self {
            use std::io::Write as _;

            let dir =
                std::env::temp_dir().join(format!("shove-client-fifo-{}-{tag}", process::id()));
            std::fs::create_dir_all(&dir).expect("create the temp dir");
            let fifo = dir.join("ca.pem");
            let path = fifo.to_str().expect("utf-8 path").to_string();
            let c_path = std::ffi::CString::new(path.as_str()).expect("no NUL");
            let created = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
            assert_eq!(created, 0, "mkfifo failed");
            let (release, released) = std::sync::mpsc::channel();
            let writer = std::thread::spawn(move || {
                let mut fifo = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&fifo)
                    .expect("open the FIFO for writing");
                let _ = released.recv_timeout(hold);
                fifo.write_all(b"not a certificate\n")
                    .expect("write the FIFO");
            });
            Self {
                path,
                dir,
                writer,
                release,
            }
        }

        /// End the hold now: the constructor fails as soon as it is released.
        fn release(&self) {
            let _ = self.release.send(());
        }

        /// Join the writer, which returns once the constructor has read the
        /// FIFO, and remove the temp dir.
        fn finish(self) {
            self.writer.join().expect("the FIFO writer thread");
            let _ = std::fs::remove_dir_all(&self.dir);
        }

        /// `base` with TLS on and its CA read from this FIFO.
        fn applied_to(&self, base: &ClientConfig) -> Arc<ClientConfig> {
            let mut config = base.clone();
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", self.path.as_str());
            Arc::new(config)
        }
    }

    /// A client whose producer reads its CA from a stalled FIFO.
    #[cfg(all(feature = "kafka-ssl", unix))]
    async fn client_whose_producer_build_stalls(hold: Duration) -> (KafkaClient, StalledCa) {
        let ca = StalledCa::new(&format!("producer-{}", hold.as_millis()), hold);
        let client = client_whose_producer_config_sets(&[
            ("security.protocol", "ssl"),
            ("ssl.ca.location", ca.path.as_str()),
        ])
        .await;
        (client, ca)
    }

    /// Drives `work` beside a 10 ms ticker on this runtime and returns its
    /// output with the longest gap between two ticks. On a current-thread
    /// runtime the ticker only runs while `work` is suspended, so the gap is
    /// how long `work` held the thread.
    #[cfg(all(feature = "kafka-ssl", unix))]
    async fn longest_stall<T>(work: impl std::future::Future<Output = T>) -> (T, Duration) {
        use std::sync::atomic::{AtomicBool, Ordering};

        let stop = Arc::new(AtomicBool::new(false));
        let ticker = tokio::spawn({
            let stop = Arc::clone(&stop);
            async move {
                let mut longest = Duration::ZERO;
                let mut last = tokio::time::Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let now = tokio::time::Instant::now();
                    longest = longest.max(now.saturating_duration_since(last));
                    last = now;
                }
                longest
            }
        });
        // The ticker takes its first reading before `work` starts.
        tokio::task::yield_now().await;
        let output = work.await;
        stop.store(true, Ordering::Relaxed);
        (output, ticker.await.expect("the ticker joins"))
    }

    /// librdkafka's constructor blocks while it reads the CA file, so the
    /// build must run off the runtime: a 10 ms timer on the same
    /// current-thread runtime fires while the constructor is held on the
    /// FIFO for 300 ms.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn a_producer_build_blocked_in_librdkafka_leaves_the_runtime_free() {
        let (client, ca) = client_whose_producer_build_stalls(Duration::from_millis(300)).await;
        let started = tokio::time::Instant::now();
        let tick = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            started.elapsed()
        };
        let (built, ticked) = tokio::join!(client.producer(), tick);
        ca.finish();

        let free = ticked < Duration::from_millis(150);
        assert!(free, "a 10 ms timer waited {ticked:?}");
        let err = built.err().expect("the FIFO bytes are not a certificate");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
    }

    /// `shutdown` waits for a producer build in flight instead of returning
    /// while librdkafka is still inside its constructor: with the build held
    /// on the FIFO for 1 s, a shutdown that only slept its 500 ms grace
    /// would return first. Nothing is built in the end, because the FIFO
    /// bytes are not a certificate.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn shutdown_waits_for_a_producer_build_in_flight() {
        let (client, ca) = client_whose_producer_build_stalls(Duration::from_secs(1)).await;
        let started = tokio::time::Instant::now();
        let build = async { client.producer().await.err() };
        let close = async {
            // Let the build take the cell first, so the shutdown finds it in
            // flight rather than an empty cell.
            tokio::time::sleep(Duration::from_millis(50)).await;
            client.shutdown().await;
            started.elapsed()
        };
        let (build_err, closed_after) = tokio::join!(build, close);
        ca.finish();

        let waited = closed_after >= Duration::from_millis(900);
        assert!(waited, "shutdown returned at {closed_after:?}");
        let refused = matches!(build_err, Some(ShoveError::Topology(_)));
        assert!(refused, "got {build_err:?}");
        assert!(
            client.producer.built().is_none(),
            "a failed build left a producer behind"
        );
    }

    /// A build that never returns cannot hold a shutdown: with the build held
    /// on the FIFO for 8 s, `shutdown` stops waiting for it after its own
    /// bound and returns long before the build does. The writer is released
    /// afterwards so the test ends.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn shutdown_does_not_wait_forever_for_a_stuck_producer_build() {
        let (client, ca) = client_whose_producer_build_stalls(Duration::from_secs(8)).await;
        let started = tokio::time::Instant::now();
        let build = async { client.producer().await.err() };
        let close = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            client.shutdown().await;
            let closed_after = started.elapsed();
            ca.release();
            closed_after
        };
        let (build_err, closed_after) = tokio::join!(build, close);
        ca.finish();

        let bounded = closed_after < Duration::from_secs(5);
        assert!(
            bounded,
            "shutdown returned at {closed_after:?}, with the build still held"
        );
        assert!(
            matches!(build_err, Some(ShoveError::Topology(_))),
            "got {build_err:?}"
        );
    }

    /// Creating an admin client runs librdkafka's constructor, which blocks
    /// while it reads the CA file, so it must not run on the runtime thread:
    /// the ticker keeps ticking while the constructor is held for 400 ms.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn creating_an_admin_client_leaves_the_runtime_free() {
        let ca = StalledCa::new("admin", Duration::from_millis(400));
        let mut client = client().await;
        client.base_config = ca.applied_to(&client.base_config);

        let (created, longest) = longest_stall(client.create_admin_default()).await;
        ca.finish();

        let err = created.expect_err("the FIFO bytes are not a certificate");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            longest < Duration::from_millis(200),
            "the runtime stalled for {longest:?} while the admin client was created"
        );
    }

    /// `connect` builds the metadata consumer, so it must not hold the
    /// runtime thread while librdkafka reads the CA file.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn connect_builds_the_metadata_consumer_off_the_runtime() {
        let ca = StalledCa::new("connect", Duration::from_millis(400));
        let config = KafkaConfig::new("127.0.0.1:1").with_tls(KafkaTls {
            ca_location: Some(ca.path.clone().into()),
            ..KafkaTls::default()
        });

        let (connected, longest) = longest_stall(KafkaClient::connect(&config)).await;
        ca.finish();

        let err = connected
            .err()
            .expect("the FIFO bytes are not a certificate");
        assert!(
            err.to_string()
                .contains("failed to create metadata consumer"),
            "{err}"
        );
        assert!(
            longest < Duration::from_millis(200),
            "the runtime stalled for {longest:?} while connect built the metadata consumer"
        );
    }

    /// The autoscaler's stats consumer is created on the first poll of a
    /// group, inside an async fn, so its constructor must run off the
    /// runtime thread as well.
    #[cfg(all(feature = "kafka-ssl", unix))]
    #[tokio::test]
    async fn creating_the_stats_consumer_leaves_the_runtime_free() {
        use super::super::autoscaler::{KafkaLagStatsProvider, KafkaQueueStatsProvider};
        use super::super::consumer_group::KafkaAutoOffsetReset;

        let ca = StalledCa::new("stats", Duration::from_millis(400));
        let mut client = client().await;
        client.base_config = ca.applied_to(&client.base_config);
        let provider = KafkaLagStatsProvider::new(client);

        let (stats, longest) = longest_stall(provider.get_queue_stats(
            "orders",
            "orders-consumer",
            KafkaAutoOffsetReset::Earliest,
        ))
        .await;
        ca.finish();

        let err = stats.expect_err("the FIFO bytes are not a certificate");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            longest < Duration::from_millis(200),
            "the runtime stalled for {longest:?} while the stats consumer was created"
        );
    }

    // -- a sequenced topic keeps the partition count it has --

    /// The fields of every WARN event `f` emits, one `name=value` list per
    /// event.
    fn warnings(f: impl FnOnce()) -> Vec<String> {
        use std::fmt::Debug;
        use std::sync::Mutex;
        use tracing::field::{Field, Visit};
        use tracing::subscriber::with_default;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

        #[derive(Clone, Default)]
        struct Warned(Arc<Mutex<Vec<String>>>);

        struct Fields(Vec<String>);

        impl Visit for Fields {
            fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
                self.0.push(format!("{}={value:?}", field.name()));
            }
        }

        impl<S: tracing::Subscriber> Layer<S> for Warned {
            fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
                if *event.metadata().level() == tracing::Level::WARN {
                    let mut fields = Fields(Vec::new());
                    event.record(&mut fields);
                    self.0.lock().unwrap().push(fields.0.join(" "));
                }
            }
        }

        let warned = Warned::default();
        with_default(tracing_subscriber::registry().with(warned.clone()), f);
        warned.0.lock().unwrap().clone()
    }

    /// Fewer partitions than routing shards is refused, and the message
    /// carries both counts.
    #[test]
    fn a_kept_topic_with_fewer_partitions_than_required_is_refused() {
        let err =
            check_kept_partitions("orders", 2, 4).expect_err("two partitions for four shards");

        let ShoveError::Topology(message) = &err else {
            panic!("expected Topology, got {err:?}");
        };
        assert!(message.contains("2 partitions"), "{message}");
        assert!(message.contains("routing_shards (4)"), "{message}");
    }

    #[test]
    fn a_kept_topic_with_exactly_the_required_partitions_passes_silently() {
        let warned = warnings(|| {
            check_kept_partitions("orders", 4, 4).expect("the counts agree");
        });

        assert!(warned.is_empty(), "{warned:?}");
    }

    /// More partitions than shards is accepted and left alone, with a
    /// warning that names the topic and both counts.
    #[test]
    fn a_kept_topic_with_more_partitions_than_required_passes_with_a_warning() {
        let warned = warnings(|| {
            check_kept_partitions("orders", 6, 4).expect("more partitions remap nothing");
        });

        assert_eq!(warned.len(), 1, "{warned:?}");
        for field in ["topic=\"orders\"", "partitions=6", "routing_shards=4"] {
            assert!(warned[0].contains(field), "{field} missing from {warned:?}");
        }
    }

    // -- ensure_partitions answers from the topic's own metadata --

    /// A mock cluster with `topic` at four partitions and a client over it.
    async fn client_on_a_mock_with(
        topic: Option<&str>,
    ) -> (MockCluster<'static, DefaultProducerContext>, KafkaClient) {
        let mock = MockCluster::new(1).expect("mock cluster");
        if let Some(topic) = topic {
            mock.create_topic(topic, 4, 1).expect("mock topic");
        }
        let client = KafkaClient::connect(&KafkaConfig::new(mock.bootstrap_servers()))
            .await
            .expect("connect to the mock cluster");
        (mock, client)
    }

    /// A topic-level metadata error is not an answer about partitions: the
    /// mock still lists the four partitions beside it, and trusting them
    /// would report a topic the principal may not even describe as ready.
    #[tokio::test]
    async fn ensure_partitions_reports_a_topic_metadata_error() {
        let (mock, client) = client_on_a_mock_with(Some("orders")).await;
        mock.topic_error(
            "orders",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )
        .expect("inject the topic error");
        let admin = client.create_admin_default().await.expect("admin client");

        let err = client
            .ensure_partitions(&admin, "orders", 4)
            .await
            .expect_err("an errored topic answers nothing");

        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            err.to_string().contains("TOPIC_AUTHORIZATION_FAILED"),
            "{err}"
        );
    }

    /// Leader election trails a topic's creation, and a declarer that lost
    /// the creation race reads the topic in that window: LEADER_NOT_AVAILABLE
    /// is read again until it clears, so the answer comes after the error does.
    #[tokio::test]
    async fn ensure_partitions_waits_out_a_leader_not_available_answer() {
        const CLEARS_AFTER: Duration = Duration::from_millis(1500);
        let (mock, client) = client_on_a_mock_with(Some("orders")).await;
        mock.topic_error(
            "orders",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE,
        )
        .expect("inject the topic error");
        let admin = client.create_admin_default().await.expect("admin client");

        let started = tokio::time::Instant::now();
        let ensure = async {
            let settled = client.ensure_partitions(&admin, "orders", 4).await;
            (settled, started.elapsed())
        };
        let ((settled, waited), ()) = tokio::join!(ensure, async {
            tokio::time::sleep(CLEARS_AFTER).await;
            mock.topic_error("orders", RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR)
                .expect("clear the topic error");
        });

        settled.expect("a topic whose leaders are elected has its partitions");
        assert!(
            waited >= CLEARS_AFTER,
            "answered at {waited:?}, before the error cleared"
        );
    }

    /// `CreateTopics` said the topic exists, yet the broker's metadata does
    /// not list it for a while: UNKNOWN_TOPIC_OR_PART is retried.
    #[tokio::test]
    async fn ensure_partitions_waits_for_a_topic_that_metadata_does_not_list_yet() {
        const LISTED_AFTER: Duration = Duration::from_millis(1200);
        let (mock, client) = client_on_a_mock_with(None).await;
        let admin = client.create_admin_default().await.expect("admin client");

        let (settled, ()) = tokio::join!(client.ensure_partitions(&admin, "orders", 4), async {
            tokio::time::sleep(LISTED_AFTER).await;
            mock.create_topic("orders", 4, 1).expect("create the topic");
        });

        settled.expect("a topic that shows up is ready");
    }

    // -- the bounded read of a topic's partition count --

    /// A probe that answers from `script`, one answer per read, and the count
    /// of reads it has been asked for. A read past the end of the script
    /// panics, which fails a test whose reads are not bounded.
    fn scripted(
        script: Vec<TopicProbe>,
    ) -> (impl FnMut() -> Ready<Result<TopicProbe>>, Rc<Cell<u32>>) {
        let reads = Rc::new(Cell::new(0));
        let counted = Rc::clone(&reads);
        let mut script: VecDeque<TopicProbe> = script.into();
        let probe = move || {
            counted.set(counted.get() + 1);
            ready(Ok(script.pop_front().expect("a read past the script")))
        };
        (probe, reads)
    }

    /// The broker that answers a metadata read can still report the count from
    /// before an expansion the controller has accepted: reads continue until
    /// the target count shows.
    #[tokio::test(start_paused = true)]
    async fn partition_count_behind_the_target_is_read_again_until_it_catches_up() {
        let (probe, reads) = scripted(vec![
            TopicProbe::Present(2),
            TopicProbe::Present(2),
            TopicProbe::Present(4),
        ]);

        let count = read_partition_count("orders", 4, probe)
            .await
            .expect("the third read shows the new count");

        assert_eq!((count, reads.get()), (4, 3));
    }

    /// A count that never catches up ends the reads at their bound and is
    /// returned for the caller to judge.
    #[tokio::test(start_paused = true)]
    async fn partition_count_that_stays_behind_the_target_is_returned_after_the_bounded_reads() {
        let (probe, reads) = scripted(
            (0..PARTITION_METADATA_ATTEMPTS)
                .map(|_| TopicProbe::Present(2))
                .collect(),
        );

        let count = read_partition_count("orders", 4, probe)
            .await
            .expect("a count that stays behind is still an answer");

        assert_eq!((count, reads.get()), (2, PARTITION_METADATA_ATTEMPTS));
    }

    /// A missing topic, an election in progress and a stale count are all
    /// transient, and one run of reads waits out the three of them.
    #[tokio::test(start_paused = true)]
    async fn partition_count_reads_wait_out_stale_and_transient_answers_together() {
        let (probe, reads) = scripted(vec![
            TopicProbe::Missing,
            TopicProbe::Failed(RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE),
            TopicProbe::Present(2),
            TopicProbe::Present(4),
        ]);

        let count = read_partition_count("orders", 4, probe)
            .await
            .expect("the fourth read shows the new count");

        assert_eq!((count, reads.get()), (4, 4));
    }

    /// Any other topic-level error is final: one read, then the error.
    #[tokio::test(start_paused = true)]
    async fn partition_count_with_a_final_topic_error_ends_the_reads_at_once() {
        let (probe, reads) = scripted(vec![TopicProbe::Failed(
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )]);

        let err = read_partition_count("orders", 4, probe)
            .await
            .expect_err("an authorization error is final");

        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            err.to_string().contains("TOPIC_AUTHORIZATION_FAILED"),
            "{err}"
        );
        assert_eq!(reads.get(), 1);
    }

    /// With no target the first count is the answer, as for a caller that
    /// only wants to know how many partitions the topic has.
    #[tokio::test(start_paused = true)]
    async fn partition_count_with_no_target_answers_on_the_first_read() {
        let (probe, reads) = scripted(vec![TopicProbe::Present(2)]);

        let count = read_partition_count("orders", 0, probe)
            .await
            .expect("the first read answers");

        assert_eq!((count, reads.get()), (2, 1));
    }

    /// A topic that never shows up fails after the bounded reads, naming what
    /// the last read showed.
    #[tokio::test(start_paused = true)]
    async fn partition_count_of_a_topic_that_never_shows_up_fails_after_the_bounded_reads() {
        let (probe, reads) = scripted(
            (0..PARTITION_METADATA_ATTEMPTS)
                .map(|_| TopicProbe::Missing)
                .collect(),
        );

        let err = read_partition_count("orders", 4, probe)
            .await
            .expect_err("a missing topic is not a count");

        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(err.to_string().contains("no such topic"), "{err}");
        assert_eq!(reads.get(), PARTITION_METADATA_ATTEMPTS);
    }

    // -- the admin OAUTHBEARER token error carries one prefix --

    /// A context whose token callback fails with the typed error the MSK IAM
    /// signer returns.
    #[cfg(feature = "kafka-msk-iam")]
    struct SigningFails;

    #[cfg(feature = "kafka-msk-iam")]
    impl ClientContext for SigningFails {
        fn generate_oauth_token(
            &self,
            _principal_name: Option<&str>,
        ) -> std::result::Result<OAuthToken, Box<dyn std::error::Error>> {
            Err(Box::new(ShoveError::Connection(
                "failed to sign an MSK IAM token: no credentials".into(),
            )))
        }
    }

    /// The signer's `Connection` error is unwrapped before the admin path
    /// wraps it, so the message reads "connection error: ..." once.
    #[cfg(feature = "kafka-msk-iam")]
    #[tokio::test]
    async fn a_failed_admin_token_error_carries_one_connection_prefix() {
        let admin: AdminClient<SigningFails> = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .create_with_context(SigningFails)
            .expect("an admin client over a context");

        let err = prime_admin_oauth_token(&admin, SigningFails)
            .await
            .expect_err("the signer fails");

        let shown = err.to_string();
        assert!(matches!(err, ShoveError::Connection(_)), "got {err:?}");
        assert_eq!(shown.matches("connection error").count(), 1, "{shown}");
        assert!(
            shown.contains("failed to generate OAUTHBEARER token for admin client"),
            "{shown}"
        );
        assert!(
            shown.contains("failed to sign an MSK IAM token: no credentials"),
            "{shown}"
        );
    }
}
