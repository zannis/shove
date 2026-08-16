use std::collections::BTreeMap;
use std::fmt;
#[cfg(feature = "kafka-ssl")]
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, Producer};

use super::constants::{MESSAGE_TIMEOUT_MS, SHUTDOWN_GRACE};
use super::publisher::publish_with_retry as publisher_publish_with_retry;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::{MskIamContext, MskIamTokenProvider};
#[cfg(feature = "kafka-msk-iam")]
use rdkafka::bindings::{
    rd_kafka_oauthbearer_set_token, rd_kafka_oauthbearer_set_token_failure, rd_kafka_t,
};
#[cfg(feature = "kafka-msk-iam")]
use rdkafka::client::OAuthToken;
#[cfg(feature = "kafka-msk-iam")]
use rdkafka::types::RDKafkaRespErr;

use crate::ShoveError;
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

#[must_use]
/// Optional librdkafka **producer** performance knobs.
///
/// shove pins the correctness-critical producer settings (`acks=all`,
/// `enable.idempotence=true`) and deliberately keeps them non-configurable.
/// The three knobs here are orthogonal to those: they trade producer-side
/// latency for throughput, and librdkafka's defaults (`linger.ms=5`,
/// no compression) are tuned for low latency rather than for high-rate
/// pipelines.
///
/// Why this matters with idempotence on: `enable.idempotence=true` caps
/// `max.in.flight.requests.per.connection` at 5, so sustained throughput is
/// bounded by *messages per request* — i.e. by batching. With a 5 ms linger a
/// high-rate producer ships many small requests and plateaus well below what
/// the broker can absorb; raising `linger.ms` and enabling compression raises
/// that plateau by multiples without touching delivery semantics.
///
/// All fields default to `None` = "leave librdkafka's default alone", so this
/// is inert unless explicitly configured.
#[derive(Debug, Clone, Default)]
pub struct KafkaProducerTuning {
    compression_type: Option<String>,
    linger_ms: Option<u32>,
    batch_size: Option<u32>,
}

impl KafkaProducerTuning {
    pub fn new() -> Self {
        Self::default()
    }

    /// `compression.type` — e.g. `"lz4"`, `"zstd"`, `"snappy"`, `"gzip"`.
    /// Compresses the batch on the client, so it cuts producer→broker bytes as
    /// well as broker storage and replication traffic.
    pub fn with_compression(mut self, codec: impl Into<String>) -> Self {
        self.compression_type = Some(codec.into());
        self
    }

    /// `linger.ms` — how long the producer waits to accumulate a batch.
    /// librdkafka defaults to 5 ms; higher values trade a little latency for
    /// materially larger (and better-compressed) batches.
    pub fn with_linger_ms(mut self, ms: u32) -> Self {
        self.linger_ms = Some(ms);
        self
    }

    /// `batch.size` — maximum bytes accumulated per batch.
    pub fn with_batch_size(mut self, bytes: u32) -> Self {
        self.batch_size = Some(bytes);
        self
    }

    /// Apply the configured knobs to a producer `ClientConfig`. Unset fields
    /// are left at librdkafka's defaults.
    fn apply(&self, cfg: &mut ClientConfig) {
        if let Some(codec) = &self.compression_type {
            cfg.set("compression.type", codec);
        }
        if let Some(ms) = self.linger_ms {
            cfg.set("linger.ms", ms.to_string());
        }
        if let Some(bytes) = self.batch_size {
            cfg.set("batch.size", bytes.to_string());
        }
    }
}

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
    /// Optional producer throughput knobs; empty = librdkafka defaults.
    pub(crate) producer_tuning: KafkaProducerTuning,
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
            producer_tuning: KafkaProducerTuning::default(),
        }
    }

    /// Bootstrap brokers string this config was built with.
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Set optional producer throughput knobs (compression / linger / batch
    /// size). Correctness settings (`acks=all`, `enable.idempotence=true`) are
    /// unaffected. See [`KafkaProducerTuning`].
    pub fn with_producer_tuning(mut self, tuning: KafkaProducerTuning) -> Self {
        self.producer_tuning = tuning;
        self
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
    producer: KafkaProducerInner,
    #[cfg(feature = "kafka-msk-iam")]
    msk_context: Option<MskIamContext>,
    shutdown_token: CancellationToken,
}

#[derive(Clone)]
enum KafkaProducerInner {
    Default(FutureProducer<DefaultClientContext>),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(FutureProducer<MskIamContext>),
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

        // Phase C — build the producer with whichever context applies.
        // sec-K-10: enable idempotent producer. The publisher.rs retry loop
        // (publish_with_retry) retries on timeouts; without idempotence a
        // timeout-then-retry can produce duplicates at the broker even if
        // the first send actually succeeded. enable.idempotence=true makes
        // the broker dedupe by producer id + sequence, restoring at-least-
        // once into exactly-once-on-the-broker semantics. Requires Kafka
        // ≥ 0.11 (universal today) and caps in-flight requests per
        // connection at 5.
        #[cfg(feature = "kafka-msk-iam")]
        let producer = if let Some(ctx) = msk_context.clone() {
            let p: FutureProducer<MskIamContext> = {
                let mut cfg = base_config.clone();
                cfg.set("client.id", &client_name)
                    .set("message.timeout.ms", MESSAGE_TIMEOUT_MS.to_string())
                    .set("acks", "all")
                    .set("enable.idempotence", "true");
                config.producer_tuning.apply(&mut cfg);
                cfg.create_with_context(ctx)
            }
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create MSK IAM producer: {e}"))
                })?;
            KafkaProducerInner::MskIam(p)
        } else {
            let p: FutureProducer<DefaultClientContext> = {
                let mut cfg = base_config.clone();
                cfg.set("client.id", &client_name)
                    .set("message.timeout.ms", MESSAGE_TIMEOUT_MS.to_string())
                    .set("acks", "all")
                    .set("enable.idempotence", "true");
                config.producer_tuning.apply(&mut cfg);
                cfg.create()
            }
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create Kafka producer: {e}"))
                })?;
            KafkaProducerInner::Default(p)
        };

        #[cfg(not(feature = "kafka-msk-iam"))]
        let producer = {
            let p: FutureProducer<DefaultClientContext> = {
                let mut cfg = base_config.clone();
                cfg.set("client.id", &client_name)
                    .set("message.timeout.ms", MESSAGE_TIMEOUT_MS.to_string())
                    .set("acks", "all")
                    .set("enable.idempotence", "true");
                config.producer_tuning.apply(&mut cfg);
                cfg.create()
            }
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create Kafka producer: {e}"))
                })?;
            KafkaProducerInner::Default(p)
        };

        Ok(Self {
            brokers: config.brokers.clone(),
            base_config: Arc::new(base_config),
            producer,
            #[cfg(feature = "kafka-msk-iam")]
            msk_context,
            shutdown_token: CancellationToken::new(),
        })
    }

    pub async fn connect_with_retry(config: &KafkaConfig, max_attempts: u32) -> Result<Self> {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
        let mut attempts = 0u32;

        loop {
            attempts += 1;
            match Self::connect(config).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = backoff
                        .next()
                        .expect("backoff iterator is infinite; this is a bug");
                    tracing::warn!(
                        attempt = attempts,
                        max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "Kafka connection failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn publish_with_retry(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        headers: OwnedHeaders,
        payload: &[u8],
        max_attempts: u32,
        label: &str,
    ) -> Result<()> {
        match &self.producer {
            KafkaProducerInner::Default(p) => {
                publisher_publish_with_retry(p, topic, key, headers, payload, max_attempts, label)
                    .await
            }
            #[cfg(feature = "kafka-msk-iam")]
            KafkaProducerInner::MskIam(p) => {
                publisher_publish_with_retry(p, topic, key, headers, payload, max_attempts, label)
                    .await
            }
        }
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

    /// Liveness check. Issues a single `fetch_metadata(None, timeout)` against
    /// the cluster via the producer's existing librdkafka client. No new
    /// socket, no consumer-group churn, no side effects.
    ///
    /// Returns `Err(ShoveError::Connection)` if the client is shut down, the
    /// metadata fetch fails, or `spawn_blocking` itself fails.
    pub(super) async fn ping(&self, timeout: Duration) -> Result<()> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection("client is shut down".into()));
        }
        let producer = self.producer.clone();
        let join = tokio::task::spawn_blocking(move || match &producer {
            KafkaProducerInner::Default(p) => p.client().fetch_metadata(None, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            KafkaProducerInner::MskIam(p) => p.client().fetch_metadata(None, timeout),
        });

        let metadata_result = tokio::time::timeout(timeout, join)
            .await
            .map_err(|_| ShoveError::Connection(format!("kafka ping timed out after {timeout:?}")))?
            .map_err(|e| ShoveError::Connection(format!("kafka ping task failed: {e}")))?;

        metadata_result
            .map(|_| ())
            .map_err(|e| ShoveError::Connection(format!("kafka ping failed: {e}")))
    }

    pub(super) async fn create_admin_default(&self) -> Result<AdminClient<DefaultClientContext>> {
        let admin: AdminClient<DefaultClientContext> = self
            .base_config
            .clone()
            .create()
            .map_err(|e| ShoveError::Topology(format!("failed to create admin client: {e}")))?;
        Ok(admin)
    }

    #[cfg(feature = "kafka-msk-iam")]
    pub(super) async fn create_admin_msk(
        &self,
        ctx: MskIamContext,
    ) -> Result<AdminClient<MskIamContext>> {
        let admin: AdminClient<MskIamContext> = self
            .base_config
            .clone()
            .create_with_context(ctx.clone())
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

    /// Create a topic, or bring an existing one up to spec: expand its
    /// partition count if lower than requested, and reconcile the given
    /// topic-level config entries if they drift from the live values.
    pub(super) async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        config: &[(String, String)],
    ) -> Result<()> {
        #[cfg(feature = "kafka-msk-iam")]
        if let Some(ctx) = self.msk_context() {
            let admin = self.create_admin_msk(ctx).await?;
            return self
                .create_topic_with_admin(&admin, name, num_partitions, replication_factor, config)
                .await;
        }
        let admin = self.create_admin_default().await?;
        self.create_topic_with_admin(&admin, name, num_partitions, replication_factor, config)
            .await
    }

    async fn create_topic_with_admin<C>(
        &self,
        admin: &AdminClient<C>,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        config: &[(String, String)],
    ) -> Result<()>
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

        for result in results {
            match result {
                Ok(_) => {}
                Err((topic, code)) => {
                    if code == RDKafkaErrorCode::TopicAlreadyExists {
                        tracing::debug!(topic, "topic already exists, checking partition count");
                        self.ensure_partitions(admin, name, num_partitions).await?;
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

        Ok(())
    }

    /// If the existing topic has fewer partitions than `desired`, expand it.
    async fn ensure_partitions<C>(
        &self,
        admin: &AdminClient<C>,
        name: &str,
        desired: i32,
    ) -> Result<()>
    where
        C: ClientContext + 'static,
    {
        use rdkafka::admin::NewPartitions;

        // Fetch current partition count from metadata. The blocking work is
        // pushed onto a dedicated thread because librdkafka's metadata fetch
        // is synchronous; the helper owns all the cfg-gated context selection.
        let base = (*self.base_config).clone();
        let topic_name = name.to_string();
        #[cfg(feature = "kafka-msk-iam")]
        let msk_ctx = self.msk_context();
        #[cfg(feature = "kafka-msk-iam")]
        let shutdown = self.shutdown_token();
        let current = tokio::task::spawn_blocking(move || {
            fetch_topic_partition_count_blocking(
                base,
                &topic_name,
                #[cfg(feature = "kafka-msk-iam")]
                msk_ctx,
                #[cfg(feature = "kafka-msk-iam")]
                shutdown,
            )
        })
        .await
        .map_err(|e| ShoveError::Topology(format!("metadata task failed: {e}")))??;

        if current >= desired {
            tracing::debug!(
                topic = name,
                current,
                desired,
                "partition count already sufficient"
            );
            return Ok(());
        }

        tracing::info!(topic = name, current, desired, "expanding partition count");
        let new_parts = NewPartitions::new(name, desired as usize);
        let results = admin
            .create_partitions(&[new_parts], &AdminOptions::new())
            .await
            .map_err(|e| {
                ShoveError::Topology(format!("failed to expand partitions for {name}: {e}"))
            })?;

        for result in results {
            if let Err((topic, code)) = result {
                metrics::record_backend_error(
                    metrics::BackendLabel::Kafka,
                    metrics::BackendErrorKind::Topology,
                );
                return Err(ShoveError::Topology(format!(
                    "failed to expand partitions for {topic}: {code:?}"
                )));
            }
        }

        Ok(())
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

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        match &self.producer {
            KafkaProducerInner::Default(p) => {
                p.flush(Duration::from_secs(5)).ok();
            }
            #[cfg(feature = "kafka-msk-iam")]
            KafkaProducerInner::MskIam(p) => {
                p.flush(Duration::from_secs(5)).ok();
            }
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

/// Build a metadata `BaseConsumer` against `base`, fetch the topic's metadata
/// synchronously, and return its partition count. Runs inside `spawn_blocking`
/// because librdkafka's `fetch_metadata` is blocking.
///
/// The cfg branching lives here so `ensure_partitions` stays readable.
fn fetch_topic_partition_count_blocking(
    base: ClientConfig,
    topic_name: &str,
    #[cfg(feature = "kafka-msk-iam")] msk_ctx: Option<MskIamContext>,
    #[cfg(feature = "kafka-msk-iam")] shutdown: CancellationToken,
) -> Result<i32> {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let mut cfg = base;
    // arch-K-10: per-process suffix on the group id so multiple shove
    // processes don't collide in kafka-consumer-groups.sh / Kafka UI /
    // MSK console under a single shared "shove-partition-check" name.
    cfg.set(
        "group.id",
        format!("shove-partition-check-{}", process::id()),
    );

    #[cfg(feature = "kafka-msk-iam")]
    let metadata = if let Some(ctx) = msk_ctx {
        let consumer: BaseConsumer<MskIamContext> = cfg.create_with_context(ctx).map_err(|e| {
            ShoveError::Topology(format!("failed to create MSK metadata consumer: {e}"))
        })?;
        // A one-shot metadata consumer never polls, so nothing would deliver
        // the initial OAUTHBEARER token (rdkafka services it via the event
        // queue, which fetch_metadata does not pump). Pump it on a scoped
        // thread for the duration of the blocking fetch; the thread is joined
        // before this block returns.
        use std::sync::atomic::{AtomicBool, Ordering};
        let done = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                while !done.load(Ordering::Relaxed) && !shutdown.is_cancelled() {
                    let _ = consumer.poll(Duration::from_millis(100));
                }
            });
            let md = consumer.fetch_metadata(Some(topic_name), Duration::from_secs(10));
            done.store(true, Ordering::Relaxed);
            md
        })
    } else {
        let consumer: BaseConsumer = cfg.create().map_err(|e| {
            ShoveError::Topology(format!("failed to create metadata consumer: {e}"))
        })?;
        consumer.fetch_metadata(Some(topic_name), Duration::from_secs(10))
    };

    #[cfg(not(feature = "kafka-msk-iam"))]
    let metadata = {
        let consumer: BaseConsumer = cfg.create().map_err(|e| {
            ShoveError::Topology(format!("failed to create metadata consumer: {e}"))
        })?;
        consumer.fetch_metadata(Some(topic_name), Duration::from_secs(10))
    };

    let md = metadata.map_err(|e| {
        ShoveError::Connection(format!("failed to fetch metadata for {topic_name}: {e}"))
    })?;
    let topic = md
        .topics()
        .first()
        .ok_or_else(|| ShoveError::Topology(format!("no metadata for topic {topic_name}")))?;
    Ok(topic.partitions().len() as i32)
}

/// Generate an OAUTHBEARER token from `ctx` and set it on the admin client's
/// native handle.
///
/// rdkafka services the OAUTHBEARER refresh event by polling the client's main
/// queue; the admin client never polls it, so we generate and set the token
/// ourselves. `generate_oauth_token` drives an async signer via
/// `Handle::block_on`, which panics on a Tokio worker thread, so it runs on a
/// blocking thread (the same contract librdkafka's own C callback thread has).
#[cfg(feature = "kafka-msk-iam")]
async fn prime_admin_oauth_token<C>(admin: &AdminClient<C>, ctx: C) -> Result<()>
where
    C: ClientContext + Send + 'static,
{
    let token = tokio::task::spawn_blocking(move || {
        // The `Box<dyn Error>` from generate_oauth_token is not `Send`; stringify
        // it inside the blocking thread so only a `String` crosses the boundary.
        ctx.generate_oauth_token(None).map_err(|e| e.to_string())
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
/// ([`prime_admin_oauth_token`]) against a caller-supplied admin client and
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
}
