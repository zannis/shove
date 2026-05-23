use std::fmt;
#[cfg(feature = "kafka-ssl")]
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, Producer};

use super::publisher::publish_with_retry as publisher_publish_with_retry;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "kafka-msk-iam")]
use std::sync::Arc;

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::{MskIamContext, MskIamTokenProvider};

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

    /// AWS MSK IAM authentication. Token is refreshed automatically by
    /// `MskIamTokenProvider` ahead of expiry (~15 min cadence).
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

pub struct KafkaConfig {
    pub brokers: String,
    #[cfg(feature = "kafka-ssl")]
    pub tls: Option<KafkaTls>,
    #[cfg(feature = "kafka-ssl")]
    pub sasl: Option<KafkaSasl>,
}

impl KafkaConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            #[cfg(feature = "kafka-ssl")]
            tls: None,
            #[cfg(feature = "kafka-ssl")]
            sasl: None,
        }
    }

    /// Bootstrap brokers string this config was built with.
    pub fn brokers(&self) -> &str {
        &self.brokers
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

const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct KafkaClient {
    brokers: String,
    /// Pre-populated ClientConfig containing `bootstrap.servers` plus any
    /// TLS/SASL settings. Every consumer/admin/metadata call clones this
    /// so security settings never have to be re-applied at call sites.
    base_config: ClientConfig,
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
            if config.sasl.is_some() && config.tls.is_none() {
                tracing::warn!(
                    "Kafka SASL enabled without TLS; credentials will be sent in plaintext over the network"
                );
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
        #[cfg(feature = "kafka-msk-iam")]
        let producer = if let Some(ctx) = msk_context.clone() {
            let p: FutureProducer<MskIamContext> = base_config
                .clone()
                .set("client.id", &client_name)
                .set("message.timeout.ms", "5000")
                .set("acks", "all")
                .create_with_context(ctx)
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create MSK IAM producer: {e}"))
                })?;
            KafkaProducerInner::MskIam(p)
        } else {
            let p: FutureProducer<DefaultClientContext> = base_config
                .clone()
                .set("client.id", &client_name)
                .set("message.timeout.ms", "5000")
                .set("acks", "all")
                .create()
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create Kafka producer: {e}"))
                })?;
            KafkaProducerInner::Default(p)
        };

        #[cfg(not(feature = "kafka-msk-iam"))]
        let producer = {
            let p: FutureProducer<DefaultClientContext> = base_config
                .clone()
                .set("client.id", &client_name)
                .set("message.timeout.ms", "5000")
                .set("acks", "all")
                .create()
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create Kafka producer: {e}"))
                })?;
            KafkaProducerInner::Default(p)
        };

        Ok(Self {
            brokers: config.brokers.clone(),
            base_config,
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
    pub fn base_config(&self) -> ClientConfig {
        self.base_config.clone()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub(super) async fn create_admin(&self) -> Result<AdminClient<DefaultClientContext>> {
        let admin: AdminClient<DefaultClientContext> = self
            .base_config
            .clone()
            .create()
            .map_err(|e| ShoveError::Topology(format!("failed to create admin client: {e}")))?;
        Ok(admin)
    }

    /// Create a topic, or expand its partition count if it already exists
    /// with fewer partitions than requested.
    pub(super) async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
    ) -> Result<()> {
        let admin = self.create_admin().await?;
        let new_topic = NewTopic::new(
            name,
            num_partitions,
            TopicReplication::Fixed(replication_factor),
        );
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
                        self.ensure_partitions(&admin, name, num_partitions).await?;
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
    async fn ensure_partitions(
        &self,
        admin: &AdminClient<DefaultClientContext>,
        name: &str,
        desired: i32,
    ) -> Result<()> {
        use rdkafka::admin::NewPartitions;

        // Fetch current partition count from metadata.
        let base = self.base_config.clone();
        let topic_name = name.to_string();
        let current = tokio::task::spawn_blocking(move || -> Result<i32> {
            use rdkafka::consumer::{BaseConsumer, Consumer as _};
            let consumer: BaseConsumer = base
                .clone()
                .set("group.id", "shove-partition-check")
                .create()
                .map_err(|e| {
                    ShoveError::Topology(format!("failed to create metadata consumer: {e}"))
                })?;
            let md = consumer
                .fetch_metadata(Some(&topic_name), Duration::from_secs(10))
                .map_err(|e| {
                    ShoveError::Connection(format!(
                        "failed to fetch metadata for {topic_name}: {e}"
                    ))
                })?;
            let topic = md.topics().first().ok_or_else(|| {
                ShoveError::Topology(format!("no metadata for topic {topic_name}"))
            })?;
            Ok(topic.partitions().len() as i32)
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
}
