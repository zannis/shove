use std::fmt;
use std::process;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::producer::{FutureProducer, Producer};
use tokio_util::sync::CancellationToken;

use crate::ShoveError;
use crate::error::Result;
use crate::retry::Backoff;

pub struct KafkaConfig {
    pub brokers: String,
}

impl KafkaConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
        }
    }
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .finish()
    }
}

const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct KafkaClient {
    brokers: String,
    producer: FutureProducer,
    shutdown_token: CancellationToken,
}

impl KafkaClient {
    pub async fn connect(config: &KafkaConfig) -> Result<Self> {
        let client_name = format!("shove-rs-{}", process::id());

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("client.id", &client_name)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .create()
            .map_err(|e| ShoveError::Connection(format!("failed to create Kafka producer: {e}")))?;

        Ok(Self {
            brokers: config.brokers.clone(),
            producer,
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
                    let delay = backoff.next().unwrap_or(Duration::from_secs(5));
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

    pub fn producer(&self) -> &FutureProducer {
        &self.producer
    }

    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub(super) async fn create_admin(&self) -> Result<AdminClient<DefaultClientContext>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .map_err(|e| ShoveError::Connection(format!("failed to create admin client: {e}")))?;
        Ok(admin)
    }

    /// Create a topic (idempotent — ignores "already exists").
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
                        tracing::debug!(topic, "topic already exists, skipping");
                    } else {
                        return Err(ShoveError::Topology(format!(
                            "failed to create topic {topic}: {code:?}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        self.producer.flush(Duration::from_secs(5)).ok();
    }
}
