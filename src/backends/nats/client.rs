use std::fmt;
use std::process;
use std::time::Duration;

use async_nats::jetstream;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::retry::Backoff;
use crate::ShoveError;

pub struct NatsConfig {
    pub url: String,
}

impl NatsConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact credentials: nats://user:pass@host → nats://***@host
        let redacted = if let Some(at_pos) = self.url.find('@') {
            if let Some(scheme_end) = self.url.find("://") {
                format!("{}://***@{}", &self.url[..scheme_end], &self.url[at_pos + 1..])
            } else {
                "***".to_string()
            }
        } else {
            self.url.clone()
        };
        f.debug_struct("NatsConfig")
            .field("url", &redacted)
            .finish()
    }
}

#[derive(Clone)]
pub struct NatsClient {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    shutdown_token: CancellationToken,
}

const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

impl NatsClient {
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        let client_name = format!("shove-rs-{}", process::id());
        let client = async_nats::ConnectOptions::new()
            .name(client_name)
            .connect(&config.url)
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        let jetstream = jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            shutdown_token: CancellationToken::new(),
        })
    }

    pub async fn connect_with_retry(config: &NatsConfig, max_attempts: u32) -> Result<Self> {
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
                        "NATS connection failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub fn jetstream(&self) -> &jetstream::Context {
        &self.jetstream
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub fn is_connected(&self) -> bool {
        matches!(
            self.client.connection_state(),
            async_nats::connection::State::Connected
        )
    }

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        let _ = self.client.drain().await;
    }
}