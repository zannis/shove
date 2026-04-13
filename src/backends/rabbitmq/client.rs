use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use lapin::options::ConfirmSelectOptions;
use lapin::{Channel, Connection, ConnectionProperties};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::SHUTDOWN_GRACE;
use crate::backends::rabbitmq::map_lapin_error;
use crate::error::{Result, ShoveError};

/// RabbitMQ connection configuration.
#[derive(Clone)]
pub struct RabbitMqConfig {
    /// AMQP connection string (e.g., "amqp://guest:guest@localhost:5672/%2f")
    pub uri: String,
}

impl Debug for RabbitMqConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let redacted_uri = if let Some(at_idx) = self.uri.find('@') {
            if let Some(proto_idx) = self.uri.find("://") {
                let prefix = &self.uri[..proto_idx + 3];
                let creds = &self.uri[proto_idx + 3..at_idx];
                let suffix = &self.uri[at_idx..];

                if let Some(colon_idx) = creds.find(':') {
                    let user = &creds[..colon_idx];
                    format!("{prefix}{user}:<redacted>{suffix}")
                } else {
                    format!("{prefix}<redacted>{suffix}")
                }
            } else {
                "<redacted>".to_string()
            }
        } else {
            self.uri.clone()
        };

        f.debug_struct("RabbitMqConfig")
            .field("uri", &redacted_uri)
            .finish()
    }
}

impl RabbitMqConfig {
    pub fn new(uri: impl Into<String>) -> Self {
        Self { uri: uri.into() }
    }
}

/// RabbitMQ client with connection management and graceful shutdown.
#[derive(Clone)]
pub struct RabbitMqClient {
    connection: Arc<Connection>,
    shutdown_token: CancellationToken,
}

impl RabbitMqClient {
    /// Establish a connection to RabbitMQ using the provided configuration.
    ///
    /// The connection is named `shove-rs-{pid}` and a fresh [`CancellationToken`]
    /// is created to coordinate shutdown across clones of this client.
    pub async fn connect(config: &RabbitMqConfig) -> Result<Self> {
        let pid = std::process::id();
        let connection_name = format!("shove-rs-{pid}");

        let properties =
            ConnectionProperties::default().with_connection_name(connection_name.into());

        let connection = Connection::connect(&config.uri, properties)
            .await
            .map_err(|e| map_lapin_error("failed to connect to RabbitMQ", e))?;

        Ok(Self {
            connection: Arc::new(connection),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Like [`connect`](Self::connect), but retries up to `max_attempts` times
    /// with exponential backoff on connection failure.
    ///
    /// Useful for services that start alongside their broker (e.g. in Docker
    /// Compose or Kubernetes) where the broker may not be ready immediately.
    pub async fn connect_with_retry(config: &RabbitMqConfig, max_attempts: u32) -> Result<Self> {
        let mut backoff = crate::retry::Backoff::default();
        let mut last_err = None;

        for attempt in 0..max_attempts {
            match Self::connect(config).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if attempt + 1 < max_attempts {
                        let delay = backoff.next().expect("backoff is infinite");
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_attempts,
                            error = %e,
                            "RabbitMQ connection failed, retrying in {delay:?}"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.expect("loop ran at least once"))
    }

    /// Open a basic channel on the underlying connection.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested
    /// or if the channel cannot be created.
    pub async fn create_channel(&self) -> Result<Channel> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot create channel: client is shutting down".into(),
            ));
        }

        self.connection
            .create_channel()
            .await
            .map_err(|e| map_lapin_error("failed to create channel", e))
    }

    /// Open a channel with publisher confirms enabled.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested,
    /// if the channel cannot be created, or if confirms cannot be enabled.
    pub async fn create_confirm_channel(&self) -> Result<Channel> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot create confirm channel: client is shutting down".into(),
            ));
        }

        let channel = self
            .connection
            .create_channel()
            .await
            .map_err(|e| map_lapin_error("failed to create confirm channel", e))?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| map_lapin_error("failed to enable publisher confirms", e))?;

        Ok(channel)
    }

    /// Open a channel with AMQP transaction mode enabled (`tx_select`).
    ///
    /// Used by consumers with [`ConsumerOptions::with_exactly_once`] to make
    /// publish-to-hold-queue and ack/nack of the original delivery atomic.
    /// Transaction mode is mutually exclusive with publisher confirms — do not
    /// mix with [`create_confirm_channel`](Self::create_confirm_channel) on the
    /// same connection.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested,
    /// if the channel cannot be created, or if `tx_select` cannot be enabled.
    #[cfg(feature = "rabbitmq-transactional")]
    pub async fn create_tx_channel(&self) -> Result<Channel> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot create tx channel: client is shutting down".into(),
            ));
        }

        let channel = self
            .connection
            .create_channel()
            .await
            .map_err(|e| map_lapin_error("failed to create tx channel", e))?;

        channel
            .tx_select()
            .await
            .map_err(|e| map_lapin_error("failed to enable tx mode", e))?;

        Ok(channel)
    }

    /// Return a clone of the shutdown [`CancellationToken`].
    ///
    /// Callers can use this token to coordinate their own teardown with the
    /// client's shutdown sequence.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Return `true` if the underlying AMQP connection is still open.
    pub fn is_connected(&self) -> bool {
        self.connection.status().connected()
    }

    /// Initiate a graceful shutdown.
    ///
    /// Cancels the shutdown token so that dependent tasks can begin winding
    /// down, waits for [`SHUTDOWN_GRACE`] to allow in-flight operations to
    /// complete, and then closes the underlying AMQP connection.
    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        sleep(SHUTDOWN_GRACE).await;

        if let Err(e) = self.connection.close(0, "shutdown".into()).await {
            tracing::warn!("error while closing RabbitMQ connection: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_debug_redacts_password_only() {
        let config = RabbitMqConfig::new("amqp://admin:s3cret!@localhost:5672/%2F");
        let debug_output = format!("{config:?}");
        assert!(!debug_output.contains("s3cret!"));
        assert!(debug_output.contains("amqp://admin:<redacted>@localhost:5672/%2F"));
    }

    #[test]
    fn config_debug_no_creds_remains_clear() {
        let config = RabbitMqConfig::new("amqp://localhost:5672/%2F");
        let debug_output = format!("{config:?}");
        assert!(debug_output.contains("amqp://localhost:5672/%2F"));
    }

    #[test]
    fn config_new_stores_uri() {
        let config = RabbitMqConfig::new("amqp://host:1234/%2F");
        assert_eq!(config.uri, "amqp://host:1234/%2F");
    }
}
