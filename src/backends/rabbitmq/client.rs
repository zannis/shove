use std::sync::Arc;

use lapin::{Channel, Connection, ConnectionProperties};
use tokio_util::sync::CancellationToken;

use crate::SHUTDOWN_GRACE;
use crate::error::ShoveError;

/// RabbitMQ connection configuration.
#[derive(Debug, Clone)]
pub struct RabbitMqConfig {
    /// AMQP connection string (e.g., "amqp://guest:guest@localhost:5672/%2f")
    pub uri: String,
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
    pub async fn connect(config: &RabbitMqConfig) -> Result<Self, ShoveError> {
        let pid = std::process::id();
        let connection_name = format!("shove-rs-{pid}");

        let properties =
            ConnectionProperties::default().with_connection_name(connection_name.into());

        let connection = Connection::connect(&config.uri, properties)
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        Ok(Self {
            connection: Arc::new(connection),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Open a basic channel on the underlying connection.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested
    /// or if the channel cannot be created.
    pub async fn create_channel(&self) -> Result<Channel, ShoveError> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot create channel: client is shutting down".into(),
            ));
        }

        self.connection
            .create_channel()
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))
    }

    /// Open a channel with publisher confirms enabled.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested,
    /// if the channel cannot be created, or if confirms cannot be enabled.
    pub async fn create_confirm_channel(&self) -> Result<Channel, ShoveError> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot create confirm channel: client is shutting down".into(),
            ));
        }

        let channel = self
            .connection
            .create_channel()
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        channel
            .confirm_select(lapin::options::ConfirmSelectOptions::default())
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

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
        tokio::time::sleep(SHUTDOWN_GRACE).await;

        if let Err(e) = self.connection.close(0, "shutdown".into()).await {
            tracing::warn!("error while closing RabbitMQ connection: {e}");
        }
    }
}
