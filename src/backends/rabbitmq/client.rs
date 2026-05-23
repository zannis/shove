use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;

use lapin::options::ConfirmSelectOptions;
use lapin::{Channel, Connection, ConnectionProperties};
use tokio::time::{Duration, sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::SHUTDOWN_GRACE;
use crate::backends::rabbitmq::map_lapin_error;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::retry::Backoff;

/// RabbitMQ connection configuration.
#[derive(Clone)]
pub struct RabbitMqConfig {
    /// AMQP connection string (e.g., "amqp://guest:guest@localhost:5672/%2f")
    pub uri: String,
}

impl Debug for RabbitMqConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let redacted_uri = if let Ok(mut url) = url::Url::parse(&self.uri) {
            url.set_password(None).ok();
            url.to_string()
        } else {
            "<unparseable>".to_string()
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

    /// AMQP connection URI this config was built with.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl Default for RabbitMqConfig {
    /// Default RabbitMQ endpoint for local development.
    fn default() -> Self {
        Self::new("amqp://guest:guest@localhost:5672")
    }
}

/// Returns true for lapin errors that indicate the underlying AMQP connection
/// is permanently dead and the only recovery is to dial a new one. Matches
/// hard AMQP errors (codes 3xx/5xx that close the connection, such as
/// `CONNECTION-FORCED`, `FRAME-ERROR`, `COMMAND-INVALID`) as well as TCP/IO
/// failures and heartbeat timeouts. Channel-level soft errors (closed channel,
/// channels limit) are excluded — those don't mean the connection itself is
/// gone.
fn is_connection_dead(e: &lapin::Error) -> bool {
    e.is_amqp_hard_error()
        || matches!(
            e.kind(),
            lapin::ErrorKind::InvalidConnectionState(_)
                | lapin::ErrorKind::IOError(_)
                | lapin::ErrorKind::MissingHeartbeatError
        )
}

/// RabbitMQ client with connection management and graceful shutdown.
///
/// Internally wraps an `ArcSwap<Connection>` so the underlying AMQP connection
/// can be replaced after a broker disconnect without invalidating outstanding
/// `RabbitMqClient` clones, publishers, or consumers.
#[derive(Clone)]
pub struct RabbitMqClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    /// Current connection. Read via `load_full()` to snapshot, replaced via
    /// `store()` after a successful reconnect.
    connection: arc_swap::ArcSwap<Connection>,
    /// Stored so `reconnect()` can dial a fresh `Connection` with the same URI.
    config: RabbitMqConfig,
    /// Single-flight guard so concurrent failures don't dial-storm the broker.
    /// Held across the async dial.
    reconnect_lock: tokio::sync::Mutex<()>,
    shutdown_token: CancellationToken,
}

impl RabbitMqClient {
    /// Establish a connection to RabbitMQ using the provided configuration.
    ///
    /// The connection is named `shove-rs-{pid}` and a fresh [`CancellationToken`]
    /// is created to coordinate shutdown across clones of this client.
    pub async fn connect(config: &RabbitMqConfig) -> Result<Self> {
        let connection = Self::dial(config).await?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                connection: arc_swap::ArcSwap::from_pointee(connection),
                config: config.clone(),
                reconnect_lock: tokio::sync::Mutex::new(()),
                shutdown_token: CancellationToken::new(),
            }),
        })
    }

    /// Like [`connect`](Self::connect), but retries up to `max_attempts` times
    /// with exponential backoff on connection failure.
    ///
    /// Useful for services that start alongside their broker (e.g. in Docker
    /// Compose or Kubernetes) where the broker may not be ready immediately.
    pub async fn connect_with_retry(config: &RabbitMqConfig, max_attempts: u32) -> Result<Self> {
        let mut backoff = Backoff::default();
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

    /// Dial a fresh `Connection` using the stored config. Used by `connect`
    /// and by `reconnect` after a broker disconnect.
    ///
    /// Times out after 5 seconds so that a broker that accepts the TCP
    /// connection but never completes the AMQP handshake (e.g. during
    /// `stop_app`) does not block the caller indefinitely.
    async fn dial(config: &RabbitMqConfig) -> Result<Connection> {
        let pid = std::process::id();
        let connection_name = format!("shove-rs-{pid}");

        let properties =
            ConnectionProperties::default().with_connection_name(connection_name.into());

        timeout(
            Duration::from_secs(5),
            Connection::connect(&config.uri, properties),
        )
        .await
        .map_err(|_| ShoveError::Connection("timed out connecting to RabbitMQ".into()))?
        .map_err(|e| map_lapin_error("failed to connect to RabbitMQ", e))
    }

    /// Snapshot the current connection. The returned `Arc<Connection>` may
    /// be replaced under us at any point; this is fine — operations on the
    /// snapshot will simply fail with a connection-class error and the
    /// caller can trigger a retry via `with_reconnect`.
    fn snapshot(&self) -> Arc<Connection> {
        self.inner.connection.load_full()
    }

    /// Run `op` against a snapshot of the current connection. If it fails with
    /// a connection-class error, dial a fresh `Connection`, swap it in via the
    /// single-flight reconnect path, and retry `op` exactly once against the
    /// new snapshot. All other lapin errors are mapped and returned directly.
    async fn with_reconnect<F, Fut, T>(&self, op_name: &'static str, op: F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: Future<Output = std::result::Result<T, lapin::Error>>,
    {
        if self.inner.shutdown_token.is_cancelled() {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Connection,
            );
            return Err(ShoveError::Connection(format!(
                "cannot {op_name}: client is shutting down"
            )));
        }

        let observed = self.snapshot();
        match op(observed.clone()).await {
            Ok(v) => Ok(v),
            Err(e) if is_connection_dead(&e) => {
                tracing::warn!(error = %e, op = op_name, "RabbitMQ connection appears dead, reconnecting");
                self.reconnect(&observed).await?;
                let fresh = self.snapshot();
                op(fresh)
                    .await
                    .map_err(|e| map_lapin_error(&format!("{op_name} failed after reconnect"), e))
            }
            Err(e) => Err(map_lapin_error(&format!("{op_name} failed"), e)),
        }
    }

    /// Dial a fresh `Connection` and swap it into `inner.connection`, but only
    /// if the current connection is still the one the caller observed failing.
    /// Single-flight via `reconnect_lock` so concurrent failures don't dial-storm.
    async fn reconnect(&self, observed: &Arc<Connection>) -> Result<()> {
        if self.inner.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection(
                "cannot reconnect: client is shutting down".into(),
            ));
        }
        let _guard = self.inner.reconnect_lock.lock().await;
        // Re-check after taking the lock — another caller may already have
        // swapped in a fresh connection while we were waiting.
        let current = self.inner.connection.load_full();
        if !Arc::ptr_eq(&current, observed) {
            return Ok(());
        }
        let new_conn = Self::dial(&self.inner.config).await?;
        self.inner.connection.store(Arc::new(new_conn));
        tracing::info!("RabbitMQ connection re-established");
        Ok(())
    }

    /// Open a basic channel on the underlying connection.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested
    /// or if the channel cannot be created.
    pub async fn create_channel(&self) -> Result<Channel> {
        self.with_reconnect("create channel", |conn| async move {
            conn.create_channel().await
        })
        .await
    }

    /// Open a channel with publisher confirms enabled.
    ///
    /// Returns [`ShoveError::Connection`] if shutdown has already been requested,
    /// if the channel cannot be created, or if confirms cannot be enabled.
    pub async fn create_confirm_channel(&self) -> Result<Channel> {
        self.with_reconnect("create confirm channel", |conn| async move {
            let channel = conn.create_channel().await?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            Ok(channel)
        })
        .await
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
        self.with_reconnect("create tx channel", |conn| async move {
            let channel = conn.create_channel().await?;
            channel.tx_select().await?;
            Ok(channel)
        })
        .await
    }

    /// Return a clone of the shutdown [`CancellationToken`].
    ///
    /// Callers can use this token to coordinate their own teardown with the
    /// client's shutdown sequence.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown_token.clone()
    }

    /// Return `true` if the underlying AMQP connection is still open.
    pub fn is_connected(&self) -> bool {
        self.snapshot().status().connected()
    }

    /// Initiate a graceful shutdown.
    ///
    /// Cancels the shutdown token so that dependent tasks can begin winding
    /// down, waits for [`SHUTDOWN_GRACE`] to allow in-flight operations to
    /// complete, and then closes the underlying AMQP connection.
    pub async fn shutdown(&self) {
        self.inner.shutdown_token.cancel();
        sleep(SHUTDOWN_GRACE).await;

        if let Err(e) = self.snapshot().close(0, "shutdown".into()).await {
            tracing::warn!("error while closing RabbitMQ connection: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use lapin::protocol::{AMQPError, AMQPErrorKind, AMQPHardError, AMQPSoftError};

    use super::*;
    use lapin::ErrorKind;
    use lapin::protocol::{AMQPError, AMQPErrorKind, AMQPHardError, AMQPSoftError};

    #[test]
    fn config_debug_redacts_password_only() {
        let config = RabbitMqConfig::new("amqp://admin:s3cret!@localhost:5672/%2F");
        let debug_output = format!("{config:?}");
        assert!(!debug_output.contains("s3cret!"));
        assert!(debug_output.contains("admin@localhost"));
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

    #[test]
    fn default_config_is_localhost() {
        let cfg = RabbitMqConfig::default();
        assert!(cfg.uri().contains("localhost:5672"));
    }

    #[test]
    fn invalid_connection_state_is_dead() {
        let err = lapin::Error::from(ErrorKind::InvalidConnectionState(
            lapin::ConnectionState::Closed,
        ));
        assert!(is_connection_dead(&err));
    }

    #[test]
    fn missing_heartbeat_is_dead() {
        let err = lapin::Error::from(ErrorKind::MissingHeartbeatError);
        assert!(is_connection_dead(&err));
    }

    #[test]
    fn invalid_channel_state_is_not_connection_dead() {
        let err = lapin::Error::from(ErrorKind::InvalidChannelState(
            lapin::ChannelState::Closed,
            "test",
        ));
        assert!(!is_connection_dead(&err));
    }

    #[test]
    fn channels_limit_is_not_connection_dead() {
        let err = lapin::Error::from(ErrorKind::ChannelsLimitReached);
        assert!(!is_connection_dead(&err));
    }

    #[test]
    fn io_error_is_dead() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "broken pipe");
        let err = lapin::Error::from(ErrorKind::IOError(Arc::new(io_err)));
        assert!(is_connection_dead(&err));
    }

    #[test]
    fn amqp_hard_error_is_dead() {
        // CONNECTION_FORCED (320) is a hard error — broker initiated the close.
        let amqp_err = AMQPError::new(
            AMQPErrorKind::Hard(AMQPHardError::CONNECTIONFORCED),
            "broker closed".into(),
        );
        let err = lapin::Error::from(ErrorKind::ProtocolError(amqp_err));
        assert!(is_connection_dead(&err));
    }

    #[test]
    fn amqp_soft_error_is_not_connection_dead() {
        // ACCESS_REFUSED (403) is a soft (channel-class) error — connection lives.
        let amqp_err = AMQPError::new(
            AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED),
            "denied".into(),
        );
        let err = lapin::Error::from(ErrorKind::ProtocolError(amqp_err));
        assert!(!is_connection_dead(&err));
    }
}
