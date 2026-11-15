use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;

use lapin::options::ConfirmSelectOptions;
use lapin::uri::AMQPUri;
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
    /// Optional management-API config. When set, the broker's
    /// `queue_stats_provider()` returns a working `ManagementClient` and
    /// `RabbitMqAutoscalerBackend` can poll real backlog. When `None`,
    /// `snapshot` returns a clear "management not configured" error.
    pub management: Option<super::management::ManagementConfig>,
}

/// Renders `raw` from scheme, username, host, port and path only. Password, query
/// and fragment are dropped; a non-empty query leaves a `?<redacted>` marker.
pub(super) fn redact_url(raw: &str) -> String {
    const UNPARSEABLE: &str = "<unparseable>";
    let Ok(url) = url::Url::parse(raw) else {
        return UNPARSEABLE.to_string();
    };
    if url.cannot_be_a_base() {
        return UNPARSEABLE.to_string();
    }
    let host = url.host_str().unwrap_or_default();
    let userinfo = if url.username().is_empty() {
        String::new()
    } else {
        format!("{}@", url.username())
    };
    let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    let query = if url.query().is_some_and(|q| !q.is_empty()) {
        "?<redacted>"
    } else {
        ""
    };
    format!(
        "{}://{userinfo}{host}{port}{}{query}",
        url.scheme(),
        url.path()
    )
}

impl Debug for RabbitMqConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let redacted_uri = redact_url(&self.uri);

        f.debug_struct("RabbitMqConfig")
            .field("uri", &redacted_uri)
            .field(
                "management",
                if self.management.is_some() {
                    &"<configured>"
                } else {
                    &"<none>"
                },
            )
            .finish()
    }
}

impl RabbitMqConfig {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            management: None,
        }
    }

    /// AMQP connection URI this config was built with.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Attach management-API credentials.
    ///
    /// Required for `Broker<RabbitMq>::queue_stats_provider().snapshot(...)`
    /// to return real data, and for `RabbitMqAutoscalerBackend` to poll
    /// queue depth. Without management, those code paths surface a
    /// `Topology` error directing the caller to set this.
    pub fn with_management(mut self, management: super::management::ManagementConfig) -> Self {
        self.management = Some(management);
        self
    }

    /// Borrow the configured management-API credentials, if any.
    pub fn management(&self) -> Option<&super::management::ManagementConfig> {
        self.management.as_ref()
    }
}

impl Default for RabbitMqConfig {
    /// Default RabbitMQ endpoint for local development.
    fn default() -> Self {
        Self::new("amqp://guest:guest@localhost:5672")
    }
}

/// Parses `uri` for lapin. Errors carry a fixed message because the input holds
/// the broker password and the underlying parsers echo it.
fn parse_uri(uri: &str) -> Result<AMQPUri> {
    let invalid = || {
        ShoveError::Validation("RabbitMqConfig::uri is not a valid amqp:// or amqps:// URI".into())
    };
    let url = url::Url::parse(uri).map_err(|_| invalid())?;
    if url.cannot_be_a_base() || !matches!(url.scheme(), "amqp" | "amqps") {
        return Err(invalid());
    }
    uri.parse::<AMQPUri>().map_err(|_| invalid())
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
        parse_uri(&config.uri)?;
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

    /// Dial an independent sibling `RabbitMqClient` that reuses this client's
    /// stored [`RabbitMqConfig`] but owns its own `Connection`, its own
    /// `reconnect_lock`, and shares this client's `shutdown_token` so the
    /// parent's shutdown propagates.
    ///
    /// Used by `RabbitMqConsumerGroup` to fan a single user-supplied client
    /// out into a small pool when `max_consumers > 50` — lapin serialises
    /// all channels of one connection through one reader task, so multiple
    /// connections are needed to scale past ~50 high-throughput channels.
    ///
    /// Returns [`ShoveError::Connection`] if the new dial fails.
    pub async fn dial_sibling(&self) -> Result<Self> {
        let connection = Self::dial(&self.inner.config).await?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                connection: arc_swap::ArcSwap::from_pointee(connection),
                config: self.inner.config.clone(),
                reconnect_lock: tokio::sync::Mutex::new(()),
                shutdown_token: self.inner.shutdown_token.clone(),
            }),
        })
    }

    /// Dial a fresh `Connection` using the stored config. Used by `connect`
    /// and by `reconnect` after a broker disconnect.
    ///
    /// Times out after 5 seconds so that a broker that accepts the TCP
    /// connection but never completes the AMQP handshake (e.g. during
    /// `stop_app`) does not block the caller indefinitely.
    async fn dial(config: &RabbitMqConfig) -> Result<Connection> {
        let uri = parse_uri(&config.uri)?;
        let pid = std::process::id();
        let connection_name = format!("shove-rs-{pid}");

        let properties =
            ConnectionProperties::default().with_connection_name(connection_name.into());

        timeout(
            Duration::from_secs(5),
            Connection::connect_uri(uri, properties),
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

    /// Snapshot the management-API credentials this client was constructed
    /// with, if any. Returned by value so the caller can move it into a
    /// `ManagementClient`.
    pub fn management_config(&self) -> Option<super::management::ManagementConfig> {
        self.inner.config.management.clone()
    }

    /// Return `true` if the underlying AMQP connection is still open.
    pub fn is_connected(&self) -> bool {
        self.snapshot().status().connected()
    }

    /// Liveness check. Opens a transient channel and closes it immediately.
    ///
    /// Both operations require a real AMQP round-trip, proving the connection
    /// is alive. `create_channel` already checks the shutdown token and
    /// triggers a single reconnect on dead-connection errors — that behavior
    /// is desirable for a liveness probe (broker reachable, our cached
    /// connection went stale).
    pub async fn ping(&self, timeout: Duration) -> Result<()> {
        let fut = async {
            let chan = self.create_channel().await?;
            chan.close(200, "ping".into()).await.map_err(|e| {
                ShoveError::Connection(format!("rabbitmq ping channel close failed: {e}"))
            })?;
            Ok::<(), ShoveError>(())
        };
        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            ShoveError::Connection(format!("rabbitmq ping timed out after {timeout:?}"))
        })?
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

    const MALFORMED_URIS: [&str; 5] = [
        "amqp://admin:s3cret@:5672/%2f",
        "amqp:admin:s3cret@host:5672",
        "admin:s3cret@host:5672/%2f",
        "http://admin:s3cret@host:5672/%2f",
        "amqp://admin:s3cret@host:notaport/%2f",
    ];

    fn assert_no_leak(rendered: &str) {
        assert!(!rendered.contains("s3cret"), "password leaked: {rendered}");
        assert!(!rendered.contains("admin"), "username leaked: {rendered}");
    }

    #[test]
    fn config_debug_never_leaks_password_for_malformed_uri() {
        for uri in MALFORMED_URIS {
            let rendered = format!("{:?}", RabbitMqConfig::new(uri));
            assert!(!rendered.contains("s3cret"), "{uri}: {rendered}");
        }
        let rendered = format!("{:?}", RabbitMqConfig::new("amqp:admin:s3cret@host:5672"));
        assert!(rendered.contains("<unparseable>"), "{rendered}");
    }

    #[test]
    fn parse_uri_rejects_malformed_without_echoing_input() {
        for uri in MALFORMED_URIS {
            let err = parse_uri(uri).unwrap_err();
            assert!(matches!(err, ShoveError::Validation(_)), "{uri}: {err:?}");
            assert_no_leak(&err.to_string());
            assert_no_leak(&format!("{err:?}"));
        }
    }

    #[test]
    fn parse_uri_accepts_amqp_and_amqps() {
        assert!(parse_uri("amqp://user:pass@host:5672/%2f").is_ok());
        assert!(parse_uri("amqps://user:pass@host:5671/%2f").is_ok());
    }

    #[test]
    fn config_debug_still_redacts_well_formed_uri() {
        let rendered = format!(
            "{:?}",
            RabbitMqConfig::new("amqp://user:pass@host:5672/%2f")
        );
        assert!(!rendered.contains("pass@"), "{rendered}");
        assert!(rendered.contains("user@host"), "{rendered}");
    }

    #[tokio::test]
    async fn connect_rejects_malformed_uri_without_echoing_input() {
        for uri in MALFORMED_URIS {
            let config = RabbitMqConfig::new(uri);
            let err = RabbitMqClient::connect(&config).await.err().unwrap();
            assert_no_leak(&err.to_string());
            assert_no_leak(&format!("{err:?}"));
        }
    }

    #[tokio::test]
    async fn connect_with_retry_fails_fast_on_malformed_uri() {
        let config = RabbitMqConfig::new("admin:s3cret@host:5672/%2f");
        let started = std::time::Instant::now();
        let err = RabbitMqClient::connect_with_retry(&config, 5)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, ShoveError::Validation(_)), "{err:?}");
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn config_debug_drops_query_and_fragment() {
        let rendered = format!(
            "{:?}",
            RabbitMqConfig::new(
                "amqp://user:s3cret@host:5672/%2f?password=s3cret&heartbeat=10#s3cret"
            )
        );
        assert!(!rendered.contains("s3cret"), "{rendered}");
        assert!(!rendered.contains("heartbeat"), "{rendered}");
        assert!(
            rendered.contains("amqp://user@host:5672/%2f?<redacted>"),
            "{rendered}"
        );
    }

    #[test]
    fn config_debug_without_query_has_no_marker() {
        let rendered = format!(
            "{:?}",
            RabbitMqConfig::new("amqp://user:pw@host:5672/%2f#frag")
        );
        assert!(!rendered.contains("redacted"), "{rendered}");
        assert!(!rendered.contains("frag"), "{rendered}");
        assert!(rendered.contains("amqp://user@host:5672/%2f"), "{rendered}");
    }

    #[test]
    fn parse_uri_accepts_hostless_forms_like_upstream() {
        let cases = [
            ("amqp://", "/"),
            ("amqp:///", ""),
            ("amqp:///%2f", "/"),
            ("amqp:///vhost", "vhost"),
        ];
        for (uri, vhost) in cases {
            let parsed = parse_uri(uri).unwrap();
            assert_eq!(parsed.authority.host, "localhost", "{uri}");
            assert_eq!(parsed.authority.port, 5672, "{uri}");
            assert_eq!(parsed.vhost, vhost, "{uri}");
        }
        assert_eq!(parse_uri("amqps://").unwrap().authority.port, 5671);
    }

    #[test]
    fn empty_host_with_userinfo_is_rejected_by_url_itself() {
        assert!(url::Url::parse("amqp://admin:s3cret@:5672/%2f").is_err());
    }

    #[test]
    fn config_debug_renders_hostless_uris() {
        let rendered = format!("{:?}", RabbitMqConfig::new("amqp:///%2f?heartbeat=10"));
        assert!(rendered.contains("amqp:///%2f?<redacted>"), "{rendered}");
        let rendered = format!("{:?}", RabbitMqConfig::new("amqp://"));
        assert!(!rendered.contains("unparseable"), "{rendered}");
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
