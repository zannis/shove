//! Redis client abstraction supporting standalone, TLS (`rediss://`), and cluster modes.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::cluster::{ClusterClient, ClusterConfig};
use redis::cluster_async::ClusterConnection;
use redis::{ConnectionAddr, IntoConnectionInfo};
use rustls::crypto::ring::default_provider;

use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};
use crate::retry::Backoff;

use super::constants::{BLOCK_MS, DEFAULT_GROUP};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How to connect to Redis/Valkey.
pub enum RedisMode {
    /// Standalone Redis or Valkey (single node or sentinel).
    /// Use `rediss://` scheme for TLS. Examples:
    ///   - `redis://127.0.0.1:6379/`
    ///   - `rediss://user:pass@myhost:6380/`
    ///
    /// For TLS URLs, shove installs the `ring` rustls provider as the process
    /// default when none is set; an application-installed provider is kept.
    Standalone { url: String },
    /// Redis Cluster. Provide one or more seed node URLs (plain or `rediss://` for TLS).
    Cluster { urls: Vec<String> },
}

/// Default per-command response timeout. Must exceed [`BLOCK_MS`] (currently
/// 2 s) so XREADGROUP doesn't trip on the timeout instead of returning the
/// blocking reply. The redis-rs default of 500 ms is too tight for both
/// blocking reads and batched XADDs under heavy concurrent load.
pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default connection-establishment timeout. A healthy Redis accepts in
/// <100 ms; the redis-rs default of 1 s misfires when fresh multiplexed
/// conns are acquired during peak traffic with many consumers blocked on
/// XREADGROUP. 10 s gives margin without hiding a truly unreachable broker.
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Default cap on a broadcast stream's length, in entries. See
/// [`RedisConfig::with_broadcast_max_len`].
pub const DEFAULT_BROADCAST_MAX_LEN: u64 = 10_000;

const DEFAULT_BROADCAST_MAX_LEN_NONZERO: NonZeroU64 =
    match NonZeroU64::new(DEFAULT_BROADCAST_MAX_LEN) {
        Some(n) => n,
        None => panic!("DEFAULT_BROADCAST_MAX_LEN must be positive"),
    };

/// Configuration for [`RedisClient`].
pub struct RedisConfig {
    /// Connection mode: standalone or cluster.
    pub mode: RedisMode,
    /// Consumer group name. All consumers of a topic share this group so that
    /// each message is delivered to exactly one consumer. Defaults to `"shove"`.
    pub group: Option<String>,
    /// Per-command response timeout. Defaults to [`DEFAULT_RESPONSE_TIMEOUT`].
    /// Must exceed [`BLOCK_MS`] (2 s).
    ///
    /// Crate-private to force callers through
    /// [`with_response_timeout`](Self::with_response_timeout), which
    /// enforces the BLOCK_MS lower bound. Setting it via a struct
    /// literal would bypass the assertion silently.
    pub(crate) response_timeout: Duration,
    /// Connection-establishment timeout. Defaults to
    /// [`DEFAULT_CONNECTION_TIMEOUT`].
    ///
    /// Crate-private to mirror `response_timeout` — set via
    /// [`with_connection_timeout`](Self::with_connection_timeout).
    pub(crate) connection_timeout: Duration,
    /// How often the maintenance sidecar trims acknowledged entries. `None`
    /// derives it from the handler timeout, floored at 30 s. Set via
    /// [`with_trim_interval`](Self::with_trim_interval).
    pub(crate) trim_interval: Option<Duration>,
    /// Approximate cap on a broadcast stream's length, applied on every
    /// broadcast publish. `None` publishes without trimming. Defaults to
    /// [`DEFAULT_BROADCAST_MAX_LEN`]. Set via
    /// [`with_broadcast_max_len`](Self::with_broadcast_max_len) or
    /// [`without_broadcast_trim`](Self::without_broadcast_trim).
    pub(crate) broadcast_max_len: Option<NonZeroU64>,
}

impl Default for RedisConfig {
    /// Defaults to `redis://127.0.0.1:6379/`, no explicit group, and the
    /// shove-tuned timeouts above. Useful with struct-update syntax:
    ///
    /// ```ignore
    /// RedisConfig {
    ///     mode: RedisMode::Standalone { url: my_url },
    ///     ..Default::default()
    /// }
    /// ```
    fn default() -> Self {
        Self {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: None,
            response_timeout: DEFAULT_RESPONSE_TIMEOUT,
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            trim_interval: None,
            broadcast_max_len: Some(DEFAULT_BROADCAST_MAX_LEN_NONZERO),
        }
    }
}

impl RedisConfig {
    /// Construct a `RedisConfig` with the given mode and shove-tuned default
    /// timeouts. Chain `.with_*` builders to customise further.
    pub fn new(mode: RedisMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    /// Set the consumer-group name.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Set the per-command response timeout.
    ///
    /// # Panics
    ///
    /// Panics if `timeout <= BLOCK_MS` (2 s). Setting it any lower would
    /// cause every XREADGROUP BLOCK 2000 in the consumer loop to trip the
    /// timeout instead of returning the reply.
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            timeout > Duration::from_millis(BLOCK_MS),
            "response_timeout ({} ms) must exceed BLOCK_MS ({} ms)",
            timeout.as_millis(),
            BLOCK_MS,
        );
        self.response_timeout = timeout;
        self
    }

    /// Set the connection-establishment timeout.
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "connection_timeout must be positive");
        self.connection_timeout = timeout;
        self
    }

    /// Set how often the maintenance sidecar trims acknowledged entries off
    /// the stream.
    ///
    /// An `XACK` only clears the pending entry; the entry itself stays in the
    /// stream until a sidecar sweep `XTRIM`s everything every group has
    /// acknowledged. By default a sweep runs every handler timeout, floored at
    /// 30 s, so at a high publish rate up to 30 s of already-consumed traffic
    /// sits in memory. Lower this when the stream's memory has to track the
    /// backlog closely; each sweep costs `XINFO GROUPS`, an `XPENDING` per
    /// group and one `XTRIM`, so sub-second values buy memory with round
    /// trips. Reclaim of stale pending entries is unaffected: it still gates
    /// on the handler timeout.
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    pub fn with_trim_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "trim_interval must be positive");
        self.trim_interval = Some(interval);
        self
    }

    /// Cap the length of every broadcast stream this client publishes to.
    ///
    /// A broadcast subscriber reads with `XREAD`, which neither acknowledges
    /// nor trims, and a broadcast stream has no consumer group for the
    /// maintenance sweep to trim behind. The publisher is therefore the only
    /// party that can bound the stream: each publish to a
    /// [`broadcast`](crate::topology::TopologyBuilder::broadcast) topology is
    /// sent as `XADD ... MAXLEN ~ max_len`. Defaults to
    /// [`DEFAULT_BROADCAST_MAX_LEN`] entries.
    ///
    /// The cap is approximate. Redis trims whole radix-tree nodes, so the
    /// stream can sit above `max_len` by up to one node (100 entries with the
    /// server's default `stream-node-max-entries`) and never drops below it.
    ///
    /// The cap is also the replay window. A subscriber that falls more than
    /// `max_len` entries behind the tail, or reconnects after an outage in
    /// which more than `max_len` entries were published, resumes at the
    /// oldest entry still in the stream and never sees the trimmed ones.
    /// Redis reports no error for a read position that has been trimmed away,
    /// so the gap is silent.
    ///
    /// Streams of every other topology are untouched: their entries leave
    /// through the acknowledged-entry sweep (see
    /// [`with_trim_interval`](Self::with_trim_interval)), and a length cap
    /// there would drop undelivered work.
    ///
    /// # Errors
    ///
    /// Returns [`ShoveError::Validation`] if `max_len` is zero or exceeds
    /// `i64::MAX`, the largest length Redis accepts.
    pub fn with_broadcast_max_len(mut self, max_len: u64) -> Result<Self> {
        let bounded = NonZeroU64::new(max_len)
            .filter(|n| i64::try_from(n.get()).is_ok())
            .ok_or_else(|| {
                ShoveError::Validation(format!(
                    "broadcast_max_len must be between 1 and {}, got {max_len}; use \
                     without_broadcast_trim() to publish without a cap",
                    i64::MAX
                ))
            })?;
        self.broadcast_max_len = Some(bounded);
        Ok(self)
    }

    /// Publish to broadcast streams without a length cap.
    ///
    /// Nothing in shove trims a broadcast stream after this, so it grows
    /// until something else bounds it, such as an external `XTRIM` job or a
    /// key expiry. See [`with_broadcast_max_len`](Self::with_broadcast_max_len).
    pub fn without_broadcast_trim(mut self) -> Self {
        self.broadcast_max_len = None;
        self
    }

    /// Return the resolved consumer group name, defaulting to `"shove"` if not set.
    pub fn resolved_group(&self) -> &str {
        self.group.as_deref().unwrap_or(DEFAULT_GROUP)
    }
}

// ---------------------------------------------------------------------------
// Internal connection enum
// ---------------------------------------------------------------------------

/// A single Redis connection, abstracting over standalone vs cluster transports.
///
/// Both variants wrap a redis-rs connection type that is `Clone` — the clones
/// share the underlying multiplexer task and TCP socket. That lets callers
/// hoist one `RedisConnection` per long-lived task (e.g. consumer loop) and
/// hand `.clone()`s to short-lived per-message work without opening a fresh
/// TCP socket on every operation.
pub(crate) enum RedisConnection {
    /// Multiplexed standalone connection – safe for BLOCK commands with finite timeouts (BLOCK 2000).
    Standalone(MultiplexedConnection),
    /// Cluster connection – safe to share; BLOCK commands work the same way.
    Cluster(ClusterConnection),
}

impl Clone for RedisConnection {
    fn clone(&self) -> Self {
        match self {
            RedisConnection::Standalone(c) => RedisConnection::Standalone(c.clone()),
            RedisConnection::Cluster(c) => RedisConnection::Cluster(c.clone()),
        }
    }
}

impl RedisConnection {
    /// Execute `cmd` while preserving redis-rs's error taxonomy for callers
    /// that need to distinguish a server response from an unresolved transport
    /// failure.
    pub(crate) async fn query_redis<T: redis::FromRedisValue + Send>(
        &mut self,
        cmd: &mut redis::Cmd,
    ) -> redis::RedisResult<T> {
        match self {
            RedisConnection::Standalone(conn) => cmd.query_async(conn).await,
            RedisConnection::Cluster(conn) => cmd.query_async(conn).await,
        }
    }

    /// Execute `cmd` and deserialize the response into `T`.
    pub(crate) async fn query<T: redis::FromRedisValue + Send>(
        &mut self,
        cmd: &mut redis::Cmd,
    ) -> Result<T> {
        self.query_redis(cmd)
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Client inner
// ---------------------------------------------------------------------------

enum ClientInner {
    Standalone(redis::Client),
    Cluster(ClusterClient),
}

// ---------------------------------------------------------------------------
// Public client handle
// ---------------------------------------------------------------------------

/// Cheap-to-clone handle to a Redis/Valkey connection pool.
///
/// Internally branches on standalone vs cluster because the two use different
/// connection types from the `redis` crate.
#[derive(Clone)]
pub struct RedisClient {
    inner: Arc<ClientInner>,
    pub(super) group: String,
    response_timeout: Duration,
    connection_timeout: Duration,
    trim_interval: Option<Duration>,
    broadcast_max_len: Option<NonZeroU64>,
}

fn is_tls_url(url: &str) -> bool {
    url.into_connection_info()
        .is_ok_and(|info| matches!(info.addr(), ConnectionAddr::TcpTls { .. }))
}

/// Installs the `ring` provider as rustls' process default when none is set.
fn ensure_crypto_provider() {
    // Err means a default is already installed, which is left in place.
    let _ = default_provider().install_default();
}

impl RedisClient {
    /// Build a [`RedisClient`] and eagerly verify connectivity by opening a
    /// test connection.
    pub(super) async fn connect(config: RedisConfig) -> Result<Self> {
        let group = config.resolved_group().to_owned();
        let response_timeout = config.response_timeout;
        let connection_timeout = config.connection_timeout;
        let trim_interval = config.trim_interval;
        let broadcast_max_len = config.broadcast_max_len;

        let inner = match config.mode {
            RedisMode::Standalone { url } => {
                if is_tls_url(&url) {
                    ensure_crypto_provider();
                }
                let client = redis::Client::open(url.as_str())
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                client
                    .get_multiplexed_async_connection_with_config(&async_config(
                        response_timeout,
                        connection_timeout,
                    ))
                    .await
                    .map_err(|e| ShoveError::Connection(format!("standalone ping failed: {e}")))?;
                ClientInner::Standalone(client)
            }
            RedisMode::Cluster { ref urls } => {
                if urls.is_empty() {
                    return Err(ShoveError::Connection(
                        "cluster URLs must not be empty".into(),
                    ));
                }
                if urls.iter().any(|u| is_tls_url(u)) {
                    ensure_crypto_provider();
                }
                let nodes: Vec<&str> = urls.iter().map(String::as_str).collect();
                let client =
                    ClusterClient::new(nodes).map_err(|e| ShoveError::Connection(e.to_string()))?;
                // Eagerly verify connectivity.
                client
                    .get_async_connection_with_config(cluster_config(
                        response_timeout,
                        connection_timeout,
                    ))
                    .await
                    .map_err(|e| ShoveError::Connection(format!("cluster ping failed: {e}")))?;
                ClientInner::Cluster(client)
            }
        };

        let client = Self {
            inner: Arc::new(inner),
            group,
            response_timeout,
            connection_timeout,
            trim_interval,
            broadcast_max_len,
        };

        let mut conn = client.multiplexed_conn().await?;
        check_min_version(&mut conn).await?;

        Ok(client)
    }

    /// Return a multiplexed (shared) connection suitable for non-blocking operations
    /// such as XADD, XACK, ZADD, and XLEN.
    ///
    /// Applies the [`RedisConfig::response_timeout`] /
    /// [`RedisConfig::connection_timeout`] this client was built with. The
    /// shove defaults (30 s response, 10 s connect) override the much tighter
    /// redis-rs defaults (500 ms / 1 s) which trip under heavy concurrent
    /// XREADGROUP load.
    pub(super) async fn multiplexed_conn(&self) -> Result<RedisConnection> {
        match self.inner.as_ref() {
            ClientInner::Standalone(client) => client
                .get_multiplexed_async_connection_with_config(&async_config(
                    self.response_timeout,
                    self.connection_timeout,
                ))
                .await
                .map(RedisConnection::Standalone)
                .map_err(|e| ShoveError::Connection(e.to_string())),
            ClientInner::Cluster(client) => client
                .get_async_connection_with_config(cluster_config(
                    self.response_timeout,
                    self.connection_timeout,
                ))
                .await
                .map(RedisConnection::Cluster)
                .map_err(|e| ShoveError::Connection(e.to_string())),
        }
    }

    /// Return a dedicated connection suitable for consumer loops that use BLOCK commands
    /// (e.g. XREADGROUP with `BLOCK 2000`).
    ///
    /// Equivalent to [`multiplexed_conn`](Self::multiplexed_conn) — the
    /// previous distinction (one disabled timeouts, the other didn't) went
    /// away when timeouts became configurable: the
    /// [`RedisConfig::with_response_timeout`] builder enforces
    /// `response_timeout > BLOCK_MS`, so blocking reads, batched publishes,
    /// and outcome routing now share a single timeout policy.
    pub(super) async fn dedicated_conn(&self) -> Result<RedisConnection> {
        self.multiplexed_conn().await
    }

    /// Liveness check. Sends a `PING` over the multiplexed connection,
    /// bounded by `timeout`. Returns `Err(ShoveError::Connection)` on
    /// timeout, transport error, or unexpected reply.
    ///
    /// The entire body — including connection establishment — runs inside the
    /// `tokio::time::timeout` wrapper so that a slow `multiplexed_conn` call
    /// (up to [`DEFAULT_CONNECTION_TIMEOUT`] by default) cannot escape the
    /// caller's deadline.
    pub(super) async fn ping(&self, timeout: Duration) -> Result<()> {
        let fut = async {
            let mut conn = self.multiplexed_conn().await?;
            let reply: String = conn
                .query::<String>(&mut redis::cmd("PING"))
                .await
                .map_err(|e| ShoveError::Connection(format!("redis ping failed: {e}")))?;
            if reply != "PONG" {
                return Err(ShoveError::Connection(format!(
                    "redis ping returned {reply:?}, expected PONG"
                )));
            }
            Ok::<(), ShoveError>(())
        };
        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            ShoveError::Connection(format!("redis ping timed out after {timeout:?}"))
        })?
    }

    /// The consumer group name shared by all consumers on this client.
    pub(super) fn group(&self) -> &str {
        &self.group
    }

    /// The configured trim cadence override, if any
    /// ([`RedisConfig::with_trim_interval`]).
    pub(super) fn trim_interval(&self) -> Option<Duration> {
        self.trim_interval
    }

    /// The broadcast stream length cap, if trimming is enabled
    /// ([`RedisConfig::with_broadcast_max_len`]).
    pub(super) fn broadcast_max_len(&self) -> Option<NonZeroU64> {
        self.broadcast_max_len
    }

    /// Identity of this client's shared inner connection state. Clones of one
    /// client share it; independently-connected clients (possibly to other
    /// servers) differ. Used by the maintenance registry to scope its
    /// per-`(client, stream, group)` deduplication.
    pub(super) fn instance_id(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }
}

/// Build an [`AsyncConnectionConfig`](redis::AsyncConnectionConfig) from the
/// configured timeouts. Used by both `connect()` (the eager ping) and
/// `multiplexed_conn()` so the verification matches runtime behaviour.
fn async_config(response: Duration, connection: Duration) -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(response))
        .set_connection_timeout(Some(connection))
}

/// Build a [`ClusterConfig`](redis::cluster::ClusterConfig) with the configured
/// timeouts. Cluster's setters take non-optional `Duration`, so both timeouts
/// are always applied.
fn cluster_config(response: Duration, connection: Duration) -> ClusterConfig {
    ClusterConfig::new()
        .set_response_timeout(response)
        .set_connection_timeout(connection)
}

// ---------------------------------------------------------------------------
// Version check
// ---------------------------------------------------------------------------

/// Verify that the connected server is Redis/Valkey 6.2 or newer.
///
/// shove uses `ZRANGE … BYSCORE` (introduced in Redis 6.2) for hold-queue
/// polling, so older servers will fail at runtime with cryptic errors.
/// Catching the version mismatch at connection time gives a clear message.
/// Parse and validate the `redis_version` field from an `INFO server` response.
///
/// Extracted as a sync function so it can be unit-tested without a real connection.
fn check_version_info(info: &str) -> Result<()> {
    let version = info
        .lines()
        .find_map(|line| line.strip_prefix("redis_version:"))
        .ok_or_else(|| ShoveError::Connection("could not determine Redis version".into()))?
        .trim();

    let mut parts = version.splitn(3, '.');
    let major: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ShoveError::Connection(format!("unparseable Redis version: {version}")))?;
    let minor: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ShoveError::Connection(format!("unparseable Redis version: {version}")))?;

    if (major, minor) < (6, 2) {
        return Err(ShoveError::Connection(format!(
            "Redis {version} is not supported; shove requires Redis 6.2 or newer"
        )));
    }

    Ok(())
}

async fn check_min_version(conn: &mut RedisConnection) -> Result<()> {
    let reply: redis::Value = conn
        .query(redis::cmd("INFO").arg("server"))
        .await
        .map_err(|e| ShoveError::Connection(format!("INFO server failed: {e}")))?;

    for info in info_payloads(reply)? {
        check_version_info(&info)?;
    }
    Ok(())
}

/// Extract one or more `INFO server` text payloads from a raw redis reply.
///
/// Standalone connections return a `BulkString` / `SimpleString` with the full
/// INFO text. Cluster connections fan `INFO server` out to every master and
/// return a `Map` keyed by `host:port` with each node's payload as the value,
/// so we have to walk the map and validate every master.
fn info_payloads(value: redis::Value) -> Result<Vec<String>> {
    fn decode_string(v: redis::Value) -> Result<String> {
        match v {
            redis::Value::SimpleString(s) => Ok(s),
            redis::Value::BulkString(b) => String::from_utf8(b)
                .map_err(|e| ShoveError::Connection(format!("INFO server not UTF-8: {e}"))),
            // VerbatimString carries a format prefix we don't care about — the
            // INFO text we want lives in the `text` field.
            redis::Value::VerbatimString { text, .. } => Ok(text),
            other => Err(ShoveError::Connection(format!(
                "unexpected INFO payload: {other:?}"
            ))),
        }
    }

    match value {
        redis::Value::SimpleString(_)
        | redis::Value::BulkString(_)
        | redis::Value::VerbatimString { .. } => Ok(vec![decode_string(value)?]),
        redis::Value::Map(entries) => {
            if entries.is_empty() {
                return Err(ShoveError::Connection("INFO server: empty map".into()));
            }
            entries
                .into_iter()
                .map(|(_node, payload)| decode_string(payload))
                .collect()
        }
        other => Err(ShoveError::Connection(format!(
            "unexpected INFO server reply: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Shared sidecar helper
// ---------------------------------------------------------------------------

/// Acquire a multiplexed Redis connection, retrying with exponential backoff
/// (1 s → 30 s, full jitter) until `shutdown` is cancelled.
///
/// Returns `None` if the token is cancelled before a connection is obtained.
/// The `task` label is embedded in the log line so callers don't need their
/// own copy of this loop.
pub(super) async fn acquire_conn_with_retry(
    client: &RedisClient,
    shutdown: &CancellationToken,
    task: &str,
) -> Option<RedisConnection> {
    let mut backoff = Backoff::default();
    loop {
        match client.multiplexed_conn().await {
            Ok(c) => return Some(c),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return None;
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    "{task}: connection failed ({e}), retrying in {:.1}s",
                    delay.as_secs_f64()
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return None,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (no running Redis required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use rustls::crypto::CryptoProvider;

    use super::*;

    #[test]
    fn broadcast_max_len_defaults_to_the_documented_cap() {
        assert_eq!(
            RedisConfig::default()
                .broadcast_max_len
                .map(NonZeroU64::get),
            Some(DEFAULT_BROADCAST_MAX_LEN)
        );
    }

    #[test]
    fn broadcast_max_len_accepts_the_bounds_redis_accepts() {
        for max_len in [1, i64::MAX as u64] {
            let cfg = RedisConfig::default()
                .with_broadcast_max_len(max_len)
                .expect("in range");
            assert_eq!(cfg.broadcast_max_len.map(NonZeroU64::get), Some(max_len));
        }
    }

    #[test]
    fn broadcast_max_len_rejects_zero_and_lengths_redis_cannot_hold() {
        for max_len in [0, i64::MAX as u64 + 1, u64::MAX] {
            let Err(err) = RedisConfig::default().with_broadcast_max_len(max_len) else {
                panic!("{max_len} was accepted");
            };
            assert!(
                matches!(&err, ShoveError::Validation(message) if message.contains("broadcast_max_len")),
                "{max_len}: {err:?}"
            );
        }
    }

    #[test]
    fn without_broadcast_trim_clears_the_cap() {
        assert!(
            RedisConfig::default()
                .without_broadcast_trim()
                .broadcast_max_len
                .is_none()
        );
    }

    #[test]
    fn config_default_group() {
        let cfg = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: None,
            ..RedisConfig::default()
        };
        assert_eq!(cfg.resolved_group(), "shove");
    }

    #[test]
    fn config_custom_group() {
        let cfg = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: Some("myapp".to_string()),
            ..RedisConfig::default()
        };
        assert_eq!(cfg.resolved_group(), "myapp");
    }

    #[test]
    fn standalone_url_preserved() {
        let url = "rediss://user:pass@myhost:6380/".to_string();
        let config = RedisConfig {
            mode: RedisMode::Standalone { url: url.clone() },
            group: None,
            ..RedisConfig::default()
        };
        match config.mode {
            RedisMode::Standalone { url: stored } => assert_eq!(stored, url),
            _ => panic!("expected Standalone"),
        }
    }

    #[test]
    fn cluster_urls_preserved() {
        let urls = vec![
            "redis://node1:6379/".to_string(),
            "redis://node2:6379/".to_string(),
            "redis://node3:6379/".to_string(),
        ];
        let config = RedisConfig {
            mode: RedisMode::Cluster { urls: urls.clone() },
            group: None,
            ..RedisConfig::default()
        };
        match config.mode {
            RedisMode::Cluster { urls: stored } => assert_eq!(stored, urls),
            _ => panic!("expected Cluster"),
        }
    }

    #[test]
    fn resolved_group_empty_string_preserved() {
        // Some("") is not None so it must be returned as-is, not replaced by the default.
        let cfg = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: Some(String::new()),
            ..RedisConfig::default()
        };
        assert_eq!(cfg.resolved_group(), "");
    }

    #[test]
    fn redis_mode_standalone_variant_matches() {
        let cfg = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://localhost/".to_string(),
            },
            group: None,
            ..RedisConfig::default()
        };
        assert!(matches!(cfg.mode, RedisMode::Standalone { .. }));
    }

    #[test]
    fn redis_mode_cluster_variant_matches() {
        let cfg = RedisConfig {
            mode: RedisMode::Cluster {
                urls: vec!["redis://node1/".to_string()],
            },
            group: None,
            ..RedisConfig::default()
        };
        assert!(matches!(cfg.mode, RedisMode::Cluster { .. }));
    }

    #[test]
    fn default_has_shove_tuned_timeouts() {
        let cfg = RedisConfig::default();
        assert_eq!(cfg.response_timeout, DEFAULT_RESPONSE_TIMEOUT);
        assert_eq!(cfg.connection_timeout, DEFAULT_CONNECTION_TIMEOUT);
        assert!(cfg.response_timeout > Duration::from_millis(BLOCK_MS));
    }

    #[test]
    fn new_constructor_seeds_defaults() {
        let cfg = RedisConfig::new(RedisMode::Standalone {
            url: "redis://127.0.0.1:6379/".to_string(),
        });
        assert_eq!(cfg.response_timeout, DEFAULT_RESPONSE_TIMEOUT);
        assert_eq!(cfg.connection_timeout, DEFAULT_CONNECTION_TIMEOUT);
        assert!(cfg.group.is_none());
    }

    #[test]
    fn with_response_timeout_round_trips() {
        let cfg = RedisConfig::default().with_response_timeout(Duration::from_secs(60));
        assert_eq!(cfg.response_timeout, Duration::from_secs(60));
    }

    #[test]
    #[should_panic(expected = "must exceed BLOCK_MS")]
    fn with_response_timeout_below_block_ms_panics() {
        let _ = RedisConfig::default().with_response_timeout(Duration::from_millis(BLOCK_MS));
    }

    #[test]
    fn with_response_timeout_at_block_ms_plus_one_accepted() {
        // Boundary regression guard: the assertion is `>`, not `>=`, so
        // BLOCK_MS + 1 ms must be the smallest accepted value.
        let cfg = RedisConfig::default().with_response_timeout(Duration::from_millis(BLOCK_MS + 1));
        assert_eq!(cfg.response_timeout, Duration::from_millis(BLOCK_MS + 1));
    }

    #[test]
    fn with_connection_timeout_round_trips() {
        let cfg = RedisConfig::default().with_connection_timeout(Duration::from_secs(5));
        assert_eq!(cfg.connection_timeout, Duration::from_secs(5));
    }

    #[test]
    #[should_panic(expected = "connection_timeout must be positive")]
    fn with_connection_timeout_zero_panics() {
        let _ = RedisConfig::default().with_connection_timeout(Duration::ZERO);
    }

    // -----------------------------------------------------------------------
    // check_version_info
    // -----------------------------------------------------------------------

    fn make_info(version: &str) -> String {
        format!("# Server\r\nredis_version:{version}\r\nredis_git_sha1:00000000\r\nos:Linux\r\n")
    }

    #[test]
    fn version_6_2_0_is_accepted() {
        assert!(check_version_info(&make_info("6.2.0")).is_ok());
    }

    #[test]
    fn version_6_2_14_is_accepted() {
        assert!(check_version_info(&make_info("6.2.14")).is_ok());
    }

    #[test]
    fn version_7_0_0_is_accepted() {
        assert!(check_version_info(&make_info("7.0.0")).is_ok());
    }

    #[test]
    fn version_8_0_0_is_accepted() {
        assert!(check_version_info(&make_info("8.0.0")).is_ok());
    }

    #[test]
    fn version_5_0_0_is_rejected() {
        let err = check_version_info(&make_info("5.0.0")).unwrap_err();
        assert!(err.to_string().contains("5.0.0"));
        assert!(err.to_string().contains("6.2"));
    }

    #[test]
    fn version_6_0_0_is_rejected() {
        let err = check_version_info(&make_info("6.0.0")).unwrap_err();
        assert!(err.to_string().contains("6.0.0"));
    }

    #[test]
    fn version_6_1_9_is_rejected() {
        let err = check_version_info(&make_info("6.1.9")).unwrap_err();
        assert!(err.to_string().contains("6.1.9"));
    }

    #[test]
    fn missing_version_line_is_an_error() {
        let info = "# Server\r\nredis_git_sha1:00000000\r\nos:Linux\r\n";
        let err = check_version_info(info).unwrap_err();
        assert!(
            err.to_string()
                .contains("could not determine Redis version")
        );
    }

    #[test]
    fn malformed_version_no_dots_is_an_error() {
        let err = check_version_info(&make_info("garbage")).unwrap_err();
        assert!(err.to_string().contains("unparseable Redis version"));
    }

    #[test]
    fn malformed_version_major_only_is_an_error() {
        let err = check_version_info(&make_info("7")).unwrap_err();
        assert!(err.to_string().contains("unparseable Redis version"));
    }

    #[test]
    fn version_line_with_surrounding_noise_is_parsed_correctly() {
        // Realistic INFO output has many lines before and after redis_version.
        let info = "# Server\r\nredis_version:7.2.5\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:abc\r\nredis_mode:standalone\r\nos:Linux\r\narch_bits:64\r\n";
        assert!(check_version_info(info).is_ok());
    }

    #[test]
    fn valkey_reports_compatible_redis_version() {
        // Valkey sets redis_version to a 7.x compat value for backwards compatibility.
        let info = "# Server\r\nredis_version:7.2.4\r\nvalkey_version:8.0.0\r\n";
        assert!(check_version_info(info).is_ok());
    }

    // -----------------------------------------------------------------------
    // info_payloads
    // -----------------------------------------------------------------------

    #[test]
    fn info_payloads_handles_standalone_bulk_string() {
        let value = redis::Value::BulkString(make_info("7.0.0").into_bytes());
        let payloads = info_payloads(value).unwrap();
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].contains("redis_version:7.0.0"));
    }

    #[test]
    fn info_payloads_handles_standalone_simple_string() {
        let value = redis::Value::SimpleString(make_info("7.0.0"));
        let payloads = info_payloads(value).unwrap();
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].contains("redis_version:7.0.0"));
    }

    #[test]
    fn info_payloads_handles_cluster_map_with_one_entry_per_master() {
        // Cluster INFO server fans out to every master and returns a map
        // keyed by host:port; the values are each node's INFO payload.
        let value = redis::Value::Map(vec![
            (
                redis::Value::BulkString(b"127.0.0.1:7001".to_vec()),
                redis::Value::BulkString(make_info("7.0.10").into_bytes()),
            ),
            (
                redis::Value::BulkString(b"127.0.0.1:7002".to_vec()),
                redis::Value::BulkString(make_info("7.0.10").into_bytes()),
            ),
            (
                redis::Value::BulkString(b"127.0.0.1:7000".to_vec()),
                redis::Value::BulkString(make_info("7.0.10").into_bytes()),
            ),
        ]);
        let payloads = info_payloads(value).unwrap();
        assert_eq!(payloads.len(), 3);
        for payload in &payloads {
            assert!(payload.contains("redis_version:7.0.10"));
        }
    }

    #[test]
    fn info_payloads_empty_map_is_an_error() {
        let err = info_payloads(redis::Value::Map(vec![])).unwrap_err();
        assert!(err.to_string().contains("empty map"));
    }

    #[test]
    fn info_payloads_unexpected_reply_is_an_error() {
        // An integer reply makes no sense for INFO; we should refuse it.
        let err = info_payloads(redis::Value::Int(42)).unwrap_err();
        assert!(err.to_string().contains("unexpected INFO server reply"));
    }

    #[tokio::test]
    async fn rediss_standalone_connect_to_closed_port_errors() {
        let cfg = RedisConfig::new(RedisMode::Standalone {
            url: "rediss://127.0.0.1:1/".to_string(),
        });
        let err = RedisClient::connect(cfg).await.err();
        assert!(matches!(err, Some(ShoveError::Connection(_))), "{err:?}");
    }

    #[tokio::test]
    async fn rediss_cluster_connect_to_closed_port_errors() {
        let cfg = RedisConfig::new(RedisMode::Cluster {
            urls: vec!["rediss://127.0.0.1:1/".to_string()],
        });
        let err = RedisClient::connect(cfg).await.err();
        assert!(matches!(err, Some(ShoveError::Connection(_))), "{err:?}");
    }

    #[test]
    fn is_tls_url_matches_tls_schemes_only() {
        assert!(is_tls_url("rediss://127.0.0.1:6379/"));
        assert!(is_tls_url("valkeys://127.0.0.1:6379/"));
        assert!(!is_tls_url("redis://127.0.0.1:6379/"));
        assert!(!is_tls_url("not a url"));
    }

    #[test]
    fn ensure_crypto_provider_keeps_an_application_installed_default() {
        default_provider().install_default().unwrap();
        let before = Arc::as_ptr(CryptoProvider::get_default().unwrap());
        ensure_crypto_provider();
        let after = Arc::as_ptr(CryptoProvider::get_default().unwrap());
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn plain_redis_connect_does_not_install_a_provider() {
        let cfg = RedisConfig::new(RedisMode::Standalone {
            url: "redis://127.0.0.1:1/".to_string(),
        });
        assert!(RedisClient::connect(cfg).await.is_err());
        assert!(CryptoProvider::get_default().is_none());
    }
}
