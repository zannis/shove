//! Redis client abstraction supporting standalone, TLS (`rediss://`), and cluster modes.

use std::sync::Arc;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::cluster::{ClusterClient, ClusterConfig};
use redis::cluster_async::ClusterConnection;

use crate::error::{Result, ShoveError};

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

/// Configuration for [`RedisClient`].
pub struct RedisConfig {
    /// Connection mode: standalone or cluster.
    pub mode: RedisMode,
    /// Consumer group name. All consumers of a topic share this group so that
    /// each message is delivered to exactly one consumer. Defaults to `"shove"`.
    pub group: Option<String>,
    /// Per-command response timeout. Defaults to [`DEFAULT_RESPONSE_TIMEOUT`].
    /// Must exceed [`BLOCK_MS`] (2 s) — enforced by
    /// [`with_response_timeout`](Self::with_response_timeout). The redis-rs
    /// default of 500 ms is too tight for both XREADGROUP BLOCK and batched
    /// XADDs under heavy concurrent load.
    pub response_timeout: Duration,
    /// Connection-establishment timeout. Defaults to
    /// [`DEFAULT_CONNECTION_TIMEOUT`]. Tune via
    /// [`with_connection_timeout`](Self::with_connection_timeout).
    pub connection_timeout: Duration,
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
    /// Execute `cmd` and deserialize the response into `T`.
    pub(crate) async fn query<T: redis::FromRedisValue + Send>(
        &mut self,
        cmd: &mut redis::Cmd,
    ) -> Result<T> {
        match self {
            RedisConnection::Standalone(conn) => cmd
                .query_async(conn)
                .await
                .map_err(|e| ShoveError::Connection(e.to_string())),
            RedisConnection::Cluster(conn) => cmd
                .query_async(conn)
                .await
                .map_err(|e| ShoveError::Connection(e.to_string())),
        }
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
}

impl RedisClient {
    /// Build a [`RedisClient`] and eagerly verify connectivity by opening a
    /// test connection.
    pub(super) async fn connect(config: RedisConfig) -> Result<Self> {
        let group = config.resolved_group().to_owned();
        let response_timeout = config.response_timeout;
        let connection_timeout = config.connection_timeout;

        let inner = match config.mode {
            RedisMode::Standalone { url } => {
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

    /// The consumer group name shared by all consumers on this client.
    pub(super) fn group(&self) -> &str {
        &self.group
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
// Tests (no running Redis required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
}
