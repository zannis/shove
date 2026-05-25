//! Redis client abstraction supporting standalone, TLS (`rediss://`), and cluster modes.

use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use redis::cluster::{ClusterClient, ClusterConfig};
use redis::cluster_async::ClusterConnection;

use crate::error::{Result, ShoveError};

use super::constants::DEFAULT_GROUP;

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

/// Configuration for [`RedisClient`].
pub struct RedisConfig {
    /// Connection mode: standalone or cluster.
    pub mode: RedisMode,
    /// Consumer group name. All consumers of a topic share this group so that
    /// each message is delivered to exactly one consumer. Defaults to `"shove"`.
    pub group: Option<String>,
}

impl RedisConfig {
    /// Return the resolved consumer group name, defaulting to `"shove"` if not set.
    pub fn resolved_group(&self) -> &str {
        self.group.as_deref().unwrap_or(DEFAULT_GROUP)
    }
}

/// Build an [`AsyncConnectionConfig`](redis::AsyncConnectionConfig) with both
/// `response_timeout` and `connection_timeout` disabled.
///
/// shove relies on Redis BLOCK semantics (XREADGROUP BLOCK 2000) and pushes
/// large batched publishes under heavy concurrent load; the redis-rs defaults
/// (500 ms response, 1 s connect) misfire in both situations. Disabling both
/// timeouts uniformly is correct for every shove code path.
fn no_timeout_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_response_timeout(None)
        .set_connection_timeout(None)
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
}

impl RedisClient {
    /// Build a [`RedisClient`] and eagerly verify connectivity by opening a
    /// test connection.
    pub(super) async fn connect(config: RedisConfig) -> Result<Self> {
        let group = config.resolved_group().to_owned();

        let inner = match config.mode {
            RedisMode::Standalone { url } => {
                let client = redis::Client::open(url.as_str())
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                client
                    .get_multiplexed_async_connection()
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
                    .get_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(format!("cluster ping failed: {e}")))?;
                ClientInner::Cluster(client)
            }
        };

        let client = Self {
            inner: Arc::new(inner),
            group,
        };

        let mut conn = client.multiplexed_conn().await?;
        check_min_version(&mut conn).await?;

        Ok(client)
    }

    /// Return a multiplexed (shared) connection suitable for non-blocking operations
    /// such as XADD, XACK, ZADD, and XLEN.
    ///
    /// The redis-rs defaults are `response_timeout=500 ms` and
    /// `connection_timeout=1 s`. Both are too aggressive for shove's high-fanout
    /// workloads: under heavy concurrent XREADGROUP load the server can take
    /// longer than 500 ms to respond to a batched publish, and acquiring a fresh
    /// multiplexed connection during peak traffic can exceed 1 s. Both surface
    /// as "connection error: timed out" mid-publish. We disable both timeouts
    /// here; TCP keepalive still surfaces a truly dead broker.
    pub(super) async fn multiplexed_conn(&self) -> Result<RedisConnection> {
        match self.inner.as_ref() {
            ClientInner::Standalone(client) => client
                .get_multiplexed_async_connection_with_config(&no_timeout_config())
                .await
                .map(RedisConnection::Standalone)
                .map_err(|e| ShoveError::Connection(e.to_string())),
            ClientInner::Cluster(client) => client
                .get_async_connection()
                .await
                .map(RedisConnection::Cluster)
                .map_err(|e| ShoveError::Connection(e.to_string())),
        }
    }

    /// Return a dedicated connection suitable for consumer loops that use BLOCK commands
    /// (e.g. XREADGROUP with `BLOCK 2000`).
    ///
    /// Uses the same no-timeout config as [`multiplexed_conn`](Self::multiplexed_conn)
    /// so blocking reads, batched publishes, and outcome routing all behave
    /// consistently under load.
    pub(super) async fn dedicated_conn(&self) -> Result<RedisConnection> {
        match self.inner.as_ref() {
            ClientInner::Standalone(client) => client
                .get_multiplexed_async_connection_with_config(&no_timeout_config())
                .await
                .map(RedisConnection::Standalone)
                .map_err(|e| ShoveError::Connection(e.to_string())),
            ClientInner::Cluster(client) => client
                .get_async_connection_with_config(ClusterConfig::new())
                .await
                .map(RedisConnection::Cluster)
                .map_err(|e| ShoveError::Connection(e.to_string())),
        }
    }

    /// The consumer group name shared by all consumers on this client.
    pub(super) fn group(&self) -> &str {
        &self.group
    }
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
    let info: String = conn
        .query(redis::cmd("INFO").arg("server"))
        .await
        .map_err(|e| ShoveError::Connection(format!("INFO server failed: {e}")))?;

    check_version_info(&info)
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
        };
        assert_eq!(cfg.resolved_group(), "myapp");
    }

    #[test]
    fn standalone_url_preserved() {
        let url = "rediss://user:pass@myhost:6380/".to_string();
        let config = RedisConfig {
            mode: RedisMode::Standalone { url: url.clone() },
            group: None,
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
        };
        assert!(matches!(cfg.mode, RedisMode::Cluster { .. }));
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
}
