//! Redis client abstraction supporting standalone, TLS (`rediss://`), and cluster modes.

#![allow(dead_code)]

use std::sync::Arc;

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

// ---------------------------------------------------------------------------
// Internal connection enum
// ---------------------------------------------------------------------------

/// A single Redis connection, abstracting over standalone vs cluster transports.
pub(crate) enum RedisConnection {
    /// Multiplexed standalone connection – safe for BLOCK commands with finite timeouts (BLOCK 2000).
    Standalone(redis::aio::MultiplexedConnection),
    /// Cluster connection – safe to share; BLOCK commands work the same way.
    Cluster(redis::cluster_async::ClusterConnection),
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
    Cluster(redis::cluster::ClusterClient),
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
                    return Err(ShoveError::Connection("cluster URLs must not be empty".into()));
                }
                let nodes: Vec<&str> = urls.iter().map(String::as_str).collect();
                let client = redis::cluster::ClusterClient::new(nodes)
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                // Eagerly verify connectivity.
                client
                    .get_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(format!("cluster ping failed: {e}")))?;
                ClientInner::Cluster(client)
            }
        };

        Ok(Self {
            inner: Arc::new(inner),
            group,
        })
    }

    /// Return a multiplexed (shared) connection suitable for non-blocking operations
    /// such as XADD, XACK, ZADD, and XLEN.
    pub(super) async fn multiplexed_conn(&self) -> Result<RedisConnection> {
        match self.inner.as_ref() {
            ClientInner::Standalone(client) => client
                .get_multiplexed_async_connection()
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
    /// (e.g. XREADGROUP with `BLOCK`).
    ///
    /// For standalone, `MultiplexedConnection` is safe for BLOCK commands with finite timeouts
    /// (BLOCK 2000). For cluster, each consumer task gets its own `ClusterConnection` handle.
    pub(super) async fn dedicated_conn(&self) -> Result<RedisConnection> {
        self.multiplexed_conn().await
    }

    /// The consumer group name shared by all consumers on this client.
    pub(super) fn group(&self) -> &str {
        &self.group
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
}
