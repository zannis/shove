//! Redis client abstraction supporting standalone, TLS (`rediss://`), and cluster modes.

// redis::aio::Connection is deprecated in 0.27 but remains the only way to get a
// non-multiplexed (dedicated) async connection required for BLOCK commands on standalone.
#![allow(deprecated)]
// Items are used by subsequent tasks (publisher, consumer, topology…).
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

// ---------------------------------------------------------------------------
// Internal connection enum
// ---------------------------------------------------------------------------

/// A single Redis connection, abstracting over standalone vs cluster transports.
pub(super) enum RedisConnection {
    /// Multiplexed standalone connection – for non-blocking ops.
    Standalone(redis::aio::MultiplexedConnection),
    /// Dedicated (non-multiplexed) standalone connection – required for BLOCK commands
    /// so they don't serialize the shared multiplexed connection.
    StandaloneDedicated(redis::aio::Connection),
    /// Cluster connection – safe to share; BLOCK commands work the same way.
    Cluster(redis::cluster_async::ClusterConnection),
}

impl RedisConnection {
    /// Execute `cmd` and deserialize the response into `T`.
    pub(super) async fn query<T: redis::FromRedisValue + Send>(
        &mut self,
        cmd: &mut redis::Cmd,
    ) -> Result<T> {
        match self {
            RedisConnection::Standalone(conn) => cmd
                .query_async(conn)
                .await
                .map_err(|e| ShoveError::Connection(e.to_string())),
            RedisConnection::StandaloneDedicated(conn) => cmd
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
        let group = config
            .group
            .unwrap_or_else(|| DEFAULT_GROUP.to_string());

        let inner = match config.mode {
            RedisMode::Standalone { url } => {
                let client = redis::Client::open(url.as_str())
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                // Eagerly verify connectivity.
                client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(format!("standalone ping failed: {e}")))?;
                ClientInner::Standalone(client)
            }
            RedisMode::Cluster { urls } => {
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
            ClientInner::Standalone(client) => {
                let conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                Ok(RedisConnection::Standalone(conn))
            }
            ClientInner::Cluster(client) => {
                let conn = client
                    .get_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                Ok(RedisConnection::Cluster(conn))
            }
        }
    }

    /// Return a dedicated (non-shared) connection suitable for consumer loops
    /// that use BLOCK commands (e.g. XREADGROUP with `BLOCK`).
    ///
    /// For standalone Redis this is a separate, non-multiplexed connection so
    /// that long-blocking calls don't block other in-flight commands.
    /// For cluster mode, `get_async_connection()` already returns an independent
    /// connection handle, so the same approach is used.
    pub(super) async fn dedicated_conn(&self) -> Result<RedisConnection> {
        match self.inner.as_ref() {
            ClientInner::Standalone(client) => {
                let conn = client
                    .get_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                Ok(RedisConnection::StandaloneDedicated(conn))
            }
            ClientInner::Cluster(client) => {
                let conn = client
                    .get_async_connection()
                    .await
                    .map_err(|e| ShoveError::Connection(e.to_string()))?;
                Ok(RedisConnection::Cluster(conn))
            }
        }
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
        let config = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: None,
        };
        let group = config.group.unwrap_or_else(|| DEFAULT_GROUP.to_string());
        assert_eq!(group, "shove");
    }

    #[test]
    fn config_custom_group() {
        let config = RedisConfig {
            mode: RedisMode::Standalone {
                url: "redis://127.0.0.1:6379/".to_string(),
            },
            group: Some("myapp".to_string()),
        };
        let group = config.group.unwrap_or_else(|| DEFAULT_GROUP.to_string());
        assert_eq!(group, "myapp");
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
