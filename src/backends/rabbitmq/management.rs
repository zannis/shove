use std::future::Future;
use std::time::Duration;

use tracing::debug;

use crate::error::{Result, ShoveError};
use crate::metrics;

#[derive(Clone)]
pub struct ManagementConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    /// URL-encoded vhost, default `"%2F"` for `"/"`
    pub vhost: String,
}

impl std::fmt::Debug for ManagementConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagementConfig")
            .field("base_url", &self.base_url)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("vhost", &self.vhost)
            .finish()
    }
}

impl ManagementConfig {
    pub fn new(
        base_url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            username: username.into(),
            password: password.into(),
            vhost: "%2F".into(),
        }
    }

    pub fn with_vhost(mut self, vhost: impl Into<String>) -> Self {
        self.vhost = vhost.into();
        self
    }
}

/// Subset of queue statistics returned by the RabbitMQ Management API.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct QueueStats {
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub consumers: u64,
}

/// Abstraction over the RabbitMQ Management HTTP API for fetching queue stats.
///
/// Using a trait here allows injecting a mock implementation in tests.
pub trait QueueStatsProvider: Send + Sync {
    fn get_queue_stats(&self, queue: &str) -> impl Future<Output = Result<QueueStats>> + Send;
}

/// HTTP client that talks to the RabbitMQ Management Plugin REST API.
pub struct ManagementClient {
    http: reqwest::Client,
    config: ManagementConfig,
}

impl ManagementClient {
    pub fn new(config: ManagementConfig) -> Self {
        let http = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self { http, config }
    }
}

impl QueueStatsProvider for ManagementClient {
    async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let encoded_queue = queue.replace('/', "%2F");
        let url = format!(
            "{}/api/queues/{}/{}",
            self.config.base_url, self.config.vhost, encoded_queue
        );
        let request = self
            .http
            .get(&url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .build()
            .map_err(|e| {
                ShoveError::Topology(format!("failed to build management API request: {e}"))
            })?;

        let response =
            self.http.execute(request).await.map_err(|e| {
                ShoveError::Connection(format!("management API request failed: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Connection(format!(
                "management API returned non-success status {status} for queue {queue}"
            )));
        }

        let stats = response.json::<QueueStats>().await.map_err(|e| {
            ShoveError::Topology(format!(
                "failed to deserialize management API response for queue {queue}: {e}"
            ))
        })?;

        debug!(
            queue,
            messages_ready = stats.messages_ready,
            messages_unacknowledged = stats.messages_unacknowledged,
            consumers = stats.consumers,
            "fetched queue stats"
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_config_debug_redacts_password() {
        let config = ManagementConfig::new("http://localhost:15672", "admin", "s3cret!");
        let debug_output = format!("{config:?}");
        assert!(!debug_output.contains("s3cret!"));
        assert!(debug_output.contains("<redacted>"));
        assert!(debug_output.contains("admin"));
        assert!(debug_output.contains("localhost"));
    }

    #[test]
    fn management_config_defaults() {
        let config = ManagementConfig::new("http://localhost:15672", "guest", "guest");
        assert_eq!(config.base_url, "http://localhost:15672");
        assert_eq!(config.username, "guest");
        assert_eq!(config.password, "guest");
        assert_eq!(config.vhost, "%2F");
    }

    #[test]
    fn management_config_with_vhost() {
        let config = ManagementConfig::new("http://localhost:15672", "guest", "guest")
            .with_vhost("my-vhost");
        assert_eq!(config.vhost, "my-vhost");
    }

    #[test]
    fn queue_stats_defaults() {
        let stats = QueueStats::default();
        assert_eq!(stats.messages_ready, 0);
        assert_eq!(stats.messages_unacknowledged, 0);
        assert_eq!(stats.consumers, 0);
    }

    #[test]
    fn queue_stats_deserialize_full() {
        let json = r#"{"messages_ready": 42, "messages_unacknowledged": 7, "consumers": 3}"#;
        let stats: QueueStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.messages_ready, 42);
        assert_eq!(stats.messages_unacknowledged, 7);
        assert_eq!(stats.consumers, 3);
    }

    #[test]
    fn queue_stats_deserialize_partial() {
        let json = r#"{"messages_ready": 10}"#;
        let stats: QueueStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.messages_ready, 10);
        assert_eq!(stats.messages_unacknowledged, 0);
        assert_eq!(stats.consumers, 0);
    }

    #[test]
    fn queue_stats_deserialize_empty_object() {
        let json = r#"{}"#;
        let stats: QueueStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.messages_ready, 0);
        assert_eq!(stats.messages_unacknowledged, 0);
        assert_eq!(stats.consumers, 0);
    }

    #[test]
    fn queue_stats_deserialize_extra_fields_ignored() {
        let json = r#"{"messages_ready": 5, "node": "rabbit@host", "state": "running"}"#;
        let stats: QueueStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.messages_ready, 5);
    }
}
