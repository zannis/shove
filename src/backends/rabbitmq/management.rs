use std::future::Future;

use crate::error::ShoveError;

#[derive(Debug, Clone)]
pub struct ManagementConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    /// URL-encoded vhost, default `"%2F"` for `"/"`
    pub vhost: String,
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
    fn get_queue_stats(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<QueueStats, ShoveError>> + Send;
}

/// HTTP client that talks to the RabbitMQ Management Plugin REST API.
pub struct ManagementClient {
    http: reqwest::Client,
    config: ManagementConfig,
}

impl ManagementClient {
    pub fn new(config: ManagementConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }
}

impl QueueStatsProvider for ManagementClient {
    fn get_queue_stats(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<QueueStats, ShoveError>> + Send {
        // URL-encode the queue name: replace `/` with `%2F`.
        let encoded_queue = queue.replace('/', "%2F");
        let url = format!(
            "{}/api/queues/{}/{}",
            self.config.base_url, self.config.vhost, encoded_queue
        );
        let request = self
            .http
            .get(&url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .build();

        let http = self.http.clone();

        async move {
            let req = request.map_err(|e| {
                ShoveError::Connection(format!("failed to build management API request: {e}"))
            })?;

            let response = http.execute(req).await.map_err(|e| {
                ShoveError::Connection(format!("management API request failed: {e}"))
            })?;

            if !response.status().is_success() {
                let status = response.status();
                return Err(ShoveError::Connection(format!(
                    "management API returned non-success status {status} for queue {queue}"
                )));
            }

            let stats = response.json::<QueueStats>().await.map_err(|e| {
                ShoveError::Connection(format!(
                    "failed to deserialize management API response for queue {queue}: {e}"
                ))
            })?;

            Ok(stats)
        }
    }
}
