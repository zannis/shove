use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use tracing::debug;
use url::Url;

use crate::error::{Result, ShoveError};
use crate::metrics;

#[derive(Clone)]
pub struct ManagementConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    /// Raw (unencoded) vhost name. Default: `"/"` (the default RabbitMQ virtual host).
    ///
    /// Pass the vhost exactly as it appears in RabbitMQ — e.g. `"my-vhost"` or
    /// `"/"`. Percent-encoding is applied automatically when the management URL
    /// is constructed.
    pub vhost: String,
    /// Skip TLS certificate verification (insecure; only for testing/dev environments).
    pub tls_skip_verify: bool,
    /// Path to a PEM-encoded CA certificate to add as a trusted root.
    pub tls_ca_cert: Option<PathBuf>,
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
            vhost: "/".into(),
            tls_skip_verify: false,
            tls_ca_cert: None,
        }
    }

    pub fn with_vhost(mut self, vhost: impl Into<String>) -> Self {
        self.vhost = vhost.into();
        self
    }

    pub fn with_tls_skip_verify(mut self) -> Self {
        self.tls_skip_verify = true;
        self
    }

    pub fn with_tls_ca_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_ca_cert = Some(path.into());
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
    /// Parsed and validated at construction. Stored to avoid re-parsing on
    /// every request and to ensure the scheme / userinfo checks are done once.
    base_url: Url,
}

impl ManagementClient {
    pub fn new(config: ManagementConfig) -> Self {
        let base_url = Url::parse(&config.base_url).unwrap_or_else(|e| {
            panic!(
                "ManagementConfig::base_url is not a valid URL ({e}): {:?}",
                config.base_url
            )
        });

        let scheme = base_url.scheme();
        assert!(
            scheme == "http" || scheme == "https",
            "ManagementConfig::base_url scheme must be \"http\" or \"https\", got {scheme:?}",
        );

        assert!(
            base_url.username().is_empty() && base_url.password().is_none(),
            "ManagementConfig::base_url must not embed credentials (found userinfo in {:?})",
            config.base_url,
        );

        let mut builder = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .danger_accept_invalid_certs(config.tls_skip_verify);

        if let Some(ca_path) = &config.tls_ca_cert {
            let pem = std::fs::read(ca_path).expect("failed to read management API CA certificate");
            let cert = reqwest::Certificate::from_pem(&pem)
                .expect("failed to parse management API CA certificate");
            builder = builder.add_root_certificate(cert);
        }

        let http = builder.build().expect("failed to build HTTP client");
        Self {
            http,
            config,
            base_url,
        }
    }
}

/// Build the management API URL for a specific vhost + queue.
///
/// Uses [`Url::path_segments_mut`] so each component is percent-encoded as an
/// opaque path segment — `/` in a vhost or queue name becomes `%2F` and cannot
/// be confused with a path separator.
///
/// Dot-segment names (`"."` and `".."`) are rejected before URL construction.
/// `path_segments_mut` does not encode `.`/`..`, and the url crate silently
/// normalises them away during serialisation, which would silently corrupt the
/// path (RFC 3986 §5.2.4). Checking the raw inputs avoids that silent
/// corruption.  A string like `"../nodes"` is safe — the interior `/` gets
/// encoded to `%2F`, making it one opaque segment.
fn build_queue_url(base: &Url, vhost: &str, queue: &str) -> Result<Url> {
    for (label, value) in [("vhost", vhost), ("queue", queue)] {
        if value == ".." || value == "." {
            return Err(ShoveError::Topology(format!(
                "management API {label} must not be a dot-segment; \
                 got {value:?} — this would silently corrupt the request URL"
            )));
        }
    }

    let mut url = base.clone();

    // `path_segments_mut` fails only for cannot-be-a-base URLs (e.g. `data:`).
    // We validated http/https at construction, so this should never fail.
    url.path_segments_mut()
        .expect("base_url is a hierarchical URL (validated at ManagementClient::new)")
        .extend(["api", "queues", vhost, queue]);

    Ok(url)
}

impl QueueStatsProvider for ManagementClient {
    async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let url = build_queue_url(&self.base_url, &self.config.vhost, queue)?;

        let request = self
            .http
            .get(url)
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

    // ---------------------------------------------------------------------------
    // ManagementConfig
    // ---------------------------------------------------------------------------

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
        // Default vhost is the raw "/" — encoding happens at URL construction time.
        assert_eq!(config.vhost, "/");
    }

    #[test]
    fn management_config_with_vhost() {
        let config = ManagementConfig::new("http://localhost:15672", "guest", "guest")
            .with_vhost("my-vhost");
        assert_eq!(config.vhost, "my-vhost");
    }

    // ---------------------------------------------------------------------------
    // QueueStats
    // ---------------------------------------------------------------------------

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

    // ---------------------------------------------------------------------------
    // URL construction — build_queue_url
    // ---------------------------------------------------------------------------

    fn base(url: &str) -> Url {
        Url::parse(url).unwrap()
    }

    #[test]
    fn url_default_vhost_encoded_as_slash() {
        // Raw "/" vhost must be percent-encoded to "%2F" in the URL path.
        let url = build_queue_url(&base("http://localhost:15672"), "/", "my-queue").unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:15672/api/queues/%2F/my-queue"
        );
    }

    #[test]
    fn url_named_vhost_passes_through() {
        let url = build_queue_url(&base("http://localhost:15672"), "staging", "orders").unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:15672/api/queues/staging/orders"
        );
    }

    #[test]
    fn url_vhost_with_slash_encoded() {
        // A vhost name that legitimately contains "/" must be encoded, not
        // interpreted as a path separator.
        let url = build_queue_url(&base("http://localhost:15672"), "ns/vhost", "q").unwrap();
        assert!(
            url.as_str().contains("ns%2Fvhost"),
            "slash in vhost not encoded: {url}"
        );
    }

    #[test]
    fn url_queue_with_slash_encoded() {
        // Queue names may legally contain "/" (e.g. namespaced queues); each
        // slash must be encoded so it doesn't introduce an extra path segment.
        let url = build_queue_url(&base("http://localhost:15672"), "/", "ns/queue").unwrap();
        assert!(
            url.as_str().contains("ns%2Fqueue"),
            "slash in queue not encoded: {url}"
        );
    }

    #[test]
    fn url_queue_with_hash_encoded() {
        let url = build_queue_url(&base("http://localhost:15672"), "/", "q#1").unwrap();
        assert!(url.as_str().contains("q%231"), "# not encoded: {url}");
    }

    #[test]
    fn url_queue_with_question_mark_encoded() {
        let url = build_queue_url(&base("http://localhost:15672"), "/", "q?1").unwrap();
        assert!(url.as_str().contains("q%3F1"), "? not encoded: {url}");
    }

    // --- dot-segment traversal rejection ---

    #[test]
    fn url_rejects_dotdot_vhost() {
        // ".." as a path segment causes HTTP servers to navigate to the parent
        // directory, reaching unrelated management endpoints.
        let result = build_queue_url(&base("http://localhost:15672"), "..", "queue");
        assert!(
            result.is_err(),
            "expected error for '..' vhost, got: {result:?}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("dot-segment"),
            "error should mention dot-segment: {msg}"
        );
    }

    #[test]
    fn url_rejects_dot_vhost() {
        let result = build_queue_url(&base("http://localhost:15672"), ".", "queue");
        assert!(result.is_err(), "expected error for '.' vhost");
    }

    #[test]
    fn url_rejects_dotdot_queue() {
        let result = build_queue_url(&base("http://localhost:15672"), "/", "..");
        assert!(result.is_err(), "expected error for '..' queue");
    }

    #[test]
    fn url_slash_in_vhost_does_not_traverse() {
        // "../nodes" contains a slash which path_segments_mut encodes as %2F,
        // so the full string becomes "..%2Fnodes" — one opaque segment, not a
        // traversal. This must succeed (the vhost is odd but not dangerous).
        let result = build_queue_url(&base("http://localhost:15672"), "../nodes", "q");
        assert!(
            result.is_ok(),
            "unexpected error for '../nodes' vhost: {result:?}"
        );
        let url = result.unwrap();
        assert!(
            url.as_str().contains("..%2Fnodes"),
            "slash not encoded within segment: {url}"
        );
    }

    // --- base_url validation (panics) ---

    #[test]
    #[should_panic(expected = "scheme must be")]
    fn management_client_rejects_file_scheme() {
        ManagementClient::new(ManagementConfig::new("file:///etc/passwd", "u", "p"));
    }

    #[test]
    #[should_panic(expected = "scheme must be")]
    fn management_client_rejects_ftp_scheme() {
        ManagementClient::new(ManagementConfig::new("ftp://host/path", "u", "p"));
    }

    #[test]
    #[should_panic(expected = "must not embed credentials")]
    fn management_client_rejects_embedded_userinfo() {
        ManagementClient::new(ManagementConfig::new(
            "http://admin:secret@localhost:15672",
            "u",
            "p",
        ));
    }

    #[test]
    #[should_panic(expected = "must not embed credentials")]
    fn management_client_rejects_embedded_username_only() {
        ManagementClient::new(ManagementConfig::new(
            "http://admin@localhost:15672",
            "u",
            "p",
        ));
    }

    #[test]
    #[should_panic(expected = "not a valid URL")]
    fn management_client_rejects_invalid_url() {
        ManagementClient::new(ManagementConfig::new("not a url", "u", "p"));
    }
}
