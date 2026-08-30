use async_nats::connection::State;
use async_nats::jetstream;
use futures_util::StreamExt;
use std::fmt;
use std::path::PathBuf;
use std::process;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::ShoveError;
use crate::error::Result;
use crate::retry::Backoff;

#[must_use]
pub struct NatsConfig {
    pub url: String,
    /// Path to a PEM-encoded CA certificate for verifying the server's TLS certificate.
    pub tls_ca_cert: Option<PathBuf>,
    /// Path to a PEM-encoded client certificate for mutual TLS.
    pub tls_client_cert: Option<PathBuf>,
    /// Path to a PEM-encoded private key matching `tls_client_cert`.
    pub tls_client_key: Option<PathBuf>,
    /// Plain-text username for NATS user/password authentication.
    pub username: Option<String>,
    /// Plain-text password for NATS user/password authentication.
    pub password: Option<String>,
    /// Static token for NATS token authentication.
    pub token: Option<String>,
    /// NKey seed string for NKey-based authentication.
    pub nkey_seed: Option<String>,
    /// Path to a NATS `.creds` file (JWT + NKey) for credentials-based authentication.
    pub creds_file: Option<PathBuf>,
    /// Send static username/password or token credentials over a plaintext
    /// `nats://` connection. Default: `false`.
    ///
    /// **For development use only.** NATS sends these credentials in the
    /// CONNECT protocol, so a plaintext transport exposes them to any network
    /// observer. Production deployments should use a `tls://` or `nats+tls://`
    /// endpoint rather than setting this.
    pub allow_plaintext_credentials: bool,
}

impl NatsConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            username: None,
            password: None,
            token: None,
            nkey_seed: None,
            creds_file: None,
            allow_plaintext_credentials: false,
        }
    }

    /// URL of the NATS server this config connects to.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Default for NatsConfig {
    /// Default NATS endpoint for local development.
    fn default() -> Self {
        Self::new("nats://localhost:4222")
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsConfig")
            .field("url", &redact_url_credentials(&self.url))
            .field("tls_ca_cert", &self.tls_ca_cert)
            .field("tls_client_cert", &self.tls_client_cert)
            .field("tls_client_key", &self.tls_client_key)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("nkey_seed", &self.nkey_seed.as_ref().map(|_| "<redacted>"))
            .field("creds_file", &self.creds_file)
            .field(
                "allow_plaintext_credentials",
                &self.allow_plaintext_credentials,
            )
            .finish()
    }
}

#[derive(Clone)]
pub struct NatsClient {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    shutdown_token: CancellationToken,
}

const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// Returns `true` when any TLS option (CA cert, client cert/key) is set.
fn has_tls_options(config: &NatsConfig) -> bool {
    config.tls_ca_cert.is_some()
        || config.tls_client_cert.is_some()
        || config.tls_client_key.is_some()
}

/// Redact URL userinfo before a NATS endpoint reaches Debug or an error that
/// callers may log. NATS URLs commonly carry `user:password` or token auth in
/// this position, so the raw value must never be included in diagnostics.
fn redact_url_credentials(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return if url.contains('@') {
            "***".to_string()
        } else {
            url.to_string()
        };
    };
    let Some(at_pos) = url_userinfo_delimiter(url) else {
        return url.to_string();
    };
    format!("{}://***@{}", &url[..scheme_end], &url[at_pos + 1..])
}

/// Return the absolute position of the final `@` in the URL authority. An `@`
/// in the path, query, or fragment is not userinfo.
fn url_userinfo_delimiter(url: &str) -> Option<usize> {
    let scheme_end = url.find("://")?;
    let authority_start = scheme_end + 3;
    let rest = &url[authority_start..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..authority_end]
        .rfind('@')
        .map(|offset| authority_start + offset)
}

/// Returns `true` when the URL scheme requests an encrypted transport.
///
/// Both `tls://` and `nats+tls://` are accepted; `nats://` is plaintext.
fn url_scheme_is_tls(url: &str) -> bool {
    url.starts_with("tls://") || url.starts_with("nats+tls://")
}

/// Reject transport/auth combinations that would silently discard TLS options
/// or disclose reusable credentials on the wire.
fn validate_transport_security(config: &NatsConfig) -> Result<()> {
    if has_tls_options(config) && !url_scheme_is_tls(&config.url) {
        return Err(ShoveError::Connection(format!(
            "TLS options are configured but NATS URL '{}' uses a plaintext scheme; \
             change the URL scheme to tls:// or nats+tls:// to prevent silent downgrade",
            redact_url_credentials(&config.url)
        )));
    }

    let has_static_credentials = (config.username.is_some() && config.password.is_some())
        || config.token.is_some()
        || url_userinfo_delimiter(&config.url).is_some();
    if has_static_credentials
        && !url_scheme_is_tls(&config.url)
        && !config.allow_plaintext_credentials
    {
        return Err(ShoveError::Connection(
            "NATS username/password and token credentials require TLS: use a tls:// or \
             nats+tls:// URL. Sending reusable credentials over plaintext exposes them to \
             network observers. For local development only, set \
             NatsConfig::allow_plaintext_credentials = true."
                .into(),
        ));
    }

    Ok(())
}

impl NatsClient {
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        validate_transport_security(config)?;

        let client_name = format!("shove-rs-{}", process::id());
        let mut opts = async_nats::ConnectOptions::new().name(client_name);

        if let Some(ca) = &config.tls_ca_cert {
            opts = opts.add_root_certificates(ca.clone());
        }
        if let (Some(cert), Some(key)) = (&config.tls_client_cert, &config.tls_client_key) {
            opts = opts.add_client_certificate(cert.clone(), key.clone());
        }
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            opts = opts.user_and_password(user.clone(), pass.clone());
        } else if let Some(token) = &config.token {
            opts = opts.token(token.clone());
        } else if let Some(seed) = &config.nkey_seed {
            opts = opts.nkey(seed.clone());
        } else if let Some(creds) = &config.creds_file {
            opts = opts.credentials_file(creds).await.map_err(|e| {
                ShoveError::Connection(format!("failed to load NATS credentials: {e}"))
            })?;
        }

        let client = opts
            .connect(&config.url)
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        let jetstream = jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            shutdown_token: CancellationToken::new(),
        })
    }

    pub async fn connect_with_retry(config: &NatsConfig, max_attempts: u32) -> Result<Self> {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
        let mut attempts = 0u32;

        loop {
            attempts += 1;
            match Self::connect(config).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = backoff
                        .next()
                        .expect("backoff iterator is infinite; this is a bug");
                    tracing::warn!(
                        attempt = attempts,
                        max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "NATS connection failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub fn jetstream(&self) -> &jetstream::Context {
        &self.jetstream
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.client.connection_state(), State::Connected)
    }

    /// Liveness check. Subscribes to a unique inbox subject, publishes to it,
    /// and awaits the echoed message — a genuine server round-trip that proves
    /// the NATS broker is processing protocol messages (not just accepting TCP).
    ///
    /// Bounded by `timeout`. Returns `Err(ShoveError::Connection)` on timeout,
    /// subscription error, publish error, or if the subscription closes before
    /// the echo arrives.
    pub(super) async fn ping(&self, timeout: std::time::Duration) -> Result<()> {
        if self.shutdown_token.is_cancelled() {
            return Err(ShoveError::Connection("client is shut down".into()));
        }
        let client = self.client.clone();
        let fut = async move {
            let inbox = client.new_inbox();
            let mut sub = client
                .subscribe(inbox.clone())
                .await
                .map_err(|e| ShoveError::Connection(format!("nats ping subscribe failed: {e}")))?;
            // Auto-unsubscribe after one delivery so a probe timeout cannot
            // leak a long-lived subscription on the server. Without this the
            // Subscriber's Drop impl schedules UNSUB via tokio::spawn
            // (fire-and-forget), which can accumulate under repeated timeouts.
            sub.unsubscribe_after(1).await.map_err(|e| {
                ShoveError::Connection(format!("nats ping unsubscribe_after failed: {e}"))
            })?;
            // Flush so the SUB and UNSUB-with-max frames are on the wire
            // before the PUB races ahead of them. Without this, the server may
            // receive PUB before SUB and drop the message (no interest).
            client
                .flush()
                .await
                .map_err(|e| ShoveError::Connection(format!("nats ping flush failed: {e}")))?;
            client
                .publish(inbox, bytes::Bytes::from_static(b"ping"))
                .await
                .map_err(|e| ShoveError::Connection(format!("nats ping publish failed: {e}")))?;
            match sub.next().await {
                Some(_) => Ok::<(), ShoveError>(()),
                None => Err(ShoveError::Connection(
                    "nats ping subscription closed before echo arrived".into(),
                )),
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| ShoveError::Connection(format!("nats ping timed out after {timeout:?}")))?
    }

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        let _ = self.client.drain().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_localhost() {
        let cfg = NatsConfig::default();
        assert!(cfg.url().contains("localhost:4222"));
    }

    #[test]
    fn new_config_has_all_options_none() {
        let cfg = NatsConfig::new("nats://localhost:4222");
        assert!(cfg.tls_ca_cert.is_none());
        assert!(cfg.tls_client_cert.is_none());
        assert!(cfg.tls_client_key.is_none());
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
        assert!(cfg.token.is_none());
        assert!(cfg.nkey_seed.is_none());
        assert!(cfg.creds_file.is_none());
        assert!(!cfg.allow_plaintext_credentials);
    }

    #[test]
    fn debug_redacts_url_credentials() {
        let cfg = NatsConfig::new("nats://user:secret@broker.example.com:4222");
        let debug = format!("{cfg:?}");
        assert!(
            !debug.contains("secret"),
            "password must not appear in debug output"
        );
        assert!(
            debug.contains("***@broker.example.com"),
            "host must remain visible"
        );
    }

    #[test]
    fn debug_url_without_credentials_is_unchanged() {
        let cfg = NatsConfig::new("nats://broker.example.com:4222");
        let debug = format!("{cfg:?}");
        assert!(debug.contains("broker.example.com"));
    }

    #[test]
    fn debug_redacts_token_and_nkey() {
        let mut cfg = NatsConfig::new("nats://localhost:4222");
        cfg.token = Some("super-secret-token".into());
        cfg.nkey_seed = Some("SUANKEY...".into());
        let debug = format!("{cfg:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must be redacted"
        );
        assert!(!debug.contains("SUANKEY"), "nkey seed must be redacted");
        assert!(
            debug.contains("<redacted>"),
            "redacted sentinel must appear"
        );
    }

    #[test]
    fn connect_with_retry_backoff_is_infinite() {
        // connect_with_retry calls backoff.next().expect("backoff iterator is infinite; this is a bug").
        // Verify that the Backoff used there (100ms initial, 5s max) never yields None.
        let delays: Vec<_> = Backoff::new(
            std::time::Duration::from_millis(100),
            std::time::Duration::from_secs(5),
        )
        .take(200)
        .collect();
        assert_eq!(delays.len(), 200, "Backoff must never return None");
    }

    // --- sec-2: username must be redacted in Debug output ---

    #[test]
    fn debug_redacts_username() {
        let mut cfg = NatsConfig::new("nats://localhost:4222");
        cfg.username = Some("alice".into());
        cfg.password = Some("sentinel-pw".into());
        let debug = format!("{cfg:?}");
        assert!(
            !debug.contains("alice"),
            "username must not appear in debug output"
        );
        assert!(
            !debug.contains("sentinel-pw"),
            "password must not appear in debug output"
        );
        assert!(
            debug.contains("password: Some(\"<redacted>\")"),
            "password field must appear as <redacted>: {debug}"
        );
        assert!(
            debug.contains("<redacted>"),
            "redacted sentinel must appear"
        );
    }

    // --- sec-7: TLS options + plaintext URL scheme must be rejected ---

    #[test]
    fn tls_options_with_plain_url_is_rejected() {
        let mut cfg = NatsConfig::new("nats://broker.example.com:4222");
        cfg.tls_ca_cert = Some(std::path::PathBuf::from("/etc/certs/ca.pem"));
        assert!(
            has_tls_options(&cfg),
            "config with ca_cert must be detected as having TLS options"
        );
        assert!(
            !url_scheme_is_tls(&cfg.url),
            "nats:// must not be considered a TLS scheme"
        );
        assert!(
            validate_transport_security(&cfg).is_err(),
            "TLS options on a plaintext URL must be rejected"
        );
    }

    #[test]
    fn tls_plaintext_error_redacts_url_credentials() {
        let mut cfg = NatsConfig::new("nats://alice:sentinel-password@broker.example.com:4222");
        cfg.tls_ca_cert = Some(std::path::PathBuf::from("/etc/certs/ca.pem"));

        let err = validate_transport_security(&cfg)
            .expect_err("TLS options with a plaintext URL must fail before connecting");
        let rendered = err.to_string();

        assert!(!rendered.contains("alice"), "username leaked: {rendered}");
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked: {rendered}"
        );
        assert!(
            rendered.contains("nats://***@broker.example.com:4222"),
            "redacted endpoint should remain actionable: {rendered}"
        );
    }

    #[test]
    fn plaintext_username_and_password_are_rejected() {
        let mut cfg = NatsConfig::new("nats://broker.example.com:4222");
        cfg.username = Some("alice".into());
        cfg.password = Some("sentinel-password".into());

        let err = validate_transport_security(&cfg)
            .expect_err("reusable credentials over plaintext must be rejected");
        let rendered = err.to_string();
        assert!(
            rendered.contains("require TLS"),
            "unexpected error: {rendered}"
        );
        assert!(!rendered.contains("alice"), "username leaked: {rendered}");
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked: {rendered}"
        );
    }

    #[test]
    fn plaintext_token_and_url_userinfo_are_rejected() {
        let mut token_cfg = NatsConfig::new("nats://broker.example.com:4222");
        token_cfg.token = Some("sentinel-token".into());
        assert!(validate_transport_security(&token_cfg).is_err());

        let url_cfg = NatsConfig::new("nats://alice:sentinel-password@broker.example.com:4222");
        assert!(validate_transport_security(&url_cfg).is_err());
    }

    /// Every `NatsConfig` field is public so callers can build one with struct
    /// update syntax. A private field would still compile in-crate and only
    /// break downstream users, so pin the property here.
    #[test]
    fn config_is_constructible_with_struct_update_syntax() {
        let cfg = NatsConfig {
            url: "tls://broker.example.com:4222".to_string(),
            token: Some("t".into()),
            ..Default::default()
        };

        assert!(validate_transport_security(&cfg).is_ok());
    }

    #[test]
    fn explicit_plaintext_credential_override_is_accepted() {
        let mut cfg = NatsConfig::new("nats://broker.example.com:4222");
        cfg.token = Some("development-token".into());
        cfg.allow_plaintext_credentials = true;

        assert!(validate_transport_security(&cfg).is_ok());
    }

    #[test]
    fn at_sign_outside_authority_is_not_treated_as_userinfo() {
        let cfg = NatsConfig::new("nats://broker.example.com/path?contact=ops@example.com");
        assert!(url_userinfo_delimiter(&cfg.url).is_none());
        assert_eq!(redact_url_credentials(&cfg.url), cfg.url);
    }

    #[test]
    fn tls_scheme_is_accepted() {
        assert!(url_scheme_is_tls("tls://broker.example.com:4222"));
        assert!(url_scheme_is_tls("nats+tls://broker.example.com:4222"));
        assert!(!url_scheme_is_tls("nats://broker.example.com:4222"));
    }

    #[test]
    fn no_tls_options_with_plain_url_is_not_flagged() {
        let cfg = NatsConfig::new("nats://broker.example.com:4222");
        assert!(!has_tls_options(&cfg));
    }
}
