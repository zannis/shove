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
    /// observer. Production deployments should use a `tls://` or `wss://`
    /// endpoint rather than setting this.
    ///
    /// This covers the explicit credential fields only. Credentials embedded in
    /// the URL are rejected on every scheme and this flag does not re-admit
    /// them.
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
    let Some(at_pos) = url_userinfo_delimiter(url) else {
        return url.to_string();
    };
    let host_and_rest = &url[at_pos + 1..];
    match url.find("://") {
        Some(scheme_end) => format!("{}://***@{host_and_rest}", &url[..scheme_end]),
        None => format!("***@{host_and_rest}"),
    }
}

/// Return the absolute position of the final `@` in the URL authority. An `@`
/// in the path, query, or fragment is not userinfo.
///
/// A schemeless address has no `://` to anchor on, but its authority still
/// starts at byte 0 — `ServerAddr::from_str` prepends `nats://` before parsing,
/// so `user:pass@host` carries userinfo just as `nats://user:pass@host` does.
fn url_userinfo_delimiter(url: &str) -> Option<usize> {
    let authority_start = url.find("://").map_or(0, |scheme_end| scheme_end + 3);
    let rest = &url[authority_start..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..authority_end]
        .rfind('@')
        .map(|offset| authority_start + offset)
}

/// Transport security implied by a NATS URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemeSecurity {
    /// The transport is encrypted before any credential is sent.
    Encrypted,
    /// The transport is cleartext; anything sent over it is readable on the wire.
    Plaintext,
}

/// The schemes `async_nats::ServerAddr` accepts, in the order the error lists
/// them. Anything else fails to parse inside the client, so shove rejects it
/// here rather than letting a connection attempt fail with a scheme error.
const SUPPORTED_SCHEMES: &str = "nats://, tls://, ws://, wss://";

/// Classify a NATS URL by the transport its scheme selects.
///
/// This mirrors `async_nats::ServerAddr`, which is the only contract that
/// matters: it accepts exactly `nats`, `tls`, `ws` and `wss`, and rejects
/// everything else — `nats+tls://` included. Of those, `tls` and `wss` are
/// encrypted (the connector wraps `wss` in a rustls connector built from the
/// same `ConnectOptions` TLS settings), while `nats` and `ws` are cleartext.
fn classify_scheme(url: &str) -> Result<SchemeSecurity> {
    let Some((scheme, _)) = url.split_once("://") else {
        // `ServerAddr::from_str` prepends `nats://` to a schemeless address.
        return Ok(SchemeSecurity::Plaintext);
    };

    // `url::Url` lowercases the scheme while parsing, so the client accepts
    // `TLS://`. Matching case-sensitively here would reject a working URL.
    match scheme.to_ascii_lowercase().as_str() {
        "tls" | "wss" => Ok(SchemeSecurity::Encrypted),
        "nats" | "ws" => Ok(SchemeSecurity::Plaintext),
        _ => Err(ShoveError::Connection(format!(
            "NATS URL '{}' uses a scheme the client cannot connect with; \
             supported schemes are {SUPPORTED_SCHEMES}",
            redact_url_credentials(url)
        ))),
    }
}

/// Reject transport/auth combinations that would silently discard TLS options
/// or disclose reusable credentials on the wire.
fn validate_transport_security(config: &NatsConfig) -> Result<()> {
    let security = classify_scheme(&config.url)?;

    // Credentials in the URL authority are rejected on every scheme, encrypted
    // ones included. `async_nats` never puts URL userinfo into the CONNECT
    // frame — `ServerAddr::username`/`password` exist but nothing in the client
    // reads them — so these credentials do not authenticate anything. They are
    // only ever rendered: `ServerAddr` derives `Debug` over `url::Url`, whose
    // `Debug` prints the password in the clear, and the connector logs
    // `server = ?addr` on every attempt and every failure. Passing them through
    // would leak a secret to buy nothing.
    if url_userinfo_delimiter(&config.url).is_some() {
        return Err(ShoveError::Connection(format!(
            "NATS URL '{}' embeds credentials in the URL. The client never sends URL \
             userinfo when authenticating, and it renders the URL — password included — \
             into its connection debug logs. Remove the credentials from the URL and set \
             NatsConfig::username and NatsConfig::password, NatsConfig::token, or \
             NatsConfig::creds_file instead.",
            redact_url_credentials(&config.url)
        )));
    }

    if has_tls_options(config) && security == SchemeSecurity::Plaintext {
        return Err(ShoveError::Connection(format!(
            "TLS options are configured but NATS URL '{}' uses a plaintext scheme; \
             change the URL scheme to tls:// or wss:// to prevent silent downgrade",
            redact_url_credentials(&config.url)
        )));
    }

    let has_static_credentials =
        (config.username.is_some() && config.password.is_some()) || config.token.is_some();
    if has_static_credentials
        && security == SchemeSecurity::Plaintext
        && !config.allow_plaintext_credentials
    {
        return Err(ShoveError::Connection(
            "NATS username/password and token credentials require an encrypted transport: \
             use a tls:// or wss:// URL. Sending reusable credentials over plaintext \
             exposes them to network observers. For local development only, set \
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
        assert_eq!(
            classify_scheme(&cfg.url).expect("nats:// is supported"),
            SchemeSecurity::Plaintext,
            "nats:// must not be considered an encrypted scheme"
        );
        assert!(
            validate_transport_security(&cfg).is_err(),
            "TLS options on a plaintext URL must be rejected"
        );
    }

    /// A URL carrying userinfo is now rejected before the TLS-downgrade check
    /// can run, so this pins the redaction on the error that actually fires for
    /// this config. The endpoint must stay readable so the message is
    /// actionable, but the credentials must not.
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
            rendered.contains("require an encrypted transport"),
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
        assert_eq!(
            classify_scheme("tls://broker.example.com:4222").expect("tls:// is supported"),
            SchemeSecurity::Encrypted
        );
        assert_eq!(
            classify_scheme("wss://broker.example.com:443").expect("wss:// is supported"),
            SchemeSecurity::Encrypted
        );
        assert_eq!(
            classify_scheme("nats://broker.example.com:4222").expect("nats:// is supported"),
            SchemeSecurity::Plaintext
        );
        assert_eq!(
            classify_scheme("ws://broker.example.com:80").expect("ws:// is supported"),
            SchemeSecurity::Plaintext
        );
        // `async_nats::ServerAddr::from_url` rejects every other scheme, so a
        // `nats+tls://` endpoint could never have connected.
        assert!(classify_scheme("nats+tls://broker.example.com:4222").is_err());
    }

    #[test]
    fn no_tls_options_with_plain_url_is_not_flagged() {
        let cfg = NatsConfig::new("nats://broker.example.com:4222");
        assert!(!has_tls_options(&cfg));
    }

    // --- Scheme classification must follow `async_nats::ServerAddr`, which
    // accepts exactly `nats`, `tls`, `ws` and `wss`. `wss` is TLS (the
    // connector builds a rustls connector for it); `nats` and `ws` are
    // plaintext; every other scheme, `nats+tls` included, is rejected by the
    // client before it can connect. ---

    fn ca_cert_config(url: &str) -> NatsConfig {
        let mut cfg = NatsConfig::new(url);
        cfg.tls_ca_cert = Some(std::path::PathBuf::from("/etc/certs/ca.pem"));
        cfg
    }

    /// The downgrade error must point at schemes that can actually connect.
    /// It previously recommended `nats+tls://`, which `ServerAddr` rejects.
    #[test]
    fn tls_downgrade_error_recommends_connectable_schemes() {
        let cfg = ca_cert_config("nats://broker.example.com:4222");
        let err = validate_transport_security(&cfg)
            .expect_err("TLS options on a plaintext URL must be rejected");
        let rendered = err.to_string();

        assert!(
            rendered.contains("tls://") && rendered.contains("wss://"),
            "error must name connectable encrypted schemes: {rendered}"
        );
        assert!(
            !rendered.contains("nats+tls"),
            "error must not recommend a scheme the client rejects: {rendered}"
        );
    }

    #[test]
    fn wss_scheme_accepts_tls_options() {
        let cfg = ca_cert_config("wss://broker.example.com:443");
        assert!(
            validate_transport_security(&cfg).is_ok(),
            "wss:// is an encrypted transport and must accept TLS options"
        );
    }

    #[test]
    fn wss_scheme_accepts_static_credentials() {
        let mut cfg = NatsConfig::new("wss://broker.example.com:443");
        cfg.username = Some("alice".into());
        cfg.password = Some("sentinel-password".into());
        assert!(
            validate_transport_security(&cfg).is_ok(),
            "wss:// encrypts before CONNECT, so credentials need no opt-in"
        );
    }

    #[test]
    fn ws_scheme_is_plaintext_for_credentials() {
        let mut cfg = NatsConfig::new("ws://broker.example.com:80");
        cfg.token = Some("sentinel-token".into());
        assert!(
            validate_transport_security(&cfg).is_err(),
            "ws:// is an unencrypted websocket and must not carry credentials"
        );
    }

    #[test]
    fn ws_scheme_rejects_tls_options() {
        let cfg = ca_cert_config("ws://broker.example.com:80");
        assert!(
            validate_transport_security(&cfg).is_err(),
            "TLS options on ws:// would be silently discarded"
        );
    }

    #[test]
    fn nats_plus_tls_scheme_is_rejected_as_unsupported() {
        let cfg = NatsConfig::new("nats+tls://broker.example.com:4222");
        let err = validate_transport_security(&cfg)
            .expect_err("nats+tls:// cannot connect and must not be accepted");
        let rendered = err.to_string();
        assert!(
            rendered.contains("nats+tls"),
            "error should name the offending scheme: {rendered}"
        );
        assert!(
            rendered.contains("tls://") && rendered.contains("wss://"),
            "error should name schemes that actually connect: {rendered}"
        );
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        let cfg = NatsConfig::new("https://broker.example.com:4222");
        assert!(
            validate_transport_security(&cfg).is_err(),
            "a scheme async-nats cannot parse must be rejected up front"
        );
    }

    #[test]
    fn scheme_matching_is_case_insensitive() {
        // `url::Url` lowercases the scheme, so async-nats accepts `TLS://`.
        // Rejecting it here would be a false negative.
        let cfg = ca_cert_config("TLS://broker.example.com:4222");
        assert!(
            validate_transport_security(&cfg).is_ok(),
            "scheme comparison must be case-insensitive"
        );
    }

    #[test]
    fn schemeless_url_is_treated_as_plaintext() {
        // `ServerAddr::from_str` prepends `nats://` when the input has no scheme.
        let mut cfg = NatsConfig::new("broker.example.com:4222");
        cfg.token = Some("sentinel-token".into());
        assert!(
            validate_transport_security(&cfg).is_err(),
            "a schemeless URL defaults to plaintext nats:// and must not carry credentials"
        );
    }

    // --- URL userinfo must never reach async-nats on any scheme. ---

    #[test]
    fn tls_url_userinfo_is_rejected() {
        let cfg = NatsConfig::new("tls://alice:sentinel-password@broker.example.com:4222");
        let err = validate_transport_security(&cfg)
            .expect_err("URL userinfo must be rejected even on an encrypted scheme");
        let rendered = err.to_string();
        assert!(!rendered.contains("alice"), "username leaked: {rendered}");
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked: {rendered}"
        );
        assert!(
            rendered.contains("tls://***@broker.example.com:4222"),
            "redacted endpoint should remain actionable: {rendered}"
        );
    }

    #[test]
    fn wss_url_userinfo_is_rejected() {
        let cfg = NatsConfig::new("wss://alice:sentinel-password@broker.example.com:443");
        assert!(
            validate_transport_security(&cfg).is_err(),
            "URL userinfo must be rejected on wss:// too"
        );
    }

    #[test]
    fn schemeless_url_userinfo_is_rejected() {
        // `ServerAddr::from_str` prepends `nats://` to a schemeless address, so
        // the authority — userinfo included — still reaches `url::Url` and the
        // connector's `server = ?addr` log. The scheme shove was handed is
        // absent, not the authority.
        let cfg = NatsConfig::new("alice:sentinel-password@broker.example.com:4222");
        let err = validate_transport_security(&cfg)
            .expect_err("URL userinfo must be rejected on a schemeless address too");
        let rendered = err.to_string();
        assert!(!rendered.contains("alice"), "username leaked: {rendered}");
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked: {rendered}"
        );
        assert!(
            rendered.contains("***@broker.example.com:4222"),
            "redacted endpoint should remain actionable: {rendered}"
        );
    }

    #[test]
    fn plaintext_opt_in_does_not_permit_url_userinfo() {
        // The development opt-in covers the explicit credential fields only.
        // URL userinfo is rejected regardless, because async-nats never sends
        // it in CONNECT and renders it into its connection debug logs.
        let mut cfg = NatsConfig::new("nats://alice:sentinel-password@broker.example.com:4222");
        cfg.allow_plaintext_credentials = true;
        assert!(
            validate_transport_security(&cfg).is_err(),
            "allow_plaintext_credentials must not re-admit URL userinfo"
        );
    }
}
