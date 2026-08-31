use async_nats::ServerAddr;
use async_nats::connection::State;
use async_nats::jetstream;
use futures_util::StreamExt;
use std::fmt;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
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
    /// Send static username/password or token credentials over a cleartext
    /// connection — `nats://`, `ws://`, or a schemeless address, which the
    /// client reads as `nats://`. Default: `false`.
    ///
    /// **For development use only.** NATS sends these credentials in the
    /// CONNECT protocol, so a cleartext transport exposes them to any network
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
///
/// Where the client can parse the URL into a real authority, its parse decides:
/// a textual scan and `url::Url` disagree on real inputs (see
/// [`parse_server_addr`]), and the side that decides what gets logged must be
/// the side that decides what is a credential. For anything else there is no
/// reliable way to tell an endpoint from a secret, so nothing is disclosed.
fn redact_url_credentials(url: &str) -> String {
    // A parsed address is only a trustworthy oracle when it actually resolved
    // an authority. `nats:///user:pass@host` parses "successfully" with no host
    // and no userinfo, because the whole authority landed in the path — so the
    // parse reports nothing to redact while the password sits in the string.
    if let Ok(addr) = parse_server_addr(url)
        && !addr.host().is_empty()
    {
        // A TCP address that carries a path, query or fragment is redacted even
        // when the parse found no userinfo: that tail is precisely where a
        // credential lands when one missing character pushes the authority out
        // of the authority (`nats//user:pass@host`), and it is where a token
        // ends up when someone writes it as a query parameter.
        return if has_url_userinfo(&addr) || carries_ignored_components(&addr) {
            redacted_endpoint(&addr)
        } else {
            url.to_string()
        };
    }

    // The client could not make an address out of this. Guessing where the
    // authority ends is what a scan does badly: in
    // `nats://alice@corp.example.com:aB3/password@broker`, terminating the
    // authority at the first `/` picks the `@` after `alice` and echoes the
    // password as if it were the host. An `@` anywhere means some part of this
    // string may be a secret and none of it can be shown.
    if url.contains('@') {
        "***".to_string()
    } else {
        url.to_string()
    }
}

/// The endpoint of a parsed address, safe to show: userinfo replaced, and for a
/// TCP endpoint everything the client ignores dropped.
///
/// Scans the address's own *serialized* form rather than the caller's input.
/// Serialization is canonical — exactly one `//` before the authority — so the
/// authority window is unambiguous here in a way it is not in raw input, and
/// unlike rebuilding from `host()`/`port()` it keeps the brackets an IPv6
/// literal needs and the path that identifies a websocket route. The endpoint
/// has to stay something the operator can copy back.
fn redacted_endpoint(addr: &ServerAddr) -> String {
    let serialized = addr.as_url_str();
    let authority_start = serialized
        .find("://")
        .map_or(0, |scheme_end| scheme_end + 3);

    // A websocket path and query are part of the address and must survive. On
    // TCP nothing past the authority is read on connect, so showing it can only
    // disclose — and this is the branch that renders `nats//user:pass@host`,
    // where the credential sits in the path.
    let visible_end = if is_websocket(addr) {
        serialized.len()
    } else {
        authority_end(serialized, authority_start)
    };
    let visible = &serialized[..visible_end];

    match url_userinfo_delimiter(visible) {
        Some(at_pos) => format!(
            "{}***@{}",
            &visible[..authority_start],
            &visible[at_pos + 1..]
        ),
        None => visible.to_string(),
    }
}

/// The offset at which the authority ends: the first `/`, `?` or `#` after it,
/// or the end of the string.
fn authority_end(url: &str, authority_start: usize) -> usize {
    url[authority_start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |offset| authority_start + offset)
}

/// Whether the address connects over a websocket, where the path and query are
/// part of the address rather than something the client ignores.
fn is_websocket(addr: &ServerAddr) -> bool {
    matches!(addr.scheme(), "ws" | "wss")
}

/// Return the absolute position of the final `@` in the URL authority. An `@`
/// in the path, query, or fragment is not userinfo.
///
/// Only ever called on a URL that `url::Url` has already serialized, where the
/// form is canonical — exactly one `//` before the authority, and `@ / ? #`
/// percent-encoded inside userinfo — so the authority window is exact. Do not
/// point it at raw user input: on input the same scan is a guess, and a wrong
/// guess prints a password.
fn url_userinfo_delimiter(url: &str) -> Option<usize> {
    let authority_start = url.find("://").map_or(0, |scheme_end| scheme_end + 3);
    url[authority_start..authority_end(url, authority_start)]
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

/// The schemes `async_nats::ServerAddr` accepts. Anything else fails to parse
/// inside the client, so shove rejects it here rather than letting a connection
/// attempt fail with a scheme error.
const SUPPORTED_SCHEMES: &str = "nats://, tls://, ws://, wss://";

/// Parse a NATS URL exactly the way the client will.
///
/// Deliberately not a hand-rolled scan. `ServerAddr::from_str` runs the WHATWG
/// URL parser, which does not agree with `split` on `"://"` on real inputs:
/// `ws`/`wss` are *special* schemes, so an arbitrary run of slashes before the
/// authority is skipped and `wss:///user:pass@host` carries live credentials;
/// leading whitespace is trimmed, so `" tls://host"` is a TLS endpoint; and an
/// empty `@` section records no credential at all. Every one of those
/// divergences is a wrong answer with a security or availability cost, so the
/// client's own parse is the only thing shove classifies from.
fn parse_server_addr(url: &str) -> Result<ServerAddr> {
    ServerAddr::from_str(url).map_err(|e| {
        // `e` is either a fixed `url::ParseError` string or the offending
        // scheme echoed back. A scheme cannot contain `:`, `@` or `/`, so no
        // credential fits through it into the message.
        ShoveError::Connection(format!(
            "NATS URL is not one the client can connect with ({e}); \
             supported schemes are {SUPPORTED_SCHEMES}"
        ))
    })
}

/// Classify a parsed address by the transport its scheme selects.
///
/// `tls` and `wss` are encrypted — the connector wraps `wss` in a rustls
/// connector built from the same `ConnectOptions` TLS settings. `nats` and `ws`
/// are cleartext, as is a schemeless address, which the parser reads as
/// `nats://`. Anything unrecognised is treated as cleartext rather than
/// trusted; `parse_server_addr` has already rejected it.
fn scheme_security(addr: &ServerAddr) -> SchemeSecurity {
    match addr.scheme() {
        "tls" | "wss" => SchemeSecurity::Encrypted,
        _ => SchemeSecurity::Plaintext,
    }
}

/// Whether the parsed address carries credentials in its authority. Empty
/// user-info (`nats://@host`, `nats://:@host`) is not a credential — the parser
/// records neither a username nor a password for it.
fn has_url_userinfo(addr: &ServerAddr) -> bool {
    addr.username().is_some() || addr.password().is_some()
}

/// Whether a TCP address carries data in a component the client never reads.
///
/// A `nats://`/`tls://` endpoint is a host and a port: the connector resolves
/// those and opens a socket, and nothing consults the path, query or fragment.
/// Anything sitting there is therefore either a typo or a secret someone put in
/// the URL — and both end up in the connector's `server = ?addr` log, which
/// renders the whole `url::Url`. A single missing character is enough:
/// `nats//user:pass@broker` parses with the host `nats` and the entire
/// credentialled authority in the path, so the no-host check cannot see it.
///
/// Websockets are the exception and always answer `false`: `as_url_str()` goes
/// straight to the websocket handshake, so there the path and query are the
/// address.
fn carries_ignored_components(addr: &ServerAddr) -> bool {
    if is_websocket(addr) {
        return false;
    }
    let url = addr.clone().into_inner();
    let carries_path = !matches!(url.path(), "" | "/");
    carries_path || url.query().is_some() || url.fragment().is_some()
}

/// Reject transport/auth combinations that would silently discard TLS options
/// or disclose reusable credentials on the wire.
fn validate_transport_security(config: &NatsConfig) -> Result<()> {
    let addr = parse_server_addr(&config.url)?;
    let security = scheme_security(&addr);

    // Credentials in the URL authority are rejected on every scheme, encrypted
    // ones included. `async_nats` never puts URL userinfo into the CONNECT
    // frame — `ServerAddr::username`/`password` exist but nothing in the client
    // reads them — so these credentials do not authenticate anything. They are
    // only ever rendered: `ServerAddr` derives `Debug` over `url::Url`, whose
    // `Debug` prints the password in the clear, and the connector logs
    // `server = ?addr` on every attempt and every failure. Passing them through
    // would leak a secret to buy nothing.
    if has_url_userinfo(&addr) {
        return Err(ShoveError::Connection(format!(
            "NATS URL '{}' embeds credentials in the URL. The client never sends URL \
             userinfo when authenticating, and it renders the URL — password included — \
             into its connection debug logs. Remove the credentials from the URL and set \
             NatsConfig::username and NatsConfig::password, NatsConfig::token, or \
             NatsConfig::creds_file instead.",
            redacted_endpoint(&addr)
        )));
    }

    // An address the parser resolved no host for cannot connect, and refusing
    // it here is what keeps `nats:///user:pass@host` — where the authority
    // parsed as a *path*, so the check above sees no credentials — from being
    // handed to the client verbatim and logged. The endpoint is redacted by the
    // textual scan, since the parse is precisely what could not be trusted.
    if addr.host().is_empty() {
        return Err(ShoveError::Connection(format!(
            "NATS URL '{}' names no host. Note that an extra slash after the scheme \
             (nats:///broker:4222) puts the whole address in the URL path, where it \
             cannot be connected to — and where credentials in it would be logged \
             verbatim by the client.",
            redact_url_credentials(&config.url)
        )));
    }

    // A `nats://`/`tls://` endpoint is a host and a port. Anything past the
    // authority is dropped on connect, so a URL carrying it is either a typo or
    // a secret in a place that authenticates nothing and gets logged — and one
    // missing character is the whole difference: `nats//user:pass@broker`
    // parses with the host `nats` and the credentialled authority in the path,
    // where neither the userinfo check nor the host check above can see it.
    if carries_ignored_components(&addr) {
        return Err(ShoveError::Connection(format!(
            "NATS URL '{}' carries a path, query or fragment. A nats:// or tls:// endpoint \
             is only a host and a port — the client never reads the rest, so it is dropped \
             on connect, and a credential placed there authenticates nothing while still \
             being rendered into the client's connection debug logs. Note that a missing \
             colon after the scheme (nats//broker:4222) puts the whole address in the path. \
             Set credentials with NatsConfig::username and NatsConfig::password, \
             NatsConfig::token, or NatsConfig::creds_file instead.",
            redacted_endpoint(&addr)
        )));
    }

    if has_tls_options(config) && security == SchemeSecurity::Plaintext {
        return Err(ShoveError::Connection(format!(
            "TLS options are configured but NATS URL '{}' uses a plaintext scheme; \
             change the URL scheme to tls:// or wss:// to prevent silent downgrade",
            // Belt and braces: the userinfo check above has already returned for
            // every URL this could redact, so today this is `config.url`
            // verbatim. It stays a redacting call so that reordering the checks
            // cannot turn this message into a disclosure.
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
        let addr = parse_server_addr(&cfg.url).expect("nats:// is supported");
        assert_eq!(
            scheme_security(&addr),
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

    /// An `@` outside the authority is not userinfo, so the host is never
    /// replaced by `***` on account of one. The tail it sits in is still
    /// dropped — on TCP the client ignores it, and shove cannot tell a contact
    /// address from a token — but that is the ignored-components rule, not a
    /// misread credential.
    #[test]
    fn at_sign_outside_authority_is_not_treated_as_userinfo() {
        let cfg = NatsConfig::new("nats://broker.example.com/path?contact=ops@example.com");
        assert!(url_userinfo_delimiter(&cfg.url).is_none());
        assert_eq!(
            redact_url_credentials(&cfg.url),
            "nats://broker.example.com",
            "the host is not userinfo and must stay visible"
        );
    }

    /// The websocket counterpart: there the path and query are the address, so
    /// nothing is dropped and an `@` in the query still is not userinfo.
    #[test]
    fn a_websocket_query_is_kept_verbatim() {
        let cfg = NatsConfig::new("wss://broker.example.com/nats-ws?contact=ops@example.com");
        assert_eq!(redact_url_credentials(&cfg.url), cfg.url);
        assert!(validate_transport_security(&cfg).is_ok());
    }

    /// The full classification matrix, including the schemeless form the
    /// client reads as `nats://`. Asserted positively so an implementation that
    /// rejected a supported scheme outright could not pass by erroring.
    #[test]
    fn every_supported_scheme_is_classified() {
        for (url, expected) in [
            ("tls://broker.example.com:4222", SchemeSecurity::Encrypted),
            ("wss://broker.example.com:443", SchemeSecurity::Encrypted),
            ("nats://broker.example.com:4222", SchemeSecurity::Plaintext),
            ("ws://broker.example.com:80", SchemeSecurity::Plaintext),
            ("broker.example.com:4222", SchemeSecurity::Plaintext),
        ] {
            let addr = parse_server_addr(url).unwrap_or_else(|e| panic!("{url} is supported: {e}"));
            assert_eq!(scheme_security(&addr), expected, "misclassified {url}");
        }

        // `async_nats::ServerAddr::from_url` rejects every other scheme, so a
        // `nats+tls://` endpoint could never have connected.
        assert!(parse_server_addr("nats+tls://broker.example.com:4222").is_err());
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

    /// Every other plaintext-scheme test asserts a *rejection*, so an
    /// over-strict validator that refused these would go unnoticed. Each of
    /// these is a config the client connects with.
    #[test]
    fn unauthenticated_endpoints_are_accepted_on_every_supported_scheme() {
        for url in [
            "nats://broker.example.com:4222",
            "tls://broker.example.com:4222",
            "ws://broker.example.com:80",
            "wss://broker.example.com:443",
            // Schemeless: the client reads this as `nats://`.
            "broker.example.com:4222",
        ] {
            assert!(
                validate_transport_security(&NatsConfig::new(url)).is_ok(),
                "{url} carries no credentials and no TLS options; it must validate"
            );
        }
    }

    /// The development opt-in has to work on every cleartext scheme it claims
    /// to cover, not just `nats://`.
    #[test]
    fn plaintext_opt_in_covers_every_cleartext_scheme() {
        for url in [
            "nats://broker.example.com:4222",
            "ws://broker.example.com:80",
            "broker.example.com:4222",
        ] {
            let mut cfg = NatsConfig::new(url);
            cfg.token = Some("development-token".into());
            cfg.allow_plaintext_credentials = true;
            assert!(
                validate_transport_security(&cfg).is_ok(),
                "the opt-in must cover the cleartext scheme {url}"
            );
        }
    }

    #[test]
    fn schemeless_url_rejects_tls_options() {
        let cfg = ca_cert_config("broker.example.com:4222");
        assert!(
            validate_transport_security(&cfg).is_err(),
            "TLS options on an implicitly-plaintext address would be discarded"
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

    /// `ws` and `wss` are *special* schemes in the WHATWG URL grammar, so the
    /// parser skips an arbitrary run of `/` before the authority instead of
    /// exactly two. `wss:///user:pass@host` therefore parses with real
    /// credentials and a real host — it connects, and the connector logs the
    /// password — while a scan anchored at `"://" + 3` sees only a path.
    /// Verified against `ServerAddr` in `serveraddr_is_the_authority_on_userinfo`.
    #[test]
    fn extra_slashes_before_the_authority_do_not_hide_userinfo() {
        for url in [
            "wss:///alice:sentinel-password@broker.example.com:443",
            "ws:///alice:sentinel-password@broker.example.com:80",
        ] {
            let cfg = NatsConfig::new(url);
            let err = validate_transport_security(&cfg)
                .err()
                .unwrap_or_else(|| panic!("userinfo must be rejected in {url}"));
            let rendered = err.to_string();
            assert!(
                !rendered.contains("sentinel-password"),
                "password leaked: {rendered}"
            );
        }
    }

    /// The mirror image of the case above, and the reason a parse alone is not
    /// a sufficient oracle. `nats` and `tls` are *not* special schemes, so the
    /// parser does the opposite of what it does for `ws`/`wss`: the extra slash
    /// is not collapsed, the whole authority lands in the path, and the parse
    /// reports no host and no userinfo. The password is still right there in
    /// the string that reaches the connector's `server = ?addr` log.
    #[test]
    fn a_hostless_url_is_rejected_rather_than_forwarded() {
        for url in [
            "nats:///alice:sentinel-password@broker.example.com:4222",
            "tls:///alice:sentinel-password@broker.example.com:4222",
        ] {
            let cfg = NatsConfig::new(url);
            let err = validate_transport_security(&cfg)
                .err()
                .unwrap_or_else(|| panic!("{url} names no host and must be rejected"));
            let rendered = err.to_string();
            assert!(
                !rendered.contains("sentinel-password"),
                "password leaked into the error: {rendered}"
            );

            let debugged = format!("{cfg:?}");
            assert!(
                !debugged.contains("sentinel-password"),
                "password leaked into Debug: {debugged}"
            );
        }
    }

    /// A TCP NATS endpoint is host and port; the connector never reads the
    /// path, query or fragment of a `nats://`/`tls://` URL. So anything there
    /// is either a typo or a secret someone put in the URL — and either way it
    /// is rendered into the connector's `server = ?addr` log. A missing colon
    /// is enough to put a whole credentialled authority in the path while the
    /// host still resolves, which the no-host check cannot see.
    #[test]
    fn a_tcp_url_carrying_a_path_query_or_fragment_is_rejected() {
        for url in [
            "nats//alice:sentinel-password@broker.example.com:4222",
            "nats://al/ice:sentinel-password@broker.example.com",
            "nats://broker.example.com:4222/?token=sentinel-password",
            "tls://broker.example.com:4222#sentinel-password",
        ] {
            let cfg = NatsConfig::new(url);
            let err = validate_transport_security(&cfg)
                .err()
                .unwrap_or_else(|| panic!("{url} puts data where the client ignores it"));
            assert!(
                !err.to_string().contains("sentinel-password"),
                "credential leaked into the error for {url}: {err}"
            );
            assert!(
                !format!("{cfg:?}").contains("sentinel-password"),
                "credential leaked into Debug for {url}"
            );
        }
    }

    /// A bare host, with or without the trailing slash a URL parser adds, is
    /// not "carrying a path".
    #[test]
    fn a_bare_tcp_endpoint_is_not_treated_as_carrying_a_path() {
        for url in [
            "nats://broker.example.com:4222",
            "nats://broker.example.com:4222/",
            "tls://broker.example.com:4222/",
            "broker.example.com:4222",
        ] {
            assert!(
                validate_transport_security(&NatsConfig::new(url)).is_ok(),
                "{url} is a plain endpoint and must validate"
            );
        }
    }

    /// Websocket endpoints are the exception: the client sends the path and
    /// query to the server, so they are part of the address and must survive.
    #[test]
    fn websocket_urls_keep_their_route() {
        for url in [
            "wss://broker.example.com/nats-ws",
            "wss://broker.example.com/nats-ws?tenant=acme",
        ] {
            assert!(
                validate_transport_security(&NatsConfig::new(url)).is_ok(),
                "{url} is a real websocket route and must validate"
            );
        }
    }

    /// When the client cannot parse a URL, the textual scan cannot tell which
    /// side of an `@` is the secret — so it must hide the endpoint rather than
    /// guess. An `@` inside the userinfo of an otherwise-unparseable URL made
    /// it guess wrong and echo the password.
    #[test]
    fn an_unparseable_url_is_hidden_rather_than_guessed_at() {
        // `aB3` is not a port, so this does not parse. The `@` in the username
        // makes a first-slash-terminated authority scan pick the wrong `@`.
        let cfg =
            NatsConfig::new("nats://alice@corp.example.com:aB3/sentinel-password@broker:4222");
        assert!(
            validate_transport_security(&cfg).is_err(),
            "an unparseable URL must be rejected"
        );
        let debugged = format!("{cfg:?}");
        assert!(
            !debugged.contains("sentinel-password"),
            "password leaked into Debug: {debugged}"
        );
    }

    /// Half of a `nats://${USER}:${PASS}@host` template with only the *user*
    /// variable unset. The password is a real credential even though the
    /// username is empty.
    #[test]
    fn password_without_a_username_is_still_a_credential() {
        let cfg = NatsConfig::new("nats://:sentinel-password@broker.example.com:4222");
        let err = validate_transport_security(&cfg)
            .expect_err("a password with no username must still be rejected");
        assert!(
            !err.to_string().contains("sentinel-password"),
            "password leaked: {err}"
        );
        assert!(
            !format!("{cfg:?}").contains("sentinel-password"),
            "password leaked into Debug"
        );
    }

    /// The redacted endpoint has to stay a usable address. Rebuilding it from
    /// `host()` and `port()` drops the brackets an IPv6 literal needs and
    /// discards the websocket path, which is the part that distinguishes two
    /// routes behind the same proxy.
    #[test]
    fn redacted_endpoint_stays_a_valid_address() {
        let ipv6 = NatsConfig::new("tls://alice:sentinel-password@[2001:db8::1]:4222");
        let rendered = validate_transport_security(&ipv6)
            .expect_err("userinfo is rejected")
            .to_string();
        assert!(
            rendered.contains("[2001:db8::1]"),
            "IPv6 host must stay bracketed so the endpoint can be copied back: {rendered}"
        );
        assert!(!rendered.contains("alice"), "username leaked: {rendered}");

        let routed = NatsConfig::new("wss://alice:sentinel-password@broker.example.com/nats-ws");
        let rendered = validate_transport_security(&routed)
            .expect_err("userinfo is rejected")
            .to_string();
        assert!(
            rendered.contains("/nats-ws"),
            "the websocket path identifies the endpoint and must survive: {rendered}"
        );
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked: {rendered}"
        );
    }

    /// The `Debug` impl must not print credentials the textual scan misses
    /// either — it is the other consumer of the redactor.
    #[test]
    fn debug_redacts_userinfo_behind_extra_slashes() {
        let cfg = NatsConfig::new("wss:///alice:sentinel-password@broker.example.com:443");
        let rendered = format!("{cfg:?}");
        assert!(
            !rendered.contains("sentinel-password"),
            "password leaked into Debug: {rendered}"
        );
        assert!(!rendered.contains("alice"), "username leaked: {rendered}");
    }

    /// `url::Url` trims leading control characters and spaces, so async-nats
    /// connects to `" tls://host"` over TLS. Rejecting it as an unsupported
    /// scheme would fail a config that works.
    #[test]
    fn surrounding_whitespace_does_not_change_the_scheme() {
        let cfg = ca_cert_config(" tls://broker.example.com:4222 ");
        assert!(
            validate_transport_security(&cfg).is_ok(),
            "a URL the client trims to tls:// must be classified as encrypted"
        );
    }

    /// An empty user-info section carries no credential — `url` records
    /// neither a username nor a password. This is what an unset
    /// `nats://${USER}:${PASS}@host` template expands to, and rejecting it
    /// with "embeds credentials" would name a secret that does not exist.
    #[test]
    fn empty_userinfo_is_not_treated_as_a_credential() {
        for url in [
            "nats://@broker.example.com:4222",
            "nats://:@broker.example.com:4222",
        ] {
            let cfg = NatsConfig::new(url);
            assert!(
                validate_transport_security(&cfg).is_ok(),
                "{url} carries no credential and must not be rejected"
            );
        }
    }

    /// Pins the parser contract the three tests above rely on, so a future
    /// async-nats bump that changes it fails here rather than silently
    /// reopening the leak.
    #[test]
    fn serveraddr_is_the_authority_on_userinfo() {
        use std::str::FromStr as _;

        let hidden = async_nats::ServerAddr::from_str(
            "wss:///alice:sentinel-password@broker.example.com:443",
        )
        .expect("wss:// with extra slashes parses");
        assert_eq!(hidden.username(), Some("alice"));
        assert_eq!(hidden.password(), Some("sentinel-password"));
        assert_eq!(hidden.host(), "broker.example.com");

        let empty = async_nats::ServerAddr::from_str("nats://:@broker.example.com:4222")
            .expect("empty userinfo parses");
        assert_eq!(empty.username(), None);
        assert_eq!(empty.password(), None);

        let trimmed = async_nats::ServerAddr::from_str(" tls://broker.example.com:4222 ")
            .expect("surrounding whitespace is trimmed");
        assert_eq!(trimmed.scheme(), "tls");
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
