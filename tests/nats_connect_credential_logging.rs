#![cfg(feature = "nats")]

//! Regression test: a NATS URL carrying userinfo must never reach `async-nats`.
//!
//! `async_nats::ServerAddr` is a newtype over `url::Url` with a derived `Debug`,
//! and `url`'s `Debug` impl renders the `username` and `password` components in
//! the clear. `async-nats`' connector logs `server = ?entry.addr` on every
//! connection attempt and on every failed attempt, so handing it a
//! `tls://user:pass@host` URL discloses the credentials to anyone reading debug
//! logs — regardless of how carefully shove redacts its own `Debug` and error
//! output.
//!
//! Every connection here is expected to fail: the assertions are about what the
//! failure path *logged*, not about reaching a broker.
//!
//! The credential URLs are now rejected by shove's own validation before
//! `async-nats` is constructed, so on their own these assertions would hold
//! vacuously — an empty capture buffer trivially contains no password. The
//! control leg exists to prevent that: it drives a credential-free URL down the
//! same path and requires the connector's `server = ?entry.addr` line to show
//! up in the buffer. If the subscriber, the writer, or the connector's log level
//! ever stops delivering, the control fails and the negative assertions are
//! never allowed to pass unarmed.

use std::io;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use shove::nats::{NatsClient, NatsConfig};
use tracing::subscriber::set_global_default;
use tracing_subscriber::fmt::MakeWriter;

/// Sentinels chosen so a substring match cannot collide with unrelated log text.
const SENTINEL_USER: &str = "sentinel-user";
const SENTINEL_PASSWORD: &str = "sentinel-password";

/// An in-memory `tracing` writer, so the test asserts on what a subscriber
/// would actually have written rather than on shove's own formatting.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        let buffer = self.0.lock().expect("log buffer mutex poisoned");
        String::from_utf8_lossy(&buffer).into_owned()
    }

    /// Drop what has been captured so far, so each leg is asserted against its
    /// own output rather than everything accumulated before it.
    fn clear(&self) {
        self.0.lock().expect("log buffer mutex poisoned").clear();
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A port nothing is listening on: bind an ephemeral port, note it, then drop
/// the listener. Connecting there fails fast with "connection refused", which
/// is exactly the failure path that logs the server address.
fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("read bound address").port();
    drop(listener);
    port
}

#[tokio::test]
async fn connect_does_not_log_url_userinfo() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    set_global_default(subscriber).expect("no other global subscriber in this test binary");

    let port = closed_port();

    // Control leg. A credential-free URL passes shove's validation and reaches
    // `async-nats`, which logs the address it is dialling. Proving that line
    // lands in the buffer is what gives the negative assertions below their
    // teeth — without it they would pass against an empty buffer.
    let control = format!("nats://127.0.0.1:{port}");
    // `NatsClient` is not `Debug`, so discard the result by hand rather than via
    // `expect_err`.
    if NatsClient::connect(&NatsConfig::new(control.clone()))
        .await
        .is_ok()
    {
        panic!("connecting to a closed port must fail: {control}");
    }
    let control_logs = logs.contents();
    assert!(
        control_logs.contains(&port.to_string()),
        "control leg captured no connector output for {control}; the capture \
         pipeline is broken and the assertions below would be vacuous:\n{control_logs}"
    );

    let credential_urls = [
        format!("tls://{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"),
        // `ws`/`wss` are special schemes, so the parser skips the extra slash
        // and this carries live credentials to the connector.
        format!("wss:///{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"),
        // `ServerAddr::from_str` prepends `nats://` to a schemeless address, so
        // the userinfo reaches the connector on this form too.
        format!("{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"),
    ];

    for url in credential_urls {
        logs.clear();

        let err = match NatsClient::connect(&NatsConfig::new(url.clone())).await {
            Ok(_) => panic!("a URL carrying credentials must be rejected: {url}"),
            Err(err) => err,
        };

        let captured = logs.contents();
        assert!(
            !captured.contains(SENTINEL_PASSWORD),
            "password leaked into tracing output for {url}:\n{captured}"
        );
        assert!(
            !captured.contains(SENTINEL_USER),
            "username leaked into tracing output for {url}:\n{captured}"
        );

        let rendered = err.to_string();
        assert!(
            !rendered.contains(SENTINEL_PASSWORD),
            "password leaked into the returned error for {url}: {rendered}"
        );
        assert!(
            !rendered.contains(SENTINEL_USER),
            "username leaked into the returned error for {url}: {rendered}"
        );
        // Pin the rejecting layer. Without this the test would still pass if a
        // future change let the URL through to a path that merely happens not
        // to log it.
        assert!(
            rendered.contains("embeds credentials in the URL"),
            "expected shove's own validation to reject {url}, got: {rendered}"
        );
    }

    // The `Debug` impl is the other place an operator sees the URL.
    let rendered = format!(
        "{:?}",
        NatsConfig::new(format!(
            "wss:///{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"
        ))
    );
    assert!(
        !rendered.contains(SENTINEL_PASSWORD) && !rendered.contains(SENTINEL_USER),
        "credentials leaked into NatsConfig's Debug output: {rendered}"
    );
}
