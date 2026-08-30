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
//! The connection here is expected to fail: the assertion is about what the
//! failure path *logged*, not about reaching a broker.

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
    let urls = [
        format!("tls://{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"),
        // `ServerAddr::from_str` prepends `nats://` to a schemeless address, so
        // the userinfo reaches the connector on this form too.
        format!("{SENTINEL_USER}:{SENTINEL_PASSWORD}@127.0.0.1:{port}"),
    ];

    for url in urls {
        // `NatsClient` is not `Debug`, so unwrap the error by hand rather than
        // via `expect_err`.
        let err = match NatsClient::connect(&NatsConfig::new(url.clone())).await {
            Ok(_) => panic!("connecting to a closed port must fail: {url}"),
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
    }
}
