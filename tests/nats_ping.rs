#![cfg(feature = "nats")]

//! Integration tests for `Broker<Nats>::ping`.

use std::time::Duration;

use shove::nats::{NatsClient, NatsConfig};
use shove::{Broker, Nats, ShoveError};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};

async fn start_broker() -> (testcontainers::ContainerAsync<NatsContainer>, Broker<Nats>) {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .start()
        .await
        .expect("start NATS container");
    let host = container.get_host().await.expect("get host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("get NATS port");
    let url = format!("nats://{host}:{port}");
    let client = NatsClient::connect_with_retry(&NatsConfig::new(&url), 10)
        .await
        .expect("connect to NATS");
    (container, Broker::<Nats>::from_client(client))
}

#[tokio::test]
async fn ping_succeeds_against_running_broker() {
    let (_container, broker) = start_broker().await;
    broker.ping().await.expect("ping should succeed");
}

#[tokio::test]
async fn ping_returns_connection_error_after_close() {
    let (_container, broker) = start_broker().await;
    broker.close().await;
    let err = broker.ping().await.expect_err("ping after close must fail");
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
}

#[tokio::test]
async fn ping_with_timeout_honors_deadline() {
    let config = NatsConfig::new("nats://127.0.0.1:1");
    let broker = match Broker::<Nats>::new(config).await {
        Ok(b) => b,
        Err(e) => {
            assert!(matches!(e, ShoveError::Connection(_)));
            return;
        }
    };
    let start = std::time::Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(200))
        .await
        .expect_err("ping against unreachable broker must fail");
    assert!(matches!(err, ShoveError::Connection(_)));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "ping_with_timeout(200ms) returned in {:?}, expected < 1s",
        start.elapsed()
    );
}
