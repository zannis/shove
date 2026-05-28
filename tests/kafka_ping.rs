#![cfg(feature = "kafka")]

//! Integration tests for `Broker<Kafka>::ping`.

use std::time::Duration;

use shove::kafka::{KafkaClient, KafkaConfig};
use shove::{Broker, Kafka, ShoveError};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};

async fn start_broker() -> (
    testcontainers::ContainerAsync<KafkaContainer>,
    Broker<Kafka>,
) {
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("get Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 10)
        .await
        .expect("connect to Kafka");
    (container, Broker::<Kafka>::from_client(client))
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
    // Point at an unreachable broker. Port 1 is reserved (tcpmux) and
    // refuses connections on every supported test platform.
    let config = KafkaConfig::new("127.0.0.1:1");
    let client = KafkaClient::connect(&config)
        .await
        .expect("connect builds the config; no real handshake yet");
    let broker = Broker::<Kafka>::from_client(client);

    let start = std::time::Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(200))
        .await
        .expect_err("ping against unreachable broker must fail");
    let elapsed = start.elapsed();

    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
    // Wide envelope: the metadata fetch is bounded by the passed timeout
    // (200 ms), plus librdkafka and spawn_blocking overhead.
    assert!(
        elapsed < Duration::from_secs(2),
        "ping_with_timeout(200ms) returned in {elapsed:?}, expected < 2s"
    );
}
