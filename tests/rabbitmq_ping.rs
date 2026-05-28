#![cfg(feature = "rabbitmq")]

//! Integration tests for `Broker<RabbitMq>::ping`.

use std::time::Duration;

use shove::markers::RabbitMq as RabbitMqMarker;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig};
use shove::{Broker, ShoveError};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

async fn start_broker() -> (
    testcontainers::ContainerAsync<RabbitMq>,
    Broker<RabbitMqMarker>,
) {
    let container = RabbitMq::default()
        .start()
        .await
        .expect("start RabbitMQ container");
    let host = container.get_host().await.expect("get host");
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("get AMQP port");
    let uri = format!("amqp://guest:guest@{host}:{port}");
    let client = RabbitMqClient::connect(&RabbitMqConfig::new(uri))
        .await
        .expect("connect to RabbitMQ");
    (container, Broker::<RabbitMqMarker>::from_client(client))
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
    let config = RabbitMqConfig::new("amqp://guest:guest@127.0.0.1:1");
    let broker = match Broker::<RabbitMqMarker>::new(config).await {
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
        start.elapsed() < Duration::from_secs(2),
        "ping_with_timeout(200ms) returned in {:?}, expected < 2s",
        start.elapsed()
    );
}
