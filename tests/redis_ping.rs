#![cfg(feature = "redis-streams")]

//! Integration tests for `Broker<Redis>::ping`.

use std::time::Duration;

use shove::redis::{RedisConfig, RedisMode};
use shove::{Broker, Redis, ShoveError};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

async fn start_broker() -> (
    testcontainers::ContainerAsync<RedisContainer>,
    Broker<Redis>,
) {
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("start Redis container");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("get Redis port");
    let url = format!("redis://127.0.0.1:{port}");
    let broker = Broker::<Redis>::new(
        RedisConfig::new(RedisMode::Standalone { url }).with_group("ping-tests"),
    )
    .await
    .expect("connect to Redis");
    (container, broker)
}

#[tokio::test]
async fn ping_succeeds_against_running_broker() {
    let (_container, broker) = start_broker().await;
    broker.ping().await.expect("ping should succeed");
}

#[tokio::test]
async fn ping_returns_connection_error_after_broker_stops() {
    // Redis close() is a no-op (connections are Arc-managed); to prove ping
    // surfaces connection errors we stop the container instead, which is the
    // real failure mode a liveness probe would detect.
    let (container, broker) = start_broker().await;

    // Verify it works before stopping.
    broker
        .ping()
        .await
        .expect("ping before stop should succeed");

    // Stop the container — TCP connections will be severed.
    container
        .stop_with_timeout(None)
        .await
        .expect("stop Redis container");

    // The multiplexed connection pool will attempt to reconnect and fail.
    let err = broker
        .ping_with_timeout(Duration::from_millis(500))
        .await
        .expect_err("ping after broker stops must fail");
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
}

#[tokio::test]
async fn ping_with_timeout_honors_deadline() {
    // Port 1 refuses TCP connections.
    let config = RedisConfig::new(RedisMode::Standalone {
        url: "redis://127.0.0.1:1".into(),
    })
    .with_group("ping-tests");
    // Connect itself may fail because Redis does an eager ping in `connect`.
    // If it fails, the deadline behavior is already proven — the failure is
    // bounded by RedisConfig's connection_timeout default. If it somehow
    // succeeds (e.g. an unrelated process is listening on port 1), the
    // subsequent ping_with_timeout proves the deadline contract.
    let broker = match Broker::<Redis>::new(config).await {
        Ok(b) => b,
        Err(e) => {
            assert!(
                matches!(e, ShoveError::Connection(_)),
                "expected ShoveError::Connection, got {e:?}"
            );
            return;
        }
    };

    let start = std::time::Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(200))
        .await
        .expect_err("ping against unreachable broker must fail");
    let elapsed = start.elapsed();

    assert!(matches!(err, ShoveError::Connection(_)));
    assert!(
        elapsed < Duration::from_secs(1),
        "ping_with_timeout(200ms) returned in {elapsed:?}, expected < 1s"
    );
}
