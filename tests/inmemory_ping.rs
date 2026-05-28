#![cfg(feature = "inmemory")]

//! Integration tests for `Broker<InMemory>::ping`.

use std::time::Duration;

use shove::inmemory::InMemoryConfig;
use shove::{Broker, InMemory, ShoveError};

#[tokio::test]
async fn ping_succeeds_against_live_broker() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.ping().await.expect("ping should succeed");
}

#[tokio::test]
async fn ping_with_timeout_ignores_deadline_because_no_io() {
    // InMemory has no transport, so ping_with_timeout does not actually
    // enforce the deadline. This test documents that the API surface
    // is accepted (the call compiles and succeeds against a live broker).
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker
        .ping_with_timeout(Duration::from_millis(200))
        .await
        .expect("ping_with_timeout should succeed");
}

#[tokio::test]
async fn ping_returns_connection_error_after_close() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.close().await;
    let err = broker.ping().await.expect_err("ping after close must fail");
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
}
