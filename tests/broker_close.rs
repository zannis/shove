#![cfg(feature = "inmemory")]

//! Regression test: `Broker<B>::close` is idempotent. Calling it twice on the
//! same broker handle must not panic, double-cancel, or otherwise fail.

use shove::Broker;
use shove::InMemory;
use shove::inmemory::InMemoryConfig;

#[tokio::test]
async fn close_is_idempotent() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.close().await;
    broker.close().await; // second call is a no-op
}
