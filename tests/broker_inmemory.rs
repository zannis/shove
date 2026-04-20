//! Smoke test for `Broker<InMemory>`.
#![cfg(feature = "inmemory")]

use std::time::Duration;

use shove::broker::Broker;
use shove::inmemory::InMemoryConfig;
use shove::markers::InMemory;

#[tokio::test]
async fn broker_connects_and_closes() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_publisher_publishes() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await.unwrap();
    // Just test that we can create a publisher without error.
    let _publisher = broker.publisher().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_topology_declares() {
    use shove::topology::{QueueTopology, TopologyBuilder};
    use shove::topic::Topic;

    struct SmokeTopic;
    impl Topic for SmokeTopic {
        type Message = String;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("smoke-test").build())
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await.unwrap();
    broker.topology().declare::<SmokeTopic>().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_supervisor_runs_empty() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await.unwrap();
    let supervisor = broker.consumer_supervisor();
    // No handlers registered -- run_until_timeout should return immediately when signal fires.
    let token = supervisor.cancellation_token();
    token.cancel(); // fire immediately
    let signal = token.clone().cancelled_owned();
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(1))
        .await;
    assert!(outcome.is_clean());
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_group_exists_for_inmemory() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await.unwrap();
    // InMemory implements HasCoordinatedGroups, so this compiles.
    let _group = broker.consumer_group();
    broker.close().await;
}
