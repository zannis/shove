//! Integration test: assert that a `ConsumerGroup` registration emits a
//! non-default `consumer_group` metric label.
//!
//! This is a separate integration binary from `metrics_inmemory` so that each
//! gets its own process and can install a fresh `DebuggingRecorder` without
//! conflicting with the other test's global recorder slot.

#![cfg(all(feature = "inmemory", feature = "metrics"))]

use std::time::Duration;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome, Publisher,
    TopologyBuilder, define_topic,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    value: u32,
}

define_topic!(
    PingGroupTopic,
    Ping,
    TopologyBuilder::new("ping_metrics_group").build()
);

#[derive(Clone)]
struct AckHandler;
impl MessageHandler<PingGroupTopic> for AckHandler {
    type Context = ();
    async fn handle(&self, _msg: Ping, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_group_roundtrip_emits_named_group_label() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<PingGroupTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");

    let mut group = broker.consumer_group();
    group
        .register::<PingGroupTopic, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
            || AckHandler,
        )
        .await
        .expect("register");

    // The group's `run_until_timeout` calls `start_all()` internally. Publish
    // before drain — the broker queues the message until consumers spin up.
    publisher
        .publish::<PingGroupTopic>(&Ping { value: 1 })
        .await
        .expect("publish");

    // Signal completion after a short window so consumers have time to dispatch.
    let group_token = group.cancellation_token();
    let signal = async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        group_token.cancel();
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "group exited cleanly");

    let snapshot = snapshotter.snapshot().into_hashmap();

    let consumed_key = snapshot
        .keys()
        .find(|k| k.key().name() == "shove_messages_consumed_total")
        .expect("consumed counter key exists");

    let labels: Vec<(String, String)> = consumed_key
        .key()
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();

    let group_label = labels
        .iter()
        .find(|(k, _)| k == "consumer_group")
        .map(|(_, v)| v.as_str())
        .expect("consumer_group label present");

    assert_ne!(
        group_label, "default",
        "expected a named consumer_group label, got 'default'"
    );
    assert_eq!(
        group_label, "ping_metrics_group",
        "expected consumer_group=ping_metrics_group, got '{group_label}'"
    );
}
