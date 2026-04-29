//! Integration test: assert metric emissions during a single inmemory
//! round trip. Uses `metrics-util::debugging::DebuggingRecorder` which
//! takes the global recorder slot — keep this in its own integration
//! binary so it does not race with any other test that emits metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::time::Duration;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{
    Broker, ConsumerSupervisor, InMemory, MessageHandler, MessageMetadata, Outcome, Publisher,
    TopologyBuilder, define_topic,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    value: u32,
}

define_topic!(PingTopic, Ping, TopologyBuilder::new("ping_metrics").build());

#[derive(Clone)]
struct AckHandler;
impl MessageHandler<PingTopic> for AckHandler {
    type Context = ();
    async fn handle(&self, _msg: Ping, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inmemory_roundtrip_emits_full_metric_set() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // ---- run a single publish + consume round trip --------------------------
    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<PingTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");
    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register::<PingTopic, _>(AckHandler, Default::default())
        .expect("register");

    publisher
        .publish::<PingTopic>(&Ping { value: 42 })
        .await
        .expect("publish");

    // give the consumer a moment to dispatch the message
    tokio::time::sleep(Duration::from_millis(100)).await;
    sup.cancellation_token().cancel();
    let outcome = sup
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly");

    // ---- assert the snapshot ------------------------------------------------
    let snapshot = snapshotter.snapshot().into_hashmap();

    let names: Vec<String> = snapshot
        .keys()
        .map(|k| k.key().name().to_string())
        .collect();

    let expected = [
        "shove_messages_published_total",
        "shove_message_publish_duration_seconds",
        "shove_messages_consumed_total",
        "shove_message_processing_duration_seconds",
        "shove_message_size_bytes",
        "shove_messages_inflight",
    ];
    for name in expected {
        assert!(
            names.iter().any(|n| n == name),
            "expected `{name}` in snapshot; got {names:?}"
        );
    }

    // The consumed counter must carry outcome=ack for this happy-path test.
    let consumed_key = snapshot
        .keys()
        .find(|k| k.key().name() == "shove_messages_consumed_total")
        .expect("consumed counter key exists");
    let labels: Vec<(String, String)> = consumed_key
        .key()
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();
    assert!(labels.iter().any(|(k, v)| k == "outcome" && v == "ack"));
    assert!(labels.iter().any(|(k, v)| k == "topic" && v == "ping_metrics"));
    assert!(labels
        .iter()
        .any(|(k, v)| k == "consumer_group" && v == "default"));
}