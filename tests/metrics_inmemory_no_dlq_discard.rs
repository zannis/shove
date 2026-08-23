//! Integration test for CAF-35: retry-budget exhaustion on a topic with **no
//! DLQ declared** silently drops the message. Before `messages_discarded_total`
//! existed this was visible only as a `WARN` line, so a service running a bare
//! topology could lose data indefinitely without a single metric moving.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary so it does not race
//! with any other test that emits metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{
    Broker, ConsumerOptions, ConsumerSupervisor, InMemory, MessageHandler, MessageMetadata,
    Outcome, Publisher, TopologyBuilder, define_topic,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    value: u32,
}

// Bare topology: no DLQ, no hold queues. This is the shape the downstream
// service in CAF-35 runs, and the shape that discards on budget exhaustion.
define_topic!(
    DiscardTopic,
    Ping,
    TopologyBuilder::new("discard_metrics").build()
);

/// Always asks to retry. With `max_retries = 0` the budget is exhausted on the
/// first attempt, so this resolves straight to `Dlq { max_retries_exceeded }`.
#[derive(Clone)]
struct AlwaysRetry {
    calls: Arc<AtomicU32>,
}

impl MessageHandler<DiscardTopic> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, _msg: Ping, _meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Retry
    }
}

#[tokio::test(flavor = "current_thread")]
async fn no_dlq_budget_exhaustion_increments_discarded_counter() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<DiscardTopic>()
        .await
        .expect("declare");

    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");
    let calls = Arc::new(AtomicU32::new(0));
    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register::<DiscardTopic, _>(
        AlwaysRetry {
            calls: calls.clone(),
        },
        ConsumerOptions::default().with_max_retries(0),
    )
    .expect("register");

    publisher
        .publish::<DiscardTopic>(&Ping { value: 7 })
        .await
        .expect("publish");

    tokio::time::sleep(Duration::from_millis(100)).await;
    sup.cancellation_token().cancel();
    let outcome = sup
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "handler runs once: max_retries=0 makes the first Retry terminal"
    );

    // ---- assert the snapshot ------------------------------------------------
    let snapshot = snapshotter.snapshot().into_hashmap();

    let discarded = snapshot
        .keys()
        .find(|k| k.key().name() == "shove_messages_discarded_total")
        .unwrap_or_else(|| {
            let names: Vec<String> = snapshot
                .keys()
                .map(|k| k.key().name().to_string())
                .collect();
            panic!("expected `shove_messages_discarded_total` in snapshot; got {names:?}")
        });

    let labels: Vec<(String, String)> = discarded
        .key()
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();

    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "topic" && v == "discard_metrics"),
        "discard counter carries the topic label; got {labels:?}"
    );
    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "reason" && v == "max_retries_exceeded"),
        "discard is attributed to budget exhaustion, not a plain reject; got {labels:?}"
    );

    // The discard is an *addition* to the failure counter, not a replacement:
    // both must move so existing `messages_failed_total` alerts keep working.
    assert!(
        snapshot
            .keys()
            .any(|k| k.key().name() == "shove_messages_failed_total"),
        "messages_failed_total still fires alongside the discard counter"
    );
}
