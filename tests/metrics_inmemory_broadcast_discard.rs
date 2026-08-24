//! Broadcast discards go through the **existing** no-DLQ terminal path, not a
//! new one.
//!
//! `.broadcast()` implies best-effort delivery, so `Outcome::Retry` and
//! `Outcome::Reject` drop the message. The requirement is not merely that they
//! drop it — it is that they drop it the same way a bare topology already does,
//! so `shove_messages_discarded_total` and `shove_messages_failed_total` keep
//! firing and an operator's existing data-loss alert covers broadcast for free.
//! A second, quieter discard path would be invisible to exactly the alert that
//! needs it.
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
use shove::inmemory::InMemoryBroker;
use shove::{
    Broker, ConsumerOptions, InMemory, MessageHandler, MessageMetadata, Outcome, TopologyBuilder,
    define_topic,
};

const TOPIC: &str = "broadcast_discard_metrics";

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    value: u32,
}

define_topic!(
    BroadcastTopic,
    Ping,
    TopologyBuilder::new("broadcast_discard_metrics")
        .broadcast()
        .build()
);

/// Always asks to retry. On a broadcast subscription the retry budget is pinned
/// to zero, so this resolves straight to the terminal arm.
#[derive(Clone)]
struct AlwaysRetry {
    calls: Arc<AtomicU32>,
}

impl MessageHandler<BroadcastTopic> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, _msg: Ping, _meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Retry
    }
}

async fn wait_for_subscribers(broker: &InMemoryBroker, n: usize) {
    for _ in 0..500 {
        if broker.broadcast_subscriber_count(TOPIC) == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!(
        "timed out waiting for {n} subscriber(s); have {}",
        broker.broadcast_subscriber_count(TOPIC)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn broadcast_retry_discards_through_the_existing_no_dlq_path() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    let calls = Arc::new(AtomicU32::new(0));
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastTopic, _>(
            AlwaysRetry {
                calls: calls.clone(),
            },
            // A generous retry budget that broadcast must override. If it were
            // honoured, the handler would run four more times and the discard
            // would be attributed to a *later* attempt.
            ConsumerOptions::new().with_max_retries(4),
        )
        .expect("subscribe");
    wait_for_subscribers(&client, 1).await;

    publisher
        .publish::<BroadcastTopic>(&Ping { value: 7 })
        .await
        .expect("publish");

    tokio::time::sleep(Duration::from_millis(100)).await;
    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "subscriber exited cleanly: {outcome:?}");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "handler runs once: broadcast pins the retry budget to zero regardless of \
         with_max_retries, so the first Retry is terminal"
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
        labels.iter().any(|(k, v)| k == "topic" && v == TOPIC),
        "the discard carries the broadcast topic label; got {labels:?}"
    );
    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "reason" && v == "max_retries_exceeded"),
        "the discard is attributed to budget exhaustion, the same reason a bare \
         topology reports — this is the existing terminal arm, not a new one; got {labels:?}"
    );

    assert!(
        snapshot
            .keys()
            .any(|k| k.key().name() == "shove_messages_failed_total"),
        "messages_failed_total fires alongside the discard, as it does for any \
         no-DLQ topology"
    );
}
