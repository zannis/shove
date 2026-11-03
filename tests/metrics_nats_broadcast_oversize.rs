#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a NATS broadcast subscription drops a message over
//! `max_message_size` as it reads it, whether or not a handler is running, and
//! counts it as discarded.
//!
//! A broadcast topology has no DLQ, so a message over the limit is gone: it is
//! counted under `shove_messages_discarded_total` and `shove_messages_failed_total`
//! with the `oversize` reason, and the handler never sees it. While a handler
//! runs, the subscription reads the stream behind it so its consumer keeps
//! pulling, and it sets those messages aside. The size gate sits on that read,
//! so a message over the limit is settled before it can be set aside, and a
//! few of them cannot fill the memory the read-ahead is bounded to.
//!
//! The fixture holds key 1 in its handler and publishes a message over the
//! limit behind it, then waits for the discard to be counted *before* it lets
//! the handler go: a gate that ran only when a message was taken for handling
//! would count it after. A message over the limit is published ahead of the
//! held one as well, which covers the idle subscription. Both carry valid
//! JSON, so a gate that let them through would hand the handler their keys.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` drains: own integration binary, a
//! single `#[test]`, and every snapshot summed.
//!
//! Requires Docker.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;

use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;

/// The subscription's `max_message_size`.
const LIMIT: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invalidate {
    key: u64,
}

shove::define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("nats-bcast-oversize")
        .broadcast()
        .build()
);

/// Records every key it is handed, and holds key 1 until the test lets it go.
#[derive(Clone)]
struct HoldFirst {
    seen: Arc<Mutex<Vec<u64>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl HoldFirst {
    fn keys(&self) -> Vec<u64> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl MessageHandler<CacheInvalidations> for HoldFirst {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.seen.lock().expect("seen lock").push(msg.key);
        if msg.key == 1 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Outcome::Ack
    }
}

type Snapshot = HashMap<
    CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// The sum of the counter `name` over the series whose `reason` label is
/// `reason`.
fn counter(snapshot: &Snapshot, name: &str, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

/// How many samples the histogram `name` holds.
fn samples(snapshot: &Snapshot, name: &str) -> usize {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => samples.len(),
            other => panic!("{name} is not a histogram: {other:?}"),
        })
        .sum()
}

/// A message with `key` as `Invalidate` encodes it, padded with whitespace to
/// twice the limit: still valid JSON, so only its size sets it apart.
fn oversized(key: u64) -> bytes::Bytes {
    let mut payload = format!(r#"{{"key":{key}}}"#).into_bytes();
    payload.resize(2 * LIMIT, b' ');
    payload.into()
}

fn small(key: u64) -> bytes::Bytes {
    format!(r#"{{"key":{key}}}"#).into_bytes().into()
}

async fn publish(client: &NatsClient, payload: bytes::Bytes) {
    client
        .jetstream()
        .publish(CacheInvalidations::topology().queue().to_string(), payload)
        .await
        .expect("the publish is submitted")
        .await
        .expect("the publish is acked");
}

#[tokio::test]
async fn an_oversized_broadcast_message_is_discarded_as_it_is_read() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .start()
        .await
        .expect("failed to start NATS container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(4222).await.expect("port");
    let client =
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 30)
            .await
            .expect("connect to NATS");
    let broker = Broker::<Nats>::from_client(client.clone());
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");

    let handler = HoldFirst {
        seen: Arc::new(Mutex::new(Vec::new())),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(
            handler.clone(),
            ConsumerOptions::<Nats>::new().with_max_message_size(LIMIT),
        )
        .expect("subscribe");

    // The ephemeral consumer is created by the spawned delivery task, not by
    // `subscribe()`, so publishing the instant it returns would race consumer
    // creation and lose the messages this test asserts on.
    let stream = CacheInvalidations::topology().queue().to_string();
    let mut live = false;
    for _ in 0..600 {
        if let Ok(s) = client.jetstream().get_stream(&stream).await {
            let mut names = s.consumer_names();
            let mut count = 0usize;
            while let Some(Ok(_)) = names.next().await {
                count += 1;
            }
            if count == 1 {
                live = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(live, "the ephemeral consumer was never created");

    // Ahead of the held message, with no handler running.
    publish(&client, oversized(100)).await;
    publish(&client, small(1)).await;
    tokio::time::timeout(Duration::from_secs(10), handler.entered.notified())
        .await
        .expect("the handler is entered");

    // Behind it, while the handler holds key 1.
    publish(&client, oversized(99)).await;
    publish(&client, small(2)).await;

    let mut discarded = 0;
    let mut failed = 0;
    let mut sizes = 0;
    let deadline = Instant::now() + Duration::from_secs(15);
    while discarded < 2 {
        let snapshot = snapshotter.snapshot().into_hashmap();
        discarded += counter(&snapshot, "shove_messages_discarded_total", "oversize");
        failed += counter(&snapshot, "shove_messages_failed_total", "oversize");
        sizes += samples(&snapshot, "shove_message_size_bytes");
        assert!(
            Instant::now() < deadline,
            "both oversized messages are discarded while key 1 is still held; counted {discarded}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        handler.keys(),
        vec![1],
        "the handler has not been released and has seen only the held message"
    );

    handler.release.notify_one();
    let deadline = Instant::now() + Duration::from_secs(15);
    while handler.keys().len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        handler.keys(),
        vec![1, 2],
        "the handler sees the small messages only, in order"
    );

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    assert!(
        outcome.is_clean(),
        "the subscription drains cleanly: {outcome:?}"
    );

    let snapshot = snapshotter.snapshot().into_hashmap();
    discarded += counter(&snapshot, "shove_messages_discarded_total", "oversize");
    failed += counter(&snapshot, "shove_messages_failed_total", "oversize");
    sizes += samples(&snapshot, "shove_message_size_bytes");
    assert_eq!(
        discarded, 2,
        "each oversized message is counted as discarded once"
    );
    assert_eq!(
        failed, 2,
        "and as one `oversize` failure, as any other drop before the handler is"
    );
    assert_eq!(
        sizes, 4,
        "every message read has its size recorded, the dropped ones included"
    );
}
