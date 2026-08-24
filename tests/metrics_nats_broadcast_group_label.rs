#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a broadcast subscription's `consumer_group` label, on NATS.
//!
//! # What is being asserted
//!
//! `BroadcastSubscriber::subscribe` takes a `ConsumerOptions`, and
//! `ConsumerOptions::with_consumer_group` is public, so a caller can label a
//! broadcast subscription like any other consumer. Kafka, RabbitMQ and
//! InMemory emitted that value; NATS and Redis hard-coded `None` at every call
//! site and reported `default` instead. Identical options therefore produced
//! different series depending on the backend underneath.
//!
//! The gap survived because it is invisible from one backend: each file was
//! self-consistent, and only a cross-backend read showed three passing the
//! group through and two dropping it. That is the same shape as the Kafka
//! batch-path label divergence (#113), which is why this asserts the *whole*
//! label set rather than probing one counter — a fix that reaches five of six
//! call sites looks identical to a complete one under a single-series check.
//!
//! Two outcomes are driven so the terminal arm is covered as well as the happy
//! path. `Outcome::Reject` on a broadcast subscription discards through
//! `settle_broadcast_outcome`, which records `shove_messages_discarded_total`
//! — the counter a data-loss alert watches, and the last one to be fixed here.
//!
//! The Redis twin of this file asserts the same property on the other backend
//! that had it wrong. Both are needed: reverting one backend's labels leaves
//! the other's test green, and the divergence this pins is precisely the kind
//! that only shows up when both are checked.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot and whose `snapshot()` drains: own integration binary, one
//! `#[test]`, one snapshot at the end, progress waited on through the handler.
//!
//! Requires Docker.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::StreamExt;
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};

use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;

/// Deliberately unlike the topic name, and unlike anything shove derives: the
/// label under test is the caller's logical group, so a value that could be
/// mistaken for a queue, stream or XGROUP name would weaken the assertion.
const GROUP: &str = "cache-fleet-eu";

/// Rejected by the handler, so the terminal discard arm runs.
const REJECT_KEY: u64 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invalidate {
    key: u64,
}

shove::define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("nats-bcast-group-label")
        .broadcast()
        .build()
);

#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn saw(&self, key: u64) -> bool {
        self.seen.lock().expect("seen lock").contains(&key)
    }
}

impl MessageHandler<CacheInvalidations> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.seen.lock().expect("seen lock").push(msg.key);
        if msg.key == REJECT_KEY {
            // No DLQ on a broadcast topology, so this discards — and records
            // the discard through the helper both backends were passing `None`
            // to.
            Outcome::Reject
        } else {
            Outcome::Ack
        }
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

/// Every `consumer_group` value carried by the consume-side series, paired with
/// the metric that carried it, so a failure names the call site that regressed
/// rather than only reporting a set mismatch.
fn group_labels_by_metric(snapshot: &Snapshot) -> BTreeSet<(String, String)> {
    snapshot
        .keys()
        .map(|k| k.key())
        .filter(|k| {
            let n = k.name();
            // Publisher series carry no `consumer_group` at all, by design.
            n != "shove_messages_published_total" && n != "shove_message_publish_duration_seconds"
        })
        .filter(|k| k.labels().any(|l| l.key() == "topic"))
        .map(|k| {
            let group = k
                .labels()
                .find(|l| l.key() == "consumer_group")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            (k.name().to_string(), group)
        })
        .collect()
}

#[tokio::test]
async fn a_broadcast_subscription_labels_every_series_with_its_configured_group() {
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

    let publisher = broker.publisher().await.expect("publisher");
    let handler = Recorder::new();

    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(
            handler.clone(),
            ConsumerOptions::<Nats>::new().with_consumer_group(GROUP),
        )
        .expect("subscribe");

    // The ephemeral consumer is created by the spawned delivery task, not by
    // `subscribe()`, so publishing the instant it returns would race consumer
    // creation and lose the messages this test asserts on.
    let stream = CacheInvalidations::topology().queue().to_string();
    let mut live = false;
    for _ in 0..600 {
        let js = client.jetstream().get_stream(&stream).await;
        if let Ok(s) = js {
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

    for key in [1, REJECT_KEY] {
        publisher
            .publish::<CacheInvalidations>(&Invalidate { key })
            .await
            .expect("publish");
    }

    for key in [1, REJECT_KEY] {
        let mut arrived = false;
        for _ in 0..200 {
            if handler.saw(key) {
                arrived = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(arrived, "the subscriber never received key {key}");
    }
    // Let the terminal arm settle before the snapshot drains.
    tokio::time::sleep(Duration::from_millis(500)).await;

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    assert!(
        !outcome.timed_out,
        "the subscription should drain cleanly: {outcome:?}"
    );

    let snapshot = snapshotter.snapshot().into_hashmap();
    let labelled = group_labels_by_metric(&snapshot);

    assert!(
        !labelled.is_empty(),
        "the subscription emitted no consume-side series at all"
    );

    let wrong: Vec<_> = labelled
        .iter()
        .filter(|(_, group)| group != GROUP)
        .collect();
    assert!(
        wrong.is_empty(),
        "every series from a broadcast subscription must carry its configured \
         group; these did not: {wrong:?}"
    );

    // The terminal arm specifically: it is the last call site to be threaded
    // through, and the counter an operator alerts data loss on.
    assert!(
        labelled
            .iter()
            .any(|(metric, _)| metric == "shove_messages_discarded_total"),
        "the rejected message should have recorded a discard; got {labelled:?}"
    );
}
