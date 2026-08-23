//! Integration test: the queue-depth gauges are published by
//! `QueueDepthSampler` alone, with no `Autoscaler` anywhere in the process.
//!
//! That independence is the whole point of the feature — before it, backlog
//! was observable only as a side effect of a scaling poll, so a service with
//! a fixed consumer pool had no backlog series at all. Asserting it needs a
//! test that never constructs an autoscaler, which is what this file is.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot. Keep this in its own integration binary so it does not race
//! with any other test that emits metrics, and — as in every other
//! `metrics_*` binary here — install it exactly once: a second `install()` in
//! the same process fails, and `cargo test` shares one process across the
//! whole binary.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::collections::HashMap;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{Broker, InMemory, Publisher, TopologyBuilder, define_topic};
use tokio_util::sync::CancellationToken;

#[derive(serde::Serialize, serde::Deserialize)]
struct Order {
    id: u32,
}

define_topic!(
    OrderTopic,
    Order,
    TopologyBuilder::new("orders_depth").build()
);

type Snapshot = HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Gauge value for `name` at the given `topic` label, or `None` when the
/// series is absent — the two cases this test needs to tell apart.
fn gauge(snapshot: &Snapshot, name: &str, topic: &str) -> Option<f64> {
    snapshot.iter().find_map(|(k, (_, _, value))| {
        let key = k.key();
        let matches = key.name() == name
            && key
                .labels()
                .any(|l| l.key() == "topic" && l.value() == topic);
        match (matches, value) {
            (true, DebugValue::Gauge(v)) => Some(v.into_inner()),
            _ => None,
        }
    })
}

#[tokio::test(flavor = "current_thread")]
async fn sampler_publishes_queue_depth_without_an_autoscaler() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<OrderTopic>()
        .await
        .expect("declare");

    // Nothing consumes: the three messages stay ready, which is exactly the
    // backlog the gauge is supposed to report.
    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");
    for id in 0..3 {
        publisher
            .publish::<OrderTopic>(&Order { id })
            .await
            .expect("publish");
    }

    // `never-declared` is watched *first* and does not exist, so its snapshot
    // errors. Two things ride on that in one pass: the failing queue must
    // publish nothing at all (rather than a fabricated zero that reads as
    // "drained"), and it must not stop the queue behind it from being polled.
    let sampler = broker
        .queue_depth_sampler()
        .watch("never-declared")
        .watch_topic::<OrderTopic>();
    assert_eq!(
        sampler.queues(),
        ["never-declared", "orders_depth"],
        "watch_topic must take the queue name off the topology",
    );

    // `sample_once` rather than `run`: the value under test is the emission,
    // not tokio's timer, and polling it directly keeps the test free of a
    // sleep that would have to be tuned against the poll interval.
    sampler.sample_once().await;

    let snapshot = snapshotter.snapshot().into_hashmap();

    for series in ["shove_queue_backlog", "shove_queue_inflight"] {
        assert_eq!(
            gauge(&snapshot, series, "never-declared"),
            None,
            "a failed snapshot must publish no `{series}` at all; a zero would \
             read as an empty queue during exactly the outage an operator is \
             looking at",
        );
    }

    assert_eq!(
        gauge(&snapshot, "shove_queue_backlog", "orders_depth"),
        Some(3.0),
        "backlog must report the three unconsumed messages — and must be here \
         at all, since the queue ahead of it in the watch set failed; got {:?}",
        snapshot.keys().map(|k| k.key().name()).collect::<Vec<_>>(),
    );
    assert_eq!(
        gauge(&snapshot, "shove_queue_inflight", "orders_depth"),
        Some(0.0),
        "the in-memory backend knows its in-flight count, so it must publish \
         it — a real zero, unlike a backend that cannot compute one",
    );

    // The autoscaler's own gauges must stay absent: nothing here ran a
    // scaling poll, and the sampler must not impersonate one.
    let names: Vec<&str> = snapshot.keys().map(|k| k.key().name()).collect();
    for autoscaler_series in [
        "shove_autoscaler_messages_ready",
        "shove_autoscaler_messages_in_flight",
        "shove_autoscaler_active_consumers",
        "shove_autoscaler_decisions_total",
    ] {
        assert!(
            !names.contains(&autoscaler_series),
            "`{autoscaler_series}` appeared without an autoscaler; got {names:?}",
        );
    }

    broker.close().await;
}

/// `run` must return on cancellation rather than outlive the service. No
/// recorder here — the assertion is about the loop, and this binary installs
/// the recorder exactly once, in the test above.
#[tokio::test(flavor = "current_thread")]
async fn run_stops_when_the_shutdown_token_is_cancelled() {
    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<OrderTopic>()
        .await
        .expect("declare");

    let token = CancellationToken::new();
    let handle = tokio::spawn(
        broker
            .queue_depth_sampler()
            .watch_topic::<OrderTopic>()
            .with_poll_interval(Duration::from_millis(10))
            .run(token.clone()),
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("sampler must stop within 2s of cancellation")
        .expect("sampler task must not panic");

    broker.close().await;
}
