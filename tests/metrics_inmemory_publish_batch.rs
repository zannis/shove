//! Integration test: `publish_batch` splits `messages_published_total` by
//! what the backend actually confirmed, and a *partial* batch does not change
//! that split.
//!
//! Reporting per-record indices instead of a bare success count is an internal
//! rewrite of the same arithmetic, and the counter is the thing that would
//! silently drift if it were wrong. Uses `metrics-util`'s
//! `DebuggingRecorder`, which takes the global recorder slot — keep this in
//! its own integration binary so it does not race with any other test that
//! emits metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::num::NonZeroUsize;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{Broker, InMemory, ShoveError, TopologyBuilder, define_topic};

#[derive(serde::Serialize, serde::Deserialize)]
struct Rec {
    value: u32,
}

define_topic!(
    BatchTopic,
    Rec,
    TopologyBuilder::new("batch_metrics").build()
);

type SnapshotMap = std::collections::HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Sum of `shove_messages_published_total` for the given `outcome` label
/// (`"success"` or `"error"`).
fn published(snapshot: &SnapshotMap, outcome: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| {
            k.key().name() == "shove_messages_published_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == outcome)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("messages_published_total should be a counter, got {other:?}"),
        })
        .sum()
}

/// A batch that stops partway must count what landed as success and the rest
/// as failure — the same split the counter carried before `publish_batch`
/// reported per-record indices.
///
/// Deterministic without timing: the broker is closed first and the queue
/// capacity is 2, so records 0 and 1 enqueue and record 2 parks on a full
/// buffer against an already-cancelled shutdown token.
#[tokio::test(flavor = "current_thread")]
async fn partial_batch_splits_the_published_counter_by_what_landed() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let config = InMemoryConfig::default()
        .with_default_capacity(NonZeroUsize::new(2).expect("2 is non-zero"));
    let broker = Broker::<InMemory>::new(config).await.expect("connect");
    broker
        .topology()
        .declare::<BatchTopic>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    broker.close().await;

    let messages: Vec<Rec> = (0..5).map(|value| Rec { value }).collect();
    let err = publisher
        .publish_batch::<BatchTopic>(&messages)
        .await
        .expect_err("a 2-of-5 batch must not report success");
    let ShoveError::PartialBatch(f) = &err else {
        panic!("expected ShoveError::PartialBatch, got {err:?}");
    };
    assert_eq!(f.succeeded(), 2);
    assert_eq!(f.to_republish().len(), 3);

    let snapshot = snapshotter.snapshot().into_hashmap();
    assert_eq!(
        published(&snapshot, "success"),
        2,
        "two records landed, so two successes"
    );
    assert_eq!(
        published(&snapshot, "error"),
        3,
        "three records need re-publishing, so three failures"
    );
    // The counter split must agree with the error payload, not merely add up.
    assert_eq!(published(&snapshot, "success") as usize, f.succeeded());
    assert_eq!(
        published(&snapshot, "error") as usize,
        f.to_republish().len()
    );
}
