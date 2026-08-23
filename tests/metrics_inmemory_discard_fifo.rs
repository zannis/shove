//! Integration test: the in-memory *sequenced* (FIFO) consumer must count a
//! DLQ-terminal discard in `messages_failed_total`, exactly like its concurrent
//! sibling.
//!
//! The FIFO shard loop routes terminal outcomes through `route_reject_sequenced`
//! rather than the shared `route_outcome`, so it did not inherit the counter the
//! concurrent path emits — a `Reject` on a sequenced topic with no DLQ was
//! dropped with no metric at all.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary so it does not race
//! with any other test that emits metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{
    Broker, ConsumerOptions, ConsumerSupervisor, InMemory, MessageHandler, MessageMetadata,
    Outcome, Publisher, SequenceFailure, SequencedTopic, TopologyBuilder, define_sequenced_topic,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Entry {
    account: String,
}

// Sequenced with no DLQ — `.allow_message_loss()` is the only way to build that
// topology, and it is precisely the shape where the counter is the sole signal
// that a message was dropped.
define_sequenced_topic!(
    LedgerTopic,
    Entry,
    |msg: &Entry| msg.account.clone(),
    TopologyBuilder::new("ledger_discard_metrics")
        .sequenced(SequenceFailure::FailAll)
        .allow_message_loss()
        .build()
);

#[derive(Clone)]
struct RejectHandler;
impl MessageHandler<LedgerTopic> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Reject
    }
}

/// Collect every `shove_messages_failed_total` sample as `(reason, count)`.
fn failed_counters(snapshotter: &Snapshotter) -> Vec<(String, u64)> {
    snapshotter
        .snapshot()
        .into_hashmap()
        .into_iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            let count = match value {
                DebugValue::Counter(c) => c,
                other => panic!("messages_failed_total is not a counter: {other:?}"),
            };
            (reason, count)
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn sequenced_reject_discard_counts_as_rejected() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<LedgerTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");

    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register_fifo::<LedgerTopic, _>(RejectHandler, ConsumerOptions::<InMemory>::new())
        .await
        .expect("register_fifo");

    publisher
        .publish::<LedgerTopic>(&Entry {
            account: "acct-1".to_string(),
        })
        .await
        .expect("publish");

    tokio::time::sleep(Duration::from_millis(100)).await;
    sup.cancellation_token().cancel();
    let outcome = sup
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly");

    let failed = failed_counters(&snapshotter);

    assert_eq!(
        failed,
        vec![("rejected".to_string(), 1)],
        "a sequenced Reject with no DLQ must produce exactly one \
         messages_failed_total sample, labelled reason=rejected; got {failed:?}"
    );
}
