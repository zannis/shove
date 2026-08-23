//! Integration test: the *sequenced* (FIFO) sibling of
//! `metrics_inmemory_discard` — a message rejected by a pre-handler gate must
//! move `messages_failed_total` exactly once, under its precise reason.
//!
//! `run_fifo_shard` handles outcomes inline rather than through
//! `route_outcome`, so it needs its own coverage: PR #67 instrumented its
//! terminal `Reject` arm, which is exactly the arm a pre-handler reject lands
//! in after the oversize gate has already counted the message as
//! `reason="oversize"`. Without the `Routed::counted` flag that arm counts it a
//! second time as `reason="rejected"`.
//!
//! `tests/metrics_inmemory_sequenced.rs` covers the handler-returned `Reject`
//! and `max_retries_exceeded` sites on this same loop, plus the `FailAll`
//! cascade rule; this file covers only the pre-handler gate.
//!
//! The topology below deliberately declares no DLQ: this is the "silent
//! discard" shape operators alert on, and the counter is the only signal.
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
    blob: String,
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

/// Never actually invoked: the message in this test is rejected by the oversize
/// gate before the handler runs. Acking keeps the assertion honest — if the gate
/// ever stopped firing, the failure counter would be absent rather than merely
/// mislabelled.
#[derive(Clone)]
struct AckHandler;
impl MessageHandler<LedgerTopic> for AckHandler {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
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
async fn sequenced_oversize_discard_counts_once_as_oversize() {
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

    // 64 bytes is far below the encoded size of the payload published next, so
    // the consumer's oversize gate fires before deserialization.
    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register_fifo::<LedgerTopic, _>(
        AckHandler,
        ConsumerOptions::<InMemory>::new().with_max_message_size(64),
    )
    .await
    .expect("register_fifo");

    publisher
        .publish::<LedgerTopic>(&Entry {
            account: "acct-1".to_string(),
            blob: "x".repeat(4096),
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
        vec![("oversize".to_string(), 1)],
        "an oversized message on a sequenced topic must produce exactly one \
         messages_failed_total sample, labelled reason=oversize; got {failed:?}"
    );
}
