//! Companion to `metrics_inmemory_discard.rs`, covering the **sequenced**
//! consumer loop.
//!
//! CAF-35 review finding: the discard counter was wired into the unsequenced
//! `route_outcome` only. The sequenced loop reaches its terminal decisions
//! through `route_reject_sequenced` instead, so a FIFO topic with no DLQ
//! discarded exactly as silently as before the counter existed — and a
//! sequenced topology is precisely where an operator is most likely to have
//! opted out of a DLQ. The counter now lives inside `route_reject_sequenced`,
//! which every sequenced terminal path funnels through.
//!
//! `DebuggingRecorder` takes the global recorder slot, so this needs its own
//! integration binary — it cannot share one with the unsequenced test.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{
    Broker, ConsumerOptions, ConsumerSupervisor, InMemory, MessageHandler, MessageMetadata,
    Outcome, Publisher, SequenceFailure, SequencedTopic, TopologyBuilder, define_sequenced_topic,
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    account_id: String,
}

// Sequenced, and bare: ordering guaranteed, nowhere to dead-letter.
define_sequenced_topic!(
    SeqDiscardTopic,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account_id.clone(),
    TopologyBuilder::new("discard_metrics_sequenced")
        .sequenced(SequenceFailure::Skip)
        // A sequenced topology must state its intent explicitly. This is the
        // `allow_message_loss` + no-DLQ shape from the review finding: the
        // topology accepts loss, which makes the discard counter the *only*
        // machine-readable evidence that loss actually happened.
        .allow_message_loss()
        .build()
);

/// Always asks to retry. With `max_retries = 0` the budget is exhausted on the
/// first attempt, so the sequenced loop routes straight to
/// `route_reject_sequenced` with `max_retries_exceeded`.
#[derive(Clone)]
struct AlwaysRetry {
    calls: Arc<AtomicU32>,
}

impl MessageHandler<SeqDiscardTopic> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, _msg: LedgerEntry, _meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Retry
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sequenced_no_dlq_budget_exhaustion_increments_discarded_counter() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<SeqDiscardTopic>()
        .await
        .expect("declare");

    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");
    let calls = Arc::new(AtomicU32::new(0));
    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register_fifo::<SeqDiscardTopic, _>(
        AlwaysRetry {
            calls: calls.clone(),
        },
        ConsumerOptions::default().with_max_retries(0),
    )
    .await
    .expect("register_fifo");

    publisher
        .publish::<SeqDiscardTopic>(&LedgerEntry {
            account_id: "acct-1".to_owned(),
        })
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

    let snapshot = snapshotter.snapshot().into_hashmap();

    let discarded = snapshot
        .keys()
        .find(|k| k.key().name() == "shove_messages_discarded_total")
        .unwrap_or_else(|| {
            let names: Vec<String> = snapshot
                .keys()
                .map(|k| k.key().name().to_string())
                .collect();
            panic!(
                "sequenced discard must be counted too; expected \
                 `shove_messages_discarded_total` in snapshot, got {names:?}"
            )
        });

    let labels: Vec<(String, String)> = discarded
        .key()
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();

    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "topic" && v == "discard_metrics_sequenced"),
        "discard counter carries the topic label; got {labels:?}"
    );
    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "reason" && v == "max_retries_exceeded"),
        "discard is attributed to budget exhaustion; got {labels:?}"
    );
}
