//! Integration test: `shove_messages_failed_total` on the *sequenced*
//! in-memory consumer path.
//!
//! `run_fifo_shard` handles outcomes inline instead of going through
//! `route_outcome`, so it did not inherit the discard counters added to the
//! standard path. This asserts both instrumented sites, and — just as
//! importantly — that a `SequenceFailure::FailAll` cascade is *not* counted.
//! See `metrics::FailReason` for why.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary, and keep it to a
//! single `#[test]`, so it does not race with anything else emitting metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome,
    SequenceFailure, SequencedTopic, TopologyBuilder, define_sequenced_topic,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    account_id: String,
}

define_sequenced_topic!(
    Ledger,
    LedgerEntry,
    |msg| msg.account_id.clone(),
    TopologyBuilder::new("ledger_seq_metrics")
        .sequenced(SequenceFailure::FailAll)
        .hold_queue(Duration::from_millis(10))
        .dlq()
        .build()
);

#[derive(Clone)]
struct Counters {
    /// Handler invocations for the key that gets poisoned. Must stay at 1:
    /// the cascaded deliveries are dead-lettered without reaching the handler.
    reject_key_calls: Arc<AtomicU32>,
    /// Handler invocations for the key that exhausts its retry budget.
    retry_key_calls: Arc<AtomicU32>,
}

struct Handler;
impl MessageHandler<Ledger> for Handler {
    type Context = Counters;
    async fn handle(&self, msg: LedgerEntry, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        if msg.account_id.starts_with("acct-reject") {
            ctx.reject_key_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Reject
        } else {
            ctx.retry_key_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Retry
        }
    }
}

/// Sum `shove_messages_failed_total` across every series whose `reason` label
/// matches, so the assertion is on the number the operator actually alerts on.
fn failed_total(
    snapshot: &std::collections::HashMap<
        metrics_util::CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        ),
    >,
    reason: &str,
) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("shove_messages_failed_total is not a counter: {other:?}"),
        })
        .sum()
}

#[tokio::test(flavor = "current_thread")]
async fn sequenced_discards_are_counted_but_failall_cascades_are_not() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<Ledger>()
        .await
        .expect("declare");

    // Publish everything *before* starting the consumer. The shard drains
    // FIFO and only clears its poisoned set once its buffer empties, so
    // enqueuing up front is what makes the cascade deterministic.
    let publisher = broker.publisher().await.expect("publisher");
    for _ in 0..3 {
        publisher
            .publish::<Ledger>(&LedgerEntry {
                account_id: "acct-reject".into(),
            })
            .await
            .expect("publish reject-key message");
    }
    publisher
        .publish::<Ledger>(&LedgerEntry {
            account_id: "acct-retry".into(),
        })
        .await
        .expect("publish retry-key message");

    let ctx = Counters {
        reject_key_calls: Arc::new(AtomicU32::new(0)),
        retry_key_calls: Arc::new(AtomicU32::new(0)),
    };
    let mut group = broker.consumer_group().with_context(ctx.clone());
    group
        .register_fifo::<Ledger, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default().with_max_retries(1)),
            || Handler,
        )
        .await
        .expect("register_fifo");

    // Drive shutdown once both keys have retired rather than on a fixed
    // sleep, so this isn't racy on a loaded CI host.
    let token = group.cancellation_token();
    let probe = ctx.clone();
    let canceller = tokio::spawn(async move {
        for _ in 0..200 {
            if probe.reject_key_calls.load(Ordering::SeqCst) >= 1
                && probe.retry_key_calls.load(Ordering::SeqCst) >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Let the final outcome routing land before cancelling.
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    canceller.await.expect("canceller");
    assert!(outcome.is_clean(), "supervisor exited cleanly: {outcome:?}");

    let snapshot = snapshotter.snapshot().into_hashmap();

    // The handler-returned Reject is an independent failure — counted once.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (the message that actually \
         failed); the two cascaded deliveries behind the poisoned key must \
         not be counted"
    );

    // The two deliveries queued behind the poisoned key were dead-lettered
    // without ever reaching the handler — that is the cascade.
    assert_eq!(
        ctx.reject_key_calls.load(Ordering::SeqCst),
        1,
        "cascaded deliveries must skip the handler"
    );

    // Retry budget exhausted is likewise an independent failure.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        1,
        "expected exactly one `max_retries_exceeded` failure"
    );
}
