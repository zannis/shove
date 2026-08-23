//! Integration test: the RabbitMQ *sequenced* (FIFO) consumer must count every
//! DLQ-terminal discard in `messages_failed_total`, exactly like its concurrent
//! sibling.
//!
//! `consume_loop_sequenced` / `consume_loop_concurrent_sequenced` and
//! `drain_pending_for_key` reach the DLQ through `router::route_reject` directly
//! rather than through the shared `route_outcome`, so before this test's fix
//! they emitted no counter at all — a `Reject` on a sequenced topic, and every
//! message discarded behind it as FailAll collateral, died silently.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. That is why this lives in its own single-test binary instead
//! of in `rabbitmq_integration`: installed there it would observe (and be
//! polluted by) every other test in that binary.
//!
//! `Snapshotter::snapshot()` **drains** every counter it reads
//! (`AtomicU64::swap(0, SeqCst)`), so this test may snapshot exactly once, at
//! the very end. Progress is waited on through DLQ arrivals and handler
//! invocations — never by peeking at the metric under test.

#![cfg(all(feature = "rabbitmq", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig, RabbitMqConsumer};
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Publisher, SequenceFailure,
    SequencedTopic, TopologyBuilder, define_sequenced_topic,
};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Entry {
    account: String,
    amount: u64,
}

// FailAll is the shape that exercises both label decisions at once: the
// handler's own `Reject` (reason=rejected) and the collateral discard of every
// later message on the poisoned key (also reason=rejected — see the PR).
define_sequenced_topic!(
    LedgerTopic,
    Entry,
    |msg: &Entry| msg.account.clone(),
    TopologyBuilder::new("rmq_seq_discard_metrics")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

/// Rejects everything, and counts how many messages actually reached a handler.
/// The collateral discard must *not* show up here — that is what makes it
/// collateral rather than a second handler rejection.
#[derive(Clone)]
struct RejectHandler {
    invocations: Arc<AtomicUsize>,
}

impl MessageHandler<LedgerTopic> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Outcome::Reject
    }
}

/// Counts arrivals on the topic's DLQ, so the test can wait on real progress
/// instead of on the counter it is about to assert.
#[derive(Clone)]
struct DlqCounter {
    arrivals: Arc<AtomicUsize>,
}

impl MessageHandler<LedgerTopic> for DlqCounter {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        self.arrivals.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

/// Collect every `shove_messages_failed_total` sample as `(reason, count)`,
/// sorted so the assertion is order-independent.
///
/// Drains the recorder — call once.
fn failed_counters(snapshotter: &Snapshotter) -> Vec<(String, u64)> {
    let mut samples: Vec<(String, u64)> = snapshotter
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
        .filter(|(_, count)| *count > 0)
        .collect();
    samples.sort();
    samples
}

async fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    tokio::time::timeout(deadline, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn sequenced_dead_letter_counts_every_discarded_message_once() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let container = RabbitMq::default()
        .start()
        .await
        .expect("failed to start RabbitMQ container");
    let host = container.get_host().await.expect("failed to get host");
    let amqp_port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("failed to get AMQP port");

    // Sequenced topologies route through a consistent-hash exchange.
    let mut result = container
        .exec(ExecCommand::new([
            "rabbitmq-plugins",
            "enable",
            "rabbitmq_consistent_hash_exchange",
        ]))
        .await
        .expect("failed to enable consistent hash plugin");
    let _ = result.stdout_to_vec().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let amqp_url = format!("amqp://guest:guest@{host}:{amqp_port}");
    let client = RabbitMqClient::connect(&RabbitMqConfig::new(amqp_url))
        .await
        .expect("connect rabbitmq");
    let broker: Broker<RabbitMqMarker> = Broker::<RabbitMqMarker>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<RabbitMqMarker> = broker.publisher().await.expect("publisher");

    // Two messages on the same sequence key. The first is rejected by the
    // handler and poisons the key; the second is discarded as FailAll
    // collateral without the handler ever seeing it. Both must be counted.
    for amount in [1_u64, 2] {
        publisher
            .publish::<LedgerTopic>(&Entry {
                account: "acct-1".to_string(),
                amount,
            })
            .await
            .expect("publish");
    }

    let dlq_arrivals = Arc::new(AtomicUsize::new(0));
    let dlq_consumer = RabbitMqConsumer::new(client.clone());
    let dlq_handler = DlqCounter {
        arrivals: Arc::clone(&dlq_arrivals),
    };
    let dlq_task = tokio::spawn(async move {
        dlq_consumer
            .run_dlq::<LedgerTopic, _>(dlq_handler, ())
            .await
    });

    let handler_invocations = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client);
    let sc = shutdown.clone();
    let handler = RejectHandler {
        invocations: Arc::clone(&handler_invocations),
    };
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<LedgerTopic, _>(
                handler,
                (),
                ConsumerOptions::<RabbitMqMarker>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10)
                    .with_max_retries(1),
            )
            .await
    });

    // Wait on DLQ arrivals, not on the metric: snapshotting drains it.
    let both_dead_lettered = wait_for(Duration::from_secs(60), || {
        dlq_arrivals.load(Ordering::SeqCst) >= 2
    })
    .await;

    // Let any *duplicate* emission land before snapshotting, so "exactly once"
    // is a real assertion rather than a race we happened to win.
    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();
    dlq_task.abort();

    let dlq_seen = dlq_arrivals.load(Ordering::SeqCst);
    let handled = handler_invocations.load(Ordering::SeqCst);
    let failed = failed_counters(&snapshotter);

    assert!(
        both_dead_lettered,
        "expected 2 messages on the DLQ within 60s, saw {dlq_seen}; failed counters: {failed:?}"
    );
    assert_eq!(
        dlq_seen, 2,
        "exactly the two published messages must be dead-lettered"
    );
    assert_eq!(
        handled, 1,
        "only the first message may reach the handler — the second must be \
         discarded as FailAll collateral"
    );
    assert_eq!(
        failed,
        vec![("rejected".to_string(), 2)],
        "both the handler's Reject and the FailAll collateral discard must be \
         counted exactly once each under reason=rejected; got {failed:?}"
    );
}
