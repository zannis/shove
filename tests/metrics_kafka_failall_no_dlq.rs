#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a `SequenceFailure::FailAll` cascade on a Kafka topic that
//! declares **no DLQ** must move `shove_messages_discarded_total` once per
//! message that is gone — including the cascaded ones — while still counting
//! exactly **one** failure.
//!
//! This is the other half of `metrics_kafka_failall.rs`. Both drive the same
//! poisoned-key arm in `src/backends/kafka/consumer.rs`, whose accounting hangs
//! off one flag:
//!
//! ```ignore
//! let pending = metrics::pending_discard(.., topology.dlq().is_some());
//! ```
//!
//! `metrics_kafka_failall.rs` declares a DLQ, so it only ever exercises the
//! `true` arm — where the pending discard settles against a DLQ retirement and
//! `shove_messages_discarded_total` correctly stays at zero. The `false` arm,
//! where the `CommitMode::Sync` commit that retires a cascaded message is the
//! thing that loses it, had no end-to-end coverage on this backend at all.
//!
//! That arm is the quietest possible regression: messages vanish and no counter
//! moves. The `FailAll` cascade here has already drifted three times (#73
//! introduced it, #86 fixed the count, #90 pinned the DLQ-settled half), and
//! each time the drift went unnoticed because nothing in CI could catch it.
//!
//! # Why the assertion is exact rather than incidental
//!
//! With no DLQ there is nothing to wait *on*: a cascaded message reaches
//! neither the handler nor any queue, and `Snapshotter::snapshot()` *drains*
//! every counter it reads, so the metric cannot be polled for progress either.
//! Asserting the counters without a barrier would make a regression that stopped
//! counting indistinguishable from a consumer that had not finished yet.
//!
//! So the fixture buys a barrier with `routing_shards(1)`: one partition, and
//! `run_fifo` runs one sequential loop over it, so consume order == publish
//! order. `key-A/0` is rejected and poisons `key-A`, `key-A/1` and `key-A/2` are
//! cascade-discarded behind it, and `key-B/0` is published last. `key-B/0`
//! reaching the handler therefore *proves* all three `key-A` deliveries were
//! already settled — the same teeth the DLQ wait gives
//! `metrics_kafka_failall.rs`, and the liveness assertion at the same time: the
//! counters cannot be satisfied by a consumer that stopped at the poisoning.
//!
//! The deviation from that file's `routing_shards(2)` is deliberate and load
//! bearing: with two partitions rdkafka interleaves them in an unspecified
//! order, so `key-B` could be handled before `key-A` was ever poisoned and the
//! barrier would prove nothing. Raising it re-introduces the race.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after the consumer has stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::{SequenceFailure, TopologyBuilder};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A counter a test can await on rather than sleeping against.
#[derive(Clone)]
struct WaitableCounter {
    count: Arc<AtomicU32>,
    signal: Arc<Notify>,
}

impl WaitableCounter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.get() >= target;
                }
            }
        }
    }
}

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    client: KafkaClient,
}

impl TestBroker {
    async fn start() -> Self {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        let bootstrap_servers = format!("127.0.0.1:{port}");

        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
            .await
            .expect("failed to connect to Kafka");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Kafka> {
        Broker::<Kafka>::from_client(self.client.clone())
    }

    fn client(&self) -> KafkaClient {
        self.client.clone()
    }
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

// Bare topology: no DLQ, no hold queues. `routing_shards(1)` is what makes the
// consume order deterministic — see the module doc.
shove::define_sequenced_topic!(
    SeqFailAllNoDlqTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("kafka-metrics-failall-no-dlq")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .build()
);

/// Rejects `key-A/0`, poisoning `key-A`. `key-A/1` and `key-A/2` are discarded
/// without reaching here, so `key_a_handled` must stay at 1.
#[derive(Clone)]
struct PoisonHandler {
    key_a_handled: WaitableCounter,
    key_b_handled: WaitableCounter,
    seen: Arc<Mutex<Vec<(String, u64)>>>,
}

impl MessageHandler<SeqFailAllNoDlqTopic> for PoisonHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .await
            .push((msg.order_id.clone(), msg.amount));
        if msg.order_id == "key-B" {
            self.key_b_handled.increment();
        } else {
            self.key_a_handled.increment();
        }
        if msg.order_id == "key-A" && msg.amount == 0 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

type Snapshot = std::collections::HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Sum `shove_messages_failed_total` across every series whose `reason` label
/// matches, so the assertion is on the number the operator actually alerts on.
fn failed_total(snapshot: &Snapshot, reason: &str) -> u64 {
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

/// Every `shove_messages_discarded_total` series in the snapshot, as
/// `(reason, count)` pairs.
///
/// Kept as pairs rather than a bare sum so a wrong total names the reason label
/// that produced it, which points straight at the call site.
fn discarded_series(snapshot: &Snapshot) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (reason, *n),
                other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failall_cascade_with_no_dlq_counts_every_message_as_discarded() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqFailAllNoDlqTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    // Publish order is consume order (one partition). key-A/0 poisons the key,
    // key-A/1 and key-A/2 cascade behind it, and key-B/0 is the barrier.
    for amount in 0..3u64 {
        publisher
            .publish::<SeqFailAllNoDlqTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount,
            })
            .await
            .expect("publish key-A");
    }
    publisher
        .publish::<SeqFailAllNoDlqTopic>(&OrderMessage {
            order_id: "key-B".into(),
            amount: 0,
        })
        .await
        .expect("publish key-B");

    let key_a_handled = WaitableCounter::new();
    let key_b_handled = WaitableCounter::new();
    let seen: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();

    let consumer = KafkaConsumer::new(client.clone());
    let handler = PoisonHandler {
        key_a_handled: key_a_handled.clone(),
        key_b_handled: key_b_handled.clone(),
        seen: seen.clone(),
    };
    let sc = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllNoDlqTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0),
            )
            .await
    });

    // The barrier. `key-B/0` is last on a single partition consumed by one
    // sequential loop, so its handler call proves key-A/0's reject and both
    // cascade discards have already been settled — and that the consumer
    // survived the poisoning.
    assert!(
        key_b_handled.wait_for(1, Duration::from_secs(60)).await,
        "key-B must be handled after the poisoned key-A backlog drains; the \
         consumer either stopped at the poisoning or never got there"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.expect("consumer task panicked").ok();

    // Single, draining snapshot, taken only once the consumer has stopped so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    let seen = seen.lock().await.clone();
    assert!(
        !seen
            .iter()
            .any(|(k, a)| k == "key-A" && (*a == 1 || *a == 2)),
        "key-A/1 and key-A/2 must not reach the handler once the key is poisoned: {seen:?}"
    );
    assert_eq!(
        key_a_handled.get(),
        1,
        "only the rejected key-A/0 reaches the handler; the cascaded deliveries \
         must not"
    );

    // All three key-A messages are gone and the topology declares nowhere for
    // them to go, so every one of them is data loss: the rejected delivery
    // through `record_terminal` and the two cascaded ones through
    // `pending_discard`, each settled by the synchronous commit that dropped it.
    let discarded = discarded_series(&snapshot);
    let discarded_total: u64 = discarded.iter().map(|(_, n)| *n).sum();
    assert_eq!(
        discarded_total, 3,
        "a no-DLQ FailAll cascade loses the rejected message and everything \
         behind its key — all 3 must be counted as discarded; got {discarded:?}"
    );

    // The rejected delivery is an independent failure — counted once. The two
    // discarded behind the poisoned key are collateral of that same failure.
    // Counting them would read 3.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (key-A/0, the delivery that \
         actually reached the handler and failed); key-A/1 and key-A/2 were \
         discarded as a FailAll cascade and must not be counted"
    );

    // Nothing in this fixture exhausts a retry budget (`max_retries(0)`, and
    // every non-poison delivery Acks), so a non-zero reading here would mean
    // the cascade was counted under a different reason rather than suppressed.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        0,
        "no delivery in this fixture exhausts a retry budget"
    );
}
