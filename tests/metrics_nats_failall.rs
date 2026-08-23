#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a `SequenceFailure::FailAll` cascade on NATS must move
//! `shove_messages_failed_total` **once**, not once per dead-lettered message.
//!
//! `metrics::FailReason` states the rule: only the delivery that actually
//! failed is counted. The messages dead-lettered behind a poisoned sequence key
//! are collateral of that one already-counted failure, so counting them scales
//! the counter by the queue depth behind the key and makes it useless for the
//! alerting it exists to support.
//!
//! Nothing in CI could catch a violation of that rule here. #73 brought
//! `FailAll` to NATS with a `record_failed(.., Rejected)` on the poisoned-key
//! arm, and it survived review and merge because the rule was pinned by exactly
//! one in-memory test. #86 removed the call, and the poisoned-key arm in
//! `src/backends/nats/consumer.rs` now takes a `metrics::pending_discard`
//! record and nothing else — the discard half of the accounting, which stays
//! inert while a DLQ exists, with no failure counted either way. This test is
//! what makes putting a `record_failed` back go red.
//!
//! # Why the assertion is exact rather than incidental
//!
//! The fixture is the one `sequenced_failall_poisons_same_key_after_reject` in
//! `nats_integration.rs` already uses, because its shape is precisely the
//! scenario the rule is about: five messages on `key-A`, the third rejected,
//! so `key-A/3` and `key-A/4` are dead-lettered without ever reaching the
//! handler. Waiting for **three** messages on the DLQ is what gives the counter
//! assertion teeth — it proves two cascade discards actually happened, so a
//! counter still reading 1 proves neither was counted. Count the cascade site
//! and this reads 3.
//!
//! `key-B` rides along for the same reason it does in the original: it proves
//! the consumer kept running past the poisoning, so `rejected == 1` cannot be
//! satisfied by a consumer that simply stopped.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` *drains* every counter it reads. Hence
//! its own integration binary, a single `#[test]`, and exactly one snapshot
//! taken after both consumers have stopped — progress is waited on through the
//! DLQ and handler counters, never by peeking at the metrics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer};
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
    _container: testcontainers::ContainerAsync<NatsContainer>,
    client: NatsClient,
}

impl TestBroker {
    async fn start() -> Self {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = NatsContainer::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("failed to start NATS container");
        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get NATS port");
        let nats_url = format!("nats://{host}:{port}");

        let client = NatsClient::connect_with_retry(&NatsConfig::new(&nats_url), 10)
            .await
            .expect("failed to connect to NATS");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    fn client(&self) -> NatsClient {
        self.client.clone()
    }
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

shove::define_sequenced_topic!(
    SeqFailAllTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-metrics-failall")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(2)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

/// Rejects `key-A/2`, poisoning `key-A`. Every later `key-A` delivery is
/// dead-lettered without reaching here, so `key_a_handled` must stay at 3
/// (amounts 0, 1 and the rejected 2).
#[derive(Clone)]
struct PoisonHandler {
    key_a_handled: WaitableCounter,
    key_b_handled: WaitableCounter,
    seen: Arc<Mutex<Vec<(String, u64)>>>,
}

impl MessageHandler<SeqFailAllTopic> for PoisonHandler {
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
        if msg.order_id == "key-A" && msg.amount == 2 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }
}

struct SeqDlqHandler(WaitableCounter);
impl MessageHandler<SeqFailAllTopic> for SeqDlqHandler {
    type Context = ();
    async fn handle(&self, _: OrderMessage, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _: OrderMessage, _: DeadMessageMetadata, _: &()) {
        self.0.increment();
    }
}

// ---------------------------------------------------------------------------
// Snapshot helper
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failall_cascade_is_counted_once_not_once_per_dead_letter() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqFailAllTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for amount in 0..5u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount,
            })
            .await
            .expect("publish key-A");
    }
    for amount in 0..3u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-B".into(),
                amount,
            })
            .await
            .expect("publish key-B");
    }

    let key_a_handled = WaitableCounter::new();
    let key_b_handled = WaitableCounter::new();
    let seen: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();

    let consumer = NatsConsumer::new(client.clone());
    let handler = PoisonHandler {
        key_a_handled: key_a_handled.clone(),
        key_b_handled: key_b_handled.clone(),
        seen: seen.clone(),
    };
    let sc = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0),
            )
            .await
    });

    // key-A/2 is rejected, then key-A/3 and key-A/4 are dead-lettered without
    // ever reaching the handler → exactly 3 dead messages. Reaching 3 is the
    // proof that two cascade discards happened, which is what makes the
    // counter assertion below exact.
    let dlq_counter = WaitableCounter::new();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handler = SeqDlqHandler(dlq_counter.clone());
    let dlq_handle = tokio::spawn(async move {
        dlq_consumer
            .run_dlq::<SeqFailAllTopic, _>(dlq_handler, ())
            .await
    });

    assert!(
        dlq_counter.wait_for(3, Duration::from_secs(30)).await,
        "expected key-A/2 plus the two poisoned messages in the DLQ, got {}",
        dlq_counter.get()
    );

    // With 2 shards key-B lands on key-A's shard about half the time, in which
    // case it is only handled after the three key-A dead-letters. Waiting for
    // it proves the consumer ran on past the poisoning rather than stopping —
    // without this, `rejected == 1` could be satisfied by a dead consumer.
    assert!(
        key_b_handled.wait_for(3, Duration::from_secs(30)).await,
        "key-B must be unaffected by key-A's poisoning, but only {} of 3 were handled",
        key_b_handled.get()
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.expect("consumer task panicked").ok();
    dlq_handle.await.expect("dlq task panicked").ok();

    // Single, draining snapshot, taken only once both consumers have stopped so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    let seen = seen.lock().await.clone();
    assert!(
        !seen
            .iter()
            .any(|(k, a)| k == "key-A" && (*a == 3 || *a == 4)),
        "key-A/3 and key-A/4 must not reach the handler once the key is poisoned: {seen:?}"
    );

    // The rejected delivery is an independent failure — counted once. The two
    // dead-lettered behind the poisoned key are collateral of that same
    // failure. Counting them would read 3.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (key-A/2, the delivery that \
         actually reached the handler and failed); key-A/3 and key-A/4 were \
         dead-lettered as a FailAll cascade and must not be counted"
    );

    // Nothing in this fixture exhausts a retry budget (`max_retries(0)`, and
    // every non-poison delivery Acks), so a non-zero reading here would mean
    // the cascade was counted under a different reason rather than suppressed.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        0,
        "no delivery in this fixture exhausts a retry budget"
    );

    assert_eq!(
        key_a_handled.get(),
        3,
        "key-A/0, key-A/1 and the rejected key-A/2 reach the handler; the \
         cascaded deliveries must not"
    );
}
