#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the batch `Outcome::Reject` arm on Kafka must move
//! `shove_messages_discarded_total` exactly when the rejected messages are
//! genuinely gone — that is, when the topology declares no DLQ — and must not
//! move it when there is a DLQ holding them.
//!
//! `flush_batch`'s `Reject` arm takes one `metrics::PendingDiscard` per message
//! and settles it against the *commit result*: `confirm` once the offsets
//! advanced, `survived` when they did not. The classifier that picks between
//! `InDlq` / `Retired` / `Lost` (`reject_settlement`) is unit-tested, but
//! nothing proved end-to-end that the counter an operator alerts on actually
//! moves. That gap is the batch-path shape of the silent-loss bug the discard
//! counter was introduced for: a bare topology rejecting a batch drops every
//! message on the floor, and before the counter existed the only trace was a
//! `WARN` line.
//!
//! The test could not live in `kafka_batch_integration.rs` when the `Reject`
//! arm landed, because the `kafka` CI leg did not enable `metrics` — a
//! metrics-asserting test there would have compiled out and reported green
//! forever. The leg enables it now.
//!
//! # Shape
//!
//! Three scenarios, one Kafka container, one `#[test]`:
//!
//! 1. **No DLQ.** `failed{rejected}` and `discarded{rejected}` both move by N.
//! 2. **DLQ declared and reachable.** `failed{rejected}` moves by N,
//!    `discarded{rejected}` stays at 0, and the N payloads are readable off the
//!    DLQ topic — the discard counter's guarantee is "this message no longer
//!    exists", so reading the bytes back is what makes the 0 mean something.
//! 3. **Handler timeout resolving to `Reject`.**
//!    `BatchConsumerOptions::with_handler_timeout_outcome(Outcome::Reject)`
//!    over a hung flush must reach the same accounting as an explicit
//!    `Reject` — that is the path the option added.
//!
//! Every scenario's handler is unconditional (always `Reject`, always hang), so
//! the assertions are on *message* totals and hold however the broker happens
//! to split the messages across polls. Nothing here depends on all N landing in
//! a single flush.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot — hence its own integration binary and a single `#[test]`.
//! The one snapshot is taken after every consumer has stopped, and the three
//! scenarios are told apart by their `topic` label rather than by the snapshot
//! draining between them.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;

/// How long a wait may take before the scenario is considered failed.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Messages per scenario. Three is enough to tell "counted per message" from
/// "counted per flush" — the bug class the batch counters exist to catch.
const BATCH: u32 = 3;

/// How long one hung flush may run in scenario 3 before it is abandoned.
const HANDLER_TIMEOUT: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
}

const BARE_TOPIC: &str = "kafka-metrics-batch-reject-bare";
const DLQ_TOPIC: &str = "kafka-metrics-batch-reject-with-dlq";
const TIMEOUT_TOPIC: &str = "kafka-metrics-batch-timeout-reject";

shove::define_topic!(
    BareRejectTopic,
    BatchMessage,
    TopologyBuilder::new(BARE_TOPIC).build()
);

shove::define_topic!(
    DlqRejectTopic,
    BatchMessage,
    TopologyBuilder::new(DLQ_TOPIC).dlq().build()
);

shove::define_topic!(
    TimeoutRejectTopic,
    BatchMessage,
    TopologyBuilder::new(TIMEOUT_TOPIC).build()
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One container for the whole test. Each scenario connects a *fresh*
/// `KafkaClient`: `broker.close()` cancels the client's shutdown token, and a
/// consumer started on an already-cancelled client stops before it consumes
/// anything.
struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    brokers: String,
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
        let brokers = format!("127.0.0.1:{port}");
        Self {
            _container: container,
            brokers,
        }
    }

    async fn client(&self) -> KafkaClient {
        KafkaClient::connect_with_retry(&KafkaConfig::new(&self.brokers), 10)
            .await
            .expect("failed to connect to Kafka")
    }
}

/// Reads up to `expected` raw payloads off `topic`, giving up after `TIMEOUT`.
///
/// Reads bytes rather than running a typed `run_dlq` consumer: `run_dlq` stops
/// only on the *client's* shutdown token, so awaiting its handle would drag
/// `broker.close()` ordering into a test that is about counters.
async fn drain_raw(brokers: &str, topic: &str, expected: usize) -> Vec<Vec<u8>> {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Message;

    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{topic}-raw-drain"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("failed to create raw consumer");
    consumer.subscribe(&[topic]).expect("subscribe should work");

    let mut out = Vec::new();
    let _ = tokio::time::timeout(TIMEOUT, async {
        while out.len() < expected {
            if let Ok(msg) = consumer.recv().await {
                out.push(msg.payload().unwrap_or_default().to_vec());
            }
        }
    })
    .await;
    out
}

async fn publish_seq<T>(broker: &Broker<Kafka>, count: u32)
where
    T: Topic<Message = BatchMessage>,
{
    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..count {
        publisher
            .publish::<T>(&BatchMessage { seq })
            .await
            .expect("publish should succeed");
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Behaviour {
    /// Reject every flush, unconditionally.
    Reject,
    /// Never return. The configured `handler_timeout` is what ends the flush,
    /// and `handler_timeout_outcome` is what it resolves to.
    Hang,
}

/// Counts the *messages* it is handed, not the flushes, and signals on each
/// one so a scenario can wait rather than sleep.
#[derive(Clone)]
struct RejectingBatchHandler {
    behaviour: Behaviour,
    seen: Arc<AtomicU32>,
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    signal: Arc<Notify>,
}

impl RejectingBatchHandler {
    fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            seen: Arc::new(AtomicU32::new(0)),
            batches: Arc::new(Mutex::new(Vec::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    async fn act(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        self.batches
            .lock()
            .expect("batches lock poisoned")
            .push(batch.iter().map(|(m, _)| m.seq).collect());
        // Recorded before hanging, so `wait_for_messages` still observes a
        // flush that is about to be abandoned by the handler timeout.
        let handled = u32::try_from(batch.len()).expect("batch larger than u32");
        self.seen.fetch_add(handled, Ordering::Relaxed);
        self.signal.notify_waiters();

        match self.behaviour {
            Behaviour::Reject => Outcome::Reject,
            // Longer than `HANDLER_TIMEOUT` and longer than `TIMEOUT`, so a
            // test that wedges fails on its own deadline rather than here.
            Behaviour::Hang => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Outcome::Ack
            }
        }
    }

    fn seen(&self) -> u32 {
        self.seen.load(Ordering::Relaxed)
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches.lock().expect("batches lock poisoned").clone()
    }

    /// Wait until at least `n` messages have been handed to the handler.
    async fn wait_for_messages(&self, n: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.seen() >= n {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.seen() >= n,
            }
        }
    }
}

macro_rules! impl_rejecting_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for RejectingBatchHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(BatchMessage, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    self.act(&messages).await
                }
            }
        )*
    };
}

impl_rejecting_for!(BareRejectTopic, DlqRejectTopic, TimeoutRejectTopic);

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

/// Sum one counter across every series for `topic` whose `reason` label
/// matches. Summing rather than picking a single series keeps the assertion on
/// the number an operator alerts on, whatever `consumer_group` label the
/// consumer happened to generate.
fn counter_total(snapshot: &Snapshot, name: &str, topic: &str, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            let mut has_topic = false;
            let mut has_reason = false;
            for label in k.key().labels() {
                match label.key() {
                    "topic" => has_topic = label.value() == topic,
                    "reason" => has_reason = label.value() == reason,
                    _ => {}
                }
            }
            has_topic && has_reason
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

fn failed_total(snapshot: &Snapshot, topic: &str, reason: &str) -> u64 {
    counter_total(snapshot, "shove_messages_failed_total", topic, reason)
}

fn discarded_total(snapshot: &Snapshot, topic: &str, reason: &str) -> u64 {
    counter_total(snapshot, "shove_messages_discarded_total", topic, reason)
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// Publish `BATCH` messages, run a batch consumer against them until every one
/// has reached the handler, then stop it.
///
/// Returns the handler so the caller can assert on what it saw.
///
/// `settle` is the grace given *after* the last message reaches the handler,
/// and it is what makes the no-redelivery assertion below mean something. The
/// batch loop is a single task and `flush_batch` is awaited inline inside a
/// `select!` branch — a selected branch's body is not cancelled — so a flush
/// already in progress finishes even if shutdown is signalled, handler timeout
/// and commit included. The window is therefore not needed to settle the
/// discards; it is there to give a redelivery every chance to appear. `Reject`
/// is terminal, so the handler must be handed exactly `BATCH` messages and no
/// more: if the offsets had not committed, the batch would come back, and the
/// `confirm` that moves the discard counter would never have fired either.
async fn run_reject_scenario<T, F>(
    tb: &TestBroker,
    behaviour: Behaviour,
    settle: Duration,
    options: F,
) -> RejectingBatchHandler
where
    T: Topic<Message = BatchMessage> + shove::NotSequenced + 'static,
    F: FnOnce(BatchConsumerOptions) -> BatchConsumerOptions,
    RejectingBatchHandler: BatchMessageHandler<T, Context = ()>,
{
    let client = tb.client().await;
    let broker = Broker::<Kafka>::from_client(client.clone());
    broker.topology().declare::<T>().await.expect("declare");
    publish_seq::<T>(&broker, BATCH).await;

    let handler = RejectingBatchHandler::new(behaviour);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client);
    let opts = options(
        BatchConsumerOptions::new()
            .with_max_batch_size(BATCH as usize)
            .with_max_batch_age(Duration::from_millis(500))
            .with_shutdown(sc),
    );
    let handle = tokio::spawn(async move { consumer.run_batch::<T, _>(h, (), opts).await });

    let all_seen = handler.wait_for_messages(BATCH, TIMEOUT).await;
    assert!(
        all_seen,
        "every published message must reach the handler; saw {} of {BATCH}: {:?}",
        handler.seen(),
        handler.batches()
    );
    tokio::time::sleep(settle).await;

    shutdown.cancel();
    handle.await.expect("consumer task panicked").ok();
    broker.close().await;

    assert_eq!(
        handler.seen(),
        BATCH,
        "`Reject` is terminal: the offsets commit and the batch must not come \
         back. A redelivery here would mean the commit never landed, which is \
         also the case where the discards are deliberately *not* counted — so \
         it would silently weaken every counter assertion below. Flushes: {:?}",
        handler.batches()
    );
    handler
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_reject_counts_a_discard_only_when_there_is_no_dlq() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;

    // -- Scenario 1: bare topology, explicit Reject -------------------------
    //
    // Nothing holds these payloads once the offsets commit, so every one of
    // them is a real discard.
    let bare = run_reject_scenario::<BareRejectTopic, _>(
        &tb,
        Behaviour::Reject,
        Duration::from_secs(5),
        |o| o,
    )
    .await;

    // -- Scenario 2: DLQ declared, explicit Reject --------------------------
    let dlq = run_reject_scenario::<DlqRejectTopic, _>(
        &tb,
        Behaviour::Reject,
        Duration::from_secs(5),
        |o| o,
    )
    .await;

    let mut dead = drain_raw(&tb.brokers, &format!("{DLQ_TOPIC}-dlq"), BATCH as usize).await;
    dead.sort();
    let mut expected: Vec<Vec<u8>> = (0..BATCH)
        .map(|seq| serde_json::to_vec(&BatchMessage { seq }).expect("encode"))
        .collect();
    expected.sort();

    // -- Scenario 3: hung flush resolved to Reject by the timeout -----------
    //
    // `Behaviour::Hang` never returns, so the flush can only end at the
    // handler timeout, and `with_handler_timeout_outcome(Reject)` is the only
    // thing that turns that into the terminal arm rather than a redelivery.
    // The settle window covers the timeout itself on top of the commit.
    let timed_out = run_reject_scenario::<TimeoutRejectTopic, _>(
        &tb,
        Behaviour::Hang,
        HANDLER_TIMEOUT + Duration::from_secs(6),
        |o| {
            o.with_handler_timeout(HANDLER_TIMEOUT)
                .with_handler_timeout_outcome(Outcome::Reject)
        },
    )
    .await;

    // Single snapshot, taken once every consumer has stopped so nothing can
    // emit into it while it is read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // -- Scenario 1 assertions ---------------------------------------------
    assert_eq!(
        failed_total(&snapshot, BARE_TOPIC, "rejected"),
        u64::from(BATCH),
        "a rejected batch counts one failure per message, not one per flush; \
         handler saw {:?}",
        bare.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, BARE_TOPIC, "rejected"),
        u64::from(BATCH),
        "with no DLQ declared, every rejected message is dropped on the floor \
         once its offsets commit — that is exactly what \
         `shove_messages_discarded_total` promises to report, and a 0 here is \
         the silent data loss the counter exists to make visible"
    );

    // -- Scenario 2 assertions ---------------------------------------------
    assert_eq!(
        dead, expected,
        "every rejected message belongs in the DLQ when one is declared"
    );
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "rejected"),
        u64::from(BATCH),
        "a DLQ does not make the rejection stop being a failure; handler saw \
         {:?}",
        dlq.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "rejected"),
        0,
        "the messages are readable off `{DLQ_TOPIC}-dlq`, so nothing was \
         discarded; counting them here would fire a data-loss alert for a \
         topology that lost nothing"
    );

    // -- Scenario 3 assertions ---------------------------------------------
    //
    // Same accounting as scenario 1, reached through the timeout rather than
    // an explicit `Reject` — that is the whole claim of
    // `with_handler_timeout_outcome`.
    assert_eq!(
        failed_total(&snapshot, TIMEOUT_TOPIC, "rejected"),
        u64::from(BATCH),
        "a timeout that resolves to `Reject` takes the same terminal arm as an \
         explicit one; handler saw {:?}",
        timed_out.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, TIMEOUT_TOPIC, "rejected"),
        u64::from(BATCH),
        "the resolved `Reject` drops the batch on a bare topology exactly as an \
         explicit one does"
    );
    assert_eq!(
        failed_total(&snapshot, TIMEOUT_TOPIC, "timeout"),
        u64::from(BATCH),
        "the abandoned flush is itself a failure, counted per message and \
         separately from the `Reject` it resolves to"
    );
    assert_eq!(
        discarded_total(&snapshot, TIMEOUT_TOPIC, "timeout"),
        0,
        "the timeout does not retire anything by itself — the discard is \
         attributed to the `Reject` it resolved to, and counting it twice \
         would double the data-loss signal"
    );
}
