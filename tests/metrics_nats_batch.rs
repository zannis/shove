#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the NATS batch consumer's metrics contract.
//!
//! - `shove_messages_consumed_total` counts **messages** under the batch's
//!   single outcome, and `shove_message_processing_duration_seconds` observes
//!   **once per flush** — the two sides of the documented batch-metrics rule.
//! - The batch `Outcome::Reject` arm moves `shove_messages_discarded_total`
//!   exactly when the rejected messages are genuinely gone — a bare topology,
//!   where the server-confirmed `double_ack` retires them with no copy — and
//!   not when a DLQ holds them.
//!
//! The discard settlement here is NATS-shaped: unlike Kafka, where the
//! `PendingDiscard` waits on a later offset commit, `settle_reject_batch`
//! settles each message the moment its `double_ack` resolves. The bare-reject
//! scenario therefore waits until the stream has actually retired the
//! messages before snapshotting, which is the observable form of "the
//! double_ack landed, so `confirm` fired".
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot — hence its own integration binary and a single `#[test]`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::BatchConsumerOptions;
use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::markers::Nats;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;

const TIMEOUT: Duration = Duration::from_secs(15);

/// Messages per scenario. Three is enough to tell "counted per message" from
/// "counted per flush" — the bug class the batch counters exist to catch.
const BATCH: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
}

const ACK_TOPIC: &str = "nats-metrics-batch-ack";
const BARE_TOPIC: &str = "nats-metrics-batch-reject-bare";
const DLQ_TOPIC: &str = "nats-metrics-batch-reject-dlq";

shove::define_topic!(
    AckTopic,
    BatchMessage,
    TopologyBuilder::new(ACK_TOPIC).build()
);

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

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

async fn publish_seq<T>(broker: &Broker<Nats>, count: u32)
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

/// Poll a stream's message count until it reaches `expected` or the deadline
/// passes; returns the last observed count.
async fn wait_for_stream_count(
    client: &NatsClient,
    stream_name: &str,
    expected: u64,
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    let mut last = u64::MAX;
    loop {
        if let Ok(mut stream) = client.jetstream().get_stream(stream_name).await
            && let Ok(info) = stream.info().await
        {
            last = info.state.messages;
            if last == expected {
                return last;
            }
        }
        if Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A stream's current message count, 0 when it does not exist yet.
async fn stream_count(client: &NatsClient, stream_name: &str) -> u64 {
    match client.jetstream().get_stream(stream_name).await {
        Ok(mut stream) => stream
            .info()
            .await
            .map(|info| info.state.messages)
            .unwrap_or(0),
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Returns one fixed outcome for every flush and counts the messages seen.
#[derive(Clone)]
struct FixedOutcomeBatchHandler {
    outcome: Outcome,
    seen: Arc<AtomicU32>,
    flushes: Arc<AtomicU32>,
    signal: Arc<Notify>,
}

impl FixedOutcomeBatchHandler {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            seen: Arc::new(AtomicU32::new(0)),
            flushes: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn act(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        self.seen.fetch_add(batch.len() as u32, Ordering::SeqCst);
        self.flushes.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
        self.outcome.clone()
    }

    fn seen(&self) -> u32 {
        self.seen.load(Ordering::SeqCst)
    }

    fn flushes(&self) -> u32 {
        self.flushes.load(Ordering::SeqCst)
    }

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

macro_rules! impl_fixed_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for FixedOutcomeBatchHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(BatchMessage, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    self.act(&messages)
                }
            }
        )*
    };
}

impl_fixed_for!(AckTopic, BareRejectTopic, DlqRejectTopic);

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

type Snapshot = HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Sum one counter across every series matching all of `labels`. Summing
/// keeps the assertion on the number an operator alerts on, whatever
/// `consumer_group` label the consumer happened to generate.
fn counter(snapshot: &Snapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            labels.iter().all(|(key, value)| {
                k.key()
                    .labels()
                    .any(|l| l.key() == *key && l.value() == *value)
            })
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(c) => *c,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

/// Every sample of one histogram across the series matching all of `labels`.
fn histogram_samples(snapshot: &Snapshot, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            labels.iter().all(|(key, value)| {
                k.key()
                    .labels()
                    .any(|l| l.key() == *key && l.value() == *value)
            })
        })
        .flat_map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().copied().map(f64::from).collect::<Vec<f64>>()
            }
            other => panic!("{name} is not a histogram: {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Scenario driver
// ---------------------------------------------------------------------------

/// Publish `BATCH` messages, run a batch consumer over them until every one
/// has reached the handler, wait for `retired` messages to remain on the
/// stream (0 = fully settled), then stop the consumer.
async fn run_scenario<T>(tb: &TestBroker, queue: &str, outcome: Outcome) -> FixedOutcomeBatchHandler
where
    T: Topic<Message = BatchMessage> + shove::NotSequenced + 'static,
    FixedOutcomeBatchHandler: BatchMessageHandler<T, Context = ()>,
{
    let broker = tb.broker();
    broker.topology().declare::<T>().await.expect("declare");
    publish_seq::<T>(&broker, BATCH).await;

    let handler = FixedOutcomeBatchHandler::new(outcome);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<T, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(BATCH as usize)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_messages(BATCH, TIMEOUT).await,
        "every published message must reach the handler; saw {} of {BATCH}",
        handler.seen()
    );
    // Settlement is per message and immediate on this backend; the stream
    // retiring every message is the observable proof it completed (for `Ack`
    // the plain acks, for `Reject` the DLQ publishes plus double_acks).
    let remaining = wait_for_stream_count(tb.client_ref(), queue, 0, TIMEOUT).await;
    assert_eq!(
        remaining, 0,
        "the {queue} stream should retire every settled message"
    );

    shutdown.cancel();
    handle.await.expect("consumer task panicked").ok();
    handler
}

impl TestBroker {
    fn client_ref(&self) -> &NatsClient {
        &self.client
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_metrics_count_messages_and_observe_duration_per_flush() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;

    // -- Scenario 1: Ack ----------------------------------------------------
    let ack = run_scenario::<AckTopic>(&tb, ACK_TOPIC, Outcome::Ack).await;

    // -- Scenario 2: bare topology, Reject ----------------------------------
    //
    // Nothing holds these payloads once the double_ack lands, so every one of
    // them is a real discard.
    let bare = run_scenario::<BareRejectTopic>(&tb, BARE_TOPIC, Outcome::Reject).await;

    // -- Scenario 3: DLQ declared, Reject -----------------------------------
    //
    // Asserted as a delta over the pre-scenario count so the test holds on a
    // shared long-lived server too, where earlier runs' dead letters persist.
    let dlq_stream = format!("{DLQ_TOPIC}-dlq");
    let dead_before = stream_count(&tb.client(), &dlq_stream).await;
    let dlq = run_scenario::<DlqRejectTopic>(&tb, DLQ_TOPIC, Outcome::Reject).await;
    let dead_after = wait_for_stream_count(
        &tb.client(),
        &dlq_stream,
        dead_before + u64::from(BATCH),
        TIMEOUT,
    )
    .await;

    // Single snapshot, taken once every consumer has stopped so nothing can
    // emit into it while it is read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // -- Scenario 1 assertions ----------------------------------------------
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_consumed_total",
            &[("topic", ACK_TOPIC), ("outcome", "ack")],
        ),
        u64::from(BATCH),
        "consumed must count messages, not flushes; handler saw {} across {} flush(es)",
        ack.seen(),
        ack.flushes()
    );
    assert_eq!(
        histogram_samples(
            &snapshot,
            "shove_message_processing_duration_seconds",
            &[("topic", ACK_TOPIC)],
        )
        .len(),
        ack.flushes() as usize,
        "processing duration must be observed once per flush, not once per message"
    );
    assert_eq!(
        histogram_samples(
            &snapshot,
            "shove_message_size_bytes",
            &[("topic", ACK_TOPIC)]
        )
        .len(),
        BATCH as usize,
        "message_size samples every pulled message"
    );

    // -- Scenario 2 assertions ----------------------------------------------
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[("topic", BARE_TOPIC), ("reason", "rejected")],
        ),
        u64::from(BATCH),
        "a rejected batch counts one failure per message, not one per flush; \
         handler saw {} across {} flush(es)",
        bare.seen(),
        bare.flushes()
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", BARE_TOPIC), ("reason", "rejected")],
        ),
        u64::from(BATCH),
        "with no DLQ declared, every rejected message is dropped on the floor \
         once its double_ack lands — that is exactly what \
         `shove_messages_discarded_total` promises to report, and a 0 here is \
         the silent data loss the counter exists to make visible"
    );

    // -- Scenario 3 assertions ----------------------------------------------
    assert_eq!(
        dead_after,
        dead_before + u64::from(BATCH),
        "every rejected message belongs in the DLQ when one is declared"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[("topic", DLQ_TOPIC), ("reason", "rejected")],
        ),
        u64::from(BATCH),
        "a DLQ does not make the rejection stop being a failure; handler saw \
         {} across {} flush(es)",
        dlq.seen(),
        dlq.flushes()
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", DLQ_TOPIC), ("reason", "rejected")],
        ),
        0,
        "the DLQ holds every rejected message, so nothing was discarded — a \
         non-zero here is a false data-loss alert on the ordinary \
         dead-letter path"
    );

    tb.broker().close().await;
}
