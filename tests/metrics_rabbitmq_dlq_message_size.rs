#![cfg(all(feature = "rabbitmq", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the RabbitMQ **DLQ-drain** loop records
//! `shove_message_size_bytes`.
//!
//! # What is being asserted
//!
//! `record_message_size` is placed on each backend's main consume path. The
//! dedicated DLQ-drain loops are a second delivery path that call site does
//! not cover: `consume_dlq_loop` in `src/backends/rabbitmq/consumer.rs` does
//! not go through `try_deserialize_or_reject` — it inlines its own
//! `validate_payload_message_size` gate and decode — so it applied the size
//! limit while never sampling the size. The same held for the Kafka, NATS and
//! in-memory DLQ loops; each has its own `metrics_*_dlq_message_size` binary
//! asserting the same rule.
//!
//! Three things are pinned here, and the third is the reason this is worth a
//! test rather than a code comment:
//!
//! 1. **A sample exists at all** for a message that only ever arrived through
//!    the DLQ drain. Deleting the call in `consume_dlq_loop` reddens this.
//! 2. **Its value is the exact encoded payload length** — computed through the
//!    topic's own codec rather than hard-coded, since the histogram is only
//!    useful for sizing `max_message_size` if it reports the bytes that limit
//!    is compared against.
//! 3. **Its labels are the SOURCE topic and no consumer group**, never the DLQ
//!    queue name. Redis already drains its DLQ through `run_stream_loop`,
//!    which labels every metric `topology.queue()` whichever stream it reads;
//!    if this path used the DLQ name instead, `topic` would mean two different
//!    things depending on the backend and a per-topic size profile would stop
//!    summing across the main and DLQ paths. The main consumer here runs under
//!    an explicit `consumer_group`, so its own sample lands on a separate
//!    series and cannot be mistaken for the DLQ one.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` drains what it reads. Hence its own
//! integration binary, a single `#[test]`, and exactly one snapshot taken
//! after both consumers have stopped — progress is waited on through handler
//! counters, never by peeking at the metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::broker::Broker;
use shove::codec::Codec;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig, RabbitMqConsumer};
use shove::topic::Topic;
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `Snapshot`.
use shove::{TopologyBuilder, define_topic};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;

/// Source topic — and therefore the expected `topic` label on *both* the main
/// and the DLQ-drain samples.
const QUEUE: &str = "rmq-dlq-size";
/// The DLQ this topic dead-letters into. Asserted to be absent from the
/// `topic` label of every `shove_message_size_bytes` series.
const DLQ: &str = "rmq-dlq-size_dlq";
/// The main consumer's group label, which keeps its sample on its own series.
const MAIN_GROUP: &str = "rmq-dlq-size-main";
/// What `run_dlq` reports: it builds default options, so no group at all,
/// which `metrics::group_label` renders as `default`.
const DLQ_GROUP: &str = "default";

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Broker connection for this test.
///
/// Shared mode (the nextest `rabbitmq-setup` script exported
/// `RABBITMQ_AMQP_URL`) carves out a private vhost, so the queues this test
/// declares are unreachable from the main suite's — and, because
/// `.config/nextest.toml` sets `retries = 1`, so that a retried run starts
/// from an empty broker rather than inheriting the failed attempt's messages.
/// Exact sample assertions depend on that isolation. Mirrors the harness in
/// `tests/metrics_rabbitmq_sequenced.rs`, minus its consistent-hash plugin —
/// the topic here is unsequenced.
struct TestContext {
    amqp_url: String,
    mgmt_url: String,
    vhost: Option<String>,
    _container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl TestContext {
    async fn new() -> Self {
        let http = reqwest::Client::new();

        if let Ok(base_amqp) = std::env::var("RABBITMQ_AMQP_URL") {
            let mgmt_url = std::env::var("RABBITMQ_MGMT_URL")
                .expect("RABBITMQ_MGMT_URL must be set when RABBITMQ_AMQP_URL is set");
            let vhost = format!("test-{}", uuid::Uuid::new_v4());

            let status = http
                .put(format!("{mgmt_url}/api/vhosts/{vhost}"))
                .basic_auth("guest", Some("guest"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .expect("failed to create vhost")
                .status();
            assert!(status.is_success(), "create vhost returned {status}");

            let status = http
                .put(format!("{mgmt_url}/api/permissions/{vhost}/guest"))
                .basic_auth("guest", Some("guest"))
                .json(&HashMap::from([
                    ("configure", ".*"),
                    ("write", ".*"),
                    ("read", ".*"),
                ]))
                .send()
                .await
                .expect("failed to set vhost permissions")
                .status();
            assert!(status.is_success(), "set permissions returned {status}");

            Self {
                amqp_url: format!("{base_amqp}/{vhost}"),
                mgmt_url,
                vhost: Some(vhost),
                _container: None,
            }
        } else {
            // Standalone mode — no setup script ran, so start a broker here.
            // The default vhost "/" is fine: the container is this test's alone.
            let container = RabbitMq::default()
                .start()
                .await
                .expect("failed to start RabbitMQ container");

            let host = container.get_host().await.expect("failed to get host");
            let amqp_port = container
                .get_host_port_ipv4(5672)
                .await
                .expect("failed to get AMQP port");
            let mgmt_port = container
                .get_host_port_ipv4(15672)
                .await
                .expect("failed to get mgmt port");

            // Give RabbitMQ time to finish accepting AMQP connections.
            tokio::time::sleep(Duration::from_secs(3)).await;

            Self {
                amqp_url: format!("amqp://guest:guest@{host}:{amqp_port}"),
                mgmt_url: format!("http://{host}:{mgmt_port}"),
                vhost: None,
                _container: Some(container),
            }
        }
    }

    fn rmq_config(&self) -> RabbitMqConfig {
        RabbitMqConfig::new(self.amqp_url.clone())
    }

    /// Drop the private vhost (shared mode only); a standalone container is
    /// reclaimed when it is dropped.
    async fn cleanup(&self) {
        let Some(vhost) = self.vhost.as_deref() else {
            return;
        };
        reqwest::Client::new()
            .delete(format!("{}/api/vhosts/{vhost}", self.mgmt_url))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .ok();
    }
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Order {
    /// Present only to give the payload a length distinctive enough that a
    /// sample from some other series could not coincidentally match it.
    reference: String,
}

define_topic!(
    Orders,
    Order,
    TopologyBuilder::new(QUEUE).dlq_named(DLQ).build()
);

/// Rejects, which is terminal — the message goes straight to the DLQ with no
/// retry, so the main path contributes exactly one size sample.
#[derive(Clone)]
struct RejectHandler(Arc<AtomicU32>);

impl MessageHandler<Orders> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Reject
    }
}

/// Counts `handle_dead` calls so the test can wait on the drain rather than
/// sleeping against it.
#[derive(Clone)]
struct DlqHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl DlqHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.count() >= target,
            }
        }
    }
}

impl MessageHandler<Orders> for DlqHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: Order, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
    }
}

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

/// Every `shove_message_size_bytes` sample recorded for `topic` under `group`.
fn size_samples(snapshot: &Snapshot, topic: &str, group: &str) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "topic" && l.value() == topic)
        })
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "consumer_group" && l.value() == group)
        })
        .flat_map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().copied().map(f64::from).collect::<Vec<f64>>()
            }
            other => panic!("shove_message_size_bytes is not a histogram: {other:?}"),
        })
        .collect()
}

/// Every distinct `topic` label value carrying a `shove_message_size_bytes`
/// series.
fn size_topics(snapshot: &Snapshot) -> Vec<String> {
    let mut topics: Vec<String> = snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .filter_map(|(k, _)| {
            k.key()
                .labels()
                .find(|l| l.key() == "topic")
                .map(|l| l.value().to_string())
        })
        .collect();
    topics.sort();
    topics.dedup();
    topics
}

/// Every `shove_message_size_bytes` series, labels and all — so a failing
/// assertion says what *was* recorded, not merely that what it wanted is
/// missing.
fn size_series(snapshot: &Snapshot) -> Vec<String> {
    let mut series: Vec<String> = snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .map(|(k, (_, _, value))| {
            let labels: Vec<String> = k
                .key()
                .labels()
                .map(|l| format!("{}={}", l.key(), l.value()))
                .collect();
            format!("{{{}}} => {value:?}", labels.join(","))
        })
        .collect();
    series.sort();
    series
}

/// The byte length the consumer should report for `msg` — the topic's own
/// encoding, so the assertion survives a codec change.
fn encoded_len<T: Topic>(msg: &T::Message) -> f64 {
    let bytes = <T::Codec as Codec<T::Message>>::encode(msg).expect("encode");
    bytes.len() as f64
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dlq_drain_records_message_size_under_the_source_topic() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config())
        .await
        .expect("connect rabbitmq");
    let broker = Broker::<RabbitMqMarker>::from_client(client.clone());
    broker
        .topology()
        .declare::<Orders>()
        .await
        .expect("declare topology");

    let msg = Order {
        reference: "order-reference-0001".into(),
    };
    let expected = encoded_len::<Orders>(&msg);

    let publisher = broker.publisher().await.expect("publisher");
    publisher.publish::<Orders>(&msg).await.expect("publish");

    let handled = Arc::new(AtomicU32::new(0));

    let dlq_handler = DlqHandler::new();
    let dlq_consumer = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { dlq_consumer.run_dlq::<Orders, _>(dh, ()).await });

    let shutdown = CancellationToken::new();
    let main_consumer = RabbitMqConsumer::new(client.clone());
    let main_shutdown = shutdown.clone();
    let handler = RejectHandler(handled.clone());
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(main_shutdown)
            .with_consumer_group(MAIN_GROUP);
        main_consumer.run::<Orders, _>(handler, (), opts).await
    });

    assert!(
        dlq_handler.wait_for_count(1, Duration::from_secs(30)).await,
        "timed out waiting for the DLQ drain: handled={} drained={}",
        handled.load(Ordering::SeqCst),
        dlq_handler.count(),
    );

    shutdown.cancel();
    main_handle
        .await
        .expect("main consumer task panicked")
        .expect("main consumer returned an error");
    dlq_handle.abort();
    client.shutdown().await;
    ctx.cleanup().await;

    // Single, draining snapshot — taken only once both consumers have stopped,
    // so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();
    let observed = size_series(&snapshot);

    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the main consumer must have handled (and rejected) exactly one message"
    );

    assert_eq!(
        size_samples(&snapshot, QUEUE, DLQ_GROUP),
        vec![expected],
        "the RabbitMQ DLQ drain must record one shove_message_size_bytes \
         sample, carrying the encoded payload length, labelled with the source \
         topic and no consumer group; observed series: {observed:?}"
    );
    assert_eq!(
        size_samples(&snapshot, QUEUE, MAIN_GROUP),
        vec![expected],
        "the main loop's own sample must still be recorded, on its own \
         consumer_group series — otherwise the assertion above could be \
         satisfied by the main loop alone; observed series: {observed:?}"
    );
    assert_eq!(
        size_topics(&snapshot),
        vec![QUEUE.to_string()],
        "no shove_message_size_bytes series may be labelled with the DLQ queue \
         name {DLQ}: the drain reports the source topic, so main-path and \
         DLQ-path sizes stay summable under one label; observed series: \
         {observed:?}"
    );
}
