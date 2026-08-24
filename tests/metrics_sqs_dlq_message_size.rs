#![cfg(all(feature = "aws-sns-sqs", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the SQS **DLQ-drain** loop records
//! `shove_message_size_bytes`.
//!
//! # What is being asserted
//!
//! `record_message_size` is placed on each backend's main consume path. The
//! dedicated DLQ-drain loops are a second delivery path that call site does
//! not cover: `consume_dlq_loop` in `src/backends/sns/consumer.rs` polls the
//! DLQ queue and applies its own `DEFAULT_MAX_MESSAGE_SIZE` gate without
//! reaching the main loop, so it applied a size limit while never sampling the
//! size. The same held for the Kafka, NATS, RabbitMQ and in-memory DLQ loops;
//! each has its own `metrics_*_dlq_message_size` binary asserting the same
//! rule. Redis is the one backend with no binary here, because its `run_dlq`
//! reuses `run_stream_loop` and is covered on the main call site.
//!
//! Three things are pinned here, and the third is the reason this is worth a
//! test rather than a code comment:
//!
//! 1. **A sample exists at all** for a message drained from the DLQ. Deleting
//!    the call in `consume_dlq_loop` reddens this.
//! 2. **Its value is the exact encoded payload length** — computed through the
//!    topic's own codec rather than hard-coded, since the histogram is only
//!    useful for sizing `max_message_size` if it reports the bytes that limit
//!    is compared against. On SQS that means the length *after*
//!    `extract_payload` has stripped the SNS notification envelope, which is
//!    what the gate compares and what the main path already reports.
//! 3. **Its labels are the SOURCE topic and no consumer group**, never the DLQ
//!    queue name. Redis already drains its DLQ through `run_stream_loop`,
//!    which labels every metric `topology.queue()` whichever stream it reads;
//!    if this path used the DLQ name instead, `topic` would mean two different
//!    things depending on the backend and a per-topic size profile would stop
//!    summing across the main and DLQ paths. The main consumer here runs under
//!    an explicit `consumer_group`, so its own samples land on a separate
//!    series and cannot satisfy the DLQ assertion.
//!
//! # How the message reaches the DLQ
//!
//! SQS does not republish on reject: `route_reject` sets visibility to 0 and
//! lets **native redrive** retire the message, exactly as
//! `dlq_consumer_handles_dead_message` in `sns_sqs_integration.rs` does. So
//! this drives the real path — publish, reject, poll the DLQ until the message
//! lands, stop the main consumer, then run `run_dlq` — rather than seeding the
//! DLQ queue directly.
//!
//! That is also why the *main* series' sample count is not pinned to an exact
//! number below. With `max_retries(1)` the retry gate short-circuits ahead of
//! `record_message_size` from the second receive onward, so the intended count
//! is one; but how many redrive receives report which `ApproximateReceiveCount`
//! is LocalStack fidelity, not consumer behaviour. The main series is asserted
//! to be non-empty and to carry only the expected length — enough to prove the
//! DLQ sample is a *second*, distinct sample without making this test a
//! redelivery-counting test.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` drains what it reads. Hence its own
//! integration binary, a single `#[test]`, and exactly one snapshot taken
//! after both consumers have stopped — progress is waited on through handler
//! counters and the DLQ itself, never by peeking at the metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::broker::Broker;
use shove::codec::Codec;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Sqs;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::sns::{SnsClient, SnsConfig, SqsConsumer};
use shove::topic::Topic;
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in the snapshot helpers.
use shove::{TopologyBuilder, define_topic};

use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Source topic — and therefore the expected `topic` label on *both* the main
/// and the DLQ-drain samples.
const QUEUE: &str = "metrics-sqs-dlq-size";
/// The DLQ this topic dead-letters into (`TopologyBuilder::dlq()` derives
/// `{queue}-dlq`). Asserted to be absent from the `topic` label of every
/// `shove_message_size_bytes` series.
const DLQ: &str = "metrics-sqs-dlq-size-dlq";
/// The main consumer's group label, which keeps its samples on their own
/// series.
const MAIN_GROUP: &str = "metrics-sqs-dlq-size-main";
/// What `consume_dlq_loop` reports: `run_dlq` takes no `ConsumerOptions`, so
/// no group at all, which `metrics::group_label` renders as `default`.
const DLQ_GROUP: &str = "default";

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Poll SNS and SQS against the LocalStack endpoint until both respond, or
/// panic after 30s, so the test does not race the container's boot.
async fn wait_for_localstack_ready(endpoint_url: &str) {
    let aws_config = aws_config::from_env()
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let sns = aws_sdk_sns::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let sns_ok = sns.list_topics().send().await.is_ok();
        let sqs_ok = sqs.list_queues().send().await.is_ok();
        if sns_ok && sqs_ok {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "LocalStack services not ready within 30s at {endpoint_url}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        // Dummy credentials for LocalStack.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let auth_token = std::env::var("LOCALSTACK_AUTH_TOKEN")
            .expect("LOCALSTACK_AUTH_TOKEN must be set to run SNS/SQS integration tests");

        let container = LocalStack::default()
            .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
            .start()
            .await
            .expect("failed to start LocalStack container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");
        let endpoint_url = format!("http://localhost:{port}");

        wait_for_localstack_ready(&endpoint_url).await;

        Self {
            container,
            endpoint_url,
        }
    }

    fn sns_config(&self) -> SnsConfig {
        SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(self.endpoint_url.clone()),
        }
    }

    async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sqs::Client::new(&aws_config)
    }
}

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
        self.count.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.get() >= target,
            }
        }
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

define_topic!(Orders, Order, TopologyBuilder::new(QUEUE).dlq().build());

/// Rejects every delivery, which on SQS means "set visibility to 0 and let
/// native redrive retire it" — the message reaches the DLQ without this test
/// seeding the queue by hand.
#[derive(Clone)]
struct RejectHandler(WaitableCounter);

impl MessageHandler<Orders> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.increment();
        Outcome::Reject
    }
}

/// Counts `handle_dead` calls so the test can wait on the drain rather than
/// sleeping against it.
#[derive(Clone)]
struct DlqHandler(WaitableCounter);

impl MessageHandler<Orders> for DlqHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: Order, _meta: DeadMessageMetadata, _: &()) {
        self.0.increment();
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

    let tb = TestBroker::start().await;
    let sns_client = SnsClient::new(&tb.sns_config())
        .await
        .expect("failed to create SNS client");
    let broker = Broker::<Sqs>::from_client(sns_client.clone());
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

    let handled = WaitableCounter::new();
    let drained = WaitableCounter::new();

    // ── Step 1: reject on the main path until native redrive retires the
    // message to the DLQ. `max_retries(1)` makes the retry gate short-circuit
    // from the second receive onward, so only the first receive reaches
    // `record_message_size`.
    let main_shutdown = CancellationToken::new();
    let main_shutdown_clone = main_shutdown.clone();
    let main_consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let handler = RejectHandler(handled.clone());
    let main_handle = tokio::spawn(async move {
        main_consumer
            .run::<Orders, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(main_shutdown_clone)
                    .with_prefetch_count(1)
                    .with_max_retries(1)
                    .with_consumer_group(MAIN_GROUP),
            )
            .await
    });

    // Poll with `visibility_timeout(0)` so the message stays immediately
    // available to the DLQ consumer started below.
    let sqs = tb.sqs_client().await;
    let dlq_url = sns_client
        .queue_registry()
        .get(DLQ)
        .await
        .expect("DLQ URL should be registered by topology declaration");
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let result = sqs
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(1)
                .wait_time_seconds(1)
                .visibility_timeout(0)
                .send()
                .await
                .expect("failed to poll DLQ");
            if !result.messages.unwrap_or_default().is_empty() {
                return;
            }
        }
    })
    .await
    .expect("message should arrive in the DLQ within 60 seconds");

    // Stop the main consumer *before* the drain, so nothing on the main path
    // can record while the DLQ series is being produced.
    main_shutdown.cancel();
    main_handle.await.expect("main consumer task panicked").ok();

    // ── Step 2: drain the DLQ.
    let dlq_consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let dlq_handler = DlqHandler(drained.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<Orders, _>(dlq_handler, ()).await });

    let reached = drained.wait_for(1, Duration::from_secs(30)).await;

    // `run_dlq` has no shutdown token of its own — it stops when the client's
    // token is cancelled. Without this the await below hangs until the job
    // times out.
    sns_client.shutdown().await;
    dlq_handle.await.expect("DLQ consumer task panicked").ok();

    assert!(
        reached,
        "timed out waiting for the DLQ drain: handled={} drained={}",
        handled.get(),
        drained.get(),
    );

    // Single, draining snapshot — taken only once both consumers have stopped,
    // so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();
    let observed = size_series(&snapshot);

    assert_eq!(
        drained.get(),
        1,
        "the DLQ consumer must have drained exactly one message"
    );

    assert_eq!(
        size_samples(&snapshot, QUEUE, DLQ_GROUP),
        vec![expected],
        "the SQS DLQ drain must record one shove_message_size_bytes sample, \
         carrying the encoded payload length, labelled with the source topic \
         and no consumer group; observed series: {observed:?}"
    );

    let main_samples = size_samples(&snapshot, QUEUE, MAIN_GROUP);
    assert!(
        !main_samples.is_empty() && main_samples.iter().all(|s| *s == expected),
        "the main loop's own samples must still be recorded, on their own \
         consumer_group series and all carrying the encoded payload length — \
         otherwise the assertion above could be satisfied by the main loop \
         alone; got {main_samples:?}, observed series: {observed:?}"
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
