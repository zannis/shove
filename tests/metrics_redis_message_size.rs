#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_message_size_bytes` on the Redis Streams consumer.
//!
//! # What is being asserted
//!
//! Redis was the only backend that never called `metrics::record_message_size`
//! — RabbitMQ, Kafka, NATS, SQS/SNS and InMemory all do — so a Redis user
//! following the observability guide got an empty histogram with no hint that
//! empty was expected. The gap survived review because
//! `record_message_size` carries `#[allow(dead_code)]` (it is genuinely
//! reachable from the other five backends), so no single-backend feature set
//! could surface it as an unused item.
//!
//! Redis has two consumer loops and the emission has to be in both:
//!
//! - `run_stream_loop_arc` — the sequential loop, which serves *both* the
//!   plain unsequenced consumer and every FIFO shard;
//! - `run_stream_loop_concurrent` — the semaphore-gated loop selected by
//!   `RedisConsumerGroupConfig::with_concurrent_processing(true)`.
//!
//! A test that only drove one of them would have passed against a half fix, so
//! this drives both against one container and asserts a sample per path,
//! carrying that path's own `topic` and `consumer_group` labels. Those two
//! labels hold the same string here, and that is not an accident of naming:
//! every backend's group registry sets the metric's `consumer_group` to the
//! registered queue name (`options.consumer_group = Some(self.queue)`), so
//! the assertion is that the label is threaded from the options at all rather
//! than falling back to `default`.
//!
//! The recorded value is asserted to be the *exact* encoded payload length, by
//! encoding through the topic's own codec rather than hard-coding a number:
//! the histogram is only useful for sizing `max_message_size` if it reports
//! the same bytes that limit is compared against.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` drains every metric it reads. So: own
//! integration binary, a single `#[test]`, and exactly one snapshot taken at
//! the end — progress is waited on through handler counters, never by peeking
//! at the metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `Snapshot`.
use shove::broker::Broker;
use shove::codec::{Codec, JsonCodec};
use shove::handler::MessageHandler;
use shove::markers::Redis;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::{SequenceFailure, SequencedTopic, TopologyBuilder, define_topic};

/// The Redis Streams group name the broker reads under.
const GROUP: &str = "redis-metrics-size-grp";

/// Stream name — and therefore also the expected `consumer_group` *label* —
/// of the topic driven through the concurrent loop.
const CONCURRENT_QUEUE: &str = "redis-metrics-size-concurrent";
/// Same, for the topic driven through the sequential/FIFO loop.
const SEQUENCED_QUEUE: &str = "redis-metrics-size-sequenced";

// ---------------------------------------------------------------------------
// Container harness
// ---------------------------------------------------------------------------

/// RAII wrapper around the test's Redis container.
///
/// The bare testcontainers `Drop` spawns a background tokio task to remove the
/// container, which can be aborted when the test runtime tears down — leaking
/// the container whenever the test panics. This runs `rm()` synchronously on a
/// dedicated runtime in a dedicated thread so cleanup completes before scope
/// exit, including on unwind from a failed assertion. Mirrors the equivalent
/// wrapper in `tests/metrics_redis_sequenced.rs`.
struct ContainerOnDrop(Option<testcontainers::ContainerAsync<RedisContainer>>);

impl Drop for ContainerOnDrop {
    fn drop(&mut self) {
        let Some(container) = self.0.take() else {
            return;
        };
        let handle = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                let _ = container.rm().await;
            });
        });
        let _ = handle.join();
    }
}

/// Start a Redis container and return it alongside its URL.
async fn start_redis() -> (ContainerOnDrop, String) {
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("start Redis container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("container Redis port");
    (
        ContainerOnDrop(Some(container)),
        format!("redis://{host}:{port}/"),
    )
}

/// Connect with a bounded retry loop — testcontainers can return before Redis
/// is actually accepting connections.
async fn connect_with_retry(url: &str, group: &str) -> Broker<Redis> {
    let start = std::time::Instant::now();
    let mut last_err: Option<shove::ShoveError> = None;
    while start.elapsed() < Duration::from_secs(30) {
        match Broker::<Redis>::new(
            RedisConfig::new(RedisMode::Standalone {
                url: url.to_owned(),
            })
            .with_group(group),
        )
        .await
        {
            Ok(broker) => return broker,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!(
        "connect to Redis at {url}: {}",
        last_err.expect("at least one error before the timeout")
    );
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reading {
    /// Distinguishes the two payloads by *length*, so a call site that sized
    /// the wrong buffer cannot accidentally match.
    sensor: String,
}

define_topic!(
    Concurrent,
    Reading,
    TopologyBuilder::new(CONCURRENT_QUEUE).build()
);

/// Sequenced sibling — drives `run_stream_loop_arc`, the loop the FIFO shards
/// and the plain consumer share.
struct Sequenced;

impl Topic for Sequenced {
    type Message = Reading;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new(SEQUENCED_QUEUE)
                // `Skip`, and one shard: this test is about the size sample,
                // so keep the run deterministic and free of cascade effects.
                .sequenced(SequenceFailure::Skip)
                .routing_shards(1)
                // Both required of every sequenced topic; unused here — the
                // handler acks.
                .hold_queue(Duration::from_millis(50))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Sequenced::sequence_key);
}

impl SequencedTopic for Sequenced {
    fn sequence_key(msg: &Reading) -> String {
        msg.sensor.clone()
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Counters {
    concurrent_calls: Arc<AtomicU32>,
    sequenced_calls: Arc<AtomicU32>,
}

#[derive(Clone)]
struct Handler;

impl MessageHandler<Concurrent> for Handler {
    type Context = Counters;
    async fn handle(&self, _msg: Reading, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        ctx.concurrent_calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

impl MessageHandler<Sequenced> for Handler {
    type Context = Counters;
    async fn handle(&self, _msg: Reading, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        ctx.sequenced_calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Snapshot helper
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

/// Every `shove_message_size_bytes` series in the snapshot, labels and all —
/// so a failing assertion says what *was* recorded rather than just that the
/// series it wanted is missing.
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
/// encoding, so the assertion stays true if the codec changes.
fn encoded_len<T: Topic>(msg: &T::Message) -> f64 {
    let bytes = <T::Codec as Codec<T::Message>>::encode(msg).expect("encode");
    bytes.len() as f64
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn both_consumer_loops_record_message_size() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_container, url) = start_redis().await;
    let broker = connect_with_retry(&url, GROUP).await;
    broker
        .topology()
        .declare::<Concurrent>()
        .await
        .expect("declare concurrent topic");
    broker
        .topology()
        .declare::<Sequenced>()
        .await
        .expect("declare sequenced topic");

    // Two distinct payload lengths, so a sample attributed to the wrong path
    // fails the assertion instead of coincidentally matching.
    let concurrent_msg = Reading { sensor: "c".into() };
    let sequenced_msg = Reading {
        sensor: "sequenced-sensor-0001".into(),
    };
    let expected_concurrent = encoded_len::<Concurrent>(&concurrent_msg);
    let expected_sequenced = encoded_len::<Sequenced>(&sequenced_msg);
    assert_ne!(
        expected_concurrent, expected_sequenced,
        "the two payloads must differ in length for this test to discriminate"
    );

    // Publish after `declare` — the consumer group is created at `$`, so
    // anything published earlier would never be delivered.
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<Concurrent>(&concurrent_msg)
        .await
        .expect("publish concurrent message");
    publisher
        .publish::<Sequenced>(&sequenced_msg)
        .await
        .expect("publish sequenced message");

    let ctx = Counters {
        concurrent_calls: Arc::new(AtomicU32::new(0)),
        sequenced_calls: Arc::new(AtomicU32::new(0)),
    };
    let mut group = broker.consumer_group().with_context(ctx.clone());
    group
        .register::<Concurrent, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::default().with_concurrent_processing(true),
            ),
            || Handler,
        )
        .await
        .expect("register concurrent consumer");
    group
        .register_fifo::<Sequenced, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default()),
            || Handler,
        )
        .await
        .expect("register sequenced consumer");

    // The size sample is recorded *before* the handler runs, so both handlers
    // having been called is a sufficient signal that both samples have landed.
    let concurrent_probe = ctx.concurrent_calls.clone();
    let sequenced_probe = ctx.sequenced_calls.clone();
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if concurrent_probe.load(Ordering::SeqCst) >= 1
                && sequenced_probe.load(Ordering::SeqCst) >= 1
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly: {outcome:?}");

    let snapshot = snapshotter.snapshot().into_hashmap();
    let observed = size_series(&snapshot);

    assert_eq!(
        ctx.concurrent_calls.load(Ordering::SeqCst),
        1,
        "the concurrent consumer must have handled its message"
    );
    assert_eq!(
        ctx.sequenced_calls.load(Ordering::SeqCst),
        1,
        "the sequenced consumer must have handled its message"
    );

    assert_eq!(
        size_samples(&snapshot, CONCURRENT_QUEUE, CONCURRENT_QUEUE),
        vec![expected_concurrent],
        "the concurrent Redis loop must record shove_message_size_bytes once, \
         with the encoded payload length, under its own topic and consumer \
         group; observed series: {observed:?}"
    );
    assert_eq!(
        size_samples(&snapshot, SEQUENCED_QUEUE, SEQUENCED_QUEUE),
        vec![expected_sequenced],
        "the sequenced Redis loop must record shove_message_size_bytes once, \
         with the encoded payload length, under its own topic and consumer \
         group; observed series: {observed:?}"
    );
}
