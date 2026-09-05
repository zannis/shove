#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the generic Redis Streams batch consumer's metrics match
//! the same documented batch-wide contract `tests/metrics_inmemory_batch.rs`
//! pins for InMemory (`docs/pages/guides/observability.mdx`):
//!
//! - `shove_messages_consumed_total` is counted **per message**, under the
//!   batch's single outcome label — not once per flush.
//! - `shove_message_processing_duration_seconds` is observed **once per
//!   flush**, in message units it may cover many of.
//! - `with_handler_timeout_outcome(Outcome::Reject)` records
//!   `shove_messages_failed_total{reason="timeout"}` for the deadline AND
//!   `reason="rejected"` for the terminal retirement — **per message**.
//! - `shove_message_size_bytes` samples every popped message, including one
//!   the oversize gate drops before the handler ever sees it.
//! - **Deviation from InMemory's F3 contract**: on Redis, a pre-handler drop
//!   (oversize / undecodable / missing-payload) is settled through
//!   `route_to_dlq` — reused verbatim from the single-message path, with or
//!   without a DLQ declared — and neither of its branches ever touches
//!   `shove_messages_discarded_total`; only `shove_messages_failed_total`
//!   is recorded. That is pre-existing single-message behaviour (Redis's
//!   pre-handler drops have never counted as a "discard", unlike InMemory's),
//!   and the batch path correctly inherits it by reusing the same function
//!   rather than reimplementing the write-back. So `discarded_total` for a
//!   pre-handler drop is `0` in BOTH the with-DLQ and no-DLQ cases here —
//!   the second test below pins that this holds without a DLQ too, not just
//!   with one.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary, install it
//! exactly once per test process, and each `#[tokio::test]` gets its own
//! Redis container so the two tests' snapshots never see each other's
//! counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};
use tokio_util::sync::CancellationToken;

use shove::handler::BatchMessageHandler;
use shove::markers::Redis;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::redis::{RedisConfig, RedisMode};
use shove::topology::TopologyBuilder;
use shove::{Backend, BatchConsumerOptions, Broker, define_topic};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchMessage {
    seq: u32,
    padding: String,
}

type Snapshot = std::collections::HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

fn counter(snapshot: &Snapshot, name: &str, extra: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            extra.iter().all(|(key, value)| {
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

fn histogram_samples(snapshot: &Snapshot, name: &str) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .flat_map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().copied().map(f64::from).collect::<Vec<f64>>()
            }
            other => panic!("{name} is not a histogram: {other:?}"),
        })
        .collect()
}

async fn start_broker(group: &str) -> Broker<Redis> {
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("failed to start Redis container");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("port");
    let url = format!("redis://{host}:{port}");
    let cfg = RedisConfig::new(RedisMode::Standalone { url }).with_group(group);
    let client = <Redis as Backend>::connect(cfg).await.expect("connect");
    // Leak the container deliberately: it lives for the remainder of this
    // one-shot test process, mirroring `tests/metrics_redis_malformed.rs`.
    std::mem::forget(container);
    Broker::<Redis>::from_client(client)
}

const GROUP: &str = "metrics-batch-grp";
const QUEUE: &str = "redis-metrics-batch";

define_topic!(
    MetricsBatchTopic,
    BatchMessage,
    TopologyBuilder::new(QUEUE).dlq().build()
);

/// Hangs well past `handler_timeout` on every call.
#[derive(Clone)]
struct HangingHandler {
    calls: Arc<AtomicUsize>,
}

impl BatchMessageHandler<MetricsBatchTopic> for HangingHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Outcome::Ack
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_metrics_match_the_documented_contract() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = start_broker(GROUP).await;
    broker
        .topology()
        .declare::<MetricsBatchTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<MetricsBatchTopic>(&BatchMessage {
            seq: 0,
            padding: String::new(),
        })
        .await
        .expect("publish");
    publisher
        .publish::<MetricsBatchTopic>(&BatchMessage {
            seq: 1,
            padding: String::new(),
        })
        .await
        .expect("publish");
    // Comfortably above a bare `BatchMessage`, far below this one.
    publisher
        .publish::<MetricsBatchTopic>(&BatchMessage {
            seq: 2,
            padding: "x".repeat(4096),
        })
        .await
        .expect("publish");

    let handler = HangingHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let calls = handler.calls.clone();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<MetricsBatchTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        // Unlike InMemory, a pre-handler drop (the oversized
                        // message) settles immediately and never joins the
                        // batch, so it does not count toward the size
                        // trigger here — only the 2 decoded messages do.
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_max_message_size(512)
                        .with_handler_timeout(Duration::from_millis(300))
                        .with_handler_timeout_outcome(Outcome::Reject)
                        .with_consumer_group(GROUP)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while calls.load(Ordering::SeqCst) < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the flush must have happened exactly once"
    );

    // Give the timed-out flush time to run its DeadLetter settlement.
    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_consumed_total",
            &[
                ("topic", QUEUE),
                ("consumer_group", GROUP),
                ("outcome", "reject")
            ],
        ),
        2,
        "consumed must count messages, not flushes"
    );

    assert_eq!(
        histogram_samples(&snapshot, "shove_message_processing_duration_seconds").len(),
        1,
        "processing duration must be observed once per flush, not once per message"
    );

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[
                ("topic", QUEUE),
                ("consumer_group", GROUP),
                ("reason", "timeout")
            ],
        ),
        2,
        "the timeout must be recorded once per message in the batch"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[
                ("topic", QUEUE),
                ("consumer_group", GROUP),
                ("reason", "rejected")
            ],
        ),
        2,
        "the terminal retirement must ALSO be recorded once per message — the doc-pinned 'counted twice' case"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[
                ("topic", QUEUE),
                ("consumer_group", GROUP),
                ("reason", "oversize")
            ],
        ),
        1,
    );

    assert_eq!(
        histogram_samples(&snapshot, "shove_message_size_bytes").len(),
        3,
        "message_size must sample every popped message, oversized ones included"
    );

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", QUEUE), ("reason", "oversize")]
        ),
        0,
        "a topic with a DLQ must not count its pre-handler drop as discarded"
    );
}

const NO_DLQ_GROUP: &str = "metrics-batch-no-dlq-grp";
const NO_DLQ_QUEUE: &str = "redis-metrics-batch-no-dlq";

define_topic!(
    MetricsBatchNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new(NO_DLQ_QUEUE).build()
);

#[derive(Clone)]
struct AckHandler;
impl BatchMessageHandler<MetricsBatchNoDlqTopic> for AckHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        Outcome::Ack
    }
}

/// Deviation from InMemory's F3 contract (see the module doc): on Redis, a
/// pre-handler drop settles through `route_to_dlq`, whose no-DLQ branch —
/// reused verbatim from the single-message path — records
/// `shove_messages_failed_total` but never `shove_messages_discarded_total`.
/// This pins that the no-DLQ case behaves exactly like the with-DLQ case
/// (both `0` on the discard counter), not like InMemory's fuller accounting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_dlq_pre_handler_drop_records_failed_but_not_discarded() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = start_broker(NO_DLQ_GROUP).await;
    broker
        .topology()
        .declare::<MetricsBatchNoDlqTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<MetricsBatchNoDlqTopic>(&BatchMessage {
            seq: 0,
            padding: String::new(),
        })
        .await
        .expect("publish");
    publisher
        .publish::<MetricsBatchNoDlqTopic>(&BatchMessage {
            seq: 1,
            padding: "x".repeat(4096),
        })
        .await
        .expect("publish");

    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<MetricsBatchNoDlqTopic, _>(
                    AckHandler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_max_message_size(512)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(600)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[("topic", NO_DLQ_QUEUE), ("reason", "oversize")]
        ),
        1,
        "the oversized message must be recorded as failed"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", NO_DLQ_QUEUE), ("reason", "oversize")]
        ),
        0,
        "Redis's route_to_dlq never records a discard for a pre-handler drop, \
         with or without a DLQ — this must match the with-DLQ case, not InMemory's F3 contract"
    );
}
