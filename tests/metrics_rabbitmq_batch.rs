//! Integration test: the RabbitMQ batch consumer's metrics match the
//! documented batch-wide contract (`docs/pages/guides/observability.mdx`),
//! mirroring `tests/metrics_inmemory_batch.rs` against a real broker:
//!
//! - `shove_messages_consumed_total` is counted **per message**, under the
//!   batch's single outcome label — not once per flush.
//! - `shove_message_processing_duration_seconds` is observed **once per
//!   flush**, in message units it may cover many of.
//! - `with_handler_timeout_outcome(Outcome::Reject)` records
//!   `shove_messages_failed_total{reason="timeout"}` for the deadline AND
//!   `reason="rejected"` for the terminal retirement — **per message**.
//! - A pre-handler drop is counted once under its precise reason, never
//!   additionally as `rejected`; its discard counts only when the topology
//!   has no DLQ to dead-letter it into.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary, and install it
//! exactly once, so it does not race any other test that emits metrics.

#![cfg(all(feature = "rabbitmq", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::codec::RawBytesCodec;
use shove::handler::BatchMessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig};
use shove::topology::TopologyBuilder;
use shove::{BatchConsumerOptions, define_topic};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

const GROUP: &str = "metrics-batch-group";
const QUEUE: &str = "rmq-metrics-batch";
const NO_DLQ_QUEUE: &str = "rmq-metrics-batch-no-dlq";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchMessage {
    seq: u32,
    padding: String,
}

define_topic!(
    MetricsBatchTopic,
    BatchMessage,
    TopologyBuilder::new(QUEUE).dlq().build()
);

define_topic!(
    MetricsBatchNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new(NO_DLQ_QUEUE).build()
);
// Raw view of the same no-DLQ queue, to inject an undecodable payload.
define_topic!(
    MetricsBatchNoDlqRawTopic,
    Vec<u8>,
    TopologyBuilder::new(NO_DLQ_QUEUE).build(),
    codec = RawBytesCodec
);

/// Hangs well past the configured `handler_timeout` on every call, so each
/// flush resolves through the timeout arm rather than returning normally.
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

/// Should never run — the no-DLQ test's only message is dropped pre-handler.
#[derive(Clone)]
struct UnreachableHandler {
    calls: Arc<AtomicUsize>,
}

impl BatchMessageHandler<MetricsBatchNoDlqTopic> for UnreachableHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

type Snapshot = HashMap<
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

/// A broker to run against: shared-container mode (`RABBITMQ_AMQP_URL`, one
/// private vhost for the whole binary — the two scenarios use disjoint queue
/// names) or a standalone container. Same split as every other rabbitmq test
/// binary.
struct BrokerEnv {
    amqp_url: String,
    mgmt_url: String,
    vhost: String,
    cleanup: Option<(String, String)>,
    _container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl BrokerEnv {
    /// Poll the management API until `queue`'s total message count (ready +
    /// unacked) reaches zero, or `timeout` elapses. The out-of-band "the
    /// settle landed" signal — the `DebuggingRecorder`'s `snapshot()` drains
    /// counter deltas, so polling snapshots would eat the very increments the
    /// final assertion needs.
    async fn wait_for_queue_drained(&self, queue: &str, timeout: Duration) -> u64 {
        let http = reqwest::Client::new();
        let vhost_path = if self.vhost == "/" {
            "%2F".to_string()
        } else {
            self.vhost.clone()
        };
        let url = format!("{}/api/queues/{vhost_path}/{queue}", self.mgmt_url);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last = u64::MAX;
        loop {
            if let Ok(resp) = http
                .get(&url)
                .basic_auth("guest", Some("guest"))
                .send()
                .await
                && resp.status().is_success()
                && let Ok(v) = resp.json::<serde_json::Value>().await
            {
                last = v.get("messages").and_then(|m| m.as_u64()).unwrap_or(0);
                if last == 0 {
                    return last;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn broker_env() -> BrokerEnv {
    if let Ok(base_amqp) = std::env::var("RABBITMQ_AMQP_URL") {
        let mgmt_url = std::env::var("RABBITMQ_MGMT_URL")
            .expect("RABBITMQ_MGMT_URL must be set when RABBITMQ_AMQP_URL is set");
        let vhost = format!("test-{}", uuid::Uuid::new_v4());
        let http = reqwest::Client::new();
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
            .json(&std::collections::HashMap::from([
                ("configure", ".*"),
                ("write", ".*"),
                ("read", ".*"),
            ]))
            .send()
            .await
            .expect("failed to set vhost permissions")
            .status();
        assert!(status.is_success(), "set permissions returned {status}");
        BrokerEnv {
            amqp_url: format!("{base_amqp}/{vhost}"),
            mgmt_url: mgmt_url.clone(),
            vhost: vhost.clone(),
            cleanup: Some((mgmt_url, vhost)),
            _container: None,
        }
    } else {
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
        BrokerEnv {
            amqp_url: format!("amqp://guest:guest@{host}:{amqp_port}"),
            mgmt_url: format!("http://{host}:{mgmt_port}"),
            vhost: "/".to_string(),
            cleanup: None,
            _container: Some(container),
        }
    }
}

/// One recorder install for the whole binary; both tests snapshot it. They
/// run serially (`--test-threads` notwithstanding, the recorder is global
/// state), so each uses its own queue names and asserts only labels scoped
/// to them.
#[tokio::test(flavor = "current_thread")]
async fn batch_metrics_match_the_documented_contract() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let env = broker_env().await;
    let client = RabbitMqClient::connect(&RabbitMqConfig::new(env.amqp_url.clone()))
        .await
        .expect("connect rabbitmq");
    let broker = Broker::<RabbitMqMarker>::from_client(client.clone());
    broker
        .topology()
        .declare::<MetricsBatchTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..2 {
        publisher
            .publish::<MetricsBatchTopic>(&BatchMessage {
                seq,
                padding: String::new(),
            })
            .await
            .expect("publish");
    }
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
                        // flush_len (2 decoded + 1 parked) reaches this
                        // exactly once — no second flush to confuse the count.
                        .with_max_batch_size(3)
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::SeqCst) < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the flush must have happened exactly once"
    );

    // Give the timed-out flush time to run its DeadLetter settlement (real
    // broker frames this time) before asserting.
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
        "the terminal retirement must ALSO be recorded once per message"
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
        "a pre-handler drop is counted once, under its precise reason"
    );
    assert_eq!(
        histogram_samples(&snapshot, "shove_message_size_bytes").len(),
        3,
        "message_size must sample every delivery, oversized ones included"
    );
    // This topology declares a DLQ, so the oversized pre-handler drop is
    // parked and dead-lettered (broker-side DLX) rather than discarded.
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", QUEUE), ("reason", "oversize")],
        ),
        0,
        "a topic with a DLQ must not count its pre-handler drop as discarded"
    );

    client.shutdown().await;

    // Second scenario, same recorder: a pre-handler drop on a topology with
    // NO DLQ must move `shove_messages_discarded_total` exactly once — the
    // all-poison window dead-letter arm settles the discard on the broker
    // accepting the nack, and with no DLX bound the message is genuinely
    // gone. Kept in this test (not a sibling `#[tokio::test]`) so the two
    // scenarios cannot race each other's snapshots on the one global
    // recorder.
    let client = RabbitMqClient::connect(&RabbitMqConfig::new(env.amqp_url.clone()))
        .await
        .expect("connect rabbitmq (no-dlq scenario)");
    let broker = Broker::<RabbitMqMarker>::from_client(client.clone());
    broker
        .topology()
        .declare::<MetricsBatchNoDlqTopic>()
        .await
        .expect("declare no-dlq");
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<MetricsBatchNoDlqRawTopic>(&b"not json".to_vec())
        .await
        .expect("publish raw");

    let handler = UnreachableHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let calls = handler.calls.clone();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<MetricsBatchNoDlqTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_consumer_group(GROUP)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // Wait out-of-band (management API) for the poison window to settle —
    // snapshots are drained on read, so they cannot be the polling signal.
    let remaining = env
        .wait_for_queue_drained(NO_DLQ_QUEUE, Duration::from_secs(10))
        .await;
    assert_eq!(
        remaining, 0,
        "the all-poison window must settle (nack accepted)"
    );
    // The discard confirm happens strictly before the broker even processes
    // the nack's effects, but give the metric a beat regardless.
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", NO_DLQ_QUEUE), ("reason", "deserialize")],
        ),
        1,
        "a no-DLQ pre-handler drop must count as discarded exactly once"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[
                ("topic", NO_DLQ_QUEUE),
                ("consumer_group", GROUP),
                ("reason", "deserialize")
            ],
        ),
        1,
        "and as failed exactly once, under its precise reason"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an all-poison window must never reach the handler"
    );

    client.shutdown().await;
    if let Some((mgmt_url, vhost)) = env.cleanup.clone() {
        let http = reqwest::Client::new();
        let _ = http
            .delete(format!("{mgmt_url}/api/vhosts/{vhost}"))
            .basic_auth("guest", Some("guest"))
            .send()
            .await;
    }
}
