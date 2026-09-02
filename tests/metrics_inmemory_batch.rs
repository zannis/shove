//! Integration test: the generic InMemory batch consumer's metrics match the
//! documented batch-wide contract (`docs/pages/guides/observability.mdx`):
//!
//! - `shove_messages_consumed_total` is counted **per message**, under the
//!   batch's single outcome label — not once per flush.
//! - `shove_message_processing_duration_seconds` is observed **once per
//!   flush**, in message units it may cover many of.
//! - `with_handler_timeout_outcome(Outcome::Reject)` records
//!   `shove_messages_failed_total{reason="timeout"}` for the deadline AND
//!   `reason="rejected"` for the terminal retirement — **per message**, the
//!   "counted twice" rule the observability guide pins.
//! - `shove_message_size_bytes` samples every popped message, including one
//!   the oversize gate drops before the handler ever sees it.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary, and install it
//! exactly once, so it does not race any other test that emits metrics (see
//! `tests/metrics_inmemory_discard.rs`).

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::inmemory::InMemoryConfig;
use shove::markers::InMemory;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use shove::{BatchConsumerOptions, define_topic};

const GROUP: &str = "metrics-batch-group";
const QUEUE: &str = "inmem-metrics-batch";

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

/// Hangs well past the configured `handler_timeout` on every call, so each
/// flush resolves through the timeout arm rather than returning normally.
/// Counts calls so the test can wait on exactly one flush before asserting.
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

#[tokio::test(flavor = "current_thread")]
async fn batch_metrics_match_the_documented_contract() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
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
                        // flush_len (2 decoded + 1 dropped) reaches this
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

    // Give the timed-out flush time to run its DeadLetter settlement before
    // asserting — the handler call above only proves the flush *started*.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();

    // `messages_consumed_total` is per message, under the batch's single
    // outcome — 2 messages reached the handler (the oversized one never
    // did), both resolving to the configured timeout outcome.
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

    // Exactly one duration sample for the one flush, however many messages
    // it covered.
    assert_eq!(
        histogram_samples(&snapshot, "shove_message_processing_duration_seconds").len(),
        1,
        "processing duration must be observed once per flush, not once per message"
    );

    // The "counted twice" rule: `timeout` fires once per message that timed
    // out (2 — the oversized one was never handed to the handler), and
    // `rejected` fires once per message the DeadLetter settlement retires
    // (the same 2).
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
        "the terminal retirement must ALSO be recorded once per message — \
         the doc-pinned 'counted twice' case"
    );
    // The oversized message is counted under its own precise reason, once —
    // never additionally as `rejected` (that would be the double-count bug
    // this crate's pre-handler-drop convention exists to avoid).
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

    // Every popped message samples its size, including the oversized one —
    // the histogram exists to size `max_message_size` against, so it must
    // see what the gate rejected too.
    assert_eq!(
        histogram_samples(&snapshot, "shove_message_size_bytes").len(),
        3,
        "message_size must sample every popped message, oversized ones included"
    );

    broker.close().await;
}
