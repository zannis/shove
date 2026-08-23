//! Integration test: a message rejected by a *pre-handler* gate (oversize)
//! must move `messages_failed_total` exactly once, under its precise reason.
//!
//! In-memory is the only backend that funnels a pre-handler reject back through
//! terminal outcome routing (to preserve ordering); every other backend routes
//! straight to the DLQ from the gate. That structural difference used to make
//! in-memory count a single oversized message twice — once as
//! `reason="oversize"` and again as `reason="rejected"` — so the same workload
//! produced a different total here than on Kafka/RabbitMQ/NATS/Redis/SQS.
//!
//! The topology below deliberately declares no DLQ: this is the "silent
//! discard" shape operators alert on, and the counter is the only signal.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot — keep this in its own integration binary so it does not race
//! with any other test that emits metrics.

#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::inmemory::InMemoryConfig;
use shove::{
    Broker, ConsumerOptions, ConsumerSupervisor, InMemory, MessageHandler, MessageMetadata,
    Outcome, Publisher, TopologyBuilder, define_topic,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Blob {
    value: String,
}

// No `.dlq(..)` — an oversized message here is dropped outright.
define_topic!(
    BlobTopic,
    Blob,
    TopologyBuilder::new("blob_discard_metrics").build()
);

/// Never actually invoked: every message in this test is rejected by the
/// oversize gate before the handler runs. Acking keeps the assertion honest —
/// if the gate ever stopped firing, the failure counter would be absent rather
/// than merely mislabelled.
#[derive(Clone)]
struct AckHandler;
impl MessageHandler<BlobTopic> for AckHandler {
    type Context = ();
    async fn handle(&self, _msg: Blob, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
}

/// Collect every `shove_messages_failed_total` sample as `(reason, count)`.
fn failed_counters(snapshotter: &Snapshotter) -> Vec<(String, u64)> {
    snapshotter
        .snapshot()
        .into_hashmap()
        .into_iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            let count = match value {
                DebugValue::Counter(c) => c,
                other => panic!("messages_failed_total is not a counter: {other:?}"),
            };
            (reason, count)
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn oversize_discard_counts_once_as_oversize() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker: Broker<InMemory> = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<BlobTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<InMemory> = broker.publisher().await.expect("publisher");

    // 64 bytes is far below the encoded size of the payload published next, so
    // the consumer's oversize gate fires before deserialization.
    let mut sup: ConsumerSupervisor<InMemory> = broker.consumer_supervisor();
    sup.register::<BlobTopic, _>(
        AckHandler,
        ConsumerOptions::<InMemory>::new().with_max_message_size(64),
    )
    .expect("register");

    publisher
        .publish::<BlobTopic>(&Blob {
            value: "x".repeat(4096),
        })
        .await
        .expect("publish");

    tokio::time::sleep(Duration::from_millis(100)).await;
    sup.cancellation_token().cancel();
    let outcome = sup
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly");

    let failed = failed_counters(&snapshotter);

    assert_eq!(
        failed,
        vec![("oversize".to_string(), 1)],
        "one oversized message must produce exactly one messages_failed_total \
         sample, labelled reason=oversize; got {failed:?}"
    );
}
