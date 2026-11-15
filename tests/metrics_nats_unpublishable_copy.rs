#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a retry or dead-letter copy that can never be published
//! is discarded and counted, never redelivered forever.
//!
//! A copy carries more headers than its original, so a message sized just
//! under a size limit fits as published and its copy does not. The server's
//! `max_payload` makes the client refuse the copy, and a stream's maximum
//! message size makes the server refuse it. Either way no retry can help, so
//! the original is retired with a confirmed ack and
//! `shove_messages_discarded_total` moves once.
//!
//! Each test installs the global `DebuggingRecorder`, so each needs a process
//! of its own, which `cargo nextest run` gives it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;
use futures_util::StreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::Nats as NatsContainer;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig, NatsConsumer};
use shove::outcome::Outcome;
use shove::topic::Topic as _;
use shove::topology::{SequenceFailure, TopologyBuilder};

/// The server's `max_payload` in the tests that need the client to refuse a
/// copy.
const MAX_PAYLOAD: usize = 2048;
/// Headers of a fresh publish, `Nats-Msg-Id` included, with room to spare.
const ORIGINAL_HEADROOM: usize = 150;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

struct TestBroker {
    _container: testcontainers::ContainerAsync<NatsContainer>,
    client: NatsClient,
}

impl TestBroker {
    /// A JetStream server, with `max_payload` set when given.
    async fn start(max_payload: Option<usize>) -> Self {
        let conf = format!(
            "{}jetstream {{ store_dir: \"/tmp/nats-js\" }}\n",
            max_payload.map_or_else(String::new, |n| format!("max_payload: {n}\n"))
        );
        let container = NatsContainer::default()
            .with_cmd(["--config", "/etc/nats-test.conf"])
            .with_copy_to("/etc/nats-test.conf", conf.into_bytes())
            .start()
            .await
            .expect("failed to start NATS container");
        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get NATS port");
        let client =
            NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Doc {
    key: String,
    body: String,
}

shove::define_topic!(
    Plain,
    Doc,
    TopologyBuilder::new("nats-unpub-plain")
        .dlq_named("nats-unpub-plain-dead")
        .build()
);

shove::define_topic!(
    Retrying,
    Doc,
    TopologyBuilder::new("nats-unpub-retry")
        .hold_queue(Duration::from_millis(100))
        .dlq_named("nats-unpub-retry-dead")
        .build()
);

shove::define_sequenced_topic!(
    Sequenced,
    Doc,
    |doc: &Doc| doc.key.clone(),
    TopologyBuilder::new("nats-unpub-seq")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(100))
        .dlq_named("nats-unpub-seq-dead")
        .build()
);

/// Handler that returns a fixed outcome and counts its calls.
#[derive(Clone)]
struct Fixed {
    outcome: Outcome,
    calls: WaitableCounter,
    keys: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Fixed {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            calls: WaitableCounter::new(),
            keys: Arc::default(),
        }
    }

    fn record(&self, doc: &Doc) -> Outcome {
        if let Ok(mut keys) = self.keys.lock() {
            keys.push(doc.key.clone());
        }
        self.calls.increment();
        self.outcome.clone()
    }
}

impl MessageHandler<Plain> for Fixed {
    type Context = ();
    async fn handle(&self, msg: Doc, _: MessageMetadata, _: &()) -> Outcome {
        self.record(&msg)
    }
}

impl MessageHandler<Retrying> for Fixed {
    type Context = ();
    async fn handle(&self, msg: Doc, _: MessageMetadata, _: &()) -> Outcome {
        self.record(&msg)
    }
}

impl MessageHandler<Sequenced> for Fixed {
    type Context = ();
    async fn handle(&self, msg: Doc, _: MessageMetadata, _: &()) -> Outcome {
        self.record(&msg)
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

fn counter_total(snapshot: &Snapshot, name: &str, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

fn discarded(snapshot: &Snapshot) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (reason, *n),
                other => panic!("discarded is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

/// Publishes `payload` as-is to `subject`, bypassing the typed publisher.
async fn publish_raw(
    client: &NatsClient,
    subject: &str,
    sequence_key: Option<&str>,
    payload: Vec<u8>,
) {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", uuid_like(subject));
    if let Some(key) = sequence_key {
        headers.insert("Shove-Sequence-Key", key);
    }
    client
        .jetstream()
        .publish_with_headers(subject.to_string(), headers, Bytes::from(payload))
        .await
        .expect("publish")
        .await
        .expect("publish ack");
}

fn uuid_like(subject: &str) -> String {
    format!("00000000-0000-4000-8000-{:012x}", subject.len())
}

/// Waits until the stream's consumers have acked everything and nothing is
/// pending, and returns the highest number of deliveries observed.
async fn wait_until_retired(client: &NatsClient, stream: &str, timeout: Duration) -> bool {
    let js = client.jetstream();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(mut stream) = js.get_stream(stream).await {
            let mut consumers = stream.consumers();
            let mut seen = false;
            let mut idle = true;
            while let Some(Ok(info)) = consumers.next().await {
                seen = true;
                idle &= info.num_pending == 0 && info.num_ack_pending == 0;
            }
            let messages = stream.info().await.map(|i| i.state.messages).unwrap_or(1);
            if seen && idle && messages == 0 {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn stream_messages(client: &NatsClient, stream: &str) -> u64 {
    client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("stream")
        .get_info()
        .await
        .expect("info")
        .state
        .messages
}

async fn limit_stream_message_size(client: &NatsClient, stream: &str, max: i32) {
    let js = client.jetstream();
    let mut config: StreamConfig = js
        .get_stream(stream)
        .await
        .expect("stream")
        .get_info()
        .await
        .expect("info")
        .config;
    config.max_message_size = max;
    js.update_stream(config).await.expect("update stream");
}

fn install_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");
    snapshotter
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The undecodable original fits under `max_payload`, its dead letter does
/// not: the client refuses the dead letter and the original is retired after
/// one delivery.
#[tokio::test]
async fn an_unpublishable_dead_letter_of_an_undecodable_message_is_discarded() {
    let snapshotter = install_recorder();
    let tb = TestBroker::start(Some(MAX_PAYLOAD)).await;
    let broker = tb.broker();
    broker.topology().declare::<Plain>().await.expect("declare");

    publish_raw(
        &tb.client,
        Plain::topology().queue(),
        None,
        vec![b'x'; MAX_PAYLOAD - ORIGINAL_HEADROOM],
    )
    .await;

    let handler = Fixed::new(Outcome::Ack);
    let shutdown = CancellationToken::new();
    let consumer = NatsConsumer::new(tb.client.clone());
    let task = {
        let (handler, shutdown) = (handler.clone(), shutdown.clone());
        tokio::spawn(async move {
            consumer
                .run::<Plain, _>(
                    handler,
                    (),
                    ConsumerOptions::<Nats>::new().with_shutdown(shutdown),
                )
                .await
        })
    };

    let retired = wait_until_retired(&tb.client, "nats-unpub-plain", Duration::from_secs(30)).await;
    shutdown.cancel();
    broker.close().await;
    task.await.expect("consumer task panicked").ok();
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert!(retired, "the original was redelivered instead of retired");
    assert_eq!(
        handler.calls.get(),
        0,
        "an undecodable message has no handler call"
    );
    assert_eq!(
        counter_total(&snapshot, "shove_messages_failed_total", "deserialize"),
        1,
        "exactly one delivery of the message"
    );
    assert_eq!(discarded(&snapshot), vec![("deserialize".to_string(), 1)]);
}

/// The server refuses the dead letter over the DLQ stream's maximum message
/// size, which is a server answer and not a client check.
#[tokio::test]
async fn a_dead_letter_over_the_stream_limit_is_discarded() {
    let snapshotter = install_recorder();
    let tb = TestBroker::start(None).await;
    let broker = tb.broker();
    broker.topology().declare::<Plain>().await.expect("declare");
    let payload_len = 1000;
    limit_stream_message_size(&tb.client, "nats-unpub-plain-dead", 1100).await;

    publish_raw(
        &tb.client,
        Plain::topology().queue(),
        None,
        vec![b'x'; payload_len],
    )
    .await;

    let shutdown = CancellationToken::new();
    let consumer = NatsConsumer::new(tb.client.clone());
    let task = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            consumer
                .run::<Plain, _>(
                    Fixed::new(Outcome::Ack),
                    (),
                    ConsumerOptions::<Nats>::new().with_shutdown(shutdown),
                )
                .await
        })
    };

    let retired = wait_until_retired(&tb.client, "nats-unpub-plain", Duration::from_secs(30)).await;
    let dead = stream_messages(&tb.client, "nats-unpub-plain-dead").await;
    shutdown.cancel();
    broker.close().await;
    task.await.expect("consumer task panicked").ok();
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert!(retired, "the original was redelivered instead of retired");
    assert_eq!(dead, 0, "the refused dead letter must not be stored");
    assert_eq!(
        counter_total(&snapshot, "shove_messages_failed_total", "deserialize"),
        1,
        "exactly one delivery of the message"
    );
    assert_eq!(discarded(&snapshot), vec![("deserialize".to_string(), 1)]);
}

/// A rejected message whose dead letter cannot be published is retired
/// after one handler call.
#[tokio::test]
async fn an_unpublishable_dead_letter_of_a_rejected_message_is_discarded() {
    let snapshotter = install_recorder();
    let tb = TestBroker::start(Some(MAX_PAYLOAD)).await;
    let broker = tb.broker();
    broker.topology().declare::<Plain>().await.expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<Plain>(&Doc {
            key: "k".into(),
            body: "y".repeat(MAX_PAYLOAD - ORIGINAL_HEADROOM),
        })
        .await
        .expect("publish");

    let handler = Fixed::new(Outcome::Reject);
    let shutdown = CancellationToken::new();
    let consumer = NatsConsumer::new(tb.client.clone());
    let task = {
        let (handler, shutdown) = (handler.clone(), shutdown.clone());
        tokio::spawn(async move {
            consumer
                .run::<Plain, _>(
                    handler,
                    (),
                    ConsumerOptions::<Nats>::new().with_shutdown(shutdown),
                )
                .await
        })
    };

    let retired = wait_until_retired(&tb.client, "nats-unpub-plain", Duration::from_secs(30)).await;
    shutdown.cancel();
    broker.close().await;
    task.await.expect("consumer task panicked").ok();
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert!(retired, "the original was redelivered instead of retired");
    assert_eq!(handler.calls.get(), 1, "the handler ran once");
    assert_eq!(discarded(&snapshot), vec![("rejected".to_string(), 1)]);
}

/// A retry whose republished copy cannot be published is retired after one
/// handler call, counted under `republish_failed`.
#[tokio::test]
async fn an_unpublishable_retry_copy_is_discarded() {
    let snapshotter = install_recorder();
    let tb = TestBroker::start(Some(MAX_PAYLOAD)).await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<Retrying>()
        .await
        .expect("declare");

    // A retry copy adds about 100 bytes of headers to its original.
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<Retrying>(&Doc {
            key: "k".into(),
            body: "y".repeat(MAX_PAYLOAD - ORIGINAL_HEADROOM),
        })
        .await
        .expect("publish");

    let handler = Fixed::new(Outcome::Retry);
    let shutdown = CancellationToken::new();
    let consumer = NatsConsumer::new(tb.client.clone());
    let task = {
        let (handler, shutdown) = (handler.clone(), shutdown.clone());
        tokio::spawn(async move {
            consumer
                .run::<Retrying, _>(
                    handler,
                    (),
                    ConsumerOptions::<Nats>::new()
                        .with_shutdown(shutdown)
                        .with_max_retries(3),
                )
                .await
        })
    };

    let retired = wait_until_retired(&tb.client, "nats-unpub-retry", Duration::from_secs(30)).await;
    shutdown.cancel();
    broker.close().await;
    task.await.expect("consumer task panicked").ok();
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert!(retired, "the original was redelivered instead of retired");
    assert_eq!(handler.calls.get(), 1, "the handler ran once");
    assert_eq!(
        discarded(&snapshot),
        vec![("republish_failed".to_string(), 1)]
    );
}

/// On a sequenced topic the unpublishable dead letter must not block the
/// shard: the message behind it is still delivered.
#[tokio::test]
async fn an_unpublishable_dead_letter_does_not_block_a_sequenced_shard() {
    let snapshotter = install_recorder();
    let tb = TestBroker::start(Some(MAX_PAYLOAD)).await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<Sequenced>()
        .await
        .expect("declare");

    let subject = format!("{}.shard.0", Sequenced::topology().queue());
    publish_raw(
        &tb.client,
        &subject,
        Some("poison"),
        vec![b'x'; MAX_PAYLOAD - ORIGINAL_HEADROOM],
    )
    .await;
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<Sequenced>(&Doc {
            key: "after".into(),
            body: "ok".into(),
        })
        .await
        .expect("publish");

    let handler = Fixed::new(Outcome::Ack);
    let shutdown = CancellationToken::new();
    let consumer = NatsConsumer::new(tb.client.clone());
    let task = {
        let (handler, shutdown) = (handler.clone(), shutdown.clone());
        tokio::spawn(async move {
            consumer
                .run_fifo::<Sequenced, _>(
                    handler,
                    (),
                    ConsumerOptions::<Nats>::new().with_shutdown(shutdown),
                )
                .await
        })
    };

    let delivered = handler.calls.wait_for(1, Duration::from_secs(30)).await;
    let retired = wait_until_retired(&tb.client, "nats-unpub-seq", Duration::from_secs(30)).await;
    shutdown.cancel();
    broker.close().await;
    task.await.expect("consumer task panicked").ok();
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert!(
        delivered,
        "the message behind the poison was never delivered"
    );
    assert!(retired, "the poison message was never retired");
    assert_eq!(
        counter_total(&snapshot, "shove_messages_failed_total", "deserialize"),
        1,
        "exactly one delivery of the poison message"
    );
    assert_eq!(discarded(&snapshot), vec![("deserialize".to_string(), 1)]);
}
