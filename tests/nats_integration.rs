//! Integration tests for the NATS backend.
//!
//! Migrated to `Broker<Nats>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `NatsConsumer` constructed from
//! the underlying `NatsClient`.

#![cfg(feature = "nats")]

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::stream::{Config as JsStreamConfig, RetentionPolicy};
use serde::{Deserialize, Serialize};
use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer, NatsConsumerGroupConfig, NatsPublisher};
use shove::outcome::Outcome;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{NatsRetention, NatsStreamConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// WaitableCounter
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
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.get() >= target;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

// ---------------------------------------------------------------------------
// Topic definitions
// ---------------------------------------------------------------------------

shove::define_topic!(
    WorkTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-work")
        .dlq()
        .hold_queue(Duration::from_millis(200))
        .hold_queue(Duration::from_millis(500))
        .build()
);

shove::define_topic!(
    NoDlqTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-nodlq").build()
);

// Bridge-style topic: shove creates and owns a WorkQueue stream over an
// externally-owned subject (not just the queue name).
shove::define_topic!(
    BridgeTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-bridge")
        .nats_subjects(["bridge-feed.price.>"])
        .dlq()
        .build()
);

shove::define_topic!(
    DeferNoHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-defer-nohold").dlq().build()
);
shove::define_topic!(
    ManagedConfigTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-managed-cfg")
        .nats_stream_config(NatsStreamConfig {
            retention: NatsRetention::Limits,
            max_age: Some(Duration::from_secs(600)),
            max_bytes: Some(1_000_000),
            max_messages: None,
            // 1 replica: the single-node test container can't satisfy R3 (the
            // num_replicas mapping is covered by the topology unit test).
            num_replicas: 1,
        })
        .dlq()
        .build()
);
shove::define_topic!(
    ExternalTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-external")
        .nats_external_stream()
        .dlq()
        .build()
);
// Two topics over the SAME stream name with different (mutable) bounds, to prove
// re-declaring reconciles an existing stream rather than silently ignoring the
// new config. Same retention so the JetStream UPDATE is permitted.
shove::define_topic!(
    ReconcileUnboundedTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-reconcile")
        .nats_stream_config(NatsStreamConfig {
            retention: NatsRetention::Limits,
            max_age: None,
            max_bytes: None,
            max_messages: None,
            num_replicas: 1,
        })
        .build()
);
shove::define_topic!(
    ReconcileBoundedTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-reconcile")
        .nats_stream_config(NatsStreamConfig {
            retention: NatsRetention::Limits,
            max_age: None,
            max_bytes: Some(2_000_000),
            max_messages: None,
            num_replicas: 1,
        })
        .build()
);

shove::define_sequenced_topic!(
    SeqSkipTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-skip")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

// Sharded topic used to fault-inject a partial `publish_batch` failure: the
// stream backing this topic is created manually (see
// `publish_batch_drains_acks_after_partial_stream_failure`) with only shard 0
// in its subject list, so shard-1 messages have no stream to ack against.
shove::define_sequenced_topic!(
    SeqPartialTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-partial-fail")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .allow_message_loss()
        .build()
);

// ---------------------------------------------------------------------------
// Test harness: shared setup
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

const TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Reusable handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CountingHandler {
    counter: WaitableCounter,
}

impl CountingHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<BridgeTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<NoDlqTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<DeferNoHoldTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<SeqSkipTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

struct FixedOutcomeHandler(Outcome);

impl MessageHandler<WorkTopic> for FixedOutcomeHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.clone()
    }
}

#[derive(Clone)]
struct RetryThenAckHandler {
    retry_until: u32,
    counter: WaitableCounter,
}

impl RetryThenAckHandler {
    fn new(retry_until: u32) -> Self {
        Self {
            retry_until,
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for RetryThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.counter.get();
        self.counter.increment();
        if attempt < self.retry_until {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

#[derive(Clone)]
struct SlowHandler {
    delay: Duration,
    counter: WaitableCounter,
}

impl SlowHandler {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for SlowHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.counter.increment();
        Outcome::Ack
    }
}

#[derive(Clone)]
struct DlqRecordingHandler {
    counter: WaitableCounter,
}

impl DlqRecordingHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for DlqRecordingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
        self.counter.increment();
    }
}

#[derive(Clone)]
struct OrderRecordingHandler {
    records: Arc<Mutex<Vec<(String, u64)>>>,
    counter: WaitableCounter,
}

impl OrderRecordingHandler {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    async fn records(&self) -> Vec<(String, u64)> {
        self.records.lock().await.clone()
    }
}

impl MessageHandler<SeqSkipTopic> for OrderRecordingHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.records.lock().await.push((msg.order_id, msg.amount));
        self.counter.increment();
        Outcome::Ack
    }
}

// ===========================================================================
// Client lifecycle
// ===========================================================================

#[tokio::test]
async fn client_connect_and_shutdown() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    assert!(
        !client.shutdown_token().is_cancelled(),
        "shutdown token should not be cancelled before shutdown"
    );

    broker.close().await;

    assert!(
        client.shutdown_token().is_cancelled(),
        "shutdown token should be cancelled after shutdown"
    );
}

#[tokio::test]
async fn client_shutdown_cancels_token() {
    let tb = TestBroker::start().await;
    let client = tb.client();
    let token = client.shutdown_token();
    assert!(!token.is_cancelled());

    let broker = tb.broker();
    broker.close().await;
    assert!(token.is_cancelled());
}

// ===========================================================================
// Topology declaration
// ===========================================================================

#[tokio::test]
async fn topology_declares_standard_stream_and_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Verify main stream exists
    let stream = client.jetstream().get_stream("nats-work").await;
    assert!(stream.is_ok(), "main stream should exist");

    // Verify DLQ stream exists
    let dlq_stream = client.jetstream().get_stream("nats-work-dlq").await;
    assert!(dlq_stream.is_ok(), "DLQ stream should exist");

    broker.close().await;
}

#[tokio::test]
async fn topology_declares_sequenced_stream_with_shards() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    // Verify main stream exists with shard subjects
    let mut stream = client
        .jetstream()
        .get_stream("nats-seq-skip")
        .await
        .expect("sequenced stream should exist");

    let info = stream.info().await.expect("should get stream info");
    let subjects = &info.config.subjects;
    assert!(
        subjects.contains(&"nats-seq-skip.shard.0".to_string()),
        "stream should contain shard.0 subject"
    );
    assert!(
        subjects.contains(&"nats-seq-skip.shard.1".to_string()),
        "stream should contain shard.1 subject"
    );

    // Verify DLQ stream
    let dlq = client.jetstream().get_stream("nats-seq-skip-dlq").await;
    assert!(dlq.is_ok(), "DLQ stream should exist for sequenced topic");

    broker.close().await;
}

#[tokio::test]
async fn topology_idempotent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    broker.topology().declare::<WorkTopic>().await.unwrap();
    broker.topology().declare::<WorkTopic>().await.unwrap(); // second call should not fail

    let stream = client.jetstream().get_stream("nats-work").await;
    assert!(
        stream.is_ok(),
        "stream should still exist after double declare"
    );

    broker.close().await;
}

#[tokio::test]
async fn topology_managed_with_config_applies_retention_and_bounds() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<ManagedConfigTopic>()
        .await
        .unwrap();

    let mut stream = client
        .jetstream()
        .get_stream("nats-managed-cfg")
        .await
        .expect("managed stream should exist");
    let info = stream.info().await.expect("should get stream info");
    assert_eq!(
        info.config.retention,
        RetentionPolicy::Limits,
        "explicit retention policy should be applied"
    );
    assert_eq!(
        info.config.max_age,
        Duration::from_secs(600),
        "explicit max_age should be applied"
    );
    assert_eq!(
        info.config.max_bytes, 1_000_000,
        "explicit max_bytes should be applied"
    );

    broker.close().await;
}

#[tokio::test]
async fn topology_external_binds_to_preprovisioned_stream() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    // Simulate infra provisioning the stream with its own config (Limits +
    // max_age) BEFORE the consumer starts.
    client
        .jetstream()
        .create_stream(JsStreamConfig {
            name: "nats-external".to_string(),
            subjects: vec!["nats-external".to_string()],
            retention: RetentionPolicy::Limits,
            max_age: Duration::from_secs(300),
            ..Default::default()
        })
        .await
        .expect("infra pre-provisions the stream");

    // External mode binds to it without recreating.
    broker.topology().declare::<ExternalTopic>().await.unwrap();

    let mut stream = client
        .jetstream()
        .get_stream("nats-external")
        .await
        .expect("external stream should still exist");
    let info = stream.info().await.expect("should get stream info");
    assert_eq!(
        info.config.retention,
        RetentionPolicy::Limits,
        "shove must bind to the infra stream, not recreate it as WorkQueue"
    );
    assert_eq!(
        info.config.max_age,
        Duration::from_secs(300),
        "infra-provisioned config must be left untouched"
    );

    // shove still owns its own DLQ stream in external mode.
    assert!(
        client
            .jetstream()
            .get_stream("nats-external-dlq")
            .await
            .is_ok(),
        "shove should create its own DLQ even when the source stream is external"
    );

    broker.close().await;
}

#[tokio::test]
async fn topology_external_fails_fast_when_stream_absent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    // No stream provisioned → external declare must error, not silently create one.
    let result = broker.topology().declare::<ExternalTopic>().await;
    assert!(
        result.is_err(),
        "external mode must fail fast when the stream is not provisioned"
    );

    broker.close().await;
}

#[tokio::test]
async fn topology_managed_config_reconciles_existing_stream() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    // First declare creates the stream unbounded.
    broker
        .topology()
        .declare::<ReconcileUnboundedTopic>()
        .await
        .unwrap();
    let mut stream = client
        .jetstream()
        .get_stream("nats-reconcile")
        .await
        .expect("stream created on first declare");
    assert_eq!(
        stream.info().await.unwrap().config.max_bytes,
        -1,
        "stream starts unbounded"
    );

    // Re-declaring the same stream with a max_bytes bound must RECONCILE it, not
    // silently leave the old config in place (the P2 fix: create_or_update_stream).
    broker
        .topology()
        .declare::<ReconcileBoundedTopic>()
        .await
        .unwrap();
    let mut stream = client
        .jetstream()
        .get_stream("nats-reconcile")
        .await
        .expect("stream still exists");
    assert_eq!(
        stream.info().await.unwrap().config.max_bytes,
        2_000_000,
        "re-declare with a changed config must update the existing stream"
    );

    broker.close().await;
}

// ===========================================================================
// Basic publish & consume
// ===========================================================================

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "simple-1".into(),
            content: "hello".into(),
        })
        .await
        .expect("publish should succeed");

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "should receive 1 message"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 1);
    broker.close().await;
}

#[tokio::test]
async fn publish_and_consume_with_headers() {
    #[derive(Clone)]
    struct HeaderCapture(Arc<Mutex<HashMap<String, String>>>);

    impl MessageHandler<WorkTopic> for HeaderCapture {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            *self.0.lock().await = (*meta.headers).clone();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-abc-123".to_string());

    publisher
        .publish_with_headers::<WorkTopic>(
            &SimpleMessage {
                id: "hdr-1".into(),
                content: "with headers".into(),
            },
            headers,
        )
        .await
        .expect("publish_with_headers should succeed");

    let captured = Arc::new(Mutex::new(HashMap::new()));
    let handler = HeaderCapture(captured.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    let result = tokio::time::timeout(TIMEOUT, async {
        loop {
            let map = captured.lock().await;
            if !map.is_empty() {
                return map.clone();
            }
            drop(map);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    shutdown.cancel();
    handle.await.unwrap().ok();

    let headers_received = result.expect("should receive headers within timeout");
    assert_eq!(
        headers_received.get("x-trace-id").map(|s| s.as_str()),
        Some("trace-abc-123"),
    );
    broker.close().await;
}

#[tokio::test]
async fn publish_and_consume_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("batch-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(5, TIMEOUT).await,
        "should receive all 5 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 5);
    broker.close().await;
}

#[tokio::test]
async fn publish_batch_happy_path_reports_accurate_count() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = NatsPublisher::new(client).await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("count-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    let (succeeded, result) = publisher.publish_batch::<WorkTopic>(&messages).await;
    assert!(result.is_ok(), "expected success, got {result:?}");
    assert_eq!(succeeded, 5, "all 5 messages should be reported as stored");

    broker.close().await;
}

/// Regression test for the `publish_batch` bug where a mid-batch failure
/// (either at submission or at ack time) abandoned every already-submitted
/// ack, always reporting `succeeded == 0` even though some messages were
/// genuinely stored. The stream backing `SeqPartialTopic` is created here
/// with only shard 0 in its subject list, so messages that hash to shard 1
/// have no stream to ack against and fail at ack time while shard-0 messages
/// succeed — a real partial failure, not a simulated one.
#[tokio::test]
async fn publish_batch_drains_acks_after_partial_stream_failure() {
    let tb = TestBroker::start().await;
    let client = tb.client();

    client
        .jetstream()
        .create_stream(JsStreamConfig {
            name: "nats-partial-fail".to_string(),
            subjects: vec!["nats-partial-fail.shard.0".to_string()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
        .expect("should create partial-coverage stream");

    let publisher = NatsPublisher::new(client.clone()).await.unwrap();
    let messages: Vec<OrderMessage> = (1..=20)
        .map(|i| OrderMessage {
            order_id: format!("order-{i}"),
            amount: i,
        })
        .collect();

    let (succeeded, result) = publisher.publish_batch::<SeqPartialTopic>(&messages).await;

    assert!(
        result.is_err(),
        "expected a partial failure from the shard-1 messages with no stream"
    );
    assert!(succeeded > 0, "shard-0 acks must not be abandoned");
    assert!(
        succeeded < messages.len() as u64,
        "shard-1 messages have no stream and must not be counted as stored"
    );

    let mut stream = client
        .jetstream()
        .get_stream("nats-partial-fail")
        .await
        .expect("stream should exist");
    let info = stream.info().await.expect("should get stream info");
    assert_eq!(
        info.state.messages, succeeded,
        "reported success count must match what NATS actually stored"
    );
}

// ===========================================================================
// Rejection & DLQ
// ===========================================================================

#[tokio::test]
async fn rejected_message_lands_in_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "reject-1".into(),
            content: "reject me".into(),
        })
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Reject),
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Verify message arrives in DLQ via a DLQ consumer
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ should receive rejected message"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

#[tokio::test]
async fn dlq_consumer_handles_dead_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "dlq-test".into(),
            content: "dead".into(),
        })
        .await
        .unwrap();

    // Step 1: reject to get message into DLQ
    let shutdown1 = CancellationToken::new();
    let sc1 = shutdown1.clone();
    let c1 = NatsConsumer::new(client.clone());
    let h1 = tokio::spawn(async move {
        c1.run::<WorkTopic, _>(
            FixedOutcomeHandler(Outcome::Reject),
            (),
            ConsumerOptions::<Nats>::new()
                .with_shutdown(sc1)
                .with_prefetch_count(1),
        )
        .await
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown1.cancel();
    h1.await.unwrap().ok();

    // Step 2: consume from DLQ
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let c2 = NatsConsumer::new(client.clone());
    let h2 = tokio::spawn(async move { c2.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ handler should receive 1 dead message"
    );
    assert_eq!(dlq_handler.counter.get(), 1);

    broker.close().await;
    h2.await.unwrap().ok();
}

// ===========================================================================
// Retry mechanism
// ===========================================================================

#[tokio::test]
async fn retry_then_ack_succeeds() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "retry-ack".into(),
            content: "retry then ack".into(),
        })
        .await
        .unwrap();

    let handler = RetryThenAckHandler::new(1);
    let counter = handler.counter.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "should have at least 2 handler calls"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn max_retries_sends_to_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "always-retry".into(),
            content: "exhaust retries".into(),
        })
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Retry),
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler
            .counter
            .wait_for(1, Duration::from_secs(30))
            .await,
        "exhausted-retry message should land in DLQ"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

// `max_retries = N` must allow 1 initial attempt + N retries before the
// message is dead-lettered (the documented contract). With max_retries=2 the
// handler runs exactly 3 times.
#[tokio::test]
async fn max_retries_allows_initial_plus_n_retries() {
    struct CountingRetry(WaitableCounter);
    impl MessageHandler<WorkTopic> for CountingRetry {
        type Context = ();
        async fn handle(&self, _: SimpleMessage, _: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            Outcome::Retry
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "retry-cap".into(),
            content: "exhaust retries".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = CountingRetry(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    // 1 initial + 2 retries = 3 attempts, then DLQ.
    assert!(
        counter.wait_for(3, Duration::from_secs(30)).await,
        "should reach 3 attempts"
    );
    // Allow any erroneous 4th redelivery to surface before asserting.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        counter.get(),
        3,
        "max_retries=2 must allow 1 initial + 2 retries = 3 attempts before DLQ"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
}

// ===========================================================================
// Defer mechanism
// ===========================================================================

#[tokio::test]
async fn defer_redelivers_message() {
    struct DeferThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let prev = self.0.get();
            self.0.increment();
            if prev == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "defer-1".into(),
            content: "defer then ack".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = DeferThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "should be called at least 2 times (1 defer + 1 ack)"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// Concurrent consumption
// ===========================================================================

#[tokio::test]
async fn concurrent_consume_processes_all_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=10)
        .map(|i| SimpleMessage {
            id: format!("cc-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(10, Duration::from_secs(30)).await,
        "should receive all 10 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 10);
    broker.close().await;
}

#[tokio::test]
async fn concurrent_consume_mixed_outcomes() {
    struct MixedHandler(WaitableCounter, WaitableCounter);

    impl MessageHandler<WorkTopic> for MixedHandler {
        type Context = ();
        async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            if msg.id.ends_with("-reject") {
                self.1.increment();
                Outcome::Reject
            } else {
                self.0.increment();
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("ack-{i}"),
                content: "ack".into(),
            })
            .await
            .unwrap();
    }
    for i in 0..2 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("{i}-reject"),
                content: "reject".into(),
            })
            .await
            .unwrap();
    }

    let ack_counter = WaitableCounter::new();
    let reject_counter = WaitableCounter::new();
    let handler = MixedHandler(ack_counter.clone(), reject_counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        ack_counter.wait_for(3, TIMEOUT).await,
        "should ack 3 messages"
    );
    assert!(
        reject_counter.wait_for(2, TIMEOUT).await,
        "should reject 2 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn graceful_shutdown_drains_inflight() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "drain-1".into(),
            content: "slow".into(),
        })
        .await
        .unwrap();

    let handler = SlowHandler::new(Duration::from_millis(500));
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        result.is_ok(),
        "consumer should exit within timeout after shutdown"
    );

    assert!(
        handler.counter.get() >= 1,
        "in-flight handler should have completed"
    );
    broker.close().await;
}

// ===========================================================================
// Handler timeout
// ===========================================================================

#[tokio::test]
async fn handler_timeout_triggers_retry() {
    struct TimeoutThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for TimeoutThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let attempt = self.0.get();
            self.0.increment();
            if attempt == 0 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "timeout-1".into(),
            content: "timeout".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = TimeoutThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1)
                    .with_handler_timeout(Duration::from_millis(500)),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "should retry after timeout"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// Registry default handler timeout reaches a registered handler when the
// per-group config does NOT call `with_handler_timeout`. Mirrors the raw
// `handler_timeout_triggers_retry` test but exercises the registry
// pre-resolution path.
#[tokio::test]
async fn registry_default_handler_timeout_triggers_retry() {
    use shove::ConsumerGroupConfig;

    #[derive(Clone)]
    struct TimeoutThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for TimeoutThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let attempt = self.0.get();
            self.0.increment();
            if attempt == 0 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "default-timeout-1".into(),
            content: "default timeout".into(),
        })
        .await
        .unwrap();

    let handler = TimeoutThenAck(WaitableCounter::new());
    let counter = handler.0.clone();
    let factory_handler = handler.clone();

    let mut group = broker
        .consumer_group()
        .with_default_handler_timeout(Duration::from_millis(500));
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(
                NatsConsumerGroupConfig::new(1..=1)
                    .with_prefetch_count(1)
                    .with_max_retries(5),
            ),
            move || factory_handler.clone(),
        )
        .await
        .unwrap();

    let token = group.cancellation_token();
    let cancel = token.clone();
    let observer = tokio::spawn(async move {
        if counter.wait_for(2, Duration::from_secs(30)).await {
            cancel.cancel();
        }
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    observer.await.unwrap();
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        handler.0.get() >= 2,
        "expected >=2 invocations (timeout+retry) via registry default, got {}",
        handler.0.get()
    );
    broker.close().await;
}

// ===========================================================================
// Sequenced (FIFO) consumption
// ===========================================================================

#[tokio::test]
async fn sequenced_consume_preserves_order() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let handler = OrderRecordingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(5, Duration::from_secs(30)).await,
        "should receive all 5 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let records = handler.records().await;
    let amounts: Vec<u64> = records.iter().map(|(_, a)| *a).collect();
    assert_eq!(amounts, vec![0, 1, 2, 3, 4], "messages should be in order");
    broker.close().await;
}

#[tokio::test]
async fn sequenced_skip_continues_after_rejection() {
    struct RejectFirstHandler {
        counter: WaitableCounter,
    }

    impl MessageHandler<SeqSkipTopic> for RejectFirstHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            if msg.amount == 0 {
                Outcome::Reject
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-B".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let counter = WaitableCounter::new();
    let handler = RejectFirstHandler {
        counter: counter.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(30)).await,
        "should process all 3 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn sequenced_multiple_keys_concurrent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "alice".into(),
                amount: i,
            })
            .await
            .unwrap();
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "bob".into(),
                amount: i + 100,
            })
            .await
            .unwrap();
    }

    let handler = OrderRecordingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(6, Duration::from_secs(30)).await,
        "should receive all 6 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let records = handler.records().await;
    let alice: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "alice")
        .map(|(_, a)| *a)
        .collect();
    let bob: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "bob")
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(alice, vec![0, 1, 2], "alice messages should be in order");
    assert_eq!(bob, vec![100, 101, 102], "bob messages should be in order");
    broker.close().await;
}

// ===========================================================================
// Consumer group
// ===========================================================================

#[tokio::test]
async fn consumer_group_processes_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("cg-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let config = NatsConsumerGroupConfig::new(2..=2)
        .with_prefetch_count(5)
        .with_max_retries(5);

    let mut group = broker.consumer_group();
    group
        .register::<WorkTopic, _>(ConsumerGroupConfig::new(config), move || {
            handler_clone.clone()
        })
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(5, Duration::from_secs(60)).await;
        t.cancel();
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());

    assert_eq!(handler.counter.get(), 5);
    broker.close().await;
}

// ===========================================================================
// Configurable NATS stream subject (bridge use case)
// ===========================================================================

#[tokio::test]
async fn nats_subjects_creates_workqueue_stream_over_external_subject() {
    use async_nats::jetstream::stream::RetentionPolicy;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<BridgeTopic>().await.unwrap();

    // Stream NAME stays the queue; only its captured subjects change.
    let mut stream = client
        .jetstream()
        .get_stream("nats-bridge")
        .await
        .expect("bridge stream should exist under the queue name");
    let info = stream.info().await.expect("stream info");

    assert_eq!(
        info.config.subjects,
        vec!["bridge-feed.price.>".to_string()],
        "stream should be created over the configured external subject"
    );
    assert_eq!(
        info.config.retention,
        RetentionPolicy::WorkQueue,
        "bridge stream keeps WorkQueue retention (delete-on-ack)"
    );

    broker.close().await;
}

#[tokio::test]
async fn nats_subjects_consumer_group_receives_messages_on_external_subject() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<BridgeTopic>().await.unwrap();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();
    let config = NatsConsumerGroupConfig::new(1..=1).with_prefetch_count(1);
    let mut group = broker.consumer_group();
    group
        .register::<BridgeTopic, _>(ConsumerGroupConfig::new(config), move || {
            handler_clone.clone()
        })
        .await
        .unwrap();

    // Upstream publishes a raw message to a CONCRETE subject under the wildcard
    // — the shove-owned WorkQueue stream captures it and the durable consumer
    // (filtered on the configured subject) delivers it to the handler.
    let payload = serde_json::to_vec(&SimpleMessage {
        id: "price-1".into(),
        content: "tick".into(),
    })
    .unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, "price-1");
    client
        .jetstream()
        .publish_with_headers(
            "bridge-feed.price.0xabc".to_string(),
            headers,
            bytes::Bytes::from(payload),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(1, Duration::from_secs(60)).await;
        t.cancel();
    });
    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(
        handler.counter.get(),
        1,
        "consumer group should receive the message published to the external subject"
    );

    broker.close().await;
}

// ===========================================================================
// Deserialization failure
// ===========================================================================

#[tokio::test]
async fn deserialization_failure_rejects_to_dlq() {
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
    use futures_util::StreamExt;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Publish raw invalid JSON directly to the stream
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, "bad-json-1");
    client
        .jetstream()
        .publish_with_headers(
            "nats-work".to_string(),
            headers,
            bytes::Bytes::from(b"not valid json".as_slice()),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    // Start consumer — should reject the bad message to DLQ
    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let _handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Verify it lands in the DLQ stream directly (the payload is not valid
    // T::Message, so run_dlq's handle_dead cannot be used here).
    let dlq_stream = client
        .jetstream()
        .get_stream("nats-work-dlq")
        .await
        .expect("DLQ stream should exist");
    let dlq_pull = dlq_stream
        .get_or_create_consumer(
            "test-dlq-reader",
            PullConsumerConfig {
                durable_name: Some("test-dlq-reader".into()),
                ..Default::default()
            },
        )
        .await
        .expect("DLQ consumer should be created");
    let mut msgs = dlq_pull.messages().await.unwrap();

    let dlq_msg = tokio::time::timeout(TIMEOUT, msgs.next())
        .await
        .expect("should receive DLQ message before timeout")
        .expect("stream should not be closed")
        .expect("message should be valid");

    assert_eq!(dlq_msg.payload.as_ref(), b"not valid json");
    dlq_msg.ack().await.unwrap();

    assert_eq!(
        handler.counter.get(),
        0,
        "handler should not be called for bad JSON"
    );

    shutdown.cancel();
    broker.close().await;
}

// ===========================================================================
// Edge cases
// ===========================================================================

#[tokio::test]
async fn consumer_run_on_undeclared_stream_fails() {
    shove::define_topic!(
        UndeclaredTopic,
        SimpleMessage,
        TopologyBuilder::new("nats-undeclared-xyz").build()
    );

    struct Noop;
    impl MessageHandler<UndeclaredTopic> for Noop {
        type Context = ();
        async fn handle(&self, _: SimpleMessage, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let client = tb.client();
    let consumer = NatsConsumer::new(client.clone());
    let shutdown = CancellationToken::new();

    let result = consumer
        .run::<UndeclaredTopic, _>(
            Noop,
            (),
            ConsumerOptions::<Nats>::new().with_shutdown(shutdown),
        )
        .await;

    assert!(result.is_err(), "run on undeclared stream should fail");
    tb.broker().close().await;
}

#[tokio::test]
async fn run_dlq_on_topic_without_dlq_fails() {
    struct Noop;
    impl MessageHandler<NoDlqTopic> for Noop {
        type Context = ();
        async fn handle(&self, _: SimpleMessage, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let client = tb.client();
    let consumer = NatsConsumer::new(client.clone());

    let result = consumer.run_dlq::<NoDlqTopic, _>(Noop, ()).await;
    assert!(result.is_err(), "run_dlq on topic without DLQ should fail");
    tb.broker().close().await;
}

#[tokio::test]
async fn defer_without_hold_queues_redelivers() {
    struct DeferThenAck(WaitableCounter);

    impl MessageHandler<DeferNoHoldTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let prev = self.0.get();
            self.0.increment();
            if prev == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<DeferNoHoldTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<DeferNoHoldTopic>(&SimpleMessage {
            id: "defer-nohold".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = DeferThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<DeferNoHoldTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "should be called at least 2 times"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn defer_preserves_retry_count() {
    struct DeferCheckRetry {
        counter: WaitableCounter,
        retry_counts: Arc<Mutex<Vec<u32>>>,
    }

    impl MessageHandler<WorkTopic> for DeferCheckRetry {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            self.retry_counts.lock().await.push(meta.retry_count);
            let call = self.counter.get();
            self.counter.increment();
            match call {
                0 => Outcome::Retry, // retry_count becomes 1
                1 => Outcome::Defer, // retry_count should still be 1
                _ => Outcome::Ack,
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "defer-retry".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let retry_counts = Arc::new(Mutex::new(Vec::new()));
    let handler = DeferCheckRetry {
        counter: counter.clone(),
        retry_counts: retry_counts.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(10)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(30)).await,
        "should be called 3 times"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let counts = retry_counts.lock().await;
    assert_eq!(counts[0], 0, "first call: retry_count should be 0");
    assert_eq!(
        counts[1], 1,
        "second call (after Retry): retry_count should be 1"
    );
    assert_eq!(
        counts[2], 1,
        "third call (after Defer): retry_count should still be 1"
    );

    broker.close().await;
}

// ===========================================================================
// JetStream stats provider
// ===========================================================================

#[tokio::test]
async fn jetstream_stats_provider_reports_pending_messages() {
    use shove::nats::{JetStreamStatsProvider, NatsQueueStatsProvider};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("stats-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let stats_provider = JetStreamStatsProvider::new(client.clone());
    let stats = stats_provider
        .get_queue_stats("nats-work")
        .await
        .expect("get_queue_stats should succeed");

    assert!(
        stats.messages_pending >= 5,
        "should report at least 5 pending messages, got {}",
        stats.messages_pending
    );

    broker.close().await;
}

#[tokio::test]
async fn jetstream_stats_provider_reports_zero_after_consumption() {
    use shove::nats::{JetStreamStatsProvider, NatsQueueStatsProvider};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("stats-zero-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "should consume all 3 messages"
    );

    // Shut down the consumer so all in-flight acks are flushed.
    shutdown.cancel();
    handle.await.unwrap().ok();

    // Poll until acks propagate and pending count drops to 0.
    let stats_provider = JetStreamStatsProvider::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = stats_provider
            .get_queue_stats("nats-work")
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "lag should be 0 after consuming all messages, still got {}",
            stats.messages_pending
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    broker.close().await;
}

// ===========================================================================
// Handler panic recovery
// ===========================================================================

#[tokio::test]
async fn handler_panic_does_not_crash_consumer() {
    struct PanicThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for PanicThenAck {
        type Context = ();
        async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            if msg.id == "panic-me" {
                panic!("intentional test panic");
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "panic-me".into(),
            content: "boom".into(),
        })
        .await
        .unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "normal".into(),
            content: "ok".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = PanicThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "consumer should recover from panic and process messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// Sequenced defer falls back to retry
// ===========================================================================

#[tokio::test]
async fn sequenced_defer_falls_back_to_retry() {
    struct DeferThenAck {
        counter: WaitableCounter,
        retry_counts: Arc<Mutex<Vec<u32>>>,
    }

    impl MessageHandler<SeqSkipTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
            self.retry_counts.lock().await.push(meta.retry_count);
            let call = self.counter.get();
            self.counter.increment();
            if call == 0 {
                Outcome::Defer // In FIFO mode, Defer should fall back to Retry
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "defer-key".into(),
            amount: 42,
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let retry_counts = Arc::new(Mutex::new(Vec::new()));
    let handler = DeferThenAck {
        counter: counter.clone(),
        retry_counts: retry_counts.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(30)).await,
        "should be called at least 2 times (1 defer->retry + 1 ack)"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let counts = retry_counts.lock().await;
    assert_eq!(counts[0], 0, "first call should have retry_count 0");
    assert_eq!(
        counts[1], 1,
        "second call should have retry_count 1 (Defer became Retry)"
    );

    broker.close().await;
}

// ---------------------------------------------------------------------------
// run_fifo_until_timeout — drain semantics for sequenced topics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_fifo_until_timeout_clean_drain() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    // Publish a small batch.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let counter = WaitableCounter::new();
    let counter_clone = counter.clone();

    #[derive(Clone)]
    struct CountSeqHandler(WaitableCounter);
    impl MessageHandler<SeqSkipTopic> for CountSeqHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            Outcome::Ack
        }
    }

    let consumer = NatsConsumer::new(client.clone());

    // Wait until the handler has acked all 5, then send the signal.
    let signal = async move {
        let _ = counter_clone.wait_for(5, Duration::from_secs(30)).await;
        // Brief grace period to let the shard finish its ack round trip.
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(
            CountSeqHandler(counter.clone()),
            (),
            ConsumerOptions::<Nats>::new().with_max_retries(3),
            signal,
            Duration::from_secs(10),
        )
        .await;

    assert!(
        outcome.is_clean(),
        "expected clean outcome, got {outcome:?}"
    );
    assert_eq!(counter.get(), 5);

    broker.close().await;
}

#[tokio::test]
async fn run_fifo_until_timeout_observes_handler_panic() {
    // Shard-level panic boundary: the NATS FIFO consume loop spawns each
    // handler via `tokio::spawn` and awaits it through a oneshot channel.
    // When the handler panics, the task panics before sending, the receiver
    // closes, and the consume loop treats the closed channel as `Outcome::Retry`.
    // The panic never propagates out of the shard task — it always returns `Ok(())`.
    // As a result, `outcome.panics` and `outcome.errors` will both be zero.
    // This test documents and verifies that contract: the harness does not
    // crash or deadlock when handlers panic, and the outcome is clean.
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    #[derive(Clone)]
    struct PanicHandler;
    impl MessageHandler<SeqSkipTopic> for PanicHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            panic!("intentional test panic");
        }
    }

    let consumer = NatsConsumer::new(client.clone());
    // max_retries=1: dispatched at retry_count 0 (0 < 1) where it panics
    // (Retry) and is republished, then dispatched again at retry_count 1 where
    // 1 >= max_retries routes it to the DLQ.
    let opts = ConsumerOptions::<Nats>::new().with_max_retries(1);

    // Generous signal — give the shard time to pull the message, panic, and DLQ it.
    let signal = tokio::time::sleep(Duration::from_secs(10));

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(
            PanicHandler,
            (),
            opts,
            signal,
            Duration::from_secs(10),
        )
        .await;

    // The shard absorbs handler panics at the oneshot channel level (see above),
    // so neither panics nor errors are incremented. The harness returns cleanly.
    assert!(
        !outcome.timed_out,
        "harness must not hang on handler panics; got {outcome:?}"
    );
    assert_eq!(
        outcome.panics, 0,
        "handler panics are absorbed at the shard boundary; got {outcome:?}"
    );
    assert_eq!(
        outcome.errors, 0,
        "handler panics are absorbed at the shard boundary; got {outcome:?}"
    );

    broker.close().await;
}

#[tokio::test]
async fn run_fifo_until_timeout_flags_timeout_when_drain_overruns() {
    // The NATS FIFO consume loop processes messages sequentially: it awaits the
    // oneshot receiver for each handler before moving to the next message.
    // A handler currently in-flight when shutdown fires keeps running until it
    // completes. With a slow handler (60 s sleep) and a short drain budget
    // (500 ms), the timeout fires before the shard finishes, the JoinSet
    // abort_all's the handle, and timed_out is set to true.
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    // Use a counter so the signal only fires once the handler is in-flight,
    // avoiding a race where the signal fires before the message is delivered.
    let started = WaitableCounter::new();
    let started_clone = started.clone();

    #[derive(Clone)]
    struct SlowHandler(WaitableCounter);
    impl MessageHandler<SeqSkipTopic> for SlowHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            // Block well beyond drain timeout.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Outcome::Ack
        }
    }

    let consumer = NatsConsumer::new(client.clone());
    // max_retries=1 so the initial delivery (retry_count=0) passes the
    // max-retries check and the handler is actually dispatched.
    // Disable the handler timeout so the 60 s sleep is not interrupted before
    // the drain budget expires.
    let opts = ConsumerOptions::<Nats>::new()
        .with_max_retries(1)
        .without_handler_timeout();

    // Signal fires only once the handler is confirmed in-flight; drain budget
    // is much shorter than the handler sleep.
    let signal = async move {
        started_clone.wait_for(1, Duration::from_secs(30)).await;
        // Small gap to make sure the handler is past its counter increment and
        // is blocked in the sleep before we fire shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let drain = Duration::from_millis(500);

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(SlowHandler(started), (), opts, signal, drain)
        .await;

    // STRICT — handler ignores shutdown, drain budget runs out, shard aborted.
    assert!(outcome.timed_out, "expected timed_out, got {outcome:?}");
    assert_eq!(outcome.exit_code(), 3);

    broker.close().await;
}

// ===========================================================================
// ConsumerGroup::register_fifo
// ===========================================================================

/// Consumer group `register_fifo` drains all messages via `run_until_timeout`.
#[tokio::test]
async fn consumer_group_register_fifo_drains_via_run_until_timeout() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<SeqSkipTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::default()),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    let counter = handler.counter.clone();
    let signal = async move {
        counter.wait_for(5, Duration::from_secs(30)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "outcome was {outcome:?}");
    assert_eq!(handler.counter.get(), 5);

    broker.close().await;
}

// ===========================================================================
// ack_wait derivation
//
// JetStream redelivers any message not acked within ack_wait; shove derives
// it as max(3 x handler timeout, 30s) at every consumer-creation site so a
// handler running to its limit never has its message redelivered mid-flight.
// ===========================================================================

shove::define_topic!(
    AckWaitExplicitTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-ackwait-explicit").build()
);

shove::define_topic!(
    AckWaitDefaultTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-ackwait-default").build()
);

shove::define_sequenced_topic!(
    AckWaitFifoTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-ackwait-fifo")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .allow_message_loss()
        .build()
);

impl MessageHandler<AckWaitExplicitTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<AckWaitDefaultTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<AckWaitFifoTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

async fn durable_ack_wait(client: &NatsClient, stream: &str, consumer: &str) -> Duration {
    client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("stream should exist")
        .consumer_info(consumer)
        .await
        .expect("durable should exist")
        .config
        .ack_wait
}

#[tokio::test]
async fn group_durable_ack_wait_derived_from_explicit_handler_timeout() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    let handler = CountingHandler::new();
    let mut group = broker.consumer_group();
    group
        .register::<AckWaitExplicitTopic, _>(
            ConsumerGroupConfig::new(
                NatsConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(20)),
            ),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        durable_ack_wait(
            &client,
            "nats-ackwait-explicit",
            "nats-ackwait-explicit-consumer"
        )
        .await,
        Duration::from_secs(60),
        "ack_wait must be 3x the explicit 20s handler timeout"
    );

    // Upgrade path: a fresh registry re-registering the same topic (as a
    // restarted deployment would) upserts the EXISTING durable — the changed
    // ack_wait must be applied, not rejected by the consumer update.
    let handler2 = CountingHandler::new();
    let mut group2 = broker.consumer_group();
    group2
        .register::<AckWaitExplicitTopic, _>(
            ConsumerGroupConfig::new(
                NatsConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(40)),
            ),
            move || handler2.clone(),
        )
        .await
        .expect("upserting the existing durable with a changed ack_wait must succeed");

    assert_eq!(
        durable_ack_wait(
            &client,
            "nats-ackwait-explicit",
            "nats-ackwait-explicit-consumer"
        )
        .await,
        Duration::from_secs(120),
        "the upsert must update ack_wait on the existing durable"
    );
    broker.close().await;
}

#[tokio::test]
async fn group_durable_ack_wait_has_margin_over_default_handler_timeout() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    let handler = CountingHandler::new();
    let mut group = broker.consumer_group();
    group
        .register::<AckWaitDefaultTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::default()),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        durable_ack_wait(
            &client,
            "nats-ackwait-default",
            "nats-ackwait-default-consumer"
        )
        .await,
        Duration::from_secs(90),
        "ack_wait must be 3x the 30s default handler timeout, not the 30s server default"
    );
    broker.close().await;
}

#[tokio::test]
async fn fifo_shard_durable_gets_derived_ack_wait() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    let handler = CountingHandler::new();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<AckWaitFifoTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::default()),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    // Shard durables are created lazily when the shard tasks start — run the
    // group briefly so they exist.
    let signal = tokio::time::sleep(Duration::from_secs(3));
    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "outcome was {outcome:?}");

    assert_eq!(
        durable_ack_wait(&client, "nats-ackwait-fifo", "nats-ackwait-fifo-shard-0").await,
        Duration::from_secs(90),
        "shard durables must get the derived ack_wait, not the 30s server default"
    );
    broker.close().await;
}

// ===========================================================================
// Autoscaling
// ===========================================================================

shove::define_topic!(
    AutoscalingTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-autoscaling").build()
);

/// Autoscaling lifecycle: slow handlers + burst → `enable_autoscaling` →
/// clean drain. Mirrors `autoscaling_scales_up_and_drains_clean` from
/// `kafka_integration.rs` but exercises the NATS backend's `spawn_autoscaler`
/// and `NatsConsumerGroup::retiring` drain path.
#[tokio::test]
async fn autoscaling_scales_up_and_drains_clean() {
    use shove::AutoscalerConfig;
    use std::sync::atomic::AtomicUsize;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<AutoscalingTopic>()
        .await
        .unwrap();

    let processed = Arc::new(AtomicUsize::new(0));

    let mut group = broker.consumer_group();
    {
        let processed = processed.clone();
        group
            .register::<AutoscalingTopic, _>(
                ConsumerGroupConfig::new(
                    NatsConsumerGroupConfig::new(1..=4).with_prefetch_count(1),
                ),
                move || {
                    #[derive(Clone)]
                    struct SlowHandler(Arc<AtomicUsize>);
                    impl MessageHandler<AutoscalingTopic> for SlowHandler {
                        type Context = ();
                        async fn handle(
                            &self,
                            _: SimpleMessage,
                            _: MessageMetadata,
                            _: &(),
                        ) -> Outcome {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            self.0.fetch_add(1, Ordering::Relaxed);
                            Outcome::Ack
                        }
                    }
                    SlowHandler(processed.clone())
                },
            )
            .await
            .unwrap();
    }

    // Publish a burst large enough to build a sustained backlog.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..20u32 {
        publisher
            .publish::<AutoscalingTopic>(&SimpleMessage {
                id: format!("as-{i}"),
                content: format!("burst {i}"),
            })
            .await
            .unwrap();
    }

    // Fast autoscaler config: short poll + hysteresis so scale-up fires
    // within the first second of the test window.
    let cfg = AutoscalerConfig {
        poll_interval: Duration::from_millis(200),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_millis(200),
        cooldown_duration: Duration::from_millis(400),
    };

    // Run for 8 s — enough time for autoscaler to scale up and drain the
    // 20-message backlog through 4 max consumers at 200 ms each.
    let signal = tokio::time::sleep(Duration::from_millis(8000));
    let outcome = group
        .enable_autoscaling(cfg)
        .run_until_timeout(signal, Duration::from_secs(15))
        .await;

    assert!(
        outcome.is_clean(),
        "autoscaling group must drain cleanly; outcome: {outcome:?}"
    );
    assert_eq!(
        processed.load(Ordering::Relaxed),
        20,
        "all 20 published messages must be handled before the group drains"
    );

    broker.close().await;
}

// ===========================================================================
// SBE codec — binary frames over a real broker
// ===========================================================================

#[cfg(feature = "sbe")]
mod sbe_codec {
    use super::*;
    use shove::{SbeCodec, SbeFrame, SbeHeader, SbeMessage};
    use std::sync::Mutex as StdMutex;

    struct SbeOrder;
    impl SbeMessage for SbeOrder {
        const SCHEMA_ID: u16 = 42;
        const TEMPLATE_ID: u16 = 7;
    }

    shove::define_topic!(
        SbeTopic,
        SbeFrame<SbeOrder>,
        TopologyBuilder::new("nats-sbe").dlq().build(),
        codec = SbeCodec
    );

    fn encode_frame(price: u64, quantity: u64) -> SbeFrame<SbeOrder> {
        let header = SbeHeader {
            block_length: 16,
            template_id: SbeOrder::TEMPLATE_ID,
            schema_id: SbeOrder::SCHEMA_ID,
            version: 1,
        };
        let mut buf = header.to_bytes(SbeOrder::BYTE_ORDER).to_vec();
        buf.extend_from_slice(&price.to_le_bytes());
        buf.extend_from_slice(&quantity.to_le_bytes());
        SbeFrame::new(buf).expect("valid frame")
    }

    fn decode_fields(frame: &SbeFrame<SbeOrder>) -> (u64, u64) {
        let field = |offset: usize| {
            frame.body()[offset..offset + 8]
                .try_into()
                .map(u64::from_le_bytes)
                .expect("body holds two u64 fields")
        };
        (field(0), field(8))
    }

    #[derive(Clone, Default)]
    struct SbeCapture {
        seen: Arc<StdMutex<Vec<(u64, u64, u16)>>>,
    }

    impl SbeCapture {
        async fn wait_for(&self, target: usize, timeout: Duration) -> bool {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if self.seen.lock().unwrap().len() >= target {
                    return true;
                }
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    impl MessageHandler<SbeTopic> for SbeCapture {
        type Context = ();
        async fn handle(&self, msg: SbeFrame<SbeOrder>, _: MessageMetadata, _: &()) -> Outcome {
            let (price, quantity) = decode_fields(&msg);
            self.seen
                .lock()
                .unwrap()
                .push((price, quantity, msg.header().version));
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn sbe_frame_round_trips_through_broker() {
        let tb = TestBroker::start().await;
        let broker = tb.broker();
        let client = tb.client();
        broker.topology().declare::<SbeTopic>().await.unwrap();

        broker
            .publisher()
            .await
            .unwrap()
            .publish::<SbeTopic>(&encode_frame(250_000, 12))
            .await
            .expect("publish should succeed");

        let handler = SbeCapture::default();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();

        let consumer = NatsConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<SbeTopic, _>(
                    hc,
                    (),
                    ConsumerOptions::<Nats>::new()
                        .with_shutdown(sc)
                        .with_prefetch_count(1),
                )
                .await
        });

        assert!(
            handler.wait_for(1, TIMEOUT).await,
            "should receive the SBE frame"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        assert_eq!(handler.seen.lock().unwrap().clone(), vec![(250_000, 12, 1)]);
        broker.close().await;
    }
}
