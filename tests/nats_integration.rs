//! Integration tests for the NATS backend.
//!
//! Migrated to `Broker<Nats>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `NatsConsumer` constructed from
//! the underlying `NatsClient`.

#![cfg(feature = "nats")]

use async_nats::header::{NATS_EXPECTED_STREAM, NATS_MESSAGE_ID};
use async_nats::jetstream::Message as JsMessage;
use async_nats::jetstream::stream::{Config as JsStreamConfig, RetentionPolicy, Source};
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
            // Register the waiter before checking the count: `notify_waiters`
            // stores no permit, so an increment landing between an
            // unregistered check and the await would otherwise be lost,
            // parking this task until the deadline.
            let mut notified = std::pin::pin!(self.signal.notified());
            notified.as_mut().enable();
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = &mut notified => {}
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
        .external()
        .dlq()
        .build()
);
// An infra-owned stream with a retry ladder. On it a `Retry` naks with the
// tier's delay instead of republishing, so the consumer never writes into the
// stream, and the count is JetStream's redelivery count.
shove::define_topic!(
    ExternalRetryTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-external-retry")
        .external()
        .dlq()
        .hold_queue(Duration::from_millis(200))
        .hold_queue(Duration::from_millis(400))
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

shove::define_sequenced_topic!(
    SeqFailAllTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-failall")
        .sequenced(SequenceFailure::FailAll)
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
    container: testcontainers::ContainerAsync<NatsContainer>,
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

        Self { container, client }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    fn client(&self) -> NatsClient {
        self.client.clone()
    }

    /// Freezes the server, keeping its connections and its host port.
    async fn pause(&self) {
        self.container
            .pause()
            .await
            .expect("the NATS container pauses");
    }

    async fn unpause(&self) {
        self.container
            .unpause()
            .await
            .expect("the NATS container resumes");
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

    let client_for_assert = client.clone();
    let publisher = NatsPublisher::new(client).await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("count-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    let result = publisher.publish_batch::<WorkTopic>(&messages).await;
    assert!(result.is_ok(), "expected success, got {result:?}");

    // "All 5 stored" is now asserted against the broker rather than against a
    // self-reported count: `Ok(())` means nothing is outstanding.
    let mut stream = client_for_assert
        .jetstream()
        .get_stream(<WorkTopic as shove::Topic>::topology().queue())
        .await
        .expect("work stream should exist");
    let info = stream.info().await.expect("should get stream info");
    assert_eq!(
        info.state.messages, 5,
        "all 5 messages should be stored on a fully successful batch"
    );

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

    let err = publisher
        .publish_batch::<SeqPartialTopic>(&messages)
        .await
        .expect_err("expected a partial failure from the shard-1 messages with no stream");

    let shove::ShoveError::PartialBatch(f) = err else {
        panic!("expected ShoveError::PartialBatch, got {err:?}");
    };

    assert!(f.succeeded() > 0, "shard-0 acks must not be abandoned");
    assert!(
        f.succeeded() < messages.len(),
        "shard-1 messages have no stream and must not be counted as stored"
    );

    // Every record was submitted — only the acks failed — so nothing is
    // unattempted and the failed set is exact.
    assert!(
        f.unattempted().is_empty(),
        "submission succeeded for the whole batch; only acks failed: {:?}",
        f.unattempted()
    );
    assert_eq!(f.failed(), f.to_republish());

    // The invariant, asserted for this backend.
    assert_eq!(f.succeeded() + f.to_republish().len(), messages.len());
    assert!(
        f.to_republish().windows(2).all(|w| w[0] < w[1]),
        "to_republish must be ascending with no duplicates: {:?}",
        f.to_republish()
    );

    // The point of exact indices: shard assignment interleaves, so records
    // *after* the first failure were stored fine. A prefix-shaped retry would
    // re-produce them; this set does not. (Deterministic — the same 20 keys
    // hash to the same shards on every run.)
    let first_failed = *f.failed().first().expect("at least one shard-1 message");
    let last_succeeded = (0..messages.len())
        .filter(|i| !f.to_republish().contains(i))
        .next_back()
        .expect("at least one shard-0 message");
    assert!(
        last_succeeded > first_failed,
        "expected sparse indices, got a prefix: failed={:?}",
        f.failed()
    );

    let mut stream = client
        .jetstream()
        .get_stream("nats-partial-fail")
        .await
        .expect("stream should exist");
    let info = stream.info().await.expect("should get stream info");
    assert_eq!(
        info.state.messages as usize,
        f.succeeded(),
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

/// CAF-84: `SequenceFailure::FailAll` must halt the failing sequence key on
/// NATS, not silently behave like `Skip`.
///
/// Also covers the wire change this needed: NATS previously encoded only the
/// shard (in the subject), so without `Shove-Sequence-Key` the consumer could
/// not tell key-A from key-B inside one shard.
#[tokio::test]
async fn sequenced_failall_poisons_same_key_after_reject() {
    #[derive(Clone)]
    struct PoisonHandler {
        seen: Arc<Mutex<Vec<(String, u64)>>>,
        key_b_handled: WaitableCounter,
    }

    impl MessageHandler<SeqFailAllTopic> for PoisonHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.seen
                .lock()
                .await
                .push((msg.order_id.clone(), msg.amount));
            if msg.order_id == "key-B" {
                self.key_b_handled.increment();
            }
            if msg.order_id == "key-A" && msg.amount == 2 {
                Outcome::Reject
            } else {
                Outcome::Ack
            }
        }
    }

    struct SeqDlqHandler(WaitableCounter);
    impl MessageHandler<SeqFailAllTopic> for SeqDlqHandler {
        type Context = ();
        async fn handle(&self, _: OrderMessage, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: OrderMessage, _: DeadMessageMetadata, _: &()) {
            self.0.increment();
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqFailAllTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for amount in 0..5u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount,
            })
            .await
            .unwrap();
    }
    for amount in 0..3u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-B".into(),
                amount,
            })
            .await
            .unwrap();
    }

    let seen: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let key_b_handled = WaitableCounter::new();
    let handler = PoisonHandler {
        seen: seen.clone(),
        key_b_handled: key_b_handled.clone(),
    };
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0),
            )
            .await
    });

    // key-A/2 is rejected, then key-A/3 and key-A/4 are dead-lettered without
    // ever reaching the handler → exactly 3 dead messages.
    let dlq_counter = WaitableCounter::new();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handler = SeqDlqHandler(dlq_counter.clone());
    let dlq_handle = tokio::spawn(async move {
        dlq_consumer
            .run_dlq::<SeqFailAllTopic, _>(dlq_handler, ())
            .await
    });

    assert!(
        dlq_counter.wait_for(3, Duration::from_secs(30)).await,
        "expected key-A/2 plus the two poisoned messages in the DLQ, got {}",
        dlq_counter.get()
    );

    // key-B shares the topic and, with 2 shards, lands on key-A's shard about
    // half the time. In that case it is only handled *after* the three key-A
    // dead-letters, so wait for it rather than reading `seen` off the back of
    // the DLQ count.
    assert!(
        key_b_handled.wait_for(3, Duration::from_secs(30)).await,
        "key-B must be unaffected by key-A's poisoning, but only {} of 3 were handled",
        key_b_handled.get()
    );

    let seen = seen.lock().await.clone();
    for amount in [3u64, 4] {
        assert!(
            !seen.contains(&("key-A".to_string(), amount)),
            "key-A/{amount} reached the handler after the key was poisoned: {seen:?}"
        );
    }
    for amount in 0..3u64 {
        assert!(
            seen.contains(&("key-B".to_string(), amount)),
            "key-B/{amount} should have been handled normally: {seen:?}"
        );
    }

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
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
        delivery_counts: Arc<Mutex<Vec<Option<u32>>>>,
    }

    impl MessageHandler<WorkTopic> for DeferCheckRetry {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            self.retry_counts.lock().await.push(meta.retry_count);
            self.delivery_counts.lock().await.push(meta.delivery_count);
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
    let delivery_counts = Arc::new(Mutex::new(Vec::new()));
    let handler = DeferCheckRetry {
        counter: counter.clone(),
        retry_counts: retry_counts.clone(),
        delivery_counts: delivery_counts.clone(),
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

    // `delivery_count` is JetStream's `num_delivered`, and it moves where
    // `retry_count` does not — and vice versa. Retry republishes a copy, so the
    // broker sees a message it has never delivered; Defer naks in place, so the
    // same message comes back with one more attempt on it. Relative assertions
    // rather than exact values: an ack_wait expiring mid-test could add a
    // redelivery, which must not turn a correct implementation red.
    let deliveries = delivery_counts.lock().await;
    assert_eq!(
        deliveries[0],
        Some(1),
        "first call: JetStream has delivered this message once"
    );
    assert_eq!(
        deliveries[1],
        Some(1),
        "second call (after Retry): a republished copy is a fresh message, \
         so the broker's delivery count restarts"
    );
    assert!(
        deliveries[2].is_some_and(|n| n > deliveries[1].unwrap_or(u32::MAX)),
        "third call (after Defer): Defer naks in place, so the same message's \
         delivery count must advance — got {:?} after {:?}",
        deliveries[2],
        deliveries[1]
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
    // Shard-level panic boundary: the NATS FIFO consume loop runs each handler
    // through `invoke_handler`, which spawns it and turns a panic into
    // `Outcome::Retry`. The panic never propagates out of the shard task — it
    // always returns `Ok(())`.
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
    // (Retry) and is held, then handed back at retry_count 1 where
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

    // The shard absorbs handler panics in `invoke_handler` (see above), so
    // neither panics nor errors are incremented. The harness returns cleanly.
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
    // The NATS FIFO consume loop processes messages sequentially: it awaits
    // each handler before moving to the next message.
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
    let dropped = WaitableCounter::new();

    /// Counts the drop of the handler call that holds it.
    struct CountOnDrop(WaitableCounter);
    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.increment();
        }
    }

    #[derive(Clone)]
    struct SlowHandler {
        started: WaitableCounter,
        dropped: WaitableCounter,
    }
    impl MessageHandler<SeqSkipTopic> for SlowHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.started.increment();
            let _dropped = CountOnDrop(self.dropped.clone());
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

    let handler = SlowHandler {
        started,
        dropped: dropped.clone(),
    };
    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(handler, (), opts, signal, drain)
        .await;

    // STRICT — handler ignores shutdown, drain budget runs out, shard aborted.
    assert!(outcome.timed_out, "expected timed_out, got {outcome:?}");
    assert_eq!(outcome.exit_code(), 3);
    // The shard takes the handler call it was awaiting down with it.
    assert!(
        dropped.wait_for(1, Duration::from_secs(5)).await,
        "the aborted shard left its handler running"
    );

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
/// `kafka_integration.rs` but drives the NATS backend's `spawn_autoscaler`
/// against a real broker: the backlog is 20 messages of 200 ms handler time
/// behind one starting consumer — sustained pressure the autoscaler reacts
/// to well before the drain can complete. Scale-up is exercised, not
/// asserted — the generic `ConsumerGroup<B>` wrapper does not surface the
/// backend registry's `active_consumers()`. Scale-down is not reached here
/// at all: it would need stats round-trips, cooldown and hysteresis to line
/// up inside a fixed post-drain window — the load-sensitive wait this test
/// exists to remove. The retiring bookkeeping it would drive is pinned by
/// this backend's `scale_down_*` unit tests; real-broker scale-down coverage
/// is tracked separately.
#[tokio::test]
async fn autoscaling_scales_up_and_drains_clean() {
    use shove::AutoscalerConfig;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicBool;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<AutoscalingTopic>()
        .await
        .unwrap();

    // JetStream is at-least-once: a consumer cancelled by scale-down naks or
    // abandons its un-acked in-flight delivery, which then redelivers
    // (`ack_wait` itself derives to 90 s here — longer than the whole test —
    // so it is not the live mechanism). Count distinct message ids rather
    // than deliveries: a raw delivery counter could both overshoot 20 and
    // trip the shutdown signal on 20 - K distinct + K duplicate deliveries.
    let seen = Arc::new(StdMutex::new(HashSet::new()));
    let distinct = WaitableCounter::new();
    // Raw delivery count, duplicates included — bounds redelivery below.
    let deliveries = Arc::new(AtomicU32::new(0));

    let mut group = broker.consumer_group();
    {
        let seen = seen.clone();
        let distinct = distinct.clone();
        let deliveries = deliveries.clone();
        group
            .register::<AutoscalingTopic, _>(
                ConsumerGroupConfig::new(
                    NatsConsumerGroupConfig::new(1..=4).with_prefetch_count(1),
                ),
                move || {
                    #[derive(Clone)]
                    struct SlowHandler {
                        seen: Arc<StdMutex<HashSet<String>>>,
                        distinct: WaitableCounter,
                        deliveries: Arc<AtomicU32>,
                    }
                    impl MessageHandler<AutoscalingTopic> for SlowHandler {
                        type Context = ();
                        async fn handle(
                            &self,
                            msg: SimpleMessage,
                            _: MessageMetadata,
                            _: &(),
                        ) -> Outcome {
                            // Count the delivery before the slow part, so a
                            // redelivery cancelled mid-handle still counts.
                            self.deliveries.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            if self.seen.lock().unwrap().insert(msg.id) {
                                self.distinct.increment();
                            }
                            Outcome::Ack
                        }
                    }
                    SlowHandler {
                        seen: seen.clone(),
                        distinct: distinct.clone(),
                        deliveries: deliveries.clone(),
                    }
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
    // within the first second after the group starts consuming.
    let cfg = AutoscalerConfig {
        poll_interval: Duration::from_millis(200),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_millis(200),
        cooldown_duration: Duration::from_millis(400),
    };

    // Shut down on the observable — all 20 distinct messages handled — not on
    // a fixed wall-clock window: on a loaded runner, stream/consumer setup and
    // first delivery can outlast any "reasonable" fixed budget (the Kafka twin
    // flaked in CI exactly this way). The 60 s ceiling is a failure bound, not
    // the expected duration; the drain itself takes ~1-2 s.
    let reached_in_time = Arc::new(AtomicBool::new(false));
    let signal = {
        let distinct = distinct.clone();
        let reached_in_time = reached_in_time.clone();
        async move {
            let ok = distinct.wait_for(20, Duration::from_secs(60)).await;
            reached_in_time.store(ok, Ordering::SeqCst);
        }
    };
    // The 30 s drain budget is also a failure bound: shutdown now fires at
    // peak activity (4 consumers mid-handle) rather than after an idle tail.
    let outcome = group
        .enable_autoscaling(cfg)
        .run_until_timeout(signal, Duration::from_secs(30))
        .await;

    // Count first, cleanliness second: on the no-delivery failure path the
    // handled count is the diagnostic and an unclean shutdown in that same
    // state must not mask it (the outcome rides along in the message).
    let handled = distinct.get();
    assert!(
        reached_in_time.load(Ordering::SeqCst),
        "all 20 published messages must be handled within the 60 s ceiling \
         (distinct handled after the shutdown drain: {handled}/20; \
         0 = the group never started consuming, 1-19 = the drain stalled, \
         20 = handled only after the ceiling elapsed or the shutdown signal \
         was cancelled early; outcome: {outcome:?})"
    );
    assert!(
        outcome.is_clean(),
        "autoscaling group must drain cleanly (handled {handled}/20); outcome: {outcome:?}"
    );
    // Guard against unbounded systematic redelivery (e.g. acks silently
    // dropped so everything redelivers), which the old exact-count assert
    // caught. Legitimate scale-down replay — the un-acked in-flight delivery
    // per cancelled consumer — stays far below 3x.
    let total = deliveries.load(Ordering::Relaxed);
    assert!(
        total <= 60,
        "{total} deliveries for 20 published messages — at-least-once \
         tolerates bounded replay, not systematic redelivery"
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

/// The stream sequence and the receive time JetStream keeps on every message
/// reach the handler as `offset` and `timestamp_ms`: a log position with no
/// partition, and the server's receive time in Unix milliseconds. A fresh
/// stream starts its sequence at 1.
#[tokio::test]
async fn nats_delivery_fills_offset_and_timestamp_from_stream_info() {
    use std::time::{SystemTime, UNIX_EPOCH};

    type Coordinates = (Option<i32>, Option<i64>, Option<i64>);

    #[derive(Clone)]
    struct CoordinateCapture(Arc<Mutex<Option<Coordinates>>>);

    impl MessageHandler<WorkTopic> for CoordinateCapture {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            *self.0.lock().await = Some((meta.partition, meta.offset, meta.timestamp_ms));
            Outcome::Ack
        }
    }

    let millis_now = || {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after the epoch")
                .as_millis(),
        )
        .expect("fits an i64")
    };

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let before_ms = millis_now();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<WorkTopic>(&SimpleMessage {
            id: "coord-1".into(),
            content: "coordinates".into(),
        })
        .await
        .expect("publish should succeed");
    let after_ms = millis_now();

    let captured = Arc::new(Mutex::new(None));
    let handler = CoordinateCapture(captured.clone());
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

    let coordinates = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(coordinates) = *captured.lock().await {
                return coordinates;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("should receive the delivery within timeout");

    shutdown.cancel();
    handle.await.unwrap().ok();

    let (partition, offset, timestamp_ms) = coordinates;
    assert_eq!(partition, None, "JetStream has no partitions");
    assert_eq!(
        offset,
        Some(1),
        "the first message on a fresh stream has stream sequence 1"
    );
    let ts = timestamp_ms.expect("a stream message always carries its publish time");
    // The container's clock and the host's may differ a little; a tolerance
    // of five minutes still rules out a unit mix-up (seconds or nanoseconds).
    let tolerance = 5 * 60 * 1000;
    assert!(
        ts >= before_ms - tolerance && ts <= after_ms + tolerance,
        "timestamp_ms {ts} is not within the publish window {before_ms}..{after_ms}"
    );
    broker.close().await;
}

/// The direct and FIFO entry points refuse a set broadcast start, as the
/// supervisor and group paths do: `with_broadcast_start` reaches
/// `NatsConsumer::run` and `run_fifo` as a `Topology` error before any
/// consumer is created, synchronously, which the timeout pins.
#[tokio::test]
async fn direct_and_fifo_entry_points_refuse_a_broadcast_start() {
    let tb = TestBroker::start().await;
    let client = tb.client();

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        NatsConsumer::new(client.clone()).run::<WorkTopic, _>(
            CountingHandler::new(),
            (),
            ConsumerOptions::<Nats>::new().with_broadcast_start(shove::BroadcastStart::Head),
        ),
    )
    .await
    .expect("the refusal is synchronous")
    .expect_err("a set broadcast start must be refused on the direct path");
    let shove::ShoveError::Topology(msg) = err else {
        panic!("expected ShoveError::Topology, got {err:?}");
    };
    assert!(
        msg.contains("nats-work")
            && msg.contains("with_broadcast_start(Head)")
            && msg.contains("NatsConsumer::run"),
        "the error names the topic, the start and the entry point: {msg}"
    );

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        NatsConsumer::new(client).run_fifo::<SeqSkipTopic, _>(
            CountingHandler::new(),
            (),
            ConsumerOptions::<Nats>::new().with_broadcast_start(shove::BroadcastStart::Tail),
        ),
    )
    .await
    .expect("the refusal is synchronous")
    .expect_err("a set broadcast start must be refused on the FIFO path");
    let shove::ShoveError::Topology(msg) = err else {
        panic!("expected ShoveError::Topology, got {err:?}");
    };
    assert!(
        msg.contains("nats-seq-skip")
            && msg.contains("with_broadcast_start(Tail)")
            && msg.contains("NatsConsumer::run_fifo"),
        "the error names the topic, the start and the entry point: {msg}"
    );
}

// ---------------------------------------------------------------------------
// External streams: a Retry never writes into the stream
// ---------------------------------------------------------------------------

/// Stands in for the infrastructure that owns `name`: a Limits stream over the
/// one subject the topic publishes to. Limits retention keeps every record
/// after its ack, so `state.messages` counts everything ever written.
async fn provision_limits_stream(client: &NatsClient, name: &str) {
    client
        .jetstream()
        .create_stream(JsStreamConfig {
            name: name.to_string(),
            subjects: vec![name.to_string()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
        .expect("infra pre-provisions the stream");
}

async fn stream_messages(client: &NatsClient, name: &str) -> u64 {
    let mut stream = client
        .jetstream()
        .get_stream(name)
        .await
        .expect("the stream exists");
    stream.info().await.expect("stream info").state.messages
}

/// Answers from a script of outcomes, the last one repeating, and keeps every
/// `MessageMetadata` it is handed.
#[derive(Clone)]
struct ScriptedHandler {
    script: Arc<Vec<Outcome>>,
    seen: Arc<Mutex<Vec<MessageMetadata>>>,
    counter: WaitableCounter,
}

impl ScriptedHandler {
    fn new(script: impl Into<Vec<Outcome>>) -> Self {
        Self {
            script: Arc::new(script.into()),
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<ExternalRetryTopic> for ScriptedHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        let mut seen = self.seen.lock().await;
        let outcome = self.script[seen.len().min(self.script.len() - 1)].clone();
        seen.push(meta);
        drop(seen);
        self.counter.increment();
        outcome
    }
}

/// Keeps every dead letter it is handed.
#[derive(Clone)]
struct DeadLetterRecorder {
    seen: Arc<Mutex<Vec<DeadMessageMetadata>>>,
    counter: WaitableCounter,
}

impl DeadLetterRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<ExternalRetryTopic> for DeadLetterRecorder {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: SimpleMessage, meta: DeadMessageMetadata, _: &()) {
        self.seen.lock().await.push(meta);
        self.counter.increment();
    }
}

/// On an external stream a `Retry` is a delayed nak of the same record: the
/// redelivered record keeps its stream sequence, the retry count climbs with
/// JetStream's delivery count instead of a header, and the stream holds
/// exactly the one record the publisher wrote.
#[tokio::test]
async fn external_stream_retry_naks_in_place_and_writes_nothing() {
    const STREAM: &str = "nats-external-retry";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    provision_limits_stream(&client, STREAM).await;
    broker
        .topology()
        .declare::<ExternalRetryTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExternalRetryTopic>(&SimpleMessage {
            id: "in-place".into(),
            content: "retry twice, then ack".into(),
        })
        .await
        .unwrap();

    let handler = ScriptedHandler::new([Outcome::Retry, Outcome::Retry, Outcome::Ack]);
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExternalRetryTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "two retries and an ack are three deliveries"
    );
    // Let a stray fourth delivery surface before the assertions.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let seen = handler.seen.lock().await.clone();
    assert_eq!(seen.len(), 3, "exactly three deliveries");
    for (i, meta) in seen.iter().enumerate() {
        let attempt = i as u32;
        assert_eq!(
            meta.retry_count, attempt,
            "delivery {i}: the retry count is the redelivery count"
        );
        assert_eq!(
            meta.delivery_count,
            Some(attempt + 1),
            "delivery {i}: JetStream counts the same record again"
        );
        assert_eq!(meta.redelivered, i > 0, "delivery {i}");
        assert_eq!(
            meta.offset,
            Some(1),
            "delivery {i}: the same record at the same stream sequence"
        );
        assert_eq!(
            meta.headers.get("Shove-Retry-Count").map(String::as_str),
            Some("0"),
            "delivery {i}: the publisher's count header never climbs, so no copy was republished"
        );
    }
    assert_eq!(
        stream_messages(&client, STREAM).await,
        1,
        "the consumer wrote nothing into the infra-owned stream"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
}

/// An exhausted in-place retry dead-letters as before: the record reaches a
/// DLQ handler after one delivery plus `max_retries` redeliveries, the dead
/// letter carries that redelivery count as its `retry_count`, and the
/// external stream still holds only the record the publisher wrote. The DLQ
/// stream itself is shove's work queue, so its count is not asserted.
#[tokio::test]
async fn external_stream_retry_exhaustion_dead_letters_with_the_redelivery_count() {
    const STREAM: &str = "nats-external-retry";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    provision_limits_stream(&client, STREAM).await;
    broker
        .topology()
        .declare::<ExternalRetryTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExternalRetryTopic>(&SimpleMessage {
            id: "exhaust".into(),
            content: "always retry".into(),
        })
        .await
        .unwrap();

    let handler = ScriptedHandler::new([Outcome::Retry]);
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExternalRetryTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead_letters = DeadLetterRecorder::new();
    let dlc = dead_letters.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<ExternalRetryTopic, _>(dlc, ()).await });

    assert!(
        dead_letters.counter.wait_for(1, TIMEOUT).await,
        "the exhausted record lands in the DLQ"
    );
    // Let a stray fourth delivery surface before the assertions.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        handler.counter.get(),
        3,
        "max_retries=2 allows one delivery plus two redeliveries"
    );
    let dead = dead_letters.seen.lock().await.clone();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason.as_deref(), Some("max_retries_exceeded"));
    assert_eq!(
        dead[0].message.retry_count, 2,
        "the dead letter carries the redelivery count"
    );
    assert_eq!(
        stream_messages(&client, STREAM).await,
        1,
        "the consumer wrote nothing into the infra-owned stream"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

// ---------------------------------------------------------------------------
// Header data: the copies a consumer publishes, and publish validation
// ---------------------------------------------------------------------------

/// A message that is only an enum. A payload naming a variant it does not have
/// fails to decode, and serde quotes that variant in its error verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Shape {
    Circle,
    Square,
}

/// The JSON string `"a\nb"`: an unknown [`Shape`] variant with a line break in
/// it, so the decode error text carries a raw newline.
const UNKNOWN_VARIANT_WITH_NEWLINE: &[u8] = br#""a\nb""#;

shove::define_topic!(
    ShapeTopic,
    Shape,
    TopologyBuilder::new("nats-shape").dlq().build()
);

shove::define_sequenced_topic!(
    SeqShapeTopic,
    Shape,
    |shape: &Shape| format!("{shape:?}"),
    TopologyBuilder::new("nats-seq-shape")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

impl MessageHandler<ShapeTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: Shape, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<SeqShapeTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: Shape, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// Publishes `payload` to `subject` straight through JetStream with exactly
/// `headers`, so the record carries no `Nats-Msg-Id` unless `headers` does.
async fn raw_publish(
    client: &NatsClient,
    subject: &str,
    headers: async_nats::HeaderMap,
    payload: &[u8],
) {
    client
        .jetstream()
        .publish_with_headers(
            subject.to_string(),
            headers,
            bytes::Bytes::copy_from_slice(payload),
        )
        .await
        .expect("the raw publish is submitted")
        .await
        .expect("the raw publish is acked");
}

/// A `SimpleMessage` as the JSON shove's codec would have written.
fn simple_payload(id: &str) -> Vec<u8> {
    serde_json::to_vec(&SimpleMessage {
        id: id.into(),
        content: "published without shove".into(),
    })
    .expect("a SimpleMessage encodes")
}

/// Pulls `count` dead letters off the DLQ stream `dlq` and acks them, failing
/// the test when they have not all arrived within `TIMEOUT`.
async fn take_dead_letters(client: &NatsClient, dlq: &str, count: usize) -> Vec<JsMessage> {
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
    use futures_util::StreamExt;

    let stream = client
        .jetstream()
        .get_stream(dlq)
        .await
        .expect("the DLQ stream exists");
    let reader = stream
        .get_or_create_consumer(
            "test-dead-letter-reader",
            PullConsumerConfig {
                durable_name: Some("test-dead-letter-reader".into()),
                ..Default::default()
            },
        )
        .await
        .expect("the DLQ reader is created");
    let mut messages = reader.messages().await.expect("the DLQ reader pulls");
    let deadline = Instant::now() + TIMEOUT;
    let mut dead = Vec::with_capacity(count);
    while dead.len() < count {
        let msg = tokio::time::timeout_at(deadline, messages.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "only {} of {count} dead letters reached {dlq} within {TIMEOUT:?}",
                    dead.len()
                )
            })
            .expect("the DLQ reader stream stays open")
            .expect("a dead letter is readable");
        msg.ack().await.expect("the dead letter is acked");
        dead.push(msg);
    }
    dead
}

/// The first value of header `name` on `msg`.
fn header<'a>(msg: &'a JsMessage, name: &str) -> Option<&'a str> {
    msg.headers.as_ref()?.get(name).map(|v| v.as_str())
}

/// serde quotes an unknown variant verbatim, so `"a\nb"` fails to decode with
/// a raw newline in the error text. The dead letter carries the fixed reason
/// instead of that text, and the consumer is still there for the next message.
#[tokio::test]
async fn decode_error_with_newline_dead_letters_without_panic() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ShapeTopic>().await.unwrap();
    raw_publish(
        &client,
        "nats-shape",
        async_nats::HeaderMap::new(),
        UNKNOWN_VARIANT_WITH_NEWLINE,
    )
    .await;

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ShapeTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-shape-dlq", 1).await;
    assert_eq!(
        header(&dead[0], "Shove-Death-Reason"),
        Some("deserialization_error")
    );
    assert_eq!(dead[0].payload.as_ref(), UNKNOWN_VARIANT_WITH_NEWLINE);

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<ShapeTopic>(&Shape::Circle)
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the consumer outlives the poison message and handles the next one"
    );

    shutdown.cancel();
    handle
        .await
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

/// The FIFO shard loop decodes on its own path: the same poison payload on a
/// shard subject dead-letters under the fixed reason, and the shard goes on
/// to handle the next message.
#[tokio::test]
async fn fifo_decode_error_with_newline_dead_letters_without_panic() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqShapeTopic>().await.unwrap();
    raw_publish(
        &client,
        "nats-seq-shape.shard.0",
        async_nats::HeaderMap::new(),
        UNKNOWN_VARIANT_WITH_NEWLINE,
    )
    .await;

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqShapeTopic, _>(hc, (), ConsumerOptions::<Nats>::new().with_shutdown(sc))
            .await
    });

    let dead = take_dead_letters(&client, "nats-seq-shape-dlq", 1).await;
    assert_eq!(
        header(&dead[0], "Shove-Death-Reason"),
        Some("deserialization_error")
    );

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SeqShapeTopic>(&Shape::Square)
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the shard outlives the poison message and handles the next one"
    );
    assert!(!handle.is_finished(), "no shard task ended");

    shutdown.cancel();
    handle
        .await
        .expect("the FIFO consumer task does not panic")
        .expect("the FIFO consumer stops cleanly");
    broker.close().await;
}

/// A record without a `Nats-Msg-Id` of its own gets a dead-letter id from its
/// delivery: stream, consumer and stream sequence. Five such records rejected
/// inside one duplicate window are five dead letters.
#[tokio::test]
async fn messages_without_msg_id_all_reach_the_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    for i in 1..=5 {
        raw_publish(
            &client,
            "nats-work",
            async_nats::HeaderMap::new(),
            &simple_payload(&format!("no-id-{i}")),
        )
        .await;
    }

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
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-work-dlq", 5).await;
    let mut ids: Vec<&str> = dead
        .iter()
        .map(|msg| header(msg, "Nats-Msg-Id").expect("every dead letter has an id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [
            "nats-work.nats-work-consumer.1-dlq",
            "nats-work.nats-work-consumer.2-dlq",
            "nats-work.nats-work-consumer.3-dlq",
            "nats-work.nats-work-consumer.4-dlq",
            "nats-work.nats-work-consumer.5-dlq",
        ],
        "each dead letter is named after the delivery it was made from"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A retry copy's id comes from the delivery too: five records without a
/// `Nats-Msg-Id`, each retried once inside one duplicate window, come back as
/// five copies, each named after the delivery it copies.
#[tokio::test]
async fn retry_copies_without_msg_id_are_distinct() {
    #[derive(Clone)]
    struct RetryOnceThenRecord {
        copies: Arc<Mutex<Vec<(String, String)>>>,
        counter: WaitableCounter,
    }

    impl MessageHandler<WorkTopic> for RetryOnceThenRecord {
        type Context = ();
        async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            if meta.retry_count == 0 {
                return Outcome::Retry;
            }
            self.copies.lock().await.push((msg.id, meta.delivery_id));
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    for i in 1..=5 {
        raw_publish(
            &client,
            "nats-work",
            async_nats::HeaderMap::new(),
            &simple_payload(&format!("no-id-{i}")),
        )
        .await;
    }

    let handler = RetryOnceThenRecord {
        copies: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
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
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });

    let all_arrived = handler.counter.wait_for(5, TIMEOUT).await;
    let mut copies = handler.copies.lock().await.clone();
    copies.sort_unstable();
    assert!(
        all_arrived,
        "only {} of 5 retry copies arrived: {copies:?}",
        copies.len()
    );
    let expected: Vec<(String, String)> = (1..=5)
        .map(|i| {
            (
                format!("no-id-{i}"),
                format!("nats-work.nats-work-consumer.{i}-r1"),
            )
        })
        .collect();
    assert_eq!(
        copies, expected,
        "each copy is named after the delivery it copies"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A header NATS cannot carry is refused as a `Validation` error before
/// anything is sent: a name with a colon or a space, a value with a line
/// break, and a sequence key with one on every publish call.
#[tokio::test]
async fn invalid_user_headers_are_validation_errors() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();

    let msg = SimpleMessage {
        id: "bad-header".into(),
        content: "refused".into(),
    };
    for (name, value) in [("a:b", "fine"), ("bad name", "fine"), ("x-fine", "x\ny")] {
        let headers = HashMap::from([(name.to_string(), value.to_string())]);
        let err = publisher
            .publish_with_headers::<WorkTopic>(&msg, headers)
            .await
            .expect_err("a header NATS cannot carry is refused");
        assert!(
            matches!(err, shove::ShoveError::Validation(_)),
            "{name:?}: {value:?} should be a validation error, got {err:?}"
        );
    }

    let order = OrderMessage {
        order_id: "line\nbreak".into(),
        amount: 1,
    };
    let refusals = [
        publisher
            .publish_with_headers::<SeqSkipTopic>(&order, HashMap::new())
            .await,
        publisher.publish::<SeqSkipTopic>(&order).await,
        publisher
            .publish_batch::<SeqSkipTopic>(std::slice::from_ref(&order))
            .await,
    ];
    for result in refusals {
        assert!(
            matches!(result, Err(shove::ShoveError::Validation(_))),
            "a sequence key with a line break should be a validation error, got {result:?}"
        );
    }

    assert_eq!(stream_messages(&client, "nats-work").await, 0);
    assert_eq!(stream_messages(&client, "nats-seq-skip").await, 0);
    broker.close().await;
}

/// `Nats-Expected-Stream` checks the original's own publish. A dead letter
/// goes to another stream, so a copy that kept the header would be refused by
/// the server on every attempt and the record would cycle forever; the copy
/// drops it and lands.
#[tokio::test]
async fn expected_stream_header_does_not_block_dead_lettering() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_EXPECTED_STREAM, "nats-work");
    headers.insert(NATS_MESSAGE_ID, "expected-stream-1");
    raw_publish(
        &client,
        "nats-work",
        headers,
        &simple_payload("expected-stream-1"),
    )
    .await;

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
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-work-dlq", 1).await;
    assert_eq!(header(&dead[0], "Shove-Death-Reason"), Some("rejected"));
    assert_eq!(
        header(&dead[0], "Nats-Expected-Stream"),
        None,
        "the copy does not replay the original's publish check"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// `Shove-Death-Count` is text any producer can write. A dead letter whose
/// count is already `u32::MAX` stays there instead of overflowing.
#[tokio::test]
async fn death_count_saturates() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    let max = u32::MAX.to_string();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Shove-Death-Count", max.as_str());
    headers.insert(NATS_MESSAGE_ID, "death-count-max");
    raw_publish(
        &client,
        "nats-work",
        headers,
        &simple_payload("death-count-max"),
    )
    .await;

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
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-work-dlq", 1).await;
    assert_eq!(header(&dead[0], "Shove-Death-Count"), Some(max.as_str()));

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A copy of a message that carries a `Nats-Msg-Id` builds its own id on that
/// one, so `delivery_id` keeps the lineage a handler may deduplicate on: the
/// retry copy of `lineage-1` is `lineage-1-r1`, and its dead letter
/// `lineage-1-r1-dlq`. Every copy names the delivery it was made from in
/// `Shove-Copy-Of`.
#[tokio::test]
async fn copies_of_a_message_with_an_id_keep_its_lineage() {
    #[derive(Clone)]
    struct RetryThenReject {
        seen: Arc<Mutex<Vec<MessageMetadata>>>,
    }

    impl MessageHandler<WorkTopic> for RetryThenReject {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            let first = meta.retry_count == 0;
            self.seen.lock().await.push(meta);
            if first {
                Outcome::Retry
            } else {
                Outcome::Reject
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, "lineage-1");
    raw_publish(&client, "nats-work", headers, &simple_payload("lineage-1")).await;

    let handler = RetryThenReject {
        seen: Arc::new(Mutex::new(Vec::new())),
    };
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
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-work-dlq", 1).await;
    let seen = handler.seen.lock().await.clone();
    assert_eq!(seen.len(), 2, "the original and its retry copy");
    assert_eq!(seen[0].delivery_id, "lineage-1");
    assert_eq!(seen[1].delivery_id, "lineage-1-r1");
    assert_eq!(
        seen[1].headers.get("Shove-Copy-Of").map(String::as_str),
        Some("nats-work.nats-work-consumer.1"),
        "the retry copy was made from stream sequence 1"
    );
    assert_eq!(header(&dead[0], "Nats-Msg-Id"), Some("lineage-1-r1-dlq"));
    assert_eq!(
        header(&dead[0], "Shove-Copy-Of"),
        Some("nats-work.nats-work-consumer.2"),
        "the dead letter was made from the retry copy, stream sequence 2"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// Shortens `stream`'s duplicate window, so a test can wait out an id that
/// another message holds.
async fn shrink_duplicate_window(client: &NatsClient, stream: &str, window: Duration) {
    let mut config = client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists")
        .cached_info()
        .config
        .clone();
    config.duplicate_window = window;
    client
        .jetstream()
        .update_stream(config)
        .await
        .expect("the duplicate window is mutable");
}

/// Waits until the durable `consumer` exists on `stream`, so a test can start
/// a clock only once the consumer is ready to take deliveries.
async fn wait_for_durable(client: &NatsClient, stream: &str, consumer: &str) {
    let stream = client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists");
    let deadline = Instant::now() + TIMEOUT;
    while stream.consumer_info(consumer).await.is_err() {
        assert!(
            Instant::now() < deadline,
            "the durable {consumer} was never created"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Polls `stream`'s message count until it is `expected` or `TIMEOUT` passes,
/// and returns the last count seen.
async fn settle_stream_messages(client: &NatsClient, stream: &str, expected: u64) -> u64 {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let messages = stream_messages(client, stream).await;
        if messages == expected || Instant::now() >= deadline {
            return messages;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The sequence of the last message `stream` ever stored.
async fn stream_last_sequence(client: &NatsClient, stream: &str) -> u64 {
    let mut stream = client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists");
    stream
        .info()
        .await
        .expect("stream info")
        .state
        .last_sequence
}

/// Rejects every message and counts how often it was asked.
struct CountingReject(WaitableCounter);

impl MessageHandler<WorkTopic> for CountingReject {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.increment();
        Outcome::Reject
    }
}

/// A dead letter's id can be predicted, so anyone able to publish to the DLQ
/// subject can store a message under it first. JetStream then answers the
/// real dead letter with a duplicate PubAck, which counts only once the stored
/// message proves to be the same copy. The impostor is not, so the original
/// stays un-acked while it holds the id, is redelivered a second apart rather
/// than at once, and dead-letters once the duplicate window lets the real copy
/// through.
#[tokio::test]
async fn impostor_under_a_dead_letter_id_does_not_retire_the_original() {
    const WINDOW: Duration = Duration::from_secs(6);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    shrink_duplicate_window(&client, "nats-work-dlq", WINDOW).await;

    let rejects = WaitableCounter::new();
    let handler = CountingReject(rejects.clone());
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
    // The window starts with the impostor's publish, so the consumer is up first.
    wait_for_durable(&client, "nats-work", "nats-work-consumer").await;

    let mut impostor = async_nats::HeaderMap::new();
    impostor.insert(NATS_MESSAGE_ID, "nats-work.nats-work-consumer.1-dlq");
    raw_publish(&client, "nats-work-dlq", impostor, b"impostor").await;
    let payload = simple_payload("held-off");
    raw_publish(&client, "nats-work", async_nats::HeaderMap::new(), &payload).await;

    assert!(
        rejects.wait_for(2, TIMEOUT).await,
        "the original is redelivered after its dead letter was refused"
    );
    assert_eq!(
        stream_messages(&client, "nats-work").await,
        1,
        "the original is not acked while the impostor holds its dead letter's id"
    );
    assert_eq!(
        stream_messages(&client, "nats-work-dlq").await,
        1,
        "only the impostor is stored so far"
    );

    let dead = take_dead_letters(&client, "nats-work-dlq", 2).await;
    let real = dead
        .iter()
        .find(|msg| msg.payload.as_ref() != b"impostor")
        .expect("the real dead letter lands once the window has passed");
    assert_eq!(real.payload.as_ref(), payload.as_slice());
    assert_eq!(
        header(real, "Nats-Msg-Id"),
        Some("nats-work.nats-work-consumer.1-dlq")
    );
    assert_eq!(
        header(real, "Shove-Copy-Of"),
        Some("nats-work.nats-work-consumer.1")
    );
    // Each refused attempt waits a second before its redelivery, so the
    // window holds at most one delivery per second plus the final one.
    let runs = rejects.get();
    assert!(
        runs <= 8,
        "the held original ran {runs} times in a {WINDOW:?} window: it is redelivered at once"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The other side of the read-back check: a crash between a dead letter's
/// publish and its original's ack leaves the dead letter stored, and the
/// redelivered original's publish comes back as a duplicate of it. That
/// stored copy is the one this delivery would publish, so it is confirmed:
/// the original is acked after one delivery and the DLQ keeps a single copy.
#[tokio::test]
async fn a_stored_dead_letter_of_the_same_delivery_retires_the_original() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let payload = simple_payload("already-dead");
    let mut copy = async_nats::HeaderMap::new();
    copy.insert("Shove-Death-Reason", "rejected");
    copy.insert("Shove-Original-Queue", "nats-work");
    copy.insert("Shove-Death-Count", "1");
    copy.insert("Shove-Copy-Of", "nats-work.nats-work-consumer.1");
    copy.insert(NATS_MESSAGE_ID, "nats-work.nats-work-consumer.1-dlq");
    raw_publish(&client, "nats-work-dlq", copy, &payload).await;
    raw_publish(&client, "nats-work", async_nats::HeaderMap::new(), &payload).await;

    let rejects = WaitableCounter::new();
    let handler = CountingReject(rejects.clone());
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

    assert_eq!(
        settle_stream_messages(&client, "nats-work", 0).await,
        0,
        "the original is acked once its stored dead letter is confirmed"
    );
    assert_eq!(rejects.get(), 1, "the original is handled once");
    assert_eq!(
        stream_messages(&client, "nats-work-dlq").await,
        1,
        "the DLQ keeps a single copy"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_topic!(
    HoldTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-hold")
        .hold_queue(Duration::from_secs(3))
        .dlq()
        .build()
);

/// The hold path's side of the read-back check: the retry copy this delivery
/// would publish is already stored, as after a crash between the copy's
/// publish and the original's ack. The copy's publish comes back as a
/// duplicate of it, the read-back confirms it, and the original is acked
/// without a second copy.
///
/// The hold frees the worker's slot, so the worker takes the stored copy
/// during the hold. Its handler acks only after the hold is over, so the copy
/// is still stored when the read-back looks for it.
#[tokio::test]
async fn a_stored_retry_copy_of_the_same_delivery_retires_the_original() {
    /// Longer than the rest of the original's three-second hold.
    const COPY_HANDLING: Duration = Duration::from_secs(5);

    #[derive(Clone)]
    struct RetryOnceThenAck {
        seen: Arc<Mutex<Vec<(u32, String)>>>,
        counter: WaitableCounter,
    }

    impl MessageHandler<HoldTopic> for RetryOnceThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            let first = meta.retry_count == 0;
            self.seen
                .lock()
                .await
                .push((meta.retry_count, meta.delivery_id));
            self.counter.increment();
            if first {
                return Outcome::Retry;
            }
            tokio::time::sleep(COPY_HANDLING).await;
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<HoldTopic>().await.unwrap();
    let payload = simple_payload("already-copied");
    raw_publish(&client, "nats-hold", async_nats::HeaderMap::new(), &payload).await;

    let handler = RetryOnceThenAck {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<HoldTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the original is retried"
    );
    // The original is now held for 3 s. Store its copy first, as a publish
    // whose ack was lost would have.
    let mut copy = async_nats::HeaderMap::new();
    copy.insert("Shove-Retry-Count", "1");
    copy.insert("Shove-Copy-Of", "nats-hold.nats-hold-consumer.1");
    copy.insert(NATS_MESSAGE_ID, "nats-hold.nats-hold-consumer.1-r1");
    raw_publish(&client, "nats-hold", copy, &payload).await;

    assert_eq!(
        settle_stream_messages(&client, "nats-hold", 0).await,
        0,
        "the original and its copy are both acked"
    );
    let seen = handler.seen.lock().await.clone();
    assert_eq!(
        seen,
        [
            (0, String::new()),
            (1, "nats-hold.nats-hold-consumer.1-r1".to_string()),
        ],
        "the original once, then its single copy"
    );
    assert_eq!(
        stream_last_sequence(&client, "nats-hold").await,
        2,
        "the copy's own publish stored nothing new"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_topic!(
    FanTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-fan")
        .external()
        .dlq_named("nats-fan-dlq")
        .build()
);

// The same records through a mirror of `nats-fan`, which keeps their stream
// sequences and headers: a second reader of each message, sharing the first
// reader's DLQ.
shove::define_topic!(
    FanMirrorTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-fan-mirror")
        .external()
        .dlq_named("nats-fan-dlq")
        .build()
);

struct RejectAll;

impl MessageHandler<FanTopic> for RejectAll {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Reject
    }
}

impl MessageHandler<FanMirrorTopic> for RejectAll {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Reject
    }
}

/// Two readers of one record, a stream and its mirror, reject it into a shared
/// DLQ. The record carries a `Nats-Msg-Id`, so both dead letters get the same
/// id, and the second publish comes back as a duplicate of the first. Its
/// different `Shove-Copy-Of` keeps it from being taken for the first: its
/// original is redelivered until the duplicate window has passed, and then
/// both dead letters are in the DLQ.
#[tokio::test]
async fn two_readers_sharing_a_dlq_both_dead_letter_one_record() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    provision_limits_stream(&client, "nats-fan").await;
    client
        .jetstream()
        .create_stream(JsStreamConfig {
            name: "nats-fan-mirror".to_string(),
            mirror: Some(Source {
                name: "nats-fan".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("infra provisions the mirror");
    broker.topology().declare::<FanTopic>().await.unwrap();
    broker.topology().declare::<FanMirrorTopic>().await.unwrap();
    shrink_duplicate_window(&client, "nats-fan-dlq", Duration::from_secs(3)).await;

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, "shared-1");
    raw_publish(&client, "nats-fan", headers, &simple_payload("shared-1")).await;

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let first = tokio::spawn(async move {
        consumer
            .run::<FanTopic, _>(
                RejectAll,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let second = tokio::spawn(async move {
        consumer
            .run::<FanMirrorTopic, _>(
                RejectAll,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dead = take_dead_letters(&client, "nats-fan-dlq", 2).await;
    for msg in &dead {
        assert_eq!(header(msg, "Nats-Msg-Id"), Some("shared-1-dlq"));
    }
    let mut origins: Vec<&str> = dead
        .iter()
        .map(|msg| header(msg, "Shove-Copy-Of").expect("every dead letter names its origin"))
        .collect();
    origins.sort_unstable();
    assert_eq!(
        origins,
        [
            "nats-fan-mirror.nats-fan-mirror-consumer.1",
            "nats-fan.nats-fan-consumer.1",
        ],
        "one dead letter per reader"
    );

    shutdown.cancel();
    first.await.unwrap().ok();
    second.await.unwrap().ok();
    broker.close().await;
}

// ---------------------------------------------------------------------------
// Sequenced retries: the record waits at its position
// ---------------------------------------------------------------------------

/// Retries `amount` 0 until its retry count reaches 2 and acks everything
/// else, keeping every call's amount and retry count in order.
#[derive(Clone)]
struct RetryFirstMessageTwice {
    calls: Arc<Mutex<Vec<(u64, u32)>>>,
    counter: WaitableCounter,
}

impl MessageHandler<SeqSkipTopic> for RetryFirstMessageTwice {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.lock().await.push((msg.amount, meta.retry_count));
        self.counter.increment();
        if msg.amount == 0 && meta.retry_count < 2 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

/// Two messages for one key, A then B. A is retried twice before it acks,
/// and B is not handled until A has been: the calls are A, A, A, B.
#[tokio::test]
async fn fifo_retry_keeps_key_order() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for amount in 0..2u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "one-key".into(),
                amount,
            })
            .await
            .unwrap();
    }

    let handler = RetryFirstMessageTwice {
        calls: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
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
        handler.counter.wait_for(4, TIMEOUT).await,
        "two retries of A, its ack and B are four calls"
    );
    // Let a stray fifth call surface before the assertions.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let calls = handler.calls.lock().await.clone();
    let labels: Vec<&str> = calls
        .iter()
        .map(|(amount, _)| if *amount == 0 { "A" } else { "B" })
        .collect();
    assert_eq!(
        labels,
        ["A", "A", "A", "B"],
        "B waits until A is settled: {calls:?}"
    );
    let counts: Vec<u32> = calls.iter().map(|(_, count)| *count).collect();
    assert_eq!(counts, [0, 1, 2, 0], "A's count climbs in place: {calls:?}");

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The hold of [`SeqLongHoldTopic`], longer than the 30 s `ack_wait` a 1 s
/// handler timeout derives.
const LONG_HOLD: Duration = Duration::from_secs(35);

shove::define_sequenced_topic!(
    SeqLongHoldTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-long-hold")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(LONG_HOLD)
        .dlq()
        .build()
);

/// Retries a message on its first call and acks it afterwards, keeping every
/// call's metadata.
#[derive(Clone)]
struct RetryOnceRecording {
    seen: Arc<Mutex<Vec<MessageMetadata>>>,
    counter: WaitableCounter,
}

impl MessageHandler<SeqLongHoldTopic> for RetryOnceRecording {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        let first = meta.retry_count == 0;
        self.seen.lock().await.push(meta);
        self.counter.increment();
        if first { Outcome::Retry } else { Outcome::Ack }
    }
}

/// A hold longer than the shard durable's `ack_wait` keeps the record: the
/// progress acks sent through the hold stop JetStream redelivering it, so the
/// handler sees it once per attempt, one retry plus the final ack, every time
/// as the same delivery of the same stream sequence.
#[tokio::test]
async fn fifo_hold_longer_than_ack_wait_is_not_redelivered() {
    const STREAM: &str = "nats-seq-long-hold";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqLongHoldTopic>()
        .await
        .unwrap();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SeqLongHoldTopic>(&OrderMessage {
            order_id: "held".into(),
            amount: 1,
        })
        .await
        .unwrap();

    let handler = RetryOnceRecording {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqLongHoldTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(1)
                    .with_handler_timeout(Duration::from_secs(1)),
            )
            .await
    });

    wait_for_durable(&client, STREAM, "nats-seq-long-hold-shard-0").await;
    let ack_wait = durable_ack_wait(&client, STREAM, "nats-seq-long-hold-shard-0").await;
    assert_eq!(ack_wait, Duration::from_secs(30));
    assert!(LONG_HOLD > ack_wait, "the hold outlasts ack_wait");

    assert!(
        handler
            .counter
            .wait_for(2, LONG_HOLD.saturating_add(TIMEOUT))
            .await,
        "the record is handed back once the hold is over"
    );
    // A redelivery JetStream made during the hold would be handled next.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let seen = handler.seen.lock().await.clone();
    assert_eq!(seen.len(), 2, "one retry plus the final ack: {seen:?}");
    for (attempt, meta) in seen.iter().enumerate() {
        assert_eq!(
            meta.retry_count,
            u32::try_from(attempt).unwrap(),
            "attempt {attempt}: the count climbs in memory"
        );
        assert_eq!(
            meta.offset,
            Some(1),
            "attempt {attempt}: the same record, nothing republished"
        );
        assert_eq!(
            meta.delivery_count,
            Some(1),
            "attempt {attempt}: JetStream delivered the record once"
        );
        assert_eq!(meta.redelivered, attempt > 0, "attempt {attempt}");
    }

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_sequenced_topic!(
    SeqShutdownHoldTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-shutdown-hold")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(60))
        .dlq()
        .build()
);

/// Each call's amount and JetStream delivery count, in call order.
type Sightings = Arc<Mutex<Vec<(u64, Option<u32>)>>>;

/// Retries `amount` 0 the first time it is seen and acks every other call,
/// keeping each call's amount and JetStream delivery count. Shared by every
/// consumer a test starts, so a restart sees what the first run saw.
#[derive(Clone)]
struct RetryFirstSight {
    seen: Sightings,
    counter: WaitableCounter,
}

impl MessageHandler<SeqShutdownHoldTopic> for RetryFirstSight {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        let mut seen = self.seen.lock().await;
        let first_sight = msg.amount == 0 && seen.iter().all(|(amount, _)| *amount != 0);
        seen.push((msg.amount, meta.delivery_count));
        drop(seen);
        self.counter.increment();
        if first_sight {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

/// Starts a FIFO consumer of `T` on `client` that stops with `shutdown`.
fn start_fifo<T, H>(
    client: &NatsClient,
    handler: H,
    shutdown: CancellationToken,
    max_retries: u32,
) -> tokio::task::JoinHandle<Result<(), shove::ShoveError>>
where
    T: shove::SequencedTopic,
    H: MessageHandler<T, Context = ()>,
{
    let consumer = NatsConsumer::new(client.clone());
    tokio::spawn(async move {
        consumer
            .run_fifo::<T, H>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(shutdown)
                    .with_max_retries(max_retries),
            )
            .await
    })
}

/// Stops the consumer `shutdown` controls, failing the test unless it ends
/// cleanly within `within`.
async fn stop_within(
    shutdown: &CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), shove::ShoveError>>,
    within: Duration,
) {
    shutdown.cancel();
    tokio::time::timeout(within, handle)
        .await
        .expect("the stop ends the consumer in time")
        .expect("the FIFO consumer task does not panic")
        .expect("the FIFO consumer stops cleanly");
}

/// A stop during a hold ends the consumer at once, with nothing behind the
/// held record handled. The record is released to the next consumer straight
/// away, well inside its `ack_wait`, and that consumer handles it and then the
/// record behind it. Nothing was republished.
#[tokio::test]
async fn fifo_shutdown_mid_hold_loses_nothing() {
    shutdown_mid_hold_loses_nothing().await;
}

/// The same stop on a multi-threaded runtime, where the task that sends the
/// dropped subscription's UNSUB can run beside the release.
#[tokio::test(flavor = "multi_thread")]
async fn fifo_shutdown_mid_hold_loses_nothing_multi_thread() {
    shutdown_mid_hold_loses_nothing().await;
}

async fn shutdown_mid_hold_loses_nothing() {
    const STREAM: &str = "nats-seq-shutdown-hold";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqShutdownHoldTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    for amount in 0..2u64 {
        publisher
            .publish::<SeqShutdownHoldTopic>(&OrderMessage {
                order_id: "one-key".into(),
                amount,
            })
            .await
            .unwrap();
    }

    let handler = RetryFirstSight {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };

    let first_run = CancellationToken::new();
    let handle =
        start_fifo::<SeqShutdownHoldTopic, _>(&client, handler.clone(), first_run.clone(), 5);
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "A is retried");
    // The shard is now holding A for a minute.
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop_within(&first_run, handle, Duration::from_secs(5)).await;
    assert_eq!(
        handler.seen.lock().await.clone(),
        [(0, Some(1))],
        "nothing behind the held record was handled"
    );

    let second_run = CancellationToken::new();
    let handle =
        start_fifo::<SeqShutdownHoldTopic, _>(&client, handler.clone(), second_run.clone(), 5);
    // The derived ack_wait is 90 s, so only a released record arrives in time.
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "the restarted consumer handles A and then B, saw {:?}",
        handler.seen.lock().await.clone()
    );
    assert_eq!(
        handler.seen.lock().await.clone(),
        [(0, Some(1)), (0, Some(2)), (1, Some(1))],
        "A comes back as JetStream's second delivery, ahead of B"
    );
    assert_eq!(
        stream_last_sequence(&client, STREAM).await,
        2,
        "no copy was published"
    );

    second_run.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_sequenced_topic!(
    SeqDeadLetterRetryTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-dlq-retry")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

/// Rejects `amount` 0 and acks every other call, keeping each call's amount.
#[derive(Clone)]
struct RejectFirstMessage {
    seen: Arc<Mutex<Vec<u64>>>,
    counter: WaitableCounter,
}

impl MessageHandler<SeqDeadLetterRetryTopic> for RejectFirstMessage {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.push(msg.amount);
        self.counter.increment();
        if msg.amount == 0 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }
}

/// A sequenced record whose dead letter is refused stays at its position: an
/// impostor holds the dead letter's id, so the publish fails until the
/// duplicate window lets the real copy through. The publish is retried in
/// place, the handler is not asked again, the record stays un-acked, and the
/// record behind it waits until the dead letter has landed.
#[tokio::test]
async fn fifo_refused_dead_letter_is_retried_in_place() {
    const STREAM: &str = "nats-seq-dlq-retry";
    const DLQ: &str = "nats-seq-dlq-retry-dlq";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqDeadLetterRetryTopic>()
        .await
        .unwrap();
    shrink_duplicate_window(&client, DLQ, Duration::from_secs(3)).await;

    let handler = RejectFirstMessage {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqDeadLetterRetryTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0),
            )
            .await
    });
    // The window starts with the impostor's publish, so the consumer is up first.
    wait_for_durable(&client, STREAM, "nats-seq-dlq-retry-shard-0").await;
    publish_behind_an_impostor(&client).await;

    assert!(handler.counter.wait_for(1, TIMEOUT).await, "A is rejected");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        handler.counter.get(),
        1,
        "the refused dead letter is retried without asking the handler again"
    );
    assert_eq!(
        stream_messages(&client, STREAM).await,
        2,
        "A is not acked while its dead letter is refused, and B waits behind it"
    );

    let dead = take_dead_letters(&client, DLQ, 2).await;
    assert!(
        dead.iter().any(|msg| msg.payload.as_ref() != b"impostor"),
        "the real dead letter lands once the window has passed"
    );
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "B is handled once A is dead-lettered"
    );
    assert_eq!(
        handler.seen.lock().await.clone(),
        [0, 1],
        "A is handled once, then B"
    );
    assert_eq!(
        stream_last_sequence(&client, DLQ).await,
        2,
        "the impostor and a single dead letter of A"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// Stores an impostor under the dead-letter id of the first record of
/// [`SeqDeadLetterRetryTopic`]'s only shard, then publishes A and B there on
/// one key, `amount` 0 and 1, without a `Nats-Msg-Id` of their own.
async fn publish_behind_an_impostor(client: &NatsClient) {
    let mut impostor = async_nats::HeaderMap::new();
    impostor.insert(
        NATS_MESSAGE_ID,
        "nats-seq-dlq-retry.nats-seq-dlq-retry-shard-0.1-dlq",
    );
    raw_publish(client, "nats-seq-dlq-retry-dlq", impostor, b"impostor").await;
    for amount in 0..2u64 {
        let payload = serde_json::to_vec(&OrderMessage {
            order_id: "one-key".into(),
            amount,
        })
        .expect("an OrderMessage encodes");
        raw_publish(
            client,
            "nats-seq-dlq-retry.shard.0",
            async_nats::HeaderMap::new(),
            &payload,
        )
        .await;
    }
}

/// A stop while a refused dead letter waits for its next attempt leaves the
/// record un-acked at its position. The consumer ends at once, A and B are
/// both still in the stream, the DLQ holds only the impostor, and the
/// handler saw A once and never saw B.
#[tokio::test]
async fn fifo_shutdown_during_dead_letter_backoff_keeps_the_record() {
    const STREAM: &str = "nats-seq-dlq-retry";
    const DLQ: &str = "nats-seq-dlq-retry-dlq";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqDeadLetterRetryTopic>()
        .await
        .unwrap();

    let handler = RejectFirstMessage {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let shutdown = CancellationToken::new();
    let handle =
        start_fifo::<SeqDeadLetterRetryTopic, _>(&client, handler.clone(), shutdown.clone(), 0);
    wait_for_durable(&client, STREAM, "nats-seq-dlq-retry-shard-0").await;
    // The default two-minute window holds the impostor's id through the test.
    publish_behind_an_impostor(&client).await;

    assert!(handler.counter.wait_for(1, TIMEOUT).await, "A is rejected");
    // Past the first refused round, so the shard is waiting out a backoff.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    stop_within(&shutdown, handle, Duration::from_secs(10)).await;

    assert_eq!(
        handler.seen.lock().await.clone(),
        [0],
        "A was handled once and nothing behind it was handled"
    );
    assert_eq!(
        stream_messages(&client, STREAM).await,
        2,
        "A was not acked, and B is still behind it"
    );
    assert_eq!(
        stream_messages(&client, DLQ).await,
        1,
        "the DLQ holds only the impostor"
    );
    broker.close().await;
}

/// A stop mid-ladder hands the record to the next consumer, and the hand-off
/// spends a retry: each delivery starts one count higher than the last. A
/// record that is retried and stopped forever still reaches the DLQ, here on
/// its third delivery with `max_retries` at 2.
#[tokio::test]
async fn fifo_restart_mid_ladder_advances_the_count() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqShutdownHoldTopic>()
        .await
        .unwrap();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SeqShutdownHoldTopic>(&OrderMessage {
            order_id: "one-key".into(),
            amount: 0,
        })
        .await
        .unwrap();

    let handler = RetryEveryCall {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    for delivery in 1..=2u32 {
        let shutdown = CancellationToken::new();
        let handle =
            start_fifo::<SeqShutdownHoldTopic, _>(&client, handler.clone(), shutdown.clone(), 2);
        assert!(
            handler.counter.wait_for(delivery, TIMEOUT).await,
            "delivery {delivery} reaches the handler"
        );
        // The shard is now holding the record for a minute.
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop_within(&shutdown, handle, Duration::from_secs(5)).await;
        let seen = handler.seen.lock().await.clone();
        assert_eq!(
            seen.last()
                .map(|meta| (meta.retry_count, meta.delivery_count)),
            Some((delivery - 1, Some(delivery))),
            "delivery {delivery} starts at count {}",
            delivery - 1
        );
    }

    // The third delivery starts at the budget, so its Retry dead-letters.
    let shutdown = CancellationToken::new();
    let handle =
        start_fifo::<SeqShutdownHoldTopic, _>(&client, handler.clone(), shutdown.clone(), 2);
    let dead = take_dead_letters(&client, "nats-seq-shutdown-hold-dlq", 1).await;
    assert_eq!(
        header(&dead[0], "Shove-Death-Reason"),
        Some("max_retries_exceeded")
    );
    assert_eq!(
        header(&dead[0], "Shove-Retry-Count"),
        Some("2"),
        "the dead letter carries the count the record was dead-lettered at"
    );
    let calls: Vec<(u32, Option<u32>)> = handler
        .seen
        .lock()
        .await
        .iter()
        .map(|meta| (meta.retry_count, meta.delivery_count))
        .collect();
    assert_eq!(calls, [(0, Some(1)), (1, Some(2)), (2, Some(3))]);
    stop_within(&shutdown, handle, Duration::from_secs(5)).await;
    broker.close().await;
}

/// Retries every call, keeping each call's metadata.
#[derive(Clone)]
struct RetryEveryCall {
    seen: Arc<Mutex<Vec<MessageMetadata>>>,
    counter: WaitableCounter,
}

impl MessageHandler<SeqShutdownHoldTopic> for RetryEveryCall {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.push(meta);
        self.counter.increment();
        Outcome::Retry
    }
}

/// A's first call, `amount` 0 at count 0, sleeps `first_delay` and answers
/// `first_outcome`; a delay past the handler timeout makes it a timeout.
/// Every other call acks. Each call's amount and metadata are kept in order.
#[derive(Clone)]
struct FirstCallOf {
    first_outcome: Outcome,
    first_delay: Duration,
    calls: Arc<Mutex<Vec<(u64, MessageMetadata)>>>,
    counter: WaitableCounter,
}

impl FirstCallOf {
    fn new(first_outcome: Outcome, first_delay: Duration) -> Self {
        Self {
            first_outcome,
            first_delay,
            calls: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    async fn answer(&self, amount: u64, meta: MessageMetadata) -> Outcome {
        let first = amount == 0 && meta.retry_count == 0;
        self.calls.lock().await.push((amount, meta));
        self.counter.increment();
        if !first {
            return Outcome::Ack;
        }
        tokio::time::sleep(self.first_delay).await;
        self.first_outcome.clone()
    }

    /// Asserts that A was retried in place, once, before B was handled: two
    /// calls of the same delivery of A at stream sequence 1, then B.
    async fn assert_a_retried_in_place(&self) {
        let calls = self.calls.lock().await.clone();
        let amounts: Vec<u64> = calls.iter().map(|(amount, _)| *amount).collect();
        assert_eq!(amounts, [0, 0, 1], "B waits until A is settled");
        for (attempt, (_, meta)) in calls.iter().take(2).enumerate() {
            assert_eq!(
                meta.retry_count,
                u32::try_from(attempt).unwrap(),
                "attempt {attempt}: the count climbs in memory"
            );
            assert_eq!(
                meta.offset,
                Some(1),
                "attempt {attempt}: the same record, nothing republished"
            );
            assert_eq!(
                meta.delivery_count,
                Some(1),
                "attempt {attempt}: JetStream delivered the record once"
            );
        }
    }
}

impl MessageHandler<SeqSkipTopic> for FirstCallOf {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.answer(msg.amount, meta).await
    }
}

/// Publishes A then B on one key of [`SeqSkipTopic`], runs a FIFO consumer
/// with `handler_timeout` until A has been called twice and B once, and
/// checks that A was retried in place.
async fn first_call_is_retried_in_place(handler: FirstCallOf, handler_timeout: Duration) {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    for amount in 0..2u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "one-key".into(),
                amount,
            })
            .await
            .unwrap();
    }

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
                    .with_max_retries(5)
                    .with_handler_timeout(handler_timeout),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "A twice and B once, saw {:?}",
        handler.calls.lock().await.clone()
    );
    // Let a stray fourth call surface before the assertions.
    tokio::time::sleep(Duration::from_millis(500)).await;
    handler.assert_a_retried_in_place().await;

    stop_within(&shutdown, handle, Duration::from_secs(5)).await;
    broker.close().await;
}

/// A `Defer` on a sequenced consumer is a `Retry`, carried out in place.
#[tokio::test]
async fn fifo_defer_retries_in_place() {
    first_call_is_retried_in_place(
        FirstCallOf::new(Outcome::Defer, Duration::ZERO),
        Duration::from_secs(30),
    )
    .await;
}

/// A handler timeout on a sequenced consumer resolves to a `Retry`, carried
/// out in place.
#[tokio::test]
async fn fifo_handler_timeout_retries_in_place() {
    first_call_is_retried_in_place(
        FirstCallOf::new(Outcome::Ack, Duration::from_secs(2)),
        Duration::from_millis(500),
    )
    .await;
}

shove::define_sequenced_topic!(
    SeqOutageTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-seq-outage")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(3))
        .dlq()
        .build()
);

impl MessageHandler<SeqOutageTopic> for FirstCallOf {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.answer(msg.amount, meta).await
    }
}

/// The server freezes one second into A's three-second hold and resumes two
/// seconds after it ended, so the shard hands A back and acks it while the
/// server is down. The renewal and the ack wait in the connection and land
/// once the server resumes: A is handled twice as one delivery, B once after
/// it, both are retired, and nothing is redelivered or dead-lettered.
#[tokio::test]
async fn fifo_broker_outage_during_a_hold_loses_and_repeats_nothing() {
    const STREAM: &str = "nats-seq-outage";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqOutageTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    for amount in 0..2u64 {
        publisher
            .publish::<SeqOutageTopic>(&OrderMessage {
                order_id: "one-key".into(),
                amount,
            })
            .await
            .unwrap();
    }

    let handler = FirstCallOf::new(Outcome::Retry, Duration::ZERO);
    let shutdown = CancellationToken::new();
    let handle = start_fifo::<SeqOutageTopic, _>(&client, handler.clone(), shutdown.clone(), 5);
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "A is retried");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tb.pause().await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    tb.unpause().await;

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "A is handed back and B follows it, saw {:?}",
        handler.calls.lock().await.clone()
    );
    assert_eq!(
        settle_stream_messages(&client, STREAM, 0).await,
        0,
        "A and B are both retired"
    );
    // A redelivery of either would be handled within this.
    tokio::time::sleep(Duration::from_secs(2)).await;
    handler.assert_a_retried_in_place().await;
    assert_eq!(
        stream_messages(&client, "nats-seq-outage-dlq").await,
        0,
        "nothing was dead-lettered"
    );

    stop_within(&shutdown, handle, Duration::from_secs(5)).await;
    broker.close().await;
}

// ===========================================================================
// Flow control: each worker bounds its own in-flight work
// ===========================================================================

/// The `max_ack_pending` of the durable `consumer` on `stream`.
async fn durable_max_ack_pending(client: &NatsClient, stream: &str, consumer: &str) -> i64 {
    client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("stream should exist")
        .consumer_info(consumer)
        .await
        .expect("durable should exist")
        .config
        .max_ack_pending
}

/// Declares the durable `consumer` on `stream` ahead of the consumer, the way
/// an operator might, with an unbounded budget and the given `ack_wait`.
async fn predeclare_durable(client: &NatsClient, stream: &str, consumer: &str, ack_wait: Duration) {
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;

    client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists")
        .create_consumer(PullConsumerConfig {
            durable_name: Some(consumer.to_string()),
            ack_policy: AckPolicy::Explicit,
            ack_wait,
            max_ack_pending: -1,
            ..Default::default()
        })
        .await
        .expect("the durable is created");
}

shove::define_topic!(
    FleetTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-fleet").dlq().build()
);

/// Runs each message for `hold`, counting the handlers running at once and
/// keeping the most it saw.
#[derive(Clone)]
struct ConcurrencyProbe {
    hold: Duration,
    running: Arc<AtomicU32>,
    peak: Arc<AtomicU32>,
    done: WaitableCounter,
}

impl ConcurrencyProbe {
    fn new(hold: Duration) -> Self {
        Self {
            hold,
            running: Arc::new(AtomicU32::new(0)),
            peak: Arc::new(AtomicU32::new(0)),
            done: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<FleetTopic> for ConcurrencyProbe {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(running, Ordering::SeqCst);
        tokio::time::sleep(self.hold).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        self.done.increment();
        Outcome::Ack
    }
}

/// Four supervisor workers on one topic, each with a prefetch of two. The
/// first to start creates the durable and the others attach to it, so a
/// budget taken from one worker's prefetch would cap all four at two
/// messages in flight. Each worker bounds itself instead, and the durable
/// has no budget of its own.
#[tokio::test]
async fn supervisor_workers_are_not_capped_by_one_prefetch() {
    const WORKERS: usize = 4;
    const PREFETCH: u16 = 2;
    const MESSAGES: u32 = 40;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<FleetTopic>().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..MESSAGES)
        .map(|i| SimpleMessage {
            id: format!("fleet-{i}"),
            content: String::new(),
        })
        .collect();
    broker
        .publisher()
        .await
        .unwrap()
        .publish_batch::<FleetTopic>(&messages)
        .await
        .unwrap();

    let probe = ConcurrencyProbe::new(Duration::from_millis(200));
    let mut tokens = Vec::with_capacity(WORKERS);
    let mut runs = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let mut supervisor = broker.consumer_supervisor();
        supervisor
            .register::<FleetTopic, _>(
                probe.clone(),
                ConsumerOptions::<Nats>::new().with_prefetch_count(PREFETCH),
            )
            .unwrap();
        tokens.push(supervisor.cancellation_token());
        runs.push(tokio::spawn(supervisor.run_until_timeout(
            std::future::pending(),
            Duration::from_secs(10),
        )));
    }

    assert!(
        probe.done.wait_for(MESSAGES, Duration::from_secs(60)).await,
        "every message is handled, saw {}",
        probe.done.get()
    );
    let peak = probe.peak.load(Ordering::SeqCst);
    assert!(
        peak >= 6,
        "{WORKERS} workers at prefetch {PREFETCH} ran at most {peak} handlers at once"
    );
    let bound = u32::try_from(WORKERS).unwrap() * u32::from(PREFETCH);
    assert!(
        peak <= bound,
        "{WORKERS} workers at prefetch {PREFETCH} ran {peak} handlers at once, past their {bound} slots"
    );
    assert_eq!(
        durable_max_ack_pending(&client, "nats-fleet", "nats-fleet-consumer").await,
        -1,
        "the durable a worker creates has no budget of its own"
    );

    for token in &tokens {
        token.cancel();
    }
    for run in runs {
        assert!(run.await.unwrap().is_clean());
    }
    broker.close().await;
}

/// A group with a zero prefetch and concurrent processing runs one message at
/// a time, the way `ConsumerOptions::with_prefetch_count` treats a zero, and
/// its durable keeps no budget of its own.
#[tokio::test]
async fn group_prefetch_zero_concurrent_consumes() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=3)
        .map(|i| SimpleMessage {
            id: format!("zero-{i}"),
            content: String::new(),
        })
        .collect();
    broker
        .publisher()
        .await
        .unwrap()
        .publish_batch::<WorkTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let config = NatsConsumerGroupConfig::new(1..=1)
        .with_prefetch_count(0)
        .with_concurrent_processing(true);
    let mut group = broker.consumer_group();
    group
        .register::<WorkTopic, _>(ConsumerGroupConfig::new(config), move || hc.clone())
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(3, TIMEOUT).await;
        t.cancel();
    });
    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;

    assert_eq!(
        handler.counter.get(),
        3,
        "a zero prefetch must run as one, not stall"
    );
    assert!(outcome.is_clean(), "the group drains: {outcome:?}");
    assert_eq!(
        durable_max_ack_pending(&client, "nats-work", "nats-work-consumer").await,
        -1,
        "the group's durable has no budget of its own"
    );
    broker.close().await;
}

shove::define_topic!(
    QueuedTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-queued").dlq().build()
);

/// Sleeps `delay`, then records the message's id and acks it.
#[derive(Clone)]
struct SlowRecorder {
    delay: Duration,
    seen: Arc<Mutex<Vec<String>>>,
    counter: WaitableCounter,
}

impl MessageHandler<QueuedTopic> for SlowRecorder {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.seen.lock().await.push(msg.id);
        self.counter.increment();
        Outcome::Ack
    }
}

/// A one-at-a-time worker on a durable whose `ack_wait` is three handler runs
/// long. Every message is handled within its `ack_wait` once it is delivered,
/// so a worker that only pulls what it can start never sees a redelivery,
/// while one that pulled the backlog up front would leave the later messages
/// waiting past `ack_wait` and handle them twice.
#[tokio::test]
async fn queued_messages_are_not_redelivered_mid_flight() {
    const ACK_WAIT: Duration = Duration::from_secs(3);
    const MESSAGES: u32 = 6;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<QueuedTopic>().await.unwrap();
    predeclare_durable(&client, "nats-queued", "nats-queued-consumer", ACK_WAIT).await;
    let messages: Vec<SimpleMessage> = (0..MESSAGES)
        .map(|i| SimpleMessage {
            id: format!("queued-{i}"),
            content: String::new(),
        })
        .collect();
    broker
        .publisher()
        .await
        .unwrap()
        .publish_batch::<QueuedTopic>(&messages)
        .await
        .unwrap();

    let handler = SlowRecorder {
        delay: Duration::from_secs(1),
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<QueuedTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler
            .counter
            .wait_for(MESSAGES, Duration::from_secs(30))
            .await,
        "every message is handled"
    );
    // A redelivery of any of them would be handled within this.
    tokio::time::sleep(ACK_WAIT.saturating_add(Duration::from_secs(2))).await;
    let seen = handler.seen.lock().await.clone();
    let mut calls: HashMap<&str, usize> = HashMap::new();
    for id in &seen {
        *calls.entry(id.as_str()).or_default() += 1;
    }
    let repeated: Vec<_> = calls.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        repeated.is_empty(),
        "redelivered mid-flight: {repeated:?}, calls {seen:?}"
    );
    assert_eq!(seen.len(), MESSAGES as usize, "calls {seen:?}");

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The delay of [`LongHoldTopic`]'s only hold tier.
const RETRY_HOLD: Duration = Duration::from_secs(5);

shove::define_topic!(
    LongHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-long-hold")
        .hold_queue(RETRY_HOLD)
        .dlq()
        .build()
);

/// Each call's message id, retry count and arrival.
type Calls = Arc<Mutex<Vec<(String, u32, Instant)>>>;

/// Retries the message `held` on its first attempt and acks every other call,
/// keeping each call.
#[derive(Clone)]
struct RetryHeldOnce {
    calls: Calls,
    counter: WaitableCounter,
}

impl RetryHeldOnce {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    /// Records a call and answers it.
    async fn answer(&self, id: String, retry_count: u32) -> Outcome {
        let retry = id == "held" && retry_count == 0;
        self.calls
            .lock()
            .await
            .push((id, retry_count, Instant::now()));
        self.counter.increment();
        if retry { Outcome::Retry } else { Outcome::Ack }
    }

    /// The retry counts of every call `id` got, in call order.
    async fn counts_of(&self, id: &str) -> Vec<u32> {
        self.calls
            .lock()
            .await
            .iter()
            .filter(|(seen, _, _)| seen == id)
            .map(|(_, count, _)| *count)
            .collect()
    }
}

impl MessageHandler<LongHoldTopic> for RetryHeldOnce {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.answer(msg.id, meta.retry_count).await
    }
}

/// A one-at-a-time worker holds a retried message for a five-second delay on
/// a durable whose `ack_wait` is two seconds. The hold gives up the worker's
/// processing slot, so the messages behind it are handled during the delay,
/// and its progress acks keep the held message from being redelivered while
/// it waits.
#[tokio::test]
async fn retry_hold_does_not_block_other_messages() {
    const ACK_WAIT: Duration = Duration::from_secs(2);
    const QUICK: usize = 3;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<LongHoldTopic>().await.unwrap();
    predeclare_durable(
        &client,
        "nats-long-hold",
        "nats-long-hold-consumer",
        ACK_WAIT,
    )
    .await;
    let publisher = broker.publisher().await.unwrap();
    let ids = std::iter::once("held".to_string()).chain((0..QUICK).map(|i| format!("quick-{i}")));
    for id in ids {
        publisher
            .publish::<LongHoldTopic>(&SimpleMessage {
                id,
                content: String::new(),
            })
            .await
            .unwrap();
    }

    let handler = RetryHeldOnce::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<LongHoldTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });

    // The held message twice and every other message once.
    let expected = u32::try_from(QUICK + 2).unwrap();
    assert!(
        handler
            .counter
            .wait_for(expected, RETRY_HOLD.saturating_add(TIMEOUT))
            .await,
        "saw {:?}",
        handler.calls.lock().await.clone()
    );
    // A redelivery of the held message would be handled within this.
    tokio::time::sleep(ACK_WAIT).await;

    let calls = handler.calls.lock().await.clone();
    let held_at = calls
        .iter()
        .find(|(id, count, _)| id == "held" && *count == 0)
        .map(|(_, _, at)| *at)
        .expect("the held message is handled");
    let quick: Vec<_> = calls
        .iter()
        .filter(|(id, _, _)| id.starts_with("quick-"))
        .collect();
    assert_eq!(quick.len(), QUICK, "each other message once: {calls:?}");
    for (id, _, at) in &quick {
        let after = at.saturating_duration_since(held_at);
        assert!(
            after < RETRY_HOLD.saturating_sub(Duration::from_secs(2)),
            "{id} waited {after:?} behind the hold: {calls:?}"
        );
    }
    assert_eq!(
        handler.counts_of("held").await,
        [0, 1],
        "the held message once, then its copy: {calls:?}"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_topic!(
    OutageHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-outage-hold")
        .hold_queue(Duration::from_secs(3))
        .dlq()
        .build()
);

impl MessageHandler<OutageHoldTopic> for RetryHeldOnce {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.answer(msg.id, meta.retry_count).await
    }
}

/// The server freezes one second into a three-second retry hold and resumes
/// two seconds after it ended, so the hold's last renewal, the copy's publish
/// and the original's ack all wait in the connection and land once the
/// server resumes. The message is handled once and its copy once, both are
/// retired, and nothing is dead-lettered.
#[tokio::test]
async fn broker_outage_during_a_retry_hold_loses_and_repeats_nothing() {
    const STREAM: &str = "nats-outage-hold";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<OutageHoldTopic>()
        .await
        .unwrap();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<OutageHoldTopic>(&SimpleMessage {
            id: "held".into(),
            content: String::new(),
        })
        .await
        .unwrap();

    let handler = RetryHeldOnce::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<OutageHoldTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "held is retried"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    tb.pause().await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    tb.unpause().await;

    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "the copy is handled once the server resumes, saw {:?}",
        handler.calls.lock().await.clone()
    );
    assert_eq!(
        settle_stream_messages(&client, STREAM, 0).await,
        0,
        "the original and its copy are both retired"
    );
    // A redelivery of either would be handled within this.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(handler.counts_of("held").await, [0, 1]);
    assert_eq!(
        stream_last_sequence(&client, STREAM).await,
        2,
        "one copy was published"
    );
    assert_eq!(
        stream_messages(&client, "nats-outage-hold-dlq").await,
        0,
        "nothing was dead-lettered"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the consumer stops in time")
        .unwrap()
        .ok();
    broker.close().await;
}

shove::define_topic!(
    IdleTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-idle").dlq().build()
);

impl MessageHandler<IdleTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// Waits until the durable `consumer` on `stream` has a pull request waiting
/// on the server.
async fn wait_for_open_pull(client: &NatsClient, stream: &str, consumer: &str) {
    let stream = client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Ok(info) = stream.consumer_info(consumer).await
            && info.num_waiting > 0
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no pull request was ever opened on {consumer}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A stop that lands while an idle worker probes its connection still ends
/// the worker at once. The server is frozen once the worker's pull is open,
/// so the pull ends on the client's backstop, five seconds past its 30 s
/// expiry, with nothing delivered, and the probe that follows waits on the
/// frozen server.
#[tokio::test]
async fn a_stop_during_the_liveness_probe_ends_the_worker() {
    /// Past the pull's expiry and the client's backstop, and two seconds into
    /// the probe's request timeout.
    const INTO_THE_PROBE: Duration = Duration::from_secs(37);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<IdleTopic>().await.unwrap();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<IdleTopic, _>(
                CountingHandler::new(),
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(2),
            )
            .await
    });
    wait_for_open_pull(&client, "nats-idle", "nats-idle-consumer").await;
    let opened = Instant::now();
    tb.pause().await;
    tokio::time::sleep_until(opened + INTO_THE_PROBE).await;

    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(10), handle).await;
    tb.unpause().await;
    stopped
        .expect("the worker stops while its probe waits on the frozen server")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

shove::define_topic!(
    MaxWaitingTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-max-waiting").dlq().build()
);

impl MessageHandler<MaxWaitingTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// A pull the server ends with a status, while the server still answers, is
/// not a connection error. The durable keeps one waiting pull at a time, so
/// once the worker has a slot free beside its open pull, the second pull it
/// asks for is refused with `409 Exceeded MaxWaiting`. The worker probes its
/// connection, finds the server answering, and goes on consuming without
/// spending the single reconnect attempt it is allowed.
#[tokio::test]
async fn a_pull_the_server_refuses_is_not_a_connection_error() {
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
    use tracing::Level;
    use tracing::subscriber::set_default;

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    let _logging = set_default(subscriber);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<MaxWaitingTopic>()
        .await
        .unwrap();
    client
        .jetstream()
        .get_stream("nats-max-waiting")
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-max-waiting-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: -1,
            max_waiting: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<MaxWaitingTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(2)
                    .with_max_reconnect_attempts(1),
            )
            .await
    });
    wait_for_open_pull(&client, "nats-max-waiting", "nats-max-waiting-consumer").await;

    let publisher = broker.publisher().await.unwrap();
    let message = |id: &str| SimpleMessage {
        id: id.to_string(),
        content: String::new(),
    };
    publisher
        .publish::<MaxWaitingTopic>(&message("first"))
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "first is handled"
    );
    // The slot the first message freed asks for a second pull, which the
    // server refuses while the first pull still waits.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let refused = logs.contents();
    assert!(
        refused.contains("Exceeded MaxWaiting"),
        "the second pull was refused: {refused}"
    );
    assert!(
        !handle.is_finished(),
        "the refused pull ended the consumer: {:?}",
        handle.await
    );

    publisher
        .publish::<MaxWaitingTopic>(&message("second"))
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "the worker goes on consuming"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the consumer stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

/// What a `tracing` subscriber wrote, for asserting on a log line.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        let buffer = self.0.lock().expect("log buffer mutex poisoned");
        String::from_utf8_lossy(&buffer).into_owned()
    }

    fn clear(&self) {
        self.0.lock().expect("log buffer mutex poisoned").clear();
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

shove::define_topic!(
    CappedTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-capped").dlq().build()
);

impl MessageHandler<CappedTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// Runs a consumer of [`CappedTopic`] until it has handled one more message.
async fn consume_one_capped(tb: &TestBroker, options: ConsumerOptions<Nats>) {
    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let options = options.with_shutdown(shutdown.clone());
    let consumer = NatsConsumer::new(tb.client());
    let handle = tokio::spawn(async move { consumer.run::<CappedTopic, _>(hc, (), options).await });
    tb.broker()
        .publisher()
        .await
        .unwrap()
        .publish::<CappedTopic>(&SimpleMessage {
            id: "capped".into(),
            content: String::new(),
        })
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the message is handled"
    );
    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

/// A consumer that attaches to a durable with a finite `max_ack_pending` it
/// did not ask for, as on a durable an earlier version created, names the
/// budget in force and the in-place change that lifts it. One given that very
/// cap says nothing.
#[tokio::test]
async fn attaching_to_a_capped_durable_names_the_budget_in_force() {
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
    use tracing::Level;
    use tracing::subscriber::set_default;

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    let _logging = set_default(subscriber);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<CappedTopic>().await.unwrap();
    client
        .jetstream()
        .get_stream("nats-capped")
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-capped-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 5,
            ..Default::default()
        })
        .await
        .unwrap();

    consume_one_capped(&tb, ConsumerOptions::<Nats>::new()).await;
    let warned = logs.contents();
    assert!(
        warned.contains("max_ack_pending=5")
            && warned
                .contains("nats consumer edit nats-capped nats-capped-consumer --max-pending=-1"),
        "the budget in force and its in-place fix are named: {warned}"
    );

    logs.clear();
    consume_one_capped(&tb, ConsumerOptions::<Nats>::new().with_max_ack_pending(5)).await;
    let quiet = logs.contents();
    assert!(
        !quiet.contains("nats consumer edit"),
        "a consumer given that cap is not warned: {quiet}"
    );
    broker.close().await;
}

/// Each ack sent for a durable: the stream sequence it is for, its payload
/// (empty or `+ACK` for an ack, `+WPI` for a progress ack, `-NAK`, ...) and
/// when it was seen.
type AckLog = Arc<Mutex<Vec<(u64, String, Instant)>>>;

/// Whether `payload` acks its message: the client sends an empty payload,
/// which the server reads as `+ACK`.
fn is_ack(payload: &str) -> bool {
    payload.is_empty() || payload == "+ACK"
}

/// Watches every ack sent for the durable `consumer` on `stream`, by
/// subscribing to the ack subjects the server routes them on,
/// `$JS.ACK.{stream}.{consumer}.{delivered}.{stream_seq}...`.
async fn watch_acks(client: &NatsClient, stream: &str, consumer: &str) -> AckLog {
    use futures_util::StreamExt;

    let raw = client.jetstream().client();
    let mut acks = raw
        .subscribe(format!("$JS.ACK.{stream}.{consumer}.>"))
        .await
        .expect("the ack subjects can be watched");
    raw.flush()
        .await
        .expect("the watch is in place before any ack");
    let log: AckLog = Arc::default();
    let sink = log.clone();
    tokio::spawn(async move {
        while let Some(ack) = acks.next().await {
            let sequence = ack
                .subject
                .split('.')
                .nth(5)
                .and_then(|token| token.parse().ok())
                .expect("an ack subject names the stream sequence");
            let payload = String::from_utf8_lossy(&ack.payload).into_owned();
            sink.lock().await.push((sequence, payload, Instant::now()));
        }
    });
    log
}

/// The delay of [`HoldSlotTopic`]'s only hold tier.
const SLOT_HOLD: Duration = Duration::from_secs(4);

shove::define_topic!(
    HoldSlotTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-hold-slot")
        .hold_queue(SLOT_HOLD)
        .dlq()
        .build()
);

/// Retries each message named in `retried` on its first attempt and acks
/// every other call, keeping each call's id, retry count and arrival.
#[derive(Clone)]
struct RetryEachOnce {
    retried: &'static [&'static str],
    calls: Calls,
    counter: WaitableCounter,
}

impl RetryEachOnce {
    fn new(retried: &'static [&'static str]) -> Self {
        Self {
            retried,
            calls: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    /// The retry count and arrival of every call `id` got, in call order.
    async fn calls_of(&self, id: &str) -> Vec<(u32, Instant)> {
        self.calls
            .lock()
            .await
            .iter()
            .filter(|(seen, _, _)| seen == id)
            .map(|(_, count, at)| (*count, *at))
            .collect()
    }
}

impl MessageHandler<HoldSlotTopic> for RetryEachOnce {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        let retry = self.retried.contains(&msg.id.as_str()) && meta.retry_count == 0;
        self.calls
            .lock()
            .await
            .push((msg.id, meta.retry_count, Instant::now()));
        self.counter.increment();
        if retry { Outcome::Retry } else { Outcome::Ack }
    }
}

/// A one-at-a-time worker retries `first`, which takes its only hold slot,
/// and then `second`, which finds no hold slot free and waits for one. The
/// durable's `ack_wait` is half the hold, so the wait outlasts it. While it
/// waits, `second` keeps its processing slot, so `third` is not pulled until
/// `first`'s hold is over, and it renews its `ack_wait`, so it is not
/// redelivered. Taking the slot sends one more progress ack at once.
#[tokio::test]
async fn a_retry_waiting_for_a_hold_slot_keeps_its_slot_and_its_delivery() {
    const ACK_WAIT: Duration = Duration::from_secs(2);
    const STREAM: &str = "nats-hold-slot";
    const DURABLE: &str = "nats-hold-slot-consumer";

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<HoldSlotTopic>().await.unwrap();
    predeclare_durable(&client, STREAM, DURABLE, ACK_WAIT).await;
    let acks = watch_acks(&client, STREAM, DURABLE).await;
    let publisher = broker.publisher().await.unwrap();
    for id in ["first", "second", "third"] {
        publisher
            .publish::<HoldSlotTopic>(&SimpleMessage {
                id: id.to_string(),
                content: String::new(),
            })
            .await
            .unwrap();
    }

    let handler = RetryEachOnce::new(&["first", "second"]);
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<HoldSlotTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });

    // `first` and `second` twice each, `third` once.
    assert!(
        handler
            .counter
            .wait_for(5, SLOT_HOLD.saturating_mul(2).saturating_add(TIMEOUT))
            .await,
        "saw {:?}",
        handler.calls.lock().await.clone()
    );
    // A redelivery of `second` would be handled within this.
    tokio::time::sleep(ACK_WAIT).await;

    let second = handler.calls_of("second").await;
    let counts: Vec<u32> = second.iter().map(|(count, _)| *count).collect();
    assert_eq!(
        counts,
        [0, 1],
        "second once, then its copy, never redelivered: {second:?}"
    );
    let second_at = second.first().map(|(_, at)| *at).unwrap();
    let third_at = handler
        .calls_of("third")
        .await
        .first()
        .map(|(_, at)| *at)
        .unwrap();
    let behind = third_at.saturating_duration_since(second_at);
    assert!(
        behind >= SLOT_HOLD.saturating_sub(Duration::from_secs(1)),
        "third started {behind:?} after second, so second gave up its processing slot while it waited"
    );

    let acks = acks.lock().await.clone();
    let first_retired = acks
        .iter()
        .find(|(sequence, payload, _)| *sequence == 1 && is_ack(payload))
        .map(|(_, _, at)| *at)
        .unwrap_or_else(|| panic!("first is acked once its copy is stored: {acks:?}"));
    let renewals = acks
        .iter()
        .filter(|(sequence, payload, at)| {
            *sequence == 2 && payload == "+WPI" && *at > second_at && *at < first_retired
        })
        .count();
    assert!(
        renewals >= 2,
        "second renewed its ack_wait {renewals} times while it waited: {acks:?}"
    );
    let on_taking = acks.iter().any(|(sequence, payload, at)| {
        *sequence == 2
            && payload == "+WPI"
            && at.saturating_duration_since(first_retired) < Duration::from_millis(250)
            && *at >= first_retired
    });
    assert!(
        on_taking,
        "second sent a progress ack on taking the freed hold slot: {acks:?}"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A stopping worker leaves no pull request behind it. Once an idle worker
/// with an open pull has stopped, a message published to the topic goes to
/// the next consumer at once, well inside the 90 s `ack_wait` a message
/// delivered to the stopped worker's pull would wait out.
#[tokio::test]
async fn a_stopped_worker_leaves_no_pull_behind() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<IdleTopic>().await.unwrap();

    let stopped = CountingHandler::new();
    let sc_handler = stopped.clone();
    let first_run = CancellationToken::new();
    let first_token = first_run.clone();
    let consumer = NatsConsumer::new(client.clone());
    let first = tokio::spawn(async move {
        consumer
            .run::<IdleTopic, _>(
                sc_handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(first_token)
                    .with_prefetch_count(1),
            )
            .await
    });
    wait_for_open_pull(&client, "nats-idle", "nats-idle-consumer").await;
    first_run.cancel();
    tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .expect("the worker stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<IdleTopic>(&SimpleMessage {
            id: "after-the-stop".into(),
            content: String::new(),
        })
        .await
        .unwrap();
    let next = CountingHandler::new();
    let hc = next.clone();
    let second_run = CancellationToken::new();
    let second_token = second_run.clone();
    let consumer = NatsConsumer::new(client.clone());
    let second = tokio::spawn(async move {
        consumer
            .run::<IdleTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(second_token)
                    .with_prefetch_count(1),
            )
            .await
    });
    assert!(
        next.counter.wait_for(1, Duration::from_secs(10)).await,
        "the next consumer gets the message at once"
    );
    assert_eq!(stopped.counter.get(), 0);

    second_run.cancel();
    second.await.unwrap().ok();
    broker.close().await;
}

/// The companion of
/// `a_stored_retry_copy_of_the_same_delivery_retires_the_original`. At a
/// prefetch of one the hold frees the worker's slot, so the worker takes the
/// stored copy during the hold and acks it. The copy's own publish then comes
/// back as a duplicate the read-back cannot confirm, and the original is
/// redelivered a second later instead of acked. Once the duplicate window
/// lets the id go, the next attempt's copy is stored and the original acked:
/// the record is delayed and copied twice, and never lost.
#[tokio::test]
async fn an_unconfirmable_duplicate_copy_delays_the_original_and_never_loses_it() {
    const WINDOW: Duration = Duration::from_secs(5);

    /// Each call's retry count and stream sequence.
    type Seen = Arc<Mutex<Vec<(u32, Option<i64>)>>>;

    /// Retries every original, acks every copy, and keeps each call.
    #[derive(Clone)]
    struct RetryOriginals {
        seen: Seen,
        counter: WaitableCounter,
    }

    impl MessageHandler<HoldTopic> for RetryOriginals {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            let original = meta.retry_count == 0;
            self.seen.lock().await.push((meta.retry_count, meta.offset));
            self.counter.increment();
            if original {
                Outcome::Retry
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<HoldTopic>().await.unwrap();
    shrink_duplicate_window(&client, "nats-hold", WINDOW).await;
    let payload = simple_payload("copied-twice");
    raw_publish(&client, "nats-hold", async_nats::HeaderMap::new(), &payload).await;

    let handler = RetryOriginals {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<HoldTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(3)
                    .with_prefetch_count(1),
            )
            .await
    });
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the original is retried"
    );
    // The original is now held for 3 s. Store its copy first, as a publish
    // whose ack was lost would have.
    let mut copy = async_nats::HeaderMap::new();
    copy.insert("Shove-Retry-Count", "1");
    copy.insert("Shove-Copy-Of", "nats-hold.nats-hold-consumer.1");
    copy.insert(NATS_MESSAGE_ID, "nats-hold.nats-hold-consumer.1-r1");
    raw_publish(&client, "nats-hold", copy, &payload).await;

    let deadline = Instant::now() + WINDOW.saturating_add(TIMEOUT);
    while stream_messages(&client, "nats-hold").await > 0 {
        assert!(
            Instant::now() < deadline,
            "the original is never retired, saw {:?}",
            handler.seen.lock().await.clone()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A redelivery of either would be handled within this.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let seen = handler.seen.lock().await.clone();
    let copies: Vec<Option<i64>> = seen
        .iter()
        .filter(|(count, _)| *count == 1)
        .map(|(_, offset)| *offset)
        .collect();
    assert_eq!(
        copies,
        [Some(2), Some(3)],
        "the stored copy, then a second one once the window let its id go: {seen:?}"
    );
    let attempts = seen.iter().filter(|(count, _)| *count == 0).count();
    assert!(
        attempts >= 2,
        "the original is redelivered rather than acked while its copy cannot be confirmed: {seen:?}"
    );
    assert_eq!(stream_last_sequence(&client, "nats-hold").await, 3);
    assert_eq!(
        stream_messages(&client, "nats-hold-dlq").await,
        0,
        "nothing was dead-lettered"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

shove::define_topic!(
    LimitedTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-limited").dlq().build()
);

impl MessageHandler<LimitedTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// A durable whose limits refuse every pull the worker can make, here a
/// `max_batch` below the worker's prefetch, ends the consumer with an error
/// once its reconnect attempts are spent, rather than leaving it pulling and
/// pausing forever.
#[tokio::test]
async fn a_durable_that_refuses_every_pull_ends_the_consumer() {
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<LimitedTopic>().await.unwrap();
    client
        .jetstream()
        .get_stream("nats-limited")
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-limited-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: -1,
            max_batch: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<LimitedTopic, _>(
                CountingHandler::new(),
                (),
                ConsumerOptions::<Nats>::new()
                    .with_prefetch_count(2)
                    .with_max_reconnect_attempts(2),
            )
            .await
    });

    let ended = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the consumer ends once its reconnect attempts are spent")
        .expect("the consumer task does not panic");
    let error = ended.expect_err("a durable that refuses every pull is an error");
    assert!(
        error.to_string().contains("Exceeded MaxRequestBatch"),
        "the error names the limit: {error}"
    );
    broker.close().await;
}

/// A free host port, for a container whose port must survive a restart.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("read the bound address")
        .port()
}

/// An idle worker's pull is lost when the server is killed and restarts: a
/// killed server never answers its waiting pulls, as a stopping one does with
/// `409 Server Shutdown`. Once the client has reconnected, the worker pulls
/// again at once, so a message published after the restart is handled
/// straight away, well inside the 35 s the lost pull would otherwise hold the
/// worker's slot for.
#[tokio::test]
async fn a_worker_pulls_again_once_its_connection_comes_back() {
    use testcontainers::core::ContainerPort;

    let port = free_port();
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .with_mapped_port(port, ContainerPort::Tcp(4222))
        .start()
        .await
        .expect("failed to start NATS container");
    let host = container.get_host().await.expect("failed to get host");
    let client =
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
            .await
            .expect("failed to connect to NATS");
    let broker = Broker::<Nats>::from_client(client.clone());
    broker.topology().declare::<IdleTopic>().await.unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<IdleTopic, _>(
                hc,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });
    wait_for_open_pull(&client, "nats-idle", "nats-idle-consumer").await;
    let opened = Instant::now();

    // SIGKILL with no SIGTERM first: a stop, even a zero-timeout one, lets
    // the server answer its waiting pulls before it goes.
    let killed = tokio::process::Command::new("docker")
        .args(["kill", container.id()])
        .output()
        .await
        .expect("docker kill runs");
    assert!(
        killed.status.success(),
        "docker kill failed: {}",
        String::from_utf8_lossy(&killed.stderr)
    );
    container.start().await.expect("the server starts again");
    let deadline = Instant::now() + TIMEOUT;
    while !client.is_connected() {
        assert!(Instant::now() < deadline, "the client never reconnected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reconnected = opened.elapsed();

    let publisher = broker.publisher().await.unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while let Err(e) = publisher
        .publish::<IdleTopic>(&SimpleMessage {
            id: "after-the-restart".into(),
            content: String::new(),
        })
        .await
    {
        assert!(
            Instant::now() < deadline,
            "JetStream never took the publish: {e}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let published = Instant::now();
    assert!(
        handler.counter.wait_for(1, Duration::from_secs(10)).await,
        "the message published {reconnected:?} after the pull opened, once the client was back, \
         waited more than 10 s: the worker kept its slot on the lost pull"
    );
    assert!(
        published.elapsed() < Duration::from_secs(10),
        "the message was handled {:?} after its publish",
        published.elapsed()
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the consumer stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

// ===========================================================================
// DLQ drain
// ===========================================================================

/// Records the id of every dead letter it is handed. A letter called `boom`
/// makes `handle_dead` panic, and one called `hang` makes it never return.
#[derive(Clone)]
struct DeadLetterProbe {
    handled: Arc<Mutex<Vec<String>>>,
    counter: WaitableCounter,
}

impl DeadLetterProbe {
    fn new() -> Self {
        Self {
            handled: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    async fn handled(&self) -> Vec<String> {
        self.handled.lock().await.clone()
    }
}

impl MessageHandler<WorkTopic> for DeadLetterProbe {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
        self.handled.lock().await.push(msg.id.clone());
        self.counter.increment();
        match msg.id.as_str() {
            "boom" => panic!("handle_dead panics on purpose"),
            "hang" => std::future::pending::<()>().await,
            _ => {}
        }
    }
}

/// Puts a dead letter carrying `payload` straight into the DLQ stream of
/// [`WorkTopic`].
async fn publish_dead_letter(client: &NatsClient, payload: &[u8]) {
    raw_publish(
        client,
        "nats-work-dlq",
        async_nats::HeaderMap::new(),
        payload,
    )
    .await;
}

/// Runs a DLQ drain of [`WorkTopic`] on a task of its own.
fn spawn_dlq_drain(
    client: &NatsClient,
    probe: &DeadLetterProbe,
    options: ConsumerOptions<Nats>,
) -> tokio::task::JoinHandle<Result<(), shove::ShoveError>> {
    let consumer = NatsConsumer::new(client.clone());
    let probe = probe.clone();
    tokio::spawn(async move {
        consumer
            .run_dlq_with_options::<WorkTopic, _>(probe, (), options)
            .await
    })
}

/// Cancels `shutdown` and expects the drain to stop cleanly.
async fn stop_dlq_drain(
    shutdown: &CancellationToken,
    drain: tokio::task::JoinHandle<Result<(), shove::ShoveError>>,
) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("the drain stops in time")
        .expect("the drain task does not panic")
        .expect("the drain stops cleanly");
}

/// The drain gates dead letters at the `max_message_size` its options carry,
/// not at a fixed 10 MiB. An oversized letter is logged with its size and
/// acked without reaching `handle_dead`, and the letter behind it is handled.
#[tokio::test]
async fn dlq_drain_honours_max_message_size() {
    use tracing::Level;
    use tracing::subscriber::set_default;

    const LIMIT: usize = 100;

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    let _logging = set_default(subscriber);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let oversized = serde_json::to_vec(&SimpleMessage {
        id: "oversized".into(),
        content: "x".repeat(400),
    })
    .unwrap();
    let small = simple_payload("small");
    assert!(small.len() <= LIMIT && LIMIT < oversized.len());
    publish_dead_letter(&client, &oversized).await;
    publish_dead_letter(&client, &small).await;

    let probe = DeadLetterProbe::new();
    let shutdown = CancellationToken::new();
    let drain = spawn_dlq_drain(
        &client,
        &probe,
        ConsumerOptions::<Nats>::new()
            .with_shutdown(shutdown.clone())
            .with_max_message_size(LIMIT),
    );

    assert!(
        probe.counter.wait_for(1, TIMEOUT).await,
        "the small letter is handled"
    );
    assert_eq!(
        settle_stream_messages(&client, "nats-work-dlq", 0).await,
        0,
        "both letters are acked"
    );
    assert_eq!(
        probe.handled().await,
        ["small"],
        "the oversized letter never reaches handle_dead"
    );
    let warned = logs.contents();
    assert!(
        warned.contains(&format!("bytes={}", oversized.len())),
        "the oversized letter is logged with its size: {warned}"
    );

    stop_dlq_drain(&shutdown, drain).await;
    broker.close().await;
}

/// A panicking `handle_dead` neither ends the drain nor keeps its letter: the
/// panic is caught, the letter is acked, and the letter behind it is handled.
#[tokio::test]
async fn dlq_drain_survives_panicking_handle_dead() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    publish_dead_letter(&client, &simple_payload("boom")).await;
    publish_dead_letter(&client, &simple_payload("after")).await;

    let probe = DeadLetterProbe::new();
    let shutdown = CancellationToken::new();
    let drain = spawn_dlq_drain(
        &client,
        &probe,
        ConsumerOptions::<Nats>::new().with_shutdown(shutdown.clone()),
    );

    assert!(
        probe.counter.wait_for(2, TIMEOUT).await,
        "the letter behind the panicking one is handled: {:?}",
        probe.handled().await
    );
    assert_eq!(probe.handled().await, ["boom", "after"]);
    assert_eq!(
        settle_stream_messages(&client, "nats-work-dlq", 0).await,
        0,
        "the panicking letter is acked too"
    );
    assert!(!drain.is_finished(), "the drain outlives the panic");

    stop_dlq_drain(&shutdown, drain).await;
    broker.close().await;
}

/// A `handle_dead` that never returns holds the drain for the handler timeout
/// at most: its letter is acked and the letter behind it is handled.
#[tokio::test]
async fn dlq_drain_bounds_handle_dead_by_the_handler_timeout() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    publish_dead_letter(&client, &simple_payload("hang")).await;
    publish_dead_letter(&client, &simple_payload("after")).await;

    let probe = DeadLetterProbe::new();
    let shutdown = CancellationToken::new();
    let drain = spawn_dlq_drain(
        &client,
        &probe,
        ConsumerOptions::<Nats>::new()
            .with_shutdown(shutdown.clone())
            .with_handler_timeout(Duration::from_millis(500)),
    );

    assert!(
        probe.counter.wait_for(2, TIMEOUT).await,
        "the letter behind the hanging one is handled: {:?}",
        probe.handled().await
    );
    assert_eq!(probe.handled().await, ["hang", "after"]);
    assert_eq!(
        settle_stream_messages(&client, "nats-work-dlq", 0).await,
        0,
        "the letter that timed out is acked too"
    );

    stop_dlq_drain(&shutdown, drain).await;
    broker.close().await;
}

/// The drain ends when the token of its options fires, with the broker still
/// open.
#[tokio::test]
async fn dlq_drain_stops_on_options_shutdown() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let probe = DeadLetterProbe::new();
    let shutdown = CancellationToken::new();
    let drain = spawn_dlq_drain(
        &client,
        &probe,
        ConsumerOptions::<Nats>::new().with_shutdown(shutdown.clone()),
    );
    wait_for_open_pull(&client, "nats-work-dlq", "nats-work-dlq-consumer").await;

    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(10), drain).await;
    assert!(
        !client.shutdown_token().is_cancelled(),
        "the broker is still open"
    );
    stopped
        .expect("the drain stops when the token of its options fires")
        .expect("the drain task does not panic")
        .expect("the drain stops cleanly");
    broker.close().await;
}

/// `with_broadcast_start` is read by a broadcast subscription alone, so the
/// drain refuses it instead of dropping it silently.
#[tokio::test]
async fn dlq_drain_refuses_a_broadcast_start() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let err = NatsConsumer::new(tb.client())
        .run_dlq_with_options::<WorkTopic, _>(
            DeadLetterProbe::new(),
            (),
            ConsumerOptions::<Nats>::new().with_broadcast_start(shove::BroadcastStart::Head),
        )
        .await
        .expect_err("a broadcast start is refused");
    let shove::ShoveError::Topology(message) = err else {
        panic!("expected ShoveError::Topology, got {err:?}");
    };
    assert!(message.contains("NatsConsumer::run_dlq"), "{message}");
    broker.close().await;
}

// ===========================================================================
// Exit drain
// ===========================================================================

shove::define_topic!(
    ExitDrainTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-exit-drain").build()
);

/// Starts, works for `work`, then finishes, so a test can tell from the two
/// counters where in a handler a consumer returned.
#[derive(Clone)]
struct SlowToFinish {
    work: Duration,
    started: WaitableCounter,
    finished: WaitableCounter,
}

impl SlowToFinish {
    fn new(work: Duration) -> Self {
        Self {
            work,
            started: WaitableCounter::new(),
            finished: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<ExitDrainTopic> for SlowToFinish {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.started.increment();
        tokio::time::sleep(self.work).await;
        self.finished.increment();
        Outcome::Ack
    }
}

/// Starts a consumer of [`ExitDrainTopic`] on `handler`, publishes one
/// message, and returns once the handler is running on it.
async fn consume_one_slow_message(
    tb: &TestBroker,
    handler: &SlowToFinish,
    options: ConsumerOptions<Nats>,
) -> tokio::task::JoinHandle<Result<(), shove::ShoveError>> {
    let broker = tb.broker();
    broker.topology().declare::<ExitDrainTopic>().await.unwrap();
    let consumer = NatsConsumer::new(tb.client());
    let hc = handler.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExitDrainTopic, _>(hc, (), options.with_prefetch_count(2))
            .await
    });
    wait_for_open_pull(&tb.client(), "nats-exit-drain", "nats-exit-drain-consumer").await;
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<ExitDrainTopic>(&SimpleMessage {
            id: "slow".into(),
            content: String::new(),
        })
        .await
        .unwrap();
    assert!(
        handler.started.wait_for(1, TIMEOUT).await,
        "the handler is running"
    );
    handle
}

/// Ends the open pull of the consumer of [`ExitDrainTopic`] with a status the
/// worker counts as a connection error, by deleting the durable it pulls from.
async fn delete_exit_drain_durable(client: &NatsClient) {
    client
        .jetstream()
        .get_stream("nats-exit-drain")
        .await
        .expect("the stream exists")
        .delete_consumer("nats-exit-drain-consumer")
        .await
        .expect("the durable is deleted");
}

/// A consumer that leaves on a stream error does not leave its handlers
/// behind. The error spends the only reconnect attempt, and the consumer
/// returns it only once the handler that was running has finished.
#[tokio::test]
async fn stream_error_exit_waits_for_in_flight_handlers() {
    let tb = TestBroker::start().await;
    let handler = SlowToFinish::new(Duration::from_secs(3));
    let handle = consume_one_slow_message(
        &tb,
        &handler,
        ConsumerOptions::<Nats>::new().with_max_reconnect_attempts(1),
    )
    .await;

    delete_exit_drain_durable(&tb.client()).await;
    let ended = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the consumer exits on the stream error")
        .expect("the consumer task does not panic");

    assert!(
        ended.is_err(),
        "the stream error is what ends the consumer: {ended:?}"
    );
    assert_eq!(
        handler.finished.get(),
        1,
        "the consumer returned while its handler was still running"
    );
    tb.broker().close().await;
}

/// A shutdown that arrives while the consumer waits to reconnect after a
/// stream error ends it as a stop would: only once the handlers it started in
/// the cycle that failed have finished.
#[tokio::test]
async fn shutdown_during_reconnect_backoff_waits_for_in_flight_handlers() {
    use tracing::Level;
    use tracing::subscriber::set_default;

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    let _logging = set_default(subscriber);

    let tb = TestBroker::start().await;
    let handler = SlowToFinish::new(Duration::from_secs(3));
    let shutdown = CancellationToken::new();
    let handle = consume_one_slow_message(
        &tb,
        &handler,
        ConsumerOptions::<Nats>::new().with_shutdown(shutdown.clone()),
    )
    .await;

    delete_exit_drain_durable(&tb.client()).await;
    let deadline = Instant::now() + TIMEOUT;
    while !logs.contents().contains("consumer error, reconnecting") {
        assert!(
            Instant::now() < deadline,
            "the stream error never sent the consumer to reconnect: {}",
            logs.contents()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    let ended = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the consumer exits on the shutdown")
        .expect("the consumer task does not panic");

    assert!(
        ended.is_ok(),
        "a shutdown ends the consumer cleanly: {ended:?}"
    );
    assert_eq!(
        handler.finished.get(),
        1,
        "the consumer returned while its handler was still running"
    );
    tb.broker().close().await;
}

// ===========================================================================
// FIFO group abort
// ===========================================================================

shove::define_sequenced_topic!(
    SeqAbortTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("nats-fifo-abort")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

/// Starts, works for `work`, then finishes and acks.
#[derive(Clone)]
struct SlowFifoHandler {
    work: Duration,
    started: WaitableCounter,
    finished: WaitableCounter,
}

impl MessageHandler<SeqAbortTopic> for SlowFifoHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.started.increment();
        tokio::time::sleep(self.work).await;
        self.finished.increment();
        Outcome::Ack
    }
}

/// A group whose drain times out aborts its shard tasks with it, and a shard
/// takes the handler call it is awaiting along. The handler never finishes,
/// and nothing settles its message after the timeout was reported: the message
/// stays in the stream, un-acked.
#[tokio::test]
async fn fifo_group_drain_timeout_stops_shards() {
    const WORK: Duration = Duration::from_secs(3);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqAbortTopic>().await.unwrap();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SeqAbortTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    let handler = SlowFifoHandler {
        work: WORK,
        started: WaitableCounter::new(),
        finished: WaitableCounter::new(),
    };
    let mut group = broker.consumer_group();
    group
        .register_fifo::<SeqAbortTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::default()),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    let signal = {
        let started = handler.started.clone();
        async move {
            started.wait_for(1, Duration::from_secs(30)).await;
        }
    };
    let outcome = group
        .run_until_timeout(signal, Duration::from_millis(500))
        .await;
    assert!(
        outcome.timed_out,
        "the handler outlasts the drain: {outcome:?}"
    );

    // Past the point where the handler would have returned, and a shard that
    // survived the abort would have acked the message.
    tokio::time::sleep(WORK + Duration::from_secs(1)).await;
    assert_eq!(
        handler.finished.get(),
        0,
        "the handler ran on after its shard was aborted"
    );
    assert_eq!(
        stream_messages(&client, "nats-fifo-abort").await,
        1,
        "a shard settled its message after the drain timeout was reported"
    );
    broker.close().await;
}

/// Dropping a direct `run_fifo` call takes its shard tasks down with it, and
/// the handler call each is awaiting. The handler never finishes and nothing
/// settles its message: the message stays in the stream, un-acked.
#[tokio::test]
async fn run_fifo_stops_its_shards_when_dropped() {
    const WORK: Duration = Duration::from_secs(3);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqAbortTopic>().await.unwrap();
    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SeqAbortTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    let handler = SlowFifoHandler {
        work: WORK,
        started: WaitableCounter::new(),
        finished: WaitableCounter::new(),
    };
    let consumer = NatsConsumer::new(client.clone());
    let run = {
        let handler = handler.clone();
        tokio::spawn(async move {
            consumer
                .run_fifo::<SeqAbortTopic, _>(handler, (), ConsumerOptions::<Nats>::new())
                .await
        })
    };
    assert!(
        handler.started.wait_for(1, TIMEOUT).await,
        "the handler is running"
    );

    run.abort();
    assert!(
        run.await
            .expect_err("the aborted call is cancelled")
            .is_cancelled(),
        "the call ended some other way"
    );

    // Past the point where the handler would have returned, and a shard that
    // survived the abort would have acked the message.
    tokio::time::sleep(WORK + Duration::from_secs(1)).await;
    assert_eq!(
        handler.finished.get(),
        0,
        "the handler ran on after its shard was dropped"
    );
    assert_eq!(
        stream_messages(&client, "nats-fifo-abort").await,
        1,
        "a shard settled its message after its run_fifo call was dropped"
    );
    broker.close().await;
}
