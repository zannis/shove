#![cfg(feature = "nats")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::consumer::{Consumer, ConsumerOptions};
use shove::handler::MessageHandler;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{
    NatsClient, NatsConfig, NatsConsumer, NatsConsumerGroup, NatsConsumerGroupConfig,
    NatsPublisher, NatsTopologyDeclarer,
};
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::topology::{SequenceFailure, TopologyBuilder, TopologyDeclarer};
use shove::SequencedTopic as _;
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

shove::define_topic!(
    DeferNoHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("nats-defer-nohold").dlq().build()
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn connect() -> NatsClient {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    NatsClient::connect(&NatsConfig::new(url))
        .await
        .expect("failed to connect to NATS")
}

async fn declare<T: shove::topic::Topic>(client: &NatsClient) {
    NatsTopologyDeclarer::new(client.clone())
        .declare(T::topology())
        .await
        .expect("topology declaration should succeed");
}

async fn make_publisher(client: &NatsClient) -> NatsPublisher {
    NatsPublisher::new(client.clone()).await.expect("publisher creation should succeed")
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
        Self { counter: WaitableCounter::new() }
    }
}

impl MessageHandler<WorkTopic> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<NoDlqTopic> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<DeferNoHoldTopic> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

struct FixedOutcomeHandler(Outcome);

impl MessageHandler<WorkTopic> for FixedOutcomeHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
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
        Self { retry_until, counter: WaitableCounter::new() }
    }
}

impl MessageHandler<WorkTopic> for RetryThenAckHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
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
        Self { delay, counter: WaitableCounter::new() }
    }
}

impl MessageHandler<WorkTopic> for SlowHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
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
        Self { counter: WaitableCounter::new() }
    }
}

impl MessageHandler<WorkTopic> for DlqRecordingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata) {
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
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
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
    let client = connect().await;

    assert!(
        !client.shutdown_token().is_cancelled(),
        "shutdown token should not be cancelled before shutdown"
    );

    client.shutdown().await;

    assert!(
        client.shutdown_token().is_cancelled(),
        "shutdown token should be cancelled after shutdown"
    );
}

#[tokio::test]
async fn client_shutdown_cancels_token() {
    let client = connect().await;
    let token = client.shutdown_token();
    assert!(!token.is_cancelled());

    client.shutdown().await;
    assert!(token.is_cancelled());
}

// ===========================================================================
// Topology declaration
// ===========================================================================

#[tokio::test]
async fn topology_declares_standard_stream_and_dlq() {
    let client = connect().await;
    declare::<WorkTopic>(&client).await;

    // Verify main stream exists
    let stream = client.jetstream().get_stream("nats-work").await;
    assert!(stream.is_ok(), "main stream should exist");

    // Verify DLQ stream exists
    let dlq_stream = client.jetstream().get_stream("nats-work-dlq").await;
    assert!(dlq_stream.is_ok(), "DLQ stream should exist");

    client.shutdown().await;
}

#[tokio::test]
async fn topology_declares_sequenced_stream_with_shards() {
    let client = connect().await;
    declare::<SeqSkipTopic>(&client).await;

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

    client.shutdown().await;
}

#[tokio::test]
async fn topology_idempotent() {
    let client = connect().await;

    declare::<WorkTopic>(&client).await;
    declare::<WorkTopic>(&client).await; // second call should not fail

    let stream = client.jetstream().get_stream("nats-work").await;
    assert!(stream.is_ok(), "stream should still exist after double declare");

    client.shutdown().await;
}

// ===========================================================================
// Basic publish & consume
// ===========================================================================

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let client = connect().await;
    declare::<WorkTopic>(&client).await;

    let pub_ = make_publisher(&client).await;
    pub_
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
        consumer.run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(1)).await
    });

    assert!(handler.counter.wait_for(1, TIMEOUT).await, "should receive 1 message");

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn publish_and_consume_with_headers() {
    #[derive(Clone)]
    struct HeaderCapture(Arc<Mutex<HashMap<String, String>>>);

    impl MessageHandler<WorkTopic> for HeaderCapture {
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata) -> Outcome {
            *self.0.lock().await = meta.headers;
            Outcome::Ack
        }
    }

    let client = connect().await;
    declare::<WorkTopic>(&client).await;

    let pub_ = make_publisher(&client).await;
    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-abc-123".to_string());

    pub_
        .publish_with_headers::<WorkTopic>(
            &SimpleMessage { id: "hdr-1".into(), content: "with headers".into() },
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
        consumer.run::<WorkTopic>(handler, ConsumerOptions::new(sc).with_prefetch_count(1)).await
    });

    let result = tokio::time::timeout(TIMEOUT, async {
        loop {
            let map = captured.lock().await;
            if !map.is_empty() { return map.clone(); }
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
    client.shutdown().await;
}

#[tokio::test]
async fn publish_and_consume_batch() {
    let client = connect().await;
    declare::<WorkTopic>(&client).await;

    let pub_ = make_publisher(&client).await;
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("batch-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    pub_
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer.run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(10)).await
    });

    assert!(handler.counter.wait_for(5, TIMEOUT).await, "should receive all 5 messages");

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 5);
    client.shutdown().await;
}
