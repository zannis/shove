//! Integration tests for the NATS backend.
//!
//! Migrated to `Broker<Nats>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `NatsConsumer` constructed from
//! the underlying `NatsClient`.

#![cfg(feature = "nats")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer, NatsConsumerGroupConfig};
use shove::outcome::Outcome;
use shove::topology::{SequenceFailure, TopologyBuilder};
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
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(1))
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
            *self.0.lock().await = meta.headers;
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
            .run::<WorkTopic>(handler, ConsumerOptions::new(sc).with_prefetch_count(1))
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
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(10))
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
            .run::<WorkTopic>(
                FixedOutcomeHandler(Outcome::Reject),
                ConsumerOptions::new(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Verify message arrives in DLQ via a DLQ consumer
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle = tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic>(dhc).await });

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
        c1.run::<WorkTopic>(
            FixedOutcomeHandler(Outcome::Reject),
            ConsumerOptions::new(sc1).with_prefetch_count(1),
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
    let h2 = tokio::spawn(async move { c2.run_dlq::<WorkTopic>(dhc).await });

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
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
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
            .run::<WorkTopic>(
                FixedOutcomeHandler(Outcome::Retry),
                ConsumerOptions::new(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle = tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic>(dhc).await });

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
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
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
    publisher.publish_batch::<WorkTopic>(&messages).await.unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = NatsConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(10))
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
            .run::<WorkTopic>(handler, ConsumerOptions::new(sc).with_prefetch_count(10))
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
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(1))
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
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
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
            .run_fifo::<SeqSkipTopic>(hc, ConsumerOptions::new(sc).with_max_retries(5))
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
            .run_fifo::<SeqSkipTopic>(handler, ConsumerOptions::new(sc).with_max_retries(5))
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
            .run_fifo::<SeqSkipTopic>(hc, ConsumerOptions::new(sc).with_max_retries(5))
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
    publisher.publish_batch::<WorkTopic>(&messages).await.unwrap();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let config = NatsConsumerGroupConfig::new(2..=2)
        .with_prefetch_count(5)
        .with_max_retries(5);

    let mut group = broker.consumer_group();
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(config),
            move || handler_clone.clone(),
        )
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
    headers.insert(async_nats::header::NATS_MESSAGE_ID, "bad-json-1");
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
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(1))
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
        .run::<UndeclaredTopic>(Noop, ConsumerOptions::new(shutdown))
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

    let result = consumer.run_dlq::<NoDlqTopic>(Noop).await;
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
    broker.topology().declare::<DeferNoHoldTopic>().await.unwrap();

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
            .run::<DeferNoHoldTopic>(handler, ConsumerOptions::new(sc).with_prefetch_count(1))
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
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
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
            .run::<WorkTopic>(hc, ConsumerOptions::new(sc).with_prefetch_count(10))
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
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
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
            .run_fifo::<SeqSkipTopic>(handler, ConsumerOptions::new(sc).with_max_retries(5))
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
