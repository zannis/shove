//! Integration tests for the Kafka backend.
//!
//! Migrated to `Broker<Kafka>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `KafkaConsumer` constructed from
//! the underlying `KafkaClient`.

#![cfg(feature = "kafka")]

use rdkafka::Message;
use serde::{Deserialize, Serialize};
use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig, KafkaTopologyDeclarer,
};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topic::Topic as _;
use shove::topology::{SequenceFailure, TopologyBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
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
    TopologyBuilder::new("kafka-work")
        .dlq()
        .hold_queue(Duration::from_millis(200))
        .hold_queue(Duration::from_millis(500))
        .build()
);

shove::define_topic!(
    NoDlqTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-nodlq").build()
);

shove::define_topic!(
    DeferNoHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-defer-nohold").dlq().build()
);

shove::define_sequenced_topic!(
    SeqSkipTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("kafka-seq-skip")
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
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    client: KafkaClient,
}

impl TestBroker {
    async fn start() -> Self {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        let bootstrap_servers = format!("127.0.0.1:{port}");

        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
            .await
            .expect("failed to connect to Kafka");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Kafka> {
        Broker::<Kafka>::from_client(self.client.clone())
    }

    fn client(&self) -> KafkaClient {
        self.client.clone()
    }
}

const TIMEOUT: Duration = Duration::from_secs(30);

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
async fn topology_declares_standard_topic_and_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    // If we got here without error, topology was declared successfully.
    broker.close().await;
}

#[tokio::test]
async fn topology_declares_sequenced_topic_with_partitions() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn topology_idempotent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    broker.topology().declare::<WorkTopic>().await.unwrap();
    broker.topology().declare::<WorkTopic>().await.unwrap(); // second call should not fail

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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Reject),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Verify message arrives in DLQ via a DLQ consumer
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
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
    let c1 = KafkaConsumer::new(client.clone());
    let h1 = tokio::spawn(async move {
        c1.run::<WorkTopic, _>(
            FixedOutcomeHandler(Outcome::Reject),
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sc1)
                .with_prefetch_count(1),
        )
        .await
    });

    tokio::time::sleep(Duration::from_secs(10)).await;
    shutdown1.cancel();
    h1.await.unwrap().ok();

    // Step 2: consume from DLQ
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let c2 = KafkaConsumer::new(client.clone());
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Retry),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler
            .counter
            .wait_for(1, Duration::from_secs(60))
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(10, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
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

    let handler = SlowHandler::new(Duration::from_secs(2));
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let options = ConsumerOptions::<Kafka>::new()
        .with_shutdown(sc)
        .with_prefetch_count(1);
    let processing_flag = options.processing_handle();
    let handle = tokio::spawn(async move { consumer.run::<WorkTopic, _>(hc, (), options).await });

    // Wait until the handler is actively processing (Kafka consumers take
    // a few seconds for group join + rebalance before they receive messages).
    let started = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if processing_flag.load(Ordering::Acquire) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        started.is_ok(),
        "handler should start processing within timeout"
    );

    // Now cancel while the handler is still sleeping.
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(10), handle).await;
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1)
                    .with_handler_timeout(Duration::from_millis(500)),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(5, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(6, Duration::from_secs(60)).await,
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
// Consumer group (via Broker<Kafka> generic wrapper)
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

    let config = KafkaConsumerGroupConfig::new(2..=2)
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
// Edge cases
// ===========================================================================

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
    let consumer = KafkaConsumer::new(client.clone());

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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<DeferNoHoldTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(10)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
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
// Deserialization failure
// ===========================================================================

#[tokio::test]
async fn deserialization_failure_rejects_to_dlq() {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Publish raw invalid JSON directly via rdkafka producer
    use rdkafka::producer::FutureRecord;
    let record: FutureRecord<str, [u8]> =
        FutureRecord::to("kafka-work").payload(b"not valid json" as &[u8]);
    client
        .producer()
        .send(record, Duration::from_secs(5))
        .await
        .expect("raw publish should succeed");

    // Start consumer — should reject the bad message to DLQ
    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Verify the message lands in the DLQ topic by consuming raw bytes.
    // The DLQ payload is un-deserializable (that's why it was rejected),
    // so we read directly with a BaseConsumer instead of run_dlq.
    let dlq_topic = WorkTopic::topology().dlq().expect("WorkTopic has a DLQ");
    let brokers = client.brokers().to_string();
    let received = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "test-dlq-verify")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("DLQ verify consumer");
        consumer
            .subscribe(&[dlq_topic])
            .expect("subscribe to DLQ topic");

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if std::time::Instant::now() > deadline {
                return None;
            }
            if let Some(result) = consumer.poll(Duration::from_secs(1)) {
                let msg = result.expect("DLQ message");
                let payload = msg.payload().unwrap_or_default().to_vec();
                return Some(payload);
            }
        }
    })
    .await
    .expect("spawn_blocking join");

    let payload = received.expect("malformed message should land in DLQ");
    assert_eq!(
        payload, b"not valid json",
        "DLQ should contain original payload"
    );
    assert_eq!(
        handler.counter.get(),
        0,
        "handler should not be called for bad JSON"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
}

// ===========================================================================
// Lag stats provider
// ===========================================================================

#[tokio::test]
async fn lag_stats_provider_reports_pending_messages() {
    use shove::kafka::{KafkaLagStatsProvider, KafkaQueueStatsProvider};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("lag-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let stats: shove::kafka::KafkaQueueStats = stats_provider
        .get_queue_stats("kafka-work")
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
async fn lag_stats_provider_reports_zero_after_consumption() {
    use shove::kafka::{KafkaLagStatsProvider, KafkaQueueStatsProvider};

    shove::define_topic!(
        LagTestTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-lag-test").dlq().build()
    );

    impl MessageHandler<LagTestTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<LagTestTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<LagTestTopic>(&SimpleMessage {
                id: format!("lag-zero-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<LagTestTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "should consume all 3 messages"
    );

    // Shut down the consumer so it performs its final offset commit.
    shutdown.cancel();
    handle.await.unwrap().ok();

    // Poll until committed offsets are visible (async commit may take a moment).
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats: shove::kafka::KafkaQueueStats = stats_provider
            .get_queue_stats("kafka-lag-test")
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    broker.close().await;
}

// ===========================================================================
// Partition expansion
// ===========================================================================

#[tokio::test]
async fn topology_expands_partitions_on_redeclare() {
    shove::define_topic!(
        ExpandTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-expand-test").build()
    );

    impl MessageHandler<ExpandTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ExpandTopic>().await.unwrap();

    // Re-declare with higher min_partitions to trigger ensure_partitions
    let declarer = KafkaTopologyDeclarer::new(client.clone()).with_min_partitions(16);
    declarer
        .declare(ExpandTopic::topology())
        .await
        .expect("re-declaring with more partitions should succeed");

    // Verify by publishing and consuming
    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExpandTopic>(&SimpleMessage {
            id: "expand-1".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExpandTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "should receive message after partition expansion"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// DLQ consumer edge cases
// ===========================================================================

#[tokio::test]
async fn dlq_consumer_handles_deserialization_failure() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Publish raw invalid JSON directly to the DLQ topic
    use rdkafka::producer::FutureRecord;
    let record: FutureRecord<str, [u8]> =
        FutureRecord::to("kafka-work-dlq").payload(b"not valid json" as &[u8]);
    client
        .producer()
        .send(record, Duration::from_secs(5))
        .await
        .expect("raw publish to DLQ should succeed");

    // Also publish a valid message to DLQ
    let valid_payload = serde_json::to_vec(&SimpleMessage {
        id: "valid-dlq".into(),
        content: "valid".into(),
    })
    .unwrap();
    let record2: FutureRecord<str, [u8]> =
        FutureRecord::to("kafka-work-dlq").payload(&valid_payload);
    client
        .producer()
        .send(record2, Duration::from_secs(5))
        .await
        .expect("valid publish to DLQ should succeed");

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    // The valid message should still be processed
    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ consumer should process valid messages even after a deserialization failure"
    );

    broker.close().await;
    dlq_handle.await.unwrap().ok();
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
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
                Outcome::Defer
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
            order_id: "defer-fifo-key".into(),
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

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should be called at least 2 times"
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
