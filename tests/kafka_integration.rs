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
    KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig, KafkaQueueStats,
    KafkaTopologyDeclarer,
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

shove::define_topic!(
    RetentionTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-retention")
        .with_topic_config("retention.ms", "3600000")
        .build()
);

shove::define_topic!(
    RetentionOverrideTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-retention-override")
        .with_topic_config("retention.ms", "1800000")
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

/// Read a live topic config value straight from the broker via a raw admin
/// client, bypassing shove.
async fn live_topic_config(brokers: &str, topic: &str, key: &str) -> Option<String> {
    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    let specifier = ResourceSpecifier::Topic(topic);
    let resources = admin
        .describe_configs([&specifier], &AdminOptions::new())
        .await
        .expect("describe_configs failed");
    let resource = resources
        .into_iter()
        .next()
        .expect("no resource returned")
        .expect("describe_configs returned an error for the topic");
    resource.entry_map().get(key).and_then(|e| e.value.clone())
}

/// Poll [`live_topic_config`] until the key reads back as `expected` or the
/// timeout elapses, returning the last observed value. Config changes commit
/// on the controller before brokers apply them to their local metadata
/// snapshot, so a describe issued immediately after a create/alter can be
/// stale.
async fn wait_for_topic_config(
    brokers: &str,
    topic: &str,
    key: &str,
    expected: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let value = live_topic_config(brokers, topic, key).await;
        if value.as_deref() == Some(expected) || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

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
async fn topology_declares_standard_topic_and_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    // If we got here without error, topology was declared successfully.
    broker.close().await;
}

#[tokio::test]
async fn topology_sets_topic_config_on_create() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RetentionTopic>().await.unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention",
        "retention.ms",
        "3600000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("3600000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_reconciles_config_on_existing_topic() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    // First declare WITHOUT any config (declarer-level knob only, so the
    // same Topic type can be re-declared with a different desired value).
    broker.topology().declare::<WorkTopic>().await.unwrap();
    // Redeclare with a declarer-level retention (via the named helper) —
    // topic already exists, so this exercises the describe → drift → alter
    // path.
    broker
        .topology()
        .with_retention(Duration::from_secs(7200))
        .declare::<WorkTopic>()
        .await
        .unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-work",
        "retention.ms",
        "7200000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("7200000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_reconcile_preserves_unrelated_dynamic_config() {
    use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Out-of-band: set an unrelated dynamic topic config, as infra might.
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", tb.client().brokers())
        .create()
        .expect("failed to create admin client");
    let alter =
        AlterConfig::new(ResourceSpecifier::Topic("kafka-work")).set("segment.bytes", "123456789");
    for r in admin
        .alter_configs([&alter], &AdminOptions::new())
        .await
        .expect("alter_configs failed")
    {
        r.expect("alter_configs returned an error");
    }
    // Wait for the out-of-band change to propagate to describe_configs —
    // shove's reconcile reads the live config, so it can only preserve
    // entries that are visible when it runs.
    let visible = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-work",
        "segment.bytes",
        "123456789",
        TIMEOUT,
    )
    .await;
    assert_eq!(visible.as_deref(), Some("123456789"));

    // Reconcile retention via shove — segment.bytes must survive the
    // legacy-AlterConfigs merge.
    broker
        .topology()
        .with_topic_config("retention.ms", "7200000")
        .declare::<WorkTopic>()
        .await
        .unwrap();

    let brokers = tb.client().brokers().to_string();
    let retention =
        wait_for_topic_config(&brokers, "kafka-work", "retention.ms", "7200000", TIMEOUT).await;
    let segment = wait_for_topic_config(
        &brokers,
        "kafka-work",
        "segment.bytes",
        "123456789",
        TIMEOUT,
    )
    .await;
    assert_eq!(retention.as_deref(), Some("7200000"));
    assert_eq!(segment.as_deref(), Some("123456789"));
    broker.close().await;
}

#[tokio::test]
async fn topology_config_redeclare_is_idempotent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RetentionTopic>().await.unwrap();
    // Second declare with identical config: no drift, must succeed quietly.
    broker.topology().declare::<RetentionTopic>().await.unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention",
        "retention.ms",
        "3600000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("3600000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_per_topic_config_overrides_declarer_default() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    // Declarer says 9999999, the topic's builder says 1800000 — builder wins.
    broker
        .topology()
        .with_topic_config("retention.ms", "9999999")
        .declare::<RetentionOverrideTopic>()
        .await
        .unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention-override",
        "retention.ms",
        "1800000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("1800000"));
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
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    // 1 initial + 2 retries = 3 attempts, then DLQ.
    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
        "should reach 3 attempts"
    );
    // Allow any erroneous 4th redelivery to surface before asserting.
    tokio::time::sleep(Duration::from_secs(2)).await;
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

// Registry default handler timeout reaches a registered handler when the
// per-group config does NOT call `with_handler_timeout`. Mirrors the
// raw-consumer `handler_timeout_triggers_retry` test but exercises the
// registry pre-resolution path.
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
                KafkaConsumerGroupConfig::new(1..=1)
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
        if counter.wait_for(2, TIMEOUT).await {
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

    // Publish raw invalid JSON directly via the client routing method
    use rdkafka::message::OwnedHeaders;
    client
        .publish_with_retry(
            "kafka-work",
            None,
            OwnedHeaders::new(),
            b"not valid json",
            1,
            "test raw publish",
        )
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
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

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
    let stats: KafkaQueueStats = stats_provider
        .get_queue_stats(
            "kafka-work",
            "kafka-work-consumer",
            KafkaAutoOffsetReset::Earliest,
        )
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
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

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
        let stats: KafkaQueueStats = stats_provider
            .get_queue_stats(
                "kafka-lag-test",
                "kafka-lag-test-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
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

#[tokio::test]
async fn committed_offsets_advance_while_consumer_is_idle() {
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

    shove::define_topic!(
        IdleCommitTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-idle-commit").dlq().build()
    );

    impl MessageHandler<IdleCommitTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<IdleCommitTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<IdleCommitTopic>(&SimpleMessage {
                id: format!("idle-commit-{i}"),
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
            .run::<IdleCommitTopic, _>(
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

    // The consumer stays running and no further messages arrive. Handler
    // completions alone must drive the offset commits — a crash or rebalance
    // in this state must not redeliver the already-processed batch.
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats: KafkaQueueStats = stats_provider
            .get_queue_stats(
                "kafka-idle-commit",
                "kafka-idle-commit-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "offsets must be committed while the consumer is idle (no new \
             traffic, no shutdown), still got lag {}",
            stats.messages_pending
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    shutdown.cancel();
    handle.await.unwrap().ok();
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
    use rdkafka::message::OwnedHeaders;
    client
        .publish_with_retry(
            "kafka-work-dlq",
            None,
            OwnedHeaders::new(),
            b"not valid json",
            1,
            "test raw publish to DLQ",
        )
        .await
        .expect("raw publish to DLQ should succeed");

    // Also publish a valid message to DLQ
    let valid_payload = serde_json::to_vec(&SimpleMessage {
        id: "valid-dlq".into(),
        content: "valid".into(),
    })
    .unwrap();
    client
        .publish_with_retry(
            "kafka-work-dlq",
            None,
            OwnedHeaders::new(),
            &valid_payload,
            1,
            "test valid publish to DLQ",
        )
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

    let consumer = KafkaConsumer::new(client.clone());

    // Wait until the handler has acked all 5, then send the signal.
    let signal = async move {
        let _ = counter_clone.wait_for(5, Duration::from_secs(30)).await;
        // Brief grace period to let the shard finish its commit round trip.
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(
            CountSeqHandler(counter.clone()),
            (),
            ConsumerOptions::<Kafka>::new().with_max_retries(3),
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
    // Shard-level panic boundary: the Kafka FIFO consume loop spawns each
    // handler via `tokio::spawn` and awaits it through a oneshot channel.
    // When the handler panics, the task panics before sending, the receiver
    // closes, and the consume loop treats the closed channel as `Outcome::Retry`.
    // The panic never escapes `run_with_reconnect` — it always returns `Ok(())`.
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

    let consumer = KafkaConsumer::new(client.clone());
    // max_retries=1 so the message is dispatched once (retry_count 0 < 1),
    // panics (Retry), then redelivered with retry_count=1 >= max_retries -> DLQ.
    let opts = ConsumerOptions::<Kafka>::new().with_max_retries(1);

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
        "Kafka shards absorb handler panics internally; got {outcome:?}"
    );
    assert_eq!(
        outcome.errors, 0,
        "Kafka shards absorb handler panics as Retry; got {outcome:?}"
    );

    broker.close().await;
}

#[tokio::test]
async fn run_fifo_until_timeout_flags_timeout_when_drain_overruns() {
    // Kafka's consume loop processes messages sequentially: it awaits the
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

    let consumer = KafkaConsumer::new(client.clone());
    // max_retries=1 so the initial delivery (retry_count=0) passes the
    // max-retries check and the handler is actually dispatched.
    // Disable the handler timeout so the 60 s sleep is not interrupted before
    // the drain budget expires.
    let opts = ConsumerOptions::<Kafka>::new()
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
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::default()),
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
// Autoscaling vertical slice
// ===========================================================================

shove::define_topic!(
    AutoscalingTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-autoscaling").build()
);

/// Autoscaling lifecycle: slow handlers + burst → `enable_autoscaling` →
/// clean drain. Mirrors `autoscaling_scales_up_under_backlog_then_drains_clean`
/// from `inmemory_integration.rs` but exercises the Kafka backend's
/// `spawn_autoscaler` and `KafkaConsumerGroup::retiring` drain path.
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
                    KafkaConsumerGroupConfig::new(1..=4).with_prefetch_count(1),
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
// Configurable consumer group_id — independent (fan-out) consumption
//
// Two independent services consuming the same topic must each receive every
// message. Each test drains once on the default group (committing offsets via
// `enable.auto.commit=false` manual commits), then drains again under an
// overridden group id which must rejoin a *fresh* group and re-receive all N
// — proving the override produces a distinct broker-side group rather than
// re-joining the committed default group.
// ===========================================================================

#[tokio::test]
async fn standard_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 5;
    for i in 0..N {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("msg-{i}"),
                content: "x".into(),
            })
            .await
            .unwrap();
    }

    // Consumer 1: default group `{queue}-consumer`, drain + commit.
    let h1 = CountingHandler::new();
    let h1c = h1.clone();
    let sd1 = CancellationToken::new();
    let sd1c = sd1.clone();
    let c1 = KafkaConsumer::new(client.clone());
    let j1 = tokio::spawn(async move {
        c1.run::<WorkTopic, _>(
            h1c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd1c)
                .with_prefetch_count(1),
        )
        .await
    });
    assert!(
        h1.counter.wait_for(N, TIMEOUT).await,
        "default consumer should receive all {N}"
    );
    // Let manual offset commits flush before tearing the consumer down.
    tokio::time::sleep(Duration::from_secs(2)).await;
    sd1.cancel();
    j1.await.unwrap().ok();

    // Consumer 2: overridden group id → independent, must also receive all N.
    let h2 = CountingHandler::new();
    let h2c = h2.clone();
    let sd2 = CancellationToken::new();
    let sd2c = sd2.clone();
    let c2 = KafkaConsumer::new(client.clone());
    let j2 = tokio::spawn(async move {
        c2.run::<WorkTopic, _>(
            h2c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd2c)
                .with_prefetch_count(1)
                .with_group_id("independent-sink"),
        )
        .await
    });
    assert!(
        h2.counter.wait_for(N, TIMEOUT).await,
        "consumer with overridden group_id must independently receive all {N}"
    );
    sd2.cancel();
    j2.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn fifo_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 5;
    for i in 0..N {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount: i as u64,
            })
            .await
            .unwrap();
    }

    // Consumer 1: default FIFO group `{queue}-fifo`, drain + commit.
    let h1 = OrderRecordingHandler::new();
    let h1c = h1.clone();
    let sd1 = CancellationToken::new();
    let sd1c = sd1.clone();
    let c1 = KafkaConsumer::new(client.clone());
    let j1 = tokio::spawn(async move {
        c1.run_fifo::<SeqSkipTopic, _>(
            h1c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd1c)
                .with_max_retries(5),
        )
        .await
    });
    assert!(
        h1.counter.wait_for(N, Duration::from_secs(60)).await,
        "default FIFO consumer should receive all {N}"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    sd1.cancel();
    j1.await.unwrap().ok();

    // Consumer 2: overridden group id → must rejoin a fresh `{group}-fifo`.
    let h2 = OrderRecordingHandler::new();
    let h2c = h2.clone();
    let sd2 = CancellationToken::new();
    let sd2c = sd2.clone();
    let c2 = KafkaConsumer::new(client.clone());
    let j2 = tokio::spawn(async move {
        c2.run_fifo::<SeqSkipTopic, _>(
            h2c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd2c)
                .with_max_retries(5)
                .with_group_id("independent-fifo-sink"),
        )
        .await
    });
    assert!(
        h2.counter.wait_for(N, Duration::from_secs(60)).await,
        "FIFO consumer with overridden group_id must independently receive all {N}"
    );
    sd2.cancel();
    j2.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn dlq_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 3;
    for i in 0..N {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("dead-{i}"),
                content: "dead".into(),
            })
            .await
            .unwrap();
    }

    // Push all N messages to the DLQ by rejecting them.
    let sd_reject = CancellationToken::new();
    let sdr = sd_reject.clone();
    let reject = KafkaConsumer::new(client.clone());
    let jr = tokio::spawn(async move {
        reject
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Reject),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sdr)
                    .with_prefetch_count(1),
            )
            .await
    });
    tokio::time::sleep(Duration::from_secs(10)).await;
    sd_reject.cancel();
    jr.await.unwrap().ok();

    // Two DLQ drains run concurrently on the same DLQ topic. Drain 1 joins the
    // default group `{dlq}-consumer`; drain 2 overrides to `{group}-dlq`. With
    // independent groups both receive every dead message; if the override were
    // ignored they would share one group and compete, so one drain would see
    // fewer than N. Both stop on `broker.close()` (run_dlq tracks the client
    // shutdown token), so neither handle is awaited before the broker closes.
    let d1 = DlqRecordingHandler::new();
    let d1c = d1.clone();
    let cd1 = KafkaConsumer::new(client.clone());
    let jd1 = tokio::spawn(async move { cd1.run_dlq::<WorkTopic, _>(d1c, ()).await });

    let d2 = DlqRecordingHandler::new();
    let d2c = d2.clone();
    let cd2 = KafkaConsumer::new(client.clone());
    let jd2 = tokio::spawn(async move {
        cd2.run_dlq_with_options::<WorkTopic, _>(
            d2c,
            (),
            ConsumerOptions::<Kafka>::new().with_group_id("independent-dlq-sink"),
        )
        .await
    });

    assert!(
        d1.counter.wait_for(N, TIMEOUT).await,
        "default DLQ drain should receive all {N} dead messages"
    );
    assert!(
        d2.counter.wait_for(N, TIMEOUT).await,
        "DLQ drain with overridden group_id must independently receive all {N}"
    );

    broker.close().await;
    jd1.await.unwrap().ok();
    jd2.await.unwrap().ok();
}

// ===========================================================================
// broker.topology() / broker.consumer_group() ergonomic knobs
//
// The replication-factor and partition-floor knobs are reachable directly on
// the broker hub rather than only on the low-level KafkaTopologyDeclarer /
// KafkaConsumerGroupRegistry. These are smoke tests that the new builder paths
// chain and produce a working topology end-to-end (RF=1 on a single-broker
// test container).
// ===========================================================================

#[tokio::test]
async fn broker_topology_exposes_replication_and_partition_knobs() {
    shove::define_topic!(
        TopoKnobsTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-topo-knobs").build()
    );
    impl MessageHandler<TopoKnobsTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    broker
        .topology()
        .with_replication_factor(1)
        .with_min_partitions(8)
        .declare::<TopoKnobsTopic>()
        .await
        .expect("declare via broker.topology() knobs should succeed");

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<TopoKnobsTopic>(&SimpleMessage {
            id: "topo-1".into(),
            content: "x".into(),
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
            .run::<TopoKnobsTopic, _>(
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
        "should consume from a topic declared through broker.topology() knobs"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_group_exposes_default_replication_factor() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "cg-rf-1".into(),
            content: "x".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    // with_default_replication_factor is reachable on broker.consumer_group()
    // and applies to the topology auto-declared by register().
    let mut group = broker.consumer_group().with_default_replication_factor(1);
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || handler_clone.clone(),
        )
        .await
        .expect("register with a default replication factor should succeed");

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(1, TIMEOUT).await;
        t.cancel();
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(handler.counter.get(), 1);
    broker.close().await;
}
