//! Integration tests for the RabbitMQ backend.
//!
//! Each test spins up a fresh RabbitMQ container via testcontainers, runs
//! the test, and drops the container on completion (automatic cleanup).
//!
//! Run with: `cargo test --test rabbitmq_integration`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use shove::rabbitmq::*;
use shove::*;

use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test harness: shared setup
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<RabbitMq>,
    amqp_url: String,
    mgmt_url: String,
}

impl TestBroker {
    async fn start() -> Self {
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
            .expect("failed to get management port");

        let amqp_url = format!("amqp://{host}:{amqp_port}");
        let mgmt_url = format!("http://{host}:{mgmt_port}");

        // Enable the consistent-hash exchange plugin (needed for sequenced topics).
        // We must wait for the command to complete (read stdout) before proceeding.
        let mut result = container
            .exec(ExecCommand::new([
                "rabbitmq-plugins",
                "enable",
                "rabbitmq_consistent_hash_exchange",
            ]))
            .await
            .expect("failed to enable consistent hash plugin");
        // Block until the command finishes
        let _ = result.stdout_to_vec().await;

        // Give RabbitMQ time to load the plugin and for the management API to initialize
        tokio::time::sleep(Duration::from_secs(3)).await;

        Self {
            _container: container,
            amqp_url,
            mgmt_url,
        }
    }

    fn rmq_config(&self) -> RabbitMqConfig {
        RabbitMqConfig {
            uri: self.amqp_url.clone(),
        }
    }

    fn mgmt_config(&self) -> ManagementConfig {
        ManagementConfig::new(&self.mgmt_url, "guest", "guest")
    }
}

// ---------------------------------------------------------------------------
// Test topics and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SimpleMessage {
    body: String,
}

define_topic!(
    SimpleWork,
    SimpleMessage,
    TopologyBuilder::new("test-simple")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct OrderMessage {
    account: String,
    seq: u32,
}

define_sequenced_topic!(
    OrderTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-orders")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(shove::topology::SequenceFailure::Skip)
        .routing_shards(2)
        .build()
);

// Topic for autoscaler tests — separate queue name to avoid collisions
define_topic!(
    ScalableWork,
    SimpleMessage,
    TopologyBuilder::new("test-scalable")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for retry tests
define_topic!(
    RetryWork,
    SimpleMessage,
    TopologyBuilder::new("test-retry")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for defer tests (no hold queue — tests the nack-requeue fallback)
define_topic!(
    DeferNoHold,
    SimpleMessage,
    TopologyBuilder::new("test-defer-nohold").dlq().build()
);

// Topic for defer tests (with hold queue)
define_topic!(
    DeferWithHold,
    SimpleMessage,
    TopologyBuilder::new("test-defer-hold")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for audit tests
define_topic!(
    AuditedWork,
    SimpleMessage,
    TopologyBuilder::new("test-audited")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Sequenced topic with FailAll — rejection poisons the sequence key
define_sequenced_topic!(
    FailAllOrders,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-failall-orders")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(shove::topology::SequenceFailure::FailAll)
        .routing_shards(2)
        .build()
);

// Sequenced topic with Skip — rejection skips the message, sequence continues
// (reuse OrderTopic which already has Skip, but define a separate one for test isolation)
define_sequenced_topic!(
    SkipOrders,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-skip-orders")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(shove::topology::SequenceFailure::Skip)
        .routing_shards(1)
        .build()
);

/// Handler that counts messages and optionally stores them.
#[derive(Clone)]
struct CountingHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl CountingHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<SimpleWork> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<OrderTopic> for CountingHandler {
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<ScalableWork> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<AuditedWork> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<SkipOrders> for CountingHandler {
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<FailAllOrders> for CountingHandler {
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler that rejects messages with a specific seq number, acks the rest.
/// Used to test Skip vs FailAll sequencing behavior.
#[derive(Clone)]
struct RejectSeqHandler {
    reject_seq: u32,
    ack_count: Arc<AtomicU32>,
    reject_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl RejectSeqHandler {
    fn new(reject_seq: u32) -> Self {
        Self {
            reject_seq,
            ack_count: Arc::new(AtomicU32::new(0)),
            reject_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }

    fn reject_count(&self) -> u32 {
        self.reject_count.load(Ordering::Relaxed)
    }

    async fn wait_for_total(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let total = self.ack_count() + self.reject_count();
            if total >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return (self.ack_count() + self.reject_count()) >= target;
                }
            }
        }
    }
}

impl MessageHandler<SkipOrders> for RejectSeqHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        if msg.seq == self.reject_seq {
            self.reject_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Reject
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

impl MessageHandler<FailAllOrders> for RejectSeqHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        if msg.seq == self.reject_seq {
            self.reject_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Reject
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

/// DLQ handler for OrderMessage topics — counts dead-lettered messages.
#[derive(Clone)]
struct OrderDlqHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl OrderDlqHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<SkipOrders> for OrderDlqHandler {
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: OrderMessage, _meta: DeadMessageMetadata) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

impl MessageHandler<FailAllOrders> for OrderDlqHandler {
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: OrderMessage, _meta: DeadMessageMetadata) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

/// Handler that always rejects (for testing DLQ routing).
struct RejectHandler;

impl MessageHandler<SimpleWork> for RejectHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Reject
    }
}

/// DLQ handler that counts dead messages.
#[derive(Clone)]
struct DlqCountingHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl DlqCountingHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<SimpleWork> for DlqCountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

/// Handler that returns Retry for the first N messages, then Ack.
#[derive(Clone)]
struct RetryThenAckHandler {
    retry_until: u32,
    attempt_count: Arc<AtomicU32>,
    ack_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl RetryThenAckHandler {
    fn new(retry_until: u32) -> Self {
        Self {
            retry_until,
            attempt_count: Arc::new(AtomicU32::new(0)),
            ack_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }
}

impl MessageHandler<RetryWork> for RetryThenAckHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        let attempt = self.attempt_count.fetch_add(1, Ordering::Relaxed);
        if attempt < self.retry_until {
            Outcome::Retry
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

/// Handler that returns Defer on the first call, then Ack on subsequent calls.
#[derive(Clone)]
struct DeferOnceHandler {
    call_count: Arc<AtomicU32>,
    ack_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl DeferOnceHandler {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicU32::new(0)),
            ack_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }
}

impl MessageHandler<DeferNoHold> for DeferOnceHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        let call = self.call_count.fetch_add(1, Ordering::Relaxed);
        if call == 0 {
            Outcome::Defer
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

impl MessageHandler<DeferWithHold> for DeferOnceHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        let call = self.call_count.fetch_add(1, Ordering::Relaxed);
        if call == 0 {
            Outcome::Defer
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

// --- Client ---

#[tokio::test]
async fn client_connect_and_create_channel() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    assert!(client.is_connected());

    let _channel = client.create_channel().await.unwrap();
    let _confirm_channel = client.create_confirm_channel().await.unwrap();

    client.shutdown().await;
    // After shutdown the connection is closed
}

#[tokio::test]
async fn client_shutdown_cancels_token() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let token = client.shutdown_token();

    assert!(!token.is_cancelled());
    client.shutdown().await;
    assert!(token.is_cancelled());
}

// --- Topology ---

#[tokio::test]
async fn topology_declare_creates_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    // Verify by checking management API
    let mgmt = ManagementConfig::new(&broker.mgmt_url, "guest", "guest");
    let stats_client = reqwest::Client::new();

    // Main queue should exist
    let resp = stats_client
        .get(format!("{}/api/queues/%2F/test-simple", broker.mgmt_url))
        .basic_auth(&mgmt.username, Some(&mgmt.password))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "main queue should exist");

    // DLQ should exist
    let resp = stats_client
        .get(format!(
            "{}/api/queues/%2F/test-simple-dlq",
            broker.mgmt_url
        ))
        .basic_auth(&mgmt.username, Some(&mgmt.password))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "DLQ should exist");

    // Hold queue should exist
    let resp = stats_client
        .get(format!(
            "{}/api/queues/%2F/test-simple-hold-1s",
            broker.mgmt_url
        ))
        .basic_auth(&mgmt.username, Some(&mgmt.password))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "hold queue should exist");

    client.shutdown().await;
}

#[tokio::test]
async fn topology_declare_sequenced_creates_sub_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(OrderTopic::topology()).await.unwrap();

    let stats_client = reqwest::Client::new();

    // Sub-queues should exist (2 shards)
    for i in 0..2 {
        let resp = stats_client
            .get(format!(
                "{}/api/queues/%2F/test-orders-seq-{i}",
                broker.mgmt_url
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "sub-queue test-orders-seq-{i} should exist"
        );
    }

    client.shutdown().await;
}

// --- Publisher ---

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    // Declare topology
    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    // Publish
    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    let msg = SimpleMessage {
        body: "hello".into(),
    };
    publisher.publish::<SimpleWork>(&msg).await.unwrap();

    // Consume
    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork>(h, opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 1);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn publish_with_headers() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    let msg = SimpleMessage {
        body: "with-headers".into(),
    };
    let mut headers = HashMap::new();
    headers.insert("x-trace-id".into(), "trace-123".into());
    publisher
        .publish_with_headers::<SimpleWork>(&msg, headers)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork>(h, opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn publish_batch() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("batch-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<SimpleWork>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<SimpleWork>(h, opts).await
    });

    assert!(handler.wait_for_count(5, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 5);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Reject → DLQ ---

#[tokio::test]
async fn rejected_message_lands_in_dlq() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    // Publish one message
    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    publisher
        .publish::<SimpleWork>(&SimpleMessage {
            body: "reject-me".into(),
        })
        .await
        .unwrap();

    // Consumer that rejects everything
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let s = shutdown.clone();
    let reject_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork>(RejectHandler, opts).await
    });

    // Give it time to reject
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    reject_handle.await.unwrap().unwrap();

    // Now consume from DLQ
    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<SimpleWork>(dh).await });

    assert!(dlq_handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
}

// --- Management API ---

#[tokio::test]
async fn management_client_fetches_queue_stats() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SimpleWork::topology()).await.unwrap();

    // Publish some messages
    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<SimpleWork>(&SimpleMessage {
                body: format!("stats-{i}"),
            })
            .await
            .unwrap();
    }

    // Management API stats update interval can be slow — poll until ready
    let mgmt_config = broker.mgmt_config();
    let http = reqwest::Client::new();
    let mut stats: Option<QueueStats> = None;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let resp = http
            .get(format!("{}/api/queues/%2F/test-simple", broker.mgmt_url))
            .basic_auth(&mgmt_config.username, Some(&mgmt_config.password))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let s: QueueStats = resp.json().await.unwrap();
        if s.messages_ready == 3 {
            stats = Some(s);
            break;
        }
    }
    assert_eq!(
        stats
            .expect("stats never reached expected count")
            .messages_ready,
        3
    );

    client.shutdown().await;
}

// --- Consumer Group + Registry ---

#[tokio::test]
async fn registry_register_declares_topology_and_starts() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    // register auto-declares topology
    registry
        .register::<SimpleWork, _>(ConsumerGroupConfig::new(1..=3), move || h.clone())
        .await
        .unwrap();

    registry.start_all();

    // Publish a message — the consumer group should pick it up
    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    publisher
        .publish::<SimpleWork>(&SimpleMessage {
            body: "registry-test".into(),
        })
        .await
        .unwrap();

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 1);

    registry.shutdown_all().await;
    client.shutdown().await;
}

// --- Sequenced consumer ---

#[tokio::test]
async fn sequenced_consume_preserves_order() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(OrderTopic::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();

    // Publish 5 messages for the same account
    for i in 0..5 {
        publisher
            .publish::<OrderTopic>(&OrderMessage {
                account: "ACC-A".into(),
                seq: i,
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_max_retries(3);
        consumer.run_sequenced::<OrderTopic>(h, opts).await
    });

    assert!(handler.wait_for_count(5, Duration::from_secs(15)).await);
    assert_eq!(handler.count(), 5);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Retry via hold queue ---

#[tokio::test]
async fn retry_via_hold_queue_then_ack() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(RetryWork::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    publisher
        .publish::<RetryWork>(&SimpleMessage {
            body: "retry-me".into(),
        })
        .await
        .unwrap();

    // retry_until=2: attempt 0 → Retry, attempt 1 → Retry, attempt 2 → Ack
    let handler = RetryThenAckHandler::new(2);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<RetryWork>(h, opts).await
    });

    // Wait up to 30s for the message to be eventually acked after 2 retries
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.ack_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ack after retries");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    assert_eq!(handler.ack_count(), 1);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Defer with hold queue ---

#[tokio::test]
async fn defer_with_hold_queue_redelivers() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(DeferWithHold::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    publisher
        .publish::<DeferWithHold>(&SimpleMessage {
            body: "defer-me".into(),
        })
        .await
        .unwrap();

    let handler = DeferOnceHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<DeferWithHold>(h, opts).await
    });

    // Wait up to 30s: first delivery → Defer → hold queue → redeliver → Ack
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.ack_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ack after defer with hold queue");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    assert_eq!(handler.ack_count(), 1);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Defer without hold queue (nack+requeue fallback) ---

#[tokio::test]
async fn defer_without_hold_queue_requeues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(DeferNoHold::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    publisher
        .publish::<DeferNoHold>(&SimpleMessage {
            body: "defer-nohold".into(),
        })
        .await
        .unwrap();

    let handler = DeferOnceHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<DeferNoHold>(h, opts).await
    });

    // Wait up to 30s: first delivery → Defer (nack+requeue) → redelivered → Ack
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.ack_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ack after defer without hold queue");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    assert_eq!(handler.ack_count(), 1);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Autoscaler scales up under load ---

#[tokio::test]
async fn autoscaler_scales_up_under_load() {
    use tokio::sync::Mutex;

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    // Use min_consumers=0 so start_all spawns no consumers, letting messages
    // accumulate in the queue so the autoscaler sees the load and scales up.
    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(0..=4).with_prefetch_count(5),
            move || h.clone(),
        )
        .await
        .unwrap();
    registry.start_all(); // starts 0 consumers

    // Publish 100 messages — above scale_up threshold (5 * 1 * 2.0 = 10)
    // even after the first scale-up event adds one consumer.
    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    let messages: Vec<SimpleMessage> = (0..100)
        .map(|i| SimpleMessage {
            body: format!("load-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ScalableWork>(&messages)
        .await
        .unwrap();

    // Give management API time to reflect the queued messages
    tokio::time::sleep(Duration::from_secs(2)).await;

    let registry = Arc::new(Mutex::new(registry));
    let autoscaler_token = CancellationToken::new();
    let reg_arc = registry.clone();
    let token_clone = autoscaler_token.clone();
    let mgmt_config = broker.mgmt_config();
    let autoscaler_handle = tokio::spawn(async move {
        let mut autoscaler = Autoscaler::new(
            &mgmt_config,
            AutoscalerConfig {
                poll_interval: Duration::from_secs(1),
                hysteresis_duration: Duration::from_secs(1),
                cooldown_duration: Duration::from_secs(1),
                ..AutoscalerConfig::default()
            },
        );
        autoscaler.run(reg_arc, token_clone).await;
    });

    // Wait up to 30s for at least one scale-up to occur
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let active = {
            let reg = registry.lock().await;
            reg.groups()["test-scalable"].active_consumers()
        };
        if active >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out: autoscaler did not scale up (active_consumers={})",
                active
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let active = {
        let reg = registry.lock().await;
        reg.groups()["test-scalable"].active_consumers()
    };
    assert!(
        active >= 1,
        "expected at least 1 consumer after scale-up, got {active}"
    );

    autoscaler_token.cancel();
    autoscaler_handle.await.unwrap();

    let mut reg = registry.lock().await;
    reg.shutdown_all().await;
    client.shutdown().await;
}

// --- Autoscaler scales down when idle ---

#[tokio::test]
async fn autoscaler_scales_down_when_idle() {
    use tokio::sync::Mutex;

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(1..=4).with_prefetch_count(5),
            move || h.clone(),
        )
        .await
        .unwrap();
    registry.start_all();

    // Manually scale up to 3 consumers
    {
        let group = registry.groups_mut().get_mut("test-scalable").unwrap();
        group.scale_up();
        group.scale_up();
    }
    assert_eq!(
        registry.groups()["test-scalable"].active_consumers(),
        3,
        "should have 3 consumers after manual scale-up"
    );

    // No messages published — queue is empty → scale_down_threshold = 5 * 3 * 0.5 = 7.5, ready=0 < 7.5
    let registry = Arc::new(Mutex::new(registry));
    let autoscaler_token = CancellationToken::new();
    let reg_arc = registry.clone();
    let token_clone = autoscaler_token.clone();
    let mgmt_config = broker.mgmt_config();
    let autoscaler_handle = tokio::spawn(async move {
        let mut autoscaler = Autoscaler::new(
            &mgmt_config,
            AutoscalerConfig {
                poll_interval: Duration::from_secs(1),
                hysteresis_duration: Duration::from_secs(1),
                cooldown_duration: Duration::from_secs(1),
                ..AutoscalerConfig::default()
            },
        );
        autoscaler.run(reg_arc, token_clone).await;
    });

    // Wait up to 20s for scale-down below 3
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let active = {
            let reg = registry.lock().await;
            reg.groups()["test-scalable"].active_consumers()
        };
        if active < 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out: autoscaler did not scale down (active_consumers={})",
                active
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let active = {
        let reg = registry.lock().await;
        reg.groups()["test-scalable"].active_consumers()
    };
    assert!(
        active < 3,
        "expected fewer than 3 consumers after scale-down, got {active}"
    );

    autoscaler_token.cancel();
    autoscaler_handle.await.unwrap();

    let mut reg = registry.lock().await;
    reg.shutdown_all().await;
    client.shutdown().await;
}

// --- Audit wrapper captures records ---

#[tokio::test]
async fn audited_handler_captures_audit_records() {
    use shove::{AuditHandler, AuditRecord, Audited};

    #[derive(Clone)]
    struct CollectingAuditHandler {
        records: Arc<std::sync::Mutex<Vec<AuditRecord<SimpleMessage>>>>,
        signal: Arc<tokio::sync::Notify>,
    }

    impl CollectingAuditHandler {
        fn new() -> Self {
            Self {
                records: Arc::new(std::sync::Mutex::new(Vec::new())),
                signal: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn record_count(&self) -> usize {
            self.records.lock().unwrap().len()
        }
    }

    impl AuditHandler<AuditedWork> for CollectingAuditHandler {
        async fn audit(&self, record: &AuditRecord<SimpleMessage>) -> shove::error::Result<()> {
            self.records.lock().unwrap().push(record.clone());
            self.signal.notify_waiters();
            Ok(())
        }
    }

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(AuditedWork::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<AuditedWork>(&SimpleMessage {
                body: format!("audit-{i}"),
            })
            .await
            .unwrap();
    }

    let inner_handler = CountingHandler::new();
    let audit_handler = CollectingAuditHandler::new();
    let audited = Audited::new(inner_handler.clone(), audit_handler.clone());

    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<AuditedWork>(audited, opts).await
    });

    // Wait for 3 audit records
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if audit_handler.record_count() >= 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for audit records (got {})",
                audit_handler.record_count()
            );
        }
        tokio::select! {
            _ = audit_handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    let records = audit_handler.records.lock().unwrap().clone();
    assert_eq!(records.len(), 3);
    for record in &records {
        assert_eq!(record.topic, "test-audited");
        assert!(matches!(record.outcome, Outcome::Ack));
        assert!(!record.trace_id.is_empty());
    }

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
}

// --- Sequenced: Skip mode ---

/// With SequenceFailure::Skip, rejecting one message should NOT affect
/// subsequent messages for the same sequence key. All non-rejected messages
/// should be acked, and only the rejected one lands in the DLQ.
#[tokio::test]
async fn sequenced_skip_continues_after_rejection() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(SkipOrders::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();

    // Publish 5 messages for the same account: seq 0,1,2,3,4
    // Handler will reject seq=2
    for i in 0..5u32 {
        publisher
            .publish::<SkipOrders>(&OrderMessage {
                account: "ACC-SKIP".into(),
                seq: i,
            })
            .await
            .unwrap();
    }

    let handler = RejectSeqHandler::new(2);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_max_retries(3);
        consumer.run_sequenced::<SkipOrders>(h, opts).await
    });

    // With Skip: 4 messages should be acked (0,1,3,4), 1 rejected (2)
    assert!(
        handler.wait_for_total(5, Duration::from_secs(15)).await,
        "timed out: ack={} reject={}",
        handler.ack_count(),
        handler.reject_count()
    );
    assert_eq!(
        handler.ack_count(),
        4,
        "should ack 4 messages (skip rejected)"
    );
    assert_eq!(handler.reject_count(), 1, "should reject 1 message");

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    // Verify the rejected message landed in DLQ
    let dlq_handler = OrderDlqHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<SkipOrders>(dh).await });

    assert!(
        dlq_handler.wait_for_count(1, Duration::from_secs(10)).await,
        "expected 1 DLQ message"
    );
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
}

// --- Sequenced: FailAll mode ---

/// With SequenceFailure::FailAll, rejecting one message should "poison" the
/// sequence key — all subsequent messages for that key are auto-rejected to DLQ.
/// Messages for OTHER keys should be unaffected.
#[tokio::test]
async fn sequenced_failall_poisons_key_after_rejection() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let channel = client.create_channel().await.unwrap();

    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declarer.declare(FailAllOrders::topology()).await.unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();

    // Publish 5 messages for ACC-FAIL: seq 0,10,20,30,40
    // Handler rejects seq=10 → key gets poisoned → seq 20,30,40 auto-DLQ'd
    for i in 0..5u32 {
        publisher
            .publish::<FailAllOrders>(&OrderMessage {
                account: "ACC-FAIL".into(),
                seq: i * 10,
            })
            .await
            .unwrap();
    }

    // Also publish 3 messages for ACC-OK (different key, uses seq 1,2,3 — none match reject_seq=10)
    for i in 1..=3u32 {
        publisher
            .publish::<FailAllOrders>(&OrderMessage {
                account: "ACC-OK".into(),
                seq: i,
            })
            .await
            .unwrap();
    }

    let handler = RejectSeqHandler::new(10); // rejects seq=10 (only ACC-FAIL has this)
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_max_retries(3);
        consumer.run_sequenced::<FailAllOrders>(h, opts).await
    });

    // Wait for the handler to process messages.
    // ACC-FAIL: seq 0 → Ack, seq 10 → Reject (poisons key), seq 20,30,40 → auto-DLQ (never reach handler)
    // ACC-OK: seq 1,2,3 → Ack (different key, none match reject_seq=10)
    // Handler sees: 1 ack (seq 0) + 1 reject (seq 10) + 3 acks (ACC-OK) = 5 total
    assert!(
        handler.wait_for_total(5, Duration::from_secs(15)).await,
        "timed out: ack={} reject={}",
        handler.ack_count(),
        handler.reject_count()
    );

    let total_ack = handler.ack_count();
    let total_reject = handler.reject_count();
    assert_eq!(
        total_ack, 4,
        "expected 4 acks (1 ACC-FAIL seq 0 + 3 ACC-OK)"
    );
    assert_eq!(total_reject, 1, "expected 1 reject (ACC-FAIL seq 10)");

    // Give the consumer time to auto-reject the poisoned messages (seq 20,30,40)
    // These don't reach the handler — they're rejected at the consumer level.
    tokio::time::sleep(Duration::from_secs(3)).await;

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    // Now check DLQ: should have at least 4 messages
    // - 1 from the explicit Reject (seq 10)
    // - 3 from poisoned key auto-rejection (seq 20,30,40)
    let dlq_handler = OrderDlqHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<FailAllOrders>(dh).await });

    assert!(
        dlq_handler.wait_for_count(4, Duration::from_secs(10)).await,
        "expected at least 4 DLQ messages, got {}",
        dlq_handler.count()
    );

    client.shutdown().await;
    dlq_handle.abort();
}
