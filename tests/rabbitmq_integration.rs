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
