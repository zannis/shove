//! Integration tests for the SNS/SQS pub-sub backend.
//!
//! Each test spins up a fresh LocalStack container via testcontainers, runs
//! the test, and drops the container on completion (automatic cleanup).
//!
//! Run with: `cargo test --features aws-sns-sqs --test sns_sqs_integration`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use shove::sns::*;
use shove::*;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        // LocalStack accepts any non-empty credentials; set them so the AWS SDK
        // credential chain succeeds without requiring real environment variables.
        // SAFETY: tests run single-threaded (--test-threads=1) so mutating the
        // environment does not cause data races.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        }

        let container = LocalStack::default()
            .start()
            .await
            .expect("failed to start LocalStack container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");

        let endpoint_url = format!("http://{host}:{port}");

        // Give LocalStack a moment to initialize SNS/SQS
        tokio::time::sleep(Duration::from_secs(2)).await;

        Self {
            container,
            endpoint_url,
        }
    }

    fn sns_config(&self) -> SnsConfig {
        SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(self.endpoint_url.clone()),
        }
    }

    /// Create an SQS client for test verification.
    async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sqs::Client::new(&aws_config)
    }

    /// Helper: receive messages from SQS queue with a timeout.
    async fn receive_messages(
        &self,
        sqs_client: &aws_sdk_sqs::Client,
        queue_url: &str,
        expected_count: usize,
        timeout: Duration,
    ) -> Vec<aws_sdk_sqs::types::Message> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut all_messages = Vec::new();

        while all_messages.len() < expected_count && tokio::time::Instant::now() < deadline {
            let result = sqs_client
                .receive_message()
                .queue_url(queue_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .message_attribute_names("All")
                .send()
                .await
                .expect("failed to receive SQS messages");

            let msgs = result.messages();
            all_messages.extend(msgs.iter().cloned());
        }

        all_messages
    }
}

// ---------------------------------------------------------------------------
// TestSetup — creates a fully wired SNS/SQS setup
// ---------------------------------------------------------------------------

struct TestSetup {
    #[allow(dead_code)]
    broker: Option<TestBroker>,
    #[allow(dead_code)]
    sns_client: SnsClient,
    topic_registry: Arc<TopicRegistry>,
    queue_registry: Arc<QueueRegistry>,
    publisher: SnsPublisher,
    consumer: SqsConsumer,
}

impl TestSetup {
    /// Create a fully wired setup with its own internal broker.
    async fn create() -> Self {
        let broker = TestBroker::start().await;
        let sns_client = SnsClient::new(&broker.sns_config())
            .await
            .expect("failed to create SNS client");

        let topic_registry = Arc::new(TopicRegistry::new());
        let queue_registry = Arc::new(QueueRegistry::new());

        let publisher = SnsPublisher::new(sns_client.clone(), topic_registry.clone());
        let consumer = SqsConsumer::new(sns_client.clone(), queue_registry.clone());

        Self {
            broker: Some(broker),
            sns_client,
            topic_registry,
            queue_registry,
            publisher,
            consumer,
        }
    }

    /// Create a fully wired setup sharing an existing broker.
    /// The caller's `broker` must outlive this setup.
    async fn new(broker: &TestBroker) -> Self {
        let sns_client = SnsClient::new(&broker.sns_config())
            .await
            .expect("failed to create SNS client");

        let topic_registry = Arc::new(TopicRegistry::new());
        let queue_registry = Arc::new(QueueRegistry::new());

        let publisher = SnsPublisher::new(sns_client.clone(), topic_registry.clone());
        let consumer = SqsConsumer::new(sns_client.clone(), queue_registry.clone());

        Self {
            broker: None,
            sns_client,
            topic_registry,
            queue_registry,
            publisher,
            consumer,
        }
    }

    /// Returns an `SnsTopologyDeclarer` with queue registry attached.
    fn declarer(&self) -> SnsTopologyDeclarer {
        SnsTopologyDeclarer::new(self.sns_client.clone(), self.topic_registry.clone())
            .with_queue_registry(self.queue_registry.clone())
    }

    /// Declare the full topology for topic `T`.
    async fn declare<T: shove::Topic>(&self) {
        self.declarer()
            .declare(T::topology())
            .await
            .expect("topology declaration should succeed");
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

// ---------------------------------------------------------------------------
// Topic definitions
// ---------------------------------------------------------------------------

define_topic!(
    WorkTopic,
    SimpleMessage,
    TopologyBuilder::new("sqs-work")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .hold_queue(Duration::from_secs(2))
        .build()
);

define_topic!(
    NoDlqTopic,
    SimpleMessage,
    TopologyBuilder::new("sqs-nodlq").build()
);

define_sequenced_topic!(
    SeqSkipTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("sqs-seq-skip")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

define_sequenced_topic!(
    SeqFailAllTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("sqs-seq-failall")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(2)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

// ---------------------------------------------------------------------------
// Reusable test handlers
// ---------------------------------------------------------------------------

/// Handler that always returns the given outcome.
struct FixedOutcomeHandler(Outcome);

impl MessageHandler<WorkTopic> for FixedOutcomeHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.0.clone()
    }
}

/// Handler that counts invocations and returns Ack.
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

impl MessageHandler<WorkTopic> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<NoDlqTopic> for CountingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler that retries the first N calls, then returns Ack.
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

    #[allow(dead_code)]
    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }
}

impl MessageHandler<WorkTopic> for RetryThenAckHandler {
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

/// Handler that sleeps for a configurable duration, then returns Ack.
#[derive(Clone)]
struct SlowHandler {
    delay: Duration,
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl SlowHandler {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[allow(dead_code)]
    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
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

impl MessageHandler<WorkTopic> for SlowHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler that records (order_id, amount) pairs for verification.
#[derive(Clone)]
struct OrderRecordingHandler {
    records: Arc<Mutex<Vec<(String, u64)>>>,
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl OrderRecordingHandler {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[allow(dead_code)]
    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    async fn records(&self) -> Vec<(String, u64)> {
        self.records.lock().await.clone()
    }

    #[allow(dead_code)]
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

impl MessageHandler<SeqSkipTopic> for OrderRecordingHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        self.records.lock().await.push((msg.order_id, msg.amount));
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<SeqFailAllTopic> for OrderRecordingHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        self.records.lock().await.push((msg.order_id, msg.amount));
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler that rejects a specific order_id and acks the rest.
#[derive(Clone)]
struct RejectOrderHandler {
    reject_order_id: String,
    ack_count: Arc<AtomicU32>,
    reject_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl RejectOrderHandler {
    fn new(reject_order_id: impl Into<String>) -> Self {
        Self {
            reject_order_id: reject_order_id.into(),
            ack_count: Arc::new(AtomicU32::new(0)),
            reject_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[allow(dead_code)]
    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    fn reject_count(&self) -> u32 {
        self.reject_count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
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

impl MessageHandler<SeqSkipTopic> for RejectOrderHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        if msg.order_id == self.reject_order_id {
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

impl MessageHandler<SeqFailAllTopic> for RejectOrderHandler {
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
        if msg.order_id == self.reject_order_id {
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

/// Handler that records dead messages for verification.
#[derive(Clone)]
struct DlqRecordingHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl DlqRecordingHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[allow(dead_code)]
    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
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

impl MessageHandler<WorkTopic> for DlqRecordingHandler {
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

// ---------------------------------------------------------------------------
// Client lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_connect_and_shutdown() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("client should connect");

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
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("client should connect");

    let token = client.shutdown_token();
    assert!(!token.is_cancelled());

    client.shutdown().await;
    assert!(token.is_cancelled());
}

// ---------------------------------------------------------------------------
// Topology declaration (with SQS queues)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn topology_declares_standard_queue_and_dlq() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let main_url = setup
        .queue_registry
        .get("sqs-work")
        .await
        .expect("main queue URL should be registered");
    assert!(!main_url.is_empty());

    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should be registered");
    assert!(!dlq_url.is_empty());

    let arn = setup
        .topic_registry
        .get("sqs-work")
        .await
        .expect("topic ARN should be registered");
    assert!(arn.contains("sqs-work"));
}

#[tokio::test]
async fn topology_declares_fifo_shard_queues() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    let arn = setup
        .topic_registry
        .get("sqs-seq-skip")
        .await
        .expect("FIFO topic ARN should be registered");
    assert!(arn.contains("sqs-seq-skip.fifo"));

    for i in 0..2 {
        let shard_name = format!("sqs-seq-skip-seq-{i}");
        let url = setup
            .queue_registry
            .get(&shard_name)
            .await
            .unwrap_or_else(|| panic!("shard queue '{shard_name}' should be registered"));
        assert!(!url.is_empty());
    }

    let dlq_url = setup
        .queue_registry
        .get("sqs-seq-skip-dlq")
        .await
        .expect("DLQ for sequenced topic should be registered");
    assert!(!dlq_url.is_empty());
}

#[tokio::test]
async fn topology_idempotent_with_queues() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;

    setup.declare::<WorkTopic>().await;
    setup.declare::<WorkTopic>().await;

    let url = setup
        .queue_registry
        .get("sqs-work")
        .await
        .expect("queue should still be registered");
    assert!(!url.is_empty());
}

// ---------------------------------------------------------------------------
// Task 4: Basic publish & consume tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "test-1".to_string(),
        content: "hello world".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler_clone,
                ConsumerOptions::new(shutdown_clone).with_prefetch_count(1),
            )
            .await
    });

    let reached = handler.wait_for_count(1, Duration::from_secs(15)).await;
    assert!(reached, "handler should have received 1 message");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert_eq!(handler.count(), 1);
}

#[tokio::test]
async fn publish_and_consume_with_headers() {
    #[derive(Clone)]
    struct HeaderCapture(Arc<tokio::sync::Mutex<HashMap<String, String>>>);

    impl MessageHandler<WorkTopic> for HeaderCapture {
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata) -> Outcome {
            *self.0.lock().await = meta.headers;
            Outcome::Ack
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "header-test".to_string(),
        content: "with headers".to_string(),
    };

    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-abc-123".to_string());

    setup
        .publisher
        .publish_with_headers::<WorkTopic>(&msg, headers)
        .await
        .expect("publish_with_headers should succeed");

    let captured = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let handler = HeaderCapture(captured.clone());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(shutdown_clone).with_prefetch_count(1),
            )
            .await
    });

    // Wait until headers are captured (non-empty map)
    let timeout_result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            {
                let map = captured.lock().await;
                if !map.is_empty() {
                    return map.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let headers_received = timeout_result.expect("should receive headers within timeout");
    assert_eq!(
        headers_received.get("x-trace-id").map(|s| s.as_str()),
        Some("trace-abc-123"),
        "x-trace-id header should be preserved"
    );
}

#[tokio::test]
async fn publish_and_consume_batch() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("batch-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    setup
        .publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler_clone,
                ConsumerOptions::new(shutdown_clone).with_prefetch_count(10),
            )
            .await
    });

    let reached = handler.wait_for_count(5, Duration::from_secs(15)).await;
    assert!(reached, "handler should have received all 5 messages");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert_eq!(handler.count(), 5);
}

// ---------------------------------------------------------------------------
// Task 5: Rejection & DLQ tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejected_message_lands_in_dlq() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "reject-me".to_string(),
        content: "should be rejected".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let handler = FixedOutcomeHandler(Outcome::Reject);

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(shutdown_clone)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Poll the DLQ directly until the message arrives (SQS native redrive)
    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = sqs_client
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .send()
                .await
                .expect("failed to poll DLQ");

            let msgs = result.messages.unwrap_or_default();
            if !msgs.is_empty() {
                return msgs;
            }
        }
    })
    .await
    .expect("message should arrive in DLQ within 30 seconds");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert!(
        !dlq_messages.is_empty(),
        "at least 1 message should be in the DLQ"
    );
}

#[tokio::test]
async fn dlq_consumer_handles_dead_message() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Step 1: publish & reject to get a message into the DLQ
    let msg = SimpleMessage {
        id: "dlq-consumer-test".to_string(),
        content: "dead message".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let reject_handler = FixedOutcomeHandler(Outcome::Reject);

    let shutdown1 = CancellationToken::new();
    let shutdown1_clone = shutdown1.clone();

    let consumer1 = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle1 = tokio::spawn(async move {
        consumer1
            .run::<WorkTopic>(
                reject_handler,
                ConsumerOptions::new(shutdown1_clone)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Wait for message to reach DLQ
    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    // Poll with visibility_timeout=0 so the message becomes visible again immediately,
    // allowing the DLQ consumer started below to pick it up without delay.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = sqs_client
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(1)
                .wait_time_seconds(1)
                .visibility_timeout(0)
                .send()
                .await
                .expect("failed to poll DLQ");

            if !result.messages.unwrap_or_default().is_empty() {
                return;
            }
        }
    })
    .await
    .expect("message should arrive in DLQ within 30 seconds");

    shutdown1.cancel();
    handle1
        .await
        .expect("reject consumer task should not panic")
        .ok();

    // Step 2: start a DLQ consumer and verify it receives the dead message
    let dlq_handler = DlqRecordingHandler::new();
    let dlq_handler_clone = dlq_handler.clone();

    let consumer2 = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let h2 = tokio::spawn(async move { consumer2.run_dlq::<WorkTopic>(dlq_handler_clone).await });

    let reached = dlq_handler.wait_for_count(1, Duration::from_secs(15)).await;

    setup.sns_client.shutdown().await;
    h2.await.expect("DLQ consumer task should not panic").ok();

    assert!(reached, "DLQ handler should have received 1 dead message");
    assert_eq!(dlq_handler.count(), 1);
}

// ---------------------------------------------------------------------------
// Task 6: Retry Mechanism Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_then_ack_succeeds() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "retry-ack".to_string(),
        content: "retry then ack".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let handler = RetryThenAckHandler::new(1);
    let attempt_count = handler.attempt_count.clone();
    let signal = handler.signal.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
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

    // Wait for at least 2 calls (1 retry + 1 ack)
    tokio::time::timeout(Duration::from_secs(30), async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if attempt_count.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::select! {
                _ = signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => { break; }
            }
        }
    })
    .await
    .expect("should complete within timeout");

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");

    assert!(
        attempt_count.load(Ordering::Relaxed) >= 2,
        "handler should have been called at least 2 times (1 retry + 1 ack)"
    );
}

#[tokio::test]
async fn max_retries_sends_to_dlq() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "always-retry".to_string(),
        content: "should exhaust retries and go to DLQ".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let handler = FixedOutcomeHandler(Outcome::Retry);

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Poll DLQ directly until message appears
    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = sqs_client
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .send()
                .await
                .expect("failed to poll DLQ");

            let msgs = result.messages.unwrap_or_default();
            if !msgs.is_empty() {
                return msgs;
            }
        }
    })
    .await
    .expect("message should arrive in DLQ within 30 seconds");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert!(
        !dlq_messages.is_empty(),
        "exhausted-retry message should land in DLQ"
    );
}

// ---------------------------------------------------------------------------
// Task 7: Defer Mechanism Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_redelivers_message() {
    struct DeferThenAck(Arc<AtomicU32>);

    impl MessageHandler<WorkTopic> for DeferThenAck {
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
            let prev = self.0.fetch_add(1, Ordering::Relaxed);
            if prev == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "defer-test".to_string(),
        content: "defer then ack".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let calls = Arc::new(AtomicU32::new(0));
    let handler = DeferThenAck(calls.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
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

    // Wait for calls >= 2 (1 defer + 1 ack)
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if calls.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("should receive deferred message redelivery within timeout");

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");

    assert!(
        calls.load(Ordering::Relaxed) >= 2,
        "handler should have been called at least 2 times (1 defer + 1 ack)"
    );
}

// ---------------------------------------------------------------------------
// Task 8: Concurrent Consumption Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_consume_processes_all_messages() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let messages: Vec<SimpleMessage> = (1..=10)
        .map(|i| SimpleMessage {
            id: format!("concurrent-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    setup
        .publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler_clone,
                ConsumerOptions::new(sc).with_prefetch_count(10),
            )
            .await
    });

    let reached = handler.wait_for_count(10, Duration::from_secs(30)).await;
    assert!(reached, "handler should have received all 10 messages");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert_eq!(handler.count(), 10);
}

#[tokio::test]
async fn concurrent_consume_mixed_outcomes_routes_correctly() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "mixed-reject".to_string(),
        content: "should be rejected to DLQ".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let handler = FixedOutcomeHandler(Outcome::Reject);

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
                    .with_max_retries(1)
                    .with_prefetch_count(5),
            )
            .await
    });

    // Poll DLQ, verify message arrives
    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = sqs_client
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .send()
                .await
                .expect("failed to poll DLQ");

            let msgs = result.messages.unwrap_or_default();
            if !msgs.is_empty() {
                return msgs;
            }
        }
    })
    .await
    .expect("message should arrive in DLQ within 30 seconds");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert!(
        !dlq_messages.is_empty(),
        "rejected message should arrive in DLQ"
    );
}

#[tokio::test]
async fn concurrent_consume_graceful_shutdown_drains_inflight() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let messages: Vec<SimpleMessage> = (1..=3)
        .map(|i| SimpleMessage {
            id: format!("slow-{i}"),
            content: format!("slow message {i}"),
        })
        .collect();

    setup
        .publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = SlowHandler::new(Duration::from_millis(500));
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler_clone,
                ConsumerOptions::new(sc).with_prefetch_count(3),
            )
            .await
    });

    // Wait for at least 1 call to ensure messages are inflight
    handler.wait_for_count(1, Duration::from_secs(15)).await;

    // Cancel shutdown token to trigger graceful shutdown
    shutdown.cancel();

    // Verify consumer task completes within 10s (drains inflight)
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("consumer should complete within 10s after shutdown signal")
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");
}

// ---------------------------------------------------------------------------
// Task 9: Handler Timeout Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_timeout_triggers_retry() {
    // This handler increments count at the START of the call (before sleeping),
    // so we can detect invocations even when the handler is cancelled by a timeout.
    struct InvocationCountingSlowHandler {
        delay: Duration,
        count: Arc<AtomicU32>,
    }

    impl MessageHandler<WorkTopic> for InvocationCountingSlowHandler {
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata) -> Outcome {
            self.count.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.delay).await;
            Outcome::Ack
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "timeout-test".to_string(),
        content: "handler takes too long".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    // Handler takes 5s but timeout is 200ms — the first call will be cancelled by timeout,
    // then SQS redelivers and the handler is invoked again.
    let calls = Arc::new(AtomicU32::new(0));
    let handler = InvocationCountingSlowHandler {
        delay: Duration::from_secs(5),
        count: calls.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler,
                ConsumerOptions::new(sc)
                    .with_handler_timeout(Duration::from_millis(200))
                    .with_max_retries(5),
            )
            .await
    });

    // Wait for calls >= 2 (timeout cancels first, SQS redelivers)
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if calls.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("handler should be called at least twice due to timeout-triggered retry");

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");

    assert!(
        calls.load(Ordering::Relaxed) >= 2,
        "handler should have been invoked at least 2 times after timeout redelivery"
    );
}

// ---------------------------------------------------------------------------
// Task 10: Sequenced FIFO Consumption Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequenced_consume_preserves_order() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    for amount in 1..=5 {
        let msg = OrderMessage {
            order_id: "ORD-A".to_string(),
            amount,
        };
        setup
            .publisher
            .publish::<SeqSkipTopic>(&msg)
            .await
            .expect("publish should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let handler = OrderRecordingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic>(
                handler_clone,
                ConsumerOptions::new(sc).with_prefetch_count(10),
            )
            .await
    });

    let reached = handler.wait_for_count(5, Duration::from_secs(30)).await;
    assert!(reached, "handler should have received all 5 messages");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let records = handler.records().await;
    let amounts: Vec<u64> = records.iter().map(|(_, a)| *a).collect();
    assert_eq!(
        amounts,
        vec![1, 2, 3, 4, 5],
        "messages should arrive in order"
    );
}

#[tokio::test]
async fn sequenced_skip_continues_after_rejection() {
    #[derive(Clone)]
    struct SkipRejectHandler {
        records: Arc<Mutex<Vec<u64>>>,
        count: Arc<AtomicU32>,
        signal: Arc<tokio::sync::Notify>,
    }

    impl MessageHandler<SeqSkipTopic> for SkipRejectHandler {
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
            if msg.amount == 2 {
                self.count.fetch_add(1, Ordering::Relaxed);
                self.signal.notify_waiters();
                Outcome::Reject
            } else {
                self.records.lock().await.push(msg.amount);
                self.count.fetch_add(1, Ordering::Relaxed);
                self.signal.notify_waiters();
                Outcome::Ack
            }
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    for amount in [1, 2, 3] {
        let msg = OrderMessage {
            order_id: "ORD-SKIP".to_string(),
            amount,
        };
        setup
            .publisher
            .publish::<SeqSkipTopic>(&msg)
            .await
            .expect("publish should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let records = Arc::new(Mutex::new(Vec::new()));
    let handler = SkipRejectHandler {
        records: records.clone(),
        count: Arc::new(AtomicU32::new(0)),
        signal: Arc::new(tokio::sync::Notify::new()),
    };
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic>(
                handler_clone,
                ConsumerOptions::new(sc)
                    .with_prefetch_count(10)
                    .with_max_retries(1),
            )
            .await
    });

    // Wait for at least 2 acked records (amounts 1 and 3)
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if records.lock().await.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("should receive at least 2 acked messages within timeout");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let recorded = records.lock().await.clone();
    assert!(
        recorded.contains(&1) && recorded.contains(&3),
        "amounts 1 and 3 should be processed, got: {recorded:?}"
    );
    assert!(
        !recorded.contains(&2),
        "amount 2 should have been rejected, not recorded"
    );
}

#[tokio::test]
async fn sequenced_failall_poisons_key() {
    #[derive(Clone)]
    struct FailAllRejectHandler {
        records: Arc<Mutex<Vec<u64>>>,
        count: Arc<AtomicU32>,
        signal: Arc<tokio::sync::Notify>,
    }

    impl MessageHandler<SeqFailAllTopic> for FailAllRejectHandler {
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata) -> Outcome {
            if msg.amount == 2 {
                self.count.fetch_add(1, Ordering::Relaxed);
                self.signal.notify_waiters();
                Outcome::Reject
            } else {
                self.records.lock().await.push(msg.amount);
                self.count.fetch_add(1, Ordering::Relaxed);
                self.signal.notify_waiters();
                Outcome::Ack
            }
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqFailAllTopic>().await;

    for amount in [1, 2, 3] {
        let msg = OrderMessage {
            order_id: "ORD-FAIL".to_string(),
            amount,
        };
        setup
            .publisher
            .publish::<SeqFailAllTopic>(&msg)
            .await
            .expect("publish should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let records = Arc::new(Mutex::new(Vec::new()));
    let handler = FailAllRejectHandler {
        records: records.clone(),
        count: Arc::new(AtomicU32::new(0)),
        signal: Arc::new(tokio::sync::Notify::new()),
    };
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllTopic>(
                handler_clone,
                ConsumerOptions::new(sc)
                    .with_prefetch_count(10)
                    .with_max_retries(1),
            )
            .await
    });

    // Wait for message 1 to be processed
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !records.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("message 1 should be processed within timeout");

    // Wait additional 5s for poisoning to take effect
    tokio::time::sleep(Duration::from_secs(5)).await;

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let recorded = records.lock().await.clone();
    assert!(
        recorded.contains(&1),
        "amount 1 should have been processed, got: {recorded:?}"
    );
    assert!(
        !recorded.contains(&3),
        "amount 3 should NOT have been processed (key poisoned by amount 2 rejection), got: {recorded:?}"
    );
}

#[tokio::test]
async fn sequenced_multiple_keys_processed_concurrently() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    for amount in [1, 2, 3] {
        let msg = OrderMessage {
            order_id: "KEY-A".to_string(),
            amount,
        };
        setup
            .publisher
            .publish::<SeqSkipTopic>(&msg)
            .await
            .expect("publish should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for amount in [1, 2, 3] {
        let msg = OrderMessage {
            order_id: "KEY-B".to_string(),
            amount,
        };
        setup
            .publisher
            .publish::<SeqSkipTopic>(&msg)
            .await
            .expect("publish should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let handler = OrderRecordingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic>(
                handler_clone,
                ConsumerOptions::new(sc).with_prefetch_count(10),
            )
            .await
    });

    let reached = handler.wait_for_count(6, Duration::from_secs(30)).await;
    assert!(reached, "handler should have received all 6 messages");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let records = handler.records().await;
    let key_a: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "KEY-A")
        .map(|(_, a)| *a)
        .collect();
    let key_b: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "KEY-B")
        .map(|(_, a)| *a)
        .collect();

    assert_eq!(key_a, vec![1, 2, 3], "KEY-A messages should be in order");
    assert_eq!(key_b, vec![1, 2, 3], "KEY-B messages should be in order");
}

// ---------------------------------------------------------------------------
// Task 11: Queue Stats Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stats_provider_fetches_queue_depth() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Publish 3 messages without consuming them.
    for i in 1..=3 {
        let msg = SimpleMessage {
            id: format!("stats-{i}"),
            content: format!("message {i}"),
        };
        setup
            .publisher
            .publish::<WorkTopic>(&msg)
            .await
            .expect("publish should succeed");
    }

    // Give SQS time to propagate.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let stats = provider
        .get_queue_stats("sqs-work")
        .await
        .expect("get_queue_stats should succeed");

    assert!(
        stats.messages_ready >= 3,
        "expected at least 3 ready messages, got {}",
        stats.messages_ready
    );
}

#[tokio::test]
async fn stats_provider_missing_queue_errors() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    // Do NOT declare any topology — queue_registry is empty.

    let provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let result = provider.get_queue_stats("nonexistent-queue").await;

    assert!(
        result.is_err(),
        "expected Err for nonexistent queue, got Ok"
    );
}

// ---------------------------------------------------------------------------
// Task 12: Consumer Group & Registry Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_group_processes_messages() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Publish 5 messages.
    for i in 1..=5 {
        let msg = SimpleMessage {
            id: format!("grp-{i}"),
            content: format!("group message {i}"),
        };
        setup
            .publisher
            .publish::<WorkTopic>(&msg)
            .await
            .expect("publish should succeed");
    }

    // Shared state across all consumer instances.
    let template_handler = CountingHandler::new();
    let handler_for_wait = template_handler.clone();

    let config = SqsConsumerGroupConfig::new(2..=4)
        .with_prefetch_count(5)
        .with_max_retries(3);

    let group_token = CancellationToken::new();

    let mut group = SqsConsumerGroup::new::<WorkTopic, CountingHandler>(
        "test-group",
        "sqs-work",
        config,
        setup.sns_client.clone(),
        setup.queue_registry.clone(),
        group_token.clone(),
        move || template_handler.clone(),
    );

    group.start();
    assert_eq!(
        group.active_consumers(),
        2,
        "group should start with 2 consumers"
    );

    let reached = handler_for_wait
        .wait_for_count(5, Duration::from_secs(30))
        .await;
    assert!(reached, "handler should have processed all 5 messages");

    group.shutdown().await;
}

#[tokio::test]
async fn consumer_group_scales_up_and_down() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let config = SqsConsumerGroupConfig::new(1..=4).with_prefetch_count(1);
    let group_token = CancellationToken::new();

    let mut group = SqsConsumerGroup::new::<WorkTopic, CountingHandler>(
        "scale-test-group",
        "sqs-work",
        config,
        setup.sns_client.clone(),
        setup.queue_registry.clone(),
        group_token.clone(),
        CountingHandler::new,
    );

    group.start();
    assert_eq!(group.active_consumers(), 1, "should start with 1 consumer");

    assert!(group.scale_up(), "first scale_up should succeed");
    assert_eq!(
        group.active_consumers(),
        2,
        "should have 2 consumers after scale_up"
    );

    assert!(group.scale_up(), "second scale_up should succeed");
    assert_eq!(
        group.active_consumers(),
        3,
        "should have 3 consumers after second scale_up"
    );

    assert!(group.scale_down(), "first scale_down should succeed");
    assert_eq!(
        group.active_consumers(),
        2,
        "should have 2 consumers after scale_down"
    );

    assert!(group.scale_down(), "second scale_down should succeed");
    assert_eq!(
        group.active_consumers(),
        1,
        "should have 1 consumer after second scale_down"
    );

    let at_min = group.scale_down();
    assert!(!at_min, "scale_down should return false when at minimum");
    assert_eq!(
        group.active_consumers(),
        1,
        "should still have 1 consumer at minimum"
    );

    group.shutdown().await;
}

#[tokio::test]
async fn registry_register_declares_topology_and_starts() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;

    let template_handler = CountingHandler::new();
    let handler_for_wait = template_handler.clone();

    let config = SqsConsumerGroupConfig::new(1..=2).with_prefetch_count(5);

    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    registry
        .register::<WorkTopic, CountingHandler>(config, move || template_handler.clone())
        .await
        .expect("register should succeed");

    // Topology should now be declared.
    assert!(
        setup.topic_registry.get("sqs-work").await.is_some(),
        "topic_registry should have 'sqs-work' entry"
    );
    assert!(
        setup.queue_registry.get("sqs-work").await.is_some(),
        "queue_registry should have 'sqs-work' entry"
    );

    // Publish 1 message before starting.
    let msg = SimpleMessage {
        id: "reg-1".to_string(),
        content: "registry message".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    registry.start_all();

    let reached = handler_for_wait
        .wait_for_count(1, Duration::from_secs(30))
        .await;
    assert!(
        reached,
        "registry consumer should have processed the message"
    );

    registry.shutdown_all().await;
}

#[tokio::test]
async fn registry_duplicate_registration_fails() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;

    let config = SqsConsumerGroupConfig::new(1..=2);

    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    // First registration should succeed.
    registry
        .register::<WorkTopic, CountingHandler>(config, CountingHandler::new)
        .await
        .expect("first registration should succeed");

    // Second registration for the same topic should fail.
    let config2 = SqsConsumerGroupConfig::new(1..=2);
    let result = registry
        .register::<WorkTopic, CountingHandler>(config2, CountingHandler::new)
        .await;

    assert!(result.is_err(), "duplicate registration should return Err");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("already registered"),
        "error message should mention 'already registered', got: {err_msg}"
    );

    registry.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Task 13: Deserialization Failure Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deserialization_failure_rejects_to_dlq() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Send a malformed message directly via SQS (bypasses SNS publisher/serialization).
    let sqs_client = broker.sqs_client().await;
    let queue_url = setup
        .queue_registry
        .get("sqs-work")
        .await
        .expect("sqs-work queue URL should be registered");

    sqs_client
        .send_message()
        .queue_url(&queue_url)
        .message_body("not valid JSON")
        .send()
        .await
        .expect("sending raw malformed message should succeed");

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic>(
                handler_clone,
                ConsumerOptions::new(sc)
                    .with_max_retries(1)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Poll DLQ until message appears or timeout.
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should be registered");

    let dlq_messages = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = sqs_client
                .receive_message()
                .queue_url(&dlq_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .send()
                .await
                .expect("failed to poll DLQ");

            let msgs = result.messages.unwrap_or_default();
            if !msgs.is_empty() {
                return msgs;
            }
        }
    })
    .await
    .expect("malformed message should be routed to DLQ within 30 seconds");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert!(
        !dlq_messages.is_empty(),
        "malformed message should have been dead-lettered"
    );
    assert_eq!(
        handler.count(),
        0,
        "handler should not have been invoked (deserialization failed before handler)"
    );
}

// ---------------------------------------------------------------------------
// Task 14: Error Handling Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_run_on_undeclared_queue_fails() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    // Do NOT declare topology — queue_registry is empty.

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let result = consumer
        .run::<WorkTopic>(handler, ConsumerOptions::new(shutdown))
        .await;

    assert!(
        result.is_err(),
        "run should return Err for undeclared queue"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no SQS queue URL registered"),
        "error should mention 'no SQS queue URL registered', got: {err_msg}"
    );
}

#[tokio::test]
async fn run_dlq_on_topic_without_dlq_fails() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<NoDlqTopic>().await;

    let handler = CountingHandler::new();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let result = consumer.run_dlq::<NoDlqTopic>(handler).await;

    assert!(
        result.is_err(),
        "run_dlq should return Err for topic without DLQ"
    );
}

#[tokio::test]
async fn consumer_run_dlq_on_topic_without_dlq_name_fails() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<NoDlqTopic>().await;

    let handler = CountingHandler::new();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let result = consumer.run_dlq::<NoDlqTopic>(handler).await;

    assert!(
        result.is_err(),
        "run_dlq should Err when topic has no DLQ configured"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no DLQ configured") || err_msg.contains("DLQ"),
        "error should mention DLQ, got: {err_msg}"
    );
}
