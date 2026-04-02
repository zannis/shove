//! Integration tests for the SNS/SQS pub-sub backend.
//!
//! Each test spins up a fresh LocalStack container via testcontainers, runs
//! the test, and drops the container on completion (automatic cleanup).
//!
//! Run with: `cargo test --features aws-sns-sqs --test sns_sqs_integration`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
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

    let reached = handler
        .wait_for_count(1, Duration::from_secs(15))
        .await;
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

    let reached = handler
        .wait_for_count(5, Duration::from_secs(15))
        .await;
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
    handle1.await.expect("reject consumer task should not panic").ok();

    // Step 2: start a DLQ consumer and verify it receives the dead message
    let dlq_handler = DlqRecordingHandler::new();
    let dlq_handler_clone = dlq_handler.clone();

    let consumer2 = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let h2 = tokio::spawn(async move { consumer2.run_dlq::<WorkTopic>(dlq_handler_clone).await });

    let reached = dlq_handler
        .wait_for_count(1, Duration::from_secs(15))
        .await;

    setup.sns_client.shutdown().await;
    h2.await.expect("DLQ consumer task should not panic").ok();

    assert!(reached, "DLQ handler should have received 1 dead message");
    assert_eq!(dlq_handler.count(), 1);
}