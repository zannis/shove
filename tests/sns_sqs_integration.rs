//! Integration tests for the SNS/SQS pub-sub backend.
//!
//! Migrated to `Broker<Sqs>` + `Publisher<Sqs>` + `TopologyDeclarer<Sqs>` +
//! `ConsumerSupervisor<Sqs>`. SQS does **not** implement
//! `HasCoordinatedGroups`, so tests that exercise the SQS-specific
//! `SqsConsumerGroup[Registry]` / `SqsAutoscalerBackend` plumbing stay on the
//! old API surface — the old types are still `pub` re-exports.
//!
//! Tests that need direct access to the queue URLs (e.g. to drive raw SQS
//! sends, receive from DLQs, or hand the registry to `SqsConsumer::new` for
//! bespoke consumer runs) keep using an external [`TopicRegistry`] /
//! [`QueueRegistry`] populated via the inherent `SnsTopologyDeclarer`; the
//! broker's client-owned registries are `pub(crate)` and so not visible from
//! tests. Topology is therefore declared through **both** paths: once via
//! `broker.topology()` (populates the client-owned registries used by
//! `Publisher<Sqs>`) and once via a test-owned `SnsTopologyDeclarer`
//! (populates the external registries queried by the test body). SNS/SQS
//! topology creation is idempotent so the double declaration is safe.
//!
//! Each test spins up a fresh LocalStack container via testcontainers, runs
//! the test, and drops the container on completion (automatic cleanup).
//!
//! Run with: `cargo test --features aws-sns-sqs,audit --test sns_sqs_integration`

use shove::Broker;
use shove::Sqs;
use shove::publisher::Publisher as PublisherV2;
use shove::sns::{SqsQueueStatsProviderTrait, *};
use shove::*;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers::ImageExt;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Shared wait-for-count utility
// ---------------------------------------------------------------------------

/// A thread-safe counter with async notification, used by test handlers to
/// signal progress and allow tests to wait for a target count.
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
// Test harness
// ---------------------------------------------------------------------------

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        // Set dummy credentials for LocalStack.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let auth_token = std::env::var("LOCALSTACK_AUTH_TOKEN")
            .expect("LOCALSTACK_AUTH_TOKEN must be set to run SNS/SQS integration tests");

        let container = LocalStack::default()
            .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
            .start()
            .await
            .expect("failed to start LocalStack container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");

        let endpoint_url = format!("http://localhost:{port}");

        // Give LocalStack a moment to finish initializing SNS/SQS services
        sleep(Duration::from_secs(1)).await;

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
        let deadline = Instant::now() + timeout;
        let mut all_messages = Vec::new();

        while all_messages.len() < expected_count && Instant::now() < deadline {
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
//
// Exposes:
//   - `sns_client`         — raw client for bespoke consumer construction
//   - `broker`             — `Broker<Sqs>::from_client(sns_client.clone())`
//   - `topic_registry`/`queue_registry` — external registries populated in
//     parallel via `SnsTopologyDeclarer` so tests can look up topic ARNs and
//     queue URLs (the client-owned registries are `pub(crate)`).
//   - `publisher`          — `Publisher<Sqs>` built via `broker.publisher()`,
//     resolving ARNs against the client-owned registry populated by
//     `broker.topology()`.

struct TestSetup {
    sns_client: SnsClient,
    broker: Broker<Sqs>,
    topic_registry: Arc<TopicRegistry>,
    queue_registry: Arc<QueueRegistry>,
    publisher: PublisherV2<Sqs>,
}

impl TestSetup {
    /// Create a fully wired setup sharing an existing broker.
    /// The caller's `broker` must outlive this setup.
    async fn new(broker: &TestBroker) -> Self {
        let sns_client = SnsClient::new(&broker.sns_config())
            .await
            .expect("failed to create SNS client");

        let broker = Broker::<Sqs>::from_client(sns_client.clone());
        let topic_registry = Arc::new(TopicRegistry::new());
        let queue_registry = Arc::new(QueueRegistry::new());

        let publisher = broker.publisher().await.expect("publisher construction");

        Self {
            sns_client,
            broker,
            topic_registry,
            queue_registry,
            publisher,
        }
    }

    /// Returns an external `SnsTopologyDeclarer` wired to `topic_registry`
    /// and `queue_registry` — used to populate the test-visible registries
    /// alongside the broker-owned ones.
    fn external_declarer(&self) -> SnsTopologyDeclarer {
        SnsTopologyDeclarer::new(self.sns_client.clone(), self.topic_registry.clone())
            .with_queue_registry(self.queue_registry.clone())
    }

    /// Declare the full topology for topic `T` via both the broker (populates
    /// the client-owned registry used by `self.publisher`) and the external
    /// declarer (populates `self.topic_registry` and `self.queue_registry`).
    /// SNS/SQS topology creation is idempotent so this is safe to call
    /// repeatedly.
    async fn declare<T: Topic>(&self) {
        self.broker
            .topology()
            .declare::<T>()
            .await
            .expect("broker topology declaration should succeed");
        self.external_declarer()
            .declare(T::topology())
            .await
            .expect("external topology declaration should succeed");
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

// Topic with no hold queues — for testing Defer fallback (visibility timeout 0)
define_topic!(
    DeferNoHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("sqs-defer-nohold").dlq().build()
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.clone()
    }
}

/// Handler that counts invocations and returns Ack.
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

    fn count(&self) -> u32 {
        self.counter.get()
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        self.counter.wait_for(target, timeout).await
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

/// Handler that retries the first N calls, then returns Ack.
#[derive(Clone)]
struct RetryThenAckHandler {
    retry_until: u32,
    attempt_counter: WaitableCounter,
}

impl RetryThenAckHandler {
    fn new(retry_until: u32) -> Self {
        Self {
            retry_until,
            attempt_counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for RetryThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.attempt_counter.get();
        self.attempt_counter.increment();
        if attempt < self.retry_until {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

/// Handler that sleeps for a configurable duration, then returns Ack.
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

    fn wait_for_count(&self, target: u32, timeout: Duration) -> impl Future<Output = bool> + '_ {
        self.counter.wait_for(target, timeout)
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

/// Handler that records (order_id, amount) pairs for verification.
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

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        self.counter.wait_for(target, timeout).await
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

impl MessageHandler<SeqFailAllTopic> for OrderRecordingHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.records.lock().await.push((msg.order_id, msg.amount));
        self.counter.increment();
        Outcome::Ack
    }
}

/// Handler that records dead messages for verification.
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

    fn count(&self) -> u32 {
        self.counter.get()
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        self.counter.wait_for(target, timeout).await
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

// ---------------------------------------------------------------------------
// Client lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_connect_and_shutdown() {
    let tb = TestBroker::start().await;
    let client = SnsClient::new(&tb.sns_config())
        .await
        .expect("client should connect");
    let broker = Broker::<Sqs>::from_client(client.clone());

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
    let client = SnsClient::new(&tb.sns_config())
        .await
        .expect("client should connect");
    let broker = Broker::<Sqs>::from_client(client.clone());

    let token = client.shutdown_token();
    assert!(!token.is_cancelled());

    broker.close().await;
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
// Basic publish & consume tests
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
            .run::<WorkTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(shutdown_clone)
                    .with_prefetch_count(1),
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
    struct HeaderCapture(Arc<Mutex<HashMap<String, String>>>);

    impl MessageHandler<WorkTopic> for HeaderCapture {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
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

    let captured = Arc::new(Mutex::new(HashMap::new()));
    let handler = HeaderCapture(captured.clone());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(shutdown_clone)
                    .with_prefetch_count(1),
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
            .run::<WorkTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(shutdown_clone)
                    .with_prefetch_count(10),
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
// Rejection & DLQ tests
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
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(shutdown_clone)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = broker
        .receive_messages(&sqs_client, &dlq_url, 1, Duration::from_secs(30))
        .await;

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
            .run::<WorkTopic, _>(
                reject_handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(shutdown1_clone)
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
    let h2 = tokio::spawn(async move { consumer2.run_dlq::<WorkTopic, _>(dlq_handler_clone, ()).await });

    let reached = dlq_handler.wait_for_count(1, Duration::from_secs(15)).await;

    setup.sns_client.shutdown().await;
    h2.await.expect("DLQ consumer task should not panic").ok();

    assert!(reached, "DLQ handler should have received 1 dead message");
    assert_eq!(dlq_handler.count(), 1);
}

// ---------------------------------------------------------------------------
// Retry Mechanism Tests
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
    let attempts = handler.attempt_counter.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Wait for at least 2 calls (1 retry + 1 ack)
    let reached = attempts.wait_for(2, Duration::from_secs(30)).await;
    assert!(
        reached,
        "handler should have been called at least 2 times (1 retry + 1 ack)"
    );

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");
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
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = broker
        .receive_messages(&sqs_client, &dlq_url, 1, Duration::from_secs(30))
        .await;

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    assert!(
        !dlq_messages.is_empty(),
        "exhausted-retry message should land in DLQ"
    );
}

// ---------------------------------------------------------------------------
// Defer Mechanism Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_redelivers_message() {
    struct DeferThenAck(Arc<AtomicU32>);

    impl MessageHandler<WorkTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
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
// Concurrent Consumption Tests
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
            .run::<WorkTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
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
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(1)
                    .with_prefetch_count(5),
            )
            .await
    });

    let sqs_client = broker.sqs_client().await;
    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should exist");

    let dlq_messages = broker
        .receive_messages(&sqs_client, &dlq_url, 1, Duration::from_secs(30))
        .await;

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
            .run::<WorkTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(3),
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
// Handler Timeout Test
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
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
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
// Sequenced FIFO Consumption Tests
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
            .run_fifo::<SeqSkipTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
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
        signal: Arc<Notify>,
    }

    impl MessageHandler<SeqSkipTopic> for SkipRejectHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
        signal: Arc::new(Notify::new()),
    };
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
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
        signal: Arc<Notify>,
    }

    impl MessageHandler<SeqFailAllTopic> for FailAllRejectHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
        signal: Arc::new(Notify::new()),
    };
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
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
            .run_fifo::<SeqSkipTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
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
// FIFO Deduplication Test
// ---------------------------------------------------------------------------

/// Publishing the same payload twice to a FIFO topic should result in exactly
/// one delivery. shove derives `MessageDeduplicationId` from a stable hash of
/// the serialised payload, so both attempts share the same dedup ID and SNS
/// deduplicates within its 5-minute window.
#[tokio::test]
async fn fifo_topic_deduplicates_identical_payloads() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    let msg = OrderMessage {
        order_id: "DEDUP-ORD".to_string(),
        amount: 100,
    };

    // Publish the same message twice — same payload → same dedup ID.
    setup
        .publisher
        .publish::<SeqSkipTopic>(&msg)
        .await
        .expect("first publish should succeed");
    setup
        .publisher
        .publish::<SeqSkipTopic>(&msg)
        .await
        .expect("second publish should succeed");

    let handler = OrderRecordingHandler::new();
    let handler_clone = handler.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(5),
            )
            .await
    });

    // Wait for at least one delivery.
    let reached = handler.wait_for_count(1, Duration::from_secs(15)).await;
    assert!(reached, "handler should have received at least 1 message");

    // Give extra time for a second delivery to appear (it shouldn't).
    tokio::time::sleep(Duration::from_secs(3)).await;

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let records = handler.records().await;
    assert_eq!(
        records.len(),
        1,
        "SNS FIFO should deduplicate identical payloads: expected 1 delivery, got {}",
        records.len()
    );
    assert_eq!(
        records[0].1, 100,
        "the delivered message should have amount=100"
    );
}

// ---------------------------------------------------------------------------
// Queue Stats Tests
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
// Consumer Group & Registry Tests
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
        (),
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
        (),
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
        .register::<WorkTopic, CountingHandler>(config, move || template_handler.clone(), ())
        .await
        .expect("register should succeed");

    // Topology should now be declared on the external registries (populated
    // by `SqsConsumerGroupRegistry.register`).
    assert!(
        setup.topic_registry.get("sqs-work").await.is_some(),
        "topic_registry should have 'sqs-work' entry"
    );
    assert!(
        setup.queue_registry.get("sqs-work").await.is_some(),
        "queue_registry should have 'sqs-work' entry"
    );

    // `setup.publisher` (a `Publisher<Sqs>`) resolves ARNs against the
    // client-owned registry, which the registry above did NOT populate.
    // Declare through the broker to populate it — SNS topic creation is
    // idempotent.
    setup
        .broker
        .topology()
        .declare::<WorkTopic>()
        .await
        .expect("broker topology declaration should succeed");

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
        .register::<WorkTopic, CountingHandler>(config, CountingHandler::new, ())
        .await
        .expect("first registration should succeed");

    // Second registration for the same topic should fail.
    let config2 = SqsConsumerGroupConfig::new(1..=2);
    let result = registry
        .register::<WorkTopic, CountingHandler>(config2, CountingHandler::new, ())
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
// Deserialization Failure Test
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
            .run::<WorkTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(1)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dlq_url = setup
        .queue_registry
        .get("sqs-work-dlq")
        .await
        .expect("DLQ URL should be registered");

    let dlq_messages = broker
        .receive_messages(&sqs_client, &dlq_url, 1, Duration::from_secs(30))
        .await;

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

/// A handler that blocks until a watch signal is set to `true`.
///
/// Unlike `Notify`, a `watch` channel retains its value, so handlers that
/// call `wait_for` after the sender fires `true` will return immediately
/// instead of blocking forever.
#[derive(Clone)]
struct StickyHandler {
    rx: tokio::sync::watch::Receiver<bool>,
}
struct StickySignal(tokio::sync::watch::Sender<bool>);
impl StickyHandler {
    fn new() -> (Self, StickySignal) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (Self { rx }, StickySignal(tx))
    }
}
impl StickySignal {
    fn release(&self) {
        let _ = self.0.send(true);
    }
}
impl<T: Topic> MessageHandler<T> for StickyHandler {
    type Context = ();
    async fn handle(&self, _: T::Message, _: MessageMetadata, _: &()) -> Outcome {
        let mut rx = self.rx.clone();
        rx.wait_for(|&v| v).await.ok();
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Autoscaler Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqs_autoscaler_scales_group_on_load() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // 1. Publish enough messages to trigger a scale-up.
    // Prefetch is 1, min_consumers is 1.
    // We'll publish 5 messages.
    for i in 1..=5 {
        let msg = SimpleMessage {
            id: format!("auto-{i}"),
            content: format!("message {i}"),
        };
        setup
            .publisher
            .publish::<WorkTopic>(&msg)
            .await
            .expect("publish should succeed");
    }

    // Give SQS a moment to reflect the count.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 2. Setup Registry and Group.
    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    let (sticky, sticky_signal) = StickyHandler::new();

    let config = SqsConsumerGroupConfig::new(1..=3).with_prefetch_count(1);

    registry
        .register::<WorkTopic, _>(
            config,
            {
                let h = sticky.clone();
                move || h.clone()
            },
            (),
        )
        .await
        .expect("register should succeed");

    registry.start_all();

    let registry_arc = Arc::new(Mutex::new(registry));

    // 3. Setup Autoscaler.
    let stats_provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());

    let auto_config = AutoscalerConfig {
        poll_interval: Duration::from_millis(100),
        scale_up_multiplier: 0.5,
        scale_down_multiplier: 0.9,
        hysteresis_duration: Duration::ZERO,
        cooldown_duration: Duration::ZERO,
    };

    let mut autoscaler =
        SqsAutoscalerBackend::autoscaler(stats_provider, registry_arc.clone(), auto_config);
    let shutdown = CancellationToken::new();

    // 4. Run autoscaler for a few cycles.
    let s_clone = shutdown.clone();
    let auto_handle = tokio::spawn(async move {
        autoscaler.run(s_clone).await;
    });

    // Wait for scale up.
    let mut scaled_up = false;
    let stats_provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    for i in 0..50 {
        let stats = stats_provider.get_queue_stats("sqs-work").await.unwrap();
        let reg = registry_arc.lock().await;
        let group = reg
            .groups()
            .values()
            .next()
            .expect("Registry should have at least one group");
        let active = group.active_consumers();

        if i % 10 == 0 {
            println!(
                "Iteration {i}: Ready={}, InFlight={}, Consumers={}",
                stats.messages_ready, stats.messages_not_visible, active
            );
        }

        if active > 1 {
            scaled_up = true;
            break;
        }
        drop(reg);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(scaled_up, "Autoscaler should have increased consumer count");

    // 5. Clear the load and wait for scale down.
    println!("Releasing messages for scale-down test...");
    sticky_signal.release();

    // Give some time for messages to be deleted and for SQS stats to update.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut scaled_down = false;
    for i in 0..50 {
        let stats = stats_provider.get_queue_stats("sqs-work").await.unwrap();
        let reg = registry_arc.lock().await;
        let group = reg
            .groups()
            .values()
            .next()
            .expect("Registry should have at least one group");
        let active = group.active_consumers();

        if i % 10 == 0 {
            println!(
                "Iteration {i} (Scale Down): Ready={}, InFlight={}, Consumers={}",
                stats.messages_ready, stats.messages_not_visible, active
            );
        }

        if active == 1 {
            scaled_down = true;
            break;
        }
        drop(reg);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        scaled_down,
        "Autoscaler should have decreased consumer count back to minimum"
    );

    // Cleanup
    shutdown.cancel();
    let _ = auto_handle.await;
    registry_arc.lock().await.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Multi-group Autoscaler Tests
// ---------------------------------------------------------------------------

/// Two independent consumer groups are each scaled up by the autoscaler when
/// both queues are under load. This verifies that `poll_and_scale` iterates
/// over all registered groups, not just the first one.
#[tokio::test]
async fn sqs_autoscaler_scales_multiple_groups_independently() {
    define_topic!(
        WorkTopicA,
        SimpleMessage,
        TopologyBuilder::new("sqs-autoscale-a").dlq().build()
    );

    define_topic!(
        WorkTopicB,
        SimpleMessage,
        TopologyBuilder::new("sqs-autoscale-b").dlq().build()
    );

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopicA>().await;
    setup.declare::<WorkTopicB>().await;

    // Publish enough messages to both queues to trigger scale-up.
    for i in 0..5u32 {
        setup
            .publisher
            .publish::<WorkTopicA>(&SimpleMessage {
                id: format!("a-{i}"),
                content: "payload".into(),
            })
            .await
            .expect("publish A should succeed");
        setup
            .publisher
            .publish::<WorkTopicB>(&SimpleMessage {
                id: format!("b-{i}"),
                content: "payload".into(),
            })
            .await
            .expect("publish B should succeed");
    }

    // Give SQS a moment to reflect the counts.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Build a registry with two groups — one per topic.
    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    let (sticky_a, signal_a) = StickyHandler::new();
    let (sticky_b, signal_b) = StickyHandler::new();

    registry
        .register::<WorkTopicA, _>(
            SqsConsumerGroupConfig::new(1..=3).with_prefetch_count(1),
            {
                let h = sticky_a.clone();
                move || h.clone()
            },
            (),
        )
        .await
        .expect("register A should succeed");

    registry
        .register::<WorkTopicB, _>(
            SqsConsumerGroupConfig::new(1..=3).with_prefetch_count(1),
            {
                let h = sticky_b.clone();
                move || h.clone()
            },
            (),
        )
        .await
        .expect("register B should succeed");

    registry.start_all();

    let registry_arc = Arc::new(Mutex::new(registry));

    // Autoscaler with fast polling and no hysteresis/cooldown.
    let stats_provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let auto_config = AutoscalerConfig {
        poll_interval: Duration::from_millis(100),
        scale_up_multiplier: 0.5,
        scale_down_multiplier: 0.9,
        hysteresis_duration: Duration::ZERO,
        cooldown_duration: Duration::ZERO,
    };
    let mut autoscaler =
        SqsAutoscalerBackend::autoscaler(stats_provider, registry_arc.clone(), auto_config);
    let shutdown = CancellationToken::new();

    let s = shutdown.clone();
    let auto_handle = tokio::spawn(async move {
        autoscaler.run(s).await;
    });

    // Wait for both groups to scale above 1 consumer.
    let mut a_scaled = false;
    let mut b_scaled = false;

    for _ in 0..50 {
        let reg = registry_arc.lock().await;
        let a = reg
            .groups()
            .get(WorkTopicA::topology().queue())
            .map(|g| g.active_consumers())
            .unwrap_or(0);
        let b = reg
            .groups()
            .get(WorkTopicB::topology().queue())
            .map(|g| g.active_consumers())
            .unwrap_or(0);
        drop(reg);

        if a > 1 {
            a_scaled = true;
        }
        if b > 1 {
            b_scaled = true;
        }
        if a_scaled && b_scaled {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(a_scaled, "autoscaler should have scaled up group A");
    assert!(b_scaled, "autoscaler should have scaled up group B");

    // Unblock sticky handlers so all messages get acked before shutdown.
    signal_a.release();
    signal_b.release();

    shutdown.cancel();
    let _ = auto_handle.await;
    registry_arc.lock().await.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Error Handling Tests
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
        .run::<WorkTopic, _>(
            handler,
            (),
            ConsumerOptions::<Sqs>::new().with_shutdown(shutdown),
        )
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
    let result = consumer.run_dlq::<NoDlqTopic, _>(handler, ()).await;

    assert!(
        result.is_err(),
        "run_dlq should return Err for topic without DLQ"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no DLQ configured") || err_msg.contains("DLQ"),
        "error should mention DLQ, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Defer — no hold queues (visibility timeout 0 fallback)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_without_hold_queues_redelivers() {
    struct DeferThenAck(Arc<AtomicU32>);

    impl MessageHandler<DeferNoHoldTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    setup.declare::<DeferNoHoldTopic>().await;

    let msg = SimpleMessage {
        id: "defer-nohold".to_string(),
        content: "defer without hold queues".to_string(),
    };
    setup
        .publisher
        .publish::<DeferNoHoldTopic>(&msg)
        .await
        .expect("publish should succeed");

    let calls = Arc::new(AtomicU32::new(0));
    let handler = DeferThenAck(calls.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<DeferNoHoldTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Defer (visibility timeout 0) → immediate redelivery → Ack
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
        "expected at least 2 calls (1 defer + 1 ack)"
    );
}

// ---------------------------------------------------------------------------
// Defer preserves retry count (SNS/SQS)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_preserves_retry_count() {
    struct RetryThenDefer {
        calls: Arc<AtomicU32>,
        retry_counts: Arc<std::sync::Mutex<Vec<u32>>>,
    }

    impl MessageHandler<WorkTopic> for RetryThenDefer {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            self.retry_counts.lock().unwrap().push(meta.retry_count);
            match call {
                0 => Outcome::Retry, // retry_count=0 → will become 1
                1 => Outcome::Defer, // retry_count=1 → should stay 1
                _ => Outcome::Ack,   // retry_count should still be 1
            }
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    let msg = SimpleMessage {
        id: "retry-defer".to_string(),
        content: "retry then defer".to_string(),
    };
    setup
        .publisher
        .publish::<WorkTopic>(&msg)
        .await
        .expect("publish should succeed");

    let calls = Arc::new(AtomicU32::new(0));
    let retry_counts: Arc<std::sync::Mutex<Vec<u32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = RetryThenDefer {
        calls: calls.clone(),
        retry_counts: retry_counts.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if calls.load(Ordering::Relaxed) >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("should complete retry→defer→ack cycle within timeout");

    let counts = retry_counts.lock().unwrap().clone();
    assert_eq!(counts.len(), 3, "expected 3 deliveries, got {counts:?}");
    assert_eq!(counts[0], 0, "first delivery should have retry_count=0");
    assert_eq!(counts[1], 1, "after Retry, retry_count should be 1");
    assert_eq!(counts[2], 1, "after Defer, retry_count should still be 1");

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");
}

// ---------------------------------------------------------------------------
// Sequenced consumer — defer redelivers (SNS/SQS)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequenced_defer_redelivers() {
    struct SeqDeferThenAck {
        calls: Arc<AtomicU32>,
        acks: WaitableCounter,
    }

    impl MessageHandler<SeqSkipTopic> for SeqDeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let prev = self.calls.fetch_add(1, Ordering::Relaxed);
            if prev == 0 {
                Outcome::Defer
            } else {
                self.acks.increment();
                Outcome::Ack
            }
        }
    }

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SeqSkipTopic>().await;

    let msg = OrderMessage {
        order_id: "SEQ-DEFER-1".to_string(),
        amount: 100,
    };
    setup
        .publisher
        .publish::<SeqSkipTopic>(&msg)
        .await
        .expect("publish should succeed");

    let calls = Arc::new(AtomicU32::new(0));
    let acks = WaitableCounter::new();
    let handler = SeqDeferThenAck {
        calls: calls.clone(),
        acks: acks.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = SqsConsumer::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        acks.wait_for(1, Duration::from_secs(30)).await,
        "timed out waiting for ack after sequenced defer"
    );

    assert!(
        calls.load(Ordering::Relaxed) >= 2,
        "expected at least 2 deliveries (1 defer + 1 ack)"
    );

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");
}

// ---------------------------------------------------------------------------
// Pluggable Strategy Integration Tests
// ---------------------------------------------------------------------------

/// A custom strategy that scales up by 1 whenever there are any ready messages,
/// regardless of capacity or thresholds.
struct AlwaysScaleUpStrategy;

impl ScalingStrategy for AlwaysScaleUpStrategy {
    fn evaluate(&mut self, _group: &str, metrics: &ScalingMetrics) -> ScalingDecision {
        if metrics.messages_ready > 0 {
            ScalingDecision::ScaleUp(1)
        } else {
            ScalingDecision::Hold
        }
    }
}

/// Tests that a custom (non-Threshold) strategy works with the generic autoscaler.
///
/// We use prefetch=5, min=1, max=4. With 3 messages ready:
/// - `ThresholdStrategy` with default multiplier 2.0 would require >10 messages to scale up
///   (capacity = 1 * 5 = 5, threshold = 5 * 2.0 = 10).
/// - `AlwaysScaleUpStrategy` fires immediately on any ready message.
#[tokio::test]
async fn autoscaler_custom_strategy_with_sqs() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Publish 8 messages. With prefetch=5 and 1 consumer, the consumer takes 5
    // in-flight, leaving 3 as messages_ready. This is below the default
    // ThresholdStrategy scale-up threshold (capacity=5, threshold=5*2.0=10),
    // but AlwaysScaleUpStrategy fires for any messages_ready > 0.
    for i in 1..=8 {
        setup
            .publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("custom-{i}"),
                content: format!("message {i}"),
            })
            .await
            .expect("publish should succeed");
    }

    // Give SQS a moment to reflect the count.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Set up registry with prefetch=5 so threshold would be 10 (3 remaining < 10).
    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    let (sticky, sticky_signal) = StickyHandler::new();
    let config = SqsConsumerGroupConfig::new(1..=4).with_prefetch_count(5);

    registry
        .register::<WorkTopic, _>(
            config,
            {
                let h = sticky.clone();
                move || h.clone()
            },
            (),
        )
        .await
        .expect("register should succeed");

    registry.start_all();

    let registry_arc = Arc::new(Mutex::new(registry));

    // Build autoscaler with custom strategy — NO Stabilized wrapper.
    let stats_provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let backend = SqsAutoscalerBackend::new(stats_provider, registry_arc.clone());
    let mut autoscaler =
        Autoscaler::new(backend, AlwaysScaleUpStrategy, Duration::from_millis(200));

    let shutdown = CancellationToken::new();
    let s_clone = shutdown.clone();
    let auto_handle = tokio::spawn(async move {
        autoscaler.run(s_clone).await;
    });

    // Wait up to 20s for scale-up triggered by the custom strategy.
    let mut scaled_up = false;
    for _ in 0..100 {
        let reg = registry_arc.lock().await;
        let active = reg
            .groups()
            .values()
            .next()
            .expect("registry should have at least one group")
            .active_consumers();
        drop(reg);

        if active >= 2 {
            scaled_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        scaled_up,
        "custom strategy should have scaled up despite low message count"
    );

    // Cleanup
    sticky_signal.release();
    shutdown.cancel();
    let _ = auto_handle.await;
    registry_arc.lock().await.shutdown_all().await;
}

/// A strategy that always requests scale-up by 2 at once when there are ready messages.
struct ScaleByTwoStrategy;

impl ScalingStrategy for ScaleByTwoStrategy {
    fn evaluate(&mut self, _group: &str, metrics: &ScalingMetrics) -> ScalingDecision {
        if metrics.messages_ready > 0 {
            ScalingDecision::ScaleUp(2)
        } else {
            ScalingDecision::Hold
        }
    }
}

/// Tests that `ScaleUp(2)` actually adds 2 consumers at once, so after the
/// first scaling action we go from 1 (initial) to 3 consumers.
#[tokio::test]
async fn autoscaler_scale_magnitude_with_sqs() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<WorkTopic>().await;

    // Publish 10 messages.
    for i in 1..=10 {
        setup
            .publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("scale2-{i}"),
                content: format!("message {i}"),
            })
            .await
            .expect("publish should succeed");
    }

    // Give SQS a moment to reflect the count.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Set up registry: min=1, max=5, prefetch=5. Starts with 1 consumer.
    let mut registry = SqsConsumerGroupRegistry::new(
        setup.sns_client.clone(),
        setup.topic_registry.clone(),
        setup.queue_registry.clone(),
    );

    let (sticky, sticky_signal) = StickyHandler::new();
    let config = SqsConsumerGroupConfig::new(1..=5).with_prefetch_count(5);

    registry
        .register::<WorkTopic, _>(
            config,
            {
                let h = sticky.clone();
                move || h.clone()
            },
            (),
        )
        .await
        .expect("register should succeed");

    registry.start_all();

    let registry_arc = Arc::new(Mutex::new(registry));

    // Build autoscaler with ScaleByTwoStrategy — NO Stabilized wrapper.
    let stats_provider =
        SqsQueueStatsProvider::new(setup.sns_client.clone(), setup.queue_registry.clone());
    let backend = SqsAutoscalerBackend::new(stats_provider, registry_arc.clone());
    let mut autoscaler = Autoscaler::new(backend, ScaleByTwoStrategy, Duration::from_millis(200));

    let shutdown = CancellationToken::new();
    let s_clone = shutdown.clone();
    let auto_handle = tokio::spawn(async move {
        autoscaler.run(s_clone).await;
    });

    // Wait up to 20s for 3 active consumers (1 initial + 2 from ScaleUp(2)).
    let mut reached_three = false;
    for _ in 0..100 {
        let reg = registry_arc.lock().await;
        let active = reg
            .groups()
            .values()
            .next()
            .expect("registry should have at least one group")
            .active_consumers();
        drop(reg);

        if active >= 3 {
            reached_three = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        reached_three,
        "ScaleUp(2) should have brought active consumers from 1 to at least 3"
    );

    // Cleanup
    sticky_signal.release();
    shutdown.cancel();
    let _ = auto_handle.await;
    registry_arc.lock().await.shutdown_all().await;
}
