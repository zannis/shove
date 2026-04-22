//! Integration tests for the RabbitMQ backend.
//!
//! Migrated to `Broker<RabbitMq>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `RabbitMqConsumer` constructed from
//! the underlying `RabbitMqClient`.
//!
//! Run with: `cargo test --features rabbitmq,audit --test rabbitmq_integration`

use lapin::options::BasicPublishOptions;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::rabbitmq::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
// Explicit import to disambiguate from shove::consumer_group::ConsumerGroupConfig (the wrapper).
use shove::rabbitmq::ConsumerGroupConfig;
use shove::*;

use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test harness: shared setup
// ---------------------------------------------------------------------------

struct TestBroker {
    container: testcontainers::ContainerAsync<RabbitMq>,
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
            container,
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

    fn broker_from(&self, client: RabbitMqClient) -> Broker<RabbitMqMarker> {
        Broker::<RabbitMqMarker>::from_client(client)
    }

    async fn stop(self) {
        self.container
            .stop_with_timeout(Some(0))
            .await
            .expect("failed to stop container");
        self.container
            .rm()
            .await
            .expect("failed to remove container");
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
        .sequenced(SequenceFailure::Skip)
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

// Topic for sequenced defer with no hold queues (allow_message_loss)
define_sequenced_topic!(
    DeferSeqNoHold,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-defer-seq-nohold")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .allow_message_loss()
        .build()
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

// Topic for handler timeout tests
define_topic!(
    TimeoutWork,
    SimpleMessage,
    TopologyBuilder::new("test-timeout")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for concurrent consumer tests
define_topic!(
    ConcurrentWork,
    SimpleMessage,
    TopologyBuilder::new("test-concurrent")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for concurrent consumer DLQ/reject tests
define_topic!(
    ConcurrentRejectWork,
    SimpleMessage,
    TopologyBuilder::new("test-concurrent-reject")
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
        .sequenced(SequenceFailure::FailAll)
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
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .build()
);

// Topics for ungraceful shutdown recovery tests
define_topic!(
    UngracefulSeq1,
    SimpleMessage,
    TopologyBuilder::new("test-ungraceful-seq-1")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

define_topic!(
    UngracefulSeq3,
    SimpleMessage,
    TopologyBuilder::new("test-ungraceful-seq-3")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

define_topic!(
    UngracefulConc1,
    SimpleMessage,
    TopologyBuilder::new("test-ungraceful-conc-1")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

define_topic!(
    UngracefulConc3,
    SimpleMessage,
    TopologyBuilder::new("test-ungraceful-conc-3")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for concurrent consumer retry tests
define_topic!(
    ConcurrentRetryWork,
    SimpleMessage,
    TopologyBuilder::new("test-concurrent-retry")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for concurrent consumer max-retry / DLQ tests
define_topic!(
    ConcurrentMaxRetry,
    SimpleMessage,
    TopologyBuilder::new("test-concurrent-maxretry")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Message type and topic for deserialization-failure DLQ tests
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct StrictMessage {
    value: u64,
}

define_topic!(
    StrictWork,
    StrictMessage,
    TopologyBuilder::new("test-strict")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .build()
);

// Topic for sequenced consumer retry via shard hold queues
define_sequenced_topic!(
    RetrySeqOrders,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-retry-seq-orders")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .build()
);

// Topic for sequenced consumer defer via shard hold queues
define_sequenced_topic!(
    DeferSeqOrders,
    OrderMessage,
    |msg: &OrderMessage| msg.account.clone(),
    TopologyBuilder::new("test-defer-seq-orders")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(SequenceFailure::Skip)
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<OrderTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<ScalableWork> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<AuditedWork> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<SkipOrders> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<FailAllOrders> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler that simulates slow work with configurable latency.
/// Records completion timestamps to verify in-order acking.
#[derive(Clone)]
struct SlowCountingHandler {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
    delay: Duration,
}

impl SlowCountingHandler {
    fn new(delay: Duration) -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
            delay,
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

impl MessageHandler<ConcurrentWork> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<SimpleWork> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulSeq1> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulSeq3> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulConc1> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulConc3> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<SkipOrders> for SlowCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Handler for concurrent consumer tests that returns mixed outcomes.
/// Messages with body "reject-N" are rejected, others are acked.
#[derive(Clone)]
struct ConcurrentMixedHandler {
    ack_count: Arc<AtomicU32>,
    reject_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl ConcurrentMixedHandler {
    fn new() -> Self {
        Self {
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

    fn total(&self) -> u32 {
        self.ack_count() + self.reject_count()
    }

    async fn wait_for_total(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.total() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.total() >= target;
                }
            }
        }
    }
}

impl MessageHandler<ConcurrentRejectWork> for ConcurrentMixedHandler {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(10)).await;
        if msg.body.starts_with("reject") {
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

impl MessageHandler<ConcurrentWork> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulSeq1> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulSeq3> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulConc1> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<UngracefulConc3> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<ConcurrentRejectWork> for DlqCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

/// Handler that sleeps longer than the timeout on first delivery, then acks on retry.
/// Tracks total delivery count to verify the message was retried.
#[derive(Clone)]
struct TimeoutThenAckHandler {
    delivery_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
    slow_duration: Duration,
}

impl TimeoutThenAckHandler {
    fn new(slow_duration: Duration) -> Self {
        Self {
            delivery_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
            slow_duration,
        }
    }

    fn delivery_count(&self) -> u32 {
        self.delivery_count.load(Ordering::Relaxed)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.delivery_count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.delivery_count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<TimeoutWork> for TimeoutThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.delivery_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.signal.notify_waiters();
        if attempt == 1 {
            // First delivery: sleep longer than the handler_timeout.
            // The consumer will cancel this future and route to Retry.
            tokio::time::sleep(self.slow_duration).await;
        }
        // Second+ delivery: return immediately with Ack.
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
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: OrderMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

impl MessageHandler<FailAllOrders> for OrderDlqHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: OrderMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

/// Handler that always rejects (for testing DLQ routing).
struct RejectHandler;

impl MessageHandler<SimpleWork> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

impl MessageHandler<ConcurrentRetryWork> for RetryThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

/// Handler that always returns Retry — used to exhaust max_retries and send to DLQ.
#[derive(Clone)]
struct AlwaysRetryHandler {
    attempt_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl AlwaysRetryHandler {
    fn new() -> Self {
        Self {
            attempt_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn attempt_count(&self) -> u32 {
        self.attempt_count.load(Ordering::Relaxed)
    }
}

impl MessageHandler<ConcurrentMaxRetry> for AlwaysRetryHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.attempt_count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Retry
    }
}

impl MessageHandler<RetryWork> for AlwaysRetryHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.attempt_count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Retry
    }
}

impl MessageHandler<ConcurrentMaxRetry> for DlqCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

impl MessageHandler<RetryWork> for DlqCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

impl MessageHandler<StrictWork> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: StrictMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

impl MessageHandler<StrictWork> for DlqCountingHandler {
    type Context = ();
    async fn handle(&self, _msg: StrictMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: StrictMessage, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }
}

impl MessageHandler<ConcurrentWork> for TimeoutThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.delivery_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.signal.notify_waiters();
        if attempt == 1 {
            tokio::time::sleep(self.slow_duration).await;
        }
        Outcome::Ack
    }
}

/// Handler that returns Retry on first delivery, then Ack.
#[derive(Clone)]
struct SeqRetryHandler {
    ack_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl SeqRetryHandler {
    fn new() -> Self {
        Self {
            ack_count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }

    async fn wait_for_ack(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.ack_count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.ack_count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<RetrySeqOrders> for SeqRetryHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        if meta.retry_count == 0 {
            Outcome::Retry
        } else {
            self.ack_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }
}

/// Handler that returns Defer on first delivery, then Ack.
#[derive(Clone)]
struct SeqDeferHandler {
    call_count: Arc<AtomicU32>,
    ack_count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl SeqDeferHandler {
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

    async fn wait_for_ack(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.ack_count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.ack_count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<DeferSeqOrders> for SeqDeferHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

impl MessageHandler<DeferSeqNoHold> for SeqDeferHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

/// Handler that Retries once, then Defers once, then Acks — recording the
/// retry_count seen on each delivery to verify Defer does not increment it.
#[derive(Clone)]
struct RetryThenDeferHandler {
    call_count: Arc<AtomicU32>,
    ack_count: Arc<AtomicU32>,
    retry_counts: Arc<std::sync::Mutex<Vec<u32>>>,
    signal: Arc<tokio::sync::Notify>,
}

impl RetryThenDeferHandler {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicU32::new(0)),
            ack_count: Arc::new(AtomicU32::new(0)),
            retry_counts: Arc::new(std::sync::Mutex::new(Vec::new())),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn ack_count(&self) -> u32 {
        self.ack_count.load(Ordering::Relaxed)
    }

    fn retry_counts(&self) -> Vec<u32> {
        self.retry_counts.lock().unwrap().clone()
    }
}

impl MessageHandler<DeferWithHold> for RetryThenDeferHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        let call = self.call_count.fetch_add(1, Ordering::Relaxed);
        self.retry_counts.lock().unwrap().push(meta.retry_count);
        match call {
            0 => Outcome::Retry, // retry_count=0 → will become 1
            1 => Outcome::Defer, // retry_count=1 → should stay 1
            _ => {
                self.ack_count.fetch_add(1, Ordering::Relaxed);
                self.signal.notify_waiters();
                Outcome::Ack // retry_count should still be 1
            }
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
    broker.stop().await;
}

#[tokio::test]
async fn client_shutdown_cancels_token() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let token = client.shutdown_token();

    assert!(!token.is_cancelled());
    client.shutdown().await;
    assert!(token.is_cancelled());
    broker.stop().await;
}

// --- Topology ---

#[tokio::test]
async fn topology_declare_creates_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

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
    broker.stop().await;
}

#[tokio::test]
async fn topology_declare_sequenced_creates_sub_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<OrderTopic>().await.unwrap();

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
    broker.stop().await;
}

// --- Publisher ---

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    // Publish
    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 1);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

#[tokio::test]
async fn publish_with_headers() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

#[tokio::test]
async fn publish_batch() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(5, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 5);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Reject → DLQ ---

#[tokio::test]
async fn rejected_message_lands_in_dlq() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    // Publish one message
    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork, _>(RejectHandler, (), opts).await
    });

    // Give it time to reject
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    reject_handle.await.unwrap().unwrap();

    // Now consume from DLQ
    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<SimpleWork, _>(dh, ()).await });

    assert!(dlq_handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Management API ---

#[tokio::test]
async fn management_client_fetches_queue_stats() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    // Publish some messages
    let publisher = b.publisher().await.unwrap();
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
    broker.stop().await;
}

// --- Consumer Group + Registry ---

#[tokio::test]
async fn registry_register_declares_topology_and_starts() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    // register auto-declares topology
    registry
        .register::<SimpleWork, _>(ConsumerGroupConfig::new(1..=3), move || h.clone(), ())
        .await
        .unwrap();

    registry.start_all();

    // Publish a message — the consumer group should pick it up
    let publisher = b.publisher().await.unwrap();
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
    broker.stop().await;
}

// --- Sequenced consumer ---

#[tokio::test]
async fn sequenced_consume_preserves_order() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<OrderTopic>().await.unwrap();

    let publisher = b.publisher().await.unwrap();

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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<OrderTopic, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(5, Duration::from_secs(15)).await);
    assert_eq!(handler.count(), 5);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Retry via hold queue ---

#[tokio::test]
async fn retry_via_hold_queue_then_ack() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<RetryWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<RetryWork, _>(h, (), opts).await
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
    broker.stop().await;
}

// --- Defer with hold queue ---

#[tokio::test]
async fn defer_with_hold_queue_redelivers() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<DeferWithHold>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<DeferWithHold, _>(h, (), opts).await
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
    broker.stop().await;
}

// --- Defer without hold queue (nack+requeue fallback) ---

#[tokio::test]
async fn defer_without_hold_queue_requeues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<DeferNoHold>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<DeferNoHold, _>(h, (), opts).await
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
    broker.stop().await;
}

// --- Autoscaler scales up under load ---

#[tokio::test]
async fn autoscaler_scales_up_under_load() {
    use tokio::sync::Mutex;

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    // Use min_consumers=0 so start_all spawns no consumers, letting messages
    // accumulate in the queue so the autoscaler sees the load and scales up.
    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(0..=4).with_prefetch_count(5),
            move || h.clone(),
            (),
        )
        .await
        .unwrap();
    registry.start_all(); // starts 0 consumers

    // Publish 100 messages — above scale_up threshold (5 * 1 * 2.0 = 10)
    // even after the first scale-up event adds one consumer.
    let publisher = b.publisher().await.unwrap();
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
    let token_clone = autoscaler_token.clone();
    let mgmt_config = broker.mgmt_config();
    let autoscaler_handle = tokio::spawn({
        let registry = registry.clone();
        async move {
            let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
                &mgmt_config,
                registry,
                AutoscalerConfig {
                    poll_interval: Duration::from_secs(1),
                    hysteresis_duration: Duration::from_secs(1),
                    cooldown_duration: Duration::from_secs(1),
                    ..AutoscalerConfig::default()
                },
            );
            autoscaler.run(token_clone).await;
        }
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
    broker.stop().await;
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
            (),
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
    let token_clone = autoscaler_token.clone();
    let mgmt_config = broker.mgmt_config();
    let autoscaler_handle = tokio::spawn({
        let registry = registry.clone();
        async move {
            let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
                &mgmt_config,
                registry,
                AutoscalerConfig {
                    poll_interval: Duration::from_secs(1),
                    hysteresis_duration: Duration::from_secs(1),
                    cooldown_duration: Duration::from_secs(1),
                    ..AutoscalerConfig::default()
                },
            );
            autoscaler.run(token_clone).await;
        }
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
    broker.stop().await;
}

// --- Autoscaler custom strategy (pluggable) ---

/// Slow handler that takes 2s per message, keeping messages queued long enough
/// for the autoscaler to observe them.
#[derive(Clone)]
struct SlowScalableHandler;

impl MessageHandler<ScalableWork> for SlowScalableHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Outcome::Ack
    }
}

#[tokio::test]
async fn autoscaler_custom_strategy_pluggable() {
    use tokio::sync::Mutex;

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

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(1..=4).with_prefetch_count(5),
            || SlowScalableHandler,
            (),
        )
        .await
        .unwrap();
    registry.start_all();

    // Publish 20 messages. With a 2s handler and prefetch=5, one consumer
    // can only process ~2.5 msg/s — plenty stay queued for the autoscaler.
    // 5 messages would be below the default ThresholdStrategy scale-up
    // threshold (capacity=5, threshold=10), but AlwaysScaleUpStrategy fires
    // for any messages_ready > 0.
    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..20)
        .map(|i| SimpleMessage {
            body: format!("custom-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ScalableWork>(&messages)
        .await
        .unwrap();

    // Give management API time to reflect the queued messages
    tokio::time::sleep(Duration::from_secs(2)).await;

    let registry = Arc::new(Mutex::new(registry));
    let mgmt_config = broker.mgmt_config();
    let backend = RabbitMqAutoscalerBackend::new(&mgmt_config, registry.clone());
    let mut autoscaler =
        Autoscaler::new(backend, AlwaysScaleUpStrategy, Duration::from_millis(500));

    let autoscaler_token = CancellationToken::new();
    let token_clone = autoscaler_token.clone();
    let autoscaler_handle = tokio::spawn(async move {
        autoscaler.run(token_clone).await;
    });

    // Wait up to 30s for at least 2 active consumers (1 initial + 1 from custom strategy)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let active = {
            let reg = registry.lock().await;
            reg.groups()["test-scalable"].active_consumers()
        };
        if active >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out: custom strategy did not trigger scale-up (active_consumers={})",
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
        active >= 2,
        "expected at least 2 consumers after custom strategy scale-up, got {active}"
    );

    autoscaler_token.cancel();
    autoscaler_handle.await.unwrap();

    let mut reg = registry.lock().await;
    reg.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

// --- Autoscaler without Stabilized scales immediately ---

#[tokio::test]
async fn autoscaler_without_stabilization_scales_immediately() {
    use tokio::sync::Mutex;

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(0..=4).with_prefetch_count(5),
            move || h.clone(),
            (),
        )
        .await
        .unwrap();
    registry.start_all(); // starts 0 consumers

    // Publish 100 messages to ensure the threshold is crossed on every poll
    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..100)
        .map(|i| SimpleMessage {
            body: format!("no-stab-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ScalableWork>(&messages)
        .await
        .unwrap();

    // Give management API time to reflect the queued messages
    tokio::time::sleep(Duration::from_secs(2)).await;

    let registry = Arc::new(Mutex::new(registry));
    let mgmt_config = broker.mgmt_config();
    let backend = RabbitMqAutoscalerBackend::new(&mgmt_config, registry.clone());
    // Raw ThresholdStrategy with no Stabilized wrapper — no hysteresis/cooldown delay
    let mut autoscaler = Autoscaler::new(
        backend,
        ThresholdStrategy::default(),
        Duration::from_millis(500),
    );

    let autoscaler_token = CancellationToken::new();
    let token_clone = autoscaler_token.clone();
    let autoscaler_handle = tokio::spawn(async move {
        autoscaler.run(token_clone).await;
    });

    // Wait up to 10s for >= 2 active consumers
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let active = {
            let reg = registry.lock().await;
            reg.groups()["test-scalable"].active_consumers()
        };
        if active >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out: unstabilized autoscaler did not scale up (active_consumers={})",
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
        active >= 2,
        "expected at least 2 consumers with no stabilization, got {active}"
    );

    autoscaler_token.cancel();
    autoscaler_handle.await.unwrap();

    let mut reg = registry.lock().await;
    reg.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

// --- Autoscaler ScaleUp magnitude > 1 ---

#[tokio::test]
async fn autoscaler_scale_up_magnitude() {
    use tokio::sync::Mutex;

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

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    // Use SlowScalableHandler (2s per message) so the queue stays deep.
    let mut registry = ConsumerGroupRegistry::new(client.clone());
    registry
        .register::<ScalableWork, _>(
            ConsumerGroupConfig::new(1..=5).with_prefetch_count(5),
            || SlowScalableHandler,
            (),
        )
        .await
        .unwrap();
    registry.start_all(); // starts 1 consumer

    // Publish 50 messages — with 2s handler, they stay queued for many poll cycles.
    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..50)
        .map(|i| SimpleMessage {
            body: format!("mag-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ScalableWork>(&messages)
        .await
        .unwrap();

    // Give management API time to reflect the queued messages
    tokio::time::sleep(Duration::from_secs(2)).await;

    let registry = Arc::new(Mutex::new(registry));
    let mgmt_config = broker.mgmt_config();
    let backend = RabbitMqAutoscalerBackend::new(&mgmt_config, registry.clone());
    let mut autoscaler = Autoscaler::new(backend, ScaleByTwoStrategy, Duration::from_millis(500));

    let autoscaler_token = CancellationToken::new();
    let token_clone = autoscaler_token.clone();
    let autoscaler_handle = tokio::spawn(async move {
        autoscaler.run(token_clone).await;
    });

    // Wait up to 30s for >= 3 consumers (1 initial + 2 from single ScaleUp(2))
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let active = {
            let reg = registry.lock().await;
            reg.groups()["test-scalable"].active_consumers()
        };
        if active >= 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out: ScaleUp(2) did not add 2 consumers (active_consumers={})",
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
        active >= 3,
        "expected at least 3 consumers after ScaleUp(2), got {active}"
    );

    autoscaler_token.cancel();
    autoscaler_handle.await.unwrap();

    let mut reg = registry.lock().await;
    reg.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
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
        async fn audit(&self, record: &AuditRecord<SimpleMessage>) -> error::Result<()> {
            self.records.lock().unwrap().push(record.clone());
            self.signal.notify_waiters();
            Ok(())
        }
    }

    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<AuditedWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<AuditedWork, _>(audited, (), opts).await
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
    broker.stop().await;
}

// --- Sequenced: Skip mode ---

/// With SequenceFailure::Skip, rejecting one message should NOT affect
/// subsequent messages for the same sequence key. All non-rejected messages
/// should be acked, and only the rejected one lands in the DLQ.
#[tokio::test]
async fn sequenced_skip_continues_after_rejection() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SkipOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();

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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<SkipOrders, _>(h, (), opts).await
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
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<SkipOrders, _>(dh, ()).await });

    assert!(
        dlq_handler.wait_for_count(1, Duration::from_secs(10)).await,
        "expected 1 DLQ message"
    );
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Sequenced: FailAll mode ---

/// With SequenceFailure::FailAll, rejecting one message should "poison" the
/// sequence key — all subsequent messages for that key are auto-rejected to DLQ.
/// Messages for OTHER keys should be unaffected.
#[tokio::test]
async fn sequenced_failall_poisons_key_after_rejection() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<FailAllOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();

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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<FailAllOrders, _>(h, (), opts).await
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
    let dlq_handle =
        tokio::spawn(async move { consumer2.run_dlq::<FailAllOrders, _>(dh, ()).await });

    assert!(
        dlq_handler.wait_for_count(4, Duration::from_secs(10)).await,
        "expected at least 4 DLQ messages, got {}",
        dlq_handler.count()
    );

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// ===========================================================================
// Concurrent consumer tests
// ===========================================================================

/// Basic concurrent consumption: all messages are processed and acked.
#[tokio::test]
async fn concurrent_consume_processes_all_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..20)
        .map(|i| SimpleMessage {
            body: format!("msg-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ConcurrentWork>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<ConcurrentWork, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_count(20, Duration::from_secs(15)).await,
        "timed out: processed {} / 20",
        handler.count()
    );
    assert_eq!(handler.count(), 20);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

/// Concurrent consumption with slow handlers is faster than sequential.
#[tokio::test]
async fn concurrent_consume_slow_handler_faster_than_sequential() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let msg_count = 10u32;
    let handler_delay = Duration::from_millis(100);

    let messages: Vec<SimpleMessage> = (0..msg_count)
        .map(|i| SimpleMessage {
            body: format!("slow-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<SimpleWork>(&messages)
        .await
        .unwrap();

    let handler = SlowCountingHandler::new(handler_delay);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();

    let start = Instant::now();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(msg_count as u16); // all in-flight at once
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(
        handler
            .wait_for_count(msg_count, Duration::from_secs(15))
            .await,
        "timed out: processed {} / {msg_count}",
        handler.count()
    );
    let concurrent_duration = start.elapsed();

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    // Sequential would take at least msg_count * handler_delay = 1000ms.
    // Concurrent should finish significantly faster (all 10 overlap).
    let sequential_min = handler_delay * msg_count;
    assert!(
        concurrent_duration < sequential_min,
        "concurrent ({concurrent_duration:?}) should be faster than sequential minimum ({sequential_min:?})"
    );

    client.shutdown().await;
    broker.stop().await;
}

/// Concurrent consumption with prefetch_count=1 behaves like sequential.
#[tokio::test]
async fn concurrent_consume_prefetch_one_is_sequential() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("seq-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ConcurrentWork>(&messages)
        .await
        .unwrap();

    let handler = SlowCountingHandler::new(Duration::from_millis(50));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();

    let start = Instant::now();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1); // forces sequential
        consumer.run::<ConcurrentWork, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_count(5, Duration::from_secs(15)).await,
        "timed out: processed {} / 5",
        handler.count()
    );
    let duration = start.elapsed();

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    // With prefetch=1, it should be sequential: at least 5 * 50ms = 250ms
    assert!(
        duration >= Duration::from_millis(200),
        "prefetch=1 should be sequential, took only {duration:?}"
    );

    client.shutdown().await;
    broker.stop().await;
}

/// Concurrent consumption correctly routes rejected messages to DLQ.
#[tokio::test]
async fn concurrent_consume_mixed_outcomes_routes_correctly() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology()
        .declare::<ConcurrentRejectWork>()
        .await
        .unwrap();

    let publisher = b.publisher().await.unwrap();
    // 7 ack, 3 reject
    let mut messages = Vec::new();
    for i in 0..10 {
        let body = if i % 3 == 0 && i > 0 {
            format!("reject-{i}")
        } else {
            format!("ok-{i}")
        };
        messages.push(SimpleMessage { body });
    }
    publisher
        .publish_batch::<ConcurrentRejectWork>(&messages)
        .await
        .unwrap();

    let handler = ConcurrentMixedHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<ConcurrentRejectWork, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_total(10, Duration::from_secs(15)).await,
        "timed out: total {} / 10",
        handler.total()
    );
    assert_eq!(handler.ack_count(), 7, "expected 7 acks");
    assert_eq!(handler.reject_count(), 3, "expected 3 rejects");

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    // Verify rejected messages are in DLQ
    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle =
        tokio::spawn(async move { consumer2.run_dlq::<ConcurrentRejectWork, _>(dh, ()).await });

    assert!(
        dlq_handler.wait_for_count(3, Duration::from_secs(10)).await,
        "expected 3 DLQ messages, got {}",
        dlq_handler.count()
    );

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

/// Concurrent consumption via consumer group with concurrent_processing enabled.
#[tokio::test]
async fn consumer_group_concurrent_processing() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    registry
        .register::<ConcurrentWork, _>(
            ConsumerGroupConfig::new(2..=2)
                .with_prefetch_count(10)
                .with_concurrent_processing(true),
            move || h.clone(),
            (),
        )
        .await
        .unwrap();

    registry.start_all();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..30)
        .map(|i| SimpleMessage {
            body: format!("group-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ConcurrentWork>(&messages)
        .await
        .unwrap();

    assert!(
        handler.wait_for_count(30, Duration::from_secs(15)).await,
        "timed out: processed {} / 30",
        handler.count()
    );
    assert_eq!(handler.count(), 30);

    registry.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

/// Concurrent consumption gracefully drains in-flight messages on shutdown.
#[tokio::test]
async fn concurrent_consume_graceful_shutdown_drains() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("drain-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<ConcurrentWork>(&messages)
        .await
        .unwrap();

    // Handler with 200ms delay — messages will be in-flight when we cancel
    let handler = SlowCountingHandler::new(Duration::from_millis(200));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(5);
        consumer.run::<ConcurrentWork, _>(h, (), opts).await
    });

    // Wait briefly for messages to be dispatched to handler tasks
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Cancel while handlers are still in-flight
    shutdown.cancel();

    // Consumer should drain all in-flight before returning
    let result = tokio::time::timeout(Duration::from_secs(5), consume_handle).await;
    assert!(result.is_ok(), "consumer should complete after draining");
    result.unwrap().unwrap().unwrap();

    // All 5 messages should have been processed (drained on shutdown)
    assert_eq!(
        handler.count(),
        5,
        "all in-flight messages should be drained"
    );

    client.shutdown().await;
    broker.stop().await;
}

// --- Handler timeout ---

/// When a handler exceeds handler_timeout, the message should be automatically
/// retried via the hold queue. On the second delivery the handler returns Ack.
#[tokio::test]
async fn handler_timeout_triggers_retry() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<TimeoutWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<TimeoutWork>(&SimpleMessage {
            body: "timeout-test".into(),
        })
        .await
        .unwrap();

    // Handler sleeps 500ms on first attempt; timeout is 100ms → Retry.
    // On second attempt (after hold queue delay) it acks immediately.
    let handler = TimeoutThenAckHandler::new(Duration::from_millis(500));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1)
            .with_handler_timeout(Duration::from_millis(100));
        consumer.run::<TimeoutWork, _>(h, (), opts).await
    });

    // Wait for at least 2 deliveries: first times out, second acks.
    // Hold queue has a 1s delay, so allow plenty of time.
    assert!(
        handler.wait_for_count(2, Duration::from_secs(15)).await,
        "expected 2 deliveries (timeout + retry), got {}",
        handler.delivery_count()
    );

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Ungraceful shutdown recovery ---

/// When a sequential consumer (prefetch=1) is aborted mid-flight, unacked
/// messages are returned to the queue and a new consumer picks them all up.
#[tokio::test]
async fn ungraceful_shutdown_sequential_prefetch_1_recovers_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<UngracefulSeq1>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("msg-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<UngracefulSeq1>(&messages)
        .await
        .unwrap();

    // Start a slow consumer — 500ms per message, so none will complete before abort
    let slow_handler = SlowCountingHandler::new(Duration::from_millis(500));
    let shutdown1 = CancellationToken::new();
    let consumer1 = RabbitMqConsumer::new(client.clone());
    let h1 = slow_handler.clone();
    let s1 = shutdown1.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s1)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer1.run::<UngracefulSeq1, _>(h1, (), opts).await
    });

    // Let the consumer pick up messages
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Abort the consumer task (ungraceful shutdown — messages not acked)
    consume_handle.abort();
    let _ = consume_handle.await;

    // No messages should have completed (500ms handler vs 100ms wait)
    assert_eq!(slow_handler.count(), 0);

    // Close the first connection so RabbitMQ releases unacked messages.
    client.shutdown().await;

    // Create a fresh client and consumer for recovery.
    let client2 = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let handler = CountingHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = RabbitMqConsumer::new(client2.clone());
    let h2 = handler.clone();
    let s2 = shutdown2.clone();
    let consume_handle2 = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s2)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer2.run::<UngracefulSeq1, _>(h2, (), opts).await
    });

    assert!(
        handler.wait_for_count(5, Duration::from_secs(10)).await,
        "expected 5 recovered messages, got {}",
        handler.count()
    );
    assert_eq!(handler.count(), 5);

    shutdown2.cancel();
    consume_handle2.await.unwrap().unwrap();
    client2.shutdown().await;
    broker.stop().await;
}

/// When a sequential consumer (prefetch=3) is aborted mid-flight, up to 3
/// unacked messages are returned to the queue and all 5 are recovered.
#[tokio::test]
async fn ungraceful_shutdown_sequential_prefetch_3_recovers_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<UngracefulSeq3>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("msg-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<UngracefulSeq3>(&messages)
        .await
        .unwrap();

    let slow_handler = SlowCountingHandler::new(Duration::from_millis(500));
    let shutdown1 = CancellationToken::new();
    let consumer1 = RabbitMqConsumer::new(client.clone());
    let h1 = slow_handler.clone();
    let s1 = shutdown1.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s1)
            .with_max_retries(3)
            .with_prefetch_count(3);
        consumer1.run::<UngracefulSeq3, _>(h1, (), opts).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    consume_handle.abort();
    let _ = consume_handle.await;

    assert_eq!(slow_handler.count(), 0);

    // Close first connection so RabbitMQ releases unacked messages.
    client.shutdown().await;

    let client2 = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let handler = CountingHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = RabbitMqConsumer::new(client2.clone());
    let h2 = handler.clone();
    let s2 = shutdown2.clone();
    let consume_handle2 = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s2)
            .with_max_retries(3)
            .with_prefetch_count(3);
        consumer2.run::<UngracefulSeq3, _>(h2, (), opts).await
    });

    assert!(
        handler.wait_for_count(5, Duration::from_secs(10)).await,
        "expected 5 recovered messages, got {}",
        handler.count()
    );
    assert_eq!(handler.count(), 5);

    shutdown2.cancel();
    consume_handle2.await.unwrap().unwrap();
    client2.shutdown().await;
    broker.stop().await;
}

/// When a concurrent consumer (prefetch=1) is aborted mid-flight, unacked
/// messages are returned to the queue and a new consumer picks them all up.
#[tokio::test]
async fn ungraceful_shutdown_concurrent_prefetch_1_recovers_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<UngracefulConc1>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("msg-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<UngracefulConc1>(&messages)
        .await
        .unwrap();

    let slow_handler = SlowCountingHandler::new(Duration::from_millis(500));
    let shutdown1 = CancellationToken::new();
    let consumer1 = RabbitMqConsumer::new(client.clone());
    let h1 = slow_handler.clone();
    let s1 = shutdown1.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s1)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer1.run::<UngracefulConc1, _>(h1, (), opts).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    consume_handle.abort();
    let _ = consume_handle.await;

    assert_eq!(slow_handler.count(), 0);

    // Close first connection so RabbitMQ releases unacked messages.
    client.shutdown().await;

    let client2 = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let handler = CountingHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = RabbitMqConsumer::new(client2.clone());
    let h2 = handler.clone();
    let s2 = shutdown2.clone();
    let consume_handle2 = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s2)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer2.run::<UngracefulConc1, _>(h2, (), opts).await
    });

    assert!(
        handler.wait_for_count(5, Duration::from_secs(10)).await,
        "expected 5 recovered messages, got {}",
        handler.count()
    );
    assert_eq!(handler.count(), 5);

    shutdown2.cancel();
    consume_handle2.await.unwrap().unwrap();
    client2.shutdown().await;
}

/// When a concurrent consumer (prefetch=3) is aborted mid-flight, up to 3
/// unacked messages are returned to the queue and all 5 are recovered.
#[tokio::test]
async fn ungraceful_shutdown_concurrent_prefetch_3_recovers_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<UngracefulConc3>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            body: format!("msg-{i}"),
        })
        .collect();
    publisher
        .publish_batch::<UngracefulConc3>(&messages)
        .await
        .unwrap();

    let slow_handler = SlowCountingHandler::new(Duration::from_millis(500));
    let shutdown1 = CancellationToken::new();
    let consumer1 = RabbitMqConsumer::new(client.clone());
    let h1 = slow_handler.clone();
    let s1 = shutdown1.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s1)
            .with_max_retries(3)
            .with_prefetch_count(3);
        consumer1.run::<UngracefulConc3, _>(h1, (), opts).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    consume_handle.abort();
    let _ = consume_handle.await;

    assert_eq!(slow_handler.count(), 0);

    // Close first connection so RabbitMQ releases unacked messages.
    client.shutdown().await;

    let client2 = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let handler = CountingHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = RabbitMqConsumer::new(client2.clone());
    let h2 = handler.clone();
    let s2 = shutdown2.clone();
    let consume_handle2 = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s2)
            .with_max_retries(3)
            .with_prefetch_count(3);
        consumer2.run::<UngracefulConc3, _>(h2, (), opts).await
    });

    assert!(
        handler.wait_for_count(5, Duration::from_secs(10)).await,
        "expected 5 recovered messages, got {}",
        handler.count()
    );
    assert_eq!(handler.count(), 5);

    shutdown2.cancel();
    consume_handle2.await.unwrap().unwrap();
    client2.shutdown().await;
}

/// Concurrent consumer: message that retries twice then acks.
#[tokio::test]
async fn concurrent_consumer_retry_then_ack() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentRetryWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<ConcurrentRetryWork>(&SimpleMessage {
            body: "retry-conc".into(),
        })
        .await
        .unwrap();

    let handler = RetryThenAckHandler::new(2);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(4);
        consumer.run::<ConcurrentRetryWork, _>(h, (), opts).await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.ack_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ack after retries in concurrent consumer");
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
    broker.stop().await;
}

// --- Concurrent consumer — max retries sends to DLQ ---

#[tokio::test]
async fn concurrent_consumer_max_retries_sends_to_dlq() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentMaxRetry>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<ConcurrentMaxRetry>(&SimpleMessage {
            body: "always-retry".into(),
        })
        .await
        .unwrap();

    let handler = AlwaysRetryHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(2)
            .with_prefetch_count(4);
        consumer.run::<ConcurrentMaxRetry, _>(h, (), opts).await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.attempt_count() >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for retry attempts");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle =
        tokio::spawn(async move { consumer2.run_dlq::<ConcurrentMaxRetry, _>(dh, ()).await });

    assert!(dlq_handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Concurrent consumer — graceful shutdown drains in-flight ---

#[tokio::test]
async fn concurrent_consumer_graceful_shutdown_drains_inflight() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    for i in 0..4 {
        publisher
            .publish::<ConcurrentWork>(&SimpleMessage {
                body: format!("drain-{i}"),
            })
            .await
            .unwrap();
    }

    let handler = SlowCountingHandler::new(Duration::from_millis(500));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(4);
        consumer.run::<ConcurrentWork, _>(h, (), opts).await
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();

    consume_handle.await.unwrap().unwrap();

    assert_eq!(
        handler.count(),
        4,
        "shutdown should drain all in-flight messages"
    );

    client.shutdown().await;
    broker.stop().await;
}

// --- Deserialization failure rejects to DLQ ---

#[tokio::test]
async fn deserialization_failure_rejects_to_dlq() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<StrictWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<StrictWork>(&StrictMessage { value: 42 })
        .await
        .unwrap();

    let raw_channel = client.create_confirm_channel().await.unwrap();
    let confirm = raw_channel
        .basic_publish(
            "".into(),
            "test-strict".into(),
            BasicPublishOptions::default(),
            b"{\"wrong_field\": \"not a number\"}",
            lapin::BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("application/json".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(!confirm.is_nack());

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(4);
        consumer.run::<StrictWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    assert_eq!(handler.count(), 1);

    // Verify the malformed message landed in the DLQ via the management API.
    // We can't use run_dlq here because the DLQ consumer also can't deserialize the malformed
    // message — it logs and acks without calling handle_dead.
    let http = reqwest::Client::new();
    let mgmt = broker.mgmt_config();
    let mut dlq_ready = 0u64;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let resp = http
            .get(format!(
                "{}/api/queues/%2F/test-strict-dlq",
                broker.mgmt_url
            ))
            .basic_auth(&mgmt.username, Some(&mgmt.password))
            .send()
            .await
            .unwrap();
        let stats: QueueStats = resp.json().await.unwrap();
        dlq_ready = stats.messages_ready;
        if dlq_ready >= 1 {
            break;
        }
    }
    assert!(
        dlq_ready >= 1,
        "expected at least 1 message in DLQ, got {dlq_ready}"
    );

    client.shutdown().await;
    broker.stop().await;
}

// --- Concurrent consumer — mixed outcomes routes correctly ---

#[tokio::test]
async fn concurrent_consumer_mixed_outcomes_routes_correctly() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology()
        .declare::<ConcurrentRejectWork>()
        .await
        .unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages = vec![
        SimpleMessage {
            body: "ok-1".into(),
        },
        SimpleMessage {
            body: "reject-1".into(),
        },
        SimpleMessage {
            body: "ok-2".into(),
        },
        SimpleMessage {
            body: "reject-2".into(),
        },
        SimpleMessage {
            body: "ok-3".into(),
        },
    ];
    publisher
        .publish_batch::<ConcurrentRejectWork>(&messages)
        .await
        .unwrap();

    let handler = ConcurrentMixedHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(8);
        consumer.run::<ConcurrentRejectWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_total(5, Duration::from_secs(15)).await);
    assert_eq!(handler.ack_count(), 3);
    assert_eq!(handler.reject_count(), 2);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle =
        tokio::spawn(async move { consumer2.run_dlq::<ConcurrentRejectWork, _>(dh, ()).await });

    assert!(dlq_handler.wait_for_count(2, Duration::from_secs(10)).await);
    assert_eq!(dlq_handler.count(), 2);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Sequential consumer — max retries to DLQ ---

#[tokio::test]
async fn sequential_consumer_max_retries_sends_to_dlq() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<RetryWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<RetryWork>(&SimpleMessage {
            body: "exhaust-retries".into(),
        })
        .await
        .unwrap();

    let handler = AlwaysRetryHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(2)
            .with_prefetch_count(1);
        consumer.run::<RetryWork, _>(h, (), opts).await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.attempt_count() >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for retry attempts");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    let dlq_handler = DlqCountingHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer2.run_dlq::<RetryWork, _>(dh, ()).await });

    assert!(dlq_handler.wait_for_count(1, Duration::from_secs(10)).await);
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Concurrent consumer — handler timeout triggers retry ---

#[tokio::test]
async fn concurrent_consumer_handler_timeout_triggers_retry() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<ConcurrentWork>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<ConcurrentWork>(&SimpleMessage {
            body: "timeout-conc".into(),
        })
        .await
        .unwrap();

    let handler = TimeoutThenAckHandler::new(Duration::from_secs(5));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(4)
            .with_handler_timeout(Duration::from_millis(200));
        consumer.run::<ConcurrentWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(2, Duration::from_secs(30)).await);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced consumer — retry via shard hold queues ---

#[tokio::test]
async fn sequenced_consumer_retry_via_shard_hold_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<RetrySeqOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<RetrySeqOrders>(&OrderMessage {
            account: "ACC-RETRY".into(),
            seq: 1,
        })
        .await
        .unwrap();

    let handler = SeqRetryHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5);
        consumer.run_fifo::<RetrySeqOrders, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_ack(1, Duration::from_secs(30)).await,
        "timed out waiting for ack after sequenced retry"
    );

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced consumer — defer via shard hold queues ---

#[tokio::test]
async fn sequenced_consumer_defer_via_shard_hold_queues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<DeferSeqOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<DeferSeqOrders>(&OrderMessage {
            account: "ACC-DEFER".into(),
            seq: 1,
        })
        .await
        .unwrap();

    let handler = SeqDeferHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5);
        consumer.run_fifo::<DeferSeqOrders, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_ack(1, Duration::from_secs(30)).await,
        "timed out waiting for ack after sequenced defer"
    );

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced FailAll — max retries poisons key ---

#[tokio::test]
async fn sequenced_failall_max_retries_poisons_key() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<FailAllOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();

    for i in 0..3u32 {
        publisher
            .publish::<FailAllOrders>(&OrderMessage {
                account: "ACC-POISON".into(),
                seq: i,
            })
            .await
            .unwrap();
    }
    for i in 0..2u32 {
        publisher
            .publish::<FailAllOrders>(&OrderMessage {
                account: "ACC-SAFE".into(),
                seq: 100 + i,
            })
            .await
            .unwrap();
    }

    let handler = RejectSeqHandler::new(0);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<FailAllOrders, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_total(3, Duration::from_secs(15)).await,
        "timed out: ack={} reject={}",
        handler.ack_count(),
        handler.reject_count()
    );

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();

    let dlq_handler = OrderDlqHandler::new();
    let consumer2 = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle =
        tokio::spawn(async move { consumer2.run_dlq::<FailAllOrders, _>(dh, ()).await });

    assert!(
        dlq_handler.wait_for_count(3, Duration::from_secs(10)).await,
        "expected 3 DLQ messages, got {}",
        dlq_handler.count()
    );

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Sequenced consumer — graceful shutdown drain ---

#[tokio::test]
async fn sequenced_consumer_graceful_shutdown_drains_inflight() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SkipOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    for i in 0..3u32 {
        publisher
            .publish::<SkipOrders>(&OrderMessage {
                account: "ACC-DRAIN".into(),
                seq: i,
            })
            .await
            .unwrap();
    }

    let handler = SlowCountingHandler::new(Duration::from_millis(300));
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<SkipOrders, _>(h, (), opts).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();

    consume_handle.await.unwrap().unwrap();

    assert!(
        handler.count() >= 1,
        "expected at least 1 message processed during shutdown drain, got {}",
        handler.count()
    );

    // Pick up remaining messages with a fresh consumer
    let remaining = 3 - handler.count();
    if remaining > 0 {
        let handler2 = CountingHandler::new();
        let shutdown2 = CancellationToken::new();
        let consumer2 = RabbitMqConsumer::new(client.clone());
        let h2 = handler2.clone();
        let s2 = shutdown2.clone();
        let consume_handle2 = tokio::spawn(async move {
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s2)
                .with_max_retries(3);
            consumer2.run_fifo::<SkipOrders, _>(h2, (), opts).await
        });

        assert!(
            handler2
                .wait_for_count(remaining, Duration::from_secs(15))
                .await,
            "expected {} requeued messages, got {}",
            remaining,
            handler2.count()
        );

        shutdown2.cancel();
        consume_handle2.await.unwrap().unwrap();
    }

    client.shutdown().await;
    broker.stop().await;
}

// --- Publisher — sequenced batch via exchange ---

#[tokio::test]
async fn sequenced_publish_batch_routes_via_exchange() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<OrderTopic>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let messages: Vec<OrderMessage> = (0..10)
        .map(|i| OrderMessage {
            account: format!("ACC-{}", i % 3),
            seq: i,
        })
        .collect();
    publisher
        .publish_batch::<OrderTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<OrderTopic, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(10, Duration::from_secs(15)).await);
    assert_eq!(handler.count(), 10);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Publisher — sequenced publish with headers ---

#[tokio::test]
async fn sequenced_publish_with_headers() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<OrderTopic>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "seq-trace-123".to_string());
    publisher
        .publish_with_headers::<OrderTopic>(
            &OrderMessage {
                account: "ACC-HDR".into(),
                seq: 1,
            },
            headers,
        )
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<OrderTopic, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(15)).await);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- DLQ consumer — deserialization failure acked ---

#[tokio::test]
async fn dlq_consumer_handles_deserialization_failure() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let raw_channel = client.create_confirm_channel().await.unwrap();
    let confirm = raw_channel
        .basic_publish(
            "".into(),
            "test-simple-dlq".into(),
            BasicPublishOptions::default(),
            b"not valid json at all {{{",
            lapin::BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("application/json".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(!confirm.is_nack());

    let confirm = raw_channel
        .basic_publish(
            "".into(),
            "test-simple-dlq".into(),
            BasicPublishOptions::default(),
            b"{\"body\": \"valid-dlq-msg\"}",
            lapin::BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("application/json".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(!confirm.is_nack());

    let dlq_handler = DlqCountingHandler::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let dh = dlq_handler.clone();
    let dlq_handle = tokio::spawn(async move { consumer.run_dlq::<SimpleWork, _>(dh, ()).await });

    assert!(dlq_handler.wait_for_count(1, Duration::from_secs(10)).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(dlq_handler.count(), 1);

    client.shutdown().await;
    dlq_handle.abort();
    broker.stop().await;
}

// --- Client shutdown guard ---

/// Creating channels after shutdown returns an error.
#[tokio::test]
async fn client_create_channel_fails_after_shutdown() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    client.shutdown().await;

    let result = client.create_channel().await;
    assert!(result.is_err(), "create_channel should fail after shutdown");

    let result = client.create_confirm_channel().await;
    assert!(
        result.is_err(),
        "create_confirm_channel should fail after shutdown"
    );

    broker.stop().await;
}

// --- Publisher channel pool ---

/// Publisher with custom channel count still publishes correctly.
#[tokio::test]
async fn publisher_with_channel_count_publishes() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let publisher = RabbitMqPublisher::with_channel_count(client.clone(), 2)
        .await
        .unwrap();

    for i in 0..6 {
        publisher
            .publish::<SimpleWork>(&SimpleMessage {
                body: format!("pool-{i}"),
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
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(10);
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(6, Duration::from_secs(10)).await);
    assert_eq!(handler.count(), 6);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

/// Publisher with channel_count=0 clamps to 1 and works.
#[tokio::test]
async fn publisher_with_zero_channels_clamps_to_one() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SimpleWork>().await.unwrap();

    let publisher = RabbitMqPublisher::with_channel_count(client.clone(), 0)
        .await
        .unwrap();
    publisher
        .publish::<SimpleWork>(&SimpleMessage {
            body: "clamped".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3)
            .with_prefetch_count(1);
        consumer.run::<SimpleWork, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(1, Duration::from_secs(10)).await);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced batch with channel pool ---

/// Batch publish on sequenced topic with channel pool.
#[tokio::test]
async fn sequenced_batch_publish_with_pool() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<OrderTopic>().await.unwrap();

    let publisher = RabbitMqPublisher::with_channel_count(client.clone(), 2)
        .await
        .unwrap();
    let messages: Vec<OrderMessage> = (0..20)
        .map(|i| OrderMessage {
            account: format!("ACC-BATCH-{}", i % 5),
            seq: i,
        })
        .collect();
    publisher
        .publish_batch::<OrderTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<OrderTopic, _>(h, (), opts).await
    });

    assert!(handler.wait_for_count(20, Duration::from_secs(20)).await);
    assert_eq!(handler.count(), 20);

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Consumer group concurrent processing ---

/// Consumer group with concurrent_processing=true processes messages.
#[tokio::test]
async fn registry_concurrent_processing_consumes_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    registry
        .register::<SimpleWork, _>(
            ConsumerGroupConfig::new(1..=3)
                .with_concurrent_processing(true)
                .with_prefetch_count(5),
            move || h.clone(),
            (),
        )
        .await
        .unwrap();

    registry.start_all();

    let publisher = b.publisher().await.unwrap();
    for i in 0..5 {
        publisher
            .publish::<SimpleWork>(&SimpleMessage {
                body: format!("conc-group-{i}"),
            })
            .await
            .unwrap();
    }

    assert!(handler.wait_for_count(5, Duration::from_secs(15)).await);
    assert_eq!(handler.count(), 5);

    registry.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

// --- Duplicate registration error ---

/// Registering the same topic twice returns an error.
#[tokio::test]
async fn registry_duplicate_registration_fails() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    registry
        .register::<SimpleWork, _>(ConsumerGroupConfig::new(1..=2), move || h.clone(), ())
        .await
        .unwrap();

    let handler2 = CountingHandler::new();
    let h2 = handler2.clone();
    let result = registry
        .register::<SimpleWork, _>(ConsumerGroupConfig::new(1..=2), move || h2.clone(), ())
        .await;

    assert!(result.is_err(), "duplicate registration should fail");

    registry.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

// --- Consumer group concurrent + handler timeout ---

/// Consumer group with concurrent + handler timeout processes messages correctly.
#[tokio::test]
async fn registry_concurrent_with_timeout_processes_messages() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());

    let mut registry = ConsumerGroupRegistry::new(client.clone());
    let handler = CountingHandler::new();
    let h = handler.clone();

    registry
        .register::<SimpleWork, _>(
            ConsumerGroupConfig::new(1..=2)
                .with_concurrent_processing(true)
                .with_prefetch_count(5)
                .with_handler_timeout(Duration::from_secs(10)),
            move || h.clone(),
            (),
        )
        .await
        .unwrap();

    registry.start_all();

    let publisher = b.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<SimpleWork>(&SimpleMessage {
                body: format!("timeout-group-{i}"),
            })
            .await
            .unwrap();
    }

    assert!(handler.wait_for_count(3, Duration::from_secs(15)).await);
    assert_eq!(handler.count(), 3);

    registry.shutdown_all().await;
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced consumer — multiple keys concurrent ---

#[tokio::test]
async fn sequenced_consumer_multiple_keys_concurrent() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<SkipOrders>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    for account_idx in 0..5u32 {
        for seq in 0..3u32 {
            publisher
                .publish::<SkipOrders>(&OrderMessage {
                    account: format!("ACC-MULTI-{account_idx}"),
                    seq: account_idx * 100 + seq,
                })
                .await
                .unwrap();
        }
    }

    let handler = CountingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(3);
        consumer.run_fifo::<SkipOrders, _>(h, (), opts).await
    });

    assert!(
        handler.wait_for_count(15, Duration::from_secs(15)).await,
        "expected 15 messages, got {}",
        handler.count()
    );

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// ===========================================================================
// Exactly-once delivery (AMQP transactions)
// ===========================================================================
//
// These tests verify that consumers using `with_exactly_once()` produce the
// same observable behavior as the default confirm-mode consumers (ack, retry,
// reject/DLQ) while using AMQP transaction mode under the hood.
//
// Run with: `cargo test --features rabbitmq-transactional --test rabbitmq_integration`

#[cfg(feature = "rabbitmq-transactional")]
mod exactly_once {
    use super::*;

    // ── Topics ──────────────────────────────────────────────────────────────

    define_topic!(
        ExactlyOnceWork,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    define_topic!(
        ExactlyOnceRetry,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-retry")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    define_topic!(
        ExactlyOnceReject,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-reject")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    // ── Handler impls ────────────────────────────────────────────────────────

    impl MessageHandler<ExactlyOnceWork> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }

    impl MessageHandler<ExactlyOnceRetry> for RetryThenAckHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

    impl MessageHandler<ExactlyOnceReject> for DlqCountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            Outcome::Reject
        }
        async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
        }
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// `create_tx_channel` succeeds and returns a usable channel.
    #[tokio::test]
    async fn client_create_tx_channel() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();

        let _channel = client.create_tx_channel().await.unwrap();

        client.shutdown().await;
        broker.stop().await;
    }

    /// A consumer with exactly-once enabled processes messages and acks them
    /// exactly once — same observable result as the default confirm-mode path.
    #[tokio::test]
    async fn exactly_once_consumer_basic_ack() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology().declare::<ExactlyOnceWork>().await.unwrap();

        let publisher = b.publisher().await.unwrap();
        for i in 0..5 {
            publisher
                .publish::<ExactlyOnceWork>(&SimpleMessage {
                    body: format!("msg-{i}"),
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
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_exactly_once();
            consumer.run::<ExactlyOnceWork, _>(h, (), opts).await
        });

        assert!(
            handler.wait_for_count(5, Duration::from_secs(15)).await,
            "expected 5 messages processed, got {}",
            handler.count()
        );
        // Brief settle period — verify no duplicate deliveries.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(handler.count(), 5, "unexpected extra deliveries");

        shutdown.cancel();
        consume_handle.await.unwrap().unwrap();
        client.shutdown().await;
        broker.stop().await;
    }

    /// Retry-then-ack works correctly under exactly-once: the message visits the
    /// hold queue the expected number of times and is ultimately acked once.
    #[tokio::test]
    async fn exactly_once_consumer_retry_then_ack() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology().declare::<ExactlyOnceRetry>().await.unwrap();

        let publisher = b.publisher().await.unwrap();
        publisher
            .publish::<ExactlyOnceRetry>(&SimpleMessage {
                body: "retry-me".into(),
            })
            .await
            .unwrap();

        // Handler retries twice (attempts 0 and 1), then acks on attempt 2.
        let handler = RetryThenAckHandler::new(2);
        let shutdown = CancellationToken::new();
        let consumer = RabbitMqConsumer::new(client.clone());
        let h = handler.clone();
        let s = shutdown.clone();
        let consume_handle = tokio::spawn(async move {
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_max_retries(5)
                .with_exactly_once();
            consumer.run::<ExactlyOnceRetry, _>(h, (), opts).await
        });

        // Hold queue TTL is 1 s per retry; allow generous timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if handler.ack_count() >= 1 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "timed out waiting for ack; ack_count={}",
                    handler.ack_count()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Exactly one final ack, no duplicates.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.ack_count(), 1, "unexpected extra acks");

        shutdown.cancel();
        consume_handle.await.unwrap().unwrap();
        client.shutdown().await;
        broker.stop().await;
    }

    define_topic!(
        ExactlyOnceDefer,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-defer")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    define_topic!(
        ExactlyOnceMaxRetries,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-max-retries")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    define_topic!(
        ExactlyOnceShutdown,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-shutdown")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    define_topic!(
        ExactlyOnceConcurrent,
        SimpleMessage,
        TopologyBuilder::new("test-exactly-once-concurrent")
            .dlq()
            .hold_queue(Duration::from_secs(1))
            .build()
    );

    impl MessageHandler<ExactlyOnceDefer> for DeferOnceHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
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

    impl MessageHandler<ExactlyOnceMaxRetries> for AlwaysRetryHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.attempt_count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Retry
        }
    }

    impl MessageHandler<ExactlyOnceMaxRetries> for DlqCountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _msg: SimpleMessage, _meta: DeadMessageMetadata, _: &()) {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
        }
    }

    impl MessageHandler<ExactlyOnceShutdown> for SlowCountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            tokio::time::sleep(self.delay).await;
            self.count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }

    impl MessageHandler<ExactlyOnceConcurrent> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.signal.notify_waiters();
            Outcome::Ack
        }
    }

    // ── Tests (continued) ───────────────────────────────────────────────────

    /// Defer outcome under exactly-once: message visits the hold queue exactly
    /// once and is acked on redelivery — no duplicates from the tx path.
    #[tokio::test]
    async fn exactly_once_consumer_defer() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology().declare::<ExactlyOnceDefer>().await.unwrap();

        let publisher = b.publisher().await.unwrap();
        publisher
            .publish::<ExactlyOnceDefer>(&SimpleMessage {
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
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_exactly_once();
            consumer.run::<ExactlyOnceDefer, _>(h, (), opts).await
        });

        // Hold queue TTL is 1 s; allow generous timeout for both deliveries.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if handler.ack_count() >= 1 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "timed out waiting for ack; ack_count={}",
                    handler.ack_count()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Exactly one final ack, no duplicate deliveries.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.ack_count(), 1, "message must be acked exactly once");

        shutdown.cancel();
        consume_handle.await.unwrap().unwrap();
        client.shutdown().await;
        broker.stop().await;
    }

    /// When max_retries is exhausted under exactly-once the message lands in
    /// the DLQ exactly once — no duplicate DLQ entries from the tx path.
    #[tokio::test]
    async fn exactly_once_consumer_max_retries_to_dlq() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology()
            .declare::<ExactlyOnceMaxRetries>()
            .await
            .unwrap();

        let publisher = b.publisher().await.unwrap();
        publisher
            .publish::<ExactlyOnceMaxRetries>(&SimpleMessage {
                body: "exhaust-me".into(),
            })
            .await
            .unwrap();

        let retry_handler = AlwaysRetryHandler::new();
        let dlq_handler = DlqCountingHandler::new();
        let shutdown = CancellationToken::new();

        // Main consumer — always retries.
        let consumer = RabbitMqConsumer::new(client.clone());
        let h = retry_handler.clone();
        let s = shutdown.clone();
        let main_handle = tokio::spawn(async move {
            // max_retries=2 so only 3 total attempts before DLQ.
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_max_retries(2)
                .with_exactly_once();
            consumer.run::<ExactlyOnceMaxRetries, _>(h, (), opts).await
        });

        // DLQ consumer.
        let dlq_consumer = RabbitMqConsumer::new(client.clone());
        let dlq_h = dlq_handler.clone();
        let dlq_s = shutdown.clone();
        let dlq_handle = tokio::spawn(async move {
            let _s = dlq_s;
            dlq_consumer
                .run_dlq::<ExactlyOnceMaxRetries, _>(dlq_h, ())
                .await
        });

        assert!(
            dlq_handler.wait_for_count(1, Duration::from_secs(20)).await,
            "message must reach the DLQ, got {}",
            dlq_handler.count()
        );
        // Brief settle — verify no duplicates in DLQ.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            dlq_handler.count(),
            1,
            "DLQ must receive message exactly once"
        );

        shutdown.cancel();
        main_handle.await.unwrap().unwrap();
        dlq_handle.abort();
        client.shutdown().await;
        broker.stop().await;
    }

    /// Graceful shutdown drains in-flight messages under exactly-once mode —
    /// every in-progress message is committed before the consumer exits.
    #[tokio::test]
    async fn exactly_once_consumer_graceful_shutdown() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology().declare::<ExactlyOnceShutdown>().await.unwrap();

        let publisher = b.publisher().await.unwrap();
        for i in 0..5 {
            publisher
                .publish::<ExactlyOnceShutdown>(&SimpleMessage {
                    body: format!("msg-{i}"),
                })
                .await
                .unwrap();
        }

        // Slow handler so some messages will be in-flight when we cancel.
        let handler = SlowCountingHandler::new(Duration::from_millis(300));
        let shutdown = CancellationToken::new();
        let consumer = RabbitMqConsumer::new(client.clone());
        let h = handler.clone();
        let s = shutdown.clone();
        let consume_handle = tokio::spawn(async move {
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_exactly_once()
                .with_prefetch_count(1);
            consumer.run::<ExactlyOnceShutdown, _>(h, (), opts).await
        });

        // Wait until at least one message is processed, then cancel.
        assert!(
            handler.wait_for_count(1, Duration::from_secs(10)).await,
            "at least one message should process before shutdown"
        );
        shutdown.cancel();

        // Consumer must exit cleanly (no panic, no error).
        consume_handle.await.unwrap().unwrap();
        client.shutdown().await;
        broker.stop().await;
    }

    /// Concurrent prefetch > 1 under exactly-once: multiple messages in-flight
    /// per consumer must all be committed without duplicates.
    #[tokio::test]
    async fn exactly_once_consumer_concurrent_prefetch() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology()
            .declare::<ExactlyOnceConcurrent>()
            .await
            .unwrap();

        let publisher = b.publisher().await.unwrap();
        let n: u32 = 10;
        for i in 0..n {
            publisher
                .publish::<ExactlyOnceConcurrent>(&SimpleMessage {
                    body: format!("msg-{i}"),
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
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(s)
                .with_exactly_once()
                .with_prefetch_count(3);
            consumer.run::<ExactlyOnceConcurrent, _>(h, (), opts).await
        });

        assert!(
            handler.wait_for_count(n, Duration::from_secs(15)).await,
            "expected {n} messages processed, got {}",
            handler.count()
        );
        // Brief settle — no duplicate deliveries.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.count(), n, "unexpected extra deliveries");

        shutdown.cancel();
        consume_handle.await.unwrap().unwrap();
        client.shutdown().await;
        broker.stop().await;
    }

    /// Messages rejected by the handler are routed to the DLQ exactly once
    /// under exactly-once mode — no duplicate DLQ entries.
    #[tokio::test]
    async fn exactly_once_consumer_reject_to_dlq() {
        let broker = TestBroker::start().await;
        let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
        let b = broker.broker_from(client.clone());
        b.topology().declare::<ExactlyOnceReject>().await.unwrap();

        let publisher = b.publisher().await.unwrap();
        for i in 0..3 {
            publisher
                .publish::<ExactlyOnceReject>(&SimpleMessage {
                    body: format!("reject-{i}"),
                })
                .await
                .unwrap();
        }

        let dlq_handler = DlqCountingHandler::new();
        let shutdown = CancellationToken::new();
        let consumer = RabbitMqConsumer::new(client.clone());
        let dlq_consumer = RabbitMqConsumer::new(client.clone());

        // Main consumer — rejects everything to DLQ.
        let main_h = dlq_handler.clone();
        let main_s = shutdown.clone();
        let main_handle = tokio::spawn(async move {
            let opts = ConsumerOptions::<RabbitMqMarker>::new()
                .with_shutdown(main_s)
                .with_exactly_once();
            consumer.run::<ExactlyOnceReject, _>(main_h, (), opts).await
        });

        // DLQ consumer — counts dead messages.
        let dlq_h = dlq_handler.clone();
        let dlq_s = shutdown.clone();
        let dlq_handle = tokio::spawn(async move {
            let _s = dlq_s;
            dlq_consumer
                .run_dlq::<ExactlyOnceReject, _>(dlq_h, ())
                .await
        });

        assert!(
            dlq_handler.wait_for_count(3, Duration::from_secs(15)).await,
            "expected 3 DLQ messages, got {}",
            dlq_handler.count()
        );
        // Brief settle — verify no duplicates in DLQ.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(dlq_handler.count(), 3, "unexpected extra DLQ messages");

        shutdown.cancel();
        main_handle.await.unwrap().unwrap();
        dlq_handle.abort();
        client.shutdown().await;
        broker.stop().await;
    }
}

// --- Defer preserves retry count ---

#[tokio::test]
async fn defer_preserves_retry_count() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<DeferWithHold>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<DeferWithHold>(&SimpleMessage {
            body: "retry-then-defer".into(),
        })
        .await
        .unwrap();

    let handler = RetryThenDeferHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5)
            .with_prefetch_count(1);
        consumer.run::<DeferWithHold, _>(h, (), opts).await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handler.ack_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ack after retry-then-defer");
        }
        tokio::select! {
            _ = handler.signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    // Delivery 0: retry_count=0 → Retry (increments to 1)
    // Delivery 1: retry_count=1 → Defer (should NOT increment)
    // Delivery 2: retry_count=1 → Ack
    let counts = handler.retry_counts();
    assert_eq!(counts.len(), 3, "expected 3 deliveries, got {counts:?}");
    assert_eq!(counts[0], 0, "first delivery should have retry_count=0");
    assert_eq!(counts[1], 1, "after Retry, retry_count should be 1");
    assert_eq!(counts[2], 1, "after Defer, retry_count should still be 1");

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}

// --- Sequenced defer without hold queues (nack+requeue fallback) ---

#[tokio::test]
async fn sequenced_defer_without_hold_queue_requeues() {
    let broker = TestBroker::start().await;
    let client = RabbitMqClient::connect(&broker.rmq_config()).await.unwrap();
    let b = broker.broker_from(client.clone());
    b.topology().declare::<DeferSeqNoHold>().await.unwrap();

    let publisher = b.publisher().await.unwrap();
    publisher
        .publish::<DeferSeqNoHold>(&OrderMessage {
            account: "ACC-DEFER-NOHOLD".into(),
            seq: 1,
        })
        .await
        .unwrap();

    let handler = SeqDeferHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_max_retries(5);
        consumer.run_fifo::<DeferSeqNoHold, _>(h, (), opts).await
    });

    // Defer (nack-requeue) → redelivered → Ack
    assert!(
        handler.wait_for_ack(1, Duration::from_secs(30)).await,
        "timed out waiting for ack after sequenced defer without hold queue"
    );

    shutdown.cancel();
    consume_handle.await.unwrap().unwrap();
    client.shutdown().await;
    broker.stop().await;
}
