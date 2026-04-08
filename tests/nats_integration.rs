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
