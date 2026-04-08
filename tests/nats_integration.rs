#![cfg(feature = "nats")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::consumer::{Consumer, ConsumerOptions};
use shove::handler::MessageHandler;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{
    NatsClient, NatsConfig, NatsConsumer, NatsPublisher, NatsTopologyDeclarer,
};
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::topology::{TopologyBuilder, TopologyDeclarer};
use tokio_util::sync::CancellationToken;

// --- Test topic definitions ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage {
    id: u32,
    body: String,
}

shove::define_topic!(
    BasicTopic,
    TestMessage,
    TopologyBuilder::new("nats-test-basic")
        .hold_queue(Duration::from_millis(100))
        .dlq()
        .build()
);

shove::define_topic!(
    RetryTopic,
    TestMessage,
    TopologyBuilder::new("nats-test-retry")
        .hold_queue(Duration::from_millis(100))
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

// --- Test handlers ---

struct CountingHandler {
    count: Arc<AtomicU32>,
}

impl MessageHandler<BasicTopic> for CountingHandler {
    async fn handle(&self, _message: TestMessage, _metadata: MessageMetadata) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

struct RetryThenAckHandler {
    ack_after: u32,
    count: Arc<AtomicU32>,
}

impl MessageHandler<RetryTopic> for RetryThenAckHandler {
    async fn handle(&self, _message: TestMessage, metadata: MessageMetadata) -> Outcome {
        if metadata.retry_count < self.ack_after {
            Outcome::Retry
        } else {
            self.count.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }
}

struct RejectHandler {
    dlq_count: Arc<AtomicU32>,
}

impl MessageHandler<RetryTopic> for RejectHandler {
    async fn handle(&self, _message: TestMessage, _metadata: MessageMetadata) -> Outcome {
        Outcome::Reject
    }

    async fn handle_dead(&self, _message: TestMessage, _metadata: DeadMessageMetadata) {
        self.dlq_count.fetch_add(1, Ordering::Relaxed);
    }
}

// --- Helpers ---

async fn setup() -> (NatsClient, NatsPublisher, NatsConsumer) {
    let config = NatsConfig::new(
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string()),
    );
    let client = NatsClient::connect(&config)
        .await
        .expect("failed to connect to NATS");
    let publisher = NatsPublisher::new(client.clone()).await.unwrap();
    let consumer = NatsConsumer::new(client.clone());
    (client, publisher, consumer)
}

async fn declare_topology<T: shove::topic::Topic>(client: &NatsClient) {
    let declarer = NatsTopologyDeclarer::new(client.clone());
    declarer.declare(T::topology()).await.unwrap();
}

// --- Tests ---

#[tokio::test]
async fn test_basic_publish_consume() {
    let (client, publisher, consumer) = setup().await;
    declare_topology::<BasicTopic>(&client).await;

    let count = Arc::new(AtomicU32::new(0));
    let shutdown = CancellationToken::new();

    // Publish 5 messages
    for i in 0..5 {
        publisher
            .publish::<BasicTopic>(&TestMessage {
                id: i,
                body: format!("msg-{i}"),
            })
            .await
            .unwrap();
    }

    // Consume
    let handler = CountingHandler {
        count: count.clone(),
    };
    let options = ConsumerOptions::new(shutdown.clone());
    let consumer_handle = tokio::spawn(async move {
        consumer.run::<BasicTopic>(handler, options).await
    });

    // Wait for all messages
    tokio::time::timeout(Duration::from_secs(10), async {
        while count.load(Ordering::Relaxed) < 5 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for messages");

    shutdown.cancel();
    let _ = consumer_handle.await;

    assert_eq!(count.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn test_retry_with_backoff() {
    let (client, publisher, consumer) = setup().await;
    declare_topology::<RetryTopic>(&client).await;

    let count = Arc::new(AtomicU32::new(0));
    let shutdown = CancellationToken::new();

    publisher
        .publish::<RetryTopic>(&TestMessage {
            id: 1,
            body: "retry-me".into(),
        })
        .await
        .unwrap();

    let handler = RetryThenAckHandler {
        ack_after: 2,
        count: count.clone(),
    };
    let options = ConsumerOptions::new(shutdown.clone()).with_max_retries(5);
    let consumer_handle = tokio::spawn(async move {
        consumer.run::<RetryTopic>(handler, options).await
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        while count.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for retried message");

    shutdown.cancel();
    let _ = consumer_handle.await;

    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_reject_to_dlq() {
    let (client, publisher, _consumer) = setup().await;
    declare_topology::<RetryTopic>(&client).await;

    let dlq_count = Arc::new(AtomicU32::new(0));
    let shutdown = CancellationToken::new();

    publisher
        .publish::<RetryTopic>(&TestMessage {
            id: 1,
            body: "reject-me".into(),
        })
        .await
        .unwrap();

    // Run main consumer that rejects
    let reject_handler = RejectHandler {
        dlq_count: Arc::new(AtomicU32::new(0)),
    };
    let options = ConsumerOptions::new(shutdown.clone());
    let main_consumer = NatsConsumer::new(client.clone());
    let main_handle = tokio::spawn(async move {
        main_consumer.run::<RetryTopic>(reject_handler, options).await
    });

    // Small delay for reject to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Run DLQ consumer
    let dlq_handler = RejectHandler {
        dlq_count: dlq_count.clone(),
    };
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle = tokio::spawn(async move {
        dlq_consumer.run_dlq::<RetryTopic>(dlq_handler).await
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        while dlq_count.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for DLQ message");

    shutdown.cancel();
    client.shutdown().await;
    let _ = main_handle.await;
    let _ = dlq_handle.await;

    assert_eq!(dlq_count.load(Ordering::Relaxed), 1);
}