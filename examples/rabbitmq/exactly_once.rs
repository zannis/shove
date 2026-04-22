//! Exactly-once delivery example (RabbitMQ backend).
//!
//! Demonstrates: `ConsumerOptions::<RabbitMq>::with_exactly_once()`, the AMQP
//! transaction mode path (tx_select / tx_commit), retry via hold queue, and
//! rejection to DLQ.
//!
//! Under exactly-once mode every routing decision (publish-to-hold-queue + ack)
//! is wrapped in a single AMQP transaction, eliminating the publish-then-ack
//! race that can produce duplicate deliveries under at-least-once semantics.
//!
//! Trade-off: expect ~10-15× lower throughput per channel compared to the
//! default confirm-mode consumer.
//!
//! Spins up a RabbitMQ testcontainer automatically (requires a running
//! Docker daemon):
//!
//!     cargo run --example rabbitmq_exactly_once --features rabbitmq-transactional

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::RabbitMqConfig;
use shove::{
    Broker, ConsumerOptions, DeadMessageMetadata, MessageHandler, MessageMetadata, Outcome,
    RabbitMq, TopologyBuilder, define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentEvent {
    payment_id: String,
    amount_cents: u64,
}

// ─── Topics ─────────────────────────────────────────────────────────────────

// Happy path: all messages acked.
define_topic!(
    PaymentTopic,
    PaymentEvent,
    TopologyBuilder::new("ex-exactly-once-payment")
        .hold_queue(Duration::from_secs(2))
        .dlq()
        .build()
);

// Retry path: first delivery retried once, then acked.
define_topic!(
    RetryPaymentTopic,
    PaymentEvent,
    TopologyBuilder::new("ex-exactly-once-retry")
        .hold_queue(Duration::from_secs(2))
        .dlq()
        .build()
);

// Reject path: all messages rejected straight to DLQ.
define_topic!(
    RejectPaymentTopic,
    PaymentEvent,
    TopologyBuilder::new("ex-exactly-once-reject").dlq().build()
);

// ─── Handlers ───────────────────────────────────────────────────────────────

struct AckHandler {
    count: Arc<AtomicU32>,
}

impl AckHandler {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<PaymentTopic> for AckHandler {
    type Context = ();
    async fn handle(&self, msg: PaymentEvent, _meta: MessageMetadata, _: &()) -> Outcome {
        let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "[ack]    payment={} amount=${:.2}  (processed: {n})",
            msg.payment_id,
            msg.amount_cents as f64 / 100.0,
        );
        Outcome::Ack
    }
}

struct RetryThenAckHandler {
    attempts: Arc<AtomicU32>,
}

impl RetryThenAckHandler {
    fn new() -> Self {
        Self {
            attempts: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<RetryPaymentTopic> for RetryThenAckHandler {
    type Context = ();
    async fn handle(&self, msg: PaymentEvent, meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        println!(
            "[retry]  payment={} attempt={} retry_count={}",
            msg.payment_id,
            attempt + 1,
            meta.retry_count,
        );
        if meta.retry_count == 0 {
            println!("[retry]    → simulated transient failure, retrying…");
            Outcome::Retry
        } else {
            println!("[retry]    → succeeded on retry");
            Outcome::Ack
        }
    }
}

struct RejectHandler {
    dead_count: Arc<AtomicU32>,
}

impl RejectHandler {
    fn new() -> Self {
        Self {
            dead_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<RejectPaymentTopic> for RejectHandler {
    type Context = ();
    async fn handle(&self, msg: PaymentEvent, _meta: MessageMetadata, _: &()) -> Outcome {
        println!("[reject] payment={} → routing to DLQ", msg.payment_id);
        Outcome::Reject
    }

    async fn handle_dead(&self, msg: PaymentEvent, meta: DeadMessageMetadata, _: &()) {
        let n = self.dead_count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "[dlq]    payment={} reason={} deaths={} (dlq total: {n})",
            msg.payment_id,
            meta.reason.as_deref().unwrap_or("unknown"),
            meta.death_count,
        );
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=debug,rabbitmq_exactly_once=debug".parse().unwrap()),
        )
        .init();

    let container = RabbitMqImage::default().start().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://guest:guest@localhost:{port}/%2f");
    let broker = Broker::<RabbitMq>::new(RabbitMqConfig::new(&uri)).await?;

    // ── Declare topologies ──
    let topology = broker.topology();
    topology.declare::<PaymentTopic>().await?;
    topology.declare::<RetryPaymentTopic>().await?;
    topology.declare::<RejectPaymentTopic>().await?;
    println!("topologies declared\n");

    // ── Publish messages ──
    let publisher = broker.publisher().await?;

    for i in 1..=5 {
        publisher
            .publish::<PaymentTopic>(&PaymentEvent {
                payment_id: format!("PAY-{i:03}"),
                amount_cents: i * 1000,
            })
            .await?;
    }
    publisher
        .publish::<RetryPaymentTopic>(&PaymentEvent {
            payment_id: "PAY-RETRY".into(),
            amount_cents: 2500,
        })
        .await?;
    for i in 1..=3 {
        publisher
            .publish::<RejectPaymentTopic>(&PaymentEvent {
                payment_id: format!("PAY-BAD-{i}"),
                amount_cents: 0,
            })
            .await?;
    }
    println!("messages published\n");

    // ── Start exactly-once consumers ──
    //
    // `with_exactly_once()` puts the consumer channel into AMQP tx mode.
    // Every routing decision (hold-queue publish + ack/nack) becomes atomic.
    // The observable behaviour is identical to confirm-mode — acks happen
    // exactly once and retries use the hold queue — but without the small
    // window in confirm-mode where a broker restart between publish and ack
    // could produce a duplicate.
    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<PaymentTopic, _>(
        AckHandler::new(),
        ConsumerOptions::<RabbitMq>::new().with_exactly_once(),
    )?;
    supervisor.register::<RetryPaymentTopic, _>(
        RetryThenAckHandler::new(),
        ConsumerOptions::<RabbitMq>::new()
            .with_max_retries(3)
            .with_exactly_once(),
    )?;
    supervisor.register::<RejectPaymentTopic, _>(
        RejectHandler::new(),
        ConsumerOptions::<RabbitMq>::new().with_exactly_once(),
    )?;

    // Let the system run long enough for the retry hold queue (2 s TTL) to fire.
    let outcome = supervisor
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(8)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(5),
        )
        .await;

    println!("done");
    std::process::exit(outcome.exit_code());
}
