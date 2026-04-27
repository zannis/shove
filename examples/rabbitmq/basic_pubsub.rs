//! Basic pub/sub examples covering all non-sequenced topology configurations.
//!
//! Demonstrates: `define_topic!`, manual `Topic` impl, all `Outcome` variants,
//! `publish`, `publish_with_headers`, `publish_batch`, and per-topic consumer
//! registration via `Broker<RabbitMq>::consumer_supervisor()`.
//!
//! Note: the earlier DLQ-consumer demo (`run_dlq::<DlqOrder>`) isn't wired
//! through the generic `ConsumerSupervisor<B>` wrapper yet — messages that
//! land in the DLQ here are simply left there for inspection.
//!
//! Spins up a RabbitMQ testcontainer automatically (requires a running
//! Docker daemon):
//!
//!     cargo run --example rabbitmq_basic_pubsub --features rabbitmq

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::RabbitMqConfig;
use shove::{
    Broker, ConsumerOptions, DeadMessageMetadata, MessageHandler, MessageMetadata, Outcome,
    QueueTopology, RabbitMq, Topic, TopologyBuilder, define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    amount_cents: u64,
}

// ─── Topic definitions ──────────────────────────────────────────────────────

// 1. Minimal: just a queue. No DLQ, no hold queues.
//    Rejected messages are discarded with a warning log.
define_topic!(
    MinimalOrder,
    OrderEvent,
    TopologyBuilder::new("ex-minimal-orders").build()
);

// 2. With DLQ: rejected messages land in a dead-letter queue for inspection.
define_topic!(
    DlqOrder,
    OrderEvent,
    TopologyBuilder::new("ex-dlq-orders").dlq().build()
);

// 3. With hold queues + DLQ: escalating retry backoff (5 s → 30 s → 120 s).
//    Messages that exceed max_retries are sent to DLQ.
define_topic!(
    RetryOrder,
    OrderEvent,
    TopologyBuilder::new("ex-retry-orders")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .hold_queue(Duration::from_secs(120))
        .dlq()
        .build()
);

// 4. Manual `Topic` impl — for topics that use `Defer` (hold without retry
//    counter increment). `OnceLock` pattern shown explicitly.
struct ScheduledOrder;

impl Topic for ScheduledOrder {
    type Message = OrderEvent;

    fn topology() -> &'static QueueTopology {
        static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
        TOPOLOGY.get_or_init(|| {
            TopologyBuilder::new("ex-scheduled-orders")
                .hold_queue(Duration::from_secs(10))
                .dlq()
                .build()
        })
    }
}

// ─── Handlers ───────────────────────────────────────────────────────────────

// Acks every message.
struct AckHandler;

impl MessageHandler<MinimalOrder> for AckHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[minimal] order={} amount=${:.2} attempt={}",
            msg.order_id,
            msg.amount_cents as f64 / 100.0,
            metadata.retry_count + 1,
        );
        Outcome::Ack
    }
}

// Rejects every message. Also implements handle_dead for DLQ processing.
struct RejectHandler;

impl MessageHandler<DlqOrder> for RejectHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, _metadata: MessageMetadata, _: &()) -> Outcome {
        println!("[dlq] rejecting order={} → DLQ", msg.order_id);
        Outcome::Reject
    }

    async fn handle_dead(&self, msg: OrderEvent, metadata: DeadMessageMetadata, _: &()) {
        println!(
            "[dlq] dead-letter: order={} reason={} deaths={}",
            msg.order_id,
            metadata.reason.as_deref().unwrap_or("unknown"),
            metadata.death_count,
        );
    }
}

// Retries once (simulates transient failure), then acks.
struct RetryHandler;

impl MessageHandler<RetryOrder> for RetryHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[retry] order={} attempt={}",
            msg.order_id,
            metadata.retry_count + 1,
        );
        if metadata.retry_count == 0 {
            println!("[retry]   → transient failure, will Retry");
            Outcome::Retry
        } else {
            println!("[retry]   → success on retry");
            Outcome::Ack
        }
    }
}

// Defers the message on first delivery (re-delivers via hold queue without
// incrementing the retry counter), then acks.
struct DeferHandler;

impl MessageHandler<ScheduledOrder> for DeferHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[defer] order={} attempt={} redelivered={}",
            msg.order_id,
            metadata.retry_count + 1,
            metadata.redelivered,
        );
        if metadata.retry_count == 0 && !metadata.redelivered {
            println!("[defer]   → not ready yet, deferring to hold queue");
            Outcome::Defer
        } else {
            println!("[defer]   → processing now");
            Outcome::Ack
        }
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Spin up a RabbitMQ testcontainer for the lifetime of this example.
    let container = RabbitMqImage::default().start().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://guest:guest@localhost:{port}/%2f");

    // [!region connect]
    let broker = Broker::<RabbitMq>::new(RabbitMqConfig::new(&uri)).await?;
    // [!endregion connect]

    // ── Declare all topologies ──
    // [!region declare]
    let topology = broker.topology();
    topology.declare::<MinimalOrder>().await?;
    topology.declare::<DlqOrder>().await?;
    topology.declare::<RetryOrder>().await?;
    topology.declare::<ScheduledOrder>().await?;
    // [!endregion declare]
    println!("topologies declared\n");

    // ── Publish ──
    // [!region publish]
    let publisher = broker.publisher().await?;

    // Single publish
    let order = OrderEvent {
        order_id: "ORD-001".into(),
        amount_cents: 5000,
    };
    publisher.publish::<MinimalOrder>(&order).await?;
    publisher.publish::<DlqOrder>(&order).await?;
    publisher.publish::<RetryOrder>(&order).await?;
    publisher.publish::<ScheduledOrder>(&order).await?;

    // Publish with custom headers
    let mut headers = HashMap::new();
    headers.insert("x-source".into(), "example".into());
    headers.insert("x-priority".into(), "high".into());
    let order2 = OrderEvent {
        order_id: "ORD-002".into(),
        amount_cents: 9900,
    };
    publisher
        .publish_with_headers::<MinimalOrder>(&order2, headers)
        .await?;

    // Batch publish
    let batch = vec![
        OrderEvent {
            order_id: "ORD-003".into(),
            amount_cents: 1000,
        },
        OrderEvent {
            order_id: "ORD-004".into(),
            amount_cents: 2500,
        },
        OrderEvent {
            order_id: "ORD-005".into(),
            amount_cents: 7777,
        },
    ];
    publisher.publish_batch::<MinimalOrder>(&batch).await?;
    // [!endregion publish]
    println!("messages published\n");

    // ── Start consumers via a single supervisor ──
    //
    // Each `register` spawns one tokio task running that topic's consumer.
    // The supervisor owns a shared shutdown token so ctrl-c or our timeout
    // stops every consumer in lock-step.
    // [!region consume]
    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<MinimalOrder, _>(AckHandler, ConsumerOptions::<RabbitMq>::new())?;
    supervisor.register::<DlqOrder, _>(RejectHandler, ConsumerOptions::<RabbitMq>::new())?;
    supervisor.register::<RetryOrder, _>(
        RetryHandler,
        ConsumerOptions::<RabbitMq>::new().with_max_retries(3),
    )?;
    supervisor.register::<ScheduledOrder, _>(DeferHandler, ConsumerOptions::<RabbitMq>::new())?;

    // Run until ctrl-c or a 5 s demo timeout, then drain for up to 5 s.
    let outcome = supervisor
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(5),
        )
        .await;
    // [!endregion consume]

    println!("done");
    std::process::exit(outcome.exit_code());
}
