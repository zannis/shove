//! Basic NATS JetStream publish/consume example.
//!
//! Spins up a NATS JetStream testcontainer automatically (requires a running
//! Docker daemon):
//!
//!     cargo run -q --example nats_basic --features nats

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::nats::{NatsConfig, NatsConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, MessageHandler, MessageMetadata, Nats, Outcome, TopologyBuilder,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsImage, NatsServerCmd};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderCreated {
    order_id: String,
    amount: f64,
}

shove::define_topic!(
    OrderTopic,
    OrderCreated,
    TopologyBuilder::new("orders")
        .hold_queue(Duration::from_secs(1))
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

struct OrderHandler;

impl MessageHandler<OrderTopic> for OrderHandler {
    type Context = ();
    async fn handle(&self, message: OrderCreated, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "Processing order {} (${:.2}) [retry={}]",
            message.order_id, message.amount, metadata.retry_count
        );
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsImage::default().with_cmd(&cmd).start().await?;
    let port = container.get_host_port_ipv4(4222).await?;
    let url = format!("nats://localhost:{port}");

    // [!region connect]
    let broker = Broker::<Nats>::new(NatsConfig::new(&url)).await?;
    // [!endregion connect]
    // [!region declare]
    broker.topology().declare::<OrderTopic>().await?;
    // [!endregion declare]

    // Publish
    // [!region publish]
    let publisher = broker.publisher().await?;
    for i in 0..3 {
        publisher
            .publish::<OrderTopic>(&OrderCreated {
                order_id: format!("ORD-{i}"),
                amount: 99.99 + i as f64,
            })
            .await?;
        println!("Published order ORD-{i}");
    }
    // [!endregion publish]

    // Consume via a coordinated consumer group.
    // [!region consume]
    let mut group = broker.consumer_group();
    group
        .register::<OrderTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::new(1..=1)),
            || OrderHandler,
        )
        .await?;

    let outcome = group
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(10),
        )
        .await;
    // [!endregion consume]

    println!("Done.");
    std::process::exit(outcome.exit_code());
}
