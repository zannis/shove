//! Kafka publish/consume example using `ProtobufCodec`.
//!
//! Demonstrates the per-topic codec slot: an `OrderEvent` defined inline as a
//! `prost::Message` rides through the wire as protobuf instead of JSON.
//!
//! Spins up a Kafka testcontainer automatically (requires a running Docker
//! daemon):
//!
//!     cargo run -q --example kafka_protobuf_pubsub --features "kafka protobuf"

use std::time::Duration;

use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, Kafka, MessageHandler, MessageMetadata, Outcome, ProtobufCodec,
    TopologyBuilder,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

#[derive(Clone, PartialEq, ::prost::Message)]
struct OrderEvent {
    #[prost(string, tag = "1")]
    order_id: String,
    #[prost(double, tag = "2")]
    amount: f64,
}

shove::define_topic!(
    Orders,
    OrderEvent,
    TopologyBuilder::new("kafka-orders-proto").dlq().build(),
    codec = ProtobufCodec<OrderEvent>
);

struct OrderHandler;

impl MessageHandler<Orders> for OrderHandler {
    type Context = ();
    async fn handle(&self, message: OrderEvent, _: MessageMetadata, _: &()) -> Outcome {
        println!(
            "Decoded protobuf order {} (${:.2})",
            message.order_id, message.amount
        );
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let container = KafkaImage::default().start().await?;
    let port = container.get_host_port_ipv4(apache::KAFKA_PORT).await?;
    let bootstrap = format!("127.0.0.1:{port}");

    let broker = Broker::<Kafka>::new(KafkaConfig::new(&bootstrap)).await?;
    broker.topology().declare::<Orders>().await?;

    let publisher = broker.publisher().await?;
    publisher
        .publish::<Orders>(&OrderEvent {
            order_id: "ORD-1".into(),
            amount: 99.99,
        })
        .await?;
    println!("Published protobuf-encoded order ORD-1");

    let mut group = broker.consumer_group();
    group
        .register::<Orders, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
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

    println!("Done.");
    std::process::exit(outcome.exit_code());
}
