//! NATS JetStream publish/consume example using `RawBytesCodec`.
//!
//! Shows the wire-format escape hatch: `Message = Vec<u8>` lets a handler
//! interpret the payload itself (Schema Registry framing, opaque blobs,
//! third-party encodings).
//!
//! Spins up a NATS JetStream testcontainer automatically (requires a running
//! Docker daemon):
//!
//!     cargo run -q --example nats_raw_bytes --features nats

use std::time::Duration;

use shove::nats::{NatsConfig, NatsConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, MessageHandler, MessageMetadata, Nats, Outcome, RawBytesCodec,
    TopologyBuilder,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsImage, NatsServerCmd};

// [!region topic]
shove::define_topic!(
    RawTopic,
    Vec<u8>,
    TopologyBuilder::new("raw-bytes").dlq().build(),
    codec = RawBytesCodec
);
// [!endregion topic]

// [!region handler]
struct RawHandler;

impl MessageHandler<RawTopic> for RawHandler {
    type Context = ();
    async fn handle(&self, message: Vec<u8>, _: MessageMetadata, _: &()) -> Outcome {
        println!("Received {} bytes: {:?}", message.len(), message);
        Outcome::Ack
    }
}
// [!endregion handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // [!region main]
    tracing_subscriber::fmt::init();

    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsImage::default().with_cmd(&cmd).start().await?;
    let port = container.get_host_port_ipv4(4222).await?;
    let url = format!("nats://localhost:{port}");

    // [!region connect]
    let broker = Broker::<Nats>::new(NatsConfig::new(&url)).await?;
    // [!endregion connect]
    // [!region declare]
    broker.topology().declare::<RawTopic>().await?;
    // [!endregion declare]

    // [!region publish]
    let publisher = broker.publisher().await?;
    let payload: Vec<u8> = vec![0x01, 0x02, 0x03];
    publisher.publish::<RawTopic>(&payload).await?;
    println!("Published raw payload: {payload:?}");
    // [!endregion publish]

    // [!region consume]
    let mut group = broker.consumer_group();
    group
        .register::<RawTopic, _>(
            ConsumerGroupConfig::new(NatsConsumerGroupConfig::new(1..=1)),
            || RawHandler,
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
    // [!endregion main]
}
