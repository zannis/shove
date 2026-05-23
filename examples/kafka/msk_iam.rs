//! MSK IAM Kafka publish/consume example.
//!
//! Connects to an IAM-enabled Amazon MSK cluster and round-trips a single
//! message to verify end-to-end connectivity.
//!
//! Required environment variables:
//!
//! - `MSK_BROKERS` — comma-separated bootstrap broker string
//!   (e.g. `b-1.example.kafka.us-east-1.amazonaws.com:9198`)
//! - `AWS_REGION` — AWS region the cluster lives in (e.g. `us-east-1`)
//!
//! Run:
//!
//!     MSK_BROKERS=... AWS_REGION=us-east-1 \
//!       cargo run -q --example kafka_msk_iam --features kafka-msk-iam

use std::sync::OnceLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig, KafkaSasl, KafkaTls};
use shove::{
    Broker, ConsumerGroupConfig, Kafka, MessageHandler, MessageMetadata, Outcome, Topic,
    TopologyBuilder,
};
use uuid::Uuid;

// --------------------------------------------------------------------------
// Message type
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping {
    id: String,
}

// --------------------------------------------------------------------------
// Topic
//
// The queue name contains a per-run UUID so reruns never collide on the same
// cluster.  We initialize the topology once via `OnceLock` before starting
// the broker and then return the same `&'static` reference on every call.
// --------------------------------------------------------------------------

static PING_TOPOLOGY: OnceLock<shove::QueueTopology> = OnceLock::new();

struct PingTopic;

impl Topic for PingTopic {
    type Message = Ping;

    fn topology() -> &'static shove::QueueTopology {
        PING_TOPOLOGY
            .get()
            .expect("PING_TOPOLOGY must be initialised before using PingTopic")
    }
}

// --------------------------------------------------------------------------
// Handler
// --------------------------------------------------------------------------

struct PingHandler;

impl MessageHandler<PingTopic> for PingHandler {
    type Context = ();
    async fn handle(&self, message: Ping, _metadata: MessageMetadata, _: &()) -> Outcome {
        println!("Received ping: id={}", message.id);
        Outcome::Ack
    }
}

// --------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let brokers = std::env::var("MSK_BROKERS").unwrap_or_else(|_| {
        eprintln!("Error: MSK_BROKERS environment variable is not set.");
        eprintln!("Set it to the comma-separated bootstrap broker string of your MSK cluster.");
        eprintln!("Example: MSK_BROKERS=b-1.example.kafka.us-east-1.amazonaws.com:9198");
        std::process::exit(1);
    });

    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| {
        eprintln!("Error: AWS_REGION environment variable is not set.");
        eprintln!("Set it to the AWS region where your MSK cluster lives.");
        eprintln!("Example: AWS_REGION=us-east-1");
        std::process::exit(1);
    });

    // Build the per-run topic name and initialise the static topology.
    let topic_name = format!("shove-msk-iam-example-{}", Uuid::new_v4().simple());
    PING_TOPOLOGY
        .set(TopologyBuilder::new(&topic_name).build())
        .expect("PING_TOPOLOGY already initialised");
    println!("Using topic: {topic_name}");

    // Connect.
    let config = KafkaConfig::new(&brokers)
        .with_tls(KafkaTls::default())
        .with_sasl(KafkaSasl::msk_iam(&region));

    println!("Connecting to MSK cluster at {brokers} (region={region}) …");
    let broker = Broker::<Kafka>::new(config).await?;
    println!("Connected.");

    // Declare the topic.
    broker.topology().declare::<PingTopic>().await?;

    // Publish one message.
    let publisher = broker.publisher().await?;
    let ping_id = Uuid::new_v4().to_string();
    publisher
        .publish::<PingTopic>(&Ping {
            id: ping_id.clone(),
        })
        .await?;
    println!("Published ping: id={ping_id}");

    // Consume until the message arrives (up to 10 s) or ctrl-c.
    let mut group = broker.consumer_group();
    group
        .register::<PingTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            || PingHandler,
        )
        .await?;

    let outcome = group
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(15),
        )
        .await;

    println!("Done.");
    std::process::exit(outcome.exit_code());
}
