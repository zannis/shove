//! Stress benchmarks for the Kafka backend.
//!
//! Spins up a Kafka testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     cargo run -q --example kafka_stress --features kafka -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig};
use shove::{Broker, Kafka};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

use harness::{HarnessConfig, run_all_scenarios};

const TOPIC_NAME: &str = "shove-stress-bench";
const DLQ_NAME: &str = "shove-stress-bench-dlq";

#[tokio::main]
async fn main() {
    let container = KafkaImage::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to read Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");

    let purge_bootstrap = bootstrap.clone();
    let purge: harness::PurgeFn = Box::new(move || {
        let bootstrap = purge_bootstrap.clone();
        Box::pin(async move {
            // Delete and recreate: removes leftover messages AND lets the next
            // scenario re-declare with a partition count sized to its own
            // `max_consumers`. Kafka's `ensure_partitions` only expands, so
            // this is the simplest way to reset to a clean baseline.
            let Ok(admin): Result<AdminClient<DefaultClientContext>, _> = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
            else {
                return;
            };
            let _ = admin
                .delete_topics(&[TOPIC_NAME, DLQ_NAME], &AdminOptions::new())
                .await;
        })
    });

    let hcfg = HarnessConfig::<Kafka>::new("kafka").with_purge(purge);
    run_all_scenarios(
        hcfg,
        || {
            let bootstrap = bootstrap.clone();
            async move {
                Broker::<Kafka>::new(KafkaConfig::new(&bootstrap))
                    .await
                    .expect("connect Kafka")
            }
        },
        |consumers, prefetch, concurrent| {
            KafkaConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;

    drop(container);
}
