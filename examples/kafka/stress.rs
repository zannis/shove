//! Stress benchmarks for the Kafka backend.
//!
//! Spins up a Kafka testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     cargo run -q --example kafka_stress --features kafka -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig};
use shove::{Broker, Kafka};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

use harness::{HarnessConfig, run_all_scenarios};

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

    let hcfg = HarnessConfig::<Kafka>::new("kafka");
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
