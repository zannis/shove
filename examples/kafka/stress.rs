//! Stress benchmarks for the Kafka backend.
//!
//! Reads bootstrap servers from `KAFKA_BOOTSTRAP` (default `localhost:9092`).
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     KAFKA_BOOTSTRAP=localhost:9092 cargo run -q --example kafka_stress --features kafka -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig};
use shove::{Broker, Kafka};

use harness::{HarnessConfig, run_all_scenarios};

#[tokio::main]
async fn main() {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
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
}
