//! Stress benchmarks for the RabbitMQ backend.
//!
//! Reads the AMQP URI from `RABBITMQ_URI` (default
//! `amqp://guest:guest@localhost:5672`). The consistent-hash exchange plugin
//! must already be enabled on the broker.
//!
//!     cargo run -q --example rabbitmq_stress --features rabbitmq
//!     RABBITMQ_URI=amqp://guest:guest@localhost:5672 cargo run -q --example rabbitmq_stress --features rabbitmq -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::rabbitmq as rmq;
use shove::{Broker, RabbitMq};

use harness::{HarnessConfig, run_all_scenarios};

#[tokio::main]
async fn main() {
    let uri = std::env::var("RABBITMQ_URI")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string());
    let hcfg = HarnessConfig::<RabbitMq>::new("rabbitmq");
    run_all_scenarios(
        hcfg,
        || {
            let uri = uri.clone();
            async move {
                Broker::<RabbitMq>::new(rmq::RabbitMqConfig::new(&uri))
                    .await
                    .expect("connect RabbitMQ")
            }
        },
        |consumers, prefetch, concurrent| {
            rmq::ConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;
}
