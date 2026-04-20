//! Stress benchmarks for the NATS JetStream backend.
//!
//! Reads the NATS URL from `NATS_URL` (default `nats://localhost:4222`).
//!
//!     cargo run -q --example nats_stress --features nats
//!     NATS_URL=nats://localhost:4222 cargo run -q --example nats_stress --features nats -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::nats::{NatsConfig, NatsConsumerGroupConfig};
use shove::{Broker, Nats};

use harness::{HarnessConfig, run_all_scenarios};

#[tokio::main]
async fn main() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let hcfg = HarnessConfig::<Nats>::new("nats");
    run_all_scenarios(
        hcfg,
        || {
            let url = url.clone();
            async move {
                Broker::<Nats>::new(NatsConfig::new(&url))
                    .await
                    .expect("connect NATS")
            }
        },
        |consumers, prefetch, concurrent| {
            NatsConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;
}
