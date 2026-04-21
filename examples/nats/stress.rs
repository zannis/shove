//! Stress benchmarks for the NATS JetStream backend.
//!
//! Spins up a NATS JetStream testcontainer for the lifetime of the process.
//! Requires a running Docker daemon.
//!
//!     cargo run -q --example nats_stress --features nats
//!     cargo run -q --example nats_stress --features nats -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::nats::{NatsConfig, NatsConsumerGroupConfig};
use shove::{Broker, Nats};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsImage, NatsServerCmd};

use harness::{HarnessConfig, run_all_scenarios};

#[tokio::main]
async fn main() {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsImage::default()
        .with_cmd(&cmd)
        .start()
        .await
        .expect("failed to start NATS container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to read NATS port");
    let url = format!("nats://localhost:{port}");

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

    drop(container);
}
