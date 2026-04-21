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

const STREAM_NAME: &str = "shove-stress-bench";

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

    let purge_url = url.clone();
    let purge: harness::PurgeFn = Box::new(move || {
        let url = purge_url.clone();
        Box::pin(async move {
            // Drop the whole stream (and its durable consumer) so the next
            // scenario creates both fresh with its own config. JetStream
            // `create_consumer` upserts, but changing `max_ack_pending` on an
            // existing consumer requires explicit update — cleanest to drop.
            let Ok(client) = async_nats::connect(&url).await else {
                return;
            };
            let js = async_nats::jetstream::new(client);
            let _ = js.delete_stream(STREAM_NAME).await;
        })
    });

    let hcfg = HarnessConfig::<Nats>::new("nats").with_purge(purge);
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
