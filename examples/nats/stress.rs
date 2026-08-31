//! Stress benchmarks for the NATS JetStream backend.
//!
//! Spins up a NATS JetStream testcontainer for the lifetime of the process.
//! Requires a running Docker daemon.
//!
//!     cargo run -q --example nats_stress --features nats
//!     cargo run -q --example nats_stress --features nats -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use async_nats::jetstream;
use shove::nats::{NatsConfig, NatsConsumer, NatsConsumerGroupConfig};
use shove::{Backend, Nats};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsImage, NatsServerCmd};

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

const STREAM_NAME: &str = "shove-stress-bench";

/// Image tag started by `testcontainers_modules::nats`, recorded in the
/// results provenance so a reader knows which server produced the numbers.
const NATS_VERSION: &str = "2.10";

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

    wait_until_ready(&url).await;

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
            let js = jetstream::new(client);
            let _ = js.delete_stream(STREAM_NAME).await;
        })
    });

    // The drain shares the fill phase's client, so it reads the DLQ the fill
    // just populated instead of racing a second connection against it.
    let dlq_drain: DlqDrainFn<Nats> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = NatsConsumer::new(client);
            let _ = consumer.run_dlq::<StressTestTopic, _>(handler, ()).await;
        })
    });

    let hcfg = HarnessConfig::<Nats>::new("nats")
        .with_purge(purge)
        .with_broker("NATS JetStream", NATS_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain);
    run_all_scenarios(
        hcfg,
        || {
            let url = url.clone();
            async move {
                <Nats as Backend>::connect(NatsConfig::new(&url))
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

/// Block until JetStream is accepting requests. Testcontainers exits
/// `.start()` once nats-server logs that it's listening, but JetStream
/// initialization can lag a few hundred ms behind. Issuing an account-info
/// call confirms the JS API is actually responding.
async fn wait_until_ready(url: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(client) = async_nats::connect(url).await {
            let js = jetstream::new(client);
            if js.query_account().await.is_ok() {
                return;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("NATS JetStream did not become ready within 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
