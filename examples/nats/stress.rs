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

/// Image tag started by `testcontainers_modules::nats` (its pinned default),
/// recorded in the
/// results provenance so a reader knows which server produced the numbers.
const NATS_VERSION: &str = "2.10.14";

#[tokio::main]
async fn main() {
    harness::spawn_ctrlc_watcher();
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
    let _container = harness::ContainerGuard::new(container);
    let url = format!("nats://localhost:{port}");

    wait_until_ready(&url).await;

    let purge_url = url.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let url = purge_url.clone();
        Box::pin(async move {
            // Drop the topology's streams (and their durable consumers) so the
            // next scenario creates them fresh with its own config. JetStream
            // `create_consumer` upserts, but changing `max_ack_pending` on an
            // existing consumer requires explicit update — cleanest to drop.
            //
            // The DLQ is its own stream (`{queue}-dlq`), not a subject inside
            // the main one, so it must be dropped by name too — the dlq_drain
            // flow measures the drain of a queue it filled itself, and a
            // leftover entry from the previous scenario counts toward this
            // scenario's N.
            let client = async_nats::connect(&url)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let js = jetstream::new(client);
            for stream in std::iter::once(topology.queue()).chain(topology.dlq()) {
                if let Err(e) = js.delete_stream(stream).await {
                    // Only a stream that does not exist is "nothing to
                    // purge" — a transport or auth failure here means the
                    // previous scenario's stream may still hold messages.
                    let not_found = matches!(
                        e.kind(),
                        jetstream::context::DeleteStreamErrorKind::JetStream(ref js_err)
                            if js_err.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND
                    );
                    if !not_found {
                        return Err(format!("delete stream {stream}: {e}"));
                    }
                }
            }
            Ok(())
        })
    });

    // The drain shares the fill phase's client, so it reads the DLQ the fill
    // just populated instead of racing a second connection against it.
    let dlq_drain: DlqDrainFn<Nats> = Box::new(|client, handler, _stop| {
        // This backend's `run_dlq` exits when the teardown closes the client;
        // the stop token is for backends without that path (see `DlqDrainFn`).
        Box::pin(async move {
            let consumer = NatsConsumer::new(client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
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
