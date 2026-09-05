//! Stress benchmarks for the NATS JetStream backend.
//!
//! Spins up a NATS JetStream testcontainer for the lifetime of the process.
//! Requires a running Docker daemon.
//!
//!     cargo run -q --example nats_stress --features nats
//!     cargo run -q --example nats_stress --features nats -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use std::time::Duration;

use async_nats::jetstream;
use shove::batch_consumer::BatchConsumerOptions;
use shove::nats::{NatsConfig, NatsConsumer, NatsConsumerGroupConfig};
use shove::{Backend, Broker, Nats};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsImage, NatsServerCmd};

use harness::{BatchConsumeFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

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

    // The harness invokes it once per scenario consumer; every invocation
    // attaches to the same durable pull consumer for the stream, so N
    // invocations compete for one corpus like N group members. That needs no
    // topology adjustment — where Kafka has to be declared with a partition
    // per consumer before a second member can be assigned any work, a
    // JetStream durable hands its next pull to whichever attached client asks.
    let batch_consume: BatchConsumeFn<Nats> = Box::new(|client, handler, opts, stop| {
        Box::pin(async move {
            Broker::<Nats>::from_client(client)
                .batch_consumer()
                .run::<StressTestTopic, _>(
                    handler,
                    (),
                    batch_consumer_options(opts).with_shutdown(stop),
                )
                .await
                .map_err(|e| format!("run_batch: {e}"))
        })
    });

    let hcfg = HarnessConfig::<Nats>::new("nats")
        .with_purge(purge)
        .with_broker("NATS JetStream", NATS_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain)
        .with_batch_consume(batch_consume);
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

/// Map the scenario's batch knobs onto shove's [`BatchConsumerOptions`].
///
/// Named (rather than inlined in the closure) so a test can prove the CLI
/// values end up inside `BatchConsumerOptions` instead of being parsed and
/// dropped. Everything except the two mapped fields stays at shove's
/// defaults — the scenario's knobs are handed to the primitive, never
/// re-derived here.
fn batch_consumer_options(opts: harness::BatchOptions) -> BatchConsumerOptions<Nats> {
    BatchConsumerOptions::new()
        .with_max_batch_size(opts.max_batch_size.get())
        .with_max_batch_age(Duration::from_millis(opts.max_batch_age_ms.get()))
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

// Example targets default to `test = false`, so this module only runs via
// tests/bench_harness_nats.rs, which pulls this file into a real test target.
#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    #[test]
    fn the_cli_batch_knobs_reach_batch_consumer_options() {
        // The end of the knob's journey: CLI → `Scenario.batch_options` →
        // `BatchConsumeFn` (both proven in the harness tests) → here, into the
        // `BatchConsumerOptions` handed to the generic batch consumer. Read
        // back through shove's getters, not inferred from the builder calls.
        let opts = harness::BatchOptions {
            max_batch_size: NonZeroUsize::new(50).expect("non-zero"),
            max_batch_age_ms: NonZeroU64::new(125).expect("non-zero"),
        };
        let mapped = batch_consumer_options(opts);
        assert_eq!(mapped.max_batch_size(), 50);
        assert_eq!(mapped.max_batch_age(), Duration::from_millis(125));
    }
}
