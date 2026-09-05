//! Stress benchmarks for the in-memory backend.
//!
//!     cargo run -q --release --example inmemory_stress --features inmemory
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --tier moderate
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --handler fast
//!
//! The sampled core matrix that feeds `benches/results/bench-results.json` is
//! pinned in `scripts/bench.sh` (runbook: `benches/README.md`):
//!
//!     scripts/bench.sh inmemory
//!
//! No containers, no external deps — useful as a ceiling for framework
//! overhead under different handler profiles.

#[path = "../common/stress_test.rs"]
mod harness;

use std::num::NonZeroUsize;
use std::time::Duration;

use shove::batch_consumer::BatchConsumerOptions;
use shove::inmemory::{InMemoryConfig, InMemoryConsumer, InMemoryConsumerGroupConfig};
use shove::{Backend, Broker, InMemory};

use harness::{BatchConsumeFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

/// Per-queue capacity for the benchmark broker.
///
/// The default is 10 000, and publishers *block* when a queue is full. The
/// publish-only flows have no consumer draining behind them, and a drain
/// scenario publishes its whole corpus before any consumer starts, so
/// anything past the default would wedge on backpressure rather than measure.
/// This is a bound, not a preallocation, so raising it costs nothing until the
/// messages actually exist. It covers the pinned matrix's largest drain corpus
/// (`--drain-messages` in `scripts/bench.sh`) with room to spare, and the
/// harness is told the bound so a corpus above it is refused before the sweep
/// rather than blocking the fill forever.
const QUEUE_CAPACITY: usize = 8_000_000;

#[tokio::main]
async fn main() {
    // The drain runs on the client the fill phase used. For this backend that
    // is not an optimisation but a correctness requirement: InMemory's queues
    // live inside the client, so a second client would drain an empty DLQ.
    let dlq_drain: DlqDrainFn<InMemory> = Box::new(|client, handler, _stop| {
        // This backend's `run_dlq` exits when the teardown closes the client;
        // the stop token is for backends without that path (see `DlqDrainFn`).
        Box::pin(async move {
            let consumer = InMemoryConsumer::new(client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
        })
    });

    // Invoked once per scenario consumer; every loop pops from the same
    // in-process queue, so N invocations compete like N group members.
    let batch_consume: BatchConsumeFn<InMemory> = Box::new(|client, handler, opts, stop| {
        Box::pin(async move {
            Broker::<InMemory>::from_client(client)
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

    let hcfg = HarnessConfig::<InMemory>::new("inmemory")
        .with_broker("shove in-process", env!("CARGO_PKG_VERSION"), "in-process")
        .with_prefill_capacity(QUEUE_CAPACITY as u64)
        .with_dlq_drain(dlq_drain)
        .with_batch_consume(batch_consume);

    run_all_scenarios(
        hcfg,
        || async {
            let capacity =
                NonZeroUsize::new(QUEUE_CAPACITY).expect("QUEUE_CAPACITY is a non-zero constant");
            <InMemory as Backend>::connect(
                InMemoryConfig::default().with_default_capacity(capacity),
            )
            .await
            .expect("connect InMemory")
        },
        |consumers, prefetch, _concurrent| {
            InMemoryConsumerGroupConfig::new(consumers..=consumers).with_prefetch_count(prefetch)
        },
    )
    .await;
}

/// Map the scenario's batch knobs onto `BatchConsumerOptions`; everything
/// else stays at shove's defaults.
fn batch_consumer_options(opts: harness::BatchOptions) -> BatchConsumerOptions<InMemory> {
    BatchConsumerOptions::new()
        .with_max_batch_size(opts.max_batch_size.get())
        .with_max_batch_age(Duration::from_millis(opts.max_batch_age_ms.get()))
}
