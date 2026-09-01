//! Stress benchmarks for the in-memory backend.
//!
//!     cargo run -q --release --example inmemory_stress --features inmemory
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --tier moderate
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --handler fast
//!
//! The sampled core matrix that feeds `benches/results/bench-results.json`:
//!
//!     cargo run -q --release --example inmemory_stress --features inmemory -- \
//!         --flow all --payload all --tier moderate --handler fast \
//!         --consumers 1,8,32 --results-file benches/results/bench-results.json
//!
//! No containers, no external deps — useful as a ceiling for framework
//! overhead under different handler profiles.

#[path = "../common/stress_test.rs"]
mod harness;

use std::num::NonZeroUsize;

use shove::inmemory::{InMemoryConfig, InMemoryConsumer, InMemoryConsumerGroupConfig};
use shove::{Backend, InMemory};

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

/// Per-queue capacity for the benchmark broker.
///
/// The default is 10 000, and publishers *block* when a queue is full. The
/// publish-only flows have no consumer draining behind them, so anything past
/// the default would wedge on backpressure rather than measure a publish rate.
/// This is a bound, not a preallocation, so raising it costs nothing until the
/// messages actually exist. Scenarios larger than this still fail cleanly —
/// the harness bounds every publish phase by the scenario deadline.
const QUEUE_CAPACITY: usize = 1_000_000;

#[tokio::main]
async fn main() {
    // The drain runs on the client the fill phase used. For this backend that
    // is not an optimisation but a correctness requirement: InMemory's queues
    // live inside the client, so a second client would drain an empty DLQ.
    let dlq_drain: DlqDrainFn<InMemory> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = InMemoryConsumer::new(client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
        })
    });

    let hcfg = HarnessConfig::<InMemory>::new("inmemory")
        .with_broker("shove in-process", env!("CARGO_PKG_VERSION"), "in-process")
        .with_dlq_drain(dlq_drain);

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
