//! Stress benchmarks for the in-memory backend.
//!
//!     cargo run -q --release --example inmemory_stress --features inmemory
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --tier moderate
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --handler fast
//!
//! No containers, no external deps — useful as a ceiling for framework
//! overhead under different handler profiles.

#[path = "../common/stress_test.rs"]
mod harness;

use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{Broker, InMemory};

use harness::{HarnessConfig, run_all_scenarios};

#[tokio::main]
async fn main() {
    let hcfg = HarnessConfig::<InMemory>::new("inmemory");
    run_all_scenarios(
        hcfg,
        || async {
            Broker::<InMemory>::new(InMemoryConfig::default())
                .await
                .expect("connect InMemory")
        },
        |consumers, prefetch, _concurrent| {
            InMemoryConsumerGroupConfig::new(consumers..=consumers).with_prefetch_count(prefetch)
        },
    )
    .await;
}
