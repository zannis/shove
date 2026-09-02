//! Bridge for the Kafka stress wrapper's test module, mirroring
//! `bench_harness.rs`: example targets default to `test = false`, so the
//! `#[cfg(test)]` module in `examples/kafka/stress.rs` — the tests proving the
//! CLI batch knobs reach `BatchConsumerOptions` — is otherwise never compiled
//! into a test binary.
//!
//! Gated on `kafka` *and* `inmemory` because including the wrapper also
//! includes the shared harness, whose own test module drives everything over
//! `shove::InMemory`. The `coverage (kafka, ...)` CI entry enables both — its
//! feature list adds `inmemory` precisely so this file becomes an executable
//! test target there — so these tests run in CI as well as on any local
//! `cargo nextest run --features kafka,inmemory`.
#![cfg(all(feature = "kafka", feature = "inmemory"))]
#![allow(dead_code)] // the wrapper's `main` and helpers are along for the ride

#[path = "../examples/kafka/stress.rs"]
mod kafka_stress;
