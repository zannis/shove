//! Bridge for the RabbitMQ stress wrapper's test module, mirroring
//! `bench_harness_kafka.rs`: example targets default to `test = false`, so the
//! `#[cfg(test)]` module in `examples/rabbitmq/stress.rs` — the test proving
//! the CLI batch knobs reach `BatchConsumerOptions` — is otherwise never
//! compiled into a test binary.
//!
//! Gated on `rabbitmq` *and* `inmemory` because including the wrapper also
//! includes the shared harness, whose own test module drives everything over
//! `shove::InMemory`. The tests execute wherever both features are enabled —
//! locally via `cargo nextest run --features rabbitmq,inmemory`, and in CI on
//! any coverage-matrix entry whose feature list includes both (which entries
//! do is `.github/workflows/ci.yml`'s to say, not this file's).
#![cfg(all(feature = "rabbitmq", feature = "inmemory"))]
#![allow(dead_code)] // the wrapper's `main` and helpers are along for the ride

#[path = "../examples/rabbitmq/stress.rs"]
mod rabbitmq_stress;
