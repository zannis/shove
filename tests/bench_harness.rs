//! Unit tests for the shared stress-benchmark harness.
//!
//! The harness lives at `examples/common/stress_test.rs` and is pulled into
//! each backend's `stress.rs` with `#[path]`. It carries its own `#[cfg(test)]`
//! module, but Cargo defaults `[[example]]` targets to `test = false`, so that
//! module was never compiled into a test binary and none of its tests ever ran.
//! Including it here — in a real integration-test target — is what makes them
//! execute.
//!
//! Gated on `inmemory` as a whole file: the harness needs a backend feature to
//! compile at all, and a half-gated fixture is exactly what reddens the
//! `clippy --no-default-features --all-targets` leg.

#![cfg(feature = "inmemory")]

#[path = "../examples/common/stress_test.rs"]
mod harness;
