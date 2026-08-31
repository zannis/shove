//! Broker-free micro-benchmarks — what shove costs with no broker at all.
//!
//! Three paths every backend routes through, none of which needs a client, a
//! connection or a feature flag:
//!
//!   - **codec** — `Codec::encode` / `Codec::decode`, the per-message serde
//!     cost every publisher and consumer pays.
//!   - **topology build** — `TopologyBuilder::build`, paid once per topic at
//!     startup, and by the FIFO shard-routing path per shard.
//!   - **autoscaler decision** — `ScalingStrategy::evaluate`, the decision an
//!     `Autoscaler` makes on every poll interval.
//!
//! This target deliberately declares **no** `required-features`, so it is
//! built by `cargo clippy --no-default-features --all-targets` and runs under
//! `cargo bench --no-default-features`. That is what keeps "the bench code
//! does not accidentally depend on a broker feature" a checked property
//! rather than a claim.
//!
//! Run with:
//!
//!     cargo bench --no-default-features --bench pure_paths
//!
//! No Docker, no broker.

mod common;

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use shove::{
    Codec, JsonCodec, ScalingMetrics, ScalingStrategy, SequenceFailure, Stabilized,
    ThresholdStrategy, TopologyBuilder,
};

use common::{PAYLOAD_SIZES, payload};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchMsg {
    id: u64,
    payload: String,
}

/// Encode and decode, parameterized by payload size.
///
/// `Throughput::Bytes` is set per input rather than per group, so the report
/// carries a meaningful MiB/s alongside the per-call time.
fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };
        let encoded = <JsonCodec as Codec<BenchMsg>>::encode(&msg).expect("encode bench fixture");

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::new("json_encode", bytes), &msg, |b, msg| {
            b.iter(|| {
                let out = <JsonCodec as Codec<BenchMsg>>::encode(black_box(msg));
                black_box(out.expect("encode"))
            });
        });
        group.bench_with_input(
            BenchmarkId::new("json_decode", bytes),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let out = <JsonCodec as Codec<BenchMsg>>::decode(black_box(encoded));
                    black_box(out.expect("decode"))
                });
            },
        );
    }

    group.finish();
}

/// `TopologyBuilder::build` for the three shapes shove ships: a bare queue, a
/// retry chain with a DLQ, and a sharded FIFO topology.
///
/// Payload-invariant — a topology carries no message — so this group has no
/// payload dimension.
fn bench_topology_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("topology_build");
    group.throughput(Throughput::Elements(1));

    group.bench_function("plain", |b| {
        b.iter(|| black_box(TopologyBuilder::new(black_box("bench-plain")).build()));
    });

    group.bench_function("hold_queues_dlq", |b| {
        b.iter(|| {
            black_box(
                TopologyBuilder::new(black_box("bench-retry"))
                    .hold_queue(Duration::from_secs(1))
                    .hold_queue(Duration::from_secs(10))
                    .hold_queue(Duration::from_secs(60))
                    .dlq()
                    .build(),
            )
        });
    });

    group.bench_function("sequenced_16_shards", |b| {
        b.iter(|| {
            black_box(
                TopologyBuilder::new(black_box("bench-fifo"))
                    .sequenced(SequenceFailure::Skip)
                    .routing_shards(16)
                    .hold_queue(Duration::from_secs(1))
                    .dlq()
                    .build(),
            )
        });
    });

    group.finish();
}

/// The `autoscaler` flow's decision step: one `evaluate` call, the work an
/// `Autoscaler` does on every poll interval.
///
/// `Autoscaler::run` itself is a poll loop whose period is wall-clock sleep —
/// benching it would measure the sleep. The decision is the part shove's own
/// code can regress, so that is what is measured.
///
/// Payload-invariant: a scaling decision reads queue depth and consumer
/// counts, never a message.
fn bench_autoscaler_decision(c: &mut Criterion) {
    let mut group = c.benchmark_group("autoscaler_decision");
    group.throughput(Throughput::Elements(1));

    // Backlog well above capacity, so the strategy takes its scale-up arm
    // rather than the cheapest branch.
    let metrics = ScalingMetrics::new(50_000, 128, 8, 32);

    group.bench_function("threshold", |b| {
        let mut strategy = ThresholdStrategy::default();
        b.iter(|| black_box(strategy.evaluate(black_box("bench-group"), black_box(&metrics))));
    });

    group.bench_function("stabilized_threshold", |b| {
        let mut strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(30),
            Duration::from_secs(60),
        );
        b.iter(|| black_box(strategy.evaluate(black_box("bench-group"), black_box(&metrics))));
    });

    // The per-group state map is what `Stabilized` adds over the bare
    // strategy; 64 live groups exercises the lookup rather than a single hit.
    group.bench_function("stabilized_threshold_64_groups", |b| {
        let mut strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(30),
            Duration::from_secs(60),
        );
        let groups: Vec<String> = (0..64).map(|i| format!("bench-group-{i}")).collect();
        for group_name in &groups {
            strategy.evaluate(group_name, &metrics);
        }
        let mut next = 0usize;
        b.iter(|| {
            next = next.wrapping_add(1) % groups.len();
            let name = groups[next].as_str();
            black_box(strategy.evaluate(black_box(name), black_box(&metrics)))
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_codec,
    bench_topology_build,
    bench_autoscaler_decision
);
criterion_main!(benches);
