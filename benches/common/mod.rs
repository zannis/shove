//! Fixtures shared by shove's benchmark targets.
//!
//! Two things live here rather than in one bench file, because more than one
//! target needs them:
//!
//! - [`PAYLOAD_SIZES`], so the payload dimension is one set rather than a
//!   constant per target. It used to be a hardcoded `256` in
//!   `publish_throughput.rs`, which made message size invisible in every
//!   number the benchmarks produced.
//! - The Tier A coverage manifest ([`TIER_A_COVERAGE`]), so "which flow is
//!   measured, and which is not measurable here and why" is a single checked
//!   table instead of prose scattered across three files.
//!
//! Each target uses a subset of this module, so it allows dead code rather
//! than growing a `#[cfg]` island per target — an in-file feature gate is the
//! shape that passes one lint job and reddens another.

#![allow(dead_code)]

/// Payload sizes threaded through every payload-sensitive benchmark:
/// 64 B, 1 KiB, 64 KiB.
///
/// Message size is the dominant serde-and-wire lever, so it is a benchmark
/// input dimension rather than a constant baked into one target.
pub const PAYLOAD_SIZES: [usize; 3] = [64, 1024, 65_536];

/// A payload of exactly `bytes` ASCII characters.
pub fn payload(bytes: usize) -> String {
    "x".repeat(bytes)
}

/// How a flow is covered by the Tier A (InMemory + pure-path) benchmarks.
pub enum Coverage {
    /// Measured. Carries the bench target and the criterion group the
    /// measurements land in, so a reader can go straight to the numbers.
    Benched {
        target: &'static str,
        group: &'static str,
    },
    /// Not measurable on this tier, with the reason. A flow recorded here is
    /// deliberately absent, not forgotten.
    NotApplicable(&'static str),
}

/// One row of the coverage manifest.
pub struct FlowCoverage {
    pub flow: &'static str,
    pub coverage: Coverage,
}

/// The closed flow set every tier reports against.
///
/// Kept verbatim, and in the same order, as the flow table the benchmark
/// results schema fixes. Adding a flow here without a matching
/// [`TIER_A_COVERAGE`] row aborts the bench run — see [`report_coverage`].
pub const CANONICAL_FLOWS: [&str; 10] = [
    "publish_single",
    "publish_batch",
    "consume_parallel",
    "consume_fifo",
    "consume_batch",
    "consumer_group",
    "supervisor",
    "broadcast",
    "dlq_drain",
    "autoscaler",
];

/// What Tier A does about each canonical flow.
pub const TIER_A_COVERAGE: [FlowCoverage; 10] = [
    FlowCoverage {
        flow: "publish_single",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_publish",
        },
    },
    FlowCoverage {
        flow: "publish_batch",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_publish",
        },
    },
    FlowCoverage {
        flow: "consume_parallel",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_consume",
        },
    },
    FlowCoverage {
        flow: "consume_fifo",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_consume",
        },
    },
    FlowCoverage {
        flow: "consume_batch",
        coverage: Coverage::NotApplicable(
            "batch consume (`run_batch`) exists on InMemory too, but this \
             bench harness has not wired a consume_batch flow for it yet",
        ),
    },
    FlowCoverage {
        flow: "consumer_group",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_group",
        },
    },
    FlowCoverage {
        flow: "supervisor",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_supervisor",
        },
    },
    FlowCoverage {
        flow: "broadcast",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_broadcast",
        },
    },
    FlowCoverage {
        flow: "dlq_drain",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_dlq",
        },
    },
    FlowCoverage {
        flow: "autoscaler",
        coverage: Coverage::Benched {
            target: "pure_paths",
            group: "autoscaler_decision",
        },
    },
];

/// The broker-free paths Tier A also measures. Not part of the canonical flow
/// set — these are shove's own per-message cost with no broker in the picture
/// at all.
pub const TIER_A_PURE_PATHS: [FlowCoverage; 3] = [
    FlowCoverage {
        flow: "codec",
        coverage: Coverage::Benched {
            target: "pure_paths",
            group: "codec",
        },
    },
    FlowCoverage {
        flow: "topology_build",
        coverage: Coverage::Benched {
            target: "pure_paths",
            group: "topology_build",
        },
    },
    FlowCoverage {
        flow: "route_outcome",
        coverage: Coverage::Benched {
            target: "inmemory_flows",
            group: "inmemory_route_outcome",
        },
    },
];

/// Print the coverage manifest, then abort if it is not complete.
///
/// The assertion is the point: every canonical flow must appear exactly once,
/// and no unknown flow may appear. A flow added to the schema's closed set
/// without a coverage row fails the bench run loudly instead of going missing
/// from the results with nothing to notice it.
///
/// Panicking here is fail-fast harness startup, not a runtime path.
pub fn report_coverage() {
    eprintln!();
    eprintln!("Tier A coverage — canonical flows");
    eprintln!("{}", "-".repeat(78));
    for entry in &TIER_A_COVERAGE {
        eprintln!("  {:<18} {}", entry.flow, describe(&entry.coverage));
    }
    eprintln!();
    eprintln!("Tier A coverage — pure paths");
    eprintln!("{}", "-".repeat(78));
    for entry in &TIER_A_PURE_PATHS {
        eprintln!("  {:<18} {}", entry.flow, describe(&entry.coverage));
    }
    eprintln!();

    for flow in CANONICAL_FLOWS {
        let hits = TIER_A_COVERAGE.iter().filter(|e| e.flow == flow).count();
        assert!(
            hits == 1,
            "flow '{flow}' has {hits} rows in the Tier A coverage manifest, expected exactly 1"
        );
    }
    for entry in &TIER_A_COVERAGE {
        assert!(
            CANONICAL_FLOWS.contains(&entry.flow),
            "'{}' is in the Tier A coverage manifest but not in the canonical flow set",
            entry.flow
        );
    }
}

fn describe(coverage: &Coverage) -> String {
    match coverage {
        Coverage::Benched { target, group } => format!("benched — {target}::{group}"),
        Coverage::NotApplicable(reason) => format!("not applicable — {reason}"),
    }
}
