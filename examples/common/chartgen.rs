//! Chart generation from the versioned benchmark results document.
//!
//! Reads `benches/results/bench-results.json` (the `bench-schema` contract at
//! v4) and renders five SVG chart families into `docs/public/bench/`.
//!
//! This module is the library half of `examples/chartgen.rs`; the example is a
//! thin CLI over it. It is pulled in with `#[path]` from two targets — the
//! example, and `tests/chartgen.rs`, which holds the test functions. Cargo
//! defaults `[[example]]` targets to `test = false`, so tests living in the
//! example target would never execute.
//!
//! ## Output must be byte-deterministic
//!
//! The committed-artifact test byte-compares a fresh render of every family
//! against the committed SVGs (and the epic's CI staleness leg will diff them
//! the same way), so identical input must produce identical bytes on any host.
//! Three rules follow, and every one of them is load-bearing:
//!
//! - **No wall clock.** Every rendered timestamp comes from the document's
//!   `generated_at`, never from `now()`.
//! - **No system fonts.** `plotters` is declared `default-features = false`
//!   without `ttf`, so text extents come from its built-in approximation rather
//!   than from host font metrics, which differ between macOS and CI and would
//!   move every label coordinate in the file.
//! - **Ordered collections only.** `BTreeMap`/`BTreeSet`, never `HashMap` —
//!   hash iteration order changes the emitted element order run to run.
//!
//! ## Every chart ships as a light/dark pair
//!
//! Each family renders twice — `<name>.svg` and `<name>-dark.svg` — from one
//! render path parameterized by [`Mode`]. Both variants paint their own
//! surface, so each file is self-contained on *any* page: the light file
//! stays legible on crates.io (light-only) and docs.rs's dark theme alike,
//! and the dark sibling exists for surfaces that can select it — GitHub's
//! `<picture>`/`prefers-color-scheme`, and the docs site's theme toggle
//! (which an `<img>`-embedded SVG's own media query could never follow; the
//! embedder picks the file, the same way the repo's `-dark` logo assets
//! work). Every color in a variant comes from that mode's validated palette,
//! never from a mid-tone compromise between the two.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use plotters::coord::cartesian::Cartesian2d;
use plotters::coord::ranged1d::{Ranged, ValueFormatter};
use plotters::coord::types::RangedCoordf64;
use plotters::coord::{CoordTranslate, Shift};
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use serde::Deserialize;

// ── The schema contract ─────────────────────────────────────────────────────

/// The only `schema_version` this generator understands.
///
/// A document declaring anything else is refused outright rather than
/// mis-rendered — in either direction. A newer file read as v4 would silently
/// drop or misread whatever changed, and an *older* one is worse than stale:
/// through v3, `duration_secs` on the consume rows included the consumer
/// group's join latency (v4 put the drain behind a readiness barrier), so a
/// prior version's throughput values are not drain rates and must never share
/// an axis with v4 numbers. A chart is exactly the artifact where that goes
/// unnoticed.
pub const SCHEMA_VERSION: u32 = 4;

/// The closed `handler_cost` set from the schema contract — what a row's
/// `throughput_msg_per_sec` is a measurement *of*.
///
/// Unlike an unknown *flow* (additive, safe to render), an unknown marker is
/// refused: every chart here decides whether a number is publishable by this
/// field, so a value outside the set would silently fall out of every filter —
/// and a row dropped by filter is indistinguishable from a row that never
/// existed, which is the omission failure rule 3 exists to prevent.
pub const HANDLER_COSTS: &[&str] = &[
    "framework",
    "setup_bound",
    "handler_amortised",
    "handler_bound",
    "no_handler",
];

/// The marker for "shove's own cost over a pure drain window" — the only rows
/// that may be published as absolute consume throughput.
pub const COST_FRAMEWORK: &str = "framework";
/// A negligible handler whose window could not separate setup from drain. The
/// throughput is a certified *lower bound* on the drain rate (the window can
/// only be too long, never too short), and must not be published as the rate
/// itself.
pub const COST_SETUP_BOUND: &str = "setup_bound";
/// A publish-only row: no consumer is constructed, so the number is publish
/// throughput and there is no setup window to separate.
pub const COST_NO_HANDLER: &str = "no_handler";

/// The closed flow set from the schema contract — the nine flows the
/// harness's merge validation resolves rows and `unsupported[]` entries
/// through, i.e. the only flows a mergeable document can carry. (The
/// harness has further entry points, but their rows never reach a results
/// file.) Used for the empty-run declaration rule and the framework-overhead
/// columns; deliberately **not** used to reject unknown flows — the contract
/// bumps `schema_version` only on removal or semantic change, so a future
/// additive flow must keep rendering (and be named in captions) rather than
/// hard-fail here.
pub const KNOWN_FLOWS: &[&str] = &[
    "publish_single",
    "publish_batch",
    "consume_parallel",
    "consume_fifo",
    "consume_batch",
    "consumer_group",
    "supervisor",
    "broadcast",
    "dlq_drain",
];

/// The backend key whose numbers measure shove itself with the broker removed.
pub const IN_PROCESS_BACKEND: &str = "inmemory";

/// The flows that construct no consumer — the only rows that may carry
/// [`COST_NO_HANDLER`], and the only rows that may not carry anything else.
pub const PUBLISH_FLOWS: &[&str] = &["publish_single", "publish_batch"];

/// The `mode` each known flow runs in — the harness's `Flow::mode`,
/// mirrored. Family 3 places bars by `mode`, so a row whose mode contradicts
/// its flow would publish one flow's number under another mode's bar.
pub const FLOW_MODES: &[(&str, &str)] = &[
    ("publish_single", "parallel"),
    ("publish_batch", "batch"),
    ("consume_parallel", "parallel"),
    ("consume_fifo", "fifo"),
    ("consume_batch", "batch"),
    ("consumer_group", "parallel"),
    ("supervisor", "parallel"),
    ("broadcast", "parallel"),
    ("dlq_drain", "parallel"),
];

/// The harness's handler profiles, by the exact label a row's `handler`
/// carries (its `HandlerProfile` `Display`), split by whether the profile
/// sleeps. The marker is derived from the profile, so a row whose marker
/// says "shove's cost" under a sleeping handler cannot have been written by
/// the harness — and a label outside the set certifies nothing, so it is
/// refused rather than tolerated: a new profile changes what every marker
/// means and is a contract change, not an additive row.
pub const NEGLIGIBLE_HANDLERS: &[&str] = &["zero (no-op)", "fast (1-5ms)"];

/// The consume flows whose driver holds no readiness barrier — the
/// harness's `Flow::holds_readiness_barrier` false set, less the publish
/// flows. Without a barrier no window can be certified as a drain, so a
/// `framework` row for one of these is unproducible.
pub const BARRIERLESS_FLOWS: &[&str] = &["consume_fifo", "dlq_drain"];

/// The payload sizes the harness runs — its `PAYLOAD_SIZES`, mirrored. The
/// label formatter truncates to whole KiB, so a size outside this set could
/// share a label with a real category; the set is closed on the producer
/// side, and a new size is a contract change, not an additive row.
pub const PAYLOAD_SIZES: &[u64] = &[64, 1024, 65536];

/// The flow a row is charted under: the alias target when the flow is
/// another flow's name on one backend, else itself. Every slice selects on
/// this, so a measured SQS `supervisor` row lands in the parallel-consume
/// slice instead of being captioned as a missing `consume_parallel`.
fn canonical_flow(flow: &str) -> &str {
    ALIASED_FLOWS
        .iter()
        .find(|(alias, _)| *alias == flow)
        .map(|(_, target)| *target)
        .unwrap_or(flow)
}
pub const SLEEPING_HANDLERS: &[&str] = &["slow (50-300ms)", "heavy (1-5s)"];
const COST_HANDLER_BOUND: &str = "handler_bound";
const COST_HANDLER_AMORTISED: &str = "handler_amortised";

/// The shortest measured window a throughput may be published from, in
/// seconds — the same floor the harness holds a consume cell to before it
/// will stamp the row `framework` (its `MIN_FRAMEWORK_WINDOW_SECS`).
///
/// A rate is a count over an interval, and under about a second the interval
/// is dominated by scheduler jitter, allocator warm-up and the first poll:
/// the committed dataset's in-process publish rows drained 5,000 messages in
/// 0.008 s, a "631,000 msg/s" that says nothing repeatable. The harness only
/// gates the `framework` marker on this floor, so the publish (`no_handler`)
/// rows and the barrier-less `setup_bound` rows can carry a sub-second window
/// with an honest marker — and this reader has to hold every published bar,
/// absolute or lower-bound, to the floor itself. A `framework` row under it
/// is a row the harness cannot produce, and is refused outright.
pub const MIN_PUBLISHABLE_WINDOW_SECS: f64 = 1.0;

/// Flows that are another flow's name on one backend, not a separate
/// measurement: `(alias, the flow it spells)`.
///
/// The results schema assigns `supervisor` to SQS and `consume_parallel` to
/// every other backend for the same `run` primitive through the same harness
/// (SQS has no coordinated groups, so its parallel consume *is* the
/// supervisor). A run that measured the primitive under one name has not
/// skipped the other — captioning the alias as an unmeasured gap says a
/// measurement was missed when none was.
pub const ALIASED_FLOWS: &[(&str, &str)] = &[("supervisor", "consume_parallel")];

#[derive(Debug, Clone, Deserialize)]
pub struct Document {
    pub schema_version: u32,
    pub generated_at: String,
    pub shove_version: String,
    #[serde(default)]
    pub rust_version: String,
    pub hardware: Hardware,
    pub runs: Vec<BackendRun>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hardware {
    pub label: String,
    #[serde(default)]
    pub cpu: String,
    #[serde(default)]
    pub physical_cores: u32,
    #[serde(default)]
    pub ram_gb: u32,
    #[serde(default)]
    pub os: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendRun {
    pub backend: String,
    #[serde(default)]
    pub broker: Option<Broker>,
    pub representative: bool,
    #[serde(default)]
    pub results: Vec<ScenarioResult>,
    /// Cells the harness ran and could not measure. A failed cell is *absent*
    /// from `results[]`, and an absence reads as a smaller sweep — so every
    /// family whose slice lost cells to failures says so in its caption
    /// instead of rendering a clean-looking chart over the gap.
    #[serde(default)]
    pub failures: Vec<FailedRow>,
    #[serde(default)]
    pub unsupported: Vec<Unsupported>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Broker {
    /// The only broker field the charts read: it names the deployment in the
    /// shape-only caveat. The writer's version/deployment fields stay in the
    /// document for provenance but are not consumed here.
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Unsupported {
    pub flow: String,
    pub reason: String,
}

/// A failed cell: its slice coordinates, to intersect a chart family's
/// slice, and the harness's diagnostic. The captions only count failures,
/// but the diagnostic is what makes an entry an *account* of the cell rather
/// than a bare coordinate — an empty run is exempt from the silent-run rule
/// only because its failures explain it, so a blank one is refused.
#[derive(Debug, Clone, Deserialize)]
pub struct FailedRow {
    pub flow: String,
    pub payload_bytes: u64,
    pub consumers: u32,
    /// The handler profile the failed cell ran under: a profile whose every
    /// cell failed was still run, and the provenance line must say so.
    #[serde(default)]
    pub handler: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioResult {
    pub flow: String,
    pub mode: String,
    pub payload_bytes: u64,
    pub consumers: u32,
    #[serde(default)]
    pub handler: String,
    /// What `throughput_msg_per_sec` measures — one of [`HANDLER_COSTS`].
    ///
    /// `default` so a pre-v2 document still deserialises far enough for the
    /// schema-version check to refuse it *by version*, which names the actual
    /// problem, rather than failing on a missing field. On a v4 document an
    /// absent marker is then caught by [`validate`] as an unknown (empty) one.
    #[serde(default)]
    pub handler_cost: String,
    /// The `consume_batch` knobs a batch row ran with — present on every row
    /// of that flow, absent everywhere else. Non-ignorable since v3: without
    /// them a 50-message-batch row and a 500-message-batch row are
    /// byte-identical, so any chart that publishes a batch number states
    /// these in its caption.
    #[serde(default)]
    pub max_batch_size: Option<u64>,
    #[serde(default)]
    pub max_batch_age_ms: Option<u64>,
    pub throughput_msg_per_sec: f64,
    /// Read solely for one cross-check: a `framework` marker asserts the
    /// window was a drain behind a readiness barrier, and a barrier always
    /// records the setup it excluded — so `framework` with no `setup_secs`
    /// is a row the harness cannot produce (a hand-edit or corruption), and
    /// publishing it as an absolute drain rate is exactly the mislabel the
    /// marker system exists to refuse.
    #[serde(default)]
    pub setup_secs: Option<f64>,
    /// The measured window the throughput was computed over. Read for one
    /// rule: a window under [`MIN_PUBLISHABLE_WINDOW_SECS`] is too short for
    /// its rate to mean anything, so no bar — absolute or lower-bound — is
    /// drawn from it. `default` for the same version-gate reason as
    /// `handler_cost`; an absent or non-positive value is then refused by
    /// [`validate`].
    #[serde(default)]
    pub duration_secs: Option<f64>,
}

impl ScenarioResult {
    /// Whether this row's throughput may be published as an absolute value.
    fn is_framework(&self) -> bool {
        self.handler_cost == COST_FRAMEWORK
    }

    /// Whether the window is long enough for the rate to be published at all
    /// — see [`MIN_PUBLISHABLE_WINDOW_SECS`].
    fn window_ok(&self) -> bool {
        self.duration_secs
            .is_some_and(|d| d.is_finite() && d >= MIN_PUBLISHABLE_WINDOW_SECS)
    }

    /// The measured window, for captions naming a withheld row. Zero only on
    /// a row `validate` has refused.
    fn window_secs(&self) -> f64 {
        self.duration_secs.unwrap_or(0.0)
    }

    /// A negligible-handler consume row whose window includes setup: its
    /// throughput is a lower bound on the drain rate, never the rate itself.
    fn is_setup_bound(&self) -> bool {
        self.handler_cost == COST_SETUP_BOUND
    }
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// Every variant here is a *loud* failure. The whole point of this generator is
/// that a bad results document cannot produce a clean-looking chart.
#[derive(Debug)]
pub enum ChartError {
    /// Rule 1 — a document from a schema this generator does not understand.
    UnsupportedSchemaVersion {
        found: u32,
        expected: u32,
    },
    /// Rule 5 — a backend measured nothing and did not declare why.
    SilentlyEmptyRun {
        backend: String,
        missing: Vec<String>,
    },
    /// Rule 6 — a row whose `handler_cost` is outside the closed set (or
    /// absent). Every publishability decision keys on this field, so an
    /// unclassifiable row cannot be charted *or* safely skipped.
    UnknownHandlerCost {
        backend: String,
        flow: String,
        value: String,
    },
    /// A row that violates a per-row invariant of the current schema: a
    /// non-positive or non-finite throughput, an empty `handler` label, or
    /// batch knobs that contradict the row's own flow. Each is an accept-path
    /// a chart would silently mis-render (a zero rate reads as "not measured",
    /// a missing handler un-states the provenance disclosure, a knob-less
    /// batch bar has no stated batch size).
    MalformedRow {
        backend: String,
        flow: String,
        what: String,
    },
    /// Two runs share a backend key. The charts key on that name, so the
    /// duplicates would silently overwrite each other in some families and
    /// render side by side in others — one document, contradictory charts.
    DuplicateBackendRun {
        backend: String,
    },
    /// A document-level provenance field is empty. A chart without a date is
    /// the problem this generator exists to fix, so an empty one cannot be
    /// allowed to render a blank caption.
    MissingProvenance {
        what: &'static str,
    },
    /// Chart 5 has no meaning without the broker-free baseline.
    MissingInProcessRun,
    /// A chart family found no plottable data at all in the document.
    NoDataForChart {
        family: &'static str,
        slice: String,
    },
    Io(std::io::Error),
    Json(serde_json::Error),
    /// A failure reported by the drawing backend.
    Render(String),
}

impl fmt::Display for ChartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion { found, expected } => write!(
                f,
                "unsupported schema_version {found}: this chartgen understands \
                 only version {expected}. Refusing to render rather than \
                 silently mis-reading a newer document."
            ),
            Self::SilentlyEmptyRun { backend, missing } => write!(
                f,
                "backend `{backend}` has an empty results[] but does not list \
                 every flow in unsupported[] (undeclared: {}). That is a \
                 silently-failed benchmark run, not a capability hole.",
                missing.join(", ")
            ),
            Self::UnknownHandlerCost {
                backend,
                flow,
                value,
            } => write!(
                f,
                "backend `{backend}` has a `{flow}` row whose handler_cost is \
                 `{value}`, which is not one of the schema's markers ({}). \
                 This field decides whether a number may be published, so a \
                 row that cannot be classified cannot be rendered — and \
                 skipping it would be a silent omission.",
                HANDLER_COSTS.join(", ")
            ),
            Self::MalformedRow {
                backend,
                flow,
                what,
            } => write!(
                f,
                "backend `{backend}` has a `{flow}` row that violates a \
                 per-row invariant of this schema version: {what}. Rendering \
                 it would mislabel the number; skipping it would be a silent \
                 omission."
            ),
            Self::DuplicateBackendRun { backend } => write!(
                f,
                "the document carries more than one run for backend \
                 `{backend}`. The charts key on that name, so duplicates \
                 would silently overwrite each other in some families and \
                 render side by side in others."
            ),
            Self::MissingProvenance { what } => write!(
                f,
                "the document's {what} is empty. Every caption renders the \
                 provenance block, and a chart without one is the problem \
                 this generator exists to fix."
            ),
            Self::MissingInProcessRun => write!(
                f,
                "the framework-overhead chart needs a `{IN_PROCESS_BACKEND}` \
                 run — it is the measurement with the broker removed, which is \
                 the only thing 'what does shove itself cost' can mean. No \
                 such run is present in this document."
            ),
            Self::NoDataForChart { family, slice } => {
                write!(f, "chart `{family}` has no data for its slice ({slice})")
            }
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Json(e) => write!(f, "could not parse the results document: {e}"),
            Self::Render(e) => write!(f, "render error: {e}"),
        }
    }
}

impl std::error::Error for ChartError {}

impl From<std::io::Error> for ChartError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ChartError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

// ── Validation: the enforcement rules that gate every render ────────────────

/// Rules 1, 5 and 6 of the schema contract. Checked once, before any chart is
/// drawn, so a bad document fails before it can write a single file.
pub fn validate(doc: &Document) -> Result<(), ChartError> {
    if doc.schema_version != SCHEMA_VERSION {
        return Err(ChartError::UnsupportedSchemaVersion {
            found: doc.schema_version,
            expected: SCHEMA_VERSION,
        });
    }

    // Everything the provenance block prints as text. The numeric hardware
    // fields are not gated: the harness writes 0 where a host does not
    // expose them, and the label already carries that as "0c / 0 GB".
    for (what, value) in [
        ("generated_at", &doc.generated_at),
        ("shove_version", &doc.shove_version),
        ("rust_version", &doc.rust_version),
        ("hardware.label", &doc.hardware.label),
        ("hardware.cpu", &doc.hardware.cpu),
        ("hardware.os", &doc.hardware.os),
    ] {
        if value.trim().is_empty() {
            return Err(ChartError::MissingProvenance { what });
        }
    }

    let mut backends: BTreeSet<&str> = BTreeSet::new();
    for run in &doc.runs {
        if !backends.insert(run.backend.as_str()) {
            return Err(ChartError::DuplicateBackendRun {
                backend: run.backend.clone(),
            });
        }
    }

    for run in &doc.runs {
        for row in &run.results {
            if !HANDLER_COSTS.contains(&row.handler_cost.as_str()) {
                return Err(ChartError::UnknownHandlerCost {
                    backend: run.backend.clone(),
                    flow: row.flow.clone(),
                    value: row.handler_cost.clone(),
                });
            }
            let malformed = |what: &str| {
                Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: row.flow.clone(),
                    what: what.to_string(),
                })
            };
            // A zero or non-finite rate is a failed measurement wearing a
            // row's clothes — the harness records those in `failures[]`, so
            // one in `results[]` is a document lying about what it measured.
            if !(row.throughput_msg_per_sec.is_finite() && row.throughput_msg_per_sec > 0.0) {
                return malformed("throughput_msg_per_sec is not a positive finite number");
            }
            // The provenance line states the handler profile on every chart;
            // a row without the label would silently un-state it.
            if row.handler.is_empty() {
                return malformed("the handler label is empty");
            }
            // A label outside the harness's set certifies no marker.
            if !NEGLIGIBLE_HANDLERS.contains(&row.handler.as_str())
                && !SLEEPING_HANDLERS.contains(&row.handler.as_str())
            {
                return malformed(&format!(
                    "handler `{}` is not one of the harness's profiles ({:?}, {:?})",
                    row.handler, NEGLIGIBLE_HANDLERS, SLEEPING_HANDLERS
                ));
            }
            // No barrier, no setup window to record: see BARRIERLESS_FLOWS.
            if BARRIERLESS_FLOWS.contains(&row.flow.as_str()) && row.setup_secs.is_some() {
                return malformed(&format!(
                    "`{}` holds no readiness barrier, so its rows never record setup_secs",
                    row.flow
                ));
            }
            // No barrier, no certified drain: see BARRIERLESS_FLOWS.
            if row.handler_cost == COST_FRAMEWORK && BARRIERLESS_FLOWS.contains(&row.flow.as_str())
            {
                return malformed(&format!(
                    "a `{COST_FRAMEWORK}` marker on `{}`, whose driver holds no readiness barrier \
                     and so can never separate setup from drain",
                    row.flow
                ));
            }
            // See the field doc: `framework` without a recorded setup window
            // is unproducible by the harness and unpublishable here.
            if row.handler_cost == COST_FRAMEWORK
                && !row.setup_secs.is_some_and(|v| v.is_finite() && v >= 0.0)
            {
                return malformed("a framework row carries no recorded setup window");
            }
            // The marker and the flow must agree: `no_handler` says no
            // consumer was constructed, which only a publish flow can say,
            // and every other marker describes a consume window a publish
            // flow does not have. A consume row wearing `no_handler` would
            // otherwise be published as an absolute cost. (The harness
            // refuses the same contradiction on the way in; a document that
            // carries it was not written by the harness.)
            let is_publish_flow = PUBLISH_FLOWS.contains(&row.flow.as_str());
            let is_publish_marker = row.handler_cost == COST_NO_HANDLER;
            if is_publish_flow != is_publish_marker {
                return malformed(&format!(
                    "handler_cost `{}` contradicts the flow: `{COST_NO_HANDLER}` belongs to the \
                     publish flows and to no other",
                    row.handler_cost
                ));
            }
            if !PAYLOAD_SIZES.contains(&row.payload_bytes) {
                return malformed(&format!(
                    "payload_bytes {} is not one of the harness's payload sizes ({:?})",
                    row.payload_bytes, PAYLOAD_SIZES
                ));
            }
            // A known flow runs in one mode. Unknown flows are additive and
            // carry whatever mode they declare.
            if let Some((_, mode)) = FLOW_MODES.iter().find(|(f, _)| *f == row.flow)
                && row.mode != *mode
            {
                return malformed(&format!(
                    "mode `{}` contradicts the flow, which runs in mode `{mode}`",
                    row.mode
                ));
            }
            // The marker is derived from the handler profile: a sleeping
            // handler's row is `handler_bound` (or `handler_amortised` in
            // batch mode) and nothing else; a negligible handler's row is
            // never either. A row disagreeing was not written by the harness,
            // and publishing it would chart a simulated sleep as shove.
            if !is_publish_flow {
                let label = row.handler.as_str();
                let sleeping_marker = row.handler_cost == COST_HANDLER_BOUND
                    || row.handler_cost == COST_HANDLER_AMORTISED;
                if SLEEPING_HANDLERS.contains(&label) {
                    let expected = if row.mode == "batch" {
                        COST_HANDLER_AMORTISED
                    } else {
                        COST_HANDLER_BOUND
                    };
                    if row.handler_cost != expected {
                        return malformed(&format!(
                            "handler_cost `{}` under the sleeping handler `{}`; the harness \
                             derives `{expected}` for it",
                            row.handler_cost, row.handler
                        ));
                    }
                } else if sleeping_marker {
                    return malformed(&format!(
                        "handler_cost `{}` under the negligible handler `{}`, which never sleeps",
                        row.handler_cost, row.handler
                    ));
                }
            }
            // The window is what makes the throughput a rate; a row without
            // one cannot be held to the publishable-window floor.
            if !row.duration_secs.is_some_and(|d| d.is_finite() && d > 0.0) {
                return malformed("duration_secs is not a positive finite number");
            }
            // The other direction of the same derivation: a negligible
            // handler on a barrier flow, with a recorded setup window and a
            // window over the floor, is what the harness stamps `framework`.
            // `setup_bound` there would publish as a lower bound a number
            // the harness certified as the rate.
            if row.handler_cost == COST_SETUP_BOUND
                && !is_publish_flow
                && !BARRIERLESS_FLOWS.contains(&row.flow.as_str())
                && row.setup_secs.is_some_and(|v| v.is_finite() && v >= 0.0)
                && row.window_ok()
            {
                return malformed(&format!(
                    "`{COST_SETUP_BOUND}` on a barrier flow with a recorded setup window and a \
                     {:.3} s drain — the harness derives `{COST_FRAMEWORK}` for that row",
                    row.window_secs()
                ));
            }
            // The harness stamps `framework` only on a window at or above its
            // own floor, so a shorter one is a hand-edit or a producer
            // regression — and the exact number the marker keeps off an
            // absolute axis.
            if row.handler_cost == COST_FRAMEWORK && !row.window_ok() {
                return malformed(&format!(
                    "a framework row's window ({:.3} s) is under the {MIN_PUBLISHABLE_WINDOW_SECS} s \
                     floor the harness requires for that marker",
                    row.window_secs()
                ));
            }
            // Since v3 the batch knobs are present on every `consume_batch`
            // row and no other — a knob-less batch bar has no stated batch
            // size, and knobs on a non-batch row caption a batch that never
            // ran.
            let has_knobs = row.max_batch_size.is_some() && row.max_batch_age_ms.is_some();
            if row.flow == "consume_batch" && !has_knobs {
                return malformed("a consume_batch row is missing its batch knobs");
            }
            // Zero is unrepresentable in the writer (its builders assert
            // > 0), so a zero here is a corrupt row that would caption
            // "up to 0 messages per batch".
            if row.max_batch_size == Some(0) || row.max_batch_age_ms == Some(0) {
                return malformed("a batch knob is zero, which no run can produce");
            }
            if row.flow != "consume_batch"
                && (row.max_batch_size.is_some() || row.max_batch_age_ms.is_some())
            {
                return malformed("a non-batch row carries batch knobs");
            }
        }
        // The unsupported[] invariants the writer also enforces: a blank
        // reason renders an unexplained absence, and a flow both measured
        // and declared unsupported is a document contradicting itself — the
        // bar would render while the declaration silently vanished. (An
        // *unknown* flow name is deliberately tolerated: a future additive
        // flow may be declared before this reader learns its name.)
        for f in &run.failures {
            if !PAYLOAD_SIZES.contains(&f.payload_bytes) {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: f.flow.clone(),
                    what: format!(
                        "a failures[] entry at payload_bytes {}, which is not one of the \
                         harness's payload sizes ({:?})",
                        f.payload_bytes, PAYLOAD_SIZES
                    ),
                });
            }
            // A failed cell ran under a profile too, and the provenance line
            // has to name it; blank or unknown is refused like a row's.
            if !NEGLIGIBLE_HANDLERS.contains(&f.handler.as_str())
                && !SLEEPING_HANDLERS.contains(&f.handler.as_str())
            {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: f.flow.clone(),
                    what: format!(
                        "a failures[] entry under handler `{}`, which is not one of the \
                         harness's profiles",
                        f.handler
                    ),
                });
            }
            if f.error.trim().is_empty() {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: f.flow.clone(),
                    what: "a failures[] entry carries no error — a coordinate is not an account"
                        .to_string(),
                });
            }
        }
        for u in &run.unsupported {
            if u.reason.trim().is_empty() {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: u.flow.clone(),
                    what: "declared unsupported without a reason".to_string(),
                });
            }
            if run.results.iter().any(|r| r.flow == u.flow) {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: u.flow.clone(),
                    what: "declared unsupported but carries measured rows for the same flow"
                        .to_string(),
                });
            }
            // A failed cell was attempted, so the flow is not unsupported;
            // one of the two accounts is false, and the charts would show
            // only one of them.
            if run.failures.iter().any(|f| f.flow == u.flow) {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: u.flow.clone(),
                    what: "declared unsupported but carries a recorded failure for the same flow"
                        .to_string(),
                });
            }
            // Two declarations for one flow: only one reason would render.
            if run.unsupported.iter().filter(|o| o.flow == u.flow).count() > 1 {
                return Err(ChartError::MalformedRow {
                    backend: run.backend.clone(),
                    flow: u.flow.clone(),
                    what: "declared unsupported more than once".to_string(),
                });
            }
        }
        if !run.results.is_empty() {
            continue;
        }
        // A run that measured nothing but *recorded why* is not silent: its
        // failures[] entries are the loud account of what happened, and the
        // slice captions surface them. Rule 5 targets the run with neither
        // numbers nor an account.
        if !run.failures.is_empty() {
            continue;
        }
        let declared: BTreeSet<&str> = run.unsupported.iter().map(|u| u.flow.as_str()).collect();
        let missing: Vec<String> = KNOWN_FLOWS
            .iter()
            .filter(|flow| !declared.contains(*flow))
            .map(|flow| (*flow).to_string())
            .collect();
        if !missing.is_empty() {
            return Err(ChartError::SilentlyEmptyRun {
                backend: run.backend.clone(),
                missing,
            });
        }
    }

    Ok(())
}

/// Parse and validate a results document from its raw JSON.
///
/// The version gate fires *before* the typed deserialisation: a foreign
/// version's document may lack (or rename) fields this schema requires, and
/// the refusal has to name the version — the actual problem — rather than
/// whichever missing field serde happens to trip on first.
pub fn parse_str(raw: &str) -> Result<Document, ChartError> {
    #[derive(Deserialize)]
    struct VersionProbe {
        #[serde(default)]
        schema_version: u32,
    }
    let probe: VersionProbe = serde_json::from_str(raw)?;
    if probe.schema_version != SCHEMA_VERSION {
        return Err(ChartError::UnsupportedSchemaVersion {
            found: probe.schema_version,
            expected: SCHEMA_VERSION,
        });
    }
    let doc: Document = serde_json::from_str(raw)?;
    validate(&doc)?;
    Ok(doc)
}

pub fn load(path: &Path) -> Result<Document, ChartError> {
    parse_str(&std::fs::read_to_string(path)?)
}

// ── Chart families ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    ThroughputVsConsumers,
    ThroughputVsPayload,
    ParallelVsSequenced,
    DispatchLatency,
    FrameworkOverhead,
}

impl Family {
    /// Every family, in the order they are generated. Fixed, not derived from
    /// iteration over a map, so the set of emitted files is stable.
    pub const ALL: [Family; 5] = [
        Family::ThroughputVsConsumers,
        Family::ThroughputVsPayload,
        Family::ParallelVsSequenced,
        Family::DispatchLatency,
        Family::FrameworkOverhead,
    ];

    /// The filenames are deliberately stable: the docs and README pages that
    /// will embed these charts reference them by name, so a rename here is a
    /// breaking change to those pages, not a refactor. The dark variant is a
    /// `-dark` sibling, matching how every other themed asset in
    /// `docs/public/` names its pair.
    pub fn filename(self, mode: Mode) -> String {
        let stem = match self {
            Self::ThroughputVsConsumers => "throughput-vs-consumers",
            Self::ThroughputVsPayload => "throughput-vs-payload",
            Self::ParallelVsSequenced => "parallel-vs-sequenced",
            Self::DispatchLatency => "dispatch-latency",
            Self::FrameworkOverhead => "framework-overhead",
        };
        format!("{stem}{}.svg", mode.file_suffix())
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::ThroughputVsConsumers => "Throughput vs consumer count",
            Self::ThroughputVsPayload => "Throughput vs payload size",
            Self::ParallelVsSequenced => "Parallel vs sequenced consume",
            Self::DispatchLatency => "Dispatch latency percentiles",
            Self::FrameworkOverhead => "Framework overhead per message",
        }
    }
}

// ── Style: one render path, two selected themes ─────────────────────────────

pub const WIDTH: u32 = 960;
/// Tall enough that a full caption block — every backend's declared holes,
/// the lower-bound and worker-count qualifiers, three provenance lines — still
/// leaves the plot body [`MIN_PLOT_PX`] high. At 560 the committed ordering
/// chart's nineteen caption lines squeezed the bars into ~50px.
pub const HEIGHT: u32 = 640;
/// The least plot-body height the frame will render. Below this the bars are
/// slivers between the legend and the caption, and a chart that cannot be
/// read is refused rather than published.
pub const MIN_PLOT_PX: i32 = 150;

/// Which of the two published variants is being rendered.
///
/// Everything color-shaped flows through the mode's [`Theme`]; layout, text
/// content and enforcement semantics are mode-blind by construction, so the
/// two variants can never disagree about *what* is published, only about the
/// ink it is published in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Light,
    Dark,
}

impl Mode {
    /// Every mode, in generation order. Fixed, like [`Family::ALL`].
    pub const ALL: [Mode; 2] = [Mode::Light, Mode::Dark];

    fn theme(self) -> &'static Theme {
        match self {
            Mode::Light => &LIGHT,
            Mode::Dark => &DARK,
        }
    }

    /// The filename suffix that makes a family's dark sibling. Stated once,
    /// here, so a light/dark pair can never drift apart letter by letter.
    fn file_suffix(self) -> &'static str {
        match self {
            Mode::Light => "",
            Mode::Dark => "-dark",
        }
    }

    /// The series palette, public so the tests can assert that every fill in
    /// a rendered variant comes from its mode's validated set.
    pub fn series(self) -> [RGBColor; 8] {
        self.theme().series
    }

    /// The surface plus the ink roles in fixed order — primary, secondary,
    /// muted — public so the suite can hold a WCAG floor for every ink
    /// against the surface it prints on: the whitelist proves a render uses
    /// only the constants; it cannot catch a typo'd constant itself.
    pub fn inks(self) -> (RGBColor, [RGBColor; 3]) {
        let t = self.theme();
        (t.surface, [t.primary, t.secondary, t.muted])
    }

    /// The pre-blended muted fills, one per series slot (what shape-only and
    /// lower-bound bars actually wear) — public for the same reason as
    /// [`Mode::inks`]: they are the lowest-contrast marks in the system and
    /// need their own visibility floor.
    pub fn muted_fills(self) -> [RGBColor; 8] {
        let t = self.theme();
        t.series.map(|c| t.blend_muted(c))
    }

    /// Every hex this mode is allowed to emit: the chrome roles, the series
    /// slots, and the pre-blended muted fills. The whitelist test renders a
    /// variant and refuses any color outside this set. Seeded from
    /// [`Mode::inks`], [`Mode::series`] and [`Mode::muted_fills`] so the
    /// role inventory is enumerated once — a colour this set knows about is
    /// a colour the contrast floors also see.
    pub fn allowed_hexes(self) -> BTreeSet<String> {
        let t = self.theme();
        let (surface, inks) = self.inks();
        let mut set: BTreeSet<String> = [surface, t.grid, t.baseline]
            .iter()
            .chain(inks.iter())
            .map(hex)
            .collect();
        for colour in self.series().iter().chain(self.muted_fills().iter()) {
            set.insert(hex(colour));
        }
        set
    }
}

/// The one spelling of a colour as uppercase hex (no `#`). Public so the
/// suite compares colours through the same encoding the whitelist uses,
/// never a re-spelled literal.
pub fn hex(colour: &RGBColor) -> String {
    format!("{:02X}{:02X}{:02X}", colour.0, colour.1, colour.2)
}

/// The color roles of one mode. Both instances come from the documented,
/// validator-passed reference palette — light validated against `#fcfcfb`,
/// dark against `#1a1a19`. The dark column is the same hues re-stepped for
/// the dark surface, a *selected* palette, never an automatic flip.
struct Theme {
    /// The painted chart surface. Exactly one canvas-sized rect per file.
    surface: RGBColor,
    /// Title ink.
    primary: RGBColor,
    /// Read-out ink: subtitle, captions, category labels, bar value labels,
    /// the rule-3 "n/s" markers, the refusal-panel sentence, legend text and
    /// line end-labels. Anything a reader consults for meaning.
    secondary: RGBColor,
    /// Furniture ink: axis tick labels, axis descriptions, provenance lines,
    /// end-label leaders. Present, recessive.
    muted: RGBColor,
    /// Hairline gridlines, one step off the surface.
    grid: RGBColor,
    /// The axis rule itself.
    baseline: RGBColor,
    /// Categorical slots 1–8, in the palette's fixed order. Assignment is by
    /// entity (backend), never by iteration index — see [`backend_slots`].
    series: [RGBColor; 8],
}

impl Theme {
    /// A muted series fill (shape-only and lower-bound bars), pre-blended to
    /// an opaque hex: 55% series over the surface. Opaque on purpose — the
    /// rounded bar caps are same-color shape unions, and translucent fills
    /// would compound to visible seams where the shapes overlap.
    fn blend_muted(&self, colour: RGBColor) -> RGBColor {
        let blend = |c: u8, s: u8| -> u8 {
            let v = (u16::from(c) * 55 + u16::from(s) * 45 + 50) / 100;
            // 0..=255 by construction (a convex blend of two u8), but the
            // clamp keeps the cast checked rather than trusted.
            u8::try_from(v).unwrap_or(u8::MAX)
        };
        RGBColor(
            blend(colour.0, self.surface.0),
            blend(colour.1, self.surface.1),
            blend(colour.2, self.surface.2),
        )
    }
}

const LIGHT: Theme = Theme {
    surface: RGBColor(0xFC, 0xFC, 0xFB),
    primary: RGBColor(0x0B, 0x0B, 0x0B),
    secondary: RGBColor(0x52, 0x51, 0x4E),
    muted: RGBColor(0x89, 0x87, 0x81),
    grid: RGBColor(0xE1, 0xE0, 0xD9),
    baseline: RGBColor(0xC3, 0xC2, 0xB7),
    series: [
        RGBColor(0x2A, 0x78, 0xD6), // blue
        RGBColor(0xEB, 0x68, 0x34), // orange
        RGBColor(0x1B, 0xAF, 0x7A), // aqua
        RGBColor(0xED, 0xA1, 0x00), // yellow
        RGBColor(0xE8, 0x7B, 0xA4), // magenta
        RGBColor(0x00, 0x83, 0x00), // green
        RGBColor(0x4A, 0x3A, 0xA7), // violet
        RGBColor(0xE3, 0x49, 0x48), // red
    ],
};

const DARK: Theme = Theme {
    surface: RGBColor(0x1A, 0x1A, 0x19),
    primary: RGBColor(0xFF, 0xFF, 0xFF),
    secondary: RGBColor(0xC3, 0xC2, 0xB7),
    muted: RGBColor(0x89, 0x87, 0x81),
    grid: RGBColor(0x2C, 0x2C, 0x2A),
    baseline: RGBColor(0x38, 0x38, 0x35),
    series: [
        RGBColor(0x39, 0x87, 0xE5),
        RGBColor(0xD9, 0x59, 0x26),
        RGBColor(0x19, 0x9E, 0x70),
        RGBColor(0xC9, 0x85, 0x00),
        RGBColor(0xD5, 0x51, 0x81),
        RGBColor(0x00, 0x83, 0x00),
        RGBColor(0x90, 0x85, 0xE9),
        RGBColor(0xE6, 0x67, 0x67),
    ],
};

const FONT: &str = "sans-serif";
/// Plotters treats these as points and emits roughly `0.806 x` in the SVG's
/// `font-size`, so the declared numbers are ~1.24x the pixel size we actually
/// want. Rendered: title ~21px, everything else ~14.5px — at or above the
/// 3:1 WCAG large-text threshold, which is what makes a single mid-tone ink
/// legible on both a light and a dark page.
const TITLE_PX: i32 = 26;
const LABEL_PX: i32 = 18;
/// Caption/footer text: ~12px rendered. Smaller than the old 18 on purpose —
/// the caption block is contract prose, and at the old size it visually
/// dominated the chart it captions (family 3's twelve lines pushed the plot
/// toward the MIN_PLOT_PX floor). A consequence worth naming: a shorter
/// footer means some documents that previously refused via the frame guard
/// now render — the guard itself is unchanged, the refused set shrinks.
const FOOT_PX: i32 = 15;
/// Bar value labels: ~12px rendered, the smallest size still legible, so a
/// value fits above a narrow grouped bar.
const VALUE_PX: i32 = 15;

/// A series colour by index, for the fixed per-family series lists (family 3
/// colors its three *modes*, family 5 its single series). Wraps
/// deterministically rather than silently reusing the first hue — two series
/// sharing a colour under distinct legend labels is a chart that cannot be
/// read. (The fixed lists are 1–3 long; the wrap is a guard, not a plan.)
fn palette_colour(mode: Mode, i: usize) -> RGBColor {
    let series = mode.theme().series;
    series[i % series.len()]
}

/// The fixed backend→slot map. Color follows the entity: a backend keeps its
/// hue in every chart, in both modes, whatever else is or isn't plotted.
const BACKEND_SLOTS: &[(&str, usize)] = &[
    ("inmemory", 0),
    ("kafka", 1),
    ("nats", 2),
    ("rabbitmq", 3),
    ("redis", 4),
    ("sqs", 5),
];

/// Assign a palette slot to every backend that a line family will draw or
/// name. Known backends take their fixed slot; a backend this file predates
/// takes one of the two remaining slots (7 then 8, in `BTreeMap` order —
/// stable within a document, though not across documents whose *other*
/// unknowns differ). More than two unknowns is a loud error: the palette has
/// eight validated hues and a ninth series color would have to be invented,
/// which is exactly the improvised-hue failure the fixed map exists to
/// prevent. The durable fix for a real new backend is a slot in
/// [`BACKEND_SLOTS`], not a bigger escape hatch.
fn backend_slots<'a, I>(backends: I) -> Result<BTreeMap<&'a str, usize>, ChartError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut slots = BTreeMap::new();
    let mut next_overflow = BACKEND_SLOTS.len();
    for backend in backends {
        let slot = match BACKEND_SLOTS.iter().find(|(name, _)| *name == backend) {
            Some((_, slot)) => *slot,
            None => {
                if next_overflow >= 8 {
                    return Err(ChartError::Render(format!(
                        "backend `{backend}` has no palette slot: the fixed map knows \
                         {} backends and only two unknowns fit the remaining validated \
                         hues — add the backend to BACKEND_SLOTS",
                        BACKEND_SLOTS.len()
                    )));
                }
                let slot = next_overflow;
                next_overflow += 1;
                slot
            }
        };
        slots.insert(backend, slot);
    }
    Ok(slots)
}

/// Read-out text: what a reader consults for meaning.
fn secondary(mode: Mode, px: i32) -> TextStyle<'static> {
    (FONT, px).into_font().color(&mode.theme().secondary)
}

/// Furniture text: axis ticks, descriptions, provenance.
fn muted(mode: Mode, px: i32) -> TextStyle<'static> {
    (FONT, px).into_font().color(&mode.theme().muted)
}

// ── How each backend is accounted for in a chart ────────────────────────────

/// Every backend in the document lands in exactly one of these for every
/// chart. There is deliberately no "silently absent" case: rule 3 exists
/// because omission reads to a reader as "we forgot to measure it".
#[derive(Debug, Clone)]
enum Presence {
    /// Real numbers, safe to publish as absolute values. `withheld` names the
    /// slice's categories that hold no plotted point and why — a shorter
    /// line must read as a caveat, not as a smaller sweep or a failed cell.
    Absolute {
        points: Vec<(f64, f64)>,
        withheld: Vec<Withheld>,
    },
    /// `representative: false` — the shape is real, the magnitude is not.
    /// Values are normalised to their own maximum and the axis numbers never
    /// describe them.
    ShapeOnly {
        points: Vec<(f64, f64)>,
        withheld: Vec<Withheld>,
    },
    /// The slice was measured, but no cell may be published: every row is
    /// setup-bound, under the window floor, or a sleeping handler. Named
    /// rather than plotted — a lower bound drawn as a line would be read as
    /// the rate.
    WithheldOnly(Vec<Withheld>),
    /// Declared in `unsupported[]`: the backend cannot do this flow at all.
    Unsupported(String),
    /// Supported, but this document carries no row for the chart's slice.
    NotMeasured,
}

/// Why one category of a backend's line has no plotted point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WithheldKind {
    /// Rows exist, but every candidate's window is under
    /// [`MIN_PUBLISHABLE_WINDOW_SECS`]. On a barrier-holding flow this is the
    /// only way a negligible-handler row ends up `setup_bound`, so it is
    /// named as the cause rather than as "coordination cost".
    ShortWindow,
    /// A setup-bound row whose window is long enough: the marker means the
    /// driver could not separate setup from drain.
    SetupBound,
    /// Only sleeping-handler rows: the number is the simulated sleep.
    HandlerBound,
    /// The harness ran the cell and recorded a failure for it.
    Failed,
    /// No row and no recorded failure for this category — a gap, not a
    /// capability hole.
    Gap,
}

impl WithheldKind {
    fn describe(self) -> String {
        match self {
            Self::ShortWindow => format!(
                "window under {MIN_PUBLISHABLE_WINDOW_SECS} s, too short to publish as a rate"
            ),
            Self::SetupBound => "setup-bound (window includes coordination cost)".to_string(),
            Self::HandlerBound => {
                "sleeping-handler rows only (the number is the simulated sleep)".to_string()
            }
            Self::Failed => failed_text().to_string(),
            Self::Gap => gap_text().to_string(),
        }
    }
}

/// One withheld cell: the category label it sits at, and why.
#[derive(Debug, Clone)]
struct Withheld {
    label: String,
    kind: WithheldKind,
}

/// "…at consumers 2, 4; …at consumers 8" — the withheld cells grouped by
/// cause, each naming its categories on the chart's own x axis.
fn describe_withheld(withheld: &[Withheld], x_desc: &str) -> String {
    let mut by_kind: BTreeMap<WithheldKind, Vec<&str>> = BTreeMap::new();
    for w in withheld {
        by_kind.entry(w.kind).or_default().push(w.label.as_str());
    }
    by_kind
        .iter()
        .map(|(kind, labels)| format!("{} at {x_desc} {}", kind.describe(), labels.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Normalise to the series' own maximum, so the curve's shape survives but no
/// absolute magnitude is ever recoverable from the chart.
fn shape_only(points: Vec<(f64, f64)>, withheld: Vec<Withheld>) -> Presence {
    // The peak is always positive here: every point is a
    // `throughput_msg_per_sec` that `validate` already required to be finite
    // and greater than zero.
    let peak = points
        .iter()
        .fold(0.0f64, |a, (_, y)| a.max(*y))
        .max(f64::MIN_POSITIVE);
    Presence::ShapeOnly {
        points: points.into_iter().map(|(x, y)| (x, y / peak)).collect(),
        withheld,
    }
}

/// Build one `Presence` per backend for a chart slice.
///
/// `extract` returns the slice's *publishable* points for a run (framework
/// rows only); `withheld` returns, per category with no publishable point,
/// why — measured-but-unpublishable cells, and gaps; `flows_for_support`
/// names the flow whose `unsupported[]` entry explains a wholly absent
/// backend. A backend whose every category is a gap measured nothing in the
/// slice and is accounted for as unsupported or not measured instead.
fn presences<F, W>(
    doc: &Document,
    flows_for_support: &[&str],
    extract: F,
    withheld: W,
) -> BTreeMap<String, Presence>
where
    F: Fn(&BackendRun) -> Vec<(f64, f64)>,
    W: Fn(&BackendRun) -> Vec<Withheld>,
{
    let mut out = BTreeMap::new();
    for run in &doc.runs {
        let points = extract(run);
        let withheld = withheld(run);
        let measured_something = withheld.iter().any(|w| w.kind != WithheldKind::Gap);
        let presence = if !points.is_empty() {
            if run.representative {
                Presence::Absolute { points, withheld }
            } else {
                shape_only(points, withheld)
            }
        } else if measured_something {
            Presence::WithheldOnly(withheld)
        } else if let Some(reason) = flows_for_support
            .iter()
            .find_map(|flow| unsupported_reason(run, flow))
        {
            Presence::Unsupported(reason)
        } else {
            Presence::NotMeasured
        };
        out.insert(run.backend.clone(), presence);
    }
    out
}

fn unsupported_reason(run: &BackendRun, flow: &str) -> Option<String> {
    run.unsupported
        .iter()
        .find(|u| u.flow == flow)
        .map(|u| u.reason.clone())
}

// ── Rendering ───────────────────────────────────────────────────────────────

/// Two lines of provenance, rendered into every chart. A chart without a date
/// is the problem this whole thing exists to fix, so this is not optional and
/// not per-family.
fn provenance(doc: &Document) -> Vec<String> {
    let first = format!(
        "Generated {} — shove {} — {}",
        doc.generated_at, doc.shove_version, doc.hardware.label
    );
    let mut parts: Vec<String> = Vec::new();
    if !doc.hardware.cpu.is_empty() {
        parts.push(doc.hardware.cpu.clone());
    }
    if doc.hardware.physical_cores > 0 {
        parts.push(format!("{} cores", doc.hardware.physical_cores));
    }
    if doc.hardware.ram_gb > 0 {
        parts.push(format!("{} GB RAM", doc.hardware.ram_gb));
    }
    if !doc.hardware.os.is_empty() {
        parts.push(doc.hardware.os.clone());
    }
    if !doc.rust_version.is_empty() {
        parts.push(doc.rust_version.clone());
    }
    // The handler profiles the dataset was measured under, on the chart's
    // face: an all-`zero (no-op)` dataset is a deliberate Tier B choice (the
    // charts publish shove's own ceiling, not handler throughput), and that
    // choice must be legible on the artifact itself, not in a PR thread.
    let handlers: BTreeSet<&str> = doc
        .runs
        .iter()
        .flat_map(|run| {
            run.results
                .iter()
                .map(|row| row.handler.as_str())
                .chain(run.failures.iter().map(|f| f.handler.as_str()))
        })
        .filter(|h| !h.is_empty())
        .collect();
    if handlers.is_empty() {
        // A document whose every run is declared unsupported ran no cell
        // under any profile; say so rather than drop the line, so the
        // absence is a statement and not a missing field.
        parts.push("handler: none — no cells were run".to_string());
    } else {
        let joined = handlers.into_iter().collect::<Vec<_>>().join(", ");
        parts.push(format!("handler: {joined}"));
    }
    // The document-level failure count. Per-slice captions name failures a
    // chart lost cells to, but a failure in a flow no family charts (the
    // committed document's broadcast timeouts, for instance) would otherwise
    // be invisible on every published artifact — silence indistinguishable
    // from a clean run.
    let mut lines = vec![first, parts.join(" — ")];
    let failed: usize = doc.runs.iter().map(|r| r.failures.len()).sum();
    if failed > 0 {
        lines.push(format!(
            "{failed} recorded failure(s) in the dataset — see failures[]"
        ));
    }
    lines.retain(|l| !l.is_empty());
    lines
}

fn fmt_count(v: f64) -> String {
    // 999,500+ rounds to "1000k" in the k branch; hand it to the M branch
    // instead so an axis tick never reads "1000k".
    if v >= 999_500.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v >= 10_000.0 {
        format!("{:.0}k", v / 1_000.0)
    } else if v >= 1_000.0 {
        // One decimal in the 1k-10k band: whole-thousand rounding renders
        // consecutive ticks (1200, 1400, …) as the same "1k" label.
        format!("{:.1}k", v / 1_000.0)
    } else if v >= 10.0 {
        format!("{v:.0}")
    } else if v >= 0.95 || v <= 0.0 {
        format!("{v:.1}")
    } else {
        // Sub-unit values: one decimal renders every decade below 0.05 as
        // the same "0.0" label, so print just enough decimals to name the
        // value ("0.01", "0.001"). Past six decimals the decimal spelling
        // (9+ chars) outgrows the space left of the axis — FRAME_MARGIN to
        // the tick anchor at plot-left minus plotters' 10px label offset,
        // ~64px at the ~8px/char the rendered LABEL_PX runs — and would
        // clip at the viewBox; exponent form stays short and exact. The
        // sub-unit tick test asserts the left-edge fit, so shrinking the
        // y-label area breaks loudly rather than clipping silently.
        let decades = -v.log10().floor();
        if decades > 6.0 {
            format!("{v:e}")
        } else {
            // 1..=6 by the branch above, so the cast is exact.
            format!("{:.*}", decades as usize, v)
        }
    }
}

fn fmt_payload(bytes: u64) -> String {
    match bytes {
        b if b >= 1024 * 1024 => format!("{} MiB", b / (1024 * 1024)),
        b if b >= 1024 => format!("{} KiB", b / 1024),
        b => format!("{b} B"),
    }
}

/// Left margin every line of chrome starts at.
const GUTTER: i32 = 24;
/// Where the plot body starts: the chart area's top (title + legend band)
/// plus the chart's own top margin.
const PLOT_TOP: i32 = 70 + 26;
/// Baseline-to-baseline spacing of legend rows.
const LEGEND_ROW: i32 = 16;
/// What sits between the plot body's bottom and the footer's first baseline:
/// the chart's bottom margin plus the reserved gap.
const PLOT_BOTTOM_GAP: i32 = 26 + 18;
/// Baseline-to-baseline spacing for the footer lines.
const LINE: i32 = 16;
/// Characters per footer line.
///
/// An estimate, not a measurement — and deliberately so. `plotters` runs
/// without the `ttf` feature, so it has no real text metrics either, and an
/// estimate is the thing that stays equal on every host. At the rendered
/// ~12.1px the usable width (`WIDTH - 2 * GUTTER`) over a ~0.52em average
/// advance holds ~145 characters; 118 keeps ~150px of slack for wide
/// fallback fonts (the overflow test checks the same extents at 0.55em).
const NOTE_WRAP: usize = 118;

/// Break a note across lines at word boundaries so a long `unsupported[]`
/// reason cannot run off the canvas. A reason that leaves the page is the same
/// failure as no reason at all.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        // A single token longer than the width (a URL in a free-text
        // unsupported[] reason) is split hard: an unbreakable token would
        // otherwise render one line far past the canvas edge — the exact
        // "reason that leaves the page" this function exists to prevent.
        if word.chars().count() > width {
            if !current.is_empty() {
                lines.push(std::mem::take(&mut current));
            }
            let chars: Vec<char> = word.chars().collect();
            for chunk in chars.chunks(width) {
                lines.push(chunk.iter().collect());
            }
            // The final chunk becomes the current line, so a following
            // word may join it when it fits — only the full-width chunks
            // are committed as lines.
            if let Some(last) = lines.pop() {
                current = last;
            }
            continue;
        }
        let extra = if current.is_empty() { 0 } else { 1 };
        if !current.is_empty() && current.chars().count() + extra + word.chars().count() > width {
            lines.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

/// Draw the title, the provenance footer and any non-plotted backend notes,
/// then hand back the region the chart body should occupy.
///
/// `notes` carries the `Unsupported` / `NotMeasured` backends. They are printed
/// as text rather than dropped, because a missing bar and a missing backend are
/// indistinguishable to a reader otherwise.
/// Characters per subtitle line — the subtitle renders at [`LABEL_PX`]
/// (~14.5px), wider than the footer's type, so its wrap is tighter.
const SUBTITLE_WRAP: usize = 100;
/// Baseline-to-baseline spacing for wrapped subtitle lines.
const SUBTITLE_LINE: i32 = 19;

/// What `frame` proved about the canvas it framed: the extra top margin its
/// wrapped subtitle consumed, and the vertical band `[band_top, band_bottom]`
/// its guard proved free between the chrome and the footer. Consumers center
/// or guard against THIS, never against a re-derivation of frame's
/// internals — the re-derived copy is the one that drifts.
struct FrameLayout {
    subtitle_extra: i32,
    band_top: i32,
    band_bottom: i32,
}

fn frame<'b>(
    root: &DrawingArea<SVGBackend<'b>, Shift>,
    doc: &Document,
    mode: Mode,
    title: &str,
    subtitle: &str,
    notes: &[String],
    legend_rows: i32,
) -> Result<(DrawingArea<SVGBackend<'b>, Shift>, FrameLayout), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let theme = mode.theme();

    // The surface: exactly one canvas-sized rect, in the mode's own surface
    // color, so the file is self-contained on any page it is embedded in.
    // The whitelist test counts on there being exactly one.
    root.fill(&theme.surface).map_err(render)?;
    // A hairline ring so the chart reads as a deliberate card where the page
    // plane is close to the surface color. Alpha serializes as an `opacity`
    // attribute, so the emitted hex stays the primary ink's.
    root.draw(&Rectangle::new(
        [(0, 0), (WIDTH as i32 - 1, HEIGHT as i32 - 1)],
        ShapeStyle {
            color: theme.primary.mix(0.10),
            filled: false,
            stroke_width: 1,
        },
    ))
    .map_err(render)?;

    // One legend row fits the band under the subtitle; every further row —
    // and every wrapped subtitle line — pushes the chart body down rather
    // than overprinting the plot.
    let legend_extra = LEGEND_ROW.saturating_mul(legend_rows.saturating_sub(1).max(0));
    let subtitle_lines = wrap(subtitle, SUBTITLE_WRAP);
    let subtitle_extra = SUBTITLE_LINE.saturating_mul(
        i32::try_from(subtitle_lines.len())
            .unwrap_or(i32::MAX)
            .saturating_sub(1)
            .max(0),
    );

    root.draw_text(
        title,
        &(FONT, TITLE_PX).into_font().color(&theme.primary),
        (GUTTER, 18),
    )
    .map_err(render)?;
    for (i, line) in subtitle_lines.iter().enumerate() {
        root.draw_text(
            line,
            &secondary(mode, LABEL_PX),
            (
                GUTTER,
                48i32.saturating_add(
                    SUBTITLE_LINE.saturating_mul(i32::try_from(i).unwrap_or(i32::MAX)),
                ),
            ),
        )
        .map_err(render)?;
    }

    // The footer: caption notes are read-outs (secondary ink), provenance is
    // furniture (muted). Both wrap at the same width and share the upward
    // layout from a fixed bottom, so the last baseline plus its descender
    // always clears the canvas edge.
    let mut footer: Vec<(String, bool)> = notes
        .iter()
        .flat_map(|n| wrap(n, NOTE_WRAP))
        .map(|l| (l, false))
        .collect();
    for line in provenance(doc) {
        footer.extend(wrap(&line, NOTE_WRAP).into_iter().map(|l| (l, true)));
    }

    let bottom = HEIGHT as i32 - 26;
    let lines = i32::try_from(footer.len()).unwrap_or(i32::MAX);
    let top_of_footer = bottom.saturating_sub(LINE.saturating_mul(lines.saturating_sub(1)));
    // The plot body runs from the chart area's top margin to the footer top
    // less the reserved gap and bottom margin; below MIN_PLOT_PX of that, the
    // bars are slivers — and past zero plotters draws the plot over the
    // legend rather than erroring. A document that gets here gets a loud
    // refusal instead of a garbage chart. The band this guard proves free is
    // returned in `FrameLayout`; the refusal panel guards and centers
    // against that, never against a copy of this arithmetic.
    if top_of_footer
        < PLOT_TOP
            .saturating_add(legend_extra)
            .saturating_add(subtitle_extra)
            + MIN_PLOT_PX
            + PLOT_BOTTOM_GAP
    {
        return Err(ChartError::Render(format!(
            "the caption block ({} lines) leaves no room for the chart body",
            footer.len()
        )));
    }
    for (i, (line, is_provenance)) in footer.iter().enumerate() {
        let style = if *is_provenance {
            muted(mode, FOOT_PX)
        } else {
            secondary(mode, FOOT_PX)
        };
        root.draw_text(
            line,
            &style,
            (
                GUTTER,
                top_of_footer
                    .saturating_add(LINE.saturating_mul(i32::try_from(i).unwrap_or(i32::MAX))),
            ),
        )
        .map_err(render)?;
    }

    let reserved = (HEIGHT as i32 - top_of_footer + 18).max(0) as u32;
    Ok((
        root.margin(
            (70 + subtitle_extra + legend_extra).max(0) as u32,
            reserved,
            FRAME_MARGIN as u32,
            FRAME_MARGIN as u32,
        ),
        FrameLayout {
            subtitle_extra,
            band_top: PLOT_TOP
                .saturating_add(legend_extra)
                .saturating_add(subtitle_extra),
            band_bottom: top_of_footer.saturating_sub(PLOT_BOTTOM_GAP),
        },
    ))
}

/// The notes line for each backend that is not plotted.
fn notes_for(doc: &Document, presences: &BTreeMap<String, Presence>, x_desc: &str) -> Vec<String> {
    let mut notes = Vec::new();
    for (backend, presence) in presences {
        match presence {
            // The document's own reason, verbatim, with no prefix of ours:
            // the harness's capability holes are self-describing ("SQS does
            // not implement HasCoordinatedGroups…"), and a hand-declared
            // entry whose reason says "not measured in this document" must
            // not be promoted into a "not supported" library claim by
            // caption prose. The axis n/s marker is the rule-3 marker; the
            // reason carries the classification.
            Presence::Unsupported(reason) => notes.push(format!("{backend}: {reason}")),
            // Deliberately NOT "not supported": this variant means the
            // capability exists and this document simply carries no row for
            // the slice. Publishing it as a capability hole would be a false
            // claim about the library rather than about the run.
            Presence::NotMeasured => notes.push(format!("{backend}: {}", gap_text())),
            Presence::WithheldOnly(withheld) => {
                // A backend whose only presence is failures did not measure
                // anything; "measured, but" would be the wrong prefix.
                if withheld.iter().all(|w| w.kind == WithheldKind::Failed) {
                    notes.push(format!(
                        "{backend}: {}",
                        describe_withheld(withheld, x_desc)
                    ));
                } else {
                    notes.push(format!(
                        "{backend}: measured, but no drain rate is published for this slice — {}",
                        describe_withheld(withheld, x_desc)
                    ));
                }
            }
            Presence::ShapeOnly { withheld, .. } => {
                // The fallback arm exists so the caveat cannot silently
                // vanish if the map key ever diverges from run.backend — a
                // normalised series with no caption would read as absolute.
                let mut note = match doc.runs.iter().find(|r| &r.backend == backend) {
                    Some(run) => shape_only_note(run),
                    None => format!(
                        "{backend}: shape only — not representative; magnitudes \
                         are not published"
                    ),
                };
                // The shape has holes too, and the caveat must not swallow
                // the reasons for them.
                if !withheld.is_empty() {
                    note.push_str(&format!(
                        "; not plotted: {}",
                        describe_withheld(withheld, x_desc)
                    ));
                }
                notes.push(note);
            }
            Presence::Absolute { withheld, .. } if !withheld.is_empty() => notes.push(format!(
                "{backend}: partial — not plotted: {}",
                describe_withheld(withheld, x_desc)
            )),
            Presence::Absolute { .. } => {}
        }
    }
    notes
}

/// The shared "supported but absent" caption text — a claim about the run,
/// never about the library. Hand-copies of this sentence drifted once
/// already; every family reads it from here.
fn gap_text() -> &'static str {
    "no measurement for this slice in this run (supported — a gap, not a \
     capability hole)"
}

/// The shared caption for a slice that was measured but whose every window
/// is under [`MIN_PUBLISHABLE_WINDOW_SECS`]: the row exists, the marker is
/// honest, and the number still is not a rate anyone should read.
/// The shared caption for a slice measured only under a sleeping handler:
/// a real measurement the charts refuse on purpose, never a gap.
fn sleeping_handler_text() -> &'static str {
    "measured only with a sleeping handler — the number is the simulated sleep, not \
     shove, so it is not published"
}

/// The shared caption for a cell the harness ran and could not measure.
fn failed_text() -> &'static str {
    "failed to run — absent, not zero; see failures[] in the results document"
}

fn short_window_text() -> String {
    format!(
        "measured, but every window under {MIN_PUBLISHABLE_WINDOW_SECS} s — too short \
         for the throughput to be a rate, so none is published (not even as a lower bound)"
    )
}

/// Collapse per-mode caption entries into lines. `entries` is
/// `text → (mode index → backends)`; modes that share a text *and* a backend
/// list are named on one line ("sqs / parallel, sequenced (fifo): …") rather
/// than repeating a three-line reason per mode — which is what pushed the
/// committed ordering chart's plot body down to ~50px.
fn mode_notes(entries: &BTreeMap<String, BTreeMap<usize, Vec<String>>>) -> Vec<String> {
    let mut notes = Vec::new();
    for (text, per_mode) in entries {
        // Group the modes by their backend list, preserving mode order.
        let mut segments: Vec<(Vec<String>, Vec<&str>)> = Vec::new();
        for (mi, backends) in per_mode {
            let label = MODES.get(*mi).map(|m| m.2).unwrap_or("?");
            match segments.iter_mut().find(|(b, _)| b == backends) {
                Some((_, labels)) => labels.push(label),
                None => segments.push((backends.clone(), vec![label])),
            }
        }
        let joined = segments
            .iter()
            .map(|(backends, labels)| format!("{} / {}", backends.join(", "), labels.join(", ")))
            .collect::<Vec<_>>()
            .join("; ");
        notes.push(format!("{joined}: {text}"));
    }
    notes
}

/// Why a run's magnitudes are not published, naming the deployment when the
/// document carries one. The LocalStack phrasing is kept verbatim for the
/// deployment the rule was written for; any other non-representative broker
/// is named rather than mislabelled as LocalStack.
fn shape_only_note(run: &BackendRun) -> String {
    let why = match &run.broker {
        Some(b) if b.name.to_ascii_lowercase().contains("localstack") => {
            "LocalStack, not AWS".to_string()
        }
        Some(b) if !b.name.is_empty() => format!("measured on {}", b.name),
        _ => "no representative deployment in this run".to_string(),
    };
    format!(
        "{}: shape only — not representative ({why}); magnitudes are not published",
        run.backend
    )
}

/// Caption lines for cells the harness ran and could not measure in a chart's
/// slice. A failed cell is absent from `results[]`, and an unexplained
/// absence reads as a smaller sweep — or, worse, as a capability hole.
fn failure_notes<P>(runs: &[BackendRun], in_slice: P) -> Vec<String>
where
    P: Fn(&FailedRow) -> bool,
{
    let mut notes = Vec::new();
    for run in runs {
        let failed = run.failures.iter().filter(|f| in_slice(f)).count();
        if failed > 0 {
            notes.push(format!(
                "{}: {failed} cell(s) in this slice failed to run — absent, \
                 not zero; see failures[] in the results document",
                run.backend
            ));
        }
    }
    notes
}

/// The best-observed row by throughput. First-wins on exact ties, which keeps
/// the render deterministic under any future reordering of equal rows.
fn best_by_throughput<'a, I>(rows: I) -> Option<&'a ScenarioResult>
where
    I: Iterator<Item = &'a ScenarioResult>,
{
    rows.fold(None, |acc: Option<&ScenarioResult>, r| match acc {
        Some(a) if a.throughput_msg_per_sec >= r.throughput_msg_per_sec => Some(a),
        _ => Some(r),
    })
}

/// Split a sorted point series into runs of *adjacent* category indices.
///
/// A line segment drawn across a missing category asserts an interpolated
/// value at exactly the x where no publishable number exists (the committed
/// kafka payload sweep had framework rows at 64 B and 64 KiB with a
/// setup-bound 1 KiB between them — the bridging segment read as ~30k msg/s
/// off an absolute axis). Only adjacent categories may be connected; a gap
/// breaks the line, and the dots carry the isolated points.
fn contiguous_runs(points: &[(f64, f64)]) -> Vec<&[(f64, f64)]> {
    let mut runs = Vec::new();
    let mut start = 0;
    for i in 1..points.len() {
        if points[i].0 - points[i - 1].0 > 1.5 {
            runs.push(&points[start..i]);
            start = i;
        }
    }
    if start < points.len() {
        runs.push(&points[start..]);
    }
    runs
}

/// The absolute y-range, taken only from series that are safe to publish.
/// `ShapeOnly` series never widen it — that is what keeps a non-representative
/// magnitude out of the axis numbers.
fn absolute_range(
    presences: &BTreeMap<String, Presence>,
) -> Result<Option<(f64, f64)>, ChartError> {
    let mut peak = f64::MIN;
    let mut floor = f64::MAX;
    let mut seen = false;
    for presence in presences.values() {
        if let Presence::Absolute { points, .. } = presence {
            for (_, y) in points {
                if *y > peak {
                    peak = *y;
                }
                if *y < floor {
                    floor = *y;
                }
                seen = true;
            }
        }
    }
    if seen && peak > 0.0 && floor > 0.0 {
        // The log axis's floor: the decade at or below the smallest plotted
        // value, so the span is data-driven and never plotters' silent
        // `end * 1e-5` clamp (which would give every chart five decades of
        // dead space regardless of the data). `validate` rejects zero and
        // negative throughput but accepts any positive finite value —
        // subnormals included — so the representability checks below are
        // load-bearing, not decoration. `y_lo <= floor <= peak < y_hi`
        // makes the range strictly ordered.
        let y_lo = 10f64.powf(floor.log10().floor());
        if !y_lo.is_finite() || y_lo <= 0.0 {
            return Err(ChartError::Render(format!(
                "axis floor {floor:e} has no representable decade"
            )));
        }
        let y_hi = headroom(peak, 1.12)?;
        // The decade span must stay representable: past ~308 decades the
        // ratio `y_hi / y_lo` overflows to +inf, the shape-only geometric
        // mapping multiplies by it, and the i32 pixel cast saturates —
        // a clean-looking SVG full of garbage coordinates (and plotters
        // grinds through hundreds of decades of ticks on the way there).
        if !(y_hi / y_lo).is_finite() {
            return Err(ChartError::Render(format!(
                "the axis span [{y_lo:e}, {y_hi:e}] covers more decades than can be drawn"
            )));
        }
        Ok(Some((y_lo, y_hi)))
    } else {
        Ok(None)
    }
}

/// `peak * factor`, refusing a peak the multiplication would carry past
/// `f64::MAX`: an infinite axis bound maps every coordinate to NaN, and
/// plotters writes that out as a clean-looking, empty chart.
fn headroom(peak: f64, factor: f64) -> Result<f64, ChartError> {
    if peak > f64::MAX / factor {
        return Err(ChartError::Render(format!(
            "axis peak {peak:e} cannot take {factor}x headroom without overflowing"
        )));
    }
    Ok(peak * factor)
}

fn stroke(colour: RGBColor, width: u32) -> ShapeStyle {
    ShapeStyle {
        color: colour.to_rgba(),
        filled: false,
        stroke_width: width,
    }
}

/// Draw the category labels under the plot area.
///
/// The x axis of every chart here is categorical (consumer counts, payload
/// sizes, backends, flows). Plotters' own tick placement is continuous, so it
/// would put labels between categories; positioning them from the chart's own
/// coordinate mapping is what keeps a label under the bar it names.
/// A centred text style in an explicit ink. The callers pick the role:
/// category labels and the rule-3 "n/s" markers are read-outs (secondary),
/// the x-axis description is furniture (muted) — one shared style hid that
/// distinction and had to go.
fn centred(colour: RGBColor, px: i32) -> TextStyle<'static> {
    (FONT, px)
        .into_font()
        .color(&colour)
        .pos(Pos::new(HPos::Center, VPos::Top))
}

/// Category labels: identity read-outs under the plot.
fn category_label(mode: Mode) -> TextStyle<'static> {
    centred(mode.theme().secondary, LABEL_PX)
}

/// The x-axis description: axis furniture.
fn x_desc_label(mode: Mode) -> TextStyle<'static> {
    centred(mode.theme().muted, LABEL_PX)
}

/// Draw the x-axis description centred under the plot area — one
/// implementation, so a spacing tweak cannot move the two chart kinds apart.
fn draw_x_desc<CT>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &ChartContext<'_, SVGBackend<'_>, CT>,
    mode: Mode,
    x_desc: &str,
) -> Result<(), ChartError>
where
    CT: CoordTranslate<From = (f64, f64)>,
{
    let px = chart.plotting_area().get_pixel_range();
    let (x0, x1) = (px.0.start, px.0.end);
    let y1 = px.1.end;
    root.draw_text(x_desc, &x_desc_label(mode), ((x0 + x1) / 2, y1 + 28))
        .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))
}

fn draw_categories<CT>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &ChartContext<'_, SVGBackend<'_>, CT>,
    mode: Mode,
    labels: &[String],
) -> Result<(), ChartError>
where
    CT: CoordTranslate<From = (f64, f64)>,
{
    let area = chart.plotting_area();
    let y_bottom = area.get_pixel_range().1.end;
    let style = category_label(mode);
    for (i, label) in labels.iter().enumerate() {
        let (px, _) = area.map_coordinate(&(i as f64, 0.0));
        root.draw_text(label, &style, (px, y_bottom + 8))
            .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))?;
    }
    Ok(())
}

/// A legend entry drawn as a swatch plus a label, laid out left to right along
/// the top of the plot area.
/// Where the legend starts.
const LEGEND_ORIGIN: (i32, i32) = (94, 74);

/// The estimated pixel width of a run of label-sized text: 9px per char. An
/// estimate rather than a measurement — text extents are approximated (no
/// ttf feature), and the estimate is what stays equal across hosts. One
/// definition, shared by every fits-or-refuses gate (legend, end-label
/// gutter, mid-chart labels), so the gates cannot disagree about the same
/// string. Saturating: a document-supplied label has no length bound.
fn text_px(label: &str) -> i32 {
    i32::try_from(label.chars().count())
        .unwrap_or(i32::MAX)
        .saturating_mul(9)
}

/// The estimated width of one legend entry: its text plus the swatch and
/// inter-entry padding.
fn legend_entry_width(label: &str) -> i32 {
    text_px(label).saturating_add(44)
}

/// Lay the legend out into rows: `(row index, x)` per entry, and the row
/// count. An entry that does not fit a row by itself has no legible place on
/// this canvas and is refused rather than drawn off the edge.
fn legend_layout(entries: &[(String, RGBColor)]) -> Result<(Vec<(i32, i32)>, i32), ChartError> {
    let right_edge = WIDTH as i32 - FRAME_MARGIN;
    let mut x = LEGEND_ORIGIN.0;
    let mut row = 0i32;
    let mut slots = Vec::with_capacity(entries.len());
    for (label, _) in entries {
        let width = legend_entry_width(label);
        if LEGEND_ORIGIN.0.saturating_add(width) > right_edge {
            return Err(ChartError::Render(format!(
                "legend entry `{label}` is wider than the canvas"
            )));
        }
        if x > LEGEND_ORIGIN.0 && x.saturating_add(width) > right_edge {
            x = LEGEND_ORIGIN.0;
            row = row.saturating_add(1);
        }
        slots.push((row, x));
        x = x.saturating_add(width);
    }
    Ok((slots, row.saturating_add(1)))
}

/// Draw the legend into the slots its caller already laid out — the caller
/// needed `legend_layout` for the row count before framing, so the same
/// slots are passed through rather than recomputed here.
fn draw_legend(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
    entries: &[(String, RGBColor)],
    slots: &[(i32, i32)],
    y_offset: i32,
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    for ((label, colour), (row, x)) in entries.iter().zip(slots.iter().copied()) {
        let y = LEGEND_ORIGIN
            .1
            .saturating_add(y_offset)
            .saturating_add(LEGEND_ROW.saturating_mul(row));
        // The swatch carries the series color; the label wears text ink —
        // identity comes from the mark beside the text, never the text.
        root.draw(&Rectangle::new(
            [(x, y + 3), (x + 14, y + 11)],
            ShapeStyle {
                color: colour.to_rgba(),
                filled: true,
                stroke_width: 0,
            },
        ))
        .map_err(render)?;
        root.draw_text(label, &secondary(mode, LABEL_PX), (x + 20, y))
            .map_err(render)?;
    }
    Ok(())
}

// ── Family 1: throughput vs consumer count ──────────────────────────────────

const HEADLINE_FLOW: &str = "consume_parallel";
/// The payload the single-payload families slice on. 64 B rather than 1 KiB
/// deliberately: the framework corpus floor caps by bytes, so the larger
/// payloads drain in under the 1 s framework window on fast cells and their
/// rows are honestly `setup_bound` — 64 B is where drain-windowed (`framework`)
/// rows exist across the consumer sweep. Every chart states its payload in the
/// subtitle, so the slice is on the chart's face.
const HEADLINE_PAYLOAD: u64 = 64;
const OVERHEAD_PAYLOAD: u64 = 64;
const BASE_CONSUMERS: u32 = 1;

fn render_throughput_vs_consumers(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
) -> Result<(), ChartError> {
    let in_slice = |r: &ScenarioResult| {
        canonical_flow(&r.flow) == HEADLINE_FLOW && r.payload_bytes == HEADLINE_PAYLOAD
    };
    render_line_family(
        doc,
        root,
        mode,
        Family::ThroughputVsConsumers.title(),
        &format!(
            "{} — {} payload — higher is better",
            HEADLINE_FLOW,
            fmt_payload(HEADLINE_PAYLOAD)
        ),
        "throughput-vs-consumers",
        format!(
            "{HEADLINE_FLOW} @ {} ({COST_FRAMEWORK} rows)",
            fmt_payload(HEADLINE_PAYLOAD)
        ),
        "consumers",
        &[HEADLINE_FLOW],
        in_slice,
        |r| r.consumers,
        |c| format!("{c}"),
        |f| canonical_flow(&f.flow) == HEADLINE_FLOW && f.payload_bytes == HEADLINE_PAYLOAD,
        |f| f.consumers,
    )
}

// ── Family 2: throughput vs payload size ────────────────────────────────────

fn render_throughput_vs_payload(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
) -> Result<(), ChartError> {
    let in_slice = |r: &ScenarioResult| {
        canonical_flow(&r.flow) == HEADLINE_FLOW && r.consumers == BASE_CONSUMERS
    };
    render_line_family(
        doc,
        root,
        mode,
        Family::ThroughputVsPayload.title(),
        &format!("{HEADLINE_FLOW} — {BASE_CONSUMERS} consumer — higher is better"),
        "throughput-vs-payload",
        format!("{HEADLINE_FLOW} @ {BASE_CONSUMERS} consumer ({COST_FRAMEWORK} rows)"),
        "payload size",
        &[HEADLINE_FLOW],
        in_slice,
        |r| r.payload_bytes,
        |s| fmt_payload(*s),
        |f| canonical_flow(&f.flow) == HEADLINE_FLOW && f.consumers == BASE_CONSUMERS,
        |f| f.payload_bytes,
    )
}

/// The shared slice pipeline for the two line families: union the category
/// keys from framework rows (a setup-bound cell must not open an empty
/// category), refuse an empty slice, fold each backend to its **best
/// framework row per category** — a valid document can carry two rows at one
/// key (two tiers, two corpus sizes), and a line needs one value per x, not a
/// vertical zigzag whose value is document order — then render.
#[allow(clippy::too_many_arguments)]
fn render_line_family<K, P, KF, LF, FF, FK>(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
    title: &str,
    subtitle: &str,
    family_key: &'static str,
    slice_desc: String,
    x_desc: &str,
    flows_for_support: &[&str],
    in_slice: P,
    key_of: KF,
    label_of: LF,
    in_failed_slice: FF,
    failed_key_of: FK,
) -> Result<(), ChartError>
where
    K: Ord + Copy,
    P: Fn(&ScenarioResult) -> bool + Copy,
    KF: Fn(&ScenarioResult) -> K + Copy,
    LF: Fn(&K) -> String + Copy,
    FF: Fn(&FailedRow) -> bool + Copy,
    FK: Fn(&FailedRow) -> K + Copy,
{
    // Every category any backend attempted — measured, withheld or failed —
    // is on the axis. A category only framework rows opened would drop a
    // payload every backend measured but none could publish, and the sweep
    // would read as one size narrower than it was.
    let mut keys: BTreeSet<K> = BTreeSet::new();
    for run in &doc.runs {
        for r in run.results.iter().filter(|r| in_slice(r)) {
            keys.insert(key_of(r));
        }
        for f in run.failures.iter().filter(|f| in_failed_slice(f)) {
            keys.insert(failed_key_of(f));
        }
    }
    let keys: Vec<K> = keys.into_iter().collect();
    // Nothing attempted in the slice at all is a chart with nothing to say.
    // A slice where every cell is withheld, failed or unsupported is not:
    // the axis and the captions are the account of why no line exists, and
    // refusing here would take every other family down with it.
    if keys.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: family_key,
            slice: slice_desc,
        });
    }
    let presences = presences(
        doc,
        flows_for_support,
        |run| {
            // Positions come straight from the sorted key list, so the
            // points are sorted by construction.
            keys.iter()
                .enumerate()
                .filter_map(|(i, k)| {
                    best_by_throughput(
                        run.results
                            .iter()
                            .filter(|r| in_slice(r) && r.is_framework() && key_of(r) == *k),
                    )
                    .map(|r| (i as f64, r.throughput_msg_per_sec))
                })
                .collect()
        },
        |run| {
            // Every category with no publishable point gets a reason. A
            // failed cell is left to `failure_notes`, which counts it.
            keys.iter()
                .filter_map(|k| {
                    let rows: Vec<&ScenarioResult> = run
                        .results
                        .iter()
                        .filter(|r| in_slice(r) && key_of(r) == *k)
                        .collect();
                    if rows.iter().any(|r| r.is_framework()) {
                        return None;
                    }
                    let failed = run
                        .failures
                        .iter()
                        .any(|f| in_failed_slice(f) && failed_key_of(f) == *k);
                    let kind = if rows.iter().any(|r| r.is_setup_bound()) {
                        if rows
                            .iter()
                            .filter(|r| r.is_setup_bound())
                            .all(|r| !r.window_ok())
                        {
                            WithheldKind::ShortWindow
                        } else {
                            WithheldKind::SetupBound
                        }
                    } else if !rows.is_empty() {
                        WithheldKind::HandlerBound
                    } else if failed {
                        WithheldKind::Failed
                    } else {
                        WithheldKind::Gap
                    };
                    Some(Withheld {
                        label: label_of(k),
                        kind,
                    })
                })
                .collect()
        },
    );

    let labels: Vec<String> = keys.iter().map(label_of).collect();
    let mut extra = alias_notes(doc, in_slice);
    extra.extend(failure_notes(&doc.runs, in_failed_slice));
    line_chart(
        doc,
        root,
        mode,
        title,
        subtitle,
        "messages / second",
        &labels,
        x_desc,
        &presences,
        &extra,
    )
}

/// One caption line per backend whose in-slice rows arrived under an alias:
/// the bar or line is real, and the reader must be told which name it was
/// measured under.
fn alias_notes<P>(doc: &Document, in_slice: P) -> Vec<String>
where
    P: Fn(&ScenarioResult) -> bool,
{
    let mut notes = Vec::new();
    for run in &doc.runs {
        let mut aliases: BTreeSet<(&str, &str)> = BTreeSet::new();
        for r in run.results.iter().filter(|r| in_slice(r)) {
            let target = canonical_flow(&r.flow);
            if target != r.flow {
                aliases.insert((r.flow.as_str(), target));
            }
        }
        for (alias, target) in aliases {
            // The harness declares the canonical flow unsupported on the
            // aliasing backend with a reason that names the alias; that
            // declaration is not a contradiction of the measured rows, it
            // explains them — so it is carried verbatim rather than
            // dropped behind our own prose.
            match unsupported_reason(run, target) {
                Some(reason) => {
                    notes.push(format!("{}: measured as {alias} — {reason}", run.backend))
                }
                None => notes.push(format!(
                    "{}: measured as {alias} — this backend's spelling of {target} (the same \
                     run primitive)",
                    run.backend
                )),
            }
        }
    }
    notes
}

/// One plotted line: its identity, ink, pre-mapped points and weight class.
/// Points are mapped into axis coordinates *before* the axis branch, so the
/// log and linear branches share every drawing decision. `gutter_label` is
/// decided once, from the raw points, where specs are built — gutter sizing
/// and label placement both read it, so they cannot disagree.
struct LineSpec {
    backend: String,
    colour: RGBColor,
    gutter_label: bool,
    points: Vec<(f64, f64)>,
    shape_only: bool,
}

/// The frame's outer margin on the left and right canvas edges (the top
/// carries the 70px title/legend band and the bottom the footer reserve —
/// this is a horizontal bound only). Every horizontal fit check measures
/// against this — never a re-spelled 16 that drifts when the frame is
/// retuned.
const FRAME_MARGIN: i32 = 16;

/// The gap between a line's endpoint dot and its mid-chart end label.
const END_LABEL_OFFSET: i32 = 8;

/// Where a gutter label anchors, right of the plot edge.
const GUTTER_LABEL_INSET: i32 = 10;
/// Where a gutter leader starts, right of its endpoint dot — shared by the
/// drawn stroke and the collision extent, which must describe the same ink.
const LEADER_STUB: i32 = 6;
/// The gap between a leader's end and its label's first character.
const LEADER_END_GAP: i32 = 3;

/// The end-label gutter's floor: room for the widest *fixed* backend name
/// (8 chars at the 9px/char cross-host estimate) plus the leader stub and
/// padding. The gutter grows beyond this to fit longer names — see
/// `end_label_gutter`.
const END_LABEL_GUTTER: i32 = 84;
/// The anchor inset plus right padding around an end label inside the
/// gutter. Derived from the inset so "the gutter fits every name it holds"
/// is structural: a label spans [inset, inset + width] of a gutter at
/// least [width + inset + 2] wide.
const END_LABEL_PAD: i32 = GUTTER_LABEL_INSET + 2;
/// The widest gutter a name may demand before the chart refuses: past a
/// quarter of the canvas the plot body itself stops being legible.
const END_LABEL_GUTTER_MAX: i32 = 240;

/// The gutter width for this chart's plotted names: sized to its content
/// with the legend's own 9px/char estimate, floored at [`END_LABEL_GUTTER`]
/// (which keeps the fixed six backends' charts stable) and refused past the
/// cap — the same fits-or-refuses contract `legend_layout` enforces, never
/// a silent clip at the viewBox edge.
fn end_label_gutter<'a, I>(plotted: I) -> Result<i32, ChartError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut gutter = END_LABEL_GUTTER;
    for name in plotted {
        let need = text_px(name).saturating_add(END_LABEL_PAD);
        if need > END_LABEL_GUTTER_MAX {
            return Err(ChartError::Render(format!(
                "backend `{name}` needs a {need}px end-label gutter (cap \
                 {END_LABEL_GUTTER_MAX}px) — it cannot be labelled legibly on this canvas"
            )));
        }
        gutter = gutter.max(need);
    }
    Ok(gutter)
}
/// Minimum vertical spacing between end-labels, and (halved) the clearance
/// that keeps a clamped label inside the plot band.
const END_LABEL_SPACING: i32 = 14;

#[allow(clippy::too_many_arguments)]
fn line_chart(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
    title: &str,
    subtitle: &str,
    y_desc: &str,
    x_labels: &[String],
    x_desc: &str,
    presences: &BTreeMap<String, Presence>,
    extra_notes: &[String],
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let mut notes = notes_for(doc, presences, x_desc);
    notes.extend_from_slice(extra_notes);
    // Color follows the backend, through the fixed slot map — never the
    // iteration index of whatever happens to be plotted. The lookup is total
    // by construction (`slots` is built from the same keys iterated below),
    // and if that invariant ever breaks it must break loudly: a silent
    // fallback hue is exactly the two-series-one-colour failure the slot map
    // exists to refuse.
    let slots = backend_slots(presences.keys().map(String::as_str))?;
    let colour_of = |backend: &str| -> Result<RGBColor, ChartError> {
        let slot = slots.get(backend).copied().ok_or_else(|| {
            ChartError::Render(format!(
                "backend `{backend}` has no palette slot — colour_of only takes presence-map keys"
            ))
        })?;
        Ok(palette_colour(mode, slot))
    };
    // The legend is known before anything is drawn, and its row count
    // decides where the chart body starts.
    let mut legend: Vec<(String, RGBColor)> = Vec::new();
    for (backend, presence) in presences {
        let label = match presence {
            Presence::Absolute { points, .. } if points.len() == 1 => {
                format!("{backend} (single point)")
            }
            Presence::Absolute { .. } => backend.clone(),
            Presence::ShapeOnly { .. } => format!("{backend} (shape only)"),
            // A line chart has no slot to put the rule-3 marker in, so
            // the legend is the axis-adjacent place a backend with no
            // line must still appear — otherwise the plot reads as a
            // smaller sweep and the caption is the only witness.
            Presence::Unsupported(_) => format!("{backend} (n/s)"),
            Presence::NotMeasured => format!("{backend} (not measured)"),
            Presence::WithheldOnly(withheld) => {
                if withheld.iter().all(|w| w.kind == WithheldKind::Failed) {
                    format!("{backend} (failed)")
                } else {
                    format!("{backend} (withheld)")
                }
            }
        };
        legend.push((label, colour_of(backend)?));
    }
    let (legend_slots, legend_rows) = legend_layout(&legend)?;
    let (area, layout) = frame(root, doc, mode, title, subtitle, &notes, legend_rows)?;

    // Absolute magnitudes come only from representative runs. A shape-only
    // series is drawn against the same box but its values are fractions, so it
    // can never be read off the axis.
    let range = absolute_range(presences)?;

    // Map every series into axis coordinates up front, so the two axis
    // branches below can never disagree about what is drawn — and decide
    // HERE, once, from the raw points, whether each series' end label lives
    // in the gutter. Gutter sizing below and label placement in the body
    // both read `gutter_label`, so they can never disagree about membership.
    let final_cat = x_labels.len().saturating_sub(1) as f64;
    let ends_at_final = |points: &[(f64, f64)]| {
        points
            .last()
            .is_some_and(|(x, _)| (*x - final_cat).abs() < f64::EPSILON)
    };
    let mut specs: Vec<LineSpec> = Vec::new();
    for (backend, presence) in presences {
        match presence {
            Presence::Absolute { points, .. } => specs.push(LineSpec {
                backend: backend.clone(),
                colour: colour_of(backend)?,
                gutter_label: ends_at_final(points),
                points: points.clone(),
                shape_only: false,
            }),
            Presence::ShapeOnly { points, .. } => {
                // Fractions mapped onto the lower part of the box: the
                // curve's shape is preserved, the magnitude is not
                // recoverable — and a normalised peak drawn at the very top
                // would tower over every real measurement. On the log axis
                // the interpolation is geometric, which is what keeps the
                // fraction linear in *position*: the point sits at 45%·f of
                // the band's height exactly. `f ∈ (0, 1]` — a fraction of
                // the series' own positive maximum — so the f = 0
                // floor-marker edge is unreachable.
                let mapped: Vec<(f64, f64)> = match range {
                    Some((y_lo, y_hi)) => points
                        .iter()
                        .map(|(x, f)| (*x, y_lo * (y_hi / y_lo).powf(0.45 * f)))
                        .collect(),
                    None => points.iter().map(|(x, f)| (*x, 0.45 * f)).collect(),
                };
                specs.push(LineSpec {
                    backend: backend.clone(),
                    colour: colour_of(backend)?,
                    gutter_label: ends_at_final(points),
                    points: mapped,
                    shape_only: true,
                });
            }
            Presence::Unsupported(_) | Presence::NotMeasured | Presence::WithheldOnly(_) => {}
        }
    }

    // The end-label gutter: carved out of the plot body, not the canvas, so
    // the labels land between the plot and the frame's right margin — and
    // sized to exactly the names it will hold. A series ending mid-chart
    // labels at its endpoint (with its own fits-or-flips check in the
    // body), so its name neither widens the gutter nor trips the cap.
    let gutter = end_label_gutter(
        specs
            .iter()
            .filter(|s| s.gutter_label)
            .map(|s| s.backend.as_str()),
    )?;
    let area = area.margin(0, 0, 0, gutter);

    let n = x_labels.len();
    let x_range = -0.35f64..(n as f64 - 0.65);
    // Only the y-range expression and the axis's self-description differ
    // between the two axis shapes; the builder chrome is set once and the
    // mesh chrome lives in `line_mesh`, so the branches cannot drift apart.
    let mut builder = ChartBuilder::on(&area);
    builder
        .margin_top(26)
        .margin_bottom(26)
        .x_label_area_size(0)
        .y_label_area_size(74);
    match range {
        Some((y_lo, y_hi)) => {
            let mut chart = builder
                .build_cartesian_2d(x_range, (y_lo..y_hi).log_scale())
                .map_err(render)?;
            // The axis names its own scale: a reader comparing line gaps
            // must know they are ratios, not differences.
            line_mesh(&mut chart, mode, format!("{y_desc} — log scale"))?;
            line_chart_body(root, &mut chart, mode, &specs, x_labels, x_desc)?;
        }
        None => {
            let mut chart = builder
                .build_cartesian_2d(x_range, 0.0f64..1.0f64)
                .map_err(render)?;
            line_mesh(
                &mut chart,
                mode,
                format!("{y_desc} (relative — no representative run)"),
            )?;
            line_chart_body(root, &mut chart, mode, &specs, x_labels, x_desc)?;
        }
    }
    draw_legend(root, mode, &legend, &legend_slots, layout.subtitle_extra)?;
    Ok(())
}

/// The line families' shared mesh chrome, generic over the y-axis for the
/// same reason as `line_chart_body`: a styling tweak must be impossible to
/// apply to only one of the log/linear branches.
fn line_mesh<Y>(
    chart: &mut ChartContext<'_, SVGBackend<'_>, Cartesian2d<RangedCoordf64, Y>>,
    mode: Mode,
    y_desc: String,
) -> Result<(), ChartError>
where
    Y: Ranged<ValueType = f64> + ValueFormatter<f64>,
{
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let theme = mode.theme();
    chart
        .configure_mesh()
        .disable_x_mesh()
        // Hairline gridlines at the labelled ticks only. On the log axis the
        // minor lines would lay ~9 hairlines into every decade; a zero-alpha
        // style is skipped by the backend entirely, so nothing is emitted.
        .light_line_style(theme.grid.mix(0.0))
        .bold_line_style(stroke(theme.grid, 1))
        .axis_style(stroke(theme.baseline, 1))
        .y_desc(y_desc)
        .y_label_style(muted(mode, LABEL_PX))
        .axis_desc_style(muted(mode, LABEL_PX))
        .y_label_formatter(&|v: &f64| fmt_count(*v))
        .x_labels(0)
        .x_label_formatter(&|_: &f64| String::new())
        .draw()
        .map_err(render)?;
    Ok(())
}

/// Everything a line chart draws inside (and around) the plot body, generic
/// over the y-axis so the log and linear branches cannot drift apart.
fn line_chart_body<Y>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &mut ChartContext<'_, SVGBackend<'_>, Cartesian2d<RangedCoordf64, Y>>,
    mode: Mode,
    specs: &[LineSpec],
    x_labels: &[String],
    x_desc: &str,
) -> Result<(), ChartError>
where
    Y: Ranged<ValueType = f64>,
{
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let theme = mode.theme();

    // Lines first, then markers, so every marker's surface ring reads over
    // any line it crosses.
    for spec in specs {
        let width = if spec.shape_only { 1 } else { 2 };
        for run_pts in contiguous_runs(&spec.points) {
            chart
                .draw_series(LineSeries::new(
                    run_pts.iter().copied(),
                    stroke(spec.colour, width),
                ))
                .map_err(render)?;
        }
    }
    for spec in specs {
        let radius = if spec.shape_only { 3 } else { 4 };
        let dot = ShapeStyle {
            color: spec.colour.to_rgba(),
            filled: true,
            stroke_width: 0,
        };
        let ring = ShapeStyle {
            color: theme.surface.to_rgba(),
            filled: true,
            stroke_width: 0,
        };
        // The 2px surface ring keeps a marker legible where it crosses
        // another line. A shape-only series stays second-class: thinner
        // line, smaller dot, no ring. One batched call per series; the
        // iterator keeps each ring immediately under its own dot.
        chart
            .draw_series(spec.points.iter().flat_map(|&(x, y)| {
                let ring_element =
                    (!spec.shape_only).then(|| Circle::new((x, y), radius + 2, ring));
                ring_element
                    .into_iter()
                    .chain(std::iter::once(Circle::new((x, y), radius, dot)))
            }))
            .map_err(render)?;
    }

    // Direct end-labels: the identity relief the legend cannot give (a
    // legend presupposes color-matching, which is exactly what sub-3:1
    // contrast and CVD take away). Every plotted series gets its name once,
    // at its line's end — in the gutter when the line reaches the final
    // category; otherwise at its endpoint, extending right when the canvas
    // has room, flipped to end just left of the dot when it does not, and
    // refused loudly when neither side fits (a leader from a mid-chart
    // endpoint into the gutter would read as the bridged line rule 2
    // forbids).
    let plot = chart.plotting_area().get_pixel_range();
    let (plot_top, plot_bottom) = (plot.1.start, plot.1.end);
    let (plot_left, plot_right) = (plot.0.start, plot.0.end);
    let half = END_LABEL_SPACING / 2;
    /// One placed end label: where its text anchors, which way it runs, and
    /// the x-extent `[x0, x1]` it occupies for collision grouping. For a
    /// gutter label the extent starts at its leader's origin, so the span
    /// the sweep reasons about covers everything the mark actually draws —
    /// x0/x1 are NOT derivable from `anchor_x` alone.
    struct EndLabel {
        backend: String,
        endpoint: (i32, i32),
        anchor_x: i32,
        /// End-anchored — the text extends leftward from `anchor_x`.
        flip: bool,
        /// A gutter label, tied to its line by a leader stub.
        leader: bool,
        x0: i32,
        x1: i32,
        y: i32,
    }
    let mut labels: Vec<EndLabel> = Vec::new();
    for spec in specs {
        let Some((x, y)) = spec.points.last() else {
            continue;
        };
        let (px, py) = chart.plotting_area().map_coordinate(&(*x, *y));
        let w = text_px(&spec.backend);
        let (anchor_x, flip, leader) = if spec.gutter_label {
            // Always fits: the gutter was sized for every name it holds.
            (plot_right.saturating_add(GUTTER_LABEL_INSET), false, true)
        } else if px.saturating_add(END_LABEL_OFFSET).saturating_add(w)
            <= WIDTH as i32 - FRAME_MARGIN
        {
            (px.saturating_add(END_LABEL_OFFSET), false, false)
        } else if px.saturating_sub(END_LABEL_OFFSET).saturating_sub(w) >= plot_left {
            // The flip must clear plot-left, not just the canvas: a label
            // across the y-label band overprints the axis ticks.
            (px.saturating_sub(END_LABEL_OFFSET), true, false)
        } else {
            return Err(ChartError::Render(format!(
                "backend `{}`'s end label fits on neither side of its endpoint — \
                 it cannot be labelled legibly on this canvas",
                spec.backend
            )));
        };
        let (x0, x1) = if leader {
            // The leader is part of the mark: anything overlapping its span
            // deconflicts vertically against this label too. A deliberate
            // over-approximation — the drawn leader is a diagonal, and this
            // models its whole span at the label's final y. Erring wide
            // means MORE vertical separation, never ink overprint; the
            // residual costs are a rare 14px displacement of a near-final
            // mid-chart label, and a displaced stack's diagonal passing
            // near a label the sweep considers separated — both cosmetic
            // corners, accepted over modelling leaders as segment
            // obstacles.
            (px.saturating_add(LEADER_STUB), anchor_x.saturating_add(w))
        } else if flip {
            (anchor_x.saturating_sub(w), anchor_x)
        } else {
            (anchor_x, anchor_x.saturating_add(w))
        };
        labels.push(EndLabel {
            backend: spec.backend.clone(),
            endpoint: (px, py),
            anchor_x,
            flip,
            leader,
            x0,
            x1,
            y: py.clamp(plot_top + half, plot_bottom - half),
        });
    }
    // Deterministic slot allocation, per collision component: two labels
    // can only overprint when their x-extents (nearly) overlap, and
    // displacing a label vertically away from an extent 700px distant
    // detaches it from its leaderless dot — collision is geometry, never
    // column identity (at a dense sweep the column pitch is narrower than
    // a label). Components come from a sweep over the sorted extents; a
    // gap under one character advance still counts as touching, because
    // two labels that close at one y read as one run-on name. Within a
    // component: order by desired y (name-tied), a forward pass pushing
    // collisions down, a backward pass pulling the tail back above the
    // bottom edge — and a loud refusal if the stack needs more height than
    // the plot band has, instead of silently escaping into the chrome.
    let touch_gap = text_px("m");
    labels.sort_by(|a, b| (a.x0, a.y, a.backend.as_str()).cmp(&(b.x0, b.y, b.backend.as_str())));
    let mut components: Vec<Vec<usize>> = Vec::new();
    let mut max_x1 = i32::MIN;
    for (i, label) in labels.iter().enumerate() {
        match components.last_mut() {
            Some(component) if label.x0 <= max_x1.saturating_add(touch_gap) => {
                component.push(i);
            }
            _ => components.push(vec![i]),
        }
        max_x1 = max_x1.max(label.x1);
    }
    for idx in &mut components {
        idx.sort_by(|&a, &b| {
            (labels[a].y, labels[a].backend.as_str())
                .cmp(&(labels[b].y, labels[b].backend.as_str()))
        });
        for k in 1..idx.len() {
            let floor = labels[idx[k - 1]].y.saturating_add(END_LABEL_SPACING);
            if labels[idx[k]].y < floor {
                labels[idx[k]].y = floor;
            }
        }
        if let Some(&last) = idx.last() {
            labels[last].y = labels[last].y.min(plot_bottom - half);
        }
        for k in (0..idx.len().saturating_sub(1)).rev() {
            let ceiling = labels[idx[k + 1]].y.saturating_sub(END_LABEL_SPACING);
            if labels[idx[k]].y > ceiling {
                labels[idx[k]].y = ceiling;
            }
        }
        if let Some(&head) = idx.first()
            && labels[head].y < plot_top + half
        {
            return Err(ChartError::Render(format!(
                "{} end labels stack taller than the plot band — the chart \
                 cannot label every series legibly",
                idx.len()
            )));
        }
    }
    let rightward = (FONT, VALUE_PX)
        .into_font()
        .color(&theme.secondary)
        .pos(Pos::new(HPos::Left, VPos::Center));
    let leftward = (FONT, VALUE_PX)
        .into_font()
        .color(&theme.secondary)
        .pos(Pos::new(HPos::Right, VPos::Center));
    for label in &labels {
        if label.leader {
            // The leader ties the label to its line across the gutter; it
            // never spans a category.
            root.draw(&PathElement::new(
                vec![
                    (label.endpoint.0 + LEADER_STUB, label.endpoint.1),
                    (label.anchor_x - LEADER_END_GAP, label.y),
                ],
                stroke(theme.muted, 1),
            ))
            .map_err(render)?;
        }
        let style = if label.flip { &leftward } else { &rightward };
        root.draw_text(&label.backend, style, (label.anchor_x, label.y))
            .map_err(render)?;
    }

    draw_categories(root, chart, mode, x_labels)?;
    draw_x_desc(root, chart, mode, x_desc)?;
    Ok(())
}

// ── Grouped bar charts (families 3, 4 and 5) ────────────────────────────────

/// One bar in a group.
///
/// `lower_bound` marks a setup-bound consume window: the recorded throughput
/// can only under-state the drain rate (the window is only ever too long), so
/// the bar is drawn at its value but muted, and the caption states the `≥`.
/// That keeps the one mode that *cannot* separate setup (sequenced consume has
/// no readiness barrier to hang a probe on) on the chart as a bounded claim
/// instead of either a false absolute or a silent omission.
#[derive(Debug, Clone, Copy)]
struct Bar {
    value: f64,
    lower_bound: bool,
}

impl Bar {
    fn absolute(value: f64) -> Self {
        Self {
            value,
            lower_bound: false,
        }
    }
}

/// One x-axis group. `bars` is positional against the chart's series list;
/// `None` means "this backend cannot do this at all" and is drawn as an
/// explicit marker, never as a zero-height bar.
#[derive(Debug, Clone)]
struct BarGroup {
    label: String,
    bars: Vec<Option<Bar>>,
    /// `representative: false` — bars are normalised within the group and the
    /// axis numbers do not describe them.
    shape_only: bool,
}

#[allow(clippy::too_many_arguments)]
fn bar_chart(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
    title: &str,
    subtitle: &str,
    y_desc: &str,
    x_desc: &str,
    series: &[(String, RGBColor)],
    groups: &[BarGroup],
    notes: &[String],
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let theme = mode.theme();
    if groups.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "bar-chart",
            slice: title.to_string(),
        });
    }
    let (legend_slots, legend_rows) = legend_layout(series)?;
    let (area, layout) = frame(root, doc, mode, title, subtitle, notes, legend_rows)?;

    // Only representative groups set the scale, so a non-representative
    // magnitude can never be read off the axis. Lower-bound bars do
    // participate: they are drawn at their value, and an axis scaled by an
    // under-statement never over-claims.
    let mut peak = 0.0f64;
    for g in groups.iter() {
        for b in g.bars.iter().flatten() {
            // A subnormal throughput turns 1e9/tp into +inf; an infinite
            // scaling peak maps coordinates to NaN and the render completes
            // as a clean-looking corrupt SVG. Shape-only groups feed their
            // own scaling peak, so they are guarded too.
            if !b.value.is_finite() {
                return Err(ChartError::Render(format!(
                    "group `{}` holds a non-finite bar value",
                    g.label
                )));
            }
        }
    }
    for g in groups.iter().filter(|g| !g.shape_only) {
        for b in g.bars.iter().flatten() {
            if b.value > peak {
                peak = b.value;
            }
        }
    }
    let absolute_axis = peak > 0.0;
    let y_hi = if absolute_axis {
        headroom(peak, 1.14)?
    } else {
        1.0
    };

    let n = groups.len();
    let mut chart = ChartBuilder::on(&area)
        .margin_top(26)
        .margin_bottom(26)
        .x_label_area_size(0)
        .y_label_area_size(74)
        .build_cartesian_2d(-0.5f64..(n as f64 - 0.5), 0f64..y_hi)
        .map_err(render)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        // Single-weight hairline grid at the labeled ticks only; a
        // zero-alpha light style is skipped by the backend entirely.
        .light_line_style(theme.grid.mix(0.0))
        .bold_line_style(stroke(theme.grid, 1))
        .axis_style(stroke(theme.baseline, 1))
        .y_desc(if absolute_axis {
            y_desc.to_string()
        } else {
            format!("{y_desc} (relative — no representative run)")
        })
        .y_label_style(muted(mode, LABEL_PX))
        .axis_desc_style(muted(mode, LABEL_PX))
        .y_label_formatter(&|v: &f64| fmt_count(*v))
        .x_labels(0)
        .x_label_formatter(&|_: &f64| String::new())
        .draw()
        .map_err(render)?;

    // Bars fill 76% of a slot, leaving a visible gutter between groups.
    let slot = 0.76f64;
    let width = slot / series.len().max(1) as f64;

    // The rule-3 marker is a read-out, not furniture: it is the explicit
    // "this backend cannot do this" claim.
    let marker: TextStyle<'_> = (FONT, LABEL_PX)
        .into_font()
        .color(&theme.secondary)
        .pos(Pos::new(HPos::Center, VPos::Bottom));
    // Smaller than the axis labels so a value fits above a narrow bar. In a
    // static chart the value label is the only magnitude read-out, so it
    // wears read-out ink, not furniture ink.
    let value_label: TextStyle<'_> = (FONT, VALUE_PX)
        .into_font()
        .color(&theme.secondary)
        .pos(Pos::new(HPos::Center, VPos::Bottom));

    let shape_peak_across_groups = groups
        .iter()
        .filter(|g| g.shape_only)
        .flat_map(|g| g.bars.iter().flatten())
        .fold(0.0f64, |a, b| a.max(b.value));
    for (gi, group) in groups.iter().enumerate() {
        // A shape-only group is scaled so relative heights survive while the
        // magnitudes do not. The scaling peak depends on the layout: in a
        // multi-series chart the shape lives *within* a group, but in a
        // single-series chart each group holds one bar — its own peak — and
        // per-group scaling would render every bar at full height, a flat
        // line regardless of the data. There the shape lives across groups,
        // so the peak does too.
        let local_peak = if series.len() == 1 {
            shape_peak_across_groups
        } else {
            group
                .bars
                .iter()
                .flatten()
                .fold(0.0f64, |a, b| a.max(b.value))
        };
        for (si, (_, colour)) in series.iter().enumerate() {
            // Slots tile edge to edge in data space; the pixel-space
            // inter-bar gap and thickness cap live in `draw_bar`.
            let left = gi as f64 - slot / 2.0 + width * si as f64;
            let right = left + width;
            let centre = (left + right) / 2.0;
            match group.bars.get(si).copied().flatten() {
                Some(bar) => {
                    // 0.45, not ~1.0: a shape-only bar drawn to the top of
                    // an axis scaled by the representative peak would read
                    // as the tallest measurement in the chart. Mid-height
                    // keeps the relative shape while staying visually
                    // subordinate to every published absolute.
                    let height = if group.shape_only {
                        if local_peak > 0.0 {
                            y_hi * 0.45 * (bar.value / local_peak)
                        } else {
                            0.0
                        }
                    } else {
                        bar.value
                    };
                    // Muted fill for anything the axis numbers do not fully
                    // describe: a normalised group, or a lower-bound bar
                    // whose caption carries the ≥. Pre-blended to an opaque
                    // hex — the rounded cap below is a same-color shape
                    // union, and a translucent fill would compound to
                    // visible seams where its shapes overlap.
                    let fill = if group.shape_only || bar.lower_bound {
                        theme.blend_muted(*colour)
                    } else {
                        *colour
                    };
                    draw_bar(root, &chart, (left, right), height, fill)?;
                    // Every published bar carries its value: one backend a
                    // decade faster than the rest scales a shared linear
                    // axis so the others are a few pixels tall, and a bar
                    // that cannot be read off the axis must still be
                    // readable off its label. Shape-only bars carry none —
                    // their height is normalised and a number on it would
                    // be read as the magnitude the rule withholds.
                    if !group.shape_only {
                        let label = if bar.lower_bound {
                            format!("≥{}", fmt_count(bar.value))
                        } else {
                            fmt_count(bar.value)
                        };
                        let (px, py) = chart.plotting_area().map_coordinate(&(centre, height));
                        root.draw_text(&label, &value_label, (px, py - 3))
                            .map_err(render)?;
                    }
                }
                None => {
                    // Rule 3: an explicit marker. Never a zero, and never a
                    // silently absent bar — omission reads as "we forgot".
                    let (px, py) = chart.plotting_area().map_coordinate(&(centre, 0.0));
                    root.draw_text("n/s", &marker, (px, py - 6))
                        .map_err(render)?;
                }
            }
        }
    }

    let labels: Vec<String> = groups.iter().map(|g| g.label.clone()).collect();
    draw_categories(root, &chart, mode, &labels)?;
    draw_x_desc(root, &chart, mode, x_desc)?;
    draw_legend(root, mode, series, &legend_slots, layout.subtitle_extra)?;
    Ok(())
}

/// The maximum bar thickness. A bar is a mark, not a block: past ~24px the
/// leftover slot width becomes air, and a 180px saturated slab reads loud
/// without saying anything more.
const BAR_MAX_PX: i32 = 24;
/// The surface-colored gap between adjacent bars in a group.
const BAR_GAP_PX: i32 = 2;

/// Draw one bar in pixel space: its slot is computed in data coordinates
/// (which is where the grouping lives), then mapped through the plot's
/// coordinate translation — the same mechanism the value labels and rule-3
/// markers already use — so the thickness cap, the inter-bar gap and the
/// rounded-cap radius are all real pixels, not data units that stretch with
/// the axis.
///
/// The rounded data-end is a same-color shape union: a body rect up to
/// `top + r`, two `r`-radius circles at the cap corners, and a cap band
/// between them, overlapping by a pixel to kill anti-aliasing seams. The
/// radius clamps continuously (`min(4, height/2, width/2)`) so a short or
/// narrow bar degrades to square rather than switching styles mid-chart.
/// The baseline end stays square.
fn draw_bar<CT>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &ChartContext<'_, SVGBackend<'_>, CT>,
    (left, right): (f64, f64),
    height: f64,
    fill: RGBColor,
) -> Result<(), ChartError>
where
    CT: CoordTranslate<From = (f64, f64)>,
{
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let area = chart.plotting_area();
    let (x0, base_y) = area.map_coordinate(&(left, 0.0));
    let (x1, top_y) = area.map_coordinate(&(right, height));
    // The slot in pixels, gap first, then the thickness cap; the bar is
    // centred in what remains of its slot.
    let slot_w = x1.saturating_sub(x0);
    let w = slot_w.saturating_sub(BAR_GAP_PX).clamp(2, BAR_MAX_PX);
    let cx = x0.saturating_add(slot_w / 2);
    let bx0 = cx.saturating_sub(w / 2);
    let bx1 = bx0.saturating_add(w);
    let (top, bottom) = (top_y.min(base_y), base_y);
    if bottom <= top {
        // A zero-height bar draws nothing; the value label (or the shape-only
        // caption) is the witness, exactly as before.
        return Ok(());
    }
    let style = ShapeStyle {
        color: fill.to_rgba(),
        filled: true,
        stroke_width: 0,
    };
    let bar_h = bottom.saturating_sub(top);
    let r = 4.min(bar_h / 2).min(w / 2).max(0);
    if r == 0 {
        root.draw(&Rectangle::new([(bx0, top), (bx1, bottom)], style))
            .map_err(render)?;
    } else {
        // Body below the cap line…
        root.draw(&Rectangle::new(
            [(bx0, top.saturating_add(r)), (bx1, bottom)],
            style,
        ))
        .map_err(render)?;
        // …the cap band between the corner circles (1px overlap downward)…
        root.draw(&Rectangle::new(
            [
                (bx0.saturating_add(r), top),
                (
                    bx1.saturating_sub(r),
                    top.saturating_add(r).saturating_add(1),
                ),
            ],
            style,
        ))
        .map_err(render)?;
        // …and the two rounded corners.
        for ccx in [bx0.saturating_add(r), bx1.saturating_sub(r)] {
            root.draw(&Circle::new((ccx, top.saturating_add(r)), r, style))
                .map_err(render)?;
        }
    }
    Ok(())
}

// ── Family 3: parallel vs sequenced ─────────────────────────────────────────

/// The consume flows, as a lookup against the schema's closed set. Membership
/// is decided here; which *bar* a row lands in is decided by its `mode` field,
/// which exists so a flow name never has to be parsed for that.
/// The plain competing/ordered/batch consume flows — family 3's slice.
///
/// `consumer_group` is deliberately absent: its rows share `mode:
/// "parallel"`, but a coordinated group is a different subscription topology,
/// and its throughput must not silently stand in for plain consume — in a
/// zero-handler run it often wins the best-of fold, so the "parallel" bar
/// would quietly be a consumer-group number. Which *bar* a row lands in is
/// still decided by its `mode` field; this list only bounds the slice, the
/// same way it bounds the `unsupported[]` lookup.
const CONSUME_FLOWS: &[&str] = &["consume_parallel", "consume_fifo", "consume_batch"];

/// `(mode, the flow whose unsupported[] entry explains a missing bar, label)`.
const MODES: &[(&str, &str, &str)] = &[
    ("parallel", "consume_parallel", "parallel"),
    ("fifo", "consume_fifo", "sequenced (fifo)"),
    ("batch", "consume_batch", "batch"),
];

fn render_parallel_vs_sequenced(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    render_mode: Mode,
) -> Result<(), ChartError> {
    let series: Vec<(String, RGBColor)> = MODES
        .iter()
        .enumerate()
        .map(|(i, (_, _, label))| ((*label).to_string(), palette_colour(render_mode, i)))
        .collect();

    let mut groups = Vec::new();
    let mut notes = Vec::new();
    let mut batch_knobs: BTreeSet<(String, u64, u64)> = BTreeSet::new();
    // Aggregated caption lines, keyed by (mode index, text) so identical
    // explanations across backends collapse to one line naming them all —
    // six single-backend repeats of "run_batch is Kafka-only" would crowd
    // out the caption block the notes exist to fill.
    // `text → mode → backends`, rendered through `mode_notes` so identical
    // explanations collapse across backends *and* modes.
    let mut qualifiers: BTreeMap<String, BTreeMap<usize, Vec<String>>> = BTreeMap::new();

    for run in &doc.runs {
        // Per mode: an absolute bar from the framework rows if any exist,
        // else a lower-bound bar from the setup-bound rows. Sequenced consume
        // holds no readiness barrier, so its zero/fast rows are *always*
        // setup-bound — without the lower-bound arm the ordering-cost chart
        // would lose the sequenced bar it exists to show. Sleeping-handler
        // rows (`handler_bound` / `handler_amortised`) never reach either arm:
        // their number is the simulated sleep, not shove.
        // One worker count per backend, shared by every mode it measured:
        // the modes pin different counts (fifo to the shard count, batch to
        // its configured set), and comparing a 1-worker parallel bar against
        // an 8-worker fifo bar inverted the ordering-cost story on four of
        // five committed backends. The least-parallel count every measured
        // mode shares is the honest common ground; when no shared count
        // exists, each mode falls back to its own minimum and the caption
        // names the counts.
        let mode_counts: Vec<BTreeSet<u32>> = MODES
            .iter()
            .map(|(mode, _, _)| {
                run.results
                    .iter()
                    .filter(|r| {
                        r.mode == *mode
                            && CONSUME_FLOWS.contains(&canonical_flow(&r.flow))
                            && r.payload_bytes == HEADLINE_PAYLOAD
                            && (r.is_framework() || r.is_setup_bound())
                            && r.window_ok()
                    })
                    .map(|r| r.consumers)
                    .collect()
            })
            .collect();
        let common_count: Option<u32> = mode_counts
            .iter()
            .filter(|c| !c.is_empty())
            .cloned()
            .reduce(|a, b| a.intersection(&b).copied().collect())
            .and_then(|shared| shared.into_iter().next());

        let bars: Vec<Option<Bar>> = MODES
            .iter()
            .enumerate()
            .map(|(mi, (mode, flow, _))| {
                let in_mode = |r: &&ScenarioResult| {
                    r.mode == *mode
                        && CONSUME_FLOWS.contains(&canonical_flow(&r.flow))
                        && r.payload_bytes == HEADLINE_PAYLOAD
                };
                // Either arm publishes a number, so either arm is held to
                // the window floor: a lower bound read off a 0.18 s window is
                // noise with a `≥` in front of it, and drawn at its value it
                // scales the axis every real bar is read against.
                let arm = |lower_bound: bool| -> Option<(Bar, &ScenarioResult)> {
                    let is_arm = |r: &&ScenarioResult| {
                        r.window_ok()
                            && if lower_bound {
                                r.is_setup_bound()
                            } else {
                                r.is_framework()
                            }
                    };
                    let at = match common_count {
                        Some(c) if mode_counts[mi].contains(&c) => c,
                        _ => run
                            .results
                            .iter()
                            .filter(in_mode)
                            .filter(is_arm)
                            .map(|r| r.consumers)
                            .min()?,
                    };
                    best_by_throughput(
                        run.results
                            .iter()
                            .filter(in_mode)
                            .filter(is_arm)
                            .filter(|r| r.consumers == at),
                    )
                    .map(|r| {
                        (
                            Bar {
                                value: r.throughput_msg_per_sec,
                                lower_bound,
                            },
                            r,
                        )
                    })
                };
                let won = arm(false).or_else(|| arm(true));
                if let Some((bar, row)) = won {
                    if row.consumers != BASE_CONSUMERS {
                        // Only claim a shared count when one exists; with
                        // disjoint counts each mode is at its own minimum
                        // and the bars are not a like-for-like comparison.
                        let text = match common_count {
                            Some(_) => format!(
                                "measured at {} workers (the least-parallel count the \
                                 measured modes share)",
                                row.consumers
                            ),
                            None => format!(
                                "measured at {} workers (no worker count is shared by the \
                                 measured modes, so the bars are not directly comparable)",
                                row.consumers
                            ),
                        };
                        qualifiers
                            .entry(text)
                            .or_default()
                            .entry(mi)
                            .or_default()
                            .push(run.backend.clone());
                    }
                    // Disclosed whether or not the run is representative: a
                    // shape-only fifo bar's shape still comes from a window
                    // that can only under-state the rate.
                    if bar.lower_bound {
                        // A shape-only bar's height is normalised, so "at
                        // least the bar shown" would hand the reader an
                        // absolute bound off a height that is not one.
                        let text = if run.representative {
                            "lower bound (muted bar) — this window cannot separate setup \
                             from drain, so the true rate is at least the bar shown"
                        } else {
                            "lower bound from a window that cannot separate setup from drain; \
                             the bar is shape only, so its height is not the bound"
                        };
                        qualifiers
                            .entry(text.to_string())
                            .or_default()
                            .entry(mi)
                            .or_default()
                            .push(run.backend.clone());
                    }
                    // A published batch bar states its knobs: since v3 they
                    // are what distinguishes a 50-message-batch row from a
                    // 500-message-batch one, so a bar without them would be a
                    // number with no stated batch size.
                    if let (Some(size), Some(age)) = (row.max_batch_size, row.max_batch_age_ms) {
                        batch_knobs.insert((run.backend.clone(), size, age));
                    }
                } else {
                    // Explain the n/s marker: rows that exist but whose every
                    // window is under the floor; else the declared capability
                    // hole when the document names one; else a
                    // supported-but-absent gap — never "not supported" for a
                    // mere gap. Verbatim reason, no prefix — see notes_for.
                    let measured_short = run
                        .results
                        .iter()
                        .filter(in_mode)
                        .any(|r| r.is_framework() || r.is_setup_bound());
                    let measured_sleeping = run.results.iter().any(|r| in_mode(&r));
                    // A failed cell in this mode is an absence with a
                    // recorded cause — not a gap, and not unsupported.
                    let failed = run.failures.iter().any(|f| {
                        f.payload_bytes == HEADLINE_PAYLOAD
                            && CONSUME_FLOWS.contains(&canonical_flow(&f.flow))
                            && FLOW_MODES
                                .iter()
                                .any(|(fl, m)| *fl == f.flow && *m == *mode)
                    });
                    let text = if measured_short {
                        short_window_text()
                    } else if measured_sleeping {
                        sleeping_handler_text().to_string()
                    } else if failed {
                        failed_text().to_string()
                    } else {
                        unsupported_reason(run, flow).unwrap_or_else(|| gap_text().to_string())
                    };
                    qualifiers
                        .entry(text)
                        .or_default()
                        .entry(mi)
                        .or_default()
                        .push(run.backend.clone());
                }
                won.map(|(bar, _)| bar)
            })
            .collect();

        // The shape-only caveat only means something when a bar exists to
        // misread; an all-n/s backend gets its per-mode explanations instead.
        if !run.representative && bars.iter().any(Option::is_some) {
            notes.push(shape_only_note(run));
        }
        // Every backend in the document gets a group — an all-n/s group is
        // still on the axis, which is the point: absence must be legible as
        // "explained", never as "forgot".
        groups.push(BarGroup {
            label: run.backend.clone(),
            bars,
            shape_only: !run.representative,
        });
    }

    notes.extend(mode_notes(&qualifiers));

    // One note when every backend's batch bar ran the same knobs (the usual
    // case), per-backend notes when they differ — never silence, because the
    // knobs are what the bar's number means.
    let configs: BTreeSet<(u64, u64)> = batch_knobs.iter().map(|(_, s, a)| (*s, *a)).collect();
    match configs.len() {
        0 => {}
        1 => {
            let (size, age) = configs.first().copied().unwrap_or((0, 0));
            notes.push(format!(
                "batch bars: up to {size} messages or {age} ms per batch"
            ));
        }
        _ => {
            for (backend, size, age) in &batch_knobs {
                notes.push(format!(
                    "{backend} / batch: up to {size} messages or {age} ms per batch"
                ));
            }
        }
    }

    // A mode outside MODES would otherwise fall out of every filter above
    // and vanish without a bar or a word — the same silent omission an
    // unknown handler_cost is refused for. New modes are additive (no schema
    // bump), so they are named rather than refused.
    let mut uncharted: BTreeSet<&str> = BTreeSet::new();
    for r in doc.runs.iter().flat_map(|run| run.results.iter()) {
        if r.payload_bytes != HEADLINE_PAYLOAD {
            continue;
        }
        let known_mode = MODES.iter().any(|(mode, _, _)| r.mode == *mode);
        if CONSUME_FLOWS.contains(&canonical_flow(&r.flow)) && !known_mode {
            uncharted.insert(r.mode.as_str());
        }
        // A consume-mode row of a flow outside both this chart's slice and
        // the schema's known set is a future additive flow: name it rather
        // than let it vanish. Known flows outside CONSUME_FLOWS (the publish
        // pair, consumer_group, …) are scoped to other charts on purpose.
        if known_mode && !KNOWN_FLOWS.contains(&r.flow.as_str()) {
            uncharted.insert(r.flow.as_str());
        }
    }
    if !uncharted.is_empty() {
        notes.push(format!(
            "measured but not charted here (unknown to this chart): {}",
            uncharted.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }

    notes.extend(alias_notes(doc, |r| {
        r.payload_bytes == HEADLINE_PAYLOAD && CONSUME_FLOWS.contains(&canonical_flow(&r.flow))
    }));
    notes.extend(failure_notes(&doc.runs, |f| {
        f.payload_bytes == HEADLINE_PAYLOAD && CONSUME_FLOWS.contains(&canonical_flow(&f.flow))
    }));

    bar_chart(
        doc,
        root,
        render_mode,
        Family::ParallelVsSequenced.title(),
        &format!(
            "{} payload — least-parallel measurement per mode — what ordering \
             costs — higher is better",
            fmt_payload(HEADLINE_PAYLOAD)
        ),
        "messages / second",
        "backend",
        &series,
        &groups,
        &notes,
    )
}

// ── Family 4: dispatch latency percentiles ──────────────────────────────────

/// Publishes an explicit refusal panel, not bars — for now.
///
/// The v4 rows' dispatch percentiles measure publish-timestamp →
/// handler-entry while the driver publishes the whole corpus concurrently
/// with the drain. Under a saturated backlog that quantity is queue
/// residency: the median is ≈ half the drain window (four of five committed
/// backends show exactly that — rabbitmq: 104.8 s window, 53,515 ms p50),
/// a function of corpus size and the publish/drain rate ratio rather than
/// of anything shove does per message. Kafka's publish path overlaps the
/// drain differently, so its 18 ms is not even the same quantity. One
/// "lower is better" axis over both would publish a false cross-backend
/// comparison — the same class of defect the `handler_cost` markers exist
/// to refuse. The bars return when the harness records dispatch latency
/// under matched load; the panel keeps the file present, provenanced and
/// byte-deterministic so the staleness contract is unaffected.
fn render_dispatch_latency(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let theme = mode.theme();
    let notes = vec![
        "the v4 dispatch percentiles time publish → handler entry while the whole corpus \
         is published concurrently with the drain: under a saturated backlog that is queue \
         residency (median ≈ half the drain window), which scales with corpus size and the \
         publish/drain rate ratio — not a per-message dispatch cost, and not comparable \
         across backends"
            .to_string(),
        "bars will be published once the harness measures dispatch latency under matched \
         load"
            .to_string(),
    ];
    // The shared reason withholds every cell, but "withheld" and "never
    // measured" are different facts about a backend, and a panel naming
    // none of them cannot show which is which.
    let (mut recorded, mut unmeasured): (Vec<&str>, Vec<&str>) = (Vec::new(), Vec::new());
    for run in &doc.runs {
        if run.results.is_empty() {
            unmeasured.push(run.backend.as_str());
        } else {
            recorded.push(run.backend.as_str());
        }
    }
    let (_, layout) = frame(
        root,
        doc,
        mode,
        Family::DispatchLatency.title(),
        "v4's dispatch percentiles are not publishable as latency — see caption",
        &notes,
        0,
    )?;
    // The panel body: the refusal sentence plus the per-backend accounting,
    // centered as a block inside the band the frame guard proves free of
    // both the chrome above (PLOT_TOP shifted by any wrapped subtitle) and
    // any permitted footer top below. The guard bounds the band, not the
    // block — a block taller than the band is refused below, exactly as it
    // was when these lines travelled through the footer and the caption
    // guard counted them. Every line wraps at NOTE_WRAP (≤ ~810px wide), so
    // a centered line spans at most [75, 885] and stays inside the canvas
    // on both edges.
    let mut body: Vec<(String, i32)> = vec![(
        "no publishable dispatch-latency measurement in this document".to_string(),
        LABEL_PX,
    )];
    if !recorded.is_empty() {
        for line in wrap(
            &format!(
                "percentiles recorded and withheld for: {}",
                recorded.join(", ")
            ),
            NOTE_WRAP,
        ) {
            body.push((line, FOOT_PX));
        }
    }
    if !unmeasured.is_empty() {
        for line in wrap(
            &format!(
                "no measured rows in this document (nothing to withhold): {}",
                unmeasured.join(", ")
            ),
            NOTE_WRAP,
        ) {
            body.push((line, FOOT_PX));
        }
    }
    // One spelling of the block's vertical rhythm: the guard measures the
    // sum of exactly the advances the drawing loop takes (the sentence gets
    // a little more air below it than the list lines), so retuning the air
    // cannot desynchronise the guard from the ink.
    let advance = |i: usize| -> i32 { if i == 0 { SUBTITLE_LINE + 6 } else { LINE } };
    let block_h = (0..body.len()).fold(0i32, |h, i| h.saturating_add(advance(i)));
    if block_h > layout.band_bottom.saturating_sub(layout.band_top) {
        return Err(ChartError::Render(format!(
            "the dispatch-latency accounting ({} lines) does not fit the refusal \
             panel's band",
            body.len()
        )));
    }
    // Non-negative by the guard above — no clamp, so an overflow can never
    // silently pin to band_top again.
    let mut y =
        layout.band_top + (layout.band_bottom - layout.band_top).saturating_sub(block_h) / 2;
    for (i, (line, px)) in body.iter().enumerate() {
        let style = centred(theme.secondary, *px);
        root.draw_text(line, &style, (WIDTH as i32 / 2, y))
            .map_err(render)?;
        y = y.saturating_add(advance(i));
    }
    Ok(())
}

// ── Family 5: framework overhead, ns/msg per flow ───────────────────────────

fn render_framework_overhead(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    mode: Mode,
) -> Result<(), ChartError> {
    let run = doc
        .runs
        .iter()
        .find(|r| r.backend == IN_PROCESS_BACKEND)
        .ok_or(ChartError::MissingInProcessRun)?;

    let series: Vec<(String, RGBColor)> = vec![(
        format!("{IN_PROCESS_BACKEND} — nanoseconds per message"),
        palette_colour(mode, 0),
    )];

    // Every flow the schema knows about gets a column: the ones measured carry
    // a bar, the ones declared unsupported carry the `n/s` marker. A flow that
    // is neither is not invented.
    let mut groups = Vec::new();
    let mut notes = Vec::new();
    let mut unmeasured: Vec<&str> = Vec::new();
    let mut setup_bound: Vec<&str> = Vec::new();
    let mut short_window: Vec<String> = Vec::new();
    let mut sleeping: Vec<&str> = Vec::new();
    let mut failed: Vec<&str> = Vec::new();
    let mut aliased: Vec<String> = Vec::new();
    let mut pinned_workers: Vec<String> = Vec::new();
    for flow in KNOWN_FLOWS {
        // Each flow is charted at its own least-parallel measurement, the
        // same policy as family 3: fifo pins its workers to the shard count,
        // so requiring the global BASE_CONSUMERS here mislabelled a flow
        // measured in every run as "not measured". Non-1 counts are named.
        let in_flow = |r: &&ScenarioResult| r.flow == *flow && r.payload_bytes == OVERHEAD_PAYLOAD;
        // "What shove itself costs" is only what the marker certifies as
        // shove: a drain-windowed framework row, or a publish row (no handler
        // exists to contaminate it). A setup-bound row would *over-state* the
        // cost — ns/msg is the reciprocal of an under-stated rate — so those
        // flows are named in the caption instead of charted too high. A
        // publish row is additionally held to the window floor the harness
        // applies to framework rows: the in-process publisher clears 5,000
        // messages in milliseconds, and the reciprocal of that is not a cost.
        let is_publish = |r: &&ScenarioResult| r.handler_cost == COST_NO_HANDLER;
        let publishable =
            |r: &&ScenarioResult| r.is_framework() || (is_publish(r) && r.window_ok());
        let min_workers = run
            .results
            .iter()
            .filter(in_flow)
            .filter(publishable)
            .map(|r| r.consumers)
            .min();
        // Best observed cost per message at that worker count — the highest
        // throughput, since ns/msg is its reciprocal: the floor is the
        // framework's own cost, anything above it is scheduling noise.
        let measured = min_workers.and_then(|min| {
            best_by_throughput(
                run.results
                    .iter()
                    .filter(in_flow)
                    .filter(publishable)
                    .filter(|r| r.consumers == min),
            )
        });

        match measured {
            Some(row) => {
                if row.consumers != BASE_CONSUMERS {
                    pinned_workers.push(format!("{flow} at {} workers", row.consumers));
                }
                // A published batch number states its knobs (non-ignorable
                // since v3): without them the bar has no stated batch size.
                if let (Some(size), Some(age)) = (row.max_batch_size, row.max_batch_age_ms) {
                    notes.push(format!(
                        "{flow}: up to {size} messages or {age} ms per batch"
                    ));
                }
                groups.push(BarGroup {
                    label: (*flow).to_string(),
                    bars: vec![Some(Bar::absolute(1e9 / row.throughput_msg_per_sec))],
                    shape_only: !run.representative,
                });
            }
            None if run.results.iter().filter(in_flow).any(|r| is_publish(&r)) => {
                // Measured, honestly marked, and still not a rate: name the
                // longest window the flow managed so the reader sees how far
                // under the floor it fell.
                let longest = run
                    .results
                    .iter()
                    .filter(in_flow)
                    .filter(is_publish)
                    .map(|r| r.window_secs())
                    .fold(0.0f64, f64::max);
                short_window.push(format!("{flow} ({longest:.3} s)"));
            }
            None if run
                .results
                .iter()
                .filter(in_flow)
                .any(|r| r.is_setup_bound()) =>
            {
                // Setup-bound *and* every window under the floor is two
                // reasons; the shorter one is the one a reader can act on.
                let windows: Vec<f64> = run
                    .results
                    .iter()
                    .filter(in_flow)
                    .filter(|r| r.is_setup_bound())
                    .map(|r| r.window_secs())
                    .collect();
                let longest = windows.iter().copied().fold(0.0f64, f64::max);
                if windows.iter().all(|w| *w < MIN_PUBLISHABLE_WINDOW_SECS) {
                    short_window.push(format!("{flow} ({longest:.3} s)"));
                } else {
                    setup_bound.push(flow);
                }
            }
            None if run.results.iter().any(|r| in_flow(&r)) => sleeping.push(*flow),
            None if run
                .failures
                .iter()
                .any(|f| f.flow == *flow && f.payload_bytes == OVERHEAD_PAYLOAD) =>
            {
                failed.push(*flow);
            }
            None => match unsupported_reason(run, flow) {
                Some(reason) => {
                    groups.push(BarGroup {
                        label: (*flow).to_string(),
                        bars: vec![None],
                        shape_only: false,
                    });
                    notes.push(format!("{flow}: {reason}"));
                }
                None => {
                    // A flow that is another flow's name on one backend is
                    // not a gap when the run measured the flow it spells.
                    let spells = ALIASED_FLOWS
                        .iter()
                        .find(|(alias, _)| alias == flow)
                        .map(|(_, target)| *target)
                        .filter(|target| run.results.iter().any(|r| r.flow == *target));
                    match spells {
                        Some(target) => aliased.push(format!(
                            "{flow}: not a separate measurement — the SQS spelling of {target} \
                             (the same run primitive); see that column"
                        )),
                        // Neither measured nor declared. Inventing a column
                        // would imply a capability hole that does not exist,
                        // but dropping it in silence is the omission the
                        // marker rule exists to prevent — so it is named in
                        // the caption instead.
                        None => unmeasured.push(*flow),
                    }
                }
            },
        }
    }

    if !short_window.is_empty() {
        notes.push(format!(
            "window under {MIN_PUBLISHABLE_WINDOW_SECS} s — too short for the throughput to be \
             a rate, so no cost is published: {}",
            short_window.join(", ")
        ));
    }

    if !setup_bound.is_empty() {
        notes.push(format!(
            "setup-bound in this run (window includes coordination cost; \
             no framework number published): {}",
            setup_bound.join(", ")
        ));
    }
    if !sleeping.is_empty() {
        notes.push(format!(
            "{}: {}",
            sleeping_handler_text(),
            sleeping.join(", ")
        ));
    }
    if !failed.is_empty() {
        notes.push(format!("{}: {}", failed_text(), failed.join(", ")));
    }
    if !unmeasured.is_empty() {
        // The one slice-scoped gap wording, with the flows appended.
        notes.push(format!("{}: {}", gap_text(), unmeasured.join(", ")));
    }
    if !pinned_workers.is_empty() {
        notes.push(format!(
            "measured at a pinned worker count: {}",
            pinned_workers.join(", ")
        ));
    }
    notes.extend(aliased);
    // A flow outside KNOWN_FLOWS would fall out of the column loop above and
    // vanish without a bar or a word — name it instead, the same additive
    // tolerance family 3 applies to unknown modes.
    let unknown_flows: BTreeSet<&str> = run
        .results
        .iter()
        .filter(|r| r.payload_bytes == OVERHEAD_PAYLOAD && !KNOWN_FLOWS.contains(&r.flow.as_str()))
        .map(|r| r.flow.as_str())
        .collect();
    if !unknown_flows.is_empty() {
        notes.push(format!(
            "measured but not charted here (flow unknown to this chart): {}",
            unknown_flows.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    // A declaration for a flow this chart has no column for would otherwise
    // vanish with the column: name it, reason verbatim.
    for u in run
        .unsupported
        .iter()
        .filter(|u| !KNOWN_FLOWS.contains(&u.flow.as_str()))
    {
        notes.push(format!(
            "{}: declared unsupported (flow unknown to this chart) — {}",
            u.flow, u.reason
        ));
    }

    // Any worker count: the bars are taken at each flow's least-parallel
    // measurement, so pinning this to one count would publish a failed cell
    // as a benign gap.
    notes.extend(failure_notes(std::slice::from_ref(run), |f| {
        KNOWN_FLOWS.contains(&f.flow.as_str()) && f.payload_bytes == OVERHEAD_PAYLOAD
    }));

    if groups.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "framework-overhead",
            slice: format!(
                "{IN_PROCESS_BACKEND} @ {} (least-parallel per flow)",
                fmt_payload(OVERHEAD_PAYLOAD)
            ),
        });
    }
    if !run.representative {
        notes.push(format!(
            "{IN_PROCESS_BACKEND}: shape only — not representative; \
             magnitudes are not published"
        ));
    }

    bar_chart(
        doc,
        root,
        mode,
        Family::FrameworkOverhead.title(),
        &format!(
            "in-process — {} payload — least-parallel per flow — what shove \
             itself costs, broker removed — lower is better",
            fmt_payload(OVERHEAD_PAYLOAD)
        ),
        "nanoseconds / message",
        "flow",
        &series,
        &groups,
        &notes,
    )
}

// ── Entry points ────────────────────────────────────────────────────────────

fn render_into(
    doc: &Document,
    family: Family,
    mode: Mode,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    match family {
        Family::ThroughputVsConsumers => render_throughput_vs_consumers(doc, root, mode),
        Family::ThroughputVsPayload => render_throughput_vs_payload(doc, root, mode),
        Family::ParallelVsSequenced => render_parallel_vs_sequenced(doc, root, mode),
        Family::DispatchLatency => render_dispatch_latency(doc, root, mode),
        Family::FrameworkOverhead => render_framework_overhead(doc, root, mode),
    }
}

/// Render one family, in one mode, to an SVG string. This is what the tests
/// assert against — the file-writing path below is the same code with a
/// different sink.
pub fn render_to_string(doc: &Document, family: Family, mode: Mode) -> Result<String, ChartError> {
    validate(doc)?;
    render_validated(doc, family, mode)
}

/// The render body behind [`render_to_string`], for callers that have already
/// validated — [`generate`] validates once for all ten variants rather than
/// re-walking every row per chart.
fn render_validated(doc: &Document, family: Family, mode: Mode) -> Result<String, ChartError> {
    let mut buf = String::new();
    {
        let root = SVGBackend::with_string(&mut buf, (WIDTH, HEIGHT)).into_drawing_area();
        render_into(doc, family, mode, &root)?;
        root.present()
            .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))?;
    }
    Ok(buf)
}

/// Render every family into `out_dir`, which must already exist.
///
/// Every family renders to memory before any file is written: render-stage
/// refusals (a missing in-process run, unusable percentiles, an oversized
/// caption block) can fire after `validate` passes, and a failure midway
/// through the loop would otherwise leave the directory a mix of regenerated
/// and stale charts — worse than none.
pub fn generate(doc: &Document, out_dir: &Path) -> Result<Vec<String>, ChartError> {
    validate(doc)?;
    let mut rendered = Vec::new();
    for family in Family::ALL {
        for mode in Mode::ALL {
            rendered.push((family.filename(mode), render_validated(doc, family, mode)?));
        }
    }
    let mut written = Vec::new();
    for (filename, svg) in rendered {
        std::fs::write(out_dir.join(&filename), svg.as_bytes())?;
        written.push(filename);
    }
    Ok(written)
}
