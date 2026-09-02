//! Chart generation from the versioned benchmark results document.
//!
//! Reads `benches/results/bench-results.json` (the `bench-schema` contract at
//! v4) and renders five SVG chart families into `docs/public/bench/`.
//!
//! This module is the library half of `examples/chartgen.rs`; the example is a
//! thin CLI over it. It is pulled in with `#[path]` from two targets — the
//! example, and `tests/chartgen.rs`, which is what actually runs the
//! `#[cfg(test)]` module below. Cargo defaults `[[example]]` targets to
//! `test = false`, so tests living only in the example would never execute.
//!
//! ## Output must be byte-deterministic
//!
//! The chart-staleness CI leg regenerates these SVGs and diffs them against the
//! committed ones, so identical input must produce identical bytes on any host.
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
//! ## Charts are theme-neutral by construction
//!
//! One file per family, legible on both a light and a dark page: the background
//! is never painted, series are mid-tone, and no fill is white or near-black.
//! The README is the binding constraint — crates.io renders it on a light
//! background with no theme-switching mechanism, so `-dark` siblings would be
//! files the README could never use.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

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

/// The closed flow set from the schema contract.
///
/// Used only to decide whether a backend with no results has *declared* every
/// flow unsupported (legitimate) or has silently measured nothing (a failed
/// run). It is deliberately **not** used to reject unknown flows: the contract
/// bumps `schema_version` only on removal or semantic change, so a future
/// additive flow must keep rendering rather than hard-fail here.
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
    "autoscaler",
];

/// The backend key whose numbers measure shove itself with the broker removed.
pub const IN_PROCESS_BACKEND: &str = "inmemory";

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
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub deployment: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Unsupported {
    pub flow: String,
    pub reason: String,
}

/// The slice coordinates of a failed cell — just enough to intersect a chart
/// family's slice. The diagnostic `error` string stays in the document; the
/// captions only count.
#[derive(Debug, Clone, Deserialize)]
pub struct FailedRow {
    pub flow: String,
    #[serde(default)]
    pub mode: String,
    pub payload_bytes: u64,
    pub consumers: u32,
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
    /// `Option` rather than a zero default: a percentile that is *absent* must
    /// be a loud refusal in the latency family, not a spectacular 0 ms bar on
    /// an absolute axis.
    #[serde(default)]
    pub dispatch_p50_ms: Option<f64>,
    #[serde(default)]
    pub dispatch_p95_ms: Option<f64>,
    #[serde(default)]
    pub dispatch_p99_ms: Option<f64>,
}

impl ScenarioResult {
    /// Whether this row's throughput may be published as an absolute value.
    fn is_framework(&self) -> bool {
        self.handler_cost == COST_FRAMEWORK
    }

    /// A negligible-handler consume row whose window includes setup: its
    /// throughput is a lower bound on the drain rate, never the rate itself.
    fn is_setup_bound(&self) -> bool {
        self.handler_cost == COST_SETUP_BOUND
    }

    /// The three dispatch percentiles — present, finite and non-negative — or
    /// `None`. The latency family refuses a row without them rather than
    /// defaulting anything to a 0 ms bar.
    fn percentiles(&self) -> Option<(f64, f64, f64)> {
        match (
            self.dispatch_p50_ms,
            self.dispatch_p95_ms,
            self.dispatch_p99_ms,
        ) {
            (Some(p50), Some(p95), Some(p99))
                if [p50, p95, p99].iter().all(|p| p.is_finite() && *p >= 0.0) =>
            {
                Some((p50, p95, p99))
            }
            _ => None,
        }
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
    /// The latency family's selected row carries no usable percentiles. A
    /// `#[serde(default)]` zero here would publish a ~0 ms bar on an absolute
    /// axis — the clean-looking chart the contract forbids.
    MissingPercentiles {
        backend: String,
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
            Self::MissingPercentiles { backend } => write!(
                f,
                "backend `{backend}`'s selected latency row carries no usable \
                 dispatch percentiles. Refusing to render rather than \
                 publishing a 0 ms bar on an absolute axis."
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
            // Since v3 the batch knobs are present on every `consume_batch`
            // row and no other — a knob-less batch bar has no stated batch
            // size, and knobs on a non-batch row caption a batch that never
            // ran.
            let has_knobs = row.max_batch_size.is_some() && row.max_batch_age_ms.is_some();
            if row.flow == "consume_batch" && !has_knobs {
                return malformed("a consume_batch row is missing its batch knobs");
            }
            if row.flow != "consume_batch"
                && (row.max_batch_size.is_some() || row.max_batch_age_ms.is_some())
            {
                return malformed("a non-batch row carries batch knobs");
            }
        }
        if !run.results.is_empty() {
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

    /// The filenames are a contract with the docs and README that embed them;
    /// they are pinned in the `chart-manifest` document and must not drift.
    pub fn filename(self) -> &'static str {
        match self {
            Self::ThroughputVsConsumers => "throughput-vs-consumers.svg",
            Self::ThroughputVsPayload => "throughput-vs-payload.svg",
            Self::ParallelVsSequenced => "parallel-vs-sequenced.svg",
            Self::DispatchLatency => "dispatch-latency.svg",
            Self::FrameworkOverhead => "framework-overhead.svg",
        }
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

// ── Style: mid-tone, background-agnostic ────────────────────────────────────

const WIDTH: u32 = 960;
const HEIGHT: u32 = 560;

/// Text and axis grey. Contrast is ~4.9:1 on white and ~3.6:1 on a typical dark
/// page background — the best a single file can do without assuming one. All
/// text is drawn at 14px or larger so the 3:1 large-text threshold applies on
/// the dark side.
const INK: RGBColor = RGBColor(0x6E, 0x76, 0x81);
/// Grid lines: same hue, light enough not to compete with the series.
const GRID: RGBColor = RGBColor(0x9A, 0xA1, 0xAA);

const FONT: &str = "sans-serif";
/// Plotters treats these as points and emits roughly `0.806 x` in the SVG's
/// `font-size`, so the declared numbers are ~1.24x the pixel size we actually
/// want. Rendered: title ~21px, everything else ~14.5px — at or above the
/// 3:1 WCAG large-text threshold, which is what makes a single mid-tone ink
/// legible on both a light and a dark page.
const TITLE_PX: i32 = 26;
const LABEL_PX: i32 = 18;
const FOOT_PX: i32 = 18;

/// The one mid-tone palette every chart draws from. None is white or
/// near-black, so each reads against a light and a dark page — and because
/// the backend legend and the per-family series index into the same values,
/// one hue never means two different things across the five published SVGs.
pub const PALETTE: [RGBColor; 6] = [
    RGBColor(0x5B, 0x8F, 0xF9),
    RGBColor(0xE8, 0x87, 0x3A),
    RGBColor(0x3F, 0xA6, 0x5C),
    RGBColor(0xC7, 0x5C, 0x5C),
    RGBColor(0x9B, 0x6B, 0xD6),
    RGBColor(0x2F, 0xA8, 0xA8),
];

/// A series colour by index, wrapping deterministically rather than silently
/// reusing the first hue — two series sharing a colour under distinct legend
/// labels is a chart that cannot be read.
fn palette_colour(i: usize) -> RGBColor {
    PALETTE[i % PALETTE.len()]
}

/// Mid-tone series colours keyed by backend.
fn backend_colour(backend: &str) -> RGBColor {
    match backend {
        "inmemory" => PALETTE[0],
        "kafka" => PALETTE[1],
        "nats" => PALETTE[2],
        "rabbitmq" => PALETTE[3],
        "redis" => PALETTE[4],
        "sqs" => PALETTE[5],
        // Deterministic fallback for a backend key added after this file:
        // a fixed hash of the name, never an iteration index.
        other => {
            let seed = other
                .bytes()
                .fold(17u32, |a, b| a.wrapping_mul(31).wrapping_add(b as u32));
            RGBColor(
                0x50u8.saturating_add((seed % 96) as u8),
                0x60u8.saturating_add((seed / 96 % 96) as u8),
                0x70u8.saturating_add((seed / 9216 % 96) as u8),
            )
        }
    }
}

fn ink(px: i32) -> TextStyle<'static> {
    (FONT, px).into_font().color(&INK)
}

// ── How each backend is accounted for in a chart ────────────────────────────

/// Every backend in the document lands in exactly one of these for every
/// chart. There is deliberately no "silently absent" case: rule 3 exists
/// because omission reads to a reader as "we forgot to measure it".
#[derive(Debug, Clone)]
enum Presence {
    /// Real numbers, safe to publish as absolute values. `partial` says the
    /// slice also held setup-bound cells that are *not* plotted — named in the
    /// caption so a shorter line reads as a caveat, not a smaller sweep.
    Absolute {
        points: Vec<(f64, f64)>,
        partial: bool,
    },
    /// `representative: false` — the shape is real, the magnitude is not.
    /// Values are normalised to their own maximum and the axis numbers never
    /// describe them.
    ShapeOnly(Vec<(f64, f64)>),
    /// The slice was measured, but every row is `setup_bound`: the windows
    /// include coordination cost, so there is no drain rate to publish. Named
    /// rather than plotted — a lower bound drawn as a line would be read as
    /// the rate.
    SetupBoundOnly,
    /// Declared in `unsupported[]`: the backend cannot do this flow at all.
    Unsupported(String),
    /// Supported, but this document carries no row for the chart's slice.
    NotMeasured,
}

/// Normalise to the series' own maximum, so the curve's shape survives but no
/// absolute magnitude is ever recoverable from the chart.
fn shape_only(points: Vec<(f64, f64)>) -> Presence {
    let peak = points.iter().fold(0.0f64, |a, (_, y)| a.max(*y));
    if peak <= 0.0 {
        return Presence::ShapeOnly(points.into_iter().map(|(x, _)| (x, 0.0)).collect());
    }
    Presence::ShapeOnly(points.into_iter().map(|(x, y)| (x, y / peak)).collect())
}

/// Build one `Presence` per backend for a chart slice.
///
/// `extract` returns the slice's *publishable* points for a run (framework
/// rows only); `in_slice_setup_bound` reports whether the run measured the
/// slice but produced only setup-bound windows; `flows_for_support` names the
/// flow whose `unsupported[]` entry explains an absence.
fn presences<F, S>(
    doc: &Document,
    flows_for_support: &[&str],
    extract: F,
    in_slice_setup_bound: S,
) -> BTreeMap<String, Presence>
where
    F: Fn(&BackendRun) -> Vec<(f64, f64)>,
    S: Fn(&BackendRun) -> bool,
{
    let mut out = BTreeMap::new();
    for run in &doc.runs {
        let points = extract(run);
        let presence = if !points.is_empty() {
            if run.representative {
                Presence::Absolute {
                    points,
                    partial: in_slice_setup_bound(run),
                }
            } else {
                shape_only(points)
            }
        } else if in_slice_setup_bound(run) {
            Presence::SetupBoundOnly
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
fn provenance(doc: &Document) -> (String, String) {
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
        .flat_map(|run| run.results.iter())
        .filter(|row| !row.handler.is_empty())
        .map(|row| row.handler.as_str())
        .collect();
    if !handlers.is_empty() {
        let joined = handlers.into_iter().collect::<Vec<_>>().join(", ");
        parts.push(format!("handler: {joined}"));
    }
    (first, parts.join(" — "))
}

fn fmt_count(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("{:.0}k", v / 1_000.0)
    } else if v >= 10.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
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
/// Baseline-to-baseline spacing for the footer lines.
const LINE: i32 = 19;
/// Characters per footer line.
///
/// An estimate, not a measurement — and deliberately so. `plotters` runs
/// without the `ttf` feature, so it has no real text metrics either, and an
/// estimate is the thing that stays equal on every host. Derived from the
/// usable width (`WIDTH - 2 * GUTTER`) over an average advance of ~0.52em at
/// the rendered ~14.5px, rounded down for headroom.
const NOTE_WRAP: usize = 108;

/// Break a note across lines at word boundaries so a long `unsupported[]`
/// reason cannot run off the canvas. A reason that leaves the page is the same
/// failure as no reason at all.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
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
fn frame<'b>(
    root: &DrawingArea<SVGBackend<'b>, Shift>,
    doc: &Document,
    title: &str,
    subtitle: &str,
    notes: &[String],
) -> Result<DrawingArea<SVGBackend<'b>, Shift>, ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());

    root.draw_text(
        title,
        &(FONT, TITLE_PX).into_font().color(&INK),
        (GUTTER, 18),
    )
    .map_err(render)?;
    root.draw_text(subtitle, &ink(LABEL_PX), (GUTTER, 48))
        .map_err(render)?;

    let (line1, line2) = provenance(doc);
    let mut footer: Vec<String> = notes.iter().flat_map(|n| wrap(n, NOTE_WRAP)).collect();
    footer.extend(wrap(&line1, NOTE_WRAP));
    if !line2.is_empty() {
        footer.extend(wrap(&line2, NOTE_WRAP));
    }

    // Lay the block out upward from a fixed bottom, so the last baseline plus
    // its descender always clears the canvas edge.
    let bottom = HEIGHT as i32 - 26;
    let top_of_footer = bottom - LINE * (footer.len() as i32 - 1);
    // Past this point the block would overprint the title band and leave the
    // chart body a negative-height region rendered as garbage — a document
    // pathological enough to get here gets a loud refusal instead.
    if top_of_footer < 96 {
        return Err(ChartError::Render(format!(
            "the caption block ({} lines) leaves no room for the chart body",
            footer.len()
        )));
    }
    for (i, line) in footer.iter().enumerate() {
        root.draw_text(
            line,
            &ink(FOOT_PX),
            (GUTTER, top_of_footer + i as i32 * LINE),
        )
        .map_err(render)?;
    }

    let reserved = (HEIGHT as i32 - top_of_footer + 18).max(0) as u32;
    Ok(root.margin(70, reserved, 16, 16))
}

/// The notes line for each backend that is not plotted.
fn notes_for(doc: &Document, presences: &BTreeMap<String, Presence>) -> Vec<String> {
    let mut notes = Vec::new();
    for (backend, presence) in presences {
        match presence {
            Presence::Unsupported(reason) => {
                notes.push(format!("{backend}: not supported — {reason}"))
            }
            // Deliberately NOT "not supported": this variant means the
            // capability exists and this document simply carries no row for
            // the slice. Publishing it as a capability hole would be a false
            // claim about the library rather than about the run.
            Presence::NotMeasured => notes.push(format!(
                "{backend}: no measurement for this slice in this run \
                 (supported — a gap in the sweep, not a capability hole)"
            )),
            Presence::SetupBoundOnly => notes.push(format!(
                "{backend}: setup-bound — the measured window includes \
                 coordination cost, so no drain rate is published for this slice"
            )),
            Presence::ShapeOnly(_) => {
                if let Some(run) = doc.runs.iter().find(|r| &r.backend == backend) {
                    notes.push(shape_only_note(run));
                }
            }
            Presence::Absolute { partial: true, .. } => notes.push(format!(
                "{backend}: partial — setup-bound cells in this slice are not \
                 plotted (window includes coordination cost)"
            )),
            Presence::Absolute { partial: false, .. } => {}
        }
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
fn failure_notes<P>(doc: &Document, in_slice: P) -> Vec<String>
where
    P: Fn(&FailedRow) -> bool,
{
    let mut notes = Vec::new();
    for run in &doc.runs {
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

/// The absolute y-range, taken only from series that are safe to publish.
/// `ShapeOnly` series never widen it — that is what keeps a non-representative
/// magnitude out of the axis numbers.
fn absolute_range(presences: &BTreeMap<String, Presence>) -> Option<(f64, f64)> {
    let mut peak = f64::MIN;
    let mut seen = false;
    for presence in presences.values() {
        if let Presence::Absolute { points, .. } = presence {
            for (_, y) in points {
                if *y > peak {
                    peak = *y;
                }
                seen = true;
            }
        }
    }
    if seen && peak > 0.0 {
        Some((0.0, peak * 1.12))
    } else {
        None
    }
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
/// The centred label style shared by the category labels, both chart kinds'
/// x-axis descriptions and the n/s markers.
fn centred_label() -> TextStyle<'static> {
    (FONT, LABEL_PX)
        .into_font()
        .color(&INK)
        .pos(Pos::new(HPos::Center, VPos::Top))
}

/// Draw the x-axis description centred under the plot area — one
/// implementation, so a spacing tweak cannot move the two chart kinds apart.
fn draw_x_desc<CT>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &ChartContext<'_, SVGBackend<'_>, CT>,
    x_desc: &str,
) -> Result<(), ChartError>
where
    CT: CoordTranslate<From = (f64, f64)>,
{
    let px = chart.plotting_area().get_pixel_range();
    let (x0, x1) = (px.0.start, px.0.end);
    let y1 = px.1.end;
    root.draw_text(x_desc, &centred_label(), ((x0 + x1) / 2, y1 + 28))
        .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))
}

fn draw_categories<CT>(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &ChartContext<'_, SVGBackend<'_>, CT>,
    labels: &[String],
) -> Result<(), ChartError>
where
    CT: CoordTranslate<From = (f64, f64)>,
{
    let area = chart.plotting_area();
    let y_bottom = area.get_pixel_range().1.end;
    let style = centred_label();
    for (i, label) in labels.iter().enumerate() {
        let (px, _) = area.map_coordinate(&(i as f64, 0.0));
        root.draw_text(label, &style, (px, y_bottom + 8))
            .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))?;
    }
    Ok(())
}

/// A legend entry drawn as a swatch plus a label, laid out left to right along
/// the top of the plot area.
fn draw_legend(
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    entries: &[(String, RGBColor)],
    origin: (i32, i32),
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let mut x = origin.0;
    for (label, colour) in entries {
        root.draw(&Rectangle::new(
            [(x, origin.1 + 3), (x + 14, origin.1 + 11)],
            ShapeStyle {
                color: colour.to_rgba(),
                filled: true,
                stroke_width: 0,
            },
        ))
        .map_err(render)?;
        root.draw_text(label, &ink(LABEL_PX), (x + 20, origin.1))
            .map_err(render)?;
        // Advance by an estimate rather than a measurement: text extents are
        // approximated (no ttf feature), and the estimate is what stays equal
        // across hosts.
        x += 20 + 9 * label.chars().count() as i32 + 24;
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
) -> Result<(), ChartError> {
    let in_slice =
        |r: &ScenarioResult| r.flow == HEADLINE_FLOW && r.payload_bytes == HEADLINE_PAYLOAD;
    render_line_family(
        doc,
        root,
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
        in_slice,
        |r| r.consumers,
        |c| format!("{c}"),
        |f| f.flow == HEADLINE_FLOW && f.payload_bytes == HEADLINE_PAYLOAD,
    )
}

// ── Family 2: throughput vs payload size ────────────────────────────────────

fn render_throughput_vs_payload(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    let in_slice = |r: &ScenarioResult| r.flow == HEADLINE_FLOW && r.consumers == BASE_CONSUMERS;
    render_line_family(
        doc,
        root,
        Family::ThroughputVsPayload.title(),
        &format!("{HEADLINE_FLOW} — {BASE_CONSUMERS} consumer — higher is better"),
        "throughput-vs-payload",
        format!("{HEADLINE_FLOW} @ {BASE_CONSUMERS} consumer ({COST_FRAMEWORK} rows)"),
        "payload size",
        in_slice,
        |r| r.payload_bytes,
        |s| fmt_payload(*s),
        |f| f.flow == HEADLINE_FLOW && f.consumers == BASE_CONSUMERS,
    )
}

/// The shared slice pipeline for the two line families: union the category
/// keys from framework rows (a setup-bound cell must not open an empty
/// category), refuse an empty slice, fold each backend to its **best
/// framework row per category** — a valid document can carry two rows at one
/// key (two tiers, two corpus sizes), and a line needs one value per x, not a
/// vertical zigzag whose value is document order — then render.
#[allow(clippy::too_many_arguments)]
fn render_line_family<K, P, KF, LF, FF>(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    title: &str,
    subtitle: &str,
    family_key: &'static str,
    slice_desc: String,
    x_desc: &str,
    in_slice: P,
    key_of: KF,
    label_of: LF,
    in_failed_slice: FF,
) -> Result<(), ChartError>
where
    K: Ord + Copy,
    P: Fn(&ScenarioResult) -> bool + Copy,
    KF: Fn(&ScenarioResult) -> K + Copy,
    LF: Fn(&K) -> String,
    FF: Fn(&FailedRow) -> bool,
{
    let mut keys: BTreeSet<K> = BTreeSet::new();
    for run in &doc.runs {
        for r in &run.results {
            if in_slice(r) && r.is_framework() {
                keys.insert(key_of(r));
            }
        }
    }
    let keys: Vec<K> = keys.into_iter().collect();
    if keys.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: family_key,
            slice: slice_desc,
        });
    }
    let idx: BTreeMap<K, f64> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (*k, i as f64))
        .collect();

    let presences = presences(
        doc,
        &[HEADLINE_FLOW],
        |run| {
            // `idx` iterates in key order, so the points come out sorted.
            idx.iter()
                .filter_map(|(k, x)| {
                    best_by_throughput(
                        run.results
                            .iter()
                            .filter(|r| in_slice(r) && r.is_framework() && key_of(r) == *k),
                    )
                    .map(|r| (*x, r.throughput_msg_per_sec))
                })
                .collect()
        },
        |run| {
            run.results
                .iter()
                .any(|r| in_slice(r) && r.is_setup_bound())
        },
    );

    let labels: Vec<String> = keys.iter().map(label_of).collect();
    line_chart(
        doc,
        root,
        title,
        subtitle,
        "messages / second",
        &labels,
        x_desc,
        &presences,
        &failure_notes(doc, in_failed_slice),
    )
}

/// Shared line-chart body for families 1 and 2.
#[allow(clippy::too_many_arguments)]
fn line_chart(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
    title: &str,
    subtitle: &str,
    y_desc: &str,
    x_labels: &[String],
    x_desc: &str,
    presences: &BTreeMap<String, Presence>,
    extra_notes: &[String],
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let mut notes = notes_for(doc, presences);
    notes.extend_from_slice(extra_notes);
    let area = frame(root, doc, title, subtitle, &notes)?;

    // Absolute magnitudes come only from representative runs. A shape-only
    // series is drawn against the same box but its values are fractions, so it
    // can never be read off the axis.
    let range = absolute_range(presences);
    let absolute_axis = range.is_some();
    let (y_lo, y_hi) = range.unwrap_or((0.0, 1.0));

    let n = x_labels.len();
    let mut chart = ChartBuilder::on(&area)
        .margin_top(26)
        .margin_bottom(26)
        .x_label_area_size(0)
        .y_label_area_size(74)
        .build_cartesian_2d(-0.35f64..(n as f64 - 0.65), y_lo..y_hi)
        .map_err(render)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .light_line_style(GRID.mix(0.18))
        .bold_line_style(GRID.mix(0.32))
        .axis_style(stroke(GRID, 1))
        .y_desc(if absolute_axis {
            y_desc.to_string()
        } else {
            format!("{y_desc} (relative — no representative run)")
        })
        .y_label_style(ink(LABEL_PX))
        .axis_desc_style(ink(LABEL_PX))
        .y_label_formatter(&|v: &f64| fmt_count(*v))
        .x_labels(0)
        .x_label_formatter(&|_: &f64| String::new())
        .draw()
        .map_err(render)?;

    let mut legend: Vec<(String, RGBColor)> = Vec::new();
    for (backend, presence) in presences {
        let colour = backend_colour(backend);
        match presence {
            Presence::Absolute { points, .. } => {
                chart
                    .draw_series(LineSeries::new(points.iter().copied(), stroke(colour, 3)))
                    .map_err(render)?;
                for (x, y) in points {
                    chart
                        .draw_series(std::iter::once(Circle::new(
                            (*x, *y),
                            4,
                            ShapeStyle {
                                color: colour.to_rgba(),
                                filled: true,
                                stroke_width: 0,
                            },
                        )))
                        .map_err(render)?;
                }
                let label = if points.len() == 1 {
                    format!("{backend} (single point)")
                } else {
                    backend.clone()
                };
                legend.push((label, colour));
            }
            Presence::ShapeOnly(points) => {
                // Fractions mapped onto the visible box: the curve's shape is
                // preserved, the magnitude is not recoverable.
                let mapped: Vec<(f64, f64)> = points
                    .iter()
                    .map(|(x, f)| (*x, y_lo + (y_hi - y_lo) * f))
                    .collect();
                chart
                    .draw_series(LineSeries::new(mapped.iter().copied(), stroke(colour, 2)))
                    .map_err(render)?;
                legend.push((format!("{backend} (shape only)"), colour));
            }
            Presence::Unsupported(_) | Presence::NotMeasured | Presence::SetupBoundOnly => {}
        }
    }

    draw_categories(root, &chart, x_labels)?;
    draw_x_desc(root, &chart, x_desc)?;
    draw_legend(root, &legend, (94, 74))?;
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
    title: &str,
    subtitle: &str,
    y_desc: &str,
    x_desc: &str,
    series: &[(String, RGBColor)],
    groups: &[BarGroup],
    notes: &[String],
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    if groups.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "bar-chart",
            slice: title.to_string(),
        });
    }
    let area = frame(root, doc, title, subtitle, notes)?;

    // Only representative groups set the scale, so a non-representative
    // magnitude can never be read off the axis. Lower-bound bars do
    // participate: they are drawn at their value, and an axis scaled by an
    // under-statement never over-claims.
    let mut peak = 0.0f64;
    for g in groups.iter().filter(|g| !g.shape_only) {
        for b in g.bars.iter().flatten() {
            if b.value > peak {
                peak = b.value;
            }
        }
    }
    let absolute_axis = peak > 0.0;
    let y_hi = if absolute_axis { peak * 1.14 } else { 1.0 };

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
        .light_line_style(GRID.mix(0.18))
        .bold_line_style(GRID.mix(0.32))
        .axis_style(stroke(GRID, 1))
        .y_desc(if absolute_axis {
            y_desc.to_string()
        } else {
            format!("{y_desc} (relative — no representative run)")
        })
        .y_label_style(ink(LABEL_PX))
        .axis_desc_style(ink(LABEL_PX))
        .y_label_formatter(&|v: &f64| fmt_count(*v))
        .x_labels(0)
        .x_label_formatter(&|_: &f64| String::new())
        .draw()
        .map_err(render)?;

    // Bars fill 76% of a slot, leaving a visible gutter between groups.
    let slot = 0.76f64;
    let width = slot / series.len().max(1) as f64;

    let marker: TextStyle<'_> = (FONT, LABEL_PX)
        .into_font()
        .color(&INK)
        .pos(Pos::new(HPos::Center, VPos::Bottom));

    for (gi, group) in groups.iter().enumerate() {
        // A shape-only group is scaled to its own maximum: the relative
        // heights survive, the magnitudes do not.
        let local_peak = group
            .bars
            .iter()
            .flatten()
            .fold(0.0f64, |a, b| a.max(b.value));
        for (si, (_, colour)) in series.iter().enumerate() {
            let left = gi as f64 - slot / 2.0 + width * si as f64;
            let right = left + width * 0.86;
            let centre = (left + right) / 2.0;
            match group.bars.get(si).copied().flatten() {
                Some(bar) => {
                    let height = if group.shape_only {
                        if local_peak > 0.0 {
                            y_hi * 0.92 * (bar.value / local_peak)
                        } else {
                            0.0
                        }
                    } else {
                        bar.value
                    };
                    chart
                        .draw_series(std::iter::once(Rectangle::new(
                            [(left, 0.0), (right, height)],
                            ShapeStyle {
                                // Muted fill for anything the axis numbers do
                                // not fully describe: a normalised group, or a
                                // lower-bound bar whose caption carries the ≥.
                                color: if group.shape_only || bar.lower_bound {
                                    colour.mix(0.55)
                                } else {
                                    colour.to_rgba()
                                },
                                filled: true,
                                stroke_width: 0,
                            },
                        )))
                        .map_err(render)?;
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
    draw_categories(root, &chart, &labels)?;
    draw_x_desc(root, &chart, x_desc)?;
    draw_legend(root, series, (94, 74))?;
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
) -> Result<(), ChartError> {
    let series: Vec<(String, RGBColor)> = MODES
        .iter()
        .enumerate()
        .map(|(i, (_, _, label))| ((*label).to_string(), palette_colour(i)))
        .collect();

    let mut groups = Vec::new();
    let mut notes = Vec::new();
    let mut batch_knobs: BTreeSet<(String, u64, u64)> = BTreeSet::new();
    // Aggregated caption lines, keyed by (mode index, text) so identical
    // explanations across backends collapse to one line naming them all —
    // six single-backend repeats of "run_batch is Kafka-only" would crowd
    // out the caption block the notes exist to fill.
    let mut missing: BTreeMap<(usize, String), Vec<String>> = BTreeMap::new();
    let mut bounded: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    let mut workers: BTreeMap<(usize, u32), Vec<String>> = BTreeMap::new();

    for run in &doc.runs {
        // Per mode: an absolute bar from the framework rows if any exist,
        // else a lower-bound bar from the setup-bound rows. Sequenced consume
        // holds no readiness barrier, so its zero/fast rows are *always*
        // setup-bound — without the lower-bound arm the ordering-cost chart
        // would lose the sequenced bar it exists to show. Sleeping-handler
        // rows (`handler_bound` / `handler_amortised`) never reach either arm:
        // their number is the simulated sleep, not shove.
        let bars: Vec<Option<Bar>> = MODES
            .iter()
            .enumerate()
            .map(|(mi, (mode, flow, _))| {
                let in_mode = |r: &&ScenarioResult| {
                    r.mode == *mode
                        && CONSUME_FLOWS.contains(&r.flow.as_str())
                        && r.payload_bytes == HEADLINE_PAYLOAD
                };
                // Each mode is measured at its own worker count: fifo pins
                // its workers to the shard count and batch to its configured
                // set, so requiring the global BASE_CONSUMERS here would
                // erase the fifo bar on every backend that measured it. Take
                // the mode's least-parallel measurement, and say so in the
                // caption when that count is not 1.
                let arm = |lower_bound: bool| -> Option<(Bar, &ScenarioResult)> {
                    let is_arm = |r: &&ScenarioResult| {
                        if lower_bound {
                            r.is_setup_bound()
                        } else {
                            r.is_framework()
                        }
                    };
                    let min = run
                        .results
                        .iter()
                        .filter(in_mode)
                        .filter(is_arm)
                        .map(|r| r.consumers)
                        .min()?;
                    best_by_throughput(
                        run.results
                            .iter()
                            .filter(in_mode)
                            .filter(is_arm)
                            .filter(|r| r.consumers == min),
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
                        workers
                            .entry((mi, row.consumers))
                            .or_default()
                            .push(run.backend.clone());
                    }
                    if bar.lower_bound && run.representative {
                        bounded.entry(mi).or_default().push(run.backend.clone());
                    }
                    // A published batch bar states its knobs: since v3 they
                    // are what distinguishes a 50-message-batch row from a
                    // 500-message-batch one, so a bar without them would be a
                    // number with no stated batch size.
                    if let (Some(size), Some(age)) = (row.max_batch_size, row.max_batch_age_ms) {
                        batch_knobs.insert((run.backend.clone(), size, age));
                    }
                } else {
                    // Explain the n/s marker: the declared capability hole
                    // when the document names one, and a supported-but-absent
                    // gap otherwise — never "not supported" for a mere gap.
                    let text = match unsupported_reason(run, flow) {
                        Some(reason) => format!("not supported — {reason}"),
                        None => "no measurement in this slice of this run \
                                 (supported — a gap, not a capability hole)"
                            .to_string(),
                    };
                    missing
                        .entry((mi, text))
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

    for ((mi, text), backends) in &missing {
        let label = MODES[*mi].2;
        notes.push(format!("{} / {label}: {text}", backends.join(", ")));
    }
    for (mi, backends) in &bounded {
        let label = MODES[*mi].2;
        notes.push(format!(
            "{} / {label}: lower bound (muted bar) — this window cannot \
             separate setup from drain, so the true rate is at least the bar \
             shown",
            backends.join(", ")
        ));
    }
    for ((mi, count), backends) in &workers {
        let label = MODES[*mi].2;
        notes.push(format!(
            "{} / {label}: measured at {count} workers (this mode pins its \
             own worker count)",
            backends.join(", ")
        ));
    }

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

    notes.extend(failure_notes(doc, |f| {
        f.payload_bytes == HEADLINE_PAYLOAD && CONSUME_FLOWS.contains(&f.flow.as_str())
    }));

    bar_chart(
        doc,
        root,
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

fn render_dispatch_latency(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    let series: Vec<(String, RGBColor)> = vec![
        ("p50".to_string(), palette_colour(0)),
        ("p95".to_string(), palette_colour(1)),
        ("p99".to_string(), palette_colour(3)),
    ];

    let mut groups = Vec::new();
    let mut notes = Vec::new();
    for run in &doc.runs {
        // Percentiles are per-message, so unlike a throughput they survive an
        // unseparated window — but only under a negligible handler. A sleeping
        // handler queues every message behind the sleeps before it, which
        // makes the tail percentiles the handler's, not shove's. Prefer the
        // framework row; fall back to a setup-bound one with a caption note,
        // since its early messages may still have queued behind the group
        // join.
        let in_slice = |r: &&ScenarioResult| {
            r.flow == HEADLINE_FLOW
                && r.payload_bytes == HEADLINE_PAYLOAD
                && r.consumers == BASE_CONSUMERS
        };
        let row = run
            .results
            .iter()
            .filter(in_slice)
            .find(|r| r.is_framework())
            .or_else(|| {
                run.results
                    .iter()
                    .filter(in_slice)
                    .find(|r| r.is_setup_bound())
            });
        match row {
            Some(r) => {
                // A selected row without usable percentiles is a refusal, not
                // a zero: `#[serde(default)]` zeros here would publish a ~0 ms
                // bar on an absolute axis scaled by the other backends.
                let (p50, p95, p99) =
                    r.percentiles()
                        .ok_or_else(|| ChartError::MissingPercentiles {
                            backend: run.backend.clone(),
                        })?;
                if !run.representative {
                    notes.push(shape_only_note(run));
                }
                if r.is_setup_bound() && run.representative {
                    notes.push(format!(
                        "{}: setup-bound window — early messages may include \
                         coordination queueing in these percentiles",
                        run.backend
                    ));
                }
                groups.push(BarGroup {
                    label: run.backend.clone(),
                    bars: vec![
                        Some(Bar::absolute(p50)),
                        Some(Bar::absolute(p95)),
                        Some(Bar::absolute(p99)),
                    ],
                    shape_only: !run.representative,
                });
            }
            None => {
                // The backend stays on the axis as explicit n/s markers; the
                // note then says whether that is a declared hole or a gap.
                let note = match unsupported_reason(run, HEADLINE_FLOW) {
                    Some(reason) => format!("{}: not supported — {reason}", run.backend),
                    None => format!(
                        "{}: no measurement for this slice in this run \
                         (supported — a gap, not a capability hole)",
                        run.backend
                    ),
                };
                notes.push(note);
                groups.push(BarGroup {
                    label: run.backend.clone(),
                    bars: vec![None, None, None],
                    shape_only: false,
                });
            }
        }
    }

    notes.extend(failure_notes(doc, |f| {
        f.flow == HEADLINE_FLOW
            && f.payload_bytes == HEADLINE_PAYLOAD
            && f.consumers == BASE_CONSUMERS
    }));

    bar_chart(
        doc,
        root,
        Family::DispatchLatency.title(),
        &format!(
            "{HEADLINE_FLOW} — {} payload — {BASE_CONSUMERS} consumer — lower is better",
            fmt_payload(HEADLINE_PAYLOAD)
        ),
        "milliseconds",
        "backend",
        &series,
        &groups,
        &notes,
    )
}

// ── Family 5: framework overhead, ns/msg per flow ───────────────────────────

fn render_framework_overhead(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    let run = doc
        .runs
        .iter()
        .find(|r| r.backend == IN_PROCESS_BACKEND)
        .ok_or(ChartError::MissingInProcessRun)?;

    let series: Vec<(String, RGBColor)> = vec![(
        format!("{IN_PROCESS_BACKEND} — nanoseconds per message"),
        palette_colour(0),
    )];

    // Every flow the schema knows about gets a column: the ones measured carry
    // a bar, the ones declared unsupported carry the `n/s` marker. A flow that
    // is neither is not invented.
    let mut groups = Vec::new();
    let mut notes = Vec::new();
    let mut unmeasured: Vec<&str> = Vec::new();
    let mut setup_bound: Vec<&str> = Vec::new();
    for flow in KNOWN_FLOWS {
        let in_slice = |r: &&ScenarioResult| {
            r.flow == *flow
                && r.payload_bytes == OVERHEAD_PAYLOAD
                && r.consumers == BASE_CONSUMERS
                && r.throughput_msg_per_sec > 0.0
        };
        // "What shove itself costs" is only what the marker certifies as
        // shove: a drain-windowed framework row, or a publish row (no handler
        // exists to contaminate it). A setup-bound row would *over-state* the
        // cost — ns/msg is the reciprocal of an under-stated rate — so those
        // flows are named in the caption instead of charted too high.
        // Best observed cost per message — the highest throughput, since
        // ns/msg is its reciprocal: the floor is the framework's own cost,
        // anything above it is scheduling noise.
        let measured = best_by_throughput(
            run.results
                .iter()
                .filter(in_slice)
                .filter(|r| r.is_framework() || r.handler_cost == COST_NO_HANDLER),
        );

        match measured {
            Some(row) => {
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
            None if run
                .results
                .iter()
                .filter(in_slice)
                .any(|r| r.is_setup_bound()) =>
            {
                setup_bound.push(flow);
            }
            None => match unsupported_reason(run, flow) {
                Some(reason) => {
                    groups.push(BarGroup {
                        label: (*flow).to_string(),
                        bars: vec![None],
                        shape_only: false,
                    });
                    notes.push(format!("{flow}: not supported — {reason}"));
                }
                // Neither measured nor declared. Inventing a column would
                // imply a capability hole that does not exist, but dropping it
                // in silence is the omission the marker rule exists to
                // prevent — so it is named in the caption instead.
                None => unmeasured.push(*flow),
            },
        }
    }

    if !setup_bound.is_empty() {
        notes.push(format!(
            "setup-bound in this run (window includes coordination cost; \
             no framework number published): {}",
            setup_bound.join(", ")
        ));
    }
    if !unmeasured.is_empty() {
        notes.push(format!(
            "not measured in this run (supported, no number here): {}",
            unmeasured.join(", ")
        ));
    }

    let failed = run
        .failures
        .iter()
        .filter(|f| f.payload_bytes == OVERHEAD_PAYLOAD && f.consumers == BASE_CONSUMERS)
        .count();
    if failed > 0 {
        notes.push(format!(
            "{failed} cell(s) in this slice failed to run — absent, not zero; \
             see failures[] in the results document"
        ));
    }

    if groups.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "framework-overhead",
            slice: format!(
                "{IN_PROCESS_BACKEND} @ {} / {BASE_CONSUMERS} consumer",
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
        Family::FrameworkOverhead.title(),
        &format!(
            "in-process backend — {} payload — {BASE_CONSUMERS} consumer — \
             what shove itself costs, broker removed — lower is better",
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
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    match family {
        Family::ThroughputVsConsumers => render_throughput_vs_consumers(doc, root),
        Family::ThroughputVsPayload => render_throughput_vs_payload(doc, root),
        Family::ParallelVsSequenced => render_parallel_vs_sequenced(doc, root),
        Family::DispatchLatency => render_dispatch_latency(doc, root),
        Family::FrameworkOverhead => render_framework_overhead(doc, root),
    }
}

/// Render one family to an SVG string. This is what the tests assert against —
/// the file-writing path below is the same code with a different sink.
pub fn render_to_string(doc: &Document, family: Family) -> Result<String, ChartError> {
    validate(doc)?;
    let mut buf = String::new();
    {
        let root = SVGBackend::with_string(&mut buf, (WIDTH, HEIGHT)).into_drawing_area();
        render_into(doc, family, &root)?;
        root.present()
            .map_err(|e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string()))?;
    }
    Ok(buf)
}

/// Render every family into `out_dir`, which must already exist.
///
/// Validation runs once up front, so a document that violates the contract
/// fails before a single file is touched — a half-written chart directory is
/// worse than none.
pub fn generate(doc: &Document, out_dir: &Path) -> Result<Vec<String>, ChartError> {
    validate(doc)?;
    let mut written = Vec::new();
    for family in Family::ALL {
        let svg = render_to_string(doc, family)?;
        let path = out_dir.join(family.filename());
        std::fs::write(&path, svg.as_bytes())?;
        written.push(family.filename().to_string());
    }
    Ok(written)
}
