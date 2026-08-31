//! Chart generation from the versioned benchmark results document.
//!
//! Reads `benches/results/bench-results.json` (the `bench-schema` v1 contract)
//! and renders five SVG chart families into `docs/public/bench/`.
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
/// mis-rendered: a v2 file read as v1 would silently drop or misread whatever
/// changed, and a chart is exactly the artifact where that goes unnoticed.
pub const SCHEMA_VERSION: u32 = 1;

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

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioResult {
    pub flow: String,
    pub mode: String,
    pub payload_bytes: u64,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub messages: u64,
    pub consumers: u32,
    #[serde(default)]
    pub handler: String,
    pub throughput_msg_per_sec: f64,
    #[serde(default)]
    pub dispatch_p50_ms: f64,
    #[serde(default)]
    pub dispatch_p95_ms: f64,
    #[serde(default)]
    pub dispatch_p99_ms: f64,
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

/// Rules 1 and 5 of the schema contract. Checked once, before any chart is
/// drawn, so a bad document fails before it can write a single file.
pub fn validate(doc: &Document) -> Result<(), ChartError> {
    if doc.schema_version != SCHEMA_VERSION {
        return Err(ChartError::UnsupportedSchemaVersion {
            found: doc.schema_version,
            expected: SCHEMA_VERSION,
        });
    }

    for run in &doc.runs {
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

pub fn load(path: &Path) -> Result<Document, ChartError> {
    let raw = std::fs::read_to_string(path)?;
    let doc: Document = serde_json::from_str(&raw)?;
    validate(&doc)?;
    Ok(doc)
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

/// Mid-tone series colours keyed by backend. None is white or near-black, so
/// each reads against a light and a dark page.
fn backend_colour(backend: &str) -> RGBColor {
    match backend {
        "inmemory" => RGBColor(0x5B, 0x8F, 0xF9),
        "kafka" => RGBColor(0xE8, 0x87, 0x3A),
        "nats" => RGBColor(0x3F, 0xA6, 0x5C),
        "rabbitmq" => RGBColor(0xC7, 0x5C, 0x5C),
        "redis" => RGBColor(0x9B, 0x6B, 0xD6),
        "sqs" => RGBColor(0x2F, 0xA8, 0xA8),
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
    /// Real numbers, safe to publish as absolute values.
    Absolute(Vec<(f64, f64)>),
    /// `representative: false` — the shape is real, the magnitude is not.
    /// Values are normalised to their own maximum and the axis numbers never
    /// describe them.
    ShapeOnly(Vec<(f64, f64)>),
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
/// `extract` returns the slice's points for a run; `flow_for_support` names the
/// flow whose `unsupported[]` entry explains an absence.
fn presences<F>(
    doc: &Document,
    flows_for_support: &[&str],
    extract: F,
) -> BTreeMap<String, Presence>
where
    F: Fn(&BackendRun) -> Vec<(f64, f64)>,
{
    let mut out = BTreeMap::new();
    for run in &doc.runs {
        let points = extract(run);
        let presence = if !points.is_empty() {
            if run.representative {
                Presence::Absolute(points)
            } else {
                shape_only(points)
            }
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
    let mut second = String::new();
    if !doc.hardware.cpu.is_empty() {
        second.push_str(&doc.hardware.cpu);
    }
    if doc.hardware.physical_cores > 0 {
        if !second.is_empty() {
            second.push_str(" — ");
        }
        second.push_str(&format!("{} cores", doc.hardware.physical_cores));
    }
    if doc.hardware.ram_gb > 0 {
        second.push_str(&format!(" — {} GB RAM", doc.hardware.ram_gb));
    }
    if !doc.hardware.os.is_empty() {
        second.push_str(&format!(" — {}", doc.hardware.os));
    }
    if !doc.rust_version.is_empty() {
        second.push_str(&format!(" — {}", doc.rust_version));
    }
    (first, second)
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
fn notes_for(presences: &BTreeMap<String, Presence>) -> Vec<String> {
    let mut notes = Vec::new();
    for (backend, presence) in presences {
        match presence {
            Presence::Unsupported(reason) => {
                notes.push(format!("{backend}: not supported — {reason}"))
            }
            Presence::NotMeasured => notes.push(format!(
                "{backend}: not supported — no measurement for this slice in this run"
            )),
            Presence::ShapeOnly(_) => notes.push(format!(
                "{backend}: shape only — not representative (LocalStack, not AWS); \
                 magnitudes are not published"
            )),
            Presence::Absolute(_) => {}
        }
    }
    notes
}

/// The absolute y-range, taken only from series that are safe to publish.
/// `ShapeOnly` series never widen it — that is what keeps a non-representative
/// magnitude out of the axis numbers.
fn absolute_range(presences: &BTreeMap<String, Presence>) -> Option<(f64, f64)> {
    let mut peak = f64::MIN;
    let mut seen = false;
    for presence in presences.values() {
        if let Presence::Absolute(points) = presence {
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
    let style: TextStyle<'_> = (FONT, LABEL_PX)
        .into_font()
        .color(&INK)
        .pos(Pos::new(HPos::Center, VPos::Top));
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
const HEADLINE_PAYLOAD: u64 = 1024;
const OVERHEAD_PAYLOAD: u64 = 64;
const BASE_CONSUMERS: u32 = 1;

fn render_throughput_vs_consumers(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    // The union of consumer counts measured anywhere in the slice, sorted, so
    // the axis is the same regardless of which backend is read first.
    let mut counts: BTreeSet<u32> = BTreeSet::new();
    for run in &doc.runs {
        for r in &run.results {
            if r.flow == HEADLINE_FLOW && r.payload_bytes == HEADLINE_PAYLOAD {
                counts.insert(r.consumers);
            }
        }
    }
    let counts: Vec<u32> = counts.into_iter().collect();
    if counts.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "throughput-vs-consumers",
            slice: format!("{HEADLINE_FLOW} @ {}", fmt_payload(HEADLINE_PAYLOAD)),
        });
    }

    let idx: BTreeMap<u32, f64> = counts
        .iter()
        .enumerate()
        .map(|(i, c)| (*c, i as f64))
        .collect();

    let presences = presences(doc, &[HEADLINE_FLOW], |run| {
        let mut pts: Vec<(f64, f64)> = run
            .results
            .iter()
            .filter(|r| r.flow == HEADLINE_FLOW && r.payload_bytes == HEADLINE_PAYLOAD)
            .filter_map(|r| {
                idx.get(&r.consumers)
                    .map(|x| (*x, r.throughput_msg_per_sec))
            })
            .collect();
        pts.sort_by(|a, b| a.0.total_cmp(&b.0));
        pts
    });

    let labels: Vec<String> = counts.iter().map(|c| format!("{c}")).collect();
    line_chart(
        doc,
        root,
        Family::ThroughputVsConsumers.title(),
        &format!(
            "{} — {} payload — higher is better",
            HEADLINE_FLOW,
            fmt_payload(HEADLINE_PAYLOAD)
        ),
        "messages / second",
        &labels,
        "consumers",
        &presences,
    )
}

// ── Family 2: throughput vs payload size ────────────────────────────────────

fn render_throughput_vs_payload(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    let mut sizes: BTreeSet<u64> = BTreeSet::new();
    for run in &doc.runs {
        for r in &run.results {
            if r.flow == HEADLINE_FLOW && r.consumers == BASE_CONSUMERS {
                sizes.insert(r.payload_bytes);
            }
        }
    }
    let sizes: Vec<u64> = sizes.into_iter().collect();
    if sizes.is_empty() {
        return Err(ChartError::NoDataForChart {
            family: "throughput-vs-payload",
            slice: format!("{HEADLINE_FLOW} @ {BASE_CONSUMERS} consumer"),
        });
    }
    let idx: BTreeMap<u64, f64> = sizes
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, i as f64))
        .collect();

    let presences = presences(doc, &[HEADLINE_FLOW], |run| {
        let mut pts: Vec<(f64, f64)> = run
            .results
            .iter()
            .filter(|r| r.flow == HEADLINE_FLOW && r.consumers == BASE_CONSUMERS)
            .filter_map(|r| {
                idx.get(&r.payload_bytes)
                    .map(|x| (*x, r.throughput_msg_per_sec))
            })
            .collect();
        pts.sort_by(|a, b| a.0.total_cmp(&b.0));
        pts
    });

    let labels: Vec<String> = sizes.iter().map(|s| fmt_payload(*s)).collect();
    line_chart(
        doc,
        root,
        Family::ThroughputVsPayload.title(),
        &format!("{HEADLINE_FLOW} — {BASE_CONSUMERS} consumer — higher is better"),
        "messages / second",
        &labels,
        "payload size",
        &presences,
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
) -> Result<(), ChartError> {
    let render = |e: DrawingAreaErrorKind<std::io::Error>| ChartError::Render(e.to_string());
    let notes = notes_for(presences);
    let area = frame(root, doc, title, subtitle, &notes)?;

    // Absolute magnitudes come only from representative runs. A shape-only
    // series is drawn against the same box but its values are fractions, so it
    // can never be read off the axis.
    let (y_lo, y_hi) = absolute_range(presences).unwrap_or((0.0, 1.0));
    let absolute_axis = absolute_range(presences).is_some();

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
            Presence::Absolute(points) => {
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
            Presence::Unsupported(_) | Presence::NotMeasured => {}
        }
    }

    draw_categories(root, &chart, x_labels)?;
    let x_style: TextStyle<'_> = (FONT, LABEL_PX)
        .into_font()
        .color(&INK)
        .pos(Pos::new(HPos::Center, VPos::Top));
    let area_px = chart.plotting_area().get_pixel_range();
    let (x0, x1) = (area_px.0.start, area_px.0.end);
    let y1 = area_px.1.end;
    root.draw_text(x_desc, &x_style, ((x0 + x1) / 2, y1 + 28))
        .map_err(render)?;
    draw_legend(root, &legend, (94, 74))?;
    Ok(())
}

// ── Grouped bar charts (families 3, 4 and 5) ────────────────────────────────

/// One x-axis group. `bars` is positional against the chart's series list;
/// `None` means "this backend cannot do this at all" and is drawn as an
/// explicit marker, never as a zero-height bar.
#[derive(Debug, Clone)]
struct BarGroup {
    label: String,
    bars: Vec<Option<f64>>,
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
    // magnitude can never be read off the axis.
    let mut peak = 0.0f64;
    for g in groups.iter().filter(|g| !g.shape_only) {
        for v in g.bars.iter().flatten() {
            if *v > peak {
                peak = *v;
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
        let local_peak = group.bars.iter().flatten().fold(0.0f64, |a, v| a.max(*v));
        for (si, (_, colour)) in series.iter().enumerate() {
            let left = gi as f64 - slot / 2.0 + width * si as f64;
            let right = left + width * 0.86;
            let centre = (left + right) / 2.0;
            match group.bars.get(si).copied().flatten() {
                Some(value) => {
                    let height = if group.shape_only {
                        if local_peak > 0.0 {
                            y_hi * 0.92 * (value / local_peak)
                        } else {
                            0.0
                        }
                    } else {
                        value
                    };
                    chart
                        .draw_series(std::iter::once(Rectangle::new(
                            [(left, 0.0), (right, height)],
                            ShapeStyle {
                                color: if group.shape_only {
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
    let centred: TextStyle<'_> = (FONT, LABEL_PX)
        .into_font()
        .color(&INK)
        .pos(Pos::new(HPos::Center, VPos::Top));
    let px = chart.plotting_area().get_pixel_range();
    let (x0, x1) = (px.0.start, px.0.end);
    let y1 = px.1.end;
    root.draw_text(x_desc, &centred, ((x0 + x1) / 2, y1 + 28))
        .map_err(render)?;
    draw_legend(root, series, (94, 74))?;
    Ok(())
}

// ── Family 3: parallel vs sequenced ─────────────────────────────────────────

/// The consume flows, as a lookup against the schema's closed set. Membership
/// is decided here; which *bar* a row lands in is decided by its `mode` field,
/// which exists so a flow name never has to be parsed for that.
const CONSUME_FLOWS: &[&str] = &[
    "consume_parallel",
    "consume_fifo",
    "consume_batch",
    "consumer_group",
];

const MODES: &[(&str, &str)] = &[
    ("parallel", "parallel"),
    ("fifo", "sequenced (fifo)"),
    ("batch", "batch"),
];

fn render_parallel_vs_sequenced(
    doc: &Document,
    root: &DrawingArea<SVGBackend<'_>, Shift>,
) -> Result<(), ChartError> {
    let palette = [
        RGBColor(0x5B, 0x8F, 0xF9),
        RGBColor(0xE8, 0x87, 0x3A),
        RGBColor(0x3F, 0xA6, 0x5C),
    ];
    let series: Vec<(String, RGBColor)> = MODES
        .iter()
        .enumerate()
        .map(|(i, (_, label))| ((*label).to_string(), *palette.get(i).unwrap_or(&palette[0])))
        .collect();

    let mut groups = Vec::new();
    let mut notes = Vec::new();
    for run in &doc.runs {
        let bars: Vec<Option<f64>> = MODES
            .iter()
            .map(|(mode, _)| {
                run.results
                    .iter()
                    .filter(|r| {
                        r.mode == *mode
                            && CONSUME_FLOWS.contains(&r.flow.as_str())
                            && r.payload_bytes == HEADLINE_PAYLOAD
                            && r.consumers == BASE_CONSUMERS
                    })
                    .map(|r| r.throughput_msg_per_sec)
                    .fold(None::<f64>, |acc, v| Some(acc.map_or(v, |a| a.max(v))))
            })
            .collect();

        if bars.iter().all(Option::is_none) {
            notes.push(format!(
                "{}: not supported — no consume measurement for this slice",
                run.backend
            ));
            continue;
        }
        if !run.representative {
            notes.push(format!(
                "{}: shape only — not representative (LocalStack, not AWS); \
                 magnitudes are not published",
                run.backend
            ));
        }
        // Explain each missing bar rather than letting the `n/s` marker stand
        // on its own.
        for ((mode, label), bar) in MODES.iter().zip(bars.iter()) {
            if bar.is_none() {
                let reason = run
                    .unsupported
                    .iter()
                    .find(|u| CONSUME_FLOWS.contains(&u.flow.as_str()) && u.flow.contains(mode))
                    .map(|u| u.reason.clone())
                    .unwrap_or_else(|| {
                        format!("no {label} consume flow measured for this backend")
                    });
                notes.push(format!(
                    "{} / {label}: not supported — {reason}",
                    run.backend
                ));
            }
        }
        groups.push(BarGroup {
            label: run.backend.clone(),
            bars,
            shape_only: !run.representative,
        });
    }

    bar_chart(
        doc,
        root,
        Family::ParallelVsSequenced.title(),
        &format!(
            "{} payload — {BASE_CONSUMERS} consumer — what ordering costs — higher is better",
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
        ("p50".to_string(), RGBColor(0x5B, 0x8F, 0xF9)),
        ("p95".to_string(), RGBColor(0xE8, 0x87, 0x3A)),
        ("p99".to_string(), RGBColor(0xC7, 0x5C, 0x5C)),
    ];

    let mut groups = Vec::new();
    let mut notes = Vec::new();
    for run in &doc.runs {
        let row = run.results.iter().find(|r| {
            r.flow == HEADLINE_FLOW
                && r.payload_bytes == HEADLINE_PAYLOAD
                && r.consumers == BASE_CONSUMERS
        });
        match row {
            Some(r) => {
                if !run.representative {
                    notes.push(format!(
                        "{}: shape only — not representative (LocalStack, not AWS); \
                         magnitudes are not published",
                        run.backend
                    ));
                }
                groups.push(BarGroup {
                    label: run.backend.clone(),
                    bars: vec![
                        Some(r.dispatch_p50_ms),
                        Some(r.dispatch_p95_ms),
                        Some(r.dispatch_p99_ms),
                    ],
                    shape_only: !run.representative,
                });
            }
            None => {
                let reason = unsupported_reason(run, HEADLINE_FLOW).unwrap_or_else(|| {
                    format!("no {HEADLINE_FLOW} measurement for this slice in this run")
                });
                notes.push(format!("{}: not supported — {reason}", run.backend));
            }
        }
    }

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
        RGBColor(0x5B, 0x8F, 0xF9),
    )];

    // Every flow the schema knows about gets a column: the ones measured carry
    // a bar, the ones declared unsupported carry the `n/s` marker. A flow that
    // is neither is not invented.
    let mut groups = Vec::new();
    let mut notes = Vec::new();
    let mut unmeasured: Vec<&str> = Vec::new();
    for flow in KNOWN_FLOWS {
        let measured = run
            .results
            .iter()
            .filter(|r| {
                r.flow == *flow
                    && r.payload_bytes == OVERHEAD_PAYLOAD
                    && r.consumers == BASE_CONSUMERS
                    && r.throughput_msg_per_sec > 0.0
            })
            .map(|r| 1e9 / r.throughput_msg_per_sec)
            // Best observed cost per message: the floor is the framework's own
            // cost, anything above it is scheduling noise.
            .fold(None::<f64>, |acc, v| Some(acc.map_or(v, |a: f64| a.min(v))));

        match measured {
            Some(ns) => groups.push(BarGroup {
                label: (*flow).to_string(),
                bars: vec![Some(ns)],
                shape_only: !run.representative,
            }),
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

    if !unmeasured.is_empty() {
        notes.push(format!(
            "not measured in this run (supported, no number here): {}",
            unmeasured.join(", ")
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
