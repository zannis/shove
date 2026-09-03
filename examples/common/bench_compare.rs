//! Tier A benchmark regression comparison against a committed baseline.
//!
//! This module is the library half of `examples/bench_compare.rs`; the
//! example is a thin CLI over it. It is pulled in with `#[path]` from two
//! targets — the example, and `tests/bench_compare.rs`, which holds the test
//! functions. Cargo defaults `[[example]]` targets to `test = false`, so
//! tests living in the example target would never execute.
//!
//! ## What it reads
//!
//! - A criterion output tree: every `<id dirs…>/new/estimates.json` under the
//!   given directory, taking `mean.point_estimate` (nanoseconds). Ids are the
//!   directory components relative to the root — criterion writes
//!   `codec/json_decode/64` as three nested directories, un-parameterised ids
//!   as two — so discovery walks recursively rather than assuming a depth,
//!   skips `report/` (criterion's HTML), and requires the file to exist: a
//!   directory skeleton with no `estimates.json` (what Swatinem/rust-cache
//!   leaves behind after pruning plain files) is not a measurement.
//! - A committed baseline document: per-id mean and an explicit `gate` flag.
//!   Gate membership is earned, not asserted — an id carries `gate: true`
//!   only after proving < 10% spread across three no-op runs on the runner
//!   that judges it. The provenance block records how and when.
//!
//! ## The failure matrix
//!
//! | Situation | Verdict |
//! |---|---|
//! | gated id > 50% slower than baseline | **fail** |
//! | id in baseline, missing from run | **fail** — a renamed/deleted bench must refresh the baseline in the same PR, and a bench target silently dropped (wrong feature set) fails closed |
//! | any id > 25% slower | warning annotation |
//! | id in run, not in baseline | warn — a new bench enters the baseline at the next refresh |
//! | > 25% faster | printed as a baseline-refresh candidate, never a failure |
//! | zero / negative / non-finite mean anywhere | **error** — corrupt data must not produce a verdict |
//! | no `estimates.json` found at all | **error** — a vacuous run must not pass by comparing nothing |

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// A gated id fails when it is strictly more than this much slower than its
/// baseline mean. 50% is ~5× the < 10% no-op spread an id must demonstrate to
/// earn `gate: true` in the first place.
pub const GATE_FAIL_RATIO: f64 = 0.50;

/// Any id strictly more than this much slower gets a warning annotation
/// (and this much faster is flagged as a baseline-refresh candidate).
pub const WARN_RATIO: f64 = 0.25;

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum CompareError {
    Io(PathBuf, std::io::Error),
    Json(PathBuf, serde_json::Error),
    /// The baseline document declares a schema version this comparator does
    /// not understand.
    SchemaVersion(u32),
    /// A mean that is zero, negative or non-finite cannot produce a
    /// trustworthy percentage.
    CorruptMean {
        id: String,
        mean_ns: f64,
    },
    /// The walk found no `new/estimates.json` anywhere: the bench step
    /// measured nothing, which must fail loudly rather than compare nothing.
    NoEstimates(PathBuf),
}

impl fmt::Display for CompareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(path, e) => write!(f, "{}: {e}", path.display()),
            Self::Json(path, e) => write!(f, "{}: {e}", path.display()),
            Self::SchemaVersion(v) => {
                write!(
                    f,
                    "baseline schema_version {v} is not supported (expected 1)"
                )
            }
            Self::CorruptMean { id, mean_ns } => {
                write!(f, "{id}: mean {mean_ns} ns is not a positive finite number")
            }
            Self::NoEstimates(path) => write!(
                f,
                "no new/estimates.json found under {} — the bench step measured nothing",
                path.display()
            ),
        }
    }
}

// ── Baseline document ───────────────────────────────────────────────────────

pub const BASELINE_SCHEMA_VERSION: u32 = 1;

/// One baseline row. `deny_unknown_fields` is load-bearing: a misspelled
/// `gate` must refuse to parse, not silently deserialize into an id that can
/// never fail.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineEntry {
    pub mean_ns: f64,
    pub gate: bool,
}

/// Where the baseline numbers came from — kept so a stale baseline is visibly
/// stale in review rather than silently trusted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub runner: String,
    pub calibrated: String,
    pub shove_version: String,
    pub rust_version: String,
    pub method: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineDoc {
    schema_version: u32,
    provenance: Provenance,
    ids: BTreeMap<String, BaselineEntry>,
}

/// The comparator's view of the baseline: ids with their gate flags.
#[derive(Debug)]
pub struct Baseline {
    pub ids: BTreeMap<String, BaselineEntry>,
}

pub fn load_baseline(path: &Path) -> Result<Baseline, CompareError> {
    let raw = fs::read_to_string(path).map_err(|e| CompareError::Io(path.to_path_buf(), e))?;
    let doc: BaselineDoc =
        serde_json::from_str(&raw).map_err(|e| CompareError::Json(path.to_path_buf(), e))?;
    if doc.schema_version != BASELINE_SCHEMA_VERSION {
        return Err(CompareError::SchemaVersion(doc.schema_version));
    }
    Ok(Baseline { ids: doc.ids })
}

// ── Criterion output ────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Estimates {
    mean: PointEstimate,
}

#[derive(Deserialize)]
struct PointEstimate {
    point_estimate: f64,
}

fn positive_finite(id: &str, mean_ns: f64) -> Result<f64, CompareError> {
    if mean_ns.is_finite() && mean_ns > 0.0 {
        Ok(mean_ns)
    } else {
        Err(CompareError::CorruptMean {
            id: id.to_string(),
            mean_ns,
        })
    }
}

/// Walk a criterion output directory and return `id -> mean ns` for every id
/// with a `new/estimates.json`.
pub fn collect_run(criterion_dir: &Path) -> Result<BTreeMap<String, f64>, CompareError> {
    let mut out = BTreeMap::new();
    let mut stack = vec![criterion_dir.to_path_buf()];
    // An unreadable root surfaces as the Io error from the first read_dir.
    while let Some(dir) = stack.pop() {
        let estimates = dir.join("new").join("estimates.json");
        if estimates.is_file() {
            let id = dir
                .strip_prefix(criterion_dir)
                .ok()
                .and_then(|rel| {
                    let parts: Vec<&str> = rel
                        .components()
                        .map(|c| c.as_os_str().to_str())
                        .collect::<Option<_>>()?;
                    Some(parts.join("/"))
                })
                .unwrap_or_else(|| dir.display().to_string());
            let raw = fs::read_to_string(&estimates)
                .map_err(|e| CompareError::Io(estimates.clone(), e))?;
            let parsed: Estimates =
                serde_json::from_str(&raw).map_err(|e| CompareError::Json(estimates.clone(), e))?;
            let mean = positive_finite(&id, parsed.mean.point_estimate)?;
            out.insert(id, mean);
            // An id's directory holds new/, base/, report/ — never another id.
            continue;
        }
        let entries = fs::read_dir(&dir).map_err(|e| CompareError::Io(dir.clone(), e))?;
        for entry in entries {
            let entry = entry.map_err(|e| CompareError::Io(dir.clone(), e))?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            // `report/` is criterion's HTML output; `new/`/`base/`/`change/`
            // are measurement slots, not id components.
            if matches!(
                entry.file_name().to_str(),
                Some("report" | "new" | "base" | "change")
            ) {
                continue;
            }
            stack.push(path);
        }
    }
    if out.is_empty() {
        return Err(CompareError::NoEstimates(criterion_dir.to_path_buf()));
    }
    Ok(out)
}

// ── Comparison ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Within the noise band either way.
    Ok,
    /// More than [`WARN_RATIO`] faster: a baseline-refresh candidate.
    Faster,
    /// More than [`WARN_RATIO`] slower (or a gated id between the warn and
    /// fail thresholds): annotated, never fatal on its own.
    Slow,
    /// A gated id more than [`GATE_FAIL_RATIO`] slower: fails the leg.
    GateFail,
    /// In the baseline, absent from the run: fails the leg.
    MissingFromRun,
    /// In the run, absent from the baseline: announced, enters the baseline
    /// at the next refresh.
    NewInRun,
}

impl Verdict {
    fn label(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Faster => "faster (refresh candidate)",
            Self::Slow => "SLOW",
            Self::GateFail => "GATE FAIL",
            Self::MissingFromRun => "MISSING FROM RUN",
            Self::NewInRun => "new (not in baseline)",
        }
    }
}

#[derive(Debug)]
pub struct Row {
    pub id: String,
    pub baseline_ns: Option<f64>,
    pub run_ns: Option<f64>,
    /// Percentage the run is slower (+) or faster (−); absent when either
    /// side is missing.
    pub delta_pct: Option<f64>,
    pub gate: bool,
    pub verdict: Verdict,
}

#[derive(Debug)]
pub struct Comparison {
    pub rows: Vec<Row>,
}

impl Comparison {
    pub fn failed(&self) -> bool {
        self.rows
            .iter()
            .any(|r| matches!(r.verdict, Verdict::GateFail | Verdict::MissingFromRun))
    }
}

pub fn compare(
    baseline: &Baseline,
    run: &BTreeMap<String, f64>,
) -> Result<Comparison, CompareError> {
    for (id, mean) in run {
        positive_finite(id, *mean)?;
    }
    let mut rows = Vec::with_capacity(baseline.ids.len());
    for (id, entry) in &baseline.ids {
        let base = positive_finite(id, entry.mean_ns)?;
        match run.get(id) {
            None => rows.push(Row {
                id: id.clone(),
                baseline_ns: Some(base),
                run_ns: None,
                delta_pct: None,
                gate: entry.gate,
                verdict: Verdict::MissingFromRun,
            }),
            Some(&measured) => {
                let ratio = measured / base - 1.0;
                let verdict = if entry.gate && ratio > GATE_FAIL_RATIO {
                    Verdict::GateFail
                } else if ratio > WARN_RATIO {
                    Verdict::Slow
                } else if ratio < -WARN_RATIO {
                    Verdict::Faster
                } else {
                    Verdict::Ok
                };
                rows.push(Row {
                    id: id.clone(),
                    baseline_ns: Some(base),
                    run_ns: Some(measured),
                    delta_pct: Some(ratio * 100.0),
                    gate: entry.gate,
                    verdict,
                });
            }
        }
    }
    for (id, &measured) in run {
        if !baseline.ids.contains_key(id) {
            rows.push(Row {
                id: id.clone(),
                baseline_ns: None,
                run_ns: Some(measured),
                delta_pct: None,
                gate: false,
                verdict: Verdict::NewInRun,
            });
        }
    }
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(Comparison { rows })
}

// ── Reporting ───────────────────────────────────────────────────────────────

fn fmt_ns(v: Option<f64>) -> String {
    match v {
        Some(ns) => format!("{ns:>14.1}"),
        None => format!("{:>14}", "-"),
    }
}

/// Render the comparison as a fixed-width table, one row per id. With
/// `annotate`, failures and warnings additionally emit GitHub Actions
/// `::error::` / `::warning::` lines so they surface on the PR without
/// opening the log.
pub fn render_report(cmp: &Comparison, annotate: bool) -> String {
    let mut out = String::new();
    let width = cmp
        .rows
        .iter()
        .map(|r| r.id.len())
        .max()
        .unwrap_or(0)
        .max(2);
    out.push_str(&format!(
        "{:<width$} {:>14} {:>14} {:>9} {:>5}  verdict\n",
        "id", "baseline ns", "run ns", "delta", "gate",
    ));
    for row in &cmp.rows {
        let delta = match row.delta_pct {
            Some(d) => format!("{d:>+8.2}%"),
            None => format!("{:>9}", "-"),
        };
        out.push_str(&format!(
            "{:<width$} {} {} {delta} {:>5}  {}\n",
            row.id,
            fmt_ns(row.baseline_ns),
            fmt_ns(row.run_ns),
            if row.gate { "yes" } else { "no" },
            row.verdict.label(),
        ));
    }
    if annotate {
        for row in &cmp.rows {
            match row.verdict {
                Verdict::GateFail => out.push_str(&format!(
                    "::error::bench {} regressed {} (gated; fails at >{}%)\n",
                    row.id,
                    row.delta_pct
                        .map_or_else(String::new, |d| format!("{d:+.2}%")),
                    GATE_FAIL_RATIO * 100.0,
                )),
                Verdict::MissingFromRun => out.push_str(&format!(
                    "::error::bench {} is in the baseline but was not measured — \
                     renamed or deleted benches must refresh the baseline in the same PR\n",
                    row.id,
                )),
                Verdict::Slow => out.push_str(&format!(
                    "::warning::bench {} is {} slower than baseline\n",
                    row.id,
                    row.delta_pct
                        .map_or_else(String::new, |d| format!("{d:+.2}%")),
                )),
                _ => {}
            }
        }
    }
    out
}

/// Render a run with no baseline: the calibration view, one stable line per
/// id, harvestable from a CI log.
pub fn render_run(run: &BTreeMap<String, f64>) -> String {
    let mut out = String::new();
    for (id, mean) in run {
        out.push_str(&format!("CALIB {id} {mean}\n"));
    }
    out
}
