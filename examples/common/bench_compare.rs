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
//! - A committed baseline document: per-id mean and per-id failure threshold.
//!   Thresholds are earned, not asserted: calibration runs the suite with no
//!   code change on separate runners of the judging fleet and sets each id's
//!   `fail_above_pct` to `max(50, 5 × its measured no-op spread)`. Stable ids
//!   keep real 50% teeth; ids the fleet's hardware diversity swings 2× can
//!   still catch a catastrophic blowup, and none of them can cry wolf — a
//!   no-op repeat of the slowest calibration runner lands at
//!   `2s / (3 + s)` above the mean for spread `s`, provably inside
//!   `max(0.5, 5s)` for every `s`. The provenance block records how and when.
//!
//! ## The failure matrix
//!
//! | Situation | Verdict |
//! |---|---|
//! | id slower than baseline mean by more than its `fail_above_pct` | **fail** |
//! | id in baseline, missing from run | **fail** — a renamed/deleted bench must refresh the baseline in the same PR, and a bench target silently dropped (wrong feature set) fails closed |
//! | id slower by more than half its `fail_above_pct` | warning annotation |
//! | id in run, not in baseline | warn — a new bench enters the baseline at the next refresh |
//! | > 25% faster | printed as a baseline-refresh candidate, never a failure |
//! | zero / negative / non-finite mean, or a threshold below the floor | **error** — corrupt data must not produce a verdict |
//! | no `estimates.json` found at all | **error** — a vacuous run must not pass by comparing nothing |

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// No id may fail below this, whatever its measured spread: it is ~5× the
/// tightest spread the calibration fleet has demonstrated, and keeps every
/// failure threshold clear of its own warn band (half the threshold).
pub const FAIL_FLOOR_PCT: f64 = 50.0;

/// The warn band is per-id: **half the id's failure threshold**, so an id at
/// the 50% floor warns above 25% and an id calibrated to 300% warns above
/// 150%. A fixed 25% band would fire on runner jitter for most of the suite
/// (measured no-op median spread: 57%) and train everyone to ignore the
/// annotations. This constant is only the *faster* band: more than 25%
/// faster is flagged as a baseline-refresh candidate.
pub const FASTER_RATIO: f64 = 0.25;

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
    /// A failure threshold below [`FAIL_FLOOR_PCT`] (or non-finite) would let
    /// runner jitter fail the leg — refuse the baseline rather than judge
    /// with it.
    BadThreshold {
        id: String,
        fail_above_pct: f64,
    },
    /// The walk found no `new/estimates.json` anywhere: the bench step
    /// measured nothing, which must fail loudly rather than compare nothing.
    NoEstimates(PathBuf),
    /// Calibration inputs whose id sets differ: a run that silently dropped
    /// an id (wrong feature set, renamed bench) must not shrink the baseline
    /// to the intersection.
    CalibrationIdMismatch {
        id: String,
    },
    /// Calibration from fewer than two runs: one run has zero spread by
    /// construction, so every id would get the floor threshold off data that
    /// demonstrated nothing.
    CalibrationTooFewRuns(usize),
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
            Self::BadThreshold { id, fail_above_pct } => write!(
                f,
                "{id}: fail_above_pct {fail_above_pct} is below the {FAIL_FLOOR_PCT}% floor \
                 (or not finite) — a threshold inside runner jitter would cry wolf"
            ),
            Self::NoEstimates(path) => write!(
                f,
                "no new/estimates.json found under {} — the bench step measured nothing",
                path.display()
            ),
            Self::CalibrationIdMismatch { id } => write!(
                f,
                "{id} is not present in every calibration run — a run that dropped \
                 an id must not shrink the baseline to the intersection"
            ),
            Self::CalibrationTooFewRuns(n) => write!(
                f,
                "calibration needs at least 2 runs, got {n} — one run has zero \
                 spread by construction"
            ),
        }
    }
}

// ── Baseline document ───────────────────────────────────────────────────────

pub const BASELINE_SCHEMA_VERSION: u32 = 1;

/// One baseline row. `deny_unknown_fields` is load-bearing: a misspelled
/// `fail_above_pct` must refuse to parse, not silently deserialize into an id
/// judged by a default.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineEntry {
    pub mean_ns: f64,
    /// The id fails when the run mean is strictly more than this percentage
    /// above `mean_ns`. Set by calibration to `max(50, 5 × measured spread)`.
    pub fail_above_pct: f64,
}

/// Where the baseline numbers came from — kept so a stale baseline is visibly
/// stale in review rather than silently trusted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub runner: String,
    pub calibrated: String,
    pub shove_version: String,
    pub rust_version: String,
    pub method: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BaselineDoc {
    schema_version: u32,
    provenance: Provenance,
    ids: BTreeMap<String, BaselineEntry>,
}

/// The comparator's view of the baseline: ids with their thresholds.
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
    for (id, entry) in &doc.ids {
        if !(entry.fail_above_pct.is_finite() && entry.fail_above_pct >= FAIL_FLOOR_PCT) {
            return Err(CompareError::BadThreshold {
                id: id.clone(),
                fail_above_pct: entry.fail_above_pct,
            });
        }
    }
    Ok(Baseline { ids: doc.ids })
}

// ── Calibration: how thresholds are earned ──────────────────────────────────

/// Build baseline entries from two or more no-op runs: per id, the mean is
/// the arithmetic mean across runs and the threshold is
/// `max(`[`FAIL_FLOOR_PCT`]`, 5 × max-vs-min spread)`. The runs must share
/// one id set — a run that silently dropped a bench target must not shrink
/// the baseline to the intersection.
pub fn calibrate(
    runs: &[BTreeMap<String, f64>],
) -> Result<BTreeMap<String, BaselineEntry>, CompareError> {
    if runs.len() < 2 {
        return Err(CompareError::CalibrationTooFewRuns(runs.len()));
    }
    for run in runs {
        for id in runs.iter().flat_map(|r| r.keys()) {
            if !run.contains_key(id) {
                return Err(CompareError::CalibrationIdMismatch { id: id.clone() });
            }
        }
        for (id, mean) in run {
            positive_finite(id, *mean)?;
        }
    }
    let mut out = BTreeMap::new();
    for id in runs[0].keys() {
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut sum = 0.0;
        for run in runs {
            let v = run[id];
            min = min.min(v);
            max = max.max(v);
            sum += v;
        }
        let spread = (max - min) / min;
        out.insert(
            id.clone(),
            BaselineEntry {
                mean_ns: (sum / runs.len() as f64 * 100.0).round() / 100.0,
                fail_above_pct: (5.0 * spread * 100.0).round().max(FAIL_FLOOR_PCT),
            },
        );
    }
    Ok(out)
}

/// Write a baseline document the loader will accept back.
pub fn write_baseline(
    path: &Path,
    ids: &BTreeMap<String, BaselineEntry>,
    provenance: Provenance,
) -> Result<(), CompareError> {
    let doc = BaselineDoc {
        schema_version: BASELINE_SCHEMA_VERSION,
        provenance,
        ids: ids
            .iter()
            .map(|(id, e)| {
                (
                    id.clone(),
                    BaselineEntry {
                        mean_ns: e.mean_ns,
                        fail_above_pct: e.fail_above_pct,
                    },
                )
            })
            .collect(),
    };
    let mut raw = serde_json::to_string_pretty(&doc)
        .map_err(|e| CompareError::Json(path.to_path_buf(), e))?;
    raw.push('\n');
    fs::write(path, raw).map_err(|e| CompareError::Io(path.to_path_buf(), e))
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
    /// More than [`FASTER_RATIO`] faster: a baseline-refresh candidate.
    Faster,
    /// Slower by more than half the id's failure threshold but inside it:
    /// annotated, never fatal on its own.
    Slow,
    /// Slower than the id's `fail_above_pct`: fails the leg.
    Regressed,
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
            Self::Regressed => "REGRESSED",
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
    /// The id's own failure threshold; absent for ids not in the baseline.
    pub fail_above_pct: Option<f64>,
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
            .any(|r| matches!(r.verdict, Verdict::Regressed | Verdict::MissingFromRun))
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
                fail_above_pct: Some(entry.fail_above_pct),
                verdict: Verdict::MissingFromRun,
            }),
            Some(&measured) => {
                let ratio = measured / base - 1.0;
                let verdict = if ratio > entry.fail_above_pct / 100.0 {
                    Verdict::Regressed
                } else if ratio > entry.fail_above_pct / 200.0 {
                    Verdict::Slow
                } else if ratio < -FASTER_RATIO {
                    Verdict::Faster
                } else {
                    Verdict::Ok
                };
                rows.push(Row {
                    id: id.clone(),
                    baseline_ns: Some(base),
                    run_ns: Some(measured),
                    delta_pct: Some(ratio * 100.0),
                    fail_above_pct: Some(entry.fail_above_pct),
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
                fail_above_pct: None,
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
        "{:<width$} {:>14} {:>14} {:>9} {:>8}  verdict\n",
        "id", "baseline ns", "run ns", "delta", "fail at",
    ));
    for row in &cmp.rows {
        let delta = match row.delta_pct {
            Some(d) => format!("{d:>+8.2}%"),
            None => format!("{:>9}", "-"),
        };
        let fail_at = match row.fail_above_pct {
            Some(t) => format!("{t:>+7.0}%"),
            None => format!("{:>8}", "-"),
        };
        out.push_str(&format!(
            "{:<width$} {} {} {delta} {fail_at}  {}\n",
            row.id,
            fmt_ns(row.baseline_ns),
            fmt_ns(row.run_ns),
            row.verdict.label(),
        ));
    }
    // Say what the gate actually covers, so a fully green run cannot be read
    // as "58 ids held to 50%": ids the fleet swings hard get thresholds that
    // only catch catastrophic blowups, and that is a property of the runner
    // pool, not of the code under test.
    let with_threshold: Vec<f64> = cmp.rows.iter().filter_map(|r| r.fail_above_pct).collect();
    let floor = with_threshold
        .iter()
        .filter(|t| **t <= FAIL_FLOOR_PCT)
        .count();
    let catastrophe = with_threshold.iter().filter(|t| **t > 200.0).count();
    out.push_str(&format!(
        "coverage: {} baseline ids — {} gated at the {}% floor, {} at >200% (catastrophe-only), {} in between\n",
        with_threshold.len(),
        floor,
        FAIL_FLOOR_PCT,
        catastrophe,
        with_threshold.len() - floor - catastrophe,
    ));
    if annotate {
        for row in &cmp.rows {
            match row.verdict {
                Verdict::Regressed => out.push_str(&format!(
                    "::error::bench {} regressed {} (fails above {}%)\n",
                    row.id,
                    row.delta_pct
                        .map_or_else(String::new, |d| format!("{d:+.2}%")),
                    row.fail_above_pct.unwrap_or(FAIL_FLOOR_PCT),
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
