//! Tests for the Tier A benchmark regression comparator.
//!
//! The comparator lives at `examples/common/bench_compare.rs` and is pulled
//! into `examples/bench_compare.rs` with `#[path]`. Cargo defaults
//! `[[example]]` targets to `test = false`, so a `#[cfg(test)]` module inside
//! the example would never be compiled into a test binary. Including it here —
//! in a real integration-test target — is what makes these run.
//!
//! Deliberately **not** feature-gated, for the same reason as
//! `tests/chartgen.rs`: the comparator needs no backend and runs in CI under
//! `--no-default-features`; gating this file would mean the gate logic goes
//! untested in exactly the configuration CI uses.

#[path = "../examples/common/bench_compare.rs"]
mod bench_compare;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use bench_compare::{
    Baseline, BaselineEntry, CompareError, Verdict, collect_run, compare, load_baseline,
};

// ── Fixtures ────────────────────────────────────────────────────────────────

/// A fresh directory under the cargo-managed test tmpdir. Unique per test so
/// parallel tests never see each other's trees.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("bench_compare")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).expect("clear stale scratch");
    }
    fs::create_dir_all(&dir).expect("create scratch");
    dir
}

/// Write a criterion `estimates.json` carrying the given mean, at
/// `<root>/<id components...>/<slot>/estimates.json`.
fn write_estimates(root: &Path, id: &str, slot: &str, mean_ns: f64) {
    let dir = id
        .split('/')
        .fold(root.to_path_buf(), |p, c| p.join(c))
        .join(slot);
    fs::create_dir_all(&dir).expect("create estimates dir");
    // Only `mean.point_estimate` is contract; the rest of the real file is
    // irrelevant to the comparator and deliberately absent here.
    let body = format!(r#"{{"mean":{{"point_estimate":{mean_ns},"standard_error":0.1}}}}"#);
    fs::write(dir.join("estimates.json"), body).expect("write estimates.json");
}

fn baseline_of(ids: &[(&str, f64, f64)]) -> Baseline {
    Baseline {
        ids: ids
            .iter()
            .map(|(id, mean_ns, fail_above_pct)| {
                (
                    (*id).to_string(),
                    BaselineEntry {
                        mean_ns: *mean_ns,
                        fail_above_pct: *fail_above_pct,
                    },
                )
            })
            .collect(),
    }
}

fn run_of(ids: &[(&str, f64)]) -> BTreeMap<String, f64> {
    ids.iter().map(|(id, m)| ((*id).to_string(), *m)).collect()
}

fn verdict_of<'c>(cmp: &'c bench_compare::Comparison, id: &str) -> &'c Verdict {
    &cmp.rows
        .iter()
        .find(|r| r.id == id)
        .unwrap_or_else(|| panic!("no row for {id}"))
        .verdict
}

// ── Run collection: the one filesystem assumption, pinned ───────────────────

#[test]
fn collect_run_finds_two_and_three_deep_ids_and_skips_reports() {
    let root = scratch("collect_basic");
    // Parameterised id: 3 path components.
    write_estimates(&root, "codec/json_decode/64", "new", 130.5);
    // Un-parameterised id: 2 path components.
    write_estimates(&root, "autoscaler_decision/threshold", "new", 9.7);
    // `base/` sits next to `new/` after a `--save-baseline` run; it must not
    // produce a second id.
    write_estimates(&root, "codec/json_decode/64", "base", 999.0);
    // Criterion's HTML report dirs carry no estimates and must not be ids.
    fs::create_dir_all(root.join("report")).expect("mk report");
    fs::create_dir_all(root.join("codec/report")).expect("mk group report");
    fs::write(root.join("report/index.html"), "<html>").expect("write report");
    // rust-cache leaves empty directory skeletons behind: a dir with a `new/`
    // but no estimates.json inside must be ignored, not treated as a run id.
    fs::create_dir_all(root.join("inmemory_publish/publish_single/64/new")).expect("mk skeleton");

    let run = collect_run(&root).expect("collect");
    let ids: Vec<&str> = run.keys().map(String::as_str).collect();
    assert_eq!(
        ids,
        vec!["autoscaler_decision/threshold", "codec/json_decode/64"]
    );
    assert_eq!(run["codec/json_decode/64"], 130.5);
    assert_eq!(run["autoscaler_decision/threshold"], 9.7);
}

#[test]
fn collect_run_with_no_estimates_is_an_error_not_an_empty_pass() {
    // A vacuous run must never feed a comparison that then "passes": zero
    // collected ids means the bench step produced nothing (wrong features,
    // wrong target, moved directory) and the leg has to fail loudly.
    let root = scratch("collect_empty");
    fs::create_dir_all(root.join("report")).expect("mk report");
    match collect_run(&root) {
        Err(CompareError::NoEstimates(_)) => {}
        other => panic!("expected NoEstimates, got {other:?}"),
    }
}

#[test]
fn collect_run_on_a_missing_directory_is_an_error() {
    let root = scratch("collect_missing").join("does_not_exist");
    assert!(collect_run(&root).is_err());
}

#[test]
fn collect_run_rejects_a_corrupt_mean() {
    let root = scratch("collect_corrupt");
    write_estimates(&root, "codec/json_decode/64", "new", -1.0);
    match collect_run(&root) {
        Err(CompareError::CorruptMean { .. }) => {}
        other => panic!("expected CorruptMean, got {other:?}"),
    }
}

// ── Comparison: the failure matrix, row by row ──────────────────────────────

#[test]
fn an_id_exactly_at_its_threshold_does_not_fail() {
    // Failure is *strictly more than* fail_above_pct slower. 100 → 150 at a
    // 50% threshold is exactly on it: a warning, not a failure.
    let cmp = compare(
        &baseline_of(&[("g/id", 100.0, 50.0)]),
        &run_of(&[("g/id", 150.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/id"), Verdict::Slow);
    assert!(!cmp.failed());
}

#[test]
fn an_id_over_its_threshold_fails() {
    let cmp = compare(
        &baseline_of(&[("g/id", 100.0, 50.0)]),
        &run_of(&[("g/id", 150.2)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/id"), Verdict::Regressed);
    assert!(cmp.failed());
}

#[test]
fn thresholds_are_per_id_not_global() {
    // A noisy id calibrated at 300% tolerates what a stable id must not:
    // +200% warns on the noisy id and fails the stable one.
    let cmp = compare(
        &baseline_of(&[("g/noisy", 100.0, 300.0), ("g/stable", 100.0, 50.0)]),
        &run_of(&[("g/noisy", 300.0), ("g/stable", 300.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/noisy"), Verdict::Slow);
    assert_eq!(*verdict_of(&cmp, "g/stable"), Verdict::Regressed);
    assert!(cmp.failed());
}

#[test]
fn a_noisy_id_still_catches_a_catastrophic_blowup() {
    let cmp = compare(
        &baseline_of(&[("g/noisy", 100.0, 300.0)]),
        &run_of(&[("g/noisy", 500.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/noisy"), Verdict::Regressed);
    assert!(cmp.failed());
}

#[test]
fn the_warn_band_is_half_the_failure_threshold() {
    // A fixed 25% warn band would fire on runner jitter for most of the
    // suite (measured no-op median spread: 57%). Warn is per-id at half the
    // failure threshold: a floor id warns strictly above 25%, an id
    // calibrated to 300% warns strictly above 150%.
    let cmp = compare(
        &baseline_of(&[
            ("g/floor_on", 100.0, 50.0),
            ("g/floor_over", 100.0, 50.0),
            ("g/noisy_on", 100.0, 300.0),
            ("g/noisy_over", 100.0, 300.0),
        ]),
        &run_of(&[
            ("g/floor_on", 125.0),
            ("g/floor_over", 125.1),
            ("g/noisy_on", 250.0),
            ("g/noisy_over", 251.0),
        ]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/floor_on"), Verdict::Ok);
    assert_eq!(*verdict_of(&cmp, "g/floor_over"), Verdict::Slow);
    assert_eq!(*verdict_of(&cmp, "g/noisy_on"), Verdict::Ok);
    assert_eq!(*verdict_of(&cmp, "g/noisy_over"), Verdict::Slow);
    assert!(!cmp.failed());
}

#[test]
fn a_clear_speedup_is_flagged_as_a_refresh_candidate_never_a_failure() {
    let cmp = compare(
        &baseline_of(&[("g/id", 100.0, 50.0)]),
        &run_of(&[("g/id", 60.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/id"), Verdict::Faster);
    assert!(!cmp.failed());
}

#[test]
fn an_id_in_the_baseline_but_not_the_run_fails_the_leg() {
    // A renamed or deleted bench must force a baseline refresh in the same
    // PR. This is also what fails closed when the bench step silently drops a
    // whole target (e.g. a missing feature flag).
    let cmp = compare(
        &baseline_of(&[("g/kept", 100.0, 50.0), ("g/gone", 100.0, 50.0)]),
        &run_of(&[("g/kept", 100.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/gone"), Verdict::MissingFromRun);
    assert!(cmp.failed());
}

#[test]
fn an_id_in_the_run_but_not_the_baseline_warns_only() {
    // A PR adding a bench must not be forced to hand-edit the baseline: the
    // new id is announced and enters the baseline at the next refresh.
    let cmp = compare(
        &baseline_of(&[("g/old", 100.0, 50.0)]),
        &run_of(&[("g/old", 100.0), ("g/new", 42.0)]),
    )
    .expect("compare");
    assert_eq!(*verdict_of(&cmp, "g/new"), Verdict::NewInRun);
    assert!(!cmp.failed());
}

#[test]
fn corrupt_means_are_errors_not_verdicts() {
    // Zero, negative and non-finite means cannot produce a trustworthy
    // percentage; the comparison itself must refuse, not emit a verdict.
    for bad in [0.0, -5.0, f64::NAN, f64::INFINITY] {
        let err = compare(
            &baseline_of(&[("g/id", 100.0, 50.0)]),
            &run_of(&[("g/id", bad)]),
        );
        assert!(err.is_err(), "run mean {bad} must be rejected");
        let err = compare(
            &baseline_of(&[("g/id", bad, 50.0)]),
            &run_of(&[("g/id", 100.0)]),
        );
        assert!(err.is_err(), "baseline mean {bad} must be rejected");
    }
}

// ── Baseline document ───────────────────────────────────────────────────────

#[test]
fn a_baseline_round_trips_and_keeps_per_id_thresholds() {
    let root = scratch("baseline_roundtrip");
    let path = root.join("baseline.json");
    fs::write(
        &path,
        r#"{
  "schema_version": 1,
  "provenance": {
    "runner": "ubuntu-latest",
    "calibrated": "2026-09-03",
    "shove_version": "0.14.0",
    "rust_version": "rustc 1.91.1",
    "method": "fail_above_pct = max(50, 5 x measured no-op spread) across 3 runs"
  },
  "ids": {
    "codec/json_decode/1024": { "mean_ns": 1650.2, "fail_above_pct": 272.0 },
    "consumer_overhead_startup/16": { "mean_ns": 2184089.0, "fail_above_pct": 50.0 }
  }
}"#,
    )
    .expect("write baseline");

    let baseline = load_baseline(&path).expect("load");
    assert_eq!(baseline.ids.len(), 2);
    assert_eq!(baseline.ids["codec/json_decode/1024"].fail_above_pct, 272.0);
    assert_eq!(
        baseline.ids["consumer_overhead_startup/16"].fail_above_pct,
        50.0
    );
}

#[test]
fn an_unknown_baseline_schema_version_is_refused() {
    let root = scratch("baseline_version");
    let path = root.join("baseline.json");
    fs::write(
        &path,
        r#"{ "schema_version": 2, "provenance": {"runner":"x","calibrated":"x","shove_version":"x","rust_version":"x","method":"x"}, "ids": {} }"#,
    )
    .expect("write baseline");
    match load_baseline(&path) {
        Err(CompareError::SchemaVersion(2)) => {}
        other => panic!("expected SchemaVersion(2), got {other:?}"),
    }
}

#[test]
fn a_baseline_with_a_misspelled_field_is_refused_not_defaulted() {
    // `"fail_above_pct"` misspelled must not deserialize into "threshold
    // absent → whatever serde would default"; a typo silently loosening an id
    // is exactly the failure mode deny_unknown_fields exists for.
    let root = scratch("baseline_typo");
    let path = root.join("baseline.json");
    fs::write(
        &path,
        r#"{
  "schema_version": 1,
  "provenance": {"runner":"x","calibrated":"x","shove_version":"x","rust_version":"x","method":"x"},
  "ids": { "g/id": { "mean_ns": 1.0, "fail_above_pctt": 50.0 } }
}"#,
    )
    .expect("write baseline");
    assert!(load_baseline(&path).is_err());
}

#[test]
fn a_threshold_below_the_floor_is_refused_at_load() {
    // A committed threshold inside runner jitter (measured no-op median: 57%)
    // would cry wolf; 50% is the floor and the loader is where it is
    // enforced, so a hand-edited baseline cannot smuggle one in.
    let root = scratch("baseline_floor");
    let path = root.join("baseline.json");
    fs::write(
        &path,
        r#"{
  "schema_version": 1,
  "provenance": {"runner":"x","calibrated":"x","shove_version":"x","rust_version":"x","method":"x"},
  "ids": { "g/id": { "mean_ns": 1.0, "fail_above_pct": 10.0 } }
}"#,
    )
    .expect("write baseline");
    match load_baseline(&path) {
        Err(CompareError::BadThreshold { .. }) => {}
        other => panic!("expected BadThreshold, got {other:?}"),
    }
}

#[test]
fn a_missing_baseline_file_is_an_error() {
    let root = scratch("baseline_missing");
    assert!(load_baseline(&root.join("nope.json")).is_err());
}

// ── Report rendering ────────────────────────────────────────────────────────

#[test]
fn the_report_prints_every_id_and_annotates_failures() {
    let cmp = compare(
        &baseline_of(&[
            ("g/fails", 100.0, 50.0),
            ("g/warns", 100.0, 50.0),
            ("g/fine", 100.0, 50.0),
        ]),
        &run_of(&[("g/fails", 200.0), ("g/warns", 130.0), ("g/fine", 101.0)]),
    )
    .expect("compare");

    let report = bench_compare::render_report(&cmp, true);
    for id in ["g/fails", "g/warns", "g/fine"] {
        assert!(report.contains(id), "report must print {id}");
    }
    assert!(
        report.contains("coverage: 3 baseline ids"),
        "the report must state what the gate covers"
    );
    assert!(
        report.contains("::error"),
        "a regression must annotate as error"
    );
    assert!(
        report.contains("::warning"),
        "warn band must annotate as warning"
    );

    let plain = bench_compare::render_report(&cmp, false);
    assert!(!plain.contains("::error") && !plain.contains("::warning"));
}
