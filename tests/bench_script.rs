//! Tests for `scripts/bench.sh`'s argument resolution.
//!
//! The script pins the published matrix in one `MATRIX` array and joins it
//! with whatever the caller passes after `--`. Joining used to mean
//! *appending*, which made a pinned knob impossible to change: clap rejects a
//! repeated argument outright ("cannot be used multiple times") and the run
//! died before a single scenario, so any cell needing a corpus other than the
//! pinned 6 M could not be started at all. These tests pin the replacement
//! rule instead — a knob named after `--` replaces its pinned copy, and
//! nothing else in the matrix moves.
//!
//! They drive `--dry-run`, which resolves the harness argv and prints it one
//! argument per line without building or running anything. That is the only
//! way to assert on the resolution without a release build, Docker and an
//! hour per backend.
//!
//! Deliberately **not** feature-gated: the script picks the feature set, so
//! these need none, and CI's `check` job runs `cargo test
//! --no-default-features`, which is where they have to hold.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Run `scripts/bench.sh` with `--dry-run` and return `(argv, stderr)`.
///
/// Panics with both streams on a non-zero exit, so a script error reads as
/// itself rather than as an empty argv.
fn dry_run(args: &[&str]) -> (Vec<String>, String) {
    // `--dry-run` is a script option, so it has to precede the `--`
    // separator; everything after that belongs to the harness.
    let (backend, rest) = args.split_first().expect("a backend");
    let out = Command::new("bash")
        .arg("scripts/bench.sh")
        .arg(backend)
        .arg("--dry-run")
        .args(rest)
        .current_dir(repo_root())
        .output()
        .expect("bash scripts/bench.sh");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "bench.sh {args:?} --dry-run exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code(),
    );
    (stdout.lines().map(str::to_string).collect(), stderr)
}

/// Every argument of `flag`, in order. Empty when the flag is absent.
fn values_of(argv: &[String], flag: &str) -> Vec<String> {
    argv.iter()
        .enumerate()
        .filter(|(_, a)| a.as_str() == flag)
        .map(|(i, _)| argv.get(i + 1).cloned().unwrap_or_default())
        .collect()
}

fn occurrences(argv: &[String], flag: &str) -> usize {
    argv.iter().filter(|a| a.as_str() == flag).count()
}

/// The pinned matrix, as the runbook publishes it. Hand-synced with the
/// `MATRIX` array: if the matrix legitimately changes, this fails and the
/// runbook's copy needs the same edit.
const PINNED: &[(&str, Option<&str>)] = &[
    ("--flow", Some("all")),
    ("--payload", Some("all")),
    ("--tier", Some("moderate")),
    ("--handler", Some("zero")),
    ("--consumers", Some("1,2,4,8")),
    ("--concurrent", None),
    ("--drain-messages", Some("6000000")),
    ("--drain-max-bytes", Some("3221225472")),
    ("--load-rates", Some("5000,25000,100000")),
    ("--load-window-secs", Some("10")),
    ("--load-producers", Some("8")),
];

#[test]
fn plain_run_resolves_to_the_pinned_matrix() {
    let (argv, _) = dry_run(&["inmemory"]);

    let mut expected: Vec<String> = Vec::new();
    for (flag, value) in PINNED {
        expected.push((*flag).to_string());
        if let Some(v) = value {
            expected.push((*v).to_string());
        }
    }
    expected.push("--results-file".to_string());
    expected.push("benches/results/bench-results.json".to_string());

    assert_eq!(
        argv, expected,
        "an override path must not perturb the argv of a plain published run"
    );
}

#[test]
fn a_pinned_knob_named_after_the_separator_replaces_its_pinned_copy() {
    let (argv, stderr) = dry_run(&["kafka", "--", "--drain-messages", "60000"]);

    assert_eq!(
        occurrences(&argv, "--drain-messages"),
        1,
        "the duplicate is exactly what clap rejects: {argv:?}"
    );
    assert_eq!(values_of(&argv, "--drain-messages"), vec!["60000"]);
    assert!(
        !argv.iter().any(|a| a == "6000000"),
        "the pinned value must be dropped, not orphaned as a positional: {argv:?}"
    );
    assert!(
        stderr.contains("--drain-messages"),
        "an overridden knob makes the row incomparable with the published \
         ones, so the run has to say so: {stderr:?}"
    );
}

#[test]
fn overriding_one_knob_leaves_every_other_pinned_knob_intact() {
    let (argv, _) = dry_run(&["kafka", "--", "--drain-messages", "60000"]);

    for (flag, value) in PINNED {
        if *flag == "--drain-messages" {
            continue;
        }
        assert_eq!(
            occurrences(&argv, flag),
            1,
            "{flag} is pinned and was not overridden: {argv:?}"
        );
        if let Some(v) = value {
            assert_eq!(values_of(&argv, flag), vec![*v], "{flag} value moved");
        }
    }
}

#[test]
fn the_equals_form_overrides_too() {
    let (argv, _) = dry_run(&["kafka", "--", "--drain-messages=60000"]);

    assert_eq!(
        occurrences(&argv, "--drain-messages"),
        0,
        "the pinned space-separated copy must go: {argv:?}"
    );
    assert!(
        argv.iter().any(|a| a == "--drain-messages=60000"),
        "the caller's form is passed through verbatim: {argv:?}"
    );
    assert!(
        !argv.iter().any(|a| a == "6000000"),
        "the pinned value must not survive as a positional: {argv:?}"
    );
}

#[test]
fn overriding_a_valueless_pinned_flag_does_not_swallow_the_next_knob() {
    // `--concurrent` takes no value. Dropping it must drop one token, not
    // two, or the knob pinned after it silently vanishes from the run.
    let (argv, _) = dry_run(&["inmemory", "--", "--concurrent"]);

    assert_eq!(occurrences(&argv, "--concurrent"), 1, "{argv:?}");
    assert_eq!(
        values_of(&argv, "--drain-messages"),
        vec!["6000000"],
        "the knob pinned after --concurrent was eaten: {argv:?}"
    );
}

#[test]
fn a_knob_the_matrix_does_not_pin_is_appended_untouched() {
    let (argv, stderr) = dry_run(&["kafka", "--", "--prefetch", "200"]);

    for (flag, _) in PINNED {
        assert_eq!(
            occurrences(&argv, flag),
            1,
            "{flag} must survive an unrelated extra: {argv:?}"
        );
    }
    assert_eq!(values_of(&argv, "--prefetch"), vec!["200"]);
    assert!(
        !stderr.contains("overrides pinned"),
        "--prefetch overrides nothing, so nothing should be announced: {stderr:?}"
    );
}

#[test]
fn several_pinned_knobs_can_be_overridden_at_once() {
    let (argv, _) = dry_run(&[
        "kafka",
        "--",
        "--drain-messages",
        "60000",
        "--load-rates",
        "500,1000",
    ]);

    assert_eq!(values_of(&argv, "--drain-messages"), vec!["60000"]);
    assert_eq!(values_of(&argv, "--load-rates"), vec!["500,1000"]);
    assert!(!argv.iter().any(|a| a == "6000000"), "{argv:?}");
    assert!(!argv.iter().any(|a| a == "5000,25000,100000"), "{argv:?}");
}

/// SQS's corpus is a published property of the matrix, substituted into
/// `MATRIX` for that target rather than passed after `--`. Substitution has to
/// happen *before* the join, or a `--dry-run` would report the pinned 6 M while
/// the real run drained 60 k.
#[test]
fn the_sqs_corpus_deviation_is_what_the_resolved_argv_carries() {
    let (argv, stderr) = dry_run(&["sqs"]);

    assert_eq!(
        values_of(&argv, "--drain-messages"),
        vec!["60000"],
        "SQS_DRAIN_MESSAGES must reach the harness: {argv:?}"
    );
    assert!(!argv.iter().any(|a| a == "6000000"), "{argv:?}");
    assert!(
        !stderr.contains("overrides pinned"),
        "the deviation is the matrix for this target, not a caller override: {stderr:?}"
    );
}

#[test]
fn the_deviation_applies_to_sqs_alone() {
    for backend in ["inmemory", "kafka", "nats", "rabbitmq", "redis"] {
        let (argv, _) = dry_run(&[backend]);
        assert_eq!(
            values_of(&argv, "--drain-messages"),
            vec!["6000000"],
            "{backend} must drain the pinned corpus: {argv:?}"
        );
    }
}

/// A caller who names the knob themselves has replaced the deviation too, so
/// the run is no longer the documented one and its log must not claim to be.
#[test]
fn a_caller_corpus_replaces_the_sqs_deviation_and_retracts_its_claim() {
    let (argv, stderr) = dry_run(&["sqs", "--", "--drain-messages", "20000"]);

    assert_eq!(values_of(&argv, "--drain-messages"), vec!["20000"]);
    assert!(!argv.iter().any(|a| a == "60000"), "{argv:?}");
    assert!(
        stderr.contains("overrides pinned"),
        "a replaced corpus is announced whichever value it replaced: {stderr:?}"
    );
}

#[test]
fn the_results_file_stays_last_and_follows_the_flag() {
    let (argv, _) = dry_run(&["inmemory", "--results-file", "/tmp/one-off.json"]);

    assert_eq!(
        argv.iter().rev().take(2).collect::<Vec<_>>(),
        vec!["/tmp/one-off.json", "--results-file"],
        "{argv:?}"
    );
}
