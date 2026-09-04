//! Compare a Tier A criterion run against the committed baseline — or
//! calibrate that baseline from no-op runs.
//!
//! ```text
//! # judge a run (the bench-tier-a CI leg):
//! cargo run --no-default-features --example bench_compare -- \
//!     --criterion-dir target/criterion \
//!     --baseline benches/baselines/tier-a-ubuntu-latest.json
//!
//! # print a run with no baseline (one CALIB line per id, exit 0):
//! cargo run --no-default-features --example bench_compare -- \
//!     --criterion-dir target/criterion
//!
//! # earn the baseline from ≥2 no-op runs' criterion trees:
//! cargo run --no-default-features --example bench_compare -- \
//!     --criterion-dir run1/ --criterion-dir run2/ --criterion-dir run3/ \
//!     --write-baseline benches/baselines/tier-a-ubuntu-latest.json \
//!     --runner ubuntu-latest --calibrated 2026-09-03 \
//!     --rust-version "rustc 1.91.1"
//! ```
//!
//! Exit codes when judging: `0` clean, `1` regression (an id above its own
//! threshold, or a baseline id that was not measured), `2` the comparison
//! itself could not run (unreadable input, corrupt mean, empty criterion
//! dir). A missing baseline *file* is a `2`, never a pass — a deleted
//! baseline must not turn the leg permanently green.
//!
//! Declared with **no `required-features`**: comparing JSON files needs no
//! backend, and the CI leg runs it under `--no-default-features`.

#[path = "common/bench_compare.rs"]
mod bench_compare;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "bench_compare",
    about = "Compare a criterion run against the committed Tier A baseline, \
             or calibrate that baseline from no-op runs"
)]
struct Cli {
    /// Criterion output directory (usually `target/criterion`). Repeat the
    /// flag with `--write-baseline` to calibrate from several runs.
    #[arg(long = "criterion-dir", required = true)]
    criterion_dirs: Vec<PathBuf>,

    /// Committed baseline document to judge against.
    #[arg(long, conflicts_with = "write_baseline")]
    baseline: Option<PathBuf>,

    /// Calibrate: write a baseline earned from the given criterion dirs.
    #[arg(long = "write-baseline", requires_all = ["runner", "calibrated", "rust_version"])]
    write_baseline: Option<PathBuf>,

    /// Provenance: the runner label the calibration ran on.
    #[arg(long)]
    runner: Option<String>,

    /// Provenance: the calibration date (passed in, never taken from the
    /// clock — chart and baseline artifacts must not depend on run time).
    #[arg(long)]
    calibrated: Option<String>,

    /// Provenance: the rustc that produced the calibration runs.
    #[arg(long = "rust-version")]
    rust_version: Option<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let mut runs = Vec::with_capacity(cli.criterion_dirs.len());
    for dir in &cli.criterion_dirs {
        match bench_compare::collect_run(dir) {
            Ok(run) => runs.push(run),
            Err(e) => {
                eprintln!("bench_compare: {e}");
                return ExitCode::from(2);
            }
        }
    }

    if let Some(out) = cli.write_baseline {
        let ids = match bench_compare::calibrate(&runs) {
            Ok(ids) => ids,
            Err(e) => {
                eprintln!("bench_compare: {e}");
                return ExitCode::from(2);
            }
        };
        // The requires_all on --write-baseline makes these Some; clap has
        // already rejected the invocation otherwise.
        let (Some(runner), Some(calibrated), Some(rust_version)) =
            (cli.runner, cli.calibrated, cli.rust_version)
        else {
            eprintln!("bench_compare: --write-baseline needs --runner/--calibrated/--rust-version");
            return ExitCode::from(2);
        };
        let provenance = bench_compare::Provenance {
            method: format!(
                "{} no-op runs of the Tier A suite on separate {runner} runners; \
                 mean_ns is the arithmetic mean across runs; fail_above_pct = \
                 max(50, 5 x max-vs-min spread), so an id fails only well outside \
                 its own demonstrated no-op noise",
                runs.len(),
            ),
            runner,
            calibrated,
            shove_version: env!("CARGO_PKG_VERSION").to_string(),
            rust_version,
        };
        if let Err(e) = bench_compare::write_baseline(&out, &ids, provenance) {
            eprintln!("bench_compare: {e}");
            return ExitCode::from(2);
        }
        let floor = ids
            .values()
            .filter(|e| e.fail_above_pct <= bench_compare::FAIL_FLOOR_PCT)
            .count();
        println!(
            "calibrated {} ids from {} runs ({} at the {}% floor) -> {}",
            ids.len(),
            runs.len(),
            floor,
            bench_compare::FAIL_FLOOR_PCT,
            out.display(),
        );
        return ExitCode::SUCCESS;
    }

    let [run] = runs.as_slice() else {
        eprintln!("bench_compare: judging takes exactly one --criterion-dir");
        return ExitCode::from(2);
    };

    let Some(baseline_path) = cli.baseline else {
        print!("{}", bench_compare::render_run(run));
        return ExitCode::SUCCESS;
    };

    let baseline = match bench_compare::load_baseline(&baseline_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("bench_compare: {e}");
            return ExitCode::from(2);
        }
    };

    let cmp = match bench_compare::compare(&baseline, run) {
        Ok(cmp) => cmp,
        Err(e) => {
            eprintln!("bench_compare: {e}");
            return ExitCode::from(2);
        }
    };

    // `::error::`/`::warning::` render as annotations on Actions; elsewhere
    // they are noise, so only emit them on a runner.
    let annotate = std::env::var_os("GITHUB_ACTIONS").is_some();
    print!("{}", bench_compare::render_report(&cmp, annotate));

    if cmp.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
