//! Compare a Tier A criterion run against the committed baseline.
//!
//! ```text
//! cargo run --no-default-features --example bench_compare -- \
//!     --criterion-dir target/criterion \
//!     --baseline benches/baselines/tier-a-ubuntu-latest.json
//! ```
//!
//! Without `--baseline` it prints every measured id and its mean and exits 0 —
//! the calibration view used to build a baseline in the first place. With
//! `--baseline`, the exit code is the verdict: `0` clean, `1` regression
//! (a gated id > 50% slower, or a baseline id that was not measured), `2` the
//! comparison itself could not run (unreadable input, corrupt mean, empty
//! criterion dir). A missing baseline *file* is a `2`, never a pass — a
//! deleted baseline must not turn the leg permanently green.
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
    about = "Compare a criterion run against the committed Tier A baseline"
)]
struct Cli {
    /// Criterion output directory (usually `target/criterion`).
    #[arg(long = "criterion-dir")]
    criterion_dir: PathBuf,

    /// Committed baseline document. Omit to print the calibration view.
    #[arg(long)]
    baseline: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let run = match bench_compare::collect_run(&cli.criterion_dir) {
        Ok(run) => run,
        Err(e) => {
            eprintln!("bench_compare: {e}");
            return ExitCode::from(2);
        }
    };

    let Some(baseline_path) = cli.baseline else {
        print!("{}", bench_compare::render_run(&run));
        return ExitCode::SUCCESS;
    };

    let baseline = match bench_compare::load_baseline(&baseline_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("bench_compare: {e}");
            return ExitCode::from(2);
        }
    };

    let cmp = match bench_compare::compare(&baseline, &run) {
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
