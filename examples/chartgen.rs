//! Render the committed benchmark results into the published SVG charts.
//!
//! ```text
//! cargo run --no-default-features --example chartgen -- \
//!     --input benches/results/bench-results.json \
//!     --out-dir docs/public/bench
//! ```
//!
//! Declared with **no `required-features`** and no backend dependency on
//! purpose: the chart-staleness CI leg only reads a JSON file and writes SVGs,
//! and gating this on a backend would drag `librdkafka-dev`/`libsasl2-dev` and
//! a full broker build into that job.
//!
//! Exits non-zero on any violation of the `bench-schema` enforcement rules —
//! an unknown `schema_version`, or a backend that measured nothing without
//! declaring why. A bad results document must not be able to produce a
//! clean-looking chart.

#[path = "common/chartgen.rs"]
mod chartgen;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "chartgen",
    about = "Render benchmark charts from a versioned results document"
)]
struct Cli {
    /// Path to the results document (`bench-schema` v4).
    #[arg(long)]
    input: PathBuf,

    /// Directory the SVGs are written into. Nothing outside it is touched.
    #[arg(long = "out-dir")]
    out_dir: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let doc = match chartgen::load(&cli.input) {
        Ok(doc) => doc,
        Err(e) => {
            eprintln!("chartgen: {e}");
            return ExitCode::FAILURE;
        }
    };

    if !cli.out_dir.is_dir() {
        eprintln!(
            "chartgen: --out-dir {} is not a directory",
            cli.out_dir.display()
        );
        return ExitCode::FAILURE;
    }

    match chartgen::generate(&doc, &cli.out_dir) {
        Ok(written) => {
            for name in written {
                println!("{}", cli.out_dir.join(name).display());
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("chartgen: {e}");
            ExitCode::FAILURE
        }
    }
}
