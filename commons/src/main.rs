//! `commons verify <path>...`: discover, load and verify artifacts, print
//! the report, exit with its code.
//!
//! Exit codes: 0 every artifact passed, 1 an artifact or the fixtures are
//! wrong, 2 the invocation is wrong, 3 the machine or the verifier failed.
//! Nothing is written but stdout and stderr.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use commons::config::{Cli, Command, CommonsConfig, CommonsError, OutputFormat};
use commons::fixtures::FixtureSet;
use commons::manifest;
use commons::verify::{Pipeline, PrismBackend};

fn exit(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

fn fail(e: &CommonsError) -> ExitCode {
    eprintln!("commons: {e}");
    exit(e.exit_code())
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            let _ = e.print();
            return exit(e.exit_code());
        }
    };
    let inv = match CommonsConfig::from_cli(cli) {
        Ok(i) => i,
        Err(e) => return fail(&e),
    };
    let config = inv.config;
    match inv.command {
        Command::Verify { paths } => {
            // Panics are contained and reported by the pipeline; the
            // default hook would print contributor-influenced text raw.
            std::panic::set_hook(Box::new(|_| {}));
            let run = || -> Result<ExitCode, CommonsError> {
                let dirs = manifest::discover(&paths, &config)?;
                let artifacts = dirs
                    .iter()
                    .map(|d| manifest::load(d, &config))
                    .collect::<Result<Vec<_>, _>>()?;
                manifest::check_unique(&artifacts)?;
                manifest::check_unique_paths(&config.artifacts_dir)?;
                let fixtures = Arc::new(FixtureSet::load(&config)?);
                let report = Pipeline::new(&config, fixtures, Arc::new(PrismBackend)).run(artifacts);
                match config.format {
                    OutputFormat::Text => print!("{}", report.render_text()),
                    OutputFormat::Json => print!("{}", report.render_json()),
                }
                Ok(exit(report.exit_code()))
            };
            run().unwrap_or_else(|e| fail(&e))
        }
        Command::Publish { .. } => fail(&CommonsError::Internal(
            "publish is not implemented in this build".into(),
        )),
    }
}
