//! The COMMONS panel: `commons verify` over a contributions repository,
//! run in-process on demand.
//!
//! COMMONS is a CLI, not a service: it has no port and writes nothing but
//! its report. DeepAna runs the same pipeline `commons verify <root>/artifacts
//! --repo-root <root> --format json` runs, on the blocking pool, one run at
//! a time, and keeps the last report.
//!
//! The repository defaults to `<DIR>/commons` (see [`crate::layout`]) and is
//! only ever read. When it does not exist, the report says so instead of
//! failing.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use commons::config::{ARTIFACTS_DIR, Cli, Command, CommonsConfig, FIXTURES_DIR};
use commons::fixtures::FixtureSet;
use commons::manifest;
use commons::verify::{Pipeline, PrismBackend};
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// Runs verify; one run at a time.
#[derive(Debug, Clone)]
pub struct CommonsPanel {
    root: Arc<PathBuf>,
    running: Arc<Mutex<()>>,
}

impl CommonsPanel {
    pub fn new(root: PathBuf) -> Self {
        Self { root: Arc::new(root), running: Arc::new(Mutex::new(())) }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Run verify and return the panel's report:
    ///
    /// ```json
    /// {"repo_root": "...", "ran_at": 1761040000, "duration_ms": 120,
    ///  "ok": true, "report": { ...commons verify --format json... }}
    /// ```
    ///
    /// or, when verify could not run, `"ok": false` with `"error"`. A second
    /// call while one runs waits for it, then runs again.
    pub async fn verify(&self) -> Value {
        let _one_at_a_time = self.running.lock().await;
        let root = PathBuf::clone(&self.root);
        let started = Instant::now();
        let outcome = tokio::task::spawn_blocking(move || run_verify(&root))
            .await
            .unwrap_or_else(|err| Err(format!("verify task failed: {err}")));
        let base = json!({
            "repo_root": self.root.display().to_string(),
            "ran_at": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        });
        let mut report = base;
        match outcome {
            Ok(verified) => {
                report["ok"] = json!(true);
                report["report"] = verified;
            }
            Err(error) => {
                report["ok"] = json!(false);
                report["error"] = json!(error);
            }
        }
        report
    }
}

/// `commons verify <root>/artifacts --repo-root <root> --format json`, as
/// a JSON value. Blocking: COMMONS drives DataFusion on its own runtime.
pub fn run_verify(root: &Path) -> Result<Value, String> {
    let artifacts = root.join(ARTIFACTS_DIR);
    if !artifacts.is_dir() || !root.join(FIXTURES_DIR).is_dir() {
        return Err(format!(
            "no COMMONS repository at {}: expected {ARTIFACTS_DIR}/ and {FIXTURES_DIR}/ in it",
            root.display()
        ));
    }
    let args: Vec<OsString> = vec![
        "commons".into(),
        "verify".into(),
        artifacts.into_os_string(),
        "--repo-root".into(),
        root.as_os_str().to_owned(),
        "--format".into(),
        "json".into(),
    ];
    let cli = Cli::try_parse_from(args)
    .map_err(|err| err.to_string())?;
    let invocation = CommonsConfig::from_cli(cli).map_err(|err| err.to_string())?;
    let config = invocation.config;
    let Command::Verify { paths } = invocation.command else {
        return Err("not a verify invocation".to_owned());
    };
    let dirs = manifest::discover(&paths, &config).map_err(|err| err.to_string())?;
    let artifacts = dirs
        .iter()
        .map(|dir| manifest::load(dir, &config))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    manifest::check_unique(&artifacts).map_err(|err| err.to_string())?;
    manifest::check_unique_paths(&config.artifacts_dir).map_err(|err| err.to_string())?;
    let fixtures = Arc::new(FixtureSet::load(&config).map_err(|err| err.to_string())?);
    let report = Pipeline::new(&config, fixtures, Arc::new(PrismBackend)).run(artifacts);
    serde_json::from_str(&report.render_json()).map_err(|err| format!("unreadable report: {err}"))
}
