//! Settings, command line and errors for COMMONS.
//!
//! Configuration comes from command-line flags only (no environment
//! variables), and every setting has a default. Clap parses the *syntax*;
//! [`CommonsConfig::from_cli`] validates the *meaning* and returns a
//! [`CommonsError::ConfigError`] naming the offending flag.
//!
//! Path arguments come from CI, but the directories they point at come from
//! the pull request, so anything echoed back goes through [`echo`].

use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Directory under the repo root holding contributed artifacts.
pub const ARTIFACTS_DIR: &str = "artifacts";
/// Directory under the repo root holding pinned golden fixtures.
pub const FIXTURES_DIR: &str = "fixtures";

pub const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;
/// Upper bound for `--max-artifact-bytes` (1 GiB).
pub const MAX_MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_VERIFY_TIMEOUT_SECS: u64 = 60;
/// Upper bound for `--verify-timeout-secs` (1 hour).
pub const MAX_VERIFY_TIMEOUT_SECS: u64 = 3600;

/// Longest string, in chars, that [`echo`] returns before truncating.
pub const ECHO_MAX_CHARS: usize = 200;

/// Success.
pub const EXIT_OK: i32 = 0;
/// The contributor's artifact is bad (manifest, verification or publish
/// integrity), or the repository's pinned fixtures are.
pub const EXIT_ARTIFACT: i32 = 1;
/// The invocation is wrong: a flag or path argument.
pub const EXIT_USAGE: i32 = 2;
/// The machine failed: I/O or an internal bug.
pub const EXIT_INTERNAL: i32 = 3;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The one error type of the crate. Every contributor-derived value inside
/// must already have been passed through [`echo`] by whoever builds it.
#[derive(Debug, Error)]
pub enum CommonsError {
    #[error("invalid {field}: {reason}")]
    ConfigError { field: String, reason: String },

    #[error("artifact {artifact}: manifest field {field}: {reason}")]
    ManifestError {
        artifact: String,
        field: String,
        reason: String,
    },

    #[error("artifact {artifact}: verify stage {stage}: {reason}")]
    VerifyError {
        artifact: String,
        stage: String,
        reason: String,
    },

    #[error("artifact {artifact}: publish: {reason}")]
    PublishError { artifact: String, reason: String },

    /// The pinned fixtures in this repository are bad, or were changed
    /// without updating `fixtures.lock`. `path` is relative to the repo root.
    #[error("fixture {path}: field {field}: {reason}")]
    FixtureError {
        path: String,
        field: String,
        reason: String,
    },

    #[error("i/o error at {}: {source}", echo(&.path.to_string_lossy()))]
    IoError {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("internal error: {0}")]
    Internal(String),
}

impl CommonsError {
    pub fn exit_code(&self) -> i32 {
        match self {
            CommonsError::ConfigError { .. } => EXIT_USAGE,
            CommonsError::ManifestError { .. }
            | CommonsError::VerifyError { .. }
            | CommonsError::PublishError { .. }
            | CommonsError::FixtureError { .. } => EXIT_ARTIFACT,
            CommonsError::IoError { .. } | CommonsError::Internal(_) => EXIT_INTERNAL,
        }
    }
}

fn config_err(field: &str, reason: impl Into<String>) -> CommonsError {
    CommonsError::ConfigError {
        field: field.to_string(),
        reason: reason.into(),
    }
}

fn io_err(path: &Path, source: io::Error) -> CommonsError {
    CommonsError::IoError {
        path: path.to_path_buf(),
        source,
    }
}

// ---------------------------------------------------------------------------
// Echo sanitiser
// ---------------------------------------------------------------------------

/// Unicode "format" characters that can reorder or hide text in a terminal
/// or a CI log: bidi controls, zero-width characters, BOM, soft hyphen.
fn is_invisible_format(c: char) -> bool {
    matches!(c,
        '\u{00AD}'
        | '\u{061C}'
        | '\u{180E}'
        | '\u{200B}'..='\u{200F}'
        | '\u{202A}'..='\u{202E}'
        | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{2069}'
        | '\u{FEFF}')
}

/// Make a contributor-derived string safe to print in a log or CI output.
///
/// Control and invisible format characters are replaced by `\u{XXXX}`
/// escapes, backslashes are doubled so escapes are unambiguous, and the
/// result is truncated to [`ECHO_MAX_CHARS`] input chars with a trailing `…`.
pub fn echo(s: &str) -> String {
    let mut out = String::with_capacity(s.len().min(ECHO_MAX_CHARS * 2));
    for (i, c) in s.chars().enumerate() {
        if i == ECHO_MAX_CHARS {
            out.push('…');
            break;
        }
        if c == '\\' {
            out.push_str("\\\\");
        } else if c.is_control() || is_invisible_format(c) {
            out.push_str(&format!("\\u{{{:04X}}}", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

fn echo_path(p: &Path) -> String {
    echo(&p.to_string_lossy())
}

// ---------------------------------------------------------------------------
// Command line (syntax only)
// ---------------------------------------------------------------------------

/// Raw command line. Values are kept as strings here on purpose: their
/// meaning is checked in [`CommonsConfig::from_cli`].
#[derive(Debug, Parser)]
#[command(name = "commons", version, about = "Verify and publish COMMONS contributions")]
pub struct Cli {
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, Subcommand)]
pub enum CliCommand {
    /// Verify one or more artifact directories.
    Verify(VerifyArgs),
    /// Export approved artifacts to a new directory.
    Publish(PublishArgs),
}

#[derive(Debug, Args)]
pub struct CommonArgs {
    /// Root of the contributions repository.
    #[arg(long, value_name = "DIR", default_value = ".")]
    pub repo_root: PathBuf,
    /// Output format: text or json.
    #[arg(long, value_name = "FORMAT", default_value = "text")]
    pub format: String,
    /// Largest accepted artifact file, in bytes.
    #[arg(long, value_name = "N")]
    pub max_artifact_bytes: Option<String>,
    /// Log level: error, warn, info, debug or trace.
    #[arg(long, value_name = "L", default_value = "info")]
    pub log_level: String,
}

#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// Artifact version directories, or the artifacts root.
    #[arg(value_name = "PATH", required = true)]
    pub paths: Vec<PathBuf>,
    #[command(flatten)]
    pub common: CommonArgs,
    /// Enable the live LENS stage against this base URL (off by default).
    #[arg(long, value_name = "URL")]
    pub lens_url: Option<String>,
    /// Timeout for verifying one artifact, in seconds.
    #[arg(long, value_name = "N")]
    pub verify_timeout_secs: Option<String>,
}

#[derive(Debug, Args)]
pub struct PublishArgs {
    /// Output directory. Must not exist yet; publish creates it.
    #[arg(long, value_name = "DIR")]
    pub out: PathBuf,
    #[command(flatten)]
    pub common: CommonArgs,
}

// ---------------------------------------------------------------------------
// Validated configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            OutputFormat::Text => "text",
            OutputFormat::Json => "json",
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonsConfig {
    /// Canonical, absolute.
    pub repo_root: PathBuf,
    /// `repo_root/artifacts`, canonical.
    pub artifacts_dir: PathBuf,
    /// `repo_root/fixtures`, canonical. `Some` only for `verify`.
    pub fixtures_dir: Option<PathBuf>,
    pub format: OutputFormat,
    pub log_level: LogLevel,
    pub max_artifact_bytes: u64,
    /// `Some` only for `verify`, and only when `--lens-url` was given.
    pub lens_url: Option<String>,
    pub verify_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Canonical, de-duplicated, sorted; none nested in another.
    Verify { paths: Vec<PathBuf> },
    /// Absolute; does not exist yet.
    Publish { out: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    pub config: CommonsConfig,
    pub command: Command,
}

impl CommonsConfig {
    /// Validate a parsed command line.
    pub fn from_cli(cli: Cli) -> Result<Invocation, CommonsError> {
        match cli.command {
            CliCommand::Verify(args) => {
                let base = Base::from_common(&args.common)?;
                let fixtures_dir = existing_dir(
                    "--repo-root",
                    &base.repo_root.join(FIXTURES_DIR),
                    "fixtures directory",
                )?;
                let lens_url = args.lens_url.as_deref().map(parse_lens_url).transpose()?;
                let timeout_secs = match &args.verify_timeout_secs {
                    Some(s) => parse_number("--verify-timeout-secs", s, MAX_VERIFY_TIMEOUT_SECS)?,
                    None => DEFAULT_VERIFY_TIMEOUT_SECS,
                };
                let paths = verify_paths(&args.paths, &base.artifacts_dir)?;
                Ok(Invocation {
                    config: base.into_config(
                        Some(fixtures_dir),
                        lens_url,
                        Duration::from_secs(timeout_secs),
                    ),
                    command: Command::Verify { paths },
                })
            }
            CliCommand::Publish(args) => {
                let base = Base::from_common(&args.common)?;
                let out = publish_out(&args.out, &base.repo_root)?;
                Ok(Invocation {
                    config: base.into_config(
                        None,
                        None,
                        Duration::from_secs(DEFAULT_VERIFY_TIMEOUT_SECS),
                    ),
                    command: Command::Publish { out },
                })
            }
        }
    }

    /// One `key=value` line per field, for logging. Pure; nothing here is secret.
    pub fn describe(&self) -> Vec<String> {
        let opt_path = |p: &Option<PathBuf>| p.as_deref().map_or("-".to_string(), echo_path);
        vec![
            format!("repo_root={}", echo_path(&self.repo_root)),
            format!("artifacts_dir={}", echo_path(&self.artifacts_dir)),
            format!("fixtures_dir={}", opt_path(&self.fixtures_dir)),
            format!("format={}", self.format),
            format!("log_level={}", self.log_level),
            format!("max_artifact_bytes={}", self.max_artifact_bytes),
            format!(
                "lens_url={}",
                self.lens_url.as_deref().map_or("-".to_string(), echo)
            ),
            format!("verify_timeout_secs={}", self.verify_timeout.as_secs()),
        ]
    }
}

/// Settings shared by both subcommands.
struct Base {
    repo_root: PathBuf,
    artifacts_dir: PathBuf,
    format: OutputFormat,
    log_level: LogLevel,
    max_artifact_bytes: u64,
}

impl Base {
    fn from_common(c: &CommonArgs) -> Result<Self, CommonsError> {
        let format = match c.format.as_str() {
            "text" => OutputFormat::Text,
            "json" => OutputFormat::Json,
            other => {
                return Err(config_err(
                    "--format",
                    format!("expected text or json, got '{}'", echo(other)),
                ));
            }
        };
        let log_level = match c.log_level.as_str() {
            "error" => LogLevel::Error,
            "warn" => LogLevel::Warn,
            "info" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            "trace" => LogLevel::Trace,
            other => {
                return Err(config_err(
                    "--log-level",
                    format!(
                        "expected error, warn, info, debug or trace, got '{}'",
                        echo(other)
                    ),
                ));
            }
        };
        let max_artifact_bytes = match &c.max_artifact_bytes {
            Some(s) => parse_number("--max-artifact-bytes", s, MAX_MAX_ARTIFACT_BYTES)?,
            None => DEFAULT_MAX_ARTIFACT_BYTES,
        };
        let repo_root = existing_dir("--repo-root", &c.repo_root, "repo root")?;
        let artifacts_dir = existing_dir(
            "--repo-root",
            &repo_root.join(ARTIFACTS_DIR),
            "artifacts directory",
        )?;
        Ok(Base {
            repo_root,
            artifacts_dir,
            format,
            log_level,
            max_artifact_bytes,
        })
    }

    fn into_config(
        self,
        fixtures_dir: Option<PathBuf>,
        lens_url: Option<String>,
        verify_timeout: Duration,
    ) -> CommonsConfig {
        CommonsConfig {
            repo_root: self.repo_root,
            artifacts_dir: self.artifacts_dir,
            fixtures_dir,
            format: self.format,
            log_level: self.log_level,
            max_artifact_bytes: self.max_artifact_bytes,
            lens_url,
            verify_timeout,
        }
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// ASCII digits only (no sign, no whitespace, no `_`, no hex), in `1..=max`.
fn parse_number(field: &str, s: &str, max: u64) -> Result<u64, CommonsError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(config_err(
            field,
            format!("expected digits only, got '{}'", echo(s)),
        ));
    }
    let n: u64 = s
        .parse()
        .map_err(|_| config_err(field, format!("'{}' is out of range (max {max})", echo(s))))?;
    if n == 0 {
        return Err(config_err(field, "must be greater than 0"));
    }
    if n > max {
        return Err(config_err(field, format!("{n} is out of range (max {max})")));
    }
    Ok(n)
}

fn parse_lens_url(s: &str) -> Result<String, CommonsError> {
    const F: &str = "--lens-url";
    let url = url::Url::parse(s)
        .map_err(|e| config_err(F, format!("'{}' is not a valid URL: {e}", echo(s))))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(config_err(
            F,
            format!("scheme must be http or https, got '{}'", echo(url.scheme())),
        ));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(config_err(F, "URL has no host"));
    }
    // Credentials would make this a secret, and the config holds none.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(config_err(F, "URL must not contain credentials"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(config_err(F, "URL must not contain a query or fragment"));
    }
    Ok(url.to_string())
}

/// The path must exist and be a real directory (not a symlink). Returns the
/// canonical form. The existence check comes first, so a missing path is a
/// usage error (exit 2), not an I/O error.
fn existing_dir(field: &str, p: &Path, what: &str) -> Result<PathBuf, CommonsError> {
    let meta = match std::fs::symlink_metadata(p) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(config_err(
                field,
                format!("{what} '{}' does not exist", echo_path(p)),
            ));
        }
        Err(e) => return Err(io_err(p, e)),
    };
    if meta.file_type().is_symlink() {
        return Err(config_err(
            field,
            format!("{what} '{}' is a symlink", echo_path(p)),
        ));
    }
    if !meta.is_dir() {
        return Err(config_err(
            field,
            format!("{what} '{}' is not a directory", echo_path(p)),
        ));
    }
    std::fs::canonicalize(p).map_err(|e| io_err(p, e))
}

fn verify_paths(raw: &[PathBuf], artifacts_dir: &Path) -> Result<Vec<PathBuf>, CommonsError> {
    const F: &str = "PATH";
    if raw.is_empty() {
        return Err(config_err(F, "at least one path is required"));
    }
    let mut paths = Vec::with_capacity(raw.len());
    for p in raw {
        let c = existing_dir(F, p, "path")?;
        if !c.starts_with(artifacts_dir) {
            return Err(config_err(
                F,
                format!(
                    "'{}' is not inside the artifacts directory '{}'",
                    echo_path(p),
                    echo_path(artifacts_dir)
                ),
            ));
        }
        paths.push(c);
    }
    paths.sort();
    paths.dedup();
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            if b.starts_with(a) {
                return Err(config_err(
                    F,
                    format!(
                        "'{}' is nested inside '{}'",
                        echo_path(b),
                        echo_path(a)
                    ),
                ));
            }
        }
    }
    Ok(paths)
}

/// `--out` must not exist yet (publish never writes into a directory it
/// didn't create), its parent must be an existing real directory, and it must
/// not land inside the repository.
fn publish_out(raw: &Path, repo_root: &Path) -> Result<PathBuf, CommonsError> {
    const F: &str = "--out";
    match std::fs::symlink_metadata(raw) {
        Ok(_) => {
            return Err(config_err(
                F,
                format!(
                    "'{}' already exists; publish only writes into a directory it creates",
                    echo_path(raw)
                ),
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(io_err(raw, e)),
    }
    let name = match raw.components().next_back() {
        Some(Component::Normal(n)) => n.to_owned(),
        _ => {
            return Err(config_err(
                F,
                format!("'{}' must end in a plain directory name", echo_path(raw)),
            ));
        }
    };
    let parent = match raw.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let parent = existing_dir(F, parent, "parent directory")?;
    let out = parent.join(name);
    if out.starts_with(repo_root) {
        return Err(config_err(
            F,
            format!(
                "'{}' is inside the repo root '{}'",
                echo_path(&out),
                echo_path(repo_root)
            ),
        ));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// A scratch repo with `artifacts/` and `fixtures/`.
    fn repo() -> TempDir {
        let t = tempfile::tempdir().unwrap();
        fs::create_dir(t.path().join(ARTIFACTS_DIR)).unwrap();
        fs::create_dir(t.path().join(FIXTURES_DIR)).unwrap();
        t
    }

    fn run(args: &[&str]) -> Result<Invocation, CommonsError> {
        let mut v = vec!["commons"];
        v.extend_from_slice(args);
        CommonsConfig::from_cli(Cli::try_parse_from(v).expect("clap parse"))
    }

    fn s(p: &Path) -> &str {
        p.to_str().unwrap()
    }

    fn field_of(e: &CommonsError) -> &str {
        match e {
            CommonsError::ConfigError { field, .. } => field,
            other => panic!("expected ConfigError, got {other:?}"),
        }
    }

    #[test]
    fn verify_defaults() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        let inv = run(&["verify", s(&art), "--repo-root", s(r.path())]).unwrap();
        let root = fs::canonicalize(r.path()).unwrap();
        assert_eq!(inv.config.repo_root, root);
        assert_eq!(inv.config.artifacts_dir, root.join(ARTIFACTS_DIR));
        assert_eq!(inv.config.fixtures_dir, Some(root.join(FIXTURES_DIR)));
        assert_eq!(inv.config.format, OutputFormat::Text);
        assert_eq!(inv.config.log_level, LogLevel::Info);
        assert_eq!(inv.config.max_artifact_bytes, DEFAULT_MAX_ARTIFACT_BYTES);
        assert_eq!(inv.config.lens_url, None);
        assert_eq!(
            inv.config.verify_timeout,
            Duration::from_secs(DEFAULT_VERIFY_TIMEOUT_SECS)
        );
        assert_eq!(
            inv.command,
            Command::Verify {
                paths: vec![root.join(ARTIFACTS_DIR)]
            }
        );
    }

    #[test]
    fn verify_all_flags() {
        let r = repo();
        let a = r.path().join(ARTIFACTS_DIR).join("a");
        fs::create_dir(&a).unwrap();
        let inv = run(&[
            "verify",
            s(&a),
            "--repo-root",
            s(r.path()),
            "--format",
            "json",
            "--lens-url",
            "http://localhost:8080",
            "--max-artifact-bytes",
            "1024",
            "--verify-timeout-secs",
            "5",
            "--log-level",
            "debug",
        ])
        .unwrap();
        assert_eq!(inv.config.format, OutputFormat::Json);
        assert_eq!(inv.config.log_level, LogLevel::Debug);
        assert_eq!(inv.config.max_artifact_bytes, 1024);
        assert_eq!(inv.config.verify_timeout, Duration::from_secs(5));
        assert_eq!(inv.config.lens_url.as_deref(), Some("http://localhost:8080/"));
    }

    #[test]
    fn verify_paths_sorted_and_deduped() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        fs::create_dir(art.join("b")).unwrap();
        fs::create_dir(art.join("a")).unwrap();
        let b = art.join("b");
        let b_dotted = art.join("a").join("..").join("b");
        let a = art.join("a");
        let inv = run(&["verify", s(&b), s(&a), s(&b_dotted), "--repo-root", s(r.path())]).unwrap();
        let canon = fs::canonicalize(&art).unwrap();
        assert_eq!(
            inv.command,
            Command::Verify {
                paths: vec![canon.join("a"), canon.join("b")]
            }
        );
    }

    #[test]
    fn verify_rejects_nested_paths() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        fs::create_dir_all(art.join("a").join("v1")).unwrap();
        let e = run(&["verify", s(&art.join("a").join("v1")), s(&art), "--repo-root", s(r.path())])
            .unwrap_err();
        assert_eq!(e.exit_code(), EXIT_USAGE);
        assert!(e.to_string().contains("nested inside"), "{e}");
    }

    #[test]
    fn verify_rejects_missing_file_symlink_and_outside() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        let root = s(r.path());

        let e = run(&["verify", s(&art.join("nope")), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("does not exist"), "{e}");

        fs::write(art.join("f"), b"x").unwrap();
        let e = run(&["verify", s(&art.join("f")), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("not a directory"), "{e}");

        #[cfg(unix)]
        {
            fs::create_dir(art.join("real")).unwrap();
            std::os::unix::fs::symlink(art.join("real"), art.join("link")).unwrap();
            let e = run(&["verify", s(&art.join("link")), "--repo-root", root]).unwrap_err();
            assert!(e.to_string().contains("symlink"), "{e}");
        }

        let e = run(&["verify", s(&r.path().join(FIXTURES_DIR)), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("not inside the artifacts"), "{e}");
    }

    #[test]
    fn missing_repo_dirs_are_usage_errors() {
        let t = tempfile::tempdir().unwrap();
        let e = run(&["verify", ".", "--repo-root", s(t.path())]).unwrap_err();
        assert_eq!(field_of(&e), "--repo-root");
        assert_eq!(e.exit_code(), EXIT_USAGE);

        // Verify needs fixtures/, publish doesn't.
        fs::create_dir(t.path().join(ARTIFACTS_DIR)).unwrap();
        let e = run(&["verify", s(&t.path().join(ARTIFACTS_DIR)), "--repo-root", s(t.path())])
            .unwrap_err();
        assert!(e.to_string().contains("fixtures"), "{e}");
    }

    #[test]
    fn numbers_are_digits_only() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        for bad in ["", "+5", "-1", " 5", "5 ", "0x10", "1_000", "1e3", "٣", "0"] {
            let e = run(&[
                "verify",
                s(&art),
                "--repo-root",
                s(r.path()),
                &format!("--max-artifact-bytes={bad}"),
            ])
            .unwrap_err();
            assert_eq!(field_of(&e), "--max-artifact-bytes", "input {bad:?}");
        }
        let e = run(&[
            "verify",
            s(&art),
            "--repo-root",
            s(r.path()),
            "--verify-timeout-secs",
            "99999999999999999999999",
        ])
        .unwrap_err();
        assert_eq!(field_of(&e), "--verify-timeout-secs");
        let e = run(&[
            "verify",
            s(&art),
            "--repo-root",
            s(r.path()),
            "--verify-timeout-secs",
            "3601",
        ])
        .unwrap_err();
        assert!(e.to_string().contains("out of range"), "{e}");
        // Leading zeros are still digits.
        let inv = run(&[
            "verify",
            s(&art),
            "--repo-root",
            s(r.path()),
            "--verify-timeout-secs",
            "007",
        ])
        .unwrap();
        assert_eq!(inv.config.verify_timeout, Duration::from_secs(7));
    }

    #[test]
    fn bad_format_and_log_level() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        let e = run(&["verify", s(&art), "--repo-root", s(r.path()), "--format", "yaml"]).unwrap_err();
        assert_eq!(field_of(&e), "--format");
        let e = run(&["verify", s(&art), "--repo-root", s(r.path()), "--log-level", "INFO"])
            .unwrap_err();
        assert_eq!(field_of(&e), "--log-level");
    }

    #[test]
    fn lens_url_rules() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        for bad in [
            "not a url",
            "ftp://host/",
            "http://user:pw@host/",
            "http://host/?token=x",
            "file:///tmp",
        ] {
            let e = run(&["verify", s(&art), "--repo-root", s(r.path()), "--lens-url", bad])
                .unwrap_err();
            assert_eq!(field_of(&e), "--lens-url", "input {bad:?}");
        }
    }

    #[test]
    fn publish_rejects_lens_and_timeout_flags() {
        assert!(Cli::try_parse_from(["commons", "publish", "--out", "x", "--lens-url", "http://h"]).is_err());
        assert!(
            Cli::try_parse_from(["commons", "publish", "--out", "x", "--verify-timeout-secs", "1"])
                .is_err()
        );
    }

    #[test]
    fn publish_ok() {
        let r = repo();
        let dest = tempfile::tempdir().unwrap();
        let out = dest.path().join("site");
        let inv = run(&["publish", "--out", s(&out), "--repo-root", s(r.path())]).unwrap();
        assert_eq!(
            inv.command,
            Command::Publish {
                out: fs::canonicalize(dest.path()).unwrap().join("site")
            }
        );
        assert_eq!(inv.config.fixtures_dir, None);
        assert_eq!(inv.config.lens_url, None);
        assert!(!out.exists(), "config must not create anything");
    }

    #[test]
    fn publish_out_rules() {
        let r = repo();
        let dest = tempfile::tempdir().unwrap();
        let root = s(r.path());

        // Already exists.
        let e = run(&["publish", "--out", s(dest.path()), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("already exists"), "{e}");

        // Parent missing.
        let deep = dest.path().join("no").join("site");
        let e = run(&["publish", "--out", s(&deep), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("does not exist"), "{e}");

        // Ends in `..`.
        let dotdot = dest.path().join("..");
        let e = run(&["publish", "--out", s(&dotdot), "--repo-root", root]).unwrap_err();
        assert_eq!(field_of(&e), "--out");

        // Inside the repo.
        let inside = r.path().join("dist");
        let e = run(&["publish", "--out", s(&inside), "--repo-root", root]).unwrap_err();
        assert!(e.to_string().contains("inside the repo root"), "{e}");
    }

    #[test]
    fn help_and_version_are_display_errors() {
        use clap::error::ErrorKind;
        let e = Cli::try_parse_from(["commons", "--help"]).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::DisplayHelp);
        let e = Cli::try_parse_from(["commons", "--version"]).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::DisplayVersion);
        assert_eq!(e.exit_code(), 0);
    }

    #[test]
    fn verify_requires_a_path() {
        assert!(Cli::try_parse_from(["commons", "verify"]).is_err());
    }

    #[test]
    fn echo_sanitises() {
        assert_eq!(echo("plain/path-1.0"), "plain/path-1.0");
        assert_eq!(echo("a\nb\tc\x1b[31m"), "a\\u{000A}b\\u{0009}c\\u{001B}[31m");
        assert_eq!(echo("x\u{202E}y\u{200B}z\u{FEFF}"), "x\\u{202E}y\\u{200B}z\\u{FEFF}");
        assert_eq!(echo("a\\u{000A}"), "a\\\\u{000A}");
        assert_eq!(echo("héllo"), "héllo");
        let long = "x".repeat(ECHO_MAX_CHARS + 10);
        let out = echo(&long);
        assert_eq!(out.chars().count(), ECHO_MAX_CHARS + 1);
        assert!(out.ends_with('…'));
        assert_eq!(echo(&"y".repeat(ECHO_MAX_CHARS)).chars().count(), ECHO_MAX_CHARS);
    }

    #[test]
    fn error_messages_echo_contributor_paths() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        let evil = art.join("evil\u{1b}[2Jname");
        let e = run(&["verify", s(&evil), "--repo-root", s(r.path())]).unwrap_err();
        let msg = e.to_string();
        assert!(!msg.contains('\u{1b}'), "{msg:?}");
        assert!(msg.contains("\\u{001B}"), "{msg:?}");
    }

    #[test]
    fn exit_codes() {
        let s = String::new;
        assert_eq!(config_err("--x", "r").exit_code(), 2);
        assert_eq!(
            CommonsError::ManifestError { artifact: s(), field: s(), reason: s() }.exit_code(),
            1
        );
        assert_eq!(
            CommonsError::VerifyError { artifact: s(), stage: s(), reason: s() }.exit_code(),
            1
        );
        assert_eq!(CommonsError::PublishError { artifact: s(), reason: s() }.exit_code(), 1);
        assert_eq!(
            CommonsError::FixtureError { path: s(), field: s(), reason: s() }.exit_code(),
            1
        );
        assert_eq!(
            io_err(Path::new("/x"), io::Error::other("boom")).exit_code(),
            3
        );
        assert_eq!(CommonsError::Internal(s()).exit_code(), 3);
    }

    #[test]
    fn describe_lists_every_field() {
        let r = repo();
        let art = r.path().join(ARTIFACTS_DIR);
        let inv = run(&["verify", s(&art), "--repo-root", s(r.path())]).unwrap();
        let d = inv.config.describe();
        let keys: Vec<&str> = d.iter().map(|l| l.split('=').next().unwrap()).collect();
        assert_eq!(
            keys,
            [
                "repo_root",
                "artifacts_dir",
                "fixtures_dir",
                "format",
                "log_level",
                "max_artifact_bytes",
                "lens_url",
                "verify_timeout_secs"
            ]
        );
        assert_eq!(d, inv.config.describe(), "describe is pure");
    }
}
