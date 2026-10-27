//! Measures how accurately `LensConfig::load` sorts environments into
//! accepted and rejected.
//!
//! Every case sets real `LENS_*` process environment variables and calls the
//! real `LensConfig::load()`. Each outcome is scored:
//!
//! - **correct reject**: a `ConfigError` naming the expected field
//! - **wrong field**: a `ConfigError` blaming a different field
//! - **false accept**: a malformed environment that loaded anyway
//! - **false reject**: a well-formed environment that failed to load
//!
//! The test prints the scorecard and requires 100%. Well-formed cases are
//! included so a `load` that rejects everything cannot pass.
//!
//! This file must contain exactly one `#[test]`: it mutates the process
//! environment, which is only sound while no other thread reads it.

use std::fs;
use std::path::{Path, PathBuf};

use lens::config::*;
use lens::{LensConfig, LensError};

/// How the case sets `LENS_DATA_DIR`.
#[derive(Clone, Copy)]
enum DataDir {
    /// A directory laid out like PRISM's.
    Valid,
    /// Not set at all.
    Unset,
    /// Set to this literal value.
    Literal(&'static str),
    /// A path under the scratch root that does not exist.
    Missing,
    /// A regular file.
    File,
    /// An existing directory with no table directories.
    Bare,
    /// Table directories directly under `data_dir`, without the chain level.
    FlatLayout,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    Accept,
    Reject(&'static str),
}

struct Case {
    name: &'static str,
    data_dir: DataDir,
    vars: &'static [(&'static str, &'static str)],
    expect: Expect,
}

const fn reject(name: &'static str, field: &'static str, vars: &'static [(&'static str, &'static str)]) -> Case {
    Case { name, data_dir: DataDir::Valid, vars, expect: Expect::Reject(field) }
}

const fn reject_dir(name: &'static str, data_dir: DataDir) -> Case {
    Case { name, data_dir, vars: &[], expect: Expect::Reject("data_dir") }
}

const fn accept(name: &'static str, vars: &'static [(&'static str, &'static str)]) -> Case {
    Case { name, data_dir: DataDir::Valid, vars, expect: Expect::Accept }
}

const CASES: &[Case] = &[
    // data_dir
    reject_dir("data_dir unset", DataDir::Unset),
    reject_dir("data_dir empty", DataDir::Literal("")),
    reject_dir("data_dir whitespace", DataDir::Literal("   ")),
    reject_dir("data_dir does not exist", DataDir::Missing),
    reject_dir("data_dir is a file", DataDir::File),
    reject_dir("data_dir has no table dirs", DataDir::Bare),
    reject_dir("data_dir tables missing chain level", DataDir::FlatLayout),
    // bind_address
    reject("bind_address empty", "bind_address", &[(ENV_BIND_ADDRESS, "")]),
    reject("bind_address whitespace", "bind_address", &[(ENV_BIND_ADDRESS, " \t ")]),
    reject("bind_address no port", "bind_address", &[(ENV_BIND_ADDRESS, "127.0.0.1")]),
    reject("bind_address no host", "bind_address", &[(ENV_BIND_ADDRESS, ":8080")]),
    reject("bind_address hostname", "bind_address", &[(ENV_BIND_ADDRESS, "localhost:8080")]),
    reject("bind_address port 0", "bind_address", &[(ENV_BIND_ADDRESS, "127.0.0.1:0")]),
    reject("bind_address port too big", "bind_address", &[(ENV_BIND_ADDRESS, "127.0.0.1:65536")]),
    reject("bind_address bad octet", "bind_address", &[(ENV_BIND_ADDRESS, "256.0.0.1:8080")]),
    reject("bind_address ipv6 unbracketed", "bind_address", &[(ENV_BIND_ADDRESS, "::1:8080")]),
    reject("bind_address url", "bind_address", &[(ENV_BIND_ADDRESS, "http://127.0.0.1:8080")]),
    // max_query_rows
    reject("max_query_rows empty", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "")]),
    reject("max_query_rows zero", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "0")]),
    reject("max_query_rows above limit", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "100001")]),
    reject("max_query_rows negative", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "-10")]),
    reject("max_query_rows float", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "10.5")]),
    reject("max_query_rows scientific", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "1e4")]),
    reject("max_query_rows separator", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "10_000")]),
    reject("max_query_rows text", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "ten")]),
    reject("max_query_rows overflow", "max_query_rows", &[(ENV_MAX_QUERY_ROWS, "99999999999999999999999")]),
    // query_timeout_secs
    reject("query_timeout_secs zero", "query_timeout_secs", &[(ENV_QUERY_TIMEOUT_SECS, "0")]),
    reject("query_timeout_secs above max", "query_timeout_secs", &[(ENV_QUERY_TIMEOUT_SECS, "301")]),
    reject("query_timeout_secs negative", "query_timeout_secs", &[(ENV_QUERY_TIMEOUT_SECS, "-1")]),
    reject("query_timeout_secs with unit", "query_timeout_secs", &[(ENV_QUERY_TIMEOUT_SECS, "30s")]),
    reject("query_timeout_secs empty", "query_timeout_secs", &[(ENV_QUERY_TIMEOUT_SECS, "")]),
    // catalog_refresh_interval_secs
    reject("refresh zero", "catalog_refresh_interval_secs", &[(ENV_CATALOG_REFRESH_INTERVAL_SECS, "0")]),
    reject("refresh above max", "catalog_refresh_interval_secs", &[(ENV_CATALOG_REFRESH_INTERVAL_SECS, "3601")]),
    reject("refresh text", "catalog_refresh_interval_secs", &[(ENV_CATALOG_REFRESH_INTERVAL_SECS, "often")]),
    reject("refresh empty", "catalog_refresh_interval_secs", &[(ENV_CATALOG_REFRESH_INTERVAL_SECS, "")]),
    // log_level
    reject("log_level unknown", "log_level", &[(ENV_LOG_LEVEL, "verbose")]),
    reject("log_level warning alias", "log_level", &[(ENV_LOG_LEVEL, "warning")]),
    reject("log_level filter directive", "log_level", &[(ENV_LOG_LEVEL, "lens=debug")]),
    reject("log_level empty", "log_level", &[(ENV_LOG_LEVEL, "")]),
    // chain
    reject("chain unsupported", "chain", &[(ENV_CHAIN, "ethereum")]),
    reject("chain path traversal", "chain", &[(ENV_CHAIN, "../solana")]),
    reject("chain empty", "chain", &[(ENV_CHAIN, "")]),
    // Well-formed environments, so rejecting everything cannot score 100%.
    accept("only data_dir", &[]),
    accept("every field at its upper bound", &[
        (ENV_MAX_QUERY_ROWS, "100000"),
        (ENV_QUERY_TIMEOUT_SECS, "300"),
        (ENV_CATALOG_REFRESH_INTERVAL_SECS, "3600"),
    ]),
    accept("every field at its lower bound", &[
        (ENV_MAX_QUERY_ROWS, "1"),
        (ENV_QUERY_TIMEOUT_SECS, "1"),
        (ENV_CATALOG_REFRESH_INTERVAL_SECS, "1"),
    ]),
    accept("ipv6 bind address", &[(ENV_BIND_ADDRESS, "[::]:8443")]),
    accept("padded values and mixed case", &[
        (ENV_MAX_QUERY_ROWS, " 250 "),
        (ENV_LOG_LEVEL, "Error"),
        (ENV_CHAIN, "SOLANA"),
    ]),
];

/// Scratch directories for the `DataDir` variants, removed on drop.
struct Fixtures {
    root: PathBuf,
}

impl Fixtures {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("lens-config-load-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("valid").join(DEFAULT_CHAIN).join("instructions")).unwrap();
        fs::create_dir_all(root.join("bare")).unwrap();
        for table in TABLE_DIRS {
            fs::create_dir_all(root.join("flat").join(table)).unwrap();
        }
        fs::write(root.join("file"), b"").unwrap();
        Self { root }
    }

    fn path(&self, data_dir: DataDir) -> Option<PathBuf> {
        let under = |name: &str| Some(self.root.join(name));
        match data_dir {
            DataDir::Valid => under("valid"),
            DataDir::Unset => None,
            DataDir::Literal(value) => Some(PathBuf::from(value)),
            DataDir::Missing => under("missing"),
            DataDir::File => under("file"),
            DataDir::Bare => under("bare"),
            DataDir::FlatLayout => under("flat"),
        }
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Replace every `LENS_*` variable with exactly `data_dir` plus `vars`.
fn set_env(data_dir: Option<&Path>, vars: &[(&str, &str)]) {
    // SAFETY: this test binary holds a single test, so no other thread reads
    // or writes the environment while it runs.
    unsafe {
        for name in ENV_VARS {
            std::env::remove_var(name);
        }
        if let Some(dir) = data_dir {
            std::env::set_var(ENV_DATA_DIR, dir);
        }
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
    }
}

enum Outcome {
    Correct,
    WrongField(String),
    FalseAccept,
    FalseReject(String),
}

fn score(expect: Expect, result: Result<LensConfig, LensError>) -> Outcome {
    match (expect, result) {
        (Expect::Accept, Ok(_)) => Outcome::Correct,
        (Expect::Accept, Err(err)) => Outcome::FalseReject(err.to_string()),
        (Expect::Reject(_), Ok(_)) => Outcome::FalseAccept,
        (Expect::Reject(want), Err(LensError::ConfigError { field, .. })) if field == want => Outcome::Correct,
        (Expect::Reject(_), Err(err)) => Outcome::WrongField(format!("{} ({})", err, err.kind())),
    }
}

#[test]
fn load_rejects_every_malformed_environment_and_accepts_every_valid_one() {
    let fixtures = Fixtures::new();
    let saved: Vec<_> = ENV_VARS.iter().map(|name| (*name, std::env::var_os(name))).collect();

    let (mut correct_rejects, mut correct_accepts) = (0, 0);
    let mut failures = Vec::new();
    for case in CASES {
        set_env(fixtures.path(case.data_dir).as_deref(), case.vars);
        match score(case.expect, LensConfig::load()) {
            Outcome::Correct if case.expect == Expect::Accept => correct_accepts += 1,
            Outcome::Correct => correct_rejects += 1,
            Outcome::WrongField(got) => failures.push(format!("{}: wrong error: {got}", case.name)),
            Outcome::FalseAccept => failures.push(format!("{}: accepted, expected {:?}", case.name, case.expect)),
            Outcome::FalseReject(got) => failures.push(format!("{}: rejected: {got}", case.name)),
        }
    }

    // SAFETY: as in `set_env`.
    unsafe {
        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }

    let malformed = CASES.iter().filter(|case| case.expect != Expect::Accept).count();
    let well_formed = CASES.len() - malformed;
    let correct = correct_rejects + correct_accepts;
    println!(
        "LensConfig::load: {correct}/{} correct ({:.1}%); \
         rejected {correct_rejects}/{malformed} malformed, accepted {correct_accepts}/{well_formed} well-formed",
        CASES.len(),
        100.0 * correct as f64 / CASES.len() as f64,
    );
    assert!(failures.is_empty(), "{} misclassified case(s):\n  {}", failures.len(), failures.join("\n  "));
}
