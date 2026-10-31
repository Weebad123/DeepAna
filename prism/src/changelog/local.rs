//! [`LocalChangeLog`]: the change log as an append-only JSON-lines file.
//!
//! The file lives at `{data_dir}/changelog.jsonl`, next to the cursor and
//! the skip log, and holds one [`EventEnvelope`] per line:
//!
//! ```text
//! {"id":1,"emitted_at":1761040000,"chain":"solana-devnet","event":"skip","slot":350123457,"reason":"validator_skipped"}
//! {"id":2,"emitted_at":1761040003,"chain":"solana-devnet","event":"new","slot":350123458,...}
//! ```
//!
//! Guarantees, which FORGE relies on when it tails the file:
//!
//! * **Gap-free, ordered ids.** Ids are assigned and the line written under
//!   one lock, so the file's lines carry `1, 2, 3, ...` in order whatever
//!   the number of producers. The id only moves on after a complete write,
//!   and on open it resumes after the last complete line.
//! * **Whole lines only.** Each line goes out in a single unbuffered
//!   `write_all`. A crash mid-write can only leave a partial last line with
//!   no newline: [`LocalChangeLog::open`] cuts it off. Any other line that
//!   does not parse, skips an id or names another chain makes `open` fail
//!   rather than guess.
//! * **One writer.** The file is exclusively locked for as long as the log
//!   is open, so a second PRISM on the same data directory fails to start.
//! * **Fail stop.** After a failed write or sync the log is poisoned: the
//!   partial line is cut off and every later emit and flush fails with a
//!   [`PrismError::ChangeLogError`] naming that first failure, whichever
//!   producer it reaches. A failed `fsync` leaves the page cache
//!   in an unknown state, so only a reopen (which re-reads the file) is
//!   trusted to continue.
//!
//! An emitted event survives a crash of the process at once; it survives a
//! power failure once [`ChangeLog::flush`] has returned.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use tracing::{error, info, warn};

use super::ChangeLog;
use crate::types::error::{BoxError, PrismError, PrismResult};
use crate::types::event::{ChangeEvent, EventEnvelope};

/// Name of the change log file [`LocalChangeLog::open`] uses in the data
/// directory.
pub const CHANGELOG_FILE_NAME: &str = "changelog.jsonl";

/// What a [`LocalChangeLog`] has recorded since it was opened. Events
/// recovered from the file on open are not counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChangeLogStats {
    pub new_events: u64,
    pub skip_events: u64,
    pub final_events: u64,
    /// `emitted_at` of the last recorded event; `None` before the first.
    pub last_emit_at: Option<i64>,
    /// Bytes appended to the file, newlines included.
    pub bytes_written: u64,
}

impl ChangeLogStats {
    /// Events of every kind.
    pub const fn total_events(&self) -> u64 {
        self.new_events + self.skip_events + self.final_events
    }

    fn record(&mut self, event: &ChangeEvent, emitted_at: i64, bytes: u64) {
        match event {
            ChangeEvent::New { .. } => self.new_events += 1,
            ChangeEvent::Skip { .. } => self.skip_events += 1,
            ChangeEvent::Final { .. } => self.final_events += 1,
        }
        self.last_emit_at = Some(emitted_at);
        self.bytes_written += bytes;
    }
}

/// The file-backed [`ChangeLog`]. Share it behind an `Arc`.
pub struct LocalChangeLog {
    path: PathBuf,
    chain: String,
    inner: Mutex<Inner>,
}

struct Inner {
    /// Opened read + append; exclusively locked until dropped.
    file: File,
    /// Id the next recorded event gets.
    next_id: u64,
    /// Length of the known-good contents: whole lines only. A failed write
    /// is cut back to this.
    len: u64,
    /// Set by a failed write or sync, to what failed; refuses every later
    /// call, naming it.
    poisoned: Option<String>,
    stats: ChangeLogStats,
}

impl LocalChangeLog {
    /// Open (or create) `{data_dir}/changelog.jsonl` for events about `chain`,
    /// a chain id such as `"solana-devnet"`.
    ///
    /// Creates `data_dir` if needed, takes the file's exclusive lock and
    /// reads the file through: numbering resumes after the last complete
    /// line, and a partial last line left by a crash is removed. Fails with
    /// [`PrismError::ChangeLogError`] when another process holds the lock or
    /// when a complete line is unreadable, out of sequence or recorded for
    /// another chain, and with [`PrismError::ConfigError`] when `chain` is
    /// empty.
    pub fn open(data_dir: impl AsRef<Path>, chain: impl Into<String>) -> PrismResult<Self> {
        let chain = chain.into();
        if chain.trim().is_empty() {
            return Err(PrismError::config("chain", "must not be empty"));
        }
        let data_dir = data_dir.as_ref();
        let path = data_dir.join(CHANGELOG_FILE_NAME);
        let fail = |operation: &str, source: BoxError| {
            PrismError::changelog(path.display().to_string(), operation, source)
        };

        fs::create_dir_all(data_dir).map_err(|err| fail("create its directory", err.into()))?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .map_err(|err| fail("open", err.into()))?;
        let metadata = file.metadata().map_err(|err| fail("open", err.into()))?;
        if !metadata.is_file() {
            return Err(fail("open", "not a regular file".into()));
        }
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(fail(
                    "lock",
                    "another process holds it (is a second PRISM running on this data directory?)"
                        .into(),
                ));
            }
            Err(TryLockError::Error(err)) => return Err(fail("lock", err.into())),
        }

        let recovered = recover(&file, &chain).map_err(|(operation, source)| fail(&operation, source))?;
        if recovered.torn_bytes > 0 {
            warn!(
                path = %path.display(),
                bytes = recovered.torn_bytes,
                last_id = recovered.last_id,
                "change log ends in a partial record; cutting it off"
            );
            file.set_len(recovered.len)
                .and_then(|()| file.sync_data())
                .map_err(|err| fail("cut off a partial record", err.into()))?;
        }
        info!(
            path = %path.display(),
            chain = %chain,
            next_id = recovered.last_id + 1,
            "change log opened"
        );
        Ok(Self {
            path,
            chain,
            inner: Mutex::new(Inner {
                file,
                next_id: recovered.last_id + 1,
                len: recovered.len,
                poisoned: None,
                stats: ChangeLogStats::default(),
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Id the next recorded event will get.
    pub fn next_id(&self) -> u64 {
        self.lock().next_id
    }

    /// Whether a failed write or sync has stopped the log.
    pub fn is_poisoned(&self) -> bool {
        self.lock().poisoned.is_some()
    }

    pub fn stats(&self) -> ChangeLogStats {
        self.lock().stats
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every update to `Inner` is a plain field write after the I/O it
        // records, so a panic cannot leave it half-updated.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn error(&self, operation: impl Into<String>, source: impl Into<BoxError>) -> PrismError {
        PrismError::changelog(self.path.display().to_string(), operation, source)
    }

    fn poisoned_error(&self, operation: &str, cause: &str) -> PrismError {
        self.error(
            operation,
            format!("stopped by an earlier failure ({cause}); restart PRISM once it is fixed"),
        )
    }
}

impl ChangeLog for LocalChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        let mut inner = self.lock();
        if let Some(cause) = &inner.poisoned {
            return Err(self.poisoned_error("append", cause));
        }
        let id = inner.next_id;
        let envelope = EventEnvelope {
            id,
            emitted_at: chrono::Utc::now().timestamp(),
            chain: self.chain.clone(),
            event,
        };
        let mut line = envelope.to_json();
        line.push('\n');

        if let Err(err) = (&inner.file).write_all(line.as_bytes()) {
            inner.poisoned = Some(format!("append event {id}: {err}"));
            // Leave whole lines only. Should this fail too, `open` cuts the
            // partial line off on the next start.
            if let Err(cut) = inner.file.set_len(inner.len) {
                error!(path = %self.path.display(), error = %cut, "could not cut off a partial record");
            }
            error!(path = %self.path.display(), id, error = %err, "change log write failed; log stopped");
            return Err(self.error(format!("append event {id}"), err));
        }
        let bytes = line.len() as u64;
        inner.len += bytes;
        inner.next_id += 1;
        inner.stats.record(&envelope.event, envelope.emitted_at, bytes);
        Ok(id)
    }

    fn flush(&self) -> PrismResult<()> {
        let mut inner = self.lock();
        if let Some(cause) = &inner.poisoned {
            return Err(self.poisoned_error("sync", cause));
        }
        if let Err(err) = inner.file.sync_data() {
            inner.poisoned = Some(format!("sync: {err}"));
            error!(path = %self.path.display(), error = %err, "change log sync failed; log stopped");
            return Err(self.error("sync", err));
        }
        Ok(())
    }
}

impl std::fmt::Debug for LocalChangeLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("LocalChangeLog")
            .field("path", &self.path)
            .field("chain", &self.chain)
            .field("next_id", &inner.next_id)
            .field("poisoned", &inner.poisoned)
            .field("stats", &inner.stats)
            .finish_non_exhaustive()
    }
}

/// What [`recover`] found in an existing file.
struct Recovered {
    /// Id of the last complete line; 0 for an empty file.
    last_id: u64,
    /// Bytes up to and including the last complete line.
    len: u64,
    /// Bytes after it: a partial record left by a crash.
    torn_bytes: u64,
}

/// Read the file through, checking that every complete line is an envelope
/// for `chain` with the next id. Errors carry the operation for the message.
fn recover(file: &File, chain: &str) -> Result<Recovered, (String, BoxError)> {
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut recovered = Recovered {
        last_id: 0,
        len: 0,
        torn_bytes: 0,
    };
    for number in 1u64.. {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|err| ("read".to_owned(), BoxError::from(err)))?;
        if read == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            recovered.torn_bytes = read as u64;
            break;
        }
        let operation = || format!("recover line {number}");
        let envelope: EventEnvelope = serde_json::from_slice(&line[..read - 1])
            .map_err(|err| (operation(), BoxError::from(err)))?;
        let expected = recovered.last_id + 1;
        if envelope.id != expected {
            return Err((operation(), invalid(format!("expected id {expected}, found {}", envelope.id))));
        }
        if envelope.chain != chain {
            return Err((
                operation(),
                invalid(format!("recorded for chain {:?}, not {chain:?}", envelope.chain)),
            ));
        }
        recovered.last_id = envelope.id;
        recovered.len += read as u64;
    }
    Ok(recovered)
}

fn invalid(reason: String) -> BoxError {
    io::Error::new(io::ErrorKind::InvalidData, reason).into()
}
