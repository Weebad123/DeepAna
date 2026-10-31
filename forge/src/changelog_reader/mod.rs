//! The change log reader: tails PRISM's change log and hands its events to
//! the alert engine in file order.
//!
//! [`ChangelogReader`] reads, parses, orders and remembers. It never decides
//! what an event means. PRISM appends one JSON envelope per line:
//!
//! ```text
//! {"id":7,"emitted_at":1761040000,"chain":"solana-devnet","event":"skip","slot":350123457,"reason":"validator_skipped"}
//! ```
//!
//! FORGE does not depend on the prism crate; the two share only the file.
//! [`EnvelopeRecord`] and [`EventRecord`] mirror the wire format.
//!
//! | File              | Holds                                                     |
//! |-------------------|-----------------------------------------------------------|
//! | `mod.rs`          | [`ChangelogReader`] and the wire-format mirror            |
//! | [`checkpoint`]    | [`Checkpoint`], [`CheckpointStore`]: the resume position  |
//!
//! # Reading
//!
//! [`ChangelogReader::poll`] returns the complete lines written since the
//! last call, parsed, in file order. A last line without its `'\n'` is
//! still being written and is left for a later poll. Lines that do not
//! parse, and ids at or below the last one yielded, are skipped and counted
//! in [`ReaderStats`]. An event kind this mirror does not know is yielded as
//! [`EventRecord::Unknown`], so PRISM can add one without breaking FORGE.
//!
//! # Committing and resuming
//!
//! Reading moves an in-memory position only. [`ChangelogReader::commit`]
//! durably records that an event, and everything before it, has been fully
//! processed. After a crash FORGE resumes right after the last committed
//! event: nothing committed is read again, and nothing after it is skipped.
//! Events read but not committed are read again, so processing is at least
//! once. [`ChangelogReader::rewind_to_committed`] does the same without a
//! crash, when processing a batch fails.
//!
//! # Resets
//!
//! On startup the checkpoint is checked against the file: the file must be at
//! least as long as the checkpoint offset, its first line must have the
//! checkpointed fingerprint, and the line ending at the offset must carry the
//! checkpointed id. While running, the reader also notices when the file at
//! the path is replaced or shrinks below the read position. Any of these
//! means the file is not the one the checkpoint describes: the reader counts
//! a reset, saves a checkpoint at the start of the file, and reads the file
//! again from the beginning. The highest committed `Final` slot is kept.

use std::fs::{self, File, Metadata};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::config::{ForgeConfig, ForgeError, ForgeResult};

pub mod checkpoint;

pub use checkpoint::{CHECKPOINT_FILE_NAME, CHECKPOINT_TEMP_SUFFIX, Checkpoint, CheckpointStore};
use checkpoint::unix_now;

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// One line of the change log: an event and the header PRISM stamps on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeRecord {
    /// Position in the log: 1 for the first event, then one more per event.
    pub id: u64,
    /// Unix seconds at which PRISM recorded the event.
    pub emitted_at: i64,
    /// Chain id: `"solana-mainnet"`, `"solana-devnet"`, ...
    pub chain: String,
    /// The event; its fields sit next to the header's on the line.
    #[serde(flatten)]
    pub event: EventRecord,
}

/// The event inside an [`EnvelopeRecord`], told apart by its `"event"` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EventRecord {
    /// A slot was written to Parquet.
    New {
        slot: u64,
        blockhash: String,
        parent_slot: u64,
        /// Unix seconds; `None` for slots without one.
        block_time: Option<i64>,
        transaction_count: u32,
        /// The files written for the slot's batch.
        parquet_paths: Vec<String>,
    },
    /// A slot produced no data.
    Skip {
        slot: u64,
        /// `"validator_skipped"`, `"rpc_unavailable"`, ... Kept as text so a
        /// reason PRISM adds later still parses.
        reason: String,
    },
    /// Every slot at or below `slot` is finalized.
    Final {
        slot: u64,
        blockhash: String,
        /// The `slot` of the previous `Final` (0 for PRISM's first one).
        previous_slot: u64,
    },
    /// An event kind this version of FORGE does not know.
    #[serde(other)]
    Unknown,
}

impl EventRecord {
    /// The slot the event is about; `None` for [`Self::Unknown`].
    pub const fn slot(&self) -> Option<u64> {
        match self {
            Self::New { slot, .. } | Self::Skip { slot, .. } | Self::Final { slot, .. } => Some(*slot),
            Self::Unknown => None,
        }
    }

    /// The `"event"` discriminator; `"unknown"` for [`Self::Unknown`].
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::New { .. } => "new",
            Self::Skip { .. } => "skip",
            Self::Final { .. } => "final",
            Self::Unknown => "unknown",
        }
    }
}

/// An envelope as [`ChangelogReader::poll`] yields it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEvent {
    pub envelope: EnvelopeRecord,
    /// Byte offset just after this line's `'\n'`.
    pub end_offset: u64,
    /// Resets of the reader when the event was read. An event read before a
    /// reset belongs to another file and cannot be committed.
    epoch: u64,
}

/// What a [`ChangelogReader`] has seen since it was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReaderStats {
    /// Events yielded by [`ChangelogReader::poll`], re-reads included.
    pub lines_yielded: u64,
    /// Complete lines that did not parse; skipped.
    pub malformed_lines: u64,
    /// Lines whose id was not above the last yielded id; skipped.
    pub non_monotonic_ids: u64,
    /// Lines yielded with an id more than one above the previous.
    pub id_gaps: u64,
    /// Times the change log was found replaced, truncated or out of step
    /// with the checkpoint, and read again from the start.
    pub resets: u64,
    /// File length minus the read position, at the last poll.
    pub bytes_behind: u64,
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Tails the change log at one path. See the [module docs](self).
#[derive(Debug)]
pub struct ChangelogReader {
    path: PathBuf,
    store: CheckpointStore,
    /// The last durable position.
    committed: Checkpoint,
    /// Where the next poll starts; never below `committed.byte_offset`.
    read_offset: u64,
    last_yielded_id: u64,
    /// Highest `Final` slot read so far, committed or not.
    highest_final_slot: u64,
    stats: ReaderStats,
    /// The open change log; `None` until the file exists.
    file: Option<File>,
    /// Identity of `file`, to notice a new file at the same path.
    identity: Option<FileId>,
    /// Fingerprint of `file`'s first line, once it has one.
    fingerprint: Option<String>,
    /// Bumped on every reset; see [`ReadEvent`].
    epoch: u64,
    /// `(end_offset, slot)` of each `Final` read but not yet committed.
    pending_finals: Vec<(u64, u64)>,
}

impl ChangelogReader {
    /// Open the change log at [`ForgeConfig::changelog_path`], resuming from
    /// `store`'s checkpoint. See [`Self::open_path`].
    pub fn open(config: &ForgeConfig, store: CheckpointStore) -> ForgeResult<Self> {
        Self::open_path(config.changelog_path(), store)
    }

    /// Load the checkpoint, check it against the change log at `path` and
    /// position the reader right after the last committed event.
    ///
    /// The change log need not exist yet; it is picked up by a later poll
    /// and checked then. A checkpoint that does not fit the file triggers a
    /// reset (see the [module docs](self)). Fails when the checkpoint is
    /// unreadable ([`CheckpointStore::load`]) or a reset cannot be saved.
    pub fn open_path(path: impl Into<PathBuf>, store: CheckpointStore) -> ForgeResult<Self> {
        let path = path.into();
        if path == store.path() {
            return Err(ForgeError::changelog(
                path.display().to_string(),
                "the checkpoint file cannot be the change log itself",
            ));
        }
        let committed = store.load()?.unwrap_or_default();
        let mut reader = Self {
            path,
            store,
            read_offset: committed.byte_offset,
            last_yielded_id: committed.last_id,
            highest_final_slot: committed.highest_final_slot,
            committed,
            stats: ReaderStats::default(),
            file: None,
            identity: None,
            fingerprint: None,
            epoch: 0,
            pending_finals: Vec::new(),
        };
        reader.attach()?;
        info!(
            path = %reader.path.display(),
            checkpoint = %reader.store.path().display(),
            resume_after_id = reader.committed.last_id,
            byte_offset = reader.committed.byte_offset,
            "change log reader positioned"
        );
        Ok(reader)
    }

    /// Up to `max` complete events written since the last poll, in file
    /// order. Never waits: returns an empty batch when nothing new is there.
    ///
    /// Notices a replaced or truncated file first (a reset).
    pub fn poll(&mut self, max: usize) -> ForgeResult<Vec<ReadEvent>> {
        self.refresh_file()?;
        let Some(file) = self.file.take() else {
            self.stats.bytes_behind = 0;
            return Ok(Vec::new());
        };
        let result = self.read_from(&file, max);
        self.file = Some(file);
        result
    }

    /// Durably record that `event` and everything before it are fully
    /// processed. After a crash, reading resumes right after it.
    ///
    /// Committing an event at or before the committed position does nothing.
    /// Fails, changing nothing, when `event` was read before a reset or lies
    /// past the read position (it was rewound over and not read again), or
    /// when the checkpoint cannot be saved.
    pub fn commit(&mut self, event: &ReadEvent) -> ForgeResult<()> {
        if event.epoch != self.epoch {
            return Err(self.error(format!(
                "event {} was read before the change log was reset and cannot be committed",
                event.envelope.id
            )));
        }
        if event.end_offset <= self.committed.byte_offset {
            return Ok(());
        }
        if event.end_offset > self.read_offset {
            return Err(self.error(format!(
                "event {} ends at offset {}, past the read position {}",
                event.envelope.id, event.end_offset, self.read_offset
            )));
        }
        let first_line_fingerprint = self
            .fingerprint
            .clone()
            .ok_or_else(|| self.error("the first line has not been read"))?;
        let highest_final_slot = self
            .pending_finals
            .iter()
            .filter(|(end, _)| *end <= event.end_offset)
            .map(|(_, slot)| *slot)
            .fold(self.committed.highest_final_slot, u64::max);
        let checkpoint = Checkpoint {
            last_id: event.envelope.id,
            byte_offset: event.end_offset,
            first_line_fingerprint,
            highest_final_slot,
            updated_at: unix_now(),
        };
        self.store.save(&checkpoint)?;
        self.pending_finals.retain(|(end, _)| *end > event.end_offset);
        self.committed = checkpoint;
        Ok(())
    }

    /// Move the read position back to the last commit, so the next poll
    /// yields the uncommitted events again. For when processing them failed.
    pub fn rewind_to_committed(&mut self) {
        self.read_offset = self.committed.byte_offset;
        self.last_yielded_id = self.committed.last_id;
        self.highest_final_slot = self.committed.highest_final_slot;
        self.pending_finals.clear();
    }

    /// Highest `Final` slot among committed events.
    pub const fn highest_final_slot(&self) -> u64 {
        self.committed.highest_final_slot
    }

    /// Highest `Final` slot among events read so far, committed or not.
    pub const fn highest_read_final_slot(&self) -> u64 {
        self.highest_final_slot
    }

    pub const fn stats(&self) -> ReaderStats {
        self.stats
    }

    /// The last durable position.
    pub const fn committed(&self) -> &Checkpoint {
        &self.committed
    }

    /// Where the next poll starts reading.
    pub const fn read_offset(&self) -> u64 {
        self.read_offset
    }

    pub const fn last_yielded_id(&self) -> u64 {
        self.last_yielded_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn store(&self) -> &CheckpointStore {
        &self.store
    }

    // -- file tracking -------------------------------------------------------

    /// Open the change log if it exists and check the committed position
    /// against it, resetting when they do not fit.
    fn attach(&mut self) -> ForgeResult<()> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(self.io(err)),
        };
        let metadata = file.metadata().map_err(|err| self.io(err))?;
        self.fingerprint = first_line_fingerprint(&file).map_err(|err| self.io(err))?;
        let mismatch = self.position_mismatch(&file, metadata.len()).map_err(|err| self.io(err))?;
        if let Some(reason) = mismatch {
            self.reset(&reason)?;
        }
        self.identity = file_id(&metadata);
        self.file = Some(file);
        Ok(())
    }

    /// Why the committed position cannot be right for `file`, if so.
    fn position_mismatch(&self, file: &File, len: u64) -> io::Result<Option<String>> {
        let offset = self.committed.byte_offset;
        if offset == 0 {
            return Ok(None);
        }
        if len < offset {
            return Ok(Some(format!("the file is {len} bytes, shorter than the checkpoint offset {offset}")));
        }
        if self.fingerprint.as_deref() != Some(self.committed.first_line_fingerprint.as_str()) {
            return Ok(Some("its first line is not the checkpointed one: the file was replaced".to_owned()));
        }
        let Some(line) = line_ending_at(file, offset)? else {
            return Ok(Some(format!("the checkpoint offset {offset} is not at the end of a line")));
        };
        Ok(match serde_json::from_slice::<EnvelopeRecord>(&line) {
            Ok(envelope) if envelope.id == self.committed.last_id => None,
            Ok(envelope) => Some(format!(
                "the line ending at offset {offset} has id {}, the checkpoint says {}",
                envelope.id, self.committed.last_id
            )),
            Err(err) => Some(format!("the line ending at offset {offset} does not parse: {err}")),
        })
    }

    /// Pick up the file once it exists, and notice when another file has
    /// taken its path. A path that disappears keeps the open file in use.
    fn refresh_file(&mut self) -> ForgeResult<()> {
        let metadata = match fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(self.io(err)),
        };
        if self.file.is_none() {
            return self.attach();
        }
        if file_id(&metadata) != self.identity {
            self.reset("a new file replaced it")?;
            self.file = None;
            return self.attach();
        }
        Ok(())
    }

    /// Start over at the beginning of the file, durably. On error nothing
    /// has changed.
    fn reset(&mut self, reason: &str) -> ForgeResult<()> {
        warn!(
            path = %self.path.display(),
            reason,
            committed_id = self.committed.last_id,
            "change log reset; reading it again from the start"
        );
        let fresh = Checkpoint {
            highest_final_slot: self.committed.highest_final_slot,
            updated_at: unix_now(),
            ..Checkpoint::default()
        };
        self.store.save(&fresh)?;
        self.committed = fresh;
        self.read_offset = 0;
        self.last_yielded_id = 0;
        self.highest_final_slot = self.committed.highest_final_slot;
        self.pending_finals.clear();
        self.epoch += 1;
        self.stats.resets += 1;
        Ok(())
    }

    // -- reading -------------------------------------------------------------

    fn read_from(&mut self, file: &File, max: usize) -> ForgeResult<Vec<ReadEvent>> {
        let len = file.metadata().map_err(|err| self.io(err))?.len();
        if len < self.read_offset {
            let reason = format!("it shrank to {len} bytes, below the read position {}", self.read_offset);
            self.reset(&reason)?;
            self.fingerprint = None;
        }
        let mut events = Vec::new();
        if max > 0 && len > self.read_offset {
            self.read_lines(file, len, max, &mut events).map_err(|err| self.io(err))?;
        }
        self.stats.bytes_behind = len - self.read_offset;
        Ok(events)
    }

    /// Read complete lines between the read position and `len` until `max`
    /// events are in `events`.
    fn read_lines(&mut self, mut file: &File, len: u64, max: usize, events: &mut Vec<ReadEvent>) -> io::Result<()> {
        file.seek(SeekFrom::Start(self.read_offset))?;
        let mut lines = BufReader::new(file.take(len - self.read_offset));
        let mut line = Vec::new();
        while events.len() < max {
            line.clear();
            let read = lines.read_until(b'\n', &mut line)?;
            if read == 0 || line.last() != Some(&b'\n') {
                // Nothing more, or a line PRISM is still writing.
                break;
            }
            let start = self.read_offset;
            self.read_offset += read as u64;
            let body = &line[..read - 1];
            if start == 0 {
                self.fingerprint = Some(fingerprint(body));
            }
            if let Some(event) = self.accept(body, start) {
                events.push(event);
            }
        }
        Ok(())
    }

    /// Parse the line that started at `start` (and ends at the read
    /// position) and check its id; `None` for a line that is skipped.
    fn accept(&mut self, body: &[u8], start: u64) -> Option<ReadEvent> {
        let envelope = match serde_json::from_slice::<EnvelopeRecord>(body) {
            Ok(envelope) => envelope,
            Err(err) => {
                self.stats.malformed_lines += 1;
                warn!(path = %self.path.display(), offset = start, error = %err, "skipping a malformed change log line");
                return None;
            }
        };
        if envelope.id <= self.last_yielded_id {
            self.stats.non_monotonic_ids += 1;
            warn!(
                path = %self.path.display(),
                offset = start,
                id = envelope.id,
                last_id = self.last_yielded_id,
                "skipping a change log line whose id does not move forward"
            );
            return None;
        }
        if envelope.id != self.last_yielded_id + 1 {
            self.stats.id_gaps += 1;
            warn!(
                path = %self.path.display(),
                offset = start,
                id = envelope.id,
                last_id = self.last_yielded_id,
                "change log ids skip ahead"
            );
        }
        self.last_yielded_id = envelope.id;
        if let EventRecord::Final { slot, .. } = envelope.event {
            self.highest_final_slot = self.highest_final_slot.max(slot);
            self.pending_finals.push((self.read_offset, slot));
        }
        self.stats.lines_yielded += 1;
        Some(ReadEvent {
            envelope,
            end_offset: self.read_offset,
            epoch: self.epoch,
        })
    }

    fn error(&self, reason: impl Into<String>) -> ForgeError {
        ForgeError::changelog(self.path.display().to_string(), reason)
    }

    fn io(&self, err: io::Error) -> ForgeError {
        ForgeError::io(self.path.display().to_string(), err)
    }
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

/// SHA-256, lowercase hex, of a line without its `'\n'`.
fn fingerprint(line: &[u8]) -> String {
    Sha256::digest(line).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Fingerprint of `file`'s first line, if the file has a complete one.
fn first_line_fingerprint(mut file: &File) -> io::Result<Option<String>> {
    file.seek(SeekFrom::Start(0))?;
    let mut line = Vec::new();
    BufReader::new(file).read_until(b'\n', &mut line)?;
    Ok(line.strip_suffix(b"\n").map(fingerprint))
}

/// The line that ends with the `'\n'` just before `offset`, without it;
/// `None` when the byte before `offset` is not a `'\n'`.
fn line_ending_at(mut file: &File, offset: u64) -> io::Result<Option<Vec<u8>>> {
    let mut window = offset.min(4096);
    loop {
        let start = offset - window;
        let mut buf = vec![0; window as usize];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf)?;
        let Some(body) = buf.strip_suffix(b"\n") else {
            return Ok(None);
        };
        match body.iter().rposition(|&byte| byte == b'\n') {
            Some(newline) => return Ok(Some(body[newline + 1..].to_vec())),
            None if start == 0 => return Ok(Some(body.to_vec())),
            None => window = (window * 2).min(offset),
        }
    }
}

/// What identifies a file independently of its path.
#[cfg(unix)]
type FileId = (u64, u64);

#[cfg(unix)]
fn file_id(metadata: &Metadata) -> Option<FileId> {
    use std::os::unix::fs::MetadataExt;
    Some((metadata.dev(), metadata.ino()))
}

/// Without inode numbers, only truncation and the startup checks detect a
/// replaced file.
#[cfg(not(unix))]
type FileId = ();

#[cfg(not(unix))]
fn file_id(_metadata: &Metadata) -> Option<FileId> {
    None
}
