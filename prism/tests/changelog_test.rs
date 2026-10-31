//! Tests for `prism::changelog`.
//!
//! Every test opens a [`LocalChangeLog`] under a fresh temp directory and
//! reads the file back. The `ids_are_gap_free_*` tests check the log's core
//! promise from every angle: whatever the number of concurrent writers, and
//! however the process stops (drop, crash mid-write, `SIGKILL`, `abort`,
//! `exit`, a failing disk), the file's ids run `1, 2, 3, ...` in order with
//! no gap, and the next open carries on from there.
//!
//! The process-level tests re-run this test binary as a child (the ignored
//! [`crash_child`] test) and stop it from outside. The disk-full test runs
//! the child under `ulimit -f`, so its writes really fail with `EFBIG`.

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use prism::changelog::{CHANGELOG_FILE_NAME, ChangeLog, ChangeLogStats, LocalChangeLog};
use prism::types::error::PrismError;
use prism::types::event::{ChangeEvent, EventEnvelope, SkipReason};

const CHAIN: &str = "solana-devnet";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, unique, not yet existing directory under the system temp dir.
fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-changelog-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join(CHANGELOG_FILE_NAME)
}

fn open(dir: &Path) -> LocalChangeLog {
    LocalChangeLog::open(dir, CHAIN).expect("change log opens")
}

/// The `seq`th event of writer `writer`. Its slot encodes both, so every
/// event is distinct and can be traced back to who emitted it.
fn event(writer: usize, seq: usize) -> ChangeEvent {
    let slot = (writer * 1_000_000 + seq) as u64;
    match seq % 3 {
        0 => ChangeEvent::New {
            slot,
            blockhash: format!("hash-{slot}"),
            parent_slot: slot.saturating_sub(1),
            block_time: Some(1_700_000_000),
            transaction_count: 3,
            parquet_paths: vec![format!("/data/solana/slots/date=2026-10-31/slots_{slot}_{slot}.parquet")],
        },
        1 => ChangeEvent::skip(slot, SkipReason::ValidatorSkipped),
        _ => ChangeEvent::finalized(slot, format!("hash-{slot}"), slot.saturating_sub(32)),
    }
}

/// Every line of the file, parsed. Panics on a line that does not parse or
/// on a partial last line: the file must hold whole envelopes only.
fn read_envelopes(dir: &Path) -> Vec<EventEnvelope> {
    let contents = fs::read_to_string(log_path(dir)).expect("change log is readable");
    assert!(
        contents.is_empty() || contents.ends_with('\n'),
        "the file ends in a partial line"
    );
    contents
        .lines()
        .map(|line| EventEnvelope::from_json(line).unwrap_or_else(|err| panic!("{err}: {line}")))
        .collect()
}

/// The ids of `envelopes` are exactly `1..=len`, in file order.
#[track_caller]
fn assert_gap_free(envelopes: &[EventEnvelope]) {
    for (index, envelope) in envelopes.iter().enumerate() {
        assert_eq!(envelope.id, index as u64 + 1, "line {} has the wrong id", index + 1);
        assert_eq!(envelope.chain, CHAIN);
    }
}

#[track_caller]
fn assert_changelog_error(err: &PrismError, operation: &str) {
    let PrismError::ChangeLogError { operation: actual, .. } = err else {
        panic!("expected ChangeLogError, got {err:?}");
    };
    assert!(actual.contains(operation), "operation {actual:?}, expected {operation:?}: {err}");
}

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

#[test]
fn open_creates_the_data_dir_and_an_empty_file_named_like_forge_expects() {
    let dir = temp_dir().join("nested").join("data");
    let log = open(&dir);
    assert_eq!(CHANGELOG_FILE_NAME, "changelog.jsonl");
    assert_eq!(log.path(), log_path(&dir));
    assert_eq!(log.chain(), CHAIN);
    assert!(log.path().is_file());
    assert_eq!(fs::metadata(log.path()).unwrap().len(), 0);
    assert_eq!(log.next_id(), 1);
    assert!(!log.is_poisoned());
    assert_eq!(log.stats(), ChangeLogStats::default());
}

#[test]
fn an_empty_chain_is_a_config_error() {
    for chain in ["", "  "] {
        let err = LocalChangeLog::open(temp_dir(), chain).unwrap_err();
        assert!(matches!(&err, PrismError::ConfigError { field, .. } if field == "chain"), "{err:?}");
    }
}

#[test]
fn a_data_dir_that_is_a_file_is_a_changelog_error() {
    let file = temp_dir();
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(&file, b"not a directory").unwrap();
    let err = LocalChangeLog::open(&file, CHAIN).unwrap_err();
    assert_eq!(err.kind(), "changelog_error");
    assert!(!err.is_retryable());
    let _ = fs::remove_file(&file);
}

#[test]
fn emit_returns_ids_from_one_and_writes_one_envelope_per_line() {
    let dir = temp_dir();
    let log = open(&dir);
    let before = chrono::Utc::now().timestamp();
    let ids: Vec<u64> = (0..3).map(|seq| log.emit(event(0, seq)).unwrap()).collect();
    let after = chrono::Utc::now().timestamp();
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(log.next_id(), 4);

    let envelopes = read_envelopes(&dir);
    assert_gap_free(&envelopes);
    for (seq, envelope) in envelopes.iter().enumerate() {
        assert_eq!(envelope.event, event(0, seq));
        assert!((before..=after).contains(&envelope.emitted_at), "{}", envelope.emitted_at);
    }
    // The wire format: envelope header, then the event's own fields.
    let first = fs::read_to_string(log.path()).unwrap().lines().next().unwrap().to_owned();
    assert!(first.starts_with(r#"{"id":1,"emitted_at":"#), "{first}");
    assert!(first.contains(r#","chain":"solana-devnet","event":"new","slot":0,"#), "{first}");
}

#[test]
fn stats_count_each_variant_and_every_byte() {
    let dir = temp_dir();
    let log = open(&dir);
    for seq in 0..7 {
        log.emit(event(0, seq)).unwrap();
    }
    log.flush().unwrap();
    let stats = log.stats();
    assert_eq!((stats.new_events, stats.skip_events, stats.final_events), (3, 2, 2));
    assert_eq!(stats.total_events(), 7);
    assert_eq!(stats.bytes_written, fs::metadata(log.path()).unwrap().len());
    let last = read_envelopes(&dir).last().unwrap().emitted_at;
    assert_eq!(stats.last_emit_at, Some(last));
}

#[test]
fn flush_succeeds_with_and_without_pending_events() {
    let dir = temp_dir();
    let log = open(&dir);
    log.flush().unwrap();
    log.emit(event(0, 0)).unwrap();
    log.flush().unwrap();
    log.flush().unwrap();
    assert_eq!(read_envelopes(&dir).len(), 1);
}

#[test]
fn works_as_a_shared_trait_object() {
    let dir = temp_dir();
    let log: Arc<dyn ChangeLog> = Arc::new(open(&dir));
    let clone = Arc::clone(&log);
    assert_eq!(log.emit(event(0, 0)).unwrap(), 1);
    assert_eq!(clone.emit(event(0, 1)).unwrap(), 2);
    clone.flush().unwrap();
    assert_gap_free(&read_envelopes(&dir));
}

// ---------------------------------------------------------------------------
// Exclusive lock
// ---------------------------------------------------------------------------

#[test]
fn a_second_open_of_the_same_file_fails_until_the_first_is_dropped() {
    let dir = temp_dir();
    let first = open(&dir);
    first.emit(event(0, 0)).unwrap();

    let err = LocalChangeLog::open(&dir, CHAIN).unwrap_err();
    assert_changelog_error(&err, "lock");
    assert!(err.to_string().contains("another process"), "{err}");
    assert_eq!(read_envelopes(&dir).len(), 1, "the failed open touched nothing");

    drop(first);
    let second = open(&dir);
    assert_eq!(second.next_id(), 2);
}

// ---------------------------------------------------------------------------
// Recovery on open
// ---------------------------------------------------------------------------

/// Write `lines` (each given without its newline) as the change log file.
fn write_lines(dir: &Path, lines: &[String]) -> Vec<u8> {
    fs::create_dir_all(dir).unwrap();
    let contents: String = lines.iter().map(|line| format!("{line}\n")).collect();
    fs::write(log_path(dir), &contents).unwrap();
    contents.into_bytes()
}

fn envelope_line(id: u64, chain: &str) -> String {
    EventEnvelope {
        id,
        emitted_at: 1_761_040_000,
        chain: chain.into(),
        event: event(0, id as usize),
    }
    .to_json()
}

#[test]
fn a_complete_file_is_resumed_after_its_last_id() {
    let dir = temp_dir();
    write_lines(&dir, &[envelope_line(1, CHAIN), envelope_line(2, CHAIN)]);
    let log = open(&dir);
    assert_eq!(log.next_id(), 3);
    assert_eq!(log.emit(event(0, 3)).unwrap(), 3);
    assert_gap_free(&read_envelopes(&dir));
    assert_eq!(log.stats().total_events(), 1, "recovered events are not counted");
}

#[test]
fn a_file_that_is_wrong_beyond_a_torn_tail_is_refused_and_left_untouched() {
    let cases: [(&str, Vec<String>, &str); 6] = [
        ("garbage line", vec![envelope_line(1, CHAIN), "not json".into(), envelope_line(2, CHAIN)], "recover line 2"),
        ("empty line", vec![envelope_line(1, CHAIN), String::new()], "recover line 2"),
        ("first id is not 1", vec![envelope_line(2, CHAIN)], "recover line 1"),
        ("gap", vec![envelope_line(1, CHAIN), envelope_line(2, CHAIN), envelope_line(4, CHAIN)], "recover line 3"),
        ("duplicate", vec![envelope_line(1, CHAIN), envelope_line(1, CHAIN)], "recover line 2"),
        ("other chain", vec![envelope_line(1, CHAIN), envelope_line(2, "solana-mainnet")], "recover line 2"),
    ];
    for (name, lines, operation) in cases {
        let dir = temp_dir();
        let before = write_lines(&dir, &lines);
        let err = LocalChangeLog::open(&dir, CHAIN).expect_err(name);
        assert_changelog_error(&err, operation);
        assert_eq!(fs::read(log_path(&dir)).unwrap(), before, "{name}: file changed");
    }
}

#[test]
fn a_bare_event_without_an_envelope_is_refused() {
    let dir = temp_dir();
    write_lines(&dir, &[event(0, 0).to_json()]);
    assert_changelog_error(&LocalChangeLog::open(&dir, CHAIN).unwrap_err(), "recover line 1");
}

// ---------------------------------------------------------------------------
// Gap-free ids: any number of writers
// ---------------------------------------------------------------------------

/// `writers` threads emit `per_writer` events each, all at once, flushing
/// now and then. Returns the ids each writer got back, in its own order.
fn emit_concurrently(log: &Arc<LocalChangeLog>, writers: usize, per_writer: usize) -> Vec<Vec<u64>> {
    let start = Arc::new(Barrier::new(writers));
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let log = Arc::clone(log);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                (0..per_writer)
                    .map(|seq| {
                        let id = log.emit(event(writer, seq)).unwrap();
                        if seq % 37 == 0 {
                            log.flush().unwrap();
                        }
                        id
                    })
                    .collect::<Vec<u64>>()
            })
        })
        .collect();
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

/// The file after `ids` were handed out: gap-free, and each id is on the
/// line holding the event its writer emitted.
#[track_caller]
fn assert_ids_match_the_file(dir: &Path, ids: &[Vec<u64>], first_id: u64) {
    let envelopes = read_envelopes(dir);
    assert_gap_free(&envelopes);
    let mut handed_out: Vec<u64> = ids.iter().flatten().copied().collect();
    handed_out.sort_unstable();
    let expected: Vec<u64> = (first_id..first_id + handed_out.len() as u64).collect();
    assert_eq!(handed_out, expected, "every id handed out once, none skipped");
    for (writer, writer_ids) in ids.iter().enumerate() {
        assert!(writer_ids.windows(2).all(|w| w[0] < w[1]), "writer {writer}: ids not increasing");
        for (seq, id) in writer_ids.iter().enumerate() {
            assert_eq!(envelopes[*id as usize - 1].event, event(writer, seq), "writer {writer}, id {id}");
        }
    }
}

#[test]
fn ids_are_gap_free_and_in_file_order_for_any_number_of_writers() {
    for writers in [1, 2, 4, 16, 64] {
        let dir = temp_dir();
        let log = Arc::new(open(&dir));
        let per_writer = 4_096 / writers;
        let ids = emit_concurrently(&log, writers, per_writer);
        assert_ids_match_the_file(&dir, &ids, 1);
        assert_eq!(log.next_id(), (writers * per_writer) as u64 + 1, "{writers} writers");
        let stats = log.stats();
        assert_eq!(stats.total_events(), (writers * per_writer) as u64);
        assert_eq!(stats.bytes_written, fs::metadata(log.path()).unwrap().len());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ids_are_gap_free_across_tokio_tasks_sharing_a_trait_object() {
    let dir = temp_dir();
    let log: Arc<dyn ChangeLog> = Arc::new(open(&dir));
    let tasks: Vec<_> = (0..32)
        .map(|writer| {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut ids = Vec::new();
                for seq in 0..100 {
                    ids.push(log.emit(event(writer, seq)).unwrap());
                    if seq % 10 == 0 {
                        tokio::task::yield_now().await;
                        log.flush().unwrap();
                    }
                }
                ids
            })
        })
        .collect();
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap());
    }
    assert_ids_match_the_file(&dir, &ids, 1);
}

// ---------------------------------------------------------------------------
// Gap-free ids: however the process stops
// ---------------------------------------------------------------------------

#[test]
fn ids_are_gap_free_across_repeated_restarts_with_concurrent_writers() {
    let dir = temp_dir();
    let mut first_id = 1;
    for round in 0..6 {
        let log = Arc::new(open(&dir));
        assert_eq!(log.next_id(), first_id, "round {round} resumes after the last id");
        let ids = emit_concurrently(&log, 1 + round, 150);
        // Without a final flush: a dropped log loses nothing the OS has.
        drop(log);
        let envelopes = read_envelopes(&dir);
        assert_gap_free(&envelopes);
        let round_ids: Vec<Vec<u64>> = ids;
        let mut handed_out: Vec<u64> = round_ids.iter().flatten().copied().collect();
        handed_out.sort_unstable();
        assert_eq!(handed_out, (first_id..first_id + handed_out.len() as u64).collect::<Vec<_>>());
        first_id += handed_out.len() as u64;
        assert_eq!(envelopes.len() as u64, first_id - 1);
    }
}

#[test]
fn ids_are_gap_free_after_a_crash_cuts_the_last_line_at_any_byte() {
    // Reference file: three whole lines.
    let dir = temp_dir();
    {
        let log = open(&dir);
        for seq in 0..3 {
            log.emit(event(0, seq)).unwrap();
        }
    }
    let full = fs::read(log_path(&dir)).unwrap();
    let two_lines = full.iter().enumerate().filter(|(_, b)| **b == b'\n').nth(1).unwrap().0 + 1;

    // A crash during the third write can leave any prefix of it behind.
    for cut in two_lines + 1..full.len() {
        fs::write(log_path(&dir), &full[..cut]).unwrap();
        let log = open(&dir);
        assert_eq!(log.next_id(), 3, "cut at byte {cut}");
        assert_eq!(fs::read(log_path(&dir)).unwrap(), &full[..two_lines], "cut at byte {cut}: partial line kept");
        assert_eq!(log.emit(event(0, 2)).unwrap(), 3);
        drop(log);
        let envelopes = read_envelopes(&dir);
        assert_gap_free(&envelopes);
        assert_eq!(envelopes.len(), 3);
    }
    // And a crash that leaves zeroes (a size update without the data).
    let mut zeroed = full[..two_lines].to_vec();
    zeroed.extend([0u8; 64]);
    fs::write(log_path(&dir), &zeroed).unwrap();
    assert_eq!(open(&dir).next_id(), 3);
    assert_gap_free(&read_envelopes(&dir));
}

const CHILD_DIR_ENV: &str = "PRISM_CHANGELOG_TEST_CHILD_DIR";
const CHILD_MODE_ENV: &str = "PRISM_CHANGELOG_TEST_CHILD_MODE";
/// Events the child must have recorded before it is stopped.
const CHILD_MIN_EVENTS: u64 = 300;
const CHILD_WRITERS: usize = 8;
/// Backstops, so that a child whose parent is gone cannot fill the disk.
const CHILD_MAX_EVENTS: u64 = 200_000;
const CHILD_MAX_LIFETIME: Duration = Duration::from_secs(30);

/// The child side of the process tests; does nothing unless started by one.
/// Opens the log in the directory it is given and then, per mode:
///
/// * `kill`, `abort`, `exit`: [`CHILD_WRITERS`] threads emit (and now and
///   then flush) without end; once [`CHILD_MIN_EVENTS`] are in, the process
///   waits to be killed, aborts, or exits without running destructors.
/// * `disk_full`: emits until a write fails (the parent set a file size
///   limit) and checks that the log poisoned itself and kept whole lines.
///
/// Either way it stops by itself after [`CHILD_MAX_EVENTS`] events or
/// [`CHILD_MAX_LIFETIME`], failing, in case nobody stops it.
#[test]
#[ignore = "child process of the process-level change log tests"]
fn crash_child() {
    let (Ok(dir), Ok(mode)) = (std::env::var(CHILD_DIR_ENV), std::env::var(CHILD_MODE_ENV)) else {
        return;
    };
    let log = Arc::new(LocalChangeLog::open(&dir, CHAIN).unwrap());
    if mode == "disk_full" {
        let mut last = log.next_id() - 1;
        let err = loop {
            assert!(last < CHILD_MAX_EVENTS, "the file size limit never kicked in");
            match log.emit(event(0, last as usize)) {
                Ok(id) => {
                    assert_eq!(id, last + 1);
                    last = id;
                }
                Err(err) => break err,
            }
        };
        assert_changelog_error(&err, &format!("append event {}", last + 1));
        assert!(log.is_poisoned());
        let emit_err = log.emit(event(0, 0)).unwrap_err();
        let flush_err = log.flush().unwrap_err();
        assert_changelog_error(&emit_err, "append");
        assert_changelog_error(&flush_err, "sync");
        let cause = format!("earlier failure (append event {}: ", last + 1);
        for later in [emit_err, flush_err] {
            assert!(later.to_string().contains(&cause), "names the original failure: {later}");
        }
        assert_eq!(log.next_id(), last + 1, "failed emits use up no id");
        assert_eq!(
            fs::metadata(log.path()).unwrap().len(),
            log.stats().bytes_written,
            "the partial line was cut off"
        );
        println!("disk_full: last id {last}");
        return;
    }
    let born = Instant::now();
    let start = log.next_id();
    for writer in 0..CHILD_WRITERS {
        let log = Arc::clone(&log);
        thread::spawn(move || {
            for seq in 0.. {
                if log.next_id() >= start + CHILD_MAX_EVENTS {
                    return;
                }
                log.emit(event(writer, seq)).unwrap();
                if seq % 16 == 0 {
                    log.flush().unwrap();
                }
            }
        });
    }
    while log.next_id() < start + CHILD_MIN_EVENTS {
        thread::sleep(Duration::from_millis(1));
    }
    match mode.as_str() {
        "abort" => std::process::abort(),
        "exit" => std::process::exit(0),
        _ => {
            while born.elapsed() < CHILD_MAX_LIFETIME {
                thread::sleep(Duration::from_millis(100));
            }
            eprintln!("crash_child: never killed; giving up");
            std::process::exit(2);
        }
    }
}

fn child_command(program: impl AsRef<std::ffi::OsStr>, dir: &Path, mode: &str) -> Command {
    let mut command = Command::new(program);
    command
        .env(CHILD_DIR_ENV, dir)
        .env(CHILD_MODE_ENV, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

/// A running child, killed and reaped when dropped, so a failing assertion
/// in the parent does not leave it running.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn child_args() -> [&'static str; 5] {
    ["crash_child", "--exact", "--ignored", "--nocapture", "--test-threads=1"]
}

fn spawn_child(dir: &Path, mode: &str) -> ChildGuard {
    let mut command = child_command(std::env::current_exe().unwrap(), dir, mode);
    command.args(child_args());
    ChildGuard(command.spawn().expect("child test process starts"))
}

/// Wait for the child to exit; its status and what it wrote to stderr.
fn finish(child: &mut ChildGuard) -> (std::process::ExitStatus, String) {
    let status = child.0.wait().unwrap();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.0.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    (status, stderr)
}

/// Lines in the file so far, partial last line not counted.
fn complete_lines(dir: &Path) -> u64 {
    fs::read(log_path(dir)).map_or(0, |bytes| bytes.iter().filter(|b| **b == b'\n').count() as u64)
}

fn wait_for_lines(dir: &Path, child: &mut Child, lines: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while complete_lines(dir) < lines {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited early ({status}) with {} lines", complete_lines(dir));
        }
        assert!(Instant::now() < deadline, "child wrote only {} lines", complete_lines(dir));
        thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(unix)]
#[test]
fn ids_are_gap_free_when_the_process_is_killed_aborted_or_exits_mid_stream() {
    let dir = temp_dir();
    fs::create_dir_all(&dir).unwrap();
    let mut recorded = 0;
    for mode in ["kill", "abort", "exit", "kill", "abort", "exit", "kill"] {
        let mut child = spawn_child(&dir, mode);
        if mode == "kill" {
            wait_for_lines(&dir, &mut child.0, recorded + CHILD_MIN_EVENTS);
            // The child holds the lock: no second writer, even across processes.
            assert_changelog_error(&LocalChangeLog::open(&dir, CHAIN).unwrap_err(), "lock");
            child.0.kill().unwrap();
        }
        let (status, stderr) = finish(&mut child);
        if mode == "exit" {
            assert!(status.success(), "{mode}: {stderr}");
        } else {
            use std::os::unix::process::ExitStatusExt;
            let expected = if mode == "kill" { 9 } else { 6 };
            assert_eq!(status.signal(), Some(expected), "{mode}: {status}: {stderr}");
        }

        // Open truncates a torn tail, if the stop left one; then the whole
        // file must be gap-free and pick up right after its last line.
        let log = open(&dir);
        let envelopes = read_envelopes(&dir);
        assert_gap_free(&envelopes);
        let total = envelopes.len() as u64;
        assert!(total >= recorded + CHILD_MIN_EVENTS, "{mode}: only {total} events");
        assert_eq!(log.next_id(), total + 1, "{mode}");
        // Each child writer's events are in its own emission order.
        for writer in 0..CHILD_WRITERS {
            let slots: Vec<u64> = envelopes[recorded as usize..]
                .iter()
                .map(|e| e.event.slot())
                .filter(|slot| *slot / 1_000_000 == writer as u64)
                .collect();
            assert!(slots.windows(2).all(|w| w[1] == w[0] + 1), "{mode}: writer {writer} out of order");
        }
        recorded = total;
    }
    let _ = fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn ids_are_gap_free_when_the_disk_refuses_a_write() {
    let dir = temp_dir();
    fs::create_dir_all(&dir).unwrap();
    // A 16-block file size limit; SIGXFSZ ignored so the write fails with
    // EFBIG instead of killing the child. Both survive the `exec`.
    let mut command = child_command("sh", &dir, "disk_full");
    command
        .arg("-c")
        .arg(r#"ulimit -f 16 && trap '' XFSZ && exec "$0" "$@""#)
        .arg(std::env::current_exe().unwrap())
        .args(child_args());
    let output = command.output().expect("sh runs");
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let envelopes = read_envelopes(&dir);
    assert!(!envelopes.is_empty(), "the child wrote nothing");
    assert_gap_free(&envelopes);
    let log = open(&dir);
    let next = envelopes.len() as u64 + 1;
    assert_eq!(log.next_id(), next);
    assert_eq!(log.emit(event(1, 0)).unwrap(), next, "without the limit, numbering carries on");
    assert_gap_free(&read_envelopes(&dir));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn distinct_events_from_distinct_writers_never_share_a_line() {
    let dir = temp_dir();
    let log = Arc::new(open(&dir));
    emit_concurrently(&log, 8, 200);
    let envelopes = read_envelopes(&dir);
    let unique: HashSet<String> = envelopes.iter().map(|e| e.event.to_json()).collect();
    assert_eq!(unique.len(), envelopes.len(), "no event lost or duplicated");
}
