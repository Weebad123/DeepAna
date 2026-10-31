//! Tests for `forge::changelog_reader::checkpoint`.

mod common;

use std::fs;

use common::{Fixture, TempDir};
use forge::{CHECKPOINT_FILE_NAME, Checkpoint, CheckpointStore, ForgeError};

const FINGERPRINT: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

fn checkpoint(last_id: u64) -> Checkpoint {
    Checkpoint {
        last_id,
        byte_offset: last_id * 100,
        first_line_fingerprint: FINGERPRINT.into(),
        highest_final_slot: last_id * 10,
        updated_at: 1_761_040_000 + last_id as i64,
    }
}

fn store(dir: &TempDir) -> CheckpointStore {
    CheckpointStore::new(dir.path().join(CHECKPOINT_FILE_NAME))
}

#[track_caller]
fn assert_changelog_error(result: Result<Option<Checkpoint>, ForgeError>, store: &CheckpointStore, needle: &str) {
    match result {
        Err(ForgeError::ChangelogError { path, reason }) => {
            assert_eq!(path, store.path().display().to_string());
            assert!(reason.contains(needle), "{reason}");
        }
        other => panic!("expected ChangelogError, got {other:?}"),
    }
}

// ------------------------------------------------------------------ load

#[test]
fn load_of_an_absent_file_is_none() {
    let dir = TempDir::new();
    assert_eq!(store(&dir).load().unwrap(), None);
}

#[test]
fn save_then_load_round_trips() {
    let dir = TempDir::new();
    let store = store(&dir);
    store.save(&checkpoint(7)).unwrap();
    assert_eq!(store.load().unwrap(), Some(checkpoint(7)));
    store.save(&checkpoint(8)).unwrap();
    assert_eq!(store.load().unwrap(), Some(checkpoint(8)), "a save replaces the previous checkpoint");
}

#[test]
fn the_start_of_a_file_is_a_valid_checkpoint() {
    let dir = TempDir::new();
    let store = store(&dir);
    let start = Checkpoint { highest_final_slot: 500, ..Checkpoint::default() };
    store.save(&start).unwrap();
    assert_eq!(store.load().unwrap(), Some(start.clone()));
    assert!(start.is_at_start());
}

#[test]
fn an_unparsable_file_is_a_changelog_error_on_the_checkpoint_path() {
    let dir = TempDir::new();
    let store = store(&dir);
    for garbage in ["", "{", "not json", r#"{"last_id":1}"#, r#"{"last_id":-1,"byte_offset":0}"#] {
        fs::write(store.path(), garbage).unwrap();
        assert_changelog_error(store.load(), &store, "malformed checkpoint");
    }
}

#[test]
fn an_impossible_position_is_a_changelog_error() {
    let dir = TempDir::new();
    let store = store(&dir);
    let cases = [
        Checkpoint { byte_offset: 0, ..checkpoint(3) },
        Checkpoint { last_id: 0, ..checkpoint(3) },
        Checkpoint { first_line_fingerprint: String::new(), ..checkpoint(3) },
        Checkpoint { first_line_fingerprint: "abc".into(), ..checkpoint(3) },
        Checkpoint { first_line_fingerprint: FINGERPRINT.to_uppercase(), ..checkpoint(3) },
    ];
    for case in cases {
        fs::write(store.path(), serde_json::to_vec(&case).unwrap()).unwrap();
        assert_changelog_error(store.load(), &store, "invalid checkpoint");
    }
}

#[test]
fn a_directory_in_place_of_the_file_is_an_io_error() {
    let dir = TempDir::new();
    let store = store(&dir);
    fs::create_dir(store.path()).unwrap();
    let err = store.load().unwrap_err();
    assert_eq!(err.kind(), "io_error", "{err}");
}

// ------------------------------------------------------------------ save

#[test]
fn save_leaves_only_the_checkpoint_behind() {
    let dir = TempDir::new();
    let store = store(&dir);
    store.save(&checkpoint(1)).unwrap();
    assert_eq!(store.temp_path(), dir.path().join("forge.checkpoint.json.tmp"));
    assert!(!store.temp_path().exists(), "the temp file was renamed into place");
    let names: Vec<_> = fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(names, [CHECKPOINT_FILE_NAME]);
    let json: serde_json::Value = serde_json::from_slice(&fs::read(store.path()).unwrap()).unwrap();
    assert_eq!(json["last_id"], 1);
}

#[test]
fn a_temp_file_left_by_a_crash_is_ignored_and_then_replaced() {
    let dir = TempDir::new();
    let store = store(&dir);
    store.save(&checkpoint(4)).unwrap();
    // A crash after step 2 or 3: the next checkpoint is in the temp file,
    // whole or torn, and was never renamed.
    for leftover in [serde_json::to_vec(&checkpoint(5)).unwrap(), b"{\"last_id\":5,\"byte_off".to_vec()] {
        fs::write(store.temp_path(), &leftover).unwrap();
        assert_eq!(store.load().unwrap(), Some(checkpoint(4)), "the rename never happened");
    }
    store.save(&checkpoint(6)).unwrap();
    assert_eq!(store.load().unwrap(), Some(checkpoint(6)));
    assert!(!store.temp_path().exists());
}

#[test]
fn a_failed_save_keeps_the_previous_checkpoint() {
    let dir = TempDir::new();
    let store = store(&dir);
    store.save(&checkpoint(2)).unwrap();
    // The temp path is taken by a directory: step 2 fails.
    fs::create_dir(store.temp_path()).unwrap();
    let err = store.save(&checkpoint(3)).unwrap_err();
    assert_eq!(err.kind(), "io_error", "{err}");
    assert_eq!(store.load().unwrap(), Some(checkpoint(2)));
}

#[test]
fn save_into_a_missing_directory_is_an_io_error() {
    let dir = TempDir::new();
    let store = CheckpointStore::new(dir.path().join("missing").join(CHECKPOINT_FILE_NAME));
    assert_eq!(store.save(&checkpoint(1)).unwrap_err().kind(), "io_error");
}

#[test]
fn for_config_puts_the_checkpoint_next_to_the_database() {
    let fixture = Fixture::new();
    let config = fixture.load(&[]).unwrap();
    let store = CheckpointStore::for_config(&config);
    assert_eq!(store.path(), config.db_path().parent().unwrap().join(CHECKPOINT_FILE_NAME));
}
