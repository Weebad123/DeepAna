//! Tests for `prism::types::error`.

use std::error::Error;
use std::io;
use std::time::Duration;

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use prism::types::error::{BoxError, PrismError, PrismResult};
use solana_client::client_error::{ClientError, ClientErrorKind};

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

fn timeout() -> ClientError {
    io::Error::new(io::ErrorKind::TimedOut, "request timed out").into()
}

fn bad_json() -> serde_json::Error {
    serde_json::from_str::<u64>("not json").unwrap_err()
}

/// One instance of every variant, in declaration order.
fn all_variants() -> Vec<PrismError> {
    vec![
        PrismError::rpc(Some(250_000_000), "getBlock", timeout()),
        PrismError::slot_skipped(250_000_001),
        PrismError::rate_limited(Some(1_500)),
        PrismError::deserialize(Some(250_000_002), "transaction meta", bad_json()),
        PrismError::decode(TOKEN_PROGRAM, 3, "unknown discriminant"),
        PrismError::storage(
            "transactions",
            (250_000_000, 250_000_099),
            ParquetError::General("disk full".into()),
        ),
        PrismError::cursor(
            "/var/lib/prism/cursor.json",
            io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
        ),
        PrismError::changelog(
            "/var/lib/prism/changelog.jsonl",
            "append event 42",
            io::Error::new(io::ErrorKind::StorageFull, "no space left on device"),
        ),
        PrismError::account_resolve(MINT, "account not found"),
        PrismError::config("rpc_url", "must not be empty"),
    ]
}

// ------------------------------------------------------------------ constructors

#[test]
fn constructors_populate_every_field() {
    match PrismError::rpc(Some(7), "getBlock", timeout()) {
        PrismError::RpcError {
            slot,
            method,
            source,
        } => {
            assert_eq!(slot, Some(7));
            assert_eq!(method, "getBlock");
            assert!(matches!(source.kind(), ClientErrorKind::Io(_)));
        }
        other => panic!("expected RpcError, got {other:?}"),
    }

    assert!(matches!(
        PrismError::slot_skipped(7),
        PrismError::SlotSkipped { slot: 7 }
    ));

    assert!(matches!(
        PrismError::rate_limited(Some(250)),
        PrismError::RateLimitError {
            retry_after_ms: Some(250)
        }
    ));
    assert!(matches!(
        PrismError::rate_limited(None),
        PrismError::RateLimitError {
            retry_after_ms: None
        }
    ));

    match PrismError::deserialize(Some(7), "mint account", bad_json()) {
        PrismError::DeserializeError {
            slot,
            context,
            source,
        } => {
            assert_eq!(slot, Some(7));
            assert_eq!(context, "mint account");
            assert!(source.is::<serde_json::Error>());
        }
        other => panic!("expected DeserializeError, got {other:?}"),
    }

    match PrismError::decode(TOKEN_PROGRAM, 3, "data too short") {
        PrismError::DecodeError {
            program_id,
            instruction_index,
            reason,
        } => {
            assert_eq!(program_id, TOKEN_PROGRAM);
            assert_eq!(instruction_index, 3);
            assert_eq!(reason, "data too short");
        }
        other => panic!("expected DecodeError, got {other:?}"),
    }

    match PrismError::storage("slots", (10, 20), ParquetError::General("boom".into())) {
        PrismError::StorageError {
            table,
            slot_range,
            source,
        } => {
            assert_eq!(table, "slots");
            assert_eq!(slot_range, (10, 20));
            assert!(matches!(source, ParquetError::General(m) if m == "boom"));
        }
        other => panic!("expected StorageError, got {other:?}"),
    }

    match PrismError::cursor("/tmp/cursor", io::Error::from(io::ErrorKind::NotFound)) {
        PrismError::CursorError { path, source } => {
            assert_eq!(path, "/tmp/cursor");
            assert_eq!(source.kind(), io::ErrorKind::NotFound);
        }
        other => panic!("expected CursorError, got {other:?}"),
    }

    match PrismError::account_resolve(MINT, "account not found") {
        PrismError::AccountResolveError { pubkey, source } => {
            assert_eq!(pubkey, MINT);
            assert_eq!(source.to_string(), "account not found");
        }
        other => panic!("expected AccountResolveError, got {other:?}"),
    }

    match PrismError::config("batch_size", "must be > 0") {
        PrismError::ConfigError { field, reason } => {
            assert_eq!(field, "batch_size");
            assert_eq!(reason, "must be > 0");
        }
        other => panic!("expected ConfigError, got {other:?}"),
    }
}

#[test]
fn storage_accepts_arrow_and_io_errors() {
    let from_arrow = PrismError::storage(
        "instructions",
        (1, 2),
        ArrowError::SchemaError("column count mismatch".into()),
    );
    let from_io = PrismError::storage(
        "instructions",
        (1, 2),
        io::Error::new(io::ErrorKind::StorageFull, "no space left on device"),
    );

    for (err, needle) in [
        (from_arrow, "column count mismatch"),
        (from_io, "no space left on device"),
    ] {
        assert!(matches!(
            &err,
            PrismError::StorageError {
                source: ParquetError::External(_),
                ..
            }
        ));
        assert!(err.to_string().contains(needle), "{err}");
    }
}

// ---------------------------------------------------------------------- Display

#[test]
fn display_carries_context_and_cause() {
    let rendered: Vec<String> = all_variants().iter().map(ToString::to_string).collect();
    assert_eq!(
        rendered,
        [
            "RPC getBlock failed at slot 250000000: request timed out",
            "slot 250000001 was skipped (no block produced)",
            "RPC rate limit exceeded, retry after 1500 ms",
            "failed to deserialize transaction meta at slot 250000002: \
             expected ident at line 1 column 2",
            "failed to decode instruction 3 of program \
             TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA: unknown discriminant",
            "storage failure on table transactions for slots 250000000..=250000099: \
             Parquet error: disk full",
            "cursor file /var/lib/prism/cursor.json is unusable: permission denied",
            "change log /var/lib/prism/changelog.jsonl failed to append event 42: \
             no space left on device",
            "failed to resolve account EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v: \
             account not found",
            "invalid config field rpc_url: must not be empty",
        ]
    );
}

#[test]
fn display_omits_absent_optional_context() {
    assert_eq!(
        PrismError::rpc(None, "getSlot", timeout()).to_string(),
        "RPC getSlot failed: request timed out"
    );
    assert_eq!(
        PrismError::rate_limited(None).to_string(),
        "RPC rate limit exceeded"
    );
    assert_eq!(
        PrismError::deserialize(None, "mint account", "unexpected length 81").to_string(),
        "failed to deserialize mint account: unexpected length 81"
    );
}

// ----------------------------------------------------------------------- source

#[test]
fn source_is_preserved_and_downcastable() {
    let rpc = PrismError::rpc(Some(1), "getBlock", timeout());
    let client = rpc
        .source()
        .and_then(|s| s.downcast_ref::<ClientError>())
        .expect("RpcError keeps the ClientError");
    assert!(matches!(client.kind(), ClientErrorKind::Io(e) if e.kind() == io::ErrorKind::TimedOut));

    let de = PrismError::deserialize(None, "transaction meta", bad_json());
    assert!(de.source().unwrap().is::<serde_json::Error>());

    let storage = PrismError::storage("slots", (1, 1), ParquetError::EOF("truncated".into()));
    assert!(matches!(
        storage.source().unwrap().downcast_ref::<ParquetError>(),
        Some(ParquetError::EOF(_))
    ));

    let cursor = PrismError::cursor("/tmp/cursor", io::Error::from(io::ErrorKind::NotFound));
    let io_err = cursor
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(io_err.kind(), io::ErrorKind::NotFound);

    let changelog = PrismError::changelog("/tmp/changelog.jsonl", "recover line 3", bad_json());
    assert!(changelog.source().unwrap().is::<serde_json::Error>());

    let resolve = PrismError::account_resolve(MINT, bad_json());
    assert!(resolve.source().unwrap().is::<serde_json::Error>());
}

#[test]
fn variants_without_a_cause_have_no_source() {
    assert!(PrismError::slot_skipped(1).source().is_none());
    assert!(PrismError::rate_limited(Some(1)).source().is_none());
    assert!(
        PrismError::decode(TOKEN_PROGRAM, 0, "borsh error")
            .source()
            .is_none()
    );
    assert!(PrismError::config("rpc_url", "missing").source().is_none());
}

#[test]
fn prism_errors_nest_as_causes() {
    // The account resolver fetches accounts over RPC; the RPC failure stays
    // reachable underneath the resolver's own error.
    let inner = PrismError::rpc(None, "getAccountInfo", timeout());
    let outer = PrismError::account_resolve(MINT, inner);

    let cause = outer
        .source()
        .and_then(|s| s.downcast_ref::<PrismError>())
        .expect("inner PrismError is preserved");
    assert!(matches!(cause, PrismError::RpcError { method, .. } if method == "getAccountInfo"));
    assert_eq!(
        outer.to_string(),
        format!("failed to resolve account {MINT}: RPC getAccountInfo failed: request timed out")
    );
}

// -------------------------------------------------------------------- accessors

#[test]
fn kind_is_a_distinct_snake_case_label_per_variant() {
    let kinds: Vec<&str> = all_variants().iter().map(PrismError::kind).collect();
    assert_eq!(
        kinds,
        [
            "rpc_error",
            "slot_skipped",
            "rate_limit_error",
            "deserialize_error",
            "decode_error",
            "storage_error",
            "cursor_error",
            "changelog_error",
            "account_resolve_error",
            "config_error",
        ]
    );
}

#[test]
fn slot_reports_the_single_slot_when_there_is_one() {
    let slots: Vec<Option<u64>> = all_variants().iter().map(PrismError::slot).collect();
    assert_eq!(
        slots,
        [
            Some(250_000_000),
            Some(250_000_001),
            None,
            Some(250_000_002),
            None,
            None,
            None,
            None,
            None,
            None,
        ]
    );

    assert_eq!(PrismError::rpc(None, "getSlot", timeout()).slot(), None);
    assert_eq!(
        PrismError::deserialize(None, "mint account", "bad").slot(),
        None
    );
}

#[test]
fn only_rpc_facing_variants_are_retryable() {
    let retryable: Vec<&str> = all_variants()
        .iter()
        .filter(|e| e.is_retryable())
        .map(PrismError::kind)
        .collect();
    assert_eq!(retryable, ["rpc_error", "rate_limit_error"]);
}

#[test]
fn retry_after_comes_only_from_the_rate_limit_hint() {
    assert_eq!(
        PrismError::rate_limited(Some(1_500)).retry_after(),
        Some(Duration::from_millis(1_500))
    );
    assert_eq!(PrismError::rate_limited(None).retry_after(), None);

    for err in all_variants() {
        if !matches!(err, PrismError::RateLimitError { .. }) {
            assert_eq!(err.retry_after(), None, "{}", err.kind());
        }
    }
}

// ------------------------------------------------------------------ propagation

#[test]
fn error_is_send_sync_and_static() {
    fn assert_bounds<T: Error + Send + Sync + 'static>() {}
    assert_bounds::<PrismError>();

    // Usable as a boxed cause, e.g. inside another PrismError.
    let boxed: BoxError = PrismError::slot_skipped(1).into();
    assert!(boxed.is::<PrismError>());
}

/// What the top level does with a failure.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    Retry(Option<Duration>),
    Skip,
    Halt,
}

/// Stand-in for the dispatcher in `lib.rs`. The match is exhaustive on
/// purpose: adding a variant must force a decision here.
fn decide(err: &PrismError) -> Action {
    match err {
        PrismError::RpcError { .. } | PrismError::RateLimitError { .. } => {
            Action::Retry(err.retry_after())
        }
        PrismError::SlotSkipped { .. }
        | PrismError::DeserializeError { .. }
        | PrismError::DecodeError { .. }
        | PrismError::AccountResolveError { .. } => Action::Skip,
        PrismError::StorageError { .. }
        | PrismError::CursorError { .. }
        | PrismError::ChangeLogError { .. }
        | PrismError::ConfigError { .. } => Action::Halt,
    }
}

fn fetch(slot: u64) -> PrismResult<Vec<u8>> {
    match slot {
        1 => Err(PrismError::rpc(Some(slot), "getBlock", timeout())),
        2 => Err(PrismError::rate_limited(Some(400))),
        3 => Err(PrismError::slot_skipped(slot)),
        _ => Ok(vec![0xFF]),
    }
}

fn decode(slot: u64, data: &[u8]) -> PrismResult<u8> {
    match (slot, data) {
        (4, _) => Err(PrismError::decode(TOKEN_PROGRAM, 0, "unknown discriminant")),
        (_, [first, ..]) => Ok(*first),
        _ => Err(PrismError::decode(TOKEN_PROGRAM, 0, "data too short")),
    }
}

fn write(slot: u64, _row: u8) -> PrismResult<()> {
    if slot == 5 {
        return Err(PrismError::storage(
            "instructions",
            (slot, slot),
            ParquetError::General("disk full".into()),
        ));
    }
    Ok(())
}

/// A miniature pipeline: each stage fails with its own variant and `?`
/// carries it to the caller untouched.
fn process(slot: u64) -> PrismResult<()> {
    let data = fetch(slot)?;
    let row = decode(slot, &data)?;
    write(slot, row)?;
    Ok(())
}

#[test]
fn errors_propagate_unchanged_to_the_top_level() {
    assert!(process(0).is_ok());

    let cases = [
        (1, "rpc_error", Action::Retry(None)),
        (
            2,
            "rate_limit_error",
            Action::Retry(Some(Duration::from_millis(400))),
        ),
        (3, "slot_skipped", Action::Skip),
        (4, "decode_error", Action::Skip),
        (5, "storage_error", Action::Halt),
    ];
    for (slot, kind, action) in cases {
        let err = process(slot).unwrap_err();
        assert_eq!(err.kind(), kind, "slot {slot}");
        assert_eq!(decide(&err), action, "slot {slot}");
    }
}

#[test]
fn every_variant_maps_to_an_action() {
    let actions: Vec<Action> = all_variants().iter().map(decide).collect();
    assert_eq!(
        actions,
        [
            Action::Retry(None),
            Action::Skip,
            Action::Retry(Some(Duration::from_millis(1_500))),
            Action::Skip,
            Action::Skip,
            Action::Halt,
            Action::Halt,
            Action::Halt,
            Action::Skip,
            Action::Halt,
        ]
    );
}
