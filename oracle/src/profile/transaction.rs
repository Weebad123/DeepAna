//! The transaction report: its types and the pure assembler.
//!
//! [`Profiler::transaction`](super::Profiler::transaction) runs the queries;
//! everything that turns their rows into a [`TransactionReport`] is here and
//! touches nothing but its arguments, so the same rows always give the same
//! report (the only field that differs between two runs is `duration_ms`,
//! which the profiler sets).
//!
//! # Two steps
//!
//! 1. [`pick`]: the rows of `solana.transactions` that carry the signature
//!    become one [`Picked`] transaction, or none. This comes first because
//!    every other query needs its slot.
//! 2. [`assemble`]: the slot row, the instruction rows and the token
//!    transfer rows of that slot and signature become the
//!    [`TransactionDetail`].
//!
//! # Duplicates
//!
//! PRISM replays a slot after a crash, and the replay appends: a row can be
//! in Parquet twice. Every table is therefore reduced to one row per key
//! before anything is built from it:
//!
//! | Rows            | Key                                              |
//! |-----------------|--------------------------------------------------|
//! | transactions    | `slot` (the signature is the query's filter)     |
//! | slots           | `slot`                                           |
//! | instructions    | `(instruction_index, inner_instruction_index)`   |
//! | token transfers | `(instruction_index, inner_instruction_index)`   |
//!
//! Copies that are identical are dropped and counted in [`Duplicates`].
//! Copies that differ (a replay that decoded an instruction the first run
//! did not) are counted as `conflicting` too, and the row kept is chosen by
//! a fixed rule, never by the order LENS returned them in: a decoded
//! instruction beats an undecoded one, then the smallest row by its JSON
//! text. The same signature in two **slots** is not a replay but a fork:
//! the newest slot at or below the snapshot is reported and the others are
//! listed in `other_slots`.
//!
//! # The instruction tree
//!
//! Top-level instructions in `instruction_index` order; each inner
//! instruction under the top-level instruction with its `instruction_index`,
//! in `inner_instruction_index` order. PRISM does not store the call depth,
//! so inner instructions are one flat list; when the logs line up
//! ([`attach_logs`]) each also gets the `stack_height` its `invoke [n]`
//! line printed. An inner instruction whose top-level row is missing is
//! kept in `orphan_instructions`, never dropped.
//!
//! # Failed transactions
//!
//! A failed transaction is in the report in full: PRISM stores its
//! instructions and the transfers it attempted. What the report adds is
//! [`Failure`]: the stored error, the top-level instruction it names, and,
//! from the logs, the innermost program that failed and why. Top-level
//! instructions after the failing one never ran and say so (`ran: false`),
//! and [`WARN_FAILED`] states that nothing but the fee took effect.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{Map, Value};

use super::decode::{self, is_memo_program};
use super::logs::{self, Logs, Outcome};
use super::{ProfileError, Window, get_u64, opt_str, opt_u64, protocol, ui_amount};

pub const WARN_NOT_FOUND: &str = "no transaction with this signature in the slots searched";
pub const WARN_FAILED: &str = "the transaction failed: its instructions and token transfers were rolled back, \
     and only the fee was charged";
pub const WARN_DUPLICATES: &str = "rows repeated by a replay were dropped; see duplicates";
pub const WARN_CONFLICTING: &str = "some repeated rows differ from each other; one was kept by a fixed rule";
pub const WARN_OTHER_SLOTS: &str = "the signature also appears in other slots (a fork); the newest is reported";
pub const WARN_NO_SLOT_ROW: &str = "the slot has no row in solana.slots, so block details are missing";
pub const WARN_LOGS_UNMATCHED: &str = "the logs do not line up with the instructions, so some instructions have \
     no log group";
pub const WARN_LOGS_CAPPED: &str = "the log text is capped; see logs for how much was dropped";
pub const WARN_LOGS_CUT_BY_RUNTIME: &str = "the runtime truncated this transaction's logs";
pub const WARN_ORPHANS: &str = "some inner instructions have no top-level instruction row";
pub const WARN_UNREADABLE_COLUMN: &str = "a stored JSON column could not be read and was treated as empty";

/// Programs the runtime runs without logging an invocation.
const SILENT_PROGRAMS: [&str; 3] = [
    "Ed25519SigVerify111111111111111111111111111",
    "KeccakSecp256k11111111111111111111111111111",
    "Secp256r1SigVerify1111111111111111111111111",
];

/// Address to display name; `None` for an address nobody named.
pub type Names<'a> = &'a dyn Fn(&str) -> Option<String>;

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// The report on one transaction.
///
/// Every field is a pure function of the request and the data at or below
/// `as_of_slot`, except `duration_ms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransactionReport {
    /// Always `"transaction"`.
    pub kind: &'static str,
    pub signature: String,
    pub found: bool,
    /// The snapshot slot `W`; pass it back as `as_of_slot` to reproduce.
    pub as_of_slot: u64,
    /// The slots searched for the signature.
    pub searched: Window,
    /// `null` when not found.
    pub transaction: Option<TransactionDetail>,
    pub warnings: Vec<String>,
    pub queries_run: u32,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransactionDetail {
    pub slot: u64,
    /// Position in the block.
    pub transaction_index: u32,
    pub blockhash: String,
    /// The slot's own row; `null` if `solana.slots` has none.
    pub block: Option<Block>,
    /// `success` or `failed`.
    pub status: String,
    /// `null` unless failed.
    pub error: Option<Failure>,
    pub fee_lamports: u64,
    /// The first account.
    pub fee_payer: Option<String>,
    pub compute_units_consumed: Option<u64>,
    /// In message order.
    pub accounts: Vec<AccountRef>,
    /// The tree; see the [module docs](self#the-instruction-tree).
    pub instructions: Vec<Instruction>,
    pub orphan_instructions: Vec<Instruction>,
    /// In instruction order.
    pub token_transfers: Vec<TokenTransfer>,
    pub memos: Vec<Memo>,
    pub logs: Logs,
    pub counts: Counts,
    pub duplicates: Duplicates,
    /// Other slots carrying the same signature, ascending.
    pub other_slots: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Block {
    pub blockhash: String,
    pub parent_slot: u64,
    /// Unix seconds, UTC.
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transaction_count: u64,
    pub leader: Option<String>,
    pub leader_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountRef {
    pub index: usize,
    pub address: String,
    pub name: Option<String>,
}

/// Why and where a transaction failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Failure {
    /// The error as PRISM stored it (the RPC's JSON).
    pub message: String,
    /// The top-level instruction the error names, if it names one.
    pub instruction_index: Option<u32>,
    /// From the logs: the innermost invocation that failed.
    pub program_id: Option<String>,
    pub program_name: Option<String>,
    /// From the logs: that invocation's `failed:` reason.
    pub reason: Option<String>,
    /// Its instruction, when the logs line up with the instructions.
    pub path: Option<String>,
    /// Its position in `logs.groups`.
    pub log_group: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Instruction {
    /// `"2"` for a top-level instruction, `"2.1"` for an inner one.
    pub path: String,
    pub instruction_index: u32,
    pub inner_instruction_index: Option<u32>,
    pub program_id: String,
    pub program_name: Option<String>,
    pub instruction_type: Option<String>,
    pub decoded: Option<Value>,
    /// `prism` or `oracle` (see [`decode`]); `null` if undecoded.
    pub decoded_by: Option<&'static str>,
    pub accounts: Vec<String>,
    pub data_base58: String,
    /// Length of the decoded data; `null` if it is not base58.
    pub data_bytes: Option<usize>,
    /// `false` for a top-level instruction after the one that failed.
    pub ran: bool,
    /// From the logs: 1 for top-level, deeper for nested calls.
    pub stack_height: Option<u32>,
    /// Position in `logs.groups`.
    pub log_group: Option<usize>,
    pub outcome: Option<Outcome>,
    pub compute_units_consumed: Option<u64>,
    /// Position in `token_transfers` of the transfer this instruction made.
    pub token_transfer: Option<usize>,
    /// Empty for an inner instruction.
    pub inner: Vec<Instruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TokenTransfer {
    /// The instruction that made it.
    pub path: String,
    pub instruction_index: u32,
    pub inner_instruction_index: Option<u32>,
    pub token_program: String,
    pub token_program_name: Option<String>,
    pub source_token_account: String,
    pub destination_token_account: String,
    pub source_owner: Option<String>,
    pub destination_owner: Option<String>,
    pub mint: Option<String>,
    pub mint_name: Option<String>,
    /// Base units, as decimal text: exact beyond 2^53.
    pub amount_raw: String,
    /// `amount_raw / 10^decimals`, exact; `null` without decimals.
    pub amount_ui: Option<String>,
    pub decimals: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Memo {
    /// The memo instruction; `null` for a memo found only in the logs.
    pub path: Option<String>,
    pub text: String,
    /// `instruction_data`, or `logs` when the data was not available.
    pub source: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Counts {
    /// Top-level and inner together.
    pub instructions: usize,
    pub inner_instructions: usize,
    pub decoded_instructions: usize,
    pub token_transfers: usize,
    pub programs: usize,
}

/// Rows dropped as repeats, per table. See the [module docs](self#duplicates).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Duplicates {
    pub transaction_rows: usize,
    pub slot_rows: usize,
    pub instruction_rows: usize,
    pub token_transfer_rows: usize,
    /// Keys whose repeated rows were not identical.
    pub conflicting: usize,
}

impl Duplicates {
    pub const fn any(&self) -> bool {
        self.transaction_rows + self.slot_rows + self.instruction_rows + self.token_transfer_rows > 0
    }
}

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

type Row = Map<String, Value>;

pub(super) fn get_u32(row: &Row, column: &str) -> Result<u32, ProfileError> {
    let v = get_u64(row, column)?;
    u32::try_from(v).map_err(|_| protocol(format!("{column} = {v} does not fit in u32")))
}

fn opt_u32(row: &Row, column: &str) -> Result<Option<u32>, ProfileError> {
    opt_u64(row, column)?
        .map(|v| u32::try_from(v).map_err(|_| protocol(format!("{column} = {v} does not fit in u32"))))
        .transpose()
}

pub(super) fn get_str(row: &Row, column: &str) -> Result<String, ProfileError> {
    opt_str(row, column)?.ok_or_else(|| protocol(format!("{column} is null")))
}

pub(super) fn opt_i64(row: &Row, column: &str) -> Result<Option<i64>, ProfileError> {
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| protocol(format!("{column} = {value} is not an i64"))),
        None => Err(protocol(format!("column {column} is missing"))),
    }
}

/// A column PRISM stores as a JSON array of strings. `Err(())` if it is not
/// one: the caller treats it as empty and warns.
fn string_list(row: &Row, column: &str) -> Result<Result<Vec<String>, ()>, ProfileError> {
    Ok(match opt_str(row, column)? {
        None => Ok(Vec::new()),
        Some(text) => serde_json::from_str::<Vec<String>>(&text).map_err(|_| ()),
    })
}

/// The row's JSON text: the tie-break between repeated rows that differ.
fn text_of(row: &Row) -> String {
    serde_json::to_string(row).unwrap_or_default()
}

/// One row per key: the smallest by `(rank, JSON text)`. Returns the rows
/// in key order and adds what was dropped to the two counters.
pub(super) fn dedup<K: Ord + Clone>(
    rows: &[Row],
    key: impl Fn(&Row) -> Result<K, ProfileError>,
    rank: impl Fn(&Row) -> u8,
    dropped: &mut usize,
    conflicting: &mut usize,
) -> Result<Vec<(K, Row)>, ProfileError> {
    let mut by_key: BTreeMap<K, Vec<(u8, String, &Row)>> = BTreeMap::new();
    for row in rows {
        by_key.entry(key(row)?).or_default().push((rank(row), text_of(row), row));
    }
    let mut out = Vec::with_capacity(by_key.len());
    for (k, mut copies) in by_key {
        copies.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        *dropped += copies.len() - 1;
        if copies.iter().any(|c| c.1 != copies[0].1) {
            *conflicting += 1;
        }
        out.push((k, copies[0].2.clone()));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Step 1: pick
// ---------------------------------------------------------------------------

/// The one transaction the report is about.
#[derive(Debug, Clone, PartialEq)]
pub struct Picked {
    pub slot: u64,
    row: Row,
    other_slots: Vec<u64>,
    duplicates: Duplicates,
}

/// Reduce the `solana.transactions` rows carrying the signature to one.
/// `None` if there are none. See the [module docs](self#duplicates).
pub fn pick(rows: &[Row]) -> Result<Option<Picked>, ProfileError> {
    let mut duplicates = Duplicates::default();
    let per_slot = dedup(
        rows,
        |r| get_u64(r, "slot"),
        |_| 0,
        &mut duplicates.transaction_rows,
        &mut duplicates.conflicting,
    )?;
    let mut slots: Vec<u64> = per_slot.iter().map(|(slot, _)| *slot).collect();
    let Some((slot, row)) = per_slot.into_iter().next_back() else { return Ok(None) };
    slots.pop();
    Ok(Some(Picked { slot, row, other_slots: slots, duplicates }))
}

// ---------------------------------------------------------------------------
// Step 2: assemble
// ---------------------------------------------------------------------------

/// The rows [`assemble`] reads besides the picked transaction, all of the
/// picked slot and signature.
#[derive(Debug, Clone, Copy)]
pub struct Rows<'a> {
    pub slots: &'a [Row],
    pub instructions: &'a [Row],
    pub token_transfers: &'a [Row],
}

/// Build the detail and its warnings. See the [module docs](self).
pub fn assemble(picked: Picked, rows: Rows<'_>, names: Names<'_>) -> Result<(TransactionDetail, Vec<String>), ProfileError> {
    let Picked { slot, row: tx, other_slots, mut duplicates } = picked;
    let mut unreadable = false;

    // The transaction row.
    let status = get_str(&tx, "status")?;
    let failed = status != "success";
    let account_list = string_list(&tx, "accounts")?.unwrap_or_else(|()| {
        unreadable = true;
        Vec::new()
    });
    let log_lines = string_list(&tx, "log_messages")?.unwrap_or_else(|()| {
        unreadable = true;
        Vec::new()
    });
    let logs = logs::group(&log_lines);
    let err = opt_str(&tx, "err")?;
    let failed_index = err.as_deref().and_then(failed_instruction_index);

    // The slot row.
    let slot_rows = dedup(rows.slots, |r| get_u64(r, "slot"), |_| 0, &mut duplicates.slot_rows, &mut duplicates.conflicting)?;
    let block = match slot_rows.into_iter().find(|(s, _)| *s == slot) {
        None => None,
        Some((_, r)) => {
            let leader = opt_str(&r, "leader")?;
            Some(Block {
                blockhash: get_str(&r, "blockhash")?,
                parent_slot: get_u64(&r, "parent_slot")?,
                block_time: opt_i64(&r, "block_time")?,
                block_height: opt_u64(&r, "block_height")?,
                transaction_count: get_u64(&r, "transaction_count")?,
                leader_name: leader.as_deref().and_then(names),
                leader,
            })
        }
    };

    // Token transfers, in instruction order.
    let ix_key = |r: &Row| Ok((get_u32(r, "instruction_index")?, opt_u32(r, "inner_instruction_index")?));
    let transfer_rows =
        dedup(rows.token_transfers, ix_key, |_| 0, &mut duplicates.token_transfer_rows, &mut duplicates.conflicting)?;
    let mut token_transfers = Vec::with_capacity(transfer_rows.len());
    let mut transfer_at: BTreeMap<(u32, Option<u32>), usize> = BTreeMap::new();
    for ((index, inner), r) in transfer_rows {
        let decimals = opt_u64(&r, "decimals")?
            .map(|d| u8::try_from(d).map_err(|_| protocol(format!("decimals {d} do not fit in u8"))))
            .transpose()?;
        let amount_raw = get_u64(&r, "amount_raw")?.to_string();
        let token_program = get_str(&r, "spl_token_program_id")?;
        let mint = opt_str(&r, "mint_token_address")?;
        transfer_at.insert((index, inner), token_transfers.len());
        token_transfers.push(TokenTransfer {
            path: path(index, inner),
            instruction_index: index,
            inner_instruction_index: inner,
            token_program_name: names(&token_program),
            token_program,
            source_token_account: get_str(&r, "source_token_account")?,
            destination_token_account: get_str(&r, "destination_token_account")?,
            source_owner: opt_str(&r, "source_owner")?,
            destination_owner: opt_str(&r, "destination_owner")?,
            mint_name: mint.as_deref().and_then(names),
            mint,
            amount_ui: decimals.map(|d| ui_amount(&amount_raw, d)),
            amount_raw,
            decimals,
        });
    }

    // Instructions: one per key, a decoded copy first.
    let instruction_rows = dedup(
        rows.instructions,
        ix_key,
        |r| u8::from(r.get("instruction_type").is_none_or(Value::is_null)),
        &mut duplicates.instruction_rows,
        &mut duplicates.conflicting,
    )?;
    let mut tops: Vec<Instruction> = Vec::new();
    let mut inners: Vec<Instruction> = Vec::new();
    for ((index, inner), r) in instruction_rows {
        let node = instruction(index, inner, &r, failed_index, transfer_at.get(&(index, inner)).copied(), names, &mut unreadable)?;
        if inner.is_none() { tops.push(node) } else { inners.push(node) }
    }
    // Keys are ordered (index, None) before (index, Some(_)), ascending, so
    // both lists are already in report order.
    let mut orphans = Vec::new();
    for node in inners {
        match tops.iter_mut().find(|t| t.instruction_index == node.instruction_index) {
            Some(top) => top.inner.push(node),
            None => orphans.push(node),
        }
    }

    let logs_line_up = attach_logs(&mut tops, &logs);
    let memos = memos(&tops, &orphans, &logs);
    let error = if failed { Some(failure(err.unwrap_or_default(), failed_index, &tops, &logs, names)) } else { None };

    let all = || tops.iter().flat_map(|t| std::iter::once(t).chain(&t.inner)).chain(&orphans);
    let counts = Counts {
        instructions: all().count(),
        inner_instructions: all().filter(|i| i.inner_instruction_index.is_some()).count(),
        decoded_instructions: all().filter(|i| i.instruction_type.is_some()).count(),
        token_transfers: token_transfers.len(),
        programs: all().map(|i| i.program_id.as_str()).collect::<BTreeSet<_>>().len(),
    };

    let mut warnings = Vec::new();
    let mut warn = |on: bool, text: &str| {
        if on {
            warnings.push(text.to_owned());
        }
    };
    warn(failed, WARN_FAILED);
    warn(!other_slots.is_empty(), WARN_OTHER_SLOTS);
    warn(duplicates.any(), WARN_DUPLICATES);
    warn(duplicates.conflicting > 0, WARN_CONFLICTING);
    warn(block.is_none(), WARN_NO_SLOT_ROW);
    warn(!orphans.is_empty(), WARN_ORPHANS);
    warn(!logs_line_up, WARN_LOGS_UNMATCHED);
    warn(logs.runtime_truncated, WARN_LOGS_CUT_BY_RUNTIME);
    warn(logs.dropped_lines() > 0 || logs.shortened_lines > 0, WARN_LOGS_CAPPED);
    warn(unreadable, WARN_UNREADABLE_COLUMN);

    let detail = TransactionDetail {
        slot,
        transaction_index: get_u32(&tx, "transaction_index")?,
        blockhash: get_str(&tx, "blockhash")?,
        block,
        status,
        error,
        fee_lamports: get_u64(&tx, "fee")?,
        fee_payer: account_list.first().cloned(),
        compute_units_consumed: opt_u64(&tx, "compute_units_consumed")?,
        accounts: account_list
            .into_iter()
            .enumerate()
            .map(|(index, address)| AccountRef { index, name: names(&address), address })
            .collect(),
        instructions: tops,
        orphan_instructions: orphans,
        token_transfers,
        memos,
        logs,
        counts,
        duplicates,
        other_slots,
    };
    Ok((detail, warnings))
}

/// `"2"` or `"2.1"`.
pub fn path(index: u32, inner: Option<u32>) -> String {
    match inner {
        None => index.to_string(),
        Some(inner) => format!("{index}.{inner}"),
    }
}

/// The top-level instruction an RPC error names: `{"InstructionError":[2,…]}`.
fn failed_instruction_index(err: &str) -> Option<u32> {
    let value: Value = serde_json::from_str(err).ok()?;
    let index = value.get("InstructionError")?.get(0)?.as_u64()?;
    u32::try_from(index).ok()
}

fn instruction(
    index: u32,
    inner: Option<u32>,
    row: &Row,
    failed_index: Option<u32>,
    token_transfer: Option<usize>,
    names: Names<'_>,
    unreadable: &mut bool,
) -> Result<Instruction, ProfileError> {
    let program_id = get_str(row, "program_id")?;
    let accounts = string_list(row, "accounts")?.unwrap_or_else(|()| {
        *unreadable = true;
        Vec::new()
    });
    let data_base58 = get_str(row, "data_base58")?;
    let data = bs58::decode(&data_base58).into_vec().ok();

    // PRISM's decoding first; ORACLE's only for what PRISM left undecoded.
    let mut instruction_type = opt_str(row, "instruction_type")?;
    let mut decoded = match opt_str(row, "decoded_data")? {
        None => None,
        Some(text) => match serde_json::from_str::<Value>(&text) {
            Ok(value) => Some(value),
            Err(_) => {
                *unreadable = true;
                None
            }
        },
    };
    let mut decoded_by = instruction_type.is_some().then_some("prism");
    if instruction_type.is_none()
        && let Some(bytes) = &data
        && let Some(d) = decode::decode(&program_id, bytes, &accounts)
    {
        instruction_type = Some(d.instruction_type.to_owned());
        decoded = Some(d.data);
        decoded_by = Some("oracle");
    }

    Ok(Instruction {
        path: path(index, inner),
        instruction_index: index,
        inner_instruction_index: inner,
        program_name: names(&program_id),
        program_id,
        instruction_type,
        decoded,
        decoded_by,
        accounts,
        data_bytes: data.as_ref().map(Vec::len),
        data_base58,
        ran: failed_index.is_none_or(|f| index <= f),
        stack_height: None,
        log_group: None,
        outcome: None,
        compute_units_consumed: None,
        token_transfer,
        inner: Vec::new(),
    })
}

/// Give each instruction the log group of its invocation. Groups and
/// instructions are both in execution order, so they are matched one after
/// another, a group to the next instruction **of the same program**:
///
/// * a depth-1 group to the next top-level instruction, stepping over
///   programs that never log ([`SILENT_PROGRAMS`]);
/// * a deeper group to the next inner instruction of that top-level one.
///
/// Returns whether every group found its instruction. On the first one
/// that does not, matching stops: guessing past a gap would attach logs to
/// the wrong instruction.
pub fn attach_logs(tops: &mut [Instruction], logs: &Logs) -> bool {
    let mut next_top = 0;
    let mut current: Option<usize> = None;
    let mut next_inner = 0;
    for group in &logs.groups {
        let set = |node: &mut Instruction| {
            node.log_group = Some(group.index);
            node.stack_height = Some(group.depth);
            node.outcome = Some(group.outcome);
            node.compute_units_consumed = group.compute_units_consumed;
        };
        if group.depth == 1 {
            while next_top < tops.len()
                && tops[next_top].program_id != group.program_id
                && SILENT_PROGRAMS.contains(&tops[next_top].program_id.as_str())
            {
                next_top += 1;
            }
            match tops.get_mut(next_top) {
                Some(top) if top.program_id == group.program_id => {
                    set(top);
                    current = Some(next_top);
                    next_top += 1;
                    next_inner = 0;
                }
                _ => return false,
            }
        } else {
            let Some(top) = current else { return false };
            match tops[top].inner.get_mut(next_inner) {
                Some(inner) if inner.program_id == group.program_id => {
                    set(inner);
                    next_inner += 1;
                }
                _ => return false,
            }
        }
    }
    true
}

/// Memos: from each memo instruction's data, from its log group when the
/// data is not UTF-8 text, and from memo log groups no instruction claimed.
fn memos(tops: &[Instruction], orphans: &[Instruction], logs: &Logs) -> Vec<Memo> {
    let mut out = Vec::new();
    let mut claimed: BTreeSet<usize> = BTreeSet::new();
    let nodes = tops.iter().flat_map(|t| std::iter::once(t).chain(&t.inner)).chain(orphans);
    for node in nodes.filter(|n| is_memo_program(&n.program_id)) {
        claimed.extend(node.log_group);
        let from_data =
            bs58::decode(&node.data_base58).into_vec().ok().and_then(|bytes| decode::memo_of(&node.program_id, &bytes));
        match from_data {
            Some(text) => out.push(Memo { path: Some(node.path.clone()), text, source: "instruction_data" }),
            None => {
                let lines = node.log_group.map(|g| logs.groups[g].lines.as_slice()).unwrap_or_default();
                out.extend(logs::memos_in(lines).into_iter().map(|text| Memo {
                    path: Some(node.path.clone()),
                    text,
                    source: "logs",
                }));
            }
        }
    }
    for group in logs.groups.iter().filter(|g| is_memo_program(&g.program_id) && !claimed.contains(&g.index)) {
        out.extend(logs::memos_in(&group.lines).into_iter().map(|text| Memo { path: None, text, source: "logs" }));
    }
    out
}

fn failure(message: String, instruction_index: Option<u32>, tops: &[Instruction], logs: &Logs, names: Names<'_>) -> Failure {
    // The innermost failure is the cause; the outer ones only pass it on.
    let group = logs
        .groups
        .iter()
        .filter(|g| g.outcome == Outcome::Failed)
        .min_by_key(|g| (std::cmp::Reverse(g.depth), g.index));
    let path = group.and_then(|g| {
        tops.iter()
            .flat_map(|t| std::iter::once(t).chain(&t.inner))
            .find(|n| n.log_group == Some(g.index))
            .map(|n| n.path.clone())
    });
    Failure {
        message,
        instruction_index,
        program_name: group.and_then(|g| names(&g.program_id)),
        program_id: group.map(|g| g.program_id.clone()),
        reason: group.and_then(|g| g.error.clone()),
        path,
        log_group: group.map(|g| g.index),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const SIG: &str = "sig";
    const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const SWAP: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
    const BUDGET: &str = decode::COMPUTE_BUDGET_PROGRAM;
    const MEMO: &str = decode::MEMO_PROGRAM;

    fn row(v: Value) -> Row {
        v.as_object().unwrap().clone()
    }

    fn no_names(_: &str) -> Option<String> {
        None
    }

    fn tx(slot: u64, status: &str, err: Option<&str>, logs: &[String]) -> Row {
        row(json!({
            "slot": slot, "blockhash": "hash", "transaction_index": 3, "tx_signature": SIG, "fee": 5000,
            "status": status, "err": err, "accounts": "[\"payer\",\"other\"]",
            "log_messages": serde_json::to_string(logs).unwrap(), "compute_units_consumed": 1234,
        }))
    }

    fn slot_row(slot: u64) -> Row {
        row(json!({
            "slot": slot, "blockhash": "hash", "parent_slot": slot - 1, "block_time": 1_793_102_400i64,
            "block_height": 77, "transaction_count": 9, "leader": "leader",
        }))
    }

    fn ix(index: u32, inner: Option<u32>, program: &str, data: &[u8], ty: Option<&str>) -> Row {
        row(json!({
            "instruction_index": index, "inner_instruction_index": inner, "program_id": program,
            "accounts": "[\"a\",\"b\"]", "data_base58": bs58::encode(data).into_string(),
            "instruction_type": ty, "decoded_data": ty.map(|_| "{\"amount\":7}"),
        }))
    }

    fn transfer(index: u32, inner: Option<u32>, amount: u64) -> Row {
        row(json!({
            "instruction_index": index, "inner_instruction_index": inner, "spl_token_program_id": TOKEN,
            "source_token_account": "src", "destination_token_account": "dst", "source_owner": "alice",
            "destination_owner": null, "mint_token_address": "mint", "amount_raw": amount, "decimals": 6,
        }))
    }

    fn build(txs: &[Row], slots: &[Row], ixs: &[Row], xfers: &[Row]) -> (TransactionDetail, Vec<String>) {
        let picked = pick(txs).unwrap().unwrap();
        assemble(picked, Rows { slots, instructions: ixs, token_transfers: xfers }, &no_names).unwrap()
    }

    fn swap_logs() -> Vec<String> {
        [
            format!("Program {BUDGET} invoke [1]"),
            format!("Program {BUDGET} success"),
            format!("Program {SWAP} invoke [1]"),
            format!("Program {TOKEN} invoke [2]"),
            format!("Program {TOKEN} consumed 4645 of 180000 compute units"),
            format!("Program {TOKEN} success"),
            format!("Program {SWAP} consumed 30000 of 200000 compute units"),
            format!("Program {SWAP} success"),
            format!("Program {MEMO} invoke [1]"),
            "Program log: Memo (len 2): \"gm\"".to_owned(),
            format!("Program {MEMO} success"),
        ]
        .to_vec()
    }

    fn swap_instructions() -> Vec<Row> {
        let limit = [[2u8].as_slice(), &200_000u32.to_le_bytes()].concat();
        vec![
            // Deliberately out of order: the assembler sorts.
            ix(2, None, MEMO, b"gm", None),
            ix(1, Some(0), TOKEN, &[3], Some("Transfer")),
            ix(1, None, SWAP, &[9, 9], None),
            ix(0, None, BUDGET, &limit, None),
        ]
    }

    #[test]
    fn the_tree_orders_top_level_then_inner_and_links_everything() {
        let (d, warnings) = build(&[tx(50, "success", None, &swap_logs())], &[slot_row(50)], &swap_instructions(), &[
            transfer(1, Some(0), u64::MAX),
        ]);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!((d.slot, d.transaction_index, d.fee_lamports), (50, 3, 5000));
        assert_eq!(d.fee_payer.as_deref(), Some("payer"));
        assert_eq!(d.accounts.iter().map(|a| (a.index, a.address.as_str())).collect::<Vec<_>>(), vec![(0, "payer"), (1, "other")]);
        assert_eq!(d.block.as_ref().unwrap().block_time, Some(1_793_102_400));

        let paths: Vec<&str> = d.instructions.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["0", "1", "2"]);
        assert_eq!(d.instructions[1].inner.iter().map(|i| i.path.as_str()).collect::<Vec<_>>(), vec!["1.0"]);
        assert!(d.orphan_instructions.is_empty());

        // Decoding: PRISM's where it decoded, ORACLE's for the rest it knows.
        let budget = &d.instructions[0];
        assert_eq!((budget.instruction_type.as_deref(), budget.decoded_by), (Some("SetComputeUnitLimit"), Some("oracle")));
        assert_eq!(budget.decoded, Some(json!({ "units": 200_000 })));
        let inner = &d.instructions[1].inner[0];
        assert_eq!((inner.instruction_type.as_deref(), inner.decoded_by), (Some("Transfer"), Some("prism")));
        assert_eq!(inner.decoded, Some(json!({ "amount": 7 })));
        assert_eq!((d.instructions[1].instruction_type.as_deref(), d.instructions[1].decoded_by), (None, None));
        assert_eq!(d.instructions[1].data_bytes, Some(2));

        // Logs: each instruction has its group, its depth and its units.
        assert_eq!(d.instructions.iter().map(|i| i.log_group).collect::<Vec<_>>(), vec![Some(0), Some(1), Some(3)]);
        assert_eq!((inner.log_group, inner.stack_height, inner.compute_units_consumed), (Some(2), Some(2), Some(4645)));
        assert_eq!(d.instructions[1].compute_units_consumed, Some(30_000));
        assert!(d.instructions.iter().all(|i| i.ran && i.outcome == Some(Outcome::Success)));

        // The transfer and the instruction that made it point at each other.
        assert_eq!(inner.token_transfer, Some(0));
        let t = &d.token_transfers[0];
        assert_eq!((t.path.as_str(), t.amount_raw.as_str()), ("1.0", "18446744073709551615"));
        assert_eq!(t.amount_ui.as_deref(), Some("18446744073709.551615"));

        assert_eq!(d.memos, vec![Memo { path: Some("2".into()), text: "gm".into(), source: "instruction_data" }]);
        assert_eq!(
            d.counts,
            Counts { instructions: 4, inner_instructions: 1, decoded_instructions: 3, token_transfers: 1, programs: 4 }
        );
        assert_eq!(d.duplicates, Duplicates::default());
    }

    #[test]
    fn a_replay_changes_nothing_but_the_duplicate_counts() {
        let once = build(&[tx(50, "success", None, &swap_logs())], &[slot_row(50)], &swap_instructions(), &[
            transfer(1, Some(0), 5),
        ]);
        let doubled = |rows: Vec<Row>| rows.iter().chain(rows.iter()).cloned().rev().collect::<Vec<_>>();
        let (mut twice, warnings) = build(
            &doubled(vec![tx(50, "success", None, &swap_logs())]),
            &doubled(vec![slot_row(50)]),
            &doubled(swap_instructions()),
            &doubled(vec![transfer(1, Some(0), 5)]),
        );
        assert_eq!(
            twice.duplicates,
            Duplicates { transaction_rows: 1, slot_rows: 1, instruction_rows: 4, token_transfer_rows: 1, conflicting: 0 }
        );
        assert_eq!(warnings, vec![WARN_DUPLICATES]);
        twice.duplicates = Duplicates::default();
        assert_eq!(twice, once.0);
    }

    #[test]
    fn repeated_rows_that_differ_are_resolved_by_rule_not_by_order() {
        // The replay decoded what the first run did not.
        let undecoded = ix(0, None, TOKEN, &[3], None);
        let decoded = ix(0, None, TOKEN, &[3], Some("Transfer"));
        let txs = [tx(50, "success", None, &[])];
        for ixs in [[undecoded.clone(), decoded.clone()], [decoded, undecoded]] {
            let (d, warnings) = build(&txs, &[slot_row(50)], &ixs, &[]);
            assert_eq!(d.instructions.len(), 1);
            assert_eq!(d.instructions[0].instruction_type.as_deref(), Some("Transfer"));
            assert_eq!((d.duplicates.instruction_rows, d.duplicates.conflicting), (1, 1));
            assert_eq!(warnings, vec![WARN_DUPLICATES, WARN_CONFLICTING]);
        }
        // Transfers that differ: the smaller JSON text, whichever came first.
        let a = build(&txs, &[slot_row(50)], &[], &[transfer(0, None, 1), transfer(0, None, 2)]).0;
        let b = build(&txs, &[slot_row(50)], &[], &[transfer(0, None, 2), transfer(0, None, 1)]).0;
        assert_eq!(a, b);
        assert_eq!(a.token_transfers.len(), 1);
    }

    #[test]
    fn the_same_signature_in_two_slots_reports_the_newest() {
        let picked = pick(&[tx(40, "failed", None, &[]), tx(50, "success", None, &[]), tx(45, "success", None, &[])])
            .unwrap()
            .unwrap();
        assert_eq!(picked.slot, 50);
        let (d, warnings) = assemble(picked, Rows { slots: &[slot_row(50)], instructions: &[], token_transfers: &[] }, &no_names).unwrap();
        assert_eq!(d.other_slots, vec![40, 45]);
        assert_eq!(d.status, "success");
        assert_eq!(warnings, vec![WARN_OTHER_SLOTS]);
        assert_eq!(pick(&[]).unwrap(), None);
    }

    #[test]
    fn a_failed_transaction_names_where_and_why() {
        let logs = [
            format!("Program {BUDGET} invoke [1]"),
            format!("Program {BUDGET} success"),
            format!("Program {SWAP} invoke [1]"),
            format!("Program {TOKEN} invoke [2]"),
            "Program log: Error: insufficient funds".to_owned(),
            format!("Program {TOKEN} failed: custom program error: 0x1"),
            format!("Program {SWAP} failed: custom program error: 0x1"),
        ];
        let err = r#"{"InstructionError":[1,{"Custom":1}]}"#;
        let names = |a: &str| (a == TOKEN).then(|| "SPL Token".to_owned());
        let picked = pick(&[tx(50, "failed", Some(err), &logs)]).unwrap().unwrap();
        let ixs = swap_instructions();
        let xfers = [transfer(1, Some(0), 5)];
        let (d, warnings) =
            assemble(picked, Rows { slots: &[slot_row(50)], instructions: &ixs, token_transfers: &xfers }, &names).unwrap();
        assert_eq!(warnings, vec![WARN_FAILED]);
        assert_eq!(
            d.error,
            Some(Failure {
                message: err.to_owned(),
                instruction_index: Some(1),
                program_id: Some(TOKEN.to_owned()),
                program_name: Some("SPL Token".to_owned()),
                reason: Some("custom program error: 0x1".to_owned()),
                path: Some("1.0".to_owned()),
                log_group: Some(2),
            })
        );
        // The instruction after the failing one never ran, and has no logs.
        assert_eq!(d.instructions.iter().map(|i| i.ran).collect::<Vec<_>>(), vec![true, true, false]);
        assert_eq!(d.instructions[2].log_group, None);
        assert_eq!(d.instructions[1].outcome, Some(Outcome::Failed));
        // What it attempted is still shown.
        assert_eq!(d.token_transfers.len(), 1);
        assert_eq!(d.instructions[1].inner[0].program_name.as_deref(), Some("SPL Token"));
        // The memo instruction never ran, but its data is still its memo.
        assert_eq!(d.memos.len(), 1);
    }

    #[test]
    fn a_failure_without_logs_or_an_instruction_still_reports_the_error() {
        let (d, _) = build(&[tx(50, "failed", Some("\"BlockhashNotFound\""), &[])], &[slot_row(50)], &[], &[]);
        let e = d.error.unwrap();
        assert_eq!(e.message, "\"BlockhashNotFound\"");
        assert_eq!((e.instruction_index, e.program_id, e.reason, e.path), (None, None, None, None));
        assert_eq!(failed_instruction_index("not json"), None);
        assert_eq!(failed_instruction_index(r#"{"InstructionError":[0,"InvalidArgument"]}"#), Some(0));
    }

    #[test]
    fn a_memo_falls_back_to_the_logs_when_the_data_is_not_text() {
        let logs = [
            format!("Program {MEMO} invoke [1]"),
            "Program log: Memo (len 6): \"from \\\"logs\\\"\"".to_owned(),
            format!("Program {MEMO} success"),
        ];
        // The instruction's data is not UTF-8: the log line is used.
        let (d, _) = build(&[tx(50, "success", None, &logs)], &[slot_row(50)], &[ix(0, None, MEMO, &[0xff, 0xfe], None)], &[]);
        assert_eq!(d.memos, vec![Memo { path: Some("0".into()), text: "from \"logs\"".into(), source: "logs" }]);
        // No instruction row at all: the memo is still found, without a path.
        let (d, warnings) = build(&[tx(50, "success", None, &logs)], &[slot_row(50)], &[], &[]);
        assert_eq!(d.memos, vec![Memo { path: None, text: "from \"logs\"".into(), source: "logs" }]);
        assert_eq!(warnings, vec![WARN_LOGS_UNMATCHED]);
    }

    #[test]
    fn logs_step_over_silent_programs_and_stop_at_a_gap() {
        let ed = SILENT_PROGRAMS[0];
        let logs = logs::group(&[format!("Program {SWAP} invoke [1]"), format!("Program {SWAP} success")]);
        let node = |index, program: &str| {
            instruction(index, None, &ix(index, None, program, &[], None), None, None, &no_names, &mut false).unwrap()
        };
        let mut tops = vec![node(0, ed), node(1, SWAP)];
        assert!(attach_logs(&mut tops, &logs));
        assert_eq!((tops[0].log_group, tops[1].log_group), (None, Some(0)));

        // A program that does log but has no group: nothing after it is guessed.
        let mut tops = vec![node(0, TOKEN), node(1, SWAP)];
        assert!(!attach_logs(&mut tops, &logs));
        assert_eq!((tops[0].log_group, tops[1].log_group), (None, None));
    }

    #[test]
    fn inner_instructions_without_a_parent_are_kept_apart() {
        let (d, warnings) =
            build(&[tx(50, "success", None, &[])], &[], &[ix(4, Some(1), TOKEN, &[], None), ix(0, None, SWAP, &[], None)], &[]);
        assert_eq!(d.instructions.len(), 1);
        assert_eq!(d.orphan_instructions.iter().map(|i| i.path.as_str()).collect::<Vec<_>>(), vec!["4.1"]);
        assert_eq!(d.block, None);
        assert_eq!(warnings, vec![WARN_NO_SLOT_ROW, WARN_ORPHANS]);
        assert_eq!(d.counts.instructions, 2);
    }

    #[test]
    fn unreadable_json_columns_are_empty_and_flagged() {
        let mut t = tx(50, "success", None, &[]);
        t.insert("accounts".into(), json!("not json"));
        let mut i = ix(0, None, SWAP, &[], Some("x"));
        i.insert("decoded_data".into(), json!("{"));
        let (d, warnings) = build(&[t], &[slot_row(50)], &[i], &[]);
        assert!(d.accounts.is_empty() && d.fee_payer.is_none());
        assert_eq!(d.instructions[0].decoded, None);
        assert_eq!(warnings, vec![WARN_UNREADABLE_COLUMN]);
    }

    #[test]
    fn a_row_of_the_wrong_shape_is_a_protocol_error() {
        let mut t = tx(50, "success", None, &[]);
        t.remove("fee");
        let picked = pick(&[t]).unwrap().unwrap();
        let err = assemble(picked, Rows { slots: &[], instructions: &[], token_transfers: &[] }, &no_names).unwrap_err();
        assert_eq!(err.kind(), "permanent");
        assert!(pick(&[row(json!({ "slot": "x" }))]).is_err());
    }

    #[test]
    fn the_report_serialises_the_same_every_time() {
        let make = || {
            let (detail, warnings) = build(&[tx(50, "success", None, &swap_logs())], &[slot_row(50)], &swap_instructions(), &[
                transfer(1, Some(0), 5),
            ]);
            serde_json::to_string(&TransactionReport {
                kind: "transaction",
                signature: SIG.into(),
                found: true,
                as_of_slot: 60,
                searched: Window::ending_at(60, 10),
                transaction: Some(detail),
                warnings,
                queries_run: 6,
                duration_ms: 0,
            })
            .unwrap()
        };
        assert_eq!(make(), make());
    }
}
