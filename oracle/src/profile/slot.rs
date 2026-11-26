//! The slot report: one block and the transactions in it.
//!
//! The step between an aggregate and a transaction: a query answer names a
//! slot, the slot report lists its transactions, and each of those opens as
//! a [`transaction`](super::transaction) report.
//!
//! As there, the profiler runs the queries and everything here is pure.
//!
//! # Paging
//!
//! A block can hold thousands of transactions, so the list is a page:
//! `limit` rows after `after_index`, in `transaction_index` order.
//! `next_after_index` is what to pass to get the next page; it is `null` on
//! the last one. The counts always cover the whole slot.
//!
//! # Duplicates
//!
//! A slot PRISM replayed is in Parquet twice. The counts use `COUNT(DISTINCT
//! tx_signature)`, the list keeps one row per signature (by the same fixed
//! rule as the transaction report), and `duplicates` says how many rows were
//! repeats. A page that was made only of repeats is still a page: its
//! `next_after_index` moves on.

use serde::Serialize;
use serde_json::{Map, Value};

use super::transaction::{Block, Names, dedup, get_str, get_u32, opt_i64};
use super::{ProfileError, get_u64, opt_str, opt_u64, protocol};

/// Transactions per page when the request has no `limit`.
pub const DEFAULT_SLOT_PAGE: u64 = 100;
/// Most transactions one page may hold.
pub const MAX_SLOT_PAGE: u64 = 500;

pub const WARN_SLOT_NOT_FOUND: &str = "no row for this slot: it was skipped, or PRISM has not ingested it";
pub const WARN_SLOT_DUPLICATES: &str = "rows repeated by a replay were dropped; see duplicates";
pub const WARN_COUNT_MISMATCH: &str =
    "the slot row's transaction_count differs from the transactions stored for the slot";

/// The report on one slot.
///
/// Every field is a pure function of the request and the data at or below
/// `as_of_slot`, except `duration_ms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SlotReport {
    /// Always `"slot"`.
    pub kind: &'static str,
    pub slot: u64,
    /// Whether `solana.slots` has a row for it.
    pub found: bool,
    /// The snapshot slot `W`; pass it back as `as_of_slot` to reproduce.
    pub as_of_slot: u64,
    /// `null` when not found.
    pub block: Option<Block>,
    pub counts: SlotCounts,
    /// One page, in `transaction_index` order.
    pub transactions: Vec<SlotTransaction>,
    pub page: Page,
    pub duplicates: SlotDuplicates,
    pub warnings: Vec<String>,
    pub queries_run: u32,
    pub duration_ms: u64,
}

/// Over the whole slot, not the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct SlotCounts {
    /// Distinct signatures.
    pub transactions: u64,
    pub failed_transactions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotTransaction {
    pub transaction_index: u32,
    pub signature: String,
    /// `success` or `failed`.
    pub status: String,
    pub err: Option<String>,
    pub fee_lamports: u64,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Page {
    pub after_index: Option<u32>,
    pub limit: u64,
    pub has_more: bool,
    /// Pass as `after_index` for the next page; `null` on the last.
    pub next_after_index: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct SlotDuplicates {
    /// Rows of `solana.transactions` beyond one per signature, whole slot.
    pub transaction_rows: u64,
    pub slot_rows: usize,
    /// Keys whose repeated rows were not identical, among the rows read.
    pub conflicting: usize,
}

type Row = Map<String, Value>;

/// The rows [`assemble`] reads.
#[derive(Debug, Clone, Copy)]
pub struct SlotRows<'a> {
    /// `solana.slots` rows of the slot.
    pub slots: &'a [Row],
    /// One row: `transactions`, `failed`, `stored_rows`.
    pub counts: &'a Row,
    /// Up to `limit + 1` rows after `after_index`, repeats included.
    pub transactions: &'a [Row],
}

/// The parts of a [`SlotReport`] the rows decide.
#[derive(Debug, Clone, PartialEq)]
pub struct SlotBody {
    pub block: Option<Block>,
    pub counts: SlotCounts,
    pub transactions: Vec<SlotTransaction>,
    pub page: Page,
    pub duplicates: SlotDuplicates,
    pub warnings: Vec<String>,
}

/// Build the body of the report on `slot`. See the [module docs](self).
pub fn assemble(
    slot: u64,
    after_index: Option<u32>,
    limit: u64,
    rows: SlotRows<'_>,
    names: Names<'_>,
) -> Result<SlotBody, ProfileError> {
    let mut duplicates = SlotDuplicates::default();

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

    let counts = SlotCounts {
        transactions: get_u64(rows.counts, "transactions")?,
        failed_transactions: get_u64(rows.counts, "failed")?,
    };
    let stored = get_u64(rows.counts, "stored_rows")?;
    if counts.failed_transactions > counts.transactions || counts.transactions > stored {
        return Err(protocol(format!(
            "{} failed of {} transactions in {stored} rows",
            counts.failed_transactions, counts.transactions
        )));
    }
    duplicates.transaction_rows = stored - counts.transactions;

    // The page. The statement asked for `limit + 1` rows in index order, so
    // a full answer means more exist; the extra row is not part of the page.
    let has_more = rows.transactions.len() as u64 > limit;
    let read = &rows.transactions[..rows.transactions.len().min(usize::try_from(limit).unwrap_or(usize::MAX))];
    let mut last_read: Option<u32> = None;
    for r in read {
        let index = get_u32(r, "transaction_index")?;
        last_read = Some(last_read.map_or(index, |max| max.max(index)));
    }
    let mut page_duplicates = 0;
    let listed = dedup(
        read,
        |r| Ok((get_u32(r, "transaction_index")?, get_str(r, "tx_signature")?)),
        |_| 0,
        &mut page_duplicates,
        &mut duplicates.conflicting,
    )?;
    let transactions = listed
        .into_iter()
        .map(|((transaction_index, signature), r)| {
            Ok(SlotTransaction {
                transaction_index,
                signature,
                status: get_str(&r, "status")?,
                err: opt_str(&r, "err")?,
                fee_lamports: get_u64(&r, "fee")?,
                compute_units_consumed: opt_u64(&r, "compute_units_consumed")?,
            })
        })
        .collect::<Result<Vec<_>, ProfileError>>()?;

    let mut warnings = Vec::new();
    if block.is_none() {
        warnings.push(WARN_SLOT_NOT_FOUND.to_owned());
    }
    if duplicates.transaction_rows > 0 || duplicates.slot_rows > 0 {
        warnings.push(WARN_SLOT_DUPLICATES.to_owned());
    }
    if block.as_ref().is_some_and(|b| b.transaction_count != counts.transactions) {
        warnings.push(WARN_COUNT_MISMATCH.to_owned());
    }

    Ok(SlotBody {
        block,
        counts,
        transactions,
        page: Page { after_index, limit, has_more, next_after_index: if has_more { last_read } else { None } },
        duplicates,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn row(v: Value) -> Row {
        v.as_object().unwrap().clone()
    }

    fn no_names(_: &str) -> Option<String> {
        None
    }

    fn slot_row(count: u64) -> Row {
        row(json!({
            "slot": 50, "blockhash": "hash", "parent_slot": 49, "block_time": 1_793_102_400i64,
            "block_height": 7, "transaction_count": count, "leader": "leader",
        }))
    }

    fn tx(index: u32, ok: bool) -> Row {
        row(json!({
            "transaction_index": index, "tx_signature": format!("sig{index}"),
            "status": if ok { "success" } else { "failed" }, "err": if ok { None } else { Some("boom") },
            "fee": 5000, "compute_units_consumed": 10,
        }))
    }

    fn counts(transactions: u64, failed: u64, stored: u64) -> Row {
        row(json!({ "transactions": transactions, "failed": failed, "stored_rows": stored }))
    }

    #[test]
    fn a_page_lists_transactions_in_index_order() {
        let txs = [tx(2, false), tx(0, true), tx(1, true)];
        let c = counts(3, 1, 3);
        let body =
            assemble(50, None, 100, SlotRows { slots: &[slot_row(3)], counts: &c, transactions: &txs }, &no_names).unwrap();
        assert_eq!(body.transactions.iter().map(|t| t.transaction_index).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(body.transactions[2].err.as_deref(), Some("boom"));
        assert_eq!(body.counts, SlotCounts { transactions: 3, failed_transactions: 1 });
        assert_eq!(body.page, Page { after_index: None, limit: 100, has_more: false, next_after_index: None });
        assert!(body.warnings.is_empty(), "{:?}", body.warnings);
        assert_eq!(body.block.unwrap().parent_slot, 49);
    }

    #[test]
    fn the_extra_row_only_says_there_is_more() {
        // limit 2, three rows returned: the third is not listed.
        let txs = [tx(5, true), tx(6, true), tx(7, true)];
        let c = counts(9, 0, 9);
        let body =
            assemble(50, Some(4), 2, SlotRows { slots: &[slot_row(9)], counts: &c, transactions: &txs }, &no_names).unwrap();
        assert_eq!(body.transactions.len(), 2);
        assert_eq!(body.page, Page { after_index: Some(4), limit: 2, has_more: true, next_after_index: Some(6) });
    }

    #[test]
    fn a_replayed_slot_lists_each_transaction_once() {
        let txs = [tx(0, true), tx(0, true), tx(1, true), tx(1, true)];
        let c = counts(2, 0, 4);
        let body = assemble(
            50,
            None,
            100,
            SlotRows { slots: &[slot_row(2), slot_row(2)], counts: &c, transactions: &txs },
            &no_names,
        )
        .unwrap();
        assert_eq!(body.transactions.len(), 2);
        assert_eq!(body.duplicates, SlotDuplicates { transaction_rows: 2, slot_rows: 1, conflicting: 0 });
        assert_eq!(body.warnings, vec![WARN_SLOT_DUPLICATES]);
        // A page made of repeats still moves on.
        let body = assemble(50, None, 2, SlotRows { slots: &[slot_row(2)], counts: &c, transactions: &txs[..3] }, &no_names)
            .unwrap();
        assert_eq!(body.transactions.len(), 1);
        assert_eq!((body.page.has_more, body.page.next_after_index), (true, Some(0)));
    }

    #[test]
    fn a_slot_without_a_row_is_not_found_and_a_wrong_count_is_flagged() {
        let c = counts(0, 0, 0);
        let body = assemble(50, None, 100, SlotRows { slots: &[], counts: &c, transactions: &[] }, &no_names).unwrap();
        assert_eq!(body.block, None);
        assert_eq!(body.warnings, vec![WARN_SLOT_NOT_FOUND]);

        let c = counts(1, 0, 1);
        let txs = [tx(0, true)];
        let body =
            assemble(50, None, 100, SlotRows { slots: &[slot_row(4)], counts: &c, transactions: &txs }, &no_names).unwrap();
        assert_eq!(body.warnings, vec![WARN_COUNT_MISMATCH]);
    }

    #[test]
    fn impossible_counts_are_a_protocol_error() {
        for c in [counts(1, 2, 2), counts(3, 0, 2)] {
            let err = assemble(50, None, 100, SlotRows { slots: &[], counts: &c, transactions: &[] }, &no_names).unwrap_err();
            assert_eq!(err.kind(), "permanent");
        }
    }
}
