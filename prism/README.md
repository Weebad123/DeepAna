# prism

PRISM is DeepAna's Solana ingestion engine. It follows the chain tip,
turns every block into rows of a fixed Universal Data Model (UDM), decodes
the instructions it recognises, and writes the rows as date-partitioned
Parquet files. Each step it takes is also published as a change event for
downstream systems.

```text
  ChainAdapter (Helius RPC)
        │
        ▼
  ┌─ Ingestor ───────────────────────────────────────┐
  │                                                  │
  │  process task                                    │
  │    SlotProcessor     fetch, retry, normalize     │
  │      └ GapHandler    skipped slots               │──── Skip ────┐
  │    DecoderRegistry   decode, find transfers      │              │
  │    AccountResolver   mint decimals               │              │
  │        │                                         │              │
  │        │ ProcessedSlot (bounded channel)         │              ▼
  │        ▼                                         │          ChangeLog ◀── Final ── CommitmentTracker
  │  writer task                                     │              ▲
  │    ParquetWriter     append, flush               │──── New ─────┘
  │    IngestorCursor    resume point                │
  └──────────────────────────────────────────────────┘
```

## Modules

| Module | What it does |
|--------|--------------|
| `types` | The shared contracts. `udm` defines the four row types (`DeepAnaSlot`, `DeepAnaTransaction`, `DeepAnaInstruction`, `DeepAnaTokenTransfer`) and their Arrow schemas. `event` defines `ChangeEvent` (`New`, `Skip`, `Final`, `Correction`) and `EventEnvelope`, the numbered form the change log stores. `error` defines `PrismError`, the single error type every module returns. |
| `adapter` | The boundary with the chain. `ChainAdapter` is the chain-agnostic trait (tip, block, block header, block list, accounts). `SolanaAdapter` implements it, adding rate limiting, retries and error mapping; a block header is a `getBlock` with `transactionDetails: none`. |
| `slot_processor` | Takes one slot number to one processed slot. `SlotFetcher` fetches the block with retries. `GapHandler` records skipped or unreachable slots as `Skip` events and in an audit log. |
| `normalizer` | Splits a raw block into a slot row, transaction rows and a flattened list of instruction rows (inner instructions included), and reads every transaction's `preTokenBalances` / `postTokenBalances` into `TokenBalanceChange`s (not a table: they feed the account resolver). v0 transactions resolve instruction accounts and token balances against the full key list: static keys, then the lookup-table addresses from `meta.loadedAddresses`. A block is normalized in full or not at all. |
| `decoder` | `DecoderRegistry` routes each instruction to the decoders registered for its program. `SplTokenDecoder` handles SPL Token and Token-2022 and produces token transfer rows. `SystemDecoder` decodes System program instructions (SOL transfers, account creation, nonces) and `MemoDecoder` memo text; see [Built-in decoders](#built-in-decoders). `AnchorDecoder` decodes any Anchor program from its IDL, `emit_cpi!` events included. |
| `account_resolver` | Completes token transfer rows: mint (for a plain `Transfer`), decimals and both owners come first from the transaction's own token balances (`TokenAccountIndex`), with no request; only transfers whose decimals are still unknown fall back to reading the mint account over RPC. `MintCache` ensures each mint is fetched from the RPC only once and remembers recent misses for a while. |
| `commitment` | Tracks the boundary between `confirmed` and `finalized` slots and emits a `Final` event whenever the finalized watermark advances. `CommitmentTracker::spawn_persistent` keeps the watermark in `finality.json`, so a restart knows finality before its first poll and resumes announcing where it stopped. The tables have no `is_finalized` column (the schema is fixed): a row is final when its `slot` is at or below the latest `Final` watermark. |
| `storage` | `Storage` is the backend trait. `ParquetWriter` buffers rows in memory and on each flush writes one file per table. `PartitionKey` computes the file paths. Flushes are all-or-nothing, and `rollback` removes slots at or above a given slot (e.g. after a fork). |
| `changelog` | `ChangeLog`, the sink every change event goes through: `emit` records an event and returns its id, `flush` makes everything recorded durable. Three producers publish to it: the gap handler (`Skip`, as soon as a slot is found skipped or unreachable), the ingestor's writer task (`New`, one per slot once its files are on disk) and the commitment tracker (`Final`). `LocalChangeLog` appends them to `{data_dir}/changelog.jsonl`, see [The change log file](#the-change-log-file). |
| `reconcile` | Checks finalized slots on disk against the finalized chain. `Reconciler` runs passes on a timer, classifies each slot of a window as ok, `orphaned`, `mismatched`, `missing`, `false_skip` or `unlogged_skip`, and when two passes agree asks the ingestor (through `Rewind`) to re-ingest from the lowest wrong slot, announcing it as a `Correction`. State lives in `reconcile.json`. See [Reconciliation](#reconciliation). |
| `metadata` | Token name, symbol and URI from each mint's Metaplex metadata account, off the hot path. The writer task hints the mints of every flushed transfer; `MetadataEnricher` batches them, reads the accounts at the derived PDA (`pda`), validates them strictly (`parse`) and writes one row per mint, once, to `token_metadata` (`writer`). See [Token metadata](#token-metadata). |
| `ingestor` | The orchestration layer. It builds and owns the slot processor, decoder registry, account resolver and `ParquetWriter`, and runs them as two Tokio tasks joined by a bounded channel of `ProcessedSlot`s: *process* (fetch, normalize, decode, resolve) and *writer* (append, flush, announce, move the cursor). It persists an `IngestorCursor` so a restart resumes after the last flushed slot. It also builds the Helius-backed adapter. See [Running the ingestor](#running-the-ingestor). |



`IngestorConfig::new(data_dir)` keeps everything under one directory: the
Parquet tables, the cursor and the skip log. `Ingestor::with_decoders` swaps
in a registry with extra decoders (e.g. `AnchorDecoder`s).

What a run does:

- **Process task.** Follows the tip at the configured commitment and runs
  each slot through the slot processor. The fetcher retries transient
  failures up to `fetcher.max_retries`. A skipped slot is recorded by the gap
  handler. Slots with data are decoded and their token transfers resolved,
  then sent to the writer. Transfer instructions of failed transactions get
  no transfer row (the transaction was rolled back, nothing moved); their
  instruction rows are still written, decoded.
- **Writer task.** Appends each slot's rows to the `ParquetWriter` and moves
  the cursor. It flushes every `flush_interval`, or as soon as
  `flush_max_slots` slots are buffered. After a successful flush it emits one
  `New` event per flushed slot (each carrying the files written), then saves
  the cursor.
- **Shutdown.** `shutdown()` (or reaching `end_slot`) stops the process task.
  The writer drains the channel, flushes what is left and saves the cursor.

When something goes wrong:

| Failure | What happens |
|---------|--------------|
| Slot fetch fails after every retry | An `RpcUnavailable` skip is recorded and the pipeline halts before that slot. A restart fetches it again. |
| Non-retryable fetch or decode error | The pipeline halts. Slots before it are flushed on the way out. |
| Flush fails | The rows stay buffered and the flush is retried at the next interval. After `max_flush_failures` failures in a row (default 3) the pipeline halts. If the final flush at shutdown fails, its error is returned right away. |
| Change log fails (emit or flush, from any producer) | The pipeline halts and the cursor does not move. A restart writes and announces those slots again. The finality poller stops too and halts the ingestor with its error. |
| Crash between a flush and the cursor save | On startup, `run()` deletes files at or above its first slot, then ingests those slots again. Every slot ends up on disk once. |
| Crash during a reconcile rewind | On startup, `run()` announces the `Correction` again, lowers the cursor, deletes the files from the rewind point and records the rewind as done before its pipeline starts. See [Reconciliation](#reconciliation). |

### Change events

| Event | Emitted by | When |
|-------|------------|------|
| `Skip` | gap handler | As soon as a slot is found skipped (`validator_skipped`) or unreachable after retries (`rpc_unavailable`). |
| `New` | writer task | Once per slot with data, after its flush succeeded. Carries the paths of every file in that flush. |
| `Final` | commitment tracker | When the finalized watermark advances: one event per newly finalized watermark, then one change log flush per poll. |
| `Correction` | ingestor, for the reconciler | Once the pipeline has stopped for a rewind, before anything is deleted: `from_slot`, `to_slot`, `reason` (the most severe present: `orphaned`, then `mismatched`, then `missing`, then `false_skip`) and `affected` (how many slots were wrong). Every file at or above `from_slot` is deleted and those slots re-announced with fresh `New` / `Skip` events: every `New` / `Skip` for a slot at or above `from_slot` after the correction is from the re-ingest. |

Delivery is at least once: a `New` or `Correction` event can repeat after a restart. A
`Skip` can arrive before the `New` events of earlier slots that are still
buffered, so consumers that need strict slot order should sort by slot.

## Built-in decoders

`DecoderRegistry::with_defaults()` is the ingestor's registry unless
`Ingestor::with_decoders` supplies another (which is then used as given).
`register_defaults()` adds the built-ins to a custom registry, skipping any
program that already has a decoder, so a caller's decoder always wins.

| Program | Id | Decoder | `instruction_type` |
|---------|----|---------|--------------------|
| SPL Token | `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA` | `SplTokenDecoder` | SPL variant name (`TransferChecked`, ...) |
| Token-2022 | `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb` | `SplTokenDecoder` | as SPL Token, plus extensions |
| System | `11111111111111111111111111111111` | `SystemDecoder` | `CreateAccount`, `Transfer`, `TransferWithSeed`, `AdvanceNonceAccount`, ... (tags 0 to 12) |
| Memo v2 / v1 | `MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr` / `Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo` | `MemoDecoder` | `Memo` |

System and memo `decoded_data` carries the instruction's fields at the top
level and its accounts, by role, under `accounts`:

```json
{ "lamports": 5000, "accounts": { "source": "<from>", "destination": "<to>" } }
{ "memo": "invoice #42", "version": "v2", "accounts": { "signers": ["<signer>"] } }
```

Roles the program finds by signature rather than position (the base account
of `CreateAccountWithSeed`, a nonce authority, ...) are `null` when absent;
any other missing account, truncated data, a seed or a
memo that is not UTF-8 is a decode error: logged, counted in
`decode_errors`, and the row is left undecoded. An unknown System tag counts
as `no_decoder_found`. SOL transfers produce no `token_transfers` row; that
table is for token accounts.

## On-disk layout

```text
{data_dir}/
  changelog.jsonl       change events, one numbered JSON envelope per line
  cursor.json           resume point: last processed and last flushed slot
  skipped_slots.log     one line per skip: {unix_time},{slot},{reason}
  reconcile.json        reconciler state: progress, strikes, attempts, rewinds
  finality.json         finalized watermark and last announced Final (persistent tracker)
  solana/
    slots/date=YYYY-MM-DD/slots_{from}_{to}.parquet
    transactions/date=YYYY-MM-DD/txs_{from}_{to}.parquet
    instructions/date=YYYY-MM-DD/ixs_{from}_{to}.parquet
    token_transfers/date=YYYY-MM-DD/transfers_{from}_{to}.parquet
    token_metadata/date=YYYY-MM-DD/metadata_{unix_ms}_{seq}.parquet
```

Slot ranges include both ends. The date is the UTC day of the first slot in
the batch. `token_metadata` is the exception: it is not keyed by slot, its
date is the day of the write, and rollback never enters it. List-shaped and decoded columns are stored as JSON strings, so
any Parquet reader can open the files.

## Reconciliation

PRISM writes at `confirmed`, so a stored slot can turn out to belong to a
fork the cluster abandoned. `Reconciler` looks back at slots that are both
flushed (per the cursor file) and finalized, and compares three sets: slots
in the `slots` Parquet table, slots the finalized chain has a block for
(`getBlocks`), and slots the skip log marks `validator_skipped`.

| Stored | On chain | Skip logged | Finding | Acted on |
|--------|----------|-------------|---------|----------|
| yes | yes | any | ok, if the same block (below) | |
| yes | yes, another block | any | `mismatched` | yes |
| yes | no | any | `orphaned` | yes |
| no | yes | no | `missing` | yes |
| no | yes | yes | `false_skip` | yes |
| no | no | yes | ok | |
| no | no | no | `unlogged_skip` | no, counted only |

A slot that is stored and on the chain must also hold the *same* block. Its
stored `parent_slot` must be the previous slot the finalized chain has a
block for (free: the block list already says), looking up to
`lineage_lookback` (512) slots below the window for the first one. A child
of an orphan of the same window is not reported separately: the rewind to
the orphan re-ingests it. With `verify_blockhashes` on, every stored
blockhash is also compared with the finalized block's, one header request
per stored slot; that also catches a duplicate block with the right parent,
which the free check cannot see. It is off by default because it costs as
many requests as the window has blocks, shared with ingestion.

A pass runs when at least 20 new safe slots are waiting, and examines at
most 2,000. A finding is acted on only when the next pass reports it again
with the same kind. If more than half of a window disagrees, the reconciler
refuses and halts (`halted: mass_mismatch`): a wrong RPC or data directory
is more likely than that many forks. The first pass pins the cluster's
genesis hash, and a different one later also halts it
(`halted: cluster_mismatch`). A halt holds across restarts until an operator
clears `halted` in `reconcile.json`. Any read failure (RPC, Parquet, skip
log) makes the pass inconclusive: nothing is concluded or changed.

Acting means a rewind to the lowest confirmed slot:

1. `in_flight` is written to `reconcile.json`.
2. The ingestor stops its pipeline without flushing, emits the `Correction`
   and flushes the change log, lowers the cursor to `from_slot - 1`,
   deletes every file at or above `from_slot`, and resumes. Nothing of the
   stopped pipeline can be announced after the correction. Slots already
   finalized are now fetched at `finalized`, so the orphan cannot be read
   again.
3. The attempt is recorded and `in_flight` cleared.

If 2 fails, the error halts PRISM and `in_flight` stays set.
`Ingestor::run` then announces the correction again (at least once: the
crash may have come before it), finishes 2 and records 3 before its
pipeline starts. The reconciler reloads `reconcile.json` before every pass
and before recording a rewind, so a rewind recovery already recorded is
adopted, not counted twice.

Finality decides which slots are examined, so it has to survive a crash
too: the reconciler only looks at slots below the *durable* finalized
watermark (`CommitmentTracker::durable_finalized_slot`, what
`finality.json` holds). After a restart the tracker starts from that file,
so the slots of an interrupted rewind are read at `finalized` from the very
first fetch, even before the first poll succeeds.

Limits keep it from looping: a slot is corrected at most twice, never when
it is more than 5,000 slots below the cursor, and at most 3 rewinds happen
per hour (after that, confirmed findings wait). A finding past the first two
limits is `unresolved`: it stays in `reconcile.json` and holds
`reconciled_through` below it until the slot is found ok again.

Wiring it up next to an ingestor:

```rust
let tracker = Arc::new(CommitmentTracker::spawn_persistent(
    Arc::clone(&adapter),
    Arc::clone(&changelog),
    DEFAULT_POLL_INTERVAL,
    data_dir.join(FINALITY_FILE_NAME),
)?);
let ingestor = Arc::new(Ingestor::new(Arc::clone(&adapter), tracker, Arc::clone(&changelog), config)?);
let genesis = adapter.genesis_hash().await?;
let reconciler = Reconciler::new(
    ReconcileConfig::default(),
    Arc::clone(&adapter),
    Arc::new(ingestor.reconcile_context(genesis)),
    Arc::new(ingestor.rewinder()),
    Arc::clone(&changelog),
    &data_dir,
)?;
let handle = reconciler.handle(); // stats() and state() for dashboards
tokio::spawn(reconciler.run(shutdown.clone()));
```

## Token metadata

`MetadataEnricher::start(config, adapter, data_dir, metrics, shutdown)`
returns a `MetadataHandle` at once and never fails: an invalid config gives
a handle that is `Disabled` and drops every hint. Attach it with
`Ingestor::with_metadata(handle.clone())`; after each successful flush the
writer task calls `hint(mint, slot)` for every mint of the flushed
transfers. `hint` is a `try_send` on a bounded queue (10,000): it never
waits, and a full queue drops the hint.

The task drops mints already written or negative-cached, batches the rest
(100 mints or 2 s), makes at most one `getMultipleAccounts` per second for
their metadata PDAs (`["metadata", program, mint]` under
`metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s`) and accepts an account only
if it is owned by the program, is a `MetadataV1`, names the requested mint,
and has a name, symbol and URI within 32, 10 and 200 bytes and valid UTF-8.
Strings are stored exactly as on chain, minus the trailing NUL padding.
Rows are flushed every 5 minutes or at 1,000 rows, and at shutdown.

```text
DeepAnaTokenMetadata { mint_address, metadata_account, name, symbol, uri: Utf8,
                       observed_slot: UInt64 }      -- all not null
```

A mint is written once, and the row is a snapshot of what was first read.
Write-once rests on the files: on startup every `token_metadata` file is
read and its mints form the known set; a mint joins it when it is buffered,
so a repeat hint cannot produce a second row. Files are written to a `.tmp`,
fsynced and renamed. On devnet most mints have no metadata, so a low fill
rate (`written` against `not_found`) is normal.

| Failure | What happens |
|---------|--------------|
| Queue full | The hint is dropped and counted; the next flush that names the mint hints it again. |
| RPC error, timeout, rate limit | Retryable errors put the mints back after a backoff (2 s doubling to 60 s, or the server's `Retry-After`), up to 5 attempts, then `exhausted`. Others drop the batch. Counted by class. |
| No account, or a rejected one | Counted (`not_found`, `rejected` by reason) and not asked for again for an hour. In memory only. |
| Flush fails | Rows stay buffered, retried one interval later; after 10 in a row the enricher is `Disabled`. Their mints are in no file, so the next run fetches them again. |
| Unreadable file at startup | `Disabled`, naming the file, which is left alone: with the known set incomplete a mint could be written twice. |
| Shutdown | Pending hints dropped, buffered rows flushed. |
| Rewind, reconcile pass, backfill | Nothing: the table is outside rollback and re-ingested mints are already known. |

`handle.stats()` holds every counter; pass it as `Sources::metadata` to
expose them through `Metrics` (`prism_metadata_*_total`). The adapter's
`getMultipleAccounts` calls are also recorded in `metrics`.

## The change log file

`LocalChangeLog::open(data_dir, chain)` opens `{data_dir}/changelog.jsonl`
(creating it if needed) and holds an exclusive lock on it until dropped, so
only one PRISM can write a data directory. Each line is one event wrapped in
an envelope:

```text
{"id":7,"emitted_at":1761040000,"chain":"solana-devnet","event":"skip","slot":350123457,"reason":"validator_skipped"}
```

- **Ids** start at 1 and go up by one per event, in file order, however many
  producers emit at once. On open, numbering continues after the last
  complete line, so ids have no gaps across restarts either.
- **Crash safety.** Each line is written with one unbuffered `write_all`. A
  partial last line left by a crash is cut off on the next open. Any other
  bad line (unparseable, out of sequence, another chain) makes `open` fail.
- **Durability.** An emitted event survives a process crash right away and
  a power failure after the next `flush`. The writer task flushes before it
  moves the cursor, and the finality poller flushes once per poll.
- **Failure.** If a write or sync fails, the partial line is cut off and the
  log refuses every later call with `PrismError::ChangeLogError`, which names
  the original failure. Fix the cause and restart.

`lens --parquet-directory DIR` opens the log at `DIR/changelog.jsonl`, the
file FORGE reads through `FORGE_CHANGELOG_PATH`.

## Tests

Each module has an integration test file in `tests/`:

```sh
cargo test -p prism
```

Two files check the guarantees end to end rather than a module:

- `completeness_test.rs` runs a block of legacy and v0 transactions (lookup
  tables, CPIs, Token-2022 with a transfer fee, created and closed accounts,
  a failed transaction, a vote) through the real ingestor and reads the
  Parquet back: every transaction and every transfer exactly once with every
  column as expected, and, per transaction, the stored transfers add up to
  the balance changes the chain reported.
- `metadata_test.rs` checks the PDA against real mainnet metadata accounts
  (USDC, BONK, JUP, in `tests/fixtures/metadata/`) and an independent
  SHA-256 derivation, rejects every malformed account for its own reason,
  times `hint` against an RPC that never answers, and covers write-once
  across restarts and metadata surviving a live rewind.
- `finality_reconcile_test.rs` covers `finality.json`, the persistent
  tracker, the lineage and blockhash checks, the ordering of `Correction`
  against re-announcements, and crashes at every step of a rewind with the
  ingestor and the reconciler live together.

Tests whose names start with `live_` run against Solana devnet through
Helius RPC
