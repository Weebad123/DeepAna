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
| `types` | The shared contracts. `udm` defines the four row types (`DeepAnaSlot`, `DeepAnaTransaction`, `DeepAnaInstruction`, `DeepAnaTokenTransfer`) and their Arrow schemas. `event` defines `ChangeEvent` (`New`, `Skip`, `Final`). `error` defines `PrismError`, the single error type every module returns. |
| `adapter` | The boundary with the chain. `ChainAdapter` is the chain-agnostic trait (tip, block, accounts). `SolanaAdapter` implements it, adding rate limiting, retries and error mapping. |
| `slot_processor` | Takes one slot number to one processed slot. `SlotFetcher` fetches the block with retries. `GapHandler` records skipped or unreachable slots as `Skip` events and in an audit log. |
| `normalizer` | Splits a raw block into a slot row, transaction rows and a flattened list of instruction rows (inner instructions included). A block is normalized in full or not at all. |
| `decoder` | `DecoderRegistry` routes each instruction to the decoders registered for its program. `SplTokenDecoder` handles SPL Token and Token-2022 and produces token transfer rows. `AnchorDecoder` decodes any Anchor program from its IDL, `emit_cpi!` events included. |
| `account_resolver` | Fills in the mint decimals of token transfers. `MintCache` ensures each mint is fetched from the RPC only once and remembers recent misses for a while. |
| `commitment` | Tracks the boundary between `confirmed` and `finalized` slots and emits a `Final` event whenever the finalized watermark advances. It can say whether a slot is finalized, but the UDM rows do not carry an `is_finalized` column yet, so nothing is stamped onto the Parquet files. |
| `storage` | `Storage` is the backend trait. `ParquetWriter` buffers rows in memory and on each flush writes one file per table. `PartitionKey` computes the file paths. Flushes are all-or-nothing, and `rollback` removes slots at or above a given slot (e.g. after a fork). |
| `changelog` | `ChangeLog`, the async sink every change event goes through. Three producers publish to it: the gap handler (`Skip`, as soon as a slot is found skipped or unreachable), the ingestor's writer task (`New`, one per slot once its files are on disk) and the commitment tracker (`Final`). No implementation ships yet: the local file sink (`local.rs`) is still empty, so callers supply their own. |
| `ingestor` | The orchestration layer. It builds and owns the slot processor, decoder registry, account resolver and `ParquetWriter`, and runs them as two Tokio tasks joined by a bounded channel of `ProcessedSlot`s: *process* (fetch, normalize, decode, resolve) and *writer* (append, flush, announce, move the cursor). It persists an `IngestorCursor` so a restart resumes after the last flushed slot. It also builds the Helius-backed adapter. See [Running the ingestor](#running-the-ingestor). |



`IngestorConfig::new(data_dir)` keeps everything under one directory: the
Parquet tables, the cursor and the skip log. `Ingestor::with_decoders` swaps
in a registry with extra decoders (e.g. `AnchorDecoder`s).

What a run does:

- **Process task.** Follows the tip at the configured commitment and runs
  each slot through the slot processor. The fetcher retries transient
  failures up to `fetcher.max_retries`. A skipped slot is recorded by the gap
  handler. Slots with data are decoded and their token transfers resolved,
  then sent to the writer.
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
| Change log refuses an event | The pipeline halts and the cursor does not move. A restart writes and announces those slots again. |
| Crash between a flush and the cursor save | On startup, `run()` deletes files at or above its first slot, then ingests those slots again. Every slot ends up on disk once. |

### Change events

| Event | Emitted by | When |
|-------|------------|------|
| `Skip` | gap handler | As soon as a slot is found skipped (`validator_skipped`) or unreachable after retries (`rpc_unavailable`). |
| `New` | writer task | Once per slot with data, after its flush succeeded. Carries the paths of every file in that flush. |
| `Final` | commitment tracker | When the finalized watermark advances. |

Delivery is at least once: a `New` event can repeat after a restart. A
`Skip` can arrive before the `New` events of earlier slots that are still
buffered, so consumers that need strict slot order should sort by slot.

## On-disk layout

```text
{data_dir}/
  cursor.json           resume point: last processed and last flushed slot
  skipped_slots.log     one line per skip: {unix_time},{slot},{reason}
  solana/
    slots/date=YYYY-MM-DD/slots_{from}_{to}.parquet
    transactions/date=YYYY-MM-DD/txs_{from}_{to}.parquet
    instructions/date=YYYY-MM-DD/ixs_{from}_{to}.parquet
    token_transfers/date=YYYY-MM-DD/transfers_{from}_{to}.parquet
```

Slot ranges include both ends. The date is the UTC day of the first slot in
the batch. List-shaped and decoded columns are stored as JSON strings, so
any Parquet reader can open the files.

## Tests

Each module has an integration test file in `tests/`:

```sh
cargo test -p prism
```

Tests whose names start with `live_` run against Solana devnet through
Helius RPC
