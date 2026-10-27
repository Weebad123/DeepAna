# prism

PRISM is DeepAna's Solana ingestion engine. It follows the chain tip,
turns every block into rows of a fixed Universal Data Model (UDM), decodes
the instructions it recognises, and writes the rows as date-partitioned
Parquet files. Each step it takes is also published as a change event for
downstream systems.

```text
  ChainAdapter ──▶ SlotProcessor ──▶ Normalizer ──▶ DecoderRegistry ──▶ AccountResolver ──▶ Storage
   (Helius RPC)    (fetch, retry,    (block → UDM    (instruction       (mint decimals)     (Parquet)
                    skipped slots)    rows)           types, transfers)
                         │                                                                     │
                         └──────────────────────────▶ ChangeLog ◀──────────── CommitmentTracker
                                                    (New / Skip / Final events)
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
| `commitment` | Tracks the boundary between `confirmed` and `finalized` slots. It supplies the `is_finalized` flag and emits a `Final` event whenever the watermark advances. |
| `storage` | `Storage` is the backend trait. `ParquetWriter` buffers rows in memory and on each flush writes one file per table. `PartitionKey` computes the file paths. Flushes are all-or-nothing, and `rollback` removes slots at or above a given slot (e.g. after a fork). |
| `changelog` | `ChangeLog`, the sink that change events are emitted to. The local file implementation (`local.rs`) is still empty. |
| `ingestor` | The orchestration layer. It builds and owns the slot processor, decoder registry, account resolver and `ParquetWriter`, and runs them as two Tokio tasks: *process* (fetch, normalize, decode, resolve) and *writer* (append, flush, emit one `New` event per flushed slot). It persists an `IngestorCursor` so a restart resumes after the last flushed slot, and rolls back files from a flush the cursor never recorded. It also builds the Helius-backed adapter. |

## On-disk layout

```text
{data_dir}/solana/
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
