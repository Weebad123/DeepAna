# ORACLE

ORACLE is the analysis layer of the platform. It answers "what's happening on-chain?" by querying LENS, flagging unusual activity, and summarising what changed. It does not use an LLM, so the same input and data always produce the same output.

## How it fits with the other services
Blockchain (for now, Solana devnet)
│
▼
PRISM ──writes──▶ Parquet files + changelog.jsonl
│ │
▼ ▼
LENS ◀──queries── FORGE
▲
└──────queries── ORACLE


| Service | Role | Relationship to ORACLE |
|---|---|---|
| **PRISM** | Ingests Solana slots, normalises and decodes them, writes Parquet and a changelog | ORACLE never touches PRISM's files. It only sees PRISM's output through LENS, and its quality checks measure PRISM's pipeline from the outside |
| **LENS** | Serves SQL over PRISM's Parquet files | ORACLE's only data source. Every query and detector reads from LENS over HTTP |
| **FORGE** | Runs user-defined alerts and webhooks off the changelog | Independent of ORACLE. Both call LENS, and neither calls the other |
| **ORACLE** | Structured queries, anomaly detection, profiles, digests | This service |

ORACLE depends only on LENS. It reads no changelog, calls neither PRISM nor FORGE, and keeps its own state in `oracle.db` (SQLite).

## What it does

- **Structured queries.** A client sends a spec (measures, grouped and filtered by dimensions, over a window of slots), and ORACLE compiles it to one safe `SELECT`. Slot bounds and row limits are enforced for you. Answers are exact: slots, not times, and raw token amounts, with decimals not applied (UI units are deferred).
- **Question matching.** Short questions such as "top 10 mints by transfers in the last hour" are matched to templates. Unrecognised questions get the nearest supported ones, never a guess.
- **Anomaly detection.** It compares recent activity against its own rolling baseline (median/MAD) and flags first-seen mints and programs, and failure-rate shifts.
- **Profiles and digests.** It reports on a mint, program or address, and on what changed recently.
- **Drill-down.** Every row of an answer opens: first as the rows it counted, then each of those as a full transaction or slot report. See below.
- **Pipeline quality checks.** Slot gaps, unresolved instruction types, and transfers missing decimals or mints.

## Drill-down: from an answer to a transaction

An answer is aggregates ("mint M, 412 transfers"); no dimension is a transaction, so an answer never names one. Three routes take a client from any row of any answer to the thing itself:

| Route | Returns |
|---|---|
| `POST /drill` | The rows behind one answer row. Send the answer's `spec` (with `as_of_slot` set to the answer's) and the row's group-by values; get the newest underlying rows of the spec's table, each with its `slot` and `tx_signature`. |
| `GET /profile/transaction/{signature}?slot=` | One transaction in full: block, fee, accounts, the instruction tree, token transfers, memos, logs grouped by invocation, and for a failed transaction where and why it failed. |
| `GET /profile/slot/{slot}?after_index=&limit=` | One slot and a page of its transactions. |

```text
POST /ask  "top 10 mints by transfers in the last hour"
  └─ row: mint EPjF…, 412 transfers
       └─ POST /drill {spec, row: {"mint": "EPjF…"}}       → 50 newest transfers: slot, tx_signature, …
            └─ GET /profile/transaction/{sig}?slot={slot}  → the transaction
```

What to know:

- **Every answer column says what its values are.** Each column of a `/query` or `/ask` answer, and of a `/drill` answer, carries `entity`: `tx_signature`, `mint`, `program`, `token_account`, `slot`, or `null`. A client links a value from that alone, never from the column's name: `mint` opens `GET /profile/mint/{value}`, `program` `/profile/program/…`, `token_account` `/profile/token_account/…`, `slot` `/profile/slot/{value}`, and `tx_signature` `/profile/transaction/{value}?slot={the row's slot}`. Placeholders (`unattributed`, `unknown`) and nulls open as nothing. Owners and leaders are wallets and validators, which ORACLE has no report for, so their columns are `null`. `GET /catalog` lists the hint of every dimension. The field is additive: no existing column field changed.
- **A drill works for every value an answer can show**, including the placeholders (`unattributed`, `unknown`) and slot buckets, which a filter cannot express. Values are checked before they reach SQL, exactly as filter values are.
- **A signature does not say which slot it is in.** Pass `slot` (every drilled row carries it) and only that slot is read. Without it the last `window_slots` (default 50,000) are searched. A signature that is not there is a 200 with `found: false`.
- **The instruction tree** lists top-level instructions by `instruction_index`, each with its inner instructions by `inner_instruction_index`. When the logs line up, every instruction also gets its log lines, compute units and call depth.
- **Decoding at read time.** PRISM decodes SPL Token and the Anchor programs it has an IDL for. The report additionally decodes System, Compute Budget, Memo and Associated Token Account instructions from their raw bytes, and marks which side decoded each one (`decoded_by: "prism"` or `"oracle"`). Nothing is re-ingested.
- **Memos** come from the memo instruction's data, and from the logs only when the data is not available.
- **Failed transactions are reported in full.** The `error` section names the top-level instruction that failed and, from the logs, the innermost program that failed and its reason. Instructions after the failing one are marked `ran: false`.
- **Rows repeated by a crash replay are dropped and counted.** PRISM appends when it replays a slot, so a row can be stored twice. Reports keep one row per key by a fixed rule and say how many they dropped in `duplicates`. (Aggregates from `/query` still count the repeats.)
- **Log text is capped** at 400 lines of 1,000 characters each; the report says how much was cut. The structure is always built from every line.
- **Deterministic.** The same request with the same `as_of_slot` returns the same bytes, except `duration_ms`.
- **Bounded.** Both reports hold one of the four profile permits and share the 120-second profile deadline; a drill shares the query permits.

The code is in `profile/transaction.rs` (types and the pure assembler), `profile/logs.rs`, `profile/decode.rs`, `profile/slot.rs` and `spec/drill.rs`.

## How it runs

ORACLE is a **library crate**. It has no binary, opens no port, and is started by a host process:

```rust
let oracle = oracle::Oracle::start(config, shutdown.clone()).await?;
let app = axum::Router::new().nest("/oracle", oracle.router());
```

The host decides the port and the route prefix. ORACLE only needs LENS to be reachable.

## Configuration

All settings are environment variables prefixed `ORACLE_`. The most important is:

```bash
ORACLE_LENS_URL
```

Everything else has a default. See `config.rs` for the full list and ranges.

## Build order across the platform

`prism` → `lens` → `forge` → `oracle`. Each crate depends only on what comes before it, and `oracle` and `forge` reach `lens` over HTTP through the shared `lens-client` crate.