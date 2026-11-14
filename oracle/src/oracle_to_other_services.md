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
- **Pipeline quality checks.** Slot gaps, unresolved instruction types, and transfers missing decimals or mints.

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