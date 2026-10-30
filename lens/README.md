# lens

LENS is DeepAna's query service. It serves read-only SQL over HTTP against
the Parquet tables PRISM writes. Queries run on DataFusion. LENS writes
nothing to the data directory: it re-scans it on a timer to pick up the
files PRISM has added since the last scan.

```text
  PRISM ──▶ {data_dir}/solana/{table}/date=YYYY-MM-DD/*.parquet
                              │
                              ▼
  ┌─ QueryEngine ────────────────────────────────────┐
  │  PrismCatalog     solana.{slots, transactions,   │◀── refresh every
  │                     instructions, token_transfers}│    catalog_refresh_interval_secs
  │  SessionContext   read-only, row/time/memory caps │
  └──────────────────────────────────────────────────┘
                              │ QueryResult (Arrow)
                              ▼
                     serialize: JSON / CSV
                              │
                              ▼
              POST /query     GET /health     (axum)
```

## Modules

| Module | What it does |
|--------|--------------|
| `config` | `LensConfig`, read once from `LENS_*` environment variables and validated at startup. It can't be changed after loading. |
| `catalog` | `PrismCatalog` registers each PRISM table under the `solana` schema with a fixed Arrow schema that mirrors PRISM's UDM. A refresh re-registers every table so new files become visible, adds tables whose directory has appeared, and drops tables whose directory has gone. |
| `engine` | `QueryEngine` owns the DataFusion session and the catalog. Statements are planned read-only, so DDL, DML, `COPY` and `SET` are rejected. Each query is capped at `max_query_rows`, `query_timeout_secs`, and a memory pool that spills to disk. |
| `result` | `QueryResult` holds the Arrow batches. `serialize` turns them into JSON or CSV. 64-bit integers stay exact and timestamps are RFC 3339. |
| `api` | The axum router, `serve` and `shutdown_signal`. Shutdown lets in-flight requests finish, then stops the refresh task. |
| `error` | `LensError`, the single error type every module returns. Each variant has a stable `kind()` that appears in logs and API errors. |

## Running

`cargo run` from the workspace root starts LENS but you need to configure the directory LENS will serve.

```sh
# LENS and PRISM together: PRISM writes to directory_you_specify and LENS serves that directory.
# configure your Environment RPC API KEY . I use Helius RPC, so i can do something like:
export HELIUS_API_KEY="my-key-here"
cargo run -- --parquet-directory <directory_you_specify>

```

With `--parquet-directory`, PRISM's ingestor runs in the same process. It
follows the Solana devnet tip and writes its tables, cursor and skip log
under the given directory, and LENS uses that directory as `LENS_DATA_DIR`. 
Ctrl-C or SIGTERM stops both, and so does either one failing.

## Configuration

| Variable | Default | Valid values |
|----------|---------|--------------|
| `LENS_DATA_DIR` | required (unless `--parquet-directory`) | existing directory with at least one PRISM table under `solana/` |
| `LENS_BIND_ADDRESS` | `127.0.0.1:8080` | `ip:port`, port not 0 |
| `LENS_MAX_QUERY_ROWS` | `10000` | `1..=100000` |
| `LENS_QUERY_TIMEOUT_SECS` | `30` | `1..=300` |
| `LENS_CATALOG_REFRESH_INTERVAL_SECS` | `30` | `1..=3600` |
| `LENS_LOG_LEVEL` | `info` | `trace` `debug` `info` `warn` `error` |
| `LENS_CHAIN` | `solana` | `solana` |

A variable that is set but blank is rejected rather than treated as unset.

## API

### `POST /query`

```sh
curl -s localhost:8080/query \
  -H 'content-type: application/json' \
  -d '{"sql": "SELECT slot, transaction_count FROM solana.slots ORDER BY slot DESC LIMIT 5"}'
```

`format` is `"json"` (the default) or `"csv"`. A JSON response looks like:

```json
{"rows":[{"slot":295000000,"transaction_count":1234}],"row_count":1,"duration_ms":47,"truncated":false,"sql":"SELECT ..."}
```

Both formats also return `x-lens-row-count`, `x-lens-truncated` and
`x-lens-duration-ms` headers. Errors return `{"error", "error_kind", "sql"}`
with a status code that says whose fault the failure was:

| Status | When |
|--------|------|
| 400 | Bad request body, unknown `format`, SQL that doesn't parse or plan, a write statement, or SQL that fails on the data (bad cast, division by zero) |
| 504 | The query ran longer than `query_timeout_secs` |
| 503 | The query ran out of memory or spill space |
| 500 | Anything else: I/O errors, unreadable Parquet files, panics |

### `GET /health`

Always returns 200. The verdict is in `status`, alongside per-table
registration and file counts, catalog refresh stats and query metrics:

| `status` | When |
|----------|------|
| `unhealthy` | No table is registered, or the table directories can't be read |
| `degraded` | A table has a directory but isn't registered, or its last refresh failed |
| `healthy` | Otherwise |

A missing table directory doesn't count against health, because PRISM only
creates a table's directory once it has a row for it.

## Tests

Each module has an integration test file in `tests/`:

```sh
cargo test -p lens
```

