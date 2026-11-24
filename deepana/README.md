# deepana

One binary that runs the whole DeepAna system (PRISM, LENS, FORGE and
ORACLE) in one process on one port, with a live dashboard. COMMONS, which
is a CLI rather than a service, is run in-process on demand for the
dashboard's verify panel.

```sh
export HELIUS_API_KEY="my-key-here"     # PRISM follows the devnet tip through Helius
cargo run -p deepana -- --parquet-directory ./data --open
```

| Flag | Meaning |
|------|---------|
| `--parquet-directory <DIR>` | The data directory (required, created if missing). The same flag LENS takes. |
| `--open` | Open the dashboard in the default browser once it is up. |
| `--commons-root <PATH>`, `--repo-root <PATH>` | COMMONS repository (`artifacts/` + `fixtures/`) to verify. Default `<DIR>/commons`. Only read. Without one the panel says "not configured". |

| Variable | Default | Meaning |
|----------|---------|---------|
| `DEEPANA_PORT` | `8099` | DeepAna listens on `127.0.0.1:<port>` |
| `DEEPANA_LOG_LEVEL` | `info` | `trace` `debug` `info` `warn` `error` |

Tuning variables of the services (`LENS_QUERY_TIMEOUT_SECS`,
`FORGE_POLL_INTERVAL_MS`, `ORACLE_BUCKET_SECS`, ...) still apply. Variables
that name a location (`LENS_DATA_DIR`, `FORGE_DB_PATH`,
`FORGE_CHANGELOG_PATH`, `ORACLE_DB_PATH`, the LENS URLs, bind addresses) are
ignored: DeepAna sets them itself from `<DIR>` and its port.

## One directory

Every file any service writes is under `<DIR>`. Nothing is written to the
working directory.

```text
<DIR>/
  solana/              PRISM's Parquet tables (PRISM writes, LENS scans); nothing else
  cursor.json          PRISM cursor
  changelog.jsonl      PRISM change log (FORGE and the dashboard tail it)
  skipped_slots.log    PRISM's skip audit log (once a slot is skipped)
  forge.db             FORGE registry + forge.db.lock, forge.db-wal, forge.db-shm, forge.checkpoint.json
  oracle.db            ORACLE store + oracle.db.lock, oracle.db-wal, oracle.db-shm
```

## One port

| Path | What |
|------|------|
| `/` | The dashboard |
| `/api/lens/...` | LENS (`POST /query`, `GET /health`) |
| `/api/forge/...` | FORGE (`/queries`, `/alerts`, `/health`, ...) |
| `/api/oracle/...` | ORACLE (`/ask`, `/anomalies`, `/digest`, `/health`, ...) |
| `/api/stream` | Server-sent events feeding the dashboard |
| `/api/commons/report`, `POST /api/commons/verify` | COMMONS verify |
| `POST /api/demo/alert` | Create FORGE's demo alert (every 50th slot) |
| `POST /demo/hook` | The demo alert's webhook: checks the signature, keeps the last 50 deliveries in memory |

The demo alert's secret is generated per process and never leaves the
server: not in any response, not on disk. On startup an existing demo alert
is given the new secret and this process's webhook URL.

FORGE and ORACLE reach LENS at `http://127.0.0.1:<port>/api/lens`, through
this same port.

```sh
# Top 10 mints, in plain English, through ORACLE
curl -s localhost:8099/api/oracle/ask -H 'content-type: application/json' \
  -d '{"question": "top 10 mints by transfers in the last hour"}'

# The same over all data, in SQL, through LENS
curl -s localhost:8099/api/lens/query -H 'content-type: application/json' \
  -d '{"sql": "SELECT mint_token_address AS mint, COUNT(*) AS transfers FROM solana.token_transfers WHERE mint_token_address IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10"}'
```

## Supervision

Each service runs on a child of one cancellation token and is fail-stop: if
LENS (or PRISM inside it), FORGE, ORACLE or the HTTP server stops on its
own, DeepAna logs the cause, stops everything else (PRISM flushes and saves
its cursor first) and exits with status 1. Ctrl-C or SIGTERM stops
everything with status 0.

## The dashboard stream

One background poller feeds a broadcast channel, however many tabs are
open. It tails `<DIR>/changelog.jsonl` every second, and every 3 seconds
asks (in-process, not over TCP) LENS, FORGE and ORACLE for their health,
LENS for `MAX(slot)`, FORGE for its alerts and recent deliveries, and ORACLE
for its detectors, new anomalies and the latest digest. `/demo/hook` receipts
are pushed as they arrive. A tab gets a full snapshot when it connects.
Data quality is not on the stream: each tab fetches `/api/oracle/quality`
itself on load and every 5 minutes, one request at a time.
