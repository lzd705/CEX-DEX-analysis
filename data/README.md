# Data Management

Code and data have separate lifecycles. GitHub stores schemas, collectors,
validation code, and documentation. Runtime market data stays outside Git.

## Directory contract

| Path | Purpose | Git |
| --- | --- | --- |
| `data/raw/` | Unmodified source responses or manual exports | ignored |
| `data/processed/` | Pipeline output awaiting publication | ignored |
| `data/local/*.csv` | Current reviewed, auditable source snapshot | ignored |
| `data/local/market_facts.sqlite3` | Indexed database read by the website | ignored |
| `data/local/admin/jobs/` | Refresh job state and server-only logs | ignored |
| `data/schema/` | Versioned SQLite schema | tracked |
| `data/public/` | Documentation for intentionally public artifacts | tracked |

The two reviewed CSV inputs are:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`

Point-in-time TVL has a separate lifecycle from daily OHLCV:

- `data/processed/dex_pool_tvl_snapshot.csv` is the current collection awaiting
  review;
- `data/local/dex_pool_tvl_latest.csv` is the complete latest pool snapshot used
  by the website;
- `data/local/dex_pool_tvl_history.csv` is append-only normalized history;
- `data/raw/tvl/<snapshot_id>/` retains source responses and a manifest.

TVL publication does not rebuild the historical SQLite database because a
current source-reported reserve is not a daily historical fact.

CEX order-book depth also has a separate point-in-time lifecycle:

- `data/processed/cex_depth_snapshot.csv` is the current collection awaiting
  review;
- `data/local/cex_depth_latest.csv` is the latest market snapshot used by the
  website;
- `data/local/cex_depth_history.csv` is append-only normalized history;
- `data/raw/cex-depth/<snapshot_id>/` retains public order-book responses,
  quote-conversion responses, failures, and a manifest.

Depth publication does not rebuild the historical SQLite database. The server
overlays the latest snapshot on the cataloged CEX market identities.

DEX fixed-block depth has the matching point-in-time lifecycle:

- `data/processed/dex_depth_snapshot.csv` is the current calculation awaiting
  review;
- `data/local/dex_depth_latest.csv` is the latest complete pool inventory;
- `data/local/dex_depth_history.csv` is append-only normalized history;
- `data/raw/dex-depth/<snapshot_id>/` retains fixed-block JSON-RPC transcripts
  and a manifest.

Fixed-notional quoted execution cost is published independently from both
catalog and depth rows:

- `data/processed/{cex,dex}_execution_cost_snapshot.csv` contains the current
  calculation;
- `data/local/{cex,dex}_execution_cost_latest.csv` contains exactly ten
  scenarios per current market.

The execution files reuse the corresponding depth raw hash and source snapshot.
They are validated as five notionals by two directions per market. Partial,
unsupported, and failed full-request costs remain blank. The public endpoint
keeps measured Decimals as exact base-10 strings rather than lossy JSON floats.

There is intentionally no monolithic execution-cost history CSV. Hourly
market-by-direction-by-notional rows would make that file grow without bound
and force every collection to reread, sort, and rewrite all history. Raw depth
transcripts and manifests remain available for audit. A future historical store
must use immutable date/snapshot partitions with an explicit retention policy.

CSV remains the interchange and audit format. Daily market APIs do not scan
their source CSVs when `market_facts.sqlite3` is present; point-in-time TVL,
depth, and execution overlays read their separately validated latest files.

## Publish workflow

```bash
python3 scripts/import_local_snapshot.py data/processed
python3 scripts/market_database.py data/local --status
```

The importer validates both complete schemas, builds a new SQLite file in a
staging directory, checks row counts and `PRAGMA integrity_check`, publishes
the reviewed CSV copies, then atomically replaces the runtime database as the
final commit point. A failed database build leaves the previous runtime
database untouched.

Each database contains:

- normalized Token, CEX daily, and DEX pool daily tables;
- indexes on date and Token/date;
- source filenames, byte counts, and SHA-256 hashes;
- immutable dataset snapshot records;
- import-run history and one explicit current-state pointer.

Coordinated collection state lives under `data/local/collection/`:

- `runs/<run_id>/manifest.json` records the ordered commands, exit codes,
  durations, log hashes, fact-file hashes, coverage, and freshness for one run;
- `runs/<run_id>/*.log` retains complete collector output;
- `latest.json` points to the latest completed collection manifest;
- `collection.lock` prevents daily and hourly timers from writing concurrently.

The cycle manifest does not claim a cross-source atomic transaction. Daily
SQLite, TVL, CEX/DEX depth, and each CEX/DEX execution latest snapshot retain
their own atomic publication boundary.

The server opens the database read-only. Set `MARKET_DATABASE` to select a
specific file, or `MARKET_DATA_DIR` to select the directory containing it.
Explicit `MARKET_CEX_DATA` and `MARKET_DEX_DATA` enable the CSV fallback.
