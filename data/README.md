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

CSV remains the interchange and audit format. The web API does not scan CSV
when `market_facts.sqlite3` is present.

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

The server opens the database read-only. Set `MARKET_DATABASE` to select a
specific file, or `MARKET_DATA_DIR` to select the directory containing it.
Explicit `MARKET_CEX_DATA` and `MARKET_DEX_DATA` enable the CSV fallback.
