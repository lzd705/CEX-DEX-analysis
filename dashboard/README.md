# CEX / DEX Market Monitor

The public frontend presents market facts only. Administrator controls live on
a separate server-authenticated page.

## Required local data

Import these two detailed daily files into `data/local/`:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`

The directory is ignored by Git. Importing a reviewed A-side snapshot also
builds `data/local/market_facts.sqlite3`, which is the indexed runtime store:

```bash
python3 scripts/import_local_snapshot.py /path/to/data/processed
```

Run the monitor:

```bash
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

Open `http://127.0.0.1:8765`.

Local startup binds to `127.0.0.1` by default. Set `HOST=0.0.0.0` only in a
controlled deployment environment.

To keep the code and data in separate locations, set `MARKET_DATA_DIR`:

```bash
MARKET_DATA_DIR=/srv/cex-dex/current ./scripts/run_dashboard.sh
```

The server prefers `market_facts.sqlite3` in that directory and opens it
read-only. `MARKET_DATABASE=/srv/cex-dex/current/market_facts.sqlite3` selects
a database explicitly. CSV remains the auditable input and fallback, not the
normal online query layer.

The Docker image uses the same contract and expects an external data volume at
`/app/data/local`. Use a read-only mount when administrator refresh is disabled
and a service-account-owned writable mount when it is enabled.

## Administrator page

After configuring `.env` as described in `docs/admin-operations.md`, open
`http://127.0.0.1:8765/admin.html`.

Set `ADMIN_LOGIN_REQUIRED=false` only when the deployment should intentionally
open the administrator workspace without a login.

With login enabled, the backend validates the session and CSRF token. In both
modes it validates the configured Token and refresh window before starting a
one-at-a-time pipeline job. Successful jobs atomically publish the two detailed
CSVs and a validated SQLite database back into `data/local/`.

## Display contract

- The global time window is the first control in the main content.
- Tokens default to descending selected-market USD volume.
- `综合`, `CEX`, and `DEX` change the sorting scope.
- Each token can select one CEX pair and one DEX pool.
- Spread is calculated from those two selected prices and appears only on the
  token summary row.
- Missing values stay `N/A`; CEX-inapplicable TVL and row-level spread use `--`.
- The shared depth column displays CEX order-book depth for CEX rows and
  fixed-block pool-state depth for supported DEX rows, both within ±100 bps.
- DEX protocols without an audited adapter display `N/A`, not a TVL-based
  estimate.
- The comparison workbench selects one Token and any two cataloged markets,
  then displays unfilled daily closes, daily USD volume, absolute USD spread,
  and midpoint-relative bps.
- `/api/markets/catalog` is the audit entrypoint; `/api/markets/compare` accepts
  only cataloged market IDs for the requested Token.
