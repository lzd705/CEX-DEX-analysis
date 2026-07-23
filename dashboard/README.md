# CEX / DEX Market Monitor

The public frontend presents market facts only. Administrator controls live on
a separate server-authenticated page.

## Required local data

Place these two detailed daily files in `data/local/`:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`

The directory is ignored by Git. Import a reviewed A-side snapshot with:

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
CSVs back into `data/local/`.

## Display contract

- The global time window is the first control in the main content.
- Tokens default to descending selected-market USD volume.
- `综合`, `CEX`, and `DEX` change the sorting scope.
- Each token can select one CEX pair and one DEX pool.
- Spread is calculated from those two selected prices and appears only on the
  token summary row.
- Missing values stay `N/A`; CEX-inapplicable TVL and row-level spread use `--`.
