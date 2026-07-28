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

Local startup binds to `127.0.0.1` by default. Keep the host process on
loopback in production; the Nginx reverse proxy is the only public listener.
The container image binds inside its own network namespace, so publish its
host port to `127.0.0.1` only, as shown in `dashboard/PUBLIC_SHARING.md`.

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

The administrator page and every `/api/admin/*` route return 404 by default.
After setting `ADMIN_ENABLED=true` and configuring login mode as described in
`docs/admin-operations.md`, open
`http://127.0.0.1:8765/admin.html`.

An open administrator workspace is limited to isolated local development. It
requires `ADMIN_ENABLED=true`, `ADMIN_LOGIN_REQUIRED=false`, and
`ADMIN_ALLOW_OPEN_LOCAL=true`; the server rejects it on a non-loopback bind.
Do not use open mode behind a reverse proxy.

With login enabled, the backend validates the session and CSRF token. In both
modes it validates the configured Token and refresh window before starting a
one-at-a-time pipeline job. Successful jobs atomically publish the two detailed
CSVs and a validated SQLite database back into `data/local/`.

## Display contract

- The global time window is the first control in the main content.
- Tokens default to descending aggregate USD volume across all cataloged
  markets. Aggregate, CEX, and DEX change the token-level sorting scope.
- Each token can select one CEX pair and one DEX pool.
- Token-level aggregate CEX/DEX volume and DEX share do not change when a
  different pair or pool is selected. Expanded rows show selected-market facts.
- Spread is calculated from those two selected prices and appears only on the
  token summary row.
- Window return uses first-to-last observed close. Daily realized volatility
  uses only adjacent UTC-day log returns; intervals across missing days are
  excluded and reported in coverage metadata.
- Missing values stay `N/A`; CEX-inapplicable TVL and row-level spread use `--`.
- TVL is a source-reported point-in-time pool snapshot. It is neither a
  historical daily series nor executable depth.
- The shared depth column displays point-in-time CEX order-book depth for CEX
  rows and fixed-block DEX pool-state depth for supported DEX rows at
  10/25/50/100 bps. TVL is never converted into depth.
- DEX protocols without an audited adapter display `N/A`, not a TVL-based
  estimate.
- The comparison workbench selects one Token and any two cataloged markets,
  then displays unfilled daily closes, daily USD volume, absolute USD spread,
  and midpoint-relative bps.
- `/api/markets/catalog` is the audit entrypoint; `/api/markets/compare` accepts
  only cataloged market IDs for the requested Token.

Catalog version 2 distinguishes a stable DEX `pool_id` from a globally unique
token-price-series `market_id`. This prevents one pool observed from both token
perspectives, or a pool whose displayed fee label changes, from producing
ambiguous selectors.

Public fact responses use a signature-aware, one-minute in-process cache. The
cache key includes every published daily, TVL, CEX-depth, and DEX-depth source,
so a changed snapshot invalidates both the assembled payload and compressed
JSON response. Concurrent cold misses are single-flight to avoid duplicate
catalog builds and gzip work.

## Production boundary

Run the application on loopback under
`deploy/systemd/cex-dex-dashboard.service.in` and expose only the read-only
dashboard through `deploy/nginx/cex-dex-dashboard.conf.in`. The proxy provides
HTTPS, access logs, and rate limiting while blocking all administrator routes.
See `docs/production-hardening.md` for installation, health checks, rollback,
cache generations, and the dry-run-first CEX-depth raw-response retention
script and systemd timer:
`scripts/retain_cex_depth_raw.py` and
`deploy/systemd/cex-dex-cex-depth-retention.service.in` plus
`deploy/systemd/cex-dex-cex-depth-retention.timer`.
