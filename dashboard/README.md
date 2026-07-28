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
- Each Screener row uses the server-selected primary CEX and DEX market for its
  compact price, depth, and TVL display. Aggregate CEX/DEX volume and DEX share
  still cover every cataloged market in the selected daily window.
- Primary Price Gap is primary DEX price / primary CEX price - 1 on their latest
  common UTC date. It is independent of the workspace A/B pair.
- Inside one Token workspace, Market A and B may be any two distinct cataloged
  markets for that Token, including CEX/CEX or DEX/DEX.
- The selected daily window is shared across Markets, Compare, Liquidity &
  Execution, and Data Quality links. Liquidity/depth/execution values remain
  independently timestamped latest snapshots; preserving the window keeps the
  workspace header and window-derived market facts consistent across pages.
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
  then displays both the latest point-in-time depth profile and the independent
  unfilled daily price/volume series.
- Initial A/B choices prefer markets with integrity-valid measured depth and no
  depth-relevant quality flags. The daily comparison uses those same selected
  market IDs, so its default can differ from the quality-weighted primary
  market. Manual selection can still choose any cataloged market and exposes
  unsupported or failed depth as `N/A`.
- The depth profile plots only the four observed 10/25/50/100 bps thresholds;
  markers are never connected or interpolated. Total depth is the default.
  Directional mode normalizes CEX bid and DEX sell depth as `sell Token`, and
  CEX ask and DEX buy depth as `buy Token`.
- Log scale is the default for cross-market comparison. Real zero depth remains
  on a labeled zero rail instead of receiving an artificial positive value.
  Missing, unsupported, failed, and not-cataloged depth stays `N/A`.
- An incomplete band is an observed lower bound, displayed with `>=` in exact
  values and a dashed ring in the plot. The table, source method, block where
  applicable, snapshot timestamps, and A/B snapshot skew remain visible for
  audit.
- The daily date controls do not change the latest depth snapshots, and the two
  selected snapshots are not claimed to be synchronized.
- `/api/markets/summary?start=...&end=...` is the only fact payload loaded by
  the Screener. It contains one compact row per Token, server-computed
  aggregates and primary-market metrics, but no daily `price_points` or
  all-market arrays.
- `/api/markets/catalog?token=...&start=...&end=...` returns the complete market
  identities, point-in-time facts, lineage, and compact metrics for the
  requested daily window for exactly one Token. The frontend loads it only
  after entering that Token's workspace and retains at most eight
  Token-window-generation catalogs in an LRU cache.
- `/api/markets/catalog` without a Token remains the backward-compatible full
  audit entrypoint. The public frontend never downloads it. `/api/markets/compare`
  accepts only cataloged market IDs for the requested Token.
- Daily metrics follow the selected UTC window. Catalog/quality counts cover
  the full available catalog, while TVL, depth, and execution cost remain the
  latest independently observed snapshots. These scopes are declared in the
  response metadata rather than implied by the date toolbar.
- `/api/markets/execution-cost` accepts the same exact Token/A/B identities and
  returns the separate long-form $1k/$5k/$10k/$50k/$100k quoted-cost facts.
  It states CEX/DEX fee scope, exclusions, source snapshots, partial reasons,
  and snapshot skew; it never derives cost from the four depth markers.
  Requested notionals are JSON numbers, while measured Decimal facts are exact
  base-10 strings (or `null`) to avoid silent IEEE-754 precision loss.

Catalog version 2 distinguishes a stable DEX `pool_id` from a globally unique
token-price-series `market_id`. This prevents one pool observed from both token
perspectives, or a pool whose displayed fee label changes, from producing
ambiguous selectors.

Public fact responses use a signature-aware, one-minute in-process cache with
64 bounded serialized entries. The cache key includes every published daily,
TVL, CEX-depth, DEX-depth, and execution-cost source, so a changed snapshot
invalidates the Screener summary, every Token catalog, assembled payloads, and
compressed JSON responses together. Concurrent cold misses are single-flight
to avoid duplicate builds and gzip work. The summary exposes a path-free data
generation hash so the browser can discard cached Token catalogs after a
publication changes.

The split primarily reduces network transfer and browser memory. Cold summary
and Token-catalog construction still reuses the shared full fact/catalog
builders, so backend query-level partitioning remains a separate optimization.

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
