# CEX-DEX Market Monitor 1.0

## Product boundary

The first product is a fact-only Market Monitor. A user selects a time window,
a Token, one CEX pair, and one DEX pool, then compares source-backed market
facts. A separate authenticated administrator page can refresh configured
Tokens. This release does not include factors, future-return research, event
study, or public data-edit controls.

## Current requirements

1. Show explicit CEX exchange/pair and DEX chain/protocol/pool identities.
2. Put the global start and end date controls at the top of the page.
3. Display price, selected-window return, daily realized volatility, USD
   volume, DEX TVL snapshot, and CEX-DEX price spread.
4. Keep spread at Token comparison level, not duplicated as a CEX or DEX
   property.
5. Sort descending by USD volume by default, with `综合`, `CEX`, and `DEX`
   sorting scopes.
6. Show data coverage, latest date, source file versions, missing values, and
   metric limitations.
7. Preserve missing values as null. Never replace unavailable facts with zero.
8. Keep administrator authentication and refresh APIs separate from the public
   Market Monitor.
9. Publish an auditable market catalog and a two-venue daily comparison that
   exposes raw closes, daily USD volume, absolute price spread, and
   midpoint-relative bps.

## Local workflow

```bash
# Refresh facts from source APIs. This does not build factors.
python3 scripts/run_fact_pipeline.py

# Refresh source-reported point-in-time TVL for every published DEX pool.
python3 scripts/fetch_tvl.py --publish-local

# Import a reviewed snapshot and atomically publish the indexed runtime database.
python3 scripts/import_local_snapshot.py data/processed

# Start the local monitor.
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

The application code and SQLite schema are versioned in GitHub. Reviewed CSV
inputs and the generated `market_facts.sqlite3` runtime database live in the
ignored `data/local/` directory, or an external directory selected by
`MARKET_DATA_DIR`. The website queries SQLite; it does not rescan all CSV rows
on each request. See `data/README.md` for the full data lifecycle.
The public catalog and comparison contract is documented in
`docs/market-facts-contract.md`.
The separate point-in-time TVL lifecycle and missing-value rules are documented
in `docs/tvl-data-contract.md`.

Administrator setup is documented in `docs/admin-operations.md`. The page is
served at `/admin.html`. It supports password authentication by default or an
explicit no-login mode through `ADMIN_LOGIN_REQUIRED=false`.

## Future scope

Events, depth, slippage, fee/gas data, anomaly rules, historical TVL backfills,
historical-end-date
backfills, and adding previously unconfigured Tokens require separate data
contracts and acceptance tests.
