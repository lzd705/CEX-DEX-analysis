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

## Local workflow

```bash
# Refresh facts from source APIs. This does not build factors.
python3 scripts/run_fact_pipeline.py

# Import a reviewed snapshot without committing CSVs to Git.
python3 scripts/import_local_snapshot.py data/processed

# Start the local monitor.
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

The application code is versioned in GitHub. Runtime CSVs live in the ignored
`data/local/` directory or an external directory selected by
`MARKET_DATA_DIR`.

Administrator setup is documented in `docs/admin-operations.md`. The page is
served at `/admin.html` but remains disabled until a password hash is supplied.

## Future scope

Events, depth, slippage, fee/gas data, anomaly rules, historical-end-date
backfills, and adding previously unconfigured Tokens require separate data
contracts and acceptance tests.
