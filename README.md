# CEX-DEX Market Monitor 1.0

## Product boundary

The first product is a fact-only Market Monitor. A user selects a time window
and a Token, then keeps two exact cataloged markets as Market A and Market B
while moving through `Markets`, `Compare`, `Liquidity & Execution`, `Events`,
and `Data Quality`. The A/B markets may be CEX/CEX, CEX/DEX, or DEX/DEX. A
separate authenticated administrator page can refresh configured Tokens when
explicitly enabled; the entire administrator surface is absent by default.
This release does not include factors, future-return research, an event study,
or public data-edit controls.

## Current requirements

1. Show explicit CEX exchange/pair and DEX chain/protocol/pool identities.
2. Put the global start and end date controls at the top of the page.
3. Display price, selected-window return, daily realized volatility, USD
   volume, DEX TVL snapshot, CEX and supported-pool DEX 10/25/50/100 bps depth
   snapshots, CEX-DEX price spread, and fixed-notional quoted execution-cost
   facts. Display execution price-source timing and withhold values whose
   required USD-price observation fails the two-hour temporal gate.
4. Keep spread at Token comparison level, not duplicated as a CEX or DEX
   property.
5. Sort Tokens by aggregate volume across all cataloged markets, while keeping
   the selected CEX pair and DEX pool facts explicit. Aggregate, CEX, and DEX
   sorting scopes do not redefine the selected-market rows.
6. Show data coverage, latest date, source file versions, missing values, and
   metric limitations.
7. Preserve missing values as null. Never replace unavailable facts with zero.
8. Keep administrator authentication and refresh APIs separate from the public
   Market Monitor.
9. Publish an auditable market catalog and a two-venue daily comparison that
   exposes raw closes, daily USD volume, absolute price spread, and
   midpoint-relative bps.
10. Plot the A/B daily comparison as selectable Price, Spread, and Volume
    lines. Missing, invalid, and nonconsecutive UTC dates break a line instead
    of being interpolated. Market A and B remain distinguishable by marker
    shape and labels as well as color.
11. Publish manually reviewed Event Facts from official sources with source,
    timing precision, lifecycle, and revision lineage. Event markers are a
    temporal overlay on Compare only; they do not claim return impact or
    causality.

## Local workflow

```bash
# Run the complete incremental collection cycle and publish every fact family.
python3 scripts/run_collection_cycle.py --profile full --publish-local

# Daily schedule profile: incremental CEX/DEX OHLCV plus point-in-time TVL.
python3 scripts/run_collection_cycle.py --profile daily --publish-local

# Hourly profile: CEX depth/cost, private DEX price refresh, then DEX depth/cost.
python3 scripts/run_collection_cycle.py --profile depth --publish-local

# Manual recovery profile: retry only the point-in-time TVL snapshot.
python3 scripts/run_collection_cycle.py --profile tvl --publish-local

# Manually build and publish the reviewed, append-only Event Fact bundle.
python3 scripts/event_facts.py --publish-local

# Import a reviewed snapshot and atomically publish the indexed runtime database.
python3 scripts/import_local_snapshot.py data/processed

# Start the local monitor.
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

The application code and SQLite schemas are versioned in GitHub. Reviewed
market CSV inputs and the generated `market_facts.sqlite3` runtime database
live in the ignored `data/local/` directory, or an external directory selected
by `MARKET_DATA_DIR`. The website queries SQLite; it does not rescan all market
CSV rows on each request. Event Facts use a separate immutable bundle under
`data/local/events/`, selected by an atomically replaced `latest.json` pointer.
The committed curated file currently contains 44 reviewed facts with at least
one official-source fact for each of the 30 configured Tokens. This is
Token-presence coverage, not complete historical event coverage. The website
reports the count and covered/uncovered inventory from the latest validated
bundle. See `data/README.md` for the market-data lifecycle and
`docs/event-facts-contract.md` for the Event Fact lifecycle.
The public catalog and comparison contract is documented in
`docs/market-facts-contract.md`.
The separate point-in-time TVL lifecycle and missing-value rules are documented
in `docs/tvl-data-contract.md`.
The CEX order-book bands, quote conversion, truncation rules, and raw-response
audit trail are documented in `docs/cex-depth-data-contract.md`.
The supported DEX invariant/tick models, fixed-block rule, unsupported statuses,
and execution limitations are documented in `docs/dex-depth-data-contract.md`.
The fixed-notional definition, quoted-cost formulas, fee scope, long-form
files, statuses, and hard validation gates are documented in
`docs/execution-cost-data-contract.md`.
The Event Fact taxonomy, official-source evidence, append-only revisions,
bundle publication, and null rules are documented in
`docs/event-facts-contract.md`.
Collection profiles, locks, manifests, freshness thresholds, systemd timers,
and recovery behavior are documented in `docs/collection-operations.md`.

Administrator setup is documented in `docs/admin-operations.md`. The page is
absent unless `ADMIN_ENABLED=true`. Login mode requires a generated password
verifier. An unauthenticated mode exists only for explicit loopback development
and requires both `ADMIN_LOGIN_REQUIRED=false` and
`ADMIN_ALLOW_OPEN_LOCAL=true`; the server rejects that mode on non-loopback
binds.

Production must keep the Python process on loopback and expose the read-only
dashboard through the Nginx HTTPS example. The systemd service, Nginx template,
health check, rollback procedure, cache behavior, and CEX-depth raw retention
timer are documented in `docs/production-hardening.md`. The deployable files
are `deploy/systemd/cex-dex-dashboard.service.in`,
`deploy/nginx/cex-dex-dashboard.conf.in`,
`scripts/retain_cex_depth_raw.py`, and
`deploy/systemd/cex-dex-cex-depth-retention.service.in` plus
`deploy/systemd/cex-dex-cex-depth-retention.timer`.

## Fact semantics

- Token-level volume and DEX share are aggregate facts across all cataloged
  markets in the selected window. The expanded CEX and DEX rows describe only
  the selected pair and pool.
- Window return is first-to-last observed close. Daily realized volatility uses
  log returns only between adjacent UTC calendar days; intervals crossing a
  missing day are excluded and reported as gaps.
- TVL is a source-reported point-in-time pool snapshot. CEX order-book depth
  and DEX pool-state depth are separately collected point-in-time snapshots.
  None is historical daily liquidity, and TVL is never converted into depth.
- Fixed-notional quoted cost walks the original CEX levels or executes the
  supported DEX V2 invariant captured by those collectors. DEX V3 execution is
  explicitly unsupported in this release. Cost is never interpolated from the
  four depth bands. Every CEX row explicitly marks its numeric trading fee as
  `excluded_unknown_account_tier`; the value remains null rather than being
  treated as zero. Supported DEX V2 rows include the pool's swap fee in the
  invariant calculation. This is a pool swap fee, not a claim about protocol
  treasury or revenue. DEX gas/router/MEV costs stay explicitly outside the
  quoted-cost fact.
- The A/B comparison reports absolute USD price difference and
  midpoint-relative bps only when both selected markets have prices on the
  same UTC date. Its Price, Spread, and Volume lines preserve gaps instead of
  filling them.
- Event Facts are manually reviewed facts about event timing and status. Their
  Compare markers show only temporal overlap with the displayed daily series;
  the monitor does not infer causal effects.

## Future scope

Funding rates, numeric CEX account fees, gas/MEV-aware quotes, DEX V3
fixed-notional execution, event-study returns/impact, additional DEX protocol
adapters, anomaly rules, historical TVL backfills, historical depth
reconstruction, and adding previously unconfigured Tokens require separate
data contracts and acceptance tests. Unsupported facts remain null or
explicitly `unsupported`; the dashboard does not estimate them from adjacent
facts.
