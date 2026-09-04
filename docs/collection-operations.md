# Collection cycle and freshness operations

## Profiles

All production collection runs enter through:

```bash
python3 scripts/run_collection_cycle.py --profile PROFILE --publish-local
```

| Profile | Ordered steps | Intended cadence |
| --- | --- | --- |
| `full` | official CEX lifecycle inventory, incremental daily OHLCV, CEX depth/cost, published TVL, then DEX depth/cost | manual catch-up and release validation |
| `daily` | official CEX lifecycle inventory, incremental daily OHLCV, TVL | daily at 00:30 UTC |
| `tvl` | TVL only | manual retry/recovery |
| `depth` | CEX depth/cost, temporary DEX USD-price refresh, then DEX depth/cost | hourly at minute 05 UTC |
| `cex_depth` | CEX depth and fixed-notional cost from one book snapshot | manual retry/recovery |
| `dex_depth` | temporary DEX USD-price refresh, then DEX depth/cost from one fixed block | manual retry/recovery |

The daily step reads the current CEX and DEX end dates, starts from the older
source with a three-day overlap, and ends at the latest completed UTC day. It
passes every configured Token to `run_fact_pipeline.py --append`, so the upsert
preserves older history. `--full-rebuild` is an explicit exception and must not
be used by timers.

### Daily CEX current-instrument lifecycle evidence

The `full` and `daily` profiles first call the official Crypto.com
`public/get-instruments` endpoint. The shared parser accepts an instrument as a
current canonical spot identity only when `inst_type` is `CCY_PAIR`, `symbol`
is `BASE_QUOTE`, `display_name` is `BASE/QUOTE`, `base_ccy` and `quote_ccy`
match, and `tradable` is the JSON boolean `true`.

Perpetuals, futures, non-tradable rows, and namespaced venue variants do not
establish presence for a canonical spot market. A catalog containing no exact
tradable spot rows is rejected. The collector compares that inventory with all
static Token pairs plus active runtime Token mappings explicitly approved for
`crypto_com`. It publishes reviews only for exact configured markets missing
from the current inventory. A market that reappears is removed from the next
manifest; no delisting or relisting date is invented.

The exact HTTP response is retained before parsing at:

```text
MARKET_DATA_DIR/raw/cex-instrument-lifecycle/<response_sha256>.json
```

The validated runtime manifest is atomically replaced at:

```text
MARKET_DATA_DIR/cex_instrument_lifecycle.json
```

Its root always records `checked_at_utc`, `response_sha256`,
`inventory_count`, and `configured_market_count`, even when `review_count` is
zero. The runner accepts the lifecycle step only while that root check is no
more than 36 hours old and no more than five minutes in the future.

A network error produces no new raw file. An HTTP or parse/contract error may
retain the received raw response for diagnosis, but none of those failures can
replace the previous manifest. Without `--fail-fast`, independent daily/TVL
work may continue; the final cycle still fails and the dashboard continues to
expose the prior manifest with its real age.

Each daily collector also writes one run-scoped attempt ledger beside its
staging CSV:

```text
data/processed/cex_daily_collection_attempts.json
data/processed/dex_daily_collection_attempts.json
```

The pipeline deletes both arbitrary staging ledgers before starting. Every
attempt records a Token and adapter or exact pool, requested date window,
observed dates, `status`, `outcome`, normalized `reason_code`, optional HTTP
status, and a bounded safe error. Raw URLs, query strings, response payloads,
credentials, and local paths are not retained in this field. The ledger is
bound to the candidate daily CSV by SHA-256.

An append also preserves applicable evidence from the currently published
`quality/daily-latest.json`. Before doing any network work, it verifies the
report schema, its `import_run_id` and snapshot ID against SQLite
`dataset_state`, and both report/CSV hashes against the SQLite snapshot
lineage. An existing but malformed or mismatched report fails closed: the
collectors and publication do not run. Only normalized non-success attempts
that still explain a gap in the new complete candidate are carried. A current
attempt replaces the overlapping part of an older market window; any
non-overlapping dates are retained as bounded split records. Succeeded or
orphaned attempts are not carried. The merged ledger is deduplicated and
rebound to the new full CSV hash. A full rebuild never carries prior evidence.

During report construction, a missing, malformed, or hash-mismatched standalone
ledger is ignored as failure evidence rather than being allowed to explain a
snapshot. During append merging, however, a malformed ledger just written by a
selected collector aborts the run before publication.

Accepted normalized attempts are embedded in the staged daily quality report;
the standalone staging ledgers are not a second public commit point. The same
evidence therefore survives both a successful quality publication and a
hard-invalid rejection report.

Only transport, rate-limit, source-availability, parse, validation, and
unexplained missing-row outcomes enter automatic retry windows. A successful
source response with no target candle is retained as non-retryable
`source_no_observation/no_candles`, including when that current evidence
explains why a formerly active market has no recent row. `not_listed` and
`source_range_unavailable` are also non-retryable, but they remain distinct:
`not_listed` enters manual review, while a documented source-history cap is
published as informational `unsupported` coverage and stays outside both the
retry and manual-review queues.

The separate tracked `data/curated/market_lifecycle_reviews.json` file can
dispose one exact stale-lifecycle issue after a declared/primary source
cross-check. Validate it before publication:

```bash
python3 scripts/market_lifecycle_reviews.py
```

The validator requires contiguous revisions, exact issue/market/date identity,
an approved source host for that market type, check timestamps, normalized
observations, and response SHA-256 hashes. Only the informational
`source_no_observation/no_candles` disposition is supported. Review evidence
does not expire into a guessed lifecycle: it applies to its one recorded UTC
date, while the next day again depends on fresh collector evidence or a new
review revision.

## Manually reviewed Event Fact publication

Event Facts do not run in the daily or hourly collection profiles. An operator
first checks the cited official page, updates the versioned source-check
record, and appends a revision to `data/curated/event_facts.csv`. Existing
published `(event_id, revision)` rows cannot be deleted or edited in place.

Build a review bundle without publishing:

```bash
python3 scripts/event_facts.py
```

After reviewing the source, manifest, hashes, latest-revision CSV, and full
revision CSV, publish it:

```bash
python3 scripts/event_facts.py --publish-local
```

The command writes an immutable bundle under
`data/local/events/bundles/<bundle_id>/` and atomically replaces
`data/local/events/latest.json` only after validation. The website reads the
selected bundle through `/api/markets/events`; it does not read the curated CSV
directly. The committed curated input contains 44 latest reviewed facts and
30/30 configured-Token presence coverage, but production count and coverage
are whatever the currently selected bundle actually contains. Presence
coverage is not a complete-history claim.

This workflow records official-source timing, precision, lifecycle, evidence
status, and revision lineage. It does not calculate event returns, impact, or
causality. Source-check freshness is a human review property, not an hourly
market-data SLA.

## CEX taker-fee evidence and the secret boundary

CEX account fees are route-cost inputs, not anonymous market facts. The fee
collector accepts only an already configured read-only client wrapper exposing
`fetch_authenticated_fee(venue=..., instrument=...)`. API keys, secrets,
passphrases, authorization headers, account IDs, and raw authenticated payloads
must remain inside that wrapper. The normalized output retains only the exact
market side, fee rate, fee asset, calculation basis, observation/expiry times,
an opaque 64-hex profile ID, and a SHA-256 of the whitelisted fee evidence.
Collector failures use bounded reason codes; exception text, client objects,
credentials, and local paths are not logged.

The request identity is canonical and indivisible:
`cex:<venue>:<BASE>/<QUOTE>`, `venue`, and `instrument=<BASE>/<QUOTE>` must
agree before any client call. The adapter alone maps that pair to the venue's
native spelling. The received fee asset is derived as BASE for a buy and QUOTE
for a sell; callers cannot substitute another Token. Binance BNB is the sole
exception, and only an authenticated response that names BNB plus an explicit
funded-account assertion can prove that branch. Strict evidence is owned by the
requested snapshot only while `observed_at <= now < valid_until`.

The authenticated response shapes were checked on 2026-08-02 against the
official interfaces:

- Binance Spot `GET /api/v3/account/commission` and its
  [commission calculation FAQ](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/commission_faq.md).
  The normalizer adds taker plus buyer/seller rates for standard, special, and
  tax commission. A Binance discount is applied only when both official flags
  are true, the response names BNB, and the caller explicitly proves BNB is
  funded; otherwise the state is rejected instead of guessed.
- Bybit V5 [`GET /v5/account/fee-rate`](https://bybit-exchange.github.io/docs/v5/account/fee-rate).
  The result category must be exactly lowercase `spot`; the bound spot
  instrument and response timestamp are mandatory.
- OKX V5 [`GET /api/v5/account/trade-fee`](https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-fee-rates).
  OKX encodes a commission as a negative rate, so the normalized non-negative
  cost is its exact magnitude. A positive taker rebate is rejected because the
  current cost-component contract cannot represent a negative cost.

For another spot venue, an operator may install a generic CSV with exactly:

```text
profile_id,venue,instrument,side,taker_fee_bps,fee_asset,basis,
observed_at,valid_until,source_record_sha256
```

The file named by `MARKET_CEX_PRIVATE_FEE_PROFILE` must be a regular non-symlink
file owned by the service user with mode `0600`. `profile_id` is an opaque
lowercase SHA-256 identifier, never a username or account number. `instrument`
uses canonical `BASE/QUOTE`, `fee_asset` must equal the received asset, and the
only accepted `basis` code is `authenticated_taker_fee`; output expands that
code through a fixed template rather than copying operator text. The loader
opens with `O_NOFOLLOW`, validates owner, mode, device, and inode with `fstat`,
then reads from that same descriptor. Duplicate profile/venue/instrument/side
keys, file swaps, future observations, expired records, non-exact numbers, and
unknown columns fail closed. A missing exact record is `unavailable`; it is
never zero and never silently replaced by a default rate.

The tracked `config/cex_public_fee_schedules.csv` is a separate, explicitly
opt-in research source. Every row has an HTTPS source, check time, expiry, and
minimum/maximum taker-fee bounds. Collection projects only the conservative
upper bound and labels it `bounded_estimate`; the full interval remains in the
basis, and `strict_eligible` is always false. Rows accept only the controlled
`official_spot_taker_fee_range` basis code and the literal
`fee_asset=received_asset`; source URLs must use HTTPS without credentials,
query strings, or fragments. The tracked file is intentionally header-only:
no venue currently has a generic public row whose account, region, pair, fee
Token, and special/tax conditions form an honest current bound. Therefore an
opted-in lookup against the tracked file returns explicit `unavailable`, with
no fabricated timestamp or rate. An operator may add a reviewed, expiring row
only when all of those conditions are actually bounded.

Funding rates are out of scope, and this fee layer does not change Upbit
catalog identities or historical facts. CEX fee collection is attached to the
synchronized route/opportunity pipeline, not to the existing daily OHLCV or
hourly depth publication cycle.

## Private route-inventory evidence and route-mode gate

Strict route capacity may consume the private CSV named by
`MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE`. Keep the variable blank when no
current inventory evidence is installed. Its value must be an absolute file
path outside the repository and web root. Production installs use a regular
non-symlink file owned by the service user with mode `0600` and exactly these
columns:

```text
profile_id,market_id,asset,available_quantity,observed_at,valid_until,
source_record_sha256
```

The loader walks from `/` to the file with a separate directory descriptor for
every path component. Every parent and the final file are opened relative to
the pinned parent with `O_NOFOLLOW`; a second root-to-leaf walk must reproduce
the same directory and file identities before parsing starts. It also rejects
any component resolving to the repository or public web root, and rejects a
multi-link file so an external hard link cannot bypass that boundary. The
final single-link file is revalidated as owner-only and parsed from the same
descriptor. A parent symlink or replacement, file swap, unknown column,
duplicate market/asset key, mixed or non-opaque profile ID, malformed source
hash, future observation, or expired/reversed validity window fails closed.
Market IDs use the canonical
`cex:<venue>:<BASE>/<QUOTE>` or `dex:<chain>:<dex>:<pool>:<TOKEN>` identity;
assets are canonical uppercase identifiers and quantities are exact,
non-negative Decimal strings rather than floats.

The private balances and per-record source hashes never enter public output or
logs. Public lineage contains only `INVENTORY_EVIDENCE_VERSION=1`, the opaque
inventory profile hash, the effective observation/expiry window, and an exact
request binding covering route ID, both Market IDs, buy quote asset/quantity,
sell Token asset/quantity, and target asset/quantity. The classifier receives
the independently constructed current request and verifies every field and
its canonical request hash; evidence from another route or quantity cannot be
replayed. Only after both balances pass does it expose the exact Token quantity
proved for that request. Local paths, account or wallet identifiers,
credentials, and raw rows are not projected. Missing, stale, wrong-asset, or
malformed evidence is
`inventory_unavailable`; an exact balance below either requirement is
`inventory_insufficient`. Both outcomes have null strict capacity.

Route-mode evidence uses two hashes with deliberately different claims.
`source_record_sha256` identifies the upstream raw record and is lineage only.
`evidence_binding_sha256` is recomputed over every field in the closed,
normalized evidence record except the binding field itself, including the
upstream source hash. It detects a field change paired with reuse of an old
binding; it is an integrity checksum, not authentication and not proof that a
caller is authoritative. Atomic and transfer expected requests independently
carry the source-record hash and evidence-binding hash they received from the
owning upstream collector. The gate requires exact equality as well as a fresh
full-record recomputation.

For `prepositioned_inventory`, the buy market must have at least the exact
quote-asset debit and the sell market must have at least the exact net Token
quantity. A DEX buy leg has no quote asset encoded in its Market ID, so it is
unavailable unless Task 5 supplies authoritative typed `MarketRules` and
`QuantityQuote` objects and their independently verified immutable hashes.
The reserved DEX quote projection binds Market, base/quote assets, exact debit
and target quantities, both upstream hashes, time window, and raw-source hash
with `evidence_binding_sha256`. A valid self-computed mapping checksum alone is
still `dex_buy_authoritative_upstream_unavailable`; malformed or unbound input
is `dex_buy_quantity_quote_unavailable`. Until the Task 5 types and source
verification are connected, every DEX buy remains fail-closed. Inventory only
limits the requested route capacity: it never upgrades depth, invents a fill,
or creates a price fact.

Independent DEX leg quotes cannot prove `atomic_onchain`. The composed-call
evidence must exactly match the classifier's expected route ID, buy/sell
Markets, cohort-state ID, target asset/quantity, composed-call hash, and route
outcome hash. Its full-record binding also covers evidence type/status,
observation/expiry, and `source_record_sha256`; the independently constructed
expected request supplies the exact expected source and binding hashes. A
`same_cohort_state=true` field supplied by the evidence is not trusted and is
not part of the accepted schema. Missing, stale, checksum-reused, or mismatched
evidence leaves the component `research_estimate` with
`atomic_route_simulation_unavailable`.

A `rebalance_required` route also remains estimate-only unless both its
inventory and transfer evidence are complete and current. Transfer evidence
must bind the route, asset, exact requested quantity, source/destination
Markets, source/destination state IDs, exact independently expected capacity,
source-record hash, observation/expiry, and a capacity quantity at least as
large as the request. Its independently expected full-record binding is
recomputed before use. The route-mode gate reports only component eligibility:
`mode_evidence_eligible` when this mode's evidence passes, otherwise
`research_estimate`. It never emits the final opportunity classification
`executable_candidate`; only the downstream full opportunity evaluator may do
that after validating every other component and positive net edge. This layer
does not add Funding Rate support or change Upbit data or identities.

## Complete route-opportunity finalization and release gate

Route finalization is deliberately separate from live collection. The
`finalize_route_opportunity_bundle()` orchestration entry point receives an
already published core cohort plus its retained raw, typed-source, fee, and
inventory evidence. It does not call a collector. It replays those pinned
inputs, publishes an immutable five-file bundle under
`MARKET_DATA_DIR/routes/bundles/<route_cohort_id>/`, validates the installed
bytes, and only then replaces `MARKET_DATA_DIR/routes/latest.json` with a
`route_opportunity_pointer/v1` pointer. A failed finalization leaves the prior
complete pointer and its original timestamp intact.

For an explicitly pinned CEX-only Shadow run, the production finalizer can
publish, reload-validate, and then start the read-only Current Opportunity
page in one process lifecycle:

```bash
MARKET_CEX_PRIVATE_FEE_PROFILE=/absolute/private/fee.csv \
MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE=/absolute/private/inventory.csv \
python3 scripts/route_opportunity_pipeline.py \
  --data-dir /absolute/local-data \
  --shadow-run-id RUN_ID \
  --expected-joint-pointer-sha256 64_HEX_SHA256 \
  --serve \
  --port 8765
```

The server is fixed to `127.0.0.1`; this command offers no external-host
option. It strips the private fee and inventory profile paths before replacing
the finalizer process with the dashboard, disables administrator and public
write surfaces, and skips ambient local-environment loading. The dashboard is
started only after `routes/latest.json` and its complete bundle have passed a
fresh full reload against the pinned core. This entry point does not collect
market data, select a Shadow run, or submit an order.

An explicitly pinned, DEX-only core can use the same publication and serving
boundary when every leg is an observed Ethereum Uniswap V2 pool, every route
is same-chain `atomic_onchain`, and every leg has the complete v2 typed-source
inventory (`dex_pool_state`, `dex_market_rules`, `dex_usd_conversion`, and
`dex_usd_price_context`):

```bash
python3 scripts/route_opportunity_pipeline.py \
  --finalizer eth-uniswap-v2-research \
  --data-dir /absolute/local-data \
  --shadow-run-id RUN_ID \
  --expected-joint-pointer-sha256 64_HEX_SHA256 \
  --research-mev-bps 25 \
  --serve \
  --port 8765
```

This mode rereads and replays only the retained local bytes; it performs no RPC
or HTTP collection. It never issues an executable attestation. When both
binding-referenced leg transcripts are observed, it projects the embedded pool
fee, per-leg max-fee gas quote, proved no-integrator-fee and no-transfer-tax
facts, and same-chain topology into the ten-row cost inventory. The optional
`--research-mev-bps` value is an explicit operator scenario, not a fact inferred
from the signed submission-loss bounds. Supplying it closes the scenario-cost
arithmetic so the page can show gross edge, known strict costs, the assumed MEV
cost, and research net edge even when the result is negative. Omitting it keeps
MEV unavailable and research net null. The option accepts canonical decimal
text from 0 through 10000 with at most six decimal places and is rejected for
the CEX finalizer.

If the pinned route-cost sidecar cannot establish either leg transcript, all
required component rows remain terminal with null amounts and the page shows
the route as `unavailable`; the MEV option does not override missing evidence.
A DEX result is therefore either `research_estimate` or `unavailable`, never
`executable_candidate`. The checked-in adapter authority currently identifies
only one supported UNI/WETH V2 pair, while this DEX-only finalizer requires two
distinct supported markets. A real two-pool result therefore also requires a
second locally retained, independently authorized pair; the test KAT proves the
full calculation path with two local synthetic pools but is not current market
data.

Each sealed route candidate also carries its two ranking-volume inputs and a
derived `route_volume_usd`. CEX legs bind the route-universe selected-window
USD volume; DEX legs bind latest 24-hour USD volume. The route value is the
exact minimum only when both inputs are positive, otherwise it is null. The
basis is fixed to `minimum_leg_source_horizon_usd`, is projected into CSV and
SQLite, and is rechecked against the public API by the release gate. It must
not be relabeled as route capacity, a synchronized flow measure, or a fill
guarantee.

The private `MARKET_DATA_DIR/routes/core/latest.json` pointer may advance while
finalization is in progress. Readers never use that core pointer as a public
opportunity generation. They continue to use the last validated complete
pointer; a core-only directory, partial complete directory, or caller-supplied
row list cannot become public by defaulting, sorting, or caching.

Run the deployment release checker after finalization with:

```bash
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8765 \
  --require-route-opportunities
```

`--require-route-cohort` remains a backward-compatible spelling for the same
complete-bundle requirement; it no longer validates the private core pointer.
Without either flag, an absent complete pointer is reported as unavailable so
deployments that have not launched the route product can proceed. A present
pointer is never ignored: malformed, partial, stale, divergent, or core-schema
content fails release validation even in optional mode.

The checker rereads the complete pointer, manifest, three CSV projections, and
SQLite. It then independently reproduces quantity lattice, leg timestamps,
skew and age, required component inventory, component freshness, exact gross
and net cost arithmetic, bps rational fields, raw/cost generations, core
binding, strict classification, and manifest/CSV/SQLite parity. Missing,
unsupported, failed, and stale cost amounts must remain null with their reason;
zero is never a fallback. An executable row requires both strict readiness and
the Task 7 publication attestation. Authenticated/estimated status cannot be
rewritten, a reflected pool fee cannot be charged twice, and an authenticated
gas row with a fabricated zero amount fails closed.

The same release run now performs a cold and warm read of the public
`/api/markets/opportunities` projection. It requires exact cohort and manifest
lineage, strict/research/unavailable count parity, full opportunity-inventory
hash parity, and filter parity for strict, estimate, unavailable, and one
Token/Venue/notional/route-type slice. Filtered responses must preserve the
full cohort, manifest, count, Venue inventory, and next-freshness-deadline
metadata. The checker independently recomputes route age/skew from the response
clock and both leg timestamps, binds source links to those exact legs, validates
the complete cost-component topology, and reproduces each public cost total
from its strict/reflected provenance flags. Every N/A route value retains a
public reason; stale strict rows retain identity but no numeric rank. Public
payloads are rejected if they contain absolute filesystem paths, secret-bearing
fields or values, or exceed the default 2,000,000-byte raw / 300,000-byte gzip
budgets. The JSON release report records cold/warm latency and byte counts.

When no complete pointer exists, the checker still requests the endpoint and
requires HTTP 200 with `availability.status=unavailable`, the fixed
`complete_pointer_absent` reason, an empty route array, and zero coverage. It
does not reinterpret absence as an empty profitable inventory or numeric zero.

This read-only release gate does not start collection. It does not install or enable a timer.
Funding Rate remains excluded and Upbit inputs remain unchanged.

## Lock and manifest

Every profile acquires `data/local/collection/collection.lock`. A second run
does not write facts while another profile owns the lock.

Each completed run writes:

```text
data/local/collection/runs/<run_id>/manifest.json
data/local/collection/runs/<run_id>/<step>.log
data/local/collection/latest.json
```

The manifest records exact argument arrays, timestamps, duration, exit status,
full-log SHA-256, a bounded log tail, current file SHA-256 values, coverage,
source-specific date ranges, lifecycle root evidence, and freshness.

Scheduled publishing also applies a post-step freshness gate. A collector that
exits zero while its expected source remains stale is recorded as failed. A
rate-limited or empty response therefore cannot masquerade as a successful
refresh.

## Exact historical-gap backfill

### One-time exact CEX identity migrations

The retired Upbit adapter could collect `TOKEN/KRW` even though the configured
market was `TOKEN/USDT`. Older Coinbase and Kraken adapters could also label
their actual USD products as `TOKEN/USDT`. These are two independent migration
scopes. Do not combine them implicitly, and do not filter the published CSV or
SQLite database manually.

For the Coinbase/Kraken correction, always name both the Token set and the two
selected exchanges. Do not pass the Upbit removal switch. Start with one Token
in a validating dry-run:

```bash
python3 scripts/migrate_cex_exact_identities.py \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --tokens 1INCH \
  --exchanges coinbase,kraken \
  --data-dir /absolute/published/data \
  --staging-dir /absolute/new/coinbase-kraken-smoke-dry-run
```

The default is a validating dry-run and the staging directory must not already
exist. After the one-Token smoke passes, run the complete declared Token set in
a different new staging directory, still without `--apply`:

```bash
python3 scripts/migrate_cex_exact_identities.py \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --tokens TOKEN1,TOKEN2 \
  --exchanges coinbase,kraken \
  --data-dir /absolute/published/data \
  --staging-dir /absolute/new/coinbase-kraken-full-dry-run
```

Publication is permitted only when both `exchanges` fields equal
`["coinbase", "kraken"]`, `preflight.upbit_rows_unchanged` is true, the scoped
legacy residue is zero, every retired row is present in the hash-bound
quarantine, all existing exact `/USD` market-dates survive, and non-target CEX
facts plus the complete DEX bytes remain unchanged. Then rerun the identical
Token, exchange, and date scope with another new staging directory and explicit
`--apply`:

```bash
python3 scripts/migrate_cex_exact_identities.py \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --tokens TOKEN1,TOKEN2 \
  --exchanges coinbase,kraken \
  --data-dir /absolute/published/data \
  --staging-dir /absolute/new/coinbase-kraken-apply \
  --apply
```

The Coinbase/Kraken run treats every Upbit row as immutable. Its preflight
compares the complete Upbit row multiset before and after staging and rejects
publication if a single Upbit fact changes.

The older Upbit KRW fallback is a separate, explicitly authorized operation.
Only that operation selects exactly `--exchanges upbit` and supplies
`--remove-legacy-upbit-krw-fallback`; neither option is implied by a
Coinbase/Kraken run.

The runner splits a longer interval into adjacent windows of at most 180
inclusive days, uses one private staging snapshot, and publishes at most once
after every window passes.

The runner holds `collection/collection.lock` from before seeding until after
the sole import. An already-held lock exits nonzero before staging, collection,
or import. The Upbit flag is deliberately opt-in and fail-closed. Every
baseline row already carrying the configured exact identity must survive on
the same UTC date. Rows positively classified as retired Upbit KRW fallback or
Coinbase/Kraken USDT mislabels are never relabeled into another market. They
leave served facts only inside the declared migration scope and only after the
complete original rows, baseline/candidate hashes, disposition counts, and a
row-set hash are written to the atomically published exact-identity quarantine.
Each publication also writes an immutable content-addressed quarantine archive;
the fixed filename is only the latest pointer, so a later independent migration
cannot overwrite the earlier reversible evidence. The baseline hash is taken
from the authoritative SQLite export, and the runner fails before collection
unless that export and the published CEX CSV contain the same normalized rows.
An alias-only date therefore becomes an explained missing exact fact; it does
not become a synthetic candle. Network, rate-limit, parse, validation,
unavailable-range, exact market-date preservation, or quarantine failures
block the complete migration and preserve the published snapshot. A
legitimately configured KRW market remains untouched during normal collection.

Every source response is first clipped to the requested inclusive UTC window.
An exact-identity migration requires a conclusive full-window outcome;
`partial`, network, rate-limit, parse, validation, or unavailable-range status
fails closed before publication. The migration also fails if any genuine exact
baseline market-date is lost, if any retired quote-label residue remains, or
if the quarantine does not account for every removed alias row. No outcome
authorizes forward filling or synthetic exact facts.

Use the internal exact-window runner for historical gaps. It does not accept an
operator-supplied Token or date, and it does not call the `daily` collection
profile because that profile also refreshes every TVL pool. Instead, it reads
the currently published `backfill_windows_by_token`, validates the quality
report's import/snapshot lineage against SQLite, and calls only
`run_fact_pipeline.py`:

```bash
python3 scripts/run_exact_backfill.py \
  --data-dir /home/ugs/workspace/cex-dex-market-monitor-v1/data/local \
  --dry-run
```

The default live batch is one exact window:

```bash
python3 scripts/run_exact_backfill.py \
  --data-dir /home/ugs/workspace/cex-dex-market-monitor-v1/data/local
```

After checking the dry-run scope and current source health, an operator can
raise the bounded sequential batch size:

```bash
python3 scripts/run_exact_backfill.py \
  --data-dir /home/ugs/workspace/cex-dex-market-monitor-v1/data/local \
  --max-windows 12
```

The runner holds the same `collection/collection.lock` for the whole batch. It
reloads the current report before and after every collector invocation. The
next window runs only if:

- the collector exited zero;
- the quality report and SQLite still share one publication lineage;
- `publication.import_run_id` changed; and
- at least one selected exact-window `issue_id` disappeared.

Any collector failure, unchanged publication, invalid report, or no-progress
publication stops the batch. The report's `market_types` controls source scope:
a DEX-only window receives `--dex-only`, a CEX-only window receives
`--cex-only`, and a genuinely mixed window runs both daily collectors. TVL,
CEX depth, DEX depth, and execution-cost collectors are never invoked.

“Exact” here means the report-authorized Token and inclusive date window.
Within an authorized `market_type`, the existing daily collector still queries
every configured market for that Token; it does not yet accept a pool/exchange
market allowlist. The state log records the affected `market_ids`, but they are
verification evidence rather than subprocess arguments. Adding true
per-market collection scope requires a separate collector-contract change and
adapter tests.

Immutable logs and recoverable state are written below:

```text
data/local/collection/exact-backfill/runs/<run_id>/state.json
data/local/collection/exact-backfill/runs/<run_id>/window-0001.log
data/local/collection/exact-backfill/latest.json
```

Resume a bounded or interrupted state log with its validated run ID:

```bash
python3 scripts/run_exact_backfill.py \
  --data-dir /home/ugs/workspace/cex-dex-market-monitor-v1/data/local \
  --resume-run-id <run_id> \
  --max-windows 12
```

Resume never trusts the prior command as authorization. It reselects an exact
window from the live report, so a window already resolved by a completed
publication is not replayed. A report created before the `market_types`
contract is rejected; publish a fresh quality report with the current code
before starting bulk backfill.

## Pre-publication coverage regression gate

TVL, depth, and their matching execution snapshots also pass a collector-level
gate **before any `data/local` latest/history file is changed**. The runner's
post-step freshness check is intentionally not used as this boundary because it
runs after the collector exits.

The gate uses these stable identities:

- TVL and DEX depth: Token, normalized chain, normalized pool address;
- CEX depth: Token, exchange, canonical CEX symbol;
- execution cost: market ID, direction, requested USD notional.

`observed` is usable for TVL. `observed` and truthful `partial` lower bounds are
usable for depth/execution. Structurally unsupported DEX adapters are excluded
from the supported denominator; a pool classified as V2/V3 depth-capable but
returned as `unsupported` counts as failed coverage. DEX V3 execution remains
structurally unsupported until exact integer swap math is implemented.

| Family | Minimum current usable coverage | Minimum retention of comparable prior usable identities |
| --- | ---: | ---: |
| DEX TVL | 80% of current inventory | 95% |
| CEX depth and execution | 90% of current inventory/scenarios | 95% |
| Supported DEX depth and execution | 80% of supported inventory/scenarios | 95% |

These absolute floors apply only to a full-inventory refresh. An explicitly
targeted `--merge-publish` recovery uses a separate fail-closed gate: the
candidate may start below the aggregate floor, but it must retain 100% of all
previously usable identities, preserve every non-target field, keep the exact
inventory/scenario keys, and resolve the target to observed or a confirmed
terminal outcome. This permits a damaged or even zero-observed baseline to be
repaired one market at a time without weakening the full-refresh policy.

Comparisons use only identities present in both the candidate and previous
latest snapshot. Catalog additions enter the current absolute floor but do not
weaken prior-retention math; catalog removals do not masquerade as collection
failures. A source cohort (TVL/DEX chain or CEX exchange) with at least five
previous usable identities is also rejected when at least two are lost and
retention falls below 50%. All thresholds use integer basis-point comparisons.

With no previous latest file, the absolute floor still applies and the passing
snapshot establishes the baseline. An existing empty, duplicate, malformed, or
identity-incomplete baseline fails closed. There is no silent override and no
mixing of old successful rows into a rejected candidate.

CEX and DEX preflight depth and execution together. If either coverage check
rejects during that bundle preflight, no public family destination is changed.
Both family reports remain available for diagnosis. The collector exits
nonzero, and the previous latest naturally becomes stale rather than being
replaced with broad failure rows. Passing and rejected structured reports are
copied into `steps[].publication_gates` in the collection manifest.

For both full-inventory and canonical one-market `--merge-publish`
publication, a shared guard first resolves and compares all destinations. Each
CEX/DEX family has four public destinations—depth history, depth latest, public
depth current, and execution latest—and two private destinations—processed
depth current and processed execution current. Any resolved private/public
overlap is rejected before `mkdir`, private writes, history reads, or public
replacement. This pre-write guard applies to both full and exact publication.

After the overlap guard, full publication validates aligned lineage, execution
scenario inventory, and both standard coverage reports. Exact publication
checks aligned lineage and complete execution scenarios, validates both
candidate-bound exact-target reports and their target/mode/common generation,
and requires exactly one target history row identical to the target
depth-latest row. The private current files are then written independently.
The four public bytes are passed to one staged replacement bundle; if an
ordinary in-process I/O exception interrupts replacement, the helper restores
every public destination to its pre-call bytes.

This is failure-atomic error handling for ordinary in-process I/O failures
only. It is not process-crash atomic, and resolving paths before writing does
not eliminate a check-to-use race if another process changes a path or symlink
after validation. Power loss, interpreter termination, an operating-system
crash, or an unsupported concurrent direct publisher can still leave state
requiring manifest/hash-based diagnosis. The private current files are not a
server-visible commit point and are not included in public rollback. The CEX
and DEX family bundles also remain separate from each other. True crash-atomic
cross-family publication would require immutable generation directories and
one atomically replaced manifest pointer.

The production runner lock serializes managed profiles; concurrent direct
collector invocations with `--publish-local` are unsupported.

The separate fact lifecycles remain explicit:

- daily source CSVs are replaced individually, while the server-visible SQLite
  database is staged and atomically replaced last through
  `import_local_snapshot.py`;
- TVL appends normalized history, then atomically replaces its latest snapshot;
- CEX and DEX each publish depth history, depth latest/current, and the matching
  long-form execution-cost latest view through its own ordinary-I/O
  failure-atomic family bundle. Depth and execution reuse the same raw
  response/fixed-block lineage. Retained raw responses and manifests are the
  execution audit history for this release.

The hourly DEX USD-price refresh reuses the TVL collector's GeckoTerminal
multi-pool response but writes only
`data/processed/dex_pool_tvl_snapshot.csv`. It does not publish a new TVL fact
or append the TVL history. The following DEX collector explicitly reads that
file. If the refresh step fails, DEX depth is recorded as
`skipped_dependency` and the prior published DEX snapshot remains untouched.

A full collection manifest coordinates these publications but does not claim
that the CSVs, histories, latest snapshots, and SQLite database form one
multi-file transaction, or that all source APIs were observed at one instant.

### Snapshot cohort and reader boundary

For one CEX or DEX family, the raw inputs are bounded sequential observations:
CEX venues are requested in sequence, while DEX pool states are fixed to their
declared per-chain blocks but collected across pools and chains over time. The
canonical earliest and latest observation timestamps define
`observation_span_seconds`. This is a cohort-skew bound, not a claim of
simultaneous, synchronous, or same-instant observation.

Every published family must expose exactly one nonempty depth `snapshot_id`,
one execution `snapshot_id`, and one execution `source_snapshot_id`; all three
must be equal. The execution Market count must also equal the exact depth
inventory row count. The ID is therefore meaningful only together with the
validated inventory count and observation bounds. It is a publication/source
lineage key, not the observation time.

Readers fail closed on malformed depth cohorts and depth/execution lineage
mismatches. Execution-cost and Quality routes return a bounded 503 rather than
assembling facts from different cohorts. A malformed depth publication also
closes every public route that consumes the depth-enriched catalog, and
`/health` reports `status=degraded`, `data_ready=false`, and HTTP 503. An
execution-only file error is isolated from routes and health checks that do not
require a valid execution publication. Genuine absence remains
`unavailable`/`null` rather than being treated as a mismatched cohort or
converted to zero.

### Exact latest-fact refresh

The public TVL/depth action uses only the single canonical Market selected by
the API. `run_collection_cycle.py --market-id ...` forwards that identity to
the relevant collector; CEX depth makes one venue-market request, TVL makes one
pool request, and DEX depth refreshes the same pool's temporary USD-price input
before reading one fixed-block pool state. `--merge-publish` is mandatory for a
filtered publication and requires an existing full baseline.

The collector replaces only the target rows inside that baseline. A CEX/DEX
depth target also replaces its exact two-direction/five-notional execution
scenario set. Non-target rows retain their source observation, raw hash, block
or sequence, status, and values; only the common latest-view `snapshot_id` (and
execution `source_snapshot_id`) is rebound to the new publication generation.
Normalized depth/TVL history appends the collected target only, rather than a
copy of the full inventory. This is a publication merge, not a claim that every
market was observed simultaneously. The bounded public files are committed as
one staged bundle for ordinary I/O failure handling. Fault-injection tests at
every server-visible replacement require all old files to remain byte-identical
on failure; this still does not claim crash-atomic multi-file semantics.
Before either the full or exact helper writes anything, the shared resolved-
destination guard also requires the two private current paths to be disjoint
from all four public paths. This overlap check is not a TOCTOU guarantee against
unsupported concurrent path or symlink mutation.

The exact preflight seal binds every raw candidate field, not only identity and
status. Commit revalidates the current baseline hash, the common
depth/execution generation, the complete candidate-row fingerprint, and one
history row that is field-for-field equal to the target row in latest. A
preflight report cannot be reused after changing values, provenance, source
hashes, status, or snapshot lineage.

TVL is the only bounded fact family allowed to add an exact cataloged target
that is missing from its latest snapshot. The candidate must be one canonical
pool row with the existing schema; all prior rows remain unchanged apart from
the shared publication-generation ID. Depth and execution never infer missing
scenario keys. An exact public candidate must resolve through the shared table
to `observed` or confirmed terminal absence before any public replacement;
measured `partial` and retryable failures are rejected before commit.

## Freshness contract

| Source | Current threshold | Reason |
| --- | --- | --- |
| CEX daily OHLCV | no more than one completed UTC day behind | permits ordinary provider delay |
| DEX daily OHLCV | no more than one completed UTC day behind | permits ordinary provider delay |
| DEX TVL | age no more than 26 hours | matches the initial daily schedule |
| CEX depth | age no more than 2 hours | allows one missed hourly run |
| DEX depth | age no more than 2 hours | keeps CEX/DEX capacity snapshots comparable |

DEX USD conversion has a stricter dependency-level contract. The fixed-block
pool-state time is compared with the time this project received the
GeckoTerminal price response. A difference of at most 15 minutes is current;
more than 15 minutes and at most 2 hours is an explicit warning; more than
2 hours, a missing timestamp, or an invalid timestamp is unusable. Unusable
inputs cannot publish measured USD depth or execution cost.

The API reports `cex_daily`, `dex_daily`, `common_comparable_end`, `dex_tvl`,
`cex_depth`, and `dex_depth` separately. A global maximum date must not hide a
lagging source. Missing facts remain unavailable/null and are never replaced
with zero.

Freshness is a data-quality signal, not process liveness. `/health` remains HTTP
200 when the server and data files are readable, while `data_status` reports
`current`, `partial`, or `stale`.

## Summary warmup readiness

After the listening socket is created, the process best-effort warms the default
Screener Summary without delaying HTTP serving. `/health` includes the bounded
`summary_warmup` record: `status` (`warming`, `ready`, or `failed`), the public
Summary `generation` when available, canonical UTC `started_at` and
`finished_at`, and integer `elapsed_ms`. It never exposes the warmup exception,
filesystem paths, SQL, or provider details. A newer warmup attempt replaces an
older attempt's state; a late result from an older worker cannot overwrite the
current generation. If source generation changes during or after warmup,
`/health` fails closed rather than reporting stale `ready` until a matching
warmup completes. The warmup uses the normal request cache path and does not
create a Summary artifact, pointer, or full-Catalog preload.

Incremental DEX collection reuses the published token-pool inventory and its
TVL base/quote lineage. It never guesses the OHLCV side. The keyless
GeckoTerminal endpoint is IP-rate-limited, so pool requests are spaced and 429
responses trigger a conservative backoff; a 148-pool refresh is intentionally
slower than the CEX phase.

## Timer installation

The repository includes user-level systemd timer templates. On the production
host, from the deployed checkout, set the same absolute runtime-data path used
by the dashboard and run:

```bash
chmod +x scripts/install_collection_timers.sh
export MARKET_DATA_DIR=/data/market/published
export ADMIN_JOB_DIR=/data/market/admin/jobs
./scripts/install_collection_timers.sh
systemctl --user list-timers cex-dex-daily.timer cex-dex-depth.timer
```

The installer validates the absolute paths and renders dedicated user-service
units. Each unit embeds `MARKET_DATA_DIR`; it does not depend on the root-only
`/etc/cex-dex/dashboard.env`. Raw evidence is written below that same runtime
root at `raw/tvl`, `raw/cex-depth`, `raw/dex-depth`, and the content-addressed
`raw/cex-instrument-lifecycle` inventory evidence. Collector staging is
the reviewed sibling `.<data-directory-name>-processed`, except that the
checkout default `data/local` uses `data/processed`.

Logs are available through:

```bash
journalctl --user -u cex-dex-daily.service
journalctl --user -u cex-dex-depth.service
```

The timers use the same lock. A failed collector leaves previously published
facts in place and records a failed run manifest. Diagnose the retained step
log, fix the source/configuration issue, then rerun the relevant profile.
A lock-contention skip does not create an empty run directory or overwrite the
latest completed run manifest.
The daily service is intentionally not fail-fast: OHLCV and TVL are still
attempted when the independent lifecycle inventory or daily OHLCV step fails,
while the final service status remains failed and auditable. A failed lifecycle
check never replaces its prior runtime manifest.
The hourly depth service is also not fail-fast across independent CEX and DEX
sources: the DEX price refresh is still attempted when a CEX venue fails.
Within the DEX chain, however, the price refresh is a hard dependency. DEX
depth is skipped when that refresh fails. The final cycle remains failed when
either supported collection step fails its freshness or dependency gate.

## Raw CEX depth retention

Hourly order-book responses grow much faster than normalized facts. Keep the
recent JSON snapshots directly inspectable, then compress and eventually
expire them with the dedicated retention command. It is a dry run by default:

```bash
python3 scripts/retain_cex_depth_raw.py
python3 scripts/retain_cex_depth_raw.py --apply
```

The default 7-day raw and 30-day archive periods are operational defaults, not
a data-contract claim. Review the printed actions and the research/audit
retention requirement before applying or enabling
`cex-dex-cex-depth-retention.timer`. See
`docs/production-hardening.md` for the systemd template and safety boundary.

## Operational acceptance

- The correct canonical repository and branch are checked before deployment.
- Daily collection uses incremental upsert unless a reviewed rebuild is
  explicitly requested.
- Every configured Token remains present after publication.
- TVL inventory matches every cataloged Token/pool key.
- CEX depth inventory matches every cataloged Token/exchange/pair key.
- DEX depth inventory matches every TVL Token/chain/pool key, including explicit
  unsupported rows.
- Each CEX and DEX execution-cost inventory contains exactly five notionals by
  two directions for every corresponding source market, with no duplicate
  scenario keys.
- Execution-cost publication passes formula, fill-state, monotonicity, fee
  scope, missing-value, and source-lineage validation. Partial scenarios never
  publish a full-request VWAP or quoted cost.
- CEX execution rows retain `excluded_unknown_account_tier` and null numeric
  fee fields. Supported DEX V2 rows retain the pool swap fee used by the pool
  mechanics; operational reports must not relabel it as protocol treasury or
  revenue.
- A timestamp-fresh TVL/depth snapshot with zero observed or partial rows is a
  failed scheduled step. CEX execution must contain a measured row; DEX
  execution may be wholly `unsupported`, but any wholly failed supported
  adapter set is a failed step.
- Raw responses and collector manifests remain available for audit.
- Collection and publication failures never zero-fill missing values.
- When route opportunities are required, `routes/latest.json` selects a
  complete five-file opportunity bundle; strict rows pass all-in cost,
  quantity, timing, attestation, generation, and cross-format checks. A newer
  core-only pointer does not substitute for that complete generation.
- The public API and rendered page display separate source dates and stale
  states.
- When Event Facts are published, the selected bundle passes source-record,
  revision-lineage, SQLite-integrity, manifest-count, and file-hash checks.
  The release checker requires the Event covered-token inventory to equal the
  current Screener Token inventory, requires `uncovered_tokens=[]`, and
  requests a non-empty scoped Event response for every covered Token.
  An unavailable Event bundle is not reported as a verified zero-event result.

Funding rates, DEX V3 fixed-notional execution, and event-study outputs remain
unsupported. Existing depth execution rows also continue to exclude
account-specific CEX fees and gas; only the synchronized route pipeline may
add authenticated or validated-private CEX fees and adapter-bound gas evidence.
Collection operations must not manufacture any of these values from spot
prices, depth, TVL, pool swap fees, public fee defaults, or Event Facts.
