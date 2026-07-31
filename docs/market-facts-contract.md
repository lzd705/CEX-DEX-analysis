# Market summary, catalog, and two-market comparison contract

## Public endpoints

- `GET /api/markets/summary?start=...&end=...` returns the window-aware
  Screener contract: one row per Token, aggregate CEX/DEX volume, DEX share,
  primary price gap and date, compact primary CEX/DEX metrics, catalog market
  counts, and quality-status counts.
- `GET /api/markets/catalog?token=...&start=...&end=...` returns the complete
  catalog entries and selected-window metrics for exactly one canonicalized
  Token. Unknown or empty Token values are errors; a successful response never
  contains another Token's market.
- `GET /api/markets/catalog` without a query remains the versioned full audit
  catalog for backward compatibility. The website does not load this response.
- `GET /api/markets/compare?token=...&market_a=...&market_b=...&start=...&end=...`
  returns the union of the two selected markets' daily UTC observations.
- `GET /api/markets/execution-cost?token=...&market_a=...&market_b=...`
  returns independent point-in-time $1k/$5k/$10k/$50k/$100k quoted execution
  facts for the same exact market identities.
- `GET /api/markets/quality?token=...&scope=...&start=...&end=...` returns
  selected-window daily quality plus independently timestamped TVL, depth, and
  execution status for each exact market. Quality contract v4 carries two
  projections on every Market: selected-window `quality_status` /
  `quality_flags`, and immutable Screener `screening_quality_status` /
  `screening_quality_flags`. The latter reproduces the Summary row from the
  same `data_generation`; it is not recomputed from the selected window.
  Contract v4 also overlays only a bounded `fact_quality_report/v1` whose
  `publication.import_run_id` matches the catalog's current SQLite import
  identity. The public projection exposes allowlisted reason/status counts and
  affected UTC dates, never raw collector errors, source paths, or protected
  review details.

`market_a` and `market_b` are exact `market_id` values returned by the catalog.
They must be different and both must belong to the requested Token.

## Transfer and cache boundaries

The Screener summary deliberately omits `markets`, `cex_markets`, `dex_pools`,
and daily `price_points`. Its compact `primary_cex` and `primary_dex` objects
carry only the values displayed or sorted by the Screener. Aggregate volume and
the primary price gap are computed by the same server functions as the full
fact payload; the frontend does not recompute them from a partial market list.

The single-Token response includes canonical market IDs, source and quality
lineage, TVL, and 10/25/50/100 bps depth. Its nested `window_metrics` omits
daily `price_points` while retaining the price, volume, return, volatility, and
coverage needed by the Token Markets page. `null`, measured zero, partial,
unsupported, failed, unavailable, and not-cataloged states retain their
original meanings.

The response intentionally combines three labeled fact scopes:

- daily prices, volumes, returns, volatility, and price gaps use the requested
  UTC `start`/`end` window;
- catalog market and quality counts cover the full available daily history plus
  the latest published snapshot facts;
- TVL and depth fields in the Token catalog are independent latest
  point-in-time snapshots and do not change when the daily window changes.
  Execution-cost sources participate in the same `data_generation`, but their
  rows load only through the independent execution-cost endpoint.

Both responses participate in the same source-signature and one-minute
serialized-response generation as the full audit catalog. Token query values
are stripped and uppercased before entering the cache key. A public,
path-free `data_generation` hash lets the browser clear its bounded Token
catalog cache whenever the published source generation changes.

The release gate requires Quality contract v4 for every Summary Token. Each
all-scope Quality response must have the exact Summary generation, Token, and
market count, with unique nonempty market IDs. Status counts are recomputed
from every `screening_quality_status`; alert counts include every structured
flag severity, including informational, capability, measurement-limit, and
market-condition flags. Counts are compared exactly after zero-valued entries
are removed. A non-OK status with no structured flag is a contract failure;
the release checker does not invent a fallback reason. Any generation drift
aborts the release instead of being retried as a count mismatch.

The optional `quality/daily-latest.json` also participates in that source
signature. A missing, malformed, oversized, path-unsafe, wrong-schema, or
identity-mismatched report is ignored and the API labels its fallback as
`catalog_window_inference`; a trusted overlay reports
`identity_status=matched_current_import` without exposing the private file
path. A matched report distinguishes retryable technical
failures (`network`, `rate_limit`, `source_unavailable`, `parse`,
`validation`), confirmed no-candle outcomes, non-retryable listing/range
review, and unexplained missing rows. Public actions always point to protected
operator review; the public endpoint cannot start a retry.

The browser's Screener ranking is explicit URL state: `sort`, `scope`, and
`dir`. Each metric has an allowlisted scope. Missing, failed, unsupported, and
non-finite rank values always remain at the bottom in either direction; zero
is a measured value. The table and CSV expose the numeric rank value used by
the comparator.

Single-Token `market_count` is Token-scoped. Nested TVL/depth/execution snapshot
inventory counts retained in metadata describe all catalog markets and are
marked `snapshot_metadata_population_scope=all_catalog_markets`.

This split is a transfer and browser-memory boundary. A cold server build still
uses the shared full catalog and fact builders before projecting the compact
response; it is not a claim that every underlying SQLite query has already
been rewritten as a Token-only query.

## Fact definitions

| Output | Definition |
| --- | --- |
| Price | Source daily close, normalized to USD |
| Volume | Source daily USD volume for that exchange pair or pool |
| Absolute spread | `abs(price_a_usd - price_b_usd)` |
| Spread bps | `absolute spread / midpoint(price_a, price_b) * 10,000` |
| Grain | One UTC day |

When a separately published `cex_depth_latest.csv` exists, CEX catalog entries
also expose point-in-time best bid/ask, quoted spread, and 10/25/50/100 bps
order-book depth. Those fields are not part of the daily comparison series.
Their calculation, quote conversion, completeness flags, and audit contract are
defined in `docs/cex-depth-data-contract.md`.

When `dex_depth_latest.csv` exists, DEX catalog entries expose fixed-block
10/25/50/100 bps sell, buy, and total pool-state depth, protocol model, fee,
block number, completeness, and source lineage. Unsupported protocols remain
`null`. The exact contract is `docs/dex-depth-data-contract.md`.

DEX USD price-time alignment flags are measured-only. They are evaluated only
for an observed, complete, or partial DEX depth snapshot that contains at least
one finite USD depth band and declares a conversion requiring temporal
alignment. Unsupported, unavailable, failed, or not-cataloged depth cannot
receive a price-time mismatch merely because its numeric depth is `null`.

The independent execution-cost endpoint is backed by
`cex_execution_cost_latest.csv` and `dex_execution_cost_latest.csv`. It never
interpolates the four depth bands. Complete, partial, unsupported, and failed
semantics, fee scope, formulas, and publication gates are defined in
`docs/execution-cost-data-contract.md`. Its requested notionals are JSON
numbers; measured Decimal facts remain exact base-10 strings or `null`.

CEX configured pair labels normally use USDT. The current adapter contract uses
USDT as a 1:1 USD proxy; Upbit KRW observations are converted through the
daily USDT/KRW rate. Some adapters fetch a venue-native USD pair even when the
stored configured label is `TOKEN/USDT`, so the catalog describes that label
as canonical rather than claiming it is the raw venue instrument.

DEX price and volume facts come from GeckoTerminal API v2 daily pool OHLCV with
`currency=usd` and the configured target Token side.

## Missing values

Missing source values remain JSON `null`. The API emits the union of observed
dates and does not forward-fill:

- `market_a_missing`: only market B has a row on that date;
- `market_b_missing`: only market A has a row on that date;
- `non_comparable_price`: both rows exist, but a finite positive close is not
  available.

Absolute spread and bps are `null` unless both prices are comparable on the
same UTC date. Volume is never replaced with zero.

Daily quality uses the intersection of the selected window and a market's
observed lifecycle. A window wholly before the first or after the last
observed date is `not_applicable`, not a failed collection. A missing date
inside the first/last observed lifecycle is `backfill_pending` with
`missing_unexplained` unless a CSV-hash-matched collector attempt proves a more
specific cause. Such evidence distinguishes retryable `collection_failed`,
non-retryable `source_no_observation` (the source answered successfully but
returned no target candle), and non-retryable `needs_review`; absence alone
never becomes `collection_failed`.
One exact `stale_market_unknown` issue can also become informational
`source_no_observation` through a validated revision in
`data/curated/market_lifecycle_reviews.json`. The revision is bound to the
original issue ID, market ID, and UTC date and retains source-check hashes and
normalized observations. It does not apply to a later date and cannot mark a
market inactive or delisted.
The separate daily quality publication decides whether a D-1 gap satisfies
the active-market retry rule. Capability limits (`unsupported`), partial
measurements, observed market conditions, and data-health failures are not
collapsed into one warning state.

Daily attempt evidence is exact-market evidence. CEX matching requires
`token_symbol × exchange × canonical instrument`; a validated source alias is
additional lineage and never replaces the canonical instrument. DEX matching
requires Token, chain, DEX adapter, and pool. Attempt IDs and UTC-aware
completion times must be valid and unique, request windows bounded and exact,
observed dates inside the window, and status/reason/outcome/count invariants
consistent. The entire malformed ledger is ignored fail-closed, leaving the
gap `missing_unexplained`; no partial identity match may assign a cause.

A successful CEX source response with no usable two-sided book is normalized
to non-retryable `source_no_observation/source_no_two_sided_book`. It remains
`N/A`, retains bounded lineage, and is not turned into measured zero or a
transport failure. Network, timeout, rate-limit, source-unavailable, parse,
and validation outcomes remain retryable technical failures.

Daily refresh and latest-snapshot refresh have separate postconditions. An
ordinary daily refresh must publish one new matching SQLite/report identity,
contain a successful row in the requested Token/window, and leave no retryable
or hard-invalid issue there; an exact daily retry additionally resolves every
requested market-date as observed or an allowlisted terminal outcome. A
TVL/depth snapshot refresh instead compares the exact canonical Market and Fact
before and after, and succeeds only when both the publication bytes and
snapshot identity change and the new exact Fact resolves to measured evidence
or a valid terminal non-retryable source outcome. Exit code zero, an unrelated
Market update, unchanged publication identity, retryable failure, or
`needs_review` is not success.

## Explicit non-claims

The comparison input is daily aggregate OHLCV. It is not order-book depth,
top-of-book bid/ask spread, an executable quote, or measured slippage. The
comparison page must not relabel it as any of those concepts. Separately
collected CEX order-book fields retain their own point-in-time timestamps and
must not be presented as daily history or guaranteed execution. Separately
collected DEX pool-state fields are also point-in-time measurements and exclude
gas, MEV, and post-block state changes.

Funding Rate is fully outside this contract and release: there is no
derivatives Market catalog, funding collector, funding Fact, placeholder
field, quality projection, or dashboard control in this round.

## Known-answer fixtures

`tests/fixtures/market_known_answers.json` fixes expected values for:

- absolute and midpoint-bps price calculations;
- integer base-unit conversion using explicit Token decimals.

The decimals utility is a normalization primitive for future raw on-chain
facts. Current GeckoTerminal observations are already decoded numeric USD
values, so the website does not pretend to reverse-engineer raw pool balances.
