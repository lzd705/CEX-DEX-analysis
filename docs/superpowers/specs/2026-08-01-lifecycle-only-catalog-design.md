# Lifecycle-only Catalog Projection Design

## Goal

Make every Crypto.com market covered by current official lifecycle evidence
auditable in the public catalog, including configured instruments that have no
historical daily, depth, or execution rows. The projection must expose absence
evidence without inventing a candle, price, volume, depth, execution result, or
delisting date.

The immediate production gap consists of five configured instruments:

- `cex:crypto_com:CAKE/USDT`
- `cex:crypto_com:EIGEN/USDT`
- `cex:crypto_com:ETHFI/USDT`
- `cex:crypto_com:JTO/USDT`
- `cex:crypto_com:MORPHO/USDT`

All five are `instrument_present=false` in the current official lifecycle
manifest and have zero historical daily rows and zero depth rows. GMX/USDT and
RAY/USDT are also absent from the official current catalog, but each already has
historical daily rows and therefore already enters the public catalog before
the lifecycle overlay.

## Product contract

1. A configured Crypto.com instrument represented by a validated lifecycle
   absence review but missing from the source fact payload becomes one
   lifecycle-only CEX catalog entry.
2. The entry is evidence, not a synthetic market observation. Every numeric
   market fact remains null, every price series remains empty, observation
   dates remain null, and only the historical source-observation count records
   zero; the current-window observation count remains null.
3. The lifecycle entry is visible in Markets and Data Quality with the existing
   `inactive_cex_instrument` critical reason, official check time, source, and
   response hash.
4. The entry is excluded from primary-market selection, aggregate metrics,
   volume/spread/return/volatility/depth/TVL ranking, Compare readiness, and
   executable-opportunity ranking. Null is never coerced to zero.
5. Daily, TVL, depth, and execution facts use their existing canonical
   lifecycle-withheld projections. The absence is non-retryable, so no public
   refresh button is shown.
6. The projection does not add or modify database rows, CSV rows, raw responses,
   quarantine records, or collection-attempt ledgers. It is a read-time API
   projection derived from the validated lifecycle manifest.
7. Upbit is outside this change. No Upbit fact, configured identity, catalog
   entry, quality reason, migration rule, or release-check exception changes.

## Projection boundary

`overlay_cex_instrument_lifecycle()` remains the only component allowed to
materialize a lifecycle-only entry. Before applying absence/staleness fields it
will:

1. Index existing CEX payload markets by canonical market ID.
2. Iterate the validated manifest's absence reviews.
3. For each reviewed ID absent from the payload, append a minimal CEX market
   seed using only the review's token symbol, exchange, instrument, and source
   lineage.
4. Run the new seed through the same lifecycle-withholding loop as an observed
   historical market.

This keeps source-row construction in the database/CSV builders and lifecycle
evidence construction in the lifecycle overlay. The release checker remains
fail-closed and is not relaxed.

Only explicit absence reviews may create a lifecycle-only seed in this version.
A configured instrument that is neither observed nor present in an absence
review remains a contract failure. That state would mean the manifest lacks the
per-market evidence required to describe the missing entry honestly.

## Lifecycle-only seed

The seed contains the normal public CEX identity fields:

- `token_symbol`
- `market = "cex"`
- `venue = "crypto_com"`
- `instrument`

The seed declares zero historical source observations and no market facts:

- source observation counts: `0`
- observation dates and requested-window coverage dates: `null`
- price, volume, return, volatility, coverage, and spread: `null`
- price points: `[]`
- bid, ask, midpoint, and every 10/25/50/100 bps depth value: `null`
- depth completeness flags: `false`

The existing lifecycle loop then attaches:

- `current_listing_status = "absent_from_official_current_catalog"`
- `current_listing_reason_code = "instrument_absent_from_current_catalog"`
- official check time, source endpoint, and response SHA-256
- `depth_status = "source_no_observation"`
- the canonical lifecycle reason across Daily, Depth, and Execution quality

After withholding, the current-window `observation_count` remains null while
`historical_observation_count` records zero. This distinguishes “no current
fact” from “zero historical source rows” without inventing availability.

Historical-observation lineage is explicitly zero/null rather than borrowed
from another exchange, quote asset, token, or window.

## API and UI effects

- The full production catalog increases from 514 to 519 markets under the
  current evidence set.
- Crypto.com catalog identities become exactly the 30 configured identities
  bound by the lifecycle manifest hash.
- Summary catalog counts, per-token catalogs, full-scope Data Quality, and
  screening-quality parity all include the five entries.
- Markets shows the entries as unavailable current instruments with N/A values
  and an information disclosure. They are not offered as default Market A/B
  choices when a measured alternative exists.
- Screener aggregates and ordering remain based only on finite source-backed
  values. The additional critical reason may affect the token's quality count,
  but never its metric value.
- A selected date window does not create or remove the lifecycle-only identity;
  only its daily fact window metadata changes. Current lifecycle evidence is a
  point-in-time catalog fact independent of historical date selection.

## Failure handling

Publication remains blocked when any of the following is true:

- a review identity is malformed or conflicts with its token/exchange/instrument;
- the manifest configured count or configured-ID hash differs from the current
  configured Crypto.com registry;
- the official catalog evidence is older than 36 hours;
- a lifecycle-only seed receives any finite market fact or non-empty series;
- duplicate market IDs appear after projection;
- summary, catalog, and full-scope quality inventories diverge.

Stale root evidence continues to withhold current facts with the existing
`stale_cex_lifecycle_evidence` reason. The projection does not claim a current
absence from stale evidence and does not infer a delisting date.

## Verification plan

Implementation starts with failing tests for:

1. a reviewed zero-history market materializing exactly once;
2. an observed historical market not being duplicated;
3. all numeric facts remaining null and series empty;
4. canonical lifecycle flags matching Daily, Depth, and Execution projections;
5. retryability remaining false and no refresh action being exposed;
6. lifecycle-only entries being excluded from primary selection and every
   metric ranking;
7. summary/catalog/quality identity parity and the exact configured lifecycle
   hash;
8. unchanged Upbit identities and facts;
9. Python 3.8 compatibility;
10. desktop and mobile N/A disclosure behavior in a real browser.

The full automated suite, production Python 3.8 suite, health checker, strict
release checker, source freshness checks, and real-browser QA must run before
deployment is called complete. Because the separately preserved historical
Upbit/KRW fallback identities remain outside configured Upbit exact identities,
the unmodified release checker may still stop at that user-approved exception;
diagnostic continuation must prove that no lifecycle-catalog mismatch remains.

## Scope boundary and approved follow-on direction

This change closes a catalog-evidence gap only. It does not implement order
routing or label a route executable.

The approved product direction is a two-layer system:

1. Research Screener: default 30-day window, custom date range, user-selectable
   sorting, and USD volume as the default research metric.
2. Executable Opportunities: cross-venue routes ranked by net executable edge
   and capacity after a maximum 60-second snapshot-skew gate, explicit trading
   fees, DEX protocol fees, gas, transfer costs, and measured slippage.

Funding rate remains excluded. Event Facts may expand through Past/Future and
lifecycle views, official revisions, affected markets, and source-backed
pre/post-event impact windows without presenting temporal coincidence as
causality.
