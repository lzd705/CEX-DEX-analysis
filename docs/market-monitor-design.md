# Market Monitor Design

## Page structure

1. `Screener` is the cross-Token entry point. The global start/end, 7D, 30D,
   90D, and All controls apply only to daily facts.
2. `Markets` is the Token-level market catalog and the only page that edits
   Market A/B. `Token Research` then contains four peer pages: `Compare`,
   `Liquidity & Execution`, `Events`, and `Data Quality`.
3. Market A and Market B are exact market IDs for the selected Token and remain
   in the URL while the user moves between the four research pages.
4. Shared definitions, source lineage, and snapshot timing live in the
   `Data Quality` page as a compact disclosure; there is no separate
   Methodology page.

The workspace separates discovery, comparison, executable-liquidity analysis,
event timing, and evidence without losing the selected Token or market pair.
TVL, depth, and execution remain independently timestamped latest snapshots
rather than pretending to follow the daily date selector. Events are a
manually reviewed source-backed timeline and do not inherit a market-data
collection cadence.

## Page responsibilities

- `Markets` lists every cataloged CEX pair and DEX pool for the Token and sets
  exact Market A/B identities.
- `Compare` aligns A/B daily observations and provides selectable Price,
  Spread, and Volume line charts plus the raw table.
- `Liquidity & Execution` compares the latest 10/25/50/100 bps depth and
  fixed-notional quoted-cost scenarios with source-state timestamps.
- `Events` lists the latest verified revision of matching Event Facts, with
  independent `Past / Future / Current` clock filters and evidence-lifecycle
  filters. Clock state never upgrades a scheduled event to occurred.
- `Data Quality` explains coverage, freshness, missing/unsupported facts,
  warnings, recovery eligibility, and lineage for the current Token and A/B
  pair. Capability limits and observed market conditions are kept separate
  from collection or validation failures.

## Spread placement

In the Screener, cross-venue is a fixed context of each Spread ranking metric,
not a fourth peer scope button. Latest absolute gap, maximum absolute gap,
mean absolute gap, and median absolute gap are independently sortable over the
selected UTC window. All four use the primary CEX/DEX pair and only common
valid observation dates; missing comparisons remain null and rank last.

Spread is not a property of either market alone. For the selected Token, Market
A, Market B, and date window, each same-date comparison is:

```text
absolute_spread_usd = abs(price_a_usd - price_b_usd)
spread_bps =
  absolute_spread_usd / ((price_a_usd + price_b_usd) / 2) * 10,000
```

The two prices must come from the same UTC date. If either price is missing or
invalid, spread is `N/A`. The Compare chart treats missing, invalid, and
nonconsecutive dates as gaps and never draws through them.

Price and Volume show both A and B; Spread is one derived series. A/B use
different marker shapes and full type/venue/instrument labels in addition to
color. This keeps the comparison legible in grayscale and for users who cannot
distinguish the palette.

## Event placement

The Events page is the auditable record. Compare may add a marker or precision
interval when a published Event Fact overlaps the chart window. The overlay
answers only “when did this source-backed event take effect?” It does not
answer “what did the event cause?” and does not calculate pre/post return,
abnormal return, volume impact, sentiment, or importance.

Event time and evidence lifecycle are deliberately separate. `Past`, `Future`,
and `Current` describe the published effective-time interval relative to the
API response clock. `Scheduled`, `Occurred`, `Postponed`, `Cancelled`, and
`Superseded` describe what the evidence supports. A past scheduled event is
therefore shown as “effective time passed; occurrence unconfirmed,” not as an
occurred event.

Event Facts are curated from official sources and published as append-only
revisions. The repository contains 44 latest facts with at least one verified
fact for each of the 30 configured Tokens. This is presence coverage, not a
claim of complete event history; production coverage and counts are always
read from the latest validated bundle selected by `latest.json`.

## Data contract

The server reads detailed daily CEX and DEX files and returns compact summaries
for the selected window. It does not send every raw observation to the browser.
Each venue/pool summary includes latest price, window return, daily log-return
volatility, summed USD volume, observation-day count, and latest observation
date. DEX summaries may also include the latest available TVL snapshot.

Every table-level missing value is rendered as `N/A` with an adjacent
information disclosure. TVL and depth show a user refresh action only when the
server's quality contract marks the exact canonical market Fact as
`retryable=true`. The public action is rate limited, job-budgeted, and accepts
only `cex:<venue>:<instrument>` or
`dex:<chain>:<dex>:<pool>:<TOKEN>` identities. Unsupported methods,
not-applicable Facts, and genuine observed zeroes never become refreshable.
Execution is refreshed only as the atomically derived companion of a supported
depth refresh; a missing execution inventory row never exposes a dead-end
execution-only action.

Source timestamps use a strict timezone-aware ISO/RFC 3339 boundary. CEX
sources may provide more than six fractional-second digits; collectors and
readers normalize these values to canonical UTC microsecond precision by
truncating, never rounding, the sub-microsecond remainder. The original source
precision remains auditable in the retained raw response and its hash. A
missing source timestamp may use the recorded response time, but a nonempty
malformed source timestamp is published as an explicit `parse` failure rather
than silently receiving a substitute observation time.

Data Quality contract v4 deliberately carries two same-publication views. The
selected-window `quality_status` / `quality_flags` explain the current research
window; `screening_quality_status` / `screening_quality_flags` preserve the
exact Screener projection. Screener status chips and every structured flag
severity must reproduce the all-scope Data Quality rows for the same
`data_generation`, Token, and unique Market inventory. The release gate checks
every configured Token, counts every flag without deduplication or category
filtering, and rejects generation drift or an unexplained non-OK status.
Summary's declared Token/Market totals, the accumulated all-Token Quality
totals, and the full catalog inventory must agree exactly; the full catalog
must expose the same Token set and valid unique Market rows. Quality and the
full catalog must also have identical `(token_symbol, market_id)` sets, without
bare IDs reused across Tokens. The selected view must name exactly two distinct
Markets and expose only sorted canonical UTC affected dates with real integer
counts.
Matched daily evidence is market-bound, not merely response-bound: every
Market receives an explicit rollup and complete Fact evidence, including a
zero-count bundle when no issue applies. The release gate validates the
per-Market mode, canonical outcome, joint status/reason counts, and date
mapping before accepting the aggregate report totals. This prevents ordinary
projection bugs from silently reassigning a problem between Market A and
Market B and rejects marginal count combinations that no real issue set could
produce.

DEX price-time alignment is shown only for measured depth: an observed,
complete, or partial fixed-block row with at least one finite USD band and an
adapter-declared time-sensitive conversion. Unsupported, unavailable, failed,
and not-cataloged depth stays `N/A` with its capability or source reason; it
does not inherit a temporal mismatch. A CEX response with no usable two-sided
book is the terminal non-retryable source outcome
`source_no_observation/source_no_two_sided_book`, not zero depth and not a
network failure.

`/api/markets/compare` supplies the aligned daily observations used by both the
chart and table. `/api/markets/events` reads the separately published Event
Fact bundle and filters by Token, optional date interval, and lifecycle.

Fixed-notional execution is a current fact, not future scope. CEX visible-book
cost explicitly excludes a numeric trading fee because the account tier is
unknown (`excluded_unknown_account_tier`). Supported DEX V2 execution includes
the pool swap fee in pool mechanics; this is not a claim about protocol
treasury or revenue fee.

## Executable-opportunity release boundary

The synchronized route pipeline is a separate fact product from the dashboard's
per-market depth execution table. Its public generation is selected only by
`data/local/routes/latest.json` (or the matching runtime-data path) when that
pointer has schema `route_opportunity_pointer/v1` and stage
`route_opportunity/v1`. A newer core-only cohort under `routes/core` is normal
in-progress work; it neither becomes public nor invalidates the last complete
generation merely because the private core pointer moved.

One complete generation contains exactly five files: route legs, cost
components, opportunities, SQLite, and manifest. The release checker rereads
all five through the complete pointer and then independently checks exact
quantity lattice, Market/leg state timestamps, 60-second skew, 120-second
route age, component topology, null terminal costs, cost freshness, exact
gross/net arithmetic, bps numerators and denominators, source generations,
core binding, attestation, manifest counts/hashes, and CSV/SQLite parity.
`executable_candidate` is accepted only when both
`strict_ready_for_publication=true` and `strict_eligible=true`; a locally ready
but unattested prepublication row remains research-only and fails the public
strict gate. Estimated, assumed, stale, missing, and unsupported components
cannot be defaulted to zero or promoted by ranking or cache code. Pool fees
already reflected by pool mechanics are excluded from nonembedded-cost sums.

This gate does not activate a public route API or collection timer. It validates
an already finalized immutable bundle; it does not accept caller-supplied rows
and does not run a collection step.

Funding Rate is fully excluded: there is no derivatives catalog, funding
collector, funding Fact, placeholder field, quality state, or UI control in
this round. Authenticated CEX fee evidence and adapter-bound gas may exist only
inside the synchronized route-cost bundle; they are not inferred for the
ordinary spot-market execution table. DEX V3 fixed-notional execution and
event-study outputs remain unsupported.
