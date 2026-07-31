# Data Quality Integrity Hardening Design

**Date:** 2026-07-31  
**Status:** Approved in conversation; written specification awaiting final review  
**Repository:** `lzd705/CEX-DEX-analysis`  
**Branch:** `codex/critical-quality-sorting-token-refresh`

## Objective

Improve the correctness and auditability of the existing fact-only dashboard
before adding new data families. The round removes false data-health alerts,
makes Screener quality links reproduce the exact reasons counted by the
Screener, prevents refresh jobs from claiming success without an exact
postcondition, and tightens collection-attempt identity and lifecycle outcome
rules.

Funding Rate is explicitly excluded. This round does not create a derivatives
catalog, funding collector, funding UI, or placeholder funding contract.

## Baseline Evidence

The production audit captured on 2026-07-31 found:

- 30 Tokens and 493 catalog Markets;
- 419 Markets reported `ok`, 67 `critical`, 6 `warning`, and 1 `info` by the
  Token Data Quality API;
- 64 unsupported DEX depth Markets also received the critical
  `depth_usd_price_time_mismatch` flag even though no measured USD depth existed;
- Screener and Token Data Quality severity/reason counts differed for 27 of 30
  Tokens because they exposed different quality projections without preserving
  both contracts in the drill-down response;
- four facts appeared retryable, although the GMX and RAY Crypto.com empty-book
  results were source outcomes rather than transport failures; the remaining
  recoverable gap was MORPHO Upbit depth/execution publication coverage;
- no negative TVL or depth values, non-monotone depth curves, coverage outside
  `[0, 1]`, hard-invalid daily facts, retryable daily gaps, or latest D-1 gaps
  were found.

These counts are an observed baseline, not permanent test constants. Automated
tests use deterministic fixtures; production acceptance recomputes counts from
the then-current generation.

## Approaches Considered

### 1. Collection-first cleanup

Refresh every warning, critical, and N/A Market and hope the counts fall. This
was rejected because unsupported methods and real market conditions are not
collection failures. It would waste source capacity and preserve the false
classification rules.

### 2. UI-only relabeling

Hide or rename critical badges in the browser. This was rejected because the
API, retry policy, public actions, exports, and release checker would remain
internally inconsistent.

### 3. Contract-first integrity hardening — selected

Correct the backend semantics and evidence gates first, then render and drill
down from those contracts, then run only the exact remaining recoverable
collection. This is the approved approach.

## Scope

### Included

1. Measured-only DEX depth temporal-alignment evaluation.
2. Stable CEX depth source-outcome reason normalization.
3. Separate selected-window and Screener quality projections in one versioned
   Data Quality response.
4. Exact Screener-to-Data-Quality reason parity.
5. Refresh postconditions bound to the requested canonical Market and Fact.
6. An exact status/reason matrix for terminal absence, retryable failure, and
   protected manual review.
7. Exact CEX collection-attempt identity, including instrument.
8. One bounded MORPHO Upbit recovery attempt after all integrity gates pass.
9. Local, production-Python, release, browser, and post-deployment verification.

### Excluded

- Funding Rate and derivatives Markets;
- cold Summary optimization;
- coordinated multi-venue snapshot cohorts;
- new CEX fee, gas, transfer-cost, or net-arbitrage facts;
- new DEX protocol adapters;
- changing the declared TVL, price-deviation, spread, or coverage thresholds;
- converting unsupported, missing, or failed values to zero;
- broad historical recollection unrelated to an exact published retry item.

The excluded performance, coordinated-snapshot, and cost-fact work must receive
separate specifications. Net spread and arbitrage capacity remain a separate
research/scenario layer rather than a fact-quality change.

## Quality Semantics

### Data-health status

The overall Market quality status remains derived only from `data_health`
flags. Capability limits, measurement limits, market conditions, and
availability remain visible reasons but do not become data failures merely
because their numeric fact is null.

### DEX depth temporal alignment

`depth_usd_price_time_mismatch` and `depth_usd_price_time_warning` are evaluated
only when all of the following are true:

- the Market is DEX;
- depth status is `observed`, `partial`, or `complete`;
- at least one USD depth band contains a finite measured value;
- the adapter declares a USD conversion requiring temporal alignment.

An unsupported, unavailable, failed, or not-cataloged depth fact cannot receive
a USD price-time mismatch flag. It retains its own capability, availability, or
failure reason.

### CEX empty-book outcomes

Collector codes are allowlisted. Legacy text is normalized at the public API
boundary. A successful source response with no usable two-sided book retains
its raw snapshot evidence but is projected publicly as status
`source_no_observation` with reason `source_no_two_sided_book`. It is
non-retryable and exposes a bounded public explanation rather than a raw
exception string. Network, timeout, 429, and 5xx failures retain retryable
technical reason codes.

An empty two-sided book is not relabeled as observed zero depth. No execution
or depth value is fabricated.

## Dual Quality Contract

The Data Quality contract advances from version 3 to version 4. Each Market
retains the existing selected-window fields:

- `quality_status`
- `quality_flags`

and adds an immutable Screener projection for the same data generation:

- `screening_quality_status`
- `screening_quality_flags`

The Screener summary and the screening projection must be produced from the
same catalog-quality helper and generation. A Data Quality request with
`origin=screener` filters and counts `screening_quality_*`; ordinary Token
research continues to use selected-window `quality_*`.

Screener deep links carry `severity` and, when a single exact reason is
selected, may also carry canonical `market_id` and `reason_code`. The receiving
page must display the same reason count as the originating chip. A successful
release requires parity for all 30 configured Tokens, not just one fixture.

## Refresh Postcondition

Before a public or administrator snapshot refresh starts, the service records:

- canonical Token, Market ID, and Fact type;
- current source publication identity and snapshot ID;
- current fact status, reason code, observation time, and retryability;
- current data generation.

After the collector exits, the service reloads uncached published state and
verifies the exact requested Market and Fact. Exit code zero alone is never a
success condition.

A refresh succeeds only when a new relevant publication is selected and the
exact Fact reaches one of these evidence-backed outcomes:

- `observed` or `partial` with a new source snapshot; or
- a valid terminal `source_no_observation` or `unsupported` status/reason pair
  produced by the new publication.

It does not succeed when:

- the relevant publication identity is unchanged;
- only another Market or Fact changed;
- the requested Fact remains retryable;
- status is `needs_review`;
- the postcondition cannot load or validate the new publication;
- a hard-invalid fact appears.

The job result reports pre/post identities and the exact resolved outcome. It
does not expose protected source paths or unbounded raw errors.

## Status and Reason Matrix

The implementation uses one shared allowlisted matrix for quality generation,
public projection, and refresh postchecks:

| Status | Valid reason class | Retryable | Resolution |
| --- | --- | ---: | --- |
| `observed` | observed | no | measured fact |
| `partial` | explicit measurement limit | no | measured lower bound |
| `collection_failed` | network, rate limit, source unavailable, parse, validation | yes | retry remains open |
| `source_no_observation` | no candles, no two-sided book | no | terminal source outcome |
| `unsupported` | validated source range, chain, protocol, or method limit | no | terminal capability outcome |
| `needs_review` | listing/lifecycle ambiguity or invalid outcome-contract evidence | no | unresolved manual queue |
| `backfill_pending` | missing unexplained | yes | retry remains open |
| `invalid` | hard fact-contract violation | no | blocked for review |

`needs_review` can never be treated as confirmed absence or successful
resolution. Unknown status/reason combinations fail closed.

## Exact Attempt Identity

CEX collection-attempt evidence matches a daily Market only when all canonical
identity components agree:

```text
token_symbol × exchange × instrument
```

Token plus exchange is insufficient. A failure for one quote instrument cannot
reclassify another instrument's missing date.

Any exchange-specific source fallback, including Upbit source-instrument
resolution, must be recorded explicitly as a validated alias while preserving
the canonical catalog instrument. It cannot be inferred from token and exchange
alone.

Attempt IDs must be non-empty and unique. Attempt timestamps must parse as
UTC-aware instants. Observed dates must fall inside the requested window, and
status, reason, outcome, and observed-count invariants must agree. Invalid
ledgers are ignored as evidence and leave the gap `missing_unexplained` rather
than assigning a fabricated cause.

## MORPHO Recovery

Production recollection happens only after the preceding contracts, tests, and
release checks pass. The service re-evaluates the current MORPHO Upbit quality
state immediately before acting. If it is no longer retryable, no job is
created.

If it remains retryable, run one bounded CEX depth profile for MORPHO. The same
source publication may produce both depth and execution facts. The refresh
postcondition decides the job result; no manual status override is permitted.

Afterward, record:

- pre/post snapshot and data-generation identities;
- depth and execution status/reason changes;
- whether the exact Market entered the publication;
- whether any new warning, critical, invalid, or retryable state appeared.

No repeated retry loop is authorized by this specification.

## Error Handling and Rollback

- Unknown reason/status combinations fail closed and do not publish a success.
- A failed quality-contract migration keeps the old production process serving
  until preflight passes.
- Deployment requires a previous commit identifier and environment backup.
- If health, release parity, browser drill-down, or post-deployment quality
  checks fail, restore the previous application commit and restart the service.
- Published data is not rolled back merely to hide a real source outcome.

## Testing and Acceptance

Implementation follows red-green-refactor TDD. Required regression coverage:

1. Unsupported DEX depth produces capability information and never a temporal
   mismatch flag.
2. Measured stale DEX USD depth still produces a critical temporal mismatch.
3. Measured warning-skew depth still produces a warning.
4. Legacy empty-book CEX results normalize to a non-retryable stable reason.
5. Technical CEX failures remain retryable.
6. Screener and screening quality projections have exact status/reason parity
   for every Token.
7. Selected-window quality remains independent from screening quality.
8. Unchanged, unrelated, and still-retryable refresh postconditions fail.
9. Exact observed, partial, source-no-observation, and unsupported postconditions
   pass only with a new relevant publication.
10. `needs_review` never resolves a retry.
11. A CEX attempt for a second instrument cannot explain the target Market.
12. Invalid IDs, timestamps, dates, and attempt invariants are rejected.
13. Missing values remain null and measured zero remains zero.

Release acceptance requires:

- the complete local test suite passes;
- production Python 3.8 compilation, imports, and tests pass, with frontend
  tests run locally when Node is unavailable on the server;
- the release checker validates quality contract v4 and 30/30 Screener
  drill-down parity;
- production has no unsupported-depth temporal mismatch flags;
- every public retry button corresponds to an exact backend-retryable Fact;
- browser QA covers desktop and 390px mobile Screener-to-Quality navigation;
- `/health` reports current usable data after deployment;
- the branch, GitHub remote, and production checkout resolve to the same commit.

## Delivery Order

1. Quality semantics and CEX reason normalization.
2. Dual quality contract and Screener parity.
3. Refresh postcondition and shared status/reason matrix.
4. Exact attempt identity and ledger validation.
5. Full local and production-compatible verification.
6. Deployment and browser QA.
7. One conditional MORPHO recovery and postcheck.

Each commit and push must carry an explicit message/comment, and deployment is
reported separately from GitHub synchronization.
