# Collection cycle and freshness operations

## Profiles

All production collection runs enter through:

```bash
python3 scripts/run_collection_cycle.py --profile PROFILE --publish-local
```

| Profile | Ordered steps | Intended cadence |
| --- | --- | --- |
| `full` | incremental daily OHLCV, CEX depth/cost, published TVL, then DEX depth/cost | manual catch-up and release validation |
| `daily` | incremental daily OHLCV, TVL | daily at 00:30 UTC |
| `tvl` | TVL only | manual retry/recovery |
| `depth` | CEX depth/cost, temporary DEX USD-price refresh, then DEX depth/cost | hourly at minute 05 UTC |
| `cex_depth` | CEX depth and fixed-notional cost from one book snapshot | manual retry/recovery |
| `dex_depth` | temporary DEX USD-price refresh, then DEX depth/cost from one fixed block | manual retry/recovery |

The daily step reads the current CEX and DEX end dates, starts from the older
source with a three-day overlap, and ends at the latest completed UTC day. It
passes every configured Token to `run_fact_pipeline.py --append`, so the upsert
preserves older history. `--full-rebuild` is an explicit exception and must not
be used by timers.

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
`source_no_observation/no_candles`. `not_listed` and
`source_range_unavailable` are also non-retryable and enter manual review.

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
source-specific date ranges, and freshness.

Scheduled publishing also applies a post-step freshness gate. A collector that
exits zero while its expected source remains stale is recorded as failed. A
rate-limited or empty response therefore cannot masquerade as a successful
refresh.

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
rejects during that bundle preflight, neither corresponding latest view is
changed. Both family reports remain available for diagnosis. The collector
exits nonzero, and the previous latest naturally becomes stale rather than
being replaced with broad failure rows. Passing and rejected structured
reports are copied into `steps[].publication_gates` in the collection
manifest.

After a passing bundle preflight, depth/history and execution latest are still
separate atomic file replacements, not one cross-file transaction. An I/O or
process failure after the commit phase begins is not rolled back across those
files. The production runner lock serializes managed profiles; concurrent
direct collector invocations with `--publish-local` are unsupported. Retained
raw lineage and the run manifest make any interrupted publication diagnosable.

The separate fact lifecycles remain explicit:

- daily source CSVs are replaced individually, while the server-visible SQLite
  database is staged and atomically replaced last through
  `import_local_snapshot.py`;
- TVL appends normalized history, then atomically replaces its latest snapshot;
- CEX and DEX depth each append normalized history, then atomically replace
  their own latest snapshots. Their matching long-form execution-cost
  latest views reuse the same raw response/fixed-block lineage. Retained raw
  responses and manifests are the execution audit history for this release.

The hourly DEX USD-price refresh reuses the TVL collector's GeckoTerminal
multi-pool response but writes only
`data/processed/dex_pool_tvl_snapshot.csv`. It does not publish a new TVL fact
or append the TVL history. The following DEX collector explicitly reads that
file. If the refresh step fails, DEX depth is recorded as
`skipped_dependency` and the prior published DEX snapshot remains untouched.

A full collection manifest coordinates these publications but does not claim
that the CSVs, histories, latest snapshots, and SQLite database form one
multi-file transaction, or that all source APIs were observed at one instant.

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
root at `raw/tvl`, `raw/cex-depth`, and `raw/dex-depth`. Collector staging is
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
The daily service is intentionally not fail-fast: TVL is still attempted when
the independent daily OHLCV step fails, while the final service status remains
failed and auditable.
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
- The public API and rendered page display separate source dates and stale
  states.
- When Event Facts are published, the selected bundle passes source-record,
  revision-lineage, SQLite-integrity, manifest-count, and file-hash checks.
  The release checker requires the Event covered-token inventory to equal the
  current Screener Token inventory, requires `uncovered_tokens=[]`, and
  requests a non-empty scoped Event response for every covered Token.
  An unavailable Event bundle is not reported as a verified zero-event result.

Funding rates, numeric account-specific CEX fees, gas, DEX V3 fixed-notional
execution, and event-study outputs remain unsupported. Collection operations
must not manufacture them from spot prices, depth, TVL, pool swap fees, or Event
Facts.
