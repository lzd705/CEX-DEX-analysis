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
