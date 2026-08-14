# Observed data-quality snapshot contract

## Purpose and boundary

`data_quality_snapshot/v1` is a deterministic, machine-readable observation of
the data that is actually present below one operator-supplied `--data-dir`.
It is not a collector, a connector, a release check, or proof that an absent
source contains zero facts. Missing inputs are reported as `not_evaluated`
with null counts and an explicit reason.

The snapshot is safe to publish. It contains only allowlisted metrics, logical
input names, byte counts, and SHA-256 hashes. It must never contain absolute or
home-relative paths, environment values, credentials, cookies, account data,
raw source payloads, exception traces, or authenticated connector content.

Funding Rate is outside this contract.

## Deterministic invocation

The command is:

```bash
python3 scripts/build_data_quality_snapshot.py \
  --data-dir /path/to/data \
  --generated-at-utc 2026-08-14T00:00:00Z \
  --window-end 2026-08-13 \
  --window-days 30 \
  --application-sha 0123456789abcdef0123456789abcdef01234567 \
  --output data/public/quality/latest.json
```

`--generated-at-utc` is required rather than read from the wall clock.
`--window-end` is the last included UTC date and `--window-days` defaults to
30. The only supported window timezone is `UTC`. `--application-sha` is a
required full 40-character Git SHA. The generator never infers source identity
from a mutable checkout or an environment variable.

Files are resolved only through fixed relative candidates below `--data-dir`.
Runtime files may be placed directly below that directory or below its
`local/` child. Curated files may be placed directly below it or below its
`curated/` child. If two candidates for one logical input both exist, the
family fails with `ambiguous_source_candidates`; no precedence is guessed.
Inputs must be bounded regular files, may not be symlinks, and are parsed from
the same stable file capture used to compute their SHA-256. A capture whose
identity or size changes during the read fails closed. The output never records
the supplied directory or a resolved private path.

Canonical JSON uses UTF-8, sorted keys, compact separators, a final newline,
and no non-finite numbers. Lists with no semantic order are sorted by their
declared identity. `snapshot_sha256` hashes the canonical payload with that
field omitted. With identical input bytes and identical explicit arguments,
the complete output bytes and hash are identical.

## Family registry

| Family | Grain | Candidate primary key | Observation time | Runtime or tracked source |
| --- | --- | --- | --- | --- |
| `cex_daily_ohlcv` | one UTC day x Token x exchange x exact instrument | `date, token_symbol, exchange, cex_symbol` | `date` | `cex_exchange_volume_daily.csv` plus the current `market_facts.sqlite3` CEX catalog |
| `dex_daily_ohlcv` | one UTC day x Token perspective x chain x pool | `date, token_symbol, chain, pool_address` | `date` | `dex_pool_volume_daily.csv` plus the current `market_facts.sqlite3` DEX catalog |
| `tvl` | one point-in-time pool observation | latest: `token_symbol, chain, pool_address`; history additionally `snapshot_id` | `observed_at` | `dex_pool_tvl_latest.csv` |
| `cex_depth` | one point-in-time exact CEX book | latest: `token_symbol, exchange, cex_symbol`; history additionally `snapshot_id` | `observed_at` | `cex_depth_latest.csv` |
| `dex_depth` | one fixed-block pool-state observation | latest: `token_symbol, chain, pool_address`; history additionally `snapshot_id` | `block_timestamp`, `observed_at` | `dex_depth_latest.csv` |
| `cex_execution_cost` | one CEX market x direction x requested notional | `snapshot_id, market_id, direction, requested_notional_usd` | `state_observed_at`, `observed_at` | `cex_execution_cost_latest.csv` |
| `dex_execution_cost` | one DEX market x direction x requested notional | `snapshot_id, market_id, direction, requested_notional_usd` | `block_timestamp`, `state_observed_at`, `observed_at` | `dex_execution_cost_latest.csv` |
| `event_facts` | one event revision | `event_id, revision` | `effective_at` with explicit precision; audit clocks use `source_checked_at_utc`, `recorded_at_utc` | `curated/event_facts.csv` and reviewed evidence records |
| `cex_instrument_lifecycle` | one current-catalog absence review | `market_id` inside one bounded manifest | `checked_at_utc` | `curated/cex_instrument_lifecycle.json` |
| `market_lifecycle_reviews` | one exact issue disposition revision | `review_id, revision` | `issue_date`, `reviewed_at_utc` | `curated/market_lifecycle_reviews.json` |
| `route_cohort_opportunity` | primary entity: one `route_id` x requested notional opportunity; nested route-core entity: one directed `route_id` | `opportunity_id`; nested `route_id` | cohort collection bounds and opportunity evaluation time | validated `routes/latest.json` bundle pointer |
| `route_shadow_route_cost_evidence` | primary entity: one route/notional cost binding; nested entities: one shadow run, selected market, and transcript | binding key declared by the sealed manifest; nested `run_id`, `market_id`, and transcript key | audit/evidence observation bounds | validated `routes/shadow/latest.json` joint pointer |

Daily OHLCV rows have no intrinsic status column. Their collection status is
separate, lineage-bound attempt or quality evidence. Point-in-time status
vocabularies remain source-specific:

- TVL: `observed`, `missing`, `not_found`, `failed`;
- CEX depth: `observed`, `partial`, `failed`;
- DEX depth and both execution families: `observed`, `partial`,
  `unsupported`, `failed`;
- Event lifecycle: `scheduled`, `occurred`, `postponed`, `cancelled`,
  `superseded`;
- Event evidence: `primary_confirmed`, `cross_checked`, `onchain_observed`;
- CEX lifecycle absence: `absent_from_official_current_catalog` with
  `instrument_absent_from_current_catalog`;
- market lifecycle review: `disposed` or `withdrawn`; a disposed current
  revision uses `source_no_observation/no_candles`.

Execution `status_reason` is retained as a bounded observed value, not
misrepresented as a globally closed enum.

## Required snapshot fields

The top-level object contains:

- `schema_version`;
- required, canonical `generated_at_utc`;
- `application.build_sha`;
- `publication.identity`, derived from the sorted input identities and the
  explicit generation arguments;
- `window.start_date`, `window.end_date`, `window.expected_days`, and
  `window.timezone = UTC`;
- `summary` counts for `evaluated`, `not_evaluated`, and `failed` families;
- exactly one entry for every registered family, sorted by family name;
- `snapshot_sha256`.

Every family entry contains its grain, primary key, time fields, state, source
identity, counts, quality metrics, observation bounds, and freshness. The
two combined route families additionally expose an `entities` object. Each
entity has its own grain, key, declared expected count, observed count, usable
count, duplicate rate, and status/reason counts; counts from different grains
are never added together. The common family counts use the declared primary
entity in the table above. The common metric shape is:

```json
{
  "state": "evaluated | not_evaluated | failed",
  "not_evaluated_reason": null,
  "failure_reason": null,
  "counts": {
    "expected": null,
    "observed": null,
    "usable": null,
    "expected_basis": null
  },
  "coverage_bps": null,
  "duplicate_primary_key": {"count": null, "rate_bps": null},
  "required_field_null": {"count": null, "rate_bps": null},
  "measurements": {
    "null_count": null,
    "zero_count": null,
    "fields": {}
  },
  "status_counts": {},
  "reason_counts": {},
  "observation_time": {
    "min": null,
    "max": null,
    "freshness_lag_seconds": null
  },
  "source": null
}
```

Rates use integer basis points with deterministic half-up rounding. A missing
denominator produces null, not zero. `observed` counts input grains;
`usable` counts rows that pass required identity, timestamp, status, and
measurement rules. A literal numeric zero increments `zero_count`; an empty
field or JSON null increments `null_count`. No normalizer converts one into
the other. Per-field measurement metrics are always retained; the aggregate
counts are a convenience total, not a substitute for field semantics.

An absent required source uses `state = not_evaluated`, null counts and rates,
and a stable reason such as `source_file_missing` or
`route_pointer_missing`. A present but malformed source uses `state = failed`,
a public allowlisted failure reason, and no exception text or path.

## Daily market-date coverage gate

For each daily family, the evaluator constructs the exact Cartesian grid:

```text
market identity x every UTC date in the inclusive requested window
```

The market inventory comes from the current, integrity-checked
`market_facts.sqlite3` dataset pointer and its distinct exact CEX or DEX market
rows. CEX catalog entries with validated `lifecycle_withheld = true` are not
ranking candidates. `expected_basis` records the SQLite input SHA, current
snapshot/import identity, market count, and a hash of the canonical market-ID
inventory. If the SQLite catalog is absent, stale relative to the CSV binding,
or invalid, the daily family is `not_evaluated` or `failed`; it never falls
back to the set of markets that happen to have CSV rows.

It never substitutes the distance between the minimum and maximum dates for
this grid. A row with measured volume `0` is observed. A missing row is not
observed and is never filled with zero.

Daily output includes:

- `expected_market_date_count` and `observed_market_date_count`;
- `complete_market_count`, `incomplete_market_count`, and
  `ranking_eligible_market_count`;
- `disposition_counts` for `observed`, `pre_listing`, `post_delisting`,
  `structurally_unsupported`, `source_no_observation`, `collection_failed`,
  and `missing_unexplained`;
- a deterministic bounded list of incomplete summaries carrying only a
  domain-separated `market_identity_sha256`, never a raw Market ID;
- `completeness_state = complete | incomplete | not_evaluated`.

Only exact, source-bound evidence may classify a non-observation. Current
catalog absence does not invent a delisting date. The first or last observed
row alone does not prove listing or delisting. Without date-effective evidence,
the disposition is `missing_unexplained`. Consequently, one observed day in a
30-day window yields an incomplete market with 29 missing dates and zero
ranking eligibility.

The implementation in this branch must also make
`scripts/route_shadow_inputs.py` apply the same non-negotiable observed-grid
rule to active CEX markets admitted to the route ranking input. Every such
market must have all 30 UTC date rows with a non-null, finite, non-negative
`quote_volume_usd`. Real zero volume passes; an absent or unusable date fails
input construction before ranking. Lifecycle-withheld markets are not rank
candidates and are not silently reclassified as historical delistings.

## Versioned family rules

The implementation registry fixes these validation inputs; it does not infer
them from headers at runtime.

| Family | Required identity/structure | Measurement fields and usable rule | Freshness clock |
| --- | --- | --- | --- |
| CEX daily | all ten `cex_market_daily` CSV fields; canonical exact instrument | positive finite OHLC and non-negative finite `base_volume`, `quote_volume_usd`; all must be valid | latest UTC `date` |
| DEX daily | all twelve `dex_pool_daily` CSV fields | positive finite OHLC, non-negative finite `dex_volume_usd`; optional `pool_tvl_usd` must be non-negative when present | latest UTC `date` |
| TVL | complete `TVL_COLUMNS`; one snapshot and exact pool identity | `status=observed` with finite non-negative `tvl_usd` | `observed_at` |
| CEX depth | complete `DEPTH_COLUMNS_ALL`; one snapshot and exact instrument | `observed` or `partial` with a valid two-sided book and measured depth fields | `observed_at` |
| DEX depth | complete `DEX_DEPTH_COLUMNS`; one snapshot and exact pool | `observed` or `partial` with fixed-block lineage and measured depth fields | `block_timestamp` |
| execution | complete `EXECUTION_COST_COLUMNS`; one snapshot/source snapshot and exact 2 x 5 scenarios per market | `observed` or `partial` with finite requested notional, filled quantity, and quoted execution cost; DEX also requires fixed-block/USD-price lineage | `state_observed_at` |
| Event Facts | exact curated header, revision sequence, precision rules, source-record binding | a normalized revision with readable bound evidence is usable; latest revisions are counted separately | `recorded_at_utc` |
| CEX lifecycle | exact manifest/review fields and declared count parity | every validated current-catalog absence review is usable | `checked_at_utc` |
| market lifecycle reviews | exact root/revision/source-check structure and continuous revisions | latest valid revision per `review_id` is usable | `reviewed_at_utc` |
| route families | exact pointer, manifest, artifact inventory, hashes, and declared counts | entity-specific valid terminal/measured states; no absent artifact is synthesized | manifest observation bounds |

The stale threshold for point-in-time families is 86,400 seconds. Daily
freshness is measured from the start of the UTC day following the latest
observed date. Event and lifecycle evidence remain evaluated when stale, with
`stale_partition` surfaced explicitly.

Event source records resolve only below the fixed `evidence/events/` root.
`source_record_file` must be a canonical relative path with no traversal; the
target must be a bounded regular non-symlink JSON file. Its `record_schema`,
source URL, checked time, and `record_locator` must agree with the curated row.
Every used evidence file is a separately hashed, sorted publication input.

## Input validation

- Required headers and fields are explicit per family.
- Primary keys must be complete and unique after canonical identity
  normalization.
- Daily dates must be canonical `YYYY-MM-DD` values.
- Point-in-time clocks must be timezone-aware RFC 3339 and must not be later
  than `generated_at_utc`.
- A latest point-in-time file must contain one `snapshot_id`; multiple snapshot
  IDs are mixed-grain input and fail closed.
- A stale partition remains evaluated but is surfaced through freshness and a
  stable `stale_partition` reason/status count; it is not refreshed or hidden.
- The fixed execution inventory is five notionals by two directions per
  market. Missing or duplicate scenarios reduce usability or fail the key
  contract; they are never synthesized.
- Route pointer and manifest IDs must be bounded, path-safe identifiers. A
  missing pointer is `not_evaluated`, not a zero-route success.
- Every fixed candidate and referenced artifact has a family-specific byte
  limit. Symlinks, directories, devices, FIFOs, candidate conflicts, and files
  that change while captured fail closed.

## Current versioned baseline and known blockers

The Git repository currently versions Event evidence, the curated Event CSV,
one bounded CEX lifecycle manifest, and a small revisioned market-lifecycle
review ledger. It does not version production daily OHLCV, TVL, depth,
execution, route, or SQLite runtime facts. A snapshot produced from a clean
checkout therefore evaluates only the tracked Event and lifecycle families;
all other families are explicitly `not_evaluated`.

This task does not add authenticated or unauthenticated connectors, expand the
canary, or implement USDT/USD conversion evidence. Those remain separate
blockers. No production deployment or `main` merge is part of this contract.
