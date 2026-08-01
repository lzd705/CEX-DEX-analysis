# Route Cohort Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a deterministic bounded route universe, deadline-aware parallel leg observations, exact ≤60-second timing classification, route-level failure isolation, and immutable cohort publication.

**Architecture:** Existing full depth/execution collectors remain authoritative for their own families. New pure route modules select a small candidate universe, reuse refactored one-leg collectors, and publish source/timing evidence as an immutable core cohort. Costs, opportunity ranking, public API/UI, and timers are enabled only by later plans.

**Tech Stack:** Python 3.8-compatible standard library, Decimal timestamp arithmetic, `concurrent.futures`, CSV/SQLite/JSON, existing atomic-publication patterns, `unittest`.

## Global Constraints

- `60.000000` seconds passes; every value greater than 60 seconds fails without float rounding.
- `snapshot_id` is lineage, never proof of synchronized observation.
- Expected route failures do not suppress healthy routes; structural corruption rejects the bundle.
- Concurrent completion order cannot change output order or fingerprints.
- This increment publishes timing/source facts only and must not use the word executable for a result.
- Daily Price Gap, existing depth/execution, Market A/B, systemd timers, and Upbit facts remain unchanged.

---

### Task 1: Exact timestamps and route identity

**Files:**
- Modify: `scripts/timestamp_contract.py`
- Create: `scripts/route_cohort.py`
- Create: `tests/test_route_cohort.py`
- Create: `docs/route-cohort-data-contract.md`

**Interfaces:**
- Produces: `exact_rfc3339_epoch_seconds(value) -> Decimal` and `exact_timestamp_skew_seconds(left, right) -> Decimal`.
- Produces: `canonical_route_id()`, `classify_route_timing()`, and `validate_route_cohort_rows()`.

- [ ] **Step 1: Write failing exact-boundary tests**

```python
self.assertEqual(
    exact_timestamp_skew_seconds(
        "2026-08-01T12:00:00.000000000Z",
        "2026-08-01T12:01:00.000000000Z",
    ),
    Decimal("60.000000000"),
)
self.assertEqual(
    classify_route_timing(candidate, buy_leg, sell_leg)["timing_status"],
    "within_sla",
)
```

Add `60.000000001`, timezone-offset equivalence, missing/naive/malformed/future
timestamps, directional stable IDs, duplicate leg/candidate, and stable reason
priority tests.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_route_cohort -v`

Expected: FAIL because route and exact timestamp helpers are absent.

- [ ] **Step 3: Implement strict parsing and classification**

Use stable reason priority:

```text
route_deadline_exceeded
execution_adapter_unsupported
buy_leg_unavailable
sell_leg_unavailable
invalid_state_timestamp
snapshot_skew_exceeded
route_mode_not_executable
```

Return exact decimal text or null; never expose raw exception strings.

- [ ] **Step 4: Run tests and documentation checks**

Run: `python3 -m unittest tests.test_route_cohort -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/timestamp_contract.py scripts/route_cohort.py tests/test_route_cohort.py docs/route-cohort-data-contract.md
git commit -m "feat(routes): define exact route cohort contract"
```

Add a GitHub commit comment with the exact 60-second boundary evidence.

### Task 2: Deterministic bounded route universe

**Files:**
- Create: `scripts/route_universe.py`
- Create: `tests/test_route_universe.py`
- Modify: `docs/route-cohort-data-contract.md`

**Interfaces:**
- Consumes: canonical catalog/depth/execution/volume/TVL inputs.
- Produces: `execution_capability_by_market()`, `select_route_legs()`, `build_route_universe()`, and `route_universe_sha256()`.

- [ ] **Step 1: Write failing bounded-selection tests**

Assert at most three CEX and three DEX legs per Token; exclude lifecycle-
withheld, missing-book, unsupported-execution, and invalid-time rows; retain all
priority inputs and source generation.

- [ ] **Step 2: Add determinism and direction tests**

Shuffle every input repeatedly and assert identical selected legs, directed
routes, five-notional grids, JSON bytes, and SHA-256. Canonical market ID is
the last tie-breaker. Cross-chain DEX routes remain research-only with an
explicit settlement reason.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_universe -v`

Expected: FAIL because the universe builder is absent.

- [ ] **Step 4: Implement the exact priority key**

```text
execution capability
→ proved execution capacity / observed 100-bps depth
→ CEX selected-window USD volume
→ DEX 24-hour USD volume
→ DEX TVL
→ canonical market_id
```

Store selection rank, inputs, window, candidate source generation, and
canonical IDs in the universe.

- [ ] **Step 5: Run tests**

Run: `python3 -m unittest tests.test_route_universe -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_universe.py tests/test_route_universe.py docs/route-cohort-data-contract.md
git commit -m "feat(routes): build deterministic route universe"
```

Add a GitHub commit comment with determinism and selection evidence.

### Task 3: Deadline-aware one-leg collectors

**Files:**
- Create: `scripts/collection_deadline.py`
- Modify: `scripts/fetch_cex_depth.py`
- Modify: `scripts/fetch_dex_depth.py`
- Create: `tests/test_route_collection.py`
- Modify: `tests/test_fetch_cex_depth.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Produces: `CollectionDeadline.for_duration()`, `remaining_seconds()`, `request_timeout()`, `sleep_before_retry()`, and `require_remaining()`.
- Produces: `collect_cex_market_observation()` and `collect_dex_pool_observation()`.
- Preserves: complete existing full-collector rows and hashes.

- [ ] **Step 1: Write failing deadline tests**

Test an expired deadline performs no request, request timeout never exceeds
remaining time, retry sleep is clamped, and exhaustion raises one stable
deadline exception.

- [ ] **Step 2: Write failing extraction-equivalence tests**

Given the same fixture, assert one-leg CEX/DEX primitives produce the same
depth and ten execution rows as the old full collector. A supplied DEX fixed
block must be the only block queried; separate pool clients cannot share RPC
IDs or transcript state.

- [ ] **Step 3: Verify RED and old baseline**

Run: `python3 -m unittest tests.test_route_collection tests.test_fetch_cex_depth tests.test_fetch_dex_depth -v`

Expected: new tests FAIL and prior collector tests PASS.

- [ ] **Step 4: Implement deadline-aware request parameters**

Add optional `deadline`, `timeout_seconds`, and `max_retries` parameters to CEX
HTTP and DEX RPC requests. Do not use Python 3.9-only executor options.

- [ ] **Step 5: Extract one-leg primitives and retain sequential callers**

The current full collectors call the new primitives sequentially, preserving
their public contract and publication behavior.

- [ ] **Step 6: Run focused regressions**

Run: `python3 -m unittest tests.test_route_collection tests.test_fetch_cex_depth tests.test_fetch_dex_depth -v`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/collection_deadline.py scripts/fetch_cex_depth.py scripts/fetch_dex_depth.py tests/test_route_collection.py tests/test_fetch_cex_depth.py tests/test_fetch_dex_depth.py
git commit -m "refactor(data): expose deadline-aware route leg collectors"
```

Add a GitHub commit comment with legacy-output equivalence evidence.

### Task 4: Concurrent cohort orchestration

**Files:**
- Create: `scripts/collect_route_cohort.py`
- Modify: `tests/test_route_collection.py`
- Modify: `docs/route-cohort-data-contract.md`

**Interfaces:**
- Consumes: universe and one-leg primitives from Tasks 2–3.
- Produces: `collect_unique_route_legs()`, `materialize_route_leg_rows()`, and `collect_route_cohort()`.

- [ ] **Step 1: Write failing concurrency and isolation tests**

Verify unique legs begin around one target, per-venue/per-chain limits hold,
same-chain pools share one fixed block, and one timeout affects only routes
that contain that leg.

- [ ] **Step 2: Write deterministic completion-order test**

Resolve fake futures in opposite orders and assert identical normalized rows
and fingerprint. Recompute source generation before publication and reject a
mid-run change.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_collection -v`

Expected: FAIL because the orchestrator is absent.

- [ ] **Step 4: Implement bounded executors and CLI**

Support:

```text
--data-dir --start --end --tokens --deadline-seconds 60
--max-workers 24 --cex-workers-per-venue 2 --dex-workers-per-chain 4
--dry-run --publish
```

`--dry-run` builds/validates the universe without network or publication.
Deadline-incomplete legs publish terminal rows; they never disappear.

- [ ] **Step 5: Run collection tests**

Run: `python3 -m unittest tests.test_route_collection -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/collect_route_cohort.py tests/test_route_collection.py docs/route-cohort-data-contract.md
git commit -m "feat(routes): collect synchronized route leg cohorts"
```

Add a GitHub commit comment with concurrency-limit and isolation evidence.

### Task 5: Immutable core bundle and pointer

**Files:**
- Create: `scripts/route_publication.py`
- Create: `tests/test_route_publication.py`
- Modify: `docs/route-cohort-data-contract.md`

**Interfaces:**
- Consumes: normalized cohort/candidate/leg/timing rows.
- Produces: `build_route_cohort_sqlite()`, `validate_route_cohort_bundle()`,
  `publish_route_cohort_bundle()`, and `load_latest_route_cohort()` through the
  core-only pointer `data/local/routes/core/latest.json`.

- [ ] **Step 1: Write failing deterministic publication tests**

Shuffled rows must produce identical CSV logical fingerprints, deterministic
SQLite content fingerprints, and manifest hashes. CSV/SQLite route inventories
must match exactly.

- [ ] **Step 2: Write failure-atomic and path-safety tests**

Test duplicate identity, enum drift, incomplete route pair, tampered hash,
lineage conflict, existing immutable ID, pointer replacement failure, symlink,
and path traversal. A pointer failure preserves the old pointer.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_publication -v`

Expected: FAIL because publication helpers are absent.

- [ ] **Step 4: Implement staged immutable publication**

Write to a hidden same-filesystem directory below
`data/local/routes/core/bundles/`, validate and fsync all files, rename once to
`<route_cohort_id>/`, fully reread, then atomically replace
`data/local/routes/core/latest.json`. Never overwrite an existing cohort. This task must not
create or replace the public complete pointer `latest.json`.

This increment uses `bundle_stage=route_cohort_core/v1` and does not invent
empty cost/opportunity files. The later cost plan creates a new complete
immutable bundle rather than mutating this one.

- [ ] **Step 5: Run publication tests**

Run: `python3 -m unittest tests.test_route_publication -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_publication.py tests/test_route_publication.py docs/route-cohort-data-contract.md
git commit -m "feat(routes): publish immutable route cohort bundles"
```

Add a GitHub commit comment with fault-injection evidence.

### Task 6: Manual collection profile and optional release validation

**Files:**
- Modify: `scripts/run_collection_cycle.py`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/collection-operations.md`
- Modify: `tests/test_collection_cycle.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_framework.py`

**Interfaces:**
- Consumes: route CLI and pointer from Tasks 4–5.
- Produces: manual `routes` profile and release option `--require-route-cohort`.

- [ ] **Step 1: Write failing profile tests**

Assert the route profile creates one bounded command, forwards token/window,
deadline and publish scope, reports timing-status counts, and leaves the
existing depth profile order unchanged.

- [ ] **Step 2: Write release validation counterexamples**

The core pointer absent remains optional. If present, hash, lineage, exact Decimal SLA,
and status/reason must validate. Normal route-level unavailable rows do not
make the bundle corrupt. `--require-route-cohort` fails when no valid pointer
exists.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_collection_cycle tests.test_release_smoke tests.test_framework -v`

Expected: FAIL on missing route profile and checker.

- [ ] **Step 4: Implement manual profile and checker**

Add `PROFILE_STEPS["routes"] = ("routes",)`. Do not add it to hourly depth or
systemd timers until the complete opportunity pipeline is ready.

- [ ] **Step 5: Run focused and full suites**

Run: `python3 -m unittest tests.test_collection_cycle tests.test_release_smoke tests.test_framework -v`

Then: `python3 -m unittest discover -s tests -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_collection_cycle.py scripts/check_dashboard_release.py docs/collection-operations.md tests/test_collection_cycle.py tests/test_release_smoke.py tests/test_framework.py
git commit -m "feat(ops): integrate route cohort collection"
```

Add a GitHub commit comment with full-suite and Python 3.8 evidence.

### Task 7: Bounded live preflight without public UI

**Files:**
- No source changes unless a tested defect is found.

**Interfaces:**
- Consumes: complete route core.
- Produces: audit evidence only; no timer or public API/UI.

- [ ] **Step 1: Run AAVE dry-run**

Run the route profile with `--dry-run --tokens AAVE`; verify no network raw
directory or pointer is created.

- [ ] **Step 2: Run one-Token live candidate without pointer cutover**

Validate raw hashes, actual leg timestamps, exact skew, fixed block reuse, and
deterministic offline rebuild.

- [ ] **Step 3: Publish one-Token `core/latest.json` pointer only after validation**

Run the strict checker with `--require-route-cohort`; confirm the dashboard
continues to ignore this core pointer until the API plan is implemented.

- [ ] **Step 4: Record evidence in the implementation plan**

Record exact command, cohort ID, start/end/deadline, leg/route counts,
within-SLA/unavailable counts, hashes, test total, and no-public-change result.
