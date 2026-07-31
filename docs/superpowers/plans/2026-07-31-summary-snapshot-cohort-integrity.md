# Summary Performance and Snapshot Cohort Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce default Summary cold latency and prevent public depth/execution responses from crossing publication cohorts.

**Architecture:** Keep the existing source-signature and Fact contracts. Replace repeated whole-payload copies with a copy-on-overlay boundary, prewarm the default serialized Summary, failure-atomically publish each full depth/execution family, and validate exact family lineage at read time and release time.

**Tech Stack:** Python 3.8+, standard library HTTP/CSV/SQLite, `unittest`, existing publication and quality helpers.

## Global Constraints

- Never describe cross-venue or cross-chain observations as simultaneous.
- Preserve nulls; never convert unavailable Facts to zero.
- Preserve `data_generation`, freshness-bucket, and source-fence semantics.
- Full publication is failure-atomic for ordinary I/O errors, not crash-atomic.
- Funding Rate and all-in fee/gas/transfer-cost Facts are out of scope.
- Every commit uses an explicit message and receives a GitHub commit comment.

---

### Task 1: Copy-on-overlay Summary optimization

**Files:**
- Modify: `dashboard/server.py`
- Test: `tests/test_dashboard.py`

**Interfaces:**
- Produces: `_copy_payload_for_overlay(payload: dict[str, Any]) -> dict[str, Any]`.
- Consumed by: `overlay_tvl_snapshot`, `overlay_cex_depth_snapshot`, and `overlay_dex_depth_snapshot`.

- [ ] **Step 1: Write the failing immutability test**

Create a payload with nested `price_points`, call the missing helper, then assert:

```python
self.assertEqual(result, payload)
self.assertIsNot(result, payload)
self.assertIsNot(result["metadata"], payload["metadata"])
self.assertIsNot(result["cex_markets"][0], payload["cex_markets"][0])
self.assertIs(
    result["cex_markets"][0]["price_points"],
    payload["cex_markets"][0]["price_points"],
)
```

Mutate cloned metadata and top-level row fields and prove the source remains
unchanged.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
python3 -m unittest tests.test_dashboard.MarketMonitorServerTest.test_overlay_copy_shares_only_read_only_daily_series
```

Expected: `AttributeError` because `_copy_payload_for_overlay` does not exist.

- [ ] **Step 3: Implement the minimal copy boundary**

Deep-copy only metadata, shallow-copy every market row, and retain all other
top-level immutable values. Replace all three `copy.deepcopy(payload)` calls.

- [ ] **Step 4: Run overlay and dashboard tests**

Run:

```bash
python3 -m unittest tests.test_dashboard tests.test_market_facts
```

Expected: all tests pass and existing overlay golden values remain unchanged.

---

### Task 2: Default Summary startup warmup

**Files:**
- Modify: `dashboard/server.py`
- Test: `tests/test_dashboard.py`

**Interfaces:**
- Produces: `warm_default_market_summary() -> None`.
- Consumes: `_build_public_api_response_cached`, `api_source_signature`, and `api_freshness_bucket`.

- [ ] **Step 1: Write failing success/failure-isolation tests**

Patch the serialized response builder and assert one `summary` call with empty
query items and the current signature/bucket. Patch it to raise and assert the
startup wrapper logs a bounded warning without preventing server startup.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_dashboard -k warm_default_market_summary
```

Expected: failure because the warmup helper is absent.

- [ ] **Step 3: Implement warmup and startup integration**

Build the default serialized Summary once after binding the socket and before
`serve_forever()`. Catch `Exception`, print only its exception class in the
bounded warning, and continue so `/health` remains diagnosable.

- [ ] **Step 4: Verify cache/freshness regressions**

Run:

```bash
python3 -m unittest tests.test_dashboard tests.test_freshness
```

Expected: all tests pass; the 60-second freshness key remains unchanged.

---

### Task 3: Full family failure-atomic publication

**Files:**
- Modify: `scripts/fetch_cex_depth.py`
- Modify: `scripts/fetch_dex_depth.py`
- Test: `tests/test_fetch_cex_depth.py`
- Test: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Produces in both collectors: `publish_full_publication_bundle(depth_rows, execution_rows, *, output_dir, publish_dir, preflight_reports) -> tuple[dict[str, Any], dict[str, Any]]`.
- Consumes: existing coverage validators, lineage validators, CSV payload helpers, and `atomic_replace_bundle`.

- [ ] **Step 1: Write CEX and DEX fault-injection tests**

Prepare valid full candidates and four pre-existing public destinations.
Inject `OSError` at each replacement position and assert every public file
retains its original bytes.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_fetch_cex_depth tests.test_fetch_dex_depth -k full_publication_bundle
```

Expected: import or attribute failure because the full bundle function is absent.

- [ ] **Step 3: Implement full bundle functions**

Validate aligned depth/execution lineage, scenario completeness, and both
standard coverage reports before preparing history/latest/current payloads.
Write private processed current files independently, then publish all public
destinations through one `atomic_replace_bundle` call.

- [ ] **Step 4: Route unfiltered main publication through the bundle**

Keep exact refresh on `publish_exact_publication_bundle`; replace only the full
publish path. Preserve result JSON fields used by collection-cycle tests.

- [ ] **Step 5: Run collector suites**

Run:

```bash
python3 -m unittest tests.test_atomic_publication tests.test_fetch_cex_depth tests.test_fetch_dex_depth tests.test_collection_cycle
```

Expected: all tests pass.

---

### Task 4: Read-time and release-time cohort lineage guard

**Files:**
- Modify: `dashboard/server.py`
- Modify: `scripts/check_dashboard_release.py`
- Test: `tests/test_dashboard.py`
- Test: `tests/test_release_smoke.py`

**Interfaces:**
- Produces: `validate_depth_execution_cohort(metadata, snapshot, market_type) -> dict[str, Any]`.
- Produces metadata fields: `observation_span_seconds` and `cohort_lineage`.

- [ ] **Step 1: Write matching and mismatch tests**

Cover wrong execution `snapshot_id`, wrong `source_snapshot_id`, multiple IDs,
and market-count mismatch. Matching input returns one bounded projection;
mismatch raises a `RuntimeError` subclass and public handlers return 503.

- [ ] **Step 2: Write release-check counterexamples**

Pass full-catalog depth metadata into `validate_execution`. Mutate each lineage
field while leaving generation unchanged and require `ReleaseCheckError`.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_dashboard tests.test_release_smoke -k cohort
```

- [ ] **Step 4: Implement metadata and guards**

Compute nonnegative spans from canonical timestamp bounds. Validate one exact
depth/execution/source snapshot ID and equal inventory counts before returning
execution or selected-quality Facts. Include the validated projection in the
execution response and independently compare it with full-catalog metadata in
the release checker.

- [ ] **Step 5: Run server/release tests**

Run:

```bash
python3 -m unittest tests.test_dashboard tests.test_release_smoke tests.test_public_quality_overlay
```

Expected: all tests pass.

---

### Task 5: Documentation, complete verification, and release

**Files:**
- Modify: `docs/cex-depth-data-contract.md`
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `docs/collection-operations.md`
- Modify: `docs/market-facts-contract.md`
- Test: all tests

**Interfaces:**
- Documents exact meanings of `snapshot_id`, cohort span, failure atomicity,
  reader fail-closed behavior, and non-simultaneity.

- [ ] **Step 1: Update contracts without overclaiming**

State explicitly that family rows are bounded sequential observations and that
the bundle handles ordinary I/O rollback but not process-crash atomicity.

- [ ] **Step 2: Run full verification**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
git diff --check
```

Then run Python 3.8 compile/import checks in the production-compatible
preflight environment.

- [ ] **Step 3: Benchmark**

Measure a fresh-process default Summary before serving and the immediate warm
call. Record latency, response bytes, and generation equality. Do not encode a
machine-dependent latency threshold as a unit test.

- [ ] **Step 4: Commit and comment**

```bash
git add dashboard scripts tests docs
git commit -m "perf(data): harden snapshot cohorts and warm summary"
git push origin codex/critical-quality-sorting-token-refresh
```

Add a GitHub commit comment with test count, benchmark, atomicity boundary, and
lineage counterexamples.

- [ ] **Step 5: Preflight, deploy, and verify**

Use an isolated production worktree/service, run the full release checker on a
stable generation, cut over the main service, rerun the checker, verify public
desktop/mobile behavior, and remove the preflight unit/worktree.
