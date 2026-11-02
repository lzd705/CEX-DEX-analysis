# Data Quality Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make quality classifications, Screener drill-downs, retry outcomes, and collection evidence exact and auditable, then perform one conditional MORPHO recovery without adding Funding Rate or fabricating facts.

**Architecture:** Preserve the existing fact builders and add small pure contract helpers at their evidence boundaries. The server will expose both selected-window and Screener quality projections in contract v4, while administrator refreshes will prove a new exact-Market publication before reporting success. Daily attempt evidence will be matched and validated by full canonical identity.

**Tech Stack:** Python 3.8-compatible standard library, SQLite, CSV/JSON publication bundles, `unittest`, vanilla JavaScript, existing dashboard HTTP server and release checker.

## Global Constraints

- Funding Rate and derivatives Markets are excluded; do not add schemas, UI, collectors, placeholders, or navigation for them.
- Preserve missing, failed, unsupported, and not-applicable values as `null`/`N/A`; measured zero remains zero.
- Overall Market `quality_status` is derived only from `data_health`; capability, availability, measurement limits, and market conditions remain separate reasons.
- Public reason codes are bounded and allowlisted; raw collector exceptions and filesystem paths are not public reason codes.
- Refresh identities remain canonical: `cex:<venue>:<instrument>` and `dex:<chain>:<dex>:<pool>:<TOKEN>`.
- Production runtime compatibility floor is Python 3.8.10; avoid new dependencies and newer-only syntax.
- Every production behavior change begins with a failing test and follows red-green-refactor.
- Every commit has an explicit message. Every push receives an explicit GitHub commit comment.
- No deployment claim is made until local tests, production preflight, release checks, browser QA, and public health verification pass.
- MORPHO receives at most one bounded recovery job, and only if the freshly deployed backend still marks the exact Fact retryable.

---

## File Responsibility Map

- `dashboard/server.py`: build quality facts, normalize public reason/status fields, share Screener alert projection, and emit quality contract v4.
- `dashboard/market_facts.py`: preserve the DEX adapter's explicit USD-alignment requirement in the Market catalog.
- `dashboard/static/app.js`: choose selected-window or Screener quality projection based on route origin.
- `scripts/quality_outcomes.py`: shared status/reason matrix and bounded source-outcome normalizers used by generation, public projection, and administrator postchecks.
- `dashboard/snapshot_refresh.py`: pure readers and postcondition evaluator for exact TVL/depth snapshot Facts.
- `dashboard/admin.py`: run collectors and set job success only from postcondition evidence.
- `scripts/fact_quality.py`: validate collection-attempt ledgers and match CEX attempts by exact instrument.
- `scripts/fetch_cex.py`, `scripts/fetch_dex.py`, `scripts/run_fact_pipeline.py`: produce unique exact-identity attempt evidence and preserve validated source-instrument aliases.
- `scripts/check_dashboard_release.py`: enforce contract v4 and 30/30 Screener drill-down parity.
- `docs/market-facts-contract.md`, `docs/admin-operations.md`, `docs/market-monitor-design.md`: publish the final semantics and operator procedure.
- Focused tests remain beside their existing domains under `tests/`.

---

### Task 1: Correct Depth Quality Semantics

**Files:**
- Create: `scripts/quality_outcomes.py`
- Create: `tests/test_quality_outcomes.py`
- Modify: `dashboard/server.py:_depth_quality_fact`
- Modify: `dashboard/server.py:_execution_quality_fact`
- Modify: `dashboard/server.py:build_market_quality`
- Modify: `dashboard/market_facts.py:catalog_from_market_payload`
- Modify: `tests/test_cex_depth_quality_reasons.py`
- Modify: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: catalog Market dictionaries containing `depth_status`, depth-band values, temporal-alignment fields, `depth_reason_code`, and `depth_error`.
- Produces: public depth/execution Facts whose `status`, bounded `reason_code`, `retryable`, and `quality_flags` describe the same outcome.
- Produces: one exact allowlisted status/reason matrix; unknown combinations fail closed.

- [ ] **Step 1: Add failing tests for unsupported DEX depth and legacy empty books**

Create `tests/test_quality_outcomes.py` with a table-driven exact contract:

```python
import unittest

from scripts.quality_outcomes import quality_outcome_rule


class QualityOutcomeRuleTest(unittest.TestCase):
    def test_allowlisted_outcomes_have_exact_resolution_semantics(self):
        cases = {
            ("observed", "observed"): (False, True, "observed"),
            ("partial", "source_level_limit"): (False, True, "partial"),
            ("source_no_observation", "no_candles"): (
                False, True, "confirmed_absence"
            ),
            ("source_no_observation", "source_no_two_sided_book"): (
                False, True, "confirmed_absence"
            ),
            ("unsupported", "unsupported_chain"): (
                False, True, "confirmed_unsupported"
            ),
            ("needs_review", "not_listed"): (
                False, False, "manual_review"
            ),
            ("needs_review", "daily_quality_outcome_invalid"): (
                False, False, "manual_review"
            ),
            ("collection_failed", "network"): (
                True, False, "retry_open"
            ),
            ("backfill_pending", "missing_unexplained"): (
                True, False, "retry_open"
            ),
            ("invalid", "invalid_positive_ohlc"): (
                False, False, "blocked_invalid"
            ),
        }
        for pair, expected in cases.items():
            with self.subTest(pair=pair):
                rule = quality_outcome_rule(*pair)
                self.assertIsNotNone(rule)
                self.assertEqual(
                    (rule.retryable, rule.terminal, rule.resolution),
                    expected,
                )

    def test_unknown_status_reason_pairs_fail_closed(self):
        for pair in (
            ("observed", "unknown"),
            ("partial", "unknown"),
            ("needs_review", "source_range_unavailable"),
            ("unsupported", "network"),
        ):
            with self.subTest(pair=pair):
                self.assertIsNone(quality_outcome_rule(*pair))
```

Add these behaviors to `tests/test_cex_depth_quality_reasons.py`:

```python
def test_unsupported_dex_depth_does_not_invent_temporal_mismatch(self):
    fact = server._depth_quality_fact({
        "market_type": "dex",
        "depth_status": "unsupported",
        "depth_error": "unsupported_chain:solana",
        "depth_usd_price_freshness_status": "unavailable",
        "total_depth_10bps_usd": None,
        "total_depth_25bps_usd": None,
        "total_depth_50bps_usd": None,
        "total_depth_100bps_usd": None,
    })
    self.assertEqual(fact["status"], "unsupported")
    self.assertNotIn(
        "depth_usd_price_time_mismatch",
        {flag["code"] for flag in fact["quality_flags"]},
    )

def test_legacy_empty_book_is_projected_as_source_no_observation(self):
    fact = server._depth_quality_fact({
        "market_type": "cex",
        "depth_status": "failed",
        "depth_reason_code": (
            "SourceBookError: crypto_com returned an empty order-book side"
        ),
        "depth_error": (
            "SourceBookError: crypto_com returned an empty order-book side"
        ),
    })
    self.assertEqual(fact["status"], "source_no_observation")
    self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
    self.assertFalse(fact["retryable"])
    self.assertNotIn(
        "depth_failed",
        {flag["code"] for flag in fact["quality_flags"]},
    )

def test_empty_book_execution_is_same_source_outcome(self):
    fact = execution_fact_for_status_reason(
        status="failed",
        reason_code="source_no_two_sided_book",
    )
    self.assertEqual(fact["status"], "source_no_observation")
    self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
    self.assertFalse(fact["retryable"])
    self.assertFalse(any(
        flag["category"] == "data_health"
        for flag in fact["quality_flags"]
    ))
```

Add a measured-stale guard test to `tests/test_dashboard.py` using an observed
DEX row with finite `total_depth_100bps_usd`,
`depth_usd_price_freshness_status="stale"`, and
`depth_requires_usd_price_alignment=True`; assert the critical mismatch flag
still appears. Add matching cases for `warning`, and for a measured DEX row with
`depth_requires_usd_price_alignment=False`; the latter must receive neither
temporal flag.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_quality_outcomes \
  tests.test_cex_depth_quality_reasons \
  tests.test_dashboard.DashboardApiTest.test_dex_depth_quality_rejects_stale_usd_alignment
```

Expected: the matrix import fails, unsupported rows receive a temporal mismatch,
raw empty-book depth/execution remain `failed`, and measured rows do not yet
respect the adapter's alignment declaration.

- [ ] **Step 3: Implement the shared matrix, measured-only timing, and bounded source normalization**

Create `scripts/quality_outcomes.py` with Python 3.8-compatible frozen
dataclasses. Enumerate exact allowlisted rules for:

- `observed/observed`;
- `partial/source_level_limit` and `partial/measurement_limit`;
- retryable `collection_failed` reasons `network`, `rate_limit`,
  `source_unavailable`, `parse`, and `validation`;
- `source_no_observation` reasons `no_candles`,
  `source_no_two_sided_book`, and `source_no_order_book`;
- `unsupported` reasons `source_range_unavailable`, `unsupported_chain`,
  `unsupported_protocol`, `unsupported_method`, `unsupported_source`, and
  `unsupported_protocol_or_chain`;
- protected `needs_review/not_listed` and
  `needs_review/stale_market_lifecycle_unknown`, plus the bounded operator
  outcomes `needs_review/source_rejected_request` and
  `needs_review/daily_quality_outcome_invalid`;
- retryable `backfill_pending/missing_unexplained`;
- the existing explicit `invalid_*` daily contract reason codes and
  `invalid/source_invalid_order_book`.

Unknown status/reason pairs return `None`. Add pure bounded helpers that project
legacy CEX errors and DEX unsupported-error prefixes onto these canonical pairs;
the public `reason_code` must never be a raw exception or a dynamic
`prefix:value` string.

In `dashboard/server.py`, make `cex_depth_reason_code()` fall through to legacy
error classification when a supplied reason code is not allowlisted. Then make
`_depth_quality_fact()` derive a public status, canonical reason, and measured
predicate:

```python
raw_status = str(market.get("depth_status") or "unavailable").lower()
public_status = raw_status
if market_type == "cex":
    reason_code = cex_depth_reason_code({
        "status": raw_status,
        "reason_code": market.get("depth_reason_code"),
        "error": market.get("depth_error"),
    })
    if reason_code in {"source_no_two_sided_book", "source_no_order_book"}:
        public_status = "source_no_observation"

measured = (
    market_type == "dex"
    and raw_status in {"observed", "partial", "complete"}
    and any(
        parse_number(market.get("total_depth_{}bps_usd".format(band)))
        is not None
        for band in (10, 25, 50, 100)
    )
)
alignment_applicable = (
    measured and bool(market.get("depth_requires_usd_price_alignment"))
)
if alignment_applicable and timing_status in {"stale", "unavailable"}:
    quality_flags.append(depth_time_mismatch_flag)
```

In `overlay_dex_depth_snapshot()` and `catalog_from_market_payload()`, preserve
an explicit `depth_requires_usd_price_alignment` boolean declared by the DEX
collector contract; no snapshot or a non-converting adapter yields `False`.

Apply the same `normalize_cex_source_outcome()` helper to both depth and
execution. For `source_no_observation`, remove inherited `depth_failed` or
execution failure data-health flags and add one bounded informational source
outcome flag. In `build_market_quality()`, do not re-add a catalog failure flag
after either normalized Fact has become `source_no_observation`.

The execution regression helper writes a real validated execution snapshot and
calls the public quality builder; it does not assert against a mocked builder.

- [ ] **Step 4: Run focused and adjacent quality tests and verify GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_quality_outcomes \
  tests.test_cex_depth_quality_reasons \
  tests.test_dashboard \
  tests.test_public_quality_overlay
```

Expected: all selected tests pass; measured stale/warning timing remains active,
unsupported depth has only capability information, and empty books are not
retryable.

- [ ] **Step 5: Commit Task 1**

```bash
git add scripts/quality_outcomes.py dashboard/server.py dashboard/market_facts.py \
  tests/test_quality_outcomes.py tests/test_cex_depth_quality_reasons.py \
  tests/test_dashboard.py
git commit -m "fix(quality): separate source outcomes from data failures"
```

---

### Task 2: Add Quality Contract v4 and Exact Screener Drill-Down

**Files:**
- Modify: `dashboard/server.py:QUALITY_CONTRACT_VERSION`
- Modify: `dashboard/server.py:build_market_quality`
- Modify: `dashboard/static/app.js:renderQualityPayload`
- Modify: `tests/test_public_quality_overlay.py`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_navigation.py`

**Interfaces:**
- Consumes: catalog `quality_status`/`quality_flag_details` and selected-window Fact quality.
- Produces: selected-window `quality_status`/`quality_flags` plus same-generation
  `screening_quality_status`/`screening_quality_flags` for every Market, with
  the exact `metadata.data_generation` used by Summary and the Token catalog.

- [ ] **Step 1: Add failing backend contract and parity tests**

In `tests/test_public_quality_overlay.py`, assert both projections exist and can
differ without overwriting each other:

```python
payload, market = self.quality_for_day("2026-01-02")
self.assertEqual(payload["metadata"]["contract_version"], 4)
self.assertEqual(
    payload["metadata"]["data_generation"],
    self.token_catalog_generation_for_day("2026-01-02"),
)
self.assertIn("screening_quality_status", market)
self.assertIn("screening_quality_flags", market)
self.assertEqual(market["quality_status"], "critical")
self.assertEqual(market["screening_quality_status"], "warning")
self.assertEqual(
    [flag["code"] for flag in market["screening_quality_flags"]],
    ["depth_unavailable", "low_daily_coverage"],
)
```

`token_catalog_generation_for_day()` calls the real Token-catalog builder under
the same frozen fixture files; it is not a copied constant or mocked response.

Add a deterministic parity test in `tests/test_dashboard.py`:

```python
summary = server.build_market_summary(start="2026-01-01", end="2026-01-08")
for token_row in summary["tokens"]:
    quality = server.build_market_quality(
        token_row["token_symbol"],
        start="2026-01-01",
        end="2026-01-08",
    )
    screening_statuses = Counter(
        market["screening_quality_status"] for market in quality["markets"]
    )
    screening_alerts = Counter(
        flag["severity"]
        for market in quality["markets"]
        for flag in market["screening_quality_flags"]
    )
    self.assertEqual(dict(screening_statuses), token_row["quality_status_counts"])
    self.assertEqual(dict(screening_alerts), token_row["quality_alert_counts"])
```

Add a catalog edge fixture with `quality_status="warning"` and an empty
`quality_flag_details` list. Both Summary and Data Quality must project exactly
one bounded fallback warning flag; this prevents a non-OK Market from producing
different alert counts at the two endpoints.

- [ ] **Step 2: Add a failing frontend origin-switch test**

In `tests/test_dashboard_frontend.py`, run `renderQualityPayload()` with one
Market whose selected-window status is `ok` and screening status is `warning`.
Set `app.qualityOrigin="screener"` and `app.qualitySeverity="warning"`; assert
one row and the screening reason are rendered. Clear the origin; assert the
selected-window view does not display that screening-only reason.

- [ ] **Step 3: Run contract tests and verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_public_quality_overlay \
  tests.test_dashboard_frontend \
  tests.test_navigation
```

Expected: failures identify contract version 3, missing screening fields, and
frontend filtering that still reads selected-window fields for Screener links.

- [ ] **Step 4: Implement the dual projection**

Set `QUALITY_CONTRACT_VERSION = 4`. Add one pure
`screening_quality_projection(market)` helper that sanitizes allowlisted
catalog flags and, only when the list is empty but status is
`info`/`warning`/`critical`, returns one bounded fallback flag at that status.
Use this same helper in both `catalog_summary_from_catalog()` and
`build_market_quality()` so the two contracts cannot drift. Preserve the
projection before building selected-window facts:

```python
screening = screening_quality_projection(market)
quality_markets.append({
    # existing identity and selected-window fields
    "quality_status": quality_status,
    "quality_flags": quality_flags,
    "screening_quality_status": screening["status"],
    "screening_quality_flags": screening["flags"],
    "facts": facts,
})
```

Copy the already-validated `catalog["metadata"]["data_generation"]` into the
quality response metadata. Do not recompute it after building Markets; Summary,
Token catalog, and Data Quality must expose the generation they actually used.

Do not add a public source-path object. The test fixture should retain its
expected catalog fields before the response projection instead of exposing a
new `screening_quality_source` field in production.

In `dashboard/static/app.js`, select fields by route origin:

```javascript
function qualityProjection(item) {
  if (app.qualityOrigin === "screener") {
    return {
      status: item.screening_quality_status || "ok",
      flags: Array.isArray(item.screening_quality_flags)
        ? item.screening_quality_flags
        : [],
    };
  }
  return {
    status: item.quality_status || "ok",
    flags: Array.isArray(item.quality_flags) ? item.quality_flags : [],
  };
}
```

Use `qualityProjection()` for severity filtering, reason counts, open details,
and rendered reason groups.

- [ ] **Step 5: Run backend/frontend tests and verify GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard \
  tests.test_public_quality_overlay \
  tests.test_dashboard_frontend \
  tests.test_navigation
```

Expected: all tests pass and selected-window tests remain unchanged unless they
explicitly inspect the new screening fields.

- [ ] **Step 6: Commit Task 2**

```bash
git add dashboard/server.py dashboard/static/app.js \
  tests/test_public_quality_overlay.py tests/test_dashboard_frontend.py \
  tests/test_dashboard.py tests/test_navigation.py
git commit -m "feat(quality): preserve screener and window audit projections"
```

---

### Task 3: Enforce the Shared Matrix in Daily Quality and Retry Resolution

**Files:**
- Modify: `dashboard/server.py:DAILY_QUALITY_REASON_RULES`
- Modify: `dashboard/admin.py:_retry_resolution_evidence`
- Modify: `scripts/fact_quality.py:gap_evidence`
- Modify: `tests/test_public_quality_overlay.py`
- Modify: `tests/test_admin.py`
- Modify: `tests/test_fact_quality.py`

**Interfaces:**
- Consumes: `quality_outcome_rule(status, reason_code)` created in Task 1.
- Produces: the same status/retryability decision in quality generation,
  public projection, and administrator postchecks.

- [ ] **Step 1: Change existing expectations to expose contradictory rules**

In `tests/test_admin.py`, replace the current acceptance of
`needs_review/source_range_unavailable` with:

```python
error, evidence = service._retry_resolution_evidence(service.jobs["retry"])
self.assertIn("remain neither observed", error)
self.assertEqual(evidence["confirmed_absence_count"], 0)
self.assertEqual(evidence["unresolved_count"], 1)
```

Add table-driven tests in `tests/test_fact_quality.py` and
`tests/test_public_quality_overlay.py` proving every daily reason maps through
the shared matrix to the exact same status and retryability. Include
`needs_review/not_listed`, which is recognized but non-terminal, plus one
unknown pair that must fail closed rather than become confirmed absence.

- [ ] **Step 2: Run admin/daily tests and verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_quality_outcomes \
  tests.test_admin \
  tests.test_fact_quality \
  tests.test_public_quality_overlay
```

Expected: the admin test shows `needs_review` is accepted as absence and the
daily generation/projection paths still rely on independent rule tables.

- [ ] **Step 3: Replace independent decisions with the shared matrix**

Use `quality_outcome_rule()` in `_retry_resolution_evidence()`. Only a
recognized rule with `terminal=True` and resolution
`confirmed_absence`/`confirmed_unsupported` may enter `confirmed_absences`;
recognized `needs_review`, retryable, invalid, and unknown pairs remain
unresolved.

In `scripts/fact_quality.py`, derive gap status and retryability through the
same matrix rather than an independent reason switch. In `dashboard/server.py`,
derive daily report status/retryability through the matrix and reject any
status/reason disagreement as `needs_review/daily_quality_outcome_invalid`.
The bounded public fallback is non-retryable and cannot resolve an admin job.

- [ ] **Step 4: Run matrix, admin, and daily quality tests and verify GREEN**

Run the Step 2 command again. Expected: all selected tests pass; no
`needs_review` pair resolves and valid source-no-observation/unsupported pairs
remain terminal everywhere.

- [ ] **Step 5: Commit Task 3**

```bash
git add dashboard/server.py dashboard/admin.py scripts/fact_quality.py \
  tests/test_public_quality_overlay.py tests/test_admin.py \
  tests/test_fact_quality.py
git commit -m "fix(quality): enforce exact outcome resolution rules"
```

---

### Task 4: Require Exact Snapshot Refresh Postconditions

**Files:**
- Create: `dashboard/snapshot_refresh.py`
- Modify: `dashboard/admin.py:_run_snapshot_refresh_job`
- Modify: `tests/test_snapshot_fact_refresh.py`
- Modify: `tests/test_public_actions.py`

**Interfaces:**
- Produces: `read_snapshot_fact_state(data_dir, request) -> SnapshotFactState`.
- Produces: `evaluate_snapshot_refresh(before, after) -> SnapshotRefreshResult`.
- Consumes: canonical Token/Market/Fact request and latest published CSV bundle.

- [ ] **Step 1: Add failing pure postcondition tests**

Expand `tests/test_snapshot_fact_refresh.py` with deterministic state objects:

```python
from dashboard.snapshot_refresh import (
    SnapshotFactState,
    evaluate_snapshot_refresh,
)

def state(
    snapshot_id,
    dataset_sha256,
    status,
    reason_code,
    *,
    retryable=False,
    market_id="cex:upbit:MORPHO/USDT"
):
    return SnapshotFactState(
        market_id=market_id,
        fact_type="depth",
        snapshot_id=snapshot_id,
        dataset_sha256=dataset_sha256,
        observed_at="2026-07-31T12:00:00+00:00",
        status=status,
        reason_code=reason_code,
        retryable=retryable,
    )

class SnapshotPostconditionTest(unittest.TestCase):
    def test_unchanged_publication_is_not_success(self):
        before = state("s1", "a" * 64, "not_cataloged_in_snapshot", None)
        result = evaluate_snapshot_refresh(before, before)
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_publication_unchanged")

    def test_success_requires_a_new_nonempty_source_snapshot_id(self):
        cases = (
            (None, None, "snapshot_publication_identity_invalid"),
            (None, "", "snapshot_publication_identity_invalid"),
            ("s1", "s1", "snapshot_publication_unchanged"),
        )
        for before_id, after_id, expected_error in cases:
            result = evaluate_snapshot_refresh(
                state(before_id, "a" * 64, "collection_failed", "network", retryable=True),
                state(after_id, "b" * 64, "observed", "observed"),
            )
            self.assertFalse(result.succeeded)
            self.assertEqual(result.error_code, expected_error)

    def test_unrelated_market_change_is_not_success(self):
        before = state("s1", "a" * 64, "not_cataloged_in_snapshot", None)
        after = state(
            "s2", "b" * 64, "observed", "observed",
            market_id="cex:upbit:AAVE/USDT",
        )
        result = evaluate_snapshot_refresh(before, after)
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_mismatch")

    def test_new_publication_without_target_fact_is_not_success(self):
        before = state(
            "s1", "a" * 64, "not_cataloged_in_snapshot",
            "not_cataloged_in_snapshot", retryable=True,
        )
        after = state(
            "s2", "b" * 64, "not_cataloged_in_snapshot",
            "not_cataloged_in_snapshot", retryable=True,
        )
        result = evaluate_snapshot_refresh(before, after)
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_unresolved")

    def test_new_retryable_failure_is_not_success(self):
        before = state(
            "s1", "a" * 64, "collection_failed", "network",
            retryable=True,
        )
        after = state(
            "s2", "b" * 64, "collection_failed", "network",
            retryable=True,
        )
        result = evaluate_snapshot_refresh(before, after)
        self.assertFalse(result.succeeded)
        self.assertTrue(result.retryable)

    def test_new_exact_observation_succeeds(self):
        before = state("s1", "a" * 64, "not_cataloged_in_snapshot", None)
        after = state("s2", "b" * 64, "observed", "observed")
        result = evaluate_snapshot_refresh(before, after)
        self.assertTrue(result.succeeded)
        self.assertEqual(result.resolution, "observed")

    def test_only_allowlisted_terminal_outcomes_succeed(self):
        for status, reason in (
            ("partial", "source_level_limit"),
            ("source_no_observation", "source_no_two_sided_book"),
            ("source_no_observation", "source_no_order_book"),
            ("unsupported", "unsupported_chain"),
            ("unsupported", "unsupported_protocol"),
        ):
            with self.subTest(status=status, reason=reason):
                result = evaluate_snapshot_refresh(
                    state("s1", "a" * 64, "collection_failed", "network", retryable=True),
                    state("s2", "b" * 64, status, reason),
                )
                self.assertTrue(result.succeeded)

    def test_unknown_observed_or_partial_reason_fails_closed(self):
        for status in ("observed", "partial"):
            result = evaluate_snapshot_refresh(
                state("s1", "a" * 64, "collection_failed", "network", retryable=True),
                state("s2", "b" * 64, status, "unknown"),
            )
            self.assertFalse(result.succeeded)
            self.assertEqual(result.error_code, "snapshot_target_unresolved")
```

`SnapshotRefreshResult` must provide the `success(...)` and `failure(...)`
constructors used by the evaluator below; those constructors are part of the
pure module's tested interface, not test-only helpers.

- [ ] **Step 2: Replace the false-success integration expectation**

Change `test_depth_refresh_runs_bounded_profile_and_token_scope` so `_run_command`
alone does not prove success. Patch `read_snapshot_fact_state` with an unchanged
before/after sequence and assert `_set_job` receives `status="partial"`,
`publication_committed=False`, and
`error_code="snapshot_publication_unchanged"`. Add a second integration test
with a changed exact state and assert `status="succeeded"`.

- [ ] **Step 3: Run snapshot/public-action tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_snapshot_fact_refresh tests.test_public_actions
```

Expected: import fails because the pure postcondition module is absent and the
existing job still marks a zero-exit command successful.

- [ ] **Step 4: Implement exact snapshot state reading and evaluation**

Create `dashboard/snapshot_refresh.py` with Python 3.8-compatible frozen
dataclasses. `read_snapshot_fact_state()` must:

- choose only `dex_pool_tvl_latest.csv`, `cex_depth_latest.csv`, or
  `dex_depth_latest.csv` from the supplied `data_dir`;
- hash the entire bounded publication;
- validate its required columns, unique exact identity, timestamps, finite
  measured values, and status/reason invariants before selecting any row;
- parse the canonical Market ID into exact CSV identity fields;
- select the exact latest row;
- normalize status/reason through `quality_outcome_rule()`;
- return an explicit `not_cataloged_in_snapshot` state when the publication is
  valid but lacks the exact Market;
- retain a bounded `publication_generation` derived from the validated dataset
  hash and snapshot ID for pre/post reporting.

Implement the evaluator:

```python
def evaluate_snapshot_refresh(before, after):
    if before.market_id != after.market_id or before.fact_type != after.fact_type:
        return SnapshotRefreshResult.failure("snapshot_target_mismatch")
    if not after.snapshot_id:
        return SnapshotRefreshResult.failure(
            "snapshot_publication_identity_invalid"
        )
    if (
        before.dataset_sha256 == after.dataset_sha256
        or before.snapshot_id == after.snapshot_id
    ):
        return SnapshotRefreshResult.failure("snapshot_publication_unchanged")
    rule = quality_outcome_rule(after.status, after.reason_code)
    if (
        rule is not None
        and rule.terminal
        and not rule.retryable
        and after.retryable is rule.retryable
    ):
        return SnapshotRefreshResult.success(rule.resolution, before, after)
    return SnapshotRefreshResult.failure(
        "snapshot_target_unresolved", retryable=after.retryable
    )
```

In `_run_snapshot_refresh_job()`, read `before` before `_run_command`, clear any
relevant reader cache, read `after` after it, evaluate, and pass the result to
`_set_job`. Include only bounded pre/post publication generations, IDs,
statuses, reasons, and observation times in the public job result. Loading or
validating either publication fails closed; a zero process exit never overrides
that result.

- [ ] **Step 5: Run snapshot/public-action tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_snapshot_fact_refresh tests.test_public_actions
```

Expected: all tests pass; public action validation remains server-owned and no
test treats process exit code as publication success.

- [ ] **Step 6: Commit Task 4**

```bash
git add dashboard/snapshot_refresh.py dashboard/admin.py \
  tests/test_snapshot_fact_refresh.py tests/test_public_actions.py
git commit -m "fix(actions): verify exact fact after snapshot refresh"
```

---

### Task 5: Bind Attempt Evidence to Exact Instrument and Valid Ledger Time

**Files:**
- Modify: `scripts/fact_quality.py:normalize_collection_attempts`
- Modify: `scripts/fact_quality.py:_attempt_matches_market`
- Modify: `scripts/fact_quality.py:attempt_for_gap`
- Modify: `scripts/fetch_cex.py:cex_attempt_record`
- Modify: `scripts/fetch_cex.py:write_attempt_ledger`
- Modify: `scripts/fetch_dex.py:dex_attempt_record`
- Modify: `scripts/fetch_dex.py:write_attempt_ledger`
- Modify: `scripts/run_fact_pipeline.py:_attempt_with_window`
- Modify: `tests/test_fact_quality.py`
- Modify: `tests/test_fetch_cex.py`
- Modify: `tests/test_fetch_dex.py`
- Modify: `tests/test_run_fact_pipeline.py`

**Interfaces:**
- Consumes: `daily_collection_attempts/v1` ledgers.
- Produces: normalized unique attempt records with complete exact identity,
  validated source-instrument aliases, and UTC-aware ordering evidence.

- [ ] **Step 1: Add failing cross-instrument and malformed-ledger tests**

In `tests/test_fact_quality.py`, create two CEX rows for one Token/exchange but
different instruments, then supply an attempt only for the second instrument:

```python
def test_cex_attempt_cannot_cross_quote_instruments(self):
    market = {
        "market_type": "cex",
        "token_symbol": "AAVE",
        "exchange": "upbit",
        "instrument": "AAVE/USDT",
    }
    attempt = cex_attempt(
        "2026-07-19",
        exchange="upbit",
        instrument="AAVE/KRW",
        reason_code="rate_limit",
    )
    self.assertIsNone(
        attempt_for_gap([attempt], market, date(2026, 7, 19))
    )
```

Add table-driven invalid ledger cases asserting `_attempt_source()` reports
`ignored_invalid` for:

```python
invalid_mutations = {
    "empty_id": {"attempt_id": ""},
    "naive_timestamp": {"finished_at_utc": "2026-07-20T00:30:00"},
    "outside_window": {"observed_dates": ["2026-07-21"]},
    "missing_instrument": {"instrument": None},
}
```

Add a duplicate-ID ledger containing two otherwise valid attempts and assert it
is ignored rather than selecting one lexicographically.

In `tests/test_fetch_cex.py` and `tests/test_fetch_dex.py`, freeze two distinct
UTC completion instants and build otherwise identical attempt records. Assert
their 20-character IDs differ and both writers reject any duplicate ID before
publication. Add an Upbit success fixture whose canonical instrument is
`AAVE/USDT` but whose returned row is `AAVE/KRW`; assert the record preserves:

```python
{
    "instrument": "AAVE/USDT",
    "source_instrument": "AAVE/KRW",
    "source_instrument_alias_validated": True,
}
```

Then assert this record matches only the canonical `AAVE/USDT` Market and can
never explain an `AAVE/KRW` catalog Market. Add a DEX discovery-error fixture
with no chain/dex/pool identity and assert it is not published as exact Market
evidence; exact pool attempts remain present.

In `tests/test_run_fact_pipeline.py`, carry two attempts for the same
Market/window/outcome but different original attempt IDs through window slicing;
assert the derived IDs remain distinct and retain the source-instrument fields.

- [ ] **Step 2: Run daily quality tests and verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_fact_quality \
  tests.test_fetch_cex \
  tests.test_fetch_dex \
  tests.test_run_fact_pipeline
```

Expected: the cross-instrument match currently returns an attempt, repeated
producer inputs collide on one ID, the Upbit source alias is absent, and
malformed IDs/timestamps can survive normalization.

- [ ] **Step 3: Implement strict identity and timestamp normalization**

Change `_attempt_matches_market()` for CEX:

```python
return (
    attempt.get("exchange") == market.get("exchange")
    and attempt.get("instrument") == market.get("instrument")
)
```

In `normalize_collection_attempts()`:

- reject empty or repeated `attempt_id` values;
- require exchange+instrument for CEX and chain+dex+pool for DEX;
- retain a bounded uppercase `source_instrument` for CEX and require
  `source_instrument_alias_validated=True` only for the explicit Upbit
  `BASE/USDT -> BASE/KRW` fallback; a different exchange, base asset, or reverse
  direction is invalid;
- parse `finished_at_utc` with `datetime.fromisoformat`, require timezone data,
  convert to UTC, and retain canonical `+00:00` text;
- require requested start/end and every observed date inside that interval;
- keep the existing status/reason/outcome/count invariants.

In `attempt_for_gap()`, sort on the parsed canonical UTC timestamp plus stable
attempt ID. Matching always uses the configured canonical instrument;
`source_instrument` remains lineage and never broadens matching.

In both collectors, capture `finished_at_utc` once before hashing and include it
in `attempt_id` material. Writers verify non-empty unique IDs and complete exact
Market identity before atomically publishing. Successful CEX attempts derive
`source_instrument` from the returned rows; validate the Upbit fallback above.
Do not publish token-level DEX discovery failures without an exact pool identity
as Market attempt evidence.

In `_attempt_with_window()`, include the immutable original `attempt_id` plus
the sliced window in the derived ID material, preserving alias lineage. This
keeps two genuine source attempts distinct after carry-forward.

- [ ] **Step 4: Run quality/collector tests and verify GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_fact_quality \
  tests.test_fetch_cex \
  tests.test_fetch_dex \
  tests.test_run_fact_pipeline \
  tests.test_run_exact_backfill
```

Expected: all tests pass and invalid attempt evidence leaves gaps unexplained
instead of assigning another instrument's failure reason.

- [ ] **Step 5: Commit Task 5**

```bash
git add scripts/fact_quality.py tests/test_fact_quality.py
git add scripts/fetch_cex.py scripts/fetch_dex.py scripts/run_fact_pipeline.py \
  tests/test_fetch_cex.py tests/test_fetch_dex.py \
  tests/test_run_fact_pipeline.py
git commit -m "fix(quality): bind attempts to exact market identity"
```

---

### Task 6: Enforce Release Parity and Update Operator Contracts

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `docs/market-facts-contract.md`
- Modify: `docs/admin-operations.md`
- Modify: `docs/market-monitor-design.md`

**Interfaces:**
- Consumes: Summary v2 and Data Quality v4 responses for every configured Token.
- Produces: a failing release if any Screener severity/status count cannot be reproduced from `screening_quality_*`.

- [ ] **Step 1: Add a failing release-validator parity test**

Extend `tests/test_release_smoke.py` with:

```python
def test_screening_quality_must_match_summary_counts(self):
    summary_row = self.summary()["tokens"][0]
    info_flag = {
        "code": "depth_unavailable",
        "severity": "info",
        "category": "availability",
        "message": "No executable-depth observation is available.",
    }
    critical_flag = {
        "code": "depth_failed",
        "severity": "critical",
        "category": "data_health",
        "message": "The latest depth collection failed.",
    }
    quality = {
        "metadata": {
            "contract_version": 4,
            "data_generation": "generation-1",
        },
        "token_symbol": "AAVE",
        "markets": [
            {
                "market_id": "cex:binance:AAVE/USDT",
                "screening_quality_status": "ok",
                "screening_quality_flags": [info_flag],
            },
            {
                "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                "screening_quality_status": "ok",
                "screening_quality_flags": [],
            },
        ],
    }
    validate_screening_quality_parity(
        summary_row, quality, expected_generation="generation-1"
    )
    quality["markets"][0]["screening_quality_flags"] = []
    quality["markets"][0]["screening_quality_status"] = "warning"
    with self.assertRaisesRegex(ReleaseCheckError, "fallback alert"):
        validate_screening_quality_parity(
            summary_row, quality, expected_generation="generation-1"
        )
    quality["markets"][0]["screening_quality_flags"] = [info_flag]
    quality["markets"][0]["screening_quality_status"] = "ok"
    quality["markets"][1]["screening_quality_status"] = "critical"
    quality["markets"][1]["screening_quality_flags"] = [critical_flag]
    with self.assertRaisesRegex(ReleaseCheckError, "screening quality"):
        validate_screening_quality_parity(
            summary_row, quality, expected_generation="generation-1"
        )

    quality["metadata"]["data_generation"] = "generation-2"
    with self.assertRaisesRegex(ReleaseCheckError, "generation"):
        validate_screening_quality_parity(
            summary_row, quality, expected_generation="generation-1"
        )
```

- [ ] **Step 2: Run release tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_release_smoke
```

Expected: import fails because `validate_screening_quality_parity` does not yet
exist and the checker still accepts quality contract v3.

- [ ] **Step 3: Implement 30-Token parity checks**

Add `validate_screening_quality_parity(summary_row, quality_payload,
expected_generation)` to `scripts/check_dashboard_release.py`. It must require
the quality metadata generation to equal the Summary generation, then recompute:

```python
status_counts = Counter(
    market["screening_quality_status"]
    for market in quality_payload["markets"]
)
alert_counts = Counter(
    flag["severity"]
    for market in quality_payload["markets"]
    for flag in market["screening_quality_flags"]
)
```

The accepted flag shape requires a bounded snake-case `code`, severity in
`info|warning|critical`, category in the documented quality dimensions, and a
nonempty public message no longer than 240 characters with no URL or protected
path marker. Reject missing/unknown screening fields, invalid flag shapes, and a
non-OK screening status with no fallback alert. Compare the exact dictionaries with
the Summary row after removing zero entries. Require contract version 4. During
the executable release check, fetch
`/api/markets/quality?token=<TOKEN>&scope=all` for every Summary Token and run
the parity validator with Summary's `metadata.data_generation`. A generation
change is a release failure, not a count mismatch to retry invisibly. Preserve
response metrics in the final report.

- [ ] **Step 4: Update contracts and operator procedure**

Document:

- quality contract v4 dual projections;
- measured-only DEX timing rules;
- stable empty-book reason semantics;
- exact snapshot refresh postconditions and job outcomes;
- full CEX attempt identity and invalid-ledger fallback;
- one-shot MORPHO recovery gate;
- explicit Funding Rate exclusion.

- [ ] **Step 5: Run release tests, full tests, syntax, and diff checks**

Run:

```bash
python3 -m unittest tests.test_release_smoke
python3 -m unittest discover -s tests -p 'test_*.py'
python3 -m py_compile \
  dashboard/server.py dashboard/admin.py dashboard/market_facts.py \
  dashboard/snapshot_refresh.py scripts/quality_outcomes.py \
  scripts/fact_quality.py scripts/fetch_cex.py scripts/fetch_dex.py \
  scripts/run_fact_pipeline.py \
  scripts/check_dashboard_release.py
node --check dashboard/static/app.js
node --check dashboard/static/navigation.js
git diff --check
```

Expected: zero failures, zero syntax errors, and no whitespace errors. Record
the exact test count; do not reuse the previous 545-test result.

- [ ] **Step 6: Commit Task 6**

```bash
git add scripts/check_dashboard_release.py tests/test_release_smoke.py \
  docs/market-facts-contract.md docs/admin-operations.md \
  docs/market-monitor-design.md
git commit -m "test(release): enforce quality drill-down parity"
```

---

### Task 7: Review, Push, Deploy, Verify, and Conditionally Recover MORPHO

**Files:**
- Review: every file changed by Tasks 1–6
- Production checkout: `/home/ugs/workspace/cex-dex-market-monitor-v1`
- Production environment: `/home/ugs/.config/cex-dex/dashboard.env`

**Interfaces:**
- Consumes: committed branch head and current production data generation.
- Produces: verified deployment evidence and at most one exact MORPHO recovery result.

- [ ] **Step 1: Review every task against the approved specification**

Run:

```bash
git status --short --branch
git diff origin/codex/critical-quality-sorting-token-refresh...HEAD --check
git log --oneline origin/codex/critical-quality-sorting-token-refresh..HEAD
```

Inspect each diff for raw-error leakage, zero filling, unsupported-to-failure
conversion, broad retryability, Python 3.8-incompatible syntax, and unrelated
Funding changes.

- [ ] **Step 2: Run one fresh complete local verification**

Run the complete command block from Task 6 Step 5 again after review changes.
Expected: all commands exit 0 with a freshly observed test count.

- [ ] **Step 3: Push every commit and attach explicit comments**

```bash
git push origin codex/critical-quality-sorting-token-refresh
```

For each newly pushed commit, attach one GitHub commit comment summarizing its
scope and exact verification evidence. Verify `git ls-remote` equals local HEAD.

- [ ] **Step 4: Run production preflight before restart**

On `ugs@43.156.102.166`, leave the live checkout untouched. Record the live
`PREV_SHA`, the target SHA, the service definition, and a permission-preserving
backup plus SHA-256 checksum of
`/home/ugs/.config/cex-dex/dashboard.env`. Fetch the target, then create a
separate detached Git worktree under
`/home/ugs/workspace/cex-dex-preflight-<TARGET_SHA>` and run there:

Before tests, require:

```bash
systemctl --user show cex-dex-dashboard.service \
  -p WorkingDirectory -p ExecStart
```

Abort unless the loaded user unit's WorkingDirectory and executable resolve to
the intended `/home/ugs/workspace/cex-dex-market-monitor-v1` release checkout.

```bash
python3 --version
python3 -m py_compile \
  dashboard/server.py dashboard/admin.py dashboard/market_facts.py \
  dashboard/snapshot_refresh.py scripts/quality_outcomes.py \
  scripts/fact_quality.py scripts/fetch_cex.py scripts/fetch_dex.py \
  scripts/run_fact_pipeline.py \
  scripts/check_dashboard_release.py
python3 -c "import dashboard.server; import dashboard.admin; import dashboard.snapshot_refresh"
python3 -m unittest discover -s tests -p 'test_*.py'
```

Expected: the actual production Python reports 3.8.10, compilation/imports
succeed, and all non-Node production tests pass; Node-skipped tests must already
have passed locally. A preflight failure leaves the old service and live
checkout unchanged.

- [ ] **Step 5: Restart and execute release/health checks**

Only after preflight succeeds, run
`systemctl --user stop cex-dex-dashboard.service` so old Python
and old static files cannot be mixed. Detach the live checkout at the exact
verified target SHA, run
`systemctl --user start cex-dex-dashboard.service`, and poll
`http://127.0.0.1:8766/health`. Then run from the target release:

```bash
python3 scripts/check_dashboard_health.py --url http://127.0.0.1:8766/health
python3 scripts/check_dashboard_release.py --base-url http://127.0.0.1:8766
```

Expected: service `active`, quality contract v4, 30/30 screening parity, no
unsupported-depth temporal mismatch flags, and all fact freshness gates current.

After start, require `systemctl --user show ... -p MainPID` to report a live
PID, verify `/proc/<MainPID>/cwd` resolves to the intended checkout, and verify
that checkout is detached at the exact target SHA before release checks.

If restart, health, release parity, browser QA, or the post-deployment quality
audit fails, run `systemctl --user stop`, detach the live checkout back to the
recorded `PREV_SHA`, restore the environment backup only if its checksum
changed, run `systemctl --user start`, and repeat the same MainPID/cwd/SHA plus
old-health verification. Record both failed target and restored SHAs. Do not
claim deployment merely because rollback succeeded.

- [ ] **Step 6: Perform desktop and 390px browser QA**

Using Ego Browser, verify:

- Screener severity chips match the exact Data Quality reason count;
- `origin=screener` displays screening reasons and ordinary Token Research
  displays selected-window reasons;
- unsupported DEX depth is capability information, not Critical time mismatch;
- empty CEX books show a stable non-retryable explanation;
- no N/A disclosure or reason panel overflows at 390px;
- browser events contain no exception or failed request.

- [ ] **Step 7: Re-evaluate and conditionally run one MORPHO refresh**

Fetch current production quality for `MORPHO`. Locate
`cex:upbit:MORPHO/USDT`. If depth is not `retryable=true`, record “no action” and
create no job. If it remains retryable, submit exactly one request:

```http
POST /api/actions/facts/refresh
Content-Type: application/json

{
  "token_symbol": "MORPHO",
  "market_id": "cex:upbit:MORPHO/USDT",
  "fact_type": "depth"
}
```

Poll only that returned job ID until terminal. Do not resubmit. Verify the job's
pre/post evidence, refreshed quality response, Depth/Execution status, retryable
count, and data generation.

- [ ] **Step 8: Verify public production and report exact outcomes**

Confirm:

```bash
curl --fail http://43.156.102.166:8765/health
git ls-remote origin refs/heads/codex/critical-quality-sorting-token-refresh
ssh ugs@43.156.102.166 \
  'cd /home/ugs/workspace/cex-dex-market-monitor-v1 && git rev-parse HEAD && systemctl --user is-active cex-dex-dashboard.service'
```

Report separately: commits/messages, push comments, local tests, production
preflight, release checker, browser QA, deployment SHA, before/after quality
counts, and MORPHO action/outcome. If a check fails, report the failure and
rollback state instead of claiming completion.
