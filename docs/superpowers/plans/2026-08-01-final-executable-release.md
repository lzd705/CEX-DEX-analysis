# Executable Opportunity Final Release Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Release the synchronized Route, all-in Cost, Opportunities, prioritized DEX adapter, and Event-clock increments as one verifiable production version with rollback and expert-quality evidence.

**Architecture:** This plan adds no new product semantics. It is the single cross-increment gate that validates exact Git/GitHub/server identity, immutable data pointers, public API contracts, browser behavior, timers, and rollback before production is called complete.

**Tech Stack:** Python 3.8 production runtime, `unittest`, release checker, systemd, immutable pointers, HTTP/browser acceptance.

## Global Constraints

- Depends on every task in the five dated implementation plans being complete and reviewed.
- Upbit facts and Funding Rate remain unchanged.
- No production timer or public pointer is enabled before all combined gates pass.
- Existing Daily/TVL/Depth/Execution and Event bundles remain independently rollback-safe.
- The last validated bundle keeps its real timestamp after a failed cycle; freshness is never extended.
- A deployment claim requires local tests, GitHub SHA, server SHA, health/release checks, two live cohorts, and desktop/mobile browser evidence.

---

### Task 1: Combined release contract and reproducible candidate

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/production-hardening.md`
- Modify: `docs/collection-operations.md`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_framework.py`

- [ ] **Step 1: Write failing cross-increment counterexamples**

Reject a core-only public pointer, adapter generation mismatch, Event API v1
payload, stale strict route numerics, route without inventory evidence,
route-capable adapter without common-quantity conformance, missing N/A reason,
asset-build SHA mismatch, and timer enabled before the complete gate.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke tests.test_framework -v`

Expected: FAIL because the combined gate is absent.

- [ ] **Step 3: Implement one release manifest**

Generate a release manifest bound to Git SHA, static-asset build SHA, Daily,
TVL, Depth, Execution, Event, lifecycle, complete-route, fee-profile,
inventory-profile, and adapter generations. Bind Route core lineage to the
exact core manifest hash embedded inside the selected complete bundle, not the
current `routes/core/latest.json` pointer. A newer core-only pointer is normal
in-progress data when finalization fails and must not invalidate the last
healthy public complete bundle. Private evidence is represented only by opaque
hashes. Validate all public N/A reasons and strict/estimate inventory
separation.

- [ ] **Step 4: Run the complete local gate**

Run focused suites, `python3 -m unittest discover -s tests -v`, Python 3.8
compile/import checks, JavaScript syntax/tests, fixture/hash verification,
`git diff --check`, and the release checker against a temporary production-like
data root.

- [ ] **Step 5: Commit**

```bash
git add scripts/check_dashboard_release.py docs/production-hardening.md docs/collection-operations.md tests/test_release_smoke.py tests/test_framework.py
git commit -m "test(release): bind executable opportunity release"
```

Add a GitHub commit comment with exact test totals and generation inventory.

### Task 2: Bounded live data preflight and quality audit

**Files:**
- No source changes unless a tested defect is found.

- [ ] **Step 1: Run no-publish AAVE candidate**

Run dry-run, one live core cohort, cost/inventory classification, complete
bundle finalization in an isolated data root, and the strict checker. Confirm
60-second skew and 120-second age boundaries use actual state timestamps.

- [ ] **Step 2: Run representative adapter candidates**

For each enabled adapter family, compare one live fixed-block/slot quote with
its offline/reference parity evidence. Any mismatch keeps that family disabled
and does not block unaffected route research output.

- [ ] **Step 3: Perform expert data-quality audit**

Check identity/grain uniqueness, exact quantity parity, timestamp/skew/age,
hash lineage, fee/inventory freshness, null-versus-zero, strict/estimate
separation, route-level failure isolation, Event clock transitions, and
Daily/TVL/Depth/Execution non-regression. Record denominators and excluded
inventories; do not convert a failed route into a missing row.

- [ ] **Step 4: Record the preflight evidence**

Record cohort IDs, collection window/deadline, source blocks/slots/timestamps,
route/class/reason counts, strict and estimate count, adapter coverage, hashes,
latency/payload sizes, and every remaining limitation.

### Task 3: Rollback-first production deployment

**Files:**
- No source changes unless a tested defect is found.

- [ ] **Step 1: Resolve exact targets and create rollback points**

Verify canonical remote/branch/SHA, clean production checkout, current service
unit, environment, app target, active data pointers, and disk headroom. Preserve
the previous application SHA/static assets and every prior active pointer as
explicit rollback targets.

- [ ] **Step 2: Deploy the exact GitHub SHA with timers disabled**

Run the Python 3.8 suite on the server, render templates from the deployed
checkout, restart the dashboard only, and verify `/health`, release checker,
asset version, APIs, and server SHA before collecting route data.

- [ ] **Step 3: Publish and validate one complete cohort**

Run one manual synchronized cohort through core, cost, opportunity, and final
publication. Validate the complete pointer, public API generation, strict and
estimate inventories, N/A disclosures, and no changes to Upbit or Funding Rate.

- [ ] **Step 4: Enable the two-minute timer and observe two cycles**

Enable only after Step 3. Require two distinct valid complete cohort IDs, real
timestamps, SLA/status counts, non-overlapping lock behavior, and bounded raw
retention dry-run output. A failed cycle must leave the previous cohort stale,
not retimestamped.

- [ ] **Step 5: Desktop/mobile browser acceptance**

Verify Screener, Markets, Token Research, Data Actions, Opportunities, Event
Past/Future, Daily Price Gap naming, filters/sorting, exact N/A disclosures,
request-race ownership, responsive alignment, and route deep links on desktop
and mobile widths.

- [ ] **Step 6: Exercise rollback and restore the release**

Prove application rollback and route-pointer rollback separately in a bounded
maintenance check, then restore the validated release. Repeat health, release,
API, timer, SHA, asset, pointer, and browser checks.

### Task 4: Final expert-readiness report

**Files:**
- Modify: `docs/market-monitor-design.md`

- [ ] **Step 1: Report product boundaries**

Separate research Facts, Daily Price Gap, Research Estimates, strict executable
candidates, and unsupported route modes. State that candidates are source-
backed analysis, not guaranteed fills or order placement.

- [ ] **Step 2: Report expert utility and remaining gaps**

Score data integrity, freshness, execution realism, coverage, observability,
performance, UX, and operational reliability with exact evidence. Prioritize
next work by measured route value and failure counts rather than feature count.

- [ ] **Step 3: Commit and comment**

```bash
git add docs/market-monitor-design.md
git commit -m "docs(report): record executable dashboard readiness"
```

Add a GitHub commit comment linking the exact production SHA, cohort IDs,
verification totals, and known limitations.
