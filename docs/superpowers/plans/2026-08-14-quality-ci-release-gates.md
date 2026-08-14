# Quality CI and Release Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the latest route-quality branch test-green and add reproducible GitHub CI evidence for Python 3.8/current Python and Node-backed checks, without changing the strict data contract or deploying production.

**Architecture:** Preserve the fail-closed route publication contract and repair the stale audit fixture that predates it. Add a thin GitHub Actions workflow that runs the repository's own compile, import, and full unittest gates on both the minimum supported Python and the current development Python, with Node available so browser-facing JavaScript checks cannot silently skip. Treat live-release drift as deployment evidence, not a reason to weaken source-code gates.

**Tech Stack:** Python 3.8-compatible standard library, `unittest`, Node.js, GitHub Actions.

## Global Constraints

- Base commit is `dee9eaceea1fcb05e05fe65ed8d21e67b57e4eae` from `codex/route-shadow-v3-performance`.
- Work only on `codex/quality-ci-release-gates`.
- Do not change Funding Rate, observed market snapshots, B-task data collection, or production infrastructure.
- Do not loosen `partial` CEX validation: it must retain `reason_code=source_level_limit`.
- Do not describe an unrun workflow or an old deployment as verified current production.
- Commit and push the branch; do not merge or deploy it.

---

### Task 1: Align route-shadow audit fixtures with the strict CEX contract

**Files:**
- Modify: `tests/test_route_shadow_audit.py`

- [ ] **Step 1: Preserve the reproduced RED evidence**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests.test_route_shadow_audit.BuildShadowAuditTests.test_partial_leg_without_literal_true_is_available
```

Expected before the fix: ERROR with `CEX leg status and reason conflict` because the old fixture sets `status=partial` but leaves `reason_code=None`.

- [ ] **Step 2: Repair only the valid partial fixture**

Set the CEX leg's `reason_code` to the contract value `source_level_limit`. Keep the test focused on the separate rule that a missing literal `available=true` must not be inferred.

- [ ] **Step 3: Add an explicit fail-closed counterexample**

Add a test showing that `status=partial` plus `reason_code=None` raises `RouteShadowAuditError` with `CEX leg status and reason conflict`.

- [ ] **Step 4: Verify targeted GREEN**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests.test_route_shadow_audit
```

Expected: PASS. No production code changes are required.

- [ ] **Step 5: Commit**

```bash
git add tests/test_route_shadow_audit.py
git commit -m "test(routes): align partial CEX audit contract"
```

### Task 2: Add minimum-version and current-version CI

**Files:**
- Create: `.github/workflows/quality-gates.yml`

- [ ] **Step 1: Define a read-only workflow boundary**

Trigger on pushes and pull requests, grant only `contents: read`, use a bounded timeout, and add no deployment credentials or deployment steps.

- [ ] **Step 2: Exercise both supported runtime edges**

Use a matrix containing Python `3.8` and `3.13`. Install a maintained Node LTS release in each job and install the locked dashboard dependency set from `dashboard/package-lock.json`.

- [ ] **Step 3: Run the repository's actual gates**

Each matrix job must run:

```bash
python -m compileall -q dashboard deploy scripts tests
python -c "import dashboard.server; import dashboard.market_facts"
python -m unittest discover -s tests -p 'test_*.py'
```

Node must be on `PATH`, so JavaScript syntax and browser-contract tests run instead of skipping.

- [ ] **Step 4: Validate locally**

Inspect the workflow diff, run `git diff --check`, and run the same gates locally with the bundled Node runtime. Record explicitly that local Python is not a substitute for the workflow's real Python 3.8 job.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/quality-gates.yml
git commit -m "ci: enforce repository quality gates"
```

### Task 3: Verify release-facing behavior and the complete repository

**Files:**
- No source change expected; modify code only if a new reproducible defect is found and covered by a failing test first.

- [ ] **Step 1: Run the changed route pipeline suite**

Run the 13 route/collector modules used in the prior audit and confirm the former 779-pass/1-error result is fully green.

- [ ] **Step 2: Run framework and release-facing suites**

Run `tests.test_framework`, `tests.test_static_delivery`, and `tests.test_release_smoke` with Node available. Preserve the exact immutable-cache checker.

- [ ] **Step 3: Run the full suite and import gates**

Run the compile, import, and complete unittest discovery commands from Task 2. Capture pass/skip counts and explain any skip.

- [ ] **Step 4: Separate code evidence from live deployment evidence**

The known live application SHA is older than this branch and fails the latest immutable-static release check. Do not deploy and do not weaken the checker; report the mismatch as a separate release blocker.

### Task 4: Independent review, push, and remote CI proof

**Files:**
- Review the complete branch diff against `dee9eac`.

- [ ] **Step 1: Review scope and contract preservation**

Confirm there are no Funding Rate, B-task, runtime-data, secret, production, or deployment changes, and that no strict validation was relaxed.

- [ ] **Step 2: Push only the A branch**

Push `codex/quality-ci-release-gates` to `origin`; do not merge it.

- [ ] **Step 3: Verify the exact remote SHA and Actions result**

Compare local HEAD with `origin/codex/quality-ci-release-gates`. Wait for both Python matrix jobs and report their actual status. If GitHub Actions is unavailable or fails, report that honestly rather than treating the local suite as remote CI proof.

