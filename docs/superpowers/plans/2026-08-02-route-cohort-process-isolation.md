# Route Cohort Time and Process Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Task 4 route cohorts future-safe, raw-evidence-backed, and bounded by killable Unix process isolation without changing the Task 5 publication boundary.

**Architecture:** The parent process owns validation, scheduling, canonical timestamps, cohort assembly, and evidence promotion. Each resolver or collector runs in one explicitly forked child process and returns one serializable result through a one-way pipe; the parent main thread polls pipes directly, and shutdown terminates and joins every child and closes every pipe. The fair scheduler limits all aggregate non-CEX work to reserve one global slot for CEX legs until all CEX work is terminal, while tests that require shared fake clocks may explicitly inject the bounded thread executor.

**Tech Stack:** Python standard library (`multiprocessing`, `concurrent.futures.Future`, `threading`, `hashlib`, `pathlib`), `unittest`, existing Task 2/3 route and collector modules.

## Global Constraints

- Python 3.8 syntax and APIs only in `scripts/collect_route_cohort.py`.
- Production/public default and CLI require Unix `fork`; fail closed when unavailable.
- No network, push, deployment, Task 5 publication, or pointer mutation.
- Direct collection requires an explicit non-symlink `raw_root`; CLI supplies it.
- Commit once with exact message `fix(routes): validate cohort time and process isolation`.

---

### Task 1: Completion time and identity validation

**Files:**
- Modify: `scripts/collect_route_cohort.py`
- Test: `tests/test_route_collection.py`

**Interfaces:**
- Consumes: `canonical_route_id()`, `classify_route_timing()`, injected `wall_clock`.
- Produces: exact canonical route inputs plus top-level and per-route `collection_completed_at`.

- [x] **Step 1: Write failing tests** proving missing/empty/noncanonical `route_id` and conflicting `market_type` reject before source reads/raw work; two 2099 observations in a 2026 cohort classify `invalid_state_timestamp`; completion time changes the cohort fingerprint.
- [x] **Step 2: Run the named tests** and confirm failures come from the missing validation/completion behavior.
- [x] **Step 3: Implement minimal validation** by requiring the exact `canonical_route_id`, validating the market ID prefix against declared type, capturing canonical completion wall time, and passing it as `validated_at` into timing classification.
- [x] **Step 4: Re-run the named tests** and the existing route timing tests until green.

### Task 2: Fixed-block lineage and reserved resolver capacity

**Files:**
- Modify: `scripts/collect_route_cohort.py`
- Test: `tests/test_route_collection.py`

**Interfaces:**
- Consumes: resolver mapping with `block_number` and `block_timestamp`.
- Produces: positive integer block numbers, canonical UTC block timestamps, and terminal DEX rows retaining resolved lineage.

- [x] **Step 1: Write failing table tests** for zero, negative, bool, missing, malformed, and future fixed-block lineage plus a terminal DEX collector after successful resolution.
- [x] **Step 2: Write a failing capacity test** with `max_workers=2`, two hung resolver chains, and two same-venue CEX legs; both CEX legs must complete sequentially. Add the `max_workers=1` CEX-first case.
- [x] **Step 3: Run the named tests** and confirm the current resolver accepts bad lineage or consumes reserved CEX capacity.
- [x] **Step 4: Implement minimal lineage normalization and scheduling reservation** with aggregate non-CEX work limited to `max_workers - 1` while CEX remains, then re-run the named tests.

### Task 3: Killable process executor and raw-evidence promotion

**Files:**
- Modify: `scripts/collect_route_cohort.py`
- Test: `tests/test_route_collection.py`

**Interfaces:**
- Produces: `_ForkProcessExecutor(max_workers)` implementing `submit()` and `shutdown()` with standard `Future` objects.
- Consumes: explicit `raw_root`, collector row status, optional `raw_response_sha256`, and staged `response.json`.

- [x] **Step 1: Write a failing repeated-call regression** that performs several blocked default collections and proves `multiprocessing.active_children()` and route-cohort monitor-thread counts return to baseline after every call.
- [x] **Step 2: Write failing raw-root tests** for omitted direct root, existing/broken symlink roots, missing observed raw, and hash mismatch; prove each produces no accepted evidence and repo-local `data/raw` remains absent.
- [x] **Step 3: Run the named tests** and observe the thread leak/default-root/raw-promotion failures.
- [x] **Step 4: Implement `_ForkProcessExecutor`** with explicit fork and single-threaded-caller detection, one child/pipe/future per task, parent-thread pipe polling, TERM/KILL/join shutdown, and closed pipe endpoints.
- [x] **Step 5: Implement raw validation** before promotion: observed/partial rows require `response.json`; any claimed SHA-256 must match its bytes; symlink roots fail before creation.
- [x] **Step 6: Re-run the named tests** and repeat them to detect process/thread leakage.

### Task 4: Exact CLI result and verification

**Files:**
- Modify: `scripts/collect_route_cohort.py`
- Modify: `docs/route-cohort-data-contract.md`
- Modify ignored report: `.superpowers/sdd/2026-08-01-route-cohort-core/task-4-report.md`
- Test: `tests/test_route_collection.py`

**Interfaces:**
- Produces: live `main()` result identical to the fingerprint-bound `route_cohort_collection/v1` mapping; dry-run remains a separate `dry_run` mapping.

- [x] **Step 1: Write a failing live-main test** that independently recomputes both hashes and rejects extra `dry_run` or absolute path fields.
- [x] **Step 2: Remove the live post-fingerprint fields** and run the named test green.
- [x] **Step 3: Update contract and report** with fork-only support, completion/fixed-block validation, evidence promotion, resolver reservation, and exact output shape.
- [x] **Step 4: Run verification:** `python3 -m unittest tests.test_route_collection tests.test_route_cohort tests.test_route_universe tests.test_fetch_cex_depth tests.test_fetch_dex_depth -v`, `python3 -m py_compile scripts/collect_route_cohort.py tests/test_route_collection.py`, and `git diff --check`.
- [x] **Step 5: Inspect the staged diff and commit** with `fix(routes): validate cohort time and process isolation`.
