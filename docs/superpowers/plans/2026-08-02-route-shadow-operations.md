# Route Shadow Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run synchronized route cohorts in a bounded private shadow, measure readiness without contaminating the public opportunity pointer, and make any later promotion require seven-day timing and 100% strict-cost evidence.

**Architecture:** Pure input, metric, retention, and gate modules sit behind one narrow `run_route_shadow.py` entrypoint. Every run binds immutable source hashes, a run-scoped universe, the existing private core bundle, an audit, and one atomic joint shadow pointer. systemd owns cadence and resource limits; the existing global collection lock gives daily/depth collection priority.

**Tech Stack:** Python 3.8-compatible standard library, SQLite/CSV fact readers, `fcntl`, existing route publication contracts, systemd user units, `unittest`.

## Global Constraints

- Shadow never calls `publish_complete_route_bundle()` and never writes `routes/latest.json`.
- Production selection is the rolling 30 complete UTC days ending yesterday UTC.
- Lock contention records `skipped_locked` and exits zero before reading sources.
- Every source identity includes a byte SHA-256; a generation change rejects the run.
- Empty metric denominators are `not_evaluated` and fail readiness gates.
- 4 GiB is a high-water admission limit, never authority to delete protected evidence.
- The public promotion command remains manual and requires seven days, 500 valid cohorts, timing gates, and strict cost/evidence completeness of exactly 100%.
- Funding Rate and multi-Market UI are outside this plan.

---

### Task 1: Immutable production inputs and run-scoped universe

**Files:**
- Create: `scripts/route_shadow_inputs.py`
- Create: `tests/test_route_shadow_inputs.py`
- Modify: `scripts/route_universe.py`

**Interfaces:**
- Produces: `SourceFileIdentity(path: str, size: int, sha256: str)`.
- Produces: `selection_window(now: datetime) -> dict[str, str]`.
- Produces: `build_shadow_universe(data_dir: Path, now: datetime) -> tuple[dict, dict]`, returning universe and source manifest.
- Produces: `write_run_universe(shadow_root: Path, run_id: str, universe: Mapping) -> Path`.
- Writes the same run-scoped source manifest as
  `routes/shadow/runs/<run_id>/baseline_manifest.json`, including calculation
  version, filters, observation bounds, input paths, sizes, and SHA-256s.

- [ ] **Step 1: Write failing window and source-identity tests**

Use a fixed `2026-08-02T13:00:00Z` clock and assert the literal window
`{"start": "2026-07-03", "end": "2026-08-01"}`. Mutating one byte of every
required source must change the canonical generation; missing, symlinked, or
non-regular required sources must fail before universe construction.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m unittest tests.test_route_shadow_inputs -v`

Expected: FAIL because `scripts.route_shadow_inputs` does not exist.

- [ ] **Step 3: Implement exact source readers and generation hashing**

Read catalog identities from `market_facts.sqlite3` plus current lifecycle and
runtime registry files; read CEX daily Volume, CEX/DEX Depth, CEX/DEX Execution,
and DEX TVL/24h Volume from their published files. Aggregate CEX
`selected_window_usd` over the exact 30-day window and pass literal rows into
`build_route_universe()`. Hash canonical manifest entries containing relative
path, byte size, and SHA-256; do not use mtime as identity.

- [ ] **Step 4: Add failing atomic run-universe tests**

Assert the destination is exactly
`routes/shadow/runs/<run_id>/route_universe.json`, cannot be overwritten, is
canonical JSON, and rereads to the same `route_universe_sha256()`. Reject run
IDs containing separators, dot segments, whitespace, or non-ASCII controls.
Assert `baseline_manifest.json` binds the same candidate generation and cannot
be replaced independently.

- [ ] **Step 5: Implement exclusive immutable publication and verify GREEN**

Create the run directory without following symlinks, write with
`O_CREAT|O_EXCL`, `fsync` the file and directory, and return the final path.

Run: `python3 -m unittest tests.test_route_shadow_inputs tests.test_route_universe -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_shadow_inputs.py scripts/route_universe.py tests/test_route_shadow_inputs.py
git commit -m "feat(routes): bind shadow universe to source hashes"
```

Add a GitHub commit comment listing the literal window, mutation cases, and focused test count.

### Task 2: Deterministic audit metrics and joint shadow pointer

**Files:**
- Create: `scripts/route_shadow_audit.py`
- Create: `tests/test_route_shadow_audit.py`
- Modify: `scripts/route_publication.py`

**Interfaces:**
- Produces: `nearest_rank(values: Iterable[Decimal], percentile: Decimal) -> str | None`.
- Produces: `build_shadow_audit(cohort: Mapping, *, run: Mapping, phase: str, audit_finished_at: str) -> dict`.
- Produces: `publish_shadow_result(shadow_root: Path, *, core_pointer: Mapping, universe_path: Path, audit: Mapping) -> dict`.
- Produces: `load_latest_shadow_result(shadow_root: Path) -> dict`.

- [ ] **Step 1: Write failing literal metric tests**

Cover zero, one, two, and twenty samples. Assert nearest-rank p95 uses
`ceil(0.95*n)`, valid-core denominator is lock-acquired runs, availability is
within/all, conditional skew is within/(within+outside), unavailable routes do
not enter the conditional denominator, and empty denominators serialize as
`{"status": "not_evaluated", "numerator": 0, "denominator": 0, "value": null}`.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_audit -v`

Expected: FAIL because the audit module is absent.

- [ ] **Step 3: Implement metrics and strict schema validation**

Derive route age as `audit_finished_at - min(buy_state, sell_state)` only for
two-leg-available routes. Preserve exact numerator/denominator integers and
canonical decimal strings. Reject duplicate route IDs, missing leg lineage,
future state times, unknown statuses, and negative durations.

- [ ] **Step 4: Add failing pointer atomicity tests**

The pointer must bind `core_manifest_sha256`, `audit_sha256`,
`route_universe_sha256`, `phase`, and `run_id`. Inject failures before audit
write, after core publication, and during pointer replacement. The prior
pointer must remain readable; an orphan core must not count as valid.

- [ ] **Step 5: Implement joint publication and verify GREEN**

Write the immutable audit under `routes/shadow/runs/<run_id>/audit.json`, fully
reread universe/audit/core, then atomically replace only
`routes/shadow/latest.json`. Reuse route-publication bounded-read and path
safety rules rather than trusting caller objects.

Run: `python3 -m unittest tests.test_route_shadow_audit tests.test_route_publication -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_shadow_audit.py scripts/route_publication.py tests/test_route_shadow_audit.py tests/test_route_publication.py
git commit -m "feat(routes): publish auditable shadow readiness"
```

Add a GitHub commit comment with denominator, percentile, and failure-injection evidence.

### Task 3: Non-blocking shadow orchestrator and run ledger

**Files:**
- Create: `scripts/run_route_shadow.py`
- Create: `tests/test_run_route_shadow.py`
- Modify: `scripts/collect_route_cohort.py`

**Interfaces:**
- Produces: `run_shadow_once(data_dir: Path, now: datetime, phase: str, ...) -> dict`.
- Produces CLI subcommands: `run` and `reconcile`.
- Consumes Task 1 universe and Task 2 audit/pointer interfaces.

- [ ] **Step 1: Write the lock-priority RED test**

Hold `collection/collection.lock`, run the real orchestrator with source readers
that raise if called, and assert exit zero plus one `skipped_locked` ledger
record and zero source calls. Assert the lock is held across universe build,
collection, private-core publication, audit, and joint pointer publication.

- [ ] **Step 2: Write the no-public-pointer RED test**

Seed `routes/latest.json` with sentinel bytes, complete a shadow run, and assert
the sentinel is byte-identical. Patch `publish_complete_route_bundle` to raise
if invoked. Assert only `routes/core/latest.json` and
`routes/shadow/latest.json` advance.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_run_route_shadow -v`

Expected: FAIL because the orchestrator does not exist.

- [ ] **Step 4: Implement canary/full execution bounds**

Canary uses the literal ten-Token allowlist, deadline 60, workers 2, per venue
1, per chain 1. Full uses every eligible Token, workers 4, per venue/chain 1.
Use dependency injection for collectors in tests, but exercise the real input,
publication, and ledger boundaries. Rehash sources immediately before private
publication and reject drift.

- [ ] **Step 5: Implement durable ledger and reconciliation**

Write `started` before source reads and a terminal result after completion.
`reconcile --run-id ... --service-result ... --exit-code ... --exit-status ...`
must atomically close a started entry as success, failed, timeout, OOM, or
unexplained termination. A new run first closes any older unterminal entry as
unexplained; it never silently drops it.

- [ ] **Step 6: Verify GREEN and collector regressions**

Run: `python3 -m unittest tests.test_run_route_shadow tests.test_route_collection -v`

Expected: PASS, including generation drift, collector terminal rows, deadline,
and lock contention.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_route_shadow.py scripts/collect_route_cohort.py tests/test_run_route_shadow.py
git commit -m "feat(routes): orchestrate bounded shadow cohorts"
```

Add a GitHub commit comment proving the public pointer sentinel and lock-priority behavior.

### Task 4: Phase gates and manual promotion boundary

**Files:**
- Create: `scripts/route_shadow_gate.py`
- Create: `scripts/promote_route_opportunities.py`
- Create: `tests/test_route_shadow_gate.py`
- Modify: `scripts/check_dashboard_release.py`

**Interfaces:**
- Produces: `evaluate_phase(history: Sequence[Mapping], phase: str) -> dict`.
- Produces: `require_public_promotion_ready(shadow_root: Path, complete_bundle: Mapping) -> dict`.

- [ ] **Step 1: Write every negative gate as a failing test**

Use literal histories to reject: less than 24 hours/85 acquired canary runs;
less than seven days/500 valid full cohorts; valid rate below 99% canary or
99.5% promotion; conditional skew below 99%; p95 skew above 30 seconds; any
passing route above 60 seconds; duration above 75/90 seconds; any lineage,
unsafe-path, OOM, orphan, interference, unexplained-ledger, or resource-limit
verification error; storage pressure; and any `not_evaluated` required metric.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_gate -v`

Expected: FAIL because the gate module is absent.

- [ ] **Step 3: Implement canary-to-full atomic phase change**

Persist `phase.json` with prior phase, new phase, evaluated-at, exact gate
inputs, and joint-pointer SHA. Time alone cannot advance. A repeated evaluation
is idempotent and cannot skip phases.

- [ ] **Step 4: Implement manual public-promotion preflight**

The command has no timer unit. It loads the complete candidate bundle, requires
all existing topology/cryptographic/publication checks, requires structural
topology 100%, and requires strict fee, inventory, quote, conversion, gas,
router, tax, MEV policy, and route-mode evidence for every intended
route/notional. Research estimates cannot satisfy strict completeness.

- [ ] **Step 5: Verify GREEN and release-checker behavior**

Run: `python3 -m unittest tests.test_route_shadow_gate tests.test_release_smoke tests.test_route_publication -v`

Expected: PASS. The promotion command rejects all current incomplete profiles
without altering `routes/latest.json`.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_shadow_gate.py scripts/promote_route_opportunities.py scripts/check_dashboard_release.py tests/test_route_shadow_gate.py tests/test_release_smoke.py
git commit -m "feat(routes): gate manual opportunity promotion"
```

Add a GitHub commit comment enumerating the passing and rejected promotion fixtures.

### Task 5: Reference-safe retention and storage admission

**Files:**
- Create: `scripts/route_shadow_retention.py`
- Create: `tests/test_route_shadow_retention.py`

**Interfaces:**
- Produces: `protected_route_evidence(data_dir: Path, rollback_pointers: Sequence[Path]) -> set[Path]`.
- Produces: `apply_route_retention(data_dir: Path, now: datetime, high_water_bytes: int = 4 * 1024**3) -> dict`.

- [ ] **Step 1: Write failing traversal/reference tests**

Cover current private core, joint shadow pointer, current public pointer,
explicit rollback pointers, seven-day raw window, 30-day audits, and active
validation history. Reject symlink ancestors, symlink members, hard-linked
regular files, unexpected file types, unresolved references, `..`, absolute
members, and any candidate outside the dedicated roots.

- [ ] **Step 2: Write the protected-over-high-water test**

Make protected fixtures alone exceed the configured limit. Assert zero deletes,
`storage_pressure=true`, and `admit_new_run=false`. Make only unprotected old
fixtures exceed it and assert oldest-first deletion stops below the limit.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_retention -v`

Expected: FAIL because the retention module is absent.

- [ ] **Step 4: Implement fail-closed retention and verify GREEN**

Build and validate the complete protected set before the first deletion. Use
descriptor-relative operations within validated route roots. Return exact byte
counts, protected/deleted file counts, and stable reason codes.

Run: `python3 -m unittest tests.test_route_shadow_retention -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/route_shadow_retention.py tests/test_route_shadow_retention.py
git commit -m "feat(routes): bound shadow evidence retention"
```

Add a GitHub commit comment with protected-over-limit and path-safety evidence.

### Task 6: systemd units, exact schedule, and runtime limits

**Files:**
- Create: `deploy/systemd/cex-dex-route-shadow-user.service.in`
- Create: `deploy/systemd/cex-dex-route-shadow.timer`
- Create: `scripts/route_shadow_runtime.py`
- Create: `tests/test_route_shadow_runtime.py`
- Modify: `deploy/render_runtime_templates.py`
- Modify: `scripts/install_collection_timers.sh`
- Modify: `tests/test_deploy_templates.py`
- Modify: `tests/test_collection_framework.py`
- Modify: `tests/test_framework.py`

**Interfaces:**
- Produces rendered `cex-dex-route-shadow-user.service` and installed `cex-dex-route-shadow.timer`.
- Produces: `verify_runtime_limits(properties: Mapping[str, str], phase: str) -> dict`.

- [ ] **Step 1: Write failing rendered-unit tests**

Assert `Nice=15`, `KillMode=control-group`, `UMask=0077`,
`TimeoutStartSec=90s`, canary `CPUQuota=50%`, `MemoryMax=512M`, `TasksMax=16`,
`ExecStart` without any public-promotion flag, and `ExecStopPost` reconciliation.
Assert write paths include only market data and required runtime paths.

Use literal `systemctl show` property maps to reject inactive limits,
`MemoryMax=infinity`, wrong CPU quota, wrong TasksMax, wrong KillMode, and a
canary/full phase mismatch. A rendered unit alone must not set
`runtime_limits_verified=true`.

- [ ] **Step 2: Write failing timer tests**

Assert exact UTC entries `01..23:13,28,43,58:00` and `00:13:00`,
`Persistent=false`, `AccuracySec=1s`, and `RandomizedDelaySec=0`. If
`systemd-analyze` is available, compare the next 48 hours to a literal expected
trigger list; otherwise validate the exact unit fields in Python.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_deploy_templates tests.test_collection_framework tests.test_framework tests.test_route_shadow_runtime -v`

Expected: FAIL because the route units are absent.

- [ ] **Step 4: Implement templates and installer without enabling by default**

Render and install the files, but require an explicit
`--enable-route-shadow-canary` installer flag before `systemctl --user enable
--now`. Existing daily/depth behavior must remain byte-equivalent without the
flag.

- [ ] **Step 5: Verify GREEN**

Run the focused tests above and render into a temporary directory. Run
`systemd-analyze verify` when available. Feed captured `systemctl --user show`
fields through `verify_runtime_limits()` and assert no unresolved placeholders.

- [ ] **Step 6: Commit**

```bash
git add deploy/systemd/cex-dex-route-shadow-user.service.in deploy/systemd/cex-dex-route-shadow.timer deploy/render_runtime_templates.py scripts/install_collection_timers.sh scripts/route_shadow_runtime.py tests/test_deploy_templates.py tests/test_collection_framework.py tests/test_framework.py tests/test_route_shadow_runtime.py
git commit -m "feat(deploy): add bounded route shadow timer"
```

Add a GitHub commit comment with schedule and rendered resource-limit evidence.

### Task 7: Integrated shadow release evidence

**Files:**
- Modify: `docs/collection-operations.md`
- Modify: `docs/market-monitor-design.md`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_release_smoke.py`

**Interfaces:**
- Consumes Tasks 1-6.
- Produces release-check output for shadow absent, canary, full, storage-pressure, and promotion-ready states.

- [ ] **Step 1: Add failing release counterexamples**

Reject mismatched source/universe/core/audit hashes, stale joint pointer,
orphan core counted valid, unexplained ledger gaps, invalid phase widening,
missing cgroup verification, and promotion-ready claims with any incomplete
strict cost component. Shadow absence remains valid while the feature is not
enabled.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke -v`

Expected: FAIL on the new shadow release cases.

- [ ] **Step 3: Implement checker output and operator documentation**

Report exact run/leg/route numerators and denominators, phase, storage, ledger,
and gate failures. Document canary enable, read-only observation, full-phase
promotion, manual public promotion, rollback, timer disable, and evidence
retention commands. Never call a running canary a public opportunity feed.

- [ ] **Step 4: Run route and full suites**

Run:

```bash
python3 -m unittest tests.test_route_shadow_inputs tests.test_route_shadow_audit tests.test_run_route_shadow tests.test_route_shadow_gate tests.test_route_shadow_retention tests.test_release_smoke -v
python3 -m unittest discover -s tests -v
git diff --check
```

Expected: PASS with no warnings or leaked processes.

- [ ] **Step 5: Commit**

```bash
git add docs/collection-operations.md docs/market-monitor-design.md scripts/check_dashboard_release.py tests/test_release_smoke.py
git commit -m "docs(routes): operationalize shadow release checks"
```

Add a GitHub commit comment with focused/full-suite counts and explicit confirmation that `routes/latest.json` was not published.
