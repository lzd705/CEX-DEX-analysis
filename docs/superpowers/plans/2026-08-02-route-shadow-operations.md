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
- Internally captures each required source once as immutable bytes (or an
  exact private SQLite snapshot) and makes every parser consume that capture;
  no parser may reopen a source path after its identity was hashed.
- Produces: `selection_window(now: datetime) -> dict[str, str]`.
- Produces: `build_shadow_universe(data_dir: Path, now: datetime, *, static_token_config: Path) -> tuple[dict, dict]`, returning universe and source manifest.
- Produces: `write_run_universe(shadow_root: Path, run_id: str, universe: Mapping, source_manifest: Mapping) -> tuple[Path, Path]`.
- Writes the same run-scoped source manifest as
  `routes/shadow/runs/<run_id>/baseline_manifest.json`, including calculation
  version, filters, observation bounds, input paths, sizes, and SHA-256s.
- Requires the canonical published inputs `market_facts.sqlite3`,
  `cex_instrument_lifecycle.json`, `admin/token_registry.json`,
  `cex_exchange_volume_daily.csv`, `cex_depth_latest.csv`,
  `dex_depth_latest.csv`, `cex_execution_cost_latest.csv`,
  `dex_execution_cost_latest.csv`, and `dex_pool_tvl_latest.csv`, plus the
  tracked `config/tokens.csv` validation authority. The runtime registry must
  exist even when empty and use its canonical empty schema. The config
  argument must resolve exactly to tracked `PROJECT_ROOT/config/tokens.csv`;
  its manifest path is always the logical `config/tokens.csv`. The other nine
  manifest paths are fixed POSIX paths relative to `MARKET_DATA_DIR`. Absolute
  host paths are never serialized.

- [ ] **Step 1: Write failing window and source-identity tests**

Use a fixed `2026-08-02T13:00:00Z` clock and assert the literal window
`{"start": "2026-07-03", "end": "2026-08-01"}`. Mutating one byte of every
required source must change the canonical generation; missing, symlinked, or
non-regular required sources must fail before universe construction. Reject a
source whose descriptor identity changes while it is read. Changing only mtime
must not change the byte generation. Cover month/year/leap-day windows and
reject naive clocks. Mutate or replace every source path immediately after its
capture and assert the universe still comes only from the captured bytes whose
SHA is in the manifest; patch any later path reopen to fail the test.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m unittest tests.test_route_shadow_inputs -v`

Expected: FAIL because `scripts.route_shadow_inputs` does not exist.

- [ ] **Step 3: Implement exact source readers and generation hashing**

Read catalog identities from `market_facts.sqlite3` plus current lifecycle and
runtime registry files; read CEX daily Volume, CEX/DEX Depth, CEX/DEX Execution,
and DEX TVL/24h Volume from their published files. Verify SQLite integrity,
schema/current-state uniqueness, and that its committed CEX source name/SHA is
the exact daily CSV being aggregated; reject unbound WAL/journal sidecars.
Open every required regular file through a verified descriptor and, in the
same pass, hash and capture the exact bytes that will be parsed. Parse JSON and
CSV from those captured bytes. Copy the exact captured SQLite bytes into a
private bounded snapshot file and open only that copy read-only with immutable
mode; never hash one path generation and later parse another. Recheck source
descriptor and parent-directory identities after capture and fail closed on
change.
Project the production schemas into canonical adapter rows before calling
`build_route_universe()`: construct Market IDs for Depth/TVL, map CEX
`observed_at` and DEX `block_timestamp` to `state_observed_at`, retain the
single aligned Depth/Execution snapshot lineage, and derive DEX 24h Volume from
the same TVL snapshot row. Aggregate CEX `selected_window_usd` over the exact
30 complete UTC days and bind it to the SQLite import timestamp. Volume and
TVL accept genuine non-negative zero; missing stays null and Depth/execution
capacity remains strictly positive. Hash canonical manifest entries containing
relative path, byte size, and SHA-256; do not use mtime as identity or expose
absolute production paths. Except for the single tracked
`PROJECT_ROOT/config/tokens.csv` authority, Shadow reads only canonical files
under `MARKET_DATA_DIR`.

Before implementation, add explicit RED tests for every adapter projection,
SQLite integrity/current-state and CSV source-SHA mismatch, WAL/journal
presence, Depth/Execution snapshot-lineage mismatch, DEX TVL/24h Volume
same-row binding, and the distinction between a real numeric zero and missing
data. These contract tests must fail against the pre-Task-1 implementation.

- [ ] **Step 4: Add failing atomic run-universe tests**

Assert the destination is exactly
`routes/shadow/runs/<run_id>/route_universe.json`, cannot be overwritten, is
canonical JSON, and rereads to the same `route_universe_sha256()`. Reject run
IDs containing separators, dot segments, whitespace, or non-ASCII controls.
Assert `baseline_manifest.json` binds the same candidate generation and cannot
be replaced independently. Inject failure after either file write and before
directory commit; no final run directory may become visible. Race two writers
for the same run ID and require exactly one no-replace winner.

- [ ] **Step 5: Implement exclusive immutable publication and verify GREEN**

Create a hidden staging directory beneath the verified `runs` descriptor,
write both files with `O_CREAT|O_EXCL|O_NOFOLLOW`, `fsync` both files and the
staging directory, then atomically rename the whole directory into `<run_id>`
with no-replace semantics and `fsync` `runs`. Never expose a one-file partial
run. Reject symlink ancestors/members, hard-linked files, and changed directory
identity. Return both final paths.

Run: `python3 -m unittest tests.test_route_shadow_inputs tests.test_route_universe tests.test_framework -v`

Expected: PASS, including the repository's Python 3.8 grammar gate.

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
- Produces: `build_shadow_audit(cohort: Mapping, *, core_pointer: Mapping, run: Mapping, phase: str, audit_finished_at: str) -> dict`.
- Produces: `publish_shadow_result(shadow_root: Path, *, core_pointer: Mapping, audit: Mapping) -> dict`.
- Produces: `load_latest_shadow_result(shadow_root: Path) -> dict`.
- Uses audit schema `route_shadow_audit/v1`. The audit contains only facts
  knowable before joint-pointer commit: `run_id`, `phase`,
  `route_cohort_id`, exact core pointer/manifest hashes, universe/baseline
  hashes and candidate generation, `audit_finished_at`, leg availability, and
  route timing/age numerators, denominators, and percentiles. Joint-pointer
  success rate and complete run duration are later ledger/gate metrics and
  must not be guessed as `1/1` inside a prepublication audit.
- The audit permits exactly `schema`, `run_id`, `phase`, `route_cohort_id`,
  `core_pointer_sha256`, `core_manifest_sha256`,
  `route_universe_sha256`, `baseline_manifest_sha256`,
  `candidate_source_generation`, `audit_finished_at`, and `metrics`.
  `build_shadow_audit()` validates the supplied exact core pointer, extracts
  its cohort/manifest identity, and hashes its canonical pointer bytes;
  publication later proves that same pointer was the current private pointer
  at commit time.
- `phase` is exactly `canary` or `full`. The `run` mapping accepted by
  `build_shadow_audit()` has exactly the required `run_id`,
  `route_universe_sha256`, `baseline_manifest_sha256`, and
  `candidate_source_generation` bindings; unknown or missing keys fail.
- The joint pointer schema is exactly `route_shadow_pointer/v1` and permits no
  keys beyond `schema`, `run_id`, `phase`, `route_cohort_id`,
  `core_pointer_sha256`, `core_manifest_sha256`, `route_universe_sha256`,
  `baseline_manifest_sha256`, `candidate_source_generation`, and
  `audit_sha256`.
- Hash domains are literal: `route_universe_sha256` is the logical canonical
  object hash returned by `route_universe_sha256()`; core pointer, core
  manifest, baseline manifest, and audit hashes are SHA-256 of their exact
  installed canonical UTF-8 bytes. The returned joint-pointer SHA is SHA-256
  of the exact canonical bytes installed at `routes/shadow/latest.json`.
- Audit `metrics` has the exact keys `leg_availability`,
  `timing_availability`, `conditional_skew_sla`,
  `passing_skew_seconds_p95`, `passing_skew_seconds_max`,
  `route_age_seconds_p95`, and `route_age_seconds_max`. Ratio metrics use only
  `status,numerator,denominator,value`; percentile/max metrics use only
  `status,sample_count,value`. Metric status is exactly `evaluated` or
  `not_evaluated`.

- [ ] **Step 1: Write failing literal metric tests**

Cover zero, one, two, and twenty samples. Assert nearest-rank p95 uses
`ceil(0.95*n)`, availability is available/all legs, timing availability is
`within/all routes`, conditional skew is `within/(within+outside)`, unavailable
routes do not enter the conditional denominator, and empty denominators
serialize as
`{"status": "not_evaluated", "numerator": 0, "denominator": 0, "value": null}`.
For displayed ratios enter an explicit local Decimal context sized for the
input integers, quantize to 12 fractional places with `ROUND_HALF_EVEN`, strip
trailing zeros and the trailing dot, and normalize any zero to `"0"`; later
gates must compare the stored integer numerator/denominator, never the rounded
display string. An empty percentile/max sample serializes as
`{"status":"not_evaluated","sample_count":0,"value":null}`. Test `1/3`,
`1/2`, very large integers, and altered global Decimal contexts. Reject invalid
percentiles, non-finite values, and negative zero.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_audit -v`

Expected: FAIL because the audit module is absent.

- [ ] **Step 3: Implement metrics and strict schema validation**

Treat an `observed`/`partial` leg as available when `available is not False`;
do not require an invented literal `true`. Derive route age as
`audit_finished_at - min(buy_state, sell_state)` for every two-leg-available
route, including `research_only`. A research-only route may have timing status
`unavailable`, so it contributes age but not the conditional-skew denominator.
Only `within_sla` routes enter passing-skew percentiles; `outside_sla` and
`unavailable` do not. Preserve exact numerator/denominator integers and
canonical decimal strings. Reject duplicate route/leg IDs, missing lineage,
future state times, unknown statuses, and negative durations. Derive the
bounded venue/adapter label only from the canonical exchange or DEX component
of Market ID; do not trust an undefined row field.

- [ ] **Step 4: Add failing pointer atomicity tests**

The pointer must bind every exact field listed above. Assert the core,
universe, baseline, and audit agree on route inventory, selection window,
notional grid, candidate generation, and run/cohort identity, rather than
merely hashing unrelated objects. Inject failures before/during/after audit
installation, after core publication, during pointer replacement, and after
replace during fsync/reread. The prior pointer must remain readable whenever
the commit still owns the replacement; a concurrent third-party pointer must
never be overwritten during rollback. Publish shadow A, then publish private
core B without a shadow commit: the loader must still resolve immutable core A
by `route_cohort_id`; B remains an orphan and cannot count as a valid shadow.
Cover hash cross-wiring, symlink/hardlink/directory-swap, run-ID traversal, and
two concurrent publishers.

For every DEX leg, also require the core's USD-price source snapshot ID,
observed time, source, endpoint, and raw-response SHA to reproduce the exact
allowlisted `collector_context` embedded in the selected universe leg. Context
is source evidence, not a caller hint; a missing/extra field or lineage mismatch
invalidates the joint publication. For an observed leg, rebuild an unordered
address-to-price map from context `base_token_id`/`quote_token_id` and exact
prices, then require the core's `token0_address`/`token1_address` and
`token0_price_usd`/`token1_price_usd` to reproduce the same map. Copying source
labels while using different price inputs must fail. Also bind context
status/reason. Each Token ID must use the pool chain prefix and contain a
different canonical 20-byte EVM address.

Derive the universe and baseline paths exclusively from the Task 1 run-ID
validator as `<shadow_root>/runs/<audit.run_id>/route_universe.json` and
`baseline_manifest.json`; callers cannot supply arbitrary paths. Reject a
lexically safe file from another run/root as a cross-run binding attempt.

- [ ] **Step 5: Implement joint publication and verify GREEN**

Install the immutable audit under
`routes/shadow/runs/<run_id>/audit.json` through a hidden temporary file,
`fsync`, and no-replace atomic installation; a killed writer must not leave a
partial final audit. Fully reread and validate universe/baseline/audit plus the
immutable `routes/core/bundles/<route_cohort_id>` while holding the core
directory shared lock, and prove the supplied core pointer is the exact current
private pointer at commit time. Atomically replace only
`routes/shadow/latest.json` under the shadow-root exclusive lock. Generalize
the complete-pointer rollback commit helper so post-replace fsync/reread
failure restores the owned prior pointer; do not use the non-rollback
`_atomic_replace_pointer_at`. Return the exact committed shadow-pointer SHA so
Task 3 can persist it in the ledger. Reuse route-publication bounded-read and
path-safety rules rather than trusting caller objects; explicitly reject
hard-linked immutable inputs.

Run: `python3 -m unittest tests.test_route_shadow_audit tests.test_route_publication tests.test_framework -v`

Expected: PASS, including Python 3.8 grammar and all A/B orphan/rollback cases.

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
- Modify: `scripts/route_shadow_inputs.py`
- Modify: `scripts/route_universe.py`
- Modify: `tests/test_route_shadow_inputs.py`
- Modify: `tests/test_route_universe.py`

**Interfaces:**
- Produces: `run_shadow_once(data_dir: Path, now: datetime, phase: str, ...) -> dict`.
- Produces CLI subcommands: `run` and `reconcile`.
- Consumes Task 1 universe and Task 2 audit/pointer interfaces.
- Produces Task 1 public helpers
  `current_source_generation(data_dir, *, static_token_config) -> str` and
  `load_run_input_binding(shadow_root, run_id) -> dict`.
  `current_source_generation()` only rehashes the current ten inputs and never
  rebuilds a universe. `load_run_input_binding()` descriptor-safely rereads and
  fully validates the installed universe plus baseline and returns their exact
  hashes/generation; neither helper trusts caller-computed hashes.
- Every selected DEX leg embeds an exact-schema
  `route_collector_context/v1` derived only from the same captured TVL row.
  The context allows exactly `schema`, `snapshot_id`, `request_started_at`,
  `observed_at`, `response_received_at`, `status`, `reason_code`, `pool_name`,
  `base_token_id`, `quote_token_id`, `base_token_price_usd`,
  `quote_token_price_usd`, `tvl_method`, `source`, `source_endpoint`, and
  `raw_response_sha256`; it excludes `error`, paths, credentials, and unknown
  fields. Observed context requires complete ordered timestamps, two distinct
  parseable EVM token addresses with the pool chain prefix, two positive prices,
  and a 64-hex source hash. Status/reason combinations exactly match the TVL
  production contract `observed|missing|not_found|failed`; unavailable numeric
  values serialize as JSON null, never empty string or zero. Non-observed
  context retains its explicit lineage and can only produce a terminal
  unavailable leg. Context is serialized inside
  `route_universe.json` and therefore bound by its SHA.
- The terminal ledger record owns `lock_acquired`, complete run duration, and
  the exact committed shadow-pointer SHA. It counts a valid joint publication
  only after rereading that pointer by SHA; these values are never inferred
  from the prepublication audit.

- [ ] **Step 1: Write the lock-priority RED test**

Hold `collection/collection.lock`, run the real orchestrator with source readers
that raise if called, and assert exit zero plus one `skipped_locked` ledger
record and zero source calls. Assert the lock is held across universe build,
collection, private-core publication, audit, and joint pointer publication.
Extend `_ForkProcessExecutor`/`collect_route_cohort()` with an exact
`child_close_fds` contract. Before a fork child invokes any collector it must
close the inherited collection-lock descriptor. In a real process test, kill
or close the parent while a child remains blocked and prove a third process can
immediately acquire the collection lock; the orphan child must not retain it.
Fix the ledger/lock order: choose and validate run ID, then attempt the
collection lock nonblocking. A successful owner first closes older unterminal
entries, then publishes its own `started.json`. A busy invocation atomically
commits one run directory containing both `started.json` and
`terminal.json(outcome=skipped_locked)` and never exposes an unterminal skipped
run. Use barriers to prove two simultaneous invocations cannot mark each other
unexplained. If `ExecStopPost` knows an invocation ID but no ledger exists, it
creates bounded synthetic `unexplained` evidence for the killed-after-lock,
before-started window. Absence cannot prove whether the lock was acquired, so
synthetic terminal evidence uses `lock_acquired: null` with status
`not_evaluated`; normal owners write `true`, busy invocations write `false`,
and null blocks later gates. Test kill-before-lock and
kill-after-lock-before-start separately without inventing a boolean.

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
1, per chain 1: `PEPE,CAKE,SHIB,SUSHI,ZK,SNX,GRT,COMP,ENS,STRK`. Full uses
every eligible Token, workers 4, per venue/chain 1, with the same 60-second
deadline. The CLI exposes no deadline/worker bypass. Canary fails if any of the
ten Tokens has no route; it never silently shrinks the denominator. Scope the
universe first, then atomically persist it. Reload it exclusively through
`load_run_input_binding()` and use only that fully reread immutable universe
for collection and audit; mutating the pre-write Python object must have no
effect. Canary requires every Token to have at least one candidate route that
contains a proved, supported constant-product-V2 DEX leg; a CEX-only or
research-only route cannot satisfy canary coverage. Audit inventory cannot
describe a wider full candidate set.

Materialize CEX collector identity only through canonical Market ID and require
the exchange to exist in the live order-book adapter/`REQUESTED_LEVELS` map.
Materialize DEX identity plus the exact embedded collector context; never call
legacy `_resolve_inventory_legs()`, `load_pool_inventory()`, or any production
inventory loader after Task 1 capture. Patch those loaders to raise in an
integration test and exercise a real DEX collector preflight.
In the parent scheduler, a non-observed DEX context becomes a terminal leg with
stable reason `usd_price_context_<status>` and retains snapshot, effective
observed time, source, endpoint, and raw SHA. Do not submit its chain resolver,
DEX collector, or RPC work; tests patch both resolver and collector to raise.

Use dependency injection for collectors in tests, but exercise the real input,
publication, and ledger boundaries. Compare
`current_source_generation()` at collection start, collection completion,
immediately before private-core publication, and immediately before the joint
pointer. Every check must equal the immutable binding's original
`candidate_source_generation`; the first runtime rehash never becomes a new
baseline. Drift before core rejects the run; drift after core leaves only a core
orphan and never writes a joint pointer. The private collector executor receives
the collection-lock FD through `child_close_fds`.

- [ ] **Step 5: Implement durable ledger and reconciliation**

The ledger is descriptor-relative under
`routes/shadow/ledger/<run_id>/{started,terminal,service}.json`, never an
append-only JSONL. Install every exact-schema canonical event with
`O_EXCL|O_NOFOLLOW`, regular/nlink checks, file/directory `fsync`, and no
overwrite. `started` records explicit run ID, phase, invocation ID, UTC start,
boot ID, and `monotonic_ns`; `terminal` records exact outcome, lock-acquired,
duration status/value, cohort ID, and committed joint-pointer SHA; `service`
records only the normalized systemd result evidence. Runner/reconciler races
have exactly one terminal winner; identical retries are idempotent and
conflicting retries fail closed.

Write `started` before source reads and a terminal result after completion.
`reconcile --run-id ... --service-result ... --exit-code ... --exit-status ...`
must atomically close a started entry as success, failed, timeout, OOM, or
unexplained termination. A new run first closes any older unterminal entry as
unexplained only after it successfully acquires the collection lock; a busy
manual invocation must not close the still-running owner. `run` accepts an
explicit validated run ID, otherwise uses systemd `INVOCATION_ID`, otherwise
generates and prints one; Task 6 must pass the same ID to `ExecStopPost`.
If explicit `--run-id` and `INVOCATION_ID` both exist they must be identical;
manual runs serialize `invocation_id` as JSON null. Test literal systemd
normalization: `SERVICE_RESULT=success`, `EXIT_CODE=exited`, `EXIT_STATUS=0`
is only service-level success; `timeout` maps timeout, `oom-kill` maps OOM,
nonzero exit/signal/core-dump/watchdog/resources map failed, and empty or
unknown combinations produce unexplained/fail-closed evidence.
`SERVICE_RESULT=success` alone never proves success: terminal success requires
`load_latest_shadow_result()` to return the same run ID and exact committed
pointer SHA produced by Task 2. Persist exact started/finished times,
run duration, lock-acquired flag, route cohort ID, and committed shadow-pointer
SHA when available; a core-only orphan is not a valid joint publication.
Use monotonic duration within one boot. Reconcile without comparable boot and
monotonic evidence writes `not_evaluated`, which fails later gates; never derive
an SLA duration from wall clock. Normalize only literal success, timeout, OOM,
failed, and unexplained systemd combinations; unknown values fail closed.

- [ ] **Step 6: Verify GREEN and collector regressions**

Run: `python3 -m unittest tests.test_run_route_shadow tests.test_route_collection tests.test_route_shadow_inputs tests.test_route_universe tests.test_framework -v`

Expected: PASS, including generation drift, collector terminal rows, deadline,
lock contention, fork-FD release, source-bound DEX context, ledger races, and
Python 3.8 grammar.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_route_shadow.py scripts/collect_route_cohort.py scripts/route_shadow_inputs.py scripts/route_universe.py tests/test_run_route_shadow.py tests/test_route_shadow_inputs.py tests/test_route_universe.py
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
Compute valid-joint-pointer rate as ledger entries whose committed pointer SHA
can be reconstructed from that run's immutable audit, universe, baseline, and
immutable core bundle divided by lock-acquired runs. Historical verification
never compares an old SHA to the moving `routes/shadow/latest.json`; rebuild
the exact `route_shadow_pointer/v1` canonical bytes for each run and compare
their SHA to its ledger entry. Compute complete run-duration percentiles/max
from every lock-acquired terminal run, including success, failed, timeout, and
OOM; exclude `skipped_locked`. Do not read either value from a Task 2 audit.
Compare every ratio by integer cross multiplication, not its rounded display
text.

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
enabled. Reject a deployed Web point-file override that would make the
dashboard read a different publication from Shadow. `MARKET_CEX_DATA` and
`MARKET_DEX_DATA` must be unset while Shadow is enabled because either switches
the Web reader away from the SQLite commit point to an independently mutable
CSV path. Cover `MARKET_DATABASE`, `MARKET_TVL_DATA`,
`MARKET_CEX_DEPTH_DATA`, `MARKET_DEX_DEPTH_DATA`,
`MARKET_CEX_EXECUTION_COST_DATA`, and `MARKET_DEX_EXECUTION_COST_DATA`: every
variable must be unset or resolve exactly to its canonical file beneath the
same `MARKET_DATA_DIR`. Apply the same rule to
`MARKET_CEX_INSTRUMENT_LIFECYCLE` and `TOKEN_REGISTRY_PATH`, resolving to
`cex_instrument_lifecycle.json` and `admin/token_registry.json` respectively.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke -v`

Expected: FAIL on the new shadow release cases.

- [ ] **Step 3: Implement checker output and operator documentation**

Report exact run/leg/route numerators and denominators, phase, storage, ledger,
and gate failures. Document canary enable, read-only observation, full-phase
promotion, manual public promotion, rollback, timer disable, and evidence
retention commands. Validate the Web/Shadow point-file environment equivalence
before canary enable and in release checking. Never call a running canary a
public opportunity feed.

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
