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

**Status on this branch:** completed and already pushed as `a855e40` (`feat(routes): bind shadow universe to source hashes`), with corrective commits
`ae6863c` (`fix(routes): seal shadow input evidence`) and `2c41d9c`
(`fix(routes): rollback swapped shadow runs`). The historical RED/GREEN steps
below are retained as execution evidence and are not rerun as a new commit;
active execution resumes at Task 2 from HEAD `2c41d9c`.

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
  its manifest path is always the logical `config/tokens.csv`. The other eight
  manifest paths are fixed POSIX paths relative to `MARKET_DATA_DIR`. Absolute
  host paths are never serialized.
- Freeze source-read limits exported by Task 1:
  `MAX_SQLITE_BYTES=192*1024**2`, `MAX_SOURCE_BYTES=32*1024**2` for each of the
  other eight data-dir inputs, `MAX_CONFIG_BYTES=4*1024**2` for tracked
  `config/tokens.csv`, and `MAX_AGGREGATE_SOURCE_BYTES=256*1024**2` across all
  captured inputs. Every descriptor reader stops after at most its limit plus
  one sentinel byte; aggregate accounting occurs before parsing/materializing.
  Exact-limit fixtures pass, every +1-byte fixture fails without reading the
  remainder, and Task 5 imports these constants rather than restating a
  different snapshot boundary.

- [x] **Step 1: Write failing window and source-identity tests**

Use a fixed `2026-08-02T13:00:00Z` clock and assert the literal window
`{"start": "2026-07-03", "end": "2026-08-01"}`. Mutating one byte of every
required source must change the canonical generation; missing, symlinked, or
non-regular required sources must fail before universe construction. Reject a
source whose descriptor identity changes while it is read. Changing only mtime
must not change the byte generation. Cover month/year/leap-day windows and
reject naive clocks. Mutate or replace every source path immediately after its
capture and assert the universe still comes only from the captured bytes whose
SHA is in the manifest; patch any later path reopen to fail the test.

- [x] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m unittest tests.test_route_shadow_inputs -v`

Expected: FAIL because `scripts.route_shadow_inputs` does not exist.

- [x] **Step 3: Implement exact source readers and generation hashing**

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

- [x] **Step 4: Add failing atomic run-universe tests**

Assert the destination is exactly
`routes/shadow/runs/<run_id>/route_universe.json`, cannot be overwritten, is
canonical JSON, and rereads to the same `route_universe_sha256()`. Reject run
IDs containing separators, dot segments, whitespace, or non-ASCII controls.
Assert `baseline_manifest.json` binds the same candidate generation and cannot
be replaced independently. Inject failure after either file write and before
directory commit; no final run directory may become visible. Race two writers
for the same run ID and require exactly one no-replace winner.

- [x] **Step 5: Implement exclusive immutable publication and verify GREEN**

Create a hidden staging directory beneath the verified `runs` descriptor,
write both files with `O_CREAT|O_EXCL|O_NOFOLLOW`, `fsync` both files and the
staging directory, then atomically rename the whole directory into `<run_id>`
with no-replace semantics and `fsync` `runs`. Never expose a one-file partial
run. Reject symlink ancestors/members, hard-linked files, and changed directory
identity. Return both final paths.

Run: `python3 -m unittest tests.test_route_shadow_inputs tests.test_route_universe tests.test_framework -v`

Expected: PASS, including the repository's Python 3.8 grammar gate.

Also run the changed modules and their focused tests under a real CPython
3.8.10 runtime, not only the repository's AST grammar check:
`python3.8 -m unittest tests.test_route_shadow_inputs tests.test_route_universe -v`,
then explicitly import `scripts.route_shadow_inputs` and
`scripts.route_universe`. Missing CPython 3.8.10 or any runtime import failure
blocks this task's commit.

- [x] **Step 6: Commit**

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
- Modify: `tests/test_route_publication.py`

**Interfaces:**
- Produces: `nearest_rank(values: Iterable[Decimal], percentile: Decimal) -> str | None`.
- Produces: `build_shadow_audit(cohort: Mapping, *, core_pointer: Mapping, run: Mapping, phase: str, audit_finished_at: str) -> dict`.
- Produces: `publish_shadow_result(shadow_root: Path, *, core_pointer: Mapping, audit: Mapping) -> dict`.
- Produces: `load_shadow_result(shadow_root: Path, *, run_id: str, expected_pointer_sha256: str) -> dict`.
- Produces: `load_latest_shadow_result(shadow_root: Path) -> dict`.
- Produces shared exact phase helpers `load_active_phase_state(shadow_root) -> dict`
  and `load_historical_phase_state(shadow_root, *, phase: str, phase_state_sha256: str, phase_transition_id: Optional[str]) -> dict`.
- Both phase helpers return the same non-serialized exact outer view with only
  `phase`, `phase_state_sha256`, `phase_transition_id`, and `state`. `state` is
  JSON null only for implicit canary; full returns exact
  `route_shadow_phase/v1` and requires
  `state.transition_id == phase_transition_id`. A full transition ID is a
  lowercase 64-hex SHA-256 path member, never arbitrary bounded text.
- Uses audit schema `route_shadow_audit/v1`. The audit contains only facts
  knowable before joint-pointer commit: `run_id`, `phase`,
  `route_cohort_id`, exact core pointer/manifest hashes, universe/baseline
  hashes and candidate generation, `audit_finished_at`, leg availability, and
  route timing/age numerators, denominators, and percentiles. Joint-pointer
  success rate and complete run duration are later ledger/gate metrics and
  must not be guessed as `1/1` inside a prepublication audit.
- The audit permits exactly `schema`, `run_id`, `phase`, `route_cohort_id`,
  `phase_state_sha256`, `phase_transition_id`,
  `core_pointer_sha256`, `core_manifest_sha256`,
  `route_cost_evidence_sha256`,
  `route_universe_sha256`, `baseline_manifest_sha256`,
  `candidate_source_generation`, `audit_finished_at`, and `metrics`.
  `build_shadow_audit()` validates the supplied exact core pointer, extracts
  its cohort/manifest identity, and hashes its canonical pointer bytes;
  publication later proves that same pointer was the current private pointer
  at commit time.
- `phase` is exactly `canary` or `full`. The `run` mapping accepted by
  `build_shadow_audit()` has exactly the required `run_id`,
  `phase_state_sha256`, `phase_transition_id`, `route_universe_sha256`,
  `baseline_manifest_sha256`, and
  `candidate_source_generation`, plus exact `route_cost_evidence_sha256`,
  bindings; unknown or missing keys fail.
- The joint pointer schema is exactly `route_shadow_pointer/v1` and permits no
  keys beyond `schema`, `run_id`, `phase`, `route_cohort_id`,
  `phase_state_sha256`, `phase_transition_id`,
  `core_pointer_sha256`, `core_manifest_sha256`, `route_universe_sha256`,
  `route_cost_evidence_sha256`, `baseline_manifest_sha256`,
  `candidate_source_generation`, and
  `audit_sha256`.
- Hash domains are literal: `route_universe_sha256` is the logical canonical
  object hash returned by `route_universe_sha256()`; core pointer, core
  manifest, baseline manifest, and audit hashes are SHA-256 of their exact
  installed canonical UTF-8 bytes. `route_cost_evidence_sha256` is the
  physical SHA-256 of the immutable Shadow sidecar
  `routes/shadow/runs/<run_id>/route-cost-evidence.json`; it does not alter the
  frozen `route_cohort_core/v1` five-file inventory, manifest, or pointer.
  Legacy core v1 therefore remains readable, while a joint Shadow pointer
  without this exact sidecar binding is not candidate-ready. `phase_state_sha256` is the exact installed
  `phase.json` byte hash, or, while that file is absent, SHA-256 of the literal
  ASCII domain string `route-shadow-phase/implicit-canary/v1\n`. The returned joint-pointer SHA is SHA-256
  of the exact canonical bytes installed at `routes/shadow/latest.json`.
- `phase_transition_id` is JSON null only for the implicit canary state and is
  lowercase 64-hex SHA-256 for full. `load_active_phase_state()` accepts absence
  only as implicit canary. A present `phase.json` must be exact canonical
  `route_shadow_phase/v1`, byte-identical to
  `transitions/<phase_transition_id>.json`, and its exact gate artifact must
  reproduce `gate_evidence_sha256`. Publication revalidates this active state
  at the final pointer boundary.
- Historical loading never compares a stored pointer with current active phase.
  It reconstructs implicit canary from the fixed domain hash; a full pointer
  resolves its immutable transition by `phase_transition_id`, checks the exact
  state-byte hash and gate artifact, and returns that historical phase. Thus a
  canary pointer remains readable after canary-to-full transition but cannot be
  counted as full.
- `publish_shadow_result()`, `load_shadow_result()`, and
  `load_latest_shadow_result()` return the same non-serialized exact outer view
  with only `pointer`, `pointer_sha256`, `audit`, `audit_sha256`, `cohort`, and
  `manifest`. `pointer` itself remains the exact persisted schema and never
  gains a convenience SHA field. The historical loader rebuilds canonical
  pointer bytes from immutable run/audit/universe/baseline/core/phase evidence
  and checks `expected_pointer_sha256`; the latest loader only snapshots latest,
  calls that same path, and rechecks pointer ownership.
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
`(within+outside)/all routes`, conditional skew is
`within/(within+outside)`, unavailable
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
future state times, unknown statuses, and negative durations.
`audit_finished_at` is canonical UTC `YYYY-MM-DDTHH:MM:SSZ` and cannot precede
the cohort's canonical `collection_completed_at`. Every evaluated
percentile/max value is finite, nonnegative plain decimal text with no exponent,
stripped redundant trailing zeros, and rejected negative zero. The current
exact audit schema intentionally contains no venue/adapter breakdown; do not
invent an unpersisted label.

- [ ] **Step 4: Add failing pointer atomicity tests**

The pointer must bind every exact field listed above. Assert the core,
universe, baseline, route-cost evidence, and audit agree on route inventory, selection window,
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

Derive the active phase identity from `shadow_root/phase.json` at publication:
absence must reproduce the literal implicit-canary hash and requires
`phase=canary`; a present canonical exact-schema state must byte-match its
immutable transition and reproduce the referenced canonical gate artifact
before it is hashed, and its declared phase/transition ID must match
audit/run/pointer. A caller-provided random
phase hash or a phase file that changes before commit fails closed. Task 3's
global collection lock prevents a legitimate transition during publication;
Task 2 still revalidates the descriptor-bound phase identity at the final
shadow-pointer boundary.

Test canary pointer A, then an exact valid full transition with no full cohort:
`load_latest_shadow_result()` still returns historical canary A, while a full
gate cannot count A. Reject a full `phase.json` with missing/mismatched
transition or gate artifacts and reject noncanonical but logically equivalent
phase bytes. Historical validation follows the pointer's own immutable phase
identity, not moving active state.

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

Task 2 RED fixtures already use exact `route_collector_context/v1`; a DEX leg
without it fails closed even though Task 3 is its eventual production writer.
Task 3 must copy the complete validated context object unchanged into every DEX
core leg. Exact redundant mappings are:
`context.snapshot_id == usd_price_source_snapshot_id`,
`context.observed_at == usd_price_observed_at`,
`context.source == usd_price_source`,
`context.source_endpoint == usd_price_source_endpoint`, and
`context.raw_response_sha256 == usd_price_raw_response_sha256`. These names are
TVL/USD-price lineage, never DEX RPC/block lineage. The original object also
preserves request/response times, `tvl_method`, status, and production
`reason_code` by canonical deep equality.

For non-observed context, require DEX leg `available=false`, leg
`reason_code=usd_price_context_<context.status>`, and the unchanged production
context/reason. Add exactly `usd_price_context_missing`,
`usd_price_context_not_found`, and `usd_price_context_failed` to the closed leg
reason set, not the route timing reason set. Route timing remains reproducible
as `buy_leg_unavailable` or `sell_leg_unavailable` from the existing classifier;
Task 2 validates both layers. No numeric address-price evidence may appear. For
observed context, independently reproduce the unordered token0/token1
address-price map in addition to exact object equality.

Derive the universe and baseline paths exclusively from the Task 1 run-ID
validator as `<shadow_root>/runs/<audit.run_id>/route_universe.json` and
`baseline_manifest.json`; callers cannot supply arbitrary paths. Reject a
lexically safe file from another run/root as a cross-run binding attempt.

Run the new atomicity/lineage corpus before implementation:
`python3 -m unittest tests.test_route_shadow_audit tests.test_route_publication -v`.
Expected: FAIL on the first missing joint-pointer/phase-lineage behavior; a
passing run here means the RED fixtures are not exercising the new contract.

- [ ] **Step 5: Implement joint publication and verify GREEN**

Install the immutable audit under
`routes/shadow/runs/<run_id>/audit.json` through a hidden temporary file,
`fsync`, and no-replace atomic installation; a killed writer must not leave a
partial final audit. Fully reread and validate universe/baseline/audit plus the
immutable `routes/core/bundles/<route_cohort_id>` while holding the core
directory shared lock, plus the descriptor-safe no-replace Shadow sidecar
`routes/shadow/runs/<run_id>/route-cost-evidence.json`, and prove the supplied core pointer is the exact current
private pointer at commit time. Descriptor-safely bind and revalidate the exact
active phase state described above. Atomically replace only
`routes/shadow/latest.json` under the shadow-root exclusive lock. Generalize
the complete-pointer rollback commit helper so post-replace fsync/reread
failure restores the owned prior pointer; do not use the non-rollback
`_atomic_replace_pointer_at`. Return the exact committed shadow-pointer SHA so
Task 3 can persist it in the ledger. Reuse route-publication bounded-read and
path-safety rules rather than trusting caller objects; explicitly reject
hard-linked immutable inputs.

Run: `python3 -m unittest tests.test_route_shadow_audit tests.test_route_publication tests.test_framework -v`

Expected: PASS, including Python 3.8 grammar and all A/B orphan/rollback cases.

Also run `python3.8 -m unittest tests.test_route_shadow_audit
tests.test_route_publication -v` with real CPython 3.8.10 and explicitly
import `scripts.route_shadow_audit` and `scripts.route_publication`. Missing
that runtime or any import failure blocks this task's commit.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_shadow_audit.py scripts/route_publication.py tests/test_route_shadow_audit.py tests/test_route_publication.py
git commit -m "feat(routes): publish auditable shadow readiness"
```

Add a GitHub commit comment with denominator, percentile, and failure-injection evidence.

### Task 3: Non-blocking shadow orchestrator and run ledger

**Files:**
- Create: `config/route_cost_adapters.json`
- Create: `config/route_cost_connector_keys.json`
- Create: `scripts/run_route_shadow.py`
- Create: `scripts/collection_lock_evidence.py`
- Create: `scripts/route_shadow_authority.py`
- Create: `scripts/route_cost_evidence.py`
- Create: `tests/test_run_route_shadow.py`
- Create: `tests/test_route_shadow_authority.py`
- Create: `tests/test_route_cost_evidence.py`
- Modify: `scripts/collect_route_cohort.py`
- Modify: `scripts/dex_route_costs.py`
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/run_collection_cycle.py`
- Modify: `scripts/route_shadow_inputs.py`
- Modify: `scripts/route_universe.py`
- Modify: `scripts/route_publication.py`
- Modify: `tests/test_route_shadow_inputs.py`
- Modify: `tests/test_route_universe.py`
- Modify: `tests/test_collection_cycle.py`
- Modify: `tests/test_route_collection.py`
- Modify: `tests/test_dex_route_costs.py`
- Modify: `tests/test_fetch_dex_depth.py`
- Modify: `tests/test_route_publication.py`

**Interfaces:**
- Produces fixed public: `run_shadow_once(data_dir: Path, *, expected_phase: Optional[str] = None) -> dict`.
- Produces CLI subcommands: `run` and `reconcile`.
- The state-changing public wrapper and `run` CLI expose no clock/callable,
  collector, source, `--now`, or timestamp override. After it
  owns the collection lock, `run_shadow_once()` samples the trusted UTC clock
  exactly once and uses that instant for the selection window, source binding,
  and run evidence. A module-private identity-capability harness alone may
  inject clock/collectors in unit tests; production imports/signatures reject
  those keywords before mutation. Production argv cannot backdate a
  schedule-linked worker.
- Consumes Task 1 universe and Task 2 audit/pointer interfaces.
- Produces shared `load_committed_route_shadow_authority(data_dir: Path) -> dict`.
  Its public/production form has no unit, property, timeout, or executable
  arguments. A sealed module-private test seam accepts one no-argument
  `AuthorityLiveProbe` whose only operation returns the fixed bounded raw
  daily/depth timer+service projection; the default implementation alone runs
  the fixed `LC_ALL=C systemctl --user show` argv/property lists and trusted
  clock. Production call sites and CLIs cannot pass or configure this seam.
- Loads the phase authority only after acquiring the global collection lock.
  Missing `routes/shadow/phase.json` means canary and uses the literal implicit
  phase hash domain defined in Task 2. A supplied `expected_phase` is only an
  equality assertion; it cannot change phase. A present phase file must match
  the exact Task 4 schema even before full mode is enabled. Persist the loaded
  phase, `phase_state_sha256`, and nullable `phase_transition_id` in
  started/audit/joint evidence.
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
  from the prepublication audit. Every acquired run also installs immutable
  `verification.json` evidence and the terminal record binds its exact byte
  SHA. Verification uses the closed `primary_failure_class` enum `none`,
  `transient_collection`, `source_generation_drift`, `lineage_invalid`,
  `unsafe_path`, `resource_limit`, `oom`, `timeout`, `orphan_core`,
  `pointer_interference`, `runtime_limits_unverified`, or `unexplained`.
  It also contains exact nonnegative bounded counts for collector processes
  started/reaped, orphan collector processes, primary-publication
  interference, core-only orphans, lineage/unsafe-path/generation/resource
  errors, and pointer interference. These are orthogonal: a
  `core_orphan_count` makes that run invalid and enters the valid-rate
  denominator, but is not an orphan process and is not itself a global
  zero-tolerance error. `orphan_process_count` and
  `primary_publication_interference_count` must be zero for every gate.
  Missing/unknown classes or counts and a missing or non-reproducing verification record
  are `not_evaluated`, never an implicit transient failure or success. The
  record also contains exact nullable `typed_source_manifest_sha256` and
  `route_cost_evidence_sha256`; every acquired Shadow run requires both
  reproducing lowercase 64-hex values, while a pre-Task3/legacy record uses
  null and is not candidate-ready. The latter is the physical SHA of the exact
  immutable Shadow run sidecar, not a caller-provided logical digest. It
  contains bounded stage/result facts, not arbitrary exception text.

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
Track every spawned collector PID to a terminal reaped state and write exact
started/reaped/orphan counts. Real-process tests cover normal exit, exception,
deadline, parent interruption, and later Task 6 `KillMode=control-group`; a
successful run cannot claim zero orphans from an unobserved process set.

While Shadow owns the lock, write descriptor-safe bounded owner evidence into
the lock file with owner kind, run ID, boot ID, and nonce. Extend the primary
`run_collection_cycle.py` busy-lock path through shared
`collection_lock_evidence.py`: every primary busy-lock result atomically
installs either its immutable contention receipt at
`routes/shadow/primary-contention/<primary_invocation_id>.json`, even if the
holder cannot yet be attributed, or the single bounded overflow marker defined
below and returns hard error.
This receipt behavior is active only when
`load_committed_route_shadow_authority()` returns exact enabled status. The
loader returns `route_shadow_authority_view/v1` with only
`schema,status,transaction_id,authority_sha256,
primary_unit_projection_sha256,reason_code`, where status is
`enabled|disabled|invalid`. In Task 3 it descriptor-loads canonical
`routes/shadow/operational/enabled.json`, recognizes absence/canonical false as
disabled, and deliberately returns invalid reason
`enable_contract_not_available` for every true record because Task 5 storage B
and Task 6 transaction evidence do not exist yet. Thus Task 3 can independently
GREEN and cannot enable feature-on behavior through a test-shaped three-field
file; disabled/invalid use null primary projection. Task 6 upgrades this same module: true must replay the named transaction's
exact identity, prepared record, admitted storage B, stage owner, terminal
`outcome=committed`, exact final-live-proof bytes/SHA, and exact `armed.json`
binding that terminal SHA, plus the immutable successful `activation.json`
receipt bound to terminal/marker; all
hashes/desired state must agree and no pending/
conflicting/interference control transaction may exist. It also descriptor-
rereads both managed drop-ins and fixed live systemd configuration, recomputes
the primary projection SHA bound by the terminal, and validates timer/service
state under the closed entry matrix. A deleted/replaced drop-in, daemon-reload
drift, timer Unit/calendar/accuracy/persistence change, or effective service
command/manual-start-policy mismatch returns invalid and every Shadow entry
makes zero operational/source calls. Dispatcher, worker, ops, primary receipt
mode, and release code may consume only this live-replaying helper.
The three-field enabled file alone is never authority. Before Task 6, absence
or the legacy/genesis canonical false with `transaction_id=null` is disabled,
while malformed, true-without-
terminal, aborted, mismatched, or unsafe evidence is invalid. Disabled or
invalid authority prevents every Shadow worker from
starting; the primary busy-lock path then preserves its pre-feature exit and
writes no Shadow files. A malformed authority is reported by Shadow health/
release checks but never causes an otherwise unchanged primary-primary lock
collision to fail. Task 6 atomically installs true before enabling units and
false only after stopping them. Feature-off tests prove byte-equivalent primary
busy behavior and zero Shadow writes. Task 3 tests the contention writer as a
pure exact-boundary component; Task 6 owns the first feature-on integration and
both unattributed owner-publication race receipts.
Primary contention, dispatcher, worker, ops, retention, and release code may
call only this shared loader; direct parsing of `enabled.json` is patched to
raise in integration tests. Task 6 barriers at true-before-terminal, aborted terminal,
transaction drift, and conflicting pending state prove zero Shadow work and
legacy primary behavior until the complete committed closure exists.
Absence or a structurally valid false control head returns disabled without
invoking the live probe. An
exact true closure invokes it once and validates the bounded output; timeout,
exception, malformed/extra properties, clock/boot mismatch, or nonzero command
status returns invalid, never stale enabled. Tests use only the sealed
no-argument probe to cover drop-in/property drift, timeout, and exact argv;
they cannot select a different unit or property set.
Task 6's false loader still descriptor-replays the immutable control journal:
a nonnull false transaction ID must be the unique committed or quarantined
disable head and its identity must name the prior true/false head; null is valid
at genesis whenever no committed/quarantined child exists, even if canonical
aborted attempts share that genesis parent. A quarantined head proves false
authority and all Shadow units stopped but carries repair-required primary-unit
evidence; it is disabled, never enabled or promotion-ready. This replay performs
no systemd/live probe I/O. Stale D1
bytes transplanted after a later D2, duplicate committed children, a false
pointer with a true head, or any ABA mismatch returns invalid. Tests execute
`true(T1) -> false(D1) -> true(T2) -> false(D2)`, reject D1 restoration, prove
re-enable starts only from D2, and make repeated disable idempotent.
Freeze the live-state matrix used by that probe. Both fixed primary timers must
always be `UnitFileState=enabled,ActiveState=active,SubState=waiting` under true
authority. Each same-basename oneshot service must be
`UnitFileState=static` and is valid either as `inactive/dead`, or as
`activating/start` with a nonzero lowercase 32-hex InvocationID, a current-boot
monotonic ExecMain start, and exact causal correspondence to that timer's most
recent trigger. Daily and depth may both be in that latter state only when each
independently satisfies its own trigger/invocation evidence; this bounded
overlap does not relax the primary-intent lock. `active/running`,
`deactivating/*`, `failed/failed`, unknown states, an activating service with
stale/missing trigger identity, an inactive timer, or any other combination is
invalid. Thus a scheduled primary process can call the shared authority loader
while its own oneshot unit is legitimately activating and still emit its
receipt; ordinary idle calls see both services inactive. Fixtures cover idle,
daily-self, depth-self, valid two-profile overlap, mismatched invocation,
failed service, disabled timer, and post-commit state drift.
Schema `route_shadow_primary_contention/v1` permits only `schema`,
`attribution_status`, `holder_run_id`, `holder_boot_id`, `holder_nonce`, `primary_profile`,
`primary_invocation_id`, `observed_at`, and `lock_identity`; profile is the
literal daily/depth production cycle, IDs/nonces are bounded canonical text,
and lock identity binds the opened regular lock file. `attribution_status` is
exactly `shadow|unattributed`; holder fields are canonical only for a validated
shadow lease and otherwise all JSON null. Install with
descriptor-relative `O_EXCL|O_NOFOLLOW`, nlink/ancestor checks, and fsync. The
primary runner returns its existing `skipped_locked` only after the receipt is
durable; receipt failure is a hard collection-framework error, not an
unrecorded skip. The gate enumerates the global receipts independently:
attributed Shadow receipts are hard interference and `unattributed` is
`not_evaluated`/hard-blocking, so the `flock`-acquired-before-owner-write and
owner-cleared-before-unlock windows cannot disappear. For each attributed run,
the receipt count must equal verification. Real-process tests start primary daily/depth collection while
Shadow holds the lock and prove the hard counter increments; nonoverlapping
scheduling proves zero. Never infer no interference from Shadow exit status.
Shadow clears and fsyncs only its owned nonce before unlocking; stale bytes
without an actually busy lock and unrelated lock owners never create a receipt.
Barrier tests freeze both owner-publication/clear windows and require an
unattributed durable receipt rather than a false zero; stale owner bytes without
an actually busy lock create no receipt.

`primary_invocation_id` full-matches lowercase `[0-9a-f]{32}` before any path
operation. A systemd primary uses its validated `INVOCATION_ID`; a direct
primary invocation generates 16 random bytes and lowercase-hex encodes them,
never accepts caller path text. Separators, dots, `%`, `@`, uppercase,
Unicode/control bytes, and wrong length fail before `openat`. Holder run/boot/
nonce fields use their separately frozen grammars and are never substituted
into a filename.

Introduce descriptor-verified `collection/primary-intent.lock` as a lock-order
guard, not a publication file. Every daily/depth runner takes it exclusively
before attempting `collection.lock` and holds it through collection-lock
release and its primary receipt. Desired-true enable, disable reconciliation,
normal promotion, and rollback final state changes also take it exclusively
before `collection.lock`; no ordinary Shadow collection worker uses it. A
normal state changer attempts intent nonblocking and returns stable
`primary_active` with zero mutation when primary already owns it. Disable first
stops all Shadow units without either lock. If primary-intent is busy, it
writes no control journal, guard, identity, drop-in, or authority byte because
no valid guarded transaction exists yet; it returns exact
`primary_active_disable_not_journaled` and never restarts work. The prior true
authority becomes live-invalid because its required route units are stopped,
so every Shadow entry remains zero-work. A later disable invocation must obtain
a fresh guard and create a new canonical transaction from the same control
head. Tests cover intent-busy, kill after stop but before intent, no phantom
pending record, live-invalid authority, and successful fresh retry.

After intent acquisition, promotion/rollback have one 31-second monotonic hold
budget; enable/disable have one 60-second budget including any owned rollback.
All network/systemctl calls have sub-deadlines within that total. The exact
primary schedule guard must prove the next earliest trigger is strictly after
the hold deadline before collection lock acquisition. At the deadline, the
operation aborts/restores its owned prior safe state, releases all locks, and
cannot commit; an over-budget rollback becomes visible interference but still
releases the OS locks. SIGKILL releases flock and the immutable transaction
journal reconciles next invocation. The fixed order is primary-intent ->
collection -> routes; no code acquires them in reverse.

Primary receipts include `intent_requested_at,intent_acquired_at,
intent_released_at,intent_wait_milliseconds`, so unexpected waiting is not
hidden from its schedule SLA. Ordinary feature-off primary output/exit remains
unchanged and an uncontended intent lock writes no Shadow evidence beyond the
feature-on receipt. Real-process tests cover nonblocking primary-first,
state-change-first at exact hold max/max+1, a primary arriving at the next
trigger, SIGKILL release, and reverse-order rejection.

Make the 4 MiB contention budget enforceable at the writers, not only in the
next retention pass. Canonical receipt bytes are at most 2 KiB. Reserve the
final 4 KiB of the 4 MiB root cap for one exact no-replace `overflow.json`;
normal receipt data may therefore total at most `4 MiB - 4 KiB`. Every feature-on
busy writer descriptor-opens a separate canonical
`primary-contention/.cap.lock`, takes its exclusive flock, rejects symlink/
hardlink/unknown members, recounts exact regular-file bytes, and only writes a
receipt when `current_receipt_bytes + candidate_size` stays within that data
limit. It fsyncs the receipt/root before releasing the cap lock. The lock is
independent of `collection.lock` and contains no owner-controlled payload.

If the next receipt would exceed the data limit, the lock holder instead
installs the sole at-most-4-KiB marker schema
`route_shadow_primary_contention_overflow/v1` with only
`schema,first_rejected_invocation_id,observed_at,cap_bytes,observed_receipt_bytes,reason_code`,
literal cap `4194304`, and reason
`primary_contention_receipt_capacity_exhausted`; then it returns a hard
collection-framework error rather than `skipped_locked`. Once the marker
exists, every later busy writer makes no additional file and returns the same
hard class. The marker is permanently protected and makes the gate/release
`not_evaluated` until a future explicit resolution contract; it cannot be
silently pruned. Real concurrent N/N+1 writers prove the cap lock admits the
last fitting receipt exactly once, creates one marker, never crosses 4 MiB,
and never loses the hard-block evidence.

Also make primary priority observable when feature-on, instead of assuming the
large systemd kill ceiling is its normal lock-hold SLA. Every systemd daily or
depth invocation installs one immutable at-most-4-KiB receipt under
`routes/shadow/primary-runs/<primary_invocation_id>.json`, schema
`route_shadow_primary_run/v1`, with only
`schema,primary_profile,primary_invocation_id,trigger_status,scheduled_for,
started_at,intent_requested_at,intent_acquired_at,intent_released_at,
intent_wait_milliseconds,lock_acquired_at,lock_released_at,finished_at,status,
lock_hold_milliseconds,collection_manifest_projection,
collection_manifest_projection_sha256,contention_receipt_sha256,
reason_code`. Profile is exactly `daily|depth`; trigger status is
`scheduled|manual|invalid`; status is
`succeeded|failed|skipped_locked|unexplained`. A scheduled invocation derives
the sole preceding literal timer instant and must begin within its existing
`AccuracySec=1min` window. Manual direct CLI receipts are explicit and never
enter readiness; a systemd invocation without one exact schedule is invalid
and hard-blocking. All results require ordered intent times and canonical
nonnegative intent wait milliseconds; acquired results additionally require
ordered collection-lock acquisition/release times, nonnegative exact
millisecond hold, and an exact final collection-manifest projection;
skipped results require null lock/manifest facts and the exact contention SHA.
No missing timestamp is converted to zero.

Freeze that embedded object as
`route_primary_collection_manifest_projection/v1`, with only
`schema,primary_profile,source_run_id,source_manifest_sha256,
source_schema_version,source_profile,source_status,source_publish_local,
source_started_at,source_finished_at,source_step_names,source_step_statuses`.
It is built once from the same descriptor-read exact source manifest bytes;
`source_manifest_sha256` is their physical SHA, schema version is literal `1`,
profile equals the receipt profile, status is `succeeded`, publish-local is
true, ordered timestamps are canonical, and names/statuses equal respectively
the fixed daily or depth `PROFILE_STEPS` list and all `succeeded`. The source
run ID full-matches the production timestamp-plus-8-hex grammar. Canonical
projection bytes are at most 2 KiB and their physical SHA equals the receipt's
redundant field. Failed/skipped receipts use JSON null for both projection
fields under their closed reason matrix. The receipt is the durable historical
copy: replay never follows, retains, or trusts a mutable/latest manifest path.

The primary-run root has its own descriptor-safe cap lock and exact 1 MiB cap:
normal receipt bytes may occupy at most `1 MiB - 4 KiB`, with the final 4 KiB
reserved for one permanent `route_shadow_primary_run_overflow/v1` marker.
Unknown members, unsafe IDs, overflow, or an unterminal scheduled invocation
hard-block gate/release. Current-window unreferenced receipts are rolling
operational bytes; any receipt bound by a historical gate moves to main
inventory. Concurrent final-fit/+1, partial write, kill, and marker tests prove
the cap. Task 3 supplies the pure writer/validator while its feature-on
authority deliberately remains unavailable; Task 6 activates it only after the
full committed authority transaction and installs verified owned primary-unit
drop-ins that pass a fixed scheduled-mode flag with `RefuseManualStart=yes`; the
base primary templates remain unchanged while disabled. Manual
operators use the direct collection CLI, not `systemctl start`.

Task 4 gate replay enumerates every expected primary depth (`*:05`) and daily
(`00:30`) slot in the same selected window. Each requires exactly one scheduled
successful receipt: depth must release the collection lock before the next
`:09` Shadow slot and daily before the next `:39` Shadow slot. Missing, late,
failed, skipped, duplicate, manual-substituted, invalid, or overflow evidence
blocks. This is a measured readiness SLA, not a claim that the existing
30-/75-minute systemd timeout is safe. Tests prove maximum-timeout primary runs
make widening unreachable, while normal bounded primary receipts coexist with
at least 85 canary/500 full Shadow cohorts without any intentional collision.

Fix the ledger/lock order: choose and validate run ID, then attempt the
collection lock nonblocking. A successful owner first closes older unterminal
entries, then publishes its own `started.json`. A busy invocation atomically
commits one run directory containing both `started.json` and
`terminal.json(outcome=skipped_locked)` and never exposes an unterminal skipped
run. Use barriers to prove two simultaneous invocations cannot mark each other
unexplained. If `ExecStopPost` knows an invocation ID but no ledger exists, it
installs the terminal-only bounded synthetic `unexplained` closure defined
below, then a service record binding that terminal; it never invents
`started.json`. Absence cannot prove whether the lock was acquired, so
synthetic terminal evidence uses `started_sha256=null,lock_acquired=null` with
status `not_evaluated`; normal owners write `true`, busy invocations write
`false`, and null blocks later gates. Test kill-before-lock and
kill-after-lock-before-start separately without inventing a boolean.

- [ ] **Step 2: Write the no-public-pointer RED test**

Seed `routes/latest.json` with sentinel bytes, complete a shadow run, and assert
the sentinel is byte-identical. Patch `publish_complete_route_bundle` to raise
if invoked. Assert only `routes/core/latest.json` and
`routes/shadow/latest.json` advance.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_run_route_shadow tests.test_route_cost_evidence tests.test_dex_route_costs tests.test_fetch_dex_depth tests.test_route_collection -v`

Expected: FAIL because the orchestrator, exact route-cost evidence module,
tracked adapter/key registries, fixed-block V2 cost adapter, and sealed DEX
projection are not implemented. Preserve the failing output for each of the
five modules; a single early import failure is not evidence that the remaining
DEX/config assertions are RED, so rerun importable modules individually if
discovery stops early.

- [ ] **Step 4: Implement canary/full execution bounds**

Canary uses the literal ten-Token allowlist, deadline 60, workers 2, per venue
1, per chain 1: `PEPE,CAKE,SHIB,SUSHI,ZK,SNX,GRT,COMP,ENS,STRK`. Full uses
every eligible Token, workers 4, per venue/chain 1, with the same 60-second
deadline. The CLI exposes no deadline/worker bypass. Canary fails if any of the
ten Tokens has no route; it never silently shrinks the denominator. Scope the
universe first, then atomically persist it. Reload it exclusively through
`load_run_input_binding()` and use only that fully reread immutable universe
for collection and audit; mutating the pre-write Python object must have no
effect. Canary requires every Token to have at least one route, either a
candidate or an explicit research-only route; it never pretends all ten ecosystems share a
cost adapter. Separately, adapter canary success requires at least one
Ethereum `uniswap_v2` constant-product route classified candidate and complete
fixed-block cost evidence for all five notionals and both directions. A
CEX-only route cannot satisfy that adapter exercise, while unsupported
ZKsync/Starknet/V3/L2 routes remain visible research-only rather than shrinking
the ten-Token inventory. Audit inventory cannot describe a wider full
candidate set.

Materialize CEX collector identity only through canonical Market ID and require
the exchange to exist in the live order-book adapter/`REQUESTED_LEVELS` map.
Materialize DEX identity plus the exact embedded collector context; never call
legacy `_resolve_inventory_legs()`, `load_pool_inventory()`, or any production
inventory loader after Task 1 capture. Patch those loaders to raise in an
integration test and exercise a real DEX collector preflight. Persist the
complete validated `route_collector_context/v1` object unchanged on the DEX
core leg together with the exact redundant `usd_price_*` fields required by
Task 2; do not drop request/response timing, `tvl_method`, status, or production
reason during collector projection.
For every collected leg, also retain the exact typed source members needed by
the existing opportunity replay adapter under that run's accepted raw-evidence
directory. CEX members include canonical market rules and the actual typed USD
conversion used with the book; DEX members include the exact pool-state/price
inputs required by its declared adapter. Their relative basenames, roles,
physical SHA-256 values, adapter IDs, and logical generations are copied into
the immutable core leg under one optional-for-legacy but mandatory-for-Shadow
field `typed_source_lineage`. It is exact
`route_leg_typed_source_lineage/v1` with only `schema,members`; members are
sorted uniquely by role and each has only
`role,status,reason_code,filename,sha256,size,logical_generation,adapter_id,content_schema`.
Closed roles are
`cex_raw_book_response|cex_market_rules|quote_usd_conversion|dex_pool_state|dex_usd_price_context`;
the market type fixes the allowed subset and a literal role -> adapter/content
schema map. Status is `observed|unavailable`: observed requires a safe ASCII
basename at most 128 bytes, 64-hex physical/logical hashes, bounded positive
size, and allowlisted adapter/schema. Unavailable still requires the role's
allowlisted `adapter_id` and `content_schema`, requires exactly
`filename,sha256,size,logical_generation` to be JSON null, and requires one
closed typed-source reason. Those four nulls distinguish absent physical
evidence from the declared adapter contract; no implementation may null or
infer the adapter/schema identity. Unknown/duplicate roles,
cross-market adapters, unsafe names, more than five members, or aggregate
per-leg bytes over existing raw caps fail. Core CSV/SQLite `row_json` and
logical hashes preserve/validate the exact object. Legacy core v1 rows without
this field remain read-only compatible but Task 3 Shadow verification and Task
4 candidate input binding require it, so an old core can never become a new
candidate accidentally.

Physical members live separately from existing
`accepted/<market-hash>/response.json`, which retains its frozen one-file
inventory. Store typed bytes under exact run-level `typed/` basenames and
atomically install `typed-manifest.json` schema
`route_typed_source_manifest/v1` with only
`schema,raw_evidence_run_id,member_count,members`; manifest members repeat only
`market_id,role,filename,sha256,size,logical_generation,adapter_id,content_schema`
in market/role order and must exactly reproduce every observed core lineage
member and the typed directory inventory. Its exact byte SHA is written into
Task 3 `verification.json` as `typed_source_manifest_sha256`, and candidate
input binding requires that verification/core/raw three-way match.
They are produced under the same collection deadline
and raw run ID, never fetched later by candidate installation and never loaded
through a caller-selected source root. Missing typed evidence is an explicit
terminal/unavailable adapter result, not an omitted file or a default value.
Task 3 tests only the producer boundary: exact retained members, core hashes,
missing-member terminal status, and post-publish path replacement cannot change
loaded core lineage. Task 4 owns the end-to-end candidate replay test after its
input builder exists: it replaces every production typed-source path and proves
replay reads only retained raw members and the installed private projection.

Keep DEX execution-cost evidence as a separate route/notional transcript set;
do not overload the per-leg typed-source roles above. The initial production
adapter boundary is exactly Ethereum mainnet, an allowlisted and code-identity-
verified Uniswap V2 Router02 deployment, direct one-pool/one-hop
ERC20-to-ERC20 execution, and the existing five notionals in both route
directions. Buy uses only `swapTokensForExactTokens` selector `0x8803dbee` and
sell uses only `swapExactTokensForTokens` selector `0x38ed1739`. Native-token
methods, a path length other than two, fee-on-transfer selectors, arbitrary
routers/factories, and every BSC, ZKsync, Starknet, Arbitrum, Optimism, Base,
or V3 fee model remain `research_only`; they may not fall back to the Ethereum
adapter. The checked-in registry descriptor pins exact chain ID, adapter ID,
protocol family, router/factory addresses, runtime-code hashes, pair fee,
gas-fee model, permitted selectors, and the three false capability flags
`supports_native,supports_multihop,supports_fee_on_transfer`. Production
implementation must source deployment identities from authoritative upstream
records and verify live fixed-block code before accepting them; the plan does
not invent or accept a caller-supplied deployment address/hash.

Before audit, the controlled collector installs private mode-0600 Shadow
sidecar `routes/shadow/runs/<run_id>/route-cost-evidence.json` with
descriptor-relative `O_EXCL|O_NOFOLLOW`, regular/nlink and ancestor-identity
checks, file/parent fsync, and exact reread. It uses exact schema
`route_cost_evidence_manifest/v1`. It permits only
`schema,run_id,route_cohort_id,phase,candidate_source_generation,
route_universe_sha256,adapter_registry,adapter_registry_sha256,
connector_key_registry,connector_key_registry_sha256,transcript_count,
trace_profile_generation,submission_connector_profile_generation,
evaluated_at,
selected_market_count,selected_markets,selected_market_set_sha256,
native_price_evidence,native_price_evidence_sha256,
chain_evidence_count,chain_evidence,chain_evidence_set_sha256,
market_evidence_count,market_evidence,market_evidence_set_sha256,
transcripts,transcript_set_sha256,binding_count,bindings,binding_set_sha256,
submission_policy_snapshot,submission_policy_snapshot_sha256,counts`.

The registry snapshot is captured before source reads and route-cost
classification. Exact `route_cost_adapter_registry/v1` permits only
`schema,registry_version,adapters`; adapters are sorted by adapter ID and each
has only `adapter_id,chain_id,protocol_family,router_address,factory_address,
router_runtime_code_sha256,factory_runtime_code_sha256,pair_fee_bps,
gas_fee_model,allowed_selectors,supports_native,supports_multihop,
supports_fee_on_transfer,trace_method,connector_family,token_funding_descriptors,native_symbol,
wrapped_native_address,simulation_sender_address,native_price_reference_market_id,
native_price_reference_adapter_id`. `gas_fee_model` is exactly
`eip1559_fee_history_v1`; the wrapped-native address is the canonical Ethereum
WETH address used only as the deterministic retained USD-price lookup key, and
the simulation sender is one checked-in public 20-byte address used with fixed
state overrides rather than a private operator address.
`trace_method` is literal `debug_traceCall_state_override_v1`. Funding
descriptors are sorted by token address and each has only
`token_address,runtime_code_sha256,proxy_implementation_address,
proxy_implementation_code_sha256,storage_layout,balance_mapping_slot,
allowance_mapping_slot,source_metadata_sha256`; storage layout is only
`solidity_mapping_v1`, slots are bounded canonical decimal integers, and the
initial adapter requires both proxy fields JSON null. Proxy/custom tokens are
statically unsupported in v1; there is no guessed EIP-1967 slot or implementation
lookup. Structural support is frozen before external reads solely from the
presence and declared capability of these captured registry descriptors. It
never depends on a later RPC/code result. During collection, the exact
fixed-block token runtime must match `runtime_code_sha256`; absence or mismatch
produces the blocking failed reason `token_funding_code_mismatch` for every
affected expected transcript while the route remains a candidate. A descriptor
missing at registry-capture time is static `strict_cost_adapter_unsupported`.
Tests distinguish those two cases and prove an RPC timeout/code drift cannot
shrink the already frozen denominator.
Exact
`route_cost_connector_key_registry/v1` permits only
`schema,registry_version,keys`; keys are sorted by key ID and each has only
`key_id,connector_id,algorithm,public_key,valid_from,valid_until,status`.
Algorithm is literal `ssh-ed25519-sshsig-v1`, public key is one canonical
OpenSSH `ssh-ed25519` key token without comment, status is `active|retired`, and
times bound the attested observation. Both registries are canonical bounded
objects embedded in the sidecar, and each physical canonical SHA must equal
its outer field. Historical replay uses these exact bytes, never the moving
current code constant or key registry. The same adapter-registry SHA and
route-universe SHA bind the pre-read structural scope, every transcript and
binding, audit/joint pointer, and candidate input; drift before joint
publication is `source_generation_drift`, never a new baseline.
Production opens only tracked direct files `config/route_cost_adapters.json`
and `config/route_cost_connector_keys.json` relative to the verified project
root; no CLI/environment path override exists. Each is canonical UTF-8/LF,
regular single-link, nofollow, at most 64 KiB, and descriptor-reread by physical
SHA. The worker rehashes both at the same four Task 3 generation checkpoints;
a change after structural classification fails the run. Tests replace either
path, symlink/hardlink/swap an ancestor, mutate one byte at each checkpoint,
and prove the embedded pre-read snapshot remains the only historical replay
authority.

The fixed production collector additionally reads exactly two private profile
paths from `MARKET_ROUTE_TRACE_RPC_PROFILE` and
`MARKET_ROUTE_SUBMISSION_CONNECTOR_PROFILE`; no function/CLI accepts a source,
URL, credential, or adapter override. Each direct regular mode-0600 file is
owner/nofollow/inode validated, capped at 64 KiB, captured once, and parsed by
an exact private schema. `route_cost_trace_rpc_profile/v1` permits only
`schema,profile_id,endpoint_id,rpc_url,authorization`; the URL must be HTTPS or
explicit local loopback, and `authorization` is a 1..4096-byte control-free
opaque string that is supplied only as the fixed connector's Authorization
header. It is never permitted in a query string, redirect, log, evidence, or
exception.
`route_cost_submission_connector_profile/v1` permits only
`schema,profile_id,connector_id,endpoint_url,authorization`; connector/key
registry identities must match. The endpoint is an origin URL with no query,
fragment, userinfo, or non-root path; the fixed client alone appends
`/v1/health` and `/v1/submission-policy-snapshots`. It has the same HTTPS/
explicit-loopback rule, redirects are forbidden, and authorization has the same
bounded control-free rule. Profile paths, URLs, authorization, and raw
bytes are never persisted or logged; only canonical profile generations and
nonsecret endpoint/connector IDs enter the sidecar. Structural classification
depends only on the frozen adapter/key registry and deterministic eight-market
cohort, never on mutable profile presence. An unset trace profile emits the
full expected transcript inventory as unavailable `trace_profile_missing`; an
unset connector emits the full unavailable policy snapshot described below.
A configured unsafe profile is `unsafe_path` and fails the entire run without
reclassification. A valid profile whose bounded trace/connector health probe
later times out likewise keeps every already structural candidate incomplete;
none of these cases shrinks intended scope. Unit tests inject profiles only
through a module-private capability harness.

Profile generation is SHA-256 of one canonical nonsecret identity projection,
not the secret file bytes. `route_cost_trace_profile_identity/v1` has only
`schema,status,profile_id,endpoint_id`; `route_cost_submission_connector_identity/v1`
has only `schema,status,profile_id,connector_id`. Status is
`available|missing`; available requires both IDs, missing requires both JSON
null. The generations are exactly
`SHA256(b"route-cost-trace-profile-identity/v1\n" + canonical_json(identity) + b"\n")`
and
`SHA256(b"route-cost-submission-connector-identity/v1\n" + canonical_json(identity) + b"\n")`,
where `canonical_json` is UTF-8, sorted-key, compact JSON without a trailing
newline. They produce the always-nonnull lowercase 64-hex generation repeated in sidecar,
transcript/binding/signed snapshot. A missing profile therefore has one fixed
typed sentinel generation. Profile rotation that preserves public identity
does not leak or hash credential/URL; a changed public ID changes generation.
Unsafe/invalid configured bytes produce no generation or successful sidecar.
Tests freeze available/missing canonical bytes and hashes, secret/URL privacy,
ID rotation, null matrix, and cross-profile transplant.

Select the adapter-coverage target before external reads; do not filter out an
unsupported high-priority market first. The eligible pool is every frozen-
universe Ethereum constant-product-V2/Router02 DEX leg, regardless of whether
its token funding descriptor is currently implemented. Select at most eight.
Build exact sorted projection `route_cost_selected_markets/v1` with only
`schema,members`; each member has only
`market_id,token_rank,selection_rank,best_route_volume_usd,dex_24h_usd,
dex_tvl_usd,adapter_id,structural_support_status,structural_reason`. Start from
all such eligible legs, deduplicate by Market ID, and compute
`best_route_volume_usd` as the maximum nonnull canonical
`route_volume_usd` among structural candidate routes containing that market.
Sort by presence then numeric value descending for best route volume, then
presence/value descending for DEX 24-hour volume, then presence/value descending
for DEX TVL, then captured configured-Token rank ascending, leg
`selection_rank` ascending, and canonical Market ID ascending; null numeric
values always sort last and never coerce to zero. Take the first eight and then
serialize members in canonical Market-ID order. The physical SHA of those exact
bytes is `selected_market_set_sha256`; the outer count and every transcript,
binding, policy snapshot, audit/joint pointer, and candidate input must
recompute the same projection from the frozen universe/registry. Permuting
input rows, duplicate markets, null/zero/decimal boundaries, ties, and a
cross-run selected-set transplant are literal fixtures. That deterministic
adapter cohort and the two registries are part of the source-generation-bound
structural scope. `structural_support_status` is `supported|unsupported` and
reason is respectively JSON null or `strict_cost_adapter_unsupported`; it is
computed only from the captured static registry/descriptors. Every selected
unsupported market still emits its ten expected unavailable transcripts and
sets the gate coverage blocker below; it cannot disappear from the coverage
denominator. A route-universe entry that contains a structurally unsupported or
out-of-cohort DEX market is explicitly
projected `research_only` with stable reason
`strict_cost_adapter_unsupported|cost_adapter_cohort_capacity`; this decision
cannot change from a later RPC outcome. CEX-only candidates are unaffected.

Transcripts are sorted uniquely by DEX market ID, direction, and numeric
requested notional. There is exactly one transcript for each selected market
times two actions times five notionals, so the maximum is 80. Fixed block and
chain-level fee/native-price observations live once in exact sorted
`chain_evidence`; router/factory/pair/core-state identity lives once per market
in exact sorted `market_evidence`. Scenario transcripts reference their exact
canonical SHAs rather than copying/re-fetching those bytes. Each transcript is exact
`route_cost_evidence_transcript/v1` with only
`schema,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,adapter_registry_sha256,selected_market_set_sha256,trace_profile_generation,
submission_connector_profile_generation,market_id,direction,
requested_notional_usd,adapter_id,core_pool_state_id,
core_pool_state_sha256,chain_evidence_sha256,
market_evidence_sha256,status,completed_stage,reason_code,
block_evidence,call_evidence,gas_evidence,router_fee_evidence,
transfer_tax_evidence,raw_transcript`.

`route_cost_chain_evidence/v1` permits only
`schema,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,selected_market_set_sha256,chain_id,rpc_source_id,captured_started_at,
captured_finished_at,status,reason_code,block_header_result,fee_history_result,
native_price_record`; the selected cohort may have zero complete chain objects
while no transcript has completed its chain stage. Once any Ethereum transcript
has a complete chain stage there is exactly one Ethereum object, referenced by
at least one transcript; future supported chains follow the same one-per-chain
rule. Each is at most 64 KiB. `rpc_source_id` must exactly equal the captured
trace profile's nonsecret `endpoint_id`; no endpoint chosen by the caller or a
redirect is legal. `route_cost_market_evidence/v1`
permits only
`schema,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,adapter_registry_sha256,selected_market_set_sha256,market_id,adapter_id,
chain_evidence_sha256,core_pool_state_id,core_pool_state_sha256,
router_address,router_runtime_code,factory_address,factory_runtime_code,
pair_address,pair_runtime_code,pair_token0,pair_token1,
captured_started_at,captured_finished_at`. Runtime code fields retain the full
lowercase even-length bytes used to recompute the pinned whole-runtime SHA,
never a slice; each code field is at most 128 KiB and a whole market object is
at most 400 KiB, leaving 16 KiB for the canonical JSON envelope and all other
fixed fields when all three code fields reach their individual limits. At most
eight objects therefore remain within the 32 MiB sidecar cap. A simultaneous
three-code-field exact-limit fixture must pass, while one additional byte in
any code field or in the 400-KiB whole object must fail before publication.
Their set hashes use literal domains
`route-cost-chain-evidence-set/v1\n` and
`route-cost-market-evidence-set/v1\n` over canonical ordered arrays.

Bindings are sorted uniquely by route ID and numeric requested notional; there
is one for each structural candidate route-notional that contains a selected
DEX leg, at most 4096. Each is exact `route_cost_evidence_binding/v1` with only
`schema,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,adapter_registry_sha256,selected_market_set_sha256,
connector_key_registry_sha256,trace_profile_generation,
submission_connector_profile_generation,route_id,requested_notional_usd,
buy_transcript_sha256,sell_transcript_sha256,
submission_policy_member_sha256,evaluated_at,status,reason_code`. A CEX side uses JSON null; every DEX side names the exact
canonical transcript SHA for its market/direction/notional. The signed policy
snapshot contains and binds the whole route scenario; the binding names that
member's exact canonical SHA rather than duplicating a signature.

The expected transcript and binding counts are derived from the frozen scoped
universe and never supplied by a caller. The exact sidecar must not exceed
`MAX_ROUTE_COST_EVIDENCE_BYTES=32*1024**2`. Exceeding either limit fails the
entire run with `resource_limit`; it never drops routes or reclassifies them.
`status` is `observed|unavailable|failed`; `core_pool_state_id` and
`core_pool_state_sha256` are either both exact/non-null or both JSON null. They
are null only when the selected market's retained typed pool state was not
produced or failed validation; the market's ten derived transcript rows remain
present and keep every containing structural route in scope. An observed transcript requires every
nested object below, while unavailable/failed uses their exact JSON-null matrix
and one closed reason. A binding is observed only when every referenced
transcript is observed and its signed policy is valid/fresh. `counts` has only
bounded nonnegative `transcript_observed,transcript_unavailable,
transcript_failed,binding_observed,binding_unavailable,binding_failed`, and is
recomputed from both arrays.
Every transcript/binding repeats and exactly matches the outer run, cohort,
source generation, universe, selected-market set, and its applicable registry
SHAs (transcripts bind the adapter registry; bindings also bind the connector-
key registry); these fields are also inside
the signed policy payload and set hashes, so an item cannot be transplanted and
then made valid merely by recomputing an outer manifest. `requested_notional_usd` is one
of the five canonical decimal strings, direction is `buy|sell`, IDs use their
existing strict grammars, numeric quantities are nonnegative canonical decimal
or lowercase quantity hex under the named raw field, and all UTC timestamps
are canonical, ordered, and within the run window. Block tag/number/hash and
chain ID must mutually reproduce the single fixed block.

Freeze the status/null matrix and closed reasons. Transcript `completed_stage`
is exactly `none|block|call|gas|router_fee|transfer_tax`. `observed` requires
stage `transfer_tax`, every nested transcript object nonnull, component status
`authenticated|not_applicable` as permitted, an empty transcript reason, and a
trace-derived zero-tax result. Shared market arrays contain only complete
observed identity objects. A shared chain object is block-anchored and
canonical but may have outer status `observed|incomplete|failed`: observed
requires observed fee-history and native-price envelopes; incomplete requires
at least one unavailable envelope; failed requires at least one failed envelope.
Its closed outer reason is deterministically recomputed from those two results.
Every shared object must be referenced by at least one transcript,
although a transcript may reference only the completed shared stages below.
`unavailable` requires exactly the presence row for its reason, with one of
`strict_cost_adapter_unsupported|core_pool_state_unavailable|rpc_unavailable|fixed_block_unavailable|router_identity_unavailable|
pair_identity_unavailable|calldata_unavailable|gas_unavailable|
native_price_unavailable|trace_profile_missing|trace_unavailable|
transfer_tax_present|transfer_behavior_unsupported`; `failed` requires one of
`core_pool_state_invalid|rpc_invalid|fixed_block_mismatch|router_identity_mismatch|
pair_identity_mismatch|token_funding_code_mismatch|calldata_mismatch|gas_invalid|native_price_invalid|
trace_invalid|resource_limit`.

The literal presence table is:

- `strict_cost_adapter_unsupported`: stage `none`, core/shared SHAs and every
  nested evidence/raw object are null; all ten selected-market rows remain and
  no RPC/connector call is made for that market;
- `core_pool_state_*`: stage `none`, the two core-state fields, both shared SHAs,
  and every nested evidence/raw object are null. One unavailable pool in an
  otherwise selected cohort still emits its ten route-cost rows and cannot be
  deleted from the denominator;
- `trace_profile_missing|rpc_*|fixed_block_*`: stage `none`, both shared SHAs
  and every nested evidence/raw object null. The core-state pair remains
  nonnull when that retained state exists. Missing profile still emits exactly
  `selected_market_count * 2 * 5` expected transcript rows (0, 10, ... 80) and
  performs zero trace-RPC calls;
- `router_identity_*|pair_identity_*|token_funding_code_mismatch`: stage `none`, a complete shared chain
  SHA may be nonnull, market SHA and every nested object are null;
- `calldata_*`: stage `block`, both shared SHAs and block evidence are nonnull,
  raw transcript contains the bounded calldata capture, later objects null;
- `gas_*`: stage `call`, both shared SHAs plus block/call/raw are nonnull, gas
  and later component objects null;
- `native_price_*`: stage `call`, the stage-complete shared chain object
  contains the exact unavailable/failed native-price envelope, both shared SHAs
  plus block/call/raw are nonnull, and gas/later component objects are null;
- `trace_*`: stage `router_fee`, block/call/gas/router-fee/raw are nonnull and
  transfer-tax is null; and
- `transfer_tax_present|transfer_behavior_unsupported`: stage `transfer_tax`,
  block/call/gas/router-fee/raw and transfer-tax evidence are nonnull. The tax
  component status is unavailable, its rate is null, and the retained bounded
  balance deltas prove the positive/short/nonstandard behavior; and
- observed: stage `transfer_tax`, every required object nonnull. Resource limit
  uses only the last complete row above and never retains a partial shared
  object.

A present nested
object must always validate even on an unavailable/failed record; unknown or
contradictory partial objects fail the whole sidecar. Component status is
exactly `authenticated|not_applicable|unavailable|failed`; rate is canonical
nonnegative decimal only for authenticated numeric evidence and JSON null for
not-applicable/unavailable/failed.

Every binding has one nonnull lowercase 64-hex
`submission_policy_member_sha256`, including missing/unavailable connector
states; it must resolve exactly one route/notional-matching member, while the
binding itself supplies and validates the transcript hashes
in the always-present snapshot. Binding `observed` requires all referenced
transcripts and that member observed plus an authenticated aggregate snapshot
whose signed validity window is fresh at the binding evaluation instant, and an
empty reason. Binding
`unavailable` uses exactly
`transcript_unavailable|submission_policy_unavailable|
submission_policy_stale`; `failed` uses
`transcript_failed|transcript_binding_mismatch|
submission_policy_invalid|resource_limit`.
Policy member status is `observed|unavailable|failed`: observed requires
nonnull mode/policy, the exact DEX-nonnull/CEX-null buy/sell-bound matrix, and
null reason; missing or remote-unavailable requires mode/policy and both bounds
JSON null and exactly
`submission_connector_missing|submission_connector_unavailable`; failed retains
only the bounded fields that parsed canonically and uses
`submission_connector_invalid|submission_policy_invalid`. A configured unsafe
profile is a hard run failure before sidecar publication and is not a policy
reason. Snapshot status is exactly
`authenticated|not_applicable|unavailable|failed`. `authenticated` means the
outer structure, member set, key identity, and aggregate signature are valid;
it does not itself assert freshness. It requires nonnull connector/times/key/
signature fields and null reason, while its members may legitimately mix
observed and other route-level unavailable decisions. Signed member bytes are
immutable and never rewritten for local clock freshness. Freshness is
recomputed for each binding at that binding's exact persisted `evaluated_at`;
an expired but otherwise valid signed snapshot remains authenticated, every
signed member retains its original byte/status/reason tuple, and only the
unsigned binding projection uses `unavailable/submission_policy_stale`.
`not_applicable` is legal only for the derived empty binding inventory defined
below. `unavailable` requires the full member inventory but null times/key/
signature/attested hash and one missing/remote-unavailable reason; `failed`
retains only bounded parseable response fields and one invalid reason and
cannot attest any member. A binding is observed only when the snapshot is
authenticated, fresh at binding `evaluated_at`, and its own member is observed;
one unavailable member does not poison unrelated observed members. Outer and
every binding `evaluated_at` are the same canonical run-window UTC instant
sampled once from the trusted clock after snapshot verification and before the
sidecar is serialized; it is not accepted from the connector or caller. For an
authenticated snapshot it must be at or after `observed_at`; at or before
`valid_until` permits an observed member to yield an observed binding, while a
strictly later instant yields only `unavailable/submission_policy_stale`.
Missing/unavailable snapshots still require this run-window instant after the
bounded connector outcome even though their snapshot times are null. Every
binding repeats the same outer value, and binding sort/uniqueness remains only
route ID then numeric notional. Changing or omitting the instant changes the
binding-set and sidecar hashes. Later Task 4 candidate evaluation rechecks
`valid_until` against its own persisted candidate `evaluated_at` and may
downgrade strict readiness without modifying historical snapshot, member, or
binding bytes.
Exact per-reason fixtures freeze every null
position and reject floats, `-0`, NaN/Infinity, malformed hex, reversed time,
unknown reason/status, and extra keys.
`transcript_set_sha256` hashes the canonical ordered `transcripts` array under
literal domain `route-cost-evidence-transcript-set/v1\n`;
`binding_set_sha256` does the same for `bindings` under
`route-cost-evidence-binding-set/v1\n`. The manifest physical
SHA is independently committed to the Task 2 audit/joint pointer and Task 3
ledger. It is deliberately not a `route_cohort_core/v1` member, so existing
core readers and historical v1 bytes remain unchanged.

Freeze the nested evidence schemas as exported non-reflective field tuples in
`scripts/route_cost_evidence.py` and independently literal-test them:

- `route_cost_block_evidence/v1` has only
  `schema,chain_evidence_sha256,market_evidence_sha256,chain_id,block_tag,
  block_number,block_hash,block_timestamp,core_pool_state_id,
  router_runtime_code_sha256,factory_runtime_code_sha256,
  pair_runtime_code_sha256,rpc_transcript_sha256`;
- `route_cost_call_evidence/v1` has only
  `schema,selector,path_token_in,path_token_out,recipient_policy,deadline,
  amount_in_raw,amount_out_raw,calldata_sha256,sender_policy,
  allowance_basis,submission_loss_bound_bps`;
- `route_cost_gas_evidence/v1` has only
  `schema,gas_units,max_fee_per_gas_wei,fee_history_sha256,native_symbol,
  native_price_usd,native_price_sha256,observed_at,valid_until`;
- `route_cost_router_fee_evidence/v1` has only
  `schema,status,rate_bps,basis_code,source_record_sha256`;
- `route_cost_transfer_tax_evidence/v1` has only
  `schema,status,rate_bps,pre_input_balance,post_input_balance,
  pre_output_balance,post_output_balance,trace_method,trace_sha256`; and
- `route_cost_submission_policy_member/v1` has only
  `schema,route_id,requested_notional_usd,status,reason_code,submission_mode,policy_id,
  buy_submission_loss_bps,sell_submission_loss_bps`; and
- `route_cost_submission_policy_snapshot/v1` has only
  `schema,run_id,route_cohort_id,candidate_source_generation,
  route_universe_sha256,adapter_registry_sha256,selected_market_set_sha256,
  connector_key_registry_sha256,trace_profile_generation,
  submission_connector_profile_generation,connector_id,member_count,members,
  member_set_sha256,status,reason_code,observed_at,valid_until,issuer_key_id,
  signature_algorithm,attested_payload_sha256,signature`; and
- `route_cost_raw_transcript/v1` has only
  `schema,chain_evidence_sha256,market_evidence_sha256,
  captured_started_at,captured_finished_at,calldata_hex,
  estimate_gas_request,estimate_gas_response,simulation_method,
  simulation_request,simulation_response,simulation_balance_deltas`.

Raw transcript fields are bounded canonical hex/decimal/object projections of
the actual captured JSON-RPC results, not hashes standing in for undecodable
evidence. `simulation_balance_deltas` is a sorted exact array whose member has
only `token_address,account_role,pre_balance_raw,post_balance_raw`; its closed
account roles are `sender|router|pair|recipient`. `simulation_method` is the
single literal `debug_traceCall_state_override_v1`; `fork_execution` and custom
tracers are not production alternatives. `simulation_request` is exact
`route_cost_trace_request/v1` with only `schema,jsonrpc,id,method,params`:
`jsonrpc="2.0"`, `id` is the canonical positive batch-call integer, method is
`debug_traceCall`, and params is exactly `[call_object,block_tag,options]`.
`call_object` has only `from,to,gas,data,value`, using the captured public simulation
sender, pinned router, exact estimate response as gas, exact calldata, and
value `0x0`. `block_tag` is the
retained fixed-block quantity. `options` has only `tracer,tracerConfig,
stateOverrides`; tracer is `prestateTracer`, tracerConfig is exactly
`{"diffMode":true,"disableCode":true,"disableStorage":false}`, and
stateOverrides is a sorted address map containing only the public sender
balance and the descriptor-derived token `stateDiff` keys for its balance and
router allowance. All overridden words are lowercase 0x plus 64 hex and are
derived from the captured nonproxy funding descriptors; caller-supplied slots
or accounts are impossible.

`estimate_gas_request` is exact `route_cost_estimate_gas_request/v1` with only
`schema,jsonrpc,id,method,params`; method is `eth_estimateGas`, params is
exactly `[estimate_call_object,block_tag,state_overrides]`, and the call object
has only `from,to,data,value` with the same identities/bytes. Its override and
block bytes equal the trace request. `estimate_gas_response` is exact
`route_cost_estimate_gas_response/v1` with only
`schema,jsonrpc,id,result`; it repeats request ID and result is the minimal
positive EVM quantity hex used as trace gas. Error responses are never stored
as successes and map to the closed unavailable/failed reason.

The bounded prestate-tracer response is decoded immediately into exact
`route_cost_trace_response/v1` with only `schema,jsonrpc,id,storage_diffs`;
it repeats the request ID and its sorted `storage_diffs` members have only
`token_address,account_role,storage_key,pre_present,pre_value,post_present,
post_value`, with address/key/
word grammars above. The decoder accepts only `pre`/`post` account objects from
that fixed tracer, rejects structLogs, custom-tracer output, duplicate keys, or
an unplanned changed token slot, and retains every planned relevant diff. The
two presence fields are booleans and may not both be false. Because Geth
prestateTracer diff mode omits a newly written slot from `pre` and a deleted/
zeroed slot from `post`, an absent side requires its presence=false and
canonical 32-byte zero word; a present side requires presence=true and the
captured 32-byte word. Omitting the entire planned diff, marking an observed
side absent, or using null/short zero is invalid.
Offline validation recomputes mapping keys from the static descriptor, derives
`simulation_balance_deltas` from `simulation_response.storage_diffs`, and requires byte-exact
equality to the stored derived array. Thus zero-tax/short-receipt conclusions
are reproducible without retaining unrelated account state or trace frames.
Each transcript is at most 16 KiB and its canonical bytes are covered by the
record/set/sidecar hashes. Exact request/result known-answer tests cover input
and output transfers, allowance/balance slot swaps, proxy/custom tokens,
duplicate/omitted diffs, wrong tracer/config/block, and one-word mutation.
The two consumer hashes are not opaque: block
`rpc_transcript_sha256 = SHA256(b"route-cost-rpc-transcript/v1\n" +
canonical_json({"estimate_request":estimate_gas_request,
"estimate_response":estimate_gas_response,"trace_request":simulation_request,
"trace_response":simulation_response}) + b"\n")`, while transfer-tax
`trace_sha256 = SHA256(b"route-cost-trace/v1\n" +
canonical_json({"request":simulation_request,"response":simulation_response}) + b"\n")`.
Both must equal their respective nested evidence fields. Cross-request ID,
request/response, estimate/trace, or route/notional transplant fixtures fail.

Freeze every shared/raw nested value. Chain ID and the estimate response's
`result` are minimal lowercase EVM quantity hex.
`block_header_result` has only
`number,hash,parent_hash,timestamp,base_fee_per_gas,gas_used,gas_limit`; numeric
fields are minimal quantity hex and hashes are lowercase 0x plus 64 hex. Router/factory/
pair code fields in shared market evidence are complete lowercase even-length
0x byte strings under the 128-KiB per-field cap and must hash to the registry/
block evidence. Pair and token values are lowercase 20-byte addresses.
`calldata_hex` is lowercase
even-length 0x bytes at most 4 KiB and must round-trip through the named selector
decoder. `fee_history_result` is exact `route_cost_fee_history_result/v1` with
only `schema,status,reason_code,oldest_block,base_fee_per_gas,reward,
gas_used_ratio`; observed requires every data field, unavailable/failed requires
the literal gas reason and the exact nulls dictated by the last complete parse
stage. Arrays have exactly one requested block, fees are minimal quantity hex,
and ratios are canonical finite decimal strings. `native_price_record` is exact
`route_cost_native_price_record/v1` with only
`schema,status,reason_code,native_symbol,wrapped_native_address,price_usd,
observed_at,valid_until,native_price_evidence_sha256,source_record_sha256`.
Observed requires every field
and null reason; unavailable retains symbol/address and nulls every source/value
field; failed retains only bounded canonically parsed identity fields and uses
`native_price_invalid`. These envelopes make incomplete/failed chain evidence
replayable without pretending absent observations were measured.
Every raw-transcript key is always present; fields not yet captured under the
completed-stage matrix are JSON null, never empty placeholders. Exact object,
array, scalar, byte, and aggregate caps are exported constants and one-byte/
one-member overflow tests fail before sidecar publication.

The native/USD source is caller-independent and synchronous with this Shadow
run, not the daily TVL snapshot. The registry fixes one public CEX reference
market (initially canonical `cex:binance:ETH/USDT`) and its existing production
order-book adapter. Under the same collection deadline, capture its exact raw
book bytes, market rules, and existing strict typed USD conversion for the quote
asset; take the executable best ask and multiply by the authenticated quote/USD
rate with exact Decimal arithmetic. Persist one top-level exact
`route_cost_native_price_evidence/v1` object with only
`schema,run_id,route_cohort_id,candidate_source_generation,source_market_id,
source_adapter_id,source_endpoint_id,book_projection,market_rules_projection,
usd_conversion_projection,raw_response_base64,raw_response_sha256,
observed_at,valid_until,source_record_sha256`, at most 4 MiB.
`route_cost_native_price_book/v1` has only
`schema,market_id,adapter_id,best_ask_price,best_ask_quantity,observed_at,
raw_response_sha256`; `route_cost_native_price_market_rules/v1` has only
`schema,market_id,price_tick,quantity_step,min_quantity,min_notional,
observed_at,source_record_sha256`; and
`route_cost_native_price_usd_conversion/v1` has only
`schema,quote_asset,usd_asset,rate,observed_at,valid_until,
source_record_sha256`. Every decimal is canonical positive finite text and
every timestamp/source identity is recomputed through the existing sealed CEX
adapter. `raw_response_base64` is canonical padded RFC 4648 with no whitespace;
its decoded bytes are at most 2 MiB and hash exactly to both
`raw_response_sha256` and the book projection's identical field.
`source_record_sha256` is exactly
`SHA256(b"route-cost-native-price-source/v1\n" + canonical_json({"book":book_projection,"market_rules":market_rules_projection,"usd_conversion":usd_conversion_projection}) + b"\n")`.
The outer `native_price_evidence_sha256` is exactly
`SHA256(b"route-cost-native-price-evidence/v1\n" + canonical_json(native_price_evidence) + b"\n")`;
the compact chain record references that SHA;
the 64-KiB chain-object cap never includes the raw book. Missing source uses
JSON null for both top-level evidence and SHA; malformed partial/null mismatch
fails. Retain the complete bounded raw bytes as canonical base64 plus both typed projections there.
Book, rules, conversion, run/cohort/source generation, endpoint ID and physical
hashes must reproduce these closed projections and their underlying sealed CEX
collector contracts; unknown nested keys are rejected. Observed time
is the later input time and validity is the earlier input validity; it must cover
every cost scenario and run terminal, with no hard-coded extension. The fixed
auxiliary capture consumes at most one extra CEX request, 2 MiB response bytes,
and one per-venue task inside the existing 60-second/worker limits; it does not
enter the route universe or create a public opportunity. If unavailable/stale,
every otherwise complete DEX scenario ends `native_price_unavailable` and
remains in intended scope. Malformed/transplanted evidence fails the run. The
sidecar and candidate-input copy retain all bytes needed for offline replay;
no caller/profile supplies a price and a later external quote is never opened.
Fixtures cover missing/stale book or USD conversion, ask/bid substitution,
one-tick/rounding boundaries, a later quote appearing after capture, and cross-
run/source transplant.

The only fee model calls
`eth_feeHistory(0x1, fixed_block, [50])`; it requires exactly two base-fee
entries and one single-value reward row. Compute
`next_base_fee_wei` from the fixed header using the exact EIP-1559 integer
formula: `target=gas_limit//2`; equality preserves base fee; above target adds
`max(base_fee*(gas_used-target)//target//8,1)`; below target subtracts
`base_fee*(target-gas_used)//target//8`. Require it to equal the returned next base fee, set
`priority_fee_wei` to the canonical p50 reward, and set
`max_fee_per_gas_wei = 2 * next_base_fee_wei + priority_fee_wei` with exact
nonnegative integer arithmetic. Network-gas USD is
`ceil_decimal18(gas_units * max_fee_per_gas_wei * native_price_usd / 10**18)`,
where `ceil_decimal18` is Decimal quantization to `0.000000000000000001` with
`ROUND_CEILING`;
no float, current-block base fee, alternate percentile, or implementation-
selected rounding is legal. Known-answer and one-wei/one-decimal boundaries
freeze the formula.

For this initial adapter, call-policy literals are fixed:
`sender_policy=registry_fixed_state_override_sender/v1`,
`recipient_policy=same_as_registry_sender/v1`,
`allowance_basis=exact_amount_state_override/v1`, and deadline is exactly the
fixed block timestamp plus 300 seconds encoded as the minimal EVM quantity.
The decoded calldata recipient and trace sender must equal the registry's public
simulation sender. The state override grants only the exact input-token balance,
router allowance, and transaction-gas native balance. It derives standard
Solidity mapping keys exactly as
`keccak256(pad32(owner)||pad32(balance_slot))` and
`keccak256(pad32(spender)||keccak256(pad32(owner)||pad32(allowance_slot)))`,
and supplies canonical `stateDiff`; no guessed slot, arbitrary storage write,
or high-level provider funding API is legal. Fixed-block code/proxy identities
must match the funding descriptor before estimate or trace. The exact JSON-RPC
params include only from/to/gas/value/data, that allowlisted override, and the
fixed block tag; unknown provider-specific fields fail. Router fee `basis_code` is only
`verified_uniswap_v2_router02_no_integrator_fee/v1`. Submission mode is only
`private_relay`; public mempool is unavailable. `policy_id` full-matches
`[a-z0-9][a-z0-9._-]{0,63}` and is namespaced by the matching connector/key
registry. Case changes, controls, extra path elements, expired policies, or any
other literal make the member unavailable/invalid rather than executable.

There is no signature/transcript hash cycle. The one signed policy snapshot is
obtained after the structural route/notional inventory is frozen but before
scenario calldata is built; signed members bind run/cohort/source/universe,
route ID and notional through their enclosing snapshot, not later transcript
SHAs. Each binding then binds that member SHA to the exact buy/sell transcript
SHAs and rechecks route/notional equality. For an observed member, each nonnull
signed leg bound must exactly equal that DEX call's
`submission_loss_bound_bps`, while each CEX leg bound is JSON null.
Buy/exact-output calldata sets
`amountInMax = ceil(quoted_amount_in_raw * (10000 + bound_bps) / 10000)`;
sell/exact-input calldata sets
`amountOutMin = floor(quoted_amount_out_raw * (10000 - bound_bps) / 10000)`.
The offline decoder recomputes both integer formulas and rejects any looser or
tighter value, swapped leg, wrong direction, negative/over-10000 bound, or signed zero with
nonzero calldata slippage. When policy is unavailable, the collector may use
one fixed research simulation bound of 100 bps, but the binding remains
unavailable and no strict component is emitted. Thus the signed numeric bound
is the actual directional calldata loss ceiling, not merely a trusted label.

The fixed-block transcript binds `eth_chainId`, block number/hash/timestamp,
router and factory runtime code, `factory.getPair`, pair runtime code and
token0/token1, the exact selector/path/amount/recipient/deadline semantics,
sender-policy and allowance preconditions, `eth_estimateGas`, fee-history,
and a time-compatible native/USD observation. Gas becomes authenticated only
through the sealed cohort issuer; the existing caller-buildable/public helper
continues to emit assumed/non-strict rows. Router fee may be strict
`not_applicable` only after the pinned router/factory/pair/path identity is
proved. Transfer tax is never inferred as zero from ABI/reserves/estimateGas:
it requires the one controlled fixed-block `debug_traceCall` state-override
simulation whose token
balance deltas prove zero. Because this first adapter deliberately rejects
fee-on-transfer selectors and quantity semantics, any positive input/output
tax, received-amount shortfall, nonstandard balance behavior, or simulation
revert remains structural candidate but makes the transcript unavailable and
strict-incomplete; it is never converted into an additive numeric tax cost.
Missing trace is unavailable. Raw RPC URLs, credentials, unrelated account
state, trace frames, unrelated response fields, and arbitrary error text are
excluded. The
exact bounded calldata and relevant sanitized response values are retained only
in this private sidecar so Task 4 can rerun selector and cost validation rather
than trusting a pre-derived row or opaque hash.

Cost collection never asks for a new `latest` block. For each selected DEX
market with an observed leg, it consumes the exact `V2PoolState` already frozen
for that run's depth/quantity quote and copies its `state_id` into
`core_pool_state_id`; transcript also copies the physical retained typed-member
SHA into `core_pool_state_sha256`. If collection did not produce that state,
both fields are null and the market's ten rows use
`core_pool_state_unavailable`; malformed/transplanted state uses the failed
reason and blocks the run. When present, chain ID, block number/hash,
block-header SHA, pool/pair address, token0/token1, fee identity, and observed
timestamp must equal that core pool state field by field; all Ethereum
transcripts share the already captured chain block anchor.
The manifest rejects a second block anchor for the same chain. Candidate
replay performs the same comparison against the retained typed pool-state
member, so a B/B+1 cross-block transplant fails even when both timestamps are
inside the nominal freshness window.

`mev_buffer` remains scenario-only and never enters strict total. Separately,
strict DEX completeness requires a fresh connector-authenticated submission-
policy fact at route/notional grain. The connector is a fixed private
production adapter, not a caller mapping or public helper; it proves the
selected submission mode/policy identity, expiry, and conservative per-leg
maximum submission-loss bounds without persisting a credential or claiming zero
MEV cost. Every observed member has a canonical bps value in `[0,10000]` for
each DEX side and JSON null for each CEX side. The bps values constrain
calldata, but the USD risk is not computed as gross USD times bps because raw-
unit ceiling/floor can dominate that approximation. For a DEX buy define
`buy_loss_raw = amountInMax - quoted_amount_in_raw`; for a DEX sell define
`sell_loss_raw = quoted_amount_out_raw - amountOutMin`. Task 4's sealed decoder
uses each leg's already authenticated asset decimals and USD conversion to
convert those exact nonnegative raw deltas with `ROUND_CEILING` to canonical
18-decimal USD, then emits one route-level strict `submission_risk_bound` equal
to their sum; a CEX leg contributes zero. Thus DEX-DEX adverse input uplift and
output haircut, including a one-smallest-unit quantum, are both charged. V2 strict
net subtracts it and missing/stale/nonnumeric policy keeps the scenario strict-
incomplete. This is a new write-only `route_cost_components/v2` component/type
and `contract_version="2"`; the frozen v1 enum/rows/read-only bundles remain
unchanged. `mev_buffer` continues to express research scenarios only and is
never double-counted. One exact sorted policy snapshot contains
all route/notional members and its exact signed payload binds the
run/cohort/source/universe/selected-market set, adapter/key-registry/profile generations,
connector, member-set SHA, observed time, and expiry;
`attested_payload_sha256` hashes those canonical bytes and Ed25519 `signature`
is verified against a closed checked-in connector public-key registry. Private
signing material never enters the worker/evidence file, and test-only keys are
accepted only by a module-private capability harness. Public-mempool, missing, expired,
replayed, or caller-constructed policy evidence remains unavailable and keeps
the route non-strict. Route class is fixed before source reads only from the
source-generation-bound structural adapter registry: a route whose chain,
protocol, router family, trace method, and connector family are supported is
`candidate` for the entire run. A timeout, stale/missing record, unavailable
trace/connector, or any other observation failure never changes it to
research-only and therefore remains in intended scope as a blocking incomplete
scenario. Only structurally unsupported families are research-only. Thus an
instantaneous collection failure cannot shrink the denominator, while the
separate adapter canary fixture must prove at least one fully observed
Ethereum V2 route across all five notionals and both directions before
production claims V2 candidate coverage.

The single signed payload is exact
`route_cost_submission_policy_snapshot_attestation/v1` with only
`schema,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,selected_market_set_sha256,adapter_registry_sha256,connector_key_registry_sha256,
trace_profile_generation,submission_connector_profile_generation,
connector_id,member_count,member_set_sha256,observed_at,valid_until`.
Hash domains are literal and use compact sorted-key UTF-8 JSON without an
implicit newline inside `canonical_json`:

- member SHA = `SHA256(b"route-cost-submission-policy-member/v1\n" + canonical_json(member) + b"\n")`;
- member-set SHA = `SHA256(b"route-cost-submission-policy-member-set/v1\n" + canonical_json(ordered_members) + b"\n")`;
- attested-payload SHA = `SHA256(b"route-cost-submission-policy-attestation/v1\n" + canonical_json(attestation) + b"\n")`; and
- outer snapshot SHA = `SHA256(b"route-cost-submission-policy-snapshot/v1\n" + canonical_json(snapshot) + b"\n")`.

Known-answer and cross-type/cross-run collision fixtures freeze all four. `signature` is one canonical
bounded OpenSSH SSHSIG armored document, at most 4 KiB, whose namespace is
literal `route-cost-submission-policy-v1`; the decoded signature must be
Ed25519 with a 32-byte public key and 64-byte signature. Verification uses the
fixed system `/usr/bin/ssh-keygen -Y verify` interface with an in-memory
descriptor-backed allowed-signers file derived only from the captured key
registry, exact identity `connector_id`, fixed namespace, fixed payload stdin,
and no caller argv/path. The exact argv is
`/usr/bin/ssh-keygen -Y verify -f <allowed-fd-path> -I <connector_id> -n route-cost-submission-policy-v1 -s <signature-fd-path>`.
The canonical allowed-signers bytes are exactly
`<connector_id> ssh-ed25519 <base64-key>\n`; connector ID full-matches
lowercase `[a-z0-9][a-z0-9_-]{0,63}` and cannot contain SSH patterns, commas,
spaces, backslashes, or options. Payload bytes go to stdin; the armored SSHSIG
bytes and allowed-signers bytes use two distinct inherited read-only FDs.
Subprocess uses `close_fds=true`, exact `pass_fds=(allowed_fd,signature_fd)`,
empty stdin only after payload delivery, `LC_ALL=C`, a 2-second timeout, and
8-KiB stdout/stderr caps; after the independent armored-format, key, identity,
namespace, and payload checks, success is exactly exit code 0. Bounded stdout/
stderr bytes are discarded and never text-matched or persisted. Darwin `/dev/fd/<n>` and Linux
`/proc/self/fd/<n>` are the only descriptor paths; binary absence, unsupported
`-Y`, fd closure/swap, nonzero exit, timeout, oversized/malformed output,
unknown/expired/duplicate key, wrong identity/namespace/domain,
signature length/encoding error, or key rotation mismatch makes evidence
unavailable/invalid. Task 3/4 real Python 3.8 tests invoke this verifier and
Task 6 enable/release preflight proves the production binary capability; no
Python crypto implementation or undeclared package is assumed.

Freeze the connector wire contract. POST only to the fixed
`/v1/submission-policy-snapshots` child with `Content-Type` and `Accept` exactly
`application/json`, one fixed Authorization header from the private profile,
`Idempotency-Key: <request_id>`, no cookies, redirects, proxies, or caller
headers. Exact `route_cost_submission_policy_request/v1` has only
`schema,request_id,run_id,route_cohort_id,candidate_source_generation,
route_universe_sha256,selected_market_set_sha256,adapter_registry_sha256,
connector_key_registry_sha256,trace_profile_generation,
submission_connector_profile_generation,connector_id,members`; request members
have only `route_id,requested_notional_usd`. `request_id` is the typed SHA of
the same canonical object excluding `request_id` under literal
`route-cost-submission-policy-request/v1\n`. HTTP 200 must be the exact canonical
`route_cost_submission_policy_snapshot/v1` object, repeat all request bindings,
and contain exactly one sorted member for each request pair. 401/403/404/409,
429, 5xx, timeout, or connection failure map to the closed connector-unavailable
reason; any other status, non-JSON/canonical mismatch, duplicate/omitted/extra
member, wrong request identity, or malformed signed response is connector-
invalid. No error body or header enters evidence. `/v1/health` accepts only the
exact `route_cost_submission_connector_health_request/v1` object with only
`schema,challenge_nonce,connector_id,connector_key_registry_sha256`; nonce is
fresh lowercase 32-hex. Its HTTP method/headers/auth/redirect rules are the same
as the batch POST. HTTP 200 must be exact
`route_cost_submission_connector_health_response/v1` with only
`schema,challenge_nonce,connector_id,connector_key_registry_sha256,
observed_at,valid_until,issuer_key_id,signature_algorithm,signature`.
It repeats the nonce/identities, is valid for at most 60 seconds, and signs
exact `route_cost_submission_connector_health_attestation/v1` bytes with only
`schema,challenge_nonce,connector_id,connector_key_registry_sha256,
observed_at,valid_until,issuer_key_id,signature_algorithm`; every field equals
the response and `signature` is deliberately excluded. The message is
`b"route-cost-submission-connector-health-attestation/v1\n" +
canonical_json(attestation) + b"\n"` under SSHSIG namespace
`route-cost-submission-connector-health-v1` with the same captured key registry
and FD verifier. Any extra/missing field, nonce/identity drift, invalid/expired
signature, non-200, timeout, or body/header cap failure is an unavailable
capability and makes promotion readiness false. The production batch path never
accepts an empty inventory. When the deterministically derived binding
inventory is empty because `selected_market_count=0` or every selected market
is structurally unsupported, the client makes zero connector requests and
constructs the local exact snapshot row `status=not_applicable`,
`reason_code=scope_empty`, `member_count=0`, `members=[]`, and the typed empty
member-set SHA. Connector ID is the configured nonsecret ID when a safe profile
exists and JSON null when it is missing; observed/valid times, issuer key,
signature algorithm, attested-payload SHA, and signature are all JSON null.
No binding rows exist, and an empty snapshot cannot make adapter coverage pass
when selected unsupported markets remain. Any nonempty member list, network
call, signature field, other reason, or binding under `not_applicable` is
invalid. Known-answer tests freeze request/response bytes, namespace, nonce
replay, wrong key, status mapping, exact-limit bodies, zero selected markets,
and an all-unsupported selected cohort with a healthy connector.

The snapshot is always present. An authenticated snapshot permits a mixed set
of observed and route-level unavailable members, has nonnull connector/times/
key/signature fields, empty snapshot reason, and one successful aggregate
signature verification. Missing/unavailable connector produces the same
complete member inventory with every member status `unavailable`, exact reason
`submission_connector_missing|submission_connector_unavailable`, JSON-null
policy/bound/time/signature fields, and no change to structural route class. A
configured unsafe profile hard-fails before any sidecar; it never creates an
`unsafe` snapshot. A response or signature mismatch makes the snapshot and all
members `failed/submission_connector_invalid`. Each binding's
`submission_policy_member_sha256` resolves exactly one member with matching
route/notional plus the binding's exact transcript SHAs; binding status derives from that member and the
snapshot. The only zero-member snapshot is the local
`not_applicable/scope_empty` row above; it is never sent to the connector.
Unknown, duplicate, omitted, or extra members invalidate the whole sidecar.

Bound live collection independently of route count. One chain snapshot uses at
most four RPC calls, each selected market identity uses at most eight calls,
and each of the at most 80 scenario transcripts uses exactly one gas estimate
and one fixed-tracer simulation: at most 228 RPC calls total. Requests are issued
in at most six sequential batches of at most 40 calls, with only one in-flight
batch per chain. The submission connector receives one bounded canonical batch
containing at most 4096 unsigned policy members and returns one signed snapshot
in one round trip; it may not require a network call or signature per binding.
The worker launches exactly one `ssh-keygen` verification process for that
snapshot. The 60-second collector critical path is exact and bounded by three
joined phases. Phase A starts at most two top-level tasks: one captures chain/
market identity within 10 seconds; the other starts the independent single
native-price CEX request and single policy batch concurrently (5 seconds each),
then performs the policy's one SSHSIG verification (2 seconds), for a 7-second
maximum. The policy inventory
is already frozen from the structural route/notional scope, and its verified
per-leg bounds must be available before any scenario calldata is built. Phase B
therefore starts only gas/trace batches, with a 35-second maximum. Phase C
validates and serializes everything in 10 seconds. The critical path is
`max(10,max(5,5)+2) + 35 + 10 = 55` seconds, leaving five seconds of scheduler/join
headroom. At most two top-level tasks exist in Phase A and one in Phase B; each
owns disjoint immutable result buffers, and only the parent joins, sorts,
validates, and serializes them. There is still only one in-flight trace-RPC
batch, one native-price request, and one connector request at a time.
There are no automatic retries: every
logical call is issued at most once, so the hard maxima remain six batches and
228 calls. Timeout/failure
keeps structural candidates incomplete. Fake slow/batch-overflow fixtures and
the real 228-call/80-transcript/4096-binding/one-signature maximum prove completion or deterministic
timeout before the worker's 90-second service limit. The same maximum fixture
also performs the one auxiliary CEX native-price request (2 MiB response and
4 MiB canonical evidence), one connector batch round trip, and one SSHSIG
verification; those operations use the exact concurrent/included subdeadlines
above rather than being silently added after the 55-second critical path.
Maximum-corpus tests assert the two-task ceiling, join ordering, and completion
by 55 seconds (or deterministic timeout), leaving the named five-second margin
before the hard 60-second collector deadline.

Bound bytes before JSON parsing or decompression can exhaust memory. Every
non-trace JSON-RPC response is at most 256 KiB, every fixed-tracer response at
most 2 MiB, each HTTP batch response at most 8 MiB, all trace-RPC response bytes
combined at most 48 MiB, and the connector request/response at most 8 MiB each.
Each outgoing RPC batch body is at most 4 MiB and all six combined are at most
24 MiB; the one connector request is at most 8 MiB and the one native-price CEX
request at most 64 KiB. Request headers use the same 32-KiB aggregate cap as
responses. The complete collector working-set proof counts at most 48 MiB
compressed/decompressed RPC wire bytes, 128 MiB bounded parsed/sanitized RPC
objects, 8 MiB connector request plus 8 MiB response, 2 MiB native-price wire
plus 4 MiB canonical native evidence, 32 MiB final sidecar, 64 MiB build/copy
overlap, and 128 MiB Python/container overhead; total 422 MiB, leaving more
than 300 MiB below the rendered 768 MiB
`MemoryMax`. `Content-Length` is only an early rejection hint. The fixed client
streams through a decompression counter, stops after `limit+1`, and never parses,
retries, logs, or persists an oversized success/error body. Chunked, absent-
length, compressed-expansion, oversized-error, exact-limit, and one-byte-over
fixtures plus the maximum 228-RPC/one-CEX/one-connector corpus prove both the 60/90-second and memory
boundaries. Each HTTP response permits at most 64 headers, 128 bytes per name,
8 KiB per value, and 32 KiB total header bytes. Before materialization, the
streaming JSON limiter permits at most 1,048,576 nodes, 4 KiB per ordinary
string (the separately bounded code/trace/signature fields use their named
caps), and 64 MiB aggregate decoded scalar bytes for RPC or 8 MiB for the
connector. Exceeding any one limit is `resource_limit`, with zero retry and no
partial evidence.

Task 3 tests freeze known-answer decoding for both selectors and exact
direction/path/amount/deadline/recipient binding; reject wrong chain/router/
factory/code/pair/path, cross-route/notional transcript transplant, native,
multihop, fee-on-transfer, BSC/ZKsync/L2 fallback, missing trace, stale native
price, and user-built policy evidence; and prove the serialized file contains
no URL, credential, private/operator/caller-supplied sender, trace frame,
unplanned/unrelated storage slot, or unrelated response payload. The only
serialized sender is the registry-fixed public simulation address; the only
serialized storage keys are the exact descriptor-derived balance/allowance
keys in the planned diff allowlist. Literal tests reject any additional account
or key. Zero balance delta produces authenticated not-applicable tax evidence;
positive input/output tax, short receipt, and revert fixtures remain blocking.
Missing evidence preserves the structural candidate and produces an explicit
strict-incomplete record. Task 4 must reread this exact Shadow sidecar offline, verify its
physical and set hashes, reconstruct the same cost rows/policy facts through a
private sealed adapter, and fail on any one-byte or cross-run mutation; after
the input binding, live RPC, original transcript paths, and connector reads are
patched to raise.

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

The ledger is descriptor-relative under `routes/shadow/ledger/<run_id>/` and
its only permitted regular-file members are `started.json,verification.json,
terminal.json,service.json,runtime.json,candidate.json,
candidate-primary-guard.json,candidate-schedule-envelope.json,
candidate-commit.json`; presence follows the
matrix below and every other member is unsafe. Task 3 writes only the first
four contracts, Task 6 activates runtime, and Task 4/5 full-run installation
activates candidate plus exactly one authorization side file and the optional
post-B commit. Before Task 4, candidate absent requires all three later files
absent. After Task 4, candidate absent plus exactly one authorization side file
is an explicit `candidate_pending` closure that blocks gate/release and is
protected for reconciliation; a commit without that authorization is unsafe.
Candidate present requires the strict guard/schedule-envelope XOR and physical
SHA equality frozen in Task 4, while installed additionally requires the exact
candidate commit. Task 3 knows
the reserved filenames but deliberately reports any present candidate closure
as `candidate_contract_not_available`; Task 4 upgrades the same loader with the
exact schemas, so Task 3 can GREEN independently without later widening the
directory allowlist. It is never an
append-only JSONL. Install every exact-schema canonical event with
`O_EXCL|O_NOFOLLOW`, regular/nlink checks, file/directory `fsync`, and no
overwrite. `started` records explicit run ID, phase, phase-state SHA,
nullable phase-transition ID, invocation ID, UTC start, boot ID, and
`monotonic_ns`. Implicit canary requires JSON null; full requires the exact
lowercase 64-hex ID returned by the active phase helper. Started, audit, and
joint pointer values must match. `verification` records
the closed primary failure class, orthogonal process/interference/error counts,
bounded stage/result evidence, and exact nullable
`run_capture_admission_sha256,run_admission_sha256` plus
`storage_admission_status=verified|not_evaluated`; `terminal` repeats those
three storage fields and records exact outcome,
lock-acquired, duration status/value, cohort ID, verification SHA,
`runtime_evidence_sha256` (null with `not_evaluated` until Task 6), and
committed joint-pointer SHA; `service`
records only the normalized systemd result evidence. Runner/reconciler races
have exactly one terminal winner; identical retries are idempotent and
conflicting retries fail closed.

Freeze exact schemas now, including Task 6 fields so later deployment does not
silently widen Task 3 evidence. `started.json` is
`route_shadow_run_started/v1` with only
`schema,run_id,dispatch_id,phase,phase_state_sha256,phase_transition_id,
invocation_id,started_at,boot_id,monotonic_ns` and at most 4 KiB.
`verification.json` is `route_shadow_run_verification/v1` with only
`schema,run_id,dispatch_id,started_sha256,verified_at,primary_failure_class,
collector_process_started_count,collector_process_reaped_count,
orphan_process_count,primary_publication_interference_count,core_orphan_count,
pointer_interference_count,lineage_error_count,unsafe_path_error_count,
source_generation_error_count,resource_limit_error_count,
runtime_limit_error_count,last_completed_stage,result_status,
typed_source_manifest_sha256,route_cost_evidence_sha256,
run_capture_admission_sha256,
run_admission_sha256,storage_admission_status,reason_codes` and at most 8 KiB.
Counts are bounded nonnegative integers and reasons are sorted unique closed
literals. `last_completed_stage` is exactly
`none|input_capture|universe|collection|core|audit|joint_pointer` and
`result_status` is `verified|failed|not_evaluated`; they are bounded facts, not
free-form logs or exception text.
`terminal.json` is `route_shadow_run_terminal/v1` with only
`schema,run_id,dispatch_id,outcome,finished_at,lock_acquired,duration_status,
duration_seconds,route_cohort_id,started_sha256,verification_sha256,
runtime_evidence_sha256,run_capture_admission_sha256,
run_admission_sha256,storage_admission_status,typed_source_manifest_sha256,
route_cost_evidence_sha256,joint_pointer_sha256,reason_code`
and at most 4 KiB. Each nonnull `started_sha256` or `verification_sha256`
binds the corresponding present member's exact bytes; null requires that member
to be absent under the matrix below. Every repeated run/dispatch/storage field
must agree.

Manual runs require `dispatch_id=null,invocation_id=null`; Task 6
schedule-linked runs require both as lowercase 32-hex and require
`invocation_id=run_id`, while `dispatch_id` matches the unique schedule link.
Task 3 fixtures may keep Task 6-only runtime/storage fields null only under
their explicit `not_evaluated` status/reason matrices. Unknown keys,
cross-run/cross-dispatch hashes, extra bytes, or a verification/terminal
transplant fail. The terminal outcome set is exactly
`success|failed|timeout|oom|unexplained|skipped_locked` with the existing
lock/duration nullable matrix.

Freeze verification consistency rather than trusting a caller-selected primary
label. At terminalization,
`collector_process_started_count = collector_process_reaped_count +
orphan_process_count`; a negative term, a reaped count above started, or an
unobserved child set is `not_evaluated`. `primary_failure_class=none` requires
all error/interference/orphan counts to be zero, `result_status=verified`, and
the successful stage/result matrix. Every non-`none` class must be justified by
its corresponding nonzero count or terminal outcome: source drift by
`source_generation_error_count`, lineage/unsafe/resource/runtime/pointer/core
classes by their named counts, `oom|timeout` by the matching terminal outcome,
and transient/unexplained by their closed terminal/result reasons. If more than
one condition exists, select the first literal in the fixed severity order
`unsafe_path,lineage_invalid,source_generation_drift,resource_limit,
runtime_limits_unverified,pointer_interference,orphan_core,oom,timeout,
transient_collection,unexplained`; all orthogonal counts remain serialized.
Gate replay recomputes this class and rejects a mismatch. A `success` terminal
requires `last_completed_stage=joint_pointer`, `result_status=verified`, all
zero-tolerance counts zero, and exact typed/cost/storage/joint evidence; a failed
terminal cannot carry that successful matrix.

Freeze the ledger presence/null matrix. `lock_acquired=true` requires an exact
`verification.json` and lowercase 64-hex `verification_sha256`, even for a
failed/timeout/OOM acquired run. `lock_acquired=false` is only
`outcome=skipped_locked`; it atomically has started+terminal, no verification,
runtime, or candidate file, and null verification/runtime/cohort/
joint/typed/cost/storage SHAs with `storage_admission_status=not_evaluated`.
`lock_acquired=null` is only the synthetic unexplained lock-boundary closure
and is terminal-only: `started_sha256=null`, no started/verification/runtime/
candidate, and all other evidence SHAs null. It represents the scheduled
ExecStopPost window in which lock ownership cannot be proved, not a fabricated
start time or phase. Both non-owner cases use
`duration_status=not_evaluated` and null duration.

Enumerate the four closures explicitly. A manual busy run has started+terminal
and no service. A schedule-linked busy run has started+terminal and must later
have service binding that terminal. A manual process killed before its durable
started event has no reconstructible supervisor evidence, produces no synthetic
ledger, and is excluded from every scheduled/gate population. A schedule-linked
process killed before durable started has terminal+service only; ExecStopPost
uses its known run/dispatch IDs, null started/verification/runtime SHAs,
`lock_acquired=null`, and exact reason `pre_started_lock_state_unknown`.
There is no legal service-only state. Task 6 `skipped_locked` and synthetic
services bind the exact terminal and use null runtime SHA only with the closed
runtime-gap reason. A Task 3 manual acquired run has verification but no
runtime, candidate, or service and records runtime/storage as explicit
not-evaluated. A Task 6 lock-acquired worker has runtime before source reads; a
full worker may have exactly one candidate event under its separate schema.
Service absence means manual or not-yet-reconciled, never implied success.
Missing a required member, creating a
forbidden member for the lock/phase state, or inventing a SHA for an absent file
fails replay and the gate.

All three systemd service locations use shared exact
`route_shadow_service/v1` with only
`schema,service_kind,dispatch_id,run_id,attempt_id,unit_name,invocation_id,
terminal_sha256,runtime_evidence_sha256,service_result,exit_code,exit_status,
normalized_outcome,started_at,finished_at,reason_code`, at most 4 KiB.
`service_kind` is `dispatcher|worker|ops`; `normalized_outcome` is
`success|timeout|oom|failed|unexplained`. Worker uses
`attempt_id=run_id` and binds this run terminal; dispatcher uses null run ID
and `attempt_id=dispatch_id`; ops uses its own 32-hex invocation as attempt ID
and binds its ops terminal. Task 3 produces only manual/worker fixtures; Task 6
uses the same frozen schema for all three kinds. Every service evidence record
binds an exact terminal SHA; runtime SHA is exact for a started service that
reached runtime capture and JSON null only for its named pre-runtime synthetic
closure. A terminal never predicts a future service SHA.
Freeze the systemd normalization matrix. Only literal
`SERVICE_RESULT=success,EXIT_CODE=exited,EXIT_STATUS=0` plus a matching terminal
normalizes to `success`; `timeout` normalizes to `timeout`, `oom-kill` to `oom`,
and the closed results `exit-code|signal|core-dump|watchdog|resources|protocol|
start-limit-hit` normalize to `failed`. Empty, unknown, contradictory, or
unparseable values normalize to `unexplained` and hard-block. `exit_code` is
exactly `exited|killed|dumped|null`; `exit_status` is a bounded canonical
decimal/signal token or null under the literal result matrix. Canonical UTC
timestamps satisfy `finished_at >= started_at`; unit, invocation, attempt,
dispatch/run, terminal SHA, and runtime SHA must reproduce the owning evidence.
Synthetic pre-start records first install the terminal, bind its SHA here, use
null runtime SHA, and require `normalized_outcome=unexplained` with
`reason_code=service_evidence_gap`.
No `SERVICE_RESULT=success` alone can turn a missing/corrupt terminal into a
success. Tests freeze every literal mapping, timeout/OOM/signal cases, unknown
values, and cross-kind/transplanted hashes.
Task 3, before Task 5 exists, requires both storage SHAs null and status
`not_evaluated`; such a run can exercise private Shadow mechanics but cannot
advance a storage-backed phase gate. Task 5 makes both lowercase 64-hex and
verified only for a production run that reached a B decision; earlier
production failures follow the staged matrix below. For each
`(operation,subject_id)` there is at
most one admitted nonterminal owner; a second A0/B is interference rather than
a retry. Verification, terminal, capture lease, storage plan, and installed
paths must all bind the same A0/B pair. B1/B2 for one run and cross-run terminal
transplant fixtures fail and keep the outstanding reservation charged.
Freeze the terminal matrix. No Task 5 adapter or failure before any A0
decision has both hashes null, status `not_evaluated`, and a closed
`storage_not_evaluated|input_capture_unavailable` reason. Any rejected or
admitted A0 decision has exact `run_capture_admission_sha256`, null run-B hash,
status `verified`, and a reason consistent with A0's admitted flag and capture
state. Once any B decision is durable, both hashes are exact and status is
`verified`, even when B denies the run; its terminal reason must reproduce the
B decision. Success requires both admissions to be admitted and exact
A0 -> capture -> B -> installed-path lineage. B-only, swapped/cross-run hashes,
nulls inconsistent with the reason, or success with a rejected decision is
interference.

For every acquired run that reached cost-sidecar/core construction,
verification and terminal repeat the exact typed-source-manifest SHA from the
reread core/raw lineage and the independently reread Shadow cost-sidecar SHA;
both are null only when their construction stage was
never reached under the closed failure-stage matrix. Success requires both
nonnull and exact cost agreement with audit/joint pointer. A missing cost sidecar,
one-byte mutation, logical/physical hash substitution, or typed/cost SHA
transplant between runs is lineage invalid and never candidate-ready.

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

Run: `python3 -m unittest tests.test_run_route_shadow tests.test_route_shadow_authority tests.test_route_cost_evidence tests.test_dex_route_costs tests.test_fetch_dex_depth tests.test_route_collection tests.test_collection_cycle tests.test_route_shadow_inputs tests.test_route_universe tests.test_route_publication tests.test_framework -v`

The focused and real-3.8 runs load both tracked registry config files from the
verified project root, freeze their canonical known bytes/SHA, and exercise the
authoritative-source/code-identity fixture; a test-only in-memory registry is
not sufficient for GREEN.

Expected: PASS, including generation drift, collector terminal rows, deadline,
lock contention, fork-FD release, source-bound DEX context, ledger races, and
Python 3.8 grammar.

Also run the same focused suites (excluding only `tests.test_framework`) under
real CPython 3.8.10 and explicitly import `scripts.run_route_shadow`,
`scripts.collect_route_cohort`, `scripts.collection_lock_evidence`,
`scripts.route_shadow_authority`,
`scripts.route_cost_evidence`, `scripts.dex_route_costs`,
`scripts.fetch_dex_depth`,
`scripts.run_collection_cycle`, `scripts.route_shadow_inputs`,
`scripts.route_universe`, and `scripts.route_publication`. This real-runtime
gate covers every Python module changed by Task 3; an unavailable runtime is a
commit blocker.

- [ ] **Step 7: Commit**

```bash
git add config/route_cost_adapters.json config/route_cost_connector_keys.json scripts/run_route_shadow.py scripts/collect_route_cohort.py scripts/collection_lock_evidence.py scripts/route_shadow_authority.py scripts/route_cost_evidence.py scripts/dex_route_costs.py scripts/fetch_dex_depth.py scripts/run_collection_cycle.py scripts/route_shadow_inputs.py scripts/route_universe.py scripts/route_publication.py tests/test_run_route_shadow.py tests/test_route_shadow_authority.py tests/test_route_cost_evidence.py tests/test_dex_route_costs.py tests/test_fetch_dex_depth.py tests/test_collection_cycle.py tests/test_route_collection.py tests/test_route_shadow_inputs.py tests/test_route_universe.py tests/test_route_publication.py
git commit -m "feat(routes): orchestrate bounded shadow cohorts"
```

Add a GitHub commit comment proving the public pointer sentinel and lock-priority behavior.

### Task 4: Phase authority, historical gates, and the sole public promotion boundary

**Files:**
- Create: `scripts/route_shadow_gate.py`
- Create: `scripts/promote_route_opportunities.py`
- Create: `scripts/route_candidate_inputs.py`
- Create: `scripts/route_root_binding.py`
- Create: `tests/test_route_shadow_gate.py`
- Create: `tests/test_route_candidate_inputs.py`
- Create: `tests/test_route_root_binding.py`
- Modify: `scripts/run_route_shadow.py`
- Modify: `scripts/route_shadow_audit.py`
- Modify: `scripts/route_publication.py`
- Modify: `scripts/collect_route_cohort.py`
- Modify: `scripts/route_opportunity.py`
- Modify: `scripts/execution_cost_components.py`
- Modify: `scripts/route_cost_evidence.py`
- Modify: `scripts/dex_route_costs.py`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `dashboard/opportunity_facts.py`
- Modify: `dashboard/freshness.py`
- Modify: `dashboard/server.py`
- Modify: `dashboard/static/app.js`
- Modify: `tests/test_run_route_shadow.py`
- Modify: `tests/test_route_shadow_audit.py`
- Modify: `tests/test_route_publication.py`
- Modify: `tests/test_route_opportunity.py`
- Modify: `tests/test_execution_cost_components.py`
- Modify: `tests/test_route_collection.py`
- Modify: `tests/test_route_cost_evidence.py`
- Modify: `tests/test_dex_route_costs.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_opportunity_api.py`
- Modify: `tests/test_freshness.py`
- Modify: `tests/test_opportunity_frontend.py`
- Modify: `tests/test_dashboard.py`

**Interfaces:**
- Produces fixed public `evaluate_phase(data_dir: Path, *, phase: str,
  evaluated_at: datetime, population_cutoff: Optional[datetime] = None) ->
  dict`.
- Produces fixed public `transition_shadow_phase(data_dir: Path) -> dict` and
  `transition_shadow_phase_held_lock(data_dir: Path, *,
  collection_lock_fd: int) -> dict`.
- Produces fixed public `install_complete_route_candidate(data_dir: Path, *,
  shadow_run_id: str, expected_shadow_pointer_sha256: str) -> dict` and its
  held-lock form with only the additional validated `collection_lock_fd`.
- Replaces legacy finalizer with: `finalize_route_opportunity_bundle(data_dir:
  Path, *, shadow_run_id: str, expected_shadow_pointer_sha256: str) -> dict`;
  no artifact/input/path/dependency/clock arguments remain.
- Produces internal: `build_candidate_input_binding(data_dir: Path, *, shadow_run_id: str, expected_shadow_pointer_sha256: str, evaluated_at: datetime) -> dict`.
- Produces: `open_bound_route_reader(data_dir: Path, route_root: Path) -> BoundRouteReader`.
- `BoundRouteReader` owns a verified `O_DIRECTORY|O_NOFOLLOW` descriptor for
  the canonical route root for the dashboard process lifetime. Its
  `root_binding()`, `load_latest_bundle()`, and all pointer/manifest/bundle
  member reads use `openat` relative to that same held descriptor; no caller or
  loader resolves or reopens the route-root path. Before and after each logical
  read, it rechecks the descriptor identity and the canonical named root
  identity. A mismatch fails the request and health binding closed. The
  non-path-leaking SHA exposed by live `/health` is derived from this exact
  reader object, never from a separately reopened path.
- The dashboard creates exactly one bound reader at startup and passes it into
  opportunity-fact loaders. Tests pause between the health check and pointer
  read, rename the root away, install a foreign root, then restore the original
  pathname; neither swap-away/swap-back nor an ABA pathname sequence can make
  the held reader serve foreign bytes or report the foreign binding as valid.
  Market-input health binding follows the same rule: process-lifetime directory
  descriptors where one root owns the files, or one descriptor-captured file
  set per request whose exact digest is the health digest used by that request.
- Produces: `require_public_promotion_ready(data_dir: Path, *, candidate_route_cohort_id: str, evaluated_at: datetime, expected_app_sha: str, expected_static_asset_sha: str, base_url: Optional[str]) -> dict`.
- Produces fixed public `promote_complete_route_candidate(data_dir: Path, *,
  candidate_route_cohort_id: str, expected_app_sha: str,
  expected_static_asset_sha: str, base_url: str) -> dict`.
- Produces fixed public `rollback_route_promotion(data_dir: Path, *,
  source_transition_id: str, expected_app_sha: str,
  expected_static_asset_sha: str, base_url: str) -> dict`.
- Produces: `check_promotion_candidate(data_dir: Path, *, candidate_route_cohort_id: str, prospective_pointer_sha256: str, evaluated_at: datetime, expected_app_sha: str, expected_static_asset_sha: str, base_url: Optional[str]) -> dict`.
- Produces: `check_rollback_target(data_dir: Path, *, source_transition_id: str, target_pointer_sha256: str, evaluated_at: datetime, expected_app_sha: str, expected_static_asset_sha: str, base_url: str) -> dict`.
- `evaluate_phase()` descriptor-safely enumerates the complete ledger beneath
  `data_dir`; it never accepts a caller-provided history sequence. Promotion
  loads an installed immutable complete candidate by identity; it never trusts
  a caller-provided bundle mapping.
- Task 4 defines the narrow `StorageAdmissionContract` protocol consumed by
  gate/state-change code. It has exactly two methods:
  `admit_held_lock(data_dir: Path, *, collection_lock_fd: int, operation: str,
  subject_id: str, artifact_plan: Mapping, evaluated_at: datetime) -> Mapping`
  and `replay(data_dir: Path, *, admission_sha256: str,
  expected_operation: str, expected_subject_id: str,
  expected_plan_sha256: str, evaluated_at: datetime) -> Mapping`.
  Both return exact `route_storage_admission_view/v1` with only
  `schema,admission_sha256,admission`; `admission` is the complete exact
  `route_shadow_storage_admission/v1` record specified in Task 5, and
  `admission_sha256` is SHA-256 of its installed canonical UTF-8 bytes.
  `admit_held_lock` proves ownership of the collection-lock descriptor, installs
  and descriptor-rereads one atomic admission directory containing canonical
  `plan.json` plus `admission.json`, verifies the plan schema/path/maxima and
  exact hash in the record, and returns that view. `replay` is
  write-free: it descriptor-loads both immutable files by admission SHA,
  revalidates the complete canonical plan bytes, and verifies the
  expected operation/subject/artifact-plan hash and
  `0 <= evaluated_at - admission.evaluated_at <= 900s`, and returns the same
  view. Missing/swapped/mutated/cross-admission plan, unknown view/record/plan
  keys, a noncanonical byte hash, future admission, or mismatched expectation
  fails closed.
- Task 5 supplies the real
  filesystem implementation. Task 4 unit success fixtures inject a strict
  deterministic implementation of those same two methods and view/record
  schemas and an immutable plan-byte store; it cannot return a looser test-only
  result. The real-adapter equivalence corpus includes missing/mutated/
  cross-admission plans. No public or CLI state-changing interface accepts a
  clock, checker, storage adapter, callable, or dependency object. Production
  resolves the trusted clock, exact public checker, and Task 5 adapter through
  fixed private imports. A module-private test harness takes one
  identity-checked module-local test capability plus the deterministic
  dependencies; only test modules patch that private resolver. Repository
  import scans reject any non-test caller of the harness, and public-signature
  tests prove `clock=`, `release_checker=`, `storage_contract=`, `base_url=None`,
  or arbitrary callables raise before locks/writes. Phase and candidate paths
  use the same sealed mechanism. Without Task 5, the production default is
  `storage_admission=not_evaluated`: immutable v1/v2 reading remains open, but
  candidate installation, phase transition, promotion, and rollback perform
  zero writes. Task 5 replaces the default through a fixed internal import and adds
  real integration success/race tests. Thus Task 4 can commit green without
pretending live storage admission exists, and production remains fail
  closed until the next task.

The public lock-owning phase and candidate wrappers cannot bypass primary
priority. They acquire nonblocking primary-intent before collection (and routes
for candidate), use receipt-mode `route_primary_schedule_guard/v1`, and proceed
only when the next primary trigger clears the complete local bound: operation
`phase_transition` uses 31 seconds/30-second hold, while
`candidate_install` uses 91 seconds/90-second hold. They release/abort at the
monotonic deadline and never bootstrap. A killed manual operation never resumes
forward under a newly sampled guard; its ordinary phase/candidate recovery
owned-cleans or terminalizes before a fresh attempt. The exported held-lock variants
are reachable only from a descriptor-validated scheduled execution: phase
requires the exact current ops attempt/dispatch envelope, and candidate
requires the exact current worker run/dispatch/runtime/slot link within Task
6's entry deadline. They require the already-held collection FD but deliberately
do not acquire or require primary-intent: the validated scheduled envelope is
their mutually exclusive authority. A manual wrapper without primary-intent
ownership, or a scheduled held-lock caller with a manual run ID or transplanted/
missing envelope, is rejected before writes. Legacy finalize delegates to one of these
two fixed paths and has no third lock route. Real-process barriers at `*:05`
and `00:30` prove a manual transition/finalize either finishes inside the
proved blackout or exits before collection, never causing a primary busy
receipt.

The sole nonpublic exception is the manual-full continuation owned by
`run_shadow_once()` itself: after the run releases its first collection-lock
phase, the wrapper obtains primary-intent and a candidate-install guard, then
reacquires collection/routes and calls an internal manual-held form carrying
the in-memory exact guard bytes/expected SHA plus the sealed run/pointer
continuation. The internal form replays those bytes under lock and is the sole
writer that installs the guard before any candidate B/work. This form is not
exported, accepts no caller lock/guard object, and rejects any continuation not
created by the same in-process public runner. It exists only to avoid recursive
flock while still closing `candidate.json` and the run terminal; it cannot be
used by a scheduled worker or legacy finalize. Real-process tests cover manual
full success, failed guard, kill between phases, a competing primary at both
lock boundaries, and prove no recursive collection acquisition occurs.

- [ ] **Step 1: Write every negative gate as a failing test**

Build immutable ledger/core/audit/universe/baseline fixtures and reject: a
canary observation span below `24 * 3600` seconds or fewer than 85 acquired
runs; a full observation span below `7 * 24 * 3600` seconds from the first
valid full cohort or fewer than 500 unique valid full cohorts; scheduled-slot
reliability below 99% canary or 99.5% full; acquired-run valid rate below 99%
canary or 99.5% promotion; pooled leg or timing availability below 95% in
canary or 99% in full; conditional skew below 99%; pooled p95 passing skew
above 30 seconds; any passing route above 60 seconds; pooled p95 route age
above 90 seconds; any two-leg-available route age above 120 seconds; pooled
p95 complete duration above 75 seconds; any acquired run duration greater
than or equal to 90 seconds; any lineage, unsafe-path, OOM,
`orphan_process_count > 0`,
`primary_publication_interference_count > 0`, pointer interference,
unexplained-ledger, runtime-limit, or resource-limit verification error;
storage pressure; any missing/duplicate/manual/invalid/failed/skipped or late
expected primary-run receipt, either primary overflow marker; and any
`not_evaluated` required metric.
Also reject any scheduled dispatcher reservation in the selected window that
has no exact terminal outcome or no unique linked worker run when a worker was
started. Scheduled, admitted, blocked-storage, phase-invalid, start-failed,
worker-linked, and unexplained counts are reported separately. A scheduled
success is one completed expected slot with one exact `worker_started`
terminal and one uniquely linked lock-acquired worker plus reconciled service
evidence. Every other completed expected slot, including `blocked_storage`,
`invalid_phase`, `worker_start_failed`, unexplained, or a
linked worker that never acquired the lock, is a scheduled-reliability
failure. Only actual lock-acquired worker runs enter the separate valid-rate
denominator, and every unexplained scheduled gap remains a hard gate failure. Every linked worker must
have exact reconciled systemd service evidence. Transition evaluates through
the latest contiguous complete scheduled slot under the no-worker-terminal and
bounded-current-in-progress rules below; it never assumes an overdue missing
service record will later be successful or moves the cutoff behind an already
terminal dispatcher failure.
An `invalid_trigger` record has no canonical slot/scheduled-for and therefore
never completes or enters the denominator for an expected grid point. It is a
separate hard-block receipt, while the expected slot remains missing/
unexplained unless another unique valid timer reservation claims it; a manual
invalid invocation cannot manufacture either success or a counted failure.

Use exact inclusive boundaries: 24 hours and 85 runs pass; seven days and 500
unique valid cohorts pass; 90 seconds fails. Scheduled reliability compares
`successful_slots * 100 >= completed_expected_slots * 99` in canary and
`successful_slots * 200 >= completed_expected_slots * 199` in full. The sole
currently due, valid in-progress slot is excluded from both integers until it
becomes complete; manual and duplicate/cross-slot invocations never enter
either integer. An acquired-run valid-rate comparison is
`valid * 100 >= acquired * 99` for canary and
`valid * 200 >= acquired * 199` for full promotion. Conditional skew uses the
same integer-count method. Leg and timing availability use
`available * 20 >= total * 19` in canary and
`available * 100 >= total * 99` in full. Exactly 30 seconds p95 and 60 seconds
maximum passing skew pass; exactly 90 seconds p95 and 120 seconds maximum route
age pass. These availability requirements prevent a tiny surviving
conditional-skew denominator from passing a mostly unavailable population;
the independent route-age gates prevent synchronized but stale legs from
passing. `evaluated_at` is an explicit input to a pure gate; no evaluator reads
wall clock. Uniqueness is independently required for run ID, cohort ID, audit
SHA, and joint-pointer SHA, so duplicated evidence cannot inflate a
denominator.

Windows are deterministic rolling suffixes ending at `population_cutoff`, the
latest contiguous complete scheduled slot at or before `evaluated_at`. A
no-worker terminal (`blocked_storage|invalid_phase|worker_start_failed`) is
itself a complete slot and remains in the population; `worker_started` is
complete only with its unique linked worker terminal and systemd service
evidence. A supplied cutoff is only an assertion of that derived value, never
a way to omit a failure. Canary uses
`[population_cutoff - 24h, population_cutoff]` and full promotion uses
`[population_cutoff - 7d, population_cutoff]`. The readiness population is
derived exclusively from the unique timer reservation for each expected grid
slot whose exact `worker_started` terminal, worker ledger, and systemd service
evidence form one unambiguous link. Assign a linked run to the rolling window
by its dispatch reservation's canonical `scheduled_for`, not the usually later
worker `started_at`. A manual invocation, an unlinked run, or a run linked to a
duplicate/cross-slot reservation is reported separately and never enters the
85/500 count, acquired or valid denominator, availability/skew/age samples, or
duration percentiles. It cannot compensate for a missing, blocked, failed, or
unexplained scheduled slot.

Enumerate every expected scheduled slot whose `scheduled_for` lies in the
inclusive interval; future reservations or runs fail. Canary additionally
requires the first trustworthy scheduled canary acquired-run evidence at or
before the lower bound. Full requires both its immutable phase transition and
its first valid scheduled full cohort at or before the seven-day lower bound.
Slots inside the window cannot be omitted; entries before the boundary
establish phase age but do not dilute the current rate/percentile population.
Every acquired scheduled full run after the transition, including failures
before the first valid full cohort, enters the denominator when its linked
slot lies inside the rolling window. Tests prove 500 otherwise-valid manual
runs cannot satisfy promotion, a run starting after its in-window
`scheduled_for` remains included, and duplicate or cross-slot links fail
closed.

Reconstruct the literal 15-minute timer grid from the lower window boundary
through `evaluated_at`, not merely through the last ledger entry. At most the
single currently due slot may remain in progress, and only while
`evaluated_at - scheduled_for <= 300s`; it is reported and excluded until its
terminal evidence arrives. Any older missing/unterminal slot is unexplained
and blocks, so stopping the timer cannot freeze an old passing cutoff. A
completed blocked/start-failed slot advances the cutoff and stays in all
scheduled counts; a later successful slot cannot hide it. Tests cover a
blocked current slot followed by success, a legitimately in-progress current
slot, a timer stopped for one day, and the exact 300-second boundary.

Compute valid-joint-pointer rate as ledger entries whose committed pointer SHA
can be reconstructed from that run's immutable audit, universe, baseline, and
immutable core bundle divided by lock-acquired runs. Historical verification
never compares an old SHA to the moving `routes/shadow/latest.json`; rebuild
the exact `route_shadow_pointer/v1` canonical bytes for each run and compare
their SHA to its ledger entry. Do not aggregate per-run p95 values: reconstruct
every passing-route skew sample from each validated immutable core and compute
one pooled nearest-rank p95 over the selected 24-hour or seven-day population.
Aggregate availability and skew ratios from their integer counts. Duration p95
likewise uses every lock-acquired terminal duration, including failed, timeout,
and OOM runs; `skipped_locked` is excluded. Compare ratios by integer cross
multiplication, never rounded display text. Reconstruct route-age samples from
the same immutable core/audit evidence rather than aggregating per-run p95;
test a one-route conditional-skew denominator inside a mostly unavailable
population and two old but equally timestamped legs as explicit failures.

`lock_acquired: null`, an unknown failure class, a missing/replaced
verification record, or non-reconstructible immutable evidence makes the gate
`not_evaluated` and blocks widening/promotion. Until Task 5 supplies validated
storage admission and Task 6 supplies live runtime-limit evidence, the
production gate remains blocked with explicit `not_evaluated` reasons; neither
condition may default to passing.

- [ ] **Step 2: Write public-pointer bypass and strict-evidence RED tests**

Seed `routes/latest.json` with sentinel bytes. Prove the legacy
`publish_complete_route_bundle()` and
`finalize_route_opportunity_bundle()` paths can currently advance it, then
require their replacements to install an immutable complete candidate without
touching the sentinel. Every negative gate, release-checker failure, exception,
or interruption before the already-prevalidated atomic pointer replacement
must leave the prior pointer byte-identical. A kill after that replace may
leave only the exact prospective pointer; prepared-record reconciliation must
prove and terminalize it. Mixed, unvalidated, or unrelated pointer bytes are
never acceptable.

Do not mutate any immutable v1 contract in place. Introduce write-only new
contracts `route_opportunity/v2`, `route_opportunity_manifest/v2`,
`route_opportunity_pointer/v2`, `route_opportunities/v2`, and
`route_opportunity_sqlite/v2`, plus `route_cost_components/v2`; v2 adds `strict_evidence_complete` and exact
literal `route_age_basis=oldest_leg_observation` to its exact row/CSV/SQLite
contract and binds the intended-scope SHA. All newly installed candidates and
every promotion must be v2. The reader keeps a separate strict
read-only v1 validator using the frozen old field/column sets, so the currently
published v1 pointer and protected v1 rollback bundles remain readable without
rewriting their bytes. A v1 candidate can never satisfy the new promotion gate.

Make the v2 delta literal rather than redefining v1 implicitly. The exact v2
opportunity row key set is frozen v1 `OPPORTUNITY_FIELDS` plus only
`strict_evidence_complete` and `route_age_basis`; no v1 key is removed or
renamed. Its existing `contract_version` field is literal string `"2"` (v1
remains literal `"1"`). V2 deliberately narrows the existing
`opportunity_class` semantic enum to
`policy_qualified_candidate|research_estimate|unavailable`; the v1 validator
retains its frozen enum, while v2 rejects `executable_candidate`. The exact v2 bundle key set is frozen v1 `_COMPLETE_BUNDLE_FIELDS`
plus `intended_scope_sha256,shadow_pointer_sha256,phase_state_sha256,
candidate_input_manifest_sha256,route_cost_evidence_sha256`. The v2 manifest key set is the frozen v1 set
`schema,bundle_stage,route_cohort_id,core_manifest_sha256,core_pointer_sha256,input_generations,requested_notionals_usd,counts,files`
plus `intended_scope_sha256,shadow_pointer_sha256,phase_state_sha256,
candidate_input_manifest_sha256,route_cost_evidence_sha256`; the v2 pointer key set is frozen v1
`schema,bundle_stage,route_cohort_id,manifest_sha256,core_manifest_sha256,core_pointer_sha256`
plus `intended_scope_sha256,shadow_pointer_sha256,phase_state_sha256,
candidate_input_manifest_sha256,route_cost_evidence_sha256`. Their schema/stage literals are the named v2
contracts above.

The v2 opportunity CSV header is the UTF-8 sequence of lexically sorted exact
v2 row keys followed by `row_json`, exactly preserving the v1 quoting/newline
rules; its manifest file schema is `route_opportunities/v2`. V2 SQLite retains
the frozen v1 tables/indexes and inserts NOT NULL text columns
`strict_evidence_complete`, then `route_age_basis`, immediately after
`strict_eligible` and before `row_json` in `route_opportunities`; completeness
is exactly text `true|false`, basis is the one literal, and `row_json` must
exactly reproduce the v2 row. Set and validate `PRAGMA user_version=2` while
    retaining the frozen v1 application ID; v1 remains user_version 1. Its `bundle_metadata` includes
the v2 bundle schema, intended scope, shadow pointer, phase-state,
candidate-input-manifest, and route-cost-evidence SHAs; its logical hash domain is
canonical JSON `{"schema":"route_opportunity_sqlite/v2","bundle":<exact-v2-bundle>}`.

V2 cost-component rows retain the frozen v1 column/key order but use literal
`contract_version="2"` and add exactly one allowed/required strict route-level
type `submission_risk_bound`; the v1 validator keeps its old enum and can never
accept it. The row is `leg=route`, status `authenticated`, nonembedded/additive,
uses basis `signed_per_leg_calldata_loss_bounds/v1`, and binds the
exact observed policy-member/snapshot SHAs and expiry. Its USD amount and the
v2 strict-net fields reproduce the signed bps ceiling formula above. Missing,
stale, failed, duplicated, negative, float, or caller-created bounds make the
scenario strict-incomplete; zero is a valid authenticated bound. The v2 cost
CSV/file schema is `route_cost_components/v2`, SQLite retains the same column
layout but validates the v2 literal/type, and v1 read-only bundles remain byte-
and semantics-compatible. Exact v1/v2 enum, total, zero/max-bound, rounding,
and downgrade fixtures prevent an authenticated risk bound from being dropped
while retaining a positive strict edge. Known answers cover CEX-DEX, DEX-CEX,
DEX-DEX, zero/max bps, raw amount 1 with bps 1, token-decimal conversion, and
both legs simultaneously hitting their integer boundaries.

Build intended scope as canonical JSON
`{"schema":"route_opportunity_scope/v1","members":[...]}` where each member
has exactly `route_id` and `requested_notional_usd`, candidate route IDs are
lexically sorted, and notionals use fixed numeric order
`1000,5000,10000,50000,100000` as canonical decimal strings. SHA-256 of those
exact UTF-8 canonical bytes is `intended_scope_sha256`, repeated identically in
bundle, manifest, and pointer. Exact-schema tests freeze row keys, CSV order,
SQLite DDL/metadata/logical hash, every outer key set, and scope bytes/hash for
both readers. Missing, mismatched, or cross-candidate cost SHA fails every v2
bundle/manifest/pointer/SQLite layer; v1 readers neither expect nor synthesize
that field.
The shadow pointer SHA is the exact Task 2 joint-pointer byte hash used to build
the candidate, and `phase_state_sha256` must reproduce that pointer's validated
full phase. Both are repeated identically in v2 bundle/manifest/pointer and
resolved through immutable shadow evidence on every load. Promotion requires
the passing gate's anchored pointer to equal this exact field; no implicit
"latest" lookup or manual cohort can supply lineage.

Candidate construction has one run-bound, caller-independent input boundary.
`build_candidate_input_binding()` descriptor-loads the named immutable joint
shadow result and its core/raw members, then deterministically constructs all
route-by-five-notional opportunity inputs through production adapters. It
accepts no `opportunity_inputs`, source-root, fee-profile path, inventory path,
or profile ID from a caller. Typed rules/conversions/pool state come only from
the same retained raw run and exact core member hashes described in Task 3.
Private evidence may come only from the fixed production settings
`MARKET_CEX_PRIVATE_FEE_PROFILE` and
`MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE`; each nonblank setting is opened once
through its existing owner/mode/nofollow descriptor boundary, parsed from
those captured bytes, and must contain exactly one opaque profile identity.
An unset setting produces canonical status `missing` and explicit unavailable
cost/capacity facts. A configured but unsafe, stale, changed, or invalid file
makes candidate installation `not_evaluated` before artifact construction; it
is never treated as absent and no public/default fee or zero inventory is
substituted.
Bound pre-admission memory and reads: each private profile is at most 4 MiB,
the canonical opportunity-input projection is at most 32 MiB, each retained
typed source member keeps its existing raw cap, canonical `typed-sources.json`
is at most 40 MiB, exact copied `cost-evidence.json` is at most 32 MiB, and
manifest is at most 64 KiB. The five non-manifest files must also satisfy one
aggregate canonical cap of `112 MiB`; adding manifest gives an exact private-
bundle cap of `112 MiB + 64 KiB`. The bounded decoded/construction working set
is at most 384 MiB. Limits are enforced while streaming,
before decoding or list materialization; a one-byte overflow is
`input_unavailable` with zero candidate/staging writes. Tests hit every
individual limit, the simultaneous aggregate limit, decoded-working-set
limit, and each +1-byte case.

After storage admission and before candidate construction, atomically install
private mode-0700 `routes/shadow/candidate-inputs/<run_id>/` containing exactly
0600 `opportunity-inputs.json`, `typed-sources.json`, `cost-evidence.json`,
`fee-profile.json`, `inventory-profile.json`, and `manifest.json`. The cost
file is the byte-identical Task 3 sidecar, not a derived or reserialized
summary. The two profile projections
contain only the normalized rows already accepted by their loaders; no path,
account, wallet, credential, or arbitrary basis text is retained. Schema
`route_candidate_input_manifest/v1` permits only
`schema,run_id,route_cohort_id,shadow_pointer_sha256,
route_cost_evidence_sha256,evaluated_at,opportunity_input_count,
opportunity_inputs_sha256,typed_source_set_sha256,fee_profile_status,
fee_profile_id,fee_profile_generation,inventory_profile_status,
inventory_profile_id,inventory_profile_generation,files`.
Statuses are exactly `available|missing`; available IDs/generations are
lowercase 64-hex, while a missing profile has ID JSON null but generation
exactly `SHA256("[]") = 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`,
matching the frozen v1 empty-row input-generation rule. Counts are bounded nonnegative integers, and
`files` gives exact physical SHA/size for the five non-manifest allowlisted
files; `manifest.json` is hashed externally and never self-hashes. The manifest SHA is repeated in the v2 logical
bundle, SQLite metadata, manifest, and pointer. Candidate building rereads the
installed private input bundle and consumes only those bytes; mutation of the
pre-install Python objects or original external profiles has no effect.
`files` is an exact mapping with only
`opportunity-inputs.json,typed-sources.json,cost-evidence.json,
fee-profile.json,inventory-profile.json`; each value
has only `sha256,size`, with a 64-hex physical hash and bounded nonnegative
integer size. The outer `opportunity_inputs_sha256` must equal its file hash;
the outer `route_cost_evidence_sha256` must equal the cost file hash;
profile status/ID/generation are recomputed from the corresponding file rows;
run ID, cohort, shadow pointer, route-cost-evidence SHA, and evaluated-at must
match across manifest, opportunity file, loaded shadow/core, and v2 bundle.
The cost SHA is lowercase 64-hex and must reproduce the exact Task 3 private
Shadow sidecar plus its audit, joint pointer, and
verification/terminal fields. The v2 frozen
`input_generations.fee_profile_generation` and
`inventory_profile_generation` must exactly equal these recomputed values,
including the nonnull empty-row hash when missing.

Freeze the five data-file contracts. `opportunity-inputs.json` is exact
`route_candidate_opportunity_inputs/v1` with only
`schema,run_id,evaluated_at,route_cost_evidence_sha256,members`; members are sorted by canonical route ID
then numeric notional and each has exactly
`classified_opportunity,build_inputs,source_members`. `build_inputs` has only
`cohort_id,route,requested_notional_usd,common_target,buy_leg,sell_leg,buy_quote,sell_quote,buy_quote_evidence,sell_quote_evidence,buy_usd_projection,sell_usd_projection,cost_components,mode_evidence,now`.
The classified row, route/leg/evidence/projection/cost/mode mappings use their
existing exact production key sets and validators. `source_members` is a
route-level mapping with side-qualified keys; each value has exactly
`market_id,typed_role,filename` and must match that side's canonical leg ID,
one generic Task 3 role, and one retained basename. Exact matrices are:

- CEX-CEX: `buy_raw_book_response,buy_market_rules,buy_usd_conversion,
  sell_raw_book_response,sell_market_rules,sell_usd_conversion`;
- CEX-DEX: the three `buy_*` CEX keys plus
  `sell_pool_state,sell_usd_price_context`;
- DEX-CEX: `buy_pool_state,buy_usd_price_context` plus the three `sell_*` CEX
  keys;
- DEX-DEX: `buy_pool_state,buy_usd_price_context,sell_pool_state,
  sell_usd_price_context`.

The suffix maps only to generic roles
`cex_raw_book_response|cex_market_rules|quote_usd_conversion|dex_pool_state|dex_usd_price_context`.
No generic role is itself a route-level key, so two same-type legs cannot
collide. CEX-CEX, CEX-DEX, DEX-CEX, DEX-DEX, buy/sell swap, and cross-leg
transplant fixtures freeze these mappings. `common_target` is tagged exact
`route_common_target/v1` with only
`schema,asset,unit_decimals,raw_quantity,lattice_raw`; each quote is tagged
`route_quantity_quote/v1` with exactly the frozen public
`QuantityQuote` dataclass fields. Nested quote evidence tags `MarketRules`,
`FeeSemantics`, and `V2PoolState` as exact
`route_market_rules/v1`, `route_fee_semantics/v1`, and
`route_v2_pool_state/v1` objects with only their respective frozen production
dataclass fields. `scripts.route_candidate_inputs` exports the four literal,
non-reflective ordered tuples
`CANDIDATE_QUANTITY_QUOTE_FIELDS,CANDIDATE_MARKET_RULES_FIELDS,CANDIDATE_FEE_SEMANTICS_FIELDS,CANDIDATE_V2_POOL_STATE_FIELDS`;
tests compare each tuple to a separately written expected literal so a
dataclass edit cannot silently widen serialization. The recursive decoder
accepts only these five tagged dataclass types, converts each canonical numeric
string to the constructor's exact Decimal/Fraction/integer type, reconstructs
inner market-rules/fee/pool-state objects before quote-evidence validation, and
then invokes normal constructors/validators. Strict CEX and strict V2 DEX
round-trip fixtures must reproduce byte-identical classified inputs; leaving
any nested object as a mapping fails.
The ordered field tuples include derived non-init fields. Freeze exact derived
sets as `MarketRules=(record_binding_sha256)`,
`FeeSemantics=(record_binding_sha256)`, and `V2PoolState=(state_id)`;
`CommonTarget` and `QuantityQuote` have no derived fields. The decoder passes
only the complementary init fields to each constructor, then requires every
freshly derived value to equal the serialized field byte-for-byte. It never
passes an `init=False` value into a constructor and never drops it without a
comparison. Mutating each of the three derived fields independently must fail
strict replay.
Raw CEX book evidence is the one non-JSON primitive. Store it once in
`typed-sources.json` under closed role `cex_raw_book_response` as exact
`route_bytes/v1` with only
`schema,encoding,size,sha256,data`: encoding is literal `base64`, size is a
bounded nonnegative integer, SHA is over decoded bytes, and data is canonical
RFC 4648 standard base64 with required padding and no whitespace. Repeated
scenario quote evidence serializes `book.raw` only as exact
`route_bytes_ref/v1` with `schema,filename,size,sha256`; filename must resolve
the corresponding retained member. The decoder validates canonical
base64/size/SHA once, substitutes the exact `bytes` object at that sole
allowlisted field, and then performs the existing supplied-book equality and
strict replay. Bytes tags/references anywhere else fail. Bound each decoded
member by its existing raw-response cap, member count by 1024, and the
aggregate decoded set by 24 MiB. Across at most 1024 separately padded members,
canonical base64 contributes at most `32 MiB + 4 KiB`; all non-base64 member
metadata, tags, separators, and the outer JSON envelope together have a hard
4 MiB canonical-byte cap. Therefore the complete typed-source projection is
strictly below its 40 MiB file cap and within the 112 MiB five-file aggregate.
Strict CEX tests round-trip exact raw bytes and reject a one-byte mutation,
wrong padding, cross-market reference, or repeated inline raw copy.
Boundary tests use 1024 members with worst-case padding and the maximum
allowable envelope, then reject one extra member, one decoded byte, one
metadata byte, and one total canonical byte independently.
The input-binding phase before B descriptor-validates the retained raw members
against the core, descriptor-rereads and validates the exact
`route_cost_evidence_manifest/v1`, and projects opportunity objects, strict
cost/policy facts, plus typed payloads once into the two canonical files in memory, and
includes their set hash in the manifest. After B, the candidate builder reads
only the atomically installed private input files; it never reopens raw or
external profile, RPC, trace, or submission-connector paths. The cost decoder
is private/sealed, verifies the route/notional/direction/leg and fixed-block
transcript bindings, and alone may turn Task 3 observed records into strict
gas/router/tax/policy facts; a caller mapping or the existing public helper
continues to produce non-strict evidence. The decoder reconstructs only these five tagged types before invoking the
existing prepublication validator; unknown tags/keys, floats, NaN/Infinity,
duplicate route-notional pairs, noncanonical decimals, or a non-five-notional
inventory fail.

`typed-sources.json` is exact `route_candidate_typed_sources/v1` with only
`schema,run_id,members`. Members are sorted by market ID, role, then relative filename and
each has only `market_id,role,filename,source_sha256,payload`; role is from the closed
adapter set, filename is the exact basename referenced by
`source_members`, market ID must match the side-qualified reference and exact
core leg, and payload must satisfy that role's already frozen typed
source schema (for CEX, the exact market-rules or USD-conversion field set;
for DEX, the declared adapter pool-state/price schema). `source_sha256` is the
physical retained raw-member hash and must reproduce core lineage; the
manifest `typed_source_set_sha256` hashes the ordered canonical member
projection. Unknown roles, duplicate role/name pairs, absolute/traversal names,
credentials, arbitrary URLs/queries, or payload fields outside the typed
adapter contract fail.

`cost-evidence.json` is byte-for-byte the exact Task 3
`route_cost_evidence_manifest/v1` sidecar. Its physical SHA must equal the
candidate input manifest, opportunity-input outer binding, joint pointer,
audit, and ledger cost SHA. The offline validator reruns every bounded raw
transcript decoder, code/pair/call/gas/price/trace check, and Ed25519 submission
policy verification from this installed file; it never accepts only the
derived `cost_components` rows. Missing bytes, a recomputed outer manifest
around a transplanted record, an untrusted signer, or any canonical one-byte
change fails before candidate construction.

`fee-profile.json` is exact `route_candidate_fee_profile/v1` with only
`schema,status,profile_id,rows`; an available row has only
`profile_id,venue,instrument,side,taker_fee_bps,fee_asset,basis,observed_at,valid_until,source_record_sha256`,
is sorted by profile/venue/instrument/side, and all rows share the outer ID.
`inventory-profile.json` is exact `route_candidate_inventory_profile/v1` with
only `schema,status,profile_id,rows`; an available row has only
`profile_hash,market_id,asset,available_quantity,observed_at,valid_until`, is
sorted by market/asset, and all rows share the outer ID. For either missing
profile, status is literal `missing`, ID is JSON null, and rows is exactly an
empty array; available requires a 64-hex ID and nonempty rows. JSON uses the
shared UTF-8/sorted-key/no-whitespace serializer and one trailing newline;
canonical decimal quantities are strings, never JSON numbers. Exact-byte
round trips and privacy scans are mandatory.
Retention counts/protects this private bundle for every gate/promotion
reference but no dashboard/API/static path can open it. Tests cover missing
profiles, configured-invalid profiles, profile/source swaps, an arbitrary
caller mapping/path, cross-run transplant, unknown members, and exact manifest
hash propagation.

Bind release decisions to the route root used by the running Web process, not
the checker's own environment. Shared `route_root_binding.py` descriptor-opens
the real nonsymlink `MARKET_DATA_DIR` and its direct real-directory `routes`
child, rejects lexical aliases/swaps, and hashes canonical private projection
`route_reader_root_binding/v1` with only
`schema,data_root_device,data_root_inode,route_root_device,route_root_inode,route_root_name`;
the name is literal `routes` and device/inode values are positive bounded
integers. The paths and raw stat values are never exposed. Dashboard startup
uses that exact opened route root for the Opportunity reader, keeps the
binding with the reader, and rechecks descriptor/path identity on each load.
Directories are validated by type, `O_NOFOLLOW`, parent/child relationship,
and stable descriptor identity; they are never required to have `st_nlink=1`
because real directory link counts are normally two or greater and change as
subdirectories are added. The single-link rule applies only to immutable
regular evidence files. A real route root containing multiple subdirectories
must pass, while a symlink, non-directory, or replaced inode fails.
`/health` exposes only `route_reader_root_status=valid|invalid` and nullable
`route_reader_root_binding_sha256`. A divergent
`MARKET_ROUTE_DATA_DIR`, symlink, later directory replacement, or a reader that
does not share the bound root yields invalid/unavailable rather than another
digest.

The same module also proves the effective market-fact readers, because release
checking promises more than Opportunity-root equivalence. Build private exact
projection `market_reader_input_binding/v1` with only
`schema,reader_mode,members`; reader mode must be literal `sqlite`, so a live
process with `MARKET_CEX_DATA` or `MARKET_DEX_DATA` set is invalid. `members`
is sorted by the closed roles
`database,tvl,cex_depth,dex_depth,cex_execution,dex_execution,cex_lifecycle,token_registry`;
each member has only `role,relative_name,device,inode,size` and must be the
descriptor-opened regular single-link canonical child beneath
`MARKET_DATA_DIR` (`token_registry` alone uses
`admin/token_registry.json`). The database and each point-file identity must
be the exact effective object used by the corresponding server reader; missing,
override, symlink, hardlink, directory swap, wrong reader mode, or a canonical
path whose inode differs from the reader is invalid. `/health` exposes only
`market_reader_input_status=valid|invalid` and nullable
`market_reader_input_binding_sha256`; it never exposes role paths or stat
values. Readers revalidate/recompute on atomic publication so health cannot
retain an old binding while a request opens new bytes.

The promotion/rollback checker independently computes both expected bindings
from its canonical `data_dir`, requires both initial and final live `/health`
responses (already pinned to the expected application SHA) to report those
exact digests, and repeats them again at `commit_at` with the served-static check.
Add `route_root_binding_sha256` to exact `route_promotion_checker/v1` and
`market_input_binding_sha256`; add both
`observed_route_root_binding_sha256,observed_market_input_binding_sha256` to
both final-validation schemas/result hashes. Candidate/promotion/rollback cannot write a public pointer when the
checker environment is canonical but the actual dashboard systemd process
serves another route root or fact file. Task 6's first-enable transaction
consumes both live proofs before authority true. Tests run real server roots
with divergent routes, database, and TVL/depth overrides and prove initial
check, final check, enable, and commit fail while the canonical process passes
without disclosing a filesystem path.

Do not mutate the HTTP `opportunity_summary/v1` contract either. Preserve the
current exact v1 response as the default compatibility projection. Query
semantics are literal: absent `contract` or one `contract=v1` returns frozen
`opportunity_summary/v1`; one `contract=v2` returns v2; empty, repeated,
case-variant, or unknown values return HTTP 400. The dashboard and candidate/
release checker always request `contract=v2`.
The normalized contract is a mandatory dimension of every server summary/
projection cache key, ETag/conditional identity, and in-flight request owner;
it is never inferred after a cache hit. One-process tests request
v1 -> v2 -> v1 (available, unavailable, and error fixtures) and require exact
noncontaminated schemas and bodies on both v1 responses.

The exact v2 outer key set remains `availability,metadata,filters,routes` and
outer/route availability retains exact nested shape `status,reason`. V2
metadata is frozen v1 metadata plus only `evidence_contract_version`,
`route_age_basis`, `execution_claim`, and `adapter_coverage_scope`; each compact route is the frozen v1 compact-route key set
plus only `strict_evidence_complete`. Its existing metadata
`contract_version` is exactly `opportunity_summary/v2`. For a v2 bundle, completeness is a
required boolean and basis is `oldest_leg_observation`. When the active pointer
is legacy v1, metadata reports `evidence_contract_version=route_opportunity/v1`
and `route_age_basis=newest_leg_observation_legacy`; `execution_claim` is always
literal `policy_qualified_snapshot_non_atomic`, never `executable`, and legacy
`adapter_coverage_scope` is JSON null. Every v2 compact route has
`strict_evidence_complete=null`, `opportunity_class=unavailable`, and
`availability={"status":"unavailable","reason":"legacy_evidence_contract"}`.
It also sets exactly `target_token_quantity,gross_edge_usd,gross_edge_bps,net_edge_usd,net_edge_bps,capacity_quantity,cost_completeness,scenario_cost_completeness`
to JSON null, all three `cost_breakdown` values to null, and every cost
component `amount_usd`/`rate_bps` to null. Volume, timestamps, skew, source
links, and identifiers remain non-actionable evidence. It never maps a legacy
ready/eligible bit to v2 completeness or lets newest-leg freshness appear
policy-qualified/actionable in the new dashboard.

For an active v2 bundle, `adapter_coverage_scope` is exact
`route_cost_adapter_coverage_scope/v1` with only
`schema,chain_id,protocol_family,router_family,max_selected_markets,
selected_market_set_sha256,target_market_count,supported_market_count,
route_volume_coverage,tvl_coverage`. Initial literals are `chain_id="0x1"`,
`protocol_family="constant_product_v2"`,
`router_family="router02_direct"`, and `max_selected_markets=8`. Counts are
bounded nonnegative JSON integers with `supported_market_count <=
target_market_count <= 8`; each coverage value is a canonical decimal string
in `[0,1]`, or JSON null exactly when its frozen denominator is zero. The
selected-set SHA is lowercase 64-hex and still hashes the exact empty set when
the target count is zero. Counts/coverage reproduce the bound sidecar/gate
metrics. API, dashboard, release output, and final report
must display this typed scope; 100% means only that slice, never all DEX
protocols/chains. A separate full-universe summary reports unsupported V3,
Balancer, other-chain, and out-of-cohort volume/TVL without counting them as
implemented adapters.

A v2 active bundle can still be projected through frozen v1 keys for existing
consumers, but its availability/freshness is computed with the stricter v2
oldest-leg rule before v2-only fields are omitted; compatibility can never
loosen a v2 decision. The class mapping is literal and non-overclaiming:
`policy_qualified_candidate -> research_estimate`, `research_estimate ->
research_estimate`, and `unavailable -> unavailable`. For the first mapping the
frozen v1 `strict_ready_for_publication` and `strict_eligible` projection bits
are false, the v1 `strict` filter never selects it, and class/coverage counts
place it under research; retained economics are explicitly research estimates.
No v2 row can project to v1 `executable_candidate`. Frontend, API, and release tests freeze both exact outer,
metadata, route, availability, filter, and unavailable-payload shapes, verify
the v2 -> v1 class/boolean/filter/count mapping, legacy-v1 redaction in v2, and reject accidental extra keys. Release checking
accepts a valid active v1 public bundle during migration but candidate-mode
promotion requires v2.

Freeze contract-specific class filters and coverage counts. The accepted query
filter literals remain `strict|estimate|all`; for `contract=v2`, `strict`
selects only `policy_qualified_candidate`, `estimate` selects only
`research_estimate`, and `all` selects all three v2 classes. V2
`metadata.coverage.class_counts` has exactly
`policy_qualified_candidate,research_estimate,unavailable`, each a bounded
nonnegative integer recomputed from the complete unfiltered v2 inventory.
Returned-count filtering never changes those inventory counts. V1 retains its
frozen `executable_candidate,research_estimate,unavailable` count keys and its
historical filter mapping. Cross-contract cache tests require that v2 strict
never invokes the v1 executable enum and that the v2-to-v1 compatibility
projection moves every policy-qualified row into the v1 research count.

For v2 with no public pointer, return HTTP 200 with exact outer availability
`{"status":"unavailable","reason":"complete_pointer_absent"}`, normalized
filters, and empty routes. Metadata retains the full frozen v1 missing-payload
keys/values (`contract_version="opportunity_summary/v2"`, null cohort,
manifest, checked/deadline fields, `publication_status="missing"`, fixed max
age/skew, empty notionals/venues, and zero/empty coverage) plus
`evidence_contract_version=null`, `route_age_basis=null`,
`execution_claim=policy_qualified_snapshot_non_atomic`, and
`adapter_coverage_scope=null`. An existing but
invalid pointer never masquerades as missing: both contract queries use the
existing exact data-validation HTTP status/body, expose no routes or contract
metadata, and remain uncached. Tests freeze missing and invalid separately.

Freeze v1's historical newest-leg freshness semantics for read-only
compatibility and project its basis explicitly as
`newest_leg_observation_legacy`; never reinterpret or rewrite those immutable
bytes. Every v2 producer, API/dashboard projection, shared semantic validator,
candidate checker, normal commit check, and recovery replay computes route age
from the oldest leg: `evaluated_at - min(buy_state_at, sell_state_at)`, exactly
matching Task 2 audit. The v2 maximum is 120 seconds. An asymmetric route with
one leg 119 seconds old and the other 179 seconds old therefore has age 179 and
fails even when its inter-leg skew is exactly 60 seconds. Tests require gate,
candidate, dashboard, and commit to reach that same result while the unchanged
v1 bundle remains readable but non-promotable.

In v2, `strict_evidence_complete` is true when every required quote, conversion, fee,
inventory, gas, router, tax, MEV-policy, and route-mode fact is strict and
complete, regardless of profit sign. It is intrinsic pre-attestation readiness:
its computation never reads the attestation field. Define the legacy fields exactly:
`strict_ready_for_publication = strict_evidence_complete && strict_net > 0`;
`strict_eligible = strict_ready_for_publication && valid_attestation`; and
`opportunity_class == policy_qualified_candidate` if and only if `strict_eligible`.
An unavailable row has all three booleans false and no attestation.

This initial v2 deliberately makes no atomic execution claim. CEX legs have
snapshot order-book/fee/inventory evidence but no authenticated IOC/FOK/
marketable-limit submission, latency, partial-fill, reject/cancel, or fill-
receipt contract, and cross-venue legs are not atomic. Therefore no v2 row or
UI label may say `executable_candidate` or “fully executable”; the exact public
label is “Policy-qualified snapshot candidate”. A future contract may upgrade
only after adding per-venue order-policy and route-level leg-risk evidence.

Candidate installation issues and validates a publication attestation for
every `strict_evidence_complete` row, including a zero/negative-net
`research_estimate`; the attestation proves row/evidence binding, not profit.
This ordering is one-way: first rebuild intrinsic completeness, then issue the
attestation over the completed row inputs, then validate the pair. Attestation
never feeds back into `strict_evidence_complete`, so no circular state exists.
The strict-economics validator runs for every such row and permits finite
strict net less than or equal to zero while still reproducing gross amounts,
all strict component totals, capacity, ratios, and evidence hashes; it is no
longer gated only by `opportunity_class == policy_qualified_candidate`.
The dashboard and complete-bundle validator accept an attested non-positive
row only when `strict_evidence_complete=true`,
`strict_ready_for_publication=false`, `strict_eligible=false`, and strict net is
not positive. Any other non-positive attestation fails. A complete
zero/negative row counts toward 100% evidence completeness but contributes zero
positive policy-qualified candidates. Bounded estimates, assumptions, research-only
routes, and missing/invalid candidate attestations never count. The release
checker and shared semantic validator require 100% strict evidence plus valid
attestations across the intended scope even when the positive qualified count is zero.

Regression tests deploy the dual reader against an unchanged v1 pointer,
reject v1 as a promotion candidate, promote v2, and then use only the narrow
audited rollback interface below to restore the exact protected v1 pointer.
No reader deployment alone advances or rewrites `routes/latest.json`; generic
promotion never accepts v1.

The intended scope is deterministic: every canonical route with
`route_class == candidate` and all five fixed notionals
`1000,5000,10000,50000,100000`. Persist and verify its canonical scope SHA.
Only canonically classified `research_only` routes are excluded; a CLI or
caller cannot manually omit a difficult route or notional.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_gate tests.test_route_candidate_inputs tests.test_route_root_binding tests.test_route_cost_evidence tests.test_dex_route_costs tests.test_execution_cost_components tests.test_run_route_shadow tests.test_route_shadow_audit tests.test_route_publication tests.test_route_opportunity tests.test_route_collection tests.test_release_smoke tests.test_opportunity_api tests.test_freshness tests.test_opportunity_frontend tests.test_dashboard -v`

Expected: FAIL because the gate module and non-bypassable
candidate/promotion boundary are absent and strict completeness is still
coupled to positive net.

- [ ] **Step 4: Implement authoritative canary-to-full phase change**

`routes/shadow/phase.json` is the only phase authority; absence means canary.
The production runner has no phase override. An optional expected phase is an
assertion that must equal the loaded authority, never a request to change it.
The runner reads phase only after acquiring the global collection lock. The
transition command acquires that same lock, so an old canary process cannot
publish after a full transition. A full `started.json`, audit, and joint
pointer all bind the exact full phase-state SHA created before that run.

Persist exact schema `route_shadow_phase/v1` with only `schema`, `prior_phase`,
`phase`, `evaluated_at`, `gate_evidence_sha256`,
`storage_admission_sha256`, `anchored_joint_pointer_sha256`, and
`primary_schedule_guard_sha256`, `schedule_envelope_sha256`,
`phase_identity_id`, `transition_id`. Before refresh B, build exact
canonical `route_shadow_phase_identity/v1` with only
`schema,prior_phase,phase,evaluated_at,gate_evidence_sha256,anchored_joint_pointer_sha256,
primary_schedule_guard_sha256,schedule_envelope_sha256`;
its byte SHA is `phase_identity_id` and is the phase admission `subject_id`.
It excludes B, while final `transition_id` hashes the full phase fields
including B and the identity but excluding itself. A changed B therefore
changes transition ID without changing the pre-B subject or reserved parent
rule.

The two ownership fields are a strict XOR. The public manual lock-owning path
persists exact canonical `route_primary_schedule_guard/v1` bytes at planned
path `transitions/guards/<phase_identity_id>.json`; its physical SHA is
`primary_schedule_guard_sha256` and `schedule_envelope_sha256` is JSON null.
The scheduled held-lock path never acquires primary-intent and instead persists
exact `route_shadow_phase_schedule_envelope/v1` at
`transitions/schedule-envelopes/<phase_identity_id>.json`; it permits only
`schema,dispatch_id,slot_claim_sha256,reservation_sha256,
dispatcher_runtime_sha256,worker_started_sha256,worker_runtime_sha256,
worker_terminal_sha256,worker_service_sha256,ops_launch_sha256,
ops_started_sha256,ops_runtime_sha256`, all nonnull and descriptor-replayed from
one dispatch/ops attempt. Its physical SHA is `schedule_envelope_sha256` and
the guard SHA is JSON null. Both planned side files are canonical, at most
4 KiB, no-replace, included in B's exact artifact plan/actual set, and protected
with the transition. Missing/both/cross-dispatch/cross-mode evidence is
interference. Recovery loads the named bytes; it never tries to invert a SHA or
sample a fresh forward guard.
Because the final identity-addressed filename is unknown before B, the phase
artifact plan uses Task 5's single typed phase-transition-child rule for the
validated `transitions/` parent; exactly one post-B `<transition_id>.json`
whose filename/declared field equal SHA-256 of canonical transition fields
excluding `transition_id` may consume that reservation. The separate
`phase_state_sha256` is SHA-256 of exact full persisted bytes. The final actual
artifact set binds the resolved path, state-byte SHA, and identity derivation.
Generate a pre-B nonce and reserve the exact literal hidden stage path
`transitions/.staging/<phase_identity_id>.<nonce>.tmp`, literal sibling
`<phase_identity_id>.<nonce>.lease.json` and `.owner.json`, plus final dynamic
child and double footprint. After B, atomically install exact
`route_phase_stage_lease/v1` containing only
`schema,phase_identity_id,storage_admission_sha256,nonce,stage_name,transition_parent`,
then O_EXCL-create/open the stage. While still holding the winning descriptor,
capture its device/inode and install exact `route_phase_stage_owner/v1` with
only `schema,phase_identity_id,stage_lease_sha256,stage_name,stage_device,stage_inode`.
IDs/SHAs are lowercase 64-hex, nonce is lowercase 32-hex, names are ASCII
basenames at most 160 bytes, device/inode are in `1..(2**63-1)`, and each
canonical lease/owner is at most 2 KiB. Only then write/fsync/reread exact
transition bytes through that descriptor, no-replace install the derived final
child, and fsync/reread before updating phase.

Recovery enumerates operation=phase B plus exact lease/owner records and
reconciles lease-only, temp-only, final-only, and both. After restart,
lease-only cannot create a new stage; it follows the universal owned-cleanup/
abandon rule. A recovered stage without a previously valid owner is interference,
even if empty; only the live O_EXCL creator holding its descriptor may finish
owner installation. Partial or wrong bytes are cleaned only when B, lease,
owner, path, opened descriptor identity, phase identity, and nonce all prove
ownership; a foreign/mismatched child is interference. Retention permanently
binds/protects phase lease/owner evidence. Kills at lease, temp create, owner,
partial write, fsync, final install, and phase replace are tested, including
restart observation of an ownerless empty stage.
Install a separate
immutable transition record first, then update `phase.json` through
stage/write/fsync/atomic replace/reread with owned rollback. The transition ID
is the SHA of exact transition inputs. Repeating the same transition is
idempotent; conflicting reuse, `full -> canary`, skipped phases, hardlinks,
symlinks, or directory swaps fail closed. Time alone cannot advance phase. The
normal transition command runs the canary gate, anchors the last validated
canary joint pointer, and performs the atomic transition; `phase.json` is not a
dead operator-edited file. Task 6 wires this exact command into a separate
post-worker ops unit launched only after the worker's service evidence is
durable; the next timer dispatch selects the full worker.
Under the one already-held collection lock, it first writes baseline admission
C after that terminal/service, evaluates gate A using C, computes A/identity/
plan in memory, then writes transaction admission B and only then installs A
and phase files. Gate A binds C; transition/phase bind A plus B. A stale pre-run
admission cannot satisfy the gate, and no recursive lock is opened. Tests prove
the newly completed canary run can transition through C -> A -> B while the
same fixture without C remains blocked.
The same command remains explicit and idempotent for manual recovery/testing.
`transition_shadow_phase()` is the public lock-owning wrapper.
`transition_shadow_phase_held_lock()` first proves the supplied descriptor is
the owned, exclusive canonical collection lock and performs the identical
gate, post-gate storage refresh, transition, and phase commit without opening
or reacquiring that lock. Task 6 ops uses only this held-lock form, so the
operation has one lock owner and cannot deadlock through recursive `flock`.
Both interfaces share one implementation and evidence contract; a real-process
test proves the held-lock path completes without a second lock attempt.
The state-changing wrappers obtain `evaluated_at` exactly once from their
trusted UTC clock after lock acquisition; the production CLI exposes no
timestamp/backdate option. Tests inject the clock callable. Only pure
`evaluate_phase()` accepts an explicit historical instant. A window that once
passed cannot be replayed to transition after a newer overdue or missing slot;
the transition record persists the trusted current instant and a backdated
wrapper counterexample fails.

Persist each exact canonical gate evaluation first at
`routes/shadow/gates/<gate_evidence_sha256>.json`, where the filename is the
SHA-256 of the exact installed UTF-8 bytes. Schema `route_shadow_gate/v1`
permits only `schema`, `phase`, `evaluated_at`, `window_start`,
`population_cutoff`,
`population_sha256`, `storage_admission_sha256`,
`runtime_evidence_set_sha256`, `anchored_joint_pointer_sha256`, `metrics`, and
`blocking_reasons`. `population_sha256` binds the ordered immutable
schedule-slot claim bytes, schedule reservation/terminal/service bytes, linked
worker started/terminal/service/runtime/verification bytes, audit, universe,
baseline, core, and interference-receipt hashes used by evaluation. Any linked
ops evidence used to establish completion/transition readiness is included as
well. It also binds every expected selected-window `primary-runs` receipt,
primary contention receipt, and present primary-run/contention overflow marker;
missing expected primary bytes are represented by a typed missing-slot member,
not silently omitted. `runtime_evidence_set_sha256` is an additional typed projection, not a
replacement for the population binding. Mutating a claim, service, or ops
byte/hash makes semantic gate replay fail, and Task 5 protects every referenced
member.

`metrics` is not an open mapping. It is exact
`route_shadow_gate_metrics/v1` with only
`schema,observation_span_seconds,expected_slot_count,
completed_expected_slot_count,successful_slot_count,scheduled_admitted_count,
scheduled_blocked_storage_count,scheduled_invalid_phase_count,
scheduled_worker_start_failed_count,scheduled_worker_linked_count,
scheduled_unexplained_count,current_in_progress_count,duplicate_invocation_count,
manual_run_count,acquired_run_count,valid_joint_pointer_count,
unique_valid_cohort_count,leg_available_count,leg_total_count,
timing_available_count,timing_total_count,conditional_skew_within_sla_count,
conditional_skew_total_count,passing_skew_sample_count,
passing_skew_p95_seconds,passing_skew_max_seconds,route_age_sample_count,
route_age_p95_seconds,route_age_max_seconds,duration_sample_count,
duration_p95_seconds,duration_max_seconds,runtime_verified_count,
runtime_required_count,lineage_error_count,unsafe_path_error_count,
source_generation_error_count,resource_limit_error_count,
runtime_limit_error_count,orphan_process_count,core_orphan_count,
primary_publication_interference_count,pointer_interference_count,
unexplained_run_count,oom_run_count,storage_pressure_count,
primary_depth_expected_count,primary_depth_success_count,
primary_depth_late_count,primary_daily_expected_count,
primary_daily_success_count,primary_daily_late_count,primary_missing_count,
primary_invalid_count,primary_contention_count,primary_overflow_marker_count,
non_grid_hard_block_count,adapter_target_market_count,
adapter_supported_market_count,adapter_route_volume_coverage,
adapter_tvl_coverage`. Every count is a bounded nonnegative JSON integer.
Every `*_seconds` value is a canonical nonnegative decimal string without
exponent, leading plus, trailing fractional zero, or negative zero; an empty
sample is JSON null and therefore not evaluated. Count identities, sample
denominators, percentile ranks, selected window, and phase-specific thresholds
are recomputed from population bytes; no caller supplies a metric.
The two adapter coverage values are canonical decimals in `[0,1]` or JSON null
only when their frozen selected-set denominator has no nonnull volume/TVL;
their numerators sum only selected markets with static `supported` status.
Promotion readiness requires every selected priority target supported and each
nonnull coverage equal exactly 1. A smaller supported low-value subset cannot
earn 100% strict completeness by excluding a higher-priority target.

`blocking_reasons` is sorted unique and drawn only from
`insufficient_observation_span,insufficient_scheduled_runs,
scheduled_reliability_below_sla,valid_rate_below_sla,
leg_availability_below_sla,timing_availability_below_sla,
conditional_skew_below_sla,skew_p95_above_sla,skew_max_above_sla,
route_age_p95_above_sla,route_age_max_above_sla,
duration_p95_above_sla,duration_max_above_sla,primary_depth_sla_failed,
primary_daily_sla_failed,primary_evidence_invalid,storage_unavailable,
adapter_coverage_below_sla,runtime_unverified,lineage_error,unsafe_path_error,source_generation_error,
resource_limit_error,orphan_process,primary_interference,pointer_interference,
unexplained_evidence,oom,pre_admission_overflow,not_evaluated`. The evaluator
derives it deterministically from metrics plus bound structural evidence;
missing/extra metric keys, an unearned/missing reason, inconsistent count,
wrong unit/type, NaN/Inf/-0, or threshold-boundary drift invalidates the gate
artifact. Tests freeze exact empty/nonempty bytes and every pass/fail boundary.

Persist transition bytes at
`routes/shadow/transitions/<transition_id>.json` before `phase.json`. It uses
the same exact `route_shadow_phase/v1` schema and key set defined above:
`schema`, `transition_id`,
`prior_phase`, `phase`, `evaluated_at`, `gate_evidence_sha256`,
`storage_admission_sha256`, and
`anchored_joint_pointer_sha256`, `primary_schedule_guard_sha256`,
`schedule_envelope_sha256`, and `phase_identity_id`; `transition_id` is the SHA-256 of canonical
transition fields excluding itself. `phase.json` is byte-identical to that
immutable transition record, so its state SHA and history can be independently
replayed. On every load, reconcile by requiring exact byte equality and gate
artifact hash validity. Hash validity alone cannot widen phase: semantically
replay the referenced gate and require `phase=canary`, empty
`blocking_reasons`, every literal threshold passing, exact population/runtime
evidence sets, matching `evaluated_at` and anchored joint pointer, plus a valid
post-gate storage-admission record exactly matching the transition's
`storage_admission_sha256`. A gate artifact with any blocking reason remains
not-ready even if its bytes/hash are canonical. An orphan transition record with no phase pointer is
ignored as uncommitted; a phase pointer without its exact transition/gate
artifacts blocks all collection. Directory swap, no-replace, fsync, owned
rollback, and crash points before/after both installations are tested.

- [ ] **Step 5: Split candidate installation from the sole public commit**

Refactor `publish_complete_route_bundle()` into
`install_complete_route_candidate()`, which builds, stages, validates,
installs, and rereads the immutable complete bundle but never reads or writes
`routes/latest.json`, and `promote_complete_route_candidate()`, the sole code
path allowed to commit that public pointer. Change
`finalize_route_opportunity_bundle()` to candidate installation only. Remove or
make private every direct public-pointer helper so collection and shadow
orchestration cannot bypass the gate.

Candidate installation requires an exact already-committed Task 2 joint-shadow
pointer SHA, descriptor-loads that immutable shadow result, and copies its
full phase-state/shadow identities into v2 bytes. It is never invoked before
joint publication and never follows a moving latest pointer after validation.
Provide lock-owning and held-lock forms. The former acquires the canonical
collection lock and delegates; the held-lock form proves that one supplied FD
is the owned exclusive canonical collection lock, then itself acquires the
routes lock in the sole allowed collection -> routes order. It never expects or
reacquires a caller-held routes lock. The legacy standalone
`finalize_route_opportunity_bundle()` can only call this same storage-controlled
writer with explicit shadow run/pointer identities; it is not a pure artifact
builder because the existing SQLite/artifact builder writes temporary files.
No candidate SQLite, CSV, JSON, directory, or tempfile may be created outside
the post-admission in-data-dir staging contract. Task 4 success tests inject
storage while production mutation is blocked. Task 4 itself refactors the
artifact/SQLite builder to accept only its descriptor-opened in-data-dir stage
and implements the candidate lease/owner/no-replace/reread state machine; it
does not call `tempfile.TemporaryDirectory` or accept an arbitrary staging
path. Task 5 supplies only the real storage-admission/retention adapter and
replays this already-implemented state machine without injection.

For a successful scheduled full-phase run, `run_shadow_once()` performs the
literal sequence core bundle -> audit -> joint pointer commit/reread ->
scheduled held-lock candidate input binding/admission/install -> terminal.
Canary stops after the joint pointer. A manual full run uses a deliberate
two-lock-phase public orchestration: its first collection-lock phase stops
immediately after the immutable joint-pointer reread and returns a sealed
continuation containing only the run ID and that exact pointer SHA; it then
releases the collection lock, acquires `primary-intent`, evaluates the manual
candidate guard bytes entirely in memory, reacquires collection (and then routes)
in canonical order, replays the guard inputs/deadline, then persists the guard
and invokes the internal manual-held-lock candidate path before writing
the run terminal. It never calls the lock-owning candidate wrapper while
holding collection and never passes a manual run to the scheduled held-lock
entry. A crash before the second phase leaves an incomplete ledger/joint-pointer
closure; reconciliation may start one fresh guarded post-run closure because no
candidate transaction exists, but it may not infer installation or resume an
already-started candidate under a new guard. The candidate call receives only
that run ID and exact reread joint-pointer SHA and may not follow a newer
`shadow/latest.json`. Install
exact `routes/shadow/ledger/<run_id>/candidate.json` schema
`route_shadow_candidate_install/v1` with only
`schema,run_id,dispatch_id,phase,shadow_pointer_sha256,route_cohort_id,
primary_schedule_guard_sha256,schedule_envelope_sha256,status,
candidate_manifest_sha256,candidate_input_manifest_sha256,
storage_admission_sha256,candidate_commit_sha256,finished_at,reason_code`.
Status is `installed|not_installed`; installed requires all four artifact/
admission/commit hashes and null reason. Not-installed always requires
`candidate_manifest_sha256=null,candidate_commit_sha256=null`
and uses this exact reason/hash matrix:

- `primary_guard_failed`, `storage_not_evaluated`, `input_unavailable`,
  `preflight_stage_interference`, or
  `candidate_attempt_interrupted_pre_admission`: both input-manifest and
  storage-admission hashes are null; no candidate input/final/stage path was
  written.
- `candidate_attempt_interrupted_post_admission`: input-manifest is null,
  storage-admission is the exact admitted B hash, and recovery proves the input
  final absent plus only removable owned partial input stage before abandoning/
  terminalizing that reservation.
- `candidate_attempt_interrupted_after_input`: both storage-admission and
  candidate-input-manifest hashes are exact, recovery proves the private input
  final plus an absent public bundle final (or only removable owned partial
  bundle stage), and it terminalizes without claiming an installed candidate.
- `storage_pressure`: input-manifest is null and
  `storage_admission_sha256` is the exact rejected B record.
- `input_stage_interference`: storage-admission is the admitted B hash,
  input-manifest is null, and no private input final was accepted.
- `build_failed`, `validation_failed`, or `bundle_stage_interference`: both
  storage-admission and candidate-input-manifest hashes are present and exact;
  the private input final is valid/protected but no public candidate final was
  accepted.

These twelve reasons are the complete closed not-installed set.
`primary_guard_failed` is legal only for the manual post-run phase, binds the
exact failed guard side file, performs no storage admission/build write, and
allows the same reacquired collection phase to close the run terminal. Every
other manual candidate outcome requires a passed guard. Candidate validation
occurs before the final candidate rename, so a validation failure
cannot leave or name an accepted candidate manifest. Reconciliation validates
the matrix against descriptor-visible stage/final state and never fills a null
from a directory guess. Retention protects every nonnull admission/input hash
and its closure; null means that evidence must be absent. Crash fixtures cover
each boundary and prove the event, directories, and protected set agree.
Canonical bytes are at most 4 KiB, contain no exception text, and are covered
by the standing operational-receipt reserve even when candidate admission B
is denied. Candidate work is included in the run's monotonic complete duration
and must still satisfy the 90-second service boundary. Candidate failure never
rolls back an already valid joint pointer, but that pointer has no promotable
candidate and release output reports the exact not-installed reason. A crash
before this event makes the linked service/run incomplete until reconciliation;
it cannot be treated as installed from a directory alone.

The candidate B plan includes literal
`routes/shadow/ledger/<run_id>/candidate-commit.json`, at most 4 KiB. After the
six-file private input and five-file public bundle finals have both been fully
descriptor-reread, install exact `route_shadow_candidate_commit/v1` with only
`schema,run_id,dispatch_id,phase,shadow_pointer_sha256,route_cohort_id,
authorization_sha256,candidate_manifest_sha256,
candidate_input_manifest_sha256,storage_admission_sha256,committed_at`.
`authorization_sha256` is exactly the physical SHA of the mutually exclusive
manual guard or scheduled envelope; every other identity/hash equals the
validated final/B bytes. `committed_at` is one trusted canonical UTC sample at
or after both immutable finals were accepted. The physical commit SHA fills
`candidate.json.candidate_commit_sha256`, and installed event `finished_at`
cannot precede it. A commit is forbidden for every not-installed row.

Freeze candidate crash reconciliation under the collection/routes locks. An
authorization sidefile without B is `candidate_pending`; a failed manual guard
closes as `primary_guard_failed`, while a passed guard/envelope whose process
was interrupted never resumes build under new authority and closes as
`candidate_attempt_interrupted_pre_admission`. Exact admitted B with an absent
input final closes as `candidate_attempt_interrupted_post_admission` only when
the input final is absent and every visible input stage member is a
descriptor-proved owned partial that recovery can remove. Exact B plus valid
input final but an absent bundle final (or only an owned removable partial
bundle stage) closes as
`candidate_attempt_interrupted_after_input`. In both post-B cases recovery uses
the original plan to remove only descriptor-proved owned partial stages. The
post-admission/no-final case may use the storage-level abandoned terminal only
after proving every B-planned artifact absent. The after-input case cannot use
that empty-set abandonment because its valid input final remains installed;
its exact not-installed candidate event is the candidate operation completion
closure that releases the verified unused remainder. A fully valid bundle final with no
commit is not inferred as installed immediately: terminal-only recovery rereads
authorization, B, input and bundle, installs the exact commit using its recovery
sample, then installs the matching installed event. Commit-only similarly
replays and installs only the event. It performs no source/RPC/build/public-
pointer mutation and needs no fresh primary guard. Any present but invalid or
conflicting input/bundle final (as distinct from an absent final or owned
partial stage), partial/conflicting commit, cross-run authorization, or event without its exact
commit is interference and stays non-promotable. Kill fixtures cover after
authorization, B, input final, bundle final, commit, and event, plus retry at
each state; only bundle-final/commit states may converge to installed.

Candidate ownership uses the same strict manual/scheduled XOR. A manual
lock-owning install or manual full-run post phase persists its exact primary
guard at
`routes/shadow/ledger/<run_id>/candidate-primary-guard.json`, names that
physical SHA, and uses null schedule envelope. The guard must be passed except
for the exact closed `not_installed/primary_guard_failed` row above. A
scheduled held-lock install
does not take primary-intent and instead persists exact
`route_shadow_candidate_schedule_envelope/v1` at
`routes/shadow/ledger/<run_id>/candidate-schedule-envelope.json`, permitting
only `schema,run_id,dispatch_id,slot_claim_sha256,reservation_sha256,
dispatcher_runtime_sha256,worker_started_sha256,worker_runtime_sha256,
shadow_pointer_sha256`; every member must descriptor-replay to that run and
dispatch and the envelope SHA fills `schedule_envelope_sha256`, while guard SHA
is null. Manual guard bytes are computed outside collection only in memory;
manual and scheduled authorization files are installed only while the canonical
collection lock is held, immediately after all referenced evidence/deadlines
are replayed. Every candidate-ledger reader also holds that lock, so no live
reader observes the write interval; a crash leaves the explicit pending state
above rather than an illegal candidate-absent directory. The at-most-4-KiB
authorization member plus candidate.json are the operational closure covered
by the manual non-grid or scheduled receipt reserve; the B-planned
candidate-commit and every referenced input/bundle/admission member move into
main inventory. Retention protects both categories as one logical candidate
closure. The candidate event is invalid if the required
authorization member is absent, both authorization members exist, an
authorization SHA is null/mismatched, an envelope is transplanted, installed
lacks its exact commit, or not-installed has any commit. This closure is
written even for a closed not-installed
outcome, so recovery never infers mode from a mutable caller.

The command has no timer unit. It descriptor-safely loads the installed
candidate and requires it to equal a valid full shadow joint pointer: cohort,
core pointer, core manifest, source generation, universe/baseline/audit
lineage, full phase-state SHA, and canonical scope SHA must reproduce. A
core-only orphan or a candidate from a different shadow pointer is forbidden.
It also loads that run's exact `candidate.json` and requires
`status=installed`, the same run/cohort/shadow-pointer/candidate-manifest/
candidate-input-manifest/storage-admission identities, the exact replayed
candidate-commit SHA, and a complete linked
worker terminal/service. A bundle directory or manifest without this matching
event is an orphan candidate and is never promotion-ready; a not-installed or
missing event exposes its stable reason rather than being inferred from files.
The candidate's exact shadow-pointer SHA and cohort must also equal the passing
full gate artifact's `anchored_joint_pointer_sha256`, which is the last valid
scheduled pointer in that evaluated population. A manual/unlinked or merely
newer pointer can never supply candidate lineage. Run the full gate and all
existing topology/cryptographic/publication checks,
then revalidate candidate, gate evidence, phase state, and prior public pointer
under the final commit lock to close TOCTOU.

Extract the complete-bundle semantic validator into a shared public module
used by both promotion and `check_dashboard_release.py`; do not import checker
private functions. `check_dashboard_release.py` exposes the public
`check_promotion_candidate()` interface above and matching CLI flags for
candidate ID, prospective-pointer SHA, expected app/static SHAs, and optional
base URL; it derives the candidate path beneath `data_dir` and never accepts an
arbitrary path. Candidate mode validates local immutable route semantics and
prospective pointer bytes. With `base_url`, it also requires live
health/app/static identities to equal the explicit expected values; production
promotion requires a base URL, while isolated unit tests may inject the public
checker callable with `base_url=None`. Run the required release checker in
explicit immutable candidate mode and validate the exact prospective pointer bytes before any
public mutation. The checker must not need a tentative `routes/latest.json`.

Before pointer replacement, atomically install an immutable `prepared`
promotion record that binds prior/prospective pointer bytes and SHAs, candidate
manifest, phase/gate evidence, scope SHA, and the successful checker result.
Use exact directory `routes/promotions/<promotion_id>/` with
`stage-owner.json`, `identity.json`, `checker.json`, `primary-guard.json`,
optional exact `recovery-guard.json`, exact
`prospective-pointer.json`, optional exact
`prior-pointer.json`, `prepared.json`, optional `final-validation.json` plus
`commit.json`, and eventual `terminal.json`, all
canonical, descriptor-relative, no-replace, and fsynced. The pointer files are
the exact bytes observed and validated by the transaction;
`prior-pointer.json` is absent only when no prior public pointer existed, in
which case production rollback is unavailable. Their byte SHAs must equal the
prepared record. Compute `promotion_id` before refresh B as SHA-256 of exact
canonical `route_promotion_identity/v1` containing only
`schema,candidate_route_cohort_id,candidate_manifest_sha256,prior_pointer_sha256,prospective_pointer_sha256,phase_state_sha256,gate_evidence_sha256,scope_sha256,prior_publication_transition_id,checker_result_sha256,primary_schedule_guard_sha256,checked_at,commit_deadline,expected_app_sha,expected_static_asset_sha`.
It explicitly excludes `storage_admission_sha256`, prepared SHA, and later
artifacts, so the known journal directory/checker/primary-guard/pointer files can enter B
without an ID/fingerprint cycle. Install a staged/fsynced/no-replace journal
directory containing exact `identity.json` as the first durable event; the
directory name equals the SHA-256 of those exact bytes, so an empty visible
journal can never exist. Use only descriptor-verified private staging and a
durable lease. Generate a 32-hex CSPRNG nonce in memory before B; B's exact plan
reserves `.stage-leases/<promotion_id>.json`,
`.staging/<promotion_id>.<nonce>.stage`, its final directory, hidden atomic-file
temps, double maximum data footprint, and metadata (with equivalent rollback
roots). After B, atomically install exact
`route_publication_stage_lease/v1` containing only
`schema,journal_kind,journal_id,storage_admission_sha256,nonce,stage_name,final_name`;
its byte SHA is retained permanently. Only then create the planned mode-0700
stage O_EXCL, capture `(st_dev,st_ino)`, and install exact `stage-owner.json`
binding lease SHA, ID, device, and inode before identity. `stage-owner.json`
remains an exact permanent allowed first member after the directory is renamed;
both journal validators and retention bind/protect it rather than deleting it.
Its exact schema is `route_publication_stage_owner/v1` and permits only
`schema,journal_kind,journal_id,stage_lease_sha256,stage_name,stage_device,stage_inode`.
`journal_kind` is exactly `promotion|rollback`, `journal_id` and lease SHA are
lowercase 64-hex, `stage_name` is the lease's identical ASCII basename of at
most 160 bytes, and device/inode are canonical integers in
`1..(2**63-1)`. Canonical owner bytes are at most 2 KiB. The owner kind/ID must
match the containing final journal and its lease; a cross-kind owner or unknown
key is interference. Promotion and rollback use this one contract unchanged.
The lease's own hidden
temp naming/maximum is in B; a killed partial temp is recoverable only through
the same lease/B plan.

Write/fsync exact identity in stage, fsync stage/parent, then no-replace rename
to the final ID and fsync/reread. Before chain replay, enumerate leases first
and reconcile lease-only, stage-only, final-only, and both. After restart,
lease-only cannot resume by creating a new stage and follows the universal
owned-cleanup/abandon rule. Only the same live process that still owns the
pre-B in-memory identity and
just won that O_EXCL creation and still holds the opened stage descriptor may
install its owner record. A stage observed during recovery without an already
valid owner is interference even when empty; lease/path equality alone never
proves its inode belongs to the interrupted transaction. Stage ownership
otherwise requires the exact lease, planned name, and opened inode matching
the permanent owner record plus unique B.
A valid stage may resume rename; final-only plus lease is normal; byte-identical
both removes only the descriptor-proved owned stage; mismatch, stage without
lease, or wrong inode is interference. Hidden partial files may be cleaned only
when lease, B plan, name, inode, and allowlisted contents all prove ownership.
Retention counts/protects leases/staging and never follows links. Kill tests
cover lease temp/final, stage mkdir, owner record, identity write/fsync, parent
fsync, rename, and both-name races.
Checker schema
`route_promotion_checker/v1` permits only `schema`, `mode`, `subject_id`,
`pointer_sha256`, `status`, `app_sha`, `static_asset_sha`,
`route_root_binding_sha256`, `market_input_binding_sha256`,
`semantic_result_sha256`, and `checked_at`; mode is `candidate|rollback`, a
persisted result has status exactly `passed`, and canonical bytes are at most
4 KiB. It stores no URL/query, headers, response body, request timing,
exception text, or unknown fields. Recovery recomputes the bounded semantic
result and exact SHA from immutable evidence rather than trusting an opaque
success boolean. Freeze the hashed result domains as canonical UTF-8 JSON from
the shared canonical serializer:

- `route_promotion_candidate_semantic/v1` permits only
  `schema,subject_id,evaluated_at,pointer_sha256,manifest_sha256,scope_sha256,route_count,scenario_count,strict_complete_scenario_count,attested_scenario_count,max_route_age_seconds,status,reason_codes`;
- `route_promotion_rollback_projection/v1` permits only
  `schema,subject_id,evaluated_at,pointer_sha256,evidence_contract_version,route_count,scenario_count,available_scenario_count,policy_qualified_scenario_count,redacted_scenario_count,status,reason_codes`;
- `route_promotion_gate_recheck/v1` permits only
  `schema,evaluated_at,phase,population_sha256,gate_evidence_sha256,anchored_joint_pointer_sha256,storage_admission_sha256,status,reason_codes`;
- `route_promotion_storage_recheck/v1` permits only
  `schema,evaluated_at,storage_admission_sha256,inventory_fingerprint_sha256,actual_artifact_set_sha256,status,reason_codes`; and
- `route_primary_schedule_guard/v1` permits only
  `schema,operation,evaluated_at,boot_id,evaluated_monotonic_ns,
  deadline_monotonic_ns,evidence_mode,bootstrap_evidence_sha256,
  previous_depth_receipt_sha256,previous_daily_receipt_sha256,
  primary_receipt_set_sha256,
  current_trigger_window_status,next_primary_trigger_at,
  route_calendar_sha256,current_route_trigger_window_status,
  next_route_trigger_at,
  historical_primary_evidence_status,recovery_transaction_id,recovery_action,
  required_clearance_seconds,operation_deadline,status,reason_codes`; and
- `route_shadow_actual_artifact_set/v1` contains sorted exact
  `path,size,sha256` members for all post-B planned files present at the check.

Rollback projection counts are derived per validated scenario, never supplied
as aggregate claims. `scenario_count` is the complete target inventory and
`scenario_count = available_scenario_count + redacted_scenario_count`;
`0 <= policy_qualified_scenario_count <= available_scenario_count`. A protected
v1 target projected through v2 contributes `(available=0,qualified=0,
redacted=1)` for every scenario, so its aggregate row is exactly
`(0,0,scenario_count)` even if the frozen v1 endpoint historically called it
available. A v2 scenario that passes the oldest-leg freshness/shape rules and
has class `policy_qualified_candidate` contributes `(1,1,0)`; a fresh valid
`research_estimate` contributes `(1,0,0)`; and any v2 unavailable, invalid,
legacy-redacted, or stale scenario contributes `(0,0,1)`. Status is passed only
when every row and aggregate satisfies that matrix at the single persisted
`evaluated_at`; a 120-second boundary passes and the first later instant
redacts. Known-answer tests cover protected v1, fresh/stale v2, mixed class
inventories, zero scenarios, and one-count mutations in each field.

Freeze primary-guard literals and nullability. `operation` is exactly
`promotion|rollback|enable|disable|enable_recovery_rollback|disable_recovery_finalize|
promotion_recovery_rollback|rollback_recovery_rollback|
phase_transition|candidate_install`;
`evidence_mode` is exactly
`receipts|bootstrap|disable_safety|recovery_bootstrap|recovery_safety`;
`historical_primary_evidence_status` is exactly
`valid|missing|invalid|overflow|not_applicable`; `recovery_action` is exactly
`rollback_only|finalize_false` or JSON null. `current_trigger_window_status` is exactly
`clear|active|unknown`; `current_route_trigger_window_status` is exactly
`clear|active|unknown|not_applicable`; and status is `passed|failed`. Receipt mode requires
nonnull lowercase 64-hex previous-depth, previous-daily, and receipt-set SHAs,
null bootstrap SHA, historical status `valid`, and both recovery fields null.
Bootstrap mode is legal only for operation `enable`,
requires a nonnull 64-hex bootstrap SHA, and requires all three receipt SHAs
null, historical status `not_applicable`, and both recovery fields null.
`disable_safety` is legal only for operation `disable` after every Shadow unit
is stopped and primary-intent is held. It binds whatever prior receipt SHAs are
descriptor-valid, records the recomputed historical status (including missing,
invalid, or overflow), requires null bootstrap/recovery fields, and may pass
despite bad historical quality because it can only narrow authority to false.
For `disable_safety|recovery_safety`, each preceding receipt SHA is nonnull iff
that expected path contains one descriptor-safe regular byte sequence (valid or
semantically invalid); it is null iff the path is missing or descriptor-unsafe.
`primary_receipt_set_sha256` is always nonnull and hashes the exact typed set,
including missing/unsafe members and any overflow marker, so null never means
ignored evidence. Historical status is `valid` only when both receipts replay;
`overflow`, then `invalid`, then `missing` are the ordered nonvalid precedence
from the typed set. Tests freeze all two-receipt presence combinations plus
marker and unsafe-path variants.
`recovery_bootstrap` is legal only for operation `enable_recovery_rollback`,
requires exact nonnull bootstrap and pending desired-true transaction SHAs,
`recovery_action=rollback_only`, null receipt SHAs, and historical status
`not_applicable`; it can remove only that pending transaction's owned bytes,
restore false/base state, and write aborted/interference evidence, never resume
forward or create a new true identity.
`recovery_safety` is legal only for
`promotion_recovery_rollback|rollback_recovery_rollback|
disable_recovery_finalize`. It requires a nonnull matching
`recovery_transaction_id`; publication recovery requires
`recovery_action=rollback_only`, while disable recovery requires
`recovery_action=finalize_false`,
null bootstrap SHA, and the typed historical-receipt matrix above; historical
failure cannot block this narrowly owned safety rollback. It may pass only
with primary-intent plus the operation's required locks held. Publication
recovery requires clear primary and route windows and both next triggers beyond
its 31-second/30-second deadline; disable recovery requires clear primary
window, all Shadow units stopped, and its 60-second safety deadline but ignores
the route calendar. Publication recovery can write only the matching optional recovery
guard and aborted/interference terminal and cannot commit public state. Disable
recovery can only stop/disable units, remove exact owned bytes, publish false,
and finish the existing desired-false transaction as committed or quarantined;
neither mode can resume forward enable or replace the original guard.
`recovery_action_mismatch` is deterministically present whenever publication
recovery does not request `rollback_only` or disable recovery does not request
`finalize_false`; no rollback-specific stale reason is accepted.
For promotion, rollback, enable, promotion/rollback recovery safety, phase
transition, and candidate install,
`route_calendar_sha256` is the physical SHA of the exact tracked
`cex-dex-route-shadow.timer` bytes, route window must be `clear`, and the next
literal `:09/:24/:39/:54` trigger must be known and strictly after the same
operation deadline. Disable, disable-recovery finalize, and enable-recovery
rollback are the safety exceptions: their route status is
`not_applicable` and both route-calendar/next-route fields are JSON null, so a
scheduled Shadow boundary cannot prevent stopping it. Passed requires the
primary window `clear`, known canonical next-primary-trigger time, the required
route-window matrix, empty reasons, one of the permitted evidence matrices,
and strict clearance;
failed may use `active|unknown`, permits null next-trigger only with the exact
unknown reason, and has one or more sorted unique reasons from only
`active_trigger_window,previous_depth_missing,previous_daily_missing,
previous_depth_invalid,previous_daily_invalid,receipt_overflow,
receipt_set_invalid,bootstrap_not_permitted,bootstrap_missing,
bootstrap_invalid,next_trigger_missing,insufficient_clearance,boot_mismatch,
clock_mismatch,evidence_drift,evidence_unsafe,route_calendar_invalid,
active_route_trigger_window,route_trigger_unknown,
next_route_trigger_missing,route_trigger_insufficient_clearance,
recovery_transaction_mismatch,recovery_action_mismatch,
shadow_units_not_stopped`. Historical receipt failures are serialized in
`historical_primary_evidence_status` but are not failed-guard reasons in
`disable_safety|recovery_bootstrap|recovery_safety`. The validator recomputes the
complete reason set; extra, missing, generic, or contradictory reasons fail.
Promotion/rollback guard `evaluated_at` equals that transaction's exact
`checked_at`; enable/disable equals identity `requested_at`; a manual
phase/candidate wrapper uses its one trusted operation-start sample; bootstrap evidence
`evaluated_at` equals its guard. Those are the same single trusted UTC sample,
not a second helper read. Desired true uses operation `enable`, desired false
uses `disable`. Tests transplant every operation/mode, shift the UTC sample by
one microsecond, and alter nulls/reasons. Enable fixtures start exactly 60
seconds before, at, and just after a route-grid instant and prove it cannot arm
the non-persistent timer unless its committed terminal is guaranteed before
the next trigger; no expected slot is exposed while true authority is pending.

Counts are bounded nonnegative integers, age is canonical finite decimal or
JSON null, status is `passed|failed`, and reason codes are sorted unique closed
values. `route_count` counts unique routes; every other count is over intended
route-notional scenario members, and candidate pass requires both strict and
attested scenario counts to equal `scenario_count`. Primary guard canonical
bytes are at most 4 KiB. Checker
`semantic_result_sha256` hashes the candidate-semantic or
rollback-projection bytes at exact `checked_at`; final `route_result_sha256`,
`gate_result_sha256`, `storage_result_sha256`, and rollback
`target_projection_sha256` hash the corresponding exact bytes at
`validated_at=commit_at`. The final gate recheck is deterministic and
write-free: it never persists a new gate artifact after B, so it cannot create
an unplanned self-invalidating inventory delta. Recovery recomputes every hash
from immutable evidence and the recorded time.

Prepared schema `route_promotion_prepared/v1` permits only
`schema`, `promotion_id`, `candidate_route_cohort_id`,
`candidate_manifest_sha256`, `prior_pointer_sha256`,
`prospective_pointer_sha256`, `phase_state_sha256`,
`gate_evidence_sha256`, `storage_admission_sha256`, `scope_sha256`,
`prior_publication_transition_id`, `checker_result_sha256`,
`primary_schedule_guard_sha256`, `checked_at`, `expected_app_sha`, and
`expected_static_asset_sha`, plus canonical `commit_deadline`; null prior SHA
means the public pointer was absent. Every SHA binds exact installed bytes, not a reserialized logical
object. `primary-guard.json` contains the exact canonical guard bytes sampled
for this identity; its physical SHA must equal the identity, prepared, and
final-validation fields. After final time-sensitive revalidation, install exact
`final-validation.json` schema `route_promotion_final_validation/v1` with only
`schema`, `promotion_id`, `validated_at`, `prospective_pointer_sha256`,
`candidate_manifest_sha256`, `observed_app_sha`,
`health_static_asset_sha`, `served_static_asset_sha`,
`observed_route_root_binding_sha256`, `observed_market_input_binding_sha256`,
`route_result_sha256`, `gate_result_sha256`, `storage_result_sha256`,
`primary_schedule_guard_sha256`, `storage_admission_sha256`,
`validated_boot_id`, `validated_monotonic_ns`, and `status`; status is exactly `passed`, its bytes
are at most 4 KiB, and all result hashes are reproducible from immutable
evidence at `validated_at`. The final check boundedly re-queries live `/health`
and fetches the actual served static asset representation/digest using the
release checker's redirect, encoding, and size rules. App SHA, health-declared
static SHA, and served static SHA must all equal the explicit expected values,
so a deploy or stale CDN/proxy between checker and commit fails. Install exact `commit.json` schema
`route_promotion_commit/v1` with only `schema`, `promotion_id`,
`prepared_sha256`, `checked_at`, `commit_at`, `commit_deadline`,
`commit_boot_id`, `commit_monotonic_ns`, and
`final_validation_sha256`; the final-validation SHA binds those installed
bytes. Terminal schema
`route_promotion_terminal/v1` permits only `schema`, `promotion_id`, `outcome`,
`commit_record_sha256`, `finished_at`, `observed_pointer_sha256`,
`post_replace_at`, `post_replace_boot_id`, `post_replace_monotonic_ns`,
`post_replace_route_result_sha256`, `recovery_guard_sha256`, and `reason_code`, with outcome exactly
`committed|aborted|interference`.
`commit_record_sha256` equals the exact installed commit record whenever one
exists, including a reconciled aborted attempt, and is JSON null only when no
commit was attempted.
Final-validation `validated_at` equals commit `commit_at`, its boot/monotonic
fields equal the commit fields, and that monotonic instant is on the guard's
boot at or before both guard and UTC commit deadlines. A committed terminal
requires all four post-replace fields nonnull: same boot, canonical wall time,
`commit_monotonic_ns <= post_replace_monotonic_ns <=
commit_monotonic_ns + 1_000_000_000`, wall/monotonic elapsed agreement within
100 milliseconds, post time within the guard deadline, exact prospective
pointer SHA, and the deterministic candidate-semantic result recomputed at
post time with route age at most 120 seconds. Its result SHA hashes those exact
canonical bytes. Aborted/interference terminals require all post fields JSON
null unless the still-running owner actually completed and recorded the post
sample; recovery never fabricates one. Missing, mixed-boot, out-of-order, or
transplanted monotonic evidence invalidates the chain.
`recovery_guard_sha256` is JSON null for every terminal written by the original
owner, including a normal pre-pointer abort. A later reconciler may write only
`aborted|interference`; then it requires the nonnull physical SHA of this
journal's exact `recovery-guard.json`. A recovered committed outcome is never
invented: an already durable committed terminal is merely replayed. The initial
B artifact plan reserves the optional 4-KiB recovery guard and terminal field,
so crash recovery consumes only the original outstanding reservation.

Treat every committed promotion or rollback as one immutable public-state
transition. Its transition ID is its `promotion_id` or `rollback_id`; every
prepared record binds the exact `prior_publication_transition_id`. For a
pre-journal public pointer, derive the 64-hex genesis ID as SHA-256 of literal
ASCII `route-publication-genesis/v1\n<pointer_sha256>\n` (use literal
`absent` in place of the SHA when no pointer exists). Before preparing any
state change, descriptor-enumerate and replay all canonical prepared/commit/
terminal records from genesis, require one unbranched committed chain, and
require its unique head pointer SHA to equal `routes/latest.json`. Duplicate
committed children from one head, missing parents, cycles, conflicting
committed heads, or a pointer with no unique matching active-head transition
are interference. Only `committed` advances the head; any number of canonical
`aborted` pre-commit retries may share the same parent and do not form a branch,
while any interference terminal globally blocks. At most one unterminated
record may exist and it is reconciled before a new prepare. A legitimate
rollback head may intentionally produce pointer bytes equal to an older node;
the new rollback transition, not byte inequality, is the unique active head.
Tests cover two consecutive aborted siblings followed by one success and the
ABA rollback sequence below.

Idempotence is recognized before ordinary head admission. If the active head
is the unique committed promotion for the same candidate and its prospective
pointer equals current bytes, return that immutable terminal without creating
a no-op child. If the active head is the unique committed rollback child of
the caller's source transition and current bytes equal that child's target,
return that rollback terminal even though the original source is no longer
head. In every other case the named prior/source must equal the active head.
Reject `prior_pointer_sha256 == prospective_pointer_sha256` for a new
transition. Aborted siblings never satisfy idempotence or advance the head.

Only pure preflight/evaluator functions accept caller-supplied historical
times. State-changing promotion and rollback wrappers obtain `checked_at` and
`commit_at` from their injected trusted UTC clock after the relevant locks are
held, pass that one exact `checked_at` explicitly to the pure checker and gate,
and persist its deterministic result; no nested helper reads a second clock.
Production CLIs expose no evaluated-at/backdate flag. Tests inject the clock
and prove an old passing gate cannot be replayed after a current missing slot
or stale route.

The final state-changing section uses the shared primary-intent -> collection
-> routes order. Under those locks, deterministically evaluate the exact
`route_primary_schedule_guard/v1`: the current daily/depth trigger window must
be clear, the immediately preceding expected receipts must be valid, and the
next earliest primary trigger must be strictly after the entire local operation
deadline. Promotion and rollback use `required_clearance_seconds=31` for the
30-second checker/commit deadline; first enable uses the stricter 60-second
mutation-plus-owned-rollback budget below. Bind the exact current primary
receipt/overflow set SHA. Recompute the guard immediately before pointer or
authority replacement and require the same set/next trigger. A primary that
acquired intent first completes and changes the set before re-evaluation; one
arriving later waits without losing its collection slot because the state
changer has already proved it will release intent before that trigger. A
missing/late/invalid receipt, active AccuracySec window, set drift, or
insufficient clearance aborts with no public pointer mutation. Barrier tests
start primary after final gate/storage validation and require either
primary-first completion plus a fresh failed guard or state-change completion
before the future trigger; it can never publish and then discover a contention
receipt.

The guard samples one trusted wall instant, one current-boot monotonic instant,
and the exact lowercase UUID boot identity while primary-intent is held.
`deadline_monotonic_ns = evaluated_monotonic_ns +
required_clearance_seconds * 1_000_000_000`; `operation_deadline` is the exact
matching wall instant, and wall/monotonic deltas must agree within 100
milliseconds. A forward resume after process death is legal only on the same
boot, before that exact monotonic deadline, with an unchanged receipt/bootstrap
set and next-trigger projection. Deadline expiry, reboot, clock-step mismatch,
or any primary-trigger drift forbids continuing the old transaction. Recovery
may acquire a fresh guard only to perform descriptor-owned rollback and write
an aborted/interference terminal; it cannot replace the identity's original
guard or resume its forward mutation. The fresh guard is persisted as the
publication transaction's separately hashed `recovery-guard.json` before the
first rollback write; its terminal binds that physical SHA. Promotion/rollback
recovery uses only the transaction-bound `recovery_safety` matrix above and
never bootstrap. Enable recovery has its separately specified
`recovery_bootstrap` side file and matrix below. If no fresh safe rollback guard is
available, route units stay stopped and the transaction remains visibly
pending/interference rather than guessing. Tests kill across the deadline,
advance the primary trigger, change boot ID, and change wall versus monotonic
time. Promotion and rollback persist these exact guard bytes as their planned
`primary-guard.json`; recovery never reconstructs a monotonic sample from a
SHA or wall timestamp. Missing, mutated, or cross-transaction guard bytes are
interference. Kill-after-final-validation, kill-after-commit, and
kill-before-pointer tests permit forward recovery only on the same live guard;
kill-after-pointer without a committed post-proof terminal performs owned
rollback. A previously durable committed terminal replays its historical
same-boot event even when the later recovery process is on another boot.

Bootstrap is permitted only when receipt evidence cannot exist by construction:
the pristine first enable, or a re-enable from a descriptor-replayed committed
false authority whose unique parent is a valid committed-true disable chain.
That re-enable additionally requires no pending/interference transaction, no
active Shadow unit, the restored base primary-unit projection, and current
canonical false authority. Its guard uses
`evidence_mode=bootstrap`, null previous-receipt/set SHAs, and nonnull exact
`bootstrap_evidence_sha256`. Schema `route_primary_bootstrap/v1` permits only
`schema,evaluated_at,daily_timer_projection,
daily_timer_projection_sha256,depth_timer_projection,
depth_timer_projection_sha256,daily_service_result,
daily_service_result_sha256,depth_service_result,
depth_service_result_sha256,daily_collection_manifest_projection,
daily_collection_manifest_projection_sha256,
depth_collection_manifest_projection,
depth_collection_manifest_projection_sha256,status,reason_codes`, at most 16 KiB.
Status is exactly `passed|failed`. Passed requires every projection/SHA pair
nonnull, all nested validators/correlations passing, and `reason_codes=[]`.
Failed requires every pair to be either both null or both canonical/nonnull and
the complete sorted unique reason set from only
`daily_timer_missing|daily_timer_invalid|depth_timer_missing|
depth_timer_invalid|daily_service_missing|daily_service_failed|
depth_service_missing|depth_service_failed|daily_manifest_missing|
daily_manifest_invalid|depth_manifest_missing|depth_manifest_invalid|
time_correlation_mismatch|boot_mismatch|data_root_mismatch|evidence_unsafe`.
A null object with nonnull SHA, a partial/invalid retained object, missing or
extra reason, or failed object cannot support a passed guard. Under primary-intent,
descriptor-load the latest canonical successful daily/depth collection
manifests and the fixed live timer/service last-trigger/start/finish/result
properties. Timer last trigger must equal the preceding expected slot; service
must have exited successfully; manifest profile/status and ordered timestamps
must match that exact service window and current canonical data root. Missing,
failed, stale, overlapping, ambiguous, or path-unsafe evidence blocks rather
than passing as no receipt. While authority is committed enabled, ordinary
widening operations use only receipts and can never downgrade to bootstrap.
Disable instead uses `disable_safety`, so unhealthy historical receipts cannot
prevent stopping units, restoring the base primary projection, and publishing
false. A descriptor-proven pending desired-true first/re-enable transaction,
with no committed true head and all Shadow units stopped, may use one fresh
`recovery_bootstrap` guard solely to owned-rollback that same transaction after
its original boot/deadline expires; this exception can neither resume true nor
admit another identity. Any other invalid/pending/interference state cannot
receipt-to-bootstrap downgrade. A committed
false re-enable may use only the fresh bootstrap proof above, never stale
pre-disable receipts. Tests cover pristine feature-off successful bootstrap,
disable followed by several primary slots then successful re-enable,
missing/failed last primary, forged time correlation, pending-first-enable
rollback after deadline/reboot, and attempted enabled/unrelated-invalid receipt-
to-bootstrap downgrades.

Freeze the nested historical observations rather than retaining hash-only
shells. Each timer object is exact `route_primary_bootstrap_timer/v1` with only
`schema,profile,unit_name,fragment_path,unit,calendar,accuracy_usec,
randomized_delay_usec,persistent,last_trigger_at,last_trigger_monotonic_ns,
boot_id`; each service object is exact
`route_primary_bootstrap_service/v1` with only
`schema,profile,unit_name,invocation_id,exec_started_at,
exec_started_monotonic_ns,exec_finished_at,exec_finished_monotonic_ns,
service_result,exit_code,exit_status,normalized_outcome,
collection_manifest_sha256,boot_id`. Profile is exactly `daily|depth`, every
unit/path/calendar/property must equal the fixed tracked primary unit, boot IDs
must equal the enclosing guard, and each nested physical canonical SHA must
equal its redundant top-level field. The bounded probe uses only fixed
`systemctl show` properties: timer `Id,FragmentPath,Unit,TimersCalendar,
AccuracyUSec,RandomizedDelayUSec,Persistent,LastTriggerUSec,
LastTriggerUSecMonotonic`; service `Id,InvocationID,ExecMainStartTimestamp,
ExecMainStartTimestampMonotonic,ExecMainExitTimestamp,
ExecMainExitTimestampMonotonic,Result,ExecMainCode,ExecMainStatus`. It applies
the shared strict C-locale duration/time/result parsers, rejects missing,
duplicate, extra, localized, zero-monotonic, cross-boot, or unordered values,
and never stores command output. Recovery semantically replays these persisted
objects plus the corresponding embedded exact
`route_primary_collection_manifest_projection/v1` objects even if live
LastTrigger advances or source run manifests are later overwritten/pruned.
Each projection's physical SHA equals its top-level field and its nested source
manifest SHA equals the corresponding service object's
`collection_manifest_sha256`; profile/status/time/invocation relationships are
recomputed. Current live state is consulted separately only for guard drift
and may block forward resume. A forged passed/bootstrap object containing only
matching hashes is structurally invalid. Tests overwrite and prune source
manifest paths and transplant a same-SHA/wrong-profile projection while
persisted semantic replay remains deterministic and strict.

Revalidate all prepared inputs while holding the final routes lock, then make
the already-validated pointer replacement the last state-changing step. Normal
commit first completes the bounded final live `/health` and served-static fetch
under the lock and validates their expected identities; only after the last
network byte does it obtain one fresh trusted UTC `commit_at` plus a monotonic
boundary sample. A slow live check that crosses the checker deadline therefore
cannot preserve an earlier timestamp. Require
`checked_at <= commit_at <= commit_deadline - 1s`, where the deadline equals
`checked_at + 30s`, then rerun every time-sensitive route-age, fee, gate, and
admission predicate deterministically with no further network/source I/O.
Executable route age must be at most 119 seconds at this sample, reserving the
one-second local commit budget against the public 120-second ceiling. A route
that was fresh when the checker/live fetch began but crossed either boundary is
rejected.
`final-validation.json.validated_at` must equal that exact `commit_at` clock
sample byte-for-byte; a one-second mismatch or a validation from before a
freshness boundary cannot be paired with the commit record.
Normal post-replace fsync/reread failure performs owned rollback without overwriting a
concurrent third-party pointer. A process kill can still interrupt between the
atomic replace and terminal logging, so every promotion/release invocation
first reconciles unterminated prepared records. An exact prospective pointer
with commit/final-validation but no committed terminal lacks durable
post-replace boundary proof: recovery CAS-restores the exact prior bytes and
terminalizes `aborted`; if exact ownership/CAS cannot be proved it records
interference. A prospective pointer without exact commit/final-validation is
immediate interference. While that unterminated prospective state is visible,
the dual reader marks all opportunity scenarios unavailable with reason
`promotion_recovery_pending` and redacts actionable economics. An exact prior
pointer with no `commit.json` terminalizes aborted. An unterminated record with
a valid commit plus exact prior is conservatively interference because recovery
cannot distinguish kill-before-replace from third-party ABA restoration; only
the still-running owner may record its own completed rollback before exiting.
Any other state is interference and blocks further promotion. Checker-level immutable predicates
may be replayed at `checked_at` but never substitute for final-boundary evidence
at `commit_at`. Never expose a pointer that has not already passed the checker.
Install and fsync `final-validation.json` plus `commit.json` after fresh
revalidation but before the public pointer, making the pointer the final public
mutation. Canonical transaction bytes are prebuilt before the clock sample;
only their bounded no-replace writes, fsyncs, and the atomic pointer replace may
follow. After exact pointer reread, sample the same monotonic/UTC clocks and
require elapsed time at most one second, UTC no later than `commit_deadline`,
and route age no greater than 120 seconds; otherwise perform owned rollback and
terminalize aborted. Persist that exact post sample/result in the immutable
terminal only after exact owned reread; require `finished_at >= post_replace_at
>= commit_at` for committed outcomes. Only a fully validated committed
terminal advances the publication chain. Historical replay of such a terminal
may occur after deadline expiry or on a later boot because it verifies the
recorded same-boot commit event; it does not demand that the old guard still be
currently live. In contrast, an unterminated pre-pointer transaction may
resume forward only under the current guard rule above, and an unterminated
post-pointer transaction is always rolled back rather than guessed committed.
Tests stall after commit before replace, kill immediately after replace, kill
after durable committed terminal, and replay the last case on a new boot.
Acquire locks in the fixed order primary-intent then collection then routes for
the final prepare/commit/reconciliation state-changing section; preflight may
remain read-only outside them. Recovery uses recorded `commit_at`, not recovery wall
clock, so a genuine kill-after-commit cannot become interference merely because
time passed. A first/normal commit always uses its fresh trusted clock. Tests
cross checker-result, 120-second route-age, and deployment-SHA-change
boundaries; every failure leaves the public pointer unchanged. The release
checker reports pending records but is read-only; only the
promotion command reconciles them under locks.
Every promotion and rollback begins by enumerating both journal roots. Any
canonical terminal with outcome `interference` remains a global hard block on
later state changes; scanning only unterminated records is forbidden. Records
are immutable and this task intentionally provides no automatic delete or
resolution mechanism, so the incident must be investigated before a later
explicitly designed recovery contract can clear it. A regression test writes
one interference terminal and proves a second otherwise-valid promotion still
fails.

Reconciliation enumerates every visible journal directory from its
`identity.json` before replaying the committed chain; it never starts only at
`prepared.json`. A missing/noncanonical identity, directory-name/SHA mismatch,
unknown member, or two simultaneous nonterminal identities is an immutable
hard interference condition and is never deleted. For one valid identity with
no prepared record, descriptor-search immutable admissions and require exactly
one valid B whose operation, subject ID, plan SHA/paths/maxima, decision, time,
and storage fingerprint authorized that identity directory plus every possible
side/terminal file; missing, duplicate, or wrong-plan B is interference. Only
the ordered subset `stage-owner`, identity, checker, primary guard, prior pointer,
prospective pointer may exist and every present byte must reproduce the
identity. If current public bytes still equal the identity's prior SHA, write a
terminal `aborted` within B's reserved terminal path/max bytes with null commit SHA and
`reason_code=preparation_interrupted`, whether the crash occurred just after
directory installation, any side file, or storage B. A prospective pointer,
commit/final-validation without prepared, mismatched/partial installed file,
or any other current pointer writes/retains interference. Once prepared exists,
use the commit-record matrix below. Promotion and rollback terminal schemas
therefore validly bind identity via their ID even when prepared is absent.
Task 5 protects every nonterminal identity and its partial dependency closure.
Kill tests cover the directory rename, each side file, B, prepared,
final-validation, commit, pointer replace, and terminal boundaries for both
journal types, including forged identity with no B and wrong-plan B.

Successful emergency rollback is a separate, deliberately narrower state
transition; it is not a second promotion path. Add
`rollback_route_promotion()` and release-checker rollback mode. The caller may
name only an immutable `source_transition_id`, never a bundle path, candidate,
or arbitrary pointer. That source must be the unique replayed active chain
head, have a canonical committed terminal, and current `routes/latest.json`
must still equal its exact `prospective-pointer.json`; the only legal target is
its exact `prior-pointer.json`. The checker uses the deployed dual reader to validate
that target's immutable v1 or v2 bundle plus explicit app/static identities,
so rollback cannot synthesize legacy bytes or widen to an unrelated v1.
The ID must full-match lowercase ASCII `[0-9a-f]{64}` and is resolved
descriptor-relatively to exactly one canonical directory in `promotions/` or
`promotion-rollbacks/`; the directory and every ancestor must be real opened
directories, never symlinks or swapped identities. Inside it, stage-owner,
identity, prepared, optional commit/final-validation, terminal, and pointer
members are each regular, `nlink=1`, exact-schema files with stable descriptor
identity. Separators, dots, NUL/control/Unicode, symlink/hardlink members or
ancestors, and the same ID directory appearing in both roots are interference
before any mutation.
Rollback does not re-certify an old target as fresh and therefore does not
apply normal-promotion freshness as an admission requirement. Instead, at the
trusted rollback evaluation time it runs the shared public projection and
requires every route older than 120 seconds to become
`availability=unavailable` with price-gap economics, net spread, capacity, and
all actionable amounts redacted to JSON null. Structure, hashes, source
promotion chain, and deployed reader compatibility remain mandatory. A
15-minute-old exact target may therefore be restored for incident recovery but
must expose zero actionable or policy-qualified opportunities; feeding that same target to normal
promotion still fails freshness.

Persist canonical rollback evidence under
`routes/promotion-rollbacks/<rollback_id>/` as `stage-owner.json`, `identity.json`, `checker.json`,
exact `primary-guard.json`, optional exact `recovery-guard.json`, exact
`prior-pointer.json` (the current head bytes), exact
`prospective-pointer.json` (the target bytes), `prepared.json`, optional
`final-validation.json` plus `commit.json`, and `terminal.json`. Prepared schema
`route_promotion_rollback_prepared/v1` permits only `schema`, `rollback_id`,
`source_transition_id`, `source_transition_prepared_sha256`,
`source_transition_terminal_sha256`, `prior_publication_transition_id`,
`current_pointer_sha256`,
`target_pointer_sha256`, `checker_result_sha256`,
`storage_admission_sha256`, `primary_schedule_guard_sha256`, `checked_at`,
`expected_app_sha`, `expected_static_asset_sha`, and `commit_deadline`.
After final revalidation, install exact `final-validation.json` schema
`route_promotion_rollback_final_validation/v1` with only `schema`,
`rollback_id`, `validated_at`, `target_pointer_sha256`,
`target_projection_sha256`, `observed_app_sha`,
`health_static_asset_sha`, `served_static_asset_sha`,
`observed_route_root_binding_sha256`, `observed_market_input_binding_sha256`,
`storage_admission_sha256`, `storage_result_sha256`,
`primary_schedule_guard_sha256`, `validated_boot_id`,
`validated_monotonic_ns`, and `status`. Status is
exactly `passed`; the target projection hash binds all freshness/legacy
redaction at `validated_at`; bounded live `/health` plus actual served-asset
digest identities equal expected values; and the
held-lock storage fingerprint still matches the prepared admission plus only
predeclared transaction files. `primary-guard.json` contains the exact
canonical guard sampled for this rollback and its physical SHA must equal the
identity, prepared, and final-validation fields. Then install `commit.json` schema
`route_promotion_rollback_commit/v1` with only `schema`, `rollback_id`,
`prepared_sha256`, `checked_at`, `commit_at`, `commit_deadline`,
`commit_boot_id`, `commit_monotonic_ns`, and
`final_validation_sha256`. Require
`checked_at <= commit_at <= commit_deadline=checked_at+30s`. Terminal schema
`route_promotion_rollback_terminal/v1` permits only `schema`, `rollback_id`,
`outcome`, `commit_record_sha256`, `finished_at`,
`observed_pointer_sha256`, `post_replace_at`, `post_replace_boot_id`,
`post_replace_monotonic_ns`, `post_replace_target_projection_sha256`,
`recovery_guard_sha256`, and
`reason_code`, with the same
`committed|aborted|interference` outcomes and exact installed commit SHA or
JSON null. Compute rollback ID before B as SHA-256 of exact canonical
`route_promotion_rollback_identity/v1` containing only
`schema,source_transition_id,source_transition_prepared_sha256,source_transition_terminal_sha256,prior_publication_transition_id,current_pointer_sha256,target_pointer_sha256,checker_result_sha256,primary_schedule_guard_sha256,checked_at,commit_deadline,expected_app_sha,expected_static_asset_sha`.
It likewise excludes storage admission and later artifacts. Tests prove both
IDs are fixed before refresh B and a changed B changes prepared SHA without
renaming or cross-wiring the journal directory.
Rollback final-validation and commit boot/monotonic samples must be identical,
on the persisted guard boot, and before its deadlines. A committed rollback
terminal requires every post field nonnull, the exact target pointer, same-boot
elapsed time in `[0,1_000_000_000]` nanoseconds with wall agreement within 100
milliseconds, and a deterministic rollback projection SHA at that post time
proving every stale/actionable scenario remains redacted. Other outcomes use
the same null/non-fabrication matrix as promotion. An unterminated exact target
pointer with commit/final-validation is CAS-restored to the source-head pointer
and terminalized aborted; CAS/ownership failure is interference. The dual
reader exposes `rollback_recovery_pending` with all actionable economics null
until closure. Only a durable committed terminal advances the chain, and its
historical recorded event can be replayed after reboot/deadline expiry without
pretending the old guard is currently live.
Rollback uses the promotion terminal's exact recovery-guard matrix: original-
owner terminals carry JSON null; a reconciler-written aborted/interference
terminal requires this rollback journal's nonnull 4-KiB recovery-guard SHA;
recovery can never synthesize committed. Its initial B plan reserves that
optional member and retention protects it with the journal closure.
Schema is part of each hashed identity domain; cross-type byte reuse/collision
between promotion and rollback identities is rejected.
Install rollback `identity.json` through the same staged directory no-replace
protocol before any side file; its exact byte SHA equals the directory ID.
Rollback `final-validation.validated_at` must equal the one exact trusted
`commit_at` sample; a one-second mismatch fails before pointer replacement.

Acquire primary-intent then collection then routes locks, reconcile prior rollback records, rerun
the dual-reader checker and exact active-head bindings, compute all
identity/checker/primary-guard/pointer and later transaction bytes/plan in memory, then make
Task 5 held-lock storage refresh B the first filesystem mutation. B reserves
exact maxima for identity/checker/primary-guard/optional-recovery-guard/pointers/prepared/final-validation/commit/
public-pointer/terminal plus directory metadata. Install the planned journal
and prepared binding B, then perform fresh target-projection, live
app/static, and storage-delta validation at `commit_at`, and perform a CAS
owned atomic replacement only from the source transition's prospective SHA to its prior
SHA. Fsync/reread failure uses the same third-party-safe rollback machinery;
retries are idempotent and a concurrent pointer change is interference, never
overwritten. Rollback reconciliation uses the same commit-record matrix:
prospective plus valid commit/final validation but no committed terminal is
owned-restored and aborted; prospective plus a valid committed terminal and
post proof is the historical committed head; prior with no commit is aborted;
prior with an unterminated valid commit is interference.
Tests cover v2-to-protected-v1 rollback, v2-to-v2 rollback,
missing/absent prior bytes, arbitrary v1 target rejection, repeated rollback,
concurrent pointer replacement, and crash points. An ABA sequence
`A -> B(P1) -> C(P2) -> B(P3/rollback)` cannot name stale P1 to jump to A;
only the unique P3 head can be rolled back, which would restore C. Task 5 protects every bundle,
pointer byte file, promotion record, and admission record reachable from an
unreconciled or committed promotion/rollback chain. This interface restores a
previously committed public state; it never makes a v1 candidate eligible for
new promotion.

- [ ] **Step 6: Verify GREEN, races, and release-checker rollback**

Run: `python3 -m unittest tests.test_route_shadow_gate tests.test_route_candidate_inputs tests.test_route_root_binding tests.test_route_cost_evidence tests.test_dex_route_costs tests.test_execution_cost_components tests.test_run_route_shadow tests.test_route_shadow_audit tests.test_route_opportunity tests.test_route_collection tests.test_release_smoke tests.test_route_publication tests.test_opportunity_api tests.test_freshness tests.test_opportunity_frontend tests.test_dashboard tests.test_framework -v`

Also run under a real CPython 3.8.10 runtime (not only `ast` feature-version
parsing): `python3.8 -m unittest tests.test_route_shadow_gate tests.test_route_candidate_inputs tests.test_route_root_binding tests.test_route_cost_evidence tests.test_dex_route_costs tests.test_execution_cost_components tests.test_run_route_shadow tests.test_route_shadow_audit tests.test_route_collection tests.test_route_publication tests.test_route_opportunity tests.test_release_smoke tests.test_opportunity_api tests.test_freshness tests.test_opportunity_frontend tests.test_dashboard -v`, followed by explicit imports of
`scripts.route_shadow_gate`, `scripts.promote_route_opportunities`,
`scripts.route_candidate_inputs`, `scripts.route_root_binding`,
`scripts.route_cost_evidence`, `scripts.dex_route_costs`,
`scripts.execution_cost_components`,
`scripts.run_route_shadow`, `scripts.route_shadow_audit`,
`scripts.collect_route_cohort`,
`scripts.route_publication`, `scripts.route_opportunity`,
`scripts.check_dashboard_release`, `dashboard.opportunity_facts`, and
`dashboard.server`. Missing CPython 3.8 is a verification blocker for this
commit, not a skipped check.

Expected: PASS, including exact threshold boundaries, pooled-p95
counterexamples, historical-pointer reconstruction, null/unknown ledger
blocking, phase/transition races, public-pointer sentinels, candidate lineage,
strict-evidence-versus-profit cases, promotion rollback, and Python 3.8
grammar. The promotion command rejects all current incomplete profiles without
altering `routes/latest.json`. Task 4 state-change success/race tests use only
the exact injected storage contract; separate production-default tests require
`not_evaluated` and an unchanged pointer while Task 5 is absent.

- [ ] **Step 7: Commit**

```bash
git add scripts/route_shadow_gate.py scripts/promote_route_opportunities.py scripts/route_candidate_inputs.py scripts/route_root_binding.py scripts/route_cost_evidence.py scripts/dex_route_costs.py scripts/execution_cost_components.py scripts/run_route_shadow.py scripts/route_shadow_audit.py scripts/route_publication.py scripts/collect_route_cohort.py scripts/route_opportunity.py scripts/check_dashboard_release.py dashboard/opportunity_facts.py dashboard/freshness.py dashboard/server.py dashboard/static/app.js tests/test_route_shadow_gate.py tests/test_route_candidate_inputs.py tests/test_route_root_binding.py tests/test_route_cost_evidence.py tests/test_dex_route_costs.py tests/test_execution_cost_components.py tests/test_run_route_shadow.py tests/test_route_shadow_audit.py tests/test_route_publication.py tests/test_route_opportunity.py tests/test_route_collection.py tests/test_release_smoke.py tests/test_opportunity_api.py tests/test_freshness.py tests/test_opportunity_frontend.py tests/test_dashboard.py
git commit -m "feat(routes): gate manual opportunity promotion"
```

Add a GitHub commit comment enumerating the passing/rejected fixtures, pooled
sample counts, and public-pointer rollback evidence.

### Task 5: Reference-safe retention and storage admission

**Files:**
- Create: `scripts/route_shadow_retention.py`
- Create: `tests/test_route_shadow_retention.py`
- Modify: `scripts/route_shadow_gate.py`
- Modify: `scripts/promote_route_opportunities.py`
- Modify: `scripts/route_candidate_inputs.py`
- Modify: `scripts/route_cost_evidence.py`
- Modify: `scripts/route_publication.py`
- Modify: `scripts/collect_route_cohort.py`
- Modify: `scripts/run_route_shadow.py`
- Modify: `scripts/route_shadow_inputs.py`
- Modify: `tests/test_route_shadow_gate.py`
- Modify: `tests/test_run_route_shadow.py`
- Modify: `tests/test_route_candidate_inputs.py`
- Modify: `tests/test_route_cost_evidence.py`
- Modify: `tests/test_route_collection.py`
- Modify: `tests/test_route_shadow_inputs.py`
- Modify: `tests/test_route_publication.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_opportunity_api.py`
- Modify: `tests/test_freshness.py`

**Interfaces:**
- Produces: `protected_route_evidence(data_dir: Path) -> set[Path]`.
- Produces: `apply_route_retention(data_dir: Path, *, operation: str, subject_id: str, artifact_plan: Mapping, high_water_bytes: int = 4 * 1024**3) -> dict`.
- Produces internal: `apply_route_retention_held_lock(data_dir: Path, *, collection_lock_fd: int, operation: str, subject_id: str, artifact_plan: Mapping, evaluated_at: datetime, high_water_bytes: int = 4 * 1024**3) -> dict`.
- Produces: `load_route_storage_admission_view(data_dir: Path, *, admission_sha256: str, expected_operation: str, expected_subject_id: str, expected_plan_sha256: str, evaluated_at: datetime) -> dict`.
- Produces internal: `_abandon_route_storage_reservation_held_lock(data_dir: Path, *, collection_lock_fd: int, admission_sha256: str, evaluated_at: datetime) -> dict`.
- Produces the fixed internal `RouteStorageAdmissionAdapter` implementing Task
  4's exact `StorageAdmissionContract`: `admit_held_lock()` delegates only to
  `apply_route_retention_held_lock()`, while `replay()` delegates only to
  `load_route_storage_admission_view()`. Both return the exact
  `route_storage_admission_view/v1`; no production caller consumes the looser
  raw helper result or constructs a view independently.
- The public mutating wrapper owns one trusted UTC sample after its locks and
  exposes no clock/evaluated-at/backdate option. Only the module-private
  identity-capability test harness may inject a clock; public signature/import
  scans and a future-dated admission counterexample prove production cannot do
  so. Held-lock helpers accept only the already sampled explicit instant from
  fixed production callers.
- Produces and validates immutable `route_shadow_storage_admission/v1`
  evidence beneath `routes/shadow/operational/storage/`, with an atomically
  replaced exact `latest.json` pointer.

The admission record exact key set is `schema,admission_id,operation,subject_id,evaluated_at,high_water_bytes,inventory_bytes,storage_journal_bytes,storage_journal_reserve_bytes,operational_receipt_bytes,operational_receipt_reserve_bytes,outstanding_reservation_bytes,outstanding_reservation_set_sha256,planned_artifact_reserved_bytes,projected_total_bytes,inventory_fingerprint_sha256,protected_set_sha256,planned_artifact_plan_sha256,storage_pressure,admit_new_run,reason_codes`.
`operation` is exactly `baseline|run_capture|run|candidate|phase|promotion|rollback|enable`; `subject_id` is the
canonical run/candidate/transition/promotion/rollback/enable identity (or dispatch identity
before a run ID exists, and a candidate/source/dispatch identity for baseline
preflight). Operation `run_capture` uses exact ASCII
`run_capture:<run_id>`; operation `run` uses exact ASCII
`run:<run_id>:<run_capture_admission_sha256>:<capture_sha256>`. The two SHA
segments are lowercase 64-hex and the run ID uses Task 1's grammar, so B's
subject itself serializes the A0/capture binding without widening the exact
artifact-plan schema. Parsers reconstruct and compare all three components;
no caller supplies an opaque subject string. Byte counts are bounded nonnegative integers,
decisions are literal booleans, and reason codes are a sorted unique closed
list. `inventory_bytes` excludes the separately measured storage journal and
only files currently classified as rolling operational receipts, while
`inventory_fingerprint_sha256` still binds those paths and every other
non-journal root. A receipt closure referenced by an immutable historical
gate/transition/promotion/rollback is reclassified into main inventory even if
its physical path remains under `schedule/`, `ledger/`, or `ops/`; no byte is
ever present in both categories. Freeze the equation:
`projected_total_bytes = inventory_bytes + storage_journal_bytes + storage_journal_reserve_bytes + operational_receipt_bytes + operational_receipt_reserve_bytes + outstanding_reservation_bytes + planned_artifact_reserved_bytes`.
No term overlaps another.
Under the collection lock, enumerate every earlier admitted nonterminal
`run_capture|run|candidate|phase|promotion|rollback|enable` record. For each,
descriptor-verify its exact plan and installed allowed paths, then reserve
`max(0, planned_artifact_reserved_bytes - installed_planned_bytes)` as an
outstanding virtual allocation. Installed bytes are already in inventory and
therefore are subtracted; rejected admissions and transactions with their exact
operation-specific completion closure reserve zero. Sort tuples
`(admission_sha256,operation,subject_id,
remaining_bytes)` and hash their exact canonical projection as
`outstanding_reservation_set_sha256`; sum them as
`outstanding_reservation_bytes`. Missing/ambiguous/unsafe transaction closure
is pressure/interference, never zero remaining.

Only the owner recovering the same immutable admission may consume its
remaining allocation through `replay()`; it does not create another B or add
the same plan twice. Every unrelated admission counts all outstanding prior
allocations plus its new plan. As owned planned files are installed,
inventory rises and that admission's virtual remainder falls by the same
verified bytes; unused remainder is released only when the matching
operation-specific completion closure is durable. This is the matching
terminal for every operation except desired-true enable: its committed terminal
does not release B until the exact bound `activation.json` is durable, while an
owned committed/quarantined desired-false child may release the abandoned true
allocation after proving its cleanup. Crash-after-B tests for run capture, run, candidate, phase,
promotion, rollback, and enable attempt an unrelated maximum admission and
then resume the owner, proving neither path can overlap or exceed 4 GiB.
For this equation, rolling operational receipts are only the current selected
window's otherwise-unreferenced schedule/slot/worker/ops events,
candidate-install events, primary-contention receipts, primary scheduled-run
receipts, and the bounded
emergency-disable recovery set. Any member referenced by frozen historical
gate evidence leaves this category and enters `inventory_bytes`. Canonical enabled authority and normal
enable-transaction leases/stages/journals are inventory/planned-artifact bytes,
even though their path contains `operational/`; a true-enable B therefore
reserves them exactly once in `planned_artifact_reserved_bytes`. A desired-false
safety transaction consumes the separate 512 KiB emergency slice and is
included in inventory at the next refresh. Tests assert category-disjoint path
classification as well as arithmetic.

Fix `operational_receipt_reserve_bytes=96 * 1024**2`; it covers the protected
inclusive seven-day population of `96 * 7 + 1 = 673` grid points plus two
maximum future dispatcher/worker/ops receipt sets (`675` total). Freeze a
per-slot maximum of 68 KiB: slot claim 2 KiB, reservation 4 KiB, schedule
terminal 2 KiB, dispatcher service plus runtime 4 KiB each; worker `started` 4
KiB, `verification` 8 KiB, `terminal` 4 KiB, `service` 4 KiB, and `runtime` 4
KiB (24 KiB worker total); ops-launch 2 KiB; one ops attempt at 12
KiB (`started` 2 + runtime 4 + terminal 2 + service 4), one logical ops
terminal 4 KiB; full-run candidate-install event 4 KiB plus its mutually
exclusive primary-guard/schedule-envelope side file 4 KiB; and 2 KiB
directory/staging metadata. The ops attempt count is exactly one under Task 6;
no stale attempt can consume another slot allocation. Thus slot reserve is
exactly `675 * 68 KiB = 47,001,600` bytes. Add at most 4 MiB of primary-contention
receipts, 1 MiB of primary scheduled-run timing receipts, and an exact 8 MiB
cap for non-grid/duplicate/synthetic pre-admission dispatcher-worker-ops
failure evidence. Reserve 512 KiB for one maximum emergency-disable
lease+stage+transaction/authority recovery set, and use the remaining
39,505,920 bytes only for bounded atomic-publication/cleanup overlap; the full
arithmetic is `47,001,600 + 4,194,304 + 1,048,576 + 8,388,608 + 524,288 +
39,505,920 = 100,663,296` bytes, exactly 96 MiB. That rolling class, not every
file beneath those physical roots, has an independent 96 MiB hard cap and bounded oldest-unprotected
cleanup. Historical referenced closures remain protected under the main 4 GiB
inventory limit; protected rolling bytes over the class cap are pressure and
never deleted. Admission conservatively
adds the full reserve on top of current operational bytes, leaving headroom for
the mandatory reservation and blocked/invalid terminal written before the next
refresh. Repeated blocked-storage slots near high water remain durable, prune
only expired unreferenced receipts, and never exceed the 4 GiB projected
boundary.
Tests fill each receipt class to its exact maximum across 673 protected plus
two future slots, one maximum control transaction, contention, and cleanup
headroom, and reject any one-byte overflow, extra record type, or a
calendar count other than the regular 96/day grid.
Separate tests commit a canary transition and multiple later full
promotion/rollback gates with disjoint seven-day populations: every frozen
population closure moves exactly once into main inventory, the current rolling
window still stays under its 96 MiB class cap, and no historical reference is
deleted or double-counted.

Every operational writer that can run before normal retention/admission shares
one descriptor-opened `routes/shadow/operational/.pre-admission-cap.lock`.
Normal grid-slot files consume the already reserved 68 KiB slot; primary
contention uses its separate 4 MiB protocol. All stale/direct/manual-trigger,
duplicate-slot loser, synthetic kill-before-reserve, direct-worker, and
direct-ops failure closures share the 8 MiB non-grid cap. One closure is at
most 16 KiB. The data limit is `8 MiB - 4 KiB`; the sole no-replace
`pre-admission-overflow.json` is at most 4 KiB and exact
`route_shadow_pre_admission_overflow/v1` with only
`schema,observed_at,attempted_kind,current_bytes,limit_bytes`. Under the cap
lock, a writer requires `current_bytes + 16 KiB <= data_limit` before creating
its invocation directory. At the first overflow it installs only that marker
from the pre-reserved final 4 KiB and returns a hard failure; while the marker
exists every later pre-admission writer creates no per-invocation file. Gate
and release checking hard-block on the marker. The marker and the exact
closures/counts that caused it are permanently protected and excluded from
oldest cleanup until a future explicit resolution contract; deleting the
marker can never silently reopen the cap.

Before admitting another attempt, reconcile every unterminated failure
closure. Cap accounting charges each such closure the full 16 KiB maximum,
including its not-yet-written bytes and directory metadata, rather than only
its current partial file size. A completed closure is charged actual verified
bytes. Thus repeated kills after mkdir or any partial event cannot accumulate
unreserved resumable tails. Concurrent N/N+1, repeated
manual starts, synthetic ExecStopPost, one-byte cap, ENOSPC, and kill tests
prove these paths cannot exceed 8 MiB or bypass the 4 GiB total.

The planned artifact plan is canonical JSON
`{"schema":"route_shadow_artifact_plan/v1","members":[...]}` with members
sorted by canonical location. A literal member contains exactly
`kind=literal,path,role,max_bytes`. The sole dynamic form contains exactly
`kind=phase_transition_child,parent,role,max_bytes,filename_contract,max_children`,
requires literal `filename_contract=declared_transition_id.json` and
`max_children=1`, and is allowed only for the phase transition role beneath
the descriptor-verified `routes/shadow/transitions` parent. It reserves an
unknown post-B filename without allowing an arbitrary prefix, second child, or
identity mismatch. The filename and serialized `transition_id` must equal the
hash of canonical fields excluding that ID; exact full-byte SHA is validated
separately. Only a literal member whose role is exactly `directory_metadata` is a virtual
filesystem-allocation member rather than a regular output file. Its path must
be one exact descriptor-opened directory that this operation creates or adds
entries beneath; `max_bytes` reserves its bounded directory-block/entry growth.
It is counted in the plan/reservation and inventory delta but excluded from the
regular-file actual-artifact set. It never authorizes a child not separately
listed as a literal/dynamic member. Unknown virtual roles, duplicate directory
paths, a directory member for an unrelated ancestor, or actual growth above its
maximum fail closed. Every operation fixture freezes the allowed directory
paths and maxima; no generic cleanup/headroom member exists.
The plan does not pretend to know post-admission content hashes that depend on B. Its
SHA and total reserved bytes are stored in the admission. Canonical plan bytes
are at most 4 MiB. Admission publication atomically installs one immutable
mode-0700 journal directory containing exactly mode-0600 `plan.json` and
`admission.json`; `plan.json` is those exact canonical bytes and its physical
SHA must equal `planned_artifact_plan_sha256`. The admission record is never
published without the plan, and a loader never reconstructs a plan from its
hash or from current source paths. After B, actual
transaction files must use only those paths/roles, stay within each maximum,
and the final-validation artifact binds their exact installed byte SHAs.
`admission_id` is SHA-256 of canonical admission fields excluding itself;
`storage_admission_sha256` elsewhere is SHA-256 of the exact installed
canonical record bytes. Pointer schema
`route_shadow_storage_admission_pointer/v1` permits only `schema,admission_id,admission_sha256`.
Set maximum canonical admission record to 64 KiB, plan to 4 MiB, reservation
terminal to 4 KiB, and pointer to 1 KiB.
`storage_journal_reserve_bytes` is exactly
`2*4MiB + 2*64KiB + 4KiB + 3*1KiB + 64KiB directory-metadata + 64KiB cleanup-overlap = 8,657,920 bytes`; it
covers plan+record staging/final coexistence, one reservation terminal,
old/new/staged latest pointer,
required directory entries, fsync-safe rename, and bounded cleanup overlap.
The high-water decision includes this full amount before writing its own
record. Exact-boundary ENOSPC/high-water tests fail without leaving a partial
record or pointer.
Unknown operation/subject, arithmetic mismatch, plan/path mismatch, or a plan
that omits directory metadata fails closed. Fixtures freeze each operation's
subject and plan so a run admission cannot be reused for promotion/rollback.

Every admitted directory may later gain at most one exact
`reservation-terminal.json` schema
`route_shadow_storage_reservation_terminal/v1` with only
`schema,admission_sha256,operation,subject_id,planned_artifact_plan_sha256,
outcome,finished_at,installed_planned_set_sha256,reason_code`. The only
storage-level outcome is `abandoned`; it releases unused allocation only under
the collection lock after descriptor replay proves the public/state pointer
never advanced and either zero planned path exists or every partial planned
path is exactly owned, safely removed, and the resulting installed set is
empty. Missing/mismatched/third-party paths are interference and retain the
full remaining reservation. Normal operation terminals release the remainder
through their separately validated transaction closure; they do not fabricate
this file. Crash immediately after B for every operation is reconciled to an
abandoned terminal before another admission, while crash after any foreign or
unprovable partial file remains outstanding and blocks. The terminal itself is
inside the journal reserve and permanently binds the plan bytes/hash used for
the decision.

This is the universal post-B restart rule. A lease/hash/ID is never treated as
an invertible copy of pre-B identity, clock, checker, source, profile, or input
bytes. Only the live process that created the lease/stage and still holds the
winning descriptor may continue before the transaction-specific canonical
identity or complete input object is durable. After process restart, a
lease-only, owner-only, or partial pre-identity stage may only be
descriptor-proved, removed, and closed through the storage `abandoned`
terminal while the relevant public/state pointer is unchanged; if any byte/
inode cannot be proved owned, it is persistent interference and the
reservation remains. Once a complete canonical identity/input object is
durable, recovery uses only those installed bytes and never samples a new
clock or reopens a mutable source/profile to recreate them. Phase, promotion,
rollback, candidate, input-capture, run, and true-enable kill-after-lease tests
all enforce this same rule.

SQLite capture and run publication use an acyclic two-admission sequence.
Before reading source bytes, the runner already owns a canonical run ID and
collection lock. It descriptor-opens/stat-checks the canonical SQLite source
without reading it, enforces Task 1's fixed source cap, and creates
`operation=run_capture` admission A0 whose exact literal plan reserves private
`routes/shadow/input-captures/.stage-leases/<run_id>.json`, one mode-0700
`input-captures/.staging/<run_id>.<nonce>.stage`, final
`input-captures/<run_id>/`, `stage-owner.json`, `capture.json`, one mode-0600
`market_facts.sqlite3`, atomic temps, directory metadata, and stage/final
double footprint. A0's maximum snapshot bytes equal the verified descriptor
size, never a caller value. Exactly one ordered pair is legal for a run: one
admitted `run_capture` A0, then one `run` B whose exact operation-specific
subject serializes A0/capture SHA as defined above. A duplicate within either operation, B without the exact
admitted A0, a cross-run A0, or any third admission is interference, not a
fresh allocation.

After A0 is durable, install exact lease
`route_shadow_input_capture_lease/v1` with only
`schema,run_id,storage_admission_sha256,nonce,stage_name,final_name,source_size`,
O_EXCL-create/open the stage, and install permanent exact owner
`route_shadow_input_capture_stage_owner/v1` with only
`schema,run_id,stage_lease_sha256,stage_name,stage_device,stage_inode` while
holding that winning descriptor. Stream the same captured source bytes into
the snapshot, hash them, fsync, and install exact
`route_shadow_input_capture/v1` `capture.json` with only
`schema,run_id,storage_admission_sha256,source_size,source_sha256`; its storage
SHA must equal the lease and exact A0 owner. No WAL/journal member is allowed.
No-replace rename/fsync/reread the final and parse SQLite only through its held
regular-file descriptor in immutable read-only mode, with pre/post-query SHA
and inode checks. All other bounded source files stay in captured memory; no
OS-temp or unadmitted snapshot is permitted. Lease-only, owner/stage, final,
and both-name crash states use the same descriptor-owned recovery rules as
candidate stages. A recovered ownerless stage is interference, not deletable.
The capture closure remains protected while its run is pending and until the
run's immutable baseline/terminal closure makes it eligible for normal
retention.

Only after parsing that admitted capture, for `operation=run` construct the exact plan from the descriptor-captured,
phase-scoped Task 1 universe before any remote collector/RPC call and before
publishing the run universe. The plan must cover the run-universe/baseline
stage and final directory; raw-evidence staging/final directories and accepted
responses; every run-level `typed/` member and `typed-manifest.json`; hidden
atomic temps, directory metadata, and stage/final double footprint; private
core bundle stage/final; exact Shadow sidecar final
`routes/shadow/runs/<run_id>/route-cost-evidence.json` plus one owned hidden
same-directory temp, each at the literal 32 MiB maximum, embedded registry
snapshots, metadata/fsync/cleanup and double footprint; and the audit plus
joint-shadow pointer transaction. The sidecar is a run-plan member, not an
operational receipt or an uncounted core file.
The typed portion
is the selected-leg inventory multiplied by each closed role's production raw
cap and manifest/member envelope cap, never the number of currently observed
members. Candidate input/bundle artifacts use their later independent
`operation=candidate` admission. Scheduled/worker `started`, `verification`,
`terminal`, `service`, and `runtime` files remain in the standing 96 MiB
operational-receipt reserve and are not double-counted in the run plan.
The A0 snapshot is the sole pre-B non-journal mutation. All other
descriptor-captured input parsing is read-only/in-memory, and no run
universe, accepted raw, typed member, core, audit, or stage path is written
until this B record is durable. A near-high-water test reserves the complete
run plan and proves that the last extra byte of a duplicated typed member
blocks before the first collector/RPC call and leaves all run output roots
byte-identical. Separate A0 tests reject one byte beyond snapshot headroom and
kill after stage creation, partial copy, fsync, and rename; failure leaves no
unadmitted temp and never parses a partial or source-mismatched database.
Additional tests leave exactly one byte less than the sidecar double-footprint
reserve, inject ENOSPC/fsync/kill before and after its no-replace install, and
prove zero unadmitted bytes, no joint pointer, and an outstanding B allocation
until owned cleanup/terminalization.
Task 5 upgrades Task 3 verification/terminal storage fields to require this A0
SHA and the exact later run-B SHA; the outstanding-reservation scanner releases
either allocation only through that matching terminal or its own proved-
abandoned storage terminal. A terminal naming another run or a second B cannot
authorize or release either plan.

- [ ] **Step 1: Write failing traversal/reference tests**

Cover current private core, joint shadow pointer, current public pointer,
every descriptor-enumerated committed/unreconciled promotion and promotion
rollback chain, seven-day raw window, 30-day audits, and active validation
history. No caller supplies a list that could omit a protected pointer or
candidate. Also protect every explicitly validated promotion candidate and
prepared-but-unterminated promotion/rollback record; retention cannot delete
evidence while preflight or crash recovery may still reference it.
Permanently protect every terminalized `interference` record and its complete
checker/pointer/prepared/final-validation/commit/dependency closure; until a
future explicit resolution contract exists, retention cannot delete the very
evidence that globally blocks later state changes. A retention cycle after
interference must leave the next promotion blocked.
Protect every storage-admission record referenced by an active gate, phase,
immutable transition, promotion prepared/terminal record, public rollback, or
current validation window. This includes pointer byte files and the original canary admission still
referenced after a long-lived full transition. Test that unreferenced journal
records are pruned while the first canary admission referenced by full phase
survives, and that protected journal bytes over 16 MiB yield pressure with zero
deletes.
Also protect the exact committed or quarantined enable transaction/identity/lease/owner/
prepared/final-live-proof/terminal/activation closure named by current authority; every pending stage/final
enable or disable transaction; every terminal interference closure; and the
latest advancing disable evidence while authority is false. An aborted,
unreferenced older control transaction is prunable only after its bounded
retention window and never while reconciliation or release output references
it. Removing the transaction behind enabled authority makes authority invalid,
not silently standalone true. Tests run retention in true, false, pending,
aborted, and interference states.
Treat external `routes/shadow/operational/armed.json` as part of that authority
closure, never as an unreferenced rolling receipt. It is included in inventory
bytes, the root fingerprint, and protected set whenever a pending/committed true
transaction owns it; retention cannot prune or rewrite it. A committed or
quarantined false head requires it absent, and only the descriptor-proved
disable/recovery owner may remove the exact marker. Missing/mutated armed bytes
under true authority are invalid evidence and remain visible, not cleanup.
Fingerprint/protection tests add, mutate, and remove one marker byte in pending,
true, committed-false, and quarantined states.
Reject symlink ancestors, symlink members, hard-linked
regular files, unexpected file types, unresolved references, `..`, absolute
members, and any candidate outside the dedicated roots.

- [ ] **Step 2: Write the protected-over-high-water test**

Make protected fixtures alone exceed the configured limit. Assert zero deletes,
`storage_pressure=true`, and `admit_new_run=false`. Make only unprotected old
fixtures exceed it and assert oldest-first deletion stops below the limit.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_shadow_retention tests.test_route_shadow_gate tests.test_run_route_shadow -v`

Expected: FAIL because the retention module is absent.

- [ ] **Step 4: Implement fail-closed retention and verify GREEN**

Build and validate the complete protected set before the first deletion. Use
descriptor-relative operations within validated route roots. Return exact byte
counts, protected/deleted file counts, stable reason codes, and a canonical
current inventory/protected-set fingerprint. The fingerprint and 4 GiB byte
total cover raw route cohorts, private core bundles, shadow runs, ledger,
gates, transitions, private `candidate-inputs/`, `schedule/`, `schedule-slots/`, `ops/`, every
`ops-launch.json`, `primary-contention/`, `primary-runs/` and both permanent
overflow markers, canonical enabled authority and
`enable-transactions/`, `enable-stage-leases/`, external exact
`operational/armed.json` and its owned atomic temporary/double-footprint,
private `input-captures/` plus their stage leases/owners and `.staging/`,
complete candidates, candidate input/bundle stage leases/owners and `.staging/`,
`promotions/`, `promotion-rollbacks/`, and every route
pointer/pointer-byte file, including both journals' private `.staging/`
directories and possible double footprint. Exclude only the storage-admission journal
from that self-referential fingerprint. Measure that journal separately,
enforce a 16 MiB hard cap with bounded record retention, and include its
measured bytes plus the full exact `storage_journal_reserve_bytes` defined
above in the admission byte total; it
is never free/unaccounted storage. Delete only unreferenced journal history. If
protected journal records alone exceed 16 MiB, record storage pressure and
delete nothing rather than breaking phase/recovery lineage. Persist exact evaluated-at,
high-water policy, totals, fingerprint, `storage_pressure`, and `admit_new_run`
in an immutable record before advancing the owned latest pointer. The phase
gate descriptor-safely reloads the record and recomputes the current inventory
fingerprint over the protected/raw/audit/candidate roots, explicitly excluding
the storage-admission journal/pointer itself to avoid a self-referential hash;
it requires `admit_new_run=true`, `storage_pressure=false`, an
evaluation at or after the newest included terminal run, and age no greater
than 900 seconds at the caller-supplied gate time. Validators require exact
`0 <= gate_evaluated_at - admission_evaluated_at <= 900s`; future evidence is
invalid, not fresh. Missing, stale, drifted, or
unreadable evidence is `not_evaluated` and blocks phase/promotion.

Expose a narrow `refresh` CLI that acquires the global collection lock before
retention/admission work and returns a machine-readable admitted/blocked
result plus exact admission-record SHA. It obtains time from an injected
trusted UTC clock and exposes no `--now`/backdate flag. The internal held-lock
helper accepts only the exact trusted instant already owned by phase/promotion/
rollback/ops; tests may call a separate pure planner with explicit time. Also expose a held-lock helper for the
runner and promotion transaction. A valid `storage_pressure` result exits zero but starts no collection;
unsafe or internally inconsistent evidence fails nonzero. Task 6's dispatcher
calls refresh before every worker launch and starts no worker unless admitted;
after acquiring the lock itself, `run_shadow_once()` always reruns/reloads the
held-lock admission check and recomputes the inventory fingerprint before its
first source read. That check is the exact `run_capture` A0 transaction above,
not a reused dispatcher baseline; after parsing, the worker obtains the
separate exact `run` B before output publication or a collector/RPC call. Thus
a manual run or dispatcher-to-worker drift cannot bypass the 4 GiB boundary.
A failed A0 makes zero source-byte reads; a failed B makes zero remote source
calls and leaves only the protected, admitted input-capture closure.
Task 6's independent post-worker ops unit refreshes after terminal/service
evidence is durable and before any transition attempt, so
the record is at least as new as the latest terminal. A failed/timeout worker
is covered by that ops unit or the next dispatcher reconciliation. The manual promotion command also
refreshes under the same lock immediately before its gate, passing its exact
candidate ID as protected evidence, and repeats that admission/fingerprint
verification under the final public-pointer commit lock. Barrier tests mutate
inventory after dispatcher admission and during promotion preflight and require
zero source calls or an unchanged public pointer.
Rollback uses the same held-lock refresh/fingerprint helper after computing
checker and exact pointer evidence in memory but before installing them, binds
the returned admission SHA in prepared/final validation, and cannot consume an
unaccounted emergency budget.

Candidate installation is also storage-controlled. Under the collection/routes
locks it writes baseline C, then candidate admission B with a typed plan
covering the exact six-file private candidate-input bundle, dedicated
in-data-dir build staging, the separate frozen five-file v2 public bundle,
SQLite/CSV/manifest maxima, final directory,
cleanup/double footprint, metadata, and the literal at-most-4-KiB
`ledger/<run_id>/candidate-commit.json`. Only
after B may it build/install/reread the immutable candidate; it never advances
the public pointer. Near high water, refusal leaves candidate/staging roots
byte-identical; success must stay within every planned maximum. Reading an
already installed v1/v2 bundle requires no mutation/admission. Tests call the
legacy finalize entry and lock-owning/held-lock writers, require the same exact
post-joint shadow binding, and prove none can bypass candidate admission.

Candidate staging has the same durable ownership standard as publication
journals without adding owner metadata inside either final. Input stage/final
allows exactly six members: opportunity, typed, cost, fee, inventory, and
manifest. Bundle stage/final retains its existing exact five public members;
the new private cost file is never inserted into that public bundle inventory.
Before B, generate a 32-hex nonce and include exact literal paths
two lease/owner pairs
`routes/candidates/.stage-leases/<run_id>.<nonce>.<stage_kind>.{lease,owner}.json`.
For `stage_kind=input`, the stage is exactly
`routes/shadow/candidate-inputs/.staging/<run_id>.<nonce>.stage` and the final
is exactly `routes/shadow/candidate-inputs/<run_id>/`. For
`stage_kind=bundle`, the stage is exactly
`routes/bundles/.staging/<route_cohort_id>.<nonce>.stage` and the final is
exactly `routes/bundles/<route_cohort_id>/`. Include both stage/final
directories and every final member in the artifact plan. After B, install one
exact `route_candidate_stage_lease/v1` per kind with only
`schema,stage_kind,run_id,route_cohort_id,shadow_pointer_sha256,storage_admission_sha256,nonce,stage_name,final_name`,
O_EXCL-create/open that kind's mode-0700 stage, and while holding its winning
descriptor install the corresponding external exact
`route_candidate_stage_owner/v1` with only
`schema,stage_kind,run_id,route_cohort_id,stage_lease_sha256,stage_name,stage_device,stage_inode`.
`stage_kind` is exactly `input|bundle`, and a lease/owner cannot authorize the
other kind.
Cohort/pointer/lease identities use their canonical lowercase formats, nonce
is 32-hex, names are ASCII basenames at most 160 bytes, device/inode are
`1..(2**63-1)`, and each lease/owner is at most 2 KiB. The six input files or
five public-bundle files, according to `stage_kind`, are then created only
inside that proved stage and the final no-replace rename never carries owner
metadata into either final. B freezes every per-file maximum, both complete
stage/final inventories, and their double footprints; wrong-kind sixth/fifth
members and one-byte maxima fail.

Before every retry, descriptor-enumerate both candidate lease/owner pairs,
stages, private input bundles, and final bundles. After restart, lease-only may
not create a fresh stage and follows the universal owned-cleanup/abandon rule;
only the live process that won O_EXCL and still holds the descriptor may write
the owner. Any recovered stage without a valid preexisting owner is
interference even if empty. Exact owner+stage may resume build/rename only
from an already durable, complete canonical input/identity object; otherwise
it is owned-cleaned/abandoned or remains interference. It may remove
only descriptor-proved owned partial files; final-only is accepted only after
full candidate/input-manifest reread; both must be byte-identical or are
interference. Install and reread the private input final before building and
installing the bundle final; the latter's manifest binds the former, and a
crash between the two is a resumable input-only state rather than an installed
candidate. Candidate retries are idempotent for the same shadow pointer and
reject a different pointer/cohort. Kill tests cover input-stage creation, each
private/public file, lease/owner, stage fsync, rename, reread, and
`candidate.json`; SIGKILL cannot leave an uncounted or foreign-deletable
`.route-opportunity-*` tempfile.
Cross-run, cross-cohort, swapped-kind, or transplanted lease/owner/stage/final
names fail before cleanup; recovery derives every path from the validated
run/cohort/nonce rather than trusting a serialized arbitrary path.

Phase, promotion, and rollback transactions use an acyclic admission order.
Under the required locks, write a fresh empty-plan baseline admission C as the
first filesystem mutation; C is at/after the newest terminal and makes gate/
preflight storage evidence current. Compute gate/checker/identity/pointer bytes,
their IDs, and the complete typed artifact plan purely in memory; network/
release checks are read-only. Call held-lock transaction admission B with that
subject/plan, which counts its own bounded journal record through the standing
journal reserve. B is the first non-storage mutation boundary. Only after B is durable may phase install gate A, its guard-or-envelope side file, transition, and
`phase.json`; promotion may install identity/checker/primary-guard/optional-recovery-guard/gate/pointer/
prepared/final-validation/commit and then public pointer; rollback may install its
corresponding planned evidence including its optional recovery guard and source-head prior pointer. Transition and
prepared bytes bind B's exact SHA. No gate, identity, checker, primary-guard, pointer side
file, or directory is created before B, and no no-replace content is mutated
after installation. ENOSPC/high-water and barrier tests before C/B
leave every non-storage root byte-identical; a crash after B but before the
first planned artifact leaves a replayable plan+admission pair whose full
unused bytes remain outstanding. Reconciliation proves zero planned paths and
writes the exact storage-level `abandoned` terminal before releasing them; it
never calls the record unreferenced or silently forgets the reservation.

Hold the collection lock (and routes lock for promotion/rollback) across
refresh B and the final mutation. B reserves the declared maximum total bytes
plus directory metadata for every exact transition/prepared,
`final-validation.json`, `commit.json`, pointer, and terminal record still to
be written. Before
the pointer mutation, re-enumerate and require the inventory to equal B plus
only the plan's paths/roles within their per-file maxima, then bind the actual
exact SHA/size set in final validation; any unrelated or oversized delta
blocks. Tests prove the transaction's own planned artifacts do not
self-invalidate while any third-party file added at the same boundary blocks.
For each named inventory root above, a one-byte growth changes both total and
fingerprint; tests cover all roots rather than one aggregate fixture.

Wire this module as Task 4's fixed production `StorageAdmissionContract` and
rerun transition, v2 promotion, rollback, crash recovery, and pointer-sentinel
integration tests without injection. Only this task turns those production
paths from explicit `not_evaluated` into eligible state changes.
Contract tests run the same corpus through the Task 4 deterministic double and
the real Task 5 adapter, requiring byte-identical views for admit/replay success
and the same closed failure codes for operation, subject, plan, time, and SHA
mismatches.

Run: `python3 -m unittest tests.test_route_shadow_retention tests.test_route_shadow_gate tests.test_route_candidate_inputs tests.test_route_cost_evidence tests.test_run_route_shadow tests.test_route_shadow_inputs tests.test_route_collection tests.test_route_publication tests.test_route_opportunity tests.test_release_smoke tests.test_opportunity_api tests.test_freshness -v`

Then use real CPython 3.8.10 for
`tests.test_route_shadow_retention tests.test_route_shadow_gate tests.test_route_candidate_inputs tests.test_route_cost_evidence tests.test_run_route_shadow tests.test_route_shadow_inputs tests.test_route_collection tests.test_route_publication tests.test_release_smoke tests.test_opportunity_api tests.test_freshness` and import
`scripts.route_shadow_retention`, `scripts.route_candidate_inputs`,
`scripts.route_cost_evidence`,
`scripts.run_route_shadow`, `scripts.route_shadow_inputs`,
`scripts.collect_route_cohort`, the Task 4
promotion/gate/publication modules,
and dashboard opportunity/server modules. This run must exercise the real
default storage adapter with no injected contract; missing 3.8 is a blocker.

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/route_shadow_retention.py scripts/route_shadow_gate.py scripts/promote_route_opportunities.py scripts/route_candidate_inputs.py scripts/route_cost_evidence.py scripts/route_publication.py scripts/collect_route_cohort.py scripts/run_route_shadow.py scripts/route_shadow_inputs.py tests/test_route_shadow_retention.py tests/test_route_shadow_gate.py tests/test_route_candidate_inputs.py tests/test_route_cost_evidence.py tests/test_route_collection.py tests/test_run_route_shadow.py tests/test_route_shadow_inputs.py tests/test_route_publication.py tests/test_release_smoke.py tests/test_opportunity_api.py tests/test_freshness.py
git commit -m "feat(routes): bound shadow evidence retention"
```

Add a GitHub commit comment with protected-over-limit and path-safety evidence.

### Task 6: systemd units, exact schedule, and runtime limits

**Files:**
- Create: `deploy/systemd/cex-dex-route-shadow-dispatch-user.service.in`
- Create: `deploy/systemd/cex-dex-route-shadow-canary-user@.service.in`
- Create: `deploy/systemd/cex-dex-route-shadow-full-user@.service.in`
- Create: `deploy/systemd/cex-dex-route-shadow-ops-user@.service.in`
- Create: `deploy/systemd/cex-dex-route-shadow.timer`
- Create: `deploy/systemd/cex-dex-route-shadow.env.example`
- Create: `deploy/systemd/cex-dex-primary-shadow-scheduled.conf.in`
- Create: `scripts/dispatch_route_shadow.py`
- Create: `scripts/run_route_shadow_ops.py`
- Create: `scripts/route_shadow_runtime.py`
- Create: `tests/test_route_shadow_runtime.py`
- Create: `tests/test_route_shadow_ops.py`
- Modify: `deploy/render_runtime_templates.py`
- Modify: `scripts/install_collection_timers.sh`
- Modify: `scripts/route_root_binding.py`
- Modify: `scripts/route_shadow_authority.py`
- Modify: `scripts/run_route_shadow.py`
- Modify: `scripts/route_shadow_gate.py`
- Modify: `scripts/route_shadow_retention.py`
- Modify: `scripts/route_cost_evidence.py`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_deploy_templates.py`
- Modify: `tests/test_collection_cycle.py`
- Modify: `tests/test_collection_framework.py`
- Modify: `tests/test_framework.py`
- Modify: `tests/test_route_root_binding.py`
- Modify: `tests/test_route_shadow_authority.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_run_route_shadow.py`
- Modify: `tests/test_route_shadow_gate.py`
- Modify: `tests/test_route_shadow_retention.py`
- Modify: `tests/test_route_cost_evidence.py`

**Interfaces:**
- Produces rendered dispatch, canary-worker, and full-worker services plus the
  installed `cex-dex-route-shadow.timer`.
- Produces fixed public: `dispatch_shadow_worker(data_dir: Path) -> str`.
- Produces fixed public: `run_shadow_ops(data_dir: Path, *, dispatch_id: str) -> dict`.
- Produces durable schedule records under
  `routes/shadow/schedule/<dispatch_id>/{runtime,reserved,terminal,service}.json`.
- Produces one no-replace slot claim at
  `routes/shadow/schedule-slots/<slot_id>.json` for every accepted timer slot.
- Produces: `verify_runtime_limits(properties: Mapping[str, str], *, service_kind: str, phase: Optional[str]) -> dict`.
- Produces internal: `persist_runtime_limit_evidence(data_dir: Path, *, service_kind: str, run_id: Optional[str], dispatch_id: str, attempt_id: str, invocation_id: str, properties: Mapping[str, str], captured_at: datetime) -> dict`; only the fixed entry wrapper supplies the already sampled trusted instant/properties.
- Produces a strict loader for the optional fixed user-manager environment file
  `%h/.config/cex-dex-market-monitor/route-shadow.env`; only the four paths
  `MARKET_CEX_PRIVATE_FEE_PROFILE` and
  `MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE`,
  `MARKET_ROUTE_TRACE_RPC_PROFILE`, and
  `MARKET_ROUTE_SUBMISSION_CONNECTOR_PROFILE` are accepted.
- Produces fixed public: `preflight_shadow_enable(data_dir: Path, *, base_url: str, expected_application_sha: str) -> dict`.
- Produces fixed read-only
  `verify_route_cost_runtime_capability(data_dir: Path) -> dict`, which performs
  the fixed trace/connector health probes and local SSHSIG known-answer check
  without accepting an argv/path/client override.
- Produces fixed public: `set_route_shadow_enabled(data_dir: Path, *, desired_enabled: bool, base_url: Optional[str], expected_application_sha: Optional[str]) -> dict`.
- `run_shadow_ops()` acquires the collection lock, samples its trusted clock
  exactly once, and passes that instant to storage refresh and every held-lock
  gate/transition helper. Its production CLI exposes no `--now` or timestamp
  input; its public signature accepts no clock. Task 3's worker follows the same rule.
  Backdated worker/ops argv is rejected before any state change.
- Every production wrapper above resolves one fixed bounded systemctl adapter,
  trusted wall/monotonic/boot clock, and the fixed Task 4/5 helpers internally.
  A module-private identity-capability test harness may substitute those exact
  no-argument seams; it cannot select units/properties/argv and is unreachable
  from CLI/public imports. Public calls with `clock`, `systemctl`, a fake
  storage contract, or any callable/None override raise before opening the
  operational root. Signature/import scans and backdated/fake-systemctl tests
  prove zero durable writes.
- `--enable-route-shadow-canary` requires explicit HTTPS/local-loopback
  `--dashboard-base-url` and lowercase 40-hex `--expected-application-sha`;
  neither can be inferred from the installer's environment after the live
  check.
- The installer also exposes mutually exclusive `--disable-route-shadow`, a
  safety transaction requiring neither URL nor app SHA. No generic boolean or
timestamp flag exists. Enable executes and validates the fixed equivalent of
  the crash-safe sequence disabled/inactive timer -> true authority + armed
  marker + durable committed terminal -> `systemctl --user enable
  cex-dex-route-shadow.timer` -> `systemctl --user start
  cex-dex-route-shadow.timer`; disable executes and validates
  `systemctl --user disable --now cex-dex-route-shadow.timer`
  and stops every dispatch/worker/ops instance. The allowlisted live timer
  projection has only `UnitFileState,ActiveState,SubState`: committed true
  requires `enabled,active,waiting`, while committed false requires
  `disabled,inactive,dead` plus no active route instance. Any other/mixed state
  is resumable only through the desired-state matrix or becomes interference.

All Task 6 operational evidence uses one shared descriptor-relative writer
contract. Starting from the already opened canonical route root, it opens each
ancestor directory with no-follow semantics, holds/rechecks `(st_dev,st_ino)`,
and never treats a directory's link count as a single-link file assertion.
Every evidence member is canonical UTF-8/LF, bounded, regular,
`st_nlink=1`, opened `O_NOFOLLOW`, installed `O_EXCL`/no-replace, file-fsynced,
then parent-fsynced and descriptor-reread before it can be referenced. Exact
identical retry is accepted only where its schema explicitly permits
idempotency; a conflicting preexisting file, hardlink, symlink, unknown member,
ancestor swap, or name-to-inode change is interference. Atomic pointer
replacement retains prior bytes and uses owned rollback on post-replace
fsync/reread failure. Apply this contract to schedule directories and slot
claims, dispatcher/worker/ops runtime and service evidence, ops launch/attempt/
logical terminal, primary-run and non-grid cap records/markers, enable leases/
stages/journals, and `enabled.json`. Specialized candidate/phase/promotion
protocols retain their stricter leases. The external
`routes/shadow/operational/armed.json` marker and its owned atomic temporary are
explicitly covered by this writer contract, inventory fingerprint, pending/
true protected closure, and false-state absence check; they are never treated
as rolling unreferenced receipts. Tests inject symlink ancestors/members,
regular hardlinks, directory ABA swaps before/after syscall, conflicting
preexistence, partial writes, fsync failure, and third-party replacement; owned
cleanup never deletes a later foreign inode.

- [ ] **Step 1: Write failing rendered-unit tests**

Assert both workers use `Type=oneshot`, `RemainAfterExit=no`,
`RefuseManualStart=no`, `Nice=15`, `KillMode=control-group`, `UMask=0077`, and
`TimeoutStartSec=90s`. Canary has `CPUQuota=50%`, `MemoryMax=512M`, and
`TasksMax=16`; full has `CPUQuota=80%`, `MemoryHigh=512M`, `MemoryMax=768M`, and
`TasksMax=32`. Each worker passes its own literal `--expected-phase`, contains
no public-promotion flag, and has no collection/transition `ExecStartPost`.
The dispatcher and ops units also use `Type=oneshot`, `RemainAfterExit=no`,
`Nice=15`, `KillMode=control-group`, `UMask=0077`, `CPUQuota=25%`,
`MemoryMax=256M`, and `TasksMax=8`. Dispatcher freezes
`TimeoutStartSec=120s,TimeoutStopSec=15s` and `[Unit] RefuseManualStart=yes`;
ops freezes `TimeoutStartSec=60s,TimeoutStopSec=15s`. A timer dependency may
start the dispatcher but direct `systemctl start` is refused and writes no slot
evidence. Every unit resets all four private-profile variables to empty and then
reads only `EnvironmentFile=-%h/.config/cex-dex-market-monitor/route-shadow.env`.
The exact tracked example file contains only the four allowlisted empty keys.
Installer/preflight rejects duplicate, unknown, shell-expansion, relative,
newline/control, or unsafe profile entries and never imports a caller shell or
user-manager variable as a fallback. `--data-dir` remains the sole canonical
data-root input and cannot be changed by this file.
Nonblank values must be canonical absolute direct regular owner-only paths and
their target mode/owner/nofollow/schema boundaries are revalidated inside the
worker. Authorization remains inside those files and never in Environment=,
argv, journal, or evidence. Enable preflight and the release checker run one
bounded trace-capability probe plus one connector health/signature probe using
the fixed production adapters, report only profile generation/endpoint ID/
connector ID/status, and redact all URL/auth bytes. Missing profiles may run a
research-only Shadow but set `route_cost_capability=unavailable` and
`promotion_ready=false`; unsafe/malformed configured paths fail enable. Only
both passing probes permit the adapter-canary/full gates to claim strict V2
coverage. Fixtures prove that a shell/user-manager value is ignored unless the
fixed file contains it, that restart/daemon-reload is required after a profile
change, and that no secret appears in rendered units, ledger, release JSON, or
logs.
`verify_route_cost_sshsig_runtime_capability()` first descriptor-stats exact
`/usr/bin/ssh-keygen` as a regular executable owned by root and then verifies a
checked-in public payload/key/armored-signature known-answer vector through the
same FD, argv, namespace, caps, and timeout as production; the fixture contains
no private key. Binary absence/change, unsupported `-Y`, nonzero exit, timeout,
or vector mismatch yields only a nonsecret unavailable status and forces
`promotion_ready=false`. The live trace probe performs fixed `eth_chainId` plus
one bounded harmless trace-capability request. Connector health posts the exact
empty-member capability challenge to `/v1/health`, verifies its signed nonce/
connector/key identity, and persists no auth/body. The trace RPC and submission
signer are operator-owned prerequisites, not undeclared services invented by
this repository: operator docs freeze profile installation/mode/owner,
connector health/batch protocol, public-key rotation with overlapping validity,
daemon reload/restart, and a passing known-answer preflight. Absent/unhealthy
services may run explicitly research-only Shadow but can never pass adapter
canary, full gate, or release.
Do not change primary service behavior merely by rendering/installing Shadow.
Only the committed enable transaction installs the managed drop-in at exact
installed user-unit paths `cex-dex-{daily,depth}.service.d/route-shadow.conf`, rendered
from `cex-dex-primary-shadow-scheduled.conf.in`. It sets
`[Unit] RefuseManualStart=yes`, clears `ExecStart=`, and restores the fixed
profile-specific command with the sole added `--scheduled-systemd` flag.
Disable stops Shadow first, removes only byte-identical owned drop-ins, runs
daemon-reload, and proves original unit commands/manual-start policy are active.
An absent/foreign/conflicting drop-in, system-level primary unit, inactive
primary timer, timer `Unit`/FragmentPath mismatch, or effective command/property mismatch blocks enable; feature-off
tests prove the original templates/commands and manual service behavior are
byte-equivalent. Timer-triggered primary starts remain permitted while direct
systemctl starts are refused only during committed Shadow authority.
Add first-enable RED cases for storage one byte over the admitted boundary,
live Web route-root mismatch, and kills after identity/B/prepared, authority
true, timer acceptance, timer verification, all units stopped, authority
false, and terminal. Each case must reconcile to one exact safe state or
persistent interference, never true-without-timer or active-with-false.
Set worker `TimeoutStopSec=15s`. One bounded `ExecStopPost` helper first
reconciles and fsyncs worker `service.json`, then uses literal
`systemctl --user --no-block start` for the matching ops template instance and
fsyncs exact `routes/shadow/ops/<dispatch_id>/launch.json` outcome
`accepted|failed`. An ops-launch failure
is durable and terminalized by the next dispatcher; it is never relaunched
outside the original entry deadline, and only a later slot may launch fresh
ops. After recording it, the helper
does not rewrite an otherwise truthful worker service result. Unsafe service
reconciliation itself still fails closed. Schema
`route_shadow_ops_launch/v1` permits only `schema`, `dispatch_id`,
`worker_run_id`, `worker_terminal_sha256`, `worker_service_sha256`, `ops_unit`,
`outcome`, `observed_at`, and `reason_code`, at most 2 KiB. It binds the exact
completed worker closure; accepted requires null reason, while failed requires
one closed launch reason. Cross-worker/dispatch transplant fails.
Assert write paths include only market data and required runtime paths.

The profile environment is intentionally optional: absent/empty keys produce
the exact missing-profile candidate generations and keep strict opportunity
status unavailable; two validated absolute profile paths reproduce their
exact retained generations. A shell containing profile variables while the
manager file is absent still yields missing, while changing the controlled
file takes effect only after `systemctl --user daemon-reload` plus restart.
Tests cover absent, available, modified, unsafe, and manager-restart cases and
prove journal/audit records retain only opaque profile IDs/generations, never
the private path or file content.

The ops service is an independent oneshot with `TimeoutStartSec=60s`,
`CPUQuota=25%`, `MemoryMax=256M`, and `TasksMax=8`. It first proves the linked
worker terminal plus service record are durable and requires its entry
monotonic time no later than `triggered_monotonic_ns + 240s` (at most 241
seconds after the scheduled point). A later queued job writes a failed attempt
and makes zero lock/refresh calls. Only then does it acquire the global
collection lock. For a completed canary worker it calls only Task 4's
`transition_shadow_phase_held_lock()`, which performs the gate and Task 5
post-gate held-lock storage refresh as one transaction without reacquiring the
lock. Full/failed worker ops call Task 5's held-lock storage refresh helper
directly and only refresh/reconcile. Neither path invokes a lock-owning CLI or
opens a second collection-lock descriptor. A normally evaluated `not_ready`
transition is exit zero;
only corrupt/unsafe state is nonzero. Persist exact
attempt evidence under
`routes/shadow/ops/<dispatch_id>/attempts/<ops_invocation_id>/
{started,runtime,terminal,service}.json` and one logical
`routes/shadow/ops/<dispatch_id>/terminal.json`. Ops invocation IDs full-match
lowercase `[0-9a-f]{32}` before any path or unit operation. Attempt
`started.json` is `route_shadow_ops_attempt_started/v1` with only
`schema,dispatch_id,worker_run_id,attempt_id,invocation_id,unit_name,
started_at,boot_id,monotonic_ns`, at most 2 KiB; `attempt_id=invocation_id` and
the unit is the exact ops instance for `dispatch_id`. Attempt `terminal.json`
is `route_shadow_ops_attempt_terminal/v1` with only
`schema,dispatch_id,worker_run_id,attempt_id,outcome,finished_at,
runtime_evidence_sha256,reason_code`, at most 2 KiB. Its service is the shared
`route_shadow_service/v1`, binds the exact attempt terminal and runtime bytes,
and is at most 4 KiB. Runtime evidence follows the exact common contract below
and is at most 4 KiB.

The single logical terminal is `route_shadow_ops_terminal/v1` with only
`schema,dispatch_id,worker_run_id,ops_launch_sha256,outcome,attempt_id,
attempt_terminal_sha256,service_sha256,finished_at,reason_code`, at most 4 KiB.
It always binds exact `launch.json`. Outcome is
`success|failed|timeout|oom|unexplained`. A failed launch has zero attempt
directory, all three attempt/service fields null, outcome `failed`, and the
matching launch reason. An accepted job that never reaches Python before the
entry deadline also has zero attempt and null fields but outcome `unexplained`
with exact reason `accepted_job_never_started`. Any actual attempt requires all
three lowercase 64-hex/bounded attempt fields and its normalized service
outcome; success requires that one successful attempt/service and no second
attempt. No other zero/one-attempt null combination is legal.
Exactly zero or one attempt may exist for a dispatch. An identical
reconciliation of that attempt is idempotent; a second/concurrent invocation,
cross-dispatch/run bytes, two winners, or reuse of an old service are hard
blocking interference. The next dispatcher never reruns an old ops job outside
its `+240s` entry deadline: it reconciles an unfinished prior attempt to one
truthful failed/timeout/OOM/unexplained logical terminal, then the new slot's
fresh worker/ops pair performs the next refresh/transition. It starts no new
worker until that old closure is terminalized. Thus a killed/launch-failed ops
unit cannot silently suppress refresh or transition, impersonate an old
invocation, or race the next collection lock. Tests kill the first slot's ops,
prove it can never become a late success, then prove only the following slot's
distinct dispatch/worker/ops closure may succeed.

Because transition runs after worker `ExecStopPost`, every completed worker in
its gate window already has exact timeout/OOM/service evidence. The gate uses
the latest contiguous complete scheduled slot under Task 4's exact no-worker
terminal and 300-second current-in-progress rules; a terminal dispatcher
failure cannot move the cutoff backward, and an overdue incomplete slot cannot
be silently omitted. Worker `TimeoutStartSec=90s` covers only bounded run work (including the
five-second dispatch-evidence wait), not storage/gate post-operations. Tests
cover an 89-second run plus slow ops without misclassifying the worker timeout,
and independent ops timeout/reconciliation.

The dispatcher uses its validated systemd `INVOCATION_ID` as `dispatch_id` and
its own service is bounded by `TimeoutStartSec=120s`, `CPUQuota=25%`,
`MemoryMax=256M`, `TasksMax=8`, and `TimeoutStopSec=15s`. `dispatch_id` and
`invocation_id` both full-match lowercase `[0-9a-f]{32}` and must be equal
before constructing a path, reading retention state, or invoking systemctl;
separators, dots, `%`, `@`, uppercase, Unicode/control bytes, and wrong length
fail first. Its live unit must also prove `RefuseManualStart=yes`. It
atomically installs exact-schema `route_shadow_schedule_reserved/v1` before
retention, phase reads, or worker start. It queries only the allowlisted timer
`LastTriggerUSec,LastTriggerUSecMonotonic` pair as actual trigger time plus the
allowlisted current-service `Id,InvocationID,ExecMainStartTimestamp,
ExecMainStartTimestampMonotonic,RefuseManualStart` properties. Calls use fixed
argv, `LC_ALL=C`, bounded output, and one strict parser; they never query a
caller-selected property. It derives the unique
preceding `scheduled_for` from the literal calendar grid and requires
`0 <= triggered_at - scheduled_for <= AccuracySec` (one second), rather than
incorrectly requiring the actual trigger itself to equal the grid timestamp.
It also requires `triggered_at <= service_started_at <= triggered_at + 1s`, the
live service InvocationID to equal its environment. Derive `slot_id` only as
SHA-256 of exact canonical UTF-8 JSON
`{"scheduled_for":<canonical-UTC>,"schema":"route_shadow_schedule_slot_identity/v1","timer_unit":"cex-dex-route-shadow.timer"}`
with sorted keys, no insignificant whitespace, and one trailing newline. The
lowercase 64-hex ID never includes `dispatch_id`. O_EXCL claim
`route_shadow_schedule_slot/v1` has only
`schema,slot_id,timer_unit,scheduled_for,dispatch_id`; its filename must equal
the recomputed ID. Therefore two invocations for one timer/grid instant race
the same path, while another timer identity or instant cannot collide. Freeze
the identity bytes/hash in tests and reject a claim whose payload or filename
substitutes dispatch identity into the slot domain. Stale LastTrigger values or
a direct script invocation outside systemd install a null-timestamp invalid reservation
plus `invalid_trigger`, make zero retention/source/systemctl calls, and fail
closed. `RefuseManualStart=yes` means a real direct `systemctl start` is rejected
by systemd and creates no dispatcher invocation or slot evidence even inside
the timer window; a golden and real-systemd test proves the timer dependency can
still start it. Defense-in-depth tests that invoke two validated dispatcher
processes for one timer event make them contend for the same no-replace slot
claim; the loser descriptor-loads the winner claim
and writes a bounded non-grid reservation/terminal with
`trigger_status=duplicate`, outcome `duplicate_slot_claim`, the canonical
slot/scheduled/triggered values, and the exact winner claim SHA. It cannot
manufacture another grid sample.
Manual research runs use Task 3's direct `run` CLI, never the dispatcher, have
no slot claim, and remain excluded from readiness. Exact terminal outcomes are
`invalid_trigger`, `blocked_storage`, `invalid_phase`, `worker_start_failed`,
`worker_started`, `duplicate_slot_claim`, or `unexplained`.
Reserved permits only `schema`, `dispatch_id`, `invocation_id`,
`trigger_status`, `slot_id`, `scheduled_for`, `triggered_at`,
`service_started_at`, `triggered_monotonic_ns`,
`service_started_monotonic_ns`, `dispatch_delay_seconds`,
`slot_claim_sha256`, `runtime_evidence_sha256`, `reserved_at`,
`reserved_monotonic_ns`, `boot_id`; timestamps/slot are canonical for
`scheduled` and JSON null for
`invalid`; duplicate preserves the canonical times/slot and references only
the already validated winner claim SHA. Scheduled winners and duplicates have
a 64-hex claim SHA; invalid has JSON null. `trigger_status` is exactly
`scheduled|duplicate|invalid`. A duplicate is a hard blocking non-grid event
in its selected window, never a second expected slot, acquired denominator, or
completed-slot substitute. Terminal schema
`route_shadow_schedule_terminal/v1` permits only `schema`, `dispatch_id`,
`outcome`, `finished_at`, `worker_unit`, and `reason_code`; worker unit is null
unless its exact template instance start was accepted. Schedule service
evidence uses a separate exact record and never overwrites either event.
Scheduled evidence requires a nonzero current-boot timer monotonic value,
ordered timer/service/reservation monotonic values, and the same validated boot
ID. Read exact lowercase UUID boot identity from the fixed Linux
`/proc/sys/kernel/random/boot_id` descriptor before the timer query and again
after the service query/immediately before publication; both bytes must match
the reserved `boot_id`. A missing, changed, symlinked, oversized, or malformed
source fails before the slot claim. Both wall and monotonic dispatcher delays
are in `[0,1s]`, and the absolute difference between those two independently
computed delays is at most 100 milliseconds; zeroed prior-boot, future,
rebooted, or a wall-clock step larger than that bound is invalid. Canonical
`dispatch_delay_seconds` is recomputed from monotonic nanoseconds, never trusted
from wall time. The exact slot population hash binds all wall/monotonic/boot
fields. Tests change the boot descriptor between the two reads and inject
99/100/101-millisecond delta boundaries, a one-second wall step, zero monotonic,
future service start, and rebooted LastTrigger evidence.
The dispatcher unit has `ExecStopPost` reconciliation. Kill tests at reserve,
refresh, phase-read, and systemctl-start boundaries prove every reservation is
terminalized; if the dispatcher dies before reserve, the reconciler creates a
synthetic unexplained reservation/terminal for its known invocation ID.
Tests cover stale LastTrigger, refused direct systemctl start, direct script
invocation, duplicate slot claims, service/timer wall/monotonic disagreement,
and a crash
between slot claim and reservation; no case can create two samples for one
grid timestamp.

The dispatcher starts the phase-specific template instance
`cex-dex-route-shadow-{phase}-user@<dispatch_id>.service` via a fixed argv and a
strict canonical dispatch ID using literal `systemctl --user --no-block start`.
After job acceptance it immediately fsyncs the `worker_started` schedule
terminal; it never waits synchronously for the oneshot worker. The worker receives `%i` only as
`--dispatch-id`; its own worker `INVOCATION_ID` remains the run ID and its
started/terminal records bind the dispatch ID. On a kill after worker start but
before dispatcher terminal, reconciliation proves the exact instance/run link
or writes unexplained. No worker or dispatcher exit code alone can invent a
linked run. The gate reconstructs every expected timer slot in its rolling
window and rejects a missing/duplicate schedule reservation, terminal, or
required worker link, including a timer trigger whose dispatcher never reached
Python.

Before attempting the collection lock, the worker uses a bounded monotonic wait
(maximum five seconds) for the exact reservation plus `worker_started`
terminal, verifies its own template instance, and requires current monotonic
time no later than `triggered_monotonic_ns + 135s` (at most 136 seconds after
the scheduled grid point including the one-second timer window). A queued job
starting later writes only its bounded failure closure and makes zero lock or
source calls. After acquiring the lock it revalidates phase/runtime/source
generation as specified; missing or mismatched dispatch evidence likewise
yields zero source calls and a failed linked run.
Barrier tests prove the dispatcher terminal is durable before a fast worker can
reach transition evaluation, eliminating a schedule-terminal/transition
dependency cycle.

The timer targets only the small dispatcher. After its durable reservation,
the dispatcher invokes Task 5 refresh, which owns the global collection lock
for retention/admission, and exits zero
without a worker when admission is blocked. When admitted it descriptor-safely reads the phase state and starts
exactly one named worker via allowlisted argv, never a shell-built unit name.
A missing phase file dispatches canary; a valid full
state dispatches full; malformed, symlinked, hard-linked, or changed phase
evidence starts neither. Each worker re-asserts phase only after acquiring the
global collection lock, so a dispatcher/transition race exits before source
reads. Tests cover canary success -> automatic transition -> next dispatch to
full and prove the old canary worker cannot collect after the transition.

Use literal `systemctl show` property maps to reject inactive limits,
`MemoryMax=infinity`, wrong CPU quota, wrong TasksMax, wrong KillMode, and a
canary/full phase mismatch. A rendered unit alone must not set
`runtime_limits_verified=true`.

Install exact runtime evidence before the first state-changing action of every
service: dispatcher before slot/reservation writes or retention, worker after
the collection lock but before source reads, and each ops attempt before the
collection lock or refresh. Paths are respectively
`schedule/<dispatch_id>/runtime.json`, `ledger/<run_id>/runtime.json`, and
`ops/<dispatch_id>/attempts/<ops_invocation_id>/runtime.json`. The dispatcher
and ops service records bind their runtime SHA; the worker terminal and service
both bind its runtime SHA. A rendered unit is never runtime evidence. Missing,
null, mismatched, or `not_evaluated` runtime evidence is a hard gate/release
block. Synthetic killed-before-runtime closures alone may use JSON null with an
exact `runtime_evidence_missing` reason and can never count as successful.
Every ordinary dispatcher reservation binds the exact already-installed
runtime SHA; only its ExecStopPost killed-before-runtime synthetic reservation
uses JSON null and the matching unexplained terminal/service matrix.

Freeze common schema `route_shadow_runtime_limits/v1`, at most 4 KiB, with only
`schema,service_kind,run_id,dispatch_id,attempt_id,invocation_id,unit_name,
phase,phase_state_sha256,captured_at,properties,properties_sha256,status,
reason_codes`. `service_kind` is `dispatcher|worker|ops`. Dispatcher requires
`run_id=null,attempt_id=dispatch_id,phase=null,phase_state_sha256=null`; worker
requires its exact run/dispatch IDs, `attempt_id=run_id`, and canary/full phase
binding; ops repeats the linked worker run ID but requires
`attempt_id=invocation_id` and null phase fields. Every ID and path must match
its validated schedule/ledger/attempt record, with one deliberate ordering
exception: dispatcher runtime is installed first from the descriptor-verified
live `Id`, `InvocationID`, boot identity, and timer-causality evidence before
any reservation exists. Its subsequent reserved and service records must both
bind that exact runtime SHA. Worker and ops retain the ordinary prerequisite
record rule. No value is inferred by splitting a caller-selected unit string.

`properties` has exactly `Id,InvocationID,ActiveState,SubState,Type,
RemainAfterExit,RefuseManualStart,CPUQuotaPerSecUSec,MemoryHigh,MemoryMax,
TasksMax,Nice,KillMode,UMask,TimeoutStartUSec,TimeoutStopUSec`. The fixed
`LC_ALL=C systemctl --user show` argv asks once for exactly those names and has
bounded stdout/stderr/deadline; no shell, caller property list, or localized
parser is accepted. Parse the bounded systemd duration/size forms into
normalized decimal microsecond/byte strings (or literal `infinity`) before
hashing, so allowlisted equivalent host renderings such as `500ms` and
`1min 30s` reproduce one value. Reject negative, fractional-byte, overflow,
unknown-unit, duplicate, missing, or trailing forms. Identity/state/mode values
remain exact strings. The property SHA is over sorted-key/no-whitespace
canonical JSON of that normalized mapping.

Every service requires `Type=oneshot,RemainAfterExit=no,ActiveState=activating,
SubState=start,Nice=15,KillMode=control-group,UMask=0077` and exact
`Id,InvocationID`. Dispatcher requires its fixed service ID,
`InvocationID=dispatch_id`, `RefuseManualStart=yes`,
`CPUQuotaPerSecUSec=250000,MemoryHigh=infinity,MemoryMax=268435456,
TasksMax=8,TimeoutStartUSec=120000000,TimeoutStopUSec=15000000`. Ops requires
the exact template instance for `dispatch_id`, its own 32-hex invocation,
`RefuseManualStart=no`, the same CPU/memory/tasks values, and
`TimeoutStartUSec=60000000,TimeoutStopUSec=15000000`. Worker requires the exact
canary/full instance for `dispatch_id`, `InvocationID=run_id`,
`RefuseManualStart=no`, and `TimeoutStartUSec=90000000,
TimeoutStopUSec=15000000`; canary requires
`CPUQuotaPerSecUSec=500000,MemoryHigh=infinity,MemoryMax=536870912,TasksMax=16`,
while full requires
`CPUQuotaPerSecUSec=800000,MemoryHigh=536870912,MemoryMax=805306368,TasksMax=32`.

Status is `verified|not_evaluated`; verified has an empty reason list. The
sorted reason set is drawn only from `missing_property,duplicate_property,
unknown_property,property_output_too_large,duration_parse_error,
size_parse_error,identity_mismatch,state_mismatch,service_type_mismatch,
manual_start_policy_mismatch,phase_mismatch,resource_limit_mismatch,
mode_mismatch,runtime_evidence_missing`. Any reason means zero downstream
state-changing/source calls except the bounded non-grid failure closure. Tests
freeze all three literal property queries, normalized evidence bytes and exact
paths, accept only the specified equivalent renderings, reject unknown extra
keys and dispatch/run/attempt transplants, and prove a dispatcher or ops drop-in
cannot reach retention or the collection lock.

- [ ] **Step 2: Write failing timer tests**

Assert the regular 96-slot UTC grid for every hour `00..23` at
`:09:00,:24:00,:39:00,:54:00`,
`Persistent=false`, `AccuracySec=1s`, and `RandomizedDelaySec=0`. If
`systemd-analyze` is available, compare the next 48 hours to a literal expected
trigger list; otherwise validate the exact unit fields in Python.
Freeze the complete timer UTF-8/LF bytes to the literal sections and order:
`[Unit]`, `Description=CEX/DEX route shadow dispatcher timer`,
`ConditionPathExists=<canonical-data-dir>/routes/shadow/operational/armed.json`, one blank line,
`[Timer]`, the four lines
`OnCalendar=*-*-* *:09:00 UTC`,
`OnCalendar=*-*-* *:24:00 UTC`,
`OnCalendar=*-*-* *:39:00 UTC`,
`OnCalendar=*-*-* *:54:00 UTC`, then
`Unit=cex-dex-route-shadow-dispatch-user.service`, `Persistent=false`,
`AccuracySec=1s`, `RandomizedDelaySec=0`, one blank line, `[Install]`, and
`WantedBy=timers.target`, with one final LF. A golden rendered-unit test
requires that exact dispatcher service target; relying on systemd's default
same-basename service would target a nonexistent unit and is forbidden.
The golden test also parses the existing primary
`cex-dex-depth.timer` (`*:05 UTC`) and `cex-dex-daily.timer` (`00:30 UTC`).
Using the conservative sequential envelope of the one-second timer window plus
dispatcher `120+15`, worker `90+15`, and ops `60+15` start/stop ceilings, a
complete closure is bounded by 316 seconds after the grid point. Worker and ops
entry-side monotonic checks reject any systemd job queue delay that would move
them beyond the `+136s`/`+241s` boundaries, before either attempts the
collection lock. Therefore `00:24` ends no later than `00:29:16` (44-second
margin before daily), while every `*:54` ends by `*:59:16` (344-second margin
before next-hour depth). A real lock-contention
counterexample at the old `:28` offset must fail; the new grid has no
intentional primary skip. If a primary run actually overruns into `:09`, the
Shadow slot truthfully records `skipped_locked` and the reliability gate may
block promotion rather than hiding the overrun. Tests hold worker/ops jobs past
their exact start deadlines and prove zero lock/source/refresh calls, and use
rebooted/zero/future monotonic values plus a wall-clock step to reject false
causality.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_deploy_templates tests.test_collection_cycle tests.test_collection_framework tests.test_framework tests.test_route_shadow_runtime tests.test_route_shadow_ops tests.test_route_root_binding tests.test_route_shadow_authority tests.test_route_cost_evidence tests.test_release_smoke tests.test_run_route_shadow tests.test_route_shadow_gate tests.test_route_shadow_retention -v`

Expected: FAIL because the route units are absent.

- [ ] **Step 4: Implement templates and installer without enabling by default**

Render and install all five unit files, but require an explicit
`--enable-route-shadow-canary` installer flag before the separately ordered
`systemctl --user enable cex-dex-route-shadow.timer` and later
`systemctl --user start cex-dex-route-shadow.timer`; `--now` is never used on
the enable path. Existing daily/depth behavior must remain byte-equivalent without the
flag. Enabling is an owned, crash-reconcilable transaction rather than a blind
authority write. Under the canonical collection lock, first validate that the
running dashboard's live route-reader root binding equals this exact
`MARKET_DATA_DIR/routes` and its expected app SHA, then run Task 5 operation
`enable` admission with the full 96 MiB (`100663296` bytes) operational reserve plus the exact
enable journal/authority plan. A blocked or unsafe admission leaves authority
false/absent and invokes no `systemctl` mutation, including at one byte below
high-water headroom.

Before storage admission, build exact canonical
`route_shadow_enable_identity/v1` with only
`schema,desired_enabled,prior_authority_sha256,prior_control_transition_id,
route_root_binding_sha256,
market_input_binding_sha256,expected_app_sha,primary_unit_plan_sha256,
primary_schedule_guard_sha256,
primary_bootstrap_sha256,requested_at`.
Its exact byte SHA is `transaction_id`, the known journal directory name and,
for desired true, the operation=enable admission subject; the identity
deliberately excludes B.
For desired true, prior SHA is lowercase 64-hex or JSON null and live
root/app fields are required 64-/40-hex values. For desired false, prior must
be the exact current true-authority SHA, or the exact quarantined-false SHA for
the single repair-finalization child, while root/app fields may be null so a
broken Web deployment cannot prevent the safety stop.
Treat enable and disable as one immutable control-state chain. Before any new
identity, descriptor-replay every canonical enable transaction and require one
unbranched state head. The genesis transition ID is SHA-256 of literal
ASCII `route-shadow-control-genesis/v1\nabsent\n`; the first true identity
binds it. Every later identity binds the exact current advancing
`prior_control_transition_id`, and current `enabled.json.transaction_id` must
equal that head. `committed` advances normally; `quarantined` advances only a
desired-false safety transition whose units are stopped but owned primary-unit
restoration is incomplete. Aborted retries may share a parent and never advance,
while two committed/quarantined children, multiple pending records, or an unexplained
pointer are interference. Repeating the already-current desired state is
idempotent and returns the head terminal without a no-op child, except that a
quarantined false head may create one new desired-false repair-finalization
child after the external base projection has been restored. With no advancing
child, any number of aborted first-enable attempts leave the legal genesis
authority absent/null rather than inventing a transaction head.
Both desired states stop immediately when requested if disabling, then acquire
primary-intent nonblocking and require an exact passing
`route_primary_schedule_guard/v1` with `required_clearance_seconds=60` before
any journal/drop-in/authority mutation. The identity binds its SHA and the
entire mutation plus owned rollback uses that guard's monotonic deadline.
Persist the exact canonical guard bytes rather than only their SHA. On a
permitted bootstrap-mode true enable (pristine first enable or the exact
committed-false re-enable state defined above), `primary_bootstrap_sha256` is
the exact physical SHA of a persisted `route_primary_bootstrap/v1` object and
must equal the guard's nonnull `bootstrap_evidence_sha256`; on every
receipt-mode operation it is JSON null and the guard bootstrap field is also
null. A bootstrap-mode identity without the matching exact bootstrap bytes,
or a receipt-mode identity with such bytes, is interference.
`primary_unit_plan` is exact `route_shadow_primary_unit_plan/v1` with only
`schema,targets,prior_effective_projection_sha256,
target_effective_projection_sha256`; its own SHA is over exact canonical bytes.
`targets` contains exactly daily then depth, each with only
`unit_name,dropin_relative_path,desired_dropin_sha256,prior_dropin_sha256,
observed_state`.
True requires absent/null prior managed drop-ins and exact rendered desired
hashes with `observed_state=absent`. False takes the two expected owned prior
hashes from the referenced committed-true plan, uses null desired hashes, and
records each current descriptor observation as exactly
`owned_match|absent|foreign|unsafe`; it does not require damaged current bytes
to reproduce their expected hash merely to construct a safety action. Relative
targets are fixed user-unit drop-in children, never caller
paths. Effective projections include both primary timers' and services' exact
stable configuration under one canonical set SHA. Each service projection has
only `Id,FragmentPath,DropInPaths,Type,RefuseManualStart,ExecStart`; each timer
projection has only `Id,FragmentPath,Unit,TimersCalendar,AccuracyUSec,
RandomizedDelayUSec,Persistent`. Literal daily/depth values must reproduce the
tracked timer bytes (`00:30`/`*:05`, one-minute accuracy, persistent true,
zero randomized delay) and target the installed same-basename services. Live
`UnitFileState,ActiveState,SubState` are validated separately under the enable/
entry state matrix and are not mixed into the stable configuration hash.
Generate a 32-hex nonce and the exact lease/stage/final inventory before either
path mutates. For desired true, include that inventory in B's plan and install
only after B. For desired false, do not call B; the same bounded inventory is
authorized solely by the emergency-control reserve. The paths are
`routes/shadow/operational/enable-stage-leases/<transaction_id>.json`, stage
`enable-transactions/.staging/<transaction_id>.<nonce>.stage`, final directory,
owner/identity/prepared/primary-guard/optional-primary-bootstrap/primary-units/
optional-recovery-guard/optional-recovery-bootstrap/optional-final-live-proof/
terminal/optional-activation, and atomic temps/double footprint.
The plan also reserves the external atomic authority paths `enabled.json` and
`armed.json` plus exact same-directory temporary names
`.enabled.<transaction_id>.<nonce>.tmp` and
`.armed.<transaction_id>.<nonce>.tmp`. Each temporary filename is derived only
from the already validated transaction/nonce, is regular single-link and
descriptor-owned, and is removed only if its inode still matches. Armed is
required from the preterminal arming boundary through activated true;
activation is required only after successful live systemd activation. Both are
forbidden after a completed desired-false/quarantined head. Then
install exact `route_shadow_enable_stage_lease/v1` with only
`schema,transaction_id,identity,storage_admission_sha256,nonce,stage_name,final_name`,
then O_EXCL-create/open the stage and, while retaining that descriptor, install
permanent exact `stage-owner.json` schema
`route_shadow_enable_stage_owner/v1` with only
`schema,transaction_id,stage_lease_sha256,stage_name,stage_device,stage_inode`.
`identity` is the complete exact `route_shadow_enable_identity/v1` object and
its canonical byte SHA must equal `transaction_id`, so a false safety action
and a true-abort can be reconstructed without reversing a hash or sampling a
new clock. IDs/SHAs are lowercase 64-hex, nonce is lowercase 32-hex, basenames are ASCII
at most 160 bytes, device/inode are in `1..(2**63-1)`, and lease/owner are each
at most 8 KiB/2 KiB respectively. Lease storage SHA is required 64-hex for desired true and JSON
null for desired false. Write/fsync identity, the exact primary guard, the
mode-required bootstrap evidence, primary-unit plan, and prepared in that
proved stage, then
no-replace rename/fsync/reread final
`routes/shadow/operational/enable-transactions/<transaction_id>/`, whose exact
allowed preterminal members are
`stage-owner.json,identity.json,prepared.json,primary-guard.json,
primary-bootstrap.json,primary-units.json,recovery-guard.json,
recovery-bootstrap.json,final-live-proof.json,activation.json`, with `primary-bootstrap.json`
required exactly in bootstrap mode and forbidden exactly in receipt mode.
Recovery files are absent in the normal owner flow, and
`final-live-proof.json` is absent until the final committed-true validation
boundary and `activation.json` is absent until post-terminal systemd activation
has been verified. `recovery-guard.json` is
required before any expired/rebooted owned rollback and must bind this exact
transaction with `recovery_action=rollback_only`; `recovery-bootstrap.json` is
required exactly for recovery-bootstrap mode and forbidden otherwise.
`identity.json` contains the exact identity bytes and `prepared.json` schema
`route_shadow_enable_prepared/v1` with only
`schema,transaction_id,enable_identity_sha256,desired_enabled,
prior_authority_sha256,prior_control_transition_id,
storage_admission_sha256,route_root_binding_sha256,
market_input_binding_sha256,expected_app_sha,primary_unit_plan_sha256,
primary_schedule_guard_sha256,primary_bootstrap_sha256,prepared_at`.
`primary-guard.json` contains the exact canonical
`route_primary_schedule_guard/v1` bytes and its physical SHA must reproduce
both identity and prepared fields. Optional `primary-bootstrap.json` contains
the exact canonical `route_primary_bootstrap/v1` bytes; its physical SHA must
reproduce both identity and guard fields. `primary-units.json` is the separately
constructed exact canonical `primary_unit_plan` bytes and its physical SHA must
reproduce the identity's hash field.
For a committed true outcome only, `final-live-proof.json` is exact
`route_shadow_enable_final_live_proof/v1` with only
`schema,transaction_id,checked_at,route_root_binding_sha256,
market_input_binding_sha256,expected_app_sha,status,reason_codes`. Status must
be `passed`, reasons must be an empty array, all three digests must exactly
equal identity/prepared, and `checked_at` is the one trusted UTC sample taken
after the bounded final live health/root/input checks and their matching
monotonic deadline check. Its physical SHA is stored by the terminal. The file
is forbidden for desired false and every noncommitted outcome; a failed final
live check writes no proof and enters owned rollback.
After terminal plus live timer activation, install exact immutable
`activation.json` schema `route_shadow_enable_activation/v1` with only
`schema,transaction_id,terminal_sha256,armed_sha256,activated_at,timer_unit,
activated_boot_id,activated_monotonic_ns,unit_file_state,active_state,
sub_state,status,reason_codes`. The two SHAs are
the physical bytes already reread; timer unit is exactly
`cex-dex-route-shadow.timer`; the three live literals are exactly
`enabled,active,waiting`; status is `activated` and reasons is an empty array.
`activated_at` and `activated_monotonic_ns` are the paired trusted samples after
the bounded live-state query; UTC cannot precede terminal `finished_at`, boot ID
must equal the original guard boot, and monotonic time must be at or after the
guard evaluation sample and at or before its exact deadline. UTC/monotonic
elapsed values must agree under the guard's existing clock-tolerance rule.
Canonical bytes are at most 4
KiB and installed once under the shared descriptor writer. No command output,
PID, free text, or mutable timestamp source is persisted.
The identity SHA must equal the directory/transaction ID; desired true binds B
while desired false requires null storage SHA. `prepared_at` must equal
identity `requested_at`, and desired state is a literal boolean.

Only after that journal is durable may the transaction mutate the two external
managed drop-ins. True installs each exact rendered file with no-follow,
no-replace, mode 0600, directory fsync, and reread; false removes only a file
whose bytes/inode match the owned plan. After both targets it runs fixed
`systemctl --user daemon-reload` and captures the exact target effective
primary-unit projection. Any partial/foreign state aborts to canonical false:
remove only exact owned installed members (or restore the exact planned prior
state), reload, and prove the prior/base projection. It never rolls back a
third-party replacement. Kills after first drop-in, second drop-in,
daemon-reload, effective projection, and rollback reload are reconciled solely
from identity/primary-units/prepared bytes and actual target identities. A true
authority/timer cannot be committed until both primary timers/services prove
the target scheduled command and policy; disable cannot commit until both prove
the restored base command/policy. External drop-in bytes are outside the 4 GiB
market-data inventory but are bounded to 8 KiB each, hash-bound by the
transaction, and covered by the same path/ownership safety tests.

For desired false, both `owned_match` files may be removed, `absent` is already
safe, and `foreign|unsafe` is never touched. The transaction still stops every
Shadow unit and publishes canonical false. It commits only if both effective
primary units reproduce the base projection; otherwise terminal outcome is
`quarantined` with false authority and the exact repair-required reason. This
safe false transition advances the control head while release remains blocked;
it never masquerades as restored base or enabled. Tests
start from committed true, then separately delete or replace either drop-in and
prove disable remains constructible, never deletes foreign bytes, never leaves
Shadow active, and never falsely reports a restored base projection.

Then descriptor-safely stage/fsync/atomically replace and reread `enabled.json`
exact `route_shadow_enabled/v1` with only `schema,enabled,transaction_id`.
Committed true/false or quarantined false binds this transaction ID and therefore the
unique control-chain head. JSON null is legal only at genesis while no
committed/quarantined child exists; aborted attempts do not make it illegal.

The true activation order has no boot-time pending window. Preflight first
requires the timer disabled/inactive and removes no foreign link. With no timer
enable link present, install true authority, perform the bounded final live app/
root/input checks, recheck the original monotonic deadline, take the single
trusted proof/terminal UTC sample, and install/reread the exact
`final-live-proof.json` above. Set terminal `finished_at` equal to that proof's
`checked_at`, bind the proof's physical SHA, and compute the exact prospective
terminal bytes. Then atomically install
`routes/shadow/operational/armed.json` as exact `route_shadow_armed/v1` with only
`schema,transaction_id,terminal_sha256`; the SHA must be the prospective
terminal's physical SHA. The tracked timer has the fixed
`ConditionPathExists` for this marker. Reread the marker, descriptor-reread the
proof, repeat the same bounded live digest comparisons, and recheck the same
boot/deadline; if any value drifts or the deadline has expired, remove only the
owned marker/proof during canonical false rollback and never install terminal.
Otherwise install/reread that exact precomputed committed terminal immediately,
with no other mutation or unbounded call between the final comparison and
terminal installation.
Pending true plus marker is still inert across reboot because the enable link
does not yet exist; a manual start sees incomplete authority and performs zero
operational/source writes. After the terminal is durable, execute fixed
`systemctl --user enable cex-dex-route-shadow.timer` (never `--now`), verify
enabled/inactive, then fixed `systemctl --user start ...` and verify
`enabled,active,waiting`, persist/reread the exact `activation.json`, and only
then return success. The shared authority loader requires terminal, marker,
activation binding, and current live state, so terminal-before-link and
link-before-start are visibly invalid/safe intermediate states. The enable B
reservation is not released merely by terminal.json; it is released only after
the matching activation receipt, or after a durable
committed/quarantined safety-disable child consumes/cleans those planned bytes.
Failure to enable/start immediately enters that owned child. Kills after true,
marker, terminal, enable-link, start, or activation receipt are resumed under the original guard or
narrowly disabled under the persisted recovery guard; only the post-terminal
link can auto-activate on reboot, when marker and committed authority already
agree. A deployment/root change before terminal restores false and aborts. Terminal schema
`route_shadow_enable_terminal/v1` permits only
`schema,transaction_id,outcome,primary_unit_projection_sha256,finished_at,
reason_code,final_live_proof_sha256,recovery_guard_sha256,
recovery_bootstrap_sha256`; outcome is
`committed|quarantined|aborted|interference`. Committed true binds the exact
target effective projection and requires the exact final-live-proof SHA;
committed false binds the exact restored prior/
base projection, and both require JSON-null reason. `quarantined` is legal only
for desired false after all Shadow units are stopped and false authority is
durable; it advances the safe control head but is never ready. It uses exactly
one reason from `primary_dropin_unsafe|primary_dropin_foreign|
primary_reload_failed|primary_base_projection_mismatch`, in that deterministic
precedence, and binds the last safely verified projection or JSON null when an
unsafe/reload failure prevents one. `aborted` does not advance and uses exactly
one of `guard_failed|bootstrap_failed|storage_pressure|root_binding_changed|
deadline_expired|owned_rollback_complete|activation_failed_rolled_back`.
`interference` does not advance and uses exactly one of
`journal_conflict|authority_cas_mismatch|stage_ownership_mismatch|
terminal_conflict|foreign_authority|recovery_evidence_invalid`; it binds the
last safe projection or null. Command output/free text is forbidden and the
validator recomputes outcome/reason/projection from identity, authority, units,
and journal bytes.

Final-proof/recovery SHA nullability is exact. Only an original-owner committed
true terminal has nonnull final-live-proof SHA; every false, quarantined,
aborted, or interference terminal requires it null. An original-owner terminal
has both recovery SHAs null.
Fresh `enable_recovery_rollback` may write only aborted/interference and
requires both the matching recovery-guard and recovery-bootstrap SHAs. Fresh
`disable_recovery_finalize` may write committed/quarantined/interference and
requires recovery guard nonnull plus bootstrap null. No committed true terminal
has recovery evidence; every other combination is invalid. Exact fixtures cover
every desired-state/outcome/reason/projection row, one-field mutations, and
cross-transaction recovery files. Every dispatcher/worker/ops entry requires a committed true authority
before doing work.

Freeze the enable artifact plan maxima: lease 8 KiB; stage owner 2 KiB,
identity 4 KiB, prepared 4 KiB, primary guard 4 KiB, optional bootstrap 16 KiB,
and primary-unit plan 8 KiB, optional recovery guard 4 KiB, and optional
recovery bootstrap 16 KiB. The worst-case two-name stage/final overlap for all
directory members is 116 KiB; adding the external lease, terminal 4 KiB,
final-live-proof 4 KiB,
activation 4 KiB, `enabled.json` 3 KiB exact old/final/staged pointer overlap,
`armed.json` 4 KiB exact final/staged overlap, and 64 KiB exact directory
metadata yields the exact total `207 KiB` (`211,968` bytes). There is no
unnamed cleanup/reload allowance. The enable plan names both pointer temporary
paths above and five `directory_metadata` members: operational parent 16 KiB,
enable-transactions parent 8 KiB, `.staging` parent 8 KiB, exact stage directory
16 KiB, and exact final directory 16 KiB. True-enable B reserves every listed
path/maximum; desired false consumes at most the same 207 KiB from the standing
512 KiB emergency slice. The unused 305 KiB is safety headroom, not permission for another control
transaction. Per-file and total +1-byte tests fail before authority/systemctl
mutation.

Disable uses the same staged transaction contract with desired false but is a
safety action, not a new-work admission. Its prepared
`storage_admission_sha256` is JSON null and its bounded lease/owner/identity/
prepared/terminal bytes consume only the already-accounted standing
operational-receipt reserve; it never waits for `admit_new_run`, a healthy Web
root, or a successful route gate. Stop and verify
the timer and all route units first, run fixed `disable --now`, descriptor-
remove only the exact armed marker bound to the current true head, and prove the
timer disabled/inactive before atomically installing canonical false
and terminalize. Before every installer/release action, reconcile prepared
transactions against live unit state and authority, including lease-only,
stage-only, final-only, and both-name states. A recovered stage without its
valid preexisting owner is interference even if empty; only the live O_EXCL
creator holding its descriptor may install owner. After restart, an exact
owned stage may resume rename only when complete canonical identity/prepared
bytes are already durable; a pre-identity state follows the universal owned-
cleanup/abandon rule. Mismatched lease/owner/identity/bytes are interference.

Freeze the desired-state recovery matrix. For `desired_enabled=true`, writing
the committed terminal is legal only when current authority is true with this
transaction ID, exact armed marker binds the prospective terminal, the timer is
disabled/inactive with no enable link, no worker/ops unit is active, and final
live app/root proof matches. Returning successful enable additionally requires
the later enable-link and timer start to prove enabled/active/waiting plus the
matching immutable activation receipt. Prior/false plus
inactive units may resume forward only under the original same-boot unexpired
guard. True plus missing terminal can never start the timer; true plus committed
terminal and inactive timer may install the link/start only under that guard.
Committed terminal plus live enabled/active/waiting timer but missing activation
may install the receipt only while that same guard remains valid and all exact
terminal/marker/live fields still match. After expiry/reboot it must enter the
desired-false safety child instead of backdating activation. An activation
receipt with wrong terminal/marker/state/timestamp is interference. Once a
valid activation receipt is durable it releases the original B reservation as
historical fact; later live-state drift makes current authority invalid but
does not recreate that released allocation.
If a missing-terminal transaction expires/reboots, it uses the separately
persisted `enable_recovery_rollback` guard/bootstrap to restore prior/genesis
false and terminalize aborted. If the committed terminal already exists but
activation cannot safely finish, recovery creates the next desired-false
safety child; it never rewrites the committed terminal. For
`desired_enabled=false`, true prior authority plus
active units must resume stopping and can never be called committed; commit is
legal only after every route unit is verified inactive and authority is
canonical false. False with any active unit or any authority/transaction ID
not explained by the unique prepared record is interference; the installer
still attempts the bounded safe stop but never fabricates committed evidence.
An expired/rebooted desired-false transaction persists a fresh
`disable_recovery_finalize/recovery_safety` guard bound to its transaction ID;
it may finish only committed or quarantined false and never re-enable. Historical
receipt missing/failure/overflow is bound by `disable_safety|recovery_safety` but
cannot prevent this false transition.
Existing identical committed terminals are idempotent; conflicting terminals
or activation receipts, or two pending transactions, are interference. Kills
after guard, bootstrap, B, prepared, the first managed drop-in, a newer
primary trigger, true authority, final-live-proof, armed marker, post-marker
live digest recheck, committed terminal before timer start,
`systemctl` start acceptance, the next route-grid boundary, unit verification,
all-units-stopped, false authority, and terminal are tested. No route process
may coexist with false/absent/malformed state. The release
checker reports malformed authority, while the primary collector preserves
legacy busy-lock behavior whenever exact true is unavailable. The service obtains the exact live allowlisted properties for its own
invocation, validates them, and passes them through the Task 3 ledger boundary;
the runner installs and binds `runtime.json` before source reads. Failure to
obtain or validate live properties records `runtime_limits_unverified` and
stops before collection.

- [ ] **Step 5: Verify GREEN**

Run the focused tests above and render into a temporary directory. Run
`systemd-analyze verify` when available. Feed captured `systemctl --user show`
fields through `verify_runtime_limits()` and assert no unresolved placeholders.
Under real CPython 3.8.10, run
`tests.test_route_shadow_runtime tests.test_route_shadow_ops tests.test_route_root_binding tests.test_route_shadow_authority tests.test_route_cost_evidence tests.test_release_smoke tests.test_run_route_shadow tests.test_route_shadow_gate tests.test_route_shadow_retention tests.test_deploy_templates tests.test_collection_cycle tests.test_collection_framework tests.test_framework`
and explicitly import `scripts.dispatch_route_shadow`,
`scripts.run_route_shadow_ops`, `scripts.route_shadow_runtime`,
`scripts.route_root_binding`, `scripts.route_shadow_authority`,
`scripts.route_cost_evidence`, `scripts.check_dashboard_release`, the runner,
gate, and retention modules. Missing 3.8 or any runtime-only typing/API failure
blocks the Task 6 commit.

- [ ] **Step 6: Commit**

```bash
git add deploy/systemd/cex-dex-route-shadow-dispatch-user.service.in deploy/systemd/cex-dex-route-shadow-canary-user@.service.in deploy/systemd/cex-dex-route-shadow-full-user@.service.in deploy/systemd/cex-dex-route-shadow-ops-user@.service.in deploy/systemd/cex-dex-route-shadow.timer deploy/systemd/cex-dex-route-shadow.env.example deploy/systemd/cex-dex-primary-shadow-scheduled.conf.in deploy/render_runtime_templates.py scripts/install_collection_timers.sh scripts/dispatch_route_shadow.py scripts/run_route_shadow.py scripts/run_route_shadow_ops.py scripts/route_shadow_gate.py scripts/route_shadow_retention.py scripts/route_shadow_runtime.py scripts/route_root_binding.py scripts/route_shadow_authority.py scripts/route_cost_evidence.py scripts/check_dashboard_release.py tests/test_deploy_templates.py tests/test_collection_cycle.py tests/test_collection_framework.py tests/test_framework.py tests/test_route_shadow_ops.py tests/test_route_root_binding.py tests/test_route_shadow_authority.py tests/test_route_cost_evidence.py tests/test_release_smoke.py tests/test_run_route_shadow.py tests/test_route_shadow_gate.py tests/test_route_shadow_retention.py tests/test_route_shadow_runtime.py
git commit -m "feat(deploy): add bounded route shadow timer"
```

Add a GitHub commit comment with schedule and rendered resource-limit evidence.

### Task 7: Integrated shadow release evidence

**Files:**
- Modify: `docs/collection-operations.md`
- Modify: `docs/market-monitor-design.md`
- Modify: `docs/route-cohort-data-contract.md`
- Modify: `docs/execution-cost-data-contract.md`
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `scripts/route_root_binding.py`
- Modify: `dashboard/opportunity_facts.py`
- Modify: `dashboard/server.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_route_root_binding.py`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_opportunity_api.py`

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
`MARKET_ROUTE_DATA_DIR` must be unset or descriptor-resolve exactly to
`<MARKET_DATA_DIR>/routes`; mismatched, symlinked, merely lexical-equivalent,
or noncanonical roots fail. This is the actual Opportunity reader override and
cannot point Web at a different `routes/latest.json` from Shadow/promotion.
Do not infer the running process from the checker's environment: start live
dashboards whose own effective environments point at a second valid routes
root or override one canonical database/TVL/depth/execution/lifecycle/registry
file while the checker environment is canonical, and require both initial and
final health binding checks to fail. Missing/invalid/null live route or market
binding fails identically. Valid fixtures prove the health digests correspond
to the same descriptor-bound roots/files used by Opportunity and Summary
responses.
Also reject `promotion_ready=true` when either immutable journal root contains
any terminal `interference` or unsafe unresolved identity/staging state; report
the stable transition ID/reason without exception text.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke -v`

Expected: FAIL on the new shadow release cases.

- [ ] **Step 3: Implement checker output and operator documentation**

Report exact run/leg/route numerators and denominators, phase, storage, ledger,
gate, publication-chain, persistent-interference, and Web route-root failures.
`promotion_ready=false` is explicit for every persistent promotion/rollback
interference even when all market-quality metrics pass, with stable reason
`publication_interference_unresolved`. Initial and final health evidence report
the matching non-path-leaking route-root binding digest used for the decision.
Document canary enable, read-only observation, full-phase
promotion, manual public promotion, rollback, timer disable, and evidence
retention commands. Validate the Web/Shadow point-file environment equivalence
before canary enable and in release checking. Never call a running canary a
public opportunity feed. Update the canonical route-cohort contract as well as
operator docs: legacy v1 remains read-only compatible, v2 collection installs
only a private candidate, and only the audited manual promotion/rollback APIs
may replace `routes/latest.json`. Remove every older instruction that says
`publish_complete_route_bundle()` or `finalize_route_opportunity_bundle()`
directly advances the public pointer.
Update the canonical execution-cost and DEX-depth contracts too: freeze the
Ethereum Router02 direct-V2 boundary, same retained core fixed block, exact
EIP-1559/native-price formula, zero-transfer-tax-only strict rule, signed
numeric submission-loss bound, operator-owned connector/trace prerequisites,
and explicit research-only chains/V3 families. Contract tests compare these
literals with implementation constants rather than accepting prose drift.

- [ ] **Step 4: Run route and full suites**

Run:

```bash
python3 -m unittest tests.test_route_shadow_inputs tests.test_route_shadow_audit tests.test_run_route_shadow tests.test_route_shadow_gate tests.test_route_shadow_retention tests.test_route_root_binding tests.test_release_smoke tests.test_dashboard tests.test_opportunity_api -v
python3 -m unittest discover -s tests -v
git diff --check
```

After all Task 7 checker/server edits, rerun the focused release suite under a
real CPython 3.8.10 runtime:
`python3.8 -m unittest tests.test_release_smoke tests.test_route_root_binding
tests.test_dashboard tests.test_opportunity_api -v`, and explicitly import
`scripts.check_dashboard_release`, `scripts.route_root_binding`,
`dashboard.opportunity_facts`, and `dashboard.server`. This final runtime check
cannot be inherited from Tasks 4-6 because their commits precede these edits;
missing Python 3.8 is a release blocker.

Expected: PASS with no warnings or leaked processes.

- [ ] **Step 5: Commit**

```bash
git add docs/collection-operations.md docs/market-monitor-design.md docs/route-cohort-data-contract.md docs/execution-cost-data-contract.md docs/dex-depth-data-contract.md scripts/check_dashboard_release.py scripts/route_root_binding.py dashboard/opportunity_facts.py dashboard/server.py tests/test_release_smoke.py tests/test_route_root_binding.py tests/test_dashboard.py tests/test_opportunity_api.py
git commit -m "docs(routes): operationalize shadow release checks"
```

Add a GitHub commit comment with focused/full-suite counts and explicit confirmation that `routes/latest.json` was not published.
