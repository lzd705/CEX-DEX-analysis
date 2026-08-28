# Uniswap V3 Minimal Production Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the reviewed two-pool exact Uniswap V3 feature directly onto
the live `fe735ef` application baseline and add a production-safe staging,
bootstrap, and checksummed rollback workflow.

**Architecture:** Keep the live application interfaces and selectively port
only V3 math, collection, evidence, publication, API, health, and release
functions from `401eaad`. A separate operator launch tool stages a complete
candidate outside production, verifies it through the unchanged release
checker, and uses compare-and-swap receipts for promotion and rollback.

**Tech Stack:** Python 3.8+ standard library, JSON-RPC, CSV/JSON, `unittest`,
GitHub Actions, Node 24, systemd user units.

**Spec:**
`docs/superpowers/specs/2026-08-28-uniswap-v3-minimal-production-deploy-design.md`

## Global Constraints

- The branch merge base is exactly
  `fe735ef821b7b4d806012acf996d1e8edc80320a`.
- Exact execution is enabled only for the two authority market IDs in
  `config/uniswap_v3_execution_markets.json`; every other V3 market remains
  unsupported.
- Pool calls, tick evidence, depth, execution, and Quoter parity share one
  finalized Ethereum block number and block hash.
- Calculations use Uniswap V3 integer rounding and token base units; `Decimal`
  is only for presentation and USD conversion.
- Missing, unsupported, partial, failed, and stale values remain null/blank,
  never zero-filled or inferred.
- The existing aggregate coverage, freshness, receipt, health, and release
  checks remain fail-closed and are not weakened for bootstrap.
- Do not import route-shadow, observed-quality, static-delivery, summary-
  warmup, or route collection-lock production code from intermediate commits.
- Python source and tests must parse under Python 3.8.
- State-changing launch phases require `--execute`; their default behavior is
  read-only planning.
- No production deployment, Git pointer switch, privilege escalation, or
  secret-bearing environment content is performed by repository tests.

---

### Task 1: Make the live baseline reproducible in CI

**Files:**
- Create: `.github/workflows/quality-gates.yml`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_compare_chart_frontend.py`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_event_frontend.py`
- Modify: `tests/test_navigation.py`
- Modify: `tests/test_opportunity_frontend.py`
- Modify: `tests/test_public_quality_overlay.py`

**Interfaces:**
- Consumes: the untouched `fe735ef` production tree.
- Produces: deterministic Python 3.8/3.14 + Node 24 push/PR gates without
  importing the intermediate quality or static-delivery production changes.

- [ ] **Step 1: Preserve the observed baseline failure**

  Run:

  ```bash
  PYTHONPATH=. python3 -m unittest discover -s tests -p 'test_*.py' -q
  ```

  Expected baseline evidence: 1,485 tests run; the only error is
  `test_summary_producer_satisfies_structured_na_release_contract` with
  `Summary CEX lifecycle evidence is stale`; 102 Node-backed tests are skipped
  when Node is absent.

- [ ] **Step 2: Fix only the fixture clock and verify GREEN**

  In the failing test, compute the freshness cache bucket from the literal
  `checked_at` timestamp and patch `server.api_freshness_bucket` to that value
  while the summary is built. Do not change production freshness logic or the
  tracked lifecycle evidence.

  Run the one test, then the full suite. Expected: the lifecycle contract test
  passes and runtime freshness remains unchanged.

- [ ] **Step 3: Port the test-harness Node stdin fixes**

  Reproduce the test-only behavior from commits `cbe4575` and `07c4c70`:
  every Node helper uses `node -` with `input=<script>` and UTF-8 text instead
  of putting large JavaScript in argv; navigation paths derive from the
  current checkout, never a machine-local `/private/tmp` checkout path.

  With the bundled Node binary on `PATH`, run the seven affected frontend
  modules and require all cases to execute rather than skip.

- [ ] **Step 4: Add the existing minimal workflow**

  The workflow triggers on `push` and `pull_request`, has `contents: read`,
  runs `ubuntu-22.04`, Python `3.8` and `3.14`, Node `24`, `npm ci` in
  `dashboard`, compile/import checks, and full unittest discovery. Do not add
  route or observed-quality jobs.

- [ ] **Step 5: Commit**

  ```bash
  git commit -m "ci: verify live baseline portability"
  ```

### Task 2: Port exact math and the two-market authority

**Files:**
- Create: `config/uniswap_v3_execution_markets.json`
- Create: `scripts/uniswap_v3_math.py`
- Create: `tests/test_uniswap_v3_math.py`
- Create: `tests/test_uniswap_v3_authority.py`
- Modify: `scripts/fetch_dex_depth.py`

**Interfaces:**
- Produces: `load_uniswap_v3_execution_authority`,
  `match_uniswap_v3_execution_authority`, exact TickMath/SqrtPriceMath/SwapMath
  helpers, and `simulate_swap`.
- Preserves: the `fe735ef` two-value DEX observation return interface.

- [ ] **Step 1: Add the reviewed tests before production code**

  Add the exact literal vector and authority behavior from the same-named test
  files at `401eaad`. The tests must independently assert Uniswap Core tick
  boundaries, overflow fallback, exact input/output steps, tick crossing,
  scan-bound partial results, exact two authority records, and fail-closed
  identity mismatch.

- [ ] **Step 2: Verify RED**

  ```bash
  python3 -m unittest tests.test_uniswap_v3_math tests.test_uniswap_v3_authority -v
  ```

  Expected: imports/functions are missing on `fe735ef`.

- [ ] **Step 3: Port the minimum production implementation**

  Add the reviewed authority JSON and the dependency-free math module. Add
  authority load/match helpers to `fetch_dex_depth.py`. Use a V3-local
  `_V3_BLOCK_HASH` regex; do not import `_ROUTE_BLOCK_HASH`, route fee identity,
  typed route payloads, or route source sinks.

- [ ] **Step 4: Verify GREEN and mutation boundaries**

  Run the two modules. Confirm that changing either approved pool identity,
  one official integer vector, tick-crossing sign, or exact-output rounding
  causes a test failure.

- [ ] **Step 5: Commit**

  ```bash
  git commit -m "feat(dex): port exact V3 math and authority"
  ```

### Task 3: Port fixed-block collection and retained parity evidence

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Create: `tests/test_uniswap_v3_collection.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Consumes: authority and `simulate_swap` from Task 2.
- Produces: exact V3 depth rows, ten execution rows per approved market, one
  retained scan manifest per pool, and QuoterV2 request/response parity.

- [ ] **Step 1: Add collector tests first**

  Add the reviewed fake-RPC integration test from `401eaad`, preserving its
  independent frozen Quoter responses. Require chain ID, factory and
  `factory.getPool` identity, finalized header start/end/final checks, numeric
  fixed block tags, bitmap word/tick evidence, complete/partial outcomes,
  four-word Quoter decoding, and transcript SHA lineage.

- [ ] **Step 2: Verify RED**

  ```bash
  python3 -m unittest tests.test_uniswap_v3_collection -v
  ```

  Expected: the `fe735ef` collector still publishes V3 execution as
  `unsupported` and lacks exact scan evidence.

- [ ] **Step 3: Port collector functions onto the live interface**

  Add finalized block identity, exact calldata, bitmap/tick scanning, exact
  band depth, fixed-notional execution, Gecko USD lineage, scan manifests, and
  strict Quoter decoding. Keep `observed_pool_row` and
  `collect_dex_pool_observation` compatible with `fe735ef`; do not add
  `reserve_timestamp_last_raw`, `fixed_chain_id`, route headers, or typed route
  payload sinks.

- [ ] **Step 4: Verify GREEN and adjacent regressions**

  ```bash
  python3 -m unittest tests.test_uniswap_v3_math tests.test_uniswap_v3_authority tests.test_uniswap_v3_collection tests.test_fetch_dex_depth -v
  ```

- [ ] **Step 5: Commit**

  ```bash
  git commit -m "feat(dex): collect exact V3 depth and execution evidence"
  ```

### Task 4: Port the exact publication, API, health, and release boundary

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/run_collection_cycle.py`
- Create: `scripts/run_uniswap_v3_canary.py`
- Modify: `dashboard/server.py`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `dashboard/PUBLIC_SHARING.md`
- Modify: `dashboard/README.md`
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `docs/execution-cost-data-contract.md`
- Create: `tests/test_uniswap_v3_exact_publication.py`
- Create: `tests/test_uniswap_v3_exact_sidecar.py`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_release_smoke.py`

**Interfaces:**
- Produces: `validate_uniswap_v3_exact_candidate`, canonical receipt bytes,
  five-file atomic publication, API scope metadata, exact health, and hard
  release validation.
- Consumes: raw evidence generated by Task 3.

- [ ] **Step 1: Add publication and sidecar tests first**

  Add the reviewed adversarial tests from `401eaad`. They must reject missing
  authority inventory, duplicate/missing scenarios, partial/unsupported rows,
  mixed block hashes, changed calldata or fee/amount/price-limit, malformed
  Quoter words, transcript/manifest/Gecko tampering, root escape/symlinks,
  wrong snapshot IDs, publication drift, missing/stale/tampered public receipt,
  and any non-atomic five-file replacement.

- [ ] **Step 2: Verify RED**

  ```bash
  PYTHONPATH=tests:. python3 -m unittest discover -s tests -p 'test_uniswap_v3_exact_*.py' -v
  ```

  Expected: exact validator, receipt, sidecar health, and release functions are
  missing.

- [ ] **Step 3: Port the raw validator and publication gate**

  Port only V3 evidence helpers and the full-inventory publication changes.
  Require aggregate coverage before any processed/public write. Publish these
  five destinations through one `atomic_replace_bundle` call:

  ```python
  (
      "dex_depth_history.csv",
      "dex_depth_latest.csv",
      "dex_depth_snapshot.csv",
      "dex_execution_cost_latest.csv",
      "uniswap_v3_exact_latest.json",
  )
  ```

  Authority pools remain forbidden from bounded merge-publication.

  Add the small non-publishing canary entrypoint only after the shared
  production validator exists. It must delegate to that validator and reject
  anything other than the exact two-market 2-by-5 result.

- [ ] **Step 4: Port collection-runner wiring**

  Add `--require-uniswap-v3-exact-validation` only to full unfiltered DEX
  depth collection. On `fe735ef`, retain the existing file-lock path; do not
  import `collection_lock_evidence.py` or the route primary-intent lock.

- [ ] **Step 5: Port public API, health, and release checks**

  Approved rows expose exact pool-only scope; unapproved V3 numeric rows are
  cleared and treated as unsupported. `/health.uniswap_v3_exact` rereads the
  staged/public rows and receipt. Missing/invalid/stale exact health downgrades
  `data_status`, and `validate_release_health` requires exact `current`, 2/2,
  20/20, hashes, authority IDs, and shared block identity.

- [ ] **Step 6: Verify GREEN**

  ```bash
  PYTHONPATH=tests:. python3 -m unittest \
    tests.test_uniswap_v3_exact_publication \
    tests.test_uniswap_v3_exact_sidecar \
    tests.test_collection_cycle \
    tests.test_dashboard \
    tests.test_release_smoke -v
  ```

- [ ] **Step 7: Commit**

  ```bash
  git commit -m "feat(dex): publish exact V3 evidence on live base"
  ```

### Task 5: Add checksummed staging, bootstrap, promotion, and rollback

**Files:**
- Create: `scripts/uniswap_v3_launch.py`
- Create: `tests/test_uniswap_v3_launch.py`
- Modify: `docs/production-hardening.md`
- Modify: `docs/collection-operations.md`

**Interfaces:**
- Produces CLI phases: `preflight`, `pause`, `backup`, `stage`,
  `verify-stage`, `promote`, `restore`, and `resume`.
- Consumes: Task 4's production validator, sidecar validator, collection
  runner, atomic publication helper, dashboard, and release checker.

- [ ] **Step 1: Write path, backup, and restore tests first**

  Name the production breaks they catch. Use real temporary files to require:
  regular non-symlink roots; fixed relative bundle names; 0700 launch roots;
  canonical SHA-256 manifests; explicit absent-sidecar state; byte/mode
  preservation; refusal after baseline or promoted-generation drift; and
  failure injection that restores every pre-call byte.

- [ ] **Step 2: Verify RED**

  ```bash
  python3 -m unittest tests.test_uniswap_v3_launch -v
  ```

  Expected: `scripts.uniswap_v3_launch` is missing.

- [ ] **Step 3: Implement pure filesystem and receipt primitives**

  Provide these exact callable contracts:

  - `snapshot_public_bundle(data_dir: Path) -> dict[str, Any]`
  - `create_backup(data_dir: Path, launch_dir: Path, *, target_sha: str,
    previous_app_sha: str) -> dict[str, Any]`
  - `verify_bundle_state(data_dir: Path, manifest: Mapping[str, Any], *,
    state: str) -> None`
  - `prepare_stage_inputs(data_dir: Path, stage_dir: Path, baseline:
    Mapping[str, Any]) -> dict[str, Any]`
  - `promote_stage(data_dir: Path, stage_dir: Path, baseline: Mapping[str,
    Any]) -> dict[str, Any]`
  - `restore_backup(data_dir: Path, backup_dir: Path, promotion: Mapping[str,
    Any]) -> dict[str, Any]`

  All reads are bounded, descriptor-checked regular files. Promotion is CAS
  against baseline hashes. Restore is CAS against promoted hashes and returns
  an initially absent sidecar to absence without leaving a partial rollback.

- [ ] **Step 4: Write timer and staging orchestration tests first**

  Use a deterministic command-runner boundary and assert resulting receipts,
  not mock call existence. Require fixed daily/depth unit names, exact prior
  state restoration, inactive services and unlocked collection before copy,
  a fresh sibling stage root, a full unfiltered publishing command directed at
  stage, target-dashboard loopback overrides, and the normal release checker.
  Default/no-`--execute` mode must create no directory, run no timer command,
  collect no network data, and publish nothing.

- [ ] **Step 5: Implement CLI orchestration**

  State-changing phases require `--execute`. Each phase consumes the previous
  canonical receipt and writes exactly one next receipt. Refuse missing,
  reordered, repeated, or drifted phases. Do not switch Git revisions or edit
  service/environment files. `verify-stage` runs a transient loopback target
  dashboard with live facts plus staged DEX depth/execution/receipt overrides
  and invokes the unchanged release checker with the target SHA.

- [ ] **Step 6: Verify GREEN and document the exact cutover**

  Run the launch tests plus atomic publication, collection runner, dashboard,
  and release tests. Document the brief dashboard stop between successful
  stage verification and five-file promotion, the external application-pointer
  switch, forward validation, rollback ordering, first-sidecar absence, and
  retention hold.

- [ ] **Step 7: Commit**

  ```bash
  git commit -m "feat(deploy): add staged V3 launch and rollback"
  ```

### Task 6: Whole-branch verification and scope audit

**Files:**
- Modify only files required to fix findings discovered by verification.

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: an independently reviewed, minimal live-derived branch. It does
  not produce a production deployment.

- [ ] **Step 1: Run focused and full tests**

  ```bash
  PYTHONPATH=tests:. python3 -m unittest discover -s tests -p 'test_*.py' -q
  python3 -m compileall -q dashboard deploy scripts tests
  git diff --check
  ```

- [ ] **Step 2: Verify Python 3.8 grammar**

  Parse every changed Python file using `ast.parse(source, filename,
  feature_version=(3, 8))`. Reject parenthesized multi-context `with`, new
  union syntax, and any
  other post-3.8 grammar.

- [ ] **Step 3: Audit scope and privacy**

  Require:

  ```bash
  git merge-base fe735ef821b7b4d806012acf996d1e8edc80320a HEAD
  git diff --name-only fe735ef821b7b4d806012acf996d1e8edc80320a..HEAD
  ```

  The merge base must equal the live SHA. Inspect every changed path and reject
  route-shadow, observed-quality, route-cost, static-delivery production, raw
  runtime data, credentials, absolute production paths, or RPC URLs in public
  receipts.

- [ ] **Step 4: Independent final review**

  Review exact math parity, data-lineage fail-closed behavior, five-file
  atomicity, launch CAS semantics, first-sidecar rollback, timer-state safety,
  Python 3.8 compatibility, and whether tests can self-validate through shared
  helpers. Fix every Critical/Important finding and rerun its covering tests.

- [ ] **Step 5: Record non-deployment blockers**

  A real production stage/release rehearsal, GitHub checks, and the historical
  SIGKILL/OOM diagnosis require remote/server evidence. Report them separately;
  do not label local code completion as live deployment.

- [ ] **Step 6: Commit review fixes when the review changed files**

  ```bash
  git commit -m "fix(deploy): close minimal V3 release review"
  ```

  If the review is clean and no files changed, record that fact in the SDD
  ledger instead of creating an empty commit.
