# Uniswap V3 Production Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the three causes that invalidated the first production
observation window: advancing-finalized-head rejection, single-provider RPC
outages, and silent collection-lock skips.

**Architecture:** Preserve one shared pinned block `F` and the existing exact
receipt, but validate later finalized heads as checkpoints `>= F`. Extend the
DEX RPC client with an ordered sequential endpoint pool, run-scoped breakers,
fixed-block identity validation, and redacted attempt evidence. Add bounded
shared-lock waiting for scheduled collection while preserving immediate manual
behavior and every existing fail-closed publication gate.

**Tech Stack:** Python 3.8+ standard library, Ethereum JSON-RPC, CSV/JSON,
`fcntl`, systemd user/system units, `unittest`, Node 24.

**Spec:**
`docs/superpowers/specs/2026-08-31-v3-production-resilience-design.md`

## Global Constraints

- The branch merge base is exactly
  `02b55059f0ac0592b6f1a011c2fe560c5c406bba`.
- The exact authority remains the two existing UNI/WETH and UNI/USDT markets.
- Every state, bitmap, tick, Quoter, depth, and execution fact remains pinned
  to one shared block number/hash `F`.
- The public receipt stays `uniswap_v3_exact_validation/v1` with its existing
  field set and meaning.
- A later finalized checkpoint may advance beyond `F`; a numeric retained
  header for `F` must still match the scan manifest exactly.
- RPC failover is sequential, deadline-bounded, and allowed only after the new
  endpoint proves the expected chain and exact pinned block identity.
- Provider exhaustion, invalid evidence, coverage loss, and stale output remain
  publication failures; no gate may be weakened or bypassed.
- RPC URLs, credentials, headers, URL query strings, and raw exception strings
  never enter public rows, portable manifests, or logs.
- The scalar `DEX_DEPTH_RPC_<CHAIN>` interface remains backward compatible.
- Scheduled lock wait is bounded at 900 seconds; manual/programmatic default
  behavior remains immediate with `lock_wait_seconds=0`.
- A lock-wait timeout exits 75, creates no run directory, does not move the
  latest manifest, and does not modify public facts.
- Daily remains at 00:30 UTC and depth remains hourly at minute 05.
- Python source and tests must parse under Python 3.8.
- Do not change route, Funding Rate, data-quality scoring, dashboard product
  scope, memory limits, swap, or the two-pool authority.

---

### Task 1: Correct pinned-block and advancing-finality evidence roles

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `tests/test_uniswap_v3_collection.py`
- Modify: `tests/test_uniswap_v3_exact_publication.py`
- Modify only if needed for a compatibility assertion:
  `tests/test_uniswap_v3_exact_sidecar.py`

**Interfaces:**
- Consumes: the existing exact V3 collection and raw-evidence validator.
- Produces: one retained numeric `F` header plus zero or more later finalized
  checkpoints whose numbers are at least `F`.
- Preserves: exact v1 public receipt bytes/fields and shared block identity.

- [ ] **Step 1: Add the production timing regression before code**

  Add a two-pool fixture in which both pools use pinned block `F`, pool A's
  later finalized checkpoint is `F`, and pool B's is `F+32` with a different
  valid hash. Keep every state/Quoter request at numeric `F`. Assert collection
  and exact candidate validation succeed and emit the unchanged v1 receipt.

  The expectation must use literal block numbers/hashes and a production-shaped
  RPC transcript, not a helper that calls the validator under test.

- [ ] **Step 2: Add independent fail-closed cases**

  Add tests that reject:

  - later finalized head `< F`;
  - later head at `F` with a different hash;
  - missing retained numeric-`F` header;
  - numeric-`F` header with wrong hash or timestamp; and
  - two pools whose pinned `F` identities differ.

- [ ] **Step 3: Verify genuine RED**

  ```bash
  PYTHONPATH=. python3 -m unittest \
    tests.test_uniswap_v3_collection \
    tests.test_uniswap_v3_exact_publication -v
  ```

  Expected: the advancing `F+32` case fails with the current
  `V3 fixed block number changed during collection` / raw finalized-proof
  rejection; the existing baseline cases remain green.

- [ ] **Step 4: Implement the minimum shared contract**

  Make the collector retain a numeric header for `F` inside each approved
  pool's transcript. Refactor `_retained_finalized_block` so it separately:

  - requires every retained numeric-`F` identity to equal the manifest's
    pinned number/hash/timestamp; and
  - parses later `"finalized"` identities, requires number `>= F`, and compares
    hash to `F` only at the same height.

  Apply the same rule during live collection. Do not add later-head fields to
  the public receipt.

- [ ] **Step 5: Verify GREEN and mutation boundaries**

  Run the focused modules plus `tests.test_uniswap_v3_exact_sidecar`. Mutate
  `>= F` to `== F`, remove the numeric-`F` requirement, and ignore same-height
  hash mismatch one at a time; each corresponding test must fail. Restore each
  mutation.

- [ ] **Step 6: Commit**

  ```bash
  git commit -m "fix(dex): distinguish pinned and advancing finality"
  ```

---

### Task 2: Add bounded ordered RPC endpoints and run-scoped failover

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Produces: `rpc_endpoints_for_chain(chain)` and an ordered endpoint-aware
  `RpcClient` while preserving construction with one legacy URL.
- Configuration: primary from `DEX_DEPTH_RPC_<CHAIN>` or the existing default;
  optional fallbacks from strict JSON-array
  `DEX_DEPTH_RPC_<CHAIN>_FALLBACKS`.
- Evidence: bounded attempt records with stable endpoint IDs and sanitized
  identities only.

- [ ] **Step 1: Add endpoint-configuration tests before code**

  Cover literal one-endpoint legacy output, ordered primary plus two fallbacks,
  and rejection of malformed JSON, non-list values, non-string/empty entries,
  duplicate URLs, and an excessive endpoint count. Ensure absent fallbacks do
  not change current behavior.

- [ ] **Step 2: Add transport-policy tests before code**

  Through an injected request boundary and clock/sleeper, prove:

  - HTTP 403 opens the primary immediately and uses the secondary;
  - HTTP 429 and 5xx exhaust the bounded per-endpoint retry budget before
    switching;
  - `URLError` and direct `TimeoutError` receive bounded retry/failover;
  - a later call skips an endpoint whose run-scoped breaker is open;
  - a valid JSON-RPC contract revert is surfaced without provider hopping;
  - total endpoint exhaustion raises one bounded `rpc_endpoint_exhausted`
    failure; and
  - configured credential-bearing URLs and raw exceptions do not appear in
    client records or error text.

  Assertions must target real client results/side effects, not mock call
  existence alone.

- [ ] **Step 3: Verify genuine RED**

  ```bash
  PYTHONPATH=. python3 -m unittest tests.test_fetch_dex_depth -v
  ```

  Expected: endpoint-pool parsing and provider switching are absent; the
  direct-timeout reproduction lacks the required bounded behavior.

- [ ] **Step 4: Implement the minimal endpoint-aware client**

  Add an immutable endpoint record containing stable ID, private URL, and
  sanitized identity. Parse fallbacks strictly and cap their count. Keep
  sequential requests and the collection deadline.

  Extend `http_json_rpc` to include direct timeout handling. Extend `RpcClient`
  to classify eligible transport/provider exceptions, manage one run-scoped
  breaker set, switch endpoints in order, and retain bounded attempt evidence.
  Keep JSON-RPC response validation and contract errors fail-closed.

- [ ] **Step 5: Verify GREEN and mutations**

  Run `tests.test_fetch_dex_depth`. Confirm that treating 403 as non-switching,
  revisiting an open endpoint, accepting duplicate config, or including the raw
  URL in an error causes a focused failure. Restore all mutations.

- [ ] **Step 6: Commit**

  ```bash
  git commit -m "feat(dex): add bounded RPC endpoint failover"
  ```

---

### Task 3: Bind failover to fixed-block collection and retained evidence

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `tests/test_uniswap_v3_collection.py`
- Modify: `tests/test_uniswap_v3_exact_publication.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Consumes: endpoint-aware `RpcClient` from Task 2 and corrected finality roles
  from Task 1.
- Produces: safe per-chain failover in real DEX collection, fixed-block
  provider validation, pool restart, and transcript-bound attempt evidence.
- Preserves: the current public CSV and receipt schemas.

- [ ] **Step 1: Add fixed-block failover integration tests first**

  Add production-shaped fixtures for:

  - Ethereum primary HTTP 403 followed by a secondary that proves chain ID and
    the exact `F` number/hash/timestamp, then completes both authority pools;
  - BSC primary timeouts followed by a valid secondary, with later BSC pools
    never calling the open primary;
  - failover during a pool, requiring that pool's evidence calculation to
    restart while retaining the failed-attempt ledger; and
  - secondary wrong chain ID, missing block, different block hash, or different
    timestamp, each rejected before its result enters a fact.

  Verify every successful state/Quoter request remains tagged with numeric
  `F`, and the final transcript SHA includes the redacted attempts.

- [ ] **Step 2: Add exhaustion/publication regressions first**

  Exhaust all endpoints for the two approved Ethereum pools and separately for
  the five observed BSC markets. Assert failed rows contain bounded reason
  codes, the exact or aggregate gate rejects, and pre-existing public bundle
  bytes remain unchanged.

- [ ] **Step 3: Verify genuine RED**

  ```bash
  PYTHONPATH=. python3 -m unittest \
    tests.test_uniswap_v3_collection \
    tests.test_uniswap_v3_exact_publication \
    tests.test_fetch_dex_depth -v
  ```

- [ ] **Step 4: Integrate endpoint pools into collection**

  Build one endpoint-aware client per chain. Once `F` is selected, bind its
  chain ID and exact header identity to the client. Before a fallback serves a
  fixed-block call, require it to prove that identity. On a validated provider
  switch during pool collection, restart the affected pool evidence boundary
  using the active fallback while preserving the prior bounded attempt ledger.

  Write the attempt ledger into the retained raw transcript before hashing.
  Map terminal provider exhaustion to a bounded reason code without exposing
  exception or URL text. Do not weaken any publication classifier.

- [ ] **Step 5: Verify GREEN, privacy, and lineage**

  Run all three modules plus `tests.test_uniswap_v3_exact_sidecar` and
  `tests.test_execution_cost`. Scan generated fixtures for configured secret
  markers and full RPC URLs. Mutate fallback block hash validation, pool
  restart, and attempt-ledger hashing; each must make a test fail.

- [ ] **Step 6: Commit**

  ```bash
  git commit -m "fix(dex): fail over fixed-block collection safely"
  ```

---

### Task 4: Make scheduled lock contention bounded and observable

**Files:**
- Modify: `scripts/run_collection_cycle.py`
- Modify: `tests/test_collection_cycle.py`
- Modify: `deploy/systemd/cex-dex-depth.service.in`
- Modify: `deploy/systemd/cex-dex-depth-user.service.in`
- Modify: `deploy/systemd/cex-dex-daily.service.in`
- Modify: `deploy/systemd/cex-dex-daily-user.service.in`
- Modify: `tests/test_framework.py`

**Interfaces:**
- Adds: `lock_wait_seconds: float = 0` to `run_collection_cycle` and
  `--lock-wait-seconds` to the CLI.
- Scheduled templates pass exactly 900 seconds.
- Timeout result: `status=skipped_locked`,
  `reason=lock_wait_timeout`, CLI exit 75.

- [ ] **Step 1: Add real lock-wait behavior tests before code**

  Use a separate process or descriptor boundary that exercises real `flock`.
  Prove a collector starts only after the holder releases the lock and then
  succeeds. Prove a zero-wait manual call retains the current immediate
  `skipped_locked` behavior.

- [ ] **Step 2: Add timeout and CLI tests before code**

  With injected monotonic clock/sleep or a short real timeout, prove timeout:

  - runs no collection step;
  - creates no run directory;
  - does not create or replace the latest manifest;
  - returns the exact structured reason; and
  - makes the CLI exit 75 rather than zero.

- [ ] **Step 3: Add service-template behavior tests before edits**

  Render both system and user services and assert:

  - daily and depth pass `--lock-wait-seconds 900`;
  - user services load
    `EnvironmentFile=-%h/.config/cex-dex/dashboard.env`;
  - system services keep `/etc/cex-dex/dashboard.env`;
  - depth timeout is 50 minutes and daily timeout is 90 minutes; and
  - timer calendar expressions remain unchanged.

- [ ] **Step 4: Verify genuine RED**

  ```bash
  PYTHONPATH=. python3 -m unittest \
    tests.test_collection_cycle tests.test_framework -v
  ```

- [ ] **Step 5: Implement bounded monotonic lock waiting**

  Poll the existing non-blocking flock until acquired or the monotonic deadline
  expires. Hold it over the same full critical section as today. Create the run
  directory only after acquisition. Keep zero-wait behavior compatible.

  Map only timeout-style `skipped_locked` to CLI exit 75. Update all four
  service templates as specified without changing daily/depth timer schedules.

- [ ] **Step 6: Verify GREEN and mutations**

  Run the two modules. Mutate the timeout exit to zero, create the run directory
  before acquisition, and omit the user environment file one at a time; each
  must fail a focused test. Restore all mutations.

- [ ] **Step 7: Commit**

  ```bash
  git commit -m "fix(collection): make lock contention observable"
  ```

---

### Task 5: Document configuration, rollout, and free-provider limits

**Files:**
- Modify: `.env.example`
- Modify: `deploy/dashboard.env.example`
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `docs/collection-operations.md`
- Modify: `docs/production-hardening.md`
- Modify only when existing documentation tests require it:
  `tests/test_framework.py`

**Interfaces:**
- Documents strict JSON-array fallback syntax and redacted evidence.
- Documents free-only best-effort status, service environment loading, lock
  waiting, exit 75, unchanged schedules, staging, and rollback.

- [ ] **Step 1: Update operator-facing configuration examples**

  Add primary/fallback placeholders for Ethereum and BSC. State that production
  values belong only in the mode-0600 environment file and that fallback JSON
  must be valid on one line for systemd. Do not add live URLs or credentials to
  tracked files.

- [ ] **Step 2: Update data and operations contracts**

  Explain pinned `F` versus later finalized head, failover eligibility,
  fixed-block provider verification, endpoint-exhaustion semantics, transcript
  privacy, 15-minute scheduled lock wait, exit 75, and free-provider limits.

- [ ] **Step 3: Add the deployment checklist**

  Require endpoint capability checks, private environment-file checksum,
  paused timers, staged non-publishing collection, forced primary-failure
  rehearsal, backups, normal release verification, and a new 26-hour window.
  Preserve all existing rollback and evidence-retention requirements.

- [ ] **Step 4: Verify docs and commit**

  ```bash
  git diff --check
  PYTHONPATH=. python3 -m unittest tests.test_framework -v
  git commit -m "docs: document resilient DEX collection"
  ```

---

### Task 6: Whole-branch verification and release readiness

**Files:**
- No planned production-file additions.
- Append task/review evidence only to the plan-owned ignored SDD workspace.

- [ ] **Step 1: Verify ancestry and scope**

  Require merge base exactly `02b55059`, a clean worktree, no authority/public
  receipt schema change, and no route/Funding/data-quality/memory-limit edits.

- [ ] **Step 2: Run focused regression**

  ```bash
  PYTHONPATH=.:tests python3 -m unittest \
    tests.test_uniswap_v3_math \
    tests.test_uniswap_v3_authority \
    tests.test_uniswap_v3_collection \
    tests.test_uniswap_v3_exact_publication \
    tests.test_uniswap_v3_exact_sidecar \
    tests.test_fetch_dex_depth \
    tests.test_collection_cycle \
    tests.test_framework -v
  ```

- [ ] **Step 3: Run the complete suite with Node**

  Put the bundled Node 24 runtime on `PATH`, set its package directory through
  `NODE_PATH`, and run:

  ```bash
  PYTHONPATH=. python3 -m unittest discover -s tests -p 'test_*.py' -q
  ```

- [ ] **Step 4: Run static and compatibility checks**

  - `python3 -m compileall -q dashboard scripts tests deploy`
  - parse every changed Python file with `ast.parse(..., feature_version=(3,8))`
  - `git diff --check 02b55059..HEAD`
  - scan changed files and generated evidence for credentials, full private RPC
    URLs, local checkout paths, and production environment contents.

- [ ] **Step 5: Independent whole-branch review**

  Require no open Critical or Important findings. Any fix receives a separate
  focused regression and scoped re-review.

## Post-merge production procedure

This procedure is executed only after branch review and GitHub checks pass.

1. Read-only capability-test two independently operated free endpoints for
   Ethereum and BSC. Each pair must agree on chain ID and one recent finalized
   block number/hash/timestamp and support the fixed-block calls used by the
   current inventory.
2. Write only the private production environment file with scalar primaries
   and JSON fallback arrays; record its checksum without displaying contents.
3. Pause daily/depth timers, prove services inactive and the shared lock free,
   and back up code SHA, unit bytes/state, environment checksum, five public
   files, trusted receipt, and raw evidence.
4. Run the existing staged no-publish launch path. Force each primary endpoint
   to fail in staging and require the secondary to produce an independently
   validated complete candidate.
5. Deploy the reviewed code and rendered service units together. Run exact
   health, full release, API, asset, and public-browser checks before restoring
   timer state.
6. Define the new observation start as the first successful scheduled exact
   run after deployment. Observe at least 26 hours, including one daily cycle
   and multiple hourly cycles. Any hard failure invalidates the window and is
   reported without automatic rollback or pool expansion.
