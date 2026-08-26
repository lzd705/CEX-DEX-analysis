# Uniswap V3 Two-Pool Production Launch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development to implement this plan task by task.

**Goal:** Publish exact, auditable pool-only execution facts for the two
authority-approved Ethereum UNI Uniswap V3 pools without weakening existing
quality gates or silently replacing prior good data.

**Architecture:** Promote the proven canary validators into the normal
publication boundary, add an identity-level two-pool gate alongside aggregate
coverage checks, expose the exact scope in the public contract and health
surface, then deploy and publish through a backed-up staged production rollout.

**Tech Stack:** Python 3.8+ standard library, existing JSON-RPC collectors,
`unittest`, GitHub Actions, Docker/systemd production runtime.

**Spec:**
`docs/superpowers/specs/2026-08-27-uniswap-v3-two-pool-production-launch-design.md`

### Task 1: Correct the public contract and prove API publication

**Files:**
- Modify: `dashboard/server.py`
- Modify: `dashboard/README.md`
- Modify: `docs/collection-operations.md`
- Modify: relevant dashboard/API tests

- [x] Write a failing API test requiring exact V3 scope metadata for the two
  authority markets while retaining the exclusions.
- [x] Write a failing end-to-end fixture for an unsupported V3 baseline merged
  with two exact candidate markets and returned as observed by the API.
- [x] Implement the smallest metadata/publication changes and make both tests
  green.

### Task 2: Add the exact-scope production publication gate

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/run_collection_cycle.py`
- Modify/create: focused V3 publication tests

- [x] Write failing tests for missing/mismatched production inventory,
  incomplete or duplicate scenarios, partial/unsupported rows, mixed block
  hashes, non-exact Quoter parity, and missing/tampered transcript, scan
  manifest, TVL manifest, or GeckoTerminal raw response.
- [x] Implement one validator requiring 2/2 observed depth, the unique 2 x 5
  observed execution grid, one finalized block identity, exact raw parity, and
  a deterministic `uniswap_v3_exact_validation/v1` receipt.
- [x] Add an explicit full-inventory production/no-publish gate flag and invoke
  the validator before any processed or public file changes; small fixture
  collections remain unchanged.
- [x] Reject bounded merge-publication for either authority market while
  leaving unapproved V3 markets structurally unsupported.
- [x] Prove a rejected candidate does not replace prior published facts.

### Task 3: Promote raw-evidence validation and expose exact health

**Files:**
- Modify: V3 canary/collector validation helpers
- Modify: production collection runner and health/release surfaces
- Modify/create: focused evidence and health tests

- [x] Replace the canary's duplicate validation logic with the Task 2 shared
  validator and prove canary/production receipt parity.
- [x] Atomically publish `uniswap_v3_exact_latest.json` with depth and
  execution facts; the receipt binds scoped-row, transcript, manifest, USD
  source, authority, and shared-block hashes without exposing local paths.
- [x] Add an `uniswap_v3_exact` health/release result that fails closed when
  either authority market or any required scenario is absent or stale.
- [x] Document bounded evidence retention and protect the current and rollback
  generations from cleanup.

### Task 4: Verify and publish the code branch

- [ ] Run focused tests after each task, then the full unittest suite, Python
  3.8 grammar checks, compile checks, and whitespace validation.
- [ ] Complete independent code and data-quality reviews.
- [ ] Commit focused changes, push the feature branch, open a PR against
  `codex/quality-ci-observed-integration`, and require all GitHub checks.
- [ ] Merge only the reviewed SHA and record the resulting immutable SHA.

### Task 5: Backed-up staged production launch

- [ ] Diagnose and restore the stale CEX depth/execution cycle so the overall
  release checker is current.
- [ ] Record production code/data identities and create a checksummed,
  versioned backup plus rollback command.
- [ ] Deploy the reviewed immutable SHA without changing publication data.
- [ ] Run a production-inventory, no-publish candidate and require every launch
  gate plus retained evidence to pass.
- [ ] Run a UNI/WETH-only no-publish rehearsal, then atomically publish the
  validated combined two-pool scope and validate API/health/release/browser.
- [ ] Confirm the next scheduled depth cycle keeps 2/2 depth and 20/20 exact
  execution; otherwise execute rollback and preserve failure evidence.

## Completion evidence

Completion requires four separate identities: local commit SHA, remote/merged
GitHub SHA with passing checks, deployed application SHA, and published data
generation/SHA. Code completion or a non-publishing canary alone is not a
production launch.
