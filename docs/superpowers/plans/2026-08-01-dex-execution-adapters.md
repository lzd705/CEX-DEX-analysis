# DEX Execution Adapter Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand strict DEX depth/execution coverage in measured production-value order using protocol-exact integer/state adapters and fail-closed capability gates.

**Architecture:** `fetch_dex_depth.py` remains the sole collector and publisher. Focused modules under `scripts/dex_adapters/` read one fixed EVM block or Solana slot and return raw-unit observations through a shared interface. The existing normalizer creates public rows; adapters never write or publish files directly.

**Tech Stack:** Python 3.8-compatible standard library, EVM JSON-RPC/ABI, Solana JSON-RPC/account data, integer fixed-point math, CSV fixtures with raw hashes, `unittest`.

## Global Constraints

- Strict execution accepts protocol integer math or an exact same-state Quoter result only.
- Decimal continuous approximations, TVL, and 24-hour volume cannot become strict execution/depth.
- Every measured row retains one fixed EVM block or consistent Solana context slot and raw evidence hash.
- Declared strict capability returning unsupported is a failed adapter, not a capability absence.
- Partial remains partial; scan-limit output cannot be extrapolated into a full quote.
- V4 Pool ID is not an EVM contract address; Solana slot is not an EVM block.
- Pool fee embedded in quote output is not double-counted later.
- A fixed-USD quote alone is not route eligibility. Every route-capable adapter
  implements the shared exact `quote_base_quantity()` contract and passes its
  common-net-quantity conformance suite.
- Funding Rate and Upbit mutation are excluded.

## Current Release Boundary (2026-08-02)

This branch executes Tasks 1-3 only, followed by the V3-specific portions of
Task 9. Tasks 4-8 remain future plans and must not widen this release to V4,
Balancer, or Solana. V3 publication below means the existing
`dex_depth_latest.csv` / `dex_execution_cost_latest.csv` fact snapshot boundary;
it never means the public route pointer `routes/latest.json`.

Production priority snapshot `20260801T082050Z-cde2a0cf`:

- Batch 0: 65 V3 markets with depth but 650 unsupported execution scenarios.
- Batch 1: 30 Uniswap V4 markets, about $50.58m TVL and $5.72m 24h volume.
- Batch 2: 2 Balancer markets, about $11.67m TVL.
- Batch 3: 18 Orca/Meteora/Raydium markets, about $11.38m TVL and $1.94m 24h volume.

---

### Task 1: Adapter types, registry, and capability gates

**Files:**
- Create: `scripts/dex_adapters/__init__.py`
- Create: `scripts/dex_adapters/types.py`
- Create: `scripts/dex_adapters/registry.py`
- Modify: `scripts/fetch_dex_depth.py`
- Create: `tests/test_dex_adapter_registry.py`
- Modify: `tests/test_route_quantity.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Produces: `AdapterCapabilities`, `StateSequence`, `StrictSwapQuote`,
  `AdapterObservation`, and `adapter_for_pool()`.
- Adds separate `strict_route_quantity` capability. It becomes true only when
  the adapter implements `quote_base_quantity(direction, target_base_raw,
  market_state)` from `scripts/route_quantity.py`.

- [ ] **Step 1: Write failing classification-equivalence tests**

Given every current V2, V3, and unsupported fixture, assert registry
classification exactly matches current behavior before enabling new
capabilities. Check chain, normalized DEX ID, and pool identity type.

- [ ] **Step 2: Write failing capability-regression tests**

```python
capabilities = AdapterCapabilities(
    adapter_id="uniswap_v3@1",
    protocol_model="concentrated_liquidity_v3",
    state_sequence_type="evm_block",
    strict_depth=True,
    strict_execution=True,
)
self.assertEqual(
    normalize_adapter_status(capabilities, "execution", "unsupported"),
    "failed",
)
```

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_dex_adapter_registry tests.test_fetch_dex_depth -v`

Expected: FAIL because adapter types/registry are absent.

- [ ] **Step 4: Implement registry without enabling new behavior**

Move protocol classification behind the registry. Keep current V2 observed,
V3 depth-only, and unsupported outputs byte-equivalent. Update depth/execution
coverage normalization so a declared capability cannot silently fall back to
unsupported.

Route-quantity capability remains false for every adapter in this registry
refactor. Enabling fixed-notional execution never automatically enables route
opportunities.

- [ ] **Step 5: Run registry/full-collector tests**

Run: `python3 -m unittest tests.test_dex_adapter_registry tests.test_fetch_dex_depth -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/dex_adapters scripts/fetch_dex_depth.py tests/test_dex_adapter_registry.py tests/test_fetch_dex_depth.py
git commit -m "refactor(dex): establish strict adapter registry"
```

Add a GitHub commit comment with byte-equivalence and capability-gate evidence.

### Task 2: Uniswap V3 integer SwapMath

**Files:**
- Create: `scripts/dex_adapters/uniswap_v3.py`
- Create: `tests/test_uniswap_v3_execution_adapter.py`
- Create: `tests/fixtures/dex_adapters/uniswap_v3/no_tick_cross/manifest.json`
- Create: `tests/fixtures/dex_adapters/uniswap_v3/one_tick_cross/manifest.json`
- Create: `tests/fixtures/dex_adapters/uniswap_v3/multi_tick_cross/manifest.json`
- Create: `tests/fixtures/dex_adapters/uniswap_v3/insufficient_scan/manifest.json`

**Interfaces:**
- Produces: `get_sqrt_ratio_at_tick()`, `mul_div()`, `mul_div_rounding_up()`, `compute_swap_step()`, `quote_exact_input()`, and `quote_exact_output()`.

- [ ] **Step 1: Write failing reference-vector tests**

Use fixed raw protocol vectors for Q64.96, exact-input/exact-output rounding,
fee pips, price boundaries, one/multiple tick crossing, and liquidity-net
direction. Assert exact integer equality, not percentage tolerance.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_uniswap_v3_execution_adapter -v`

Expected: FAIL because integer SwapMath is absent.

- [ ] **Step 3: Implement protocol integer operations**

Mirror protocol rounding rules and bounds. Return `StrictSwapQuote` raw units,
status, steps, ending price, and stable reason. Do not call the existing Decimal
depth approximation from strict execution functions.

- [ ] **Step 4: Add bounded expanding tick scan**

Load bitmap words in swap direction until target complete, protocol boundary,
liquidity exhaustion, or configured guard. Guard exhaustion returns
`partial/source_tick_scan_limit`; no last-tick extrapolation is allowed.

- [ ] **Step 5: Run V3 adapter tests**

Run: `python3 -m unittest tests.test_uniswap_v3_execution_adapter -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/dex_adapters/uniswap_v3.py tests/test_uniswap_v3_execution_adapter.py tests/fixtures/dex_adapters/uniswap_v3
git commit -m "feat(dex): implement exact Uniswap V3 SwapMath"
```

Add a GitHub commit comment with exact-vector hashes and rounding coverage.

### Task 3: V3 fixed-block Quoter parity and publication integration

**Files:**
- Create: `config/dex_protocol_deployments.csv`
- Create: `scripts/check_dex_adapter_parity.py`
- Create: `tests/test_dex_adapter_parity.py`
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/execution_cost.py`
- Modify: `tests/test_fetch_dex_depth.py`
- Modify: `tests/test_execution_cost.py`
- Modify: `docs/execution-cost-data-contract.md`

**Interfaces:**
- Consumes: Task 2 SwapMath.
- Produces: ten observed/partial execution scenarios for parity-approved V3 deployments.
- Produces exact deployment identity fields: chain ID, factory address/code
  hash, Quoter address/code hash, SwapRouter address/code hash, and pool
  factory-lineage proof.

- [ ] **Step 1: Add failing same-block parity fixtures**

Every fixture records chain, pool, fixed block, raw RPC hash, protocol reference
URL/commit, and Quoter exact-input/output raw integers. Standard Uniswap V3
must match exactly; fork-specific deployment remains unsupported until its own
fixture passes.

Add negative fixtures for a right DEX label with a wrong factory, Quoter,
SwapRouter, code hash, or pool-factory lineage. A V3-like ABI or source label
must never enable strict execution.

- [ ] **Step 2: Replace the old all-V3-unsupported expectation**

Assert ten unique direction/notional rows, same raw hash/block as depth,
observed-prefix/partial monotonicity, and cost/VWAP monotonicity. A partial
scenario cannot recover at a larger notional.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_dex_adapter_parity tests.test_fetch_dex_depth tests.test_execution_cost -v`

Expected: FAIL because parity/integration is absent.

- [ ] **Step 4: Enable only parity-approved deployment families**

Use the exact deployment registry, not a DEX-name allowlist. A Quoter mismatch,
code-identity mismatch, lineage mismatch, or revert is failed for a declared
capability; an unapproved fork stays unsupported with no numeric rows.

- [ ] **Step 5: Run V3 integration and full suite**

Run: `python3 -m unittest tests.test_dex_adapter_parity tests.test_uniswap_v3_execution_adapter tests.test_fetch_dex_depth tests.test_execution_cost -v`

Then: `python3 -m unittest discover -s tests -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add config/dex_protocol_deployments.csv scripts/check_dex_adapter_parity.py scripts/fetch_dex_depth.py scripts/execution_cost.py tests/test_dex_adapter_parity.py tests/test_fetch_dex_depth.py tests/test_execution_cost.py docs/execution-cost-data-contract.md
git commit -m "feat(dex): publish parity-checked V3 execution"
```

Add a GitHub commit comment with exact parity counts, deployment code hashes,
newly usable scenarios, and confirmation that `routes/latest.json` was not
written.

### Task 4: General pool identity and state-sequence schema

**Files:**
- Modify: `scripts/dex_adapters/types.py`
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/execution_cost.py`
- Modify: `dashboard/snapshot_refresh.py`
- Modify: `dashboard/server.py`
- Modify: `dashboard/market_facts.py`
- Modify: `dashboard/static/app.js`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_execution_cost.py`
- Modify: `tests/test_snapshot_fact_refresh.py`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_dashboard_frontend.py`

**Interfaces:**
- Produces fields: `pool_identity_type`, `state_sequence_type`, `state_sequence`, and `state_observed_at`.
- Preserves EVM compatibility fields: `pool_address`, `block_number`, `block_timestamp`.

- [ ] **Step 1: Write failing identity/state tests**

Cover `evm_contract`, `evm_pool_id`, and `solana_account`; `evm_block` and
`solana_slot`; reject mismatched field families and labels. Ensure UI says
`slot` for Solana and never `block`.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_execution_cost tests.test_snapshot_fact_refresh tests.test_dashboard tests.test_dashboard_frontend -v`

Expected: FAIL because generic state fields are absent.

- [ ] **Step 3: Implement one full-inventory schema migration**

Readers accept legacy EVM rows while new writers emit both generic and EVM
compatibility fields. Exact one-market merge cannot mix pre/post-migration
schemas; require a full inventory publication.

- [ ] **Step 4: Run migration regressions**

Run the focused tests above plus `python3 -m unittest tests.test_release_smoke -v`.

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/dex_adapters/types.py scripts/fetch_dex_depth.py scripts/execution_cost.py dashboard/snapshot_refresh.py dashboard/server.py dashboard/market_facts.py dashboard/static/app.js scripts/check_dashboard_release.py tests/test_execution_cost.py tests/test_snapshot_fact_refresh.py tests/test_dashboard.py tests/test_dashboard_frontend.py
git commit -m "feat(contract): support DEX pool IDs and state sequences"
```

Add a GitHub commit comment with legacy/full-migration evidence.

### Task 5: Uniswap V4 hook-free adapter

**Files:**
- Create: `config/dex_protocol_deployments.csv`
- Create: `scripts/dex_adapters/uniswap_v4.py`
- Create: `tests/test_uniswap_v4_adapter.py`
- Create: `tests/fixtures/dex_adapters/uniswap_v4/initialize_event.json`
- Create: `tests/fixtures/dex_adapters/uniswap_v4/hook_free_pool.json`
- Create: `tests/fixtures/dex_adapters/uniswap_v4/hooked_pool.json`
- Modify: `scripts/dex_adapters/registry.py`
- Modify: `scripts/fetch_dex_depth.py`

**Interfaces:**
- Consumes: V3 integer tick math where V4 semantics are identical and parity-proved.
- Produces: PoolKey/PoolManager/StateView identity, hook policy, strict hook-free depth/execution.

- [ ] **Step 1: Write failing Initialize/PoolKey tests**

Decode fixed-block Initialize evidence and require Pool ID, currencies, fee,
tick spacing, hooks, PoolManager deployment, and inventory token identities to
match exactly. Zero or conflicting events fail closed.

- [ ] **Step 2: Write hook/native/dynamic-fee counterexamples**

Only zero-hook, non-dynamic-fee pools may be strict initially. Nonzero hooks or
dynamic fees stay unsupported with `unvalidated_uniswap_v4_hook_behavior`.
Normalize native currency explicitly; never call ERC-20 decimals on zero.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_uniswap_v4_adapter -v`

Expected: FAIL because V4 adapter is absent.

- [ ] **Step 4: Implement StateView and same-block Quoter flow**

Use one fixed block for StateView, tick state, and quote. Quoter revert is
failed for supported hook-free pools. A 32-byte Pool ID never passes the EVM
contract-address path.

- [ ] **Step 5: Run V4 and collector tests**

Run: `python3 -m unittest tests.test_uniswap_v4_adapter tests.test_fetch_dex_depth tests.test_execution_cost -v`

Expected: PASS with all 30 markets explicitly classified.

- [ ] **Step 6: Commit**

```bash
git add config/dex_protocol_deployments.csv scripts/dex_adapters/uniswap_v4.py scripts/dex_adapters/registry.py scripts/fetch_dex_depth.py tests/test_uniswap_v4_adapter.py tests/fixtures/dex_adapters/uniswap_v4
git commit -m "feat(dex): add fail-closed Uniswap V4 adapter"
```

Add a GitHub commit comment with supported/unsupported hook classifications.

### Task 6: Balancer V2 and V3 adapters

**Files:**
- Create: `scripts/dex_adapters/balancer_v2.py`
- Create: `scripts/dex_adapters/balancer_v3.py`
- Create: `tests/test_balancer_v2_adapter.py`
- Create: `tests/test_balancer_v3_adapter.py`
- Create: `tests/fixtures/dex_adapters/balancer_v2/weighted_pool/manifest.json`
- Create: `tests/fixtures/dex_adapters/balancer_v2/unknown_pool/manifest.json`
- Modify: `scripts/dex_adapters/registry.py`
- Modify: `scripts/fetch_dex_depth.py`

**Interfaces:**
- Produces: distinct V2 Vault and V3 Router/Vault implementations.
- Allows: strict execution while model-specific depth remains explicitly unsupported.

- [ ] **Step 1: Write failing V2 implementation-identity tests**

Validate `getPoolId`, Vault, token order, swap fee, implementation allowlist,
paused/recovery state, and exact GIVEN_IN/GIVEN_OUT `queryBatchSwap`. Unknown
pool model remains unsupported; never use `x*y=k` or TVL.

- [ ] **Step 2: Write failing V3 separation tests**

Ensure V2 completion does not enable V3. Validate V3 Router/Vault, pool hooks,
fee and query API independently.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_balancer_v2_adapter tests.test_balancer_v3_adapter -v`

Expected: FAIL because Balancer adapters are absent.

- [ ] **Step 4: Implement execution-first adapters**

Publish observed/partial execution only when same-block query parity passes.
Until model-specific marginal depth passes its own parity, keep depth
unsupported with `model_specific_depth_not_implemented` and clear N/A reason.

- [ ] **Step 5: Run Balancer and integration tests**

Run: `python3 -m unittest tests.test_balancer_v2_adapter tests.test_balancer_v3_adapter tests.test_fetch_dex_depth tests.test_execution_cost -v`

Expected: PASS with both catalog markets explicitly classified.

- [ ] **Step 6: Commit**

```bash
git add scripts/dex_adapters/balancer_v2.py scripts/dex_adapters/balancer_v3.py scripts/dex_adapters/registry.py scripts/fetch_dex_depth.py tests/test_balancer_v2_adapter.py tests/test_balancer_v3_adapter.py tests/fixtures/dex_adapters/balancer_v2
git commit -m "feat(dex): add model-specific Balancer adapters"
```

Add a GitHub commit comment stating whether each pool has execution-only or full support.

### Task 7: Solana fixed-slot RPC foundation

**Files:**
- Create: `scripts/dex_adapters/solana_rpc.py`
- Create: `tests/test_solana_state_snapshot.py`
- Modify: `scripts/dex_adapters/types.py`
- Modify: `scripts/fetch_dex_depth.py`

**Interfaces:**
- Produces: `get_multiple_accounts()` and `get_block_time()` with `AccountBatch` lineage.

- [ ] **Step 1: Write failing consistent-slot/account tests**

Require one `getMultipleAccounts` context slot for all pool accounts, matching
program owner, mint/Token Program identity, decimals, block time, per-account
data hash, and whole-response hash. Reject mixed slots and owner/mint mismatch.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_solana_state_snapshot -v`

Expected: FAIL because Solana foundation is absent.

- [ ] **Step 3: Implement bounded account reader**

Retain lamports, executable, rent epoch, base64 bytes, hashes, context slot,
and UTC block time. `minContextSlot` is a lower bound, not an exact historical
slot; never merge accounts from different responses without equal context.

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest tests.test_solana_state_snapshot -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/dex_adapters/solana_rpc.py scripts/dex_adapters/types.py scripts/fetch_dex_depth.py tests/test_solana_state_snapshot.py
git commit -m "feat(dex): establish fixed-slot Solana state lineage"
```

Add a GitHub commit comment with mixed-slot/owner rejection evidence.

### Task 8: Orca, Meteora, and Raydium protocol adapters

**Files:**
- Create: `scripts/dex_adapters/orca_whirlpool.py`
- Create: `scripts/dex_adapters/meteora_dlmm.py`
- Create: `scripts/dex_adapters/raydium.py`
- Create: `tests/test_orca_whirlpool_adapter.py`
- Create: `tests/test_meteora_dlmm_adapter.py`
- Create: `tests/test_raydium_adapter.py`
- Create: `tests/fixtures/dex_adapters/solana/manifest.json`
- Modify: `scripts/dex_adapters/registry.py`
- Modify: `scripts/fetch_dex_depth.py`

**Interfaces:**
- Consumes: Task 7 fixed-slot accounts.
- Produces: isolated strict adapters for Orca Whirlpool, Meteora DLMM, Raydium AMM, and Raydium CLMM.

- [ ] **Step 1: Implement Orca through RED/GREEN fixtures**

Test pool/mint/vault/tick-array/oracle identity, exact input/output, tick-array
crossing, missing array, insufficient liquidity, owner mismatch, and
Token-2022 transfer-fee unsupported. Match a fixed official SDK commit raw quote.

- [ ] **Step 2: Implement Meteora through RED/GREEN fixtures**

Test bin arrays, active-bin change, dynamic fee, exact output, missing bin
array, and same-slot evidence. Do not reuse Whirlpool tick math.

- [ ] **Step 3: Implement Raydium AMM and CLMM separately**

Use separate program IDs, layouts, fee and quote math. Test each model, account
identity, exact input/output, partial liquidity, and mismatched model.

- [ ] **Step 4: Run protocol and collector tests**

Run: `python3 -m unittest tests.test_orca_whirlpool_adapter tests.test_meteora_dlmm_adapter tests.test_raydium_adapter tests.test_fetch_dex_depth tests.test_execution_cost -v`

Expected: PASS; all 18 target markets have explicit observed/partial/unsupported/failed reasons.

- [ ] **Step 5: Commit each protocol separately**

```bash
git add scripts/dex_adapters/orca_whirlpool.py tests/test_orca_whirlpool_adapter.py tests/fixtures/dex_adapters/solana
git commit -m "feat(dex): add Orca Whirlpool adapter"
```

```bash
git add scripts/dex_adapters/meteora_dlmm.py tests/test_meteora_dlmm_adapter.py tests/fixtures/dex_adapters/solana
git commit -m "feat(dex): add Meteora DLMM adapter"
```

```bash
git add scripts/dex_adapters/raydium.py scripts/dex_adapters/registry.py scripts/fetch_dex_depth.py tests/test_raydium_adapter.py tests/fixtures/dex_adapters/solana
git commit -m "feat(dex): add Raydium AMM and CLMM adapters"
```

Add one GitHub commit comment per protocol with fixture/parity and market counts.

### Task 9: Global adapter release gates and staged production rollout

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `docs/collection-operations.md`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_publication_gate.py`

**Interfaces:**
- Consumes: Tasks 1–8.
- Produces: capability, identity, state-sequence, raw-evidence, scenario-grid, coverage, and atomic-publication gates.

- [ ] **Step 1: Add failing counterexamples**

Reject unsupported regression from declared capability, float/approximate
strict result, wrong PoolManager/Vault/program owner, mixed block/slot, missing
raw hash, non-ten-scenario inventory, partial promoted to observed, and
cross-protocol model reuse. Also reject any route-eligible adapter that lacks
exact base-quantity conformance or returns a different net Token quantity than
requested.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke tests.test_publication_gate -v`

Expected: FAIL because global adapter gates are incomplete.

- [ ] **Step 3: Implement and document gates**

Keep existing 80% supported current usability and 95% comparable baseline
retention. Full depth/history/latest/execution remains one failure-atomic
family bundle.

- [ ] **Step 4: Run full suite and Python 3.8 checks**

Run: `python3 -m unittest discover -s tests -v`, Python 3.8 compile/import
checks for every adapter, `git diff --check`, and fixture hash verification.
Run the shared `tests.test_route_quantity` conformance cases once per adapter
family before setting `strict_route_quantity=True`.

- [ ] **Step 5: Stage production enablement per adapter**

For each deployment family: no-publish candidate, offline parity audit, one
exact market publish, small family publish, full inventory publish. Record
before/after observed/partial/unsupported/failed counts and never enable a
family that misses parity.

This task prepares validated data candidates only. Production application
deployment and public timer enablement occur in the cross-increment final
release plan after Route, Costs, Opportunities, adapters, and Events all pass
their combined release gate.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_dashboard_release.py docs/dex-depth-data-contract.md docs/collection-operations.md tests/test_release_smoke.py tests/test_publication_gate.py
git commit -m "test(dex): enforce protocol adapter release gates"
```

Add a GitHub commit comment with full-suite, parity, and staged-rollout evidence.
