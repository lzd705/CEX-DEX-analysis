# Uniswap V3 Exact Swap MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use one protocol-exact integer Uniswap V3 engine to calculate both marginal-price-band depth and fixed-notional execution for two approved Ethereum UNI pools.

**Architecture:** A dependency-free Python 3.8 math module ports the observable integer behavior of Uniswap V3 `TickMath`, `SqrtPriceMath`, and `SwapMath`. The existing fixed-block collector supplies immutable pool state and a bounded, complete bitmap/tick window to that module; a checked-in authority file enables execution only for the two reviewed pools, while every other V3 market remains explicitly unsupported.

**Tech Stack:** Python 3.8+ standard library, existing JSON-RPC collector, `unittest`, checked-in JSON authority.

**Spec:** `docs/dex-depth-data-contract.md` and `docs/execution-cost-data-contract.md`

## Global Constraints

- Every pool call, bitmap read, tick read, depth result, and execution result must bind to one fixed EVM block per chain run.
- Calculations use integer token base units and Uniswap V3 rounding; `Decimal` is allowed only for human-unit and USD presentation.
- Raw RPC transcripts and SHA-256 lineage remain the evidence source.
- Missing, unsupported, partial, and failed values remain blank or null; they are never replaced with zero or interpolated from depth bands.
- USD conversion evidence must pass the existing two-hour observation-skew gate.
- The execution MVP is limited to the two exact market identities listed in `config/uniswap_v3_execution_markets.json`.
- Gas, router fee, transfer tax, MEV, and post-block changes remain outside this pool-only execution fact.
- A bounded tick scan may publish `partial`, but may never claim `observed` unless the requested amount is completely resolved inside the verified scan window.

---

### Task 1: Protocol-exact integer math

**Files:**
- Create: `scripts/uniswap_v3_math.py`
- Create: `tests/test_uniswap_v3_math.py`

**Interfaces:**
- Produces: `get_sqrt_ratio_at_tick`, `get_tick_at_sqrt_ratio`, `get_amount0_delta`, `get_amount1_delta`, `compute_swap_step`, `sqrt_price_limit_for_bps`, and `simulate_swap`.
- Produces: immutable `SwapStep` and `SwapResult` values containing gross input, output, fee, final price/tick/liquidity, step count, completeness, and terminal reason.

- [x] **Step 1: Write failing official-vector tests**

  Add literal expectations from Uniswap V3 core tests for tick boundaries and `computeSwapStep`, including capped and fully consumed exact-input/output cases.

- [x] **Step 2: Run the math tests and verify RED**

  Run: `python3 -m unittest tests.test_uniswap_v3_math -v`

  Expected: import failure because `scripts.uniswap_v3_math` does not exist.

- [x] **Step 3: Implement the minimum exact math port**

  Port the Q64.96 constants, `mulDiv` rounding, tick ratio constants, amount deltas, next-price functions, and step calculation using Python integers only.

- [x] **Step 4: Add failing multi-tick simulation tests**

  Cover both directions, exact input/output, liquidity-net sign on crossings, scan-limit partial output, and a completed quote before the scan boundary.

- [x] **Step 5: Implement `simulate_swap` and verify GREEN**

  Run: `python3 -m unittest tests.test_uniswap_v3_math -v`

  Expected: all exact-vector and multi-tick tests pass.

### Task 2: Two-pool authority and bounded tick evidence

**Files:**
- Create: `config/uniswap_v3_execution_markets.json`
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Consumes: exact math functions from Task 1.
- Produces: validated authority loading for the UNI/USDT and UNI/WETH 0.3% pools.
- Produces: one bounded bitmap/tick window containing its exact word range and every initialized tick returned at the fixed block.

- [x] **Step 1: Write failing authority-validation tests**

  Require canonical market IDs, pool/token addresses, fee `3000`, tick spacing `60`, unique identities, and a positive bounded word radius.

- [x] **Step 2: Run the collector tests and verify RED**

  Run: `python3 -m unittest tests.test_fetch_dex_depth.DexDepthMathTest tests.test_fetch_dex_depth.DexDepthCollectionTest -v`

  Expected: missing authority loader and tick-window interface.

- [x] **Step 3: Add the checked-in authority and loader**

  Configure exactly:

  - `dex:eth:uniswap_v3:0x3470447f3cecffac709d3e783a307790b0208d60:UNI` (`UNI/USDT`, fee 3000, spacing 60)
  - `dex:eth:uniswap_v3:0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801:UNI` (`UNI/WETH`, fee 3000, spacing 60)

- [x] **Step 4: Add failing bounded-scan tests**

  Prove that every bitmap and tick call uses the fixed block, the transcript retains the calls, an identity mismatch fails closed, and exhaustion at the verified boundary becomes partial rather than observed.

- [x] **Step 5: Implement the bounded window and verify GREEN**

  Query the current bitmap word plus the configured number of words in each direction, load every initialized tick in that range, and expose exact lower/upper price limits to the engine.

### Task 3: Shared exact depth and execution output

**Files:**
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `scripts/execution_cost.py` only if an existing validation rule cannot express the exact result
- Modify: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Consumes: `simulate_swap`, the authority record, fixed-block state, token metadata, prices, and bounded tick window.
- Produces: exact integer V3 depth amounts for 10/25/50/100 bps and ten execution rows per approved market.

- [x] **Step 1: Replace the old unsupported test with a failing approved-market test**

  Assert that the approved fake V3 pool produces observed exact depth and ten measured execution rows with fixed-block identities, integer-base-unit alignment, included pool fee, raw hash lineage, and excluded-cost labels.

- [x] **Step 2: Add failing terminal-path tests**

  Assert that an unapproved V3 market stays unsupported, approved-market calculation defects are failed, and scan-bound exhaustion is partial with no fabricated full cost.

- [x] **Step 3: Implement shared depth calculations**

  Derive conservative integer sqrt-price limits for each band and run the exact engine to the limit. Mark the band complete only when the engine reaches that exact limit.

- [x] **Step 4: Implement execution calculations**

  Floor target Token quantities to base units; use exact input for sells and exact output for buys; convert only final integer quantities to human units and USD; retain tick/step count, final marginal price, fee scope, and partial evidence.

- [x] **Step 5: Make publication coverage fail closed for approved pools**

  Reclassify `unsupported` as an eligible failure for the two approved market IDs so they cannot disappear from the supported denominator. Leave all other V3 markets structurally excluded.

- [x] **Step 6: Run collector and contract tests**

  Run: `python3 -m unittest tests.test_uniswap_v3_math tests.test_fetch_dex_depth tests.test_execution_cost -v`

  Expected: all tests pass.

### Task 4: Contract update and real two-pool verification

**Files:**
- Modify: `docs/dex-depth-data-contract.md`
- Modify: `docs/execution-cost-data-contract.md`
- Modify: `README.md`

**Interfaces:**
- Documents: exact supported scope, formulas, scan boundary, statuses, validation evidence, and exclusions.

- [x] **Step 1: Update the contracts**

  Replace the blanket V3-execution unsupported statement with the exact two-market MVP and keep every other V3 identity unsupported.

- [x] **Step 2: Run a real fixed-block two-pool candidate without publishing**

  Collect both authority markets against Ethereum JSON-RPC, retain the raw transcript and manifest in ignored runtime paths, and validate all depth/execution rows locally.

- [x] **Step 3: Differential-check representative quotes**

  Compare both directions for at least one small and one large target per pool against the official Uniswap Quoter at the identical block. Exact amounts must match; otherwise the candidate remains unpublished and the mismatch is investigated.

- [x] **Step 4: Run full verification**

  Run the targeted suites, `python3 -m compileall scripts dashboard tests`, `git diff --check`, and the full `unittest` suite with the known local port-binding limitation reported separately.

- [x] **Step 5: Review and commit**

  Review the complete branch diff for scope, data-quality gates, and Python 3.8 compatibility before creating one or more focused commits.

## Verification Record

- A non-publishing real canary collected the two approved pools at finalized
  Ethereum block `25840461`, hash
  `0x05f08e9c9cfd927775676fb6a94e9d45e01ed57f362806063accdb9ccb0ef77a`.
- The result contained two complete depth rows and 20 complete execution rows:
  two directions by five USD notionals for each pool.
- Every complete execution scenario matched Uniswap QuoterV2 in raw token
  units, final `sqrtPriceX96`, and initialized-tick count at the identical
  block. The retained transcript validator independently requires the same
  unique 2-by-5 scenario inventory.
- The GeckoTerminal response, its SHA-256 digest, pool transcripts, fixed-block
  identity, and final post-Quoter header check were retained under the ignored
  canary runtime directory. The run did not move any publication pointer.
- The final full repository suite passed 2,105 tests. Python compilation,
  Python 3.8 grammar checks, and whitespace validation also passed before the
  implementation commit.
