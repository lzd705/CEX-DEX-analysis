# Historical Foundry Replay Opportunity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver one real, independently replayable historical UNI/WETH Opportunity bundle for Uniswap V2 and SushiSwap V2, covering the exact seven-day finalized window, two directions, five notionals, ten successful Foundry/Anvil receipts, at least one strictly positive policy-net research estimate, and a separate historical Dashboard surface.

**Architecture:** Build four independently green phases. Phase 1 freezes policy, authority, toolchain, shared arithmetic, and the immutable Solidity executor. Phase 2 captures the complete seven-day archive evidence and replays candidate blocks in fresh Anvil forks. Phase 3 converts the selected evidence into the existing Opportunity economics through sealed historical core and complete-bundle profiles, then publishes only after connected verification. Phase 4 adds a historical-only API/UI/release gate and executes the first real nonfixture run. Live Shadow and live Opportunity contracts remain unchanged.

**Tech Stack:** Python 3.8.10+ standard library, Solidity 0.8.36, Foundry/Anvil/Forge/Cast v1.7.1, forge-std v1.16.1 pinned to a full commit, Ethereum archive JSON-RPC, existing CSV/SQLite publication primitives, vanilla dashboard JavaScript and Python `unittest`.

**Spec:** `docs/superpowers/specs/2026-08-20-historical-foundry-replay-opportunity-design.md`

## Global Constraints

- This is a historical counterfactual research replay, not a current quote, executable route, attestation, or transaction broadcaster.
- The public result is exactly two routes, five notionals per route, ten status-one `research_estimate` rows, ninety cost rows, zero strict/executable/attested/unavailable rows, and at least one exact positive policy-net result.
- The scan denominator is every block in the inclusive seven-day range ending at one frozen `finalized` anchor. Missing reserve, price, fee, header, or candidate evidence is inconclusive, never “no opportunity.”
- Pairs are derived by verified factories. No public API accepts a pair, block, notional grid, endpoint, sender, executor, compiler, hardfork, MEV rate, price TTL, or economic result.
- The current policy uses 10 bps acceptance MEV and 25/50 bps stress MEV. The generic policy validator permits a reviewed exact zero rate; no implementation may hard-code a positive-rate restriction or a hidden 10 bps fallback.
- Every selected scenario runs in a fresh fork with one sealed overlay and exactly one type-2 transaction in synthetic block `B+1`. No reusable private key is retained.
- Historical paths are namespaced under `raw/historical-foundry-replay` and `routes/historical`. Both live pointers, `routes/core/latest.json` and `routes/latest.json`, must remain byte-for-byte unchanged in every historical success and failure test.
- All Python production code must parse and run on real CPython 3.8.10. Foundry is a pinned offline collection dependency and is never installed by the dashboard.
- Use exact fixed-point strings, integers, `Decimal`, or `Fraction`; never binary floating point for economic authority.
- Tests are written and observed RED before production code. Every task ends with focused GREEN, the phase ends with system and real-3.8 regressions, and no phase is called complete from fixture-only evidence.
- Never persist an RPC URL, provider credential, header, cookie, private key, arbitrary error body, local absolute path, or exception text.
- The user-set task budget is capped at 600,000,000 tokens. Never silently exceed it; if the goal meter cannot enforce that budget, stop manually when `tokensUsed` reaches 600,000,000 and report the tooling limitation rather than inventing usage.

## Phase Plans and Dependency Order

1. [Foundation and pinned executor](2026-08-20-historical-foundry-foundation-plan.md)
   - Exit gate: three canonical config contracts, shared exact arithmetic, a hash-bound executor, offline unit tests, and one connected fixed-block fork KAT.
2. [Seven-day scan and candidate replay](2026-08-20-historical-foundry-scan-replay-plan.md)
   - Depends on Phase 1.
   - Exit gate: immutable full-window raw run with exact coverage, descending candidate resolution, and either a closed nonpublication result or one selected ten-success block.
3. [Opportunity bridge and historical publication](2026-08-20-historical-foundry-publication-plan.md)
   - Depends on Phases 1–2.
   - Exit gate: sealed historical core/context, ten Opportunity rows, ninety components, compact replay evidence, connected report, and atomic historical pointer publication.
4. [Historical Dashboard, release gate, and real run](2026-08-20-historical-foundry-dashboard-release-plan.md)
   - Depends on Phases 1–3.
   - Exit gate: historical API/UI parity, dedicated release-checker success, audit-only replay success, dual-runtime regressions, and the first real nonfixture published bundle.

## Cross-Phase Review Gates

- [ ] After Phase 1, independently review config override resistance, compiler/runtime identities, generic zero-MEV policy acceptance, and offline/connected Foundry gate separation.
- [ ] After Phase 2, independently review coverage arithmetic, RPC response-set closure, safe-exclusion soundness, closed-revert classification, fresh-fork isolation, and credential redaction.
- [ ] After Phase 3, independently review topology parity, staged/committed context identity, live-pointer nonmutation, report/pointer hash closure, descriptor safety, and dry-run zero-publication behavior.
- [ ] After Phase 4, independently review historical/current UI race isolation, cache invalidation after report mutation, release-checker denominator parity, and the real run's raw evidence inventory.

## Final Verification Command Set

Run from the repository root with `MARKET_DATA_DIR` pointing at the isolated historical replay data directory and `DEX_DEPTH_RPC_ETH` set only in the process environment:

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_replay \
  tests.test_historical_foundry_verifier \
  tests.test_historical_route_publication \
  tests.test_run_historical_foundry_replay \
  tests.test_historical_opportunity_api \
  tests.test_historical_opportunity_frontend \
  tests.test_route_cost_topology \
  tests.test_route_quantity \
  tests.test_bounded_json \
  tests.test_route_publication \
  tests.test_opportunity_api \
  tests.test_opportunity_frontend \
  tests.test_navigation \
  tests.test_release_smoke -v
```

```bash
python3.8 -c 'import sys; assert sys.version_info[:3] == (3, 8, 10), sys.version'
python3.8 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_replay \
  tests.test_historical_foundry_verifier \
  tests.test_historical_route_publication \
  tests.test_run_historical_foundry_replay \
  tests.test_historical_opportunity_api \
  tests.test_historical_opportunity_frontend \
  tests.test_route_cost_topology \
  tests.test_route_quantity \
  tests.test_bounded_json \
  tests.test_route_publication \
  tests.test_opportunity_api \
  tests.test_opportunity_frontend \
  tests.test_navigation \
  tests.test_release_smoke -v
```

The exact CPython command is a release blocker until the preceding version assertion succeeds; `/usr/bin/python3` 3.9.x or a system Python 3.13 run is not a substitute.

```bash
python3 -m unittest discover -s tests -v
python3.8 -m unittest discover -s tests -v
```

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-connected-kat
```

```bash
python3 -m scripts.run_historical_foundry_replay scan \
  --data-dir "$MARKET_DATA_DIR" --publish
python3 -m scripts.run_historical_foundry_replay verify \
  --data-dir "$MARKET_DATA_DIR" \
  --bundle "$MARKET_DATA_DIR/routes/historical/bundles/$REPLAY_ID"
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8765 \
  --require-historical-foundry-replay
```

Do not replace the connected run with fixtures, do not use `--require-route-opportunities` as a substitute for the historical gate, and do not report completion unless all evidence listed in the design's Completion Evidence section exists and has been reread.
