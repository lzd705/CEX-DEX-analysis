# Route Cost and Opportunity Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Calculate comparable cross-venue route candidates from one common Token quantity and explicit all-in cost components, while keeping strict executable candidates separate from research estimates.

**Architecture:** New focused modules define cost-component facts, authenticated or estimated CEX fees, route-specific DEX costs, and opportunity math. The existing fixed-notional quoted-cost contract remains unchanged. The synchronized route collector supplies leg state; this plan supplies exact quantity quotes, cost completeness, and strict/scenario classification.

**Tech Stack:** Python 3.8-compatible standard library, Decimal/integer arithmetic, existing CEX book and DEX pool adapters, CSV/SQLite immutable bundles, `unittest`.

## Global Constraints

- Never subtract two independently derived same-USD-notional execution rows.
- Both legs quote one exact common Token quantity.
- CEX account fees are strict only from authenticated or validated private profiles; credentials and account identity never enter public output or logs.
- Pool fee already embedded in a DEX quote is not added a second time.
- Gas requires a concrete transaction call; MEV is a scenario buffer/protection policy, never a measured zero.
- `assumed` and `bounded_estimate` components never enter strict ranking.
- Missing and unsupported values remain null/N/A, never zero.
- Funding Rate and Upbit mutation are excluded.

---

### Task 1: Cost-component fact contract

**Files:**
- Create: `scripts/execution_cost_components.py`
- Create: `tests/test_execution_cost_components.py`
- Modify: `docs/execution-cost-data-contract.md`

**Interfaces:**
- Produces: `COST_COMPONENT_CONTRACT_VERSION = "1"`.
- Produces: `cost_component_row(...)`, `validate_cost_components(rows)`, and `aggregate_cost_components(rows, include_assumptions)`.

- [ ] **Step 1: Write failing schema and strictness tests**

```python
def test_assumed_component_cannot_be_strict_eligible(self):
    with self.assertRaisesRegex(ValueError, "assumed.*strict"):
        cost_component_row(
            cohort_id="cohort-1",
            opportunity_id="route-1:10000",
            leg="route",
            market_id="",
            direction="route",
            requested_notional_usd=Decimal("10000"),
            target_token_quantity=Decimal("100"),
            component_type="mev_buffer",
            value_status="assumed",
            amount_usd=Decimal("10"),
            rate_bps=Decimal("10"),
            basis="user scenario buffer",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="user scenario",
            source_record_sha256=None,
        )
```

Also test duplicate component keys, negative/non-finite values, numeric
USD/bps recomputation, `not_applicable` with blank numbers, measured values
requiring timestamp/hash, and terminal values containing no numeric amount.

- [ ] **Step 2: Run the contract tests and verify RED**

Run: `python3 -m unittest tests.test_execution_cost_components -v`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement normalized rows and validation**

Allow only:

```python
COMPONENT_TYPES = {
    "venue_taker_fee", "pool_swap_fee", "network_gas",
    "router_or_integrator_fee", "token_transfer_tax",
    "rebalancing_or_transfer", "mev_buffer",
}
VALUE_STATUSES = {
    "measured", "authenticated", "quoted", "bounded_estimate", "assumed",
    "not_applicable", "unavailable", "unsupported", "failed", "stale",
}
```

Use exact Decimal strings in storage. `aggregate_cost_components()` returns
strict amount, scenario amount, missing required kinds, and completeness; it
must never coerce a missing value to zero.

- [ ] **Step 4: Run tests and contract documentation checks**

Run: `python3 -m unittest tests.test_execution_cost_components -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/execution_cost_components.py tests/test_execution_cost_components.py docs/execution-cost-data-contract.md
git commit -m "feat(contract): add route cost component facts"
```

Add a GitHub commit comment describing strict eligibility and null handling.

### Task 2: CEX fee facts and secret boundary

**Files:**
- Create: `scripts/cex_fee_facts.py`
- Create: `tests/test_cex_fee_facts.py`
- Create: `config/cex_public_fee_schedules.csv`
- Modify: `deploy/dashboard.env.example`
- Modify: `docs/collection-operations.md`

**Interfaces:**
- Consumes: component rows from Task 1.
- Produces: `normalize_binance_taker_fee()`, `normalize_bybit_taker_fee()`, `normalize_okx_taker_fee()`, `load_validated_fee_profile()`, and `collect_cex_fee_snapshot()`.

- [ ] **Step 1: Add official-response fixture tests**

Use redacted fixtures for Binance standard/special/tax/discount commission,
Bybit buy/sell taker fee, and OKX maker/taker fee. Assert side, instrument,
fee asset/basis, timestamp, opaque profile hash, and exact rate.

- [ ] **Step 2: Add secret non-disclosure counterexamples**

Serialize normalized rows and collector logs after clients contain sentinel
API key, secret, passphrase, account ID, and Authorization values. Assert no
sentinel appears. Missing authentication must publish `unavailable`, not a
public/default fee as strict.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_cex_fee_facts -v`

Expected: FAIL because fee adapters are absent.

- [ ] **Step 4: Implement authenticated adapters and generic private profiles**

The live adapter boundary receives an already configured client and returns
only normalized fee evidence. A generic validated private CSV supports other
venues with columns:

```text
profile_id,venue,instrument,side,taker_fee_bps,fee_asset,basis,
observed_at,valid_until,source_record_sha256
```

Reject world-readable private profile files, duplicate keys, stale records,
and non-opaque profile IDs. The repository public schedule contains official
source URL/check time/rate bounds and always projects `bounded_estimate`.

- [ ] **Step 5: Run fee and redaction tests**

Run: `python3 -m unittest tests.test_cex_fee_facts -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/cex_fee_facts.py tests/test_cex_fee_facts.py config/cex_public_fee_schedules.csv deploy/dashboard.env.example docs/collection-operations.md
git commit -m "feat(costs): collect bounded and authenticated CEX fees"
```

Add a GitHub commit comment listing official-source checks and redaction tests.

### Task 3: DEX gas, router, transfer, and MEV policy

**Files:**
- Create: `scripts/dex_route_costs.py`
- Create: `tests/test_dex_route_costs.py`
- Modify: `scripts/fetch_dex_depth.py`
- Modify: `docs/dex-depth-data-contract.md`

**Interfaces:**
- Consumes: fixed-block adapter quote plus component rows from Task 1.
- Produces: validated `GasQuoteRequest`, `estimate_route_gas()`,
  `router_fee_component()`, `transfer_tax_component()`, and
  `mev_route_policy()`.

- [ ] **Step 1: Write failing concrete-gas lineage tests**

```python
component = estimate_route_gas(
    rpc=fake_rpc,
    request=GasQuoteRequest(
        chain_id=1,
        tx_call={"from": SENDER, "to": ROUTER, "data": CALLDATA, "value": "0x0"},
        tx_call_sha256=CALL_SHA,
        sender_policy="opaque_simulation_sender",
        allowance_basis="preapproved",
        block_tag="0x1234",
        max_fee_per_gas_wei=20_000_000_000,
        fee_cap_source="eth_feeHistory",
        fee_cap_observed_at="2026-08-01T12:00:00Z",
        fee_cap_valid_until="2026-08-01T12:02:00Z",
        fee_cap_source_sha256=FEE_SHA,
        native_token_usd=Decimal("3500"),
        native_price_observed_at="2026-08-01T12:00:00Z",
        native_price_sha256=PRICE_SHA,
        adapter_id="uniswap_v3_router/v1",
    ),
)
self.assertEqual(component["value_status"], "quoted")
self.assertEqual(component["gas_units"], "150000")
```

Assert missing chain/sender/calldata/call hash/block/allowance basis, stale
fee cap or native USD price, source-hash mismatch, arbitrary caller-only fee
cap, or RPC failure is unavailable/failed and contains no strict amount.

- [ ] **Step 2: Write router/transfer/MEV strictness tests**

Unknown router fee and transfer-tax behavior cannot be `not_applicable`.
Only adapter evidence may prove numeric/NA. `public_mempool` without a bounded
protection policy is strict unavailable; a user MEV buffer is `assumed` and
strict-ineligible.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_dex_route_costs -v`

Expected: FAIL because DEX route-cost functions are absent.

- [ ] **Step 4: Implement exact component builders**

Gas USD is:

```text
gas_units × max_fee_per_gas_wei / 1e18 × native_token_usd
```

Retain chain ID, RPC block, transaction-call hash, sender/allowance policy,
price timestamp/hash, gas units, fee-cap source/timestamp/validity/hash, and
adapter ID. Strict eligibility requires every field. Never publish
wallet/sender as a public account identity; hash or redact it according to the
component contract.

- [ ] **Step 5: Run DEX cost tests**

Run: `python3 -m unittest tests.test_dex_route_costs tests.test_fetch_dex_depth -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/dex_route_costs.py tests/test_dex_route_costs.py scripts/fetch_dex_depth.py docs/dex-depth-data-contract.md
git commit -m "feat(costs): model route-specific DEX costs"
```

Add a GitHub commit comment distinguishing quotes, assumptions, and measured facts.

### Task 4: Private inventory evidence and route-mode gate

**Files:**
- Create: `scripts/route_inventory.py`
- Create: `tests/test_route_inventory.py`
- Modify: `deploy/dashboard.env.example`
- Modify: `docs/collection-operations.md`

**Interfaces:**
- Produces: `INVENTORY_EVIDENCE_VERSION = "1"`,
  `load_validated_inventory_profile()`, `inventory_capacity_for_route()`, and
  `classify_route_mode_evidence()`.

- [ ] **Step 1: Write failing capacity, freshness, and privacy tests**

Use a private profile with exact rows:

```text
profile_id,market_id,asset,available_quantity,observed_at,valid_until,
source_record_sha256
```

For `prepositioned_inventory`, require enough quote asset on the buy market
and enough net Token quantity on the sell market. Assert missing, stale,
insufficient, duplicated, wrong-asset, world-readable, or non-opaque evidence
returns `inventory_unavailable`/`inventory_insufficient` with no numeric strict
capacity. Serialize every public row and log and prove account, wallet, API-key,
and local-profile path sentinels are absent.

- [ ] **Step 2: Add route-mode fail-closed tests**

Independent DEX leg quotes cannot prove `atomic_onchain`. Until a composed
router call is built and simulated at one cohort state, classify the route as
Research Estimate with `atomic_route_simulation_unavailable`. A
`rebalance_required` route remains estimate-only unless its transfer leg and
inventory evidence are complete and current.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_inventory -v`

Expected: FAIL because the inventory contract is absent.

- [ ] **Step 4: Implement private validation and exact capacity**

Reject symlinks and broad/untrusted paths, require owner-only file permissions,
validate timestamps/hashes/unique keys, and retain only an opaque profile hash
in public lineage. Inventory limits route capacity but never upgrades market
depth or creates a price fact.

- [ ] **Step 5: Run inventory and redaction tests**

Run: `python3 -m unittest tests.test_route_inventory -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_inventory.py tests/test_route_inventory.py deploy/dashboard.env.example docs/collection-operations.md
git commit -m "feat(routes): validate private inventory evidence"
```

Add a GitHub commit comment describing capacity, expiry, and secret-redaction
evidence.

### Task 5: Common-quantity leg quoting

**Files:**
- Create: `scripts/route_quantity.py`
- Create: `tests/test_route_quantity.py`
- Modify: `scripts/execution_cost.py`
- Modify: `scripts/fetch_cex_depth.py`
- Modify: `scripts/fetch_dex_depth.py`
- Test: `tests/test_execution_cost.py`
- Test: `tests/test_fetch_cex_depth.py`
- Test: `tests/test_fetch_dex_depth.py`

**Interfaces:**
- Produces: `MarketRules` with canonical `base_asset`, `quote_asset`, base/quote
  increments and minima; `QuantityQuote` with
  `quote_debit_asset`, `quote_debit_quantity`,
  `gross_base_received_quantity`, `net_base_received_quantity`, and
  `fee_debit_asset`/`fee_debit_quantity`; and the adapter contract
  `quote_base_quantity(direction, target_base_raw, market_state)`.
- Produces: `common_net_target_quantity()`,
  `quote_cex_book_quantity(levels, target_token_quantity, market_rules,
  fee_semantics)`, and
  `quote_v2_pool_quantity(..., target_token_quantity, market_rules)`.
- Preserves: existing `execution_fact_row()` and fixed-notional v1 outputs.

- [ ] **Step 1: Write failing known-answer quantity tests**

Use one common net quantity against two different reference prices and assert
both leg results retain that exact quantity. Add CEX partial level, base-unit
and lot increment, minimum-notional after rounding, V2 integer rounding,
insufficient reserve, and target-one-base-unit cases. Cover fees charged in
base, quote, and third assets; when third-asset conversion is unavailable the
strict common net quantity must be unavailable.

- [ ] **Step 2: Verify RED and old-contract baseline**

Run: `python3 -m unittest tests.test_route_quantity tests.test_execution_cost tests.test_fetch_cex_depth tests.test_fetch_dex_depth -v`

Expected: new tests FAIL while existing fixed-notional tests remain PASS.

- [ ] **Step 3: Extract quantity-based helpers without changing v1 formulas**

The CEX helper walks original levels; the V2 helper uses integer invariant
math. Return `complete`, exact gross and net filled quantities, raw base units,
asset-qualified quote debit, VWAP, ending price, asset-qualified fee debit,
and consumed level/tick count. Normalize market rules separately from the
book. `inventory_capacity_for_route()` compares canonical like-asset
quantities only; it never compares a venue balance to normalized USD. Do not derive from
10/25/50/100-bps bands.

Every current and future DEX adapter must pass the same quantity-adapter
conformance suite before it becomes route eligible. Fixed-USD-only adapters
remain liquidity facts but are excluded from strict route generation.

- [ ] **Step 4: Run focused execution tests**

Run: `python3 -m unittest tests.test_route_quantity tests.test_execution_cost tests.test_fetch_cex_depth tests.test_fetch_dex_depth -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/route_quantity.py tests/test_route_quantity.py scripts/execution_cost.py scripts/fetch_cex_depth.py scripts/fetch_dex_depth.py tests/test_execution_cost.py tests/test_fetch_cex_depth.py tests/test_fetch_dex_depth.py
git commit -m "feat(execution): quote one common Token quantity"
```

Add a GitHub commit comment with known-answer and v1 non-regression evidence.

### Task 6: Route-opportunity math and classification

**Files:**
- Create: `scripts/route_opportunity.py`
- Create: `tests/test_route_opportunity.py`

**Interfaces:**
- Consumes: synchronized legs, quantity quotes, cost components, inventory
  evidence, and route-mode evidence.
- Produces: `common_target_quantity()`, `build_route_opportunity()`, and `validate_route_opportunity()`.

- [ ] **Step 1: Write failing common-quantity and edge tests**

```python
quantity = common_target_quantity(
    requested_notional_usd=Decimal("10000"),
    buy_reference_price_usd=Decimal("101"),
    sell_reference_price_usd=Decimal("100"),
    market_rules=MarketRules(base_increment=Decimal("0.01")),
)
self.assertEqual(quantity, Decimal("99.00"))
```

Add exact gross edge, cost sums, negative edge, partial leg, different fee
assets, inventory capacity, and DEX pool-fee double-count counterexamples.

- [ ] **Step 2: Add strict/scenario boundary tests**

Assert 60 seconds passes and `60.000001` fails; 120-second cohort age passes
only at the exact boundary; bounded estimate, assumed component, stale fee,
no pre-positioned inventory, non-atomic independent DEX quotes, cross-chain
route, or missing router behavior makes strict unavailable while a labeled
research estimate may remain.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_opportunity -v`

Expected: FAIL because the evaluator is absent.

- [ ] **Step 4: Implement calculation and validation**

Use:

```text
gross_edge_usd = sell_quote_received_usd - buy_quote_paid_usd
strict_net_edge_usd = gross_edge_usd - strict_nonembedded_cost_usd
research_net_edge_usd = gross_edge_usd
                        - strict_nonembedded_cost_usd
                        - research_estimated_cost_usd
                        - research_assumed_cost_usd
```

Pool swap fees embedded in leg quotes are recorded but excluded from every
nonembedded total exactly once. Output the canonical stored enum
`executable_candidate`, `research_estimate`, or `unavailable` with a single
primary reason and structured component reasons. Positive strict net edge is
required for executable ranking.

- [ ] **Step 5: Run opportunity tests**

Run: `python3 -m unittest tests.test_route_opportunity -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_opportunity.py tests/test_route_opportunity.py
git commit -m "feat(opportunities): calculate strict and estimated route edge"
```

Add a GitHub commit comment containing common-quantity and 60-second evidence.

### Task 7: Complete immutable opportunity bundle

**Files:**
- Modify: `scripts/route_publication.py`
- Modify: `scripts/collect_route_cohort.py`
- Modify: `tests/test_route_publication.py`
- Modify: `tests/test_route_collection.py`
- Modify: `docs/route-cohort-data-contract.md`

**Interfaces:**
- Consumes: one validated `route_cohort_core/v1` bundle plus quantity quotes,
  cost components, and classified opportunities from Tasks 1–6.
- Produces: `build_complete_route_bundle()`,
  `publish_complete_route_bundle()`, and one new immutable
  `route_opportunity/v1` bundle selected by
  `data/local/routes/latest.json`.

- [ ] **Step 1: Write failing complete-inventory and lineage tests**

Assert the final directory contains `route_legs.csv`, `cost_components.csv`,
`route_opportunities.csv`, SQLite, and a manifest. Require exact cohort,
market, route, opportunity, notional, component, count, and source-generation
parity across CSV/SQLite/manifest. Every opportunity must reference two
published legs and the exact set of published cost components.

- [ ] **Step 2: Write immutable-finalization fault tests**

Simulate validation failure, source-core mutation, duplicate final cohort ID,
partial file write, final-directory rename failure, reread failure, and pointer
replacement failure. The old core bundle and old final pointer must remain
unchanged. A retry creates or selects a new immutable final bundle; it never
fills cost/opportunity files into the core directory.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_route_publication tests.test_route_collection -v`

Expected: FAIL because complete-bundle finalization is absent.

- [ ] **Step 4: Implement deterministic final assembly**

Sort all logical rows by canonical identity before serialization. Bind the
final manifest to the core manifest hash and all input generations, write and
fsync in a hidden same-filesystem staging directory, validate by full reread,
rename once, then atomically replace `data/local/routes/latest.json`. The public
API must only follow this complete pointer, never the core pointer.

- [ ] **Step 5: Run publication and orchestration regressions**

Run: `python3 -m unittest tests.test_route_publication tests.test_route_collection tests.test_route_opportunity tests.test_execution_cost_components -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/route_publication.py scripts/collect_route_cohort.py tests/test_route_publication.py tests/test_route_collection.py docs/route-cohort-data-contract.md
git commit -m "feat(routes): publish complete opportunity bundles"
```

Add a GitHub commit comment with immutable-core preservation and failure-
atomic pointer evidence.

### Task 8: Contract integration and release counterexamples

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/market-monitor-design.md`
- Modify: `docs/collection-operations.md`
- Test: `tests/test_release_smoke.py`
- Test: `tests/test_publication_gate.py`

**Interfaces:**
- Consumes: Tasks 1–7 and the route bundle plan.
- Produces: fail-closed release checks for quantity, costs, strictness, and provenance.

- [ ] **Step 1: Add malformed-opportunity counterexamples**

Mutate common quantity on one leg, remove a required component, mark an
assumption strict, double-count a pool fee, exceed skew/age, and inject a
credential sentinel. Each mutation must fail release validation.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke tests.test_publication_gate -v`

Expected: FAIL because opportunity validation is absent.

- [ ] **Step 3: Integrate strict contract validation**

Validate scenario grids, exact arithmetic, component inventory, source hashes,
strict/estimate separation, no secret material, and manifest count/hash parity.

- [ ] **Step 4: Run contract and release suites**

Run: `python3 -m unittest tests.test_execution_cost_components tests.test_cex_fee_facts tests.test_dex_route_costs tests.test_route_opportunity tests.test_release_smoke tests.test_publication_gate -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/check_dashboard_release.py docs/market-monitor-design.md docs/collection-operations.md tests/test_release_smoke.py tests/test_publication_gate.py
git commit -m "test(opportunities): enforce all-in cost provenance"
```

Add a GitHub commit comment listing every counterexample covered.
