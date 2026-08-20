# Historical Foundry Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert one selected immutable Foundry replay into the existing Opportunity economics without trusting stored profits, publish an isolated historical private core and six-file complete bundle, and move the historical pointer only after full connected verification.

**Architecture:** A sealed historical-publication wrapper validates its identity-sealed build context, descriptor-rereads every bound raw/proof member, and issues one sentinel-guarded immutable `ValidatedHistoricalScenarioInputs` capability per scenario. The pure bridge accepts only those capabilities or its own immutable projections and deterministically derives the ten Opportunity inputs, ninety cost rows, and compact replay evidence without filesystem access. Shared low-level CSV/SQLite/hash/descriptor validators are extracted beneath unchanged live wrappers. Dedicated historical core and complete-bundle writers use separate schemas, roots, raw readers, and an identity-sealed build context. A fresh-process connected verifier re-fetches the full seven-day inventory and replays the selected ten plus every newer replay-required scenario, then closes the report/pointer hash graph before atomic publication.

**Tech Stack:** Python 3.8.10+ standard library, existing route quantity/opportunity/publication modules, canonical CSV/SQLite/JSON, descriptor-safe publication primitives, archive JSON-RPC, pinned Anvil/Foundry.

**Spec:** `docs/superpowers/specs/2026-08-20-historical-foundry-replay-opportunity-design.md` sections “Cost-component topology”, “Compact replay evidence”, “Bridge into Opportunity economics”, “Historical publication”, “Error model”, and “Historical publication” TDD matrix.

## Global Constraints

- The bridge is pure: no filesystem, path, descriptor, `HistoricalReplayBuildContext`, network, subprocess, clock, URL, caller policy, serialized quote, serialized target, profit, status, or publication profile input. Scenario economics accept only a sentinel-issued immutable `ValidatedHistoricalScenarioInputs` capability or an immutable projection returned from one.
- Historical core and complete paths are separate from live paths. Historical wrappers never call live wrappers, and live wrappers cannot accept historical schemas, roots, inventories, profiles, or timing.
- Existing live CSV/SQLite row schemas and live bundle bytes remain unchanged. Foundry-only fields live only in `replay_evidence.json`.
- Historical V2 economics replay both legs as exact-input cashflows: the first leg consumes the receipt-bound WETH input and the second leg consumes the first leg's exact integer UNI output. The historical bridge never substitutes the live buy-side exact-output inversion, and it does not add a parameter, profile, or evidence kind to a live quote/Opportunity wrapper.
- Exactly one route-level gas row is charged per atomic scenario. There is no leg-level duplicate gas. Historical atomic topology is nine rows; the live topology stays byte-for-byte unchanged.
- The historical nine-row contract includes exact `value_status` and `embedded_in_leg_quote` values, not only component keys. Producer, complete publication, published reader, dashboard projection, and both release-checker paths enforce the same matrix.
- The staged and committed historical core are both fully validated. Only module-private loaders can construct `HistoricalReplayBuildContext`, and identical staged/committed bytes yield byte-equivalent context projections.
- `scripts/route_cost_topology.py` never imports `HistoricalReplayBuildContext` or `scripts/historical_route_publication.py`. Its historical validator is module-private and context-free. Only sealed wrappers in `scripts/historical_route_publication.py` may invoke it: the writer wrapper holds and validates `HistoricalReplayBuildContext`; the reader wrapper holds and validates the pointer/manifest view. Dashboard and release consume the reader's validated result and never import the low-level validator.
- Connected verification can confirm or reject the selected block but cannot choose a replacement or move a pointer.
- Publish and dry-run share all logical validation. Dry-run moves neither core nor complete pointer and installs no public verification report.
- The exact execution claim is `historical_counterfactual_state_override_next_block`. Phase-3 terminal reasons additionally close to `positive_gate_failed`, `historical_bundle_invalid`, and `publication_race`; all retain only closed codes and leave the prior public pointer unchanged.

## Task 1: Centralize the Closed Cost-Component Matrices Without a Context Dependency

**Files:**

- Create: `scripts/route_cost_topology.py`
- Create: `tests/test_route_cost_topology.py`
- Modify: `scripts/route_opportunity.py`
- Modify: `scripts/route_publication.py`
- Modify: `dashboard/opportunity_facts.py`
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_route_opportunity.py`
- Modify: `tests/test_route_publication.py`
- Modify: `tests/test_opportunity_api.py`
- Modify: `tests/test_release_smoke.py`

- [ ] **Step 1: Add cross-consumer topology RED tests**

Freeze the live output from current behavior and the historical atomic output from the design:

```python
LIVE_DEX_DEX_KEYS = {
    ("buy", "pool_swap_fee"),
    ("buy", "network_gas"),
    ("buy", "router_or_integrator_fee"),
    ("buy", "token_transfer_tax"),
    ("sell", "pool_swap_fee"),
    ("sell", "network_gas"),
    ("sell", "router_or_integrator_fee"),
    ("sell", "token_transfer_tax"),
    ("route", "rebalancing_or_transfer"),
    ("route", "mev_buffer"),
}

HISTORICAL_ATOMIC_COMPONENT_MATRIX = (
    # leg, component_type, value_status, embedded_in_leg_quote
    ("buy", "pool_swap_fee", "bounded_estimate", True),
    ("buy", "router_or_integrator_fee", "bounded_estimate", False),
    ("buy", "token_transfer_tax", "bounded_estimate", False),
    ("sell", "pool_swap_fee", "bounded_estimate", True),
    ("sell", "router_or_integrator_fee", "bounded_estimate", False),
    ("sell", "token_transfer_tax", "bounded_estimate", False),
    ("route", "network_gas", "assumed", False),
    ("route", "rebalancing_or_transfer", "not_applicable", False),
    ("route", "mev_buffer", "assumed", False),
)
```

Freeze these module interfaces:

```python
HistoricalAtomicComponentShape = Tuple[str, str, str, bool]

def live_complete_cost_component_keys(
    route: Mapping[str, Any],
) -> FrozenSet[Tuple[str, str]]: ...

def _validate_historical_atomic_cost_component_matrix(
    route: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_cohort_id: str,
    expected_opportunity_id: str,
    expected_pool_fee_source_sha256_by_leg: Mapping[str, str],
    expected_pool_fee_amount_usd_by_leg: Mapping[str, str],
    expected_zero_fee_proof_sha256_by_key: Mapping[Tuple[str, str], str],
    expected_gas_amount_usd: str,
    expected_gas_source_sha256: str,
    expected_mev_amount_usd: str,
    expected_policy_sha256: str,
) -> None: ...
```

`live_complete_cost_component_keys()` preserves the current live inventory exactly. `_validate_historical_atomic_cost_component_matrix()` is module-private, context-free, and is a closed validator rather than a profile selector: it accepts no schema/stage/profile/context/member list and requires `route_mode == "atomic_onchain"`, exactly the nine unique matrix rows above, and exact cohort/opportunity lineage. It additionally enforces:

- both pool-fee rows have rate 30 bps, exact `amount_usd == expected_pool_fee_amount_usd_by_leg[leg]`, the exact expected fee-proof SHA for their leg, and are embedded; their informational amounts never enter the nonembedded sum;
- router and transfer-tax rows have exact amount/rate zero and the expected receipt/balance/adapter proof bound by `source_record_sha256`; they remain `bounded_estimate`, not `not_applicable`;
- route gas equals `expected_gas_amount_usd`, binds `expected_gas_source_sha256`, and is not embedded;
- route transfer uses `source="validated route topology"`, is `not_applicable`, has null amount/rate, and is proved by `atomic_onchain` mode;
- MEV equals `expected_mev_amount_usd`, is nonembedded, binds `expected_policy_sha256`, and independently recomputes to requested notional times exactly 10 bps divided by 10,000 for the frozen MVP policy; and
- every row has `strict_eligible == False`.

Freeze the only two call edges, implemented later in `scripts/historical_route_publication.py`:

```python
def _validate_historical_cost_rows_for_build_context(
    *,
    context: "HistoricalReplayBuildContext",
    route: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    scenario_inputs: "ValidatedHistoricalScenarioInputs",
) -> None: ...

def _validate_historical_cost_rows_for_published_view(
    *,
    validated_view: "ValidatedHistoricalReplayBundleView",
    route: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    proof_inputs: "ValidatedHistoricalCostProofInputs",
) -> None: ...
```

The first wrapper accepts only a scenario capability issued after the held build context and exact Phase-2 proof object were validated. It revalidates the capability sentinel, scenario/context binding, and held descriptors before deriving scalar expectations and calling the low-level validator; the second validates the held pointer/report/manifest view and exact retained proof object first. Producer and writer use the first wrapper. The published reader uses the second. Dashboard and both release paths consume only the reader's validated view. Add a spy/event-order regression proving writer order `context_validated`, `raw_descriptors_reread`, `proof_inputs_validated`, `scenario_capability_issued`, `pure_bridge_called`, `capability_current_rechecked`, `low_level_matrix_called`; the reader retains `view_validated`, `proof_inputs_validated`, `low_level_matrix_called`. Forged/stale subjects, forged/transplanted capabilities, and bad proof hashes must fail before the low-level call count changes.

The topology module must not import `scripts.historical_route_publication`, and `dashboard/opportunity_facts.py` plus `scripts/check_dashboard_release.py` must not import the module-private historical validator; verify both directions in `tests/test_route_cost_topology.py`. Reject private profile strings, arbitrary component inventories, missing/duplicate/extra keys, every amount/status/embedded mutation, and historical rows passed through a live manifest. Add a live fixture byte snapshot before refactoring and assert identical output afterward.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_route_cost_topology -v
```

Expected: missing shared topology module and historical profile.

- [ ] **Step 3: Implement sealed topology derivation**

Use private singleton identities only inside the publication modules that already validated their context or manifest. Do not put a context class, proof-input issuer, sealed wrapper, or context factory in `route_cost_topology.py`; the exact matrix and its low-level validator are context-free so Task 1 can precede Task 4 without an import or implementation cycle.

Refactor the current live paths in `route_opportunity.py`, `route_publication.py`, `dashboard/opportunity_facts.py`, and the release checker to delegate only to `live_complete_cost_component_keys()`. Delete their private copied live expected-key sets. Historical delegation is added solely through the sealed publication wrappers in Tasks 4–6. Keep the signatures of `build_route_opportunity`, `_validate_complete_route_bundle_at`, `_complete_manifest_payload`, `_complete_artifact_bytes`, and `load_latest_complete_route_bundle` unchanged and add a signature regression for each live wrapper.

- [ ] **Step 4: Run GREEN and live regressions**

```bash
python3 -m unittest \
  tests.test_route_cost_topology \
  tests.test_route_opportunity \
  tests.test_route_publication \
  tests.test_opportunity_api \
  tests.test_release_smoke -v
```

- [ ] **Step 5: Commit the topology extraction**

```bash
git add scripts/route_cost_topology.py \
  scripts/route_opportunity.py scripts/route_publication.py \
  dashboard/opportunity_facts.py scripts/check_dashboard_release.py \
  tests/test_route_cost_topology.py tests/test_route_opportunity.py \
  tests/test_route_publication.py tests/test_opportunity_api.py \
  tests/test_release_smoke.py
git commit -m "refactor(opportunity): centralize component topology"
```

## Task 2: Add Exact-Input V2 Integer Math Without Expanding Live Wrappers

**Files:**

- Modify: `scripts/route_quantity.py`
- Modify: `tests/test_route_quantity.py`

- [ ] **Step 1: Write exact-input RED tests**

The existing live V2 buy quote performs exact-output inversion and must remain unchanged. Freeze only this lower-level exact-input integer primitive:

```python
def v2_exact_input_amount_out_raw(
    *,
    reserve_in_raw: int,
    reserve_out_raw: int,
    amount_in_raw: int,
    fee_numerator: int,
    fee_denominator: int,
) -> int: ...
```

Cover token0/token1 reserve order at the historical caller, integer floor, zero/negative/boolean inputs, reserve exhaustion, one-wei changes, and exact-input/output plateau divergence. Freeze this exact raw-unit known-answer test:

```text
buy pool reserve_in(WETH)=10, reserve_out(UNI)=10
receipt-bound exact input WETH=4 -> exact output UNI=2
legacy live exact-output inversion for target UNI=2 -> WETH debit=3
sell pool reserve_in(UNI)=10, reserve_out(WETH)=20
second-leg exact input UNI=2 -> exact output WETH=3
```

The test must assert the old inversion understates the actual first-leg debit by exactly one raw wei and therefore would overstate gross cashflow. It must also assert the second leg consumes the exact first-leg output `2`, not a requested target or a value recovered by inversion.

Do not add `dex_v2_exact_input` to `route_opportunity._validated_quote_evidence`. Do not change the signatures or accepted evidence kinds of `quote_v2_pool_quantity`, `validate_v2_quantity_quote_against_state`, or `build_route_opportunity`. Snapshot their live known-answer objects and bytes before the refactor and require exact equality afterward. The dedicated historical builder is added in Task 5.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_route_quantity.V2ExactInputIntegerMathTests \
  tests.test_route_quantity.V2PoolQuantityQuoteTests -v
```

- [ ] **Step 3: Implement exact-input quote and validation**

Implement `amount_in_raw * fee_numerator * reserve_out_raw // (reserve_in_raw * fee_denominator + amount_in_raw * fee_numerator)` with checked positive integers and an output strictly between zero and `reserve_out_raw`. Refactor only the existing live sell-side internal calculation to call this primitive; the live buy-side exact-output inversion stays unchanged.

- [ ] **Step 4: Run GREEN plus live regressions**

```bash
python3 -m unittest \
  tests.test_route_quantity \
  tests.test_route_publication -v
```

- [ ] **Step 5: Commit exact-input support**

```bash
git add scripts/route_quantity.py tests/test_route_quantity.py
git commit -m "feat(opportunity): validate V2 exact-input cashflow"
```

## Task 3: Replay the Raw Run Into a Fixed Historical Research Universe

**Files:**

- Create: `scripts/historical_foundry_replay.py`
- Create: `tests/test_historical_foundry_replay.py`
- Reference: `scripts/route_universe.py`
- Reference: `scripts/route_quantity.py`
- Reference: `scripts/route_cost_evidence.py`
- Reference: `scripts/route_opportunity.py`

- [ ] **Step 1: Write pure bridge RED tests**

Freeze narrow functions:

```python
def validate_selected_historical_run(
    *,
    config: HistoricalFoundryConfigSet,
    run_evidence: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def build_historical_research_universe(
    *,
    config: HistoricalFoundryConfigSet,
    validated_run: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def build_historical_core_projection(
    *,
    config: HistoricalFoundryConfigSet,
    validated_run: Mapping[str, Any],
    universe: Mapping[str, Any],
) -> Mapping[str, Any]: ...
```

Tests require exact fixed Uniswap/Sushi market IDs, pair addresses derived from captured factories, two opposite route IDs, exact UNI/WETH token identities, selected-block reserves, V2 depth/TVL projections, null 24-hour/route volume, exact `historical_replay` temporal scope, exact execution claim, and a 30-calendar-day provenance window ending on the anchor UTC date. Assert that this provenance window is not exposed as measured 30-day volume coverage.

Transplant or rehash attacks against run/policy/authority/toolchain/selection/block/pair/token/route/direction must fail after independent recomputation.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_replay.HistoricalResearchUniverseTests -v
```

- [ ] **Step 3: Implement the universe/core projection**

Reuse existing canonical route-ID, market-ID, V2 quantity, selected-market hashing, depth, and TVL arithmetic where the semantics match. Do not invoke live route-shadow baseline/ranking/phase/joint validators. The I/O entrypoint descriptor-rereads the two markets' retained `dex_pool_state` and `dex_usd_price_context` members and supplies their immutable canonical bytes with the selected-block reserve/feed projection. The pure functions independently rebuild the expected bytes in memory, require exact equality, and return normalized immutable projections; they never open, stat, reread, or write a member.

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest \
  tests.test_historical_foundry_replay \
  tests.test_route_universe \
  tests.test_route_quantity -v
```

- [ ] **Step 5: Commit the universe bridge**

```bash
git add scripts/historical_foundry_replay.py \
  tests/test_historical_foundry_replay.py
git commit -m "feat(opportunity): derive historical research universe"
```

## Task 4: Publish and Load the Isolated Historical Private Core

**Files:**

- Create: `scripts/historical_route_publication.py`
- Create: `tests/test_historical_route_publication.py`
- Modify: `scripts/route_publication.py`
- Modify: `tests/test_route_publication.py`

- [ ] **Step 1: Write historical-core RED tests**

Freeze exact paths/schemas and functions:

```python
def stage_historical_replay_core(
    *,
    data_dir: Path,
    raw_root: Path,
    core_projection: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def publish_historical_replay_core(
    *,
    data_dir: Path,
    staged_core: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def load_validated_historical_replay_core_at(
    *,
    data_dir: Path,
    raw_root: Path,
    staged_bundle: Path,
    prospective_pointer_bytes: bytes,
) -> "HistoricalReplayBuildContext": ...

def load_latest_historical_replay_core(
    *,
    data_dir: Path,
    raw_root: Path,
) -> "HistoricalReplayBuildContext": ...
```

The context class constructor is sentinel-guarded and its `repr` omits paths/member bytes. Tests cover exact pointer `route_historical_replay_core_pointer/v1`, stage `route_historical_replay_core/v1`, manifest `route_historical_replay_core_manifest/v1`, five core files, historical raw-root reread, staged/committed byte-equivalence, forged context rejection, and live wrappers rejecting every historical artifact.

Use a historical-specific cohort validator and internal `run_directory_name(run_id)`. Do not call or relax the live `_normalize_and_validate_cohort`, whose `_SAFE_RUN_ID` intentionally rejects historical `run:<64hex>` identifiers.

Snapshot `routes/core/latest.json` and `routes/latest.json` before every test and require exact equality afterward.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_route_publication.HistoricalCorePublicationTests -v
```

- [ ] **Step 3: Extract shared primitives beneath unchanged live wrappers**

Keep `_validate_complete_route_bundle_at`, `_complete_manifest_payload`, `_complete_artifact_bytes`, `publish_route_cohort_bundle`, and `load_latest_route_cohort` live-only with unchanged signatures. Extract descriptor/CSV/SQLite/hash/foreign-key/row primitives beneath them, then build dedicated historical wrappers. A historical wrapper must never call a live wrapper.

The staged loader validates exact prospective pointer bytes without reading/moving `latest.json`. The committed loader follows the historical-core pointer. Both reread the raw run's typed members through explicit roots and bind core pointer bytes/SHA, manifest/stage, policy/authority/toolchain, selection, selected block, and historical source reader.

- [ ] **Step 4: Run GREEN including live byte parity**

```bash
python3 -m unittest \
  tests.test_historical_route_publication \
  tests.test_route_publication -v
```

- [ ] **Step 5: Commit the core publication slice**

```bash
git add scripts/historical_route_publication.py scripts/route_publication.py \
  tests/test_historical_route_publication.py tests/test_route_publication.py
git commit -m "feat(opportunity): publish isolated historical core"
```

## Task 5: Recompute Ten Scenarios, Ninety Costs, and Compact Replay Evidence

**Files:**

- Modify: `scripts/historical_foundry_replay.py`
- Modify: `scripts/historical_route_publication.py`
- Modify: `tests/test_historical_foundry_replay.py`
- Modify: `tests/test_historical_route_publication.py`
- Reference: `scripts/route_quantity.py`
- Reference: `scripts/route_opportunity.py`
- Reference: `scripts/route_cost_topology.py`

- [ ] **Step 1: Write economic-bridge RED tests**

Freeze the capability issuer in `scripts/historical_route_publication.py` and
the capability-consuming arithmetic in `scripts/historical_foundry_replay.py`:

```python
@dataclass(frozen=True)
class ValidatedHistoricalCostProofInputs:
    scenario_key: str
    proof_inputs_hash: str
    object_value: Mapping[str, Any]

def load_historical_cost_proof_inputs_for_build_context(
    *,
    context: "HistoricalReplayBuildContext",
    scenario_key: str,
) -> ValidatedHistoricalCostProofInputs: ...

@dataclass(frozen=True, init=False)
class ValidatedHistoricalScenarioInputs:
    scenario_key: str
    context_projection_sha256: str
    source_descriptor_set_sha256: str
    proof_inputs_hash: str
    canonical_projection_bytes: bytes

def _issue_validated_historical_scenario_inputs(
    *,
    context: "HistoricalReplayBuildContext",
    scenario_key: str,
) -> ValidatedHistoricalScenarioInputs: ...

def _require_historical_scenario_inputs_current(
    *,
    context: "HistoricalReplayBuildContext",
    inputs: ValidatedHistoricalScenarioInputs,
) -> None: ...

def _build_historical_scenario_for_publication(
    *,
    context: "HistoricalReplayBuildContext",
    scenario_key: str,
) -> Mapping[str, Any]: ...

def build_historical_atomic_v2_cashflow(
    inputs: ValidatedHistoricalScenarioInputs,
) -> Mapping[str, Any]: ...

def build_historical_route_opportunity(
    inputs: ValidatedHistoricalScenarioInputs,
) -> Mapping[str, Any]: ...

def build_historical_scenario_projection(
    inputs: ValidatedHistoricalScenarioInputs,
) -> bytes: ...

def build_historical_replay_evidence(
    canonical_scenario_projection_bytes: Tuple[bytes, ...],
) -> bytes: ...
```

`ValidatedHistoricalCostProofInputs` and `ValidatedHistoricalScenarioInputs` have module-private issuer sentinels and cannot be caller-constructed, copied with `dataclasses.replace`, unpickled, or transplanted across a scenario/context. The scenario capability stores only canonical immutable bytes and digests; its `repr` omits member bytes, paths, and sentinels. The concrete class and private constructor live beside the pure bridge so the bridge can reject any wrong concrete type or sentinel without importing publication; the only authorized call edge to that constructor is `_issue_validated_historical_scenario_inputs()` in historical publication. Publication imports the pure bridge in one direction only.

`_issue_validated_historical_scenario_inputs()` first validates the held `HistoricalReplayBuildContext`, then uses its historical raw reader to descriptor-reread every scenario dependency: run/policy/authority/toolchain/selection/block members, both pool/feed members, overlay, receipt, trace, result, balance deltas, executor/adapter proof, and the exact Phase-2 proof object. The proof object schema is exactly `historical_foundry_cost_proof_inputs/v1`; its fields are exactly `schema`, `scenario_key`, `policy_sha256`, `receipt_sha256`, `trace_sha256`, `adapter_proof_sha256`, `rows`, and `proof_inputs_hash`. Recompute `proof_inputs_hash` as the typed hash over the preceding seven fields and require equality before issuing the scenario capability. Its `rows` value is the exact ordered nine-row array produced by Phase 2. Each proof row has exactly `grain`, `component`, `value_status`, `embedded`, `amount_usd_exact`, `rate_bps_exact`, `proof_role`, and `proof_sha256`.

The sealed `_build_historical_scenario_for_publication()` order is normative: `context_validated` -> `raw_descriptors_reread` -> `proof_inputs_validated` -> `scenario_capability_issued` -> `pure_bridge_called` -> `capability_current_rechecked` -> `low_level_matrix_called` -> `rows_serialized_or_returned`. `_require_historical_scenario_inputs_current()` descriptor-rereads the held ancestry after pure computation and before matrix validation, and rejects if any bound member identity, byte count, physical SHA, context projection, scenario key, or `proof_inputs_hash` changed. Thus a concurrent source mutation cannot publish a result computed from a stale capability.

Phase 3 projects those exact proof values into the immutable scenario capability and may not reconstruct, rename, reorder, normalize from a private variant, or infer them from public cost rows. Map `grain -> leg`, `component -> component_type`, `embedded -> embedded_in_leg_quote`, `amount_usd_exact -> amount_usd`, and `rate_bps_exact -> rate_bps` one-to-one; the public row proof source must equal the proof row's exact role/SHA. Bind `proof_inputs_hash` into the corresponding compact replay scenario and therefore into the scenario-set and complete-manifest hashes.

The four functions in `scripts/historical_foundry_replay.py` are deterministic calculations over the sealed capability or the canonical immutable bytes they return. They accept no `HistoricalReplayBuildContext`, config object, path, descriptor, raw/proof reader, mutable mapping, filesystem/network/subprocess/clock/RPC handle, or callback. `build_historical_atomic_v2_cashflow()` reads its selected-block pool state and receipt/trace/balance values only from `canonical_projection_bytes`, verifies the capability's concrete type, sentinel, and projection digest before arithmetic, then executes these two exact-input steps in order:

1. first-leg `weth_in_raw` is the transaction/overlay-bound requested input; recompute `uni_out_raw = v2_exact_input_amount_out_raw(...)` against the buy pool;
2. second-leg `uni_in_raw` must equal that exact `uni_out_raw`; recompute final `weth_out_raw = v2_exact_input_amount_out_raw(...)` against the sell pool.

Require both integer outputs, both pool post-state reserves, executor balance deltas, trace call amounts, receipt status, direction, route, scenario, and requested notional to match retained evidence. Reject an input recovered by applying the live exact-output inversion to `uni_out_raw`, even if that inversion differs by only one raw wei. Run the exact `10/10, input 4 -> output 2, legacy inverse 3; 10/20, input 2 -> output 3` KAT from Task 2 through this builder for both route directions.

`build_historical_route_opportunity()` is historical-only and consumes only the same sealed scenario capability; it recomputes its cashflow internally rather than accepting a caller-supplied cashflow or cost list. It writes the existing Opportunity row schema but does not call or alter the live public `build_route_opportunity()` wrapper and does not introduce a new live quote-evidence kind. It independently rechecks `gross_edge = final_weth_out - first_weth_in`, USD conversion, nonembedded cost sum, research net, bps denominators, research-only classification, and evidence binding.

Require two routes, five notionals each, ten scenario IDs, ten status-one receipts, ten independently validated `historical_foundry_cost_proof_inputs/v1` objects, ten bound `proof_inputs_hash` values, exactly ninety cost rows, and the exact nine-row `(leg, component_type, value_status, embedded_in_leg_quote)` matrix for every scenario. Require exactly one route gas row, no leg gas, embedded 30-bps pool fees whose informational public `amount_usd` exactly equals the proof row's `amount_usd_exact`, adapter-proved bounded-estimate zero router/tax, proved route transfer N/A, exact receipt gas cost, and policy-bound assumed MEV. Require baseline p50+10, stress p90+25/50, and at least one exact positive baseline/research net. All ten rows remain `research_estimate`; strict/executable/attested/unavailable counts are zero.

Attack every Foundry-supplied projection: first-leg input/output, second-leg input/output, one-wei legacy-inversion substitution, profit, gas, balance delta, direction, status, route ID, opportunity ID, proof-input schema/field/order/hash, any of nine proof rows, either pool-fee `amount_usd_exact`, component amount/status/embedded bit, stress value, or positive flag. Rehashing all dependent stored hashes must still fail because the publication wrapper independently descriptor-rereads raw receipt/trace/balance/authority bytes and the sealed proof-input loader rejects before capability issuance or the low-level topology validator.

Add `test_pure_bridge_rejects_forged_scenario_capability_before_arithmetic`, `test_publication_rejects_transplanted_scenario_capability_before_pure_bridge`, `test_publication_rejects_stale_capability_before_matrix_and_serialization`, `test_same_capability_produces_byte_identical_projection`, `test_equivalent_staged_and_committed_capabilities_produce_identical_projection`, and `test_pure_bridge_performs_no_io_clock_rpc_or_subprocess`. The no-I/O test patches `builtins.open`, `pathlib.Path.open/stat/read_bytes`, `os.open/stat`, `socket.socket`, `subprocess.run/Popen`, and `time.time/time_ns` to raise, then executes all four pure functions twice. A mutation injected after capability issuance must leave the pure result byte-identical to its first result but cause `_require_historical_scenario_inputs_current()` to fail before the matrix-call and serialization spies fire.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_replay.HistoricalOpportunityBridgeTests -v
```

- [ ] **Step 3: Implement pure economics and compact evidence**

Use the selected block's feed answer for both WETH/USD conversions, exact receipt `gasUsed * effectiveGasPrice`, policy MEV, and exact fixed-point serialization. Require no executed `GASPRICE` opcode in relevant contracts before reusing gas units for stress. For each scenario, the publication wrapper loads and validates the exact Phase-2 proof object, derives the immutable scalar/byte projection, and issues the scenario capability. The pure bridge derives cost rows and Opportunity output only from that capability. After the pure call, publication rechecks descriptor currentness and derives the shared validator's two pool-fee amounts and SHAs, four zero-fee proof SHAs, gas amount/source SHA, and MEV amount/policy SHA only from the same capability; never derive expected values from the public rows being checked. Call `_validate_historical_cost_rows_for_build_context()` only after that recheck. Validate the same matrix again over the completed ten-scenario inventory, then build the closed ten-row `historical_foundry_replay_evidence/v1` projection with each exact `proof_inputs_hash` plus overlay-set/scenario-set digests. Candidate status-zero evidence remains raw-only.

Invoke `build_historical_route_opportunity` with the selected state-block timestamp, not current time. Because the Chainlink policy boundary is inclusive at 3,600 seconds while existing typed `valid_until` checks are exclusive, project the historical typed validity boundary as `updated_at + 3601 seconds`; test age 3600 accepted and 3601 rejected without routing through the live wrapper.

Do not modify the signature or accepted evidence contract of `build_route_opportunity`, `quote_v2_pool_quantity`, or `_validated_quote_evidence`. Add byte/object snapshot assertions proving their live known answers remain unchanged.

- [ ] **Step 4: Run GREEN and topology parity**

```bash
python3 -m unittest \
  tests.test_historical_foundry_replay \
  tests.test_route_cost_topology \
  tests.test_route_quantity \
  tests.test_route_opportunity -v
```

- [ ] **Step 5: Commit the economics slice**

```bash
git add scripts/historical_foundry_replay.py \
  scripts/historical_route_publication.py \
  tests/test_historical_foundry_replay.py \
  tests/test_historical_route_publication.py
git commit -m "feat(opportunity): build historical replay economics"
```

## Task 6: Stage and Validate the Six-File Historical Complete Bundle

**Files:**

- Modify: `scripts/historical_route_publication.py`
- Modify: `scripts/route_publication.py`
- Modify: `tests/test_historical_route_publication.py`
- Modify: `tests/test_route_publication.py`

- [ ] **Step 1: Write complete-bundle RED tests**

Freeze:

```python
def stage_historical_replay_bundle(
    *,
    data_dir: Path,
    raw_root: Path,
    context: "HistoricalReplayBuildContext",
) -> Mapping[str, Any]: ...

def validate_historical_replay_bundle(
    *,
    data_dir: Path,
    raw_root: Path,
    bundle_path: Path,
    expected_pointer_core: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]: ...

@dataclass(frozen=True)
class ValidatedHistoricalReplayBundleView:
    replay_id: str
    route_cohort_id: str
    manifest_sha256: str

def _load_historical_cost_proof_inputs_for_published_view(
    *,
    validated_view: "ValidatedHistoricalReplayBundleView",
    scenario_key: str,
) -> "ValidatedHistoricalCostProofInputs": ...
```

The stage wrapper accepts no caller-built Opportunity/cost/replay mapping. It enumerates the context-bound ten scenario keys, issues each scenario capability through `_issue_validated_historical_scenario_inputs()`, calls the pure bridge, rechecks capability currentness, validates the exact matrix, and only then serializes. Require exactly six files total: three CSVs, SQLite, `replay_evidence.json`, and manifest. Manifest inventories the other five and binds exact historical core manifest/pointer SHA, policy/authority/toolchain/run/selection/block, temporal/execution claims, five notionals, 2/2/10/90/10 counts, ten research, zero unavailable/strict/executable/attested, and positive count >=1.

`ValidatedHistoricalReplayBundleView` is module-private/sentinel-issued only after pointer/report/manifest, immutable core, replay-evidence join, and retained raw-member descriptors are all held and validated. The proof loader follows the compact row's exact `proof_inputs_hash` to the referenced retained `result.json`, consumes its exact `historical_foundry_cost_proof_inputs/v1` object, recomputes the typed hash, and independently rechecks receipt/trace/adapter/policy evidence before issuing `ValidatedHistoricalCostProofInputs`.

Test CSV/SQLite parity, stable canonical row ordering, no Foundry field added to current row schemas, exact one-to-one replay-evidence join by opportunity ID, immutable core resolution after historical core latest advances, missing/extra/orphan/duplicate/mutated members, live-wrapper rejection, symlink/hardlink/traversal/TOCTOU, and live bundle byte parity. For each of the ten scenarios, independently mutate every one of the nine expected keys, every `value_status`, every `embedded_in_leg_quote` bit, either pool-fee public amount, the compact `proof_inputs_hash`, the raw proof object's hash, field set/order, and each proof row. Stage validation and public read validation must both reject even after all attacker-controlled descendant hashes are recomputed. Explicitly cover bounded-estimate zero router/tax versus forbidden `not_applicable`, embedded pool fee amount parity versus double subtraction, route-only gas, proved transfer N/A, and assumed nonembedded MEV.

Instrument the sealed wrappers in tests and assert exact writer event order: `context_validated`, `raw_descriptors_reread`, `proof_inputs_validated`, `scenario_capability_issued`, `pure_bridge_called`, `capability_current_rechecked`, `low_level_matrix_called`, `rows_serialized_or_returned`. Reader order is `view_validated`, `proof_inputs_validated`, `low_level_matrix_called`, `rows_returned`. A forged/stale context/view/capability, mismatched `proof_inputs_hash`, or pool-fee amount tamper must leave all later call counters at zero.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_route_publication.HistoricalCompleteBundleTests -v
```

- [ ] **Step 3: Implement sealed historical complete profile**

Extract shared CSV/SQLite/logical primitives, but leave the existing live wrapper signatures and outputs unchanged. The historical writer alone holds `HistoricalReplayBuildContext`; it validates context, descriptor-rereads all scenario raw/proof inputs, issues `ValidatedHistoricalScenarioInputs`, calls the context-free pure bridge, and rechecks capability ancestry/currentness before `_validate_historical_cost_rows_for_build_context()` derives the two pool-fee expected amounts and all other scalar expectations and invokes the context-free low-level matrix validator. The reader selects historical semantics only after pointer schema/report/manifest/core/raw validation creates `ValidatedHistoricalReplayBundleView`; `_validate_historical_cost_rows_for_published_view()` loads the exact retained proof capability, derives expectations from that capability plus manifest-bound policy, and only then invokes the same low-level validator before returning normalized objects. No expected amount/hash may be copied from the public cost row under test. Dashboard/release never call the low-level validator. No caller profile/member list/timing policy/source-root default exists, and neither historical path creates an import cycle back from topology to context or from the pure bridge back to publication.

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest \
  tests.test_historical_route_publication \
  tests.test_route_publication \
  tests.test_route_opportunity -v
```

- [ ] **Step 5: Commit the complete-bundle slice**

```bash
git add scripts/historical_route_publication.py scripts/route_publication.py \
  tests/test_historical_route_publication.py tests/test_route_publication.py
git commit -m "feat(opportunity): stage historical replay bundle"
```

## Task 7: Build Connected Verification and the Report/Pointer Closure

**Files:**

- Create: `scripts/historical_foundry_verifier.py`
- Create: `tests/test_historical_foundry_verifier.py`
- Modify: `scripts/historical_route_publication.py`
- Modify: `tests/test_historical_route_publication.py`

- [ ] **Step 1: Write connected-verifier RED tests**

The verifier subject is sentinel-constructed by staged/committed loaders; ordinary callers cannot choose scenario set, selected block, profile, or pointer fields.

```python
def run_connected_historical_verification(
    subject: "HistoricalVerificationSubject",
    *,
    mode: str,
) -> Mapping[str, Any]: ...

def historical_replay_pointer_core(
    pointer: Mapping[str, Any],
) -> Mapping[str, Any]: ...
```

`mode` is private/closed to `staged`, `publish`, or `audit` at trusted call sites; do not expose it on the user CLI.

Tests require a new process and fresh RPC connection; full seven-day headers/reserves/prices/fees; recomputed entire prefilter grid; fork replay of selected ten plus every newer original `replay_required` scenario; independent safe-exclusion replay; exact original success/closed-revert resolution; unchanged winner; raw/core/bundle/evidence/pointer-core parity; and no selection by the verifier.

Freeze pointer core as the exact final pointer with only `verification_report_sha256` removed; its JSON schema remains `route_historical_replay_pointer/v1`. Test report deletion/mutation/race, wrong filename physical SHA, wrong scenario set, a newer resolution transplant, report reuse across pointer cores, and report install before pointer move.

Also freeze the immutable installer result and interface:

```python
@dataclass(frozen=True)
class VerificationReportInstallResult:
    path: Path
    sha256: str
    size: int
    disposition: str  # exactly "created" or "matched_existing"

def install_historical_verification_report(
    *,
    verification_root: Path,
    report_bytes: bytes,
) -> VerificationReportInstallResult: ...
```

Add RED cases named `test_report_eexist_exact_bytes_is_idempotent`, `test_report_eexist_different_bytes_rejects`, `test_report_eexist_matching_identities_but_different_bytes_rejects`, `test_report_eexist_symlink_or_nonregular_rejects`, and `test_concurrent_report_install_accepts_only_exact_winner_bytes`. In every accepted case descriptor-reread the existing file and prove filename hash, physical hash, size, and exact byte equality.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_verifier -v
```

- [ ] **Step 3: Implement full connected verification**

Reuse Phase-2 capture/replay engines with a fresh capability and no output mutation. Compare canonical typed projections, not provider JSON whitespace. Produce canonical `route_historical_replay_verification/v1` bytes without URL/path/body/credential/exception fields.

In publish mode, `install_historical_verification_report()` computes the SHA from `report_bytes`, derives only `routes/historical/verifications/by-sha256/<sha>.json`, and attempts descriptor-safe `O_CREAT|O_EXCL|O_NOFOLLOW` installation. On `EEXIST`, it never replaces, truncates, unlinks, chmods, or accepts by matching selected identities alone: it opens the existing regular file without following links and accepts only exact bytes plus exact filename/physical SHA/size, returning `matched_existing`. Any other `EEXIST` object or byte difference is `historical_bundle_invalid` and leaves the pointer unchanged. After a created or matched-existing result, descriptor-reread again immediately before constructing the final pointer. In staged/dry-run mode, validate would-be report/pointer bytes only in staging and never call the installer.

- [ ] **Step 4: Run GREEN and tamper matrix**

```bash
python3 -m unittest \
  tests.test_historical_foundry_verifier \
  tests.test_historical_route_publication -v
```

- [ ] **Step 5: Commit verification closure**

```bash
git add scripts/historical_foundry_verifier.py \
  scripts/historical_route_publication.py \
  tests/test_historical_foundry_verifier.py \
  tests/test_historical_route_publication.py
git commit -m "feat(opportunity): verify historical replay publication"
```

## Task 8: Add the Scan/Publish/Dry-Run/Verify CLI and Reference-Aware GC

**Files:**

- Create: `scripts/run_historical_foundry_replay.py`
- Create: `tests/test_run_historical_foundry_replay.py`
- Modify: `scripts/historical_foundry_storage.py`
- Modify: `tests/test_historical_foundry_storage.py`
- Modify: `scripts/historical_route_publication.py`
- Create: `docs/superpowers/reports/2026-08-20-historical-foundry-publication-report.md`

- [ ] **Step 1: Write orchestration RED tests**

The production commands are exactly:

```text
scan --data-dir PATH (--publish | --dry-run)
verify --data-dir PATH --bundle ABSOLUTE_IMMUTABLE_BUNDLE_PATH
```

Reject all policy/authority/toolchain/block/notional/MEV/RPC/binary/profile/root/member overrides. Require clean tracked source, fixed HEAD, tracked config/source/submodule bytes, fixed binary hashes/versions, and `DEX_DEPTH_RPC_ETH` only in the environment.

Implement `verify_clean_tracked_historical_source()` as a sealed preflight used at scan and connected-verify start. It requires empty `git status --porcelain=v1 --untracked-files=all`, records HEAD, compares the three configs, Foundry source, `foundry.toml`, lockfile, and submodule pointer to HEAD bytes, verifies the checked-out forge-std commit, and verifies actual forge/anvil/cast/solc hashes and versions. It may not trust PATH name resolution alone or retain a local binary path in evidence.

Test the exact 15-step operational sequence. On every injected failure, prior historical pointer bytes remain unchanged; live core/complete pointers always remain unchanged. Dry-run stages/rereads core, context, complete bundle, connected report, and final pointer bytes but moves no core/complete pointer and installs no report. Publish commits core, reconstructs equal context, stages complete bundle, verifies, installs report, rereads all inputs, then moves only `routes/historical/latest.json`.

Audit-only verify pins current historical pointer and requires the explicit bundle path to be its exact replay directory; it performs zero mutation.

Freeze the live-pointer guard used by both scan modes and audit-only verify:

```python
@dataclass(frozen=True)
class LivePointerSnapshot:
    relative_path: str
    present: bool
    size: Optional[int]
    sha256: Optional[str]
    bytes_value: Optional[bytes]

def capture_live_pointer_snapshots(
    *,
    data_dir: Path,
) -> Tuple[LivePointerSnapshot, LivePointerSnapshot]: ...

def require_live_pointer_snapshots_unchanged(
    before: Sequence[LivePointerSnapshot],
    after: Sequence[LivePointerSnapshot],
) -> None: ...
```

The inventory is exactly `routes/core/latest.json` followed by `routes/latest.json`. Capture descriptor-safe bytes before the first run-evidence read, recapture after success and in the outermost failure/finally path, and require presence, size, SHA-256, and bytes to match. The canonical command result and real-run report retain, for both before and after snapshots, exactly `relative_path`, `present`, `size`, `sha256`, and `bytes_base64`; base64 is null when absent and otherwise decodes to the exact held bytes. Live pointers contain no secret and their exact bytes are completion evidence. Tests cover both absent, both present, one absent, invalid/noncanonical bytes, same-size replacement, inode swap, and every injected failure step.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_run_historical_foundry_replay -v
```

- [ ] **Step 3: Implement orchestration and GC**

Keep transport, subprocess, filesystem, and clock in this entrypoint/private capabilities. Capture the live-pointer guard before any scan/verify work and enforce it in the outermost completion path, including exceptions. Build a reference set by fully validating every retained historical complete/core manifest and both historical pointers. Delete only descriptor-proven unreferenced raw runs and orphan reports; any invalid inventory makes the deletion set empty. Do not schedule automatic runs in this MVP.

- [ ] **Step 4: Run full Phase 3 verification on both runtimes**

```bash
python3 -m unittest \
  tests.test_route_cost_topology \
  tests.test_route_quantity \
  tests.test_historical_foundry_replay \
  tests.test_historical_route_publication \
  tests.test_historical_foundry_verifier \
  tests.test_run_historical_foundry_replay \
  tests.test_route_opportunity \
  tests.test_route_publication -v
python3.8 -m unittest \
  tests.test_route_cost_topology \
  tests.test_route_quantity \
  tests.test_historical_foundry_replay \
  tests.test_historical_route_publication \
  tests.test_historical_foundry_verifier \
  tests.test_run_historical_foundry_replay \
  tests.test_route_opportunity \
  tests.test_route_publication -v
python3 -m py_compile \
  scripts/route_cost_topology.py \
  scripts/historical_foundry_replay.py \
  scripts/historical_route_publication.py \
  scripts/historical_foundry_verifier.py \
  scripts/run_historical_foundry_replay.py
git diff --check
```

If exact CPython 3.8.10 is not installed, record Phase 3 as blocked; do not substitute another interpreter.

- [ ] **Step 5: Write the report and commit**

The publication report must copy the command's two before snapshots and two after snapshots verbatim, including exact `bytes_base64`, size, and SHA-256, decode/re-hash the base64 as a report check, and state the byte-equality result separately for `routes/core/latest.json` and `routes/latest.json`. A statement such as “live pointers unchanged” without these four captured records is not sufficient evidence.

```bash
git add scripts/run_historical_foundry_replay.py \
  scripts/historical_foundry_storage.py \
  scripts/historical_route_publication.py \
  tests/test_run_historical_foundry_replay.py \
  tests/test_historical_foundry_storage.py \
  docs/superpowers/reports/2026-08-20-historical-foundry-publication-report.md
git commit -m "feat(opportunity): publish verified historical replay"
```

## Phase Exit Review

- [ ] Compare live fixture bytes before/after shared-validator extraction.
- [ ] Prove the historical nine-row topology is used by producer, publication, reader, and release logic from one source.
- [ ] Recompute all ten scenarios and ninety components from raw evidence, not stored result projections.
- [ ] Forge/transplant staged and committed contexts; all attempts must fail.
- [ ] Advance historical core latest after complete publication and prove the older complete bundle still resolves its immutable core.
- [ ] Warm-read then mutate pointer/report/member/ancestor and confirm fail-closed behavior.
- [ ] Verify dry-run zero pointer/report installation and audit-only zero mutation.
- [ ] Verify the real command result captures before/after bytes, sizes, and SHA-256 for `routes/core/latest.json` and `routes/latest.json`, and prove both live pointers remain byte-for-byte unchanged on success and every failure path.
- [ ] Request independent code review before exposing historical data through the dashboard.
