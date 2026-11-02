# SHIB V2/V2 Research Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and publish a deterministic, research-only, fixed-block comparison of the canonical Uniswap V2 and ShibaSwap V1 SHIB/WETH pools at five USD notionals, backed by complete dual-provider public-chain evidence and never presented as executable arbitrage.

**Architecture:** A versioned registry fixes the two pool authorities, token identities, fee models, and Chainlink ETH/USD feed. A single network-capable capture command collects a closed EIP-1898 call inventory from two providers at one finalized block and writes only agreed, bounded public-chain evidence; a separate pure module validates and replays those bytes with the existing exact V2 quantity engine, and an offline command writes a canonical research snapshot. Runtime URLs, arbitrary provider payloads, mutable discovery, private connectors, route pointers, V3, and production paths remain outside the system.

**Tech Stack:** Python 3.8-compatible standard library, `unittest`, Ethereum JSON-RPC/ABI encoding, SHA-256, exact `int`/`Fraction`/`Decimal` arithmetic, and existing `scripts.route_quantity` V2 contracts.

**Spec:** `docs/superpowers/specs/2026-08-29-shib-v2v2-research-loop-design.md`

## Global Constraints

- Work only in the isolated worktree on `codex/shib-v2v2-research-loop`, based on `9249e4d179a35f2202ab40e53f39683999d95b73`; do not modify the user's checkout or the separate V3 branch.
- Support Python 3.8 grammar and the standard library only; do not add a package dependency.
- Keep exactly two Ethereum pools: Uniswap V2 `0x811beed0119b4afce20d2583eb608c6f7af1954f` and ShibaSwap V1 `0xcf6daab95c476106eca715d48de4b13287ffdeaa`.
- Keep exactly five notionals in this order: `1000`, `5000`, `10000`, `50000`, `100000` USD; valid output has two directed routes and ten scenarios.
- Capture one finalized Ethereum block agreed by two distinct opaque provider labels; every `eth_call` must use EIP-1898 `{blockHash, requireCanonical: true}` and byte-identical results.
- Evidence, registry, and snapshot inputs must be regular non-symlink files no larger than 1 MiB; reject duplicate JSON keys, unknown fields, noncanonical values, oversized strings/hex, and unsafe nesting before use.
- Never write RPC URLs, headers, credentials, cookies, account or wallet identities, arbitrary provider errors, absolute/private paths, environment values, or unreviewed raw envelopes.
- Preserve measured zero separately from missing null; absent evidence is `not_evaluated`, not zero coverage. Do not claim 30-day completeness for this point-in-time family.
- Reuse `V2PoolState`, `MarketRules`, `CommonTarget`, `quote_v2_pool_quantity`, and `validate_v2_quantity_quote_against_state`; do not copy the constant-product formula.
- Always persist `mode="historical_replay"`, `strict_eligible=false`, and `executable=false`; gas, router fee, transfer tax, MEV, atomic execution, and net edge remain null.
- Do not modify V3, CEX, USDT/USD, Funding Rate, connector, canary, dashboard, route-cost authority, route pointer, production collector, workflow, or deployment files.
- Do not publish evidence unless all expected calls and both provider observations are usable, unique, and agreed. A failed capture writes no evidence file.
- Use only read-only JSON-RPC methods; never sign, simulate a wallet, submit a transaction, deploy, or merge `main`.

## Audited Authority Bootstrap

The registry constants below were independently read from two public Ethereum
operators at finalized block `25859880`
(`0x0987ec11fb832dc492cd7fdfbd737404e6e788d0e343608e22bc55577bf66023`,
timestamp `1787994071`). Both endpoints returned chain ID 1, the same header by
hash, accepted EIP-1898 for `eth_call` and `eth_getCode`, and returned the same
raw runtime bytes. This bootstrap fixes authority before implementation; the
capture command only verifies these constants and never discovers or rewrites
the registry.

| Authority | Address | Raw code bytes | SHA-256 of decoded runtime bytes |
| --- | --- | ---: | --- |
| Uniswap V2 factory | `0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f` | 13859 | `3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321` |
| Uniswap V2 Router02 | `0x7a250d5630b4cf539739df2c5dacb4c659f2488d` | 21943 | `ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854` |
| Uniswap V2 SHIB/WETH pair | `0x811beed0119b4afce20d2583eb608c6f7af1954f` | 11293 | `8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4` |
| ShibaSwap V1 factory | `0x115934131916c8b277dd010ee02de363c09d037c` | 15527 | `bccd00fecc8d072c7635ef40bd5b7721057975123aa8639d62a37f90f6a45b53` |
| ShibaSwap V1 router | `0x03f7724180aa6b939894b5ca4314783b0b36b329` | 18469 | `bb5f84ee54eacd3a273b2a3942ad904f8194a999f32394682cda2080b14b0423` |
| ShibaSwap V1 SHIB/WETH pair | `0xcf6daab95c476106eca715d48de4b13287ffdeaa` | 10654 | `83589060885cd6b139ce4b4ed723653d124a00b50c0fa203dbd5a425cb272bc7` |
| SHIB | `0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce` | 4852 | `5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3` |
| WETH | `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` | 3124 | `5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739` |
| Chainlink ETH/USD proxy | `0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419` | 9571 | `ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b` |

At that block, both factories returned the specified pairs; both pairs had
`token0=SHIB`, `token1=WETH`; both tokens returned 18 decimals; ShibaSwap pair
calls returned `totalFee=3`, `alpha=1`, `beta=3`. Therefore the registry pins
native fee denominator 1000, trader fee numerator `1000-totalFee=997`, and 30
bps; alpha/beta are retained protocol-liquidity fee parameters and are not
added to the trader fee. Chainlink returned description `ETH / USD`, decimals
8, round ID `129127208515966894402`, and a positive answer. The registry policy
sets `max_feed_age_seconds=3600`; this is a conservative project gate, not a
Chainlink on-chain field.

A second read-only check proved that each router's `factory()` and `WETH()`
values match its registry factory and the canonical WETH address. These four
calls are part of the formal inventory, so the fixed denominator is 35 logical
state reads and 70 provider observations.

The bootstrap endpoints are distinct operators/endpoints. The contract does
not claim cryptographic independence or prove that their cloud/client/upstream
infrastructure cannot overlap.

## Contract Shape Ledger

Implement exact-field validation for every object below. `sha256` means 64
lowercase hexadecimal characters, `hash32` means `0x` plus 64 lowercase hex
characters, `address` means `0x` plus 40 lowercase hex characters, `uintN`
means a Python integer in that ABI range with `type(value) is int`, and
`ratio` means exactly `{"numerator": int, "denominator": positive int}` reduced
by `Fraction`. No listed object accepts an additional member.

Evidence top level is exactly:

```text
schema: "shib_v2_research_evidence/v1"
registry_sha256: sha256
chain: {name: "eth", chain_id: 1}
block: Block
logical_calls: list[LogicalCall]                  # canonical call-id order
provider_observations: list[ProviderObservation]  # call-id, provider order
tokens: list[TokenObservation]                    # SHIB, WETH
usd_reference: UsdReference
pools: list[PoolObservation]                      # uniswap_v2, shibaswap_v1
collection_quality: CollectionQuality
evidence_identity: sha256
```

Nested evidence objects are exactly:

```text
InventoryCall = {                              # process-only, never persisted
  logical_call_id: "call:" + sha256,
  method: one of the 35 fixed inventory method labels,
  target: address, calldata: bounded even-length lowercase 0x hex,
  calldata_sha256: sha256,
  block_selector: "eip1898_block_hash_require_canonical"
}

Block = {
  number: positive uint256, hash: hash32, parent_hash: hash32,
  timestamp: positive int, timestamp_utc: canonical RFC3339 UTC,
  state_root: hash32, base_fee_per_gas: uint256,
  canonical_header_sha256: sha256,
  provider_header_observations: [
    {provider_label: "provider_a", canonical_header_sha256: sha256,
     status: "observed"},
    {provider_label: "provider_b", canonical_header_sha256: sha256,
     status: "observed"}
  ]
}

LogicalCall = {
  logical_call_id: "call:" + sha256,
  method: one of the 35 fixed inventory method labels,
  target: address, calldata: bounded even-length lowercase 0x hex,
  calldata_sha256: sha256, result_hex: bounded even-length lowercase 0x hex,
  result_sha256: sha256
}

ProviderObservation = {
  provider_label: "provider_a" | "provider_b",
  logical_call_id: "call:" + sha256, block_hash: hash32,
  result_sha256: sha256, status: "observed"
}

TokenObservation = {
  symbol: "SHIB" | "WETH", address: address, decimals: uint8,
  runtime_code_size_bytes: positive int, runtime_code_sha256: sha256,
  call_results_sha256: sha256
}

PoolObservation = {
  dex: "uniswap_v2" | "shibaswap_v1",
  factory_address: address, router_address: address, pair_address: address,
  factory_runtime_code_size_bytes: positive int,
  factory_runtime_code_sha256: sha256,
  router_runtime_code_size_bytes: positive int,
  router_runtime_code_sha256: sha256,
  pair_runtime_code_size_bytes: positive int,
  pair_runtime_code_sha256: sha256,
  factory_get_pair_result: address, router_factory_result: address,
  router_weth_result: address, pair_factory_result: address,
  token0_address: address, token1_address: address,
  token0_decimals: uint8, token1_decimals: uint8,
  reserve0_raw: positive uint112, reserve1_raw: positive uint112,
  reserve_timestamp_last_raw: uint32,
  token0_balance_raw: positive uint256, token1_balance_raw: positive uint256,
  reserve_lag_seconds: uint32,
  fee_bps: uint16, fee_numerator: positive int,
  fee_denominator: positive int,
  fee_formula: exact registry formula,
  fee_parameters: {kind: "runtime_code_bound"} |
    {kind: "pair_native_parameters", native_fee_denominator: positive int,
     total_fee: uint256, alpha: uint256, beta: uint256},
  fee_evidence_sha256: sha256, call_results_sha256: sha256
}

UsdReference = {
  kind: "chainlink_aggregator_v3", proxy_address: address,
  proxy_runtime_code_size_bytes: positive int,
  proxy_runtime_code_sha256: sha256,
  description: "ETH / USD", decimals: uint8,
  round_id: positive uint80, answer: positive int256,
  started_at: positive uint256, updated_at: positive uint256,
  answered_in_round: positive uint80, freshness_lag_seconds: uint256,
  call_results_sha256: sha256
}

CollectionQuality = {
  state: "evaluated",
  expected_logical_call_count: 35,
  observed_logical_call_count: 35,
  usable_logical_call_count: 35,
  expected_provider_observation_count: 70,
  observed_provider_observation_count: 70,
  usable_provider_observation_count: 70,
  duplicate_logical_call_key_count: 0,
  duplicate_provider_observation_key_count: 0,
  required_field_null_count: 0,
  measured_zero_count: nonnegative int,
  missing_null_count: 0,
  provider_agreement_count: 35,
  provider_disagreement_count: 0,
  status_counts: {observed: 70}
}
```

`build_logical_call_inventory()` returns `InventoryCall` objects. After a
result has been collected at the fixed block, `_persisted_logical_call()`
projects exactly `logical_call_id`, `method`, `target`, `calldata`,
`calldata_sha256`, `result_hex`, and `result_sha256`; `block_selector` is a
process-only capture instruction, is not persisted, and is rejected as an
additional `LogicalCall` member.

`required_field_null_count` scans every required scalar field in the evidence
ledger before type validation. `measured_zero_count` scans numeric observation
fields in `block`, `tokens`, `pools`, and `usd_reference` (not count/summary
fields); required-positive fields still fail if zero. `missing_null_count`
counts optional evidence observation fields, and v1 has none, so it must be
zero. The seven null route-cost fields belong only to the snapshot and never
inflate evidence `missing_null_count`. `status_counts` counts only the 70
provider state observations; header observations are separately fixed in the
block record.

Snapshot top level is exactly:

```text
schema: "shib_v2_research_snapshot/v1"
application_sha: 40 lowercase hex
registry_sha256: sha256
evidence_identity: sha256
as_of_block_number: positive uint256
as_of_block_hash: hash32
as_of_utc: canonical RFC3339 UTC
mode: "historical_replay"
token: {symbol: "SHIB", address: address, decimals: 18}
quote_asset: {symbol: "WETH", address: address, decimals: 18}
requested_notionals_usd: ["1000", "5000", "10000", "50000", "100000"]
pool_identities: list[PoolIdentity]  # uniswap_v2, shibaswap_v1
scenario_count: 10
summary: SnapshotSummary
scenarios: list[Scenario]            # route order, then notional order
snapshot_sha256: sha256
```

The two route IDs are fixed as:

```text
shib-v2v2:eth:uniswap_v2:0x811beed0119b4afce20d2583eb608c6f7af1954f:to:shibaswap_v1:0xcf6daab95c476106eca715d48de4b13287ffdeaa
shib-v2v2:eth:shibaswap_v1:0xcf6daab95c476106eca715d48de4b13287ffdeaa:to:uniswap_v2:0x811beed0119b4afce20d2583eb608c6f7af1954f
```

Snapshot nested objects are exactly:

```text
PoolIdentity = {
  dex: "uniswap_v2" | "shibaswap_v1", pair_address: address,
  state_id: "dex-v2-quantity:" + sha256,
  call_results_sha256: sha256, fee_evidence_sha256: sha256,
  reserve_timestamp_last_raw: uint32, reserve_lag_seconds: uint32
}

Scenario = {
  route_id: one of the two fixed IDs,
  buy_dex: "uniswap_v2" | "shibaswap_v1", buy_pair_address: address,
  sell_dex: "uniswap_v2" | "shibaswap_v1", sell_pair_address: address,
  requested_notional_usd: one of the five fixed strings,
  buy_pool_reference_shib_usd: ratio,
  sell_pool_reference_shib_usd: ratio,
  common_target_reference_shib_usd: ratio,
  common_shib_raw: positive int | null,
  buy_weth_raw: positive int | null,
  sell_weth_raw: nonnegative int | null,
  gross_edge_weth_raw: int | null,
  buy_cost_usd: ratio | null,
  sell_proceeds_usd: ratio | null,
  gross_edge_usd: ratio | null,
  gross_edge_bps: ratio | null,
  buy_pool_state_id: "dex-v2-quantity:" + sha256,
  sell_pool_state_id: "dex-v2-quantity:" + sha256,
  buy_quote_status: "calculation_complete" | "unavailable",
  sell_quote_status: "calculation_complete" | "unavailable",
  buy_quote_reason: V2QuoteReason,
  sell_quote_reason: V2QuoteReason,
  classification: "non_positive_pool_edge" |
    "positive_pool_edge_costs_incomplete" | "unavailable",
  reason_codes: ordered list[ScenarioReason],
  strict_eligible: false, executable: false,
  limitations: ordered list of the five fixed limitations,
  network_gas_usd: null, router_or_integrator_fee_usd: null,
  token_transfer_tax_usd: null, mev_cost_usd: null,
  atomic_execution_cost_usd: null, net_edge_usd: null,
  net_edge_bps: null
}

V2QuoteReason =
  "fixed_block_fee_proof_not_authenticated" |
  "pool_state_binding_mismatch" | "pool_state_not_current" |
  "market_rules_binding_mismatch" | "market_rules_not_current" |
  "pool_state_market_mismatch" | "target_asset_mismatch" |
  "pool_state_token_address_mismatch" |
  "pool_state_token_decimals_mismatch" |
  "target_base_unit_misaligned" | "target_lot_misaligned" |
  "minimum_base_quantity_not_met" | "pool_output_below_one_raw" |
  "pool_reserve_insufficient" | "minimum_notional_not_met"

ScenarioReason = V2QuoteReason | "route_costs_not_evaluated"

SnapshotSummary = {
  expected_scenario_count: 10, observed_scenario_count: 10,
  usable_scenario_count: int in [0,10],
  classification_counts: {
    non_positive_pool_edge: int,
    positive_pool_edge_costs_incomplete: int,
    unavailable: int
  },
  strict_eligible_count: 0, executable_count: 0,
  missing_cost_field_count: 70
}
```

Quote status and reason construction is exact, not free text:

- `calculation_complete` requires reason
  `fixed_block_fee_proof_not_authenticated`;
- `unavailable` requires one of the other fourteen `V2QuoteReason` values;
- null, unknown, mixed-case, URL-like, or otherwise free-form reasons are
  rejected;
- when either leg is unavailable, `reason_codes` is the ordered unique list of
  both leg quote reasons in buy-then-sell order; a complete leg contributes
  `fixed_block_fee_proof_not_authenticated`, and duplicates are removed after
  their first occurrence;
- when both legs are complete and the edge is non-positive, `reason_codes` is
  exactly `["fixed_block_fee_proof_not_authenticated"]`;
- when both legs are complete and the edge is positive but route costs are
  missing, `reason_codes` is exactly
  `["fixed_block_fee_proof_not_authenticated",
  "route_costs_not_evaluated"]`.

`buy_dex`, `sell_dex`, pair addresses, and `route_id` must agree with one of
the two fixed route tuples; a validator never accepts arbitrary recombinations.

The test module defines these fixture helpers once and later tasks consume the
same names:

- `valid_registry_payload()` deep-copies the repository registry;
  `fixture_registry_and_code_results()` replaces only code size/hash members
  with hashes of nine deterministic bounded byte strings
  `bytes([index + 1]) * 32` in the authority-table order, and returns both the
  normalized registry and the address-to-`0x`-hex result map, so unit evidence
  can be built before a real capture exists.
- `valid_evidence_payload(registry)` ABI-encodes one internally consistent
  block, 35 results, 70 observations, two header observations, decoded entity
  records, recomputed quality, group hashes, and evidence identity. It uses the
  fixture code results returned with the fixture registry. Its exact synthetic
  observations are block number `20000000`, hash byte `0x11` repeated 32,
  parent byte `0x22` repeated 32, state-root byte `0x33` repeated 32, timestamp
  `1710000000` / `2024-03-09T16:00:00Z`, base fee zero, both pools token0 SHIB/token1 WETH with reserves
  `10**30` SHIB raw and `10**24` WETH raw, matching balances, reserve timestamp
  `1709999990`, Shiba fee values 3/1/3, and Chainlink round 1 with answer
  `200000000000`, decimals 8, started/updated time `1709999990`, and
  answered-in-round 1.
- Registry mutations `add_unknown_field`, `uppercase_shib`, and
  `duplicate_first_pool` perform exactly the named single mutation on a deep
  copy.
- Evidence mutations `remove_logical_call`, `add_unknown_call`,
  `duplicate_logical_key`, `remove_provider_observation`,
  `disagree_provider_result`, `change_block_hash`, `change_factory_pair`,
  `change_pair_factory`, `change_router_factory`, `change_router_weth`,
  `change_token_order`, `change_runtime_code_hash`, `change_balance`,
  `change_shibaswap_fee`, `stale_chainlink_round`,
  `future_chainlink_round`, `forge_quality_summary`, and
  `forge_evidence_identity` each make only that mutation and intentionally do
  not rebind the affected authority unless the test explicitly says so.
- `RecordingRpc(response_map)` is a callable fake keyed by canonical
  `(method, params)` bytes; it records all requests and rejects an unlisted
  request. `valid_rpc_responses()` derives both providers' complete response
  maps from `valid_evidence_payload`, including chain ID, finalized header,
  by-hash reread, 35 fixed state results, and final by-hash reread.
  `fixture_providers()` wraps them as
  `Provider("provider_a", "a" * 64, rpc_a)` and
  `Provider("provider_b", "b" * 64, rpc_b)`; the middle value is a distinct
  process-only endpoint identity and never enters evidence.
- Capture failure builders `wrong_chain_id`, `different_finalized_hash`,
  `different_call_bytes`, `eip1898_error`, `missing_state`,
  `changed_round_trip_header`, and `oversized_response` deep-copy the two valid
  `Provider` fakes and their response maps, alter only the named boundary, and
  return the two-provider sequence accepted by `capture_research_evidence`.
- `expected_scenario_keys()` returns the Cartesian product of the two fixed
  route IDs above and the five notional strings in route-major order.
  `build_from_reserves(pair)` replaces both fixture pool reserve/balance pairs,
  recomputes dependent group/evidence hashes, and calls the real builder.
  `positive_edge_reserves()` returns Uniswap
  `(1_000_000 * 10**18, 90 * 10**18)` and ShibaSwap
  `(1_000_000 * 10**18, 100 * 10**18)`. With the fixture ETH/USD value of
  2000, the first $1,000 route has a 5,000-SHIB common target. Its independent
  integer oracle is buy WETH `453622173051818773`, sell WETH
  `496027303890107812`, and positive gross WETH `42405130838289039`.
  `v2_quote_oracle(reserve_shib_raw, reserve_weth_raw, target_shib_raw,
  direction, shib_is_token0=True)` constructs only the existing
  `V2PoolState`, `MarketRules`, and `CommonTarget` inputs and calls the existing
  quote function; it does not calculate an expected result.
  `mutate_reserve_and_rebind(evidence)` increments one reserve and matching
  balance by one, then recomputes call result, group, quality, and evidence
  identities so snapshot mutation behavior is tested past the evidence gate.

---

### Task 1: Canonical Registry and Safe JSON Boundary

**Files:**
- Create: `config/shib_v2_research_pools.json`
- Create: `scripts/shib_v2_research.py`
- Create: `scripts/shib_v2_research_io.py`
- Create: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: the two-pool identities and quality rules in the design spec.
- Produces from pure `scripts.shib_v2_research`: `ResearchContractError`, `canonical_json_bytes(value: object) -> bytes`, `load_research_registry(payload: object) -> dict`, `registry_sha256(registry: dict) -> str`, and `scan_public_payload(payload: object) -> None`.
- Produces from `scripts.shib_v2_research_io`: `load_bounded_json(path: Path, label: str) -> object` and `atomic_write_canonical_json(path: Path, payload: object) -> None`.

- [ ] **Step 1: Add registry RED tests with a complete valid fixture**

```python
class ResearchRegistryTests(unittest.TestCase):
    def test_repository_registry_fixes_exactly_two_shib_weth_pools(self):
        registry = shib_v2_research.load_research_registry(
            json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        )
        self.assertEqual(registry["schema"], "shib_v2_research_registry/v1")
        self.assertEqual(registry["chain"], {"name": "eth", "chain_id": 1})
        self.assertEqual(
            [pool["pair"]["address"] for pool in registry["pools"]],
            [
                "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
            ],
        )
        self.assertEqual(registry["requested_notionals_usd"], [
            "1000", "5000", "10000", "50000", "100000",
        ])

    def test_registry_rejects_unknown_fields_case_drift_and_duplicate_pools(self):
        for mutation in (add_unknown_field, uppercase_shib, duplicate_first_pool):
            with self.subTest(mutation=mutation.__name__):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.load_research_registry(
                        mutation(copy.deepcopy(valid_registry_payload()))
                    )
```

The test fixture must include the exact SHIB address
`0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce`, WETH address
`0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`, both audited factories and
routers, the Chainlink ETH/USD proxy, the exact runtime-code SHA-256 values in
the audited bootstrap table, normalized fee fractions, and the 3600-second
maximum feed age. Use 64 lowercase hex characters for each code hash; test
mutations use known fixture values rather than weakening the production
registry.
Runtime-code SHA-256 always hashes decoded code bytes, not the ASCII `0x...`
representation. The authority audit must compare both providers' decoded bytes
before a hash is admitted to the registry.

- [ ] **Step 2: Run the registry tests and verify RED**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchRegistryTests -v`

Expected: FAIL because `scripts.shib_v2_research` and the registry do not exist.

- [ ] **Step 3: Implement canonical JSON, exact-field validation, and the registry**

```python
class ResearchContractError(ValueError):
    pass


def canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise ResearchContractError("value is not canonical JSON") from error


def _exact_fields(value: object, fields: Sequence[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ResearchContractError("{} schema is invalid".format(label))
    return value


def registry_sha256(registry: dict) -> str:
    normalized = load_research_registry(registry)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
```

The checked-in registry must be compact canonical JSON plus one newline with
this exact semantic content (the writer sorts object keys; array order remains
as shown):

```json
{
  "schema": "shib_v2_research_registry/v1",
  "chain": {"name": "eth", "chain_id": 1},
  "tokens": {
    "SHIB": {
      "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
      "decimals": 18,
      "runtime_code_size_bytes": 4852,
      "runtime_code_sha256": "5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3"
    },
    "WETH": {
      "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "decimals": 18,
      "runtime_code_size_bytes": 3124,
      "runtime_code_sha256": "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739"
    }
  },
  "pools": [
    {
      "dex": "uniswap_v2",
      "factory": {
        "address": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
        "runtime_code_size_bytes": 13859,
        "runtime_code_sha256": "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321"
      },
      "router": {
        "address": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
        "runtime_code_size_bytes": 21943,
        "runtime_code_sha256": "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854"
      },
      "pair": {
        "address": "0x811beed0119b4afce20d2583eb608c6f7af1954f",
        "runtime_code_size_bytes": 11293,
        "runtime_code_sha256": "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4"
      },
      "token0": "SHIB",
      "token1": "WETH",
      "fee_model": {
        "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
        "fee_bps": 30,
        "fee_numerator": 997,
        "fee_denominator": 1000,
        "evidence": {"kind": "runtime_code_bound"}
      }
    },
    {
      "dex": "shibaswap_v1",
      "factory": {
        "address": "0x115934131916c8b277dd010ee02de363c09d037c",
        "runtime_code_size_bytes": 15527,
        "runtime_code_sha256": "bccd00fecc8d072c7635ef40bd5b7721057975123aa8639d62a37f90f6a45b53"
      },
      "router": {
        "address": "0x03f7724180aa6b939894b5ca4314783b0b36b329",
        "runtime_code_size_bytes": 18469,
        "runtime_code_sha256": "bb5f84ee54eacd3a273b2a3942ad904f8194a999f32394682cda2080b14b0423"
      },
      "pair": {
        "address": "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
        "runtime_code_size_bytes": 10654,
        "runtime_code_sha256": "83589060885cd6b139ce4b4ed723653d124a00b50c0fa203dbd5a425cb272bc7"
      },
      "token0": "SHIB",
      "token1": "WETH",
      "fee_model": {
        "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
        "fee_bps": 30,
        "fee_numerator": 997,
        "fee_denominator": 1000,
        "evidence": {
          "kind": "pair_native_parameters",
          "target": "pair",
          "native_fee_denominator": 1000,
          "total_fee": 3,
          "alpha": 1,
          "beta": 3
        }
      }
    }
  ],
  "usd_reference": {
    "kind": "chainlink_aggregator_v3",
    "proxy_address": "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
    "runtime_code_size_bytes": 9571,
    "runtime_code_sha256": "ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b",
    "description": "ETH / USD",
    "decimals": 8,
    "max_age_seconds": 3600
  },
  "requested_notionals_usd": ["1000", "5000", "10000", "50000", "100000"]
}
```

Uniswap's fee evidence is its pinned pair runtime code. ShibaSwap's three
native calls target the pair, never the factory; validation requires
`fee_numerator = native_fee_denominator - total_fee` and
`fee_bps * fee_denominator = total_fee * 10000`.

- [ ] **Step 4: Add bounded-file, duplicate-key, symlink, canonical-byte, and public-scan tests**

```python
def test_bounded_loader_rejects_duplicate_json_keys_and_symlink(self):
    duplicate = self.root / "duplicate.json"
    duplicate.write_text('{"schema":"a","schema":"b"}', encoding="utf-8")
    with self.assertRaisesRegex(ResearchContractError, "duplicate JSON key"):
        load_bounded_json(duplicate, "registry")
    link = self.root / "link.json"
    link.symlink_to(duplicate)
    with self.assertRaisesRegex(ResearchContractError, "regular file"):
        load_bounded_json(link, "registry")

def test_public_scan_rejects_url_secret_private_path_and_provider_error(self):
    slash = chr(47)
    for value in (
        "https://rpc.example/key", "sk-live-secretmaterial",
        slash + "Users" + slash + "private" + slash + "research",
        {"provider_error": "arbitrary text"},
    ):
        with self.subTest(value=value):
            with self.assertRaises(ResearchContractError):
                scan_public_payload({"value": value})

def test_bounded_loader_rejects_float_exponent_and_nonfinite_tokens(self):
    for token in ("1.0", "1e3", "NaN", "Infinity", "-Infinity"):
        path = self.root / "numeric.json"
        path.write_text('{"value":' + token + '}', encoding="utf-8")
        with self.subTest(token=token):
            with self.assertRaises(ResearchContractError):
                load_bounded_json(path, "numeric fixture")
```

Open every directory component and the final file without following symlinks;
use `fstat` to require a single-link regular file, verify device/inode/size
before and after the bounded read, and enforce the 1 MiB limit.
Run a byte-level JSON preflight before `json.loads` to bound nesting, members,
string-token length, and integer-token length. Then use `json.loads(...,
object_pairs_hook=..., parse_int=_bounded_int, parse_float=_reject_float,
parse_constant=_reject_constant)` to reject duplicate keys, oversized
integers, binary-float/exponent tokens, NaN, and Infinity; use
`type(value) is int` in schema validators so booleans cannot pass as numbers.
After parsing, require the original file bytes to equal
`canonical_json_bytes(value) + b"\n"`.
Use a same-directory staged file plus `fsync` and
`os.replace` for output, while refusing a symlink/non-regular destination and
unsafe ancestor chain. The public scan is schema-aware: it checks only
forbidden keys and unsafe free-text/path/URL patterns and must not reject
legitimate `token` fields or canonical EVM addresses.

- [ ] **Step 5: Run Task 1 tests and Python 3.8 grammar checks**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchRegistryTests tests.test_framework.FrameworkStructureTest.test_all_python_sources_parse_with_python_38_grammar -v`

Expected: PASS.

- [ ] **Step 6: Commit the registry foundation**

```bash
git add config/shib_v2_research_pools.json scripts/shib_v2_research.py scripts/shib_v2_research_io.py tests/test_shib_v2_research.py
git commit -m "feat(research): define SHIB V2 authority registry"
```

### Task 2: Closed Call Inventory and Evidence Validator

**Files:**
- Modify: `scripts/shib_v2_research.py`
- Modify: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: `load_research_registry`, `registry_sha256`, and the canonical registry from Task 1.
- Produces: `build_logical_call_inventory(registry: dict) -> List[dict]`
  (every member has the exact `InventoryCall` ledger shape),
  `abi_encode_call(signature: str,
  arguments: Sequence[object]) -> str`, `abi_decode_result(kind: str,
  result_hex: str) -> object`, `validate_research_evidence(payload: object,
  registry: dict) -> dict`, and `evidence_identity(payload: dict) -> str`.

- [ ] **Step 1: Add RED tests for the exact inventory and valid evidence fixture**

```python
class ResearchEvidenceTests(unittest.TestCase):
    def test_inventory_is_closed_unique_and_registry_derived(self):
        calls = shib_v2_research.build_logical_call_inventory(self.registry)
        self.assertEqual(len(calls), 35)
        self.assertEqual(len(calls), len({call["logical_call_id"] for call in calls}))
        self.assertEqual(
            {call["block_selector"] for call in calls},
            {"eip1898_block_hash_require_canonical"},
        )
        self.assertEqual(
            {call["method"] for call in calls},
            {
                "eth_getCode", "factory.getPair", "router.factory",
                "router.weth", "pair.factory",
                "pair.token0", "pair.token1", "pair.getReserves",
                "erc20.decimals", "erc20.balanceOf", "fee.totalFee",
                "fee.alpha", "fee.beta", "feed.decimals",
                "feed.description", "feed.latestRoundData",
            },
        )

    def test_valid_evidence_recomputes_complete_quality_and_identity(self):
        evidence = shib_v2_research.validate_research_evidence(
            valid_evidence_payload(self.registry), self.registry
        )
        quality = evidence["collection_quality"]
        self.assertEqual(quality["state"], "evaluated")
        self.assertEqual(quality["expected_logical_call_count"], 35)
        self.assertEqual(quality["observed_logical_call_count"], 35)
        self.assertEqual(quality["usable_logical_call_count"], 35)
        self.assertEqual(quality["expected_provider_observation_count"], 70)
        self.assertEqual(quality["observed_provider_observation_count"], 70)
        self.assertEqual(quality["usable_provider_observation_count"], 70)
        self.assertEqual(quality["provider_disagreement_count"], 0)
```

The count is fixed: 9 runtime-code reads, 2 factory `getPair` calls, 4 router
identity calls, 8 pair identity/reserve calls, 2 unique token-decimal calls, 4
token-balance calls, 3 ShibaSwap pair fee calls, and 3 Chainlink calls. Chain ID, finalized-header
selection, and by-hash header rereads are a separate capture preflight and do
not silently inflate or shrink the 35/70 state inventory. The evidence block
record retains exactly two reviewed header observations
`{provider_label, canonical_header_sha256, status}` so offline validation can
prove that both fixed opaque providers agreed on the persisted header.

The fixture builder must create ABI-valid bounded result bytes, compute every
call/result/group hash, bind all children to one nonzero block hash, and derive
the quality summary rather than copying production output.

- [ ] **Step 2: Run evidence tests and verify RED**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchEvidenceTests.test_inventory_is_closed_unique_and_registry_derived tests.test_shib_v2_research.ResearchEvidenceTests.test_valid_evidence_recomputes_complete_quality_and_identity -v`

Expected: FAIL because the inventory and evidence validator do not exist.

- [ ] **Step 3: Implement the deterministic call inventory and minimal ABI codec**

```python
def _inventory_call(method: str, target: str, calldata: str) -> dict:
    calldata_bytes = bytes.fromhex(calldata[2:])
    identity = {
        "method": method,
        "target": target,
        "calldata_sha256": hashlib.sha256(calldata_bytes).hexdigest(),
        "block_selector": "eip1898_block_hash_require_canonical",
    }
    return dict(
        identity,
        logical_call_id="call:" + hashlib.sha256(
            b"shib-v2-logical-call/v1\n" + canonical_json_bytes(identity)
        ).hexdigest(),
        calldata=calldata,
    )
```

This returns the internal `InventoryCall` shape. The evidence builder uses the
separate `_persisted_logical_call(inventory_call, result_hex)` projection and
must not copy `block_selector` into public evidence.

Implement only the ABI shapes required by the registry: address, uint8,
uint32, uint112 tuple, uint256/int256, string, and the five-value Chainlink
round tuple. Function selectors are fixed reviewed constants in the module;
tests assert each selector. Reject non-minimal offsets, malformed padding,
trailing words, oversized strings, empty code, and results over the per-call
hex bound.

- [ ] **Step 4: Implement exact evidence validation and recomputed quality**

```python
def evidence_identity(payload: dict) -> str:
    body = dict(payload)
    body.pop("evidence_identity", None)
    return hashlib.sha256(
        b"shib-v2-research-evidence/v1\n" + canonical_json_bytes(body)
    ).hexdigest()


def validate_research_evidence(payload: object, registry: dict) -> dict:
    registry = load_research_registry(registry)
    evidence = _validate_evidence_shape(payload)
    expected = build_logical_call_inventory(registry)
    _require_exact_call_set(evidence["logical_calls"], expected)
    _require_two_agreed_observations(evidence)
    _validate_header_and_every_block_binding(evidence)
    _validate_tokens_pools_fees_and_feed(evidence, registry)
    recomputed = _recompute_collection_quality(evidence, expected)
    if evidence["collection_quality"] != recomputed:
        raise ResearchContractError("collection quality does not recompute")
    if evidence["evidence_identity"] != evidence_identity(evidence):
        raise ResearchContractError("evidence identity does not recompute")
    scan_public_payload(evidence)
    return json.loads(canonical_json_bytes(evidence).decode("utf-8"))
```

Require exact top-level and nested fields. Validate chain ID, block header
round-trip projection/hash, two opaque provider labels, exact call keys,
result SHA-256, code hashes, factory/pair round trips, each router's
`factory()==registry factory` and `WETH()==canonical WETH`, token set/order and
decimals, reserves and balance equality, fee arithmetic, Chainlink
`answered_in_round`, positive answer, and update age at block time. Recompute
all quality metrics, including measured-zero and missing-null counts.

- [ ] **Step 5: Add adversarial evidence tests**

```python
def test_evidence_fails_closed_on_each_completeness_and_authority_break(self):
    mutations = (
        remove_logical_call, add_unknown_call, duplicate_logical_key,
        remove_provider_observation, disagree_provider_result,
        change_block_hash, change_factory_pair, change_pair_factory,
        change_router_factory, change_router_weth, change_token_order,
        change_runtime_code_hash, change_balance,
        change_shibaswap_fee, stale_chainlink_round, future_chainlink_round,
        forge_quality_summary, forge_evidence_identity,
    )
    for mutation in mutations:
        with self.subTest(mutation=mutation.__name__):
            payload = mutation(valid_evidence_payload(self.registry))
            with self.assertRaises(shib_v2_research.ResearchContractError):
                shib_v2_research.validate_research_evidence(payload, self.registry)
```

Add separate assertions that a fixture block with a legitimate zero
`base_fee_per_gas` stays integer zero and increments `measured_zero_count`,
whereas removing the field fails and never becomes zero. Do not invent a
`feeTo` call that is absent from the closed 35-call inventory.

- [ ] **Step 6: Run all pure registry/evidence tests**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchRegistryTests tests.test_shib_v2_research.ResearchEvidenceTests -v`

Expected: PASS.

- [ ] **Step 7: Commit the evidence contract**

```bash
git add scripts/shib_v2_research.py tests/test_shib_v2_research.py
git commit -m "feat(research): validate bounded SHIB V2 evidence"
```

### Task 3: Dual-Provider EIP-1898 Capture Command

**Files:**
- Create: `scripts/capture_shib_v2_research_evidence.py`
- Modify: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: Task 2 inventory/evidence validation and Task 1 atomic writer.
- Produces: `CaptureError`, process-only `Provider(label, endpoint_identity,
  rpc)`, `capture_research_evidence(registry: dict,
  providers: Sequence[Provider], output_path: Path) -> dict`,
  `sanitize_capture_failure(error: BaseException) -> str`, and a CLI with
  `--registry`, `--output`, `--rpc-url-a`, `--rpc-url-b`, bounded timeout,
  internally fixed labels, distinct runtime endpoint identities, and no
  implicit paths.

- [ ] **Step 1: Add fake-transport RED tests for the happy path**

```python
class ResearchCaptureTests(unittest.TestCase):
    def test_capture_uses_one_finalized_hash_and_eip1898_for_every_call(self):
        responses_a, responses_b = valid_rpc_responses()
        provider_a = RecordingRpc(responses_a)
        provider_b = RecordingRpc(responses_b)
        evidence = capture.capture_research_evidence(
            self.registry,
            [Provider("provider_a", "a" * 64, provider_a),
             Provider("provider_b", "b" * 64, provider_b)],
            self.output,
        )
        self.assertEqual(evidence["collection_quality"]["state"], "evaluated")
        for recorder in (provider_a, provider_b):
            for request in recorder.fixed_block_state_requests:
                self.assertEqual(request["params"][1], {
                    "blockHash": self.block_hash,
                    "requireCanonical": True,
                })
        self.assertEqual(self.output.read_bytes(), canonical_json_bytes(evidence) + b"\n")
```

- [ ] **Step 2: Run the capture happy-path test and verify RED**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchCaptureTests.test_capture_uses_one_finalized_hash_and_eip1898_for_every_call -v`

Expected: FAIL because the capture command does not exist.

- [ ] **Step 3: Implement bounded JSON-RPC transport and finalized-block agreement**

```python
@dataclass(frozen=True)
class Provider:
    label: str
    endpoint_identity: str
    rpc: Callable[[str, list], object]


def capture_research_evidence(registry: dict, providers: Sequence[Provider], output_path: Path) -> dict:
    if [provider.label for provider in providers] != ["provider_a", "provider_b"]:
        raise CaptureError("capture_configuration_invalid")
    if len({provider.endpoint_identity for provider in providers}) != 2:
        raise CaptureError("capture_configuration_invalid")
    _require_chain_ids(providers, expected=registry["chain"]["chain_id"])
    headers = [_load_finalized_header(provider) for provider in providers]
    header = _require_identical_headers(headers)
    for provider in providers:
        _require_header_round_trip(provider, header)
    calls = build_logical_call_inventory(registry)
    logical, observations = _collect_agreed_calls(providers, header, calls)
    candidate = _decode_evidence(registry, header, logical, observations)
    candidate["evidence_identity"] = evidence_identity(candidate)
    validated = validate_research_evidence(candidate, registry)
    atomic_write_canonical_json(output_path, validated)
    return validated
```

Use `urllib.request` with POST only, fixed JSON-RPC fields, response byte and
member limits, a redirect-rejecting handler, and exact result/error envelope
shapes. Require HTTPS, reject URL userinfo, and install an explicit empty
`ProxyHandler` so the TLS-authenticated connection never inherits ambient
proxy routing or proxy credentials. Accept
only `eth_chainId`, `eth_getBlockByNumber("finalized", false)`,
`eth_getBlockByHash(hash, false)`, `eth_getCode`, and `eth_call`; no generic
method parameter from the CLI. Do not include URLs or response text in raised
errors. Before opening either URL, the CLI rejects exact equality and derives
each process-only `endpoint_identity` as SHA-256 of
`b"shib-v2-rpc-endpoint/v1\n" + url.encode("utf-8")`. The identity is used
only to reject duplicate configured endpoints; it is never returned or
persisted. The contract claims distinct configured endpoints/operators, not
cryptographic independence of their hidden infrastructure.

- [ ] **Step 4: Add fail-closed transport and privacy RED tests**

```python
def test_capture_writes_nothing_on_provider_disagreement_or_eip1898_rejection(self):
    for failure in (wrong_chain_id, different_finalized_hash,
                    different_call_bytes, eip1898_error, missing_state,
                    changed_round_trip_header, oversized_response):
        with self.subTest(failure=failure.__name__):
            with self.assertRaises(capture.CaptureError):
                capture.capture_research_evidence(
                    self.registry, failure(self.providers), self.output
                )
            self.assertFalse(self.output.exists())

def test_capture_failure_never_contains_url_key_or_provider_body(self):
    secret_url = "https://rpc.example/v2/sk-live-private"
    class FailingRpc:
        def __call__(self, method, params):
            raise OSError(secret_url + " provider said account@example.test")
    with self.assertRaises(capture.CaptureError) as caught:
        capture.capture_research_evidence(
            self.registry,
            [Provider("provider_a", "a" * 64, FailingRpc()),
             Provider("provider_b", "b" * 64, FailingRpc())],
            self.output,
        )
    rendered = str(caught.exception)
    self.assertNotIn(secret_url, rendered)
    self.assertNotIn("account@example.test", rendered)
```

Also test that retry repeats the same block hash only, two identical labels are
rejected, duplicate endpoint identities are rejected before transport, one
provider is rejected, a block-number selector is never emitted, and no
majority/single-provider fallback exists. Repeat failure tests with a
pre-existing output file and assert its bytes remain unchanged. Test both
`eth_call` and `eth_getCode` requests carry the EIP-1898 block object.

Add a real subprocess boundary test using a valid temporary registry and the
same literal URL for `--rpc-url-a` and `--rpc-url-b`. It must exit nonzero,
write exactly `capture_configuration_invalid\n` to stderr, perform no network
request, and leave an absent or pre-existing output byte-for-byte unchanged.

- [ ] **Step 5: Implement stable allowlisted capture failures and CLI bootstrap**

```python
CAPTURE_REASONS = {
    "capture_configuration_invalid", "chain_id_mismatch",
    "provider_disagreement", "canonical_block_unavailable",
    "eip1898_unavailable", "required_call_missing",
    "pool_authority_mismatch", "router_authority_mismatch",
    "fee_authority_mismatch", "usd_reference_unavailable",
    "registry_invalid", "rpc_response_invalid", "unsafe_output_path",
}


class CaptureError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        if reason_code not in CAPTURE_REASONS:
            reason_code = "rpc_response_invalid"
        self.reason_code = reason_code
        super().__init__(reason_code)


def sanitize_capture_failure(error: BaseException) -> str:
    if isinstance(error, CaptureError):
        return error.reason_code
    return "rpc_response_invalid"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
```

Map internal exceptions to one of the fixed reasons, print only that reason to
stderr, and exit nonzero. Validate all evidence before writing; never leave a
partial or stable-looking file on failure.

- [ ] **Step 6: Run capture tests and direct CLI help from two working directories**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchCaptureTests -v`

Run: `python3 scripts/capture_shib_v2_research_evidence.py --help`

Set `REPO_ROOT` to the isolated worktree root, change to a newly created
external temporary directory, then run:
`python3 "$REPO_ROOT/scripts/capture_shib_v2_research_evidence.py" --help`.

Expected: all tests PASS; both help commands exit 0 without importing V3, CEX,
USDT, connector, dashboard, or production collector modules.

- [ ] **Step 7: Commit the capture command**

```bash
git add scripts/capture_shib_v2_research_evidence.py tests/test_shib_v2_research.py
git commit -m "feat(research): capture dual-provider SHIB V2 evidence"
```

### Task 4: Exact Offline Scenario Replay

**Files:**
- Modify: `scripts/shib_v2_research.py`
- Modify: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: validated evidence and the exact V2 types/functions from `scripts.route_quantity`.
- Produces: `build_research_snapshot(evidence: dict, registry: dict, application_sha: str) -> dict`, `validate_research_snapshot(payload: object, evidence: dict, registry: dict) -> dict`, and `snapshot_sha256(payload: dict) -> str`.

- [ ] **Step 1: Add RED tests for ten exact research scenarios**

```python
class ResearchSnapshotTests(unittest.TestCase):
    def test_snapshot_has_two_routes_five_notionals_and_no_executable_claim(self):
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        self.assertEqual(snapshot["schema"], "shib_v2_research_snapshot/v1")
        self.assertEqual(snapshot["mode"], "historical_replay")
        self.assertEqual(snapshot["scenario_count"], 10)
        self.assertEqual(
            [(row["route_id"], row["requested_notional_usd"]) for row in snapshot["scenarios"]],
            expected_scenario_keys(),
        )
        for row in snapshot["scenarios"]:
            self.assertFalse(row["strict_eligible"])
            self.assertFalse(row["executable"])
            self.assertIsNone(row["network_gas_usd"])
            self.assertIsNone(row["net_edge_usd"])
```

- [ ] **Step 2: Run the scenario test and verify RED**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchSnapshotTests.test_snapshot_has_two_routes_five_notionals_and_no_executable_claim -v`

Expected: FAIL because the snapshot builder does not exist.

- [ ] **Step 3: Build authenticated `V2PoolState` and `MarketRules` objects**

```python
def _pool_state(pool: dict, block: dict) -> V2PoolState:
    return V2PoolState(
        chain="eth", chain_id=1, dex=pool["dex"],
        pool_address=pool["pair_address"],
        token0_address=pool["token0_address"],
        token1_address=pool["token1_address"],
        token0_decimals=pool["token0_decimals"],
        token1_decimals=pool["token1_decimals"],
        reserve0_raw=pool["reserve0_raw"], reserve1_raw=pool["reserve1_raw"],
        reserve_timestamp_last_raw=pool["reserve_timestamp_last_raw"],
        fee_bps=pool["fee_bps"], fee_numerator=pool["fee_numerator"],
        fee_denominator=pool["fee_denominator"], fee_formula=pool["fee_formula"],
        fee_proof_sha256=pool["fee_evidence_sha256"],
        block_number=block["number"], block_hash=block["hash"],
        block_header_sha256=block["canonical_header_sha256"],
        observed_at=block["timestamp_utc"],
        raw_response_sha256=pool["call_results_sha256"],
    )
```

Create one-wei SHIB and WETH `MarketRules` with `observed_at` equal to the
block time and deterministic `valid_until` exactly one second later. The
source hash is the pool's call-results hash. Keep `cohort_now` equal to the
fixed block time so historical replay cannot acquire wall-clock freshness.

- [ ] **Step 4: Implement exact target and route quote calculation**

```python
def _common_target(notional_usd: int, reference_price: Fraction) -> CommonTarget:
    # reference_price is exact USD per one whole SHIB.
    theoretical_raw = Fraction(notional_usd * 10**18, 1) / reference_price
    raw = theoretical_raw.numerator // theoretical_raw.denominator
    if raw <= 0:
        raise ResearchContractError("common target is below one SHIB wei")
    return CommonTarget(asset="SHIB", unit_decimals=18, raw_quantity=raw, lattice_raw=1)


buy_quote = quote_v2_pool_quantity(
    buy_state, target, buy_rules, direction="buy",
    target_token_address=shib_address, quote_token_address=weth_address,
    cohort_now=block_time,
)
sell_quote = quote_v2_pool_quantity(
    sell_state, target, sell_rules, direction="sell",
    target_token_address=shib_address, quote_token_address=weth_address,
    cohort_now=block_time,
)
validate_v2_quantity_quote_against_state(
    buy_quote, buy_state, target, buy_rules, direction="buy",
    target_token_address=shib_address, quote_token_address=weth_address,
    cohort_now=block_time,
)


def _quote_weth_raw(quote: QuantityQuote) -> int:
    if quote.gross_quote_quantity is None:
        raise ResearchContractError("quote has no WETH amount")
    raw = Fraction(quote.gross_quote_quantity) * 10**18
    if raw.denominator != 1:
        raise ResearchContractError("quote WETH amount is not raw-unit exact")
    return raw.numerator
```

Calculate each pool marginal SHIB/USD reference as an exact `Fraction` from
reserves, token ordering, ETH/USD answer, and decimals; define it as USD per
one whole SHIB and use the maximum of the two references for the common target.
Recover buy/sell WETH raw quantities from the validated quote Decimal through
`_quote_weth_raw`; `QuantityQuote` does not expose a raw WETH field. Persist
exact integers for raw amounts and exact
`{"numerator": int, "denominator": int}` objects for USD and bps ratios; never
round through binary float.

- [ ] **Step 5: Implement scenario classification, stable nulls, summary, and self-hash**

```python
MISSING_COST_FIELDS = (
    "network_gas_usd", "router_or_integrator_fee_usd",
    "token_transfer_tax_usd", "mev_cost_usd",
    "atomic_execution_cost_usd", "net_edge_usd", "net_edge_bps",
)
LIMITATIONS = (
    "network_gas_not_evaluated", "router_fee_not_evaluated",
    "token_transfer_tax_not_evaluated", "mev_not_evaluated",
    "atomic_route_simulation_unavailable",
)

classification = (
    "positive_pool_edge_costs_incomplete"
    if gross_edge_weth_raw > 0 else "non_positive_pool_edge"
)
scenario.update({field: None for field in MISSING_COST_FIELDS})
scenario.update({
    "classification": classification,
    "strict_eligible": False,
    "executable": False,
    "limitations": list(LIMITATIONS),
})


def snapshot_sha256(payload: dict) -> str:
    body = dict(payload)
    body.pop("snapshot_sha256", None)
    return hashlib.sha256(
        b"shib-v2-research-snapshot/v1\n" + canonical_json_bytes(body)
    ).hexdigest()
```

If either exact quote is unavailable, keep quote-derived and edge fields null,
set classification `unavailable`, retain the fixed reason code, and do not
convert it to a zero/negative edge. Compute the top-level summary from scenario
records and hash the complete snapshot excluding `snapshot_sha256` under a
domain-separated SHA-256.

- [ ] **Step 6: Add arithmetic, ordering, unavailable, null/zero, mutation, and determinism tests**

```python
def test_positive_edge_is_cost_incomplete_not_opportunity(self):
    snapshot = build_from_reserves(positive_edge_reserves())
    row = next(item for item in snapshot["scenarios"] if item["gross_edge_weth_raw"] > 0)
    self.assertEqual(row["common_shib_raw"], 5_000 * 10**18)
    self.assertEqual(row["buy_weth_raw"], 453622173051818773)
    self.assertEqual(row["sell_weth_raw"], 496027303890107812)
    self.assertEqual(row["gross_edge_weth_raw"], 42405130838289039)
    self.assertEqual(row["classification"], "positive_pool_edge_costs_incomplete")
    self.assertEqual(row["reason_codes"], [
        "fixed_block_fee_proof_not_authenticated",
        "route_costs_not_evaluated",
    ])
    self.assertNotIn("opportunity", json.dumps(row))
    self.assertIsNone(row["net_edge_usd"])

def test_v2_integer_rounding_oracles_and_token_order(self):
    shib = 1_000_000 * 10**18
    target = 5_000 * 10**18
    expected = {
        (90 * 10**18, "buy"): 453622173051818773,
        (90 * 10**18, "sell"): 446424573501097031,
        (100 * 10**18, "buy"): 504024636724243082,
        (100 * 10**18, "sell"): 496027303890107812,
    }
    for shib_is_token0 in (True, False):
        for (weth, direction), expected_raw in expected.items():
            with self.subTest(
                shib_is_token0=shib_is_token0,
                weth=weth,
                direction=direction,
            ):
                quote = v2_quote_oracle(
                    shib, weth, target, direction,
                    shib_is_token0=shib_is_token0,
                )
                self.assertEqual(quote.status, "calculation_complete")
                self.assertEqual(_quote_weth_raw(quote), expected_raw)

def test_exact_output_at_reserve_is_unavailable_not_zero(self):
    reserve_shib = 1_000_000 * 10**18
    quote = v2_quote_oracle(
        reserve_shib,
        90 * 10**18,
        reserve_shib,
        "buy",
    )
    self.assertEqual(quote.status, "unavailable")
    self.assertEqual(quote.reason_code, "pool_reserve_insufficient")
    self.assertIsNone(quote.gross_quote_quantity)

def test_same_inputs_are_byte_identical_and_mutation_changes_identity(self):
    first = build_research_snapshot(self.evidence, self.registry, "1" * 40)
    second = build_research_snapshot(self.evidence, self.registry, "1" * 40)
    self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
    changed = mutate_reserve_and_rebind(self.evidence)
    self.assertNotEqual(
        build_research_snapshot(changed, self.registry, "1" * 40)["snapshot_sha256"],
        first["snapshot_sha256"],
    )
```

The four hard-coded raw WETH values above are independent constant-product
oracles: exact-output uses integer round-up and exact-input uses integer
round-down. Cover negative and measured-zero edge with separate reserve
fixtures, positive incomplete edge, reserve insufficiency, all ten keys,
stable ordering, self-hash recomputation, forged scenario/summary/hash,
arbitrary DEX strings, arbitrary quote reasons, and non-canonical
`reason_codes` rejection.

- [ ] **Step 7: Run snapshot and existing exact-math tests**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchSnapshotTests tests.test_route_quantity -v`

Expected: PASS.

- [ ] **Step 8: Commit the offline replay engine**

```bash
git add scripts/shib_v2_research.py tests/test_shib_v2_research.py
git commit -m "feat(research): replay exact SHIB V2 comparisons"
```

### Task 5: Offline Build CLI and Consumer Contract

**Files:**
- Create: `scripts/build_shib_v2_research_snapshot.py`
- Create: `docs/shib-v2v2-research-contract.md`
- Modify: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: Task 4 pure builder/validator and Task 1 safe file/write helpers.
- Produces: an offline CLI requiring `--registry`, `--evidence`, `--application-sha`, and `--output`; a durable consumer contract covering grains, keys, states, lineage, collection criteria, replay, and limitations.

- [ ] **Step 1: Add CLI RED tests from repository and external working directories**

```python
class ResearchBuildCliTests(unittest.TestCase):
    def test_cli_replays_without_network_from_external_working_directory(self):
        result = subprocess.run(
            [
                sys.executable, str(BUILD_SCRIPT),
                "--registry", str(self.registry_path),
                "--evidence", str(self.evidence_path),
                "--application-sha", "1" * 40,
                "--output", str(self.output_path),
            ],
            cwd=str(self.external_cwd), capture_output=True, text=True,
            timeout=15, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.output_path.read_bytes(),
            canonical_json_bytes(validate_research_snapshot(
                load_bounded_json(self.output_path, "snapshot"),
                self.evidence,
                self.registry,
            )) + b"\n",
        )
```

Add a separate in-process test that patches `socket.socket` and
`urllib.request.urlopen` to fail if the pure builder attempts a network path;
patching the parent process does not protect the external subprocess. The
subprocess test proves import/cwd behavior and byte parity independently.

- [ ] **Step 2: Run the CLI test and verify RED**

Run: `python3 -m unittest tests.test_shib_v2_research.ResearchBuildCliTests.test_cli_replays_without_network_from_external_working_directory -v`

Expected: FAIL because the build command does not exist.

- [ ] **Step 3: Implement the explicit offline CLI**

```python
def main() -> int:
    args = parse_args()
    try:
        registry_payload = load_bounded_json(args.registry, "registry")
        registry = load_research_registry(registry_payload)
    except (OSError, ResearchContractError):
        print("registry_invalid", file=sys.stderr)
        return 1
    try:
        evidence_payload = load_bounded_json(args.evidence, "evidence")
    except FileNotFoundError:
        print("evidence_not_evaluated", file=sys.stderr)
        return 2
    except (OSError, ResearchContractError):
        print("evidence_failed", file=sys.stderr)
        return 1
    try:
        evidence = validate_research_evidence(evidence_payload, registry)
        snapshot = build_research_snapshot(
            evidence, registry, args.application_sha
        )
        snapshot = validate_research_snapshot(snapshot, evidence, registry)
        atomic_write_canonical_json(args.output, snapshot)
    except (OSError, ResearchContractError):
        print("evidence_failed", file=sys.stderr)
        return 1
    return 0
```

Bootstrap the repository root exactly as the checked-in data-quality CLI does.
Require a lowercase 40-character Git SHA. Do not provide default mutable input
paths, a network fallback, or a current-time option. Convert only
`ResearchContractError`/`OSError` into stable stderr; do not mask import bugs.

- [ ] **Step 4: Write the contract document with exact semantics**

Document:

```text
family: shib_v2v2_research
evidence grain: (registry_sha256, chain_id, block_hash)
scenario grain: (route_id, requested_notional_usd)
mode: historical_replay
classifications: non_positive_pool_edge,
                 positive_pool_edge_costs_incomplete,
                 unavailable
not evaluated costs: gas, router fee, transfer tax, MEV, atomic execution
strict_eligible: false
executable: false
```

Include the exact call inventory, collection-quality fields, two-provider rule,
EIP-1898 rule, finality/freshness semantics, null-vs-zero rules, evidence and
snapshot self-hashes, byte-for-byte replay command, public/private boundary,
and explicit non-claims: no live/current ranking, no 30-day coverage, no
atomic execution, no opportunity, no V3/CEX/USDT/production.

Define absent evidence explicitly: the build CLI exits 2, emits only
`evidence_not_evaluated`, and writes no output. It must not emit a zero-call or
zero-scenario snapshot. Invalid present evidence exits nonzero as failed and
also leaves the prior output unchanged.

- [ ] **Step 5: Add privacy, shape-limit, direct-help, and import-boundary tests**

```python
def test_public_artifacts_exclude_private_inputs_and_forbidden_imports(self):
    rendered = self.output_path.read_text(encoding="utf-8")
    slash = chr(47)
    for forbidden in (
        "https://", slash + "Users" + slash,
        slash + "private" + slash, "rpc_url", "cookie", "authorization",
    ):
        self.assertNotIn(forbidden, rendered.lower())
    source = (PROJECT_ROOT / "scripts/shib_v2_research.py").read_text(encoding="utf-8")
    for forbidden in ("fetch_cex", "usdt", "uniswap_v3", "connector", "dashboard"):
        self.assertNotIn(forbidden, source.lower())
```

Also test missing evidence returns nonzero with `evidence_not_evaluated`,
symlink/oversized/noncanonical inputs fail, and `--help` works from both working
directories. For both missing and present-invalid evidence, pre-create output
with `b"old-output\n"` and assert the command leaves those bytes unchanged;
repeat with no prior output and assert no output is created.

- [ ] **Step 6: Run all new tests and relevant regressions**

Run: `python3 -m unittest tests.test_shib_v2_research tests.test_fetch_dex_depth tests.test_route_quantity tests.test_route_opportunity tests.test_route_publication tests.test_route_cost_evidence tests.test_framework -v`

Expected: PASS.

- [ ] **Step 7: Commit the build CLI and contract**

```bash
git add scripts/build_shib_v2_research_snapshot.py docs/shib-v2v2-research-contract.md tests/test_shib_v2_research.py
git commit -m "docs(research): define SHIB V2 replay contract"
```

Record this commit's full SHA as `APPLICATION_SHA`; later artifact generation
must use that exact value and must not substitute the eventual data commit.

### Task 6: Capture and Review One Real Finalized Evidence Generation

**Files:**
- Create: `data/public/research/shib-v2v2/evidence.json`

**Interfaces:**
- Consumes: the checked-in registry and capture command; two independently
  operated public Ethereum endpoints provided only as process inputs.
- Produces: one bounded `shib_v2_research_evidence/v1` generation whose
  registry, block, calls, provider observations, decoded entities, quality,
  and identity all validate offline.

- [ ] **Step 1: Capture to a temporary candidate path with fixed opaque labels**

Set process-local `SHIB_RPC_URL_A` and `SHIB_RPC_URL_B` without writing their
values to a file, then run:

```bash
python3 scripts/capture_shib_v2_research_evidence.py \
  --registry config/shib_v2_research_pools.json \
  --rpc-url-a "$SHIB_RPC_URL_A" \
  --rpc-url-b "$SHIB_RPC_URL_B" \
  --output /tmp/shib-v2v2-evidence.candidate.json
```

The command internally persists fixed labels `provider_a` and `provider_b` and
resolves one finalized block; callers do not select a block or label. Invoke it
through a protected shell/session so expanded URLs are not copied into Git,
evidence, test output, or documentation.

Expected: exit 0 and one canonical candidate file. If either provider lacks
EIP-1898/archive state or disagrees, stop with the stable reason and select a
different independent public provider; never relax the two-provider rule.

- [ ] **Step 2: Run independent offline evidence validation and metric checks**

```bash
python3 -c 'import json; from pathlib import Path; from scripts.shib_v2_research import load_research_registry,validate_research_evidence; r=load_research_registry(json.loads(Path("config/shib_v2_research_pools.json").read_text())); e=json.loads(Path("/tmp/shib-v2v2-evidence.candidate.json").read_text()); v=validate_research_evidence(e,r); q=v["collection_quality"]; assert q["state"]=="evaluated"; assert q["expected_logical_call_count"]==q["observed_logical_call_count"]==q["usable_logical_call_count"]; assert q["expected_provider_observation_count"]==q["observed_provider_observation_count"]==q["usable_provider_observation_count"]; assert q["duplicate_logical_call_key_count"]==q["duplicate_provider_observation_key_count"]==q["required_field_null_count"]==q["missing_null_count"]==q["provider_disagreement_count"]==0'
```

Separately recompute `registry_sha256()` over canonical semantic JSON (without
the file newline), every logical result SHA, block projection SHA, fee evidence
SHA, evidence identity, and public file size.
Confirm both pairs round-trip through the correct factory and contain only
SHIB/WETH.

Recompute evidence identity as SHA-256 of
`b"shib-v2-research-evidence/v1\n" + canonical_json_bytes(body_without_identity)`;
do not include the file newline.

- [ ] **Step 3: Security-scan the exact candidate bytes**

Scan for URL schemes, query strings, authorization/cookie/key terms, common
token formats, emails, macOS home-root or system temporary-root fragments,
home-directory names, provider error text, RPC envelopes, and account/wallet
labels. Confirm the only provider values are `provider_a` and `provider_b`, and
all result data are bounded hex members from the closed inventory.

Expected: no finding. Any finding invalidates the candidate rather than being
redacted in place; fix capture and repeat at a new finalized block.

- [ ] **Step 4: Copy the validated canonical bytes to the tracked evidence path**

Use `apply_patch` for the tracked-file addition. Then compare its bytes to the
validated candidate and rerun `validate_research_evidence` from the tracked
path.

- [ ] **Step 5: Run evidence tests against the real tracked file**

Add a test that loads the repository registry and tracked evidence, validates
it, asserts exact registry SHA, exact block number/hash, exact evidence
identity, evaluated quality, two providers per logical call, and no forbidden
public text. Do not assert wall-clock freshness.

Run: `python3 -m unittest tests.test_shib_v2_research -v`

Expected: PASS.

- [ ] **Step 6: Commit only evidence and its integration assertions**

```bash
git add data/public/research/shib-v2v2/evidence.json tests/test_shib_v2_research.py
git commit -m "data(research): record SHIB V2 fixed-block evidence"
```

Do not commit RPC configuration, logs, temporary files, raw envelopes, or any
`data/local` content.

### Task 7: Publish the Deterministic Research Snapshot

**Files:**
- Create: `data/public/research/shib-v2v2/latest.json`
- Modify: `tests/test_shib_v2_research.py`

**Interfaces:**
- Consumes: the tracked registry, tracked evidence, and `APPLICATION_SHA` from Task 5.
- Produces: one canonical `shib_v2_research_snapshot/v1` with exactly ten historical scenarios and a clean-checkout byte-parity test.

- [ ] **Step 1: Generate a candidate snapshot with the implementation SHA**

```bash
python3 scripts/build_shib_v2_research_snapshot.py \
  --registry config/shib_v2_research_pools.json \
  --evidence data/public/research/shib-v2v2/evidence.json \
  --application-sha "$APPLICATION_SHA" \
  --output /tmp/shib-v2v2-latest.candidate.json
```

Expected: exit 0, ten scenarios, historical mode, zero executable/strict rows,
and a self-hash that independently recomputes.

- [ ] **Step 2: Add the tracked snapshot and byte-parity RED test**

Use `apply_patch` to add the exact candidate bytes to
`data/public/research/shib-v2v2/latest.json`, then add:

```python
def test_checked_in_snapshot_regenerates_byte_for_byte(self):
    expected = (PUBLIC_ROOT / "latest.json").read_bytes()
    rebuilt = build_research_snapshot(
        validate_research_evidence(self.evidence, self.registry),
        self.registry,
        json.loads(expected)["application_sha"],
    )
    self.assertEqual(expected, canonical_json_bytes(rebuilt) + b"\n")
```

Temporarily mutate one scenario or the checked-in self-hash and confirm the
test fails before restoring the candidate bytes.

- [ ] **Step 3: Validate classification and missing-cost semantics on real data**

Assert the exact classification counts recompute from ten scenarios; every
available row has all five stable limitations; all seven missing cost/net
fields are JSON null; every unavailable row has null edge fields; every
positive row is `positive_pool_edge_costs_incomplete`; and no persisted
classification contains `opportunity`.

- [ ] **Step 4: Re-run the CLI and compare exact bytes**

Generate into a second temporary path with the same application SHA and run a
byte comparison against the checked-in `latest.json`. Independently recompute
`snapshot_sha256` as SHA-256 of
`b"shib-v2-research-snapshot/v1\n" + canonical_json_bytes(body_without_hash)`.

Expected: byte-identical and both hashes valid.

- [ ] **Step 5: Run focused and relevant regression suites**

Run: `python3 -m unittest tests.test_shib_v2_research tests.test_fetch_dex_depth tests.test_route_quantity tests.test_route_opportunity tests.test_route_publication tests.test_route_cost_evidence tests.test_framework -v`

Expected: PASS.

- [ ] **Step 6: Commit the public research snapshot**

```bash
git add data/public/research/shib-v2v2/latest.json tests/test_shib_v2_research.py
git commit -m "data(research): publish SHIB V2 research snapshot"
```

The snapshot's `application_sha` remains the Task 5 implementation SHA; the
Task 7 data commit must not be written into the snapshot.

### Task 8: Independent Acceptance Review and Remote Push

**Files:**
- Review only: all files changed from `9249e4d179a35f2202ab40e53f39683999d95b73` to branch HEAD.

**Interfaces:**
- Consumes: all preceding commits and artifacts.
- Produces: fresh test/security/replay evidence, an independent code/data-quality review, and a verified remote branch SHA.

- [ ] **Step 1: Verify scope and worktree cleanliness**

Run: `git diff --name-only 9249e4d179a35f2202ab40e53f39683999d95b73...HEAD`

Expected: only the registry, four dedicated scripts, one dedicated test file,
the design/plan/contract documents, and two public research JSON files. Confirm
no V3, CEX, USDT, Funding, connector, canary, dashboard, workflow, route-cost,
route-pointer, production, deployment, raw, or `data/local` file changed.

- [ ] **Step 2: Run fresh syntax and focused tests sequentially**

```bash
python3 -m py_compile scripts/shib_v2_research.py scripts/shib_v2_research_io.py scripts/capture_shib_v2_research_evidence.py scripts/build_shib_v2_research_snapshot.py
python3 -m unittest tests.test_shib_v2_research -v
python3 -m unittest tests.test_fetch_dex_depth tests.test_route_quantity tests.test_route_opportunity tests.test_route_publication tests.test_route_cost_evidence tests.test_framework -v
git diff --check 9249e4d179a35f2202ab40e53f39683999d95b73...HEAD
```

Expected: all PASS and no diff-check output. Run suites sequentially to avoid
the repository's known cross-process timing/call-count interference.

- [ ] **Step 3: Rebuild from a clean archive/worktree and compare bytes**

Create a clean detached worktree or archive at HEAD, run the offline build CLI
with the persisted `application_sha`, and compare generated bytes with tracked
`latest.json`. Revalidate the tracked evidence and snapshot without relying on
test fixture helpers.

Expected: exact byte parity, valid evidence identity, valid snapshot self-hash,
ten unique scenario keys, and no network access during replay.

- [ ] **Step 4: Run a fresh public-artifact security scan**

Scan the exact registry/evidence/snapshot bytes for RPC URLs, tokens,
authorization/cookies, emails, absolute paths, provider errors,
wallet/account labels, raw JSON-RPC envelopes, and mutable runtime paths. Scan
the full branch diff separately for actual credentials, endpoint values, user-
specific paths, or unreviewed provider payloads; test vectors that construct
forbidden patterns without containing real secrets are allowed. Confirm file
sizes are below 1 MiB and all three files are regular non-symlinks.

Expected: no finding.

- [ ] **Step 5: Request independent two-stage review**

First reviewer checks spec/plan compliance, mathematical semantics, states,
lineage, null/zero separation, and file boundaries. Second reviewer performs
adversarial code/data review: duplicate keys, reorg/block-hash binding,
provider disagreement, ABI bounds, authority drift, self-hash mutation,
privacy leakage, deterministic replay, and false opportunity/executable
claims. Resolve every Critical/Important/Minor finding with RED-to-GREEN tests
and rerun Step 2 after the final change.

- [ ] **Step 6: Push only the target branch and verify GitHub SHA**

```bash
git push -u origin codex/shib-v2v2-research-loop
git ls-remote --heads origin refs/heads/codex/shib-v2v2-research-loop
```

Expected: the remote SHA exactly equals local `git rev-parse HEAD`. Do not push
`main`, the V3 branch, or any other ref; do not deploy.

- [ ] **Step 7: Report evidence and limitations separately**

Report the actual base SHA, branch/final SHA, exact block number/hash/time,
registry/evidence/snapshot hashes, pool identities, provider agreement counts,
ten-scenario coverage and classifications, test totals, security/replay checks,
remote push verification, and whether real public RPC data was accessed.
Separately retain the explicit blockers: gas, router fee, SHIB transfer-tax
behavior, MEV, atomic route simulation/executor, connector/canary expansion,
V3, CEX/USDT/USD, and production deployment. Do not call a positive static
pool edge an opportunity or arbitrage.
