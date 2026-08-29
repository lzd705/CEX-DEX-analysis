# SHIB V2/V2 Research Loop Design

## Status and decision

This design completes a deterministic, research-only SHIB DEX-DEX loop on
Ethereum. It does not create an executable arbitrage system.

The loop uses exactly two canonical SHIB/WETH constant-product pools:

- Uniswap V2 pair `0x811beed0119b4afce20d2583eb608c6f7af1954f`;
- ShibaSwap V1 pair `0xcf6daab95c476106eca715d48de4b13287ffdeaa`.

ShibaSwap V1 is described as V2-style because it uses the same full-range
constant-product model. This work does not inspect, change, or consume the
separate Uniswap V3 or ShibaSwap V2 concentrated-liquidity work.

The implementation will capture a bounded set of public Ethereum calls at one
canonical finalized block, replay those bytes offline, calculate both directed
routes at the existing five USD notionals, and publish one canonical research
snapshot. A negative comparison is a valid result. A positive pool comparison
with incomplete route costs is only a research lead, never an opportunity.

## Alternatives considered

### Chosen: bounded evidence plus offline replay

A dedicated SHIB research registry pins the factories, pairs, tokens, fee
models, and ETH/USD feed. A capture command writes only reviewed public-chain
responses. A separate replay command has no network fallback and produces a
deterministic snapshot from that evidence.

This is the smallest approach that is reproducible from a clean checkout,
retains exact integer AMM arithmetic, and cannot silently inherit mutable
runtime discovery or private connector state.

### Rejected for this increment: immediate complete route-opportunity bundle

The existing complete route bundle is structurally reusable, but its strict
route-cost authority is currently pinned to one UNI/WETH Uniswap V2 adapter and
uses a Binance ETH/USDT reference. Extending it would require new funding,
router, token-transfer, gas, and atomic-route authorities. Calling that work a
small SHIB data change would hide the largest risks.

The research snapshot will retain identities and fields that can later be
projected into `route_opportunity/v1`, but it will not publish a route pointer
or enter the Opportunities API in this increment.

### Deferred: strict adapter and atomic execution simulation

A strict implementation would require an executor or equivalent atomic call,
exact calldata, sender and allowance state, final balance deltas, gas evidence,
router-fee evidence, token-transfer behavior, and MEV policy. It would also
require separately reviewed Uniswap V2 and ShibaSwap V1 authorities. This is
not necessary to close the research loop and is explicitly deferred.

## Scope

### Included

- one Ethereum block number and hash shared by both pools and the ETH/USD feed;
- canonical factory-to-pair and pair-to-factory round trips;
- pair and token runtime-code hashes;
- `token0`, `token1`, token decimals, reserves, reserve timestamp, and pair
  token balances;
- Uniswap V2 30 bps fee authority and ShibaSwap V1 block-specific fee
  parameters;
- Chainlink ETH/USD proxy evidence at the same block, used only for USD
  projection and the five-notional grid;
- exact V2 integer quoting for both directed routes at USD 1,000, 5,000,
  10,000, 50,000, and 100,000;
- canonical JSON input evidence, canonical JSON output, source identities,
  application SHA, and a self-hash;
- checked-in, bounded public-chain evidence and one checked-in research
  snapshot generated from it;
- stable tests for identity, arithmetic, missing data, determinism, and privacy.

### Excluded

- Uniswap V3 and ShibaSwap V2;
- CEX data, ETH/USDT, USDT/USD, Funding Rate, cross-chain routes, or bridges;
- wallets, keys, approvals, balances, order placement, transaction submission,
  flash loans, private relays, production connectors, timers, or deployment;
- fabricated gas, router-fee, token-tax, MEV, or atomic-execution values;
- modification of `config/route_cost_adapters.json`, route-cost authority files,
  route pointers, the Opportunities API, or dashboard ranking;
- publication of RPC URLs, headers, credentials, private paths, or unbounded
  provider error payloads.

## Files and responsibilities

### `config/shib_v2_research_pools.json`

The versioned authority registry contains exactly two pools and one ETH/USD
feed. It pins:

- chain name and chain ID;
- SHIB and WETH addresses and decimals;
- DEX ID, factory, router, pair, expected token ordering, and runtime-code
  hashes;
- fee denominator, normalized numerator, fee bps, formula identifier, and the
  DEX-specific fee evidence requirements;
- Chainlink proxy address, expected description and decimals, plus a maximum
  feed age;
- the exact five USD notionals.

No runtime discovery may add or replace a pool. The registry is canonical JSON
and its SHA-256 is part of every evidence and snapshot identity.

### `scripts/shib_v2_research.py`

This pure module owns schemas, canonicalization, validation, and calculation.
It does not open sockets or write files. Its public boundaries are:

```python
load_research_registry(payload: object) -> dict
validate_research_evidence(payload: object, registry: dict) -> dict
build_research_snapshot(
    evidence: dict,
    registry: dict,
    application_sha: str,
) -> dict
canonical_json_bytes(payload: object) -> bytes
validate_research_snapshot(payload: object) -> dict
```

The module reuses `V2PoolState`, `MarketRules`, `CommonTarget`, and the exact V2
quote functions from `scripts/route_quantity.py`. It does not copy the V2
formula into a second implementation.

### `scripts/capture_shib_v2_research_evidence.py`

This command is the only network-capable part of the increment. It accepts a
registry path, an explicit output path, and an RPC URL supplied at runtime. It:

1. resolves one finalized Ethereum header;
2. re-reads that header by hash and verifies number/hash/parent/timestamp;
3. uses EIP-1898 `{blockHash, requireCanonical: true}` for every `eth_call`;
4. verifies factory `getPair`, pair factory, token ordering, code, decimals,
   reserves, balances, fee parameters, and Chainlink round data;
5. writes only the bounded result hex and decoded public fields needed for
   replay.

If EIP-1898 is unsupported, state is pruned, the block is non-canonical, or an
identity differs, capture fails closed. There is no block-number fallback.
The RPC URL and provider error body are never written to the evidence file or
included in the exception returned by the CLI.

### `scripts/build_shib_v2_research_snapshot.py`

This offline command reads the registry and evidence, requires an explicit
40-character application SHA, builds the snapshot, validates the completed
object, and atomically writes canonical JSON. It never performs network I/O and
never searches `data/local` or mutable latest files.

### Tracked evidence and output

- `data/public/research/shib-v2v2/evidence.json` contains one bounded,
  reviewed public-chain evidence generation.
- `data/public/research/shib-v2v2/latest.json` is its deterministic research
  snapshot.

Neither file is a live feed. Both retain the exact historical block time and
hash. A clean checkout can regenerate `latest.json` byte-for-byte without
network access.

### Tests and contract documentation

- `tests/test_shib_v2_research.py` covers pure contracts and both CLIs.
- `docs/shib-v2v2-research-contract.md` documents grains, fields, state
  semantics, replay, and limitations for consumers.

No production collection or dashboard files are modified.

## Evidence contract

`shib_v2_research_evidence/v1` has these top-level members:

```text
schema
registry_sha256
chain
block
tokens
usd_reference
pools
evidence_identity
```

The evidence grain is one `(chain_id, block_hash, registry_sha256)` generation.
The block record retains number, hash, parent hash, timestamp, state root,
base-fee-per-gas, and the hash of its canonical reviewed projection.

Each pool record retains:

```text
dex, factory_address, router_address, pair_address
factory_runtime_code_sha256, router_runtime_code_sha256,
pair_runtime_code_sha256
factory_get_pair_result, pair_factory_result
token0_address, token1_address, token0_decimals, token1_decimals
reserve0_raw, reserve1_raw, reserve_timestamp_last_raw
token0_balance_raw, token1_balance_raw
fee_bps, fee_numerator, fee_denominator, fee_formula
fee_parameters, fee_evidence_sha256, call_results_sha256
```

The validator requires `factory_get_pair_result == pair_address`,
`pair_factory_result == factory_address`, the exact SHIB/WETH token set and
ordering, non-empty code with matching registry hashes, uint bounds, nonzero
reserves, and balances equal to reserves. ShibaSwap `totalFee` must match the
normalized fraction. A missing field is invalid, not zero.

The Chainlink record retains proxy code hash, description, decimals, round ID,
positive answer, started/updated timestamps, answered-in-round, and call-result
hash. `updated_at` must not be in the future and must be within the registry's
maximum age at the fixed block. No alternative price source is inferred.

`evidence_identity` is the SHA-256 of the canonical evidence object excluding
that field. Reordering object keys cannot change it; changing any reviewed
claim must change it.

## Calculation and scenario grain

The output scenario grain is:

```text
(route_id, requested_notional_usd)
```

There are exactly two directed routes and five notionals, so a valid snapshot
contains exactly ten scenarios.

For each route and notional:

1. derive each pool's marginal SHIB/USD reference from its SHIB/WETH reserves
   and the fixed-block ETH/USD answer;
2. use the larger route-leg reference price to floor one common SHIB raw
   quantity onto the shared one-wei lattice;
3. quote the buy pool as exact-output SHIB and the sell pool as exact-input
   SHIB with the pool's normalized V2 fee;
4. convert the two WETH cash flows with the same fixed-block ETH/USD answer;
5. calculate the pool-fee-adjusted gross edge in WETH, USD, and basis points.

All reserve, quantity, and quote arithmetic uses integers or exact rational
values. Binary floating point is rejected. Measured zero remains zero; missing
values remain null.

## Status semantics

The snapshot is always labeled `historical_replay`. It never enters current or
executable ranking.

Each scenario has one of these classifications:

- `non_positive_pool_edge`: both pool quotes are complete and their
  pool-fee-adjusted gross edge is zero or negative;
- `positive_pool_edge_costs_incomplete`: the static pool comparison is
  positive, but unmeasured route costs and atomic execution prevent an
  opportunity claim;
- `unavailable`: required identity, price, quantity, or quote evidence is not
  usable.

`strict_eligible` and `executable` are always `false` in v1. The following
fields are always null rather than fabricated:

```text
network_gas_usd
router_or_integrator_fee_usd
token_transfer_tax_usd
mev_cost_usd
atomic_execution_cost_usd
net_edge_usd
net_edge_bps
```

Every available scenario lists the stable limitations
`network_gas_not_evaluated`, `router_fee_not_evaluated`,
`token_transfer_tax_not_evaluated`, `mev_not_evaluated`, and
`atomic_route_simulation_unavailable`. A positive pool comparison must not use
the word `opportunity` in its persisted classification.

The top-level summary separately counts the three classifications. It does not
turn unavailable scenarios into zero-edge scenarios.

## Snapshot contract and determinism

`shib_v2_research_snapshot/v1` contains:

```text
schema, application_sha, registry_sha256, evidence_identity
as_of_block_number, as_of_block_hash, as_of_utc
mode, token, quote_asset, requested_notionals_usd
pool_identities, scenario_count, summary, scenarios
snapshot_sha256
```

Pools, routes, notionals, limitations, and reason codes use fixed canonical
ordering. `snapshot_sha256` hashes canonical JSON with that field omitted.
Given identical registry bytes, evidence bytes, and application SHA, the output
must be byte-identical. Wall-clock time, UUIDs, absolute paths, directory
metadata, and RPC endpoint text do not enter the snapshot.

The checked-in `latest.json` is generated in two commits: implementation first,
then a data commit whose `application_sha` points to the implementation commit.
This avoids a self-referential Git SHA.

## Security and publication boundary

- Capture performs read-only JSON-RPC methods only.
- Capture never signs or submits a transaction.
- The RPC URL is accepted only as process input and is never projected.
- Public evidence allows only fixed-size hex call results and reviewed decoded
  public-chain fields; arbitrary provider payloads are rejected.
- Replay refuses symlinks, non-regular inputs, oversized files, duplicate JSON
  keys, unknown schema fields, noncanonical addresses, timestamps, hashes, and
  decimals.
- Output rejects absolute paths, home-directory fragments, URLs, authorization
  material, cookies, emails, wallet/account labels, and common secret formats.
- Writes use a same-directory temporary file, flush, and atomic replacement.
- No `data/local`, raw connector output, or production pointer is committed.

## Test strategy

Tests use bounded fixed-block fixtures and no network unless they are testing
the capture transport with a fake RPC server.

Required coverage:

- exact registry shape and exactly two canonical pools;
- wrong chain, factory, router, pair, token, token ordering, code hash, fee, or
  `getPair` round trip;
- block-number/hash mismatch, changed header, noncanonical block, and EIP-1898
  rejection;
- missing, malformed, zero, negative, overflowed, or mismatched reserves and
  balances;
- ShibaSwap fee-parameter mismatch and Uniswap fee-authority mismatch;
- stale, future, zero, negative, or wrong-decimal ETH/USD round;
- exact-output buy and exact-input sell integer rounding in both directions;
- ten-scenario completeness and requested-notional ordering;
- negative edge, zero edge, positive pool comparison, and unavailable states;
- missing costs remain null and never become zero;
- identical inputs produce identical canonical bytes and self-hash;
- any input mutation changes evidence/publication identity or fails validation;
- direct CLI execution from repository and external working directories;
- no V3/CEX/USDT/connector import or invocation;
- no RPC URL, secret, private path, unreviewed error, or raw arbitrary payload
  in evidence or output;
- clean-checkout regeneration matches the checked-in snapshot byte-for-byte;
- relevant existing `route_quantity`, DEX-depth, opportunity, and publication
  tests remain green.

## Delivery and acceptance

The work is complete only when:

1. the two pool authorities and one fixed-block evidence generation validate;
2. offline replay produces exactly ten scenarios;
3. the checked-in snapshot regenerates byte-for-byte from a clean checkout;
4. negative output is preserved as a result, while missing costs remain null;
5. no scenario is strict or executable;
6. focused and relevant regression tests pass;
7. a fresh security scan finds no secrets or private paths;
8. the branch is committed and pushed to
   `codex/shib-v2v2-research-loop` with its final SHA verified on GitHub.

No deployment, merge to `main`, timer activation, production data mutation, or
transaction execution is part of acceptance.
