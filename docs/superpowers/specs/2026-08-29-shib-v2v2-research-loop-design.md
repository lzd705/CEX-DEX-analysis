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

- one Ethereum block number and hash, independently agreed by two providers,
  shared by both pools and the ETH/USD feed;
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

This pure module owns schemas, canonicalization, validation, privacy scanning,
and calculation. It does not open sockets, read paths, or write files. Its
public boundaries are:

```python
load_research_registry(payload: object) -> dict
validate_research_evidence(payload: object, registry: dict) -> dict
build_research_snapshot(
    evidence: dict,
    registry: dict,
    application_sha: str,
) -> dict
canonical_json_bytes(payload: object) -> bytes
validate_research_snapshot(
    payload: object,
    evidence: dict,
    registry: dict,
) -> dict
```

The module reuses `V2PoolState`, `MarketRules`, `CommonTarget`, and the exact V2
quote functions from `scripts/route_quantity.py`. It does not copy the V2
formula into a second implementation.

### `scripts/shib_v2_research_io.py`

This narrow I/O module owns bounded no-symlink JSON reads and same-directory
atomic canonical JSON writes. It imports the pure validators but contains no
network logic. Capture and replay CLIs use it so filesystem concerns do not
leak into `scripts/shib_v2_research.py`.

### `scripts/capture_shib_v2_research_evidence.py`

This command is the only network-capable part of the increment. It accepts a
registry path, an explicit output path, and two RPC URLs supplied at runtime.
The persisted opaque labels are fixed as `provider_a` and `provider_b`; callers
cannot vary them and thereby change evidence identity. The URLs are
process-only inputs. The CLI and in-process capture boundary reject equal
endpoint identities before any request; this proves only that the configured
endpoint strings differ, not that their hidden infrastructure is independent.
Each endpoint must be HTTPS with no URL userinfo. The transport makes a direct,
TLS-authenticated HTTPS connection using the platform trust store, disables
ambient HTTP(S) proxies explicitly, and therefore cannot inherit proxy routes
or proxy credentials from process environment variables. Query or path
credentials, when an operator requires them, remain process-only URL material
and are never projected into an artifact or error.
It:

1. resolves one finalized Ethereum header;
2. re-reads that header by hash and verifies number/hash/parent/timestamp;
3. uses EIP-1898 `{blockHash, requireCanonical: true}` for every `eth_call` and
   `eth_getCode` state read;
4. executes the complete registry-derived call inventory against both
   providers at that exact block hash;
5. requires byte-identical reviewed results from both providers;
6. verifies factory `getPair`, pair factory, token ordering, code, decimals,
   reserves, balances, fee parameters, and Chainlink round data;
7. writes only the bounded result hex, provider-independent result hashes, and
   decoded public fields needed for replay.

If either provider rejects EIP-1898, lacks the state, returns a non-canonical
block, disagrees on a required byte, or exposes an identity mismatch, capture
fails closed. A retry may repeat the same call at the same block hash but may
not select a new block or change providers. There is no block-number fallback,
single-provider publication, or majority-vote repair. RPC URLs and provider
error bodies are never written to the evidence file or included in the
exception returned by the CLI.

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

## Collection quality gate

The checked-in evidence is publishable only when every rule in this section
passes. These are executable contract requirements, not documentation-only
expectations.

### Intended grain and keys

| Entity | Grain | Candidate primary key |
| --- | --- | --- |
| evidence generation | one registry at one canonical Ethereum block | `(registry_sha256, chain_id, block_hash)` |
| logical RPC call | one method, call target, and calldata at the fixed block | `(block_hash, method, to_address, calldata_sha256)` |
| provider observation | one provider result for one logical call | `(provider_label, block_hash, method, to_address, calldata_sha256)` |
| token | one canonical token identity at the fixed block | `(block_hash, token_address)` |
| pool | one canonical DEX pair state at the fixed block | `(block_hash, dex, pair_address)` |
| USD reference | one Chainlink proxy round visible at the fixed block | `(block_hash, proxy_address, round_id)` |
| research scenario | one directed route and requested USD notional | `(route_id, requested_notional_usd)` |

Addresses are lower-case canonical EVM addresses before key construction.
Near-duplicates caused by address case, whitespace, decimal formatting, or
provider ordering are rejected rather than silently normalized into multiple
records.

### Authoritative sources

The registry is the identity authority. The Ethereum state at the bound block
is the observation authority. Documentation URLs may explain an authority
record during review, but they are not runtime data and are not projected into
the public evidence.

The publication trust anchor is the reviewed dual-provider capture process,
the expected registry/evidence identities, and the Git commit that publishes
the reviewed canonical bytes. The generic evidence validator proves schema,
hash, authority-record, and internal state consistency against the supplied
registry. Because public evidence retains opaque labels and agreed result
hashes rather than signed provider attestations or Ethereum state proofs, the
validator does not by itself prove that either provider returned those bytes
or that the bytes are members of the claimed state root. Snapshot validation
rebuilds every derived value from evidence and registry, but inherits that
publication trust boundary.

For every pool, authority requires all of the following to agree:

- registry chain, factory, router, pair, SHIB, WETH, runtime-code hashes, and
  fee model;
- factory `getPair(SHIB,WETH)`;
- router `factory()` and `WETH()`;
- pair `factory()`, `token0()`, and `token1()`;
- token decimals and runtime code;
- pair reserves and both ERC-20 `balanceOf(pair)` values;
- DEX-specific fee evidence.

The ETH/USD authority is the registry-pinned Chainlink proxy read through
`AggregatorV3Interface` at the same block hash. No exchange ticker, current
HTTP price, stablecoin parity assumption, or fallback feed is allowed.

### Exact expected inventory

The registry deterministically expands into one closed call inventory before
any request is sent. Chain ID, two finalized-header reads, and the two by-hash
header rereads are capture preflight rather than logical state reads. The state
inventory contains exactly 35 logical reads and 70 provider observations:

- runtime code for every unique factory, router, pair, token, and feed proxy
  (9 reads);
- one `getPair(SHIB,WETH)` per factory;
- `factory` and `WETH` per router;
- `factory`, `token0`, `token1`, and `getReserves` per pair;
- `decimals` for SHIB and WETH, plus both `balanceOf(pair)` calls per pair;
- the registry-declared ShibaSwap pair calls `totalFee`, `alpha`, and `beta`;
- Chainlink `decimals`, `description`, and `latestRoundData`.

Every logical call must have exactly two provider observations and one agreed
result. The expected inventory cannot shrink after a failed call. Unknown,
extra, duplicate, or missing calls fail the generation. A pool with only one
usable call is not partial coverage and cannot enter calculation.

### Completeness, uniqueness, and quality metrics

Evidence contains a `collection_quality` object with:

```text
state
expected_logical_call_count
observed_logical_call_count
usable_logical_call_count
expected_provider_observation_count
observed_provider_observation_count
usable_provider_observation_count
duplicate_logical_call_key_count
duplicate_provider_observation_key_count
required_field_null_count
measured_zero_count
missing_null_count
provider_agreement_count
provider_disagreement_count
status_counts
```

For a publishable generation, `state` is `evaluated`; all three logical-call
counts are equal; all three provider-observation counts equal twice the logical
count; duplicate, required-null, missing-null, and disagreement counts are
zero; and agreement count equals the logical-call count. The validator
recomputes every metric and rejects supplied summaries that do not match the
records.

A failed live capture returns one allowlisted reason such as
`provider_disagreement`, `canonical_block_unavailable`,
`required_call_missing`, `pool_authority_mismatch`, `fee_authority_mismatch`,
or `usd_reference_unavailable`. It does not write a stable-looking evidence
file. If a consumer is asked to build from an absent evidence file, the family
is `not_evaluated`; it is never represented as zero calls or zero coverage.

### Null, zero, and numeric validity

Required on-chain values cannot be null. Required result hex cannot be empty.
Chain ID, block number, timestamps, round ID, price answer, decimals, reserves,
and code length obey their ABI integer bounds. Reserves and the ETH/USD answer
must be positive.

A measured zero that is valid for its field, such as a zero protocol-fee
recipient, is retained and counted in `measured_zero_count`. Missing route-cost
families are null and counted separately; they are never converted to zero.
NaN, Infinity, exponent-form decimal text, binary floats, negative unsigned
values, and values exceeding ABI bounds are rejected.

### Time, finality, and freshness

All times are canonical UTC. The block must be returned by the `finalized` tag
from both providers and must round-trip by hash. All state calls use that hash.
No observation from a different block can be joined into the generation.

Chainlink `updated_at` must be positive, no later than the block timestamp, and
no older than the registry maximum. Pair reserve timestamps are recorded and
their lag is reported per pool; an old reserve timestamp is not rewritten to
the block time. Because balances must equal reserves, it is an observed
no-update interval rather than an inferred missing observation.

The public snapshot is always historical even if captured recently. It never
extends freshness from the replay time and never becomes current merely
because `latest.json` was regenerated.

This dataset has point-in-time grain, not daily grain. A 30-day date-span or
daily completeness claim is therefore prohibited and is not part of its
quality summary.

### Consistency and integrity

- block number, hash, timestamp, state root, and canonical-header hash agree
  across providers and every entity;
- every child entity refers to the single evidence generation;
- pool token balances equal the decoded reserves at the fixed block;
- fee bps, numerator, denominator, formula, and native fee parameters
  recompute exactly;
- Chainlink answer and decimals recompute the same exact ETH/USD rational used
  in every scenario;
- the ten scenario keys equal the Cartesian product of two routes and five
  notionals, with no orphan or extra row;
- evidence, registry, application, and snapshot hashes form an unbroken
  lineage and are independently recomputed on load.

Any integrity mismatch invalidates the complete generation; the loader does
not drop the bad row and continue.

### Shape, size, and privacy limits

Registry, evidence, and snapshot files are regular non-symlink files no larger
than 1 MiB. JSON nesting, member counts, string lengths, and result-hex lengths
are bounded before full materialization. Duplicate JSON keys are rejected.

Provider labels are fixed opaque identifiers. URLs, headers, query strings,
credentials, cookies, account or wallet identities, arbitrary provider error
text, absolute paths, home-directory fragments, and environment values are not
allowed fields. Capture and replay run a final public-output scan before an
atomic write.

## Evidence contract

`shib_v2_research_evidence/v1` has these top-level members:

```text
schema
registry_sha256
chain
block
logical_calls
provider_observations
tokens
usd_reference
pools
collection_quality
evidence_identity
```

The evidence grain is one `(chain_id, block_hash, registry_sha256)` generation.
The block record retains number, hash, parent hash, timestamp, state root,
base-fee-per-gas, the hash of its canonical reviewed projection, and exactly
two reviewed header observations containing only provider label, canonical
header SHA-256, and status.

The in-memory inventory additionally retains the fixed selector declaration
`eip1898_block_hash_require_canonical`; it is a capture instruction, not an
observed fact. Each persisted `logical_calls` member is its strict projection:
a canonical logical call ID, fixed method, target, calldata, calldata SHA-256,
bounded result hex, and result SHA-256.
`eth_getCode` uses canonical empty calldata while remaining distinct by method.
Each
`provider_observations` member retains only the opaque provider label, logical
call ID, block hash, result SHA-256, and status. The validator requires exactly
two observations per logical call and requires both hashes to equal the stored
logical result. Provider URLs and full JSON-RPC envelopes are never stored.

Each pool record retains:

```text
dex, factory_address, router_address, pair_address
factory_runtime_code_sha256, router_runtime_code_sha256,
pair_runtime_code_sha256
factory_get_pair_result, router_factory_result, router_weth_result,
pair_factory_result
token0_address, token1_address, token0_decimals, token1_decimals
reserve0_raw, reserve1_raw, reserve_timestamp_last_raw
token0_balance_raw, token1_balance_raw
fee_bps, fee_numerator, fee_denominator, fee_formula
fee_parameters, fee_evidence_sha256, call_results_sha256
```

The validator requires `factory_get_pair_result == pair_address`,
`router_factory_result == factory_address`, `router_weth_result == WETH`,
`pair_factory_result == factory_address`, the exact SHIB/WETH token set and
ordering, non-empty code with matching registry hashes, uint bounds, nonzero
reserves, and balances equal to reserves. ShibaSwap `totalFee` must match the
normalized fraction. A missing field is invalid, not zero.

The Chainlink record retains proxy code hash, description, decimals, round ID,
positive answer, started/updated timestamps, answered-in-round, and call-result
hash. `updated_at` must not be in the future and must be within the registry's
maximum age at the fixed block. No alternative price source is inferred.

`evidence_identity` is the SHA-256 of the byte string
`b"shib-v2-research-evidence/v1\n"` followed by the canonical evidence object
excluding that field. Reordering object keys cannot change it; changing any
reviewed claim must change it. The trailing file newline is not hashed.

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

The only DEX values are `uniswap_v2` and `shibaswap_v1`. Quote reasons are
closed over the existing V2 quote contract: complete quotes use
`fixed_block_fee_proof_not_authenticated`; unavailable quotes may use only
`pool_state_binding_mismatch`, `pool_state_not_current`,
`market_rules_binding_mismatch`, `market_rules_not_current`,
`pool_state_market_mismatch`, `target_asset_mismatch`,
`pool_state_token_address_mismatch`, `pool_state_token_decimals_mismatch`,
`target_base_unit_misaligned`, `target_lot_misaligned`,
`minimum_base_quantity_not_met`, `pool_output_below_one_raw`,
`pool_reserve_insufficient`, or `minimum_notional_not_met`. Free text is never
published. Scenario `reason_codes` are derived deterministically. If either
leg is unavailable, both legs contribute their quote reason in buy-then-sell
order, including `fixed_block_fee_proof_not_authenticated` for a complete leg,
and duplicate reasons are removed after preserving their first occurrence.
Otherwise the list contains the complete-quote reason, followed by
`route_costs_not_evaluated` only for a positive pool edge whose route costs
remain missing.

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
ordering. `snapshot_sha256` hashes
`b"shib-v2-research-snapshot/v1\n"` followed by canonical JSON with that field
omitted. The trailing file newline is not hashed.
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
- exact call-inventory expansion, expected/observed/usable parity, duplicate
  keys, recomputed quality metrics, and two-provider agreement;
- wrong chain, factory, router, pair, token, token ordering, code hash, fee, or
  `getPair` round trip;
- block-number/hash mismatch, changed header, noncanonical block, and EIP-1898
  rejection by either provider;
- divergent provider header, code, call result, missing observation, retry that
  attempts to change block, and attempted single-provider fallback;
- missing, malformed, zero, negative, overflowed, or mismatched reserves and
  balances;
- ShibaSwap fee-parameter mismatch and Uniswap fee-authority mismatch;
- stale, future, zero, negative, or wrong-decimal ETH/USD round;
- exact-output buy and exact-input sell integer rounding in both directions;
- ten-scenario completeness and requested-notional ordering;
- negative edge, zero edge, positive pool comparison, and unavailable states;
- missing costs remain null and never become zero;
- valid measured zero remains distinct from missing/null and is counted
  separately;
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

1. the two pool authorities and one dual-provider fixed-block evidence
   generation pass every collection-quality gate;
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
