# SHIB V2/V2 Historical Replay Consumer Contract

## Scope and fixed semantics

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

This family is a deterministic, point-in-time research comparison of the
registry-fixed Uniswap V2 and ShibaSwap V1 SHIB/WETH pools on Ethereum. A
snapshot contains exactly two directed routes by five requested USD notionals,
so its scenario inventory contains exactly ten rows. The primary scenario key
is `(route_id, requested_notional_usd)`; duplicates, missing rows, extra rows,
or a different order are invalid.

The output is a historical replay, even when the source evidence was captured
recently or the output filename is `latest.json`. Regeneration does not extend
freshness and does not turn the snapshot into a current market observation.

## Authority, collection, and exact inventory

`config/shib_v2_research_pools.json` is the identity authority. The Ethereum
state at the single bound canonical block is the observation authority. The
evidence generation key is `(registry_sha256, chain_id, block_hash)`.

Before collection, the registry expands to exactly 35 logical state reads and
70 provider observations:

- nine `eth_getCode` reads: both factories, routers, and pairs; SHIB; WETH; and
  the Chainlink ETH/USD proxy;
- two factory `getPair(SHIB,WETH)` reads;
- `factory()` and `WETH()` on each router;
- `factory()`, `token0()`, `token1()`, and `getReserves()` on each pair;
- `decimals()` on SHIB and WETH;
- SHIB and WETH `balanceOf(pair)` for each pair;
- `totalFee()`, `alpha()`, and `beta()` on the ShibaSwap pair; and
- `decimals()`, `description()`, and `latestRoundData()` on the Chainlink
  proxy.

Chain ID, selection of the finalized header, and by-hash header rereads are
capture preflight and are not part of the 35 logical state reads. Every state
call uses EIP-1898 with the same `blockHash` and
`requireCanonical: true`. There is no block-number fallback.

Exactly two distinct configured providers must independently return the same
finalized header and byte-identical result for every logical call. Each logical
call therefore has exactly two observations. Missing state, EIP-1898 rejection,
provider disagreement, non-canonical state, an identity mismatch, or any
missing, duplicate, or extra call invalidates the whole generation. There is no
single-provider publication, majority vote, partial-row publication, or
inventory shrink after failure.

## Collection quality

Evidence contains these recomputed `collection_quality` fields:

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

A valid generation has `state: evaluated`; all logical-call counts are 35; all
provider-observation counts are 70; provider agreement is 35; and duplicate,
required-null, missing-null, and disagreement counts are zero. The validator
recomputes these fields from the closed inventory and rejects a supplied
summary that differs.

Pool authority additionally requires registry, runtime code, factory
`getPair`, router factory and WETH, pair factory and token order, token
decimals, reserves, token balances, and DEX-specific fee evidence to agree.
Balances must equal reserves at the bound block. The USD reference is only the
registry-pinned Chainlink ETH/USD proxy at that same block; no exchange price,
stablecoin-parity assumption, or fallback source is permitted.

## Finality, time, and freshness

Both providers must select the same block through Ethereum's `finalized` tag
and successfully reread it by hash. Block number, hash, parent hash, timestamp,
state root, base fee, and the canonical header projection must remain
consistent across all records. All published times are canonical UTC.

Chainlink `updated_at` must be positive, no later than the block timestamp, and
no older than the registry's 3,600-second maximum age at that block. Pair
reserve timestamps and their lags are retained as observed; they are not
rewritten to the block time. A snapshot never claims freshness beyond this
fixed-block evidence.

## Calculation and scenario states

Each route uses one common raw SHIB quantity derived from the requested USD
notional, the two fixed-block pool reference prices, and the fixed-block
ETH/USD rational. The buy leg is an exact-output V2 quote; the sell leg is an
exact-input V2 quote. Integer and exact rational arithmetic preserve EVM
rounding and token order.

Scenario classifications mean:

- `non_positive_pool_edge`: both pool quotes are complete and the
  pool-fee-adjusted gross edge is zero or negative;
- `positive_pool_edge_costs_incomplete`: the pool-only gross edge is positive,
  but required route costs and atomic execution are not evaluated; and
- `unavailable`: required identity, price, quantity, or quote evidence is not
  usable.

Every scenario has `strict_eligible: false` and `executable: false`. For every
available scenario, these fields are always JSON null:

```text
network_gas_usd
router_or_integrator_fee_usd
token_transfer_tax_usd
mev_cost_usd
atomic_execution_cost_usd
net_edge_usd
net_edge_bps
```

Measured zero and missing data are different states. A valid measured zero is
stored as numeric zero and counted in `measured_zero_count`. A cost that was not
measured is null, never zero. An unavailable quote keeps dependent quote and
edge values null; it does not become a zero-edge scenario. NaN, Infinity,
binary floats, exponent-form decimal text, negative unsigned values, and ABI
overflows are invalid.

## Lineage, hashes, shape, and privacy

The snapshot binds `application_sha`, `registry_sha256`,
`evidence_identity`, the single block identity, pool state identities, pool
call-result hashes, and fee-evidence hashes. `application_sha` must be exactly
40 lowercase hexadecimal characters.

`evidence_identity` is SHA-256 over
`b"shib-v2-research-evidence/v1\n"` followed by canonical JSON with the
identity field omitted. `snapshot_sha256` is SHA-256 over
`b"shib-v2-research-snapshot/v1\n"` followed by canonical JSON with the
self-hash omitted. The trailing file newline is not hashed. Both validators
recompute their self-hashes and all dependent lineage; the public snapshot
validator rebuilds the complete expected snapshot from the supplied evidence
and registry before accepting it.

Registry, evidence, and snapshot inputs are canonical UTF-8 JSON regular files
of at most 1 MiB. Symlinks, non-regular files, duplicate keys, excessive JSON
depth or members, oversized string, integer, or result tokens, unknown fields,
and non-canonical bytes fail closed. Output is scanned and atomically replaced
only after the complete snapshot passes authority-bound validation.

Public evidence and snapshots contain only reviewed public-chain facts,
canonical identities, bounded result bytes, opaque provider labels, and
derived research values. They must not contain endpoint URLs, headers, query
strings, credentials, cookies, authorization material, account or wallet
labels, arbitrary provider errors, environment values, private input names, or
absolute local paths. Raw provider payloads and local capture configuration
remain private and outside the public artifact boundary.

## Offline build and byte-for-byte replay

The build command requires all four inputs explicitly and has no mutable input
defaults, network fallback, or wall-clock option:

```bash
python3 scripts/build_shib_v2_research_snapshot.py \
  --registry config/shib_v2_research_pools.json \
  --evidence data/public/research/shib-v2v2/evidence.json \
  --application-sha <40-lowercase-implementation-commit-sha> \
  --output /tmp/shib-v2v2-replayed.json
cmp data/public/research/shib-v2v2/latest.json /tmp/shib-v2v2-replayed.json
```

With identical registry bytes, evidence bytes, and application SHA, `cmp`
must report byte identity. Canonical object-key order and the single trailing
newline are part of the file representation. Filesystem paths, file metadata,
the current time, provider ordering, and working directory do not enter the
snapshot.

If the evidence path is absent, the CLI exits 2, writes only
`evidence_not_evaluated` to standard error, and does not create or replace the
output. It must not synthesize a zero-call or zero-scenario snapshot. If an
evidence file is present but invalid, or replay/validation/write fails, the CLI
exits 1 with `evidence_failed` and also leaves an absent or pre-existing output
unchanged. Invalid registry input exits 1 with `registry_invalid` and likewise
does not touch the output.

## Explicit non-claims

This family makes no claim of live or current ranking, 30-day coverage, daily
completeness, atomic execution, executable routing, or an opportunity. It does
not cover V3, CEX markets, USDT, connectors, dashboards, or production. It does
not estimate gas, router or integrator fees, token transfer tax, MEV, or atomic
execution cost. A positive pool-only edge is a historical research fact with
incomplete costs, not a trading instruction.
