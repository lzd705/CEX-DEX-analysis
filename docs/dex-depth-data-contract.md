# Point-in-time DEX pool-state depth contract

## What the number means

DEX depth answers:

> At one fixed blockchain block, how much USD quote notional can trade against
> this one pool before its marginal price reaches a specified distance from the
> starting price?

The published bands are 10, 25, 50, and 100 basis points. One hundred basis
points equals 1%. For each band the snapshot keeps:

- `sell_depth_*_usd`: quote value received while selling the configured Token;
- `buy_depth_*_usd`: quote value paid while buying the configured Token;
- `total_depth_*_usd`: sell plus buy depth;
- `depth_*_complete`: whether both sides reached the full band.

This is the DEX analogue of consuming CEX bids and asks out to a price band. It
is not calculated from TVL or historical volume.

## Fixed-block rule

The collector requests one block number per chain. Every pool-state `eth_call`
on that chain uses that exact block tag. The row stores the block number, RPC
endpoint without credentials/query parameters, observation time, raw transcript
hash, protocol model, token addresses/decimals, fee, and source token prices.
The endpoint field is either an allowlisted public scheme/host/optional-port
label or an opaque SHA-256 endpoint label; credentials, paths, queries, and
unapproved hostnames are never published.

For Uniswap V3 exact authority pools, the shared finalized block `F` has two
separate evidence roles. Numeric reads of `F` must match the scan manifest's
block number, hash, and timestamp exactly; those are the state calls used for
depth, execution, and the public exact receipt. Later `finalized` reads are only
checkpoint proofs that the chain's finalized head is still at or beyond `F`. If
that later head has advanced, its hash is not compared with `F`.

This prevents different calls for one snapshot from silently mixing states from
different blocks while allowing normal finalized-head advancement after the
fixed observation block has already been chosen.

## Supported models

### Constant-product V2

Supported in the first release:

- Uniswap V2;
- SushiSwap V2;
- ShibaSwap V1;
- PancakeSwap V2 on BNB Smart Chain and ZKsync Era.

For reserves `x` and `y`, invariant `k = x*y`, fee fraction `f`, and a downward
price factor `m = 1 - bps/10,000`, the net token-0 input needed to reach the
band is:

```text
net_input_0 = x * (1 / sqrt(m) - 1)
gross_input_0 = net_input_0 / (1 - f)
output_1 = y - k / (x + net_input_0)
```

The reverse direction uses `m = 1 + bps/10,000`. The implementation reads
`token0`, `token1`, `getReserves`, token decimals, and token symbols at the fixed
block. The original Uniswap V2 pair contract exposes `getReserves` and enforces
the fee-adjusted constant-product check in its swap function:
[Uniswap V2 pair source](https://github.com/Uniswap/v2-core/blob/master/contracts/UniswapV2Pair.sol).

ShibaSwap documents the same constant-product model and a fixed 0.30% V1 swap
fee:
[ShibaSwap V1 overview](https://docs.shib.io/shibaswap/shibaswap-v1/overview),
[ShibaSwap V1 fees](https://docs.shib.io/shibaswap/shibaswap-v1/fees).
PancakeSwap documents a fixed 0.25% V2 fee:
[PancakeSwap trading documentation](https://docs.pancakeswap.finance/trade/pancakeswap-exchange/trade).

### Concentrated-liquidity V3

Supported in the first release:

- Uniswap V3 on Ethereum, Arbitrum, Optimism, Base, and ZKsync;
- SushiSwap V3 on Ethereum;
- PancakeSwap V3 on BNB Smart Chain;
- Aerodrome Slipstream on Base;
- Velodrome Slipstream on Optimism.

The collector reads:

```text
slot0, liquidity, fee, tickSpacing, tickBitmap, ticks, token0, token1
```

It scans initialized ticks covering at least ±100 bps, integrates input/output
amounts through every active-liquidity segment, applies the pool swap fee to gross
input, and changes active liquidity when crossing an initialized tick. The
Uniswap V3 core contract defines those state fields and follows the same
tick-by-tick swap sequence:
[Uniswap V3 pool source](https://github.com/Uniswap/v3-core/blob/ed88be38ab2032d82bf10ac6f8d03aa631889d48/contracts/UniswapV3Pool.sol).

### Exact shared-engine canary scope

The Ethereum Uniswap V3 UNI/USDT 0.3% and UNI/WETH 0.3% pools listed in
`config/uniswap_v3_execution_markets.json` use the shared protocol-integer
engine for both the four depth bands and fixed-notional execution. The
collector binds one finalized block, verifies the canonical factory and
`factory.getPool` result, reads every initialized tick advertised by each
captured bitmap word, and rechecks the same block number and hash after the
pool reads and again after the final Quoter call. If any approved V3 pool is in
a chain cohort, that chain's block is selected from `finalized` before the
first pool, so input order cannot make the V3 pool inherit a newer head block.
The raw per-pool evidence contains the RPC transcript plus a
`uniswap_v3_tick_scan_manifest/v1` summary of the authority, word range,
initialized ticks, directional proof boundary, QuoterV2 parity, and terminal
reason.

The transcript also binds the exact GeckoTerminal source snapshot, response
time, endpoint, raw-response SHA-256, token identities, and USD prices used for
conversion. Publication requires every depth row and all ten matching
execution rows to carry the same per-market transcript SHA-256.

The configured bitmap radius is a proof bound, not an estimate. Depth is
complete only when both directions reach the exact integer 10/25/50/100 bps
price limits. Execution is complete only when the full integer target amount
resolves before that bound and its same-block QuoterV2 check matches. A bound
that is too short yields `partial`; an identity, RPC, arithmetic, transcript,
or parity defect yields `failed`. Other V3 markets continue through the
existing depth adapter but do not gain fixed-notional execution support from a
DEX-name match.

PancakeSwap documents its V3 fee tiers as 0.01%, 0.05%, 0.25%, and 1%; the
collector reads the actual fee from each pool rather than parsing the pool name:
[PancakeSwap trading documentation](https://docs.pancakeswap.finance/trade/pancakeswap-exchange/trade).
Aerodrome and Velodrome publish Slipstream as concentrated-liquidity contracts
adapted from Uniswap V3. The adapter reads each pool's live `fee()` value rather
than assuming the fee from its display name:
[Velodrome Slipstream source](https://github.com/velodrome-finance/slipstream).

## Unsupported is not zero

The first release deliberately marks the following as `unsupported` unless a
protocol-specific state adapter exists:

- Uniswap V4 pool IDs and hooks;
- Curve and Balancer invariants;
- Algebra/Camelot V3;
- Aerodrome and Velodrome stable/volatile V2 pools;
- SyncSwap and other ZKsync-specific invariants;
- Solana Raydium, Orca, Meteora, and other Solana account layouts.

These rows keep all depth values blank/JSON `null`. The site never substitutes
TVL, daily volume, or a generic constant-product approximation.

## Statuses

| Status | Meaning |
| --- | --- |
| `observed` | Both sides reached all four bands from fixed-block pool state |
| `partial` | Some measured side ran out of active liquidity before a band |
| `unsupported` | No protocol-specific, project-validated adapter exists for this chain/protocol model |
| `failed` | A supported adapter encountered an RPC, ABI, token, or validation error |

## USD conversion

Pool math is performed in integer token base units. Token decimals come from the
contracts. Immediately before an hourly DEX run, the pipeline refreshes
GeckoTerminal base/quote token prices into a temporary, non-published inventory.
The quote-side token is converted to USD using that response.

The two time inputs remain separate and auditable:

- `block_timestamp` is the fixed EVM pool-state time;
- `usd_price_observed_at` is when this project received the GeckoTerminal
  response containing the token USD prices.

Their absolute difference is the USD-price observation skew. Up to 15 minutes
is current; more than 15 minutes and no more than 2 hours is published with a
warning; more than 2 hours, a missing timestamp, or an invalid timestamp is
unusable. Unusable input produces no measured USD depth or execution value.
This is an observation-skew rule; GeckoTerminal does not expose the internal
event time of the price itself.

The collector also compares the pool-state implied target-token price with that
source price and stores midpoint-relative `price_difference_bps`. A large
difference is not silently corrected: it may identify a stale, tiny, or
off-market pool. The actual pool-state depth remains visible with its lineage.

## Files and publication

| Boundary | File | Meaning |
| --- | --- | --- |
| Public bundle 1/5 | `data/local/dex_depth_history.csv` | Normalized depth history keyed by snapshot and pool |
| Public bundle 2/5 | `data/local/dex_depth_latest.csv` | Latest published complete depth inventory |
| Public bundle 3/5 | `data/local/dex_depth_snapshot.csv` | Public current depth view |
| Public bundle 4/5 | `data/local/dex_execution_cost_latest.csv` | Latest execution scenarios derived from the same source cohort |
| Public exact sidecar 5/5 | `data/local/uniswap_v3_exact_latest.json` | Canonical two-authority-market validation receipt, replaced in the same bundle when exact scope is present |
| Private current 1/2 | `data/processed/dex_depth_snapshot.csv` | Candidate depth current file awaiting/recording publication work |
| Private current 2/2 | `data/processed/dex_execution_cost_snapshot.csv` | Candidate execution current file awaiting/recording publication work |
| Price dependency | `data/processed/dex_pool_tvl_snapshot.csv` | Temporary current-run GeckoTerminal USD-price input; not a published TVL fact |
| Raw evidence | `data/raw/dex-depth/<snapshot_id>/*.json` | Per-pool RPC transcripts and errors |
| Raw manifest | `data/raw/dex-depth/<snapshot_id>/manifest.json` | Fixed blocks, status counts, bands, and raw-file inventory |
| Raw exact receipt | `data/raw/dex-depth/<snapshot_id>/uniswap_v3_exact_validation.json` | Private canonical result written after raw validation and before processed/public writes |

The public sidecar is not its own trust anchor. Dashboard health first
canonicalizes and validates it against the two public CSVs, then safely
resolves the retained raw receipt from its validated `depth_snapshot_id`.
Snapshot IDs must remain canonical, the evidence root/snapshot must be real
directories, and the receipt must be a regular non-symlink file whose bytes
exactly match the public sidecar. Only then may health report `current` or
`stale`; it exposes equal `receipt_sha256` and `trusted_receipt_sha256` values,
never a raw path. An unresolved raw root reports `missing`; an expected raw
artifact that is absent, nonregular, symlinked, tampered, or byte-mismatched
reports `invalid`. Release requires `current` and equal valid receipt hashes.

Production normally resolves the raw root as
`$MARKET_DATA_DIR/raw/dex-depth`. Staging or any split data layout sets
`MARKET_UNISWAP_V3_EXACT_RAW_ROOT` explicitly. The retained receipt remains
private and outside the atomic five-file public bundle; there is no sixth
public file or allow-missing bootstrap mode.

The two-pool canary is intentionally non-publishing:

```bash
python3 scripts/run_uniswap_v3_canary.py --evidence-root /absolute/new/path
```

It creates a new evidence directory, refreshes the GeckoTerminal price input,
collects both pools, and returns `"published": false`. It never replaces a
`data/local` current pointer or production bundle. A passing run writes the
shared raw validation receipt under the depth snapshot and embeds that
identical, path-free receipt in `canary_result.json`. The receipt records the
one shared block number/hash, both pool transcript hashes, retained USD-source
and manifest hashes, authority hash, scoped row hashes, and the exact
two-direction by five-notional scenario inventory.

The public and private depth-current files intentionally share the basename
`dex_depth_snapshot.csv`; their `data/local` versus `data/processed`
directories define different destinations and reader roles.

Publishing requires exactly one explicit status row per TVL-inventory
Token/chain/pool key and at least one measured pool. Latest replacement is
performed only after candidate validation; history remains keyed by snapshot
and pool.

Before the managed commit phase starts, both depth and execution-cost coverage
are preflighted. Supported rows must retain at least 80% current usable
coverage and 95% of comparable prior usable identities. Structural
`unsupported` rows do not enter the supported denominator. The adapter
classifier is recomputed for this gate, so a V2/V3-capable pool mislabeled
`unsupported` cannot hide an RPC or collector failure. Each destination file
participates in the full family bundle described below. The shared operational
details are also recorded in `collection-operations.md`.

## Cohort identity, skew, and failure boundary

One accepted fixed-block pool state produces one depth row and the ten matching
execution rows (two directions at five notionals) for that exact Market. A full
DEX family candidate has exactly one nonempty `snapshot_id`; the execution
`snapshot_id` and `source_snapshot_id` must equal the depth `snapshot_id`, and
the execution Market count must equal the exact published depth inventory row
count. The ID binds a publication/source cohort to its inventory. It is not a
block timestamp, observation timestamp, or proof that pools on independent
chains were observed simultaneously.

Within one chain, every call for a pool uses the declared fixed block. Across
pools and chains, the raw inputs are bounded sequential observations. Metadata
therefore exposes canonical `observed_at_min`, `observed_at_max`, and
`observation_span_seconds`. The span measures earliest-to-latest cohort skew;
it does not redefine the snapshot ID or create cross-chain simultaneity.

Both full and exact publication first resolve the two private and four
ordinary public destinations above and reject any private/public overlap
before making any write. A full publication containing the exact two-pool V3
scope also validates the sidecar receipt and resolves it as the fifth public
destination. Full publication then validates aligned depth/execution lineage, the
exact execution scenario inventory (including DEX USD-price timing), and both
standard coverage reports. Exact publication instead checks aligned lineage
and complete execution scenarios with DEX USD-price timing, validates both
candidate-bound exact-target coverage reports and their target/mode/common
generation, and requires exactly one target history row identical to the
target depth-latest row. Only after those guards pass are the two private
current files written independently and all applicable public destinations
passed to one family bundle.

An ordinary in-process I/O exception rolls all public destinations back to
their pre-call bytes. This is failure-atomic for ordinary I/O failures only,
not process-crash atomic. The resolved-path overlap check also does not prevent
a concurrent path or symlink change after the check (a TOCTOU race). Power
loss, interpreter termination, an operating-system crash, or an unsupported
concurrent direct publisher can still require manifest/hash diagnosis. The two
private current files are outside the public bundle and its rollback.

Real absence and capability limits retain their existing meanings:
`unsupported`, `failed`, or otherwise unavailable facts keep their measured
values blank/JSON `null`. Cohort validation never substitutes zero.

## RPC endpoints

RPC URLs can be overridden with `DEX_DEPTH_RPC_<CHAIN>`. Defaults are:

| Chain | Default |
| --- | --- |
| Ethereum | `https://ethereum-rpc.publicnode.com` |
| Arbitrum | `https://arb1.arbitrum.io/rpc` |
| Optimism | `https://mainnet.optimism.io` |
| Base | `https://base-rpc.publicnode.com` |
| BNB Smart Chain | `https://bsc-dataseed.bnbchain.org` |
| ZKsync Era | `https://mainnet.era.zksync.io` |

Official chain documentation confirms the public endpoints for
[Optimism](https://docs.optimism.io/op-mainnet/network-information/connecting-to-op),
[BNB Smart Chain](https://docs.bnbchain.org/bnb-smart-chain/developers/json_rpc/json-rpc-endpoint/),
and [ZKsync Era](https://docs.zksync.io/zksync-network/zksync-era/network-details).
Base explicitly labels `mainnet.base.org` rate-limited and unsuitable for
production, so the default uses PublicNode while retaining an environment
override:
[Base connection documentation](https://docs.base.org/base-chain/quickstart/connecting-to-base).

Optional ordered fallbacks use strict JSON-array variables such as
`DEX_DEPTH_RPC_ETH_FALLBACKS` and `DEX_DEPTH_RPC_BSC_FALLBACKS`. The scalar
primary remains backward compatible; fallbacks are used sequentially, never in
parallel. The collector rejects malformed JSON, empty entries, duplicate URLs,
and endpoint pools larger than one primary plus two fallbacks.

Eligible provider failures, including HTTP 401/403/404, 429/5xx, connection
errors, DNS failures, and timeouts, can move the run to the next endpoint.
JSON-RPC contract reverts, malformed protocol facts, wrong-chain responses, and
fixed-block identity mismatches remain data failures rather than hidden
provider hops. Before a fallback can serve fixed-block state, it must prove the
expected chain ID and the exact `F` number, hash, and timestamp. If all
endpoints are exhausted, the affected facts fail with bounded reason codes such
as `rpc_endpoint_exhausted`; existing publication gates keep the previous
complete public generation.

Retained evidence binds a bounded RPC-attempt ledger into the transcript hash.
That ledger keeps stable endpoint IDs, sanitized endpoint identities, method
stage, bounded outcome category, status code when safe, retry/failover decision,
and timing. It does not retain URL credentials, query strings, headers, raw
exception text, or unbounded response bodies. A free-only endpoint pair improves
best-effort availability for that observation; it is not an uptime guarantee or
a substitute for a paid RPC SLA.

## Separate route-cost evidence

Depth and fixed-notional pool execution remain pool-state facts and continue to
exclude non-pool route costs. `scripts/dex_route_costs.py` provides a separate
route-cost integrity gate for a later synchronized opportunity pipeline; it
does not add costs to the existing depth or execution CSVs by inference.

### Current trust boundary: integrity is not authentication

Frozen dataclasses, exact field checks, and canonical SHA-256 records can show
that one in-process value was not silently changed after construction. They do
not prove who supplied the value or whether it came from a chain, a cohort
bundle, an adapter execution, or a relay. Every evidence builder currently
exposed by `scripts/dex_route_costs.py` is caller-buildable. Those types are
therefore integrity records, not authenticated evidence, and they cannot create
a strict route-cost fact.

The current gas path still validates and redacts one concrete fixed-block RPC
calculation. Its integrity checks include:

- a canonical Market ID shaped as
  `dex:<chain>:<dex>:<pool-address>:<UPPERCASE-TOKEN>`, where `pool-address`
  is a 20-byte EVM address encoded as `0x` followed by exactly 40 lowercase
  hexadecimal digits;
- the registered chain, adapter, and router identity;
- exact `from`, `to`, calldata, and value committed by canonical JSON SHA-256;
  SwapRouter02 `exactInputSingle` decoding checks Token addresses, fee,
  recipient, direction, target raw quantity, block tag, and context fields;
- controlled opaque-sender and allowance policy identifiers;
- a fixed block number, hash, timestamp, and canonical `baseFeePerGas`;
- an `eth_feeHistory` request for exactly one block and the 50th-percentile
  reward. The response has an exact shape and canonical hexadecimal values.
  Its first base fee must exactly equal the fixed block's `baseFeePerGas`; a
  mismatch is `failed` before `eth_estimateGas`. The calculation derives
  `max_fee_per_gas_wei = 2 * next_base_fee + median_priority_fee`;
- a typed native/USD integrity record whose fields, time window, and record
  hash match the requested context.

The collector verifies `eth_chainId`, resolves the block, validates the block
base fee against `eth_feeHistory`, validates the native/USD integrity record,
and only then calls `eth_estimateGas`. The arithmetic remains exact:

```text
gas_units * max_fee_per_gas_wei / 10^18 * native_token_usd
```

However, the call-to-Market association, pool metadata, requested notional, and
native/USD value still originate in public builders. A caller can re-sign the
same call under a fake pool, a USD 1 or USD 1 billion notional, or a USD 1
native-Token price. Consequently a successful current calculation is
`value_status=assumed`, `strict_eligible=false`; invalid or incomplete inputs
remain terminal with null amount/rate. It is never `quoted` or strict.

The current registry contains only Ethereum Uniswap V3 using adapter
`uniswap_v3_router/v1` and SwapRouter02 target
`0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45`. The transaction `to` address
must equal that registered target. A different chain, DEX name, adapter ID,
router target, noncanonical Market ID, or unregistered combination is
unavailable. Registry membership limits behavior; it does not authenticate
pool facts.

Every JSON-RPC response used by this evidence gate, including
`eth_feeHistory`, must contain
`jsonrpc: "2.0"` and an integer response ID exactly equal to the integer request
ID (booleans are not integers for this contract). Chain ID, block number, and
gas estimate, block timestamp, fee-history base fees, and reward must be
canonical Ethereum quantities: a lowercase `0x` string,
`0x0` for zero, otherwise no leading zero and only lowercase hexadecimal
digits. Native JSON integers, decimal text, uppercase digits, and nonminimal
forms such as `0x01` are rejected and revalidated before entering calculation
lineage.

The output is a redacted gas envelope containing a canonical `cost_component`
plus chain, block, call hash, policy, gas-unit, fee-cap, price, adapter, and
integrity-hash fields. Missing or inconsistent inputs and RPC failure produce
terminal components with null amount/rate; they never become zero. Persisted
`eth_estimateGas`
RPC records replace the call with its SHA-256, redact error text, and reduce the
published endpoint to scheme, host, and optional port only for exact
allowlisted public hosts. Any other syntactically valid host becomes
`rpc-endpoint-sha256:<digest>`; malformed endpoints become the fixed
`rpc-endpoint:redacted` label. Thus a wallet, API key, credential-bearing
hostname, RPC credential, calldata, or private path cannot escape through
lineage. A
stored response ID comes from the already-validated request ID, never from an
untrusted response. Error records contain only a fixed status and, when the
provider supplied a safe integer code, that code; nested provider messages and
data are neither returned nor persisted.

Caller-buildable `router_or_integrator_fee` and `token_transfer_tax` numeric
records are likewise only `assumed` and non-strict, even when every context and
hash field matches. A caller declaration of `not_applicable` is `unavailable`,
because absence requires authenticated adapter behavior, not a signed absence
claim. Replayed, stale, malformed, or unknown records remain terminal.

`mev_buffer` is scenario-only. Every positive public scenario, including one
accompanied by a caller-buildable `max_loss_bps`, route/adapter/submission/policy
record, is `assumed`. It cannot become `bounded_estimate`. The code intentionally
has no public override that can impersonate future verified relay evidence.
Missing or zero scenarios remain unavailable as applicable. Funding rates
remain outside this contract.

### Dependencies before any future strict upgrade

No current public type satisfies the authentication boundary. Strict or bounded
statuses must remain closed until all relevant upstream integrations exist:

1. Task 5's typed `QuantityQuote`, binding direction, requested notional, target
   quantity, calldata amounts, and quote lineage as one verified object;
2. fixed-block `PoolState` read through a verified factory/pool relationship,
   binding the actual pool address, Token contracts, fee, and block;
3. an actual synchronized cohort-bundle reader that verifies the immutable
   bundle and record membership instead of accepting caller-supplied hashes;
4. a controlled adapter execution result using observed balance deltas or a
   protocol-specific deterministic rule for router fees and transfer taxes;
5. authenticated relay/provider policy evidence for route-specific maximum
   loss, submission mode, policy identity, and validity.

Only connector-owned private verification paths may eventually elevate those
facts. Adding another frozen class, public builder, hash, or boolean override is
not sufficient.

## Non-claims

The measurement does not include gas, router fees, token transfer taxes,
MEV/sandwiching, transaction reordering, or state changes after the fixed
block. It is an auditable pool-state capacity measurement, not a guaranteed
future trade quote.
