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

This prevents different calls for one snapshot from silently mixing states from
different blocks.

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
[Uniswap V3 pool source](https://github.com/Uniswap/v3-core/blob/main/contracts/UniswapV3Pool.sol).

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
| Public bundle 1/4 | `data/local/dex_depth_history.csv` | Normalized depth history keyed by snapshot and pool |
| Public bundle 2/4 | `data/local/dex_depth_latest.csv` | Latest published complete depth inventory |
| Public bundle 3/4 | `data/local/dex_depth_snapshot.csv` | Public current depth view |
| Public bundle 4/4 | `data/local/dex_execution_cost_latest.csv` | Latest execution scenarios derived from the same source cohort |
| Private current 1/2 | `data/processed/dex_depth_snapshot.csv` | Candidate depth current file awaiting/recording publication work |
| Private current 2/2 | `data/processed/dex_execution_cost_snapshot.csv` | Candidate execution current file awaiting/recording publication work |
| Price dependency | `data/processed/dex_pool_tvl_snapshot.csv` | Temporary current-run GeckoTerminal USD-price input; not a published TVL fact |
| Raw evidence | `data/raw/dex-depth/<snapshot_id>/*.json` | Per-pool RPC transcripts and errors |
| Raw manifest | `data/raw/dex-depth/<snapshot_id>/manifest.json` | Fixed blocks, status counts, bands, and raw-file inventory |

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

Both full and exact publication first resolve the two private and four public
destinations above and reject any private/public overlap before making any
write. Full publication then validates aligned depth/execution lineage, the
exact execution scenario inventory (including DEX USD-price timing), and both
standard coverage reports. Exact publication instead checks aligned lineage
and complete execution scenarios with DEX USD-price timing, validates both
candidate-bound exact-target coverage reports and their target/mode/common
generation, and requires exactly one target history row identical to the
target depth-latest row. Only after those guards pass are the two private
current files written independently and the four public destinations passed
to one family bundle.

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

## Non-claims

The measurement does not include gas, router fees, token transfer taxes,
MEV/sandwiching, transaction reordering, or state changes after the fixed
block. It is an auditable pool-state capacity measurement, not a guaranteed
future trade quote.
