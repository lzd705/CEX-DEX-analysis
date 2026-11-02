# CEX–DEX Market Structure & Execution Research

English | [简体中文](README.zh-CN.md)

**[Open the market dashboard](http://43.156.102.166:8765)** · [Implementation map](#implementation-map) · [Reproduce the research](#reproduce-the-research)

**Source-backed market data, protocol-exact AMM pricing, and execution-cost-aware cross-venue research.**

## Overview

The project studies how cross-market price differences change with trade size,
liquidity distribution, fees, token precision, and market-state timing. It combines
a CEX/DEX comparison platform with order-book execution models, fixed-block pool
state, integer swap simulation, and reproducible cash-flow experiments.

| Component | Scale | Observation basis |
| --- | --- | --- |
| Market-data platform | 30 tokens, 12 CEXs, 493 market series | July 29, 2026 release: 345 CEX series and 148 DEX series across 147 physical pools |
| Historical daily observations | 94,778 rows | July release: 66,015 CEX and 28,763 DEX daily observations |
| Liquidity bands | 10 / 25 / 50 / 100 bp | Directional order-book and supported pool-state depth |
| Trade-size grid | $1K / $5K / $10K / $50K / $100K | Quantity-matched buy and sell quotes |
| V3 integer engine | Two Ethereum UNI pools | UNI/USDT and UNI/WETH, exact input/output and same-block QuoterV2 verification |
| SHIB V2 pool study | 10 directional size scenarios | Fixed-block Uniswap V2 and ShibaSwap V1 comparison |
| November UNI CEX/V3 experiment | 51 distinct blocks, 510 DEX quotes | 21,420 fee/Gas cash-flow scenarios with published inputs and offline replay |

The [July release report](docs/senior-report.md) and
[November experiment](experiments/uni_cex_v3_20261102/README.md) provide the dated
data and calculation context for these figures.

## Architecture

```text
CEX spot APIs             GeckoTerminal             EVM JSON-RPC
      └───────────────────── collectors ──────────────────┘
                                  │
                  Market identity + unit normalization
                                  │
                Schema / coverage / timestamp validation
                                  │
          ┌───────────────────────┼─────────────────────────┐
     Daily OHLCV          TVL / depth / quotes       Research cohorts
       SQLite             Latest snapshots          Fixed-state inputs
          └───────────────────────┼─────────────────────────┘
                         Python API + dashboard

Research: CEX level walks · V2 integer reserves · V3 integer SwapMath
          Quoter parity · same-quantity cash flows · fee/Gas/delay analysis
```

**Stack:** Python, SQLite, Decimal/integer arithmetic, JSON-RPC, JavaScript,
HTML/CSS, Docker, Nginx, systemd, and a dedicated Foundry/Anvil replay toolchain.

## Data quality

- **Identity:** preserve venue/pair for CEX instruments and chain/protocol/pool/token addresses for DEX markets.
- **Units:** retain base/quote assets, token decimals, raw integer quantities, and explicit quote-to-USD conversion methods.
- **Completeness:** validate market/scenario inventories and book boundaries; record truncated depth as an observed lower bound.
- **Status:** distinguish `observed`, `partial`, `unsupported`, and `failed`; missing numeric values remain `null`.
- **Time:** retain source event time where supplied, request/receipt time, collection span, block time, and conversion observation time.
- **Evidence:** preserve raw responses, manifests, source hashes, and calculation lineage. Publication gates evaluate usable coverage and retention of prior usable markets.

Daily OHLCV, point-in-time liquidity, and route cohorts each retain their own
storage and publication lifecycle. See [data management](data/README.md) and
[collection operations](docs/collection-operations.md).

## Quantitative methodology

### CEX: level-by-level execution

For a target token quantity `q`, buy quotes consume asks in ascending price order;
sell quotes consume bids in descending order. The last level is partially consumed
when needed. Total quote cash divided by filled quantity gives VWAP.

```math
C_{buy}(q)=\sum_i p_i\Delta q_i,\qquad
R_{sell}(q)=\sum_j b_j\Delta q_j
```

The same book's midpoint supplies the reference price. Execution shortfall is
`buy cash − q × reference price` for buys and `q × reference price − sell cash`
for sells. Account-fee scenarios form a separate cost layer.

### V2: fee-adjusted integer reserve math

For input reserve `x`, output reserve `y`, gross input `a`, and fee fraction `f`,
exact-input output follows:

```math
amountOut=\left\lfloor\frac{y(1-f)a}{x+(1-f)a}\right\rfloor
```

Exact-output quotes invert the reserve equation and apply the protocol's integer
division/`+1` rule. Calculations use raw token units, pool-specific fees, and the
captured reserve state. Pool depth follows the curve to a target price boundary.

### V3: concentrated liquidity and exact-integer swaps

The [V3 module](research/v3_exact/README.md) reads `slot0`, active `liquidity`,
`fee`, `tickSpacing`, `tickBitmap`, initialized ticks, and `liquidityNet`.
`sqrtPriceX96` represents the square root of the raw token1/token0 price in Q64.96.
Human-unit price additionally applies the token-decimal adjustment.

Within one active-liquidity segment, selling token0 moves the square-root price
from `s0` down to `s1`. With `s = sqrtPriceX96 / 2^96`, the segment relations are:

```math
amount0_{effective}=L(1/s_1-1/s_0),\qquad
amount1_{out}=L(s_0-s_1)
```

The engine evaluates these relations with integer fixed-point operations and
direction-dependent rounding:

1. `compute_swap_step` resolves input/output, fee, and the next price within a segment.
2. `simulate_swap` locates the next initialized tick or price limit.
3. At a crossed tick, active liquidity changes by `liquidityNet`, with the sign reversed for `zeroForOne`.
4. The loop continues until the requested amount is resolved or the bounded price limit is reached.

Sell scenarios use exact input; buy scenarios use exact output for the same target
token quantity. For the two registered UNI pools, this engine supports both depth
boundaries and fixed-quantity execution. Additional V3-style depth adapters retain
their protocol-specific calculation paths.

### Same-block QuoterV2 verification

The two-pool canary binds state to one finalized block and validates pool,
factory, token order, decimals, and fee. Local results are compared to QuoterV2
using raw integer amounts, `sqrtPriceX96After`, and Quoter-compatible
`initializedTicksCrossed`. The receipt records the shared block and source lineage.

The November experiment uses latest-block quotes bound by block hash, followed by
a canonical-hash recheck. Its captured block age and CEX observation age are
published with each sample.

### Cross-venue cash flow

The route model starts with a common **net token quantity**. CEX cash flows apply
the selected fee asset, quantity gross-up, increments, and fee rounding. DEX swap
quotes supply fee-inclusive pool cash flows; the route layer adds remaining costs.

```math
\Pi_{CEX\to DEX}=DEX_{sell}(q)-CEX_{buy}(q)-G
```

```math
\Pi_{DEX\to CEX}=CEX_{sell}(q)-DEX_{buy}(q)-G
```

`G` is the selected Gas scenario. Fee, inventory, and timing assumptions travel
with the result. See the [execution-cost contract](docs/execution-cost-data-contract.md).

## Published research results

### UNI: Binance ↔ Uniswap V3, November 2, 2026

The approximately ten-minute capture contains 60 polls, 51 distinct accepted
blocks, and 510 QuoterV2 quotes. The grid combines two directions, five sizes,
six CEX fee assumptions, and seven Gas assumptions.

| Measurement | Result |
| --- | --- |
| Pool | Ethereum UNI/USDT, 30 bp swap fee |
| Fee/Gas cash-flow rows | 21,420 |
| Positive rows in the sampled grid | 0 |
| Zero CEX fee / zero Gas diagnostic | Negative in both directions at every size |
| Best $1K anchor, CEX fee=10 bp, Gas=1 USDT | Binance buy → V3 sell: −2.578299 USDT |
| Delay experiment | Exact +1/+2/+3 block repricing, including conditional CEX-first cash flow |

[Capture and reproduction guide](experiments/uni_cex_v3_20261102/README.md) ·
[Raw sample index](experiments/uni_cex_v3_20261102/capture/manifest.json) ·
[Results](experiments/uni_cex_v3_20261102/results/summary.json) ·
[Detailed report](docs/uni-cex-v3-experiment-2026-11-02.md)

Offline replay verifies original responses, quantity/state bindings, fee evidence,
and cash flows, then reproduces all four reference analysis files byte-for-byte.

### SHIB: Uniswap V2 ↔ ShibaSwap V1

Two RPC providers cross-check 35 logical reads at one finalized block, yielding
70 provider observations. The study evaluates the same raw SHIB quantity across
two directions and five sizes. All ten retained scenarios have non-positive
pool-fee-adjusted edges. The [public snapshot](research/shib_v2v2/data/public/research/shib-v2v2/latest.json)
and [calculation](research/shib_v2v2/scripts/shib_v2_research.py) are included.

### Foundry: historical two-venue execution harness

The [UNI/WETH harness](foundry/src/TwoVenueV2Executor.sol) follows WETH → UNI →
WETH through Uniswap and SushiSwap. It checks router identity, sender, inventory,
and residual balances. Current swap parameters use `amountOutMin=0` and a
`block.timestamp + 60` deadline. [Unit/fork tests and toolchain links](foundry/README.md)
describe the replay workflow.

## Implementation map

| Component | Default-branch source |
| --- | --- |
| Market-data application | [dashboard/](dashboard/), [collection scripts](scripts/run_collection_cycle.py) |
| V3 integer engine | [uniswap_v3_math.py](research/v3_exact/scripts/uniswap_v3_math.py) |
| V3 state, depth, Quoter parity | [fetch_dex_depth.py](research/v3_exact/scripts/fetch_dex_depth.py) |
| V3 two-pool registry | [uniswap_v3_execution_markets.json](research/v3_exact/config/uniswap_v3_execution_markets.json) |
| SHIB V2 study | [shib_v2_research.py](research/shib_v2v2/scripts/shib_v2_research.py) |
| Foundry replay | [TwoVenueV2Executor.sol](foundry/src/TwoVenueV2Executor.sol) |
| UNI collector / verifier | [collector](scripts/uni_cex_v3_experiment.py), [analyzer](scripts/analyze_uni_cex_v3_experiment.py), [offline replay](scripts/replay_uni_cex_v3.py) |
| Source provenance | [Pinned commits and file hashes](research/sources.json) |

The root application and Foundry source originate at `83e66079`; isolated V3 and
SHIB modules preserve `401eaad1` and `b16ee24d`. [Research navigation](docs/research-index.md)
connects entry points, tests, evidence, and source versions.

## Reproduce the research

Clone the default branch and run the offline workflows with Python 3.10+:

```bash
git clone https://github.com/lzd705/CEX-DEX-analysis.git
cd CEX-DEX-analysis
python3 scripts/run_research.py list
python3 scripts/run_research.py v3-math
python3 scripts/run_research.py shib-replay
python3 scripts/run_research.py uni-v3-replay
```

The V3 command runs 23 math tests. SHIB replay checks byte identity with its
retained snapshot. UNI replay prints observation counts, result hashes, and a
new local output path. All three workflows use bundled evidence and standard-library Python.

Check source provenance and bilingual navigation:

```bash
python3 scripts/verify_research_sources.py
python3 scripts/check_research_docs.py
```

For application data collection and dashboard setup, see [dashboard operations](dashboard/README.md).
Foundry replay uses its [pinned toolchain](config/historical_foundry_replay_toolchain.json).

## Next research tasks

1. Add event-time and receipt-time models with measured quote-to-submission latency distributions.
2. Maintain sequenced WebSocket books and extend continuous observation windows.
3. Integrate account fee tiers, settlement precision, inventory constraints, partial fills, and hedge recovery.
4. Calibrate transaction Gas and ordering against fork execution and receipts.
5. Extend route and protocol coverage using the same identity, evidence, and reproducibility contracts.
