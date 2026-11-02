# CEX–DEX Market Research

English | [简体中文](README.zh-CN.md)

**Website:** [Open the market dashboard](http://43.156.102.166:8765)

A cross-market research platform for comparing prices, measuring liquidity, estimating execution costs, and evaluating arbitrage across centralized exchanges and on-chain liquidity pools.

The project connects a market-data dashboard with quantity-aware quote engines and reproducible opportunity workflows. Its central question is: **how does an observed price difference change after trade size, market depth, fees, and timing are taken into account?**

[Explore the application](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/historical-foundry-opportunity) · [Data contracts](#documentation) · [Run a demo](#quick-start) · [Research workstreams](#repository-map)

## What the platform does

| Area | Capabilities |
| --- | --- |
| Market comparison | Select a token and two exact markets; compare daily prices, returns, volatility, volume, and price spreads across CEX/CEX, CEX/DEX, or DEX/DEX pairs. |
| Liquidity analysis | Measure directional depth at 10, 25, 50, and 100 bp from CEX order books and supported DEX pool states. Display pool TVL alongside depth. |
| Execution-cost research | Quote buys and sells at five trade sizes by walking order books or simulating pool swaps; retain quantities, VWAP, fee scope, and source state. |
| Opportunity evaluation | Match buy and sell legs on a common token quantity, attach cost evidence, apply timing checks, and publish route-level research results. |
| Historical replay | Preserve block identities and collection evidence; rebuild fixed-state scenarios through dedicated replay and validation workflows. |
| Data operations | Run incremental collection, quality audits, snapshot publication, health checks, and authenticated token-review workflows. |

The application workstream has a **30-token configured universe** and **12 public CEX order-book adapters**. Market availability and usable coverage are reported for each collected snapshot.

## Architecture

```text
Public CEX APIs                    DEX APIs + EVM JSON-RPC
      │                                      │
      └─────────────── Collectors ────────────┘
                           │
                Raw responses + manifests
                           │
             Identity, schema, and timing checks
                           │
          ┌────────────────┼──────────────────┐
          │                │                  │
     Daily facts      Depth + quotes     Route evaluation
          │                │                  │
       SQLite       Latest snapshots    Immutable bundles
          └────────────────┼──────────────────┘
                           │
                  Python API + dashboard
```

**Stack:** Python, SQLite, Decimal/integer arithmetic, JSON-RPC, JavaScript, HTML/CSS, Docker, Nginx, and systemd. The historical replay workstream also integrates Foundry/Anvil.

Daily time series, point-in-time liquidity, and route cohorts have separate storage and publication lifecycles. The dashboard combines them through explicit market identities and source lineage.

## Data quality

Quality checks are part of collection, calculation, publication, and API delivery.

### Market identity and units

- CEX observations retain the exchange, spot instrument, base asset, and quote asset.
- DEX observations retain chain, protocol, pool address, token addresses, token order, and decimals.
- Quote-to-USD conversions carry their source and method. USDT-based workflows identify the USD-parity proxy explicitly.

### Validity and completeness

- Validate schemas, positive book levels, bid/ask ordering, exact scenario inventories, and source hashes.
- Preserve missing numeric values as `null`; distinguish measured zero, partial coverage, unsupported models, and collection failures.
- Detect truncated order books. A depth band receives a completeness flag; an incomplete measured band is displayed as an observed lower bound.
- Calculate coverage against an explicit market inventory. Publication checks evaluate current usable coverage and retention of previously usable markets.

### Time and reproducibility

- Retain source timestamps, observation times, collection spans, and block identities where applicable.
- Read a DEX pool's required state at a fixed block. Dedicated exact/replay workstreams add block-hash binding and canonicality checks.
- Preserve original responses, manifests, normalized facts, and SHA-256 lineage for inspection and replay.
- Keep gaps visible in daily charts and calculate daily returns only across adjacent valid dates.

See the [collection runbook](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/collection-operations.md) and [data lifecycle](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/data/README.md).

## Depth calculation

Depth measures the quote notional available within a specified distance from a reference price. Buy and sell directions are retained separately.

### CEX order books

For best bid `b`, best ask `a`, and band `k` in basis points:

```text
midpoint = (b + a) / 2
bid depth = Σ(price × quantity × quote_to_usd), price ≥ midpoint × (1 − k / 10,000)
ask depth = Σ(price × quantity × quote_to_usd), price ≤ midpoint × (1 + k / 10,000)
```

The collector checks whether the returned levels reach the relevant boundaries. This distinguishes complete band measurements from depth limited by an API's level count.

### DEX pool states

- **V2-style pools:** read reserves, token metadata, and the applicable pool fee; use the constant-product model to calculate movement to each price boundary.
- **V3-style pools:** read `slot0`, active liquidity, fee, tick spacing, tick bitmaps, and initialized ticks. Integrate amounts through successive liquidity segments, updating active liquidity at crossed ticks.
- **V3 exact-swap workstream:** a dedicated integer engine implements exact-input and exact-output swaps, protocol-style rounding, and bounded tick traversal for UNI/USDT and UNI/WETH, with QuoterV2 comparison tooling.

TVL describes total pool assets; depth describes liquidity along a particular price movement. In concentrated-liquidity pools, their relationship depends on the distribution of liquidity across ticks.

## Execution cost

The standard quote inventory uses **$1K, $5K, $10K, $50K, and $100K**, with a buy and sell scenario at each size.

For a reference notional `N` and reference price `P`:

```text
target quantity q = N / P, adjusted to the applicable quantity precision
reference notional = q × P

buy shortfall  = quote paid − reference notional
sell shortfall = reference notional − quote received
quoted cost bp = shortfall / reference notional × 10,000
```

CEX quotes consume the original price levels in order. V2 execution uses integer swap arithmetic. The V3 exact workstream traverses initialized ticks. Each method calculates the requested quantity directly.

### Cost layers

| Layer | Treatment |
| --- | --- |
| Spread and price impact | Reflected in order-book walks or pool swap quotes. |
| Pool swap fee | Included in the pool's swap calculation. |
| CEX trading fee | Added by the opportunity cost layer with an explicit fee source, rate, and evidence lifetime. |
| Gas, router fees, transfer tax, and rebalancing | Represented as separate route components with values, applicability, and evidence status. |
| Timing | Recorded through quote ages, source spans, route deadlines, and cohort freshness. |

The ordinary market quote view measures visible-book shortfall for CEX markets and fee-inclusive swap shortfall for supported V2 pools. The dedicated opportunity layer supplies route-specific costs and assumptions; V3 exact execution lives in its own workstream.

## How an opportunity is evaluated

1. **Select exact markets.** Define the token, buy venue, sell venue, quote assets, direction, and requested size.
2. **Collect a source cohort.** Save books or pool state, instrument rules, timestamps, and raw evidence for both legs.
3. **Choose a common net token quantity.** Apply quantity increments and fee-asset rules so both legs refer to the same inventory change.
4. **Quote each leg.** Calculate total buy expenditure and sell proceeds against the captured liquidity.
5. **Attach cost evidence.** Record each fee or route component, including whether it is already embedded in a quote.
6. **Evaluate the route.** Calculate the scenario net edge and check timing, completeness, and required evidence.
7. **Publish and inspect.** Store the complete route bundle, expose results through the API, and retain reasons for unavailable scenarios.

```text
scenario net edge = sell proceeds after sell fees
                  − buy expenditure including buy fees
                  − additional applicable route costs
```

Every cost is counted once. Current public CEX workflows use a stated pre-positioned-inventory scenario and return `research_estimate` or `unavailable` classifications with supporting cost records.

### Reference workflows

| Workflow | Markets and scenario inventory | Implementation |
| --- | --- | --- |
| Public CEX opportunity | Binance and Bybit; UNI/USDT and CAKE/USDT; four directed routes × five sizes | Live collection, quantity matching, public-fee evidence, complete-bundle publication, API, and dashboard. |
| V3 exact execution | Ethereum UNI/USDT and UNI/WETH; two pools × two directions × five sizes | Shared integer swap engine, fixed-state collection, QuoterV2 checks, and two-pool canary tooling. |
| SHIB pool comparison | Ethereum Uniswap V2 and ShibaSwap V1 SHIB/WETH; two routes × five sizes | Finalized-block capture, 35 logical reads checked through two providers, and deterministic pool-fee-adjusted edge calculation. |
| Historical opportunity | Fixed-state route scenarios | Foundry/RPC replay, scanning, evidence validation, and historical presentation tooling. |

### Recorded CEX observation

The [2026-09-05 public collection](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/live-cex-multi-token-acceptance-2026-09-05.md) captured all four configured CEX markets and published 20 scenario rows. At the $1,000 size, the recorded UNI research net edges were **−$3.5735828** for Bybit → Binance and **−$4.35663** for Binance → Bybit. CAKE rows recorded the missing exact fee evidence. The acceptance record includes collection times, source hashes, fee treatment, and freshness checks.

## Quick start

### 1. Get the application and research demos

The default `main` branch provides this project overview. The following branch contains the dashboard, market collectors, live CEX workflow, and historical demo:

```bash
git clone --branch codex/historical-foundry-opportunity \
  https://github.com/lzd705/CEX-DEX-analysis.git
cd CEX-DEX-analysis
```

The demo runners use Python's standard library. Use Python 3.8 or later for the application and demos; the Foundry replay toolchain has its own pinned runtime configuration.

### 2. Open an offline workflow demo

```bash
python3 scripts/run_historical_opportunity_demo.py --port 0
```

Open the printed URL. This demo builds ten deterministic synthetic scenarios and exercises validation, API delivery, filtering, and historical result presentation.

For the sealed Current Opportunity workflow:

```bash
python3 scripts/run_current_opportunity_demo.py --port 0
```

This demo uses two synthetic AAA/WETH V2 pools and five trade sizes. Open the complete printed URL to activate the fixture view. Both runners manage their temporary local data and stop with `Ctrl-C`.

### 3. Collect current public CEX quotes

Review the official fee sources and exact pair rows in [the public fee schedule](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/config/cex_public_fee_schedules.csv), including their check and expiry times. Then select an absolute output directory:

```bash
python3 scripts/run_live_cex_opportunity.py \
  --data-dir /absolute/path/to/market-data \
  --public-fee-schedule config/cex_public_fee_schedules.csv \
  --serve --port 8765
```

The command collects public books and instrument rules, publishes a new bundle, verifies a cold reload, and prints the dashboard URL. Each cohort has a 120-second quote lifetime. A new collection generates a fresh cohort.

### 4. Run the market monitor with collected data

```bash
python3 scripts/run_collection_cycle.py --profile full --publish-local
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

Open `http://127.0.0.1:8765`. The `daily` and `depth` collection profiles support separate update schedules. Existing reviewed CSV inputs can be loaded through `scripts/import_local_snapshot.py`.

## Repository map

| Branch | Role | Entry point |
| --- | --- | --- |
| [`main`](https://github.com/lzd705/CEX-DEX-analysis/tree/main) | Project overview and navigation | This README |
| [`codex/historical-foundry-opportunity`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/historical-foundry-opportunity) | Application, current CEX research, historical replay, and demos | Recommended quick-start branch |
| [`codex/executable-opportunity-pipeline`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/executable-opportunity-pipeline) | Route cohorts, quantity matching, cost components, and publication | [Route contract](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/executable-opportunity-pipeline/docs/route-cohort-data-contract.md) |
| [`codex/v3-exact-swap-mvp`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/v3-exact-swap-mvp) | V3 integer math and two-pool exact execution | [Swap engine](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/v3-exact-swap-mvp/scripts/uniswap_v3_math.py) |
| [`codex/v3-production-resilience`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/v3-production-resilience) | V3 collection and operational follow-up | [Collection operations](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/v3-production-resilience/docs/collection-operations.md) |
| [`codex/shib-v2v2-research-loop`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/shib-v2v2-research-loop) | Fixed-block SHIB pool comparison and replay | [Research contract](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/shib-v2v2-research-loop/docs/shib-v2v2-research-contract.md) |
| [`website-v1`](https://github.com/lzd705/CEX-DEX-analysis/tree/website-v1) | Original website framework | [Dashboard](https://github.com/lzd705/CEX-DEX-analysis/tree/website-v1/dashboard) |

Earlier feature branches preserve the development history of market facts, collection quality, liquidity, and navigation. Browse the [complete branch directory](https://github.com/lzd705/CEX-DEX-analysis/branches) for those iterations.

Application layout in the quick-start branch:

```text
config/                 Token universe, route registries, fee evidence, replay policies
dashboard/              Python API, SQLite queries, and browser interface
scripts/                Collection, math, quality checks, publication, and demo runners
data/schema/            Versioned database schemas
data/curated/           Reviewed events and market-lifecycle evidence
docs/                   Data contracts, design notes, acceptance records, and runbooks
tests/                  Calculation, collection, publication, API, and frontend tests
deploy/                 Service and reverse-proxy templates
foundry/                Foundry replay harness
```

Collected responses and generated databases live in Git-ignored runtime directories. The [data lifecycle guide](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/data/README.md) explains their structure and publication sequence.

## Documentation

- [Market facts and comparison](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/market-facts-contract.md)
- [CEX depth](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/cex-depth-data-contract.md)
- [DEX depth](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/dex-depth-data-contract.md)
- [Execution cost and route cost components](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/execution-cost-data-contract.md)
- [Route cohorts and opportunity publication](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/route-cohort-data-contract.md)
- [Collection schedules and quality gates](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/collection-operations.md)
- [Event facts](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/event-facts-contract.md)
- [Administration and token review](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/admin-operations.md)
- [Deployment operations](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/production-hardening.md)

## Verification

From an application checkout:

```bash
python3 -m unittest discover -s tests -v
git diff --check
```

Tests cover book normalization, pool calculations, quantity rules, missing-value handling, lineage, publication behavior, API contracts, and frontend state. Acceptance records document dated live-source observations alongside the corresponding code and evidence.

## Next milestones

- Maintain sequenced WebSocket order books for finer-grained opportunity observation.
- Integrate authenticated account fees and venue-specific settlement precision.
- Extend route research with transaction-level gas, inclusion delay, and MEV scenarios.
- Add inventory constraints, rebalancing economics, and execution-feedback measurements.
- Expand protocol adapters and continuous, multi-window opportunity experiments.
