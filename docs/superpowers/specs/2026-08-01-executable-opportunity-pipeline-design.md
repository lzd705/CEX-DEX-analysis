# Executable Opportunity Pipeline Design

## Decision

The product will keep the existing research facts and add a separate,
auditable route-opportunity layer. A daily close-price gap is not an
executable opportunity. A route may be called executable only when its two
legs belong to one bounded-skew cohort, use one common Token quantity, have
complete quotes, and contain every cost component required by the declared
route mode.

The public product uses two deliberately separate outputs:

1. **Executable candidate** — fail closed unless every strict requirement is
   satisfied.
2. **Research estimate** — may use explicit schedules, buffers, or user
   assumptions, but is labeled estimated and never enters the executable
   ranking.

Funding Rate remains excluded. The system identifies source-backed
opportunities; it does not place orders, custody funds, or promise fills.

## Existing boundary

The current hourly collectors publish CEX and DEX depth/execution families
sequentially. Each family already guarantees that its depth row and ten
fixed-notional execution rows share source lineage, but CEX and DEX are not one
observation cohort. The current A/B API reports snapshot skew without rejecting
an over-skew route.

The current execution contract also cannot be subtracted leg-to-leg:

- the same USD notional can map to a different Token quantity on each market;
- CEX account taker fees are explicitly excluded and numeric fee fields are
  rejected;
- supported DEX V2 quotes include pool fees but exclude gas, router fees,
  transfer mechanics, and MEV;
- CEX-involving routes are normally non-atomic and require a declared
  pre-positioned inventory or rebalancing model.

The new route pipeline therefore consumes existing catalog and liquidity
evidence for candidate selection, but it does not relabel existing snapshots
as synchronized.

## Delivery decomposition

The work is split into five dependent increments on one feature branch:

1. synchronized route cohort and immutable publication;
2. cost-component and route-opportunity contracts;
3. separate Daily Price Gap and Opportunities API/UI;
4. DEX execution-adapter expansion in measured-value order;
5. Event Past/Future clock view.

Each increment has its own failing tests, commit, GitHub commit comment, push
comment, and verification evidence. Production is deployed only after the
complete release gate passes.

## 1. Route universe

### Candidate identity

One route candidate is identified by:

```text
route_id =
  Token + buy_market_id + sell_market_id + route_mode
```

The buy and sell markets must be distinct canonical catalog identities for the
same Token. A stable canonical ordering is used for storage, while direction
remains explicit. Every candidate contains a fixed grid of requested USD
notionals matching the execution contract: 1,000, 5,000, 10,000, 50,000, and
100,000.

### Supported route modes

- `prepositioned_inventory`: CEX-CEX or CEX-DEX analysis where the required
  quote and Token balances are already available on the two venues.
- `atomic_onchain`: same-chain DEX-DEX analysis only when one validated router
  transaction or equivalent atomic call is represented.
- `rebalance_required`: a research route whose transfer/rebalancing leg is
  explicit but not part of the immediate trade. This mode is never strict
  executable unless the rebalancing component is complete and current.

Cross-chain DEX routes and routes that require an unmodeled bridge remain
research-only and use `unsupported_cross_chain_settlement`.

### Bounded selection

The route collector does not request every pairwise combination of all 519
markets. For each Token it selects at most three currently usable CEX legs and
three currently usable DEX legs using a deterministic priority key:

1. current execution adapter capability;
2. observed 100-bps depth or proved executable capacity;
3. selected-window USD volume for CEX, latest 24-hour volume and TVL for DEX;
4. canonical market ID as the final tie-breaker.

Inactive lifecycle identities, missing two-sided books, unsupported execution,
and invalid source timestamps are retained in market quality but excluded from
strict route generation. Candidate selection records the source generation and
all ranking inputs so the route universe is reproducible.

## 2. Synchronized route cohort

### Observation model

Every run declares:

- `route_cohort_id`;
- `target_observed_at` in canonical UTC;
- `collection_started_at` and `collection_deadline_at`;
- `skew_sla_seconds = 60`;
- `route_age_sla_seconds = 120`;
- the exact candidate-universe generation.

Unique route legs are collected in parallel around the target time:

- CEX requests use per-venue concurrency and rate-limit budgets;
- each EVM chain selects one fixed block near the target, and all selected
  pools on that chain use that block;
- each Solana cohort selects one fixed slot and retains slot/block-time
  lineage;
- DEX USD conversion observations retain their own timestamps and hashes.

The collector uses deadline-aware timeouts and retries. A retry cannot extend
past the cohort deadline. Concurrent results are sorted by canonical identity
before validation and hashing.

### Time contract

Pair skew is calculated from the two actual `state_observed_at` values, never
from a publication ID:

```text
skew_seconds = abs(buy_state_epoch - sell_state_epoch)
```

The calculation preserves fractional seconds with Decimal or integer epoch
units. `60.000000` seconds passes; any value greater than 60 seconds fails.
Missing, naive, malformed, or unreasonably future timestamps fail the route.
The public API also compares the newest leg time with its current response
clock. A cohort older than 120 seconds remains auditable but is strict
unavailable with `cohort_stale`; bounded skew alone cannot make old quotes
executable.

### Route isolation

Expected route-level failures do not reject the complete cohort. They publish
the route with no opportunity values and one stable reason:

- `route_deadline_exceeded`;
- `buy_leg_unavailable` or `sell_leg_unavailable`;
- `snapshot_skew_exceeded`;
- `cohort_stale`;
- `invalid_state_timestamp`;
- `execution_adapter_unsupported`;
- `cost_components_incomplete`;
- `route_mode_not_executable`.

Unknown enums, duplicate identities, invalid numerics, hash mismatch, manifest
lineage conflict, or an incomplete scenario grid reject the whole bundle.

## 3. Cost components

### Separate fact contract

Existing `quoted_execution_cost_*` remains a source-mechanics fact and is not
silently redefined. New `execution_cost_component/v1` rows have this grain:

```text
route_cohort_id × route_id × leg/component × notional
```

Required component kinds are:

- `venue_taker_fee`;
- `pool_swap_fee`;
- `network_gas`;
- `router_or_integrator_fee`;
- `token_transfer_tax`;
- `rebalancing_or_transfer`;
- `mev_buffer` for research scenarios only.

Each component retains exact value or bound, USD/bps unit, status, strict
eligibility, source URL/endpoint, source hash where available, observation
time, validity window, calculation basis, and safe reason code.

Allowed value statuses are:

- `measured`;
- `authenticated`;
- `quoted`;
- `bounded_estimate`;
- `assumed`;
- `not_applicable`;
- `unavailable`;
- `unsupported`;
- `failed`;
- `stale`.

Missing is never zero. A component may be `not_applicable` only when the route
contract proves that it does not apply.

### CEX fees

Account-specific fees are not anonymous market facts. Strict CEX opportunity
eligibility requires a current authenticated, read-only fee response or an
operator-provided validated fee profile. Credentials, account identifiers,
and raw authenticated payloads never enter public files, API responses, or
logs.

Official public fee schedules may support a `bounded_estimate`; browser-local
user overrides may support an `assumed` scenario. Neither enters the strict
ranking. Fee asset, discount-token behavior, tax/special commission, and
whether the received Token quantity is net of fees remain explicit.

### DEX costs

- Pool swap fees remain derived from the same fixed-block pool state.
- Gas requires concrete chain, sender assumptions, target contract, calldata,
  value, allowance state, gas estimate, bounded fee-per-gas, and native-asset
  USD lineage. A static pool-level gas constant cannot be strict.
- Router fees are numeric or `not_applicable` only when the selected adapter or
  quote proves the result.
- Transfer-tax behavior is strict only when the complete call simulation or a
  validated token adapter proves it.
- MEV has no universal deterministic fee. It is represented by protection mode
  plus an optional scenario buffer and is never a measured Fact.

## 4. Opportunity calculation

### Common quantity

For each route/notional, the evaluator first derives one exact Token quantity
that both legs can represent after base-unit, lot-size, minimum-notional, and
fee-asset rules. Both legs quote that same net quantity:

- buy leg: total quote paid for the common net Token quantity;
- sell leg: total quote received from that same Token quantity.

If the common quantity cannot be proved, the scenario is unavailable. The
system never subtracts two independently derived same-USD-notional rows.

### Output fields

`route_opportunity/v1` retains:

- gross buy cost and sell proceeds;
- `gross_edge_usd` and `gross_edge_bps`;
- known strict costs by component;
- scenario-only costs by component;
- `net_edge_usd` and `net_edge_bps`;
- common Token quantity;
- maximum proved route capacity;
- `cost_completeness`;
- route mode;
- cohort skew and leg lineage;
- `opportunity_class` and exact reason.

Strict ranking requires:

1. both legs observed and completely filled;
2. common quantity proved;
3. skew no greater than 60 seconds;
4. every required component strict eligible and current;
5. executable route mode;
6. positive net edge after all required costs.

Research estimates retain their assumptions and may be negative or positive.
They appear in a separate table and never upgrade a strict-unavailable route.

## 5. Storage and publication

Each validated cohort is immutable:

```text
data/local/routes/bundles/<route_cohort_id>/
├── route_legs.csv
├── cost_components.csv
├── route_opportunities.csv
├── route_cohort.sqlite3
└── manifest.json
```

The manifest hashes every file and records schema versions, candidate source
generation, collection deadline, route/status counts, observation bounds,
cost-completeness counts, adapter versions, and the exact fee-profile
generation. Route-core preflight bundles live below
`data/local/routes/core/bundles/` and use the private
`data/local/routes/core/latest.json` pointer. The public complete pointer,
`data/local/routes/latest.json`, is replaced only after all leg, cost, and
opportunity files validate. The API reads only the complete pointer and never
assembles a stable-looking route from either a core-only bundle or mutable CEX
and DEX latest files.

The existing daily, TVL, depth, and execution publications remain unchanged.
Raw route transcripts use bounded retention and content hashes.

Strict `prepositioned_inventory` eligibility also consumes a private,
read-only inventory evidence profile. It proves the available quote asset on
the buy venue and net Token quantity on the sell venue, retains observation
and expiry times plus an opaque profile hash, and never publishes account or
wallet identity. Missing, stale, or insufficient evidence yields
`inventory_unavailable` or `inventory_insufficient`; it is never inferred from
market depth.

The first release does not call independent DEX leg quotes atomic. A DEX–DEX
route remains a Research Estimate with
`atomic_route_simulation_unavailable` until one route-composition adapter
builds and simulates the complete two-leg calldata at the cohort block,
including allowance, native-value, router-fee, gas, and final-output evidence.

The production route timer runs every two minutes under the existing
collection lock. A missed or failed cycle leaves the last validated bundle in
place with its real timestamp; it never extends freshness or rewrites an old
cohort as current.

## 6. API and dashboard

### Daily Price Gap

Existing Screener metrics are renamed and described consistently as Daily
Price Gap:

- latest symmetric daily price gap;
- maximum gap in the selected window;
- mean gap;
- median gap.

They retain the user-selected date window and are never called executable.

### Opportunities page

A new primary navigation page, `Opportunities`, is independent of Market A/B
and the separate multi-market work. It exposes:

- strict executable table;
- research-estimate table;
- cohort freshness and 60-second SLA summary;
- filters for Token, route type, venue, notional, and availability;
- sorting by strict net USD edge by default, then net bps, capacity, skew,
  volume, and freshness;
- expandable cost breakdown and exact source timestamps;
- adjacent information disclosure for every N/A;
- a link from a route into the existing Token liquidity/execution research
  page without changing A/B state globally.

An available empty strict table is distinct from a missing/corrupt bundle. The
API returns route-level unavailable states with HTTP 200; a structurally
invalid bundle returns 503. Frontend requests keep route ownership and cannot
overwrite a newer filter/navigation state.

## 7. DEX adapter order

Development follows measured production value, not protocol count.

### Batch 0: existing concentrated-liquidity execution

Implement exact fixed-block execution for the 65 markets whose V3 depth is
already observed. Use protocol-exact integer math or a validated same-block
Quoter. Continuous segment approximations cannot publish strict execution.
Standard Uniswap V3-compatible pools precede fork-specific variants.

### Batch 1: Uniswap V4

Implement PoolManager/PoolKey/StateView identity and standard, allowlisted
hook behavior. A V4 Pool ID is not treated as a contract address. Pools with
hooks that can change swap mechanics remain unsupported until separately
validated.

### Batch 2: Balancer

Implement Balancer V2 and V3 as different adapters. They must not share a
generic invariant assumption.

### Batch 3: Solana

Build common fixed-slot RPC, program-owner validation, mint decimals, raw
account hash, and slot/block-time lineage, then implement Orca, Meteora, and
Raydium in that order. Protocol-specific math remains isolated.

### Batch 4: long tail

Camelot/Algebra, PancakeSwap Infinity, Curve implementation families,
SyncSwap/ZKSwap, and Velodrome V2 follow after the higher-value batches.

Every adapter has known-answer fixtures, same-block/slot lineage tests,
integer/base-unit checks, monotonicity checks, unsupported-model counterexamples,
and live read-only smoke verification before production enablement.

## 8. Event clock view

Event lifecycle remains evidence-based and unchanged. The API derives a
separate clock state at request time:

- `past` when the effective interval ends before `clock_as_of_utc`;
- `future` when it starts after `clock_as_of_utc`;
- `current_window` when the interval contains the clock time.

Minute/second facts use their exact instant. Day/month precision uses the
existing effective interval, so the API does not invent a first-day instant.
The API adds `clock_as_of_utc`, per-event clock metadata,
`clock_state_counts`, and a `clock_state` query filter.

The Events page has independent Time (`All`, `Future`, `Past`, `Current`) and
Evidence lifecycle filters. A `scheduled + past` event displays “effective
time passed; occurrence unconfirmed” and remains scheduled. URL round-trip,
mobile layout, empty states, and clock/lifecycle intersections are tested.

## 9. Quality and release gates

Required automated evidence includes:

- exact 0, 60, and greater-than-60-second skew boundaries;
- deterministic concurrent ordering and bundle hashes;
- per-venue limit/deadline behavior;
- one failed route not suppressing healthy routes;
- common-quantity and fee-asset known answers;
- strict-incomplete components excluded from executable ranking;
- assumed/estimated values never promoted to strict;
- secret/account material absent from output and logs;
- adapter known answers and unsupported counterexamples;
- Event precision and Past/Future/lifecycle intersections;
- API generation, caching, 200/503, N/A reason, sorting, navigation, desktop,
  and mobile browser tests;
- Python 3.8 grammar/import compatibility;
- full local and production test suites;
- health, release checker, source freshness, exact GitHub/server SHA, and real
  browser verification.

Production collection starts with a dry-run manifest and one bounded Token
cohort. The old site remains the rollback target. Upbit daily identities and
facts are not modified by this work.

## Non-goals

- Funding Rate or perpetual-market catalog;
- order submission, API-key custody, wallet signing, or capital allocation;
- guaranteed fills or profit;
- cross-chain bridge execution without a dedicated adapter;
- treating a public/default fee schedule, MEV buffer, or user assumption as a
  measured strict Fact;
- changing Market A/B into a multi-market selector.
