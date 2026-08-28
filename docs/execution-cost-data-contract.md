# Fixed-notional quoted execution-cost contract

## Question answered

For each market and requested notional in USD, the snapshot answers:

> If the requested Token quantity were executed against this one captured
> source state, how much quote asset would be received or paid, and what would
> the quoted execution shortfall be relative to the pre-trade reference price?

The requested notionals are:

```text
$1,000, $5,000, $10,000, $50,000, $100,000
```

This is a point-in-time source-mechanics fact.  It is not realized execution,
an all-in trading cost, or a promise that the captured liquidity remains
available.

## One comparable Token quantity

For requested notional `N`:

```text
target Token quantity = N / reference price in USD per Token
```

DEX quantities are rounded down to the Token's integer base unit.  The row
therefore retains both `requested_notional_usd` and the exact
`reference_notional_usd` of the quantized quantity.  Quoted cost uses the
latter so rounding residue is not mislabeled as slippage.

Both directions use that same target Token quantity:

- `sell_token`: exact-input Token; consume CEX bids or swap Token to quote;
- `buy_token`: exact-output Token; consume CEX asks or swap quote to Token.

Defining buy as "spend exactly N dollars" would compare a different Token
quantity and is deliberately not used.

## Reference prices

- CEX: `(best bid + best ask) / 2` from the same normalized order-book
  response used for the level walk.
- DEX: the pre-trade, pre-fee marginal pool price at one fixed block.

The DEX inventory's external Token USD price is retained for off-market
quality checks.  It does not replace the pool-state reference price.

For DEX, the USD reference is reconstructed as:

```text
pool quote per Token x quote-token USD price
```

The fixed-block state time and the time this project received the external USD
price response are independent inputs. Their absolute observation skew must be
no more than two hours. A missing, invalid, or older price input fails the
publication gate. The public API also applies the same rule defensively to an
older snapshot: scenario lineage remains visible, but fill, VWAP, and cost are
withheld as `null`/N/A rather than shown as zero. USD and USDT identity/proxy
conversions do not have an independent price timestamp and are labeled not
applicable instead of pretending their skew is zero.

## Calculation

For a complete `sell_token` request:

```text
quote received USD = quote received * quote_to_usd
quoted execution cost USD
  = reference notional USD - quote received USD
quoted execution cost bps
  = quoted execution cost USD / reference notional USD * 10,000
```

For a complete `buy_token` request:

```text
quote paid USD = quote paid * quote_to_usd
quoted execution cost USD
  = quote paid USD - reference notional USD
quoted execution cost bps
  = quoted execution cost USD / reference notional USD * 10,000
```

No value is interpolated from the cumulative 10/25/50/100 bps depth bands.
CEX results walk the original returned price levels. Supported DEX V2 results
execute the exact integer constant-product invariant captured at the fixed
block. The two authority-bound Ethereum Uniswap V3 UNI pools use the exact
integer adapter described below; other V3 markets remain structurally
unsupported.

## Cost scope

| Source | Included | Explicitly excluded |
| --- | --- | --- |
| CEX | quoted spread and visible-book price impact | account-specific taker fee, lot-size rejection, latency, hidden liquidity |
| DEX | pool swap fee and pool-state price impact | gas, router fee, Token transfer tax, MEV, state changes after the fixed block |

CEX account fee tiers are not public facts for an anonymous market-data
request.  They remain excluded and null rather than being assumed to be zero.
The fields are named `quoted_execution_cost_*`; they must not be described as
realized or all-in cost.

## Route cost-component facts

The fixed-notional contract above remains unchanged. Route evaluation uses a
separate `execution_cost_component/v1` fact contract implemented by
`scripts.execution_cost_components`. It records the provenance and strictness
of each route-specific cost without redefining a quoted execution shortfall as
an all-in cost.

### Grain and identity

One component row has the fixed key:

```text
cohort_id × opportunity_id × leg × component_type
```

`opportunity_id` identifies one directed route and requested-notional scenario.
Every row for that opportunity must retain the same positive
`requested_notional_usd` and positive common `target_token_quantity`. `leg` is
exactly `buy`, `sell`, or `route`; buy and sell rows retain their canonical
market ID and direction, while a route-level row has a blank market ID and the
direction `route`. Duplicate component keys and conflicting leg/scenario
identity reject the inventory. The row schema is closed: missing or additional
columns, non-string schema keys, unknown legs/directions, and a leg whose market
or direction changes within one opportunity all fail with a controlled contract
error.

The required strict component inventory is:

```text
venue_taker_fee
pool_swap_fee
network_gas
router_or_integrator_fee
token_transfer_tax
rebalancing_or_transfer
```

Every kind must be represented, including a proven `not_applicable` row when
the route contract establishes that the cost does not apply. Absence is not a
zero. `mev_buffer` is an optional research-scenario component and is never
required to complete the strict inventory.

### Numeric and evidence contract

All stored quantities, USD amounts, and bps rates are canonical base-10
strings. Binary floating-point input, negative values, non-finite values, zero
notional, and zero Token quantity are rejected. A numeric component must retain
both `amount_usd` and `rate_bps`, with this exact identity:

```text
amount_usd × 10,000
  = rate_bps × requested_notional_usd
```

Its non-empty `basis` states the calculation or applicability proof. The exact
requested notional is therefore the bps denominator; no implicit capital basis
is allowed. Validation and aggregation decompose every finite Decimal into its
integer coefficient and base-10 exponent. Equality and sums use arbitrary-
precision integer arithmetic, not the process Decimal context or an arbitrary
fixed precision. Lowering the caller's Decimal precision therefore cannot make
an approximate rate pass or truncate a large aggregate.

Allowed component value statuses are:

```text
measured authenticated quoted bounded_estimate assumed
not_applicable unavailable unsupported failed stale
```

`measured`, `authenticated`, and `quoted` values require a timezone-aware RFC
3339 observation time and a lowercase 64-hex source-record SHA-256.
Authenticated and quoted evidence additionally requires a `valid_until` later
than its observation time. `bounded_estimate` and `assumed` are always strict
ineligible. `unavailable`, `unsupported`, `failed`, and `stale` are also strict
ineligible, require a stable reason code, and contain null amount/rate fields.
A proven `not_applicable` row is strict eligible but contains no numeric zero.

`mev_buffer` is narrower than the general status inventory. It may be
`bounded_estimate`, `assumed`, or a non-numeric terminal status only. It may
never be `measured`, `authenticated`, `quoted`, or `not_applicable`, is always
strict ineligible, and is excluded defensively from the strict total. When
explicit assumptions are enabled, a numeric MEV buffer contributes only to the
scenario total.

Pool swap fees are already part of the exact DEX leg quote. A numeric
`pool_swap_fee` row must therefore set `embedded_in_leg_quote = true`; no other
component may do so. Aggregation records that evidence but excludes the amount
from every non-embedded additive total, preventing the fee from being charged
twice.

### Aggregation

`aggregate_cost_components(rows, include_assumptions)` accepts one opportunity
and returns exact Decimal strings or null:

- `strict_amount_usd` is present only when all six required kinds have strict
  evidence or proven non-applicability;
- `scenario_amount_usd` may also use `bounded_estimate` and `assumed` rows only
  when `include_assumptions` is true;
- `missing_required_kinds` and `scenario_missing_required_kinds` name every
  incomplete kind;
- `completeness` and `scenario_completeness` are explicitly `complete` or
  `incomplete`.

An incomplete aggregate has a null total even when some known component rows
are numeric. This prevents a partial sum from being presented as an all-in
route cost. A complete all-`not_applicable` inventory may produce a calculated
aggregate zero; that differs from converting absent evidence to zero.

## Long-form grain and identity

One row is one:

```text
snapshot × market_id × direction × requested_notional_usd
```

Every cataloged market must have exactly ten rows: five notionals times two
directions.  CEX IDs identify an exchange instrument.  DEX IDs identify one
Token perspective in one physical pool.

The row retains source snapshot IDs, timestamps, endpoint, sequence or block,
raw hash, reference and USD-conversion lineage, Token and quote identities,
fill facts, fee scope, exclusions, status, reason, and error.

## Statuses

| Status | Meaning |
| --- | --- |
| `observed` | The exact target Token quantity was completely filled in the captured state. |
| `partial` | The source state was valid, but the request could not be completely filled or proved within the captured level/tick guard. |
| `unsupported` | No protocol-specific, project-validated adapter exists for the venue, chain, or pool model. |
| `failed` | A supported adapter encountered an API, RPC, ABI, normalization, conversion, or validation failure. |

Partial rows may retain filled Token quantity, fill ratio, and observed quote
amount.  Full-request VWAP and quoted-cost fields remain blank.  A partial cost
is not a lower bound and must never be displayed with `>=`.

`status_reason` distinguishes at least:

- `target_filled`;
- `source_level_limit`;
- `full_book_insufficient_liquidity`;
- `full_pool_reserve_insufficient`;
- `full_target_quantity_filled`;
- `source_tick_scan_limit`;
- `exact_integer_swap_math_not_implemented`;
- `unsupported_protocol_or_chain`;
- `order_book_fetch_or_normalization_failed`,
  `pool_state_collection_failed`, or `execution_calculation_failed`.

## DEX adapter boundary

Constant-product V2 pools calculate the configured targets directly, subject
to reserve constraints. V3 execution is enabled only for the exact identities
in `config/uniswap_v3_execution_markets.json`: the Ethereum Uniswap V3
UNI/USDT 0.3% and UNI/WETH 0.3% pools. The authority binds chain ID, pool,
factory, `factory.getPool`, token order and decimals, fee, tick spacing, and
QuoterV2 address. Any mismatch fails the supported adapter; it does not fall
back to a generic V3 label.

For those two markets, depth and execution share one finalized-block state
window. The collector reads the current bitmap word and expands in each
direction up to the configured bounded radius, retaining every bitmap word and
every initialized tick in the raw transcript. `scripts/uniswap_v3_math.py`
ports the observable integer rounding of core `TickMath`, `SqrtPriceMath`, and
`SwapMath`; sells are exact-input simulations and buys are exact-output
simulations. Decimal arithmetic is used only after the integer quote for human
units and USD presentation.

Every complete scenario must match same-block QuoterV2 on the quoted raw
amount, final `sqrtPriceX96`, and the periphery bitmap tick counter. A mismatch
or invalid response makes all ten execution scenarios for that pool `failed`,
while independently valid depth remains available. The Quoter response must be
the complete four-word ABI result, with the documented uint160 and uint32
widths; truncated or over-wide evidence also fails. Reaching the proven bitmap
boundary before the amount resolves is `partial` with
`source_tick_scan_limit`; full cost fields remain blank. All V3 identities
outside the two-record authority remain `unsupported` with
`exact_integer_swap_math_not_implemented` and null numeric execution fields.

## Files and publication

| File | Meaning |
| --- | --- |
| `data/processed/cex_execution_cost_snapshot.csv` | Current CEX calculation awaiting publication |
| `data/local/cex_execution_cost_latest.csv` | Latest published complete CEX scenario inventory |
| `data/processed/dex_execution_cost_snapshot.csv` | Current DEX calculation awaiting publication |
| `data/local/dex_execution_cost_latest.csv` | Latest published complete DEX scenario inventory |

The execution rows reuse the corresponding depth collector's raw order-book or
fixed-block transcript and its SHA-256 lineage.  Publication requires exact
ten-row coverage for every current source inventory market.

Depth and execution coverage are preflighted as one publication bundle. A
coverage regression in execution blocks the matching depth commit phase. CEX
requires 90% current usable scenario coverage; supported DEX execution
requires 80%; both retain at least 95% of comparable prior usable scenario
identities. An authority-approved V3 market is in the supported denominator,
so `unsupported` or `failed` rows cannot hide as a structural gap. Other V3
identities remain excluded until separately approved.
Full-inventory publishes still replace the files separately. A canonical
one-market retry stages and failure-atomically replaces the bounded
depth/history/execution bundle for ordinary I/O exceptions, while deliberately
making no crash-atomic multi-file claim; see `collection-operations.md`.

For the two authority-approved V3 markets, release health also binds the
canonical public exact receipt to the retained private raw receipt created
before publication. The two byte hashes must be valid and equal; a missing,
nonregular, symlinked, tampered, or mismatched private receipt cannot release.
The raw artifact remains outside the five public destinations and its path is
never projected through the API.

For a canonical one-market retry, the candidate must contain the exact ten
scenario keys already assigned to that market. The merge preserves every
non-target scenario fact, rejects mixed source lineage, and publishes the full
inventory under the new depth publication generation. No execution-history
rows are synthesized for unchanged markets.

This release deliberately publishes current/latest execution snapshots only.
It does not build one unbounded hourly history CSV: at the current inventory
size that design would add more than one hundred thousand rows per day and
repeatedly rewrite all prior history. Retained raw depth transcripts and
manifests remain the audit source. Historical execution snapshots require
immutable date/snapshot partitions plus an explicit retention and index
contract; until that store exists, no execution-history availability is
claimed.

## Publication hard gates

- no duplicate primary keys and no missing market/direction/notional rows;
- exact configured notionals and the same source lineage across one market;
- the canonical notional definition and all measured source, endpoint,
  timestamp, fee/conversion scope, and 64-hex raw-response hash fields exist;
- measured DEX rows retain one coherent block number/timestamp plus target and
  quote Token identities and decimals, the exact pool swap fee, and the external
  Token-price snapshot lineage used to define the USD target;
- measured DEX USD-price response time is no more than two hours from the fixed
  block time;
- observed fill ratio equals one; partial fill ratio is below one;
- partial filled quantity and quote amount are either both present and
  recomputable or both absent;
- unsupported and failed rows contain no execution numbers;
- reference USD, quote USD, VWAP, cost, and bps all recompute from retained
  fields;
- observed scenarios form a prefix as notional grows;
- fill ratio cannot improve after notional grows;
- observed quoted cost bps cannot decrease with notional beyond the retained
  quote asset's one-base-unit rounding resolution;
- sell VWAP cannot improve and buy VWAP cannot improve as notional grows beyond
  that same explicit raw-unit resolution;
- missing values remain blank/JSON null; only a measured zero is zero.

The dedicated API compares only exact catalog market IDs for the same Token
and returns its contract, fee scope, exclusions, source snapshots, and snapshot
skew. `requested_notional_usd` is a safe JSON number; all measured Decimal
fields are exact base-10 JSON strings (or `null`) so quantities above IEEE-754's
safe integer range remain auditable. Consumers may parse strings for plotting,
but must retain the strings for formula and base-unit verification. Execution
rows are not embedded wholesale into the market catalog.
