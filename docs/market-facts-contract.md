# Market catalog and two-venue comparison contract

## Public endpoints

- `GET /api/markets/catalog` returns the versioned catalog, dataset hashes,
  available date range, market identities, source fields, quote assets, and
  semantic limits.
- `GET /api/markets/compare?token=...&market_a=...&market_b=...&start=...&end=...`
  returns the union of the two selected markets' daily UTC observations.
- `GET /api/markets/execution-cost?token=...&market_a=...&market_b=...`
  returns independent point-in-time $1k/$5k/$10k/$50k/$100k quoted execution
  facts for the same exact market identities.

`market_a` and `market_b` are exact `market_id` values returned by the catalog.
They must be different and both must belong to the requested Token.

## Fact definitions

| Output | Definition |
| --- | --- |
| Price | Source daily close, normalized to USD |
| Volume | Source daily USD volume for that exchange pair or pool |
| Absolute spread | `abs(price_a_usd - price_b_usd)` |
| Spread bps | `absolute spread / midpoint(price_a, price_b) * 10,000` |
| Grain | One UTC day |

When a separately published `cex_depth_latest.csv` exists, CEX catalog entries
also expose point-in-time best bid/ask, quoted spread, and 10/25/50/100 bps
order-book depth. Those fields are not part of the daily comparison series.
Their calculation, quote conversion, completeness flags, and audit contract are
defined in `docs/cex-depth-data-contract.md`.

When `dex_depth_latest.csv` exists, DEX catalog entries expose fixed-block
10/25/50/100 bps sell, buy, and total pool-state depth, protocol model, fee,
block number, completeness, and source lineage. Unsupported protocols remain
`null`. The exact contract is `docs/dex-depth-data-contract.md`.

The independent execution-cost endpoint is backed by
`cex_execution_cost_latest.csv` and `dex_execution_cost_latest.csv`. It never
interpolates the four depth bands. Complete, partial, unsupported, and failed
semantics, fee scope, formulas, and publication gates are defined in
`docs/execution-cost-data-contract.md`. Its requested notionals are JSON
numbers; measured Decimal facts remain exact base-10 strings or `null`.

CEX configured pair labels normally use USDT. The current adapter contract uses
USDT as a 1:1 USD proxy; Upbit KRW observations are converted through the
daily USDT/KRW rate. Some adapters fetch a venue-native USD pair even when the
stored configured label is `TOKEN/USDT`, so the catalog describes that label
as canonical rather than claiming it is the raw venue instrument.

DEX price and volume facts come from GeckoTerminal API v2 daily pool OHLCV with
`currency=usd` and the configured target Token side.

## Missing values

Missing source values remain JSON `null`. The API emits the union of observed
dates and does not forward-fill:

- `market_a_missing`: only market B has a row on that date;
- `market_b_missing`: only market A has a row on that date;
- `non_comparable_price`: both rows exist, but a finite positive close is not
  available.

Absolute spread and bps are `null` unless both prices are comparable on the
same UTC date. Volume is never replaced with zero.

## Explicit non-claims

The comparison input is daily aggregate OHLCV. It is not order-book depth,
top-of-book bid/ask spread, an executable quote, or measured slippage. The
comparison page must not relabel it as any of those concepts. Separately
collected CEX order-book fields retain their own point-in-time timestamps and
must not be presented as daily history or guaranteed execution. Separately
collected DEX pool-state fields are also point-in-time measurements and exclude
gas, MEV, and post-block state changes.

## Known-answer fixtures

`tests/fixtures/market_known_answers.json` fixes expected values for:

- absolute and midpoint-bps price calculations;
- integer base-unit conversion using explicit Token decimals.

The decimals utility is a normalization primitive for future raw on-chain
facts. Current GeckoTerminal observations are already decoded numeric USD
values, so the website does not pretend to reverse-engineer raw pool balances.
