# Point-in-time CEX order-book depth contract

## Scope

The collector covers every distinct CEX Token/exchange/instrument identity in
the currently published market catalog. It reads public spot order books. It
does not calculate depth from daily volume, infer hidden orders, or claim that
a REST snapshot is executable after collection.

Depth is a point-in-time fact and therefore remains separate from the daily
OHLCV SQLite database.

## Calculation

For best bid \(b\) and best ask \(a\):

```text
midpoint = (b + a) / 2
quoted_spread = a - b
quoted_spread_bps = (a - b) / midpoint * 10,000
```

For each symmetric band \(k \in \{10,25,50,100\}\) basis points:

```text
bid boundary = midpoint * (1 - k / 10,000)
ask boundary = midpoint * (1 + k / 10,000)

bid depth USD = sum(price * base quantity * quote_to_usd)
                for returned bids at or above the bid boundary

ask depth USD = sum(price * base quantity * quote_to_usd)
                for returned asks at or below the ask boundary

total depth USD = bid depth USD + ask depth USD
```

This is quote notional resting inside a price band. It is not trade volume,
TVL, active DEX liquidity, guaranteed executable size, or measured slippage.

USDT books use the explicit `1 USDT = 1 USD` proxy. Coinbase and Kraken USD
books use `1 USD = 1 USD`. When Upbit falls back to a KRW market, the collector
converts KRW notional using the midpoint of a separately retained
`KRW-USDT` order-book response.

## Completeness

Most public REST endpoints return a limited number of price levels. A band's
depth is marked complete only when:

- the farthest returned bid reaches or crosses the lower band boundary; and
- the farthest returned ask reaches or crosses the upper band boundary.

Coinbase `level=2` is documented as the full aggregated order book and is
treated as complete. Every other unproven band is marked incomplete. Its
reported depth is an observed lower bound and the website prefixes it with
`≥`.

`observed` means every configured band is complete. `partial` means the book
and best quotes were observed but one or more bands are truncated. `failed`
means no valid two-sided order book was normalized for that cataloged market.
Failures remain missing; they are never replaced with zero.

## Collection sequence

1. Read distinct Token, exchange, and canonical instrument identities from the
   published SQLite database; fall back to the reviewed detailed CEX CSV only
   when the database is unavailable.
2. Translate the canonical instrument into the venue's native symbol.
3. Request one public spot order-book snapshot per cataloged market.
4. Save the unmodified successful response, or a structured failure record.
5. Validate positive price/quantity levels and reject empty, locked, or crossed
   books.
6. Calculate best quotes, spread, band depth, quote conversion, and
   conservative completeness flags.
7. Validate exact market inventory coverage and publish the latest snapshot
   plus append-only normalized history.
8. Before the managed commit phase starts, preflight both depth and
   execution-cost coverage: require at least 90% current usable coverage and
   95% retention of comparable prior `observed`/`partial` identities,
   including the exchange-cohort gate in `collection-operations.md`.

## Cohort identity and publication boundary

One accepted order-book response produces one depth row and the ten matching
execution rows (two directions at five notionals) for that exact Market. A full
family candidate has exactly one nonempty `snapshot_id`; the execution
`snapshot_id` and `source_snapshot_id` must equal the depth `snapshot_id`, and
the execution Market count must equal the exact published depth inventory row
count. The ID is therefore an inventory-bound publication/source-lineage key.
It is not an observation timestamp and does not prove that different venues
were observed simultaneously.

The public depth history, depth latest/current views, and execution latest view
are validated before replacement and then passed to one family publication
bundle. The bundle restores all pre-existing public bytes when an ordinary
in-process I/O exception interrupts replacement. This is failure-atomic error
handling for ordinary I/O failures only. It is not process-crash atomic: power
loss, interpreter termination, or an operating-system crash can still leave a
partially replaced multi-file family and requires manifest/hash diagnosis.
The private processed depth/execution `current` files are written independently
and are not part of the public reader boundary.

Metadata exposes canonical `observed_at_min`, `observed_at_max`, and
`observation_span_seconds` for the family. These bounds describe bounded
sequential observations collected across venues. They do not convert the
family into a same-instant order book. A genuine source absence remains its
explicit non-measured status with numeric values blank/JSON `null`; neither
publication nor lineage validation converts it to zero.

## Files

| File | Meaning |
| --- | --- |
| `data/processed/cex_depth_snapshot.csv` | Current collection awaiting review |
| `data/local/cex_depth_latest.csv` | Latest published complete market inventory |
| `data/local/cex_depth_history.csv` | Append-only normalized snapshot history |
| `data/raw/cex-depth/<snapshot_id>/*.json` | Raw venue responses, conversion responses, failures, and manifest |

Runtime files are Git-ignored. The collector, tests, and this contract are
versioned.

## Public sources

| Exchange | Public spot order-book endpoint | Requested levels |
| --- | --- | ---: |
| Binance | `GET /api/v3/depth` | 100 |
| OKX | `GET /api/v5/market/books` | 400 |
| Bybit | `GET /v5/market/orderbook?category=spot` | 1,000 |
| KuCoin | `GET /api/v1/market/orderbook/level2_100` | 100 |
| Gate | `GET /api/v4/spot/order_book` | 100 |
| Bitget | `GET /api/v2/spot/market/orderbook` | 150 |
| MEXC | `GET /api/v3/depth` | 100 |
| HTX | `GET /market/depth?type=step0` | up to 150 |
| Coinbase Exchange | `GET /products/{product}/book?level=2` | full aggregated book |
| Kraken | `GET /0/public/Depth` | 500 |
| Crypto.com Exchange | `GET /public/get-book` | 50 |
| Upbit Korea | `GET /v1/orderbook` | 30 |

The implementation uses only unauthenticated market-data endpoints. Venue
limits are recorded per row so coverage can be audited rather than assumed.

## Freshness and interpretation

The observation time prefers the exchange-provided order-book timestamp and
falls back to the local response-received time when the venue omits one.
Because markets change continuously, comparisons across exchanges are
bounded sequential observations, not a simultaneous or atomic cross-venue
book. `observation_span_seconds` reports the canonical earliest-to-latest
observation span; it is cohort-skew metadata, not a simultaneity guarantee.

Repeated snapshots can form an observation history, but the current collector
does not claim tick-perfect reconstruction. WebSocket sequence maintenance is
required for that stronger use case.
