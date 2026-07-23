# Market Monitor Design

## Page structure

1. Global time window: start, end, 7D, 30D, 90D, and All.
2. Sorting controls: `综合`, `CEX`, `DEX`; default USD volume descending.
3. Token table: one comparison row followed by one selected CEX row and one
   selected DEX row.
4. Source footer: file fingerprints, freshness, formula, and data limitations.

The dense table follows the market-terminal reference. It avoids card-heavy
research-report composition and keeps repeated comparison work visible in one
scan.

## Spread placement

Spread is not a property of either market alone. For a selected Token, CEX pair,
DEX pool, and date window:

```text
price_spread = dex_price / cex_price - 1
```

The two prices must come from the latest date observed by both selected markets.
It is shown only on the Token comparison row. CEX and DEX rows show `--` in the
spread column. If either selected price or a common date is missing, spread is
`N/A`.

## Data contract

The server reads detailed daily CEX and DEX files and returns compact summaries
for the selected window. It does not send every raw observation to the browser.
Each venue/pool summary includes latest price, window return, daily log-return
volatility, summed USD volume, observation-day count, and latest observation
date. DEX summaries may also include the latest available TVL snapshot.
