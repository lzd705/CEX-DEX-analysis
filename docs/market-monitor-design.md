# Market Monitor Design

## Page structure

1. `Screener` is the cross-Token entry point. The global start/end, 7D, 30D,
   90D, and All controls apply only to daily facts.
2. Selecting a Token opens one persistent workspace with four pages:
   `Markets`, `Compare`, `Liquidity & Execution`, and `Data Quality`.
3. Market A and Market B are exact market IDs for the selected Token and remain
   in the URL while the user moves between those pages.
4. `Methodology` is the shared definition layer; Data Quality reports the
   current status and lineage of one Token's actual markets.

The workspace separates discovery, comparison, executable-liquidity analysis,
and evidence without losing the selected Token or market pair. TVL, depth, and
execution remain independently timestamped latest snapshots rather than
pretending to follow the daily date selector.

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
