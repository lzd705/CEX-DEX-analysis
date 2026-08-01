# P0 Trust Closure Design

## Goal

Close the five research-trust failures found in production without discarding
valid historical observations or changing the already-correct date transaction
boundary.

## Decisions

1. Crypto.com candles are accepted only after one run-scoped official
   `get-instruments` inventory preflight. A catalog failure is a retryable
   technical failure; an absent instrument is a non-retryable current catalog
   fact. Existing flat candles are retained as historical evidence but are
   quarantined from current prices, returns, volatility, coverage, aggregates,
   primary selection, and comparisons by a reviewed lifecycle manifest.
2. The manifest says only that the instrument was absent from the official
   current catalog at the recorded check. It never invents a delisting date.
   The `full` and `daily` profiles refresh this evidence from the exact official
   tradable spot inventory. Raw bytes are retained by SHA-256 and the runtime
   manifest is replaced only after strict parsing and validation. Root
   source/freshness evidence remains present even when there are zero absences.
3. CEX identities preserve the exact venue base and quote assets. Endpoint
   encodings such as `MORPHO-USDT`, `MORPHOUSDT`, or `USDT-MORPHO` may differ,
   but `MORPHO/KRW`, `MORPHO/USDT`, and `MORPHO/USD` are three different
   markets and are never aliases for one another. Upbit therefore collects only
   the exact configured instrument. Coinbase and Kraken rows use `/USD` because
   those adapters call USD products. Within a conclusive recollection window,
   known legacy Coinbase/Kraken rows that were incorrectly labeled `/USDT` are
   replaced by same-date exact `/USD` rows; this is a bounded correction of
   corrupted identity metadata, not quote-asset aliasing. Historical Upbit `/KRW` rows
   produced by the retired fallback path are removed only with the explicit
   `--remove-legacy-upbit-krw-fallback` migration switch, inside a declared
   window, and only when the candidate contains the same-date exact `/USDT`
   observation. `no_data` or `not_listed` alone never authorizes deletion of a
   published historical row. The default collector continues to preserve a
   legitimately configured `/KRW` market.
4. Screener/catalog quality and selected-window quality remain separate facts.
   Both disclose their evaluation windows, observed values, and thresholds.
5. Every execution N/A is derived from the canonical status/reason published
   for that scenario or result. A structural unsupported result is never
   described as uncollected.
6. Token or A/B identity changes clear Screener-origin severity filters. On the
   Quality page a valid A/B change defaults to the selected-pair scope.
7. Static asset URLs use a runtime content/release fingerprint and `/health`
   exposes the application and asset identities used for deployment evidence.

## Safety boundary

Valid historical observations and raw source evidence are retained. A
conclusive bounded recollection may replace normalized rows whose market
identity was demonstrably mislabeled; it does not rewrite an actual USD, USDT,
or KRW market into another quote asset. Quarantine is reversible and source-
backed. Missing facts stay null and do not become zero. No public retry control
is shown for an official-current-catalog absence.
