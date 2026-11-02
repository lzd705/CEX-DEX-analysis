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
   retired through a migration explicitly scoped to those two exchanges,
   without treating USD and USDT as interchangeable. During that run every
   Upbit row is immutable and the preflight verifies the complete Upbit row
   multiset. Historical Upbit `/KRW` rows produced by the retired fallback path
   are a separate migration and leave served facts only through the explicit
   Upbit switch and declared Upbit-only scope. Removed rows are never rewritten
   as another market: complete original rows and their dispositions are
   retained in an atomically published quarantine bound to baseline,
   candidate, and row-set SHA-256 values. Genuine exact baseline dates remain
   mandatory, while an alias-only date becomes missing rather than synthetic.
   Every quarantine is also retained under an immutable content-addressed
   filename. The baseline hash is computed from the authoritative SQLite export
   after normalized equality with the public CSV is proven. Partial collection
   evidence blocks publication. The default collector continues to preserve a
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

Valid exact historical observations and raw source evidence are retained. A
conclusive bounded recollection may retire rows whose market identity was
demonstrably mislabeled; it does not rewrite an actual USD, USDT, or KRW market
into another quote asset. The complete retired rows remain reversible in the
hash-bound quarantine. Missing facts stay null and do not become zero. No
public retry control is shown for an official-current-catalog absence.
