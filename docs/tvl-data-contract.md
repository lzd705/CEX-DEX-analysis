# Point-in-time DEX pool TVL contract

## Scope

The TVL collector covers every DEX pool already present in the published market
catalog. It does not discover new pools and it does not rewrite daily OHLCV.

The source fact is GeckoTerminal API v2 `reserve_in_usd`, described by the
provider as the pool's total liquidity/reserve in USD. The website labels this
as source-reported point-in-time TVL. It is not:

- a historical daily TVL series;
- CEX order-book or DEX pool-state depth;
- active Uniswap V3 liquidity;
- a substitute for protocol-specific reserve, tick, or quote data.

## Collection sequence

1. Read distinct Token, chain, DEX, pool address, and pool name identities from
   the published SQLite database. Fall back to the reviewed DEX CSV only when
   the database is unavailable.
2. Group pools by chain.
3. Query at most 30 same-chain pool addresses through
   `/api/v2/networks/{network}/pools/multi/{addresses}`.
4. Save each raw JSON response before publishing normalized rows.
5. Write one explicit result row for every cataloged Token/pool identity.
6. Validate exact inventory coverage, unique keys, statuses, and non-negative
   finite TVL.
7. Publish the current snapshot and append immutable local history.

## Files

| File | Meaning |
| --- | --- |
| `data/processed/dex_pool_tvl_snapshot.csv` | Current collection awaiting review |
| `data/local/dex_pool_tvl_latest.csv` | Latest published complete pool coverage |
| `data/local/dex_pool_tvl_history.csv` | Append-only normalized snapshot history |
| `data/raw/tvl/<snapshot_id>/*.json` | Unmodified batch responses and manifest |

Runtime files remain Git-ignored. Code, tests, and this contract are versioned.

## Row contract

Each row contains:

- snapshot, request, response, and observation timestamps in UTC;
- Token, chain, configured DEX, pool address, and configured pool name;
- source DEX, source pool name, and base/quote token relationship IDs;
- source-reported TVL, base/quote USD prices, 24-hour volume, and pool creation
  time when returned;
- method, source endpoint, and full raw-response SHA-256;
- one of `observed`, `missing`, `not_found`, or `failed`, plus an error reason.

Missing TVL remains blank/JSON `null`. It is never replaced with zero and the
website does not fall back to an older value when a complete latest snapshot
explicitly reports a missing or failed pool.

## Run

```bash
python3 scripts/fetch_tvl.py --publish-local
```

The public GeckoTerminal API documents a 30-call-per-minute headline limit.
Multi-pool requests can consume more quota than a single-pool call, so the
collector uses same-chain batches, stays below five batches per minute by
default, and honors 429 backoff before retrying.

## Acceptance checks

- Inventory contains the same Token/pool keys as the published market database.
- Every current pool has exactly one status row.
- A full-inventory publication contains at least one observed TVL fact; a total
  source outage fails the full publication. An exact one-pool recovery may
  start from a zero-observed baseline only when the target becomes observed or
  a resolver-confirmed terminal absence and every non-target fact is retained.
- At least 80% of the current inventory is observed, and at least 95% of
  comparable previously observed pools remain observed; chain-cohort regression
  is also gated as documented in `collection-operations.md`.
- Observed TVL is finite and non-negative.
- Every successful batch has a retained raw response and SHA-256.
- Repeated full publication appends history and atomically replaces its latest
  view. A canonical one-pool retry stages history/latest/current together and
  restores all prior bytes on any ordinary replacement exception; it is not a
  crash-atomic multi-file guarantee.
- A canonical one-pool retry collects only that pool, requires an existing
  full baseline, appends only the target observation to history, and merges the
  target into the complete latest inventory without changing any non-target
  source-evidence field. Its exact gate does not require the repaired full
  snapshot to immediately cross the full 80% floor; it instead requires 100%
  retention of prior observations and a complete-row sealed target result.
- The website exposes TVL observation time, method, status, and source lineage
  through its market payload and catalog.
- DEX depth reads this snapshot only for pool inventory and token USD prices;
  it never converts TVL itself into depth.
