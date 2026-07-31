# Administrator Operations

The administrator surface is absent by default. With `ADMIN_ENABLED` unset or
false, `/admin.html`, `/admin.js`, and every `/api/admin/*` route return 404.
Supplying a username or password hash alone does not expose the routes.

## Local setup

Generate a password verifier:

```bash
python3 scripts/admin_password.py
```

Copy `.env.example` to `.env`, set the following values, and paste the generated
value as `ADMIN_PASSWORD_HASH`. `.env` is ignored by Git.

```env
ADMIN_ENABLED=true
ADMIN_LOGIN_REQUIRED=true
ADMIN_ALLOW_OPEN_LOCAL=false
ADMIN_USERNAME=admin
ADMIN_PASSWORD_HASH=pbkdf2_sha256$...
```

Restart a server bound to loopback, then open:

```text
http://127.0.0.1:8765/admin.html
```

The plaintext password is never written by the setup script.

For isolated local development only, open mode requires both explicit flags:

```env
ADMIN_ENABLED=true
ADMIN_LOGIN_REQUIRED=false
ADMIN_ALLOW_OPEN_LOCAL=true
```

Open mode is rejected when the application is bound to `0.0.0.0`, `::`, a
public address, or a hostname other than `localhost`. It skips authentication
but keeps Token/date validation and permits only one queued or running refresh
job at a time. Do not use it behind a reverse proxy.

## Refresh contract

The administrator selects an active Token and an inclusive UTC date range.
Current exchange adapters support a rolling refresh only:

- `end_date` must be the latest completed UTC day;
- the window must contain 1 to 180 days;
- a runtime Token whose CEX identity has not been manually approved is
  refreshed DEX-only.

The server queues one job at a time. A job:

1. seeds an isolated collector staging directory from the selected runtime
   snapshot directory (`data/local/` by default);
2. refreshes the selected Token from CEX and DEX sources;
3. upserts rows by venue/pool/date without deleting other Tokens or older dates;
4. validates both detailed CSV schemas;
5. audits daily facts for duplicate keys, invalid OHLC/volume values,
   historical gaps, and active-market D-1 gaps;
6. rejects the candidate before publication if any hard-invalid row is found;
7. builds an indexed SQLite database with source hashes and an import record;
8. validates database integrity and row counts;
9. publishes the reviewed CSV copies, atomically replaces
   `market_facts.sqlite3` as the runtime commit point under `data/local/`, and
   atomically replaces the daily quality report.

Historical retry capacity is deliberately bounded by the collector contracts:

- every requested window is inclusive and limited to 180 UTC days;
- Binance, OKX, Bybit, KuCoin, Gate, Bitget, MEXC, Coinbase, Crypto.com, and
  Upbit use a source time bound/cursor for that window; Upbit's 200-candle cap
  remains above the 180-day application cap;
- HTX and Kraken are recent-only responses capped at 2,000 and 720 bars. The
  collector checks the oldest returned bar and records
  `source_range_unavailable` instead of pretending an unreachable date was
  collected;
- GeckoTerminal DEX OHLCV uses an exclusive `before_timestamp` cursor and a
  collector limit no greater than 180 rows for one retry.

Combining disconnected gaps into a wider interval is not automatically safe
merely because it remains under 180 days: it re-requests already observed
facts and is outside the current exact-window allowlist.
These endpoint bounds are not a parallel-request budget. Retry publications
remain sequential under the collection lock and must reload the current
quality-report lineage after every committed window.

Collection-attempt ledgers are accepted only as whole, exact evidence. A CEX
record binds Token, exchange, and canonical instrument; an Upbit or other
source instrument is a separately validated alias, never an inferred
replacement. A DEX record binds Token, chain, DEX adapter, and pool. IDs are
nonempty and unique, completion times are UTC-aware, windows are canonical and
bounded, observed dates stay inside the request, and status/reason/outcome
counts must agree. Writers validate the same contract before atomic
publication. If any record violates it, the consumer ignores the complete
ledger and leaves affected gaps `missing_unexplained`; it does not salvage a
broader Token/venue match or fabricate a source cause.

Job state and server-only logs live under the selected runtime data directory
at `admin/jobs/`.
The public server opens `market_facts.sqlite3` read-only. Existing requests
continue using the previous complete file until replacement finishes.

## Daily quality publication

Every successful `import_local_snapshot.py` run writes:

```text
data/local/quality/daily-latest.json
```

The report is built against the staged candidate, linked to the resulting
dataset snapshot and import run, then published by a single-file atomic rename.
`market_facts.sqlite3` remains the runtime data commit point; the CSV files,
database, and quality report are prepared in the same private staging directory
before any published file is replaced.

The public Data Quality API is contract v4. It preserves selected-window
`quality_*` and same-generation Screener `screening_quality_*` projections.
Release preflight requests all-scope Quality exactly once for every Summary
Token and recomputes every market status and every flag severity. Token,
generation, market count, unique market IDs, flag fields, and exact nonzero
count dictionaries must agree; a non-OK market with no structured fallback
flag or any mid-run generation drift fails the release.

The report keeps data-quality states separate:

- `hard_invalid`: duplicate primary keys; missing/invalid identities or dates;
  non-finite, zero, or negative OHLC values; inconsistent daily high/low bounds;
  or non-finite/negative volume. Any such issue blocks publication, so the
  previously published CSVs, database, and `daily-latest.json` remain unchanged.
- `backfill_pending`: a missing date strictly between a market's first and last
  observed dates. These issues appear in `backfill_pending` and
  `backfill_windows_by_token`; they do not enter the daily retry queue.
- `d1_active_gap`: the latest completed UTC day is absent for a market with at
  least three valid observations in the inclusive prior seven-day window.
  Recent trailing missing days appear in `retry_queue` and
  `retry_windows_by_token`.
- `source_no_observation`: an accepted, lineage-matched source attempt covered
  the missing day but returned no candle. It remains visible as an
  informational source outcome and is neither a warning nor an automatic
  retry candidate.
- `stale_market_unknown`: a formerly active market has aged out of the
  evidence-backed retry window without explicit inactive/delisted metadata.
  It is retained as a non-retryable `needs_review` item rather than silently
  disappearing or creating an endless automatic retry queue. An operator must
  verify current source inventory or lifecycle before disposition. If a
  lineage-matched attempt covering the latest completed UTC day instead proves
  that the source returned no candle, the market is classified as
  informational `source_no_observation/no_candles`; it is not mislabeled as
  an unknown lifecycle and does not remain in the manual-review queue.

Dates before the first observed row are never inferred to be failures or
historical gaps. A newly listed or otherwise sparse market also does not enter
the D-1 retry queue until it meets the activity threshold.

Missing-row causes are evidence-based, not inferred from absence alone:

- no matching accepted attempt: `status=backfill_pending`,
  `reason_code=missing_unexplained`;
- request/network evidence: `status=collection_failed`, with one of
  `network`, `rate_limit`, `source_unavailable`, `parse`, or `validation`;
- a successful source response without the target candle:
  `status=source_no_observation`, `reason_code=no_candles`, with no automatic
  retry because repeating the same successful empty response creates a loop.
  The explanatory issue remains visible, but it is excluded from
  `backfill_pending`, retry windows, and the report's warning status;
- source says the market is unavailable: `status=needs_review`,
  `reason_code=not_listed`, with no automatic retry;
- the source's documented recent-only range cannot reach the audited day:
  `status=unsupported`, `reason_code=source_range_unavailable`, with no
  automatic retry or manual-review queue item. The missing value stays `N/A`
  and the coverage limitation remains visible as information.

For point-in-time CEX depth/execution, a successful response without a usable
two-sided book is likewise a terminal source outcome:
`source_no_observation/source_no_two_sided_book`. It is non-retryable, remains
`N/A`, and never becomes zero depth. Transport, rate-limit, source-unavailable,
parse, and validation failures remain retryable and retain a bounded public
reason rather than a raw exception.

Only a ledger whose recorded CSV SHA-256 matches the staged candidate can
change a missing issue from `missing_unexplained`. Accepted attempts and their
ledger hashes are embedded in `daily-latest.json`; untrusted raw exception
strings, URLs, secrets, and local paths are not published.

A Token-scoped append preserves an unselected Token's prior source outcome
only when the existing report, SQLite commit identifiers, published CSVs, and
source hashes all agree. The old attempt must still explain a gap in the new
candidate. A new attempt replaces only the overlapping part of the same market
window; retained pieces are normalized, deduplicated, and rebound to the new
candidate hash. Any existing lineage or attempt-contract mismatch blocks the
append rather than silently converting a known cause back to
`missing_unexplained`.

The top-level `status` describes quality completeness. Publication outcome is
reported separately at `publication.status`:

- `published`
- `published_with_backfill`
- `published_with_retry_queue`

Every retry/backfill item includes a reason code, Token, market identity, date,
and primary-source URL hints. Windows are contiguous, Token-scoped, and limited
to 180 days. The Admin page exposes two explicitly labelled audited queues:

- `latest_completed_day` for recent active-market D-1 gaps;
- `historical_gap` for historical `backfill_pending` windows.

An authenticated operator may queue either exact window through
`job_type=retry_failed`. The request also carries its queue type; arbitrary
dates and windows not present in the current report are rejected. A completed
collector process is not enough. The quality report and SQLite database must
carry the same new import identity. Each exact expected market/date pair must
then be either present in SQLite or identified by the new report as a specific,
non-retryable source outcome such as `not_listed`,
`source_no_observation`, or `source_range_unavailable`. Retryable gaps,
`collection_failed`, and `missing_unexplained` remain unresolved. The job
result reports `observed_count`, `confirmed_absence_count`, and
`unresolved_count`, so a confirmed source absence cannot be mistaken for a
successfully collected candle.
Each emitted window also lists the affected `market_types` (`cex`, `dex`, or
both). This is source-scope evidence for a separately tested executor; the
current operator path must not silently use it to broaden dates or bypass the
exact-window allowlist.

`hard_invalid`, `stale_market_unknown`, and lineage-matched `needs_review`
findings such as `not_listed` are different.
The Admin page reads their sanitized entries from `manual_review_queue` and
shows the Token, market, date, reason, and primary-source URL hints in a
separate, read-only table. Those items never receive a retry button. The
operator must record source evidence and a disposition outside the automatic
collection queue before changing their lifecycle.

A lineage-matched `source_range_unavailable` issue does not enter that table:
it is a documented structural coverage limit, exposed as informational
`unsupported` until an entitled source is configured.

Persistent dispositions live in the revisioned, tracked
`data/curated/market_lifecycle_reviews.json` contract. A disposed revision must
bind one pending issue ID, market ID, Token, market type, and UTC issue date;
record the check actor/method/time; retain successful declared or primary
source URLs, normalized observations, and response SHA-256 hashes; and select
only the non-retryable `source_no_observation/no_candles` outcome. The quality
builder validates the entire file and contiguous revision history before use.
An invalid, ambiguous, cross-market-source, or mismatched record fails closed.

A matching disposition removes that exact `stale_market_unknown` item from the
manual queue and retains the complete review under the informational issue's
protected details. It never carries forward to tomorrow. A future missing date
still requires a new lineage-matched collector attempt or a new reviewed
revision. Corrections append the next contiguous revision; a `withdrawn`
latest revision disables the prior disposition without deleting history.

The initial reviewed records make two deliberately narrow findings for
2026-07-29:

- the configured GRT Arbitrum Uniswap V3 pool exists, while the declared
  GeckoTerminal source's newest daily candle is 2026-07-22 and its pool
  endpoint reports zero 24-hour volume and transactions;
- Upbit's official inventory contains `USDT-LDO`, not `KRW-LDO`, and its
  official `USDT-LDO` ticker reports the last trade on 2026-07-06 with zero
  24-hour accumulated volume.

Neither record marks a market delisted, invents a candle, or makes a claim
about a future UTC date.
For Upbit, the hints include the fact's configured quote market, the
collector's KRW/USDT fallback, and the official market inventory endpoint; a
missing KRW market therefore cannot be presented as evidence that an observed
USDT market is delisted.

Hard-invalid candidates never overwrite the last good daily publication. They
are persisted below `quality/rejected/<rejection-id>/`, and
`quality/rejected/latest.json` points at the newest evidence bundle. Admin
accepts that pointer only when it remains below the rejected directory, its
SHA-256 matches `report.json`, and both pointer and rejection schemas match.
The rejected candidate's hard-invalid items are merged into manual review with
`candidate_rejected=true` and their `rejection_id`; a malformed, traversing, or
tampered pointer is ignored rather than trusted.

## Refresh completion contract

An ordinary `refresh` is publication-aware. Before collection, Admin records
the current quality-report and database import identities. Exit status zero
from the collector is not success by itself. After collection:

1. `daily-latest.json` must be readable and expose a new import identity;
2. SQLite must be readable and expose that same identity;
3. the requested Token/date window must contain at least one successful row;
4. the same window must have no remaining `collection_failed`, retryable gap,
   or `hard_invalid` issue.

If a new publication is valid but still incomplete, the job is `partial` with
`publication_committed=true`. If identity is unchanged, unreadable, or
inconsistent, the job is `partial` and the publication is not certified.
Explicit structural outcomes such as unsupported markets, `not_listed`, or
`no_candles` are shown in the result's reason counts and are not relabelled as
successful rows.

### Latest TVL/depth snapshot completion

Snapshot refresh is a separate Fact contract, not a shortcut through the daily
SQLite postcheck. Before collection, the service reads and validates the exact
canonical Market/Fact row and records its snapshot ID, complete publication
SHA-256, observation time, status/reason, retryability, and publication
generation. After collection it rereads the uncached publication and requires:

1. the same requested Market and Fact identity;
2. a valid new snapshot ID and different publication bytes;
3. a producer-valid row and exact allowlisted status/reason pair;
4. `observed`, valid measured `partial`, or a terminal non-retryable
   `source_no_observation` / `unsupported` resolution.

An unchanged snapshot, an unrelated Market update, a retryable failure,
unknown status/reason pair, invalid publication, or `needs_review` is
unresolved even if the collector exits zero. `needs_review` is protected
manual work; it is never confirmed absence, unsupported capability, or refresh
success. For DEX depth, USD time-alignment warnings are evaluated only when a
measured band and a declared time-sensitive conversion exist. Unsupported or
failed `N/A` rows do not receive a synthetic temporal mismatch.

### One-shot MORPHO recovery gate

The bounded MORPHO Upbit action is an operator release step, not a retry loop.
It may run only after local tests, production-compatible checks, Quality v4
all-Token parity, deployment health, and browser verification pass. Immediately
before acting, refetch `cex:upbit:MORPHO/USDT` and its `depth` Fact. If it is no
longer `retryable=true`, record `no action`. If still retryable, submit at most
one bounded depth refresh; the exact snapshot postcondition above decides the
result. Record pre/post publication and generation identities plus depth and
execution status/reason changes. Do not repeat automatically after a failure.

## Add Token by contract

The Admin page supports DEX-first runtime onboarding:

1. choose one allowlisted chain and enter the smart-contract address;
2. validate the address and resolve Token identity through GeckoTerminal;
3. review the returned symbol, name, exact address, and strictly validated
   pools;
4. confirm `Add & collect`;
5. wait for DEX daily publication, post-publication SQLite verification, TVL,
   and protocol-dependent DEX depth collection.

The runtime identity is stored in:

```text
data/local/admin/token_registry.json
```

The registry is locked, validated, fsynced, and atomically replaced. The
version-controlled `config/tokens.csv` and `config/token_chains.csv` files are
not edited by the website. A Token remains `pending` until
`market_facts.sqlite3` contains at least one DEX daily row for its symbol. A
daily failure marks the record `failed`; daily success activates it. If TVL or
DEX depth then fails, the job is `partial` because the already-published daily
facts are not rolled back.

An on-chain address does not prove a centralized-exchange instrument.
Therefore a runtime Token starts with:

```text
CEX mapping = requires_manual_review
```

No `SYMBOL/USDT` pair is guessed and no CEX request is made until an operator
adds a separately reviewed mapping. Duplicate chain/address submissions are
idempotent; the same symbol on another contract is blocked for manual review.

For an isolated audit without publishing, run:

```bash
python3 scripts/fact_quality.py \
  --cex-csv data/processed/cex_exchange_volume_daily.csv \
  --dex-csv data/processed/dex_pool_volume_daily.csv \
  --output /tmp/fact-quality.json \
  --fail-on-hard
```

## Excluded data family

Funding Rate is fully excluded from this release. Operations must not create a
derivatives Market mapping, funding collection job, placeholder Fact, retry
queue entry, or dashboard status for it. Numeric CEX account-tier fees, gas,
transfer costs, and net-arbitrage outputs also remain outside this quality
hardening procedure.

## Security boundary

- Passwords use PBKDF2-SHA256 and are stored only as an environment verifier.
- Sessions are random, server-side, expire after eight hours, and use an
  HttpOnly SameSite cookie.
- Data-changing requests require a session-specific CSRF token.
- Repeated login failures are rate limited.
- The entire surface is opt-in through `ADMIN_ENABLED=true`; a valid generated
  verifier is also required in login mode.
- Open mode additionally requires `ADMIN_ALLOW_OPEN_LOCAL=true` and a loopback
  application bind.
- Pipeline commands use fixed argument arrays without a shell.
- Production requires HTTPS and `ADMIN_COOKIE_SECURE=true`.
- The data directory must be writable only by the deployment service account.

The public Nginx example in `deploy/nginx/cex-dex-dashboard.conf.in` blocks the
administrator surface. Operate it through an SSH tunnel, VPN-restricted
hostname, or a separately reviewed proxy policy rather than exposing it on the
public dashboard hostname.

The administrator page does not accept arbitrary historical date input.
Historical collection is available only for an exact, bounded window emitted
under `backfill_windows_by_token` by the currently published quality report.
Any broader source-specific backfill still requires a separately tested
collector change rather than bypassing this allowlist.

For a reviewed sequential batch of those same exact historical windows, use
`scripts/run_exact_backfill.py` as documented in
`docs/collection-operations.md`. That internal runner shares the collection
lock, narrows CEX/DEX source scope from the report's `market_types`, skips
TVL/depth work, and stops if the committed publication or selected issue set
does not make verifiable progress. It does not create a broader date-input
surface.
