# Event Facts contract

## Product boundary

Event Facts are a source-backed timeline, not an event study. The first
taxonomy is deliberately limited to:

| `event_type` | `event_subtype` | Effective-time meaning |
| --- | --- | --- |
| `unlock` | `scheduled_release` | The dated release in an official lock-up schedule |
| `airdrop` | `claim_start` | The time users can first claim the distribution |
| `cex_listing` | `spot_trading_start` | The time spot trading opens, not the deposit or auction time |

The fact layer does not calculate or label price returns, volume impact,
sentiment, importance, abnormal performance, or causality. Those require a
separate event-study contract and are not fields in the schema or API helper.

## Verified 30-Token coverage release

The committed curated input contains 45 immutable revisions representing 44
latest facts, backed only by official public sources. Every one of the 30
configured Tokens has at least one published Event Fact:

- 15 monthly STRK scheduled-release dates from 2026-01-15 through 2027-03-15.
  The official Starknet schedule says each date is **up to** 127 million STRK
  and **up to** 1.27% of supply. January through July 2026 are recorded as
  `occurred` from the page's statement that releases in the interval "have
  been and will be" unlocked; the remaining eight future dates are
  `scheduled`. These are schedule facts, not onchain transfer confirmations.
- The EIGEN Season 1 Phase 1 claim opening on 2024-05-10. The 6.05% value is
  the source-reported share ready to claim that day. The source's approximate
  full-season token total is not reassigned to this one opening fact.
- The OP Airdrop #1 claim opening on 2022-05-31. The official project page
  confirms that claims were live, but does not assign a supported amount or
  supply percentage to this one fact, so those fields remain null.
- 27 CEX spot-trading-start facts, including the existing OKX MORPHO/USDT fact
  and one catalog-compatible listing fact for each of the other 26 Tokens.
  Six official pages directly confirm the market as started or available and
  therefore support `occurred`: UNI, AAVE, CRV, COMP, SNX, and GRT. The other
  21 pages are pre-opening announcements, so they remain `scheduled` even
  though their announced times are now in the past. Each row identifies the
  exact exchange, pair, and `market_id`; deposit, call-auction, withdrawal,
  Launchpool, and unrelated quote-pair times are excluded.

The latest release contains 15 `occurred` and 29 `scheduled` facts. MORPHO has
two revisions: revision 1 is retained byte-for-byte, while revision 2 corrects
the latest lifecycle from `occurred` to `scheduled` because the cited page is
a pre-opening announcement rather than a post-opening confirmation.

The 17 original revisions were source-checked at 2026-07-29 07:33 UTC. The 27
coverage additions were checked at 2026-07-29 09:37:30 UTC, and the MORPHO
lifecycle correction was rechecked at 09:54:30 UTC. The release uses 29
distinct official project or exchange pages because the JTO and BONK facts
share one announcement and the 15 STRK schedule rows share one source.

Important source decisions are preserved in evidence records:

- ARB uses the final Binance time update as its source URL and links the
  original Binance listing page to bind that update to `ARB/USDT`; the
  superseded 17:00 UTC time is not published.
- OP uses the official Optimism claim-opening page rather than treating an
  inaccessible exchange announcement as evidence.
- RAY uses the explicit 10:00 UTC spot-opening time in the official
  announcement. A conflicting current instruments-API `listTime` is recorded
  as a limitation and is not substituted.
- CAKE uses the later CAKE/USDT pair start because that is the market identity
  present in this project's catalog; it is not labeled as CAKE's first-ever
  exchange listing.

Coverage means **at least one verified timeline fact per configured Token**.
It does not mean complete historical event coverage for any Token. No
Tokenomist, DefiLlama-Pro, paywalled event feed, secondary calendar, search
snippet, or inferred first-observation listing is used.

## Curated and normalized storage

| Path | Role |
| --- | --- |
| `data/curated/event_facts.csv` | Versioned human-reviewed revisions |
| `data/evidence/events/*.json` | Small source-check records with supported facts and limitations |
| `data/templates/event_facts_curated.csv` | Header-only authoring template |
| `data/schema/event_facts_v1.sql` | Indexed revision table and latest-revision view |
| `data/processed/events/` | Ignored staging bundles |
| `data/local/events/` | Ignored published bundles used by the website |

Each immutable bundle contains:

- `event_fact_revisions.csv`: all revisions;
- `event_facts_latest.csv`: highest revision for every stable `event_id`;
- `event_facts.sqlite3`: the same rows plus `event_facts_latest` view;
- `manifest.json`: counts and SHA-256 hashes.

`latest.json` is replaced atomically only after every row, source record, CSV,
SQLite integrity check, and manifest has passed. It points to one immutable
bundle ID. The dashboard helper verifies the pointer, manifest, and all three
file hashes before querying the database.

## Timing and precision

`announced_at`, `effective_at`, and `source_published_at` never receive an
invented midnight. The paired precision field controls the accepted form:

| Precision | Accepted form | API date interval |
| --- | --- | --- |
| `second` | `YYYY-MM-DDTHH:MM:SSZ` or an offset equivalent | One UTC date |
| `minute` | `YYYY-MM-DDTHH:MMZ` or an offset equivalent | One UTC date |
| `day` | `YYYY-MM-DD` | One UTC date |
| `month` | `YYYY-MM` | First through last day of that month |

The builder normalizes offset timestamps to UTC. An occurred event cannot use
month-only precision. Date-window filtering uses interval overlap, so a
month-precision fact is not silently pinned to the first day in the public
payload.

## Lifecycle

- `scheduled`: an official source publishes a schedule, but occurrence has not
  been confirmed. It may remain scheduled after its effective date; elapsed
  time alone is not evidence that the event occurred.
- `occurred`: the source supports that the effective event occurred.
- `postponed`: the source says the represented scheduled time was postponed.
- `cancelled`: the source says the represented scheduled time was cancelled.
- `superseded`: a distinct newer fact replaces this logical event rather than
  merely correcting one of its fields.

Lifecycle answers what happened to the event. It is separate from
`evidence_status`, which answers how the row is supported:

- `primary_confirmed`: one official primary source directly supports the row;
- `cross_checked`: the source-check record documents consistent independent
  sources;
- `onchain_observed`: a transaction source and related transaction hash support
  direct chain observation.

Postponement, cancellation, corrected time, or corrected size is a new
revision of the same `event_id`; the old row is retained. Revisions must start
at 1, remain contiguous, increase `recorded_at_utc`, preserve Token and
taxonomy identity, and contain a material change. Revision 1 uses
`revision_reason=initial`.

Before moving `latest.json`, the builder reads the prior bundle and requires
every published `(event_id, revision)` row to remain present and byte-for-byte
equivalent at the normalized field level. New input may only add a new event
starting at revision 1 or the next contiguous revision. Deleting history or
editing revision 1 in place fails closed.

## Source and record evidence

Every row requires:

- a public HTTPS `source_url`;
- an allowed official/onchain `source_kind`;
- an independent `evidence_status`;
- `source_checked_at_utc`;
- a versioned JSON `source_record_file`;
- a `record_locator` naming the supported fact inside that record;
- the builder-computed `record_sha256`.

`record_sha256` authenticates the project's checked source record. It is
intentionally **not** called a raw-response hash and does not prove that a
third-party webpage will never change. A later source check writes a new
source-record file and the next event revision; it never edits the record used
by an already published revision. A provenance-only recheck may retain all
event fields because its new check time and record hash are themselves an
auditable revision change.
The validator resolves each dot-delimited `record_locator` inside the JSON and
rejects missing or empty paths; a decorative locator string is not accepted.
New source-check fact objects may declare `supported_lifecycle`; when present,
the builder rejects a curated lifecycle that does not match it. The 27 facts
added for 30-Token coverage all declare this field, preventing a pre-opening
announcement from being silently promoted to `occurred`.
Published source records are immutable in practice because changing their
content changes `record_sha256` and triggers the append-only revision gate.

CEX listings require an official-exchange source plus exact `venue`,
`market_symbol`, and catalog-compatible `market_id`. The builder derives the
allowed IDs from each Token's configured `cex_symbol`, `primary_cex`, and
`secondary_cex`; a correctly formatted but unconfigured market fails
publication. An exchange appearing in daily data for the first time is not
enough to create a listing fact.

## Size and null rules

`amount_token`, `amount_usd`, and `percent_of_supply` are optional exact
base-10 strings. Blank input becomes JSON `null`; it never becomes zero.

- `size_relation` is required when any size is present and is one of `exact`,
  `up_to`, or `approximately`.
- `amount_usd` is accepted only with
  `amount_usd_basis=source_reported`. This first contract does not multiply a
  token amount by a later or current market price.
- `percent_of_supply` must be positive and no greater than 100.
- CEX listings do not accept size fields.

## Build and publish workflow

1. Verify the official page and update its source-check JSON.
2. Add revision rows to `data/curated/event_facts.csv`; leave unsupported
   values blank.
3. Build the review bundle:

   ```bash
   python3 scripts/event_facts.py
   ```

4. Review `manifest.json`, both CSVs, and the cited official page.
5. Publish the exact validated facts:

   ```bash
   python3 scripts/event_facts.py --publish-local
   ```

6. The server loads `data/local/events` with
   `dashboard.event_facts.load_latest_event_rows()` and constructs filtered
   responses with `build_event_payload()`.
7. `scripts/check_dashboard_release.py` compares the Event covered-token
   inventory with the current Screener catalog and requests every Token scope;
   any uncovered or empty Token blocks release.

The helper returns `event_facts_api/v2`, lifecycle and evidence status,
source/revision lineage, explicit date bounds, exact null-preserving size
values, and configured/covered/uncovered Token inventory. The dashboard
exposes this through
`GET /api/markets/events?token=&start=&end=&lifecycle=&clock_state=` and the
Token workspace Events page, whose summary displays the release coverage
ratio. `clock_state` is independently filterable as `past`, `future`, or
`current_window`; it never changes the source-backed lifecycle. The response
publishes `clock_as_of_utc`, the same per-row `as_of_utc`, a calculation
`basis`, and clock-state counts for the returned rows.
When `clock_state` is filtered, the counts describe only the returned rows;
an available empty intersection therefore has `event_count=0`, an empty
`events` array, and empty counts.
Minute and second precision use the exact published instant. Day and month
precision use the complete UTC calendar interval, so the API does not invent
a midnight event time. A scheduled event whose effective interval has passed
remains scheduled and carries the explicit notice
human-readable “Effective time passed; occurrence unconfirmed” disclosure.
Event responses bypass the ordinary minute-level serialized-response cache,
so an exact event cannot remain Future after its published instant. Compare may
overlay matching event dates as temporal context only. Event bundle files
participate in the server source signature so a validated pointer change
invalidates the cached generation; an unavailable Event publication remains
distinct from an available feed with zero matching rows.

## Update rule

This is a curated fact feed, so freshness is source-check freshness rather than
an hourly market-data SLA. Before a presentation or production release:

1. re-open every source used by visible facts;
2. write a new source-record file and record the new check time;
3. append the next revision, changing event fields only when the source
   supports the change;
4. rebuild and verify bundle checksums and counts;
5. never overwrite a healthy published bundle with empty or invalid input.

Token-presence coverage is now 30/30. Future additions should deepen each
Token's verified event history and periodically recheck sources, not claim that
one fact is a complete timeline. They must not import the old research table
as production data: that table mixed official vesting rules with secondary
calendars and contained no airdrop or listing coverage in its final 67-row
output.
