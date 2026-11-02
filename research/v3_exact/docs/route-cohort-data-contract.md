# Route Cohort Data Contract

## Scope

This contract defines pure route identity and timing evidence. It does not
calculate costs, rank routes, or infer an opportunity from a shared snapshot
identifier. A `snapshot_id` is lineage only; timing is measured from the two
actual `state_observed_at` values.

## Complete opportunity publication

The private core and the public opportunity generation are separate immutable
namespaces. `routes/core/latest.json` selects only a validated
`route_cohort_core/v1` bundle. `routes/latest.json` selects only a validated
`route_opportunity/v1` bundle; the public loader rejects a core pointer and an
incomplete directory. Finalization never adds files to, rewrites, or otherwise
uses the private core directory as a public bundle.

`build_complete_route_bundle()` begins by resolving the actual private pointer
with `load_latest_route_cohort()`. It pins the pointer bytes, core manifest,
cohort, descriptors, and the exact five-notional grid. For each observed leg it
then resolves only
`<raw-root>/<raw_evidence_run_id>/accepted/<sha256(market_id)>/response.json`
through descriptor-relative non-symlink reads and requires its physical hash
to equal the published core leg. Caller-supplied core hashes are checked only
as lineage claims; they never establish membership.

Strict publication is currently bounded to prepositioned CEX-to-CEX routes on
two distinct non-Upbit venues. The finalizer replays the actual Binance or
Bybit response with the declared source request and parser, typed market rules,
the owner-only fee profile, exact fee asset and rounding semantics, the typed
USD conversion member, and the owner-only inventory profile. It reruns the
quantity and route-mode classifiers, calls the private Task 6 attestation
issuer only after those checks, and rebuilds the opportunity. A missing typed
adapter or source member leaves the already classified row non-strict; DEX,
gas, router, tax, MEV, atomic, transfer, same-venue, Upbit, and other unsupported
strict replay paths are not promoted. `rebalance_required` remains an explicit
route mode and round-trips unchanged, but it is not silently remapped into a
strict prepositioned route.

One complete bundle lives at `routes/bundles/<route_cohort_id>` and contains
exactly:

```text
route_legs.csv
cost_components.csv
route_opportunities.csv
route_cohort.sqlite3
manifest.json
```

Each route has exactly `[1000, 5000, 10000, 50000, 100000]` USD scenarios.
Every opportunity binds one core cohort and manifest, two published legs, its
canonical route/notional identity, an exact topology-dependent component set,
and a recomputed evidence and publication-attestation hash. Extra, missing,
duplicate, transplanted, or orphan component rows fail closed. The manifest
records physical and logical hashes, exact row counts, strict/research/
unavailable counts, both cost-completeness count pairs, the raw/quote/cost/
classified/fee/inventory/typed-source generations, and named adapter versions.
CSV projections, SQLite columns/keys/foreign keys/indexes/metadata, and the
manifest are fully reread and required to agree exactly. Public artifacts are
recursively checked for paths, secrets, credentials, unsafe endpoints, and
other private evidence.

`publish_complete_route_bundle()` writes the five members into a hidden
same-filesystem directory, fsyncs them, validates by full reread, performs an
atomic no-replace directory rename, fsyncs the parent, and rereads the final
directory. It holds the public routes lock across staging and pointer commit,
rechecks the private core through one shared-locked descriptor snapshot, and
only then atomically replaces the public pointer. Failures before commit keep
the old pointer byte-for-byte. If the attempted pointer cannot be distinguished
from a concurrent writer after an error, finalization reports commit uncertainty
and never overwrites the other writer. A retry may select an already complete,
byte-identical orphan left after a pointer failure; it does not mutate that
bundle. `finalize_route_opportunity_bundle()` only forwards already collected
Task 1-6 artifacts to this boundary and never recollects market data.

## Deterministic bounded route universe

`scripts.route_universe` is a pure, collection-free input-selection boundary.
It consumes canonical catalog rows plus depth, fixed-notional execution,
CEX selected-window volume, DEX 24-hour volume, and DEX TVL rows. It neither
fetches facts nor writes or publishes a bundle.

`select_route_legs()` returns no more than three `cex` and three `dex` legs
per Token. A selected catalog row must have a canonical `market_id`, its
declared `market_type` prefix, a non-empty Token symbol, a timezone-aware
`observed_at`, a non-withheld lifecycle, a supported adapter, and one observed
depth row with a valid timezone-aware `state_observed_at`, positive
`total_depth_100bps_usd`, and both positive directional 100-bps values. CEX
requires `bid_depth_100bps_usd` and `ask_depth_100bps_usd`; DEX requires
`buy_depth_100bps_usd` and `sell_depth_100bps_usd`. A total-depth value never
proves a two-sided book. Lifecycle-withheld/inactive identities, missing,
one-sided, or failed books, unsupported adapters, and invalid source-time rows
therefore do not enter strict route generation.

Catalog identities are a fail-closed input boundary: any duplicate non-empty
canonical `market_id`, even when the duplicate rows conflict or would
otherwise be unusable, raises `ValueError("duplicate canonical market ID")`.
The selector never applies last-write-wins behavior to catalog rows.

`execution_capability_by_market()` derives `proved` capability only when both
`buy_token` and `sell_token` have current `observed` execution rows. Its
`proved_execution_capacity_usd` is the lower of their two largest requested
notionals. Current `partial` or `failed` rows prove only `supported`; a current
`unsupported` row proves `unsupported`. Invalid execution timestamps prove
nothing.

For every usable leg, `selection_inputs` retains exactly these source-derived
priority values (decimal text or JSON `null`):

```text
execution_capability
proved_execution_capacity_usd
observed_100bps_depth_usd
cex_selected_window_usd
dex_24h_usd
dex_tvl_usd
```

The descending deterministic priority key is:

```text
execution capability
→ proved execution capacity (or observed 100-bps depth when capacity is absent)
→ CEX selected-window USD volume
→ DEX 24-hour USD volume
→ DEX TVL
→ canonical market_id
```

Each selected leg records its one-based `selection_rank`, canonical ID,
selection window, and `candidate_source_generation`. Duplicate source rows are
resolved only by valid source timestamp text and numeric value, so input order
cannot affect the result.

`build_route_universe()` emits the schema `route_universe/v1`, the five
requested USD notionals `[1000, 5000, 10000, 50000, 100000]`, selected legs,
and every directional same-Token pair of distinct selected legs. CEX-involving
pairs use `prepositioned_inventory`; same-chain DEX pairs use
`atomic_onchain` as an unvalidated candidate; and cross-chain DEX pairs use
`research_only` with `route_class = research_only` and
`settlement_reason = unsupported_cross_chain_settlement`. The route ID remains
directional and is generated by `canonical_route_id`.

`route_universe_sha256()` hashes canonical JSON encoded as UTF-8 with sorted
keys and compact separators. Shuffling any input iterable must not change
selected legs, routes, canonical JSON bytes, or that SHA-256.

## Concurrent cohort collection

`scripts.collect_route_cohort.collect_unique_route_legs()` resolves the unique
canonical market IDs referenced by a route universe in lexical order.
`collect_route_cohort()` schedules each such leg once, with a process-wide
`max_workers` cap, an active-work cap of `cex_workers_per_venue` for each CEX
venue, and an active-work cap of `dex_workers_per_chain` for each DEX chain.
Completion order is not evidence: normalized leg rows, route timing rows, and
the cohort fingerprint are sorted by canonical identity.

Before any pool work on a chain, a DEX cohort requires one fixed-block resolver
for that chain. Resolver and DEX jobs share the fair scheduler with CEX
collection, but all active non-CEX work is capped at `max_workers - 1` while
any CEX work remains. With one worker, every CEX leg becomes terminal before a
resolver starts. Thus a slow resolver or pool call cannot occupy the slot
reserved for same-venue CEX legs.

Production collection requires Unix `fork` and fails closed when that process
isolation is unavailable. Each resolver or collector runs in its own inherited
child process and returns through a one-way pipe and standard `Future`.
Shutdown sends TERM, joins, escalates to KILL when necessary, joins again, and
closes every pipe. The parent main thread polls those pipes directly; the
process executor creates no monitor threads. It also rejects a caller that is
already multithreaded before source reads or raw creation, rather than risking
an unsafe fork. Consequently, an uncooperative transport is killable and
repeated deadline calls leave neither child processes nor route-cohort threads
behind. In-process thread executors are an explicit test-only injection for
shared fake clocks and state.

Every pool collector on a resolved chain receives exactly its common positive,
non-boolean integer `fixed_block_number` and non-empty canonical UTC
`fixed_block_timestamp`. Malformed lineage and a timestamp later than the
collection deadline reject before pool calls; lineage later than the actual
`collection_completed_at` is rejected again before raw promotion. Valid fixed
block lineage is retained even when its pool leg becomes terminal. A resolver
or leg `CollectionDeadlineExceeded` always becomes a retained terminal leg row
with `status = deadline_exceeded`, `available = false`, and
`reason_code = route_deadline_exceeded`; it is never mislabeled as a generic
failure or omitted. Consequently, only routes containing that leg classify as
deadline-exceeded.

Collection is bound to two separate generations. The universe's original
`candidate_source_generation` is preserved as candidate lineage. The CLI also
computes `collection_input_generation` as the canonical SHA-256 of the entire
validated route universe, the exact requested token filter, and the exact
authoritative inventory rows for the selected legs. It recomputes that complete
input before work and after work; a mismatch raises
`ValueError("collection input generation changed")`. Direct callers must supply
both the expected value and a reader that can reproduce it. Thus changing an
inventory fact without changing `candidate_source_generation` still rejects
the cohort.

The Task 4 command reads `<data-dir>/route_universe.json` and accepts
`--data-dir`, `--start`, `--end`, `--tokens`, `--deadline-seconds`,
`--max-workers`, `--cex-workers-per-venue`, and `--dex-workers-per-chain`.
`--start` and `--end`, when supplied, must equal the corresponding values in
the universe's `selection_window`; they are not informational arguments.
Every route must supply the exact non-empty ID produced by
`canonical_route_id`, and each selected leg's declared `market_type` must agree
with its `market_id` prefix. A CEX leg ID is exactly
`cex:<venue>:<BASE>/<QUOTE>`: venue is lower-case ASCII identifier text and the
two symbols are upper-case ASCII identifier text. A DEX leg ID is exactly
`dex:<chain>:<dex>:<pool>:<TOKEN>`: chain and DEX are lower-case ASCII
identifier text, Token is upper-case ASCII identifier text, and pool matches
`[A-Za-z0-9][A-Za-z0-9._-]{0,255}`. A pool beginning `0x` must be entirely
lower-case. Every supplied selected-leg identity field must itself be a
non-empty string without leading or trailing whitespace, must agree with the
ID after its declared case normalization, and `pool_address` must agree
exactly. A missing collector identity cannot make a malformed requested ID
valid. These identity failures occur before generation reads or raw directory
creation. Malformed or duplicate routes, invalid token
filters, non-finite/non-positive deadlines, and invalid worker limits fail
before collection. `--dry-run`
performs the same read-only full-universe, authoritative-inventory, and stable
`collection_input_generation` validation as live mode. It performs no network
calls, creates no raw artifacts, and publishes nothing. The Task 4 CLI still
fails explicitly on `--publish`; Task 5 exposes the separate immutable-core
publisher, while later orchestration owns wiring the two boundaries together.

Live CLI collection resolves every selected canonical CEX ID against the
authoritative CEX catalog and every selected canonical DEX ID against the TVL
pool inventory. Authoritative identity fields win during binding; any conflict
with a selected universe identity, missing identity, or duplicate inventory ID
fails closed. Collector rows are checked against the requested canonical
identity, including any partial identity fields they return. A returned DEX
`pool_address` is always compared as exact case-sensitive text, including for
non-EVM identifiers; only the requested canonical-ID boundary applies the
lower-case `0x` rule. The CEX and DEX
adapters invoke the Task 3 one-leg primitives using only their declared
arguments; the DEX adapter receives the common fixed block number and block
timestamp, never a target-time pseudo-argument.

Each invocation owns one collision-safe raw evidence directory below
`<raw-root>/<raw_evidence_run_id>/`. Caller-supplied snapshot IDs are restricted
to a bounded path-safe identifier and an existing run directory is never
reused. Direct API callers must supply `raw_root`; there is no repository-local
fallback. Existing and broken symlink roots are rejected before source reads or
artifact creation. Default IDs combine canonical UTC wall time with a UUID, so
two calls in the same clock tick remain distinct. Market identities are
represented only by SHA-256 directory names, never interpolated into paths.
Workers write under
`staging/`. An `observed` or `partial` row, and any row claiming
`raw_response_sha256`, requires a regular `response.json`. The parent hashes
the exact file; a claimed hash must match, while an omitted hash is filled with
the computed value and bound into the cohort. The caller-controlled raw-root
ancestry may not contain symlinks. The known macOS `/tmp` and `/var` system
aliases are first canonicalized to `/private/tmp` and `/private/var`; no other
ancestor alias is accepted. Root, run, staging, accepted, and per-market stage
directories must be real directories and remain bound to their original
device/inode identities. Before promotion, `response.json` is checked with
`lstat`, opened without following a final symlink where the platform supports
it, and its open descriptor and final path are rechecked as the same regular
file. The run, staging, accepted, and promoted stage identities are then bound
to already-open directory descriptors. Promotion is a descriptor-relative,
atomic no-replace rename (`RENAME_EXCL` on Darwin or `RENAME_NOREPLACE` on
Linux); a platform without that primitive fails closed. It never resolves a
new staging or accepted parent path after the final guard. Missing, mismatched,
symlinked, escaped, or directory-swapped evidence
becomes a terminal leg and never moves to `accepted/`. Only a completed,
identity-valid, within-deadline, evidence-valid observation moves to
`accepted/`, and only after the final input-generation check. Immediately
after promotion, the accepted entry, its still-open directory, response
identity, and response hash are revalidated through the same descriptors. A
failed check is rolled back through those descriptors without following a
swapped path. If an untrusted same-name staging entry blocks rollback, Darwin
`RENAME_SWAP` or Linux `RENAME_EXCHANGE` atomically returns the evidence inode
to its identity-bound staging name; the displaced entry is then recoverably
moved to a unique staging quarantine name. The caller verifies the rollback
result, the still-open stage inode, the real nonsymlink staging entry, and the
absence of that entry from accepted. A successful rollback preserves the
original per-market terminal failure. If the rollback exchange, quarantine, or
final-state verification fails, the collector closes the entry and raw-run
descriptors and then hard-fails the entire collection with
`raw evidence rollback could not be verified`; it returns no terminal cohort or
other publishable result. A swapped
`run/accepted` symlink is unlinked relative to the verified run descriptor and
is never used as a destination. A worker that
returns or writes after the deadline remains in staging and cannot mutate
accepted evidence.

The returned `route_cohort_collection/v1` declares a deterministic
`route_cohort_id`, `raw_evidence_run_id`, canonical `target_observed_at`,
actual wall-clock `collection_started_at`, active monotonic-deadline-derived
`collection_deadline_at`, truthful canonical `collection_completed_at`,
`skew_sla_seconds = "60"`,
`route_age_sla_seconds = "120"`, candidate generation, selected-window/notional
lineage, complete collection-input generation/source state, normalized legs,
and route timing rows. Every route timing candidate and returned route row uses
that completion instant as `validated_at`; a leg state timestamp after it
classifies as `invalid_state_timestamp`. Target timestamps with a numeric
offset are canonicalized to UTC. Separate invocations may intentionally have
different start/deadline times and run IDs; with those invocation inputs held
fixed, completion order does not affect normalized rows or fingerprints. A
fixed DEX observation must echo its resolved block number and timestamp; a mismatch becomes the retained
terminal reason `fixed_block_lineage_mismatch`. Leg projections exclude raw
paths, exception traces, and credential-like fields recursively through nested
mappings, lists, and tuples before the future bundle boundary. HTTP(S)
endpoints retain only scheme, host, port, and path; userinfo, query, and
fragment are removed, while hierarchical or opaque non-HTTP credential-bearing
URLs and malformed opaque HTTP(S) forms are dropped. Path objects, path-like
keys, absolute, home-relative, UNC, Windows-drive, and `file:` path strings,
plus any string whose slash- or backslash-delimited segments contain `.` or
`..`, are dropped. Ordinary symbols such as `UNI/USDT` and canonical market
IDs remain valid. Non-finite numbers, custom objects, and other non-JSON values
are also dropped. A final canonical-JSON and recursive unsafe-evidence scan
fails closed before returning a projected leg.
The cohort ID and
fingerprint hash all of those declared logical fields, including the canonical
collection timestamps and SLA/selection lineage, so a later bundle can detect
metadata conflicts without consulting mutable sources.

Live `main()` returns exactly this fingerprint-bound mapping. It does not append
`dry_run`, absolute universe paths, or any other post-fingerprint field. Dry-run
is a separate validation result and retains `dry_run = true`.

## Immutable core publication

`scripts.route_publication` owns the private, source-and-timing-only core
publication boundary. For the default production root, one normalized cohort
is published below its content-derived identity with exactly five files:

```text
data/local/routes/core/
├── bundles/
│   └── <route_cohort_id>/
│       ├── manifest.json
│       ├── route_candidates.csv
│       ├── route_cohort.sqlite3
│       ├── route_legs.csv
│       └── route_timing.csv
└── latest.json
```

No extra or missing bundle entry is valid. Candidate, leg, and timing rows are
normalized into canonical identity order before encoding. CSV headers and
projections are fixed, each `row_json` value is canonical JSON, and SQLite is
built from the same normalized mapping with fixed tables, indexes, pragmas,
and row order. Consequently, shuffling input rows cannot change any of the
five output files, the manifest hash, or the pointer payload for the same
logical cohort. An existing `<route_cohort_id>` directory is immutable and is
never overwritten or reused.

The manifest identifies `route_cohort_core/v1` and records the cohort ID and
fingerprint, both source generations, raw-evidence run, collection timestamps,
SLAs, selection window, requested notionals, exact candidate/leg/timing counts,
timing-status counts, and observation bounds. For each of the four non-manifest
artifacts it records schema, row count, a physical `sha256` of the exact bytes,
and a `logical_sha256`. The three CSV logical hashes bind their schema and
decoded logical rows; the SQLite logical hash binds its schema and the complete
normalized cohort. The manifest does not self-hash. Instead, the private
pointer carries the physical SHA-256 of the exact `manifest.json` bytes.

Validation requires exact parity, not merely compatible counts. Every CSV
projection must reconstruct its `row_json`; CSV candidate, leg, and timing
inventories must equal their corresponding SQLite inventories; and all three
must equal the respective `routes`, `legs`, and `route_rows` members of the
cohort mapping stored in SQLite metadata. The validator
recomputes every logical and physical hash and reconstructs the complete
manifest for exact equality. SQLite must also retain its exact column types,
primary keys, `NOT NULL` constraints, foreign key, indexes, `WITHOUT ROWID`
tables, application ID, user version, page size, foreign-key check, and
integrity result. A well-formed manifest cannot legitimize divergent CSV or
SQLite content.

Publication revalidates the Task 4 lineage rather than trusting a serialized
row. Top-level collection timestamps, every non-empty leg state timestamp,
route `validated_at`, and DEX fixed-block timestamps must be canonical UTC text
ending in `Z`; equal instants written with numeric offsets are rejected at this
boundary. Collection completion cannot precede collection start, the deadline
cannot precede the start, and the target cannot exceed the deadline. Each leg
state timestamp is no later than collection completion, and every route
`validated_at` equals that completion timestamp before timing classification
is recomputed. A DEX fixed-block number and timestamp are either both absent or
both present; observed or partial DEX legs require them, CEX legs cannot carry
them, and all DEX legs on one chain either have no lineage or share one exact
positive block number and timestamp. The fixed-block timestamp cannot exceed
the earlier of collection completion and deadline.

Expected source failures remain data, not bundle corruption. A structurally
valid cohort containing `unsupported`, `failed`, or `deadline_exceeded` legs
and `unavailable` route timing may be published, including a retained
`raw_evidence_path_unsafe` terminal reason. Status, availability, reason, raw
hash, fixed-block lineage, and recomputed timing must still agree exactly.
Thus a terminal route does not suppress healthy routes or invalidate the core,
while malformed identity, unsafe evidence, enum drift, incomplete pairs, or
inconsistent classification fails the whole publication.

The filesystem transaction is descriptor-relative and fail closed. The core,
bundle root, hidden staging directory, final directory, and files are bound to
opened directory or file descriptors and stable device/inode identities.
Caller-controlled symlink ancestry is rejected; final components are opened
without following symlinks where supported. Files are created exclusively,
written and fsynced through the staging directory descriptor. The complete
stage is validated and fsynced, then moved on the same filesystem with an
atomic no-replace directory rename (`RENAME_EXCL` on Darwin or
`RENAME_NOREPLACE` on Linux). A platform without a supported no-replace
primitive fails closed.

After the no-replace rename, publication verifies the final directory entry,
fsyncs the bundle root, and performs a complete validation of the final bundle
before touching the pointer. Validation starts from the exact five-name
inventory, opens every file once relative to the bundle descriptor, and keeps
all five descriptors open. After all CSV, SQLite, manifest, lineage, and hash
checks, it rechecks the exact inventory and every directory entry, rereads the
same open descriptors, and requires unchanged bytes, hashes, inode identity,
size, mode, ownership, link count, and stable timestamps/flags. It then checks
the inventory and entries once more and verifies the final bundle directory
identity. A directory or file swap at any point therefore prevents pointer
publication.

The only pointer this module creates or replaces is the private core pointer
`data/local/routes/core/latest.json` (or `<core_root>/latest.json` for an
explicit test root). Its exact schema binds `route_cohort_id`,
`bundle_stage = route_cohort_core/v1`, and the physical manifest hash. Pointer
writers take an advisory exclusive lock on the verified core directory
descriptor, create and fsync a private temporary file, and call descriptor-
relative `os.replace` for `latest.json`.

POSIX rename durability defines the pointer failure contract. Before
`os.replace` succeeds, a validation, temporary-write, lock-acquisition, or
replace failure leaves an existing pointer A exactly intact. Once
`os.replace` succeeds, B is visible and publication never compensates by
restoring A or unlinking B. A subsequent directory-fsync or diagnostic failure
raises `pointer state uncertain`; it leaves whichever valid B or concurrent C
is currently present. After a successful directory fsync, publication rereads
the pointer and returns only if both its exact bytes and stable file metadata
still match the owned B snapshot. A concurrent C instead causes
`pointer state uncertain` and remains untouched. A lock-release failure after
an otherwise successful commit is reported explicitly; when another pointer
error is already active, lock release cannot mask that primary error, and the
outer descriptor close releases the advisory lock.

`load_latest_route_cohort()` resolves only this private pointer. It validates
the pointer schema and path-safe cohort ID, requires the exact manifest hash,
performs the full bundle validation above, then verifies that the pointer,
bundle root, and core root did not change during the read. It never constructs
a stable-looking cohort from mutable CSV or SQLite files independently.

The public complete pointer `data/local/routes/latest.json` and public
`data/local/routes/bundles/` are not core artifacts. Task 5 neither creates nor
replaces them, does not invent empty cost or opportunity files, and does not
make the API consume a core-only cohort. Route-core participation in a later
release remains optional by default: an absent private core pointer makes no
route-cohort claim and does not alter public output. If a release opts into
route-core validation, any present pointer must pass the complete contract; an
explicit require-route-cohort mode must fail when it is absent or invalid.
Normal terminal legs and unavailable route timing remain valid release input
and are not, by themselves, a release failure.

## Exact timestamp arithmetic

`scripts.timestamp_contract.exact_rfc3339_epoch_seconds(value)` accepts a
timezone-aware RFC 3339 timestamp with an optional arbitrary-length fractional
second and returns a `Decimal` epoch value. `Z` and numeric timezone offsets
refer to the same UTC instant. The existing microsecond parser remains
unchanged.

`exact_timestamp_skew_seconds(left, right)` returns the absolute difference of
those values as a `Decimal`. No float conversion is permitted. For example,
the skew from `2026-08-01T12:00:00.000000000Z` to
`2026-08-01T12:01:00.000000000Z` is exactly `Decimal("60.000000000")` and
passes a 60-second SLA; `60.000000001` does not.

Timestamp arithmetic uses an operation-local Decimal precision derived from
the inputs, not the process-wide Decimal context. Arbitrarily long RFC 3339
fractions remain exact: `60.0000000000000000001` remains distinct from 60 and
does not pass the SLA.

## Candidate and leg identity

A route candidate must provide canonical strings for `token_symbol`,
`buy_market_id`, `sell_market_id`, and `route_mode`; identity is never coerced
with `str()`. Token symbols are upper-case identifier text; market IDs have no
surrounding whitespace and begin `cex:` or `dex:`; route modes are lower-case
underscore identifiers. Missing, empty, non-string, or noncanonical values
raise `ValueError("route candidate identity is invalid")`.
`canonical_route_id(candidate)` returns:

```text
route:{token_symbol}:{buy_market_id}->{sell_market_id}:{route_mode}
```

The arrow is directional: reversing the buy and sell market IDs produces a
different identifier. Identical buy and sell canonical market IDs are rejected
with `ValueError("route candidate legs must be directional")` by
`canonical_route_id`, `classify_route_timing`, and
`validate_route_cohort_rows`; no public entry point may emit a same-market
route. A collected route candidate must carry `route_id`, and it must equal
this canonical value exactly; missing and empty IDs are not inferred at the
collection boundary.

Each route leg must provide a non-empty `leg_id` and `market_id`. A leg is
explicitly unavailable only when `available` is `false` or its `status` (or
`collection_status`) states an unavailable terminal condition. Missing state
timestamps are timestamp failures, not an implicit unavailable-leg state.

`validate_route_cohort_rows(candidates, legs)` rejects duplicate directed
candidates, duplicate non-empty `candidate_id` values, same-market routes, a
non-canonical supplied route ID, duplicate leg IDs, and incomplete leg
identity. A supplied `candidate_id` must be a canonical string; including an
unhashable value raises `ValueError("route candidate ID is invalid")` rather
than leaking a Python `TypeError`. The validator never silently deduplicates
data.

## Timing classification

`classify_route_timing(candidate, buy_leg, sell_leg)` returns only these
fields:

```text
route_id       canonical directional route ID
skew_seconds   exact fixed-point decimal text, or null when unavailable
timing_status  within_sla, outside_sla, or unavailable
reason_code    null or one stable reason code
```

`skew_sla_seconds` defaults to `60`; it may be supplied as a non-negative
decimal-text value. A route at the threshold is `within_sla`, and any larger
value is `outside_sla` with `snapshot_skew_exceeded`.

When `validated_at` is supplied, either state timestamp later than that exact
instant is invalid. Missing, timezone-naive, malformed, and future state
timestamps return `timing_status = unavailable`,
`reason_code = invalid_state_timestamp`, and `skew_seconds = null`. Parser
exception text is never returned.

If more than one condition applies, the first matching reason is used in this
fixed priority order:

1. `route_deadline_exceeded`
2. `execution_adapter_unsupported`
3. `buy_leg_unavailable`
4. `sell_leg_unavailable`
5. `invalid_state_timestamp`
6. `snapshot_skew_exceeded`
7. `route_mode_not_executable`

The deadline reason is asserted by `route_deadline_exceeded = true` or a
candidate or leg status of `deadline_exceeded`; it is not inferred from a
historic timestamp. Adapter support is false when
`execution_adapter_supported = false` or `execution_adapter_status = unsupported`
on the candidate or a leg. The final mode reason applies to `route_mode` values `research_only`,
`unsupported`, or `not_executable`, or when
`route_mode_not_executable = true`.
