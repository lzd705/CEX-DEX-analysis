# Route Shadow, Static Delivery, and V3 Execution Design

## Decision

This release implements the first four approved next-stage items as three
independently testable increments:

1. a production shadow route-cohort orchestrator with readiness metrics,
   retention, resource limits, and a non-automatic promotion gate;
2. compressed, version-bound static delivery while preserving the existing
   Summary core/freshness cache separation; and
3. an exact Uniswap V3 execution adapter, enabled first for parity-approved
   canonical Ethereum deployments.

The fifth item, multi-Market selection, is excluded because another developer
owns that work. Funding Rate remains excluded. Shadow collection never places
orders, never writes the public opportunity pointer, and never promotes a
missing or estimated cost to zero.

## Planning baseline

The branch starts from production release
`fe735ef821b7b4d806012acf996d1e8edc80320a`. The clean isolated worktree passes
all 1,485 tests before this design is implemented.

The market totals below are planning inputs, not release acceptance evidence.
They were computed from production TVL snapshot
`20260802T010624Z-643fdc46` and depth/execution snapshot
`20260802T130926Z-3f852b82`, deduplicating physical pools by canonical chain,
DEX, and pool identity. Directed route upper bounds use at most three CEX and
three DEX legs per Token and count both directions. Implementation must emit a
read-only baseline manifest containing exact input paths, byte SHA-256s,
observation timestamps, filters, and calculation version before these values
can be reused as release evidence.

Production currently has 147 physical DEX pools and 148 Token-market series.
Only 19 series have proved DEX execution; all are constant-product V2. The
next measured-value execution family is V3:

- 65 V3 markets across 25 Tokens;
- approximately USD 65.265 million TVL;
- approximately USD 10.309 million source-reported 24-hour volume; and
- an upper bound of 360 directed CEX-to-DEX candidate routes before timing,
  capacity, fee, inventory, gas, router, MEV, and profitability gates.

Canonical Ethereum Uniswap V3 is the first deployment family:

- 29 pools across 17 Tokens;
- approximately USD 49.661 million TVL;
- approximately USD 5.753 million source-reported 24-hour volume; and
- an upper bound of 174 directed CEX-to-DEX candidates before strict gates.

These route counts are coverage upper bounds, not detected opportunities.

The deployed host has 2 vCPU, about 3.6 GiB RAM, no swap, and a dashboard RSS
near 210 MiB. The normal hourly depth cycle takes roughly five minutes. The
route collector's code default of 24 workers is therefore not a safe
production default on this host.

The current Summary path already has source-signature caches, a separate
lifecycle-state core, minute freshness overlays, gzip response caching,
single-flight locks, and background startup warmup. Static delivery is the
larger first-load bottleneck: the five principal public assets total about
793,009 bytes uncompressed and about 177,841 bytes at gzip level 5.

## Increment A: shadow route-cohort operations

### A1. One bounded orchestrator

A new route-shadow entrypoint owns one scheduled run. It performs these steps
in order:

1. acquire the existing global collection lock without waiting;
2. if the lock is busy, record `skipped_locked` and exit successfully without
   reading sources;
3. build a deterministic route universe from the currently published catalog,
   depth, execution, selected-window CEX volume, DEX 24-hour volume, and TVL;
4. atomically write the exact universe selected for this run to a run-scoped,
   immutable path;
5. collect a synchronized core with the existing 60-second deadline and
   process-isolated collector;
6. validate and publish only the private `routes/core/latest.json` pointer;
7. calculate the shadow audit from the fully reread private bundle;
8. atomically publish a joint shadow pointer binding the core manifest, audit,
   universe, and phase, then update bounded phase history; and
9. release the collection lock before running best-effort retention.

The runner never calls `publish_complete_route_bundle()` and cannot write
`routes/latest.json`. The systemd service receives no public-promotion flag.

Universe generation and collection remain one source-bound operation. A source
generation change between selection and publication rejects the run. A process
exit code of zero is not success by itself; success is derived from the
validated leg and route outcomes.

Production selection always uses the rolling 30 complete UTC days ending on
the latest completed UTC calendar day: `start = end - 29 days`. The candidate
source generation is the canonical hash of the exact published identities and
byte SHA-256s for the catalog/SQLite store, lifecycle and runtime Token
registries, CEX daily Volume, CEX/DEX Depth, CEX/DEX Execution, and DEX
TVL/24-hour Volume inputs. Each run stores its universe at
`routes/shadow/runs/<run_id>/route_universe.json` and records its SHA-256 in the
audit. The shared `route_universe.json`, if retained for the existing CLI, is a
diagnostic convenience and never the authoritative run identity.

`routes/shadow/latest.json` is the only readiness and phase-transition
boundary. It atomically binds `core_manifest_sha256`, `audit_sha256`,
`route_universe_sha256`, and `phase`. A core that publishes before its audit
fails remains available for diagnosis, but it is not a valid cohort and cannot
advance a phase.

### A2. Phase policy

The approved same-host rollout has two automatic shadow phases.

#### Canary phase

- Tokens: `PEPE,CAKE,SHIB,SUSHI,ZK,SNX,GRT,COMP,ENS,STRK`, the ten Tokens with
  currently proved V2 DEX execution, ordered by measured Volume/TVL value.
- Cadence: `:13`, `:28`, `:43`, and `:58` each UTC hour, except the 00:28,
  00:43, and 00:58 runs are omitted to avoid the daily collector at 00:30 UTC.
- Timer contract: UTC calendar entries are `01..23:13,28,43,58:00` plus
  `00:13:00`, with `Persistent=false`, `AccuracySec=1s`, and
  `RandomizedDelaySec=0`.
- Deadline: 60 seconds.
- Global workers: 2.
- Per CEX venue: 1.
- Per DEX chain: 1.
- Minimum observation period: 24 elapsed hours and at least 85 lock-acquired
  runs.

The phase advances to full shadow only when all of these are true:

- valid private-core rate is at least 99%;
- conditional route-skew pass rate is at least 99%;
- p95 passing-route skew is at most 30 seconds;
- every passing route has skew at most 60 seconds;
- p95 run duration is at most 75 seconds and every run is below 90 seconds;
- identity, generation, raw-hash, unsafe-path, and fixed-block lineage errors
  are all zero;
- OOM events, orphan collector processes, and interference with daily/depth
  publication are all zero; and
- the systemd CPU, memory, and task limits are verified as effective.

Failure to meet a gate keeps the service in canary. It never widens on elapsed
time alone.

#### Full-shadow phase

- Tokens: every currently configured Token with an eligible route leg.
- Cadence: the same 15-minute schedule.
- Global workers: 4.
- Per venue and per chain: 1.
- Minimum observation period before public-promotion eligibility: seven days
  and at least 500 valid private cohorts.

The phase transition is an atomic local state update with the prior phase and
gate evidence retained. It changes collection scope only; it does not publish
public opportunities.

### A3. Metrics contract

Every audit records exact numerator and denominator fields, not only rounded
percentages.

Metric arithmetic is deterministic:

- valid-core rate is `valid_joint_shadow_pointers / lock_acquired_runs`;
  `skipped_locked` runs are reported separately and never enter that
  denominator;
- end-to-end availability is `within_sla_routes / all_candidate_routes`;
- conditional skew pass rate is
  `within_sla_routes / (within_sla_routes + outside_sla_routes)`, so unavailable
  routes are excluded only from this conditional denominator;
- route age is `audit_finished_at - min(buy_state_observed_at,
  sell_state_observed_at)` for a two-leg-available route;
- percentiles use deterministic nearest-rank, `ceil(p * n)` after ascending
  sort; one sample returns itself; and
- an empty denominator or sample is `not_evaluated`, never zero or 100%, and
  fails every advancement or promotion gate that depends on it.

Run metrics:

- scheduled, lock-acquired, skipped-locked, valid-core, failed, and timed-out
  counts;
- valid-core rate and skipped-lock rate;
- p50/p95/max duration; and
- resource-limit, OOM, orphan-process, and primary-publication-interference
  counts.

Leg metrics:

- selected and observed counts;
- partial, failed, deadline, unsupported, and invalid counts;
- breakdowns by CEX venue, DEX chain, and adapter ID; and
- structural lineage error counts.

Timing metrics:

- all candidate routes, routes with two available legs, within-SLA routes, and
  outside-SLA routes;
- end-to-end route availability;
- conditional skew pass rate; and
- p50/p95/max skew and final route age.

Cost readiness has three distinct states:

1. structural topology completeness;
2. strict cost/evidence completeness; and
3. research-scenario completeness.

An unavailable fee or inventory profile is counted as incomplete, not zero.
If opportunity inputs are not yet installed, strict completeness is explicitly
`not_evaluated` and public promotion is blocked.

### A4. Retention and resource isolation

Raw route evidence is retained for seven days, shadow audit summaries for 30
days, and 4 GiB is the route-storage admission/high-water mark. Retention
builds a reference set before deletion and must preserve:

- evidence referenced by the current private core;
- evidence referenced by the current public pointer and explicitly configured
  rollback bundles;
- the latest audit and phase-transition evidence; and
- any run inside the active validation window.

Deletion is limited to validated descendants of the dedicated route raw and
audit roots. Symlinks, hard-link ambiguity, unexpected members, and unresolved
references fail closed. Protected evidence is never deleted to force the
storage total below 4 GiB. If protected evidence reaches the high-water mark,
the service records `storage_pressure`, stops before new source reads, and
blocks phase advancement and promotion until an operator archives evidence or
raises the limit.

The user service uses `Nice=15`, `KillMode=control-group`, `UMask=0077`,
`TimeoutStartSec=90s`, and canary limits of `CPUQuota=50%`, `MemoryMax=512M`,
and `TasksMax=16`. Full shadow raises only the declared bounds to
`CPUQuota=80%`, `MemoryHigh=512M`, `MemoryMax=768M`, and `TasksMax=32`.
Deployment must verify the limits through the running cgroup; a rendered unit
file alone is not evidence.

Each scheduled run writes a durable `started` ledger entry before resource-heavy
work. An `ExecStopPost` reconciler records the systemd result, exit code, and
exit status after normal failure, timeout, or OOM termination. The next run
also closes any unreconciled prior entry as `unexplained_termination`.
Promotion requires no unexplained scheduled-run ledger gaps; a process killed
before it can write its own audit can therefore never disappear from the
denominator.

### A5. Public promotion gate

Shadow never promotes automatically. A separate operator command may only
commit `routes/latest.json` after a complete opportunity bundle passes all
existing cryptographic, SQLite, CSV, component-topology, and generation gates
plus the following readiness policy:

- at least seven days and 500 valid shadow cohorts;
- valid-core rate at least 99.5%;
- conditional skew pass rate at least 99%;
- every passing route at most 60 seconds skew;
- no structural lineage or unsafe-evidence errors;
- no primary-collection interference;
- structural component topology exactly 100%;
- every route/notional in the intended strict scope has strict fee, inventory,
  quote, conversion, gas, router, tax, and route-mode evidence either complete
  or source-proved not applicable;
- strict cost/evidence completeness exactly 100%; and
- the release checker is run with route opportunities required.

A non-positive edge may legitimately produce no executable candidate, but it
may not bypass complete cost evidence. Research estimates and assumptions
never satisfy the strict gate. Promotion remains a deliberate, separately
logged operation even after every threshold passes.

## Increment B: static delivery and cache boundaries

### B1. Application-level gzip

The server precompresses only public, release-fingerprinted assets at process
startup with deterministic gzip settings. For clients that accept gzip, the
server returns the precompressed representation with the exact compressed
`Content-Length`, `Content-Encoding: gzip`, and `Vary: Accept-Encoding`.

Acceptance is parsed as an HTTP content-coding preference, not a substring:
`gzip;q=0` and wildcard exclusions receive the identity representation.

Clients without gzip receive the original bytes. Decompression must reproduce
the source file byte-for-byte, and the release asset SHA continues to hash the
uncompressed public bundle.

HTML may be compressed but remains `no-cache`. API responses remain
`no-store`; no static/CDN rule may cache market data or opportunity responses.

### B2. Immutable version binding

Only a public asset requested with exactly one query field, the exact current
`?v=<application-and-asset-version>`, receives:

```text
Cache-Control: public, max-age=31536000, immutable
```

An absent, duplicated, blank, incorrect, or accompanied-by-another-field
version remains `no-cache`.
Protected admin files never enter the public precompressed map. HEAD and GET
must return identical representation headers while HEAD sends no body.
Every response carries exactly one `Cache-Control` field: an exact version has
`public, max-age=31536000, immutable` and no `no-cache`/`no-store`; every other
static request has only `no-cache`.

The release checker enforces gzip for public assets over 1 KiB and a combined
gzip budget of at most 220 KiB for the first-load public bundle.

### B3. Summary and Catalog policy

The existing Summary architecture is retained rather than replaced:

- source-derived core cache;
- lifecycle fresh/stale state separation;
- dynamic freshness overlay;
- per-route single-flight;
- gzip response cache; and
- default startup warmup.

This release adds observability for warmup duration, cache generation, and
warmup success, and caches both gzip and identity serialization for the default
Summary. It does not create a new Summary artifact/pointer contract because
production measurements do not justify that additional publication boundary.

The full Catalog remains an audit endpoint. The browser continues to use the
lightweight Summary and one Token catalog at a time.

### B4. CDN boundary

An external CDN is not activated in this release because production has only
a raw HTTP IP and no approved domain, certificate, or CDN account. The exact
versioning, immutable headers, compressed representations, and release checker
make the origin CDN-ready. CDN activation is a later infrastructure change and
must preserve HTML revalidation and API `no-store` behavior.

## Increment C: exact Uniswap V3 execution

### C1. Adapter registry and capability separation

DEX model classification moves behind a typed adapter registry without
changing existing V2, V3-depth, or unsupported output bytes. Capabilities are
separate for:

- strict depth;
- fixed-notional execution; and
- exact route base-quantity quoting.

A deployment family that declares a capability but returns `unsupported` is a
failed adapter. A V3-like fork is not enabled merely because its current depth
classifier resembles V3.

### C2. Exact integer SwapMath

The V3 adapter implements protocol integer behavior for:

- TickMath bounds and Q64.96 prices;
- full-precision multiplication/division and round-up variants;
- amount0/amount1 deltas;
- exact-input and exact-output swap steps;
- fee-pip rounding;
- tick bitmap traversal;
- signed liquidity-net changes in both directions; and
- base-unit target quantities shared with the route evaluator.

The existing Decimal depth approximation is not reused for strict execution.
Every result is an exact integer or an explicit terminal state.

Tick scanning is bounded. If the adapter cannot prove completion before its
scan guard, it returns `partial/source_tick_scan_limit`; it never extrapolates
the final tick. A larger notional cannot recover from an incomplete smaller
scenario.

### C3. Same-block parity gate

Reference fixtures bind chain ID, deployment family, pool, fixed block, raw RPC
hashes, exact-input/output raw quantities, and an official V3 implementation
reference. Local SwapMath and the fixed-block Quoter must agree exactly before
a deployment family is enabled. Enablement also requires an exact registry of
factory, Quoter, and SwapRouter addresses and code identities, pool-factory
lineage, and an allowed pool discovery rule or explicit pool list. A DEX label
or V3-like ABI is never sufficient; any identity mismatch remains unsupported.

The first production family is canonical Ethereum Uniswap V3. Rollout is:

1. offline reference vectors;
2. no-publish LINK/WETH canary at one fixed block;
3. one exact-market bounded `dex_execution_cost` fact publication;
4. canonical Ethereum family bounded `dex_execution_cost` fact publication;
   and
5. full `dex_execution_cost` fact-inventory publication only after release
   checks and before/after status reconciliation pass.

These are DEX fact publications through the existing depth/execution snapshot
boundary. None writes the public opportunity pointer `routes/latest.json`.

Arbitrum V3, Pancake V3, Slipstream, Algebra/Camelot, and other forks remain
unsupported until each deployment family has independent parity evidence.

### C4. Route and cost compatibility

The adapter must implement the shared exact `quote_base_quantity()` contract.
Fixed-USD scenario support alone cannot make a route eligible. Pool fees already
embedded in the quote remain completeness evidence and are not added twice.

The current route-cost registry already recognizes Ethereum Uniswap V3, but a
DEX route remains non-strict when gas, router, tax, MEV policy, inventory, CEX
fee, USD conversion, or synchronized cohort evidence is absent. Adapter
coverage therefore cannot create a public opportunity by itself.

## Error handling

All three increments fail closed:

- lock contention creates a bounded skip record;
- source-generation drift rejects the shadow run;
- malformed or unsafe raw evidence blocks core publication;
- incomplete strict costs block public promotion;
- missing static compression falls back to correct uncompressed delivery,
  never corrupted bytes;
- wrong asset versions never receive immutable caching;
- adapter parity mismatch is `failed` for a declared family;
- unapproved forks remain `unsupported`; and
- missing facts remain N/A, never numeric zero.

## Testing and release evidence

Implementation follows RED-GREEN-REFACTOR for each increment.

Route tests cover:

- deterministic universe generation and atomic replacement;
- rolling 30-complete-day selection and exact source-generation binding;
- shared-lock contention and primary-priority behavior;
- the atomic joint shadow pointer and orphan-core exclusion;
- canary/full phase transitions and every negative gate;
- exact metric denominators, empty samples, and nearest-rank percentiles;
- exact 60-second skew and 120-second route-age boundaries;
- resource-unit rendering;
- scheduled-run reconciliation after timeout/OOM/unexplained termination;
- reference-safe retention and protected-set storage pressure; and
- inability of the shadow service to write the public pointer.

Static tests cover:

- gzip byte equality and content length;
- version-exact immutable caching;
- wrong/missing version fallback;
- `gzip;q=0` and wildcard content-coding exclusions;
- exactly one non-conflicting `Cache-Control` field;
- HEAD/GET parity;
- protected-file isolation;
- release asset SHA stability; and
- the 220 KiB public gzip budget.

V3 tests cover:

- official integer vectors and boundary rounding;
- exact input/output with no, one, and multiple tick crossings;
- both swap directions and liquidity-net signs;
- bounded-scan partials;
- same-block Quoter parity;
- exact factory/router/quoter code-identity and pool-lineage gating;
- fork non-enablement; and
- route common-quantity conformance.

Every increment must pass its focused tests, the complete suite, Python 3.8
grammar/compile checks, JavaScript syntax checks, `git diff --check`, the
release smoke suite, production-server tests, `/health`, the release checker,
and desktop/mobile browser QA before it is called deployed.

The deployment sequence preserves a rollback SHA and separates:

1. application deployment;
2. static-delivery verification;
3. shadow canary enablement;
4. V3 no-publish/candidate collection; and
5. any later full DEX fact publication.

No public opportunity pointer is published in this release unless the full
seven-day promotion gate has actually elapsed and passed.

## Alternatives considered

### Route operations

- **Chosen:** staged same-host shadow. It fits the current infrastructure while
  bounding risk on a 2-vCPU server.
- Direct all-Token same-host shadow is faster but exposes provider and resource
  limits before canary evidence exists.
- A dedicated collector VM is the eventual choice for sub-120-second public
  opportunities, but it adds infrastructure cost and coordination outside this
  release.

### Static delivery

- **Chosen:** application-level precompression and immutable cache semantics.
  It works with the current TCP proxy and Python 3.8 runtime.
- Switching immediately to Nginx expands the deployment boundary before a
  domain and HTTPS are available.
- Collection-time Summary artifacts add a new pointer and rollback contract
  without evidence that Summary computation is the current bottleneck.

### DEX adapters

- **Chosen:** exact V3 execution first because it has the highest measured
  Volume, TVL, and route breadth among missing execution families.
- Uniswap V4 is second but requires Pool ID, PoolKey, StateView, hook, and
  dynamic-fee validation; nonzero hooks can alter swap cash flows.
- Solana adapters are third and require a separate fixed-slot account lineage
  before Orca, Meteora, or Raydium math can be trusted.
