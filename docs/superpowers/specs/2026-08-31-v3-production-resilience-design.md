# Uniswap V3 Production Resilience Design

## Decision

Repair the failed Uniswap V3 production observation window on top of the
deployed revision
`02b55059f0ac0592b6f1a011c2fe560c5c406bba`. The repair has three bounded
parts:

1. distinguish the pinned finalized block from a later advancing finalized
   head;
2. add ordered, sequential JSON-RPC failover for DEX depth collection; and
3. replace silent collection-lock skips with bounded waiting and an observable
   temporary failure.

The deployment initially uses two independent free RPC providers per required
chain because no paid RPC is currently available. This is a best-effort
availability improvement, not an SLA. A credentialed primary can later be
configured through the same interface without changing the data contract.

## Incident facts

The first 26-hour production observation was not qualified. Its scheduled
hourly depth runs contained five failures and one lock-contention skip:

- two runs rejected valid evidence after the Ethereum finalized head advanced
  by one 32-block checkpoint while the fixed observation block stayed stable;
- one run lost all five BSC depth markets after the sole BSC endpoint timed
  out, and the unchanged coverage gate correctly rejected the candidate;
- two runs lost both authority Uniswap V3 pools after the sole Ethereum
  endpoint returned HTTP 403, and the exact gate correctly rejected the
  candidate; and
- the 01:05 UTC depth run overlapped the 00:30 UTC daily run, returned
  `skipped_locked`, and exited zero, so systemd treated the missing hourly
  observation as success.

There was no observed SIGKILL or OOM in the failed window. The repair therefore
does not change memory limits, swap, or calculation scope.

## Invariants that do not change

- The authority remains exactly the reviewed UNI/WETH and UNI/USDT Ethereum
  Uniswap V3 pools.
- All pool state, bitmap, tick, depth, execution, and QuoterV2 calls for one
  candidate remain pinned to one shared finalized block number and hash `F`.
- The `uniswap_v3_exact_validation/v1` public receipt continues to expose `F`
  as its shared finalized identity. No receipt or dashboard schema is changed.
- Missing, failed, partial, stale, and unsupported facts remain null or blank;
  they are never converted to zero or inferred.
- Exact, aggregate coverage, freshness, health, and release gates remain
  fail-closed. Endpoint exhaustion retains evidence and the previous complete
  public generation rather than publishing a degraded candidate.
- The result remains pool-only fixed-block depth and execution cost. It is not
  an all-in future trade guarantee.

## Finalized evidence roles

Let `F` be the shared finalized block selected for the collection cohort.
There are two different evidence roles:

1. **Pinned block identity.** A retained numeric
   `eth_getBlockByNumber(hex(F), false)` response must exactly match the scan
   manifest's start and final block number, hash, and timestamp. This is the
   identity used by all state and Quoter calls and by the public receipt.
2. **Later finality checkpoint.** A later
   `eth_getBlockByNumber("finalized", false)` response proves that the chain's
   finalized head is at or beyond `F`. Its number must be `>= F`. If it is
   still exactly `F`, its hash must match the pinned hash. If it is greater
   than `F`, a different hash is expected and must not be compared with the
   hash of `F`.

The collector and the offline retained-evidence validator must enforce the
same rules. A finalized checkpoint below `F`, a same-height hash change, a
missing numeric `F` header, a mismatched numeric `F` header, or different
pinned identities across the two pools is rejected.

This fixes the deterministic contract error without weakening reorg or
same-block identity checks and without adding a time-varying later head to the
shared receipt.

## Ordered RPC endpoint pool

### Configuration

The existing scalar variables remain backward compatible and define the first
endpoint:

- `DEX_DEPTH_RPC_ETH`
- `DEX_DEPTH_RPC_BSC`
- the corresponding existing scalar variables for other supported chains.

An optional JSON-array variable adds ordered fallbacks:

- `DEX_DEPTH_RPC_ETH_FALLBACKS`
- `DEX_DEPTH_RPC_BSC_FALLBACKS`
- the corresponding variable for any other supported chain.

For example, the value is a JSON list of URL strings. Empty strings,
non-string entries, duplicate URLs, malformed JSON, or more than the bounded
maximum number of endpoints fail configuration before collection. Legacy
configuration with only the scalar variable behaves as it does today.

Production endpoint values live in the existing mode-`0600` systemd
environment file. Repository examples use placeholders and never contain
credentials. Endpoint identities retained in evidence are stable positional
IDs such as `eth-primary` and `eth-fallback-1`; full configured URLs are never
written to public facts or logs.

### Failure policy

One run-scoped client is reused for each chain. Requests remain sequential;
there are no hedged or parallel calls.

- HTTP 401, 403, and endpoint-level 404 open that endpoint's breaker
  immediately for the rest of the run and move to the next endpoint.
- HTTP 429, HTTP 5xx, connection resets, DNS failures, and timeouts receive a
  small bounded retry budget with capped backoff. Exhausting that budget opens
  the run-scoped breaker and moves to the next endpoint.
- Valid JSON-RPC contract reverts, malformed protocol results, and semantic
  identity failures do not get hidden by trying another provider as if the
  underlying market fact had changed.
- The collection deadline bounds retries, failover, and the whole request.

Before a fallback endpoint may serve a fixed-block request, it must return the
expected chain ID and the exact pinned block number, hash, and timestamp. If it
cannot prove that identity, it is rejected and no result from it is mixed into
the candidate. The affected pool is restarted from its fixed-block evidence
boundary rather than combining an unverified partial transcript with a new
provider.

### Evidence and privacy

The retained transcript adds a bounded RPC-attempt ledger containing only:

- stable endpoint ID and sanitized endpoint identity;
- method/evidence stage;
- bounded outcome category and HTTP status where applicable;
- retry/failover decision and duration; and
- the fixed block identity used to approve a fallback.

It never contains URL query strings, credentials, request headers, raw
exception strings, or unbounded response bodies. Public row errors use bounded
reason codes such as `rpc_endpoint_exhausted`. Existing transcript SHA lineage
binds the attempt ledger to the candidate.

With free-only providers, a successful failover establishes evidence quality
for that observation but not a future uptime guarantee. Production reporting
must continue to call this configuration best-effort until credentialed
service/SLA terms exist.

## Collection lock behavior

The daily timer remains at `00:30 UTC` and hourly depth remains at `*:05 UTC`.
Changing the daily start earlier could collect before all daily upstream
sources are ready, so schedule movement is not part of this repair.

Scheduled collection commands receive a bounded lock wait, initially 15
minutes, measured with a monotonic clock. They acquire the same exclusive lock
and keep it through the complete collection/publication critical section.

- If the holder exits before the deadline, the waiting run proceeds normally.
- If the deadline expires, the structured result remains
  `status=skipped_locked` with `reason=lock_wait_timeout`, but the CLI exits
  with temporary-failure status `75` so systemd and monitoring cannot call the
  missed observation successful.
- A timeout creates no empty run directory, does not move the latest-run
  pointer, and does not modify public data.
- Manual and existing programmatic callers retain immediate non-blocking
  behavior with `lock_wait_seconds=0`.

The depth service timeout must include the 15-minute wait plus its normal
collection envelope. Timer persistence remains enabled. Automatic concurrent
publication, an unbounded wait, and a generic retry timer are explicitly out
of scope.

## Component flow

For an ordinary depth run:

1. the runner waits for and acquires the shared collection lock;
2. the chain client loads and validates its ordered endpoint pool;
3. the primary proves chain identity and selects shared finalized block `F`;
4. all fixed-block evidence is requested at numeric `F`;
5. on an eligible provider failure, a fallback proves the same chain and
   exact `F` identity before the affected evidence unit restarts;
6. retained numeric-`F` and advancing-finalized-head evidence is validated
   under its correct role;
7. exact and aggregate publication gates run unchanged; and
8. only a complete accepted generation replaces the public bundle.

If any step fails, evidence is retained, the service exits nonzero, and the
last complete published generation remains in place.

## Test strategy

### Finality

- Accept pool A with later finalized head `F` and pool B with `F+32` while all
  state calls and both manifests remain pinned to `F`.
- Reject later head `< F`.
- Reject same-height/different-hash evidence.
- Reject missing or mismatched retained numeric-`F` headers.
- Preserve the exact v1 receipt fields and shared `F` identity.

### RPC failover

- Preserve one-endpoint legacy behavior.
- Reject malformed, empty, duplicate, or excessive endpoint configuration.
- Reproduce Ethereum HTTP 403 and prove immediate fallback completes both
  authority pools without revisiting the open primary.
- Reproduce BSC timeout and prove bounded retry, one run-scoped breaker, and
  reuse of the fallback for later BSC pools.
- Cover 429, 5xx, retry deadline, and complete endpoint exhaustion.
- Reject fallback wrong-chain, missing-block, different-hash, and
  different-timestamp identities.
- Prove exhaustion still fails exact/coverage publication and retains the old
  public generation.
- Prove logs, rows, manifests, and transcripts do not contain configured URLs,
  credentials, or raw exceptions.

### Lock waiting

- Prove no collector step starts while another process owns the lock, then the
  waiter succeeds after release.
- Prove timeout exits `75`, creates no run/latest mutation, and publishes
  nothing.
- Preserve immediate `lock_wait_seconds=0` behavior for manual callers.
- Verify both user and system service templates pass the scheduled wait and
  allow the enlarged timeout.

### Regression

- Run focused collector, exact-publication, collection-runner, systemd-template,
  dashboard, health, and release tests.
- Run the complete repository suite with Node available.
- Parse every changed Python file using Python 3.8 grammar, compile the tree,
  check whitespace, and scan changed artifacts for URL credentials and local
  production paths.

## Rollout and acceptance

1. Implement on an isolated branch whose merge base is exactly `02b55059`.
2. Run all local and GitHub quality gates.
3. Configure two independently operated free endpoints for Ethereum and BSC
   in the private production environment and capability-test both at one
   finalized block. Do not commit endpoint secrets.
4. Pause scheduled collectors and use the existing staged V3 launch workflow
   to run a full non-publishing candidate with forced primary-failure
   rehearsals.
5. Back up the active application revision, unit bytes/state, environment-file
   hash, public five-file bundle, trusted receipt, and retained raw evidence.
6. Deploy code and service-template changes together, run normal release
   checks, and restore timer state.
7. Start a new observation window only after the first successful scheduled
   exact run. Require at least 26 hours, one daily cycle, multiple hourly
   cycles, 2/2 exact depth, 20/20 exact execution, matching trusted/public
   receipts, no silent lock skip, no unexplained scheduled gap, and no
   coverage/release failure.

Any hard failure invalidates that observation window. Do not automatically
roll back or expand beyond the two authority pools; report the evidence and
diagnose first.

## Deferred work

- Paid-provider procurement and an availability SLA.
- Cross-run persistent circuit-breaker state or provider scoring.
- Parallel/hedged RPC requests.
- A general job queue or complete collection scheduler rewrite.
- Automatic rollback and automatic V3 market expansion.
- Changing the daily source-readiness schedule without measured upstream
  readiness evidence.
