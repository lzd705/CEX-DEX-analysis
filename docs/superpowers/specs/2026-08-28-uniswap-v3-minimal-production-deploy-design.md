# Uniswap V3 Minimal Production Deploy Design

## Decision

Build a production deployment branch directly from the currently deployed
application revision
`fe735ef821b7b4d806012acf996d1e8edc80320a`. Port only the code required to
publish exact pool-only depth and execution facts for these two reviewed
Ethereum Uniswap V3 markets:

- UNI/WETH: `0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801`
- UNI/USDT: `0x3470447f3cecffac709d3e783a307790b0208d60`

The branch must not inherit route-shadow, observed-data-quality, static-cache,
or summary-warmup changes solely because those changes sit between the live
revision and the existing V3 feature branch. The existing feature revision
`401eaad12965560ffd67e60caad60d2a267e206d` is a reference implementation,
not the deployment base.

## Supported fact and non-claims

The supported result is an exact, fixed-block, pool-only swap calculation. It
includes the pool fee and Uniswap V3 integer rounding. It does not include gas,
router fees, transfer taxes, MEV, wallet balances, approvals, private order
flow, or a guarantee that a future transaction will execute at the observed
block state.

All other V3 markets remain explicitly unsupported. Missing, partial, failed,
stale, and unsupported facts remain null or blank and are never converted to
zero.

## Minimal code boundary

The branch ports these capabilities at function level from the reviewed V3
implementation:

1. the two-market authority file and dependency-free integer Uniswap V3 math;
2. fixed-finalized-block pool identity, bitmap/tick scanning, exact-input and
   exact-output simulation, and QuoterV2 parity;
3. retained RPC, scan-manifest, and GeckoTerminal USD evidence with SHA-256
   lineage;
4. the full-inventory two-pool exact validator and deterministic
   `uniswap_v3_exact_validation/v1` receipt;
5. one five-file atomic public bundle containing depth history, depth latest,
   depth snapshot, execution latest, and the exact validation sidecar;
6. public API scope metadata, fail-closed handling of unapproved V3 rows,
   exact health, and the unchanged hard release requirement; and
7. a non-publishing canary that delegates to the same production validator.

The port preserves the `fe735ef` interfaces where intermediate route work had
changed them. In particular, DEX observation keeps its original two-value
return contract and does not gain route-cost typed-payload parameters,
route-specific block hashes, or the route collection lock implementation.

## Exact publication invariant

A candidate can be published only when all of the following hold:

- both authority identities are present and match chain, factory, pool,
  token0, token1, decimals, fee, and tick spacing;
- both depth rows are observed and complete at 10, 25, 50, and 100 bps;
- each market has exactly ten observed scenarios: two directions by five
  configured USD notionals;
- every scenario matches the retained QuoterV2 request and four-word response;
- the two pools share one finalized block number and block hash;
- depth and execution rows share the retained per-pool transcript hash;
- the raw RPC transcript, scan manifest, TVL manifest, and GeckoTerminal bytes
  are reread from regular non-symlink files and match their identities/hashes;
- normal aggregate depth and execution coverage gates pass; and
- the receipt and candidate rows are validated again before the public bundle
  is replaced.

The authority pools cannot use bounded `merge-publish`. A one-pool rehearsal
is a calculation/connectivity check only and never produces the two-pool
publication receipt.

## Production staging architecture

Add one operator-invoked tool, `scripts/uniswap_v3_launch.py`. It uses a
versioned launch directory and canonical receipts for these phases:

1. **preflight** — verify the target checkout SHA, live application SHA,
   current public health, configured production paths, fixed systemd unit
   identities, and that required inputs are regular non-symlink files;
2. **pause** — record the prior enabled/active state of the daily and depth
   timers, stop/disable those timers, wait for their oneshot services to be
   inactive, and prove the collection lock is unheld;
3. **backup** — copy and fsync the five public bundle files into a private
   `0700` launch directory, recording byte hashes and modes. A sidecar that did
   not exist before first launch is recorded as `exists: false`, not fabricated
   as an empty file;
4. **stage** — recheck the live hashes, copy the immutable runtime inputs into
   a new non-symlink staging data directory, and run the full unfiltered DEX
   depth profile with publication directed only at that staging directory;
5. **verify-stage** — run the target dashboard on loopback using live unchanged
   facts while overriding the staged DEX depth, execution, and exact sidecar;
   run the normal health and release checker against that transient process;
6. **promote** — compare-and-swap: require live hashes still equal the backup
   baseline, validate the staged receipt and candidate again, then replace the
   five live files as one existing atomic publication bundle;
7. **restore** — require live hashes equal the recorded promoted generation,
   restore all original bytes/modes, and restore an originally absent sidecar
   to absence; refuse to overwrite third-party drift; and
8. **resume** — restore exactly the recorded timer enabled/active state only
   after either forward release validation or verified rollback succeeds.

Every state-changing phase requires an explicit execution flag. The default is
a read-only plan. The tool never changes the Git application pointer, never
edits the environment file, never escalates privileges, and never logs RPC
URLs, credentials, environment contents, or absolute production paths in its
portable receipts.

## Cutover ordering

The old dashboard remains active while the candidate is collected and checked
through the transient target dashboard. After staging passes:

1. keep daily/depth timers paused;
2. stop the production dashboard to avoid a mixed old-code/new-data window;
3. promote the five-file candidate;
4. switch the already-reviewed application checkout to the target SHA;
5. start the dashboard and run the unchanged health, release, and browser
   checks; and
6. resume timers only after success.

On failure, keep timers paused, stop the dashboard, restore the previous
application SHA and checksummed data generation, start the old dashboard,
verify the old SHA and release health, and only then restore the prior timer
state.

## CI boundary

Because `fe735ef` predates repository CI, the minimal branch adds only:

- the existing Python 3.8/3.14 and Node 24 quality-gate workflow;
- the test-only Node stdin portability fixes needed on Linux; and
- a deterministic lifecycle fixture clock so a tracked August 1 evidence file
  does not make a contract test fail as wall-clock time advances.

These changes do not refresh or reinterpret production data. Runtime
freshness remains fail-closed.

## Verification

Completion requires all of the following separately:

- branch ancestry proves the merge base is exactly `fe735ef`;
- the diff contains no route-shadow, observed-quality, static-delivery, or
  unrelated production files;
- focused V3, publication, launch, dashboard, release, collection, and atomic
  rollback tests pass;
- the complete repository suite passes on the available local runtime;
- Python 3.8 grammar, compile, whitespace, and secret/path scans pass;
- GitHub Python 3.8 and 3.14 jobs pass with Node 24; and
- a real non-publishing staged rehearsal retains exact raw evidence.

Code completion, a pushed branch, a staged candidate, live application SHA,
and live published generation are reported as different states. This design
does not authorize production deployment by itself.

## Deferred work

- A generic crash-recovery transaction framework for every fact family.
- Automatic rollback or long-running post-launch monitoring.
- A general Git checkout/deployment manager.
- Any weakening of freshness, coverage, exact receipt, health, or release
  acceptance rules.
- An OOM claim or resource-limit change without server journal, cgroup, and
  peak-memory evidence.
