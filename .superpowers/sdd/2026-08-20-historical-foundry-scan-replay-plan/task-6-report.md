# Phase 2 Task 6 implementation report

Date: 2026-08-31

Branch: `codex/historical-foundry-opportunity`

Starting HEAD: `e730a85467686197c280b1c8c18ffe87a19df81f`

Scope: fresh-Anvil single-scenario evidence capture only. No connected RPC,
real endpoint, descending controller, winner/full-ten selection, publication,
deployment, dashboard, or Task-7 manifest work was performed.

## Result

Task 6 now consumes one scan-issued scenario from the exact current
staging/window/grid lineage, preserves the capture run's relay capability,
starts and reaps one fresh Anvil process through the reviewed toolchain,
derives/applies the fixed state override, validates the local receipt/trace and
closed-revert contract, and commits overlay/receipt/trace/result as one
storage-owned no-replace transaction. A successful first freeze contains the
complete ordered nine-row `historical_foundry_cost_proof_inputs/v1` object and
its typed hash. The returned projection contains digests and normalized scalar
evidence only.

## SDD and TDD evidence

The task brief, the plan's Global Constraints/Task 6/Task 7 ingress, the replay
design's overlay/relay/Anvil/revert/result/ledger sections, and progress rulings
R6.1-R6.8 were read before implementation. No contradiction outside those
rulings was found.

The first clean RED command was:

```text
python3 -m unittest tests.test_historical_foundry_anvil -v
```

After correcting test-only imported-`TestCase` discovery, it ran four intended
Task-6 cases and failed for missing production features (the Anvil module,
sealed relay/process/scenario authorities, and storage sink), rather than an
import or fixture error.

Focused RED-to-GREEN groups were then run before each implementation slice:

- relay lease/HMAC/allowlist/resource/deadline: 2 feature-missing RED cases,
  then 2/2 GREEN;
- process lease/signature/TERM-KILL-reap/control flow: 3 RED, then 3/3 GREEN;
- exact scenario lineage and overlay KAT/runtime drift: 4 RED, then 4/4 GREEN;
- quartet transaction/proof/member limits: 2 RED, then 2/2 GREEN;
- exact outer plus inner closed-revert classification: 1 RED, then 1/1 GREEN;
- offline independent two-repeat/local RPC boundaries: 2 RED with 24
  feature-missing subtest errors, then 2/2 GREEN;
- final scenario 120-second and actual UNI `Transfer` delta boundary:
  2 `AttributeError` RED cases for the missing helpers, then 2/2 GREEN.
- process stdout+stderr combined inclusive 64-KiB boundary and total-plus-one
  rejection:
  1 `AttributeError` RED case for the missing output-count validator, then
  1/1 GREEN with both streams continuously drained.
- exact `evm_setNextBlockTimestamp` local-method identity: one allowlist
  mismatch RED, then 1/1 GREEN on both runtimes;
- complete eight-field fork-base header equality: one missing-helper RED,
  then 1/1 GREEN on both runtimes;
- exact pool/gas/MEV proof derivation and the closed nine-row semantic matrix:
  one unexpected-signature RED and one missing-validator RED, then the two
  focused tests passed on both runtimes.

The final focused Task-6 command was:

```text
python3 -m unittest tests.test_historical_foundry_anvil -v
```

After the final self-review additions, the focused suite result was
`Ran 17 tests in 25.917s` / `OK` on the system runtime. The exact-runtime
result is included in the final combined command below.

Native seam tests were also added to the owning scan, RPC, storage, and
toolchain modules, rather than relying only on `test_historical_foundry_anvil`.

## Regression failures and resolution

The first system plan command:

```text
python3 -m unittest tests.test_historical_foundry_anvil tests.test_historical_foundry_storage -v
```

reported `Ran 187 tests in 579.320s` with three failures. The failures were
fully diagnosed: a direct RPC import violated the storage offline import graph,
a strong Task-6 registry reference kept a successor snapshot live, and the
source-inventory golden changed. The import was replaced by an already-bound
module bridge, Task-6 sink/ledger registries were made weak without weakening
the existing strong snapshot authority, and the golden was independently
rederived.

The initial count of 187 was not the intended final discovery count. At that
point `test_historical_foundry_anvil` still exposed 14 imported upstream tests
to unittest discovery in addition to its 14 Task-6 tests; those upstream tests
were consequently repeated when the storage module was also named. The
imports were converted to module-qualified fixture/helper references. Three
new Task-6 boundary tests were subsequently added (scenario deadline/actual
`Transfer` delta plus full header equality, process stdout/stderr limits, and
closed proof-row semantics), so final discovery is exactly 17 Anvil tests plus
159 storage tests, or 176. No real owning-module test was removed. Both
runtimes independently reported these same counts.

The golden was not copied from a failure message. The small fixture was
materialized again; only the runtime-varying
`source_identity.python` field was removed; the complete remaining inventory
was encoded with sorted keys, compact separators, and ASCII JSON; SHA-256 was
then computed independently. Evidence:

```text
encoded bytes: 32196
computed: 2e7ba9c9e8b13155926a157f65d6fe48e577400d7301df7839028f381f6761f2
materialized common tail: 2e7ba9c9e8b13155926a157f65d6fe48e577400d7301df7839028f381f6761f2
```

The first affected-suite run reported 326 tests with three failures: the fixed
source inventory expected tuple had not yet included the Anvil source, and two
isolated main-only scan aliases encountered a canonical-module lookup. The
inventory was updated, and scan identity resolution now accepts only the
canonical module or a `__main__` module whose spec has the exact canonical scan
name. All three focused reproductions passed.

A later combined run exposed one test-lifecycle failure: the offline repeat
test read a generation-3 successor snapshot but did not explicitly close it,
so a following Task-4b strong-snapshot-registry invariant correctly failed.
The test now closes the successor. The reproducing sequence
`two-repeat + slice4_stop` passed 2/2 before the full rerun.

Final staged-diff review caught that the first output-limit implementation had
mistakenly applied 64 KiB to each stream instead of the frozen 64-KiB combined
`stdout+stderr` budget. The corrected exact/total-plus-one tests produced three
expected assertion failures across the Anvil and native toolchain seams before
the implementation changed; both focused tests then passed on both runtimes.
The same review corrected the timestamp method name, expanded fork-base
comparison from hash/root to all eight canonical fields, and replaced
placeholder proof amounts with exact decimal derivations from the sealed
notional, receipt gas, block-bound feed price, and policy MEV rate.

## Final verification

System runtime:

```text
python3 -m unittest tests.test_historical_foundry_scan tests.test_historical_foundry_rpc tests.test_historical_foundry_toolchain tests.test_historical_foundry_contracts -v
Ran 326 tests in 960.334s
OK

python3 -m unittest tests.test_historical_foundry_anvil tests.test_historical_foundry_storage -v
Ran 176 tests in 982.905s
OK
```

Exact CPython 3.8.10 runtime:

```text
/Users/luchuanyu/on-chain-market-structure-analysis/.task4b-recovery/cpython-3.8.10-runtime/bin/python3.8 -B -m unittest tests.test_historical_foundry_scan tests.test_historical_foundry_rpc tests.test_historical_foundry_toolchain tests.test_historical_foundry_contracts -v
Ran 326 tests in 1460.919s
OK (skipped=1)

/Users/luchuanyu/on-chain-market-structure-analysis/.task4b-recovery/cpython-3.8.10-runtime/bin/python3.8 -B -m unittest tests.test_historical_foundry_anvil tests.test_historical_foundry_storage -v
Ran 176 tests in 1551.529s
OK
```

The one exact-runtime skip is an existing runtime-conditional case; discovery
remained 326. Loader audits on both runtimes produced the same module counts:
Anvil 17, storage 159, scan 118, RPC 124, toolchain 48, contracts 36.

Both runtimes successfully compiled every changed Python production/test file
with `py_compile`. `git diff --check` returned no output. No connected gate was
run.

## Changed files

- `scripts/historical_foundry_anvil.py` (new): sealed replay context, fixed
  override, relay server/local RPC limits, fresh-process orchestration,
  complete fork-base header comparison, measured token deltas,
  receipt/trace/revert normalization, scenario deadline, deterministic members,
  and exact proof-at-first-freeze derivation.
- `tests/test_historical_foundry_anvil.py` (new): proportional data-driven
  relay/process/scenario/overlay/quartet/revert/limit/two-repeat offline tests.
- `scripts/historical_foundry_storage.py`: one-scenario transition, sealed sink
  and ledger, quartet validation and no-replace commit, exact caps,
  closed nine-row proof semantics, monotonic quota/generation,
  freeze/reread/closure, and relay final cleanup.
- `tests/test_historical_foundry_storage.py`: native Task-6 authority/cap tests,
  R6.2 lifecycle expectation, and independently derived source golden.
- `scripts/historical_foundry_scan.py`: exact-lineage sealed scenario issuer and
  one-shot storage transition bridge.
- `tests/test_historical_foundry_scan.py`: scenario issuer signature/lineage and
  retained relay-lifecycle fixture behavior.
- `scripts/historical_foundry_rpc.py`: one shared run-scoped relay lease/HMAC
  authority, bounded upstream bridge, storage handoff/final cleanup, and Anvil
  source inventory.
- `tests/test_historical_foundry_rpc.py`: owning-module inventory and sealed
  relay tests.
- `scripts/bootstrap_historical_foundry_toolchain.py`: reviewed fixed-argv,
  fixed-env, private-cwd Anvil process lease with exact TERM/KILL/reap cleanup.
- `tests/test_historical_foundry_toolchain.py`: owning-module spawn/lifecycle
  boundary tests.

## Security and authority boundaries

- Scenario issuance accepts only the exact live staging/window/grid and a key
  already present in that grid; no caller row or mapping is accepted.
- The public Task-6 signature has no endpoint, path, flags, binary, router,
  token, slot, value, key, or secret input.
- One capture-created key/HMAC capability is moved through storage into replay;
  reconnects do not reread the endpoint environment or mint a new key. Final
  owner cleanup zeroes the key.
- The relay binds loopback only, logs nothing, accepts one canonical JSON-RPC
  object, applies the exact nine-method allowlist, authenticates every request,
  and enforces fixed byte/cumulative/deadline limits.
- Anvil is spawned only by `ReviewedHistoricalToolchain` from held reviewed
  binaries, a fixed argument vector/environment, private held working
  directory, and fixed TERM-then-KILL/reap sequence. Dynamic ports and endpoint
  strings are absent from the retained argv projection. Stdout and stderr are
  continuously drained under one combined inclusive 64-KiB cap; overflow
  closes the lease, and cleanup joins the drainers before final validation.
- Sender native funding is derived internally as
  `gas_limit * max_fee_per_gas`; transaction value and executor native balance
  are zero.
- Status-zero evidence is accepted only when exact outer executor failure and
  exact inner selector/full-data hash/router/leg/call path classify as
  `closed_revert`; `second_leg_zero_output` remains unresolved.
- Overlay, receipt, deterministic gzip trace, and result are invisible as a
  committed ledger until all four have been frozen, reread, hash-closed, and
  quota-charged. Replacement and partial successor issuance are rejected.
- Every status-one first freeze contains the complete ordered nine-row proof;
  there is no later augmentation seam for Task 7.
- Errors, reprs, and return projections do not expose raw member bytes, paths,
  endpoint text, relay keys, or process ports.

## Limitations intentionally retained

- Per the Task-6 ingress, all tests are offline and use test-controlled fake
  relay/process leases. The connected fixed-block repeat and live Anvil
  call-tracer compatibility are intentionally deferred to Task 7's connected
  gate; no endpoint was contacted here.
- Task 6 issues/consumes a single scenario and one-scenario ledger only. It does
  not implement descending replay, a winner, a full-ten bundle, selection,
  manifests, publication, deployment, or dashboard behavior.
