# Task 5 report — staged exact V3 launch and rollback

## Status

Complete. The scoped implementation is committed with the exact message
`feat(deploy): add staged V3 launch and rollback`; the final commit SHA is
reported in the handoff because a commit cannot contain its own SHA.

No push, deployment, public-data publication, live collection, production
systemd action, application-pointer switch, SSH session, or live RPC access was
performed.

## TDD evidence

1. **Filesystem RED:** The launch test module was written before the launch
   script and failed to import `scripts.uniswap_v3_launch`. The first GREEN
   implementation covered the fixed five-file manifest, bounded descriptor
   reads, private backup, fresh isolated staging, CAS promotion, and restore.
2. **Orchestration RED:** Eight command/ledger tests initially failed because
   `LaunchConfig` and the phase executor did not exist. They became GREEN after
   adding dry planning, fixed systemd-unit management, flock ownership, staged
   collection, transient verification, receipt chaining, promotion, rollback,
   and resume.
3. **Adversarial RED/GREEN rounds:** Regression tests first exposed and then
   fixed candidate mutation during Task 4 revalidation, promoted-generation
   drift during restore, required-input drift after preflight, partial pause
   failure, backup-manifest metadata tamper, and a completed forward-resume
   ledger that could otherwise be followed by a late restore.
4. **Trusted-receipt RED/GREEN:** Tests were added first for a missing or
   tampered staged private receipt, live collision, forward-promotion cleanup,
   live-root forward health, rollback removal versus preservation, and trusted
   receipt drift. The launch now installs only the validated canonical private
   receipt into the matching live raw snapshot path.

## Implemented boundaries

- The public generation is exactly, and in order,
  `dex_depth_history.csv`, `dex_depth_latest.csv`,
  `dex_depth_snapshot.csv`, `dex_execution_cost_latest.csv`, and
  `uniswap_v3_exact_latest.json`. The initial missing sidecar is represented as
  `{\"exists\":false}` and is never backed up as an empty file.
- All public, backup, receipt, and trusted-receipt reads use lstat plus a
  descriptor identity check, reject symlinks and nonregular files, and enforce
  size bounds. Canonical receipts and manifests bind SHA-256, size, mode, and
  presence. Launch/stage/backup directories are private; exclusive files are
  written with `O_EXCL`, mode `0600`, file fsync, and directory fsync.
- Staging requires a fresh nonexisting sibling data root and its distinct
  processed sibling. Only the database, DEX daily input, and depth history are
  copied; staged raw and processed outputs are new. Root device/inode/hash
  bindings and live baseline/input CAS checks prevent alias and drift.
- The stage invokes the full unfiltered `dex_depth` profile with local publish
  and the exact-validation flag. Transient dashboard verification overrides
  the staged depth, execution, sidecar, and mandatory staged raw root while
  retaining the normal target-SHA release checker.
- Every live-reading or live-mutating phase holds the live collection flock for
  the phase's critical work and revalidates the recorded paused timer/service
  state. Only the fixed daily/depth timers and matching services are managed,
  with exact enabled/active state restored after verified forward or rollback
  evidence.
- Promotion reruns Task 4 raw/public validation, snapshots exact staged bytes,
  installs or validates only the matching canonical private raw receipt, then
  uses the existing `atomic_replace_bundle` for the five forward public bytes.
  A public promotion failure removes only a launch-created trusted receipt.
- Restore CAS-validates both the exact promoted public generation and trusted
  receipt. A private replace-or-remove transaction restores all five public
  files, supports an initially absent sidecar, removes a launch-created trusted
  receipt, preserves a byte-identical preexisting receipt, and rolls back every
  completed replace/remove after ordinary I/O failures.
- Canonical `0600` phase receipts form an exclusive predecessor-SHA ledger and
  reject replay, reorder, tamper, SHA mismatch, stage drift, live drift, and
  late rollback after resume. Receipt content rejects absolute paths,
  environment, RPC, URL, password, and secret metadata.
- The default CLI only emits a redacted plan. It does not create directories,
  write files, invoke systemd, start a dashboard, run collection, publish, or
  access the network. Application and Git pointer switches remain an explicit
  external operator boundary.

## Negative and failure coverage

The 35 launch tests cover modes, bytes, presence, absent sidecar, symlinked
roots/files, FIFO/special files, bounded reads, fresh-root isolation, raw and
processed root binding, baseline and required-input drift, candidate mutation,
private receipt absence/tamper/collision, public promotion cleanup, trusted
receipt drift, byte-identical preservation, restore CAS at the transaction
boundary, and injected failure at each of the five public replace/remove points
plus trusted-receipt removal. Orchestration tests cover exact commands and
environment, live flock ownership, fixed systemd states, partial pause recovery,
canonical predecessor receipts, replay/order/tamper rejection, target and
previous-SHA release evidence, live-root trusted SHA equality, timer restoration,
privacy, and default zero side effects.

## Final verification

- `python3 -m unittest tests.test_uniswap_v3_launch -q`: 35 tests passed.
- Launch, Task 4 exact publication/sidecar, atomic publication, collection
  runner, dashboard, and release focus with `PYTHONPATH=tests:.` and bundled
  Node: 313 tests passed.
- Full discovery with `PYTHONPATH=tests:.` and bundled Node: 1,598 tests passed
  with no skips.
- Python 3.8 AST parsing passed for the launch script and its tests. `py_compile`
  and repository `compileall` passed.
- `git diff --check`, exact-path scope audit, and secret/absolute-path/forbidden
  command scans passed.
- Final self-review found the late-restore ledger edge described above. Its
  targeted test failed before the fix, all 35 launch tests passed afterward,
  and the focused and full suites were rerun on that final tree.

## Files

- `scripts/uniswap_v3_launch.py`
- `tests/test_uniswap_v3_launch.py`
- `docs/production-hardening.md`
- `docs/collection-operations.md`
- `.superpowers/sdd/2026-08-28-uniswap-v3-minimal-production-deploy/task-5-report.md`

## Concerns and manual production evidence

The code is Python 3.8 compatible but its execution path is Linux-specific:
`fcntl.flock`, descriptor no-follow support, and user-level systemd are required.
The restore transaction promises rollback for ordinary I/O exceptions only; it
does not claim multi-file crash consistency or power-loss atomicity.

Actual production unit state, cgroup/OOM evidence, dashboard stop/start,
external application-pointer switching, public-browser smoke, live release
evidence, and the observation window remain manual operator work. Backups,
launch receipts, the live private receipt, and the complete staged raw/TVL and
processed evidence must remain under the documented retention hold. No local
test fabricates any of that server-dependent evidence.
