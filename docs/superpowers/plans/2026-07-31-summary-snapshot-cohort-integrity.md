# Summary Performance and Snapshot Cohort Integrity Execution Record

**Goal:** Reduce default Summary cold latency and prevent public
depth/execution responses from crossing publication cohorts.

**Status:** Local implementation, contract reconciliation, and local regression
verification are complete. The remaining work is explicitly listed under
`Pending external release gates`; nothing else in this record is an instruction
to rerun historical RED tests, publish data, push, or deploy.

## Binding constraints

- Cross-venue and cross-chain observations are bounded sequential
  observations, never simultaneous observations.
- Missing/unavailable Facts remain `null`; measured zero remains zero.
- `data_generation`, freshness-bucket, and source-fence semantics are
  unchanged.
- Full and exact family publication are failure-atomic only for ordinary
  in-process I/O failures. They are not process-crash atomic or TOCTOU-atomic.
- Funding Rate and all-in fee, gas, and transfer-cost Facts remain out of scope.
- External release actions require the controller's explicit authorization and
  their own recorded evidence.

## Completed implementation record

The commands and expected failures from the original implementation plan were
historical TDD steps. They have been removed so an operator cannot rerun stale
RED commands that expected missing helpers or reuse the obsolete broad
`git add`/single-commit/push recipe.

### Task 1 — Copy-on-overlay Summary optimization: complete

- [x] Added the overlay-safe payload copy boundary.
- [x] Preserved input immutability and shared read-only daily series.
- [x] Routed TVL, CEX-depth, and DEX-depth overlays through that boundary.
- Commit: `f57be27` (`perf(summary): reduce overlay copy cost`).

### Task 2 — Default Summary startup warmup: complete

- [x] Added default serialized Summary warmup before normal serving.
- [x] Preserved source signature, freshness bucket, and generation fences.
- [x] Isolated warmup failures so startup remains diagnosable.
- Commit: `f485e57` (`perf(summary): warm default response at startup`).

### Task 3 — Full/exact family publication: complete

- [x] Full CEX and DEX publication validate depth/execution lineage, scenario
  inventories, and both coverage reports before private or public publication.
- [x] Full publication passes four public destinations to one ordinary-I/O
  failure-atomic bundle per family.
- [x] Full publication rejects resolved private/public destination overlap
  before any write.
- [x] Exact publication checks aligned lineage and complete execution
  scenarios, validates candidate-bound exact-target reports and their
  target/mode/common generation, and seals one target history row to the
  target depth-latest row.
- [x] Exact publication uses the same resolved destination overlap guard before
  any write and the same four-destination public bundle boundary.
- [x] Fault-injection and same/aliased-directory overlap regressions are covered
  for CEX and DEX.
- Commits:
  - `90b3db7` — `fix(data): publish depth execution as one cohort`
  - `406f135` — `fix(data): reject overlapping publication paths`
  - `1b14f2a` — `fix(data): reject exact publication path overlap`

The overlap guard resolves two private and four public paths and compares them
before `mkdir` or write. It does not eliminate a check-to-use race caused by an
unsupported concurrent path/symlink mutation. Public rollback covers ordinary
in-process I/O errors, not process crashes; private current files remain outside
the public rollback boundary.

### Task 4 — Read/release cohort lineage guard: complete

- [x] Added canonical observation bounds and
  `observation_span_seconds` validation.
- [x] Required exactly one equal depth, execution, and execution-source
  snapshot ID per loaded family.
- [x] Bound snapshot/source identity to equal depth/execution Market inventory
  counts.
- [x] Made execution-cost, Quality, and depth-consuming public routes fail
  closed on their applicable invalid cohort boundary.
- [x] Added bounded HTTP 503 handling, degraded health for malformed
  depth-consuming state, and independent release-checker counterexamples.
- [x] Aligned the empty-book fixture with the real same-source failed depth row
  plus ten execution rows.
- Commits:
  - `a4f9b2d` — `fix(api): fail closed on snapshot cohort mismatch`
  - `9434f7e` — `fix(api): validate raw cohort evidence strictly`
  - `cfe35b8` — `test(quality): align empty-book cohort fixture`

### Task 5 — Local documentation and verification: complete

- [x] Documented bounded sequential observation semantics, inventory-bound
  lineage, null preservation, route/health fail-closed boundaries, and the
  ordinary-I/O-only family publication guarantee.
- [x] Listed the four public bundle destinations and two private current
  destinations separately for CEX and DEX.
- [x] Recorded that both full and exact publication perform the resolved-path
  overlap guard before any write, without claiming crash or TOCTOU atomicity.
- [x] Added this implementation record to the repository.
- Initial documentation commit: `5f97669`
  (`docs(data): document cohort and summary guarantees`).
- Contract reconciliation: the follow-up commit containing this execution
  record; its SHA is intentionally obtained from Git rather than self-embedded.

## Local verification evidence

- Fresh complete local suite at `1b14f2a`: 778 tests, 0 failures, 0 errors.
- Python 3.8 grammar gate passed under the local compatibility test.
- Local Python 3.13.5 py_compile/import checks passed for the changed
  production modules and related tests.
- Diff whitespace checks passed for the completed commits.
- The only local cold/warm measurement used a 1-Token/3-Market QA fixture. It
  is retained as a development measurement only and is not production-scale
  evidence or a substitute for the required 493-Market benchmark.

## Pending external release gates

These are the only incomplete items in this execution record:

- [ ] Run compile and import preflight with the actual production Python
  3.8.10 interpreter.
- [ ] Run a fresh-process cold build and immediate warm default Summary call
  against the production 493-Market snapshot; record unmodified latency,
  response bytes, and `data_generation` equality without a machine-dependent
  pass/fail threshold.
- [ ] Run the release checker against one stable production generation before
  and after cutover.
- [ ] Push the currently unpushed documentation, exact-overlap, and follow-up
  reconciliation commits, then add the required commit comments with test and
  benchmark evidence. Do not collapse them into the obsolete historical
  single-commit recipe.
- [ ] Use an isolated production preflight worktree/service, perform cutover,
  verify public desktop and mobile behavior plus health, and remove the
  temporary preflight resources only after successful verification.

No item above was executed as part of the local documentation fix round.
