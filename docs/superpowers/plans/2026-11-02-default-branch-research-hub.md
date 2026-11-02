# Default-branch Research Hub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Publish the application, version-isolated V3/SHIB research, and reproducible November 2 experiment through the default branch with technical English/Chinese READMEs.

**Architecture:** Import the existing application/Foundry baseline and preserve V3 and SHIB source trees in isolated research directories. A fixed offline command dispatcher selects subprocess contexts, while a separate replay utility verifies the published capture and writes regenerated analysis to a new local output directory.

**Tech Stack:** Python standard library, Git, SQLite, existing Foundry configuration, GitHub API/CLI, Markdown.

**Spec:** `docs/superpowers/specs/2026-11-02-default-branch-research-hub-design.md`

## Global Constraints

- Main publication parent: `7f1cae5c920090e27a749e62d3655910923356b0`, refreshed before publication.
- Root application/Foundry source: `83e66079f679c8133c6a6415238d27377d42293b`.
- V3 source: `401eaad12965560ffd67e60caad60d2a267e206d`.
- SHIB source: `b16ee24d79806a0de655b28caa3ce6f9d248ba1e`.
- Preserve current worktree changes and original branches. Use an isolated `codex/` integration branch.
- Preserve source bytes, source executable modes, Git submodule pins, and raw-data hash links; record navigation/helper overrides explicitly.
- V3/SHIB subprocesses use their own source trees and exclude ambient `PYTHONPATH`.
- Offline replay preserves published inputs/reference results and creates a separate new output directory.
- Both READMEs retain `http://43.156.102.166:8765`, reciprocal language links, and matching reproduction commands.
- Describe source methods, parameters, dated results, and future tasks directly; remove defensive scope callouts from the homepages.
- Scope is repository publication and reproduction tooling; production services and data remain unchanged.

## Review Focus

- Dirty originating worktree: preserve its tracked/untracked bytes while importing explicit source versions elsewhere; Task 1.
- Namespace collision or hostile ambient Python import path: dispatch into the selected module's own context; Task 2.
- Output paths overlapping capture, results, source tree, or existing directories: reject before creating/writing output; Task 3.
- Tampered or incomplete raw evidence and repeated/regressed blocks: reject invalid input and preserve the existing observation gates; Task 3.
- Concurrent remote `main` changes: refresh and preserve remote history, publish without force, and inspect the final tree; Task 5.

---

### Task 1: Materialize version-isolated source modules with provenance

**Files:**
- Import: application tree at the root, `research/v3_exact/`, `research/shib_v2v2/`.
- Create: `research/sources.json`, `scripts/verify_research_sources.py`, `tests/test_research_sources.py`.
- Preserve source overview documents under `docs/source-readmes/` and module `SOURCE_README.md`.
- Carry approved design and this plan into the integration checkout.

**Interfaces:**
- Consumes: the three pinned Git source trees and the current remote-main commit.
- Produces: `verify_sources(root: Path, manifest: dict) -> dict` with checked file/mode counts; manifest records `component`, `source_commit`, `source_path`, `published_path`, `sha256`, and Git mode for every imported source blob, plus declared overrides.

- [x] Read using-git-worktrees at execution time; establish an isolated integration branch from current remote `main`. Record originating worktree status and hashes of its changed files.
- [x] Write `test_all_source_files_have_unique_safe_paths`, `test_hash_change_is_rejected`, `test_executable_mode_is_preserved`, and `test_declared_readme_relocation_keeps_original_hash` using small temporary source inventories. Assert changed bytes and `../` paths raise a controlled error.
- [x] Run the new tests and record the expected failure before the verifier exists.
- [x] Implement `verify_sources`; mechanically import the pinned Git trees. Preserve full original module trees, put original overview files at declared source-readme paths, and add only declared navigation/helper overrides.
- [x] Preserve root `lib/forge-std` as the original Gitlink and `.gitmodules` configuration. Verify all imported tree entries against the source trees, including modes and Gitlink SHA.
- [x] Run provenance tests and the complete provenance verifier. Recheck the originating worktree hashes.
- [x] Commit the source organization and provenance tooling with explicit paths only.

### Task 2: Add a fixed unified research dispatcher

**Files:**
- Create: `scripts/run_research.py`, `tests/test_run_research.py`.
- Create: navigation READMEs under `research/` and `foundry/README.md`.

**Interfaces:**
- Consumes: module directories from Task 1 and Task 3's replay CLI path.
- Produces: `main(argv: Optional[Sequence[str]] = None) -> int`; fixed commands `list`, `v3-math`, `shib-replay`, `uni-v3-replay`, and `historical-demo`.
- CLI: `python3 scripts/run_research.py uni-v3-replay --output-dir <new-path>`; default output is a unique new directory under root `data/local/`.

- [x] Write tests asserting `list` performs no subprocess launch, unknown commands are rejected, `v3-math` uses the V3 module cwd, and SHIB replay reads its module's published evidence and recorded application SHA.
- [x] Add a poison `PYTHONPATH` fixture and subprocess regression asserting module execution resolves its own `scripts` package. Test that a child nonzero exit is propagated.
- [x] Run dispatcher tests and record RED.
- [x] Implement argparse-based fixed dispatch, argument-list subprocess execution, module cwd selection, and exclusion of ambient `PYTHONPATH`/`PYTHONHOME`. Keep live collection as separate existing commands.
- [x] Implement SHIB replay to a separate temporary/local output; verify generated bytes against the module's retained snapshot and print the result.
- [x] Run dispatcher tests, isolated V3 math tests, and the SHIB replay command. Record actual counts and output hashes.
- [x] Commit dispatcher, focused tests, and component navigation.

### Task 3: Publish and reproduce the real UNI/V3 experiment

**Files:**
- Import current experiment code: `scripts/uni_cex_v3_experiment.py`, `scripts/analyze_uni_cex_v3_experiment.py`, `scripts/uni_experiments.py`, `scripts/analyze_uni_experiments.py`.
- Import current focused tests and the reviewed fee schedule.
- Create: `scripts/replay_uni_cex_v3.py`, `tests/test_replay_uni_cex_v3.py`.
- Create: `experiments/uni_cex_v3_20261102/{README.md,capture/,results/,FILES.sha256,evidence-index.json}`.
- Publish current report under `docs/uni-cex-v3-experiment-2026-11-02.md`.

**Interfaces:**
- Consumes: retained `data/local/uni-cex-v3-20261102/` and existing `verify_capture`, `analyze`, and `write_csv` functions.
- Produces: `replay_capture(capture_dir: Path, reference_dir: Path, output_dir: Path) -> dict`; result reports capture/hash checks, independent block/quote counts, scenario row counts, and reference comparison.
- CLI: `python3 scripts/replay_uni_cex_v3.py --output-dir <new-path>` with tracked experiment defaults and optional explicit capture/reference directories for tests.

- [x] Inventory retained evidence and scan every publication candidate for credentials, private endpoints, machine-specific metadata, and unrelated data. Record any required metadata transformations in the evidence index; preserve required raw response bytes/hash links.
- [x] Separate original `analysis/` into committed `results/`; put remaining required capture files under `capture/`. Generate file-integrity inventory excluding itself and generated local replay output.
- [x] Write tests for existing/nonempty output, capture/results/source-tree overlaps, symlink aliases, and parent output paths; assert rejection occurs before any writes.
- [x] Write offline replay tests that deny socket connections, verify input/reference hashes remain unchanged, and reject tampered/missing evidence. Reuse existing repeated/regressed-head and exact-delay tests.
- [x] Run new replay tests and record RED.
- [x] Implement `replay_capture` by reusing pure verifier/analysis functions; create a new output directory only after input verification, write analysis there, and compare regenerated rows/summaries against the reference results. Cleanly report a failed comparison without changing committed data.
- [x] Run experiment/analyzer/replay tests and full retained-data offline replay. Assert 60 polls, 51 accepted distinct blocks, 510 quotes, 21,420 fee/Gas rows, 13,770 delay rows, and zero positive fee/Gas rows. Compare exact cash-flow strings and stable summary values, not just counts.
- [x] Commit audited experiment code, evidence, report, integrity manifest, and replay utility.

### Task 4: Rewrite bilingual technical homepages and research navigation

**Files:**
- Modify: `readme.md`, `README.zh-CN.md`.
- Create: `docs/research-index.md`, `tests/test_research_docs.py`.
- Add dated verification report to `docs/` after the final commands run.

**Interfaces:**
- Consumes: actual paths, provenance, CLI commands, and evidence/results from Tasks 1–3.
- Produces: relative default-branch source links, matching runnable commands, and evidence-backed English/Chinese explanations based on the supplied technical rewrite.

- [x] Write tests for reciprocal language links, website URL, local file/anchor resolution, identical fenced shell commands, exact experiment arithmetic, and the absence of defensive scope-callout text.
- [x] Run documentation tests and record RED against the old landing pages.
- [x] Rewrite both homepages: overview, dated July scale, architecture, CEX/V2/V3 methods, integer rounding, same-block parity fields, cash-flow equations, published November results, implementation map, reproduction, and next tasks.
- [x] Use a directional V3 segment example with explicitly named input/output amounts; explain Q96 and token decimals. Link Foundry behavior to actual source parameters. Put the historical missing-canary availability in the evidence index rather than inventing receipts.
- [x] Run documentation tests and GitHub Markdown rendering for both languages. Check source links against the assembled default-branch tree, not old branch names alone.
- [x] Commit bilingual documentation and source navigation.

### Task 5: Verify, independently review, and publish the assembled branch

**Files:**
- Update: `docs/research-hub-verification-2026-11-02.md`, plan checklist.

**Interfaces:**
- Consumes: complete integration branch and recorded source identities.
- Produces: a tested default-branch tree with a published commit URL and precise verification record.

- [x] Run source-provenance verification, all new hub/replay/documentation tests, relevant root quantity/fee/experiment regressions, V3 module tests, and SHIB module tests. Run complete offline experiment replay and byte-identical SHIB snapshot rebuild.
- [x] Verify published input/reference hashes before and after reproduction. Check import isolation and current originating-worktree hashes. Report full-suite results only if the full suite is actually run.
- [x] Request one independent whole-branch review covering module boundaries, publication privacy, reproducibility, README accuracy, and unexpected files. Resolve actionable findings and rerun affected verification.
- [x] Inspect `git diff --check`, final file inventory, per-file size, source modes/Gitlinks, and credential scan. Ensure the publication contains only the approved repository organization and evidence.
- [x] Refresh remote `main`. If it advanced, preserve and integrate the concurrent changes and repeat affected tests; never force-push over them.
- [ ] Push the integration branch and merge/fast-forward the tested result into `main` using normal GitHub workflow under the user's publication authorization.
- [ ] Read the final default-branch tree and selected file hashes through GitHub; confirm both READMEs, module entry points, experiment capture/results, and provenance files are present.
- [ ] Report main commit, website/source links, reproduction commands, and actual verification counts.

## Self-review

Every design section maps to Tasks 1–5. Tests explicitly cover all five Review
Focus conditions. Module source identities are independent from newly generated
hub helpers; reference input and regenerated output paths remain separate.
Existing production deployment and the originating worktree are preserved.
