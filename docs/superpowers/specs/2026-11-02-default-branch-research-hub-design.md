# Default-branch research hub: design A

Date: 2026-11-02, Asia/Hong_Kong
Status: approved by the user; implementation plan in preparation

## Intent

Make the default GitHub branch a usable entry point to the market-data application,
V3 exact pricing, SHIB V2 research, Foundry replay, and the November 2 UNI CEX/V3
experiment. Rewrite English and Chinese homepages from the user-supplied
`CEX_DEX_README_technical_rewrite.md`, using direct technical descriptions,
source links, dated results, and reproduction commands.

The user selected approach A: a unified entry point with version-isolated research
modules. This document specifies that approach before implementation.

## Approaches considered

- **A, selected:** put the existing application baseline on `main`, preserve the
  original V3 and SHIB implementations in isolated modules, and add a common
  research entry point. This preserves each workstream's dependency context.
- **B:** merge V3, SHIB, and the application into one shared implementation and
  dashboard runtime. This involves resolving overlapping collectors, quantity
  engines, API changes, and release contracts. It is a separate integration task.

## Source authorities

| Component | Exact source |
| --- | --- |
| Current `main` parent and bilingual landing page | `7f1cae5c920090e27a749e62d3655910923356b0` |
| Application and Foundry baseline | `83e66079f679c8133c6a6415238d27377d42293b` (`codex/historical-foundry-opportunity`) |
| V3 exact module | `401eaad12965560ffd67e60caad60d2a267e206d` (`codex/v3-exact-swap-mvp`) |
| SHIB module | `b16ee24d79806a0de655b28caa3ce6f9d248ba1e` (`codex/shib-v2v2-research-loop`) |
| November experiment | Existing local scripts, tests, report, and `data/local/uni-cex-v3-20261102/`, identified by file SHA-256 |

The existing worktree contains unfinished experiment and documentation changes.
Preserve them. Implementation uses a separate `codex/` integration branch and
isolated worktree, retaining the current remote `main` as the publication parent.

## Target layout

```text
readme.md / README.zh-CN.md        Bilingual GitHub landing pages
dashboard/, scripts/, config/    Application baseline
foundry/                         Existing UNI/WETH Uniswap/SushiSwap replay harness
research/
  v3_exact/                      Version-isolated V3 source tree
  shib_v2v2/                     Version-isolated SHIB source tree
  sources.json                   Commit and file-hash provenance index
experiments/
  uni_cex_v3_20261102/
    README.md                    Experiment design, dataset, and reproduction guide
    capture/                     Reviewed source capture and required raw evidence
    results/                     Published reference analysis
    FILES.sha256                 Published file-integrity inventory
docs/
  research-index.md              Reading order and implementation map
  source-readmes/                Preserved source overview documents
scripts/run_research.py           Unified offline research command entry point
```

### Isolated modules

Preserve the complete tracked source tree from each of the two module commits,
including its scripts, configuration, tests, public fixtures, and operation files.
The source snapshots are approximately 9.4 MB and 9.8 MB before compression.
Git stores reused blob contents by identity; no duplicate runtime market data is
added from the original ignored directories.

Each module receives a short navigation README. Its original README is preserved
as `SOURCE_README.md`. `sources.json` records source commit, original path,
published path, mode, and content hash, plus an explicit list of added navigation
or package-helper files. Source file contents remain byte-identical.

Commands execute in subprocesses with the selected module as their working
directory and an explicit import context. The V3 module imports its own collector
and quantity implementation; the SHIB module imports its own contracts and
quantity implementation. Ambient `PYTHONPATH` is excluded from module launches.

The Foundry source and its existing toolchain configuration remain at the root
with the application baseline. Its README links the harness, tests, bootstrap
configuration, and original source SHA. Git submodule pins retain their recorded
Git identities; external toolchain installation is an explicit operator step.

## Experiment publication

Publish the existing collection and analysis scripts with their dependency
helpers and tests against the root application baseline. Publish the experiment
report and the full necessary retained capture after a file-by-file privacy and
credential review. The retained capture contains 329 files and is approximately
10 MB on disk.

Preserve the raw public market/RPC payload bytes and the sample-to-response hash
links. Review metadata for local paths, credentials, authentication headers,
private endpoint parameters, and unrelated data. If a metadata transformation is
needed, record the transformation and source/published hashes explicitly. A
required raw artifact containing credentials blocks publication of that artifact
until its dependency-preserving public representation has been designed.

The experiment's public evidence describes:

- 60 polls, including 9 duplicate-head polls;
- 51 distinct accepted blocks and 510 DEX quotes;
- two directions, five initial size anchors, six CEX fee assumptions, seven Gas
  assumptions, and 21,420 fee/Gas cash-flow rows;
- exact +1/+2/+3 block delay matching and actual elapsed time;
- the sampled zero-positive result and the best recorded cash-flow examples.

Separate published input capture from reference analysis and regenerated output.
The replay command defaults to a new directory under ignored `data/local/` and
accepts an explicit new output directory. It verifies capture hashes, market
identity, normalized books, quote calldata, raw Quoter returns, fixed quantities,
and fee evidence before recomputing results. Replay performs no network requests
and does not overwrite the committed capture or reference results.

The original two-pool/20-scenario canary receipt is not present in the inspected
local workspaces. The evidence index records its availability separately from
the November single-pool experiment. README results cite the evidence actually
published with this integration.

## Unified entry point

Provide a small fixed command dispatcher, with commands for:

1. Listing components and reproduction commands.
2. Running isolated V3 math tests.
3. Rebuilding the SHIB public snapshot from the module's retained evidence.
4. Replaying the November UNI/V3 capture into a separate local output directory.
5. Starting the existing synthetic historical demonstration explicitly.

The dispatcher contains no arbitrary shell-command interface. Paths and module
choices are fixed in source; an output path is validated before creating files.
Live collectors keep their existing explicit command entry points.

## README treatment

- Keep the website `http://43.156.102.166:8765` at the top of both versions.
- Retain reciprocal English/Chinese links.
- Follow the supplied technical draft: motivation, architecture, depth and
  execution math, V3 integer steps, Quoter parity, matched-quantity cash flows,
  dated results, implementation map, reproduction, and next research tasks.
- Replace defensive scope callouts and repeated non-claim paragraphs with
  concrete source methods, input timestamps, fee assumptions, status fields,
  evidence links, and forward-looking milestones.
- Explain the directional V3 segment formulas with consistently defined input
  and output amounts, then distinguish those relations from integer execution.
- Describe actual Foundry code behavior from its source, including its current
  swap parameters, rather than inheriting assumptions from the screenshot.
- Label the July scale figures as the July release snapshot. Lead the new
  experiment section with the published November code and evidence.
- Use default-branch-relative source links for implementations now present in
  the tree, with pinned historical links in the provenance table.

## Verification and acceptance

1. Preserve source bytes and executable modes; validate the provenance inventory.
2. Red/green tests for dispatcher isolation, output handling, and offline replay.
3. Run packaged V3 math tests and relevant V3 module tests; report actual scope.
4. Run the SHIB tests and rebuild its public snapshot byte-for-byte with its
   recorded application SHA.
5. Run the November experiment/analyzer regression tests and complete offline
   replay; compare regenerated scenario inventories, cash flows, and summaries
   with the retained reference outputs.
6. Demonstrate that offline replay creates no HTTP/RPC requests and preserves
   the published input/reference file hashes.
7. Verify bilingual links, GitHub Markdown rendering, source paths, equations,
   scenario counts, and matching reproduction commands.
8. Inspect the exact publication diff for credentials, unintended runtime files,
   unrelated worktree edits, and accidental production configuration changes.
9. Publish the tested integration branch and integrate into `main` through a
   normal fast-forward/merge, preserving any concurrent remote change. Verify
   the final default-branch tree and file hashes via GitHub after publication.

## Scope and authority

Authorized changes: repository organization, necessary reproduction tooling,
public research code/evidence, and bilingual documentation. Existing branches
remain available with their original history. Production services, timers,
server checkouts, wallet actions, and order submission are outside this change.

## Choices left to the builder

Choose the exact CLI option spelling, focused test grouping, integration branch
name, documentation formatting, and CI command selection. Preserve the layout,
source identities, import isolation, offline replay behavior, and publication
checks specified above. Resolve routine dependency inclusion from source rather
than requesting additional product decisions.
