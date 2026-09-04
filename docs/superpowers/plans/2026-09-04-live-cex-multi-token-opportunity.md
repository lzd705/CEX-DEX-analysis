# Live CEX Multi-Token Opportunity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the real, read-only live CEX Opportunity workflow from UNI-only to the reviewed UNI+CAKE inventory while publishing isolated terminal rows when one token's source is unavailable.

**Architecture:** A sealed two-pair universe derives all market, route, and scenario counts. Standard routes retain the existing exact book/rule replay; routes proven terminal by the retained cohort use a separate null-economics build input that is reconstructed and validated by the complete publisher and dashboard loader.

**Tech Stack:** Python 3.8-compatible standard library, `Decimal`, existing immutable route cohort/publication contracts, `unittest`, local loopback smoke checks.

**Spec:** `docs/superpowers/specs/2026-09-04-live-cex-multi-token-opportunity-design.md`

## Global Constraints

- Reviewed pairs are exactly `UNI/USDT` and `CAKE/USDT`; venues are exactly Binance and Bybit.
- Requested notionals remain exactly 1,000, 5,000, 10,000, 50,000, and 100,000 USD.
- The CLI gains no token, venue, URL, host, proxy, credential, wallet, order, transfer, or RPC input.
- Every result remains `research_estimate` or `unavailable`, with `strict_eligible=false`, `strict_ready_for_publication=false`, and no attestation.
- Missing CAKE fee evidence stays terminal; no UNI rate, wildcard, cached value, synthetic input, or numeric zero substitutes for it.
- A route with terminal retained timing may have a null target only through the explicit terminal-input contract; normal paths retain all current positive-quantity and replay checks.
- A collector-wide exception, corrupt source lineage, or publication/cold-reload failure preserves the prior complete pointer.
- Tests do not contact public hosts; the real acceptance run uses only existing fixed Binance/Bybit adapters.
- Every production change follows RED, GREEN, focused regression, review, commit, and push.

---

### Task 1: Sealed UNI+CAKE universe and derived runner inventory

**Files:**
- Modify: `scripts/live_cex_research.py`
- Modify: `scripts/run_live_cex_opportunity.py`
- Modify: `scripts/route_opportunity_pipeline.py`
- Modify: `tests/test_live_cex_research.py`
- Modify: `tests/test_run_live_cex_opportunity.py`
- Modify: `tests/test_route_opportunity_pipeline.py`

**Interfaces:**
- Produces: `LIVE_CEX_RESEARCH_PAIRS = (("UNI", "UNI/USDT"), ("CAKE", "CAKE/USDT"))`.
- `build_live_cex_research_universe()` produces four legs and four same-token routes.
- `live_cex_research_generation()` returns `2b473d16979914513eb60843c0c3574141b01ba0f0d193628aa54d62c101bb9b`.
- Produces a v2 receipt with plural `token_pairs` and derived market/route/opportunity counts.

- [ ] **Step 1: Write failing universe and runner tests**

Assert the literal market order:

```python
[
    "cex:binance:UNI/USDT",
    "cex:bybit:UNI/USDT",
    "cex:binance:CAKE/USDT",
    "cex:bybit:CAKE/USDT",
]
```

Assert the four sorted route IDs, five notionals, no route whose two market
IDs contain different instruments, and the literal generation above. Update
the runner happy-path fixture to four legs/four timing rows and assert this
receipt shape:

```python
{
    "schema": "live_cex_opportunity_refresh/v2",
    "status": "published",
    "token_pairs": ["UNI/USDT", "CAKE/USDT"],
    "venues": ["binance", "bybit"],
    "market_count": 4,
    "route_count": 4,
    "route_cohort_id": COHORT_ID,
    "manifest_sha256": COMPLETE_MANIFEST_SHA256,
    "opportunity_count": 20,
    "strict_eligible_count": 0,
    "served": False,
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_live_cex_research.LiveCexResearchUniverseTests \
  tests.test_run_live_cex_opportunity.LiveCexOpportunityOrchestrationTests -v
```

Expected: FAIL on UNI-only markets, routes, generation, and literal runner
counts.

- [ ] **Step 3: Implement the fixed multi-token universe and derived checks**

Build legs by iterating the ordered reviewed pair inventory, then venues. Build
routes only inside each token/pair group and sort routes by `route_id`. In the
runner, derive expected IDs and counts from the rebuilt sealed universe at the
start of the refresh; reject duplicates and any cold-loaded scenario count
other than `route_count * notional_count`. In the public finalizer, compare the
exact `{market_id: token_symbol}` map and exact routes with the sealed universe;
remove the UNI-only token-set assertion.

- [ ] **Step 4: Verify GREEN and focused regressions**

Run:

```bash
python3 -m unittest \
  tests.test_live_cex_research \
  tests.test_run_live_cex_opportunity \
  tests.test_route_opportunity_pipeline.PublicCexResearchFinalizerTests -v
```

Expected: PASS with twenty happy-path scenarios and no strict row.

- [ ] **Step 5: Commit and push**

```bash
git add scripts/live_cex_research.py scripts/run_live_cex_opportunity.py scripts/route_opportunity_pipeline.py tests/test_live_cex_research.py tests/test_run_live_cex_opportunity.py tests/test_route_opportunity_pipeline.py
git commit -m "feat(opportunities): expand live CEX token inventory"
git push origin codex/historical-foundry-opportunity
```

### Task 2: Explicit terminal opportunity contract

**Files:**
- Modify: `scripts/execution_cost_components.py`
- Modify: `scripts/route_opportunity.py`
- Modify: `scripts/route_publication.py`
- Modify: `dashboard/opportunity_facts.py`
- Modify: `tests/test_execution_cost_components.py`
- Modify: `tests/test_route_opportunity.py`
- Modify: `tests/test_route_publication.py`
- Modify: `tests/test_opportunity_api.py`

**Interfaces:**
- Produces: `build_terminal_route_opportunity(*, cohort_id, route, requested_notional_usd, buy_leg, sell_leg, route_timing, cost_components, mode_evidence, now, core_manifest_sha256) -> Dict[str, Any]`.
- Adds terminal `build_inputs` with `input_kind="terminal_route"` and no quote/projection fields.
- A cost `target_token_quantity` may be `None` only when its status is one of `unavailable`, `unsupported`, `failed`, or `stale` and every row in that scenario also has a null target.

- [ ] **Step 1: Write failing cost and terminal-opportunity tests**

Add a cost-component test proving a three-row CEX terminal topology accepts
null targets only when all rows are terminal. Mutations to a positive-status
row, numeric amount/rate, strict flag, or mixed null/non-null targets must fail.

Add a route-opportunity test using one failed CAKE leg and retained timing
reason `sell_leg_unavailable`. Assert the exact terminal null shape, three
terminal costs, non-strict classification, canonical opportunity ID, core hash
on both legs, cost-set hash, and evidence-binding hash. Mutate the timing reason,
route identity, target, state, economics, and attestation one at a time and
assert rejection.

- [ ] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_execution_cost_components \
  tests.test_route_opportunity.TerminalRouteOpportunityTests -v
```

Expected: FAIL because null targets and the terminal builder are unsupported.

- [ ] **Step 3: Implement null-target terminal costs and builder**

Keep `cost_component_row()` positive-target behavior unless the caller passes
`None` with a terminal status. Make `validate_cost_components()` reject any
mixed target representation inside one opportunity. Implement the terminal
builder by validating the canonical route, recomputing timing with
`classify_route_timing()`, requiring non-`within_sla`, validating the exact CEX
cost topology, hashing the costs and mode evidence, setting every source/economic
field specified by the design to null, and hashing the final row.

- [ ] **Step 4: Write failing publisher and dashboard replay tests**

Create an explicit terminal input with empty `source_members`. Assert the
complete publisher reconstructs it and cold-loads it. Assert rejection for a
standard input with null target and for terminal inputs with nonempty source
members, a `within_sla` timing row, nonterminal costs, different core legs,
numeric economics, a state ID, or an attestation. Add the same malformed bundle
cases at the dashboard loader boundary.

- [ ] **Step 5: Verify publisher/dashboard RED**

Run:

```bash
python3 -m unittest \
  tests.test_route_publication.TerminalOpportunityPublicationTests \
  tests.test_opportunity_api.TerminalOpportunityBundleTests -v
```

Expected: FAIL because the publisher recognizes only quote-backed inputs and
the API validator requires positive component targets.

- [ ] **Step 6: Implement terminal replay in publisher and loader**

Add a terminal build-field set alongside the unchanged standard field set.
Dispatch `_validated_prepublication_input()` by the explicit terminal kind and
rebuild with `build_terminal_route_opportunity()`. Require exact core route,
legs, timing, notional, and manifest lineage. In typed-source validation require
`source_members == {}` for terminal scenarios; skip only their quote-generation
record. Extend complete-bundle and dashboard logical validation with one narrow
terminal-null branch; standard rows keep every existing check.

- [ ] **Step 7: Verify GREEN and regressions**

Run:

```bash
python3 -m unittest \
  tests.test_execution_cost_components \
  tests.test_route_opportunity \
  tests.test_route_publication \
  tests.test_opportunity_api -v
```

Expected: PASS; historical and strict quote-backed paths remain unchanged.

- [ ] **Step 8: Commit and push**

```bash
git add scripts/execution_cost_components.py scripts/route_opportunity.py scripts/route_publication.py dashboard/opportunity_facts.py tests/test_execution_cost_components.py tests/test_route_opportunity.py tests/test_route_publication.py tests/test_opportunity_api.py
git commit -m "feat(opportunities): publish isolated terminal routes"
git push origin codex/historical-foundry-opportunity
```

### Task 3: Pipeline isolation, mixed-token UI behavior, and operations text

**Files:**
- Modify: `scripts/route_opportunity_pipeline.py`
- Modify: `scripts/run_live_cex_opportunity.py`
- Modify: `tests/test_route_opportunity_pipeline.py`
- Modify: `tests/test_run_live_cex_opportunity.py`
- Modify: `tests/test_opportunity_api.py`
- Modify: `tests/test_opportunity_frontend.py`
- Modify: `tests/test_current_opportunity_dashboard.py`
- Modify: `README.md`
- Modify: `docs/collection-operations.md`
- Modify: `docs/superpowers/specs/2026-09-04-live-cex-opportunity-design.md`

**Interfaces:**
- `_load_cex_sources()` loads only `observed`/`partial` legs and requires exact equality with the eligible raw/typed member set.
- `_build_public_cex_research_inputs()` emits standard inputs for `within_sla` routes and terminal inputs for every other retained timing row.
- `_collection_is_publishable()` accepts a closed four-leg/four-route cohort with terminal leg/timing outcomes, while still rejecting missing identities, duplicate identities, unknown statuses, and timing inconsistent with the routes.

- [ ] **Step 1: Write failing pipeline and runner isolation tests**

Build a valid four-market core in which `cex:bybit:CAKE/USDT` is failed and
both CAKE timing rows carry the corresponding buy/sell unavailable reasons.
Assert finalization returns twenty scenarios: ten quote-backed UNI rows and ten
terminal CAKE rows. Assert the receipt remains published with
`opportunity_count=20`. Mutate one terminal route reason, omit a route, omit a
leg, duplicate an ID, or falsely mark the failed leg's typed lineage observed
without matching retained evidence and assert the prior pointer is preserved.

Add a separate usable-CAKE fixture with no CAKE fee rows. Assert only CAKE
economics are unavailable with `cex_fee_public_bound_unavailable`; UNI fee
components and economics are unchanged.

- [ ] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_route_opportunity_pipeline.PublicCexResearchFinalizerTests \
  tests.test_run_live_cex_opportunity.LiveCexOpportunityOrchestrationTests -v
```

Expected: FAIL because source loading and the runner currently require every
leg and timing row to be observed/within SLA.

- [ ] **Step 3: Implement route-local terminal input generation**

Index the retained timing rows by exact route ID. Load raw and typed sources
only for eligible legs. For each route, use the current standard replay path
only when timing is `within_sla`; otherwise create the exact three terminal
costs, ineligible mode evidence, and terminal build input for each notional.
Change the runner gate from all-observed to closed-inventory validation and
keep the exact identity checks derived from the sealed universe.

- [ ] **Step 4: Add mixed-token API/frontend tests**

Use one UNI research row and one CAKE unavailable row. Assert the unfiltered
API returns both; `token=UNI` and `token=CAKE` return only the requested token;
sorting never interprets CAKE null economics as zero. Assert the frontend
renders both token labels, shows CAKE as unavailable with no target/net value,
and preserves the normal Current Opportunity copy without a demo-fixture
marker.

- [ ] **Step 5: Update operator documentation**

Document the four-market/twenty-scenario scope, the v2 receipt, per-token
failure isolation, and the explicit CAKE fee-evidence limitation. Keep the old
single-token design document as historical design context and add a pointer to
this superseding increment; do not rewrite historical acceptance evidence.

- [ ] **Step 6: Verify GREEN and focused integration**

Run:

```bash
python3 -m unittest \
  tests.test_live_cex_research \
  tests.test_run_live_cex_opportunity \
  tests.test_route_opportunity_pipeline \
  tests.test_opportunity_api \
  tests.test_opportunity_frontend \
  tests.test_current_opportunity_dashboard -v
```

Expected: PASS with dynamic counts, mixed-token filtering, terminal isolation,
and unchanged no-order semantics.

- [ ] **Step 7: Commit and push**

```bash
git add scripts/route_opportunity_pipeline.py scripts/run_live_cex_opportunity.py tests/test_route_opportunity_pipeline.py tests/test_run_live_cex_opportunity.py tests/test_opportunity_api.py tests/test_opportunity_frontend.py tests/test_current_opportunity_dashboard.py README.md docs/collection-operations.md docs/superpowers/specs/2026-09-04-live-cex-opportunity-design.md
git commit -m "feat(opportunities): isolate multi-token live failures"
git push origin codex/historical-foundry-opportunity
```

### Task 4: Full verification and bounded real acceptance

**Files:**
- Modify only when a reproduced defect first has a failing regression test.
- Runtime output: a fresh ignored directory below `/private/tmp`.

**Interfaces:**
- Refresh: `python3 scripts/run_live_cex_opportunity.py --data-dir /private/tmp/cex-dex-live-multi-token`.
- Serve: `python3 scripts/run_current_opportunity_dashboard.py --data-dir /private/tmp/cex-dex-live-multi-token --port 8765`.

- [ ] **Step 1: Run focused and publication/release suites**

Run:

```bash
python3 -m unittest \
  tests.test_live_cex_research tests.test_run_live_cex_opportunity \
  tests.test_fetch_cex_depth tests.test_cex_fee_facts \
  tests.test_execution_cost_components tests.test_route_opportunity \
  tests.test_route_opportunity_pipeline tests.test_route_publication \
  tests.test_opportunity_api tests.test_opportunity_frontend \
  tests.test_current_opportunity_dashboard tests.test_release_smoke -v
```

Expected: PASS.

- [ ] **Step 2: Run the repository-wide Python suite**

Run: `python3 -m unittest discover -s tests -p 'test_*.py'`

Record the exact count and any pre-existing environment failures separately.
Do not weaken a test to obtain a pass.

- [ ] **Step 3: Run one bounded real refresh**

Use the tracked fee schedule and existing fixed adapters only. Verify the
receipt is v2 with four markets, four routes, twenty opportunities, and zero
strict rows. Verify four retained market identities, non-fixture raw hashes,
typed rule/conversion members for every observed leg, and cold-reload identity.
CAKE rows may be terminal because of source availability or missing exact fee
evidence; they must never contain numeric fee or net economics in that state.

- [ ] **Step 4: Verify loopback API and UI fresh/stale behavior**

Query `/health` and
`/api/markets/opportunities?notional=1000&class=all&route_type=cex_cex` while
fresh. Verify UNI and CAKE filtering independently and no demo marker. After
the existing 120-second boundary, verify economics are suppressed rather than
reused.

- [ ] **Step 5: Fix only reproduced acceptance defects through RED/GREEN**

For each defect, add one failing regression test naming the observed break,
run it to prove RED, implement the smallest fix, and rerun the focused and
repository-wide commands. Commit as:

```bash
git commit -m "fix(opportunities): close multi-token acceptance gap"
git push origin codex/historical-foundry-opportunity
```

Do not create an empty commit when no defect exists.
