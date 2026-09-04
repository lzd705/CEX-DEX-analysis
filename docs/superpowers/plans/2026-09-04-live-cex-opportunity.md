# Live CEX Opportunity Research Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver one command that collects real public `UNI/USDT` Binance and Bybit books, publishes research-only Opportunity results, and optionally serves them through the normal Current Opportunity page.

**Architecture:** A fixed pure universe feeds the existing bounded cohort collector and immutable core publisher. A separate public-research finalizer replays retained book/rule evidence with reviewed public fee bounds, forces non-strict classification, and uses the existing complete-bundle publisher and dashboard. The production Shadow authority and authenticated finalizer remain unchanged.

**Tech Stack:** Python 3.8-compatible standard library, `Decimal`/integer-lattice quantity math, existing route collectors/publication contracts, `unittest`, local HTTP smoke tests.

**Spec:** `docs/superpowers/specs/2026-09-04-live-cex-opportunity-design.md`

## Global Constraints

- The fixed market is `UNI/USDT`; the fixed venues are Binance and Bybit; both directions are evaluated.
- Requested notionals are exactly 1,000, 5,000, 10,000, 50,000, and 100,000 USD.
- Network calls are public HTTPS GET requests through existing fixed adapters; no CLI URL, host, proxy, credential, profile, wallet, order, or RPC input is added.
- Raw book and rule bytes, typed source lineage, state IDs, core hashes, opportunity bindings, and complete-bundle hashes remain validated by existing contracts.
- Public fee estimates are opt-in, use the current upper bound, and are always `strict_eligible=false`.
- Every result is `research_estimate` or `unavailable`; no result may carry a publication attestation or become `executable_candidate`.
- The production route Shadow authority and authenticated CEX finalizer remain unchanged.
- The server binds only `127.0.0.1` and imports the existing isolated Current Opportunity dashboard.
- Tests never contact public hosts; one bounded real run is a separate acceptance step.
- Every production-code change follows RED, GREEN, focused regression, review, commit, and push.

---

### Task 1: Fixed live-research universe and non-redirecting public transport

**Files:**
- Create: `scripts/live_cex_research.py`
- Create: `tests/test_live_cex_research.py`
- Modify: `scripts/fetch_cex_depth.py`
- Modify: `tests/test_fetch_cex_depth.py`

**Interfaces:**
- Produces: `build_live_cex_research_universe() -> Dict[str, Any]`.
- Produces: `live_cex_research_generation() -> str`, the canonical SHA-256 of the fixed universe input contract.
- Produces: `open_public_json_request(request: urllib.request.Request, *, timeout: float) -> context manager`, used only by `request_json()` and configured to reject HTTP redirects.
- The universe contains exactly two selected legs and two directional routes, with `selection_inputs.cex_selected_window_usd=None` and null route-volume fields.

- [ ] **Step 1: Write failing fixed-universe tests**

Add literal assertions that the universe has schema `route_universe/v1`, exactly these legs in canonical order:

```python
[
    "cex:binance:UNI/USDT",
    "cex:bybit:UNI/USDT",
]
```

and exactly these directional route IDs:

```python
[
    "route:UNI:cex:binance:UNI/USDT->cex:bybit:UNI/USDT:prepositioned_inventory",
    "route:UNI:cex:bybit:UNI/USDT->cex:binance:UNI/USDT:prepositioned_inventory",
]
```

Assert the five literal notionals, null volume claims, fixed adapter-supported flags, one hand-derived literal generation hash, and that every route carries that same generation.

- [ ] **Step 2: Run the focused universe test and verify RED**

Run: `python3 -m unittest tests.test_live_cex_research.LiveCexResearchUniverseTests -v`

Expected: FAIL because `scripts.live_cex_research` does not exist.

- [ ] **Step 3: Implement the smallest pure universe builder**

Use `canonical_route_id()` and the repository constants `ROUTE_UNIVERSE_SCHEMA` and `REQUESTED_NOTIONALS_USD`. Hash canonical UTF-8 JSON with sorted keys and compact separators. Do not read files, environment, clocks, or network state.

- [ ] **Step 4: Run the universe test and verify GREEN**

Run: `python3 -m unittest tests.test_live_cex_research.LiveCexResearchUniverseTests -v`

Expected: PASS.

- [ ] **Step 5: Write a failing redirect-boundary test**

Use a local HTTP server whose first endpoint returns `302 Location` to a second endpoint. Call the transport through `request_json()` and assert it raises on the 302 while the second endpoint request count stays zero. The production change this catches is restoring urllib's automatic redirect following.

- [ ] **Step 6: Run the redirect test and verify RED**

Run: `python3 -m unittest tests.test_fetch_cex_depth.FetchCexDepthTest.test_request_json_does_not_follow_redirects -v`

Expected: FAIL because the current `urllib.request.urlopen()` follows the redirect and contacts the second endpoint.

- [ ] **Step 7: Add a no-redirect opener beneath `request_json()`**

Add a private `HTTPRedirectHandler` whose `redirect_request()` returns `None`, create the opener with the existing TLS context, and route `request_json()` through a narrow helper. Preserve timeout, maximum-byte, retry, HTTP 429/5xx, deadline, and test-injection behavior. Redirect HTTP errors are non-retryable.

- [ ] **Step 8: Run focused transport regressions and verify GREEN**

Run: `python3 -m unittest tests.test_fetch_cex_depth.FetchCexDepthTest.test_request_json_does_not_follow_redirects tests.test_fetch_cex_depth.FetchCexDepthTest.test_rules_http_body_is_read_with_an_explicit_bound tests.test_fetch_cex_depth.FetchCexDepthTest.test_rules_http_body_one_byte_over_bound_is_rejected -v`

Expected: PASS with the redirect target request count equal to zero.

- [ ] **Step 9: Commit and push Task 1**

```bash
git add scripts/live_cex_research.py scripts/fetch_cex_depth.py tests/test_live_cex_research.py tests/test_fetch_cex_depth.py
git commit -m "feat(opportunities): define safe live CEX research inputs"
git push origin codex/historical-foundry-opportunity
```

### Task 2: Public-fee research input builder and finalizer

**Files:**
- Modify: `scripts/cex_fee_facts.py`
- Modify: `scripts/live_cex_research.py`
- Modify: `scripts/route_opportunity_pipeline.py`
- Modify: `scripts/route_publication.py`
- Modify: `tests/test_cex_fee_facts.py`
- Modify: `tests/test_live_cex_research.py`
- Modify: `tests/test_route_opportunity_pipeline.py`

**Interfaces:**
- Produces: `public_fee_semantics(component: Mapping[str, Any], *, direction: str, rules: MarketRules, now: str) -> FeeSemantics`.
- Internally uses `_build_public_cex_research_inputs(..., public_fee_schedule_snapshot: _PublicFeeScheduleSnapshot)`; neither the immutable snapshot nor parsed schedule rows are a public collector/builder input.
- Produces: `finalize_public_cex_research_opportunities(*, data_dir: Path, public_fee_schedule_path: Path, expected_route_cohort_id: str, expected_core_manifest_sha256: str) -> Dict[str, Any]`.
- The finalizer consumes only the current core just collected by the same command, binds it to the expected cohort and manifest identity, and enforces the exact fixed UNI/USDT Binance/Bybit universe. It does not accept a Shadow run ID or private profile.

- [ ] **Step 1: Write failing public-fee semantics tests**

Construct one literal `bounded_estimate` fee component and assert:

```python
fee.rate_bps == Decimal("10")
fee.fee_asset == "UNI"          # buy received asset
fee.charge_basis == "received_base"
fee.fee_increment == rules.base_increment
fee.rounding_mode == "ceiling"
fee.source_record_sha256 == component["source_record_sha256"]
```

For sell, assert `USDT`, `received_quote`, and `rules.quote_increment`. A terminal unavailable component must create a zero-rate mechanics object with a deterministic non-fee source hash, while the component itself remains unavailable. Reject mismatched leg, market, rate, time window, and source hash.

- [ ] **Step 2: Run the fee semantics tests and verify RED**

Run: `python3 -m unittest tests.test_live_cex_research.PublicFeeSemanticsTests -v`

Expected: FAIL because `public_fee_semantics()` does not exist.

- [ ] **Step 3: Implement public fee semantics**

For `bounded_estimate`, build `FeeSemantics` from the component's upper-bound rate and source hash. For terminal `unavailable`, build only the zero-rate mechanics object needed to replay gross book cash flow; bind it to a canonical hash of the terminal component and never change the component to numeric or strict.

- [ ] **Step 4: Run the fee semantics tests and verify GREEN**

Run: `python3 -m unittest tests.test_live_cex_research.PublicFeeSemanticsTests -v`

Expected: PASS.

- [ ] **Step 5: Write failing end-to-end finalizer tests**

Reuse the real core/raw/typed-source fixture builders from `tests.test_route_publication`, adapted to publish the exact fixed UNI/USDT Binance/Bybit universe. Use a literal public schedule whose Binance and Bybit rows exactly match `UNI/USDT`. Assert ten scenarios are published and cold-loadable, and every row has:

```python
row["opportunity_class"] == "research_estimate"
row["strict_eligible"] is False
row["strict_ready_for_publication"] is False
row["publication_attestation_sha256"] is None
```

Assert the buy and sell quotes carry the exact same target base-token quantity, fee components are `bounded_estimate`, the maximum fee rates are used, inventory mode evidence is ineligible, and rebalancing is an explicit scenario-only `assumed` zero with `inventory_not_observed_for_public_research`.

Add separate cases for missing fee rows, stale schedules, ambiguous schedules, typed-source mutation, raw-book mutation, transient fee-schedule replacement during calculation, same-byte schedule inode replacement, stale expected core identity, an identity-matching AAVE core, and cold-reload failure. Preserve a sentinel prior `routes/latest.json` on each failure, while proving a concurrent third-party pointer is never overwritten.

- [ ] **Step 6: Run the finalizer tests and verify RED**

Run: `python3 -m unittest tests.test_route_opportunity_pipeline.PublicCexResearchFinalizerTests -v`

Expected: FAIL because the separate finalizer does not exist.

- [ ] **Step 7: Extract shared CEX source loading without behavior change**

Move the raw-book, market-rule, and USD-conversion replay portion of `_build_inputs()` into:

```python
_load_cex_sources(
    *, root: Path, cohort: Mapping[str, Any], source_root: Path, now: str
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]
```

The second return value is the core leg map. Keep all existing descriptor, identity, validity-window, and hash checks. Make authenticated `_build_inputs()` call this helper and run its existing focused tests before adding research behavior.

- [ ] **Step 8: Implement the public research builder**

For each route and notional:

1. derive `CommonTarget` with `common_target_quantity()`;
2. resolve each leg with the private immutable-schedule-snapshot fee helper;
3. derive matching `FeeSemantics`;
4. replay each retained book with `route_quantity_quote_for_book()`;
5. build authenticated USDT=USD projections from retained typed sources;
6. add a scenario-only assumed-zero route-level rebalancing component whose
   basis says prepositioned inventory is hypothetical and unobserved;
7. call `classify_route_mode_evidence(route, now=now)` without inventory;
8. call `build_route_opportunity()` and assert the claim boundary before returning the input.

Securely open and parse the public fee schedule once, bind every fee calculation to that immutable bytes/device/inode snapshot, and verify the same path identity and bytes immediately before commit. Pass `source_root` but no private fee or inventory paths to `publish_complete_route_bundle()`. Cold reload through a postcommit validator inside the publisher's CAS-safe rollback transaction, compare the exact pointer, and restore the prior pointer on failure only when the attempted pointer remains current.

- [ ] **Step 9: Run finalizer and authenticated-path regressions and verify GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_route_opportunity_pipeline.PublicCexResearchFinalizerTests \
  tests.test_route_opportunity_pipeline.RouteOpportunityPipelineTests \
  -v
```

Expected: PASS; existing private-profile finalization behavior is unchanged.

- [ ] **Step 10: Commit and push Task 2**

```bash
git add scripts/live_cex_research.py scripts/route_opportunity_pipeline.py tests/test_live_cex_research.py tests/test_route_opportunity_pipeline.py
git commit -m "feat(opportunities): publish public CEX research estimates"
git push origin codex/historical-foundry-opportunity
```

### Task 3: One-command collection, publication, and normal dashboard

**Files:**
- Create: `scripts/run_live_cex_opportunity.py`
- Create: `tests/test_run_live_cex_opportunity.py`
- Modify: `scripts/live_cex_research.py`

**Interfaces:**
- Produces: `collect_and_publish_live_cex_research(*, data_dir: Path, public_fee_schedule_path: Path, deadline_seconds: int, wall_clock: Callable[[], datetime]) -> Dict[str, Any]`.
- Produces: `parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace`.
- Produces: `main(argv: Optional[Sequence[str]]) -> int`.
- Optional serving calls `serve_current_dashboard(data_dir=..., port=...)` only after cold reload.

- [ ] **Step 1: Write failing orchestration tests**

Assert the command parser accepts only `--data-dir`, `--public-fee-schedule`, `--deadline-seconds`, `--serve`, and `--port`; rejects relative data paths, deadlines outside 10..60, and ports outside 1..65535; and has no token, venue, URL, profile, run-selection, host, or finalizer option.

Using injected collectors and a temporary data root, assert the exact order:

```text
build fixed universe
collect_route_cohort
publish_route_cohort_bundle
finalize_public_cex_research_opportunities
load_latest_complete_route_bundle
optional serve_current_dashboard
```

Assert the finalizer receives the cohort ID and core manifest hash just published, a collector failure never calls it, a reload mismatch never serves, and a failure leaves prior complete-pointer bytes unchanged.

- [ ] **Step 2: Run orchestration tests and verify RED**

Run: `python3 -m unittest tests.test_run_live_cex_opportunity -v`

Expected: FAIL because the runner does not exist.

- [ ] **Step 3: Implement collection and publication orchestration**

Call `collect_route_cohort()` with the fixed universe, generation reader, raw root, existing CEX collector, `max_workers=4`, `cex_workers_per_venue=2`, and the validated deadline. Require both legs observed and both route timing rows `within_sla` before publishing core. Then pass the returned cohort ID and manifest hash to the public finalizer and cold loader.

Return a receipt with exactly:

```python
{
    "schema": "live_cex_opportunity_refresh/v1",
    "status": "published",
    "token_pair": "UNI/USDT",
    "venues": ["binance", "bybit"],
    "route_cohort_id": pointer["route_cohort_id"],
    "manifest_sha256": pointer["manifest_sha256"],
    "opportunity_count": 10,
    "strict_eligible_count": 0,
    "served": False,
}
```

Do not include paths, raw URLs, exception payloads, or environment values.

- [ ] **Step 4: Implement the CLI and loopback serving gate**

Create the data directory if its absolute parent is real and non-symlinked. Resolve the default schedule to the tracked repository file. Print the compact JSON receipt. With `--serve`, set `served=true`, print before entering the blocking server, then call the existing normal dashboard runner. On failure print one stable code among `preflight_failed`, `collection_failed`, `publication_failed`, `reload_failed`, or `serve_failed`, and return 1; return 130 on interruption.

- [ ] **Step 5: Run orchestration and dashboard regressions and verify GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_run_live_cex_opportunity \
  tests.test_current_opportunity_dashboard \
  tests.test_opportunity_api \
  -v
```

Expected: PASS; the normal server remains loopback-only with write surfaces disabled.

- [ ] **Step 6: Commit and push Task 3**

```bash
git add scripts/run_live_cex_opportunity.py scripts/live_cex_research.py tests/test_run_live_cex_opportunity.py
git commit -m "feat(opportunities): add one-command live CEX refresh"
git push origin codex/historical-foundry-opportunity
```

### Task 4: Reviewed public fee bounds and operator documentation

**Files:**
- Modify: `config/cex_public_fee_schedules.csv`
- Modify: `README.md`
- Modify: `docs/collection-operations.md`
- Modify: `tests/test_cex_fee_facts.py`

**Interfaces:**
- The tracked schedule supplies one exact `both` side row for Binance and one for Bybit, each matching only `UNI/USDT`.
- Documentation publishes the exact refresh and serve commands and explains research-only classification.

- [ ] **Step 1: Verify official fee sources**

Use only current official Binance and Bybit pages. Record the public standard spot taker-fee interval supported by each page, the check time in UTC, a validity window no longer than 30 days, `fee_asset=received_asset`, and `basis=official_spot_taker_fee_range`. If an official page does not support a defensible bound, leave that venue unmatched; do not use a blog, aggregator, or invented value.

- [ ] **Step 2: Write failing tracked-schedule behavior tests**

At a fixed clock inside the declared window, assert the tracked schedule produces either the exact reviewed `bounded_estimate` maximum for each supported venue or the explicit `cex_fee_public_bound_unavailable` terminal state. Assert no tracked row uses a wildcard instrument, non-official host, validity longer than 30 days, or future `checked_at`.

- [ ] **Step 3: Run the schedule tests and verify RED**

Run: `python3 -m unittest tests.test_cex_fee_facts.FeeCollectorTests.test_tracked_live_research_schedule_is_reviewed_and_bounded -v`

Expected: FAIL because the tracked schedule currently contains only its header.

- [ ] **Step 4: Add only source-supported rows and documentation**

Edit the CSV through `apply_patch`. Document that public fees are conservative research inputs, account fees may differ, no inventory is observed, negative output is valid, the page ages out after 120 seconds, and rerunning the command creates a new immutable cohort.

- [ ] **Step 5: Run schedule and documentation-adjacent regressions**

Run:

```bash
python3 -m unittest \
  tests.test_cex_fee_facts \
  tests.test_route_opportunity_pipeline.PublicCexResearchFinalizerTests \
  -v
```

Expected: PASS.

- [ ] **Step 6: Commit and push Task 4**

```bash
git add config/cex_public_fee_schedules.csv README.md docs/collection-operations.md tests/test_cex_fee_facts.py
git commit -m "docs(opportunities): document live CEX research refresh"
git push origin codex/historical-foundry-opportunity
```

### Task 5: Full verification and one real acceptance run

**Files:**
- Modify only if a reproduced defect requires a TDD fix.
- Runtime output: an ignored local data directory below `/private/tmp` or the worktree's ignored `data/local` tree.

**Interfaces:**
- Acceptance command: `python3 scripts/run_live_cex_opportunity.py --data-dir /private/tmp/cex-dex-live-opportunity`.
- Read-only UI command: `python3 scripts/run_current_opportunity_dashboard.py --data-dir /private/tmp/cex-dex-live-opportunity --port 8765`.

- [ ] **Step 1: Run focused suites**

```bash
python3 -m unittest \
  tests.test_live_cex_research \
  tests.test_run_live_cex_opportunity \
  tests.test_fetch_cex_depth \
  tests.test_cex_fee_facts \
  tests.test_route_opportunity_pipeline \
  tests.test_current_opportunity_dashboard \
  -v
```

Expected: PASS.

- [ ] **Step 2: Run publication/API/release regressions**

```bash
python3 -m unittest \
  tests.test_route_publication \
  tests.test_route_opportunity \
  tests.test_opportunity_api \
  tests.test_release_smoke \
  -v
```

Expected: PASS.

- [ ] **Step 3: Run repository framework checks**

Run the repository's documented full Python test command and grammar/import checks. Any existing unrelated failure is recorded with the exact command and output; no test is weakened.

- [ ] **Step 4: Perform one bounded real collection**

Use a fresh explicit local data directory. Inspect the receipt and verify:

- both leg statuses are observed;
- raw response hashes differ from repository fixture hashes;
- typed market-rule and USD-conversion members exist for both venues;
- the core and complete pointers cold reload;
- all ten rows are research/unavailable and none is strict eligible;
- fee rows match the reviewed public schedule or remain explicitly unavailable.

- [ ] **Step 5: Verify normal API and UI while fresh**

Start the normal loopback dashboard, request `/health` and
`/api/markets/opportunities?notional=1000&class=all&route_type=cex_cex`, and
open `/opportunities`. Confirm the payload and UI identify UNI, Binance,
Bybit, direction, source times, common quantity, gross edge, research cost/net
when complete, and no demo fixture marker.

- [ ] **Step 6: Verify freshness fail-closed behavior**

After the 120-second boundary, request the API again and confirm stale or
unavailable projection with no current economic claim. Do not mutate the
published bundle to simulate age.

- [ ] **Step 7: Review, commit any TDD fixes, and push final verified state**

If acceptance exposed a defect, first add a failing regression test, implement
the minimal fix, rerun Steps 1-6, then:

Stage exactly the production and regression-test files changed by the reproduced
defect, commit them as `fix(opportunities): close live CEX acceptance gap`, and
push `codex/historical-foundry-opportunity` to `origin`.

If no defect is found, do not create an empty commit. Report branch, full hash,
every exact commit message, remote, push result, test commands/counts, real
source timestamps/hashes, API result, UI URL, and remaining non-implemented
scope.
