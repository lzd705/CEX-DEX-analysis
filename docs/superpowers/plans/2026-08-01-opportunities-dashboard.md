# Opportunities API and Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish strict and estimated route opportunities through a compact API and a dedicated Opportunities page, while clearly retaining Daily Price Gap as a separate research metric.

**Architecture:** A focused dashboard module validates the immutable route bundle and builds filtered summary payloads. The main server owns source signatures, caching, routing, and HTTP errors. Navigation and UI state are independent of Token Market A/B, with request ownership preventing stale responses from replacing newer filters.

**Tech Stack:** Python 3.8-compatible standard library, SQLite immutable bundle, HTTP JSON API, vanilla JavaScript/HTML/CSS, Node contract tests, `unittest`.

## Global Constraints

- Strict and research-estimate rows are separate inventories and labels.
- Daily Price Gap remains date-window research; it is never labeled executable.
- Route-level unavailable is HTTP 200 with exact N/A reason; corrupt publication is HTTP 503.
- The Summary payload remains compact and does not embed route legs/raw transcripts.
- A late API response cannot overwrite newer route/filter/navigation state.
- Opportunities does not modify Market A/B or the separate multi-market work.
- Funding Rate and Upbit mutation are excluded.

---

### Task 1: Immutable bundle reader and payload builder

**Files:**
- Create: `dashboard/opportunity_facts.py`
- Create: `tests/test_opportunity_api.py`

**Interfaces:**
- Consumes: final bundle files from route/cost plans.
- Produces: `resolve_opportunity_bundle()`, `load_latest_opportunities()`, and `build_opportunity_payload()`.

- [ ] **Step 1: Write failing pointer/hash/identity tests**

Build one valid fixture bundle, then test pointer traversal, manifest hash
tamper, SQLite/CSV fingerprint mismatch, duplicate route/scenario key, unknown
status, assumed-as-strict row, and missing cost inventory.

- [ ] **Step 2: Write failing filter/sort tests**

```python
payload = build_opportunity_payload(
    rows,
    manifest=manifest,
    token="AAVE",
    notional_usd=10000,
    opportunity_class="strict",
    route_type="cex_dex",
    availability="available",
    sort="net_edge_usd",
    direction="desc",
    now=NOW,
)
self.assertTrue(all(
    row["opportunity_class"] == "executable_candidate"
    for row in payload["routes"]
))
```

Assert null ranks last, canonical route ID breaks ties, stale cohorts expose
no strict numeric ranking, and available-empty differs from unavailable bundle.
The API maps filter aliases `strict → executable_candidate` and
`estimate → research_estimate`; stored and returned opportunity enums remain
canonical. `unavailable` rows are selected through the availability filter,
not silently merged into either class.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_opportunity_api -v`

Expected: FAIL because the dashboard module does not exist.

- [ ] **Step 4: Implement fail-closed reader and compact projection**

Return metadata, coverage/status counts, filter echo, sorted route summaries,
cost breakdowns, leg timestamps, skew/age, capacity, source links, and stable
reason codes. Do not expose raw authenticated payloads, sender/account identity,
order-book levels, or internal exceptions.

- [ ] **Step 5: Run API module tests**

Run: `python3 -m unittest tests.test_opportunity_api -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/opportunity_facts.py tests/test_opportunity_api.py
git commit -m "feat(api): build compact opportunity payloads"
```

Add a GitHub commit comment with strict/estimate separation and tamper tests.

### Task 2: Server endpoint, generation, freshness, and cache

**Files:**
- Modify: `dashboard/server.py`
- Modify: `dashboard/freshness.py`
- Modify: `deploy/dashboard.env.example`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_freshness.py`
- Modify: `tests/test_opportunity_api.py`

**Interfaces:**
- Consumes: Task 1 builders.
- Produces: `GET /api/markets/opportunities` and route-source metadata in health.

- [ ] **Step 1: Write failing query normalization tests**

Allow only Token, collected notional, `strict|estimate|all`, route type,
availability, sort, and direction. Invalid enum/notional returns bounded 400.
Unknown query keys do not enter cache ownership.

- [ ] **Step 2: Write failing generation and error tests**

Pointer/manifest files participate in `api_source_signature()`. Missing bundle
returns availability unavailable or 503 according to contract; corrupt bundle
always maps to public 503 without raw path/error. A source change during build
cannot return mixed generation.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_opportunity_api tests.test_dashboard tests.test_freshness -v`

Expected: FAIL because the route and source signature are absent.

- [ ] **Step 4: Implement endpoint and 120-second age gate**

Resolve `MARKET_ROUTE_DATA_DIR` or `<MARKET_DATA_DIR>/routes`. Strict routes
older than 120 seconds remain visible but unavailable with `cohort_stale`.
Health distinguishes missing, current, stale, and invalid route publication.

- [ ] **Step 5: Run focused server tests**

Run: `python3 -m unittest tests.test_opportunity_api tests.test_dashboard tests.test_freshness -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/server.py dashboard/freshness.py deploy/dashboard.env.example tests/test_dashboard.py tests/test_freshness.py tests/test_opportunity_api.py
git commit -m "feat(api): expose synchronized route opportunities"
```

Add a GitHub commit comment with cache/generation/age evidence.

### Task 3: Independent Opportunities route and page shell

**Files:**
- Modify: `dashboard/static/navigation.js`
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/styles.css`
- Modify: `tests/test_navigation.py`
- Create: `tests/test_opportunity_frontend.py`

**Interfaces:**
- Produces: top-level route `{kind: "opportunities", filters}` and `/opportunities`.
- Produces: independent strict and estimate page sections.

- [ ] **Step 1: Write failing navigation round-trip tests**

```javascript
const parsed = navigation.parseRoute(
  "/opportunities",
  "?token=AAVE&notional=10000&class=strict&sort=net_edge_usd&dir=desc"
);
assert.equal(parsed.kind, "opportunities");
assert.equal(parsed.filters.token, "AAVE");
assert.equal(navigation.buildOpportunitiesPath(parsed.filters).startsWith("/opportunities?"), true);
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_navigation tests.test_opportunity_frontend -v`

Expected: FAIL because the route/page is absent.

- [ ] **Step 3: Add primary navigation and page structure**

Add `Opportunities` between Screener and Markets. The page contains cohort
status, Token/notional/route/availability filters, strict table, estimates
table, exact legend, loading/error/available-empty states, and cost disclosure.
Hide the daily date toolbar on this route.

- [ ] **Step 4: Add responsive layout contracts**

Desktop tables keep aligned numeric columns. At 700px, route cards expand
inline; information disclosures are not clipped by table overflow; primary
navigation remains fully visible.

- [ ] **Step 5: Run shell/navigation tests**

Run: `python3 -m unittest tests.test_navigation tests.test_opportunity_frontend -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/static/navigation.js dashboard/static/index.html dashboard/static/styles.css tests/test_navigation.py tests/test_opportunity_frontend.py
git commit -m "feat(ui): add independent Opportunities workspace"
```

Add a GitHub commit comment noting no Market A/B change.

### Task 4: Renderer, sorting, N/A disclosure, and request ownership

**Files:**
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/styles.css`
- Modify: `tests/test_opportunity_frontend.py`
- Modify: `tests/test_dashboard_frontend.py`

**Interfaces:**
- Consumes: API from Task 2 and page shell from Task 3.
- Produces: `loadOpportunities()`, `renderOpportunities()`, and route-filter state.

- [ ] **Step 1: Write failing renderer contract tests**

Render one strict, one estimate, one skew-unavailable, and one stale row.
Assert strict and estimate DOM inventories do not mix, every N/A has an
accessible disclosure, zero remains zero, and costs reconcile to net edge.

- [ ] **Step 2: Write failing request-race tests**

Start two requests with different Token/notional filters, resolve the older
last, and assert only the newer response updates DOM/URL/status. Route change
away from Opportunities aborts ownership.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_opportunity_frontend tests.test_dashboard_frontend -v`

Expected: FAIL because rendering/loading is absent.

- [ ] **Step 4: Implement deterministic UI behavior**

Default strict ordering is positive `net_edge_usd` descending. Provide net
bps, capacity, skew, volume, and freshness sorts. Strict-empty copy explains
that no route currently satisfies every gate; it does not imply no price gap
or no market.

- [ ] **Step 5: Run frontend tests**

Run: `python3 -m unittest tests.test_opportunity_frontend tests.test_dashboard_frontend -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/static/app.js dashboard/static/styles.css tests/test_opportunity_frontend.py tests/test_dashboard_frontend.py
git commit -m "feat(ui): render strict and estimated route rankings"
```

Add a GitHub commit comment with request-race and N/A evidence.

### Task 5: Rename research spread consistently to Daily Price Gap

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `docs/market-monitor-design.md`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_compare_chart_frontend.py`

**Interfaces:**
- Preserves: existing symmetric midpoint-relative formula and sort keys.
- Produces: user-facing Daily Price Gap terminology only.

- [ ] **Step 1: Write failing copy-boundary tests**

Assert Screener and Compare say `Daily Price Gap`, formula disclosure says
same-UTC-date closes, and no daily value is called executable, live, quoted
spread, or arbitrage.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_dashboard_frontend tests.test_compare_chart_frontend -v`

Expected: FAIL on old labels.

- [ ] **Step 3: Update labels without changing metric keys or math**

Keep `spread`, `spread_max`, `spread_mean`, and `spread_median` URL/API keys for
backward compatibility. Change visible labels, captions, tooltips, empty states,
and methodology copy only.

- [ ] **Step 4: Run comparison regressions**

Run: `python3 -m unittest tests.test_dashboard_frontend tests.test_compare_chart_frontend -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard/static/index.html dashboard/static/app.js docs/market-monitor-design.md tests/test_dashboard_frontend.py tests/test_compare_chart_frontend.py
git commit -m "fix(copy): distinguish Daily Price Gap from opportunities"
```

Add a GitHub commit comment confirming no formula or stored data changed.

### Task 6: Release checker, performance, and browser acceptance

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/collection-operations.md`
- Modify: `docs/market-monitor-design.md`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_framework.py`

**Interfaces:**
- Consumes: complete API/UI.
- Produces: production release gate and documented operator workflow.

- [ ] **Step 1: Write failing release counterexamples**

Reject mixed strict/estimate rows, invalid counts, unknown reasons, stale
strict numerics, generation mismatch, route inventory divergence, missing
N/A reason, and oversized summary payload.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke tests.test_framework -v`

Expected: FAIL because opportunity release checks are absent.

- [ ] **Step 3: Implement release and performance checks**

Require full route pointer validation, API filter cross-checks, compact payload
budget, no secret material, and exact cohort generation. Record cold/warm API
latency and bytes without weakening freshness.

- [ ] **Step 4: Run focused and full tests**

Run: `python3 -m unittest tests.test_opportunity_api tests.test_opportunity_frontend tests.test_navigation tests.test_release_smoke tests.test_framework -v`

Then: `python3 -m unittest discover -s tests -v`

Expected: PASS.

- [ ] **Step 5: Perform desktop/mobile browser acceptance**

Verify strict/estimate tabs, sorting/filter URL round-trip, N/A reasons,
stale/skew states, available-empty and corrupt error states, Daily Price Gap
copy, navigation, and no page overflow.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_dashboard_release.py docs/collection-operations.md docs/market-monitor-design.md tests/test_release_smoke.py tests/test_framework.py
git commit -m "test(release): validate Opportunities dashboard contract"
```

Add a GitHub commit comment with test totals, payload benchmark, and browser evidence.

### Task 7: Two-minute collection candidate and bounded retention

**Files:**
- Create: `deploy/systemd/cex-dex-routes.service.in`
- Create: `deploy/systemd/cex-dex-routes.timer`
- Create: `scripts/retain_route_bundles.py`
- Create: `scripts/retain_route_raw.py`
- Modify: `scripts/install_collection_timers.sh`
- Modify: `deploy/render_runtime_templates.py`
- Modify: `docs/production-hardening.md`
- Modify: `tests/test_deploy_templates.py`
- Modify: `tests/test_admin.py`

**Interfaces:**
- Consumes: complete validated route collector/API/UI release.
- Produces: locked two-minute collection cadence and dry-run-first retention
  for both immutable bundles and raw route transcripts.

- [ ] **Step 1: Write failing timer and retention tests**

Assert the route service runs only the route profile with publication enabled,
uses the existing collection lock, has bounded runtime/resources, and the timer
runs every two minutes. Retention defaults to dry-run, rejects broad/symlink
roots, never removes the pointed latest bundle, and keeps a declared rollback
window. Raw transcripts live under `data/raw/routes/<cohort_id>/`, retain their
manifest/content hashes, and cannot outlive all bundles that reference them.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_deploy_templates tests.test_admin -v`

Expected: FAIL because route deployment templates/retention are absent.

- [ ] **Step 3: Implement templates and guarded retention**

The timer may be installed only after the complete route release checker
passes. A failed cycle leaves the prior bundle with its original timestamp so
the API naturally becomes stale; it never extends freshness.

Default policy keeps raw route transcripts for 7 days and complete bundles for
30 days with at least 20 newest valid bundles. Both scripts default to
`--dry-run`, preserve the active and declared rollback pointers, reject
symlink/path traversal/broad roots, and emit an exact deletion inventory before
an explicit `--apply` run.

- [ ] **Step 4: Run deployment and full suites**

Run: `python3 -m unittest tests.test_deploy_templates tests.test_admin -v`

Then: `python3 -m unittest discover -s tests -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add deploy/systemd/cex-dex-routes.service.in deploy/systemd/cex-dex-routes.timer scripts/retain_route_bundles.py scripts/retain_route_raw.py scripts/install_collection_timers.sh deploy/render_runtime_templates.py docs/production-hardening.md tests/test_deploy_templates.py tests/test_admin.py
git commit -m "feat(ops): schedule synchronized route collection"
```

Add a GitHub commit comment with timer, lock, retention, and full-suite evidence.

- [ ] **Step 6: Produce a deployment candidate, but do not enable production**

Render and validate service/timer/retention artifacts in a temporary candidate
root. Production application deployment, pointer cutover, and timer enablement
are deferred to the cross-increment final release plan after all adapter and
Event gates pass.
