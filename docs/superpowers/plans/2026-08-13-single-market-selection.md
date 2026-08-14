# Intentional Single-Market Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user intentionally choose `Market A only — no comparison`, apply that selection, and use the existing Token Research pages with only Market A facts, while preserving the current A/B workflow and deferring candlesticks.

**Architecture:** `selection=single` is the only intentional single-market marker. Navigation, session state, API cardinality, response identity, render mode, and async request ownership all carry that marker explicitly; an empty Market B without it remains incomplete. Compare and execution return bounded Market-A-only projections, Quality returns an exact one-ID selected inventory, Events remains Token-scoped, and the existing paired contracts remain unchanged.

**Tech Stack:** Python 3.8-compatible standard library and HTTP server, SQLite/CSV read-only fact sources, vanilla JavaScript/HTML/CSS, `unittest`, Node contract tests, and pinned Playwright Chromium browser tests.

## Global Constraints

- The visible option is exactly `Market A only — no comparison`; it is not a synthetic market catalog entry.
- Market A is always one exact catalog `market_id` for the current Token.
- `selection=single` plus no Market B is valid. No Market B without that marker is incomplete. The marker plus a Market B is invalid.
- Paired URLs omit `selection`; paired request and response shapes remain unchanged.
- A valid selection is either one exact A with the single marker or two exact, distinct A/B IDs. Invalid shared links are never silently repaired.
- Single mode hides Market B and pair-derived UI from layout, keyboard order, and the accessibility tree; it does not fill those elements with `N/A`.
- Genuine missing Market A facts keep existing structured `N/A`, reason, retryability, zero-versus-null, and no-fill semantics.
- Events remains Token-scoped. Event timing overlays do not claim impact or causality.
- Existing collectors, raw/processed data, SQLite schemas, publication pointers, source signatures, cache generations, Quality contract version 4, and execution-cost source contract remain unchanged.
- Candlesticks/OHLC, more than two markets, Benchmark mode, Route Opportunities, Funding Rate, and production deployment are out of scope.
- Do not claim a production release from code/tests alone. A later deployment must separately prove the exact Git SHA, static asset SHA, unmocked release check, and public-browser behavior.

## Dependency and File Map

```text
Task 1  deterministic baseline test clock

Task 2  navigation + canonical selection contract
   |                         \
   v                          v
Task 5  selector/session/UI   Task 3  API cardinality + Compare projection
   |                              |
   v                              v
Task 6  Compare UI <---------- Task 4  Execution + Quality projections
   |                              |
   +-------------> Task 7 <-------+
                    remaining research pages + request ownership
                              |
                              v
                    Task 8 release gate + docs
                              |
                              v
                    Task 9 real-browser regression + final verification
```

- `dashboard/static/navigation.js`: parse/build URL state and validate exact selection cardinality only.
- `dashboard/static/app.js`: own draft/applied/session selection, request ownership, API identity checks, and page rendering.
- `dashboard/static/index.html`: provide optional-B copy, stable pair-only hooks, captions, live status, and accessible controls.
- `dashboard/static/styles.css`: remove blank pair columns in single mode; do not encode business state only in CSS.
- `dashboard/market_facts.py`: project one daily series without manufacturing Market B fields.
- `dashboard/server.py`: validate request cardinality, build bounded single projections, and preserve pair/cache/source contracts.
- `scripts/check_dashboard_release.py`: fail closed on wrong single identities, leaked pair facts, or generation drift.
- `tests/test_*.py`: own deterministic unit, API, frontend, release, and compatibility counterexamples.
- `dashboard/tests/single-market-selection.spec.js`: own real DOM/browser flows; mocked browser fixtures do not replace the later unmocked public-release smoke.

## Required JavaScript test preflight

The desktop shell used for this implementation does not expose Node on its default `PATH`; without this preflight the Python wrappers report frontend suites as skipped while returning success. In every implementation shell, expose the bundled Node 24.19.0 and Playwright 1.62.1 runtime first:

```bash
export PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/bin/fallback:$PATH"
export NODE_PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules"
node --version
node -p "require('playwright/package.json').version"
```

Expected: `v24.19.0` and `1.62.1`. In another environment, install the locked equivalents. Every frontend verification must report executed tests; any `Node.js is not installed in this runtime` skip is a failed gate, not a pass.

---

### Task 1: Make the pre-existing lifecycle fixture deterministic

**Files:**
- Modify: `tests/test_dashboard.py`

**Interfaces:**
- Changes no production code.
- Freezes both source-freshness and lifecycle overlay clocks in `test_summary_producer_satisfies_structured_na_release_contract`.

- [ ] **Step 1: Reproduce the existing time-dependent failure**

Run:

```bash
python3 -m unittest tests.test_dashboard.MarketMonitorServerTest.test_summary_producer_satisfies_structured_na_release_contract -v
```

Expected before the test fix: unittest ERROR from the uncaught `ReleaseCheckError("Summary CEX lifecycle evidence is stale")`, because source freshness is frozen at 2026-08-01 while `api_freshness_bucket()` uses the current wall clock. This failure predates the single-market work and is the exact RED being corrected.

- [ ] **Step 2: Freeze the lifecycle reference bucket in the fixture**

Inside the test, derive the bucket from the existing `checked_at` value and patch it in the same context as `build_source_freshness`:

```python
freshness_bucket = int(
    datetime.fromisoformat(checked_at).timestamp()
    // server.API_FRESHNESS_CACHE_SECONDS
)
with patch.dict(
    server.os.environ,
    self.environment,
    clear=True,
), patch.object(
    server,
    "build_source_freshness",
    return_value=current_freshness,
), patch.object(
    server,
    "api_freshness_bucket",
    return_value=freshness_bucket,
):
    summary = server.build_market_summary()
```

Do not change the 36-hour lifecycle rule or the curated lifecycle evidence timestamps.

- [ ] **Step 3: Verify GREEN and no lifecycle weakening**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard.MarketMonitorServerTest.test_summary_producer_satisfies_structured_na_release_contract \
  tests.test_cex_instrument_lifecycle \
  tests.test_framework -v
```

Expected: PASS. Existing stale-evidence tests must still reject genuinely stale runtime evidence.

- [ ] **Step 4: Commit**

```bash
git add tests/test_dashboard.py
git commit -m "test(summary): freeze lifecycle fixture clock"
```

### Task 2: Define the explicit navigation and selection contract

**Files:**
- Modify: `dashboard/static/navigation.js`
- Modify: `tests/test_navigation.py`

**Interfaces:**
- Produces: `validateSelection(markets, marketA, marketB, selection)`.
- Preserves: `validatePair(markets, marketA, marketB)` as a paired compatibility wrapper.
- Adds URL state: `selection=single` on every workspace page only for intentional single mode.

- [ ] **Step 1: Write failing route round-trip tests**

Add `test_single_selection_round_trips_on_every_workspace_page`. For each value in `navigation.WORKSPACE_PAGES`, build and reparse this shared state:

```javascript
{
  marketA: "cex:binance:AAVE/USDT",
  marketB: "",
  selection: "single",
  start: "2026-07-01",
  end: "2026-07-28"
}
```

Assert each URL contains the encoded Market A and `selection=single`, contains no `marketB`, and reparses the marker plus only that page's existing page-specific fields. Add a paired regression asserting an A/B URL has no `selection` and reparses exactly as before.

- [ ] **Step 2: Write failing cardinality and invalid-link tests**

Add table-driven tests for:

```text
A exact, B empty, marker single        -> valid, mode single
A exact, B empty, no marker            -> market_b_required
A exact, B exact distinct, no marker   -> valid, mode pair
A exact, B exact, marker single        -> selection_market_b_conflict
A unknown, B empty, marker single      -> market_a_not_found
A exact, B unknown, no marker          -> market_b_not_found
A equals B, no marker                  -> same_market
unknown nonempty marker                -> selection_invalid
one-row catalog, exact A, marker single -> valid; no insufficient_markets
```

Also assert `buildWorkspacePath()` throws for an unknown nonempty marker instead of dropping it and producing a repaired URL.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_navigation -v`

Expected: FAIL because `selection` is neither parsed nor emitted and the current validator always requires B.

- [ ] **Step 4: Implement parse/build without inference**

In `parseWorkspaceState()`, preserve a supplied nonempty marker verbatim so an invalid shared link can be rejected later:

```javascript
const selection = firstParam(params, ["selection"]);
if (selection !== null) state.selection = selection;
```

In `buildWorkspacePath()`, emit only the exact supported marker:

```javascript
if (state.selection !== undefined && state.selection !== "") {
  if (state.selection !== "single") {
    throw new TypeError("Unknown market selection marker");
  }
  params.set("selection", "single");
}
```

Do not map `pairMode=manual` or `pairMode=transient` to single mode. Those existing values continue to mean incomplete/manual state and transient Opportunity pair state.

- [ ] **Step 5: Implement one canonical validator**

Add and export this contract shape:

```javascript
function validateSelection(markets, a, b, selection = "") {
  const rows = Array.isArray(markets) ? markets : [];
  const byId = new Map();
  const duplicateIds = new Set();
  rows.forEach((market) => {
    const id = marketIdentifier(market);
    if (id === null) return;
    if (byId.has(id)) duplicateIds.add(id);
    else byId.set(id, market);
  });

  const marketAId = stringValue(a);
  const marketBId = stringValue(b);
  const marker = stringValue(selection) ?? "";
  const wantsSingle = marker === "single";
  const errors = [];

  if (marker && !wantsSingle) {
    errors.push(validationError("selection_invalid", "selection", marker));
  }
  if (marketAId === null) {
    errors.push(validationError("market_a_required", "marketA", a));
  }
  if (wantsSingle && marketBId !== null) {
    errors.push(validationError(
      "selection_market_b_conflict",
      "marketB",
      marketBId,
    ));
  } else if (!wantsSingle && marketBId === null) {
    errors.push(validationError("market_b_required", "marketB", b));
  }
  if (!wantsSingle
      && marketAId !== null
      && marketBId !== null
      && marketAId === marketBId) {
    errors.push(validationError("same_market", "marketB", marketBId));
  }
  if (marketAId !== null && !byId.has(marketAId)) {
    errors.push(validationError("market_a_not_found", "marketA", marketAId));
  }
  if (!wantsSingle && marketBId !== null && !byId.has(marketBId)) {
    errors.push(validationError("market_b_not_found", "marketB", marketBId));
  }
  if (marketAId !== null && duplicateIds.has(marketAId)) {
    errors.push(validationError("market_a_ambiguous", "marketA", marketAId));
  }
  if (!wantsSingle && marketBId !== null && duplicateIds.has(marketBId)) {
    errors.push(validationError("market_b_ambiguous", "marketB", marketBId));
  }
  if (!wantsSingle && rows.length < 2) {
    errors.push(validationError("insufficient_markets", "markets", rows.length));
  }

  const resolvedA = marketAId !== null && !duplicateIds.has(marketAId)
    ? byId.get(marketAId) ?? null
    : null;
  const resolvedB = !wantsSingle
      && marketBId !== null
      && !duplicateIds.has(marketBId)
    ? byId.get(marketBId) ?? null
    : null;

  return {
    valid: errors.length === 0,
    mode: errors.length ? null : wantsSingle ? "single" : "pair",
    marketA: resolvedA,
    marketB: wantsSingle ? null : resolvedB,
    errors,
  };
}

function validatePair(markets, a, b) {
  const { mode, ...legacyResult } = validateSelection(markets, a, b, "");
  return legacyResult;
}
```

Keep error values case-sensitive and never substitute a default ID inside this function. Add an exact-key regression proving `validatePair()` still returns only `valid`, `marketA`, `marketB`, and `errors`.

- [ ] **Step 6: Verify GREEN**

Run:

```bash
python3 -m unittest tests.test_navigation tests.test_opportunity_frontend -v
```

Expected: PASS, including the unchanged transient Opportunity pair flow.

- [ ] **Step 7: Commit**

```bash
git add dashboard/static/navigation.js tests/test_navigation.py
git commit -m "feat(navigation): encode intentional single selection"
```

### Task 3: Add fail-closed API cardinality and the single Compare projection

**Files:**
- Modify: `dashboard/market_facts.py`
- Modify: `dashboard/server.py`
- Modify: `tests/test_market_facts.py`
- Modify: `tests/test_dashboard.py`

**Interfaces:**
- Produces: `validate_market_selection_cardinality(...) -> "single" | "pair"`.
- Produces: `single_market_daily_rows(rows_a)`.
- Extends: `build_market_comparison(..., *, selection=None, source_signature=None)`.
- Adds `selection` to Compare, execution-cost, and Quality query allowlists after `market_b`.

- [ ] **Step 1: Projector cycle — write and verify the failing pure test**

In `tests/test_market_facts.py`, add a row set containing one measured zero and one null. Assert:

```python
self.assertEqual(single_market_daily_rows(rows), [
    {
        "date": "2026-01-01",
        "market_a": {"price_usd": 100, "volume_usd": 0},
    },
    {
        "date": "2026-01-02",
        "market_a": {"price_usd": None, "volume_usd": None},
    },
])
```

Every projected row must have exact keys `date` and `market_a`; it must have no B, spread, or `market_b_missing` field.

Run `python3 -m unittest tests.test_market_facts -v`. Expected: RED because `single_market_daily_rows` does not exist; import/setup errors must be corrected until the assertion itself fails for that reason.

- [ ] **Step 2: Projector cycle — implement the minimal helper and verify GREEN**

Beside `compare_daily_rows()` add:

```python
def single_market_daily_rows(
    rows_a: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Project one raw series without manufacturing pair fields."""
    return [
        {
            "date": row["date"],
            "market_a": {
                "price_usd": row.get("price_usd"),
                "volume_usd": row.get("volume_usd"),
            },
        }
        for row in rows_a
    ]
```

Run `python3 -m unittest tests.test_market_facts -v`. Expected: GREEN before beginning the server/cache cycle.

- [ ] **Step 3: Cardinality/cache cycle — write failing query and HTTP tests**

In `tests/test_dashboard.py`, add:

- `test_compare_requires_explicit_single_marker_when_market_b_missing`;
- `test_single_selection_cardinality_rejects_conflicting_or_unknown_modes`;
- `test_public_api_query_items_keeps_selection_in_projection_cache_key`;
- `test_public_response_cache_separates_pair_and_single_projections`.

Build both a paired and a single normalized query tuple and assert they differ. Adding an unsupported query key must change neither tuple. Exercise `_build_public_api_response_cached` in pair -> single -> pair order and assert the final pair cache hit retains the exact pair payload while the single response retains only A.

At the public HTTP boundary, add requests proving missing marker, marker+B conflict, and `scope=all&selection=single` return 400. Patch the builders/source readers with poisoned callables and assert none is invoked for those rejected queries.

- [ ] **Step 4: Cardinality/cache cycle — verify RED**

Run:

```bash
python3 -m unittest tests.test_dashboard -v
```

Expected: FAIL because the explicit marker is not accepted, is absent from the allowlist/cache key, and the HTTP boundary still reaches old validation.

- [ ] **Step 5: Cardinality/cache cycle — implement the shared gate and cache identity**

Use the gate from both direct builders and `_validate_public_api_client_query()`:

```python
def validate_market_selection_cardinality(
    market_a_id: str | None,
    market_b_id: str | None,
    selection: str | None,
) -> str:
    if selection not in (None, "", "single"):
        raise PublicClientRequestError("selection must be single when provided")
    if not market_a_id:
        raise PublicClientRequestError("market_a is required")
    if selection == "single":
        if market_b_id:
            raise PublicClientRequestError(
                "market_b must be omitted when selection=single"
            )
        return "single"
    if not market_b_id:
        raise PublicClientRequestError(
            "market_b is required unless selection=single"
        )
    if market_a_id == market_b_id:
        raise PublicClientRequestError(
            "market_a and market_b must be different"
        )
    return "pair"
```

Continue to validate Token presence and catalog membership separately. For Quality, reject any `selection` when `scope=all`.

Bind `selection` to projection/cache identity in the same minimal cycle:

Update `PUBLIC_API_QUERY_FIELDS` in this order:

```python
"compare": ("token", "market_a", "market_b", "selection", "start", "end"),
"execution_cost": ("token", "market_a", "market_b", "selection"),
"quality": (
    "token", "scope", "market_a", "market_b", "selection", "start", "end",
),
```

Because omitted fields are excluded from `public_api_query_items()`, existing paired cache tuples stay identical. Do not add `selection` to `api_source_signature()`; it is a response projection, not a publication source.

Forward `selection=query.get("selection")` from `_build_public_api_payload()` into all three builders.

- [ ] **Step 6: Cardinality/cache cycle — verify GREEN**

Run the focused new cardinality, HTTP-400, normalized-key, and alternating response-cache tests plus existing public API cache tests. Expected: GREEN before any Compare projection code is written; paired keys/responses remain exact.

- [ ] **Step 7: Compare cycle — write and verify failing single-response tests**

Add `test_compare_single_selection_returns_only_market_a_projection` and `test_paired_compare_projection_is_unchanged_when_selection_is_absent`. The valid single response must satisfy:

```python
self.assertEqual(result["selection_mode"], "single")
self.assertEqual(result["market_a"]["market_id"], cex_id)
self.assertIsNone(result["market_b"])
self.assertNotIn("market_b_statistics", result)
self.assertNotIn("latest_comparable_observation", result)
self.assertNotIn("comparison_days", result["metadata"])
self.assertEqual(result["latest_market_a_observation"], result["observations"][-1])
self.assertTrue(all(set(row) == {"date", "market_a"} for row in result["observations"]))
```

Run the same assertions with a CEX ID and a DEX ID as A. Spy on `selected_market_rows()` and prove only A is requested; do not corrupt the shared catalog source. Add an empty-series case with `observations == []`, `latest_market_a_observation is None`, structured unavailable statistics, and no B placeholder. Run the focused tests and observe a response-contract RED.

- [ ] **Step 8: Compare cycle — implement the single branch**

In `build_market_comparison()`, resolve and load only A in single mode. Extract the current inline catalog/window metadata into `comparison_metadata(*, observation_days, comparison_days=None)`. The helper always returns the existing contract, availability, source-range, freshness, requested-window, source, storage, and generation fields. When `comparison_days is None`, it adds only `observation_days`; otherwise it adds the existing `comparison_days` and `union_observation_days` keys, with `observation_days` supplying the latter value. Use that pair form from the existing branch so its serialized fields and values stay unchanged. Then return:

```python
single_payload = {
    "selection_mode": "single",
    "metadata": comparison_metadata(observation_days=len(observations)),
    "token_symbol": token,
    "market_a": market_a,
    "market_b": None,
    "market_a_statistics": statistics_a,
    "latest_market_a_observation": observations[-1] if observations else None,
    "observations": observations,
}
```

Do not include `market_b_statistics`, `latest_comparable_observation`, `comparison_days`, or any pair-derived number in this branch. Leave the existing pair branch byte/schema compatible.

- [ ] **Step 9: Compare cycle — verify GREEN and Python 3.8 grammar**

Run:

```bash
python3 -m unittest tests.test_market_facts tests.test_dashboard tests.test_framework -v
python3 -m py_compile dashboard/market_facts.py dashboard/server.py
```

Expected: PASS, including exact null/zero preservation and existing A/B comparison tests.

- [ ] **Step 10: Commit**

```bash
git add dashboard/market_facts.py dashboard/server.py \
  tests/test_market_facts.py tests/test_dashboard.py
git commit -m "feat(api): project intentional single-market compare"
```

### Task 4: Add single-market Execution and selected Quality projections

**Files:**
- Modify: `dashboard/server.py`
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_public_quality_overlay.py`

**Interfaces:**
- Extends: `build_execution_cost_comparison(..., *, selection=None, source_signature=None)`.
- Extends: `build_market_quality(..., *, selection=None)`.
- Preserves: Quality contract version 4 and all paired response fields.

- [ ] **Step 1: Write failing single Execution tests**

Add `test_execution_cost_single_selection_loads_only_market_a_and_has_no_snapshot_skew`. Configure a valid CEX A source and deliberately malformed unselected DEX execution source. Assert:

```python
self.assertEqual(payload["selection_mode"], "single")
self.assertEqual(payload["market_a"]["market"]["market_id"], cex_id)
self.assertIsNone(payload["market_b"])
self.assertIsNone(payload["metadata"]["snapshot_skew_seconds"])
self.assertEqual(set(payload["metadata"]["snapshots"]), {"cex"})
self.assertEqual(set(payload["metadata"]["cohort_lineage"]), {"cex"})
self.assertEqual(len(payload["market_a"]["rows"]), 10)
```

Parameterize the producer contract for CEX-as-A and DEX-as-A. Each case must validate only A's source family and must tolerate the opposite, unselected family being malformed. Apply the same two A identities to selected-Quality inventory and rollup assertions.

Add producer negative cases for missing marker, marker+B conflict, unknown A, and proof that a malformed unselected DEX execution source is never loaded. Response-level rejection of non-null skew or leaked DEX lineage belongs to the release-validator tests in Task 8.

- [ ] **Step 2: Write failing single selected-Quality tests**

Add `test_quality_selected_single_returns_exact_market_a_identity_inventory`. Assert:

```python
self.assertEqual(payload["metadata"]["scope"], "selected")
self.assertEqual(payload["metadata"]["selected_market_ids"], [cex_id])
self.assertEqual([row["market_id"] for row in payload["markets"]], [cex_id])
self.assertEqual(
    [row["market_id"] for row in payload["metadata"]["daily_quality_report"]["market_issue_rollups"]],
    [cex_id],
)
```

Verify status/reason/retryability/action, freshness, lineage, and structured quality flags remain the existing v4 shapes. `scope=all&selection=single`, selected A without marker, marker+B, and a wrong-Token A must fail.

- [ ] **Step 3: Verify RED**

Run:

```bash
python3 -m unittest tests.test_dashboard tests.test_public_quality_overlay -v
```

Expected: FAIL because execution and selected Quality currently require two IDs.

- [ ] **Step 4: Implement the Execution single branch**

Build `selected_markets = [market_a]` in single mode and `[market_a, market_b]` in pair mode. Derive `selected_market_types`, snapshots, and cohort lineage only from that list. Compute `market_result()` once for A in single mode. Extract the current inline execution metadata dictionary unchanged into `execution_metadata(*, snapshot_skew_seconds, cohort_lineage, snapshots)`, where `snapshots` is converted with `_execution_snapshot_metadata()` exactly as it is today. Call the helper from both branches so the pair payload remains byte/schema compatible. The single return is:

```python
single_payload = {
    "selection_mode": "single",
    "metadata": execution_metadata(
        snapshot_skew_seconds=None,
        cohort_lineage=cohort_lineage,
        snapshots=snapshots,
    ),
    "token_symbol": token,
    "market_a": result_a,
    "market_b": None,
}
```

Never load, validate, or mention an unselected source family. Leave the existing two-result skew calculation unchanged in pair mode.

- [ ] **Step 5: Implement selected Quality cardinality without a contract bump**

For `scope=selected`, use the shared gate and select:

```python
selected_ids = (
    [market_a_id]
    if selection_mode == "single"
    else [market_a_id, market_b_id]
)
token_markets = [by_id[market_id] for market_id in selected_ids]
```

The existing downstream issue filtering, fact aggregation, execution-source loading, and daily report rollups already derive from `token_markets`; keep that boundary. Do not add a second identity inventory: `metadata.selected_market_ids` plus the exact returned `markets` rows is authoritative. Do not add pair fields to Quality merely to mirror Compare.

- [ ] **Step 6: Verify GREEN and paired non-regression**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard \
  tests.test_public_quality_overlay \
  tests.test_execution_cost \
  tests.test_framework -v
```

Expected: PASS. Existing two-market execution skew and two-ID selected Quality tests remain unchanged.

- [ ] **Step 7: Commit**

```bash
git add dashboard/server.py tests/test_dashboard.py tests/test_public_quality_overlay.py
git commit -m "feat(api): bound execution and quality to Market A"
```

### Task 5: Make Market B optional without adding a mode switch

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/package.json`
- Modify: `dashboard/package-lock.json`
- Modify: `.gitignore`
- Create: `dashboard/playwright.config.js`
- Create: `dashboard/tests/fixtures/single-market-responses.json`
- Create: `dashboard/tests/single-market-selection.spec.js`
- Modify: `tests/test_dashboard_frontend.py`

**Interfaces:**
- Produces canonical UI state: `{marketA, marketB, selection}`.
- Saves a valid single record as `{marketA, marketB: "", selection: "single"}` under the existing per-Token session key.
- Changes the command label to `Apply selection`.

- [ ] **Step 1: Establish the real-browser harness and a failing Apply flow**

Pin `playwright` 1.62.1 as a dev dependency, add `test:browser: "playwright test"`, and generate the npm lockfile with npm 10.9.4 (`pnpm dlx npm@10.9.4 install --package-lock-only --ignore-scripts` from `dashboard/`) if a local npm executable is unavailable. Create `playwright.config.js` with a real dashboard server on 127.0.0.1:8767, root `/` as the readiness URL, and desktop Chrome plus a Chromium `Pixel 5` profile overridden to 390x844. Require `playwright/test`, matching the pinned full Playwright package and the verified bundled runtime.

Add `dashboard/test-results/` and `dashboard/playwright-report/` to `.gitignore` before the first browser run so traces/screenshots cannot dirty the branch or enter a commit.

```javascript
const { defineConfig, devices } = require("playwright/test");

module.exports = defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  use: {
    baseURL: "http://127.0.0.1:8767",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "cd .. && python3 dashboard/server.py --host 127.0.0.1 --port 8767",
    url: "http://127.0.0.1:8767/",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    {
      name: "desktop",
      use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } },
    },
    {
      name: "mobile",
      use: {
        ...devices["Pixel 5"],
        browserName: "chromium",
        viewport: { width: 390, height: 844 },
      },
    },
  ],
});
```

Create a checked fixture with concrete AAVE Summary, one-market and two-market catalogs, single/pair Compare, Execution, selected Quality, and Token Events payloads. Install every API route interception before `page.goto()`; never mock HTML, CSS, `navigation.js`, or `app.js`.

Install and verify Chromium before interpreting any browser RED:

```bash
node "$NODE_PATH/playwright/cli.js" install chromium
node -e "const fs=require('fs'); const {chromium}=require('playwright'); fs.accessSync(chromium.executablePath(), fs.constants.X_OK); console.log(chromium.executablePath())"
```

If installation or the executable check is blocked, stop before editing production UI and report the browser infrastructure blocker. A missing executable is not an acceptable feature RED.

Write `single selection can be confirmed when Market B starts empty`. Load the one-market catalog, verify B already has the empty `Market A only — no comparison` option, click `Apply selection` without relying on a `change` event, and assert navigation to Compare with exact A plus `selection=single` and no `marketB`.

Also write `restoring Market B restores the pair contract` with the two-market catalog: apply single, choose a real B, apply again, and assert the marker disappears, B/spread controls return, and the pair survives refresh. Run both with:

```bash
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --project=desktop \
  --grep "can be confirmed when Market B starts empty|restoring Market B restores the pair contract"
```

Expected: RED before the UI implementation. A skipped/missing browser is not a passing RED.

- [ ] **Step 2: Write failing accessible-control and persistence tests**

Add behavioral tests for:

- Market B label and `aria-label` equal `Market B (optional)`;
- Market B's first option has empty value and exact text `Market A only — no comparison`;
- Market A retains `Select market` as its empty option;
- command text is `Apply selection`;
- applying exact A + explicit empty-B choice stores the explicit marker and navigates to Compare;
- merely loading with B absent does not create the marker;
- automatically clearing an equal B does not create the marker;
- an incomplete/invalid draft does not delete the previous valid session record;
- BTC single and ETH pair selections restore independently when switching Tokens;
- selecting a real B replaces the saved single record with a marker-free pair.
- a valid single workspace URL contains no legacy `pairMode=manual` parameter.

- [ ] **Step 3: Write failing invalid-deep-link tests**

For `selection=single&marketA=<exact>&marketB=<exact>`, an unknown A, and `marketA=<exact>&selection=bogus`, assert the raw IDs/marker remain visible in the bounded error, no default is selected, no data request starts, and `history.replaceState` does not produce a repaired valid link. Mirror the bogus-marker case in Playwright so real hydration also proves zero API requests and zero canonicalizing `replaceState` calls.

- [ ] **Step 4: Verify unit RED**

Run: `python3 -m unittest tests.test_dashboard_frontend -v`

Expected: FAIL because current session and route code treats every empty B as incomplete and deletes prior records.

- [ ] **Step 5: Add canonical workspace selection state**

Add `workspaceSelection: ""` to `app` and replace `selectedPairState()` calls in workspace code with:

```javascript
function selectedMarketSelection() {
  return {
    marketA: byId("facts-market-a")?.value || "",
    marketB: byId("facts-market-b")?.value || "",
    selection: app.workspaceSelection === "single" ? "single" : "",
  };
}
```

Keep a small `selectedPairState()` compatibility wrapper only for existing transient pair-only callers until they are migrated. Include `selection` in snapshot-refresh and workspace request context keys.

In `currentWorkspaceRouteState()`, set `pairMode=manual` only when A is missing or when B is missing and `selection !== "single"`. A valid single route therefore carries only A plus `selection=single`; it never carries the legacy incomplete-state marker.

- [ ] **Step 6: Migrate session records without breaking old pairs**

Retain `market-monitor:token-pairs:v1`; the schema change is additive. Normalize only these saved forms:

```javascript
function normalizedSavedSelection(record) {
  if (!record || typeof record !== "object") return null;
  if (record.selection === "single" && record.marketA && !record.marketB) {
    return { marketA: record.marketA, marketB: "", selection: "single" };
  }
  if (!record.selection && record.marketA && record.marketB) {
    return { marketA: record.marketA, marketB: record.marketB, selection: "" };
  }
  return null;
}
```

Rename `persistSelectedPair()` to `persistSelectedSelection()`. It may overwrite storage only after `validateSelection()` returns valid. An incomplete or invalid draft leaves the last valid record untouched. Remove the current `delete app.pairSelections[newToken]` behavior from `selectWorkspaceToken()`.

`selectWorkspaceToken(newToken)` must normalize the destination Token's saved record before navigation. When valid saved state exists, construct the destination route directly from its A/B/selection and omit `pairMode`; when none exists, call `workspaceStateWithoutMarkets()`, which must delete A, B, and any inherited `selection` before setting `pairMode=manual`. Pass the normalized saved record into `populateFactsMarkets()`: a saved single disables `allowDefaults` for B, while a saved pair restores both exact IDs. Catalog validation happens after the destination catalog loads, so IDs from the previous Token can never leak.

- [ ] **Step 7: Render the exact optional B option and mode-neutral copy**

Change `factsOptions()` to accept an empty label:

```javascript
function factsOptions(markets, selectedId, { emptyLabel = "Select market" } = {}) {
  return [
    `<option value="">${escapeHtml(emptyLabel)}</option>`,
    ...markets.map((market) => (
      `<option value="${escapeHtml(market.market_id)}" ${market.market_id === selectedId ? "selected" : ""}>`
        + `${escapeHtml(factsMarketLabel(market))}</option>`
    )),
  ].join("");
}
```

Call it for B with `emptyLabel: "Market A only — no comparison"`. Update pair-only Markets copy (`A/B selectable`, `Set any two`, `Pair Actions`, warning link text) to mode-neutral market-selection wording while retaining `Set A` and `Set B` actions.

- [ ] **Step 8: Hydrate/apply the explicit marker**

In `applyWorkspaceRoute()`:

- validate raw route A/B/selection before defaults;
- hydrate exact A + empty B for a valid single route;
- never default B in valid single mode;
- keep no-marker/no-B on the existing incomplete/manual path;
- skip `canonicalizeCurrentRoute()` while a shared selection is invalid;
- persist only valid non-transient selections;
- announce either exact A/B or `Market A only`.

In `selectWorkspaceMarket("b", value)`, set `app.workspaceSelection = "single"` only when the user explicitly chooses empty B; clear the marker when a real B is chosen. If code clears B automatically because it equals A, clear the marker too so the state remains incomplete.

- [ ] **Step 9: Apply and navigate**

Rename `applySelectedPair()` to `applySelectedSelection()`. Pressing Apply is itself an explicit user action: when A is exact, B is empty, and there is no conflicting/unknown marker, set `app.workspaceSelection = "single"` immediately before validation. This makes a one-market catalog and an incomplete no-B link intentionally confirmable without pretending that page load created single mode. A valid single or pair then calls:

```javascript
navigateTo(currentWorkspacePath("compare"));
```

An invalid selection stays on the current page with a bounded error and starts no Compare/execution/Quality request.

- [ ] **Step 10: Verify GREEN in unit and real browser**

Run:

```bash
python3 -m unittest tests.test_navigation tests.test_dashboard_frontend \
  tests.test_opportunity_frontend tests.test_public_actions_frontend -v
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --project=desktop \
  --grep "can be confirmed when Market B starts empty|restoring Market B restores the pair contract"
```

Expected: PASS, including per-Token restoration and the unchanged pair flow.

- [ ] **Step 11: Commit**

```bash
git add dashboard/static/index.html dashboard/static/app.js .gitignore \
  dashboard/package.json dashboard/package-lock.json \
  dashboard/playwright.config.js dashboard/tests/fixtures/single-market-responses.json \
  dashboard/tests/single-market-selection.spec.js tests/test_dashboard_frontend.py
git commit -m "feat(web): apply optional Market B selection"
```

### Task 6: Render Compare as a true Market-A-only page

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/styles.css`
- Modify: `tests/test_compare_chart_frontend.py`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_event_frontend.py`
- Modify: `dashboard/tests/single-market-selection.spec.js`

**Interfaces:**
- Consumes: single Compare API contract from Task 3.
- Produces: one A Price/Volume line, A statistics, Event overlay, and a three-column daily table.
- Produces: request ownership keyed by Token, A, B, marker, page, and applied window.

- [ ] **Step 1: Write failing single-chart model tests**

Use a literal single payload with one measured zero, one null, and one nonconsecutive date. Assert:

- Price and Volume models contain only A series;
- spread is not an allowed single metric;
- zero remains plotted and null/calendar gaps break the line;
- the A marker is centered at offset 0;
- legend and tooltip never mention B, comparable days, or Daily Price Gap;
- Event marker/timing copy remains and does not claim causality.

Add a transition test: if `app.comparisonMetric === "spread"`, entering single mode normalizes it to `price`; restoring a real B makes the spread control available again.

- [ ] **Step 2: Write failing single renderer and ownership tests**

Assert single Compare:

- shows A return and A volatility with existing structured `N/A` rules;
- hides all `[data-pair-only]` comparable/gap/B controls and summary cards;
- produces exactly Date, A Price, and A Volume columns;
- has no `market_b_missing`, pair copy, blank B cells, or eight-column loading row;
- requests `token`, `market_a`, `selection=single`, start/end and omits `market_b`;
- rejects a current response with wrong Token/A/mode as a page-local contract error;
- ignores a delayed old single response after the user applies a pair, even if that old request resolves last;
- ignores a delayed catalog/snapshot-generation refresh captured under pair mode after the user applies single mode, proving the marker participates in the refresh owner key;
- keeps already rendered A facts when the independent Event request fails.

Before implementation, extend `dashboard/tests/single-market-selection.spec.js` with `single Compare renders only Market A`. Run it in both configured projects against the single Compare fixture. Assert the three table headings, one chart identity, centered A marker, hidden pair-only roles absent from the accessibility/focus order, retained Event overlay, no unexpected console/page error, and no horizontal overflow at 390x844.

Add both unit and browser fixtures for a valid single response with `observations: []`, `latest_market_a_observation: null`, and structured unavailable statistics. Assert the page renders a bounded empty/N/A state, does not crash, and never creates a B legend, B placeholder, comparable card, or pair-derived table column.

- [ ] **Step 3: Verify RED**

Run:

```bash
python3 -m unittest \
  tests.test_compare_chart_frontend \
  tests.test_dashboard_frontend \
  tests.test_event_frontend -v
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --grep "single Compare renders only Market A"
```

Expected: FAIL because the current renderer dereferences Market B and always exposes spread/comparable UI.

- [ ] **Step 4: Add stable pair-only hooks and mode application**

Mark B, gap, comparable, and spread-control nodes in `index.html` with `data-pair-only`. Add dynamic IDs for the heading, caption, table region label, and chart description. Do not mark the optional Market B selector itself pair-only; it remains visible so a user can restore comparison.

Implement:

```javascript
function applyWorkspaceSelectionMode(mode) {
  const single = mode === "single";
  byId("facts-workbench").dataset.selectionMode = single ? "single" : "pair";
  document.querySelectorAll("[data-pair-only]").forEach((element) => {
    element.hidden = single;
  });
  if (single && app.comparisonMetric === "spread") {
    app.comparisonMetric = "price";
  }
}
```

Call this on loading, valid route hydration, render, mode transition, and unavailable/error resets so stale B DOM never survives a Token or selection change.

- [ ] **Step 5: Branch the chart and table on the response contract**

`comparisonChartModel()` must inspect `payload.selection_mode === "single"`. In that branch, create only `series-a`, omit spread definitions, and use centered A markers. `renderComparison()` consumes `latest_market_a_observation`, `market_a_statistics`, and `{date, market_a}` rows. It must not read `payload.market_b` or pair metadata in the single branch.

Keep the existing pair branch intact, including B shapes, pair tooltips, spreads, missing reasons, and eight-column table.

- [ ] **Step 6: Add exact request ownership and response identity checks**

Introduce a reusable immutable key:

```javascript
function workspaceRequestKey(page, selection, extra = {}) {
  return JSON.stringify({
    page,
    token: selectedWorkspaceToken(),
    marketA: selection.marketA,
    marketB: selection.marketB,
    selection: selection.selection,
    start: appliedTimeWindow().start,
    end: appliedTimeWindow().end,
    ...extra,
  });
}
```

`loadComparison()` captures both `requestId` and key. Before every `app.comparison`, DOM, status, Event-overlay, or controller commit, require both still match. Validate a current single payload with:

```javascript
payload.selection_mode === "single"
&& payload.token_symbol === token
&& payload.market_a?.market_id === marketA
&& payload.market_b === null
```

For pair mode require exact A and B identities and no single marker. Invalidate Compare immediately on A/B/Token/window/apply/navigation changes.

- [ ] **Step 7: Collapse single-mode layout without blank columns**

Use the workbench data attribute only for layout:

```css
.facts-workbench[data-selection-mode="single"] .comparison-summary {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.facts-workbench[data-selection-mode="single"] .comparison-table {
  min-width: 640px;
}

@media (max-width: 640px) {
  .facts-workbench[data-selection-mode="single"] .comparison-table {
    min-width: 0;
  }
}
```

Keep the existing global `[hidden]` rule as the accessibility/layout authority. The real-browser test, rather than a fake DOM or source-text assertion, owns the 390px overflow, focus-order, and accessibility checks.

- [ ] **Step 8: Verify GREEN and pair restoration**

Run:

```bash
python3 -m unittest \
  tests.test_compare_chart_frontend \
  tests.test_dashboard_frontend \
  tests.test_event_frontend \
  tests.test_navigation -v
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --grep "single Compare renders only Market A"
```

Expected: PASS. Switching single -> pair must restore all existing B/spread/comparable behavior.

- [ ] **Step 9: Commit**

```bash
git add dashboard/static/index.html dashboard/static/app.js \
  dashboard/static/styles.css tests/test_compare_chart_frontend.py \
  tests/test_dashboard_frontend.py tests/test_event_frontend.py \
  dashboard/tests/single-market-selection.spec.js
git commit -m "feat(web): render Market A without comparison"
```

### Task 7: Bound Liquidity, Execution, Quality, and Events to the selection

**Files:**
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/styles.css`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_event_frontend.py`
- Modify: `dashboard/tests/single-market-selection.spec.js`

**Interfaces:**
- Consumes: single Execution and Quality API contracts from Task 4.
- Preserves: Liquidity's catalog-derived A facts and Token-scoped Event API.
- Extends: exact request ownership to execution and selected Quality.

- [ ] **Step 1: Liquidity cycle — write and verify failing behavior tests**

Add tests proving that single mode:

- renders only A total/directional depth series and A legend from a payload whose B side is poisoned/missing;
- succeeds with observable A output and no skew/B DOM, without asserting which private helper functions were called;
- renders five Liquidity table columns: Band, A Total, A Sell, A Buy, A Completeness;
- hides B depth summary/meta, snapshot skew, paired-band count, and every B cell;
- keeps A completeness, lower-bound, timing, flags, and structured `N/A` reasons.

Run the focused Liquidity renderer tests and observe an assertion RED caused by visible/derived B output, not by a poisoned helper crash.

- [ ] **Step 2: Liquidity cycle — implement the A-only branch and verify GREEN**

In `renderLiquidityCurve()`, derive the canonical selection first. In single mode build only A series, summary, table, and metadata; add `data-pair-only` hooks for B/skew/paired DOM and dynamic singular captions. Do not derive paired bands or construct B status arrays. Re-run only the Liquidity-focused tests until GREEN, then run the existing paired Liquidity regressions.

- [ ] **Step 3: Execution cycle — write and verify failing behavior tests**

Add focused tests proving the request carries A + marker with no B; the response accepts only exact A identity, `market_b=null`, and null skew; the renderer shows only A cost/fill/status/timing and A fee scope; notional and direction controls redraw the owned payload; and an Execution failure never clears Compare. Observe the intended assertion RED.

- [ ] **Step 4: Execution cycle — implement the A-only loader/renderer and verify GREEN**

Single `renderExecution()` uses only `payload.market_a`, one timing card, A scenarios, and `executionFeeScope([selectedA])`; it never builds B timing/rows. The request-owner key contains only Token/A/B/marker because the response already contains all scenarios. Validate exact owner and identity before render; a current wrong identity produces only an Execution contract error. Re-run focused Execution tests to GREEN before continuing.

- [ ] **Step 5: Quality/Events cycle — write failing behavior tests**

Assert that in single mode:

- Data Quality forces effective `scope=selected` and hides the All/Selected-A&B group;
- request URL is exactly bounded by Token, start/end, `scope=selected`, A, and marker, with no B;
- returned `metadata.selected_market_ids` must equal `[marketA]` and every row must be that exact A;
- endpoint failure may show only an A-filtered catalog fallback, never the full catalog;
- status/caption is singular and contains no pair count copy;
- workspace Events URLs retain A + marker across tabs, refresh, and back/forward;
- `/api/markets/events` requests contain only Token/date/lifecycle/clock filters and never A/B/selection;
- an Event failure changes only Event status/overlay and leaves other page facts intact.

Run the focused Quality/Event tests and observe assertion REDs for wrong scope/cardinality or leaked market fields; correct fixture/setup errors before implementation.

- [ ] **Step 6: Quality/Events cycle — implement exact A scope and verify GREEN**

Add `effectiveQualityScope(selection)` and use it in route state, visible controls, query construction, and owner keys. Single Quality always requests selected exact A, validates ordered `[A]`, and filters any catalog fallback to A. Events remain Token-only; workspace links retain selection, and Event failure never clears Compare. Re-run the focused Quality/Event tests to GREEN before continuing.

- [ ] **Step 7: Ownership cycle — write failing race and navigation-away tests**

Create deferred Compare, execution, and Quality responses. Start A-only, apply A/B before the old responses resolve, resolve the new requests, then resolve the old ones. Assert the latest pair result remains in `app.*` and DOM. Repeat with Token and date-window changes. Assert old abort signals fire and late completions make no DOM/status assignments.

Add a navigation-away case for each owned loader: start the request, navigate to another research page before it resolves, then resolve it. The old Compare, execution, or Quality response must not commit application state, DOM, or status after route ownership changed. Mirror the most integration-sensitive case in Playwright with delayed intercepted responses.

- [ ] **Step 8: Ownership cycle — implement owner keys/invalidation and verify GREEN**

Use immutable keys containing page, Token, A, B, marker, and applied window, plus only page controls that change the network response. Invalidate/abort on A/B/Token/window/apply/navigation changes and require request ID plus key before every state/DOM/status commit. Re-run the focused deferred-response tests to GREEN, including the catalog/snapshot generation-refresh marker regression.

- [ ] **Step 9: Layout/browser cycle — write and verify failing real-browser tests**

Extend the Playwright spec with `single selection survives research pages`, `single mobile layout has no pair slots`, `latest selection owns async responses`, and `page failures are isolated`. Exercise all four research pages, refresh, history, delayed responses, and independent failures. At 390x844 use real role queries and Tab presses to prove B/skew/scope controls are absent from layout, focus, and accessibility, and assert no horizontal overflow or unexpected console/page error.

Run:

```bash
python3 -m unittest tests.test_dashboard_frontend tests.test_event_frontend -v
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --grep "single selection survives research pages|single mobile layout|latest selection owns async responses|page failures are isolated"
```

Expected: RED on observable browser layout/accessibility assertions after the page logic cycles are already GREEN; missing Chromium or fixture errors are not acceptable REDs.

- [ ] **Step 10: Layout/browser cycle — implement single layout and accessibility**

Add single-mode grid/table overrides:

```css
.facts-workbench[data-selection-mode="single"] .liquidity-summary,
.facts-workbench[data-selection-mode="single"] .execution-summary {
  grid-template-columns: 1fr;
}

.facts-workbench[data-selection-mode="single"] .liquidity-table,
.facts-workbench[data-selection-mode="single"] .execution-table {
  min-width: 640px;
}

@media (max-width: 640px) {
  .facts-workbench[data-selection-mode="single"] .liquidity-table,
  .facts-workbench[data-selection-mode="single"] .execution-table {
    min-width: 0;
  }
}
```

Use `hidden`, not opacity or off-screen positioning, for B/skew/scope controls. The optional B selector remains focusable on Markets so users can restore a pair. Browser tests own focus/accessibility/viewport proof; Node unit tests own state, URL, request, and render-model contracts.

- [ ] **Step 11: Layout/browser cycle — verify GREEN and page isolation**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard_frontend \
  tests.test_compare_chart_frontend \
  tests.test_event_frontend \
  tests.test_navigation -v
node "$NODE_PATH/playwright/cli.js" test \
  -c dashboard/playwright.config.js \
  --grep "single selection survives research pages|single mobile layout|latest selection owns async responses|page failures are isolated"
```

Expected: PASS for A-only, pair restoration, races, failure isolation, keyboard-hidden nodes, and 390px structural assertions.

- [ ] **Step 12: Commit**

```bash
git add dashboard/static/index.html dashboard/static/app.js \
  dashboard/static/styles.css tests/test_dashboard_frontend.py \
  tests/test_event_frontend.py dashboard/tests/single-market-selection.spec.js
git commit -m "feat(web): scope research pages to Market A"
```

### Task 8: Extend release validation and document the public contract

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_release_smoke.py`
- Modify: `README.md`
- Modify: `dashboard/README.md`
- Modify: `docs/market-monitor-design.md`
- Modify: `docs/market-facts-contract.md`
- Modify: `docs/execution-cost-data-contract.md`
- Modify: `docs/production-hardening.md`

**Interfaces:**
- Preserves all existing paired release calls and validators.
- Adds one Compare, one selected Quality, and one execution-cost single smoke per release check.
- Produces release result `single_market_smoke` with exact endpoint count and selected A identity.

- [ ] **Step 1: Write failing single validator counterexamples**

In `tests/test_release_smoke.py`, create valid single fixtures, then independently reject:

- wrong/missing Compare or Execution `selection_mode`;
- non-null Market B;
- B statistics, comparable/spread fields, or non-`{date, market_a}` Compare rows;
- latest-A observation not equal to the last row;
- wrong Quality `selected_market_ids`, extra market row, or extra report rollup;
- non-null execution snapshot skew;
- snapshot/cohort lineage for an unselected market family;
- wrong Token/A identity or generation on any single endpoint.

Keep all existing paired fixtures and mutations unchanged.

- [ ] **Step 2: Write failing release orchestration tests**

Extend the mocked full release check to recognize and record six expert endpoint URLs: three existing pair requests and three new single requests. Assert single URLs omit `market_b`, include `selection=single`, use `scope=selected` for Quality, and bind to the same Summary generation. A server returning the wrong A for one single URL must fail the release.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke tests.test_framework -v`

Expected: FAIL because the checker validates only paired projections.

- [ ] **Step 4: Branch validators explicitly**

Extend validator signatures with an expected mode while keeping `pair` as the default. For single Compare require:

```python
require(payload.get("selection_mode") == "single", "Compare mode is wrong")
require(payload.get("market_b") is None, "Compare leaked Market B")
require(
    all(set(row) == {"date", "market_a"} for row in observations),
    "Compare leaked pair-derived observation fields",
)
```

For single Execution validate only A's ten scenarios and selected market-type lineage, and require null B/skew. For single Quality derive `expected_ids = [market_a]` and reuse the existing v4 fact/evidence reconciliation for one market. Quality uses `metadata.selected_market_ids`; it does not require a new top-level mode field.

- [ ] **Step 5: Add bounded single release requests**

After the existing pair checks in `release_check()`, request:

```python
single_query = {
    "token": token,
    "market_a": market_a,
    "selection": "single",
}
```

Use it for Compare with start/end, Quality with `scope=selected`, and execution-cost. Append all response metrics and return:

```python
"single_market_smoke": {
    "market_a": market_a,
    "endpoint_count": 3,
}
```

Do not weaken the final Health/Summary reread, source-generation equality, asset hash, gzip, or payload budget checks.

- [ ] **Step 6: Update durable contract documentation**

Document all of the following without rewriting historical evidence in `docs/senior-report.md`:

- A is required; B is optional only with the explicit marker;
- empty B and structured `N/A` are different states;
- paired URLs and API shapes remain unchanged;
- exact single Compare and execution response fields;
- exact one-ID selected Quality inventory;
- page-by-page hidden pair facts and unchanged Token Events;
- per-Token session restore and request ownership;
- line charts remain; candlesticks/multi-market/Benchmark are deferred;
- release acceptance now exercises both paired and single projections;
- production deployment and public-browser evidence are separate from code completion.

Review those documentation statements as a human checklist against the shipped API/UI tests. Do not add source-text or prose assertions to `tests/test_framework.py`; executable behavior belongs to the API, frontend, browser, and release tests above.

- [ ] **Step 7: Verify GREEN, budgets, and Python 3.8 grammar compatibility**

Run:

```bash
python3 -m unittest tests.test_release_smoke tests.test_framework -v
python3 -m py_compile dashboard/server.py dashboard/market_facts.py \
  scripts/check_dashboard_release.py
python3 -c "import dashboard.server; import dashboard.market_facts; import scripts.check_dashboard_release"
git diff --check
```

Expected: PASS. Static gzip bytes must remain below the existing 220,000-byte gate; record the measured total rather than copying the planning snapshot. `tests.test_framework` proves Python 3.8 grammar only in this desktop environment. If a Python 3.8.10 interpreter is unavailable locally, record runtime import/full-suite compatibility as pending the separate Tencent deployment preflight rather than claiming it passed.

- [ ] **Step 8: Commit**

```bash
git add scripts/check_dashboard_release.py tests/test_release_smoke.py \
  README.md dashboard/README.md docs/market-monitor-design.md \
  docs/market-facts-contract.md docs/execution-cost-data-contract.md \
  docs/production-hardening.md
git commit -m "test(release): validate single-market projections"
```

### Task 9: Complete browser regression and run the final implementation gate

**Files:**
- Verify only; any correction discovered here must first gain a focused failing test in the owning Task 2-8 test file.

**Interfaces:**
- Runs: the pinned real-browser suite established in Task 5 and extended RED-first in Tasks 6 and 7.
- Uses the real HTML/CSS/JavaScript bundle served by the local dashboard process.
- Intercepts API calls only for deterministic browser race/error fixtures; a later public deployment still requires unmocked browser smoke.

- [ ] **Step 1: Verify the complete browser suite**

Recheck the already installed Chromium executable, then run:

```bash
node -e "const fs=require('fs'); const {chromium}=require('playwright'); fs.accessSync(chromium.executablePath(), fs.constants.X_OK)"
node "$NODE_PATH/playwright/cli.js" test -c dashboard/playwright.config.js
```

Expected: both desktop and mobile Chromium projects PASS. If Chromium installation cannot complete because dependency access is unavailable, report the exact command as a blocked browser gate; do not silently downgrade to Node DOM tests or claim browser completion.

- [ ] **Step 2: Run all focused and complete suites**

Run:

```bash
python3 -m unittest \
  tests.test_navigation \
  tests.test_market_facts \
  tests.test_dashboard \
  tests.test_dashboard_frontend \
  tests.test_compare_chart_frontend \
  tests.test_event_frontend \
  tests.test_public_quality_overlay \
  tests.test_release_smoke \
  tests.test_framework -v

python3 -m unittest discover -s tests -v
python3 -m py_compile dashboard/server.py dashboard/market_facts.py \
  scripts/check_dashboard_release.py
python3 -c "import dashboard.server; import dashboard.market_facts; import scripts.check_dashboard_release"
git diff --check
git status --short
```

If loopback HTTP tests are denied only by the execution sandbox, rerun those exact tests with loopback permission; do not classify a socket-denied error as a product failure or skip it in final evidence. The full suite must have no unexplained failures.

- [ ] **Step 3: Run a local unmocked candidate gate when a reviewed runtime data root is available**

Start the candidate against that read-only data root on a non-production port. In the candidate worktree, compute identities before startup:

```bash
export CEX_DEX_CANDIDATE_DATA_DIR="/absolute/reviewed/data"
export CEX_DEX_EXPECTED_APPLICATION_SHA="$(git rev-parse HEAD)"
export CEX_DEX_EXPECTED_ASSET_SHA="$(python3 -c \
  'from dashboard.server import static_asset_sha; print(static_asset_sha())')"
```

Start a controlled foreground server in a separate terminal/session, retaining its session ID for shutdown:

```bash
CEX_DEX_RELEASE_SHA="$CEX_DEX_EXPECTED_APPLICATION_SHA" \
PORT=8767 ./scripts/run_dashboard.sh --data-dir "$CEX_DEX_CANDIDATE_DATA_DIR"
```

Then run from the first shell:

```bash
python3 scripts/check_dashboard_health.py --url http://127.0.0.1:8767/health
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8767 \
  --expected-application-sha "$CEX_DEX_EXPECTED_APPLICATION_SHA" \
  --expected-asset-sha "$CEX_DEX_EXPECTED_ASSET_SHA"
```

This must report the paired smoke plus `single_market_smoke.endpoint_count == 3`. If no reviewed runtime data root is present locally, record this gate as pending deployment preflight; do not fabricate passing evidence and do not touch production.

Stop only the retained candidate-server session after the checks; do not kill by a broad process pattern.

This local command proves real server/API projections, while the deterministic Playwright suite proves real UI behavior with intercepted APIs; it does not prove the browser-to-live-server seam. Record an unmocked Playwright smoke against the prospective Tencent release URL as deployment-pending and do not include that claim in code-completion evidence.

- [ ] **Step 4: Confirm the supported-runtime boundary**

Record the local Python version used for the full suite. If it is not Python 3.8.10, do not describe the desktop result as a Python 3.8 runtime pass. The later Tencent preflight must use the service's exact Python 3.8.10 interpreter for compile, import, focused/full tests, and the unmocked release checker before any deployment switch.

- [ ] **Step 5: Final branch review and handoff**

Inspect the complete diff and commit list. Confirm there are no collector, schema, publication pointer, Route Opportunity, Funding Rate, OHLC/candlestick, deployment, or unrelated user-file changes. Report code completion and any later live deployment separately, each with its exact SHA.
