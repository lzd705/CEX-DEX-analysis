# Time Window State Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make payload metadata the sole applied-window truth, keep Custom inputs draft-only, and commit a new window when its market summary succeeds.

**Architecture:** DOM date inputs remain the lightweight Custom draft store; no second global draft object is added. Every non-submit consumer reads `appliedTimeWindow()`. `applyWindow(candidate)` sends an explicit candidate to `loadMarket()`, updates the route only after summary success, and then lets the Workspace catalog load independently with latest-request ownership guards.

**Tech Stack:** Static browser JavaScript, HTML/CSS already implemented, Python `unittest`, executable Node.js frontend contract tests.

## Global Constraints

- `app.payload.metadata.start_date/end_date` are the only applied-window truth after initial hydration.
- `#date-start/#date-end` are Custom draft fields and cannot affect routes, links, APIs, exports, summary, or active state before Apply.
- A successful market summary is the applied-window commit point.
- Summary failure leaves applied data and URL unchanged and keeps the submitted Custom draft open.
- Catalog failure is independent: retain the committed summary/URL and show the existing catalog error.
- Latest request wins; stale summary/catalog work produces no UI, route, payload, draft, or focus mutation.
- Check route ownership before a data-generation-mismatch retry calls `loadMarket()`.
- Preserve current route shapes, API contracts, backend data, calculations, validation messages, Option A layout, focus, and ARIA behavior.
- Do not stage or commit `.superpowers/` artifacts.

---

## File map

- `dashboard/static/app.js`: state readers/writers, applied consumers, summary commit flow, catalog ownership.
- `tests/test_dashboard_frontend.py`: executable state, event-binding, request, route, and concurrency contracts.
- `tests/test_dashboard.py`: update the legacy static contract that currently requires Comparison to read DOM dates.

### Task 1: Isolate draft and applied window consumers

**Files:**

- Modify: `dashboard/static/app.js`
- Modify: `tests/test_dashboard_frontend.py`
- Modify: `tests/test_dashboard.py`

**Interfaces:**

- Produces `draftTimeWindow() -> {start: string, end: string}`.
- Produces `setDraftTimeWindow(window: {start: string, end: string}) -> void`.
- Preserves `appliedTimeWindow() -> {start: string, end: string}` as the committed reader.
- Extends `currentScreenerFilters({ window } = {})`, `currentWorkspaceRouteState(page, { window } = {})`, `currentWorkspacePath(page, { window } = {})`, and `replaceCurrentRoute({ window } = {})` with an optional explicit window; their default is applied state.

- [ ] **Step 1: Write an executable draft-isolation test**

Add `test_unsubmitted_custom_window_never_leaks_to_applied_consumers`. Its
fixture must set payload metadata to `2026-07-01..2026-07-29` and the DOM draft
to `2026-07-20..2026-07-22`, then exercise real production functions and
assert literal applied values for:

```python
self.assertEqual(result["screenerWindow"], {
    "start": "2026-07-01",
    "end": "2026-07-29",
})
self.assertEqual(result["workspaceWindow"], {
    "start": "2026-07-01",
    "end": "2026-07-29",
})
self.assertIn("start=2026-07-01", result["qualityUrl"])
self.assertIn("end=2026-07-29", result["qualityUrl"])
self.assertIn("start=2026-07-01", result["comparisonUrl"])
self.assertEqual(
    result["exportName"],
    "cex-dex-market-facts-2026-07-01-2026-07-29.csv",
)
self.assertEqual(result["draft"], {
    "start": "2026-07-20",
    "end": "2026-07-22",
})
```

The DOM/fetch/Blob/URL doubles must be listener-capable only where the real
function needs them. Capture request URLs and the created download filename;
do not replace the production functions under test.

- [ ] **Step 2: Run the isolation test and verify RED**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -k unsubmitted_custom_window -v
```

Expected: failure because at least route/API/export consumers return
`2026-07-20..2026-07-22`.

- [ ] **Step 3: Add explicit draft helpers and remove DOM defaults**

Implement:

```javascript
function draftTimeWindow() {
  return {
    start: byId("date-start")?.value || "",
    end: byId("date-end")?.value || "",
  };
}

function setDraftTimeWindow({ start = "", end = "" } = {}) {
  byId("date-start").value = start;
  byId("date-end").value = end;
}
```

Change `validateDateRange` to require explicit values:

```javascript
function validateDateRange(start = "", end = "", { required = false } = {}) {
```

`applyWindow()` is the only business command allowed to call
`draftTimeWindow()`. UI hydration/open/cancel may call `setDraftTimeWindow()`.

Keep payload metadata as the first applied source and use the parsed route only
for bootstrap before a payload exists:

```javascript
function appliedTimeWindow() {
  const metadata = app.payload?.metadata || app.defaultPayload?.metadata;
  if (metadata?.start_date && metadata?.end_date) {
    return { start: metadata.start_date, end: metadata.end_date };
  }
  if (app.route?.kind === "workspace") {
    return {
      start: app.route.state?.start || "",
      end: app.route.state?.end || "",
    };
  }
  return {
    start: app.route?.filters?.start || "",
    end: app.route?.filters?.end || "",
  };
}
```

- [ ] **Step 4: Make route builders applied-by-default with candidate override**

Use these signatures:

```javascript
function currentScreenerFilters({ window = appliedTimeWindow() } = {})
function currentWorkspaceRouteState(
  page,
  { window = appliedTimeWindow() } = {},
)
function currentWorkspacePath(
  page = app.route?.page || "markets",
  { window = appliedTimeWindow() } = {},
)
function replaceCurrentRoute({ window = appliedTimeWindow() } = {})
```

Populate `start/end` from the explicit `window`. Keep existing query omission
for the normalized default window. All ordinary sort/scope/pair/tab/back-link
calls use the default applied window; only Task 2's successful date command
passes an explicit candidate.

- [ ] **Step 5: Move API and export consumers to applied state**

At the beginning of `loadQuality()` and `loadComparison()`, read once:

```javascript
const window = appliedTimeWindow();
```

Use `window.start/end` for validation, URL query parameters, and Event Facts.
Use the same applied values for the CSV filename in `exportVisibleCsv()`.
Update the legacy test in `tests/test_dashboard.py` so it requires the applied
window rather than zero-argument DOM validation.

- [ ] **Step 6: Run focused and frontend tests**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -k 'unsubmitted_custom_window' -v
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -v
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard.py' -v
```

Expected: all selected tests pass with no warnings.

- [ ] **Step 7: Commit the state-reader boundary**

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py tests/test_dashboard.py
git commit -m "refactor(ui): isolate draft time window state"
```

### Task 2: Make summary success the single commit point

**Files:**

- Modify: `dashboard/static/app.js`
- Modify: `tests/test_dashboard_frontend.py`

**Interfaces:**

- Consumes Task 1's draft/applied helpers and explicit route-window override.
- Produces `applyWindow(candidate = draftTimeWindow()) -> Promise<boolean>`.
- A `true` result means the summary payload and route are committed; catalog outcome is not part of this boolean.

- [ ] **Step 1: Write real bound-event summary transaction tests**

Add or replace tests so real `bindEvents()` and real `applyWindow()` cover:

```text
Custom summary failure:
  payload/summary/active/URL remain on old applied dates;
  submitted draft remains visible;
  editor remains open; no focus restore.

Custom summary success:
  payload/summary/active/URL change to the response metadata exactly once;
  draft synchronizes to applied; editor closes; focus returns once.

Preset summary failure:
  preset candidate never writes the Custom draft or active state;
  URL and payload remain applied.

Preset summary success:
  exactly one summary request uses preset dates;
  route commits after success; editor closes without forced focus.

Two overlapping summary requests:
  only the latest response may commit payload, route, draft, active state,
  close, or focus.
```

Use controlled fetch promises and real event listeners. Assert literal URLs,
request counts, dates, `hidden`, `aria-expanded`, `aria-pressed`, and focus
call deltas.

- [ ] **Step 2: Run the transaction tests and verify RED**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -k 'summary_window_commit' -v
```

Expected: failure because Workspace currently writes the candidate route
before summary success and couples success to full route/catalog outcome.

- [ ] **Step 3: Remove loader ownership of the draft**

In `updateMetadata()`, always update date bounds and applied presentation, but
do not overwrite an open Custom draft. Introduce:

```javascript
function customWindowIsOpen() {
  return byId("custom-window-toggle")?.getAttribute("aria-expanded") === "true";
}

function syncClosedDraftToApplied() {
  if (!customWindowIsOpen()) setDraftTimeWindow(appliedTimeWindow());
}
```

Call the latter after metadata/applied controls render. Remove direct date
input writes from `loadMarket()` failure handling. Summary failure must retain
the candidate draft.

- [ ] **Step 4: Rewrite `applyWindow(candidate)` around `loadMarket()`**

Use this control flow:

```javascript
async function applyWindow(candidate = draftTimeWindow()) {
  const { start, end } = candidate;
  const dateError = validateDateRange(start, end, { required: true });
  if (dateError) {
    showDateWindowError(dateError);
    return false;
  }
  showDateWindowError("");
  const routeKind = app.route.kind;
  const loaded = await loadMarket(start, end, { preserve: Boolean(app.payload) });
  if (!loaded) return false;
  const applied = appliedTimeWindow();
  replaceCurrentRoute({ window: applied });
  if (routeKind === "workspace") void applyRouteFromLocation();
  return true;
}
```

Preserve latest-wins behavior in `loadMarket()`. Remove the payload/token/route
rollback code introduced by the prior Workspace fix; no route is written
before summary success, so summary failure needs no transaction rollback.

- [ ] **Step 5: Bind explicit Custom and preset candidates**

Custom submit calls `applyWindow(draftTimeWindow())`. A preset handler calls:

```javascript
const applied = await applyWindow(presetWindow(button.dataset.days));
```

It must not call `setPreset()` or write the date inputs before success. On a
successful current command, call `setDraftTimeWindow(appliedTimeWindow())`,
sync applied controls, and close as specified. Failure leaves the Custom draft
and editor unchanged.

- [ ] **Step 6: Run focused, frontend, and route tests**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -k 'summary_window_commit' -v
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -v
```

Expected: all tests pass; no stale rollback snapshot remains in
`applyWindow()`.

- [ ] **Step 7: Commit the summary commit flow**

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py
git commit -m "refactor(ui): commit time window on summary success"
```

### Task 3: Isolate catalog outcome and guard generation refresh

**Files:**

- Modify: `dashboard/static/app.js`
- Modify: `tests/test_dashboard_frontend.py`

**Interfaces:**

- Consumes the committed payload/URL established by Task 2.
- Preserves `applyRouteFromLocation() -> Promise<boolean>` for route/catalog readiness.
- Catalog failure does not change the already-applied summary transaction.

- [ ] **Step 1: Write catalog-boundary and stale-retry tests**

Add executable tests using real `applyRouteFromLocation()`:

```text
Catalog failure after summary commit:
  applied payload, summary, active state, draft, and URL stay on the new window;
  workspace catalog error/unavailable state is visible;
  no date rollback or editor reopen occurs.

Stale generation-mismatch response:
  a newer route owns app.routeRequestId before the old catch branch runs;
  old branch never calls loadMarket;
  newer payload, catalog, visible Tokens, URL, and applied controls remain.

Current generation-mismatch response:
  exactly one guarded summary refresh occurs and catalog retry uses its new
  generation key.
```

- [ ] **Step 2: Run the catalog tests and verify RED**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -k 'catalog_window_boundary' -v
```

Expected: stale generation mismatch calls `loadMarket()` before ownership is
checked, or the existing catalog-failure expectations still attempt rollback.

- [ ] **Step 3: Guard before generation-mismatch refresh**

In the `data_generation_mismatch` catch branch, place:

```javascript
if (requestId !== app.routeRequestId) return false;
```

immediately before `loadMarket(...)`. Retain the existing post-await guard.
Do not add retries beyond the current two-attempt catalog loop.

- [ ] **Step 4: Remove obsolete rollback-era tests and assertions**

Delete only tests whose required behavior contradicts the approved boundary:
catalog failure rolling back a successfully loaded summary, preset writing a
draft before request, or non-submit consumers reading DOM dates. Keep and
adapt the real event, focus, ARIA, summary latest-wins, route ownership, and
catalog stale tests.

- [ ] **Step 5: Run all verification**

Run:

```bash
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_dashboard_frontend.py' -v
env PATH="/Users/laozhendong/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:/opt/anaconda3/bin:/usr/bin:/bin:/usr/sbin:/sbin" /opt/anaconda3/bin/python -m unittest discover -s tests -v
git diff --check
git status --short
```

Expected: every test passes, whitespace check is clean, and only intended
tracked source/tests plus untracked `.superpowers/` appear.

- [ ] **Step 6: Verify Option A in a real browser**

Run:

```bash
./scripts/run_dashboard.sh
```

Check `http://127.0.0.1:8765/screener` and
`http://127.0.0.1:8765/tokens/AAVE/markets` at `1440x900` and `390x844`:

```text
Draft edit + sort/tab/pair/quality action: applied URL/data remain unchanged.
Summary failure: old applied summary/URL remain; candidate draft stays open.
Summary success: new summary/URL active once; editor closes correctly.
Catalog failure: new summary/URL remain; workspace shows catalog error.
Preset: one request, no pre-request draft/active mutation.
Keyboard: open focuses Start; Cancel and successful Custom Apply return focus.
```

- [ ] **Step 7: Commit catalog isolation**

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py
git commit -m "fix(ui): isolate catalog from window commit"
```
