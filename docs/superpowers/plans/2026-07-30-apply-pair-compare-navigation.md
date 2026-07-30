# Apply Pair Compare Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a valid `Apply pair` click persist Market A/B and navigate to the current Token's Compare page.

**Architecture:** Keep pair validation and persistence in the existing `persistSelectedPair()` function. Add one UI command that chooses between the existing invalid-pair refresh path and SPA navigation to the Compare route, allowing the existing route builder to preserve Token, Market A/B, and dates.

**Tech Stack:** Vanilla JavaScript SPA, Python `unittest` frontend contract tests.

## Global Constraints

- Do not change data collection, APIs, market-selection defaults, or production deployment.
- Use SPA history navigation without a full page reload.
- Preserve the current invalid/incomplete-pair validation behavior.

---

### Task 1: Apply Pair Compare Navigation

**Files:**
- Modify: `dashboard/static/app.js`
- Test: `tests/test_dashboard_frontend.py`

**Interfaces:**
- Consumes: `persistSelectedPair() -> boolean`, `replaceCurrentRoute()`, `refreshWorkspacePageData()`, `currentWorkspacePath(page) -> string`, and `navigateTo(path)`.
- Produces: `applySelectedPair() -> boolean`, called by the `#compare-markets` click handler.

- [ ] **Step 1: Write the failing regression test**

Add a test that extracts `applySelectedPair()` from `dashboard/static/app.js`
and asserts that invalid persistence refreshes in place while valid persistence
navigates with:

```js
navigateTo(currentWorkspacePath("compare"));
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_apply_pair_navigates_to_compare_after_persisting_valid_selection
```

Expected: failure because `function applySelectedPair()` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Add:

```js
function applySelectedPair() {
  if (!persistSelectedPair()) {
    replaceCurrentRoute();
    refreshWorkspacePageData();
    return false;
  }
  navigateTo(currentWorkspacePath("compare"));
  return true;
}
```

Change the `#compare-markets` click handler to:

```js
byId("compare-markets").addEventListener("click", applySelectedPair);
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_apply_pair_navigates_to_compare_after_persisting_valid_selection
```

Expected: one passing test.

- [ ] **Step 5: Run full verification**

Run:

```bash
python -m unittest discover -s tests
```

Expected: all tests pass.

- [ ] **Step 6: Commit and push**

Stage the design, plan, test, and implementation; commit with:

```bash
git commit -m "fix(ui): open compare when applying pair"
```

Push the tracked branch:

```bash
git push origin codex/critical-quality-sorting-token-refresh
```
