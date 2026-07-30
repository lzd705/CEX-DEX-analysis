# Compact Time Window Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the always-open date form with the approved Option A toolbar: a readable applied-range summary, immediate presets, and an inline `Custom…` editor.

**Architecture:** Keep the existing date inputs, validation, route synchronization, and market refresh pipeline. Add a small presentation layer that derives summary and active-control state from the last applied payload, while the date inputs serve as a custom draft until submission. The responsive layout remains native HTML/CSS and the behavior remains dependency-free JavaScript.

**Tech Stack:** Static HTML, CSS, browser JavaScript, Python `unittest`, Node.js JavaScript contract tests.

## Global Constraints

- Presets `7D`, `30D`, `90D`, and `All` apply immediately.
- Custom input changes are drafts and must not refresh data before submission.
- `Cancel` restores the applied dates, clears custom errors, and collapses the editor.
- A valid custom submission updates the applied summary and collapses the editor; invalid input keeps it open.
- Active styling represents the applied range, not an unsubmitted custom draft.
- Existing date bounds, validation, routes, calculations, APIs, and datasets remain unchanged.
- The toolbar must remain keyboard-operable and responsive at widths below `700px`.
- Do not stage or commit `.superpowers/` visual-companion artifacts.

---

## File map

- `dashboard/static/index.html`: semantic toolbar structure, applied summary, preset group, custom toggle, and collapsible custom form.
- `dashboard/static/styles.css`: desktop toolbar grid, custom drawer, states, focus treatment, and narrow-screen stacking.
- `dashboard/static/app.js`: applied-range formatting, preset/custom state synchronization, drawer lifecycle, and submission bindings.
- `tests/test_dashboard_frontend.py`: HTML/CSS/JavaScript contracts and executable JavaScript state tests.

### Task 1: Build the collapsed toolbar and inline custom drawer

**Files:**

- Modify: `dashboard/static/index.html:39-94`
- Modify: `dashboard/static/styles.css:150-228`
- Modify: `dashboard/static/styles.css:1865-1955`
- Test: `tests/test_dashboard_frontend.py`

**Interfaces:**

- Consumes: existing element IDs `date-window-form`, `date-start`, `date-end`, `apply-window`, `date-window-error`, and preset attribute `data-days`.
- Produces: `applied-window-summary`, `time-presets`, `custom-window-toggle`, `custom-window-editor`, and `cancel-window` elements for Task 2 and Task 3.

- [ ] **Step 1: Write the failing semantic-structure test**

Add this test to `DashboardFrontendContractTest`:

```python
def test_time_window_uses_summary_presets_and_inline_custom_editor(self):
    index = INDEX_PATH.read_text(encoding="utf-8")
    styles = STYLES_PATH.read_text(encoding="utf-8")

    self.assertIn('id="applied-window-summary"', index)
    self.assertIn('id="time-presets"', index)
    self.assertIn('id="custom-window-toggle"', index)
    self.assertIn('aria-controls="custom-window-editor"', index)
    self.assertIn('aria-expanded="false"', index)
    self.assertIn('id="custom-window-editor"', index)
    self.assertIn('id="cancel-window"', index)

    editor_start = index.index('id="custom-window-editor"')
    editor_end = index.index("</form>", editor_start)
    editor = index[editor_start:editor_end]
    self.assertIn('id="date-start"', editor)
    self.assertIn('id="date-end"', editor)
    self.assertIn('id="apply-window"', editor)
    self.assertIn("Apply custom range", editor)
    self.assertIn('id="date-window-error"', editor)

    self.assertIn(".time-toolbar-row", styles)
    self.assertIn(".custom-window-editor[hidden]", styles)
    mobile_start = styles.index("@media (max-width: 700px)")
    mobile = styles[mobile_start:]
    self.assertIn(".time-window-actions", mobile)
    self.assertIn(".custom-window-editor", mobile)
    self.assertIn(".custom-window-commands", mobile)
```

- [ ] **Step 2: Run the structure test and verify it fails**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_time_window_uses_summary_presets_and_inline_custom_editor -v
```

Expected: `FAIL` because `applied-window-summary` and the custom-editor controls do not exist.

- [ ] **Step 3: Replace the toolbar markup with the approved structure**

Keep the existing title and scope tooltip, then use this structure inside
`#time-toolbar`:

```html
<div class="time-toolbar-row">
  <div class="toolbar-title">...</div>
  <div class="applied-window" aria-live="polite">
    <span>Applied range</span>
    <strong id="applied-window-summary">Loading selected dates</strong>
  </div>
  <div class="time-window-actions">
    <div id="time-presets" class="preset-group" role="group" aria-label="Time presets">
      <button type="button" data-days="7" aria-pressed="false">7D</button>
      <button type="button" data-days="30" aria-pressed="false">30D</button>
      <button type="button" data-days="90" aria-pressed="false">90D</button>
      <button type="button" data-days="all" aria-pressed="false">All</button>
    </div>
    <button
      id="custom-window-toggle"
      class="custom-window-toggle"
      type="button"
      aria-controls="custom-window-editor"
      aria-expanded="false"
      aria-pressed="false"
    >Custom…</button>
  </div>
</div>
<form
  id="date-window-form"
  class="custom-window-editor"
  novalidate
  hidden
>
  <div class="date-fields">...</div>
  <div class="custom-window-commands">
    <button id="cancel-window" class="secondary-command" type="button">Cancel</button>
    <button id="apply-window" class="icon-command" type="submit">
      <i data-lucide="refresh-cw"></i>
      <span>Apply custom range</span>
    </button>
  </div>
  <div
    id="date-window-error"
    class="date-window-error"
    role="alert"
    aria-live="assertive"
    hidden
  ></div>
</form>
```

Preserve the exact existing date input attributes: `type="date"`, `required`,
`aria-invalid="false"`, and `aria-describedby="date-window-error"`.

- [ ] **Step 4: Add desktop and mobile layout rules**

Replace the toolbar's single-row flex assumptions with:

```css
.time-toolbar {
  position: sticky;
  top: 0;
  z-index: 20;
  display: block;
}
.time-toolbar-row {
  display: grid;
  grid-template-columns: minmax(210px, 1fr) minmax(220px, auto) auto;
  align-items: center;
  gap: 18px;
}
.applied-window span {
  display: block;
  margin-bottom: 4px;
  color: var(--muted);
  font-size: 11px;
  text-transform: uppercase;
}
.applied-window strong {
  color: var(--text);
  font-size: 15px;
}
.time-window-actions,
.custom-window-commands {
  display: flex;
  align-items: center;
  gap: 8px;
}
.custom-window-toggle,
.secondary-command {
  min-height: 44px;
  padding: 0 14px;
  border: 1px solid var(--line);
  border-radius: 4px;
  background: var(--surface-2);
  color: var(--text);
  font-weight: 750;
}
.custom-window-toggle.active {
  border-color: #477e83;
  background: var(--accent-soft);
  color: #a9e1e5;
}
.custom-window-editor {
  position: relative;
  display: flex;
  align-items: end;
  gap: 12px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--line);
}
.custom-window-editor[hidden] {
  display: none;
}
```

At `max-width: 900px`, make `.time-toolbar-row` use one title row followed by
the summary and actions. At `max-width: 700px`, use:

```css
.time-toolbar-row { display: flex; flex-direction: column; align-items: stretch; }
.time-window-actions { display: grid; grid-template-columns: 1fr; }
.preset-group { display: flex; width: 100%; }
.preset-group button { flex: 1; }
.custom-window-toggle { width: 100%; }
.custom-window-editor { display: grid; grid-template-columns: minmax(0, 1fr); }
.date-fields { display: grid; grid-template-columns: minmax(0, 1fr); }
.date-arrow { display: none; }
.custom-window-commands { display: grid; grid-template-columns: minmax(0, .8fr) minmax(0, 1.2fr); }
.custom-window-commands button { width: 100%; justify-content: center; }
```

Retain `.custom-window-editor[hidden] { display: none; }` after responsive
rules so the hidden drawer never becomes visible due to a media query.

- [ ] **Step 5: Run the structure test and existing form accessibility tests**

Run:

```bash
python -m unittest \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_time_window_uses_summary_presets_and_inline_custom_editor \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_date_apply_button_is_inside_the_date_range_form \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_date_error_is_inline_only_and_updates_input_accessibility_state \
  -v
```

Expected: all three tests report `ok`.

- [ ] **Step 6: Commit the structural layout**

```bash
git add dashboard/static/index.html dashboard/static/styles.css tests/test_dashboard_frontend.py
git commit -m "feat(ui): add compact time window layout"
```

### Task 2: Derive summary and active state from applied dates

**Files:**

- Modify: `dashboard/static/app.js:538-565`
- Modify: `dashboard/static/app.js:2818-2834`
- Test: `tests/test_dashboard_frontend.py`

**Interfaces:**

- Consumes: `app.payload.metadata.start_date`, `end_date`, `available_start`, and `available_end`.
- Produces:
  - `formatAppliedWindowSummary(start: string, end: string) -> string`
  - `appliedTimeWindow() -> {start: string, end: string}`
  - `presetWindow(days: "7" | "30" | "90" | "all") -> {start: string, end: string}`
  - `syncTimeWindowControls() -> void`

- [ ] **Step 1: Write the failing executable JavaScript test**

Add this test:

```python
def test_time_window_summary_and_active_state_use_applied_payload(self):
    result = run_app_javascript(
        """
function control(dataset = {}) {
  return {
    dataset,
    textContent: "",
    attributes: {},
    active: false,
    classList: {
      owner: null,
      toggle(name, enabled) {
        if (name === "active") this.owner.active = enabled;
      },
    },
    setAttribute(name, value) { this.attributes[name] = value; },
  };
}
const summary = control();
const custom = control();
summary.classList.owner = summary;
custom.classList.owner = custom;
const presets = ["7", "30", "90", "all"].map((days) => {
  const button = control({ days });
  button.classList.owner = button;
  return button;
});
global.document = {
  getElementById(id) {
    return {
      "applied-window-summary": summary,
      "custom-window-toggle": custom,
    }[id] || null;
  },
  querySelectorAll(selector) {
    return selector === "[data-days]" ? presets : [];
  },
};
app.payload = {
  metadata: {
    start_date: "2026-07-23",
    end_date: "2026-07-29",
    available_start: "2025-05-14",
    available_end: "2026-07-29",
  },
};
syncTimeWindowControls();
const presetState = {
  summary: summary.textContent,
  active: presets.find((button) => button.active)?.dataset.days,
  custom: custom.active,
};
app.payload.metadata.start_date = "2026-07-01";
syncTimeWindowControls();
console.log(JSON.stringify({
  presetState,
  customState: {
    activePreset: presets.find((button) => button.active)?.dataset.days || "",
    custom: custom.active,
    pressed: custom.attributes["aria-pressed"],
  },
}));
"""
    )
    self.assertEqual(result["presetState"]["summary"], "23–29 Jul 2026 · 7 days")
    self.assertEqual(result["presetState"]["active"], "7")
    self.assertFalse(result["presetState"]["custom"])
    self.assertEqual(result["customState"]["activePreset"], "")
    self.assertTrue(result["customState"]["custom"])
    self.assertEqual(result["customState"]["pressed"], "true")
```

- [ ] **Step 2: Run the executable test and verify it fails**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_time_window_summary_and_active_state_use_applied_payload -v
```

Expected: `FAIL` because `syncTimeWindowControls` is not defined.

- [ ] **Step 3: Implement applied-window helpers**

Add these functions near the existing segmented-control synchronizers:

```javascript
function appliedTimeWindow() {
  return {
    start: app.payload?.metadata?.start_date || "",
    end: app.payload?.metadata?.end_date || "",
  };
}

function presetWindow(days) {
  const availableStart = app.payload?.metadata?.available_start || "";
  const availableEnd = app.payload?.metadata?.available_end || "";
  if (!availableStart || !availableEnd) return { start: "", end: "" };
  if (days === "all") return { start: availableStart, end: availableEnd };
  const startDate = new Date(`${availableEnd}T00:00:00Z`);
  startDate.setUTCDate(startDate.getUTCDate() - Number(days) + 1);
  const candidate = startDate.toISOString().slice(0, 10);
  return {
    start: candidate < availableStart ? availableStart : candidate,
    end: availableEnd,
  };
}

function formatAppliedWindowSummary(start, end) {
  const startDate = new Date(`${start}T00:00:00Z`);
  const endDate = new Date(`${end}T00:00:00Z`);
  if (
    !start
    || !end
    || Number.isNaN(startDate.getTime())
    || Number.isNaN(endDate.getTime())
  ) return "No applied range";

  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const inclusiveDays = Math.round(
    (endDate.getTime() - startDate.getTime()) / 86_400_000,
  ) + 1;
  const sameYear = startDate.getUTCFullYear() === endDate.getUTCFullYear();
  const sameMonth = sameYear
    && startDate.getUTCMonth() === endDate.getUTCMonth();
  let range;
  if (sameMonth) {
    range = `${startDate.getUTCDate()}–${endDate.getUTCDate()} `
      + `${months[endDate.getUTCMonth()]} ${endDate.getUTCFullYear()}`;
  } else if (sameYear) {
    range = `${startDate.getUTCDate()} ${months[startDate.getUTCMonth()]}–`
      + `${endDate.getUTCDate()} ${months[endDate.getUTCMonth()]} `
      + `${endDate.getUTCFullYear()}`;
  } else {
    range = `${startDate.getUTCDate()} ${months[startDate.getUTCMonth()]} `
      + `${startDate.getUTCFullYear()}–${endDate.getUTCDate()} `
      + `${months[endDate.getUTCMonth()]} ${endDate.getUTCFullYear()}`;
  }
  return `${range} · ${inclusiveDays} ${inclusiveDays === 1 ? "day" : "days"}`;
}
```

Replace `syncTimePresetButtons()` with `syncTimeWindowControls()`. It must read
`start` and `end` from `appliedTimeWindow()`, compare them with each exact
`presetWindow(button.dataset.days)` result, update `[data-days]`, then set:

```javascript
byId("applied-window-summary").textContent = formatAppliedWindowSummary(start, end);
const customActive = Boolean(start && end && !activePreset);
byId("custom-window-toggle").classList.toggle("active", customActive);
byId("custom-window-toggle").setAttribute("aria-pressed", String(customActive));
```

Update every existing `syncTimePresetButtons()` call to
`syncTimeWindowControls()`. Update `setPreset(days)` to assign the two values
returned by `presetWindow(days)` so preset calculation has one source of
truth:

```javascript
function setPreset(days) {
  if (!app.payload) return;
  const { start, end } = presetWindow(days);
  byId("date-start").value = start;
  byId("date-end").value = end;
}
```

- [ ] **Step 4: Run the summary test**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_time_window_summary_and_active_state_use_applied_payload -v
```

Expected: the test reports `ok`.

- [ ] **Step 5: Run existing route and preset regression tests**

Run:

```bash
python -m unittest \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_compare_window_preset_resolves_to_explicit_utc_dates \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_screener_drill_down_preserves_the_rendered_summary_window \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_route_and_loading_contract_prevents_stale_window_or_permanent_loading \
  -v
```

Expected: all three tests report `ok`.

- [ ] **Step 6: Commit applied-state rendering**

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py
git commit -m "feat(ui): render applied time window state"
```

### Task 3: Implement custom draft, cancel, apply, and preset behavior

**Files:**

- Modify: `dashboard/static/app.js:1232-1250`
- Modify: `dashboard/static/app.js:4963-4995`
- Modify: `dashboard/static/app.js:5170-5200`
- Test: `tests/test_dashboard_frontend.py`

**Interfaces:**

- Consumes: `appliedTimeWindow()` and `syncTimeWindowControls()` from Task 2; existing `setPreset(days)`, `validateDateRange()`, `showDateWindowError()`, `loadMarket()`, `replaceCurrentRoute()`, and `applyRouteFromLocation()`.
- Produces:
  - `setCustomWindowOpen(open: boolean, {restoreFocus?: boolean}) -> void`
  - `openCustomWindowEditor() -> void`
  - `cancelCustomWindowEditor() -> void`
  - `applyWindow() -> Promise<boolean>`

- [ ] **Step 1: Write the failing custom-editor lifecycle test**

Add this static contract test:

```python
def test_custom_time_window_lifecycle_preserves_applied_state(self):
    app_js = APP_PATH.read_text(encoding="utf-8")
    self.assertIn("function setCustomWindowOpen(", app_js)
    self.assertIn("function openCustomWindowEditor()", app_js)
    self.assertIn("function cancelCustomWindowEditor()", app_js)

    opener = app_js[
        app_js.index("function openCustomWindowEditor()"):
        app_js.index("function cancelCustomWindowEditor()")
    ]
    self.assertIn("appliedTimeWindow()", opener)
    self.assertIn('byId("date-start").value = start;', opener)
    self.assertIn('byId("date-end").value = end;', opener)
    self.assertIn("setCustomWindowOpen(true)", opener)

    cancel = app_js[
        app_js.index("function cancelCustomWindowEditor()"):
        app_js.index("function setPreset(")
    ]
    self.assertIn("appliedTimeWindow()", cancel)
    self.assertIn('showDateWindowError("");', cancel)
    self.assertIn("setCustomWindowOpen(false", cancel)

    apply_window = app_js[
        app_js.index("async function applyWindow()"):
        app_js.index("function persistSelectedPair()")
    ]
    self.assertIn("return false;", apply_window)
    self.assertIn("return true;", apply_window)
```

- [ ] **Step 2: Run the lifecycle test and verify it fails**

Run:

```bash
python -m unittest tests.test_dashboard_frontend.DashboardFrontendContractTest.test_custom_time_window_lifecycle_preserves_applied_state -v
```

Expected: `FAIL` because the custom-editor lifecycle functions do not exist.

- [ ] **Step 3: Implement drawer lifecycle**

Add these functions immediately before `setPreset()`:

```javascript
function setCustomWindowOpen(open, { restoreFocus = false } = {}) {
  const editor = byId("custom-window-editor");
  const toggle = byId("custom-window-toggle");
  editor.hidden = !open;
  toggle.setAttribute("aria-expanded", String(open));
  if (open) byId("date-start").focus();
  else if (restoreFocus) toggle.focus();
}

function openCustomWindowEditor() {
  const { start, end } = appliedTimeWindow();
  byId("date-start").value = start;
  byId("date-end").value = end;
  showDateWindowError("");
  setCustomWindowOpen(true);
}

function cancelCustomWindowEditor() {
  const { start, end } = appliedTimeWindow();
  byId("date-start").value = start;
  byId("date-end").value = end;
  showDateWindowError("");
  setCustomWindowOpen(false, { restoreFocus: true });
}
```

The `Custom…` command should toggle the editor. If it is currently open, use
`cancelCustomWindowEditor()` so closing always discards the draft.

- [ ] **Step 4: Make apply report success without changing validation rules**

Change `applyWindow()` so the validation branch returns `false`, the workspace
branch returns `true` after `await applyRouteFromLocation()`, and the screener
branch returns the boolean from `loadMarket()`:

```javascript
async function applyWindow() {
  const start = byId("date-start").value;
  const end = byId("date-end").value;
  const dateError = validateDateRange(start, end, { required: true });
  if (dateError) {
    showDateWindowError(dateError);
    return false;
  }
  showDateWindowError("");
  if (app.route.kind === "workspace") {
    replaceCurrentRoute();
    await applyRouteFromLocation();
    return true;
  }
  const loaded = await loadMarket(start, end, { preserve: Boolean(app.payload) });
  if (!loaded) return false;
  replaceCurrentRoute();
  return true;
}
```

Do not change `validateDateRange()` or the routes constructed by
`replaceCurrentRoute()`.

- [ ] **Step 5: Bind custom commands and make preset state applied-only**

Replace the custom form submission binding with:

```javascript
byId("date-window-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const applied = await applyWindow();
  if (applied) {
    syncTimeWindowControls();
    setCustomWindowOpen(false, { restoreFocus: true });
  }
});
byId("custom-window-toggle").addEventListener("click", () => {
  if (byId("custom-window-toggle").getAttribute("aria-expanded") === "true") {
    cancelCustomWindowEditor();
  } else {
    openCustomWindowEditor();
  }
});
byId("cancel-window").addEventListener("click", cancelCustomWindowEditor);
```

Replace each preset click callback with:

```javascript
button.addEventListener("click", async () => {
  setPreset(button.dataset.days);
  const applied = await applyWindow();
  if (applied) {
    syncTimeWindowControls();
    setCustomWindowOpen(false);
  }
});
```

Remove the old pre-request loop that marks the clicked preset active. This
ensures active styling only changes after the applied payload changes.

Extend `setDateWindowDisabled(disabled)` so it disables the custom form,
preset buttons, and custom toggle during the existing refresh:

```javascript
document.querySelectorAll(
  "#date-window-form input, #date-window-form button, "
  + "#time-presets button, #custom-window-toggle",
).forEach((control) => {
  control.disabled = disabled;
});
```

- [ ] **Step 6: Run focused behavior tests**

Run:

```bash
python -m unittest \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_custom_time_window_lifecycle_preserves_applied_state \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_date_error_is_inline_only_and_updates_input_accessibility_state \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_workspace_window_change_reloads_matching_summary_and_catalog_in_order \
  -v
```

Expected: all three tests report `ok`.

- [ ] **Step 7: Run the full automated suite**

Run:

```bash
python -m unittest discover -s tests -v
```

Expected: exit code `0` with every test passing.

- [ ] **Step 8: Inspect the final diff**

Run:

```bash
git diff --check
git status --short
git diff -- dashboard/static/index.html dashboard/static/styles.css dashboard/static/app.js tests/test_dashboard_frontend.py
```

Expected: no whitespace errors; only the four intended implementation files
and the already-untracked `.superpowers/` directory appear.

- [ ] **Step 9: Verify the UI in a real browser**

Start the existing local application:

```bash
./scripts/run_dashboard.sh
```

Open `http://127.0.0.1:8765`, then inspect:

```text
Desktop: 1440 × 900
Mobile: 390 × 844
Routes: /screener and /token/AAVE/markets
```

At both widths, verify:

```text
Collapsed: applied summary visible; four presets and Custom visible.
Preset: one click reloads and moves active styling only after success.
Custom open: inputs contain applied dates and focus starts at Start.
Draft: changing a date does not reload or change active styling.
Cancel: applied dates return, errors clear, drawer closes, focus returns.
Invalid Apply: inline error is visible and drawer stays open.
Valid Apply: summary and route update, drawer closes, focus returns.
```

- [ ] **Step 10: Commit the completed interaction**

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py
git commit -m "feat(ui): add custom time window interaction"
```
