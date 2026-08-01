# Event Clock View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an independently derived Past/Future/Current clock view to Event Facts without changing evidence lifecycle.

**Architecture:** `dashboard/event_facts.py` derives clock state from the validated effective time and one explicit UTC response clock. The server filters and caches that projection, while navigation and the Events page expose an independent Time control beside lifecycle. No curated event row or lifecycle is rewritten.

**Tech Stack:** Python 3.8-compatible standard library, SQLite event bundle, vanilla JavaScript, HTML/CSS, `unittest`, Node-based frontend contract tests.

## Global Constraints

- Funding Rate remains excluded.
- `scheduled + past` stays `scheduled` and displays occurrence unconfirmed.
- Month/day precision uses the existing effective interval; minute/second uses the exact UTC instant.
- Missing publication and available-zero-results remain different states.
- All N/A values retain an adjacent reason disclosure.
- Upbit data is not modified.

---

### Task 1: Pure Event clock-state contract

**Files:**
- Modify: `dashboard/event_facts.py`
- Modify: `scripts/event_facts.py`
- Test: `tests/test_event_api.py`
- Test: `tests/test_event_facts.py`

**Interfaces:**
- Produces: `effective_datetime_interval()` with explicit UTC `[start, end)`
  semantics, plus `EVENT_API_SCHEMA = "event_facts_api/v2"`,
  `EVENT_CLOCK_STATES`, and
  `event_clock_projection(effective_at: str, precision: str, as_of: datetime) -> dict[str, str]`.
- Produces: public event field `clock = {state, as_of_utc, basis}`.

- [ ] **Step 1: Write failing precision-boundary tests**

```python
def test_clock_projection_respects_exact_and_interval_precision(self):
    as_of = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
    self.assertEqual(
        event_clock_projection("2026-08-01T12:01Z", "minute", as_of)["state"],
        "future",
    )
    self.assertEqual(
        event_clock_projection("2026-08-01", "day", as_of)["state"],
        "current_window",
    )
    self.assertEqual(
        event_clock_projection("2026-07", "month", as_of)["state"],
        "past",
    )
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 -m unittest tests.test_event_api.EventApiIntegrationTest.test_clock_projection_respects_exact_and_interval_precision -v`

Expected: FAIL because `event_clock_projection` is not defined.

- [ ] **Step 3: Implement exact UTC and interval comparison**

Implement `effective_datetime_interval()` and `event_clock_projection()` with
timezone-aware `datetime` and stable bases:

```python
{
    "state": "past" | "future" | "current_window",
    "as_of_utc": canonical_utc_text,
    "basis": "exact_instant" | "effective_date_interval",
}
```

Reject naive clocks and unknown precision; do not infer lifecycle.
Month/day values use calendar UTC `[start, end)` intervals. Minute/second
values compare the exact instant and do not collapse to a date. Keep
`effective_date_bounds()` unchanged for date filtering only.
Keep the stored Event Fact schema at v1; only the additive public API contract
advances to `event_facts_api/v2`.

- [ ] **Step 4: Add payload-level clock counts and cross-filter tests**

Call `build_event_payload(..., clock_state="future", clock_as_of=as_of)` and
assert that `clock_state_counts` describes the filtered rows, every returned
event is future, and an elapsed `scheduled` row remains scheduled.

- [ ] **Step 5: Run Event API tests**

Run: `python3 -m unittest tests.test_event_api -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/event_facts.py scripts/event_facts.py tests/test_event_api.py tests/test_event_facts.py
git commit -m "feat(events): derive evidence-independent clock state"
```

Add a GitHub commit comment explaining that lifecycle was not mutated.

### Task 2: Public API query and response-clock ownership

**Files:**
- Modify: `dashboard/server.py`
- Test: `tests/test_event_api.py`
- Test: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: `build_event_payload(..., clock_state, clock_as_of)` from Task 1.
- Produces: `GET /api/markets/events?...&clock_state=past|future|current_window`.

- [ ] **Step 1: Write failing query-contract tests**

Assert that `public_api_query_items("events", ...)` retains a normalized
`clock_state` and rejects `predicted`. Build a second-precision event at
`12:00:30Z`, request at `12:00:29Z` and `12:00:31Z` within the same minute,
and prove the older cached Future projection cannot survive the transition.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_event_api -v`

Expected: FAIL on missing `clock_state` support.

- [ ] **Step 3: Extend validation and builder**

Add `clock_state` to the events route allowlist and pass one server-owned
`datetime.now(timezone.utc)` value through the complete builder. The response
must expose the same `clock_as_of_utc` in metadata and every event projection.
Cache ownership uses the next known event transition (or bypasses caching when
none can be proved); minute buckets are forbidden for exact events.

- [ ] **Step 4: Test missing publication and empty intersection**

Verify that an unavailable bundle still validates the enum, and that an
available bundle with no `future + occurred` rows returns availability
`available`, `event_count=0`, and empty counts.

- [ ] **Step 5: Run focused server tests**

Run: `python3 -m unittest tests.test_event_api tests.test_dashboard -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/server.py tests/test_event_api.py tests/test_dashboard.py
git commit -m "feat(api): expose Event Past and Future filters"
```

Add a GitHub commit comment with the query and cache semantics.

### Task 3: Navigation and Events controls

**Files:**
- Modify: `dashboard/static/navigation.js`
- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/styles.css`
- Test: `tests/test_navigation.py`
- Test: `tests/test_event_frontend.py`

**Interfaces:**
- Consumes: Event API `clock_state` from Task 2.
- Produces: route state `eventClockState` and independent Time segmented control.

- [ ] **Step 1: Write failing URL round-trip test**

```javascript
const parsed = navigation.parseRoute(
  "/tokens/STRK/events",
  "?clock_state=future&lifecycle=scheduled"
);
assert.equal(parsed.state.clockState, "future");
assert.match(
  navigation.buildWorkspacePath("STRK", "events", parsed.state),
  /clock_state=future/
);
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_navigation tests.test_event_frontend -v`

Expected: FAIL because the clock filter is absent.

- [ ] **Step 3: Add the independent Time control**

Add `All`, `Future`, `Past`, and `Current` buttons using
`data-event-clock-state`. Keep the existing lifecycle control and label it
`Evidence status`. Route hydration must restore both controls.

- [ ] **Step 4: Render clock badges and elapsed scheduled copy**

For every event, render a clock badge. When `clock.state === "past"` and
`lifecycle === "scheduled"`, render exactly:

```text
Effective time passed; occurrence unconfirmed
```

Do not change the lifecycle badge.

- [ ] **Step 5: Test request ownership, empty state, and mobile wrapping**

The Event request ID/controller must own Token, lifecycle, and clock state.
A late prior response cannot replace a newer filter. At `max-width: 700px`,
both segmented groups wrap without clipping or horizontal page overflow.

- [ ] **Step 6: Run frontend tests**

Run: `python3 -m unittest tests.test_navigation tests.test_event_frontend -v`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add dashboard/static/navigation.js dashboard/static/index.html dashboard/static/app.js dashboard/static/styles.css tests/test_navigation.py tests/test_event_frontend.py
git commit -m "feat(ui): add Event Past and Future views"
```

Add a GitHub commit comment noting that time and evidence are independent.

### Task 4: Release contract and browser verification

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `docs/event-facts-contract.md`
- Modify: `docs/market-monitor-design.md`
- Test: `tests/test_release_smoke.py`

**Interfaces:**
- Consumes: Event API and UI from Tasks 1–3.
- Produces: release rejection for inconsistent clock counts, states, or query filters.

- [ ] **Step 1: Write failing release counterexamples**

Mutate one event clock state, one count, and one `scheduled + past` lifecycle;
assert that all three counterexamples fail the release checker.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke.DashboardReleaseSmokeTest -v`

Expected: FAIL because clock state is not validated.

- [ ] **Step 3: Implement release checks and contract documentation**

Require allowed clock states, exact counts, one shared response clock, and
cross-filter consistency. Document that clock is derived and lifecycle remains
evidence-based.

- [ ] **Step 4: Run Event and release suites**

Run: `python3 -m unittest tests.test_event_api tests.test_event_frontend tests.test_navigation tests.test_release_smoke -v`

Expected: PASS.

- [ ] **Step 5: Perform desktop/mobile browser acceptance**

Verify All/Future/Past/Current, lifecycle intersections, URL refresh, an
elapsed scheduled fact, available-empty state, and mobile wrapping.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_dashboard_release.py docs/event-facts-contract.md docs/market-monitor-design.md tests/test_release_smoke.py
git commit -m "test(events): enforce clock-view release contract"
```

Add a GitHub commit comment containing the focused test and browser evidence.
