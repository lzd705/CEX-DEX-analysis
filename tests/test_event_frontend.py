import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
NAVIGATION_PATH = PROJECT_ROOT / "dashboard" / "static" / "navigation.js"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"


def run_app_javascript(source):
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("Node.js is not installed in this runtime")
    completed = subprocess.run(
        [node, "-e", APP_PATH.read_text(encoding="utf-8") + "\n" + source],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


class EventFrontendTest(unittest.TestCase):
    def test_events_are_a_token_workspace_page_with_explicit_fact_boundary(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        app = APP_PATH.read_text(encoding="utf-8")

        self.assertIn('data-workspace-page="events"', index)
        self.assertIn('data-workspace-view="events"', index)
        self.assertIn('id="events-body"', index)
        self.assertIn('data-event-lifecycle="occurred"', index)
        self.assertIn('data-event-lifecycle="scheduled"', index)
        self.assertIn('data-event-clock-state="future"', index)
        self.assertIn('data-event-clock-state="past"', index)
        self.assertIn('data-event-clock-state="current_window"', index)
        self.assertIn("does not estimate return", index)
        self.assertIn("or causality", index)
        self.assertIn("This is not proof that no event exists", app)
        self.assertIn("availability?.status", app)
        self.assertIn("/api/markets/events?", app)
        self.assertIn('rel="noopener noreferrer"', app)
        self.assertIn('page === "events"', app)
        self.assertIn("four research pages", app)
        self.assertNotIn("five research pages", app)

    def test_mobile_header_keeps_all_navigation_and_freshness_visible(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        mobile = styles.split("@media (max-width: 700px)", 1)[1]
        navigation_rule = re.search(r"\.primary-navigation\s*\{([^}]*)\}", mobile)
        navigation_link_rule = re.search(r"\.primary-navigation a\s*\{([^}]*)\}", mobile)
        freshness_rule = re.search(r"\.status-cluster\s*\{([^}]*)\}", mobile)

        self.assertIsNotNone(navigation_rule)
        self.assertIsNotNone(navigation_link_rule)
        self.assertIsNotNone(freshness_rule)
        self.assertIn("display: grid", navigation_rule.group(1))
        self.assertIn(
            "grid-template-columns: repeat(2, minmax(0, 1fr))",
            navigation_rule.group(1),
        )
        self.assertIn("overflow: visible", navigation_rule.group(1))
        self.assertNotIn("overflow-x: auto", navigation_rule.group(1))
        self.assertIn("min-width: 0", navigation_link_rule.group(1))
        self.assertEqual(index.count('data-app-route="screener"'), 1)
        self.assertEqual(index.count('data-app-route="markets"'), 1)
        self.assertEqual(index.count('data-app-route="research"'), 1)
        self.assertNotIn('data-app-route="methodology"', index)
        self.assertIn("min-width: 0", freshness_rule.group(1))
        self.assertIn("overflow: visible", freshness_rule.group(1))
        self.assertIn("white-space: normal", freshness_rule.group(1))
        self.assertIn("#freshness { min-width: 0; overflow-wrap: anywhere; }", mobile)
        self.assertIn("/styles.css?v=__ASSET_VERSION__", index)
        self.assertIn("/app.js?v=__ASSET_VERSION__", index)

    def test_event_route_round_trips_lifecycle_filter(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is not installed in this runtime")
        script = f"""
const navigation = require({json.dumps(str(NAVIGATION_PATH))});
const parsed = navigation.parseRoute(
  "/tokens/STRK/events",
  "?lifecycle=scheduled&clock_state=future&marketA=cex%3Aokx%3ASTRK%2FUSDT"
);
const built = navigation.buildWorkspacePath("STRK", "events", parsed.state);
console.log(JSON.stringify({{ parsed, built }}));
        """
        completed = subprocess.run(
            [node, "-e", script],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(completed.stdout)

        self.assertEqual(result["parsed"]["page"], "events")
        self.assertEqual(result["parsed"]["state"]["lifecycle"], "scheduled")
        self.assertEqual(result["parsed"]["state"]["clockState"], "future")
        self.assertIn("lifecycle=scheduled", result["built"])
        self.assertIn("clock_state=future", result["built"])
        self.assertIn("marketA=", result["built"])

    def test_renderer_distinguishes_available_empty_and_unpublished(self):
        result = run_app_javascript(
            r"""
const ids = [
  "events-count", "events-occurred", "events-scheduled",
  "events-source-count", "events-token-coverage",
  "events-body", "events-status", "events-error",
];
const elements = Object.fromEntries(ids.map((id) => [id, {
  textContent: "",
  innerHTML: "",
  hidden: false,
  dataset: {},
}]));
elements["facts-token"] = { value: "STRK" };
global.document = {
  getElementById(id) { return elements[id] || null; },
};
const available = {
  availability: { status: "available", reason: null },
  bundle_id: "abc123",
  query: { token: "STRK", lifecycle: null },
  event_count: 1,
  lifecycle_counts: { scheduled: 1 },
  clock_state_counts: { past: 1 },
  coverage: {
    configured_token_count: 30,
    covered_token_count: 30,
    uncovered_tokens: [],
    query_token_has_published_fact: true,
  },
  events: [{
    revision: 1,
    token_symbol: "STRK",
    event_type: "unlock",
    event_subtype: "scheduled_release",
    event_name: "<verified unlock>",
    lifecycle: "scheduled",
    clock: {
      state: "past",
      as_of_utc: "2026-08-01T00:00:00Z",
      basis: "effective_date_interval",
    },
    evidence_status: "primary_confirmed",
    notes: null,
    time: {
      announced_at: null,
      effective_at: "2026-07-15",
      effective_at_precision: "day",
    },
    size: {
      amount_token: "127000000",
      percent_of_supply: "1.27",
      relation: "up_to",
    },
    market: {},
    source: {
      url: "https://docs.example.test/event",
      kind: "official_project",
      checked_at_utc: "2026-07-29T00:00:00Z",
    },
    revision_lineage: { reason: "initial" },
  }],
};
renderEventFacts(available);
const availableState = {
  count: elements["events-count"].textContent,
  tokenCoverage: elements["events-token-coverage"].textContent,
  html: elements["events-body"].innerHTML,
  status: elements["events-status"].textContent,
};
renderEventFacts({
  ...available,
  event_count: 0,
  lifecycle_counts: {},
  events: [],
});
const emptyState = {
  html: elements["events-body"].innerHTML,
  status: elements["events-status"].textContent,
};
renderEventFacts({
  availability: { status: "unavailable", reason: "No publication" },
  query: { token: "STRK" },
  events: [],
});
const unavailableState = {
  count: elements["events-count"].textContent,
  countHtml: elements["events-count"].innerHTML,
  html: elements["events-body"].innerHTML,
  status: elements["events-status"].textContent,
};
console.log(JSON.stringify({ availableState, emptyState, unavailableState }));
"""
        )

        self.assertEqual(result["availableState"]["count"], "1")
        self.assertEqual(result["availableState"]["tokenCoverage"], "30 / 30")
        self.assertIn("&lt;verified unlock&gt;", result["availableState"]["html"])
        self.assertNotIn("<verified unlock>", result["availableState"]["html"])
        self.assertIn("docs.example.test", result["availableState"]["html"])
        self.assertIn("Past", result["availableState"]["html"])
        self.assertIn(
            "Effective time passed; occurrence unconfirmed",
            result["availableState"]["html"],
        )
        self.assertIn("latest verified Event Facts", result["availableState"]["status"])
        self.assertIn("not proof", result["emptyState"]["html"])
        self.assertIn("absence is not inferred", result["emptyState"]["status"])
        self.assertEqual(result["unavailableState"]["count"], "")
        self.assertIn('class="na-disclosure"', result["unavailableState"]["countHtml"])
        self.assertIn("No publication", result["unavailableState"]["countHtml"])
        self.assertIn("different from a verified zero-event", result["unavailableState"]["html"])
        self.assertEqual(result["unavailableState"]["status"], "No publication")

    def test_event_request_sends_and_validates_independent_clock_filter(self):
        result = run_app_javascript(
            r"""
(async () => {
  const urls = [];
  let responseClock = "future";
  global.fetch = async (url) => {
    urls.push(url);
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          schema: "event_facts_api/v2",
          clock_as_of_utc: "2026-08-01T00:00:00Z",
          query: {
            token: "STRK",
            lifecycle: "scheduled",
            clock_state: responseClock,
          },
          events: [],
        };
      },
    };
  };
  const accepted = await fetchEventFacts({
    token: "STRK",
    lifecycle: "scheduled",
    clockState: "future",
  });
  responseClock = "past";
  let mismatch = "";
  try {
    await fetchEventFacts({
      token: "STRK",
      lifecycle: "scheduled",
      clockState: "future",
    });
  } catch (error) {
    mismatch = error.message;
  }
  console.log(JSON.stringify({
    urls,
    acceptedClock: accepted.query.clock_state,
    mismatch,
  }));
})();
"""
        )

        self.assertEqual(result["acceptedClock"], "future")
        self.assertIn("lifecycle=scheduled", result["urls"][0])
        self.assertIn("clock_state=future", result["urls"][0])
        self.assertIn("clock scope", result["mismatch"])

    def test_event_request_rejects_missing_scope_and_cross_scope_rows(self):
        result = run_app_javascript(
            r"""
(async () => {
  const cases = [
    {
      schema: "event_facts_api/v2",
      clock_as_of_utc: "2026-08-01T00:00:00Z",
      query: { lifecycle: "scheduled", clock_state: "future" },
      events: [],
    },
    {
      schema: "event_facts_api/v2",
      clock_as_of_utc: "2026-08-01T00:00:00Z",
      query: {
        token: "STRK",
        lifecycle: "scheduled",
        clock_state: "future",
      },
      events: [{
        token_symbol: "AAVE",
        lifecycle: "occurred",
        clock: {
          state: "past",
          as_of_utc: "2026-08-01T00:00:00Z",
        },
      }],
    },
  ];
  const errors = [];
  for (const payload of cases) {
    global.fetch = async () => ({
      ok: true,
      status: 200,
      async json() { return payload; },
    });
    try {
      await fetchEventFacts({
        token: "STRK",
        lifecycle: "scheduled",
        clockState: "future",
      });
      errors.push("accepted");
    } catch (error) {
      errors.push(error.message);
    }
  }
  console.log(JSON.stringify({ errors }));
})();
"""
        )

        self.assertIn("Token-scope", result["errors"][0])
        self.assertIn("row scope", result["errors"][1])


if __name__ == "__main__":
    unittest.main()
