import json
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
        self.assertIn("does not estimate return", index)
        self.assertIn("or causality", index)
        self.assertIn("This is not proof that no event exists", app)
        self.assertIn("availability?.status", app)
        self.assertIn("/api/markets/events?", app)
        self.assertIn('rel="noopener noreferrer"', app)
        self.assertIn('page === "events"', app)
        self.assertIn("five research pages", app)
        self.assertNotIn("four research pages", app)

    def test_mobile_header_keeps_all_navigation_and_freshness_visible(self):
        styles = STYLES_PATH.read_text(encoding="utf-8")

        mobile = styles.split("@media (max-width: 700px)", 1)[1]
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr))", mobile)
        self.assertIn(".primary-navigation a:last-child { grid-column: 1 / -1; }", mobile)
        self.assertIn("white-space: normal", mobile)
        self.assertIn("#freshness { min-width: 0; overflow-wrap: anywhere; }", mobile)

    def test_event_route_round_trips_lifecycle_filter(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is not installed in this runtime")
        script = f"""
const navigation = require({json.dumps(str(NAVIGATION_PATH))});
const parsed = navigation.parseRoute(
  "/tokens/STRK/events",
  "?lifecycle=scheduled&marketA=cex%3Aokx%3ASTRK%2FUSDT"
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
        self.assertIn("lifecycle=scheduled", result["built"])
        self.assertIn("marketA=", result["built"])

    def test_renderer_distinguishes_available_empty_and_unpublished(self):
        result = run_app_javascript(
            r"""
const ids = [
  "events-count", "events-occurred", "events-scheduled",
  "events-source-count", "events-body", "events-status", "events-error",
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
  lifecycle_counts: { occurred: 1 },
  events: [{
    revision: 1,
    token_symbol: "STRK",
    event_type: "unlock",
    event_subtype: "scheduled_release",
    event_name: "<verified unlock>",
    lifecycle: "occurred",
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
  html: elements["events-body"].innerHTML,
  status: elements["events-status"].textContent,
};
console.log(JSON.stringify({ availableState, emptyState, unavailableState }));
"""
        )

        self.assertEqual(result["availableState"]["count"], "1")
        self.assertIn("&lt;verified unlock&gt;", result["availableState"]["html"])
        self.assertNotIn("<verified unlock>", result["availableState"]["html"])
        self.assertIn("docs.example.test", result["availableState"]["html"])
        self.assertIn("latest verified Event Facts", result["availableState"]["status"])
        self.assertIn("not proof", result["emptyState"]["html"])
        self.assertIn("absence is not inferred", result["emptyState"]["status"])
        self.assertEqual(result["unavailableState"]["count"], "N/A")
        self.assertIn("different from a verified zero-event", result["unavailableState"]["html"])
        self.assertEqual(result["unavailableState"]["status"], "No publication")


if __name__ == "__main__":
    unittest.main()
