"""Behavior tests for the local-only, user-triggered market refresh control."""

import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/local_opportunity_refresh.js"

DOM = r"""
const elements = new Map();
class Element {
  constructor(tag) {
    this.tagName = tag; this.children = []; this.listeners = {};
    this.attributes = {}; this.textContent = ''; this.disabled = false;
  }
  set id(value) { this._id = value; elements.set(value, this); }
  get id() { return this._id; }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  appendChild(child) { this.children.push(child); return child; }
  addEventListener(event, handler) { this.listeners[event] = handler; }
  click() { return this.listeners.click?.({preventDefault() {}}); }
}
const context = new Element('div'); context.id = 'opportunity-current-context';
globalThis.document = {
  getElementById: id => elements.get(id) || null,
  createElement: tag => new Element(tag),
};
const requests = [];
const pending = [];
const timers = new Map();
let nextTimer = 0;
globalThis.setTimeout = (callback, delay) => {
  const id = ++nextTimer; timers.set(id, {callback, delay}); return id;
};
globalThis.clearTimeout = id => timers.delete(id);
globalThis.fetch = (url, options = {}) => {
  requests.push({url, ...options});
  return new Promise((resolve, reject) => pending.push({resolve, reject}));
};
const originalFilters = {token: 'UNI', notional: '1000', opportunityScope: 'current'};
const newReceipt = {
  status:'published', route_cohort_id:'cohort:' + 'b'.repeat(64),
  manifest_sha256:'c'.repeat(64), token_pairs:['UNI/USDT', 'CAKE/USDT'],
  venues:['binance', 'bybit'], market_count:4, route_count:4,
  opportunity_count:20, strict_eligible_count:0,
};
const app = {route: {kind: 'opportunities', filters: originalFilters}};
const reloadArguments = [];
let visibleTable = 'original published snapshot';
function opportunityScope(filters = {}) {
  return filters.opportunityScope === 'historical' ? 'historical' : 'current';
}
async function loadOpportunities(...args) {
  reloadArguments.push(args); visibleTable = 'new published snapshot'; return true;
}
async function flush() { for (let i = 0; i < 12; i++) await Promise.resolve(); }
async function respond(payload, status = 200) {
  if (!pending.length) return;
  pending.shift().resolve({ok: status >= 200 && status < 300, status,
    json: async () => payload});
  await flush();
}
async function fireTimer() {
  const entry = timers.entries().next().value;
  if (!entry) return;
  const [id, timer] = entry; timers.delete(id); timer.callback(); await flush();
}
function button() { return elements.get('local-opportunity-refresh'); }
function message() { return elements.get('local-opportunity-refresh-status'); }
function state() {
  return {mounted: Boolean(button()), disabled: button()?.disabled,
    message: message()?.textContent, live: message()?.attributes['aria-live'],
    requests, table: visibleTable, reloadArguments,
    filtersPreserved: app.route.filters === originalFilters,
    timerDelays: [...timers.values()].map(timer => timer.delay)};
}
"""


class LocalOpportunityRefreshFrontendTests(unittest.TestCase):
    def run_behavior(self, action):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is unavailable")
        # An absent control is the behavior before this feature is implemented.
        production = SCRIPT.read_text() if SCRIPT.exists() else ""
        program = DOM + "\n" + production + "\n(async () => {\n" + action
        program += "\nconsole.log(JSON.stringify(state()));\n})().catch(error => {console.error(error); process.exitCode = 1;});"
        result = subprocess.run(
            [node, "-e", program], cwd=ROOT, text=True,
            capture_output=True, check=True,
        )
        return json.loads(result.stdout)

    def test_initial_control_checks_status_without_collecting(self):
        result = self.run_behavior("await respond({state:'idle', retry_after_seconds:0});")
        self.assertTrue(result["mounted"])
        self.assertFalse(result["disabled"])
        self.assertEqual(result["live"], "polite")
        self.assertEqual(len(result["requests"]), 1)
        self.assertEqual(result["requests"][0]["url"], "/api/local/opportunity-refresh")
        self.assertEqual(result["requests"][0].get("method", "GET"), "GET")
        self.assertEqual(result["requests"][0]["credentials"], "omit")
        self.assertEqual(result["table"], "original published snapshot")
        self.assertEqual(result["timerDelays"], [])

    def test_success_collects_once_and_reloads_the_current_filters(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0});
button()?.click(); button()?.click(); await flush();
await respond({state:'succeeded', retry_after_seconds:30,
  receipt:newReceipt});
""")
        self.assertEqual(result["table"], "new published snapshot")
        self.assertEqual(result["reloadArguments"], [[]])
        self.assertTrue(result["filtersPreserved"])
        posts = [row for row in result["requests"] if row.get("method") == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["headers"]["X-Opportunity-Refresh"], "1")
        self.assertEqual(posts[0]["credentials"], "omit")
        self.assertNotIn("body", posts[0])
        self.assertTrue(result["disabled"])
        self.assertEqual(result["timerDelays"], [30000])

    def test_failed_collection_keeps_existing_results_visible(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
await respond({state:'failed', error:'refresh_failed', retry_after_seconds:30}, 502);
""")
        self.assertTrue(result["mounted"])
        self.assertEqual(result["table"], "original published snapshot")
        self.assertEqual(result["reloadArguments"], [])
        self.assertIn("failed", result["message"].lower())
        self.assertTrue(result["disabled"])

    def test_uncertain_response_never_claims_failure_or_success(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
pending.shift()?.reject(new Error('Connection closed')); await flush();
""")
        self.assertTrue(result["mounted"])
        self.assertEqual(result["table"], "original published snapshot")
        self.assertIn("could not be confirmed", result["message"])
        self.assertEqual(result["reloadArguments"], [])

    def test_switching_to_historical_while_collecting_does_not_reload_it(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
app.route.filters = {opportunityScope:'historical'};
await respond({state:'succeeded', retry_after_seconds:30,
  receipt:newReceipt});
""")
        self.assertTrue(result["mounted"])
        self.assertEqual(result["reloadArguments"], [])
        self.assertEqual(result["table"], "original published snapshot")

    def test_cooldown_releases_button_without_automatic_collection(self):
        result = self.run_behavior("""
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
button()?.click(); await fireTimer();
await respond({state:'succeeded', retry_after_seconds:0, receipt:newReceipt});
""")
        self.assertTrue(result["mounted"])
        self.assertFalse(result["disabled"])
        self.assertTrue(all(row.get('method', 'GET') == 'GET' for row in result["requests"]))
        self.assertEqual(result["reloadArguments"], [])

    def test_another_tab_running_remains_locked_until_status_completes(self):
        result = self.run_behavior("""
await respond({state:'running', retry_after_seconds:1});
button()?.click(); await fireTimer();
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
""")
        self.assertTrue(result["mounted"])
        self.assertTrue(result["disabled"])
        self.assertEqual(len(result["requests"]), 2)
        self.assertTrue(all(row.get('method', 'GET') == 'GET' for row in result["requests"]))
        self.assertEqual(result["table"], "new published snapshot")
        self.assertEqual(result["reloadArguments"], [[]])

    def test_server_conflict_waits_without_starting_another_collection(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
await respond({state:'running', retry_after_seconds:1}, 409);
button()?.click(); await fireTimer();
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
""")
        self.assertTrue(result["disabled"])
        self.assertEqual(result["table"], "new published snapshot")
        self.assertEqual(result["reloadArguments"], [[]])
        self.assertEqual(
            [row.get("method", "GET") for row in result["requests"]],
            ["GET", "POST", "GET"],
        )

    def test_server_cooldown_does_not_mislabel_the_click_as_success(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
await respond({state:'succeeded', retry_after_seconds:20, receipt:newReceipt}, 429);
""")
        self.assertTrue(result["disabled"])
        self.assertEqual(result["table"], "original published snapshot")
        self.assertEqual(result["reloadArguments"], [])
        self.assertIn("wait", result["message"].lower())
        self.assertEqual(result["timerDelays"], [20000])

    def test_leaving_opportunities_while_collecting_does_not_reload_another_view(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
app.route = {kind:'screener', filters:{}};
await respond({state:'succeeded', retry_after_seconds:30,
  receipt:newReceipt});
""")
        self.assertTrue(result["mounted"])
        self.assertEqual(result["reloadArguments"], [])
        self.assertEqual(result["table"], "original published snapshot")

    def test_lost_post_response_reloads_once_when_get_confirms_publication(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
pending.shift()?.reject(new Error('Connection closed')); await flush();
await fireTimer();
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
await fireTimer();
await respond({state:'succeeded', retry_after_seconds:0, receipt:newReceipt});
""")
        self.assertEqual(result["table"], "new published snapshot")
        self.assertEqual(result["reloadArguments"], [[]])
        self.assertTrue(result["filtersPreserved"])
        self.assertEqual(
            [row.get("method", "GET") for row in result["requests"]],
            ["GET", "POST", "GET", "GET"],
        )

    def test_successful_post_is_not_reloaded_again_by_unchanged_status(self):
        result = self.run_behavior("""
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
await fireTimer();
await respond({state:'succeeded', retry_after_seconds:0, receipt:newReceipt});
""")
        self.assertEqual(result["reloadArguments"], [[]])
        self.assertEqual(result["table"], "new published snapshot")

    def test_get_confirmation_does_not_reload_historical_or_another_page(self):
        for route in (
            "{kind:'opportunities', filters:{opportunityScope:'historical'}}",
            "{kind:'screener', filters:{}}",
        ):
            with self.subTest(route=route):
                result = self.run_behavior("""
await respond({state:'running', retry_after_seconds:1});
app.route = """ + route + ";" + """
await fireTimer();
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
""")
                self.assertEqual(result["reloadArguments"], [])
                self.assertEqual(result["table"], "original published snapshot")

    def test_confirmed_publication_preserves_invalid_route_validation(self):
        for response_route in ("post", "get"):
            with self.subTest(response_route=response_route):
                action = """
await respond({state:'idle', retry_after_seconds:0}); button()?.click();
app.route.validationErrors = [{field:'opportunity_scope', code:'duplicate_filter'}];
"""
                if response_route == "get":
                    action += """
pending.shift()?.reject(new Error('Connection closed')); await flush();
await fireTimer();
"""
                action += """
await respond({state:'succeeded', retry_after_seconds:30, receipt:newReceipt});
"""
                result = self.run_behavior(action)
                self.assertEqual(result["reloadArguments"], [])
                self.assertEqual(result["table"], "original published snapshot")


if __name__ == "__main__":
    unittest.main()
