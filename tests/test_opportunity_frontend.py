import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

from scripts.route_opportunity import ROUTE_OPPORTUNITY_REASON_CODES


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"


def run_app_javascript(source: str, *, prelude: str = ""):
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("Node.js is not installed in this runtime")
    script = prelude + "\n" + APP_PATH.read_text(encoding="utf-8") + "\n" + source
    completed = subprocess.run(
        [node, "-e", script],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


DOM_FIXTURE = r"""
function opportunityControl() {
  return {
    value: "", hidden: false, disabled: false, textContent: "", innerHTML: "",
    dataset: {}, attributes: {}, listeners: {},
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
    addEventListener(name, callback) { this.listeners[name] = callback; },
  };
}
const opportunityIds = [
  "opportunity-filter-form", "opportunity-token", "opportunity-venue",
  "opportunity-venue-options", "opportunity-filter-error",
  "opportunity-notional", "opportunity-class",
  "opportunity-route-type", "opportunity-availability", "opportunity-sort",
  "opportunity-direction", "opportunity-cohort-status", "opportunity-loading",
  "opportunity-status", "opportunity-error", "opportunity-bundle-unavailable",
  "strict-opportunities", "estimate-opportunities", "unavailable-opportunities",
  "strict-opportunity-empty", "estimate-opportunity-empty",
  "unavailable-opportunity-empty", "strict-opportunity-count",
  "estimate-opportunity-count", "unavailable-opportunity-count",
  "strict-opportunity-body", "estimate-opportunity-body",
  "unavailable-opportunity-body", "opportunities-view",
  "time-toolbar",
];
const opportunityElements = Object.fromEntries(
  opportunityIds.map((id) => [id, opportunityControl()]),
);
global.document = {
  getElementById(id) { return opportunityElements[id] || null; },
  querySelectorAll() { return []; },
};
global.window = { lucide: null };
"""


PAYLOAD_FIXTURE = r"""
const opportunityPayload = {
  availability: { status: "available", reason: "complete_bundle_published" },
  metadata: {
    contract_version: "opportunity_dashboard/v1",
    route_cohort_id: "cohort:" + "a".repeat(64),
    manifest_sha256: "a".repeat(64),
    publication_status: "published",
    checked_at: "2026-08-01T00:00:30Z",
    max_route_age_seconds: 120,
    max_route_skew_seconds: 60,
    available_venues: ["alpha", "beta", "swap"],
    available_notionals_usd: [10000],
    coverage: {
      route_count: 4, scenario_count: 4, returned_count: 4,
      class_counts: {
        executable_candidate: 1, research_estimate: 2, unavailable: 1,
      },
      availability_counts: { available: 2, unavailable: 2 },
    },
  },
  filters: {
    token: null, venue: null, notional_usd: 10000, opportunity_class: "all",
    route_type: "all", availability: "all", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: [
    {
      route_id: "route:strict", opportunity_id: "route:strict:10000",
      token_symbol: "AAVE", buy_market_id: "cex:alpha:AAVE/USD",
      sell_market_id: "cex:beta:AAVE/USD", route_type: "cex_cex",
      route_mode: "prepositioned_inventory", requested_notional_usd: 10000,
      target_token_quantity: 100, opportunity_class: "executable_candidate",
      availability: { status: "available", reason: null },
      gross_edge_usd: 120, gross_edge_bps: 120, net_edge_usd: 100,
      net_edge_bps: 100,
      cost_breakdown: {
        strict_nonembedded_usd: 20, research_bounded_usd: 0,
        research_assumed_usd: 0,
      },
      cost_components: [], cost_completeness: "complete",
      scenario_cost_completeness: "complete",
      leg_timestamps: { buy: "2026-08-01T00:00:00Z", sell: "2026-08-01T00:00:01Z" },
      skew_seconds: 1, route_age_seconds: 29, capacity_quantity: 0,
      primary_reason: "positive_strict_net_edge", reason_codes: [], source_links: [],
    },
    {
      route_id: "route:estimate", opportunity_id: "route:estimate:10000",
      token_symbol: "ETH", buy_market_id: "cex:alpha:ETH/USD",
      sell_market_id: "dex:eth:swap:pool:ETH", route_type: "cex_dex",
      route_mode: "prepositioned_inventory", requested_notional_usd: 10000,
      target_token_quantity: 4, opportunity_class: "research_estimate",
      availability: { status: "available", reason: null },
      gross_edge_usd: 80, gross_edge_bps: 80, net_edge_usd: 50,
      net_edge_bps: 50,
      cost_breakdown: {
        strict_nonembedded_usd: 10, research_bounded_usd: 15,
        research_assumed_usd: 5,
      },
      cost_components: [], cost_completeness: "incomplete",
      scenario_cost_completeness: "complete",
      leg_timestamps: { buy: "2026-08-01T00:00:00Z", sell: "2026-08-01T00:00:30Z" },
      skew_seconds: 30, route_age_seconds: 30, capacity_quantity: 4,
      primary_reason: "cost_component_estimated",
      reason_codes: ["cost_component_estimated"], source_links: [],
    },
    {
      route_id: "route:stale-zero", opportunity_id: "route:stale-zero:10000",
      token_symbol: "UNI", buy_market_id: "cex:alpha:UNI/USD",
      sell_market_id: "cex:beta:UNI/USD", route_type: "cex_cex",
      route_mode: "prepositioned_inventory", requested_notional_usd: 10000,
      target_token_quantity: null, opportunity_class: "research_estimate",
      availability: { status: "unavailable", reason: "cohort_stale" },
      gross_edge_usd: null, gross_edge_bps: null, net_edge_usd: null,
      net_edge_bps: null,
      cost_breakdown: {
        strict_nonembedded_usd: null, research_bounded_usd: null,
        research_assumed_usd: null,
      },
      cost_components: [], cost_completeness: "complete",
      scenario_cost_completeness: "complete",
      leg_timestamps: { buy: "2026-08-01T00:00:00Z", sell: "2026-08-01T00:00:00Z" },
      skew_seconds: 0, route_age_seconds: 121, capacity_quantity: null,
      primary_reason: "cohort_stale", reason_codes: ["cohort_stale"], source_links: [],
    },
    {
      route_id: "route:unavailable", opportunity_id: "route:unavailable:10000",
      token_symbol: "LINK", buy_market_id: "cex:alpha:LINK/USD",
      sell_market_id: "dex:eth:swap:pool:LINK", route_type: "cex_dex",
      route_mode: "research_only", requested_notional_usd: 10000,
      target_token_quantity: null, opportunity_class: "unavailable",
      availability: { status: "unavailable", reason: "snapshot_skew_exceeded" },
      gross_edge_usd: null, gross_edge_bps: null, net_edge_usd: null,
      net_edge_bps: null,
      cost_breakdown: {
        strict_nonembedded_usd: null, research_bounded_usd: null,
        research_assumed_usd: null,
      },
      cost_components: [], cost_completeness: "unavailable",
      scenario_cost_completeness: "unavailable",
      leg_timestamps: { buy: "2026-08-01T00:00:00Z", sell: "2026-08-01T00:02:00Z" },
      skew_seconds: 120, route_age_seconds: null, capacity_quantity: null,
      primary_reason: "snapshot_skew_exceeded",
      reason_codes: ["snapshot_skew_exceeded"], source_links: [],
    },
  ],
};
"""


class OpportunityPageShellTest(unittest.TestCase):
    def test_every_route_reason_has_an_expert_facing_label(self):
        source = APP_PATH.read_text(encoding="utf-8")
        registry = source.split(
            "const OPPORTUNITY_REASON_LABELS = Object.freeze({",
            1,
        )[1].split("});", 1)[0]
        labels = set(re.findall(r"^\s{2}([a-z0-9_]+):", registry, re.MULTILINE))

        self.assertLessEqual(ROUTE_OPPORTUNITY_REASON_CODES, labels)

    def test_opportunity_deep_link_is_revealed_without_daily_time_toolbar(self):
        result = run_app_javascript(
            r"""
const views = ["screener", "opportunities", "workspace"].map((kind) => ({
  dataset: { appView: kind }, hidden: true,
}));
const timeToolbar = { hidden: false };
global.document = {
  getElementById(id) { return id === "time-toolbar" ? timeToolbar : null; },
  querySelectorAll(selector) { return selector === "[data-app-view]" ? views : []; },
};
primeInitialRouteView({ kind: "opportunities", filters: {} });
console.log(JSON.stringify({
  visible: views.filter((view) => !view.hidden).map((view) => view.dataset.appView),
  toolbarHidden: timeToolbar.hidden,
  title: routeTitle({ kind: "opportunities", filters: {} }),
}));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )
        self.assertEqual(result["visible"], ["opportunities"])
        self.assertTrue(result["toolbarHidden"])
        self.assertEqual(
            result["title"],
            "Opportunities · CEX / DEX Market Monitor",
        )

    def test_primary_navigation_and_independent_page_shell_are_complete(self):
        index = INDEX_PATH.read_text(encoding="utf-8")

        self.assertIn('<option value="volume">Route volume (USD)</option>', index)

        screener = index.index('data-app-route="screener"')
        opportunities = index.index('data-app-route="opportunities"')
        markets = index.index('data-app-route="markets"')
        self.assertLess(screener, opportunities)
        self.assertLess(opportunities, markets)
        self.assertIn('href="/opportunities"', index)

        view_start = index.index('id="opportunities-view"')
        view_end = index.index('id="screener-view"', view_start)
        page = index[view_start:view_end]
        self.assertIn('data-app-view="opportunities"', page)
        self.assertIn('aria-labelledby="opportunities-title"', page)
        self.assertIn('>Route Opportunities</h2>', page)
        self.assertNotIn('>Executable Opportunities</h2>', page)
        for control_id in (
            "opportunity-token",
            "opportunity-venue",
            "opportunity-venue-options",
            "opportunity-notional",
            "opportunity-class",
            "opportunity-route-type",
            "opportunity-availability",
            "opportunity-sort",
            "opportunity-direction",
            "apply-opportunity-filters",
            "opportunity-filter-error",
        ):
            self.assertIn(f'id="{control_id}"', page)

        for state_id in (
            "opportunity-status",
            "opportunity-error",
            "opportunity-bundle-unavailable",
            "opportunity-loading",
            "strict-opportunity-empty",
            "estimate-opportunity-empty",
            "unavailable-opportunity-empty",
        ):
            self.assertIn(f'id="{state_id}"', page)

        for section_id, body_id in (
            ("strict-opportunities", "strict-opportunity-body"),
            ("estimate-opportunities", "estimate-opportunity-body"),
            ("unavailable-opportunities", "unavailable-opportunity-body"),
        ):
            self.assertIn(f'id="{section_id}"', page)
            self.assertIn(f'id="{body_id}"', page)
        self.assertIn("Strict executable candidates", page)
        self.assertIn("Research estimates", page)
        self.assertIn("Unavailable routes", page)
        self.assertIn("Daily Price Gap", page)
        self.assertIn("not an executable route", page)

    def test_opportunity_tables_and_mobile_disclosures_have_no_clipping_contract(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        self.assertGreaterEqual(index.count('class="opportunity-table-scroll"'), 3)
        self.assertGreaterEqual(index.count('class="opportunity-table"'), 3)
        self.assertIn(".opportunity-table", styles)
        self.assertIn("font-variant-numeric: tabular-nums", styles)

        mobile_match = re.search(
            r"@media \(max-width: 700px\) \{(?P<body>[\s\S]+)\}\s*$",
            styles,
        )
        self.assertIsNotNone(mobile_match)
        mobile = mobile_match.group("body")
        self.assertIn(".opportunity-table-scroll", mobile)
        self.assertIn("overflow: visible", mobile)
        self.assertIn(".opportunity-table", mobile)
        self.assertIn("display: block", mobile)
        self.assertIn(".opportunity-na-disclosure", mobile)
        self.assertIn("position: static", mobile)
        thead_rule = mobile[
            mobile.index(".opportunity-table thead {"):
            mobile.index(".opportunity-table tbody {", mobile.index(".opportunity-table thead {"))
        ]
        self.assertNotIn("display: none", thead_rule)
        self.assertIn("clip: rect(0, 0, 0, 0)", thead_rule)

    def test_timing_rank_defaults_put_smallest_age_and_skew_first(self):
        result = run_app_javascript(
            r"""
console.log(JSON.stringify({
  age: normalizedOpportunityFilters({ sort: "route_age_seconds" }).dir,
  skew: normalizedOpportunityFilters({ sort: "skew_seconds" }).dir,
  edge: normalizedOpportunityFilters({ sort: "net_edge_usd" }).dir,
  explicit: normalizedOpportunityFilters({
    sort: "route_age_seconds", dir: "desc",
  }).dir,
}));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )
        self.assertEqual(result, {
            "age": "asc",
            "skew": "asc",
            "edge": "desc",
            "explicit": "desc",
        })

    def test_filter_submit_blocks_invalid_token_without_navigation_or_scope_widening(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + r"""
const navigations = [];
navigateTo = (path) => navigations.push(path);
opportunityElements["opportunity-token"].value = "AAVE?BAD";
opportunityElements["opportunity-venue"].value = "kraken";
bindOpportunityFilterEvents();
const event = {
  prevented: false,
  preventDefault() { this.prevented = true; },
};
opportunityElements["opportunity-filter-form"].listeners.submit(event);
const invalidState = {
  prevented: event.prevented,
  navigations: [...navigations],
  token: opportunityElements["opportunity-token"].value,
  error: opportunityElements["opportunity-filter-error"].textContent,
  errorHidden: opportunityElements["opportunity-filter-error"].hidden,
  ariaInvalid: opportunityElements["opportunity-token"].attributes["aria-invalid"],
};
opportunityElements["opportunity-token"].value = "AAVE";
opportunityElements["opportunity-filter-form"].listeners.submit({
  preventDefault() {},
});
console.log(JSON.stringify({ invalidState, navigations }));
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )

        self.assertTrue(result["invalidState"]["prevented"])
        self.assertEqual(result["invalidState"]["navigations"], [])
        self.assertEqual(result["invalidState"]["token"], "AAVE?BAD")
        self.assertFalse(result["invalidState"]["errorHidden"])
        self.assertIn("Token", result["invalidState"]["error"])
        self.assertEqual(result["invalidState"]["ariaInvalid"], "true")
        self.assertEqual(len(result["navigations"]), 1)
        self.assertIn("token=AAVE", result["navigations"][0])
        self.assertIn("venue=kraken", result["navigations"][0])

    def test_direct_invalid_opportunity_url_renders_error_and_never_fetches(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + r"""
let fetchCount = 0;
global.fetch = () => { fetchCount += 1; throw new Error("must not fetch"); };
const route = MarketMonitorNavigation.parseRoute(
  "/opportunities",
  "?token=AAVE%3FBAD&venue=kraken",
);
Promise.resolve(applyOpportunitiesRoute(route)).then((loaded) => {
  console.log(JSON.stringify({
    loaded,
    fetchCount,
    token: opportunityElements["opportunity-token"].value,
    venue: opportunityElements["opportunity-venue"].value,
    error: opportunityElements["opportunity-filter-error"].textContent,
    ariaInvalid: opportunityElements["opportunity-token"].attributes["aria-invalid"],
  }));
});
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )

        self.assertFalse(result["loaded"])
        self.assertEqual(result["fetchCount"], 0)
        self.assertEqual(result["token"], "AAVE?BAD")
        self.assertEqual(result["venue"], "kraken")
        self.assertIn("Token", result["error"])
        self.assertEqual(result["ariaInvalid"], "true")

    def test_real_route_startup_keeps_invalid_opportunity_url_inline(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + r"""
let fetchCount = 0;
global.fetch = () => { fetchCount += 1; throw new Error("must not fetch"); };
window.location = {
  pathname: "/opportunities",
  search: "?token=AAVE%3FBAD&venue=kraken",
};
window.history = {
  replacements: [],
  replaceState(_state, _title, path) { this.replacements.push(path); },
};
announceRoute = () => {};
updateRouteLinks = () => {};

Promise.resolve(applyRouteFromLocation()).then(
  (loaded) => console.log(JSON.stringify({
    loaded,
    threw: false,
    fetchCount,
    routeKind: app.route.kind,
    token: opportunityElements["opportunity-token"].value,
    venue: opportunityElements["opportunity-venue"].value,
    error: opportunityElements["opportunity-filter-error"].textContent,
    errorHidden: opportunityElements["opportunity-filter-error"].hidden,
    replacements: window.history.replacements,
  })),
  (error) => console.log(JSON.stringify({
    threw: true,
    message: error.message,
    fetchCount,
  })),
);
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )

        self.assertFalse(result["threw"])
        self.assertFalse(result["loaded"])
        self.assertEqual(result["fetchCount"], 0)
        self.assertEqual(result["routeKind"], "opportunities")
        self.assertEqual(result["token"], "AAVE?BAD")
        self.assertEqual(result["venue"], "kraken")
        self.assertFalse(result["errorHidden"])
        self.assertIn("Token", result["error"])
        self.assertEqual(result["replacements"], [])


class OpportunityRendererTest(unittest.TestCase):
    def test_cost_na_reasons_and_fractional_timing_remain_exact(self):
        result = run_app_javascript(
            r"""
const route = {
  route_id: "route:timing-evidence",
  opportunity_id: "opportunity:timing-evidence",
  token_symbol: "AAVE",
  buy_market_id: "cex:alpha:AAVE/USD",
  sell_market_id: "cex:beta:AAVE/USD",
  route_type: "cex_cex",
  route_mode: "prepositioned_inventory",
  requested_notional_usd: "10000",
  opportunity_class: "executable_candidate",
  availability: { status: "unavailable", reason: "cohort_stale" },
  gross_edge_usd: null,
  net_edge_usd: null,
  net_edge_bps: null,
  capacity_quantity: null,
  cost_breakdown: {
    strict_nonembedded_usd: null,
    research_bounded_usd: null,
    research_assumed_usd: null,
  },
  cost_components: [
    {
      leg: "route", component_type: "rebalancing_or_transfer",
      value_status: "not_applicable", strict_eligible: true,
      reflected_or_embedded: false, amount_usd: null, rate_bps: null,
      reason_code: null,
    },
    {
      leg: "buy", component_type: "venue_taker_fee",
      value_status: "authenticated", strict_eligible: true,
      reflected_or_embedded: true, amount_usd: null, rate_bps: null,
      reason_code: null,
    },
    {
      leg: "sell", component_type: "venue_taker_fee",
      value_status: "stale", strict_eligible: false,
      reflected_or_embedded: false, amount_usd: null, rate_bps: null,
      reason_code: "cost_component_stale",
    },
    {
      leg: "route", component_type: "network_gas",
      value_status: "failed", strict_eligible: false,
      reflected_or_embedded: false, amount_usd: null, rate_bps: null,
      reason_code: "cost_evidence_failed",
    },
  ],
  leg_timestamps: {
    buy: "2026-08-01T12:00:00.000000Z",
    sell: "2026-08-01T12:00:00.123456Z",
  },
  skew_seconds: 60.0000001,
  route_age_seconds: 120.0000001,
  primary_reason: "positive_strict_net_edge",
  reason_codes: [],
  source_links: [],
};
const markup = opportunityRowMarkup(route);
console.log(JSON.stringify({ markup }));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )

        markup = result["markup"]
        self.assertIn("Not applicable under this route contract", markup)
        self.assertIn(
            "The synchronized route cohort is older than the strict freshness SLA",
            markup,
        )
        self.assertIn("At least one route cost component is stale", markup)
        self.assertIn("cost_evidence_failed", markup)
        self.assertIn("not_applicable", markup)
        self.assertIn("authenticated", markup)
        self.assertIn("stale", markup)
        self.assertIn("2026-08-01 12:00:00.000000 UTC", markup)
        self.assertIn("2026-08-01 12:00:00.123456 UTC", markup)
        self.assertIn("60.0000001 s", markup)
        self.assertIn("120.0000001 s", markup)
        self.assertIn('data-label="Route volume"', markup)
        self.assertIn(
            "One or both route legs lack a positive source-horizon USD volume",
            markup,
        )
        self.assertIn("ranking reference, not executable capacity", markup)

    def test_research_link_pair_is_ephemeral_until_user_applies_it(self):
        result = run_app_javascript(
            r"""
function control(value = "") {
  return {
    value, hidden: false, disabled: false, textContent: "", innerHTML: "",
    dataset: {}, attributes: {},
    setAttribute(name, nextValue) { this.attributes[name] = String(nextValue); },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}
const controls = new Map([
  ["facts-token", control("AAVE")],
  ["facts-market-a", control("")],
  ["facts-market-b", control("")],
  ["workspace-context-notice", control()],
  ["error-banner", control()],
  ["time-toolbar", control()],
  ["facts-workbench", control()],
  ["execution-notional", control("10000")],
]);
global.document = {
  getElementById(id) {
    if (!controls.has(id)) controls.set(id, control());
    return controls.get(id);
  },
  querySelector() { return null; },
  querySelectorAll() { return []; },
};

const savedPair = {
  marketA: "cex:coinbase:AAVE/USD",
  marketB: "dex:eth:uniswap:old-pool:AAVE",
};
let stored = JSON.stringify({ AAVE: savedPair });
const storageWrites = [];
global.window = {
  location: { pathname: "/opportunities", search: "" },
  history: { replaceState() {}, pushState() {} },
  sessionStorage: {
    getItem() { return stored; },
    setItem(_key, value) {
      stored = value;
      storageWrites.push(JSON.parse(value));
    },
  },
  lucide: null,
};

const newBuy = "cex:alpha:AAVE/USD";
const newSell = "cex:beta:AAVE/USD";
app.payload = {
  metadata: {},
  tokens: [{ token_symbol: "AAVE" }],
};
app.catalog = {
  markets: [
    { market_id: savedPair.marketA, token_symbol: "AAVE", market_type: "cex", venue: "coinbase", instrument: "AAVE/USD" },
    { market_id: savedPair.marketB, token_symbol: "AAVE", market_type: "dex", venue: "uniswap", instrument: "old-pool" },
    { market_id: newBuy, token_symbol: "AAVE", market_type: "cex", venue: "alpha", instrument: "AAVE/USD" },
    { market_id: newSell, token_symbol: "AAVE", market_type: "cex", venue: "beta", instrument: "AAVE/USD" },
  ],
};
app.pairSelections = readPairSelections();

const row = opportunityRowMarkup({
  route_id: "route:alpha-beta",
  token_symbol: "AAVE",
  buy_market_id: newBuy,
  sell_market_id: newSell,
  availability: { status: "unavailable", reason: "cost_components_incomplete" },
  cost_breakdown: {},
  cost_components: [],
  leg_timestamps: {},
});
const href = (row.match(/class="opportunity-route-id route-action" href="([^"]+)"/)?.[1] || "")
  .replaceAll("&amp;", "&");
const target = new URL(href, "https://dashboard.example");
const route = MarketMonitorNavigation.parseRoute(target.pathname, target.search);

preferredCatalogMarket = () => null;
renderFactsMarketWarnings = () => {};
renderLiquidityCurve = () => {};
renderWorkspaceContext = () => {};
renderWorkspaceMarkets = () => {};
renderQualityFromCatalog = () => {};
updateFactsContract = () => {};
syncMarketPayloadForWindow = () => {};
syncTimeWindowControls = () => {};
setDraftTimeWindow = () => {};
setActiveAppView = () => {};
setActiveWorkspacePage = () => {};
syncSegmentedControls = () => {};
loadExecutionCost = () => {};
applyWorkspaceRoute(route);
// A same-window Summary refresh repopulates the controls with preserve=true.
// It must retain the transient view without silently turning it into a saved pair.
populateFactsMarkets({ preserve: true });

const beforeApply = {
  currentPair: selectedPairState(),
  savedPair: { ...app.pairSelections.AAVE },
  storedPair: { ...JSON.parse(stored).AAVE },
  storageWrites: [...storageWrites],
};
const manuallyApplied = persistSelectedPair();
const comparePath = currentWorkspacePath("compare");

console.log(JSON.stringify({
  href,
  pairMode: route.state.pairMode || "",
  beforeApply,
  manuallyApplied,
  savedPairAfterApply: app.pairSelections.AAVE,
  storedPairAfterApply: JSON.parse(stored).AAVE,
  storageWritesAfterApply: storageWrites,
  comparePath,
}));
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )

        expected_saved = {
            "marketA": "cex:coinbase:AAVE/USD",
            "marketB": "dex:eth:uniswap:old-pool:AAVE",
        }
        self.assertIn("pairMode=transient", result["href"])
        self.assertEqual(result["pairMode"], "transient")
        self.assertEqual(result["beforeApply"]["currentPair"], {
            "marketA": "cex:alpha:AAVE/USD",
            "marketB": "cex:beta:AAVE/USD",
        })
        self.assertEqual(result["beforeApply"]["savedPair"], expected_saved)
        self.assertEqual(result["beforeApply"]["storedPair"], expected_saved)
        self.assertEqual(result["beforeApply"]["storageWrites"], [])
        expected_applied = {
            "marketA": "cex:alpha:AAVE/USD",
            "marketB": "cex:beta:AAVE/USD",
        }
        self.assertTrue(result["manuallyApplied"])
        self.assertEqual(result["savedPairAfterApply"], expected_applied)
        self.assertEqual(result["storedPairAfterApply"], expected_applied)
        self.assertEqual(len(result["storageWritesAfterApply"]), 1)
        self.assertNotIn("pairMode=transient", result["comparePath"])

    def test_renderer_partitions_classes_preserves_zero_and_reconciles_costs(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + r"""
renderOpportunities(opportunityPayload);
const strict = opportunityElements["strict-opportunity-body"].innerHTML;
const estimate = opportunityElements["estimate-opportunity-body"].innerHTML;
const unavailable = opportunityElements["unavailable-opportunity-body"].innerHTML;
const allMarkup = strict + estimate + unavailable;
console.log(JSON.stringify({
  strict, estimate, unavailable,
  naValues: (allMarkup.match(/<span>N\/A<\/span>/g) || []).length,
  naLabels: (allMarkup.match(/aria-label="N\/A reason/g) || []).length,
  naIcons: (allMarkup.match(/data-lucide="info"/g) || []).length,
  counts: {
    strict: opportunityElements["strict-opportunity-count"].textContent,
    estimate: opportunityElements["estimate-opportunity-count"].textContent,
    unavailable: opportunityElements["unavailable-opportunity-count"].textContent,
  },
}));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )
        self.assertIn("route:strict", result["strict"])
        self.assertNotIn("route:estimate", result["strict"])
        self.assertIn("route:estimate", result["estimate"])
        self.assertNotIn("route:stale-zero", result["estimate"])
        self.assertNotIn("route:strict", result["estimate"])
        self.assertIn("route:unavailable", result["unavailable"])
        self.assertIn("route:stale-zero", result["unavailable"])
        self.assertNotIn("route:estimate", result["unavailable"])

        self.assertIn('data-opportunity-value="0">0</span>', result["strict"])
        self.assertEqual(result["naValues"], result["naLabels"])
        self.assertEqual(result["naValues"], result["naIcons"])
        self.assertGreater(result["naValues"], 0)
        self.assertIn("Snapshot skew exceeds", result["unavailable"])
        self.assertIn("cohort_stale", result["unavailable"])

        self.assertIn('data-gross-edge-usd="120"', result["strict"])
        self.assertIn('data-total-cost-usd="20"', result["strict"])
        self.assertIn('data-net-edge-usd="100"', result["strict"])
        self.assertIn("Reconciled", result["strict"])
        self.assertIn('data-total-cost-usd="30"', result["estimate"])
        self.assertEqual(result["counts"], {
            "strict": "1 route",
            "estimate": "1 route",
            "unavailable": "2 routes",
        })

    def test_cohort_badge_is_compact_but_keeps_full_identity(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + r"""
renderOpportunities(opportunityPayload);
const badge = opportunityElements["opportunity-cohort-status"];
console.log(JSON.stringify({
  visible: badge.textContent,
  title: badge.attributes.title,
  aria: badge.attributes["aria-label"],
  venueOptions: opportunityElements["opportunity-venue-options"].innerHTML,
  full: opportunityPayload.metadata.route_cohort_id,
  status: opportunityElements["opportunity-status"].textContent,
}));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )
        self.assertLess(len(result["visible"]), len(result["full"]))
        self.assertEqual(result["title"], result["full"])
        self.assertIn(result["full"], result["aria"])
        self.assertIn('value="alpha"', result["venueOptions"])
        self.assertIn('value="swap"', result["venueOptions"])
        self.assertIn("age ≤ 120s", result["status"])
        self.assertIn("skew ≤ 60s", result["status"])

    def test_missing_bundle_is_unavailable_while_published_empty_is_not_zero(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + r"""
const missing = {
  availability: { status: "unavailable", reason: "complete_pointer_absent" },
  metadata: null,
  filters: {},
  routes: [],
};
renderOpportunities(missing);
const missingState = {
  message: opportunityElements["opportunity-bundle-unavailable"].textContent,
  hidden: opportunityElements["opportunity-bundle-unavailable"].hidden,
  inventories: ["strict", "estimate", "unavailable"].map(
    (name) => opportunityElements[`${name}-opportunities`].hidden,
  ),
};
const publishedEmpty = {
  availability: { status: "available", reason: "complete_bundle_published" },
  metadata: {
    route_cohort_id: "cohort-empty", publication_status: "published",
    checked_at: "2026-08-01T00:00:30Z",
    coverage: { route_count: 20, scenario_count: 100, returned_count: 0 },
  },
  filters: {
    token: "AAVE", notional_usd: 10000, opportunity_class: "strict",
    route_type: "all", availability: "available", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: [],
};
renderOpportunities(publishedEmpty);
const emptyState = {
  bundleHidden: opportunityElements["opportunity-bundle-unavailable"].hidden,
  strictHidden: opportunityElements["strict-opportunities"].hidden,
  strictEmptyHidden: opportunityElements["strict-opportunity-empty"].hidden,
  strictEmpty: opportunityElements["strict-opportunity-empty"].textContent,
};
console.log(JSON.stringify({ missingState, emptyState }));
""",
            prelude="globalThis.MarketMonitorNavigation = {};",
        )
        self.assertFalse(result["missingState"]["hidden"])
        self.assertTrue(all(result["missingState"]["inventories"]))
        self.assertIn("not published", result["missingState"]["message"])
        self.assertIn("No numeric zero is inferred", result["missingState"]["message"])
        self.assertTrue(result["emptyState"]["bundleHidden"])
        self.assertFalse(result["emptyState"]["strictHidden"])
        self.assertFalse(result["emptyState"]["strictEmptyHidden"])
        self.assertIn("does not mean there is no Daily Price Gap", result["emptyState"]["strictEmpty"])

    def test_latest_filter_request_owns_render_and_route_change_rejects_late_response(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + r"""
const requests = [];
global.AbortController = class {
  constructor() { this.signal = {}; this.aborted = false; }
  abort() { this.aborted = true; }
};
global.fetch = (url) => new Promise((resolve) => requests.push({ url, resolve }));
function response(payload) {
  return { ok: true, status: 200, async json() { return payload; } };
}
function payloadFor(token, routeId) {
  const payload = JSON.parse(JSON.stringify(opportunityPayload));
  payload.filters.token = token;
  payload.filters.notional_usd = token === "AAVE" ? 10000 : 50000;
  payload.routes = [payload.routes[0]];
  payload.routes[0].token_symbol = token;
  payload.routes[0].route_id = routeId;
  payload.metadata.coverage.returned_count = 1;
  return payload;
}
(async () => {
  const firstFilters = {
    token: "AAVE", venue: "alpha", notionalUsd: 10000, opportunityClass: "all",
    routeType: "all", availability: "all", sort: "net_edge_usd", dir: "desc",
  };
  const secondFilters = { ...firstFilters, token: "ETH", notionalUsd: 50000 };
  app.route = { kind: "opportunities", filters: firstFilters };
  const first = loadOpportunities(firstFilters);
  app.route = { kind: "opportunities", filters: secondFilters };
  const second = loadOpportunities(secondFilters);
  requests[1].resolve(response(payloadFor("ETH", "route:latest")));
  const secondResult = await second;
  requests[0].resolve(response(payloadFor("AAVE", "route:late-old")));
  const firstResult = await first;

  const third = loadOpportunities(secondFilters);
  app.route = { kind: "screener", filters: {} };
  requests[2].resolve(response(payloadFor("ETH", "route:after-navigation")));
  const thirdResult = await third;
  console.log(JSON.stringify({
    urls: requests.map((request) => request.url),
    secondResult, firstResult, thirdResult,
    rendered: opportunityElements["strict-opportunity-body"].innerHTML,
    retainedRoute: app.opportunities.routes[0].route_id,
    status: opportunityElements["opportunity-status"].textContent,
  }));
})();
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )
        self.assertIn("token=AAVE", result["urls"][0])
        self.assertIn("venue=alpha", result["urls"][0])
        self.assertIn("notional=50000", result["urls"][1])
        self.assertTrue(result["secondResult"])
        self.assertFalse(result["firstResult"])
        self.assertFalse(result["thirdResult"])
        self.assertEqual(result["retainedRoute"], "route:latest")
        self.assertIn("route:latest", result["rendered"])
        self.assertNotIn("route:late-old", result["rendered"])
        self.assertNotIn("route:after-navigation", result["rendered"])

    def test_route_application_loads_api_and_server_error_replaces_previous_rows(self):
        result = run_app_javascript(
            DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + r"""
global.AbortController = class {
  constructor() { this.signal = {}; }
  abort() {}
};
const requests = [];
global.fetch = async (url) => {
  requests.push(url);
  if (requests.length === 1) {
    return {
      ok: true, status: 200,
      async json() { return opportunityPayload; },
    };
  }
  return {
    ok: false, status: 503,
    async json() {
      return {
        code: "opportunity_bundle_validation_failed",
        message: "Published route opportunity data failed validation. Retry after the next complete publication.",
        error: "PRIVATE_EXCEPTION_SENTINEL /private/data/routes/latest.json",
      };
    },
  };
};
(async () => {
  const route = {
    kind: "opportunities",
    filters: {
      token: "AAVE", notionalUsd: 10000, opportunityClass: "all",
      routeType: "all", availability: "all", sort: "net_edge_usd", dir: "desc",
    },
  };
  const loaded = await applyOpportunitiesRoute(route);
  const renderedBeforeError = opportunityElements["strict-opportunity-body"].innerHTML;
  const failed = await loadOpportunities(route.filters);
  console.log(JSON.stringify({
    requests, loaded, failed, renderedBeforeError,
    renderedAfterError: opportunityElements["strict-opportunity-body"].innerHTML,
    error: opportunityElements["opportunity-error"].textContent,
    toolbarHidden: opportunityElements["time-toolbar"].hidden,
  }));
})();
""",
            prelude="""
globalThis.MarketMonitorNavigation = require(
  '/private/tmp/CEX-DEX-analysis-critical-round/dashboard/static/navigation.js'
);
""",
        )
        self.assertTrue(result["loaded"])
        self.assertFalse(result["failed"])
        self.assertIn("/api/markets/opportunities?", result["requests"][0])
        self.assertIn("route:strict", result["renderedBeforeError"])
        self.assertEqual(result["renderedAfterError"], "")
        self.assertEqual(
            result["error"],
            (
                "Published route opportunity data failed validation. "
                "Retry after the next complete publication."
            ),
        )
        self.assertNotIn("PRIVATE_EXCEPTION_SENTINEL", result["error"])
        self.assertNotIn("/private/data", result["error"])
        self.assertTrue(result["toolbarHidden"])


if __name__ == "__main__":
    unittest.main()
