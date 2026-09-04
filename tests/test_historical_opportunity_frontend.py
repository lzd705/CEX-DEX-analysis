import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

from scripts.historical_opportunity_demo_fixture import (
    HistoricalOpportunityDemoFixture,
)
from tests.test_opportunity_frontend import PAYLOAD_FIXTURE


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
NAVIGATION_PATH = PROJECT_ROOT / "dashboard" / "static" / "navigation.js"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"
CURRENT_FRONTEND_TEST_PATH = PROJECT_ROOT / "tests" / "test_opportunity_frontend.py"

HISTORICAL_DISCLAIMER = (
    "Historical Scenarios. Production rows are fixed-block counterfactual "
    "replays under their declared execution model. Local demo rows are "
    "deterministic synthetic fixture outputs; no live RPC, execution, or "
    "Foundry verification is run. Evidence status is shown per row. Values "
    "are research estimates at the displayed block or fixture reference; "
    "they are not current and are not executable candidates."
)


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


def navigation_prelude() -> str:
    return (
        "globalThis.MarketMonitorNavigation = require("
        + json.dumps(str(NAVIGATION_PATH))
        + ");"
    )


HISTORICAL_DOM_FIXTURE = r"""
function opportunityControl() {
  return {
    value: "", hidden: false, disabled: false, textContent: "", innerHTML: "",
    dataset: {}, attributes: {}, listeners: {}, classList: {
      values: new Set(),
      toggle(name, enabled) {
        if (enabled) this.values.add(name); else this.values.delete(name);
      },
      contains(name) { return this.values.has(name); },
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
      if (name.startsWith("data-")) {
        const key = name.slice(5).replace(/-([a-z])/g, (_match, letter) => letter.toUpperCase());
        this.dataset[key] = String(value);
      }
    },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
    addEventListener(name, callback) { this.listeners[name] = callback; },
  };
}
const opportunityIds = [
  "opportunity-filter-form", "opportunity-token", "opportunity-venue",
  "opportunity-venue-options", "opportunity-filter-error",
  "opportunity-notional", "opportunity-class", "opportunity-route-type",
  "opportunity-availability", "opportunity-sort", "opportunity-direction",
  "opportunity-cohort-status", "opportunity-loading", "opportunity-status",
  "opportunity-error", "opportunity-bundle-unavailable", "opportunities-view",
  "opportunity-current-context", "opportunity-historical-context",
  "historical-opportunity-demo-label",
  "data-actions-nav",
  "freshness", "freshness-cluster",
  "historical-opportunity-inventory", "historical-opportunity-count",
  "historical-opportunity-empty", "historical-opportunity-body",
  "strict-opportunities", "estimate-opportunities", "unavailable-opportunities",
  "strict-opportunity-empty", "estimate-opportunity-empty",
  "unavailable-opportunity-empty", "strict-opportunity-count",
  "estimate-opportunity-count", "unavailable-opportunity-count",
  "strict-opportunity-body", "estimate-opportunity-body",
  "unavailable-opportunity-body", "time-toolbar",
];
const opportunityElements = Object.fromEntries(
  opportunityIds.map((id) => [id, opportunityControl()]),
);
opportunityElements["freshness"].textContent = "Loading data";
opportunityElements["freshness-cluster"].dataset.status = "loading";
const opportunityScopeButtons = ["current", "historical"].map((scope) => {
  const button = opportunityControl();
  button.dataset.opportunityScope = scope;
  return button;
});
global.document = {
  getElementById(id) { return opportunityElements[id] || null; },
  querySelectorAll(selector) {
    if (selector === "[data-opportunity-scope]") return opportunityScopeButtons;
    return [];
  },
};
global.window = { lucide: null };
global.AbortController = class {
  constructor() { this.signal = {}; }
  abort() { this.signal.aborted = true; }
};
"""


HISTORICAL_PAYLOAD_FIXTURE = r"""
const historicalGeneration = "a".repeat(64);
const historicalReplayId = "replay:" + "b".repeat(64);
const historicalRows = Array.from({ length: 10 }, (_unused, index) => {
  const direction = index < 5 ? "uniswap_to_sushiswap" : "sushiswap_to_uniswap";
  const notional = ["1000", "5000", "10000", "50000", "100000"][index % 5];
  const suffix = String(index).padStart(2, "0");
  return {
    opportunity_id: `historical:${suffix}`,
    route_id: `historical:${direction}`,
    token_symbol: "UNI",
    buy_market_id: direction === "uniswap_to_sushiswap" ? "dex:ethereum:uniswap_v2:pool:UNI-WETH" : "dex:ethereum:sushiswap_v2:pool:UNI-WETH",
    sell_market_id: direction === "uniswap_to_sushiswap" ? "dex:ethereum:sushiswap_v2:pool:UNI-WETH" : "dex:ethereum:uniswap_v2:pool:UNI-WETH",
    route_type: "dex_dex",
    route_mode: "historical_counterfactual_state_override_next_block",
    direction,
    requested_notional_usd: notional,
    opportunity_class: "research_estimate",
    availability: { status: "available", reason: null },
    selected_block_number: 18000000,
    selected_block_hash: "0x" + "c".repeat(64),
    selected_block_timestamp: "2023-08-17T00:00:00Z",
    state_age_seconds: 12,
    foundry_verified: true,
    gas_used: 180000 + index,
    receipt_sha256: "d".repeat(62) + suffix,
    trace_sha256: "e".repeat(62) + suffix,
    executor_model: "prefunded_predeployed_preapproved",
    policy_net_edge_usd: index === 0 ? "3.25" : `-${index}.25`,
    research_net_edge_usd: index === 0 ? "3.25" : `-${index}.25`,
    net_edge_usd: index === 0 ? "3.25" : `-${index}.25`,
    baseline_net_edge_usd: index === 0 ? "3.25" : `-${index}.25`,
    stress_25_net_edge_usd: index === 0 ? "1.10" : `-${index}.50`,
    stress_50_net_edge_usd: index === 0 ? "0.25" : `-${index}.75`,
    stress_robust: index === 0,
  };
});
const historicalPayload = {
  availability: { status: "available", reason: null },
  metadata: {
    contract_version: "opportunity_historical_summary/v1",
    temporal_scope: "historical_replay",
    execution_claim: "historical_counterfactual_state_override_next_block",
    data_generation: historicalGeneration,
    replay_id: historicalReplayId,
    selected_block_number: 18000000,
    coverage: {
      route_count: 2,
      scenario_count: 10,
      returned_count: 10,
      foundry_verified_count: 10,
      research_estimate_count: 10,
      positive_count: 1,
      strict_count: 0,
      executable_count: 0,
      attested_count: 0,
      unavailable_count: 0,
    },
  },
  freshness: {
    applicable: false,
    reason_code: "historical_replay",
    next_deadline: null,
  },
  filters: {
    token: null, venue: null, notional_usd: null, opportunity_class: "all",
    route_type: "all", availability: "all", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: historicalRows,
};
"""


DEMO_PAYLOAD_FIXTURE = r"""
const demoPayload = JSON.parse(JSON.stringify(historicalPayload));
Object.assign(demoPayload.metadata, {
  contract_version: "opportunity_historical_demo_summary/v1",
  demo_fixture: true,
  evidence_mode: "offline_test_fixture",
  verification_status: "structurally_validated",
  validation_boundary: "spawned_local_process",
  temporal_scope: "historical_demo_fixture",
  execution_claim: "synthetic_fixture_no_execution",
  execution_status: "not_run",
  simulation_basis: "deterministic_repository_fixture",
  reference_kind: "synthetic_block_coordinate",
});
demoPayload.metadata.coverage.foundry_verified_count = 0;
demoPayload.metadata.coverage.positive_count = 0;
demoPayload.freshness.reason_code = "historical_demo_fixture";
demoPayload.routes.forEach((route) => {
  route.route_mode = "synthetic_fixture_no_execution";
  route.foundry_verified = false;
  route.executor_model = "not_run";
  route.gas_assumption = route.gas_used;
  delete route.gas_used;
  route.stress_robust = false;
});
Object.assign(demoPayload.routes[0], {
  policy_net_edge_usd: "-0.25",
  research_net_edge_usd: "-0.25",
  net_edge_usd: "-0.25",
  baseline_net_edge_usd: "-0.25",
  stress_25_net_edge_usd: "-1.1",
  stress_50_net_edge_usd: "-2.5",
});
"""


class HistoricalOpportunityShellTests(unittest.TestCase):
    def test_node_harnesses_do_not_pin_a_private_tmp_worktree(self):
        harness = CURRENT_FRONTEND_TEST_PATH.read_text(encoding="utf-8")

        self.assertNotIn("/private/tmp/", harness)
        self.assertIn("NAVIGATION_PATH", harness)

    def test_shell_has_scoped_control_context_and_release_probe_hooks(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        view_start = index.index('id="opportunities-view"')
        view_end = index.index('id="screener-view"', view_start)
        page = index[view_start:view_end]

        self.assertIn('data-opportunity-scope="current"', page)
        self.assertIn('data-opportunity-scope="historical"', page)
        self.assertIn('id="opportunity-current-context"', page)
        self.assertIn('id="opportunity-historical-context"', page)
        self.assertIn('id="historical-opportunity-demo-label"', page)
        self.assertIn("LOCAL DEMO FIXTURE", page)
        self.assertIn('id="historical-opportunity-inventory"', page)
        self.assertIn('id="historical-opportunity-body"', page)
        self.assertIn('id="historical-opportunity-count"', page)
        self.assertIn(HISTORICAL_DISCLAIMER, " ".join(page.split()))
        self.assertIn("Historical / Fixture Scenarios", page)
        self.assertIn("Block / fixture reference", page)
        self.assertIn("Scenario result", page)
        self.assertIn("State-age input", page)
        self.assertIn("Evidence record", page)
        self.assertNotIn("under a hash-bound state override model", page)
        self.assertNotIn("Foundry-verified", page)
        for attribute in (
            "data-api-generation",
            "data-replay-id",
            "data-scenario-count",
            "data-selected-block-number",
        ):
            self.assertIn(attribute, page)

    def test_historical_route_identity_wraps_before_hiding_value_columns(self):
        page = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")
        route_rule = re.search(
            r"\.opportunity-table td:nth-child\(2\)\s*\{([^}]+)\}",
            styles,
        )

        self.assertIn(
            'class="opportunity-table historical-opportunity-table"',
            page,
        )
        self.assertIsNotNone(route_rule)
        self.assertIn("max-width:", route_rule.group(1))
        self.assertIn("white-space: normal", route_rule.group(1))
        self.assertIn("overflow-wrap: anywhere", route_rule.group(1))


class HistoricalOpportunityRendererTests(unittest.TestCase):
    def test_local_demo_fragment_labels_historical_scope_and_survives_filters(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "127.0.0.1",
};
syncOpportunityScopePresentation("historical");
const historical = {
  labelHidden: opportunityElements["historical-opportunity-demo-label"].hidden,
  dataActionsHidden: opportunityElements["data-actions-nav"].hidden,
  preservedPath: preserveHistoricalDemoFragment(
    "/opportunities?opportunity_scope=historical&notional=1000",
  ),
};
syncOpportunityScopePresentation("current");
const current = {
  labelHidden: opportunityElements["historical-opportunity-demo-label"].hidden,
};
global.window.location.hash = "";
syncOpportunityScopePresentation("historical");
const ordinaryHistorical = {
  labelHidden: opportunityElements["historical-opportunity-demo-label"].hidden,
};
console.log(JSON.stringify({ historical, current, ordinaryHistorical }));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "historical": {
                "labelHidden": False,
                "dataActionsHidden": True,
                "preservedPath": (
                    "/opportunities?opportunity_scope=historical&notional=1000"
                    "#local-demo-fixture"
                ),
            },
            "current": {"labelHidden": True},
            "ordinaryHistorical": {"labelHidden": True},
        })

    def test_filter_form_navigation_retains_local_demo_fragment(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "127.0.0.1",
};
let pushedPath = null;
let routeApplicationCount = 0;
global.window.history = {
  pushState(_state, _title, path) { pushedPath = path; },
  replaceState(_state, _title, path) { pushedPath = path; },
};
global.window.scrollTo = () => {};
applyRouteFromLocation = () => { routeApplicationCount += 1; };
app.route = {
  kind: "opportunities",
  filters: { opportunityScope: "historical" },
};
opportunityElements["opportunity-notional"].value = "1000";
bindOpportunityFilterEvents();
opportunityElements["opportunity-filter-form"].listeners.submit({
  preventDefault() {},
});
console.log(JSON.stringify({ pushedPath, routeApplicationCount }));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result["routeApplicationCount"], 1)
        self.assertIn("notional=1000", result["pushedPath"])
        self.assertIn("opportunity_scope=historical", result["pushedPath"])
        self.assertTrue(result["pushedPath"].endswith("#local-demo-fixture"))

    def test_demo_fragment_is_ignored_outside_an_exact_loopback_origin(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "production.example",
};
syncOpportunityScopePresentation("historical");
console.log(JSON.stringify({
  labelHidden: opportunityElements["historical-opportunity-demo-label"].hidden,
  dataActionsHidden: opportunityElements["data-actions-nav"].hidden,
  preservedPath: preserveHistoricalDemoFragment(
    "/opportunities?opportunity_scope=historical&notional=1000",
  ),
}));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "labelHidden": True,
            "dataActionsHidden": False,
            "preservedPath": (
                "/opportunities?opportunity_scope=historical&notional=1000"
            ),
        })

    def test_structural_demo_evidence_is_only_accepted_and_labelled_on_loopback(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + DEMO_PAYLOAD_FIXTURE
            + r"""
const requested = normalizedOpportunityFilters({
  opportunityScope: "historical",
});

global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "127.0.0.1",
};
const loopbackAccepted = historicalOpportunityResponseMatchesRequest(
  demoPayload, requested,
);
const productionAcceptedOnDemoLocation = historicalOpportunityResponseMatchesRequest(
  historicalPayload, requested,
);
const loopbackMarkup = historicalOpportunityRowMarkup(
  demoPayload.routes[0], historicalGeneration, historicalReplayId, true,
);
const semanticMutations = {
  temporalScope(payload) { payload.metadata.temporal_scope = "historical_replay"; },
  executionClaim(payload) {
    payload.metadata.execution_claim = "historical_counterfactual_state_override_next_block";
  },
  executionStatus(payload) { payload.metadata.execution_status = "verified"; },
  simulationBasis(payload) { payload.metadata.simulation_basis = "anvil_state_override"; },
  referenceKind(payload) { payload.metadata.reference_kind = "ethereum_block"; },
  validationBoundary(payload) { payload.metadata.validation_boundary = "same_process"; },
  routeMode(payload) {
    payload.routes[0].route_mode = "historical_counterfactual_state_override_next_block";
  },
  executorModel(payload) {
    payload.routes[0].executor_model = "prefunded_predeployed_preapproved";
  },
  gasField(payload) {
    payload.routes[0].gas_used = payload.routes[0].gas_assumption;
    delete payload.routes[0].gas_assumption;
  },
};
const semanticMutationAccepted = {};
for (const [name, mutate] of Object.entries(semanticMutations)) {
  const payload = JSON.parse(JSON.stringify(demoPayload));
  mutate(payload);
  semanticMutationAccepted[name] = historicalOpportunityResponseMatchesRequest(
    payload, requested,
  );
}

global.window.location.hostname = "production.example";
const remoteAccepted = historicalOpportunityResponseMatchesRequest(
  demoPayload, requested,
);
const productionAcceptedRemotely = historicalOpportunityResponseMatchesRequest(
  historicalPayload, requested,
);
const mislabeledProduction = JSON.parse(JSON.stringify(historicalPayload));
mislabeledProduction.metadata.evidence_mode = "offline_test_fixture";
mislabeledProduction.metadata.verification_status = "structurally_validated";
const mislabeledProductionAccepted = historicalOpportunityResponseMatchesRequest(
  mislabeledProduction, requested,
);
console.log(JSON.stringify({
  loopbackAccepted,
  loopbackMarkup,
  mislabeledProductionAccepted,
  productionAcceptedOnDemoLocation,
  productionAcceptedRemotely,
  remoteAccepted,
  semanticMutationAccepted,
}));
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["loopbackAccepted"])
        self.assertFalse(result["remoteAccepted"])
        self.assertFalse(result["mislabeledProductionAccepted"])
        self.assertFalse(result["productionAcceptedOnDemoLocation"])
        self.assertTrue(result["productionAcceptedRemotely"])
        self.assertEqual(result["semanticMutationAccepted"], {
            "temporalScope": False,
            "executionClaim": False,
            "executionStatus": False,
            "simulationBasis": False,
            "referenceKind": False,
            "validationBoundary": False,
            "routeMode": False,
            "executorModel": False,
            "gasField": False,
        })
        self.assertIn(
            "Synthetic fixture evidence — execution and verification not run",
            result["loopbackMarkup"],
        )
        self.assertIn("Fixture gas assumption", result["loopbackMarkup"])
        self.assertIn("Receipt-record digest", result["loopbackMarkup"])
        self.assertIn("Workflow-trace digest", result["loopbackMarkup"])
        self.assertIn("Fixture reference", result["loopbackMarkup"])
        self.assertIn("Fixture model result", result["loopbackMarkup"])
        self.assertIn("Fixture state-age input", result["loopbackMarkup"])
        self.assertIn("Execution not run", result["loopbackMarkup"])
        self.assertIn('data-foundry-verified="false"', result["loopbackMarkup"])
        self.assertNotIn("Foundry verified", result["loopbackMarkup"])

    def test_demo_success_resolves_global_loading_with_fixture_disclosure(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + DEMO_PAYLOAD_FIXTURE
            + r"""
global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "127.0.0.1",
};
global.fetch = async () => ({
  ok: true,
  status: 200,
  async json() { return demoPayload; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities",
    filters: { opportunityScope: "historical" },
  });
  console.log(JSON.stringify({
    loaded,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "loaded": True,
            "freshness": (
                "LOCAL DEMO FIXTURE · Foundry verification not run · "
                "fixture reference 18000000"
            ),
            "freshnessStatus": "partial",
        })

    def test_repository_demo_payload_is_accepted_and_rendered_on_loopback(self):
        with HistoricalOpportunityDemoFixture() as fixture:
            payload = fixture.build_payload()

        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + "\nconst actualDemoPayload = "
            + json.dumps(payload, separators=(",", ":"), sort_keys=True)
            + r""";
global.window.location = {
  pathname: "/opportunities",
  search: "?opportunity_scope=historical",
  hash: "#local-demo-fixture",
  hostname: "127.0.0.1",
};
global.fetch = async () => ({
  ok: true,
  status: 200,
  async json() { return actualDemoPayload; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities",
    filters: { opportunityScope: "historical" },
  });
  const body = opportunityElements["historical-opportunity-body"].innerHTML;
  console.log(JSON.stringify({
    loaded,
    rowCount: (body.match(/<tr /g) || []).length,
    body,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["loaded"])
        self.assertEqual(result["rowCount"], 10)
        self.assertIn("Fixture reference", result["body"])
        self.assertIn("Fixture model result", result["body"])
        self.assertIn("Execution not run", result["body"])
        self.assertNotIn("Foundry verified", result["body"])
        self.assertEqual(
            result["freshness"],
            "LOCAL DEMO FIXTURE · Foundry verification not run · fixture reference 18000000",
        )
        self.assertEqual(result["freshnessStatus"], "partial")

    def test_leaving_historical_for_screener_restores_market_freshness(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
renderHistoricalOpportunities(historicalPayload);
app.payload = {
  metadata: {
    freshness: {
      cex_daily: { available_end: "2026-08-31" },
      dex_daily: { available_end: "2026-08-30" },
      common_comparable_end: "2026-08-30",
      overall_status: "partial",
    },
  },
};
hydrateScreenerControls = () => ({ start: "", end: "" });
syncTimeWindowControls = () => {};
renderTable = () => {};
syncMarketPayloadForWindow = () => {};
applyScreenerRoute({ kind: "screener", filters: {} });
console.log(JSON.stringify({
  freshness: opportunityElements["freshness"].textContent,
  freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
}));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "freshness": "CEX 2026-08-31 · DEX 2026-08-30 · common 2026-08-30",
            "freshnessStatus": "partial",
        })

    def test_historical_decimal_strings_render_as_visible_currency_values(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
const positive = historicalOpportunityRowMarkup(
  historicalRows[0], historicalGeneration, historicalReplayId,
);
const negative = historicalOpportunityRowMarkup(
  historicalRows[1], historicalGeneration, historicalReplayId,
);
console.log(JSON.stringify({ positive, negative }));
""",
            prelude=navigation_prelude(),
        )

        self.assertIn(
            '<td data-label="Notional">$1,000</td>', result["positive"]
        )
        self.assertIn(
            '<td data-label="Net result at replay block"><strong>$3.25</strong>',
            result["positive"],
        )
        self.assertIn("Policy baseline $3.25", result["positive"])
        self.assertIn("Stress 25 bps $1.1", result["positive"])
        self.assertIn(
            '<td data-label="Notional">$5,000</td>', result["negative"]
        )
        self.assertIn(
            '<td data-label="Net result at replay block"><strong>$-1.25</strong>',
            result["negative"],
        )
        self.assertNotIn('data-label="Notional">N/A', result["positive"])
        self.assertNotIn(
            'data-label="Net result at replay block"><strong>N/A',
            result["negative"],
        )

    def test_scope_switch_hides_and_clears_the_losing_scope_before_fetch_finishes(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
function unresolvedFetch() { return new Promise(() => {}); }
renderOpportunities(opportunityPayload);
global.fetch = unresolvedFetch;
applyOpportunitiesRoute({
  kind: "opportunities", filters: { opportunityScope: "historical" },
});
const enteringHistorical = {
  currentSectionsHidden: ["strict", "estimate", "unavailable"].every(
    (name) => opportunityElements[`${name}-opportunities`].hidden,
  ),
  currentRows: ["strict", "estimate", "unavailable"].map(
    (name) => opportunityElements[`${name}-opportunity-body`].innerHTML,
  ),
  currentContextHidden: opportunityElements["opportunity-current-context"].hidden,
  historicalContextHidden: opportunityElements["opportunity-historical-context"].hidden,
};
renderHistoricalOpportunities(historicalPayload);
global.fetch = unresolvedFetch;
applyOpportunitiesRoute({ kind: "opportunities", filters: {} });
const enteringCurrent = {
  historicalHidden: opportunityElements["historical-opportunity-inventory"].hidden,
  historicalRows: opportunityElements["historical-opportunity-body"].innerHTML,
  generation: opportunityElements["historical-opportunity-inventory"]
    .dataset.apiGeneration || "",
  currentContextHidden: opportunityElements["opportunity-current-context"].hidden,
  historicalContextHidden: opportunityElements["opportunity-historical-context"].hidden,
  freshness: opportunityElements["freshness"].textContent,
  freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
};
renderOpportunities(opportunityPayload);
const currentResolved = {
  freshness: opportunityElements["freshness"].textContent,
  freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
};
console.log(JSON.stringify({ enteringHistorical, enteringCurrent, currentResolved }));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result["enteringHistorical"], {
            "currentSectionsHidden": True,
            "currentRows": ["", "", ""],
            "currentContextHidden": True,
            "historicalContextHidden": False,
        })
        self.assertEqual(result["enteringCurrent"], {
            "historicalHidden": True,
            "historicalRows": "",
            "generation": "",
            "currentContextHidden": False,
            "historicalContextHidden": True,
            "freshness": "Loading current opportunities",
            "freshnessStatus": "loading",
        })
        self.assertEqual(result["currentResolved"], {
            "freshness": "Current opportunities checked 2026-08-01 00:00:30 UTC",
            "freshnessStatus": "partial",
        })

    def test_current_unavailable_resolves_global_loading_state(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + r"""
const unavailablePayload = JSON.parse(JSON.stringify(opportunityPayload));
unavailablePayload.availability = {
  status: "unavailable", reason: "complete_pointer_absent",
};
unavailablePayload.routes = [];
unavailablePayload.metadata.coverage.returned_count = 0;
global.fetch = async () => ({
  ok: true,
  status: 200,
  async json() { return unavailablePayload; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: { notionalUsd: 10000 },
  });
  console.log(JSON.stringify({
    loaded,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "loaded": True,
            "freshness": "Current opportunities unavailable",
            "freshnessStatus": "unavailable",
        })

    def test_current_api_error_resolves_global_loading_state(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
global.fetch = async () => ({
  ok: false,
  status: 503,
  async json() { return { error: "Current opportunity service unavailable." }; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: {},
  });
  console.log(JSON.stringify({
    loaded,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "loaded": False,
            "freshness": "Current opportunities failed to load",
            "freshnessStatus": "unavailable",
        })

    def test_historical_scope_fetches_isolated_api_and_renders_all_audit_rows(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
const requests = [];
global.fetch = async (url) => {
  requests.push(url);
  return {
    ok: true,
    status: 200,
    async json() { return historicalPayload; },
  };
};
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities",
    filters: { opportunityScope: "historical" },
  });
  const root = opportunityElements["historical-opportunity-inventory"];
  const body = opportunityElements["historical-opportunity-body"].innerHTML;
  console.log(JSON.stringify({
    loaded,
    requests,
    rootHidden: root.hidden,
    strictHidden: opportunityElements["strict-opportunities"].hidden,
    estimateHidden: opportunityElements["estimate-opportunities"].hidden,
    unavailableHidden: opportunityElements["unavailable-opportunities"].hidden,
    currentContextHidden: opportunityElements["opportunity-current-context"].hidden,
    historicalContextHidden: opportunityElements["opportunity-historical-context"].hidden,
    rootDataset: root.dataset,
    count: opportunityElements["historical-opportunity-count"].textContent,
    rowCount: (body.match(/<tr /g) || []).length,
    body,
    scopes: opportunityScopeButtons.map((button) => ({
      scope: button.dataset.opportunityScope,
      pressed: button.attributes["aria-pressed"],
      active: button.classList.contains("active"),
    })),
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["loaded"])
        self.assertEqual(
            result["requests"],
            [
                "/api/markets/opportunities/historical?class=all&route_type=all"
                "&availability=all&sort=net_edge_usd&dir=desc"
            ],
        )
        self.assertFalse(result["rootHidden"])
        self.assertTrue(result["strictHidden"])
        self.assertTrue(result["estimateHidden"])
        self.assertTrue(result["unavailableHidden"])
        self.assertTrue(result["currentContextHidden"])
        self.assertFalse(result["historicalContextHidden"])
        self.assertEqual(result["rootDataset"], {
            "apiGeneration": "a" * 64,
            "replayId": "replay:" + "b" * 64,
            "scenarioCount": "10",
            "selectedBlockNumber": "18000000",
        })
        self.assertEqual(result["count"], "10 scenarios")
        self.assertEqual(result["rowCount"], 10)
        self.assertEqual(
            result["freshness"], "Historical replay block 18000000"
        )
        self.assertEqual(result["freshnessStatus"], "partial")
        self.assertEqual(
            result["scopes"],
            [
                {"scope": "current", "pressed": "false", "active": False},
                {"scope": "historical", "pressed": "true", "active": True},
            ],
        )
        for value in (
            'data-opportunity-id="historical:00"',
            'data-api-generation="' + "a" * 64 + '"',
            'data-replay-id="replay:' + "b" * 64 + '"',
            'data-block-number="18000000"',
            'data-direction="uniswap_to_sushiswap"',
            'data-notional-usd="1000"',
            'data-foundry-verified="true"',
            'data-policy-net-edge-usd="3.25"',
            'data-research-net-edge-usd="3.25"',
            'data-receipt-sha256="' + "d" * 62 + '00"',
            'data-trace-sha256="' + "e" * 62 + '00"',
            "Foundry verified",
            "Gas 180,000",
            "Stress 25 bps",
            "Stress 50 bps",
        ):
            self.assertIn(value, result["body"])
        self.assertIn("-1.25", result["body"])
        row_tags = re.findall(r"<tr ([^>]*)>", result["body"])
        row_attributes = [dict(re.findall(r'data-([^=]+)="([^"]*)"', tag)) for tag in row_tags]
        self.assertEqual(
            {row["opportunity-id"] for row in row_attributes},
            {f"historical:{index:02d}" for index in range(10)},
        )
        self.assertEqual(
            {row["direction"] for row in row_attributes},
            {"uniswap_to_sushiswap", "sushiswap_to_uniswap"},
        )
        self.assertEqual(
            {row["notional-usd"] for row in row_attributes},
            {"1000", "5000", "10000", "50000", "100000"},
        )
        self.assertTrue(any(not row["research-net-edge-usd"].startswith("-") for row in row_attributes))
        self.assertTrue(any(row["research-net-edge-usd"].startswith("-") for row in row_attributes))
        self.assertNotIn("Strict executable candidate", result["body"])
        self.assertNotIn("Current", result["body"])

    def test_invalid_historical_generation_clears_previously_rendered_rows(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
let responseCount = 0;
global.fetch = async () => {
  responseCount += 1;
  const payload = JSON.parse(JSON.stringify(historicalPayload));
  if (responseCount === 2) payload.metadata.data_generation = "A".repeat(64);
  return { ok: true, status: 200, async json() { return payload; } };
};
(async () => {
  const route = {
    kind: "opportunities",
    filters: { opportunityScope: "historical" },
  };
  const first = await applyOpportunitiesRoute(route);
  const firstRows = (opportunityElements["historical-opportunity-body"]
    .innerHTML.match(/<tr /g) || []).length;
  const second = await loadOpportunities(route.filters);
  const root = opportunityElements["historical-opportunity-inventory"];
  console.log(JSON.stringify({
    first,
    second,
    firstRows,
    finalRows: (opportunityElements["historical-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
    rootHidden: root.hidden,
    generation: root.dataset.apiGeneration || "",
    error: opportunityElements["opportunity-error"].textContent,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["first"])
        self.assertFalse(result["second"])
        self.assertEqual(result["firstRows"], 10)
        self.assertEqual(result["finalRows"], 0)
        self.assertTrue(result["rootHidden"])
        self.assertEqual(result["generation"], "")
        self.assertIn("request-bound payload contract", result["error"])

    def test_filtered_historical_response_keeps_full_scenario_denominator(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
const filteredPayload = JSON.parse(JSON.stringify(historicalPayload));
filteredPayload.routes = filteredPayload.routes.slice(0, 2);
filteredPayload.metadata.coverage.returned_count = 2;
filteredPayload.filters.token = "UNI";
global.fetch = async (url) => ({
  ok: true,
  status: 200,
  async json() { return filteredPayload; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities",
    filters: { opportunityScope: "historical", token: "UNI" },
  });
  const root = opportunityElements["historical-opportunity-inventory"];
  console.log(JSON.stringify({
    loaded,
    scenarioCount: root.dataset.scenarioCount,
    rows: (opportunityElements["historical-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "loaded": True,
            "scenarioCount": "10",
            "rows": 2,
        })

    def test_absent_historical_pointer_is_normal_unavailable_not_invalid(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
const unavailablePayload = {
  availability: { status: "unavailable", reason: "historical_replay_pointer_absent" },
  metadata: {
    contract_version: "opportunity_historical_summary/v1",
    data_generation: null,
    coverage: {
      route_count: 0, scenario_count: 0, returned_count: 0,
      foundry_verified_count: 0, research_estimate_count: 0,
      positive_count: 0, strict_count: 0, executable_count: 0,
      attested_count: 0, unavailable_count: 0,
    },
  },
  filters: {
    token: null, venue: null, notional_usd: null, opportunity_class: "all",
    route_type: "all", availability: "all", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: [],
};
global.fetch = async () => ({
  ok: true, status: 200, async json() { return unavailablePayload; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: { opportunityScope: "historical" },
  });
  console.log(JSON.stringify({
    loaded,
    unavailable: opportunityElements["opportunity-bundle-unavailable"].textContent,
    unavailableHidden: opportunityElements["opportunity-bundle-unavailable"].hidden,
    error: opportunityElements["opportunity-error"].textContent,
    errorHidden: opportunityElements["opportunity-error"].hidden,
    badge: opportunityElements["opportunity-cohort-status"].textContent,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["loaded"])
        self.assertFalse(result["unavailableHidden"])
        self.assertIn("No historical replay has been published yet", result["unavailable"])
        self.assertTrue(result["errorHidden"])
        self.assertNotIn("invalid", result["badge"].lower())
        self.assertEqual(result["freshness"], "Historical replay unavailable")
        self.assertEqual(result["freshnessStatus"], "unavailable")

    def test_historical_api_error_resolves_global_loading_state(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
global.fetch = async () => ({
  ok: false,
  status: 503,
  async json() { return { error: "Historical replay service unavailable." }; },
});
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: { opportunityScope: "historical" },
  });
  console.log(JSON.stringify({
    loaded,
    error: opportunityElements["opportunity-error"].textContent,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertFalse(result["loaded"])
        self.assertEqual(result["error"], "Historical replay service unavailable.")
        self.assertEqual(result["freshness"], "Historical replay failed to load")
        self.assertEqual(result["freshnessStatus"], "unavailable")

    def test_invalid_historical_filters_resolve_global_loading_state(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities",
    filters: { opportunityScope: "historical", token: "bad token" },
    validationErrors: [{ field: "token", reason: "invalid" }],
  });
  console.log(JSON.stringify({
    loaded,
    freshness: opportunityElements["freshness"].textContent,
    freshnessStatus: opportunityElements["freshness-cluster"].dataset.status,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertFalse(result["loaded"])
        self.assertEqual(result["freshness"], "Historical replay filters invalid")
        self.assertEqual(result["freshnessStatus"], "unavailable")

    def test_other_historical_unavailable_reasons_fail_closed(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
const payload = {
  availability: { status: "unavailable", reason: "complete_pointer_absent" },
  metadata: {
    contract_version: "opportunity_historical_summary/v1",
    data_generation: null,
    coverage: {
      route_count: 0, scenario_count: 0, returned_count: 0,
      foundry_verified_count: 0, research_estimate_count: 0,
      positive_count: 0, strict_count: 0, executable_count: 0,
      attested_count: 0, unavailable_count: 0,
    },
  },
  filters: {
    token: null, venue: null, notional_usd: null, opportunity_class: "all",
    route_type: "all", availability: "all", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: [],
};
global.fetch = async () => ({ ok: true, status: 200, async json() { return payload; } });
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: { opportunityScope: "historical" },
  });
  console.log(JSON.stringify({
    loaded,
    error: opportunityElements["opportunity-error"].textContent,
    badge: opportunityElements["opportunity-cohort-status"].textContent,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertFalse(result["loaded"])
        self.assertIn("request-bound payload contract", result["error"])
        self.assertEqual(result["badge"], "Bundle invalid")

    def test_absent_historical_pointer_rejects_availability_extra_fields(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + r"""
const payload = {
  availability: {
    status: "unavailable", reason: "historical_replay_pointer_absent",
    stale: false,
  },
  metadata: {
    contract_version: "opportunity_historical_summary/v1",
    data_generation: null,
    coverage: {
      route_count: 0, scenario_count: 0, returned_count: 0,
      foundry_verified_count: 0, research_estimate_count: 0,
      positive_count: 0, strict_count: 0, executable_count: 0,
      attested_count: 0, unavailable_count: 0,
    },
  },
  filters: {
    token: null, venue: null, notional_usd: null, opportunity_class: "all",
    route_type: "all", availability: "all", sort: "net_edge_usd",
    direction: "desc",
  },
  routes: [],
};
global.fetch = async () => ({ ok: true, status: 200, async json() { return payload; } });
(async () => {
  const loaded = await applyOpportunitiesRoute({
    kind: "opportunities", filters: { opportunityScope: "historical" },
  });
  console.log(JSON.stringify({
    loaded,
    error: opportunityElements["opportunity-error"].textContent,
    badge: opportunityElements["opportunity-cohort-status"].textContent,
  }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertFalse(result["loaded"])
        self.assertIn("request-bound payload contract", result["error"])
        self.assertEqual(result["badge"], "Bundle invalid")

    def test_historical_rows_must_match_every_filter_and_response_order(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
const requested = normalizedOpportunityFilters({
  opportunityScope: "historical", token: "UNI", venue: "uniswap_v2",
  notionalUsd: "1000", opportunityClass: "estimate", routeType: "dex_dex",
  availability: "available", sort: "net_edge_usd", dir: "desc",
});
function candidate() {
  const payload = JSON.parse(JSON.stringify(historicalPayload));
  payload.routes = [payload.routes[0]];
  payload.metadata.coverage.returned_count = 1;
  payload.filters = {
    token: "UNI", venue: "uniswap_v2", notional_usd: "1000",
    opportunity_class: "estimate", route_type: "dex_dex",
    availability: "available", sort: "net_edge_usd",
    direction: "desc",
  };
  return payload;
}
const mutations = {
  token(payload) { payload.routes[0].token_symbol = "AAVE"; },
  venue(payload) {
    payload.routes[0].buy_market_id = "dex:ethereum:curve:pool:UNI-WETH";
    payload.routes[0].sell_market_id = "dex:ethereum:curve:pool:UNI-WETH";
  },
  notional(payload) { payload.routes[0].requested_notional_usd = "5000"; },
  opportunityClass(payload) { payload.routes[0].opportunity_class = "executable_candidate"; },
  routeType(payload) { payload.routes[0].route_type = "cex_dex"; },
  availability(payload) { payload.routes[0].availability.status = "unavailable"; },
};
const mismatches = {};
for (const [name, mutate] of Object.entries(mutations)) {
  const payload = candidate();
  mutate(payload);
  mismatches[name] = historicalOpportunityResponseMatchesRequest(payload, requested);
}
const descendingPayload = JSON.parse(JSON.stringify(historicalPayload));
descendingPayload.filters.sort = "net_edge_usd";
descendingPayload.filters.direction = "desc";
descendingPayload.routes = [descendingPayload.routes[1], descendingPayload.routes[0]];
descendingPayload.metadata.coverage.returned_count = 2;
const descendingFilters = normalizedOpportunityFilters({
  opportunityScope: "historical", sort: "net_edge_usd", dir: "desc",
});
const ascendingPayload = JSON.parse(JSON.stringify(historicalPayload));
ascendingPayload.filters.sort = "net_edge_usd";
ascendingPayload.filters.direction = "asc";
ascendingPayload.routes = [ascendingPayload.routes[0], ascendingPayload.routes[1]];
ascendingPayload.metadata.coverage.returned_count = 2;
const ascendingFilters = normalizedOpportunityFilters({
  opportunityScope: "historical", sort: "net_edge_usd", dir: "asc",
});
console.log(JSON.stringify({
  valid: historicalOpportunityResponseMatchesRequest(candidate(), requested),
  mismatches,
  wrongDescendingOrder: historicalOpportunityResponseMatchesRequest(
    descendingPayload, descendingFilters,
  ),
  wrongAscendingOrder: historicalOpportunityResponseMatchesRequest(
    ascendingPayload, ascendingFilters,
  ),
}));
""",
            prelude=navigation_prelude(),
        )

        self.assertTrue(result["valid"])
        self.assertEqual(result["mismatches"], {
            "token": False,
            "venue": False,
            "notional": False,
            "opportunityClass": False,
            "routeType": False,
            "availability": False,
        })
        self.assertFalse(result["wrongDescendingOrder"])
        self.assertFalse(result["wrongAscendingOrder"])

    def test_available_historical_payload_requires_exact_top_level_availability(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
const requested = normalizedOpportunityFilters({ opportunityScope: "historical" });
const valid = JSON.parse(JSON.stringify(historicalPayload));
const mixedState = JSON.parse(JSON.stringify(historicalPayload));
mixedState.availability = {
  status: "unavailable", reason: "historical_replay_pointer_absent",
};
const extraField = JSON.parse(JSON.stringify(historicalPayload));
extraField.availability = { status: "available", reason: null, stale: false };
console.log(JSON.stringify({
  valid: historicalOpportunityResponseMatchesRequest(valid, requested),
  mixedState: historicalOpportunityResponseMatchesRequest(mixedState, requested),
  extraField: historicalOpportunityResponseMatchesRequest(extraField, requested),
}));
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result, {
            "valid": True,
            "mixedState": False,
            "extraField": False,
        })

    def test_cross_scope_late_responses_never_overwrite_the_winning_scope(self):
        result = run_app_javascript(
            HISTORICAL_DOM_FIXTURE
            + PAYLOAD_FIXTURE
            + HISTORICAL_PAYLOAD_FIXTURE
            + r"""
function response(payload) {
  return { ok: true, status: 200, async json() { return payload; } };
}
async function currentThenHistorical() {
  let releaseCurrent;
  global.fetch = async (url) => {
    if (!url.includes("/historical")) {
      return new Promise((resolve) => { releaseCurrent = () => resolve(response(opportunityPayload)); });
    }
    return response(historicalPayload);
  };
  const currentRoute = {
    kind: "opportunities",
    filters: {
      notionalUsd: 10000, opportunityClass: "all", routeType: "all",
      availability: "all", sort: "net_edge_usd", dir: "desc",
    },
  };
  const historicalRoute = {
    kind: "opportunities", filters: { opportunityScope: "historical" },
  };
  const slowCurrent = applyOpportunitiesRoute(currentRoute);
  const historical = await applyOpportunitiesRoute(historicalRoute);
  releaseCurrent();
  const current = await slowCurrent;
  return {
    historical,
    current,
    historicalRows: (opportunityElements["historical-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
    currentRows: (opportunityElements["strict-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
    rootHidden: opportunityElements["historical-opportunity-inventory"].hidden,
  };
}
async function historicalThenCurrent() {
  let releaseHistorical;
  global.fetch = async (url) => {
    if (url.includes("/historical")) {
      return new Promise((resolve) => { releaseHistorical = () => resolve(response(historicalPayload)); });
    }
    return response(opportunityPayload);
  };
  const historicalRoute = {
    kind: "opportunities", filters: { opportunityScope: "historical" },
  };
  const currentRoute = {
    kind: "opportunities",
    filters: {
      notionalUsd: 10000, opportunityClass: "all", routeType: "all",
      availability: "all", sort: "net_edge_usd", dir: "desc",
    },
  };
  const slowHistorical = applyOpportunitiesRoute(historicalRoute);
  const current = await applyOpportunitiesRoute(currentRoute);
  releaseHistorical();
  const historical = await slowHistorical;
  return {
    current,
    historical,
    historicalRows: (opportunityElements["historical-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
    currentRows: (opportunityElements["strict-opportunity-body"]
      .innerHTML.match(/<tr /g) || []).length,
    rootHidden: opportunityElements["historical-opportunity-inventory"].hidden,
  };
}
(async () => {
  const first = await currentThenHistorical();
  const second = await historicalThenCurrent();
  console.log(JSON.stringify({ first, second }));
})();
""",
            prelude=navigation_prelude(),
        )

        self.assertEqual(result["first"], {
            "historical": True,
            "current": False,
            "historicalRows": 10,
            "currentRows": 0,
            "rootHidden": False,
        })
        self.assertEqual(result["second"], {
            "current": True,
            "historical": False,
            "historicalRows": 0,
            "currentRows": 1,
            "rootHidden": True,
        })


if __name__ == "__main__":
    unittest.main()
