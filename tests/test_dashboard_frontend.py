import json
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"


def run_app_javascript(source: str):
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("Node.js is not installed in this runtime")
    script = APP_PATH.read_text(encoding="utf-8") + "\n" + source
    completed = subprocess.run(
        [node, "-e", script],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


class DashboardFrontendContractTest(unittest.TestCase):
    def test_expert_context_is_compact_but_remains_accessible(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        self.assertIn("<h3>Token Market Coverage</h3>", index)
        self.assertNotIn("Where this Token trades", index)
        self.assertIn('class="module-chip"', index)

        disclosures = index.count('<details class="context-info">')
        self.assertGreaterEqual(disclosures, 8)
        self.assertGreaterEqual(index.count('role="tooltip"'), disclosures)

        for disclosure in index.split('<details class="context-info">')[1:]:
            summary = disclosure.split("</summary>", 1)[0]
            self.assertIn("aria-label=", summary)

        # The visible copy is compact, while the full expert caveats remain
        # available through native keyboard/touch-accessible disclosures.
        self.assertIn("no values are interpolated between them", index)
        self.assertIn("A past timestamp is never promoted to occurred", index)
        self.assertIn(
            "Daily quality and coverage use the selected date window",
            index,
        )
        self.assertIn("account-specific taker fees", index)
        self.assertIn(
            ".context-info:focus-within .context-tooltip",
            styles,
        )
        self.assertIn(
            ".context-info[open] .context-tooltip",
            styles,
        )
        self.assertIn("@media (max-width: 700px)", styles)
        self.assertIn("max-height: 52vh", styles)

    def test_execution_timing_is_visible_and_distinct_from_market_state_skew(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        app_js = APP_PATH.read_text(encoding="utf-8")

        self.assertIn("A/B state-time skew", index)
        self.assertIn('id="execution-a-state-time"', index)
        self.assertIn('id="execution-a-price-time"', index)
        self.assertIn('id="execution-a-price-skew"', index)
        self.assertIn('id="execution-b-state-time"', index)
        self.assertIn("function renderExecutionTiming(slot, result)", app_js)
        self.assertIn("costs withheld; N/A is not zero", app_js)
        self.assertIn("maximum ${formatDurationSeconds(", app_js)

    def test_screener_payload_contract_rejects_legacy_full_market_shape(self):
        result = run_app_javascript(
            """
const summary = {
  metadata: {
    response_scope: "screener_summary",
    summary_version: 2,
    data_generation: "g1",
  },
  tokens: [{
    token_symbol: "AAVE",
    primary_cex: null,
    primary_dex: null,
  }],
};
const staleSummary = {
  ...summary,
  metadata: {...summary.metadata, summary_version: 1},
};
const legacy = {
  metadata: {},
  tokens: [],
  cex_markets: [],
  dex_pools: [],
};
console.log(JSON.stringify({
  summary: isMarketPayload(summary),
  staleSummary: isMarketPayload(staleSummary),
  legacy: isMarketPayload(legacy),
  missingAggregates: aggregateFacts({}, [], []),
}));
"""
        )
        self.assertTrue(result["summary"])
        self.assertFalse(result["staleSummary"])
        self.assertFalse(result["legacy"])
        self.assertEqual(
            result["missingAggregates"],
            {
                "aggregateCex": None,
                "aggregateDex": None,
                "aggregateTotal": None,
                "aggregateDexShare": None,
            },
        )

    def test_token_catalog_cache_promotes_hits_for_lru_eviction(self):
        result = run_app_javascript(
            """
app.catalogsByToken = new Map([
  ["A|2026-01-01|2026-01-02|g", { token_symbol: "A" }],
  ["B|2026-01-01|2026-01-02|g", { token_symbol: "B" }],
]);
const hit = cachedTokenCatalog("A|2026-01-01|2026-01-02|g");
console.log(JSON.stringify({
  hit: hit.token_symbol,
  order: [...app.catalogsByToken.keys()],
}));
"""
        )
        self.assertEqual(result["hit"], "A")
        self.assertEqual(
            result["order"],
            [
                "B|2026-01-01|2026-01-02|g",
                "A|2026-01-01|2026-01-02|g",
            ],
        )

    def test_default_summary_cache_is_invalidated_across_data_generations(self):
        result = run_app_javascript(
            """
app.defaultPayload = { metadata: { data_generation: "g1" } };
app.defaultPayloadIsCached = false;
clearDefaultMarketCache();
console.log(JSON.stringify({
  payload: app.defaultPayload,
  cached: app.defaultPayloadIsCached,
}));
"""
        )
        self.assertIsNone(result["payload"])
        self.assertFalse(result["cached"])

        app_js = APP_PATH.read_text(encoding="utf-8")
        display = app_js[
            app_js.index("function displayMarket("):
            app_js.index("function setMarketLoading(")
        ]
        synchronizer = app_js[
            app_js.index("function syncMarketPayloadForWindow("):
            app_js.index("function routeTitle(")
        ]
        self.assertIn("clearDefaultMarketCache();", display)
        self.assertIn("defaultGeneration === currentGeneration", synchronizer)

    def test_token_catalog_request_is_window_scoped_and_generation_checked(self):
        result = run_app_javascript(
            """
(async () => {
  const requested = [];
  global.fetch = async (url) => {
    requested.push(url);
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          token_symbol: "AAVE",
          metadata: {
            data_generation: "g1",
            window_start: "2026-01-01",
            window_end: "2026-01-31",
          },
          markets: [{ token_symbol: "AAVE" }],
        };
      },
    };
  };
  app.payload = {
    metadata: {
      response_scope: "screener_summary",
      summary_version: 2,
      data_generation: "g1",
    },
    tokens: [],
  };
  const key = tokenCatalogCacheKey(
    "AAVE",
    "2026-01-01",
    "2026-01-31",
    "g1",
  );
  await loadTokenCatalog(
    "AAVE",
    "2026-01-01",
    "2026-01-31",
    undefined,
    key,
  );
  console.log(JSON.stringify({
    requested,
    cached: app.catalogsByToken.has(key),
  }));
})();
"""
        )
        self.assertEqual(
            result["requested"],
            ["/api/markets/catalog?token=AAVE&start=2026-01-01&end=2026-01-31"],
        )
        self.assertTrue(result["cached"])

    def test_token_catalog_generation_mismatch_fails_closed_without_caching(self):
        result = run_app_javascript(
            """
(async () => {
  global.fetch = async () => ({
    ok: true,
    status: 200,
    async json() {
      return {
        token_symbol: "AAVE",
        metadata: {
          data_generation: "g2",
          window_start: "2026-01-01",
          window_end: "2026-01-31",
        },
        markets: [{ token_symbol: "AAVE" }],
      };
    },
  });
  app.payload = {
    metadata: {
      response_scope: "screener_summary",
      summary_version: 2,
      data_generation: "g1",
    },
    tokens: [],
  };
  app.catalogsByToken = new Map([["stale", { token_symbol: "BTC" }]]);
  let errorCode = "";
  try {
    await loadTokenCatalog(
      "AAVE",
      "2026-01-01",
      "2026-01-31",
      undefined,
      "AAVE|2026-01-01|2026-01-31|g1",
    );
  } catch (error) {
    errorCode = error.code || "";
  }
  console.log(JSON.stringify({
    errorCode,
    cacheSize: app.catalogsByToken.size,
  }));
})();
"""
        )
        self.assertEqual(result["errorCode"], "data_generation_mismatch")
        self.assertEqual(result["cacheSize"], 0)

    def test_screener_quality_never_labels_missing_counts_as_healthy(self):
        result = run_app_javascript(
            """
app.payload = {
  metadata: { response_scope: "screener_summary" },
  tokens: [],
};
global.document = {
  getElementById(id) {
    return id === "sort-field" ? { value: "volume" } : null;
  },
};
app.selections = { AAVE: { cex: null, dex: null } };
const html = screenerTokenRow({
  token_symbol: "AAVE",
  primary_cex: null,
  primary_dex: null,
  market_count: 3,
  cex_market_count: null,
  dex_market_count: null,
  quality_status_counts: {},
  aggregate_cex_volume_usd: 0,
  aggregate_dex_volume_usd: 0,
  aggregate_volume_usd: 0,
  aggregate_dex_volume_share: null,
  price_spread: null,
});
console.log(JSON.stringify({ html }));
"""
        )
        self.assertIn("Catalog quality counts are incomplete", result["html"])
        self.assertIn('aria-label="N/A reason"', result["html"])
        self.assertNotIn("Healthy", result["html"])

    def test_compact_primary_markets_keep_cex_and_dex_selection_metrics(self):
        result = run_app_javascript(
            """
const tokenSummary = {
  token_symbol: "AAVE",
  primary_cex_id: "binance|AAVE/USDT",
  primary_dex_id: "0xpool",
  primary_cex: {
    market_type: "cex",
    token_symbol: "AAVE",
    venue: "binance",
    instrument: "AAVE/USDT",
    pool_address: null,
    window_return: 0,
    daily_volatility: 0.12,
    total_depth_100bps_usd: 1000,
    depth_100bps_complete: true,
    price_usd: 100,
  },
  primary_dex: {
    market_type: "dex",
    token_symbol: "AAVE",
    venue: "eth / uniswap",
    instrument: "AAVE / USDC",
    pool_address: "0xpool",
    window_return: -0.02,
    daily_volatility: 0.2,
    total_depth_100bps_usd: 500,
    depth_100bps_complete: true,
    price_usd: 101,
  },
  price_spread: 0.01,
  spread_date: "2026-07-28",
};
app.payload = {
  metadata: { response_scope: "screener_summary" },
  tokens: [tokenSummary],
};
app.selections = {};
app.selectionOverrides = {};
ensureSelections();
const selected = comparison(tokenSummary);
const noCommonDate = comparison({ ...tokenSummary, price_spread: null, spread_date: null });
console.log(JSON.stringify({
  cexId: marketId(tokenSummary.primary_cex),
  dexId: marketId(tokenSummary.primary_dex),
  selectedCex: selected.cex?.venue,
  selectedDex: selected.dex?.venue,
  spread: selected.spread,
  zeroReturn: selected.cex?.window_return,
  noCommonDateSpread: noCommonDate.spread,
}));
"""
        )
        self.assertEqual(result["cexId"], "binance|AAVE/USDT")
        self.assertEqual(result["dexId"], "0xpool")
        self.assertEqual(result["selectedCex"], "binance")
        self.assertEqual(result["selectedDex"], "eth / uniswap")
        self.assertEqual(result["spread"], 0.01)
        self.assertEqual(result["zeroReturn"], 0)
        self.assertIsNone(result["noCommonDateSpread"])

    def test_screener_sort_keeps_missing_last_and_preserves_numeric_order(self):
        result = run_app_javascript(
            """
const sortField = { value: "return" };
global.document = {
  getElementById(id) {
    return id === "sort-field" ? sortField : null;
  },
};
function token(symbol, windowReturn) {
  const instrument = `${symbol}/USDT`;
  return {
    token_symbol: symbol,
    primary_cex_id: `binance|${instrument}`,
    primary_cex: {
      market_type: "cex",
      token_symbol: symbol,
      venue: "binance",
      instrument,
      window_return: windowReturn,
    },
    primary_dex: null,
  };
}
const rows = [
  token("NEG_B", -0.2),
  token("MISSING", null),
  token("POS", 0.1),
  token("ZERO", 0),
  token("NEG_A", -0.2),
];
app.payload = {
  metadata: { response_scope: "screener_summary" },
  tokens: rows,
};
app.selections = {};
app.selectionOverrides = {};
app.scope = "cex";
ensureSelections();
app.sortDirection = "asc";
const ascending = [...rows]
  .sort(compareScreenerTokens)
  .map((row) => row.token_symbol);
app.sortDirection = "desc";
const descending = [...rows]
  .sort(compareScreenerTokens)
  .map((row) => row.token_symbol);
console.log(JSON.stringify({
  ascending,
  descending,
  zeroValue: sortValue(rows.find((row) => row.token_symbol === "ZERO")),
  missingValue: sortValue(rows.find((row) => row.token_symbol === "MISSING")),
}));
"""
        )
        self.assertEqual(
            result["ascending"],
            ["NEG_A", "NEG_B", "ZERO", "POS", "MISSING"],
        )
        self.assertEqual(
            result["descending"],
            ["POS", "ZERO", "NEG_A", "NEG_B", "MISSING"],
        )
        self.assertEqual(result["zeroValue"], 0)
        self.assertEqual(result["missingValue"], None)

    def test_sort_registry_forces_cross_spread_and_forbids_combined_returns(self):
        result = run_app_javascript(
            """
console.log(JSON.stringify({
  spread: SCREENER_SORT_DEFINITIONS.spread,
  returns: SCREENER_SORT_DEFINITIONS.return,
  volatility: SCREENER_SORT_DEFINITIONS.volatility,
  tvl: SCREENER_SORT_DEFINITIONS.dex_tvl,
}));
"""
        )
        self.assertEqual(result["spread"]["allowedScopes"], ["cross"])
        self.assertEqual(result["spread"]["defaultScope"], "cross")
        self.assertNotIn("combined", result["returns"]["allowedScopes"])
        self.assertEqual(result["returns"]["defaultScope"], "cex")
        self.assertNotIn("combined", result["volatility"]["allowedScopes"])
        self.assertEqual(result["volatility"]["defaultScope"], "cex")
        self.assertEqual(result["tvl"]["allowedScopes"], ["dex"])

    def test_cross_venue_is_metric_context_and_missing_facts_explain_recovery(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        app_js = APP_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        self.assertNotIn('data-scope="cross"', index)
        self.assertIn('id="sort-scope-fixed"', index)
        self.assertIn('value="spread_max"', index)
        self.assertIn('value="spread_mean"', index)
        self.assertIn('value="spread_median"', index)
        self.assertIn("Cross-venue · Primary CEX ↔ DEX", app_js)
        self.assertIn("function naFactMarkup(", app_js)
        self.assertIn('data-refresh-market-id=', app_js)
        self.assertIn('fetch("/api/actions/facts/refresh"', app_js)
        self.assertIn('market?.[`${fact}_retryable`] === true', app_js)
        self.assertIn(".na-disclosure-panel", styles)

    def test_market_pair_controls_share_one_aligned_grid_without_warning_column(self):
        styles = STYLES_PATH.read_text(encoding="utf-8")
        app_js = APP_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "grid-template-columns: minmax(110px, .55fr) minmax(240px, 1.5fr) minmax(240px, 1.5fr) auto",
            styles,
        )
        self.assertIn(".market-selector-shell {\n  position: relative;", styles)
        warning_rule = styles[
            styles.index(".market-warning-anchor {"):
            styles.index(".research-pair-context {")
        ]
        self.assertIn("position: absolute", warning_rule)
        self.assertNotIn("grid-template-columns", warning_rule)
        self.assertIn('data-label="±100 bps depth"', app_js)
        self.assertIn("#workspace-market-table tbody", styles)
        self.assertIn("#workspace-market-table .na-disclosure-panel", styles)

    def test_rank_value_is_rendered_and_csv_carries_sort_contract(self):
        result = run_app_javascript(
            """
const sortField = { value: "return" };
global.document = {
  getElementById(id) {
    return id === "sort-field" ? sortField : null;
  },
};
const tokenSummary = {
  token_symbol: "AAVE",
  primary_cex_id: "binance|AAVE/USDT",
  primary_cex: {
    market_type: "cex",
    token_symbol: "AAVE",
    venue: "binance",
    instrument: "AAVE/USDT",
    window_return: -0.02,
  },
  primary_dex: null,
  market_count: 1,
  cex_market_count: 1,
  dex_market_count: 0,
  quality_status_counts: { ok: 1 },
  aggregate_cex_volume_usd: 10,
  aggregate_dex_volume_usd: null,
  aggregate_volume_usd: 10,
  aggregate_dex_volume_share: null,
  price_spread: null,
};
app.payload = {
  metadata: {
    response_scope: "screener_summary",
    start_date: "2026-07-01",
    end_date: "2026-07-28",
  },
  tokens: [tokenSummary],
};
app.selections = {};
app.selectionOverrides = {};
app.scope = "cex";
app.sortDirection = "desc";
ensureSelections();
console.log(JSON.stringify({
  html: screenerTokenRow(tokenSummary),
  rankValue: formatRankValue(tokenSummary),
}));
"""
        )
        self.assertIn('data-label="Rank value"', result["html"])
        self.assertIn("-2%", result["html"])
        self.assertEqual(result["rankValue"], "-2%")

        app_js = APP_PATH.read_text(encoding="utf-8")
        export_source = app_js[
            app_js.index("function exportVisibleCsv()"):
            app_js.index("function bindEvents()")
        ]
        for field in (
            "rank_metric",
            "rank_scope",
            "rank_direction",
            "rank_value",
            "rank_eligible",
        ):
            self.assertIn(f'"{field}"', export_source)
        self.assertIn("const rankValue = sortValue(tokenSummary);", export_source)

    def test_execution_helpers_preserve_zero_and_distinguish_missing(self):
        result = run_app_javascript(
            """
const market = {
  status: "available",
  rows: [
    {
      direction: "buy_token",
      requested_notional_usd: 10000,
      quoted_execution_cost_bps: "0",
      quoted_execution_cost_usd: "0",
      fill_ratio: "1",
      status: "observed",
    },
    {
      direction: "sell_token",
      requested_notional_usd: 10000,
      quoted_execution_cost_bps: null,
      quoted_execution_cost_usd: null,
      fill_ratio: null,
      status: "unsupported",
    },
  ],
};
const observed = executionScenario(market, "buy_token", 10000);
const unsupported = executionScenario(market, "sell_token", 10000);
console.log(JSON.stringify({
  zeroNumber: decimalNumber("0"),
  missingNumber: decimalNumber(null),
  observedCost: formatExecutionCost(observed),
  observedFill: formatExecutionFill(observed),
  unsupportedCost: formatExecutionCost(unsupported),
  missingScenario: executionScenario(market, "buy_token", 50000),
}));
"""
        )
        self.assertEqual(result["zeroNumber"], 0)
        self.assertIsNone(result["missingNumber"])
        self.assertIn("0 bps", result["observedCost"])
        self.assertIn("$0", result["observedCost"])
        self.assertEqual(result["observedFill"], "100%")
        self.assertEqual(result["unsupportedCost"], "N/A")
        self.assertIsNone(result["missingScenario"])

    def test_quality_status_counts_keep_fact_states_separate(self):
        result = run_app_javascript(
            """
console.log(JSON.stringify(qualityStatusCounts({
  markets: [
    { facts: {
      daily: { status: "observed" },
      tvl: { status: "not_applicable" },
      depth: { status: "partial" },
      execution: { status: "unsupported" },
    }},
    { facts: {
      daily: { status: "observed" },
      tvl: { status: "observed" },
      depth: { status: "failed" },
      execution: { status: "unavailable" },
    }},
  ],
})));
"""
        )
        self.assertEqual(
            result,
            {
                "observed": 3,
                "not_applicable": 1,
                "partial": 1,
                "unsupported": 1,
                "failed": 1,
                "unavailable": 1,
            },
        )

    def test_source_no_observation_is_informational_not_warning(self):
        result = run_app_javascript(
            """
console.log(JSON.stringify(qualityStatusTiers({
  source_no_observation: 2,
  unsupported: 1,
  not_applicable: 1,
})));
"""
        )
        self.assertEqual(
            result,
            {
                "critical": 0,
                "pending": 0,
                "informational": 4,
            },
        )

    def test_codes_only_quality_flags_use_per_flag_severity(self):
        result = run_app_javascript(
            """
console.log(JSON.stringify(qualityFlagObjects({
  quality_flags: ["depth_unsupported", "depth_failed"],
  depth_status: "unsupported",
}, "cex").map((flag) => ({
  code: flag.code,
  severity: flag.severity,
}))));
"""
        )
        by_code = {item["code"]: item["severity"] for item in result}
        self.assertEqual(by_code["depth_unsupported"], "warning")
        self.assertEqual(by_code["depth_failed"], "critical")

    def test_html_declares_two_views_and_core_workspace_controls(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        for view in ("screener", "workspace"):
            self.assertIn(f'data-app-view="{view}"', index)
        self.assertNotIn('data-app-view="methodology"', index)
        self.assertNotIn('data-app-route="methodology"', index)
        for page in ("markets", "compare", "liquidity", "quality"):
            self.assertIn(f'data-workspace-view="{page}"', index)
        self.assertIn('id="execution-notional"', index)
        self.assertIn('data-execution-direction="buy_token"', index)
        self.assertIn('data-quality-scope="selected"', index)
        self.assertIn('id="sort-field"', index)
        self.assertIn('id="sort-direction"', index)
        self.assertIn('id="rank-value-heading"', index)
        self.assertIn('id="workspace-market-body"', index)
        self.assertIn('id="route-announcer"', index)
        self.assertIn(
            "Daily quality and coverage use the selected date window",
            index,
        )

    def test_deep_link_view_is_revealed_before_cached_or_network_data(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        initializer = app_js[
            app_js.index("async function initialize()"):
            app_js.index('if (typeof document !== "undefined") initialize();')
        ]
        self.assertIn("function primeInitialRouteView(route)", app_js)
        self.assertIn('setActiveAppView("workspace")', app_js)
        self.assertIn("setActiveWorkspacePage(route.page)", app_js)
        self.assertIn('byId("date-start").value = window.start;', app_js)
        self.assertIn('byId("date-end").value = window.end;', app_js)
        self.assertLess(
            initializer.index("primeInitialRouteView(initialRoute)"),
            initializer.index("readDefaultMarketCache()"),
        )

    def test_screener_deep_link_controls_are_hydrated_before_data_load(self):
        result = run_app_javascript(
            """
const elements = {
  "token-search": { value: "" },
  "sort-field": { value: "volume" },
  "sort-direction": { value: "desc" },
  "date-start": { value: "" },
  "date-end": { value: "" },
  "rank-value-heading": { textContent: "", title: "" },
  "time-toolbar": { hidden: true },
};
const scopeButtons = ["combined", "cross", "cex", "dex"].map((scope) => ({
  dataset: { scope },
  textContent: scope,
  disabled: false,
  active: false,
  attributes: {},
  classList: {
    toggle(name, active) {
      if (name === "active") this.owner.active = active;
    },
    owner: null,
  },
  setAttribute(name, value) {
    this.attributes[name] = value;
  },
}));
scopeButtons.forEach((button) => {
  button.classList.owner = button;
});
const appViews = [
  { dataset: { appView: "screener" }, hidden: true },
  { dataset: { appView: "workspace" }, hidden: false },
];
global.document = {
  getElementById(id) {
    return elements[id] || null;
  },
  querySelectorAll(selector) {
    if (selector === "[data-scope]") return scopeButtons;
    if (selector === "[data-app-view]") return appViews;
    return [];
  },
};
primeInitialRouteView({
  kind: "screener",
  filters: {
    q: "aave",
    sort: "return",
    scope: "dex",
    dir: "asc",
    start: "2026-07-01",
    end: "2026-07-28",
  },
});
console.log(JSON.stringify({
  searchQuery: app.searchQuery,
  tokenSearch: elements["token-search"].value,
  sort: elements["sort-field"].value,
  scope: app.scope,
  direction: app.sortDirection,
  directionControl: elements["sort-direction"].value,
  start: elements["date-start"].value,
  end: elements["date-end"].value,
  activeScope: scopeButtons.find((button) => button.active)?.dataset.scope,
  screenerVisible: !appViews[0].hidden,
  workspaceHidden: appViews[1].hidden,
  toolbarVisible: !elements["time-toolbar"].hidden,
}));
"""
        )
        self.assertEqual(result["searchQuery"], "AAVE")
        self.assertEqual(result["tokenSearch"], "aave")
        self.assertEqual(result["sort"], "return")
        self.assertEqual(result["scope"], "dex")
        self.assertEqual(result["direction"], "asc")
        self.assertEqual(result["directionControl"], "asc")
        self.assertEqual(result["start"], "2026-07-01")
        self.assertEqual(result["end"], "2026-07-28")
        self.assertEqual(result["activeScope"], "dex")
        self.assertTrue(result["screenerVisible"])
        self.assertTrue(result["workspaceHidden"])
        self.assertTrue(result["toolbarVisible"])

    def test_removed_methodology_has_no_frontend_view_or_dead_route_branches(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        app_js = APP_PATH.read_text(encoding="utf-8")
        self.assertNotIn("Methodology", index)
        self.assertNotIn('setActiveAppView("methodology")', app_js)
        self.assertNotIn('route.kind === "methodology"', app_js)
        self.assertNotIn('initialRoute.kind === "methodology"', app_js)
        for source_id in (
            "facts-contract-copy",
            "facts-source-copy",
            "source-list",
            "daily-source-status",
            "tvl-source-status",
            "depth-source-status",
            "dex-depth-source-status",
            "execution-source-status",
        ):
            self.assertIn(f'id="{source_id}"', index)

    def test_date_apply_button_is_inside_the_date_range_form(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        self.assertIn('<form id="date-window-form"', index)
        form_start = index.index('<form id="date-window-form"')
        form_end = index.index("</form>", form_start)
        form = index[form_start:form_end]
        self.assertIn('id="date-start"', form)
        self.assertIn('id="date-end"', form)
        self.assertIn('id="apply-window"', form)
        self.assertIn('type="submit"', form)

    def test_apply_pair_navigates_to_compare_after_persisting_valid_selection(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        self.assertIn("function applySelectedPair()", app_js)

        command = app_js[
            app_js.index("function applySelectedPair()"):
            app_js.index("function refreshWorkspacePageData()")
        ]
        self.assertIn("if (!persistSelectedPair())", command)
        self.assertIn("replaceCurrentRoute();", command)
        self.assertIn("refreshWorkspacePageData();", command)
        self.assertIn('navigateTo(currentWorkspacePath("compare"));', command)
        self.assertLess(
            command.index("if (!persistSelectedPair())"),
            command.index('navigateTo(currentWorkspacePath("compare"));'),
        )

        binding = app_js[
            app_js.index('byId("compare-markets").addEventListener("click"'):
            app_js.index('byId("export-csv").addEventListener("click"')
        ]
        self.assertIn(
            'byId("compare-markets").addEventListener("click", applySelectedPair);',
            binding,
        )
        self.assertNotIn("replaceCurrentRoute();", binding)
        self.assertNotIn("refreshWorkspacePageData();", binding)

    def test_date_error_is_inline_only_and_updates_input_accessibility_state(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        form_start = index.index('<form id="date-window-form"')
        form_end = index.index("</form>", form_start)
        form = index[form_start:form_end]
        self.assertEqual(form.count('aria-describedby="date-window-error"'), 2)
        self.assertEqual(form.count('aria-invalid="false"'), 2)

        app_js = APP_PATH.read_text(encoding="utf-8")
        apply_window = app_js[
            app_js.index("async function applyWindow()"):
            app_js.index("function persistSelectedPair()")
        ]
        invalid_branch = apply_window[
            apply_window.index("if (dateError)"):
            apply_window.index('showDateWindowError("");')
        ]
        self.assertIn("showDateWindowError(dateError);", invalid_branch)
        self.assertNotIn("showError(", invalid_branch)
        self.assertNotIn("clearComparisonResult(", invalid_branch)

        result = run_app_javascript(
            """
function inputControl() {
  return {
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
  };
}
const start = inputControl();
const end = inputControl();
const error = { hidden: true, textContent: "" };
global.document = {
  getElementById(id) {
    return {
      "date-start": start,
      "date-end": end,
      "date-window-error": error,
    }[id] || null;
  },
};
showDateWindowError("Choose both dates.");
const invalid = {
  start: start.attributes["aria-invalid"],
  end: end.attributes["aria-invalid"],
  hidden: error.hidden,
  message: error.textContent,
};
showDateWindowError("");
console.log(JSON.stringify({
  invalid,
  cleared: {
    start: start.attributes["aria-invalid"],
    end: end.attributes["aria-invalid"],
    hidden: error.hidden,
    message: error.textContent,
  },
}));
"""
        )
        self.assertEqual(result["invalid"]["start"], "true")
        self.assertEqual(result["invalid"]["end"], "true")
        self.assertFalse(result["invalid"]["hidden"])
        self.assertEqual(result["invalid"]["message"], "Choose both dates.")
        self.assertEqual(result["cleared"]["start"], "false")
        self.assertEqual(result["cleared"]["end"], "false")
        self.assertTrue(result["cleared"]["hidden"])
        self.assertEqual(result["cleared"]["message"], "")

    def test_monitor_toolbar_wraps_before_the_observed_overflow_width(self):
        styles = STYLES_PATH.read_text(encoding="utf-8")
        breakpoint_start = styles.index("@media (max-width: 1320px)")
        breakpoint_end = styles.index("@media (max-width: 1100px)", breakpoint_start)
        breakpoint_rule = styles[breakpoint_start:breakpoint_end]
        self.assertIn(".monitor-toolbar { flex-wrap: wrap; }", breakpoint_rule)

    def test_public_error_message_hides_server_checked_paths(self):
        result = run_app_javascript(
            """
console.log(JSON.stringify({
  hidden: publicErrorMessage(
    new Error(
      "No detailed market snapshot found. Checked: "
      + "/home/service/data/cex_markets.csv, /private/tmp/dex_pools.csv",
    ),
    "Market data is unavailable.",
  ),
  ordinary: publicErrorMessage(
    new Error("The selected date window is unavailable."),
    "Market data is unavailable.",
  ),
  fallback: publicErrorMessage(new Error(""), "Market data is unavailable."),
}));
"""
        )
        self.assertEqual(
            result["hidden"],
            "No detailed market snapshot found.",
        )
        self.assertNotIn("/home/", result["hidden"])
        self.assertNotIn("/private/", result["hidden"])
        self.assertEqual(
            result["ordinary"],
            "The selected date window is unavailable.",
        )
        self.assertEqual(result["fallback"], "Market data is unavailable.")

    def test_workspace_window_change_reloads_matching_summary_and_catalog_in_order(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        apply_window = app_js[
            app_js.index("async function applyWindow()"):
            app_js.index("function persistSelectedPair()")
        ]
        workspace_branch = apply_window[
            apply_window.index(
                'if (app.route.kind === "workspace")'
            ):
        ]
        self.assertLess(
            workspace_branch.index("replaceCurrentRoute();"),
            workspace_branch.index("await applyRouteFromLocation();"),
        )
        self.assertIn("return;", workspace_branch)
        self.assertNotIn("Promise.allSettled", apply_window)
        self.assertNotIn("loadComparison()", apply_window)
        self.assertNotIn(
            'app.route.kind === "workspace" && app.route.page === "compare"',
            apply_window,
        )

    def test_route_and_loading_contract_prevents_stale_window_or_permanent_loading(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        router = app_js[
            app_js.index("async function applyRouteFromLocation()"):
            app_js.index("function validateDateRange(")
        ]
        unavailable = app_js[
            app_js.index("function setWorkspaceDataUnavailable("):
            app_js.index("async function applyRouteFromLocation()")
        ]
        workspace_markets = app_js[
            app_js.index("function renderWorkspaceMarkets()"):
            app_js.index("function catalogQualityPayload()")
        ]
        loader = app_js[
            app_js.index("function setMarketLoading("):
            app_js.index("function setPreset(")
        ]

        self.assertIn("if (app.marketController)", router)
        self.assertIn("invalidateMarketRequest();", router)
        self.assertIn('byId("export-csv").disabled = !app.payload;', router)
        self.assertIn("const loaded = await loadMarket(", router)
        self.assertIn("!marketPayloadMatchesWindow(", router)
        self.assertEqual(router.count("compareRouteWindow(route)"), 2)
        self.assertNotIn('route.page === "compare"', router)
        self.assertGreaterEqual(router.count("setWorkspaceDataUnavailable("), 3)
        self.assertIn('setAttribute("aria-busy", "false")', unavailable)
        self.assertNotIn("Loading", unavailable)
        self.assertIn("formatRatio(row?.coverage_ratio)", workspace_markets)
        self.assertNotIn("formatRatio(market.coverage_ratio)", workspace_markets)
        self.assertIn('byId("export-csv").disabled = true;', loader)
        self.assertIn(
            'byId("date-start").value = app.payload.metadata.start_date;',
            loader,
        )

    def test_screener_drill_down_preserves_the_rendered_summary_window(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        summary_state = app_js[
            app_js.index("function currentSummaryWindowRouteState()"):
            app_js.index("function updateRouteLinks()")
        ]
        row_renderer = app_js[
            app_js.index("function screenerTokenRow("):
            app_js.index("function renderTable()")
        ]
        route_links = app_js[
            app_js.index("function updateRouteLinks()"):
            app_js.index("function replaceCurrentRoute()")
        ]

        self.assertIn("app.payload?.metadata?.start_date", summary_state)
        self.assertIn("app.payload?.metadata?.end_date", summary_state)
        self.assertIn("currentSummaryWindowRouteState()", row_renderer)
        self.assertIn("workspaceEntryRouteState", route_links)
        self.assertIn("currentSummaryWindowRouteState()", summary_state)

    def test_compare_window_preset_resolves_to_explicit_utc_dates(self):
        result = run_app_javascript(
            """
app.payload = {
  metadata: {
    available_start: "2026-01-01",
    available_end: "2026-07-28",
  },
};
console.log(JSON.stringify({
  defaultWindow: normalizedMarketWindow("", ""),
  sevenDays: compareRouteWindow({ state: { window: "7d" } }),
  all: compareRouteWindow({ state: { window: "all" } }),
  explicit: compareRouteWindow({
    state: {
      window: "7d",
      start: "2026-06-01",
      end: "2026-06-30",
    },
  }),
}));
"""
        )
        self.assertEqual(
            result["defaultWindow"],
            {"start": "2026-06-29", "end": "2026-07-28"},
        )
        self.assertEqual(
            result["sevenDays"],
            {"start": "2026-07-22", "end": "2026-07-28"},
        )
        self.assertEqual(
            result["all"],
            {"start": "2026-01-01", "end": "2026-07-28"},
        )
        self.assertEqual(
            result["explicit"],
            {"start": "2026-06-01", "end": "2026-06-30"},
        )

    def test_catalog_quality_fallback_keeps_missing_coverage_unavailable(self):
        result = run_app_javascript(
            """
global.document = {
  getElementById(id) {
    return { value: id === "facts-token" ? "AAVE" : "" };
  },
};
app.catalog = {
  metadata: {
    market_quality_thresholds: { minimum_primary_coverage_ratio: 0.8 },
  },
  markets: [{
    market_id: "cex:binance:AAVE/USDT",
    market_type: "cex",
    token_symbol: "AAVE",
    venue: "binance",
    instrument: "AAVE/USDT",
    coverage_ratio: null,
    observation_days: null,
    quality_status: "ok",
  }],
};
app.payload = { cex_markets: [], dex_pools: [] };
const fallback = catalogQualityPayload();
console.log(JSON.stringify(fallback.markets[0].facts.daily));
"""
        )
        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["observed_value"])
        self.assertEqual(
            result["message"],
            "Daily observation count is unavailable.",
        )

    def test_quality_fallback_preserves_camel_case_flag_observation(self):
        result = run_app_javascript(
            """
const qualityBody = { innerHTML: "" };
global.document = {
  getElementById(id) {
    return id === "quality-body" ? qualityBody : null;
  },
};
renderQualityPayload({
  markets: [{
    market: {
      market_id: "dex:eth:uniswap_v3:pool:AAVE",
      market_type: "dex",
      token_symbol: "AAVE",
      venue: "uniswap_v3",
      instrument: "AAVE/WETH",
    },
    facts: {},
    quality_flags: [{
      code: "tiny_pool",
      severity: "warning",
      explanation: "Pool TVL is below the declared threshold.",
      observedValue: 5000,
      threshold: 100000,
    }],
  }],
});
console.log(JSON.stringify({ html: qualityBody.innerHTML }));
"""
        )
        self.assertIn("Observed $5,000", result["html"])
        self.assertIn("minimum $100,000", result["html"])

    def test_screener_quality_origin_uses_screening_projection_everywhere(self):
        result = run_app_javascript(
            """
const qualityBody = { innerHTML: "" };
const filterSummary = { hidden: true, textContent: "", dataset: {} };
global.document = {
  getElementById(id) {
    return id === "quality-body" ? qualityBody
      : id === "quality-filter-summary" ? filterSummary : null;
  },
};
const payload = {
  markets: [{
    market_id: "cex:binance:AAVE/USDT",
    market_type: "cex",
    token_symbol: "AAVE",
    venue: "binance",
    instrument: "AAVE/USDT",
    quality_status: "ok",
    quality_flags: [],
    screening_quality_status: "warning",
    screening_quality_flags: [{
      code: "low_daily_coverage",
      severity: "warning",
      category: "data_health",
      message: "Screener-only warning reason.",
    }],
    facts: {},
  }],
};
app.qualityOrigin = "screener";
app.qualitySeverity = "warning";
renderQualityPayload(payload);
const screener = { html: qualityBody.innerHTML, summary: filterSummary.textContent };
app.qualityOrigin = "";
app.qualitySeverity = "";
renderQualityPayload(payload);
console.log(JSON.stringify({ screener, selected: qualityBody.innerHTML }));
"""
        )
        self.assertIn("Screener-only warning reason.", result["screener"]["html"])
        self.assertIn("1 warning reason", result["screener"]["summary"])
        self.assertNotIn("Screener-only warning reason.", result["selected"])

    def test_catalog_fallback_keeps_screener_projection_after_quality_request_failure(self):
        result = run_app_javascript(
            """
(async () => {
  const nodes = {
    "facts-token": { value: "AAVE" },
    "facts-market-a": { value: "" },
    "facts-market-b": { value: "" },
    "date-start": { value: "" },
    "date-end": { value: "" },
    "quality-body": { innerHTML: "" },
    "quality-filter-summary": { hidden: true, textContent: "", dataset: {} },
    "quality-error": { hidden: true, textContent: "", dataset: {} },
    "quality-status": { textContent: "", dataset: {} },
  };
  global.document = { getElementById(id) { return nodes[id] || null; } };
  app.payload = { metadata: { default_workspace_token: "AAVE" }, tokens: [] };
  app.catalog = {
    metadata: { market_quality_thresholds: { minimum_primary_coverage_ratio: 0.8 } },
    markets: [{
      market_id: "cex:binance:AAVE/USDT",
      market_type: "cex",
      token_symbol: "AAVE",
      venue: "binance",
      instrument: "AAVE/USDT",
      quality_status: "ok",
      quality_flags: [],
      screening_quality_status: "warning",
      screening_quality_flags: [{
        code: "low_daily_coverage",
        severity: "warning",
        category: "data_health",
        message: "Exact catalog screening reason.",
      }],
    }],
  };
  app.qualityScope = "all";
  app.qualityOrigin = "screener";
  app.qualitySeverity = "warning";
  renderQualityFromCatalog();
  const initial = {
    html: nodes["quality-body"].innerHTML,
    summary: nodes["quality-filter-summary"].textContent,
  };
  global.fetch = async () => { throw new Error("quality endpoint unavailable"); };
  const loaded = await loadQuality();
  console.log(JSON.stringify({
    initial,
    afterFailure: {
      html: nodes["quality-body"].innerHTML,
      summary: nodes["quality-filter-summary"].textContent,
    },
    loaded,
  }));
})();
"""
        )
        self.assertFalse(result["loaded"])
        for state in (result["initial"], result["afterFailure"]):
            self.assertIn("Exact catalog screening reason.", state["html"])
            self.assertIn("1 warning reason", state["summary"])


if __name__ == "__main__":
    unittest.main()
