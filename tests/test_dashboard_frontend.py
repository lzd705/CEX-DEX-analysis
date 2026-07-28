import json
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"


def run_app_javascript(source: str):
    script = APP_PATH.read_text(encoding="utf-8") + "\n" + source
    completed = subprocess.run(
        ["node", "-e", script],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


class DashboardFrontendContractTest(unittest.TestCase):
    def test_screener_payload_contract_rejects_legacy_full_market_shape(self):
        result = run_app_javascript(
            """
const summary = {
  metadata: {
    response_scope: "screener_summary",
    summary_version: 1,
    data_generation: "g1",
  },
  tokens: [{
    token_symbol: "AAVE",
    primary_cex: null,
    primary_dex: null,
  }],
};
const legacy = {
  metadata: {},
  tokens: [],
  cex_markets: [],
  dex_pools: [],
};
console.log(JSON.stringify({
  summary: isMarketPayload(summary),
  legacy: isMarketPayload(legacy),
  missingAggregates: aggregateFacts({}, [], []),
}));
"""
        )
        self.assertTrue(result["summary"])
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
      summary_version: 1,
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
      summary_version: 1,
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
        self.assertIn("Unavailable", result["html"])
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

    def test_html_declares_all_three_views_and_core_workspace_controls(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        for view in ("screener", "workspace", "methodology"):
            self.assertIn(f'data-app-view="{view}"', index)
        for page in ("markets", "compare", "liquidity", "quality"):
            self.assertIn(f'data-workspace-view="{page}"', index)
        self.assertIn('id="execution-notional"', index)
        self.assertIn('data-execution-direction="buy_token"', index)
        self.assertIn('data-quality-scope="selected"', index)
        self.assertIn('id="workspace-market-body"', index)
        self.assertIn('id="route-announcer"', index)
        self.assertIn(
            "Daily quality and coverage audit the full catalog history",
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

    def test_methodology_starts_without_facts_but_navigation_lazily_loads_summary(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        router = app_js[
            app_js.index("async function applyRouteFromLocation()"):
            app_js.index("function validateDateRange(")
        ]
        initializer = app_js[
            app_js.index("async function initialize()"):
            app_js.index('if (typeof document !== "undefined") initialize();')
        ]
        self.assertIn('if (route.kind === "methodology")', router)
        self.assertIn("if (!app.payload)", router)
        self.assertIn("await loadMarket(start, end);", router)
        self.assertIn('if (initialRoute.kind === "methodology")', initializer)
        self.assertNotIn(
            'initialRoute.kind === "workspace" && initialRoute.page === "compare"',
            initializer,
        )
        self.assertLess(
            initializer.index('if (initialRoute.kind === "methodology")'),
            initializer.index("readDefaultMarketCache()"),
        )
        self.assertNotIn("locationKey", router)
        self.assertIn("const latestRoute = navigation.parseRoute(", router)
        self.assertIn("route = { ...latestRoute, token: exactToken };", router)
        self.assertIn("setWorkspaceCatalogLoading(provisionalToken", router)

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
        self.assertIn("currentSummaryWindowRouteState()", route_links)

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


if __name__ == "__main__":
    unittest.main()
