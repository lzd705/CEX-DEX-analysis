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
