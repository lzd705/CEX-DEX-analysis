import json
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NAVIGATION_PATH = PROJECT_ROOT / "dashboard" / "static" / "navigation.js"


def run_navigation_javascript(source: str):
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("Node.js is not installed in this runtime")
    script = (
        f"const navigation = require({json.dumps(str(NAVIGATION_PATH))});\n"
        f"{source}"
    )
    completed = subprocess.run(
        [node, "-e", script],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


class MarketMonitorNavigationTest(unittest.TestCase):
    def test_workspace_route_round_trip_preserves_encoded_market_ids_and_case(self):
        result = run_navigation_javascript(
            """
const marketA = "cex:Binance:AAVE/USDT";
const marketB = "dex:solana:Orca:9WzDXwBbmkg8Z9qVhP2X6eQ/AAVE";
const path = navigation.buildWorkspacePath("AAVE", "compare", {
  marketA,
  marketB,
  start: "2026-07-01",
  end: "2026-07-28",
  window: "30d",
});
const url = new URL(path, "https://example.test");
console.log(JSON.stringify({
  path,
  parsed: navigation.parseRoute(url.pathname, url.search),
}));
"""
        )
        self.assertIn("marketA=cex%3ABinance%3AAAVE%2FUSDT", result["path"])
        self.assertEqual(result["parsed"]["token"], "AAVE")
        self.assertEqual(
            result["parsed"]["state"]["marketA"],
            "cex:Binance:AAVE/USDT",
        )
        self.assertEqual(
            result["parsed"]["state"]["marketB"],
            "dex:solana:Orca:9WzDXwBbmkg8Z9qVhP2X6eQ/AAVE",
        )
        self.assertEqual(result["parsed"]["state"]["start"], "2026-07-01")
        self.assertEqual(result["parsed"]["state"]["window"], "30d")

    def test_page_specific_parameters_do_not_leak_between_workspace_pages(self):
        result = run_navigation_javascript(
            """
const shared = {
  marketA: "cex:Binance:AAVE/USDT",
  marketB: "dex:ethereum:uniswap-v3:0xAbC:AAVE",
  start: "2026-07-01",
  end: "2026-07-28",
  window: "30d",
  side: "buy",
  notionalUsd: 10000,
  view: "directional",
  scale: "log",
  scope: "selected",
};
const result = {};
for (const page of navigation.WORKSPACE_PAGES) {
  const path = navigation.buildWorkspacePath("AAVE", page, shared);
  const url = new URL(path, "https://example.test");
  result[page] = {
    path,
    state: navigation.parseRoute(url.pathname, url.search).state,
  };
}
console.log(JSON.stringify(result));
"""
        )
        self.assertEqual(
            set(result["markets"]["state"]),
            {"marketA", "marketB", "start", "end"},
        )
        self.assertEqual(
            set(result["compare"]["state"]),
            {"marketA", "marketB", "start", "end", "window"},
        )
        self.assertEqual(
            set(result["liquidity"]["state"]),
            {
                "marketA",
                "marketB",
                "start",
                "end",
                "side",
                "notionalUsd",
                "view",
                "scale",
            },
        )
        self.assertEqual(
            set(result["quality"]["state"]),
            {"marketA", "marketB", "start", "end", "scope"},
        )

    def test_validate_pair_accepts_two_exact_catalog_ids(self):
        result = run_navigation_javascript(
            """
const markets = [
  { market_id: "cex:Binance:AAVE/USDT", venue: "Binance" },
  { market_id: "dex:solana:Orca:9WzDXwBbmkg8Z9qVhP2X6eQ/AAVE", venue: "Orca" },
];
console.log(JSON.stringify(navigation.validatePair(
  markets,
  markets[0].market_id,
  markets[1].market_id,
)));
"""
        )
        self.assertTrue(result["valid"])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["marketA"]["venue"], "Binance")
        self.assertEqual(result["marketB"]["venue"], "Orca")

    def test_validate_pair_rejects_missing_invalid_same_and_case_changed_ids(self):
        result = run_navigation_javascript(
            """
const idA = "cex:Binance:AAVE/USDT";
const idB = "dex:solana:Orca:9WzDXwBbmkg8Z9qVhP2X6eQ/AAVE";
const markets = [{ market_id: idA }, { market_id: idB }];
const cases = {
  missing: navigation.validatePair(markets, idA, null),
  invalid: navigation.validatePair(markets, idA, "cex:Unknown:AAVE/USD"),
  same: navigation.validatePair(markets, idA, idA),
  wrongCase: navigation.validatePair(markets, idA, idB.toLowerCase()),
};
console.log(JSON.stringify(Object.fromEntries(
  Object.entries(cases).map(([name, value]) => [
    name,
    {
      valid: value.valid,
      codes: value.errors.map((error) => error.code),
      marketB: value.marketB,
    },
  ]),
)));
"""
        )
        self.assertIn("market_b_required", result["missing"]["codes"])
        self.assertIn("market_b_not_found", result["invalid"]["codes"])
        self.assertIn("same_market", result["same"]["codes"])
        self.assertIn("market_b_not_found", result["wrongCase"]["codes"])
        self.assertIsNone(result["wrongCase"]["marketB"])

    def test_validate_pair_with_one_catalog_market_never_invents_a_second(self):
        result = run_navigation_javascript(
            """
const only = { market_id: "cex:Binance:AAVE/USDT" };
console.log(JSON.stringify(navigation.validatePair(
  [only],
  only.market_id,
  null,
)));
"""
        )
        self.assertFalse(result["valid"])
        self.assertEqual(result["marketA"]["market_id"], "cex:Binance:AAVE/USDT")
        self.assertIsNone(result["marketB"])
        self.assertIn(
            "insufficient_markets",
            [error["code"] for error in result["errors"]],
        )

    def test_screener_and_methodology_routes_round_trip(self):
        result = run_navigation_javascript(
            """
const screenerPath = navigation.buildScreenerPath({
  q: "AAVE/ETH",
  scope: "dex",
  sort: "spread",
  status: "warning",
  start: "2026-07-01",
  end: "2026-07-28",
});
const screenerUrl = new URL(screenerPath, "https://example.test");
const methodologyPath = navigation.buildMethodologyPath("execution/cost:v1");
const methodologyUrl = new URL(methodologyPath, "https://example.test");
console.log(JSON.stringify({
  screenerPath,
  screener: navigation.parseRoute(screenerUrl.pathname, screenerUrl.search),
  methodologyPath,
  methodology: navigation.parseRoute(
    methodologyUrl.pathname,
    methodologyUrl.search,
  ),
}));
"""
        )
        self.assertEqual(result["screener"]["filters"]["q"], "AAVE/ETH")
        self.assertEqual(result["screener"]["filters"]["scope"], "dex")
        self.assertEqual(result["screener"]["filters"]["sort"], "spread")
        self.assertNotIn("status", result["screener"]["filters"])
        self.assertNotIn("status=", result["screenerPath"])
        self.assertEqual(result["screener"]["filters"]["start"], "2026-07-01")
        self.assertEqual(result["screener"]["filters"]["end"], "2026-07-28")
        self.assertEqual(
            result["methodology"]["anchor"],
            "execution/cost:v1",
        )

    def test_unknown_token_is_parsed_but_unknown_routes_are_rejected(self):
        result = run_navigation_javascript(
            """
console.log(JSON.stringify({
  unknownToken: navigation.parseRoute(
    "/tokens/NOT-A-CATALOG-TOKEN/markets",
    "",
  ),
  unknownPage: navigation.parseRoute("/tokens/AAVE/not-a-page", ""),
  extraSegment: navigation.parseRoute("/tokens/AAVE/markets/extra", ""),
  malformed: navigation.parseRoute("/tokens/%E0%A4%A/markets", ""),
}));
"""
        )
        self.assertEqual(result["unknownToken"]["kind"], "workspace")
        self.assertEqual(result["unknownToken"]["token"], "NOT-A-CATALOG-TOKEN")
        self.assertEqual(result["unknownPage"]["kind"], "unknown")
        self.assertEqual(result["extraSegment"]["kind"], "unknown")
        self.assertEqual(result["malformed"]["kind"], "unknown")

    def test_liquidity_route_rejects_uncollected_notional(self):
        result = run_navigation_javascript(
            """
const built = navigation.buildWorkspacePath("AAVE", "liquidity", {
  notionalUsd: 12345,
});
const parsed = navigation.parseRoute(
  "/tokens/AAVE/liquidity",
  "?notionalUsd=12345",
);
console.log(JSON.stringify({ built, parsed }));
"""
        )
        self.assertNotIn("notionalUsd", result["built"])
        self.assertNotIn("notionalUsd", result["parsed"]["state"])

    def test_manual_pair_mode_round_trips_for_incomplete_selection(self):
        result = run_navigation_javascript(
            """
const path = navigation.buildWorkspacePath("ETH", "markets", {
  marketA: "cex:binance:ETH/USDT",
  pairMode: "manual",
});
const url = new URL(path, "https://example.test");
console.log(JSON.stringify({
  path,
  parsed: navigation.parseRoute(url.pathname, url.search),
}));
"""
        )
        self.assertIn("pairMode=manual", result["path"])
        self.assertEqual(result["parsed"]["state"]["pairMode"], "manual")
        self.assertEqual(
            result["parsed"]["state"]["marketA"],
            "cex:binance:ETH/USDT",
        )
        self.assertNotIn("marketB", result["parsed"]["state"])


if __name__ == "__main__":
    unittest.main()
