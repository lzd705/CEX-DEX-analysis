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
    def test_opportunity_scope_defaults_to_current_and_historical_round_trips(self):
        result = run_navigation_javascript(
            """
const current = navigation.parseRoute("/opportunities", "?token=UNI");
const explicitCurrent = navigation.parseRoute(
  "/opportunities", "?opportunity_scope=current&token=UNI"
);
const historical = navigation.parseRoute(
  "/opportunities", "?opportunity_scope=historical&token=UNI"
);
const historicalPath = navigation.buildOpportunitiesPath(historical.filters);
const currentPath = navigation.buildOpportunitiesPath({
  ...explicitCurrent.filters,
  opportunityScope: "current",
});
console.log(JSON.stringify({
  current,
  explicitCurrent,
  historical,
  historicalPath,
  currentPath,
}));
"""
        )

        self.assertEqual(result["current"], {
            "kind": "opportunities",
            "filters": {"token": "UNI"},
        })
        self.assertEqual(result["explicitCurrent"], result["current"])
        self.assertEqual(
            result["historical"]["filters"]["opportunityScope"],
            "historical",
        )
        self.assertIn("opportunity_scope=historical", result["historicalPath"])
        self.assertNotIn("opportunity_scope", result["currentPath"])

    def test_invalid_or_duplicate_opportunity_scope_is_explicitly_rejected(self):
        result = run_navigation_javascript(
            """
const invalid = navigation.parseRoute(
  "/opportunities", "?opportunity_scope=future&token=UNI"
);
const duplicate = navigation.parseRoute(
  "/opportunities",
  "?opportunity_scope=current&opportunity_scope=historical&token=UNI",
);
let buildError = null;
try {
  navigation.buildOpportunitiesPath({ opportunityScope: "future" });
} catch (error) {
  buildError = error.message;
}
console.log(JSON.stringify({ invalid, duplicate, buildError }));
"""
        )

        expected_error = {
            "code": "invalid_opportunity_scope",
            "field": "opportunity_scope",
        }
        self.assertEqual(
            result["invalid"]["validationErrors"],
            [{**expected_error, "value": "future"}],
        )
        self.assertEqual(
            result["duplicate"]["validationErrors"],
            [{**expected_error, "value": "duplicate"}],
        )
        self.assertIn("scope", result["buildError"].lower())

    def test_opportunity_volume_sort_round_trips(self):
        result = run_navigation_javascript(
            """
const parsed = navigation.parseRoute(
  "/opportunities", "?sort=volume&dir=desc"
);
const built = navigation.buildOpportunitiesPath(parsed.filters);
console.log(JSON.stringify({ parsed, built }));
"""
        )

        self.assertEqual(result["parsed"], {
            "kind": "opportunities",
            "filters": {"sort": "volume", "dir": "desc"},
        })
        self.assertIn("sort=volume", result["built"])

    def test_opportunities_route_round_trip_is_independent_from_market_pair_state(self):
        result = run_navigation_javascript(
            """
const parsed = navigation.parseRoute(
  "/opportunities",
  "?token=AAVE&notional=10000&class=strict&route_type=cex_dex"
    + "&venue=uniswap-v3&availability=available&sort=net_edge_usd&dir=desc"
    + "&marketA=cex%3Abinance%3AAAVE%2FUSDT",
);
const path = navigation.buildOpportunitiesPath(parsed.filters);
const url = new URL(path, "https://example.test");
console.log(JSON.stringify({
  parsed,
  path,
  reparsed: navigation.parseRoute(url.pathname, url.search),
}));
"""
        )
        expected = {
            "token": "AAVE",
            "venue": "uniswap-v3",
            "notionalUsd": 10000,
            "opportunityClass": "strict",
            "routeType": "cex_dex",
            "availability": "available",
            "sort": "net_edge_usd",
            "dir": "desc",
        }
        self.assertEqual(result["parsed"], {
            "kind": "opportunities",
            "filters": expected,
        })
        self.assertEqual(result["reparsed"], result["parsed"])
        self.assertTrue(result["path"].startswith("/opportunities?"))
        self.assertIn("route_type=cex_dex", result["path"])
        self.assertIn("venue=uniswap-v3", result["path"])
        self.assertNotIn("marketA", result["path"])

    def test_opportunities_invalid_token_and_venue_are_explicit_not_all_scope(self):
        result = run_navigation_javascript(
            """
const parsedToken = navigation.parseRoute(
  "/opportunities",
  "?token=AAVE%3FBAD&venue=kraken",
);
const parsedVenue = navigation.parseRoute(
  "/opportunities",
  "?token=AAVE&venue=kraken%2Fbinance",
);
let tokenBuildError = null;
let venueBuildError = null;
try {
  navigation.buildOpportunitiesPath({ token: "AAVE?BAD" });
} catch (error) {
  tokenBuildError = error.message;
}
try {
  navigation.buildOpportunitiesPath({ token: "AAVE", venue: "kraken/binance" });
} catch (error) {
  venueBuildError = error.message;
}
console.log(JSON.stringify({
  parsedToken,
  parsedVenue,
  tokenBuildError,
  venueBuildError,
}));
"""
        )

        self.assertEqual(result["parsedToken"]["filters"]["token"], "AAVE?BAD")
        self.assertEqual(result["parsedToken"]["filters"]["venue"], "kraken")
        self.assertEqual(result["parsedToken"]["validationErrors"], [{
            "code": "invalid_token",
            "field": "token",
            "value": "AAVE?BAD",
        }])
        self.assertEqual(result["parsedVenue"]["filters"]["token"], "AAVE")
        self.assertEqual(
            result["parsedVenue"]["filters"]["venue"],
            "kraken/binance",
        )
        self.assertEqual(result["parsedVenue"]["validationErrors"], [{
            "code": "invalid_venue",
            "field": "venue",
            "value": "kraken/binance",
        }])
        self.assertIn("Token", result["tokenBuildError"])
        self.assertIn("venue", result["venueBuildError"])

    def test_opportunities_route_drops_unknown_filters_and_uncollected_notional(self):
        result = run_navigation_javascript(
            """
const parsed = navigation.parseRoute(
  "/opportunities/",
  "?token=%20&notional=12345&class=profit&route_type=bridge"
    + "&availability=maybe&sort=roi&dir=sideways&start=2026-07-01",
);
const built = navigation.buildOpportunitiesPath({
  token: "",
  notionalUsd: 12345,
  opportunityClass: "profit",
  routeType: "bridge",
  availability: "maybe",
  sort: "roi",
  dir: "sideways",
  marketA: "cex:binance:AAVE/USDT",
});
console.log(JSON.stringify({ parsed, built }));
"""
        )
        self.assertEqual(result["parsed"], {
            "kind": "opportunities",
            "filters": {},
        })
        self.assertEqual(result["built"], "/opportunities")

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
  lifecycle: "scheduled",
  clockState: "future",
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
        self.assertEqual(
            set(result["events"]["state"]),
            {
                "marketA",
                "marketB",
                "start",
                "end",
                "lifecycle",
                "clockState",
            },
        )

    def test_screener_quality_alert_link_preserves_exact_severity(self):
        result = run_navigation_javascript(
            """
const path = navigation.buildWorkspacePath("AAVE", "quality", {
  start: "2026-07-01",
  end: "2026-07-28",
  scope: "all",
  severity: "warning",
  origin: "screener",
});
const url = new URL(path, "https://example.test");
console.log(JSON.stringify({
  path,
  route: navigation.parseRoute(url.pathname, url.search),
}));
"""
        )
        self.assertIn("severity=warning", result["path"])
        self.assertIn("origin=screener", result["path"])
        self.assertEqual(result["route"]["state"]["severity"], "warning")
        self.assertEqual(result["route"]["state"]["origin"], "screener")

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

    def test_screener_sort_scope_direction_and_dates_round_trip(self):
        result = run_navigation_javascript(
            """
const screenerPath = navigation.buildScreenerPath({
  q: "AAVE/ETH",
  scope: "dex",
  sort: "return",
  dir: "asc",
  status: "warning",
  start: "2026-07-01",
  end: "2026-07-28",
});
const screenerUrl = new URL(screenerPath, "https://example.test");
console.log(JSON.stringify({
  screenerPath,
  screener: navigation.parseRoute(screenerUrl.pathname, screenerUrl.search),
}));
"""
        )
        self.assertEqual(result["screener"]["filters"]["q"], "AAVE/ETH")
        self.assertEqual(result["screener"]["filters"]["scope"], "dex")
        self.assertEqual(result["screener"]["filters"]["sort"], "return")
        self.assertEqual(result["screener"]["filters"]["dir"], "asc")
        self.assertNotIn("status", result["screener"]["filters"])
        self.assertNotIn("status=", result["screenerPath"])
        self.assertEqual(result["screener"]["filters"]["start"], "2026-07-01")
        self.assertEqual(result["screener"]["filters"]["end"], "2026-07-28")

    def test_screener_rejects_unknown_sort_scope_direction_and_malformed_dates(self):
        result = run_navigation_javascript(
            """
const parsed = navigation.parseRoute(
  "/screener",
  "?sort=profit&scope=all-venues&dir=sideways"
    + "&start=2026-7-01&end=not-a-date&unknown=retained",
);
const built = navigation.buildScreenerPath({
  sort: "profit",
  scope: "all-venues",
  dir: "sideways",
  start: "2026-7-01",
  end: "not-a-date",
});
console.log(JSON.stringify({ parsed, built }));
"""
        )
        self.assertEqual(result["parsed"], {"kind": "screener", "filters": {}})
        self.assertEqual(result["built"], "/screener")

    def test_routes_reject_impossible_calendar_dates_and_keep_real_leap_days(self):
        result = run_navigation_javascript(
            """
const invalidScreener = navigation.parseRoute(
  "/screener",
  "?start=2026-02-29&end=2026-04-31",
);
const invalidWorkspace = navigation.parseRoute(
  "/tokens/AAVE/markets",
  "?start=2026-02-31&end=2026-13-01",
);
const invalidBuilt = navigation.buildWorkspacePath("AAVE", "markets", {
  start: "2026-02-29",
  end: "2026-04-31",
});
const leapBuilt = navigation.buildScreenerPath({
  start: "2024-02-29",
  end: "2024-03-01",
});
const leapUrl = new URL(leapBuilt, "https://example.test");
console.log(JSON.stringify({
  invalidScreener,
  invalidWorkspace,
  invalidBuilt,
  leapBuilt,
  leapParsed: navigation.parseRoute(leapUrl.pathname, leapUrl.search),
}));
"""
        )
        self.assertEqual(result["invalidScreener"]["filters"], {})
        self.assertEqual(result["invalidWorkspace"]["state"], {})
        self.assertEqual(result["invalidBuilt"], "/tokens/AAVE/markets")
        self.assertIn("start=2024-02-29", result["leapBuilt"])
        self.assertEqual(
            result["leapParsed"]["filters"],
            {"start": "2024-02-29", "end": "2024-03-01"},
        )

    def test_screener_normalizes_metric_scope_combinations(self):
        result = run_navigation_javascript(
            """
const cases = {
  spread: navigation.parseRoute("/screener", "?sort=spread&scope=cex"),
  spreadMax: navigation.parseRoute(
    "/screener",
    "?sort=spread_max&scope=dex",
  ),
  spreadMean: navigation.parseRoute(
    "/screener",
    "?sort=spread_mean&scope=combined",
  ),
  spreadMedian: navigation.parseRoute(
    "/screener",
    "?sort=spread_median&scope=cex",
  ),
  return: navigation.parseRoute("/screener", "?sort=return&scope=combined"),
  volatility: navigation.parseRoute(
    "/screener",
    "?sort=volatility&scope=combined",
  ),
  dexTvl: navigation.parseRoute("/screener", "?sort=dex_tvl&scope=cex"),
};
console.log(JSON.stringify(cases));
"""
        )
        self.assertEqual(result["spread"]["filters"]["sort"], "spread")
        self.assertEqual(result["spread"]["filters"]["scope"], "cross")
        for key, field in (
            ("spreadMax", "spread_max"),
            ("spreadMean", "spread_mean"),
            ("spreadMedian", "spread_median"),
        ):
            self.assertEqual(result[key]["filters"]["sort"], field)
            self.assertEqual(result[key]["filters"]["scope"], "cross")
        self.assertEqual(result["return"]["filters"]["scope"], "cex")
        self.assertEqual(result["volatility"]["filters"]["scope"], "cex")
        self.assertEqual(result["dexTvl"]["filters"]["scope"], "dex")

    def test_removed_methodology_urls_soft_route_to_screener(self):
        result = run_navigation_javascript(
            """
const root = navigation.parseRoute("/methodology", "");
const anchored = navigation.parseRoute("/methodology/execution-cost", "");
console.log(JSON.stringify({
  root,
  anchored,
  rootCanonical: navigation.buildScreenerPath(root.filters),
  anchoredCanonical: navigation.buildScreenerPath(anchored.filters),
  methodologyBuilderType: typeof navigation.buildMethodologyPath,
}));
"""
        )
        for route_name in ("root", "anchored"):
            self.assertEqual(result[route_name]["kind"], "screener")
            self.assertTrue(result[route_name]["legacyMethodologyPath"])
            self.assertEqual(result[f"{route_name}Canonical"], "/screener")
        self.assertEqual(result["methodologyBuilderType"], "undefined")

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
