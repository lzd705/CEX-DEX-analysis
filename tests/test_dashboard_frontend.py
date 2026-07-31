import json
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"


def run_app_javascript(source: str, prelude: str = ""):
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


class DashboardFrontendContractTest(unittest.TestCase):
    def test_unsubmitted_custom_window_never_leaks_to_applied_consumers(self):
        result = run_app_javascript(
            """
function control(value = "") {
  return {
    value,
    hidden: false,
    disabled: false,
    textContent: "",
    innerHTML: "",
    dataset: {},
    attributes: {},
    setAttribute(name, nextValue) { this.attributes[name] = nextValue; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}

const elements = new Map(Object.entries({
  "date-start": control("2026-07-20"),
  "date-end": control("2026-07-22"),
  "token-search": control(""),
  "sort-field": control("volume"),
  "facts-token": control("BTC"),
  "facts-market-a": control("cex:binance:BTC/USDT"),
  "facts-market-b": control("dex:uniswap:BTC/USDC"),
}));
const requestUrls = [];
let exportName = "";
let replacedPath = "";

global.document = {
  body: { appendChild() {} },
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, control());
    return elements.get(id);
  },
  querySelector() { return null; },
  querySelectorAll() { return []; },
  createElement(tag) {
    if (tag !== "a") throw new Error(`Unexpected element: ${tag}`);
    return {
      href: "",
      download: "",
      click() { exportName = this.download; },
      remove() {},
    };
  },
};
global.window = {
  location: { pathname: "/tokens/BTC/compare", search: "" },
  history: {
    replaceState(_state, _title, path) { replacedPath = path; },
  },
};
global.URL.createObjectURL = () => "blob:market-facts";
global.URL.revokeObjectURL = () => {};
global.fetch = async (url) => {
  requestUrls.push(url);
  if (url.startsWith("/api/markets/quality?")) {
    return {
      ok: true,
      json: async () => ({
        token_symbol: "BTC",
        metadata: {
          scope: "all",
          window_start: "2026-07-01",
          window_end: "2026-07-29",
          daily_quality_report: { status: "unavailable" },
        },
        markets: [],
      }),
    };
  }
  if (url.startsWith("/api/markets/events?")) {
    return { ok: true, json: async () => ({ query: { token: "BTC" }, events: [] }) };
  }
  if (url.startsWith("/api/markets/compare?")) {
    return { ok: true, json: async () => ({ metadata: {} }) };
  }
  throw new Error(`Unexpected request: ${url}`);
};

renderComparison = (payload) => { app.comparison = payload; };
renderComparisonChart = () => {};
clearComparisonChart = () => {};
renderEventFacts = (payload) => { app.eventFacts = payload; };

const tokenSummary = {
  token_symbol: "BTC",
  aggregate_volume_usd: 1,
  primary_cex: null,
  primary_dex: null,
};
app.payload = {
  metadata: {
    start_date: "2026-07-01",
    end_date: "2026-07-29",
    available_start: "2026-05-01",
    available_end: "2026-07-29",
  },
  tokens: [tokenSummary],
};
app.defaultPayload = null;
app.visibleTokens = [tokenSummary];
app.catalog = { metadata: {} };
app.route = {
  kind: "workspace",
  token: "BTC",
  page: "compare",
  state: { start: "2026-07-01", end: "2026-07-29" },
};
app.routeReady = true;
app.qualityScope = "all";
app.scope = "combined";
app.sortDirection = "desc";

const screenerWindow = currentScreenerFilters();
const workspaceWindow = currentWorkspaceRouteState("compare");
const workspacePath = currentWorkspacePath("compare");
const candidate = { start: "2026-07-10", end: "2026-07-11" };
const candidateScreener = currentScreenerFilters({ window: candidate });
const candidateWorkspace = currentWorkspaceRouteState("compare", { window: candidate });
replaceCurrentRoute({ window: candidate });
(async () => {
  await loadQuality();
  await loadComparison();
  await loadEvents();
  exportVisibleCsv();
  const draftHelperAvailable = typeof draftTimeWindow === "function";
  const draft = draftHelperAvailable
    ? draftTimeWindow()
    : {
        start: elements.get("date-start").value,
        end: elements.get("date-end").value,
      };
  const draftSetterAvailable = typeof setDraftTimeWindow === "function";
  let draftSetterRoundTrip = null;
  if (draftSetterAvailable) {
    setDraftTimeWindow({ start: "2026-07-24", end: "2026-07-25" });
    draftSetterRoundTrip = draftTimeWindow();
    setDraftTimeWindow(draft);
  }
  const payload = app.payload;
  app.payload = null;
  app.defaultPayload = {
    metadata: { start_date: "2026-06-01", end_date: "2026-06-30" },
  };
  const defaultPayloadBootstrap = appliedTimeWindow();
  app.defaultPayload = null;
  app.route = {
    kind: "workspace",
    state: { start: "2026-05-01", end: "2026-05-31" },
  };
  const workspaceBootstrap = appliedTimeWindow();
  app.route = {
    kind: "screener",
    filters: { start: "2026-04-01", end: "2026-04-30" },
  };
  const screenerBootstrap = appliedTimeWindow();
  app.payload = payload;

  console.log(JSON.stringify({
    screenerWindow: { start: screenerWindow.start, end: screenerWindow.end },
    workspaceWindow: { start: workspaceWindow.start, end: workspaceWindow.end },
    workspacePath,
    qualityUrl: requestUrls.find((url) => url.startsWith("/api/markets/quality?")),
    comparisonUrl: requestUrls.find((url) => url.startsWith("/api/markets/compare?")),
    eventUrls: requestUrls.filter((url) => url.startsWith("/api/markets/events?")),
    exportName,
    draft,
    draftHelperAvailable,
    draftSetterAvailable,
    draftSetterRoundTrip,
    defaultPayloadBootstrap,
    workspaceBootstrap,
    screenerBootstrap,
    candidateScreener: {
      start: candidateScreener.start,
      end: candidateScreener.end,
    },
    candidateWorkspace: {
      start: candidateWorkspace.start,
      end: candidateWorkspace.end,
    },
    replacedPath,
  }));
})();
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  buildScreenerPath(filters) {
    return `/screener?${new URLSearchParams(filters).toString()}`;
  },
  buildWorkspacePath(token, page, state) {
    return `/tokens/${token}/${page}?${new URLSearchParams(state).toString()}`;
  },
  parseRoute() {
    return {
      kind: "workspace",
      token: "BTC",
      page: "compare",
      state: {},
    };
  },
};
""",
        )
        self.assertEqual(result["screenerWindow"], {
            "start": "2026-07-01",
            "end": "2026-07-29",
        })
        self.assertEqual(result["workspaceWindow"], {
            "start": "2026-07-01",
            "end": "2026-07-29",
        })
        self.assertIn("start=2026-07-01", result["workspacePath"])
        self.assertIn("end=2026-07-29", result["workspacePath"])
        self.assertIn("start=2026-07-01", result["qualityUrl"])
        self.assertIn("end=2026-07-29", result["qualityUrl"])
        self.assertIn("start=2026-07-01", result["comparisonUrl"])
        self.assertIn("end=2026-07-29", result["comparisonUrl"])
        self.assertEqual(len(result["eventUrls"]), 2)
        for event_url in result["eventUrls"]:
            self.assertIn("start=2026-07-01", event_url)
            self.assertIn("end=2026-07-29", event_url)
        self.assertEqual(
            result["exportName"],
            "cex-dex-market-facts-2026-07-01-2026-07-29.csv",
        )
        self.assertEqual(result["draft"], {
            "start": "2026-07-20",
            "end": "2026-07-22",
        })
        self.assertTrue(result["draftHelperAvailable"])
        self.assertTrue(result["draftSetterAvailable"])
        self.assertEqual(result["draftSetterRoundTrip"], {
            "start": "2026-07-24",
            "end": "2026-07-25",
        })
        self.assertEqual(result["defaultPayloadBootstrap"], {
            "start": "2026-06-01",
            "end": "2026-06-30",
        })
        self.assertEqual(result["workspaceBootstrap"], {
            "start": "2026-05-01",
            "end": "2026-05-31",
        })
        self.assertEqual(result["screenerBootstrap"], {
            "start": "2026-04-01",
            "end": "2026-04-30",
        })
        candidate = {
            "start": "2026-07-10",
            "end": "2026-07-11",
        }
        self.assertEqual(result["candidateScreener"], candidate)
        self.assertEqual(result["candidateWorkspace"], candidate)
        self.assertIn("start=2026-07-10", result["replacedPath"])
        self.assertIn("end=2026-07-11", result["replacedPath"])

    def test_workspace_route_reports_load_abort_and_catalog_outcomes(self):
        result = run_app_javascript(
            """
function control() {
  return {
    value: "",
    hidden: false,
    disabled: false,
    textContent: "",
    dataset: {},
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
  };
}
const controls = new Map();
global.document = {
  getElementById(id) {
    if (!controls.has(id)) controls.set(id, control());
    return controls.get(id);
  },
  querySelectorAll() { return []; },
};
global.window = {
  location: { pathname: "/tokens/BTC/markets", search: "" },
  history: { replaceState() {} },
  lucide: null,
};
let requestedStart = "2026-06-30";
globalThis.__workspaceRoute = () => ({
  kind: "workspace",
  token: "BTC",
  page: "markets",
  state: { start: requestedStart, end: "2026-07-29" },
});

setActiveAppView = () => {};
setActiveWorkspacePage = () => {};
setWorkspaceCatalogLoading = () => {};
setWorkspaceDataUnavailable = () => {};
applyWorkspaceRoute = (route) => { app.route = route; };
announceRoute = () => {};
updateRouteLinks = () => {};
canonicalizeCurrentRoute = () => {};
cachedTokenCatalog = () => null;

function payload(start = "2026-06-30", end = "2026-07-29") {
  return {
    metadata: {
      start_date: start,
      end_date: end,
      available_start: "2026-05-01",
      available_end: "2026-07-29",
      data_generation: "generation-a",
    },
    tokens: [{ token_symbol: "BTC" }],
  };
}

async function runScenario(mode) {
  requestedStart = mode === "market-load-failure" ? "2026-07-23" : "2026-06-30";
  app.payload = payload();
  app.route = globalThis.__workspaceRoute();
  app.routeReady = false;
  app.routeRequestId = 0;
  app.marketRequestId = 0;
  app.marketController = null;
  app.catalogController = null;
  app.catalogsByToken.clear();
  let marketLoads = 0;
  let catalogLoads = 0;
  loadMarket = async () => {
    marketLoads += 1;
    return mode !== "market-load-failure" && mode !== "generation-refresh-failure";
  };
  loadTokenCatalog = async () => {
    catalogLoads += 1;
    if (mode === "catalog-abort") {
      const error = new Error("cancelled");
      error.name = "AbortError";
      throw error;
    }
    if (mode === "catalog-failure") throw new Error("catalog unavailable");
    if (mode === "generation-refresh-failure") {
      const error = new Error("generation changed");
      error.code = "data_generation_mismatch";
      throw error;
    }
    return { metadata: {} };
  };
  const applied = await applyRouteFromLocation();
  return { applied, marketLoads, catalogLoads };
}

const outcomes = {};
for (const mode of [
  "market-load-failure",
  "catalog-abort",
  "catalog-failure",
  "generation-refresh-failure",
  "success",
]) {
  outcomes[mode] = await runScenario(mode);
}
console.log(JSON.stringify(outcomes));
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  parseRoute() { return globalThis.__workspaceRoute(); },
};
""",
        )
        self.assertEqual(result, {
            "market-load-failure": {
                "applied": False,
                "marketLoads": 1,
                "catalogLoads": 0,
            },
            "catalog-abort": {
                "applied": False,
                "marketLoads": 0,
                "catalogLoads": 1,
            },
            "catalog-failure": {
                "applied": False,
                "marketLoads": 0,
                "catalogLoads": 1,
            },
            "generation-refresh-failure": {
                "applied": False,
                "marketLoads": 1,
                "catalogLoads": 1,
            },
            "success": {
                "applied": True,
                "marketLoads": 0,
                "catalogLoads": 1,
            },
        })

    def test_catalog_window_boundary_keeps_committed_state_and_guards_generation_refresh(self):
        self.maxDiff = None
        result = run_app_javascript(
            """
function control(dataset = {}) {
  const item = {
    value: "", hidden: false, disabled: false, textContent: "", innerHTML: "",
    dataset, attributes: {}, active: false, style: {},
    addEventListener() {},
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
    contains() { return false; },
  };
  item.classList = {
    toggle(_name, active) { item.active = Boolean(active); },
    add() { item.active = true; },
    remove() { item.active = false; },
  };
  item.parentElement = item;
  return item;
}

const controls = new Map();
const presets = ["7", "30", "90", "all"].map((days) => control({ days }));
const routeWrites = [];
global.document = {
  getElementById(id) {
    if (!controls.has(id)) controls.set(id, control());
    return controls.get(id);
  },
  querySelector(selector) { return selector === ".facts-controls" ? control() : null; },
  querySelectorAll(selector) { return selector === "[data-days]" ? presets : []; },
};
global.window = {
  location: { pathname: "/tokens/BTC/markets", search: "" },
  history: { replaceState(_state, _title, path) { routeWrites.push(path); } },
  lucide: null,
};

const OLD_START = "2026-06-30";
const NEW_START = "2026-07-23";
const END = "2026-07-29";
function payload(start, generation) {
  return {
    metadata: {
      response_scope: "screener_summary", summary_version: 2,
      data_generation: generation, start_date: start, end_date: END,
      available_start: "2026-05-01", available_end: END,
      sources: [], tvl_note: "TVL unavailable",
      cex_depth_note: "CEX depth unavailable",
      dex_depth_note: "DEX depth unavailable",
    },
    tokens: [{ token_symbol: "BTC", primary_cex: null, primary_dex: null }],
  };
}
function writeApplied(start, generation, visible, catalog = null) {
  app.payload = payload(start, generation);
  app.visibleTokens = [{ token_symbol: visible }];
  app.catalog = catalog;
  app.activeCatalogKey = catalog ? `BTC|${start}|${END}|${generation}` : "";
  setDraftTimeWindow({ start, end: END });
  syncTimeWindowControls();
}
function moveRoute(start, generation, visible, catalog = null) {
  window.location.search = `?start=${start}&end=${END}`;
  app.route = {
    kind: "workspace", token: "BTC", page: "markets",
    state: { start, end: END },
  };
  writeApplied(start, generation, visible, catalog);
}
function reset(start, generation) {
  app.defaultPayload = null;
  app.routeReady = true;
  app.routeRequestId = 0;
  app.marketRequestId = 0;
  app.marketController = null;
  app.catalogController = null;
  app.catalogsByToken.clear();
  routeWrites.length = 0;
  document.getElementById("custom-window-toggle")
    .setAttribute("aria-expanded", "false");
  document.getElementById("custom-window-editor").hidden = true;
  moveRoute(start, generation, generation);
}
function state() {
  return {
    payload: appliedTimeWindow(),
    draft: draftTimeWindow(),
    summary: controls.get("applied-window-summary").textContent,
    active: presets.filter((button) => button.active).map((button) => button.dataset.days),
    editorHidden: controls.get("custom-window-editor").hidden,
    expanded: controls.get("custom-window-toggle").getAttribute("aria-expanded"),
    route: `${window.location.pathname}${window.location.search}`,
    visibleTokens: app.visibleTokens.map((token) => token.token_symbol),
    catalogMarker: app.catalog?.marker || null,
    activeCatalogKey: app.activeCatalogKey,
  };
}

announceRoute = () => {};
updateRouteLinks = () => {};
canonicalizeCurrentRoute = () => {};
cachedTokenCatalog = () => null;
applyWorkspaceRoute = (route) => { app.route = route; };
const realLoadMarket = loadMarket;
const realLoadTokenCatalog = loadTokenCatalog;

async function catalogFailure() {
  reset(NEW_START, "g2");
  app.routeReady = false;
  let marketLoads = 0;
  loadMarket = async () => { marketLoads += 1; return true; };
  loadTokenCatalog = async () => { throw new Error("catalog unavailable"); };
  const applied = await applyRouteFromLocation();
  replaceCurrentRoute();
  return {
    applied, marketLoads, ...state(),
    routeReady: app.routeReady,
    routeWrites: [...routeWrites],
    notice: controls.get("workspace-context-notice").textContent,
    noticeState: controls.get("workspace-context-notice").dataset.state,
    globalError: controls.get("global-error").textContent,
    busy: controls.get("facts-workbench").getAttribute("aria-busy"),
  };
}

async function staleMismatch() {
  reset(OLD_START, "g1");
  let rejectCatalog;
  let marketLoads = 0;
  loadTokenCatalog = () => new Promise((_resolve, reject) => { rejectCatalog = reject; });
  loadMarket = async (start) => {
    marketLoads += 1;
    writeApplied(start, "stale-refresh", "STALE_REFRESH");
    return true;
  };
  const completion = applyRouteFromLocation();
  if (!rejectCatalog) throw new Error("The controlled catalog request did not start.");
  invalidateRouteRequest();
  moveRoute(NEW_START, "g2", "NEWER", { marker: "newer" });
  const mismatch = new Error("generation changed");
  mismatch.code = "data_generation_mismatch";
  rejectCatalog(mismatch);
  return { applied: await completion, marketLoads, ...state() };
}

async function currentMismatch() {
  reset(OLD_START, "g1");
  const summaryRequests = [];
  const catalogKeys = [];
  global.fetch = async (url) => {
    summaryRequests.push(url);
    return { ok: true, status: 200, json: async () => payload(OLD_START, "g2") };
  };
  loadMarket = realLoadMarket;
  loadTokenCatalog = async (_token, _start, _end, _signal, cacheKey) => {
    catalogKeys.push(cacheKey);
    if (catalogKeys.length === 1) {
      const mismatch = new Error("generation changed");
      mismatch.code = "data_generation_mismatch";
      throw mismatch;
    }
    return { marker: "g2" };
  };
  const applied = await applyRouteFromLocation();
  return {
    applied, summaryRequests, catalogKeys, routeRequests: app.routeRequestId,
    catalogMarker: app.catalog?.marker || null,
    activeCatalogKey: app.activeCatalogKey,
    cacheKeys: [...app.catalogsByToken.keys()],
  };
}

async function staleSuccess() {
  reset(OLD_START, "g1");
  let resolveCatalog;
  global.fetch = () => new Promise((resolve) => { resolveCatalog = resolve; });
  loadTokenCatalog = realLoadTokenCatalog;
  const completion = applyRouteFromLocation();
  await Promise.resolve();
  invalidateRouteRequest();
  window.location.search = `?start=${NEW_START}&end=${END}`;
  app.route = {
    kind: "workspace", token: "BTC", page: "markets",
    state: { start: NEW_START, end: END },
  };
  resolveCatalog({
    ok: true,
    status: 200,
    json: async () => ({
      token_symbol: "BTC",
      metadata: {
        data_generation: "g1",
        window_start: OLD_START,
        window_end: END,
      },
      markets: [{ token_symbol: "BTC" }],
    }),
  });
  return {
    applied: await completion,
    cacheKeys: [...app.catalogsByToken.keys()],
  };
}

async function competingSummaryBeatsOlderCatalogRetry() {
  reset(OLD_START, "g1");
  const summaryUrls = [];
  let resolveFirstCatalog;
  let resolveNewerSummary;
  let catalogCount = 0;
  global.fetch = (url) => {
    if (url.startsWith("/api/markets/catalog?")) {
      catalogCount += 1;
      if (catalogCount === 1) {
        return new Promise((resolve) => { resolveFirstCatalog = resolve; });
      }
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({
          token_symbol: "BTC",
          metadata: {
            data_generation: "g2",
            window_start: OLD_START,
            window_end: END,
          },
          markets: [{ token_symbol: "BTC" }],
        }),
      });
    }
    summaryUrls.push(url);
    if (url.includes(`start=${NEW_START}`)) {
      return new Promise((resolve) => { resolveNewerSummary = resolve; });
    }
    return Promise.resolve({
      ok: true,
      status: 200,
      json: async () => payload(OLD_START, "g2"),
    });
  };
  loadMarket = realLoadMarket;
  loadTokenCatalog = realLoadTokenCatalog;
  const routeCompletion = applyRouteFromLocation();
  await Promise.resolve();
  const summaryCompletion = loadMarket(NEW_START, END, {
    preserve: true,
    refreshWorkspaceOnGenerationChange: false,
  });
  await Promise.resolve();
  resolveFirstCatalog({
    ok: true,
    status: 200,
    json: async () => ({
      token_symbol: "BTC",
      metadata: {
        data_generation: "g2",
        window_start: OLD_START,
        window_end: END,
      },
      markets: [{ token_symbol: "BTC" }],
    }),
  });
  const routeApplied = await routeCompletion;
  resolveNewerSummary({
    ok: true,
    status: 200,
    json: async () => payload(NEW_START, "g2"),
  });
  const summaryApplied = await summaryCompletion;
  return {
    routeApplied,
    summaryApplied,
    summaryUrls,
    catalogCount,
    payload: appliedTimeWindow(),
  };
}

(async () => console.log(JSON.stringify({
  catalogFailure: await catalogFailure(),
  staleMismatch: await staleMismatch(),
  currentMismatch: await currentMismatch(),
  staleSuccess: await staleSuccess(),
  competingSummary: await competingSummaryBeatsOlderCatalogRetry(),
})))();
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  parseRoute(pathname, search) {
    const parts = pathname.split("/");
    const params = new URLSearchParams(search);
    return {
      kind: "workspace", token: parts[2], page: parts[3],
      state: { start: params.get("start") || "", end: params.get("end") || "" },
    };
  },
  buildWorkspacePath(token, page, state) {
    return `/tokens/${token}/${page}?start=${state.start || ""}&end=${state.end || ""}`;
  },
};
""",
        )

        new_window = {
            "payload": {"start": "2026-07-23", "end": "2026-07-29"},
            "draft": {"start": "2026-07-23", "end": "2026-07-29"},
            "summary": "23–29 Jul 2026 · 7 days",
            "active": ["7"],
            "editorHidden": True,
            "expanded": "false",
            "route": (
                "/tokens/BTC/markets?start=2026-07-23&end=2026-07-29"
            ),
        }
        self.assertEqual(result["catalogFailure"], {
            "applied": False,
            "marketLoads": 0,
            **new_window,
            "visibleTokens": ["g2"],
            "catalogMarker": None,
            "activeCatalogKey": "",
            "notice": "BTC facts are unavailable; no previous Token data is shown.",
            "noticeState": "critical",
            "globalError": "The BTC market catalog failed to load: catalog unavailable",
            "busy": "false",
            "routeReady": True,
            "routeWrites": [
                "/tokens/BTC/markets?start=2026-07-23&end=2026-07-29"
            ],
        })
        self.assertEqual(result["staleMismatch"], {
            "applied": False,
            "marketLoads": 0,
            **new_window,
            "visibleTokens": ["NEWER"],
            "catalogMarker": "newer",
            "activeCatalogKey": "BTC|2026-07-23|2026-07-29|g2",
        })
        self.assertEqual(result["currentMismatch"], {
            "applied": True,
            "summaryRequests": [
                "/api/markets/summary?start=2026-06-30&end=2026-07-29"
            ],
            "catalogKeys": [
                "BTC|2026-06-30|2026-07-29|g1",
                "BTC|2026-06-30|2026-07-29|g2",
            ],
            "routeRequests": 1,
            "catalogMarker": "g2",
            "activeCatalogKey": "BTC|2026-06-30|2026-07-29|g2",
            "cacheKeys": ["BTC|2026-06-30|2026-07-29|g2"],
        })
        self.assertEqual(result["staleSuccess"], {
            "applied": False,
            "cacheKeys": [],
        })
        self.assertEqual(result["competingSummary"], {
            "routeApplied": False,
            "summaryApplied": True,
            "summaryUrls": [
                "/api/markets/summary?start=2026-07-23&end=2026-07-29"
            ],
            "catalogCount": 1,
            "payload": {"start": "2026-07-23", "end": "2026-07-29"},
        })

    def test_summary_window_commit_uses_summary_as_the_only_transaction_boundary(self):
        self.maxDiff = None
        result = run_app_javascript(
            """
function control({ value = "", hidden = false, dataset = {} } = {}) {
  return {
    value,
    hidden,
    dataset,
    disabled: false,
    textContent: "",
    innerHTML: "",
    attributes: {},
    listeners: {},
    active: false,
    focusCalls: 0,
    classList: {
      owner: null,
      toggle(name, active) {
        if (name === "active") this.owner.active = active;
      },
      contains() { return false; },
    },
    addEventListener(type, listener) {
      this.listeners[type] = this.listeners[type] || [];
      this.listeners[type].push(listener);
    },
    setAttribute(name, nextValue) { this.attributes[name] = nextValue; },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
    focus() { this.focusCalls += 1; },
  };
}

async function trigger(target, type) {
  for (const listener of target.listeners[type] || []) {
    await listener({ preventDefault() {} });
  }
}

function summaryPayload(start, end, generation = "generation-a") {
  return {
    metadata: {
      response_scope: "screener_summary",
      summary_version: 2,
      data_generation: generation,
      start_date: start,
      end_date: end,
      available_start: "2026-05-01",
      available_end: "2026-07-29",
      default_workspace_token: "BTC",
      sources: [],
      tvl_note: "TVL unavailable",
      cex_depth_note: "CEX depth unavailable",
      dex_depth_note: "DEX depth unavailable",
    },
    tokens: [{
      token_symbol: "BTC",
      primary_cex: null,
      primary_dex: null,
    }],
  };
}

const start = control();
const end = control();
const error = control({ hidden: true });
const editor = control({ hidden: true });
const toggle = control();
const form = control();
const cancel = control();
const summary = control();
const presets = ["7", "30", "90", "all"].map((days) => (
  control({ dataset: { days } })
));
const genericControls = new Map();
for (const item of [start, end, error, editor, toggle, form, cancel, summary, ...presets]) {
  item.classList.owner = item;
}
toggle.setAttribute("aria-expanded", "false");
const elements = {
  "date-start": start,
  "date-end": end,
  "date-window-error": error,
  "custom-window-editor": editor,
  "custom-window-toggle": toggle,
  "date-window-form": form,
  "cancel-window": cancel,
  "applied-window-summary": summary,
};
global.document = {
  getElementById(id) {
    if (elements[id]) return elements[id];
    if (!genericControls.has(id)) {
      const item = control();
      item.classList.owner = item;
      genericControls.set(id, item);
    }
    return genericControls.get(id);
  },
  querySelector() { return null; },
  querySelectorAll(selector) {
    if (selector === "[data-days]") return presets;
    return [];
  },
  addEventListener() {},
};

const routeWrites = [];
const routeDrafts = [];
const catalogLaunches = [];
const requests = [];
global.window = {
  location: {
    pathname: "/tokens/BTC/markets",
    search: "?start=2026-06-30&end=2026-07-29",
  },
  history: {
    replaceState(_state, _title, path) {
      routeWrites.push(path);
      routeDrafts.push(draftTimeWindow());
      const [pathname, query = ""] = path.split("?");
      global.window.location.pathname = pathname;
      global.window.location.search = query ? `?${query}` : "";
    },
  },
  addEventListener() {},
  visualViewport: null,
  matchMedia() { return { addEventListener() {} }; },
  localStorage: { setItem() {}, removeItem() {} },
  lucide: null,
};
global.fetch = (url) => new Promise((resolve) => {
  requests.push({ url, resolve });
});

function respond(index, { ok, body }) {
  if (!requests[index]) return;
  requests[index].resolve({
    ok,
    status: ok ? 200 : 503,
    json: async () => body,
  });
}

function routeUrl() {
  return `${window.location.pathname}${window.location.search}`;
}

function pressedState() {
  return Object.fromEntries(presets.map((button) => [
    button.dataset.days,
    button.getAttribute("aria-pressed"),
  ]));
}

function visibleState(focusBefore = toggle.focusCalls) {
  return {
    payload: appliedTimeWindow(),
    draft: draftTimeWindow(),
    summary: summary.textContent,
    active: presets.filter((button) => button.active).map((button) => button.dataset.days),
    pressed: pressedState(),
    editorHidden: editor.hidden,
    expanded: toggle.getAttribute("aria-expanded"),
    focusDelta: toggle.focusCalls - focusBefore,
    route: routeUrl(),
    routeWrites: [...routeWrites],
    routeDrafts: [...routeDrafts],
    requests: requests.map((request) => request.url),
    catalogLaunches: [...catalogLaunches],
  };
}

function reset({ draft, open = true, routeReady = true } = {}) {
  app.payload = summaryPayload("2026-06-30", "2026-07-29");
  app.defaultPayload = null;
  app.visibleTokens = [...app.payload.tokens];
  app.catalog = null;
  app.route = {
    kind: "workspace",
    token: "BTC",
    page: "markets",
    state: { start: "2026-06-30", end: "2026-07-29" },
  };
  app.routeReady = routeReady;
  app.marketRequestId = 0;
  app.marketRequestWindowKey = "";
  app.marketController = null;
  window.location.pathname = "/tokens/BTC/markets";
  window.location.search = "?start=2026-06-30&end=2026-07-29";
  routeWrites.length = 0;
  routeDrafts.length = 0;
  catalogLaunches.length = 0;
  requests.length = 0;
  setDraftTimeWindow(draft || appliedTimeWindow());
  editor.hidden = !open;
  toggle.setAttribute("aria-expanded", String(open));
  toggle.focusCalls = 0;
  start.focusCalls = 0;
  syncTimeWindowControls();
}

renderTable = () => {
  app.visibleTokens = [...app.payload.tokens];
};
updateRouteLinks = () => {};
applyRouteFromLocation = async () => {
  catalogLaunches.push(routeUrl());
  return false;
};

bindEvents();

(async () => {
  reset({
    draft: { start: "2026-07-20", end: "2026-07-21" },
    open: false,
  });
  await trigger(toggle, "click");
  const boundCustomOpened = {
    draft: draftTimeWindow(),
    editorHidden: editor.hidden,
    expanded: toggle.getAttribute("aria-expanded"),
    startFocusCalls: start.focusCalls,
  };
  setDraftTimeWindow({ start: "2026-07-20", end: "2026-07-21" });
  await trigger(cancel, "click");
  const boundCustomCancelled = {
    draft: draftTimeWindow(),
    editorHidden: editor.hidden,
    expanded: toggle.getAttribute("aria-expanded"),
    toggleFocusCalls: toggle.focusCalls,
  };

  reset({ draft: { start: "2026-07-20", end: "2026-07-21" } });
  let focusBefore = toggle.focusCalls;
  let completion = trigger(form, "submit");
  await Promise.resolve();
  respond(0, { ok: false, body: { error: "summary unavailable" } });
  await completion;
  const customFailure = visibleState(focusBefore);

  reset({ draft: { start: "2026-07-22", end: "2026-07-29" } });
  focusBefore = toggle.focusCalls;
  completion = trigger(form, "submit");
  await Promise.resolve();
  respond(0, { ok: true, body: summaryPayload("2026-07-23", "2026-07-29") });
  await completion;
  const customSuccess = visibleState(focusBefore);

  reset({ draft: { start: "2026-07-10", end: "2026-07-11" } });
  focusBefore = toggle.focusCalls;
  completion = trigger(presets[0], "click");
  await Promise.resolve();
  respond(0, { ok: false, body: { error: "summary unavailable" } });
  await completion;
  const presetFailure = visibleState(focusBefore);

  reset({ draft: { start: "2026-07-10", end: "2026-07-11" } });
  focusBefore = toggle.focusCalls;
  completion = trigger(presets[0], "click");
  await Promise.resolve();
  respond(0, { ok: true, body: summaryPayload("2026-07-23", "2026-07-29") });
  await completion;
  const presetSuccess = visibleState(focusBefore);

  reset({ draft: { start: "2026-07-22", end: "2026-07-29" } });
  focusBefore = toggle.focusCalls;
  completion = trigger(form, "submit");
  await Promise.resolve();
  respond(0, {
    ok: true,
    body: summaryPayload("2026-07-23", "2026-07-29", "generation-b"),
  });
  await completion;
  const generationSuccess = visibleState(focusBefore);

  reset({ routeReady: false });
  replaceCurrentRoute({
    window: { start: "2026-07-23", end: "2026-07-29" },
  });
  const ordinaryBeforeReady = {
    route: routeUrl(),
    routeWrites: [...routeWrites],
    routeReady: app.routeReady,
  };

  reset({
    draft: { start: "2026-07-22", end: "2026-07-29" },
    routeReady: false,
  });
  focusBefore = toggle.focusCalls;
  completion = trigger(form, "submit");
  await Promise.resolve();
  respond(0, { ok: true, body: summaryPayload("2026-07-23", "2026-07-29") });
  await completion;
  const initializingWorkspaceSuccess = {
    ...visibleState(focusBefore),
    routeReady: app.routeReady,
  };

  reset({ draft: { start: "2026-07-20", end: "2026-07-21" } });
  focusBefore = toggle.focusCalls;
  const olderCompletion = trigger(form, "submit");
  await Promise.resolve();
  setDraftTimeWindow({ start: "2026-07-23", end: "2026-07-29" });
  const newerCompletion = trigger(form, "submit");
  await Promise.resolve();
  respond(1, { ok: true, body: summaryPayload("2026-07-23", "2026-07-29") });
  await newerCompletion;
  respond(0, { ok: true, body: summaryPayload("2026-07-20", "2026-07-21") });
  await olderCompletion;
  const overlapping = visibleState(focusBefore);

  console.log(JSON.stringify({
    listenerCounts: {
      submit: form.listeners.submit?.length || 0,
      presets: presets.map((button) => button.listeners.click?.length || 0),
      toggle: toggle.listeners.click?.length || 0,
      cancel: cancel.listeners.click?.length || 0,
    },
    boundCustomOpened,
    boundCustomCancelled,
    customFailure,
    customSuccess,
    presetFailure,
    presetSuccess,
    generationSuccess,
    ordinaryBeforeReady,
    initializingWorkspaceSuccess,
    overlapping,
  }));
})();
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  buildScreenerPath(filters) {
    return `/screener?start=${encodeURIComponent(filters.start || "")}`
      + `&end=${encodeURIComponent(filters.end || "")}`;
  },
  buildWorkspacePath(token, page, state) {
    return `/tokens/${token}/${page}?start=${encodeURIComponent(state.start || "")}`
      + `&end=${encodeURIComponent(state.end || "")}`;
  },
  parseRoute(pathname, search) {
    const params = new URLSearchParams(search);
    const parts = pathname.split("/");
    return {
      kind: "workspace",
      token: parts[2] || "BTC",
      page: parts[3] || "markets",
      state: {
        start: params.get("start") || "",
        end: params.get("end") || "",
      },
    };
  },
};
""",
        )

        old_applied = {
            "payload": {"start": "2026-06-30", "end": "2026-07-29"},
            "summary": "30 Jun–29 Jul 2026 · 30 days",
            "active": ["30"],
            "pressed": {
                "7": "false",
                "30": "true",
                "90": "false",
                "all": "false",
            },
            "route": (
                "/tokens/BTC/markets?start=2026-06-30&end=2026-07-29"
            ),
        }
        new_applied = {
            "payload": {"start": "2026-07-23", "end": "2026-07-29"},
            "draft": {"start": "2026-07-23", "end": "2026-07-29"},
            "summary": "23–29 Jul 2026 · 7 days",
            "active": ["7"],
            "pressed": {
                "7": "true",
                "30": "false",
                "90": "false",
                "all": "false",
            },
            "editorHidden": True,
            "expanded": "false",
            "route": (
                "/tokens/BTC/markets?start=2026-07-23&end=2026-07-29"
            ),
            "routeWrites": [
                "/tokens/BTC/markets?start=2026-07-23&end=2026-07-29"
            ],
            "catalogLaunches": [
                "/tokens/BTC/markets?start=2026-07-23&end=2026-07-29"
            ],
        }
        self.assertEqual(result["listenerCounts"], {
            "submit": 1,
            "presets": [1, 1, 1, 1],
            "toggle": 1,
            "cancel": 1,
        })
        self.assertEqual(result["boundCustomOpened"], {
            "draft": {"start": "2026-06-30", "end": "2026-07-29"},
            "editorHidden": False,
            "expanded": "true",
            "startFocusCalls": 1,
        })
        self.assertEqual(result["boundCustomCancelled"], {
            "draft": {"start": "2026-06-30", "end": "2026-07-29"},
            "editorHidden": True,
            "expanded": "false",
            "toggleFocusCalls": 1,
        })
        self.assertEqual(result["customFailure"], {
            **old_applied,
            "draft": {"start": "2026-07-20", "end": "2026-07-21"},
            "editorHidden": False,
            "expanded": "true",
            "focusDelta": 0,
            "routeWrites": [],
            "routeDrafts": [],
            "requests": [
                "/api/markets/summary?start=2026-07-20&end=2026-07-21"
            ],
            "catalogLaunches": [
                "/tokens/BTC/markets?start=2026-06-30&end=2026-07-29"
            ],
        })
        self.assertEqual(result["customSuccess"], {
            **new_applied,
            "focusDelta": 1,
            "routeDrafts": [
                {"start": "2026-07-22", "end": "2026-07-29"}
            ],
            "requests": [
                "/api/markets/summary?start=2026-07-22&end=2026-07-29"
            ],
        })
        self.assertEqual(result["presetFailure"], {
            **old_applied,
            "draft": {"start": "2026-07-10", "end": "2026-07-11"},
            "editorHidden": False,
            "expanded": "true",
            "focusDelta": 0,
            "routeWrites": [],
            "routeDrafts": [],
            "requests": [
                "/api/markets/summary?start=2026-07-23&end=2026-07-29"
            ],
            "catalogLaunches": [
                "/tokens/BTC/markets?start=2026-06-30&end=2026-07-29"
            ],
        })
        self.assertEqual(result["presetSuccess"], {
            **new_applied,
            "focusDelta": 0,
            "routeDrafts": [
                {"start": "2026-07-10", "end": "2026-07-11"}
            ],
            "requests": [
                "/api/markets/summary?start=2026-07-23&end=2026-07-29"
            ],
        })
        self.assertEqual(result["generationSuccess"], {
            **new_applied,
            "focusDelta": 1,
            "routeDrafts": [
                {"start": "2026-07-22", "end": "2026-07-29"}
            ],
            "requests": [
                "/api/markets/summary?start=2026-07-22&end=2026-07-29"
            ],
        })
        self.assertEqual(result["ordinaryBeforeReady"], {
            "route": (
                "/tokens/BTC/markets?start=2026-06-30&end=2026-07-29"
            ),
            "routeWrites": [],
            "routeReady": False,
        })
        self.assertEqual(result["initializingWorkspaceSuccess"], {
            **new_applied,
            "focusDelta": 1,
            "routeDrafts": [
                {"start": "2026-07-22", "end": "2026-07-29"}
            ],
            "requests": [
                "/api/markets/summary?start=2026-07-22&end=2026-07-29"
            ],
            "routeReady": False,
        })
        self.assertEqual(result["overlapping"], {
            **new_applied,
            "focusDelta": 1,
            "routeDrafts": [
                {"start": "2026-07-23", "end": "2026-07-29"}
            ],
            "requests": [
                "/api/markets/summary?start=2026-07-20&end=2026-07-21",
                "/api/markets/summary?start=2026-07-23&end=2026-07-29",
            ],
        })

    def test_workspace_apply_commits_dates_to_original_route_before_missing_token_fallback(self):
        result = run_app_javascript(
            """
function control(value = "") {
  return {
    value,
    hidden: false,
    disabled: false,
    textContent: "",
    innerHTML: "",
    dataset: {},
    attributes: {},
    classList: { toggle() {}, contains() { return false; } },
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}
const controls = new Map([
  ["facts-token", control("BTC")],
  ["facts-market-a", control("cex:binance:BTC/USDT")],
  ["facts-market-b", control("dex:uniswap:BTC/USDC")],
  ["date-start", control("2026-07-20")],
  ["date-end", control("2026-07-21")],
]);
global.document = {
  getElementById(id) {
    if (!controls.has(id)) controls.set(id, control());
    return controls.get(id);
  },
  querySelector() { return null; },
  querySelectorAll() { return []; },
};
const writes = [];
global.window = {
  location: {
    pathname: "/tokens/BTC/compare",
    search: "?marketA=cex%3Abinance%3ABTC%2FUSDT&marketB=dex%3Auniswap%3ABTC%2FUSDC&start=2026-06-30&end=2026-07-29",
  },
  history: {
    replaceState(_state, _title, path) {
      writes.push(path);
      const [pathname, query = ""] = path.split("?");
      window.location.pathname = pathname;
      window.location.search = query ? `?${query}` : "";
    },
  },
  localStorage: { setItem() {}, removeItem() {} },
  lucide: null,
};

function summaryPayload() {
  return {
    metadata: {
      response_scope: "screener_summary",
      summary_version: 2,
      data_generation: "generation-b",
      start_date: "2026-07-23",
      end_date: "2026-07-29",
      available_start: "2026-05-01",
      available_end: "2026-07-29",
      default_workspace_token: "ETH",
      sources: [],
    },
    tokens: [{ token_symbol: "ETH", primary_cex: null, primary_dex: null }],
  };
}
global.fetch = async (url) => {
  if (url.startsWith("/api/markets/summary?")) {
    return { ok: true, status: 200, json: async () => summaryPayload() };
  }
  if (url.startsWith("/api/markets/catalog?")) {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        token_symbol: "ETH",
        metadata: {
          data_generation: "generation-b",
          window_start: "2026-07-23",
          window_end: "2026-07-29",
        },
        markets: [],
      }),
    };
  }
  throw new Error(`Unexpected request: ${url}`);
};

renderTable = () => { app.visibleTokens = [...app.payload.tokens]; };
updateMetadata = () => {};
setActiveAppView = () => {};
setActiveWorkspacePage = () => {};
setWorkspaceCatalogLoading = () => {};
setWorkspaceDataUnavailable = () => {};
announceRoute = () => {};
updateRouteLinks = () => {};
canonicalizeCurrentRoute = () => {};
let screenerFallbacks = 0;
let workspaceApplications = 0;
applyScreenerRoute = (route) => { screenerFallbacks += 1; app.route = route; };
applyWorkspaceRoute = (route) => { workspaceApplications += 1; app.route = route; };

app.payload = {
  metadata: {
    response_scope: "screener_summary",
    summary_version: 2,
    data_generation: "generation-a",
    start_date: "2026-06-30",
    end_date: "2026-07-29",
    available_start: "2026-05-01",
    available_end: "2026-07-29",
    default_workspace_token: "BTC",
  },
  tokens: [{ token_symbol: "BTC", primary_cex: null, primary_dex: null }],
};
app.route = navigation.parseRoute(window.location.pathname, window.location.search);
app.routeReady = true;

(async () => {
  const applied = await applyWindow({ start: "2026-07-22", end: "2026-07-29" });
  await Promise.resolve();
  await Promise.resolve();
  console.log(JSON.stringify({
    applied,
    writes,
    screenerFallbacks,
    workspaceApplications,
    route: app.route,
  }));
})();
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  parseRoute(pathname, search) {
    const params = new URLSearchParams(search);
    if (pathname === "/screener") {
      return {
        kind: "screener",
        filters: { start: params.get("start") || "", end: params.get("end") || "" },
      };
    }
    const parts = pathname.split("/");
    return {
      kind: "workspace",
      token: parts[2] || "",
      page: parts[3] || "markets",
      state: {
        marketA: params.get("marketA") || "",
        marketB: params.get("marketB") || "",
        start: params.get("start") || "",
        end: params.get("end") || "",
      },
    };
  },
  buildWorkspacePath(token, page, state) {
    const query = new URLSearchParams({
      marketA: state.marketA || "",
      marketB: state.marketB || "",
      start: state.start || "",
      end: state.end || "",
    });
    return `/tokens/${token}/${page}?${query.toString()}`;
  },
  buildScreenerPath(filters) {
    return `/screener?start=${filters.start || ""}&end=${filters.end || ""}`;
  },
};
""",
        )
        self.assertTrue(result["applied"])
        self.assertGreaterEqual(result["screenerFallbacks"], 1)
        self.assertEqual(result["workspaceApplications"], 0)
        self.assertTrue(result["writes"][0].startswith("/tokens/BTC/compare?"))
        self.assertIn("marketA=cex%3Abinance%3ABTC%2FUSDT", result["writes"][0])
        self.assertIn("marketB=dex%3Auniswap%3ABTC%2FUSDC", result["writes"][0])
        self.assertIn("start=2026-07-23", result["writes"][0])
        self.assertIn("end=2026-07-29", result["writes"][0])
        self.assertFalse(any(path.startswith("/tokens/ETH/") for path in result["writes"]))
        self.assertEqual(result["route"]["kind"], "screener")

    def test_summary_window_commit_route_hydration_preserves_an_open_draft(self):
        result = run_app_javascript(
            """
function control(dataset = {}) {
  return {
    value: "",
    dataset,
    hidden: false,
    textContent: "",
    innerHTML: "",
    attributes: {},
    active: false,
    classList: {
      owner: null,
      toggle(name, active) {
        if (name === "active") this.owner.active = active;
      },
    },
    setAttribute(name, value) { this.attributes[name] = value; },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}

const controls = new Map();
const presets = ["7", "30", "90", "all"].map((days) => control({ days }));
presets.forEach((item) => { item.classList.owner = item; });
for (const id of [
  "date-start",
  "date-end",
  "custom-window-toggle",
  "applied-window-summary",
]) {
  const item = control();
  item.classList.owner = item;
  controls.set(id, item);
}
global.document = {
  getElementById(id) {
    if (!controls.has(id)) {
      const item = control();
      item.classList.owner = item;
      controls.set(id, item);
    }
    return controls.get(id);
  },
  querySelectorAll(selector) {
    return selector === "[data-days]" ? presets : [];
  },
};

app.payload = {
  metadata: {
    start_date: "2026-06-30",
    end_date: "2026-07-29",
    available_start: "2026-05-01",
    available_end: "2026-07-29",
  },
  tokens: [{ token_symbol: "BTC" }],
};
app.catalog = { markets: [] };
app.pairSelections = {};
app.route = {
  kind: "workspace",
  token: "BTC",
  page: "markets",
  state: { start: "2026-06-30", end: "2026-07-29" },
};

syncScreenerSortControls = () => {};
renderTable = () => {};
syncMarketPayloadForWindow = () => {};
populateFactsMarkets = () => {};
setActiveAppView = () => {};
setActiveWorkspacePage = () => {};
renderWorkspaceContext = () => {};
renderWorkspaceMarkets = () => {};
renderQualityFromCatalog = () => {};
updateFactsContract = () => {};

const toggle = controls.get("custom-window-toggle");
const routeWindow = { start: "2026-07-10", end: "2026-07-11" };
const customDraft = { start: "2026-07-20", end: "2026-07-21" };

toggle.setAttribute("aria-expanded", "true");
setDraftTimeWindow(customDraft);
hydrateScreenerControls({
  kind: "screener",
  filters: { ...routeWindow },
});
const openScreener = draftTimeWindow();

toggle.setAttribute("aria-expanded", "false");
hydrateScreenerControls({
  kind: "screener",
  filters: { ...routeWindow },
});
const closedScreener = draftTimeWindow();

toggle.setAttribute("aria-expanded", "true");
setDraftTimeWindow(customDraft);
applyWorkspaceRoute({
  kind: "workspace",
  token: "BTC",
  page: "markets",
  state: { ...routeWindow },
});
const openWorkspace = draftTimeWindow();

toggle.setAttribute("aria-expanded", "false");
applyWorkspaceRoute({
  kind: "workspace",
  token: "BTC",
  page: "markets",
  state: { ...routeWindow },
});
const closedWorkspace = draftTimeWindow();

console.log(JSON.stringify({
  openScreener,
  closedScreener,
  openWorkspace,
  closedWorkspace,
}));
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  validatePair() {
    return { valid: false, errors: [], marketA: null, marketB: null };
  },
};
""",
        )
        self.assertEqual(result, {
            "openScreener": {"start": "2026-07-20", "end": "2026-07-21"},
            "closedScreener": {"start": "2026-07-10", "end": "2026-07-11"},
            "openWorkspace": {"start": "2026-07-20", "end": "2026-07-21"},
            "closedWorkspace": {"start": "2026-07-10", "end": "2026-07-11"},
        })

    def test_time_window_summary_and_active_state_use_applied_payload(self):
        result = run_app_javascript(
            """
function control(dataset = {}) {
  return {
    dataset,
    textContent: "",
    attributes: {},
    active: false,
    classList: {
      owner: null,
      toggle(name, enabled) {
        if (name === "active") this.owner.active = enabled;
      },
    },
    setAttribute(name, value) { this.attributes[name] = value; },
  };
}
const summary = control();
const custom = control();
summary.classList.owner = summary;
custom.classList.owner = custom;
const presets = ["7", "30", "90", "all"].map((days) => {
  const button = control({ days });
  button.classList.owner = button;
  return button;
});
global.document = {
  getElementById(id) {
    return {
      "applied-window-summary": summary,
      "custom-window-toggle": custom,
    }[id] || null;
  },
  querySelectorAll(selector) {
    return selector === "[data-days]" ? presets : [];
  },
};
app.payload = {
  metadata: {
    start_date: "2026-07-23",
    end_date: "2026-07-29",
    available_start: "2025-05-14",
    available_end: "2026-07-29",
  },
};
syncTimeWindowControls();
const presetState = {
  summary: summary.textContent,
  active: presets.find((button) => button.active)?.dataset.days,
  custom: custom.active,
};
app.payload.metadata.start_date = "2026-07-01";
syncTimeWindowControls();
console.log(JSON.stringify({
  presetState,
  customState: {
    activePreset: presets.find((button) => button.active)?.dataset.days || "",
    custom: custom.active,
    pressed: custom.attributes["aria-pressed"],
  },
}));
"""
        )
        self.assertEqual(result["presetState"]["summary"], "23–29 Jul 2026 · 7 days")
        self.assertEqual(result["presetState"]["active"], "7")
        self.assertFalse(result["presetState"]["custom"])
        self.assertEqual(result["customState"]["activePreset"], "")
        self.assertTrue(result["customState"]["custom"])
        self.assertEqual(result["customState"]["pressed"], "true")

    def test_full_short_available_range_activates_only_all_preset(self):
        result = run_app_javascript(
            """
function control(dataset = {}) {
  return {
    dataset,
    attributes: {},
    textContent: "",
    active: false,
    classList: {
      owner: null,
      toggle(name, enabled) {
        if (name === "active") this.owner.active = enabled;
      },
    },
    setAttribute(name, value) { this.attributes[name] = value; },
  };
}
const summary = control();
const custom = control();
summary.classList.owner = summary;
custom.classList.owner = custom;
const presets = ["7", "30", "90", "all"].map((days) => {
  const button = control({ days });
  button.classList.owner = button;
  return button;
});
global.document = {
  getElementById(id) {
    return {
      "applied-window-summary": summary,
      "custom-window-toggle": custom,
    }[id] || null;
  },
  querySelectorAll(selector) {
    return selector === "[data-days]" ? presets : [];
  },
};
app.payload = {
  metadata: {
    start_date: "2026-07-25",
    end_date: "2026-07-29",
    available_start: "2026-07-25",
    available_end: "2026-07-29",
  },
};
syncTimeWindowControls();
console.log(JSON.stringify({
  active: presets.filter((button) => button.active).map((button) => button.dataset.days),
  pressed: presets.filter((button) => button.attributes["aria-pressed"] === "true")
    .map((button) => button.dataset.days),
}));
"""
        )
        self.assertEqual(result["active"], ["all"])
        self.assertEqual(result["pressed"], ["all"])

    def test_custom_time_window_lifecycle_preserves_applied_state(self):
        result = run_app_javascript(
            """
function control({ value = "", hidden = false, dataset = {} } = {}) {
  return {
    value,
    hidden,
    dataset,
    disabled: false,
    textContent: "",
    attributes: {},
    focused: false,
    setAttribute(name, value) { this.attributes[name] = value; },
    getAttribute(name) { return this.attributes[name] || null; },
    focus() { this.focused = true; },
  };
}
const start = control();
const end = control();
const error = control({ hidden: true });
const editor = control({ hidden: true });
const toggle = control();
toggle.setAttribute("aria-expanded", "false");
const presets = ["7", "30", "90", "all"].map((days) => control({ dataset: { days } }));
const formButton = control();
const controls = [start, end, formButton, ...presets, toggle];
global.document = {
  getElementById(id) {
    return {
      "date-start": start,
      "date-end": end,
      "date-window-error": error,
      "custom-window-editor": editor,
      "custom-window-toggle": toggle,
    }[id] || null;
  },
  querySelectorAll(selector) {
    if (selector === "[data-days]") return presets;
    if (selector.includes("#date-window-form input")) return controls;
    return [];
  },
};
global.window = {
  location: { pathname: "/screener", search: "" },
  history: { replaceState() {} },
};
app.payload = {
  metadata: {
    start_date: "2026-07-01",
    end_date: "2026-07-29",
    available_start: "2026-05-01",
    available_end: "2026-07-29",
  },
};
app.route = { kind: "screener" };

if (typeof openCustomWindowEditor !== "function") {
  console.log(JSON.stringify({ missingLifecycle: true }));
} else {
  start.value = "2026-07-20";
  end.value = "2026-07-21";
  showDateWindowError("Old error");
  openCustomWindowEditor();
  const opened = {
    start: start.value,
    end: end.value,
    hidden: editor.hidden,
    expanded: toggle.getAttribute("aria-expanded"),
    errorHidden: error.hidden,
    startFocused: start.focused,
  };

  start.value = "2026-07-22";
  end.value = "2026-07-23";
  cancelCustomWindowEditor();
  const cancelled = {
    start: start.value,
    end: end.value,
    hidden: editor.hidden,
    expanded: toggle.getAttribute("aria-expanded"),
    toggleFocused: toggle.focused,
  };

  openCustomWindowEditor();
  start.value = "";
  const invalidApplied = await applyWindow();
  const invalid = {
    applied: invalidApplied,
    hidden: editor.hidden,
    errorHidden: error.hidden,
  };

  start.value = "2026-07-23";
  end.value = "2026-07-29";
  let loadedWindow = null;
  loadMarket = async (loadedStart, loadedEnd) => {
    loadedWindow = { start: loadedStart, end: loadedEnd };
    return true;
  };
  const screenerApplied = await applyWindow();

  app.route = { kind: "workspace" };
  let workspaceReloaded = false;
  applyRouteFromLocation = async () => {
    workspaceReloaded = true;
    return true;
  };
  const workspaceApplied = await applyWindow();

  const appliedWindow = appliedTimeWindow();
  setDateWindowDisabled(true);
  const disabled = controls.every((control) => control.disabled);
  setDateWindowDisabled(false);
  const enabled = controls.every((control) => !control.disabled);

  console.log(JSON.stringify({
    missingLifecycle: false,
    opened,
    cancelled,
    invalid,
    screenerApplied,
    loadedWindow,
    workspaceApplied,
    workspaceReloaded,
    appliedWindow,
    disabled,
    enabled,
  }));
}
"""
        )
        self.assertFalse(result["missingLifecycle"])
        self.assertEqual(result["opened"], {
            "start": "2026-07-01",
            "end": "2026-07-29",
            "hidden": False,
            "expanded": "true",
            "errorHidden": True,
            "startFocused": True,
        })
        self.assertEqual(result["cancelled"], {
            "start": "2026-07-01",
            "end": "2026-07-29",
            "hidden": True,
            "expanded": "false",
            "toggleFocused": True,
        })
        self.assertEqual(result["invalid"], {
            "applied": False,
            "hidden": False,
            "errorHidden": False,
        })
        self.assertTrue(result["screenerApplied"])
        self.assertEqual(result["loadedWindow"], {
            "start": "2026-07-23",
            "end": "2026-07-29",
        })
        self.assertTrue(result["workspaceApplied"])
        self.assertTrue(result["workspaceReloaded"])
        self.assertEqual(result["appliedWindow"], {
            "start": "2026-07-01",
            "end": "2026-07-29",
        })
        self.assertTrue(result["disabled"])
        self.assertTrue(result["enabled"])

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

    def test_token_catalog_loader_is_window_scoped_without_writing_route_cache(self):
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
        self.assertFalse(result["cached"])

    def test_token_catalog_generation_mismatch_preserves_route_cache(self):
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
        self.assertEqual(result["cacheSize"], 1)

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
        primer = app_js[
            app_js.index("function primeInitialRouteView(route)"):
            app_js.index("async function initialize()")
        ]
        self.assertIn("function primeInitialRouteView(route)", app_js)
        self.assertIn('setActiveAppView("workspace")', app_js)
        self.assertIn("setActiveWorkspacePage(route.page)", app_js)
        self.assertIn("setDraftTimeWindow(window);", primer)
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

    def test_time_window_uses_summary_presets_and_inline_custom_editor(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        self.assertIn('id="applied-window-summary"', index)
        self.assertIn('id="time-presets"', index)
        self.assertIn('id="custom-window-toggle"', index)
        self.assertIn('aria-controls="custom-window-editor"', index)
        self.assertIn('aria-expanded="false"', index)
        self.assertIn('id="custom-window-editor"', index)
        self.assertIn('id="cancel-window"', index)

        editor_start = index.index('id="custom-window-editor"')
        editor_end = index.index("</form>", editor_start)
        editor = index[editor_start:editor_end]
        self.assertIn('id="date-start"', editor)
        self.assertIn('id="date-end"', editor)
        self.assertIn('id="apply-window"', editor)
        self.assertIn("Apply custom range", editor)
        self.assertIn('id="date-window-error"', editor)

        self.assertIn(".time-toolbar-row", styles)
        self.assertIn(".custom-window-editor[hidden]", styles)
        mobile_start = styles.index("@media (max-width: 700px)")
        mobile = styles[mobile_start:]
        self.assertIn(".time-window-actions", mobile)
        self.assertIn(".custom-window-editor", mobile)
        self.assertIn(".custom-window-commands", mobile)

    def test_bound_apply_pair_click_persists_and_navigates_with_applied_window(self):
        result = run_app_javascript(
            """
function control(value = "") {
  return {
    value,
    hidden: false,
    disabled: false,
    textContent: "",
    innerHTML: "",
    dataset: {},
    attributes: {},
    listeners: {},
    addEventListener(type, listener) {
      this.listeners[type] = this.listeners[type] || [];
      this.listeners[type].push(listener);
    },
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] || null; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}

const controls = new Map([
  ["facts-token", control("BTC")],
  ["facts-market-a", control("cex:binance:BTC/USDT")],
  ["facts-market-b", control("dex:uniswap:BTC/USDC")],
  ["compare-markets", control()],
]);
global.document = {
  getElementById(id) {
    if (!controls.has(id)) controls.set(id, control());
    return controls.get(id);
  },
  querySelector() { return null; },
  querySelectorAll() { return []; },
  addEventListener() {},
};
const pushes = [];
let stored = null;
global.window = {
  location: { pathname: "/tokens/BTC/markets", search: "" },
  history: {
    pushState(_state, _title, path) { pushes.push(path); },
    replaceState() {},
  },
  sessionStorage: {
    getItem() { return null; },
    setItem(_key, value) { stored = JSON.parse(value); },
  },
  addEventListener() {},
  visualViewport: null,
  matchMedia() { return { addEventListener() {} }; },
  queueMicrotask(callback) { callback(); },
  scrollTo() {},
};
app.payload = {
  metadata: {
    start_date: "2026-07-23",
    end_date: "2026-07-29",
    available_start: "2026-05-01",
    available_end: "2026-07-29",
  },
  tokens: [],
};
app.route = {
  kind: "workspace",
  token: "BTC",
  page: "markets",
  state: { start: "2026-07-23", end: "2026-07-29" },
};
app.routeReady = true;
let routeCalls = 0;
applyRouteFromLocation = async () => { routeCalls += 1; return true; };
bindEvents();
for (const listener of controls.get("compare-markets").listeners.click || []) {
  listener({});
}
console.log(JSON.stringify({ pushes, stored, routeCalls }));
""",
            prelude="""
globalThis.MarketMonitorNavigation = {
  buildWorkspacePath(token, page, state) {
    const query = new URLSearchParams({
      marketA: state.marketA,
      marketB: state.marketB,
      start: state.start,
      end: state.end,
    });
    return `/tokens/${token}/${page}?${query.toString()}`;
  },
};
""",
        )
        self.assertEqual(result["routeCalls"], 1)
        self.assertEqual(result["stored"], {
            "BTC": {
                "marketA": "cex:binance:BTC/USDT",
                "marketB": "dex:uniswap:BTC/USDC",
            },
        })
        self.assertEqual(len(result["pushes"]), 1)
        path = result["pushes"][0]
        self.assertTrue(path.startswith("/tokens/BTC/compare?"))
        self.assertIn("marketA=cex%3Abinance%3ABTC%2FUSDT", path)
        self.assertIn("marketB=dex%3Auniswap%3ABTC%2FUSDC", path)
        self.assertIn("start=2026-07-23", path)
        self.assertIn("end=2026-07-29", path)

    def test_date_error_is_inline_only_and_updates_input_accessibility_state(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        form_start = index.index('<form id="date-window-form"')
        form_end = index.index("</form>", form_start)
        form = index[form_start:form_end]
        self.assertEqual(form.count('aria-describedby="date-window-error"'), 2)
        self.assertEqual(form.count('aria-invalid="false"'), 2)

        app_js = APP_PATH.read_text(encoding="utf-8")
        apply_window = app_js[
            app_js.index("async function applyWindow("):
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
            app_js.index("async function applyWindow(")
        ]

        self.assertIn("if (app.marketController)", router)
        self.assertIn("invalidateMarketRequest();", router)
        self.assertIn('byId("export-csv").disabled = !app.payload;', router)
        self.assertIn("const loaded = await loadMarketForRoute(", router)
        self.assertIn("!marketPayloadMatchesWindow(", router)
        self.assertEqual(router.count("compareRouteWindow(route)"), 2)
        self.assertNotIn('route.page === "compare"', router)
        self.assertGreaterEqual(router.count("setWorkspaceDataUnavailable("), 3)
        self.assertIn('setAttribute("aria-busy", "false")', unavailable)
        self.assertNotIn("Loading", unavailable)
        self.assertIn("formatRatio(row?.coverage_ratio)", workspace_markets)
        self.assertNotIn("formatRatio(market.coverage_ratio)", workspace_markets)
        self.assertIn('byId("export-csv").disabled = true;', loader)
        self.assertNotIn('byId("date-start").value =', loader)
        self.assertIn("syncClosedDraftToApplied();", app_js)

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
            app_js.index("function replaceCurrentRoute(")
        ]

        self.assertIn("return appliedTimeWindow();", summary_state)
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


if __name__ == "__main__":
    unittest.main()
