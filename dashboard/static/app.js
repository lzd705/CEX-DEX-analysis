const app = {
  payload: null,
  defaultPayload: null,
  defaultPayloadIsCached: false,
  catalog: null,
  catalogsByToken: new Map(),
  activeCatalogToken: "",
  activeCatalogKey: "",
  comparison: null,
  execution: null,
  eventFacts: null,
  quality: null,
  opportunities: null,
  scope: "combined",
  sortDirection: "desc",
  workspaceMarketType: "all",
  qualityScope: "all",
  qualitySeverity: "",
  qualityOrigin: "",
  liquidityView: "total",
  liquidityScale: "log",
  comparisonMetric: "price",
  eventLifecycle: "all",
  eventClockState: "all",
  liquidityEffectiveScale: null,
  liquidityEffectiveScaleLabel: "",
  executionDirection: "buy_token",
  executionNotionalUsd: 10000,
  route: { kind: "screener", filters: {} },
  pairSelections: {},
  pairSelectionSource: "",
  workspaceSelection: "",
  workspaceSelectionInvalid: false,
  routeReady: false,
  selections: {},
  selectionOverrides: {},
  searchQuery: "",
  visibleTokens: [],
  marketRequestId: 0,
  routeRequestId: 0,
  comparisonRequestId: 0,
  executionRequestId: 0,
  qualityRequestId: 0,
  eventRequestId: 0,
  snapshotRefreshRequestId: 0,
  opportunityRequestId: 0,
  marketController: null,
  catalogController: null,
  marketRequestWindowKey: "",
  comparisonController: null,
  comparisonRequestKey: "",
  executionController: null,
  executionRequestKey: "",
  qualityController: null,
  qualityRequestKey: "",
  eventController: null,
  eventRequestKey: "",
  snapshotRefreshController: null,
  opportunityController: null,
  liquidityLayoutMode: null,
  liquidityResizeScheduled: false,
  liquidityResizeObserver: null,
  comparisonChartLayoutMode: null,
  comparisonChartActiveIndex: 0,
  comparisonChartResizeScheduled: false,
  comparisonChartResizeObserver: null,
};

const DEFAULT_MARKET_CACHE_KEY = "market-monitor:screener-summary:v3";
const TOKEN_PAIR_CACHE_KEY = "market-monitor:token-pairs:v1";
const navigation = globalThis.MarketMonitorNavigation;
const DEPTH_BANDS = [10, 25, 50, 100];
const MEASURED_DEPTH_STATUSES = new Set(["observed", "complete", "partial"]);
const LIQUIDITY_CHART = {
  width: 760,
  height: 360,
  left: 78,
  right: 24,
  top: 24,
  bottom: 302,
};
const COMPARISON_CHART = {
  width: 900,
  height: 360,
  left: 82,
  right: 24,
  top: 30,
  bottom: 302,
};
const QUALITY_FLAG_LABELS = {
  depth_unavailable: "Depth unavailable",
  depth_unsupported: "Depth unsupported",
  unsupported_depth: "Depth unsupported",
  depth_partial: "Partial depth",
  partial_depth: "Partial depth",
  depth_failed: "Depth failed",
  failed_depth: "Depth failed",
  depth_not_cataloged: "Depth not cataloged",
  zero_depth_10bps: "No depth inside 10 bps",
  zero_depth_inside_spread: "Band lies inside spread",
  tiny_pool: "Tiny pool",
  off_market_pool_state_price: "Off-market pool price",
  off_market_price: "Off-market pool price",
  wide_quoted_spread: "Wide quoted spread",
  low_daily_coverage: "Low daily coverage",
  stale_snapshot: "Stale snapshot",
  daily_collection_failed: "Daily collection failed",
  daily_needs_review: "Daily source outcome needs review",
  daily_backfill_pending: "Daily backfill pending",
  daily_source_no_observation: "Daily source returned no candle",
  daily_unsupported: "Daily source history unsupported",
};
const QUALITY_FLAG_DEFAULT_SEVERITIES = {
  depth_unavailable: "info",
  depth_unsupported: "info",
  unsupported_depth: "info",
  depth_partial: "info",
  partial_depth: "info",
  depth_failed: "critical",
  failed_depth: "critical",
  depth_not_cataloged: "info",
  zero_depth_10bps: "info",
  zero_depth_inside_spread: "info",
  tiny_pool: "info",
  off_market_pool_state_price: "warning",
  off_market_price: "warning",
  wide_quoted_spread: "info",
  low_daily_coverage: "warning",
  stale_snapshot: "warning",
  daily_collection_failed: "critical",
  daily_needs_review: "warning",
  daily_backfill_pending: "warning",
  daily_source_no_observation: "info",
  daily_unsupported: "info",
};
const DAILY_QUALITY_REASON_LABELS = {
  network: "Network request failed",
  rate_limit: "Source rate limit",
  source_unavailable: "Source unavailable",
  parse: "Response parsing failed",
  validation: "Response validation failed",
  no_candles: "Source returned no candle",
  not_listed: "Instrument not listed",
  source_range_unavailable: "Requested source history unavailable",
  stale_market_lifecycle_unknown: "Market lifecycle needs review",
  missing_unexplained: "No matching collection attempt",
  daily_audit_no_matching_issue: "No exact published audit issue",
  source_no_two_sided_book: "Source has no two-sided order book",
  source_no_order_book: "Source returned no order book",
  source_invalid_order_book: "Source returned an invalid order book",
  source_rejected_request: "Source rejected the request",
  unsupported_source: "Source is unsupported",
  collection_failed: "Collection failed",
  depth_usd_price_time_mismatch: "Pool state and USD price are not time-aligned",
  source_no_tvl_observation: "Source returned no TVL observation",
  source_pool_not_found: "Pool was not returned by the TVL source",
  tvl_snapshot_unavailable: "TVL snapshot unavailable",
  depth_snapshot_unavailable: "Depth snapshot unavailable",
  tvl_market_not_cataloged_in_snapshot: "Market not included in the TVL snapshot",
  depth_market_not_cataloged_in_snapshot: "Market not included in the depth snapshot",
  measurement_limit: "Published depth is a measured lower bound",
  source_level_limit: "Published depth is a lower bound",
  target_filled: "Requested execution size was filled",
  full_book_insufficient_liquidity: "Full book cannot fill the requested size",
  execution_snapshot_unavailable: "Execution snapshot unavailable",
  execution_snapshot_invalid: "Execution snapshot invalid",
  execution_market_not_cataloged_in_snapshot: "Market not included in the published execution snapshot",
  instrument_absent_from_current_catalog: "Instrument absent from the official current exchange catalog",
  official_catalog_evidence_stale: "Official catalog evidence is older than 36 hours",
  unsupported_protocol_or_chain: "Execution not supported for this protocol or chain",
  unsupported_protocol: "Execution not supported for this protocol",
  unsupported_chain: "Execution not supported for this chain",
  unsupported_method: "Execution method is unsupported",
};
const QUALITY_SEVERITY_RANK = { info: 1, warning: 2, critical: 3 };
const SCREENER_SORT_DEFINITIONS = Object.freeze({
  volume: Object.freeze({
    label: "USD Volume",
    allowedScopes: Object.freeze(["combined", "cex", "dex"]),
    defaultScope: "combined",
    snapshot: false,
  }),
  spread: Object.freeze({
    label: "Latest Daily Price Gap",
    allowedScopes: Object.freeze(["cross"]),
    defaultScope: "cross",
    snapshot: false,
  }),
  spread_max: Object.freeze({
    label: "Maximum Daily Price Gap",
    allowedScopes: Object.freeze(["cross"]),
    defaultScope: "cross",
    snapshot: false,
  }),
  spread_mean: Object.freeze({
    label: "Average Daily Price Gap",
    allowedScopes: Object.freeze(["cross"]),
    defaultScope: "cross",
    snapshot: false,
  }),
  spread_median: Object.freeze({
    label: "Median Daily Price Gap",
    allowedScopes: Object.freeze(["cross"]),
    defaultScope: "cross",
    snapshot: false,
  }),
  return: Object.freeze({
    label: "Window Return",
    allowedScopes: Object.freeze(["cex", "dex"]),
    defaultScope: "cex",
    snapshot: false,
  }),
  volatility: Object.freeze({
    label: "Daily Volatility",
    allowedScopes: Object.freeze(["cex", "dex"]),
    defaultScope: "cex",
    snapshot: false,
  }),
  depth_100bps: Object.freeze({
    label: "Primary ±100 bps Depth",
    allowedScopes: Object.freeze(["cex", "dex"]),
    defaultScope: "cex",
    snapshot: true,
  }),
  dex_tvl: Object.freeze({
    label: "Primary DEX TVL",
    allowedScopes: Object.freeze(["dex"]),
    defaultScope: "dex",
    snapshot: true,
  }),
});
const OPPORTUNITY_REASON_LABELS = Object.freeze({
  complete_pointer_absent: "The complete opportunity bundle is not published.",
  complete_bundle_published: "The complete opportunity bundle is published.",
  positive_strict_net_edge: "Every strict route and publication gate passed.",
  route_deadline_exceeded: "The synchronized route collection deadline expired.",
  execution_adapter_unsupported: "At least one route leg lacks a supported execution adapter.",
  buy_leg_unavailable: "The buy-leg execution observation is unavailable.",
  sell_leg_unavailable: "The sell-leg execution observation is unavailable.",
  leg_not_completely_filled: "At least one route leg could not fill the common Token quantity.",
  invalid_state_timestamp: "A route-leg timestamp is invalid.",
  snapshot_skew_exceeded: "Snapshot skew exceeds the synchronized route SLA.",
  common_quantity_unavailable: "The route legs do not share a proved Token quantity.",
  quantity_quote_evidence_mismatch: "Quantity-quote evidence could not be replayed exactly.",
  usd_conversion_unavailable: "A route-leg USD conversion is unavailable.",
  cohort_stale: "The synchronized route cohort is older than the strict freshness SLA.",
  unsupported_cross_chain_settlement: "Cross-chain settlement is not proved for this route.",
  atomic_route_simulation_unavailable: "Atomic route simulation evidence is unavailable.",
  mode_expected_request_unavailable: "Required route-mode request evidence is unavailable.",
  inventory_unavailable: "Pre-positioned inventory evidence is unavailable.",
  inventory_request_mismatch: "Inventory evidence does not match this route request.",
  inventory_insufficient: "Proved inventory is insufficient for the common quantity.",
  dex_buy_quantity_quote_unavailable: "The DEX buy leg has no quantity-specific quote evidence.",
  dex_buy_authoritative_upstream_unavailable: "The DEX buy quote lacks authoritative upstream evidence.",
  rebalance_transfer_evidence_unavailable: "Rebalance or transfer evidence is unavailable.",
  quantity_quote_evidence_not_strict: "Quantity-quote evidence is estimate-only.",
  usd_conversion_not_strict: "USD conversion evidence is estimate-only.",
  cost_components_incomplete: "Required route costs are incomplete.",
  cost_component_stale: "At least one route cost component is stale.",
  cost_component_estimated: "At least one route cost component is estimated.",
  non_positive_net_edge: "The proved net edge is not positive.",
  publication_evidence_unverified: "Strict publication attestation is not verified.",
});

const byId = (id) => document.getElementById(id);
const compactCurrency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 2,
});
const priceFormat = new Intl.NumberFormat("en-US", { maximumSignificantDigits: 7 });
const percent = new Intl.NumberFormat("en-US", {
  style: "percent",
  signDisplay: "exceptZero",
  maximumFractionDigits: 2,
});
const unsignedPercent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 2,
});
const rawUsd = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 12,
  maximumSignificantDigits: 12,
});
const rawVolume = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});
const bpsFormat = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 4,
});
function finite(value) {
  return Number.isFinite(value);
}

function firstFinite(...values) {
  return values.find((value) => finite(value));
}

function sumFinite(rows, field) {
  return rows.reduce((total, row) => total + (finite(row?.[field]) ? row[field] : 0), 0);
}

function formatPrice(value) {
  return finite(value) ? `$${priceFormat.format(value)}` : "N/A";
}

function formatCurrency(value) {
  if (!finite(value)) return "N/A";
  if (value !== 0 && Math.abs(value) < 1) return `$${rawUsd.format(value)}`;
  return compactCurrency.format(value);
}

function formatDepth(value, complete) {
  if (!finite(value)) return "N/A";
  return `${complete ? "" : "≥"}${formatCurrency(value)}`;
}

function formatPercent(value) {
  return finite(value) ? percent.format(value) : "N/A";
}

function formatShare(value) {
  return finite(value) ? unsignedPercent.format(value) : "N/A";
}

function formatRatio(value) {
  return finite(value)
    ? new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 }).format(value)
    : "N/A";
}

function formatQualityRatio(value) {
  return finite(value)
    ? new Intl.NumberFormat("en-US", {
        style: "percent",
        maximumFractionDigits: 4,
      }).format(value)
    : "N/A";
}

function formatQualityUsd(value) {
  return finite(value) ? `$${rawUsd.format(value)}` : "N/A";
}

function formatRawUsd(value) {
  return finite(value) ? `$${rawUsd.format(value)}` : "N/A";
}

function formatRawVolume(value) {
  return finite(value) ? `$${rawVolume.format(value)}` : "N/A";
}

function formatUtcTimestamp(value) {
  if (!value) return "time unavailable";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return `${parsed.toISOString().replace("T", " ").slice(0, 19)} UTC`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function grouped(rows) {
  return rows.reduce((result, row) => {
    if (!result[row.token_symbol]) result[row.token_symbol] = [];
    result[row.token_symbol].push(row);
    return result;
  }, {});
}

function marketId(row) {
  return row.market === "cex" || row.market_type === "cex"
    ? `${row.venue}|${row.instrument}`
    : row.pool_address;
}

function readPairSelections() {
  try {
    const value = JSON.parse(window.sessionStorage.getItem(TOKEN_PAIR_CACHE_KEY));
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  } catch {
    return {};
  }
}

function writePairSelections() {
  try {
    window.sessionStorage.setItem(TOKEN_PAIR_CACHE_KEY, JSON.stringify(app.pairSelections));
  } catch {
    // URL state remains authoritative when browser storage is unavailable.
  }
}

function normalizedSavedSelection(record) {
  if (!record || typeof record !== "object") return null;
  if (record.selection === "single" && record.marketA && !record.marketB) {
    return { marketA: record.marketA, marketB: "", selection: "single" };
  }
  if (!record.selection && record.marketA && record.marketB) {
    return { marketA: record.marketA, marketB: record.marketB, selection: "" };
  }
  return null;
}

function selectedMarketSelection() {
  return {
    marketA: byId("facts-market-a")?.value || "",
    marketB: byId("facts-market-b")?.value || "",
    selection: app.workspaceSelection === "single" ? "single" : "",
  };
}

function selectedPairState() {
  const { marketA, marketB } = selectedMarketSelection();
  return { marketA, marketB };
}

function validateWorkspaceSelection(markets, marketA, marketB, selection = "") {
  if (typeof navigation?.validateSelection === "function") {
    return navigation.validateSelection(markets, marketA, marketB, selection);
  }
  const wantsSingle = selection === "single";
  const valid = Boolean(
    marketA
    && (wantsSingle ? !marketB : marketB && marketA !== marketB),
  );
  const resolve = (id) => markets.find((market) => market.market_id === id) || { market_id: id };
  return {
    valid,
    mode: valid ? (wantsSingle ? "single" : "pair") : null,
    marketA: marketA ? resolve(marketA) : null,
    marketB: !wantsSingle && marketB ? resolve(marketB) : null,
    errors: valid ? [] : [{ code: "selection_invalid" }],
  };
}

function selectedWorkspaceToken() {
  return byId("facts-token")?.value
    || app.payload?.metadata?.default_workspace_token
    || app.payload?.tokens?.[0]?.token_symbol
    || "";
}

function applyWorkspaceSelectionMode(mode) {
  const single = mode === "single";
  const workbench = byId("facts-workbench");
  if (workbench) workbench.dataset.selectionMode = single ? "single" : "pair";
  document.querySelectorAll?.("[data-pair-only]").forEach((element) => {
    element.hidden = single;
  });
  document.querySelectorAll?.("[data-single-only]").forEach((element) => {
    element.hidden = !single;
  });
  if (single && app.comparisonMetric === "spread") {
    app.comparisonMetric = "price";
  }
  document.querySelectorAll?.("[data-comparison-metric]").forEach((button) => {
    const active = button.dataset.comparisonMetric === app.comparisonMetric;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
}

function workspaceRequestKey(page, selection, extra = {}) {
  const window = appliedTimeWindow();
  return JSON.stringify({
    page,
    token: selectedWorkspaceToken(),
    marketA: selection.marketA,
    marketB: selection.marketB,
    selection: selection.selection,
    start: window.start,
    end: window.end,
    ...extra,
  });
}

function tokenCatalogCacheKey(token, start, end, generation) {
  return [token, start || "", end || "", generation || ""].join("|");
}

function cachedTokenCatalog(cacheKey) {
  const catalog = app.catalogsByToken.get(cacheKey);
  if (!catalog) return null;
  app.catalogsByToken.delete(cacheKey);
  app.catalogsByToken.set(cacheKey, catalog);
  return catalog;
}

function cacheTokenCatalog(cacheKey, catalog) {
  if (app.catalogsByToken.has(cacheKey)) app.catalogsByToken.delete(cacheKey);
  app.catalogsByToken.set(cacheKey, catalog);
  while (app.catalogsByToken.size > 8) {
    app.catalogsByToken.delete(app.catalogsByToken.keys().next().value);
  }
}

function currentScreenerFilters({ window = appliedTimeWindow() } = {}) {
  const filters = {
    q: byId("token-search")?.value.trim() || "",
    scope: app.scope,
    sort: byId("sort-field")?.value || "volume",
    dir: app.sortDirection,
    start: window.start || "",
    end: window.end || "",
  };
  if (filters.scope === "combined") delete filters.scope;
  if (filters.sort === "volume") delete filters.sort;
  if (filters.dir === "desc") delete filters.dir;
  const metadata = app.payload?.metadata || app.defaultPayload?.metadata;
  const defaultWindow = normalizedMarketWindow("", "");
  if (
    metadata
    && filters.start === defaultWindow.start
    && filters.end === defaultWindow.end
  ) {
    delete filters.start;
    delete filters.end;
  }
  return filters;
}

function currentWorkspaceRouteState(
  page,
  { window = appliedTimeWindow() } = {},
) {
  const state = selectedMarketSelection();
  if (app.route?.kind === "workspace" && !app.catalog) {
    state.marketA ||= app.route.state?.marketA || "";
    state.marketB ||= app.route.state?.marketB || "";
    state.selection ||= app.route.state?.selection === "single" ? "single" : "";
  }
  if (app.pairSelectionSource === "transient") state.pairMode = "transient";
  if (!state.marketA || (!state.marketB && state.selection !== "single")) {
    state.pairMode = "manual";
  }
  state.start = window.start || "";
  state.end = window.end || "";
  if (page === "liquidity") {
    state.side = app.executionDirection === "sell_token" ? "sell" : "buy";
    state.notionalUsd = app.executionNotionalUsd;
    state.view = app.liquidityView;
    state.scale = app.liquidityScale;
  } else if (page === "quality") {
    state.scope = effectiveQualityScope(state);
    if (app.qualitySeverity) state.severity = app.qualitySeverity;
    if (app.qualityOrigin) state.origin = app.qualityOrigin;
  } else if (page === "events") {
    state.lifecycle = app.eventLifecycle;
    state.clockState = app.eventClockState;
  }
  return state;
}

function currentWorkspacePath(
  page = app.route?.page || "markets",
  { window = appliedTimeWindow() } = {},
) {
  if (!navigation) return "/screener";
  return navigation.buildWorkspacePath(
    selectedWorkspaceToken(),
    page,
    currentWorkspaceRouteState(page, { window }),
  );
}

function currentSummaryWindowRouteState() {
  return appliedTimeWindow();
}

function workspaceEntryRouteState(page) {
  if (app.route?.kind === "screener") {
    return currentSummaryWindowRouteState();
  }
  return currentWorkspaceRouteState(page);
}

function updateRouteLinks() {
  const token = (
    app.route?.kind === "workspace"
      ? String(app.route.token || "").toUpperCase()
      : selectedWorkspaceToken()
  ) || "AAVE";
  if (navigation) {
    const marketsLink = document.querySelector('[data-app-route="markets"]');
    const researchLink = document.querySelector('[data-app-route="research"]');
    if (marketsLink) {
      marketsLink.href = navigation.buildWorkspacePath(
        token,
        "markets",
        workspaceEntryRouteState("markets"),
      );
    }
    if (researchLink) {
      const researchPage = app.route?.kind === "workspace" && app.route.page !== "markets"
        ? app.route.page
        : "compare";
      researchLink.href = navigation.buildWorkspacePath(
        token,
        researchPage,
        workspaceEntryRouteState(researchPage),
      );
    }
  }
  document.querySelectorAll("[data-workspace-page]").forEach((link) => {
    const page = link.dataset.workspacePage;
    if (navigation) {
      const state = currentWorkspaceRouteState(page);
      if (link.classList.contains("warning-quality-link")) state.scope = "selected";
      link.href = navigation.buildWorkspacePath(token, page, state);
    }
    const active = app.route?.kind === "workspace" && app.route.page === page;
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  document.querySelectorAll("[data-app-route]").forEach((link) => {
    const activeRoute = app.route?.kind !== "workspace"
      ? app.route?.kind
      : app.route.page === "markets"
        ? "markets"
        : "research";
    const active = link.dataset.appRoute === activeRoute;
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  const back = byId("back-to-screener");
  if (back && navigation) back.href = navigation.buildScreenerPath(currentScreenerFilters());
  const changeMarkets = byId("change-markets");
  if (changeMarkets && navigation) {
    changeMarkets.href = navigation.buildWorkspacePath(
      token,
      "markets",
      currentWorkspaceRouteState("markets"),
    );
  }
}

function replaceCurrentRoute({
  window = appliedTimeWindow(),
  allowBeforeReady = false,
} = {}) {
  if (!navigation || (!app.routeReady && !allowBeforeReady)) return;
  if (
    app.route?.kind === "opportunities"
    && Array.isArray(app.route.validationErrors)
    && app.route.validationErrors.length
  ) return;
  if (app.route?.kind === "workspace" && app.workspaceSelectionInvalid) return;
  let path;
  if (app.route.kind === "workspace") {
    path = currentWorkspacePath(app.route.page, { window });
  } else if (app.route.kind === "opportunities") {
    path = navigation.buildOpportunitiesPath(app.route.filters || {});
  } else {
    path = navigation.buildScreenerPath(currentScreenerFilters({ window }));
  }
  const current = `${globalThis.window.location.pathname}${globalThis.window.location.search}`;
  if (path !== current) invalidateSnapshotRefreshRequest({ clearFeedback: true });
  globalThis.window.history.replaceState({}, "", path);
  app.route = navigation.parseRoute(
    globalThis.window.location.pathname,
    globalThis.window.location.search,
  );
  updateRouteLinks();
}

function navigateTo(path, { replace = false } = {}) {
  invalidateSnapshotRefreshRequest({ clearFeedback: true });
  if (replace) window.history.replaceState({}, "", path);
  else window.history.pushState({}, "", path);
  applyRouteFromLocation();
  window.scrollTo({ top: 0, left: 0, behavior: "auto" });
}

function canonicalizeCurrentRoute() {
  if (!navigation || !app.routeReady) return;
  if (
    app.route?.kind === "opportunities"
    && Array.isArray(app.route.validationErrors)
    && app.route.validationErrors.length
  ) return;
  if (app.route?.kind === "workspace" && app.workspaceSelectionInvalid) return;
  let path;
  if (app.route.kind === "workspace") {
    path = currentWorkspacePath(app.route.page);
  } else if (app.route.kind === "opportunities") {
    path = navigation.buildOpportunitiesPath(app.route.filters || {});
  } else {
    path = navigation.buildScreenerPath(currentScreenerFilters());
  }
  const current = `${window.location.pathname}${window.location.search}`;
  if (path !== current) window.history.replaceState({}, "", path);
  app.route = navigation.parseRoute(window.location.pathname, window.location.search);
}

function setActiveAppView(kind) {
  const viewKind = kind === "workspace" ? "workspace" : kind;
  document.querySelectorAll("[data-app-view]").forEach((view) => {
    view.hidden = view.dataset.appView !== viewKind;
  });
}

function setActiveWorkspacePage(page) {
  if (page !== "compare") invalidateComparisonRequest();
  if (page !== "liquidity") invalidateExecutionRequest();
  if (page !== "quality") invalidateQualityRequest();
  if (page !== "events") invalidateEventRequest();
  document.querySelectorAll("[data-workspace-view]").forEach((view) => {
    view.hidden = view.dataset.workspaceView !== page;
  });
  const compareVisible = page === "compare";
  byId("comparison-status").hidden = !compareVisible;
  if (!compareVisible) byId("comparison-error").hidden = true;
  const isMarkets = page === "markets";
  byId("time-toolbar").hidden = page === "events" || page === "liquidity";
  byId("facts-workbench").classList.toggle("markets-page", isMarkets);
  byId("research-pair-context").hidden = isMarkets;
  document.querySelector(".facts-controls").hidden = !isMarkets;
  byId("selector-policy").hidden = !isMarkets;
}

function syncSegmentedControls() {
  const groups = [
    ["[data-workspace-market-type]", "workspaceMarketType"],
    ["[data-liquidity-view]", "liquidityView"],
    ["[data-liquidity-scale]", "liquidityScale"],
    ["[data-comparison-metric]", "comparisonMetric"],
    ["[data-event-lifecycle]", "eventLifecycle"],
    ["[data-event-clock-state]", "eventClockState"],
    ["[data-execution-direction]", "executionDirection"],
    ["[data-quality-scope]", "qualityScope"],
  ];
  groups.forEach(([selector, stateKey]) => {
    document.querySelectorAll(selector).forEach((button) => {
      const key = Object.keys(button.dataset).find((name) => (
        [
          "workspaceMarketType",
          "liquidityView",
          "liquidityScale",
          "comparisonMetric",
          "eventLifecycle",
          "eventClockState",
          "executionDirection",
          "qualityScope",
        ]
          .includes(name)
      ));
      const active = key ? button.dataset[key] === app[stateKey] : false;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
  });
}

function draftTimeWindow() {
  return {
    start: byId("date-start")?.value || "",
    end: byId("date-end")?.value || "",
  };
}

function setDraftTimeWindow({ start = "", end = "" } = {}) {
  byId("date-start").value = start;
  byId("date-end").value = end;
}

function appliedTimeWindow() {
  const metadata = app.payload?.metadata || app.defaultPayload?.metadata;
  if (metadata?.start_date && metadata?.end_date) {
    return { start: metadata.start_date, end: metadata.end_date };
  }
  if (app.route?.kind === "workspace") {
    return {
      start: app.route.state?.start || "",
      end: app.route.state?.end || "",
    };
  }
  return {
    start: app.route?.filters?.start || "",
    end: app.route?.filters?.end || "",
  };
}

function customWindowIsOpen() {
  return byId("custom-window-toggle")?.getAttribute("aria-expanded") === "true";
}

function syncClosedDraftToApplied() {
  if (!customWindowIsOpen()) setDraftTimeWindow(appliedTimeWindow());
}

function presetWindow(days) {
  const availableStart = app.payload?.metadata?.available_start || "";
  const availableEnd = app.payload?.metadata?.available_end || "";
  if (!availableStart || !availableEnd) return { start: "", end: "" };
  if (days === "all") return { start: availableStart, end: availableEnd };
  const startDate = new Date(`${availableEnd}T00:00:00Z`);
  startDate.setUTCDate(startDate.getUTCDate() - Number(days) + 1);
  const candidate = startDate.toISOString().slice(0, 10);
  return {
    start: candidate < availableStart ? availableStart : candidate,
    end: availableEnd,
  };
}

function formatAppliedWindowSummary(start, end) {
  const startDate = new Date(`${start}T00:00:00Z`);
  const endDate = new Date(`${end}T00:00:00Z`);
  if (
    !start
    || !end
    || Number.isNaN(startDate.getTime())
    || Number.isNaN(endDate.getTime())
  ) return "No applied range";

  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const inclusiveDays = Math.round(
    (endDate.getTime() - startDate.getTime()) / 86_400_000,
  ) + 1;
  const sameYear = startDate.getUTCFullYear() === endDate.getUTCFullYear();
  const sameMonth = sameYear
    && startDate.getUTCMonth() === endDate.getUTCMonth();
  let range;
  if (sameMonth) {
    range = `${startDate.getUTCDate()}–${endDate.getUTCDate()} `
      + `${months[endDate.getUTCMonth()]} ${endDate.getUTCFullYear()}`;
  } else if (sameYear) {
    range = `${startDate.getUTCDate()} ${months[startDate.getUTCMonth()]}–`
      + `${endDate.getUTCDate()} ${months[endDate.getUTCMonth()]} `
      + `${endDate.getUTCFullYear()}`;
  } else {
    range = `${startDate.getUTCDate()} ${months[startDate.getUTCMonth()]} `
      + `${startDate.getUTCFullYear()}–${endDate.getUTCDate()} `
      + `${months[endDate.getUTCMonth()]} ${endDate.getUTCFullYear()}`;
  }
  return `${range} · ${inclusiveDays} ${inclusiveDays === 1 ? "day" : "days"}`;
}

function renderAppliedTimeWindowControls() {
  const { start, end } = appliedTimeWindow();
  const allPreset = presetWindow("all");
  const fullRangeActive = Boolean(
    start
    && end
    && start === allPreset.start
    && end === allPreset.end,
  );
  let activePreset = "";
  document.querySelectorAll("[data-days]").forEach((button) => {
    const preset = presetWindow(button.dataset.days);
    const active = fullRangeActive
      ? button.dataset.days === "all"
      : Boolean(
        start
        && end
        && start === preset.start
        && end === preset.end,
      );
    if (active) activePreset = button.dataset.days;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
  byId("applied-window-summary").textContent = formatAppliedWindowSummary(start, end);
  const customActive = Boolean(start && end && !activePreset);
  byId("custom-window-toggle").classList.toggle("active", customActive);
  byId("custom-window-toggle").setAttribute("aria-pressed", String(customActive));
}

function syncTimeWindowControls() {
  renderAppliedTimeWindowControls();
}

function availableMarketWindow() {
  const metadata = app.payload?.metadata || app.defaultPayload?.metadata || {};
  return {
    start: metadata.available_start || "",
    end: metadata.available_end || "",
  };
}

function normalizedMarketWindow(start = "", end = "") {
  const available = availableMarketWindow();
  const effectiveEnd = end || available.end;
  let defaultStart = "";
  if (effectiveEnd) {
    const startDate = new Date(`${effectiveEnd}T00:00:00Z`);
    startDate.setUTCDate(startDate.getUTCDate() - 29);
    const candidate = startDate.toISOString().slice(0, 10);
    defaultStart = available.start && candidate < available.start
      ? available.start
      : candidate;
  }
  return {
    start: start || defaultStart,
    end: effectiveEnd,
  };
}

function compareRouteWindow(route) {
  const explicitStart = route.state?.start || "";
  const explicitEnd = route.state?.end || "";
  if (explicitStart || explicitEnd || !route.state?.window) {
    return normalizedMarketWindow(explicitStart, explicitEnd);
  }
  const available = availableMarketWindow();
  if (!available.start || !available.end || route.state.window === "all") {
    return available;
  }
  const days = Number.parseInt(route.state.window, 10);
  if (![7, 30, 90].includes(days)) return available;
  const startDate = new Date(`${available.end}T00:00:00Z`);
  startDate.setUTCDate(startDate.getUTCDate() - days + 1);
  const candidate = startDate.toISOString().slice(0, 10);
  return {
    start: candidate < available.start ? available.start : candidate,
    end: available.end,
  };
}

function marketWindowKey(start, end) {
  const normalized = normalizedMarketWindow(start, end);
  return `${normalized.start}|${normalized.end}`;
}

function marketPayloadMatchesWindow(payload, start, end) {
  if (!payload?.metadata) return false;
  const normalized = normalizedMarketWindow(start, end);
  return (
    payload.metadata.start_date === normalized.start
    && payload.metadata.end_date === normalized.end
  );
}

function isDefaultMarketPayload(payload) {
  if (!payload?.metadata?.available_start || !payload.metadata.available_end) return false;
  const effectiveEnd = payload.metadata.available_end;
  const startDate = new Date(`${effectiveEnd}T00:00:00Z`);
  startDate.setUTCDate(startDate.getUTCDate() - 29);
  const candidate = startDate.toISOString().slice(0, 10);
  const effectiveStart = candidate < payload.metadata.available_start
    ? payload.metadata.available_start
    : candidate;
  return (
    payload.metadata.start_date === effectiveStart
    && payload.metadata.end_date === effectiveEnd
  );
}

function syncMarketPayloadForWindow(start, end) {
  const normalized = normalizedMarketWindow(start, end);
  if (!normalized.start || !normalized.end) return;
  const desiredKey = marketWindowKey(normalized.start, normalized.end);
  if (app.marketRequestWindowKey === desiredKey) return;
  if (app.marketController && app.marketRequestWindowKey !== desiredKey) {
    invalidateMarketRequest();
    hideStatus(byId("market-loading"));
    byId("market-panel").setAttribute("aria-busy", "false");
    setDateWindowDisabled(false);
    byId("export-csv").disabled = !app.payload;
  }
  if (marketPayloadMatchesWindow(app.payload, normalized.start, normalized.end)) return;

  const defaultWindow = normalizedMarketWindow("", "");
  const wantsDefault = (
    normalized.start === defaultWindow.start
    && normalized.end === defaultWindow.end
  );
  const currentGeneration = app.payload?.metadata?.data_generation;
  const defaultGeneration = app.defaultPayload?.metadata?.data_generation;
  if (
    wantsDefault
    && app.defaultPayload
    && (!currentGeneration || defaultGeneration === currentGeneration)
  ) {
    const cached = app.defaultPayloadIsCached;
    displayMarket(app.defaultPayload, { cached });
    if (!cached) return;
  }

  const requestStart = wantsDefault ? "" : normalized.start;
  const requestEnd = wantsDefault ? "" : normalized.end;
  void loadMarket(requestStart, requestEnd, { preserve: Boolean(app.payload) });
}

function routeTitle(route) {
  if (route.kind === "workspace") {
    const labels = {
      markets: "Markets",
      compare: "Compare",
      liquidity: "Liquidity & Execution",
      events: "Events",
      quality: "Data Quality",
    };
    return `${route.token} ${labels[route.page]} · CEX / DEX Market Monitor`;
  }
  if (route.kind === "opportunities") {
    return "Opportunities · CEX / DEX Market Monitor";
  }
  return "Market Screener · CEX / DEX Market Monitor";
}

function announceRoute(route) {
  document.title = routeTitle(route);
  const label = route.kind === "workspace"
    ? `${route.token} ${route.page} page`
    : route.kind === "opportunities"
      ? "Opportunities page"
    : "Market Screener page";
  byId("route-announcer").textContent = `Showing ${label}.`;
}

function hydrateScreenerControls(route, { normalizeWindow = true } = {}) {
  app.searchQuery = (route.filters?.q || "").toUpperCase();
  byId("token-search").value = route.filters?.q || "";
  const sortKey = SCREENER_SORT_DEFINITIONS[route.filters?.sort]
    ? route.filters.sort
    : "volume";
  byId("sort-field").value = sortKey;
  const definition = SCREENER_SORT_DEFINITIONS[sortKey];
  app.scope = definition.allowedScopes.includes(route.filters?.scope)
    ? route.filters.scope
    : definition.defaultScope;
  app.sortDirection = ["asc", "desc"].includes(route.filters?.dir)
    ? route.filters.dir
    : "desc";
  byId("sort-direction").value = app.sortDirection;
  syncScreenerSortControls();
  const window = normalizeWindow
    ? normalizedMarketWindow(route.filters?.start, route.filters?.end)
    : {
        start: route.filters?.start || "",
        end: route.filters?.end || "",
      };
  if (!customWindowIsOpen()) setDraftTimeWindow(window);
  return window;
}

function applyScreenerRoute(route) {
  app.route = route;
  const window = hydrateScreenerControls(route);
  syncTimeWindowControls();
  setActiveAppView("screener");
  byId("time-toolbar").hidden = false;
  renderTable();
  syncMarketPayloadForWindow(window.start, window.end);
}

function applyOpportunitiesRoute(route) {
  app.route = route;
  const filters = hydrateOpportunityControls(route);
  setActiveAppView("opportunities");
  byId("time-toolbar").hidden = true;
  if (Array.isArray(route?.validationErrors) && route.validationErrors.length) {
    clearOpportunityFilterResult(route.validationErrors);
    return Promise.resolve(false);
  }
  return loadOpportunities(filters);
}

function applyWorkspaceRoute(route) {
  const exactToken = app.payload.tokens
    .map((token) => token.token_symbol)
    .find((token) => token === String(route.token || "").toUpperCase());
  if (!exactToken) {
    const fallbackPath = navigation.buildScreenerPath(currentScreenerFilters());
    window.history.replaceState({}, "", fallbackPath);
    applyScreenerRoute(navigation.parseRoute(
      window.location.pathname,
      window.location.search,
    ));
    showError(byId("error-banner"), `Unknown Token in URL: ${route.token}.`);
    return;
  }
  hideError(byId("error-banner"));
  app.route = { ...route, token: exactToken };
  app.pairSelectionSource = route.state?.pairMode === "transient"
    ? "transient"
    : "";
  app.workspaceSelection = route.state?.selection || "";
  app.workspaceSelectionInvalid = false;
  byId("facts-token").value = exactToken;
  const markets = factsMarketsForToken(exactToken);
  const routeProvidedSelection = Boolean(
    route.state?.marketA
    || route.state?.marketB
    || route.state?.selection,
  );
  const manualPair = route.state?.pairMode === "manual";
  const savedSelection = normalizedSavedSelection(app.pairSelections[exactToken]);
  const validation = validateWorkspaceSelection(
    markets,
    route.state?.marketA,
    route.state?.marketB,
    route.state?.selection,
  );
  if (routeProvidedSelection || manualPair) {
    const invalidReferenceErrors = validation.errors.filter((error) => (
      !["market_a_required", "market_b_required"].includes(error.code)
    ));
    app.workspaceSelectionInvalid = invalidReferenceErrors.length > 0;
    if (app.workspaceSelectionInvalid) {
      populateFactsMarkets({
        requestedA: "",
        requestedB: "",
        allowDefaults: false,
        persistSelection: false,
      });
      const details = [
        `marketA=${String(route.state?.marketA || "(empty)")}`,
        `marketB=${String(route.state?.marketB || "(empty)")}`,
        `selection=${String(route.state?.selection || "(empty)")}`,
      ].join(", ");
      const codes = validation.errors.map((error) => error.code).join(", ");
      showStatus(
        byId("workspace-context-notice"),
        `The shared selection is invalid (${codes}). Raw values: ${details}. No replacement market was chosen and no data request was started.`,
        "stale",
      );
    } else {
      app.workspaceSelection = validation.mode === "single" ? "single" : "";
      populateFactsMarkets({
        requestedA: validation.marketA?.market_id || "",
        requestedB: validation.marketB?.market_id || "",
        allowDefaults: false,
        persistSelection: app.pairSelectionSource !== "transient" && validation.valid,
      });
    }
    if (!app.workspaceSelectionInvalid && !validation.valid) {
      showStatus(
        byId("workspace-context-notice"),
        `Market selection is in progress for ${exactToken}. Choose Market A and optionally Market B, then apply the selection.`,
        "stale",
      );
    } else if (!app.workspaceSelectionInvalid) {
      hideStatus(byId("workspace-context-notice"));
    }
  } else {
    const savedValidation = savedSelection
      ? validateWorkspaceSelection(
        markets,
        savedSelection.marketA,
        savedSelection.marketB,
        savedSelection.selection,
      )
      : null;
    if (savedValidation?.valid) {
      app.workspaceSelection = savedSelection.selection;
      populateFactsMarkets({
        requestedA: savedSelection.marketA,
        requestedB: savedSelection.marketB,
        allowDefaults: false,
        persistSelection: false,
      });
    } else {
      app.workspaceSelection = "";
      populateFactsMarkets();
    }
    showStatus(
      byId("workspace-context-notice"),
      savedValidation?.valid
        ? `Restored the saved ${exactToken} market selection.`
        : `Prepared source-backed ${exactToken} market defaults. Review them before applying.`,
      "success",
    );
  }

  const window = compareRouteWindow(route);
  if (!customWindowIsOpen()) setDraftTimeWindow(window);
  syncTimeWindowControls();
  syncMarketPayloadForWindow(window.start, window.end);

  if (route.page === "liquidity") {
    app.executionDirection = route.state?.side === "sell" ? "sell_token" : "buy_token";
    app.executionNotionalUsd = route.state?.notionalUsd || 10000;
    app.liquidityView = route.state?.view || "total";
    app.liquidityScale = route.state?.scale || "log";
    byId("execution-notional").value = String(app.executionNotionalUsd);
    syncSegmentedControls();
  } else if (route.page === "quality") {
    app.qualityScope = route.state?.scope || "all";
    app.qualitySeverity = route.state?.severity || "";
    app.qualityOrigin = route.state?.origin || "";
    syncSegmentedControls();
  } else if (route.page === "events") {
    app.eventLifecycle = route.state?.lifecycle || "all";
    app.eventClockState = route.state?.clockState || "all";
    syncSegmentedControls();
  }
  setActiveAppView("workspace");
  setActiveWorkspacePage(route.page);
  applyWorkspaceSelectionMode(app.workspaceSelection);
  renderWorkspaceContext();
  renderWorkspaceMarkets();
  renderQualityFromCatalog();
  updateFactsContract();
  byId("facts-workbench").setAttribute("aria-busy", "false");
  if (app.workspaceSelectionInvalid) return;
  if (route.page === "compare") loadComparison();
  if (route.page === "liquidity") {
    renderLiquidityCurve();
    loadExecutionCost();
  }
  if (route.page === "quality") loadQuality();
  if (route.page === "events") loadEvents();
}

function invalidateRouteRequest() {
  if (app.catalogController) app.catalogController.abort();
  app.catalogController = null;
  app.routeRequestId += 1;
  return app.routeRequestId;
}

function setWorkspacePageIdentity(token, page = app.route?.page) {
  const marketsPage = page === "markets";
  byId("workspace-eyebrow").textContent = marketsPage
    ? "Single Token market catalog"
    : "Single Token research workspace";
  byId("facts-title").textContent = marketsPage
    ? `${token} Markets`
    : `${token} Token Research`;
}

function setWorkspaceCatalogLoading(
  token,
  page,
  catalogKey = "",
  { preserveGlobalError = false } = {},
) {
  if (!preserveGlobalError) hideError(byId("global-error"));
  const retainCatalog = app.activeCatalogKey === catalogKey && app.catalog;
  invalidateComparisonRequest();
  invalidateExecutionRequest();
  invalidateQualityRequest();
  invalidateEventRequest();
  if (retainCatalog) return;
  app.catalog = null;
  app.activeCatalogToken = "";
  app.activeCatalogKey = "";
  app.comparison = null;
  app.execution = null;
  app.eventFacts = null;
  app.quality = null;
  hideLiquidityTooltip();
  closeFactsMarketWarnings();
  renderFactsMarketWarning("a", null);
  renderFactsMarketWarning("b", null);
  setWorkspacePageIdentity(token, page);
  byId("workspace-description").textContent = (
    `Loading ${token} market identities, liquidity, and quality facts.`
  );
  byId("facts-token").value = token;
  byId("facts-market-a").innerHTML = '<option value="">Loading markets…</option>';
  byId("facts-market-b").innerHTML = '<option value="">Loading markets…</option>';
  byId("workspace-market-body").innerHTML = (
    `<tr><td colspan="9" class="missing">Loading ${escapeHtml(token)} market catalog…</td></tr>`
  );
  byId("quality-body").innerHTML = (
    `<tr><td colspan="6" class="missing">Loading ${escapeHtml(token)} quality facts…</td></tr>`
  );
  byId("events-body").innerHTML = (
    `<tr><td colspan="8" class="missing">Loading ${escapeHtml(token)} verified Event Facts…</td></tr>`
  );
  ["events-count", "events-occurred", "events-scheduled", "events-source-count"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  byId("liquidity-chart").innerHTML = "";
  byId("liquidity-empty").textContent = `Loading ${token} liquidity facts…`;
  byId("liquidity-empty").hidden = false;
  byId("liquidity-table-body").innerHTML = (
    '<tr><td colspan="9" class="missing">Loading Token liquidity facts…</td></tr>'
  );
  byId("liquidity-legend").innerHTML = "";
  byId("liquidity-a-label").textContent = "Market A at ±100 bps";
  byId("liquidity-b-label").textContent = "Market B at ±100 bps";
  ["liquidity-a-100", "liquidity-b-100", "liquidity-skew", "liquidity-paired-bands"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  byId("liquidity-market-a-meta").innerHTML = "<strong>Market A · loading</strong>";
  byId("liquidity-market-b-meta").innerHTML = "<strong>Market B · loading</strong>";
  byId("liquidity-status").textContent = `Loading ${token} point-in-time depth facts.`;
  byId("liquidity-status").dataset.state = "warning";
  byId("liquidity-chart-description").textContent = `${token} liquidity facts are loading.`;
  byId("quality-status").textContent = `Loading ${token} quality facts.`;
  byId("quality-status").dataset.state = "warning";
  hideError(byId("quality-error"));
  showStatus(
    byId("events-status"),
    `Loading ${token} verified Event Facts.`,
  );
  hideError(byId("events-error"));
  byId("facts-contract-copy").textContent = "Loading the market fact contract…";
  byId("facts-source-copy").textContent = "Loading source lineage…";
  byId("workspace-market-count").textContent = "Loading markets";
  byId("workspace-as-of").textContent = "Checking timestamps";
  byId("workspace-quality-status").textContent = "Checking quality";
  clearComparisonResult();
  clearExecutionResult();
  showStatus(
    byId("workspace-context-notice"),
    `Loading the source-backed ${token} catalog for ${page}.`,
  );
  byId("facts-workbench").setAttribute("aria-busy", "true");
  announceRoute(app.route);
  updateRouteLinks();
}

function setWorkspaceDataUnavailable(token, message) {
  const exactToken = String(token || "Selected Token").toUpperCase();
  app.catalog = null;
  app.activeCatalogToken = "";
  app.activeCatalogKey = "";
  app.comparison = null;
  app.execution = null;
  app.quality = null;
  hideLiquidityTooltip();
  closeFactsMarketWarnings();
  renderFactsMarketWarning("a", null);
  renderFactsMarketWarning("b", null);
  setWorkspacePageIdentity(exactToken, app.route?.page);
  byId("workspace-description").textContent = (
    "Source-backed Token facts are unavailable. No previous Token catalog is retained on screen."
  );
  if (app.payload?.tokens?.some((row) => row.token_symbol === exactToken)) {
    byId("facts-token").value = exactToken;
  } else {
    byId("facts-token").innerHTML = (
      `<option value="">${escapeHtml(exactToken)} unavailable</option>`
    );
    byId("facts-token").value = "";
  }
  byId("facts-market-a").innerHTML = '<option value="">Markets unavailable</option>';
  byId("facts-market-b").innerHTML = '<option value="">Markets unavailable</option>';
  byId("workspace-market-body").innerHTML = (
    `<tr><td colspan="9" class="missing">No ${escapeHtml(exactToken)} market catalog is available.</td></tr>`
  );
  byId("quality-body").innerHTML = (
    `<tr><td colspan="6" class="missing">No ${escapeHtml(exactToken)} quality facts are available.</td></tr>`
  );
  byId("events-body").innerHTML = (
    `<tr><td colspan="8" class="missing">No ${escapeHtml(exactToken)} Event Fact dataset is available.</td></tr>`
  );
  ["events-count", "events-occurred", "events-scheduled", "events-source-count"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  byId("liquidity-chart").innerHTML = "";
  byId("liquidity-empty").textContent = (
    `No ${exactToken} depth facts are available because the market catalog failed to load.`
  );
  byId("liquidity-empty").hidden = false;
  byId("liquidity-table-body").innerHTML = (
    '<tr><td colspan="9" class="missing">No current Token liquidity result.</td></tr>'
  );
  byId("liquidity-legend").innerHTML = "";
  byId("liquidity-market-a-meta").innerHTML = "<strong>Market A · unavailable</strong>";
  byId("liquidity-market-b-meta").innerHTML = "<strong>Market B · unavailable</strong>";
  [
    "liquidity-a-100",
    "liquidity-b-100",
    "liquidity-skew",
    "liquidity-paired-bands",
  ].forEach((id) => {
    byId(id).textContent = "—";
  });
  showStatus(
    byId("liquidity-status"),
    `${exactToken} liquidity facts are unavailable; missing values were not converted to zero.`,
    "critical",
  );
  showStatus(
    byId("quality-status"),
    `${exactToken} quality facts are unavailable.`,
    "critical",
  );
  showError(byId("quality-error"), message);
  showStatus(
    byId("events-status"),
    `${exactToken} Event Facts cannot be scoped because the Token catalog is unavailable.`,
    "critical",
  );
  showError(byId("events-error"), message);
  byId("facts-contract-copy").textContent = "Market fact contract unavailable for this response.";
  byId("facts-source-copy").textContent = "Source lineage unavailable for this response.";
  byId("workspace-market-count").textContent = "Markets unavailable";
  byId("workspace-as-of").textContent = "Snapshot unavailable";
  byId("workspace-quality-status").textContent = "Quality unavailable";
  byId("workspace-quality-status").dataset.state = "critical";
  clearComparisonResult(message);
  clearExecutionResult(message);
  showStatus(
    byId("workspace-context-notice"),
    `${exactToken} facts are unavailable; no previous Token data is shown.`,
    "critical",
  );
  byId("facts-workbench").setAttribute("aria-busy", "false");
  updateRouteLinks();
}

async function applyRouteFromLocation({ preserveWorkspaceError = false } = {}) {
  if (!navigation) return false;
  if (app.marketController) {
    invalidateMarketRequest();
    hideStatus(byId("market-loading"));
    byId("market-panel").setAttribute("aria-busy", "false");
    setDateWindowDisabled(false);
    byId("export-csv").disabled = !app.payload;
  }
  const requestId = invalidateRouteRequest();
  let marketRequestIntent = app.marketRequestId;
  const loadMarketForRoute = (start, end, options = {}) => {
    let ownedRequestId = marketRequestIntent;
    const completion = loadMarket(start, end, {
      ...options,
      onRequestStart(requestId) { ownedRequestId = requestId; },
    });
    marketRequestIntent = ownedRequestId;
    return completion;
  };
  const routeStillOwnsIntent = () => (
    requestId === app.routeRequestId
    && marketRequestIntent === app.marketRequestId
  );
  let route = navigation.parseRoute(window.location.pathname, window.location.search);
  if (route.kind !== "unknown") app.route = route;
  if (route.kind === "opportunities") {
    finalizeRoutePresentation();
    return applyOpportunitiesRoute(route);
  }
  invalidateOpportunityRequest();
  if (route.kind === "workspace") {
    const provisionalToken = String(route.token || "").toUpperCase();
    const provisionalWindow = app.payload
      ? compareRouteWindow(route)
      : { start: "", end: "" };
    const provisionalKey = tokenCatalogCacheKey(
      provisionalToken,
      provisionalWindow.start,
      provisionalWindow.end,
      app.payload?.metadata?.data_generation,
    );
    setActiveAppView("workspace");
    setActiveWorkspacePage(route.page);
    setWorkspaceCatalogLoading(
      provisionalToken,
      route.page,
      provisionalKey,
      { preserveGlobalError: preserveWorkspaceError },
    );
  } else {
    setActiveAppView("screener");
    byId("time-toolbar").hidden = false;
  }
  if (!app.payload) {
    const start = route.kind === "screener"
      ? route.filters?.start || ""
      : route.kind === "workspace"
        ? route.state?.start || ""
        : "";
    const end = route.kind === "screener"
      ? route.filters?.end || ""
      : route.kind === "workspace"
        ? route.state?.end || ""
        : "";
    const loaded = await loadMarketForRoute(start, end);
    if (!routeStillOwnsIntent()) return false;
    if (!loaded || !app.payload) {
      if (route.kind === "workspace") {
        setWorkspaceDataUnavailable(
          String(route.token || "").toUpperCase(),
          "The Screener summary required for this Token could not be loaded.",
        );
      }
      return false;
    }
  }
  if (route.kind === "workspace") {
      const requestedWindow = compareRouteWindow(route);
      if (
        !marketPayloadMatchesWindow(
          app.payload,
          requestedWindow.start,
          requestedWindow.end,
        )
      ) {
        const loaded = await loadMarketForRoute(
          requestedWindow.start,
          requestedWindow.end,
          { preserve: true },
        );
        if (!routeStillOwnsIntent()) return false;
        if (
          !loaded
          || !app.payload
          || !marketPayloadMatchesWindow(
            app.payload,
            requestedWindow.start,
            requestedWindow.end,
          )
        ) {
          setWorkspaceDataUnavailable(
            String(route.token || "").toUpperCase(),
            "The requested daily window could not be loaded, so no mismatched Token catalog is shown.",
          );
          return false;
        }
      }
      const exactToken = app.payload.tokens
        .map((token) => token.token_symbol)
        .find((token) => token === String(route.token || "").toUpperCase());
      if (!exactToken) {
        const fallbackPath = navigation.buildScreenerPath(currentScreenerFilters());
        window.history.replaceState({}, "", fallbackPath);
        applyScreenerRoute(navigation.parseRoute(
          window.location.pathname,
          window.location.search,
        ));
        showError(byId("error-banner"), `Unknown Token in URL: ${route.token}.`);
      } else {
        const exactRoute = { ...route, token: exactToken };
        app.route = exactRoute;
        const catalogWindow = {
          start: app.payload.metadata.start_date,
          end: app.payload.metadata.end_date,
        };
        let catalogKey = tokenCatalogCacheKey(
          exactToken,
          catalogWindow.start,
          catalogWindow.end,
          app.payload.metadata.data_generation,
        );
        setActiveAppView("workspace");
        setActiveWorkspacePage(exactRoute.page);
        setWorkspaceCatalogLoading(
          exactToken,
          exactRoute.page,
          catalogKey,
          { preserveGlobalError: preserveWorkspaceError },
        );
        try {
          if (!routeStillOwnsIntent()) return false;
          let catalog = cachedTokenCatalog(catalogKey);
          for (let attempt = 0; !catalog && attempt < 2; attempt += 1) {
            if (!routeStillOwnsIntent()) return false;
            const controller = new AbortController();
            app.catalogController = controller;
            try {
              catalog = await loadTokenCatalog(
                exactToken,
                catalogWindow.start,
                catalogWindow.end,
                controller.signal,
                catalogKey,
              );
            } catch (error) {
              if (error.code !== "data_generation_mismatch" || attempt > 0) throw error;
              if (!routeStillOwnsIntent()) return false;
              app.catalogsByToken.clear();
              const refreshed = await loadMarketForRoute(
                catalogWindow.start,
                catalogWindow.end,
                {
                  preserve: true,
                  refreshWorkspaceOnGenerationChange: false,
                },
              );
              if (!routeStillOwnsIntent()) return false;
              if (
                !refreshed
                || !app.payload
                || !marketPayloadMatchesWindow(
                  app.payload,
                  catalogWindow.start,
                  catalogWindow.end,
                )
              ) {
                throw new Error(
                  `The ${exactToken} Screener summary could not be refreshed.`,
                );
              }
              catalogKey = tokenCatalogCacheKey(
                exactToken,
                catalogWindow.start,
                catalogWindow.end,
                app.payload.metadata.data_generation,
              );
            }
          }
          const latestRoute = navigation.parseRoute(
            window.location.pathname,
            window.location.search,
          );
          if (
            !routeStillOwnsIntent()
            || latestRoute.kind !== "workspace"
            || String(latestRoute.token || "").toUpperCase() !== exactToken
          ) {
            return false;
          }
          if (!catalog) throw new Error(`The ${exactToken} catalog is unavailable.`);
          cacheTokenCatalog(catalogKey, catalog);
          app.catalogController = null;
          app.catalog = catalog;
          app.activeCatalogToken = exactToken;
          app.activeCatalogKey = catalogKey;
          route = { ...latestRoute, token: exactToken };
          applyWorkspaceRoute(route);
        } catch (error) {
          if (error.name === "AbortError" || !routeStillOwnsIntent()) return false;
          app.catalogController = null;
          const message = (
            `The ${exactToken} market catalog failed to load: `
            + publicErrorMessage(error, "Market catalog is unavailable.")
          );
          setWorkspaceDataUnavailable(exactToken, message);
          showError(
            byId("global-error"),
            message,
          );
          finalizeRoutePresentation();
          return false;
        }
      }
  } else if (route.kind === "screener") {
    applyScreenerRoute(route);
  } else {
    window.history.replaceState({}, "", "/screener");
    applyScreenerRoute(navigation.parseRoute("/screener", ""));
  }
  if (requestId !== app.routeRequestId) return false;
  finalizeRoutePresentation();
  return true;
}

function finalizeRoutePresentation() {
  app.routeReady = true;
  announceRoute(app.route);
  updateRouteLinks();
  canonicalizeCurrentRoute();
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

function validateDateRange(start = "", end = "", { required = false } = {}) {
  if (required && (!start || !end)) {
    return "Choose both a start date and an end date.";
  }
  const isoDate = /^\d{4}-\d{2}-\d{2}$/;
  const validIsoDate = (value) => {
    if (!isoDate.test(value)) return false;
    const parsed = new Date(`${value}T00:00:00Z`);
    return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
  };
  if ((start && !validIsoDate(start)) || (end && !validIsoDate(end))) {
    return "Dates must use the YYYY-MM-DD format.";
  }
  if (start && end && start > end) {
    return "Start date must not be after end date.";
  }
  const availableStart = app.payload?.metadata?.available_start || "";
  const availableEnd = app.payload?.metadata?.available_end || "";
  if (availableStart && start && start < availableStart) {
    return `Start date must be on or after ${availableStart}.`;
  }
  if (availableEnd && end && end > availableEnd) {
    return `End date must be on or before ${availableEnd}.`;
  }
  return "";
}

function showDateWindowError(message) {
  const invalid = Boolean(message);
  ["date-start", "date-end"].forEach((id) => {
    byId(id)?.setAttribute("aria-invalid", String(invalid));
  });
  const element = byId("date-window-error");
  if (!element) return;
  if (message) showError(element, message);
  else hideError(element);
}

function setDateWindowDisabled(disabled) {
  document.querySelectorAll(
    "#date-window-form input, #date-window-form button, "
    + "#time-presets button, #custom-window-toggle",
  )
    .forEach((control) => {
      control.disabled = disabled;
    });
}

function showStatus(element, message, state = "") {
  element.textContent = message;
  element.dataset.state = state;
  element.hidden = false;
}

function hideStatus(element) {
  element.hidden = true;
  element.textContent = "";
  delete element.dataset.state;
}

function showError(element, message) {
  element.textContent = message;
  element.hidden = false;
}

function hideError(element) {
  element.hidden = true;
  element.textContent = "";
}

function ensureSelections() {
  app.payload.tokens.forEach((token) => {
    const symbol = token.token_symbol;
    if (!app.selections[symbol]) app.selections[symbol] = {};
    if (!app.selectionOverrides[symbol]) app.selectionOverrides[symbol] = {};
    const cexIds = token.primary_cex ? [marketId(token.primary_cex)] : [];
    const dexIds = token.primary_dex ? [marketId(token.primary_dex)] : [];
    if (
      !app.selectionOverrides[symbol].cex
      || !cexIds.includes(app.selections[symbol].cex)
    ) {
      app.selections[symbol].cex = token.primary_cex_id || cexIds[0] || null;
      app.selectionOverrides[symbol].cex = false;
    }
    if (
      !app.selectionOverrides[symbol].dex
      || !dexIds.includes(app.selections[symbol].dex)
    ) {
      app.selections[symbol].dex = token.primary_dex_id || dexIds[0] || null;
      app.selectionOverrides[symbol].dex = false;
    }
  });
}

function selectedMarket(token, market) {
  const tokenSummary = app.payload.tokens.find((row) => row.token_symbol === token);
  const row = market === "cex" ? tokenSummary?.primary_cex : tokenSummary?.primary_dex;
  const selectedId = app.selections[token]?.[market];
  return row && marketId(row) === selectedId ? row : null;
}

function comparison(tokenSummary) {
  const token = tokenSummary.token_symbol;
  const cex = selectedMarket(token, "cex");
  const dex = selectedMarket(token, "dex");
  const spreadDate = tokenSummary.spread_date || null;
  const cexSpreadPrice = null;
  const dexSpreadPrice = null;
  const spread = finite(tokenSummary.price_spread) ? tokenSummary.price_spread : null;
  return { cex, dex, spread, spreadDate, cexSpreadPrice, dexSpreadPrice };
}

function aggregateFacts(tokenSummary, cexOptions, dexOptions) {
  const cexFallback = cexOptions.length ? sumFinite(cexOptions, "volume_usd") : null;
  const dexFallback = dexOptions.length ? sumFinite(dexOptions, "volume_usd") : null;
  const aggregateCex = firstFinite(
    tokenSummary.aggregate_cex_volume_usd,
    tokenSummary.cex_volume_usd,
    cexFallback,
  ) ?? null;
  const aggregateDex = firstFinite(
    tokenSummary.aggregate_dex_volume_usd,
    tokenSummary.dex_volume_usd,
    dexFallback,
  ) ?? null;
  const summedTotal = finite(aggregateCex) && finite(aggregateDex)
    ? aggregateCex + aggregateDex
    : null;
  const aggregateTotal = firstFinite(
    tokenSummary.aggregate_volume_usd,
    tokenSummary.total_volume_usd,
    summedTotal,
  ) ?? null;
  const aggregateDexShare = firstFinite(
    tokenSummary.aggregate_dex_volume_share,
    tokenSummary.aggregate_dex_share,
    tokenSummary.observed_dex_share,
    finite(aggregateTotal) && aggregateTotal !== 0 && finite(aggregateDex)
      ? aggregateDex / aggregateTotal
      : null,
  ) ?? null;
  return { aggregateCex, aggregateDex, aggregateTotal, aggregateDexShare };
}

function currentScreenerSortDefinition() {
  return SCREENER_SORT_DEFINITIONS[byId("sort-field")?.value]
    || SCREENER_SORT_DEFINITIONS.volume;
}

function syncScreenerSortControls() {
  const definition = currentScreenerSortDefinition();
  if (!definition.allowedScopes.includes(app.scope)) {
    app.scope = definition.defaultScope;
  }
  const fixedScope = definition.allowedScopes.length === 1;
  const scopeButtons = byId("sort-scope-buttons");
  const fixedScopeChip = byId("sort-scope-fixed");
  if (scopeButtons) scopeButtons.hidden = fixedScope;
  if (fixedScopeChip) {
    fixedScopeChip.hidden = !fixedScope;
    fixedScopeChip.textContent = definition.allowedScopes[0] === "cross"
      ? "Cross-venue · Primary CEX ↔ DEX"
      : "DEX market scope";
  }
  document.querySelectorAll("[data-scope]").forEach((button) => {
    const allowed = definition.allowedScopes.includes(button.dataset.scope);
    const active = allowed && button.dataset.scope === app.scope;
    button.hidden = !allowed;
    button.disabled = false;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
    button.setAttribute(
      "aria-label",
      `${button.textContent.trim()} ranking scope`,
    );
  });
  const scopeLabel = {
    combined: "Aggregate",
    cross: "Cross-venue",
    cex: "Primary CEX",
    dex: "Primary DEX",
  }[app.scope] || app.scope;
  const directionLabel = app.sortDirection === "asc"
    ? "Lowest first"
    : "Highest first";
  const rankHeading = byId("rank-value-heading");
  if (rankHeading) {
    rankHeading.textContent = `${definition.label} · ${scopeLabel}`;
    rankHeading.title = (
      `${directionLabel}. Missing, failed, and unsupported values stay last.`
      + (definition.snapshot ? " This Fact is a latest snapshot." : "")
    );
  }
}

function sortValue(tokenSummary) {
  const aggregates = aggregateFacts(tokenSummary, [], []);
  const field = byId("sort-field").value;
  if (field === "spread") {
    const value = tokenSummary.absolute_price_gap;
    return finite(value) ? value : -Infinity;
  }
  if (field === "spread_max") {
    const value = tokenSummary.maximum_absolute_price_spread;
    return finite(value) ? value : -Infinity;
  }
  if (field === "spread_mean") {
    const value = tokenSummary.mean_absolute_price_spread;
    return finite(value) ? value : -Infinity;
  }
  if (field === "spread_median") {
    const value = tokenSummary.median_absolute_price_spread;
    return finite(value) ? value : -Infinity;
  }
  const { cex, dex } = comparison(tokenSummary);
  if (field === "return") {
    const value = app.scope === "dex" ? dex?.window_return : cex?.window_return;
    return finite(value) ? value : -Infinity;
  }
  if (field === "volatility") {
    const value = app.scope === "dex" ? dex?.daily_volatility : cex?.daily_volatility;
    return finite(value) ? value : -Infinity;
  }
  if (field === "depth_100bps") {
    const value = app.scope === "dex"
      ? dex?.total_depth_100bps_usd
      : cex?.total_depth_100bps_usd;
    return finite(value) ? value : -Infinity;
  }
  if (field === "dex_tvl") {
    return finite(dex?.tvl_usd) ? dex.tvl_usd : -Infinity;
  }
  if (app.scope === "cex") {
    return finite(aggregates.aggregateCex) ? aggregates.aggregateCex : -Infinity;
  }
  if (app.scope === "dex") {
    return finite(aggregates.aggregateDex) ? aggregates.aggregateDex : -Infinity;
  }
  return finite(aggregates.aggregateTotal) ? aggregates.aggregateTotal : -Infinity;
}

function compareScreenerTokens(a, b) {
  const valueA = sortValue(a);
  const valueB = sortValue(b);
  const missingA = !finite(valueA);
  const missingB = !finite(valueB);
  if (missingA && missingB) {
    return a.token_symbol.localeCompare(b.token_symbol);
  }
  if (missingA) return 1;
  if (missingB) return -1;
  const compared = app.sortDirection === "asc"
    ? valueA - valueB
    : valueB - valueA;
  return compared || a.token_symbol.localeCompare(b.token_symbol);
}

function formatRankValue(tokenSummary) {
  const value = sortValue(tokenSummary);
  if (!finite(value)) return null;
  const field = byId("sort-field").value;
  if (field.startsWith("spread")) return `${bpsFormat.format(value * 10_000)} bps`;
  if (field === "return" || field === "volatility") return formatPercent(value);
  return formatCurrency(value);
}

function metricClass(value) {
  if (!finite(value) || value === 0) return "";
  return value > 0 ? "positive" : "negative";
}

function qualityFlagObjects(row, market) {
  if (!row) return [];
  const suppliedDetails = Array.isArray(row?.quality_flag_details)
      ? row.quality_flag_details.map((flag) => ({
        code: flag.code,
        severity: flag.severity || "warning",
        category: flag.category || "data_health",
        explanation: flag.explanation || flag.message || "",
        observedValue: flag.observed_value ?? flag.observedValue ?? null,
        threshold: flag.threshold ?? null,
      }))
    : [];
  const suppliedCodes = Array.isArray(row?.quality_flags)
    ? row.quality_flags.map((flag) => (
        typeof flag === "string"
          ? {
              code: flag,
              severity: QUALITY_FLAG_DEFAULT_SEVERITIES[flag] || "warning",
              category: "data_health",
              explanation: "",
              observedValue: null,
              threshold: null,
            }
          : {
              code: flag.code,
              severity: flag.severity || "warning",
              category: flag.category || "data_health",
              explanation: flag.explanation || flag.message || "",
              observedValue: flag.observed_value ?? flag.observedValue ?? null,
              threshold: flag.threshold ?? null,
            }
      ))
    : [];
  const flags = [...suppliedDetails, ...suppliedCodes];
  const add = (
    code,
    severity,
    explanation,
    observedValue = null,
    threshold = null,
  ) => {
    const existing = flags.find((flag) => flag.code === code);
    if (!existing) {
      flags.push({
        code,
        severity,
        category: "data_health",
        explanation,
        observedValue,
        threshold,
      });
      return;
    }
    if (!existing.explanation && explanation) existing.explanation = explanation;
    if (
      (QUALITY_SEVERITY_RANK[severity] || 0)
      > (QUALITY_SEVERITY_RANK[existing.severity] || 0)
    ) {
      existing.severity = severity;
    }
    if (existing.observedValue === null && observedValue !== null) {
      existing.observedValue = observedValue;
    }
    if (existing.threshold === null && threshold !== null) existing.threshold = threshold;
  };
  const status = market === "cex"
    ? row?.depth_status
    : row?.dex_depth_status ?? row?.depth_status;
  const normalizedStatus = String(status || "").toLowerCase();
  if (["unsupported", "unsupported_protocol", "unsupported_chain"].includes(normalizedStatus)) {
    add(
      "depth_unsupported",
      "warning",
      row?.dex_depth_error || "Executable depth is unsupported for this market.",
      status,
    );
  }
  if (normalizedStatus === "partial") {
    add(
      "depth_partial",
      "warning",
      "Returned levels are a lower bound.",
      status,
    );
  }
  if (["failed", "error"].includes(normalizedStatus)) {
    add("depth_failed", "critical", "Depth collection failed.", status);
  }
  const knownDepthStatuses = new Set([
    "observed",
    "complete",
    "partial",
    "unsupported",
    "unsupported_protocol",
    "unsupported_chain",
    "failed",
    "error",
  ]);
  if (!knownDepthStatuses.has(normalizedStatus)) {
    add(
      "depth_unavailable",
      "info",
      normalizedStatus === "not_cataloged_in_snapshot"
        ? "Market was not present in the latest depth snapshot."
        : "No executable-depth observation is available.",
      status,
    );
  }
  if (
    ["observed", "partial", "complete"].includes(normalizedStatus)
    && row?.total_depth_10bps_usd === 0
  ) {
    add(
      "zero_depth_10bps",
      "warning",
      "The ±10 bps band may lie inside the quoted spread.",
      0,
      {
        band_bps: 10,
        quoted_spread_bps: row?.spread_bps ?? row?.quoted_spread_bps ?? null,
      },
    );
  }
  const thresholds = app.catalog?.metadata?.market_quality_thresholds
    || app.catalog?.metadata?.quality_thresholds
    || app.payload?.metadata?.market_quality_thresholds
    || app.payload?.metadata?.quality_thresholds
    || {};
  const tinyPoolThreshold = thresholds.tiny_pool_tvl_usd ?? 100_000;
  if (market === "dex" && finite(row?.tvl_usd) && row.tvl_usd < tinyPoolThreshold) {
    add(
      "tiny_pool",
      "warning",
      `TVL is below ${formatCurrency(tinyPoolThreshold)}.`,
      row.tvl_usd,
      tinyPoolThreshold,
    );
  }
  const deviationThreshold = thresholds.off_market_price_deviation_bps ?? 100;
  const criticalDeviationThreshold = thresholds.critical_off_market_price_deviation_bps ?? 500;
  if (
    market === "dex"
    && finite(row?.price_difference_bps)
    && Math.abs(row.price_difference_bps) > deviationThreshold
  ) {
    add(
      "off_market_pool_state_price",
      Math.abs(row.price_difference_bps) > criticalDeviationThreshold ? "critical" : "warning",
      `Pool-state/source deviation is ${bpsFormat.format(row.price_difference_bps)} bps.`,
      row.price_difference_bps,
      Math.abs(row.price_difference_bps) > criticalDeviationThreshold
        ? criticalDeviationThreshold
        : deviationThreshold,
    );
  }
  const wideSpreadThreshold = thresholds.wide_cex_quoted_spread_bps ?? 100;
  const quotedSpreadBps = row?.spread_bps ?? row?.quoted_spread_bps;
  if (
    market === "cex"
    && finite(quotedSpreadBps)
    && quotedSpreadBps > wideSpreadThreshold
  ) {
    add(
      "wide_quoted_spread",
      "warning",
      `Quoted spread is ${bpsFormat.format(quotedSpreadBps)} bps.`,
      quotedSpreadBps,
      wideSpreadThreshold,
    );
  }
  const coverageThreshold = thresholds.minimum_primary_coverage_ratio ?? 0.8;
  if (finite(row?.coverage_ratio) && row.coverage_ratio < coverageThreshold) {
    add(
      "low_daily_coverage",
      "warning",
      `Daily coverage is ${formatRatio(row.coverage_ratio)}.`,
      row.coverage_ratio,
      coverageThreshold,
    );
  }
  return flags.filter(
    (flag, index, values) => values.findIndex((candidate) => candidate.code === flag.code) === index,
  );
}

function qualityFlagLabel(flag) {
  return QUALITY_FLAG_LABELS[flag.code]
    || flag.code.replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase());
}

function qualityFlagMeasurement(flag) {
  const observed = flag.observedValue;
  const threshold = flag.threshold;
  if (flag.code === "low_daily_coverage") {
    const values = [
      finite(observed) ? `Observed ${formatQualityRatio(observed)}` : "",
      finite(threshold) ? `minimum ${formatQualityRatio(threshold)}` : "",
    ].filter(Boolean);
    return values.join(" · ");
  }
  if (flag.code === "tiny_pool") {
    const values = [
      finite(observed) ? `Observed ${formatQualityUsd(observed)}` : "",
      finite(threshold) ? `minimum ${formatQualityUsd(threshold)}` : "",
    ].filter(Boolean);
    return values.join(" · ");
  }
  if (flag.code === "off_market_pool_state_price") {
    const values = [
      finite(observed) ? `Observed ${rawUsd.format(observed)} bps` : "",
      finite(threshold) ? `threshold ${rawUsd.format(threshold)} bps` : "",
    ].filter(Boolean);
    return values.join(" · ");
  }
  if (flag.code === "wide_quoted_spread") {
    const values = [
      finite(observed) ? `Observed ${rawUsd.format(observed)} bps` : "",
      finite(threshold) ? `maximum ${rawUsd.format(threshold)} bps` : "",
    ].filter(Boolean);
    return values.join(" · ");
  }
  if (
    flag.code === "zero_depth_10bps"
    || flag.code === "zero_depth_inside_spread"
  ) {
    const band = finite(threshold?.band_bps) ? `±${threshold.band_bps} bps band` : "inner band";
    const spread = finite(threshold?.quoted_spread_bps)
      ? ` · quoted spread ${rawUsd.format(threshold.quoted_spread_bps)} bps`
      : "";
    return finite(observed)
      ? `Observed ${formatQualityUsd(observed)} in the ${band}${spread}`
      : "";
  }
  if (typeof observed === "string" && observed) return `Observed status: ${observed}`;
  if (finite(observed) || finite(threshold)) {
    return [
      finite(observed) ? `Observed ${rawUsd.format(observed)}` : "",
      finite(threshold) ? `threshold ${rawUsd.format(threshold)}` : "",
    ].filter(Boolean).join(" · ");
  }
  return "";
}

function renderQualityBadges(flags) {
  if (!flags.length) return '<span class="quality-flag good">No quality flags</span>';
  return flags.map((flag) => {
    const severityClass = flag.severity === "critical"
      ? "danger"
      : flag.severity === "warning"
        ? "warn"
        : "info";
    const label = qualityFlagLabel(flag);
    return `<span class="quality-flag ${severityClass}" title="${escapeHtml(flag.explanation)}">`
      + `${escapeHtml(label)}</span>`;
  }).join("");
}

function screenerMetricTooltip(value, tooltip) {
  const label = `${value}. ${tooltip}`;
  return `<span class="screener-metric-tooltip" tabindex="0" `
    + `aria-label="${escapeHtml(label)}" data-tooltip="${escapeHtml(tooltip)}">`
    + `${escapeHtml(value)}</span>`;
}

function publicFactRefreshEnabled() {
  const metadata = app.route?.kind === "workspace"
    ? app.catalog?.metadata
    : app.payload?.metadata;
  return metadata?.public_actions?.fact_refresh_enabled === true;
}

function naFactAriaLabel({
  token = "",
  marketId = "",
  marketLabel = "",
  fact = "",
  factLabel = "",
  bandBps = null,
  notionalUsd = null,
} = {}) {
  const hasBand = bandBps !== null && bandBps !== "" && finite(Number(bandBps));
  const hasNotional = notionalUsd !== null
    && notionalUsd !== ""
    && finite(Number(notionalUsd));
  const context = [
    token ? `Token ${token}` : "",
    marketLabel ? `market ${marketLabel}` : marketId ? `market ${marketId}` : "",
    factLabel ? `fact ${factLabel}` : fact ? `fact ${fact}` : "",
    hasBand ? `band ±${Number(bandBps)} bps` : "",
    hasNotional ? `notional ${formatRawUsd(Number(notionalUsd))}` : "",
  ].filter(Boolean);
  return context.length ? `N/A reason: ${context.join(" · ")}` : "N/A reason";
}

function naFactMarkup(reason, {
  retryable = false,
  token = "",
  marketId = "",
  marketLabel = "",
  fact = "",
  factLabel = "",
  bandBps = null,
  notionalUsd = null,
} = {}) {
  const context = {
    token,
    marketId,
    marketLabel,
    fact,
    factLabel,
    bandBps,
    notionalUsd,
  };
  const action = retryable && publicFactRefreshEnabled() && token && marketId && fact
    ? `<button type="button" class="na-refresh-action secondary-command" `
      + `data-refresh-fact="${escapeHtml(fact)}" `
      + `data-refresh-token="${escapeHtml(token)}" `
      + `data-refresh-market-id="${escapeHtml(marketId)}">Refresh this fact</button>`
      + `<span class="na-refresh-status" data-refresh-status role="status" `
      + `aria-live="polite" aria-atomic="true" hidden></span>`
    : "";
  return `<details class="na-disclosure">
    <summary aria-label="${escapeHtml(naFactAriaLabel(context))}"><span>N/A</span><i data-lucide="info"></i></summary>
    <div class="na-disclosure-panel"><p>${escapeHtml(reason)}</p>${action}</div>
  </details>`;
}

function opportunityNumber(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string" || !value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function opportunityReason(code, fallback = "The published route value is unavailable.") {
  return OPPORTUNITY_REASON_LABELS[code] || fallback;
}

function opportunityNaMarkup(reason, context = {}) {
  return naFactMarkup(reason, context).replace(
    'class="na-disclosure"',
    'class="na-disclosure opportunity-na-disclosure"',
  );
}

function opportunityValueMarkup(value, formatter, reason, context = {}) {
  const number = opportunityNumber(value);
  if (number === null) return opportunityNaMarkup(reason, context);
  return `<span data-opportunity-value="${escapeHtml(String(number))}">`
    + `${escapeHtml(formatter(number))}</span>`;
}

function opportunityRouteReason(route) {
  const code = route?.availability?.reason
    || route?.primary_reason
    || route?.reason_codes?.[0]
    || "";
  return opportunityReason(code);
}

function opportunityComponentReason(component, route) {
  const status = String(component?.value_status || "");
  if (status === "not_applicable") {
    return "Not applicable under this route contract; no numeric cost is inferred.";
  }
  if (component?.reason_code) {
    const known = OPPORTUNITY_REASON_LABELS[component.reason_code];
    return known || (
      `Cost component status: ${status || "unavailable"}. `
      + `Reason code: ${component.reason_code}.`
    );
  }
  if (route?.availability?.status === "unavailable") {
    return opportunityRouteReason(route);
  }
  if (["unavailable", "unsupported", "failed", "stale"].includes(status)) {
    return `Cost component status: ${status}. No numeric cost is inferred.`;
  }
  return "No numeric amount was published for this cost component.";
}

function opportunityComponentEvidence(component) {
  const status = String(component?.value_status || "status unavailable");
  const strictness = component?.strict_eligible === true
    ? "strict eligible"
    : "not strict eligible";
  const reflection = component?.reflected_or_embedded === true
    ? "reflected or embedded"
    : "not reflected or embedded";
  const reason = component?.reason_code
    ? ` · reason ${component.reason_code}`
    : "";
  return `${status} · ${strictness} · ${reflection}${reason}`;
}

function formatOpportunityTimestamp(value) {
  if (!value) return "time unavailable";
  const exact = String(value).trim().match(
    /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?:Z|\+00:00)$/,
  );
  if (exact) return `${exact[1]} ${exact[2]} UTC`;
  return String(value);
}

function formatOpportunitySeconds(value) {
  return String(value);
}

function opportunitySourceLinks(route) {
  const links = Array.isArray(route?.source_links) ? route.source_links : [];
  if (!links.length) return "";
  return `<div class="opportunity-source-links">${links.map((link) => {
    const marketId = escapeHtml(link?.market_id || "Source evidence");
    const safeUrl = typeof link?.url === "string"
      && /^https:\/\//i.test(link.url)
      ? link.url
      : null;
    if (safeUrl) {
      return `<a href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">${marketId}</a>`;
    }
    return `<span class="opportunity-source-identity" aria-label="Source URL withheld for ${marketId}">`
      + `<span>${marketId}</span><span class="metric-note">Source URL withheld</span></span>`;
  }).join("")}</div>`;
}

function opportunityCostMarkup(route) {
  const reason = opportunityRouteReason(route);
  const context = {
    token: route?.token_symbol || "",
    marketLabel: route?.route_id || "",
  };
  const gross = opportunityNumber(route?.gross_edge_usd);
  const strictCost = opportunityNumber(route?.cost_breakdown?.strict_nonembedded_usd);
  const boundedCost = opportunityNumber(route?.cost_breakdown?.research_bounded_usd);
  const assumedCost = opportunityNumber(route?.cost_breakdown?.research_assumed_usd);
  const net = opportunityNumber(route?.net_edge_usd);
  const costs = [strictCost, boundedCost, assumedCost];
  const totalCost = costs.every((value) => value !== null)
    ? costs.reduce((total, value) => total + value, 0)
    : null;
  const reconciled = gross !== null && totalCost !== null && net !== null
    && Math.abs((gross - totalCost) - net) <= 1e-8;
  const components = Array.isArray(route?.cost_components)
    ? route.cost_components
    : [];
  const componentMarkup = components.length
    ? `<ul class="opportunity-cost-components">${components.map((component) => {
        const label = [component.leg, component.component_type]
          .filter(Boolean).join(" · ") || "Cost component";
        return `<li><span>${escapeHtml(label)}</span>`
          + `<span class="metric-note opportunity-cost-evidence">${escapeHtml(opportunityComponentEvidence(component))}</span>`
          + opportunityValueMarkup(
            component.amount_usd,
            formatRawUsd,
            opportunityComponentReason(component, route),
            { ...context, factLabel: label },
          )
          + "</li>";
      }).join("")}</ul>`
    : "";
  const attr = (value) => value === null ? "" : escapeHtml(String(value));
  return `<details class="opportunity-cost-disclosure"
      data-gross-edge-usd="${attr(gross)}"
      data-total-cost-usd="${attr(totalCost)}"
      data-net-edge-usd="${attr(net)}">
    <summary aria-label="Cost and evidence details for ${escapeHtml(route?.route_id || "route")}">Cost details</summary>
    <div class="na-disclosure-panel opportunity-cost-panel">
      <dl>
        <div><dt>Gross edge</dt><dd>${opportunityValueMarkup(gross, formatRawUsd, reason, { ...context, factLabel: "gross edge" })}</dd></div>
        <div><dt>Strict published costs</dt><dd>${opportunityValueMarkup(strictCost, formatRawUsd, reason, { ...context, factLabel: "strict published costs" })}</dd></div>
        <div><dt>Bounded research costs</dt><dd>${opportunityValueMarkup(boundedCost, formatRawUsd, reason, { ...context, factLabel: "bounded research costs" })}</dd></div>
        <div><dt>Assumed research costs</dt><dd>${opportunityValueMarkup(assumedCost, formatRawUsd, reason, { ...context, factLabel: "assumed research costs" })}</dd></div>
        <div><dt>Total applied costs</dt><dd>${opportunityValueMarkup(totalCost, formatRawUsd, reason, { ...context, factLabel: "total applied costs" })}</dd></div>
        <div><dt>Published net edge</dt><dd>${opportunityValueMarkup(net, formatRawUsd, reason, { ...context, factLabel: "published net edge" })}</dd></div>
      </dl>
      <p class="metric-note">${reconciled
        ? "Reconciled: gross edge − published costs = net edge."
        : "Cost reconciliation is unavailable because at least one published input is missing."}</p>
      ${componentMarkup}
      ${opportunitySourceLinks(route)}
    </div>
  </details>`;
}

function opportunityRowMarkup(route) {
  const reason = opportunityRouteReason(route);
  const reasonCode = route?.availability?.reason
    || route?.primary_reason
    || route?.reason_codes?.[0]
    || "reason_unavailable";
  const token = route?.token_symbol || "Unknown Token";
  const routeId = route?.route_id || "Unknown route";
  const context = { token, marketLabel: routeId };
  const buy = route?.buy_market_id || "Buy leg unavailable";
  const sell = route?.sell_market_id || "Sell leg unavailable";
  const routePath = typeof navigation?.buildWorkspacePath === "function"
    ? navigation.buildWorkspacePath(token, "liquidity", {
      marketA: route?.buy_market_id || "",
      marketB: route?.sell_market_id || "",
      pairMode: "transient",
    })
    : "";
  const routeIdentity = routePath
    ? `<a class="opportunity-route-id route-action" href="${escapeHtml(routePath)}">${escapeHtml(routeId)}</a>`
    : `<span class="opportunity-route-id">${escapeHtml(routeId)}</span>`;
  const buyObserved = route?.leg_timestamps?.buy
    ? formatOpportunityTimestamp(route.leg_timestamps.buy)
    : "unavailable";
  const sellObserved = route?.leg_timestamps?.sell
    ? formatOpportunityTimestamp(route.leg_timestamps.sell)
    : "unavailable";
  const routeVolumeReason = [
    "One or both route legs lack a positive source-horizon USD volume; no zero is inferred.",
    "CEX uses selected-window USD volume and DEX uses latest 24-hour USD volume.",
    "This value is a ranking reference, not executable capacity.",
  ].join(" ");
  return `<tr data-opportunity-class="${escapeHtml(route?.opportunity_class || "unavailable")}" data-route-id="${escapeHtml(routeId)}">
    <td data-label="Token"><strong>${escapeHtml(token)}</strong></td>
    <td data-label="Route">${routeIdentity}<span class="metric-note">${escapeHtml(buy)} → ${escapeHtml(sell)}</span><span class="metric-note">Buy ${escapeHtml(buyObserved)} · Sell ${escapeHtml(sellObserved)}</span><span class="metric-note">${escapeHtml(route?.route_type || "route type unavailable")} · ${escapeHtml(route?.route_mode || "mode unavailable")}</span></td>
    <td data-label="Notional">${opportunityValueMarkup(route?.requested_notional_usd, formatRawUsd, reason, { ...context, factLabel: "requested notional" })}</td>
    <td data-label="Route volume">${opportunityValueMarkup(route?.route_volume_usd, formatRawVolume, routeVolumeReason, { ...context, factLabel: "route reference volume" })}</td>
    <td data-label="Net edge">${opportunityValueMarkup(route?.net_edge_usd, formatRawUsd, reason, { ...context, factLabel: "net edge" })}</td>
    <td data-label="Net bps">${opportunityValueMarkup(route?.net_edge_bps, (value) => `${bpsFormat.format(value)} bps`, reason, { ...context, factLabel: "net edge bps" })}</td>
    <td data-label="Capacity">${opportunityValueMarkup(route?.capacity_quantity, (value) => rawVolume.format(value), reason, { ...context, factLabel: "proved capacity" })}</td>
    <td data-label="Skew">${opportunityValueMarkup(route?.skew_seconds, (value) => `${formatOpportunitySeconds(value)} s`, reason, { ...context, factLabel: "snapshot skew" })}</td>
    <td data-label="Age">${opportunityValueMarkup(route?.route_age_seconds, (value) => `${formatOpportunitySeconds(value)} s`, reason, { ...context, factLabel: "route age" })}</td>
    <td data-label="Costs & evidence">${opportunityCostMarkup(route)}<span class="opportunity-reason-code">${escapeHtml(reasonCode)}</span><span class="metric-note">${escapeHtml(reason)}</span></td>
  </tr>`;
}

function opportunityInventoryVisibility(filters = {}) {
  const opportunityClass = filters.opportunity_class || "all";
  const availability = filters.availability || "all";
  return {
    strict: opportunityClass !== "estimate" && availability !== "unavailable",
    estimate: opportunityClass !== "strict" && availability !== "unavailable",
    unavailable: availability !== "available",
  };
}

function renderOpportunityInventory(name, routes, visible) {
  const section = byId(`${name}-opportunities`);
  const body = byId(`${name}-opportunity-body`);
  const count = byId(`${name}-opportunity-count`);
  const empty = byId(`${name}-opportunity-empty`);
  section.hidden = !visible;
  body.innerHTML = routes.map(opportunityRowMarkup).join("");
  count.textContent = `${routes.length} ${routes.length === 1 ? "route" : "routes"}`;
  const emptyMessages = {
    strict: "No route currently satisfies every strict gate. This does not mean there is no Daily Price Gap or no market.",
    estimate: "No research estimates match the current filters.",
    unavailable: "No unavailable routes match the current filters.",
  };
  empty.textContent = emptyMessages[name];
  empty.hidden = !visible || routes.length > 0;
}

function renderOpportunities(payload) {
  app.opportunities = payload;
  const venueOptions = Array.isArray(payload?.metadata?.available_venues)
    ? payload.metadata.available_venues.filter((venue) => (
        typeof venue === "string" && venue.length
      ))
    : [];
  const venueList = byId("opportunity-venue-options");
  if (venueList) {
    venueList.innerHTML = venueOptions
      .map((venue) => `<option value="${escapeHtml(venue)}"></option>`)
      .join("");
  }
  byId("opportunities-view")?.setAttribute("aria-busy", "false");
  hideStatus(byId("opportunity-loading"));
  hideError(byId("opportunity-error"));
  const bundleAvailable = payload?.availability?.status === "available";
  if (!bundleAvailable) {
    const reasonCode = payload?.availability?.reason || "complete_pointer_absent";
    const cohortBadge = byId("opportunity-cohort-status");
    cohortBadge.textContent = "Bundle unavailable";
    cohortBadge.removeAttribute("title");
    cohortBadge.setAttribute("aria-label", "Route opportunity cohort unavailable");
    showStatus(
      byId("opportunity-bundle-unavailable"),
      `${opportunityReason(reasonCode)} No route inventory is inferred. No numeric zero is inferred.`,
      "warning",
    );
    ["strict", "estimate", "unavailable"].forEach((name) => {
      byId(`${name}-opportunities`).hidden = true;
      byId(`${name}-opportunity-body`).innerHTML = "";
      byId(`${name}-opportunity-empty`).hidden = true;
      byId(`${name}-opportunity-count`).textContent = "Unavailable";
    });
    showStatus(
      byId("opportunity-status"),
      "Synchronized route opportunity publication is unavailable.",
      "warning",
    );
    if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
    return;
  }

  hideStatus(byId("opportunity-bundle-unavailable"));
  const routes = Array.isArray(payload.routes) ? payload.routes : [];
  const strict = routes.filter((route) => (
    route?.opportunity_class === "executable_candidate"
    && route?.availability?.status === "available"
  ));
  const estimates = routes.filter((route) => (
    route?.opportunity_class === "research_estimate"
    && route?.availability?.status === "available"
  ));
  const unavailable = routes.filter((route) => (
    route?.opportunity_class === "unavailable"
    || route?.availability?.status !== "available"
  ));
  const visible = opportunityInventoryVisibility(payload.filters || {});
  renderOpportunityInventory("strict", strict, visible.strict);
  renderOpportunityInventory("estimate", estimates, visible.estimate);
  renderOpportunityInventory("unavailable", unavailable, visible.unavailable);
  const cohortId = payload?.metadata?.route_cohort_id || "Published cohort";
  const cohortBadge = byId("opportunity-cohort-status");
  const compactCohortId = cohortId.length > 30
    ? `${cohortId.slice(0, 15)}…${cohortId.slice(-10)}`
    : cohortId;
  cohortBadge.textContent = compactCohortId;
  cohortBadge.setAttribute("title", cohortId);
  cohortBadge.setAttribute("aria-label", `Route opportunity cohort ${cohortId}`);
  const checkedAt = payload?.metadata?.checked_at
    ? ` · checked ${formatUtcTimestamp(payload.metadata.checked_at)}`
    : "";
  const maxAge = Number(payload?.metadata?.max_route_age_seconds);
  const maxSkew = Number(payload?.metadata?.max_route_skew_seconds);
  const sla = Number.isFinite(maxAge) && Number.isFinite(maxSkew)
    ? ` · SLA age ≤ ${rawVolume.format(maxAge)}s · skew ≤ ${rawVolume.format(maxSkew)}s`
    : "";
  showStatus(
    byId("opportunity-status"),
    `${routes.length} published route ${routes.length === 1 ? "scenario" : "scenarios"} match the current filters${checkedAt}${sla}.`,
    routes.length ? "success" : "warning",
  );
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

function defaultOpportunityDirection(sort) {
  return ["route_age_seconds", "skew_seconds"].includes(sort)
    ? "asc"
    : "desc";
}

function normalizedOpportunityFilters(filters = {}) {
  const sort = filters.sort || "net_edge_usd";
  return {
    token: filters.token || "",
    venue: filters.venue || "",
    notionalUsd: filters.notionalUsd || "",
    opportunityClass: filters.opportunityClass || "all",
    routeType: filters.routeType || "all",
    availability: filters.availability || "all",
    sort,
    dir: ["asc", "desc"].includes(filters.dir)
      ? filters.dir
      : defaultOpportunityDirection(sort),
  };
}

function hydrateOpportunityControls(route) {
  const filters = normalizedOpportunityFilters(route?.filters || {});
  const values = {
    "opportunity-token": filters.token,
    "opportunity-venue": filters.venue,
    "opportunity-notional": filters.notionalUsd ? String(filters.notionalUsd) : "",
    "opportunity-class": filters.opportunityClass,
    "opportunity-route-type": filters.routeType,
    "opportunity-availability": filters.availability,
    "opportunity-sort": filters.sort,
    "opportunity-direction": filters.dir,
  };
  Object.entries(values).forEach(([id, value]) => {
    const control = byId(id);
    if (control) control.value = value;
  });
  setOpportunityFilterValidation(route?.validationErrors || []);
  return filters;
}

function opportunityFilterErrorMessage(errors = []) {
  const fields = new Set(errors.map((error) => error?.field));
  if (fields.has("token")) {
    return "Token is invalid. Use 1–64 letters, numbers, dots, underscores, or hyphens.";
  }
  if (fields.has("venue")) {
    return "Venue is invalid. Use one exact CEX or DEX label shown by this publication.";
  }
  return "Opportunity filters are invalid. Correct them before applying.";
}

function setOpportunityFilterValidation(errors = []) {
  const rows = Array.isArray(errors) ? errors : [];
  ["token", "venue"].forEach((field) => {
    const control = byId(`opportunity-${field}`);
    if (!control) return;
    control.setAttribute(
      "aria-invalid",
      String(rows.some((error) => error?.field === field)),
    );
  });
  const banner = byId("opportunity-filter-error");
  if (!banner) return rows.length === 0;
  if (rows.length) showError(banner, opportunityFilterErrorMessage(rows));
  else hideError(banner);
  return rows.length === 0;
}

function opportunityFiltersFromControls() {
  return normalizedOpportunityFilters({
    token: byId("opportunity-token")?.value.trim().toUpperCase() || "",
    venue: byId("opportunity-venue")?.value.trim().toLowerCase() || "",
    notionalUsd: byId("opportunity-notional")?.value || "",
    opportunityClass: byId("opportunity-class")?.value || "all",
    routeType: byId("opportunity-route-type")?.value || "all",
    availability: byId("opportunity-availability")?.value || "all",
    sort: byId("opportunity-sort")?.value || "net_edge_usd",
    dir: byId("opportunity-direction")?.value || "desc",
  });
}

function clearOpportunityFilterResult(errors) {
  invalidateOpportunityRequest();
  app.opportunities = null;
  byId("opportunities-view")?.setAttribute("aria-busy", "false");
  hideStatus(byId("opportunity-loading"));
  hideStatus(byId("opportunity-status"));
  hideStatus(byId("opportunity-bundle-unavailable"));
  hideError(byId("opportunity-error"));
  setOpportunityFilterValidation(errors);
  const badge = byId("opportunity-cohort-status");
  badge.textContent = "Filters invalid";
  badge.removeAttribute("title");
  badge.setAttribute("aria-label", "Route opportunity filters invalid");
  ["strict", "estimate", "unavailable"].forEach((name) => {
    byId(`${name}-opportunities`).hidden = true;
    byId(`${name}-opportunity-body`).innerHTML = "";
    byId(`${name}-opportunity-empty`).hidden = true;
    byId(`${name}-opportunity-count`).textContent = "Unavailable";
  });
}

function opportunityRequestKey(filters) {
  return navigation?.buildOpportunitiesPath(normalizedOpportunityFilters(filters))
    || "/opportunities";
}

function opportunityRequestIsOwned(requestId, requestKey) {
  return requestId === app.opportunityRequestId
    && app.route?.kind === "opportunities"
    && opportunityRequestKey(app.route.filters || {}) === requestKey;
}

function opportunityResponseFiltersMatch(payloadFilters, requestedFilters) {
  if (
    !payloadFilters
    || typeof payloadFilters !== "object"
    || Array.isArray(payloadFilters)
  ) return false;
  const expected = {
    token: requestedFilters.token || null,
    venue: requestedFilters.venue || null,
    notional_usd: requestedFilters.notionalUsd
      ? String(requestedFilters.notionalUsd)
      : null,
    opportunity_class: requestedFilters.opportunityClass,
    route_type: requestedFilters.routeType,
    availability: requestedFilters.availability,
    sort: requestedFilters.sort,
    direction: requestedFilters.dir,
  };
  const actualFields = Object.keys(payloadFilters).sort();
  const expectedFields = Object.keys(expected).sort();
  if (
    actualFields.length !== expectedFields.length
    || actualFields.some((field, index) => field !== expectedFields[index])
  ) return false;
  return Object.entries(expected).every(([field, value]) => {
    const actual = payloadFilters[field];
    return actual === value;
  });
}

function opportunityVenueFromMarketId(marketId) {
  if (typeof marketId !== "string") return null;
  const parts = marketId.split(":");
  if (parts[0] === "cex" && parts.length >= 3) return parts[1];
  if (parts[0] === "dex" && parts.length >= 5) return parts[2];
  return null;
}

function opportunityRowMatchesRequest(row, filters) {
  if (!row || typeof row !== "object") return false;
  if (typeof row.requested_notional_usd !== "string") return false;
  if (filters.token && row.token_symbol !== filters.token) return false;
  if (filters.venue) {
    const venues = [row.buy_market_id, row.sell_market_id]
      .map(opportunityVenueFromMarketId);
    if (!venues.includes(filters.venue)) return false;
  }
  if (
    filters.notionalUsd
    && row.requested_notional_usd !== String(filters.notionalUsd)
  ) return false;
  const classByFilter = {
    strict: "executable_candidate",
    estimate: "research_estimate",
  };
  if (
    filters.opportunityClass !== "all"
    && row.opportunity_class !== classByFilter[filters.opportunityClass]
  ) return false;
  if (filters.routeType !== "all" && row.route_type !== filters.routeType) {
    return false;
  }
  if (
    filters.availability !== "all"
    && row?.availability?.status !== filters.availability
  ) return false;
  return true;
}

function opportunityIdentityComparison(left, right) {
  for (const field of ["route_id", "opportunity_id"]) {
    const leftValue = String(left?.[field] || "");
    const rightValue = String(right?.[field] || "");
    if (leftValue < rightValue) return -1;
    if (leftValue > rightValue) return 1;
  }
  return 0;
}

function opportunityDecimalParts(value) {
  if (!["string", "number"].includes(typeof value)) return null;
  if (
    typeof value === "number"
    && (
      !Number.isFinite(value)
      || (Number.isInteger(value) && !Number.isSafeInteger(value))
    )
  ) return null;
  const match = String(value).match(/^([+-]?)(\d+)(?:\.(\d+))?$/);
  if (!match) return null;
  const integer = match[2].replace(/^0+(?=\d)/, "");
  const fraction = (match[3] || "").replace(/0+$/, "");
  const zero = integer === "0" && !fraction;
  return {
    negative: match[1] === "-" && !zero,
    integer,
    fraction,
  };
}

function opportunityDecimalComparison(left, right) {
  const leftParts = opportunityDecimalParts(left);
  const rightParts = opportunityDecimalParts(right);
  if (!leftParts || !rightParts) return null;
  if (leftParts.negative !== rightParts.negative) {
    return leftParts.negative ? -1 : 1;
  }
  let magnitude = 0;
  if (leftParts.integer.length !== rightParts.integer.length) {
    magnitude = leftParts.integer.length < rightParts.integer.length ? -1 : 1;
  } else if (leftParts.integer !== rightParts.integer) {
    magnitude = leftParts.integer < rightParts.integer ? -1 : 1;
  } else {
    const width = Math.max(
      leftParts.fraction.length,
      rightParts.fraction.length,
    );
    const leftFraction = leftParts.fraction.padEnd(width, "0");
    const rightFraction = rightParts.fraction.padEnd(width, "0");
    if (leftFraction !== rightFraction) {
      magnitude = leftFraction < rightFraction ? -1 : 1;
    }
  }
  return leftParts.negative ? -magnitude : magnitude;
}

function opportunitySortComparison(left, right, filters) {
  const field = filters.sort === "volume"
    ? "route_volume_usd"
    : filters.sort;
  const leftRaw = left?.[field];
  const rightRaw = right?.[field];
  const leftMissing = leftRaw === null || leftRaw === undefined;
  const rightMissing = rightRaw === null || rightRaw === undefined;
  if (leftMissing || rightMissing) {
    if (leftMissing !== rightMissing) return leftMissing ? 1 : -1;
    return opportunityIdentityComparison(left, right);
  }
  let comparison = 0;
  if (["token_symbol", "route_id"].includes(filters.sort)) {
    const leftValue = String(leftRaw);
    const rightValue = String(rightRaw);
    comparison = leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  } else {
    comparison = opportunityDecimalComparison(leftRaw, rightRaw);
    if (comparison === null) return null;
  }
  if (comparison !== 0 && filters.dir === "desc") comparison *= -1;
  return comparison || opportunityIdentityComparison(left, right);
}

function opportunityResponseMatchesRequest(payload, requestedFilters) {
  if (!opportunityResponseFiltersMatch(payload?.filters, requestedFilters)) {
    return false;
  }
  const routes = Array.isArray(payload?.routes) ? payload.routes : [];
  if (!routes.every((row) => opportunityRowMatchesRequest(row, requestedFilters))) {
    return false;
  }
  const returnedCount = payload?.metadata?.coverage?.returned_count;
  if (!Number.isInteger(returnedCount) || returnedCount !== routes.length) {
    return false;
  }
  for (let index = 1; index < routes.length; index += 1) {
    const comparison = opportunitySortComparison(
      routes[index - 1], routes[index], requestedFilters,
    );
    if (comparison === null || comparison > 0) return false;
  }
  return true;
}

function invalidateOpportunityRequest() {
  if (app.opportunityController) app.opportunityController.abort();
  app.opportunityController = null;
  app.opportunityRequestId += 1;
  return app.opportunityRequestId;
}

function clearOpportunityResult(message) {
  app.opportunities = null;
  byId("opportunities-view")?.setAttribute("aria-busy", "false");
  hideStatus(byId("opportunity-loading"));
  showError(byId("opportunity-error"), message);
  showStatus(
    byId("opportunity-bundle-unavailable"),
    "The published opportunity bundle could not be validated. No route inventory or numeric zero is inferred.",
    "critical",
  );
  byId("opportunity-cohort-status").textContent = "Bundle invalid";
  ["strict", "estimate", "unavailable"].forEach((name) => {
    byId(`${name}-opportunities`).hidden = true;
    byId(`${name}-opportunity-body`).innerHTML = "";
    byId(`${name}-opportunity-empty`).hidden = true;
    byId(`${name}-opportunity-count`).textContent = "Unavailable";
  });
}

async function loadOpportunities(filters = app.route?.filters || {}) {
  const normalized = normalizedOpportunityFilters(filters);
  const requestKey = opportunityRequestKey(normalized);
  const requestId = invalidateOpportunityRequest();
  const controller = new AbortController();
  app.opportunityController = controller;
  const pageUrl = new URL(requestKey, "https://dashboard.invalid");
  const apiUrl = `/api/markets/opportunities${pageUrl.search}`;
  byId("opportunities-view")?.setAttribute("aria-busy", "true");
  hideError(byId("opportunity-error"));
  showStatus(byId("opportunity-loading"), "Loading synchronized route opportunities…");
  try {
    const response = await fetch(apiUrl, { signal: controller.signal });
    const payload = await responseJson(response);
    if (!response.ok) {
      throw new Error(
        payload.message
        || payload.error
        || "Opportunity publication failed to load.",
      );
    }
    if (!opportunityRequestIsOwned(requestId, requestKey)) return false;
    if (
      !payload?.availability
      || !["available", "unavailable"].includes(payload.availability.status)
      || !Array.isArray(payload.routes)
    ) {
      throw new Error("The opportunity response failed its compact payload contract.");
    }
    if (!opportunityResponseMatchesRequest(payload, normalized)) {
      throw new Error(
        "The opportunity response failed its request-bound payload contract.",
      );
    }
    renderOpportunities(payload);
    return true;
  } catch (error) {
    if (error.name === "AbortError" || !opportunityRequestIsOwned(requestId, requestKey)) {
      return false;
    }
    clearOpportunityResult(
      publicErrorMessage(error, "Opportunity publication failed to load."),
    );
    return false;
  } finally {
    if (requestId === app.opportunityRequestId) app.opportunityController = null;
  }
}

function setFactValue(
  id,
  available,
  displayValue,
  reason,
  disclosureOptions = {},
) {
  const element = byId(id);
  if (!element) return;
  if (available) {
    element.innerHTML = "";
    element.textContent = String(displayValue);
    return;
  }
  element.textContent = "";
  element.innerHTML = naFactMarkup(reason, disclosureOptions);
}

function snapshotRetryable(market, fact) {
  return market?.[`${fact}_retryable`] === true;
}

function snapshotMissingReason(market, fact, fallback) {
  const status = market?.[`${fact}_status`] || "unavailable";
  const reasonCode = market?.[`${fact}_na_reason`];
  const observedAt = market?.[`${fact}_observed_at`];
  const reason = reasonCode
    ? DAILY_QUALITY_REASON_LABELS[reasonCode]
      || reasonCode.replaceAll("_", " ")
    : fallback;
  const lastAttempt = observedAt
    ? `Last collection attempt: ${formatUtcTimestamp(observedAt)}.`
    : "Last collection attempt time is not published.";
  return `${reason} Status: ${status}. ${lastAttempt}`;
}

function dailyMarketMissingReason(market, fallback) {
  const reasonCode = market?.current_listing_reason_code;
  if (![
    "instrument_absent_from_current_catalog",
    "official_catalog_evidence_stale",
  ].includes(reasonCode)) return fallback;
  const reason = DAILY_QUALITY_REASON_LABELS[reasonCode]
    || reasonCode.replaceAll("_", " ");
  const checked = market?.current_listing_checked_at
    ? ` Official catalog checked ${formatUtcTimestamp(market.current_listing_checked_at)}.`
    : " Official catalog check time is not published.";
  return `${reason}. Current daily facts remain N/A, not zero.${checked}`;
}

function snapshotMarketContextLabel(market) {
  if (!market) return "";
  return [
    market.market_type?.toUpperCase(),
    market.venue,
    market.instrument || market.pool_address,
  ].filter(Boolean).join(" · ");
}

function screenerDepthMarkup(market, token) {
  if (finite(market?.total_depth_100bps_usd)) {
    return formatDepth(market.total_depth_100bps_usd, market.depth_100bps_complete);
  }
  return naFactMarkup(snapshotMissingReason(
    market,
    "depth",
    "No measured ±100 bps depth is published.",
  ), {
    retryable: snapshotRetryable(market, "depth"),
    token,
    marketId: market?.refresh_market_id,
    marketLabel: snapshotMarketContextLabel(market),
    fact: "depth",
    factLabel: "executable depth",
    bandBps: 100,
  });
}

function screenerQualityMarkup(token, statusCounts, countsComplete) {
  if (!countsComplete) {
    return naFactMarkup(
      "Catalog quality counts are incomplete for this Token.",
      { token, factLabel: "catalog quality counts" },
    );
  }
  const chips = [
    ["critical", "Critical"],
    ["warning", "Warning"],
    ["info", "Info"],
  ].filter(([severity]) => finite(statusCounts?.[severity]) && statusCounts[severity] > 0)
    .map(([severity, label]) => {
      const path = navigation
        ? navigation.buildWorkspacePath(token, "quality", {
          ...currentSummaryWindowRouteState(),
          scope: "all",
          severity,
          origin: "screener",
        })
        : "#";
      return `<a class="quality-count-chip" data-severity="${severity}" `
        + `href="${escapeHtml(path)}">${statusCounts[severity]} ${label} `
        + `reason${statusCounts[severity] === 1 ? "" : "s"}</a>`;
    });
  return chips.length
    ? `<span class="quality-count-chips">${chips.join("")}</span>`
    : qualityStateMarkup("ok");
}

function screenerTokenRow(tokenSummary) {
  const token = tokenSummary.token_symbol;
  const { cex, dex, spread } = comparison(tokenSummary);
  const aggregates = aggregateFacts(tokenSummary, [], []);
  const statusCounts = tokenSummary.quality_status_counts || null;
  const alertCounts = tokenSummary.quality_alert_counts || statusCounts;
  const catalogCount = tokenSummary.market_count;
  const qualityCountTotal = statusCounts
    ? Object.values(statusCounts).reduce((total, value) => (
        total + (finite(value) ? value : 0)
      ), 0)
    : null;
  const qualityCountsComplete = (
    finite(catalogCount)
    && catalogCount > 0
    && qualityCountTotal === catalogCount
  );
  const aggregateValue = formatCurrency(aggregates.aggregateTotal);
  const priceGapValue = formatPercent(spread);
  const rankValue = formatRankValue(tokenSummary);
  const researchPath = navigation
    ? navigation.buildWorkspacePath(
      token,
      "markets",
      currentSummaryWindowRouteState(),
    )
    : "#";
  return `<tr class="token-row screener-token-row">
    <td data-label="Token" class="sticky-token token-name">${escapeHtml(token)}</td>
    <td data-label="Rank value" class="rank-value">
      ${rankValue === null
        ? naFactMarkup(
          "The selected ranking metric has no valid source observation for this Token and window.",
          { token, factLabel: "selected ranking metric" },
        )
        : escapeHtml(rankValue)}
    </td>
    <td data-label="Covered markets">
      ${tokenSummary.cex_market_count ?? "—"} CEX · ${tokenSummary.dex_market_count ?? "—"} DEX
    </td>
    <td data-label="Aggregate USD volume">
      ${finite(aggregates.aggregateTotal)
        ? screenerMetricTooltip(
          aggregateValue,
          `CEX ${finite(aggregates.aggregateCex) ? formatCurrency(aggregates.aggregateCex) : "unavailable"} · `
            + `DEX ${finite(aggregates.aggregateDex) ? formatCurrency(aggregates.aggregateDex) : "unavailable"}`,
        )
        : naFactMarkup(
          "No finite daily volume is available in the selected window.",
          { token, factLabel: "aggregate daily USD volume" },
        )}
    </td>
    <td data-label="DEX share">${finite(aggregates.aggregateDexShare)
      ? formatShare(aggregates.aggregateDexShare)
      : naFactMarkup(
        "DEX volume share cannot be calculated because aggregate CEX/DEX volume is unavailable or zero.",
        { token, factLabel: "DEX volume share" },
      )}</td>
    <td data-label="Primary DEX/CEX basis" class="${metricClass(spread)}">
      ${finite(spread)
        ? screenerMetricTooltip(priceGapValue, "Primary DEX / CEX − 1.")
        : naFactMarkup(
          "Primary CEX and DEX prices are not comparable on a common observed date.",
          { token, factLabel: "primary cross-venue price gap" },
        )}
    </td>
    <td data-label="Primary ±100 bps depth">
      <span class="depth-pair-values">${screenerDepthMarkup(cex, token)} / ${screenerDepthMarkup(dex, token)}</span>
    </td>
    <td data-label="Primary DEX TVL">${finite(dex?.tvl_usd)
      ? formatCurrency(dex.tvl_usd)
      : naFactMarkup(snapshotMissingReason(
        dex,
        "tvl",
        "No DEX TVL value is published.",
      ), {
        retryable: snapshotRetryable(dex, "tvl"),
        token,
        marketId: dex?.refresh_market_id,
        marketLabel: snapshotMarketContextLabel(dex),
        fact: "tvl",
        factLabel: "pool TVL",
      })}</td>
    <td data-label="Quality">
      ${screenerQualityMarkup(token, alertCounts, qualityCountsComplete)}
    </td>
    <td data-label="Research">
      <a class="route-action" href="${escapeHtml(researchPath)}" data-open-token="${escapeHtml(token)}">Open workspace</a>
    </td>
  </tr>`;
}

function renderTable() {
  if (!app.payload) return;
  ensureSelections();
  const query = app.searchQuery;
  const tokens = app.payload.tokens
    .filter((row) => !query || row.token_symbol.includes(query))
    .sort(compareScreenerTokens);
  app.visibleTokens = tokens;

  byId("market-body").innerHTML = tokens.length
    ? tokens.map((token) => screenerTokenRow(token)).join("")
    : `<tr><td data-label="Result" colspan="10" class="missing">No Token matches this search.</td></tr>`;
  byId("row-count").textContent = `${tokens.length} Tokens · one row per Token`;
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

const SNAPSHOT_REFRESH_JOB_ID = /^[0-9a-f]{32}$/;
const SNAPSHOT_REFRESH_TERMINAL_STATUSES = new Set([
  "succeeded",
  "partial",
  "failed",
  "interrupted",
]);
const SNAPSHOT_REFRESH_POLL_INTERVAL_MS = 2000;
const SNAPSHOT_REFRESH_MAX_POLLS = 300;

function snapshotRefreshInlineStatus(button) {
  return button?.closest?.(".na-disclosure-panel")
    ?.querySelector?.("[data-refresh-status]") || null;
}

function snapshotRefreshRouteContext() {
  const route = app.route || { kind: "unknown" };
  const routeState = route.kind === "workspace" ? route.state || {} : route.filters || {};
  const selected = route.kind === "workspace" ? selectedMarketSelection() : {};
  const location = globalThis.window?.location;
  return JSON.stringify({
    kind: route.kind || "unknown",
    page: route.kind === "workspace" ? route.page || "" : "",
    token: route.kind === "workspace" ? String(route.token || "").toUpperCase() : "",
    marketA: selected.marketA || routeState.marketA || "",
    marketB: selected.marketB || routeState.marketB || "",
    selection: selected.selection || routeState.selection || "",
    start: routeState.start || "",
    end: routeState.end || "",
    location: location ? `${location.pathname || ""}${location.search || ""}` : "",
  });
}

function invalidateSnapshotRefreshRequest({ clearFeedback = false } = {}) {
  if (app.snapshotRefreshController) app.snapshotRefreshController.abort();
  app.snapshotRefreshController = null;
  app.snapshotRefreshRequestId += 1;
  if (clearFeedback) {
    const globalStatus = byId("action-status");
    if (globalStatus) hideStatus(globalStatus);
  }
  return app.snapshotRefreshRequestId;
}

function beginSnapshotRefreshRequest(payload) {
  const requestId = invalidateSnapshotRefreshRequest();
  const controller = new AbortController();
  app.snapshotRefreshController = controller;
  return {
    requestId,
    controller,
    routeContext: snapshotRefreshRouteContext(),
    tokenSymbol: payload.token_symbol,
    marketId: payload.market_id,
    factType: payload.fact_type,
  };
}

function snapshotRefreshOwnerIsCurrent(owner) {
  if (!owner) return true;
  if (
    owner.requestId !== app.snapshotRefreshRequestId
    || owner.controller !== app.snapshotRefreshController
    || owner.controller.signal.aborted
    || owner.routeContext !== snapshotRefreshRouteContext()
  ) {
    return false;
  }
  if (app.route?.kind === "workspace") {
    return String(app.route.token || "").toUpperCase() === owner.tokenSymbol;
  }
  return true;
}

function showSnapshotRefreshFeedback(button, message, state = "info", owner = null) {
  if (!snapshotRefreshOwnerIsCurrent(owner)) return false;
  const inline = snapshotRefreshInlineStatus(button);
  if (inline) showStatus(inline, message, state);
  const globalStatus = byId("action-status");
  if (globalStatus) showStatus(globalStatus, message, state);
  return true;
}

function waitForSnapshotRefreshPoll() {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, SNAPSHOT_REFRESH_POLL_INTERVAL_MS);
  });
}

function snapshotRefreshFailureMessage(job, { reloaded = false } = {}) {
  const reasons = {
    snapshot_target_unresolved: (
      "The source completed, but the requested fact is still unavailable."
    ),
    snapshot_refresh_failed: "The collector could not complete the refresh.",
    snapshot_refresh_failed_after_publication: (
      "A new publication was written, but the collector did not finish cleanly."
    ),
    snapshot_publication_unreadable: (
      "The server could not verify the published snapshot."
    ),
    snapshot_refresh_no_longer_retryable: (
      "The fact is no longer eligible for an automatic refresh."
    ),
    process_interrupted: "The refresh worker was interrupted.",
  };
  const reason = reasons[job?.error_code]
    || "The refresh did not produce a verified observed fact.";
  const nextStep = job?.retryable
    ? "Retry this fact after the source recovers, or inspect Data Quality for the current reason."
    : "Inspect Data Quality for the current reason before taking further action.";
  const publication = reloaded
    ? " The latest validated publication was reloaded for the current page."
    : "";
  return `Job ${job?.job_id || "unavailable"} · ${job?.status || "failed"}. `
    + `${reason}${publication} The value remains N/A, not zero. ${nextStep}`;
}

async function pollSnapshotFactRefresh(button, jobId, owner = null) {
  for (let attempt = 0; attempt < SNAPSHOT_REFRESH_MAX_POLLS; attempt += 1) {
    if (!snapshotRefreshOwnerIsCurrent(owner)) return null;
    const response = await fetch(
      `/api/actions/jobs/${jobId}`,
      owner ? { signal: owner.controller.signal } : undefined,
    );
    const job = await responseJson(response);
    if (!snapshotRefreshOwnerIsCurrent(owner)) return null;
    if (!response.ok) {
      throw new Error(job.error || "Refresh job status is unavailable.");
    }
    if (job?.job_id !== jobId) {
      throw new Error("Refresh job status did not match the accepted job.");
    }
    const status = String(job.status || "").toLowerCase();
    if (status === "queued" || status === "running") {
      showSnapshotRefreshFeedback(
        button,
        `Job ${jobId} · ${status}${job.stage ? ` · ${job.stage}` : ""}. `
          + "The existing N/A remains visible until a validated snapshot is published.",
        "info",
        owner,
      );
      await waitForSnapshotRefreshPoll();
      if (!snapshotRefreshOwnerIsCurrent(owner)) return null;
      continue;
    }
    if (!SNAPSHOT_REFRESH_TERMINAL_STATUSES.has(status)) {
      throw new Error("Refresh job returned an invalid status.");
    }
    return { ...job, status };
  }
  throw new Error(
    "Refresh job is still running. Check Data Actions or retry status later; the current N/A was preserved.",
  );
}

async function reloadFactsAfterSnapshotRefresh(owner = null) {
  if (!snapshotRefreshOwnerIsCurrent(owner)) return false;
  const window = appliedTimeWindow();
  const loaded = await loadMarket(window.start, window.end, {
    preserve: Boolean(app.payload),
    refreshWorkspaceOnGenerationChange: false,
    responseIsOwned: () => snapshotRefreshOwnerIsCurrent(owner),
  });
  if (!loaded || !snapshotRefreshOwnerIsCurrent(owner)) return false;
  if (app.route?.kind !== "workspace") return true;
  app.catalogsByToken.clear();
  app.catalog = null;
  app.activeCatalogToken = "";
  app.activeCatalogKey = "";
  const applied = await applyRouteFromLocation({ preserveWorkspaceError: true });
  return snapshotRefreshOwnerIsCurrent(owner) && applied;
}

async function requestSnapshotFactRefresh(button) {
  if (!button || button.disabled) return false;
  const payload = {
    token_symbol: button.dataset.refreshToken || "",
    market_id: button.dataset.refreshMarketId || "",
    fact_type: button.dataset.refreshFact || "",
  };
  const owner = beginSnapshotRefreshRequest(payload);
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "Queuing refresh…";
  showSnapshotRefreshFeedback(
    button,
    `${payload.token_symbol} ${payload.fact_type.toUpperCase()} refresh is being submitted. `
      + "The current N/A is preserved.",
    "info",
    owner,
  );
  try {
    const response = await fetch("/api/actions/facts/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: owner.controller.signal,
    });
    const result = await responseJson(response);
    if (!snapshotRefreshOwnerIsCurrent(owner)) return false;
    if (!response.ok) throw new Error(result.error || "Fact refresh was rejected.");
    const jobId = String(result.job_id || "").toLowerCase();
    if (!SNAPSHOT_REFRESH_JOB_ID.test(jobId)) {
      throw new Error("Fact refresh returned an invalid public job ID.");
    }
    button.textContent = "Refresh in progress…";
    showSnapshotRefreshFeedback(
      button,
      `Job ${jobId} · ${result.status || "queued"}. `
        + "The N/A remains until a validated snapshot is published.",
      "info",
      owner,
    );
    const job = await pollSnapshotFactRefresh(button, jobId, owner);
    if (!job || !snapshotRefreshOwnerIsCurrent(owner)) return false;
    if (job.status === "succeeded") {
      const reloaded = await reloadFactsAfterSnapshotRefresh(owner);
      if (!snapshotRefreshOwnerIsCurrent(owner)) return false;
      if (!reloaded) {
        throw new Error(
          "The refresh succeeded, but the current page could not reload the validated facts. Reload the page to view the new publication.",
        );
      }
      button.textContent = "Refresh complete";
      showSnapshotRefreshFeedback(
        button,
        `Job ${jobId} · succeeded. The latest validated facts were reloaded for the current page.`,
        "success",
        owner,
      );
      return true;
    }
    let reloaded = false;
    if (job.publication_committed === true) {
      reloaded = await reloadFactsAfterSnapshotRefresh(owner);
    }
    if (!snapshotRefreshOwnerIsCurrent(owner)) return false;
    button.disabled = false;
    button.textContent = originalText;
    showSnapshotRefreshFeedback(
      button,
      snapshotRefreshFailureMessage(job, { reloaded }),
      "critical",
      owner,
    );
    return false;
  } catch (error) {
    if (error.name === "AbortError" || !snapshotRefreshOwnerIsCurrent(owner)) return false;
    button.disabled = false;
    button.textContent = originalText;
    const message = publicErrorMessage(error, "Fact refresh is unavailable.");
    showSnapshotRefreshFeedback(
      button,
      `${message} The value remains N/A, not zero. Retry later or inspect Data Quality for the current reason.`,
      "critical",
      owner,
    );
    return false;
  } finally {
    const sequenceStillCurrent = owner.requestId === app.snapshotRefreshRequestId;
    if (!sequenceStillCurrent && owner.routeContext === snapshotRefreshRouteContext()) {
      button.disabled = false;
      button.textContent = originalText;
    }
    if (sequenceStillCurrent) {
      app.snapshotRefreshController = null;
    }
  }
}

function payloadMarketForCatalog(market) {
  if (!market) return null;
  return market.window_metrics || null;
}

function qualityStateMarkup(status, label = "", tooltip = "") {
  const normalized = String(status || "unavailable").toLowerCase();
  const display = label || (
    normalized === "ok"
      ? "No active alerts"
      : normalized.replaceAll("_", " ")
  );
  const tooltipAttributes = tooltip
    ? ` tabindex="0" aria-label="${escapeHtml(`${display}. ${tooltip}`)}" data-tooltip="${escapeHtml(tooltip)}"`
    : "";
  return `<span class="quality-state" data-state="${escapeHtml(normalized)}"${tooltipAttributes}>${escapeHtml(display)}</span>`;
}

function renderWorkspaceContext() {
  if (!app.catalog || !app.payload) return;
  const token = selectedWorkspaceToken();
  const markets = factsMarketsForToken(token);
  const tokenSummary = app.payload.tokens.find((row) => row.token_symbol === token);
  const cexCount = markets.filter((market) => market.market_type === "cex").length;
  const dexCount = markets.length - cexCount;
  const counts = markets.reduce((result, market) => {
    const status = market.quality_status || "ok";
    result[status] = (result[status] || 0) + 1;
    return result;
  }, {});
  const qualityState = counts.critical ? "critical" : counts.warning ? "warning" : "ok";
  const qualityText = counts.critical
    ? `${counts.critical} critical · ${counts.warning || 0} warning`
    : counts.warning
      ? `${counts.warning} warning${counts.warning === 1 ? "" : "s"}`
      : "No active alerts";
  setWorkspacePageIdentity(token, app.route?.page);
  byId("workspace-market-count").textContent = `${cexCount} CEX · ${dexCount} DEX`;
  const snapshotTimes = markets.flatMap((market) => [
    market.depth_observed_at,
    market.tvl_observed_at,
  ]).filter(Boolean).sort();
  byId("workspace-as-of").textContent = snapshotTimes.length
    ? `Latest snapshot ${formatUtcTimestamp(snapshotTimes.at(-1))}`
    : `Daily through ${app.payload.metadata.available_end || "unavailable"}`;
  byId("workspace-quality-status").textContent = qualityText;
  byId("workspace-quality-status").dataset.state = qualityState;
  if (tokenSummary) {
    const aggregates = aggregateFacts(tokenSummary, [], []);
    const aggregateText = finite(aggregates.aggregateTotal)
      ? `${formatCurrency(aggregates.aggregateTotal)} aggregate window volume`
      : "Aggregate window volume unavailable";
    const shareText = finite(aggregates.aggregateDexShare)
      ? `${formatShare(aggregates.aggregateDexShare)} DEX share`
      : "DEX share unavailable";
    byId("workspace-description").textContent = `${aggregateText} · ${shareText}. `
      + "Applied market identities stay shared across the four research pages.";
  }
  const pair = selectedPairState();
  const byMarketId = new Map(markets.map((market) => [market.market_id, market]));
  byId("research-market-a").textContent = pair.marketA
    ? factsMarketLabel(byMarketId.get(pair.marketA))
    : "Not selected";
  byId("research-market-b").textContent = pair.marketB
    ? factsMarketLabel(byMarketId.get(pair.marketB))
    : "Not selected";
  updateRouteLinks();
}

function renderWorkspaceMarkets() {
  if (!app.catalog || !app.payload || !byId("workspace-market-body")) return;
  const token = selectedWorkspaceToken();
  const pair = selectedPairState();
  const markets = factsMarketsForToken(token).filter((market) => (
    app.workspaceMarketType === "all" || market.market_type === app.workspaceMarketType
  ));
  byId("workspace-market-body").innerHTML = markets.length
    ? markets.map((market) => {
        const row = payloadMarketForCatalog(market);
        const flags = factsMarketWarningFlags(market);
        const rowQualityStatus = flags.length
          ? factsMarketWarningSeverity(market, flags)
          : market.quality_status || "ok";
        const rowQualityLabel = rowQualityStatus === "info" ? "Informational" : "";
        const selectedA = pair.marketA === market.market_id;
        const selectedB = pair.marketB === market.market_id;
        const depth = finite(market.total_depth_100bps_usd)
          ? formatDepth(
            market.total_depth_100bps_usd,
            market.depth_100bps_complete,
          )
          : naFactMarkup(
            snapshotMissingReason(
              market,
              "depth",
              "No measured ±100 bps depth is published.",
            ),
            {
              retryable: snapshotRetryable(market, "depth"),
              token,
              marketId: market.market_id,
              marketLabel: factsMarketLabel(market),
              fact: "depth",
              factLabel: "executable depth",
              bandBps: 100,
            },
          );
        const tvlValue = firstFinite(row?.tvl_usd, market.tvl_usd);
        const tvl = market.market_type === "cex"
          ? naFactMarkup(
            "TVL is not applicable to a centralized order book.",
            {
              token,
              marketLabel: factsMarketLabel(market),
              factLabel: "pool TVL",
            },
          )
          : finite(tvlValue)
            ? formatCurrency(tvlValue)
            : naFactMarkup(
              snapshotMissingReason(
                market,
                "tvl",
                "No DEX TVL value is published.",
              ),
              {
                retryable: snapshotRetryable(market, "tvl"),
                token,
                marketId: market.market_id,
                marketLabel: factsMarketLabel(market),
                fact: "tvl",
                factLabel: "pool TVL",
              },
            );
        const identityMeta = [
          market.market_type.toUpperCase(),
          market.chain,
          market.source_quote_asset_label,
        ].filter(Boolean).join(" · ");
        return `<tr>
          <td data-label="Market">
            <span class="market-identity">
              <strong>${escapeHtml(factsMarketLabel(market))}</strong>
              <small>${escapeHtml(identityMeta)}</small>
              ${market.pool_address ? `<small>${escapeHtml(market.pool_address)}</small>` : ""}
            </span>
          </td>
          <td data-label="Type">${qualityStateMarkup(market.market_type, market.market_type.toUpperCase())}</td>
          <td data-label="Window price">${finite(row?.price_usd)
            ? formatPrice(row.price_usd)
            : naFactMarkup(
              dailyMarketMissingReason(
                market,
                "No finite daily close is available for this market in the selected window.",
              ),
              { token, marketLabel: factsMarketLabel(market), factLabel: "daily close" },
            )}</td>
          <td data-label="Window volume">${finite(row?.volume_usd)
            ? formatCurrency(row.volume_usd)
            : naFactMarkup(
              dailyMarketMissingReason(
                market,
                "No finite daily USD volume is available for this market in the selected window.",
              ),
              { token, marketLabel: factsMarketLabel(market), factLabel: "daily USD volume" },
            )}</td>
          <td data-label="TVL">${tvl}</td>
          <td data-label="±100 bps depth">${depth}<span class="metric-note">${escapeHtml(market.depth_status || "unavailable")}</span></td>
          <td data-label="Coverage">${finite(row?.coverage_ratio)
            ? formatRatio(row?.coverage_ratio)
            : naFactMarkup(
              "Coverage is unavailable because no valid daily observation count was published.",
              { token, marketLabel: factsMarketLabel(market), factLabel: "daily coverage" },
            )}</td>
          <td data-label="Quality">
            ${qualityStateMarkup(rowQualityStatus, rowQualityLabel)}
            <span class="metric-note">${flags.length} reason${flags.length === 1 ? "" : "s"}</span>
          </td>
          <td data-label="Market selection">
            <span class="pair-actions">
              <button
                type="button"
                class="pair-action ${selectedA ? "selected" : ""}"
                data-set-market-slot="a"
                data-market-id="${escapeHtml(market.market_id)}"
                aria-pressed="${String(selectedA)}"
              >Set A</button>
              <button
                type="button"
                class="pair-action ${selectedB ? "selected" : ""}"
                data-set-market-slot="b"
                data-market-id="${escapeHtml(market.market_id)}"
                aria-pressed="${String(selectedB)}"
              >Set B</button>
            </span>
          </td>
        </tr>`;
      }).join("")
    : '<tr><td colspan="9" class="missing">No markets match this filter.</td></tr>';
}

function catalogQualityPayload() {
  if (!app.catalog) return { token_symbol: "", markets: [] };
  const token = selectedWorkspaceToken();
  const selected = new Set(Object.values(selectedPairState()).filter(Boolean));
  const scope = effectiveQualityScope();
  const markets = factsMarketsForToken(token)
    .filter((market) => scope === "all" || selected.has(market.market_id))
    .map((market) => {
      const row = payloadMarketForCatalog(market);
      const dailyCoverage = firstFinite(market.coverage_ratio, row?.coverage_ratio);
      const dailyObservationDays = firstFinite(
        market.observation_days,
        row?.observation_days,
      );
      const coverageThreshold = (
        app.catalog.metadata.market_quality_thresholds?.minimum_primary_coverage_ratio ?? 0.8
      );
      const dailyStatus = !finite(dailyCoverage)
        || (
          dailyCoverage === 0
          && (!finite(dailyObservationDays) || dailyObservationDays === 0)
        )
        ? "unavailable"
        : dailyCoverage < coverageThreshold
          ? "warning"
          : "observed";
      const dailyMessage = finite(dailyObservationDays)
        ? `${dailyObservationDays} observed daily closes`
        : "Daily observation count is unavailable.";
      return {
        market,
        quality_flags: factsMarketWarningFlags(market),
        screening_quality_status: market.screening_quality_status,
        screening_quality_flags: Array.isArray(market.screening_quality_flags)
          ? market.screening_quality_flags
          : [],
        facts: {
          daily: {
            status: dailyStatus,
            observed_at: market.observed_end,
            method: market.daily_volatility_method,
            observed_value: dailyCoverage ?? null,
            message: dailyMessage,
          },
          tvl: market.market_type === "cex"
            ? { status: "not_applicable", message: "TVL does not apply to a CEX order book." }
            : {
                status: row?.tvl_status || market.tvl_status || "unavailable",
                observed_at: row?.tvl_observed_at || market.tvl_observed_at,
                method: row?.tvl_method || market.tvl_method,
                observed_value: firstFinite(row?.tvl_usd, market.tvl_usd),
              },
          depth: {
            status: market.depth_status || "unavailable",
            observed_at: market.depth_observed_at,
            method: market.depth_method,
            observed_value: market.total_depth_100bps_usd,
          },
          execution: {
            status: "unavailable",
            message: "Execution quality is loading from its separate source snapshot.",
          },
        },
      };
    });
  return { token_symbol: token, markets };
}

function qualityFactMarkup(name, fact) {
  const status = fact?.status || "unavailable";
  const value = fact?.observed_value ?? fact?.value_usd;
  let valueText = "";
  if (name === "daily" && finite(fact?.coverage_ratio ?? value)) {
    valueText = `${formatRatio(fact.coverage_ratio ?? value)} coverage`;
  } else if (name === "depth") {
    const depth100 = fact?.bands_bps?.["100"]?.total_usd ?? value;
    if (finite(depth100)) valueText = `${formatCurrency(depth100)} at ±100 bps`;
  } else if (name === "tvl" && finite(value)) {
    valueText = formatCurrency(value);
  } else if (name === "execution" && finite(fact?.scenario_count)) {
    valueText = `${fact.scenario_count} collected scenarios`;
  } else if (value !== null && value !== undefined && value !== "") {
    valueText = String(value);
  }
  const observedText = fact?.observed_at
    ? formatUtcTimestamp(fact.observed_at)
    : "";
  const reasonCode = fact?.reason_code || "";
  const reasonLabel = reasonCode
    ? DAILY_QUALITY_REASON_LABELS[reasonCode]
      || reasonCode.replaceAll("_", " ")
    : "";
  const sourceDetail = fact?.message || fact?.reason || "";
  const details = [];
  if (reasonLabel) details.push(`Cause: ${reasonLabel}`);
  if (sourceDetail && sourceDetail !== reasonCode) {
    details.push(`Source detail: ${sourceDetail}`);
  }
  if (name === "daily") {
    if (fact?.coverage_expected_start && fact?.coverage_expected_end) {
      details.push(
        `Expected window: ${fact.coverage_expected_start} → ${fact.coverage_expected_end}`,
      );
    }
    if (finite(fact?.missing_calendar_days) && fact.missing_calendar_days > 0) {
      details.push(`${fact.missing_calendar_days} expected day(s) missing`);
    }
    const reasonCounts = fact?.reason_code_counts;
    if (reasonCounts && typeof reasonCounts === "object") {
      const causeText = Object.entries(reasonCounts)
        .filter(([, count]) => finite(count) && count > 0)
        .map(([reason, count]) => (
          `${DAILY_QUALITY_REASON_LABELS[reason] || reason.replaceAll("_", " ")} (${count})`
        ))
        .join("; ");
      if (causeText) {
        const evidenceLabel = fact?.daily_evidence_mode === "published_daily_audit"
          ? "Published daily-audit causes"
          : "Catalog/audit reconciliation";
        details.push(`${evidenceLabel}: ${causeText}`);
      }
    }
    const affectedDates = Array.isArray(fact?.affected_dates)
      ? fact.affected_dates
      : [];
    if (affectedDates.length) {
      const visibleDates = affectedDates.slice(0, 8);
      const remaining = affectedDates.length - visibleDates.length;
      details.push(
        `Affected UTC dates: ${visibleDates.join(", ")}${remaining > 0 ? `, +${remaining} more` : ""}`,
      );
    }
  }
  if (fact?.retryable) {
    details.push(
      fact?.action === "operator_review_retry_and_manual_queues"
        ? "Operator actions: inspect both protected operator queues: retry and "
          + "manual review. This public page is read-only."
        : "Operator action: inspect the protected operator retry queue. "
          + "This public page is read-only.",
    );
  } else if (fact?.action === "operator_manual_review") {
    details.push(
      "Operator action: verify the source listing or history range in the "
        + "protected manual-review queue.",
    );
  } else if (fact?.action === "operator_review_source_outcome") {
    details.push(
      "Operator action: review the confirmed source outcome; no automatic "
        + "retry is scheduled.",
    );
  }
  const temporal = fact?.temporal_alignment;
  if (temporal && name === "execution") {
    details.push(
      `State time: ${formatUtcTimestamp(temporal.state_observed_at)}`,
      temporal.status === "not_applicable"
        ? "USD price time: not applicable — USD/USDT identity or proxy"
        : `USD price time: ${formatUtcTimestamp(temporal.usd_price_observed_at)}`,
      temporal.usd_price_state_skew_seconds === null
        || temporal.usd_price_state_skew_seconds === undefined
        ? `Price/state skew: not applicable · ${temporal.status || "unavailable"}`
        : `Price/state skew: ${formatDurationSeconds(
          temporal.usd_price_state_skew_seconds,
        )} · maximum ${formatDurationSeconds(
          temporal.max_usd_price_state_skew_seconds,
        )}`,
    );
  } else if (temporal && name === "depth" && temporal.status !== "not_applicable") {
    details.push(
      `Pool state time: ${formatUtcTimestamp(temporal.state_observed_at)}`,
      `USD price time: ${formatUtcTimestamp(temporal.usd_price_observed_at)}`,
      temporal.usd_price_state_skew_seconds === null
        || temporal.usd_price_state_skew_seconds === undefined
        ? `Price/state skew: unavailable · ${temporal.status || "unavailable"}`
        : `Price/state skew: ${formatDurationSeconds(
          temporal.usd_price_state_skew_seconds,
        )} · maximum ${formatDurationSeconds(
          temporal.max_usd_price_state_skew_seconds,
        )}`,
    );
  }
  const lineage = [
    fact?.source ? `Source: ${fact.source}` : "",
    fact?.method ? `Method: ${fact.method}` : "",
    fact?.snapshot_id ? `Snapshot: ${fact.snapshot_id}` : "",
    fact?.dataset_sha256 ? `Dataset SHA-256: ${fact.dataset_sha256}` : "",
    fact?.raw_response_sha256 ? `Raw-response SHA-256: ${fact.raw_response_sha256}` : "",
  ].filter(Boolean);
  const statusTooltip = name === "daily"
    ? [
        fact?.reason || "",
        ...(Array.isArray(fact?.affected_dates)
          ? fact.affected_dates.map((day) => `${day} UTC`)
          : []),
      ].filter(Boolean).join(" · ")
    : fact?.reason || "";
  const hasDetails = details.length > 0 || lineage.length > 0;
  const factLabel = (
    {
      daily: "daily Fact",
      tvl: "TVL Fact",
      depth: "depth Fact",
      execution: "execution Fact",
    }[name] || `${name} Fact`
  );
  return `<div class="quality-fact">
    ${qualityStateMarkup(status, "", statusTooltip)}
    ${valueText ? `<strong class="quality-primary-value">${escapeHtml(valueText)}</strong>` : ""}
    ${observedText ? `<span class="quality-observed-time">${escapeHtml(observedText)}</span>` : ""}
    ${hasDetails
      ? `<details class="quality-fact-details">
          <summary aria-label="Open ${escapeHtml(factLabel)} details"><span aria-hidden="true">i</span></summary>
          <div class="quality-fact-detail-body">
            ${details.map((detail) => `<p>${escapeHtml(detail)}</p>`).join("")}
            ${lineage.length
              ? `<strong class="quality-lineage-heading">Lineage</strong>${lineage.map((detail) => `<p>${escapeHtml(detail)}</p>`).join("")}`
              : ""}
          </div>
        </details>`
      : ""}
  </div>`;
}

function qualityProjection(item) {
  const source = item || {};
  const market = source.market || source;
  if (app.qualityOrigin === "screener") {
    return {
      status: source.screening_quality_status || "ok",
      flags: Array.isArray(source.screening_quality_flags)
        ? source.screening_quality_flags
        : [],
      scope: source.screening_quality_scope || "catalog",
      evaluationWindow: source.screening_quality_window || null,
    };
  }
  return {
    status: source.quality_status || market.quality_status || "ok",
    flags: Array.isArray(source.quality_flags)
      ? source.quality_flags
      : Array.isArray(market.quality_flags)
        ? market.quality_flags
        : [],
    scope: "selected",
    evaluationWindow: null,
  };
}

function renderQualityPayload(payload) {
  app.quality = payload;
  const sourceRows = Array.isArray(payload?.markets) ? payload.markets : [];
  const severity = app.qualityOrigin === "screener" ? app.qualitySeverity : "";
  const rows = severity
    ? sourceRows.filter((item) => {
      const projection = qualityProjection(item);
      return projection.status === severity
        || projection.flags.some((flag) => flag?.severity === severity);
    })
    : sourceRows;
  const matchingReasonCount = severity
    ? rows.reduce((total, item) => {
      const projection = qualityProjection(item);
      const flags = projection.flags;
      const matches = flags.filter((flag) => flag?.severity === severity).length;
      return total + (matches || (projection.status === severity ? 1 : 0));
    }, 0)
    : 0;
  const filterSummary = byId("quality-filter-summary");
  if (filterSummary) {
    filterSummary.hidden = !severity;
    filterSummary.textContent = severity
      ? `Showing ${rows.length} market${rows.length === 1 ? "" : "s"} with ${matchingReasonCount} ${severity} reason${matchingReasonCount === 1 ? "" : "s"} linked from the Screener.`
      : "";
    filterSummary.dataset.state = severity || "info";
  }
  byId("quality-body").innerHTML = rows.length
    ? rows.map((item) => {
        const market = item.market || item;
        const facts = item.facts || {};
        const projection = qualityProjection(item);
        const flags = Array.isArray(projection.flags)
          ? projection.flags.map((flag) => ({
              code: flag.code,
              severity: flag.severity || "warning",
              category: flag.category || "data_health",
              explanation: flag.message || flag.explanation || "",
              observedValue: flag.observed_value ?? flag.observedValue ?? null,
              threshold: flag.threshold ?? null,
            }))
          : factsMarketWarningFlags(market);
        const reasonGroups = flags.reduce((groups, flag) => {
          const category = flag.category || "data_health";
          if (!groups[category]) groups[category] = [];
          groups[category].push(flag);
          return groups;
        }, {});
        const screeningWindow = projection.evaluationWindow;
        const screeningScope = app.qualityOrigin === "screener"
          ? `<p class="quality-evaluation-scope"><strong>Screener catalog window</strong> ${escapeHtml(screeningWindow?.start || "unavailable")} → ${escapeHtml(screeningWindow?.end || "unavailable")}. The Daily Facts column remains the selected date window; this Screener reason uses the catalog evaluation window.</p>`
          : "";
        const reasons = flags.length
          ? `<details class="quality-reasons" ${severity ? "open" : ""}>
              <summary>${flags.length} current reason${flags.length === 1 ? "" : "s"}</summary>
              ${screeningScope}
              ${Object.entries(reasonGroups).map(([category, categoryFlags]) => `
                <strong class="quality-reason-category">${escapeHtml(category.replaceAll("_", " "))}</strong>
                <ul>${categoryFlags.map((flag) => `<li data-severity="${escapeHtml(flag.severity)}">
                  <strong>${escapeHtml(qualityFlagLabel(flag))}</strong>
                  ${escapeHtml(flag.explanation || "No additional explanation supplied.")}
                  ${qualityFlagMeasurement(flag) ? `<small>${escapeHtml(qualityFlagMeasurement(flag))}</small>` : ""}
                </li>`).join("")}</ul>
              `).join("")}
            </details>`
          : '<span class="missing">No current quality flags</span>';
        return `<tr ${severity ? `class="quality-linked-row" data-highlight-severity="${escapeHtml(severity)}"` : ""}>
          <td data-label="Market"><span class="market-identity"><strong>${escapeHtml(factsMarketLabel(market))}</strong><small>${escapeHtml(market.market_id)}</small></span></td>
          <td data-label="Daily Facts">${qualityFactMarkup("daily", facts.daily)}</td>
          <td data-label="TVL">${qualityFactMarkup("tvl", facts.tvl)}</td>
          <td data-label="Depth">${qualityFactMarkup("depth", facts.depth)}</td>
          <td data-label="Execution">${qualityFactMarkup("execution", facts.execution)}</td>
          <td data-label="Current Reasons">${reasons}</td>
        </tr>`;
      }).join("")
    : `<tr><td colspan="6" class="missing">${severity
      ? `No ${escapeHtml(severity)} market alerts match this Screener link.`
      : "No markets are available for this quality scope."}</td></tr>`;
}

function renderQualityFromCatalog() {
  if (!app.catalog || !app.payload) return;
  renderQualityPayload(catalogQualityPayload());
}

function decimalNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatExecutionCost(row) {
  const bps = decimalNumber(row?.quoted_execution_cost_bps);
  const usd = decimalNumber(row?.quoted_execution_cost_usd);
  if (bps === null && usd === null) return "N/A";
  const components = [];
  if (bps !== null) components.push(`${bpsFormat.format(bps)} bps`);
  if (usd !== null) components.push(formatRawUsd(usd));
  return components.join(" · ");
}

function formatExecutionFill(row) {
  const fill = decimalNumber(row?.fill_ratio);
  return fill === null ? "N/A" : formatShare(fill);
}

function executionScenarioNaReason(row, result, factLabel) {
  const status = row?.status || result?.status || "unavailable";
  const reasonCode = row?.status_reason || result?.reason_code || ({
    not_cataloged_in_snapshot: "execution_market_not_cataloged_in_snapshot",
    unavailable: "execution_snapshot_unavailable",
  }[status] || "");
  const exactReasons = {
    unsupported_protocol_or_chain: "Execution is not supported for this protocol or chain.",
    unsupported_protocol: "Execution is not supported for this protocol.",
    unsupported_chain: "Execution is not supported for this chain.",
    unsupported_method: "The requested execution method is unsupported.",
    unsupported_source: "Execution is not supported by this source adapter.",
    source_no_order_book: "The source returned no order book for this market.",
    source_no_two_sided_book: "The source did not publish a two-sided order book.",
    full_book_insufficient_liquidity: "The full published order book cannot fill this requested size.",
    execution_market_not_cataloged_in_snapshot: "This market was not included in the published execution snapshot.",
    execution_snapshot_unavailable: "The execution snapshot is unavailable.",
    execution_snapshot_invalid: "The execution snapshot failed validation.",
    instrument_absent_from_current_catalog: "The instrument is absent from the official current exchange catalog.",
  };
  const reason = exactReasons[reasonCode]
    || DAILY_QUALITY_REASON_LABELS[reasonCode]
    || (reasonCode ? reasonCode.replaceAll("_", " ") : "No canonical execution reason was published.");
  return `${reason} ${factLabel} remains N/A, not zero. Status: ${status}.`;
}

function executionDisclosureContext(row, result, factLabel, notionalUsd = null) {
  const market = result?.market;
  return {
    token: market?.token_symbol || "",
    marketId: market?.market_id || "",
    marketLabel: market
      ? [market.market_type?.toUpperCase(), market.venue, market.instrument]
        .filter(Boolean).join(" · ")
      : "",
    fact: "execution",
    factLabel,
    notionalUsd: row?.requested_notional_usd ?? notionalUsd,
  };
}

function executionCostMarkup(row, result = null, notionalUsd = null) {
  const value = formatExecutionCost(row);
  return value === "N/A"
    ? naFactMarkup(
      executionScenarioNaReason(row, result, "Execution cost"),
      executionDisclosureContext(row, result, "execution cost", notionalUsd),
    )
    : escapeHtml(value);
}

function executionFillMarkup(row, result = null, notionalUsd = null) {
  const value = formatExecutionFill(row);
  return value === "N/A"
    ? naFactMarkup(
      executionScenarioNaReason(row, result, "Fill ratio"),
      executionDisclosureContext(row, result, "fill ratio", notionalUsd),
    )
    : escapeHtml(value);
}

function executionScenario(result, direction, notional) {
  if (!result || result.status !== "available") return null;
  return result.rows.find((row) => (
    row.direction === direction
    && Number(row.requested_notional_usd) === Number(notional)
  )) || null;
}

function executionStatusMarkup(row, result) {
  const status = row?.status || (
    result?.status === "available" ? "unavailable" : result?.status
  ) || "unavailable";
  const label = status === "not_cataloged_in_snapshot" ? "not cataloged" : status;
  const reason = row?.status_reason || row?.error || "";
  return `${qualityStateMarkup(status, label)}${reason
    ? `<span class="metric-note">${escapeHtml(reason)}</span>`
    : ""}`;
}

function executionMarketName(result, slot) {
  const market = result?.market;
  return market ? `${slot} · ${market.venue} · ${market.instrument}` : `Market ${slot}`;
}

function formatDurationSeconds(value) {
  if (!finite(value)) return "unavailable";
  const seconds = Math.round(value);
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const minutes = Math.round(seconds / 60);
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function executionFeeScope(rows) {
  const statuses = [...new Set(rows.map((row) => row?.fee_status).filter(Boolean))];
  if (!statuses.length) return null;
  const labels = {
    excluded_unknown_account_tier: "CEX account fee excluded",
    included_protocol_fee: "DEX pool swap fee included",
    not_applicable: "No separate fee",
  };
  return statuses.map((status) => labels[status] || status.replaceAll("_", " ")).join(" · ");
}

function renderExecutionTiming(slot, result) {
  const normalized = slot.toLowerCase();
  const timing = result?.timing;
  const card = byId(`execution-${normalized}-timing-card`);
  const status = timing?.status || "unavailable";
  const labels = {
    current: "Current",
    warning: "Usable · timing warning",
    stale: "Withheld · stale",
    unavailable: "Withheld · unavailable",
    not_applicable: "Not applicable · identity/proxy",
    not_evaluated: "Not evaluated",
  };
  card.dataset.state = status;
  byId(`execution-${normalized}-timing-status`).textContent = (
    result?.publication_status === "withheld"
      ? labels[status] || "Withheld"
      : labels[status] || status.replaceAll("_", " ")
  );
  byId(`execution-${normalized}-state-time`).textContent = (
    `State time ${formatUtcTimestamp(timing?.state_observed_at)}`
  );
  byId(`execution-${normalized}-price-time`).textContent = (
    status === "not_applicable"
      ? "USD price time not applicable — USD/USDT identity or proxy"
      : `USD price time ${formatUtcTimestamp(timing?.usd_price_observed_at)}`
  );
  const skew = formatDurationSeconds(timing?.usd_price_state_skew_seconds);
  const maximum = formatDurationSeconds(
    timing?.max_usd_price_state_skew_seconds,
  );
  byId(`execution-${normalized}-price-skew`).textContent = (
    status === "not_applicable" || status === "not_evaluated"
      ? `Price/state skew not applicable${timing?.reason ? ` · ${timing.reason}` : ""}`
      : `Price/state skew ${skew} · max ${maximum}${
        result?.publication_status === "withheld"
          ? " · costs withheld; N/A is not zero"
          : ""
      }`
  );
}

function setExecutionLoading(message) {
  hideError(byId("execution-error"));
  showStatus(byId("execution-status"), message);
  ["execution-a-cost", "execution-b-cost", "execution-skew", "execution-fee-scope"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  ["a", "b"].forEach((slot) => renderExecutionTiming(slot, null));
  byId("execution-a-fill").textContent = "—";
  byId("execution-b-fill").textContent = "—";
  byId("execution-table-body").innerHTML = (
    '<tr><td data-label="Status" colspan="7" class="missing">Loading source-backed execution scenarios…</td></tr>'
  );
}

function invalidateExecutionRequest() {
  if (app.executionController) app.executionController.abort();
  app.executionController = null;
  app.executionRequestKey = "";
  app.executionRequestId += 1;
  return app.executionRequestId;
}

function clearExecutionResult(message = "") {
  app.execution = null;
  byId("execution-a-label").textContent = "Market A cost";
  byId("execution-b-label").textContent = "Market B cost";
  byId("execution-a-cost-heading").textContent = "A Cost";
  byId("execution-b-cost-heading").textContent = "B Cost";
  ["execution-a-cost", "execution-b-cost", "execution-skew", "execution-fee-scope"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  ["a", "b"].forEach((slot) => renderExecutionTiming(slot, null));
  byId("execution-a-fill").textContent = "—";
  byId("execution-b-fill").textContent = "—";
  byId("execution-table-body").innerHTML = (
    '<tr><td data-label="Status" colspan="7" class="missing">No current execution result.</td></tr>'
  );
  if (message) {
    showError(byId("execution-error"), message);
    showStatus(
      byId("execution-status"),
      "Execution facts are unavailable; no missing result was converted to zero.",
      "stale",
    );
  } else {
    hideError(byId("execution-error"));
    hideStatus(byId("execution-status"));
  }
}

function renderExecution(payload) {
  app.execution = payload;
  const single = payload?.selection_mode === "single";
  const notionals = payload.metadata?.notionals_usd || [1000, 5000, 10000, 50000, 100000];
  const resultA = payload.market_a;
  const resultB = single ? null : payload.market_b;
  renderExecutionTiming("a", resultA);
  if (!single) renderExecutionTiming("b", resultB);
  const rowsA = notionals.map((notional) => executionScenario(
    resultA,
    app.executionDirection,
    notional,
  ));
  const rowsB = single ? [] : notionals.map((notional) => executionScenario(
    resultB,
    app.executionDirection,
    notional,
  ));
  byId("execution-a-cost-heading").textContent = `${executionMarketName(resultA, "A")} Cost`;
  if (!single) byId("execution-b-cost-heading").textContent = `${executionMarketName(resultB, "B")} Cost`;
  byId("execution-table-caption").textContent = single
    ? "Execution cost and fill ratio for Market A at each collected notional in the selected direction."
    : "Execution cost and fill ratio for each collected notional in the selected direction.";
  byId("execution-table-body").innerHTML = notionals.map((notional, index) => {
    const rowA = rowsA[index];
    const rowB = rowsB[index];
    return `<tr>
      <th scope="row" data-label="Requested Notional">${formatCurrency(Number(notional))}</th>
      <td data-label="A Cost">${executionCostMarkup(rowA, resultA, notional)}</td>
      <td data-label="A Fill">${executionFillMarkup(rowA, resultA, notional)}</td>
      <td data-label="A Status">${executionStatusMarkup(rowA, resultA)}</td>
      ${single ? "" : `<td data-label="B Cost">${executionCostMarkup(rowB, resultB, notional)}</td>
      <td data-label="B Fill">${executionFillMarkup(rowB, resultB, notional)}</td>
      <td data-label="B Status">${executionStatusMarkup(rowB, resultB)}</td>`}
    </tr>`;
  }).join("");

  const selectedA = executionScenario(
    resultA,
    app.executionDirection,
    app.executionNotionalUsd,
  );
  const selectedB = single ? null : executionScenario(
    resultB,
    app.executionDirection,
    app.executionNotionalUsd,
  );
  byId("execution-a-label").textContent = `${executionMarketName(resultA, "A")} cost`;
  if (!single) byId("execution-b-label").textContent = `${executionMarketName(resultB, "B")} cost`;
  byId("execution-a-cost").innerHTML = executionCostMarkup(
    selectedA,
    resultA,
    app.executionNotionalUsd,
  );
  byId("execution-b-cost").innerHTML = single ? "" : executionCostMarkup(
    selectedB, resultB, app.executionNotionalUsd,
  );
  byId("execution-a-fill").innerHTML = executionFillMarkup(
    selectedA,
    resultA,
    app.executionNotionalUsd,
  );
  byId("execution-b-fill").innerHTML = single ? "" : executionFillMarkup(
    selectedB, resultB, app.executionNotionalUsd,
  );
  const snapshotSkew = payload.metadata?.snapshot_skew_seconds;
  if (!single) setFactValue(
    "execution-skew",
    finite(snapshotSkew),
    formatDurationSeconds(snapshotSkew),
    "Snapshot skew requires valid state timestamps for both selected execution markets.",
    { token: payload.token_symbol, factLabel: "execution snapshot skew" },
  );
  const feeScope = executionFeeScope(single ? [selectedA] : [selectedA, selectedB]);
  setFactValue(
    "execution-fee-scope",
    Boolean(feeScope),
    feeScope || "",
    "Fee scope is unavailable because neither selected scenario published a fee-status field.",
    {
      token: payload.token_symbol,
      factLabel: "execution fee scope",
      notionalUsd: app.executionNotionalUsd,
    },
  );

  const scenarioRows = [...rowsA, ...rowsB].filter(Boolean);
  const statuses = scenarioRows.map((row) => row.status);
  const unavailableResults = (single ? [resultA] : [resultA, resultB]).filter((result) => (
    !result || result.status !== "available"
  ));
  const failed = statuses.filter((status) => status === "failed").length;
  const partial = statuses.filter((status) => status === "partial").length;
  const unsupported = statuses.filter((status) => status === "unsupported").length;
  const observed = statuses.filter((status) => status === "observed").length;
  const withheldResults = (single ? [["A", resultA]] : [["A", resultA], ["B", resultB]])
    .filter(([, result]) => result?.publication_status === "withheld");
  const state = failed
    ? "critical"
    : partial || unsupported || unavailableResults.length
      ? "warning"
      : observed
        ? "success"
        : "warning";
  const direction = app.executionDirection === "buy_token" ? "buy Token" : "sell Token";
  showStatus(
    byId("execution-status"),
    `${payload.token_symbol} · ${direction} · ${observed} observed, ${partial} partial, `
      + `${unsupported} unsupported scenarios shown. `
      + (withheldResults.length
        ? `${withheldResults.map(([slot]) => `Market ${slot}`).join(" and ")} costs withheld because USD price timing failed the 2h gate. `
        : "")
      + "Null cost means the full request was not measured; it is not zero.",
    withheldResults.length ? "critical" : state,
  );
  hideError(byId("execution-error"));
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

function executionPayloadMatchesSelection(payload, token, selection) {
  if (
    payload?.token_symbol !== token
    || payload?.market_a?.market?.market_id !== selection.marketA
  ) return false;
  if (selection.selection === "single") {
    return payload.selection_mode === "single" && payload.market_b === null;
  }
  return (
    payload.selection_mode == null
    && payload?.market_b?.market?.market_id === selection.marketB
  );
}

function executionRequestIsCurrent(requestId, requestKey, selection) {
  return (
    requestId === app.executionRequestId
    && requestKey === app.executionRequestKey
    && app.route?.page === "liquidity"
    && requestKey === workspaceRequestKey("liquidity", selection)
  );
}

async function loadExecutionCost() {
  const requestId = invalidateExecutionRequest();
  const token = selectedWorkspaceToken();
  const selection = selectedMarketSelection();
  const { marketA, marketB } = selection;
  const single = selection.selection === "single";
  if (!app.catalog || !token || !marketA || (single ? Boolean(marketB) : !marketB || marketA === marketB)) {
    clearExecutionResult(single
      ? "Choose Market A and leave Market B empty to inspect execution cost."
      : "Choose two distinct markets for this Token to inspect execution cost.");
    return false;
  }
  const requestKey = workspaceRequestKey("liquidity", selection);
  app.executionRequestKey = requestKey;
  const controller = new AbortController();
  app.executionController = controller;
  const query = new URLSearchParams({
    token,
    market_a: marketA,
  });
  if (single) query.set("selection", "single");
  else query.set("market_b", marketB);
  setExecutionLoading(`Loading ${token} fixed-notional execution facts…`);
  try {
    const response = await fetch(`/api/markets/execution-cost?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Execution facts failed to load.");
    if (!executionRequestIsCurrent(requestId, requestKey, selection)) return false;
    if (!executionPayloadMatchesSelection(payload, token, selection)) {
      throw new Error("The execution response failed its Token, market, or selection contract.");
    }
    renderExecution(payload);
    return true;
  } catch (error) {
    if (error.name === "AbortError" || !executionRequestIsCurrent(requestId, requestKey, selection)) return false;
    clearExecutionResult(publicErrorMessage(error, "Execution facts failed to load."));
    return false;
  } finally {
    if (executionRequestIsCurrent(requestId, requestKey, selection)) app.executionController = null;
  }
}

function invalidateQualityRequest() {
  if (app.qualityController) app.qualityController.abort();
  app.qualityController = null;
  app.qualityRequestKey = "";
  app.qualityRequestId += 1;
  return app.qualityRequestId;
}

function qualityStatusCounts(payload) {
  const counts = {};
  (payload?.markets || []).forEach((market) => {
    Object.values(market.facts || {}).forEach((fact) => {
      const status = fact?.status || "unavailable";
      counts[status] = (counts[status] || 0) + 1;
    });
  });
  return counts;
}

function qualityStatusTiers(counts) {
  return {
    critical: (
      (counts.failed || 0)
      + (counts.collection_failed || 0)
      + (counts.invalid || 0)
    ),
    pending: (
      (counts.backfill_pending || 0)
      + (counts.missing_unexplained || 0)
      + (counts.stale || 0)
      + (counts.needs_review || 0)
    ),
    informational: (
      (counts.source_no_observation || 0)
      + (counts.unsupported || 0)
      + (counts.not_applicable || 0)
    ),
  };
}

function effectiveQualityScope(selection = selectedMarketSelection()) {
  return selection.selection === "single" ? "selected" : app.qualityScope;
}

function qualityPayloadMatchesSelection(payload, token, selection, scope) {
  if (payload?.token_symbol !== token || payload?.metadata?.scope !== scope) return false;
  const expectedIds = scope === "selected"
    ? selection.selection === "single"
      ? [selection.marketA]
      : [selection.marketA, selection.marketB]
    : null;
  if (!expectedIds) return true;
  const actualIds = payload?.metadata?.selected_market_ids;
  if (
    !Array.isArray(actualIds)
    || actualIds.length !== expectedIds.length
    || actualIds.some((id, index) => id !== expectedIds[index])
  ) return false;
  const rows = Array.isArray(payload?.markets) ? payload.markets : [];
  return rows.length === expectedIds.length
    && rows.every((row, index) => (row?.market_id || row?.market?.market_id) === expectedIds[index]);
}

function qualityRequestIsCurrent(requestId, requestKey, selection, scope) {
  return (
    requestId === app.qualityRequestId
    && requestKey === app.qualityRequestKey
    && app.route?.page === "quality"
    && requestKey === workspaceRequestKey("quality", selection, { scope })
  );
}

async function loadQuality() {
  const window = appliedTimeWindow();
  const requestId = invalidateQualityRequest();
  const token = selectedWorkspaceToken();
  const selection = selectedMarketSelection();
  const { marketA, marketB } = selection;
  const scope = effectiveQualityScope(selection);
  if (selection.selection === "single") app.qualityScope = "selected";
  if (!app.catalog || !token) {
    showError(byId("quality-error"), "Market catalog is unavailable.");
    return false;
  }
  if (scope === "selected" && (
    !marketA
    || (selection.selection === "single" ? Boolean(marketB) : !marketB || marketA === marketB)
  )) {
    renderQualityFromCatalog();
    showStatus(
      byId("quality-status"),
      selection.selection === "single"
        ? "Selected scope needs exact Market A. The A-only catalog fallback remains visible."
        : "Selected scope needs two distinct markets. The catalog-level fallback remains visible.",
      "stale",
    );
    showError(byId("quality-error"), selection.selection === "single"
      ? "Choose exact Market A and leave Market B empty."
      : "Choose distinct Market A and Market B.");
    return false;
  }
  const requestKey = workspaceRequestKey("quality", selection, { scope });
  app.qualityRequestKey = requestKey;
  const controller = new AbortController();
  app.qualityController = controller;
  const query = new URLSearchParams({ token, scope });
  if (window.start) query.set("start", window.start);
  if (window.end) query.set("end", window.end);
  if (scope === "selected") {
    query.set("market_a", marketA);
    if (selection.selection === "single") query.set("selection", "single");
    else query.set("market_b", marketB);
  }
  hideError(byId("quality-error"));
  showStatus(byId("quality-status"), `Loading ${token} fact lineage and quality states…`);
  try {
    const response = await fetch(`/api/markets/quality?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Quality facts failed to load.");
    if (!qualityRequestIsCurrent(requestId, requestKey, selection, scope)) return false;
    if (!qualityPayloadMatchesSelection(payload, token, selection, scope)) {
      throw new Error("The quality response failed its Token, market, or selection contract.");
    }
    renderQualityPayload(payload);
    const counts = qualityStatusCounts(payload);
    const { critical, pending, informational } = qualityStatusTiers(counts);
    const state = critical ? "critical" : pending ? "warning" : "success";
    const dailyAudit = payload.metadata.daily_quality_report || {};
    const dailyAuditText = dailyAudit.status === "matched"
      ? `${dailyAudit.selected_window_issue_count || 0} published daily-audit issue(s)`
      : `daily audit ${dailyAudit.status || "unavailable"}; catalog-window inference shown`;
    showStatus(
      byId("quality-status"),
      `${payload.token_symbol} · ${payload.metadata.scope} scope · `
        + `${payload.metadata.window_start || "—"} → ${payload.metadata.window_end || "—"} · `
        + `${payload.markets.length} market${payload.markets.length === 1 ? "" : "s"} · ${counts.observed || 0} observed · `
        + `${pending} attention/limits · ${critical} failed/invalid · `
        + `${informational} informational/structural · ${counts.partial || 0} partial · `
        + `${dailyAuditText}.`,
      state,
    );
    if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
    hideError(byId("quality-error"));
    return true;
  } catch (error) {
    if (error.name === "AbortError" || !qualityRequestIsCurrent(requestId, requestKey, selection, scope)) return false;
    renderQualityFromCatalog();
    showStatus(
      byId("quality-status"),
      "Catalog-level quality remains visible; detailed lineage could not be loaded.",
      "stale",
    );
    showError(
      byId("quality-error"),
      publicErrorMessage(error, "Quality facts failed to load."),
    );
    return false;
  } finally {
    if (qualityRequestIsCurrent(requestId, requestKey, selection, scope)) app.qualityController = null;
  }
}

function invalidateEventRequest() {
  if (app.eventController) app.eventController.abort();
  app.eventController = null;
  app.eventRequestKey = "";
  app.eventRequestId += 1;
  return app.eventRequestId;
}

function eventRequestIsCurrent(requestId, requestKey, selection) {
  return (
    requestId === app.eventRequestId
    && requestKey === app.eventRequestKey
    && app.route?.page === "events"
    && requestKey === workspaceRequestKey("events", selection, {
      lifecycle: app.eventLifecycle,
      clockState: app.eventClockState,
    })
  );
}

function eventAvailabilityStatus(payload) {
  if (typeof payload?.availability === "string") return payload.availability;
  return payload?.availability?.status || "unavailable";
}

function eventLabel(value) {
  return String(value || "unavailable")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function eventEffectiveTime(event) {
  const time = event?.time || {};
  const value = time.effective_at || time.effective_date_start || "Unavailable";
  return `${value} (${time.effective_at_precision || "unknown"} precision)`;
}

function eventClockLabel(event) {
  const state = event?.clock?.state;
  if (state === "current_window") return "Current";
  return eventLabel(state || "unavailable");
}

function eventClockNotice(event) {
  if (event?.clock?.state === "past" && event?.lifecycle === "scheduled") {
    return "Effective time passed; occurrence unconfirmed";
  }
  if (event?.clock?.basis === "effective_date_interval") {
    return "Clock state uses the full published precision interval.";
  }
  return "Clock state uses the exact published instant.";
}

function eventSizeOrMarket(event) {
  const pieces = [];
  const size = event?.size || {};
  const relation = size.relation ? `${eventLabel(size.relation)} ` : "";
  if (size.amount_token) {
    pieces.push(
      `${relation}${size.amount_token} ${event?.token_symbol || "tokens"}`,
    );
  }
  if (size.percent_of_supply) {
    pieces.push(`${relation}${size.percent_of_supply}% of supply`);
  }
  if (size.amount_usd) {
    pieces.push(`${relation}$${size.amount_usd} source-reported`);
  }
  const market = event?.market || {};
  if (market.market_id) pieces.push(market.market_id);
  else if (market.venue || market.market_symbol) {
    pieces.push([market.venue, market.market_symbol].filter(Boolean).join(" · "));
  }
  return pieces.length ? pieces.join(" · ") : "Not reported or not applicable";
}

function eventSourceHostname(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return "Official source";
  }
}

function renderEventFacts(payload) {
  app.eventFacts = payload;
  const eventToken = payload?.query?.token || selectedWorkspaceToken();
  const availability = eventAvailabilityStatus(payload);
  const events = Array.isArray(payload?.events) ? payload.events : [];
  const lifecycleCounts = payload?.lifecycle_counts || {};
  const sources = new Set(events.map((event) => event?.source?.url).filter(Boolean));
  const unavailableReason = payload?.availability?.reason
    || "The Event Fact dataset is not published.";
  setFactValue(
    "events-count",
    availability === "available",
    String(payload?.event_count ?? events.length),
    unavailableReason,
    { token: eventToken, factLabel: "verified event count" },
  );
  setFactValue(
    "events-occurred",
    availability === "available",
    String(lifecycleCounts.occurred || 0),
    unavailableReason,
    { token: eventToken, factLabel: "occurred event count" },
  );
  setFactValue(
    "events-scheduled",
    availability === "available",
    String(lifecycleCounts.scheduled || 0),
    unavailableReason,
    { token: eventToken, factLabel: "scheduled event count" },
  );
  setFactValue(
    "events-source-count",
    availability === "available",
    String(sources.size),
    unavailableReason,
    { token: eventToken, factLabel: "official event source count" },
  );
  const configuredTokenCount = Number(payload?.coverage?.configured_token_count);
  const coveredTokenCount = Number(payload?.coverage?.covered_token_count);
  const tokenCoverageAvailable = (
    availability === "available"
    && Number.isFinite(configuredTokenCount)
    && Number.isFinite(coveredTokenCount)
  );
  setFactValue(
    "events-token-coverage",
    tokenCoverageAvailable,
    `${coveredTokenCount} / ${configuredTokenCount}`,
    tokenCoverageAvailable
      ? ""
      : `${unavailableReason} Token coverage therefore cannot be calculated.`,
    { token: eventToken, factLabel: "event Token coverage" },
  );

  if (availability !== "available") {
    byId("events-body").innerHTML = (
      '<tr><td colspan="8" class="missing">'
      + "The Event Fact dataset is unavailable. This is different from a verified zero-event result."
      + "</td></tr>"
    );
    showStatus(
      byId("events-status"),
      payload?.availability?.reason
        || "The Event Fact dataset is not published; market facts remain available.",
      "warning",
    );
    hideError(byId("events-error"));
    if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
    return;
  }

  byId("events-body").innerHTML = events.length
    ? events.map((event) => {
        const source = event.source || {};
        const revision = event.revision_lineage || {};
        const sourceLink = source.url
          ? `<a class="event-source-link" href="${escapeHtml(source.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(eventSourceHostname(source.url))}</a>`
          : "Source unavailable";
        return `<tr>
          <td data-label="Effective time">
            <strong>${escapeHtml(eventEffectiveTime(event))}</strong>
            <span class="metric-note">Announced ${escapeHtml(event.time?.announced_at || "not reported")}</span>
          </td>
          <td data-label="Type">${escapeHtml(eventLabel(event.event_type))}
            <span class="metric-note">${escapeHtml(eventLabel(event.event_subtype))}</span>
          </td>
          <td data-label="Event Fact">
            <strong>${escapeHtml(event.event_name || "Unnamed event")}</strong>
            <span class="metric-note">${escapeHtml(event.notes || "No additional source-backed note.")}</span>
          </td>
          <td data-label="Lifecycle"><span class="event-state" data-state="${escapeHtml(event.lifecycle || "unavailable")}">${escapeHtml(eventLabel(event.lifecycle))}</span></td>
          <td data-label="Time state">
            <span class="event-clock-state" data-state="${escapeHtml(event.clock?.state || "unavailable")}">${escapeHtml(eventClockLabel(event))}</span>
            <span class="metric-note">${escapeHtml(eventClockNotice(event))}</span>
          </td>
          <td data-label="Size / Market">${escapeHtml(eventSizeOrMarket(event))}</td>
          <td data-label="Evidence">${escapeHtml(eventLabel(event.evidence_status))}
            <span class="metric-note">${escapeHtml(eventLabel(source.kind))} · checked ${escapeHtml(source.checked_at_utc || "time unavailable")}</span>
          </td>
          <td data-label="Source & Revision">${sourceLink}
            <span class="metric-note">Revision ${escapeHtml(event.revision)} · ${escapeHtml(revision.reason || "reason unavailable")}</span>
          </td>
        </tr>`;
      }).join("")
    : `<tr><td colspan="8" class="missing">No verified Event Facts for ${escapeHtml(
        payload?.query?.token || selectedWorkspaceToken() || "this Token",
      )} match this release and filter. This is not proof that no event exists.</td></tr>`;

  const lifecycle = payload?.query?.lifecycle || "all lifecycles";
  const clockState = payload?.query?.clock_state || "all times";
  showStatus(
    byId("events-status"),
    events.length
      ? `${payload.query?.token || "Token"} · ${events.length} latest verified Event Facts · ${eventLabel(clockState)} · ${eventLabel(lifecycle)} · bundle ${payload.bundle_id || "unavailable"}.`
      : `No verified Event Facts match ${payload.query?.token || "this Token"}, ${eventLabel(clockState)}, and ${eventLabel(lifecycle)} in this release; absence is not inferred.`,
    events.length ? "success" : "warning",
  );
  hideError(byId("events-error"));
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

async function fetchEventFacts({
  token,
  start = "",
  end = "",
  lifecycle = "",
  clockState = "",
  signal,
}) {
  const query = new URLSearchParams({ token });
  if (start) query.set("start", start);
  if (end) query.set("end", end);
  if (lifecycle && lifecycle !== "all") query.set("lifecycle", lifecycle);
  if (clockState && clockState !== "all") query.set("clock_state", clockState);
  const response = await fetch(`/api/markets/events?${query.toString()}`, { signal });
  const payload = await responseJson(response);
  if (!response.ok) {
    throw new Error(payload.error || "Event Facts failed to load.");
  }
  if (payload?.schema !== "event_facts_api/v2") {
    throw new Error("The Event Fact response has an unsupported schema.");
  }
  if (
    !payload?.query
    || String(payload.query.token || "").toUpperCase()
      !== String(token).toUpperCase()
  ) {
    throw new Error("The Event Fact response failed its Token-scope contract.");
  }
  const expectedLifecycle = lifecycle && lifecycle !== "all" ? lifecycle : null;
  const expectedClock = clockState && clockState !== "all" ? clockState : null;
  if ((payload?.query?.lifecycle || null) !== expectedLifecycle) {
    throw new Error("The Event Fact response failed its lifecycle scope contract.");
  }
  if ((payload?.query?.clock_state || null) !== expectedClock) {
    throw new Error("The Event Fact response failed its clock scope contract.");
  }
  const events = Array.isArray(payload?.events) ? payload.events : null;
  if (!events || typeof payload?.clock_as_of_utc !== "string") {
    throw new Error("The Event Fact response failed its row scope contract.");
  }
  const rowScopeMismatch = events.some((event) => (
    String(event?.token_symbol || "").toUpperCase()
      !== String(token).toUpperCase()
    || (expectedLifecycle && event?.lifecycle !== expectedLifecycle)
    || (expectedClock && event?.clock?.state !== expectedClock)
    || event?.clock?.as_of_utc !== payload.clock_as_of_utc
  ));
  if (rowScopeMismatch) {
    throw new Error("The Event Fact response failed its row scope contract.");
  }
  return payload;
}

async function loadEvents() {
  const requestId = invalidateEventRequest();
  const token = selectedWorkspaceToken();
  const selection = selectedMarketSelection();
  if (!app.catalog || !token) {
    showError(byId("events-error"), "Token catalog is unavailable.");
    return false;
  }
  const controller = new AbortController();
  app.eventController = controller;
  const requestKey = workspaceRequestKey("events", selection, {
    lifecycle: app.eventLifecycle,
    clockState: app.eventClockState,
  });
  app.eventRequestKey = requestKey;
  hideError(byId("events-error"));
  showStatus(byId("events-status"), `Loading ${token} verified Event Facts…`);
  try {
    const payload = await fetchEventFacts({
      token,
      lifecycle: app.eventLifecycle,
      clockState: app.eventClockState,
      signal: controller.signal,
    });
    if (!eventRequestIsCurrent(requestId, requestKey, selection)) return false;
    renderEventFacts(payload);
    return true;
  } catch (error) {
    if (error.name === "AbortError" || !eventRequestIsCurrent(requestId, requestKey, selection)) return false;
    app.eventFacts = null;
    byId("events-body").innerHTML = (
      '<tr><td colspan="8" class="missing">Verified Event Facts could not be loaded. No zero-event claim is made.</td></tr>'
    );
    [
      ["events-count", "verified event count"],
      ["events-occurred", "occurred event count"],
      ["events-scheduled", "scheduled event count"],
      ["events-source-count", "official event source count"],
      ["events-token-coverage", "event Token coverage"],
    ]
      .forEach(([id, factLabel]) => setFactValue(
        id,
        false,
        "",
        "Verified Event Facts could not be loaded from the published bundle.",
        { token, factLabel },
      ));
    showStatus(
      byId("events-status"),
      "Event Fact publication is unavailable; market facts remain usable.",
      "critical",
    );
    showError(
      byId("events-error"),
      publicErrorMessage(error, "Event Facts failed to load."),
    );
    if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
    return false;
  } finally {
    if (eventRequestIsCurrent(requestId, requestKey, selection)) app.eventController = null;
  }
}

function dailyFreshnessText(label, item) {
  if (!item || !item.available_end) return `${label} unavailable`;
  return `${label} through ${item.available_end} · ${item.status} · lag ${item.lag_days}d`;
}

function snapshotFreshnessText(item) {
  if (!item || item.age_hours === null || item.age_hours === undefined) {
    return "freshness unavailable";
  }
  return `${item.status} · age ${item.age_hours.toFixed(1)}h`;
}

function updateMetadata() {
  const metadata = app.payload.metadata;
  const start = byId("date-start");
  const end = byId("date-end");
  start.min = metadata.available_start;
  start.max = metadata.available_end;
  end.min = metadata.available_start;
  end.max = metadata.available_end;
  syncTimeWindowControls();
  syncClosedDraftToApplied();
  byId("available-range").textContent = `Available ${metadata.available_start} to ${metadata.available_end}`;
  const freshness = metadata.freshness;
  if (freshness) {
    byId("freshness").textContent = [
      `CEX ${freshness.cex_daily.available_end || "unavailable"}`,
      `DEX ${freshness.dex_daily.available_end || "unavailable"}`,
      `common ${freshness.common_comparable_end || "unavailable"}`,
    ].join(" · ");
    byId("freshness-cluster").dataset.status = freshness.overall_status;
    byId("daily-source-status").textContent = [
      dailyFreshnessText("CEX daily", freshness.cex_daily),
      dailyFreshnessText("DEX daily", freshness.dex_daily),
      `common comparable end ${freshness.common_comparable_end || "unavailable"}`,
    ].join(" | ");
  } else {
    byId("freshness").textContent = `Data through ${metadata.available_end}`;
    byId("freshness-cluster").dataset.status = "unavailable";
    byId("daily-source-status").textContent = "Source-specific freshness unavailable";
  }
  const sourceText = metadata.sources
    .map((source) => `${source.name} · ${source.sha256}`)
    .join(" | ");
  const storage = metadata.storage || { engine: "csv" };
  const storageText = storage.engine === "sqlite"
    ? `SQLite snapshot · ${storage.snapshot_id}`
    : "CSV fallback";
  byId("source-list").textContent = `${storageText} | ${sourceText}`;
  const tvl = metadata.tvl_snapshot;
  const tvlSeries = tvl?.market_series_rows ?? tvl?.pool_rows;
  const tvlPools = tvl?.unique_pool_count ?? metadata.dex_unique_pool_count;
  byId("tvl-source-status").textContent = tvl
    ? `TVL snapshot ${formatUtcTimestamp(tvl.observed_at)} · ${tvl.status_counts.observed}/${tvlSeries} series observed`
      + `${finite(tvlPools) ? ` · ${tvlPools} physical pools` : ""}`
      + ` · ${snapshotFreshnessText(freshness?.dex_tvl)} · ${tvl.method}`
    : metadata.tvl_note;
  const depth = metadata.cex_depth_snapshot;
  byId("depth-source-status").textContent = depth
    ? `CEX depth ${formatUtcTimestamp(depth.observed_at)} · ${depth.status_counts.observed} complete · ${depth.status_counts.partial} partial · ${depth.status_counts.failed} failed · ${snapshotFreshnessText(freshness?.cex_depth)} · ${depth.method}`
    : metadata.cex_depth_note;
  const dexDepth = metadata.dex_depth_snapshot;
  const dexStatuses = dexDepth?.status_counts || {};
  byId("dex-depth-source-status").textContent = dexDepth
    ? `DEX depth ${formatUtcTimestamp(dexDepth.observed_at)} · ${dexStatuses.observed || 0} complete · ${dexStatuses.partial || 0} partial · ${dexStatuses.unsupported || 0} unsupported · ${dexStatuses.failed || 0} failed · ${snapshotFreshnessText(freshness?.dex_depth)} · ${dexDepth.method}`
    : metadata.dex_depth_note;
  byId("execution-source-status").textContent = [
    `CEX execution ${snapshotFreshnessText(freshness?.cex_execution)}`,
    `DEX execution ${snapshotFreshnessText(freshness?.dex_execution)}`,
    "Execution freshness uses its own state timestamp and is not borrowed from depth.",
  ].join(" | ");
}

function factsMarketLabel(market) {
  const type = market.market_type.toUpperCase();
  return `${type} · ${market.venue} · ${market.instrument}`;
}

function factsMarketsForToken(token) {
  return app.catalog.markets.filter((market) => market.token_symbol === token);
}

function factsOptions(markets, selectedId, { emptyLabel = "Select market" } = {}) {
  return [
    `<option value="">${escapeHtml(emptyLabel)}</option>`,
    ...markets.map((market) => (
      `<option value="${escapeHtml(market.market_id)}" ${market.market_id === selectedId ? "selected" : ""}>`
        + `${escapeHtml(factsMarketLabel(market))}</option>`
    )),
  ].join("");
}

function factsMarketWarningFlags(market) {
  if (!market) return [];
  const flags = qualityFlagObjects(market, market?.market_type);
  if (flags.length || !market?.quality_status || market.quality_status === "ok") return flags;
  return [{
    code: "catalog_quality_status",
    severity: ["info", "warning", "critical"].includes(market.quality_status)
      ? market.quality_status
      : "warning",
    explanation: (
      `The catalog reported a ${market.quality_status} quality status `
      + "but did not supply a structured reason."
    ),
    observedValue: market.quality_status,
    threshold: null,
  }];
}

function factsMarketWarningSeverity(market, flags) {
  if (market?.quality_status === "critical" || flags.some((flag) => flag.severity === "critical")) {
    return "critical";
  }
  if (market?.quality_status === "warning" || flags.some((flag) => flag.severity === "warning")) {
    return "warning";
  }
  return "info";
}

function factsMarketWarningMarkup(slotLabel, market, flags, severity) {
  const alertLabel = flags.length === 1 ? "quality alert" : "quality alerts";
  const items = flags.map((flag) => {
    const measurement = qualityFlagMeasurement(flag);
    const explanation = flag.explanation || "No additional explanation was supplied.";
    return `<li class="market-warning-item" data-severity="${escapeHtml(flag.severity)}">
      <div class="market-warning-item-heading">
        <strong>${escapeHtml(qualityFlagLabel(flag))}</strong>
        <span>${escapeHtml(flag.severity)}</span>
      </div>
      <p>${escapeHtml(explanation)}</p>
      ${measurement ? `<small>${escapeHtml(measurement)}</small>` : ""}
    </li>`;
  }).join("");
  const qualityPath = navigation
    ? navigation.buildWorkspacePath(
        selectedWorkspaceToken(),
        "quality",
        { ...selectedPairState(), scope: "selected" },
      )
    : "#";
  return `
    <div class="market-warning-tooltip-heading">
      <strong>${escapeHtml(slotLabel)} · ${escapeHtml(severity)}</strong>
      <span>${flags.length} ${alertLabel}</span>
    </div>
    <div class="market-warning-market">${escapeHtml(factsMarketLabel(market))}</div>
    <ul>${items}</ul>
    <a class="warning-quality-link" href="${escapeHtml(qualityPath)}" data-workspace-page="quality">
      Inspect this selection in Data Quality
    </a>
  `;
}

function hideFactsMarketWarning(slot, { force = false } = {}) {
  const anchor = byId(`facts-market-${slot}-warning`);
  const trigger = byId(`facts-market-${slot}-warning-trigger`);
  const tooltip = byId(`facts-market-${slot}-warning-tooltip`);
  if (!anchor || !trigger || !tooltip) return;
  if (!force && anchor.dataset.pinned === "true") return;
  anchor.dataset.pinned = "false";
  tooltip.hidden = true;
  trigger.setAttribute("aria-expanded", "false");
}

function showFactsMarketWarning(slot) {
  const trigger = byId(`facts-market-${slot}-warning-trigger`);
  const tooltip = byId(`facts-market-${slot}-warning-tooltip`);
  if (!trigger || trigger.hidden || !tooltip) return;
  const protectedOtherWarning = ["a", "b"].some((otherSlot) => {
    if (otherSlot === slot) return false;
    const otherAnchor = byId(`facts-market-${otherSlot}-warning`);
    return (
      otherAnchor?.dataset.pinned === "true"
      || otherAnchor?.contains(document.activeElement)
    );
  });
  if (protectedOtherWarning) return;
  closeFactsMarketWarnings(slot);
  tooltip.hidden = false;
  trigger.setAttribute("aria-expanded", "true");
}

function closeFactsMarketWarnings(exceptSlot = null) {
  ["a", "b"].forEach((slot) => {
    if (slot !== exceptSlot) hideFactsMarketWarning(slot, { force: true });
  });
}

function renderFactsMarketWarning(slot, market) {
  const slotLabel = `Market ${slot.toUpperCase()}`;
  const anchor = byId(`facts-market-${slot}-warning`);
  const trigger = byId(`facts-market-${slot}-warning-trigger`);
  const tooltip = byId(`facts-market-${slot}-warning-tooltip`);
  const status = byId(`facts-market-${slot}-warning-status`);
  const shell = anchor.parentElement;
  const flags = factsMarketWarningFlags(market);
  hideFactsMarketWarning(slot, { force: true });
  if (!flags.length) {
    anchor.hidden = true;
    shell.classList.remove("has-market-warning");
    trigger.hidden = true;
    anchor.removeAttribute("data-severity");
    tooltip.textContent = "";
    status.textContent = `${slotLabel} has no quality alerts.`;
    return;
  }
  const severity = factsMarketWarningSeverity(market, flags);
  const alertLabel = flags.length === 1 ? "quality alert" : "quality alerts";
  anchor.hidden = false;
  shell.classList.add("has-market-warning");
  anchor.dataset.severity = severity;
  trigger.hidden = false;
  trigger.setAttribute(
    "aria-label",
    `${slotLabel} ${severity} details: ${flags.length} ${alertLabel}`,
  );
  tooltip.innerHTML = factsMarketWarningMarkup(slotLabel, market, flags, severity);
  status.textContent = `${slotLabel} has ${flags.length} ${alertLabel}. Use the information button for details.`;
}

function renderFactsMarketWarnings() {
  const { marketA, marketB } = selectedLiquidityMarkets();
  renderFactsMarketWarning("a", marketA);
  renderFactsMarketWarning("b", marketB);
}

function bindFactsMarketWarningEvents() {
  ["a", "b"].forEach((slot) => {
    const anchor = byId(`facts-market-${slot}-warning`);
    const trigger = byId(`facts-market-${slot}-warning-trigger`);
    anchor.addEventListener("pointerenter", () => showFactsMarketWarning(slot));
    anchor.addEventListener("pointerleave", () => {
      if (!anchor.contains(document.activeElement)) hideFactsMarketWarning(slot);
    });
    anchor.addEventListener("focusin", () => showFactsMarketWarning(slot));
    anchor.addEventListener("focusout", (event) => {
      if (!anchor.contains(event.relatedTarget)) {
        hideFactsMarketWarning(slot, { force: true });
      }
    });
    trigger.addEventListener("click", (event) => {
      event.stopPropagation();
      const shouldPin = anchor.dataset.pinned !== "true";
      closeFactsMarketWarnings(slot);
      anchor.dataset.pinned = String(shouldPin);
      if (shouldPin) showFactsMarketWarning(slot);
      else hideFactsMarketWarning(slot, { force: true });
    });
  });
  document.addEventListener("pointerdown", (event) => {
    if (!event.target.closest?.(".market-warning-anchor")) closeFactsMarketWarnings();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    const activeAnchor = document.activeElement?.closest?.(".market-warning-anchor");
    const returnTrigger = activeAnchor?.querySelector?.(".market-warning-trigger");
    closeFactsMarketWarnings();
    if (returnTrigger && !returnTrigger.hidden) {
      event.preventDefault();
      returnTrigger.focus();
    }
  });
}

function catalogMarketMatchesPrimary(market, tokenSummary) {
  if (!tokenSummary) return false;
  if (market.market_type === "cex") {
    return `${market.venue}|${market.instrument}` === tokenSummary.primary_cex_id;
  }
  return market.pool_address === tokenSummary.primary_dex_id;
}

function catalogMarketScore(market, tokenSummary) {
  let score = catalogMarketMatchesPrimary(market, tokenSummary) ? 1_000_000 : 0;
  if (market.selection_rank === 1 || market.is_primary) score += 1_000_000;
  const status = market.depth_status;
  if (status === "observed" || status === "complete") score += 100_000;
  if (status === "partial") score += 50_000;
  if (market.quality_status === "critical") score -= 200_000;
  if (market.quality_status === "warning") score -= 20_000;
  score += (market.observation_days || 0) * 100;
  score += Math.log10(Math.max(1, market.volume_usd || market.tvl_usd || 1));
  return score;
}

function preferredCatalogMarket(markets, type, tokenSummary) {
  const candidates = markets.filter((market) => market.market_type === type);
  const depthReadyCandidates = candidates.filter((market) => (
    Boolean(liquidityRenderableMarket(market))
  ));
  const cleanDepthReadyCandidates = depthReadyCandidates.filter((market) => (
    liquidityRelevantFlags(market).length === 0
  ));
  const rankedCandidates = cleanDepthReadyCandidates.length
    ? cleanDepthReadyCandidates
    : depthReadyCandidates.length
      ? depthReadyCandidates
      : candidates;
  return [...rankedCandidates]
    .sort((a, b) => (
      catalogMarketScore(b, tokenSummary) - catalogMarketScore(a, tokenSummary)
      || a.market_id.localeCompare(b.market_id)
    ))[0];
}

function validDepth(value) {
  return finite(value) && value >= 0;
}

function isMeasuredDepthStatus(market) {
  return MEASURED_DEPTH_STATUSES.has(market?.depth_status);
}

function selectedLiquidityMarkets() {
  if (!app.catalog) return { token: "", marketA: null, marketB: null };
  const token = byId("facts-token").value;
  const markets = factsMarketsForToken(token);
  return {
    token,
    marketA: markets.find((market) => market.market_id === byId("facts-market-a").value) || null,
    marketB: markets.find((market) => market.market_id === byId("facts-market-b").value) || null,
  };
}

function liquiditySideDefinition(market) {
  if (market?.market_type === "cex") {
    return {
      sellField: "bid",
      buyField: "ask",
      sellLabel: "Bid · sell Token",
      buyLabel: "Ask · buy Token",
    };
  }
  return {
    sellField: "sell",
    buyField: "buy",
    sellLabel: "Sell Token",
    buyLabel: "Buy Token",
  };
}

function liquidityMarketLabel(slot, market) {
  if (!market) return `${slot} · unavailable`;
  return `${slot} · ${market.market_type.toUpperCase()} · ${market.venue} · ${market.instrument}`;
}

function liquidityDepthValue(market, band, component = "total") {
  if (!market) return null;
  const value = market[`${component}_depth_${band}bps_usd`];
  return validDepth(value) ? value : null;
}

function liquidityDepthIssues(market) {
  if (!market) return [];
  const sides = liquiditySideDefinition(market);
  const components = ["total", sides.sellField, sides.buyField];
  const issues = [];
  const measuredStatus = isMeasuredDepthStatus(market);
  const shape = DEPTH_BANDS.map((band) => ({
    band,
    complete: Boolean(market[`depth_${band}bps_complete`]),
    values: components.map((component) => market[`${component}_depth_${band}bps_usd`]),
  }));
  if (measuredStatus) {
    shape.forEach(({ band, values }) => {
      if (values.some((value) => !validDepth(value))) {
        issues.push(`${market.depth_status} depth is missing a total or directional value at ±${band} bps`);
      }
    });
    if (
      (market.depth_status === "observed" || market.depth_status === "complete")
      && shape.some(({ complete }) => !complete)
    ) {
      issues.push(`${market.depth_status} status contains an incomplete measured band`);
    }
    if (
      market.depth_status === "partial"
      && shape.every(({ complete }) => complete)
    ) {
      issues.push("partial status contains no incomplete measured band");
    }
  } else if (
    shape.some(({ complete, values }) => (
      complete || values.some((value) => value !== null && value !== undefined)
    ))
  ) {
    issues.push(`${market.depth_status || "unavailable"} status contains measured depth fields`);
  }
  components.forEach((component) => {
    let previous = null;
    DEPTH_BANDS.forEach((band) => {
      const value = market[`${component}_depth_${band}bps_usd`];
      if (value !== null && value !== undefined && !validDepth(value)) {
        issues.push(`${component} depth at ±${band} bps is negative or non-finite`);
        return;
      }
      if (validDepth(value) && previous !== null) {
        const tolerance = Math.max(1e-8, Math.abs(previous) * 1e-10);
        if (value + tolerance < previous) {
          issues.push(`${component} cumulative depth falls between measured bands`);
        }
      }
      if (validDepth(value)) previous = value;
    });
  });
  let incompleteSeen = false;
  DEPTH_BANDS.forEach((band) => {
    const total = liquidityDepthValue(market, band);
    const sell = liquidityDepthValue(market, band, sides.sellField);
    const buy = liquidityDepthValue(market, band, sides.buyField);
    const hasPoint = total !== null || sell !== null || buy !== null;
    const complete = Boolean(market[`depth_${band}bps_complete`]);
    if (hasPoint && !complete) incompleteSeen = true;
    if (hasPoint && complete && incompleteSeen) {
      issues.push(`completeness returns to true at ±${band} bps after an incomplete band`);
    }
    if (total !== null && sell !== null && buy !== null) {
      const tolerance = Math.max(1e-8, Math.abs(total) * 1e-10);
      if (Math.abs(total - sell - buy) > tolerance) {
        issues.push(`directional depth does not sum to total at ±${band} bps`);
      }
    }
  });
  return [...new Set(issues)];
}

function liquidityRenderableMarket(market, issues = liquidityDepthIssues(market)) {
  return market && isMeasuredDepthStatus(market) && !issues.length ? market : null;
}

function preferredLiquidityToken(catalog) {
  const withCleanPair = catalog.tokens.find((token) => {
    const markets = catalog.markets.filter((market) => market.token_symbol === token);
    return ["cex", "dex"].every((type) => {
      const market = preferredCatalogMarket(markets, type, null);
      return Boolean(
        liquidityRenderableMarket(market)
        && liquidityRelevantFlags(market).length === 0
        && market.depth_status !== "partial"
      );
    });
  });
  if (withCleanPair) return withCleanPair;
  const withBothMarketTypes = catalog.tokens.find((token) => {
    const marketTypes = new Set(catalog.markets
      .filter((market) => (
        market.token_symbol === token && liquidityRenderableMarket(market)
      ))
      .map((market) => market.market_type));
    return marketTypes.has("cex") && marketTypes.has("dex");
  });
  if (withBothMarketTypes) return withBothMarketTypes;
  return catalog.tokens.find((token) => (
    catalog.markets.filter((market) => (
      market.token_symbol === token && liquidityRenderableMarket(market)
    )).length >= 2
  )) || catalog.tokens[0] || "";
}

function liquiditySeriesForMarket(slot, market) {
  if (!liquidityRenderableMarket(market)) return [];
  const sides = liquiditySideDefinition(market);
  const configurations = app.liquidityView === "total"
    ? [{
        component: "total",
        label: `${liquidityMarketLabel(slot, market)} · Total`,
        className: `series-${slot.toLowerCase()}-total`,
        filled: true,
      }]
    : [
        {
          component: sides.sellField,
          label: `${liquidityMarketLabel(slot, market)} · ${sides.sellLabel}`,
          className: `series-${slot.toLowerCase()}-sell`,
          filled: true,
        },
        {
          component: sides.buyField,
          label: `${liquidityMarketLabel(slot, market)} · ${sides.buyLabel}`,
          className: `series-${slot.toLowerCase()}-buy`,
          filled: false,
        },
      ];
  return configurations.map((configuration) => ({
    ...configuration,
    slot,
    market,
    points: DEPTH_BANDS.map((band) => ({
      band,
      value: liquidityDepthValue(market, band, configuration.component),
      complete: Boolean(market[`depth_${band}bps_complete`]),
    })),
  })).filter((item) => item.points.some((point) => validDepth(point.value)));
}

function formatLiquidityAxisUsd(value) {
  if (value === 0) return "$0";
  if (Math.abs(value) < 1) return formatRawUsd(value);
  return compactCurrency.format(value);
}

function formatExactDepth(value, complete) {
  if (!validDepth(value)) return "N/A";
  return `${complete ? "" : "≥"}${formatRawUsd(value)}`;
}

function formatSummaryDepth(value, complete) {
  if (!validDepth(value)) return "N/A";
  return `${complete ? "" : "≥"}${formatCurrency(value)}`;
}

function niceLinearMaximum(maximum) {
  if (!validDepth(maximum) || maximum === 0) return 1;
  const exponent = 10 ** Math.floor(Math.log10(maximum));
  const fraction = maximum / exponent;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * exponent;
}

function liquidityAxis(values, dimensions) {
  const { top, bottom } = dimensions;
  const positive = values.filter((value) => value > 0);
  const hasZero = values.some((value) => value === 0);
  if (app.liquidityScale === "log" && positive.length) {
    let minimumExponent = Math.floor(Math.log10(Math.min(...positive)));
    let maximumExponent = Math.ceil(Math.log10(Math.max(...positive)));
    if (minimumExponent === maximumExponent) {
      minimumExponent -= 1;
      maximumExponent += 1;
    }
    const exponentStep = Math.max(
      1,
      Math.ceil((maximumExponent - minimumExponent) / 5),
    );
    const exponents = [];
    for (
      let exponent = minimumExponent;
      exponent <= maximumExponent;
      exponent += exponentStep
    ) {
      exponents.push(exponent);
    }
    if (exponents.at(-1) !== maximumExponent) exponents.push(maximumExponent);
    const positiveBottom = bottom - (hasZero ? 16 : 0);
    const exponentSpan = maximumExponent - minimumExponent;
    return {
      mode: "log",
      scaleLabel: hasZero ? "Log USD · measured zero rail" : "Log USD",
      hasZero,
      ticks: exponents.map((exponent) => 10 ** exponent),
      y(value) {
        if (value === 0) return bottom;
        return positiveBottom - (
          (Math.log10(value) - minimumExponent) / exponentSpan
        ) * (positiveBottom - top);
      },
    };
  }
  if (!positive.length && hasZero) {
    return {
      mode: "linear",
      scaleLabel: "All observed values are measured zero",
      hasZero: true,
      ticks: [0],
      y() {
        return bottom;
      },
    };
  }
  const maximum = niceLinearMaximum(Math.max(0, ...values));
  return {
    mode: "linear",
    scaleLabel: app.liquidityScale === "log"
      ? "All observed values are zero · linear zero baseline"
      : "Linear USD",
    hasZero,
    ticks: [0, maximum * 0.25, maximum * 0.5, maximum * 0.75, maximum],
    y(value) {
      return bottom - (value / maximum) * (bottom - top);
    },
  };
}

function liquidityChartDimensions() {
  const renderedWidth = byId("liquidity-plot")?.clientWidth;
  if (window.matchMedia("(max-width: 700px)").matches) {
    return {
      width: Math.max(280, Math.round(renderedWidth || 320)),
      height: 300,
      left: 66,
      right: 14,
      top: 22,
      bottom: 250,
      layout: "mobile",
    };
  }
  return {
    ...LIQUIDITY_CHART,
    width: Math.max(640, Math.round(renderedWidth || LIQUIDITY_CHART.width)),
    layout: "desktop",
  };
}

function liquidityTooltipText(series, point) {
  const status = point.complete ? "complete measured band" : "observed lower bound";
  const block = series.market.depth_block_number
    ? `block ${series.market.depth_block_number}`
    : "";
  const flags = qualityFlagObjects(series.market, series.market.market_type)
    .filter((flag) => flag.code !== "low_daily_coverage")
    .map((flag) => QUALITY_FLAG_LABELS[flag.code] || flag.code.replaceAll("_", " "))
    .join(", ");
  return [
    series.label,
    `±${point.band} bps`,
    formatExactDepth(point.value, point.complete),
    status,
    formatUtcTimestamp(series.market.depth_observed_at),
    series.market.depth_method || "method unavailable",
    block,
    flags ? `flags: ${flags}` : "",
  ].filter(Boolean).join(" · ");
}

function liquidityMarkerMarkup(series, point, x, y) {
  const markerClass = `liquidity-marker ${series.className}${series.filled ? " filled" : " outlined"}`;
  const title = escapeHtml(liquidityTooltipText(series, point));
  const circleRadius = app.liquidityView === "directional" && !series.filled ? 7 : 5;
  const diamondRadius = app.liquidityView === "directional" && !series.filled ? 9 : 7;
  const core = series.slot === "A"
    ? `<circle class="${markerClass}" cx="${x}" cy="${y}" r="${circleRadius}"></circle>`
    : `<path class="${markerClass}" d="M ${x} ${y - diamondRadius} L ${x + diamondRadius} ${y} L ${x} ${y + diamondRadius} L ${x - diamondRadius} ${y} Z"></path>`;
  const incomplete = point.complete
    ? ""
    : series.slot === "A"
      ? `<circle class="liquidity-lower-bound-ring ${series.className}" cx="${x}" cy="${y}" r="${circleRadius + 4}"></circle>`
      : `<path class="liquidity-lower-bound-ring ${series.className}" d="M ${x} ${y - diamondRadius - 4} L ${x + diamondRadius + 4} ${y} L ${x} ${y + diamondRadius + 4} L ${x - diamondRadius - 4} ${y} Z"></path>`;
  return `<g
      class="liquidity-point"
      tabindex="0"
      role="graphics-symbol"
      aria-label="${title}"
      aria-describedby="liquidity-tooltip"
      data-tooltip="${title}"
    >
      <title>${title}</title>
      <circle class="liquidity-hit-target" cx="${x}" cy="${y}" r="22"></circle>
      <circle class="liquidity-focus-ring" cx="${x}" cy="${y}" r="13"></circle>
      ${incomplete}
      ${core}
    </g>`;
}

function renderLiquiditySvg(series) {
  const svg = byId("liquidity-chart");
  const emptyState = byId("liquidity-empty");
  emptyState.textContent = "No source-backed depth bands are available for the selected markets.";
  const dimensions = liquidityChartDimensions();
  app.liquidityLayoutMode = dimensions.layout;
  svg.setAttribute("viewBox", `0 0 ${dimensions.width} ${dimensions.height}`);
  const plotWidth = dimensions.width - dimensions.left - dimensions.right;
  const values = series.flatMap((item) => item.points)
    .map((point) => point.value)
    .filter(validDepth);
  if (!values.length) {
    svg.innerHTML = "";
    app.liquidityEffectiveScale = null;
    app.liquidityEffectiveScaleLabel = "";
    emptyState.hidden = false;
    return false;
  }
  emptyState.hidden = true;
  const axis = liquidityAxis(values, dimensions);
  app.liquidityEffectiveScale = axis.mode;
  app.liquidityEffectiveScaleLabel = axis.scaleLabel;
  const x = (band) => dimensions.left + (band / 100) * plotWidth;
  const yGrid = axis.ticks.map((tick) => {
    const y = axis.y(tick);
    return `<line class="liquidity-grid-line" x1="${dimensions.left}" y1="${y}" x2="${dimensions.width - dimensions.right}" y2="${y}"></line>
      <text class="liquidity-axis-label" x="${dimensions.left - 9}" y="${y + 4}" text-anchor="end">${escapeHtml(formatLiquidityAxisUsd(tick))}</text>`;
  }).join("");
  const zeroRail = axis.mode === "log" && axis.hasZero
    ? `<line class="liquidity-zero-rail" x1="${dimensions.left}" y1="${dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.bottom}"></line>
       <text class="liquidity-axis-label" x="${dimensions.left - 9}" y="${dimensions.bottom + 4}" text-anchor="end">$0</text>
       <text class="liquidity-zero-label" x="${dimensions.left + 6}" y="${dimensions.bottom - 5}">measured zero</text>`
    : `<line class="liquidity-axis-line" x1="${dimensions.left}" y1="${dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.bottom}"></line>`;
  const xTicks = DEPTH_BANDS.map((band) => {
    const xValue = x(band);
    return `<line class="liquidity-x-guide" x1="${xValue}" y1="${dimensions.top}" x2="${xValue}" y2="${dimensions.bottom}"></line>
      <text class="liquidity-axis-label" x="${xValue}" y="${dimensions.bottom + 22}" text-anchor="middle">±${band}</text>`;
  }).join("");
  const markers = series.map((item) => item.points
    .filter((point) => validDepth(point.value))
    .map((point) => liquidityMarkerMarkup(
      item,
      point,
      Number(x(point.band).toFixed(2)),
      Number(axis.y(point.value).toFixed(2)),
    )).join("")).join("");
  svg.innerHTML = `
    <title id="liquidity-svg-title">Discrete cumulative liquidity depth profile</title>
    <desc id="liquidity-svg-description">Measured point-in-time USD depth at four price-distance thresholds. No values are interpolated between markers.</desc>
    ${yGrid}
    ${xTicks}
    ${zeroRail}
    <line class="liquidity-axis-line" x1="${dimensions.left}" y1="${dimensions.top}" x2="${dimensions.left}" y2="${dimensions.bottom}"></line>
    <text class="liquidity-axis-title" x="${(dimensions.left + dimensions.width - dimensions.right) / 2}" y="${dimensions.height - 12}" text-anchor="middle">Absolute distance from reference price (bps)</text>
    <text class="liquidity-axis-title" transform="translate(16 ${(dimensions.top + dimensions.bottom) / 2}) rotate(-90)" text-anchor="middle">Cumulative source-backed depth (USD)</text>
    <text class="liquidity-scale-label" x="${dimensions.width - dimensions.right}" y="${dimensions.top - 8}" text-anchor="end">${escapeHtml(axis.scaleLabel)}</text>
    ${markers}
  `;
  return true;
}

function renderLiquidityLegend(series) {
  byId("liquidity-legend").innerHTML = series.length
    ? series.map((item) => `<div class="liquidity-legend-item">
        <span class="liquidity-legend-marker ${item.className} ${item.filled ? "filled" : "outlined"} ${item.slot === "B" ? "diamond" : ""}" aria-hidden="true"></span>
        <span>${escapeHtml(item.label)}</span>
      </div>`).join("")
    : '<span class="missing">No measured series for this view.</span>';
}

function liquidityCompletenessLabel(market, dataMarket, band, invalid) {
  if (invalid) return "Invalid facts";
  if (!dataMarket) return market?.depth_status || "Unavailable";
  const sides = liquiditySideDefinition(dataMarket);
  const hasValue = [
    liquidityDepthValue(dataMarket, band),
    liquidityDepthValue(dataMarket, band, sides.sellField),
    liquidityDepthValue(dataMarket, band, sides.buyField),
  ].some((value) => value !== null);
  if (!hasValue) return dataMarket.depth_status || "Unavailable";
  return dataMarket[`depth_${band}bps_complete`] ? "Complete" : "Lower bound";
}

function liquidityDepthMarkup(value, complete, market, band, component) {
  if (validDepth(value)) return formatExactDepth(value, complete);
  return naFactMarkup(snapshotMissingReason(
    market,
    "depth",
    `No ${component} depth was published inside ±${band} bps.`,
  ), {
    retryable: snapshotRetryable(market, "depth"),
    token: selectedWorkspaceToken(),
    marketId: market?.market_id,
    marketLabel: snapshotMarketContextLabel(market),
    fact: "depth",
    factLabel: `${component} executable depth`,
    bandBps: band,
  });
}

function renderLiquidityTable(
  marketA,
  marketB,
  dataMarketA,
  dataMarketB,
  invalidA,
  invalidB,
  { single = false } = {},
) {
  const sidesA = liquiditySideDefinition(marketA);
  const sidesB = liquiditySideDefinition(marketB);
  byId("liquidity-a-total-heading").textContent = "A Total";
  byId("liquidity-a-sell-heading").textContent = `A ${sidesA.sellLabel}`;
  byId("liquidity-a-buy-heading").textContent = `A ${sidesA.buyLabel}`;
  byId("liquidity-b-total-heading").textContent = "B Total";
  byId("liquidity-b-sell-heading").textContent = `B ${sidesB.sellLabel}`;
  byId("liquidity-b-buy-heading").textContent = `B ${sidesB.buyLabel}`;
  byId("liquidity-table-caption").textContent = single
    ? "Exact cumulative USD depth for Market A at each observed price-distance band."
    : "Exact cumulative USD depth for the selected two markets at each observed price-distance band.";
  byId("liquidity-table-body").innerHTML = DEPTH_BANDS.map((band) => {
    const completeA = Boolean(dataMarketA?.[`depth_${band}bps_complete`]);
    const completeB = Boolean(dataMarketB?.[`depth_${band}bps_complete`]);
    return `<tr>
      <th scope="row" data-label="Band">±${band} bps</th>
      <td data-label="A Total">${liquidityDepthMarkup(liquidityDepthValue(dataMarketA, band), completeA, marketA, band, "total")}</td>
      <td data-label="A Sell execution">${liquidityDepthMarkup(liquidityDepthValue(dataMarketA, band, sidesA.sellField), completeA, marketA, band, sidesA.sellLabel.toLowerCase())}</td>
      <td data-label="A Buy execution">${liquidityDepthMarkup(liquidityDepthValue(dataMarketA, band, sidesA.buyField), completeA, marketA, band, sidesA.buyLabel.toLowerCase())}</td>
      <td data-label="A Completeness">${escapeHtml(liquidityCompletenessLabel(marketA, dataMarketA, band, invalidA))}</td>
      ${single ? "" : `<td data-label="B Total">${liquidityDepthMarkup(liquidityDepthValue(dataMarketB, band), completeB, marketB, band, "total")}</td>
      <td data-label="B Sell execution">${liquidityDepthMarkup(liquidityDepthValue(dataMarketB, band, sidesB.sellField), completeB, marketB, band, sidesB.sellLabel.toLowerCase())}</td>
      <td data-label="B Buy execution">${liquidityDepthMarkup(liquidityDepthValue(dataMarketB, band, sidesB.buyField), completeB, marketB, band, sidesB.buyLabel.toLowerCase())}</td>
      <td data-label="B Completeness">${escapeHtml(liquidityCompletenessLabel(marketB, dataMarketB, band, invalidB))}</td>`}
    </tr>`;
  }).join("");
}

function liquidityRelevantFlags(market) {
  const relevantCodes = new Set([
    "depth_unavailable",
    "depth_unsupported",
    "unsupported_depth",
    "depth_partial",
    "partial_depth",
    "depth_failed",
    "failed_depth",
    "depth_not_cataloged",
    "zero_depth_10bps",
    "zero_depth_inside_spread",
    "tiny_pool",
    "off_market_pool_state_price",
    "off_market_price",
    "wide_quoted_spread",
    "depth_usd_price_time_mismatch",
    "depth_usd_price_time_warning",
  ]);
  return qualityFlagObjects(market, market?.market_type)
    .filter((flag) => relevantCodes.has(flag.code));
}

function renderLiquidityMarketMeta(slot, market, issues) {
  const element = byId(`liquidity-market-${slot.toLowerCase()}-meta`);
  if (!market) {
    element.innerHTML = `<strong>${slot} · no selected market</strong>`;
    element.dataset.state = "warning";
    return;
  }
  const model = market.depth_protocol_model
    ? `${market.depth_method || "method unavailable"} · ${market.depth_protocol_model}`
    : market.depth_method || "method unavailable";
  const block = market.depth_block_number ? ` · block ${market.depth_block_number}` : "";
  const issueMarkup = issues.length
    ? `<span class="liquidity-integrity-error">${escapeHtml(issues.join("; "))}</span>`
    : "";
  const status = market.depth_status || "unavailable";
  const relevantFlags = liquidityRelevantFlags(market);
  const hasCriticalFlag = relevantFlags.some((flag) => flag.severity === "critical");
  const usdTiming = market.market_type === "dex"
    ? `<span>Pool state ${escapeHtml(formatUtcTimestamp(
      market.depth_block_timestamp,
    ))}</span>
       <span>USD price response ${escapeHtml(formatUtcTimestamp(
         market.depth_usd_price_observed_at,
       ))} · skew ${escapeHtml(formatDurationSeconds(
         market.depth_usd_price_skew_seconds,
       ))} · ${escapeHtml(
         market.depth_usd_price_freshness_status || "unavailable",
       )}</span>`
    : '<span>USD conversion uses the order-book quote basis; an independent price time is not applicable.</span>';
  element.dataset.state = issues.length || status === "failed" || hasCriticalFlag
    ? "critical"
    : status === "observed" || status === "complete"
      ? relevantFlags.length
        ? "warning"
        : "success"
      : "warning";
  element.innerHTML = `
    <div>
      <strong>${escapeHtml(liquidityMarketLabel(slot, market))}</strong>
      <span>${escapeHtml(status)} · ${escapeHtml(formatUtcTimestamp(market.depth_observed_at))}</span>
      <span>${escapeHtml(model)}${escapeHtml(block)}</span>
      ${usdTiming}
      ${issueMarkup}
    </div>
    <div class="quality-badges">${renderQualityBadges(relevantFlags)}</div>
  `;
}

function liquiditySnapshotSkew(marketA, marketB) {
  if (
    !isMeasuredDepthStatus(marketA)
    || !isMeasuredDepthStatus(marketB)
    || liquidityDepthIssues(marketA).length
    || liquidityDepthIssues(marketB).length
  ) {
    return null;
  }
  const timestampA = Date.parse(marketA?.depth_observed_at || "");
  const timestampB = Date.parse(marketB?.depth_observed_at || "");
  if (!Number.isFinite(timestampA) || !Number.isFinite(timestampB)) return null;
  const seconds = Math.round(Math.abs(timestampA - timestampB) / 1000);
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const totalMinutes = Math.round(seconds / 60);
  return `${Math.floor(totalMinutes / 60)}h ${totalMinutes % 60}m`;
}

function renderLiquiditySummary(marketA, marketB, dataMarketA, dataMarketB, { single = false } = {}) {
  const completeA = Boolean(dataMarketA?.depth_100bps_complete);
  const completeB = Boolean(dataMarketB?.depth_100bps_complete);
  byId("liquidity-a-label").textContent = marketA
    ? `A · ${marketA.venue} total at ±100 bps`
    : "Market A at ±100 bps";
  byId("liquidity-b-label").textContent = marketB
    ? `B · ${marketB.venue} total at ±100 bps`
    : "Market B at ±100 bps";
  const depthA = liquidityDepthValue(dataMarketA, 100);
  const depthB = liquidityDepthValue(dataMarketB, 100);
  setFactValue(
    "liquidity-a-100",
    validDepth(depthA),
    formatSummaryDepth(depthA, completeA),
    snapshotMissingReason(
      marketA,
      "depth",
      "Market A has no measured total depth inside ±100 bps.",
    ),
    {
      retryable: snapshotRetryable(marketA, "depth"),
      token: selectedWorkspaceToken(),
      marketId: marketA?.market_id,
      marketLabel: snapshotMarketContextLabel(marketA),
      fact: "depth",
      factLabel: "total executable depth",
      bandBps: 100,
    },
  );
  if (single) return;
  setFactValue(
    "liquidity-b-100",
    validDepth(depthB),
    formatSummaryDepth(depthB, completeB),
    snapshotMissingReason(
      marketB,
      "depth",
      "Market B has no measured total depth inside ±100 bps.",
    ),
    {
      retryable: snapshotRetryable(marketB, "depth"),
      token: selectedWorkspaceToken(),
      marketId: marketB?.market_id,
      marketLabel: snapshotMarketContextLabel(marketB),
      fact: "depth",
      factLabel: "total executable depth",
      bandBps: 100,
    },
  );
  const skew = liquiditySnapshotSkew(marketA, marketB);
  setFactValue(
    "liquidity-skew",
    Boolean(skew),
    skew || "",
    "Snapshot skew requires valid measured-depth timestamps for both selected markets.",
    { token: selectedWorkspaceToken(), factLabel: "depth snapshot skew" },
  );
  const pairedBands = DEPTH_BANDS.filter((band) => (
    liquidityDepthValue(dataMarketA, band) !== null
    && liquidityDepthValue(dataMarketB, band) !== null
    && dataMarketA?.[`depth_${band}bps_complete`]
    && dataMarketB?.[`depth_${band}bps_complete`]
  )).length;
  byId("liquidity-paired-bands").textContent = `${pairedBands} / ${DEPTH_BANDS.length}`;
}

function renderLiquidityCurve() {
  if (!app.catalog) return;
  const selection = selectedMarketSelection();
  const single = selection.selection === "single" && !selection.marketB;
  const { token, marketA, marketB } = selectedLiquidityMarkets();
  const issuesA = liquidityDepthIssues(marketA);
  const issuesB = single ? [] : liquidityDepthIssues(marketB);
  const dataMarketA = liquidityRenderableMarket(marketA, issuesA);
  const dataMarketB = single ? null : liquidityRenderableMarket(marketB, issuesB);
  const series = [
    ...liquiditySeriesForMarket("A", marketA),
    ...(single ? [] : liquiditySeriesForMarket("B", marketB)),
  ];
  const plotted = renderLiquiditySvg(series);
  const plottedSlots = new Set(series.map((item) => item.slot));
  const selectedSlots = single ? [["A", marketA]] : [["A", marketA], ["B", marketB]];
  const unavailableSlots = selectedSlots.filter(([slot]) => !plottedSlots.has(slot));
  const failedSlots = unavailableSlots
    .filter(([, market]) => market?.depth_status === "failed")
    .map(([slot]) => slot);
  const partialSlots = selectedSlots.filter(([, market]) => market?.depth_status === "partial");
  const qualityWarningSlots = selectedSlots.map(([slot, market]) => [slot, liquidityRelevantFlags(market)])
    .filter(([, flags]) => flags.length);
  const hasCriticalQualityFlag = qualityWarningSlots.some(([, flags]) => (
    flags.some((flag) => flag.severity === "critical")
  ));
  renderLiquidityLegend(series);
  renderLiquiditySummary(marketA, marketB, dataMarketA, dataMarketB, { single });
  renderLiquidityTable(
    marketA,
    marketB,
    dataMarketA,
    dataMarketB,
    Boolean(marketA && issuesA.length),
    Boolean(marketB && issuesB.length),
    { single },
  );
  renderLiquidityMarketMeta("A", marketA, issuesA);
  if (single) byId("liquidity-market-b-meta").innerHTML = "";
  else renderLiquidityMarketMeta("B", marketB, issuesB);
  const skew = single ? null : liquiditySnapshotSkew(marketA, marketB);
  const status = byId("liquidity-status");
  const integrityIssues = [...issuesA, ...issuesB];
  const scaleStatus = !plotted
    ? "no drawable scale"
    : app.liquidityScale === "log" && app.liquidityEffectiveScale !== "log"
      ? "measured-zero baseline (log not applicable)"
      : `${app.liquidityEffectiveScale || app.liquidityScale} scale`;
  status.dataset.state = integrityIssues.length
    ? "critical"
    : failedSlots.length
      ? "critical"
      : hasCriticalQualityFlag
        ? "critical"
      : unavailableSlots.length
        ? "warning"
        : partialSlots.length
          ? "warning"
          : qualityWarningSlots.length
            ? "warning"
    : plotted
      ? "success"
      : "warning";
  status.textContent = integrityIssues.length
    ? `Invalid market series suppressed: ${integrityIssues.join("; ")}.`
    : `${token || "Selected Token"} · ${app.liquidityView} markers · ${scaleStatus}`
      + `${single ? "" : skew ? ` · snapshot skew ${skew}` : " · snapshot skew unavailable"}`
      + `${unavailableSlots.length
        ? ` · ${unavailableSlots.map(([slot, market]) => (
            `Market ${slot} ${market?.depth_status || "unavailable"}`
          )).join(", ")}; no missing depth was converted to zero`
        : ""}`
      + `${partialSlots.length
        ? ` · ${partialSlots.map(([slot]) => `Market ${slot} partial`).join(", ")}; incomplete bands are lower bounds`
        : ""}`
      + `${qualityWarningSlots.length
        ? ` · ${qualityWarningSlots.map(([slot, flags]) => (
            `Market ${slot} flags: ${flags.map((flag) => (
              QUALITY_FLAG_LABELS[flag.code] || flag.code.replaceAll("_", " ")
            )).join(", ")}`
          )).join("; ")}`
        : ""}`
      + ". Daily date controls do not change these point-in-time snapshots.";
  byId("liquidity-chart-description").textContent = [
    `${token || "Selected Token"} discrete depth profile.`,
    `${app.liquidityView} view on ${scaleStatus}.`,
    marketA
      ? `${liquidityMarketLabel("A", marketA)} is ${marketA.depth_status || "unavailable"} at ${formatUtcTimestamp(marketA.depth_observed_at)}.`
      : "Market A is unavailable.",
    !single && marketB
      ? `${liquidityMarketLabel("B", marketB)} is ${marketB.depth_status || "unavailable"} at ${formatUtcTimestamp(marketB.depth_observed_at)}.`
      : !single ? "Market B is unavailable." : "",
    `Only the four labeled thresholds are measured; missing ${single ? "values are" : "markets are"} not replaced with zero or TVL.`,
  ].join(" ");
  hideLiquidityTooltip();
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

function showLiquidityTooltip(point) {
  const tooltip = byId("liquidity-tooltip");
  if (!point?.dataset.tooltip) return;
  tooltip.textContent = point.dataset.tooltip;
  tooltip.hidden = false;
}

function hideLiquidityTooltip() {
  const tooltip = byId("liquidity-tooltip");
  if (!tooltip) return;
  tooltip.hidden = true;
  tooltip.textContent = "";
}

function bindLiquidityTooltipEvents() {
  const svg = byId("liquidity-chart");
  const plot = byId("liquidity-plot");
  svg.addEventListener("pointerover", (event) => {
    showLiquidityTooltip(event.target.closest?.(".liquidity-point"));
  });
  svg.addEventListener("pointerout", (event) => {
    const point = event.target.closest?.(".liquidity-point");
    if (point && !point.contains(event.relatedTarget)) hideLiquidityTooltip();
  });
  svg.addEventListener("focusin", (event) => {
    showLiquidityTooltip(event.target.closest?.(".liquidity-point"));
  });
  svg.addEventListener("focusout", (event) => {
    const point = event.target.closest?.(".liquidity-point");
    if (point && !point.contains(event.relatedTarget)) hideLiquidityTooltip();
  });
  plot.addEventListener("click", (event) => {
    const point = event.target.closest?.(".liquidity-point");
    if (point) showLiquidityTooltip(point);
    else hideLiquidityTooltip();
  });
  plot.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideLiquidityTooltip();
      plot.focus();
    }
  });
}

function populateFactsMarkets({
  preserve = false,
  requestedA = null,
  requestedB = null,
  allowDefaults = true,
  persistSelection = app.pairSelectionSource !== "transient",
} = {}) {
  if (!app.catalog) return;
  const token = byId("facts-token").value;
  const markets = factsMarketsForToken(token);
  const tokenSummary = app.payload?.tokens.find((row) => row.token_symbol === token);
  const saved = normalizedSavedSelection(app.pairSelections[token]) || {};
  const previousA = requestedA ?? (preserve ? byId("facts-market-a").value : saved.marketA || "");
  const previousB = requestedB ?? (preserve ? byId("facts-market-b").value : saved.marketB || "");
  const cex = preferredCatalogMarket(markets, "cex", tokenSummary);
  const dex = preferredCatalogMarket(markets, "dex", tokenSummary);
  const marketA = markets.find((market) => market.market_id === previousA)
    || (allowDefaults ? cex || markets[0] : null);
  let marketB = markets.find((market) => (
    market.market_id === previousB && market.market_id !== marketA?.market_id
  )) || (allowDefaults
    ? dex || markets.find((market) => market.market_id !== marketA?.market_id)
    : null);
  if (marketA && marketB && marketB.market_id === marketA.market_id) {
    marketB = allowDefaults
      ? markets.find((market) => market.market_id !== marketA.market_id)
      : null;
    app.workspaceSelection = "";
  }
  byId("facts-market-a").innerHTML = factsOptions(markets, marketA?.market_id);
  byId("facts-market-b").innerHTML = factsOptions(
    markets,
    marketB?.market_id,
    { emptyLabel: "Market A only — no comparison" },
  );
  byId("facts-market-a").value = marketA?.market_id || "";
  byId("facts-market-b").value = marketB?.market_id || "";
  if (persistSelection) {
    persistSelectedSelection();
  }
  renderFactsMarketWarnings();
  renderLiquidityCurve();
  renderWorkspaceContext();
  renderWorkspaceMarkets();
}

function updateFactsContract() {
  const metadata = app.catalog.metadata;
  const weights = metadata.primary_selection?.weights || {};
  const selectionWeights = [
    ["volume", weights.window_volume_share],
    ["coverage", weights.coverage_ratio],
    ["quote quality", weights.quote_quality],
    ["depth support", weights.depth_support],
  ].filter(([, value]) => finite(value));
  byId("facts-contract-copy").textContent = [
    `Grain: ${metadata.time_grain}.`,
    `Price: ${metadata.price_field}, quoted in ${metadata.price_quote_asset}.`,
    "Volume: daily USD.",
    `Missing values: ${metadata.missing_value_rule}`,
    selectionWeights.length
      ? `Primary score weights: ${selectionWeights.map(([label, value]) => `${label} ${value}%`).join(", ")}.`
      : "",
    metadata.semantic_boundary,
  ].filter(Boolean).join(" ");
  const tvl = metadata.tvl_snapshot;
  const seriesRows = tvl?.market_series_rows ?? tvl?.pool_rows;
  const physicalPools = tvl?.unique_pool_count ?? metadata.dex_unique_pool_count;
  byId("facts-source-copy").textContent = [
    `Catalog v${metadata.catalog_version}.`,
    metadata.cex_normalization_note,
    `Sources: ${metadata.sources.map((source) => `${source.name} (${source.sha256})`).join(" | ")}.`,
    tvl
      ? `TVL: ${tvl.status_counts.observed}/${seriesRows} market series`
        + `${finite(physicalPools) ? ` across ${physicalPools} physical pools` : ""}`
        + ` observed at ${formatUtcTimestamp(tvl.observed_at)}.`
      : "",
    metadata.cex_depth_snapshot
      ? `CEX depth: ${metadata.cex_depth_snapshot.status_counts.observed} complete, ${metadata.cex_depth_snapshot.status_counts.partial} partial, ${metadata.cex_depth_snapshot.status_counts.failed} failed at ${formatUtcTimestamp(metadata.cex_depth_snapshot.observed_at)}.`
      : "",
    metadata.dex_depth_snapshot
      ? `DEX depth: ${metadata.dex_depth_snapshot.status_counts.observed || 0} complete, ${metadata.dex_depth_snapshot.status_counts.partial || 0} partial, ${metadata.dex_depth_snapshot.status_counts.unsupported || 0} unsupported, ${metadata.dex_depth_snapshot.status_counts.failed || 0} failed at ${formatUtcTimestamp(metadata.dex_depth_snapshot.observed_at)}.`
      : "",
  ].join(" ");
}

function comparisonDateMs(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ""))) return null;
  const parsed = Date.parse(`${value}T00:00:00Z`);
  if (!Number.isFinite(parsed)) return null;
  return new Date(parsed).toISOString().slice(0, 10) === value ? parsed : null;
}

function comparisonMetricDefinition(metric) {
  const definitions = {
    price: {
      key: "price",
      title: "Daily price observations",
      note: "USD source prices · gaps break the line",
      axisTitle: "Daily source price (USD)",
      scaleLabel: "Linear USD",
    },
    spread: {
      key: "spread",
      title: "Daily Price Gap",
      note: "Symmetric midpoint gap from same-UTC-date closes · research metric only",
      axisTitle: "Daily Price Gap (bps)",
      scaleLabel: "Linear bps",
    },
    volume: {
      key: "volume",
      title: "Daily reported volume",
      note: "Source-reported USD volume · missing is not zero",
      axisTitle: "Daily source-reported volume (USD)",
      scaleLabel: "Linear USD",
    },
  };
  return definitions[metric] || definitions.price;
}

function comparisonChartValue(row, metric, slot) {
  let value;
  if (metric === "spread") value = row?.spread_bps;
  else value = row?.[`market_${slot.toLowerCase()}`]?.[`${metric}_usd`];
  if (!finite(value)) return null;
  if (metric === "price" && value <= 0) return null;
  if ((metric === "volume" || metric === "spread") && value < 0) return null;
  return value;
}

function comparisonSeriesSegments(points) {
  const segments = [];
  let active = [];
  let previousDateMs = null;
  const flush = () => {
    if (active.length) segments.push(active);
    active = [];
    previousDateMs = null;
  };
  points.forEach((point) => {
    if (!finite(point.value) || !finite(point.dateMs)) {
      flush();
      return;
    }
    if (active.length && point.dateMs - previousDateMs !== 86_400_000) flush();
    active.push(point);
    previousDateMs = point.dateMs;
  });
  flush();
  return segments;
}

function comparisonMarketIdentity(slot, market) {
  if (!market) return `${slot} · unavailable`;
  const type = market.market_type ? market.market_type.toUpperCase() : "MARKET";
  return [
    slot,
    type,
    market.venue || "venue unavailable",
    market.instrument || market.market_id || "instrument unavailable",
  ].join(" · ");
}

function comparisonEventMarkers(eventPayload, rows) {
  if (
    eventAvailabilityStatus(eventPayload) !== "available"
    || !rows.length
    || !Array.isArray(eventPayload?.events)
  ) {
    return [];
  }
  const windowStart = rows[0].dateMs;
  const windowEnd = rows.at(-1).dateMs;
  const groupedMarkers = new Map();
  eventPayload.events.forEach((event) => {
    const start = comparisonDateMs(event?.time?.effective_date_start);
    const end = comparisonDateMs(event?.time?.effective_date_end);
    if (!finite(start) || !finite(end) || end < windowStart || start > windowEnd) return;
    const clippedStart = Math.max(start, windowStart);
    const clippedEnd = Math.min(end, windowEnd);
    const key = `${clippedStart}|${clippedEnd}`;
    if (!groupedMarkers.has(key)) {
      groupedMarkers.set(key, {
        startMs: clippedStart,
        endMs: clippedEnd,
        events: [],
      });
    }
    groupedMarkers.get(key).events.push(event);
  });
  return [...groupedMarkers.values()].sort((left, right) => (
    left.startMs - right.startMs || left.endMs - right.endMs
  ));
}

function comparisonChartModel(payload, metric = "price", eventPayload = null) {
  const single = payload?.selection_mode === "single";
  const definition = comparisonMetricDefinition(single && metric === "spread" ? "price" : metric);
  const rows = (Array.isArray(payload?.observations) ? payload.observations : [])
    .map((row, sourceIndex) => ({
      ...row,
      sourceIndex,
      dateMs: comparisonDateMs(row?.date),
    }))
    .filter((row) => finite(row.dateMs))
    .sort((left, right) => (
      left.dateMs - right.dateMs || left.sourceIndex - right.sourceIndex
    ));
  const configurations = single
    ? [{
        slot: "A",
        className: "series-a",
        label: comparisonMarketIdentity("A", payload?.market_a),
      }]
    : definition.key === "spread"
    ? [{
        slot: "spread",
        className: "series-spread",
        label: "A ↔ B Daily Price Gap",
      }]
    : [
        {
          slot: "A",
          className: "series-a",
          label: comparisonMarketIdentity("A", payload?.market_a),
        },
        {
          slot: "B",
          className: "series-b",
          label: comparisonMarketIdentity("B", payload?.market_b),
        },
      ];
  const series = configurations.map((configuration) => {
    const points = rows.map((row) => ({
      date: row.date,
      dateMs: row.dateMs,
      row,
      value: comparisonChartValue(row, definition.key, configuration.slot),
    }));
    return {
      ...configuration,
      points,
      segments: comparisonSeriesSegments(points),
    };
  });
  return {
    definition,
    rows,
    series,
    marketA: payload?.market_a || null,
    marketB: payload?.market_b || null,
    selectionMode: single ? "single" : "pair",
    eventMarkers: comparisonEventMarkers(eventPayload, rows),
  };
}

function comparisonNiceMaximum(maximum) {
  if (!finite(maximum) || maximum <= 0) return 1;
  const exponent = 10 ** Math.floor(Math.log10(maximum));
  const fraction = maximum / exponent;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * exponent;
}

function comparisonChartAxis(values, metric, dimensions) {
  const { top, bottom } = dimensions;
  let minimum;
  let maximum;
  if (metric === "price") {
    minimum = Math.min(...values);
    maximum = Math.max(...values);
    const spread = maximum - minimum;
    const padding = spread > 0
      ? spread * 0.08
      : Math.max(Math.abs(maximum) * 0.02, 1e-9);
    minimum = Math.max(0, minimum - padding);
    maximum += padding;
  } else {
    minimum = 0;
    maximum = comparisonNiceMaximum(Math.max(...values));
  }
  if (!(maximum > minimum)) maximum = minimum + 1;
  const span = maximum - minimum;
  return {
    minimum,
    maximum,
    ticks: Array.from({ length: 5 }, (_, index) => (
      minimum + (span * index) / 4
    )),
    y(value) {
      return bottom - ((value - minimum) / span) * (bottom - top);
    },
  };
}

function comparisonTickRows(rows, maximumTicks) {
  if (rows.length <= maximumTicks) return rows;
  const selected = new Set();
  for (let index = 0; index < maximumTicks; index += 1) {
    selected.add(Math.round((index * (rows.length - 1)) / (maximumTicks - 1)));
  }
  return [...selected].sort((left, right) => left - right).map((index) => rows[index]);
}

function formatComparisonChartValue(metric, value) {
  if (!finite(value)) return "Unavailable · no fill";
  if (metric === "spread") return `${bpsFormat.format(value)} bps`;
  if (metric === "volume") return formatCurrency(value);
  return formatPrice(value);
}

function comparisonEventsOnDate(model, dateMs) {
  return model.eventMarkers.flatMap((marker) => (
    dateMs >= marker.startMs && dateMs <= marker.endMs ? marker.events : []
  ));
}

function comparisonChartTooltipText(model, point) {
  const row = point.row;
  const metric = model.definition.key;
  const eventFacts = comparisonEventsOnDate(model, point.dateMs);
  const eventText = eventFacts.length
    ? `verified events: ${eventFacts.map((event) => [
        event.event_name,
        eventLabel(event.event_type),
        eventLabel(event.lifecycle),
        `${event.time?.effective_at || point.date} (${event.time?.effective_at_precision || "unknown"} precision)`,
        eventSourceHostname(event.source?.url),
        `revision ${event.revision || "unavailable"}`,
      ].join(" · ")).join("; ")} · timing overlay only, not causality`
    : "no verified event marker on this date in the current release";
  if (metric === "spread") {
    return [
      `${point.date} UTC`,
      `Daily Price Gap ${formatComparisonChartValue("spread", point.value)}`,
      "same-UTC-date closes",
      `A price ${formatComparisonChartValue("price", comparisonChartValue(row, "price", "A"))}`,
      `B price ${formatComparisonChartValue("price", comparisonChartValue(row, "price", "B"))}`,
      `absolute closing-price difference ${formatComparisonChartValue("price", row.absolute_spread_usd)}`,
      "research metric only",
      eventText,
    ].join(" · ");
  }
  const label = metric === "price" ? "price" : "volume";
  if (model.selectionMode === "single") {
    return [
      `${point.date} UTC`,
      `${model.series[0]?.label || "Market A"} ${label} ${formatComparisonChartValue(
        metric,
        comparisonChartValue(row, metric, "A"),
      )}`,
      "missing values are not filled",
      eventText,
    ].join(" · ");
  }
  return [
    `${point.date} UTC`,
    `${model.series[0]?.label || "Market A"} ${label} ${formatComparisonChartValue(
      metric,
      comparisonChartValue(row, metric, "A"),
    )}`,
    `${model.series[1]?.label || "Market B"} ${label} ${formatComparisonChartValue(
      metric,
      comparisonChartValue(row, metric, "B"),
    )}`,
    "missing values are not filled",
    eventText,
  ].join(" · ");
}

function comparisonChartDimensions() {
  const renderedWidth = byId("comparison-plot")?.clientWidth;
  if (window.matchMedia("(max-width: 700px)").matches) {
    return {
      width: Math.max(300, Math.round(renderedWidth || 320)),
      height: 300,
      left: 68,
      right: 12,
      top: 28,
      bottom: 248,
      layout: "mobile",
    };
  }
  return {
    ...COMPARISON_CHART,
    width: Math.max(680, Math.round(renderedWidth || COMPARISON_CHART.width)),
    layout: "desktop",
  };
}

function comparisonPathData(segment, x, y) {
  return segment.map((point, index) => (
    `${index ? "L" : "M"} ${x(point.dateMs).toFixed(2)} ${y(point.value).toFixed(2)}`
  )).join(" ");
}

function comparisonPointMarkup(model, series, point, x, y) {
  const tooltip = escapeHtml(comparisonChartTooltipText(model, point));
  const offset = model.selectionMode === "single"
    ? 0
    : series.slot === "A" ? -2.5 : series.slot === "B" ? 2.5 : 0;
  const xValue = Number((x(point.dateMs) + offset).toFixed(2));
  const yValue = Number(y(point.value).toFixed(2));
  if (series.slot === "B") {
    return `<rect class="comparison-marker ${series.className}" x="${xValue - 3.5}" y="${yValue - 3.5}" width="7" height="7"><title>${tooltip}</title></rect>`;
  }
  if (series.slot === "spread") {
    return `<path class="comparison-marker ${series.className}" d="M ${xValue} ${yValue - 4.5} L ${xValue + 4.5} ${yValue} L ${xValue} ${yValue + 4.5} L ${xValue - 4.5} ${yValue} Z"><title>${tooltip}</title></path>`;
  }
  return `<circle class="comparison-marker ${series.className}" data-series-offset="${offset}" cx="${xValue}" cy="${yValue}" r="3.7"><title>${tooltip}</title></circle>`;
}

function comparisonEventMarkerMarkup(marker, x, dimensions) {
  const startX = Number(x(marker.startMs).toFixed(2));
  const endX = Number(x(marker.endMs).toFixed(2));
  const label = escapeHtml([
    marker.events.map((event) => event.event_name).join("; "),
    "verified source-backed event timing",
    "no causal or return claim",
  ].join(" · "));
  if (Math.abs(endX - startX) < 1) {
    return `<g class="comparison-event-overlay" aria-hidden="true">
      <title>${label}</title>
      <line class="comparison-event-line" x1="${startX}" y1="${dimensions.top}" x2="${startX}" y2="${dimensions.bottom}"></line>
      <path class="comparison-event-pin" d="M ${startX - 5} ${dimensions.top} L ${startX + 5} ${dimensions.top} L ${startX} ${dimensions.top + 8} Z"></path>
    </g>`;
  }
  return `<g class="comparison-event-overlay" aria-hidden="true">
    <title>${label}</title>
    <rect class="comparison-event-band" x="${startX}" y="${dimensions.top}" width="${Math.max(1, endX - startX)}" height="${dimensions.bottom - dimensions.top}"></rect>
    <line class="comparison-event-line" x1="${startX}" y1="${dimensions.top}" x2="${startX}" y2="${dimensions.bottom}"></line>
    <line class="comparison-event-line" x1="${endX}" y1="${dimensions.top}" x2="${endX}" y2="${dimensions.bottom}"></line>
  </g>`;
}

function comparisonDateHitMarkup(model, row, index, x, dimensions) {
  const currentX = x(row.dateMs);
  const previousX = index > 0 ? x(model.rows[index - 1].dateMs) : dimensions.left;
  const nextX = index + 1 < model.rows.length
    ? x(model.rows[index + 1].dateMs)
    : dimensions.width - dimensions.right;
  const left = index > 0 ? (previousX + currentX) / 2 : dimensions.left;
  const right = index + 1 < model.rows.length
    ? (currentX + nextX) / 2
    : dimensions.width - dimensions.right;
  const tooltip = escapeHtml(comparisonChartTooltipText(model, {
    date: row.date,
    dateMs: row.dateMs,
    row,
  }));
  return `<rect
    id="comparison-date-${index}"
    class="comparison-date-hit"
    x="${Number(left.toFixed(2))}"
    y="${dimensions.top}"
    width="${Number(Math.max(1, right - left).toFixed(2))}"
    height="${dimensions.bottom - dimensions.top}"
    data-index="${index}"
    data-date="${escapeHtml(row.date)}"
    data-tooltip="${tooltip}"
    aria-hidden="true"
  ></rect>`;
}

function renderComparisonSvg(model) {
  const svg = byId("comparison-chart");
  const emptyState = byId("comparison-chart-empty");
  const dimensions = comparisonChartDimensions();
  app.comparisonChartLayoutMode = dimensions.layout;
  svg.setAttribute("viewBox", `0 0 ${dimensions.width} ${dimensions.height}`);
  const values = model.series.flatMap((series) => series.points)
    .map((point) => point.value)
    .filter(finite);
  if (!model.rows.length || !values.length) {
    svg.innerHTML = "";
    emptyState.textContent = "No source-backed values are available for this metric and date window.";
    emptyState.hidden = false;
    return false;
  }
  emptyState.hidden = true;
  const axis = comparisonChartAxis(values, model.definition.key, dimensions);
  const firstDateMs = model.rows[0].dateMs;
  const lastDateMs = model.rows.at(-1).dateMs;
  const plotWidth = dimensions.width - dimensions.left - dimensions.right;
  const x = firstDateMs === lastDateMs
    ? () => dimensions.left + plotWidth / 2
    : (dateMs) => dimensions.left + (
      (dateMs - firstDateMs) / (lastDateMs - firstDateMs)
    ) * plotWidth;
  const yGrid = axis.ticks.map((tick) => {
    const yValue = axis.y(tick);
    return `<line class="comparison-grid-line" x1="${dimensions.left}" y1="${yValue}" x2="${dimensions.width - dimensions.right}" y2="${yValue}"></line>
      <text class="comparison-axis-label" x="${dimensions.left - 9}" y="${yValue + 4}" text-anchor="end">${escapeHtml(
        formatComparisonChartValue(model.definition.key, tick),
      )}</text>`;
  }).join("");
  const xTicks = comparisonTickRows(
    model.rows,
    dimensions.layout === "mobile" ? 3 : 5,
  ).map((row) => {
    const xValue = x(row.dateMs);
    return `<line class="comparison-x-guide" x1="${xValue}" y1="${dimensions.top}" x2="${xValue}" y2="${dimensions.bottom}"></line>
      <text class="comparison-axis-label" x="${xValue}" y="${dimensions.bottom + 21}" text-anchor="middle">${escapeHtml(row.date)}</text>`;
  }).join("");
  const zeroLine = model.definition.key === "spread"
    ? `<line class="comparison-zero-line" x1="${dimensions.left}" y1="${axis.y(0)}" x2="${dimensions.width - dimensions.right}" y2="${axis.y(0)}"></line>`
    : "";
  const eventOverlays = model.eventMarkers
    .map((marker) => comparisonEventMarkerMarkup(marker, x, dimensions))
    .join("");
  const lines = model.series.map((series) => series.segments
    .filter((segment) => segment.length >= 2)
    .map((segment) => `<path
      class="comparison-series-line ${series.className}"
      d="${comparisonPathData(segment, x, axis.y)}"
      data-segment-start="${escapeHtml(segment[0].date)}"
      data-segment-end="${escapeHtml(segment.at(-1).date)}"
      aria-hidden="true"
    ></path>`).join("")).join("");
  const points = model.series.map((series) => series.points
    .filter((point) => finite(point.value))
    .map((point) => comparisonPointMarkup(model, series, point, x, axis.y))
    .join("")).join("");
  const dateHits = model.rows
    .map((row, index) => comparisonDateHitMarkup(model, row, index, x, dimensions))
    .join("");
  svg.innerHTML = `
    <title id="comparison-svg-title">${escapeHtml(model.definition.title)}</title>
    <desc id="comparison-svg-description">Straight-line source observations. Missing values and non-consecutive UTC dates split each series into separate path segments; no values are interpolated. Verified event overlays show timing only and do not assert causality.</desc>
    ${yGrid}
    ${xTicks}
    ${zeroLine}
    ${eventOverlays}
    <line class="comparison-axis-line" x1="${dimensions.left}" y1="${dimensions.top}" x2="${dimensions.left}" y2="${dimensions.bottom}"></line>
    <line class="comparison-axis-line" x1="${dimensions.left}" y1="${dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.bottom}"></line>
    <text class="comparison-axis-title" x="${(dimensions.left + dimensions.width - dimensions.right) / 2}" y="${dimensions.height - 11}" text-anchor="middle">Observation date (UTC)</text>
    <text class="comparison-axis-title" transform="translate(16 ${(dimensions.top + dimensions.bottom) / 2}) rotate(-90)" text-anchor="middle">${escapeHtml(model.definition.axisTitle)}</text>
    <text class="comparison-scale-label" x="${dimensions.width - dimensions.right}" y="${dimensions.top - 9}" text-anchor="end">${escapeHtml(model.definition.scaleLabel)}</text>
    ${lines}
    ${points}
    ${dateHits}
  `;
  app.comparisonChartActiveIndex = Math.max(
    0,
    Math.min(app.comparisonChartActiveIndex, model.rows.length - 1),
  );
  return true;
}

function renderComparisonLegend(model) {
  const identities = model.selectionMode === "single"
    ? [["A", "series-a", model.marketA]]
    : [
        ["A", "series-a", model.marketA],
        ["B", "series-b", model.marketB],
      ];
  const identityItems = identities.map(([slot, className, market]) => `<div class="comparison-legend-item">
      <span class="comparison-legend-line ${className}" aria-hidden="true"></span>
      <span>${escapeHtml(comparisonMarketIdentity(slot, market))}${model.definition.key === "spread" ? " · input" : ""}</span>
    </div>`).join("");
  const derived = model.definition.key === "spread"
    ? `<div class="comparison-legend-item comparison-derived-key">
        <span class="comparison-legend-line series-spread" aria-hidden="true"></span>
        <span>Displayed series · |A price − B price| ÷ midpoint × 10,000</span>
      </div>`
    : "";
  const events = model.eventMarkers.length
    ? `<div class="comparison-legend-item comparison-event-key">
        <span class="comparison-event-legend" aria-hidden="true"></span>
        <span>${model.eventMarkers.reduce((count, marker) => count + marker.events.length, 0)} verified Event Facts · timing overlay only, not causality</span>
      </div>`
    : "";
  byId("comparison-chart-legend").innerHTML = identityItems + derived + events;
}

function clearComparisonChart(message) {
  const definition = comparisonMetricDefinition(app.comparisonMetric);
  byId("comparison-chart-title").textContent = definition.title;
  byId("comparison-chart-note").textContent = definition.note;
  byId("comparison-chart").innerHTML = "";
  byId("comparison-chart-empty").textContent = message;
  byId("comparison-chart-empty").hidden = false;
  byId("comparison-chart-legend").innerHTML = "";
  byId("comparison-event-status").textContent = (
    "Event overlay is unavailable without a current comparison."
  );
  byId("comparison-event-status").dataset.state = "unavailable";
  byId("comparison-chart-description").textContent = `${definition.title}. ${message}`;
  hideComparisonChartTooltip();
}

function renderComparisonChart(payload) {
  const model = comparisonChartModel(
    payload,
    app.comparisonMetric,
    app.eventFacts,
  );
  byId("comparison-chart-title").textContent = model.definition.title;
  byId("comparison-chart-note").textContent = model.definition.note;
  const plotted = renderComparisonSvg(model);
  renderComparisonLegend(model);
  const eventAvailability = eventAvailabilityStatus(app.eventFacts);
  const eventCount = model.eventMarkers.reduce(
    (count, marker) => count + marker.events.length,
    0,
  );
  if (eventAvailability !== "available") {
    byId("comparison-event-status").textContent = (
      "Verified Event Fact overlay is unavailable for this request; this is not a zero-event result."
    );
    byId("comparison-event-status").dataset.state = "unavailable";
  } else if (eventCount) {
    byId("comparison-event-status").textContent = (
      `${eventCount} verified Event Facts overlap this chart window. `
      + "Markers show timing only; they do not claim return impact or causality."
    );
    byId("comparison-event-status").dataset.state = "available";
  } else {
    byId("comparison-event-status").textContent = (
      "No verified Event Facts overlap this chart window in the current release; "
      + "this does not prove no event exists."
    );
    byId("comparison-event-status").dataset.state = "empty";
  }
  const validPointCount = model.series.reduce((count, series) => (
    count + series.points.filter((point) => finite(point.value)).length
  ), 0);
  const segmentCount = model.series.reduce((count, series) => (
    count + series.segments.length
  ), 0);
  byId("comparison-chart-description").textContent = [
    `${payload?.token_symbol || "Selected Token"} ${model.definition.title.toLowerCase()}.`,
    `${validPointCount} source-backed plotted values across ${segmentCount} uninterrupted daily segments.`,
    "Missing or invalid observations and non-consecutive UTC dates break lines; no interpolation or forward fill is used.",
    model.eventMarkers.length
      ? `${model.eventMarkers.reduce((count, marker) => count + marker.events.length, 0)} verified Event Facts are overlaid by effective date; timing does not imply causality.`
      : "No verified Event Fact marker is available inside this chart window.",
    plotted
      ? "Use Left/Right, Home/End, pointer, or click on the chart to inspect one non-overlapping UTC date; exact observations remain in the table."
      : "No values are drawable for this metric and window.",
  ].join(" ");
  hideComparisonChartTooltip();
}

function showComparisonChartTooltip(target) {
  const tooltip = byId("comparison-chart-tooltip");
  if (!target?.dataset.tooltip) return;
  const parsedIndex = Number.parseInt(target.dataset.index, 10);
  if (Number.isInteger(parsedIndex)) {
    app.comparisonChartActiveIndex = parsedIndex;
  }
  tooltip.textContent = target.dataset.tooltip;
  tooltip.hidden = false;
}

function hideComparisonChartTooltip() {
  const tooltip = byId("comparison-chart-tooltip");
  if (!tooltip) return;
  tooltip.hidden = true;
  tooltip.textContent = "";
}

function bindComparisonChartTooltipEvents() {
  const svg = byId("comparison-chart");
  const plot = byId("comparison-plot");
  const zoneForIndex = (index) => {
    const zones = [...svg.querySelectorAll?.(".comparison-date-hit") || []];
    if (!zones.length) return null;
    const bounded = Math.max(0, Math.min(index, zones.length - 1));
    return zones[bounded];
  };
  svg.addEventListener("pointerover", (event) => {
    showComparisonChartTooltip(event.target.closest?.(".comparison-date-hit"));
  });
  svg.addEventListener("pointerout", (event) => {
    const zone = event.target.closest?.(".comparison-date-hit");
    if (zone && !zone.contains(event.relatedTarget)) hideComparisonChartTooltip();
  });
  plot.addEventListener("click", (event) => {
    const zone = event.target.closest?.(".comparison-date-hit");
    if (zone) showComparisonChartTooltip(zone);
    else hideComparisonChartTooltip();
  });
  plot.addEventListener("focus", () => {
    showComparisonChartTooltip(zoneForIndex(app.comparisonChartActiveIndex));
  });
  plot.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideComparisonChartTooltip();
      plot.focus();
      return;
    }
    const zones = [...svg.querySelectorAll?.(".comparison-date-hit") || []];
    if (!zones.length) return;
    let nextIndex = app.comparisonChartActiveIndex;
    if (event.key === "ArrowLeft") nextIndex -= 1;
    else if (event.key === "ArrowRight") nextIndex += 1;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = zones.length - 1;
    else return;
    event.preventDefault();
    showComparisonChartTooltip(zoneForIndex(nextIndex));
  });
  plot.addEventListener("blur", (event) => {
    if (!plot.contains(event.relatedTarget)) {
      hideComparisonChartTooltip();
    }
  });
}

function setComparisonLoading(message) {
  app.comparison = null;
  app.eventFacts = null;
  applyWorkspaceSelectionMode(app.workspaceSelection);
  setComparisonDocumentCopy(app.workspaceSelection === "single");
  byId("facts-workbench").setAttribute("aria-busy", "true");
  byId("compare-markets").disabled = true;
  hideError(byId("comparison-error"));
  showStatus(byId("comparison-status"), message);
  [
    "compare-date",
    "compare-absolute",
    "compare-bps",
    "compare-days",
    "compare-a-return",
    "compare-b-return",
    "compare-a-volatility",
    "compare-b-volatility",
  ].forEach((id) => {
    byId(id).textContent = "—";
  });
  const single = app.workspaceSelection === "single";
  clearComparisonChart(single ? "Loading Market A…" : "Loading the selected markets…");
  byId("comparison-body").innerHTML = `<tr><td colspan="${single ? 3 : 8}" class="missing">${single ? "Loading Market A…" : "Loading the selected markets…"}</td></tr>`;
}

function invalidateComparisonRequest() {
  if (app.comparisonController) app.comparisonController.abort();
  app.comparisonController = null;
  app.comparisonRequestKey = "";
  app.comparisonRequestId += 1;
  return app.comparisonRequestId;
}

function clearComparisonResult(message = "") {
  app.comparison = null;
  applyWorkspaceSelectionMode(app.workspaceSelection);
  setComparisonDocumentCopy(app.workspaceSelection === "single");
  [
    "compare-date",
    "compare-absolute",
    "compare-bps",
    "compare-days",
    "compare-a-return",
    "compare-b-return",
    "compare-a-volatility",
    "compare-b-volatility",
  ].forEach((id) => {
    byId(id).textContent = "—";
  });
  byId("market-a-price-heading").textContent = "Market A Price (USD)";
  byId("market-a-volume-heading").textContent = "Market A Volume (USD)";
  byId("market-b-price-heading").textContent = "Market B Price (USD)";
  byId("market-b-volume-heading").textContent = "Market B Volume (USD)";
  clearComparisonChart(message || "No current comparison result.");
  byId("comparison-body").innerHTML = `<tr><td colspan="${app.workspaceSelection === "single" ? 3 : 8}" class="missing">No current result.</td></tr>`;
  hideStatus(byId("comparison-status"));
  if (message) showError(byId("comparison-error"), message);
  else hideError(byId("comparison-error"));
  byId("facts-workbench").setAttribute("aria-busy", "false");
  byId("compare-markets").disabled = false;
}

function comparisonValueMarkup(value, formatter, reason, context = {}) {
  return finite(value)
    ? escapeHtml(formatter(value))
    : naFactMarkup(reason, context);
}

function setComparisonDocumentCopy(single) {
  byId("daily-comparison-title").textContent = single
    ? "Daily price and volume · Market A"
    : "Daily price and volume comparison";
  byId("comparison-plot").setAttribute(
    "aria-label",
    single ? "Interactive daily Market A chart" : "Interactive daily market comparison chart",
  );
  byId("comparison-table-region").setAttribute(
    "aria-label",
    single ? "Daily Market A observations" : "Daily two-market comparison observations",
  );
  byId("comparison-table-caption").textContent = single
    ? "Daily observations for Market A; missing values are never filled."
    : "Daily observations for the selected two markets; missing values are never filled.";
}

function renderSingleComparison(payload) {
  app.comparison = payload;
  applyWorkspaceSelectionMode("single");
  setComparisonDocumentCopy(true);
  const latest = payload.latest_market_a_observation;
  setFactValue(
    "compare-date",
    Boolean(latest?.date),
    latest?.date || "",
    "Market A has no source-backed UTC observation in this window.",
    { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_a), factLabel: "latest Market A UTC date" },
  );
  [
    ["compare-a-return", payload.market_a_statistics?.window_return, "Market A window return requires at least two valid daily closes."],
    ["compare-a-volatility", payload.market_a_statistics?.daily_volatility, "Market A daily volatility requires sufficient valid return observations."],
  ].forEach(([id, value, reason]) => setFactValue(
    id,
    finite(value),
    formatPercent(value),
    reason,
    {
      token: payload.token_symbol,
      marketLabel: snapshotMarketContextLabel(payload.market_a),
      factLabel: id.includes("return") ? "window return" : "daily volatility",
    },
  ));
  byId("market-a-price-heading").textContent = `${payload.market_a.venue} Price (USD)`;
  byId("market-a-volume-heading").textContent = `${payload.market_a.venue} Volume (USD)`;
  renderComparisonChart(payload);
  const rows = [...(payload.observations || [])].reverse();
  byId("comparison-body").innerHTML = rows.length
    ? rows.map((row) => `<tr>
        <td>${escapeHtml(row.date)}</td>
        <td>${comparisonValueMarkup(row.market_a?.price_usd, formatRawUsd, "Market A has no valid daily USD price on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_a), factLabel: `daily USD price on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.market_a?.volume_usd, formatRawVolume, "Market A has no valid daily USD volume on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_a), factLabel: `daily USD volume on ${row.date}` })}</td>
      </tr>`).join("")
    : '<tr><td colspan="3" class="missing">No observations in this window.</td></tr>';
  hideError(byId("comparison-error"));
  showStatus(
    byId("comparison-status"),
    `${payload.token_symbol} Market A current · ${payload.metadata?.union_observation_days || 0} observation days.`,
    "success",
  );
  byId("facts-workbench").setAttribute("aria-busy", "false");
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

function renderComparison(payload) {
  if (payload?.selection_mode === "single") {
    renderSingleComparison(payload);
    return;
  }
  app.comparison = payload;
  applyWorkspaceSelectionMode("pair");
  setComparisonDocumentCopy(false);
  const latest = payload.latest_comparable_observation;
  setFactValue(
    "compare-date",
    Boolean(latest?.date),
    latest?.date || "",
    "No UTC date in this window has valid prices for both selected markets.",
    { token: payload.token_symbol, factLabel: "latest comparable UTC date" },
  );
  setFactValue(
    "compare-absolute",
    finite(latest?.absolute_spread_usd),
    formatRawUsd(latest?.absolute_spread_usd),
    "Absolute price difference requires valid Market A and B prices on the same UTC date.",
    { token: payload.token_symbol, factLabel: "absolute price difference" },
  );
  setFactValue(
    "compare-bps",
    finite(latest?.spread_bps),
    finite(latest?.spread_bps) ? `${bpsFormat.format(latest.spread_bps)} bps` : "",
    "Daily Price Gap requires valid Market A and B closing prices on the same UTC date.",
    { token: payload.token_symbol, factLabel: "Daily Price Gap" },
  );
  byId("compare-days").textContent = `${payload.metadata.comparison_days} / ${payload.metadata.union_observation_days}`;
  [
    ["compare-a-return", payload.market_a_statistics?.window_return, "Market A window return requires at least two valid daily closes."],
    ["compare-b-return", payload.market_b_statistics?.window_return, "Market B window return requires at least two valid daily closes."],
    ["compare-a-volatility", payload.market_a_statistics?.daily_volatility, "Market A daily volatility requires sufficient valid return observations."],
    ["compare-b-volatility", payload.market_b_statistics?.daily_volatility, "Market B daily volatility requires sufficient valid return observations."],
  ].forEach(([id, value, reason]) => setFactValue(
    id,
    finite(value),
    formatPercent(value),
    reason,
    {
      token: payload.token_symbol,
      marketLabel: id.includes("-a-")
        ? snapshotMarketContextLabel(payload.market_a)
        : snapshotMarketContextLabel(payload.market_b),
      factLabel: id.includes("return") ? "window return" : "daily volatility",
    },
  ));
  byId("market-a-price-heading").textContent = `${payload.market_a.venue} Price (USD)`;
  byId("market-a-volume-heading").textContent = `${payload.market_a.venue} Volume (USD)`;
  byId("market-b-price-heading").textContent = `${payload.market_b.venue} Price (USD)`;
  byId("market-b-volume-heading").textContent = `${payload.market_b.venue} Volume (USD)`;
  renderComparisonChart(payload);
  const missingLabels = {
    market_a_missing: "A missing · no fill",
    market_b_missing: "B missing · no fill",
    non_comparable_price: "Price null/invalid",
  };
  const rows = [...payload.observations].reverse();
  byId("comparison-body").innerHTML = rows.length
    ? rows.map((row) => `<tr>
        <td>${escapeHtml(row.date)}</td>
        <td>${comparisonValueMarkup(row.market_a.price_usd, formatRawUsd, "Market A has no valid daily USD price on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_a), factLabel: `daily USD price on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.market_a.volume_usd, formatRawVolume, "Market A has no valid daily USD volume on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_a), factLabel: `daily USD volume on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.market_b.price_usd, formatRawUsd, "Market B has no valid daily USD price on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_b), factLabel: `daily USD price on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.market_b.volume_usd, formatRawVolume, "Market B has no valid daily USD volume on this UTC date.", { token: payload.token_symbol, marketLabel: snapshotMarketContextLabel(payload.market_b), factLabel: `daily USD volume on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.absolute_spread_usd, formatRawUsd, "Absolute Daily Price Gap requires valid Market A and B closing prices on the same UTC date.", { token: payload.token_symbol, factLabel: `absolute Daily Price Gap on ${row.date}` })}</td>
        <td>${comparisonValueMarkup(row.spread_bps, (value) => bpsFormat.format(value), "Daily Price Gap requires valid Market A and B closing prices on the same UTC date.", { token: payload.token_symbol, factLabel: `Daily Price Gap on ${row.date}` })}</td>
        <td class="${row.missing_reason ? "missing" : ""}">${escapeHtml(missingLabels[row.missing_reason] || "Comparable")}</td>
      </tr>`).join("")
    : '<tr><td colspan="8" class="missing">No observations in this window.</td></tr>';
  hideError(byId("comparison-error"));
  showStatus(
    byId("comparison-status"),
    `${payload.token_symbol} comparison current · ${payload.metadata.comparison_days} comparable days.`,
    "success",
  );
  byId("facts-workbench").setAttribute("aria-busy", "false");
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

async function responseJson(response) {
  try {
    return await response.json();
  } catch {
    throw new Error(`Server returned ${response.status} without a valid JSON body.`);
  }
}

function publicErrorMessage(error, fallback = "Request failed.") {
  const rawMessage = typeof error === "string"
    ? error
    : error?.message || "";
  const normalized = String(rawMessage).trim();
  if (!normalized) return fallback;
  const checkedSuffix = normalized.search(/\bChecked:\s*/i);
  const message = checkedSuffix >= 0
    ? normalized.slice(0, checkedSuffix).trim()
    : normalized;
  return message || fallback;
}

function comparisonPayloadMatchesSelection(payload, token, selection) {
  if (
    payload?.token_symbol !== token
    || payload?.market_a?.market_id !== selection.marketA
  ) return false;
  if (selection.selection === "single") {
    return payload.selection_mode === "single" && payload.market_b === null;
  }
  return (
    payload.selection_mode == null
    && payload?.market_b?.market_id === selection.marketB
  );
}

function comparisonRequestIsCurrent(requestId, requestKey, selection) {
  return (
    requestId === app.comparisonRequestId
    && requestKey === app.comparisonRequestKey
    && app.route?.page === "compare"
    && requestKey === workspaceRequestKey("compare", selection)
  );
}

async function loadComparison() {
  const window = appliedTimeWindow();
  const requestId = invalidateComparisonRequest();
  if (!app.catalog) {
    clearComparisonResult("Market catalog is unavailable.");
    return false;
  }
  const dateError = validateDateRange(window.start, window.end);
  if (dateError) {
    clearComparisonResult(dateError);
    return false;
  }
  const token = byId("facts-token").value;
  const marketA = byId("facts-market-a").value;
  const marketB = byId("facts-market-b").value;
  const selection = {
    marketA,
    marketB,
    selection: app.workspaceSelection === "single" ? "single" : "",
  };
  const single = selection.selection === "single";
  if (!token || !marketA || (single ? Boolean(marketB) : !marketB || marketA === marketB)) {
    clearComparisonResult(
      token
        ? single
          ? "Select Market A and leave Market B empty for the single-market view."
          : "This Token does not currently have two distinct market series to compare."
        : "Select a Token and valid market selection.",
    );
    return false;
  }
  const requestKey = workspaceRequestKey("compare", selection);
  app.comparisonRequestKey = requestKey;
  const controller = new AbortController();
  app.comparisonController = controller;
  const query = new URLSearchParams({ token, market_a: marketA });
  if (single) query.set("selection", "single");
  else query.set("market_b", marketB);
  if (window.start) query.set("start", window.start);
  if (window.end) query.set("end", window.end);
  setComparisonLoading(single ? `Loading ${token} Market A…` : `Loading ${token} comparison…`);
  try {
    const eventPromise = fetchEventFacts({
      token,
      start: window.start,
      end: window.end,
      signal: controller.signal,
    }).catch(() => null);
    const response = await fetch(`/api/markets/compare?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Comparison failed to load.");
    if (!comparisonRequestIsCurrent(requestId, requestKey, selection)) return false;
    if (!comparisonPayloadMatchesSelection(payload, token, selection)) {
      throw new Error("The comparison response failed its Token, market, or selection contract.");
    }
    renderComparison(payload);
    const eventPayload = await eventPromise;
    if (!comparisonRequestIsCurrent(requestId, requestKey, selection)) return false;
    app.eventFacts = eventPayload;
    renderComparisonChart(payload);
    return true;
  } catch (error) {
    if (
      error.name === "AbortError"
      || !comparisonRequestIsCurrent(requestId, requestKey, selection)
    ) return false;
    clearComparisonResult(publicErrorMessage(error, "Comparison failed to load."));
    return false;
  } finally {
    if (comparisonRequestIsCurrent(requestId, requestKey, selection)) {
      app.comparisonController = null;
      byId("compare-markets").disabled = false;
    }
  }
}

async function loadTokenCatalog(token, start, end, signal, cacheKey) {
  const query = new URLSearchParams({ token });
  if (start) query.set("start", start);
  if (end) query.set("end", end);
  const response = await fetch(`/api/markets/catalog?${query.toString()}`, { signal });
  const payload = await responseJson(response);
  if (!response.ok) throw new Error(payload.error || `${token} market catalog failed to load.`);
  if (
    payload?.token_symbol !== token
    || !Array.isArray(payload?.markets)
    || payload.markets.some((market) => market.token_symbol !== token)
  ) {
    throw new Error(`The ${token} catalog response failed its Token-scope contract.`);
  }
  const summaryGeneration = app.payload?.metadata?.data_generation;
  const catalogGeneration = payload.metadata?.data_generation;
  if (!summaryGeneration || !catalogGeneration) {
    throw new Error("The summary/catalog generation contract is missing.");
  }
  if (summaryGeneration !== catalogGeneration) {
    const error = new Error(
      "The data generation changed during navigation. Refreshing the summary.",
    );
    error.code = "data_generation_mismatch";
    throw error;
  }
  if (
    payload.metadata?.window_start !== start
    || payload.metadata?.window_end !== end
  ) {
    throw new Error(`The ${token} catalog returned the wrong daily window.`);
  }
  return payload;
}

function isMarketPayload(payload) {
  return Boolean(
    payload
    && payload.metadata
    && payload.metadata.response_scope === "screener_summary"
    && payload.metadata.summary_version === 3
    && typeof payload.metadata.data_generation === "string"
    && payload.metadata.data_generation.length > 0
    && Array.isArray(payload.tokens)
    && payload.tokens.every((token) => (
      token
      && typeof token.token_symbol === "string"
      && Object.hasOwn(token, "absolute_price_gap")
      && (
        token.absolute_price_gap === null
        || (finite(token.absolute_price_gap) && token.absolute_price_gap >= 0)
      )
      && token.absolute_price_gap_method === "symmetric_midpoint_relative_gap"
      && Object.hasOwn(token, "primary_cex")
      && Object.hasOwn(token, "primary_dex")
    ))
  );
}

function cacheSafeMarketPayload(payload) {
  const metadata = { ...(payload?.metadata || {}) };
  delete metadata.public_actions;
  return { ...payload, metadata };
}

function readDefaultMarketCache() {
  try {
    const payload = JSON.parse(window.localStorage.getItem(DEFAULT_MARKET_CACHE_KEY));
    return isMarketPayload(payload) ? cacheSafeMarketPayload(payload) : null;
  } catch {
    return null;
  }
}

function writeDefaultMarketCache(payload) {
  try {
    window.localStorage.setItem(
      DEFAULT_MARKET_CACHE_KEY,
      JSON.stringify(cacheSafeMarketPayload(payload)),
    );
  } catch {
    // A fresh network response still renders when browser storage is unavailable.
  }
}

function clearDefaultMarketCache() {
  app.defaultPayload = null;
  app.defaultPayloadIsCached = false;
  try {
    window.localStorage.removeItem(DEFAULT_MARKET_CACHE_KEY);
  } catch {
    // In-memory invalidation is authoritative when browser storage is unavailable.
  }
}

function displayMarket(
  payload,
  { cached = false, refreshWorkspaceOnGenerationChange = true } = {},
) {
  const previousGeneration = app.payload?.metadata?.data_generation;
  const nextGeneration = payload.metadata?.data_generation;
  const generationChanged = Boolean(
    previousGeneration
    && nextGeneration
    && previousGeneration !== nextGeneration
  );
  if (generationChanged) {
    app.catalogsByToken.clear();
    app.catalog = null;
    app.activeCatalogToken = "";
    app.activeCatalogKey = "";
    if (
      app.defaultPayload
      && app.defaultPayload.metadata?.data_generation !== nextGeneration
    ) {
      clearDefaultMarketCache();
    }
  }
  app.payload = payload;
  if (isDefaultMarketPayload(payload)) {
    app.defaultPayload = payload;
    app.defaultPayloadIsCached = cached;
  }
  hideError(byId("error-banner"));
  hideError(byId("global-error"));
  const currentToken = byId("facts-token").value;
  const tokens = payload.tokens.map((token) => token.token_symbol);
  byId("facts-token").innerHTML = tokens
    .map((token) => `<option value="${escapeHtml(token)}">${escapeHtml(token)}</option>`)
    .join("");
  byId("facts-token").value = tokens.includes(currentToken)
    ? currentToken
    : payload.metadata.default_workspace_token || tokens[0] || "";
  updateMetadata();
  renderTable();
  byId("market-panel").setAttribute("aria-busy", "false");
  byId("export-csv").disabled = false;
  if (cached) {
    showStatus(
      byId("market-status"),
      `Cached facts through ${payload.metadata.available_end} are visible while a fresh snapshot loads.`,
      "stale",
    );
    byId("freshness").textContent = `Cached through ${payload.metadata.available_end} · refreshing`;
  } else {
    showStatus(
      byId("market-status"),
      `Facts current for ${payload.metadata.start_date} through ${payload.metadata.end_date}.`,
      "success",
    );
  }
  if (
    app.catalog
    && app.activeCatalogToken === selectedWorkspaceToken()
    && app.catalog.metadata?.window_start === payload.metadata.start_date
    && app.catalog.metadata?.window_end === payload.metadata.end_date
  ) {
    populateFactsMarkets({ preserve: true });
  }
  if (
    refreshWorkspaceOnGenerationChange
    && generationChanged
    && app.routeReady
    && app.route?.kind === "workspace"
  ) {
    void applyRouteFromLocation();
  }
}

function setMarketLoading(message, preserve) {
  setDateWindowDisabled(true);
  byId("export-csv").disabled = true;
  byId("market-panel").setAttribute("aria-busy", "true");
  hideError(byId("error-banner"));
  showStatus(byId("market-loading"), message, preserve ? "stale" : "");
  if (!preserve) {
    byId("freshness").textContent = "Loading fact summary on demand";
    byId("freshness-cluster").dataset.status = "loading";
    app.payload = null;
    app.visibleTokens = [];
    hideStatus(byId("market-status"));
    byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="10" class="missing">Loading the requested time window…</td></tr>';
    byId("row-count").textContent = "Loading…";
  }
}

function invalidateMarketRequest() {
  if (app.marketController) app.marketController.abort();
  app.marketController = null;
  app.marketRequestWindowKey = "";
  app.marketRequestId += 1;
  return app.marketRequestId;
}

function clearMarketResult(message = "") {
  app.payload = null;
  app.visibleTokens = [];
  byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="10" class="missing">No current market result.</td></tr>';
  byId("row-count").textContent = "No current result";
  hideStatus(byId("market-loading"));
  hideStatus(byId("market-status"));
  if (message) showError(byId("error-banner"), message);
  else hideError(byId("error-banner"));
  byId("market-panel").setAttribute("aria-busy", "false");
  setDateWindowDisabled(false);
  byId("export-csv").disabled = true;
}

async function loadMarket(
  start = "",
  end = "",
  {
    preserve = false,
    refreshWorkspaceOnGenerationChange = true,
    onRequestStart = null,
    responseIsOwned = null,
  } = {},
) {
  const requestId = invalidateMarketRequest();
  if (onRequestStart) onRequestStart(requestId);
  const dateError = validateDateRange(start, end);
  if (dateError) {
    clearMarketResult(dateError);
    return false;
  }
  const requestWindowKey = marketWindowKey(start, end);
  app.marketRequestWindowKey = requestWindowKey;
  const controller = new AbortController();
  app.marketController = controller;
  const query = new URLSearchParams();
  if (start) query.set("start", start);
  if (end) query.set("end", end);
  setMarketLoading(
    preserve
      ? "Refreshing source-backed facts; the cached snapshot is explicitly marked until replacement."
      : "Loading source-backed facts for the requested time window…",
    preserve,
  );
  try {
    const response = await fetch(`/api/markets/summary?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Screener summary failed to load.");
    if (!isMarketPayload(payload)) {
      throw new Error("Screener summary failed its compact response contract.");
    }
    if (requestId !== app.marketRequestId) return false;
    if (responseIsOwned && !responseIsOwned()) return false;
    displayMarket(payload, { refreshWorkspaceOnGenerationChange });
    if (!start && !end) writeDefaultMarketCache(payload);
    hideStatus(byId("market-loading"));
    return true;
  } catch (error) {
    if (error.name === "AbortError" || requestId !== app.marketRequestId) return false;
    hideStatus(byId("market-loading"));
    byId("market-panel").setAttribute("aria-busy", "false");
    const failure = publicErrorMessage(error, "Screener summary failed to load.");
    const retained = preserve && app.payload
      ? " The explicitly marked cached snapshot remains visible."
      : " No result is shown for the failed request.";
    if (preserve && app.payload) {
      byId("export-csv").disabled = false;
      showStatus(
        byId("market-status"),
        "Cached facts remain visible; the fresh request failed.",
        "stale",
      );
      showError(byId("error-banner"), `${failure}${retained}`);
    } else {
      clearMarketResult(`${failure}${retained}`);
    }
    if (app.route?.kind !== "screener") {
      showError(byId("global-error"), `${failure}${retained}`);
    }
    return false;
  } finally {
    if (requestId === app.marketRequestId) {
      app.marketController = null;
      app.marketRequestWindowKey = "";
      setDateWindowDisabled(false);
    }
  }
}

function setCustomWindowOpen(open, { restoreFocus = false } = {}) {
  const editor = byId("custom-window-editor");
  const toggle = byId("custom-window-toggle");
  editor.hidden = !open;
  toggle.setAttribute("aria-expanded", String(open));
  if (open) byId("date-start").focus();
  else if (restoreFocus) toggle.focus();
}

function openCustomWindowEditor() {
  setDraftTimeWindow(appliedTimeWindow());
  showDateWindowError("");
  setCustomWindowOpen(true);
}

function cancelCustomWindowEditor() {
  setDraftTimeWindow(appliedTimeWindow());
  showDateWindowError("");
  setCustomWindowOpen(false, { restoreFocus: true });
}

async function applyWindow(candidate = draftTimeWindow()) {
  const { start, end } = candidate;
  const dateError = validateDateRange(start, end, { required: true });
  if (dateError) {
    showDateWindowError(dateError);
    return false;
  }
  showDateWindowError("");
  invalidateSnapshotRefreshRequest({ clearFeedback: true });
  const routeAtApply = app.route.kind === "workspace"
    ? { ...app.route, state: { ...(app.route.state || {}) } }
    : app.route;
  const appliedLocation = `${window.location.pathname}${window.location.search}`;
  const loading = loadMarket(start, end, {
    preserve: Boolean(app.payload),
    refreshWorkspaceOnGenerationChange: false,
  });
  const applyRequestId = app.marketRequestId;
  const loaded = await loading;
  if (!loaded) {
    const locationUnchanged = (
      `${window.location.pathname}${window.location.search}` === appliedLocation
    );
    if (
      routeAtApply.kind === "workspace"
      && applyRequestId === app.marketRequestId
      && locationUnchanged
    ) {
      void applyRouteFromLocation({ preserveWorkspaceError: true });
    }
    return false;
  }
  const applied = appliedTimeWindow();
  if (routeAtApply.kind === "workspace" && navigation) {
    const latestRoute = navigation.parseRoute(
      window.location.pathname,
      window.location.search,
    );
    const routeForCommit = (
      latestRoute.kind === "workspace"
      && String(latestRoute.token || "").toUpperCase()
        === String(routeAtApply.token || "").toUpperCase()
      && latestRoute.page === routeAtApply.page
    )
      ? latestRoute
      : routeAtApply;
    const state = {
      ...(routeForCommit.state || {}),
      start: applied.start,
      end: applied.end,
    };
    const path = navigation.buildWorkspacePath(
      routeForCommit.token,
      routeForCommit.page,
      state,
    );
    window.history.replaceState({}, "", path);
    app.route = navigation.parseRoute(window.location.pathname, window.location.search);
    updateRouteLinks();
    void applyRouteFromLocation();
  } else {
    replaceCurrentRoute({ window: applied, allowBeforeReady: true });
    if (routeAtApply.kind === "workspace") void applyRouteFromLocation();
  }
  return true;
}

function persistSelectedSelection() {
  const token = selectedWorkspaceToken();
  const selection = selectedMarketSelection();
  const markets = app.catalog && token ? factsMarketsForToken(token) : [];
  const validation = validateWorkspaceSelection(
    markets,
    selection.marketA,
    selection.marketB,
    selection.selection,
  );
  if (token && validation?.valid) {
    app.pairSelectionSource = "";
    app.workspaceSelectionInvalid = false;
    app.pairSelections[token] = {
      marketA: validation.marketA.market_id,
      marketB: validation.marketB?.market_id || "",
      ...(validation.mode === "single" ? { selection: "single" } : {}),
    };
    writePairSelections();
    return true;
  }
  return false;
}

function persistSelectedPair() {
  return persistSelectedSelection();
}

function clearScreenerQualityDrilldown({ pairComplete = false } = {}) {
  app.qualityOrigin = "";
  app.qualitySeverity = "";
  if (app.route?.kind === "workspace" && app.route.page === "quality") {
    app.qualityScope = pairComplete ? "selected" : "all";
  }
}

function applySelectedSelection() {
  const draft = selectedMarketSelection();
  if (
    draft.marketA
    && !draft.marketB
    && !app.workspaceSelectionInvalid
    && ["", "single"].includes(app.workspaceSelection)
  ) {
    app.workspaceSelection = "single";
  }
  if (!persistSelectedSelection()) {
    const attempted = selectedMarketSelection();
    const validation = validateWorkspaceSelection(
      app.catalog ? factsMarketsForToken(selectedWorkspaceToken()) : [],
      attempted.marketA,
      attempted.marketB,
      app.workspaceSelection,
    );
    const codes = validation?.errors?.map((error) => error.code).join(", ") || "selection_invalid";
    showStatus(
      byId("workspace-context-notice"),
      `The market selection is invalid (${codes}). The last valid saved selection was kept.`,
      "stale",
    );
    return false;
  }
  navigateTo(currentWorkspacePath("compare"));
  return true;
}

function refreshWorkspacePageData() {
  renderFactsMarketWarnings();
  renderWorkspaceContext();
  renderWorkspaceMarkets();
  renderQualityFromCatalog();
  renderLiquidityCurve();
  if (app.route?.kind !== "workspace") return;
  if (app.route.page === "compare") loadComparison();
  if (app.route.page === "liquidity") loadExecutionCost();
  if (app.route.page === "events") loadEvents();
  if (app.route.page === "quality") loadQuality();
}

function selectWorkspaceMarket(slot, marketIdValue) {
  const target = byId(`facts-market-${slot}`);
  const otherSlot = slot === "a" ? "b" : "a";
  const other = byId(`facts-market-${otherSlot}`);
  target.value = marketIdValue;
  app.workspaceSelectionInvalid = false;
  if (slot === "b") {
    app.workspaceSelection = marketIdValue ? "" : "single";
  }
  if (marketIdValue && other.value === marketIdValue) {
    other.value = "";
    app.workspaceSelection = "";
    showStatus(
      byId("workspace-context-notice"),
      `Market ${otherSlot.toUpperCase()} was cleared because A and B must be different. Choose another market explicitly.`,
      "stale",
    );
  } else if (persistSelectedSelection()) {
    hideStatus(byId("workspace-context-notice"));
  }
  const pair = selectedPairState();
  clearScreenerQualityDrilldown({
    pairComplete: Boolean(
      pair.marketA && pair.marketB && pair.marketA !== pair.marketB
    ),
  });
  persistSelectedSelection();
  replaceCurrentRoute();
  refreshWorkspacePageData();
}

function selectWorkspaceToken(newToken) {
  const previousToken = app.route?.kind === "workspace" ? app.route.token : "";
  if (!newToken || !navigation) return false;
  clearScreenerQualityDrilldown({ pairComplete: false });
  const page = app.route?.kind === "workspace" ? app.route.page : "markets";
  const saved = normalizedSavedSelection(app.pairSelections[newToken]);
  const state = saved
    ? {
      ...currentWorkspaceRouteState(page),
      marketA: saved.marketA,
      marketB: saved.marketB,
      selection: saved.selection,
    }
    : workspaceStateWithoutMarkets(page);
  delete state.pairMode;
  if (!saved) state.pairMode = "manual";
  navigateTo(navigation.buildWorkspacePath(
    newToken,
    page,
    state,
  ));
  showStatus(
    byId("workspace-context-notice"),
    previousToken
      ? `Token changed from ${previousToken} to ${newToken}. ${saved ? "The saved market selection was restored." : "Choose its markets."}`
      : `Choose the ${newToken} market selection.`,
    "stale",
  );
  return true;
}

function workspaceStateWithoutMarkets(page) {
  const state = currentWorkspaceRouteState(page);
  delete state.marketA;
  delete state.marketB;
  delete state.selection;
  state.pairMode = "manual";
  return state;
}

function csvEscape(value) {
  const text = value === null || value === undefined ? "" : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}

function exportVisibleCsv() {
  const window = appliedTimeWindow();
  if (!app.payload || !app.visibleTokens.length) return;
  const headers = [
    "token",
    "rank_metric",
    "rank_scope",
    "rank_direction",
    "rank_value",
    "rank_eligible",
    "row_type",
    "venue",
    "instrument",
    "first_date",
    "latest_date",
    "observation_days",
    "coverage_ratio",
    "price_usd",
    "window_return",
    "daily_volatility",
    "volume_usd",
    "tvl_usd",
    "depth_10bps_usd",
    "depth_25bps_usd",
    "depth_50bps_usd",
    "depth_100bps_usd",
    "depth_status",
    "quality_flags",
    "selected_dex_cex_spread",
  ];
  const lines = [headers.map(csvEscape).join(",")];
  app.visibleTokens.forEach((tokenSummary) => {
    const token = tokenSummary.token_symbol;
    const aggregates = aggregateFacts(tokenSummary, [], []);
    const selected = comparison(tokenSummary);
    const rankValue = sortValue(tokenSummary);
    const rankFields = [
      byId("sort-field").value,
      app.scope,
      app.sortDirection,
      finite(rankValue) ? rankValue : "",
      finite(rankValue) ? "true" : "false",
    ];
    lines.push([
      token,
      ...rankFields,
      "aggregate",
      "all",
      "all cataloged markets",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      aggregates.aggregateTotal,
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      selected.spread,
    ].map(csvEscape).join(","));
    [["cex", selected.cex], ["dex", selected.dex]].forEach(([market, row]) => {
      if (!row) return;
      const flags = qualityFlagObjects(row, market).map((flag) => flag.code).join("|");
      lines.push([
        token,
        ...rankFields,
        `selected_${market}`,
        row.venue,
        row.instrument,
        row.first_observed_date || row.first_date || row.price_points?.[0]?.date || "",
        row.latest_observed_date || row.latest_date,
        row.observation_days,
        row.coverage_ratio ?? row.observation_coverage_ratio ?? "",
        row.price_usd,
        row.window_return,
        row.daily_volatility,
        row.volume_usd,
        row.tvl_usd,
        row.total_depth_10bps_usd,
        row.total_depth_25bps_usd,
        row.total_depth_50bps_usd,
        row.total_depth_100bps_usd,
        row.depth_status,
        flags,
        selected.spread,
      ].map(csvEscape).join(","));
    });
  });
  const blob = new Blob([`${lines.join("\n")}\n`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `cex-dex-market-facts-${window.start}-${window.end}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  showStatus(byId("market-status"), `Exported ${app.visibleTokens.length} visible Tokens as CSV.`, "success");
}

function bindOpportunityFilterEvents() {
  byId("opportunity-filter-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const filters = opportunityFiltersFromControls();
    const validation = navigation.validateOpportunityFilters(filters);
    if (!setOpportunityFilterValidation(validation.errors)) return;
    const path = navigation.buildOpportunitiesPath(filters);
    navigateTo(path);
  });
  byId("opportunity-sort").addEventListener("change", () => {
    byId("opportunity-direction").value = defaultOpportunityDirection(
      byId("opportunity-sort").value,
    );
  });
  ["opportunity-token", "opportunity-venue"].forEach((id) => {
    byId(id).addEventListener("input", () => setOpportunityFilterValidation([]));
  });
}

function bindEvents() {
  const applyTokenSearch = () => {
    app.searchQuery = byId("token-search").value.trim().toUpperCase();
    renderTable();
    replaceCurrentRoute();
  };
  bindOpportunityFilterEvents();
  byId("date-window-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const applied = await applyWindow(draftTimeWindow());
    if (applied) {
      setDraftTimeWindow(appliedTimeWindow());
      syncTimeWindowControls();
      setCustomWindowOpen(false, { restoreFocus: true });
    }
  });
  byId("custom-window-toggle").addEventListener("click", () => {
    if (byId("custom-window-toggle").getAttribute("aria-expanded") === "true") {
      cancelCustomWindowEditor();
    } else {
      openCustomWindowEditor();
    }
  });
  byId("cancel-window").addEventListener("click", cancelCustomWindowEditor);
  ["date-start", "date-end"].forEach((id) => {
    byId(id).addEventListener("input", () => showDateWindowError(""));
  });
  document.querySelectorAll("[data-days]").forEach((button) => {
    button.addEventListener("click", async () => {
      const applied = await applyWindow(presetWindow(button.dataset.days));
      if (applied) {
        setDraftTimeWindow(appliedTimeWindow());
        syncTimeWindowControls();
        setCustomWindowOpen(false);
      }
    });
  });
  document.querySelectorAll("[data-scope]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.disabled) return;
      app.scope = button.dataset.scope;
      syncScreenerSortControls();
      renderTable();
      replaceCurrentRoute();
    });
  });
  byId("sort-field").addEventListener("change", () => {
    syncScreenerSortControls();
    renderTable();
    replaceCurrentRoute();
  });
  byId("sort-direction").addEventListener("change", () => {
    app.sortDirection = byId("sort-direction").value === "asc" ? "asc" : "desc";
    syncScreenerSortControls();
    renderTable();
    replaceCurrentRoute();
  });
  byId("search-token").addEventListener("click", applyTokenSearch);
  byId("token-search").addEventListener("keydown", (event) => {
    if (event.key === "Enter") applyTokenSearch();
  });
  byId("token-search").addEventListener("search", applyTokenSearch);
  byId("facts-token").addEventListener("change", () => {
    selectWorkspaceToken(byId("facts-token").value);
  });
  byId("facts-market-a").addEventListener("change", () => {
    selectWorkspaceMarket("a", byId("facts-market-a").value);
  });
  byId("facts-market-b").addEventListener("change", () => {
    selectWorkspaceMarket("b", byId("facts-market-b").value);
  });
  document.querySelectorAll("[data-workspace-market-type]").forEach((button) => {
    button.addEventListener("click", () => {
      app.workspaceMarketType = button.dataset.workspaceMarketType;
      syncSegmentedControls();
      renderWorkspaceMarkets();
    });
  });
  document.querySelectorAll("[data-liquidity-view]").forEach((button) => {
    button.addEventListener("click", () => {
      app.liquidityView = button.dataset.liquidityView;
      syncSegmentedControls();
      renderLiquidityCurve();
      replaceCurrentRoute();
    });
  });
  document.querySelectorAll("[data-liquidity-scale]").forEach((button) => {
    button.addEventListener("click", () => {
      app.liquidityScale = button.dataset.liquidityScale;
      syncSegmentedControls();
      renderLiquidityCurve();
      replaceCurrentRoute();
    });
  });
  document.querySelectorAll("[data-comparison-metric]").forEach((button) => {
    button.addEventListener("click", () => {
      app.comparisonMetric = comparisonMetricDefinition(
        button.dataset.comparisonMetric,
      ).key;
      syncSegmentedControls();
      if (app.comparison) {
        renderComparisonChart(app.comparison);
      } else {
        const definition = comparisonMetricDefinition(app.comparisonMetric);
        byId("comparison-chart-title").textContent = definition.title;
        byId("comparison-chart-note").textContent = definition.note;
        hideComparisonChartTooltip();
      }
    });
  });
  document.querySelectorAll("[data-event-lifecycle]").forEach((button) => {
    button.addEventListener("click", () => {
      app.eventLifecycle = button.dataset.eventLifecycle || "all";
      syncSegmentedControls();
      replaceCurrentRoute();
      loadEvents();
    });
  });
  document.querySelectorAll("[data-event-clock-state]").forEach((button) => {
    button.addEventListener("click", () => {
      app.eventClockState = button.dataset.eventClockState || "all";
      syncSegmentedControls();
      replaceCurrentRoute();
      loadEvents();
    });
  });
  document.querySelectorAll("[data-execution-direction]").forEach((button) => {
    button.addEventListener("click", () => {
      app.executionDirection = button.dataset.executionDirection;
      syncSegmentedControls();
      if (app.execution) renderExecution(app.execution);
      else loadExecutionCost();
      replaceCurrentRoute();
    });
  });
  byId("execution-notional").addEventListener("change", () => {
    app.executionNotionalUsd = Number(byId("execution-notional").value);
    if (app.execution) renderExecution(app.execution);
    else loadExecutionCost();
    replaceCurrentRoute();
  });
  document.querySelectorAll("[data-quality-scope]").forEach((button) => {
    button.addEventListener("click", () => {
      app.qualityScope = button.dataset.qualityScope;
      app.qualitySeverity = "";
      app.qualityOrigin = "";
      syncSegmentedControls();
      renderQualityFromCatalog();
      replaceCurrentRoute();
      loadQuality();
    });
  });
  bindLiquidityTooltipEvents();
  bindComparisonChartTooltipEvents();
  bindFactsMarketWarningEvents();
  const scheduleLiquidityResize = () => {
    if (app.liquidityResizeScheduled) return;
    app.liquidityResizeScheduled = true;
    window.queueMicrotask(() => {
      app.liquidityResizeScheduled = false;
      if (app.catalog) renderLiquidityCurve();
    });
  };
  window.addEventListener("resize", scheduleLiquidityResize);
  window.visualViewport?.addEventListener("resize", scheduleLiquidityResize);
  window.matchMedia("(max-width: 700px)")
    .addEventListener("change", scheduleLiquidityResize);
  if (window.ResizeObserver) {
    app.liquidityResizeObserver = new ResizeObserver(scheduleLiquidityResize);
    app.liquidityResizeObserver.observe(byId("liquidity-plot"));
  }
  const scheduleComparisonChartResize = () => {
    if (app.comparisonChartResizeScheduled) return;
    app.comparisonChartResizeScheduled = true;
    window.queueMicrotask(() => {
      app.comparisonChartResizeScheduled = false;
      if (app.comparison && app.route?.page === "compare") {
        renderComparisonChart(app.comparison);
      }
    });
  };
  window.addEventListener("resize", scheduleComparisonChartResize);
  window.visualViewport?.addEventListener("resize", scheduleComparisonChartResize);
  window.matchMedia("(max-width: 700px)")
    .addEventListener("change", scheduleComparisonChartResize);
  if (window.ResizeObserver) {
    app.comparisonChartResizeObserver = new ResizeObserver(
      scheduleComparisonChartResize,
    );
    app.comparisonChartResizeObserver.observe(byId("comparison-plot"));
  }
  byId("compare-markets").addEventListener("click", applySelectedSelection);
  byId("export-csv").addEventListener("click", exportVisibleCsv);
  document.addEventListener("click", (event) => {
    const refreshButton = event.target.closest?.("[data-refresh-fact]");
    if (refreshButton) {
      event.preventDefault();
      void requestSnapshotFactRefresh(refreshButton);
      return;
    }
    const pairButton = event.target.closest?.("[data-set-market-slot]");
    if (pairButton) {
      selectWorkspaceMarket(
        pairButton.dataset.setMarketSlot,
        pairButton.dataset.marketId,
      );
      return;
    }
    const link = event.target.closest?.("a[href]");
    if (
      !link
      || event.defaultPrevented
      || event.button !== 0
      || event.metaKey
      || event.ctrlKey
      || event.shiftKey
      || event.altKey
      || link.target === "_blank"
      || link.hasAttribute("download")
    ) {
      return;
    }
    const url = new URL(link.href, window.location.href);
    if (url.origin !== window.location.origin) return;
    const parsed = navigation?.parseRoute(url.pathname, url.search);
    if (!parsed || parsed.kind === "unknown") return;
    event.preventDefault();
    navigateTo(`${url.pathname}${url.search}${url.hash}`);
  });
  window.addEventListener("popstate", () => {
    invalidateSnapshotRefreshRequest({ clearFeedback: true });
    void applyRouteFromLocation();
  });
}

function primeInitialRouteView(route) {
  if (route.kind === "workspace") {
    const window = compareRouteWindow(route);
    setDraftTimeWindow(window);
    setActiveAppView("workspace");
    setActiveWorkspacePage(route.page);
    updateRouteLinks();
    return;
  }
  if (route.kind === "screener") {
    hydrateScreenerControls(route, { normalizeWindow: false });
  }
  if (route.kind === "opportunities") {
    hydrateOpportunityControls(route);
    setActiveAppView("opportunities");
    byId("time-toolbar").hidden = true;
  } else {
    setActiveAppView("screener");
    byId("time-toolbar").hidden = false;
  }
}

async function initialize() {
  app.pairSelections = readPairSelections();
  bindEvents();
  const initialRoute = navigation
    ? navigation.parseRoute(window.location.pathname, window.location.search)
    : { kind: "unknown" };
  if (initialRoute.kind !== "unknown") app.route = initialRoute;
  primeInitialRouteView(initialRoute);
  if (initialRoute.kind === "opportunities") {
    await applyRouteFromLocation();
    if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
    return;
  }
  const initialStart = initialRoute.kind === "screener"
    ? initialRoute.filters?.start || ""
    : initialRoute.kind === "workspace"
      ? initialRoute.state?.start || ""
      : "";
  const initialEnd = initialRoute.kind === "screener"
    ? initialRoute.filters?.end || ""
    : initialRoute.kind === "workspace"
      ? initialRoute.state?.end || ""
      : "";
  const cachedPayload = readDefaultMarketCache();
  if (cachedPayload) displayMarket(cachedPayload, { cached: true });
  await loadMarket(initialStart, initialEnd, { preserve: Boolean(cachedPayload) });
  if (!app.payload) {
    byId("facts-workbench").setAttribute("aria-busy", "false");
  } else {
    if (!navigation) {
      showError(byId("error-banner"), "Navigation module failed to load.");
    } else {
      await applyRouteFromLocation();
    }
  }
  if (globalThis.window?.lucide) globalThis.window.lucide.createIcons();
}

if (typeof document !== "undefined") initialize();
