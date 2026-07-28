const app = {
  payload: null,
  defaultPayload: null,
  defaultPayloadIsCached: false,
  catalog: null,
  comparison: null,
  execution: null,
  quality: null,
  scope: "combined",
  workspaceMarketType: "all",
  qualityScope: "all",
  liquidityView: "total",
  liquidityScale: "log",
  liquidityEffectiveScale: null,
  liquidityEffectiveScaleLabel: "",
  executionDirection: "buy_token",
  executionNotionalUsd: 10000,
  route: { kind: "screener", filters: {} },
  pairSelections: {},
  pairSelectionSource: "",
  routeReady: false,
  selections: {},
  selectionOverrides: {},
  searchQuery: "",
  visibleTokens: [],
  marketRequestId: 0,
  comparisonRequestId: 0,
  executionRequestId: 0,
  qualityRequestId: 0,
  marketController: null,
  marketRequestWindowKey: "",
  comparisonController: null,
  executionController: null,
  qualityController: null,
  liquidityLayoutMode: null,
  liquidityResizeScheduled: false,
  liquidityResizeObserver: null,
};

const DEFAULT_MARKET_CACHE_KEY = "market-monitor:default-payload:v2";
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
};
const QUALITY_FLAG_DEFAULT_SEVERITIES = {
  depth_unavailable: "info",
  depth_unsupported: "warning",
  unsupported_depth: "warning",
  depth_partial: "warning",
  partial_depth: "warning",
  depth_failed: "critical",
  failed_depth: "critical",
  depth_not_cataloged: "info",
  zero_depth_10bps: "warning",
  zero_depth_inside_spread: "warning",
  tiny_pool: "warning",
  off_market_pool_state_price: "warning",
  off_market_price: "warning",
  wide_quoted_spread: "warning",
  low_daily_coverage: "warning",
  stale_snapshot: "warning",
};
const QUALITY_SEVERITY_RANK = { info: 1, warning: 2, critical: 3 };

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
  return row.market === "cex" ? `${row.venue}|${row.instrument}` : row.pool_address;
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

function selectedPairState() {
  return {
    marketA: byId("facts-market-a")?.value || "",
    marketB: byId("facts-market-b")?.value || "",
  };
}

function selectedWorkspaceToken() {
  return byId("facts-token")?.value || app.catalog?.tokens?.[0] || "";
}

function currentScreenerFilters() {
  const filters = {
    q: byId("token-search")?.value.trim() || "",
    scope: app.scope,
    sort: byId("sort-field")?.value || "volume",
    start: byId("date-start")?.value || "",
    end: byId("date-end")?.value || "",
  };
  if (filters.scope === "combined") delete filters.scope;
  if (filters.sort === "volume") delete filters.sort;
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

function currentWorkspaceRouteState(page) {
  const state = selectedPairState();
  if (!state.marketA || !state.marketB) state.pairMode = "manual";
  if (page === "compare") {
    state.start = byId("date-start")?.value || "";
    state.end = byId("date-end")?.value || "";
  } else if (page === "liquidity") {
    state.side = app.executionDirection === "sell_token" ? "sell" : "buy";
    state.notionalUsd = app.executionNotionalUsd;
    state.view = app.liquidityView;
    state.scale = app.liquidityScale;
  } else if (page === "quality") {
    state.scope = app.qualityScope;
  }
  return state;
}

function currentWorkspacePath(page = app.route?.page || "markets") {
  if (!navigation) return "/screener";
  return navigation.buildWorkspacePath(
    selectedWorkspaceToken(),
    page,
    currentWorkspaceRouteState(page),
  );
}

function updateRouteLinks() {
  const token = selectedWorkspaceToken() || "AAVE";
  const primaryWorkspace = document.querySelector('[data-app-route="workspace"]');
  if (primaryWorkspace && navigation) {
    primaryWorkspace.href = navigation.buildWorkspacePath(token, app.route?.page || "markets");
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
    const active = link.dataset.appRoute === (
      app.route?.kind === "workspace" ? "workspace" : app.route?.kind
    );
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  const back = byId("back-to-screener");
  if (back && navigation) back.href = navigation.buildScreenerPath(currentScreenerFilters());
}

function replaceCurrentRoute() {
  if (!navigation || !app.routeReady) return;
  let path;
  if (app.route.kind === "workspace") {
    path = currentWorkspacePath(app.route.page);
  } else if (app.route.kind === "methodology") {
    path = navigation.buildMethodologyPath(app.route.anchor);
  } else {
    path = navigation.buildScreenerPath(currentScreenerFilters());
  }
  window.history.replaceState({}, "", path);
  app.route = navigation.parseRoute(window.location.pathname, window.location.search);
  updateRouteLinks();
}

function navigateTo(path, { replace = false } = {}) {
  if (replace) window.history.replaceState({}, "", path);
  else window.history.pushState({}, "", path);
  applyRouteFromLocation();
  window.scrollTo({ top: 0, left: 0, behavior: "auto" });
}

function canonicalizeCurrentRoute() {
  if (!navigation || !app.routeReady) return;
  let path;
  if (app.route.kind === "workspace") {
    path = currentWorkspacePath(app.route.page);
  } else if (app.route.kind === "methodology") {
    path = navigation.buildMethodologyPath(app.route.anchor);
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
  document.querySelectorAll("[data-workspace-view]").forEach((view) => {
    view.hidden = view.dataset.workspaceView !== page;
  });
  const compareVisible = page === "compare";
  byId("comparison-status").hidden = !compareVisible;
  if (!compareVisible) byId("comparison-error").hidden = true;
  byId("time-toolbar").hidden = !compareVisible;
}

function syncSegmentedControls() {
  const groups = [
    ["[data-workspace-market-type]", "workspaceMarketType"],
    ["[data-liquidity-view]", "liquidityView"],
    ["[data-liquidity-scale]", "liquidityScale"],
    ["[data-execution-direction]", "executionDirection"],
    ["[data-quality-scope]", "qualityScope"],
  ];
  groups.forEach(([selector, stateKey]) => {
    document.querySelectorAll(selector).forEach((button) => {
      const key = Object.keys(button.dataset).find((name) => (
        ["workspaceMarketType", "liquidityView", "liquidityScale", "executionDirection", "qualityScope"]
          .includes(name)
      ));
      const active = key ? button.dataset[key] === app[stateKey] : false;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
  });
}

function syncTimePresetButtons() {
  if (!app.payload) return;
  const start = byId("date-start").value;
  const end = byId("date-end").value;
  let activePreset = "";
  if (start && end) {
    if (
      start === app.payload.metadata.available_start
      && end === app.payload.metadata.available_end
    ) {
      activePreset = "all";
    } else {
      const startTime = Date.parse(`${start}T00:00:00Z`);
      const endTime = Date.parse(`${end}T00:00:00Z`);
      if (Number.isFinite(startTime) && Number.isFinite(endTime)) {
        const inclusiveDays = Math.round((endTime - startTime) / 86_400_000) + 1;
        if ([7, 30, 90].includes(inclusiveDays)) activePreset = String(inclusiveDays);
      }
    }
  }
  document.querySelectorAll("[data-days]").forEach((button) => {
    const active = button.dataset.days === activePreset;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
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
    byId("apply-window").disabled = false;
  }
  if (marketPayloadMatchesWindow(app.payload, normalized.start, normalized.end)) return;

  const defaultWindow = normalizedMarketWindow("", "");
  const wantsDefault = (
    normalized.start === defaultWindow.start
    && normalized.end === defaultWindow.end
  );
  if (wantsDefault && app.defaultPayload) {
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
      quality: "Data Quality",
    };
    return `${route.token} ${labels[route.page]} · CEX / DEX Market Monitor`;
  }
  if (route.kind === "methodology") return "Methodology · CEX / DEX Market Monitor";
  return "Market Screener · CEX / DEX Market Monitor";
}

function announceRoute(route) {
  document.title = routeTitle(route);
  const label = route.kind === "workspace"
    ? `${route.token} ${route.page} page`
    : route.kind === "methodology"
      ? "Methodology page"
      : "Market Screener page";
  byId("route-announcer").textContent = `Showing ${label}.`;
}

function applyScreenerRoute(route) {
  app.route = route;
  app.searchQuery = (route.filters?.q || "").toUpperCase();
  byId("token-search").value = route.filters?.q || "";
  app.scope = ["combined", "cex", "dex"].includes(route.filters?.scope)
    ? route.filters.scope
    : "combined";
  document.querySelectorAll("[data-scope]").forEach((button) => {
    const active = button.dataset.scope === app.scope;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
  byId("sort-field").value = route.filters?.sort || "volume";
  const window = normalizedMarketWindow(route.filters?.start, route.filters?.end);
  byId("date-start").value = window.start;
  byId("date-end").value = window.end;
  syncTimePresetButtons();
  setActiveAppView("screener");
  byId("time-toolbar").hidden = false;
  renderTable();
  syncMarketPayloadForWindow(window.start, window.end);
}

function applyWorkspaceRoute(route) {
  const exactToken = app.catalog.tokens.find(
    (token) => token === String(route.token || "").toUpperCase(),
  );
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
  byId("facts-token").value = exactToken;
  const markets = factsMarketsForToken(exactToken);
  const routeProvidedPair = Boolean(route.state?.marketA || route.state?.marketB);
  const manualPair = route.state?.pairMode === "manual";
  const hadSavedPair = Boolean(app.pairSelections[exactToken]);
  const validation = navigation.validatePair(
    markets,
    route.state?.marketA,
    route.state?.marketB,
  );
  if (routeProvidedPair || manualPair) {
    populateFactsMarkets({
      requestedA: validation.marketA?.market_id || "",
      requestedB: validation.marketB?.market_id || "",
      allowDefaults: false,
    });
    const invalidReferenceErrors = validation.errors.filter((error) => (
      !["market_a_required", "market_b_required"].includes(error.code)
    ));
    if (invalidReferenceErrors.length) {
      const codes = validation.errors.map((error) => error.code).join(", ");
      showStatus(
        byId("workspace-context-notice"),
        `The shared link contains an invalid market pair (${codes}). Valid selections were kept; no replacement market was chosen.`,
        "stale",
      );
    } else if (!validation.valid) {
      showStatus(
        byId("workspace-context-notice"),
        `Pair selection is in progress for ${exactToken}. Choose two distinct markets; no replacement market was selected.`,
        "stale",
      );
    } else {
      hideStatus(byId("workspace-context-notice"));
    }
  } else {
    populateFactsMarkets();
    showStatus(
      byId("workspace-context-notice"),
      hadSavedPair
        ? `Restored the saved ${exactToken} market pair.`
        : `Auto-selected a source-backed ${exactToken} market pair. Review it before comparing.`,
      "success",
    );
  }

  if (route.page === "compare") {
    const window = compareRouteWindow(route);
    byId("date-start").value = window.start;
    byId("date-end").value = window.end;
    syncTimePresetButtons();
    syncMarketPayloadForWindow(window.start, window.end);
  } else if (route.page === "liquidity") {
    app.executionDirection = route.state?.side === "sell" ? "sell_token" : "buy_token";
    app.executionNotionalUsd = route.state?.notionalUsd || 10000;
    app.liquidityView = route.state?.view || "total";
    app.liquidityScale = route.state?.scale || "log";
    byId("execution-notional").value = String(app.executionNotionalUsd);
    syncSegmentedControls();
  } else if (route.page === "quality") {
    app.qualityScope = route.state?.scope || "all";
    syncSegmentedControls();
  }
  if (route.page !== "compare") {
    const window = normalizedMarketWindow("", "");
    syncMarketPayloadForWindow(window.start, window.end);
  }
  setActiveAppView("workspace");
  setActiveWorkspacePage(route.page);
  renderWorkspaceContext();
  renderWorkspaceMarkets();
  renderQualityFromCatalog();
  if (route.page === "compare") loadComparison();
  if (route.page === "liquidity") {
    renderLiquidityCurve();
    loadExecutionCost();
  }
  if (route.page === "quality") loadQuality();
}

function applyMethodologyRoute(route) {
  app.route = route;
  setActiveAppView("methodology");
  byId("time-toolbar").hidden = true;
  if (route.anchor) {
    window.requestAnimationFrame(() => {
      byId(route.anchor)?.scrollIntoView({ block: "start" });
    });
  }
}

function applyRouteFromLocation() {
  if (!navigation) return;
  const route = navigation.parseRoute(window.location.pathname, window.location.search);
  if (route.kind === "methodology") {
    applyMethodologyRoute(route);
  } else if (!app.catalog || !app.payload) {
    return;
  } else if (route.kind === "workspace") applyWorkspaceRoute(route);
  else if (route.kind === "screener") applyScreenerRoute(route);
  else navigateTo("/screener", { replace: true });
  app.routeReady = true;
  announceRoute(app.route);
  updateRouteLinks();
  canonicalizeCurrentRoute();
  if (window.lucide) window.lucide.createIcons();
}

function validateDateRange(
  start = byId("date-start").value,
  end = byId("date-end").value,
) {
  if (start && end && start > end) {
    return "Start date must not be after end date.";
  }
  return "";
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
  const cexByToken = grouped(app.payload.cex_markets);
  const dexByToken = grouped(app.payload.dex_pools);
  app.payload.tokens.forEach((token) => {
    const symbol = token.token_symbol;
    if (!app.selections[symbol]) app.selections[symbol] = {};
    if (!app.selectionOverrides[symbol]) app.selectionOverrides[symbol] = {};
    const cexIds = (cexByToken[symbol] || []).map(marketId);
    const dexIds = (dexByToken[symbol] || []).map(marketId);
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
  const rows = market === "cex" ? app.payload.cex_markets : app.payload.dex_pools;
  const selectedId = app.selections[token]?.[market];
  return rows.find((row) => row.token_symbol === token && marketId(row) === selectedId) || null;
}

function comparison(tokenSummary) {
  const token = tokenSummary.token_symbol;
  const cex = selectedMarket(token, "cex");
  const dex = selectedMarket(token, "dex");
  const cexPrices = new Map((cex?.price_points || []).map((point) => [point.date, point.price_usd]));
  const dexPrices = new Map((dex?.price_points || []).map((point) => [point.date, point.price_usd]));
  const commonDates = [...cexPrices.keys()].filter((date) => dexPrices.has(date)).sort();
  const spreadDate = commonDates.at(-1) || null;
  const cexSpreadPrice = spreadDate ? cexPrices.get(spreadDate) : null;
  const dexSpreadPrice = spreadDate ? dexPrices.get(spreadDate) : null;
  const spread = cexSpreadPrice && finite(dexSpreadPrice)
    ? dexSpreadPrice / cexSpreadPrice - 1
    : null;
  return { cex, dex, spread, spreadDate, cexSpreadPrice, dexSpreadPrice };
}

function aggregateFacts(tokenSummary, cexOptions, dexOptions) {
  const aggregateCex = firstFinite(
    tokenSummary.aggregate_cex_volume_usd,
    tokenSummary.cex_volume_usd,
    sumFinite(cexOptions, "volume_usd"),
  ) ?? 0;
  const aggregateDex = firstFinite(
    tokenSummary.aggregate_dex_volume_usd,
    tokenSummary.dex_volume_usd,
    sumFinite(dexOptions, "volume_usd"),
  ) ?? 0;
  const aggregateTotal = firstFinite(
    tokenSummary.aggregate_volume_usd,
    tokenSummary.total_volume_usd,
    aggregateCex + aggregateDex,
  ) ?? 0;
  const aggregateDexShare = firstFinite(
    tokenSummary.aggregate_dex_volume_share,
    tokenSummary.aggregate_dex_share,
    tokenSummary.observed_dex_share,
    aggregateTotal ? aggregateDex / aggregateTotal : null,
  );
  return { aggregateCex, aggregateDex, aggregateTotal, aggregateDexShare };
}

function sortValue(tokenSummary) {
  const cexOptions = app.payload.cex_markets.filter(
    (row) => row.token_symbol === tokenSummary.token_symbol,
  );
  const dexOptions = app.payload.dex_pools.filter(
    (row) => row.token_symbol === tokenSummary.token_symbol,
  );
  const aggregates = aggregateFacts(tokenSummary, cexOptions, dexOptions);
  const { cex, dex, spread } = comparison(tokenSummary);
  const field = byId("sort-field").value;
  if (field === "spread") return finite(spread) ? Math.abs(spread) : -Infinity;
  if (field === "return") {
    if (app.scope === "cex") return cex?.window_return ?? -Infinity;
    if (app.scope === "dex") return dex?.window_return ?? -Infinity;
    return Math.max(cex?.window_return ?? -Infinity, dex?.window_return ?? -Infinity);
  }
  if (field === "volatility") {
    if (app.scope === "cex") return cex?.daily_volatility ?? -Infinity;
    if (app.scope === "dex") return dex?.daily_volatility ?? -Infinity;
    return Math.max(cex?.daily_volatility ?? -Infinity, dex?.daily_volatility ?? -Infinity);
  }
  if (app.scope === "cex") return aggregates.aggregateCex;
  if (app.scope === "dex") return aggregates.aggregateDex;
  return aggregates.aggregateTotal;
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
              explanation: "",
              observedValue: null,
              threshold: null,
            }
          : {
              code: flag.code,
              severity: flag.severity || "warning",
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
    const severityClass = flag.severity === "critical" ? "danger" : "warn";
    const label = qualityFlagLabel(flag);
    return `<span class="quality-flag ${severityClass}" title="${escapeHtml(flag.explanation)}">`
      + `${escapeHtml(label)}</span>`;
  }).join("");
}

function screenerTokenRow(tokenSummary, cexOptions, dexOptions) {
  const token = tokenSummary.token_symbol;
  const { cex, dex, spread } = comparison(tokenSummary);
  const aggregates = aggregateFacts(tokenSummary, cexOptions, dexOptions);
  const tokenMarkets = app.catalog?.markets.filter((market) => market.token_symbol === token) || [];
  const statusCounts = tokenMarkets.reduce((counts, market) => {
    const status = market.quality_status || "ok";
    counts[status] = (counts[status] || 0) + 1;
    return counts;
  }, {});
  const qualityText = statusCounts.critical
    ? `${statusCounts.critical} critical`
    : statusCounts.warning
      ? `${statusCounts.warning} warning`
      : "Healthy";
  const qualityState = statusCounts.critical
    ? "critical"
    : statusCounts.warning
      ? "warning"
      : "ok";
  const researchPath = navigation
    ? navigation.buildWorkspacePath(token, "markets")
    : "#";
  return `<tr class="token-row screener-token-row">
    <td data-label="Token" class="sticky-token token-name">${escapeHtml(token)}</td>
    <td data-label="Covered markets">
      ${cexOptions.length} CEX · ${dexOptions.length} DEX
      <span class="metric-note">${tokenMarkets.length} catalog series</span>
    </td>
    <td data-label="Aggregate USD volume">
      ${formatCurrency(aggregates.aggregateTotal)}
      <span class="metric-note">CEX ${formatCurrency(aggregates.aggregateCex)} · DEX ${formatCurrency(aggregates.aggregateDex)}</span>
    </td>
    <td data-label="DEX share">${formatShare(aggregates.aggregateDexShare)}</td>
    <td data-label="Primary price gap" class="${metricClass(spread)}">
      ${formatPercent(spread)}
      <span class="metric-note">Primary DEX / CEX − 1</span>
    </td>
    <td data-label="Primary ±100 bps depth">
      ${formatDepth(cex?.total_depth_100bps_usd, cex?.depth_100bps_complete)}
      /
      ${formatDepth(dex?.total_depth_100bps_usd, dex?.depth_100bps_complete)}
      <span class="metric-note">Primary CEX / DEX</span>
    </td>
    <td data-label="Primary DEX TVL">${formatCurrency(dex?.tvl_usd)}</td>
    <td data-label="Quality">
      <span class="quality-state" data-state="${escapeHtml(qualityState)}">${escapeHtml(qualityText)}</span>
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
  const cexByToken = grouped(app.payload.cex_markets);
  const dexByToken = grouped(app.payload.dex_pools);
  const tokens = app.payload.tokens
    .filter((row) => !query || row.token_symbol.includes(query))
    .sort((a, b) => sortValue(b) - sortValue(a) || a.token_symbol.localeCompare(b.token_symbol));
  app.visibleTokens = tokens;

  byId("market-body").innerHTML = tokens.length
    ? tokens.map((token) => screenerTokenRow(
        token,
        cexByToken[token.token_symbol] || [],
        dexByToken[token.token_symbol] || [],
      )).join("")
    : `<tr><td data-label="Result" colspan="9" class="missing">No Token matches this search.</td></tr>`;
  byId("row-count").textContent = `${tokens.length} Tokens · one row per Token`;
}

function payloadMarketForCatalog(market) {
  if (!app.payload || !market) return null;
  const rows = market.market_type === "cex"
    ? app.payload.cex_markets
    : app.payload.dex_pools;
  return rows.find((row) => {
    if (row.token_symbol !== market.token_symbol) return false;
    if (market.market_type === "cex") {
      return row.venue === market.venue && row.instrument === market.instrument;
    }
    return (
      row.pool_address === market.pool_address
      && row.venue === market.venue
    );
  }) || null;
}

function qualityStateMarkup(status, label = "") {
  const normalized = String(status || "unavailable").toLowerCase();
  const display = label || normalized.replaceAll("_", " ");
  return `<span class="quality-state" data-state="${escapeHtml(normalized)}">${escapeHtml(display)}</span>`;
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
      ? `${counts.warning} warnings`
      : "No catalog warnings";
  byId("facts-title").textContent = `${token} Token Research`;
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
    const aggregates = aggregateFacts(
      tokenSummary,
      app.payload.cex_markets.filter((row) => row.token_symbol === token),
      app.payload.dex_pools.filter((row) => row.token_symbol === token),
    );
    byId("workspace-description").textContent = (
      `${formatCurrency(aggregates.aggregateTotal)} aggregate window volume · `
      + `${formatShare(aggregates.aggregateDexShare)} DEX share. `
      + "Market A/B stay shared across the four research pages."
    );
  }
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
        const selectedA = pair.marketA === market.market_id;
        const selectedB = pair.marketB === market.market_id;
        const depth = formatDepth(
          market.total_depth_100bps_usd,
          market.depth_100bps_complete,
        );
        const tvl = market.market_type === "dex"
          ? formatCurrency(firstFinite(row?.tvl_usd, market.tvl_usd))
          : "N/A";
        const identityMeta = [
          market.market_type.toUpperCase(),
          market.chain,
          market.source_quote_asset_label,
        ].filter(Boolean).join(" · ");
        return `<tr>
          <td>
            <span class="market-identity">
              <strong>${escapeHtml(factsMarketLabel(market))}</strong>
              <small>${escapeHtml(identityMeta)}</small>
              ${market.pool_address ? `<small>${escapeHtml(market.pool_address)}</small>` : ""}
            </span>
          </td>
          <td>${qualityStateMarkup(market.market_type, market.market_type.toUpperCase())}</td>
          <td>${formatPrice(row?.price_usd)}</td>
          <td>${formatCurrency(row?.volume_usd)}</td>
          <td>${tvl}</td>
          <td>${depth}<span class="metric-note">${escapeHtml(market.depth_status || "unavailable")}</span></td>
          <td>${formatRatio(market.coverage_ratio)}</td>
          <td>
            ${qualityStateMarkup(market.quality_status || "ok")}
            <span class="metric-note">${flags.length} reason${flags.length === 1 ? "" : "s"}</span>
          </td>
          <td>
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
  const markets = factsMarketsForToken(token)
    .filter((market) => app.qualityScope === "all" || selected.has(market.market_id))
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
  const details = [
    valueText,
    fact?.observed_at ? formatUtcTimestamp(fact.observed_at) : "",
    fact?.message || fact?.reason,
  ].filter(Boolean);
  const lineage = [
    fact?.source ? `Source: ${fact.source}` : "",
    fact?.method ? `Method: ${fact.method}` : "",
    fact?.snapshot_id ? `Snapshot: ${fact.snapshot_id}` : "",
    fact?.dataset_sha256 ? `Dataset SHA-256: ${fact.dataset_sha256}` : "",
    fact?.raw_response_sha256 ? `Raw-response SHA-256: ${fact.raw_response_sha256}` : "",
  ].filter(Boolean);
  return `<div class="quality-fact">
    ${qualityStateMarkup(status)}
    ${details.map((detail) => `<small>${escapeHtml(detail)}</small>`).join("")}
    ${lineage.length
      ? `<details><summary>Lineage</summary>${lineage.map((detail) => `<small>${escapeHtml(detail)}</small>`).join("")}</details>`
      : ""}
  </div>`;
}

function renderQualityPayload(payload) {
  app.quality = payload;
  const rows = Array.isArray(payload?.markets) ? payload.markets : [];
  byId("quality-body").innerHTML = rows.length
    ? rows.map((item) => {
        const market = item.market || item;
        const facts = item.facts || {};
        const flags = Array.isArray(item.quality_flags)
          ? item.quality_flags.map((flag) => ({
              code: flag.code,
              severity: flag.severity || "warning",
              explanation: flag.message || flag.explanation || "",
              observedValue: flag.observed_value ?? flag.observedValue ?? null,
              threshold: flag.threshold ?? null,
            }))
          : factsMarketWarningFlags(market);
        const reasons = flags.length
          ? `<details class="quality-reasons">
              <summary>${flags.length} current reason${flags.length === 1 ? "" : "s"}</summary>
              <ul>${flags.map((flag) => `<li data-severity="${escapeHtml(flag.severity)}">
                <strong>${escapeHtml(qualityFlagLabel(flag))}</strong>
                ${escapeHtml(flag.explanation || "No additional explanation supplied.")}
                ${qualityFlagMeasurement(flag) ? `<small>${escapeHtml(qualityFlagMeasurement(flag))}</small>` : ""}
              </li>`).join("")}</ul>
            </details>`
          : '<span class="missing">No current quality flags</span>';
        return `<tr>
          <td data-label="Market"><span class="market-identity"><strong>${escapeHtml(factsMarketLabel(market))}</strong><small>${escapeHtml(market.market_id)}</small></span></td>
          <td data-label="Daily Facts">${qualityFactMarkup("daily", facts.daily)}</td>
          <td data-label="TVL">${qualityFactMarkup("tvl", facts.tvl)}</td>
          <td data-label="Depth">${qualityFactMarkup("depth", facts.depth)}</td>
          <td data-label="Execution">${qualityFactMarkup("execution", facts.execution)}</td>
          <td data-label="Current Reasons">${reasons}</td>
        </tr>`;
      }).join("")
    : '<tr><td colspan="6" class="missing">No markets are available for this quality scope.</td></tr>';
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
  if (!finite(value)) return "N/A";
  const seconds = Math.round(value);
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const minutes = Math.round(seconds / 60);
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function executionFeeScope(rows) {
  const statuses = [...new Set(rows.map((row) => row?.fee_status).filter(Boolean))];
  if (!statuses.length) return "N/A";
  const labels = {
    excluded_unknown_account_tier: "CEX account fee excluded",
    included_protocol_fee: "DEX pool fee included",
    not_applicable: "No separate fee",
  };
  return statuses.map((status) => labels[status] || status.replaceAll("_", " ")).join(" · ");
}

function setExecutionLoading(message) {
  hideError(byId("execution-error"));
  showStatus(byId("execution-status"), message);
  ["execution-a-cost", "execution-b-cost", "execution-skew", "execution-fee-scope"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
  byId("execution-a-fill").textContent = "—";
  byId("execution-b-fill").textContent = "—";
  byId("execution-table-body").innerHTML = (
    '<tr><td data-label="Status" colspan="7" class="missing">Loading source-backed execution scenarios…</td></tr>'
  );
}

function invalidateExecutionRequest() {
  if (app.executionController) app.executionController.abort();
  app.executionController = null;
  app.executionRequestId += 1;
  return app.executionRequestId;
}

function clearExecutionResult(message = "") {
  app.execution = null;
  ["execution-a-cost", "execution-b-cost", "execution-skew", "execution-fee-scope"]
    .forEach((id) => {
      byId(id).textContent = "—";
    });
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
  const notionals = payload.metadata?.notionals_usd || [1000, 5000, 10000, 50000, 100000];
  const resultA = payload.market_a;
  const resultB = payload.market_b;
  const rowsA = notionals.map((notional) => executionScenario(
    resultA,
    app.executionDirection,
    notional,
  ));
  const rowsB = notionals.map((notional) => executionScenario(
    resultB,
    app.executionDirection,
    notional,
  ));
  byId("execution-a-cost-heading").textContent = `${executionMarketName(resultA, "A")} Cost`;
  byId("execution-b-cost-heading").textContent = `${executionMarketName(resultB, "B")} Cost`;
  byId("execution-table-body").innerHTML = notionals.map((notional, index) => {
    const rowA = rowsA[index];
    const rowB = rowsB[index];
    return `<tr>
      <th scope="row" data-label="Requested Notional">${formatCurrency(Number(notional))}</th>
      <td data-label="A Cost">${escapeHtml(formatExecutionCost(rowA))}</td>
      <td data-label="A Fill">${escapeHtml(formatExecutionFill(rowA))}</td>
      <td data-label="A Status">${executionStatusMarkup(rowA, resultA)}</td>
      <td data-label="B Cost">${escapeHtml(formatExecutionCost(rowB))}</td>
      <td data-label="B Fill">${escapeHtml(formatExecutionFill(rowB))}</td>
      <td data-label="B Status">${executionStatusMarkup(rowB, resultB)}</td>
    </tr>`;
  }).join("");

  const selectedA = executionScenario(
    resultA,
    app.executionDirection,
    app.executionNotionalUsd,
  );
  const selectedB = executionScenario(
    resultB,
    app.executionDirection,
    app.executionNotionalUsd,
  );
  byId("execution-a-label").textContent = `${executionMarketName(resultA, "A")} cost`;
  byId("execution-b-label").textContent = `${executionMarketName(resultB, "B")} cost`;
  byId("execution-a-cost").textContent = formatExecutionCost(selectedA);
  byId("execution-b-cost").textContent = formatExecutionCost(selectedB);
  byId("execution-a-fill").textContent = (
    `${formatExecutionFill(selectedA)} fill · ${selectedA?.status_reason || selectedA?.status || resultA?.status || "unavailable"}`
  );
  byId("execution-b-fill").textContent = (
    `${formatExecutionFill(selectedB)} fill · ${selectedB?.status_reason || selectedB?.status || resultB?.status || "unavailable"}`
  );
  byId("execution-skew").textContent = formatDurationSeconds(
    payload.metadata?.snapshot_skew_seconds,
  );
  byId("execution-fee-scope").textContent = executionFeeScope([selectedA, selectedB]);

  const scenarioRows = [...rowsA, ...rowsB].filter(Boolean);
  const statuses = scenarioRows.map((row) => row.status);
  const unavailableResults = [resultA, resultB].filter((result) => (
    !result || result.status !== "available"
  ));
  const failed = statuses.filter((status) => status === "failed").length;
  const partial = statuses.filter((status) => status === "partial").length;
  const unsupported = statuses.filter((status) => status === "unsupported").length;
  const observed = statuses.filter((status) => status === "observed").length;
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
      + "Null cost means the full request was not measured; it is not zero.",
    state,
  );
  hideError(byId("execution-error"));
}

async function loadExecutionCost() {
  const requestId = invalidateExecutionRequest();
  const token = selectedWorkspaceToken();
  const { marketA, marketB } = selectedPairState();
  if (!app.catalog || !token || !marketA || !marketB || marketA === marketB) {
    clearExecutionResult("Choose two distinct markets for this Token to inspect execution cost.");
    return false;
  }
  const controller = new AbortController();
  app.executionController = controller;
  const query = new URLSearchParams({
    token,
    market_a: marketA,
    market_b: marketB,
  });
  setExecutionLoading(`Loading ${token} fixed-notional execution facts…`);
  try {
    const response = await fetch(`/api/markets/execution-cost?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Execution facts failed to load.");
    if (requestId !== app.executionRequestId) return false;
    renderExecution(payload);
    return true;
  } catch (error) {
    if (error.name === "AbortError" || requestId !== app.executionRequestId) return false;
    clearExecutionResult(error.message || String(error));
    return false;
  } finally {
    if (requestId === app.executionRequestId) app.executionController = null;
  }
}

function invalidateQualityRequest() {
  if (app.qualityController) app.qualityController.abort();
  app.qualityController = null;
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

async function loadQuality() {
  const requestId = invalidateQualityRequest();
  const token = selectedWorkspaceToken();
  const { marketA, marketB } = selectedPairState();
  if (!app.catalog || !token) {
    showError(byId("quality-error"), "Market catalog is unavailable.");
    return false;
  }
  if (app.qualityScope === "selected" && (!marketA || !marketB || marketA === marketB)) {
    renderQualityFromCatalog();
    showStatus(
      byId("quality-status"),
      "Selected scope needs two distinct markets. The catalog-level fallback remains visible.",
      "stale",
    );
    showError(byId("quality-error"), "Choose distinct Market A and Market B.");
    return false;
  }
  const controller = new AbortController();
  app.qualityController = controller;
  const query = new URLSearchParams({ token, scope: app.qualityScope });
  if (app.qualityScope === "selected") {
    query.set("market_a", marketA);
    query.set("market_b", marketB);
  }
  hideError(byId("quality-error"));
  showStatus(byId("quality-status"), `Loading ${token} fact lineage and quality states…`);
  try {
    const response = await fetch(`/api/markets/quality?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Quality facts failed to load.");
    if (requestId !== app.qualityRequestId) return false;
    renderQualityPayload(payload);
    const counts = qualityStatusCounts(payload);
    const critical = (counts.failed || 0);
    const warnings = (
      (counts.partial || 0)
      + (counts.unsupported || 0)
      + (counts.unavailable || 0)
      + (counts.not_cataloged_in_snapshot || 0)
    );
    const state = critical ? "critical" : warnings ? "warning" : "success";
    showStatus(
      byId("quality-status"),
      `${payload.token_symbol} · ${payload.metadata.scope} scope · `
        + `${payload.markets.length} markets · ${counts.observed || 0} observed, `
        + `${counts.partial || 0} partial, ${counts.unsupported || 0} unsupported, `
        + `${counts.failed || 0} failed, ${counts.unavailable || 0} unavailable facts.`,
      state,
    );
    hideError(byId("quality-error"));
    return true;
  } catch (error) {
    if (error.name === "AbortError" || requestId !== app.qualityRequestId) return false;
    renderQualityFromCatalog();
    showStatus(
      byId("quality-status"),
      "Catalog-level quality remains visible; detailed lineage could not be loaded.",
      "stale",
    );
    showError(byId("quality-error"), error.message || String(error));
    return false;
  } finally {
    if (requestId === app.qualityRequestId) app.qualityController = null;
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
  start.value = metadata.start_date;
  end.value = metadata.end_date;
  syncTimePresetButtons();
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

function factsOptions(markets, selectedId) {
  return '<option value="">Select market</option>' + markets.map((market) => (
    `<option value="${escapeHtml(market.market_id)}" ${market.market_id === selectedId ? "selected" : ""}>`
      + `${escapeHtml(factsMarketLabel(market))}</option>`
  )).join("");
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
      Inspect this pair in Data Quality
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
    byId("liquidity-empty").hidden = false;
    return false;
  }
  byId("liquidity-empty").hidden = true;
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

function renderLiquidityTable(marketA, marketB, dataMarketA, dataMarketB, invalidA, invalidB) {
  const sidesA = liquiditySideDefinition(marketA);
  const sidesB = liquiditySideDefinition(marketB);
  byId("liquidity-a-total-heading").textContent = "A Total";
  byId("liquidity-a-sell-heading").textContent = `A ${sidesA.sellLabel}`;
  byId("liquidity-a-buy-heading").textContent = `A ${sidesA.buyLabel}`;
  byId("liquidity-b-total-heading").textContent = "B Total";
  byId("liquidity-b-sell-heading").textContent = `B ${sidesB.sellLabel}`;
  byId("liquidity-b-buy-heading").textContent = `B ${sidesB.buyLabel}`;
  byId("liquidity-table-body").innerHTML = DEPTH_BANDS.map((band) => {
    const completeA = Boolean(dataMarketA?.[`depth_${band}bps_complete`]);
    const completeB = Boolean(dataMarketB?.[`depth_${band}bps_complete`]);
    return `<tr>
      <th scope="row" data-label="Band">±${band} bps</th>
      <td data-label="A Total">${formatExactDepth(liquidityDepthValue(dataMarketA, band), completeA)}</td>
      <td data-label="A Sell execution">${formatExactDepth(liquidityDepthValue(dataMarketA, band, sidesA.sellField), completeA)}</td>
      <td data-label="A Buy execution">${formatExactDepth(liquidityDepthValue(dataMarketA, band, sidesA.buyField), completeA)}</td>
      <td data-label="A Completeness">${escapeHtml(liquidityCompletenessLabel(marketA, dataMarketA, band, invalidA))}</td>
      <td data-label="B Total">${formatExactDepth(liquidityDepthValue(dataMarketB, band), completeB)}</td>
      <td data-label="B Sell execution">${formatExactDepth(liquidityDepthValue(dataMarketB, band, sidesB.sellField), completeB)}</td>
      <td data-label="B Buy execution">${formatExactDepth(liquidityDepthValue(dataMarketB, band, sidesB.buyField), completeB)}</td>
      <td data-label="B Completeness">${escapeHtml(liquidityCompletenessLabel(marketB, dataMarketB, band, invalidB))}</td>
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

function renderLiquiditySummary(marketA, marketB, dataMarketA, dataMarketB) {
  const completeA = Boolean(dataMarketA?.depth_100bps_complete);
  const completeB = Boolean(dataMarketB?.depth_100bps_complete);
  byId("liquidity-a-label").textContent = marketA
    ? `A · ${marketA.venue} total at ±100 bps`
    : "Market A at ±100 bps";
  byId("liquidity-b-label").textContent = marketB
    ? `B · ${marketB.venue} total at ±100 bps`
    : "Market B at ±100 bps";
  byId("liquidity-a-100").textContent = formatSummaryDepth(
    liquidityDepthValue(dataMarketA, 100),
    completeA,
  );
  byId("liquidity-b-100").textContent = formatSummaryDepth(
    liquidityDepthValue(dataMarketB, 100),
    completeB,
  );
  byId("liquidity-skew").textContent = liquiditySnapshotSkew(marketA, marketB) || "N/A";
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
  const { token, marketA, marketB } = selectedLiquidityMarkets();
  const issuesA = liquidityDepthIssues(marketA);
  const issuesB = liquidityDepthIssues(marketB);
  const dataMarketA = liquidityRenderableMarket(marketA, issuesA);
  const dataMarketB = liquidityRenderableMarket(marketB, issuesB);
  const series = [
    ...liquiditySeriesForMarket("A", marketA),
    ...liquiditySeriesForMarket("B", marketB),
  ];
  const plotted = renderLiquiditySvg(series);
  const plottedSlots = new Set(series.map((item) => item.slot));
  const unavailableSlots = [
    ["A", marketA],
    ["B", marketB],
  ].filter(([slot]) => !plottedSlots.has(slot));
  const failedSlots = unavailableSlots
    .filter(([, market]) => market?.depth_status === "failed")
    .map(([slot]) => slot);
  const partialSlots = [
    ["A", marketA],
    ["B", marketB],
  ].filter(([, market]) => market?.depth_status === "partial");
  const qualityWarningSlots = [
    ["A", marketA],
    ["B", marketB],
  ].map(([slot, market]) => [slot, liquidityRelevantFlags(market)])
    .filter(([, flags]) => flags.length);
  const hasCriticalQualityFlag = qualityWarningSlots.some(([, flags]) => (
    flags.some((flag) => flag.severity === "critical")
  ));
  renderLiquidityLegend(series);
  renderLiquiditySummary(marketA, marketB, dataMarketA, dataMarketB);
  renderLiquidityTable(
    marketA,
    marketB,
    dataMarketA,
    dataMarketB,
    Boolean(marketA && issuesA.length),
    Boolean(marketB && issuesB.length),
  );
  renderLiquidityMarketMeta("A", marketA, issuesA);
  renderLiquidityMarketMeta("B", marketB, issuesB);
  const skew = liquiditySnapshotSkew(marketA, marketB);
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
      + `${skew ? ` · snapshot skew ${skew}` : " · snapshot skew unavailable"}`
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
    marketB
      ? `${liquidityMarketLabel("B", marketB)} is ${marketB.depth_status || "unavailable"} at ${formatUtcTimestamp(marketB.depth_observed_at)}.`
      : "Market B is unavailable.",
    "Only the four labeled thresholds are measured; missing markets are not replaced with zero or TVL.",
  ].join(" ");
  hideLiquidityTooltip();
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
} = {}) {
  if (!app.catalog) return;
  const token = byId("facts-token").value;
  const markets = factsMarketsForToken(token);
  const tokenSummary = app.payload?.tokens.find((row) => row.token_symbol === token);
  const saved = app.pairSelections[token] || {};
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
    marketB = markets.find((market) => market.market_id !== marketA.market_id);
  }
  byId("facts-market-a").innerHTML = factsOptions(markets, marketA?.market_id);
  byId("facts-market-b").innerHTML = factsOptions(markets, marketB?.market_id);
  byId("facts-market-a").value = marketA?.market_id || "";
  byId("facts-market-b").value = marketB?.market_id || "";
  if (marketA && marketB) {
    app.pairSelections[token] = {
      marketA: marketA.market_id,
      marketB: marketB.market_id,
    };
    writePairSelections();
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

function setComparisonLoading(message) {
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
  byId("comparison-body").innerHTML = '<tr><td colspan="8" class="missing">Loading the selected markets…</td></tr>';
}

function invalidateComparisonRequest() {
  if (app.comparisonController) app.comparisonController.abort();
  app.comparisonController = null;
  app.comparisonRequestId += 1;
  return app.comparisonRequestId;
}

function clearComparisonResult(message = "") {
  app.comparison = null;
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
  byId("comparison-body").innerHTML = '<tr><td colspan="8" class="missing">No current result.</td></tr>';
  hideStatus(byId("comparison-status"));
  if (message) showError(byId("comparison-error"), message);
  else hideError(byId("comparison-error"));
  byId("facts-workbench").setAttribute("aria-busy", "false");
  byId("compare-markets").disabled = false;
}

function renderComparison(payload) {
  app.comparison = payload;
  const latest = payload.latest_comparable_observation;
  byId("compare-date").textContent = latest?.date || "N/A";
  byId("compare-absolute").textContent = formatRawUsd(latest?.absolute_spread_usd);
  byId("compare-bps").textContent = finite(latest?.spread_bps)
    ? `${bpsFormat.format(latest.spread_bps)} bps`
    : "N/A";
  byId("compare-days").textContent = `${payload.metadata.comparison_days} / ${payload.metadata.union_observation_days}`;
  byId("compare-a-return").textContent = formatPercent(
    payload.market_a_statistics?.window_return,
  );
  byId("compare-b-return").textContent = formatPercent(
    payload.market_b_statistics?.window_return,
  );
  byId("compare-a-volatility").textContent = formatPercent(
    payload.market_a_statistics?.daily_volatility,
  );
  byId("compare-b-volatility").textContent = formatPercent(
    payload.market_b_statistics?.daily_volatility,
  );
  byId("market-a-price-heading").textContent = `${payload.market_a.venue} Price (USD)`;
  byId("market-a-volume-heading").textContent = `${payload.market_a.venue} Volume (USD)`;
  byId("market-b-price-heading").textContent = `${payload.market_b.venue} Price (USD)`;
  byId("market-b-volume-heading").textContent = `${payload.market_b.venue} Volume (USD)`;
  const missingLabels = {
    market_a_missing: "A missing · no fill",
    market_b_missing: "B missing · no fill",
    non_comparable_price: "Price null/invalid",
  };
  const rows = [...payload.observations].reverse();
  byId("comparison-body").innerHTML = rows.length
    ? rows.map((row) => `<tr>
        <td>${escapeHtml(row.date)}</td>
        <td>${formatRawUsd(row.market_a.price_usd)}</td>
        <td>${formatRawVolume(row.market_a.volume_usd)}</td>
        <td>${formatRawUsd(row.market_b.price_usd)}</td>
        <td>${formatRawVolume(row.market_b.volume_usd)}</td>
        <td>${formatRawUsd(row.absolute_spread_usd)}</td>
        <td>${finite(row.spread_bps) ? bpsFormat.format(row.spread_bps) : "N/A"}</td>
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
}

async function responseJson(response) {
  try {
    return await response.json();
  } catch {
    throw new Error(`Server returned ${response.status} without a valid JSON body.`);
  }
}

async function loadComparison() {
  const requestId = invalidateComparisonRequest();
  if (!app.catalog) {
    clearComparisonResult("Market catalog is unavailable.");
    return false;
  }
  const dateError = validateDateRange();
  if (dateError) {
    clearComparisonResult(dateError);
    return false;
  }
  const token = byId("facts-token").value;
  const marketA = byId("facts-market-a").value;
  const marketB = byId("facts-market-b").value;
  if (!token || !marketA || !marketB || marketA === marketB) {
    clearComparisonResult(
      token
        ? "This Token does not currently have two distinct market series to compare."
        : "Select a Token and two distinct market series.",
    );
    return false;
  }
  const controller = new AbortController();
  app.comparisonController = controller;
  const query = new URLSearchParams({ token, market_a: marketA, market_b: marketB });
  if (byId("date-start").value) query.set("start", byId("date-start").value);
  if (byId("date-end").value) query.set("end", byId("date-end").value);
  setComparisonLoading(`Loading ${token} comparison…`);
  try {
    const response = await fetch(`/api/markets/compare?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Comparison failed to load.");
    if (requestId !== app.comparisonRequestId) return false;
    renderComparison(payload);
    return true;
  } catch (error) {
    if (error.name === "AbortError" || requestId !== app.comparisonRequestId) return false;
    clearComparisonResult(error.message || String(error));
    return false;
  } finally {
    if (requestId === app.comparisonRequestId) {
      app.comparisonController = null;
      byId("compare-markets").disabled = false;
    }
  }
}

async function loadCatalog() {
  const response = await fetch("/api/markets/catalog");
  const payload = await responseJson(response);
  if (!response.ok) throw new Error(payload.error || "Market catalog failed to load.");
  app.catalog = payload;
  const currentToken = byId("facts-token").value;
  byId("facts-token").innerHTML = payload.tokens
    .map((token) => `<option value="${escapeHtml(token)}">${escapeHtml(token)}</option>`)
    .join("");
  byId("facts-token").value = payload.tokens.includes(currentToken)
    ? currentToken
    : preferredLiquidityToken(payload);
  populateFactsMarkets();
  updateFactsContract();
  return payload;
}

function isMarketPayload(payload) {
  return Boolean(
    payload
    && payload.metadata
    && Array.isArray(payload.tokens)
    && Array.isArray(payload.cex_markets)
    && Array.isArray(payload.dex_pools)
  );
}

function readDefaultMarketCache() {
  try {
    const payload = JSON.parse(window.localStorage.getItem(DEFAULT_MARKET_CACHE_KEY));
    return isMarketPayload(payload) ? payload : null;
  } catch {
    return null;
  }
}

function writeDefaultMarketCache(payload) {
  try {
    window.localStorage.setItem(DEFAULT_MARKET_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // A fresh network response still renders when browser storage is unavailable.
  }
}

function displayMarket(payload, { cached = false } = {}) {
  app.payload = payload;
  if (isDefaultMarketPayload(payload)) {
    app.defaultPayload = payload;
    app.defaultPayloadIsCached = cached;
  }
  hideError(byId("error-banner"));
  hideError(byId("global-error"));
  updateMetadata();
  renderTable();
  byId("market-panel").setAttribute("aria-busy", "false");
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
  if (app.catalog) populateFactsMarkets({ preserve: true });
}

function setMarketLoading(message, preserve) {
  byId("apply-window").disabled = true;
  byId("market-panel").setAttribute("aria-busy", "true");
  hideError(byId("error-banner"));
  showStatus(byId("market-loading"), message, preserve ? "stale" : "");
  if (!preserve) {
    app.payload = null;
    app.visibleTokens = [];
    hideStatus(byId("market-status"));
    byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="9" class="missing">Loading the requested time window…</td></tr>';
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
  byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="9" class="missing">No current market result.</td></tr>';
  byId("row-count").textContent = "No current result";
  hideStatus(byId("market-loading"));
  hideStatus(byId("market-status"));
  if (message) showError(byId("error-banner"), message);
  else hideError(byId("error-banner"));
  byId("market-panel").setAttribute("aria-busy", "false");
  byId("apply-window").disabled = false;
}

async function loadMarket(start = "", end = "", { preserve = false } = {}) {
  const requestId = invalidateMarketRequest();
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
    const response = await fetch(`/api/market?${query.toString()}`, {
      signal: controller.signal,
    });
    const payload = await responseJson(response);
    if (!response.ok) throw new Error(payload.error || "Market data failed to load.");
    if (requestId !== app.marketRequestId) return false;
    displayMarket(payload);
    if (!start && !end) writeDefaultMarketCache(payload);
    hideStatus(byId("market-loading"));
    return true;
  } catch (error) {
    if (error.name === "AbortError" || requestId !== app.marketRequestId) return false;
    hideStatus(byId("market-loading"));
    byId("market-panel").setAttribute("aria-busy", "false");
    const retained = preserve && app.payload
      ? " The explicitly marked cached snapshot remains visible."
      : " No result is shown for the failed request.";
    if (preserve && app.payload) {
      showStatus(
        byId("market-status"),
        "Cached facts remain visible; the fresh request failed.",
        "stale",
      );
      showError(byId("error-banner"), `${error.message || String(error)}${retained}`);
    } else {
      clearMarketResult(`${error.message || String(error)}${retained}`);
    }
    if (app.route?.kind !== "screener") {
      showError(byId("global-error"), `${error.message || String(error)}${retained}`);
    }
    return false;
  } finally {
    if (requestId === app.marketRequestId) {
      app.marketController = null;
      app.marketRequestWindowKey = "";
      byId("apply-window").disabled = false;
    }
  }
}

function setPreset(days) {
  if (!app.payload) return;
  const end = new Date(`${app.payload.metadata.available_end}T00:00:00Z`);
  const start = new Date(end);
  if (days === "all") {
    byId("date-start").value = app.payload.metadata.available_start;
  } else {
    start.setUTCDate(start.getUTCDate() - Number(days) + 1);
    const candidate = start.toISOString().slice(0, 10);
    byId("date-start").value = candidate < app.payload.metadata.available_start
      ? app.payload.metadata.available_start
      : candidate;
  }
  byId("date-end").value = app.payload.metadata.available_end;
}

async function applyWindow() {
  const start = byId("date-start").value;
  const end = byId("date-end").value;
  if (validateDateRange(start, end)) {
    if (app.route.kind === "workspace" && app.route.page === "compare") {
      clearComparisonResult(validateDateRange(start, end));
    } else {
      showError(byId("error-banner"), validateDateRange(start, end));
    }
    return;
  }
  const tasks = [loadMarket(start, end, { preserve: Boolean(app.payload) })];
  if (app.route.kind === "workspace" && app.route.page === "compare") {
    tasks.push(loadComparison());
  }
  await Promise.allSettled(tasks);
  replaceCurrentRoute();
}

function persistSelectedPair() {
  const token = selectedWorkspaceToken();
  const { marketA, marketB } = selectedPairState();
  if (token && marketA && marketB && marketA !== marketB) {
    app.pairSelections[token] = { marketA, marketB };
    writePairSelections();
    return true;
  }
  if (token && Object.hasOwn(app.pairSelections, token)) {
    delete app.pairSelections[token];
    writePairSelections();
  }
  return false;
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
  if (app.route.page === "quality") loadQuality();
}

function selectWorkspaceMarket(slot, marketIdValue) {
  const target = byId(`facts-market-${slot}`);
  const otherSlot = slot === "a" ? "b" : "a";
  const other = byId(`facts-market-${otherSlot}`);
  target.value = marketIdValue;
  if (marketIdValue && other.value === marketIdValue) {
    other.value = "";
    showStatus(
      byId("workspace-context-notice"),
      `Market ${otherSlot.toUpperCase()} was cleared because A and B must be different. Choose another market explicitly.`,
      "stale",
    );
  } else if (persistSelectedPair()) {
    hideStatus(byId("workspace-context-notice"));
  }
  persistSelectedPair();
  replaceCurrentRoute();
  refreshWorkspacePageData();
}

function workspaceStateWithoutMarkets(page) {
  const state = currentWorkspaceRouteState(page);
  delete state.marketA;
  delete state.marketB;
  state.pairMode = "manual";
  return state;
}

function csvEscape(value) {
  const text = value === null || value === undefined ? "" : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}

function exportVisibleCsv() {
  if (!app.payload || !app.visibleTokens.length) return;
  const cexByToken = grouped(app.payload.cex_markets);
  const dexByToken = grouped(app.payload.dex_pools);
  const headers = [
    "token",
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
    const cexOptions = cexByToken[token] || [];
    const dexOptions = dexByToken[token] || [];
    const aggregates = aggregateFacts(tokenSummary, cexOptions, dexOptions);
    const selected = comparison(tokenSummary);
    lines.push([
      token,
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
        market === "cex" ? row.depth_status : row.dex_depth_status,
        flags,
        selected.spread,
      ].map(csvEscape).join(","));
    });
  });
  const blob = new Blob([`${lines.join("\n")}\n`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `cex-dex-market-facts-${byId("date-start").value}-${byId("date-end").value}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  showStatus(byId("market-status"), `Exported ${app.visibleTokens.length} visible Tokens as CSV.`, "success");
}

function bindEvents() {
  const applyTokenSearch = () => {
    app.searchQuery = byId("token-search").value.trim().toUpperCase();
    renderTable();
    replaceCurrentRoute();
  };
  byId("apply-window").addEventListener("click", applyWindow);
  document.querySelectorAll("[data-days]").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("[data-days]").forEach((item) => {
        const active = item === button;
        item.classList.toggle("active", active);
        item.setAttribute("aria-pressed", String(active));
      });
      setPreset(button.dataset.days);
      applyWindow();
    });
  });
  document.querySelectorAll("[data-scope]").forEach((button) => {
    button.addEventListener("click", () => {
      app.scope = button.dataset.scope;
      document.querySelectorAll("[data-scope]").forEach((item) => {
        const active = item === button;
        item.classList.toggle("active", active);
        item.setAttribute("aria-pressed", String(active));
      });
      renderTable();
      replaceCurrentRoute();
    });
  });
  byId("sort-field").addEventListener("change", () => {
    renderTable();
    replaceCurrentRoute();
  });
  byId("search-token").addEventListener("click", applyTokenSearch);
  byId("token-search").addEventListener("keydown", (event) => {
    if (event.key === "Enter") applyTokenSearch();
  });
  byId("token-search").addEventListener("search", applyTokenSearch);
  byId("facts-token").addEventListener("change", () => {
    const newToken = byId("facts-token").value;
    const previousToken = app.route?.kind === "workspace" ? app.route.token : "";
    if (!newToken || !navigation) return;
    delete app.pairSelections[newToken];
    writePairSelections();
    const page = app.route?.kind === "workspace" ? app.route.page : "markets";
    navigateTo(navigation.buildWorkspacePath(
      newToken,
      page,
      workspaceStateWithoutMarkets(page),
    ));
    showStatus(
      byId("workspace-context-notice"),
      previousToken
        ? `Token changed from ${previousToken} to ${newToken}. The previous markets were cleared.`
        : `Choose two ${newToken} markets.`,
      "stale",
    );
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
      syncSegmentedControls();
      renderQualityFromCatalog();
      replaceCurrentRoute();
      loadQuality();
    });
  });
  bindLiquidityTooltipEvents();
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
  byId("compare-markets").addEventListener("click", () => {
    persistSelectedPair();
    replaceCurrentRoute();
    refreshWorkspacePageData();
  });
  byId("export-csv").addEventListener("click", exportVisibleCsv);
  document.addEventListener("click", (event) => {
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
  window.addEventListener("popstate", applyRouteFromLocation);
}

function primeInitialRouteView(route) {
  if (route.kind === "workspace") {
    setActiveAppView("workspace");
    setActiveWorkspacePage(route.page);
    return;
  }
  if (route.kind === "methodology") {
    applyMethodologyRoute(route);
    return;
  }
  setActiveAppView("screener");
  byId("time-toolbar").hidden = false;
}

async function initialize() {
  app.pairSelections = readPairSelections();
  bindEvents();
  const initialRoute = navigation
    ? navigation.parseRoute(window.location.pathname, window.location.search)
    : { kind: "unknown" };
  if (initialRoute.kind !== "unknown") app.route = initialRoute;
  primeInitialRouteView(initialRoute);
  const initialStart = initialRoute.kind === "screener"
    ? initialRoute.filters?.start || ""
    : initialRoute.kind === "workspace" && initialRoute.page === "compare"
      ? initialRoute.state?.start || ""
      : "";
  const initialEnd = initialRoute.kind === "screener"
    ? initialRoute.filters?.end || ""
    : initialRoute.kind === "workspace" && initialRoute.page === "compare"
      ? initialRoute.state?.end || ""
      : "";
  const cachedPayload = readDefaultMarketCache();
  if (cachedPayload) displayMarket(cachedPayload, { cached: true });
  const [marketResult, catalogResult] = await Promise.allSettled([
    loadMarket(initialStart, initialEnd, { preserve: Boolean(cachedPayload) }),
    loadCatalog(),
  ]);
  if (marketResult.status === "rejected") {
    showError(byId("error-banner"), marketResult.reason?.message || String(marketResult.reason));
  }
  if (catalogResult.status === "rejected") {
    const message = catalogResult.reason?.message || String(catalogResult.reason);
    showError(byId("comparison-error"), message);
    showError(byId("global-error"), `Market catalog failed to load: ${message}`);
    byId("facts-workbench").setAttribute("aria-busy", "false");
  } else {
    if (!navigation) {
      showError(byId("error-banner"), "Navigation module failed to load.");
    } else {
      applyRouteFromLocation();
    }
  }
  if (window.lucide) window.lucide.createIcons();
}

if (typeof document !== "undefined") initialize();
