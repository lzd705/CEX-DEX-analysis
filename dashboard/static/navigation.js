(function attachMarketMonitorNavigation(root, factory) {
  "use strict";

  const navigation = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = navigation;
  }
  if (root) {
    root.MarketMonitorNavigation = navigation;
  }
})(typeof globalThis !== "undefined" ? globalThis : this, function createNavigation() {
  "use strict";

  const WORKSPACE_PAGES = Object.freeze([
    "markets",
    "compare",
    "liquidity",
    "events",
    "quality",
  ]);
  const WORKSPACE_PAGE_SET = new Set(WORKSPACE_PAGES);
  const SCREENER_SCOPES = new Set(["combined", "cross", "cex", "dex"]);
  const SCREENER_SORTS = new Set([
    "volume",
    "spread",
    "spread_max",
    "spread_mean",
    "spread_median",
    "return",
    "volatility",
    "depth_100bps",
    "dex_tvl",
  ]);
  const SCREENER_DIRECTIONS = new Set(["asc", "desc"]);
  const SCREENER_SORT_RULES = Object.freeze({
    volume: Object.freeze({
      scopes: new Set(["combined", "cex", "dex"]),
      defaultScope: "combined",
    }),
    spread: Object.freeze({
      scopes: new Set(["cross"]),
      defaultScope: "cross",
    }),
    spread_max: Object.freeze({
      scopes: new Set(["cross"]),
      defaultScope: "cross",
    }),
    spread_mean: Object.freeze({
      scopes: new Set(["cross"]),
      defaultScope: "cross",
    }),
    spread_median: Object.freeze({
      scopes: new Set(["cross"]),
      defaultScope: "cross",
    }),
    return: Object.freeze({
      scopes: new Set(["cex", "dex"]),
      defaultScope: "cex",
    }),
    volatility: Object.freeze({
      scopes: new Set(["cex", "dex"]),
      defaultScope: "cex",
    }),
    depth_100bps: Object.freeze({
      scopes: new Set(["cex", "dex"]),
      defaultScope: "cex",
    }),
    dex_tvl: Object.freeze({
      scopes: new Set(["dex"]),
      defaultScope: "dex",
    }),
  });
  const COMPARE_WINDOWS = new Set(["7d", "30d", "90d", "all"]);
  const LIQUIDITY_SIDES = new Set(["buy", "sell"]);
  const LIQUIDITY_VIEWS = new Set(["total", "directional"]);
  const LIQUIDITY_SCALES = new Set(["linear", "log"]);
  const QUALITY_SCOPES = new Set(["all", "selected"]);
  const QUALITY_SEVERITIES = new Set(["critical", "warning", "info"]);
  const QUALITY_ORIGINS = new Set(["screener"]);
  const EVENT_LIFECYCLES = new Set([
    "all",
    "occurred",
    "scheduled",
    "postponed",
    "cancelled",
    "superseded",
  ]);
  const EVENT_CLOCK_STATES = new Set([
    "all",
    "future",
    "current_window",
    "past",
  ]);
  const PAIR_MODES = new Set(["manual"]);
  const EXECUTION_NOTIONALS = new Set([1000, 5000, 10000, 50000, 100000]);
  const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

  function stringValue(value) {
    return typeof value === "string" && value.length ? value : null;
  }

  function safeDecode(value) {
    try {
      return decodeURIComponent(value);
    } catch {
      return null;
    }
  }

  function searchParams(search) {
    const value = typeof search === "string" ? search : "";
    return new URLSearchParams(value.startsWith("?") ? value.slice(1) : value);
  }

  function firstParam(params, names) {
    for (const name of names) {
      const value = stringValue(params.get(name));
      if (value !== null) return value;
    }
    return null;
  }

  function setString(params, name, value) {
    const normalized = stringValue(value);
    if (normalized !== null) params.set(name, normalized);
  }

  function setEnum(params, name, value, allowed) {
    if (typeof value === "string" && allowed.has(value)) {
      params.set(name, value);
    }
  }

  function isIsoDate(value) {
    if (typeof value !== "string" || !ISO_DATE.test(value)) return false;
    const parsed = new Date(`${value}T00:00:00Z`);
    return (
      !Number.isNaN(parsed.getTime())
      && parsed.toISOString().slice(0, 10) === value
    );
  }

  function setDate(params, name, value) {
    if (isIsoDate(value)) {
      params.set(name, value);
    }
  }

  function positiveNotional(value) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  }

  function collectedNotional(value) {
    const parsed = positiveNotional(value);
    return parsed !== null && EXECUTION_NOTIONALS.has(parsed) ? parsed : null;
  }

  function withQuery(path, params) {
    const query = params.toString();
    return query ? `${path}?${query}` : path;
  }

  function parseScreener(params) {
    const filters = {};
    const query = firstParam(params, ["q"]);
    const scope = firstParam(params, ["scope"]);
    const sort = firstParam(params, ["sort"]);
    const direction = firstParam(params, ["dir"]);
    const start = firstParam(params, ["start"]);
    const end = firstParam(params, ["end"]);
    if (query !== null) filters.q = query;
    const validSort = sort !== null && SCREENER_SORTS.has(sort) ? sort : null;
    const validScope = scope !== null && SCREENER_SCOPES.has(scope) ? scope : null;
    const effectiveSort = validSort || "volume";
    const rule = SCREENER_SORT_RULES[effectiveSort];
    if (validSort !== null) filters.sort = validSort;
    if (validScope !== null && rule.scopes.has(validScope)) {
      filters.scope = validScope;
    } else if (validSort !== null && rule.defaultScope !== "combined") {
      filters.scope = rule.defaultScope;
    }
    if (
      direction !== null
      && SCREENER_DIRECTIONS.has(direction)
    ) {
      filters.dir = direction;
    }
    if (isIsoDate(start)) filters.start = start;
    if (isIsoDate(end)) filters.end = end;
    return { kind: "screener", filters };
  }

  function parseWorkspaceState(page, params) {
    const state = {};
    const marketA = firstParam(params, ["marketA", "a"]);
    const marketB = firstParam(params, ["marketB", "b"]);
    const pairMode = firstParam(params, ["pairMode", "pair"]);
    const start = firstParam(params, ["start"]);
    const end = firstParam(params, ["end"]);
    if (marketA !== null) state.marketA = marketA;
    if (marketB !== null) state.marketB = marketB;
    if (pairMode !== null && PAIR_MODES.has(pairMode)) state.pairMode = pairMode;
    if (isIsoDate(start)) state.start = start;
    if (isIsoDate(end)) state.end = end;

    if (page === "compare") {
      const window = firstParam(params, ["window"]);
      if (window !== null && COMPARE_WINDOWS.has(window)) state.window = window;
    } else if (page === "liquidity") {
      const side = firstParam(params, ["side"]);
      const notional = collectedNotional(firstParam(params, ["notionalUsd", "notional"]));
      const view = firstParam(params, ["view"]);
      const scale = firstParam(params, ["scale"]);
      if (side !== null && LIQUIDITY_SIDES.has(side)) state.side = side;
      if (notional !== null) state.notionalUsd = notional;
      if (view !== null && LIQUIDITY_VIEWS.has(view)) state.view = view;
      if (scale !== null && LIQUIDITY_SCALES.has(scale)) state.scale = scale;
    } else if (page === "quality") {
      const scope = firstParam(params, ["scope"]);
      const severity = firstParam(params, ["severity"]);
      const origin = firstParam(params, ["origin"]);
      if (scope !== null && QUALITY_SCOPES.has(scope)) state.scope = scope;
      if (severity !== null && QUALITY_SEVERITIES.has(severity)) {
        state.severity = severity;
      }
      if (origin !== null && QUALITY_ORIGINS.has(origin)) state.origin = origin;
    } else if (page === "events") {
      const lifecycle = firstParam(params, ["lifecycle"]);
      const clockState = firstParam(params, ["clock_state"]);
      if (
        lifecycle !== null
        && EVENT_LIFECYCLES.has(lifecycle)
        && lifecycle !== "all"
      ) {
        state.lifecycle = lifecycle;
      }
      if (
        clockState !== null
        && EVENT_CLOCK_STATES.has(clockState)
        && clockState !== "all"
      ) {
        state.clockState = clockState;
      }
    }
    return state;
  }

  function parseRoute(pathname, search) {
    const rawPath = typeof pathname === "string" && pathname ? pathname : "/";
    const path = rawPath.length > 1 ? rawPath.replace(/\/+$/, "") : rawPath;
    const params = searchParams(search);

    if (path === "/" || path === "/screener") {
      return parseScreener(params);
    }

    const rawSegments = path.split("/").filter(Boolean);
    const segments = rawSegments.map(safeDecode);
    if (segments.some((segment) => segment === null)) {
      return { kind: "unknown", pathname: rawPath };
    }

    if (
      (segments.length === 1 || segments.length === 2)
      && segments[0] === "methodology"
    ) {
      return { kind: "screener", filters: {}, legacyMethodologyPath: true };
    }

    if (
      segments.length === 3
      && segments[0] === "tokens"
      && stringValue(segments[1]) !== null
      && WORKSPACE_PAGE_SET.has(segments[2])
    ) {
      return {
        kind: "workspace",
        token: segments[1],
        page: segments[2],
        state: parseWorkspaceState(segments[2], params),
      };
    }

    return { kind: "unknown", pathname: rawPath };
  }

  function buildScreenerPath(filters = {}) {
    const params = new URLSearchParams();
    setString(params, "q", filters.q);
    const validSort = SCREENER_SORTS.has(filters.sort) ? filters.sort : null;
    const effectiveSort = validSort || "volume";
    const rule = SCREENER_SORT_RULES[effectiveSort];
    const validScope = SCREENER_SCOPES.has(filters.scope) ? filters.scope : null;
    const normalizedScope = validScope && rule.scopes.has(validScope)
      ? validScope
      : validSort
        ? rule.defaultScope
        : null;
    if (normalizedScope !== null && normalizedScope !== "combined") {
      params.set("scope", normalizedScope);
    }
    if (validSort !== null && validSort !== "volume") params.set("sort", validSort);
    setEnum(params, "dir", filters.dir, SCREENER_DIRECTIONS);
    setDate(params, "start", filters.start);
    setDate(params, "end", filters.end);
    return withQuery("/screener", params);
  }

  function buildWorkspacePath(token, page, state = {}) {
    const normalizedToken = stringValue(token);
    if (normalizedToken === null) {
      throw new TypeError("token is required");
    }
    if (!WORKSPACE_PAGE_SET.has(page)) {
      throw new TypeError(`Unknown workspace page: ${page}`);
    }

    const params = new URLSearchParams();
    setString(params, "marketA", state.marketA);
    setString(params, "marketB", state.marketB);
    setEnum(params, "pairMode", state.pairMode, PAIR_MODES);
    setDate(params, "start", state.start);
    setDate(params, "end", state.end);

    if (page === "compare") {
      setEnum(params, "window", state.window, COMPARE_WINDOWS);
    } else if (page === "liquidity") {
      setEnum(params, "side", state.side, LIQUIDITY_SIDES);
      const notional = collectedNotional(state.notionalUsd);
      if (notional !== null) params.set("notionalUsd", String(notional));
      setEnum(params, "view", state.view, LIQUIDITY_VIEWS);
      setEnum(params, "scale", state.scale, LIQUIDITY_SCALES);
    } else if (page === "quality") {
      setEnum(params, "scope", state.scope, QUALITY_SCOPES);
      setEnum(params, "severity", state.severity, QUALITY_SEVERITIES);
      setEnum(params, "origin", state.origin, QUALITY_ORIGINS);
    } else if (page === "events") {
      if (state.lifecycle !== "all") {
        setEnum(params, "lifecycle", state.lifecycle, EVENT_LIFECYCLES);
      }
      if (state.clockState !== "all") {
        setEnum(
          params,
          "clock_state",
          state.clockState,
          EVENT_CLOCK_STATES,
        );
      }
    }

    return withQuery(
      `/tokens/${encodeURIComponent(normalizedToken)}/${page}`,
      params,
    );
  }

  function marketIdentifier(market) {
    if (typeof market === "string") return market;
    return stringValue(market?.market_id);
  }

  function validationError(code, field, value) {
    return { code, field, value: value ?? null };
  }

  function validatePair(markets, a, b) {
    const rows = Array.isArray(markets) ? markets : [];
    const byId = new Map();
    const duplicateIds = new Set();
    rows.forEach((market) => {
      const id = marketIdentifier(market);
      if (id === null) return;
      if (byId.has(id)) duplicateIds.add(id);
      else byId.set(id, market);
    });

    const marketAId = stringValue(a);
    const marketBId = stringValue(b);
    const errors = [];
    if (marketAId === null) {
      errors.push(validationError("market_a_required", "marketA", a));
    }
    if (marketBId === null) {
      errors.push(validationError("market_b_required", "marketB", b));
    }
    if (marketAId !== null && marketBId !== null && marketAId === marketBId) {
      errors.push(validationError("same_market", "marketB", marketBId));
    }
    if (marketAId !== null && !byId.has(marketAId)) {
      errors.push(validationError("market_a_not_found", "marketA", marketAId));
    }
    if (marketBId !== null && !byId.has(marketBId)) {
      errors.push(validationError("market_b_not_found", "marketB", marketBId));
    }
    if (marketAId !== null && duplicateIds.has(marketAId)) {
      errors.push(validationError("market_a_ambiguous", "marketA", marketAId));
    }
    if (marketBId !== null && duplicateIds.has(marketBId)) {
      errors.push(validationError("market_b_ambiguous", "marketB", marketBId));
    }
    if (rows.length < 2) {
      errors.push(validationError("insufficient_markets", "markets", rows.length));
    }

    return {
      valid: errors.length === 0,
      marketA: marketAId !== null && !duplicateIds.has(marketAId)
        ? byId.get(marketAId) ?? null
        : null,
      marketB: marketBId !== null && !duplicateIds.has(marketBId)
        ? byId.get(marketBId) ?? null
        : null,
      errors,
    };
  }

  return Object.freeze({
    WORKSPACE_PAGES,
    parseRoute,
    buildScreenerPath,
    buildWorkspacePath,
    validatePair,
  });
});
