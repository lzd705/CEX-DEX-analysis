const app = {
  payload: null,
  catalog: null,
  comparison: null,
  scope: "combined",
  liquidityView: "total",
  liquidityScale: "log",
  liquidityEffectiveScale: null,
  liquidityEffectiveScaleLabel: "",
  selections: {},
  selectionOverrides: {},
  searchQuery: "",
  visibleTokens: [],
  marketRequestId: 0,
  comparisonRequestId: 0,
  marketController: null,
  comparisonController: null,
  liquidityLayoutMode: null,
  liquidityResizeScheduled: false,
  liquidityResizeObserver: null,
};

const DEFAULT_MARKET_CACHE_KEY = "market-monitor:default-payload:v2";
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

function selectOptions(rows, selectedId) {
  return rows.map((row) => {
    const id = marketId(row);
    return `<option value="${escapeHtml(id)}" ${id === selectedId ? "selected" : ""}>`
      + `${escapeHtml(row.venue)} · ${escapeHtml(row.instrument)}</option>`;
  }).join("");
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

function depthCell(row, market) {
  const status = market === "cex" ? row?.depth_status : row?.dex_depth_status;
  if (!row || !DEPTH_BANDS.some((band) => finite(row[`total_depth_${band}bps_usd`]))) {
    const reason = market === "dex" ? row?.dex_depth_error : "";
    return `<span class="missing">${escapeHtml(status || "unavailable")}</span>`
      + `${reason ? `<span class="metric-note">${escapeHtml(reason)}</span>` : ""}`;
  }
  const sideA = market === "cex" ? "bid" : "buy";
  const sideB = market === "cex" ? "ask" : "sell";
  const sideALabel = market === "cex" ? "Bid" : "Buy";
  const sideBLabel = market === "cex" ? "Ask" : "Sell";
  const rows = DEPTH_BANDS.map((band) => {
    const total = row[`total_depth_${band}bps_usd`];
    const complete = Boolean(row[`depth_${band}bps_complete`]);
    const sideAValue = row[`${sideA}_depth_${band}bps_usd`];
    const sideBValue = row[`${sideB}_depth_${band}bps_usd`];
    const sides = finite(sideAValue) || finite(sideBValue)
      ? `${sideALabel} ${formatCurrency(sideAValue)} · ${sideBLabel} ${formatCurrency(sideBValue)}`
      : "Directional split unavailable";
    return `<span>±${band} bps</span><strong>${formatDepth(total, complete)}</strong>`
      + `<span class="metric-note">${escapeHtml(sides)}</span><span></span>`;
  }).join("");
  const observedAt = market === "cex" ? row.depth_observed_at : row.dex_depth_observed_at;
  const model = market === "cex"
    ? row.depth_method
    : row.dex_depth_protocol_model || row.dex_depth_method;
  const block = market === "dex" && row.dex_depth_block_number
    ? ` · block ${row.dex_depth_block_number}`
    : "";
  return `<details class="depth-details">
      <summary>±100 bps ${formatDepth(row.total_depth_100bps_usd, row.depth_100bps_complete)}</summary>
      <div class="depth-grid">${rows}</div>
      <span class="metric-note">${escapeHtml(status || "unavailable")} · ${escapeHtml(formatUtcTimestamp(observedAt))}</span>
      <span class="metric-note">${escapeHtml(model || "model unavailable")}${escapeHtml(block)}</span>
    </details>`;
}

function coverageCell(row, market) {
  const firstDate = row.first_observed_date
    || row.first_date
    || row.observed_start
    || row.price_points?.[0]?.date
    || "N/A";
  const latestDate = row.latest_observed_date || row.latest_date || row.observed_end || "N/A";
  const calendarDays = row.calendar_span_days || row.window_calendar_days;
  const coverage = firstFinite(
    row.coverage_ratio,
    row.observation_coverage_ratio,
    finite(calendarDays) && calendarDays > 0 ? row.observation_days / calendarDays : null,
  );
  const intervalCount = firstFinite(row.return_interval_count, row.valid_return_intervals);
  const gapCount = firstFinite(
    row.skipped_gap_interval_count,
    row.gap_interval_count,
    row.missing_interval_count,
  );
  const flags = qualityFlagObjects(row, market);
  const intervalText = finite(intervalCount)
    ? `${intervalCount} daily returns${finite(gapCount) ? ` · ${gapCount} gaps excluded` : ""}`
    : "Return interval detail unavailable";
  return `<span>${escapeHtml(firstDate)} → ${escapeHtml(latestDate)}</span>
    <span class="metric-note">${row.observation_days ?? 0} observations · ${formatRatio(coverage)} coverage</span>
    <span class="metric-note">${escapeHtml(intervalText)}</span>
    <span class="quality-badges">${renderQualityBadges(flags)}</span>`;
}

function marketRow(token, market, row, options) {
  const label = market === "cex" ? "CEX" : "DEX";
  if (!row) {
    return `<tr class="market-row ${market}">
      <td data-label="Market" class="sticky-token market-label">${label}</td>
      <td data-label="Status" colspan="10" class="missing">No observations in this window</td>
    </tr>`;
  }
  const tvl = market === "dex" ? formatCurrency(row.tvl_usd) : "Not applicable";
  const tvlTitle = market === "dex"
    ? `${row.tvl_status || "unavailable"} · ${formatUtcTimestamp(row.tvl_observed_at)}`
    : "TVL is not applicable to CEX order books";
  const returnMethod = row.window_return_method
    || row.return_method
    || "first/last finite close";
  const volatilityMethod = row.daily_volatility_method
    || row.volatility_method
    || "adjacent UTC daily close log returns";
  return `<tr class="market-row ${market}">
    <td data-label="Market" class="sticky-token market-label"><span class="market-dot"></span>${label}</td>
    <td data-label="Selected market" colspan="2">
      <select
        class="venue-select"
        data-token="${escapeHtml(token)}"
        data-market="${market}"
        aria-label="${escapeHtml(`${token} selected ${label} market`)}"
      >
        ${selectOptions(options, marketId(row))}
      </select>
      ${row.selection_reason ? `<span class="metric-note">${escapeHtml(row.selection_reason)}</span>` : ""}
    </td>
    <td data-label="Latest price" class="price-cell">${formatPrice(row.price_usd)}</td>
    <td data-label="Window return" class="${metricClass(row.window_return)}">
      ${formatPercent(row.window_return)}
      <span class="metric-note">${escapeHtml(returnMethod)}</span>
    </td>
    <td data-label="Daily volatility">
      ${formatPercent(row.daily_volatility)}
      <span class="metric-note">${escapeHtml(volatilityMethod)}</span>
    </td>
    <td data-label="Selected market volume">
      ${formatCurrency(row.volume_usd)}
      <span class="metric-note">Selected market only</span>
    </td>
    <td data-label="TVL snapshot" title="${escapeHtml(tvlTitle)}">${tvl}</td>
    <td data-label="Depth bands">${depthCell(row, market)}</td>
    <td data-label="Selected spread" class="not-applicable">Shown on Token row</td>
    <td data-label="Coverage & quality">${coverageCell(row, market)}</td>
  </tr>`;
}

function primarySelectionText(label, reason) {
  if (typeof reason === "string") return `${label}: ${reason}`;
  if (!reason || typeof reason !== "object") {
    return `${label}: primary selection favors data quality, coverage, and volume`;
  }
  const score = finite(reason.score) ? `${reason.score.toFixed(1)}/100` : "score unavailable";
  const count = finite(reason.candidate_count)
    ? `${reason.candidate_count} candidate${reason.candidate_count === 1 ? "" : "s"}`
    : "candidate count unavailable";
  return `${label} ${score} (${count})`;
}

function selectionAuditText(label, selected, primaryId, reason) {
  if (selected && primaryId && marketId(selected) !== primaryId) {
    return `${label} user-selected (not current primary)`;
  }
  return primarySelectionText(label, reason);
}

function tokenRows(tokenSummary, cexOptions, dexOptions) {
  const token = tokenSummary.token_symbol;
  const { cex, dex, spread, spreadDate, cexSpreadPrice, dexSpreadPrice } = comparison(tokenSummary);
  const aggregates = aggregateFacts(tokenSummary, cexOptions, dexOptions);
  const selectedVolume = (cex?.volume_usd ?? 0) + (dex?.volume_usd ?? 0);
  const cexDepth = formatDepth(cex?.total_depth_100bps_usd, cex?.depth_100bps_complete);
  const dexDepth = formatDepth(dex?.total_depth_100bps_usd, dex?.depth_100bps_complete);
  const cexSelectionReason = tokenSummary.primary_cex_selection_reason
    || tokenSummary.selection_reason
    || tokenSummary.primary_selection_reason
    || "Primary selection favors data quality, coverage, and volume.";
  const dexSelectionReason = tokenSummary.primary_dex_selection_reason
    || tokenSummary.selection_reason
    || tokenSummary.primary_selection_reason
    || "Primary selection favors data quality, coverage, and volume.";
  const selectionReason = [
    selectionAuditText("CEX", cex, tokenSummary.primary_cex_id, cexSelectionReason),
    selectionAuditText("DEX", dex, tokenSummary.primary_dex_id, dexSelectionReason),
  ].join(" · ");
  return `<tr class="token-row">
      <td data-label="Token" class="sticky-token token-name">${escapeHtml(token)}</td>
      <td data-label="Scope">All cataloged markets</td>
      <td data-label="Aggregate DEX share">
        <span class="share-label">Aggregate DEX ${formatShare(aggregates.aggregateDexShare)}</span>
        <span class="metric-note">${cexOptions.length} CEX · ${dexOptions.length} DEX series</span>
      </td>
      <td data-label="Selected comparable prices" class="price-cell">
        <span class="paired-value">${formatPrice(cexSpreadPrice)} / ${formatPrice(dexSpreadPrice)}</span>
        <span class="metric-note">Selected CEX / selected DEX</span>
      </td>
      <td data-label="Window return">See selected market rows</td>
      <td data-label="Daily volatility">See selected market rows</td>
      <td data-label="Aggregate USD volume">
        ${formatCurrency(aggregates.aggregateTotal)}
        <span class="metric-note">All CEX ${formatCurrency(aggregates.aggregateCex)} · all DEX ${formatCurrency(aggregates.aggregateDex)}</span>
        <span class="metric-note">Selected pair total ${formatCurrency(selectedVolume)}</span>
      </td>
      <td data-label="Selected DEX TVL">${formatCurrency(dex?.tvl_usd)}</td>
      <td data-label="Selected depth">
        <span class="paired-value">${cexDepth} / ${dexDepth}</span>
        <span class="metric-note">Selected CEX / DEX at ±100 bps</span>
      </td>
      <td data-label="Selected DEX/CEX spread" class="${metricClass(spread)} spread-value">
        ${formatPercent(spread)}
        <span class="spread-date">${spreadDate || "No common date"}</span>
      </td>
      <td data-label="Selection audit">
        <span>${escapeHtml(selectionReason)}</span>
        <span class="metric-note">Primary score is quality-weighted; declared weights are in the data contract.</span>
        <span class="metric-note">Spread formula: DEX / CEX − 1</span>
      </td>
    </tr>
    ${marketRow(token, "cex", cex, cexOptions)}
    ${marketRow(token, "dex", dex, dexOptions)}`;
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
    ? tokens.map((token) => tokenRows(
        token,
        cexByToken[token.token_symbol] || [],
        dexByToken[token.token_symbol] || [],
      )).join("")
    : `<tr><td data-label="Result" colspan="11" class="missing">No Token matches this search.</td></tr>`;
  byId("row-count").textContent = `${tokens.length} Tokens · ${tokens.length * 3} fact rows`;

  document.querySelectorAll(".venue-select").forEach((select) => {
    select.addEventListener("change", () => {
      app.selections[select.dataset.token][select.dataset.market] = select.value;
      if (!app.selectionOverrides[select.dataset.token]) {
        app.selectionOverrides[select.dataset.token] = {};
      }
      app.selectionOverrides[select.dataset.token][select.dataset.market] = true;
      renderTable();
    });
  });
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
}

function factsMarketLabel(market) {
  const type = market.market_type.toUpperCase();
  return `${type} · ${market.venue} · ${market.instrument}`;
}

function factsMarketsForToken(token) {
  return app.catalog.markets.filter((market) => market.token_symbol === token);
}

function factsOptions(markets, selectedId) {
  return markets.map((market) => (
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
  return `
    <div class="market-warning-tooltip-heading">
      <strong>${escapeHtml(slotLabel)} · ${escapeHtml(severity)}</strong>
      <span>${flags.length} ${alertLabel}</span>
    </div>
    <div class="market-warning-market">${escapeHtml(factsMarketLabel(market))}</div>
    <ul>${items}</ul>
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
    if (event.key === "Escape") closeFactsMarketWarnings();
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

function populateFactsMarkets({ preserve = false } = {}) {
  if (!app.catalog) return;
  const token = byId("facts-token").value;
  const markets = factsMarketsForToken(token);
  const tokenSummary = app.payload?.tokens.find((row) => row.token_symbol === token);
  const previousA = preserve ? byId("facts-market-a").value : "";
  const previousB = preserve ? byId("facts-market-b").value : "";
  const cex = preferredCatalogMarket(markets, "cex", tokenSummary);
  const dex = preferredCatalogMarket(markets, "dex", tokenSummary);
  const marketA = markets.find((market) => market.market_id === previousA) || cex || markets[0];
  let marketB = markets.find((market) => (
    market.market_id === previousB && market.market_id !== marketA?.market_id
  )) || dex || markets.find((market) => market.market_id !== marketA?.market_id);
  if (marketB?.market_id === marketA?.market_id) {
    marketB = markets.find((market) => market.market_id !== marketA.market_id);
  }
  byId("facts-market-a").innerHTML = factsOptions(markets, marketA?.market_id);
  byId("facts-market-b").innerHTML = factsOptions(markets, marketB?.market_id);
  renderFactsMarketWarnings();
  renderLiquidityCurve();
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
  ["compare-date", "compare-absolute", "compare-bps", "compare-days"].forEach((id) => {
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
  ["compare-date", "compare-absolute", "compare-bps", "compare-days"].forEach((id) => {
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
  hideError(byId("error-banner"));
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
    byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="11" class="missing">Loading the requested time window…</td></tr>';
    byId("row-count").textContent = "Loading…";
  }
}

function invalidateMarketRequest() {
  if (app.marketController) app.marketController.abort();
  app.marketController = null;
  app.marketRequestId += 1;
  return app.marketRequestId;
}

function clearMarketResult(message = "") {
  app.payload = null;
  app.visibleTokens = [];
  byId("market-body").innerHTML = '<tr><td data-label="Status" colspan="11" class="missing">No current market result.</td></tr>';
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
    return false;
  } finally {
    if (requestId === app.marketRequestId) {
      app.marketController = null;
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
  await Promise.allSettled([
    loadMarket(start, end),
    loadComparison(),
  ]);
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
    });
  });
  byId("sort-field").addEventListener("change", renderTable);
  byId("search-token").addEventListener("click", applyTokenSearch);
  byId("token-search").addEventListener("keydown", (event) => {
    if (event.key === "Enter") applyTokenSearch();
  });
  byId("token-search").addEventListener("search", applyTokenSearch);
  byId("facts-token").addEventListener("change", () => {
    populateFactsMarkets();
    loadComparison();
  });
  byId("facts-market-a").addEventListener("change", () => {
    if (byId("facts-market-a").value === byId("facts-market-b").value) {
      const alternatives = factsMarketsForToken(byId("facts-token").value)
        .filter((market) => market.market_id !== byId("facts-market-a").value);
      if (alternatives.length) byId("facts-market-b").value = alternatives[0].market_id;
    }
    renderFactsMarketWarnings();
    renderLiquidityCurve();
    loadComparison();
  });
  byId("facts-market-b").addEventListener("change", () => {
    if (byId("facts-market-a").value === byId("facts-market-b").value) {
      const alternatives = factsMarketsForToken(byId("facts-token").value)
        .filter((market) => market.market_id !== byId("facts-market-b").value);
      if (alternatives.length) byId("facts-market-a").value = alternatives[0].market_id;
    }
    renderFactsMarketWarnings();
    renderLiquidityCurve();
    loadComparison();
  });
  document.querySelectorAll("[data-liquidity-view]").forEach((button) => {
    button.addEventListener("click", () => {
      app.liquidityView = button.dataset.liquidityView;
      document.querySelectorAll("[data-liquidity-view]").forEach((item) => {
        const active = item === button;
        item.classList.toggle("active", active);
        item.setAttribute("aria-pressed", String(active));
      });
      renderLiquidityCurve();
    });
  });
  document.querySelectorAll("[data-liquidity-scale]").forEach((button) => {
    button.addEventListener("click", () => {
      app.liquidityScale = button.dataset.liquidityScale;
      document.querySelectorAll("[data-liquidity-scale]").forEach((item) => {
        const active = item === button;
        item.classList.toggle("active", active);
        item.setAttribute("aria-pressed", String(active));
      });
      renderLiquidityCurve();
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
    renderLiquidityCurve();
    loadComparison();
  });
  byId("export-csv").addEventListener("click", exportVisibleCsv);
}

async function initialize() {
  bindEvents();
  const cachedPayload = readDefaultMarketCache();
  if (cachedPayload) displayMarket(cachedPayload, { cached: true });
  const [marketResult, catalogResult] = await Promise.allSettled([
    loadMarket("", "", { preserve: Boolean(cachedPayload) }),
    loadCatalog(),
  ]);
  if (marketResult.status === "rejected") {
    showError(byId("error-banner"), marketResult.reason?.message || String(marketResult.reason));
  }
  if (catalogResult.status === "rejected") {
    showError(byId("comparison-error"), catalogResult.reason?.message || String(catalogResult.reason));
    byId("facts-workbench").setAttribute("aria-busy", "false");
  } else {
    populateFactsMarkets();
    await loadComparison();
  }
  if (window.lucide) window.lucide.createIcons();
}

if (typeof document !== "undefined") initialize();
