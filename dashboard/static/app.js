const app = {
  payload: null,
  catalog: null,
  comparison: null,
  scope: "combined",
  selections: {},
  selectionOverrides: {},
  searchQuery: "",
  visibleTokens: [],
  marketRequestId: 0,
  comparisonRequestId: 0,
  marketController: null,
  comparisonController: null,
};

const DEFAULT_MARKET_CACHE_KEY = "market-monitor:default-payload:v2";
const DEPTH_BANDS = [10, 25, 50, 100];
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
  const suppliedDetails = Array.isArray(row?.quality_flag_details)
    ? row.quality_flag_details.map((flag) => ({
        code: flag.code,
        severity: flag.severity || "warning",
        explanation: flag.explanation || flag.message || "",
      }))
    : [];
  const suppliedCodes = Array.isArray(row?.quality_flags)
    ? row.quality_flags.map((flag) => (
        typeof flag === "string"
          ? { code: flag, severity: "warning", explanation: "" }
          : {
              code: flag.code,
              severity: flag.severity || "warning",
              explanation: flag.explanation || flag.message || "",
            }
      ))
    : [];
  const flags = [...suppliedDetails, ...suppliedCodes];
  const add = (code, severity, explanation) => {
    if (!flags.some((flag) => flag.code === code)) flags.push({ code, severity, explanation });
  };
  const status = market === "cex" ? row?.depth_status : row?.dex_depth_status;
  if (status === "unsupported") add("depth_unsupported", "warning", row?.dex_depth_error || "");
  if (status === "partial") add("depth_partial", "warning", "Returned levels are a lower bound.");
  if (status === "failed") add("depth_failed", "critical", "Depth collection failed.");
  if (status === "not_cataloged_in_snapshot") {
    add("depth_not_cataloged", "warning", "Market was not present in the latest depth snapshot.");
  }
  if (
    status === "observed"
    && row?.total_depth_10bps_usd === 0
  ) {
    add(
      "zero_depth_inside_spread",
      "warning",
      "The ±10 bps band may lie inside the quoted spread.",
    );
  }
  const thresholds = app.payload?.metadata?.market_quality_thresholds
    || app.payload?.metadata?.quality_thresholds
    || {};
  const tinyPoolThreshold = thresholds.tiny_pool_tvl_usd ?? 100_000;
  if (market === "dex" && finite(row?.tvl_usd) && row.tvl_usd < tinyPoolThreshold) {
    add("tiny_pool", "warning", `TVL is below ${formatCurrency(tinyPoolThreshold)}.`);
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
    );
  }
  const wideSpreadThreshold = thresholds.wide_cex_quoted_spread_bps ?? 100;
  if (
    market === "cex"
    && finite(row?.spread_bps)
    && row.spread_bps > wideSpreadThreshold
  ) {
    add("wide_quoted_spread", "warning", `Quoted spread is ${bpsFormat.format(row.spread_bps)} bps.`);
  }
  if (finite(row?.coverage_ratio) && row.coverage_ratio < 0.8) {
    add("low_daily_coverage", "warning", `Daily coverage is ${formatRatio(row.coverage_ratio)}.`);
  }
  return flags.filter(
    (flag, index, values) => values.findIndex((candidate) => candidate.code === flag.code) === index,
  );
}

function renderQualityBadges(flags) {
  if (!flags.length) return '<span class="quality-flag good">No quality flags</span>';
  return flags.map((flag) => {
    const severityClass = flag.severity === "critical" ? "danger" : "warn";
    const label = QUALITY_FLAG_LABELS[flag.code]
      || flag.code.replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase());
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
  const quality = market.quality_status && market.quality_status !== "ok"
    ? ` · ${market.quality_status}`
    : "";
  return `${type} · ${market.venue} · ${market.instrument}${quality}`;
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
  return [...markets]
    .filter((market) => market.market_type === type)
    .sort((a, b) => (
      catalogMarketScore(b, tokenSummary) - catalogMarketScore(a, tokenSummary)
      || a.market_id.localeCompare(b.market_id)
    ))[0];
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
  if (payload.tokens.includes(currentToken)) byId("facts-token").value = currentToken;
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
    loadComparison();
  });
  byId("facts-market-b").addEventListener("change", () => {
    if (byId("facts-market-a").value === byId("facts-market-b").value) {
      const alternatives = factsMarketsForToken(byId("facts-token").value)
        .filter((market) => market.market_id !== byId("facts-market-b").value);
      if (alternatives.length) byId("facts-market-a").value = alternatives[0].market_id;
    }
    loadComparison();
  });
  byId("compare-markets").addEventListener("click", loadComparison);
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

initialize();
