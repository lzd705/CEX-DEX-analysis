const app = {
  payload: null,
  catalog: null,
  comparison: null,
  scope: "combined",
  selections: {},
  searchQuery: "",
};
const DEFAULT_MARKET_CACHE_KEY = "market-monitor:default-payload:v1";

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

function formatPrice(value) {
  if (!finite(value)) return "N/A";
  return `$${priceFormat.format(value)}`;
}

function formatCurrency(value) {
  return finite(value) ? compactCurrency.format(value) : "N/A";
}

function formatPercent(value) {
  return finite(value) ? percent.format(value) : "N/A";
}

function formatRawUsd(value) {
  return finite(value) ? `$${rawUsd.format(value)}` : "N/A";
}

function formatRawVolume(value) {
  return finite(value) ? `$${rawVolume.format(value)}` : "N/A";
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

function ensureSelections() {
  const cexByToken = grouped(app.payload.cex_markets);
  const dexByToken = grouped(app.payload.dex_pools);
  app.payload.tokens.forEach((token) => {
    const symbol = token.token_symbol;
    if (!app.selections[symbol]) app.selections[symbol] = {};
    const cexIds = (cexByToken[symbol] || []).map(marketId);
    const dexIds = (dexByToken[symbol] || []).map(marketId);
    if (!cexIds.includes(app.selections[symbol].cex)) {
      app.selections[symbol].cex = token.primary_cex_id || cexIds[0] || null;
    }
    if (!dexIds.includes(app.selections[symbol].dex)) {
      app.selections[symbol].dex = token.primary_dex_id || dexIds[0] || null;
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

function sortValue(tokenSummary) {
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
  if (app.scope === "cex") return cex?.volume_usd ?? 0;
  if (app.scope === "dex") return dex?.volume_usd ?? 0;
  return (cex?.volume_usd ?? 0) + (dex?.volume_usd ?? 0);
}

function selectOptions(rows, selectedId) {
  return rows.map((row) => {
    const id = marketId(row);
    const label = row.market === "cex"
      ? `${row.venue} · ${row.instrument}`
      : `${row.venue} · ${row.instrument}`;
    return `<option value="${escapeHtml(id)}" ${id === selectedId ? "selected" : ""}>${escapeHtml(label)}</option>`;
  }).join("");
}

function metricClass(value) {
  if (!finite(value) || value === 0) return "";
  return value > 0 ? "positive" : "negative";
}

function marketRow(token, market, row, options) {
  const label = market === "cex" ? "CEX" : "DEX";
  const tvl = market === "dex" ? formatCurrency(row?.tvl_usd) : "--";
  if (!row) {
    return `<tr class="market-row ${market}"><td class="sticky-token market-label">${label}</td><td colspan="9" class="missing">No observations in this window</td></tr>`;
  }
  return `<tr class="market-row ${market}">
    <td class="sticky-token market-label"><span class="market-dot"></span>${label}</td>
    <td colspan="2">
      <select class="venue-select" data-token="${escapeHtml(token)}" data-market="${market}">
        ${selectOptions(options, marketId(row))}
      </select>
    </td>
    <td>${formatPrice(row.price_usd)}</td>
    <td class="${metricClass(row.window_return)}">${formatPercent(row.window_return)}</td>
    <td>${formatPercent(row.daily_volatility)}</td>
    <td>${formatCurrency(row.volume_usd)}</td>
    <td>${tvl}</td>
    <td class="not-applicable">--</td>
    <td>${row.observation_days}D · ${escapeHtml(row.latest_date)}</td>
  </tr>`;
}

function tokenRows(tokenSummary, cexOptions, dexOptions) {
  const token = tokenSummary.token_symbol;
  const { cex, dex, spread, spreadDate, cexSpreadPrice, dexSpreadPrice } = comparison(tokenSummary);
  const combinedVolume = (cex?.volume_usd ?? 0) + (dex?.volume_usd ?? 0);
  const observedShare = combinedVolume ? (dex?.volume_usd ?? 0) / combinedVolume : null;
  return `<tr class="token-row">
      <td class="sticky-token token-name">${escapeHtml(token)}</td>
      <td>Selected markets</td>
      <td><span class="share-label">Observed DEX ${formatPercent(observedShare)}</span></td>
      <td><span class="paired-value">${formatPrice(cexSpreadPrice)} / ${formatPrice(dexSpreadPrice)}</span></td>
      <td>--</td>
      <td>--</td>
      <td>${formatCurrency(combinedVolume)}</td>
      <td>${formatCurrency(dex?.tvl_usd)}</td>
      <td class="${metricClass(spread)} spread-value">${formatPercent(spread)}<span class="spread-date">${spreadDate || ""}</span></td>
      <td>${cexOptions.length} CEX · ${dexOptions.length} pools</td>
    </tr>
    ${marketRow(token, "cex", cex, cexOptions)}
    ${marketRow(token, "dex", dex, dexOptions)}`;
}

function renderTable() {
  ensureSelections();
  const query = app.searchQuery;
  const cexByToken = grouped(app.payload.cex_markets);
  const dexByToken = grouped(app.payload.dex_pools);
  const tokens = app.payload.tokens
    .filter((row) => !query || row.token_symbol.includes(query))
    .sort((a, b) => sortValue(b) - sortValue(a) || a.token_symbol.localeCompare(b.token_symbol));

  byId("market-body").innerHTML = tokens.map((token) => tokenRows(
    token,
    cexByToken[token.token_symbol] || [],
    dexByToken[token.token_symbol] || [],
  )).join("");
  byId("row-count").textContent = `${tokens.length} tokens · ${tokens.length * 3} rows`;

  document.querySelectorAll(".venue-select").forEach((select) => {
    select.addEventListener("change", () => {
      app.selections[select.dataset.token][select.dataset.market] = select.value;
      renderTable();
    });
  });
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
  byId("freshness").textContent = `Data through ${metadata.available_end}`;
  const sourceText = metadata.sources
    .map((source) => `${source.name} · ${source.sha256}`)
    .join(" | ");
  const storage = metadata.storage || { engine: "csv" };
  const storageText = storage.engine === "sqlite"
    ? `SQLite snapshot · ${storage.snapshot_id}`
    : "CSV fallback";
  byId("source-list").textContent = `${storageText} | ${sourceText}`;
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

function populateFactsMarkets({ preserve = false } = {}) {
  if (!app.catalog) return;
  const token = byId("facts-token").value;
  const markets = factsMarketsForToken(token);
  const previousA = preserve ? byId("facts-market-a").value : "";
  const previousB = preserve ? byId("facts-market-b").value : "";
  const cex = markets.find((market) => market.market_type === "cex");
  const dex = markets.find((market) => market.market_type === "dex");
  const marketA = markets.find((market) => market.market_id === previousA) || cex || markets[0];
  let marketB = markets.find((market) => market.market_id === previousB && market.market_id !== marketA?.market_id)
    || dex
    || markets.find((market) => market.market_id !== marketA?.market_id);
  if (marketB?.market_id === marketA?.market_id) {
    marketB = markets.find((market) => market.market_id !== marketA.market_id);
  }
  byId("facts-market-a").innerHTML = factsOptions(markets, marketA?.market_id);
  byId("facts-market-b").innerHTML = factsOptions(markets, marketB?.market_id);
}

function updateFactsContract() {
  const metadata = app.catalog.metadata;
  byId("facts-contract-copy").textContent = [
    `Grain: ${metadata.time_grain}.`,
    `Price: ${metadata.price_field}, quoted in ${metadata.price_quote_asset}.`,
    `Volume: daily USD.`,
    `Missing values: ${metadata.missing_value_rule}`,
    metadata.semantic_boundary,
  ].join(" ");
  byId("facts-source-copy").textContent = [
    `Catalog v${metadata.catalog_version}.`,
    metadata.cex_normalization_note,
    `Sources: ${metadata.sources.map((source) => `${source.name} (${source.sha256})`).join(" | ")}.`,
  ].join(" ");
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
    : `<tr><td colspan="8" class="missing">No observations in this window</td></tr>`;
  byId("comparison-error").hidden = true;
}

async function loadComparison() {
  if (!app.catalog) return;
  const token = byId("facts-token").value;
  const marketA = byId("facts-market-a").value;
  const marketB = byId("facts-market-b").value;
  if (!token || !marketA || !marketB) return;
  const query = new URLSearchParams({
    token,
    market_a: marketA,
    market_b: marketB,
  });
  if (byId("date-start").value) query.set("start", byId("date-start").value);
  if (byId("date-end").value) query.set("end", byId("date-end").value);
  byId("compare-markets").disabled = true;
  try {
    const response = await fetch(`/api/markets/compare?${query.toString()}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Comparison failed to load");
    renderComparison(payload);
  } catch (error) {
    byId("comparison-error").hidden = false;
    byId("comparison-error").textContent = error.message || String(error);
  } finally {
    byId("compare-markets").disabled = false;
  }
}

async function loadCatalog() {
  const response = await fetch("/api/markets/catalog");
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Market catalog failed to load");
  app.catalog = payload;
  byId("facts-token").innerHTML = payload.tokens
    .map((token) => `<option value="${escapeHtml(token)}">${escapeHtml(token)}</option>`)
    .join("");
  populateFactsMarkets();
  updateFactsContract();
  await loadComparison();
}

function showError(error) {
  byId("error-banner").hidden = false;
  byId("error-banner").textContent = error.message || String(error);
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
    // The network response still renders when browser storage is unavailable.
  }
}

function displayMarket(payload, { cached = false } = {}) {
  app.payload = payload;
  byId("error-banner").hidden = true;
  updateMetadata();
  if (cached) {
    byId("freshness").textContent = `Cached through ${payload.metadata.available_end} · refreshing`;
  }
  renderTable();
}

async function loadMarket(start = "", end = "") {
  byId("apply-window").disabled = true;
  const query = new URLSearchParams();
  if (start) query.set("start", start);
  if (end) query.set("end", end);
  try {
    const response = await fetch(`/api/market?${query.toString()}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Market data failed to load");
    displayMarket(payload);
    if (!start && !end) writeDefaultMarketCache(payload);
  } finally {
    byId("apply-window").disabled = false;
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

function bindEvents() {
  const applyTokenSearch = () => {
    app.searchQuery = byId("token-search").value.trim().toUpperCase();
    renderTable();
  };
  byId("apply-window").addEventListener("click", () => {
    loadMarket(byId("date-start").value, byId("date-end").value).catch(showError);
    loadComparison();
  });
  document.querySelectorAll("[data-days]").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("[data-days]").forEach((item) => item.classList.toggle("active", item === button));
      setPreset(button.dataset.days);
      loadMarket(byId("date-start").value, byId("date-end").value).catch(showError);
      loadComparison();
    });
  });
  document.querySelectorAll("[data-scope]").forEach((button) => {
    button.addEventListener("click", () => {
      app.scope = button.dataset.scope;
      document.querySelectorAll("[data-scope]").forEach((item) => item.classList.toggle("active", item === button));
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
  });
  byId("facts-market-b").addEventListener("change", () => {
    if (byId("facts-market-a").value === byId("facts-market-b").value) {
      const alternatives = factsMarketsForToken(byId("facts-token").value)
        .filter((market) => market.market_id !== byId("facts-market-b").value);
      if (alternatives.length) byId("facts-market-a").value = alternatives[0].market_id;
    }
  });
  byId("compare-markets").addEventListener("click", loadComparison);
}

bindEvents();
const cachedPayload = readDefaultMarketCache();
if (cachedPayload) displayMarket(cachedPayload, { cached: true });
loadMarket().catch(showError);
loadCatalog().catch(showError);
if (window.lucide) window.lucide.createIcons();
