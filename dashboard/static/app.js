const palette = ["#0f7c82", "#356fb6", "#c76d1d", "#aa4d64", "#477a59", "#6b5b95", "#8a6f30", "#487b9a", "#8b5d52", "#586574"];

const app = {
  payload: null,
  state: null,
  history: [],
  charts: {},
  factorHorizon: "7d",
};

const byId = (id) => document.getElementById(id);
const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 2 });
const number = new Intl.NumberFormat("en", { maximumFractionDigits: 2 });
const percent = new Intl.NumberFormat("en", { style: "percent", maximumFractionDigits: 2 });

function median(values) {
  const clean = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!clean.length) return null;
  const middle = Math.floor(clean.length / 2);
  return clean.length % 2 ? clean[middle] : (clean[middle - 1] + clean[middle]) / 2;
}

function sum(values) {
  return values.filter(Number.isFinite).reduce((total, value) => total + value, 0);
}

function formatValue(value, format = "number") {
  if (!Number.isFinite(value)) return "N/A";
  if (format === "currency") return `$${compact.format(value)}`;
  if (format === "percent") return percent.format(value);
  if (format === "ratio") return `${number.format(value)}x`;
  if (format === "integer") return String(Math.round(value));
  return number.format(value);
}

function selectedTokens() {
  return [...document.querySelectorAll("#token-list input:checked")].map((input) => input.value);
}

function selectedMetric() {
  return byId("metric-select").value;
}

function filteredRows() {
  const tokens = new Set(selectedTokens());
  const start = byId("date-start").value;
  const end = byId("date-end").value;
  return app.payload.rows.filter((row) => {
    return tokens.has(row.token_symbol) && (!start || row.date >= start) && (!end || row.date <= end);
  });
}

function groupedBy(rows, key) {
  return rows.reduce((groups, row) => {
    const value = row[key];
    if (!groups[value]) groups[value] = [];
    groups[value].push(row);
    return groups;
  }, {});
}

function normalizedSeries(rows, metric) {
  const values = rows.map((row) => row[metric]).filter(Number.isFinite);
  const center = values.reduce((total, value) => total + value, 0) / (values.length || 1);
  const variance = values.reduce((total, value) => total + (value - center) ** 2, 0) / Math.max(values.length - 1, 1);
  const deviation = Math.sqrt(variance) || 1;
  return rows.map((row) => ({ ...row, display_value: Number.isFinite(row[metric]) ? (row[metric] - center) / deviation : null }));
}

function replaceChart(name, canvasId, config) {
  if (app.charts[name]) app.charts[name].destroy();
  app.charts[name] = new Chart(byId(canvasId), config);
}

function chartDefaults() {
  Chart.defaults.color = "#68737f";
  Chart.defaults.font.family = 'Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
  Chart.defaults.font.size = 10;
  Chart.defaults.borderColor = "#d9dee4";
}

function renderTokens() {
  const selected = new Set(app.state.selected_tokens?.length ? app.state.selected_tokens : app.payload.tokens.slice(0, 3));
  byId("token-list").innerHTML = app.payload.tokens.map((token) => `
    <label class="token-chip">
      <input type="checkbox" value="${token}" ${selected.has(token) ? "checked" : ""}>
      <span>${token}</span>
    </label>`).join("");
}

function renderMetricOptions() {
  const grouped = Object.entries(app.payload.metrics).reduce((groups, [key, metric]) => {
    const category = metric.category || "其他";
    if (!groups[category]) groups[category] = [];
    groups[category].push([key, metric]);
    return groups;
  }, {});
  byId("metric-select").innerHTML = Object.entries(grouped).map(([category, metrics]) =>
    `<optgroup label="${category}">${metrics.map(([key, metric]) => `<option value="${key}">${metric.label}</option>`).join("")}</optgroup>`
  ).join("");
  const stateMetric = app.state.metric;
  byId("metric-select").value = app.payload.metrics[stateMetric] ? stateMetric : "dex_share";
}

function renderKpis(rows) {
  const cex = sum(rows.map((row) => row.cex_volume_usd));
  const dex = sum(rows.map((row) => row.dex_volume_usd));
  const observedShare = cex + dex ? dex / (cex + dex) : null;
  const ratio = dex ? cex / dex : null;
  const latestDate = rows.map((row) => row.date).sort().at(-1);
  const cards = [
    ["CEX 成交额", formatValue(cex, "currency"), `${rows.length} token-day 合计`],
    ["DEX 成交额", formatValue(dex, "currency"), "已抓取池子合计"],
    ["Observed DEX Share", formatValue(observedShare, "percent"), "非全市场份额"],
    ["CEX / DEX", formatValue(ratio, "ratio"), "相同筛选区间"],
    ["最新完整日期", latestDate || "N/A", `${selectedTokens().length} 个 Token`],
  ];
  byId("kpi-grid").innerHTML = cards.map(([label, value, context]) => `
    <article class="kpi-card">
      <div class="kpi-label">${label}</div>
      <div class="kpi-value">${value}</div>
      <div class="kpi-context">${context}</div>
    </article>`).join("");
}

function renderMetricChart(rows) {
  const metric = selectedMetric();
  const meta = app.payload.metrics[metric];
  const normalize = byId("normalize-toggle").checked;
  const groups = groupedBy(rows, "token_symbol");
  const datasets = Object.entries(groups).map(([token, tokenRows], index) => {
    const sorted = tokenRows.sort((a, b) => a.date.localeCompare(b.date));
    const displayRows = normalize ? normalizedSeries(sorted, metric) : sorted.map((row) => ({ ...row, display_value: row[metric] }));
    return {
      label: token,
      data: displayRows.map((row) => ({ x: Date.parse(`${row.date}T00:00:00Z`), y: row.display_value })),
      borderColor: palette[index % palette.length],
      backgroundColor: palette[index % palette.length],
      pointRadius: 0,
      borderWidth: 1.7,
      tension: 0.12,
      spanGaps: true,
    };
  });
  byId("metric-title").textContent = `${meta.label} 时间序列`;
  byId("metric-subtitle").textContent = normalize ? "按 Token 样本期标准化，单位为 z-score" : `${meta.unit}，按所选 Token 与时间区间`;
  byId("comparison-subtitle").textContent = `${meta.label}的期间中位数`;
  byId("metric-unit").textContent = `单位：${meta.unit}`;
  byId("selection-count").textContent = `${datasets.length} 个序列`;
  replaceChart("metric", "metric-chart", {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      plugins: { legend: { position: "top", align: "start", labels: { boxWidth: 14, usePointStyle: true, pointStyle: "line" } } },
      scales: {
        x: { type: "timeseries", adapters: {}, ticks: { maxTicksLimit: 9 }, grid: { display: false } },
        y: { beginAtZero: false, title: { display: true, text: normalize ? "z-score" : meta.unit } },
      },
      parsing: false,
    },
  });
}

function renderComparisonChart(rows) {
  const metric = selectedMetric();
  const meta = app.payload.metrics[metric];
  const groups = groupedBy(rows, "token_symbol");
  const entries = Object.entries(groups).map(([token, tokenRows]) => ({ token, value: median(tokenRows.map((row) => row[metric])) }))
    .filter((item) => Number.isFinite(item.value)).sort((a, b) => b.value - a.value);
  replaceChart("comparison", "comparison-chart", {
    type: "bar",
    data: {
      labels: entries.map((item) => item.token),
      datasets: [{ label: meta.label, data: entries.map((item) => item.value), backgroundColor: "#0f7c82", borderColor: "#075a60", borderWidth: 1 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (context) => formatValue(context.raw, meta.format) } } },
      scales: { x: { beginAtZero: !["return_1d", "future_return_7d", "mom_7d", "cex_vol_z", "dex_vol_z", "volume_divergence", "volume_growth_divergence_7d"].includes(metric) }, y: { grid: { display: false } } },
    },
  });
}

function renderVolumeChart(rows) {
  const dates = [...new Set(rows.map((row) => row.date))].sort();
  const dateGroups = groupedBy(rows, "date");
  replaceChart("volume", "volume-chart", {
    type: "line",
    data: {
      labels: dates,
      datasets: [
        { label: "CEX", data: dates.map((date) => sum(dateGroups[date].map((row) => row.cex_volume_usd))), borderColor: "#356fb6", backgroundColor: "#356fb6", pointRadius: 0, borderWidth: 1.7 },
        { label: "DEX", data: dates.map((date) => sum(dateGroups[date].map((row) => row.dex_volume_usd))), borderColor: "#c76d1d", backgroundColor: "#c76d1d", pointRadius: 0, borderWidth: 1.7 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top", align: "start", labels: { usePointStyle: true, pointStyle: "line" } }, tooltip: { callbacks: { label: (context) => `${context.dataset.label}: ${formatValue(context.raw, "currency")}` } } },
      scales: { x: { ticks: { maxTicksLimit: 8 }, grid: { display: false } }, y: { type: "logarithmic", title: { display: true, text: "USD / day (log)" } } },
    },
  });
}

function renderScopeChart() {
  const rows = app.payload.scope_sensitivity;
  replaceChart("scope", "scope-chart", {
    type: "bar",
    data: {
      labels: rows.map((row) => row.scope),
      datasets: [
        { label: "加权 Observed DEX Share", data: rows.map((row) => row.weighted_dex_share), backgroundColor: "#0f7c82", borderColor: "#075a60", borderWidth: 1 },
        { label: "Token-day 中位数", data: rows.map((row) => row.median_token_day_dex_share), backgroundColor: "#e0a14a", borderColor: "#a95f18", borderWidth: 1 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "top", align: "start" }, tooltip: { callbacks: { label: (context) => `${context.dataset.label}: ${percent.format(context.raw)}` } } },
      scales: { x: { grid: { display: false } }, y: { beginAtZero: true, ticks: { callback: (value) => percent.format(value) }, title: { display: true, text: "Observed DEX Share" } } },
    },
  });
}

function renderCoverageTable() {
  const selected = new Set(selectedTokens());
  const rows = app.payload.coverage.filter((row) => selected.has(row.token_symbol));
  byId("coverage-table").innerHTML = `
    <thead><tr><th>Token</th><th>天数</th><th>CEX 中位数</th><th>DEX 中位数</th><th>DEX Share</th><th>Top Pool</th><th>Pool HHI</th><th>Volume / TVL</th></tr></thead>
    <tbody>${rows.map((row) => `<tr>
      <td>${row.token_symbol}</td><td>${row.row_count ?? "N/A"}</td>
      <td>${formatValue(row.cex_volume_median, "currency")}</td><td>${formatValue(row.dex_volume_median, "currency")}</td>
      <td>${formatValue(row.dex_share_median, "percent")}</td><td>${formatValue(row.top_pool_volume_share_median, "percent")}</td>
      <td>${formatValue(row.dex_pool_herfindahl_median)}</td><td>${formatValue(row.dex_volume_to_tvl_median, "ratio")}</td>
    </tr>`).join("")}</tbody>`;
}

function renderFactorControls() {
  const factors = [...new Set(app.payload.factor_results.map((row) => row.factor_name))].sort();
  byId("factor-select").innerHTML = factors.map((factor) => `<option value="${factor}">${factor}</option>`).join("");
  const preferred = ["joint_volume_confirmed_mom_7d", "cex_volume_confirmed_mom_7d", "volume_growth_divergence_7d"];
  byId("factor-select").value = preferred.find((factor) => factors.includes(factor)) || factors[0] || "";
}

function renderFactorChart() {
  const factor = byId("factor-select").value;
  const horizon = app.factorHorizon;
  const field = `future_return_${horizon}_mean`;
  const rows = app.payload.factor_results.filter((row) => row.factor_name === factor).sort((a, b) => a.factor_bucket - b.factor_bucket);
  const values = rows.map((row) => row[field]);
  byId("factor-chart-title").textContent = `${factor} 与未来 ${horizon.toUpperCase()} 收益`;
  replaceChart("factor", "factor-chart", {
    type: "bar",
    data: {
      labels: rows.map((row) => `Q${Number(row.factor_bucket) + 1}`),
      datasets: [{ label: `未来 ${horizon.toUpperCase()} 平均收益`, data: values, backgroundColor: values.map((value) => value >= 0 ? "#477a59" : "#aa4d64"), borderColor: "#17212b", borderWidth: 1 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (context) => percent.format(context.raw) } } },
      scales: { x: { grid: { display: false } }, y: { ticks: { callback: (value) => percent.format(value) }, title: { display: true, text: "平均未来收益" } } },
    },
  });
  const low = rows[0];
  const high = rows.at(-1);
  const spread = high && low && Number.isFinite(high[field]) && Number.isFinite(low[field]) ? high[field] - low[field] : null;
  byId("factor-summary").innerHTML = `
    <div class="summary-row"><span>Q5 - Q1 收益差</span><strong>${formatValue(spread, "percent")}</strong></div>
    <div class="summary-row"><span>最低组样本</span><strong>${low?.row_count ?? "N/A"}</strong></div>
    <div class="summary-row"><span>最高组样本</span><strong>${high?.row_count ?? "N/A"}</strong></div>`;
}

function renderWorkspace() {
  const items = app.state.checklist || [];
  byId("checklist").innerHTML = items.map((item) => `
    <label class="check-item"><input type="checkbox" data-check-id="${item.id}" ${item.done ? "checked" : ""}><span>${item.label}</span></label>`).join("");
  byId("research-notes").value = app.state.notes || "";
  renderProgress();

  const meta = app.payload.metadata;
  const status = [
    ["数据源", meta.source_path], ["粒度", meta.grain], ["样本行数", compact.format(meta.row_count)], ["Token", meta.token_count],
    ["日期范围", `${meta.start_date} — ${meta.end_date}`], ["CEX 完整率", percent.format(meta.cex_completeness)],
    ["DEX 完整率", percent.format(meta.dex_completeness)], ["数据版本", meta.data_version],
  ];
  byId("data-status-grid").innerHTML = status.map(([label, value]) => `<div class="status-cell"><span>${label}</span><strong>${value}</strong></div>`).join("");
  renderHistory();
}

function renderHistory() {
  const history = app.history || [];
  byId("history-list").innerHTML = history.length ? history.map((entry) => {
    const workspace = entry.workspace || {};
    const data = entry.data || {};
    const tokens = (workspace.selected_tokens || []).join(", ") || "未选择";
    return `<div class="history-item">
      <div class="history-time">${new Date(entry.saved_at).toLocaleString("zh-CN")}</div>
      <div class="history-main"><strong>${workspace.metric || "N/A"}</strong><span>${tokens}</span></div>
      <div class="history-meta"><span>${workspace.completed_tasks ?? 0}/${workspace.total_tasks ?? 0} 任务</span><span>data ${data.data_version || "N/A"}</span></div>
    </div>`;
  }).join("") : '<div class="history-empty">保存一次私人工作状态后，这里会出现历史记录。</div>';
}

function renderProgress() {
  const boxes = [...document.querySelectorAll("[data-check-id]")];
  const done = boxes.filter((box) => box.checked).length;
  byId("progress-label").textContent = `${done} / ${boxes.length} 已完成`;
  byId("progress-bar").style.width = `${boxes.length ? done / boxes.length * 100 : 0}%`;
}

function currentState() {
  return {
    selected_tokens: selectedTokens(),
    metric: selectedMetric(),
    date_start: byId("date-start").value,
    date_end: byId("date-end").value,
    normalize: byId("normalize-toggle").checked,
    notes: byId("research-notes").value,
    checklist: [...document.querySelectorAll("[data-check-id]")].map((box) => ({
      id: box.dataset.checkId,
      label: box.nextElementSibling.textContent,
      done: box.checked,
    })),
  };
}

async function saveState() {
  const response = await fetch("/api/state", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(currentState()) });
  if (!response.ok) throw new Error("状态保存失败");
  app.state = await response.json();
  const historyResponse = await fetch("/api/history");
  app.history = historyResponse.ok ? await historyResponse.json() : [];
  renderHistory();
  byId("save-status").textContent = `已保存 ${new Date(app.state.saved_at).toLocaleString("zh-CN")}`;
}

function exportData() {
  const rows = filteredRows();
  if (!rows.length) return;
  const columns = Object.keys(rows[0]);
  const escape = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
  const csv = [columns.join(","), ...rows.map((row) => columns.map((column) => escape(row[column])).join(","))].join("\n");
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
  link.download = `market_structure_${selectedMetric()}_${new Date().toISOString().slice(0, 10)}.csv`;
  link.click();
  URL.revokeObjectURL(link.href);
}

function renderDashboard() {
  const rows = filteredRows();
  renderKpis(rows);
  renderMetricChart(rows);
  renderComparisonChart(rows);
  renderVolumeChart(rows);
  if (app.payload.scope_sensitivity.length) renderScopeChart();
  renderCoverageTable();
}

function bindEvents() {
  document.querySelectorAll("#token-list input, #metric-select, #date-start, #date-end, #normalize-toggle")
    .forEach((element) => element.addEventListener("change", renderDashboard));
  document.querySelectorAll(".tab").forEach((button) => button.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((tab) => tab.classList.toggle("active", tab === button));
    document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === `view-${button.dataset.view}`));
    Object.values(app.charts).forEach((chart) => chart.resize());
  }));
  byId("factor-select").addEventListener("change", renderFactorChart);
  document.querySelectorAll("[data-horizon]").forEach((button) => button.addEventListener("click", () => {
    app.factorHorizon = button.dataset.horizon;
    document.querySelectorAll("[data-horizon]").forEach((item) => item.classList.toggle("active", item === button));
    renderFactorChart();
  }));
  byId("save-state").addEventListener("click", () => saveState().catch(showError));
  byId("export-data").addEventListener("click", exportData);
  byId("checklist").addEventListener("change", renderProgress);
}

function showError(error) {
  byId("error-banner").hidden = false;
  byId("error-banner").textContent = error.message || String(error);
}

async function initialize() {
  chartDefaults();
  const [payloadResponse, stateResponse, historyResponse] = await Promise.all([fetch("/api/dashboard"), fetch("/api/state"), fetch("/api/history")]);
  if (!payloadResponse.ok) throw new Error((await payloadResponse.json()).error || "数据载入失败");
  app.payload = await payloadResponse.json();
  app.state = await stateResponse.json();
  app.history = historyResponse.ok ? await historyResponse.json() : [];
  const isPublic = app.payload.metadata.access_mode === "public_read_only";
  document.body.dataset.mode = isPublic ? "public" : "private";
  byId("save-state").hidden = isPublic;
  byId("save-status").hidden = isPublic;
  byId("public-status").hidden = !isPublic;
  document.querySelector('[data-view="workspace"]').hidden = isPublic;
  byId("view-workspace").hidden = isPublic;
  byId("scope-panel").hidden = !app.payload.scope_sensitivity.length;

  renderTokens();
  renderMetricOptions();
  byId("date-start").min = app.payload.metadata.start_date;
  byId("date-start").max = app.payload.metadata.end_date;
  byId("date-end").min = app.payload.metadata.start_date;
  byId("date-end").max = app.payload.metadata.end_date;
  byId("date-start").value = app.state.date_start || app.payload.metadata.start_date;
  byId("date-end").value = app.state.date_end || app.payload.metadata.end_date;
  byId("normalize-toggle").checked = Boolean(app.state.normalize);
  const sourcePrefix = isPublic
    ? (app.payload.metadata.synthetic ? "Synthetic sample" : "Public research snapshot")
    : app.payload.metadata.source_path;
  if (isPublic) {
    byId("public-status").textContent = app.payload.metadata.synthetic
      ? "公开只读模式 · 合成样例数据"
      : `公开只读模式 · ${app.payload.metadata.token_count} Token 研究快照`;
  }
  byId("source-line").textContent = `${sourcePrefix} · ${compact.format(app.payload.metadata.row_count)} rows · ${app.payload.metadata.grain}`;
  byId("freshness").textContent = `截至 ${app.payload.metadata.end_date}`;
  if (!isPublic) byId("save-status").textContent = app.state.saved_at ? `上次保存 ${new Date(app.state.saved_at).toLocaleString("zh-CN")}` : "尚未保存";

  renderFactorControls();
  if (!isPublic) renderWorkspace();
  bindEvents();
  renderDashboard();
  renderFactorChart();
  if (window.lucide) window.lucide.createIcons();
}

initialize().catch(showError);
