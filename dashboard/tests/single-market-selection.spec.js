const fs = require("fs");
const path = require("path");
const { test, expect } = require("playwright/test");

const fixture = JSON.parse(fs.readFileSync(
  path.join(__dirname, "fixtures", "single-market-responses.json"),
  "utf8",
));

async function installApiRoutes(page, {
  catalog = fixture.catalog_pair,
  calls = [],
  compareSingle = fixture.compare_single,
  events = fixture.events,
  executionSingle = null,
  qualitySelected = null,
  failures = new Set(),
} = {}) {
  await page.route("**/api/markets/**", async (route) => {
    const url = new URL(route.request().url());
    calls.push(`${url.pathname}${url.search}`);
    const endpoint = url.pathname.split("/").at(-1);
    if (failures.has(endpoint)) {
      await route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ error: `${endpoint} unavailable` }) });
      return;
    }
    let body;
    if (url.pathname.endsWith("/summary")) body = fixture.summary;
    else if (url.pathname.endsWith("/catalog")) body = catalog;
    else if (url.pathname.endsWith("/compare")) {
      body = url.searchParams.get("selection") === "single"
        ? compareSingle
        : fixture.compare_pair;
    } else if (url.pathname.endsWith("/execution-cost")) {
      body = url.searchParams.get("selection") === "single"
        ? executionSingle || singleExecution
        : fixture.execution_pair;
    } else if (url.pathname.endsWith("/quality")) body = qualitySelected || singleQuality;
    else if (url.pathname.endsWith("/events")) body = events;
    else throw new Error(`Unexpected API route: ${url.pathname}`);
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  });
}

const singleCompare = {
  token_symbol: "AAVE",
  selection_mode: "single",
  market_a: {
    market_id: "cex:binance:AAVE/USDT",
    market_type: "cex",
    venue: "binance",
    instrument: "AAVE/USDT",
  },
  market_b: null,
  market_a_statistics: { window_return: 0.01, daily_volatility: 0.02 },
  latest_market_a_observation: {
    date: "2026-07-30",
    market_a: { price_usd: 300, volume_usd: 0 },
  },
  observations: [
    { date: "2026-07-28", market_a: { price_usd: 297, volume_usd: 800000 } },
    { date: "2026-07-29", market_a: { price_usd: null, volume_usd: null } },
    { date: "2026-07-30", market_a: { price_usd: 300, volume_usd: 0 } },
  ],
  metadata: { union_observation_days: 3 },
};

const overlayEvents = {
  schema: "event_facts_api/v2",
  clock_as_of_utc: "2026-08-01T00:00:00Z",
  availability: { status: "available", reason: null },
  query: { token: "AAVE", lifecycle: null, clock_state: null },
  events: [{
    token_symbol: "AAVE",
    event_name: "Verified release",
    event_type: "unlock",
    lifecycle: "scheduled",
    revision: 1,
    clock: { state: "past", as_of_utc: "2026-08-01T00:00:00Z" },
    time: {
      effective_date_start: "2026-07-30",
      effective_date_end: "2026-07-30",
      effective_at: "2026-07-30",
      effective_at_precision: "day",
    },
    source: { url: "https://example.test/release" },
  }],
  metadata: {},
};

const executionRows = [1000, 5000, 10000, 50000, 100000].flatMap((notional) => (
  ["buy_token", "sell_token"].map((direction) => ({
    direction,
    requested_notional_usd: notional,
    status: "observed",
    quoted_execution_cost_bps: 2,
    quoted_execution_cost_usd: 2,
    fill_ratio: 1,
    fee_status: "excluded_unknown_account_tier",
  }))
));

const singleExecution = {
  token_symbol: "AAVE",
  selection_mode: "single",
  market_a: {
    market: {
      market_id: "cex:binance:AAVE/USDT",
      token_symbol: "AAVE",
      market_type: "cex",
      venue: "binance",
      instrument: "AAVE/USDT",
    },
    status: "available",
    publication_status: "published",
    rows: executionRows,
    timing: {
      status: "not_applicable",
      state_observed_at: "2026-07-30T00:00:00Z",
      usd_price_observed_at: null,
      usd_price_state_skew_seconds: null,
    },
  },
  market_b: null,
  metadata: {
    notionals_usd: [1000, 5000, 10000, 50000, 100000],
    snapshot_skew_seconds: null,
  },
};

const singleQuality = {
  token_symbol: "AAVE",
  metadata: {
    scope: "selected",
    selected_market_ids: ["cex:binance:AAVE/USDT"],
    window_start: "2026-07-01",
    window_end: "2026-07-30",
    daily_quality_report: { status: "unavailable" },
  },
  markets: [{
    market_id: "cex:binance:AAVE/USDT",
    token_symbol: "AAVE",
    market_type: "cex",
    venue: "binance",
    instrument: "AAVE/USDT",
    quality_status: "ok",
    quality_flags: [],
    facts: {},
  }],
  rollups: [],
};

test("single Compare renders only Market A", async ({ page }) => {
  const consoleErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));
  await installApiRoutes(page, { compareSingle: singleCompare, events: overlayEvents });
  await page.goto(
    "/tokens/AAVE/compare?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30",
  );
  await expect(page.locator("#comparison-status")).toContainText("Market A current");
  const summaryColumns = await page.locator(".comparison-summary").evaluate((element) => (
    getComputedStyle(element).gridTemplateColumns.split(" ").filter(Boolean).length
  ));
  expect(summaryColumns).toBe(await page.evaluate(() => (window.innerWidth <= 640 ? 1 : 2)));
  await expect(page.locator("#comparison-table-region").getByRole("columnheader"))
    .toHaveText(["Date (UTC)", "binance Price (USD)", "binance Volume (USD)"]);
  await expect(page.locator("#comparison-chart-legend .comparison-legend-item")).toHaveCount(2);
  await expect(page.locator("#comparison-chart-legend")).toContainText("A · CEX · binance · AAVE/USDT");
  await expect(page.locator("#comparison-chart-legend")).not.toContainText("B ·");
  await expect(page.locator("#comparison-chart-description")).not.toContainText("Market B");
  await expect(page.locator("#comparison-chart-description")).not.toContainText("comparable");
  await expect(page.locator("#workspace-page-compare [data-single-only]"))
    .toContainText("Only Market A source observations");
  await expect(page.locator(".comparison-marker.series-a")).toHaveCount(2);
  await expect(page.locator(".comparison-marker.series-a").first())
    .toHaveAttribute("data-series-offset", "0");
  await expect(page.locator(".comparison-event-overlay")).toHaveCount(1);
  await expect(page.locator("#comparison-event-status")).toContainText("timing only");
  await expect(page.locator("[data-pair-only]:visible")).toHaveCount(0);
  expect(await page.locator("[data-pair-only]").evaluateAll((nodes) => nodes.every((node) => (
    node.hidden
  )))).toBe(true);
  await expect(page.getByRole("button", { name: "Daily Price Gap" })).toHaveCount(0);
  for (let index = 0; index < 12; index += 1) {
    await page.keyboard.press("Tab");
    expect(await page.evaluate(() => Boolean(document.activeElement?.closest?.("[data-pair-only]"))))
      .toBe(false);
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  expect(consoleErrors).toEqual([]);
});

test("single Compare handles empty observations", async ({ page }) => {
  const empty = {
    ...singleCompare,
    market_a_statistics: { window_return: null, daily_volatility: null },
    latest_market_a_observation: null,
    observations: [],
    metadata: { union_observation_days: 0 },
  };
  await installApiRoutes(page, { compareSingle: empty });
  await page.goto(
    "/tokens/AAVE/compare?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30",
  );
  await expect(page.locator("#comparison-status")).toContainText("Market A current");
  await expect(page.locator("#comparison-chart-empty")).toContainText("No source-backed values");
  await expect(page.locator("#comparison-body")).toContainText("No observations");
  await expect(page.locator("#compare-date .na-disclosure")).toHaveCount(1);
  await expect(page.locator("#comparison-chart-legend")).not.toContainText("B ·");
  await expect(page.locator("[data-pair-only]:visible")).toHaveCount(0);
  await expect(page.locator("#comparison-table-region").getByRole("columnheader")).toHaveCount(3);
});

test("single selection can be confirmed when Market B starts empty", async ({ page }) => {
  await installApiRoutes(page, { catalog: fixture.catalog_one });
  await page.goto("/tokens/AAVE/markets");
  const marketA = page.getByLabel("Market A", { exact: true });
  const marketB = page.getByLabel("Market B (optional)", { exact: true });
  await expect(marketA).toHaveValue("cex:binance:AAVE/USDT");
  await expect(marketB).toHaveValue("");
  await expect(marketB.locator("option").first()).toHaveText("Market A only — no comparison");
  expect(new URL(page.url()).searchParams.get("selection")).toBeNull();
  await page.getByRole("button", { name: "Apply selection" }).click();
  await expect(page).toHaveURL(/\/tokens\/AAVE\/compare\?/);
  const url = new URL(page.url());
  expect(url.searchParams.get("marketA")).toBe("cex:binance:AAVE/USDT");
  expect(url.searchParams.get("selection")).toBe("single");
  expect(url.searchParams.has("marketB")).toBe(false);
  expect(url.searchParams.has("pairMode")).toBe(false);
});

test("restoring Market B restores the pair contract", async ({ page }) => {
  await installApiRoutes(page);
  await page.goto("/tokens/AAVE/markets");
  const marketB = page.getByLabel("Market B (optional)", { exact: true });
  await marketB.selectOption("");
  await page.getByRole("button", { name: "Apply selection" }).click();
  await expect(page).toHaveURL(/selection=single/);
  await page.getByRole("link", { name: "Change markets" }).click();
  await expect(page).toHaveURL(/\/tokens\/AAVE\/markets\?/);
  await marketB.selectOption("dex:eth:uniswap_v3:AAVE/WETH:0.05%");
  await page.getByRole("button", { name: "Apply selection" }).click();
  await expect(page).not.toHaveURL(/selection=single/);
  await expect(page.locator("#comparison-status")).toContainText("comparison current");
  await expect(page.getByText("Latest comparable date", { exact: true })).toBeVisible();
  await expect(page.locator("#comparison-chart-legend")).toContainText("B · DEX");
  await page.reload();
  await expect(marketB).toHaveValue("dex:eth:uniswap_v3:AAVE/WETH:0.05%");
  expect(new URL(page.url()).searchParams.get("selection")).toBeNull();
});

test("invalid selection marker is not repaired", async ({ page }) => {
  const calls = [];
  await installApiRoutes(page, { calls });
  await page.addInitScript(() => {
    const original = history.replaceState.bind(history);
    window.__replaceCalls = [];
    history.replaceState = (...args) => {
      window.__replaceCalls.push(String(args[2] || ""));
      return original(...args);
    };
  });
  const exactA = "cex:binance:AAVE/USDT";
  const exactB = "dex:eth:uniswap_v3:AAVE/WETH:0.05%";
  const cases = [
    {
      search: `marketA=${encodeURIComponent(exactA)}&selection=bogus`,
      code: "selection_invalid",
      raw: [exactA, "bogus"],
    },
    {
      search: `marketA=${encodeURIComponent(exactA)}&marketB=${encodeURIComponent(exactB)}&selection=single`,
      code: "selection_market_b_conflict",
      raw: [exactA, exactB, "single"],
    },
    {
      search: `marketA=${encodeURIComponent("cex:unknown:AAVE/USDT")}&marketB=${encodeURIComponent(exactB)}`,
      code: "market_a_not_found",
      raw: ["cex:unknown:AAVE/USDT", exactB],
    },
  ];
  for (const item of cases) {
    await page.goto(`/tokens/AAVE/markets?${item.search}`);
    const notice = page.locator("#workspace-context-notice");
    await expect(notice).toContainText(item.code);
    for (const rawValue of item.raw) await expect(notice).toContainText(rawValue);
    await expect(page.getByLabel("Market A", { exact: true })).toHaveValue("");
    await expect(page.getByLabel("Market B (optional)", { exact: true })).toHaveValue("");
    expect(await page.evaluate(() => window.__replaceCalls.length)).toBe(0);
    expect(page.url()).toContain(item.search);
  }
  expect(calls.filter((call) => /\/(compare|execution-cost|quality|events)\?/.test(call))).toEqual([]);
});

test("single selection survives research pages", async ({ page }) => {
  const calls = [];
  await installApiRoutes(page, { calls, compareSingle: singleCompare, events: overlayEvents });
  const start = "/tokens/AAVE/compare?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30";
  await page.goto(start);
  await expect(page.locator("#comparison-status")).toContainText("Market A current");
  await page.getByRole("link", { name: "Liquidity & Execution" }).click();
  await expect(page).toHaveURL(/\/tokens\/AAVE\/liquidity\?.*selection=single/);
  await expect(page.locator("#execution-status")).toContainText("AAVE");
  await expect(page.locator("#execution-table-body tr")).toHaveCount(5);
  await page.getByRole("link", { name: "Events", exact: true }).click();
  await expect(page).toHaveURL(/\/tokens\/AAVE\/events\?.*selection=single/);
  await expect(page.locator("#events-status")).toContainText(/event/i);
  await page.getByRole("link", { name: "Data Quality", exact: true }).click();
  await expect(page).toHaveURL(/\/tokens\/AAVE\/quality\?.*selection=single/);
  await expect(page.locator("#quality-status")).toContainText("1 market");
  await expect(page.locator("#quality-body")).toContainText("cex:binance:AAVE/USDT");
  await page.reload();
  await expect(page.locator("#quality-status")).toContainText("1 market");
  await page.goBack();
  expect(new URL(page.url()).searchParams.get("selection")).toBe("single");
  expect(calls.filter((call) => call.includes("/events?")).every((call) => (
    !call.includes("market_a=") && !call.includes("market_b=") && !call.includes("selection=")
  ))).toBe(true);
});

test("single mobile layout has no pair slots", async ({ page, isMobile }) => {
  const browserErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") browserErrors.push(message.text());
  });
  page.on("pageerror", (error) => browserErrors.push(error.message));
  await installApiRoutes(page, { compareSingle: singleCompare, events: overlayEvents });
  await page.goto(
    "/tokens/AAVE/liquidity?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30",
  );
  await expect(page.locator("#execution-status")).toContainText("AAVE");
  await expect(page.locator("[data-pair-only]:visible")).toHaveCount(0);
  await expect(page.locator(".liquidity-table").first().getByRole("columnheader")).toHaveCount(5);
  await expect(page.locator(".execution-table").getByRole("columnheader")).toHaveCount(4);
  for (let index = 0; index < 28; index += 1) {
    await page.keyboard.press("Tab");
    expect(await page.evaluate(() => Boolean(document.activeElement?.closest?.("[data-pair-only]"))))
      .toBe(false);
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  const summaryColumns = await page.locator(".liquidity-summary").evaluate((element) => (
    getComputedStyle(element).gridTemplateColumns.split(" ").filter(Boolean).length
  ));
  expect(summaryColumns).toBe(1);
  const executionColumns = await page.locator(".execution-summary").evaluate((element) => (
    getComputedStyle(element).gridTemplateColumns.split(" ").filter(Boolean).length
  ));
  expect(executionColumns).toBe(1);
  expect(browserErrors).toEqual([]);
});

test("latest selection owns async responses", async ({ page }) => {
  let releaseOld;
  const oldGate = new Promise((resolve) => { releaseOld = resolve; });
  let singleRequestSeen;
  const singleSeen = new Promise((resolve) => { singleRequestSeen = resolve; });
  await page.route("**/api/markets/**", async (route) => {
    const url = new URL(route.request().url());
    let body = fixture.summary;
    if (url.pathname.endsWith("/catalog")) body = fixture.catalog_pair;
    else if (url.pathname.endsWith("/events")) body = overlayEvents;
    else if (url.pathname.endsWith("/compare")) {
      if (url.searchParams.get("selection") === "single") {
        singleRequestSeen();
        await oldGate;
        body = singleCompare;
      } else body = fixture.compare_pair;
    }
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  });
  await page.goto(
    "/tokens/AAVE/compare?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30",
    { waitUntil: "domcontentloaded" },
  );
  await singleSeen;
  await page.evaluate(() => {
    const b = document.getElementById("facts-market-b");
    b.value = "dex:eth:uniswap_v3:AAVE/WETH:0.05%";
    app.workspaceSelection = "";
    app.route.state.marketB = b.value;
    delete app.route.state.selection;
    void loadComparison();
  });
  await expect(page.locator("#comparison-status")).toContainText("comparison current");
  releaseOld();
  await page.waitForTimeout(100);
  await expect(page.locator("#comparison-status")).toContainText("comparison current");
  await expect(page.locator("#comparison-chart-legend")).toContainText("B · DEX");
});

test("page failures are isolated", async ({ page }) => {
  await installApiRoutes(page, {
    compareSingle: singleCompare,
    events: overlayEvents,
    failures: new Set(["execution-cost", "events", "quality"]),
  });
  await page.goto(
    "/tokens/AAVE/liquidity?marketA=cex%3Abinance%3AAAVE%2FUSDT&selection=single&start=2026-07-01&end=2026-07-30",
  );
  await expect(page.locator("#execution-error")).toContainText("execution-cost unavailable");
  await expect(page.locator("#liquidity-status")).not.toContainText("execution-cost unavailable");
  await expect(page.locator("#global-error")).toBeHidden();
  await expect(page.locator("#liquidity-a-label")).toContainText("A · binance");
  await page.getByRole("link", { name: "Events", exact: true }).click();
  await expect(page.locator("#events-error")).toContainText("events unavailable");
  await expect(page.locator("#global-error")).toBeHidden();
  await page.getByRole("link", { name: "Data Quality", exact: true }).click();
  await expect(page.locator("#quality-error")).toContainText("quality unavailable");
  await expect(page.locator("#quality-body")).toContainText("cex:binance:AAVE/USDT");
  await expect(page.locator("#quality-body")).not.toContainText("uniswap_v3");
  await page.getByRole("link", { name: "Compare", exact: true }).click();
  await expect(page.locator("#comparison-status")).toContainText("Market A current");
});
