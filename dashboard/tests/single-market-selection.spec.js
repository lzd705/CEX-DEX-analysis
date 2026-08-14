const fs = require("fs");
const path = require("path");
const { test, expect } = require("playwright/test");

const fixture = JSON.parse(fs.readFileSync(
  path.join(__dirname, "fixtures", "single-market-responses.json"),
  "utf8",
));

async function installApiRoutes(page, { catalog = fixture.catalog_pair, calls = [] } = {}) {
  await page.route("**/api/markets/**", async (route) => {
    const url = new URL(route.request().url());
    calls.push(`${url.pathname}${url.search}`);
    let body;
    if (url.pathname.endsWith("/summary")) body = fixture.summary;
    else if (url.pathname.endsWith("/catalog")) body = catalog;
    else if (url.pathname.endsWith("/compare")) {
      body = url.searchParams.get("selection") === "single"
        ? fixture.compare_single
        : fixture.compare_pair;
    } else if (url.pathname.endsWith("/execution-cost")) {
      body = url.searchParams.get("selection") === "single"
        ? fixture.execution_single
        : fixture.execution_pair;
    } else if (url.pathname.endsWith("/quality")) body = fixture.quality_selected;
    else if (url.pathname.endsWith("/events")) body = fixture.events;
    else throw new Error(`Unexpected API route: ${url.pathname}`);
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  });
}

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
  await expect(page.getByText("Latest comparable date", { exact: true })).toBeVisible();
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
