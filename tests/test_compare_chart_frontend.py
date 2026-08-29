import json
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard" / "static" / "app.js"
INDEX_PATH = PROJECT_ROOT / "dashboard" / "static" / "index.html"
STYLES_PATH = PROJECT_ROOT / "dashboard" / "static" / "styles.css"


def run_app_javascript(source: str):
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("Node.js is not installed in this runtime")
    script = APP_PATH.read_text(encoding="utf-8") + "\n" + source
    completed = subprocess.run(
        [node, "-"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        input=script,
    )
    return json.loads(completed.stdout)


FIXTURE = r"""
const comparisonFixture = {
  token_symbol: "TEST",
  market_a: {
    market_type: "cex",
    venue: "alpha <cex>",
    instrument: "TEST/USD",
  },
  market_b: {
    market_type: "dex",
    venue: "beta",
    instrument: "TEST / USDC",
  },
  market_a_statistics: {
    window_return: 99,
    daily_volatility: 88,
  },
  market_b_statistics: {
    window_return: 77,
    daily_volatility: 66,
  },
  observations: [
    {
      date: "2026-01-03",
      market_a: { price_usd: 103, volume_usd: 0 },
      market_b: { price_usd: 203, volume_usd: 300 },
      absolute_spread_usd: 100,
      spread_bps: 75,
      missing_reason: null,
    },
    {
      date: "2026-01-01",
      market_a: { price_usd: 100, volume_usd: 100 },
      market_b: { price_usd: 200, volume_usd: 200 },
      absolute_spread_usd: 100,
      spread_bps: 50,
      missing_reason: null,
    },
    {
      date: "not-a-date",
      market_a: { price_usd: 999, volume_usd: 999 },
      market_b: { price_usd: 999, volume_usd: 999 },
      absolute_spread_usd: 0,
      spread_bps: 0,
      missing_reason: null,
    },
    {
      date: "2026-01-02",
      market_a: { price_usd: null, volume_usd: null },
      market_b: { price_usd: 202, volume_usd: 250 },
      absolute_spread_usd: null,
      spread_bps: null,
      missing_reason: "market_a_missing",
    },
    {
      date: "2026-01-05",
      market_a: { price_usd: 105, volume_usd: 500 },
      market_b: { price_usd: 205, volume_usd: -1 },
      absolute_spread_usd: 100,
      spread_bps: 0,
      missing_reason: null,
    },
  ],
};
"""


class CompareChartFrontendTest(unittest.TestCase):
    def test_daily_price_gap_copy_uses_same_utc_closes_and_never_claims_execution(self):
        result = run_app_javascript(
            FIXTURE
            + r"""
const model = comparisonChartModel(comparisonFixture, "spread");
const point = model.series[0].points.find((row) => row.date === "2026-01-01");
console.log(JSON.stringify({
  definition: model.definition,
  seriesLabel: model.series[0].label,
  tooltip: comparisonChartTooltipText(model, point),
}));
"""
        )
        self.assertEqual(result["definition"]["title"], "Daily Price Gap")
        self.assertEqual(result["definition"]["axisTitle"], "Daily Price Gap (bps)")
        self.assertIn("same-UTC-date closes", result["definition"]["note"])
        self.assertEqual(result["seriesLabel"], "A ↔ B Daily Price Gap")
        self.assertIn("Daily Price Gap 50 bps", result["tooltip"])
        self.assertIn("same-UTC-date closes", result["tooltip"])
        for prohibited in ("arbitrage", "live spread", "quoted spread"):
            self.assertNotIn(prohibited, result["tooltip"].lower())

        index = INDEX_PATH.read_text(encoding="utf-8")
        compare_start = index.index('data-workspace-view="compare"')
        compare_end = index.index('data-workspace-view="events"', compare_start)
        compare = index[compare_start:compare_end]
        self.assertIn(">Daily Price Gap</button>", compare)
        self.assertIn("Absolute Daily Price Gap", compare)
        self.assertIn("Daily Price Gap (bps)", compare)
        self.assertIn("same UTC date closing prices", compare)
        self.assertNotIn(">Spread</button>", compare)
        self.assertNotIn("midpoint-relative spread", compare.lower())

    def test_chart_contract_is_accessible_and_keeps_exact_table(self):
        index = INDEX_PATH.read_text(encoding="utf-8")
        styles = STYLES_PATH.read_text(encoding="utf-8")

        self.assertIn('id="comparison-chart"', index)
        self.assertIn('id="comparison-plot"', index)
        self.assertIn('aria-label="Comparison chart metric"', index)
        self.assertIn('data-comparison-metric="price"', index)
        self.assertIn('data-comparison-metric="spread"', index)
        self.assertIn('data-comparison-metric="volume"', index)
        self.assertIn('id="comparison-chart-tooltip"', index)
        self.assertIn('role="tooltip"', index)
        self.assertIn('aria-live="polite"', index)
        self.assertIn('tabindex="0"', index)
        self.assertIn('aria-hidden="true"', index)
        self.assertIn("no interpolation or forward fill", index)
        self.assertIn('id="comparison-body"', index)
        self.assertLess(
            index.index('id="comparison-chart"'),
            index.index('id="comparison-body"'),
        )

        self.assertIn(".comparison-date-hit", styles)
        self.assertIn(".comparison-series-line.series-a", styles)
        self.assertIn(".comparison-series-line.series-b", styles)
        self.assertIn("stroke-dasharray: 7 4", styles)
        self.assertIn(".comparison-event-line", styles)
        self.assertIn(".comparison-series-line.series-spread", styles)
        self.assertIn("#comparison-chart", styles)
        self.assertIn("@media (max-width: 700px)", styles)

    def test_model_sorts_dates_breaks_gaps_and_preserves_measured_zero(self):
        result = run_app_javascript(
            FIXTURE
            + r"""
const price = comparisonChartModel(comparisonFixture, "price");
const volume = comparisonChartModel(comparisonFixture, "volume");
const spread = comparisonChartModel(comparisonFixture, "spread");
const segmentDates = (series) => series.segments.map(
  (segment) => segment.map((point) => point.date),
);
console.log(JSON.stringify({
  dates: price.rows.map((row) => row.date),
  priceA: price.series[0].points.map((point) => point.value),
  priceASegments: segmentDates(price.series[0]),
  priceBSegments: segmentDates(price.series[1]),
  volumeA: volume.series[0].points.map((point) => point.value),
  volumeB: volume.series[1].points.map((point) => point.value),
  spreadSeries: spread.series.map((series) => series.className),
  spreadValues: spread.series[0].points.map((point) => point.value),
  spreadSegments: segmentDates(spread.series[0]),
}));
"""
        )

        self.assertEqual(
            result["dates"],
            ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-05"],
        )
        self.assertEqual(result["priceA"], [100, None, 103, 105])
        self.assertEqual(
            result["priceASegments"],
            [["2026-01-01"], ["2026-01-03"], ["2026-01-05"]],
        )
        self.assertEqual(
            result["priceBSegments"],
            [
                ["2026-01-01", "2026-01-02", "2026-01-03"],
                ["2026-01-05"],
            ],
        )
        self.assertEqual(result["volumeA"], [100, None, 0, 500])
        self.assertEqual(result["volumeB"], [200, 250, 300, None])
        self.assertEqual(result["spreadSeries"], ["series-spread"])
        self.assertEqual(result["spreadValues"], [50, None, 75, 0])
        self.assertEqual(
            result["spreadSegments"],
            [["2026-01-01"], ["2026-01-03"], ["2026-01-05"]],
        )

    def test_svg_uses_straight_paths_nonoverlapping_date_hits_and_distinct_markers(self):
        result = run_app_javascript(
            FIXTURE
            + r"""
const elements = {
  "comparison-chart": {
    innerHTML: "",
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
  },
  "comparison-chart-empty": { hidden: false, textContent: "" },
  "comparison-plot": { clientWidth: 900 },
};
global.document = {
  getElementById(id) { return elements[id] || null; },
};
global.window = {
  matchMedia() { return { matches: false }; },
};
const priceModel = comparisonChartModel(comparisonFixture, "price");
const plottedPrice = renderComparisonSvg(priceModel);
const priceMarkup = elements["comparison-chart"].innerHTML;
const pathData = [...priceMarkup.matchAll(
  /class="comparison-series-line[^"]*"[\s\S]*?\sd="([^"]+)"/g,
)].map((match) => match[1]);
const markerCount = (priceMarkup.match(/class="comparison-marker/g) || []).length;
const dateHitCount = (priceMarkup.match(/class="comparison-date-hit"/g) || []).length;
const januarySecondHits = (
  priceMarkup.match(/data-date="2026-01-02"/g) || []
).length;

global.window.matchMedia = () => ({ matches: true });
elements["comparison-plot"].clientWidth = 300;
const spreadModel = comparisonChartModel(comparisonFixture, "spread");
const plottedSpread = renderComparisonSvg(spreadModel);
const spreadMarkup = elements["comparison-chart"].innerHTML;

console.log(JSON.stringify({
  plottedPrice,
  pathData,
  markerCount,
  dateHitCount,
  januarySecondHits,
  priceHasExpectedSegment: priceMarkup.includes('data-segment-start="2026-01-01"')
    && priceMarkup.includes('data-segment-end="2026-01-03"'),
  allPathsStraight: pathData.every((path) => (
    /^M [\d.-]+ [\d.-]+(?: L [\d.-]+ [\d.-]+)*$/.test(path)
  )),
  plottedSpread,
  spreadHasZeroLine: spreadMarkup.includes('class="comparison-zero-line"'),
  mobileViewBox: elements["comparison-chart"].attributes.viewBox,
  focusableSvgNodes: (spreadMarkup.match(/tabindex=/g) || []).length,
  distinctAMarker: priceMarkup.includes('<circle class="comparison-marker series-a"'),
  distinctBMarker: priceMarkup.includes('<rect class="comparison-marker series-b"'),
}));
"""
        )

        self.assertTrue(result["plottedPrice"])
        self.assertEqual(len(result["pathData"]), 1)
        self.assertTrue(result["priceHasExpectedSegment"])
        self.assertTrue(result["allPathsStraight"])
        self.assertEqual(result["markerCount"], 7)
        self.assertEqual(result["dateHitCount"], 4)
        self.assertEqual(result["januarySecondHits"], 1)
        self.assertTrue(result["plottedSpread"])
        self.assertTrue(result["spreadHasZeroLine"])
        self.assertEqual(result["mobileViewBox"], "0 0 300 300")
        self.assertEqual(result["focusableSvgNodes"], 0)
        self.assertTrue(result["distinctAMarker"])
        self.assertTrue(result["distinctBMarker"])

    def test_tooltip_supports_nonoverlapping_pointer_keyboard_and_escape(self):
        result = run_app_javascript(
            r"""
function fakeElement() {
  const listeners = {};
  return {
    listeners,
    dataset: {},
    hidden: false,
    textContent: "",
    focusCount: 0,
    attributes: {},
    addEventListener(type, callback) { listeners[type] = callback; },
    focus() { this.focusCount += 1; },
    setAttribute(name, value) { this.attributes[name] = value; },
    removeAttribute(name) { delete this.attributes[name]; },
    contains() { return false; },
  };
}
const svg = fakeElement();
const plot = fakeElement();
const tooltip = fakeElement();
tooltip.hidden = true;
const zones = [
  {
    id: "comparison-date-0",
    dataset: { index: "0", tooltip: "2026-01-01 UTC · A price $100" },
    contains() { return false; },
  },
  {
    id: "comparison-date-1",
    dataset: { index: "1", tooltip: "2026-01-02 UTC · A price $101" },
    contains() { return false; },
  },
];
svg.querySelectorAll = () => zones;
const target = {
  closest(selector) {
    return selector === ".comparison-date-hit" ? zones[0] : null;
  },
};
const secondTarget = {
  closest(selector) {
    return selector === ".comparison-date-hit" ? zones[1] : null;
  },
};
global.document = {
  getElementById(id) {
    return {
      "comparison-chart": svg,
      "comparison-plot": plot,
      "comparison-chart-tooltip": tooltip,
    }[id];
  },
};
bindComparisonChartTooltipEvents();
svg.listeners.pointerover({ target });
const pointerState = {
  hidden: tooltip.hidden,
  text: tooltip.textContent,
};
let prevented = 0;
plot.listeners.keydown({
  key: "ArrowRight",
  preventDefault() { prevented += 1; },
});
const keyboardState = {
  hidden: tooltip.hidden,
  text: tooltip.textContent,
};
plot.listeners.click({ target: secondTarget });
plot.listeners.keydown({ key: "Escape" });
console.log(JSON.stringify({
  pointerState,
  keyboardState,
  prevented,
  escaped: tooltip.hidden,
  escapedText: tooltip.textContent,
  plotFocusCount: plot.focusCount,
}));
"""
        )

        self.assertFalse(result["pointerState"]["hidden"])
        self.assertIn("A price $100", result["pointerState"]["text"])
        self.assertFalse(result["keyboardState"]["hidden"])
        self.assertIn("2026-01-02", result["keyboardState"]["text"])
        self.assertEqual(result["prevented"], 1)
        self.assertTrue(result["escaped"])
        self.assertEqual(result["escapedText"], "")
        self.assertEqual(result["plotFocusCount"], 1)

    def test_dense_mobile_dates_have_one_nonoverlapping_hit_zone_and_event_overlay(self):
        result = run_app_javascript(
            r"""
const start = Date.parse("2026-01-01T00:00:00Z");
const observations = Array.from({ length: 90 }, (_, index) => {
  const date = new Date(start + index * 86_400_000).toISOString().slice(0, 10);
  return {
    date,
    market_a: { price_usd: 100 + index, volume_usd: 1000 },
    market_b: { price_usd: 101 + index, volume_usd: 1200 },
    absolute_spread_usd: 1,
    spread_bps: 10,
    missing_reason: null,
  };
});
const payload = {
  token_symbol: "TEST",
  market_a: { market_type: "cex", venue: "A", instrument: "TEST/USD" },
  market_b: { market_type: "dex", venue: "B", instrument: "TEST/USDC" },
  observations,
};
const eventPayload = {
  availability: { status: "available", reason: null },
  events: [{
    event_name: "Verified unlock",
    time: {
      effective_date_start: "2026-02-15",
      effective_date_end: "2026-02-15",
    },
  }],
};
const elements = {
  "comparison-chart": {
    innerHTML: "",
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
  },
  "comparison-chart-empty": { hidden: false, textContent: "" },
  "comparison-plot": { clientWidth: 300 },
};
global.document = {
  getElementById(id) { return elements[id] || null; },
};
global.window = {
  matchMedia() { return { matches: true }; },
};
const model = comparisonChartModel(payload, "price", eventPayload);
renderComparisonSvg(model);
const markup = elements["comparison-chart"].innerHTML;
const ranges = [...markup.matchAll(
  /class="comparison-date-hit"\s+x="([\d.-]+)"\s+y="[\d.-]+"\s+width="([\d.-]+)"/g,
)].map((match) => ({ x: Number(match[1]), width: Number(match[2]) }));
console.log(JSON.stringify({
  count: ranges.length,
  positive: ranges.every((range) => range.width > 0),
  nonoverlap: ranges.every((range, index) => (
    index === 0 || Math.abs(
      (ranges[index - 1].x + ranges[index - 1].width) - range.x
    ) <= 0.02
  )),
  eventMarkers: model.eventMarkers.length,
  eventLine: markup.includes("comparison-event-line"),
  causalBoundary: markup.includes("no causal or return claim"),
  svgTabStops: (markup.match(/tabindex=/g) || []).length,
}));
"""
        )

        self.assertEqual(result["count"], 90)
        self.assertTrue(result["positive"])
        self.assertTrue(result["nonoverlap"])
        self.assertEqual(result["eventMarkers"], 1)
        self.assertTrue(result["eventLine"])
        self.assertTrue(result["causalBoundary"])
        self.assertEqual(result["svgTabStops"], 0)

    def test_chart_reuses_comparison_observations_without_new_fact_endpoint(self):
        app_js = APP_PATH.read_text(encoding="utf-8")
        model_source = app_js[
            app_js.index("function comparisonChartModel("):
            app_js.index("function comparisonNiceMaximum(")
        ]
        loader_source = app_js[
            app_js.index("async function loadComparison()"):
            app_js.index("async function loadTokenCatalog(")
        ]

        self.assertIn("payload?.observations", model_source)
        self.assertNotIn("window_return", model_source)
        self.assertNotIn("daily_volatility", model_source)
        self.assertIn("fetch(`/api/markets/compare?", loader_source)
        self.assertNotIn("/api/markets/chart", app_js)


if __name__ == "__main__":
    unittest.main()
