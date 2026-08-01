# Lifecycle-only Catalog Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Project five zero-history Crypto.com absence reviews into the public catalog as non-retryable, all-N/A lifecycle evidence without changing source data or Upbit.

**Architecture:** `dashboard.server.overlay_cex_instrument_lifecycle()` will materialize a minimal read-time CEX seed only for a validated absence review whose canonical market ID is not already present. The existing lifecycle-withholding, quality, catalog, summary, and primary-selection code then processes that seed, while a small frontend helper explains lifecycle-specific daily N/A values. The strict release checker remains unchanged.

**Tech Stack:** Python 3.8, standard-library `unittest`, vanilla JavaScript, Node-based frontend contract tests, SQLite/CSV read-only fact sources, systemd user services, Ego Browser production QA.

## Global Constraints

- Missing facts remain JSON `null`; never forward-fill them or replace them with zero.
- A lifecycle-only entry has `historical_observation_count = 0`, current-window observation counts `null`, and `price_points = []`.
- Only validated Crypto.com absence reviews may materialize zero-history entries.
- Lifecycle-only entries never become primary markets and never contribute to volume, spread, return, volatility, depth, TVL, or executable-opportunity ranking.
- Official absence and stale-catalog evidence remain non-retryable and expose no public refresh action.
- Do not add, delete, relabel, filter, or otherwise modify any Upbit fact or identity.
- Do not relax `scripts/check_dashboard_release.py` to hide either the configured-Upbit identity failure or lifecycle-catalog mismatch.
- Preserve Python 3.8 compatibility and add no dependency.
- Every commit uses an explicit message; every push is followed by a verified GitHub commit comment.
- The approved follow-on product direction is documented only in this change; executable-opportunity routing is out of implementation scope.

---

### Task 1: Materialize validated lifecycle-only markets

**Files:**
- Modify: `dashboard/server.py:1203-1465`
- Test: `tests/test_dashboard.py:2700-3220`
- Modify: `docs/market-facts-contract.md:195-235`

**Interfaces:**
- Consumes: `cex_market_id(venue: str, instrument: str) -> str`, validated lifecycle review dictionaries, and `overlay_cex_instrument_lifecycle(payload, reviews, lifecycle_evidence, now)`.
- Produces: `_lifecycle_only_cex_market(market_id: str, review: dict[str, Any]) -> dict[str, Any]`, returning one minimal CEX seed with exact identity, zero historical observations, empty price series, and no measured fact.

- [ ] **Step 1: Add failing direct-overlay tests**

Add these cases to `MarketMonitorServerTest` in `tests/test_dashboard.py`:

```python
def test_lifecycle_review_materializes_zero_history_market(self):
    review = {
        "market_id": "cex:crypto_com:CAKE/USDT",
        "token_symbol": "CAKE",
        "exchange": "crypto_com",
        "instrument": "CAKE/USDT",
        "current_listing_status": "absent_from_official_current_catalog",
        "reason_code": "instrument_absent_from_current_catalog",
        "checked_at_utc": "2026-08-01T07:22:59+00:00",
        "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
        "response_sha256": "9" * 64,
    }
    evidence = {
        "checked_at_utc": review["checked_at_utc"],
        "response_sha256": review["response_sha256"],
        "inventory_count": 919,
        "configured_market_count": 1,
        "configured_market_ids_sha256": server.configured_market_ids_sha256(
            [review["market_id"]]
        ),
    }

    result = server.overlay_cex_instrument_lifecycle(
        {"metadata": {}, "cex_markets": [], "dex_pools": []},
        {review["market_id"]: review},
        lifecycle_evidence=evidence,
        now=datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc),
    )

    self.assertEqual(len(result["cex_markets"]), 1)
    market = result["cex_markets"][0]
    self.assertEqual(market["token_symbol"], "CAKE")
    self.assertEqual(market["venue"], "crypto_com")
    self.assertEqual(market["instrument"], "CAKE/USDT")
    self.assertEqual(market["historical_observation_count"], 0)
    self.assertIsNone(market["observation_count"])
    self.assertEqual(market["price_points"], [])
    self.assertIsNone(market["price_usd"])
    self.assertIsNone(market["volume_usd"])
    for field in (
        "window_return",
        "daily_volatility",
        "spread_bps",
        "total_depth_10bps_usd",
        "total_depth_25bps_usd",
        "total_depth_50bps_usd",
        "total_depth_100bps_usd",
    ):
        self.assertIsNone(market[field])
    self.assertEqual(
        market["current_listing_reason_code"],
        "instrument_absent_from_current_catalog",
    )
    self.assertEqual(market["current_listing_source"], review["source_url"])
    self.assertEqual(
        market["current_listing_response_sha256"],
        review["response_sha256"],
    )
    self.assertEqual(market["depth_status"], "source_no_observation")

def test_lifecycle_review_does_not_duplicate_observed_market(self):
    review = self.crypto_com_lifecycle_review("GMX")
    payload = {
        "metadata": {},
        "cex_markets": [{
            "token_symbol": "GMX",
            "market": "cex",
            "venue": "crypto_com",
            "instrument": "GMX/USDT",
            "observation_count": 196,
            "price_points": [{"date": "2026-07-30", "price_usd": 10.0}],
        }],
        "dex_pools": [],
    }
    result = server.overlay_cex_instrument_lifecycle(
        payload,
        {review["market_id"]: review},
        lifecycle_evidence=self.lifecycle_evidence([review]),
        now=datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc),
    )
    self.assertEqual(len(result["cex_markets"]), 1)
    self.assertEqual(
        result["cex_markets"][0]["historical_observation_count"],
        196,
    )
```

Use concrete local test helpers `crypto_com_lifecycle_review()` and
`lifecycle_evidence()` in the test class to remove repeated dictionaries; the
helpers are:

```python
def crypto_com_lifecycle_review(self, token):
    market_id = f"cex:crypto_com:{token}/USDT"
    return {
        "market_id": market_id,
        "token_symbol": token,
        "exchange": "crypto_com",
        "instrument": f"{token}/USDT",
        "current_listing_status": "absent_from_official_current_catalog",
        "reason_code": "instrument_absent_from_current_catalog",
        "checked_at_utc": "2026-08-01T07:22:59+00:00",
        "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
        "response_sha256": "9" * 64,
    }

def lifecycle_evidence(self, reviews):
    market_ids = [review["market_id"] for review in reviews]
    return {
        "checked_at_utc": reviews[0]["checked_at_utc"],
        "response_sha256": reviews[0]["response_sha256"],
        "inventory_count": 919,
        "configured_market_count": len(market_ids),
        "configured_market_ids_sha256": (
            server.configured_market_ids_sha256(market_ids)
        ),
    }
```

- [ ] **Step 2: Run the new direct-overlay tests and prove the missing projection**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard.MarketMonitorServerTest.test_lifecycle_review_materializes_zero_history_market \
  tests.test_dashboard.MarketMonitorServerTest.test_lifecycle_review_does_not_duplicate_observed_market -v
```

Expected result: the zero-history test fails because `cex_markets` remains
empty; the existing-history test passes or exposes no duplicate.

- [ ] **Step 3: Implement the minimal validated seed**

Add this private helper immediately before
`overlay_cex_instrument_lifecycle()` in `dashboard/server.py`:

```python
def _lifecycle_only_cex_market(
    market_id: str,
    review: dict[str, Any],
) -> dict[str, Any]:
    token = review.get("token_symbol")
    exchange = review.get("exchange")
    instrument = review.get("instrument")
    if not all(
        isinstance(value, str) and value == value.strip() and bool(value)
        for value in (token, exchange, instrument)
    ):
        raise ValueError("CEX lifecycle review identity is incomplete")
    expected_market_id = cex_market_id(exchange, instrument)
    if exchange != "crypto_com" or expected_market_id != market_id:
        raise ValueError("CEX lifecycle review identity does not match the market")
    return {
        "token_symbol": token,
        "market": "cex",
        "venue": exchange,
        "instrument": instrument,
        "observation_count": 0,
        "observation_days": 0,
        "price_points": [],
    }
```

After the manifest/configured-set validation and before the lifecycle loop,
index current payload identities and append only missing reviews in sorted ID
order:

```python
existing_market_ids = {
    cex_market_id(market.get("venue"), market.get("instrument"))
    for market in result["cex_markets"]
}
for market_id in sorted(reviews):
    if market_id in existing_market_ids:
        continue
    result["cex_markets"].append(
        _lifecycle_only_cex_market(market_id, reviews[market_id])
    )
    existing_market_ids.add(market_id)
```

Do not add values for price, volume, return, volatility, coverage, spread,
depth, or dates. The existing lifecycle loop is responsible for setting every
current fact to null and attaching canonical evidence.

- [ ] **Step 4: Add malformed-identity and stale-evidence tests**

Add one test that maps a `CAKE/USDT` review under
`cex:crypto_com:EIGEN/USDT` and expects `ValueError` containing `identity does
not match`. Add one stale-root test with `now=2026-08-03T00:00:00Z` and assert
the materialized market has:

```python
self.assertEqual(
    market["current_listing_status"],
    "official_catalog_evidence_stale",
)
self.assertEqual(market["depth_status"], "needs_review")
self.assertEqual(market["historical_observation_count"], 0)
```

Run the same fresh review through payloads whose metadata declares two
different selected windows and assert the projected market-ID set is identical
in both results. This proves date selection cannot create or remove the
current lifecycle identity.

- [ ] **Step 5: Add end-to-end catalog, quality, and ranking assertions**

Build this payload with one observed Binance CAKE market and the zero-history
Crypto.com CAKE review:

```python
payload = {
    "metadata": {
        "available_start": "2026-07-31",
        "available_end": "2026-07-31",
        "source_date_ranges": {},
        "sources": [],
        "storage": "sqlite",
    },
    "cex_markets": [{
        "token_symbol": "CAKE",
        "market": "cex",
        "venue": "binance",
        "instrument": "CAKE/USDT",
        "price_usd": 1.0,
        "volume_usd": 100.0,
        "coverage_ratio": 1.0,
        "observation_count": 1,
        "observation_days": 1,
        "requested_window_days": 1,
        "price_points": [{"date": "2026-07-31", "price_usd": 1.0}],
        "depth_status": "observed",
    }],
    "dex_pools": [],
}
review = self.crypto_com_lifecycle_review("CAKE")
evidence = self.lifecycle_evidence([review])
```

Then run:

```python
projected = server.overlay_cex_instrument_lifecycle(
    payload,
    {review["market_id"]: review},
    lifecycle_evidence=evidence,
    now=datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc),
)
finalized = server.finalize_fact_contract(projected)
catalog = market_facts.catalog_from_market_payload(finalized)
catalog_summary = server.catalog_summary_from_catalog(catalog)
```

Assert all of the following:

```python
crypto = next(
    market for market in catalog["markets"]
    if market["market_id"] == "cex:crypto_com:CAKE/USDT"
)
self.assertEqual(crypto["quality_status"], "critical")
self.assertIn("inactive_cex_instrument", crypto["quality_flags"])
self.assertEqual(crypto["historical_observation_count"], 0)
self.assertIsNone(crypto["observation_days"])
self.assertEqual(catalog_summary["metadata"]["market_count"], 2)
self.assertEqual(finalized["tokens"][0]["primary_cex_id"], "binance|CAKE/USDT")
self.assertEqual(finalized["tokens"][0]["aggregate_cex_volume_usd"], 100.0)
self.assertEqual(
    evidence["configured_market_ids_sha256"],
    server.configured_market_ids_sha256({crypto["market_id"]}),
)
```

For the projected `crypto` catalog market, call the quality functions exactly
as follows:

```python
facts = {
    "daily": server._daily_quality_fact(
        crypto,
        {"window_start": "2026-07-01", "window_end": "2026-07-31"},
    ),
    "depth": server._depth_quality_fact(crypto),
    "execution": server._execution_quality_fact(
        crypto,
        {"snapshot": None, "error_code": None},
    ),
}
for fact in facts.values():
    self.assertEqual(
        fact["reason_code"],
        "instrument_absent_from_current_catalog",
    )
    self.assertFalse(fact["retryable"])
    self.assertTrue(any(
        flag["code"] == "inactive_cex_instrument"
        for flag in fact["quality_flags"]
    ))
```

- [ ] **Step 6: Run the focused backend and release-contract tests**

Run:

```bash
python3 -m unittest tests.test_dashboard.MarketMonitorServerTest -v
python3 -m unittest tests.test_release_smoke -v
```

Expected result: all tests pass and the release checker itself has no code
diff.

- [ ] **Step 7: Document the read-time lifecycle-only fact boundary**

Add this paragraph after the exact CEX identity discussion in
`docs/market-facts-contract.md`:

```markdown
A configured Crypto.com identity with a validated official absence review but
no historical source row remains catalog-visible through a lifecycle-only
read-time projection. The projection stores no database or CSV row, contributes
no aggregate or ranking value, and publishes no candle, price, volume, depth,
execution result, or delisting date. Historical observation count is zero;
current facts remain null and non-retryable. Upbit identities are outside this
projection.
```

- [ ] **Step 8: Commit the backend contract**

Run:

```bash
git add dashboard/server.py tests/test_dashboard.py docs/market-facts-contract.md
git commit -m "fix(catalog): project zero-history lifecycle markets"
```

---

### Task 2: Explain lifecycle-specific daily N/A values

**Files:**
- Modify: `dashboard/static/app.js:2057-2070,2576-2665`
- Test: `tests/test_dashboard_frontend.py:4740-4895`

**Interfaces:**
- Consumes: `market.current_listing_reason_code`,
  `market.current_listing_checked_at`, `DAILY_QUALITY_REASON_LABELS`, and
  `naFactMarkup(reason, context)`.
- Produces: `dailyMarketMissingReason(market, fallback) -> string`, used for
  Markets-page daily close and daily USD-volume N/A disclosures.

- [ ] **Step 1: Add a failing lifecycle daily-N/A frontend test**

Add this test to `DashboardFrontendContractTest`:

```python
def test_lifecycle_only_daily_na_explains_absence_without_refresh(self):
    result = run_app_javascript(
        """
const market = {
  token_symbol: "CAKE",
  market_id: "cex:crypto_com:CAKE/USDT",
  market_type: "cex",
  venue: "crypto_com",
  instrument: "CAKE/USDT",
  current_listing_reason_code: "instrument_absent_from_current_catalog",
  current_listing_checked_at: "2026-08-01T07:22:59+00:00",
};
const reason = dailyMarketMissingReason(
  market,
  "No finite daily close is available for this market in the selected window.",
);
const html = naFactMarkup(reason, {
  token: "CAKE",
  marketId: market.market_id,
  marketLabel: "CEX · crypto_com · CAKE/USDT",
  factLabel: "daily close",
});
console.log(JSON.stringify({ reason, html }));
"""
    )
    self.assertIn("absent from the official current exchange catalog", result["reason"])
    self.assertIn("N/A, not zero", result["reason"])
    self.assertIn("2026-08-01", result["reason"])
    self.assertIn('aria-label="N/A reason', result["html"])
    self.assertNotIn("na-refresh-action", result["html"])
```

Also assert that a market with no lifecycle reason returns the exact fallback,
and that `official_catalog_evidence_stale` says the evidence is older than 36
hours rather than claiming current absence. Read the
`renderWorkspaceMarkets()` source slice from `APP_PATH` and assert it contains
two calls to `dailyMarketMissingReason(`, covering daily close and daily USD
volume.

- [ ] **Step 2: Run the frontend test and prove the helper is missing**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_lifecycle_only_daily_na_explains_absence_without_refresh -v
```

Expected result: FAIL with `ReferenceError: dailyMarketMissingReason is not defined`.

- [ ] **Step 3: Implement the lifecycle-aware daily reason helper**

Add after `snapshotMissingReason()` in `dashboard/static/app.js`:

```javascript
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
```

In `renderWorkspaceMarkets()`, replace the fixed daily-close and daily-volume
reasons with:

```javascript
dailyMarketMissingReason(
  market,
  "No finite daily close is available for this market in the selected window.",
)
```

and:

```javascript
dailyMarketMissingReason(
  market,
  "No finite daily USD volume is available for this market in the selected window.",
)
```

Do not pass `retryable: true` or a refresh fact/action for either disclosure.

- [ ] **Step 4: Run focused frontend and N/A regressions**

Run:

```bash
python3 -m unittest \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_lifecycle_only_daily_na_explains_absence_without_refresh \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_execution_na_disclosure_uses_canonical_scenario_reason \
  tests.test_dashboard_frontend.DashboardFrontendContractTest.test_comparison_and_liquidity_summary_na_values_disclose_exact_reason -v
```

Expected result: all tests pass.

- [ ] **Step 5: Commit the frontend explanation**

Run:

```bash
git add dashboard/static/app.js tests/test_dashboard_frontend.py
git commit -m "fix(ui): explain lifecycle-only daily N/A"
```

---

### Task 3: Verify the complete branch before publication

**Files:**
- Verify only: all tracked source and test files

**Interfaces:**
- Consumes: both implementation commits and the existing release/test tooling.
- Produces: a clean, Python-3.8-compatible branch whose release checker still fails closed on the preserved Upbit exception but no longer has a lifecycle-catalog mismatch.

- [ ] **Step 1: Run source-format and import checks**

Run:

```bash
git diff --check
python3 -m py_compile dashboard/server.py dashboard/market_facts.py scripts/check_dashboard_release.py
python3 -c "import dashboard.server; import dashboard.market_facts"
```

Expected result: all commands exit zero.

- [ ] **Step 2: Run the complete local suite**

Run:

```bash
python3 -m unittest discover -s tests -v
```

Expected result: every test passes; environment-specific tests may be reported
as skipped, with no failure or error.

- [ ] **Step 3: Review exact scope and source-data immutability**

Run:

```bash
git diff e925ee9541f674d1674a44c19594c52c79600347 -- dashboard/server.py dashboard/static/app.js tests docs/market-facts-contract.md
git status --short
```

Confirm there is no diff in collectors, migration code, Upbit configuration,
SQLite/CSV data, quarantine files, or `scripts/check_dashboard_release.py`.
The worktree must be clean after the two commits.

- [ ] **Step 4: Obtain two-stage code review**

Request one specification-compliance review against
`docs/superpowers/specs/2026-08-01-lifecycle-only-catalog-design.md`, then one
code-quality review focused on duplicate identities, null-vs-zero semantics,
ranking exclusion, stale-evidence behavior, and Python 3.8. Resolve every
Critical or Important finding with a new failing test and a separate explicit
commit message.

- [ ] **Step 5: Push every implementation commit and add verified comments**

Run:

```bash
git push origin codex/critical-quality-sorting-token-refresh
```

For each pushed implementation SHA, add a GitHub commit comment describing the
fact contract, tests, and Upbit boundary. Use Ego Browser task space `14` to
open each commit page and submit the corresponding concrete note:

```text
AI implementation note: This commit implements the approved lifecycle-only
catalog contract. Zero-history Crypto.com absence reviews remain all-N/A,
non-retryable, and excluded from ranking. Upbit and source data are unchanged.
Focused backend and release-contract regressions passed.

AI implementation note: This commit adds lifecycle-specific daily N/A
explanations without exposing a refresh action. Missing values remain null,
and Upbit and source data are unchanged. Focused frontend N/A regressions
passed.
```

Verify the rendered `#commitcomment-...` URL before reporting the push complete.

---

### Task 4: Deploy and verify production without weakening the gate

**Files:**
- Production checkout: `/home/ugs/workspace/cex-dex-market-monitor-v1`
- Production data: `/home/ugs/workspace/cex-dex-market-monitor-v1/data/local`
- Service: `cex-dex-dashboard.service`
- Timers: `cex-dex-depth.timer`, `cex-dex-daily.timer`

**Interfaces:**
- Consumes: the reviewed GitHub branch SHA and unchanged production fact files.
- Produces: a deployed catalog with 519 markets under the current evidence set, 30 exact configured Crypto.com identities, unchanged Upbit inventory, current source freshness, and browser-visible lifecycle N/A reasons.

- [ ] **Step 1: Capture the rollback and immutability fence**

Before changing the production checkout, record:

```bash
git -C /home/ugs/workspace/cex-dex-market-monitor-v1 rev-parse HEAD
git -C /home/ugs/workspace/cex-dex-market-monitor-v1 status --short
systemctl --user is-active cex-dex-dashboard.service
systemctl --user list-timers --all --no-pager
```

Create the rollback branch and immutable fence with:

```bash
LIFECYCLE_PREVIOUS_SHA="$(git -C /home/ugs/workspace/cex-dex-market-monitor-v1 rev-parse HEAD)"
LIFECYCLE_PREVIOUS_SHORT_SHA="$(git -C /home/ugs/workspace/cex-dex-market-monitor-v1 rev-parse --short=7 HEAD)"
git -C /home/ugs/workspace/cex-dex-market-monitor-v1 branch \
  "codex/rollback-pre-lifecycle-only-catalog-${LIFECYCLE_PREVIOUS_SHORT_SHA}" \
  "${LIFECYCLE_PREVIOUS_SHA}"
sha256sum \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/cex_exchange_volume_daily.csv \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/dex_pool_volume_daily.csv \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/market_facts.sqlite3 \
  > /tmp/lifecycle-catalog-data-before.sha256
```

Fetch `/api/markets/catalog`, sort the market IDs beginning with `cex:upbit:`,
and save that JSON list with:

```bash
/usr/bin/python3 -c 'import json; from urllib.request import urlopen; payload=json.load(urlopen("http://127.0.0.1:8766/api/markets/catalog", timeout=30)); print(json.dumps(sorted(m["market_id"] for m in payload["markets"] if m["market_id"].startswith("cex:upbit:")), separators=(",", ":")))' \
  > /tmp/lifecycle-catalog-upbit-before.json
```

This deployment must not change either fence.

- [ ] **Step 2: Fast-forward and run the production Python 3.8 preflight**

Fetch and fast-forward the production checkout to the exact reviewed branch
SHA:

```bash
git fetch origin codex/critical-quality-sorting-token-refresh
git checkout codex/critical-quality-sorting-token-refresh
git merge --ff-only origin/codex/critical-quality-sorting-token-refresh
LIFECYCLE_EXPECTED_SHA="$(git rev-parse origin/codex/critical-quality-sorting-token-refresh)"
test "$(git rev-parse HEAD)" = "${LIFECYCLE_EXPECTED_SHA}"
```

Then run with `/usr/bin/python3`:

```bash
/usr/bin/python3 --version
/usr/bin/python3 -m py_compile dashboard/server.py dashboard/market_facts.py scripts/check_dashboard_release.py
/usr/bin/python3 -c "import dashboard.server; import dashboard.market_facts"
/usr/bin/python3 -m unittest discover -s tests -v
```

Expected result: Python 3.8.10, successful compile/import, and the complete
production test suite passing.

- [ ] **Step 3: Restart only the dashboard application**

Run:

```bash
systemctl --user restart cex-dex-dashboard.service
systemctl --user is-active cex-dex-dashboard.service
systemctl --user is-active cex-dex-depth.timer
systemctl --user is-active cex-dex-daily.timer
```

Do not run a data migration or collector as part of this code-only deployment.
Both timers must remain active.

- [ ] **Step 4: Verify health, identity, lifecycle parity, and unchanged data**

Compute the prospective identities from the deployed checkout, then run the
public checks:

```bash
LIFECYCLE_RELEASE_SHA="$(git rev-parse HEAD)"
LIFECYCLE_ASSET_SHA="$(/usr/bin/python3 -c 'from dashboard.server import static_asset_sha; print(static_asset_sha())')"
python3 scripts/check_dashboard_health.py --url http://43.156.102.166:8765/health --timeout 15
python3 scripts/check_dashboard_release.py \
  --base-url http://43.156.102.166:8765 \
  --timeout 30 \
  --expected-application-sha "${LIFECYCLE_RELEASE_SHA}" \
  --expected-asset-sha "${LIFECYCLE_ASSET_SHA}"
```

The health check must report `data_status=current`. The unmodified release
checker is expected to stop only at `Full catalog market is not a configured
Upbit exact identity` while the user-approved Upbit/KRW identities remain.

Independently fetch `/api/markets/summary` and `/api/markets/catalog`, then run
these exact assertions in a read-only Python process:

```python
import json
from urllib.request import urlopen

from scripts.cex_instrument_lifecycle import configured_market_ids_sha256

with urlopen("http://127.0.0.1:8766/api/markets/summary", timeout=30) as response:
    summary = json.load(response)
with urlopen("http://127.0.0.1:8766/api/markets/catalog", timeout=30) as response:
    full_catalog = json.load(response)
crypto_com_market_ids = {
    market["market_id"]
    for market in full_catalog["markets"]
    if market["market_id"].startswith("cex:crypto_com:")
}
assert len(crypto_com_market_ids) == 30
assert configured_market_ids_sha256(crypto_com_market_ids) == (
    summary["metadata"]["cex_instrument_lifecycle"]
    ["configured_market_ids_sha256"]
)
assert len(full_catalog["markets"]) == 519
```

For each lifecycle-only token, fetch the all-scope quality endpoint and prove
the exact market and non-retryable facts are present:

```python
from urllib.parse import urlencode

for token in ("CAKE", "EIGEN", "ETHFI", "JTO", "MORPHO"):
    query = urlencode({"token": token, "scope": "all"})
    with urlopen(
        "http://127.0.0.1:8766/api/markets/quality?" + query,
        timeout=30,
    ) as response:
        quality = json.load(response)
    market_id = f"cex:crypto_com:{token}/USDT"
    market = next(
        row for row in quality["markets"]
        if row["market_id"] == market_id
    )
    assert market["quality_status"] == "critical"
    assert any(
        flag["code"] == "inactive_cex_instrument"
        for flag in market["quality_flags"]
    )
    for fact_name in ("daily", "depth", "execution"):
        fact = market["facts"][fact_name]
        assert fact["reason_code"] == "instrument_absent_from_current_catalog"
        assert fact["retryable"] is False
    assert market["facts"]["tvl"]["retryable"] is False
```

Recreate the sorted Upbit market-ID JSON as
`/tmp/lifecycle-catalog-upbit-after.json` and rerun the data hash fence:

```bash
/usr/bin/python3 -c 'import json; from urllib.request import urlopen; payload=json.load(urlopen("http://127.0.0.1:8766/api/markets/catalog", timeout=30)); print(json.dumps(sorted(m["market_id"] for m in payload["markets"] if m["market_id"].startswith("cex:upbit:")), separators=(",", ":")))' \
  > /tmp/lifecycle-catalog-upbit-after.json
sha256sum \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/cex_exchange_volume_daily.csv \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/dex_pool_volume_daily.csv \
  /home/ugs/workspace/cex-dex-market-monitor-v1/data/local/market_facts.sqlite3 \
  > /tmp/lifecycle-catalog-data-after.sha256
```

Require both comparison commands to exit zero:

```bash
cmp /tmp/lifecycle-catalog-upbit-before.json /tmp/lifecycle-catalog-upbit-after.json
cmp /tmp/lifecycle-catalog-data-before.sha256 /tmp/lifecycle-catalog-data-after.sha256
```

- [ ] **Step 5: Perform real-browser desktop and mobile QA**

Reuse Ego Browser task space `14` and verify:

1. CAKE, EIGEN, ETHFI, JTO, and MORPHO Markets pages contain the Crypto.com
   lifecycle-only entry.
2. Daily price, daily volume, depth, and execution values display N/A with an
   adjacent information control and the official-catalog reason.
3. None of those absence disclosures contains `Refresh this fact`.
4. Data Quality shows `inactive_cex_instrument`, official check time, and exact
   market identity.
5. The five entries are not selected as Market A/B defaults and do not become
   Screener metric values.
6. At 390 x 844, the page has zero document-level horizontal overflow and N/A
   details expand inline.

- [ ] **Step 6: Report the exact release boundary**

Report GitHub SHA, server SHA, asset SHA, tests, service/timer state, catalog
count, five lifecycle-only identities, unchanged Upbit/data hashes, and browser
evidence. Do not claim the strict release checker passed while the preserved
Upbit identity exception remains. State separately that the lifecycle-catalog
mismatch is closed and all remaining checker assertions pass under diagnostic
continuation.
