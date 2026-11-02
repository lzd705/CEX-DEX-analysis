import csv
import json
import tempfile
import unittest
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

from scripts.fact_quality import (
    CEX_REQUIRED_COLUMNS,
    DEX_REQUIRED_COLUMNS,
    build_report,
    issue_id,
)
from scripts.market_lifecycle_reviews import (
    DEFAULT_REVIEW_PATH,
    LifecycleReviewError,
    load_lifecycle_reviews,
)


def write_csv(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def cex_row(day):
    return {
        "date": day,
        "token_symbol": "AAVE",
        "exchange": "upbit",
        "cex_symbol": "AAVE/USDT",
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "102",
        "base_volume": "10",
        "quote_volume_usd": "1020",
    }


def review_payload(issue):
    market = issue["market"]
    return {
        "schema": "market_lifecycle_reviews/v1",
        "generated_at_utc": "2026-07-30T02:00:00Z",
        "review_count": 1,
        "reviews": [
            {
                "review_id": "aave-upbit-usdt-no-recent-candle",
                "revision": 1,
                "supersedes_revision": None,
                "review_status": "disposed",
                "reviewed_issue_id": issue["issue_id"],
                "original_category": issue["category"],
                "original_reason_code": issue["reason_code"],
                "market_id": market["market_id"],
                "market_type": market["market_type"],
                "token_symbol": market["token_symbol"],
                "issue_date": issue["date"],
                "disposition_status": "source_no_observation",
                "disposition_reason_code": "no_candles",
                "market_lifecycle": "listed_quote_market_dormant",
                "evidence_status": "primary_confirmed",
                "review_method": "manual_primary_source_cross_check",
                "review_actor": "test-reviewer",
                "reviewed_at_utc": "2026-07-30T02:00:00Z",
                "disposition_note": (
                    "The exact market remains listed and the official source "
                    "contains no candle for this reviewed date."
                ),
                "source_checks": [
                    {
                        "source_kind": "official_exchange_ticker",
                        "url": (
                            "https://api.upbit.com/v1/ticker?"
                            "markets=USDT-AAVE"
                        ),
                        "http_status": 200,
                        "response_sha256": "a" * 64,
                        "checked_at_utc": "2026-07-30T01:59:00Z",
                        "observations": {
                            "market": "USDT-AAVE",
                            "last_trade_date_utc": "2026-07-23",
                        },
                    }
                ],
            }
        ],
    }


def dex_review_payload():
    pool = "0xbec22ca49e499c752542ca242b708c97739e4baf"
    return {
        "schema": "market_lifecycle_reviews/v1",
        "generated_at_utc": "2026-07-30T03:00:00Z",
        "review_count": 1,
        "reviews": [
            {
                "review_id": "grt-arbitrum-exact-pool-no-recent-candle",
                "revision": 1,
                "supersedes_revision": None,
                "review_status": "disposed",
                "reviewed_issue_id": "1" * 20,
                "original_category": "stale_market_unknown",
                "original_reason_code": "stale_market_lifecycle_unknown",
                "market_id": (
                    "dex:arbitrum:uniswap_v3_arbitrum:{}:GRT".format(pool)
                ),
                "market_type": "dex",
                "token_symbol": "GRT",
                "issue_date": "2026-07-29",
                "disposition_status": "source_no_observation",
                "disposition_reason_code": "no_candles",
                "market_lifecycle": "pool_exists_dormant",
                "evidence_status": "declared_source_confirmed",
                "review_method": "manual_declared_source_cross_check",
                "review_actor": "test-reviewer",
                "reviewed_at_utc": "2026-07-30T03:00:00Z",
                "disposition_note": (
                    "The exact pool exists but its latest exact daily candle "
                    "precedes the reviewed issue date."
                ),
                "source_checks": [
                    {
                        "source_kind": "declared_dex_market_data_api",
                        "url": (
                            "https://api.geckoterminal.com/api/v2/networks/"
                            "arbitrum/pools/{}".format(pool)
                        ),
                        "http_status": 200,
                        "response_sha256": "b" * 64,
                        "checked_at_utc": "2026-07-30T02:58:00Z",
                        "observations": {
                            "pool_address": pool,
                            "dex_id": "uniswap_v3_arbitrum",
                            "base_token_id": (
                                "arbitrum_"
                                "0x9623063377ad1b27544c965ccd7342f7ea7e88c7"
                            ),
                            "quote_token_id": (
                                "arbitrum_"
                                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                            ),
                        },
                    },
                    {
                        "source_kind": "declared_dex_daily_ohlcv_api",
                        "url": (
                            "https://api.geckoterminal.com/api/v2/networks/"
                            "arbitrum/pools/{}/ohlcv/day?aggregate=1&"
                            "currency=usd&limit=30".format(pool)
                        ),
                        "http_status": 200,
                        "response_sha256": "c" * 64,
                        "checked_at_utc": "2026-07-30T02:59:00Z",
                        "observations": {
                            "latest_candle_timestamp": 1784678400,
                            "latest_candle_date_utc": "2026-07-22",
                        },
                    },
                ],
            }
        ],
    }


class MarketLifecycleReviewTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.cex_path = self.root / "cex.csv"
        self.dex_path = self.root / "dex.csv"
        write_csv(
            self.cex_path,
            sorted(CEX_REQUIRED_COLUMNS),
            [
                cex_row("2026-07-21"),
                cex_row("2026-07-22"),
                cex_row("2026-07-23"),
            ],
        )
        write_csv(
            self.dex_path,
            sorted(DEX_REQUIRED_COLUMNS),
            [],
        )

    def tearDown(self):
        self.temporary.cleanup()

    def baseline_stale_issue(self):
        baseline = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=None,
            today=date(2026, 7, 29),
        )
        return next(
            issue
            for issue in baseline["issues"]
            if issue["category"] == "stale_market_unknown"
        )

    def write_reviews(self, payload, name="reviews.json"):
        review_path = self.root / name
        review_path.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        return review_path

    def test_committed_grt_and_ldo_reviews_are_valid_and_exact_date(self):
        reviews, metadata = load_lifecycle_reviews(DEFAULT_REVIEW_PATH)

        self.assertEqual(metadata["status"], "accepted")
        self.assertEqual(
            metadata["generated_at_utc"],
            "2026-07-31T17:20:30Z",
        )
        self.assertEqual(metadata["revision_count"], 3)
        self.assertEqual(metadata["active_disposition_count"], 2)
        self.assertEqual(
            {review["review_id"] for review in reviews},
            {
                "grt-arbitrum-uniswap-v3-no-recent-candle",
                "upbit-ldo-usdt-listed-no-recent-candle",
            },
        )
        self.assertEqual(
            {review["issue_date"] for review in reviews},
            {"2026-07-29"},
        )
        upbit = next(
            review for review in reviews if review["market_type"] == "cex"
        )
        grt = next(
            review for review in reviews if review["market_type"] == "dex"
        )
        self.assertEqual(grt["revision"], 2)
        grt_pool = next(
            check
            for check in grt["source_checks"]
            if check["source_kind"] == "declared_dex_market_data_api"
        )
        self.assertEqual(
            {
                key: grt_pool["observations"][key]
                for key in ("base_token_id", "quote_token_id")
            },
            {
                "base_token_id": (
                    "arbitrum_"
                    "0x9623063377ad1b27544c965ccd7342f7ea7e88c7"
                ),
                "quote_token_id": (
                    "arbitrum_"
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
            },
        )
        inventory = next(
            check
            for check in upbit["source_checks"]
            if check["source_kind"] == "official_exchange_market_inventory"
        )
        self.assertEqual(
            inventory["observations"]["present_market_codes"],
            ["USDT-LDO"],
        )
        self.assertEqual(
            inventory["observations"]["absent_market_codes"],
            ["KRW-LDO"],
        )
        expected_issue_ids = {
            "dex:arbitrum:uniswap_v3_arbitrum:0xbec22ca49e499c752542ca242b708c97739e4baf:GRT": issue_id(
                "stale_market_unknown",
                "stale_market_lifecycle_unknown",
                (
                    "dex:arbitrum:uniswap_v3_arbitrum:"
                    "0xbec22ca49e499c752542ca242b708c97739e4baf:GRT"
                ),
                "2026-07-29",
                {
                    "last_observed_date": "2026-07-22",
                    "missing_since": "2026-07-23",
                    "last_active_reference_window_start": "2026-07-16",
                    "last_active_reference_window_end": "2026-07-22",
                    "last_active_observation_count": 3,
                    "explicit_inactive_metadata": False,
                },
            ),
            "cex:upbit:LDO/USDT": issue_id(
                "stale_market_unknown",
                "stale_market_lifecycle_unknown",
                "cex:upbit:LDO/USDT",
                "2026-07-29",
                {
                    "last_observed_date": "2026-07-06",
                    "missing_since": "2026-07-07",
                    "last_active_reference_window_start": "2026-06-30",
                    "last_active_reference_window_end": "2026-07-06",
                    "last_active_observation_count": 3,
                    "explicit_inactive_metadata": False,
                },
            ),
        }
        self.assertEqual(
            {
                review["market_id"]: review["reviewed_issue_id"]
                for review in reviews
            },
            expected_issue_ids,
        )

    def test_exact_disposition_resolves_manual_review_without_future_carry(self):
        baseline = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=None,
            today=date(2026, 7, 29),
        )
        stale = next(
            issue
            for issue in baseline["issues"]
            if issue["category"] == "stale_market_unknown"
        )
        review_path = self.root / "reviews.json"
        review_path.write_text(
            json.dumps(review_payload(stale), indent=2) + "\n",
            encoding="utf-8",
        )

        disposed = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=review_path,
            today=date(2026, 7, 29),
        )

        issue = next(
            item
            for item in disposed["issues"]
            if item["category"] == "source_no_observation"
        )
        self.assertEqual(issue["status"], "source_no_observation")
        self.assertEqual(issue["reason_code"], "no_candles")
        self.assertFalse(issue["retryable"])
        self.assertEqual(
            issue["details"]["manual_lifecycle_review"]["review_id"],
            "aave-upbit-usdt-no-recent-candle",
        )
        self.assertEqual(disposed["status"], "ok")
        self.assertEqual(disposed["manual_review_queue"], [])
        self.assertEqual(
            disposed["summary"]["stale_market_unknown_count"],
            0,
        )
        self.assertEqual(
            disposed["summary"]["source_no_observation_count"],
            1,
        )
        self.assertEqual(
            disposed["lifecycle_review_source"]["applied_review_ids"],
            ["aave-upbit-usdt-no-recent-candle"],
        )
        self.assertFalse(disposed["markets"][0]["stale_market_unknown"])
        self.assertTrue(disposed["markets"][0]["source_no_observation"])

        next_day = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=review_path,
            today=date(2026, 7, 30),
        )
        self.assertEqual(
            next_day["lifecycle_review_source"]["applied_disposition_count"],
            0,
        )
        self.assertEqual(
            next_day["summary"]["stale_market_unknown_count"],
            1,
        )
        self.assertEqual(next_day["summary"]["manual_review_count"], 1)
        self.assertFalse(
            any(
                issue["category"] == "source_no_observation"
                for issue in next_day["issues"]
            )
        )

    def test_unofficial_or_cross_market_source_host_is_rejected(self):
        baseline = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=None,
            today=date(2026, 7, 29),
        )
        stale = next(
            issue
            for issue in baseline["issues"]
            if issue["category"] == "stale_market_unknown"
        )
        payload = review_payload(stale)
        payload["reviews"][0]["source_checks"][0]["url"] = (
            "https://api.geckoterminal.com/api/v2/networks/eth/pools/0xpool"
        )
        review_path = self.root / "invalid-reviews.json"
        review_path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(
            LifecycleReviewError,
            "source host does not match",
        ):
            load_lifecycle_reviews(review_path)

    def test_revision_sequence_and_review_count_are_fail_closed(self):
        baseline = build_report(
            self.cex_path,
            self.dex_path,
            lifecycle_reviews=None,
            today=date(2026, 7, 29),
        )
        stale = next(
            issue
            for issue in baseline["issues"]
            if issue["category"] == "stale_market_unknown"
        )
        payload = review_payload(stale)
        payload["review_count"] = 2
        review_path = self.root / "truncated-reviews.json"
        review_path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(
            LifecycleReviewError,
            "count is inconsistent",
        ):
            load_lifecycle_reviews(review_path)

    def test_cex_market_identity_must_be_canonical_and_match_token(self):
        stale = self.baseline_stale_issue()
        cases = (
            ("cex:upbit:AAVE", "AAVE"),
            ("cex:Upbit:AAVE/USDT", "AAVE"),
            ("cex:upbit:aave/USDT", "AAVE"),
            ("cex:upbit:AAVE/USDT", "LDO"),
            ("cex:binance:AAVE/USDT", "AAVE"),
        )
        for position, (market_id, token_symbol) in enumerate(cases):
            with self.subTest(market_id=market_id, token_symbol=token_symbol):
                payload = review_payload(stale)
                review = payload["reviews"][0]
                review["market_id"] = market_id
                review["token_symbol"] = token_symbol
                path = self.write_reviews(payload, "bad-cex-{}.json".format(position))
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(path)

    def test_cex_ticker_query_and_observation_bind_exact_instrument(self):
        stale = self.baseline_stale_issue()
        mutations = (
            (
                "wrong path",
                "url",
                "https://api.upbit.com/v1/trades/ticks?markets=USDT-AAVE",
            ),
            (
                "wrong query",
                "url",
                "https://api.upbit.com/v1/ticker?markets=KRW-AAVE",
            ),
            (
                "duplicate query",
                "url",
                (
                    "https://api.upbit.com/v1/ticker?markets=USDT-AAVE&"
                    "markets=KRW-AAVE"
                ),
            ),
            ("wrong observation", "market", "KRW-AAVE"),
        )
        for position, (label, field, value) in enumerate(mutations):
            with self.subTest(case=label):
                payload = review_payload(stale)
                check = payload["reviews"][0]["source_checks"][0]
                if field == "url":
                    check["url"] = value
                else:
                    check["observations"][field] = value
                path = self.write_reviews(
                    payload,
                    "bad-ticker-binding-{}.json".format(position),
                )
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(path)

    def test_cex_ticker_requires_a_strictly_earlier_trade_date(self):
        stale = self.baseline_stale_issue()
        issue_day = stale["date"]
        next_day = (
            date.fromisoformat(issue_day) + timedelta(days=1)
        ).isoformat()
        for trade_day in (issue_day, next_day):
            with self.subTest(last_trade_date_utc=trade_day):
                payload = review_payload(stale)
                payload["reviews"][0]["source_checks"][0]["observations"][
                    "last_trade_date_utc"
                ] = trade_day
                path = self.write_reviews(
                    payload,
                    "bad-trade-day-{}.json".format(trade_day),
                )
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(path)

    def test_dex_market_identity_is_canonical_and_matches_trailing_token(self):
        base = dex_review_payload()
        pool = "0xbec22ca49e499c752542ca242b708c97739e4baf"
        cases = (
            "dex:Arbitrum:uniswap_v3_arbitrum:{}:GRT".format(pool),
            "dex:arbitrum:uniswap_v3_arbitrum:{}:LDO".format(pool),
            "dex:arbitrum:uniswap_v3_arbitrum:{}:GRT".format(pool.upper()),
            "dex:arbitrum:uniswap_v3_arbitrum:{}".format(pool),
        )
        for position, market_id in enumerate(cases):
            with self.subTest(market_id=market_id):
                payload = deepcopy(base)
                payload["reviews"][0]["market_id"] = market_id
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "bad-dex-identity-{}.json".format(position),
                        )
                    )

    def test_upbit_inventory_is_auxiliary_and_must_include_exact_target(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        ticker = payload["reviews"][0]["source_checks"][0]
        inventory = {
            "source_kind": "official_exchange_market_inventory",
            "url": "https://api.upbit.com/v1/market/all?is_details=true",
            "http_status": 200,
            "response_sha256": "d" * 64,
            "checked_at_utc": "2026-07-30T01:58:00Z",
            "observations": {
                "present_market_codes": ["KRW-AAVE"],
                "absent_market_codes": ["USDT-AAVE"],
            },
        }

        inventory_only = deepcopy(payload)
        inventory_only["reviews"][0]["source_checks"] = [inventory]
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(inventory_only, "inventory-only.json")
            )

        wrong_inventory = deepcopy(payload)
        wrong_inventory["reviews"][0]["source_checks"] = [inventory, ticker]
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(wrong_inventory, "wrong-inventory.json")
            )

        exact_inventory = deepcopy(payload)
        exact_inventory_check = deepcopy(inventory)
        exact_inventory_check["observations"]["present_market_codes"] = [
            "USDT-AAVE"
        ]
        exact_inventory_check["observations"]["absent_market_codes"] = [
            "KRW-AAVE"
        ]
        exact_inventory["reviews"][0]["source_checks"] = [
            exact_inventory_check,
            ticker,
        ]
        reviews, _ = load_lifecycle_reviews(
            self.write_reviews(exact_inventory, "exact-inventory.json")
        )
        self.assertEqual(len(reviews), 1)

    def test_upbit_inventory_lists_are_bounded_canonical_unique_and_disjoint(self):
        stale = self.baseline_stale_issue()
        base = review_payload(stale)
        ticker = base["reviews"][0]["source_checks"][0]
        inventory = {
            "source_kind": "official_exchange_market_inventory",
            "url": "https://api.upbit.com/v1/market/all?is_details=true",
            "http_status": 200,
            "response_sha256": "d" * 64,
            "checked_at_utc": "2026-07-30T01:58:00Z",
            "observations": {
                "present_market_codes": ["USDT-AAVE"],
                "absent_market_codes": ["KRW-AAVE"],
            },
        }
        cases = (
            ("present-not-list", "present_market_codes", "USDT-AAVE"),
            ("present-non-string", "present_market_codes", ["USDT-AAVE", 1]),
            ("present-malformed", "present_market_codes", ["USDT-AAVE", "krw-AAVE"]),
            ("present-duplicate", "present_market_codes", ["USDT-AAVE", "USDT-AAVE"]),
            ("absent-not-list", "absent_market_codes", "KRW-AAVE"),
            ("absent-non-string", "absent_market_codes", ["KRW-AAVE", 1]),
            ("absent-malformed", "absent_market_codes", ["KRW/AAVE"]),
            ("absent-duplicate", "absent_market_codes", ["KRW-AAVE", "KRW-AAVE"]),
            ("target-contradiction", "absent_market_codes", ["USDT-AAVE"]),
        )
        for position, (label, field, value) in enumerate(cases):
            with self.subTest(case=label):
                payload = deepcopy(base)
                invalid_inventory = deepcopy(inventory)
                invalid_inventory["observations"][field] = value
                payload["reviews"][0]["source_checks"] = [
                    invalid_inventory,
                    ticker,
                ]
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "bad-inventory-list-{}.json".format(position),
                        )
                    )

        overlapping = deepcopy(base)
        overlapping_inventory = deepcopy(inventory)
        overlapping_inventory["observations"]["present_market_codes"] = [
            "USDT-AAVE",
            "KRW-AAVE",
        ]
        overlapping_inventory["observations"]["absent_market_codes"] = [
            "KRW-AAVE"
        ]
        overlapping["reviews"][0]["source_checks"] = [
            overlapping_inventory,
            ticker,
        ]
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(overlapping, "overlapping-inventory.json")
            )

        oversized = deepcopy(base)
        oversized_inventory = deepcopy(inventory)
        oversized_inventory["observations"]["present_market_codes"] = [
            "USDT-AAVE"
        ] + ["Q{:04d}-AAVE".format(index) for index in range(1_001)]
        oversized["reviews"][0]["source_checks"] = [
            oversized_inventory,
            ticker,
        ]
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(oversized, "oversized-inventory.json")
            )

    def test_evidence_must_be_checked_after_issue_day_completes(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        review = payload["reviews"][0]
        issue_day = stale["date"]
        review["reviewed_at_utc"] = issue_day + "T23:59:59Z"
        review["source_checks"][0]["checked_at_utc"] = (
            issue_day + "T23:59:58Z"
        )
        path = self.write_reviews(payload, "same-day-evidence.json")
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(path)

    def test_source_kinds_are_allowlisted_unique_and_market_specific(self):
        stale = self.baseline_stale_issue()
        for position, source_kind in enumerate(
            ("unknown_source", "declared_dex_daily_ohlcv_api")
        ):
            with self.subTest(source_kind=source_kind):
                payload = review_payload(stale)
                payload["reviews"][0]["source_checks"][0][
                    "source_kind"
                ] = source_kind
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "bad-source-kind-{}.json".format(position),
                        )
                    )

        duplicate = review_payload(stale)
        duplicate["reviews"][0]["source_checks"].append(
            deepcopy(duplicate["reviews"][0]["source_checks"][0])
        )
        duplicate["reviews"][0]["source_checks"][1]["url"] = (
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE#duplicate"
        )
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(duplicate, "duplicate-source-kind.json")
            )

    def test_urls_reject_ports_fragments_credentials_and_extra_components(self):
        stale = self.baseline_stale_issue()
        unsafe_urls = (
            "https://api.upbit.com:443/v1/ticker?markets=USDT-AAVE",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE#fragment",
            "https://user@api.upbit.com/v1/ticker?markets=USDT-AAVE",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE&extra=1",
            "https://api.upbit.com/v1/ticker?markets=%55SDT-AAVE",
            " https://api.upbit.com/v1/ticker?markets=USDT-AAVE",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE ",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE\n",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE\t",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE\x00",
            "https://api.upbit.com/v1/ticker?markets=USDT-AAVE​",
        )
        for position, unsafe_url in enumerate(unsafe_urls):
            with self.subTest(url=unsafe_url):
                payload = review_payload(stale)
                payload["reviews"][0]["source_checks"][0]["url"] = unsafe_url
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "unsafe-url-{}.json".format(position),
                        )
                    )

    def test_unknown_ledger_revision_and_source_fields_fail_closed(self):
        stale = self.baseline_stale_issue()
        locations = ("root", "revision", "source")
        for position, location in enumerate(locations):
            with self.subTest(location=location):
                payload = review_payload(stale)
                if location == "root":
                    payload["unexpected"] = True
                elif location == "revision":
                    payload["reviews"][0]["unexpected"] = True
                else:
                    payload["reviews"][0]["source_checks"][0][
                        "unexpected"
                    ] = True
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "unknown-field-{}.json".format(position),
                        )
                    )

    def test_dex_market_and_checks_bind_exact_network_pool_and_dex(self):
        base = dex_review_payload()
        pool = "0xbec22ca49e499c752542ca242b708c97739e4baf"
        other_pool = "0x" + "2" * 40
        mutations = (
            ("wrong network", (0, "url"), (
                "https://api.geckoterminal.com/api/v2/networks/ethereum/"
                "pools/{}".format(pool)
            )),
            ("wrong pool path", (0, "url"), (
                "https://api.geckoterminal.com/api/v2/networks/arbitrum/"
                "pools/{}".format(other_pool)
            )),
            ("wrong observation pool", (0, "pool_address"), other_pool),
            ("wrong observation dex", (0, "dex_id"), "camelot"),
            ("wrong OHLCV observation pool", (1, "pool_address"), other_pool),
            ("wrong OHLCV observation dex", (1, "dex_id"), "camelot"),
            ("wrong ohlcv pool", (1, "url"), (
                "https://api.geckoterminal.com/api/v2/networks/arbitrum/"
                "pools/{}/ohlcv/day?aggregate=1&currency=usd&limit=30".format(
                    other_pool
                )
            )),
            ("historical cutoff", (1, "url"), (
                "https://api.geckoterminal.com/api/v2/networks/arbitrum/"
                "pools/{}/ohlcv/day?aggregate=1&currency=usd&limit=30&"
                "before_timestamp=1784678400".format(pool)
            )),
        )
        for position, (label, target, value) in enumerate(mutations):
            with self.subTest(case=label):
                payload = deepcopy(base)
                check = payload["reviews"][0]["source_checks"][target[0]]
                if target[1] == "url":
                    check["url"] = value
                else:
                    check["observations"][target[1]] = value
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "bad-dex-binding-{}.json".format(position),
                        )
                    )

    def test_dex_pool_token_evidence_accepts_target_as_base_or_quote(self):
        base_payload = dex_review_payload()
        base_reviews, _ = load_lifecycle_reviews(
            self.write_reviews(base_payload, "dex-target-base.json")
        )
        base_observations = base_reviews[0]["source_checks"][0][
            "observations"
        ]
        self.assertEqual(
            base_observations["base_token_id"],
            "arbitrum_0x9623063377ad1b27544c965ccd7342f7ea7e88c7",
        )

        quote_payload = deepcopy(base_payload)
        quote_observations = quote_payload["reviews"][0]["source_checks"][0][
            "observations"
        ]
        quote_observations["base_token_id"], quote_observations[
            "quote_token_id"
        ] = (
            quote_observations["quote_token_id"],
            quote_observations["base_token_id"],
        )
        quote_reviews, _ = load_lifecycle_reviews(
            self.write_reviews(quote_payload, "dex-target-quote.json")
        )
        self.assertEqual(
            quote_reviews[0]["source_checks"][0]["observations"][
                "quote_token_id"
            ],
            "arbitrum_0x9623063377ad1b27544c965ccd7342f7ea7e88c7",
        )

    def test_latest_dex_review_requires_both_pool_token_identities(self):
        removals = (
            ("base_token_id",),
            ("quote_token_id",),
            ("base_token_id", "quote_token_id"),
        )
        for position, removed_fields in enumerate(removals):
            with self.subTest(removed_fields=removed_fields):
                payload = dex_review_payload()
                observations = payload["reviews"][0]["source_checks"][0][
                    "observations"
                ]
                for field in removed_fields:
                    observations.pop(field)
                with self.assertRaisesRegex(
                    LifecycleReviewError,
                    "Token identity|reviewed Token",
                ):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "missing-dex-token-identity-{}.json".format(
                                position
                            ),
                        )
                    )

    def test_dex_exact_pool_with_wrong_reviewed_token_is_rejected(self):
        payload = dex_review_payload()
        review = payload["reviews"][0]
        review["token_symbol"] = "AAVE"
        review["market_id"] = review["market_id"].rsplit(":", 1)[0] + ":AAVE"

        try:
            load_lifecycle_reviews(
                self.write_reviews(payload, "wrong-dex-token.json")
            )
        except LifecycleReviewError as error:
            self.assertIn("reviewed Token", str(error))
        else:
            self.fail("exact pool evidence for another Token was accepted")

    def test_dex_review_requires_trusted_chain_contract_configuration(self):
        token_chain_path = self.root / "token-chains-without-grt.csv"
        token_chain_path.write_text(
            "token_symbol,chain,contract_address,notes\n"
            "AAVE,arbitrum,"
            "0xba5ddd1f9d7f570dc94a51479a000e3bce967196,"
            "test fixture\n",
            encoding="utf-8",
        )
        try:
            load_lifecycle_reviews(
                self.write_reviews(
                    dex_review_payload(),
                    "dex-missing-trusted-token.json",
                ),
                token_chain_path=token_chain_path,
            )
        except LifecycleReviewError as error:
            self.assertIn("trusted Token contract identity", str(error))
        except TypeError as error:
            self.fail(
                "validator cannot receive trusted Token configuration: {}".format(
                    error
                )
            )
        else:
            self.fail("DEX review without trusted Token identity was accepted")

    def test_dex_revision_cannot_rewrite_issue_market_type_market_or_date(self):
        cases = (
            ("reviewed_issue_id", "f" * 20),
            ("issue_date", "2026-07-28"),
            ("market_id", "other_pool"),
            ("market_type", "cex"),
        )
        for position, (field, value) in enumerate(cases):
            with self.subTest(field=field):
                payload = dex_review_payload()
                payload["generated_at_utc"] = "2026-07-30T05:00:00Z"
                revision_2 = deepcopy(payload["reviews"][0])
                revision_2["revision"] = 2
                revision_2["supersedes_revision"] = 1
                revision_2["reviewed_at_utc"] = "2026-07-30T04:00:00Z"
                if field == "market_id":
                    other_pool = "0x" + "2" * 40
                    revision_2["market_id"] = (
                        "dex:arbitrum:uniswap_v3_arbitrum:{}:GRT".format(
                            other_pool
                        )
                    )
                    pool_check, ohlcv_check = revision_2["source_checks"]
                    pool_check["url"] = (
                        "https://api.geckoterminal.com/api/v2/networks/"
                        "arbitrum/pools/{}".format(other_pool)
                    )
                    pool_check["observations"]["pool_address"] = other_pool
                    ohlcv_check["url"] = (
                        "https://api.geckoterminal.com/api/v2/networks/"
                        "arbitrum/pools/{}/ohlcv/day?aggregate=1&"
                        "currency=usd&limit=30".format(other_pool)
                    )
                elif field == "market_type":
                    revision_2["market_type"] = "cex"
                    revision_2["market_id"] = "cex:upbit:GRT/USDT"
                    revision_2["market_lifecycle"] = (
                        "listed_quote_market_dormant"
                    )
                    revision_2["evidence_status"] = "primary_confirmed"
                    revision_2["review_method"] = (
                        "manual_primary_source_cross_check"
                    )
                    revision_2["source_checks"] = [
                        {
                            "source_kind": "official_exchange_ticker",
                            "url": (
                                "https://api.upbit.com/v1/ticker?"
                                "markets=USDT-GRT"
                            ),
                            "http_status": 200,
                            "response_sha256": "e" * 64,
                            "checked_at_utc": "2026-07-30T03:59:00Z",
                            "observations": {
                                "market": "USDT-GRT",
                                "last_trade_date_utc": "2026-07-22",
                            },
                        }
                    ]
                else:
                    revision_2[field] = value
                payload["reviews"].append(revision_2)
                payload["review_count"] = 2
                with self.assertRaisesRegex(
                    LifecycleReviewError,
                    "revision identity is immutable",
                ):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "mutated-dex-identity-{}.json".format(position),
                        )
                    )

    def test_dex_requires_pool_and_daily_ohlcv_checks(self):
        payload = dex_review_payload()
        for keep_position in (0, 1):
            with self.subTest(keep_position=keep_position):
                only_one = deepcopy(payload)
                only_one["reviews"][0]["source_checks"] = [
                    only_one["reviews"][0]["source_checks"][keep_position]
                ]
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            only_one,
                            "incomplete-dex-evidence-{}.json".format(keep_position),
                        )
                    )

    def test_dex_latest_candle_must_be_consistent_and_precede_issue(self):
        cases = (
            ("2026-07-29", 1785283200),
            ("2026-07-30", 1785369600),
            ("2026-07-22", 1784764800),
            ("2026-07-22", True),
        )
        for position, (latest_day, timestamp) in enumerate(cases):
            with self.subTest(latest_day=latest_day, timestamp=timestamp):
                payload = dex_review_payload()
                observations = payload["reviews"][0]["source_checks"][1][
                    "observations"
                ]
                observations["latest_candle_date_utc"] = latest_day
                observations["latest_candle_timestamp"] = timestamp
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "bad-dex-candle-{}.json".format(position),
                        )
                    )

    def test_boolean_values_cannot_impersonate_integer_contract_fields(self):
        stale = self.baseline_stale_issue()
        mutations = ("revision", "http_status")
        for position, field in enumerate(mutations):
            with self.subTest(field=field):
                payload = review_payload(stale)
                if field == "revision":
                    payload["reviews"][0]["revision"] = True
                else:
                    payload["reviews"][0]["source_checks"][0][field] = True
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "boolean-contract-{}.json".format(position),
                        )
                    )

    def test_review_count_and_supersedes_revision_require_real_integers(self):
        stale = self.baseline_stale_issue()
        for position, invalid_count in enumerate((True, 1.0)):
            with self.subTest(review_count=invalid_count):
                payload = review_payload(stale)
                payload["review_count"] = invalid_count
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "invalid-review-count-{}.json".format(position),
                        )
                    )

        for position, invalid_supersedes in enumerate((True, 1.0)):
            with self.subTest(supersedes_revision=invalid_supersedes):
                payload = review_payload(stale)
                payload["generated_at_utc"] = "2026-07-30T03:00:00Z"
                revision_2 = deepcopy(payload["reviews"][0])
                revision_2["revision"] = 2
                revision_2["supersedes_revision"] = invalid_supersedes
                revision_2["reviewed_at_utc"] = "2026-07-30T03:00:00Z"
                revision_2["source_checks"][0]["checked_at_utc"] = (
                    "2026-07-30T02:59:00Z"
                )
                payload["reviews"].append(revision_2)
                payload["review_count"] = 2
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "invalid-supersedes-{}.json".format(position),
                        )
                    )

    def test_utc_fields_use_one_exact_portable_grammar(self):
        stale = self.baseline_stale_issue()
        cases = (
            ("generated", "2026-07-30T02:00:00+00:00"),
            ("generated", "2026-W31-4T02:00:00Z"),
            ("reviewed", "2026-07-30 02:00:00Z"),
            ("reviewed", "2026-07-30T02:00:00.000Z"),
            ("checked", "2026-07-30T01:59:00+00:00"),
            ("checked", "2026-07-30T01:59:00.000Z"),
        )
        for position, (location, timestamp) in enumerate(cases):
            with self.subTest(location=location, timestamp=timestamp):
                payload = review_payload(stale)
                if location == "generated":
                    payload["generated_at_utc"] = timestamp
                elif location == "reviewed":
                    payload["reviews"][0]["reviewed_at_utc"] = timestamp
                else:
                    payload["reviews"][0]["source_checks"][0][
                        "checked_at_utc"
                    ] = timestamp
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "invalid-utc-{}.json".format(position),
                        )
                    )

    def test_generation_time_cannot_precede_a_review(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        payload["generated_at_utc"] = "2026-07-30T01:59:59Z"
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(payload, "generation-before-review.json")
            )

    def test_market_type_lifecycle_evidence_and_method_must_agree(self):
        mutations = (
            ("market_lifecycle", "listed_quote_market_dormant"),
            ("evidence_status", "primary_confirmed"),
            ("review_method", "manual_primary_source_cross_check"),
        )
        for position, (field, value) in enumerate(mutations):
            with self.subTest(field=field):
                payload = dex_review_payload()
                payload["reviews"][0][field] = value
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "wrong-dex-combo-{}.json".format(position),
                        )
                    )

    def test_revision_identity_is_immutable_for_every_frozen_field(self):
        stale = self.baseline_stale_issue()
        mutations = {
            "reviewed_issue_id": "f" * 20,
            "original_category": "other_category",
            "original_reason_code": "other_reason",
            "market_id": "cex:upbit:AAVE/BTC",
            "market_type": "dex",
            "token_symbol": "LDO",
            "issue_date": "2026-07-27",
        }
        for position, (field, value) in enumerate(mutations.items()):
            with self.subTest(field=field):
                payload = review_payload(stale)
                payload["generated_at_utc"] = "2026-07-30T03:00:00Z"
                revision_2 = deepcopy(payload["reviews"][0])
                revision_2["revision"] = 2
                revision_2["supersedes_revision"] = 1
                revision_2["reviewed_at_utc"] = "2026-07-30T03:00:00Z"
                revision_2[field] = value
                if field == "market_id":
                    revision_2["source_checks"][0]["url"] = (
                        "https://api.upbit.com/v1/ticker?markets=BTC-AAVE"
                    )
                    revision_2["source_checks"][0]["observations"][
                        "market"
                    ] = "BTC-AAVE"
                if field == "market_type":
                    dex_revision = dex_review_payload()["reviews"][0]
                    revision_2["market_id"] = (
                        "dex:arbitrum:uniswap_v3_arbitrum:"
                        "0xbec22ca49e499c752542ca242b708c97739e4baf:AAVE"
                    )
                    revision_2["market_lifecycle"] = "pool_exists_dormant"
                    revision_2["evidence_status"] = "declared_source_confirmed"
                    revision_2["review_method"] = (
                        "manual_declared_source_cross_check"
                    )
                    revision_2["source_checks"] = deepcopy(
                        dex_revision["source_checks"]
                    )
                if field == "token_symbol":
                    revision_2["market_id"] = "cex:upbit:LDO/USDT"
                    revision_2["source_checks"][0]["url"] = (
                        "https://api.upbit.com/v1/ticker?markets=USDT-LDO"
                    )
                    revision_2["source_checks"][0]["observations"][
                        "market"
                    ] = "USDT-LDO"
                payload["reviews"].append(revision_2)
                payload["review_count"] = 2
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "mutated-identity-{}.json".format(position),
                        )
                    )

    def test_revision_may_update_evidence_note_or_withdraw_disposition(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        payload["generated_at_utc"] = "2026-07-30T03:00:00Z"
        revision_2 = deepcopy(payload["reviews"][0])
        revision_2["revision"] = 2
        revision_2["supersedes_revision"] = 1
        revision_2["reviewed_at_utc"] = "2026-07-30T03:00:00Z"
        revision_2["source_checks"][0]["checked_at_utc"] = (
            "2026-07-30T02:59:00Z"
        )
        revision_2["source_checks"][0]["response_sha256"] = "e" * 64
        revision_2["disposition_note"] = "Updated exact-source evidence note."
        payload["reviews"].append(revision_2)
        payload["review_count"] = 2

        reviews, metadata = load_lifecycle_reviews(
            self.write_reviews(payload, "valid-revision.json")
        )
        self.assertEqual(metadata["revision_count"], 2)
        self.assertEqual(reviews[0]["revision"], 2)
        self.assertEqual(
            reviews[0]["disposition_note"],
            "Updated exact-source evidence note.",
        )

        withdrawn = deepcopy(payload)
        withdrawn_revision = withdrawn["reviews"][1]
        withdrawn_revision["review_status"] = "withdrawn"
        withdrawn_revision["disposition_status"] = None
        withdrawn_revision["disposition_reason_code"] = None
        withdrawn_revision["market_lifecycle"] = None
        reviews, metadata = load_lifecycle_reviews(
            self.write_reviews(withdrawn, "withdrawn-revision.json")
        )
        self.assertEqual(reviews, [])
        self.assertEqual(metadata["active_disposition_count"], 0)

    def test_revision_review_times_must_increase(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        revision_2 = deepcopy(payload["reviews"][0])
        revision_2["revision"] = 2
        revision_2["supersedes_revision"] = 1
        payload["reviews"].append(revision_2)
        payload["review_count"] = 2
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(payload, "nonmonotonic-revision.json")
            )

    def test_duplicate_active_reviewed_issue_id_is_rejected(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        duplicate = deepcopy(payload["reviews"][0])
        duplicate["review_id"] = "aave-upbit-second-active-review"
        duplicate["disposition_note"] = "A second active disposition is invalid."
        payload["reviews"].append(duplicate)
        payload["review_count"] = 2
        with self.assertRaises(LifecycleReviewError):
            load_lifecycle_reviews(
                self.write_reviews(payload, "duplicate-active-issue.json")
            )

    def test_reviewed_issue_id_has_one_lineage_across_entire_ledger(self):
        stale = self.baseline_stale_issue()
        for position, statuses in enumerate(
            (("disposed", "withdrawn"), ("withdrawn", "withdrawn"))
        ):
            with self.subTest(statuses=statuses):
                payload = review_payload(stale)
                first = payload["reviews"][0]
                second = deepcopy(first)
                second["review_id"] = "aave-upbit-forked-review-chain"
                second["disposition_note"] = "Forked issue lineage is invalid."
                payload["reviews"].append(second)
                payload["review_count"] = 2
                for review, status in zip(payload["reviews"], statuses):
                    review["review_status"] = status
                    if status == "withdrawn":
                        review["disposition_status"] = None
                        review["disposition_reason_code"] = None
                        review["market_lifecycle"] = None
                with self.assertRaises(LifecycleReviewError):
                    load_lifecycle_reviews(
                        self.write_reviews(
                            payload,
                            "forked-issue-lineage-{}.json".format(position),
                        )
                    )

    def test_build_report_rejects_wrong_source_evidence_fail_closed(self):
        stale = self.baseline_stale_issue()
        payload = review_payload(stale)
        payload["reviews"][0]["source_checks"][0]["url"] = (
            "https://api.upbit.com/v1/ticker?markets=KRW-AAVE"
        )
        path = self.write_reviews(payload, "wrong-evidence-report.json")
        with self.assertRaises(LifecycleReviewError):
            build_report(
                self.cex_path,
                self.dex_path,
                lifecycle_reviews=path,
                today=date(2026, 7, 29),
            )


if __name__ == "__main__":
    unittest.main()
