import csv
import json
import tempfile
import unittest
from datetime import date
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
        "generated_at_utc": "2026-07-29T02:00:00Z",
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
                "reviewed_at_utc": "2026-07-29T02:00:00Z",
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
                        "checked_at_utc": "2026-07-29T01:59:00Z",
                        "observations": {
                            "market": "USDT-AAVE",
                            "last_trade_date_utc": "2026-07-23",
                        },
                    }
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

    def test_committed_grt_and_ldo_reviews_are_valid_and_exact_date(self):
        reviews, metadata = load_lifecycle_reviews(DEFAULT_REVIEW_PATH)

        self.assertEqual(metadata["status"], "accepted")
        self.assertEqual(metadata["revision_count"], 2)
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


if __name__ == "__main__":
    unittest.main()
