import json
import unittest
from decimal import Decimal
from pathlib import Path

from dashboard.market_facts import (
    absolute_price_spread,
    catalog_from_market_payload,
    compare_daily_rows,
    decimal_adjust,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "market_known_answers.json"


class MarketFactKnownAnswerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_price_spread_known_answers(self):
        for case in self.fixture["price_spreads"]:
            with self.subTest(case=case["name"]):
                absolute, bps = absolute_price_spread(
                    case["price_a_usd"],
                    case["price_b_usd"],
                )
                self.assertEqual(
                    Decimal(str(absolute)),
                    Decimal(case["expected_absolute_spread_usd"]),
                )
                self.assertAlmostEqual(
                    bps,
                    float(case["expected_spread_bps"]),
                    places=12,
                )

    def test_decimal_adjustment_known_answers(self):
        for case in self.fixture["decimal_adjustments"]:
            with self.subTest(case=case["name"]):
                result = decimal_adjust(case["raw_amount"], case["decimals"])
                self.assertEqual(result, Decimal(case["expected_units"]))

    def test_decimal_adjustment_rejects_fractional_base_units(self):
        with self.assertRaises(ValueError):
            decimal_adjust("1.5", 6)

    def test_comparison_keeps_union_dates_and_does_not_fill(self):
        rows_a = [
            {"date": "2026-01-01", "price_usd": 100, "volume_usd": 1000},
            {"date": "2026-01-02", "price_usd": 102, "volume_usd": 1200},
        ]
        rows_b = [
            {"date": "2026-01-02", "price_usd": 105, "volume_usd": 400},
            {"date": "2026-01-03", "price_usd": 106, "volume_usd": None},
        ]

        result = compare_daily_rows(rows_a, rows_b)

        self.assertEqual([row["date"] for row in result], [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
        ])
        self.assertEqual(result[0]["missing_reason"], "market_b_missing")
        self.assertIsNone(result[0]["market_b"]["price_usd"])
        self.assertIsNone(result[0]["spread_bps"])
        self.assertEqual(result[1]["absolute_spread_usd"], 3.0)
        self.assertEqual(result[2]["missing_reason"], "market_a_missing")

    def test_catalog_rejects_duplicate_global_market_ids(self):
        market = {
            "token_symbol": "BTC",
            "market": "cex",
            "venue": "binance",
            "instrument": "BTC/USDT",
            "price_points": [{"date": "2026-01-01", "price_usd": 100}],
            "latest_date": "2026-01-01",
            "observation_days": 1,
        }
        payload = {
            "metadata": {
                "available_start": "2026-01-01",
                "available_end": "2026-01-01",
                "sources": [],
                "storage": {"engine": "test"},
            },
            "cex_markets": [market, dict(market)],
            "dex_pools": [],
        }

        with self.assertRaisesRegex(ValueError, "globally unique"):
            catalog_from_market_payload(payload)


if __name__ == "__main__":
    unittest.main()
