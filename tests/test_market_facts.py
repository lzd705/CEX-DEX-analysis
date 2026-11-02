import json
import unittest
from decimal import Decimal
from pathlib import Path

from dashboard.market_facts import (
    absolute_price_spread,
    build_token_summaries,
    catalog_from_market_payload,
    compare_daily_rows,
    decimal_adjust,
    dex_market_id,
    dex_pool_id,
    market_quality_assessment,
    market_series_statistics,
    select_primary_market,
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

    def test_dex_identity_only_lowercases_evm_addresses(self):
        self.assertEqual(
            dex_pool_id("ETH", "Uniswap_V3", "0xAbCd"),
            "dex:eth:uniswap_v3:0xabcd",
        )
        self.assertEqual(
            dex_market_id("Solana", "Orca", "AbCdEf", "btc"),
            "dex:solana:orca:AbCdEf:BTC",
        )

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

    def test_series_statistics_skips_gaps_for_daily_volatility(self):
        rows = [
            {"date": "2026-01-01", "close": 100},
            {"date": "2026-01-02", "close": 110},
            # The Jan 2 -> Jan 4 move must not be treated as a one-day return.
            {"date": "2026-01-04", "close": 121},
            {"date": "2026-01-05", "close": 133.1},
        ]

        result = market_series_statistics(
            rows,
            requested_start="2026-01-01",
            requested_end="2026-01-05",
        )

        self.assertEqual(result["first_observed_date"], "2026-01-01")
        self.assertEqual(result["latest_observed_date"], "2026-01-05")
        self.assertEqual(result["calendar_span_days"], 5)
        self.assertEqual(result["requested_window_days"], 5)
        self.assertEqual(result["observation_count"], 4)
        self.assertEqual(result["coverage_ratio"], 0.8)
        self.assertEqual(result["missing_calendar_days"], 1)
        self.assertEqual(result["return_interval_count"], 2)
        self.assertEqual(result["skipped_gap_interval_count"], 1)
        self.assertEqual(result["max_gap_days"], 1)
        self.assertAlmostEqual(result["window_return"], 0.331, places=12)
        self.assertAlmostEqual(result["daily_volatility"], 0.0, places=12)
        self.assertEqual(
            result["daily_volatility_method"],
            "adjacent_utc_daily_log_returns_only_v1",
        )

    def test_series_statistics_preserves_nulls_and_never_fills_a_gap(self):
        result = market_series_statistics(
            [
                {"date": "2026-01-01", "close": 10},
                {"date": "2026-01-02", "close": None},
                {"date": "2026-01-03", "close": 12},
            ],
            requested_start="2026-01-01",
            requested_end="2026-01-03",
        )

        self.assertEqual(result["observation_count"], 2)
        self.assertEqual(result["coverage_ratio"], 2 / 3)
        self.assertEqual(result["return_interval_count"], 0)
        self.assertEqual(result["skipped_gap_interval_count"], 1)
        self.assertIsNone(result["daily_volatility"])
        self.assertAlmostEqual(result["window_return"], 0.2)

    def test_quality_flags_are_auditable_and_keep_missing_depth_null(self):
        result = market_quality_assessment(
            {
                "market": "dex",
                "dex_depth_status": "unsupported_protocol",
                "tvl_usd": 50_000,
                "price_difference_bps": 750,
                "total_depth_10bps_usd": None,
            }
        )

        self.assertEqual(result["quality_status"], "critical")
        self.assertEqual(
            result["quality_flags"],
            [
                "depth_unsupported",
                "tiny_pool",
                "off_market_pool_state_price",
            ],
        )
        details = {
            detail["code"]: detail
            for detail in result["quality_flag_details"]
        }
        self.assertEqual(details["tiny_pool"]["threshold"], 100_000.0)
        self.assertEqual(
            details["off_market_pool_state_price"]["threshold"],
            500.0,
        )
        self.assertEqual(
            details["off_market_pool_state_price"]["severity"],
            "critical",
        )
        self.assertNotIn("zero_depth_10bps", result["quality_flags"])

    def test_quality_distinguishes_warning_deviation_and_low_coverage(self):
        result = market_quality_assessment(
            {
                "market": "dex",
                "dex_depth_status": "observed",
                "tvl_usd": 1_000_000,
                "price_difference_bps": 250,
                "coverage_ratio": 0.5,
                "total_depth_10bps_usd": 10_000,
            }
        )

        details = {
            detail["code"]: detail
            for detail in result["quality_flag_details"]
        }
        self.assertEqual(result["quality_status"], "warning")
        self.assertEqual(details["off_market_pool_state_price"]["severity"], "warning")
        self.assertEqual(details["off_market_pool_state_price"]["threshold"], 100.0)
        self.assertEqual(details["low_daily_coverage"]["threshold"], 0.8)

    def test_quality_flags_zero_depth_only_when_snapshot_is_measured(self):
        observed = market_quality_assessment(
            {
                "market": "cex",
                "depth_status": "observed",
                "total_depth_10bps_usd": 0,
                "spread_bps": 25,
            }
        )
        failed = market_quality_assessment(
            {
                "market": "cex",
                "depth_status": "failed",
                "total_depth_10bps_usd": 0,
            }
        )

        self.assertIn("zero_depth_10bps", observed["quality_flags"])
        self.assertNotIn("zero_depth_10bps", failed["quality_flags"])
        self.assertIn("depth_failed", failed["quality_flags"])

    def test_quality_computes_pool_state_deviation_when_source_omits_bps(self):
        result = market_quality_assessment(
            {
                "market": "dex",
                "dex_depth_status": "observed",
                "pool_state_price_usd": 1.10,
                "source_target_price_usd": 1.00,
            }
        )

        self.assertIn(
            "off_market_pool_state_price",
            result["quality_flags"],
        )
        detail = next(
            item
            for item in result["quality_flag_details"]
            if item["code"] == "off_market_pool_state_price"
        )
        self.assertAlmostEqual(detail["observed_value"], 1_000)

    def test_primary_market_selection_balances_volume_and_quality(self):
        high_volume_low_quality = {
            "market": "cex",
            "token_symbol": "UNI",
            "venue": "wide",
            "instrument": "UNI/USD",
            "price_usd": 10,
            "volume_usd": 1_000,
            "coverage_ratio": 1,
            "depth_status": "unavailable",
            "spread_bps": 150,
        }
        lower_volume_high_quality = {
            "market": "cex",
            "token_symbol": "UNI",
            "venue": "deep",
            "instrument": "UNI/USDT",
            "price_usd": 10,
            "volume_usd": 500,
            "coverage_ratio": 1,
            "depth_status": "observed",
            "spread_bps": 2,
        }

        selected, reason = select_primary_market(
            [high_volume_low_quality, lower_volume_high_quality]
        )

        self.assertIs(selected, lower_volume_high_quality)
        self.assertEqual(reason["method"], "quality_weighted_primary_v1")
        self.assertEqual(reason["candidate_count"], 2)
        self.assertEqual(reason["selected_market_id"], "deep|UNI/USDT")
        self.assertEqual(
            set(reason["components"]),
            {
                "window_volume_share",
                "coverage_ratio",
                "quote_quality",
                "depth_support",
            },
        )
        self.assertEqual(reason["inputs"]["window_volume_usd"], 500)
        self.assertEqual(reason["inputs"]["coverage_ratio"], 1)
        self.assertEqual(reason["inputs"]["quote_quality_score"], 1)
        self.assertEqual(reason["inputs"]["depth_support_score"], 1)

    def test_primary_market_selection_rejects_negative_volume_instead_of_zero_filling(self):
        invalid = {
            "market": "cex",
            "token_symbol": "UNI",
            "venue": "binance",
            "instrument": "UNI/USDT",
            "price_usd": 10,
            "volume_usd": -1,
            "coverage_ratio": 1,
            "depth_status": "observed",
        }

        with self.assertRaisesRegex(ValueError, "negative volume"):
            select_primary_market([invalid])

        with self.assertRaisesRegex(ValueError, "negative volume"):
            build_token_summaries([invalid], [])

    def test_primary_market_penalizes_off_market_pool_price(self):
        off_market = {
            "market": "dex",
            "token_symbol": "UNI",
            "venue": "eth / off-market",
            "instrument": "UNI / USD",
            "pool_address": "0xoff",
            "price_usd": 10,
            "volume_usd": 1_000,
            "coverage_ratio": 1,
            "dex_depth_status": "observed",
            "price_difference_bps": 250,
            "tvl_usd": 1_000_000,
        }
        representative = {
            "market": "dex",
            "token_symbol": "UNI",
            "venue": "eth / representative",
            "instrument": "UNI / USDC",
            "pool_address": "0xrepresentative",
            "price_usd": 10,
            "volume_usd": 800,
            "coverage_ratio": 1,
            "dex_depth_status": "observed",
            "price_difference_bps": 2,
            "tvl_usd": 1_000_000,
        }

        selected, reason = select_primary_market([off_market, representative])

        self.assertIs(selected, representative)
        self.assertEqual(reason["inputs"]["quote_quality_score"], 1)

    def test_token_summary_separates_aggregate_and_selected_volume(self):
        def row(market, venue, instrument, volume, price, depth_status):
            result = {
                "market": market,
                "token_symbol": "UNI",
                "venue": venue,
                "instrument": instrument,
                "price_usd": price,
                "volume_usd": volume,
                "coverage_ratio": 1,
                "price_points": [
                    {"date": "2026-01-01", "price_usd": price},
                ],
            }
            if market == "cex":
                result["depth_status"] = depth_status
            else:
                result["dex_depth_status"] = depth_status
                result["pool_address"] = instrument
            return result

        result = build_token_summaries(
            [
                row("cex", "binance", "UNI/USDT", 1_000, 10, "observed"),
                row("cex", "coinbase", "UNI/USD", 500, 10, "observed"),
            ],
            [
                row("dex", "eth / uniswap", "0xpool1", 300, 10.1, "observed"),
                row("dex", "eth / uniswap", "0xpool2", 200, 9.9, "observed"),
            ],
        )[0]

        self.assertEqual(result["aggregate_cex_volume_usd"], 1_500)
        self.assertEqual(result["aggregate_dex_volume_usd"], 500)
        self.assertEqual(result["aggregate_volume_usd"], 2_000)
        self.assertEqual(result["aggregate_dex_volume_share"], 0.25)
        self.assertEqual(result["selected_cex_volume_usd"], 1_000)
        self.assertEqual(result["selected_dex_volume_usd"], 300)
        self.assertEqual(result["selected_pair_volume_usd"], 1_300)
        self.assertEqual(result["cex_volume_usd"], 1_500)
        self.assertEqual(result["observed_dex_share"], 0.25)

    def test_token_summary_does_not_convert_all_missing_volume_to_zero(self):
        def row(market, volume):
            result = {
                "market": market,
                "token_symbol": "UNI",
                "venue": "binance" if market == "cex" else "eth / uniswap",
                "instrument": "UNI/USDT" if market == "cex" else "UNI/USDC",
                "pool_address": None if market == "cex" else "0xpool",
                "price_usd": 10,
                "volume_usd": volume,
                "coverage_ratio": 1,
                "price_points": [{"date": "2026-01-01", "price_usd": 10}],
            }
            return result

        missing = build_token_summaries(
            [row("cex", None)],
            [row("dex", None)],
        )[0]
        measured_zero = build_token_summaries(
            [row("cex", 0)],
            [row("dex", 0)],
        )[0]

        self.assertIsNone(missing["aggregate_cex_volume_usd"])
        self.assertIsNone(missing["aggregate_dex_volume_usd"])
        self.assertIsNone(missing["aggregate_volume_usd"])
        self.assertIsNone(missing["aggregate_dex_volume_share"])
        self.assertEqual(measured_zero["aggregate_cex_volume_usd"], 0)
        self.assertEqual(measured_zero["aggregate_dex_volume_usd"], 0)
        self.assertEqual(measured_zero["aggregate_volume_usd"], 0)
        self.assertIsNone(measured_zero["aggregate_dex_volume_share"])

    def test_catalog_distinguishes_market_series_from_physical_pools(self):
        def pool(token):
            return {
                "token_symbol": token,
                "market": "dex",
                "venue": "eth / uniswap",
                "instrument": f"{token} / OTHER",
                "pool_address": "0xpool",
                "price_points": [
                    {"date": "2026-01-01", "price_usd": 1},
                ],
                "latest_date": "2026-01-01",
                "observation_days": 1,
            }

        payload = {
            "metadata": {
                "available_start": "2026-01-01",
                "available_end": "2026-01-01",
                "sources": [],
                "storage": {"engine": "test"},
                "tvl_snapshot": {"pool_rows": 2},
                "dex_depth_snapshot": {"pool_rows": 2},
            },
            "cex_markets": [],
            "dex_pools": [pool("AAA"), pool("BBB")],
        }

        metadata = catalog_from_market_payload(payload)["metadata"]

        self.assertEqual(metadata["dex_market_series_rows"], 2)
        self.assertEqual(metadata["dex_unique_pool_count"], 1)
        self.assertEqual(metadata["tvl_snapshot"]["market_series_rows"], 2)
        self.assertEqual(metadata["tvl_snapshot"]["unique_pool_count"], 1)
        self.assertEqual(metadata["tvl_snapshot"]["pool_rows"], 2)
        self.assertIn(
            "Use market_series_rows",
            metadata["tvl_snapshot"]["pool_rows_deprecated"],
        )


if __name__ == "__main__":
    unittest.main()
