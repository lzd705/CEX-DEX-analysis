import csv
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import server
from scripts.fetch_cex_depth import DEPTH_COLUMNS_ALL
from scripts.market_database import build_database


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class MarketMonitorServerTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        data_dir = Path(self.temporary_directory.name)
        self.cex_path = data_dir / server.CEX_FILENAME
        self.dex_path = data_dir / server.DEX_FILENAME
        write_csv(
            self.cex_path,
            [
                "date",
                "token_symbol",
                "exchange",
                "cex_symbol",
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume_usd",
            ],
            [
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "close": "100",
                    "quote_volume_usd": "1000",
                },
                {
                    "date": "2026-01-02",
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "close": "102",
                    "quote_volume_usd": "1200",
                },
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "exchange": "okx",
                    "cex_symbol": "BTC/USDT",
                    "close": "99",
                    "quote_volume_usd": "100",
                },
                {
                    "date": "2026-01-02",
                    "token_symbol": "BTC",
                    "exchange": "okx",
                    "cex_symbol": "BTC/USDT",
                    "close": "101",
                    "quote_volume_usd": "100",
                },
            ],
        )
        write_csv(
            self.dex_path,
            [
                "date",
                "token_symbol",
                "chain",
                "dex",
                "pool_address",
                "pool_name",
                "open",
                "high",
                "low",
                "close",
                "dex_volume_usd",
                "pool_tvl_usd",
            ],
            [
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                    "pool_name": "WBTC / USDC",
                    "close": "101",
                    "dex_volume_usd": "300",
                    "pool_tvl_usd": "",
                },
                {
                    "date": "2026-01-02",
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                    "pool_name": "WBTC / USDC",
                    "close": "105",
                    "dex_volume_usd": "400",
                    "pool_tvl_usd": "5000",
                },
            ],
        )
        self.environment = {
            "MARKET_CEX_DATA": str(self.cex_path),
            "MARKET_DEX_DATA": str(self.dex_path),
        }
        self.tvl_path = data_dir / server.TVL_FILENAME
        self.depth_path = data_dir / server.CEX_DEPTH_FILENAME

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_parse_number_preserves_missing_values(self):
        self.assertIsNone(server.parse_number(""))
        self.assertIsNone(server.parse_number("nan"))
        self.assertEqual(server.parse_number("12.5"), 12.5)
        self.assertEqual(server.parse_number(12.5), 12.5)

    def test_payload_contains_only_market_facts_and_token_level_spread(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")

        self.assertEqual(payload["metadata"]["token_count"], 1)
        self.assertEqual(len(payload["cex_markets"]), 2)
        self.assertEqual(len(payload["dex_pools"]), 1)
        self.assertNotIn("factor_results", payload)
        self.assertAlmostEqual(payload["tokens"][0]["price_spread"], 105 / 102 - 1)
        self.assertEqual(payload["dex_pools"][0]["tvl_usd"], 5000)

    def test_payload_reports_source_specific_ranges_and_freshness(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")
            catalog = server.build_market_catalog()

        ranges = payload["metadata"]["source_date_ranges"]
        self.assertEqual(ranges["cex_daily"]["available_end"], "2026-01-02")
        self.assertEqual(ranges["dex_daily"]["available_end"], "2026-01-02")
        self.assertEqual(
            payload["metadata"]["freshness"]["common_comparable_end"],
            "2026-01-02",
        )
        self.assertEqual(payload["metadata"]["freshness"]["overall_status"], "stale")
        self.assertEqual(
            catalog["metadata"]["freshness"]["common_comparable_end"],
            "2026-01-02",
        )

    def test_point_in_time_tvl_snapshot_overlays_legacy_ohlcv_value(self):
        write_csv(
            self.tvl_path,
            [
                "snapshot_id",
                "observed_at",
                "token_symbol",
                "chain",
                "pool_address",
                "tvl_usd",
                "tvl_method",
                "source",
                "source_endpoint",
                "raw_response_sha256",
                "status",
            ],
            [
                {
                    "snapshot_id": "tvl-snapshot-1",
                    "observed_at": "2026-07-27T01:02:03+00:00",
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "pool_address": "0xpool",
                    "tvl_usd": "7654.32",
                    "tvl_method": "geckoterminal_reserve_in_usd",
                    "source": "GeckoTerminal API v2",
                    "source_endpoint": "https://example.test/pool",
                    "raw_response_sha256": "abc123",
                    "status": "observed",
                }
            ],
        )
        environment = {
            **self.environment,
            "MARKET_TVL_DATA": str(self.tvl_path),
        }

        with patch.dict(server.os.environ, environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")
            catalog = server.build_market_catalog()

        pool = payload["dex_pools"][0]
        self.assertEqual(pool["tvl_usd"], 7654.32)
        self.assertEqual(pool["tvl_status"], "observed")
        self.assertEqual(pool["tvl_observed_at"], "2026-07-27T01:02:03+00:00")
        self.assertEqual(payload["metadata"]["tvl_snapshot"]["matched_market_rows"], 1)
        catalog_pool = next(
            market for market in catalog["markets"] if market["market_type"] == "dex"
        )
        self.assertEqual(catalog_pool["tvl_usd"], 7654.32)
        self.assertEqual(catalog_pool["tvl_method"], "geckoterminal_reserve_in_usd")

    def test_point_in_time_cex_depth_overlays_cataloged_market(self):
        depth_row = {field: "" for field in DEPTH_COLUMNS_ALL}
        depth_row.update(
            {
                "snapshot_id": "depth-snapshot-1",
                "observed_at": "2026-07-27T02:03:04+00:00",
                "response_received_at": "2026-07-27T02:03:05+00:00",
                "token_symbol": "BTC",
                "exchange": "binance",
                "cex_symbol": "BTC/USDT",
                "source_instrument": "BTCUSDT",
                "source_quote_asset": "USDT",
                "quote_conversion_method": "USDT=USD proxy",
                "best_bid": "100",
                "best_ask": "100.1",
                "midpoint": "100.05",
                "spread_quote": "0.1",
                "spread_bps": "9.995002498750624",
                "total_depth_10bps_usd": "1000",
                "total_depth_25bps_usd": "2000",
                "total_depth_50bps_usd": "3000",
                "total_depth_100bps_usd": "4000",
                "depth_10bps_complete": "1",
                "depth_25bps_complete": "1",
                "depth_50bps_complete": "1",
                "depth_100bps_complete": "0",
                "depth_method": "midpoint_symmetric_quote_notional",
                "source_endpoint": "https://example.test/depth",
                "raw_response_sha256": "def456",
                "status": "partial",
            }
        )
        write_csv(self.depth_path, DEPTH_COLUMNS_ALL, [depth_row])
        environment = {
            **self.environment,
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
        }

        with patch.dict(server.os.environ, environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")
            catalog = server.build_market_catalog()

        binance = next(row for row in payload["cex_markets"] if row["venue"] == "binance")
        self.assertEqual(binance["depth_status"], "partial")
        self.assertEqual(binance["total_depth_100bps_usd"], 4000)
        self.assertFalse(binance["depth_100bps_complete"])
        self.assertEqual(payload["metadata"]["cex_depth_snapshot"]["matched_market_rows"], 1)
        catalog_binance = next(
            market
            for market in catalog["markets"]
            if market["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertEqual(catalog_binance["total_depth_100bps_usd"], 4000)
        self.assertEqual(catalog_binance["depth_status"], "partial")

    def test_catalog_identifies_markets_and_declares_fact_contract(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            catalog = server.build_market_catalog()

        self.assertEqual(catalog["metadata"]["catalog_version"], 1)
        self.assertEqual(catalog["metadata"]["time_grain"], "1 day, UTC")
        self.assertEqual(catalog["metadata"]["price_quote_asset"], "USD")
        self.assertIn("not order-book depth", catalog["metadata"]["semantic_boundary"])
        self.assertEqual(catalog["tokens"], ["BTC"])
        market_ids = {market["market_id"] for market in catalog["markets"]}
        self.assertEqual(
            market_ids,
            {
                "cex:binance:BTC/USDT",
                "cex:okx:BTC/USDT",
                "dex:eth:uniswap:0xpool",
            },
        )

    def test_comparison_returns_raw_daily_facts_absolute_spread_and_bps(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            result = server.build_market_comparison(
                "BTC",
                "cex:binance:BTC/USDT",
                "dex:eth:uniswap:0xpool",
                "2026-01-01",
                "2026-01-02",
            )

        self.assertEqual(result["metadata"]["comparison_days"], 2)
        self.assertEqual(result["market_a"]["price_quote_asset"], "USD")
        self.assertEqual(result["observations"][0]["market_a"]["price_usd"], 100)
        self.assertEqual(result["observations"][0]["market_a"]["volume_usd"], 1000)
        self.assertEqual(result["observations"][0]["market_b"]["price_usd"], 101)
        self.assertEqual(result["observations"][0]["market_b"]["volume_usd"], 300)
        self.assertEqual(result["observations"][0]["absolute_spread_usd"], 1)
        self.assertAlmostEqual(
            result["observations"][0]["spread_bps"],
            1 / 100.5 * 10_000,
        )

    def test_comparison_rejects_same_or_wrong_token_market(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            with self.assertRaisesRegex(ValueError, "must be different"):
                server.build_market_comparison(
                    "BTC",
                    "cex:binance:BTC/USDT",
                    "cex:binance:BTC/USDT",
                )
            with self.assertRaisesRegex(ValueError, "not cataloged"):
                server.build_market_comparison(
                    "ETH",
                    "cex:binance:BTC/USDT",
                    "dex:eth:uniswap:0xpool",
                )

    def test_date_window_limits_volume_and_coverage(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-02", "2026-01-02")

        primary_cex = payload["cex_markets"][0]
        self.assertEqual(primary_cex["volume_usd"], 1200)
        self.assertEqual(primary_cex["observation_days"], 1)
        self.assertIsNone(primary_cex["window_return"])

    def test_invalid_date_window_is_rejected(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            with self.assertRaises(ValueError):
                server.build_market_payload("2026-01-03", "2026-01-02")

    def test_spread_uses_latest_common_date_not_each_markets_latest_date(self):
        with self.cex_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(["2026-01-03", "BTC", "binance", "BTC/USDT", "", "", "", "200", "", "1500"])

        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-03")

        token = payload["tokens"][0]
        self.assertEqual(token["spread_date"], "2026-01-02")
        self.assertAlmostEqual(token["price_spread"], 105 / 102 - 1)

    def test_payload_cache_invalidates_when_source_file_changes(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            first = server.build_market_payload("2026-01-01", "2026-01-02")

            with self.cex_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(
                    ["2026-01-02", "BTC", "kraken", "BTC/USDT", "", "", "", "103", "", "300"]
                )

            second = server.build_market_payload("2026-01-01", "2026-01-02")

        self.assertEqual(len(first["cex_markets"]), 2)
        self.assertEqual(len(second["cex_markets"]), 3)

    def test_large_json_payload_uses_gzip_when_supported(self):
        payload = {"rows": ["repeated-market-fact"] * 500}

        body, compressed = server.encode_json_payload(payload, "br, gzip")
        plain_body, plain_compressed = server.encode_json_payload(payload, "")

        self.assertTrue(compressed)
        self.assertFalse(plain_compressed)
        self.assertLess(len(body), len(plain_body))
        self.assertEqual(json.loads(gzip.decompress(body)), payload)

    def test_sqlite_runtime_matches_csv_facts(self):
        data_dir = self.cex_path.parent
        database_path = data_dir / server.DATABASE_FILENAME
        build_database(data_dir, database_path)

        with patch.dict(
            server.os.environ,
            {"MARKET_DATABASE": str(database_path)},
            clear=True,
        ):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")

        self.assertEqual(payload["metadata"]["storage"]["engine"], "sqlite")
        self.assertEqual(payload["metadata"]["token_count"], 1)
        self.assertEqual(len(payload["cex_markets"]), 2)
        self.assertEqual(len(payload["dex_pools"]), 1)
        self.assertAlmostEqual(payload["tokens"][0]["price_spread"], 105 / 102 - 1)
        self.assertEqual(payload["dex_pools"][0]["tvl_usd"], 5000)


if __name__ == "__main__":
    unittest.main()
