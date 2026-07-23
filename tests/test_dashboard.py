import csv
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import server


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

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_parse_number_preserves_missing_values(self):
        self.assertIsNone(server.parse_number(""))
        self.assertIsNone(server.parse_number("nan"))
        self.assertEqual(server.parse_number("12.5"), 12.5)

    def test_payload_contains_only_market_facts_and_token_level_spread(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")

        self.assertEqual(payload["metadata"]["token_count"], 1)
        self.assertEqual(len(payload["cex_markets"]), 2)
        self.assertEqual(len(payload["dex_pools"]), 1)
        self.assertNotIn("factor_results", payload)
        self.assertAlmostEqual(payload["tokens"][0]["price_spread"], 105 / 102 - 1)
        self.assertEqual(payload["dex_pools"][0]["tvl_usd"], 5000)

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


if __name__ == "__main__":
    unittest.main()
