import csv
import gzip
import json
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from dashboard import server
from scripts.fetch_cex_depth import DEPTH_COLUMNS_ALL
from scripts.fetch_dex_depth import DEX_DEPTH_COLUMNS
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
                    "pool_name": "WBTC / USDC 0.30%",
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
                    "pool_name": "WBTC / USDC 0.25%",
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
        self.dex_depth_path = data_dir / server.DEX_DEPTH_FILENAME

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
        self.assertEqual(payload["dex_pools"][0]["instrument"], "WBTC / USDC 0.25%")
        self.assertNotIn("factor_results", payload)
        self.assertAlmostEqual(payload["tokens"][0]["price_spread"], 105 / 102 - 1)
        self.assertEqual(payload["dex_pools"][0]["tvl_usd"], 5000)
        token = payload["tokens"][0]
        self.assertEqual(token["aggregate_cex_volume_usd"], 2400)
        self.assertEqual(token["aggregate_dex_volume_usd"], 700)
        self.assertEqual(token["aggregate_volume_usd"], 3100)
        self.assertEqual(token["selected_cex_volume_usd"], 2200)
        self.assertEqual(token["selected_dex_volume_usd"], 700)
        self.assertIsInstance(token["primary_cex_selection_reason"], dict)
        self.assertIn("market_quality_thresholds", payload["metadata"])
        self.assertEqual(payload["metadata"]["dex_market_series_rows"], 1)
        self.assertEqual(payload["metadata"]["dex_unique_pool_count"], 1)

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
                "bid_depth_10bps_usd": "400",
                "ask_depth_10bps_usd": "600",
                "total_depth_10bps_usd": "1000",
                "bid_depth_25bps_usd": "800",
                "ask_depth_25bps_usd": "1200",
                "total_depth_25bps_usd": "2000",
                "bid_depth_50bps_usd": "1200",
                "ask_depth_50bps_usd": "1800",
                "total_depth_50bps_usd": "3000",
                "bid_depth_100bps_usd": "1600",
                "ask_depth_100bps_usd": "2400",
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
        self.assertEqual(binance["bid_depth_100bps_usd"], 1600)
        self.assertEqual(binance["ask_depth_100bps_usd"], 2400)
        self.assertEqual(binance["total_depth_100bps_usd"], 4000)
        self.assertFalse(binance["depth_100bps_complete"])
        self.assertEqual(payload["metadata"]["cex_depth_snapshot"]["matched_market_rows"], 1)
        catalog_binance = next(
            market
            for market in catalog["markets"]
            if market["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertEqual(catalog_binance["total_depth_100bps_usd"], 4000)
        self.assertEqual(catalog_binance["bid_depth_100bps_usd"], 1600)
        self.assertEqual(catalog_binance["ask_depth_100bps_usd"], 2400)
        self.assertEqual(catalog_binance["depth_status"], "partial")
        for band, bid, ask, total, complete in (
            (10, 400, 600, 1000, True),
            (25, 800, 1200, 2000, True),
            (50, 1200, 1800, 3000, True),
            (100, 1600, 2400, 4000, False),
        ):
            self.assertEqual(catalog_binance[f"bid_depth_{band}bps_usd"], bid)
            self.assertEqual(catalog_binance[f"ask_depth_{band}bps_usd"], ask)
            self.assertEqual(catalog_binance[f"total_depth_{band}bps_usd"], total)
            self.assertEqual(catalog_binance[f"depth_{band}bps_complete"], complete)

    def test_fixed_block_dex_depth_overlays_pool_without_using_tvl_proxy(self):
        depth_row = {field: "" for field in DEX_DEPTH_COLUMNS}
        depth_row.update(
            {
                "snapshot_id": "dex-depth-1",
                "observed_at": "2026-07-28T01:02:03+00:00",
                "response_received_at": "2026-07-28T01:02:04+00:00",
                "token_symbol": "BTC",
                "chain": "eth",
                "dex": "uniswap",
                "pool_address": "0xpool",
                "protocol_model": "concentrated_liquidity_v3",
                "block_number": "123456",
                "fee_bps": "30",
                "pool_state_price_usd": "104.8",
                "source_target_price_usd": "105",
                "price_difference_bps": "19.066",
                "sell_depth_10bps_usd": "400",
                "buy_depth_10bps_usd": "600",
                "total_depth_10bps_usd": "1000",
                "sell_depth_25bps_usd": "800",
                "buy_depth_25bps_usd": "1200",
                "total_depth_25bps_usd": "2000",
                "sell_depth_50bps_usd": "1200",
                "buy_depth_50bps_usd": "1800",
                "total_depth_50bps_usd": "3000",
                "sell_depth_100bps_usd": "1600",
                "buy_depth_100bps_usd": "2400",
                "total_depth_100bps_usd": "4000",
                "depth_10bps_complete": "1",
                "depth_25bps_complete": "1",
                "depth_50bps_complete": "1",
                "depth_100bps_complete": "1",
                "depth_method": "fixed_block_pool_state_marginal_price_band",
                "source_endpoint": "https://rpc.example.test",
                "raw_response_sha256": "abc123",
                "status": "observed",
            }
        )
        write_csv(self.dex_depth_path, DEX_DEPTH_COLUMNS, [depth_row])
        environment = {
            **self.environment,
            "MARKET_DEX_DEPTH_DATA": str(self.dex_depth_path),
        }

        with patch.dict(server.os.environ, environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")
            catalog = server.build_market_catalog()

        pool = payload["dex_pools"][0]
        self.assertEqual(pool["dex_depth_status"], "observed")
        self.assertEqual(pool["total_depth_100bps_usd"], 4000)
        self.assertEqual(pool["dex_depth_block_number"], 123456)
        self.assertTrue(pool["depth_100bps_complete"])
        self.assertEqual(
            payload["metadata"]["dex_depth_snapshot"]["matched_market_rows"],
            1,
        )
        catalog_pool = next(
            market for market in catalog["markets"] if market["market_type"] == "dex"
        )
        self.assertEqual(catalog_pool["depth_status"], "observed")
        self.assertEqual(catalog_pool["total_depth_100bps_usd"], 4000)
        for band, sell, buy, total in (
            (10, 400, 600, 1000),
            (25, 800, 1200, 2000),
            (50, 1200, 1800, 3000),
            (100, 1600, 2400, 4000),
        ):
            self.assertEqual(catalog_pool[f"sell_depth_{band}bps_usd"], sell)
            self.assertEqual(catalog_pool[f"buy_depth_{band}bps_usd"], buy)
            self.assertEqual(catalog_pool[f"total_depth_{band}bps_usd"], total)
            self.assertTrue(catalog_pool[f"depth_{band}bps_complete"])

    def test_catalog_identifies_markets_and_declares_fact_contract(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            catalog = server.build_market_catalog()

        self.assertEqual(catalog["metadata"]["catalog_version"], 2)
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
                "dex:eth:uniswap:0xpool:BTC",
            },
        )

    def test_comparison_returns_raw_daily_facts_absolute_spread_and_bps(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            result = server.build_market_comparison(
                "BTC",
                "cex:binance:BTC/USDT",
                "dex:eth:uniswap:0xpool:BTC",
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
                    "dex:eth:uniswap:0xpool:BTC",
                )

    def test_same_dex_pool_has_unique_token_series_ids(self):
        with self.dex_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "2026-01-02",
                    "ETH",
                    "eth",
                    "uniswap",
                    "0xpool",
                    "WBTC / USDC 0.25%",
                    "",
                    "",
                    "",
                    "2000",
                    "50",
                    "5000",
                ]
            )

        with patch.dict(server.os.environ, self.environment, clear=True):
            catalog = server.build_market_catalog()

        pools = [
            market
            for market in catalog["markets"]
            if market["market_type"] == "dex"
        ]
        self.assertEqual(
            {market["market_id"] for market in pools},
            {
                "dex:eth:uniswap:0xpool:BTC",
                "dex:eth:uniswap:0xpool:ETH",
            },
        )
        self.assertEqual(
            {market["pool_id"] for market in pools},
            {"dex:eth:uniswap:0xpool"},
        )
        self.assertEqual(
            len({market["market_id"] for market in catalog["markets"]}),
            len(catalog["markets"]),
        )

    def test_date_window_limits_volume_and_coverage(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-02", "2026-01-02")

        primary_cex = payload["cex_markets"][0]
        self.assertEqual(primary_cex["volume_usd"], 1200)
        self.assertEqual(primary_cex["observation_days"], 1)
        self.assertIsNone(primary_cex["window_return"])

    def test_api_statistics_exclude_cross_gap_returns_and_report_coverage(self):
        with self.cex_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "2026-01-04",
                    "BTC",
                    "binance",
                    "BTC/USDT",
                    "",
                    "",
                    "",
                    "108",
                    "",
                    "1400",
                ]
            )

        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload("2026-01-01", "2026-01-04")

        binance = next(
            row for row in payload["cex_markets"] if row["venue"] == "binance"
        )
        self.assertEqual(binance["observation_count"], 3)
        self.assertEqual(binance["requested_window_days"], 4)
        self.assertEqual(binance["coverage_ratio"], 0.75)
        self.assertEqual(binance["return_interval_count"], 1)
        self.assertEqual(binance["skipped_gap_interval_count"], 1)
        self.assertEqual(binance["max_gap_days"], 1)
        self.assertIsNone(binance["daily_volatility"])
        self.assertEqual(
            binance["daily_volatility_method"],
            "adjacent_utc_daily_log_returns_only_v1",
        )

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

    def test_public_api_response_cache_reuses_serialized_payload_and_invalidates(self):
        server._build_public_api_response_cached.cache_clear()
        signature = (("facts.sqlite3", 1, 100),)
        with patch.object(
            server,
            "build_market_catalog",
            return_value={"metadata": {}, "markets": []},
        ) as build_catalog:
            first = server._build_public_api_response_cached(
                "catalog",
                (),
                signature,
                100,
            )
            second = server._build_public_api_response_cached(
                "catalog",
                (),
                signature,
                100,
            )
            invalidated = server._build_public_api_response_cached(
                "catalog",
                (),
                (("facts.sqlite3", 2, 100),),
                100,
            )

        self.assertEqual(first, second)
        self.assertEqual(first, invalidated)
        self.assertEqual(build_catalog.call_count, 2)

    def test_public_api_cache_key_ignores_unsupported_query_fields(self):
        query = {
            "start": ["2026-01-01"],
            "end": ["2026-01-02"],
            "cache_buster": ["unbounded-user-value"],
        }

        self.assertEqual(
            server.public_api_query_items("market", query),
            (
                ("start", "2026-01-01"),
                ("end", "2026-01-02"),
            ),
        )

    def test_public_api_cold_miss_is_single_flight(self):
        server._build_public_api_response_cached.cache_clear()
        payload = {"metadata": {}, "markets": []}

        def slow_catalog():
            time.sleep(0.02)
            return payload

        with ThreadPoolExecutor(max_workers=8) as executor:
            with patch.object(
                server,
                "api_source_signature",
                return_value=(("facts.sqlite3", 1, 100),),
            ):
                with patch.object(server, "api_freshness_bucket", return_value=100):
                    with patch.object(
                        server,
                        "build_market_catalog",
                        side_effect=slow_catalog,
                    ) as build_catalog:
                        responses = list(
                            executor.map(
                                lambda _: server.build_public_api_response(
                                    "catalog",
                                    (),
                                    True,
                                ),
                                range(8),
                            )
                        )

        self.assertEqual(build_catalog.call_count, 1)
        self.assertTrue(all(response == responses[0] for response in responses))

    def test_vendored_lucide_resource_is_packaged(self):
        lucide_path = server.VENDOR_FILES["/vendor/lucide.js"]

        self.assertTrue(lucide_path.is_file())
        self.assertIn(
            "@license lucide v0.468.0",
            lucide_path.read_text(encoding="utf-8")[:200],
        )

    def test_expert_dashboard_static_contract_prevents_stale_and_ambiguous_results(self):
        index = (server.STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")

        self.assertIn('<html lang="en">', index)
        self.assertIn('id="comparison-status"', index)
        self.assertIn('role="alert"', index)
        self.assertIn('aria-busy="true"', index)
        self.assertIn("Midpoint-relative Spread (bps)", index)
        self.assertIn("Selected DEX/CEX Spread", index)
        self.assertIn('id="export-csv"', index)
        self.assertNotIn("综合", index)

        self.assertIn("new AbortController()", app_js)
        self.assertIn("comparisonController.abort()", app_js)
        self.assertIn("marketController.abort()", app_js)
        comparison_loader = app_js[
            app_js.index("async function loadComparison()"):
            app_js.index("async function loadCatalog()")
        ]
        market_loader = app_js[
            app_js.index("async function loadMarket("):
            app_js.index("function setPreset(")
        ]
        self.assertLess(
            comparison_loader.index("invalidateComparisonRequest()"),
            comparison_loader.index("validateDateRange()"),
        )
        self.assertLess(
            market_loader.index("invalidateMarketRequest()"),
            market_loader.index("validateDateRange("),
        )
        self.assertIn("clearComparisonResult(", comparison_loader)
        self.assertIn("clearMarketResult(", market_loader)
        self.assertIn("marketA === marketB", comparison_loader)
        self.assertIn('hideStatus(byId("market-status"))', app_js)
        self.assertIn("app.payload = null;", app_js)
        self.assertIn("No current market result.", app_js)
        self.assertIn("validateDateRange()", app_js)
        self.assertIn("selectionOverrides", app_js)
        self.assertIn("user-selected (not current primary)", app_js)
        self.assertIn("aggregate_cex_volume_usd", app_js)
        self.assertIn("aggregate_dex_volume_share", app_js)
        self.assertIn("Aggregate DEX", app_js)
        self.assertIn("formatShare(aggregates.aggregateDexShare)", app_js)
        self.assertIn("value !== 0 && Math.abs(value) < 1", app_js)
        self.assertIn("quality_flags", app_js)
        self.assertNotIn("Observed DEX ${formatPercent(observedShare)}", app_js)

    def test_market_table_has_accessible_mobile_card_and_depth_contracts(self):
        index = (server.STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        styles = (server.STATIC_ROOT / "styles.css").read_text(encoding="utf-8")

        self.assertGreaterEqual(index.count("<caption>"), 2)
        self.assertIn('scope="col"', index)
        self.assertIn('aria-label="Token and selected market facts"', index)
        self.assertIn('aria-label="${escapeHtml(`${token} selected ${label} market`)}"', app_js)
        for band in (10, 25, 50, 100):
            self.assertIn(str(band), app_js)
        self.assertIn('const sideA = market === "cex" ? "bid" : "buy";', app_js)
        self.assertIn('const sideB = market === "cex" ? "ask" : "sell";', app_js)
        self.assertIn("#market-table td::before", styles)
        self.assertIn('content: attr(data-label)', styles)
        self.assertIn("min-height: 44px", styles)

    def test_liquidity_profile_compares_only_discrete_source_backed_depth(self):
        index = (server.STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        styles = (server.STATIC_ROOT / "styles.css").read_text(encoding="utf-8")

        self.assertIn("Discrete cumulative depth profile", index)
        self.assertIn('id="liquidity-chart"', index)
        self.assertIn('id="liquidity-table-body"', index)
        self.assertIn('aria-label="Exact liquidity depth values"', index)
        self.assertIn("no values are interpolated between them", index)
        self.assertIn('data-liquidity-view="directional"', index)
        self.assertIn('data-liquidity-scale="log"', index)
        self.assertIn('aria-label="Interactive liquidity depth chart"', index)
        self.assertIn("quality-weighted primary market", index)
        self.assertIn('role="group"', index)
        self.assertNotIn('aria-hidden="true"', index[index.index('id="liquidity-chart"'):index.index('id="liquidity-empty"')])

        self.assertIn("const DEPTH_BANDS = [10, 25, 50, 100];", app_js)
        self.assertIn('sellField: "bid"', app_js)
        self.assertIn('buyField: "ask"', app_js)
        self.assertIn('sellField: "sell"', app_js)
        self.assertIn('buyField: "buy"', app_js)
        self.assertIn("function liquidityDepthIssues(market)", app_js)
        self.assertIn("cleanDepthReadyCandidates.length", app_js)
        self.assertIn("liquidityRelevantFlags(market).length === 0", app_js)
        self.assertIn("MEASURED_DEPTH_STATUSES", app_js)
        self.assertIn("new ResizeObserver(scheduleLiquidityResize)", app_js)
        self.assertIn('window.visualViewport?.addEventListener("resize"', app_js)
        self.assertIn("directional depth does not sum to total", app_js)
        self.assertIn("status contains measured depth fields", app_js)
        self.assertIn("depth is missing a total or directional value", app_js)
        self.assertIn("cumulative depth falls between measured bands", app_js)
        self.assertIn("completeness returns to true", app_js)
        self.assertIn("function liquidityRenderableMarket", app_js)
        self.assertIn("isMeasuredDepthStatus(market)", app_js)
        self.assertIn('if (value === 0) return bottom;', app_js)
        self.assertIn('<th scope="row" data-label="Band">', app_js)
        self.assertNotIn("liquidity-series-line", app_js)

        self.assertIn(".liquidity-zero-rail", styles)
        self.assertIn(".liquidity-legend-marker.series-b-total", styles)
        self.assertIn(".liquidity-lower-bound-ring", styles)
        self.assertIn(".liquidity-point:focus .liquidity-focus-ring", styles)
        self.assertIn(".liquidity-table td::before", styles)
        self.assertIn("#liquidity-chart", styles)

    def test_liquidity_profile_behavior_preserves_fact_boundaries(self):
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        behavior_checks = r"""
const assert = require("node:assert/strict");

function depthFixture(status = "observed") {
  const market = {
    market_type: "cex",
    market_id: "cex:test:UNI/USDT",
    venue: "test",
    instrument: "UNI/USDT",
    depth_status: status,
  };
  for (const band of DEPTH_BANDS) {
    market[`bid_depth_${band}bps_usd`] = band;
    market[`ask_depth_${band}bps_usd`] = band * 2;
    market[`total_depth_${band}bps_usd`] = band * 3;
    market[`depth_${band}bps_complete`] = true;
  }
  return market;
}

assert.equal(formatSummaryDepth(0, true), "$0");
assert.notEqual(formatSummaryDepth(0.0005167977547470759, true), "$0");
assert.match(formatSummaryDepth(0.0005167977547470759, true), /^\$0\.0005/);

const unsupported = depthFixture("unsupported");
assert.match(liquidityDepthIssues(unsupported).join(";"), /status contains measured depth fields/);
assert.deepEqual(liquiditySeriesForMarket("A", unsupported), []);
assert.equal(liquidityRenderableMarket(unsupported), null);
assert.equal(liquiditySnapshotSkew(depthFixture(), unsupported), null);

const partial = depthFixture("partial");
partial.depth_50bps_complete = false;
partial.depth_100bps_complete = false;
assert.deepEqual(liquidityDepthIssues(partial), []);
assert.equal(liquiditySeriesForMarket("A", partial).length, 1);
assert.equal(formatExactDepth(partial.total_depth_100bps_usd, false).startsWith("≥"), true);

const missingSide = depthFixture("observed");
missingSide.ask_depth_25bps_usd = null;
assert.match(
  liquidityDepthIssues(missingSide).join(";"),
  /missing a total or directional value/,
);
assert.deepEqual(liquiditySeriesForMarket("A", missingSide), []);
assert.equal(liquidityRenderableMarket(missingSide), null);

app.liquidityScale = "log";
const zeroAxis = liquidityAxis([0, 0, 0, 0], { top: 10, bottom: 100 });
assert.deepEqual(zeroAxis.ticks, [0]);
assert.equal(zeroAxis.y(0), 100);
assert.match(zeroAxis.scaleLabel, /measured zero/);

const goodCex = depthFixture();
goodCex.token_symbol = "GOOD";
const goodDex = {
  ...depthFixture(),
  token_symbol: "GOOD",
  market_type: "dex",
  market_id: "dex:test:pool:GOOD",
};
for (const band of DEPTH_BANDS) {
  goodDex[`sell_depth_${band}bps_usd`] = band;
  goodDex[`buy_depth_${band}bps_usd`] = band * 2;
  delete goodDex[`bid_depth_${band}bps_usd`];
  delete goodDex[`ask_depth_${band}bps_usd`];
}
const flaggedDex = {
  ...goodDex,
  market_id: "dex:test:flagged:GOOD",
  price_difference_bps: 200,
  is_primary: true,
};
assert.match(
  liquidityRelevantFlags(flaggedDex).map((flag) => flag.code).join(","),
  /off_market_pool_state_price/,
);
assert.equal(
  preferredCatalogMarket([flaggedDex, goodDex], "dex", null).market_id,
  goodDex.market_id,
);
const badOnlyCex = depthFixture();
badOnlyCex.token_symbol = "BAD";
assert.equal(
  preferredLiquidityToken({
    tokens: ["BAD", "GOOD"],
    markets: [badOnlyCex, goodCex, goodDex],
  }),
  "GOOD",
);
"""
        completed = subprocess.run(
            ["node", "-"],
            input=f"{app_js}\n{behavior_checks}",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            msg=f"Node behavior checks failed:\n{completed.stderr}",
        )

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
