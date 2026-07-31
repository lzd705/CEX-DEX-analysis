import csv
import gzip
import json
import shutil
import subprocess
import tempfile
import time
import unittest
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from dashboard import market_facts, server
from scripts.fetch_cex_depth import DEPTH_COLUMNS_ALL
from scripts.fetch_dex_depth import DEX_DEPTH_COLUMNS
from scripts.execution_cost import (
    EXECUTION_COST_COLUMNS,
    EXECUTION_NOTIONALS_USD,
    RESULT_NUMERIC_COLUMNS,
    execution_fact_row,
)
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
        self.cex_execution_path = (
            data_dir / server.CEX_EXECUTION_COST_FILENAME
        )
        self.dex_execution_path = (
            data_dir / server.DEX_EXECUTION_COST_FILENAME
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    @staticmethod
    def execution_rows(
        market_id,
        market_type,
        *,
        state_observed_at,
        status="observed",
        status_reason=None,
        exchange="binance",
        cex_symbol="BTC/USDT",
        source_instrument="BTCUSDT",
        zero_cost=False,
        usd_price_observed_at=None,
    ):
        if market_type == "cex":
            identity = {
                "exchange": exchange,
                "cex_symbol": cex_symbol,
                "source_instrument": source_instrument,
                "base_asset": "BTC",
                "source_quote_asset": "USDT",
                "reference_price_method": "order_book_midpoint",
                "fee_status": "excluded_unknown_account_tier",
                "usd_conversion_status": "USDT=USD proxy",
                "excluded_costs": "taker_fee,lot_size,latency",
            }
        else:
            identity = {
                "chain": "eth",
                "dex": "uniswap",
                "pool_address": "0xpool",
                "block_number": "123",
                "block_timestamp": state_observed_at,
                "source_sequence": "123",
                "protocol_model": "constant_product_v2",
                "target_token_address": "0xtarget",
                "target_token_decimals": "8",
                "quote_token_address": "0xquote",
                "quote_token_decimals": "6",
                "reference_price_method": "fixed_block_pool_state_marginal_price",
                "fee_status": "included_protocol_fee",
                "fee_rate_bps": "30",
                "usd_price_source_snapshot_id": "tvl-1",
                "usd_price_observed_at": (
                    usd_price_observed_at or state_observed_at
                ),
                "usd_conversion_status": "tvl_inventory_token_price",
                "excluded_costs": "gas,router_fee,transfer_tax,MEV",
            }
        common = {
            "snapshot_id": f"{market_type}-execution-1",
            "source_snapshot_id": f"{market_type}-depth-1",
            "calculation_method": f"{market_type}_fixture_walk",
            "observed_at": "2026-01-02T00:02:00+00:00",
            "state_observed_at": state_observed_at,
            "request_started_at": "2026-01-02T00:00:00+00:00",
            "response_received_at": "2026-01-02T00:02:00+00:00",
            "market_id": market_id,
            "market_type": market_type,
            "token_symbol": "BTC",
            "source": f"{market_type} fixture source",
            "source_endpoint": "https://example.test/source",
            "raw_response_sha256": "a" * 64,
            **identity,
        }
        if status in {"unsupported", "failed"}:
            # Terminal execution rows retain identity and lineage, not measured
            # numeric result fields such as an applied fee rate.
            common.pop("fee_rate_bps", None)
        rows = []
        for index, notional in enumerate(
            EXECUTION_NOTIONALS_USD,
            start=1,
        ):
            target = notional / Decimal(100)
            rate = Decimal(index) / Decimal(10_000)
            for direction in ("sell_token", "buy_token"):
                if status in {"unsupported", "failed"}:
                    rows.append(
                        execution_fact_row(
                            common=common,
                            direction=direction,
                            requested_notional_usd=notional,
                            status=status,
                            status_reason=(
                                status_reason
                                or (
                                    "unsupported_protocol_or_chain"
                                    if status == "unsupported"
                                    else "execution_calculation_failed"
                                )
                            ),
                            error=(
                                "fixture execution failure"
                                if status == "failed"
                                else "fixture unsupported market"
                            ),
                        )
                    )
                    continue
                filled_target = (
                    target / Decimal(2)
                    if status == "partial"
                    else target
                )
                reference_for_fill = filled_target * Decimal(100)
                quote = reference_for_fill * (
                    Decimal(1) - (Decimal(0) if zero_cost else rate)
                    if direction == "sell_token"
                    else Decimal(1) + (Decimal(0) if zero_cost else rate)
                )
                rows.append(
                    execution_fact_row(
                        common=common,
                        direction=direction,
                        requested_notional_usd=notional,
                        status=status,
                        status_reason=(
                            status_reason
                            or (
                                "source_level_limit"
                                if status == "partial"
                                else "target_filled"
                            )
                        ),
                        reference_price_quote_per_token=100,
                        quote_to_usd=1,
                        target_token_quantity=target,
                        filled_token_quantity=filled_target,
                        quote_amount=quote,
                        levels_or_ticks_consumed=index,
                        ending_marginal_price_quote_per_token=(
                            Decimal(100) * (
                                Decimal(1) - rate
                                if direction == "sell_token"
                                else Decimal(1) + rate
                            )
                        ),
                    )
                )
        return rows

    def test_parse_number_preserves_missing_values(self):
        self.assertIsNone(server.parse_number(""))
        self.assertIsNone(server.parse_number("nan"))
        self.assertEqual(server.parse_number("12.5"), 12.5)
        self.assertEqual(server.parse_number(12.5), 12.5)

    def test_screener_refresh_identity_is_canonical_and_retryability_is_server_owned(self):
        cex = server._compact_screener_market(
            {
                "token_symbol": "AAVE",
                "venue": "binance",
                "instrument": "AAVE/USDT",
                "depth_status": "collection_failed",
                "depth_reason_code": "network",
            },
            "cex",
        )
        dex = server._compact_screener_market(
            {
                "token_symbol": "AAVE",
                "venue": "eth / uniswap_v3",
                "pool_address": "0xAbC",
                "tvl_status": "not_cataloged_in_snapshot",
                "dex_depth_status": "unsupported",
                "depth_error": "unsupported_protocol",
            },
            "dex",
        )

        self.assertEqual(cex["market_id"], "binance|AAVE/USDT")
        self.assertEqual(
            cex["refresh_market_id"],
            "cex:binance:AAVE/USDT",
        )
        self.assertTrue(cex["depth_retryable"])
        self.assertEqual(dex["market_id"], "0xAbC")
        self.assertEqual(
            dex["refresh_market_id"],
            "dex:eth:uniswap_v3:0xabc:AAVE",
        )
        self.assertTrue(dex["tvl_retryable"])
        self.assertFalse(dex["depth_retryable"])

    def test_spread_summary_supports_latest_max_mean_and_median_ranking(self):
        summary = market_facts._common_price_comparison(
            {
                "price_points": [
                    {"date": "2026-07-01", "price_usd": 100},
                    {"date": "2026-07-02", "price_usd": 100},
                    {"date": "2026-07-03", "price_usd": 100},
                ]
            },
            {
                "price_points": [
                    {"date": "2026-07-01", "price_usd": 99},
                    {"date": "2026-07-02", "price_usd": 102},
                    {"date": "2026-07-03", "price_usd": 104},
                ]
            },
        )

        self.assertEqual(summary["date"], "2026-07-03")
        self.assertAlmostEqual(summary["latest"], 0.04)
        self.assertAlmostEqual(summary["maximum_absolute"], 0.04)
        self.assertAlmostEqual(summary["mean_absolute"], (0.01 + 0.02 + 0.04) / 3)
        self.assertAlmostEqual(summary["median_absolute"], 0.02)
        self.assertEqual(summary["comparable_days"], 3)

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
        self.assertEqual(catalog_pool["tvl_snapshot_id"], "tvl-snapshot-1")
        self.assertEqual(
            catalog_pool["tvl_source_endpoint"],
            "https://example.test/pool",
        )
        self.assertEqual(
            catalog_pool["tvl_raw_response_sha256"],
            "abc123",
        )

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
                "bid_depth_10bps_usd": "0",
                "ask_depth_10bps_usd": "0",
                "total_depth_10bps_usd": "0",
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
            quality = server.build_market_quality("BTC")

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
        self.assertEqual(
            catalog_binance["depth_snapshot_id"],
            "depth-snapshot-1",
        )
        self.assertEqual(
            catalog_binance["depth_source_endpoint"],
            "https://example.test/depth",
        )
        for band, bid, ask, total, complete in (
            (10, 0, 0, 0, True),
            (25, 800, 1200, 2000, True),
            (50, 1200, 1800, 3000, True),
            (100, 1600, 2400, 4000, False),
        ):
            self.assertEqual(catalog_binance[f"bid_depth_{band}bps_usd"], bid)
            self.assertEqual(catalog_binance[f"ask_depth_{band}bps_usd"], ask)
            self.assertEqual(catalog_binance[f"total_depth_{band}bps_usd"], total)
            self.assertEqual(catalog_binance[f"depth_{band}bps_complete"], complete)
        quality_binance = next(
            market
            for market in quality["markets"]
            if market["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertEqual(
            quality_binance["facts"]["depth"]["bands_bps"]["10"][
                "total_usd"
            ],
            0,
        )
        self.assertIn(
            "zero_depth_10bps",
            {
                flag["code"]
                for flag in quality_binance["facts"]["depth"][
                    "quality_flags"
                ]
            },
        )

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
                "block_timestamp": "2026-07-28T01:02:03+00:00",
                "usd_price_source_snapshot_id": "tvl-1",
                "usd_price_observed_at": "2026-07-28T01:01:03+00:00",
                "usd_price_skew_seconds": "60",
                "usd_price_freshness_status": "current",
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

    def test_screener_summary_is_compact_and_matches_full_fact_aggregates(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            full_payload = server.build_market_payload()
            summary = server.build_market_summary()
            token_catalog = server.build_token_market_catalog(
                "BTC",
                summary["metadata"]["start_date"],
                summary["metadata"]["end_date"],
            )

        self.assertEqual(set(summary), {"metadata", "tokens"})
        self.assertEqual(summary["metadata"]["response_scope"], "screener_summary")
        self.assertEqual(summary["metadata"]["summary_version"], 2)
        self.assertTrue(summary["metadata"]["data_generation"])
        self.assertNotIn("markets", summary)
        self.assertNotIn("cex_markets", summary)
        self.assertNotIn("dex_pools", summary)
        self.assertEqual(len(summary["tokens"]), 1)
        self.assertEqual(
            summary["metadata"]["data_generation"],
            token_catalog["metadata"]["data_generation"],
        )

        compact = summary["tokens"][0]
        original = full_payload["tokens"][0]
        for field in (
            "aggregate_cex_volume_usd",
            "aggregate_dex_volume_usd",
            "aggregate_volume_usd",
            "aggregate_dex_volume_share",
            "price_spread",
            "spread_date",
        ):
            self.assertEqual(compact[field], original[field], field)
        self.assertEqual(compact["market_count"], 3)
        self.assertEqual(compact["cex_market_count"], 2)
        self.assertEqual(compact["dex_market_count"], 1)
        self.assertEqual(
            sum(compact["quality_status_counts"].values()),
            compact["market_count"],
        )
        self.assertGreater(compact["quality_alert_counts"].get("info", 0), 0)
        self.assertEqual(compact["primary_cex"]["market_type"], "cex")
        self.assertEqual(compact["primary_dex"]["market_type"], "dex")
        self.assertNotIn("price_points", compact["primary_cex"])
        self.assertNotIn("price_points", compact["primary_dex"])
        for primary in (compact["primary_cex"], compact["primary_dex"]):
            self.assertIn("refresh_market_id", primary)
            self.assertIsInstance(primary["depth_retryable"], bool)
            for field in (
                "first_observed_date",
                "latest_observed_date",
                "coverage_ratio",
                "total_depth_10bps_usd",
                "total_depth_25bps_usd",
                "total_depth_50bps_usd",
                "total_depth_100bps_usd",
                "depth_status",
                "quality_flags",
            ):
                self.assertIn(field, primary)

    def test_single_token_catalog_filters_and_preserves_window_metrics(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            token_catalog = server.build_token_market_catalog(" btc ")

        self.assertEqual(token_catalog["token_symbol"], "BTC")
        self.assertEqual(token_catalog["metadata"]["catalog_scope"], "single_token")
        self.assertEqual(token_catalog["metadata"]["market_count"], 3)
        self.assertTrue(token_catalog["metadata"]["data_generation"])
        self.assertEqual(token_catalog["token_summary"]["token_symbol"], "BTC")
        self.assertTrue(token_catalog["markets"])
        self.assertTrue(
            all(market["token_symbol"] == "BTC" for market in token_catalog["markets"])
        )
        self.assertTrue(
            all("price_points" not in market for market in token_catalog["markets"])
        )
        binance = next(
            market
            for market in token_catalog["markets"]
            if market["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertEqual(binance["window_metrics"]["price_usd"], 102)
        self.assertEqual(binance["window_metrics"]["volume_usd"], 2200)

        with patch.dict(server.os.environ, self.environment, clear=True):
            first_day_catalog = server.build_token_market_catalog(
                "BTC",
                "2026-01-01",
                "2026-01-01",
            )
        first_day_binance = next(
            market
            for market in first_day_catalog["markets"]
            if market["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertEqual(first_day_catalog["metadata"]["window_start"], "2026-01-01")
        self.assertEqual(first_day_catalog["metadata"]["window_end"], "2026-01-01")
        self.assertEqual(first_day_binance["window_metrics"]["price_usd"], 100)
        self.assertEqual(first_day_binance["window_metrics"]["volume_usd"], 1000)

        with patch.dict(server.os.environ, self.environment, clear=True):
            with self.assertRaisesRegex(ValueError, "required"):
                server.build_token_market_catalog("")
            with self.assertRaisesRegex(ValueError, "not cataloged"):
                server.build_token_market_catalog("ETH")

    def test_screener_default_token_always_exists_in_selected_window(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload()
            catalog = server.build_market_catalog()
        aave_markets = []
        for market in catalog["markets"]:
            aave_market = {
                **market,
                "token_symbol": "AAVE",
                "market_id": market["market_id"].replace("BTC", "AAVE"),
            }
            aave_markets.append(aave_market)
        catalog = {
            **catalog,
            "tokens": ["AAVE", *catalog["tokens"]],
            "markets": [*aave_markets, *catalog["markets"]],
        }

        summary = server.market_summary_from_payload(payload, catalog)

        response_tokens = {
            token_summary["token_symbol"] for token_summary in summary["tokens"]
        }
        self.assertEqual(response_tokens, {"BTC"})
        self.assertEqual(summary["metadata"]["default_workspace_token"], "BTC")
        self.assertIn(
            summary["metadata"]["default_workspace_token"],
            response_tokens,
        )

    def test_public_generation_covers_sources_and_contract_but_not_query_window(self):
        metadata = {
            "available_end": "2026-01-02",
            "catalog_version": 2,
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "sources": [],
        }
        first_signature = (
            ("market_facts.sqlite3", 100, 1000),
            ("cex_execution_cost_latest.csv", 200, 2000),
        )
        changed_execution_signature = (
            ("market_facts.sqlite3", 100, 1000),
            ("cex_execution_cost_latest.csv", 201, 2000),
        )

        first = server._public_data_generation(metadata, first_signature)
        changed_execution = server._public_data_generation(
            metadata,
            changed_execution_signature,
        )
        changed_window = server._public_data_generation(
            {**metadata, "start_date": "2025-12-01"},
            first_signature,
        )
        with patch.object(server, "CATALOG_SUMMARY_VERSION", 3):
            changed_contract = server._public_data_generation(
                metadata,
                first_signature,
            )

        self.assertNotEqual(first, changed_execution)
        self.assertNotEqual(first, changed_contract)
        self.assertEqual(first, changed_window)
        same_stat_different_path = server._public_data_generation(
            metadata,
            (
                ("/another-release/market_facts.sqlite3", 100, 1000),
                ("cex_execution_cost_latest.csv", 200, 2000),
            ),
        )
        self.assertNotEqual(first, same_stat_different_path)

    def test_summary_rejects_missing_catalog_join_and_quality_status(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_payload()
            catalog = server.build_market_catalog()
        missing_join_payload = {
            **payload,
            "tokens": [
                *payload["tokens"],
                {
                    "token_symbol": "ETH",
                    "primary_cex_id": None,
                    "primary_dex_id": None,
                },
            ],
        }
        with self.assertRaisesRegex(ValueError, "missing from the market catalog"):
            server.market_summary_from_payload(missing_join_payload, catalog)

        for field, error in (
            ("primary_cex_id", "Primary CEX market"),
            ("primary_dex_id", "Primary DEX market"),
        ):
            broken_primary = {
                **payload,
                "tokens": [
                    {
                        **payload["tokens"][0],
                        field: "missing-market-id",
                    }
                ],
            }
            with self.assertRaisesRegex(ValueError, error):
                server.market_summary_from_payload(broken_primary, catalog)

        invalid_market = {**catalog["markets"][0]}
        invalid_market.pop("quality_status")
        invalid_catalog = {
            **catalog,
            "markets": [invalid_market, *catalog["markets"][1:]],
        }
        with self.assertRaisesRegex(ValueError, "quality status"):
            server.catalog_summary_from_catalog(invalid_catalog)

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
        self.assertAlmostEqual(
            result["market_a_statistics"]["window_return"],
            102 / 100 - 1,
        )
        self.assertAlmostEqual(
            result["market_b_statistics"]["window_return"],
            105 / 101 - 1,
        )
        self.assertEqual(
            result["market_a_statistics"]["daily_volatility_method"],
            "adjacent_utc_daily_log_returns_only_v1",
        )
        self.assertEqual(
            result["market_a_statistics"]["coverage_ratio"],
            1,
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

    def test_execution_cost_api_returns_long_form_source_backed_facts(self):
        cex_id = "cex:binance:BTC/USDT"
        dex_id = "dex:eth:uniswap:0xpool:BTC"
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                cex_id,
                "cex",
                state_observed_at="2026-01-02T00:00:00+00:00",
            ),
        )
        write_csv(
            self.dex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                dex_id,
                "dex",
                state_observed_at="2026-01-02T00:01:00+00:00",
            ),
        )
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
            "MARKET_DEX_EXECUTION_COST_DATA": str(self.dex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                payload = server.build_execution_cost_comparison(
                    "BTC",
                    cex_id,
                    dex_id,
                )
                encoded, compressed = server.build_public_api_response(
                    "execution_cost",
                    (
                        ("market_a", cex_id),
                        ("market_b", dex_id),
                        ("token", "BTC"),
                    ),
                    False,
                )
        finally:
            server.clear_runtime_caches()

        self.assertFalse(compressed)
        self.assertEqual(json.loads(encoded)["token_symbol"], "BTC")
        self.assertEqual(payload["market_a"]["status"], "available")
        self.assertEqual(payload["market_b"]["status"], "available")
        self.assertEqual(len(payload["market_a"]["rows"]), 10)
        self.assertEqual(len(payload["market_b"]["rows"]), 10)
        self.assertEqual(payload["metadata"]["snapshot_skew_seconds"], 60)
        self.assertEqual(payload["market_a"]["timing"]["status"], "not_applicable")
        self.assertEqual(payload["market_b"]["timing"]["status"], "current")
        self.assertEqual(
            payload["market_b"]["timing"][
                "usd_price_state_skew_seconds"
            ],
            0,
        )
        self.assertEqual(
            payload["market_a"]["rows"][0]["fee_status"],
            "excluded_unknown_account_tier",
        )
        self.assertEqual(
            payload["market_b"]["rows"][0]["fee_status"],
            "included_protocol_fee",
        )
        self.assertEqual(
            payload["market_a"]["rows"][0]["quoted_execution_cost_bps"],
            "1",
        )

    def test_stale_dex_price_time_withholds_public_execution_and_flags_quality(self):
        cex_id = "cex:binance:BTC/USDT"
        dex_id = "dex:eth:uniswap:0xpool:BTC"
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                cex_id,
                "cex",
                state_observed_at="2026-01-02T00:00:00+00:00",
            ),
        )
        write_csv(
            self.dex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                dex_id,
                "dex",
                state_observed_at="2026-01-02T00:00:00+00:00",
                usd_price_observed_at="2026-01-01T05:59:59+00:00",
            ),
        )
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
            "MARKET_DEX_EXECUTION_COST_DATA": str(self.dex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                execution = server.build_execution_cost_comparison(
                    "BTC",
                    cex_id,
                    dex_id,
                )
                quality = server.build_market_quality("BTC")
        finally:
            server.clear_runtime_caches()

        dex_result = execution["market_b"]
        self.assertEqual(dex_result["publication_status"], "withheld")
        self.assertEqual(dex_result["timing"]["status"], "stale")
        self.assertEqual(
            dex_result["timing"]["usd_price_state_skew_seconds"],
            64801,
        )
        first = dex_result["rows"][0]
        self.assertEqual(first["source_status"], "observed")
        self.assertEqual(first["status"], "failed")
        for field in RESULT_NUMERIC_COLUMNS:
            self.assertIsNone(first[field], field)
        dex_quality = next(
            market["facts"]["execution"]
            for market in quality["markets"]
            if market["market_id"] == dex_id
        )
        self.assertEqual(dex_quality["status"], "failed")
        self.assertIn(
            "execution_usd_price_time_mismatch",
            {
                flag["code"]
                for flag in dex_quality["quality_flags"]
            },
        )

    def test_execution_cost_api_requires_two_exact_token_market_ids(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            with self.assertRaisesRegex(ValueError, "must be different"):
                server.build_execution_cost_comparison(
                    "BTC",
                    "cex:binance:BTC/USDT",
                    "cex:binance:BTC/USDT",
                )
            with self.assertRaisesRegex(ValueError, "not cataloged"):
                server.build_execution_cost_comparison(
                    "UNI",
                    "cex:binance:BTC/USDT",
                    "dex:eth:uniswap:0xpool:BTC",
                )

    def test_execution_cost_api_does_not_load_an_unselected_broken_source(self):
        cex_id = "cex:binance:BTC/USDT"
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                cex_id,
                "cex",
                state_observed_at="2026-01-02T00:00:00+00:00",
            ),
        )
        self.dex_execution_path.write_text("broken\nvalue\n", encoding="utf-8")
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
            "MARKET_DEX_EXECUTION_COST_DATA": str(self.dex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                payload = server.build_execution_cost_comparison(
                    "BTC",
                    cex_id,
                    "cex:okx:BTC/USDT",
                )
        finally:
            server.clear_runtime_caches()

        self.assertEqual(payload["market_a"]["status"], "available")
        self.assertEqual(
            payload["market_b"]["status"],
            "not_cataloged_in_snapshot",
        )
        self.assertNotIn("dex", payload["metadata"]["snapshots"])

    def test_quality_api_all_preserves_fact_applicability_and_missing_states(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_quality("btc")

        self.assertEqual(payload["token_symbol"], "BTC")
        self.assertEqual(payload["metadata"]["scope"], "all")
        self.assertEqual(
            payload["metadata"]["facts"],
            ["daily", "tvl", "depth", "execution"],
        )
        self.assertEqual(len(payload["markets"]), 3)
        by_id = {
            market["market_id"]: market
            for market in payload["markets"]
        }
        binance = by_id["cex:binance:BTC/USDT"]["facts"]
        pool = by_id["dex:eth:uniswap:0xpool:BTC"]["facts"]
        self.assertEqual(binance["daily"]["status"], "observed")
        self.assertEqual(binance["tvl"]["status"], "not_applicable")
        self.assertIsNone(binance["tvl"]["value_usd"])
        self.assertEqual(binance["depth"]["status"], "unavailable")
        self.assertEqual(binance["execution"]["status"], "unavailable")
        self.assertEqual(pool["tvl"]["status"], "legacy_ohlcv_snapshot")
        self.assertEqual(pool["tvl"]["value_usd"], 5000)
        self.assertEqual(pool["depth"]["status"], "unavailable")
        self.assertIn(
            "Measured zero remains zero",
            payload["metadata"]["missing_value_rule"],
        )

    def test_quality_window_separates_prelisting_from_retryable_historical_gap(self):
        with self.cex_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "2026-01-01",
                    "BTC",
                    "kraken",
                    "BTC/USD",
                    "",
                    "",
                    "",
                    "100",
                    "",
                    "50",
                ]
            )
            writer.writerow(
                [
                    "2026-01-03",
                    "BTC",
                    "kraken",
                    "BTC/USD",
                    "",
                    "",
                    "",
                    "103",
                    "",
                    "60",
                ]
            )
            writer.writerow(
                [
                    "2026-01-02",
                    "BTC",
                    "coinbase",
                    "BTC/USD",
                    "",
                    "",
                    "",
                    "103",
                    "",
                    "70",
                ]
            )

        with patch.dict(server.os.environ, self.environment, clear=True):
            before_listing = server.build_market_quality(
                "BTC",
                start="2026-01-01",
                end="2026-01-01",
            )
            internal_gap = server.build_market_quality(
                "BTC",
                start="2026-01-02",
                end="2026-01-02",
            )
            after_last_observation = server.build_market_quality(
                "BTC",
                start="2026-01-03",
                end="2026-01-03",
            )

        coinbase = next(
            market
            for market in before_listing["markets"]
            if market["market_id"] == "cex:coinbase:BTC/USD"
        )
        kraken = next(
            market
            for market in internal_gap["markets"]
            if market["market_id"] == "cex:kraken:BTC/USD"
        )
        self.assertEqual(coinbase["facts"]["daily"]["status"], "not_applicable")
        self.assertFalse(coinbase["facts"]["daily"]["retryable"])
        self.assertEqual(
            coinbase["facts"]["daily"]["reason_code"],
            "selected_window_before_first_market_observation",
        )
        self.assertEqual(kraken["facts"]["daily"]["status"], "backfill_pending")
        self.assertTrue(kraken["facts"]["daily"]["retryable"])
        self.assertEqual(kraken["facts"]["daily"]["missing_calendar_days"], 1)
        trailing_gap = next(
            market
            for market in after_last_observation["markets"]
            if market["market_id"] == "cex:coinbase:BTC/USD"
        )
        self.assertEqual(
            trailing_gap["facts"]["daily"]["status"],
            "missing_unexplained",
        )
        self.assertTrue(trailing_gap["facts"]["daily"]["retryable"])
        self.assertEqual(
            trailing_gap["facts"]["daily"]["reason_code"],
            "no_daily_observations_after_latest_observed_market_date",
        )

    def test_quality_window_reports_a_global_internal_gap_instead_of_failing(self):
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
                    "date": day,
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "close": close,
                    "quote_volume_usd": "1000",
                }
                for day, close in (
                    ("2026-01-01", "100"),
                    ("2026-01-03", "103"),
                )
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
                    "date": day,
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                    "pool_name": "WBTC / USDC 0.30%",
                    "close": close,
                    "dex_volume_usd": "300",
                    "pool_tvl_usd": "5000",
                }
                for day, close in (
                    ("2026-01-01", "101"),
                    ("2026-01-03", "104"),
                )
            ],
        )

        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_quality(
                "BTC",
                start="2026-01-02",
                end="2026-01-02",
            )

        self.assertEqual(payload["metadata"]["window_start"], "2026-01-02")
        self.assertEqual(payload["metadata"]["window_end"], "2026-01-02")
        self.assertEqual(len(payload["markets"]), 2)
        for market in payload["markets"]:
            self.assertEqual(
                market["facts"]["daily"]["status"],
                "backfill_pending",
            )
            self.assertTrue(market["facts"]["daily"]["retryable"])

    def test_quality_api_selected_scope_validates_exact_token_market_ids(self):
        cex_id = "cex:binance:BTC/USDT"
        dex_id = "dex:eth:uniswap:0xpool:BTC"
        with patch.dict(server.os.environ, self.environment, clear=True):
            selected = server.build_market_quality(
                "BTC",
                "selected",
                cex_id,
                dex_id,
            )
            with self.assertRaisesRegex(ValueError, "scope must"):
                server.build_market_quality("BTC", "pair")
            with self.assertRaisesRegex(ValueError, "are required"):
                server.build_market_quality(
                    "BTC",
                    "selected",
                    cex_id,
                    None,
                )
            with self.assertRaisesRegex(ValueError, "must be different"):
                server.build_market_quality(
                    "BTC",
                    "selected",
                    cex_id,
                    cex_id,
                )
            with self.assertRaisesRegex(ValueError, "requested token"):
                server.build_market_quality(
                    "BTC",
                    "selected",
                    cex_id,
                    "dex:eth:uniswap:0xpool:ETH",
                )
            with self.assertRaisesRegex(ValueError, "not cataloged"):
                server.build_market_quality("ETH")

        self.assertEqual(
            [market["market_id"] for market in selected["markets"]],
            [cex_id, dex_id],
        )
        self.assertEqual(
            selected["metadata"]["selected_market_ids"],
            [cex_id, dex_id],
        )

    def test_quality_api_distinguishes_execution_states_and_preserves_zero(self):
        binance_id = "cex:binance:BTC/USDT"
        okx_id = "cex:okx:BTC/USDT"
        kraken_id = "cex:kraken:BTC/USDT"
        dex_id = "dex:eth:uniswap:0xpool:BTC"
        with self.cex_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle, lineterminator="\n").writerow(
                [
                    "2026-01-02",
                    "BTC",
                    "kraken",
                    "BTC/USDT",
                    "",
                    "",
                    "",
                    "100",
                    "",
                    "50",
                ]
            )
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            [
                *self.execution_rows(
                    binance_id,
                    "cex",
                    state_observed_at="2026-01-02T00:00:00+00:00",
                    zero_cost=True,
                ),
                *self.execution_rows(
                    okx_id,
                    "cex",
                    state_observed_at="2026-01-02T00:00:30+00:00",
                    status="failed",
                    exchange="okx",
                    source_instrument="BTC-USDT",
                ),
            ],
        )
        write_csv(
            self.dex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                dex_id,
                "dex",
                state_observed_at="2026-01-02T00:01:00+00:00",
                status="unsupported",
            ),
        )
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
            "MARKET_DEX_EXECUTION_COST_DATA": str(self.dex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                quality = server.build_market_quality("BTC")
                execution = server.build_execution_cost_comparison(
                    "BTC",
                    binance_id,
                    okx_id,
                )
        finally:
            server.clear_runtime_caches()

        by_id = {
            market["market_id"]: market["facts"]["execution"]
            for market in quality["markets"]
        }
        self.assertEqual(by_id[binance_id]["status"], "observed")
        self.assertEqual(by_id[okx_id]["status"], "failed")
        self.assertEqual(
            by_id[dex_id]["status"],
            "unsupported",
            by_id[dex_id],
        )
        self.assertEqual(
            by_id[kraken_id]["status"],
            "not_cataloged_in_snapshot",
        )
        self.assertEqual(
            by_id[binance_id]["raw_response_sha256"],
            "a" * 64,
        )
        self.assertEqual(
            by_id[okx_id]["status_reason_counts"],
            {"execution_calculation_failed": 10},
        )
        self.assertEqual(
            execution["market_a"]["rows"][0][
                "quoted_execution_cost_bps"
            ],
            "0",
        )
        self.assertIsNone(
            execution["market_b"]["rows"][0][
                "quoted_execution_cost_bps"
            ]
        )
        self.assertEqual(
            quality["metadata"]["freshness"]["cex_execution"][
                "observed_at"
            ],
            "2026-01-02T00:00:30+00:00",
        )
        self.assertEqual(
            quality["metadata"]["freshness"]["dex_execution"][
                "observed_at"
            ],
            "2026-01-02T00:01:00+00:00",
        )

    def test_execution_quality_load_errors_do_not_expose_private_paths(self):
        private_path = Path("/srv/private-market-data/execution-secret.csv")
        missing_message = f"No execution snapshot exists at {private_path}"
        unreadable_message = f"[Errno 13] Permission denied: '{private_path}'"

        def missing_resolver():
            raise FileNotFoundError(missing_message)

        missing_state = server._execution_quality_source(missing_resolver)
        with patch.object(
            server,
            "load_execution_cost_snapshot",
            side_effect=PermissionError(unreadable_message),
        ):
            unreadable_state = server._execution_quality_source(
                lambda: private_path
            )

        for source_state in (missing_state, unreadable_state):
            fact = server._execution_quality_fact({}, source_state)
            serialized = json.dumps(fact)
            self.assertEqual(fact["status"], "failed")
            self.assertEqual(
                fact["reason_code"],
                "execution_snapshot_invalid",
            )
            self.assertEqual(
                fact["reason"],
                (
                    "The execution snapshot could not be loaded or validated. "
                    "An operator must inspect the protected service logs."
                ),
            )
            self.assertNotIn(str(private_path), serialized)
            self.assertNotIn("Permission denied", serialized)
            self.assertNotIn(missing_message, serialized)

    def test_quality_api_reports_partial_execution_without_cost_claim(self):
        binance_id = "cex:binance:BTC/USDT"
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            self.execution_rows(
                binance_id,
                "cex",
                state_observed_at="2026-01-02T00:00:00+00:00",
                status="partial",
            ),
        )
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                quality = server.build_market_quality("BTC")
                execution = server.build_execution_cost_comparison(
                    "BTC",
                    binance_id,
                    "cex:okx:BTC/USDT",
                )
        finally:
            server.clear_runtime_caches()

        binance_quality = next(
            market["facts"]["execution"]
            for market in quality["markets"]
            if market["market_id"] == binance_id
        )
        self.assertEqual(binance_quality["status"], "partial")
        self.assertEqual(binance_quality["status_counts"], {"partial": 10})
        first = execution["market_a"]["rows"][0]
        self.assertEqual(first["fill_ratio"], "0.5")
        self.assertIsNone(first["quoted_execution_cost_bps"])

    def test_execution_cost_api_joins_case_sensitive_solana_pool_identity(self):
        solana_address = "AbCdEfGh"
        solana_id = f"dex:solana:orca:{solana_address}:BTC"
        eth_id = "dex:eth:uniswap:0xpool:BTC"
        with self.dex_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "2026-01-02",
                    "BTC",
                    "solana",
                    "orca",
                    solana_address,
                    "WBTC / USDC",
                    "",
                    "",
                    "",
                    "100",
                    "10",
                    "1000",
                ]
            )
        solana_common = {
            "snapshot_id": "dex-execution-1",
            "source_snapshot_id": "dex-depth-1",
            "calculation_method": "unsupported_pool_model",
            "observed_at": "2026-01-02T00:02:00+00:00",
            "request_started_at": "2026-01-02T00:00:00+00:00",
            "response_received_at": "2026-01-02T00:02:00+00:00",
            "market_id": solana_id,
            "market_type": "dex",
            "token_symbol": "BTC",
            "chain": "solana",
            "dex": "orca",
            "pool_address": solana_address,
            "protocol_model": "unsupported",
            "source": "fixture",
        }
        solana_rows = [
            execution_fact_row(
                common=solana_common,
                direction=direction,
                requested_notional_usd=notional,
                status="unsupported",
                status_reason="unsupported_protocol_or_chain",
            )
            for notional in EXECUTION_NOTIONALS_USD
            for direction in ("sell_token", "buy_token")
        ]
        write_csv(
            self.dex_execution_path,
            EXECUTION_COST_COLUMNS,
            [
                *self.execution_rows(
                    eth_id,
                    "dex",
                    state_observed_at="2026-01-02T00:01:00+00:00",
                ),
                *solana_rows,
            ],
        )
        environment = {
            **self.environment,
            "MARKET_DEX_EXECUTION_COST_DATA": str(self.dex_execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                payload = server.build_execution_cost_comparison(
                    "BTC",
                    solana_id,
                    eth_id,
                )
                quality = server.build_market_quality("BTC")
        finally:
            server.clear_runtime_caches()

        self.assertEqual(payload["market_a"]["market"]["market_id"], solana_id)
        self.assertEqual(payload["market_a"]["status"], "available")
        self.assertEqual(
            {row["status"] for row in payload["market_a"]["rows"]},
            {"unsupported"},
        )
        solana_quality = next(
            market
            for market in quality["markets"]
            if market["market_id"] == solana_id
        )
        self.assertEqual(
            solana_quality["facts"]["execution"]["status"],
            "unsupported",
        )
        self.assertFalse(
            any(
                market["market_id"]
                == f"dex:solana:orca:{solana_address.lower()}:BTC"
                for market in quality["markets"]
            )
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
        next_signature = (("facts.sqlite3", 2, 100),)
        with patch.object(
            server,
            "build_market_catalog",
            return_value={"metadata": {}, "markets": []},
        ) as build_catalog, patch.object(
            server,
            "api_source_signature",
            side_effect=[signature, next_signature],
        ):
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
                next_signature,
                100,
            )

        self.assertEqual(first, second)
        self.assertEqual(first, invalidated)
        self.assertEqual(build_catalog.call_count, 2)

    def test_public_response_discards_a_generation_that_changes_mid_build(self):
        signature_a = (("/release-a/facts.sqlite3", 1, 100),)
        signature_b = (("/release-b/facts.sqlite3", 1, 100),)
        captured = []

        def build_payload(route, query_items, source_signature=None):
            captured.append(source_signature)
            return {"metadata": {}, "markets": []}

        server._build_public_api_response_cached.cache_clear()
        with patch.object(
            server,
            "_build_public_api_payload",
            side_effect=build_payload,
        ), patch.object(
            server,
            "api_source_signature",
            return_value=signature_b,
        ):
            with self.assertRaises(server.SourceGenerationChanged):
                server._build_public_api_response_cached(
                    "catalog",
                    (),
                    signature_a,
                    100,
                )

        self.assertEqual(captured, [signature_a])

    def test_all_missing_market_volume_remains_null_but_true_zero_is_zero(self):
        cex_missing = server.summarize_cex(
            [
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "close": "100",
                    "quote_volume_usd": "",
                }
            ],
            "2026-01-01",
            "2026-01-01",
        )
        dex_missing = server.summarize_dex(
            [
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                    "pool_name": "WBTC/USDC",
                    "close": "100",
                    "dex_volume_usd": "",
                    "pool_tvl_usd": "",
                }
            ],
            "2026-01-01",
            "2026-01-01",
        )
        cex_zero = server.summarize_cex(
            [
                {
                    "date": "2026-01-01",
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "close": "100",
                    "quote_volume_usd": "0",
                }
            ],
            "2026-01-01",
            "2026-01-01",
        )

        self.assertIsNone(cex_missing[0]["volume_usd"])
        self.assertIsNone(dex_missing[0]["volume_usd"])
        self.assertEqual(cex_zero[0]["volume_usd"], 0)

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
        self.assertEqual(
            server.public_api_query_items(
                "catalog",
                {
                    "token": [" btc "],
                    "start": ["2026-01-01"],
                    "end": ["2026-01-02"],
                    "unbounded": ["ignored"],
                },
            ),
            (
                ("token", "BTC"),
                ("start", "2026-01-01"),
                ("end", "2026-01-02"),
            ),
        )
        self.assertEqual(
            server.public_api_query_items(
                "catalog",
                {
                    "start": ["2026-01-01"],
                    "end": ["2026-01-02"],
                    "unbounded": ["ignored"],
                },
            ),
            (),
        )
        self.assertEqual(
            server.public_api_query_items(
                "summary",
                {
                    "start": ["2026-01-01"],
                    "end": ["2026-01-02"],
                    "token": ["ignored"],
                },
            ),
            (
                ("start", "2026-01-01"),
                ("end", "2026-01-02"),
            ),
        )
        self.assertEqual(
            server.public_api_query_items(
                "quality",
                {
                    "token": ["BTC"],
                    "scope": ["selected"],
                    "market_a": ["cex:binance:BTC/USDT"],
                    "market_b": ["dex:eth:uniswap:0xpool:BTC"],
                    "start": ["2026-01-01"],
                    "unbounded": ["ignored"],
                },
            ),
            (
                ("token", "BTC"),
                ("scope", "selected"),
                ("market_a", "cex:binance:BTC/USDT"),
                ("market_b", "dex:eth:uniswap:0xpool:BTC"),
                ("start", "2026-01-01"),
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

    def test_only_declared_spa_routes_serve_the_application_shell(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.directory = str(server.STATIC_ROOT)
        shell = str(server.STATIC_ROOT / "index.html")
        for path in (
            "/screener",
            "/tokens/aave/markets",
            "/tokens/AAVE/compare?marketA=encoded",
            "/tokens/1INCH/liquidity",
            "/tokens/BTC/quality",
            "/methodology",
            "/methodology/execution-cost",
        ):
            self.assertTrue(server.is_spa_shell_path(path), path)
            self.assertEqual(handler.translate_path(path), shell, path)

        for path in (
            "/tokens/AAVE",
            "/tokens/AAVE/compare/extra",
            "/tokens/AAVE/compare.js",
            "/methodology/Execution-Cost",
            "/api/not-a-real-endpoint",
            "/missing-static.js",
        ):
            self.assertFalse(server.is_spa_shell_path(path), path)
            self.assertNotEqual(handler.translate_path(path), shell, path)

    def test_expert_dashboard_static_contract_prevents_stale_and_ambiguous_results(self):
        index = (server.STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")

        self.assertIn('<html lang="en">', index)
        self.assertIn('id="comparison-status"', index)
        self.assertIn('role="alert"', index)
        self.assertIn('aria-busy="true"', index)
        self.assertIn("Midpoint-relative Spread (bps)", index)
        self.assertIn("Primary Price Gap", index)
        self.assertIn('class="column-info"', index)
        self.assertIn(
            "Hover, focus, or tap a value for its CEX / DEX split.",
            index,
        )
        self.assertIn('id="export-csv"', index)
        self.assertNotIn("综合", index)

        self.assertIn("new AbortController()", app_js)
        self.assertIn("comparisonController.abort()", app_js)
        self.assertIn("marketController.abort()", app_js)
        self.assertIn("catalogController.abort()", app_js)
        self.assertIn("fetch(`/api/markets/summary?", app_js)
        self.assertIn("fetch(`/api/markets/catalog?", app_js)
        self.assertNotIn('fetch("/api/markets/catalog")', app_js)
        self.assertNotIn("fetch(`/api/market?", app_js)
        self.assertIn("requestId !== app.routeRequestId", app_js)
        self.assertIn("app.catalogsByToken.size > 8", app_js)
        comparison_loader = app_js[
            app_js.index("async function loadComparison()"):
            app_js.index("async function loadTokenCatalog(")
        ]
        market_loader = app_js[
            app_js.index("async function loadMarket("):
            app_js.index("async function applyWindow(")
        ]
        self.assertLess(
            comparison_loader.index("invalidateComparisonRequest()"),
            comparison_loader.index("validateDateRange(window.start, window.end)"),
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
        self.assertIn("validateDateRange(window.start, window.end)", app_js)
        self.assertIn("selectionOverrides", app_js)
        self.assertIn('state.pairMode = "manual";', app_js)
        self.assertIn(
            "A and B must be different. Choose another market explicitly.",
            app_js,
        )
        self.assertIn("The previous markets were cleared.", app_js)
        self.assertIn("aggregate_cex_volume_usd", app_js)
        self.assertIn("aggregate_dex_volume_share", app_js)
        self.assertIn("formatCurrency(aggregates.aggregateDex)", app_js)
        self.assertIn("formatShare(aggregates.aggregateDexShare)", app_js)
        self.assertIn("function screenerMetricTooltip(", app_js)
        screener_row = app_js[
            app_js.index("function screenerTokenRow("):
            app_js.index("function renderTable()")
        ]
        self.assertNotIn("Highest first", screener_row)
        self.assertNotIn("catalog series", screener_row)
        self.assertNotIn('class="metric-note"', screener_row)
        self.assertIn(
            'screenerMetricTooltip(priceGapValue, "Primary DEX / CEX − 1.")',
            screener_row,
        )
        self.assertIn("screenerDepthMarkup(cex, token)", screener_row)
        self.assertIn("screenerDepthMarkup(dex, token)", screener_row)
        self.assertIn("naFactMarkup(snapshotMissingReason(", screener_row)
        self.assertIn("value !== 0 && Math.abs(value) < 1", app_js)
        self.assertIn("quality_flags", app_js)
        self.assertIn('id="facts-market-a-warning-trigger"', index)
        self.assertIn('id="facts-market-b-warning-trigger"', index)
        self.assertIn('aria-controls="facts-market-a-warning-tooltip"', index)
        self.assertIn('aria-controls="facts-market-b-warning-tooltip"', index)
        self.assertGreaterEqual(index.count('aria-haspopup="dialog"'), 2)
        for slot in ("a", "b"):
            trigger_start = index.index(f'id="facts-market-{slot}-warning-trigger"')
            tooltip_start = index.index(f'id="facts-market-{slot}-warning-tooltip"')
            trigger_markup = index[trigger_start:tooltip_start]
            tooltip_markup = index[tooltip_start:tooltip_start + 220]
            self.assertIn('data-lucide="info"', trigger_markup)
            self.assertIn('role="dialog"', tooltip_markup)
        market_label = app_js[
            app_js.index("function factsMarketLabel(market)"):
            app_js.index("function factsMarketsForToken(token)")
        ]
        self.assertNotIn("quality_status", market_label)
        self.assertIn("function factsMarketWarningFlags(market)", app_js)
        self.assertIn("function bindFactsMarketWarningEvents()", app_js)
        self.assertIn('event.key === "Escape"', app_js)
        self.assertIn('document.addEventListener("pointerdown"', app_js)
        self.assertIn('document.addEventListener("keydown"', app_js)
        self.assertNotIn("Observed DEX ${formatPercent(observedShare)}", app_js)

    def test_market_table_has_accessible_mobile_card_and_depth_contracts(self):
        index = (server.STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        app_js = (server.STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        styles = (server.STATIC_ROOT / "styles.css").read_text(encoding="utf-8")

        self.assertGreaterEqual(index.count("<caption>"), 2)
        self.assertIn('scope="col"', index)
        self.assertIn('aria-label="Token and selected market facts"', index)
        self.assertIn(
            'aria-label="Selected Token market inventory"',
            index,
        )
        self.assertIn('data-set-market-slot="a"', app_js)
        self.assertIn('data-set-market-slot="b"', app_js)
        self.assertIn('aria-pressed="${String(selectedA)}"', app_js)
        self.assertIn('aria-pressed="${String(selectedB)}"', app_js)
        for band in (10, 25, 50, 100):
            self.assertIn(str(band), app_js)
        self.assertIn("function liquiditySideDefinition(market)", app_js)
        self.assertIn('sellField: "bid"', app_js)
        self.assertIn('buyField: "ask"', app_js)
        self.assertIn('sellField: "sell"', app_js)
        self.assertIn('buyField: "buy"', app_js)
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

const warningMarket = {
  market_type: "dex",
  market_id: "dex:test:warning:GOOD",
  venue: "ethereum / test",
  instrument: "GOOD / USD <pool>",
  quality_status: "warning",
  depth_status: "unsupported",
  quality_flag_details: [
    {
      code: "depth_unsupported",
      severity: "warning",
      message: "Executable depth is unsupported for this market.",
      observed_value: "unsupported",
      threshold: null,
    },
    {
      code: "tiny_pool",
      severity: "warning",
      message: "Pool TVL is below the quality threshold.",
      observed_value: 5000,
      threshold: 100000,
    },
  ],
};
const warningFlags = factsMarketWarningFlags(warningMarket);
assert.equal(warningFlags.length, 2);
assert.equal(warningFlags[1].observedValue, 5000);
assert.equal(warningFlags[1].threshold, 100000);
assert.match(qualityFlagMeasurement(warningFlags[1]), /Observed \$5,000 · minimum \$100,000/);
const warningMarkup = factsMarketWarningMarkup(
  "Market A",
  warningMarket,
  warningFlags,
  "warning",
);
assert.match(warningMarkup, /Depth unsupported/);
assert.match(warningMarkup, /Tiny pool/);
assert.match(warningMarkup, /GOOD \/ USD &lt;pool&gt;/);
assert.doesNotMatch(warningMarkup, /GOOD \/ USD <pool>/);

const cleanMarket = {
  market_type: "cex",
  market_id: "cex:test:GOOD/USDT",
  venue: "test",
  instrument: "GOOD/USDT",
  quality_status: "ok",
  depth_status: "observed",
};
assert.deepEqual(factsMarketWarningFlags(cleanMarket), []);
assert.deepEqual(factsMarketWarningFlags(null), []);

const unexplainedWarning = {
  ...cleanMarket,
  market_id: "cex:test:warning:GOOD/USDT",
  quality_status: "warning",
};
const fallbackFlags = factsMarketWarningFlags(unexplainedWarning);
assert.equal(fallbackFlags.length, 1);
assert.equal(fallbackFlags[0].code, "catalog_quality_status");
assert.match(fallbackFlags[0].explanation, /did not supply a structured reason/);

const unexplainedInfo = {
  ...cleanMarket,
  market_id: "cex:test:info:GOOD/USDT",
  quality_status: "info",
};
const fallbackInfoFlags = factsMarketWarningFlags(unexplainedInfo);
assert.equal(fallbackInfoFlags.length, 1);
assert.equal(fallbackInfoFlags[0].severity, "info");

const zeroDepthMarket = {
  ...cleanMarket,
  total_depth_10bps_usd: 0,
  quality_status: "warning",
  quality_flags: ["zero_depth_10bps"],
  quality_flag_details: [{
    code: "zero_depth_10bps",
    severity: "warning",
    message: "No executable notional was observed inside the ±10 bps band.",
    observed_value: 0,
    threshold: { band_bps: 10, quoted_spread_bps: 25 },
  }],
};
const zeroDepthFlags = factsMarketWarningFlags(zeroDepthMarket);
assert.equal(
  zeroDepthFlags.filter((flag) => flag.code === "zero_depth_10bps").length,
  1,
);
assert.equal(zeroDepthFlags.length, 1);

const notCatalogedMarket = {
  ...cleanMarket,
  depth_status: "not_cataloged_in_snapshot",
  quality_status: "info",
  quality_flags: ["depth_unavailable"],
  quality_flag_details: [{
    code: "depth_unavailable",
    severity: "info",
    message: "No executable-depth observation is available.",
    observed_value: "not_cataloged_in_snapshot",
    threshold: null,
  }],
};
const notCatalogedFlags = factsMarketWarningFlags(notCatalogedMarket);
assert.equal(notCatalogedFlags.length, 1);
assert.equal(notCatalogedFlags[0].code, "depth_unavailable");
assert.equal(notCatalogedFlags[0].severity, "info");

const mixedCodesOnly = {
  market_type: "dex",
  market_id: "dex:test:mixed:GOOD",
  venue: "ethereum / test",
  instrument: "GOOD / USD",
  quality_status: "critical",
  depth_status: "unsupported_protocol",
  tvl_usd: 5000,
  price_difference_bps: 750,
  quality_flags: [
    "depth_unsupported",
    "tiny_pool",
    "off_market_pool_state_price",
  ],
};
const mixedCodeFlags = factsMarketWarningFlags(mixedCodesOnly);
assert.deepEqual(
  Object.fromEntries(mixedCodeFlags.map((flag) => [flag.code, flag.severity])),
  {
    depth_unsupported: "warning",
    tiny_pool: "warning",
    off_market_pool_state_price: "critical",
  },
);
assert.equal(factsMarketWarningSeverity(mixedCodesOnly, mixedCodeFlags), "critical");

assert.equal(
  qualityFlagMeasurement({
    code: "zero_depth_10bps",
    observedValue: 0,
    threshold: { band_bps: 10, quoted_spread_bps: 25 },
  }),
  "Observed $0 in the ±10 bps band · quoted spread 25 bps",
);
assert.equal(
  qualityFlagMeasurement({
    code: "tiny_pool",
    observedValue: 99999.99,
    threshold: 100000,
  }),
  "Observed $99,999.99 · minimum $100,000",
);
assert.equal(
  qualityFlagMeasurement({
    code: "low_daily_coverage",
    observedValue: 0.79999,
    threshold: 0.8,
  }),
  "Observed 79.999% · minimum 80%",
);
assert.equal(
  qualityFlagMeasurement({
    code: "wide_quoted_spread",
    observedValue: 100.0001,
    threshold: 100,
  }),
  "Observed 100.0001 bps · maximum 100 bps",
);
assert.equal(
  qualityFlagMeasurement({
    code: "off_market_pool_state_price",
    observedValue: -500.0001,
    threshold: 500,
  }),
  "Observed -500.0001 bps · threshold 500 bps",
);

function fakeClassList() {
  const values = new Set();
  return {
    add: (...items) => items.forEach((item) => values.add(item)),
    remove: (...items) => items.forEach((item) => values.delete(item)),
    contains: (item) => values.has(item),
  };
}

function fakeElement(parentElement = null) {
  const listeners = {};
  const attributes = {};
  return {
    parentElement,
    listeners,
    attributes,
    classList: fakeClassList(),
    dataset: {},
    hidden: false,
    innerHTML: "",
    textContent: "",
    addEventListener: (type, callback) => {
      listeners[type] = callback;
    },
    contains: (candidate) => candidate === null ? false : candidate === this,
    getAttribute: (name) => attributes[name] ?? null,
    removeAttribute: (name) => {
      delete attributes[name];
    },
    setAttribute: (name, value) => {
      attributes[name] = String(value);
    },
  };
}

const warningDom = {};
for (const slot of ["a", "b"]) {
  const shell = fakeElement();
  const anchor = fakeElement(shell);
  const trigger = fakeElement(anchor);
  const tooltip = fakeElement(anchor);
  const status = fakeElement(anchor);
  anchor.hidden = true;
  trigger.hidden = true;
  anchor.contains = (candidate) => (
    candidate === anchor
    || candidate === trigger
    || candidate === tooltip
    || candidate === status
  );
  warningDom[`facts-market-${slot}-warning`] = anchor;
  warningDom[`facts-market-${slot}-warning-trigger`] = trigger;
  warningDom[`facts-market-${slot}-warning-tooltip`] = tooltip;
  warningDom[`facts-market-${slot}-warning-status`] = status;
}
const documentListeners = {};
global.document = {
  activeElement: null,
  addEventListener: (type, callback) => {
    documentListeners[type] = callback;
  },
  getElementById: (id) => warningDom[id] || null,
};

renderFactsMarketWarning("a", warningMarket);
renderFactsMarketWarning("b", warningMarket);
assert.equal(warningDom["facts-market-a-warning"].hidden, false);
assert.equal(
  warningDom["facts-market-a-warning"].parentElement.classList.contains("has-market-warning"),
  true,
);
bindFactsMarketWarningEvents();

const anchorA = warningDom["facts-market-a-warning"];
const triggerA = warningDom["facts-market-a-warning-trigger"];
const tooltipA = warningDom["facts-market-a-warning-tooltip"];
const anchorB = warningDom["facts-market-b-warning"];
const triggerB = warningDom["facts-market-b-warning-trigger"];
const tooltipB = warningDom["facts-market-b-warning-tooltip"];

anchorA.listeners.pointerenter();
assert.equal(tooltipA.hidden, false);
anchorA.listeners.pointerleave();
assert.equal(tooltipA.hidden, true);

global.document.activeElement = triggerA;
anchorA.listeners.focusin();
anchorA.listeners.pointerleave();
assert.equal(tooltipA.hidden, false);
global.document.activeElement = null;
anchorA.listeners.focusout({ relatedTarget: null });
assert.equal(tooltipA.hidden, true);

global.document.activeElement = triggerA;
triggerA.listeners.click({ stopPropagation: () => {} });
assert.equal(anchorA.dataset.pinned, "true");
assert.equal(tooltipA.hidden, false);
anchorB.listeners.pointerenter();
assert.equal(tooltipA.hidden, false);
assert.equal(tooltipB.hidden, true);

documentListeners.pointerdown({ target: { closest: () => null } });
assert.equal(tooltipA.hidden, true);
assert.equal(anchorA.dataset.pinned, "false");

global.document.activeElement = null;
anchorB.listeners.pointerenter();
assert.equal(tooltipB.hidden, false);
documentListeners.keydown({ key: "Escape" });
assert.equal(tooltipB.hidden, true);

global.document.activeElement = triggerB;
triggerB.listeners.click({ stopPropagation: () => {} });
assert.equal(tooltipA.hidden, true);
assert.equal(tooltipB.hidden, false);

renderFactsMarketWarning("a", cleanMarket);
assert.equal(anchorA.hidden, true);
assert.equal(anchorA.parentElement.classList.contains("has-market-warning"), false);
renderFactsMarketWarning("b", null);
assert.equal(anchorB.hidden, true);

delete global.document;

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
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is not installed in this runtime")
        completed = subprocess.run(
            [node, "-"],
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
