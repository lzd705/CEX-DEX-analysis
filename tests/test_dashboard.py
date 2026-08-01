import csv
import gzip
import inspect
import json
import shutil
import subprocess
import tempfile
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

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
from scripts.quality_outcomes import quality_outcome_rule
from scripts.check_dashboard_release import (
    ResponseMetrics,
    validate_quality,
    validate_summary,
)


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

    def cohort_validator(self):
        validator = getattr(
            server,
            "validate_depth_execution_cohort",
            None,
        )
        self.assertIsNotNone(
            validator,
            "validate_depth_execution_cohort must be implemented",
        )
        return validator

    @staticmethod
    def cohort_fixture(market_type="cex"):
        row_count_field = "market_rows" if market_type == "cex" else "pool_rows"
        metadata = {
            f"{market_type}_depth_snapshot": {
                "snapshot_ids": [f"{market_type}-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:07+00:00",
                "observation_span_seconds": 7,
                row_count_field: 2,
            }
        }
        snapshot = {
            "snapshot_ids": [f"{market_type}-cohort-1"],
            "source_snapshot_ids": [f"{market_type}-cohort-1"],
            "observed_at": "2026-01-02T00:00:08+00:00",
            "observed_at_min": "2026-01-02T00:00:08+00:00",
            "observed_at_max": "2026-01-02T00:00:12+00:00",
            "observation_span_seconds": 4,
            "market_count": 2,
        }
        return metadata, snapshot

    def test_matching_depth_execution_cohort_returns_one_bounded_projection(self):
        metadata, snapshot = self.cohort_fixture()

        projection = self.cohort_validator()(metadata, snapshot, "cex")

        self.assertEqual(
            projection,
            {
                "market_type": "cex",
                "depth_snapshot_id": "cex-cohort-1",
                "execution_snapshot_id": "cex-cohort-1",
                "execution_source_snapshot_id": "cex-cohort-1",
                "depth_market_count": 2,
                "execution_market_count": 2,
            },
        )

    def test_depth_execution_cohort_rejects_wrong_lineage_ids(self):
        mutations = (
            ("snapshot_ids", ["wrong-execution-snapshot"]),
            ("source_snapshot_ids", ["wrong-source-snapshot"]),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                metadata, snapshot = self.cohort_fixture()
                snapshot[field] = value
                with self.assertRaises(RuntimeError):
                    self.cohort_validator()(metadata, snapshot, "cex")

    def test_depth_execution_cohort_rejects_multiple_lineage_ids(self):
        mutations = (
            ("depth", "snapshot_ids"),
            ("execution", "snapshot_ids"),
            ("execution", "source_snapshot_ids"),
        )
        for location, field in mutations:
            with self.subTest(location=location, field=field):
                metadata, snapshot = self.cohort_fixture()
                target = (
                    metadata["cex_depth_snapshot"]
                    if location == "depth"
                    else snapshot
                )
                target[field] = ["cex-cohort-1", "cex-cohort-2"]
                with self.assertRaises(RuntimeError):
                    self.cohort_validator()(metadata, snapshot, "cex")

    def test_depth_cohort_loaders_reject_invalid_raw_snapshot_ids(self):
        families = (
            (
                "cex",
                self.depth_path,
                DEPTH_COLUMNS_ALL,
                lambda: self.write_cex_depth_cohort(
                    [
                        {"exchange": "binance"},
                        {
                            "exchange": "okx",
                            "source_instrument": "BTC-USDT",
                        },
                    ]
                ),
                server._load_cex_depth_snapshot_cached,
            ),
            (
                "dex",
                self.dex_depth_path,
                DEX_DEPTH_COLUMNS,
                lambda: self.write_dex_depth_cohort(
                    [
                        {
                            "chain": "eth",
                            "dex": "uniswap",
                            "pool_address": "0xpool",
                        },
                        {
                            "chain": "eth",
                            "dex": "sushiswap",
                            "pool_address": "0xother",
                        },
                    ]
                ),
                server._load_dex_depth_snapshot_cached,
            ),
        )
        for family, path, columns, write_fixture, loader in families:
            for invalid_value in ("", f" {family}-depth-1 ", f"{family}-depth-2"):
                with self.subTest(family=family, value=invalid_value):
                    write_fixture()
                    with path.open(newline="", encoding="utf-8") as handle:
                        rows = list(csv.DictReader(handle))
                    rows[-1]["snapshot_id"] = invalid_value
                    write_csv(path, columns, rows)
                    loader.cache_clear()
                    with self.assertRaises(server.DepthExecutionCohortError):
                        loader(str(path), server.data_signature([path]))

    def test_depth_cohort_loaders_reject_duplicate_market_identity(self):
        fixtures = (
            (
                "cex",
                self.depth_path,
                lambda: self.write_cex_depth_cohort(
                    [
                        {"exchange": "binance"},
                        {
                            "exchange": "binance",
                            "observed_at": "2026-01-02T00:00:01+00:00",
                        },
                        {
                            "exchange": "okx",
                            "source_instrument": "BTC-USDT",
                        },
                    ]
                ),
                server._load_cex_depth_snapshot_cached,
            ),
            (
                "dex",
                self.dex_depth_path,
                lambda: self.write_dex_depth_cohort(
                    [
                        {
                            "chain": "eth",
                            "dex": "uniswap",
                            "pool_address": "0xpool",
                        },
                        {
                            "chain": "eth",
                            "dex": "uniswap",
                            "pool_address": "0xpool",
                            "observed_at": "2026-01-02T00:00:01+00:00",
                        },
                        {
                            "chain": "eth",
                            "dex": "sushiswap",
                            "pool_address": "0xother",
                        },
                    ]
                ),
                server._load_dex_depth_snapshot_cached,
            ),
        )
        for family, path, write_fixture, loader in fixtures:
            with self.subTest(family=family):
                write_fixture()
                loader.cache_clear()
                with self.assertRaises(server.DepthExecutionCohortError):
                    loader(str(path), server.data_signature([path]))

    def test_execution_loader_classifies_multiple_ids_as_cohort_mismatch(self):
        market_id = "cex:binance:BTC/USDT"
        error_type = getattr(server, "DepthExecutionCohortError", RuntimeError)
        for field, value in (
            ("snapshot_id", "cex-depth-2"),
            ("source_snapshot_id", "cex-depth-2"),
            ("snapshot_id", " cex-depth-1 "),
        ):
            with self.subTest(field=field, value=value):
                rows = self.execution_rows(
                    market_id,
                    "cex",
                    state_observed_at="2026-01-02T00:00:00+00:00",
                )
                rows[-1][field] = value
                write_csv(
                    self.cex_execution_path,
                    EXECUTION_COST_COLUMNS,
                    rows,
                )
                server._load_execution_cost_snapshot_cached.cache_clear()
                try:
                    server.load_execution_cost_snapshot(
                        self.cex_execution_path
                    )
                except error_type:
                    pass
                except ValueError:
                    self.fail(
                        "execution lineage mismatch must use the cohort error"
                    )
                else:
                    self.fail("execution lineage mismatch was accepted")

    def test_execution_loader_short_row_is_typed_cohort_mismatch(self):
        rows = self.execution_rows(
            "cex:binance:BTC/USDT",
            "cex",
            state_observed_at="2026-01-02T00:00:00+00:00",
        )
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            rows,
        )
        with self.cex_execution_path.open(
            "a",
            newline="",
            encoding="utf-8",
        ) as handle:
            csv.writer(handle, lineterminator="\n").writerow(
                ["cex-depth-1"]
            )
        server._load_execution_cost_snapshot_cached.cache_clear()

        try:
            server.load_execution_cost_snapshot(self.cex_execution_path)
        except server.DepthExecutionCohortError:
            pass
        except TypeError:
            self.fail("short execution rows must not fail during set sorting")
        else:
            self.fail("short execution row lineage was accepted")

    def test_depth_execution_cohort_rejects_market_count_mismatch(self):
        metadata, snapshot = self.cohort_fixture("dex")
        snapshot["market_count"] = 1

        with self.assertRaises(RuntimeError):
            self.cohort_validator()(metadata, snapshot, "dex")

    def test_observation_span_helper_preserves_missing_bound_for_noncohort_callers(self):
        span = getattr(server, "observation_span_seconds", None)
        self.assertIsNotNone(
            span,
            "observation_span_seconds must be implemented",
        )
        self.assertEqual(
            span(
                "2026-01-02T00:00:07+00:00",
                "2026-01-02T00:00:12+00:00",
            ),
            5,
        )
        with self.assertRaises(server.DepthExecutionCohortError):
            span(
                "2026-01-02T00:00:12+00:00",
                "2026-01-02T00:00:07+00:00",
            )
        with self.assertRaises(server.DepthExecutionCohortError):
            span("not-a-timestamp", "2026-01-02T00:00:07+00:00")
        with self.assertRaises(server.DepthExecutionCohortError):
            span(
                "2026-01-02T00:00:00",
                "2026-01-02T00:00:07+00:00",
            )
        self.assertIsNone(span(None, "2026-01-02T00:00:07+00:00"))

    def test_depth_execution_cohort_rejects_inconsistent_observation_metadata(self):
        mutations = (
            ("depth", "observed_at_min", "not-a-timestamp"),
            ("depth", "observed_at_max", "2026-01-02T00:00:07"),
            ("depth", "observation_span_seconds", -1),
            ("execution", "observation_span_seconds", 99),
            ("execution", "observed_at_min", None),
        )
        for location, field, value in mutations:
            with self.subTest(location=location, field=field, value=value):
                metadata, snapshot = self.cohort_fixture()
                target = (
                    metadata["cex_depth_snapshot"]
                    if location == "depth"
                    else snapshot
                )
                target[field] = value
                with self.assertRaises(server.DepthExecutionCohortError):
                    self.cohort_validator()(metadata, snapshot, "cex")

    def test_positive_cohort_inventory_rejects_all_null_observation_bounds(self):
        for location in ("depth", "execution"):
            with self.subTest(location=location):
                metadata, snapshot = self.cohort_fixture()
                target = (
                    metadata["cex_depth_snapshot"]
                    if location == "depth"
                    else snapshot
                )
                for field in (
                    "observed_at",
                    "observed_at_min",
                    "observed_at_max",
                    "observation_span_seconds",
                ):
                    target[field] = None
                with self.assertRaises(server.DepthExecutionCohortError):
                    self.cohort_validator()(metadata, snapshot, "cex")

    def test_cohort_loaders_reject_malformed_or_naive_observation_timestamps(self):
        depth_fixtures = (
            (
                "cex",
                self.depth_path,
                lambda: self.write_cex_depth_cohort(
                    [
                        {"exchange": "binance"},
                        {
                            "exchange": "okx",
                            "source_instrument": "BTC-USDT",
                        },
                    ]
                ),
                DEPTH_COLUMNS_ALL,
                server._load_cex_depth_snapshot_cached,
            ),
            (
                "dex",
                self.dex_depth_path,
                lambda: self.write_dex_depth_cohort(
                    [
                        {
                            "chain": "eth",
                            "dex": "uniswap",
                            "pool_address": "0xpool",
                        },
                        {
                            "chain": "eth",
                            "dex": "sushiswap",
                            "pool_address": "0xother",
                        },
                    ]
                ),
                DEX_DEPTH_COLUMNS,
                server._load_dex_depth_snapshot_cached,
            ),
        )
        for family, path, write_fixture, columns, loader in depth_fixtures:
            for invalid_time in ("", "not-a-time", "2026-01-02T00:00:00"):
                with self.subTest(family=family, invalid_time=invalid_time):
                    write_fixture()
                    with path.open(newline="", encoding="utf-8") as handle:
                        rows = list(csv.DictReader(handle))
                    rows[-1]["observed_at"] = invalid_time
                    write_csv(path, columns, rows)
                    loader.cache_clear()
                    with self.assertRaises(server.DepthExecutionCohortError):
                        loader(str(path), server.data_signature([path]))

        for field in ("observed_at", "state_observed_at"):
            invalid_times = ("not-a-time", "2026-01-02T00:00:00")
            if field == "observed_at":
                invalid_times = ("", *invalid_times)
            for invalid_time in invalid_times:
                with self.subTest(field=field, invalid_time=invalid_time):
                    rows = self.execution_rows(
                        "cex:binance:BTC/USDT",
                        "cex",
                        state_observed_at="2026-01-02T00:00:00+00:00",
                    )
                    rows[-1][field] = invalid_time
                    write_csv(
                        self.cex_execution_path,
                        EXECUTION_COST_COLUMNS,
                        rows,
                    )
                    server._load_execution_cost_snapshot_cached.cache_clear()
                    with self.assertRaises(server.DepthExecutionCohortError):
                        server.load_execution_cost_snapshot(
                            self.cex_execution_path
                        )

    def test_invalid_execution_cohort_isolated_from_unrelated_fact_endpoints(self):
        rows = [
            *self.execution_rows(
                "cex:binance:BTC/USDT",
                "cex",
                state_observed_at="2026-01-02T00:00:00+00:00",
            ),
            *self.execution_rows(
                "cex:okx:BTC/USDT",
                "cex",
                state_observed_at="2026-01-02T00:00:01+00:00",
                exchange="okx",
                source_instrument="BTC-USDT",
            ),
        ]
        rows[-1]["snapshot_id"] = "cex-depth-2"
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            rows,
        )
        environment = {
            **self.environment,
            "MARKET_CEX_EXECUTION_COST_DATA": str(self.cex_execution_path),
        }
        market_a = "cex:binance:BTC/USDT"
        market_b = "cex:okx:BTC/USDT"
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                unrelated_requests = (
                    ("catalog", ()),
                    ("summary", ()),
                    ("market", ()),
                    (
                        "compare",
                        (
                            ("token", "BTC"),
                            ("market_a", market_a),
                            ("market_b", market_b),
                        ),
                    ),
                )
                for route, query in unrelated_requests:
                    with self.subTest(route=route):
                        try:
                            payload = server._build_public_api_payload(
                                route,
                                query,
                            )
                        except server.DepthExecutionCohortError:
                            self.fail(
                                f"{route} leaked execution cohort failure"
                            )
                        self.assertIsInstance(payload, dict)

                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = "/health"
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    try:
                        handler.do_GET()
                    except server.DepthExecutionCohortError:
                        self.fail("health leaked execution cohort failure")
                self.assertEqual(send_json.call_args.args[0]["status"], "ok")

                with self.assertRaises(server.DepthExecutionCohortError):
                    server.build_execution_cost_comparison(
                        "BTC",
                        market_a,
                        market_b,
                    )
                with self.assertRaises(server.DepthExecutionCohortError):
                    server.build_market_quality(
                        "BTC",
                        "selected",
                        market_a,
                        market_b,
                    )
        finally:
            server.clear_runtime_caches()

    def test_cex_depth_cohort_metadata_exposes_canonical_observation_span(self):
        depth_row = {field: "" for field in DEPTH_COLUMNS_ALL}
        depth_row.update(
            {
                "snapshot_id": "cex-cohort-1",
                "observed_at": "2026-01-02T00:00:07+00:00",
                "response_received_at": "2026-01-02T00:00:08+00:00",
                "token_symbol": "BTC",
                "exchange": "binance",
                "cex_symbol": "BTC/USDT",
                "source_instrument": "BTCUSDT",
                "source_quote_asset": "USDT",
                "quote_conversion_method": "USDT=USD proxy",
                "depth_method": "midpoint_symmetric_quote_notional",
                "source_endpoint": "https://example.test/depth",
                "raw_response_sha256": "a" * 64,
                "status": "observed",
            }
        )
        write_csv(self.depth_path, DEPTH_COLUMNS_ALL, [depth_row])

        snapshot = server._load_cex_depth_snapshot_cached(
            str(self.depth_path),
            server.data_signature([self.depth_path]),
        )

        self.assertIn("observation_span_seconds", snapshot)
        self.assertEqual(snapshot["observation_span_seconds"], 0)

    def test_cex_nanosecond_source_timestamps_are_canonicalized_on_load(self):
        source_time = "2026-07-31T23:05:47.660676312Z"
        canonical_time = "2026-07-31T23:05:47.660676+00:00"
        self.write_cex_depth_cohort(
            [
                {
                    "exchange": "coinbase",
                    "source_instrument": "BTC-USDT",
                    "observed_at": source_time,
                }
            ]
        )
        depth_snapshot = server._load_cex_depth_snapshot_cached(
            str(self.depth_path),
            server.data_signature([self.depth_path]),
        )
        self.assertEqual(depth_snapshot["observed_at"], canonical_time)
        self.assertEqual(
            next(iter(depth_snapshot["rows"].values()))["observed_at"],
            canonical_time,
        )

        rows = self.execution_rows(
            "cex:coinbase:BTC/USDT",
            "cex",
            state_observed_at=source_time,
            exchange="coinbase",
            source_instrument="BTC-USDT",
        )
        for row in rows:
            row["observed_at"] = source_time
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            rows,
        )
        execution_snapshot = server.load_execution_cost_snapshot(
            self.cex_execution_path
        )
        self.assertEqual(execution_snapshot["observed_at"], canonical_time)
        self.assertTrue(
            all(
                row["observed_at"] == canonical_time
                and row["state_observed_at"] == canonical_time
                for row in execution_snapshot["rows"]
            )
        )

    def test_depth_execution_cohort_mismatch_is_http_503_for_public_fact_routes(self):
        error_type = getattr(server, "DepthExecutionCohortError", None)
        self.assertIsNotNone(
            error_type,
            "DepthExecutionCohortError must be implemented",
        )
        paths = (
            "/api/markets/execution-cost?token=BTC&market_a=a&market_b=b",
            (
                "/api/markets/quality?token=BTC&scope=selected"
                "&market_a=a&market_b=b"
            ),
        )
        expected = {
            "code": "public_data_validation_failed",
            "message": (
                "Published market fact data failed validation. "
                "Retry after the next refresh."
            ),
        }
        for path in paths:
            with self.subTest(path=path):
                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = path
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_public_api",
                    side_effect=error_type("private cohort mismatch"),
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    try:
                        handler.do_GET()
                    except RuntimeError:
                        pass

                send_json.assert_called_once_with(expected, 503)
                self.assertNotIn(
                    "private cohort mismatch",
                    json.dumps(send_json.call_args.args[0]),
                )

    def test_depth_cohort_mismatch_is_controlled_on_shared_fact_routes(self):
        error = server.DepthExecutionCohortError(
            "private malformed depth publication"
        )
        expected = {
            "code": "public_data_validation_failed",
            "message": (
                "Published market fact data failed validation. "
                "Retry after the next refresh."
            ),
        }
        for path in (
            "/api/markets/catalog",
            "/api/markets/summary",
            "/api/markets/compare?token=BTC&market_a=a&market_b=b",
            "/api/market",
        ):
            with self.subTest(path=path):
                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = path
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_public_api",
                    side_effect=error,
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    handler.do_GET()
                send_json.assert_called_once_with(expected, 503)

        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/health"
        with patch.object(
            server,
            "build_market_payload",
            side_effect=error,
        ), patch.object(
            server.MarketMonitorHandler,
            "send_json",
        ) as send_json:
            handler.do_GET()
        send_json.assert_called_once_with(
            {
                "status": "degraded",
                "data_ready": False,
                "error": server.PUBLIC_DATA_UNAVAILABLE_MESSAGE,
            },
            503,
        )

    def test_utc_normalization_overflow_is_controlled_for_get_and_health(self):
        expected_get = {
            "code": "public_data_validation_failed",
            "message": (
                "Published market fact data failed validation. "
                "Retry after the next refresh."
            ),
        }
        expected_health = {
            "status": "degraded",
            "data_ready": False,
            "error": server.PUBLIC_DATA_UNAVAILABLE_MESSAGE,
        }
        for timestamp in (
            "0001-01-01T00:00:00+23:59",
            "9999-12-31T23:59:59-23:59",
        ):
            with self.subTest(timestamp=timestamp):
                def parse_overflow(*_args, **_kwargs):
                    return server._parse_cohort_timestamp(
                        timestamp,
                        "private cohort timestamp",
                    )

                with self.assertRaises(server.DepthExecutionCohortError):
                    parse_overflow()

                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = "/api/market"
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_public_api",
                    side_effect=parse_overflow,
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    handler.do_GET()
                send_json.assert_called_once_with(expected_get, 503)

                health_handler = object.__new__(server.MarketMonitorHandler)
                health_handler.path = "/health"
                with patch.object(
                    server,
                    "build_market_payload",
                    side_effect=parse_overflow,
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    health_handler.do_GET()
                send_json.assert_called_once_with(expected_health, 503)

    def test_corrupt_depth_file_is_controlled_by_real_public_handlers(self):
        self.write_cex_depth_cohort(
            [
                {"exchange": "binance"},
                {
                    "exchange": "okx",
                    "source_instrument": "BTC-USDT",
                },
            ]
        )
        with self.depth_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[-1]["snapshot_id"] = ""
        write_csv(self.depth_path, DEPTH_COLUMNS_ALL, rows)
        environment = {
            **self.environment,
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
        }
        expected = {
            "code": "public_data_validation_failed",
            "message": (
                "Published market fact data failed validation. "
                "Retry after the next refresh."
            ),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                for path in (
                    "/api/markets/catalog",
                    "/api/markets/summary",
                    (
                        "/api/markets/compare?token=BTC"
                        "&market_a=cex:binance:BTC/USDT"
                        "&market_b=cex:okx:BTC/USDT"
                    ),
                    "/api/market",
                ):
                    with self.subTest(path=path):
                        handler = object.__new__(server.MarketMonitorHandler)
                        handler.path = path
                        handler.headers = {}
                        with patch.object(
                            server.MarketMonitorHandler,
                            "send_json",
                        ) as send_json, patch.object(
                            server.MarketMonitorHandler,
                            "send_encoded_json",
                        ) as send_encoded_json:
                            handler.do_GET()
                        send_json.assert_called_once_with(expected, 503)
                        send_encoded_json.assert_not_called()

                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = "/health"
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    handler.do_GET()
                send_json.assert_called_once_with(
                    {
                        "status": "degraded",
                        "data_ready": False,
                        "error": server.PUBLIC_DATA_UNAVAILABLE_MESSAGE,
                    },
                    503,
                )
        finally:
            server.clear_runtime_caches()

    def test_warm_default_market_summary_uses_current_cache_generation(self):
        source_signature = (("market_facts.sqlite3", 10, 20),)
        with patch.object(
            server,
            "api_source_signature",
            return_value=source_signature,
        ), patch.object(
            server, "api_freshness_bucket", return_value=12345
        ), \
            patch.object(
                server,
                "_build_public_api_response_cached",
                return_value=(b"{}", True),
            ) as build_response:
            server.warm_default_market_summary()

        build_response.assert_called_once_with(
            "summary",
            (),
            source_signature,
            12345,
        )

    def test_warm_default_market_summary_failure_does_not_prevent_startup(self):
        source_signature = (("market_facts.sqlite3", 10, 20),)
        http_server = Mock()
        http_server.serve_forever.side_effect = KeyboardInterrupt
        args = server.argparse.Namespace(host="127.0.0.1", port=8765, data_dir=None)

        with patch.object(server, "parse_args", return_value=args), \
            patch.object(server, "ThreadingHTTPServer", return_value=http_server), \
            patch.object(
                server,
                "api_source_signature",
                return_value=source_signature,
            ), \
            patch.object(server, "api_freshness_bucket", return_value=12345), \
            patch.object(
                server,
                "_build_public_api_response_cached",
                side_effect=RuntimeError("private details"),
            ) as build_response, \
            patch("builtins.print") as print_warning:
            server.main()

        build_response.assert_called_once_with(
            "summary",
            (),
            source_signature,
            12345,
        )
        print_warning.assert_any_call(
            "Default summary warmup failed: RuntimeError"
        )
        http_server.serve_forever.assert_called_once_with()
        http_server.server_close.assert_called_once_with()

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
            "snapshot_id": f"{market_type}-depth-1",
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

    def write_cex_depth_cohort(self, markets):
        rows = []
        for market in markets:
            row = {field: "" for field in DEPTH_COLUMNS_ALL}
            row.update(
                {
                    "snapshot_id": "cex-depth-1",
                    "observed_at": market.get(
                        "observed_at",
                        "2026-01-02T00:00:00+00:00",
                    ),
                    "response_received_at": market.get(
                        "observed_at",
                        "2026-01-02T00:00:00+00:00",
                    ),
                    "token_symbol": market.get("token_symbol", "BTC"),
                    "exchange": market["exchange"],
                    "cex_symbol": market.get("cex_symbol", "BTC/USDT"),
                    "source_instrument": market.get(
                        "source_instrument",
                        "BTCUSDT",
                    ),
                    "source_quote_asset": "USDT",
                    "quote_conversion_method": "USDT=USD proxy",
                    "depth_method": "midpoint_symmetric_quote_notional",
                    "source_endpoint": "https://example.test/depth",
                    "raw_response_sha256": "d" * 64,
                    "status": "observed",
                }
            )
            rows.append(row)
        write_csv(self.depth_path, DEPTH_COLUMNS_ALL, rows)

    def write_dex_depth_cohort(self, pools):
        rows = []
        for pool in pools:
            row = {field: "" for field in DEX_DEPTH_COLUMNS}
            row.update(
                {
                    "snapshot_id": "dex-depth-1",
                    "observed_at": pool.get(
                        "observed_at",
                        "2026-01-02T00:00:00+00:00",
                    ),
                    "response_received_at": pool.get(
                        "observed_at",
                        "2026-01-02T00:00:00+00:00",
                    ),
                    "token_symbol": pool.get("token_symbol", "BTC"),
                    "chain": pool["chain"],
                    "dex": pool["dex"],
                    "pool_address": pool["pool_address"],
                    "protocol_model": pool.get(
                        "protocol_model",
                        "constant_product_v2",
                    ),
                    "block_number": pool.get("block_number", "123"),
                    "block_timestamp": pool.get(
                        "observed_at",
                        "2026-01-02T00:00:00+00:00",
                    ),
                    "depth_method": (
                        "fixed_block_pool_state_marginal_price_band"
                    ),
                    "source_endpoint": "https://example.test/depth",
                    "raw_response_sha256": "d" * 64,
                    "status": "observed",
                }
            )
            rows.append(row)
        write_csv(self.dex_depth_path, DEX_DEPTH_COLUMNS, rows)

    def test_parse_number_preserves_missing_values(self):
        self.assertIsNone(server.parse_number(""))
        self.assertIsNone(server.parse_number("nan"))
        self.assertEqual(server.parse_number("12.5"), 12.5)
        self.assertEqual(server.parse_number(12.5), 12.5)

    def test_failed_tvl_and_dex_depth_use_same_bounded_retryable_outcome(self):
        private_error = (
            "PermissionError: /srv/private/facts.csv?credential=secret"
        )
        tvl = server._tvl_quality_fact({
            "market_type": "dex",
            "tvl_status": "failed",
            "tvl_error": private_error,
        })
        depth = server._depth_quality_fact({
            "market_type": "dex",
            "depth_status": "failed",
            "depth_error": private_error,
        })

        for fact in (tvl, depth):
            with self.subTest(fact=fact):
                self.assertEqual(fact["status"], "collection_failed")
                self.assertEqual(fact["reason"], "source_unavailable")
                self.assertEqual(fact["reason_code"], "source_unavailable")
                self.assertTrue(fact["retryable"])
                self.assertIsNotNone(
                    quality_outcome_rule(fact["status"], fact["reason_code"])
                )
                self.assertNotIn(private_error, json.dumps(fact))

        unavailable = server._depth_quality_fact({
            "market_type": "cex",
            "depth_status": "unavailable",
        })
        self.assertEqual(
            (unavailable["status"], unavailable["reason_code"]),
            ("unavailable", "depth_snapshot_unavailable"),
        )
        self.assertIsNotNone(
            quality_outcome_rule(
                unavailable["status"], unavailable["reason_code"]
            )
        )

    def test_public_api_projection_recursively_removes_private_collector_evidence(self):
        malicious_catalog = {
            "metadata": {"catalog_version": 3, "data_generation": "generation"},
            "markets": [{
                "market_id": "cex:test:BTC/USDT",
                "depth_error": "PermissionError: /srv/private/depth.csv",
                "tvl_error": "C:\\service\\private\\tvl.csv",
                "depth_source_endpoint": (
                    "https://user:password@api.example.test:8443/v2/depth"
                    "?api_key=raw-secret#collector"
                ),
                "quote_conversion_endpoint": (
                    "https://fx-user:fx-secret@fx.example.test/v1/quote"
                    "?access_token=private"
                ),
                "nested": {
                    "errors": [
                        "SourceException: /srv/private/collector.py",
                    ],
                    "source_endpoint": (
                        "http://collector:credential@rpc.example.test/path/to/key"
                    ),
                },
            }],
        }
        with patch.object(
            server,
            "build_market_catalog",
            return_value=malicious_catalog,
        ):
            payload = server._build_public_api_payload(
                "catalog",
                (),
                source_signature=(("/srv/private/facts.sqlite3", 1, 1),),
            )

        market = payload["markets"][0]
        self.assertNotIn("depth_error", market)
        self.assertNotIn("tvl_error", market)
        self.assertNotIn("errors", market["nested"])
        self.assertEqual(
            market["depth_source_endpoint"],
            "https://api.example.test:8443",
        )
        self.assertEqual(
            market["quote_conversion_endpoint"],
            "https://fx.example.test",
        )
        self.assertEqual(
            market["nested"]["source_endpoint"],
            "http://rpc.example.test",
        )
        serialized = json.dumps(payload)
        for forbidden in (
            "/srv/",
            "C:\\\\service",
            "user:password",
            "collector:credential",
            "fx-user:fx-secret",
            "api_key=raw-secret",
            "access_token=private",
            "PermissionError",
            "SourceException",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, serialized)

    def test_public_get_endpoints_sanitize_internal_value_errors(self):
        private_error = ValueError(
            "/srv/private/book.csv?api_key=secret failed validation"
        )
        paths = (
            "/api/markets/catalog?token=BTC",
            "/api/markets/summary",
            "/api/markets/compare?token=BTC&market_a=a&market_b=b",
            "/api/markets/execution-cost?token=BTC&market_a=a&market_b=b",
            "/api/markets/quality?token=BTC",
            "/api/markets/events?token=BTC",
            "/api/market",
        )
        expected = {
            "code": "public_data_validation_failed",
            "message": (
                "Published market fact data failed validation. "
                "Retry after the next refresh."
            ),
        }

        for path in paths:
            with self.subTest(path=path):
                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = path
                with patch.object(
                    server.MarketMonitorHandler,
                    "send_public_api",
                    side_effect=private_error,
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    handler.do_GET()

                send_json.assert_called_once_with(expected, 503)
                serialized = json.dumps(send_json.call_args.args[0])
                self.assertNotIn("/srv/private", serialized)
                self.assertNotIn("api_key", serialized)
                self.assertNotIn("secret", serialized)

    def test_public_get_keeps_safe_client_parameter_errors_at_400(self):
        with self.assertRaises(server.PublicClientRequestError) as caught:
            server._build_public_api_payload("compare", ())

        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/compare"
        with patch.object(
            server.MarketMonitorHandler,
            "send_public_api",
            side_effect=caught.exception,
        ), patch.object(
            server.MarketMonitorHandler,
            "send_json",
        ) as send_json:
            handler.do_GET()

        send_json.assert_called_once_with(
            {"error": "token, market_a, and market_b are required"},
            400,
        )

    def test_screener_refresh_identity_is_canonical_and_retryability_is_server_owned(self):
        cex = server._compact_screener_market(
            {
                "token_symbol": "AAVE",
                "venue": "binance",
                "instrument": "AAVE/USDT",
                "depth_status": "collection_failed",
                "depth_reason_code": "network",
                "depth_observed_at": "2026-08-01T01:02:03+00:00",
            },
            "cex",
        )
        dex = server._compact_screener_market(
            {
                "token_symbol": "AAVE",
                "venue": "eth / uniswap_v3",
                "pool_address": "0xAbC",
                "tvl_status": "not_cataloged_in_snapshot",
                "tvl_observed_at": "2026-08-01T01:03:04+00:00",
                "dex_depth_status": "unsupported",
                "depth_error": "unsupported_protocol",
                "depth_observed_at": "2026-08-01T01:04:05+00:00",
            },
            "dex",
        )

        self.assertEqual(cex["market_id"], "binance|AAVE/USDT")
        self.assertEqual(
            cex["refresh_market_id"],
            "cex:binance:AAVE/USDT",
        )
        self.assertEqual(
            (
                cex["tvl_status"],
                cex["tvl_na_reason"],
                cex["tvl_retryable"],
            ),
            (
                "not_applicable",
                "cex_markets_do_not_have_pool_tvl",
                False,
            ),
        )
        self.assertEqual(
            (
                cex["depth_status"],
                cex["depth_na_reason"],
                cex["depth_retryable"],
            ),
            ("collection_failed", "network", True),
        )
        self.assertEqual(
            cex["depth_observed_at"],
            "2026-08-01T01:02:03+00:00",
        )
        self.assertEqual(dex["market_id"], "0xAbC")
        self.assertEqual(
            dex["refresh_market_id"],
            "dex:eth:uniswap_v3:0xabc:AAVE",
        )
        self.assertEqual(
            (
                dex["tvl_status"],
                dex["tvl_na_reason"],
                dex["tvl_retryable"],
            ),
            (
                "not_cataloged_in_snapshot",
                "tvl_market_not_cataloged_in_snapshot",
                True,
            ),
        )
        self.assertEqual(
            (dex["tvl_observed_at"], dex["depth_observed_at"]),
            (
                "2026-08-01T01:03:04+00:00",
                "2026-08-01T01:04:05+00:00",
            ),
        )
        self.assertEqual(
            (
                dex["depth_status"],
                dex["depth_na_reason"],
                dex["depth_retryable"],
            ),
            ("unsupported", "unsupported_protocol", False),
        )

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
        midpoint_gaps = [1 / 99.5, 2 / 101, 4 / 102]
        self.assertAlmostEqual(
            summary["latest_absolute_midpoint"],
            midpoint_gaps[-1],
        )
        self.assertAlmostEqual(summary["maximum_absolute"], max(midpoint_gaps))
        self.assertAlmostEqual(
            summary["mean_absolute"],
            sum(midpoint_gaps) / 3,
        )
        self.assertAlmostEqual(summary["median_absolute"], sorted(midpoint_gaps)[1])
        self.assertEqual(
            summary["absolute_method"],
            "symmetric_midpoint_relative_gap",
        )
        self.assertEqual(summary["comparable_days"], 3)

    def test_overlay_copy_shares_only_read_only_daily_series(self):
        payload = {
            "metadata": {"data_generation": "generation", "sources": []},
            "cex_markets": [
                {
                    "market_id": "cex:binance:BTC/USDT",
                    "price_points": [{"date": "2026-01-01", "price_usd": 100}],
                }
            ],
            "dex_pools": [],
            "tokens": [{"token_symbol": "BTC"}],
        }

        result = server._copy_payload_for_overlay(payload)

        self.assertEqual(result, payload)
        self.assertIsNot(result, payload)
        self.assertIsNot(result["metadata"], payload["metadata"])
        self.assertIsNot(result["cex_markets"][0], payload["cex_markets"][0])
        self.assertIs(
            result["cex_markets"][0]["price_points"],
            payload["cex_markets"][0]["price_points"],
        )

        result["metadata"]["data_generation"] = "changed"
        result["metadata"]["sources"].append({"path": "snapshot.csv"})
        result["cex_markets"][0]["market_id"] = "changed"

        self.assertEqual(payload["metadata"]["data_generation"], "generation")
        self.assertEqual(payload["metadata"]["sources"], [])
        self.assertEqual(
            payload["cex_markets"][0]["market_id"], "cex:binance:BTC/USDT"
        )

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

    def test_bounded_target_refresh_keeps_full_inventory_freshness_conservative(self):
        old_observed_at = "2026-07-01T00:00:00+00:00"
        refreshed_observed_at = "2026-08-01T11:30:00+00:00"

        tvl_columns = [
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
        ]
        write_csv(
            self.tvl_path,
            tvl_columns,
            [
                {
                    "snapshot_id": "tvl-merged-2",
                    "observed_at": refreshed_observed_at,
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "pool_address": "0xpool",
                    "tvl_usd": "5000",
                    "tvl_method": "fixture",
                    "source": "fixture",
                    "source_endpoint": "https://example.test/new",
                    "raw_response_sha256": "a" * 64,
                    "status": "observed",
                },
                {
                    "snapshot_id": "tvl-merged-2",
                    "observed_at": old_observed_at,
                    "token_symbol": "ETH",
                    "chain": "eth",
                    "pool_address": "0xoldpool",
                    "tvl_usd": "1000",
                    "tvl_method": "fixture",
                    "source": "fixture",
                    "source_endpoint": "https://example.test/old",
                    "raw_response_sha256": "b" * 64,
                    "status": "observed",
                },
            ],
        )

        def depth_row(columns, *, family, observed_at, identity):
            row = {field: "" for field in columns}
            row.update(
                {
                    "snapshot_id": "{}-merged-2".format(family),
                    "observed_at": observed_at,
                    "response_received_at": observed_at,
                    "status": "failed",
                    "error": "fixture failure",
                    **identity,
                }
            )
            return row

        write_csv(
            self.depth_path,
            DEPTH_COLUMNS_ALL,
            [
                depth_row(
                    DEPTH_COLUMNS_ALL,
                    family="cex",
                    observed_at=refreshed_observed_at,
                    identity={
                        "token_symbol": "BTC",
                        "exchange": "binance",
                        "cex_symbol": "BTC/USDT",
                    },
                ),
                depth_row(
                    DEPTH_COLUMNS_ALL,
                    family="cex",
                    observed_at=old_observed_at,
                    identity={
                        "token_symbol": "BTC",
                        "exchange": "okx",
                        "cex_symbol": "BTC/USDT",
                    },
                ),
            ],
        )
        write_csv(
            self.dex_depth_path,
            DEX_DEPTH_COLUMNS,
            [
                depth_row(
                    DEX_DEPTH_COLUMNS,
                    family="dex",
                    observed_at=refreshed_observed_at,
                    identity={
                        "token_symbol": "BTC",
                        "chain": "eth",
                        "dex": "uniswap",
                        "pool_address": "0xpool",
                    },
                ),
                depth_row(
                    DEX_DEPTH_COLUMNS,
                    family="dex",
                    observed_at=old_observed_at,
                    identity={
                        "token_symbol": "ETH",
                        "chain": "eth",
                        "dex": "uniswap",
                        "pool_address": "0xoldpool",
                    },
                ),
            ],
        )

        def stamp_execution(rows, observed_at):
            for row in rows:
                row["observed_at"] = observed_at
                row["request_started_at"] = observed_at
                row["response_received_at"] = observed_at
            return rows

        cex_execution_rows = stamp_execution(
            self.execution_rows(
                "cex:binance:BTC/USDT",
                "cex",
                state_observed_at=refreshed_observed_at,
            ),
            refreshed_observed_at,
        ) + stamp_execution(
            self.execution_rows(
                "cex:okx:BTC/USDT",
                "cex",
                state_observed_at=old_observed_at,
                exchange="okx",
                source_instrument="BTC-USDT",
            ),
            old_observed_at,
        )
        write_csv(
            self.cex_execution_path,
            EXECUTION_COST_COLUMNS,
            cex_execution_rows,
        )
        dex_execution_rows = stamp_execution(
            self.execution_rows(
                "dex:eth:uniswap:0xpool:BTC",
                "dex",
                state_observed_at=refreshed_observed_at,
            ),
            refreshed_observed_at,
        )
        old_dex_rows = self.execution_rows(
            "dex:eth:sushiswap:0xoldpool:BTC",
            "dex",
            state_observed_at=old_observed_at,
        )
        for row in old_dex_rows:
            row["dex"] = "sushiswap"
            row["pool_address"] = "0xoldpool"
        dex_execution_rows += stamp_execution(old_dex_rows, old_observed_at)
        write_csv(
            self.dex_execution_path,
            EXECUTION_COST_COLUMNS,
            dex_execution_rows,
        )

        server.clear_runtime_caches()
        try:
            tvl = server._load_tvl_snapshot_cached(
                str(self.tvl_path), server.data_signature([self.tvl_path])
            )
            cex_depth = server._load_cex_depth_snapshot_cached(
                str(self.depth_path), server.data_signature([self.depth_path])
            )
            dex_depth = server._load_dex_depth_snapshot_cached(
                str(self.dex_depth_path),
                server.data_signature([self.dex_depth_path]),
            )
            cex_execution = server.load_execution_cost_snapshot(
                self.cex_execution_path
            )
            dex_execution = server.load_execution_cost_snapshot(
                self.dex_execution_path
            )

            for snapshot in (tvl, cex_depth, dex_depth):
                with self.subTest(snapshot=snapshot["path"].name):
                    self.assertEqual(snapshot["observed_at"], old_observed_at)
                    self.assertEqual(
                        snapshot["observed_at_min"], old_observed_at
                    )
                    self.assertEqual(
                        snapshot["observed_at_max"], refreshed_observed_at
                    )
            for snapshot in (cex_execution, dex_execution):
                with self.subTest(snapshot=snapshot["path"].name):
                    self.assertEqual(snapshot["observed_at"], old_observed_at)
                    self.assertEqual(
                        snapshot["observed_at_min"], old_observed_at
                    )
                    self.assertEqual(
                        snapshot["observed_at_max"], refreshed_observed_at
                    )
                    self.assertEqual(
                        snapshot["state_observed_at"], old_observed_at
                    )
                    self.assertEqual(
                        snapshot["state_observed_at_min"], old_observed_at
                    )
                    self.assertEqual(
                        snapshot["state_observed_at_max"],
                        refreshed_observed_at,
                    )

            cex_environment = {
                **self.environment,
                "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
            }
            with patch.dict(
                server.os.environ, cex_environment, clear=True
            ), patch(
                "dashboard.freshness.utc_now",
                return_value=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            ):
                market_payload = server.build_market_payload()
                catalog = server.build_market_catalog()
            cex_by_venue = {
                market["venue"]: market
                for market in market_payload["cex_markets"]
            }
            self.assertEqual(
                cex_by_venue["binance"]["depth_observed_at"],
                refreshed_observed_at,
            )
            self.assertEqual(
                cex_by_venue["okx"]["depth_observed_at"],
                old_observed_at,
            )
            self.assertEqual(
                market_payload["metadata"]["freshness"]["cex_depth"][
                    "status"
                ],
                "stale",
            )
            self.assertEqual(
                catalog["metadata"]["cex_depth_snapshot"][
                    "observed_at_min"
                ],
                old_observed_at,
            )
            self.assertEqual(
                catalog["metadata"]["cex_depth_snapshot"][
                    "observed_at_max"
                ],
                refreshed_observed_at,
            )

            payload = {
                "metadata": {
                    "source_date_ranges": {},
                    "tvl_snapshot": tvl,
                    "cex_depth_snapshot": cex_depth,
                    "dex_depth_snapshot": dex_depth,
                }
            }
            with patch.object(
                server,
                "resolve_cex_execution_cost_path",
                return_value=self.cex_execution_path,
            ), patch.object(
                server,
                "resolve_dex_execution_cost_path",
                return_value=self.dex_execution_path,
            ), patch(
                "dashboard.freshness.utc_now",
                return_value=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            ):
                freshness = server.attach_freshness_metadata(payload)[
                    "metadata"
                ]["freshness"]
            for family in (
                "dex_tvl",
                "cex_depth",
                "dex_depth",
                "cex_execution",
                "dex_execution",
            ):
                with self.subTest(family=family):
                    self.assertEqual(freshness[family]["status"], "stale")
                    self.assertEqual(
                        freshness[family]["observed_at"], old_observed_at
                    )
        finally:
            server.clear_runtime_caches()

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
            "https://example.test",
        )
        self.assertEqual(
            catalog_pool["tvl_raw_response_sha256"],
            "abc123",
        )

    def test_tvl_loader_rejects_missing_or_noncanonical_observation_time(self):
        fieldnames = [
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
        ]
        base_row = {
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
        for invalid_time in (
            "",
            "2026-07-27T01:02:03",
            "2026-07-27T09:02:03+08:00",
            "not-a-time",
        ):
            with self.subTest(observed_at=invalid_time):
                write_csv(
                    self.tvl_path,
                    fieldnames,
                    [{**base_row, "observed_at": invalid_time}],
                )
                server._load_tvl_snapshot_cached.cache_clear()
                with self.assertRaises(ValueError):
                    server._load_tvl_snapshot_cached(
                        str(self.tvl_path),
                        server.data_signature([self.tvl_path]),
                    )

    def test_tvl_loader_rejects_status_value_contradictions_and_keeps_zero(self):
        fieldnames = [
            "snapshot_id", "observed_at", "token_symbol", "chain",
            "pool_address", "tvl_usd", "tvl_method", "source",
            "source_endpoint", "raw_response_sha256", "status",
        ]
        base_row = {
            "snapshot_id": "tvl-snapshot-1",
            "observed_at": "2026-07-27T01:02:03+00:00",
            "token_symbol": "BTC",
            "chain": "eth",
            "pool_address": "0xpool",
            "tvl_usd": "1",
            "tvl_method": "geckoterminal_reserve_in_usd",
            "source": "GeckoTerminal API v2",
            "source_endpoint": "https://example.test/pool",
            "raw_response_sha256": "abc123",
            "status": "observed",
        }
        invalid_rows = (
            {**base_row, "tvl_usd": ""},
            {**base_row, "status": "missing", "tvl_usd": "123"},
        )
        for row in invalid_rows:
            with self.subTest(status=row["status"], tvl_usd=row["tvl_usd"]):
                write_csv(self.tvl_path, fieldnames, [row])
                server._load_tvl_snapshot_cached.cache_clear()
                with self.assertRaisesRegex(ValueError, "TVL"):
                    server._load_tvl_snapshot_cached(
                        str(self.tvl_path),
                        server.data_signature([self.tvl_path]),
                    )

        write_csv(self.tvl_path, fieldnames, [{**base_row, "tvl_usd": "0"}])
        server._load_tvl_snapshot_cached.cache_clear()
        snapshot = server._load_tvl_snapshot_cached(
            str(self.tvl_path),
            server.data_signature([self.tvl_path]),
        )
        self.assertEqual(snapshot["rows"][("BTC", "eth", "0xpool")]["tvl_usd"], "0")

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
            "https://example.test",
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

        self.assertEqual(catalog["metadata"]["catalog_version"], 3)
        self.assertEqual(catalog["metadata"]["time_grain"], "1 day, UTC")
        self.assertEqual(catalog["metadata"]["price_quote_asset"], "USD")
        self.assertIn("not order-book depth", catalog["metadata"]["semantic_boundary"])
        normalization_note = catalog["metadata"]["cex_normalization_note"]
        self.assertIn("exact venue market identity", normalization_note)
        self.assertIn("USD, USDT, and KRW", normalization_note)
        self.assertIn("never relabeled", normalization_note)
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

    def test_selected_quality_producer_satisfies_release_contract(self):
        market_a = "cex:binance:BTC/USDT"
        market_b = "cex:okx:BTC/USDT"
        with patch.dict(server.os.environ, self.environment, clear=True):
            quality = server.build_market_quality(
                "BTC",
                "selected",
                market_a,
                market_b,
                "2026-01-01",
                "2026-01-02",
            )

        validate_quality(
            quality,
            token="BTC",
            market_a=market_a,
            market_b=market_b,
            expected_generation=quality["metadata"]["data_generation"],
        )

    def test_empty_cex_book_quality_producer_satisfies_v4_flag_contract(self):
        depth_row = {field: "" for field in DEPTH_COLUMNS_ALL}
        depth_row.update(
            {
                "snapshot_id": "empty-book-snapshot",
                "observed_at": "2026-07-27T02:03:04+00:00",
                "response_received_at": "2026-07-27T02:03:05+00:00",
                "token_symbol": "BTC",
                "exchange": "binance",
                "cex_symbol": "BTC/USDT",
                "source_instrument": "BTCUSDT",
                "source_quote_asset": "USDT",
                "quote_conversion_method": "USDT=USD proxy",
                "depth_method": "midpoint_symmetric_quote_notional",
                "source_endpoint": "https://example.test/depth",
                "raw_response_sha256": "d" * 64,
                "status": "failed",
                "reason_code": "source_no_two_sided_book",
            }
        )
        write_csv(self.depth_path, DEPTH_COLUMNS_ALL, [depth_row])
        environment = {
            **self.environment,
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
        }
        market_a = "cex:binance:BTC/USDT"
        market_b = "cex:okx:BTC/USDT"

        with patch.dict(server.os.environ, environment, clear=True):
            quality = server.build_market_quality(
                "BTC",
                "selected",
                market_a,
                market_b,
                "2026-01-01",
                "2026-01-02",
            )

        binance = next(
            market
            for market in quality["markets"]
            if market["market_id"] == market_a
        )
        depth_fact = binance["facts"]["depth"]
        self.assertEqual(depth_fact["status"], "source_no_observation")
        source_flag = next(
            flag
            for flag in depth_fact["quality_flags"]
            if flag["code"] == "depth_source_no_observation"
        )
        self.assertEqual(
            set(source_flag),
            {
                "code",
                "severity",
                "category",
                "message",
                "observed_value",
                "threshold",
            },
        )
        self.assertIsNone(source_flag["observed_value"])
        self.assertIsNone(source_flag["threshold"])
        validate_quality(
            quality,
            token="BTC",
            market_a=market_a,
            market_b=market_b,
            expected_generation=quality["metadata"]["data_generation"],
        )

    def test_fail_closed_quality_flag_has_stable_v4_shape(self):
        fact = server._validated_public_quality_fact(
            {
                "status": "unknown_status",
                "reason_code": "unknown_reason",
                "retryable": True,
                "quality_flags": [],
            }
        )

        flag = fact["quality_flags"][0]
        self.assertEqual(
            set(flag),
            {
                "code",
                "severity",
                "category",
                "message",
                "observed_value",
                "threshold",
            },
        )
        self.assertIsNone(flag["observed_value"])
        self.assertIsNone(flag["threshold"])

    def test_compare_and_execution_producers_share_catalog_generation(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            source_signature = server.api_source_signature()
            catalog = server.build_market_catalog(
                source_signature=source_signature,
            )
            comparison = server.build_market_comparison(
                "BTC",
                "cex:binance:BTC/USDT",
                "dex:eth:uniswap:0xpool:BTC",
                "2026-01-01",
                "2026-01-02",
                source_signature=source_signature,
            )
            execution = server.build_execution_cost_comparison(
                "BTC",
                "cex:binance:BTC/USDT",
                "dex:eth:uniswap:0xpool:BTC",
                source_signature=source_signature,
            )

        self.assertEqual(
            comparison["metadata"]["data_generation"],
            catalog["metadata"]["data_generation"],
        )
        self.assertEqual(
            execution["metadata"]["data_generation"],
            catalog["metadata"]["data_generation"],
        )

    def test_summary_producer_satisfies_structured_na_release_contract(self):
        checked_at = "2026-08-01T06:00:00+00:00"
        daily = {
            "status": "current",
            "available_start": "2026-07-01",
            "available_end": "2026-07-31",
            "latest_completed_utc_day": "2026-07-31",
            "lag_days": 0,
            "max_lag_days": 1,
        }
        current_freshness = {
            "checked_at": checked_at,
            "overall_status": "current",
            "common_comparable_end": "2026-07-31",
            "cex_daily": {"source": "cex_daily", **daily},
            "dex_daily": {"source": "dex_daily", **daily},
            **{
                source: {
                    "source": source,
                    "status": "current",
                    "observed_at": "2026-08-01T05:00:00+00:00",
                    "age_hours": 1.0,
                    "max_age_hours": maximum,
                }
                for source, maximum in (
                    ("dex_tvl", 26.0),
                    ("cex_depth", 2.0),
                    ("dex_depth", 2.0),
                    ("cex_execution", 2.0),
                    ("dex_execution", 2.0),
                )
            },
        }
        with patch.dict(
            server.os.environ,
            self.environment,
            clear=True,
        ), patch.object(
            server,
            "build_source_freshness",
            return_value=current_freshness,
        ):
            summary = server.build_market_summary()

        validate_summary(
            summary,
            ResponseMetrics(
                "/api/markets/summary",
                0.0,
                1,
                1,
                True,
            ),
            raw_max=10_000_000,
            gzip_max=10_000_000,
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
        self.assertEqual(summary["metadata"]["summary_version"], 3)
        self.assertTrue(summary["metadata"]["data_generation"])
        self.assertNotIn("markets", summary)
        self.assertNotIn("cex_markets", summary)
        self.assertNotIn("dex_pools", summary)
        self.assertEqual(len(summary["tokens"]), 1)
        self.assertEqual(
            summary["metadata"]["data_generation"],
            token_catalog["metadata"]["data_generation"],
        )
        self.assertIn(
            "configured_cex_market_identities",
            summary["metadata"],
        )
        self.assertIn(
            "configured_cex_market_identities",
            token_catalog["metadata"],
        )
        self.assertEqual(
            summary["metadata"]["configured_cex_market_identities"],
            token_catalog["metadata"]["configured_cex_market_identities"],
        )

        compact = summary["tokens"][0]
        original = full_payload["tokens"][0]
        for field in (
            "aggregate_cex_volume_usd",
            "aggregate_dex_volume_usd",
            "aggregate_volume_usd",
            "aggregate_dex_volume_share",
            "price_spread",
            "price_spread_method",
            "absolute_price_gap",
            "absolute_price_gap_method",
            "spread_date",
            "maximum_absolute_price_spread",
            "mean_absolute_price_spread",
            "median_absolute_price_spread",
            "spread_comparable_days",
        ):
            self.assertEqual(compact[field], original[field], field)
        self.assertNotAlmostEqual(
            compact["absolute_price_gap"],
            abs(compact["price_spread"]),
        )
        self.assertEqual(
            compact["absolute_price_gap_method"],
            "symmetric_midpoint_relative_gap",
        )
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
        self.assertEqual(
            (
                compact["primary_cex"]["tvl_status"],
                compact["primary_cex"]["tvl_na_reason"],
                compact["primary_cex"]["tvl_retryable"],
            ),
            (
                "not_applicable",
                "cex_markets_do_not_have_pool_tvl",
                False,
            ),
        )
        self.assertNotIn("price_points", compact["primary_cex"])
        self.assertNotIn("price_points", compact["primary_dex"])
        for primary in (compact["primary_cex"], compact["primary_dex"]):
            for catalog_only_field in (
                "observation_count",
                "quality_flag_details",
                "current_listing_status",
                "current_listing_reason_code",
                "current_listing_checked_at",
            ):
                self.assertNotIn(catalog_only_field, primary)
            self.assertNotIn(
                "requested_window_days",
                primary,
            )
        self.assertNotIn("tvl_usd", compact["primary_cex"])
        for primary in (compact["primary_cex"], compact["primary_dex"]):
            self.assertIn("refresh_market_id", primary)
            self.assertIsInstance(primary["depth_retryable"], bool)
            for fact_name in ("tvl", "depth"):
                rule = quality_outcome_rule(
                    primary[f"{fact_name}_status"],
                    primary[f"{fact_name}_na_reason"],
                )
                self.assertIsNotNone(rule)
                self.assertIs(
                    primary[f"{fact_name}_retryable"],
                    rule.retryable,
                )
            for field in (
                "first_observed_date",
                "latest_observed_date",
                "coverage_ratio",
                "depth_status",
                "quality_flags",
            ):
                self.assertIn(field, primary)
            for field in (
                "total_depth_10bps_usd",
                "total_depth_25bps_usd",
                "total_depth_50bps_usd",
                "total_depth_100bps_usd",
            ):
                if field in primary:
                    self.assertIsNotNone(primary[field])

    def test_screener_quality_counts_match_screening_projection_for_every_token(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            summary = server.build_market_summary(
                start="2026-01-01",
                end="2026-01-02",
            )
            for token_row in summary["tokens"]:
                quality = server.build_market_quality(
                    token_row["token_symbol"],
                    start="2026-01-01",
                    end="2026-01-02",
                )
                screening_statuses = Counter(
                    market["screening_quality_status"]
                    for market in quality["markets"]
                )
                screening_alerts = Counter(
                    flag["severity"]
                    for market in quality["markets"]
                    for flag in market["screening_quality_flags"]
                )
                self.assertEqual(
                    dict(screening_statuses),
                    token_row["quality_status_counts"],
                )
                self.assertEqual(
                    dict(screening_alerts),
                    token_row["quality_alert_counts"],
                )

    def test_empty_catalog_warning_projects_one_shared_bounded_fallback(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            catalog = server.build_market_catalog()
            fallback_market = {
                **catalog["markets"][0],
                "quality_status": "warning",
                "quality_flag_details": [],
            }
            fallback_catalog = {
                **catalog,
                "markets": [fallback_market, *catalog["markets"][1:]],
            }
            summary = server.catalog_summary_from_catalog(fallback_catalog)
            with patch.object(server, "build_market_catalog", return_value=fallback_catalog):
                quality = server.build_market_quality(
                    "BTC",
                    start="2026-01-01",
                    end="2026-01-02",
                )

        token_summary = summary["token_summaries"][0]
        fallback_quality = next(
            market
            for market in quality["markets"]
            if market["market_id"] == fallback_market["market_id"]
        )
        self.assertEqual(token_summary["quality_alert_counts"]["warning"], 1)
        self.assertEqual(fallback_quality["screening_quality_status"], "warning")
        self.assertEqual(len(fallback_quality["screening_quality_flags"]), 1)
        self.assertEqual(
            fallback_quality["screening_quality_flags"][0]["code"],
            "catalog_quality_status",
        )

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

    def test_single_token_catalog_includes_bounded_screener_projection(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            catalog = server.build_token_market_catalog("BTC")

        market = next(
            item
            for item in catalog["markets"]
            if item["market_id"] == "cex:binance:BTC/USDT"
        )
        self.assertIn("screening_quality_status", market)
        self.assertIn("screening_quality_flags", market)
        self.assertEqual(market["screening_quality_status"], "ok")
        self.assertEqual(
            [flag["code"] for flag in market["screening_quality_flags"]],
            ["depth_unavailable"],
        )
        serialized = json.dumps(catalog)
        self.assertNotIn("screening_quality_source", serialized)
        self.assertNotIn(str(self.temporary_directory.name), serialized)

    def test_screening_projection_preserves_measurement_and_evaluation_window(self):
        projection = server.screening_quality_projection(
            {
                "quality_status": "warning",
                "coverage_expected_start": "2026-01-16",
                "coverage_expected_end": "2026-07-30",
                "coverage_start_method": "max_query_source_market_observed_start",
                "quality_flag_details": [
                    {
                        "code": "low_daily_coverage",
                        "severity": "warning",
                        "category": "data_health",
                        "message": "Internal message must not leak.",
                        "observed_value": 0.750958,
                        "threshold": 0.8,
                    }
                ],
            }
        )

        self.assertEqual(projection["scope"], "catalog")
        self.assertEqual(
            projection["evaluation_window"],
            {
                "start": "2026-01-16",
                "end": "2026-07-30",
                "method": "max_query_source_market_observed_start",
            },
        )
        self.assertEqual(projection["flags"][0]["observed_value"], 0.750958)
        self.assertEqual(projection["flags"][0]["threshold"], 0.8)

    def test_inactive_current_cex_instrument_withholds_stale_facts_and_ranking(self):
        market = {
            "token_symbol": "GMX",
            "market": "cex",
            "venue": "crypto_com",
            "instrument": "GMX/USDT",
            "price_usd": 10.0,
            "volume_usd": 0.0,
            "window_return": 0.0,
            "daily_volatility": 0.0,
            "coverage_ratio": 1.0,
            "observation_count": 30,
            "observation_days": 30,
            "price_points": [{"date": "2026-07-30", "price_usd": 10.0}],
            "depth_status": "observed",
            "depth_observed_at": "2026-07-31T23:59:00+00:00",
            "best_bid": 9.9,
            "best_ask": 10.1,
            "spread_bps": 200.0,
            "bid_depth_10bps_usd": 100.0,
            "ask_depth_10bps_usd": 120.0,
            "total_depth_10bps_usd": 220.0,
            "bid_depth_100bps_usd": 1000.0,
            "ask_depth_100bps_usd": 1200.0,
            "total_depth_100bps_usd": 2200.0,
            "depth_10bps_complete": True,
            "depth_100bps_complete": True,
            "depth_snapshot_id": "old-depth",
        }
        payload = {
            "metadata": {},
            "cex_markets": [market],
            "dex_pools": [],
        }
        review = {
            "market_id": "cex:crypto_com:GMX/USDT",
            "token_symbol": "GMX",
            "exchange": "crypto_com",
            "instrument": "GMX/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "reason_code": "instrument_absent_from_current_catalog",
            "checked_at_utc": "2026-08-01T03:10:02+00:00",
            "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
            "response_sha256": "9" * 64,
        }

        result = server.overlay_cex_instrument_lifecycle(
            payload,
            {review["market_id"]: review},
            now=datetime(2026, 8, 1, 4, 0, tzinfo=timezone.utc),
        )
        quarantined = result["cex_markets"][0]

        for field in (
            "price_usd",
            "volume_usd",
            "window_return",
            "daily_volatility",
            "coverage_ratio",
        ):
            self.assertIsNone(quarantined[field])
        self.assertEqual(quarantined["price_points"], [])
        for field in (
            "best_bid", "best_ask", "spread_bps",
            "bid_depth_10bps_usd", "ask_depth_10bps_usd",
            "total_depth_10bps_usd", "bid_depth_100bps_usd",
            "ask_depth_100bps_usd", "total_depth_100bps_usd",
        ):
            self.assertIsNone(quarantined[field], field)
        self.assertFalse(quarantined["depth_10bps_complete"])
        self.assertFalse(quarantined["depth_100bps_complete"])
        self.assertEqual(quarantined["depth_status"], "source_no_observation")
        self.assertEqual(
            quarantined["depth_reason_code"],
            "instrument_absent_from_current_catalog",
        )
        self.assertIsNone(quarantined["depth_snapshot_id"])
        self.assertEqual(quarantined["historical_observation_count"], 30)
        self.assertEqual(
            quarantined["current_listing_status"],
            "absent_from_official_current_catalog",
        )
        finalized = server.finalize_fact_contract(result)
        self.assertIsNone(finalized["tokens"][0]["aggregate_cex_volume_usd"])
        self.assertIsNone(finalized["tokens"][0]["primary_cex_id"])
        self.assertEqual(finalized["cex_markets"][0]["quality_status"], "critical")
        self.assertIn(
            "inactive_cex_instrument",
            finalized["cex_markets"][0]["quality_flags"],
        )
        depth_fact = server._depth_quality_fact({
            **finalized["cex_markets"][0],
            "market_type": "cex",
        })
        self.assertEqual(depth_fact["status"], "source_no_observation")
        self.assertFalse(depth_fact["retryable"])
        self.assertIsNone(depth_fact["bands_bps"]["10"]["total_usd"])

    def test_stale_lifecycle_evidence_stays_withheld_but_no_longer_claims_current_absence(self):
        payload = {
            "metadata": {},
            "cex_markets": [{
                "token_symbol": "GMX",
                "market": "cex",
                "venue": "crypto_com",
                "instrument": "GMX/USDT",
                "price_usd": 10.0,
                "volume_usd": 100.0,
                "price_points": [{"date": "2026-07-30", "price_usd": 10.0}],
            }],
            "dex_pools": [],
        }
        review = {
            "market_id": "cex:crypto_com:GMX/USDT",
            "token_symbol": "GMX",
            "exchange": "crypto_com",
            "instrument": "GMX/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "reason_code": "instrument_absent_from_current_catalog",
            "checked_at_utc": "2026-07-29T00:00:00+00:00",
            "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
            "response_sha256": "9" * 64,
        }

        result = server.overlay_cex_instrument_lifecycle(
            payload,
            {review["market_id"]: review},
            now=datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc),
        )
        market = result["cex_markets"][0]
        self.assertIsNone(market["price_usd"])
        self.assertEqual(
            market["current_listing_status"],
            "official_catalog_evidence_stale",
        )
        self.assertEqual(
            market["current_listing_reason_code"],
            "official_catalog_evidence_stale",
        )
        fact = server._daily_quality_fact(
            {**market, "market_id": review["market_id"], "market_type": "cex"},
            {"window_start": "2026-07-01", "window_end": "2026-07-30"},
        )
        self.assertEqual(fact["status"], "needs_review")
        self.assertEqual(fact["reason_code"], "official_catalog_evidence_stale")
        self.assertTrue(any(
            flag["code"] == "stale_cex_lifecycle_evidence"
            and flag["severity"] == "critical"
            for flag in fact["quality_flags"]
        ))

    def test_zero_absence_lifecycle_still_projects_catalog_wide_root_evidence(self):
        payload = {"metadata": {}, "cex_markets": [], "dex_pools": []}
        evidence = {
            "checked_at_utc": "2026-08-01T03:10:02+00:00",
            "response_sha256": "9" * 64,
            "inventory_count": 1_000,
            "configured_market_count": 30,
        }

        result = server.overlay_cex_instrument_lifecycle(
            payload,
            {},
            lifecycle_evidence=evidence,
            now=datetime(2026, 8, 1, 4, 0, tzinfo=timezone.utc),
        )

        lifecycle = result["metadata"]["cex_instrument_lifecycle"]
        self.assertEqual(lifecycle["reviewed_market_count"], 30)
        self.assertEqual(lifecycle["absence_market_count"], 0)
        self.assertEqual(lifecycle["checked_at_min"], evidence["checked_at_utc"])
        self.assertEqual(lifecycle["checked_at_max"], evidence["checked_at_utc"])
        self.assertEqual(lifecycle["official_inventory_count"], 1_000)
        self.assertEqual(lifecycle["response_sha256"], "9" * 64)
        self.assertEqual(lifecycle["stale_evidence_market_count"], 0)

    def test_lifecycle_withheld_fact_flags_are_identical_across_fact_families(self):
        for listing_status, expected_code in (
            ("absent_from_official_current_catalog", "inactive_cex_instrument"),
            ("official_catalog_evidence_stale", "stale_cex_lifecycle_evidence"),
        ):
            with self.subTest(listing_status=listing_status):
                market = {
                    "market_id": "cex:crypto_com:GMX/USDT",
                    "market_type": "cex",
                    "current_listing_status": listing_status,
                    "current_listing_checked_at": "2026-08-01T00:00:00+00:00",
                    "current_listing_source": (
                        "https://api.crypto.com/exchange/v1/public/get-instruments"
                    ),
                    "current_listing_response_sha256": "9" * 64,
                }

                daily = server._daily_quality_fact(
                    market,
                    {"window_start": "2026-07-01", "window_end": "2026-07-30"},
                )
                execution = server._execution_quality_fact(
                    market,
                    {"snapshot": {}, "error_code": None},
                )
                daily_flag = next(
                    flag
                    for flag in daily["quality_flags"]
                    if flag["code"] == expected_code
                )
                execution_flag = next(
                    flag
                    for flag in execution["quality_flags"]
                    if flag["code"] == expected_code
                )
                selected_flag = next(
                    flag
                    for flag in server.enrich_market_quality(market)[
                        "quality_flag_details"
                    ]
                    if flag["code"] == expected_code
                )

                self.assertEqual(daily_flag, execution_flag)
                self.assertEqual(daily_flag, selected_flag)

    def test_configured_upbit_identity_metadata_is_hash_bound_and_propagates_to_catalog(self):
        attach = getattr(
            server,
            "attach_configured_cex_identity_metadata",
            None,
        )
        self.assertIsNotNone(
            attach,
            "configured CEX identity metadata attachment is missing",
        )
        payload = {
            "metadata": {
                "available_start": "2026-01-01",
                "available_end": "2026-01-02",
                "sources": [],
                "storage": {"engine": "test"},
            },
            "cex_markets": [],
            "dex_pools": [],
        }
        with patch.object(
            server,
            "current_configured_upbit_market_ids",
            return_value=(
                "cex:upbit:AAVE/KRW",
                "cex:upbit:UNI/USDT",
            ),
        ):
            attached = attach(payload)

        expected = {
            "schema": "configured_cex_market_identities/v1",
            "upbit": {
                "market_count": 2,
                "market_ids": [
                    "cex:upbit:AAVE/KRW",
                    "cex:upbit:UNI/USDT",
                ],
                "market_ids_sha256": (
                    "440b52cffc9da70c7adaf402da4131c48"
                    "1e4356cb85ef9994da59d0a2f1f9154"
                ),
            },
        }
        self.assertEqual(
            attached["metadata"]["configured_cex_market_identities"],
            expected,
        )
        self.assertNotIn(
            "configured_cex_market_identities",
            payload["metadata"],
        )
        catalog = market_facts.catalog_from_market_payload(attached)
        self.assertEqual(
            catalog["metadata"]["configured_cex_market_identities"],
            expected,
        )

    def test_stale_root_lifecycle_withholds_present_crypto_com_market(self):
        payload = {
            "metadata": {},
            "cex_markets": [{
                "token_symbol": "AAVE",
                "venue": "crypto_com",
                "instrument": "AAVE/USDT",
                "price_usd": 123.0,
                "volume_usd": 456.0,
                "coverage_ratio": 1.0,
                "price_points": [
                    {"date": "2026-07-30", "price_usd": 123.0}
                ],
                "best_bid": 122.0,
                "best_ask": 124.0,
                "depth_status": "observed",
            }],
            "dex_pools": [],
        }
        evidence = {
            "checked_at_utc": "2026-07-29T00:00:00+00:00",
            "response_sha256": "9" * 64,
            "inventory_count": 1_000,
            "configured_market_count": 1,
            "configured_market_ids_sha256": server.configured_market_ids_sha256(
                ["cex:crypto_com:AAVE/USDT"]
            ),
        }

        result = server.overlay_cex_instrument_lifecycle(
            payload,
            {},
            lifecycle_evidence=evidence,
            now=datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc),
        )

        market = result["cex_markets"][0]
        self.assertIsNone(market["price_usd"])
        self.assertIsNone(market["volume_usd"])
        self.assertEqual(market["price_points"], [])
        self.assertIsNone(market["best_bid"])
        self.assertIsNone(market["best_ask"])
        self.assertEqual(
            market["current_listing_status"],
            "official_catalog_evidence_stale",
        )
        self.assertEqual(
            market["current_listing_reason_code"],
            "official_catalog_evidence_stale",
        )
        self.assertEqual(market["depth_status"], "needs_review")
        self.assertEqual(
            result["metadata"]["cex_instrument_lifecycle"][
                "withheld_payload_market_count"
            ],
            1,
        )

    def test_lifecycle_manifest_is_rejected_after_same_size_catalog_identity_change(self):
        root = Path(self.temporary_directory.name) / "lifecycle-binding"
        root.mkdir()
        tokens_path = root / "tokens.csv"
        registry_path = root / "token_registry.json"
        manifest_path = root / "cex_instrument_lifecycle.json"

        def write_tokens(first_symbol):
            write_csv(
                tokens_path,
                ["token_symbol", "cex_symbol"],
                [
                    {
                        "token_symbol": first_symbol,
                        "cex_symbol": first_symbol + "/USDT",
                    },
                    {"token_symbol": "GMX", "cex_symbol": "GMX/USDT"},
                ],
            )

        write_tokens("AAVE")
        review = {
            "market_id": "cex:crypto_com:GMX/USDT",
            "market_type": "cex",
            "token_symbol": "GMX",
            "exchange": "crypto_com",
            "instrument": "GMX/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "reason_code": "instrument_absent_from_current_catalog",
            "checked_at_utc": "2026-08-01T03:10:02+00:00",
            "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
            "http_status": 200,
            "response_sha256": "9" * 64,
            "inventory_count": 1_000,
            "instrument_present": False,
        }
        manifest_path.write_text(
            json.dumps(
                {
                    "schema": "cex_instrument_lifecycle/v1",
                    "generated_at_utc": review["checked_at_utc"],
                    "checked_at_utc": review["checked_at_utc"],
                    "response_sha256": review["response_sha256"],
                    "inventory_count": review["inventory_count"],
                    "configured_market_count": 2,
                    "configured_market_ids_sha256": (
                        "2ac2c0098299d53cc739440dae18d8a422e26054c429ed506ea4fa954c8695a9"
                    ),
                    "review_count": 1,
                    "reviews": [review],
                }
            ),
            encoding="utf-8",
        )
        payload = {
            "metadata": {},
            "cex_markets": [
                {
                    "token_symbol": "GMX",
                    "market": "cex",
                    "venue": "crypto_com",
                    "instrument": "GMX/USDT",
                }
            ],
            "dex_pools": [],
        }

        server._load_cex_instrument_lifecycle_cached.cache_clear()
        with patch.object(
            server,
            "CEX_LIFECYCLE_TOKEN_CONFIG_PATH",
            tokens_path,
            create=True,
        ), patch.dict(
            "os.environ",
            {"TOKEN_REGISTRY_PATH": str(registry_path)},
            clear=False,
        ), patch.object(
            server,
            "resolve_cex_instrument_lifecycle_path",
            return_value=manifest_path,
        ):
            server.overlay_cex_instrument_lifecycle(
                payload,
                now=datetime(2026, 8, 1, 4, 0, tzinfo=timezone.utc),
            )
            write_tokens("RAY")
            with self.assertRaisesRegex(ValueError, "configured market.*hash"):
                server.overlay_cex_instrument_lifecycle(
                    payload,
                    now=datetime(2026, 8, 1, 4, 0, tzinfo=timezone.utc),
                )

    def test_lifecycle_manifest_applies_catalog_policy_when_window_has_no_row(self):
        review = {
            "market_id": "cex:crypto_com:GMX/USDT",
            "token_symbol": "GMX",
            "exchange": "crypto_com",
            "instrument": "GMX/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "reason_code": "instrument_absent_from_current_catalog",
            "checked_at_utc": "2026-08-01T03:10:02+00:00",
            "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
            "response_sha256": "9" * 64,
        }
        evidence = {
            "checked_at_utc": review["checked_at_utc"],
            "response_sha256": review["response_sha256"],
            "inventory_count": 1_000,
            "configured_market_count": 1,
        }

        result = server.overlay_cex_instrument_lifecycle(
            {"metadata": {}, "cex_markets": [], "dex_pools": []},
            {review["market_id"]: review},
            lifecycle_evidence=evidence,
            now=datetime(2026, 8, 1, 4, 0, tzinfo=timezone.utc),
        )

        lifecycle = result["metadata"]["cex_instrument_lifecycle"]
        self.assertEqual(lifecycle["absence_market_count"], 1)
        self.assertEqual(lifecycle["applied_market_count"], 1)
        self.assertEqual(lifecycle["withheld_payload_market_count"], 0)

    def test_lifecycle_projection_cache_reclassifies_unchanged_evidence_after_ttl(self):
        payload = {"metadata": {}, "cex_markets": [], "dex_pools": []}
        calls = []

        def project(source_payload, *, now=None):
            calls.append(now)
            return {
                **source_payload,
                "lifecycle_projection": (
                    "fresh" if now.timestamp() < 10_000 else "stale"
                ),
            }

        server._build_lifecycle_payload_cached.cache_clear()
        with patch.object(
            server,
            "_build_enriched_payload_cached",
            return_value=payload,
        ), patch.object(
            server,
            "overlay_cex_instrument_lifecycle",
            side_effect=project,
        ), patch.object(
            server,
            "finalize_fact_contract",
            side_effect=lambda value: value,
        ):
            fresh = server._build_lifecycle_payload_cached(("same",), 100)
            stale = server._build_lifecycle_payload_cached(("same",), 200)

        self.assertEqual(fresh["lifecycle_projection"], "fresh")
        self.assertEqual(stale["lifecycle_projection"], "stale")
        self.assertEqual(
            [int(value.timestamp()) for value in calls],
            [6_000, 12_000],
        )

    def test_inactive_current_cex_instrument_has_terminal_daily_na_and_no_rows(self):
        market = {
            "market_id": "cex:crypto_com:GMX/USDT",
            "market_type": "cex",
            "token_symbol": "GMX",
            "venue": "crypto_com",
            "instrument": "GMX/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "current_listing_reason_code": "instrument_absent_from_current_catalog",
            "current_listing_checked_at": "2026-08-01T03:10:02+00:00",
            "window_metrics": {
                "observation_count": 30,
                "requested_window_days": 30,
                "coverage_ratio": 1.0,
            },
        }
        metadata = {
            "window_start": "2026-07-01",
            "window_end": "2026-07-30",
        }

        fact = server._daily_quality_fact(market, metadata)
        with patch.object(server, "database_market_rows") as database_rows, patch.object(
            server,
            "resolve_database_path",
            return_value=Path("facts.sqlite3"),
        ):
            rows = server.selected_market_rows(
                market,
                "2026-07-01",
                "2026-07-30",
            )

        database_rows.assert_not_called()
        self.assertEqual(rows, [])
        self.assertEqual(fact["status"], "source_no_observation")
        self.assertEqual(
            fact["reason_code"],
            "instrument_absent_from_current_catalog",
        )
        self.assertFalse(fact["retryable"])
        self.assertIsNone(fact["action"])

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
            "catalog_version": 3,
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
        with patch.object(server, "CATALOG_SUMMARY_VERSION", 4):
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
        self.write_cex_depth_cohort([{"exchange": "binance"}])
        self.write_dex_depth_cohort(
            [
                {
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                }
            ]
        )
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
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
            "MARKET_DEX_DEPTH_DATA": str(self.dex_depth_path),
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
        self.write_cex_depth_cohort([{"exchange": "binance"}])
        self.write_dex_depth_cohort(
            [
                {
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                }
            ]
        )
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
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
            "MARKET_DEX_DEPTH_DATA": str(self.dex_depth_path),
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
        self.write_cex_depth_cohort([{"exchange": "binance"}])
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
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
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

        for market in payload["markets"]:
            for fact_name, fact in market["facts"].items():
                with self.subTest(
                    market_id=market["market_id"],
                    fact_name=fact_name,
                ):
                    rule = quality_outcome_rule(
                        fact["status"],
                        fact["reason_code"],
                    )
                    self.assertIsNotNone(rule, fact)
                    self.assertIs(fact["retryable"], rule.retryable)

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
        self.write_cex_depth_cohort(
            [
                {"exchange": "binance"},
                {
                    "exchange": "okx",
                    "source_instrument": "BTC-USDT",
                },
            ]
        )
        self.write_dex_depth_cohort(
            [
                {
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                }
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
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
            "MARKET_DEX_DEPTH_DATA": str(self.dex_depth_path),
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
        self.assertFalse(by_id[kraken_id]["retryable"])
        self.assertIsNone(by_id[kraken_id]["action"])
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
            "2026-01-02T00:00:00+00:00",
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

    def test_execution_quality_aggregates_multiple_retryable_cex_reasons(self):
        market_id = "cex:binance:BTC/USDT"
        network_rows = self.execution_rows(
            market_id,
            "cex",
            state_observed_at="2026-01-02T00:00:00+00:00",
            status="failed",
            status_reason="network",
        )
        rate_limit_rows = self.execution_rows(
            market_id,
            "cex",
            state_observed_at="2026-01-02T00:00:00+00:00",
            status="failed",
            status_reason="rate_limit",
        )
        source_state = {
            "snapshot": {
                "by_market": {
                    market_id: network_rows[:5] + rate_limit_rows[:5],
                },
                "observed_at": "2026-01-02T00:02:00+00:00",
            },
            "error_code": None,
        }

        fact = server._execution_quality_fact(
            {"market_id": market_id, "market_type": "cex"},
            source_state,
        )

        self.assertEqual(fact["status"], "collection_failed")
        self.assertEqual(
            fact["reason_code"],
            "multiple_daily_quality_reasons",
        )
        self.assertTrue(fact["retryable"])
        self.assertEqual(fact["action"], "retry_execution_collection")
        self.assertEqual(fact["status_counts"], {"collection_failed": 10})
        self.assertEqual(
            fact["status_reason_counts"],
            {"network": 5, "rate_limit": 5},
        )

    def test_empty_cex_execution_book_uses_stable_v4_flag_shape(self):
        market_id = "cex:binance:BTC/USDT"
        rows = self.execution_rows(
            market_id,
            "cex",
            state_observed_at="2026-01-02T00:00:00+00:00",
            status="failed",
            status_reason="source_no_two_sided_book",
        )
        fact = server._execution_quality_fact(
            {"market_id": market_id, "market_type": "cex"},
            {
                "snapshot": {
                    "by_market": {market_id: rows},
                    "observed_at": "2026-01-02T00:02:00+00:00",
                },
                "error_code": None,
            },
        )

        self.assertEqual(fact["status"], "source_no_observation")
        flag = next(
            flag
            for flag in fact["quality_flags"]
            if flag["code"] == "execution_source_no_observation"
        )
        self.assertEqual(
            set(flag),
            {
                "code",
                "severity",
                "category",
                "message",
                "observed_value",
                "threshold",
            },
        )
        self.assertIsNone(flag["observed_value"])
        self.assertIsNone(flag["threshold"])

    def test_execution_quality_does_not_hide_an_illegal_tuple_in_retryable_rows(self):
        market_id = "cex:binance:BTC/USDT"
        rows = self.execution_rows(
            market_id,
            "cex",
            state_observed_at="2026-01-02T00:00:00+00:00",
            status="failed",
            status_reason="network",
        )
        illegal_row = {
            **rows[-1],
            "status": "unexpected_status",
            "status_reason": "network",
        }
        source_state = {
            "snapshot": {
                "by_market": {market_id: rows[:9] + [illegal_row]},
                "observed_at": "2026-01-02T00:02:00+00:00",
            },
            "error_code": None,
        }

        fact = server._execution_quality_fact(
            {"market_id": market_id, "market_type": "cex"},
            source_state,
        )

        self.assertEqual(fact["status"], "needs_review")
        self.assertEqual(
            fact["reason_code"],
            "daily_quality_outcome_invalid",
        )
        self.assertFalse(fact["retryable"])
        self.assertIsNone(fact["action"])
        self.assertEqual(
            fact["status_counts"],
            {"collection_failed": 9, "needs_review": 1},
        )

    def test_quality_api_reports_partial_execution_without_cost_claim(self):
        binance_id = "cex:binance:BTC/USDT"
        self.write_cex_depth_cohort([{"exchange": "binance"}])
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
            "MARKET_CEX_DEPTH_DATA": str(self.depth_path),
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
            "snapshot_id": "dex-depth-1",
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
        self.write_dex_depth_cohort(
            [
                {
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                },
                {
                    "chain": "solana",
                    "dex": "orca",
                    "pool_address": solana_address,
                    "protocol_model": "unsupported",
                },
            ]
        )
        environment = {
            **self.environment,
            "MARKET_DEX_DEPTH_DATA": str(self.dex_depth_path),
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

        def slow_catalog(*, source_signature=None):
            self.assertEqual(
                source_signature,
                (("facts.sqlite3", 1, 100),),
            )
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

    def test_static_html_uses_runtime_release_and_content_fingerprint(self):
        asset_sha = server.static_asset_sha()
        release_sha = server.application_release_sha()
        version = server.static_asset_version()
        html = server.render_versioned_html(server.STATIC_ROOT / "index.html")

        self.assertRegex(asset_sha, r"^[0-9a-f]{64}$")
        self.assertRegex(release_sha, r"^(?:[0-9a-f]{40,64}|unavailable)$")
        self.assertIn(asset_sha[:12], version)
        if release_sha != "unavailable":
            self.assertIn(release_sha[:12], version)
        self.assertNotIn("__ASSET_VERSION__", html)
        for asset in (
            "styles.css",
            "navigation.js",
            "app.js",
            "vendor/lucide.js",
        ):
            self.assertIn(f"/{asset}?v={version}", html)

    def test_static_asset_sha_includes_the_served_vendor_bundle(self):
        with tempfile.TemporaryDirectory() as directory_name:
            static_root = Path(directory_name)
            vendor_root = static_root / "vendor"
            vendor_root.mkdir()
            for filename in (
                "actions.css",
                "actions.js",
                "app.js",
                "navigation.js",
                "styles.css",
            ):
                (static_root / filename).write_text(filename, encoding="utf-8")
            lucide_path = vendor_root / "lucide.min.js"
            lucide_path.write_text("vendor-one", encoding="utf-8")
            with patch.object(server, "STATIC_ROOT", static_root):
                first = server._compute_static_asset_sha()
                lucide_path.write_text("vendor-two", encoding="utf-8")
                second = server._compute_static_asset_sha()

        self.assertNotEqual(first, second)

    def test_static_asset_sha_tracks_public_bundle_not_protected_admin_assets(self):
        with tempfile.TemporaryDirectory() as directory_name:
            static_root = Path(directory_name)
            vendor_root = static_root / "vendor"
            vendor_root.mkdir()
            for filename in (
                "actions.css",
                "actions.js",
                "admin.css",
                "admin.js",
                "app.js",
                "navigation.js",
                "styles.css",
            ):
                (static_root / filename).write_text(filename, encoding="utf-8")
            (vendor_root / "lucide.min.js").write_text(
                "vendor", encoding="utf-8"
            )
            with patch.object(server, "STATIC_ROOT", static_root):
                baseline = server._compute_static_asset_sha()
                (static_root / "admin.css").write_text(
                    "protected-admin-change", encoding="utf-8"
                )
                after_admin_change = server._compute_static_asset_sha()
                (static_root / "actions.css").write_text(
                    "public-actions-change", encoding="utf-8"
                )
                after_public_change = server._compute_static_asset_sha()

        self.assertEqual(after_admin_change, baseline)
        self.assertNotEqual(after_public_change, baseline)

    def test_release_evidence_is_frozen_for_the_process_lifetime(self):
        initial_application = server.application_release_sha()
        initial_asset = server.static_asset_sha()
        application_clear = getattr(server.application_release_sha, "cache_clear", None)
        asset_clear = getattr(server.static_asset_sha, "cache_clear", None)
        if application_clear:
            application_clear()
        if asset_clear:
            asset_clear()
        try:
            with tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                git_dir = root / ".git"
                static_dir = root / "static"
                git_dir.mkdir()
                static_dir.mkdir()
                (git_dir / "HEAD").write_text("b" * 40 + "\n", encoding="utf-8")
                (static_dir / "app.js").write_text("changed", encoding="utf-8")
                with patch.object(server, "PROJECT_ROOT", root), patch.object(
                    server,
                    "STATIC_ROOT",
                    static_dir,
                ), patch.dict(server.os.environ, {}, clear=True):
                    self.assertEqual(
                        server.application_release_sha(),
                        initial_application,
                    )
                    self.assertEqual(server.static_asset_sha(), initial_asset)
        finally:
            if application_clear:
                application_clear()
            if asset_clear:
                asset_clear()

    def test_health_exposes_application_and_asset_release_evidence(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/health"
        payload = {
            "metadata": {
                "storage": {"engine": "sqlite"},
                "freshness": {"overall_status": "current"},
                "cex_instrument_lifecycle": {
                    "absence_market_count": 1,
                    "applied_market_count": 1,
                    "stale_evidence_market_count": 0,
                },
            }
        }
        with patch.object(
            server,
            "build_market_payload",
            return_value=payload,
        ), patch.object(
            server.MarketMonitorHandler,
            "send_json",
        ) as send_json:
            handler.do_GET()

        health = send_json.call_args.args[0]
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["application_sha"], server.application_release_sha())
        self.assertEqual(health["asset_sha"], server.static_asset_sha())
        self.assertEqual(health["asset_version"], server.static_asset_version())
        self.assertEqual(health["data_status"], "current")
        self.assertEqual(
            health["cex_instrument_lifecycle"],
            payload["metadata"]["cex_instrument_lifecycle"],
        )

    def test_health_data_status_is_stale_when_lifecycle_evidence_is_stale(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/health"
        payload = {
            "metadata": {
                "storage": {"engine": "sqlite"},
                "freshness": {"overall_status": "current"},
                "cex_instrument_lifecycle": {
                    "absence_market_count": 1,
                    "applied_market_count": 1,
                    "stale_evidence_market_count": 1,
                },
            }
        }
        with patch.object(
            server,
            "build_market_payload",
            return_value=payload,
        ), patch.object(
            server.MarketMonitorHandler,
            "send_json",
        ) as send_json:
            handler.do_GET()

        health = send_json.call_args.args[0]
        self.assertEqual(health["status"], "ok")
        self.assertTrue(health["data_ready"])
        self.assertEqual(health["data_status"], "stale")

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
        self.assertIn("Primary DEX/CEX Basis", index)
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
        imported_at = "2026-08-01T02:03:04+00:00"
        with patch(
            "scripts.market_database.utc_now_text",
            return_value=imported_at,
        ):
            build_database(data_dir, database_path)

        with patch.dict(
            server.os.environ,
            {"MARKET_DATABASE": str(database_path)},
            clear=True,
        ):
            payload = server.build_market_payload("2026-01-01", "2026-01-02")

        self.assertEqual(payload["metadata"]["storage"]["engine"], "sqlite")
        self.assertEqual(
            payload["metadata"]["storage"]["imported_at"],
            imported_at,
        )
        daily_source_names = {server.CEX_FILENAME, server.DEX_FILENAME}
        daily_sources = [
            source
            for source in payload["metadata"]["sources"]
            if source["name"] in daily_source_names
        ]
        self.assertEqual(
            {source["name"] for source in daily_sources},
            daily_source_names,
        )
        for source in daily_sources:
            with self.subTest(source=source["name"]):
                self.assertNotIn("modified_at", source)
                self.assertEqual(source["ingested_at"], imported_at)
        self.assertEqual(payload["metadata"]["token_count"], 1)
        self.assertEqual(len(payload["cex_markets"]), 2)
        self.assertEqual(len(payload["dex_pools"]), 1)
        self.assertAlmostEqual(payload["tokens"][0]["price_spread"], 105 / 102 - 1)
        self.assertEqual(payload["dex_pools"][0]["tvl_usd"], 5000)

class DashboardApiTest(unittest.TestCase):
    def test_public_api_metadata_exposes_fail_closed_fact_refresh_capability(self):
        source_payload = {
            "metadata": {"response_scope": "screener_summary"},
            "tokens": [],
        }

        with patch.object(
            server,
            "build_market_summary",
            return_value=source_payload,
        ), patch.object(
            server,
            "PUBLIC_ACTION_POLICY",
            server.PublicActionPolicy(fact_refresh_enabled=False),
        ):
            disabled = server._build_public_api_payload("summary", ())

        with patch.object(
            server,
            "build_market_summary",
            return_value=source_payload,
        ), patch.object(
            server,
            "PUBLIC_ACTION_POLICY",
            server.PublicActionPolicy(fact_refresh_enabled=True),
        ):
            enabled = server._build_public_api_payload("summary", ())

        self.assertEqual(
            disabled["metadata"]["public_actions"],
            {"fact_refresh_enabled": False},
        )
        self.assertEqual(
            enabled["metadata"]["public_actions"],
            {"fact_refresh_enabled": True},
        )
        self.assertNotIn("public_actions", source_payload["metadata"])

    def test_dex_depth_quality_rejects_stale_usd_alignment(self):
        base_market = {
            "market_type": "dex",
            "depth_status": "observed",
            "depth_error": "observed",
            "total_depth_100bps_usd": 42.0,
            "depth_requires_usd_price_alignment": True,
        }
        stale_fact = server._depth_quality_fact({
            **base_market,
            "depth_usd_price_freshness_status": "stale",
        })
        warning_fact = server._depth_quality_fact({
            **base_market,
            "depth_usd_price_freshness_status": "warning",
        })
        no_alignment_fact = server._depth_quality_fact({
            **base_market,
            "depth_requires_usd_price_alignment": False,
            "depth_usd_price_freshness_status": "stale",
        })

        self.assertIn(
            "depth_usd_price_time_mismatch",
            {flag["code"] for flag in stale_fact["quality_flags"]},
        )
        self.assertIn(
            "depth_usd_price_time_warning",
            {flag["code"] for flag in warning_fact["quality_flags"]},
        )
        self.assertFalse({
            "depth_usd_price_time_mismatch",
            "depth_usd_price_time_warning",
        } & {flag["code"] for flag in no_alignment_fact["quality_flags"]})


if __name__ == "__main__":
    unittest.main()
