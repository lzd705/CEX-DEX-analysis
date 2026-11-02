import csv
import hashlib
import io
import json
import os
import sqlite3
import stat
import tempfile
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

from scripts import route_shadow_inputs
from scripts.execution_cost import EXECUTION_COST_COLUMNS
from scripts.fetch_cex_depth import (
    DEPTH_COLUMNS_ALL as CEX_DEPTH_COLUMNS,
    execution_rows_for_book,
    observed_row as observed_cex_depth_row,
)
from scripts.fetch_dex_depth import (
    DEX_DEPTH_COLUMNS,
    base_row as dex_depth_base_row,
    v2_execution_rows,
)
from scripts.fetch_tvl import TVL_COLUMNS, base_row as tvl_base_row
from scripts.route_shadow_inputs import (
    SourceFileIdentity,
    build_shadow_universe,
    current_source_generation,
    load_run_input_binding,
    selection_window,
    typed_source_lineage_observed_members,
    validate_typed_source_lineage,
    write_run_universe,
)
from scripts.route_universe import route_universe_sha256


NOW = datetime(2026, 8, 2, 13, 0, 0, tzinfo=timezone.utc)
OBSERVED_AT = "2026-08-02T12:00:00+00:00"
BLOCK_TIME = "2026-08-02T11:59:45+00:00"
POOL = "0x1111111111111111111111111111111111111111"
REQUIRED_DATA_PATHS = (
    "market_facts.sqlite3",
    "cex_instrument_lifecycle.json",
    "admin/token_registry.json",
    "cex_exchange_volume_daily.csv",
    "cex_depth_latest.csv",
    "dex_depth_latest.csv",
    "cex_execution_cost_latest.csv",
    "dex_execution_cost_latest.csv",
    "dex_pool_tvl_latest.csv",
)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def csv_bytes(fieldnames, rows):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


class ProductionInputFixture:
    def __init__(self, root):
        self.root = Path(root)
        self.data_dir = self.root / "market-data"
        self.project_root = self.root / "project"
        self.config_path = self.project_root / "config/tokens.csv"
        self.chain_config_path = self.project_root / "config/token_chains.csv"
        self.data_dir.mkdir()
        (self.data_dir / "admin").mkdir()
        self.config_path.parent.mkdir(parents=True)
        self.write_inputs()

    def write_inputs(self):
        write_csv(
            self.config_path,
            [
                "token_symbol", "coingecko_id", "chain", "contract_address",
                "cex_symbol", "primary_cex", "secondary_cex", "dex_source",
                "primary_dex", "pool_address", "notes",
            ],
            [{
                "token_symbol": "UNI",
                "coingecko_id": "uniswap",
                "chain": "eth",
                "contract_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                "cex_symbol": "UNI/USDT",
                "primary_cex": "binance",
                "secondary_cex": "okx",
                "dex_source": "geckoterminal",
                "primary_dex": "uniswap_v2",
                "pool_address": "",
                "notes": "fixture",
            }],
        )
        write_csv(
            self.chain_config_path,
            ("token_symbol", "chain", "contract_address", "notes"),
            [{
                "token_symbol": "UNI",
                "chain": "eth",
                "contract_address": (
                    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
                ),
                "notes": "fixture",
            }],
        )
        window_start = datetime.strptime("2026-07-03", "%Y-%m-%d").date()
        window_end = datetime.strptime("2026-08-01", "%Y-%m-%d").date()
        self.cex_rows = [{
            "date": "2026-07-02", "token_symbol": "UNI",
            "exchange": "binance", "cex_symbol": "UNI/USDT",
            "open": "7", "high": "8", "low": "6", "close": "7",
            "base_volume": "10", "quote_volume_usd": "999",
        }]
        self.cex_rows.extend({
            "date": (window_start + timedelta(days=offset)).isoformat(),
            "token_symbol": "UNI",
            "exchange": "binance",
            "cex_symbol": "UNI/USDT",
            "open": "7",
            "high": "8",
            "low": "6",
            "close": "7",
            "base_volume": "0",
            "quote_volume_usd": "0",
        } for offset in range((window_end - window_start).days + 1))
        cex_fields = [
            "date", "token_symbol", "exchange", "cex_symbol", "open", "high",
            "low", "close", "base_volume", "quote_volume_usd",
        ]
        write_csv(
            self.data_dir / "cex_exchange_volume_daily.csv",
            cex_fields,
            self.cex_rows,
        )
        self._write_database()
        configured_crypto = ("cex:crypto_com:UNI/USDT",)
        (self.data_dir / "cex_instrument_lifecycle.json").write_text(
            json.dumps({
                "schema": "cex_instrument_lifecycle/v1",
                "generated_at_utc": OBSERVED_AT,
                "checked_at_utc": OBSERVED_AT,
                "response_sha256": "a" * 64,
                "inventory_count": 1,
                "configured_market_count": 1,
                "configured_market_ids_sha256": hashlib.sha256(json.dumps(
                    configured_crypto, ensure_ascii=True, separators=(",", ":")
                ).encode("ascii")).hexdigest(),
                "review_count": 0,
                "reviews": [],
            }, sort_keys=True),
            encoding="utf-8",
        )
        (self.data_dir / "admin/token_registry.json").write_text(
            '{"schema_version":1,"tokens":{}}\n', encoding="utf-8"
        )
        cex_depth = observed_cex_depth_row(
            {"token_symbol": "UNI", "exchange": "binance", "cex_symbol": "UNI/USDT"},
            self._cex_book(),
            snapshot_id="cex-depth-1",
            request_started_at="2026-08-02T11:59:59+00:00",
            response_received_at=OBSERVED_AT,
        )
        write_csv(
            self.data_dir / "cex_depth_latest.csv",
            CEX_DEPTH_COLUMNS,
            [cex_depth],
        )
        dex_depth = dex_depth_base_row(
            self._dex_pool(),
            snapshot_id="dex-depth-1",
            request_started_at="2026-08-02T11:59:44+00:00",
            response_received_at=BLOCK_TIME,
        )
        dex_depth.update({
            "protocol_model": "constant_product_v2",
            "block_number": "123",
            "block_timestamp": BLOCK_TIME,
            "status": "observed",
            "reason_code": "observed",
            "source_endpoint": "https://rpc.example.test",
            "raw_response_sha256": "d" * 64,
        })
        for index, band in enumerate((10, 25, 50, 100), start=1):
            dex_depth["buy_depth_{}bps_usd".format(band)] = str(index * 20)
            dex_depth["sell_depth_{}bps_usd".format(band)] = str(index * 15)
            dex_depth["total_depth_{}bps_usd".format(band)] = str(index * 35)
            dex_depth["depth_{}bps_complete".format(band)] = "1"
        write_csv(
            self.data_dir / "dex_depth_latest.csv",
            DEX_DEPTH_COLUMNS,
            [dex_depth],
        )
        write_csv(
            self.data_dir / "cex_execution_cost_latest.csv",
            EXECUTION_COST_COLUMNS,
            execution_rows_for_book(
                {"token_symbol": "UNI", "exchange": "binance", "cex_symbol": "UNI/USDT"},
                self._cex_book(),
                snapshot_id="cex-depth-1",
                request_started_at="2026-08-02T11:59:59+00:00",
                response_received_at=OBSERVED_AT,
            ),
        )
        write_csv(
            self.data_dir / "dex_execution_cost_latest.csv",
            EXECUTION_COST_COLUMNS,
            self._dex_execution_rows(),
        )
        tvl = tvl_base_row(
            self._dex_pool(),
            snapshot_id="tvl-1",
            request_started_at="2026-08-02T11:59:58+00:00",
            response_received_at=OBSERVED_AT,
            source_endpoint="https://api.example.test/pools",
        )
        tvl.update({
            "base_token_id": (
                "eth_0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
            ),
            "quote_token_id": "eth_0x" + "2" * 40,
            "base_token_price_usd": "100",
            "quote_token_price_usd": "1",
            "tvl_usd": "0",
            "volume_24h_usd": "",
            "raw_response_sha256": "e" * 64,
            "status": "observed",
            "reason_code": "observed",
        })
        write_csv(
            self.data_dir / "dex_pool_tvl_latest.csv",
            TVL_COLUMNS,
            [tvl],
        )

    def _cex_book(self):
        return {
            "bids": [(Decimal("99.99"), Decimal("2000")), (Decimal("98"), Decimal("2000"))],
            "asks": [(Decimal("100.01"), Decimal("2000")), (Decimal("102"), Decimal("2000"))],
            "source_instrument": "UNIUSDT",
            "source_sequence": "123",
            "source_observed_at": OBSERVED_AT,
            "source_endpoint": "https://api.example.test/depth",
            "raw": b'{"book":"fixture"}',
            "source_quote_asset": "USDT",
            "quote_to_usd": Decimal("1"),
            "quote_conversion_method": "USDT=USD proxy",
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
            "full_book_reported": True,
        }

    def _dex_pool(self):
        return {
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": POOL,
            "pool_name": "UNI / USDC",
            "snapshot_id": "tvl-1",
            "observed_at": BLOCK_TIME,
            "response_received_at": BLOCK_TIME,
            "source": "GeckoTerminal API v2",
            "source_endpoint": "https://api.example.test/pools",
            "raw_response_sha256": "e" * 64,
        }

    def _dex_execution_rows(self):
        pool = self._dex_pool()
        common = {
            "snapshot_id": "dex-depth-1",
            "source_snapshot_id": "dex-depth-1",
            "calculation_method": "fixed_block_pool_state_exact_target_quantity_v1",
            "observed_at": BLOCK_TIME,
            "state_observed_at": BLOCK_TIME,
            "request_started_at": "2026-08-02T11:59:44+00:00",
            "response_received_at": BLOCK_TIME,
            "market_id": "dex:eth:uniswap_v2:{}:UNI".format(POOL),
            "market_type": "dex",
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": POOL,
            "block_number": "123",
            "block_timestamp": BLOCK_TIME,
            "protocol_model": "constant_product_v2",
            "target_token_address": (
                "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
            ),
            "target_token_decimals": "18",
            "quote_token_address": "0x" + "2" * 40,
            "quote_token_decimals": "6",
            "reference_price_method": "pre_fee_pool_state_marginal_price",
            "usd_price_source_snapshot_id": "tvl-1",
            "usd_price_observed_at": BLOCK_TIME,
            "fee_status": "included_protocol_fee",
            "fee_rate_bps": "30",
            "usd_conversion_status": "observed_inventory_token_price",
            "excluded_costs": "gas,router_fee,token_transfer_tax,MEV,post_block_state_changes",
            "source": "fixed-block EVM JSON-RPC pool state",
            "source_endpoint": "https://rpc.example.test",
            "source_sequence": "123",
            "raw_response_sha256": "d" * 64,
        }
        return v2_execution_rows(
            pool,
            common=common,
            target_position_index=0,
            token0_decimals=18,
            token1_decimals=6,
            token0_price=Decimal("100"),
            token1_price=Decimal("1"),
            reserve0=Decimal(100_000) * (Decimal(10) ** 18),
            reserve1=Decimal(10_000_000) * (Decimal(10) ** 6),
            fee_bps=Decimal("30"),
        )

    def _write_database(self):
        database = self.data_dir / "market_facts.sqlite3"
        if database.exists():
            database.unlink()
        schema = (Path(__file__).resolve().parents[1] / "data/schema/001_market_facts.sql")
        connection = sqlite3.connect(str(database))
        try:
            connection.executescript(schema.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations VALUES (1, ?)", (OBSERVED_AT,)
            )
            connection.execute(
                "INSERT INTO tokens VALUES ('UNI', '2026-07-02', '2026-08-01')"
            )
            connection.executemany(
                "INSERT INTO cex_market_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [tuple(row[field] for field in (
                    "date", "token_symbol", "exchange", "cex_symbol", "open",
                    "high", "low", "close", "base_volume", "quote_volume_usd",
                )) for row in self.cex_rows],
            )
            connection.execute(
                "INSERT INTO dex_pool_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "2026-08-01", "UNI", "eth", "uniswap_v2", POOL,
                    "UNI / USDC", "7", "8", "6", "7", "5", "0",
                ),
            )
            cex_path = self.data_dir / "cex_exchange_volume_daily.csv"
            cex_bytes = cex_path.read_bytes()
            cex_sha = hashlib.sha256(cex_bytes).hexdigest()
            connection.execute(
                "INSERT INTO dataset_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "snapshot-1", OBSERVED_AT, "2026-07-02", "2026-08-01", 1,
                    len(self.cex_rows), 1, "cex_exchange_volume_daily.csv",
                    "dex_pool_volume_daily.csv", len(cex_bytes), 1, cex_sha,
                    "b" * 64,
                ),
            )
            connection.execute(
                "INSERT INTO import_runs VALUES (?, ?, ?, ?, ?)",
                ("import-1", "snapshot-1", OBSERVED_AT, "/private/import", "published"),
            )
            connection.execute(
                "INSERT INTO dataset_state VALUES (1, 'snapshot-1', 'import-1')"
            )
            connection.commit()
        finally:
            connection.close()

    def rebind_database_cex_source(self):
        path = self.data_dir / "cex_exchange_volume_daily.csv"
        payload = path.read_bytes()
        connection = sqlite3.connect(str(self.data_dir / "market_facts.sqlite3"))
        try:
            connection.execute(
                "UPDATE dataset_snapshots SET cex_source_bytes = ?, cex_sha256 = ? "
                "WHERE snapshot_id = 'snapshot-1'",
                (len(payload), hashlib.sha256(payload).hexdigest()),
            )
            connection.commit()
        finally:
            connection.close()

    def write_cex_rows(self, rows):
        write_csv(
            self.data_dir / "cex_exchange_volume_daily.csv",
            list(self.cex_rows[0]),
            rows,
        )
        self.rebind_database_cex_source()

    def add_cex_catalog_market(self, exchange):
        connection = sqlite3.connect(str(self.data_dir / "market_facts.sqlite3"))
        try:
            connection.execute(
                "INSERT INTO cex_market_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "2026-08-01", "UNI", exchange, "UNI/USDT",
                    "7", "8", "6", "7", "0", "0",
                ),
            )
            connection.execute(
                "UPDATE dataset_snapshots SET cex_row_count = "
                "(SELECT COUNT(*) FROM cex_market_daily) "
                "WHERE snapshot_id = 'snapshot-1'"
            )
            connection.commit()
        finally:
            connection.close()

    def build(self):
        with patch.object(route_shadow_inputs, "PROJECT_ROOT", self.project_root):
            return build_shadow_universe(
                self.data_dir, NOW, static_token_config=self.config_path
            )


class SelectionWindowTests(unittest.TestCase):
    def test_window_is_exactly_thirty_complete_utc_days(self):
        cases = (
            (datetime(2026, 8, 2, 13, tzinfo=timezone.utc),
             {"start": "2026-07-03", "end": "2026-08-01"}),
            (datetime(2026, 1, 1, 0, tzinfo=timezone.utc),
             {"start": "2025-12-02", "end": "2025-12-31"}),
            (datetime(2024, 3, 1, 1, tzinfo=timezone.utc),
             {"start": "2024-01-31", "end": "2024-02-29"}),
            (datetime(2025, 3, 1, 1, tzinfo=timezone.utc),
             {"start": "2025-01-30", "end": "2025-02-28"}),
        )
        for now, expected in cases:
            with self.subTest(now=now.isoformat()):
                self.assertEqual(selection_window(now), expected)

    def test_naive_clock_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            selection_window(datetime(2026, 8, 2, 13))

    def test_expected_utc_dates_are_the_exact_inclusive_window(self):
        expected = (
            "2026-07-03", "2026-07-04", "2026-07-05", "2026-07-06",
            "2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10",
            "2026-07-11", "2026-07-12", "2026-07-13", "2026-07-14",
            "2026-07-15", "2026-07-16", "2026-07-17", "2026-07-18",
            "2026-07-19", "2026-07-20", "2026-07-21", "2026-07-22",
            "2026-07-23", "2026-07-24", "2026-07-25", "2026-07-26",
            "2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30",
            "2026-07-31", "2026-08-01",
        )

        self.assertEqual(
            route_shadow_inputs._expected_utc_dates({
                "start": "2026-07-03",
                "end": "2026-08-01",
            }),
            expected,
        )

    def test_expected_utc_dates_reject_noncanonical_or_non_thirty_day_windows(self):
        cases = (
            {"start": "2026-7-03", "end": "2026-08-01"},
            {"start": "2026-07-03", "end": "2026-07-31"},
            {"start": "2026-08-01", "end": "2026-07-03"},
        )
        for window in cases:
            with self.subTest(window=window):
                with self.assertRaisesRegex(ValueError, "window|date|canonical|30"):
                    route_shadow_inputs._expected_utc_dates(window)


class ShadowInputBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture = ProductionInputFixture(self.temporary.name)

    def test_lifecycle_response_hash_requires_a_string(self):
        path = self.fixture.data_dir / "cex_instrument_lifecycle.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["response_sha256"] = int("1" * 64)
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "lifecycle response SHA"):
            self.fixture.build()

    def test_build_projects_canonical_lineage_and_preserves_zero_vs_missing(self):
        universe, manifest = self.fixture.build()

        self.assertEqual(universe["selection_window"], {
            "start": "2026-07-03", "end": "2026-08-01",
        })
        legs = {row["market_id"]: row for row in universe["selected_legs"]}
        cex = legs["cex:binance:UNI/USDT"]
        dex_id = "dex:eth:uniswap_v2:{}:UNI".format(POOL)
        dex = legs[dex_id]
        self.assertEqual(cex["selection_inputs"]["cex_selected_window_usd"], "0")
        self.assertEqual(dex["selection_inputs"]["dex_tvl_usd"], "0")
        self.assertIsNone(dex["selection_inputs"]["dex_24h_usd"])
        self.assertEqual(cex["selection_inputs"]["observed_100bps_depth_usd"], "400000")
        self.assertEqual(dex["selection_inputs"]["observed_100bps_depth_usd"], "140")
        self.assertEqual(cex["selection_inputs"]["proved_execution_capacity_usd"], "100000")
        self.assertEqual(dex["selection_inputs"]["proved_execution_capacity_usd"], "100000")
        self.assertEqual(
            dex["target_token_address"],
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        )
        self.assertEqual(dex["target_token_side"], "base")
        self.assertEqual(universe["candidate_source_generation"], manifest[
            "candidate_source_generation"
        ])
        self.assertEqual(
            [entry["path"] for entry in manifest["inputs"]],
            list(REQUIRED_DATA_PATHS)
            + ["config/tokens.csv", "config/token_chains.csv"],
        )
        self.assertFalse(any(
            str(self.fixture.root) in entry["path"] for entry in manifest["inputs"]
        ))
        self.assertEqual(manifest["observation_bounds"], {
            "start_inclusive": "2026-07-03T00:00:00Z",
            "end_exclusive": "2026-08-02T00:00:00Z",
        })

    def test_cex_volume_window_rejects_missing_interior_date(self):
        rows = [
            row for row in self.fixture.cex_rows
            if row["date"] != "2026-07-17"
        ]
        self.fixture.write_cex_rows(rows)

        with self.assertRaisesRegex(ValueError, "CEX volume window is incomplete"):
            self.fixture.build()

    def test_cex_volume_window_rejects_one_day_only(self):
        rows = [
            row for row in self.fixture.cex_rows
            if row["date"] in {"2026-07-02", "2026-07-03"}
        ]
        self.fixture.write_cex_rows(rows)

        with self.assertRaisesRegex(ValueError, "CEX volume window is incomplete"):
            self.fixture.build()

    def test_cex_volume_window_requires_every_active_market_date(self):
        self.fixture.add_cex_catalog_market("okx")
        second_market_rows = [
            {**row, "exchange": "okx"}
            for row in self.fixture.cex_rows
            if (
                "2026-07-03" <= row["date"] <= "2026-08-01"
                and row["date"] != "2026-07-17"
            )
        ]
        self.fixture.write_cex_rows(self.fixture.cex_rows + second_market_rows)

        with self.assertRaisesRegex(ValueError, "CEX volume window is incomplete"):
            self.fixture.build()

    def test_cex_volume_all_thirty_zero_days_sum_to_zero(self):
        universe, _manifest = self.fixture.build()

        cex = next(
            row for row in universe["selected_legs"]
            if row["market_id"] == "cex:binance:UNI/USDT"
        )
        self.assertEqual(cex["selection_inputs"]["cex_selected_window_usd"], "0")

    def test_cex_volume_rejects_market_absent_from_captured_catalog(self):
        unknown_rows = [
            {**row, "exchange": "okx"}
            for row in self.fixture.cex_rows
            if "2026-07-03" <= row["date"] <= "2026-08-01"
        ]
        self.fixture.write_cex_rows(self.fixture.cex_rows + unknown_rows)

        with self.assertRaisesRegex(ValueError, "catalog|unknown"):
            self.fixture.build()

    def test_lifecycle_withheld_cex_market_is_exempt_from_volume_grid(self):
        withheld_market = {
            "token_symbol": "UNI",
            "exchange": "crypto_com",
            "cex_symbol": "UNI/USDT",
        }
        self.fixture.add_cex_catalog_market("crypto_com")
        withheld_volume_row = {
            **next(
                row for row in self.fixture.cex_rows
                if row["date"] == "2026-07-03"
            ),
            "exchange": "crypto_com",
        }
        self.fixture.write_cex_rows(
            self.fixture.cex_rows + [withheld_volume_row]
        )

        lifecycle_path = self.fixture.data_dir / "cex_instrument_lifecycle.json"
        lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
        lifecycle["review_count"] = 1
        lifecycle["reviews"] = [{
            "market_id": "cex:crypto_com:UNI/USDT",
            "market_type": "cex",
            "token_symbol": "UNI",
            "exchange": "crypto_com",
            "instrument": "UNI/USDT",
            "current_listing_status": "absent_from_official_current_catalog",
            "reason_code": "instrument_absent_from_current_catalog",
            "checked_at_utc": OBSERVED_AT,
            "source_url": (
                "https://api.crypto.com/exchange/v1/public/get-instruments"
            ),
            "http_status": 200,
            "response_sha256": "a" * 64,
            "inventory_count": 1,
            "instrument_present": False,
        }]
        lifecycle_path.write_text(
            json.dumps(lifecycle, sort_keys=True), encoding="utf-8"
        )

        depth_path = self.fixture.data_dir / "cex_depth_latest.csv"
        with depth_path.open(newline="", encoding="utf-8") as handle:
            depth_rows = list(csv.DictReader(handle))
        depth_rows.append(observed_cex_depth_row(
            withheld_market,
            self.fixture._cex_book(),
            snapshot_id="cex-depth-1",
            request_started_at="2026-08-02T11:59:59+00:00",
            response_received_at=OBSERVED_AT,
        ))
        write_csv(depth_path, CEX_DEPTH_COLUMNS, depth_rows)

        execution_path = self.fixture.data_dir / "cex_execution_cost_latest.csv"
        with execution_path.open(newline="", encoding="utf-8") as handle:
            execution_rows = list(csv.DictReader(handle))
        execution_rows.extend(execution_rows_for_book(
            withheld_market,
            self.fixture._cex_book(),
            snapshot_id="cex-depth-1",
            request_started_at="2026-08-02T11:59:59+00:00",
            response_received_at=OBSERVED_AT,
        ))
        write_csv(execution_path, EXECUTION_COST_COLUMNS, execution_rows)

        try:
            universe, _manifest = self.fixture.build()
        except ValueError as error:
            self.fail(
                "lifecycle-withheld market was incorrectly required: {}".format(
                    error
                )
            )

        cex_market_ids = {
            row["market_id"] for row in universe["selected_legs"]
            if row["market_type"] == "cex"
        }
        self.assertEqual(cex_market_ids, {"cex:binance:UNI/USDT"})

    def test_database_cex_source_sha_must_match_exact_captured_csv(self):
        path = self.fixture.data_dir / "cex_exchange_volume_daily.csv"
        path.write_bytes(path.read_bytes() + b"\n")

        with self.assertRaisesRegex(ValueError, "CEX CSV.*SHA|SHA.*CEX CSV"):
            self.fixture.build()

    def test_sqlite_parser_opens_a_private_named_copy_of_the_capture(self):
        source = self.fixture.data_dir / "market_facts.sqlite3"
        expected_size = source.stat().st_size
        expected_sha = hashlib.sha256(source.read_bytes()).hexdigest()
        real_connect = sqlite3.connect
        staged_paths = []

        def connect_and_assert(database, *args, **kwargs):
            uri = str(database)
            parsed = urlsplit(uri)
            self.assertTrue(kwargs.get("uri"))
            self.assertEqual(parsed.scheme, "file")
            self.assertEqual(parsed.query, "mode=ro&immutable=1")
            staged_path = Path(unquote(parsed.path))
            self.assertFalse(str(staged_path).startswith("/proc/self/fd/"))
            self.assertFalse(str(staged_path).startswith("/dev/fd/"))
            self.assertEqual(staged_path.name, "market_facts.sqlite3")

            metadata = os.stat(str(staged_path), follow_symlinks=False)
            self.assertTrue(stat.S_ISREG(metadata.st_mode))
            self.assertEqual(metadata.st_nlink, 1)
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            self.assertEqual(metadata.st_size, expected_size)
            self.assertEqual(
                hashlib.sha256(staged_path.read_bytes()).hexdigest(),
                expected_sha,
            )

            parent_metadata = os.stat(
                str(staged_path.parent), follow_symlinks=False
            )
            self.assertTrue(stat.S_ISDIR(parent_metadata.st_mode))
            self.assertEqual(stat.S_IMODE(parent_metadata.st_mode), 0o700)
            staged_paths.append(staged_path)
            return real_connect(database, *args, **kwargs)

        with patch.object(
            route_shadow_inputs.sqlite3,
            "connect",
            side_effect=connect_and_assert,
        ):
            _, manifest = self.fixture.build()

        sqlite_identity = next(
            item for item in manifest["inputs"]
            if item["path"] == "market_facts.sqlite3"
        )
        self.assertEqual(sqlite_identity["size"], expected_size)
        self.assertEqual(sqlite_identity["sha256"], expected_sha)
        self.assertEqual(len(staged_paths), 1)
        self.assertFalse(staged_paths[0].exists())
        self.assertFalse(staged_paths[0].parent.exists())

    def test_sqlite_parser_rejects_a_valid_private_copy_mutated_before_open(self):
        original = route_shadow_inputs._stage_sqlite_capture

        @contextmanager
        def stage_then_mutate(capture):
            with original(capture) as uri:
                original_bytes = os.pread(
                    capture.descriptor, capture.identity.size, 0
                )
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "changed.sqlite3"
                    path.write_bytes(original_bytes)
                    connection = sqlite3.connect(str(path))
                    try:
                        connection.execute(
                            "UPDATE import_runs SET imported_at = ? "
                            "WHERE run_id = 'import-1'",
                            ("2026-08-02T12:00:01+00:00",),
                        )
                        connection.commit()
                    finally:
                        connection.close()
                    changed_bytes = path.read_bytes()
                os.ftruncate(capture.descriptor, len(changed_bytes))
                offset = 0
                while offset < len(changed_bytes):
                    offset += os.pwrite(
                        capture.descriptor,
                        changed_bytes[offset:],
                        offset,
                    )
                os.fsync(capture.descriptor)
                yield uri

        with patch.object(
            route_shadow_inputs,
            "_stage_sqlite_capture",
            stage_then_mutate,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "SQLite.*capture|capture.*SQLite|source capture|binding",
            ):
                self.fixture.build()

    def test_sqlite_staging_rejects_in_place_mutation_after_read(self):
        original = route_shadow_inputs._stage_sqlite_capture

        @contextmanager
        def stage_then_mutate_after_read(capture):
            with original(capture) as uri:
                yield uri
                path = Path(unquote(urlsplit(uri).path))
                descriptor = os.open(str(path), os.O_RDWR)
                try:
                    original_byte = os.pread(descriptor, 1, 0)
                    os.pwrite(descriptor, b"X" if original_byte != b"X" else b"Y", 0)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)

        with patch.object(
            route_shadow_inputs,
            "_stage_sqlite_capture",
            stage_then_mutate_after_read,
        ):
            with self.assertRaisesRegex(ValueError, "SQLite staging|staging identity"):
                self.fixture.build()

    def test_sqlite_staging_rejects_same_byte_path_replacement_after_read(self):
        original = route_shadow_inputs._stage_sqlite_capture

        @contextmanager
        def stage_then_replace_after_read(capture):
            with original(capture) as uri:
                yield uri
                path = Path(unquote(urlsplit(uri).path))
                payload = path.read_bytes()
                replacement = path.parent / "replacement.sqlite3"
                descriptor = os.open(
                    str(replacement),
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                try:
                    os.fchmod(descriptor, 0o600)
                    offset = 0
                    while offset < len(payload):
                        offset += os.write(descriptor, payload[offset:])
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                os.replace(str(replacement), str(path))

        with patch.object(
            route_shadow_inputs,
            "_stage_sqlite_capture",
            stage_then_replace_after_read,
        ):
            with self.assertRaisesRegex(ValueError, "SQLite staging|staging identity"):
                self.fixture.build()

    def test_minimal_observed_execution_rows_do_not_prove_capacity(self):
        minimal_fields = [
            "snapshot_id", "source_snapshot_id", "observed_at",
            "state_observed_at", "market_id", "market_type", "token_symbol",
            "exchange", "cex_symbol", "chain", "dex", "pool_address",
            "direction", "requested_notional_usd", "status",
        ]
        for family in ("cex", "dex"):
            path = self.fixture.data_dir / "{}_execution_cost_latest.csv".format(family)
            original = path.read_bytes()
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            minimal = [
                {field: row.get(field, "") for field in minimal_fields}
                for row in rows
                if row["requested_notional_usd"] == "1000"
            ]
            write_csv(path, minimal_fields, minimal)
            with self.subTest(family=family):
                with self.assertRaisesRegex(
                    ValueError,
                    "Execution|notional|contract|provenance|columns|rows",
                ):
                    self.fixture.build()
            path.write_bytes(original)

    def test_latest_depth_and_tvl_files_reject_duplicate_market_rows(self):
        for filename in (
            "cex_depth_latest.csv",
            "dex_depth_latest.csv",
            "dex_pool_tvl_latest.csv",
        ):
            path = self.fixture.data_dir / filename
            original = path.read_bytes()
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                rows = list(reader)
            write_csv(path, fields, rows + [dict(rows[0])])
            with self.subTest(filename=filename):
                with self.assertRaisesRegex(ValueError, "duplicate|publication"):
                    self.fixture.build()
            path.write_bytes(original)

    def test_each_fact_family_rejects_mixed_snapshot_ids(self):
        family_files = (
            "cex_depth_latest.csv",
            "dex_depth_latest.csv",
            "cex_execution_cost_latest.csv",
            "dex_execution_cost_latest.csv",
            "dex_pool_tvl_latest.csv",
        )
        for filename in family_files:
            path = self.fixture.data_dir / filename
            original = path.read_bytes()
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                rows = list(reader)
            if len(rows) == 1:
                second = dict(rows[0])
                second["snapshot_id"] = second["snapshot_id"] + "-other"
                rows.append(second)
            else:
                rows[-1]["snapshot_id"] = rows[-1]["snapshot_id"] + "-other"
            write_csv(path, fields, rows)
            with self.subTest(filename=filename):
                with self.assertRaisesRegex(ValueError, "snapshot|publication|duplicate"):
                    self.fixture.build()
            path.write_bytes(original)

    def test_depth_and_tvl_families_reject_mixed_snapshots_across_distinct_markets(self):
        cases = []
        for filename, fields, parser, first_id, second_id in (
            (
                "cex_depth_latest.csv",
                CEX_DEPTH_COLUMNS,
                lambda payload, expected: route_shadow_inputs._parse_depth(
                    payload, market_type="cex", expected_market_ids=expected
                ),
                "cex:binance:UNI/USDT",
                "cex:okx:UNI/USDT",
            ),
            (
                "dex_depth_latest.csv",
                DEX_DEPTH_COLUMNS,
                lambda payload, expected: route_shadow_inputs._parse_depth(
                    payload, market_type="dex", expected_market_ids=expected
                ),
                "dex:eth:uniswap_v2:{}:UNI".format(POOL),
                "dex:eth:uniswap_v2:{}:UNI".format("0x" + "3" * 40),
            ),
            (
                "dex_pool_tvl_latest.csv",
                TVL_COLUMNS,
                lambda payload, expected: route_shadow_inputs._parse_tvl_and_volume(
                    payload, expected_market_ids=expected
                ),
                "dex:eth:uniswap_v2:{}:UNI".format(POOL),
                "dex:eth:uniswap_v2:{}:UNI".format("0x" + "3" * 40),
            ),
        ):
            path = self.fixture.data_dir / filename
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            second = dict(rows[0])
            second["snapshot_id"] = second["snapshot_id"] + "-other"
            if filename == "cex_depth_latest.csv":
                second["exchange"] = "okx"
            else:
                second["pool_address"] = "0x" + "3" * 40
            cases.append((filename, parser, {first_id, second_id}, rows + [second], fields))

        for filename, parser, expected, rows, fields in cases:
            with self.subTest(filename=filename):
                with self.assertRaisesRegex(ValueError, "one nonempty snapshot|snapshot ID"):
                    parser(csv_bytes(fields, rows), expected)

    def test_execution_families_reject_duplicate_scenario_rows(self):
        for family, market_id, depth_snapshot in (
            ("cex", "cex:binance:UNI/USDT", "cex-depth-1"),
            (
                "dex",
                "dex:eth:uniswap_v2:{}:UNI".format(POOL),
                "dex-depth-1",
            ),
        ):
            path = self.fixture.data_dir / "{}_execution_cost_latest.csv".format(family)
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            with self.subTest(family=family):
                with self.assertRaisesRegex(ValueError, "duplicate scenario"):
                    route_shadow_inputs._parse_execution(
                        csv_bytes(EXECUTION_COST_COLUMNS, rows + [dict(rows[0])]),
                        market_type=family,
                        depth_snapshot_id=depth_snapshot,
                        expected_market_ids={market_id},
                    )

    def test_execution_source_snapshot_matches_its_depth_family_publication(self):
        for family in ("cex", "dex"):
            path = self.fixture.data_dir / "{}_execution_cost_latest.csv".format(family)
            original = path.read_bytes()
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                rows = list(reader)
            for row in rows:
                row["source_snapshot_id"] = "other-depth-snapshot"
            write_csv(path, fields, rows)
            with self.subTest(family=family):
                with self.assertRaisesRegex(ValueError, "Depth.*Execution|source.*snapshot|lineage"):
                    self.fixture.build()
            path.write_bytes(original)

    def test_canonical_nonobserved_tvl_rows_do_not_abort_shadow_build(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            baseline = list(reader)[0]
        reasons = {
            "missing": "source_no_tvl_observation",
            "not_found": "source_pool_not_found",
            "failed": "collection_failed",
        }
        for status, reason in reasons.items():
            row = dict(baseline)
            row.update({
                "status": status,
                "reason_code": reason,
                "tvl_usd": "",
                "volume_24h_usd": "",
                "base_token_id": "",
                "quote_token_id": "",
                "base_token_price_usd": "",
                "quote_token_price_usd": "",
                "error": "fixture {}".format(status),
            })
            write_csv(path, fields, [row])
            with self.subTest(status=status):
                universe, _manifest = self.fixture.build()
                dex = next(
                    item for item in universe["selected_legs"]
                    if item["market_type"] == "dex"
                )
                self.assertIsNone(dex["selection_inputs"]["dex_tvl_usd"])
                self.assertIsNone(dex["selection_inputs"]["dex_24h_usd"])
                self.assertEqual(
                    dex["collector_context"]["status"], status
                )
                self.assertEqual(
                    dex["collector_context"]["reason_code"], reason
                )
                self.assertEqual(
                    dex["target_token_address"],
                    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                )
                self.assertIsNone(dex["target_token_side"])

    def test_quote_side_target_is_derived_from_captured_token_identity(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            rows = list(reader)
        rows[0]["base_token_id"] = "eth_0x" + "2" * 40
        rows[0]["quote_token_id"] = (
            "eth_0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
        )
        write_csv(path, fields, rows)

        universe, _manifest = self.fixture.build()

        dex = next(
            item for item in universe["selected_legs"]
            if item["market_type"] == "dex"
        )
        self.assertEqual(dex["target_token_side"], "quote")

    def test_cross_chain_target_uses_captured_chain_config_identity(self):
        write_csv(
            self.fixture.chain_config_path,
            ("token_symbol", "chain", "contract_address", "notes"),
            [
                {
                    "token_symbol": "UNI",
                    "chain": "eth",
                    "contract_address": (
                        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
                    ),
                    "notes": "ethereum canonical",
                },
                {
                    "token_symbol": "UNI",
                    "chain": "arbitrum",
                    "contract_address": (
                        "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0"
                    ),
                    "notes": "arbitrum bridged",
                },
            ],
        )
        with self.fixture.data_dir.joinpath(
            "dex_pool_tvl_latest.csv"
        ).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            tvl = list(reader)[0]
        tvl.update({
            "chain": "arbitrum",
            "base_token_id": (
                "arbitrum_0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0"
            ),
            "quote_token_id": "arbitrum_0x" + "2" * 40,
        })
        write_csv(
            self.fixture.data_dir / "dex_pool_tvl_latest.csv", fields, [tvl]
        )
        with self.fixture.data_dir.joinpath(
            "dex_depth_latest.csv"
        ).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            depth = list(reader)[0]
        depth.update({"chain": "arbitrum"})
        write_csv(
            self.fixture.data_dir / "dex_depth_latest.csv", fields, [depth]
        )
        with self.fixture.data_dir.joinpath(
            "dex_execution_cost_latest.csv"
        ).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            execution = list(reader)
        for row in execution:
            row.update({
                "chain": "arbitrum",
                "market_id": "dex:arbitrum:uniswap_v2:{}:UNI".format(POOL),
                "target_token_address": (
                    "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0"
                ),
            })
        write_csv(
            self.fixture.data_dir / "dex_execution_cost_latest.csv",
            fields,
            execution,
        )
        database = self.fixture.data_dir / "market_facts.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "UPDATE dex_pool_daily SET chain = 'arbitrum'"
            )
            connection.commit()
        finally:
            connection.close()

        universe, manifest = self.fixture.build()

        dex = next(
            row for row in universe["selected_legs"]
            if row["market_type"] == "dex"
        )
        self.assertEqual(
            dex["target_token_address"],
            "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0",
        )
        self.assertEqual(dex["target_token_side"], "base")
        self.assertIn(
            "config/token_chains.csv",
            [entry["path"] for entry in manifest["inputs"]],
        )

    def test_observed_context_requires_two_chain_native_ids_and_prices(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            baseline = list(reader)[0]
        for field, value in (
            ("base_token_price_usd", ""),
            ("quote_token_id", baseline["base_token_id"]),
            ("base_token_id", "solana_" + "1" * 32),
        ):
            row = {**baseline, field: value}
            write_csv(path, fields, [row])
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    ValueError, "collector|Token|price|identity|observed"
                ):
                    self.fixture.build()

    def test_nonobserved_context_rejects_stale_token_or_price_fields(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            baseline = list(reader)[0]
        row = {
            **baseline,
            "status": "missing",
            "reason_code": "source_no_tvl_observation",
            "tvl_usd": "",
            "volume_24h_usd": "",
        }
        for field in (
            "base_token_id", "quote_token_id",
            "base_token_price_usd", "quote_token_price_usd",
        ):
            forged = dict(row)
            forged[field] = baseline[field]
            for other in (
                "base_token_id", "quote_token_id",
                "base_token_price_usd", "quote_token_price_usd",
            ):
                if other != field:
                    forged[other] = ""
            write_csv(path, fields, [forged])
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    ValueError, "non-observed|collector|Token|price"
                ):
                    self.fixture.build()

    def test_collector_context_rejects_unsafe_or_noncanonical_endpoint(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            baseline = list(reader)[0]
        for endpoint in (
            "https://user:secret@api.example.test/pools",
            "https://api.example.test/pools?token=secret",
            "file:///tmp/pool.json",
            "HTTPS://api.example.test/pools",
        ):
            write_csv(
                path, fields, [{**baseline, "source_endpoint": endpoint}]
            )
            with self.subTest(endpoint=endpoint):
                with self.assertRaisesRegex(ValueError, "endpoint|unsafe"):
                    self.fixture.build()

    def test_chain_config_rejects_duplicate_or_conflicting_identity(self):
        rows = [
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "contract_address": (
                    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
                ),
                "notes": "canonical",
            },
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "contract_address": "0x" + "9" * 40,
                "notes": "conflict",
            },
        ]
        write_csv(
            self.fixture.chain_config_path,
            ("token_symbol", "chain", "contract_address", "notes"),
            rows,
        )
        with self.assertRaisesRegex(ValueError, "duplicate|conflict|identity"):
            self.fixture.build()

    def test_active_runtime_identity_must_not_conflict_with_captured_chain_config(self):
        registry = {
            "schema_version": 1,
            "tokens": {
                "eth:0x{}".format("9" * 40): {
                    "token_symbol": "UNI",
                    "token_name": "UNI Token",
                    "chain": "eth",
                    "contract_address": "0x" + "9" * 40,
                    "decimals": 18,
                    "coingecko_id": None,
                    "source": "geckoterminal",
                    "source_token_id": "eth_0x" + "9" * 40,
                    "status": "active",
                    "cex_mapping": {
                        "status": "requires_manual_review",
                        "cex_symbol": None,
                        "exchanges": [],
                    },
                    "created_at": OBSERVED_AT,
                    "created_by": "fixture",
                    "activated_at": OBSERVED_AT,
                    "last_job_id": None,
                },
            },
        }
        (self.fixture.data_dir / "admin/token_registry.json").write_text(
            json.dumps(registry, sort_keys=True) + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "conflict|collision|identity"):
            self.fixture.build()

    def test_pending_runtime_identity_does_not_override_captured_chain_config(self):
        registry = {
            "schema_version": 1,
            "tokens": {
                "eth:0x{}".format("9" * 40): {
                    "token_symbol": "UNI",
                    "token_name": "UNI Token",
                    "chain": "eth",
                    "contract_address": "0x" + "9" * 40,
                    "decimals": 18,
                    "coingecko_id": None,
                    "source": "geckoterminal",
                    "source_token_id": "eth_0x" + "9" * 40,
                    "status": "pending",
                    "cex_mapping": {
                        "status": "requires_manual_review",
                        "cex_symbol": None,
                        "exchanges": [],
                    },
                    "created_at": OBSERVED_AT,
                    "created_by": "fixture",
                    "activated_at": None,
                    "last_job_id": None,
                },
            },
        }
        (self.fixture.data_dir / "admin/token_registry.json").write_text(
            json.dumps(registry, sort_keys=True) + "\n", encoding="utf-8"
        )

        universe, _manifest = self.fixture.build()

        dex = next(
            row for row in universe["selected_legs"]
            if row["market_type"] == "dex"
        )
        self.assertEqual(
            dex["target_token_address"],
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        )

    def test_tvl_status_value_contract_rejects_negative_or_nonobserved_values(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            baseline = list(reader)[0]
        bad_rows = (
            {**baseline, "status": "observed", "reason_code": "observed", "tvl_usd": "-1"},
            {
                **baseline,
                "status": "missing",
                "reason_code": "source_no_tvl_observation",
                "tvl_usd": "1",
            },
        )
        for row in bad_rows:
            write_csv(path, fields, [row])
            with self.subTest(status=row["status"], value=row["tvl_usd"]):
                with self.assertRaisesRegex(ValueError, "TVL|tvl_usd|sign|non-observed"):
                    self.fixture.build()

    def test_aggregate_capture_budget_fails_before_retaining_all_sources(self):
        total = sum(
            (self.fixture.data_dir / relative).stat().st_size
            for relative in REQUIRED_DATA_PATHS
        ) + sum(
            path.stat().st_size
            for path in (
                self.fixture.config_path,
                self.fixture.chain_config_path,
            )
        )
        with patch.object(
            route_shadow_inputs,
            "MAX_AGGREGATE_SOURCE_BYTES",
            total - 1,
            create=True,
        ):
            with self.assertRaisesRegex(ValueError, "aggregate|budget|bounded"):
                self.fixture.build()

    def test_single_source_capture_limit_is_enforced(self):
        source_name = "cex_instrument_lifecycle.json"
        source_size = (self.fixture.data_dir / source_name).stat().st_size
        constrained = tuple(
            (logical, relative, source_size - 1 if logical == source_name else maximum)
            for logical, relative, maximum in route_shadow_inputs._DATA_INPUTS
        )
        with patch.object(route_shadow_inputs, "_DATA_INPUTS", constrained):
            with self.assertRaisesRegex(ValueError, "bounded input limit"):
                self.fixture.build()

    def test_unbound_sqlite_sidecars_fail_closed(self):
        database = self.fixture.data_dir / "market_facts.sqlite3"
        for suffix in ("-wal", "-journal", "-shm"):
            sidecar = Path(str(database) + suffix)
            sidecar.write_bytes(b"unbound")
            with self.subTest(suffix=suffix):
                with self.assertRaisesRegex(ValueError, "sidecar"):
                    self.fixture.build()
            sidecar.unlink()

    def test_sqlite_current_state_must_be_unique_and_complete(self):
        database = self.fixture.data_dir / "market_facts.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute("DELETE FROM dataset_state")
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(ValueError, "current.*state|dataset_state"):
            self.fixture.build()

    def test_sqlite_schema_version_must_match_the_published_contract(self):
        database = self.fixture.data_dir / "market_facts.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute("PRAGMA user_version = 99")
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(ValueError, "schema|version"):
            self.fixture.build()

    def test_sqlite_current_dex_row_count_must_match_the_captured_database(self):
        database = self.fixture.data_dir / "market_facts.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "UPDATE dataset_snapshots SET dex_row_count = 2 "
                "WHERE snapshot_id = 'snapshot-1'"
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(ValueError, "DEX row count"):
            self.fixture.build()

    def test_depth_and_execution_source_snapshot_lineage_must_match(self):
        path = self.fixture.data_dir / "dex_execution_cost_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["source_snapshot_id"] = "another-depth"
        write_csv(path, list(rows[0]), rows)

        with self.assertRaisesRegex(
            ValueError,
            "Depth.*Execution|lineage|source_snapshot|source snapshot",
        ):
            self.fixture.build()

    def test_dex_tvl_latest_rejects_multiple_publication_rows_for_one_market(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            fields = list(csv.DictReader(handle).fieldnames)
        write_csv(path, fields, [
            {
                "snapshot_id": "tvl-old", "observed_at": "2026-08-02T10:00:00Z",
                "token_symbol": "UNI", "chain": "eth", "dex": "uniswap_v2",
                "pool_address": POOL, "pool_name": "UNI / USDC",
                "tvl_usd": "999", "volume_24h_usd": "1", "status": "observed",
            },
            {
                "snapshot_id": "tvl-new", "observed_at": OBSERVED_AT,
                "token_symbol": "UNI", "chain": "eth", "dex": "uniswap_v2",
                "pool_address": POOL, "pool_name": "UNI / USDC",
                "tvl_usd": "0", "volume_24h_usd": "7", "status": "observed",
            },
        ])

        with self.assertRaisesRegex(ValueError, "duplicate|publication"):
            self.fixture.build()

    def test_dex_tvl_latest_does_not_pick_between_multiple_snapshot_instants(self):
        path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            fields = list(csv.DictReader(handle).fieldnames)
        write_csv(path, fields, [
            {
                "snapshot_id": "tvl-earlier", "observed_at": "2026-08-02T12:30:00+01:00",
                "token_symbol": "UNI", "chain": "eth", "dex": "uniswap_v2",
                "pool_address": POOL, "pool_name": "UNI / USDC",
                "tvl_usd": "999", "volume_24h_usd": "999", "status": "observed",
            },
            {
                "snapshot_id": "tvl-later", "observed_at": "2026-08-02T12:00:00Z",
                "token_symbol": "UNI", "chain": "eth", "dex": "uniswap_v2",
                "pool_address": POOL, "pool_name": "UNI / USDC",
                "tvl_usd": "4", "volume_24h_usd": "5", "status": "observed",
            },
        ])

        with self.assertRaisesRegex(ValueError, "duplicate|publication"):
            self.fixture.build()

    def test_cex_volume_missing_value_is_not_treated_as_zero(self):
        rows = list(self.fixture.cex_rows)
        rows[1] = {**rows[1], "quote_volume_usd": ""}
        rows[2] = {**rows[2], "quote_volume_usd": "5"}
        self.fixture.write_cex_rows(rows)

        with self.assertRaisesRegex(ValueError, "incomplete|quote_volume_usd"):
            self.fixture.build()

    def test_cex_volume_rejects_nonfinite_or_negative_amounts(self):
        for amount in ("NaN", "Infinity", "-1"):
            rows = list(self.fixture.cex_rows)
            rows[1] = {**rows[1], "quote_volume_usd": amount}
            self.fixture.write_cex_rows(rows)
            with self.subTest(amount=amount):
                with self.assertRaisesRegex(
                    ValueError, "quote_volume_usd|finite|sign|magnitude"
                ):
                    self.fixture.build()

    def test_signed_zero_is_published_as_canonical_zero(self):
        tvl_path = self.fixture.data_dir / "dex_pool_tvl_latest.csv"
        with tvl_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames)
            rows = list(reader)
        rows[0]["tvl_usd"] = "-0.0"
        rows[0]["volume_24h_usd"] = "-0"
        write_csv(tvl_path, fields, rows)

        universe, _manifest = self.fixture.build()

        dex = next(
            row for row in universe["selected_legs"] if row["market_type"] == "dex"
        )
        self.assertEqual(dex["selection_inputs"]["dex_tvl_usd"], "0")
        self.assertEqual(dex["selection_inputs"]["dex_24h_usd"], "0")
        self.assertTrue(all(
            row["route_volume_usd"] == "0" for row in universe["routes"]
        ))

    def test_symlinked_static_config_argument_is_rejected(self):
        link = self.fixture.project_root / "tokens-link.csv"
        link.symlink_to(self.fixture.config_path)
        with patch.object(route_shadow_inputs, "PROJECT_ROOT", self.fixture.project_root):
            with self.assertRaisesRegex(ValueError, "tracked config|symlink"):
                build_shadow_universe(
                    self.fixture.data_dir, NOW, static_token_config=link
                )

    def test_parsers_use_captured_bytes_after_every_source_path_is_replaced(self):
        original = route_shadow_inputs._capture_required_sources

        def capture_then_replace(data_dir, static_token_config):
            captures = original(data_dir, static_token_config)
            for relative in REQUIRED_DATA_PATHS:
                path = Path(data_dir) / relative
                replacement = path.with_name(path.name + ".replacement")
                replacement.write_bytes(b"later path generation")
                os.replace(str(replacement), str(path))
            replacement = Path(static_token_config).with_name("tokens.replacement")
            replacement.write_bytes(b"later config generation")
            os.replace(str(replacement), str(static_token_config))
            chain_config = Path(static_token_config).with_name("token_chains.csv")
            replacement = chain_config.with_name("token_chains.replacement")
            replacement.write_bytes(b"later chain config generation")
            os.replace(str(replacement), str(chain_config))
            return captures

        with patch.object(route_shadow_inputs, "PROJECT_ROOT", self.fixture.project_root), patch.object(
            route_shadow_inputs,
            "_capture_required_sources",
            side_effect=capture_then_replace,
        ):
            universe, manifest = build_shadow_universe(
                self.fixture.data_dir, NOW,
                static_token_config=self.fixture.config_path,
            )

        self.assertEqual(len(universe["selected_legs"]), 2)
        self.assertEqual(len(manifest["inputs"]), 11)

    def test_descriptor_or_path_identity_change_during_capture_is_rejected(self):
        target = self.fixture.data_dir / "cex_depth_latest.csv"
        original = route_shadow_inputs._copy_source_descriptor
        changed = {"done": False}

        target_identity = (target.stat().st_dev, target.stat().st_ino)

        def copy_then_replace(source_descriptor, capture_descriptor, maximum_bytes):
            result = original(source_descriptor, capture_descriptor, maximum_bytes)
            metadata = os.fstat(source_descriptor)
            if not changed["done"] and (
                metadata.st_dev, metadata.st_ino
            ) == target_identity:
                replacement = target.with_name("cex_depth.replacement")
                replacement.write_bytes(target.read_bytes())
                os.replace(str(replacement), str(target))
                changed["done"] = True
            return result

        with patch.object(route_shadow_inputs, "PROJECT_ROOT", self.fixture.project_root), patch.object(
            route_shadow_inputs, "_copy_source_descriptor", side_effect=copy_then_replace
        ):
            with self.assertRaisesRegex(ValueError, "changed|identity"):
                build_shadow_universe(
                    self.fixture.data_dir, NOW,
                    static_token_config=self.fixture.config_path,
                )

    def test_missing_symlinked_and_non_regular_sources_fail_before_parsing(self):
        target = self.fixture.data_dir / "cex_depth_latest.csv"
        original = target.read_bytes()
        target.unlink()
        with self.assertRaises((FileNotFoundError, ValueError)):
            self.fixture.build()

        target.write_bytes(original)
        external = self.fixture.root / "external.csv"
        external.write_bytes(original)
        target.unlink()
        target.symlink_to(external)
        with self.assertRaisesRegex(ValueError, "symlink|regular"):
            self.fixture.build()

        target.unlink()
        target.mkdir()
        with self.assertRaisesRegex(ValueError, "regular"):
            self.fixture.build()

    def test_mtime_changes_do_not_change_candidate_generation(self):
        _universe, before = self.fixture.build()
        for relative in REQUIRED_DATA_PATHS:
            path = self.fixture.data_dir / relative
            stat_result = path.stat()
            os.utime(path, (stat_result.st_atime + 100, stat_result.st_mtime + 100))
        stat_result = self.fixture.config_path.stat()
        os.utime(
            self.fixture.config_path,
            (stat_result.st_atime + 100, stat_result.st_mtime + 100),
        )
        stat_result = self.fixture.chain_config_path.stat()
        os.utime(
            self.fixture.chain_config_path,
            (stat_result.st_atime + 100, stat_result.st_mtime + 100),
        )
        _universe, after = self.fixture.build()

        self.assertEqual(
            before["candidate_source_generation"],
            after["candidate_source_generation"],
        )

    def test_mutating_each_source_identity_changes_generation(self):
        identities = [
            SourceFileIdentity(path, index + 1, hashlib.sha256(path.encode()).hexdigest())
            for index, path in enumerate(
                list(REQUIRED_DATA_PATHS)
                + ["config/tokens.csv", "config/token_chains.csv"]
            )
        ]
        baseline = route_shadow_inputs._candidate_source_generation(identities)

        for index, identity in enumerate(identities):
            mutated = list(identities)
            mutated[index] = SourceFileIdentity(
                identity.path, identity.size,
                hashlib.sha256((identity.sha256 + "x").encode()).hexdigest(),
            )
            with self.subTest(path=identity.path):
                self.assertNotEqual(
                    route_shadow_inputs._candidate_source_generation(mutated),
                    baseline,
                )


class RunUniversePublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.shadow_root = Path(self.temporary.name) / "routes/shadow"
        identities = [
            SourceFileIdentity(path, index + 1, hashlib.sha256(path.encode()).hexdigest())
            for index, path in enumerate(
                list(REQUIRED_DATA_PATHS)
                + ["config/tokens.csv", "config/token_chains.csv"]
            )
        ]
        generation = route_shadow_inputs._candidate_source_generation(identities)
        self.universe = {
            "schema": "route_universe/v1",
            "candidate_source_generation": generation,
            "selection_window": {"start": "2026-07-03", "end": "2026-08-01"},
            "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
            "selected_legs": [],
            "routes": [],
        }
        self.manifest = {
            "schema": "route_shadow_baseline_manifest/v1",
            "calculation_version": "route_shadow_inputs/v1",
            "candidate_source_generation": generation,
            "selection_window": {"start": "2026-07-03", "end": "2026-08-01"},
            "filters": {"window_days": 30},
            "observation_bounds": {
                "start_inclusive": "2026-07-03T00:00:00Z",
                "end_exclusive": "2026-08-02T00:00:00Z",
            },
            "inputs": [
                {"path": item.path, "size": item.size, "sha256": item.sha256}
                for item in identities
            ],
        }

    def test_joint_publication_is_canonical_immutable_and_hash_bound(self):
        universe_path, manifest_path = write_run_universe(
            self.shadow_root, "run-001", self.universe, self.manifest
        )

        expected_directory = self.shadow_root / "runs/run-001"
        self.assertEqual(universe_path, expected_directory / "route_universe.json")
        self.assertEqual(manifest_path, expected_directory / "baseline_manifest.json")
        self.assertEqual(universe_path.read_bytes(), canonical_bytes(self.universe))
        reread = json.loads(universe_path.read_text(encoding="utf-8"))
        self.assertEqual(route_universe_sha256(reread), route_universe_sha256(self.universe))
        published_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(
            published_manifest["route_universe_sha256"],
            route_universe_sha256(self.universe),
        )
        self.assertEqual(
            published_manifest["candidate_source_generation"],
            self.universe["candidate_source_generation"],
        )

        with self.assertRaisesRegex(ValueError, "already exists|immutable"):
            write_run_universe(
                self.shadow_root, "run-001", self.universe, self.manifest
            )
        self.assertEqual(universe_path.read_bytes(), canonical_bytes(self.universe))

    def test_invalid_run_ids_are_rejected(self):
        invalid = (
            "", ".", "..", "../run", "run/name", "run\\name", " run",
            "run ", "run\nname", "运行", "\x00run",
        )
        for run_id in invalid:
            with self.subTest(run_id=repr(run_id)):
                with self.assertRaisesRegex(ValueError, "run ID"):
                    write_run_universe(
                        self.shadow_root, run_id, self.universe, self.manifest
                    )

    def test_manifest_generation_is_recomputed_from_exact_logical_inputs(self):
        mutated = json.loads(json.dumps(self.manifest))
        mutated["inputs"][0]["sha256"] = "f" * 64

        with self.assertRaisesRegex(ValueError, "generation|source identity"):
            write_run_universe(
                self.shadow_root, "run-mutated-manifest", self.universe, mutated
            )
        self.assertFalse(
            (self.shadow_root / "runs/run-mutated-manifest").exists()
        )

    def test_absolute_manifest_input_path_is_never_serialized(self):
        mutated = json.loads(json.dumps(self.manifest))
        mutated["inputs"][0]["path"] = "/srv/private/market_facts.sqlite3"

        with self.assertRaisesRegex(ValueError, "source identity|manifest"):
            write_run_universe(
                self.shadow_root, "run-absolute-input", self.universe, mutated
            )
        self.assertFalse((self.shadow_root / "runs/run-absolute-input").exists())

    def test_symlinked_shadow_root_is_rejected(self):
        real_root = Path(self.temporary.name) / "real-shadow"
        real_root.mkdir()
        linked_root = Path(self.temporary.name) / "linked-shadow"
        linked_root.symlink_to(real_root, target_is_directory=True)

        with self.assertRaisesRegex(ValueError, "symlink"):
            write_run_universe(
                linked_root, "run-symlink", self.universe, self.manifest
            )
        self.assertFalse((real_root / "runs/run-symlink").exists())

    def test_failure_between_file_writes_never_exposes_partial_final_run(self):
        original = route_shadow_inputs._write_staged_file
        calls = {"count": 0}

        def fail_second(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                raise OSError("injected second-file failure")
            return original(*args, **kwargs)

        with patch.object(
            route_shadow_inputs, "_write_staged_file", side_effect=fail_second
        ):
            with self.assertRaisesRegex(OSError, "injected"):
                write_run_universe(
                    self.shadow_root, "run-failed", self.universe, self.manifest
                )

        self.assertFalse((self.shadow_root / "runs/run-failed").exists())
        self.assertEqual(list((self.shadow_root / "runs").glob(".stage-*")), [])

    def test_stage_swapped_after_final_check_is_rolled_back_from_run_path(self):
        real_library = route_shadow_inputs.ctypes.CDLL(None, use_errno=True)
        operation_name = (
            "renameatx_np"
            if route_shadow_inputs.sys.platform == "darwin"
            else "renameat2"
        )
        real_operation = getattr(real_library, operation_name)

        class SwapBeforeRename:
            argtypes = None
            restype = None

            def __call__(self, *arguments):
                runs_descriptor = arguments[0]
                source_name = os.fsdecode(arguments[1])
                os.rename(
                    source_name,
                    source_name + "-owned",
                    src_dir_fd=runs_descriptor,
                    dst_dir_fd=runs_descriptor,
                )
                os.mkdir(source_name, 0o700, dir_fd=runs_descriptor)
                attacker_descriptor = os.open(
                    source_name,
                    os.O_RDONLY | os.O_DIRECTORY,
                    dir_fd=runs_descriptor,
                )
                try:
                    for member in (
                        "route_universe.json",
                        "baseline_manifest.json",
                    ):
                        descriptor = os.open(
                            member,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                            0o600,
                            dir_fd=attacker_descriptor,
                        )
                        try:
                            os.write(descriptor, b'{"attacker":true}')
                        finally:
                            os.close(descriptor)
                finally:
                    os.close(attacker_descriptor)
                return real_operation(*arguments)

        class SwapLibrary:
            pass

        swap_library = SwapLibrary()
        setattr(swap_library, operation_name, SwapBeforeRename())
        with patch.object(
            route_shadow_inputs.ctypes,
            "CDLL",
            return_value=swap_library,
        ):
            with self.assertRaisesRegex(ValueError, "installed|identity|changed"):
                write_run_universe(
                    self.shadow_root,
                    "run-mid-syscall-swap",
                    self.universe,
                    self.manifest,
                )

        self.assertFalse(
            (self.shadow_root / "runs/run-mid-syscall-swap").exists()
        )

    def test_swapped_staging_directory_cannot_be_renamed_as_the_run(self):
        original = route_shadow_inputs._rename_directory_noreplace_at

        def swap_then_rename(runs_descriptor, source_name, destination_name, *args, **kwargs):
            stolen_name = source_name + "-stolen"
            os.rename(
                source_name,
                stolen_name,
                src_dir_fd=runs_descriptor,
                dst_dir_fd=runs_descriptor,
            )
            os.mkdir(source_name, 0o700, dir_fd=runs_descriptor)
            attacker_fd = os.open(
                source_name,
                os.O_RDONLY | os.O_DIRECTORY,
                dir_fd=runs_descriptor,
            )
            try:
                for member in ("route_universe.json", "baseline_manifest.json"):
                    descriptor = os.open(
                        member,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=attacker_fd,
                    )
                    try:
                        os.write(descriptor, b'{"attacker":true}')
                    finally:
                        os.close(descriptor)
            finally:
                os.close(attacker_fd)
            return original(
                runs_descriptor,
                source_name,
                destination_name,
                *args,
                **kwargs,
            )

        with patch.object(
            route_shadow_inputs,
            "_rename_directory_noreplace_at",
            side_effect=swap_then_rename,
        ):
            with self.assertRaisesRegex(ValueError, "staging|identity|changed"):
                write_run_universe(
                    self.shadow_root, "run-swapped-stage", self.universe, self.manifest
                )

        self.assertFalse((self.shadow_root / "runs/run-swapped-stage").exists())

    def test_owned_run_is_removed_if_post_install_bytes_fail_verification(self):
        original = route_shadow_inputs._rename_directory_noreplace_at

        def rename_then_mutate(runs_descriptor, source_name, destination_name, *args, **kwargs):
            result = original(
                runs_descriptor,
                source_name,
                destination_name,
                *args,
                **kwargs,
            )
            installed_fd = os.open(
                destination_name,
                os.O_RDONLY | os.O_DIRECTORY,
                dir_fd=runs_descriptor,
            )
            try:
                member_fd = os.open(
                    "route_universe.json",
                    os.O_WRONLY | os.O_TRUNC,
                    dir_fd=installed_fd,
                )
                try:
                    os.write(member_fd, b'{"changed":true}')
                    os.fsync(member_fd)
                finally:
                    os.close(member_fd)
            finally:
                os.close(installed_fd)
            return result

        with patch.object(
            route_shadow_inputs,
            "_rename_directory_noreplace_at",
            side_effect=rename_then_mutate,
        ):
            with self.assertRaisesRegex(ValueError, "installed|verification|bytes|changed"):
                write_run_universe(
                    self.shadow_root, "run-post-install", self.universe, self.manifest
                )

        self.assertFalse((self.shadow_root / "runs/run-post-install").exists())

    def test_racing_writers_have_exactly_one_no_replace_winner(self):
        barrier = threading.Barrier(2)
        results = []

        def publish():
            barrier.wait()
            try:
                write_run_universe(
                    self.shadow_root, "run-race", self.universe, self.manifest
                )
            except Exception as error:
                results.append(("error", str(error)))
            else:
                results.append(("success", ""))

        threads = [threading.Thread(target=publish) for _index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual([status for status, _message in results].count("success"), 1)
        self.assertEqual([status for status, _message in results].count("error"), 1)
        run_directory = self.shadow_root / "runs/run-race"
        self.assertEqual(
            sorted(path.name for path in run_directory.iterdir()),
            ["baseline_manifest.json", "route_universe.json"],
        )

    def test_run_input_binding_descriptor_rereads_exact_published_bytes(self):
        write_run_universe(
            self.shadow_root, "run-binding", self.universe, self.manifest
        )

        binding = load_run_input_binding(self.shadow_root, "run-binding")

        self.assertEqual(binding["run_id"], "run-binding")
        self.assertEqual(binding["universe"], self.universe)
        self.assertEqual(
            binding["candidate_source_generation"],
            self.universe["candidate_source_generation"],
        )
        self.assertEqual(
            binding["route_universe_sha256"],
            route_universe_sha256(self.universe),
        )
        self.assertEqual(
            binding["baseline_manifest_sha256"],
            hashlib.sha256(
                canonical_bytes(
                    {
                        **self.manifest,
                        "route_universe_sha256": route_universe_sha256(
                            self.universe
                        ),
                    }
                )
            ).hexdigest(),
        )

    def test_run_input_binding_rejects_hardlinked_member(self):
        write_run_universe(
            self.shadow_root, "run-hardlink", self.universe, self.manifest
        )
        run_dir = self.shadow_root / "runs/run-hardlink"
        os.link(
            run_dir / "route_universe.json",
            Path(self.temporary.name) / "route-universe-hardlink.json",
        )

        with self.assertRaisesRegex(ValueError, "hard|link|unsafe"):
            load_run_input_binding(self.shadow_root, "run-hardlink")


class CurrentSourceGenerationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture = ProductionInputFixture(self.temporary.name)

    def test_current_generation_rehashes_the_exact_eleven_inputs_without_building(self):
        _universe, manifest = self.fixture.build()
        with patch.object(
            route_shadow_inputs,
            "build_shadow_universe",
            side_effect=AssertionError("must not rebuild universe"),
        ), patch.object(
            route_shadow_inputs, "PROJECT_ROOT", self.fixture.project_root
        ):
            actual = current_source_generation(
                self.fixture.data_dir,
                static_token_config=self.fixture.config_path,
            )
        self.assertEqual(actual, manifest["candidate_source_generation"])

        path = self.fixture.data_dir / "cex_depth_latest.csv"
        path.write_bytes(path.read_bytes() + b"\n")
        with patch.object(
            route_shadow_inputs, "PROJECT_ROOT", self.fixture.project_root
        ):
            self.assertNotEqual(
                current_source_generation(
                    self.fixture.data_dir,
                    static_token_config=self.fixture.config_path,
                ),
                actual,
            )


class TypedSourceLineageTests(unittest.TestCase):
    def _cex_lineage(self):
        return {
            "schema": "route_leg_typed_source_lineage/v1",
            "members": sorted([
                self.observed_member(
                    "cex_raw_book_response",
                    "fetch_cex_depth/parse_book/v1",
                    "route_bytes/v1",
                    "book.json",
                ),
                self.observed_member(
                    "cex_market_rules",
                    "route_quantity_quote_for_book/v1",
                    "route_market_rules_source/v1",
                    "rules.json",
                ),
                self.observed_member(
                    "quote_usd_conversion",
                    "route_usd_conversion_source/v1",
                    "route_usd_conversion_source/v1",
                    "usd.json",
                ),
            ], key=lambda row: row["role"]),
        }

    def test_hash_fields_require_lowercase_hex_strings_not_string_coercion(self):
        for field in ("sha256", "logical_generation"):
            for invalid in (int("1" * 64), b"1" * 64):
                with self.subTest(field=field, invalid_type=type(invalid).__name__):
                    lineage = self._cex_lineage()
                    lineage["members"][0][field] = invalid
                    with self.assertRaisesRegex(ValueError, "typed-source"):
                        validate_typed_source_lineage(
                            lineage, market_type="cex"
                        )

    def observed_member(self, role, adapter_id, content_schema, filename):
        return {
            "role": role,
            "status": "observed",
            "reason_code": None,
            "filename": filename,
            "sha256": "a" * 64,
            "size": 17,
            "logical_generation": "b" * 64,
            "adapter_id": adapter_id,
            "content_schema": content_schema,
        }

    def test_exact_cex_lineage_and_observed_projection_are_canonical(self):
        members = [
            self.observed_member(
                "quote_usd_conversion",
                "route_usd_conversion_source/v1",
                "route_usd_conversion_source/v1",
                "usd.json",
            ),
            self.observed_member(
                "cex_raw_book_response",
                "fetch_cex_depth/parse_book/v1",
                "route_bytes/v1",
                "book.json",
            ),
            self.observed_member(
                "cex_market_rules",
                "route_quantity_quote_for_book/v1",
                "route_market_rules_source/v1",
                "rules.json",
            ),
        ]
        lineage = {
            "schema": "route_leg_typed_source_lineage/v1",
            "members": sorted(members, key=lambda row: row["role"]),
        }

        validated = validate_typed_source_lineage(
            lineage, market_type="cex"
        )
        self.assertEqual(validated, lineage)
        self.assertIsNot(validated, lineage)
        self.assertEqual(
            typed_source_lineage_observed_members(
                validated, market_type="cex"
            ),
            [
                {
                    field: member[field]
                    for field in (
                        "role", "filename", "sha256", "size",
                        "logical_generation", "adapter_id", "content_schema",
                    )
                }
                for member in lineage["members"]
            ],
        )

    def test_unavailable_member_has_exact_null_and_reason_matrix(self):
        lineage = {
            "schema": "route_leg_typed_source_lineage/v1",
            "members": [
                {
                    "role": "dex_pool_state",
                    "status": "unavailable",
                    "reason_code": "typed_source_failed",
                    "filename": None,
                    "sha256": None,
                    "size": None,
                    "logical_generation": None,
                    "adapter_id": "route_quantity_quote_for_v2_pool/v1",
                    "content_schema": "route_v2_pool_state/v1",
                },
                {
                    "role": "dex_usd_price_context",
                    "status": "unavailable",
                    "reason_code": "typed_source_missing",
                    "filename": None,
                    "sha256": None,
                    "size": None,
                    "logical_generation": None,
                    "adapter_id": "route_dex_usd_price_context/v1",
                    "content_schema": "route_dex_usd_price_context/v1",
                },
            ],
        }
        self.assertEqual(
            validate_typed_source_lineage(lineage, market_type="dex"),
            lineage,
        )
        self.assertEqual(
            typed_source_lineage_observed_members(
                lineage, market_type="dex"
            ),
            [],
        )

    def test_cross_market_role_unsafe_name_and_wrong_contract_fail(self):
        base_members = [
            self.observed_member(
                "cex_raw_book_response",
                "fetch_cex_depth/parse_book/v1",
                "route_bytes/v1",
                "book.json",
            ),
            self.observed_member(
                "cex_market_rules",
                "route_quantity_quote_for_book/v1",
                "route_market_rules_source/v1",
                "rules.json",
            ),
            self.observed_member(
                "quote_usd_conversion",
                "route_usd_conversion_source/v1",
                "route_usd_conversion_source/v1",
                "usd.json",
            ),
        ]
        cases = (
            ({**base_members[0], "role": "dex_pool_state"}, "role|market"),
            ({**base_members[0], "filename": "../book.json"}, "filename|basename"),
            ({**base_members[0], "adapter_id": "caller/adapter/v1"}, "adapter|contract"),
            ({**base_members[0], "size": 8 * 1024 * 1024 + 1}, "size|bytes"),
        )
        for member, message in cases:
            with self.subTest(member=member):
                mutated = [member] + base_members[1:]
                mutated.sort(key=lambda row: row["role"])
                with self.assertRaisesRegex(ValueError, message):
                    validate_typed_source_lineage(
                        {
                            "schema": "route_leg_typed_source_lineage/v1",
                            "members": mutated,
                        },
                        market_type="cex",
                    )


if __name__ == "__main__":
    unittest.main()
