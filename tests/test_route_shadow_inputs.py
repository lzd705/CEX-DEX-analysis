import csv
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

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
    selection_window,
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
        self.cex_rows = [
            {
                "date": "2026-07-02", "token_symbol": "UNI",
                "exchange": "binance", "cex_symbol": "UNI/USDT",
                "open": "7", "high": "8", "low": "6", "close": "7",
                "base_volume": "10", "quote_volume_usd": "999",
            },
            {
                "date": "2026-07-03", "token_symbol": "UNI",
                "exchange": "binance", "cex_symbol": "UNI/USDT",
                "open": "7", "high": "8", "low": "6", "close": "7",
                "base_volume": "0", "quote_volume_usd": "0",
            },
            {
                "date": "2026-08-01", "token_symbol": "UNI",
                "exchange": "binance", "cex_symbol": "UNI/USDT",
                "open": "7", "high": "8", "low": "6", "close": "7",
                "base_volume": "0", "quote_volume_usd": "0",
            },
        ]
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
            "target_token_address": "0x" + "1" * 40,
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


class ShadowInputBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture = ProductionInputFixture(self.temporary.name)

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
        self.assertEqual(universe["candidate_source_generation"], manifest[
            "candidate_source_generation"
        ])
        self.assertEqual(
            [entry["path"] for entry in manifest["inputs"]],
            list(REQUIRED_DATA_PATHS) + ["config/tokens.csv"],
        )
        self.assertFalse(any(
            str(self.fixture.root) in entry["path"] for entry in manifest["inputs"]
        ))
        self.assertEqual(manifest["observation_bounds"], {
            "start_inclusive": "2026-07-03T00:00:00Z",
            "end_exclusive": "2026-08-02T00:00:00Z",
        })

    def test_database_cex_source_sha_must_match_exact_captured_csv(self):
        path = self.fixture.data_dir / "cex_exchange_volume_daily.csv"
        path.write_bytes(path.read_bytes() + b"\n")

        with self.assertRaisesRegex(ValueError, "CEX CSV.*SHA|SHA.*CEX CSV"):
            self.fixture.build()

    def test_sqlite_parser_rejects_a_valid_private_copy_mutated_before_open(self):
        original = route_shadow_inputs._sqlite_capture_uri

        def uri_then_mutate(capture):
            uri = original(capture)
            original_bytes = os.pread(capture.descriptor, capture.identity.size, 0)
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "changed.sqlite3"
                path.write_bytes(original_bytes)
                connection = sqlite3.connect(str(path))
                try:
                    connection.execute(
                        "UPDATE import_runs SET imported_at = ? WHERE run_id = 'import-1'",
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
            return uri

        with patch.object(
            route_shadow_inputs,
            "_sqlite_capture_uri",
            side_effect=uri_then_mutate,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "SQLite.*capture|capture.*SQLite|source capture|binding",
            ):
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
        ) + self.fixture.config_path.stat().st_size
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

    def test_cex_volume_missing_then_numeric_is_numeric_not_zero_or_error(self):
        path = self.fixture.data_dir / "cex_exchange_volume_daily.csv"
        rows = list(self.fixture.cex_rows)
        rows[1] = {**rows[1], "quote_volume_usd": ""}
        rows[2] = {**rows[2], "quote_volume_usd": "5"}
        write_csv(path, list(rows[0]), rows)
        self.fixture.rebind_database_cex_source()

        universe, _manifest = self.fixture.build()

        cex = next(
            row for row in universe["selected_legs"] if row["market_type"] == "cex"
        )
        self.assertEqual(cex["selection_inputs"]["cex_selected_window_usd"], "5")

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
        self.assertEqual(len(manifest["inputs"]), 10)

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
        _universe, after = self.fixture.build()

        self.assertEqual(
            before["candidate_source_generation"],
            after["candidate_source_generation"],
        )

    def test_mutating_each_source_identity_changes_generation(self):
        identities = [
            SourceFileIdentity(path, index + 1, hashlib.sha256(path.encode()).hexdigest())
            for index, path in enumerate(list(REQUIRED_DATA_PATHS) + ["config/tokens.csv"])
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
            for index, path in enumerate(list(REQUIRED_DATA_PATHS) + ["config/tokens.csv"])
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


if __name__ == "__main__":
    unittest.main()
