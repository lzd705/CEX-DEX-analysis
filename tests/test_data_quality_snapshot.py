"""Contract tests for the publish-safe observed data-quality snapshot."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path


from scripts.data_quality_snapshot import build_snapshot, canonical_snapshot_bytes
from scripts.market_database import build_database


FAMILY_NAMES = [
    "cex_daily_ohlcv",
    "cex_depth",
    "cex_execution_cost",
    "cex_instrument_lifecycle",
    "dex_daily_ohlcv",
    "dex_depth",
    "dex_execution_cost",
    "event_facts",
    "market_lifecycle_reviews",
    "route_cohort_opportunity",
    "route_shadow_route_cost_evidence",
    "tvl",
]

CEX_HEADER = [
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
]
DEX_HEADER = [
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
]
TVL_HEADER = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "source_dex",
    "source_pool_name",
    "base_token_id",
    "quote_token_id",
    "tvl_usd",
    "base_token_price_usd",
    "quote_token_price_usd",
    "volume_24h_usd",
    "pool_created_at",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
]


def _write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _window_dates():
    first = date(2026, 7, 15)
    return [(first + timedelta(days=offset)).isoformat() for offset in range(30)]


def _cex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "BTC",
        "exchange": "Binance",
        "cex_symbol": "BTCUSDT",
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "base_volume": "2",
        "quote_volume_usd": "200",
    }
    row.update(overrides)
    return row


def _dex_row(day):
    return {
        "date": day,
        "token_symbol": "BTC",
        "chain": "ethereum",
        "dex": "uniswap_v3",
        "pool_address": "0x1111111111111111111111111111111111111111",
        "pool_name": "WBTC-USDC",
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "dex_volume_usd": "50",
        "pool_tvl_usd": "500",
    }


def _tvl_row(**overrides):
    row = {
        "snapshot_id": "snapshot-001",
        "observed_at": "2026-08-13T12:00:00Z",
        "request_started_at": "2026-08-13T11:59:58Z",
        "response_received_at": "2026-08-13T11:59:59Z",
        "token_symbol": "BTC",
        "chain": "ethereum",
        "dex": "uniswap_v3",
        "pool_address": "0x1111111111111111111111111111111111111111",
        "pool_name": "WBTC-USDC",
        "source_dex": "uniswap_v3",
        "source_pool_name": "WBTC-USDC",
        "base_token_id": "wbtc",
        "quote_token_id": "usdc",
        "tvl_usd": "5000000",
        "base_token_price_usd": "60000",
        "quote_token_price_usd": "1",
        "volume_24h_usd": "1000000",
        "pool_created_at": "2021-01-01T00:00:00Z",
        "tvl_method": "source_reported",
        "source": "geckoterminal",
        "source_endpoint": "https://example.invalid/pool",
        "raw_response_sha256": "b" * 64,
        "status": "observed",
        "reason_code": "observed",
        "error": "",
    }
    row.update(overrides)
    return row


def _build_bound_database(directory, cex_rows):
    data_dir = Path(directory)
    _write_csv(data_dir / "cex_exchange_volume_daily.csv", CEX_HEADER, cex_rows)
    _write_csv(data_dir / "dex_pool_volume_daily.csv", DEX_HEADER, [_dex_row(_window_dates()[0])])
    build_database(data_dir, data_dir / "market_facts.sqlite3")


def _rebind_cex_source(directory, rows):
    data_dir = Path(directory)
    path = data_dir / "cex_exchange_volume_daily.csv"
    _write_csv(path, CEX_HEADER, rows)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = sqlite3.connect(data_dir / "market_facts.sqlite3")
    try:
        connection.execute(
            """
            UPDATE dataset_snapshots
            SET cex_source_bytes = ?, cex_sha256 = ?, cex_row_count = ?
            WHERE snapshot_id = (SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)
            """,
            (path.stat().st_size, digest, len(rows)),
        )
        connection.commit()
    finally:
        connection.close()


def _update_database(directory, statements):
    connection = sqlite3.connect(Path(directory) / "market_facts.sqlite3")
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        for statement, parameters in statements:
            connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def _snapshot(directory):
    return build_snapshot(
        Path(directory),
        "2026-08-14T00:00:00Z",
        date(2026, 8, 13),
        30,
        "a" * 40,
    )


def _family(snapshot, name):
    return next(family for family in snapshot["families"] if family["name"] == name)


class DataQualitySnapshotCoreTests(unittest.TestCase):
    def test_empty_directory_is_explicitly_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = build_snapshot(
                Path(directory),
                "2026-08-14T00:00:00Z",
                date(2026, 8, 13),
                30,
                "a" * 40,
            )

            self.assertEqual(snapshot["schema_version"], "data_quality_snapshot/v1")
            self.assertEqual(snapshot["generated_at_utc"], "2026-08-14T00:00:00Z")
            self.assertEqual(snapshot["application"], {"build_sha": "a" * 40})
            self.assertEqual(snapshot["window"], {
                "start_date": "2026-07-15",
                "end_date": "2026-08-13",
                "expected_days": 30,
                "timezone": "UTC",
            })
            self.assertEqual(
                [family["name"] for family in snapshot["families"]],
                FAMILY_NAMES,
            )
            self.assertEqual(snapshot["summary"], {
                "evaluated_family_count": 0,
                "failed_family_count": 0,
                "not_evaluated_family_count": 12,
                "total_family_count": 12,
            })
            for family in snapshot["families"]:
                self.assertEqual(family["state"], "not_evaluated")
                self.assertIsNone(family["failure_reason"])
                expected_reason = (
                    "route_pointer_missing"
                    if family["name"].startswith("route_")
                    else "source_file_missing"
                )
                self.assertEqual(family["not_evaluated_reason"], expected_reason)
                self.assertEqual(family["counts"], {
                    "expected": None,
                    "observed": None,
                    "usable": None,
                    "expected_basis": None,
                })
                self.assertIsNone(family["coverage_bps"])
                self.assertEqual(family["duplicate_primary_key"], {
                    "count": None,
                    "rate_bps": None,
                })
                self.assertEqual(family["required_field_null"], {
                    "count": None,
                    "rate_bps": None,
                })
                self.assertEqual(family["measurements"], {
                    "null_count": None,
                    "zero_count": None,
                    "fields": {},
                })
                self.assertEqual(family["status_counts"], {})
                self.assertEqual(family["reason_counts"], {})
                self.assertEqual(family["observation_time"], {
                    "min": None,
                    "max": None,
                    "freshness_lag_seconds": None,
                })
                self.assertIsNone(family["source"])

            self.assertRegex(snapshot["publication"]["identity"], r"^[0-9a-f]{64}$")
            self.assertRegex(snapshot["snapshot_sha256"], r"^[0-9a-f]{64}$")
            self.assertNotIn(str(Path(directory)), json.dumps(snapshot, sort_keys=True))


class DataQualitySnapshotDailyTests(unittest.TestCase):
    def test_daily_source_without_authoritative_inventory_is_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "cex_exchange_volume_daily.csv",
                CEX_HEADER,
                [_cex_row(_window_dates()[0])],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "not_evaluated")
            self.assertEqual(family["not_evaluated_reason"], "authoritative_inventory_missing")
            self.assertEqual(family["counts"]["expected"], None)
            self.assertEqual(family["coverage_bps"], None)

    def test_duplicate_primary_key_fails_with_measured_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            duplicate_rows = rows + [dict(rows[0])]
            _rebind_cex_source(directory, duplicate_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "duplicate_primary_key")
            self.assertEqual(family["duplicate_primary_key"], {"count": 1, "rate_bps": 323})

    def test_blank_required_identity_fails_without_echoing_value(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            broken_rows = [dict(row) for row in rows]
            broken_rows[0]["cex_symbol"] = ""
            _rebind_cex_source(directory, broken_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "required_field_null")
            self.assertEqual(family["required_field_null"]["count"], 1)
            self.assertNotIn(str(Path(directory)), json.dumps(family, sort_keys=True))

    def test_null_measurement_and_real_zero_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows[3]["quote_volume_usd"] = "0"
            rows[7]["quote_volume_usd"] = ""
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"], {
                "expected": 30,
                "observed": 30,
                "usable": 29,
                "expected_basis": family["counts"]["expected_basis"],
            })
            self.assertEqual(family["measurements"]["fields"]["quote_volume_usd"], {
                "null_count": 1,
                "zero_count": 1,
            })
            self.assertEqual(family["measurements"]["null_count"], 1)
            self.assertEqual(family["measurements"]["zero_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)

    def test_interior_missing_date_is_incomplete_even_with_both_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates() if day != "2026-07-29"]
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 29)
            self.assertEqual(family["coverage_bps"], 9667)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "incomplete")
            self.assertEqual(family["daily_coverage"]["incomplete_market_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"]["missing_unexplained"],
                1,
            )
            incomplete = family["daily_coverage"]["incomplete_markets"]
            self.assertEqual(len(incomplete), 1)
            self.assertRegex(incomplete[0]["market_identity_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(incomplete[0]["missing_date_count"], 1)
            self.assertNotIn("BTCUSDT", json.dumps(incomplete, sort_keys=True))

    def test_one_day_does_not_pass_a_thirty_day_window(self):
        with tempfile.TemporaryDirectory() as directory:
            _build_bound_database(directory, [_cex_row("2026-08-13")])

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["coverage_bps"], 333)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "incomplete")
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"]["missing_unexplained"],
                29,
            )

    def test_complete_thirty_day_window_accepts_measured_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows[10]["quote_volume_usd"] = "0"
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 30)
            self.assertEqual(family["counts"]["usable"], 30)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "complete")
            self.assertEqual(family["daily_coverage"]["complete_market_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 1)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"],
                {
                    "collection_failed": 0,
                    "missing_unexplained": 0,
                    "observed": 30,
                    "post_delisting": 0,
                    "pre_listing": 0,
                    "source_no_observation": 0,
                    "structurally_unsupported": 0,
                },
            )

    def test_window_external_null_does_not_pollute_window_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows.append(_cex_row("2026-07-14", quote_volume_usd=""))
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["observed"], 30)
            self.assertEqual(family["counts"]["usable"], 30)
            self.assertEqual(family["measurements"]["null_count"], 0)
            self.assertEqual(family["required_field_null"], {"count": 0, "rate_bps": 0})

    def test_window_external_duplicate_primary_key_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows.append(_cex_row("2026-07-14"))
            _build_bound_database(directory, rows)
            _rebind_cex_source(directory, rows + [_cex_row("2026-07-14")])

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "duplicate_primary_key")

    def test_csv_extra_trailing_column_fails_before_database_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            path = Path(directory) / "cex_exchange_volume_daily.csv"
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    "2026-07-15,BTC,Binance,BTCUSDT,100,110,90,105,2,200\n",
                    "2026-07-15,BTC,Binance,BTCUSDT,100,110,90,105,2,200,tail\n",
                ),
                encoding="utf-8",
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "schema_mismatch")

    def test_database_run_must_bind_to_current_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            _update_database(
                directory,
                [
                    (
                        "UPDATE import_runs SET snapshot_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        ("f" * 24,),
                    )
                ],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")

    def test_database_secret_snapshot_id_is_never_published(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            secret = "api_key=top-secret/private/operator"
            _update_database(
                directory,
                [
                    (
                        "UPDATE dataset_snapshots SET snapshot_id = ? WHERE snapshot_id = "
                        "(SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    (
                        "UPDATE import_runs SET snapshot_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    ("UPDATE dataset_state SET snapshot_id = ? WHERE singleton_id = 1", (secret,)),
                ],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_database_secret_import_run_id_is_never_published(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            secret = "api_key=top-secret/private/operator"
            _update_database(
                directory,
                [
                    (
                        "UPDATE import_runs SET run_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    ("UPDATE dataset_state SET import_run_id = ? WHERE singleton_id = 1", (secret,)),
                ],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_database_declared_row_count_must_match_csv_and_table(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            _update_database(
                directory,
                [
                    (
                        "UPDATE dataset_snapshots SET cex_row_count = cex_row_count + 1 "
                        "WHERE snapshot_id = (SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)",
                        (),
                    )
                ],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")

    def test_database_market_inventory_must_match_bound_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            changed_rows = [dict(row, cex_symbol="BTCUSD") for row in rows]
            _rebind_cex_source(directory, changed_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_market_mismatch")


class DataQualitySnapshotPointInTimeTests(unittest.TestCase):
    def test_timezone_naive_timestamp_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-13T12:00:00")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "timezone_naive_timestamp")

    def test_future_timestamp_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-14T00:00:01Z")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "future_observation_timestamp")

    def test_latest_file_with_two_snapshot_ids_is_mixed_grain(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(), _tvl_row(snapshot_id="snapshot-002", pool_address="0x" + "2" * 40)],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "mixed_snapshot_id")

    def test_invalid_tvl_snapshot_id_fails_without_projecting_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = "api_key=top-secret /private/operator/token"
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(snapshot_id=secret)],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "invalid_snapshot_id")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_tvl_extra_trailing_column_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dex_pool_tvl_latest.csv"
            _write_csv(path, TVL_HEADER, [_tvl_row()])
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    ",observed,observed,\n", ",observed,observed,,tail\n"
                ),
                encoding="utf-8",
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "schema_mismatch")

    def test_stale_partition_remains_evaluated_and_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-12T00:00:00Z")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 1)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(family["observation_time"], {
                "min": "2026-08-12T00:00:00Z",
                "max": "2026-08-12T00:00:00Z",
                "freshness_lag_seconds": 172800,
            })
            self.assertEqual(
                family["reason_counts"],
                {"observed": 1, "stale_partition": 1},
            )


class DataQualitySnapshotDeterminismTests(unittest.TestCase):
    def test_identical_inputs_produce_identical_canonical_content_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            first = _snapshot(directory)
            second = _snapshot(directory)

            self.assertEqual(first, second)
            self.assertEqual(canonical_snapshot_bytes(first), canonical_snapshot_bytes(second))
            self.assertEqual(first["snapshot_sha256"], second["snapshot_sha256"])
            unsigned = dict(first)
            del unsigned["snapshot_sha256"]
            expected = hashlib.sha256(
                (
                    json.dumps(
                        unsigned,
                        ensure_ascii=False,
                        allow_nan=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
            ).hexdigest()
            self.assertEqual(first["snapshot_sha256"], expected)

    def test_publication_identity_changes_with_input_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dex_pool_tvl_latest.csv"
            _write_csv(path, TVL_HEADER, [_tvl_row(source_endpoint="https://example.invalid/one")])
            first = _snapshot(directory)
            _write_csv(path, TVL_HEADER, [_tvl_row(source_endpoint="https://example.invalid/two")])
            second = _snapshot(directory)

            self.assertNotEqual(first["publication"]["identity"], second["publication"]["identity"])

    def test_output_never_projects_private_path_cookie_or_api_key(self):
        with tempfile.TemporaryDirectory() as directory:
            private_path = str(Path(directory) / "operator" / "secret.json")
            secret = "api_key=top-secret cookie=session-secret " + private_path
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(error=secret, source_endpoint=secret)],
            )

            serialized = canonical_snapshot_bytes(_snapshot(directory)).decode("utf-8")

            self.assertNotIn("top-secret", serialized)
            self.assertNotIn("session-secret", serialized)
            self.assertNotIn(private_path, serialized)
            self.assertNotIn(str(Path(directory)), serialized)
            self.assertNotIn("api_key", serialized)
            self.assertNotIn("cookie", serialized)

    def test_cli_writes_the_exact_canonical_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "input"
            data_dir.mkdir()
            output = Path(directory) / "quality" / "latest.json"

            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/build_data_quality_snapshot.py",
                    "--data-dir",
                    str(data_dir),
                    "--generated-at-utc",
                    "2026-08-14T00:00:00Z",
                    "--window-end",
                    "2026-08-13",
                    "--window-days",
                    "30",
                    "--application-sha",
                    "a" * 40,
                    "--output",
                    str(output),
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            expected = canonical_snapshot_bytes(_snapshot(data_dir))
            self.assertEqual(output.read_bytes(), expected)
            self.assertNotIn(str(data_dir), output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
