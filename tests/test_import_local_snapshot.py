import csv
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path
from unittest.mock import patch

from scripts.import_local_snapshot import (
    DAILY_QUALITY_FILENAME,
    FILES,
    QUALITY_DIRECTORY,
    REJECTED_LATEST_FILENAME,
    REJECTED_QUALITY_DIRECTORY,
    REJECTED_QUALITY_FILENAME,
    import_snapshot,
)
from scripts.market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS


def cex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "AAVE",
        "exchange": "binance",
        "cex_symbol": "AAVE/USDT",
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "102",
        "base_volume": "10",
        "quote_volume_usd": "1020",
    }
    row.update(overrides)
    return row


def dex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "AAVE",
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0xAAVEPOOL",
        "pool_name": "AAVE / WETH",
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "101",
        "dex_volume_usd": "500",
        "pool_tvl_usd": "1000000",
    }
    row.update(overrides)
    return row


def write_csv(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_snapshot(source, *, cex_rows, dex_rows):
    write_csv(
        source / "cex_exchange_volume_daily.csv",
        CEX_COLUMNS,
        cex_rows,
    )
    write_csv(
        source / "dex_pool_volume_daily.csv",
        DEX_COLUMNS,
        dex_rows,
    )


def write_cex_attempts(source, attempts):
    csv_path = source / "cex_exchange_volume_daily.csv"
    (source / "cex_daily_collection_attempts.json").write_text(
        json.dumps(
            {
                "schema": "daily_collection_attempts/v1",
                "collector": "cex",
                "generated_at_utc": "2026-07-20T01:00:00+00:00",
                "requested_window": {
                    "start_date": attempts[0]["requested_start_date"],
                    "end_date": attempts[0]["requested_end_date"],
                },
                "source_csv": csv_path.name,
                "source_csv_sha256": hashlib.sha256(
                    csv_path.read_bytes()
                ).hexdigest(),
                "attempt_count": len(attempts),
                "attempts": attempts,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def cex_attempt(day, **overrides):
    row = {
        "attempt_id": "attempt-cex",
        "market_type": "cex",
        "token_symbol": "AAVE",
        "exchange": "binance",
        "instrument": "AAVE/USDT",
        "chain": None,
        "dex": None,
        "pool_address": None,
        "requested_start_date": day,
        "requested_end_date": day,
        "observed_dates": [],
        "observed_day_count": 0,
        "status": "failed",
        "outcome": "request_failed",
        "reason_code": "rate_limit",
        "http_status": 429,
        "error": "The source rejected the request because its rate limit was reached.",
        "finished_at_utc": "2026-07-20T00:30:00+00:00",
    }
    row.update(overrides)
    return row


def read_quality_report(target):
    return json.loads(
        (
            target
            / QUALITY_DIRECTORY
            / DAILY_QUALITY_FILENAME
        ).read_text(encoding="utf-8")
    )


def read_rejected_report(target):
    rejected_root = (
        target
        / QUALITY_DIRECTORY
        / REJECTED_QUALITY_DIRECTORY
    )
    pointer = json.loads(
        (rejected_root / REJECTED_LATEST_FILENAME).read_text(
            encoding="utf-8"
        )
    )
    report_path = rejected_root / pointer["report"]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return rejected_root, pointer, report_path, report


class ImportLocalSnapshotTest(unittest.TestCase):
    def test_lineage_matched_attempt_is_embedded_and_explains_retry(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[
                    cex_row("2026-07-16"),
                    cex_row("2026-07-17"),
                    cex_row("2026-07-18"),
                ],
                dex_rows=[
                    dex_row(
                        "2026-01-01",
                        token_symbol="UNI",
                        pool_address="0xUNIPOOL",
                        pool_name="UNI / WETH",
                    )
                ],
            )
            write_cex_attempts(
                source,
                [cex_attempt("2026-07-19")],
            )

            import_snapshot(
                source,
                target,
                quality_today=date(2026, 7, 20),
            )

            report = read_quality_report(target)
            retry = report["retry_queue"][0]
            self.assertEqual(retry["reason_code"], "rate_limit")
            self.assertEqual(retry["status"], "collection_failed")
            self.assertEqual(report["attempt_sources"][0]["status"], "accepted")
            self.assertEqual(
                report["collection_attempts"][0]["error"],
                "The source rejected the request because its rate limit was reached.",
            )

    def test_hard_rejection_report_retains_normalized_attempt_evidence(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[
                    cex_row(
                        "2026-07-19",
                        high="90",
                        low="110",
                    )
                ],
                dex_rows=[dex_row("2026-07-19")],
            )
            write_cex_attempts(
                source,
                [
                    cex_attempt(
                        "2026-07-19",
                        status="succeeded",
                        outcome="observed",
                        reason_code="observed",
                        http_status=None,
                        error=None,
                        observed_dates=["2026-07-19"],
                        observed_day_count=1,
                    )
                ],
            )

            with self.assertRaisesRegex(ValueError, "hard-invalid"):
                import_snapshot(
                    source,
                    target,
                    quality_today=date(2026, 7, 20),
                )

            _root, _pointer, _path, report = read_rejected_report(target)
            self.assertEqual(report["attempt_sources"][0]["status"], "accepted")
            self.assertEqual(len(report["collection_attempts"]), 1)
            self.assertEqual(
                report["collection_attempts"][0]["reason_code"],
                "observed",
            )

    def test_successful_import_publishes_database_and_daily_quality_report(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-01-01")],
                dex_rows=[dex_row("2026-01-01")],
            )

            counts = import_snapshot(
                source,
                target,
                quality_today=date(2026, 1, 10),
            )

            self.assertEqual(set(counts), set(FILES))
            self.assertTrue((target / "cex_exchange_volume_daily.csv").exists())
            self.assertTrue((target / "dex_pool_volume_daily.csv").exists())
            self.assertTrue((target / DATABASE_FILENAME).exists())
            report = read_quality_report(target)
            self.assertEqual(report["schema"], "fact_quality_report/v1")
            self.assertEqual(report["publication"]["status"], "published")
            self.assertTrue(report["publication"]["dataset_snapshot_id"])
            self.assertTrue(report["publication"]["import_run_id"])
            with closing(
                sqlite3.connect(target / DATABASE_FILENAME)
            ) as connection:
                database_state = connection.execute(
                    """
                    SELECT snapshot_id, import_run_id
                    FROM dataset_state
                    WHERE singleton_id = 1
                    """
                ).fetchone()
            self.assertEqual(
                database_state,
                (
                    report["publication"]["dataset_snapshot_id"],
                    report["publication"]["import_run_id"],
                ),
            )
            self.assertEqual(report["summary"]["backfill_pending_count"], 0)
            self.assertEqual(report["summary"]["retry_queue_count"], 0)
            self.assertEqual(report["retry_queue"], [])
            self.assertEqual(report["backfill_pending"], [])
            self.assertEqual(
                {item["name"] for item in report["sources"]},
                set(FILES),
            )
            self.assertTrue(
                all(
                    item["path"].startswith("data/local/")
                    and ".snapshot-" not in item["path"]
                    for item in report["sources"]
                )
            )
            for item in report["sources"]:
                published_file = target / item["name"]
                self.assertEqual(
                    item["sha256"],
                    hashlib.sha256(published_file.read_bytes()).hexdigest(),
                )
            self.assertEqual(
                list(target.glob(".snapshot-*")),
                [],
            )

    def test_quality_publication_failure_cannot_advance_sqlite_commit_point(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-01-01", close="100")],
                dex_rows=[dex_row("2026-01-01", close="100")],
            )
            import_snapshot(
                source,
                target,
                quality_today=date(2026, 1, 10),
            )
            database_before = (target / DATABASE_FILENAME).read_bytes()
            cex_before = (
                target / "cex_exchange_volume_daily.csv"
            ).read_bytes()
            dex_before = (
                target / "dex_pool_volume_daily.csv"
            ).read_bytes()
            quality_path = (
                target
                / QUALITY_DIRECTORY
                / DAILY_QUALITY_FILENAME
            )
            quality_before = quality_path.read_bytes()

            write_snapshot(
                source,
                cex_rows=[cex_row("2026-01-01", close="101")],
                dex_rows=[dex_row("2026-01-01", close="101")],
            )
            original_replace = Path.replace

            def fail_quality_replace(path, target_path):
                if (
                    path.name == DAILY_QUALITY_FILENAME
                    and Path(target_path) == quality_path
                ):
                    raise OSError("injected quality publication failure")
                return original_replace(path, target_path)

            with patch.object(Path, "replace", new=fail_quality_replace):
                with self.assertRaisesRegex(
                    OSError,
                    "injected quality publication failure",
                ):
                    import_snapshot(
                        source,
                        target,
                        quality_today=date(2026, 1, 10),
                    )

            self.assertEqual(
                (target / DATABASE_FILENAME).read_bytes(),
                database_before,
            )
            self.assertEqual(
                (target / "cex_exchange_volume_daily.csv").read_bytes(),
                cex_before,
            )
            self.assertEqual(
                (target / "dex_pool_volume_daily.csv").read_bytes(),
                dex_before,
            )
            self.assertEqual(quality_path.read_bytes(), quality_before)

    def test_hard_invalid_candidate_does_not_overwrite_published_snapshot(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-01-01")],
                dex_rows=[dex_row("2026-01-01")],
            )
            import_snapshot(
                source,
                target,
                quality_today=date(2026, 1, 10),
            )
            published_paths = [
                target / "cex_exchange_volume_daily.csv",
                target / "dex_pool_volume_daily.csv",
                target / DATABASE_FILENAME,
                target / QUALITY_DIRECTORY / DAILY_QUALITY_FILENAME,
            ]
            published_bytes = {
                path: path.read_bytes()
                for path in published_paths
            }

            write_snapshot(
                source,
                cex_rows=[
                    cex_row(
                        "2026-01-02",
                        high="90",
                        low="110",
                        quote_volume_usd="-1",
                    )
                ],
                dex_rows=[dex_row("2026-01-02")],
            )

            with self.assertRaisesRegex(ValueError, "hard-invalid"):
                import_snapshot(
                    source,
                    target,
                    quality_today=date(2026, 1, 10),
                )

            for path, expected in published_bytes.items():
                self.assertEqual(path.read_bytes(), expected)
            self.assertEqual(list(target.glob(".snapshot-*")), [])
            rejected_root, pointer, report_path, report = read_rejected_report(
                target
            )
            self.assertEqual(
                pointer["schema"],
                "fact_quality_rejection_pointer/v1",
            )
            self.assertEqual(
                pointer["report_sha256"],
                hashlib.sha256(report_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                report["rejection"]["status"],
                "rejected_hard_invalid",
            )
            self.assertTrue(
                report["rejection"]["published_snapshot_unchanged"]
            )
            self.assertTrue(
                report["rejection"][
                    "published_daily_quality_unchanged"
                ]
            )
            self.assertEqual(
                report["summary"]["manual_review_count"],
                report["summary"]["hard_invalid_count"],
            )
            self.assertIn(
                "invalid_non_negative_volume",
                {
                    issue["reason_code"]
                    for issue in report["issues"]
                },
            )
            evidence_bundle = report_path.parent
            for filename in FILES:
                self.assertEqual(
                    (evidence_bundle / filename).read_bytes(),
                    (source / filename).read_bytes(),
                )
            self.assertEqual(
                list(rejected_root.glob(".*.tmp")),
                [],
            )

    def test_future_date_and_negative_tvl_are_rejected_with_full_evidence(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-07-20")],
                dex_rows=[dex_row("2026-07-19", pool_tvl_usd="-1")],
            )

            with self.assertRaisesRegex(ValueError, "hard-invalid"):
                import_snapshot(
                    source,
                    target,
                    quality_today=date(2026, 7, 20),
                )

            self.assertFalse((target / DATABASE_FILENAME).exists())
            self.assertFalse(
                (
                    target
                    / QUALITY_DIRECTORY
                    / DAILY_QUALITY_FILENAME
                ).exists()
            )
            _, pointer, report_path, report = read_rejected_report(target)
            self.assertEqual(pointer["hard_invalid_count"], 2)
            self.assertEqual(
                {
                    issue["reason_code"]
                    for issue in report["issues"]
                },
                {
                    "incomplete_or_future_date",
                    "invalid_non_negative_pool_tvl",
                },
            )
            self.assertEqual(
                report["publication"]["status"],
                "blocked_hard_invalid",
            )
            self.assertEqual(
                {
                    path.name
                    for path in report_path.parent.iterdir()
                },
                {
                    REJECTED_QUALITY_FILENAME,
                    *FILES,
                },
            )

    def test_historical_holes_are_backfill_and_stale_market_needs_review(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-01-01", token_symbol="BTC", cex_symbol="BTC/USDT")],
                dex_rows=[
                    dex_row("2026-07-06"),
                    dex_row("2026-07-07"),
                    dex_row("2026-07-11"),
                    dex_row("2026-07-12"),
                ],
            )

            import_snapshot(
                source,
                target,
                quality_today=date(2026, 7, 20),
            )

            report = read_quality_report(target)
            self.assertEqual(
                [item["date"] for item in report["backfill_pending"]],
                ["2026-07-08", "2026-07-09", "2026-07-10"],
            )
            self.assertTrue(
                all(
                    item["status"] == "backfill_pending"
                    and item["reason_code"] == "missing_unexplained"
                    for item in report["backfill_pending"]
                )
            )
            self.assertEqual(report["retry_queue"], [])
            self.assertEqual(report["retry_windows_by_token"], {})
            self.assertEqual(
                report["manual_review_queue"][0]["reason_code"],
                "stale_market_lifecycle_unknown",
            )
            self.assertEqual(
                report["backfill_windows_by_token"]["AAVE"][0]["day_count"],
                3,
            )
            self.assertEqual(
                report["publication"]["status"],
                "published_with_backfill",
            )

    def test_active_d1_gap_is_published_into_explicit_retry_queue(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[
                    cex_row("2026-07-03"),
                    cex_row("2026-07-04"),
                    cex_row("2026-07-05"),
                    cex_row("2026-07-06"),
                    cex_row("2026-07-07"),
                    cex_row("2026-07-08"),
                ],
                dex_rows=[
                    dex_row(
                        "2026-01-01",
                        token_symbol="UNI",
                        pool_address="0xUNIPOOL",
                        pool_name="UNI / WETH",
                    )
                ],
            )

            import_snapshot(
                source,
                target,
                quality_today=date(2026, 7, 10),
            )

            report = read_quality_report(target)
            self.assertEqual(report["summary"]["retry_queue_count"], 1)
            retry = report["retry_queue"][0]
            self.assertEqual(retry["date"], "2026-07-09")
            self.assertEqual(retry["reason_code"], "missing_unexplained")
            self.assertEqual(retry["status"], "backfill_pending")
            self.assertNotEqual(retry["status"], "collection_failed")
            self.assertEqual(retry["queue_status"], "pending")
            self.assertEqual(retry["action"], "retry_daily_market_window")
            self.assertEqual(
                report["retry_windows_by_token"]["AAVE"][0]["start_date"],
                "2026-07-09",
            )
            self.assertEqual(
                report["publication"]["status"],
                "published_with_retry_queue",
            )

    def test_first_observation_does_not_create_prelisting_or_d1_failure(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            write_snapshot(
                source,
                cex_rows=[cex_row("2026-07-08")],
                dex_rows=[
                    dex_row(
                        "2026-01-01",
                        token_symbol="UNI",
                        pool_address="0xUNIPOOL",
                        pool_name="UNI / WETH",
                    )
                ],
            )

            import_snapshot(
                source,
                target,
                quality_today=date(2026, 7, 10),
            )

            report = read_quality_report(target)
            self.assertEqual(report["summary"]["historical_gap_count"], 0)
            self.assertEqual(report["summary"]["d1_active_gap_count"], 0)
            self.assertEqual(report["backfill_pending"], [])
            self.assertEqual(report["retry_queue"], [])
            self.assertEqual(report["publication"]["status"], "published")


if __name__ == "__main__":
    unittest.main()
