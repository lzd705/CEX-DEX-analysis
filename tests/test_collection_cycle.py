import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts.run_collection_cycle import (
    build_collection_status,
    build_step_commands,
    resolve_incremental_window,
    run_collection_cycle,
)


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CollectionCycleTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.data_dir = self.root / "data"
        write_csv(
            self.data_dir / "cex_exchange_volume_daily.csv",
            ["date", "token_symbol"],
            [{"date": "2026-07-24", "token_symbol": "UNI"}],
        )
        write_csv(
            self.data_dir / "dex_pool_volume_daily.csv",
            ["date", "token_symbol"],
            [{"date": "2026-07-22", "token_symbol": "UNI"}],
        )
        write_csv(
            self.data_dir / "dex_pool_tvl_latest.csv",
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "tvl-1",
                    "observed_at": "2026-07-27T11:00:00+00:00",
                    "status": "observed",
                }
            ],
        )
        write_csv(
            self.data_dir / "cex_depth_latest.csv",
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-1",
                    "observed_at": "2026-07-27T10:30:00+00:00",
                    "status": "partial",
                }
            ],
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_incremental_window_starts_from_lagging_source_with_overlap(self):
        self.assertEqual(
            resolve_incremental_window(self.data_dir, now=NOW),
            ("2026-07-20", "2026-07-26"),
        )

    def test_full_profile_builds_incremental_daily_tvl_and_depth_commands(self):
        commands = build_step_commands(
            "full",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
            tokens=["UNI", "AAVE"],
        )

        self.assertEqual([name for name, _ in commands], ["daily", "tvl", "depth"])
        daily = commands[0][1]
        self.assertIn("--append", daily)
        self.assertEqual(daily[daily.index("--tokens") + 1], "UNI,AAVE")
        self.assertEqual(daily[daily.index("--start") + 1], "2026-07-20")
        self.assertEqual(daily[daily.index("--end") + 1], "2026-07-26")
        self.assertTrue(all("--publish-local" in command for _, command in commands))

    def test_tvl_profile_builds_manual_recovery_command(self):
        commands = build_step_commands(
            "tvl",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
        )

        self.assertEqual([name for name, _ in commands], ["tvl"])
        self.assertIn("scripts/fetch_tvl.py", commands[0][1][1])
        self.assertIn("--publish-local", commands[0][1])

    def test_collection_status_keeps_source_specific_ranges(self):
        status = build_collection_status(self.data_dir, now=NOW)

        self.assertEqual(
            status["source_date_ranges"]["cex_daily"]["available_end"],
            "2026-07-24",
        )
        self.assertEqual(
            status["source_date_ranges"]["dex_daily"]["available_end"],
            "2026-07-22",
        )
        self.assertEqual(status["freshness"]["common_comparable_end"], "2026-07-22")
        self.assertEqual(status["tvl_snapshot"]["status_counts"], {"observed": 1})

    def test_successful_cycle_writes_per_step_logs_and_latest_manifest(self):
        def runner(command, log_path):
            log_path.write_text("ok\n", encoding="utf-8")
            return 0

        latest = self.root / "latest.json"
        result = run_collection_cycle(
            "depth",
            publish_local=True,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=latest,
            lock_path=self.root / "collection.lock",
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["steps"][0]["name"], "depth")
        self.assertEqual(result["steps"][0]["log_tail"], ["ok"])
        self.assertTrue(Path(result["manifest_path"]).exists())
        self.assertEqual(json.loads(latest.read_text())["status"], "succeeded")

    def test_fail_fast_records_failed_step(self):
        def runner(command, log_path):
            log_path.write_text("source failed\n", encoding="utf-8")
            return 2

        result = run_collection_cycle(
            "full",
            publish_local=True,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=self.root / "latest.json",
            lock_path=self.root / "collection.lock",
            now=NOW,
            fail_fast=True,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(result["steps"]), 1)
        self.assertEqual(result["steps"][0]["exit_code"], 2)

    def test_scheduled_daily_step_fails_when_published_sources_remain_stale(self):
        def runner(command, log_path):
            log_path.write_text("collector exited zero\n", encoding="utf-8")
            return 0

        result = run_collection_cycle(
            "daily",
            publish_local=True,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=self.root / "latest.json",
            lock_path=self.root / "collection.lock",
            now=NOW,
            fail_fast=True,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["steps"][0]["exit_code"], 3)
        self.assertEqual(result["steps"][0]["validation"]["status"], "failed")
        self.assertIn("dex_daily", result["steps"][0]["error"])


if __name__ == "__main__":
    unittest.main()
