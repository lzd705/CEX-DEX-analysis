import fcntl
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts.run_exact_backfill import (
    QualityContractError,
    build_backfill_command,
    load_quality_snapshot,
    run_exact_backfill,
)


NOW = datetime(2026, 7, 30, 3, 0, tzinfo=timezone.utc)


def issue(
    issue_id,
    *,
    token,
    day,
    market_type,
    market_id,
    reason_code="missing_unexplained",
):
    return {
        "issue_id": issue_id,
        "category": "historical_gap",
        "status": "backfill_pending",
        "reason_code": reason_code,
        "retryable": True,
        "date": day,
        "market": {
            "token_symbol": token,
            "market_type": market_type,
            "market_id": market_id,
        },
    }


def exact_window(token, start, end, issues):
    market_types = sorted(
        {item["market"]["market_type"] for item in issues}
    )
    return {
        "token_symbol": token,
        "start_date": start,
        "end_date": end,
        "day_count": (
            datetime.fromisoformat(end) - datetime.fromisoformat(start)
        ).days
        + 1,
        "market_types": market_types,
        "market_ids": sorted(
            {item["market"]["market_id"] for item in issues}
        ),
        "reason_codes": sorted({item["reason_code"] for item in issues}),
        "issue_ids": sorted({item["issue_id"] for item in issues}),
        "_issues": issues,
    }


class ExactBackfillTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.data_dir = self.root / "runtime"
        self.run_root = self.root / "backfill-runs"
        self.lock_path = self.root / "collection.lock"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def publish(self, import_run_id, windows):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        snapshot_id = "snapshot-{}".format(import_run_id)
        database = sqlite3.connect(
            str(self.data_dir / "market_facts.sqlite3")
        )
        try:
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS dataset_state (
                    singleton_id INTEGER PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    import_run_id TEXT NOT NULL
                )
                """
            )
            database.execute(
                """
                INSERT OR REPLACE INTO dataset_state (
                    singleton_id,
                    snapshot_id,
                    import_run_id
                ) VALUES (1, ?, ?)
                """,
                (snapshot_id, import_run_id),
            )
            database.commit()
        finally:
            database.close()

        issues = []
        grouped = {}
        for source_window in windows:
            serialized = {
                key: value
                for key, value in source_window.items()
                if key != "_issues"
            }
            grouped.setdefault(source_window["token_symbol"], []).append(
                serialized
            )
            issues.extend(source_window["_issues"])
        report = {
            "schema": "fact_quality_report/v1",
            "publication": {
                "status": "published_with_backfill" if issues else "published",
                "dataset_snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
            },
            "issues": issues,
            "backfill_windows_by_token": grouped,
        }
        quality_path = self.data_dir / "quality/daily-latest.json"
        quality_path.parent.mkdir(parents=True, exist_ok=True)
        quality_path.write_text(
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def dex_window(token="ARB", day="2026-07-08", suffix="1"):
        item = issue(
            "dex-issue-{}".format(suffix),
            token=token,
            day=day,
            market_type="dex",
            market_id="dex:arbitrum:0xpool{}".format(suffix),
        )
        return exact_window(token, day, day, [item])

    @staticmethod
    def cex_window(token="LDO", day="2026-07-09", suffix="1"):
        item = issue(
            "cex-issue-{}".format(suffix),
            token=token,
            day=day,
            market_type="cex",
            market_id="cex:upbit:{}/USDT".format(token),
        )
        return exact_window(token, day, day, [item])

    def test_dry_run_is_read_only_and_scopes_dex_without_tvl_or_depth(self):
        self.publish("import-1", [self.dex_window()])

        result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=1,
            dry_run=True,
            run_root=self.run_root,
            lock_path=self.lock_path,
            python_executable="/usr/bin/python3",
            step_runner=lambda _command, _log: self.fail(
                "dry run must not invoke a collector"
            ),
        )

        self.assertEqual(result["status"], "dry_run")
        command = result["candidates"][0]["command"]
        self.assertIn("--dex-only", command)
        self.assertNotIn("--cex-only", command)
        self.assertIn("run_fact_pipeline.py", command[1])
        self.assertFalse(any("fetch_tvl.py" in item for item in command))
        self.assertFalse(any("depth" in item for item in command))
        self.assertFalse(self.run_root.exists())

    def test_dynamic_reload_runs_only_current_windows_and_verifies_progress(self):
        dex_window = self.dex_window(day="2026-07-08")
        cex_window = self.cex_window(day="2026-07-09")
        self.publish("import-1", [dex_window, cex_window])
        commands = []

        def runner(command, log_path):
            commands.append(list(command))
            log_path.write_text("collector ok\n", encoding="utf-8")
            if len(commands) == 1:
                self.publish("import-2", [cex_window])
            else:
                self.publish("import-3", [])
            return 0

        result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=2,
            run_root=self.run_root,
            lock_path=self.lock_path,
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "exhausted")
        self.assertEqual(len(commands), 2)
        self.assertIn("--dex-only", commands[0])
        self.assertNotIn("--cex-only", commands[0])
        self.assertIn("--cex-only", commands[1])
        self.assertNotIn("--dex-only", commands[1])
        self.assertEqual(
            [entry["status"] for entry in result["windows"]],
            ["progress", "progress"],
        )
        self.assertEqual(result["remaining_issue_count"], 0)
        state_path = Path(result["state_path"])
        self.assertTrue(state_path.exists())
        latest_path = self.run_root.parent / "latest.json"
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["run_id"],
            result["run_id"],
        )

    def test_changed_publication_without_selected_issue_progress_stops(self):
        window = self.dex_window()
        self.publish("import-1", [window])
        call_count = 0

        def runner(_command, log_path):
            nonlocal call_count
            call_count += 1
            log_path.write_text("empty response\n", encoding="utf-8")
            self.publish("import-2", [window])
            return 0

        result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=5,
            run_root=self.run_root,
            lock_path=self.lock_path,
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "no_progress")
        self.assertEqual(call_count, 1)
        self.assertEqual(result["windows"][0]["resolved_issue_ids"], [])
        self.assertEqual(
            result["windows"][0]["unresolved_issue_ids"],
            ["dex-issue-1"],
        )

    def test_unchanged_import_run_id_stops_even_if_collector_exits_zero(self):
        self.publish("import-1", [self.dex_window()])

        def runner(_command, log_path):
            log_path.write_text("no publication\n", encoding="utf-8")
            return 0

        result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=2,
            run_root=self.run_root,
            lock_path=self.lock_path,
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed_verification")
        self.assertIn(
            "import_run_id did not change",
            result["windows"][0]["verification_error"],
        )

    def test_nonzero_collector_exit_stops_before_another_window(self):
        self.publish(
            "import-1",
            [self.dex_window(), self.cex_window()],
        )
        calls = []

        def runner(command, log_path):
            calls.append(command)
            log_path.write_text("source failed\n", encoding="utf-8")
            return 7

        result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=2,
            run_root=self.run_root,
            lock_path=self.lock_path,
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed_collector")
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["windows"][0]["exit_code"], 7)

    def test_existing_collection_lock_prevents_backfill(self):
        self.publish("import-1", [self.dex_window()])
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(
                lock_handle.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
            result = run_exact_backfill(
                data_dir=self.data_dir,
                run_root=self.run_root,
                lock_path=self.lock_path,
                step_runner=lambda _command, _log: self.fail(
                    "locked executor must not invoke a collector"
                ),
            )

        self.assertEqual(result["status"], "skipped_locked")
        self.assertFalse(self.run_root.exists())

    def test_missing_market_types_is_rejected_fail_closed(self):
        window = self.dex_window()
        self.publish("import-1", [window])
        quality_path = self.data_dir / "quality/daily-latest.json"
        report = json.loads(quality_path.read_text(encoding="utf-8"))
        del report["backfill_windows_by_token"]["ARB"][0]["market_types"]
        quality_path.write_text(json.dumps(report), encoding="utf-8")

        with self.assertRaisesRegex(
            QualityContractError,
            "market_types",
        ):
            load_quality_snapshot(self.data_dir)

    def test_issue_and_window_market_scope_mismatch_is_rejected(self):
        window = self.dex_window()
        self.publish("import-1", [window])
        quality_path = self.data_dir / "quality/daily-latest.json"
        report = json.loads(quality_path.read_text(encoding="utf-8"))
        report["backfill_windows_by_token"]["ARB"][0]["market_types"] = ["cex"]
        quality_path.write_text(json.dumps(report), encoding="utf-8")

        with self.assertRaisesRegex(
            QualityContractError,
            "market_types do not match",
        ):
            load_quality_snapshot(self.data_dir)

    def test_mixed_scope_command_runs_both_daily_collectors_only(self):
        cex_issue = issue(
            "mixed-cex",
            token="ARB",
            day="2026-07-08",
            market_type="cex",
            market_id="cex:binance:ARB/USDT",
        )
        dex_issue = issue(
            "mixed-dex",
            token="ARB",
            day="2026-07-08",
            market_type="dex",
            market_id="dex:arbitrum:0xpool",
        )
        window = exact_window(
            "ARB",
            "2026-07-08",
            "2026-07-08",
            [cex_issue, dex_issue],
        )
        self.publish("import-1", [window])
        current = load_quality_snapshot(self.data_dir)["windows"][0]

        command = build_backfill_command(
            current,
            data_dir=self.data_dir,
        )

        self.assertNotIn("--cex-only", command)
        self.assertNotIn("--dex-only", command)
        self.assertIn("run_fact_pipeline.py", command[1])
        self.assertFalse(any("tvl" in value for value in command))
        self.assertFalse(any("depth" in value for value in command))

    def test_state_log_can_resume_after_a_bounded_batch(self):
        first = self.dex_window(day="2026-07-08", suffix="1")
        second = self.cex_window(day="2026-07-09", suffix="2")
        self.publish("import-1", [first, second])

        def first_runner(_command, log_path):
            log_path.write_text("first ok\n", encoding="utf-8")
            self.publish("import-2", [second])
            return 0

        first_result = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=1,
            run_root=self.run_root,
            lock_path=self.lock_path,
            now=NOW,
            step_runner=first_runner,
        )
        self.assertEqual(first_result["status"], "batch_limit_reached")

        def second_runner(_command, log_path):
            log_path.write_text("second ok\n", encoding="utf-8")
            self.publish("import-3", [])
            return 0

        resumed = run_exact_backfill(
            data_dir=self.data_dir,
            max_windows=1,
            resume_run_id=first_result["run_id"],
            run_root=self.run_root,
            lock_path=self.lock_path,
            step_runner=second_runner,
        )

        self.assertEqual(resumed["run_id"], first_result["run_id"])
        self.assertEqual(resumed["status"], "exhausted")
        self.assertEqual(len(resumed["batches"]), 2)
        self.assertEqual(len(resumed["windows"]), 2)
        self.assertEqual(
            [window["status"] for window in resumed["windows"]],
            ["progress", "progress"],
        )

    def test_batch_limit_is_bounded(self):
        self.publish("import-1", [])
        with self.assertRaisesRegex(ValueError, "between 1 and 50"):
            run_exact_backfill(
                data_dir=self.data_dir,
                max_windows=51,
                run_root=self.run_root,
                lock_path=self.lock_path,
            )


if __name__ == "__main__":
    unittest.main()
