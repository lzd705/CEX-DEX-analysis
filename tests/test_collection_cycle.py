import csv
import fcntl
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.run_collection_cycle import (
    build_collection_status,
    build_step_commands,
    configured_data_dir,
    processed_dir_for,
    publication_gates_from_log,
    resolve_incremental_window,
    run_collection_cycle,
    validate_step_freshness,
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
        write_csv(
            self.data_dir / "dex_depth_latest.csv",
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "dex-depth-1",
                    "observed_at": "2026-07-27T10:45:00+00:00",
                    "status": "observed",
                }
            ],
        )
        write_csv(
            self.data_dir / "cex_execution_cost_latest.csv",
            ["snapshot_id", "source_snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-1",
                    "source_snapshot_id": "depth-1",
                    "observed_at": "2026-07-27T10:30:00+00:00",
                    "status": "observed",
                }
            ],
        )
        write_csv(
            self.data_dir / "dex_execution_cost_latest.csv",
            ["snapshot_id", "source_snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "dex-depth-1",
                    "source_snapshot_id": "dex-depth-1",
                    "observed_at": "2026-07-27T10:45:00+00:00",
                    "status": "observed",
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

    def test_market_data_environment_and_default_cycle_artifacts_share_one_root(self):
        with patch.dict(
            "os.environ",
            {"MARKET_DATA_DIR": str(self.data_dir)},
            clear=True,
        ):
            self.assertEqual(
                configured_data_dir(),
                self.data_dir.resolve(),
            )

        def runner(_command, log_path):
            log_path.write_text("ok\n", encoding="utf-8")
            return 0

        result = run_collection_cycle(
            "tvl",
            publish_local=False,
            data_dir=self.data_dir,
            now=NOW,
            step_runner=runner,
        )

        self.assertIn(
            self.data_dir.resolve() / "collection/runs",
            Path(result["manifest_path"]).parents,
        )
        self.assertTrue(
            (self.data_dir / "collection/latest.json").exists()
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

        self.assertEqual(
            [name for name, _ in commands],
            ["daily", "depth", "tvl", "dex_depth"],
        )
        daily = commands[0][1]
        self.assertIn("--append", daily)
        self.assertEqual(daily[daily.index("--tokens") + 1], "UNI,AAVE")
        self.assertEqual(daily[daily.index("--start") + 1], "2026-07-20")
        self.assertEqual(daily[daily.index("--end") + 1], "2026-07-26")
        self.assertEqual(
            daily[daily.index("--data-dir") + 1],
            str(self.data_dir.resolve()),
        )
        self.assertIn("--publish-local", daily)
        for _name, command in commands[1:]:
            self.assertEqual(
                command[command.index("--publish-dir") + 1],
                str(self.data_dir.resolve()),
            )
        expected_raw_roots = {
            "depth": "cex-depth",
            "tvl": "tvl",
            "dex_depth": "dex-depth",
        }
        for name, command in commands[1:]:
            self.assertEqual(
                command[command.index("--raw-root") + 1],
                str(self.data_dir.resolve() / "raw" / expected_raw_roots[name]),
            )
        self.assertTrue(
            any(
                item.endswith("scripts/fetch_dex_depth.py")
                for item in commands[-1][1]
            )
        )
        self.assertIn("--tvl-csv", commands[-1][1])

    def test_scheduled_daily_profile_includes_only_active_runtime_tokens(self):
        runtime_records = [
            {"token_symbol": "ACTIVE_RUNTIME", "status": "active"},
            {"token_symbol": "PENDING_RUNTIME", "status": "pending"},
            {"token_symbol": "FAILED_RUNTIME", "status": "failed"},
        ]
        with patch(
            "scripts.run_collection_cycle.TokenRegistry.list_records",
            return_value=runtime_records,
        ) as list_records:
            commands = build_step_commands(
                "daily",
                publish_local=False,
                python_executable="python3",
                data_dir=self.data_dir,
                now=NOW,
            )

        list_records.assert_called_once_with(statuses={"active"})
        daily_tokens = set(
            commands[0][1][commands[0][1].index("--tokens") + 1].split(",")
        )
        self.assertIn("ACTIVE_RUNTIME", daily_tokens)
        self.assertNotIn("PENDING_RUNTIME", daily_tokens)
        self.assertNotIn("FAILED_RUNTIME", daily_tokens)

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
        self.assertEqual(
            commands[0][1][commands[0][1].index("--publish-dir") + 1],
            str(self.data_dir.resolve()),
        )
        self.assertEqual(
            commands[0][1][commands[0][1].index("--raw-root") + 1],
            str(self.data_dir.resolve() / "raw/tvl"),
        )

    def test_hourly_depth_refreshes_private_price_input_before_dex(self):
        commands = build_step_commands(
            "depth",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
        )

        self.assertEqual(
            [name for name, _ in commands],
            ["depth", "dex_price", "dex_depth"],
        )
        price_command = commands[1][1]
        self.assertIn("scripts/fetch_tvl.py", price_command[1])
        self.assertNotIn("--publish-local", price_command)
        self.assertEqual(
            price_command[price_command.index("--raw-root") + 1],
            str(self.data_dir.resolve() / "raw/tvl"),
        )
        dex_command = commands[2][1]
        self.assertIn("--tvl-csv", dex_command)
        self.assertEqual(
            commands[0][1][commands[0][1].index("--raw-root") + 1],
            str(self.data_dir.resolve() / "raw/cex-depth"),
        )
        self.assertEqual(
            dex_command[dex_command.index("--raw-root") + 1],
            str(self.data_dir.resolve() / "raw/dex-depth"),
        )
        self.assertEqual(
            dex_command[dex_command.index("--tvl-csv") + 1],
            str(
                processed_dir_for(self.data_dir)
                / "dex_pool_tvl_snapshot.csv"
            ),
        )

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
        self.assertEqual(
            status["cex_execution_cost_snapshot"]["source_snapshot_ids"],
            ["depth-1"],
        )

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

    def test_locked_cycle_does_not_leave_an_empty_run_directory(self):
        lock_path = self.root / "collection.lock"
        run_root = self.root / "locked-runs"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = run_collection_cycle(
                "tvl",
                publish_local=False,
                data_dir=self.data_dir,
                run_root=run_root,
                latest_status_path=self.root / "locked-latest.json",
                lock_path=lock_path,
                now=NOW,
                step_runner=lambda _command, _log_path: self.fail(
                    "locked cycle must not run a collector"
                ),
            )

        self.assertEqual(result["status"], "skipped_locked")
        self.assertFalse(run_root.exists())

    def test_cycle_manifest_keeps_structured_publication_gate_evidence(self):
        gate = {
            "gate": "coverage_regression",
            "fact_family": "dex_tvl",
            "status": "passed",
        }

        def runner(command, log_path):
            log_path.write_text(
                "[1/1] source: observed\n"
                + json.dumps({"publication_gates": {"dex_tvl": gate}}),
                encoding="utf-8",
            )
            return 0

        result = run_collection_cycle(
            "tvl",
            publish_local=False,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=self.root / "latest.json",
            lock_path=self.root / "collection.lock",
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(
            result["steps"][0]["publication_gates"],
            {"dex_tvl": gate},
        )

    def test_rejected_publication_gate_is_parsed_from_traceback_log(self):
        gate = {
            "gate": "coverage_regression",
            "fact_family": "cex_depth",
            "status": "rejected",
        }
        log_path = self.root / "rejected.log"
        log_path.write_text(
            "Traceback (most recent call last):\n"
            "CoverageRegressionError: PUBLICATION_COVERAGE_GATE="
            + json.dumps(gate, separators=(",", ":"))
            + "\n",
            encoding="utf-8",
        )

        self.assertEqual(
            publication_gates_from_log(log_path),
            {"cex_depth": gate},
        )

    def test_rejected_bundle_keeps_every_family_report(self):
        gates = {
            "cex_depth": {
                "gate": "coverage_regression",
                "fact_family": "cex_depth",
                "status": "passed",
            },
            "cex_execution_cost": {
                "gate": "coverage_regression",
                "fact_family": "cex_execution_cost",
                "status": "rejected",
            },
        }
        bundle = {
            "gate": "coverage_regression_bundle",
            "bundle": "cex_depth_execution",
            "status": "rejected",
            "publication_gates": gates,
        }
        log_path = self.root / "rejected-bundle.log"
        log_path.write_text(
            "CoverageRegressionError: PUBLICATION_COVERAGE_GATE="
            + json.dumps(bundle, separators=(",", ":"))
            + "\n",
            encoding="utf-8",
        )

        self.assertEqual(publication_gates_from_log(log_path), gates)

    def test_passing_gate_is_kept_when_freshness_text_follows_json(self):
        gate = {
            "gate": "coverage_regression",
            "fact_family": "cex_depth",
            "status": "passed",
        }
        log_path = self.root / "trailing-text.log"
        log_path.write_text(
            "[1/1] source: observed\n"
            + json.dumps({"publication_gates": {"cex_depth": gate}}, indent=2)
            + "\nFreshness validation failed for: cex_depth\n",
            encoding="utf-8",
        )

        self.assertEqual(
            publication_gates_from_log(log_path),
            {"cex_depth": gate},
        )

    def test_rejected_gate_sets_structured_cycle_error(self):
        gate = {
            "gate": "coverage_regression",
            "fact_family": "dex_tvl",
            "status": "rejected",
        }

        def runner(command, log_path):
            log_path.write_text(
                "CoverageRegressionError: PUBLICATION_COVERAGE_GATE="
                + json.dumps(gate, separators=(",", ":"))
                + "\n",
                encoding="utf-8",
            )
            return 2

        result = run_collection_cycle(
            "tvl",
            publish_local=False,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=self.root / "latest.json",
            lock_path=self.root / "collection.lock",
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(
            result["steps"][0]["error"],
            "Publication coverage gate rejected: dex_tvl",
        )
        self.assertEqual(
            result["steps"][0]["publication_gates"],
            {"dex_tvl": gate},
        )

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

    def test_failed_price_refresh_skips_dependent_dex_collection(self):
        calls = []

        def runner(command, log_path):
            calls.append(command)
            log_path.write_text("fixture\n", encoding="utf-8")
            return 2 if "fetch_tvl.py" in command[1] else 0

        result = run_collection_cycle(
            "depth",
            publish_local=True,
            data_dir=self.data_dir,
            run_root=self.root / "runs",
            latest_status_path=self.root / "latest.json",
            lock_path=self.root / "collection.lock",
            now=NOW,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(
            [step["status"] for step in result["steps"]],
            ["succeeded", "failed", "skipped_dependency"],
        )
        self.assertEqual(len(calls), 2)
        self.assertIn(
            "required fresh DEX USD-price input",
            result["steps"][-1]["error"],
        )

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

    def test_depth_step_fails_when_matching_execution_snapshot_is_missing(self):
        (self.data_dir / "cex_execution_cost_latest.csv").unlink()

        def runner(command, log_path):
            log_path.write_text("collector exited zero\n", encoding="utf-8")
            return 0

        result = run_collection_cycle(
            "cex_depth",
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
        self.assertIn("cex_execution_cost", result["steps"][0]["error"])

    def test_fresh_all_failed_cex_snapshots_cannot_masquerade_as_success(self):
        write_csv(
            self.data_dir / "cex_depth_latest.csv",
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-failed",
                    "observed_at": "2026-07-27T11:30:00+00:00",
                    "status": "failed",
                }
            ],
        )
        write_csv(
            self.data_dir / "cex_execution_cost_latest.csv",
            ["snapshot_id", "source_snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-failed",
                    "source_snapshot_id": "depth-failed",
                    "observed_at": "2026-07-27T11:30:00+00:00",
                    "status": "failed",
                }
            ],
        )

        status = build_collection_status(self.data_dir, now=NOW)
        self.assertEqual(status["freshness"]["cex_depth"]["status"], "current")
        invalid = validate_step_freshness("depth", status)

        self.assertIn("cex_depth_no_measured_rows", invalid)
        self.assertIn("cex_execution_cost_no_measured_rows", invalid)

    def test_dex_unsupported_execution_is_truthful_but_all_failed_is_not(self):
        write_csv(
            self.data_dir / "dex_execution_cost_latest.csv",
            ["snapshot_id", "source_snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "dex-depth-1",
                    "source_snapshot_id": "dex-depth-1",
                    "observed_at": "2026-07-27T10:45:00+00:00",
                    "status": "unsupported",
                }
            ],
        )
        unsupported_status = build_collection_status(self.data_dir, now=NOW)
        self.assertNotIn(
            "dex_execution_cost_supported_rows_all_failed",
            validate_step_freshness("dex_depth", unsupported_status),
        )

        write_csv(
            self.data_dir / "dex_execution_cost_latest.csv",
            ["snapshot_id", "source_snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "dex-depth-1",
                    "source_snapshot_id": "dex-depth-1",
                    "observed_at": "2026-07-27T10:45:00+00:00",
                    "status": "failed",
                }
            ],
        )
        failed_status = build_collection_status(self.data_dir, now=NOW)

        self.assertIn(
            "dex_execution_cost_supported_rows_all_failed",
            validate_step_freshness("dex_depth", failed_status),
        )


if __name__ == "__main__":
    unittest.main()
