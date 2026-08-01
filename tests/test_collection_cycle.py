import csv
import fcntl
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.run_collection_cycle import (
    build_collection_status,
    build_step_commands,
    configured_data_dir,
    parse_args,
    processed_dir_for,
    publication_gates_from_log,
    resolve_incremental_window,
    run_collection_cycle,
    snapshot_summary,
    validate_step_freshness,
)
from scripts.timestamp_contract import validate_observation_bounds


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
        (self.data_dir / "cex_instrument_lifecycle.json").write_text(
            json.dumps(
                {
                    "schema": "cex_instrument_lifecycle/v1",
                    "generated_at_utc": NOW.isoformat(),
                    "checked_at_utc": NOW.isoformat(),
                    "response_sha256": "a" * 64,
                    "inventory_count": 1,
                    "configured_market_count": 1,
                    "configured_market_ids_sha256": "a" * 64,
                    "review_count": 0,
                    "reviews": [],
                }
            ),
            encoding="utf-8",
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
            ["lifecycle", "daily", "depth", "tvl", "dex_depth"],
        )
        lifecycle = commands[0][1]
        self.assertIn("scripts/collect_cex_instrument_lifecycle.py", lifecycle[1])
        self.assertEqual(
            lifecycle[lifecycle.index("--manifest") + 1],
            str(self.data_dir.resolve() / "cex_instrument_lifecycle.json"),
        )
        self.assertEqual(
            lifecycle[lifecycle.index("--raw-root") + 1],
            str(
                self.data_dir.resolve()
                / "raw/cex-instrument-lifecycle"
            ),
        )
        daily = commands[1][1]
        self.assertIn("--append", daily)
        self.assertEqual(daily[daily.index("--tokens") + 1], "UNI,AAVE")
        self.assertEqual(daily[daily.index("--start") + 1], "2026-07-20")
        self.assertEqual(daily[daily.index("--end") + 1], "2026-07-26")
        self.assertEqual(
            daily[daily.index("--data-dir") + 1],
            str(self.data_dir.resolve()),
        )
        self.assertIn("--publish-local", daily)
        for _name, command in commands[2:]:
            self.assertEqual(
                command[command.index("--publish-dir") + 1],
                str(self.data_dir.resolve()),
            )
        expected_raw_roots = {
            "depth": "cex-depth",
            "tvl": "tvl",
            "dex_depth": "dex-depth",
        }
        for name, command in commands[2:]:
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

    def test_routes_profile_builds_one_bounded_publish_command(self):
        commands = build_step_commands(
            "routes",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            start="2026-07-01",
            end="2026-07-30",
            tokens=["AAVE", "UNI"],
            deadline_seconds=45.5,
        )

        self.assertEqual(
            commands,
            [
                (
                    "routes",
                    [
                        "python3",
                        str(
                            Path(__file__).resolve().parents[1]
                            / "scripts/collect_route_cohort.py"
                        ),
                        "--data-dir",
                        str(self.data_dir.resolve()),
                        "--start",
                        "2026-07-01",
                        "--end",
                        "2026-07-30",
                        "--tokens",
                        "AAVE,UNI",
                        "--deadline-seconds",
                        "45.5",
                        "--publish",
                    ],
                )
            ],
        )

    def test_routes_dry_run_forwards_deadline_without_enabling_publish(self):
        result = run_collection_cycle(
            "routes",
            publish_local=False,
            data_dir=self.data_dir,
            now=NOW,
            start="2026-07-01",
            end="2026-07-30",
            tokens=["AAVE"],
            deadline_seconds=12.5,
            dry_run=True,
        )

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(len(result["commands"]), 1)
        command = result["commands"][0]["command"]
        self.assertEqual(
            command[command.index("--deadline-seconds") + 1],
            "12.5",
        )
        self.assertNotIn("--publish", command)

    def test_routes_profile_rejects_nonpositive_or_nonfinite_deadline(self):
        for deadline_seconds in (0, -1, float("nan"), float("inf"), float("-inf")):
            with self.subTest(deadline_seconds=deadline_seconds):
                with self.assertRaisesRegex(ValueError, "deadline"):
                    build_step_commands(
                        "routes",
                        publish_local=False,
                        data_dir=self.data_dir,
                        deadline_seconds=deadline_seconds,
                    )

    def test_collection_cli_parses_routes_deadline(self):
        with patch(
            "sys.argv",
            [
                "run_collection_cycle.py",
                "--profile",
                "routes",
                "--deadline-seconds",
                "17.5",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.profile, "routes")
        self.assertEqual(args.deadline_seconds, 17.5)

    def test_routes_cycle_accepts_terminal_unavailable_report(self):
        def runner(_command, log_path):
            log_path.write_text(
                json.dumps(
                    {
                        "schema": "route_cohort_collection/v1",
                        "route_cohort_id": "cohort:" + "a" * 64,
                        "route_rows": [
                            {
                                "route_id": "route:deadline",
                                "timing_status": "unavailable",
                                "reason_code": "route_deadline_exceeded",
                            },
                            {
                                "route_id": "route:unsupported",
                                "timing_status": "unavailable",
                                "reason_code": "execution_adapter_unsupported",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            return 0

        result = run_collection_cycle(
            "routes",
            publish_local=False,
            data_dir=self.data_dir,
            run_root=self.root / "route-runs",
            latest_status_path=self.root / "route-latest.json",
            lock_path=self.root / "route.lock",
            now=NOW,
            deadline_seconds=5,
            step_runner=runner,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(
            result["steps"][0]["timing_status_counts"],
            {"outside_sla": 0, "unavailable": 2, "within_sla": 0},
        )

    def test_routes_cycle_fails_when_zero_exit_has_no_valid_report(self):
        invalid_reports = (
            "",
            "not-json",
            json.dumps({"schema": "route_cohort_collection/v1"}),
            json.dumps(
                {
                    "schema": "route_cohort_collection/v1",
                    "route_cohort_id": "cohort:" + "a" * 64,
                    "route_rows": [
                        {
                            "route_id": "route:bad",
                            "timing_status": "within_sla",
                            "reason_code": "route_deadline_exceeded",
                        }
                    ],
                }
            ),
        )
        for index, report in enumerate(invalid_reports):
            with self.subTest(report_index=index):
                def runner(_command, log_path, report=report):
                    log_path.write_text(report, encoding="utf-8")
                    return 0

                result = run_collection_cycle(
                    "routes",
                    publish_local=False,
                    data_dir=self.data_dir,
                    run_root=self.root / "invalid-route-runs" / str(index),
                    latest_status_path=self.root / "invalid-route-latest.json",
                    lock_path=self.root / "invalid-route.lock",
                    now=NOW,
                    step_runner=runner,
                )

                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["steps"][0]["exit_code"], 3)
                self.assertIn(
                    "Route cohort report validation failed",
                    result["steps"][0]["error"],
                )

    def test_routes_cycle_persists_failure_for_non_scalar_json_fields(self):
        def malformed_report(field, value):
            payload = {
                "schema": "route_cohort_collection/v1",
                "route_cohort_id": "cohort:" + "a" * 64,
                "route_rows": [
                    {
                        "route_id": "route:bad-shape",
                        "timing_status": "within_sla",
                        "reason_code": None,
                    }
                ],
            }
            if field == "route_row":
                payload["route_rows"][0] = value
            elif field in payload:
                payload[field] = value
            else:
                payload["route_rows"][0][field] = value
            return json.dumps(payload)

        invalid_fields = (
            ("schema", []),
            ("route_cohort_id", {}),
            ("route_rows", True),
            ("route_row", []),
            ("route_id", True),
            ("timing_status", []),
            ("timing_status", {}),
            ("timing_status", True),
            ("reason_code", []),
            ("reason_code", {}),
            ("reason_code", True),
        )
        for index, (field, value) in enumerate(invalid_fields):
            with self.subTest(field=field, value=value):
                report = malformed_report(field, value)

                def runner(_command, log_path, report=report):
                    log_path.write_text(report, encoding="utf-8")
                    return 0

                latest_status_path = (
                    self.root / "malformed-route-latest" / f"{index}.json"
                )
                result = run_collection_cycle(
                    "routes",
                    publish_local=False,
                    data_dir=self.data_dir,
                    run_root=self.root / "malformed-route-runs" / str(index),
                    latest_status_path=latest_status_path,
                    lock_path=self.root / "malformed-route.lock",
                    now=NOW,
                    step_runner=runner,
                )

                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["steps"][0]["exit_code"], 3)
                self.assertIn(
                    "Route cohort report validation failed",
                    result["steps"][0]["error"],
                )
                self.assertTrue(Path(result["manifest_path"]).is_file())
                self.assertEqual(
                    json.loads(latest_status_path.read_text(encoding="utf-8"))[
                        "status"
                    ],
                    "failed",
                )

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
        self.assertEqual(
            [name for name, _command in commands],
            ["lifecycle", "daily", "tvl"],
        )
        daily_tokens = set(
            commands[1][1][commands[1][1].index("--tokens") + 1].split(",")
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

    def test_public_cex_depth_refresh_is_bound_to_one_canonical_market(self):
        market_id = "cex:binance:AAVE/USDT"
        commands = build_step_commands(
            "cex_depth",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
            market_id=market_id,
        )

        self.assertEqual([name for name, _ in commands], ["depth"])
        command = commands[0][1]
        self.assertEqual(
            command[command.index("--market-id") + 1],
            market_id,
        )
        self.assertIn("--merge-publish", command)
        self.assertNotIn("--tokens", command)

    def test_public_dex_depth_refresh_bounds_price_and_depth_to_one_pool(self):
        market_id = (
            "dex:eth:uniswap_v3:"
            "0x1111111111111111111111111111111111111111:AAVE"
        )
        commands = build_step_commands(
            "dex_depth",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
            market_id=market_id,
        )

        self.assertEqual(
            [name for name, _ in commands],
            ["dex_price", "dex_depth"],
        )
        for _name, command in commands:
            self.assertEqual(
                command[command.index("--market-id") + 1],
                market_id,
            )
        self.assertNotIn("--merge-publish", commands[0][1])
        self.assertIn("--merge-publish", commands[1][1])

    def test_public_tvl_refresh_is_bound_to_one_canonical_pool(self):
        market_id = (
            "dex:eth:uniswap_v3:"
            "0x1111111111111111111111111111111111111111:AAVE"
        )
        commands = build_step_commands(
            "tvl",
            publish_local=True,
            python_executable="python3",
            data_dir=self.data_dir,
            now=NOW,
            market_id=market_id,
        )

        command = commands[0][1]
        self.assertEqual(
            command[command.index("--market-id") + 1],
            market_id,
        )
        self.assertIn("--merge-publish", command)

    def test_exact_market_scope_rejects_wrong_or_full_profiles(self):
        with self.assertRaisesRegex(ValueError, "DEX market"):
            build_step_commands(
                "cex_depth",
                publish_local=True,
                data_dir=self.data_dir,
                market_id="dex:eth:uniswap_v3:0xpool:AAVE",
            )
        with self.assertRaisesRegex(ValueError, "exact market"):
            build_step_commands(
                "full",
                publish_local=True,
                data_dir=self.data_dir,
                market_id="cex:binance:AAVE/USDT",
            )
        with self.assertRaisesRegex(ValueError, "publishing"):
            build_step_commands(
                "tvl",
                publish_local=False,
                data_dir=self.data_dir,
                market_id="dex:eth:uniswap_v3:0xpool:AAVE",
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
        self.assertEqual(
            status["cex_instrument_lifecycle"]["status"],
            "current",
        )
        self.assertEqual(
            status["cex_instrument_lifecycle"]["checked_at_utc"],
            NOW.isoformat(),
        )
        self.assertEqual(
            status["cex_instrument_lifecycle"]["response_sha256"],
            "a" * 64,
        )
        self.assertEqual(
            status["cex_instrument_lifecycle"]["inventory_count"],
            1,
        )
        self.assertEqual(
            status["cex_instrument_lifecycle"]["configured_market_count"],
            1,
        )
        self.assertEqual(
            status["cex_instrument_lifecycle"][
                "configured_market_ids_sha256"
            ],
            "a" * 64,
        )
        self.assertEqual(status["tvl_snapshot"]["status_counts"], {"observed": 1})
        self.assertEqual(
            status["cex_execution_cost_snapshot"]["source_snapshot_ids"],
            ["depth-1"],
        )

    def test_lifecycle_freshness_rejects_stale_or_future_manifest(self):
        manifest_path = self.data_dir / "cex_instrument_lifecycle.json"
        for generated_at, expected_status in (
            ("2026-07-25T00:00:00+00:00", "stale"),
            ("2026-07-27T12:10:00+00:00", "invalid"),
        ):
            with self.subTest(generated_at=generated_at):
                manifest_path.write_text(
                    json.dumps(
                        {
                            "schema": "cex_instrument_lifecycle/v1",
                            "generated_at_utc": generated_at,
                            "checked_at_utc": generated_at,
                            "response_sha256": "a" * 64,
                            "inventory_count": 1,
                            "configured_market_count": 1,
                            "configured_market_ids_sha256": "a" * 64,
                            "review_count": 0,
                            "reviews": [],
                        }
                    ),
                    encoding="utf-8",
                )

                status = build_collection_status(self.data_dir, now=NOW)

                self.assertEqual(
                    status["cex_instrument_lifecycle"]["status"],
                    expected_status,
                )
                self.assertEqual(
                    validate_step_freshness("lifecycle", status),
                    ["cex_instrument_lifecycle"],
                )

    def test_snapshot_freshness_uses_oldest_inventory_observation(self):
        write_csv(
            self.data_dir / "dex_pool_tvl_latest.csv",
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "tvl-2",
                    "observed_at": "2026-07-26T00:00:00+00:00",
                    "status": "observed",
                },
                {
                    "snapshot_id": "tvl-2",
                    "observed_at": "2026-07-27T11:59:00+00:00",
                    "status": "observed",
                },
            ],
        )

        status = build_collection_status(self.data_dir, now=NOW)

        self.assertEqual(
            status["tvl_snapshot"]["observed_at"],
            "2026-07-26T00:00:00+00:00",
        )
        self.assertEqual(
            status["tvl_snapshot"]["observed_at_min"],
            "2026-07-26T00:00:00+00:00",
        )
        self.assertEqual(
            status["tvl_snapshot"]["observed_at_max"],
            "2026-07-27T11:59:00+00:00",
        )
        self.assertEqual(status["freshness"]["dex_tvl"]["status"], "stale")

    def test_snapshot_summary_rejects_any_missing_inventory_observation(self):
        path = self.data_dir / "cex_depth_latest.csv"
        write_csv(
            path,
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-2",
                    "observed_at": "2026-07-27T10:30:00+00:00",
                    "status": "observed",
                },
                {
                    "snapshot_id": "depth-2",
                    "observed_at": "",
                    "status": "failed",
                },
            ],
        )

        with self.assertRaisesRegex(ValueError, "observed_at"):
            snapshot_summary(path, require_complete_observations=True)

    def test_collection_status_rejects_missing_tvl_inventory_observation(self):
        path = self.data_dir / "dex_pool_tvl_latest.csv"
        write_csv(
            path,
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "tvl-2",
                    "observed_at": "2026-07-27T10:30:00+00:00",
                    "status": "observed",
                },
                {
                    "snapshot_id": "tvl-2",
                    "observed_at": "",
                    "status": "failed",
                },
            ],
        )

        with self.assertRaisesRegex(ValueError, "observed_at"):
            build_collection_status(self.data_dir, now=NOW)

    def test_snapshot_summary_rejects_noncanonical_or_naive_observations(self):
        path = self.data_dir / "cex_depth_latest.csv"
        for observed_at in (
            "2026-07-27T10:30:00",
            "2026-07-27T10:30:00Z",
            "2026-07-27T18:30:00+08:00",
            " 2026-07-27T10:30:00+00:00",
        ):
            with self.subTest(observed_at=observed_at):
                write_csv(
                    path,
                    ["snapshot_id", "observed_at", "status"],
                    [
                        {
                            "snapshot_id": "depth-2",
                            "observed_at": observed_at,
                            "status": "observed",
                        }
                    ],
                )

                with self.assertRaisesRegex(ValueError, "observed_at"):
                    snapshot_summary(
                        path,
                        require_complete_observations=True,
                    )

    def test_snapshot_summary_bounds_exactly_span_every_inventory_member(self):
        path = self.data_dir / "cex_depth_latest.csv"
        write_csv(
            path,
            ["snapshot_id", "observed_at", "status"],
            [
                {
                    "snapshot_id": "depth-2",
                    "observed_at": "2026-07-27T10:30:05+00:00",
                    "status": "observed",
                },
                {
                    "snapshot_id": "depth-2",
                    "observed_at": "2026-07-27T10:29:59+00:00",
                    "status": "partial",
                },
                {
                    "snapshot_id": "depth-2",
                    "observed_at": "2026-07-27T10:30:02+00:00",
                    "status": "failed",
                },
            ],
        )

        summary = snapshot_summary(
            path,
            require_complete_observations=True,
        )

        self.assertIsNotNone(summary)
        self.assertEqual(
            summary["observed_at"],
            "2026-07-27T10:29:59+00:00",
        )
        self.assertEqual(
            summary["observed_at_min"],
            "2026-07-27T10:29:59+00:00",
        )
        self.assertEqual(
            summary["observed_at_max"],
            "2026-07-27T10:30:05+00:00",
        )

    def test_declared_observation_bounds_must_equal_validated_inventory(self):
        rows = [
            {"observed_at": "2026-07-27T10:30:00+00:00"},
            {"observed_at": "2026-07-27T10:30:05+00:00"},
        ]
        self.assertEqual(
            validate_observation_bounds(
                rows,
                declared_min="2026-07-27T10:30:00+00:00",
                declared_max="2026-07-27T10:30:05+00:00",
            ),
            (
                "2026-07-27T10:30:00+00:00",
                "2026-07-27T10:30:05+00:00",
            ),
        )
        for declared_min, declared_max in (
            (
                "2026-07-27T10:29:59+00:00",
                "2026-07-27T10:30:05+00:00",
            ),
            (
                "2026-07-27T10:30:00+00:00",
                "2026-07-27T10:30:04+00:00",
            ),
        ):
            with self.subTest(
                declared_min=declared_min,
                declared_max=declared_max,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "validated inventory|outside declared bounds",
                ):
                    validate_observation_bounds(
                        rows,
                        declared_min=declared_min,
                        declared_max=declared_max,
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

    def test_exact_market_cycle_defers_global_freshness_to_target_postcondition(self):
        def runner(_command, log_path):
            log_path.write_text("exact target published\n", encoding="utf-8")
            return 0

        result = run_collection_cycle(
            "cex_depth",
            publish_local=True,
            data_dir=self.data_dir,
            run_root=self.root / "exact-runs",
            latest_status_path=self.root / "exact-latest.json",
            lock_path=self.root / "exact.lock",
            now=NOW,
            market_id="cex:binance:AAVE/USDT",
            step_runner=runner,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["steps"][0]["exit_code"], 0)
        self.assertEqual(
            result["steps"][0]["validation"],
            {
                "checked": False,
                "reason": "non-publishing or bounded/manual refresh",
            },
        )

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

    def test_long_step_validates_snapshot_against_completion_clock(self):
        finished = NOW + timedelta(minutes=10)
        clock_calls = 0

        def advancing_clock():
            nonlocal clock_calls
            clock_calls += 1
            return NOW if clock_calls <= 2 else finished

        def runner(_command, log_path):
            write_csv(
                self.data_dir / "dex_pool_tvl_latest.csv",
                ["snapshot_id", "observed_at", "status"],
                [{
                    "snapshot_id": "tvl-after-long-step",
                    "observed_at": finished.isoformat(),
                    "status": "observed",
                }],
            )
            log_path.write_text("published\n", encoding="utf-8")
            return 0

        with patch(
            "scripts.run_collection_cycle.utc_now",
            side_effect=advancing_clock,
        ):
            result = run_collection_cycle(
                "tvl",
                publish_local=True,
                data_dir=self.data_dir,
                run_root=self.root / "long-runs",
                latest_status_path=self.root / "long-latest.json",
                lock_path=self.root / "long.lock",
                step_runner=runner,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["steps"][0]["exit_code"], 0)
        self.assertEqual(
            result["steps"][0]["validation"]["status"],
            "passed",
        )

    def test_post_publication_validation_error_is_recorded_in_manifest(self):
        def runner(_command, log_path):
            write_csv(
                self.data_dir / "dex_pool_tvl_latest.csv",
                ["snapshot_id", "observed_at", "status"],
                [{
                    "snapshot_id": "tvl-invalid-future",
                    "observed_at": "2099-01-01T00:00:00+00:00",
                    "status": "observed",
                }],
            )
            log_path.write_text("published\n", encoding="utf-8")
            return 0

        with patch(
            "scripts.run_collection_cycle.utc_now",
            return_value=NOW,
        ):
            result = run_collection_cycle(
                "tvl",
                publish_local=True,
                data_dir=self.data_dir,
                run_root=self.root / "invalid-runs",
                latest_status_path=self.root / "invalid-latest.json",
                lock_path=self.root / "invalid.lock",
                now=NOW,
                step_runner=runner,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["steps"][0]["exit_code"], 3)
        self.assertEqual(
            result["steps"][0]["validation"]["status"],
            "failed",
        )
        self.assertIn(
            "Post-publication validation failed",
            result["steps"][0]["error"],
        )
        self.assertTrue((self.root / "invalid-latest.json").is_file())

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
        self.assertEqual(result["steps"][0]["name"], "lifecycle")
        self.assertEqual(result["steps"][0]["exit_code"], 0)
        self.assertEqual(result["steps"][1]["name"], "daily")
        self.assertEqual(result["steps"][1]["exit_code"], 3)
        self.assertEqual(result["steps"][1]["validation"]["status"], "failed")
        self.assertIn("dex_daily", result["steps"][1]["error"])

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
