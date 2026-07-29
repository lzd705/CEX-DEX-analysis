import json
import os
import fcntl
import hashlib
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dashboard import server
from dashboard.admin import (
    AdminService,
    MAX_QUALITY_REPORT_BYTES,
    QUALITY_REPORT_SCHEMA,
    hash_password,
    password_hash_is_configured,
    verify_password,
)
from scripts.retain_cex_depth_raw import apply_retention, plan_retention


def write_retry_database(path, import_run_id, *, cex_rows=()):
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE dataset_state "
        "(singleton_id INTEGER PRIMARY KEY, import_run_id TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO dataset_state VALUES (1, ?)",
        (import_run_id,),
    )
    connection.execute(
        """
        CREATE TABLE cex_market_daily (
            date TEXT, token_symbol TEXT, exchange TEXT, cex_symbol TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE dex_pool_daily (
            date TEXT, token_symbol TEXT, chain TEXT, dex TEXT,
            pool_address TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO cex_market_daily VALUES (?, ?, ?, ?)",
        list(cex_rows),
    )
    connection.commit()
    connection.close()


class AdminServiceTest(unittest.TestCase):
    @staticmethod
    def runtime_record(
        *,
        status="active",
        symbol="XYZ",
        address="0x" + "12" * 20,
    ):
        return {
            "token_symbol": symbol,
            "token_name": f"{symbol} Token",
            "chain": "base",
            "contract_address": address,
            "decimals": 18,
            "coingecko_id": None,
            "source": "geckoterminal",
            "source_token_id": f"base_{address}",
            "status": status,
            "cex_mapping": {
                "status": "requires_manual_review",
                "cex_symbol": None,
                "exchanges": [],
            },
            "created_at": "2026-07-29T00:00:00+00:00",
            "created_by": "admin",
            "activated_at": (
                "2026-07-29T00:01:00+00:00"
                if status == "active"
                else None
            ),
            "last_job_id": None,
        }

    def test_quality_report_reader_rejects_oversized_file_with_stable_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "quality/daily-latest.json"
            report_path.parent.mkdir(parents=True)
            report_path.write_bytes(b"{" + b"x" * MAX_QUALITY_REPORT_BYTES)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                quality_report_path=report_path,
            )

            with self.assertRaises(ValueError) as context:
                service._read_quality_report(required=True)

            self.assertEqual(
                str(context.exception),
                "Daily quality report exceeds the operator size limit",
            )
            self.assertNotIn(str(root), str(context.exception))

    def test_quality_report_reader_enforces_schema_and_basic_structure(self):
        valid_base = {
            "schema": QUALITY_REPORT_SCHEMA,
            "publication": {},
            "issues": [],
            "retry_windows_by_token": {},
            "backfill_windows_by_token": {},
            "manual_review_queue": [],
        }
        invalid_reports = [
            {**valid_base, "schema": "fact_quality_report/v999"},
            {**valid_base, "publication": []},
            {**valid_base, "issues": {}},
            {**valid_base, "issues": ["not-an-object"]},
            {
                **valid_base,
                "retry_windows_by_token": {"AAVE": "not-a-window-list"},
            },
            {**valid_base, "manual_review_queue": ["not-an-object"]},
        ]
        for index, payload in enumerate(invalid_reports):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report_path = root / "daily-latest.json"
                report_path.write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )
                service = AdminService(
                    username="admin",
                    password_hash="hash",
                    job_dir=root / "jobs",
                    quality_report_path=report_path,
                )

                with self.assertRaises(ValueError) as context:
                    service._read_quality_report(required=True)

                self.assertEqual(
                    str(context.exception),
                    "Daily quality report has an invalid contract",
                )

    def test_password_hash_round_trip(self):
        encoded = hash_password("a-secure-test-password", salt=b"0123456789abcdef", iterations=10_000)

        self.assertTrue(verify_password("a-secure-test-password", encoded))
        self.assertFalse(verify_password("wrong-password-value", encoded))
        self.assertNotIn("a-secure-test-password", encoded)

    def test_login_creates_expiring_server_side_session(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="research-admin",
                password_hash=hash_password("a-secure-test-password"),
                job_dir=Path(directory),
                login_required=True,
                enabled=True,
            )

            token, public_session = service.login(
                "127.0.0.1",
                "research-admin",
                "a-secure-test-password",
            )

            self.assertTrue(public_session["authenticated"])
            self.assertNotIn(token, public_session.values())
            self.assertEqual(service.get_session(token)["username"], "research-admin")
            service.logout(token)
            self.assertIsNone(service.get_session(token))

    def test_admin_surface_is_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="research-admin",
                password_hash=hash_password("a-secure-test-password"),
                job_dir=Path(directory),
                login_required=True,
                enabled=False,
            )

            self.assertFalse(service.available)
            self.assertFalse(service.public_session(None)["authenticated"])
            with self.assertRaises(RuntimeError):
                service.login("127.0.0.1", "research-admin", "a-secure-test-password")

    def test_legacy_no_login_flag_cannot_reopen_admin_by_itself(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"ADMIN_LOGIN_REQUIRED": "false"},
                clear=True,
            ):
                service = AdminService(job_dir=Path(directory))

            self.assertFalse(service.enabled)
            self.assertFalse(service.open_mode)
            self.assertFalse(service.available)

    def test_open_mode_requires_two_explicit_local_development_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            unsafe_service = AdminService(
                job_dir=Path(directory),
                login_required=False,
                enabled=True,
                allow_open_local=False,
            )
            service = AdminService(
                job_dir=Path(directory),
                login_required=False,
                enabled=True,
                allow_open_local=True,
            )

            session = service.public_session(None)

            self.assertFalse(unsafe_service.available)
            self.assertTrue(session["authenticated"])
            self.assertTrue(session["configured"])
            self.assertFalse(session["login_required"])
            self.assertEqual(session["username"], "open-admin")

    def test_malformed_or_placeholder_password_hash_does_not_enable_admin(self):
        self.assertTrue(password_hash_is_configured(hash_password("a-secure-test-password")))
        self.assertFalse(password_hash_is_configured(""))
        self.assertFalse(
            password_hash_is_configured(
                "pbkdf2_sha256$600000$replace_salt$replace_digest"
            )
        )

    def test_admin_routes_are_recognized_and_public_binding_is_rejected(self):
        self.assertTrue(server.is_admin_surface_path("/admin.html"))
        self.assertTrue(server.is_admin_surface_path("/ADMIN.HTML"))
        self.assertTrue(server.is_admin_surface_path("/%61dmin.html"))
        self.assertTrue(server.is_admin_surface_path("/assets/../admin.html"))
        self.assertTrue(server.is_admin_surface_path("/api/admin/session"))
        self.assertFalse(server.is_admin_surface_path("/api/market"))
        self.assertTrue(server.is_loopback_host("127.0.0.1"))
        self.assertTrue(server.is_loopback_host("::1"))
        self.assertFalse(server.is_loopback_host("0.0.0.0"))
        self.assertFalse(server.is_loopback_host("43.156.102.166"))

    def test_disabled_admin_page_and_api_return_404_but_local_open_mode_is_tested(self):
        with tempfile.TemporaryDirectory() as directory:
            disabled = AdminService(
                job_dir=Path(directory),
                enabled=False,
            )
            local_open = AdminService(
                job_dir=Path(directory),
                enabled=True,
                login_required=False,
                allow_open_local=True,
            )
            handler = object.__new__(server.MarketMonitorHandler)
            handler.server = SimpleNamespace(server_address=("127.0.0.1", 8765))
            handler.send_error = Mock()
            handler.send_json = Mock()
            handler.admin_session_token = Mock(return_value=None)

            with patch.object(server, "ADMIN_SERVICE", disabled):
                handler.path = "/admin.html"
                handler.do_GET()
                handler.send_error.assert_called_once_with(server.HTTPStatus.NOT_FOUND)
                handler.send_error.reset_mock()

                handler.path = "/api/admin/session"
                handler.do_GET()
                handler.send_json.assert_called_once_with(
                    {"error": "Not found"},
                    server.HTTPStatus.NOT_FOUND,
                )
                handler.send_json.reset_mock()

            with patch.object(server, "ADMIN_SERVICE", local_open):
                self.assertTrue(handler.admin_surface_available())
                handler.path = "/api/admin/session"
                handler.do_GET()
                session = handler.send_json.call_args.args[0]
                self.assertEqual(session["username"], "open-admin")
                handler.send_json.reset_mock()
                with patch.object(
                    local_open,
                    "manual_review_items",
                    return_value=[
                        {
                            "review_id": "review-hard-1",
                            "retryable": False,
                        }
                    ],
                ):
                    handler.path = "/api/admin/quality/manual-review"
                    handler.do_GET()
                handler.send_json.assert_called_once_with(
                    {
                        "review_items": [
                            {
                                "review_id": "review-hard-1",
                                "retryable": False,
                            }
                        ],
                        "review_count": 1,
                        "retryable": False,
                    }
                )

    def test_repeated_invalid_logins_are_rate_limited(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="admin",
                password_hash=hash_password("a-secure-test-password"),
                job_dir=Path(directory),
                login_required=True,
                enabled=True,
            )
            for _ in range(5):
                with self.assertRaises(ValueError):
                    service.login("127.0.0.1", "admin", "wrong-password-value")

            with self.assertRaises(PermissionError):
                service.login("127.0.0.1", "admin", "a-secure-test-password")

    def test_job_window_is_bounded_to_latest_completed_utc_day(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(username="admin", password_hash="hash", job_dir=Path(directory))

            result = service.validate_job(
                {
                    "token_symbol": "aave",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-22",
                },
                today=date(2026, 7, 23),
            )

            self.assertEqual(result["token_symbol"], "AAVE")
            with self.assertRaises(ValueError):
                service.validate_job(
                    {
                        "token_symbol": "AAVE",
                        "start_date": "2026-07-01",
                        "end_date": "2026-07-20",
                    },
                    today=date(2026, 7, 23),
                )

    def test_worker_uses_argument_array_and_records_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "quality/daily-latest.json"
            report_path.parent.mkdir(parents=True)
            database_path = root / "market_facts.sqlite3"
            write_retry_database(database_path, "import-before")
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {"import_run_id": "import-before"},
                        "issues": [],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                data_dir=root,
                quality_report_path=report_path,
                database_path=database_path,
            )
            job_id = "test-job"
            service.jobs[job_id] = {
                "job_id": job_id,
                "job_type": "refresh",
                "token_symbol": "AAVE",
                "start_date": "2026-07-01",
                "end_date": "2026-07-22",
                "requested_by": "admin",
                "status": "queued",
                "created_at": "2026-07-23T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
            }

            def publish_new_snapshot(*_args, **_kwargs):
                write_retry_database(
                    database_path,
                    "import-after",
                    cex_rows=[
                        (
                            "2026-07-22",
                            "AAVE",
                            "BINANCE",
                            "AAVE/USDT",
                        )
                    ],
                )
                report_path.write_text(
                    json.dumps(
                        {
                            "schema": "fact_quality_report/v1",
                            "publication": {
                                "import_run_id": "import-after"
                            },
                            "issues": [
                                {
                                    "date": "2026-07-22",
                                    "category": "historical_gap",
                                    "status": "source_no_observation",
                                    "reason_code": "no_candles",
                                    "retryable": False,
                                    "market": {
                                        "token_symbol": "AAVE",
                                        "market_id": "cex:okx:AAVE/USDT",
                                    },
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )

            with patch(
                "dashboard.admin.subprocess.run",
                side_effect=publish_new_snapshot,
            ) as run:
                service._run_job(job_id)

            command = run.call_args.args[0]
            self.assertIsInstance(command, list)
            self.assertIn("--publish-local", command)
            self.assertIn("AAVE", command)
            self.assertNotIn("shell", run.call_args.kwargs)
            self.assertEqual(service.jobs[job_id]["status"], "succeeded")
            self.assertEqual(
                service.jobs[job_id]["result"]["quality_outcomes"][
                    "structural_non_error_count"
                ],
                1,
            )
            self.assertEqual(
                service.jobs[job_id]["result"]["quality_outcomes"][
                    "reason_counts"
                ],
                {"no_candles": 1},
            )

    def test_refresh_exit_zero_without_new_publication_is_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "quality/daily-latest.json"
            report_path.parent.mkdir(parents=True)
            database_path = root / "market_facts.sqlite3"
            write_retry_database(
                database_path,
                "import-before",
                cex_rows=[
                    ("2026-07-22", "AAVE", "BINANCE", "AAVE/USDT")
                ],
            )
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {"import_run_id": "import-before"},
                        "issues": [],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                data_dir=root,
                quality_report_path=report_path,
                database_path=database_path,
            )
            service.jobs["refresh"] = {
                "job_id": "refresh",
                "job_type": "refresh",
                "token_symbol": "AAVE",
                "start_date": "2026-07-22",
                "end_date": "2026-07-22",
                "requested_by": "admin",
                "status": "queued",
                "created_at": "2026-07-23T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
            }

            with patch("dashboard.admin.subprocess.run"):
                service._run_job("refresh")

            self.assertEqual(service.jobs["refresh"]["status"], "partial")
            self.assertEqual(
                service.jobs["refresh"]["error_code"],
                "refresh_publication_unchanged",
            )
            self.assertFalse(
                service.jobs["refresh"]["publication_committed"]
            )

    def test_refresh_new_publication_with_retryable_gap_is_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "quality/daily-latest.json"
            report_path.parent.mkdir(parents=True)
            database_path = root / "market_facts.sqlite3"
            write_retry_database(database_path, "import-before")
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {"import_run_id": "import-before"},
                        "issues": [],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                data_dir=root,
                quality_report_path=report_path,
                database_path=database_path,
            )
            service.jobs["refresh"] = {
                "job_id": "refresh",
                "job_type": "refresh",
                "token_symbol": "AAVE",
                "start_date": "2026-07-22",
                "end_date": "2026-07-22",
                "requested_by": "admin",
                "status": "queued",
                "created_at": "2026-07-23T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
            }

            def publish_with_gap(*_args, **_kwargs):
                write_retry_database(
                    database_path,
                    "import-after",
                    cex_rows=[
                        (
                            "2026-07-22",
                            "AAVE",
                            "BINANCE",
                            "AAVE/USDT",
                        )
                    ],
                )
                report_path.write_text(
                    json.dumps(
                        {
                            "schema": "fact_quality_report/v1",
                            "publication": {
                                "import_run_id": "import-after"
                            },
                            "issues": [
                                {
                                    "date": "2026-07-22",
                                    "category": "d1_active_gap",
                                    "status": "collection_failed",
                                    "reason_code": "rate_limit",
                                    "retryable": True,
                                    "market": {
                                        "token_symbol": "AAVE",
                                        "market_id": "cex:okx:AAVE/USDT",
                                    },
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )

            with patch(
                "dashboard.admin.subprocess.run",
                side_effect=publish_with_gap,
            ):
                service._run_job("refresh")

            job = service.jobs["refresh"]
            self.assertEqual(job["status"], "partial")
            self.assertEqual(
                job["error_code"],
                "refresh_quality_incomplete",
            )
            self.assertTrue(job["publication_committed"])
            self.assertTrue(job["retryable"])
            self.assertEqual(
                job["result"]["quality_outcomes"]["reason_counts"],
                {"rate_limit": 1},
            )

    def test_create_job_rejects_when_another_job_is_active(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=Path(directory),
                enabled=True,
            )
            service.jobs["active-job"] = {
                "job_id": "active-job",
                "status": "running",
                "created_at": "2026-07-23T00:00:00+00:00",
            }

            with patch.object(
                service,
                "validate_job",
                return_value={
                    "token_symbol": "AAVE",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-22",
                },
            ):
                with self.assertRaises(RuntimeError):
                    service.create_job({}, "admin")

    def test_job_persistence_failure_does_not_leave_a_queued_ghost(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=Path(directory) / "jobs",
            )
            request = {
                "token_symbol": "AAVE",
                "start_date": "2026-07-28",
                "end_date": "2026-07-28",
            }
            with patch.object(
                service,
                "validate_job",
                return_value=request,
            ), patch.object(
                service,
                "_save_job",
                side_effect=OSError("disk unavailable"),
            ), patch(
                "dashboard.admin.threading.Thread"
            ) as thread:
                with self.assertRaisesRegex(OSError, "disk unavailable"):
                    service.create_job({}, "admin")

            self.assertEqual(service.jobs, {})
            thread.assert_not_called()

    def test_runtime_token_is_refreshable_only_after_activation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                registry_path=root / "token_registry.json",
            )
            service.registry.upsert(self.runtime_record(status="pending"))
            self.assertNotIn("XYZ", service.configured_tokens())

            active = self.runtime_record(status="active")
            service.registry.upsert(active)

            self.assertIn("XYZ", service.configured_tokens())
            runtime = next(
                record
                for record in service.configured_token_records()
                if record["token_symbol"] == "XYZ"
            )
            self.assertEqual(runtime["origin"], "admin_runtime")
            self.assertEqual(
                runtime["cex_mapping_status"],
                "requires_manual_review",
            )

    def test_retry_job_must_match_audited_quality_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "daily-latest.json"
            database_path = root / "market_facts.sqlite3"
            write_retry_database(database_path, "import-before")
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {
                            "dataset_snapshot_id": "snapshot-before",
                            "import_run_id": "import-before",
                        },
                        "retry_windows_by_token": {
                            "AAVE": [
                                {
                                    "start_date": "2026-07-28",
                                    "end_date": "2026-07-28",
                                    "reason_codes": ["active_market_missing_d1"],
                                    "market_ids": ["cex:binance:AAVE/USDT"],
                                    "issue_ids": ["issue-before"],
                                }
                            ]
                        },
                        "backfill_windows_by_token": {},
                        "issues": [
                            {
                                "issue_id": "issue-before",
                                "date": "2026-07-28",
                                "market": {
                                    "token_symbol": "AAVE",
                                    "market_id": "cex:binance:AAVE/USDT",
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                quality_report_path=report_path,
                database_path=database_path,
            )

            request = service.validate_retry_job(
                {
                    "token_symbol": "AAVE",
                    "start_date": "2026-07-28",
                    "end_date": "2026-07-28",
                }
            )
            self.assertEqual(request["queue_type"], "latest_completed_day")
            with self.assertRaisesRegex(ValueError, "current quality report"):
                service.validate_retry_job(
                    {
                        "token_symbol": "AAVE",
                        "start_date": "2026-07-27",
                        "end_date": "2026-07-28",
                    }
                )

    def test_historical_backfill_is_an_exact_separate_retry_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "daily-latest.json"
            database_path = root / "market_facts.sqlite3"
            write_retry_database(database_path, "import-before")
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {
                            "dataset_snapshot_id": "snapshot-before",
                            "import_run_id": "import-before",
                        },
                        "retry_windows_by_token": {},
                        "backfill_windows_by_token": {
                            "AAVE": [
                                {
                                    "start_date": "2026-07-01",
                                    "end_date": "2026-07-03",
                                    "reason_codes": ["historical_market_gap"],
                                    "market_ids": [
                                        "cex:binance:AAVE/USDT"
                                    ],
                                    "issue_ids": [
                                        "historical-1",
                                        "historical-2",
                                        "historical-3",
                                    ],
                                }
                            ]
                        },
                        "issues": [
                            {
                                "issue_id": f"historical-{index}",
                                "date": f"2026-07-0{index}",
                                "market": {
                                    "token_symbol": "AAVE",
                                    "market_id": "cex:binance:AAVE/USDT",
                                },
                            }
                            for index in range(1, 4)
                        ],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                quality_report_path=report_path,
                database_path=database_path,
            )

            windows = service.retryable_windows()
            self.assertEqual(len(windows), 1)
            self.assertEqual(windows[0]["queue_type"], "historical_gap")
            request = service.validate_retry_job(
                {
                    "token_symbol": "AAVE",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-03",
                    "queue_type": "historical_gap",
                }
            )
            self.assertEqual(
                request["expected_observations"],
                [
                    {
                        "market_id": "cex:binance:AAVE/USDT",
                        "date": f"2026-07-0{index}",
                    }
                    for index in range(1, 4)
                ],
            )
            with self.assertRaisesRegex(ValueError, "current quality report"):
                service.validate_retry_job(
                    {
                        "token_symbol": "AAVE",
                        "start_date": "2026-07-01",
                        "end_date": "2026-07-03",
                        "queue_type": "latest_completed_day",
                    }
                )

    def test_manual_review_items_are_sanitized_and_never_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "daily-latest.json"
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {},
                        "manual_review_queue": [
                            {
                                "review_id": "review-hard-1",
                                "issue_id": "hard-1",
                                "token_symbol": " aave ",
                                "market_id": "cex:binance:AAVE/USDT",
                                "date": "2026-07-28",
                                "category": "hard_invalid",
                                "reason_code": "invalid_positive_ohlc",
                                "source_url_hints": [
                                    "https://api.binance.com/api/v3/klines?symbol=AAVEUSDT#fragment",
                                    "javascript:alert(1)",
                                    "https://user:secret@example.com/private",
                                ],
                            },
                            {
                                "review_id": "review-gap",
                                "issue_id": "gap-1",
                                "token_symbol": "AAVE",
                                "market_id": "cex:binance:AAVE/USDT",
                                "date": "2026-07-10",
                                "category": "historical_gap",
                                "reason_code": "historical_market_gap",
                                "source_url_hints": [],
                            },
                            {
                                "review_id": "review-bad-date",
                                "issue_id": "bad-date",
                                "token_symbol": "AAVE",
                                "market_id": "cex:binance:AAVE/USDT",
                                "date": "not-a-date",
                                "category": "stale_market_unknown",
                                "reason_code": "stale_market_lifecycle_unknown",
                                "source_url_hints": [],
                            },
                        ],
                        "issues": [
                            {
                                "issue_id": "hard-1",
                                "message": "  Close price must be   positive.  ",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                quality_report_path=report_path,
            )

            items = service.manual_review_items()

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["token_symbol"], "AAVE")
            self.assertEqual(
                items[0]["reason_message"],
                "Close price must be positive.",
            )
            self.assertEqual(
                items[0]["source_url_hints"],
                [
                    "https://api.binance.com/api/v3/klines?symbol=AAVEUSDT"
                ],
            )
            self.assertFalse(items[0]["retryable"])
            self.assertEqual(
                items[0]["action"],
                "manual_primary_source_review",
            )
            self.assertFalse(items[0]["candidate_rejected"])
            self.assertIsNone(items[0]["rejection_id"])

    def test_lineage_matched_source_absence_enters_manual_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "daily-latest.json"
            report_path.write_text(
                json.dumps(
                    {
                        "schema": "fact_quality_report/v1",
                        "publication": {},
                        "manual_review_queue": [
                            {
                                "review_id": "review-range-1",
                                "issue_id": "range-1",
                                "token_symbol": "AAVE",
                                "market_id": "cex:htx:AAVE/USDT",
                                "date": "2026-01-01",
                                "category": "historical_gap",
                                "reason_code": "source_range_unavailable",
                                "source_url_hints": [
                                    "https://api.huobi.pro/market/history/kline"
                                ],
                            }
                        ],
                        "issues": [
                            {
                                "issue_id": "range-1",
                                "category": "historical_gap",
                                "status": "needs_review",
                                "retryable": False,
                                "message": (
                                    "The source endpoint cannot reach the "
                                    "requested date window."
                                ),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                quality_report_path=report_path,
            )

            items = service.manual_review_items()

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["reason_code"], "source_range_unavailable")
            self.assertEqual(items[0]["category"], "historical_gap")
            self.assertFalse(items[0]["retryable"])

    def test_latest_integrity_checked_rejection_enters_manual_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rejection_id = "20260729T000000000000Z-abcdef123456"
            bundle = root / "quality/rejected" / rejection_id
            bundle.mkdir(parents=True)
            rejected_report = {
                "schema": "fact_quality_report/v1",
                "rejection": {
                    "schema": "fact_quality_rejection/v1",
                    "rejection_id": rejection_id,
                    "status": "rejected_hard_invalid",
                },
                "manual_review_queue": [
                    {
                        "review_id": "review-hard-1",
                        "issue_id": "hard-1",
                        "token_symbol": "AAVE",
                        "market_id": "cex:binance:AAVE/USDT",
                        "date": "2026-07-28",
                        "category": "hard_invalid",
                        "reason_code": "invalid_positive_ohlc",
                        "source_url_hints": [
                            "https://api.binance.com/api/v3/klines"
                        ],
                    }
                ],
                "issues": [
                    {
                        "issue_id": "hard-1",
                        "message": "Close price must be positive.",
                    }
                ],
            }
            report_path = bundle / "report.json"
            report_path.write_text(
                json.dumps(rejected_report),
                encoding="utf-8",
            )
            pointer = {
                "schema": "fact_quality_rejection_pointer/v1",
                "rejection_id": rejection_id,
                "report": f"{rejection_id}/report.json",
                "report_sha256": hashlib.sha256(
                    report_path.read_bytes()
                ).hexdigest(),
            }
            (root / "quality/rejected/latest.json").write_text(
                json.dumps(pointer),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                data_dir=root,
                quality_report_path=root / "quality/daily-latest.json",
            )

            items = service.manual_review_items()

            self.assertEqual(len(items), 1)
            self.assertTrue(items[0]["candidate_rejected"])
            self.assertEqual(items[0]["rejection_id"], rejection_id)
            self.assertEqual(items[0]["category"], "hard_invalid")

    def test_rejected_quality_pointer_traversal_and_hash_mismatch_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rejected_root = root / "quality/rejected"
            rejected_root.mkdir(parents=True)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                data_dir=root,
                quality_report_path=root / "quality/daily-latest.json",
            )
            pointer_path = rejected_root / "latest.json"
            invalid_pointers = [
                {
                    "schema": "fact_quality_rejection_pointer/v1",
                    "rejection_id": "bundle",
                    "report": "../../outside.json",
                    "report_sha256": "0" * 64,
                },
                {
                    "schema": "fact_quality_rejection_pointer/v1",
                    "rejection_id": "bundle",
                    "report": "bundle/report.json",
                    "report_sha256": "0" * 64,
                },
            ]
            bundle = rejected_root / "bundle"
            bundle.mkdir()
            (bundle / "report.json").write_text("{}", encoding="utf-8")
            for pointer in invalid_pointers:
                with self.subTest(report=pointer["report"]):
                    pointer_path.write_text(
                        json.dumps(pointer),
                        encoding="utf-8",
                    )
                    self.assertEqual(service.manual_review_items(), [])

    def test_retry_job_requires_new_report_and_resolved_market_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "quality/daily-latest.json"
            report_path.parent.mkdir(parents=True)
            database_path = root / "market_facts.sqlite3"
            write_retry_database(database_path, "import-before")
            baseline_report = {
                "schema": "fact_quality_report/v1",
                "publication": {
                    "dataset_snapshot_id": "snapshot-before",
                    "import_run_id": "import-before",
                },
                "retry_windows_by_token": {
                    "AAVE": [
                        {
                            "start_date": "2026-07-28",
                            "end_date": "2026-07-28",
                            "reason_codes": ["d1_active_market_gap"],
                            "market_ids": ["cex:binance:AAVE/USDT"],
                            "issue_ids": ["issue-before"],
                        }
                    ]
                },
                "issues": [
                    {
                        "date": "2026-07-28",
                        "issue_id": "issue-before",
                        "category": "d1_active_gap",
                        "market": {
                            "token_symbol": "AAVE",
                            "market_id": "cex:binance:AAVE/USDT",
                        },
                    }
                ],
            }
            report_path.write_text(
                json.dumps(baseline_report),
                encoding="utf-8",
            )
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "admin/jobs",
                quality_report_path=report_path,
                database_path=database_path,
                data_dir=root,
            )
            request = service.validate_retry_job(
                {
                    "token_symbol": "AAVE",
                    "start_date": "2026-07-28",
                    "end_date": "2026-07-28",
                }
            )
            service.jobs["retry"] = {
                "job_id": "retry",
                "job_type": "retry_failed",
                **request,
                "requested_by": "admin",
                "status": "queued",
                "stage": "queued",
                "created_at": "2026-07-29T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }

            with patch("dashboard.admin.subprocess.run"):
                service._run_job("retry")

            self.assertEqual(service.jobs["retry"]["status"], "partial")
            self.assertEqual(
                service.jobs["retry"]["error_code"],
                "retry_not_resolved",
            )
            self.assertTrue(service.jobs["retry"]["publication_committed"])

            refreshed_report = {
                **baseline_report,
                "publication": {
                    "dataset_snapshot_id": "snapshot-after",
                    "import_run_id": "import-after",
                },
                "retry_windows_by_token": {},
                "issues": [
                    {
                        "date": "2026-07-28",
                        "category": "historical_gap",
                        "market": {
                            "token_symbol": "AAVE",
                            "market_id": "cex:binance:AAVE/USDT",
                        },
                    }
                ],
            }
            report_path.write_text(
                json.dumps(refreshed_report),
                encoding="utf-8",
            )
            write_retry_database(database_path, "import-after")
            self.assertIn(
                "neither observed",
                service._verify_retry_resolution(service.jobs["retry"]),
            )

            refreshed_report["issues"] = []
            report_path.write_text(
                json.dumps(refreshed_report),
                encoding="utf-8",
            )
            write_retry_database(
                database_path,
                "import-after",
                cex_rows=[
                    (
                        "2026-07-28",
                        "AAVE",
                        "BINANCE",
                        "aave/usdt",
                    )
                ],
            )
            self.assertIsNone(
                service._verify_retry_resolution(service.jobs["retry"])
            )
            error, evidence = service._retry_resolution_evidence(
                service.jobs["retry"]
            )
            self.assertIsNone(error)
            self.assertEqual(evidence["observed_count"], 1)
            self.assertEqual(evidence["confirmed_absence_count"], 0)

            write_retry_database(database_path, "import-after")
            refreshed_report["issues"] = [
                    {
                        "date": "2026-07-28",
                        "category": "historical_gap",
                        "status": "needs_review",
                        "reason_code": "source_range_unavailable",
                        "retryable": False,
                    "market": {
                        "token_symbol": "AAVE",
                        "market_id": "cex:binance:AAVE/USDT",
                    },
                }
            ]
            report_path.write_text(
                json.dumps(refreshed_report),
                encoding="utf-8",
            )

            error, evidence = service._retry_resolution_evidence(
                service.jobs["retry"]
            )
            self.assertIsNone(error)
            self.assertEqual(evidence["observed_count"], 0)
            self.assertEqual(evidence["confirmed_absence_count"], 1)
            self.assertEqual(
                evidence["confirmed_absence_reason_counts"],
                {"source_range_unavailable": 1},
            )

    def test_onboarding_lock_conflict_reconciles_pending_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock_path = root / "collection/collection.lock"
            lock_path.parent.mkdir(parents=True)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "admin/jobs",
                registry_path=root / "admin/token_registry.json",
                data_dir=root,
                collection_lock_path=lock_path,
            )
            record = self.runtime_record(status="pending")
            record["last_job_id"] = "onboard"
            service.registry.upsert(record)
            service.jobs["onboard"] = {
                "job_id": "onboard",
                "job_type": "token_onboarding",
                "token_symbol": "XYZ",
                "chain": "base",
                "contract_address": record["contract_address"],
                "start_date": "2026-07-01",
                "end_date": "2026-07-28",
                "status": "queued",
                "stage": "queued",
                "created_at": "2026-07-29T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }

            with lock_path.open("a+", encoding="utf-8") as lock_handle:
                fcntl.flock(
                    lock_handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                with patch("dashboard.admin.subprocess.run") as run:
                    service._run_job("onboard")
                run.assert_not_called()

            self.assertEqual(
                service.jobs["onboard"]["error_code"],
                "collection_in_progress",
            )
            self.assertEqual(
                service.registry.get("base", record["contract_address"])["status"],
                "needs_review",
            )

    def test_onboarding_queues_pending_registry_without_cex_inference(self):
        candidate = {
            "identity": {
                "token_symbol": "XYZ",
                "token_name": "XYZ Token",
                "chain": "base",
                "contract_address": "0x" + "12" * 20,
                "decimals": 18,
                "coingecko_id": None,
                "source": "geckoterminal",
                "source_token_id": "base_0x" + "12" * 20,
            },
            "discovery": {
                "usable_pool_count": 1,
                "top_pools": [{"pool_address": "0x" + "34" * 20}],
            },
            "capabilities": {
                "dex_daily": "available",
                "tvl": "available_after_collection",
                "dex_depth": "protocol_dependent",
                "cex": "requires_manual_mapping",
            },
            "already_configured": False,
            "registration": {
                "origin": None,
                "status": None,
                "cex_mapping_status": "requires_manual_review",
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                registry_path=root / "token_registry.json",
            )
            with patch.object(service, "resolve_token", return_value=candidate), patch(
                "dashboard.admin.threading.Thread"
            ) as thread:
                job = service.create_onboarding_job(
                    {
                        "chain": "base",
                        "contract_address": candidate["identity"]["contract_address"],
                        "expected_token_symbol": "XYZ",
                        "history_days": 30,
                    },
                    "admin",
                )

            record = service.registry.get(
                "base",
                candidate["identity"]["contract_address"],
            )
            self.assertEqual(job["job_type"], "token_onboarding")
            self.assertEqual(record["status"], "pending")
            self.assertEqual(
                record["cex_mapping"]["status"],
                "requires_manual_review",
            )
            thread.return_value.start.assert_called_once_with()

    def test_onboarding_job_persistence_failure_precedes_registry_mutation(self):
        candidate = {
            "identity": {
                "token_symbol": "XYZ",
                "token_name": "XYZ Token",
                "chain": "base",
                "contract_address": "0x" + "12" * 20,
                "decimals": 18,
                "coingecko_id": None,
                "source": "geckoterminal",
                "source_token_id": "base_0x" + "12" * 20,
            },
            "discovery": {
                "usable_pool_count": 1,
                "top_pools": [{"pool_address": "0x" + "34" * 20}],
            },
            "capabilities": {
                "dex_daily": "available",
                "tvl": "available_after_collection",
                "dex_depth": "protocol_dependent",
                "cex": "requires_manual_mapping",
            },
            "already_configured": False,
            "registration": {
                "origin": None,
                "status": None,
                "cex_mapping_status": "requires_manual_review",
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                registry_path=root / "token_registry.json",
            )
            with patch.object(
                service,
                "resolve_token",
                return_value=candidate,
            ), patch.object(
                service,
                "_save_job",
                side_effect=OSError("disk unavailable"),
            ), patch(
                "dashboard.admin.threading.Thread"
            ) as thread:
                with self.assertRaisesRegex(OSError, "disk unavailable"):
                    service.create_onboarding_job(
                        {
                            "chain": "base",
                            "contract_address": candidate["identity"][
                                "contract_address"
                            ],
                            "expected_token_symbol": "XYZ",
                        },
                        "admin",
                    )

            self.assertEqual(service.jobs, {})
            self.assertIsNone(
                service.registry.get(
                    "base",
                    candidate["identity"]["contract_address"],
                )
            )
            thread.assert_not_called()

    def test_onboarding_activates_only_after_published_dex_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_path = root / "market_facts.sqlite3"
            connection = sqlite3.connect(database_path)
            connection.execute(
                "CREATE TABLE dex_pool_daily (token_symbol TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO dex_pool_daily (token_symbol) VALUES ('XYZ')"
            )
            connection.commit()
            connection.close()
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                registry_path=root / "token_registry.json",
                database_path=database_path,
            )
            record = self.runtime_record(status="pending")
            service.registry.upsert(record)
            service.jobs["onboard"] = {
                "job_id": "onboard",
                "job_type": "token_onboarding",
                "token_symbol": "XYZ",
                "chain": "base",
                "contract_address": record["contract_address"],
                "start_date": "2026-07-01",
                "end_date": "2026-07-28",
                "status": "queued",
                "stage": "resolve_identity",
                "created_at": "2026-07-29T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }

            with patch("dashboard.admin.subprocess.run") as run:
                service._run_job("onboard")

            commands = [call.args[0] for call in run.call_args_list]
            self.assertIn("--dex-only", commands[0])
            self.assertEqual(
                run.call_args_list[0].kwargs["env"]["TOKEN_ONBOARDING_JOB_ID"],
                "onboard",
            )
            self.assertNotIn(
                "TOKEN_ONBOARDING_JOB_ID",
                run.call_args_list[1].kwargs["env"],
            )
            self.assertTrue(any("fetch_tvl.py" in command[1] for command in commands))
            self.assertTrue(any("fetch_dex_depth.py" in command[1] for command in commands))
            processed_dir = (root.parent / f".{root.name}-processed").resolve()
            tvl_command = next(
                command for command in commands if "fetch_tvl.py" in command[1]
            )
            dex_depth_command = next(
                command for command in commands if "fetch_dex_depth.py" in command[1]
            )
            self.assertEqual(
                tvl_command[tvl_command.index("--output-dir") + 1],
                str(processed_dir),
            )
            self.assertEqual(
                tvl_command[tvl_command.index("--raw-root") + 1],
                str((root / "raw/tvl").resolve()),
            )
            self.assertEqual(
                dex_depth_command[dex_depth_command.index("--output-dir") + 1],
                str(processed_dir),
            )
            self.assertEqual(
                dex_depth_command[dex_depth_command.index("--raw-root") + 1],
                str((root / "raw/dex-depth").resolve()),
            )
            self.assertEqual(service.jobs["onboard"]["status"], "succeeded")
            self.assertTrue(service.jobs["onboard"]["publication_committed"])
            self.assertEqual(
                service.registry.get("base", record["contract_address"])["status"],
                "active",
            )

    def test_onboarding_verification_failure_records_committed_partial_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                username="admin",
                password_hash="hash",
                job_dir=root / "jobs",
                registry_path=root / "token_registry.json",
                database_path=root / "missing-market-facts.sqlite3",
            )
            record = self.runtime_record(status="pending")
            service.registry.upsert(record)
            service.jobs["onboard"] = {
                "job_id": "onboard",
                "job_type": "token_onboarding",
                "token_symbol": "XYZ",
                "chain": "base",
                "contract_address": record["contract_address"],
                "start_date": "2026-07-01",
                "end_date": "2026-07-28",
                "status": "queued",
                "stage": "resolve_identity",
                "created_at": "2026-07-29T00:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }

            with patch("dashboard.admin.subprocess.run"):
                service._run_job("onboard")

            job = service.jobs["onboard"]
            self.assertEqual(job["status"], "partial")
            self.assertEqual(
                job["error_code"],
                "token_onboarding_verification_failed",
            )
            self.assertTrue(job["publication_committed"])
            self.assertEqual(job["result"]["dex_daily"], "published_unverified")
            self.assertEqual(
                service.registry.get("base", record["contract_address"])["status"],
                "needs_review",
            )

    def test_public_response_cache_keeps_only_active_generation_and_minute(self):
        server.clear_runtime_caches()
        self.assertEqual(
            server._build_public_api_response_cached.cache_info().maxsize,
            server.SERIALIZED_RESPONSE_CACHE_SIZE,
        )
        self.assertEqual(
            server._build_enriched_payload_cached.cache_info().maxsize,
            server.LARGE_PAYLOAD_CACHE_SIZE,
        )
        first_signature = (("facts.sqlite3", 1, 100),)
        second_signature = (("facts.sqlite3", 2, 100),)
        payload = {"metadata": {}, "markets": []}

        with patch.object(server, "build_market_catalog", return_value=payload):
            with patch.object(server, "api_source_signature", return_value=first_signature):
                with patch.object(server, "api_freshness_bucket", return_value=100):
                    server.build_public_api_response("catalog", (), True)
            self.assertEqual(server._build_public_api_response_cached.cache_info().currsize, 1)

            with patch.object(server, "api_source_signature", return_value=second_signature):
                with patch.object(server, "api_freshness_bucket", return_value=100):
                    server.build_public_api_response("catalog", (), True)
            self.assertEqual(server._build_public_api_response_cached.cache_info().currsize, 1)

            with patch.object(server, "api_source_signature", return_value=second_signature):
                with patch.object(server, "api_freshness_bucket", return_value=101):
                    server.build_public_api_response("catalog", (), True)
            self.assertEqual(server._build_public_api_response_cached.cache_info().currsize, 1)

    def test_source_generation_change_clears_large_assembled_payloads(self):
        server.clear_runtime_caches()
        first_signature = (("facts.sqlite3", 1, 100),)
        second_signature = (("facts.sqlite3", 2, 100),)
        cache_key = (
            None,
            None,
            "",
            "cex.csv",
            "dex.csv",
            first_signature,
            "",
            (),
            "",
            (),
            "",
            (),
        )
        payload = {"metadata": {}, "cex_markets": [], "dex_pools": []}

        with patch.object(
            server,
            "_build_market_payload_cached",
            return_value=payload,
        ):
            with patch.object(server, "overlay_tvl_snapshot", side_effect=lambda value, _: value):
                with patch.object(
                    server,
                    "overlay_cex_depth_snapshot",
                    side_effect=lambda value, _: value,
                ):
                    with patch.object(
                        server,
                        "overlay_dex_depth_snapshot",
                        side_effect=lambda value, _: value,
                    ):
                        server._build_enriched_payload_cached(cache_key)

        self.assertEqual(server._build_enriched_payload_cached.cache_info().currsize, 1)
        server.ensure_source_cache_generation(first_signature)
        self.assertEqual(server._build_enriched_payload_cached.cache_info().currsize, 1)
        server.ensure_source_cache_generation(second_signature)
        self.assertEqual(server._build_enriched_payload_cached.cache_info().currsize, 0)

    def test_source_generation_clear_waits_for_payload_cache_write_back(self):
        server.clear_runtime_caches()
        first_signature = (("facts.sqlite3", 1, 100),)
        second_signature = (("facts.sqlite3", 2, 100),)
        build_started = Event()
        release_build = Event()
        generation_change_started = Event()
        generation_change_finished = Event()
        payload = {"metadata": {}, "cex_markets": [], "dex_pools": []}

        def slow_build(_cache_key):
            build_started.set()
            if not release_build.wait(timeout=2):
                raise TimeoutError("test did not release the payload build")
            return payload

        def change_generation():
            generation_change_started.set()
            server.ensure_source_cache_generation(second_signature)
            generation_change_finished.set()

        with patch.object(
            server,
            "api_source_signature",
            return_value=first_signature,
        ), patch.object(
            server,
            "market_payload_cache_key",
            return_value=("first-generation",),
        ), patch.object(
            server,
            "_build_enriched_payload_cached",
            side_effect=slow_build,
        ), patch.object(
            server,
            "attach_freshness_metadata",
            side_effect=lambda value: value,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                build_future = executor.submit(server.build_market_payload)
                self.assertTrue(build_started.wait(timeout=1))
                generation_future = executor.submit(change_generation)
                self.assertTrue(generation_change_started.wait(timeout=1))
                try:
                    self.assertFalse(generation_change_finished.wait(timeout=0.1))
                finally:
                    release_build.set()
                self.assertEqual(build_future.result(timeout=1), payload)
                generation_future.result(timeout=1)

        self.assertEqual(server._SOURCE_CACHE_GENERATION, second_signature)

    def test_cex_depth_retention_plan_is_dry_until_explicitly_applied(self):
        now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "cex-depth"
            old_snapshot = raw_root / "20260718T120000Z-old"
            recent_snapshot = raw_root / "20260726T120000Z-recent"
            old_snapshot.mkdir(parents=True)
            recent_snapshot.mkdir(parents=True)
            (old_snapshot / "manifest.json").write_text('{"snapshot":"old"}\n', encoding="utf-8")
            (recent_snapshot / "manifest.json").write_text(
                '{"snapshot":"recent"}\n',
                encoding="utf-8",
            )

            actions = plan_retention(
                raw_root,
                now=now,
                keep_raw_days=7,
                keep_archive_days=30,
            )

            self.assertEqual([action.action for action in actions], ["compress"])
            self.assertTrue(old_snapshot.exists())
            self.assertTrue(recent_snapshot.exists())

            apply_retention(actions)

            self.assertFalse(old_snapshot.exists())
            self.assertTrue(recent_snapshot.exists())
            self.assertTrue(
                (raw_root / "archives/20260718T120000Z-old.tar.gz").is_file()
            )

    def test_cex_depth_retention_rejects_broad_targets_and_expires_archives(self):
        now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            plan_retention(
                Path("/"),
                now=now,
                keep_raw_days=7,
                keep_archive_days=30,
            )

        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "cex-depth"
            archive_root = raw_root / "archives"
            archive_root.mkdir(parents=True)
            expired = archive_root / "20260601T120000Z-expired.tar.gz"
            expired.write_bytes(b"already-reviewed-archive")

            actions = plan_retention(
                raw_root,
                now=now,
                keep_raw_days=7,
                keep_archive_days=30,
            )
            self.assertEqual([action.action for action in actions], ["delete_archive"])
            apply_retention(actions)
            self.assertFalse(expired.exists())

    def test_cex_depth_retention_rejects_symlink_root_before_resolving_it(self):
        now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            real_root = base / "real/cex-depth"
            real_root.mkdir(parents=True)
            symlink_root = base / "cex-depth"
            symlink_root.symlink_to(real_root, target_is_directory=True)

            with self.assertRaisesRegex(ValueError, "not a symlink"):
                plan_retention(
                    symlink_root,
                    now=now,
                    keep_raw_days=7,
                    keep_archive_days=30,
                )

    def test_production_templates_enforce_loopback_restart_https_and_retention(self):
        project_root = Path(__file__).resolve().parents[1]
        service = (
            project_root / "deploy/systemd/cex-dex-dashboard.service.in"
        ).read_text(encoding="utf-8")
        user_service = (
            project_root / "deploy/systemd/cex-dex-dashboard-user.service.in"
        ).read_text(encoding="utf-8")
        nginx = (
            project_root / "deploy/nginx/cex-dex-dashboard.conf.in"
        ).read_text(encoding="utf-8")
        retention = (
            project_root / "deploy/systemd/cex-dex-cex-depth-retention.service.in"
        ).read_text(encoding="utf-8")
        daily_collection = (
            project_root / "deploy/systemd/cex-dex-daily.service.in"
        ).read_text(encoding="utf-8")
        depth_collection = (
            project_root / "deploy/systemd/cex-dex-depth.service.in"
        ).read_text(encoding="utf-8")
        daily_user_collection = (
            project_root / "deploy/systemd/cex-dex-daily-user.service.in"
        ).read_text(encoding="utf-8")
        depth_user_collection = (
            project_root / "deploy/systemd/cex-dex-depth-user.service.in"
        ).read_text(encoding="utf-8")
        runbook = (
            project_root / "docs/production-hardening.md"
        ).read_text(encoding="utf-8")

        self.assertIn("--host 127.0.0.1", service)
        self.assertIn("Restart=on-failure", service)
        self.assertIn("EnvironmentFile=-/etc/cex-dex/dashboard.env", service)
        self.assertIn("ReadWritePaths=@MARKET_DATA_DIR@", service)
        self.assertIn("ReadWritePaths=@MARKET_WORK_DIR@", service)
        self.assertIn("ReadWritePaths=@ADMIN_JOB_DIR@", service)
        self.assertNotIn("ReadWritePaths=-@PROJECT_ROOT@/data/", service)
        self.assertIn("Environment=ADMIN_ENABLED=false", user_service)
        self.assertIn("--host @BIND_HOST@", user_service)
        self.assertIn("Restart=on-failure", user_service)
        self.assertIn("WantedBy=default.target", user_service)
        self.assertNotIn("CapabilityBoundingSet=", user_service)
        self.assertIn("listen 443 ssl", nginx)
        self.assertIn("return 301 https://@DOMAIN@$request_uri;", nginx)
        self.assertNotIn("https://$host$request_uri", nginx)
        self.assertIn("proxy_pass http://127.0.0.1:8765", nginx)
        self.assertIn("location ^~ /api/admin/ { return 404; }", nginx)
        self.assertIn("retain_cex_depth_raw.py", retention)
        self.assertIn("--root @MARKET_DATA_DIR@/raw/cex-depth", retention)
        self.assertIn(
            "ReadWritePaths=@MARKET_DATA_DIR@/raw/cex-depth",
            retention,
        )
        self.assertIn("--apply", retention)
        self.assertIn(
            "EnvironmentFile=-/etc/cex-dex/dashboard.env",
            daily_collection,
        )
        self.assertIn(
            "EnvironmentFile=-/etc/cex-dex/dashboard.env",
            depth_collection,
        )
        for collection in (daily_collection, depth_collection):
            self.assertIn("User=@SERVICE_USER@", collection)
            self.assertIn("Group=@SERVICE_GROUP@", collection)
            self.assertIn("--data-dir @MARKET_DATA_DIR@", collection)
            self.assertIn("ReadOnlyPaths=@PROJECT_ROOT@", collection)
            self.assertIn("ReadWritePaths=@MARKET_DATA_DIR@", collection)
            self.assertIn("ReadWritePaths=@MARKET_WORK_DIR@", collection)
            self.assertIn("ProtectSystem=strict", collection)
        for collection in (daily_user_collection, depth_user_collection):
            self.assertIn("Environment=MARKET_DATA_DIR=@MARKET_DATA_DIR@", collection)
            self.assertIn("--data-dir @MARKET_DATA_DIR@", collection)
            self.assertNotIn("/etc/cex-dex", collection)
            self.assertNotIn("User=root", collection)
        self.assertIn("non-destructive unless `--apply`", runbook)
        self.assertIn("systemctl --user enable --now", runbook)


if __name__ == "__main__":
    unittest.main()
