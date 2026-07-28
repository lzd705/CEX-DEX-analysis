import os
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
    hash_password,
    password_hash_is_configured,
    verify_password,
)
from scripts.retain_cex_depth_raw import apply_retention, plan_retention


class AdminServiceTest(unittest.TestCase):
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
            service = AdminService(username="admin", password_hash="hash", job_dir=Path(directory))
            job_id = "test-job"
            service.jobs[job_id] = {
                "job_id": job_id,
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

            with patch("dashboard.admin.subprocess.run") as run:
                service._run_job(job_id)

            command = run.call_args.args[0]
            self.assertIsInstance(command, list)
            self.assertIn("--publish-local", command)
            self.assertIn("AAVE", command)
            self.assertNotIn("shell", run.call_args.kwargs)
            self.assertEqual(service.jobs[job_id]["status"], "succeeded")

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
        runbook = (
            project_root / "docs/production-hardening.md"
        ).read_text(encoding="utf-8")

        self.assertIn("--host 127.0.0.1", service)
        self.assertIn("Restart=on-failure", service)
        self.assertIn("EnvironmentFile=-/etc/cex-dex/dashboard.env", service)
        self.assertIn("Environment=ADMIN_ENABLED=false", user_service)
        self.assertIn("--host @BIND_HOST@", user_service)
        self.assertIn("Restart=on-failure", user_service)
        self.assertIn("WantedBy=default.target", user_service)
        self.assertIn("listen 443 ssl", nginx)
        self.assertIn("return 301 https://@DOMAIN@$request_uri;", nginx)
        self.assertNotIn("https://$host$request_uri", nginx)
        self.assertIn("proxy_pass http://127.0.0.1:8765", nginx)
        self.assertIn("location ^~ /api/admin/ { return 404; }", nginx)
        self.assertIn("retain_cex_depth_raw.py", retention)
        self.assertIn("--apply", retention)
        self.assertIn("non-destructive unless `--apply`", runbook)
        self.assertIn("systemctl --user enable --now", runbook)


if __name__ == "__main__":
    unittest.main()
