import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from dashboard.admin import AdminService, hash_password, verify_password


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

    def test_open_mode_skips_login(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                job_dir=Path(directory),
                login_required=False,
            )

            session = service.public_session(None)

            self.assertTrue(session["authenticated"])
            self.assertTrue(session["configured"])
            self.assertFalse(session["login_required"])
            self.assertEqual(session["username"], "open-admin")

    def test_repeated_invalid_logins_are_rate_limited(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                username="admin",
                password_hash=hash_password("a-secure-test-password"),
                job_dir=Path(directory),
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


if __name__ == "__main__":
    unittest.main()
