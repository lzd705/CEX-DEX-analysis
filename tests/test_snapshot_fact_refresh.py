import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dashboard import admin
from dashboard.admin import AdminService


class SnapshotFactRefreshTest(unittest.TestCase):
    def test_validator_requires_canonical_market_and_dex_for_tvl(self):
        self.assertEqual(
            AdminService.validate_snapshot_refresh_job(
                {
                    "token_symbol": "aave",
                    "market_id": "cex:binance:AAVE/USDT",
                    "fact_type": "depth",
                }
            ),
            {
                "token_symbol": "AAVE",
                "market_id": "cex:binance:AAVE/USDT",
                "market_type": "cex",
                "fact_type": "depth",
            },
        )
        with self.assertRaisesRegex(ValueError, "canonical"):
            AdminService.validate_snapshot_refresh_job(
                {
                    "token_symbol": "AAVE",
                    "market_id": "binance|AAVE/USDT",
                    "fact_type": "depth",
                }
            )
        with self.assertRaisesRegex(ValueError, "DEX"):
            AdminService.validate_snapshot_refresh_job(
                {
                    "token_symbol": "AAVE",
                    "market_id": "cex:binance:AAVE/USDT",
                    "fact_type": "tvl",
                }
            )

    def test_depth_refresh_runs_bounded_profile_and_token_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                data_dir=root / "data",
                job_dir=root / "jobs",
            )
            run_command = Mock()
            set_job = Mock()
            job = {
                "token_symbol": "AAVE",
                "market_id": "cex:binance:AAVE/USDT",
                "market_type": "cex",
                "fact_type": "depth",
            }

            with patch.object(service, "_run_command", run_command), patch.object(
                service,
                "_set_job",
                set_job,
            ):
                service._run_snapshot_refresh_job(
                    "job-1",
                    job,
                    root / "jobs/job-1.log",
                )

            command = run_command.call_args.args[0]
            self.assertEqual(command[0], sys.executable)
            self.assertEqual(
                command[1],
                str(admin.PROJECT_ROOT / "scripts/run_collection_cycle.py"),
            )
            self.assertIn("cex_depth", command)
            self.assertEqual(command[-2:], ["--tokens", "AAVE"])
            set_job.assert_called_once()
            self.assertTrue(set_job.call_args.kwargs["publication_committed"])

    def test_tvl_refresh_uses_published_tvl_profile_without_fake_token_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                data_dir=root / "data",
                job_dir=root / "jobs",
            )
            run_command = Mock()
            with patch.object(service, "_run_command", run_command), patch.object(
                service,
                "_set_job",
            ):
                service._run_snapshot_refresh_job(
                    "job-2",
                    {
                        "token_symbol": "AAVE",
                        "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
                        "market_type": "dex",
                        "fact_type": "tvl",
                    },
                    root / "jobs/job-2.log",
                )

            command = run_command.call_args.args[0]
            self.assertIn("tvl", command)
            self.assertNotIn("--tokens", command)


if __name__ == "__main__":
    unittest.main()
