import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dashboard import admin
from dashboard.admin import AdminService
from dashboard.snapshot_refresh import (
    SnapshotFactState,
    evaluate_snapshot_refresh,
    read_snapshot_fact_state,
)


def state(
    snapshot_id,
    dataset_sha256,
    status,
    reason_code,
    *,
    retryable=False,
    market_id="cex:upbit:MORPHO/USDT",
    fact_type="depth",
):
    return SnapshotFactState(
        market_id=market_id,
        fact_type=fact_type,
        snapshot_id=snapshot_id,
        dataset_sha256=dataset_sha256,
        observed_at="2026-07-31T12:00:00+00:00",
        status=status,
        reason_code=reason_code,
        retryable=retryable,
        publication_generation="generation",
    )


def cex_row(**changes):
    row = {
        "snapshot_id": "cex-1",
        "observed_at": "2026-07-31T12:00:00+00:00",
        "response_received_at": "2026-07-31T12:00:00+00:00",
        "token_symbol": "AAVE",
        "exchange": "upbit",
        "cex_symbol": "AAVE/USDT",
        "source_instrument": "USDT-AAVE",
        "source_quote_asset": "USDT",
        "quote_conversion_method": "USDT=USD proxy",
        "best_bid": "99",
        "best_ask": "101",
        "midpoint": "100",
        "spread_quote": "2",
        "spread_bps": "200",
        "bid_depth_10bps_usd": "0",
        "ask_depth_10bps_usd": "0",
        "total_depth_10bps_usd": "0",
        "bid_depth_25bps_usd": "0",
        "ask_depth_25bps_usd": "0",
        "total_depth_25bps_usd": "0",
        "bid_depth_50bps_usd": "0",
        "ask_depth_50bps_usd": "0",
        "total_depth_50bps_usd": "0",
        "bid_depth_100bps_usd": "0",
        "ask_depth_100bps_usd": "0",
        "total_depth_100bps_usd": "0",
        "depth_10bps_complete": "1",
        "depth_25bps_complete": "1",
        "depth_50bps_complete": "1",
        "depth_100bps_complete": "1",
        "depth_method": "test",
        "source_endpoint": "https://example.invalid",
        "raw_response_sha256": "a" * 64,
        "status": "observed",
        "error": "",
    }
    row.update(changes)
    if "status" not in changes and "reason_code" not in changes:
        row["reason_code"] = "observed"
    return row


def dex_depth_row(**changes):
    row = {
        "snapshot_id": "dex-1",
        "observed_at": "2026-07-31T12:00:00+00:00",
        "response_received_at": "2026-07-31T12:00:00+00:00",
        "token_symbol": "AAVE",
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0xabc",
        "protocol_model": "constant_product",
        "block_number": "1",
        "fee_bps": "0",
        "pool_state_price_usd": "1",
        "source_target_price_usd": "1",
        "price_difference_bps": "0",
        "sell_depth_10bps_usd": "0",
        "buy_depth_10bps_usd": "0",
        "total_depth_10bps_usd": "0",
        "sell_depth_25bps_usd": "0",
        "buy_depth_25bps_usd": "0",
        "total_depth_25bps_usd": "0",
        "sell_depth_50bps_usd": "0",
        "buy_depth_50bps_usd": "0",
        "total_depth_50bps_usd": "0",
        "sell_depth_100bps_usd": "0",
        "buy_depth_100bps_usd": "0",
        "total_depth_100bps_usd": "0",
        "depth_10bps_complete": "1",
        "depth_25bps_complete": "1",
        "depth_50bps_complete": "1",
        "depth_100bps_complete": "1",
        "depth_method": "test",
        "source_endpoint": "https://example.invalid",
        "raw_response_sha256": "a" * 64,
        "status": "observed",
        "error": "",
    }
    row.update(changes)
    if "status" not in changes and "reason_code" not in changes:
        row["reason_code"] = "observed"
    return row


def tvl_row(**changes):
    row = {
        "snapshot_id": "tvl-1",
        "observed_at": "2026-07-31T12:00:00+00:00",
        "token_symbol": "AAVE",
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0xabc",
        "tvl_usd": "0",
        "tvl_method": "test",
        "source": "test",
        "source_endpoint": "https://example.invalid",
        "raw_response_sha256": "a" * 64,
        "status": "observed",
        "error": "",
    }
    row.update(changes)
    if "status" not in changes and "reason_code" not in changes:
        row["reason_code"] = "observed"
    return row


def unmeasured(row, fields):
    result = dict(row)
    for field in fields:
        result[field] = ""
    return result


CEX_MEASURED = (
    "best_bid", "best_ask", "midpoint", "spread_quote", "spread_bps",
    "bid_depth_10bps_usd", "ask_depth_10bps_usd", "total_depth_10bps_usd",
    "bid_depth_25bps_usd", "ask_depth_25bps_usd", "total_depth_25bps_usd",
    "bid_depth_50bps_usd", "ask_depth_50bps_usd", "total_depth_50bps_usd",
    "bid_depth_100bps_usd", "ask_depth_100bps_usd", "total_depth_100bps_usd",
    "depth_10bps_complete", "depth_25bps_complete", "depth_50bps_complete",
    "depth_100bps_complete",
)
DEX_MEASURED = (
    "fee_bps", "pool_state_price_usd", "source_target_price_usd",
    "price_difference_bps", "sell_depth_10bps_usd", "buy_depth_10bps_usd",
    "total_depth_10bps_usd", "sell_depth_25bps_usd", "buy_depth_25bps_usd",
    "total_depth_25bps_usd", "sell_depth_50bps_usd", "buy_depth_50bps_usd",
    "total_depth_50bps_usd", "sell_depth_100bps_usd", "buy_depth_100bps_usd",
    "total_depth_100bps_usd",
    "depth_10bps_complete", "depth_25bps_complete", "depth_50bps_complete",
    "depth_100bps_complete",
)
def write_rows(path, rows):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


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

    def test_depth_refresh_command_alone_does_not_commit_publication(self):
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

            before = state("s1", "a" * 64, "collection_failed", "network", retryable=True,
                           market_id=job["market_id"])
            with patch.object(service, "_run_command", run_command), patch.object(
                service,
                "_set_job",
                set_job,
            ), patch.object(
                admin, "read_snapshot_fact_state", side_effect=[before, before]
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
            market_option = command.index("--market-id")
            self.assertEqual(command[market_option + 1], job["market_id"])
            self.assertNotIn("--tokens", command)
            set_job.assert_called_once()
            self.assertEqual(set_job.call_args.kwargs["status"], "partial")
            self.assertFalse(set_job.call_args.kwargs["publication_committed"])
            self.assertEqual(
                set_job.call_args.kwargs["error_code"],
                "snapshot_publication_unchanged",
            )

    def test_depth_refresh_commits_only_changed_exact_fact_and_clears_caches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            job = {
                "token_symbol": "AAVE",
                "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
                "market_type": "dex",
                "fact_type": "depth",
            }
            before = state("s1", "a" * 64, "collection_failed", "network", retryable=True,
                           market_id=job["market_id"])
            after = state("s2", "b" * 64, "observed", "observed",
                          market_id=job["market_id"])
            set_job = Mock()
            with patch.object(service, "_run_command") as run_command, patch.object(
                admin, "read_snapshot_fact_state", side_effect=[before, after]
            ), patch("dashboard.server.clear_runtime_caches") as clear_caches, patch.object(
                service, "_set_job", set_job
            ):
                service._run_snapshot_refresh_job("job-1", job, root / "job.log")
            command = run_command.call_args.args[0]
            self.assertIn("dex_depth", command)
            market_option = command.index("--market-id")
            self.assertEqual(command[market_option + 1], job["market_id"])
            self.assertNotIn("--tokens", command)
            clear_caches.assert_called_once_with()
            self.assertEqual(set_job.call_args.kwargs["status"], "succeeded")
            self.assertTrue(set_job.call_args.kwargs["publication_committed"])
            self.assertEqual(set_job.call_args.kwargs["result"]["after"]["status"], "observed")

    def test_after_read_failure_is_partial_and_result_never_exposes_raw_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            before = state("s1", "a" * 64, "collection_failed", "network", retryable=True,
                           market_id="cex:binance:AAVE/USDT")
            fail_job = Mock()
            with patch.object(service, "_run_command"), patch.object(
                admin, "read_snapshot_fact_state", side_effect=[before, ValueError("/private/secret")]
            ), patch("dashboard.server.clear_runtime_caches"), patch.object(service, "_fail_job", fail_job):
                service._run_snapshot_refresh_job(
                    "job-1", {"token_symbol": "AAVE", "market_id": "cex:binance:AAVE/USDT",
                              "market_type": "cex", "fact_type": "depth"}, root / "job.log",
                )
            self.assertEqual(fail_job.call_args.kwargs["status"], "partial")
            self.assertEqual(fail_job.call_args.kwargs["result"], None)

    def test_nonzero_command_is_partial_even_after_a_valid_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            before = state("s1", "a" * 64, "collection_failed", "network", retryable=True,
                           market_id="cex:binance:AAVE/USDT")
            fail_job = Mock()
            with patch.object(service, "_run_command", side_effect=__import__("subprocess").CalledProcessError(1, ["x"])), patch.object(
                admin, "read_snapshot_fact_state", return_value=before
            ), patch.object(service, "_fail_job", fail_job):
                service._run_snapshot_refresh_job(
                    "job-1", {"token_symbol": "AAVE", "market_id": "cex:binance:AAVE/USDT",
                              "market_type": "cex", "fact_type": "depth"}, root / "job.log",
                )
            self.assertEqual(fail_job.call_args.kwargs["status"], "partial")
            self.assertFalse(fail_job.call_args.kwargs["publication_committed"])

    def test_nonzero_command_reports_a_target_publication_that_already_committed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            before = state(
                "s1",
                "a" * 64,
                "collection_failed",
                "network",
                retryable=True,
                market_id="cex:binance:AAVE/USDT",
            )
            after = state(
                "s2",
                "b" * 64,
                "observed",
                "observed",
                market_id="cex:binance:AAVE/USDT",
            )
            set_job = Mock()
            with patch.object(
                service,
                "_run_command",
                side_effect=__import__("subprocess").CalledProcessError(1, ["x"]),
            ), patch.object(
                admin,
                "read_snapshot_fact_state",
                side_effect=[before, after],
            ), patch(
                "dashboard.server.clear_runtime_caches"
            ), patch.object(
                service,
                "_set_job",
                set_job,
            ):
                service._run_snapshot_refresh_job(
                    "job-1",
                    {
                        "token_symbol": "AAVE",
                        "market_id": "cex:binance:AAVE/USDT",
                        "market_type": "cex",
                        "fact_type": "depth",
                    },
                    root / "job.log",
                )

            self.assertEqual(set_job.call_args.kwargs["status"], "partial")
            self.assertTrue(set_job.call_args.kwargs["publication_committed"])
            self.assertEqual(
                set_job.call_args.kwargs["error_code"],
                "snapshot_refresh_failed_after_publication",
            )
            self.assertEqual(
                set_job.call_args.kwargs["result"]["after"]["status"],
                "observed",
            )

    def test_before_read_failure_does_not_invoke_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            run_command = Mock()
            fail_job = Mock()
            with patch.object(service, "_run_command", run_command), patch.object(
                admin, "read_snapshot_fact_state", side_effect=ValueError("secret path")
            ), patch.object(service, "_fail_job", fail_job):
                service._run_snapshot_refresh_job(
                    "job-1",
                    {"token_symbol": "AAVE", "market_id": "cex:binance:AAVE/USDT",
                     "market_type": "cex", "fact_type": "depth"},
                    root / "job.log",
                )
            run_command.assert_not_called()
            self.assertEqual(fail_job.call_args.kwargs["status"], "partial")
            self.assertEqual(fail_job.call_args.kwargs["error_code"], "snapshot_publication_unreadable")

    def test_worker_noops_when_queued_fact_has_already_resolved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(data_dir=root / "data", job_dir=root / "jobs")
            run_command = Mock()
            set_job = Mock()
            resolved = state(
                "s2",
                "b" * 64,
                "observed",
                "observed",
                retryable=False,
                market_id="cex:binance:AAVE/USDT",
            )
            with patch.object(service, "_run_command", run_command), patch.object(
                service, "_set_job", set_job
            ), patch.object(
                admin, "read_snapshot_fact_state", return_value=resolved
            ):
                service._run_snapshot_refresh_job(
                    "job-1",
                    {
                        "token_symbol": "AAVE",
                        "market_id": "cex:binance:AAVE/USDT",
                        "market_type": "cex",
                        "fact_type": "depth",
                    },
                    root / "job.log",
                )

            run_command.assert_not_called()
            self.assertEqual(set_job.call_args.kwargs["status"], "succeeded")
            self.assertFalse(set_job.call_args.kwargs["publication_committed"])
            self.assertEqual(
                set_job.call_args.kwargs["result"]["outcome"],
                "already_resolved",
            )

    def test_tvl_refresh_uses_published_tvl_profile_without_fake_token_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                data_dir=root / "data",
                job_dir=root / "jobs",
            )
            run_command = Mock()
            before = state("s1", "a" * 64, "collection_failed", "network", retryable=True,
                           market_id="dex:eth:uniswap_v3:0xabc:AAVE", fact_type="tvl")
            after = state("s2", "b" * 64, "observed", "observed",
                          market_id="dex:eth:uniswap_v3:0xabc:AAVE", fact_type="tvl")
            with patch.object(service, "_run_command", run_command), patch.object(
                service,
                "_set_job",
            ), patch.object(
                admin, "read_snapshot_fact_state", side_effect=[before, after]
            ), patch("dashboard.server.clear_runtime_caches"):
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
            market_option = command.index("--market-id")
            self.assertEqual(
                command[market_option + 1],
                "dex:eth:uniswap_v3:0xabc:AAVE",
            )
            self.assertNotIn("--tokens", command)


class SnapshotPostconditionTest(unittest.TestCase):
    def test_missing_tvl_row_remains_retryable_for_safe_exact_insert(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xdef:AAVE",
            "fact_type": "tvl",
        }
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            write_rows(
                data_dir / "dex_pool_tvl_latest.csv",
                [
                    tvl_row(
                        token_symbol="UNI",
                        pool_address="0xabc",
                    )
                ],
            )
            result = read_snapshot_fact_state(data_dir, request)

        self.assertEqual(result.status, "not_cataloged_in_snapshot")
        self.assertEqual(
            result.reason_code,
            "tvl_market_not_cataloged_in_snapshot",
        )
        self.assertTrue(result.retryable)

    def test_changed_non_target_row_does_not_refresh_unchanged_exact_target(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            publication = data_dir / "cex_depth_latest.csv"
            write_rows(publication, [
                cex_row(),
                cex_row(token_symbol="UNI", cex_symbol="UNI/USDT"),
            ])
            before = read_snapshot_fact_state(data_dir, request)
            write_rows(publication, [
                cex_row(snapshot_id="cex-2"),
                cex_row(
                    snapshot_id="cex-2",
                    token_symbol="UNI",
                    cex_symbol="UNI/USDT",
                    raw_response_sha256="b" * 64,
                ),
            ])
            after = read_snapshot_fact_state(data_dir, request)

        result = evaluate_snapshot_refresh(before, after)

        self.assertNotEqual(before.dataset_sha256, after.dataset_sha256)
        self.assertEqual(before.target_fingerprint, after.target_fingerprint)
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_publication_unchanged")

    def test_changed_exact_target_evidence_refreshes_new_publication(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            publication = data_dir / "cex_depth_latest.csv"
            write_rows(publication, [cex_row()])
            before = read_snapshot_fact_state(data_dir, request)
            write_rows(publication, [
                cex_row(
                    snapshot_id="cex-2",
                    raw_response_sha256="b" * 64,
                ),
            ])
            after = read_snapshot_fact_state(data_dir, request)

        result = evaluate_snapshot_refresh(before, after)

        self.assertNotEqual(before.target_fingerprint, after.target_fingerprint)
        self.assertTrue(result.succeeded)
        self.assertEqual(result.resolution, "observed")

    def test_unchanged_publication_is_not_success(self):
        before = state("s1", "a" * 64, "not_cataloged_in_snapshot", None)
        result = evaluate_snapshot_refresh(before, before)
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_publication_unchanged")

    def test_success_requires_a_new_nonempty_source_snapshot_id(self):
        cases = ((None, None, "snapshot_publication_identity_invalid"),
                 (None, "", "snapshot_publication_identity_invalid"),
                 ("s1", "s1", "snapshot_publication_unchanged"))
        for before_id, after_id, expected_error in cases:
            with self.subTest(before_id=before_id, after_id=after_id):
                result = evaluate_snapshot_refresh(
                    state(before_id, "a" * 64, "collection_failed", "network", retryable=True),
                    state(after_id, "b" * 64, "observed", "observed"),
                )
                self.assertFalse(result.succeeded)
                self.assertEqual(result.error_code, expected_error)

    def test_same_snapshot_with_different_hash_and_different_snapshot_with_same_hash_fail(self):
        for before, after in (
            (state("s1", "a" * 64, "collection_failed", "network", retryable=True),
             state("s1", "b" * 64, "observed", "observed")),
            (state("s1", "a" * 64, "collection_failed", "network", retryable=True),
             state("s2", "a" * 64, "observed", "observed")),
        ):
            result = evaluate_snapshot_refresh(before, after)
            self.assertFalse(result.succeeded)
            self.assertEqual(result.error_code, "snapshot_publication_unchanged")

    def test_unrelated_market_change_is_not_success(self):
        result = evaluate_snapshot_refresh(
            state("s1", "a" * 64, "not_cataloged_in_snapshot", None),
            state("s2", "b" * 64, "observed", "observed",
                  market_id="cex:upbit:AAVE/USDT"),
        )
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_mismatch")

    def test_new_publication_without_target_fact_is_not_success(self):
        result = evaluate_snapshot_refresh(
            state("s1", "a" * 64, "not_cataloged_in_snapshot", "not_cataloged_in_snapshot", retryable=True),
            state("s2", "b" * 64, "not_cataloged_in_snapshot", "not_cataloged_in_snapshot", retryable=True),
        )
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_unresolved")

    def test_new_retryable_failure_is_not_success(self):
        result = evaluate_snapshot_refresh(
            state("s1", "a" * 64, "collection_failed", "network", retryable=True),
            state("s2", "b" * 64, "collection_failed", "network", retryable=True),
        )
        self.assertFalse(result.succeeded)
        self.assertTrue(result.retryable)

    def test_new_exact_observation_succeeds(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            publication = data_dir / "cex_depth_latest.csv"
            write_rows(publication, [
                cex_row(token_symbol="UNI", cex_symbol="UNI/USDT"),
            ])
            before = read_snapshot_fact_state(data_dir, request)
            write_rows(publication, [cex_row(snapshot_id="cex-2")])
            after = read_snapshot_fact_state(data_dir, request)

        result = evaluate_snapshot_refresh(before, after)

        self.assertIsNone(before.target_fingerprint)
        self.assertIsNotNone(after.target_fingerprint)
        self.assertTrue(result.succeeded)
        self.assertEqual(result.resolution, "observed")

    def test_only_allowlisted_terminal_outcomes_succeed(self):
        for status, reason in (
            ("source_no_observation", "source_no_two_sided_book"),
            ("source_no_observation", "source_no_order_book"),
            ("unsupported", "unsupported_chain"),
            ("unsupported", "unsupported_protocol"),
        ):
            with self.subTest(status=status, reason=reason):
                result = evaluate_snapshot_refresh(
                    state("s1", "a" * 64, "collection_failed", "network", retryable=True),
                    state("s2", "b" * 64, status, reason),
                )
                self.assertTrue(result.succeeded)

    def test_partial_outcome_does_not_resolve_refresh(self):
        result = evaluate_snapshot_refresh(
            state("s1", "a" * 64, "collection_failed", "network", retryable=True),
            state("s2", "b" * 64, "partial", "source_level_limit"),
        )

        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_unresolved")

    def test_unknown_observed_or_partial_reason_fails_closed(self):
        for status in ("observed", "partial"):
            result = evaluate_snapshot_refresh(
                state("s1", "a" * 64, "collection_failed", "network", retryable=True),
                state("s2", "b" * 64, status, "unknown"),
            )
            self.assertFalse(result.succeeded)
            self.assertEqual(result.error_code, "snapshot_target_unresolved")


class SnapshotFactReaderTest(unittest.TestCase):
    def read(self, filename, request, rows):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            write_rows(data_dir / filename, rows)
            return read_snapshot_fact_state(data_dir, request)

    def test_reads_zero_as_a_measured_cex_observation(self):
        result = self.read("cex_depth_latest.csv", {
            "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
        }, [cex_row()])
        self.assertEqual(result.status, "observed")
        self.assertEqual(result.reason_code, "observed")
        self.assertTrue(result.publication_generation)

    def test_reads_legacy_coinbase_nanosecond_timestamp(self):
        result = self.read(
            "cex_depth_latest.csv",
            {
                "token_symbol": "AAVE",
                "market_id": "cex:coinbase:AAVE/USDT",
                "fact_type": "depth",
            },
            [
                cex_row(
                    exchange="coinbase",
                    source_instrument="AAVE-USDT",
                    observed_at="2026-07-31T23:05:47.660676312Z",
                )
            ],
        )

        self.assertEqual(
            result.observed_at,
            "2026-07-31T23:05:47.660676+00:00",
        )

    def test_rejects_blank_negative_nan_and_infinite_measured_values(self):
        for value in ("", "-1", "NaN", "inf", "-Infinity"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.read("cex_depth_latest.csv", {
                        "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
                    }, [cex_row(best_bid=value)])

    def test_rejects_locked_and_crossed_cex_books(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        for best_bid in ("101", "102"):
            with self.subTest(best_bid=best_bid):
                with self.assertRaises(ValueError):
                    self.read(
                        "cex_depth_latest.csv",
                        request,
                        [cex_row(best_bid=best_bid)],
                    )

    def test_rejects_zero_cex_best_bid_even_when_book_arithmetic_is_consistent(self):
        with self.assertRaises(ValueError):
            self.read(
                "cex_depth_latest.csv",
                {
                    "token_symbol": "AAVE",
                    "market_id": "cex:upbit:AAVE/USDT",
                    "fact_type": "depth",
                },
                [cex_row(
                    best_bid="0",
                    best_ask="1",
                    midpoint="0.5",
                    spread_quote="1",
                    spread_bps="20000",
                )],
            )

    def test_rejects_inconsistent_cex_midpoint_and_spread_metrics(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        for field, value in (
            ("midpoint", "99"),
            ("spread_quote", "3"),
            ("spread_bps", "201"),
        ):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    self.read(
                        "cex_depth_latest.csv",
                        request,
                        [cex_row(**{field: value})],
                    )

    def test_rejects_decreasing_cex_depth_bands(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        for side in ("bid", "ask", "total"):
            changes = {
                "{}_depth_10bps_usd".format(side): "2",
                "{}_depth_25bps_usd".format(side): "1",
                "{}_depth_50bps_usd".format(side): "1",
                "{}_depth_100bps_usd".format(side): "1",
            }
            with self.subTest(side=side):
                with self.assertRaises(ValueError):
                    self.read(
                        "cex_depth_latest.csv",
                        request,
                        [cex_row(**changes)],
                    )

    def test_rejects_cex_depth_totals_inconsistent_with_sides(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        row = cex_row(
            bid_depth_10bps_usd="1",
            bid_depth_25bps_usd="1",
            bid_depth_50bps_usd="1",
            bid_depth_100bps_usd="1",
        )

        with self.assertRaises(ValueError):
            self.read("cex_depth_latest.csv", request, [row])

    def test_rejects_depth_status_and_completeness_conflicts(self):
        cex_request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        dex_request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        cases = (
            (
                "cex observed incomplete",
                "cex_depth_latest.csv",
                cex_request,
                cex_row(depth_100bps_complete="0"),
            ),
            (
                "cex partial complete",
                "cex_depth_latest.csv",
                cex_request,
                cex_row(status="partial", reason_code="source_level_limit"),
            ),
            (
                "dex observed incomplete",
                "dex_depth_latest.csv",
                dex_request,
                dex_depth_row(depth_100bps_complete="0"),
            ),
            (
                "dex partial complete",
                "dex_depth_latest.csv",
                dex_request,
                dex_depth_row(status="partial"),
            ),
        )
        for name, filename, request, row in cases:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    self.read(filename, request, [row])

    def test_rejects_completeness_that_recovers_at_a_wider_band(self):
        cases = (
            (
                "cex_depth_latest.csv",
                {
                    "token_symbol": "AAVE",
                    "market_id": "cex:upbit:AAVE/USDT",
                    "fact_type": "depth",
                },
                cex_row(
                    status="partial",
                    reason_code="source_level_limit",
                    depth_10bps_complete="0",
                    depth_25bps_complete="1",
                    depth_50bps_complete="0",
                    depth_100bps_complete="0",
                ),
            ),
            (
                "dex_depth_latest.csv",
                {
                    "token_symbol": "AAVE",
                    "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
                    "fact_type": "depth",
                },
                dex_depth_row(
                    status="partial",
                    depth_10bps_complete="0",
                    depth_25bps_complete="1",
                    depth_50bps_complete="0",
                    depth_100bps_complete="0",
                ),
            ),
        )
        for filename, request, row in cases:
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    self.read(filename, request, [row])

    def test_requires_canonical_raw_hash_for_measured_and_terminal_source_rows(self):
        cex_request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        dex_depth_request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        tvl_request = {**dex_depth_request, "fact_type": "tvl"}
        cases = (
            ("cex measured blank", "cex_depth_latest.csv", cex_request,
             cex_row(raw_response_sha256="")),
            ("cex measured nonhex", "cex_depth_latest.csv", cex_request,
             cex_row(raw_response_sha256="g" * 64)),
            ("cex terminal blank", "cex_depth_latest.csv", cex_request,
             unmeasured(cex_row(
                 status="failed",
                 reason_code="source_no_order_book",
                 raw_response_sha256="",
             ), CEX_MEASURED)),
            ("dex measured blank", "dex_depth_latest.csv", dex_depth_request,
             dex_depth_row(raw_response_sha256="")),
            ("dex failed blank", "dex_depth_latest.csv", dex_depth_request,
             unmeasured(dex_depth_row(
                 status="failed",
                 raw_response_sha256="",
             ), DEX_MEASURED)),
            ("tvl measured blank", "dex_pool_tvl_latest.csv", tvl_request,
             tvl_row(raw_response_sha256="")),
            ("tvl terminal blank", "dex_pool_tvl_latest.csv", tvl_request,
             tvl_row(status="not_found", tvl_usd="", raw_response_sha256="")),
        )
        for name, filename, request, row in cases:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    self.read(filename, request, [row])

    def test_accepts_actual_locally_classified_dex_unsupported_row_without_hash(self):
        from scripts.fetch_dex_depth import unsupported_row

        row = unsupported_row(
            {
                "snapshot_id": "tvl-1",
                "observed_at": "2026-07-31T11:59:00+00:00",
                "response_received_at": "2026-07-31T11:59:00+00:00",
                "token_symbol": "AAVE",
                "chain": "eth",
                "dex": "uniswap_v3",
                "pool_address": "0xabc",
                "source": "test",
                "source_endpoint": "https://example.invalid",
                "raw_response_sha256": "b" * 64,
            },
            snapshot_id="dex-1",
            request_started_at="2026-07-31T12:00:00+00:00",
            response_received_at="2026-07-31T12:00:01+00:00",
            reason="unsupported_pool_model: test",
        )

        result = self.read(
            "dex_depth_latest.csv",
            {
                "token_symbol": "AAVE",
                "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
                "fact_type": "depth",
            },
            [row],
        )

        self.assertEqual(row["raw_response_sha256"], "")
        self.assertEqual(result.status, "unsupported")
        self.assertFalse(result.retryable)

    def test_retryable_cex_may_lack_hash_but_failed_tvl_requires_one(self):
        cex = self.read(
            "cex_depth_latest.csv",
            {
                "token_symbol": "AAVE",
                "market_id": "cex:upbit:AAVE/USDT",
                "fact_type": "depth",
            },
            [unmeasured(cex_row(
                status="failed",
                reason_code="network",
                raw_response_sha256="",
            ), CEX_MEASURED)],
        )
        self.assertEqual(cex.status, "collection_failed")
        self.assertTrue(cex.retryable)
        with self.assertRaisesRegex(ValueError, "source.*hash"):
            self.read(
                "dex_pool_tvl_latest.csv",
                {
                    "token_symbol": "AAVE",
                    "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
                    "fact_type": "tvl",
                },
                [tvl_row(
                    status="failed",
                    reason_code="network",
                    tvl_usd="",
                    raw_response_sha256="",
                )],
            )

    def test_rejects_duplicate_target_non_target_and_normalized_identity(self):
        base = cex_row()
        duplicate_target = cex_row()
        duplicate_other = cex_row(token_symbol="UNI", cex_symbol="UNI/USDT")
        duplicate_other_case = cex_row(token_symbol="uni", cex_symbol="uni/usdt")
        for rows in ((base, duplicate_target), (base, duplicate_other, duplicate_other_case)):
            with self.subTest(rows=len(rows)):
                with self.assertRaises(ValueError):
                    self.read("cex_depth_latest.csv", {
                        "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
                    }, list(rows))

    def test_rejects_mixed_snapshot_ids_and_naive_timestamp(self):
        for row in (cex_row(snapshot_id=""), cex_row(observed_at="2026-07-31T12:00:00")):
            with self.subTest(row=row):
                with self.assertRaises(ValueError):
                    self.read("cex_depth_latest.csv", {
                        "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
                    }, [row])

    def test_rejects_wrong_file_fact_and_malformed_market_id(self):
        with self.assertRaises(ValueError):
            self.read("dex_pool_tvl_latest.csv", {
                "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
            }, [tvl_row()])
        with self.assertRaises(ValueError):
            self.read("cex_depth_latest.csv", {
                "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE:USDT:extra", "fact_type": "depth",
            }, [cex_row()])

    def test_valid_publication_with_a_different_exact_market_is_unresolved(self):
        result = self.read("cex_depth_latest.csv", {
            "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/KRW", "fact_type": "depth",
        }, [cex_row()])
        self.assertEqual(result.status, "not_cataloged_in_snapshot")
        self.assertIsNone(result.reason_code)

    def test_unknown_non_target_status_invalidates_the_whole_publication(self):
        with self.assertRaises(ValueError):
            self.read("cex_depth_latest.csv", {
                "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
            }, [cex_row(), unmeasured(
                cex_row(token_symbol="UNI", cex_symbol="UNI/USDT", status="mystery"),
                CEX_MEASURED,
            )])

    def test_rejects_conflicting_cex_raw_status_and_reason(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/USDT",
            "fact_type": "depth",
        }
        rows = (
            unmeasured(
                cex_row(status="failed", reason_code="observed"),
                CEX_MEASURED,
            ),
            cex_row(status="observed", reason_code="network"),
        )
        for row in rows:
            with self.subTest(status=row["status"], reason_code=row["reason_code"]):
                with self.assertRaises(ValueError):
                    self.read("cex_depth_latest.csv", request, [row])

    def test_fail_closed_cex_and_bounded_dex_tvl_normalization(self):
        cex = self.read("cex_depth_latest.csv", {
            "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
        }, [unmeasured(cex_row(reason_code="unknown", status="failed"), CEX_MEASURED)])
        self.assertIsNone(cex.reason_code)
        dex = self.read("dex_depth_latest.csv", {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
        }, [unmeasured(dex_depth_row(status="unsupported", error="untrusted: secret"), DEX_MEASURED)])
        self.assertEqual((dex.status, dex.reason_code), ("unsupported", "unsupported_source"))
        self.assertNotIn("secret", repr(dex))
        tvl = self.read("dex_pool_tvl_latest.csv", {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "tvl",
        }, [tvl_row(status="failed", tvl_usd="", error="private endpoint failure")])
        self.assertEqual(
            (tvl.status, tvl.reason_code),
            ("collection_failed", "collection_failed"),
        )
        self.assertTrue(tvl.retryable)
        self.assertNotIn("private", repr(tvl))

    def test_dex_and_tvl_zero_are_valid_but_blank_observed_values_are_invalid(self):
        dex_request = {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
        }
        result = self.read("dex_depth_latest.csv", dex_request, [dex_depth_row()])
        self.assertEqual(result.status, "observed")
        with self.assertRaises(ValueError):
            self.read("dex_depth_latest.csv", dex_request, [dex_depth_row(sell_depth_10bps_usd="")])
        tvl_request = {**dex_request, "fact_type": "tvl"}
        result = self.read("dex_pool_tvl_latest.csv", tvl_request, [tvl_row(tvl_usd="0")])
        self.assertEqual(result.status, "observed")
        with self.assertRaises(ValueError):
            self.read("dex_pool_tvl_latest.csv", tvl_request, [tvl_row(tvl_usd="")])

    def test_failed_tvl_uses_shared_retryable_outcome(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "tvl",
        }
        result = self.read(
            "dex_pool_tvl_latest.csv",
            request,
            [tvl_row(status="failed", tvl_usd="", error="private failure")],
        )

        self.assertEqual(result.status, "collection_failed")
        self.assertEqual(result.reason_code, "collection_failed")
        self.assertTrue(result.retryable)
        self.assertNotIn("private", repr(result))

    def test_snapshot_refresh_preserves_bounded_tvl_and_dex_depth_reasons(self):
        dex_request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        depth = self.read(
            "dex_depth_latest.csv",
            dex_request,
            [unmeasured(dex_depth_row(
                status="failed",
                reason_code="depth_usd_price_time_mismatch",
            ), DEX_MEASURED)],
        )
        tvl = self.read(
            "dex_pool_tvl_latest.csv",
            {**dex_request, "fact_type": "tvl"},
            [tvl_row(
                status="failed",
                reason_code="rate_limit",
                tvl_usd="",
            )],
        )
        self.assertEqual(
            (depth.status, depth.reason_code),
            ("collection_failed", "depth_usd_price_time_mismatch"),
        )
        self.assertEqual(
            (tvl.status, tvl.reason_code),
            ("collection_failed", "rate_limit"),
        )

    def test_legacy_snapshot_without_reason_uses_generic_or_status_proven_reason(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "tvl",
        }
        legacy_failed = tvl_row(status="failed", tvl_usd="")
        legacy_failed.pop("reason_code", None)
        legacy_missing = tvl_row(status="missing", tvl_usd="")
        legacy_missing.pop("reason_code", None)
        failed = self.read(
            "dex_pool_tvl_latest.csv", request, [legacy_failed]
        )
        missing = self.read(
            "dex_pool_tvl_latest.csv", request, [legacy_missing]
        )
        self.assertEqual(failed.reason_code, "collection_failed")
        self.assertEqual(missing.reason_code, "source_no_tvl_observation")

    def test_snapshot_refresh_rejects_unknown_tvl_and_dex_depth_reasons(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        cases = (
            (
                "dex_depth_latest.csv",
                request,
                [unmeasured(dex_depth_row(
                    status="failed",
                    reason_code="",
                ), DEX_MEASURED)],
            ),
            (
                "dex_pool_tvl_latest.csv",
                {**request, "fact_type": "tvl"},
                [tvl_row(
                    status="failed",
                    reason_code="",
                    tvl_usd="",
                )],
            ),
            (
                "dex_depth_latest.csv",
                request,
                [unmeasured(dex_depth_row(
                    status="failed",
                    reason_code="private_raw_error",
                ), DEX_MEASURED)],
            ),
            (
                "dex_pool_tvl_latest.csv",
                {**request, "fact_type": "tvl"},
                [tvl_row(
                    status="failed",
                    reason_code="private_raw_error",
                    tvl_usd="",
                )],
            ),
        )
        for filename, target, rows in cases:
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    self.read(filename, target, rows)

    def test_rejects_decreasing_dex_depth_bands(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        for side in ("sell", "buy", "total"):
            changes = {
                "{}_depth_10bps_usd".format(side): "2",
                "{}_depth_25bps_usd".format(side): "1",
                "{}_depth_50bps_usd".format(side): "1",
                "{}_depth_100bps_usd".format(side): "1",
            }
            with self.subTest(side=side):
                with self.assertRaises(ValueError):
                    self.read(
                        "dex_depth_latest.csv",
                        request,
                        [dex_depth_row(**changes)],
                    )

    def test_rejects_dex_depth_totals_inconsistent_with_sides(self):
        request = {
            "token_symbol": "AAVE",
            "market_id": "dex:eth:uniswap_v3:0xabc:AAVE",
            "fact_type": "depth",
        }
        row = dex_depth_row(
            sell_depth_10bps_usd="0.00000000000000000001",
            sell_depth_25bps_usd="0.00000000000000000001",
            sell_depth_50bps_usd="0.00000000000000000001",
            sell_depth_100bps_usd="0.00000000000000000001",
        )

        with self.assertRaises(ValueError):
            self.read("dex_depth_latest.csv", request, [row])

    def test_producer_failed_dex_row_retains_block_provenance_but_no_measurements(self):
        request = {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
        }
        failed = unmeasured(
            dex_depth_row(status="failed", block_number="123", error="RpcError: unavailable"),
            DEX_MEASURED,
        )
        after = self.read("dex_depth_latest.csv", request, [failed])
        self.assertEqual(after.status, "collection_failed")
        self.assertEqual(after.reason_code, "collection_failed")
        self.assertTrue(after.retryable)
        result = evaluate_snapshot_refresh(
            state("before", "a" * 64, "collection_failed", "network", retryable=True,
                  market_id=request["market_id"]),
            after,
        )
        self.assertFalse(result.succeeded)
        self.assertEqual(result.error_code, "snapshot_target_unresolved")
        self.assertTrue(result.retryable)
        for value in ("NaN", "-1.5"):
            with self.subTest(block_number=value):
                invalid = unmeasured(dex_depth_row(status="failed", block_number=value), DEX_MEASURED)
                with self.assertRaises(ValueError):
                    self.read("dex_depth_latest.csv", request, [invalid])

    def test_complete_dex_row_requires_complete_measurements_and_normalizes_observed(self):
        request = {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
        }
        complete = self.read("dex_depth_latest.csv", request, [dex_depth_row(status="complete")])
        self.assertEqual((complete.status, complete.reason_code), ("observed", "observed"))
        for field, value in (("sell_depth_10bps_usd", ""), ("pool_state_price_usd", "NaN")):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    self.read("dex_depth_latest.csv", request, [dex_depth_row(status="complete", **{field: value})])

    def test_dex_block_identity_status_matrix(self):
        request = {
            "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "depth",
        }

        def row_for(status, block_number):
            row = dex_depth_row(status=status, block_number=block_number)
            if status == "partial":
                row["depth_100bps_complete"] = "0"
            if status in {"failed", "unsupported"}:
                row = unmeasured(row, DEX_MEASURED)
            return row

        for status in ("observed", "partial", "complete"):
            with self.subTest(status=status, block_number=""):
                with self.assertRaises(ValueError):
                    self.read("dex_depth_latest.csv", request, [row_for(status, "")])
            with self.subTest(status=status, block_number="123"):
                self.read("dex_depth_latest.csv", request, [row_for(status, "123")])
        for status in ("failed", "unsupported"):
            for block_number in ("", "123"):
                with self.subTest(status=status, block_number=block_number):
                    self.read("dex_depth_latest.csv", request, [row_for(status, block_number)])
        for status in ("observed", "partial", "complete", "failed", "unsupported"):
            for block_number in ("-1", "1.5", "NaN", "not-a-number", "²", "١", "１２"):
                with self.subTest(status=status, block_number=block_number):
                    with self.assertRaises(ValueError):
                        self.read("dex_depth_latest.csv", request, [row_for(status, block_number)])

    def test_mixed_snapshot_ids_are_rejected_even_for_non_target_rows(self):
        with self.assertRaises(ValueError):
            self.read("cex_depth_latest.csv", {
                "token_symbol": "AAVE", "market_id": "cex:upbit:AAVE/USDT", "fact_type": "depth",
            }, [cex_row(), cex_row(snapshot_id="cex-2", token_symbol="UNI", cex_symbol="UNI/USDT")])

    def test_tvl_conflicting_dex_labels_for_one_pool_is_invalid(self):
        with self.assertRaises(ValueError):
            self.read("dex_pool_tvl_latest.csv", {
                "token_symbol": "AAVE", "market_id": "dex:eth:uniswap_v3:0xabc:AAVE", "fact_type": "tvl",
            }, [tvl_row(), tvl_row(dex="another_dex")])


if __name__ == "__main__":
    unittest.main()
