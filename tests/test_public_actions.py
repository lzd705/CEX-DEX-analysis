import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from dashboard import server
from dashboard.admin import (
    AdminActionError,
    AdminService,
    AdminWorkerStartError,
)
from dashboard.public_actions import (
    PUBLIC_ADD_TOKEN_ACTOR,
    PUBLIC_FACT_REFRESH_ACTOR,
    PUBLIC_FACT_REFRESH_PATH,
    PUBLIC_QUALITY_RETRY_ACTOR,
    PUBLIC_QUALITY_RETRYABLE_PATH,
    PUBLIC_QUALITY_RETRY_PATH,
    PUBLIC_TOKEN_ADD_PATH,
    PUBLIC_TOKEN_HISTORY_DAYS,
    PUBLIC_TOKEN_RESOLVE_PATH,
    MAX_RATE_LIMIT_BUCKETS,
    PublicActionError,
    PublicActionPolicy,
    public_job,
    public_retry_window,
    public_token_candidate,
    require_exact_string_fields,
)


class PublicActionPolicyTest(unittest.TestCase):
    def test_public_snapshot_job_result_exposes_only_bounded_snapshot_fields(self):
        response = public_job({
            "job_id": "job-1",
            "job_type": "snapshot_refresh",
            "status": "partial",
            "error_code": "snapshot_publication_unreadable",
            "result": {
                "before": {
                    "publication_generation": "a" * 16 + ":s1",
                    "snapshot_id": "s1",
                    "status": "collection_failed",
                    "reason_code": "network",
                    "observed_at": "2026-07-31T12:00:00+00:00",
                    "path": "/private/secret.csv",
                    "error": "private raw collection error",
                },
                "after": {
                    "publication_generation": "b" * 16 + ":s2",
                    "snapshot_id": "s2",
                    "status": "observed",
                    "reason_code": "observed",
                    "observed_at": "2026-07-31T12:01:00+00:00",
                    "raw_error": "private raw collection error",
                },
            },
        })
        summary = response["result_summary"]
        self.assertEqual(summary["after"]["status"], "observed")
        self.assertNotIn("path", summary["before"])
        self.assertNotIn("error", summary["before"])
        self.assertNotIn("raw_error", summary["after"])
    def test_feature_flags_are_independent_and_fail_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            disabled = PublicActionPolicy()
        self.assertFalse(disabled.enabled_for_path(PUBLIC_TOKEN_ADD_PATH))
        self.assertFalse(
            disabled.enabled_for_path(PUBLIC_QUALITY_RETRY_PATH)
        )
        self.assertFalse(disabled.enabled_for_path(PUBLIC_FACT_REFRESH_PATH))

        enabled = PublicActionPolicy(
            add_token_enabled=True,
            quality_retry_enabled=False,
        )
        self.assertTrue(enabled.enabled_for_path(PUBLIC_TOKEN_RESOLVE_PATH))
        self.assertTrue(enabled.enabled_for_path(PUBLIC_TOKEN_ADD_PATH))
        self.assertFalse(
            enabled.enabled_for_path(PUBLIC_QUALITY_RETRYABLE_PATH)
        )

        fact_enabled = PublicActionPolicy(fact_refresh_enabled=True)
        self.assertTrue(
            fact_enabled.enabled_for_path(PUBLIC_FACT_REFRESH_PATH)
        )
        self.assertFalse(fact_enabled.enabled_for_path(PUBLIC_TOKEN_ADD_PATH))

    def test_rate_limit_is_per_client_and_returns_retry_after(self):
        clock = [100.0]
        policy = PublicActionPolicy(
            add_token_enabled=True,
            monotonic=lambda: clock[0],
        )
        for _ in range(12):
            with policy.permit("token_resolve", "203.0.113.10"):
                pass

        with self.assertRaises(PublicActionError) as context:
            with policy.permit("token_resolve", "203.0.113.10"):
                pass
        self.assertEqual(
            context.exception.code,
            "public_rate_limit_exceeded",
        )
        self.assertGreater(context.exception.retry_after_seconds, 0)

        with policy.permit("token_resolve", "203.0.113.11"):
            pass
        clock[0] += 15 * 60
        with policy.permit("token_resolve", "203.0.113.10"):
            pass

    def test_public_mutations_share_one_concurrency_gate(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        policy = PublicActionPolicy(
            add_token_enabled=True,
            quality_retry_enabled=True,
        )

        with policy.permit(
            "token_add",
            "203.0.113.10",
            service=service,
        ):
            for _ in range(10):
                with self.assertRaises(PublicActionError) as context:
                    with policy.permit(
                        "quality_retry",
                        "203.0.113.11",
                        service=service,
                    ):
                        pass
        self.assertEqual(context.exception.code, "public_action_busy")

        # Busy rejections do not consume the successful-action rate budget.
        with policy.permit(
            "quality_retry",
            "203.0.113.11",
            service=service,
        ):
            pass

    def test_daily_budget_counts_persisted_accepted_jobs(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 3
        now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
        policy = PublicActionPolicy(
            add_token_enabled=True,
            utc_now=lambda: now,
        )

        with self.assertRaises(PublicActionError) as context:
            with policy.permit(
                "token_add",
                "203.0.113.10",
                service=service,
            ):
                pass

        self.assertEqual(
            context.exception.code,
            "public_daily_budget_exhausted",
        )
        self.assertEqual(
            context.exception.retry_after_seconds,
            12 * 60 * 60,
        )
        service.count_jobs_created_on.assert_called_once_with(
            requested_by=PUBLIC_ADD_TOKEN_ACTOR,
            job_type="token_onboarding",
            created_on=date(2026, 7, 30),
        )

    def test_rate_limit_accounting_has_a_hard_memory_bound(self):
        policy = PublicActionPolicy(add_token_enabled=True)
        for index in range(MAX_RATE_LIMIT_BUCKETS + 20):
            with policy.permit("token_resolve", f"198.51.100.{index}"):
                pass

        self.assertEqual(
            len(policy._request_times),
            MAX_RATE_LIMIT_BUCKETS,
        )

    def test_admin_service_job_budget_uses_utc_and_exact_actor_type(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(job_dir=Path(directory))
            service.jobs = {
                "included": {
                    "requested_by": PUBLIC_ADD_TOKEN_ACTOR,
                    "job_type": "token_onboarding",
                    "created_at": "2026-07-30T01:00:00+00:00",
                },
                "same-utc-day-offset": {
                    "requested_by": PUBLIC_ADD_TOKEN_ACTOR,
                    "job_type": "token_onboarding",
                    "created_at": "2026-07-30T23:00:00-04:00",
                },
                "wrong-actor": {
                    "requested_by": "admin",
                    "job_type": "token_onboarding",
                    "created_at": "2026-07-30T02:00:00+00:00",
                },
                "malformed": {
                    "requested_by": PUBLIC_ADD_TOKEN_ACTOR,
                    "job_type": "token_onboarding",
                    "created_at": "not-a-date",
                },
            }

            self.assertEqual(
                service.count_jobs_created_on(
                    requested_by=PUBLIC_ADD_TOKEN_ACTOR,
                    job_type="token_onboarding",
                    created_on=date(2026, 7, 30),
                ),
                1,
            )
            self.assertEqual(
                service.count_jobs_created_on(
                    requested_by=PUBLIC_ADD_TOKEN_ACTOR,
                    job_type="token_onboarding",
                    created_on=date(2026, 7, 31),
                ),
                1,
            )

    def test_retry_authorization_is_rechecked_after_collection_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                job_dir=root / "jobs",
                collection_lock_path=root / "collection.lock",
            )
            job = {
                "job_id": "retry-job",
                "job_type": "retry_failed",
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
                "quality_dataset_snapshot_id": "snapshot-before",
                "quality_import_run_id": "import-before",
                "market_types": ["cex"],
                "market_ids": ["cex:binance:AAVE/USDT"],
                "reason_codes": ["rate_limit"],
                "issue_ids": ["issue-1"],
                "expected_observations": [
                    {
                        "market_id": "cex:binance:AAVE/USDT",
                        "date": "2026-07-29",
                    }
                ],
                "requested_by": PUBLIC_QUALITY_RETRY_ACTOR,
                "status": "queued",
                "stage": "queued",
                "created_at": "2026-07-30T12:00:00+00:00",
                "started_at": None,
                "finished_at": None,
                "error": None,
            }
            service.jobs[job["job_id"]] = job

            with patch.object(
                service,
                "revalidate_retry_authorization",
                side_effect=AdminActionError(
                    "retry_authorization_changed",
                    "publication changed",
                    retryable=True,
                ),
            ), patch.object(service, "_run_refresh_job") as run_refresh:
                service._run_job(job["job_id"])

            run_refresh.assert_not_called()
            failed = service.jobs[job["job_id"]]
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(
                failed["stage"],
                "authorize_retry",
            )
            self.assertEqual(
                failed["error_code"],
                "retry_authorization_expired",
            )
            self.assertTrue(failed["retryable"])

    def test_job_acceptance_response_is_immutable_across_worker_start(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(job_dir=Path(directory))
            request = {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
            }

            def start_and_mutate(job_id):
                service.jobs[job_id]["status"] = "running"
                service.jobs[job_id]["stage"] = "collect_daily_facts"

            with patch.object(
                service,
                "validate_job",
                return_value=request,
            ), patch.object(
                service,
                "_start_job_thread",
                side_effect=start_and_mutate,
            ):
                response = service.create_job({}, "admin")

            self.assertEqual(response["status"], "queued")
            self.assertEqual(response["stage"], "queued")
            self.assertEqual(
                service.jobs[response["job_id"]]["status"],
                "running",
            )
            self.assertEqual(len(response["job_id"]), 32)

    def test_retry_revalidation_requires_same_publication_and_market_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(job_dir=Path(directory))
            authorization = {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
                "quality_dataset_snapshot_id": "snapshot-before",
                "quality_import_run_id": "import-before",
                "market_types": ["cex"],
                "market_ids": ["cex:binance:AAVE/USDT"],
                "reason_codes": ["rate_limit"],
                "issue_ids": ["issue-1"],
                "expected_observations": [
                    {
                        "market_id": "cex:binance:AAVE/USDT",
                        "date": "2026-07-29",
                    }
                ],
            }
            with patch.object(
                service,
                "retryable_windows",
                return_value=[dict(authorization)],
            ):
                service.revalidate_retry_authorization(
                    dict(authorization)
                )

                changed = dict(authorization)
                changed["quality_import_run_id"] = "import-stale"
                with self.assertRaises(AdminActionError) as context:
                    service.revalidate_retry_authorization(changed)

            self.assertEqual(
                context.exception.code,
                "retry_authorization_changed",
            )

    def test_retry_command_is_bounded_to_audited_source_type(self):
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(
                job_dir=Path(directory) / "jobs",
                data_dir=Path(directory),
            )
            base_job = {
                "job_type": "retry_failed",
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
            }

            cex_command = service._daily_pipeline_command(
                {**base_job, "market_types": ["cex"]},
                dex_only=False,
            )
            dex_command = service._daily_pipeline_command(
                {**base_job, "market_types": ["dex"]},
                dex_only=False,
            )
            mixed_command = service._daily_pipeline_command(
                {**base_job, "market_types": ["cex", "dex"]},
                dex_only=False,
            )

            self.assertIn("--cex-only", cex_command)
            self.assertNotIn("--dex-only", cex_command)
            self.assertIn("--dex-only", dex_command)
            self.assertNotIn("--cex-only", dex_command)
            self.assertNotIn("--cex-only", mixed_command)
            self.assertNotIn("--dex-only", mixed_command)

    def test_already_configured_token_is_a_non_persisted_noop(self):
        candidate = {
            "identity": {
                "token_symbol": "AAVE",
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
            },
            "registration": {
                "origin": "static_catalog",
                "status": "active",
                "cex_mapping_status": "configured",
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            service = AdminService(job_dir=Path(directory) / "jobs")
            with patch.object(
                service,
                "resolve_token",
                return_value=candidate,
            ), patch.object(service, "_start_job_thread") as start_worker:
                job = service.create_onboarding_job(
                    {
                        "chain": "eth",
                        "contract_address": "0x" + "12" * 20,
                        "expected_token_symbol": "AAVE",
                        "history_days": 30,
                    },
                    PUBLIC_ADD_TOKEN_ACTOR,
                )

            self.assertEqual(job["status"], "succeeded")
            self.assertTrue(job["result"]["already_configured"])
            self.assertEqual(service.jobs, {})
            self.assertFalse((Path(directory) / "jobs").exists())
            start_worker.assert_not_called()

    def test_public_onboarding_defers_full_market_tvl_and_depth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                job_dir=root / "jobs",
                data_dir=root,
                database_path=root / "market_facts.sqlite3",
            )
            job = {
                "job_id": "public-onboard",
                "job_type": "token_onboarding",
                "token_symbol": "TEST",
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "start_date": "2026-07-01",
                "end_date": "2026-07-29",
                "requested_by": PUBLIC_ADD_TOKEN_ACTOR,
                "status": "running",
                "stage": "collect_dex_daily",
                "created_at": "2026-07-30T12:00:00+00:00",
                "started_at": "2026-07-30T12:00:01+00:00",
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }
            service.jobs[job["job_id"]] = dict(job)

            with patch.object(service, "_run_command") as run_command, patch.object(
                service,
                "_published_dex_row_count",
                return_value=1,
            ), patch.object(service, "_update_runtime_status"):
                service._run_onboarding_job(
                    job["job_id"],
                    job,
                    root / "job.log",
                )

            self.assertEqual(run_command.call_count, 1)
            self.assertEqual(
                run_command.call_args.kwargs["timeout_seconds"],
                20 * 60,
            )
            completed = service.jobs[job["job_id"]]
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(
                completed["result"]["tvl"],
                "deferred_to_scheduled_collection",
            )
            self.assertEqual(
                completed["result"]["dex_depth"],
                "deferred_to_scheduled_collection",
            )
            public_status = public_job(completed)
            self.assertEqual(
                public_status["result_summary"]["tvl"],
                "deferred_to_scheduled_collection",
            )
            self.assertNotIn("quality_report", public_status)

    def test_protected_onboarding_keeps_full_tvl_and_depth_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = AdminService(
                job_dir=root / "jobs",
                data_dir=root,
                database_path=root / "market_facts.sqlite3",
            )
            job = {
                "job_id": "admin-onboard",
                "job_type": "token_onboarding",
                "token_symbol": "TEST",
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "start_date": "2026-07-01",
                "end_date": "2026-07-29",
                "requested_by": "admin",
                "status": "running",
                "stage": "collect_dex_daily",
                "created_at": "2026-07-30T12:00:00+00:00",
                "started_at": "2026-07-30T12:00:01+00:00",
                "finished_at": None,
                "error": None,
                "error_code": None,
                "retryable": False,
                "publication_committed": False,
                "result": None,
            }
            service.jobs[job["job_id"]] = dict(job)

            with patch.object(service, "_run_command") as run_command, patch.object(
                service,
                "_published_dex_row_count",
                return_value=1,
            ), patch.object(service, "_update_runtime_status"):
                service._run_onboarding_job(
                    job["job_id"],
                    job,
                    root / "job.log",
                )

            self.assertEqual(run_command.call_count, 3)
            self.assertEqual(
                service.jobs[job["job_id"]]["result"]["tvl"],
                "observed_or_explicit_source_status",
            )
            self.assertEqual(
                service.jobs[job["job_id"]]["result"]["dex_depth"],
                "observed_or_explicit_unsupported",
            )

    def test_request_contract_rejects_extra_missing_and_non_string_fields(self):
        contract = {"chain": 32, "contract_address": 128}
        for payload in (
            {"chain": "eth"},
            {
                "chain": "eth",
                "contract_address": "0xabc",
                "command": "anything",
            },
            {"chain": ["eth"], "contract_address": "0xabc"},
        ):
            with self.subTest(payload=payload), self.assertRaises(
                PublicActionError
            ) as context:
                require_exact_string_fields(payload, contract)
            self.assertEqual(
                context.exception.code,
                "invalid_public_action_request",
            )

    def test_public_projections_remove_internal_retry_and_job_fields(self):
        window = public_retry_window(
            {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
                "market_types": ["cex"],
                "reason_codes": ["rate_limit"],
                "market_ids": ["cex:binance:AAVE/USDT"],
                "quality_import_run_id": "private-import-id",
                "expected_observations": [{"market_id": "private"}],
            }
        )
        self.assertEqual(
            set(window),
            {
                "token_symbol",
                "start_date",
                "end_date",
                "queue_type",
                "market_types",
                "reason_codes",
            },
        )

        job = public_job(
            {
                "job_id": "job-1",
                "job_type": "retry_failed",
                "token_symbol": "AAVE",
                "status": "queued",
                "requested_by": PUBLIC_QUALITY_RETRY_ACTOR,
                "quality_import_run_id": "private-import-id",
                "expected_observations": [{"market_id": "private"}],
                "result": {"internal": "private"},
            }
        )
        self.assertNotIn("requested_by", job)
        self.assertNotIn("quality_import_run_id", job)
        self.assertNotIn("expected_observations", job)
        self.assertNotIn("result", job)

        with self.assertRaisesRegex(ValueError, "invalid contract"):
            public_retry_window(
                {
                    "token_symbol": "AAVE",
                    "start_date": "2026-07-29",
                    "end_date": "2026-07-29",
                    "queue_type": "latest_completed_day",
                    "reason_codes": "rate_limit",
                }
            )


class PublicActionHandlerTest(unittest.TestCase):
    @staticmethod
    def handler(path, payload=None):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = path
        handler.client_address = ("203.0.113.10", 40000)
        handler.headers = {"Content-Type": "application/json"}
        handler.send_json = Mock()
        handler.read_json = Mock(return_value=payload or {})
        return handler

    def test_disabled_public_action_returns_404_before_reading_body(self):
        handler = self.handler(
            PUBLIC_TOKEN_ADD_PATH,
            {
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "expected_token_symbol": "TEST",
            },
        )
        policy = PublicActionPolicy(
            add_token_enabled=False,
            quality_retry_enabled=False,
        )

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy):
            handler.do_POST()

        handler.read_json.assert_not_called()
        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.NOT_FOUND)
        self.assertEqual(response["error_code"], "public_action_disabled")

    def test_public_actions_activate_the_loopback_write_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            disabled_admin = AdminService(
                enabled=False,
                job_dir=Path(directory),
            )
            public_policy = PublicActionPolicy(add_token_enabled=True)
            disabled_policy = PublicActionPolicy()

            with patch.object(
                server,
                "ADMIN_SERVICE",
                disabled_admin,
            ), patch.object(server, "PUBLIC_ACTION_POLICY", public_policy):
                self.assertTrue(server.write_surface_enabled())

            with patch.object(
                server,
                "ADMIN_SERVICE",
                disabled_admin,
            ), patch.object(server, "PUBLIC_ACTION_POLICY", disabled_policy):
                self.assertFalse(server.write_surface_enabled())

    def test_proxy_client_address_requires_explicit_trust_and_loopback_peer(self):
        handler = self.handler(PUBLIC_TOKEN_RESOLVE_PATH)
        handler.headers = {"X-Real-IP": "198.51.100.24"}
        handler.client_address = ("127.0.0.1", 40000)
        with patch.object(
            server,
            "TRUST_LOOPBACK_PROXY_CLIENT_IP",
            False,
        ):
            self.assertEqual(
                handler.public_client_address(),
                "127.0.0.1",
            )

        with patch.object(
            server,
            "TRUST_LOOPBACK_PROXY_CLIENT_IP",
            True,
        ):
            self.assertEqual(
                handler.public_client_address(),
                "198.51.100.24",
            )
            handler.client_address = ("203.0.113.10", 40000)
            self.assertEqual(
                handler.public_client_address(),
                "203.0.113.10",
            )

        nginx = (
            Path(__file__).resolve().parents[1]
            / "deploy/nginx/cex-dex-dashboard.conf.in"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "proxy_set_header X-Real-IP $remote_addr;",
            nginx,
        )
        self.assertIn("proxy_read_timeout 75s;", nginx)

    def test_unknown_token_service_error_never_exposes_path_or_details(self):
        handler = self.handler(PUBLIC_TOKEN_RESOLVE_PATH)
        error = ValueError(
            "Token registry /srv/private/token_registry.json is unreadable"
        )
        error.code = "invalid_registry"
        error.details = {
            "path": "/srv/private/token_registry.json",
            "reason": "permission denied",
        }

        handler.send_public_token_error(error)

        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(
            response["error_code"],
            "token_onboarding_unavailable",
        )
        self.assertEqual(status, server.HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertNotIn("/srv/private", response["error"])
        self.assertNotIn("details", response)

    def test_public_mutation_rejects_simple_cross_origin_content_type(self):
        handler = self.handler(
            PUBLIC_TOKEN_ADD_PATH,
            {
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "expected_token_symbol": "TEST",
            },
        )
        handler.headers = {"Content-Type": "text/plain"}
        policy = PublicActionPolicy(add_token_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy):
            handler.do_POST()

        handler.read_json.assert_not_called()
        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
        self.assertEqual(
            response["error_code"],
            "public_action_json_required",
        )

    def test_public_token_resolve_accepts_only_chain_and_contract(self):
        candidate = {
            "identity": {
                "token_symbol": "TEST",
                "token_name": "Test Token",
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "decimals": 18,
                "coingecko_id": None,
                "source": "geckoterminal",
                "source_token_id": "eth_0x" + "12" * 20,
                "internal_path": "/srv/private/identity",
            },
            "discovery": {
                "usable_pool_count": 0,
                "top_pools": [],
                "internal_response": "private",
            },
            "capabilities": {
                "dex_daily": "available",
                "tvl": "available_after_collection",
                "dex_depth": "protocol_dependent",
                "cex": "requires_manual_mapping",
                "admin": "private",
            },
            "already_configured": False,
            "registration": {
                "origin": None,
                "status": None,
                "cex_mapping_status": "requires_manual_review",
                "registry_path": "/srv/private/token_registry.json",
            },
            "internal_job_dir": "/srv/private/jobs",
        }
        service = Mock()
        service.resolve_token.return_value = candidate
        handler = self.handler(
            PUBLIC_TOKEN_RESOLVE_PATH,
            {
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
            },
        )
        policy = PublicActionPolicy(add_token_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_POST()

        service.resolve_token.assert_called_once_with(
            "eth",
            "0x" + "12" * 20,
        )
        response = handler.send_json.call_args.args[0]
        self.assertEqual(response, public_token_candidate(candidate))
        self.assertNotIn("internal_job_dir", response)
        self.assertNotIn("internal_path", response["identity"])
        self.assertNotIn("registration", response)

    def test_public_add_token_injects_fixed_budget_and_sanitizes_job(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        service.create_onboarding_job.return_value = {
            "job_id": "job-add",
            "job_type": "token_onboarding",
            "token_symbol": "TEST",
            "chain": "eth",
            "contract_address": "0x" + "12" * 20,
            "start_date": "2026-07-01",
            "end_date": "2026-07-30",
            "status": "queued",
            "stage": "resolve_identity",
            "created_at": "2026-07-30T12:00:00+00:00",
            "requested_by": PUBLIC_ADD_TOKEN_ACTOR,
            "quality_import_run_id": "internal",
        }
        handler = self.handler(
            PUBLIC_TOKEN_ADD_PATH,
            {
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "expected_token_symbol": "TEST",
            },
        )
        policy = PublicActionPolicy(add_token_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_POST()

        request, actor = service.create_onboarding_job.call_args.args
        self.assertEqual(actor, PUBLIC_ADD_TOKEN_ACTOR)
        self.assertEqual(
            request,
            {
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "expected_token_symbol": "TEST",
                "history_days": PUBLIC_TOKEN_HISTORY_DAYS,
            },
        )
        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.ACCEPTED)
        self.assertNotIn("requested_by", response)
        self.assertNotIn("quality_import_run_id", response)

    def test_public_retry_injects_job_type_and_requires_exact_contract(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        service.create_job.return_value = {
            "job_id": "job-retry",
            "job_type": "retry_failed",
            "token_symbol": "AAVE",
            "start_date": "2026-07-29",
            "end_date": "2026-07-29",
            "status": "queued",
            "stage": "queued",
            "created_at": "2026-07-30T12:00:00+00:00",
            "requested_by": PUBLIC_QUALITY_RETRY_ACTOR,
        }
        payload = {
            "token_symbol": "aave",
            "start_date": "2026-07-29",
            "end_date": "2026-07-29",
            "queue_type": "latest_completed_day",
        }
        handler = self.handler(PUBLIC_QUALITY_RETRY_PATH, payload)
        policy = PublicActionPolicy(quality_retry_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_POST()

        request, actor = service.create_job.call_args.args
        self.assertEqual(actor, PUBLIC_QUALITY_RETRY_ACTOR)
        self.assertEqual(
            request,
            {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
                "job_type": "retry_failed",
            },
        )

        invalid = self.handler(
            PUBLIC_QUALITY_RETRY_PATH,
            {**payload, "job_type": "refresh"},
        )
        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            invalid.do_POST()
        invalid_response = invalid.send_json.call_args.args[0]
        self.assertEqual(
            invalid_response["error_code"],
            "invalid_public_action_request",
        )

    def test_fact_refresh_accepts_only_backend_verified_canonical_identity(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        service.create_job.return_value = {
            "job_id": "job-fact-refresh",
            "job_type": "snapshot_refresh",
            "token_symbol": "AAVE",
            "market_id": "cex:binance:AAVE/USDT",
            "fact_type": "depth",
            "status": "queued",
            "stage": "queued",
            "created_at": "2026-07-31T12:00:00+00:00",
            "requested_by": PUBLIC_FACT_REFRESH_ACTOR,
        }
        quality = {
            "markets": [
                {
                    "market_id": "cex:binance:AAVE/USDT",
                    "facts": {"depth": {"retryable": True}},
                }
            ]
        }
        handler = self.handler(
            PUBLIC_FACT_REFRESH_PATH,
            {
                "token_symbol": "aave",
                "market_id": "cex:binance:AAVE/USDT",
                "fact_type": "depth",
            },
        )
        policy = PublicActionPolicy(fact_refresh_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ), patch.object(server, "build_market_quality", return_value=quality):
            handler.do_POST()

        request, actor = service.create_job.call_args.args
        self.assertEqual(actor, PUBLIC_FACT_REFRESH_ACTOR)
        self.assertEqual(
            request,
            {
                "token_symbol": "AAVE",
                "market_id": "cex:binance:AAVE/USDT",
                "fact_type": "depth",
                "job_type": "snapshot_refresh",
            },
        )
        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.ACCEPTED)
        self.assertEqual(response["job_id"], "job-fact-refresh")

        legacy = self.handler(
            PUBLIC_FACT_REFRESH_PATH,
            {
                "token_symbol": "AAVE",
                "market_id": "binance|AAVE/USDT",
                "fact_type": "depth",
            },
        )
        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ), patch.object(server, "build_market_quality", return_value=quality):
            legacy.do_POST()
        legacy_response, legacy_status = legacy.send_json.call_args.args[:2]
        self.assertEqual(legacy_status, server.HTTPStatus.UNPROCESSABLE_ENTITY)
        self.assertEqual(legacy_response["error_code"], "fact_refresh_not_found")

    def test_public_retry_maps_unapproved_window_to_stable_error(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        service.create_job.side_effect = AdminActionError(
            "retry_window_not_approved",
            "internal exact report message",
        )
        handler = self.handler(
            PUBLIC_QUALITY_RETRY_PATH,
            {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
            },
        )
        policy = PublicActionPolicy(quality_retry_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_POST()

        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(
            response["error_code"],
            "retry_window_not_approved",
        )
        self.assertEqual(status, server.HTTPStatus.UNPROCESSABLE_ENTITY)
        self.assertNotIn("internal exact report message", response["error"])

    def test_worker_start_failure_is_not_misreported_as_contention(self):
        service = Mock()
        service.count_jobs_created_on.return_value = 0
        service.create_job.side_effect = AdminWorkerStartError(
            "thread failed"
        )
        handler = self.handler(
            PUBLIC_QUALITY_RETRY_PATH,
            {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
            },
        )
        policy = PublicActionPolicy(quality_retry_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_POST()

        response, status = handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertEqual(
            response["error_code"],
            "public_worker_start_failed",
        )

    def test_public_retryable_get_is_required_sanitized_and_queryless(self):
        service = Mock()
        service.retryable_windows.return_value = [
            {
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "queue_type": "latest_completed_day",
                "market_types": ["cex"],
                "reason_codes": ["rate_limit"],
                "market_ids": ["cex:binance:AAVE/USDT"],
                "quality_import_run_id": "internal-import",
            }
        ]
        handler = self.handler(PUBLIC_QUALITY_RETRYABLE_PATH)
        policy = PublicActionPolicy(quality_retry_enabled=True)

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_GET()

        service.retryable_windows.assert_called_once_with(required=True)
        response = handler.send_json.call_args.args[0]
        self.assertEqual(response["count"], 1)
        self.assertNotIn("market_ids", response["windows"][0])
        self.assertNotIn("quality_import_run_id", response["windows"][0])

        query_handler = self.handler(
            PUBLIC_QUALITY_RETRYABLE_PATH + "?start=2020-01-01"
        )
        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            query_handler.do_GET()
        query_response = query_handler.send_json.call_args.args[0]
        self.assertEqual(
            query_response["error_code"],
            "invalid_public_action_request",
        )

    def test_exact_public_job_status_is_sanitized_and_rejects_admin_jobs(self):
        public_job_id = "a" * 32
        admin_job_id = "b" * 32
        service = Mock()
        jobs = {
            public_job_id: {
                "job_id": public_job_id,
                "job_type": "retry_failed",
                "token_symbol": "AAVE",
                "start_date": "2026-07-29",
                "end_date": "2026-07-29",
                "status": "failed",
                "stage": "authorize_retry",
                "created_at": "2026-07-30T12:00:00+00:00",
                "started_at": None,
                "finished_at": "2026-07-30T12:00:01+00:00",
                "error": "/srv/private/worker.log failed",
                "error_code": "retry_authorization_expired",
                "retryable": True,
                "requested_by": PUBLIC_QUALITY_RETRY_ACTOR,
                "command": ["/srv/private/script.py"],
                "environment": {"SECRET": "private"},
                "result": {"quality_import_run_id": "internal"},
            },
            admin_job_id: {
                "job_id": admin_job_id,
                "job_type": "refresh",
                "status": "queued",
                "requested_by": "admin",
            },
        }
        service.get_job.side_effect = jobs.get
        policy = PublicActionPolicy(quality_retry_enabled=True)
        handler = self.handler(
            f"/api/actions/jobs/{public_job_id}"
        )

        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            handler.do_GET()

        response = handler.send_json.call_args.args[0]
        self.assertEqual(response["job_id"], public_job_id)
        self.assertEqual(
            response["error_code"],
            "retry_authorization_expired",
        )
        for private_field in (
            "requested_by",
            "error",
            "command",
            "environment",
            "result",
        ):
            self.assertNotIn(private_field, response)

        admin_handler = self.handler(
            f"/api/actions/jobs/{admin_job_id}"
        )
        with patch.object(server, "PUBLIC_ACTION_POLICY", policy), patch.object(
            server,
            "ADMIN_SERVICE",
            service,
        ):
            admin_handler.do_GET()
        admin_response, status = admin_handler.send_json.call_args.args[:2]
        self.assertEqual(status, server.HTTPStatus.NOT_FOUND)
        self.assertEqual(
            admin_response["error_code"],
            "public_job_not_found",
        )

    def test_public_surface_exposes_only_the_bounded_fact_refresh_route(self):
        self.assertNotIn("/api/actions/jobs", server.PUBLIC_ACTION_PATHS)
        self.assertNotIn("/api/actions/refresh", server.PUBLIC_ACTION_PATHS)
        self.assertIn(PUBLIC_FACT_REFRESH_PATH, server.PUBLIC_ACTION_PATHS)
        self.assertNotIn(
            "/api/actions/quality/manual-review",
            server.PUBLIC_ACTION_PATHS,
        )


if __name__ == "__main__":
    unittest.main()
