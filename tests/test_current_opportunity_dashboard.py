"""Tests for the isolated, read-only Current Opportunity server."""

from __future__ import annotations

import json
import io
import os
from http import HTTPStatus
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from contextlib import redirect_stderr
from unittest.mock import Mock, patch

from scripts.token_registry import TokenRegistry


class CurrentOpportunityDashboardTests(unittest.TestCase):
    def test_health_reports_every_readable_opportunity_state_without_base_data(self):
        from scripts import run_current_opportunity_dashboard as runner

        handler_factory = getattr(
            runner, "_current_opportunity_handler", None
        )
        self.assertIsNotNone(handler_factory)

        class BaseHandler:
            def do_GET(self):  # noqa: N802
                raise AssertionError(
                    "route-only health delegated to base market health"
                )

        for route_status, reason in (
            ("current", None),
            ("stale", "cohort_stale"),
            ("unavailable", "snapshot_skew_exceeded"),
        ):
            with self.subTest(route_status=route_status):
                route_health = {
                    "status": route_status,
                    "reason": reason,
                    "route_cohort_id": "cohort:" + "c" * 64,
                    "manifest_sha256": "d" * 64,
                    "observed_at": "2026-09-04T12:00:00+00:00",
                    "age_seconds": 30,
                    "max_age_seconds": 120,
                    "max_skew_seconds": 60,
                    "scenario_count": 10,
                }
                dashboard = SimpleNamespace(
                    MarketMonitorHandler=BaseHandler,
                    opportunity_publication_health=lambda: route_health,
                    application_release_sha=lambda: "a" * 40,
                    static_asset_sha=lambda: "b" * 64,
                    static_asset_version=lambda: (
                        "a" * 12 + "-" + "b" * 12
                    ),
                )
                handler_type = handler_factory(dashboard)
                handler = object.__new__(handler_type)
                handler.path = "/health?probe=current-opportunity"
                responses = []
                handler.send_json = lambda payload, status=HTTPStatus.OK: (
                    responses.append((status, payload))
                )

                handler.do_GET()

                self.assertEqual(responses, [(HTTPStatus.OK, {
                    "status": "ok",
                    "data_ready": True,
                    "storage": "route_bundle",
                    "data_status": route_status,
                    "route_opportunities": route_health,
                    "application_sha": "a" * 40,
                    "asset_sha": "b" * 64,
                    "asset_version": "a" * 12 + "-" + "b" * 12,
                })])

    def test_health_fails_closed_for_missing_and_invalid_opportunity_data(self):
        from scripts import run_current_opportunity_dashboard as runner

        handler_factory = getattr(
            runner, "_current_opportunity_handler", None
        )
        self.assertIsNotNone(handler_factory)

        class BaseHandler:
            def do_GET(self):  # noqa: N802
                raise AssertionError(
                    "route-only health delegated to base market health"
                )

        for route_status, reason in (
            ("missing", "complete_pointer_absent"),
            ("invalid", "opportunity_bundle_validation_failed"),
        ):
            with self.subTest(route_status=route_status):
                route_health = {
                    "status": route_status,
                    "reason": reason,
                    "route_cohort_id": None,
                    "manifest_sha256": None,
                    "observed_at": None,
                    "age_seconds": None,
                    "max_age_seconds": 120,
                    "max_skew_seconds": 60,
                }
                dashboard = SimpleNamespace(
                    MarketMonitorHandler=BaseHandler,
                    opportunity_publication_health=lambda: route_health,
                    application_release_sha=lambda: "a" * 40,
                    static_asset_sha=lambda: "b" * 64,
                    static_asset_version=lambda: (
                        "a" * 12 + "-" + "b" * 12
                    ),
                )
                handler_type = handler_factory(dashboard)
                handler = object.__new__(handler_type)
                handler.path = "/health"
                responses = []
                handler.send_json = lambda payload, status=HTTPStatus.OK: (
                    responses.append((status, payload))
                )

                handler.do_GET()

                self.assertEqual(responses, [(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "status": "degraded",
                        "data_ready": False,
                        "storage": "route_bundle",
                        "data_status": route_status,
                        "route_opportunities": route_health,
                        "application_sha": "a" * 40,
                        "asset_sha": "b" * 64,
                        "asset_version": "a" * 12 + "-" + "b" * 12,
                    },
                )])
                self.assertNotIn(
                    "/private/",
                    json.dumps(responses[0][1], sort_keys=True),
                )

    def test_opportunity_handler_delegates_non_health_gets(self):
        from scripts import run_current_opportunity_dashboard as runner

        handler_factory = getattr(
            runner, "_current_opportunity_handler", None
        )
        self.assertIsNotNone(handler_factory)

        class BaseHandler:
            def do_GET(self):  # noqa: N802
                self.delegated_path = self.path

        dashboard = SimpleNamespace(MarketMonitorHandler=BaseHandler)
        handler_type = handler_factory(dashboard)
        handler = object.__new__(handler_type)
        handler.path = "/api/markets/opportunities?notional=1000"

        handler.do_GET()

        self.assertEqual(
            handler.delegated_path,
            "/api/markets/opportunities?notional=1000",
        )

    def test_opportunity_handler_preserves_distinct_uni_and_cake_filters(self):
        from scripts import run_current_opportunity_dashboard as runner

        handler_factory = runner._current_opportunity_handler

        class BaseHandler:
            def do_GET(self):  # noqa: N802
                self.delegated_path = self.path

        handler_type = handler_factory(SimpleNamespace(
            MarketMonitorHandler=BaseHandler
        ))
        for token in ("UNI", "CAKE"):
            with self.subTest(token=token):
                handler = object.__new__(handler_type)
                handler.path = (
                    "/api/markets/opportunities?token={}&sort=net_edge_usd"
                    .format(token)
                )

                handler.do_GET()

                self.assertEqual(
                    handler.delegated_path,
                    "/api/markets/opportunities?token={}"
                    "&sort=net_edge_usd".format(token),
                )

    def test_fresh_process_isolates_inherited_admin_state_before_import(self):
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_dir = root / "market-data"
            (data_dir / "routes").mkdir(parents=True)
            ambient_jobs = root / "ambient-admin" / "jobs"
            ambient_jobs.mkdir(parents=True)
            ambient_job = ambient_jobs / "running.json"
            original = (
                b'{"job_id":"ambient","status":"running"}\n'
            )
            ambient_job.write_bytes(original)
            ambient_registry = root / "ambient-admin" / "registry.json"
            TokenRegistry(ambient_registry).upsert({
                "token_symbol": "AMBIENT",
                "token_name": "Ambient Token",
                "chain": "eth",
                "contract_address": "0x" + "12" * 20,
                "decimals": 18,
                "coingecko_id": None,
                "source": "geckoterminal",
                "source_token_id": "eth_0x" + "12" * 20,
                "status": "pending",
                "cex_mapping": {
                    "status": "requires_manual_review",
                    "cex_symbol": None,
                    "exchanges": [],
                },
                "created_at": "2026-09-04T00:00:00+00:00",
                "created_by": "test",
                "activated_at": None,
                "last_job_id": "missing-job",
            })
            original_registry = ambient_registry.read_bytes()
            runtime_root = root / "isolated-runtime"
            script = r"""
import json
import sys
from pathlib import Path

from scripts.run_current_opportunity_dashboard import (
    _isolated_dashboard_environment,
    _load_dashboard_server,
)

data_dir = Path(sys.argv[1])
runtime_root = Path(sys.argv[2])
with _isolated_dashboard_environment(data_dir, runtime_root):
    server = _load_dashboard_server()
    print(json.dumps({
        "admin_enabled": server.ADMIN_SERVICE.enabled,
        "admin_job_dir": str(server.ADMIN_SERVICE.job_dir),
        "registry_path": str(server.ADMIN_SERVICE.registry.path),
        "market_database": __import__("os").environ.get("MARKET_DATABASE"),
        "market_cex_data": __import__("os").environ.get("MARKET_CEX_DATA"),
        "market_event_data_dir": __import__("os").environ["MARKET_EVENT_DATA_DIR"],
        "market_lifecycle": __import__("os").environ["MARKET_CEX_INSTRUMENT_LIFECYCLE"],
        "route_data_dir": __import__("os").environ["MARKET_ROUTE_DATA_DIR"],
        "write_surface_enabled": server.write_surface_enabled(),
    }, sort_keys=True))
"""
            environment = os.environ.copy()
            environment.update({
                "ADMIN_ENABLED": "true",
                "ADMIN_LOGIN_REQUIRED": "false",
                "ADMIN_ALLOW_OPEN_LOCAL": "true",
                "PUBLIC_ADD_TOKEN_ENABLED": "true",
                "PUBLIC_QUALITY_RETRY_ENABLED": "true",
                "PUBLIC_FACT_REFRESH_ENABLED": "true",
                "ADMIN_JOB_DIR": str(ambient_jobs),
                "TOKEN_REGISTRY_PATH": str(
                    ambient_registry
                ),
                "MARKET_DATABASE": str(root / "ambient.sqlite3"),
                "MARKET_CEX_DATA": str(root / "ambient-cex.csv"),
                "MARKET_DEX_DATA": str(root / "ambient-dex.csv"),
                "MARKET_EVENT_DATA_DIR": str(root / "ambient-events"),
                "MARKET_CEX_INSTRUMENT_LIFECYCLE": str(
                    root / "ambient-lifecycle.json"
                ),
            })

            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    script,
                    str(data_dir),
                    str(runtime_root),
                ],
                cwd=project_root,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(ambient_job.read_bytes(), original)
            self.assertEqual(ambient_registry.read_bytes(), original_registry)
            self.assertEqual(json.loads(completed.stdout), {
                "admin_enabled": False,
                "admin_job_dir": str(
                    (runtime_root / "admin/jobs").resolve()
                ),
                "market_cex_data": None,
                "market_database": None,
                "market_event_data_dir": str(
                    (data_dir / "events").resolve()
                ),
                "market_lifecycle": str(
                    (data_dir / "cex_instrument_lifecycle.json").resolve()
                ),
                "registry_path": str(
                    (runtime_root / "admin/token_registry.json").resolve()
                ),
                "route_data_dir": str((data_dir / "routes").resolve()),
                "write_surface_enabled": False,
            })

    def test_server_binds_only_loopback_and_cleans_runtime_state(self):
        from scripts import run_current_opportunity_dashboard as runner

        server_instances = []

        class FakeHttpServer:
            def __init__(self, address, handler):
                self.address = address
                self.handler = handler
                self.server_address = address
                self.daemon_threads = None
                self.served = False
                self.closed = False
                server_instances.append(self)

            def serve_forever(self):
                self.served = True

            def server_close(self):
                self.closed = True

        clear_runtime_caches = Mock()

        class FakeMarketMonitorHandler:
            pass

        dashboard_server = SimpleNamespace(
            ADMIN_SERVICE=SimpleNamespace(enabled=False),
            PUBLIC_ACTION_POLICY=SimpleNamespace(
                add_token_enabled=False,
                quality_retry_enabled=False,
                fact_refresh_enabled=False,
            ),
            MarketMonitorHandler=FakeMarketMonitorHandler,
            ThreadingHTTPServer=FakeHttpServer,
            clear_runtime_caches=clear_runtime_caches,
            write_surface_enabled=lambda: False,
        )

        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary) / "market-data"
            (data_dir / "routes").mkdir(parents=True)
            output = io.StringIO()
            original_job_dir = os.environ.get("ADMIN_JOB_DIR")
            with patch.object(
                runner,
                "_load_dashboard_server",
                return_value=dashboard_server,
            ):
                runner.serve_current_dashboard(
                    data_dir=data_dir,
                    port=43210,
                    output=output,
                )

        self.assertEqual(len(server_instances), 1)
        instance = server_instances[0]
        self.assertEqual(instance.address, ("127.0.0.1", 43210))
        self.assertTrue(issubclass(
            instance.handler,
            dashboard_server.MarketMonitorHandler,
        ))
        self.assertIsNot(
            instance.handler,
            dashboard_server.MarketMonitorHandler,
        )
        self.assertFalse(instance.daemon_threads)
        self.assertTrue(instance.served)
        self.assertTrue(instance.closed)
        self.assertEqual(clear_runtime_caches.call_count, 2)
        self.assertEqual(os.environ.get("ADMIN_JOB_DIR"), original_job_dir)
        announcement = json.loads(output.getvalue().splitlines()[0])
        self.assertEqual(announcement, {
            "current_opportunity": True,
            "network_scope": "loopback_only",
            "url": "http://127.0.0.1:43210/opportunities",
            "write_surfaces": "disabled",
        })

    def test_invalid_port_is_rejected_before_dashboard_import(self):
        from scripts import run_current_opportunity_dashboard as runner

        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary) / "market-data"
            data_dir.mkdir()
            with patch.object(runner, "_load_dashboard_server") as loader:
                with self.assertRaisesRegex(ValueError, "between 1 and 65535"):
                    runner.serve_current_dashboard(
                        data_dir=data_dir,
                        port=0,
                        output=io.StringIO(),
                    )

        loader.assert_not_called()

    def test_cli_has_no_host_override(self):
        from scripts import run_current_opportunity_dashboard as runner

        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                runner.parse_args([
                    "--data-dir", "/tmp/current-opportunity-test",
                    "--host", "0.0.0.0",
                ])

        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
