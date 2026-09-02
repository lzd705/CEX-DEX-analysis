import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import scripts.run_historical_opportunity_demo as demo


def fake_dashboard(http_server):
    class FakeMarketMonitorHandler:
        pass

    admin_service = SimpleNamespace(enabled=True)
    public_actions = SimpleNamespace(
        add_token_enabled=True,
        quality_retry_enabled=True,
        fact_refresh_enabled=True,
    )
    dashboard = SimpleNamespace(
        ADMIN_SERVICE=admin_service,
        PUBLIC_ACTION_POLICY=public_actions,
        MarketMonitorHandler=FakeMarketMonitorHandler,
        ThreadingHTTPServer=Mock(return_value=http_server),
        clear_runtime_caches=Mock(),
    )
    dashboard.write_surface_enabled = lambda: bool(
        admin_service.enabled
        or public_actions.add_token_enabled
        or public_actions.quality_retry_enabled
        or public_actions.fact_refresh_enabled
    )
    return dashboard


class HistoricalOpportunityDemoTests(unittest.TestCase):
    def test_cli_defaults_to_loopback_only_port_and_validates_range(self):
        self.assertEqual(demo.parse_args([]).port, 8765)
        self.assertEqual(demo.parse_args(["--port", "0"]).port, 0)
        self.assertEqual(demo.parse_args(["--port", "65535"]).port, 65535)

        for argv in (
            ["--port", "-1"],
            ["--port", "65536"],
            ["--port", "not-a-port"],
            ["--host", "0.0.0.0"],
        ):
            with self.subTest(argv=argv), redirect_stderr(
                io.StringIO()
            ), self.assertRaises(SystemExit):
                demo.parse_args(argv)

    def test_demo_serves_fixture_on_loopback_and_cleans_up_on_control_c(self):
        fixture = Mock()
        fixture.data_dir = Path("/fixture")
        fixture.historical_root = Path("/fixture/routes/historical")
        fixture.pointer = {"replay_id": "replay:" + "a" * 64}
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 43210)
        http_server.serve_forever.side_effect = KeyboardInterrupt
        dashboard = fake_dashboard(http_server)
        output = io.StringIO()

        with patch.object(
            demo, "HistoricalOpportunityDemoFixture", return_value=fixture
        ), patch.object(
            demo, "_load_dashboard_server", return_value=dashboard
        ), patch.dict(
            os.environ,
            {"MARKET_ROUTE_DATA_DIR": "/original/routes"},
            clear=False,
        ):
            demo.serve_demo(port=0, output=output)

            self.assertEqual(
                os.environ["MARKET_ROUTE_DATA_DIR"], "/original/routes"
            )

        server_call = dashboard.ThreadingHTTPServer.call_args
        self.assertEqual(server_call.args[0], ("127.0.0.1", 0))
        self.assertTrue(
            issubclass(server_call.args[1], dashboard.MarketMonitorHandler)
        )
        http_server.serve_forever.assert_called_once_with()
        http_server.server_close.assert_called_once_with()
        self.assertIs(http_server.daemon_threads, False)
        fixture.close.assert_called_once_with()
        self.assertEqual(dashboard.clear_runtime_caches.call_count, 2)

        lines = output.getvalue().splitlines()
        self.assertIn("Preparing LOCAL DEMO FIXTURE", lines[0])
        self.assertIn("no external RPC", lines[0])
        ready = json.loads(lines[1])
        self.assertEqual(
            ready,
            {
                "contract_version": "opportunity_historical_demo_summary/v1",
                "demo_fixture": True,
                "evidence_mode": "offline_test_fixture",
                "network_scope": "loopback_only",
                "replay_id": "replay:" + "a" * 64,
                "verification_status": "structurally_validated",
                "url": (
                    "http://127.0.0.1:43210/opportunities"
                    "?opportunity_scope=historical#local-demo-fixture"
                ),
            },
        )
        self.assertIn("verification was not run", lines[2].lower())
        self.assertIn("not live", lines[2].lower())
        self.assertIn("Ctrl-C", lines[3])

    def test_demo_propagates_server_failure_after_cleanup(self):
        fixture = Mock()
        fixture.data_dir = Path("/fixture")
        fixture.historical_root = Path("/fixture/routes/historical")
        fixture.pointer = {"replay_id": "replay:" + "b" * 64}
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 8765)
        http_server.serve_forever.side_effect = RuntimeError("serve failed")
        dashboard = fake_dashboard(http_server)

        with patch.dict(os.environ, {}, clear=True):
            with patch.object(
                demo, "HistoricalOpportunityDemoFixture", return_value=fixture
            ), patch.object(
                demo, "_load_dashboard_server", return_value=dashboard
            ), self.assertRaisesRegex(RuntimeError, "serve failed"):
                demo.serve_demo(port=8765, output=io.StringIO())
            self.assertNotIn("MARKET_ROUTE_DATA_DIR", os.environ)

        http_server.server_close.assert_called_once_with()
        fixture.close.assert_called_once_with()

    def test_demo_forces_inherited_write_surfaces_off_then_restores_them(self):
        fixture = Mock()
        fixture.data_dir = Path("/fixture")
        fixture.historical_root = Path("/fixture/routes/historical")
        fixture.pointer = {"replay_id": "replay:" + "c" * 64}
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 43211)
        http_server.serve_forever.side_effect = KeyboardInterrupt
        dashboard = fake_dashboard(http_server)
        inherited_flags = {
            "ADMIN_ENABLED": "true",
            "PUBLIC_ADD_TOKEN_ENABLED": "true",
            "PUBLIC_QUALITY_RETRY_ENABLED": "true",
            "PUBLIC_FACT_REFRESH_ENABLED": "true",
        }

        def read_only_server_factory(*_args):
            self.assertFalse(dashboard.write_surface_enabled())
            for name in inherited_flags:
                self.assertEqual(os.environ[name], "false")
            return http_server

        def load_read_only_dashboard():
            for name in inherited_flags:
                self.assertEqual(os.environ[name], "false")
            return dashboard

        dashboard.ThreadingHTTPServer.side_effect = read_only_server_factory
        with patch.object(
            demo, "HistoricalOpportunityDemoFixture", return_value=fixture
        ), patch.object(
            demo, "_load_dashboard_server", side_effect=load_read_only_dashboard
        ), patch.dict(os.environ, inherited_flags, clear=False):
            demo.serve_demo(port=0, output=io.StringIO())
            for name, value in inherited_flags.items():
                self.assertEqual(os.environ[name], value)

        self.assertTrue(dashboard.ADMIN_SERVICE.enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.add_token_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.quality_retry_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.fact_refresh_enabled)

    def test_dashboard_import_isolated_from_ambient_running_job(self):
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ambient = root / "ambient"
            ambient_jobs = ambient / "admin" / "jobs"
            ambient_jobs.mkdir(parents=True)
            sentinel = ambient_jobs / "running.json"
            original = b'{"job_id":"sentinel","status":"running"}\n'
            sentinel.write_bytes(original)
            isolated = root / "isolated"
            isolated.mkdir()
            script = r"""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.run_historical_opportunity_demo as demo

fixture = SimpleNamespace(
    data_dir=Path(sys.argv[1]),
    historical_root=Path(sys.argv[1]) / "routes" / "historical",
)
with demo._isolated_demo_environment(fixture):
    server = demo._load_dashboard_server()
    print(json.dumps({
        "admin_enabled": server.ADMIN_SERVICE.enabled,
        "job_dir": str(server.ADMIN_SERVICE.job_dir),
        "registry_path": str(server.ADMIN_SERVICE.registry.path),
        "skip_local_env": __import__("os").environ.get(
            "DASHBOARD_SKIP_LOCAL_ENV"
        ),
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
                "MARKET_DATA_DIR": str(ambient),
                "ADMIN_JOB_DIR": str(ambient_jobs),
                "TOKEN_REGISTRY_PATH": str(
                    ambient / "admin" / "token_registry.json"
                ),
            })

            completed = subprocess.run(
                [sys.executable, "-c", script, str(isolated)],
                cwd=project_root,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            state = json.loads(completed.stdout)
            self.assertEqual(sentinel.read_bytes(), original)
            isolated_runtime = isolated.resolve() / "demo-runtime"
            self.assertEqual(state, {
                "admin_enabled": False,
                "job_dir": str(isolated_runtime / "admin" / "jobs"),
                "registry_path": str(
                    isolated_runtime / "admin" / "token_registry.json"
                ),
                "skip_local_env": "true",
                "write_surface_enabled": False,
            })

    def test_demo_handler_exposes_only_fixture_api_and_read_only_static_ui(self):
        class FakeMarketMonitorHandler:
            pass

        class FakeClientError(ValueError):
            pass

        dashboard = SimpleNamespace(
            MarketMonitorHandler=FakeMarketMonitorHandler,
            PublicClientRequestError=FakeClientError,
            public_api_query_items=Mock(return_value=(("notional", "1000"),)),
            _historical_opportunity_arguments=Mock(
                return_value={"notional_usd": "1000"}
            ),
            is_admin_surface_path=lambda path: (
                path == "/admin.html" or path.startswith("/api/admin")
            ),
        )
        fixture = Mock()
        fixture.build_payload.return_value = {"demo_fixture": True}
        handler_type = demo._demo_handler(dashboard, fixture)

        handler = object.__new__(handler_type)
        handler.path = (
            "/api/markets/opportunities/historical?notional=1000"
        )
        handler.send_json = Mock()
        handler.do_GET()
        fixture.build_payload.assert_called_once_with(notional_usd="1000")
        handler.send_json.assert_called_once_with({"demo_fixture": True})

        for path in (
            "/api/admin/session",
            "/api/markets/summary",
            "/health",
            "/admin.html",
            "/actions.html",
            "/actions.js",
            "/actions.css",
            "/actions%2ehtml",
            "/foo/../actions.html",
            "/nested/%2e%2e/ACTIONS.HTML",
        ):
            with self.subTest(path=path):
                denied = object.__new__(handler_type)
                denied.path = path
                denied.send_json = Mock()
                denied.do_GET()
                denied.send_json.assert_called_once_with(
                    {"error": "Not found"}, HTTPStatus.NOT_FOUND
                )

        mutation = object.__new__(handler_type)
        mutation.path = "/api/actions/tokens/resolve"
        mutation.send_json = Mock()
        mutation.do_POST()
        mutation.send_json.assert_called_once_with(
            {"error": "Not found"}, HTTPStatus.NOT_FOUND
        )

    def test_historical_projection_accepts_only_declared_evidence_pairs(self):
        from dashboard import opportunity_facts as facts

        class ExplodingLoaded(dict):
            def __getitem__(self, _key):
                raise AssertionError("loaded data must not be read")

        with self.assertRaises(facts.OpportunityBundleInvalid):
            facts._historical_projected_rows(
                ExplodingLoaded(),
                expected_verification_status="invented",
                expected_evidence_mode="invented",
            )


class HistoricalOpportunityDemoFixtureTests(unittest.TestCase):
    def test_fixture_payload_is_structural_and_never_claims_foundry_verification(self):
        with demo.HistoricalOpportunityDemoFixture() as fixture:
            payload = fixture.build_payload()
            filtered = fixture.build_payload(notional_usd="1000")

        metadata = payload["metadata"]
        coverage = metadata["coverage"]
        self.assertEqual(
            metadata["contract_version"],
            "opportunity_historical_demo_summary/v1",
        )
        self.assertTrue(metadata["demo_fixture"])
        self.assertEqual(metadata["evidence_mode"], "offline_test_fixture")
        self.assertEqual(
            metadata["verification_status"], "structurally_validated"
        )
        self.assertEqual(coverage["scenario_count"], 10)
        self.assertEqual(coverage["returned_count"], 10)
        self.assertEqual(coverage["foundry_verified_count"], 0)
        self.assertEqual(coverage["strict_count"], 0)
        self.assertEqual(coverage["executable_count"], 0)
        self.assertEqual(coverage["attested_count"], 0)
        self.assertEqual(len(payload["routes"]), 10)
        self.assertTrue(all(
            row["foundry_verified"] is False
            and row["opportunity_class"] == "research_estimate"
            for row in payload["routes"]
        ))
        self.assertNotIn("production_connected", json.dumps(payload))
        self.assertEqual(len(filtered["routes"]), 2)
        self.assertEqual(
            filtered["metadata"]["coverage"]["scenario_count"], 10
        )
        self.assertEqual(
            filtered["metadata"]["coverage"]["returned_count"], 2
        )


if __name__ == "__main__":
    unittest.main()
