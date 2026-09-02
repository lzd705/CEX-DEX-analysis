import copy
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from decimal import Decimal
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
                "execution_claim": "synthetic_fixture_no_execution",
                "execution_status": "not_run",
                "network_scope": "loopback_only",
                "replay_id": "replay:" + "a" * 64,
                "simulation_basis": "deterministic_repository_fixture",
                "temporal_scope": "historical_demo_fixture",
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

    def test_main_handles_control_c_during_fixture_preparation(self):
        errors = io.StringIO()

        with patch.object(
            demo, "serve_demo", side_effect=KeyboardInterrupt
        ), redirect_stderr(errors):
            exit_code = demo.main(["--port", "0"])

        self.assertEqual(exit_code, 130)
        self.assertIn("interrupted before the demo was ready", errors.getvalue().lower())
        self.assertNotIn("traceback", errors.getvalue().lower())

    def test_main_reports_startup_failure_without_a_traceback(self):
        errors = io.StringIO()

        with patch.object(
            demo, "serve_demo", side_effect=RuntimeError("fixture unavailable")
        ), redirect_stderr(errors):
            exit_code = demo.main(["--port", "0"])

        self.assertEqual(exit_code, 1)
        self.assertIn("local demo could not start", errors.getvalue().lower())
        self.assertIn("fixture unavailable", errors.getvalue().lower())
        self.assertNotIn("traceback", errors.getvalue().lower())

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

    def test_production_historical_projection_rejects_evidence_mode_overrides(self):
        from dashboard import opportunity_facts as facts

        with self.assertRaises(TypeError):
            facts._historical_projected_rows(
                {},
                expected_verification_status="structurally_validated",
                expected_evidence_mode="offline_test_fixture",
            )


class HistoricalOpportunityDemoFixtureTests(unittest.TestCase):
    def test_fixture_builds_without_tests_submodules_or_ignored_toolchain(self):
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "clean-source"
            checkout.mkdir()
            ignored = shutil.ignore_patterns("__pycache__", "*.pyc")
            for directory in ("dashboard", "scripts"):
                shutil.copytree(
                    project_root / directory,
                    checkout / directory,
                    ignore=ignored,
                )
            self.assertFalse((checkout / "tests").exists())
            self.assertFalse((checkout / "lib").exists())
            self.assertFalse((checkout / ".historical-foundry").exists())

            script = r"""
import json
from scripts.historical_opportunity_demo_fixture import (
    HistoricalOpportunityDemoFixture,
)

with HistoricalOpportunityDemoFixture() as fixture:
    payload = fixture.build_payload(notional_usd="1000")
print(json.dumps({
    "contract": payload["metadata"]["contract_version"],
    "mode": payload["metadata"]["evidence_mode"],
    "returned": payload["metadata"]["coverage"]["returned_count"],
    "verified": payload["metadata"]["coverage"]["foundry_verified_count"],
}, sort_keys=True))
"""
            environment = os.environ.copy()
            environment.pop("DEX_DEPTH_RPC_ETH", None)
            environment["PYTHONDONTWRITEBYTECODE"] = "1"
            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=checkout,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout), {
            "contract": "opportunity_historical_demo_summary/v1",
            "mode": "offline_test_fixture",
            "returned": 2,
            "verified": 0,
        })

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
        self.assertEqual(
            metadata["validation_boundary"], "spawned_local_process"
        )
        self.assertEqual(metadata["temporal_scope"], "historical_demo_fixture")
        self.assertEqual(
            metadata["execution_claim"], "synthetic_fixture_no_execution"
        )
        self.assertEqual(metadata["execution_status"], "not_run")
        self.assertEqual(
            metadata["simulation_basis"], "deterministic_repository_fixture"
        )
        self.assertEqual(
            metadata["reference_kind"], "synthetic_block_coordinate"
        )
        self.assertEqual(coverage["scenario_count"], 10)
        self.assertEqual(coverage["returned_count"], 10)
        self.assertEqual(coverage["foundry_verified_count"], 0)
        self.assertEqual(coverage["strict_count"], 0)
        self.assertEqual(coverage["executable_count"], 0)
        self.assertEqual(coverage["attested_count"], 0)
        self.assertEqual(coverage["positive_count"], 0)
        self.assertEqual(len(payload["routes"]), 10)
        self.assertTrue(all(
            row["foundry_verified"] is False
            and row["opportunity_class"] == "research_estimate"
            and row["route_mode"] == "synthetic_fixture_no_execution"
            and row["executor_model"] == "not_run"
            and "gas_used" not in row
            and type(row["gas_assumption"]) is int
            and row["gas_assumption"] >= 0
            and Decimal(row["research_net_edge_usd"]) <= 0
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

    def test_fixture_artifact_hashes_bind_content_and_reject_tampering(self):
        from scripts import historical_opportunity_demo_fixture as fixture_module

        def canonical_sha256(value):
            payload = json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            return hashlib.sha256(payload).hexdigest()

        bundle = fixture_module._build_repository_fixture_bundle()
        rows = fixture_module._validate_and_project_demo_bundle(bundle)
        self.assertEqual(len(rows), 10)
        scenario = bundle["evidence"]["scenarios"][0]
        artifact_set = bundle["artifacts"][scenario["opportunity_id"]]
        self.assertEqual(
            scenario["receipt_sha256"],
            canonical_sha256(artifact_set["receipt_record"]),
        )
        self.assertEqual(
            scenario["trace_sha256"],
            canonical_sha256(artifact_set["workflow_trace"]),
        )
        self.assertEqual(
            scenario["result_sha256"],
            canonical_sha256(artifact_set["result_record"]),
        )
        self.assertEqual(
            scenario["proof_inputs_hash"],
            canonical_sha256(artifact_set["proof_inputs"]),
        )

        def mutate_receipt(artifacts):
            artifacts["receipt_record"]["gas_assumption"] += 1

        def mutate_trace(artifacts):
            artifacts["workflow_trace"]["steps"].append("invented_step")

        def mutate_result(artifacts):
            artifacts["result_record"]["research_net_edge_usd"] = "999"

        def mutate_inputs(artifacts):
            artifacts["proof_inputs"]["notional_usd"] = "999"

        mutations = (
            ("receipt", mutate_receipt),
            ("trace", mutate_trace),
            ("result", mutate_result),
            ("proof_inputs", mutate_inputs),
        )
        for artifact_name, mutate in mutations:
            with self.subTest(artifact=artifact_name):
                tampered = copy.deepcopy(bundle)
                tampered_scenario = tampered["evidence"]["scenarios"][0]
                tampered_artifacts = tampered["artifacts"][
                    tampered_scenario["opportunity_id"]
                ]
                mutate(tampered_artifacts)
                with self.assertRaisesRegex(
                    ValueError, "fixture artifact hash binding failed"
                ):
                    fixture_module._validate_and_project_demo_bundle(tampered)

    def test_fixture_close_removes_temporary_tree_and_is_idempotent(self):
        fixture = demo.HistoricalOpportunityDemoFixture()
        temporary_root = Path(fixture.data_dir)
        self.assertTrue(temporary_root.is_dir())

        fixture.close()

        self.assertFalse(temporary_root.exists())
        with self.assertRaisesRegex(RuntimeError, "offline fixture is closed"):
            fixture.build_payload()
        fixture.close()


if __name__ == "__main__":
    unittest.main()
