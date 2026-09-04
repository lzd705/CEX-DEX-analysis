"""End-to-end contract tests for the loopback Current Opportunity demo."""

from __future__ import annotations

import errno
import io
import json
import os
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import scripts.run_current_opportunity_demo as demo
from scripts.current_opportunity_demo_fixture import (
    DEMO_ASSET_PATH,
    load_demo_fixture_bundle,
)
from scripts.route_publication import (
    RoutePublicationError,
    load_latest_complete_route_bundle,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _fake_dashboard(http_server):
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


class CurrentOpportunityDemoRunnerTests(unittest.TestCase):
    def test_cli_is_fixed_to_loopback_and_validates_port(self):
        self.assertEqual(demo.parse_args([]).port, 8765)
        self.assertEqual(demo.parse_args(["--port", "0"]).port, 0)
        self.assertEqual(demo.parse_args(["--port", "65535"]).port, 65535)
        for argv in (
            ["--port", "-1"],
            ["--port", "65536"],
            ["--port", "not-a-port"],
            ["--host", "0.0.0.0"],
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                demo.parse_args(argv)

    def test_runner_serves_only_loopback_and_restores_all_state(self):
        fixture = Mock()
        fixture.data_dir = Path("/fixture")
        fixture.routes_root = Path("/fixture/routes")
        fixture.pointer = {
            "route_cohort_id": "cohort:" + "a" * 64,
            "manifest_sha256": "b" * 64,
        }
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 43210)
        http_server.serve_forever.side_effect = KeyboardInterrupt
        dashboard = _fake_dashboard(http_server)
        output = io.StringIO()

        inherited_environment = {
            "MARKET_ROUTE_DATA_DIR": "/original/routes",
            "MARKET_CEX_DATA": "/must/not/be/read.csv",
            "MARKET_CEX_PRIVATE_FEE_PROFILE": "/must/not/be/read.json",
        }

        def load_dashboard():
            self.assertEqual(
                os.environ["MARKET_ROUTE_DATA_DIR"], "/fixture/routes"
            )
            self.assertNotIn("MARKET_CEX_DATA", os.environ)
            self.assertNotIn("MARKET_CEX_PRIVATE_FEE_PROFILE", os.environ)
            self.assertEqual(os.environ["ADMIN_ENABLED"], "false")
            return dashboard

        with patch.object(
            demo, "CurrentOpportunityDemoFixture", return_value=fixture
        ), patch.object(
            demo, "_load_dashboard_server", side_effect=load_dashboard
        ), patch.dict(
            os.environ,
            inherited_environment,
            clear=False,
        ):
            demo.serve_demo(port=0, output=output)
            for name, value in inherited_environment.items():
                self.assertEqual(os.environ[name], value)

        call = dashboard.ThreadingHTTPServer.call_args
        self.assertEqual(call.args[0], ("127.0.0.1", 0))
        self.assertTrue(issubclass(call.args[1], dashboard.MarketMonitorHandler))
        self.assertIs(http_server.daemon_threads, False)
        http_server.serve_forever.assert_called_once_with()
        http_server.server_close.assert_called_once_with()
        fixture.close.assert_called_once_with()
        self.assertTrue(dashboard.ADMIN_SERVICE.enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.add_token_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.quality_retry_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.fact_refresh_enabled)

        lines = output.getvalue().splitlines()
        ready = json.loads(lines[1])
        self.assertEqual(ready["contract_version"], demo.DEMO_CONTRACT)
        self.assertTrue(ready["demo_fixture"])
        self.assertEqual(ready["network_scope"], "loopback_only")
        self.assertEqual(ready["token_pair"], "AAA/WETH")
        self.assertEqual(ready["research_mev_bps"], "25")
        self.assertEqual(
            ready["signed_scope"], "submission_policy_snapshot_only"
        )
        self.assertEqual(
            ready["url"],
            "http://127.0.0.1:43210/opportunities"
            "?notional=1000&class=all&route_type=dex_dex&availability=all"
            "&sort=net_edge_usd&dir=desc#local-demo-fixture",
        )

    def test_write_surface_check_failure_still_restores_globals(self):
        fixture = Mock(
            data_dir=Path("/fixture"),
            routes_root=Path("/fixture/routes"),
        )
        dashboard = _fake_dashboard(Mock())
        dashboard.write_surface_enabled = Mock(
            side_effect=RuntimeError("write state unavailable")
        )
        with patch.object(
            demo, "CurrentOpportunityDemoFixture", return_value=fixture
        ), patch.object(
            demo, "_load_dashboard_server", return_value=dashboard
        ), self.assertRaisesRegex(RuntimeError, "write state unavailable"):
            demo.serve_demo(port=0, output=io.StringIO())
        fixture.close.assert_called_once_with()
        self.assertTrue(dashboard.ADMIN_SERVICE.enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.add_token_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.quality_retry_enabled)
        self.assertTrue(dashboard.PUBLIC_ACTION_POLICY.fact_refresh_enabled)

    def test_handler_allowlists_one_read_only_api(self):
        class FakeMarketMonitorHandler:
            pass

        class FakeClientError(ValueError):
            pass

        dashboard = SimpleNamespace(
            MarketMonitorHandler=FakeMarketMonitorHandler,
            PublicClientRequestError=FakeClientError,
            public_api_query_items=Mock(
                return_value=(("notional", "1000"),)
            ),
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
        handler.path = "/api/markets/opportunities?notional=1000"
        handler.send_json = Mock()
        handler.do_GET()
        fixture.build_payload.assert_called_once_with(notional_usd="1000")
        handler.send_json.assert_called_once_with({"demo_fixture": True})

        for path in (
            "/api/markets/opportunities/historical",
            "/api/markets/summary",
            "/api/admin/session",
            "/health",
            "/admin.html",
            "/actions.html",
            "/actions.js",
            "/actions.css",
            "/foo/../actions.html",
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
        mutation.path = "/api/markets/opportunities"
        mutation.send_json = Mock()
        mutation.do_POST()
        mutation.send_json.assert_called_once_with(
            {"error": "Not found"}, HTTPStatus.NOT_FOUND
        )

    def test_real_process_serves_workflow_and_denies_other_surfaces(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(("127.0.0.1", 0))
        except OSError as error:
            if error.errno == errno.EPERM:
                self.skipTest(
                    "execution sandbox does not permit loopback bind"
                )
            raise

        process = subprocess.Popen(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts/run_current_opportunity_demo.py"),
                "--port",
                "0",
            ],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdout is not None
        try:
            preparing = process.stdout.readline().strip()
            ready_line = process.stdout.readline()
            if not ready_line:
                _stdout, startup_error = process.communicate(timeout=5)
                self.fail(
                    "Current Opportunity demo exited before ready: {}".format(
                        startup_error
                    )
                )
            ready = json.loads(ready_line)
            self.assertIn("no external RPC", preparing)
            parsed = urlparse(ready["url"])
            self.assertEqual(parsed.hostname, "127.0.0.1")
            origin = "http://127.0.0.1:{}".format(parsed.port)

            with urlopen(ready["url"], timeout=10) as response:
                self.assertEqual(response.status, HTTPStatus.OK)
                self.assertIn(
                    b'id="opportunities-view"', response.read()
                )
            with urlopen(ready["url"], timeout=10) as response:
                self.assertIn(b'id="estimate-opportunity-body"', response.read())

            api_url = (
                origin + "/api/markets/opportunities?" + parsed.query
            )
            with urlopen(api_url, timeout=10) as response:
                payload = json.load(response)
            self.assertEqual(
                payload["metadata"]["contract_version"], demo.DEMO_CONTRACT
            )
            self.assertEqual(len(payload["routes"]), 1)
            self.assertLess(
                Decimal(payload["routes"][0]["net_edge_usd"]), 0
            )

            for request in (
                Request(origin + "/api/markets/summary"),
                Request(
                    origin + "/api/markets/opportunities",
                    data=b"{}",
                    method="POST",
                ),
            ):
                with self.subTest(url=request.full_url), self.assertRaises(
                    HTTPError
                ) as captured:
                    urlopen(request, timeout=10)
                self.assertEqual(captured.exception.code, HTTPStatus.NOT_FOUND)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                _stdout, stderr = process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                _stdout, stderr = process.communicate(timeout=5)
                self.fail("Current Opportunity demo did not stop after SIGINT")
        self.assertEqual(process.returncode, 0, stderr)


class CurrentOpportunityDemoFixtureTests(unittest.TestCase):
    def test_fixture_runs_signed_replay_through_publication_and_api_projection(self):
        with demo.CurrentOpportunityDemoFixture() as fixture:
            payload = fixture.build_payload(
                notional_usd="1000",
                opportunity_class="estimate",
                route_type="dex_dex",
                availability="available",
                sort="net_edge_usd",
                direction="desc",
            )
            loaded = load_latest_complete_route_bundle(
                fixture.routes_root,
                core_root=fixture.routes_root / "core",
            )
            self.assertEqual(loaded["pointer"], fixture.pointer)
            published = {
                row["opportunity_id"]: row
                for row in loaded["opportunities"]
            }
            temporary_root = fixture.data_dir

        self.assertFalse(temporary_root.exists())
        metadata = payload["metadata"]
        self.assertEqual(metadata["contract_version"], demo.DEMO_CONTRACT)
        self.assertTrue(metadata["demo_fixture"])
        self.assertEqual(
            metadata["evidence_mode"],
            "offline_sha256_sealed_fixture_with_signed_policy",
        )
        self.assertEqual(metadata["verification_status"], "fixture_integrity_verified")
        self.assertEqual(metadata["temporal_scope"], "fixed_fixture_clock")
        self.assertEqual(metadata["execution_claim"], "synthetic_fixture_no_execution")
        self.assertEqual(metadata["token_pair"], "AAA/WETH")
        self.assertEqual(metadata["research_mev_bps"], "25")
        self.assertEqual(
            metadata["signed_scope"], "submission_policy_snapshot_only"
        )
        self.assertEqual(metadata["coverage"]["returned_count"], 1)

        self.assertEqual(len(payload["routes"]), 1)
        route = payload["routes"][0]
        self.assertEqual(route["token_symbol"], "AAA")
        self.assertEqual(route["route_type"], "dex_dex")
        self.assertEqual(route["opportunity_class"], "research_estimate")
        self.assertEqual(route["availability"], {
            "status": "available", "reason": None,
        })
        self.assertIsNotNone(route["gross_edge_usd"])
        self.assertIsNotNone(route["net_edge_usd"])
        self.assertLess(Decimal(str(route["net_edge_usd"])), 0)
        self.assertEqual(len(route["cost_components"]), 10)
        pool_fees = [
            row for row in route["cost_components"]
            if row["component_type"] == "pool_swap_fee"
        ]
        self.assertEqual(len(pool_fees), 2)
        self.assertTrue(all(
            row["value_status"] == "measured"
            and row["rate_bps"] == "30"
            and row["embedded_in_leg_quote"] is True
            for row in pool_fees
        ))
        mev = next(
            row for row in route["cost_components"]
            if row["component_type"] == "mev_buffer"
        )
        self.assertEqual(mev["value_status"], "assumed")
        self.assertEqual(mev["rate_bps"], "25")
        self.assertEqual(Decimal(str(mev["amount_usd"])), Decimal("2.5"))
        self.assertEqual(len(route["source_links"]), 2)
        self.assertTrue(all(
            source["market_id"] and source["url"] is None
            for source in route["source_links"]
        ))
        published_route = published[route["opportunity_id"]]
        self.assertFalse(published_route["strict_eligible"])
        self.assertFalse(published_route["strict_ready_for_publication"])
        self.assertIsNone(published_route["publication_attestation_sha256"])

    def test_fixture_has_five_reconciled_notionals_and_fixed_clock(self):
        with demo.CurrentOpportunityDemoFixture() as fixture:
            first = fixture.build_payload(
                opportunity_class="estimate",
                route_type="dex_dex",
                availability="available",
                sort="net_edge_usd",
                direction="desc",
            )
            second = fixture.build_payload(
                opportunity_class="estimate",
                route_type="dex_dex",
                availability="available",
                sort="net_edge_usd",
                direction="desc",
            )
            loaded = fixture._loaded()

        notionals = {
            route["requested_notional_usd"] for route in first["routes"]
        }
        self.assertEqual(
            notionals, {"1000", "5000", "10000", "50000", "100000"}
        )
        self.assertEqual(first["routes"], second["routes"])
        self.assertEqual(first["metadata"]["coverage"]["returned_count"], 5)

        for opportunity in loaded["opportunities"]:
            reconciled = (
                Decimal(opportunity["gross_edge_usd"])
                - Decimal(opportunity["strict_nonembedded_cost_usd"])
                - Decimal(opportunity["research_bounded_cost_usd"])
                - Decimal(opportunity["research_assumed_cost_usd"])
            )
            self.assertEqual(
                reconciled, Decimal(opportunity["research_net_edge_usd"])
            )

    def test_asset_and_published_pointer_tampering_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            tampered_path = Path(temporary) / "fixture.json.gz.b64"
            tampered = bytearray(DEMO_ASSET_PATH.read_bytes())
            tampered[10] = ord("A") if tampered[10] != ord("A") else ord("B")
            tampered_path.write_bytes(tampered)
            with self.assertRaisesRegex(RuntimeError, "hash differs"):
                load_demo_fixture_bundle(tampered_path)

        with demo.CurrentOpportunityDemoFixture() as fixture:
            (fixture.routes_root / "latest.json").write_bytes(b"{}")
            with self.assertRaises(RoutePublicationError):
                fixture.build_payload()


if __name__ == "__main__":
    unittest.main()
