import io
import json
import os
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import Mock, patch

import scripts.run_historical_opportunity_demo as demo


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
        fixture.historical_root = Path("/fixture/routes/historical")
        fixture.pointer = {"replay_id": "replay:" + "a" * 64}
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 43210)
        http_server.serve_forever.side_effect = KeyboardInterrupt
        output = io.StringIO()

        with patch.object(
            demo, "PublishedHistoricalReplayFixture", return_value=fixture
        ), patch.object(
            demo.dashboard_server,
            "ThreadingHTTPServer",
            return_value=http_server,
        ) as server_factory, patch.object(
            demo.dashboard_server, "clear_runtime_caches"
        ) as clear_caches, patch.dict(
            os.environ,
            {"MARKET_ROUTE_DATA_DIR": "/original/routes"},
            clear=False,
        ):
            demo.serve_demo(port=0, output=output)

            self.assertEqual(
                os.environ["MARKET_ROUTE_DATA_DIR"], "/original/routes"
            )

        server_factory.assert_called_once_with(
            ("127.0.0.1", 0), demo.dashboard_server.MarketMonitorHandler
        )
        http_server.serve_forever.assert_called_once_with()
        http_server.server_close.assert_called_once_with()
        fixture.close.assert_called_once_with()
        self.assertEqual(clear_caches.call_count, 2)

        lines = output.getvalue().splitlines()
        self.assertIn("Preparing LOCAL DEMO FIXTURE", lines[0])
        self.assertIn("no external RPC", lines[0])
        ready = json.loads(lines[1])
        self.assertEqual(
            ready,
            {
                "demo_fixture": True,
                "network_scope": "loopback_only",
                "replay_id": "replay:" + "a" * 64,
                "url": (
                    "http://127.0.0.1:43210/opportunities"
                    "?opportunity_scope=historical#local-demo-fixture"
                ),
            },
        )
        self.assertIn("not live", lines[2].lower())
        self.assertIn("Ctrl-C", lines[3])

    def test_demo_propagates_server_failure_after_cleanup(self):
        fixture = Mock()
        fixture.historical_root = Path("/fixture/routes/historical")
        fixture.pointer = {"replay_id": "replay:" + "b" * 64}
        http_server = Mock()
        http_server.server_address = ("127.0.0.1", 8765)
        http_server.serve_forever.side_effect = RuntimeError("serve failed")

        with patch.dict(os.environ, {}, clear=True):
            with patch.object(
                demo, "PublishedHistoricalReplayFixture", return_value=fixture
            ), patch.object(
                demo.dashboard_server,
                "ThreadingHTTPServer",
                return_value=http_server,
            ), patch.object(
                demo.dashboard_server, "clear_runtime_caches"
            ), self.assertRaisesRegex(RuntimeError, "serve failed"):
                demo.serve_demo(port=8765, output=io.StringIO())
            self.assertNotIn("MARKET_ROUTE_DATA_DIR", os.environ)

        http_server.server_close.assert_called_once_with()
        fixture.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
