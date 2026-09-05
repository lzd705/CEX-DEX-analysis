"""Behavioral tests for the opt-in loopback manual refresh boundary."""

import importlib
import io
import json
import threading
import unittest
from email.message import Message
from pathlib import Path
from types import SimpleNamespace


class LocalRefreshTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.find_spec("scripts.local_opportunity_refresh")
        self.assertIsNotNone(spec, "local refresh feature is not implemented")
        self.module = importlib.import_module("scripts.local_opportunity_refresh")

    def test_success_cooldown_and_second_snapshot(self):
        calls = []
        now = [0]

        def collect():
            calls.append(1)
            return {"status": "published", "route_cohort_id": str(len(calls)),
                    "private_path": "/private/not-public"}

        controller = self.module.LocalOpportunityRefresh(collect, clock=lambda: now[0])
        self.assertEqual(controller.status()["state"], "idle")
        status, body = controller.refresh()
        self.assertEqual(status, 200)
        self.assertEqual(body["receipt"]["route_cohort_id"], "1")
        self.assertNotIn("private_path", body["receipt"])
        self.assertEqual(controller.refresh()[0], 429)
        self.assertEqual(len(calls), 1)
        now[0] = 30
        self.assertEqual(controller.refresh()[0], 200)
        self.assertEqual(controller.status()["receipt"]["route_cohort_id"], "2")

    def test_failures_are_redacted_and_throttled(self):
        def collect():
            raise RuntimeError("secret local path and credential")

        controller = self.module.LocalOpportunityRefresh(collect)
        code, body = controller.refresh()
        self.assertEqual(code, 502)
        self.assertEqual(body["state"], "failed")
        self.assertEqual(body["error"], "refresh_failed")
        self.assertNotIn("secret", json.dumps(body))
        self.assertEqual(controller.refresh()[0], 429)

    def test_concurrent_requests_never_overlap_collection(self):
        entered, release = threading.Event(), threading.Event()

        def collect():
            entered.set()
            self.assertTrue(release.wait(5))
            return {"status": "published"}

        controller = self.module.LocalOpportunityRefresh(collect)
        worker = threading.Thread(target=controller.refresh)
        worker.start()
        try:
            self.assertTrue(entered.wait(5))
            self.assertEqual(controller.status()["state"], "running")
            self.assertEqual(controller.refresh()[0], 409)
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())

    def handler(self, *, headers=None, path="/api/local/opportunity-refresh", client="127.0.0.1"):
        calls = []

        class Base:
            def do_GET(self):
                self.fallback = True

            def do_POST(self):
                self.fallback = True

            def translate_path(self, path):
                return "/static/index.html"

            def send_head(self):
                return io.BytesIO(b"original")

        dashboard = SimpleNamespace(
            STATIC_ROOT=Path("/static"),
            render_versioned_html=lambda path: "<html><body>app</body></html>",
        )
        controller = self.module.LocalOpportunityRefresh(lambda: calls.append(1) or {"status": "published"})
        kind = self.module.local_refresh_handler(Base, dashboard, controller)
        handler = object.__new__(kind)
        handler.path = path
        handler.client_address = (client, 1234)
        handler.server = SimpleNamespace(server_address=("127.0.0.1", 8765))
        handler.command = "GET"
        handler.headers = Message()
        values = {"Host": "127.0.0.1:8765", "Origin": "http://127.0.0.1:8765",
                  "X-Opportunity-Refresh": "1", "Content-Length": "0"}
        if headers is not None:
            values.update(headers)
        for name, value in values.items():
            if value is not None:
                handler.headers[name] = value
        handler.responses, handler.sent_headers = [], {}
        handler.send_json = lambda body, status=200: handler.responses.append((status, body))
        handler.send_response = lambda code: handler.responses.append(code)
        handler.send_header = lambda name, value: handler.sent_headers.update({name: value})
        handler.end_headers = lambda: None
        handler.wfile = io.BytesIO()
        return handler, calls

    def test_valid_post_refreshes_but_get_only_reads(self):
        handler, calls = self.handler()
        handler.do_GET()
        self.assertEqual(calls, [])
        self.assertEqual(handler.responses[0][1]["state"], "idle")
        handler.do_POST()
        self.assertEqual(calls, [1])
        self.assertEqual(handler.responses[-1][0], 200)

    def test_request_boundary_rejects_cross_site_and_payload_controls(self):
        cases = [
            ({"Host": "evil.example"}, None, "127.0.0.1"),
            ({"Origin": "http://evil.example"}, None, "127.0.0.1"),
            ({"Origin": None}, None, "127.0.0.1"),
            ({"X-Opportunity-Refresh": None}, None, "127.0.0.1"),
            ({"Sec-Fetch-Site": "cross-site"}, None, "127.0.0.1"),
            ({"Content-Length": "2"}, None, "127.0.0.1"),
            ({"Transfer-Encoding": "chunked"}, None, "127.0.0.1"),
            ({}, "/api/local/opportunity-refresh?url=https://other", "127.0.0.1"),
            ({}, None, "192.0.2.1"),
        ]
        for headers, path, client in cases:
            with self.subTest(headers=headers, path=path, client=client):
                handler, calls = self.handler(headers=headers, client=client, **({"path": path} if path else {}))
                handler.do_POST()
                self.assertIn(handler.responses[-1][0], (400, 403))
                self.assertEqual(calls, [])

    def test_duplicate_origin_is_rejected(self):
        handler, calls = self.handler()
        handler.headers["Origin"] = "http://evil.example"
        handler.do_POST()
        self.assertEqual(handler.responses[-1][0], 403)
        self.assertEqual(calls, [])

    def test_local_shell_injects_external_script_and_other_gets_delegate(self):
        handler, calls = self.handler(path="/opportunities?notional=1000")
        body = handler.send_head().read().decode()
        self.assertIn('src="/local-opportunity-refresh.js"', body)
        self.assertNotIn("<script>", body)
        self.assertEqual(int(handler.sent_headers["Content-Length"]), len(body.encode()))
        handler.path = "/health"
        handler.do_GET()
        self.assertTrue(handler.fallback)
        self.assertEqual(calls, [])

    def test_script_is_served_without_inline_csp_exception(self):
        handler, calls = self.handler(path="/local-opportunity-refresh.js")
        handler.do_GET()
        self.assertIn("javascript", handler.sent_headers["Content-Type"])
        self.assertGreater(len(handler.wfile.getvalue()), 0)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
