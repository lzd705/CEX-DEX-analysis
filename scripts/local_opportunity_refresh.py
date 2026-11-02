"""Opt-in, loopback-only manual collection for the fixed live CEX runner."""

from __future__ import annotations

import io
import math
from pathlib import Path
import threading
import time
from urllib.parse import urlparse


REFRESH_PATH = "/api/local/opportunity-refresh"
SCRIPT_PATH = "/local-opportunity-refresh.js"
_RECEIPT_FIELDS = (
    "status", "route_cohort_id", "manifest_sha256", "token_pairs", "venues",
    "market_count", "route_count", "opportunity_count", "strict_eligible_count",
)


class LocalOpportunityRefresh:
    """One synchronous collection at a time; the HTTP worker owns its lifetime."""

    def __init__(self, callback, *, clock=time.monotonic):
        self._callback = callback
        self._clock = clock
        self._lock = threading.Lock()
        self._state = "idle"
        self._next_allowed = 0.0
        self._receipt = None

    def _snapshot(self):
        result = {
            "state": self._state,
            "retry_after_seconds": max(0, math.ceil(self._next_allowed - self._clock())),
        }
        if self._state == "failed":
            result["error"] = "refresh_failed"
        if self._receipt is not None:
            result["receipt"] = dict(self._receipt)
        return result

    def status(self):
        with self._lock:
            return self._snapshot()

    def refresh(self):
        with self._lock:
            if self._state == "running":
                return 409, self._snapshot()
            if self._clock() < self._next_allowed:
                return 429, self._snapshot()
            self._state = "running"
        try:
            receipt = self._callback()
            if not isinstance(receipt, dict) or receipt.get("status") != "published":
                raise ValueError("publication did not complete")
            public_receipt = {key: receipt[key] for key in _RECEIPT_FIELDS if key in receipt}
        except Exception:
            with self._lock:
                self._state = "failed"
                self._next_allowed = self._clock() + 30
                return 502, self._snapshot()
        with self._lock:
            self._receipt = public_receipt
            self._state = "succeeded"
            self._next_allowed = self._clock() + 30
            return 200, self._snapshot()


def local_refresh_handler(base_handler, dashboard, controller):
    """Add a fixed local action without enabling the dashboard's write surfaces."""

    class LocalRefreshHandler(base_handler):
        def _local_request(self):
            host = "127.0.0.1:{}".format(self.server.server_address[1])
            return (
                self.client_address[0] == "127.0.0.1"
                and self.headers.get_all("Host", []) == [host]
            )

        def do_GET(self):  # noqa: N802
            path = urlparse(self.path).path
            if path not in (REFRESH_PATH, SCRIPT_PATH):
                return super().do_GET()
            if not self._local_request():
                self.send_json({"error": "local_request_required"}, 403)
                return
            if self.path != path:
                self.send_json({"error": "invalid_refresh_request"}, 400)
                return
            if path == REFRESH_PATH:
                self.send_json(controller.status())
                return
            body = Path(__file__).with_suffix(".js").read_bytes()
            self._cache_control = "no-store"
            self.send_response(200)
            self.send_header("Content-Type", "text/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):  # noqa: N802
            if urlparse(self.path).path != REFRESH_PATH:
                return super().do_POST()
            # Reject without reading bodies: callers cannot supply targets or settings.
            self.close_connection = True
            origin = "http://127.0.0.1:{}".format(self.server.server_address[1])
            if (
                not self._local_request()
                or self.headers.get_all("Origin", []) != [origin]
                or self.headers.get_all("X-Opportunity-Refresh", []) != ["1"]
                or self.headers.get("Sec-Fetch-Site", "same-origin") != "same-origin"
            ):
                self.send_json({"error": "local_request_required"}, 403)
                return
            if (
                self.path != REFRESH_PATH
                or self.headers.get_all("Content-Length", []) not in ([], ["0"])
                or self.headers.get_all("Transfer-Encoding", [])
            ):
                self.send_json({"error": "invalid_refresh_request"}, 400)
                return
            status, body = controller.refresh()
            self.send_json(body, status)

        def send_head(self):
            translated = Path(self.translate_path(self.path))
            if translated != dashboard.STATIC_ROOT / "index.html":
                return super().send_head()
            html = dashboard.render_versioned_html(translated)
            body = html.replace(
                "</body>", '<script src="' + SCRIPT_PATH + '"></script></body>', 1,
            ).encode("utf-8")
            self._cache_control = "no-store"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return io.BytesIO(body)

    return LocalRefreshHandler
