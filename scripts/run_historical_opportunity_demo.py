#!/usr/bin/env python3
"""Serve a disposable, repository-only historical Opportunity demo."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import posixpath
import sys
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path
from typing import Iterator, Optional, Sequence, TextIO
from urllib.parse import parse_qs, unquote, urlparse


if __package__ in {None, ""}:  # pragma: no cover - direct script bootstrap
    _PROJECT_ROOT_TEXT = str(Path(__file__).resolve().parents[1])
    if _PROJECT_ROOT_TEXT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT_TEXT)

from scripts.historical_opportunity_demo_fixture import (
    HistoricalOpportunityDemoFixture,
)


DEMO_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
HISTORICAL_DEMO_PATH = (
    "/opportunities?opportunity_scope=historical#local-demo-fixture"
)
DEMO_DENIED_STATIC_PATHS = frozenset({
    "/actions.html", "/actions.js", "/actions.css",
})
WRITE_SURFACE_ENVIRONMENT_FLAGS = (
    "ADMIN_ENABLED",
    "PUBLIC_ADD_TOKEN_ENABLED",
    "PUBLIC_QUALITY_RETRY_ENABLED",
    "PUBLIC_FACT_REFRESH_ENABLED",
)
DEMO_ENVIRONMENT_NAMES = (
    "DASHBOARD_SKIP_LOCAL_ENV",
    "MARKET_ROUTE_DATA_DIR",
    "MARKET_DATA_DIR",
    "MARKET_DATABASE",
    "ADMIN_JOB_DIR",
    "TOKEN_REGISTRY_PATH",
    "ADMIN_USERNAME",
    "ADMIN_PASSWORD_HASH",
    "ADMIN_LOGIN_REQUIRED",
    "ADMIN_ALLOW_OPEN_LOCAL",
    "TRUST_LOOPBACK_PROXY_CLIENT_IP",
    *WRITE_SURFACE_ENVIRONMENT_FLAGS,
)


def _load_dashboard_server():
    if "dashboard.server" in sys.modules:
        raise RuntimeError(
            "local demo requires a fresh process before dashboard import"
        )
    return importlib.import_module("dashboard.server")


def _demo_static_path_is_denied(path: str) -> bool:
    normalized = (
        "/" + posixpath.normpath(unquote(path)).lstrip("/")
    ).casefold()
    return normalized in DEMO_DENIED_STATIC_PATHS


@contextmanager
def _isolated_demo_environment(
    fixture: object,
) -> Iterator[None]:
    data_dir = Path(getattr(fixture, "data_dir")).resolve()
    historical_root = Path(getattr(fixture, "historical_root")).resolve()
    runtime_root = data_dir / "demo-runtime"
    values = {
        "DASHBOARD_SKIP_LOCAL_ENV": "true",
        "MARKET_ROUTE_DATA_DIR": str(historical_root.parent),
        "MARKET_DATA_DIR": str(runtime_root),
        "MARKET_DATABASE": str(runtime_root / "market_facts.sqlite3"),
        "ADMIN_JOB_DIR": str(runtime_root / "admin" / "jobs"),
        "TOKEN_REGISTRY_PATH": str(
            runtime_root / "admin" / "token_registry.json"
        ),
        "ADMIN_USERNAME": "",
        "ADMIN_PASSWORD_HASH": "",
        "ADMIN_LOGIN_REQUIRED": "true",
        "ADMIN_ALLOW_OPEN_LOCAL": "false",
        "TRUST_LOOPBACK_PROXY_CLIENT_IP": "false",
    }
    values.update({
        name: "false" for name in WRITE_SURFACE_ENVIRONMENT_FLAGS
    })
    prior = {
        name: os.environ[name]
        for name in DEMO_ENVIRONMENT_NAMES
        if name in os.environ
    }
    try:
        os.environ.update(values)
        yield
    finally:
        for name in DEMO_ENVIRONMENT_NAMES:
            if name in prior:
                os.environ[name] = prior[name]
            else:
                os.environ.pop(name, None)


def _demo_handler(dashboard_server: object, fixture: object):
    """Serve static UI plus the one read-only fixture API."""

    class HistoricalOpportunityDemoHandler(
        dashboard_server.MarketMonitorHandler
    ):
        def _not_found(self, *, head_only: bool = False) -> None:
            if head_only:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self.send_json(
                    {"error": "Not found"},
                    HTTPStatus.NOT_FOUND,
                )

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/markets/opportunities/historical":
                try:
                    query_items = dashboard_server.public_api_query_items(
                        "opportunities_historical",
                        parse_qs(parsed.query, keep_blank_values=True),
                    )
                    payload = fixture.build_payload(
                        **dashboard_server._historical_opportunity_arguments(
                            query_items
                        )
                    )
                except dashboard_server.PublicClientRequestError as error:
                    self.send_json(
                        {"error": str(error)},
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                self.send_json(payload)
                return
            if (
                parsed.path.startswith("/api/")
                or parsed.path == "/health"
                or _demo_static_path_is_denied(parsed.path)
                or dashboard_server.is_admin_surface_path(parsed.path)
            ):
                self._not_found()
                return
            super().do_GET()

        def do_HEAD(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if (
                parsed.path.startswith("/api/")
                or parsed.path == "/health"
                or _demo_static_path_is_denied(parsed.path)
                or dashboard_server.is_admin_surface_path(parsed.path)
            ):
                self._not_found(head_only=True)
                return
            super().do_HEAD()

        def do_POST(self) -> None:  # noqa: N802
            self._not_found()

        do_PUT = do_POST
        do_PATCH = do_POST
        do_DELETE = do_POST

    return HistoricalOpportunityDemoHandler


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535")
    return port


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a disposable loopback-only Historical Opportunity demo "
            "from repository fixtures"
        )
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help="loopback port; use 0 to select an available port",
    )
    return parser.parse_args(argv)


def serve_demo(*, port: int, output: TextIO = sys.stdout) -> None:
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("port must be an integer between 0 and 65535")

    fixture = None
    http_server = None
    dashboard_server = None
    runtime_write_state = None
    try:
        print(
            "Preparing LOCAL DEMO FIXTURE (repository-only; no external RPC)...",
            file=output,
            flush=True,
        )
        fixture = HistoricalOpportunityDemoFixture()
        with _isolated_demo_environment(fixture):
            dashboard_server = _load_dashboard_server()
            runtime_write_state = (
                (
                    dashboard_server.ADMIN_SERVICE,
                    "enabled",
                    dashboard_server.ADMIN_SERVICE.enabled,
                ),
                (
                    dashboard_server.PUBLIC_ACTION_POLICY,
                    "add_token_enabled",
                    dashboard_server.PUBLIC_ACTION_POLICY.add_token_enabled,
                ),
                (
                    dashboard_server.PUBLIC_ACTION_POLICY,
                    "quality_retry_enabled",
                    dashboard_server.PUBLIC_ACTION_POLICY.quality_retry_enabled,
                ),
                (
                    dashboard_server.PUBLIC_ACTION_POLICY,
                    "fact_refresh_enabled",
                    dashboard_server.PUBLIC_ACTION_POLICY.fact_refresh_enabled,
                ),
            )
            for target, attribute, _prior_value in runtime_write_state:
                setattr(target, attribute, False)
            if dashboard_server.write_surface_enabled():
                raise RuntimeError(
                    "local demo write surfaces could not be disabled"
                )
            try:
                dashboard_server.clear_runtime_caches()
                http_server = dashboard_server.ThreadingHTTPServer(
                    (DEMO_HOST, port),
                    _demo_handler(dashboard_server, fixture),
                )
                http_server.daemon_threads = False
                bound_port = int(http_server.server_address[1])
                print(
                    json.dumps(
                        {
                            "contract_version": (
                                "opportunity_historical_demo_summary/v1"
                            ),
                            "demo_fixture": True,
                            "evidence_mode": "offline_test_fixture",
                            "network_scope": "loopback_only",
                            "replay_id": fixture.pointer["replay_id"],
                            "verification_status": (
                                "structurally_validated"
                            ),
                            "url": "http://{}:{}{}".format(
                                DEMO_HOST, bound_port, HISTORICAL_DEMO_PATH
                            ),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    file=output,
                    flush=True,
                )
                print(
                    "LOCAL DEMO FIXTURE: synthetic repository evidence; "
                    "Foundry verification was not run; not live, current, "
                    "executable, or a profit claim.",
                    file=output,
                    flush=True,
                )
                print(
                    "Press Ctrl-C to stop and clean up.",
                    file=output,
                    flush=True,
                )
                try:
                    http_server.serve_forever()
                except KeyboardInterrupt:
                    pass
            finally:
                try:
                    if http_server is not None:
                        http_server.server_close()
                finally:
                    try:
                        dashboard_server.clear_runtime_caches()
                    finally:
                        for target, attribute, prior_value in (
                            runtime_write_state
                        ):
                            setattr(target, attribute, prior_value)
    finally:
        if fixture is not None:
            fixture.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    arguments = parse_args(argv)
    serve_demo(port=arguments.port)


if __name__ == "__main__":
    main()
