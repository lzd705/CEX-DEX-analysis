#!/usr/bin/env python3
"""Serve the sealed synthetic Current Opportunity workflow on loopback."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from http import HTTPStatus
import importlib
import json
import os
from pathlib import Path
import posixpath
import sys
from typing import Iterator, Optional, Sequence, TextIO
from urllib.parse import parse_qs, unquote, urlparse


if __package__ in {None, ""}:  # pragma: no cover - direct script bootstrap
    _PROJECT_ROOT_TEXT = str(Path(__file__).resolve().parents[1])
    if _PROJECT_ROOT_TEXT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT_TEXT)

from scripts.current_opportunity_demo_fixture import (
    DEMO_CONTRACT,
    DEMO_EVIDENCE_MODE,
    DEMO_EXECUTION_CLAIM,
    DEMO_RESEARCH_MEV_BPS,
    DEMO_SIGNED_SCOPE,
    DEMO_SIMULATION_BASIS,
    DEMO_TEMPORAL_SCOPE,
    DEMO_TOKEN_PAIR,
    DEMO_VERIFICATION_STATUS,
    CurrentOpportunityDemoFixture,
)


DEMO_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
CURRENT_DEMO_PATH = (
    "/opportunities?notional=1000&class=all&route_type=dex_dex"
    "&availability=all&sort=net_edge_usd&dir=desc#local-demo-fixture"
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
REMOVED_DATA_OVERRIDE_ENVIRONMENT_NAMES = (
    "MARKET_DATABASE",
    "MARKET_CEX_DATA",
    "MARKET_DEX_DATA",
    "MARKET_TVL_DATA",
    "MARKET_CEX_DEPTH_DATA",
    "MARKET_DEX_DEPTH_DATA",
    "MARKET_CEX_EXECUTION_COST_DATA",
    "MARKET_DEX_EXECUTION_COST_DATA",
    "MARKET_HISTORICAL_OPPORTUNITY_DATA_DIR",
)
REMOVED_PRIVATE_PROFILE_ENVIRONMENT_NAMES = (
    "MARKET_CEX_PRIVATE_FEE_PROFILE",
    "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE",
)
DEMO_ENVIRONMENT_NAMES = (
    "DASHBOARD_SKIP_LOCAL_ENV",
    "MARKET_ROUTE_DATA_DIR",
    "MARKET_DATA_DIR",
    "MARKET_EVENT_DATA_DIR",
    "MARKET_CEX_INSTRUMENT_LIFECYCLE",
    "ADMIN_JOB_DIR",
    "TOKEN_REGISTRY_PATH",
    "ADMIN_USERNAME",
    "ADMIN_PASSWORD_HASH",
    "ADMIN_LOGIN_REQUIRED",
    "ADMIN_ALLOW_OPEN_LOCAL",
    "TRUST_LOOPBACK_PROXY_CLIENT_IP",
    *REMOVED_DATA_OVERRIDE_ENVIRONMENT_NAMES,
    *REMOVED_PRIVATE_PROFILE_ENVIRONMENT_NAMES,
    *WRITE_SURFACE_ENVIRONMENT_FLAGS,
)


def _load_dashboard_server():
    if "dashboard.server" in sys.modules:
        raise RuntimeError(
            "Current Opportunity demo requires a fresh process before "
            "dashboard import"
        )
    return importlib.import_module("dashboard.server")


def _demo_static_path_is_denied(path: str) -> bool:
    normalized = (
        "/" + posixpath.normpath(unquote(path)).lstrip("/")
    ).casefold()
    return normalized in DEMO_DENIED_STATIC_PATHS


@contextmanager
def _isolated_demo_environment(fixture: object) -> Iterator[None]:
    """Remove inherited data/profile overrides before importing the server."""

    data_dir = Path(getattr(fixture, "data_dir")).resolve()
    routes_root = Path(getattr(fixture, "routes_root")).resolve()
    runtime_root = data_dir / "demo-runtime"
    values = {
        "DASHBOARD_SKIP_LOCAL_ENV": "true",
        "MARKET_ROUTE_DATA_DIR": str(routes_root),
        "MARKET_DATA_DIR": str(runtime_root),
        "MARKET_EVENT_DATA_DIR": str(runtime_root / "events"),
        "MARKET_CEX_INSTRUMENT_LIFECYCLE": str(
            runtime_root / "cex_instrument_lifecycle.json"
        ),
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
        for name in DEMO_ENVIRONMENT_NAMES:
            os.environ.pop(name, None)
        os.environ.update(values)
        yield
    finally:
        for name in DEMO_ENVIRONMENT_NAMES:
            if name in prior:
                os.environ[name] = prior[name]
            else:
                os.environ.pop(name, None)


def _demo_handler(dashboard_server: object, fixture: object):
    """Serve static UI and exactly one read-only fixture API."""

    class CurrentOpportunityDemoHandler(
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
            if parsed.path == "/api/markets/opportunities":
                try:
                    query_items = dashboard_server.public_api_query_items(
                        "opportunities",
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
                except Exception:
                    validation_error = getattr(
                        self,
                        "send_opportunity_data_validation_error",
                        None,
                    )
                    if callable(validation_error):
                        validation_error()
                    else:  # pragma: no cover - production handler provides it
                        self.send_json(
                            {"error": "Opportunity data validation failed"},
                            HTTPStatus.SERVICE_UNAVAILABLE,
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

    return CurrentOpportunityDemoHandler


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
            "Run a disposable loopback-only Current Opportunity demo from "
            "one SHA-256-sealed synthetic repository fixture"
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
    try:
        print(
            "Preparing sealed synthetic Current Opportunity workflow "
            "(repository-only; no external RPC)...",
            file=output,
            flush=True,
        )
        fixture = CurrentOpportunityDemoFixture()
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
            try:
                for target, attribute, _prior_value in runtime_write_state:
                    setattr(target, attribute, False)
                if dashboard_server.write_surface_enabled():
                    raise RuntimeError(
                        "Current Opportunity demo write surfaces could not "
                        "be disabled"
                    )

                dashboard_server.clear_runtime_caches()
                http_server = dashboard_server.ThreadingHTTPServer(
                    (DEMO_HOST, port),
                    _demo_handler(dashboard_server, fixture),
                )
                http_server.daemon_threads = False
                bound_host = str(http_server.server_address[0])
                bound_port = int(http_server.server_address[1])
                if bound_host != DEMO_HOST:
                    raise RuntimeError(
                        "Current Opportunity demo did not bind to loopback"
                    )
                print(
                    json.dumps(
                        {
                            "contract_version": DEMO_CONTRACT,
                            "demo_fixture": True,
                            "evidence_mode": DEMO_EVIDENCE_MODE,
                            "execution_claim": DEMO_EXECUTION_CLAIM,
                            "execution_status": "not_run",
                            "live_rpc": False,
                            "network_scope": "loopback_only",
                            "research_mev_bps": DEMO_RESEARCH_MEV_BPS,
                            "route_cohort_id": fixture.pointer.get(
                                "route_cohort_id"
                            ),
                            "simulation_basis": DEMO_SIMULATION_BASIS,
                            "signed_scope": DEMO_SIGNED_SCOPE,
                            "temporal_scope": DEMO_TEMPORAL_SCOPE,
                            "token_pair": DEMO_TOKEN_PAIR,
                            "url": "http://{}:{}{}".format(
                                DEMO_HOST, bound_port, CURRENT_DEMO_PATH
                            ),
                            "verification_status": DEMO_VERIFICATION_STATUS,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    file=output,
                    flush=True,
                )
                print(
                    "LOCAL SYNTHETIC KAT: fixed fixture clock; no live RPC, "
                    "market-data request, execution, or profit claim. SHA-256 "
                    "pins seal the full fixture; SSHSIG authenticates only "
                    "the submission-policy snapshot.",
                    file=output,
                    flush=True,
                )
                print(
                    "Press Ctrl-C to stop and remove the temporary bundle.",
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


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = parse_args(argv)
    try:
        serve_demo(port=arguments.port)
    except KeyboardInterrupt:
        print(
            "Current Opportunity demo interrupted before startup; cleanup "
            "complete.",
            file=sys.stderr,
            flush=True,
        )
        return 130
    except Exception as error:
        print(
            "Current Opportunity demo could not start: {}: {}".format(
                type(error).__name__, error
            ),
            file=sys.stderr,
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
