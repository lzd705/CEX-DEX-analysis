#!/usr/bin/env python3
"""Serve one locally published Current Opportunity bundle read-only."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from http import HTTPStatus
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Callable, Iterator, Optional, Sequence, TextIO
from urllib.parse import urlparse


if __package__ in {None, ""}:  # pragma: no cover - direct script bootstrap
    _PROJECT_ROOT_TEXT = str(Path(__file__).resolve().parents[1])
    if _PROJECT_ROOT_TEXT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT_TEXT)


CURRENT_DASHBOARD_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
CURRENT_OPPORTUNITY_PATH = "/opportunities"
READABLE_OPPORTUNITY_HEALTH_STATUSES = frozenset({
    "current", "stale", "unavailable",
})
WRITE_SURFACE_ENVIRONMENT_FLAGS = (
    "ADMIN_ENABLED",
    "PUBLIC_ADD_TOKEN_ENABLED",
    "PUBLIC_QUALITY_RETRY_ENABLED",
    "PUBLIC_FACT_REFRESH_ENABLED",
)
_REMOVED_DATA_OVERRIDE_ENVIRONMENT_NAMES = (
    "MARKET_DATABASE",
    "MARKET_CEX_DATA",
    "MARKET_DEX_DATA",
    "MARKET_TVL_DATA",
    "MARKET_CEX_DEPTH_DATA",
    "MARKET_DEX_DEPTH_DATA",
    "MARKET_CEX_EXECUTION_COST_DATA",
    "MARKET_DEX_EXECUTION_COST_DATA",
)
_REMOVED_PRIVATE_PROFILE_ENVIRONMENT_NAMES = (
    "MARKET_CEX_PRIVATE_FEE_PROFILE",
    "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE",
)
CURRENT_DASHBOARD_ENVIRONMENT_NAMES = (
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
    *_REMOVED_DATA_OVERRIDE_ENVIRONMENT_NAMES,
    *_REMOVED_PRIVATE_PROFILE_ENVIRONMENT_NAMES,
    *WRITE_SURFACE_ENVIRONMENT_FLAGS,
)


def _load_dashboard_server():
    if "dashboard.server" in sys.modules:
        raise RuntimeError(
            "Current Opportunity dashboard requires a fresh process before "
            "dashboard import"
        )
    return importlib.import_module("dashboard.server")


def _current_opportunity_handler(dashboard_server: object):
    """Use route-publication health for the route-only dashboard."""

    class CurrentOpportunityHandler(
        dashboard_server.MarketMonitorHandler
    ):
        def do_GET(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/health":
                super().do_GET()
                return

            route_health = dashboard_server.opportunity_publication_health()
            route_status = route_health.get("status")
            data_ready = (
                route_status in READABLE_OPPORTUNITY_HEALTH_STATUSES
            )
            self.send_json(
                {
                    "status": "ok" if data_ready else "degraded",
                    "data_ready": data_ready,
                    "storage": "route_bundle",
                    "data_status": route_status,
                    "route_opportunities": route_health,
                    "application_sha": (
                        dashboard_server.application_release_sha()
                    ),
                    "asset_sha": dashboard_server.static_asset_sha(),
                    "asset_version": (
                        dashboard_server.static_asset_version()
                    ),
                },
                (
                    HTTPStatus.OK
                    if data_ready
                    else HTTPStatus.SERVICE_UNAVAILABLE
                ),
            )

    return CurrentOpportunityHandler


@contextmanager
def _isolated_dashboard_environment(
    data_dir: Path,
    runtime_root: Path,
) -> Iterator[None]:
    """Constrain dashboard reads and all mutable runtime paths before import."""

    source_root = Path(data_dir).expanduser().resolve()
    isolated_root = Path(runtime_root).expanduser().resolve()
    (isolated_root / "admin/jobs").mkdir(parents=True, exist_ok=True)
    values = {
        "DASHBOARD_SKIP_LOCAL_ENV": "true",
        "MARKET_ROUTE_DATA_DIR": str((source_root / "routes").resolve()),
        "MARKET_DATA_DIR": str(source_root),
        "MARKET_EVENT_DATA_DIR": str((source_root / "events").resolve()),
        "MARKET_CEX_INSTRUMENT_LIFECYCLE": str(
            (source_root / "cex_instrument_lifecycle.json").resolve()
        ),
        "ADMIN_JOB_DIR": str((isolated_root / "admin/jobs").resolve()),
        "TOKEN_REGISTRY_PATH": str(
            (isolated_root / "admin/token_registry.json").resolve()
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
        for name in CURRENT_DASHBOARD_ENVIRONMENT_NAMES
        if name in os.environ
    }
    try:
        for name in CURRENT_DASHBOARD_ENVIRONMENT_NAMES:
            os.environ.pop(name, None)
        os.environ.update(values)
        yield
    finally:
        for name in CURRENT_DASHBOARD_ENVIRONMENT_NAMES:
            if name in prior:
                os.environ[name] = prior[name]
            else:
                os.environ.pop(name, None)


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(
            "port must be between 1 and 65535"
        )
    return port


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Serve a published Current Opportunity bundle read-only on "
            "127.0.0.1"
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="local market-data root containing routes/latest.json",
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help="loopback dashboard port (default: 8765)",
    )
    return parser.parse_args(argv)


def serve_current_dashboard(
    *,
    data_dir: Path,
    port: int,
    output: TextIO = sys.stdout,
    refresh_callback: Optional[Callable[[], dict]] = None,
) -> None:
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("port must be an integer between 1 and 65535")
    source_root = Path(data_dir).expanduser().resolve()
    if not source_root.is_dir():
        raise ValueError("data directory does not exist")

    with tempfile.TemporaryDirectory(
        prefix="current-opportunity-dashboard-"
    ) as temporary:
        runtime_root = Path(temporary)
        with _isolated_dashboard_environment(source_root, runtime_root):
            dashboard_server = _load_dashboard_server()
            if dashboard_server.write_surface_enabled():
                raise RuntimeError(
                    "Current Opportunity dashboard write surfaces could not "
                    "be disabled"
                )

            http_server = None
            try:
                dashboard_server.clear_runtime_caches()
                handler = _current_opportunity_handler(dashboard_server)
                if refresh_callback is not None:
                    from scripts.local_opportunity_refresh import (
                        LocalOpportunityRefresh, local_refresh_handler,
                    )
                    handler = local_refresh_handler(
                        handler, dashboard_server,
                        LocalOpportunityRefresh(refresh_callback),
                    )
                http_server = dashboard_server.ThreadingHTTPServer(
                    (CURRENT_DASHBOARD_HOST, port),
                    handler,
                )
                http_server.daemon_threads = False
                bound_port = int(http_server.server_address[1])
                print(
                    json.dumps(
                        {
                            "current_opportunity": True,
                            "network_scope": "loopback_only",
                            "url": "http://{}:{}{}".format(
                                CURRENT_DASHBOARD_HOST,
                                bound_port,
                                CURRENT_OPPORTUNITY_PATH,
                            ),
                            "write_surfaces": "disabled",
                            **({"manual_refresh": True} if refresh_callback is not None else {}),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    file=output,
                    flush=True,
                )
                print(
                    (
                        "Press Ctrl-C to stop. Manual refresh is enabled; "
                        "there is no automatic collection schedule."
                        if refresh_callback is not None else
                        "Press Ctrl-C to stop; the published Opportunity data "
                        "will remain unchanged."
                    ),
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
                    dashboard_server.clear_runtime_caches()


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = parse_args(argv)
    try:
        serve_current_dashboard(
            data_dir=arguments.data_dir,
            port=arguments.port,
        )
    except KeyboardInterrupt:
        print(
            "Current Opportunity dashboard interrupted before startup; "
            "cleanup complete.",
            file=sys.stderr,
            flush=True,
        )
        return 130
    except Exception as error:
        print(
            "Current Opportunity dashboard could not start: {}: {}".format(
                type(error).__name__, error
            ),
            file=sys.stderr,
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
