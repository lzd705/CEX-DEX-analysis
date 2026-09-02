#!/usr/bin/env python3
"""Serve a disposable, repository-only historical Opportunity demo."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, Sequence, TextIO


if __package__ in {None, ""}:  # pragma: no cover - direct script bootstrap
    _PROJECT_ROOT_TEXT = str(Path(__file__).resolve().parents[1])
    if _PROJECT_ROOT_TEXT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT_TEXT)

from dashboard import server as dashboard_server
from tests.historical_replay_fixture import PublishedHistoricalReplayFixture


DEMO_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
HISTORICAL_DEMO_PATH = "/opportunities?opportunity_scope=historical"


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

    prior_route_root = os.environ.get("MARKET_ROUTE_DATA_DIR")
    had_route_root = "MARKET_ROUTE_DATA_DIR" in os.environ
    fixture = None
    http_server = None
    try:
        print(
            "Preparing LOCAL DEMO FIXTURE (repository-only; no external RPC)...",
            file=output,
            flush=True,
        )
        fixture = PublishedHistoricalReplayFixture()
        os.environ["MARKET_ROUTE_DATA_DIR"] = str(
            fixture.historical_root.parent.resolve()
        )
        dashboard_server.clear_runtime_caches()
        http_server = dashboard_server.ThreadingHTTPServer(
            (DEMO_HOST, port), dashboard_server.MarketMonitorHandler
        )
        bound_port = int(http_server.server_address[1])
        print(
            json.dumps(
                {
                    "demo_fixture": True,
                    "network_scope": "loopback_only",
                    "replay_id": fixture.pointer["replay_id"],
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
            "LOCAL DEMO FIXTURE: not live, not current, and not an execution "
            "or profit claim.",
            file=output,
            flush=True,
        )
        print("Press Ctrl-C to stop and clean up.", file=output, flush=True)
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
                if had_route_root:
                    os.environ["MARKET_ROUTE_DATA_DIR"] = prior_route_root or ""
                else:
                    os.environ.pop("MARKET_ROUTE_DATA_DIR", None)
                if fixture is not None:
                    fixture.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    arguments = parse_args(argv)
    serve_demo(port=arguments.port)


if __name__ == "__main__":
    main()
