#!/usr/bin/env python3
"""Fail non-zero unless the local Market Monitor reports usable fact data."""

from __future__ import annotations

import argparse
import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check the Market Monitor /health endpoint")
    parser.add_argument("--url", default="http://127.0.0.1:8765/health")
    parser.add_argument("--timeout", type=float, default=5.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        with urlopen(args.url, timeout=args.timeout) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        raise SystemExit(f"health check failed: {error}") from error
    if (
        response.status != 200
        or payload.get("status") != "ok"
        or payload.get("data_ready") is not True
    ):
        raise SystemExit(f"health check failed: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
