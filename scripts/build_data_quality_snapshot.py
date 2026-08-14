#!/usr/bin/env python3
"""Command-line entry point for the observed data-quality snapshot."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

try:
    from scripts.data_quality_snapshot import build_snapshot, write_snapshot
except ModuleNotFoundError:  # Direct ``python scripts/...`` execution.
    from data_quality_snapshot import build_snapshot, write_snapshot


def _canonical_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected canonical YYYY-MM-DD")
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("expected canonical YYYY-MM-DD")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic observed data-quality snapshot"
    )
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--generated-at-utc", required=True)
    parser.add_argument("--window-end", required=True, type=_canonical_date)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--application-sha", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/public/quality/latest.json"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        snapshot = build_snapshot(
            args.data_dir,
            args.generated_at_utc,
            args.window_end,
            args.window_days,
            args.application_sha,
        )
        write_snapshot(args.output, snapshot)
    except (OSError, ValueError) as error:
        raise SystemExit(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

