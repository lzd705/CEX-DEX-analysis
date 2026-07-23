"""Run only the source-backed CEX and DEX data collectors."""

from __future__ import annotations

import argparse
import shutil
from datetime import date
from pathlib import Path

if __package__:
    from . import fetch_cex, fetch_dex
    from .import_local_snapshot import import_snapshot
else:
    import fetch_cex
    import fetch_dex
    from import_local_snapshot import import_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data/processed"
LOCAL_DIR = PROJECT_ROOT / "data/local"
DETAILED_FILES = [
    "cex_exchange_volume_daily.csv",
    "dex_pool_volume_daily.csv",
]


def parse_list(value: str | None, *, upper: bool) -> list[str] | None:
    if not value:
        return None
    transform = str.upper if upper else str.lower
    values = [transform(item.strip()) for item in value.split(",") if item.strip()]
    return values or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh fact-only market data")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cex-only", action="store_true")
    mode.add_argument("--dex-only", action="store_true")
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--exchanges", help="Comma-separated CEX names")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Upsert selected Token dates while preserving the current snapshot",
    )
    parser.add_argument("--start", help="Inclusive UTC date")
    parser.add_argument("--end", help="Inclusive UTC date")
    parser.add_argument("--publish-local", action="store_true")
    return parser.parse_args()


def resolve_limit_days(start: str | None, end: str | None) -> int:
    if not start and not end:
        return 180
    if not start or not end:
        raise ValueError("--start and --end must be provided together")
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    days = (end_date - start_date).days + 1
    if days < 1 or days > 180:
        raise ValueError("Refresh window must contain between 1 and 180 days")
    return min(180, days + 3)


def seed_processed_from_local() -> None:
    """Start an incremental refresh from the currently published snapshot."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for filename in DETAILED_FILES:
        source = LOCAL_DIR / filename
        target = PROCESSED_DIR / filename
        if not source.exists():
            raise FileNotFoundError(f"Cannot append without current local snapshot: {source}")
        shutil.copyfile(source, target)


def main() -> None:
    args = parse_args()
    tokens = parse_list(args.tokens, upper=True)
    exchanges = parse_list(args.exchanges, upper=False)
    limit_days = resolve_limit_days(args.start, args.end)

    if args.append:
        if tokens is None:
            raise ValueError("--append requires --tokens")
        seed_processed_from_local()

    if not args.dex_only:
        fetch_cex.main(
            token_symbols=tokens,
            exchanges=exchanges,
            append=args.append,
            start_date=args.start,
            end_date=args.end,
            limit_days=limit_days,
        )
    if not args.cex_only:
        fetch_dex.main(
            token_symbols=tokens,
            append=args.append,
            start_date=args.start,
            end_date=args.end,
            limit_days=limit_days,
        )
    if args.publish_local:
        import_snapshot(PROCESSED_DIR)


if __name__ == "__main__":
    main()
