"""Run only the source-backed CEX and DEX data collectors."""

from __future__ import annotations

import argparse

if __package__:
    from . import fetch_cex, fetch_dex
else:
    import fetch_cex
    import fetch_dex


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
        "--append-dex",
        action="store_true",
        help="Replace selected DEX tokens while preserving other local rows",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tokens = parse_list(args.tokens, upper=True)
    exchanges = parse_list(args.exchanges, upper=False)

    if not args.dex_only:
        fetch_cex.main(token_symbols=tokens, exchanges=exchanges)
    if not args.cex_only:
        fetch_dex.main(token_symbols=tokens, append=args.append_dex)


if __name__ == "__main__":
    main()
