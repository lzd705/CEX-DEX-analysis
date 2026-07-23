"""Validate and atomically import detailed market CSVs into data/local."""

from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_DIR = PROJECT_ROOT / "data/local"
FILES = {
    "cex_exchange_volume_daily.csv": {
        "date",
        "token_symbol",
        "exchange",
        "cex_symbol",
        "close",
        "quote_volume_usd",
    },
    "dex_pool_volume_daily.csv": {
        "date",
        "token_symbol",
        "chain",
        "dex",
        "pool_address",
        "pool_name",
        "close",
        "dex_volume_usd",
        "pool_tvl_usd",
    },
}


def validate_csv(path: Path, required_columns: set[str]) -> int:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = sorted(required_columns - columns)
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(missing)}")
        count = sum(1 for row in reader if row.get("date") and row.get("token_symbol"))
    if count == 0:
        raise ValueError(f"{path.name} has no dated token rows")
    return count


def import_snapshot(source_dir: Path, target_dir: Path = LOCAL_DATA_DIR) -> dict[str, int]:
    source_dir = source_dir.expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    staged = []
    for filename, required_columns in FILES.items():
        source = source_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"Missing snapshot file: {source}")
        counts[filename] = validate_csv(source, required_columns)
        temporary = target_dir / f".{filename}.tmp"
        shutil.copyfile(source, temporary)
        staged.append((temporary, target_dir / filename))
    for temporary, target in staged:
        temporary.replace(target)
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import a local CEX/DEX fact snapshot")
    parser.add_argument("source_dir", type=Path)
    return parser.parse_args()


def main() -> None:
    counts = import_snapshot(parse_args().source_dir)
    for filename, row_count in counts.items():
        print(f"Imported {filename}: {row_count} rows")


if __name__ == "__main__":
    main()
