"""Validate reviewed CSVs and atomically publish the runtime SQLite database."""

from __future__ import annotations

import argparse
import csv
import shutil
import uuid
from pathlib import Path

if __package__:
    from .market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS, build_database
else:
    from market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS, build_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_DIR = PROJECT_ROOT / "data/local"
FILES = {
    "cex_exchange_volume_daily.csv": set(CEX_COLUMNS),
    "dex_pool_volume_daily.csv": set(DEX_COLUMNS),
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
    staging_dir = target_dir / f".snapshot-{uuid.uuid4().hex}"
    staging_dir.mkdir()
    try:
        for filename, required_columns in FILES.items():
            source = source_dir / filename
            if not source.exists():
                raise FileNotFoundError(f"Missing snapshot file: {source}")
            counts[filename] = validate_csv(source, required_columns)
            shutil.copyfile(source, staging_dir / filename)

        staged_database = staging_dir / DATABASE_FILENAME
        build_database(
            staging_dir,
            staged_database,
            previous_database=target_dir / DATABASE_FILENAME,
        )
        for filename in FILES:
            (staging_dir / filename).replace(target_dir / filename)
        staged_database.replace(target_dir / DATABASE_FILENAME)
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
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
