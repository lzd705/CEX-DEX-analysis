"""Build and inspect the SQLite market-facts database from reviewed CSV inputs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "data/schema/001_market_facts.sql"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data/local"
DATABASE_FILENAME = "market_facts.sqlite3"
CEX_FILENAME = "cex_exchange_volume_daily.csv"
DEX_FILENAME = "dex_pool_volume_daily.csv"
SCHEMA_VERSION = 1

CEX_COLUMNS = [
    "date",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
]
DEX_COLUMNS = [
    "date",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "open",
    "high",
    "low",
    "close",
    "dex_volume_usd",
    "pool_tvl_usd",
]
NUMERIC_COLUMNS = {
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
    "dex_volume_usd",
    "pool_tvl_usd",
}


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_number(value: str | None, *, field: str, row_number: int) -> float | None:
    if value is None or value.strip() == "":
        return None
    try:
        number = float(value)
    except ValueError as error:
        raise ValueError(f"Invalid {field} at CSV row {row_number}: {value}") from error
    if not math.isfinite(number):
        raise ValueError(f"Non-finite {field} at CSV row {row_number}: {value}")
    return number


def required_text(row: dict[str, str], field: str, row_number: int) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"Missing {field} at CSV row {row_number}")
    return value


def normalized_row(
    row: dict[str, str],
    columns: list[str],
    *,
    row_number: int,
) -> tuple[Any, ...]:
    values = []
    for field in columns:
        if field in NUMERIC_COLUMNS:
            values.append(parse_number(row.get(field), field=field, row_number=row_number))
        else:
            value = required_text(row, field, row_number)
            if field == "date":
                value = date.fromisoformat(value).isoformat()
            elif field == "token_symbol":
                value = value.upper()
            values.append(value)
    return tuple(values)


def iter_normalized_rows(path: Path, columns: list[str]) -> Iterable[tuple[Any, ...]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(columns) - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(missing)}")
        for row_number, row in enumerate(reader, start=2):
            yield normalized_row(row, columns, row_number=row_number)


def read_history(database_path: Path) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    if not database_path.exists():
        return [], []
    try:
        connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
        snapshots = connection.execute(
            """
            SELECT snapshot_id, created_at, available_start, available_end,
                   token_count, cex_row_count, dex_row_count,
                   cex_source_name, dex_source_name,
                   cex_source_bytes, dex_source_bytes,
                   cex_sha256, dex_sha256
            FROM dataset_snapshots
            ORDER BY created_at
            """
        ).fetchall()
        runs = connection.execute(
            """
            SELECT run_id, snapshot_id, imported_at, source_directory, status
            FROM import_runs
            ORDER BY imported_at
            """
        ).fetchall()
    except sqlite3.Error:
        return [], []
    finally:
        if "connection" in locals():
            connection.close()
    return snapshots, runs


def build_database(
    source_dir: Path,
    target_database: Path | None = None,
    *,
    previous_database: Path | None = None,
) -> dict[str, Any]:
    """Build a complete database in a temporary file, validate it, then publish."""
    source_dir = source_dir.expanduser().resolve()
    target_database = (target_database or (source_dir / DATABASE_FILENAME)).expanduser().resolve()
    previous_database = (previous_database or target_database).expanduser().resolve()
    cex_path = source_dir / CEX_FILENAME
    dex_path = source_dir / DEX_FILENAME
    if not cex_path.exists() or not dex_path.exists():
        raise FileNotFoundError(f"Expected {CEX_FILENAME} and {DEX_FILENAME} in {source_dir}")

    target_database.parent.mkdir(parents=True, exist_ok=True)
    temporary = target_database.with_name(f".{target_database.name}.{uuid.uuid4().hex}.tmp")
    prior_snapshots, prior_runs = read_history(previous_database)
    imported_at = utc_now_text()
    cex_sha = sha256_file(cex_path)
    dex_sha = sha256_file(dex_path)
    snapshot_id = hashlib.sha256(f"{cex_sha}:{dex_sha}".encode("ascii")).hexdigest()[:24]
    run_id = uuid.uuid4().hex

    connection = sqlite3.connect(temporary)
    try:
        connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, imported_at),
        )
        connection.executemany(
            """
            INSERT OR IGNORE INTO dataset_snapshots
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prior_snapshots,
        )
        connection.executemany(
            "INSERT OR IGNORE INTO import_runs VALUES (?, ?, ?, ?, ?)",
            prior_runs,
        )

        cex_rows = list(iter_normalized_rows(cex_path, CEX_COLUMNS))
        dex_rows = list(iter_normalized_rows(dex_path, DEX_COLUMNS))
        token_dates: dict[str, list[str]] = {}
        for rows in (cex_rows, dex_rows):
            for row in rows:
                token_dates.setdefault(row[1], []).append(row[0])
        if not token_dates:
            raise ValueError("The reviewed snapshot contains no Token rows")
        connection.executemany(
            "INSERT INTO tokens (token_symbol, first_observed_date, last_observed_date) VALUES (?, ?, ?)",
            (
                (token, min(dates), max(dates))
                for token, dates in sorted(token_dates.items())
            ),
        )

        cex_placeholders = ", ".join("?" for _ in CEX_COLUMNS)
        dex_placeholders = ", ".join("?" for _ in DEX_COLUMNS)
        connection.executemany(
            f"INSERT INTO cex_market_daily ({', '.join(CEX_COLUMNS)}) VALUES ({cex_placeholders})",
            cex_rows,
        )
        connection.executemany(
            f"INSERT INTO dex_pool_daily ({', '.join(DEX_COLUMNS)}) VALUES ({dex_placeholders})",
            dex_rows,
        )
        available_start = min(min(row[0] for row in cex_rows), min(row[0] for row in dex_rows))
        available_end = max(max(row[0] for row in cex_rows), max(row[0] for row in dex_rows))
        snapshot_row = (
            snapshot_id,
            imported_at,
            available_start,
            available_end,
            len(token_dates),
            len(cex_rows),
            len(dex_rows),
            cex_path.name,
            dex_path.name,
            cex_path.stat().st_size,
            dex_path.stat().st_size,
            cex_sha,
            dex_sha,
        )
        connection.execute(
            "INSERT OR IGNORE INTO dataset_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            snapshot_row,
        )
        connection.execute(
            "INSERT INTO import_runs VALUES (?, ?, ?, ?, 'published')",
            (run_id, snapshot_id, imported_at, str(source_dir)),
        )
        connection.execute(
            "INSERT INTO dataset_state VALUES (1, ?, ?)",
            (snapshot_id, run_id),
        )
        connection.execute("ANALYZE")
        connection.commit()

        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise ValueError(f"SQLite integrity check failed: {integrity}")
        actual_cex = connection.execute("SELECT COUNT(*) FROM cex_market_daily").fetchone()[0]
        actual_dex = connection.execute("SELECT COUNT(*) FROM dex_pool_daily").fetchone()[0]
        if actual_cex != len(cex_rows) or actual_dex != len(dex_rows):
            raise ValueError("Database row counts do not match the reviewed CSV snapshot")
    except Exception:
        connection.close()
        temporary.unlink(missing_ok=True)
        raise
    else:
        connection.close()
        os.replace(temporary, target_database)

    return {
        "database": str(target_database),
        "snapshot_id": snapshot_id,
        "import_run_id": run_id,
        "token_count": len(token_dates),
        "cex_row_count": len(cex_rows),
        "dex_row_count": len(dex_rows),
        "available_start": available_start,
        "available_end": available_end,
    }


def database_status(database_path: Path) -> dict[str, Any]:
    database_path = database_path.expanduser().resolve()
    connection = sqlite3.connect(f"{database_path.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT s.*, r.run_id, r.imported_at
            FROM dataset_state state
            JOIN dataset_snapshots s ON s.snapshot_id = state.snapshot_id
            JOIN import_runs r ON r.run_id = state.import_run_id
            WHERE state.singleton_id = 1
            """
        ).fetchone()
        if row is None:
            raise ValueError("Database does not contain a published dataset state")
        return dict(row)
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build or inspect the market-facts SQLite database")
    parser.add_argument("source_dir", type=Path, nargs="?", default=DEFAULT_DATA_DIR)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--status", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_path = args.database or (args.source_dir / DATABASE_FILENAME)
    result = database_status(database_path) if args.status else build_database(args.source_dir, database_path)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
