"""Validate reviewed CSVs and atomically publish the runtime SQLite database."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

if __package__:
    from .fact_quality import build_report, build_retry_windows
    from .market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS, build_database
else:
    from fact_quality import build_report, build_retry_windows
    from market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS, build_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_DIR = PROJECT_ROOT / "data/local"
QUALITY_DIRECTORY = "quality"
DAILY_QUALITY_FILENAME = "daily-latest.json"
REJECTED_QUALITY_DIRECTORY = "rejected"
REJECTED_QUALITY_FILENAME = "report.json"
REJECTED_LATEST_FILENAME = "latest.json"
REJECTED_REPORT_SCHEMA = "fact_quality_rejection/v1"
REJECTED_POINTER_SCHEMA = "fact_quality_rejection_pointer/v1"
FILES = {
    "cex_exchange_volume_daily.csv": set(CEX_COLUMNS),
    "dex_pool_volume_daily.csv": set(DEX_COLUMNS),
}
ATTEMPT_FILES = {
    "cex": "cex_daily_collection_attempts.json",
    "dex": "dex_daily_collection_attempts.json",
}


def validate_csv(path: Path, required_columns: Set[str]) -> int:
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


def _issue_queue_entry(issue: Mapping[str, Any]) -> Dict[str, Any]:
    market = issue.get("market") or {}
    return {
        "issue_id": issue.get("issue_id"),
        "token_symbol": market.get("token_symbol"),
        "market_id": market.get("market_id"),
        "market_type": market.get("market_type"),
        "date": issue.get("date"),
        "status": issue.get("status"),
        "reason_code": issue.get("reason_code"),
        "retryable": issue.get("retryable"),
        "source_url_hints": list(issue.get("source_url_hints") or []),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _persist_rejected_quality_report(
    report: Dict[str, Any],
    *,
    source_dir: Path,
    staging_dir: Path,
    target_dir: Path,
) -> Path:
    rejected_at = datetime.now(timezone.utc)
    rejection_id = "{}-{}".format(
        rejected_at.strftime("%Y%m%dT%H%M%S%fZ"),
        uuid.uuid4().hex[:12],
    )
    logical_bundle = "data/local/{}/{}/{}".format(
        QUALITY_DIRECTORY,
        REJECTED_QUALITY_DIRECTORY,
        rejection_id,
    )
    source_names = {
        "cex": "cex_exchange_volume_daily.csv",
        "dex": "dex_pool_volume_daily.csv",
    }
    for source in report["sources"]:
        filename = source_names[str(source["market_type"])]
        source["name"] = filename
        source["candidate_source_path"] = str((source_dir / filename).resolve())
        source["path"] = "{}/{}".format(logical_bundle, filename)

    report["rejection"] = {
        "schema": REJECTED_REPORT_SCHEMA,
        "rejection_id": rejection_id,
        "status": "rejected_hard_invalid",
        "reason_code": "hard_invalid_candidate",
        "rejected_at_utc": rejected_at.isoformat(),
        "published_snapshot_unchanged": True,
        "published_daily_quality_unchanged": True,
        "candidate_source_directory": str(source_dir),
        "evidence_files": [
            "{}/{}".format(logical_bundle, filename)
            for filename in FILES
        ],
    }
    report["publication"] = {
        "status": "blocked_hard_invalid",
        "published_snapshot_unchanged": True,
        "published_daily_quality_unchanged": True,
    }

    staged_bundle = staging_dir / ".rejected-quality-{}".format(rejection_id)
    staged_bundle.mkdir()
    for filename in FILES:
        shutil.copyfile(staging_dir / filename, staged_bundle / filename)
    staged_report = staged_bundle / REJECTED_QUALITY_FILENAME
    _write_json(staged_report, report)

    rejected_root = (
        target_dir
        / QUALITY_DIRECTORY
        / REJECTED_QUALITY_DIRECTORY
    )
    rejected_root.mkdir(parents=True, exist_ok=True)
    target_bundle = rejected_root / rejection_id
    staged_bundle.replace(target_bundle)
    target_report = target_bundle / REJECTED_QUALITY_FILENAME

    pointer = {
        "schema": REJECTED_POINTER_SCHEMA,
        "rejection_id": rejection_id,
        "rejected_at_utc": rejected_at.isoformat(),
        "report": "{}/{}".format(
            rejection_id,
            REJECTED_QUALITY_FILENAME,
        ),
        "report_sha256": _sha256_file(target_report),
        "hard_invalid_count": report["summary"]["hard_invalid_count"],
    }
    pointer_target = rejected_root / REJECTED_LATEST_FILENAME
    pointer_temporary = rejected_root / ".{}.{}.tmp".format(
        REJECTED_LATEST_FILENAME,
        uuid.uuid4().hex,
    )
    try:
        _write_json(pointer_temporary, pointer)
        pointer_temporary.replace(pointer_target)
    finally:
        pointer_temporary.unlink(missing_ok=True)
    return target_report


def _prepare_daily_quality_report(
    report: Dict[str, Any],
    database_result: Mapping[str, Any],
) -> Dict[str, Any]:
    historical_issues = [
        issue
        for issue in report["issues"]
        if issue.get("category") == "historical_gap"
    ]
    d1_issues = [
        issue
        for issue in report["issues"]
        if issue.get("category") == "d1_active_gap"
    ]

    backfill_pending = [_issue_queue_entry(issue) for issue in historical_issues]
    retry_queue: List[Dict[str, Any]] = []
    for issue in d1_issues:
        if issue.get("retryable") is not True:
            continue
        queue_entry = _issue_queue_entry(issue)
        queue_entry.update(
            {
                "queue_id": "daily-retry-{}".format(issue["issue_id"]),
                "queue_status": "pending",
                "action": "retry_daily_market_window",
            }
        )
        retry_queue.append(queue_entry)

    for source in report["sources"]:
        filename = (
            "cex_exchange_volume_daily.csv"
            if source.get("market_type") == "cex"
            else "dex_pool_volume_daily.csv"
        )
        source["name"] = filename
        source["path"] = "data/local/{}".format(filename)

    report["backfill_pending"] = backfill_pending
    report["backfill_windows_by_token"] = build_retry_windows(historical_issues)
    report["retry_queue"] = retry_queue
    report["retry_windows_by_token"] = build_retry_windows(d1_issues)
    report["summary"]["backfill_pending_count"] = len(backfill_pending)
    report["summary"]["retry_queue_count"] = len(retry_queue)

    if retry_queue:
        publication_status = "published_with_retry_queue"
    elif backfill_pending:
        publication_status = "published_with_backfill"
    else:
        publication_status = "published"
    report["publication"] = {
        "status": publication_status,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_snapshot_id": database_result["snapshot_id"],
        "import_run_id": database_result["import_run_id"],
        "runtime_commit_point": "data/local/{}".format(DATABASE_FILENAME),
        "quality_report": "data/local/{}/{}".format(
            QUALITY_DIRECTORY,
            DAILY_QUALITY_FILENAME,
        ),
    }
    return report


def import_snapshot(
    source_dir: Path,
    target_dir: Path = LOCAL_DATA_DIR,
    *,
    quality_today: Optional[date] = None,
) -> Dict[str, int]:
    source_dir = source_dir.expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    counts: Dict[str, int] = {}
    staging_dir = target_dir / f".snapshot-{uuid.uuid4().hex}"
    staging_dir.mkdir()
    try:
        for filename, required_columns in FILES.items():
            source = source_dir / filename
            if not source.exists():
                raise FileNotFoundError(f"Missing snapshot file: {source}")
            counts[filename] = validate_csv(source, required_columns)
            shutil.copyfile(source, staging_dir / filename)
        staged_attempts: Dict[str, Optional[Path]] = {}
        for market_type, filename in ATTEMPT_FILES.items():
            source = source_dir / filename
            if source.exists():
                staged = staging_dir / filename
                shutil.copyfile(source, staged)
                staged_attempts[market_type] = staged
            else:
                staged_attempts[market_type] = None

        quality_report = build_report(
            staging_dir / "cex_exchange_volume_daily.csv",
            staging_dir / "dex_pool_volume_daily.csv",
            cex_attempts=staged_attempts["cex"],
            dex_attempts=staged_attempts["dex"],
            today=quality_today,
        )
        hard_invalid_count = quality_report["summary"]["hard_invalid_count"]
        if hard_invalid_count:
            rejected_report = _persist_rejected_quality_report(
                quality_report,
                source_dir=source_dir,
                staging_dir=staging_dir,
                target_dir=target_dir,
            )
            raise ValueError(
                "Snapshot publication blocked by {} hard-invalid daily fact "
                "issue(s); inspect the rejected audit at {} before "
                "retrying.".format(
                    hard_invalid_count,
                    rejected_report,
                )
            )

        staged_database = staging_dir / DATABASE_FILENAME
        database_result = build_database(
            staging_dir,
            staged_database,
            previous_database=target_dir / DATABASE_FILENAME,
        )
        _prepare_daily_quality_report(quality_report, database_result)
        staged_quality_dir = staging_dir / QUALITY_DIRECTORY
        staged_quality_dir.mkdir()
        staged_quality_report = staged_quality_dir / DAILY_QUALITY_FILENAME
        _write_json(staged_quality_report, quality_report)

        target_quality_dir = target_dir / QUALITY_DIRECTORY
        target_quality_dir.mkdir(parents=True, exist_ok=True)
        supporting_publications = [
            (staging_dir / filename, target_dir / filename)
            for filename in FILES
        ]
        supporting_publications.append(
            (
                staged_quality_report,
                target_quality_dir / DAILY_QUALITY_FILENAME,
            )
        )
        rollback_dir = staging_dir / ".rollback"
        rollback_dir.mkdir()
        rollback_entries = []
        for index, (_candidate, target) in enumerate(supporting_publications):
            backup = rollback_dir / "{}-{}".format(index, target.name)
            if target.exists():
                shutil.copyfile(target, backup)
                rollback_entries.append((target, backup))
            else:
                rollback_entries.append((target, None))
        try:
            for candidate, target in supporting_publications:
                candidate.replace(target)
            # SQLite is the server-visible commit point. Publish it only after
            # the supporting CSV copies and quality evidence are in place.
            staged_database.replace(target_dir / DATABASE_FILENAME)
        except BaseException:
            rollback_errors = []
            for target, backup in reversed(rollback_entries):
                try:
                    if backup is None:
                        target.unlink(missing_ok=True)
                    else:
                        backup.replace(target)
                except OSError as rollback_error:
                    rollback_errors.append(
                        "{}: {}".format(target, rollback_error)
                    )
            if rollback_errors:
                raise RuntimeError(
                    "Snapshot publication failed and supporting-file rollback "
                    "was incomplete: {}".format("; ".join(rollback_errors))
                )
            raise
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
