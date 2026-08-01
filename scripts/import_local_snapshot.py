"""Validate reviewed CSVs and atomically publish the runtime SQLite database."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import uuid
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

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
EXACT_IDENTITY_QUARANTINE_FILENAME = "cex-exact-identity-quarantine.json"
EXACT_IDENTITY_QUARANTINE_SCHEMA = "cex_exact_identity_quarantine/v1"
EXACT_IDENTITY_QUARANTINE_ARCHIVE_PREFIX = (
    "cex-exact-identity-quarantine-"
)
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
QUARANTINE_DISPOSITIONS = {
    "same_date_exact_present",
    "alias_only_no_exact_observation",
}
EXACT_IDENTITY_QUARANTINE_EXCHANGES = (
    "upbit",
    "coinbase",
    "kraken",
)


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


def exact_identity_quarantine_rows_sha256(
    rows: Sequence[Mapping[str, Any]],
) -> str:
    encoded = json.dumps(
        rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_exact_identity_quarantine(
    path: Path,
    *,
    candidate_cex_path: Path,
) -> Dict[str, Any]:
    """Validate the durable audit for rows retired by exact-ID migration."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_fields = {
        "schema",
        "status",
        "reason_code",
        "created_at_utc",
        "scope",
        "baseline_cex_sha256",
        "candidate_cex_sha256",
        "configured_market_set_sha256",
        "alias_row_count",
        "same_date_exact_present_count",
        "alias_only_quarantined_count",
        "token_counts",
        "exchange_counts",
        "rows_sha256",
        "rows",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise ValueError("exact-identity quarantine fields are invalid")
    if payload.get("schema") != EXACT_IDENTITY_QUARANTINE_SCHEMA:
        raise ValueError("exact-identity quarantine schema is unsupported")
    if payload.get("status") != "retired_aliases_quarantined":
        raise ValueError("exact-identity quarantine status is invalid")
    if payload.get("reason_code") != "retired_cex_quote_identity_alias":
        raise ValueError("exact-identity quarantine reason is invalid")
    for field in (
        "baseline_cex_sha256",
        "candidate_cex_sha256",
        "configured_market_set_sha256",
        "rows_sha256",
    ):
        value = payload.get(field)
        if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-f]{64}", value
        ):
            raise ValueError("exact-identity quarantine hash is invalid")
    if payload["candidate_cex_sha256"] != _sha256_file(candidate_cex_path):
        raise ValueError("exact-identity quarantine candidate hash does not match")
    created_text = payload.get("created_at_utc")
    if not isinstance(created_text, str) or not created_text:
        raise ValueError("exact-identity quarantine timestamp is missing")
    parsed_created = datetime.fromisoformat(
        created_text[:-1] + "+00:00"
        if created_text.endswith("Z")
        else created_text
    )
    if parsed_created.tzinfo is None or parsed_created.utcoffset() is None:
        raise ValueError("exact-identity quarantine timestamp is not aware")

    scope = payload.get("scope")
    if not isinstance(scope, dict) or set(scope) != {
        "start_date",
        "end_date",
        "tokens",
        "exchanges",
        "remove_legacy_upbit_krw_fallback",
    }:
        raise ValueError("exact-identity quarantine scope is invalid")
    start_text = scope.get("start_date")
    end_text = scope.get("end_date")
    if not isinstance(start_text, str) or not isinstance(end_text, str):
        raise ValueError("exact-identity quarantine date scope is invalid")
    start_day = date.fromisoformat(start_text)
    end_day = date.fromisoformat(end_text)
    if (
        start_text != start_day.isoformat()
        or end_text != end_day.isoformat()
        or end_day < start_day
    ):
        raise ValueError("exact-identity quarantine date scope is invalid")
    tokens = scope.get("tokens")
    exchanges = scope.get("exchanges")
    remove_upbit_alias = scope.get(
        "remove_legacy_upbit_krw_fallback"
    )
    if (
        not isinstance(tokens, list)
        or not tokens
        or any(
            not isinstance(token, str) or token != token.strip().upper()
            for token in tokens
        )
        or len(tokens) != len(set(tokens))
        or not isinstance(exchanges, list)
        or not exchanges
        or any(
            not isinstance(exchange, str)
            or exchange not in EXACT_IDENTITY_QUARANTINE_EXCHANGES
            for exchange in exchanges
        )
        or exchanges != [
            exchange
            for exchange in EXACT_IDENTITY_QUARANTINE_EXCHANGES
            if exchange in exchanges
        ]
        or len(exchanges) != len(set(exchanges))
        or type(remove_upbit_alias) is not bool
        or (remove_upbit_alias and "upbit" not in exchanges)
    ):
        raise ValueError("exact-identity quarantine market scope is invalid")

    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ValueError("exact-identity quarantine rows are missing")
    if payload.get(
        "rows_sha256"
    ) != exact_identity_quarantine_rows_sha256(rows):
        raise ValueError("exact-identity quarantine row hash does not match")
    if payload.get("alias_row_count") != len(rows):
        raise ValueError("exact-identity quarantine row count does not match")
    candidate_exact_dates = set()
    with candidate_cex_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            candidate_exact_dates.add(
                (
                    str(row.get("date") or ""),
                    str(row.get("token_symbol") or "").strip().upper(),
                    str(row.get("exchange") or "").strip().lower(),
                    str(row.get("cex_symbol") or "").strip().upper(),
                )
            )
    natural_keys = set()
    disposition_counts = Counter()
    token_counts = Counter()
    exchange_counts = Counter()
    for item in rows:
        if not isinstance(item, dict) or set(item) != {
            "disposition",
            "expected_instrument",
            "row",
        }:
            raise ValueError(
                "exact-identity quarantine row envelope is invalid"
            )
        disposition = item.get("disposition")
        if disposition not in QUARANTINE_DISPOSITIONS:
            raise ValueError(
                "exact-identity quarantine disposition is invalid"
            )
        row = item.get("row")
        if not isinstance(row, dict) or set(row) != set(CEX_COLUMNS):
            raise ValueError("exact-identity quarantine CEX row is invalid")
        day_text = row.get("date")
        token = str(row.get("token_symbol") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip().lower()
        instrument = str(row.get("cex_symbol") or "").strip().upper()
        expected_instrument = item.get("expected_instrument")
        valid_identity = (
            exchange == "upbit"
            and exchange in exchanges
            and remove_upbit_alias
            and instrument == "{}/KRW".format(token)
            and expected_instrument == "{}/USDT".format(token)
        ) or (
            exchange in {"coinbase", "kraken"}
            and exchange in exchanges
            and instrument == "{}/USDT".format(token)
            and expected_instrument == "{}/USD".format(token)
        )
        if (
            not isinstance(day_text, str)
            or not start_text <= day_text <= end_text
            or token not in tokens
            or not valid_identity
        ):
            raise ValueError("exact-identity quarantine alias identity is invalid")
        natural_key = (day_text, token, exchange, instrument)
        if natural_key in natural_keys:
            raise ValueError("exact-identity quarantine contains duplicate rows")
        natural_keys.add(natural_key)
        exact_key = (day_text, token, exchange, expected_instrument)
        exact_exists = exact_key in candidate_exact_dates
        if exact_exists != (disposition == "same_date_exact_present"):
            raise ValueError(
                "exact-identity quarantine disposition is inconsistent"
            )
        disposition_counts[disposition] += 1
        token_counts[token] += 1
        exchange_counts[exchange] += 1
    if payload.get("same_date_exact_present_count") != disposition_counts[
        "same_date_exact_present"
    ] or payload.get("alias_only_quarantined_count") != disposition_counts[
        "alias_only_no_exact_observation"
    ]:
        raise ValueError("exact-identity quarantine disposition counts do not match")
    if payload.get("token_counts") != dict(sorted(token_counts.items())):
        raise ValueError("exact-identity quarantine Token counts do not match")
    if payload.get("exchange_counts") != dict(sorted(exchange_counts.items())):
        raise ValueError("exact-identity quarantine exchange counts do not match")
    return payload


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
    retryable_historical_issues = [
        issue
        for issue in historical_issues
        if issue.get("retryable") is True
    ]
    d1_issues = [
        issue
        for issue in report["issues"]
        if issue.get("category") == "d1_active_gap"
    ]

    backfill_pending = [
        _issue_queue_entry(issue)
        for issue in retryable_historical_issues
    ]
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
        source_quarantine = (
            source_dir
            / QUALITY_DIRECTORY
            / EXACT_IDENTITY_QUARANTINE_FILENAME
        )
        quarantine_payload = None
        if source_quarantine.exists():
            quarantine_payload = validate_exact_identity_quarantine(
                source_quarantine,
                candidate_cex_path=(
                    staging_dir / "cex_exchange_volume_daily.csv"
                ),
            )

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
        staged_quarantine = None
        staged_quarantine_archive = None
        if quarantine_payload is not None:
            staged_quarantine = (
                staged_quality_dir
                / EXACT_IDENTITY_QUARANTINE_FILENAME
            )
            _write_json(staged_quarantine, quarantine_payload)
            staged_quarantine_archive = staged_quality_dir / (
                "{}{}.json".format(
                    EXACT_IDENTITY_QUARANTINE_ARCHIVE_PREFIX,
                    _sha256_file(staged_quarantine),
                )
            )
            shutil.copyfile(
                staged_quarantine,
                staged_quarantine_archive,
            )

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
        if staged_quarantine is not None:
            supporting_publications.append(
                (
                    staged_quarantine_archive,
                    target_quality_dir
                    / staged_quarantine_archive.name,
                )
            )
            supporting_publications.append(
                (
                    staged_quarantine,
                    target_quality_dir
                    / EXACT_IDENTITY_QUARANTINE_FILENAME,
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
