"""Safely execute exact historical-gap windows from the live quality report.

This is an internal operator tool, not a public write API.  It deliberately:

* accepts no Token or date from the operator;
* reads only ``backfill_windows_by_token`` from the current published report;
* validates that report against the SQLite publication commit point;
* holds the shared collection lock for the whole batch;
* invokes only ``run_fact_pipeline.py`` (never TVL or depth collectors);
* reloads the current report after every window; and
* stops on collector failure, unchanged publication lineage, or no issue
  progress.

The default is one window.  Larger batches remain bounded and are still
strictly sequential.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data/local"
DATABASE_FILENAME = "market_facts.sqlite3"
QUALITY_REPORT_PATH = Path("quality/daily-latest.json")
QUALITY_REPORT_SCHEMA = "fact_quality_report/v1"
STATE_SCHEMA = "exact_backfill_state/v1"
MAX_QUALITY_REPORT_BYTES = 16 * 1024 * 1024
MAX_WINDOWS_PER_BATCH = 50
DEFAULT_MAX_WINDOWS = 1
RUN_ID_PATTERN = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$")
TOKEN_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
MARKET_TYPES = frozenset(("cex", "dex"))


class QualityContractError(ValueError):
    """The published quality report cannot safely authorize a backfill."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: Optional[datetime] = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".{}.{}.tmp".format(path.name, uuid.uuid4().hex))
    try:
        temporary.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(str(temporary), str(path))
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_bounded_json(path: Path, maximum_bytes: int) -> Any:
    try:
        with path.open("rb") as handle:
            payload = handle.read(maximum_bytes + 1)
    except OSError as error:
        raise QualityContractError(
            "Published quality report is unavailable: {}".format(path)
        ) from error
    if len(payload) > maximum_bytes:
        raise QualityContractError(
            "Published quality report exceeds the {} byte limit".format(
                maximum_bytes
            )
        )
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QualityContractError(
            "Published quality report is not valid UTF-8 JSON"
        ) from error


def database_publication(data_dir: Path) -> Dict[str, str]:
    database_path = data_dir / DATABASE_FILENAME
    if not database_path.exists():
        raise QualityContractError(
            "Published SQLite commit point is unavailable: {}".format(database_path)
        )
    try:
        connection = sqlite3.connect(
            "{}?mode=ro".format(database_path.resolve().as_uri()),
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                """
                SELECT snapshot_id, import_run_id
                FROM dataset_state
                WHERE singleton_id = 1
                """
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise QualityContractError(
            "Published SQLite commit point cannot be read"
        ) from error
    if row is None or not row["snapshot_id"] or not row["import_run_id"]:
        raise QualityContractError(
            "Published SQLite commit point has no dataset identity"
        )
    return {
        "dataset_snapshot_id": str(row["snapshot_id"]),
        "import_run_id": str(row["import_run_id"]),
    }


def _parse_inclusive_window(start_text: Any, end_text: Any) -> Tuple[str, str, int]:
    try:
        start_day = date.fromisoformat(str(start_text))
        end_day = date.fromisoformat(str(end_text))
    except (TypeError, ValueError) as error:
        raise QualityContractError(
            "Backfill window contains an invalid ISO date"
        ) from error
    day_count = (end_day - start_day).days + 1
    if day_count < 1 or day_count > 180:
        raise QualityContractError(
            "Backfill window must contain between 1 and 180 inclusive UTC days"
        )
    return start_day.isoformat(), end_day.isoformat(), day_count


def _string_set(value: Any, *, field: str) -> Set[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise QualityContractError(
            "Backfill window {} must be a non-empty string list".format(field)
        )
    result = set(value)
    if len(result) != len(value):
        raise QualityContractError(
            "Backfill window {} contains duplicate values".format(field)
        )
    return result


def window_fingerprint(window: Mapping[str, Any]) -> str:
    material = {
        "token_symbol": window["token_symbol"],
        "start_date": window["start_date"],
        "end_date": window["end_date"],
        "market_types": window["market_types"],
        "market_ids": window["market_ids"],
        "reason_codes": window["reason_codes"],
        "issue_ids": window["issue_ids"],
    }
    return hashlib.sha256(
        json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:20]


def _validated_windows(
    report: Mapping[str, Any],
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    raw_issues = report.get("issues")
    if (
        not isinstance(raw_issues, list)
        or any(not isinstance(issue, dict) for issue in raw_issues)
    ):
        raise QualityContractError(
            "Quality report issues must be a list of objects"
        )
    retryable_issues: Dict[str, Mapping[str, Any]] = {}
    for issue in raw_issues:
        if (
            issue.get("category") != "historical_gap"
            or issue.get("retryable") is not True
        ):
            continue
        issue_id = issue.get("issue_id")
        if not isinstance(issue_id, str) or not issue_id:
            raise QualityContractError(
                "Retryable historical issue has no stable issue_id"
            )
        if issue_id in retryable_issues:
            raise QualityContractError(
                "Quality report contains duplicate retryable issue_ids"
            )
        retryable_issues[issue_id] = issue

    grouped = report.get("backfill_windows_by_token")
    if not isinstance(grouped, dict):
        raise QualityContractError(
            "Quality report backfill_windows_by_token must be an object"
        )

    windows: List[Dict[str, Any]] = []
    represented_issue_ids: Set[str] = set()
    for raw_token, raw_windows in grouped.items():
        token = str(raw_token).strip().upper()
        if (
            not isinstance(raw_token, str)
            or raw_token != token
            or not TOKEN_PATTERN.fullmatch(token)
            or not isinstance(raw_windows, list)
        ):
            raise QualityContractError(
                "Quality report contains an invalid Token backfill group"
            )
        for raw_window in raw_windows:
            if not isinstance(raw_window, dict):
                raise QualityContractError(
                    "Quality report contains a non-object backfill window"
                )
            raw_window_token = raw_window.get("token_symbol")
            if (
                not isinstance(raw_window_token, str)
                or raw_window_token != token
            ):
                raise QualityContractError(
                    "Backfill window Token does not match its report group"
                )
            start_text, end_text, inclusive_days = _parse_inclusive_window(
                raw_window.get("start_date"),
                raw_window.get("end_date"),
            )
            raw_day_count = raw_window.get("day_count")
            if (
                not isinstance(raw_day_count, int)
                or isinstance(raw_day_count, bool)
                or raw_day_count != inclusive_days
            ):
                raise QualityContractError(
                    "Backfill window day_count does not match its inclusive dates"
                )
            market_types = _string_set(
                raw_window.get("market_types"),
                field="market_types",
            )
            if not market_types.issubset(MARKET_TYPES):
                raise QualityContractError(
                    "Backfill window contains an unsupported market type"
                )
            market_ids = _string_set(
                raw_window.get("market_ids"),
                field="market_ids",
            )
            reason_codes = _string_set(
                raw_window.get("reason_codes"),
                field="reason_codes",
            )
            issue_ids = _string_set(
                raw_window.get("issue_ids"),
                field="issue_ids",
            )
            if represented_issue_ids.intersection(issue_ids):
                raise QualityContractError(
                    "One retryable issue is represented by multiple backfill windows"
                )

            expected_market_types: Set[str] = set()
            expected_market_ids: Set[str] = set()
            expected_reason_codes: Set[str] = set()
            expected_dates: Set[date] = set()
            for issue_id in issue_ids:
                issue = retryable_issues.get(issue_id)
                if issue is None:
                    raise QualityContractError(
                        "Backfill window references a non-retryable or missing issue"
                    )
                market = issue.get("market")
                if not isinstance(market, dict):
                    raise QualityContractError(
                        "Backfill issue has no market identity"
                    )
                issue_token = market.get("token_symbol")
                if not isinstance(issue_token, str) or issue_token != token:
                    raise QualityContractError(
                        "Backfill issue Token does not match its window"
                    )
                market_type = market.get("market_type")
                market_id = market.get("market_id")
                reason_code = issue.get("reason_code")
                if market_type not in MARKET_TYPES or not isinstance(
                    market_id, str
                ) or not market_id or not isinstance(reason_code, str) or not reason_code:
                    raise QualityContractError(
                        "Backfill issue has an incomplete market or reason identity"
                    )
                try:
                    issue_day = date.fromisoformat(str(issue.get("date")))
                except ValueError as error:
                    raise QualityContractError(
                        "Backfill issue contains an invalid date"
                    ) from error
                if not date.fromisoformat(start_text) <= issue_day <= date.fromisoformat(
                    end_text
                ):
                    raise QualityContractError(
                        "Backfill issue date is outside its exact window"
                    )
                expected_market_types.add(str(market_type))
                expected_market_ids.add(market_id)
                expected_reason_codes.add(reason_code)
                expected_dates.add(issue_day)

            expected_days = {
                date.fromordinal(date.fromisoformat(start_text).toordinal() + offset)
                for offset in range(inclusive_days)
            }
            if expected_dates != expected_days:
                raise QualityContractError(
                    "Backfill window dates are not the exact consecutive issue dates"
                )
            if market_types != expected_market_types:
                raise QualityContractError(
                    "Backfill window market_types do not match its issues"
                )
            if market_ids != expected_market_ids:
                raise QualityContractError(
                    "Backfill window market_ids do not match its issues"
                )
            if reason_codes != expected_reason_codes:
                raise QualityContractError(
                    "Backfill window reason_codes do not match its issues"
                )

            window: Dict[str, Any] = {
                "token_symbol": token,
                "start_date": start_text,
                "end_date": end_text,
                "day_count": inclusive_days,
                "market_types": sorted(market_types),
                "market_ids": sorted(market_ids),
                "reason_codes": sorted(reason_codes),
                "issue_ids": sorted(issue_ids),
            }
            window["window_id"] = window_fingerprint(window)
            windows.append(window)
            represented_issue_ids.update(issue_ids)

    if represented_issue_ids != set(retryable_issues):
        raise QualityContractError(
            "Retryable historical issues and exact backfill windows disagree"
        )
    windows.sort(
        key=lambda item: (
            item["start_date"],
            item["end_date"],
            item["token_symbol"],
            item["window_id"],
        )
    )
    return windows, represented_issue_ids


def load_quality_snapshot(data_dir: Path) -> Dict[str, Any]:
    data_dir = data_dir.expanduser().resolve()
    report_path = data_dir / QUALITY_REPORT_PATH
    report = read_bounded_json(report_path, MAX_QUALITY_REPORT_BYTES)
    if (
        not isinstance(report, dict)
        or report.get("schema") != QUALITY_REPORT_SCHEMA
    ):
        raise QualityContractError(
            "Published quality report has an unsupported schema"
        )
    publication = report.get("publication")
    if not isinstance(publication, dict):
        raise QualityContractError(
            "Published quality report has no publication identity"
        )
    quality_import_run_id = publication.get("import_run_id")
    quality_snapshot_id = publication.get("dataset_snapshot_id")
    if not isinstance(quality_import_run_id, str) or not quality_import_run_id:
        raise QualityContractError(
            "Published quality report has no import_run_id"
        )
    if not isinstance(quality_snapshot_id, str) or not quality_snapshot_id:
        raise QualityContractError(
            "Published quality report has no dataset_snapshot_id"
        )
    database_identity = database_publication(data_dir)
    if (
        quality_import_run_id != database_identity["import_run_id"]
        or quality_snapshot_id != database_identity["dataset_snapshot_id"]
    ):
        raise QualityContractError(
            "Quality report does not match the published SQLite commit point"
        )
    windows, issue_ids = _validated_windows(report)
    return {
        "report_path": str(report_path),
        "report_sha256": sha256_file(report_path),
        "import_run_id": quality_import_run_id,
        "dataset_snapshot_id": quality_snapshot_id,
        "windows": windows,
        "issue_ids": issue_ids,
    }


def build_backfill_command(
    window: Mapping[str, Any],
    *,
    data_dir: Path,
    python_executable: str = sys.executable,
) -> List[str]:
    command = [
        python_executable,
        str(PROJECT_ROOT / "scripts/run_fact_pipeline.py"),
    ]
    market_types = set(window["market_types"])
    if market_types == {"cex"}:
        command.append("--cex-only")
    elif market_types == {"dex"}:
        command.append("--dex-only")
    elif market_types != {"cex", "dex"}:
        raise QualityContractError(
            "Exact backfill window has no executable market scope"
        )
    command.extend(
        [
            "--append",
            "--tokens",
            str(window["token_symbol"]),
            "--start",
            str(window["start_date"]),
            "--end",
            str(window["end_date"]),
            "--data-dir",
            str(data_dir.expanduser().resolve()),
            "--publish-local",
        ]
    )
    return command


def default_step_runner(command: Sequence[str], log_path: Path) -> int:
    with log_path.open("w", encoding="utf-8") as log_handle:
        completed = subprocess.run(
            list(command),
            cwd=str(PROJECT_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    return int(completed.returncode)


def _new_run_id(now: Optional[datetime] = None) -> str:
    started = (now or utc_now()).astimezone(timezone.utc)
    return "{}-{}".format(
        started.strftime("%Y%m%dT%H%M%SZ"),
        uuid.uuid4().hex[:8],
    )


def _validate_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("Resume run ID has an invalid format")
    return run_id


def _state_paths(
    data_dir: Path,
    run_id: str,
    run_root: Optional[Path],
) -> Tuple[Path, Path]:
    root = (
        run_root.expanduser().resolve()
        if run_root is not None
        else data_dir / "collection/exact-backfill/runs"
    )
    return root / run_id / "state.json", root.parent / "latest.json"


def _persist_state(
    state_path: Path,
    latest_path: Path,
    state: Dict[str, Any],
) -> None:
    state["updated_at_utc"] = utc_text()
    state["state_path"] = str(state_path)
    atomic_write_json(state_path, state)
    atomic_write_json(latest_path, state)


def _load_resume_state(state_path: Path, data_dir: Path, run_id: str) -> Dict[str, Any]:
    payload = read_bounded_json(state_path, MAX_QUALITY_REPORT_BYTES)
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != STATE_SCHEMA
        or payload.get("run_id") != run_id
        or payload.get("data_dir") != str(data_dir)
        or not isinstance(payload.get("batches"), list)
        or not isinstance(payload.get("windows"), list)
    ):
        raise ValueError("Resume state does not match this exact-backfill run")
    return payload


def _interrupt_unfinished_state(state: Dict[str, Any]) -> None:
    if state.get("status") != "running":
        return
    for window in reversed(state["windows"]):
        if window.get("status") == "running":
            window["status"] = "interrupted_before_resume"
            window["finished_at_utc"] = utc_text()
            break
    if state["batches"] and state["batches"][-1].get("status") == "running":
        state["batches"][-1]["status"] = "interrupted_before_resume"
        state["batches"][-1]["finished_at_utc"] = utc_text()


def _log_tail(path: Path, maximum_lines: int = 30) -> List[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-maximum_lines:]


def _dry_run_result(
    snapshot: Mapping[str, Any],
    *,
    data_dir: Path,
    max_windows: int,
    python_executable: str,
) -> Dict[str, Any]:
    candidates = []
    for window in snapshot["windows"][:max_windows]:
        candidates.append(
            {
                "window": dict(window),
                "command": build_backfill_command(
                    window,
                    data_dir=data_dir,
                    python_executable=python_executable,
                ),
            }
        )
    return {
        "schema": STATE_SCHEMA,
        "status": "dry_run",
        "data_dir": str(data_dir),
        "quality_import_run_id": snapshot["import_run_id"],
        "quality_dataset_snapshot_id": snapshot["dataset_snapshot_id"],
        "quality_report_sha256": snapshot["report_sha256"],
        "candidate_count": len(snapshot["windows"]),
        "preview_count": len(candidates),
        "max_windows": max_windows,
        "candidates": candidates,
        "note": (
            "Execution reloads the current quality report after every committed "
            "window; later preview entries are not pre-authorized commands."
        ),
    }


def run_exact_backfill(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    max_windows: int = DEFAULT_MAX_WINDOWS,
    dry_run: bool = False,
    resume_run_id: Optional[str] = None,
    run_root: Optional[Path] = None,
    lock_path: Optional[Path] = None,
    python_executable: str = sys.executable,
    now: Optional[datetime] = None,
    step_runner: Callable[[Sequence[str], Path], int] = default_step_runner,
) -> Dict[str, Any]:
    if max_windows < 1 or max_windows > MAX_WINDOWS_PER_BATCH:
        raise ValueError(
            "max_windows must be between 1 and {}".format(MAX_WINDOWS_PER_BATCH)
        )
    if dry_run and resume_run_id is not None:
        raise ValueError("--dry-run cannot resume a prior state")

    data_dir = data_dir.expanduser().resolve()
    selected_lock_path = (
        lock_path.expanduser().resolve()
        if lock_path is not None
        else data_dir / "collection/collection.lock"
    )
    selected_lock_path.parent.mkdir(parents=True, exist_ok=True)

    with selected_lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {
                "schema": STATE_SCHEMA,
                "status": "skipped_locked",
                "data_dir": str(data_dir),
                "lock_path": str(selected_lock_path),
            }

        initial_snapshot = load_quality_snapshot(data_dir)
        if dry_run:
            return _dry_run_result(
                initial_snapshot,
                data_dir=data_dir,
                max_windows=max_windows,
                python_executable=python_executable,
            )

        if resume_run_id is None:
            run_id = _new_run_id(now)
        else:
            run_id = _validate_run_id(resume_run_id)
        state_path, latest_path = _state_paths(data_dir, run_id, run_root)

        if resume_run_id is None:
            if state_path.exists():
                raise FileExistsError(
                    "Exact-backfill state already exists: {}".format(state_path)
                )
            state: Dict[str, Any] = {
                "schema": STATE_SCHEMA,
                "run_id": run_id,
                "data_dir": str(data_dir),
                "lock_path": str(selected_lock_path),
                "started_at_utc": utc_text(now),
                "updated_at_utc": utc_text(now),
                "status": "running",
                "batches": [],
                "windows": [],
            }
        else:
            state = _load_resume_state(state_path, data_dir, run_id)
            _interrupt_unfinished_state(state)
            state["status"] = "running"

        batch: Dict[str, Any] = {
            "batch_number": len(state["batches"]) + 1,
            "started_at_utc": utc_text(),
            "finished_at_utc": None,
            "status": "running",
            "max_windows": max_windows,
            "baseline_import_run_id": initial_snapshot["import_run_id"],
            "attempted_window_count": 0,
            "progress_window_count": 0,
        }
        state["batches"].append(batch)
        _persist_state(state_path, latest_path, state)

        attempted_in_batch = 0
        while attempted_in_batch < max_windows:
            before = load_quality_snapshot(data_dir)
            if not before["windows"]:
                state["status"] = "exhausted"
                batch["status"] = "exhausted"
                break

            window = before["windows"][0]
            command = build_backfill_command(
                window,
                data_dir=data_dir,
                python_executable=python_executable,
            )
            attempt_number = len(state["windows"]) + 1
            log_path = state_path.parent / "window-{:04d}.log".format(
                attempt_number
            )
            attempt: Dict[str, Any] = {
                "attempt_number": attempt_number,
                "batch_number": batch["batch_number"],
                "status": "running",
                "started_at_utc": utc_text(),
                "finished_at_utc": None,
                "window": dict(window),
                "command": command,
                "log_path": str(log_path),
                "before_import_run_id": before["import_run_id"],
                "before_dataset_snapshot_id": before["dataset_snapshot_id"],
                "before_quality_report_sha256": before["report_sha256"],
                "before_backfill_issue_count": len(before["issue_ids"]),
            }
            state["windows"].append(attempt)
            attempted_in_batch += 1
            batch["attempted_window_count"] = attempted_in_batch
            _persist_state(state_path, latest_path, state)

            try:
                exit_code = step_runner(command, log_path)
            except KeyboardInterrupt:
                attempt["exit_code"] = 130
                attempt["status"] = "interrupted"
                attempt["error"] = "KeyboardInterrupt"
                attempt["finished_at_utc"] = utc_text()
                state["status"] = "interrupted"
                batch["status"] = "interrupted"
                _persist_state(state_path, latest_path, state)
                raise
            except Exception as error:
                exit_code = 1
                attempt["runner_error"] = "{}: {}".format(
                    type(error).__name__,
                    error,
                )

            attempt["exit_code"] = int(exit_code)
            attempt["log_sha256"] = (
                sha256_file(log_path) if log_path.exists() else None
            )
            attempt["log_tail"] = _log_tail(log_path)
            if exit_code != 0:
                attempt["status"] = "failed_collector"
                attempt["finished_at_utc"] = utc_text()
                state["status"] = "failed_collector"
                batch["status"] = "failed_collector"
                break

            try:
                after = load_quality_snapshot(data_dir)
            except QualityContractError as error:
                attempt["status"] = "failed_verification"
                attempt["verification_error"] = str(error)
                attempt["finished_at_utc"] = utc_text()
                state["status"] = "failed_verification"
                batch["status"] = "failed_verification"
                break

            before_issue_ids = set(window["issue_ids"])
            unresolved_issue_ids = sorted(
                before_issue_ids.intersection(after["issue_ids"])
            )
            resolved_issue_ids = sorted(
                before_issue_ids.difference(after["issue_ids"])
            )
            after_window_ids = {
                current["window_id"] for current in after["windows"]
            }
            attempt["after_import_run_id"] = after["import_run_id"]
            attempt["after_dataset_snapshot_id"] = after["dataset_snapshot_id"]
            attempt["after_quality_report_sha256"] = after["report_sha256"]
            attempt["after_backfill_issue_count"] = len(after["issue_ids"])
            attempt["resolved_issue_ids"] = resolved_issue_ids
            attempt["unresolved_issue_ids"] = unresolved_issue_ids
            attempt["exact_window_still_present"] = (
                window["window_id"] in after_window_ids
            )

            if after["import_run_id"] == before["import_run_id"]:
                attempt["status"] = "failed_verification"
                attempt["verification_error"] = (
                    "Collector exited zero but publication import_run_id did not change"
                )
                state["status"] = "failed_verification"
                batch["status"] = "failed_verification"
            elif not resolved_issue_ids:
                attempt["status"] = "no_progress"
                attempt["verification_error"] = (
                    "Publication changed but none of the selected exact-window "
                    "issue_ids were resolved"
                )
                state["status"] = "no_progress"
                batch["status"] = "no_progress"
            else:
                attempt["status"] = "progress"
                batch["progress_window_count"] += 1
            attempt["finished_at_utc"] = utc_text()
            _persist_state(state_path, latest_path, state)

            if attempt["status"] != "progress":
                break

        if state["status"] == "running":
            final_snapshot = load_quality_snapshot(data_dir)
            if not final_snapshot["windows"]:
                state["status"] = "exhausted"
                batch["status"] = "exhausted"
            else:
                state["status"] = "batch_limit_reached"
                batch["status"] = "batch_limit_reached"
            state["remaining_window_count"] = len(final_snapshot["windows"])
            state["remaining_issue_count"] = len(final_snapshot["issue_ids"])
            state["final_import_run_id"] = final_snapshot["import_run_id"]
            state["final_dataset_snapshot_id"] = final_snapshot[
                "dataset_snapshot_id"
            ]
        else:
            try:
                final_snapshot = load_quality_snapshot(data_dir)
            except QualityContractError:
                final_snapshot = None
            if final_snapshot is not None:
                state["remaining_window_count"] = len(final_snapshot["windows"])
                state["remaining_issue_count"] = len(final_snapshot["issue_ids"])
                state["final_import_run_id"] = final_snapshot["import_run_id"]
                state["final_dataset_snapshot_id"] = final_snapshot[
                    "dataset_snapshot_id"
                ]

        batch["finished_at_utc"] = utc_text()
        state["finished_at_utc"] = utc_text()
        _persist_state(state_path, latest_path, state)
        return state


def positive_bounded_windows(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 1 or parsed > MAX_WINDOWS_PER_BATCH:
        raise argparse.ArgumentTypeError(
            "must be between 1 and {}".format(MAX_WINDOWS_PER_BATCH)
        )
    return parsed


def configured_data_dir() -> Path:
    configured = os.environ.get("MARKET_DATA_DIR")
    return (
        Path(configured).expanduser().resolve()
        if configured
        else DEFAULT_DATA_DIR.resolve()
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute current exact historical-gap windows under the collection lock"
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=configured_data_dir(),
        help="Published runtime snapshot directory (defaults to MARKET_DATA_DIR)",
    )
    parser.add_argument(
        "--max-windows",
        type=positive_bounded_windows,
        default=DEFAULT_MAX_WINDOWS,
        help=(
            "Maximum sequential collector invocations in this batch "
            "(default: 1; maximum: {})".format(MAX_WINDOWS_PER_BATCH)
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print current candidates without running collectors",
    )
    parser.add_argument(
        "--resume-run-id",
        help=(
            "Append to an earlier exact-backfill state log; live quality "
            "authorization is still reloaded"
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    try:
        result = run_exact_backfill(
            data_dir=args.data_dir,
            max_windows=args.max_windows,
            dry_run=args.dry_run,
            resume_run_id=args.resume_run_id,
        )
    except (OSError, QualityContractError, ValueError) as error:
        print(
            json.dumps(
                {
                    "schema": STATE_SCHEMA,
                    "status": "rejected",
                    "error": "{}: {}".format(type(error).__name__, error),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit(2)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["status"] in {
        "failed_collector",
        "failed_verification",
        "interrupted",
        "no_progress",
        "skipped_locked",
    }:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
