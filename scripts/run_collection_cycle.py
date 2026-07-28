"""Run auditable CEX/DEX fact collection profiles under one process lock.

The fact families retain separate publication semantics:

- daily CEX/DEX OHLCV is incrementally upserted and atomically indexed in SQLite;
- TVL, CEX depth, and DEX pool-state depth append normalized history and
  replace latest views;
- fixed-notional execution cost atomically replaces validated latest/current
  views and deliberately does not rewrite one unbounded hourly history CSV.

This runner coordinates those collectors, records every command and result in a
single manifest, and emits source-specific freshness without pretending that the
separate lifecycles form one cross-source atomic transaction.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import uuid
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.freshness import build_source_freshness


DEFAULT_DATA_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RUN_ROOT = DEFAULT_DATA_DIR / "collection/runs"
DEFAULT_LATEST_STATUS = DEFAULT_DATA_DIR / "collection/latest.json"
DEFAULT_LOCK_FILE = DEFAULT_DATA_DIR / "collection/collection.lock"
TOKEN_CONFIG = PROJECT_ROOT / "config/tokens.csv"
DEX_PRICE_INPUT = PROJECT_ROOT / "data/processed/dex_pool_tvl_snapshot.csv"
PROFILE_STEPS = {
    "full": ("daily", "depth", "tvl", "dex_depth"),
    "daily": ("daily", "tvl"),
    "tvl": ("tvl",),
    "depth": ("depth", "dex_price", "dex_depth"),
    "cex_depth": ("depth",),
    "dex_depth": ("dex_price", "dex_depth"),
}
DAILY_FILENAMES = {
    "cex_daily": "cex_exchange_volume_daily.csv",
    "dex_daily": "dex_pool_volume_daily.csv",
}
SNAPSHOT_FILENAMES = {
    "dex_tvl": "dex_pool_tvl_latest.csv",
    "cex_depth": "cex_depth_latest.csv",
    "dex_depth": "dex_depth_latest.csv",
    "cex_execution_cost": "cex_execution_cost_latest.csv",
    "dex_execution_cost": "dex_execution_cost_latest.csv",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def csv_date_bounds(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    with path.open("r", newline="", encoding="utf-8") as handle:
        dates = [row["date"] for row in csv.DictReader(handle) if row.get("date")]
    if not dates:
        return None
    return {
        "available_start": min(dates),
        "available_end": max(dates),
    }


def snapshot_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return None
    return {
        "snapshot_ids": sorted(
            {row["snapshot_id"] for row in rows if row.get("snapshot_id")}
        ),
        "source_snapshot_ids": sorted(
            {
                row["source_snapshot_id"]
                for row in rows
                if row.get("source_snapshot_id")
            }
        ),
        "observed_at": max(
            (row["observed_at"] for row in rows if row.get("observed_at")),
            default=None,
        ),
        "row_count": len(rows),
        "status_counts": dict(Counter(row.get("status") or "missing_status" for row in rows)),
    }


def file_record(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    return {
        "path": str(path),
        "exists": True,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def build_collection_status(
    data_dir: Path = DEFAULT_DATA_DIR,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    checked_at = (now or utc_now()).astimezone(timezone.utc)
    cex_path = data_dir / DAILY_FILENAMES["cex_daily"]
    dex_path = data_dir / DAILY_FILENAMES["dex_daily"]
    tvl_path = data_dir / SNAPSHOT_FILENAMES["dex_tvl"]
    depth_path = data_dir / SNAPSHOT_FILENAMES["cex_depth"]
    dex_depth_path = data_dir / SNAPSHOT_FILENAMES["dex_depth"]
    cex_execution_path = data_dir / SNAPSHOT_FILENAMES["cex_execution_cost"]
    dex_execution_path = data_dir / SNAPSHOT_FILENAMES["dex_execution_cost"]
    source_date_ranges = {
        "cex_daily": csv_date_bounds(cex_path),
        "dex_daily": csv_date_bounds(dex_path),
    }
    tvl = snapshot_summary(tvl_path)
    depth = snapshot_summary(depth_path)
    dex_depth = snapshot_summary(dex_depth_path)
    cex_execution = snapshot_summary(cex_execution_path)
    dex_execution = snapshot_summary(dex_execution_path)
    return {
        "checked_at": utc_text(checked_at),
        "source_date_ranges": source_date_ranges,
        "tvl_snapshot": tvl,
        "cex_depth_snapshot": depth,
        "dex_depth_snapshot": dex_depth,
        "cex_execution_cost_snapshot": cex_execution,
        "dex_execution_cost_snapshot": dex_execution,
        "freshness": build_source_freshness(
            source_date_ranges,
            tvl_observed_at=tvl.get("observed_at") if tvl else None,
            depth_observed_at=depth.get("observed_at") if depth else None,
            dex_depth_observed_at=(
                dex_depth.get("observed_at") if dex_depth else None
            ),
            cex_execution_observed_at=(
                cex_execution.get("observed_at")
                if cex_execution
                else None
            ),
            dex_execution_observed_at=(
                dex_execution.get("observed_at")
                if dex_execution
                else None
            ),
            now=checked_at,
        ),
        "files": {
            name: file_record(data_dir / filename)
            for name, filename in {**DAILY_FILENAMES, **SNAPSHOT_FILENAMES}.items()
        },
    }


def configured_tokens(path: Path = TOKEN_CONFIG) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        tokens = [
            row["token_symbol"].strip().upper()
            for row in csv.DictReader(handle)
            if row.get("token_symbol")
        ]
    if not tokens or len(tokens) != len(set(tokens)):
        raise ValueError("Token configuration must contain unique non-empty symbols")
    return tokens


def resolve_incremental_window(
    data_dir: Path = DEFAULT_DATA_DIR,
    *,
    now: datetime | None = None,
    overlap_days: int = 3,
) -> tuple[str, str] | None:
    if overlap_days < 1:
        raise ValueError("overlap_days must be positive")
    bounds = [
        csv_date_bounds(data_dir / filename)
        for filename in DAILY_FILENAMES.values()
    ]
    if any(item is None for item in bounds):
        return None
    earliest_end = min(
        date.fromisoformat(item["available_end"])
        for item in bounds
        if item is not None
    )
    latest_completed = (now or utc_now()).astimezone(timezone.utc).date() - timedelta(days=1)
    start = earliest_end - timedelta(days=overlap_days - 1)
    start = max(start, latest_completed - timedelta(days=179))
    if start > latest_completed:
        start = latest_completed
    return start.isoformat(), latest_completed.isoformat()


def build_step_commands(
    profile: str,
    *,
    publish_local: bool,
    python_executable: str = sys.executable,
    data_dir: Path = DEFAULT_DATA_DIR,
    now: datetime | None = None,
    start: str | None = None,
    end: str | None = None,
    tokens: list[str] | None = None,
    full_rebuild: bool = False,
) -> list[tuple[str, list[str]]]:
    if profile not in PROFILE_STEPS:
        raise ValueError(f"Unknown collection profile: {profile}")
    if bool(start) != bool(end):
        raise ValueError("--start and --end must be provided together")
    commands: list[tuple[str, list[str]]] = []
    for step in PROFILE_STEPS[profile]:
        if step == "daily":
            command = [
                python_executable,
                str(PROJECT_ROOT / "scripts/run_fact_pipeline.py"),
            ]
            if not full_rebuild:
                window = (start, end) if start and end else resolve_incremental_window(
                    data_dir,
                    now=now,
                )
                if window is not None:
                    selected_tokens = tokens or configured_tokens()
                    command.extend(
                        [
                            "--append",
                            "--tokens",
                            ",".join(selected_tokens),
                            "--start",
                            window[0],
                            "--end",
                            window[1],
                        ]
                    )
            elif start and end:
                command.extend(["--start", start, "--end", end])
            if publish_local:
                command.append("--publish-local")
        elif step in {"tvl", "dex_price"}:
            command = [
                python_executable,
                str(PROJECT_ROOT / "scripts/fetch_tvl.py"),
            ]
            if step == "tvl" and publish_local:
                command.append("--publish-local")
        elif step == "depth":
            command = [
                python_executable,
                str(PROJECT_ROOT / "scripts/fetch_cex_depth.py"),
            ]
            if publish_local:
                command.append("--publish-local")
        else:
            command = [
                python_executable,
                str(PROJECT_ROOT / "scripts/fetch_dex_depth.py"),
                "--tvl-csv",
                str(DEX_PRICE_INPUT),
            ]
            if publish_local:
                command.append("--publish-local")
        commands.append((step, command))
    return commands


def default_step_runner(command: list[str], log_path: Path) -> int:
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        return process.wait()


def validate_step_freshness(name: str, status: dict[str, Any]) -> list[str]:
    """Return stale, unavailable, or wholly unusable scheduled outputs."""
    expected_sources = {
        "daily": ("cex_daily", "dex_daily"),
        "tvl": ("dex_tvl",),
        "depth": ("cex_depth",),
        "dex_depth": ("dex_depth",),
    }[name]
    freshness = status["freshness"]
    invalid = [
        source
        for source in expected_sources
        if freshness[source]["status"] != "current"
    ]
    snapshot_requirement = {
        "tvl": ("tvl_snapshot", "dex_tvl"),
        "depth": ("cex_depth_snapshot", "cex_depth"),
        "dex_depth": ("dex_depth_snapshot", "dex_depth"),
    }.get(name)
    if snapshot_requirement is not None:
        snapshot_key, source_name = snapshot_requirement
        snapshot = status.get(snapshot_key)
        counts = snapshot.get("status_counts", {}) if snapshot else {}
        measured_count = int(counts.get("observed", 0)) + int(
            counts.get("partial", 0)
        )
        if measured_count == 0:
            invalid.append(f"{source_name}_no_measured_rows")

    execution_requirement = {
        "depth": ("cex_execution_cost_snapshot", "cex_depth_snapshot"),
        "dex_depth": ("dex_execution_cost_snapshot", "dex_depth_snapshot"),
    }.get(name)
    if execution_requirement is not None:
        execution_key, source_key = execution_requirement
        execution_source_name = (
            execution_key[:-9]
            if execution_key.endswith("_snapshot")
            else execution_key
        )
        execution_snapshot = status.get(execution_key)
        source_snapshot = status.get(source_key)
        if execution_snapshot is None:
            invalid.append(execution_source_name)
        elif source_snapshot is None or (
            execution_snapshot.get("source_snapshot_ids")
            != source_snapshot.get("snapshot_ids")
        ):
            invalid.append(f"{execution_source_name}_lineage")
        if execution_snapshot is not None:
            execution_counts = execution_snapshot.get("status_counts", {})
            measured_execution_count = int(
                execution_counts.get("observed", 0)
            ) + int(execution_counts.get("partial", 0))
            failed_execution_count = int(execution_counts.get("failed", 0))
            if name == "depth" and measured_execution_count == 0:
                invalid.append(
                    f"{execution_source_name}_no_measured_rows"
                )
            elif (
                name == "dex_depth"
                and measured_execution_count == 0
                and failed_execution_count > 0
            ):
                # An inventory containing only explicit unsupported adapters is
                # a truthful result. Any failed supported adapter without one
                # measured V2 result must not be reported as a successful run.
                invalid.append(
                    f"{execution_source_name}_supported_rows_all_failed"
                )
    return invalid


def log_tail(path: Path, lines: int = 20) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]


def run_collection_cycle(
    profile: str,
    *,
    publish_local: bool,
    data_dir: Path = DEFAULT_DATA_DIR,
    run_root: Path = DEFAULT_RUN_ROOT,
    latest_status_path: Path = DEFAULT_LATEST_STATUS,
    lock_path: Path = DEFAULT_LOCK_FILE,
    now: datetime | None = None,
    start: str | None = None,
    end: str | None = None,
    tokens: list[str] | None = None,
    full_rebuild: bool = False,
    fail_fast: bool = False,
    dry_run: bool = False,
    step_runner: Callable[[list[str], Path], int] = default_step_runner,
) -> dict[str, Any]:
    started = (now or utc_now()).astimezone(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    commands = build_step_commands(
        profile,
        publish_local=publish_local,
        data_dir=data_dir,
        now=started,
        start=start,
        end=end,
        tokens=tokens,
        full_rebuild=full_rebuild,
    )
    if dry_run:
        return {
            "run_id": run_id,
            "profile": profile,
            "status": "dry_run",
            "publish_local": publish_local,
            "commands": [
                {"name": name, "command": command}
                for name, command in commands
            ],
        }

    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {
                "run_id": run_id,
                "profile": profile,
                "status": "skipped_locked",
                "publish_local": publish_local,
            }

        step_results = []
        for name, command in commands:
            step_started = utc_now()
            log_path = run_dir / f"{name}.log"
            price_dependency = (
                step_results[-1]
                if (
                    name == "dex_depth"
                    and step_results
                    and step_results[-1]["name"] in {"tvl", "dex_price"}
                )
                else None
            )
            if (
                price_dependency is not None
                and price_dependency["exit_code"] != 0
            ):
                step_error = (
                    "Skipped because the required fresh DEX USD-price input "
                    f"step {price_dependency['name']} failed"
                )
                log_path.write_text(step_error + "\n", encoding="utf-8")
                step_finished = utc_now()
                step_results.append(
                    {
                        "name": name,
                        "command": command,
                        "started_at": utc_text(step_started),
                        "finished_at": utc_text(step_finished),
                        "duration_seconds": round(
                            (step_finished - step_started).total_seconds(),
                            3,
                        ),
                        "exit_code": 4,
                        "status": "skipped_dependency",
                        "log_path": str(log_path),
                        "log_sha256": sha256_file(log_path),
                        "log_tail": log_tail(log_path),
                        "error": step_error,
                        "validation": {
                            "checked": False,
                            "reason": "failed_price_dependency",
                        },
                    }
                )
                continue
            print(f"[collection] starting {name}: {' '.join(command)}", flush=True)
            step_error = None
            try:
                exit_code = step_runner(command, log_path)
            except KeyboardInterrupt:
                exit_code = 130
                step_error = "KeyboardInterrupt: collection cycle interrupted"
                with log_path.open("a", encoding="utf-8") as log:
                    log.write(step_error + "\n")
            except Exception as error:
                exit_code = 1
                step_error = f"{type(error).__name__}: {error}"
                with log_path.open("a", encoding="utf-8") as log:
                    log.write(step_error + "\n")
            validation = None
            should_validate = publish_local and name != "dex_price" and (
                name != "daily" or (tokens is None and start is None and end is None)
            )
            if exit_code == 0 and should_validate:
                post_step_status = build_collection_status(data_dir, now=started)
                invalid_sources = validate_step_freshness(name, post_step_status)
                validation_sources = {
                    source: post_step_status["freshness"][source]
                    for source in {
                        "daily": ("cex_daily", "dex_daily"),
                        "tvl": ("dex_tvl",),
                        "depth": ("cex_depth",),
                        "dex_depth": ("dex_depth",),
                    }[name]
                }
                if name == "depth":
                    validation_sources["cex_execution_cost"] = (
                        post_step_status["cex_execution_cost_snapshot"]
                    )
                elif name == "dex_depth":
                    validation_sources["dex_execution_cost"] = (
                        post_step_status["dex_execution_cost_snapshot"]
                    )
                validation = {
                    "checked": True,
                    "sources": validation_sources,
                    "status": "passed" if not invalid_sources else "failed",
                }
                if invalid_sources:
                    exit_code = 3
                    step_error = (
                        "Freshness validation failed for: "
                        + ", ".join(invalid_sources)
                    )
                    with log_path.open("a", encoding="utf-8") as log:
                        log.write(step_error + "\n")
            elif exit_code == 0:
                validation = {
                    "checked": False,
                    "reason": "non-publishing or bounded/manual refresh",
                }
            step_finished = utc_now()
            result = {
                "name": name,
                "command": command,
                "started_at": utc_text(step_started),
                "finished_at": utc_text(step_finished),
                "duration_seconds": round(
                    (step_finished - step_started).total_seconds(),
                    3,
                ),
                "exit_code": exit_code,
                "status": "succeeded" if exit_code == 0 else "failed",
                "log_path": str(log_path),
                "log_sha256": sha256_file(log_path),
                "log_tail": log_tail(log_path),
                "error": step_error,
                "validation": validation,
            }
            step_results.append(result)
            if exit_code != 0 and (fail_fast or exit_code == 130):
                break

        status = (
            "succeeded"
            if len(step_results) == len(commands)
            and all(item["exit_code"] == 0 for item in step_results)
            else "failed"
        )
        manifest = {
            "schema_version": 1,
            "run_id": run_id,
            "profile": profile,
            "status": status,
            "publish_local": publish_local,
            "started_at": utc_text(started),
            "finished_at": utc_text(),
            "atomicity": (
                "The server-visible SQLite database and each latest snapshot "
                "file are staged and atomically replaced per file after "
                "validation. Source CSVs and append-only histories are not a "
                "single multi-file transaction, and this manifest does not "
                "claim cross-source atomicity."
            ),
            "steps": step_results,
            "facts": build_collection_status(data_dir),
            "dependency_files": (
                {"dex_price_input": file_record(DEX_PRICE_INPUT)}
                if any(
                    name in {"dex_price", "dex_depth"}
                    for name, _command in commands
                )
                else {}
            ),
        }
        manifest_path = run_dir / "manifest.json"
        atomic_write_json(manifest_path, manifest)
        manifest["manifest_path"] = str(manifest_path)
        atomic_write_json(latest_status_path, manifest)
        return manifest


def parse_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    values = [item.strip().upper() for item in value.split(",") if item.strip()]
    return values or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a coordinated fact collection cycle")
    parser.add_argument("--profile", choices=sorted(PROFILE_STEPS), default="full")
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument("--start", help="Inclusive UTC date override for daily facts")
    parser.add_argument("--end", help="Inclusive UTC date override for daily facts")
    parser.add_argument("--tokens", help="Comma-separated daily Token override")
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Replace daily processed output instead of incremental upsert",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_collection_cycle(
        args.profile,
        publish_local=args.publish_local,
        start=args.start,
        end=args.end,
        tokens=parse_list(args.tokens),
        full_rebuild=args.full_rebuild,
        fail_fast=args.fail_fast,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
