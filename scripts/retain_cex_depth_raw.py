#!/usr/bin/env python3
"""Compress and expire raw CEX depth snapshots with a dry-run default."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/cex-depth"
SNAPSHOT_PATTERN = re.compile(r"^(?P<stamp>\d{8}T\d{6}Z)-[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class RetentionAction:
    action: str
    source: str
    target: str | None
    snapshot_at: str


def parse_snapshot_time(snapshot_id: str) -> datetime | None:
    match = SNAPSHOT_PATTERN.fullmatch(snapshot_id)
    if not match:
        return None
    try:
        return datetime.strptime(match.group("stamp"), "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def validate_retention_root(raw_root: Path) -> Path:
    """Refuse broad or ambiguous deletion targets before planning any action."""
    expanded = raw_root.expanduser()
    if expanded.is_symlink():
        raise ValueError("Retention root must be a real directory, not a symlink")
    resolved = expanded.resolve()
    forbidden = {
        Path("/"),
        Path.home().resolve(),
        PROJECT_ROOT.resolve(),
        (PROJECT_ROOT / "data").resolve(),
        (PROJECT_ROOT / "data/raw").resolve(),
    }
    if resolved in forbidden or resolved.name != "cex-depth":
        raise ValueError("Retention root must be a dedicated directory named cex-depth")
    if resolved.exists() and (not resolved.is_dir() or resolved.is_symlink()):
        raise ValueError("Retention root must be a real directory, not a file or symlink")
    return resolved


def plan_retention(
    raw_root: Path,
    *,
    now: datetime,
    keep_raw_days: int,
    keep_archive_days: int,
) -> list[RetentionAction]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if keep_raw_days < 1:
        raise ValueError("keep_raw_days must be at least 1")
    if keep_archive_days <= keep_raw_days:
        raise ValueError("keep_archive_days must be greater than keep_raw_days")

    root = validate_retention_root(raw_root)
    if not root.exists():
        return []
    archive_root = root / "archives"
    raw_cutoff = now.astimezone(timezone.utc) - timedelta(days=keep_raw_days)
    archive_cutoff = now.astimezone(timezone.utc) - timedelta(days=keep_archive_days)
    actions: list[RetentionAction] = []

    for snapshot_dir in sorted(root.iterdir()):
        if snapshot_dir.name == "archives" or not snapshot_dir.is_dir():
            continue
        if snapshot_dir.is_symlink():
            continue
        snapshot_at = parse_snapshot_time(snapshot_dir.name)
        if snapshot_at is None or snapshot_at >= raw_cutoff:
            continue
        archive_path = archive_root / f"{snapshot_dir.name}.tar.gz"
        if archive_path.exists():
            continue
        actions.append(
            RetentionAction(
                action="compress",
                source=str(snapshot_dir),
                target=str(archive_path),
                snapshot_at=snapshot_at.isoformat(),
            )
        )

    if archive_root.exists() and archive_root.is_dir() and not archive_root.is_symlink():
        for archive_path in sorted(archive_root.glob("*.tar.gz")):
            snapshot_id = archive_path.name[: -len(".tar.gz")]
            snapshot_at = parse_snapshot_time(snapshot_id)
            if snapshot_at is None or snapshot_at >= archive_cutoff:
                continue
            actions.append(
                RetentionAction(
                    action="delete_archive",
                    source=str(archive_path),
                    target=None,
                    snapshot_at=snapshot_at.isoformat(),
                )
            )
    return actions


def _reject_links(snapshot_dir: Path) -> None:
    for path in snapshot_dir.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"Refusing to archive symlink: {path}")
        if not path.is_file() and not path.is_dir():
            raise ValueError(f"Refusing to archive special file: {path}")


def _verify_archive(archive_path: Path, snapshot_id: str) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        if not members:
            raise ValueError(f"Archive is empty: {archive_path}")
        prefix = f"{snapshot_id}/"
        if any(
            member.name != snapshot_id and not member.name.startswith(prefix)
            for member in members
        ):
            raise ValueError(f"Archive contains an unexpected path: {archive_path}")


def _regular_file_filter(member: tarfile.TarInfo) -> tarfile.TarInfo:
    if not member.isfile() and not member.isdir():
        raise ValueError(f"Refusing to archive non-regular member: {member.name}")
    return member


def apply_retention(actions: Iterable[RetentionAction]) -> None:
    """Apply a previously reviewed plan; each compression is atomic and verified."""
    for action in actions:
        source = Path(action.source)
        if action.action == "compress":
            if action.target is None:
                raise ValueError("Compression action is missing a target")
            if not source.is_dir() or source.is_symlink():
                raise ValueError(f"Snapshot directory disappeared or is unsafe: {source}")
            _reject_links(source)
            target = Path(action.target)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                raise FileExistsError(f"Archive already exists: {target}")
            temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
            try:
                with tarfile.open(temporary, "w:gz") as archive:
                    archive.add(
                        source,
                        arcname=source.name,
                        recursive=True,
                        filter=_regular_file_filter,
                    )
                _verify_archive(temporary, source.name)
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
            shutil.rmtree(source)
        elif action.action == "delete_archive":
            if source.parent.name != "archives" or source.suffixes[-2:] != [".tar", ".gz"]:
                raise ValueError(f"Refusing to delete unexpected path: {source}")
            source.unlink(missing_ok=True)
        else:
            raise ValueError(f"Unknown retention action: {action.action}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compress old raw CEX depth snapshot directories and expire older archives. "
            "The default is a non-destructive dry run."
        )
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--keep-raw-days", type=int, default=7)
    parser.add_argument("--keep-archive-days", type=int, default=30)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the printed plan. Omit this flag for the safe dry run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    actions = plan_retention(
        args.root,
        now=now,
        keep_raw_days=args.keep_raw_days,
        keep_archive_days=args.keep_archive_days,
    )
    result = {
        "mode": "apply" if args.apply else "dry-run",
        "root": str(validate_retention_root(args.root)),
        "keep_raw_days": args.keep_raw_days,
        "keep_archive_days": args.keep_archive_days,
        "actions": [asdict(action) for action in actions],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.apply:
        apply_retention(actions)


if __name__ == "__main__":
    main()
