"""Failure-atomic replacement of a bounded group of publication files."""

from __future__ import annotations

import csv
import io
import os
import stat
import uuid
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


BundleItem = Tuple[Path, bytes]


def csv_payload(
    fieldnames: Iterable[str],
    rows: Iterable[dict],
) -> bytes:
    """Serialize one deterministic UTF-8 CSV payload before commit begins."""
    buffer = io.StringIO(newline="")
    fields = list(fieldnames)
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(
        {field: row.get(field, "") for field in fields}
        for row in rows
    )
    return buffer.getvalue().encode("utf-8")


def _write_staged(path: Path, payload: bytes, mode: int) -> None:
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("publication staging write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_backup(source: Path, destination: Path) -> None:
    source_descriptor = os.open(str(source), os.O_RDONLY)
    try:
        destination_descriptor = os.open(
            str(destination),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IMODE(os.fstat(source_descriptor).st_mode),
        )
        try:
            while True:
                block = os.read(source_descriptor, 1024 * 1024)
                if not block:
                    break
                view = memoryview(block)
                while view:
                    written = os.write(destination_descriptor, view)
                    if written <= 0:
                        raise OSError(
                            "publication backup write made no progress"
                        )
                    view = view[written:]
            os.fsync(destination_descriptor)
        finally:
            os.close(destination_descriptor)
    finally:
        os.close(source_descriptor)


def _fsync_directories(paths: Iterable[Path]) -> None:
    for directory in sorted({path.parent for path in paths}, key=str):
        try:
            descriptor = os.open(str(directory), os.O_RDONLY)
        except OSError:
            continue
        try:
            os.fsync(descriptor)
        except OSError:
            # Some filesystems do not support directory fsync. File fsync and
            # same-directory replace still provide the strongest local option.
            pass
        finally:
            os.close(descriptor)


def atomic_replace_bundle(items: Iterable[BundleItem]) -> None:
    """Replace every target or restore every pre-call byte on an exception.

    All staged and backup files live beside their destination, so every commit
    and rollback uses a same-filesystem ``os.replace``. This guarantees failure
    atomicity for ordinary I/O exceptions. It is deliberately a bounded helper,
    not a claim of crash-atomic multi-file semantics.
    """
    normalized: List[BundleItem] = []
    destinations = set()
    for raw_path, raw_payload in items:
        path = Path(raw_path)
        if path in destinations:
            raise ValueError("publication bundle contains duplicate destination")
        if not isinstance(raw_payload, bytes):
            raise TypeError("publication bundle payload must be bytes")
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise ValueError("publication destination is not a regular file")
        destinations.add(path)
        normalized.append((path, raw_payload))
    if not normalized:
        raise ValueError("publication bundle is empty")

    staged: List[Tuple[Path, Path]] = []
    backups: dict[Path, Optional[Path]] = {}
    committed: List[Path] = []
    transaction_id = uuid.uuid4().hex
    try:
        for path, payload in normalized:
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = (
                stat.S_IMODE(path.stat().st_mode)
                if path.exists()
                else 0o644
            )
            staged_path = path.with_name(
                ".{}.{}.stage".format(path.name, transaction_id)
            )
            _write_staged(staged_path, payload, mode)
            staged.append((path, staged_path))
            if path.exists():
                backup_path = path.with_name(
                    ".{}.{}.backup".format(path.name, transaction_id)
                )
                _copy_backup(path, backup_path)
                backups[path] = backup_path
            else:
                backups[path] = None

        for path, staged_path in staged:
            os.replace(staged_path, path)
            committed.append(path)
        _fsync_directories(path for path, _payload in normalized)
    except BaseException:
        rollback_errors = []
        for path in reversed(committed):
            backup_path = backups[path]
            try:
                if backup_path is None:
                    path.unlink(missing_ok=True)
                else:
                    os.replace(backup_path, path)
                    backups[path] = None
            except OSError as error:
                rollback_errors.append(error)
        _fsync_directories(path for path, _payload in normalized)
        if rollback_errors:
            raise RuntimeError(
                "publication failed and rollback could not restore every file"
            ) from rollback_errors[0]
        raise
    finally:
        for _path, staged_path in staged:
            staged_path.unlink(missing_ok=True)
        for backup_path in backups.values():
            if backup_path is not None:
                backup_path.unlink(missing_ok=True)


def atomic_replace_prepared_bundle(
    items: Iterable[Tuple[Path, Path]],
) -> None:
    """Replace a bundle from prepared same-directory files.

    The prepared files are streamed by the caller instead of materialized as
    in-memory byte payloads.  This function takes ownership of every prepared
    file after validation and removes any uncommitted files on success or
    failure.  Commit and rollback semantics match ``atomic_replace_bundle``.
    """
    normalized: List[Tuple[Path, Path]] = []
    destinations = set()
    prepared_paths = set()
    for raw_path, raw_prepared_path in items:
        path = Path(raw_path)
        prepared_path = Path(raw_prepared_path)
        if path in destinations:
            raise ValueError("publication bundle contains duplicate destination")
        if prepared_path in prepared_paths:
            raise ValueError("publication bundle contains duplicate prepared file")
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise ValueError("publication destination is not a regular file")
        if (
            not prepared_path.exists()
            or prepared_path.is_symlink()
            or not prepared_path.is_file()
        ):
            raise ValueError("prepared publication payload is not a regular file")
        if prepared_path.parent.resolve() != path.parent.resolve():
            raise ValueError(
                "prepared publication payload must share destination directory"
            )
        destinations.add(path)
        prepared_paths.add(prepared_path)
        normalized.append((path, prepared_path))
    if not normalized:
        raise ValueError("publication bundle is empty")
    if destinations & prepared_paths:
        raise ValueError(
            "publication destinations and prepared files must be disjoint"
        )

    backups: dict[Path, Optional[Path]] = {}
    committed: List[Path] = []
    transaction_id = uuid.uuid4().hex
    try:
        for path, prepared_path in normalized:
            mode = (
                stat.S_IMODE(path.stat().st_mode)
                if path.exists()
                else stat.S_IMODE(prepared_path.stat().st_mode) & 0o644
            )
            os.chmod(prepared_path, mode)
            descriptor = os.open(str(prepared_path), os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if path.exists():
                backup_path = path.with_name(
                    ".{}.{}.backup".format(path.name, transaction_id)
                )
                _copy_backup(path, backup_path)
                backups[path] = backup_path
            else:
                backups[path] = None

        for path, prepared_path in normalized:
            os.replace(prepared_path, path)
            committed.append(path)
        _fsync_directories(path for path, _prepared_path in normalized)
    except BaseException:
        rollback_errors = []
        for path in reversed(committed):
            backup_path = backups[path]
            try:
                if backup_path is None:
                    path.unlink(missing_ok=True)
                else:
                    os.replace(backup_path, path)
                    backups[path] = None
            except OSError as error:
                rollback_errors.append(error)
        _fsync_directories(path for path, _prepared_path in normalized)
        if rollback_errors:
            raise RuntimeError(
                "publication failed and rollback could not restore every file"
            ) from rollback_errors[0]
        raise
    finally:
        for _path, prepared_path in normalized:
            prepared_path.unlink(missing_ok=True)
        for backup_path in backups.values():
            if backup_path is not None:
                backup_path.unlink(missing_ok=True)
