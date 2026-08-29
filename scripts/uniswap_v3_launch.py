#!/usr/bin/env python3
"""Checksummed staging and rollback primitives for the exact V3 launch.

The command-line workflow is intentionally added after the filesystem
transaction boundary below.  These helpers make no crash-atomic multi-file
claim: they restore all pre-call bytes after ordinary I/O exceptions.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, TextIO, Tuple

from scripts.atomic_publication import atomic_replace_bundle
from scripts.fetch_dex_depth import (
    uniswap_v3_exact_receipt_bytes,
    validate_uniswap_v3_exact_candidate,
    validate_uniswap_v3_exact_public_receipt,
)
from scripts.run_collection_cycle import processed_dir_for


PUBLIC_BUNDLE_NAMES = (
    "dex_depth_history.csv",
    "dex_depth_latest.csv",
    "dex_depth_snapshot.csv",
    "dex_execution_cost_latest.csv",
    "uniswap_v3_exact_latest.json",
)
PUBLIC_BUNDLE_SCHEMA = "uniswap_v3_public_bundle/v1"
BACKUP_SCHEMA = "uniswap_v3_launch_backup/v1"
STAGE_INPUT_SCHEMA = "uniswap_v3_stage_inputs/v1"
PROMOTION_SCHEMA = "uniswap_v3_launch_promotion/v1"
RESTORE_SCHEMA = "uniswap_v3_launch_restore/v1"
RECEIPT_SCHEMA = "uniswap_v3_launch_receipt/v1"
RAW_RECEIPT_NAME = "uniswap_v3_exact_validation.json"
MAX_PUBLIC_FILE_BYTES = 512 * 1024 * 1024
MAX_INPUT_FILE_BYTES = 8 * 1024 * 1024 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
SHA_PATTERN = re.compile(r"[0-9a-f]{40,64}")
PROJECT_ROOT = Path(__file__).resolve().parents[1]

TIMER_UNITS = (
    "cex-dex-daily.timer",
    "cex-dex-depth.timer",
)
SERVICE_UNITS = (
    "cex-dex-daily.service",
    "cex-dex-depth.service",
)
MANAGED_UNITS = TIMER_UNITS + SERVICE_UNITS
PHASES = (
    "preflight",
    "pause",
    "backup",
    "stage",
    "verify-stage",
    "promote",
    "restore",
    "resume",
)
RECEIPT_FILES = {
    "preflight": "01-preflight.json",
    "pause": "02-pause.json",
    "backup": "03-backup.json",
    "stage": "04-stage.json",
    "verify-stage": "05-verify-stage.json",
    "promote": "06-promote.json",
    "restore": "07-restore.json",
    "resume": "08-resume.json",
}

_REQUIRED_STAGE_INPUTS = (
    "market_facts.sqlite3",
    "dex_pool_volume_daily.csv",
    "dex_depth_history.csv",
)


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    """Return the canonical on-disk representation for private receipts."""
    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _validate_sha(value: str, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if SHA_PATTERN.fullmatch(normalized) is None:
        raise ValueError("{} SHA is invalid".format(label))
    return normalized


def _require_directory(path: Path, label: str, *, mode: Optional[int] = None) -> os.stat_result:
    path = Path(path)
    try:
        metadata = os.lstat(str(path))
    except OSError as error:
        raise ValueError("{} must be a regular non-symlink directory".format(label)) from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("{} must be a regular non-symlink directory".format(label))
    if mode is not None and stat.S_IMODE(metadata.st_mode) != mode:
        raise ValueError("{} must have mode {:04o}".format(label, mode))
    return metadata


def _create_private_directory(path: Path, label: str) -> None:
    path = Path(path)
    try:
        os.mkdir(str(path), 0o700)
    except FileExistsError:
        _require_directory(path, label, mode=0o700)
        return
    os.chmod(str(path), 0o700)
    _require_directory(path, label, mode=0o700)


def _open_regular_read(
    path: Path,
    label: str,
    *,
    limit: int,
) -> Tuple[int, os.stat_result]:
    path = Path(path)
    try:
        before = os.lstat(str(path))
    except OSError as error:
        raise ValueError("{} must be a regular non-symlink file".format(label)) from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("{} must be a regular non-symlink file".format(label))
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(str(path), flags)
    except OSError as error:
        raise ValueError("{} must be a regular non-symlink file".format(label)) from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != before.st_dev
            or opened.st_ino != before.st_ino
        ):
            raise ValueError("{} must be a regular non-symlink file".format(label))
        if opened.st_size < 0 or opened.st_size > limit:
            raise ValueError("{} exceeds the bounded read limit".format(label))
        return descriptor, opened
    except BaseException:
        os.close(descriptor)
        raise


def _read_regular_bytes(path: Path, label: str, *, limit: int) -> Tuple[bytes, int]:
    descriptor, opened = _open_regular_read(path, label, limit=limit)
    payload = bytearray()
    try:
        while len(payload) <= limit:
            block = os.read(descriptor, min(1024 * 1024, limit + 1 - len(payload)))
            if not block:
                break
            payload.extend(block)
        after = os.fstat(descriptor)
        if (
            after.st_dev != opened.st_dev
            or after.st_ino != opened.st_ino
            or after.st_size != opened.st_size
        ):
            raise ValueError("{} changed during the bounded read".format(label))
    finally:
        os.close(descriptor)
    if len(payload) > limit or len(payload) != opened.st_size:
        raise ValueError("{} exceeds the bounded read limit".format(label))
    return bytes(payload), stat.S_IMODE(opened.st_mode)


def _write_exclusive(path: Path, payload: bytes, mode: int) -> None:
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        mode,
    )
    try:
        os.fchmod(descriptor, mode)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("exclusive write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _file_record(path: Path, name: str, *, required: bool) -> dict[str, Any]:
    try:
        os.lstat(str(path))
    except FileNotFoundError:
        if required:
            raise ValueError("{} must be a regular non-symlink file".format(name))
        return {"exists": False}
    payload, mode = _read_regular_bytes(
        path,
        name,
        limit=MAX_PUBLIC_FILE_BYTES,
    )
    return {
        "exists": True,
        "mode": mode,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
    }


def snapshot_public_bundle(data_dir: Path) -> dict[str, Any]:
    """Describe the fixed public generation without following symlinks."""
    data_dir = Path(data_dir)
    _require_directory(data_dir, "public data root")
    files = {}
    for index, name in enumerate(PUBLIC_BUNDLE_NAMES):
        files[name] = _file_record(
            data_dir / name,
            name,
            required=index < len(PUBLIC_BUNDLE_NAMES) - 1,
        )
    return {
        "schema": PUBLIC_BUNDLE_SCHEMA,
        "order": list(PUBLIC_BUNDLE_NAMES),
        "files": files,
    }


def _validate_bundle_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(manifest, Mapping)
        or set(manifest) != {"schema", "order", "files"}
        or manifest.get("schema") != PUBLIC_BUNDLE_SCHEMA
        or manifest.get("order") != list(PUBLIC_BUNDLE_NAMES)
        or not isinstance(manifest.get("files"), Mapping)
        or list(manifest["files"].keys()) != list(PUBLIC_BUNDLE_NAMES)
    ):
        raise ValueError("public bundle manifest is invalid")
    normalized = {
        "schema": manifest["schema"],
        "order": list(manifest["order"]),
        "files": {},
    }
    for index, name in enumerate(PUBLIC_BUNDLE_NAMES):
        record = manifest["files"].get(name)
        if record == {"exists": False}:
            if index != len(PUBLIC_BUNDLE_NAMES) - 1:
                raise ValueError("public bundle manifest is invalid")
            normalized["files"][name] = {"exists": False}
            continue
        if (
            not isinstance(record, Mapping)
            or set(record) != {"exists", "mode", "sha256", "size"}
            or record.get("exists") is not True
            or type(record.get("mode")) is not int
            or not 0 <= record["mode"] <= 0o7777
            or type(record.get("size")) is not int
            or not 0 <= record["size"] <= MAX_PUBLIC_FILE_BYTES
            or type(record.get("sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) is None
        ):
            raise ValueError("public bundle manifest is invalid")
        normalized["files"][name] = dict(record)
    return normalized


def verify_bundle_state(
    data_dir: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
) -> None:
    """Require exact byte, mode, and presence equality with a manifest."""
    expected = _validate_bundle_manifest(manifest)
    actual = snapshot_public_bundle(Path(data_dir))
    if actual != expected:
        raise ValueError("{} public bundle drift detected".format(state))


def _manifest_sha(manifest: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()


def _copy_regular_private(source: Path, destination: Path, label: str, *, limit: int) -> dict[str, Any]:
    payload, source_mode = _read_regular_bytes(source, label, limit=limit)
    _write_exclusive(destination, payload, 0o600)
    return {
        "name": destination.name,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
        "source_mode": source_mode,
        "copy_mode": 0o600,
    }


def create_backup(
    data_dir: Path,
    launch_dir: Path,
    *,
    target_sha: str,
    previous_app_sha: str,
) -> dict[str, Any]:
    """Create one immutable private backup and its canonical manifest."""
    target_sha = _validate_sha(target_sha, "target")
    previous_app_sha = _validate_sha(previous_app_sha, "previous application")
    data_dir = Path(data_dir)
    launch_dir = Path(launch_dir)
    _require_directory(data_dir, "public data root")
    _create_private_directory(launch_dir, "launch root")
    backup_dir = launch_dir / "backup"
    if os.path.lexists(str(backup_dir)):
        raise FileExistsError("backup directory already exists")
    os.mkdir(str(backup_dir), 0o700)
    os.chmod(str(backup_dir), 0o700)
    baseline = snapshot_public_bundle(data_dir)
    copies = []
    try:
        for name in PUBLIC_BUNDLE_NAMES:
            record = baseline["files"][name]
            if not record["exists"]:
                continue
            copy_record = _copy_regular_private(
                data_dir / name,
                backup_dir / name,
                name,
                limit=MAX_PUBLIC_FILE_BYTES,
            )
            if (
                copy_record["sha256"] != record["sha256"]
                or copy_record["size"] != record["size"]
                or copy_record["source_mode"] != record["mode"]
            ):
                raise ValueError("backup source changed during copy")
            copies.append(copy_record)
        result = {
            "schema": BACKUP_SCHEMA,
            "target_sha": target_sha,
            "previous_app_sha": previous_app_sha,
            "baseline_sha256": _manifest_sha(baseline),
            "baseline": baseline,
            "copies": copies,
        }
        _write_exclusive(
            backup_dir / "manifest.json",
            canonical_json_bytes(result),
            0o600,
        )
        _fsync_directory(backup_dir)
        _fsync_directory(launch_dir)
        return result
    except BaseException:
        # Preserve partial private evidence for inspection; O_EXCL prevents a
        # retry from silently treating it as a complete backup.
        _fsync_directory(backup_dir)
        raise


def _path_binding(path: Path) -> dict[str, Any]:
    metadata = _require_directory(path, "staging root", mode=0o700)
    resolved = os.path.realpath(str(path))
    return {
        "sha256": hashlib.sha256(resolved.encode("utf-8")).hexdigest(),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def _require_fresh_sibling(data_dir: Path, stage_dir: Path) -> None:
    data_dir = Path(data_dir)
    stage_dir = Path(stage_dir)
    _require_directory(data_dir, "public data root")
    if not stage_dir.is_absolute():
        stage_dir = stage_dir.absolute()
    if os.path.lexists(str(stage_dir)):
        raise FileExistsError("staging root must be fresh and nonexisting")
    if os.path.realpath(str(stage_dir.parent)) != os.path.realpath(str(data_dir.parent)):
        raise ValueError("staging root must be a sibling of the live data root")
    data_real = os.path.realpath(str(data_dir))
    stage_real = os.path.realpath(str(stage_dir))
    if stage_real == data_real or stage_real.startswith(data_real + os.sep):
        raise ValueError("staging root cannot alias or descend from live data")


def prepare_stage_inputs(
    data_dir: Path,
    stage_dir: Path,
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    """Seed only immutable inputs into a fresh private sibling stage."""
    data_dir = Path(data_dir).absolute()
    stage_dir = Path(stage_dir).absolute()
    verify_bundle_state(data_dir, baseline, state="baseline")
    _require_fresh_sibling(data_dir, stage_dir)
    processed_dir = processed_dir_for(stage_dir)
    if os.path.lexists(str(processed_dir)):
        raise FileExistsError("staging processed root must be fresh and nonexisting")
    if os.path.realpath(str(processed_dir.parent)) != os.path.realpath(str(data_dir.parent)):
        raise ValueError("staging processed root must be a sibling")
    os.mkdir(str(stage_dir), 0o700)
    os.chmod(str(stage_dir), 0o700)
    try:
        os.mkdir(str(processed_dir), 0o700)
        os.chmod(str(processed_dir), 0o700)
    except BaseException:
        os.rmdir(str(stage_dir))
        raise
    copied = []
    try:
        for name in _REQUIRED_STAGE_INPUTS:
            source = data_dir / name
            destination = stage_dir / name
            copied.append(
                _copy_regular_private(
                    source,
                    destination,
                    name,
                    limit=(
                        MAX_PUBLIC_FILE_BYTES
                        if name == PUBLIC_BUNDLE_NAMES[0]
                        else MAX_INPUT_FILE_BYTES
                    ),
                )
            )
        verify_bundle_state(data_dir, baseline, state="baseline")
        _fsync_directory(stage_dir)
        _fsync_directory(processed_dir)
        return {
            "schema": STAGE_INPUT_SCHEMA,
            "baseline_sha256": _manifest_sha(_validate_bundle_manifest(baseline)),
            "stage_root_sha256": _path_binding(stage_dir)["sha256"],
            "processed_root_sha256": _path_binding(processed_dir)["sha256"],
            "stage_root": _path_binding(stage_dir),
            "processed_root": _path_binding(processed_dir),
            "inputs": copied,
        }
    except BaseException:
        # Do not recursively delete evidence after any bytes were copied.
        _fsync_directory(stage_dir)
        _fsync_directory(processed_dir)
        raise


def _read_csv_regular(path: Path, label: str) -> list[dict[str, str]]:
    payload, _mode = _read_regular_bytes(
        path,
        label,
        limit=MAX_PUBLIC_FILE_BYTES,
    )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("{} is not UTF-8".format(label)) from error
    return list(csv.DictReader(io.StringIO(text, newline="")))


def _read_canonical_json(path: Path, label: str, *, limit: int) -> dict[str, Any]:
    payload, _mode = _read_regular_bytes(path, label, limit=limit)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("{} is not canonical JSON".format(label)) from error
    if not isinstance(value, dict) or canonical_json_bytes(value) != payload:
        raise ValueError("{} is not canonical JSON".format(label))
    return value


def _validate_stage_candidate(stage_dir: Path) -> dict[str, Any]:
    """Re-run Task 4 raw and public receipt validation at promotion time."""
    stage_dir = Path(stage_dir)
    _require_directory(stage_dir, "staging root", mode=0o700)
    inventory = _read_csv_regular(
        stage_dir / "dex_pool_tvl_latest.csv",
        "staged DEX TVL inventory",
    )
    depth_rows = _read_csv_regular(
        stage_dir / "dex_depth_latest.csv",
        "staged DEX depth",
    )
    execution_rows = _read_csv_regular(
        stage_dir / "dex_execution_cost_latest.csv",
        "staged DEX execution cost",
    )
    public_receipt = _read_canonical_json(
        stage_dir / "uniswap_v3_exact_latest.json",
        "staged Uniswap V3 exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    raw_receipt = validate_uniswap_v3_exact_candidate(
        inventory,
        depth_rows,
        execution_rows,
        tvl_raw_root=stage_dir / "raw/tvl",
        depth_raw_root=stage_dir / "raw/dex-depth",
    )
    validated = validate_uniswap_v3_exact_public_receipt(
        public_receipt,
        depth_rows,
        execution_rows,
    )
    if uniswap_v3_exact_receipt_bytes(raw_receipt) != uniswap_v3_exact_receipt_bytes(validated):
        raise ValueError("staged raw and public exact receipts differ")
    snapshot_id = str(validated["depth_snapshot_id"])
    retained_path = (
        stage_dir
        / "raw/dex-depth"
        / snapshot_id
        / "uniswap_v3_exact_validation.json"
    )
    retained, _mode = _read_regular_bytes(
        retained_path,
        "staged retained exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    if retained != uniswap_v3_exact_receipt_bytes(validated):
        raise ValueError("staged retained exact receipt differs")
    return validated


def _validated_stage_private_receipt(
    stage_dir: Path,
) -> Tuple[dict[str, Any], bytes]:
    public_path = Path(stage_dir) / PUBLIC_BUNDLE_NAMES[-1]
    public_receipt = _read_canonical_json(
        public_path,
        "staged public exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    snapshot_id = public_receipt.get("depth_snapshot_id")
    if (
        type(snapshot_id) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", snapshot_id) is None
        or snapshot_id in {".", ".."}
    ):
        raise ValueError("staged exact receipt snapshot is invalid")
    public_bytes = canonical_json_bytes(public_receipt)
    retained_path = (
        Path(stage_dir)
        / "raw/dex-depth"
        / snapshot_id
        / RAW_RECEIPT_NAME
    )
    retained = _read_canonical_json(
        retained_path,
        "staged private exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    retained_bytes = canonical_json_bytes(retained)
    if retained_bytes != public_bytes:
        raise ValueError("staged private exact receipt differs from public receipt")
    return public_receipt, public_bytes


def _ensure_private_relative_directories(
    data_dir: Path,
    snapshot_id: str,
) -> Tuple[Path, list[Path]]:
    current = Path(data_dir)
    created = []
    for name in ("raw", "dex-depth", snapshot_id):
        current = current / name
        if os.path.lexists(str(current)):
            _require_directory(current, "trusted receipt directory")
        else:
            os.mkdir(str(current), 0o700)
            os.chmod(str(current), 0o700)
            created.append(current)
    return current, created


def _install_trusted_receipt(
    data_dir: Path,
    stage_dir: Path,
) -> Tuple[dict[str, Any], list[Path]]:
    public_receipt, payload = _validated_stage_private_receipt(stage_dir)
    snapshot_id = public_receipt["depth_snapshot_id"]
    destination_dir, created_directories = _ensure_private_relative_directories(
        data_dir,
        snapshot_id,
    )
    destination = destination_dir / RAW_RECEIPT_NAME
    created = False
    if os.path.lexists(str(destination)):
        existing, mode = _read_regular_bytes(
            destination,
            "live trusted exact receipt",
            limit=MAX_RECEIPT_BYTES,
        )
        if existing != payload:
            raise ValueError("live trusted exact receipt differs from candidate")
    else:
        _write_exclusive(destination, payload, 0o600)
        _fsync_directory(destination_dir)
        mode = 0o600
        created = True
    return (
        {
            "schema": "uniswap_v3_launch_trusted_receipt/v1",
            "snapshot_id": snapshot_id,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
            "mode": mode,
            "created": created,
        },
        created_directories,
    )


def _trusted_receipt_path(data_dir: Path, record: Mapping[str, Any]) -> Path:
    snapshot_id = record.get("snapshot_id")
    if (
        record.get("schema") != "uniswap_v3_launch_trusted_receipt/v1"
        or set(record) != {
            "schema", "snapshot_id", "sha256", "size", "mode", "created"
        }
        or type(snapshot_id) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", snapshot_id) is None
        or type(record.get("created")) is not bool
        or type(record.get("size")) is not int
        or type(record.get("mode")) is not int
        or type(record.get("sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) is None
    ):
        raise ValueError("trusted receipt record is invalid")
    return Path(data_dir) / "raw/dex-depth" / snapshot_id / RAW_RECEIPT_NAME


def _read_verified_trusted_receipt(
    data_dir: Path,
    record: Mapping[str, Any],
) -> Tuple[Path, bytes, int]:
    path = _trusted_receipt_path(data_dir, record)
    payload, mode = _read_regular_bytes(
        path,
        "live trusted exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    if (
        len(payload) != record["size"]
        or hashlib.sha256(payload).hexdigest() != record["sha256"]
        or mode != record["mode"]
    ):
        raise ValueError("trusted exact receipt drift detected")
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("trusted exact receipt drift detected") from error
    if not isinstance(parsed, dict) or canonical_json_bytes(parsed) != payload:
        raise ValueError("trusted exact receipt drift detected")
    return path, payload, mode


def _cleanup_created_trusted_receipt(
    data_dir: Path,
    record: Mapping[str, Any],
    created_directories: Iterable[Path],
) -> None:
    if not record.get("created"):
        return
    path, _payload, _mode = _read_verified_trusted_receipt(data_dir, record)
    os.unlink(str(path))
    _fsync_directory(path.parent)
    for directory in reversed(list(created_directories)):
        try:
            os.rmdir(str(directory))
        except OSError:
            break


def promote_stage(
    data_dir: Path,
    stage_dir: Path,
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    """CAS-promote the validated staged five-file candidate."""
    data_dir = Path(data_dir)
    stage_dir = Path(stage_dir)
    expected = _validate_bundle_manifest(baseline)
    verify_bundle_state(data_dir, expected, state="baseline")
    staged_candidate = snapshot_public_bundle(stage_dir)
    if any(
        not staged_candidate["files"][name]["exists"]
        for name in PUBLIC_BUNDLE_NAMES
    ):
        raise ValueError("staged candidate is incomplete")
    _validate_stage_candidate(stage_dir)
    # Re-read the canonical retained receipt independently of the Task 4
    # validator return value so it can be installed under the live trust root.
    _validated_stage_private_receipt(stage_dir)
    items = []
    for name in PUBLIC_BUNDLE_NAMES:
        payload, mode = _read_regular_bytes(
            stage_dir / name,
            "staged {}".format(name),
            limit=MAX_PUBLIC_FILE_BYTES,
        )
        staged_record = staged_candidate["files"][name]
        if (
            len(payload) != staged_record["size"]
            or hashlib.sha256(payload).hexdigest() != staged_record["sha256"]
            or mode != staged_record["mode"]
        ):
            raise ValueError("staged candidate drift detected")
        # Descriptor-check every live destination before the existing helper.
        record = expected["files"][name]
        if record["exists"]:
            _read_regular_bytes(
                data_dir / name,
                "live {}".format(name),
                limit=MAX_PUBLIC_FILE_BYTES,
            )
        elif os.path.lexists(str(data_dir / name)):
            raise ValueError("baseline public bundle drift detected")
        items.append((data_dir / name, payload))
    if snapshot_public_bundle(stage_dir) != staged_candidate:
        raise ValueError("staged candidate drift detected")
    verify_bundle_state(data_dir, expected, state="baseline")
    trusted_receipt, created_directories = _install_trusted_receipt(
        data_dir,
        stage_dir,
    )
    try:
        atomic_replace_bundle(items)
    except BaseException as promotion_error:
        try:
            _cleanup_created_trusted_receipt(
                data_dir,
                trusted_receipt,
                created_directories,
            )
        except BaseException as cleanup_error:
            raise RuntimeError(
                "public promotion failed and trusted receipt cleanup failed"
            ) from cleanup_error
        raise promotion_error
    promoted = snapshot_public_bundle(data_dir)
    return {
        "schema": PROMOTION_SCHEMA,
        "baseline_sha256": _manifest_sha(expected),
        "baseline": expected,
        "promoted_sha256": _manifest_sha(promoted),
        "promoted": promoted,
        "trusted_receipt": trusted_receipt,
    }


def _load_backup_manifest(backup_dir: Path) -> dict[str, Any]:
    backup_dir = Path(backup_dir)
    _require_directory(backup_dir, "backup root", mode=0o700)
    manifest = _read_canonical_json(
        backup_dir / "manifest.json",
        "backup manifest",
        limit=MAX_RECEIPT_BYTES,
    )
    if (
        manifest.get("schema") != BACKUP_SCHEMA
        or set(manifest) != {
            "schema",
            "target_sha",
            "previous_app_sha",
            "baseline_sha256",
            "baseline",
            "copies",
        }
    ):
        raise ValueError("backup manifest is invalid")
    baseline = _validate_bundle_manifest(manifest["baseline"])
    if manifest.get("baseline_sha256") != _manifest_sha(baseline):
        raise ValueError("backup manifest hash is invalid")
    _validate_sha(manifest.get("target_sha"), "target")
    _validate_sha(manifest.get("previous_app_sha"), "previous application")
    copies = manifest.get("copies")
    if not isinstance(copies, list):
        raise ValueError("backup manifest is invalid")
    by_name = {}
    for copy in copies:
        if (
            not isinstance(copy, dict)
            or set(copy) != {
                "name",
                "sha256",
                "size",
                "source_mode",
                "copy_mode",
            }
            or copy.get("name") in by_name
        ):
            raise ValueError("backup manifest is invalid")
        by_name[copy.get("name")] = copy
    for name in PUBLIC_BUNDLE_NAMES:
        record = baseline["files"][name]
        backup_path = backup_dir / name
        if not record["exists"]:
            if os.path.lexists(str(backup_path)):
                raise ValueError("absent backup file was fabricated")
            continue
        payload, mode = _read_regular_bytes(
            backup_path,
            "backup {}".format(name),
            limit=MAX_PUBLIC_FILE_BYTES,
        )
        if (
            mode != 0o600
            or hashlib.sha256(payload).hexdigest() != record["sha256"]
            or len(payload) != record["size"]
            or by_name.get(name) != {
                "name": name,
                "sha256": record["sha256"],
                "size": record["size"],
                "source_mode": record["mode"],
                "copy_mode": 0o600,
            }
        ):
            raise ValueError("backup file does not match manifest")
    return manifest


def _replace_or_remove_bundle(
    changes: Iterable[Tuple[Path, Optional[bytes], Optional[int]]],
    expected_promoted: Mapping[str, Any],
    *,
    trusted_removal: Optional[Tuple[Path, bytes, int]] = None,
) -> None:
    """Replace or remove a fixed bundle, rolling back ordinary I/O errors."""
    normalized = list(changes)
    if [path.name for path, _payload, _mode in normalized] != list(PUBLIC_BUNDLE_NAMES):
        raise ValueError("restore transaction must use the fixed public bundle order")
    expected = _validate_bundle_manifest(expected_promoted)
    transaction_changes = list(normalized)
    trusted_path = None
    trusted_expected = None
    if trusted_removal is not None:
        trusted_path, trusted_payload, trusted_mode = trusted_removal
        transaction_changes.append((trusted_path, None, None))
        trusted_expected = {
            "size": len(trusted_payload),
            "sha256": hashlib.sha256(trusted_payload).hexdigest(),
            "mode": trusted_mode,
        }
    transaction_id = uuid.uuid4().hex
    staged = {}
    rollback = {}
    committed = []
    try:
        for path, payload, mode in transaction_changes:
            current_payload, current_mode = _read_regular_bytes(
                path,
                "promoted {}".format(path.name),
                limit=MAX_PUBLIC_FILE_BYTES,
            )
            current_record = (
                trusted_expected
                if trusted_path is not None and path == trusted_path
                else expected["files"][path.name]
            )
            if (
                ("exists" in current_record and not current_record["exists"])
                or len(current_payload) != current_record["size"]
                or hashlib.sha256(current_payload).hexdigest()
                != current_record["sha256"]
                or current_mode != current_record["mode"]
            ):
                raise ValueError("promoted public bundle drift detected")
            rollback_path = path.with_name(
                ".{}.{}.restore-backup".format(path.name, transaction_id)
            )
            _write_exclusive(rollback_path, current_payload, current_mode)
            rollback[path] = rollback_path
            if payload is not None:
                assert mode is not None
                stage_path = path.with_name(
                    ".{}.{}.restore-stage".format(path.name, transaction_id)
                )
                _write_exclusive(stage_path, payload, mode)
                staged[path] = stage_path
        for path, payload, _mode in transaction_changes:
            if payload is None:
                os.unlink(str(path))
            else:
                os.replace(str(staged[path]), str(path))
                staged[path] = None
            committed.append(path)
        for directory in {path.parent for path, _payload, _mode in transaction_changes}:
            _fsync_directory(directory)
    except BaseException:
        rollback_errors = []
        for path in reversed(committed):
            try:
                os.replace(str(rollback[path]), str(path))
                rollback[path] = None
            except OSError as error:
                rollback_errors.append(error)
        for directory in {path.parent for path, _payload, _mode in transaction_changes}:
            _fsync_directory(directory)
        if rollback_errors:
            raise RuntimeError(
                "restore failed and rollback could not restore every promoted file"
            ) from rollback_errors[0]
        raise
    finally:
        for path in list(staged.values()) + list(rollback.values()):
            if path is not None:
                try:
                    os.unlink(str(path))
                except FileNotFoundError:
                    pass


def restore_backup(
    data_dir: Path,
    backup_dir: Path,
    promotion: Mapping[str, Any],
) -> dict[str, Any]:
    """CAS-restore the checksummed backup, including sidecar absence."""
    data_dir = Path(data_dir)
    backup_dir = Path(backup_dir)
    if (
        not isinstance(promotion, Mapping)
        or promotion.get("schema") != PROMOTION_SCHEMA
        or set(promotion) != {
            "schema",
            "baseline_sha256",
            "baseline",
            "promoted_sha256",
            "promoted",
            "trusted_receipt",
        }
    ):
        raise ValueError("promotion receipt is invalid")
    promoted = _validate_bundle_manifest(promotion["promoted"])
    baseline = _validate_bundle_manifest(promotion["baseline"])
    if (
        promotion.get("promoted_sha256") != _manifest_sha(promoted)
        or promotion.get("baseline_sha256") != _manifest_sha(baseline)
    ):
        raise ValueError("promotion receipt hash is invalid")
    verify_bundle_state(data_dir, promoted, state="promoted")
    trusted_record = promotion["trusted_receipt"]
    trusted_path, trusted_payload, trusted_mode = _read_verified_trusted_receipt(
        data_dir,
        trusted_record,
    )
    promoted_sidecar, _sidecar_mode = _read_regular_bytes(
        data_dir / PUBLIC_BUNDLE_NAMES[-1],
        "promoted public exact receipt",
        limit=MAX_RECEIPT_BYTES,
    )
    if promoted_sidecar != trusted_payload:
        raise ValueError("trusted exact receipt drift detected")
    backup = _load_backup_manifest(backup_dir)
    if (
        backup["baseline"] != baseline
        or backup["baseline_sha256"] != promotion["baseline_sha256"]
    ):
        raise ValueError("backup and promotion baseline differ")
    changes = []
    for name in PUBLIC_BUNDLE_NAMES:
        record = baseline["files"][name]
        if record["exists"]:
            payload, backup_mode = _read_regular_bytes(
                backup_dir / name,
                "backup {}".format(name),
                limit=MAX_PUBLIC_FILE_BYTES,
            )
            if backup_mode != 0o600:
                raise ValueError("backup file mode is invalid")
            changes.append((data_dir / name, payload, record["mode"]))
        else:
            changes.append((data_dir / name, None, None))
    verify_bundle_state(data_dir, promoted, state="promoted")
    _read_verified_trusted_receipt(data_dir, trusted_record)
    _replace_or_remove_bundle(
        changes,
        promoted,
        trusted_removal=(trusted_path, trusted_payload, trusted_mode)
        if trusted_record["created"]
        else None,
    )
    verify_bundle_state(data_dir, baseline, state="restored")
    if trusted_record["created"]:
        if os.path.lexists(str(trusted_path)):
            raise ValueError("launch-created trusted receipt was not removed")
        trusted_status = "removed"
        for directory in (
            trusted_path.parent,
            trusted_path.parent.parent,
            trusted_path.parent.parent.parent,
        ):
            try:
                os.rmdir(str(directory))
            except OSError:
                break
    else:
        _read_verified_trusted_receipt(data_dir, trusted_record)
        trusted_status = "preserved"
    return {
        "schema": RESTORE_SCHEMA,
        "promotion_sha256": _manifest_sha(promoted),
        "restored_sha256": _manifest_sha(baseline),
        "restored": baseline,
        "trusted_receipt": {
            "sha256": trusted_record["sha256"],
            "status": trusted_status,
        },
    }


def write_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    """Write one canonical 0600 receipt exactly once."""
    path = Path(path)
    _require_directory(path.parent, "receipt root", mode=0o700)
    payload = canonical_json_bytes(receipt)
    if len(payload) > MAX_RECEIPT_BYTES:
        raise ValueError("receipt exceeds the bounded write limit")
    _write_exclusive(path, payload, 0o600)
    _fsync_directory(path.parent)


def read_receipt(path: Path) -> dict[str, Any]:
    """Read one descriptor-checked canonical receipt."""
    return _read_canonical_json(Path(path), "launch receipt", limit=MAX_RECEIPT_BYTES)


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class LaunchConfig:
    data_dir: Path
    launch_dir: Path
    stage_dir: Path
    target_sha: str
    previous_app_sha: str
    live_base_url: str = "http://127.0.0.1:8765"
    stage_port: int = 18765

    def normalized(self) -> "LaunchConfig":
        target_sha = _validate_sha(self.target_sha, "target")
        previous_app_sha = _validate_sha(
            self.previous_app_sha,
            "previous application",
        )
        if not re.fullmatch(r"http://127\.0\.0\.1:[0-9]{1,5}", self.live_base_url):
            raise ValueError("live base URL must be an explicit loopback HTTP endpoint")
        if not 1 <= int(self.stage_port) <= 65535:
            raise ValueError("stage port is invalid")
        return LaunchConfig(
            data_dir=Path(self.data_dir).expanduser().absolute(),
            launch_dir=Path(self.launch_dir).expanduser().absolute(),
            stage_dir=Path(self.stage_dir).expanduser().absolute(),
            target_sha=target_sha,
            previous_app_sha=previous_app_sha,
            live_base_url=self.live_base_url,
            stage_port=int(self.stage_port),
        )


class SubprocessRunner:
    """Small deterministic boundary for foreground and transient commands."""

    @staticmethod
    def _environment(overrides: Optional[Mapping[str, Optional[str]]]) -> dict[str, str]:
        environment = dict(os.environ)
        for key, value in (overrides or {}).items():
            if value is None:
                environment.pop(key, None)
            else:
                environment[key] = value
        return environment

    def run(
        self,
        command: Iterable[str],
        *,
        env: Optional[Mapping[str, Optional[str]]] = None,
    ) -> CommandResult:
        completed = subprocess.run(
            list(command),
            cwd=PROJECT_ROOT,
            env=self._environment(env),
            capture_output=True,
            text=True,
            check=False,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)

    def start(
        self,
        command: Iterable[str],
        *,
        env: Optional[Mapping[str, Optional[str]]] = None,
    ) -> subprocess.Popen:
        return subprocess.Popen(
            list(command),
            cwd=PROJECT_ROOT,
            env=self._environment(env),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _checked_run(
    runner: Any,
    command: Iterable[str],
    *,
    env: Optional[Mapping[str, Optional[str]]] = None,
    accepted: Tuple[int, ...] = (0,),
    label: str,
) -> CommandResult:
    result = runner.run(list(command), env=env)
    if result.returncode not in accepted:
        raise RuntimeError("{} failed with exit {}".format(label, result.returncode))
    return result


def _json_command_evidence(result: CommandResult, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("{} did not return JSON evidence".format(label)) from error
    if not isinstance(payload, dict):
        raise ValueError("{} did not return JSON evidence".format(label))
    return payload


@contextmanager
def _live_collection_lock(data_dir: Path) -> Iterable[None]:
    """Hold the live collector flock for an entire live-state phase."""
    data_dir = Path(data_dir)
    _require_directory(data_dir, "public data root")
    collection_dir = data_dir / "collection"
    if os.path.lexists(str(collection_dir)):
        _require_directory(collection_dir, "collection lock root")
    else:
        os.mkdir(str(collection_dir), 0o700)
        os.chmod(str(collection_dir), 0o700)
    lock_path = collection_dir / "collection.lock"
    if os.path.lexists(str(lock_path)):
        before = os.lstat(str(lock_path))
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise ValueError("collection lock must be a regular non-symlink file")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(str(lock_path), flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("collection lock must be a regular non-symlink file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("live collection lock is held") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _systemd_state(runner: Any, operation: str, unit: str) -> str:
    if unit not in MANAGED_UNITS or operation not in {"is-enabled", "is-active"}:
        raise ValueError("unsupported systemd query")
    result = _checked_run(
        runner,
        ["systemctl", "--user", operation, unit],
        accepted=(0, 1, 3, 4),
        label="systemd {} {}".format(operation, unit),
    )
    value = result.stdout.strip()
    allowed = (
        {"enabled", "disabled"}
        if operation == "is-enabled"
        else {"active", "inactive"}
    )
    if value not in allowed:
        raise ValueError("unsupported {} state for {}".format(operation, unit))
    return value


def _capture_timer_states(runner: Any) -> dict[str, dict[str, str]]:
    return {
        unit: {
            "active": _systemd_state(runner, "is-active", unit),
            "enabled": _systemd_state(runner, "is-enabled", unit),
        }
        for unit in TIMER_UNITS
    }


def _require_paused(runner: Any) -> None:
    for unit in TIMER_UNITS:
        if _systemd_state(runner, "is-enabled", unit) != "disabled":
            raise RuntimeError("managed collection timers are not disabled")
        if _systemd_state(runner, "is-active", unit) != "inactive":
            raise RuntimeError("managed collection timers are not inactive")
    for unit in SERVICE_UNITS:
        if _systemd_state(runner, "is-active", unit) != "inactive":
            raise RuntimeError("managed collection services are not inactive")


def _phase_predecessor(phase: str, launch_dir: Path) -> Optional[str]:
    if phase == "preflight":
        return None
    if phase == "resume":
        restore_path = launch_dir / RECEIPT_FILES["restore"]
        return "restore" if os.path.lexists(str(restore_path)) else "promote"
    return PHASES[PHASES.index(phase) - 1]


def _receipt_sha(receipt: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(receipt)).hexdigest()


def _load_predecessor(
    phase: str,
    config: LaunchConfig,
) -> Optional[dict[str, Any]]:
    receipt_path = config.launch_dir / RECEIPT_FILES[phase]
    if os.path.lexists(str(receipt_path)):
        raise FileExistsError("{} phase is already completed".format(phase))
    for later_phase in PHASES[PHASES.index(phase) + 1:]:
        if os.path.lexists(str(config.launch_dir / RECEIPT_FILES[later_phase])):
            raise ValueError("launch receipt ledger order is invalid")
    predecessor_phase = _phase_predecessor(phase, config.launch_dir)
    if predecessor_phase is None:
        return None
    predecessor_path = config.launch_dir / RECEIPT_FILES[predecessor_phase]
    if not os.path.lexists(str(predecessor_path)):
        raise ValueError("{} predecessor receipt is missing".format(phase))

    chain = []
    for candidate_phase in PHASES:
        if candidate_phase == "restore" and predecessor_phase == "promote":
            continue
        candidate_path = config.launch_dir / RECEIPT_FILES[candidate_phase]
        if not os.path.lexists(str(candidate_path)):
            if candidate_phase == predecessor_phase:
                raise ValueError("{} predecessor receipt is missing".format(phase))
            continue
        receipt = read_receipt(candidate_path)
        if (
            receipt.get("schema") != RECEIPT_SCHEMA
            or receipt.get("phase") != candidate_phase
            or receipt.get("target_sha") != config.target_sha
            or receipt.get("previous_app_sha") != config.previous_app_sha
        ):
            raise ValueError("launch receipt chain or target SHA is invalid")
        expected_predecessor = chain[-1] if chain else None
        expected_hash = _receipt_sha(expected_predecessor) if expected_predecessor else None
        if receipt.get("predecessor_receipt_sha256") != expected_hash:
            raise ValueError("launch receipt chain is invalid")
        chain.append(receipt)
        if candidate_phase == predecessor_phase:
            return receipt
    raise ValueError("{} predecessor receipt is missing".format(phase))


def _base_receipt(
    phase: str,
    config: LaunchConfig,
    predecessor: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": RECEIPT_SCHEMA,
        "phase": phase,
        "predecessor_receipt_sha256": (
            _receipt_sha(predecessor) if predecessor is not None else None
        ),
        "target_sha": config.target_sha,
        "previous_app_sha": config.previous_app_sha,
    }


def _write_phase_receipt(
    phase: str,
    config: LaunchConfig,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    _assert_portable_receipt(receipt)
    write_receipt(config.launch_dir / RECEIPT_FILES[phase], receipt)
    return dict(receipt)


def _assert_portable_receipt(value: Any, *, key: str = "") -> None:
    forbidden_keys = ("path", "environment", "rpc", "secret", "password", "url")
    if isinstance(value, Mapping):
        for child_key, child in value.items():
            lower = str(child_key).lower()
            if any(fragment in lower for fragment in forbidden_keys):
                raise ValueError("launch receipt contains forbidden private metadata")
            _assert_portable_receipt(child, key=lower)
    elif isinstance(value, list):
        for child in value:
            _assert_portable_receipt(child, key=key)
    elif isinstance(value, str):
        lowered = value.lower()
        if os.path.isabs(value) or "://" in value or "rpc" in lowered:
            raise ValueError("launch receipt contains forbidden private metadata")


def _preflight(config: LaunchConfig, runner: Any) -> dict[str, Any]:
    if os.path.lexists(str(config.launch_dir)):
        raise FileExistsError("launch root must be fresh and nonexisting")
    _require_fresh_sibling(config.data_dir, config.stage_dir)
    checkout = _checked_run(
        runner,
        ["git", "rev-parse", "HEAD"],
        label="target checkout verification",
    ).stdout.strip().lower()
    if checkout != config.target_sha:
        raise ValueError("target checkout SHA does not match")
    with _live_collection_lock(config.data_dir):
        baseline = snapshot_public_bundle(config.data_dir)
        inputs = []
        for name in _REQUIRED_STAGE_INPUTS[:2]:
            payload, mode = _read_regular_bytes(
                config.data_dir / name,
                name,
                limit=MAX_INPUT_FILE_BYTES,
            )
            inputs.append({
                "name": name,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
                "mode": mode,
            })
        health_result = _checked_run(
            runner,
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts/check_dashboard_health.py"),
                "--url",
                config.live_base_url + "/health",
            ],
            label="live health verification",
        )
        health = _json_command_evidence(health_result, "live health verification")
        if (
            health.get("status") != "ok"
            or health.get("data_ready") is not True
            or health.get("data_status") != "current"
            or health.get("application_sha") != config.previous_app_sha
        ):
            raise ValueError("live health or application SHA is not current")
    os.mkdir(str(config.launch_dir), 0o700)
    os.chmod(str(config.launch_dir), 0o700)
    receipt = _base_receipt("preflight", config, None)
    receipt.update({
        "baseline": baseline,
        "baseline_sha256": _manifest_sha(baseline),
        "required_inputs": inputs,
        "live_health": {
            "application_sha": config.previous_app_sha,
            "data_ready": True,
            "data_status": "current",
            "status": "ok",
        },
        "managed_units": list(MANAGED_UNITS),
    })
    return _write_phase_receipt("preflight", config, receipt)


def _pause(config: LaunchConfig, runner: Any, predecessor: Mapping[str, Any]) -> dict[str, Any]:
    timer_states = _capture_timer_states(runner)
    try:
        for unit in TIMER_UNITS:
            _checked_run(
                runner,
                ["systemctl", "--user", "disable", "--now", unit],
                label="disable managed timer",
            )
        for unit in SERVICE_UNITS:
            _checked_run(
                runner,
                ["systemctl", "--user", "stop", unit],
                label="stop managed collection service",
            )
        _require_paused(runner)
        with _live_collection_lock(config.data_dir):
            pass
    except BaseException as phase_error:
        try:
            _restore_timer_states(runner, timer_states)
        except BaseException as restore_error:
            raise RuntimeError(
                "pause failed and captured timer states could not be restored"
            ) from restore_error
        raise phase_error
    receipt = _base_receipt("pause", config, predecessor)
    receipt.update({
        "timer_states": timer_states,
        "services": {unit: "inactive" for unit in SERVICE_UNITS},
        "collection_lock": "verified_unheld",
    })
    return _write_phase_receipt("pause", config, receipt)


def _backup(config: LaunchConfig, runner: Any, predecessor: Mapping[str, Any]) -> dict[str, Any]:
    _require_paused(runner)
    preflight = read_receipt(config.launch_dir / RECEIPT_FILES["preflight"])
    with _live_collection_lock(config.data_dir):
        verify_bundle_state(config.data_dir, preflight["baseline"], state="baseline")
        backup = create_backup(
            config.data_dir,
            config.launch_dir,
            target_sha=config.target_sha,
            previous_app_sha=config.previous_app_sha,
        )
    receipt = _base_receipt("backup", config, predecessor)
    receipt.update({
        "backup_manifest_sha256": hashlib.sha256(
            canonical_json_bytes(backup)
        ).hexdigest(),
        "baseline": backup["baseline"],
        "baseline_sha256": backup["baseline_sha256"],
    })
    return _write_phase_receipt("backup", config, receipt)


def _verify_required_inputs(
    data_dir: Path,
    records: Any,
) -> None:
    if not isinstance(records, list) or [
        record.get("name") if isinstance(record, Mapping) else None
        for record in records
    ] != list(_REQUIRED_STAGE_INPUTS[:2]):
        raise ValueError("required input manifest is invalid")
    for record in records:
        if set(record) != {"name", "sha256", "size", "mode"}:
            raise ValueError("required input manifest is invalid")
        payload, mode = _read_regular_bytes(
            Path(data_dir) / record["name"],
            record["name"],
            limit=MAX_INPUT_FILE_BYTES,
        )
        if (
            len(payload) != record["size"]
            or hashlib.sha256(payload).hexdigest() != record["sha256"]
            or mode != record["mode"]
        ):
            raise ValueError("required input drift detected")


def _stage(config: LaunchConfig, runner: Any, predecessor: Mapping[str, Any]) -> dict[str, Any]:
    _require_paused(runner)
    baseline = predecessor["baseline"]
    preflight = read_receipt(config.launch_dir / RECEIPT_FILES["preflight"])
    with _live_collection_lock(config.data_dir):
        _verify_required_inputs(
            config.data_dir,
            preflight.get("required_inputs"),
        )
        inputs = prepare_stage_inputs(config.data_dir, config.stage_dir, baseline)
        command = [
            sys.executable,
            str(PROJECT_ROOT / "scripts/run_collection_cycle.py"),
            "--profile",
            "dex_depth",
            "--publish-local",
            "--data-dir",
            str(config.stage_dir),
            "--require-uniswap-v3-exact-validation",
        ]
        _checked_run(
            runner,
            command,
            env={
                "MARKET_DATA_DIR": str(config.stage_dir),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            label="staged full DEX depth collection",
        )
        candidate = snapshot_public_bundle(config.stage_dir)
    receipt = _base_receipt("stage", config, predecessor)
    receipt.update({
        "baseline": baseline,
        "baseline_sha256": predecessor["baseline_sha256"],
        "candidate": candidate,
        "candidate_sha256": _manifest_sha(candidate),
        "stage_roots": {
            "data": _path_binding(config.stage_dir),
            "processed": _path_binding(processed_dir_for(config.stage_dir)),
        },
        "input_manifest_sha256": hashlib.sha256(
            canonical_json_bytes(inputs)
        ).hexdigest(),
        "collection": {
            "profile": "dex_depth",
            "publish_local": True,
            "require_uniswap_v3_exact_validation": True,
            "status": "passed",
        },
    })
    return _write_phase_receipt("stage", config, receipt)


def _verify_bound_stage(config: LaunchConfig, stage_receipt: Mapping[str, Any]) -> None:
    expected_roots = stage_receipt.get("stage_roots")
    actual_roots = {
        "data": _path_binding(config.stage_dir),
        "processed": _path_binding(processed_dir_for(config.stage_dir)),
    }
    if expected_roots != actual_roots:
        raise ValueError("staged roots drift detected")
    try:
        verify_bundle_state(
            config.stage_dir,
            stage_receipt["candidate"],
            state="staged candidate",
        )
    except (KeyError, ValueError) as error:
        raise ValueError("staged candidate drift detected") from error


def _release_command(base_url: str, expected_sha: str) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts/check_dashboard_release.py"),
        "--base-url",
        base_url,
        "--expected-application-sha",
        expected_sha,
    ]


def _verified_release(
    runner: Any,
    *,
    base_url: str,
    expected_sha: str,
    retries: int = 1,
) -> dict[str, Any]:
    last_result = None
    for attempt in range(retries):
        result = runner.run(_release_command(base_url, expected_sha), env=None)
        last_result = result
        if result.returncode == 0:
            evidence = _json_command_evidence(result, "release verification")
            if evidence.get("application_sha") != expected_sha:
                raise ValueError("release evidence application SHA differs")
            return {
                "application_sha": expected_sha,
                "status": "passed",
            }
        if attempt + 1 < retries:
            time.sleep(0.25)
    assert last_result is not None
    raise RuntimeError(
        "release verification failed with exit {}".format(last_result.returncode)
    )


def _verified_rollback_health(
    runner: Any,
    *,
    base_url: str,
    expected_sha: str,
) -> dict[str, Any]:
    """Verify the restored pre-V3 application without requiring V3 health."""
    result = _checked_run(
        runner,
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts/check_dashboard_health.py"),
            "--url",
            base_url + "/health",
        ],
        label="rollback health verification",
    )
    health = _json_command_evidence(result, "rollback health verification")
    if (
        health.get("status") != "ok"
        or health.get("data_ready") is not True
        or health.get("data_status") != "current"
        or health.get("application_sha") != expected_sha
    ):
        raise ValueError("rollback health or application SHA is not current")
    return {"application_sha": expected_sha, "status": "passed"}


def _verify_stage(
    config: LaunchConfig,
    runner: Any,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    _require_paused(runner)
    _verify_bound_stage(config, predecessor)
    _validate_stage_candidate(config.stage_dir)
    environment = {
        "MARKET_DATA_DIR": str(config.data_dir),
        "MARKET_CEX_DATA": None,
        "MARKET_DEX_DATA": None,
        "MARKET_DATABASE": None,
        "MARKET_DEX_DEPTH_DATA": str(config.stage_dir / "dex_depth_latest.csv"),
        "MARKET_DEX_EXECUTION_COST_DATA": str(
            config.stage_dir / "dex_execution_cost_latest.csv"
        ),
        "MARKET_UNISWAP_V3_EXACT_DATA": str(
            config.stage_dir / "uniswap_v3_exact_latest.json"
        ),
        "MARKET_UNISWAP_V3_EXACT_RAW_ROOT": str(
            config.stage_dir / "raw/dex-depth"
        ),
        "CEX_DEX_RELEASE_SHA": config.target_sha,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    with _live_collection_lock(config.data_dir):
        process = runner.start(
            [
                sys.executable,
                str(PROJECT_ROOT / "dashboard/server.py"),
                "--host",
                "127.0.0.1",
                "--port",
                str(config.stage_port),
            ],
            env=environment,
        )
        try:
            evidence = _verified_release(
                runner,
                base_url="http://127.0.0.1:{}".format(config.stage_port),
                expected_sha=config.target_sha,
                retries=20,
            )
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    receipt = _base_receipt("verify-stage", config, predecessor)
    receipt.update({
        "candidate": predecessor["candidate"],
        "candidate_sha256": predecessor["candidate_sha256"],
        "stage_roots": predecessor["stage_roots"],
        "release_evidence": evidence,
    })
    return _write_phase_receipt("verify-stage", config, receipt)


def _promote(
    config: LaunchConfig,
    runner: Any,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    _require_paused(runner)
    _verify_bound_stage(config, predecessor)
    backup_receipt = read_receipt(config.launch_dir / RECEIPT_FILES["backup"])
    with _live_collection_lock(config.data_dir):
        _require_paused(runner)
        promotion = promote_stage(
            config.data_dir,
            config.stage_dir,
            backup_receipt["baseline"],
        )
    receipt = _base_receipt("promote", config, predecessor)
    receipt.update({
        "promotion": promotion,
        "promotion_sha256": hashlib.sha256(
            canonical_json_bytes(promotion)
        ).hexdigest(),
        "dashboard_management": "external_operator_boundary",
    })
    return _write_phase_receipt("promote", config, receipt)


def _restore(
    config: LaunchConfig,
    runner: Any,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    _require_paused(runner)
    with _live_collection_lock(config.data_dir):
        _require_paused(runner)
        restored = restore_backup(
            config.data_dir,
            config.launch_dir / "backup",
            predecessor["promotion"],
        )
    receipt = _base_receipt("restore", config, predecessor)
    receipt.update({
        "restore": restored,
        "rollback_application_sha": config.previous_app_sha,
        "dashboard_management": "external_operator_boundary",
    })
    return _write_phase_receipt("restore", config, receipt)


def _timer_states_from_pause(config: LaunchConfig) -> dict[str, dict[str, str]]:
    pause = read_receipt(config.launch_dir / RECEIPT_FILES["pause"])
    states = pause.get("timer_states")
    if not isinstance(states, dict) or set(states) != set(TIMER_UNITS):
        raise ValueError("recorded timer states are invalid")
    for unit, state_value in states.items():
        if state_value not in (
            {"enabled": "enabled", "active": "active"},
            {"enabled": "enabled", "active": "inactive"},
            {"enabled": "disabled", "active": "active"},
            {"enabled": "disabled", "active": "inactive"},
        ):
            raise ValueError("recorded timer states are invalid")
    return states


def _restore_timer_states(runner: Any, states: Mapping[str, Mapping[str, str]]) -> None:
    for unit in TIMER_UNITS:
        enabled_command = "enable" if states[unit]["enabled"] == "enabled" else "disable"
        _checked_run(
            runner,
            ["systemctl", "--user", enabled_command, unit],
            label="restore timer enabled state",
        )
        active_command = "start" if states[unit]["active"] == "active" else "stop"
        _checked_run(
            runner,
            ["systemctl", "--user", active_command, unit],
            label="restore timer active state",
        )
    actual = _capture_timer_states(runner)
    if actual != states:
        raise RuntimeError("timer states were not restored exactly")


def _resume(
    config: LaunchConfig,
    runner: Any,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    _require_paused(runner)
    expected_sha = (
        config.previous_app_sha
        if predecessor.get("phase") == "restore"
        else config.target_sha
    )
    states = _timer_states_from_pause(config)
    with _live_collection_lock(config.data_dir):
        _require_paused(runner)
        if predecessor.get("phase") == "restore":
            evidence = _verified_rollback_health(
                runner,
                base_url=config.live_base_url,
                expected_sha=expected_sha,
            )
        else:
            promotion = predecessor.get("promotion")
            if not isinstance(promotion, Mapping):
                raise ValueError("forward promotion receipt is invalid")
            verify_bundle_state(
                config.data_dir,
                promotion.get("promoted"),
                state="promoted",
            )
            _trusted_path, trusted_payload, _trusted_mode = (
                _read_verified_trusted_receipt(
                    config.data_dir,
                    promotion.get("trusted_receipt"),
                )
            )
            public_sidecar, _public_mode = _read_regular_bytes(
                config.data_dir / PUBLIC_BUNDLE_NAMES[-1],
                "promoted public exact receipt",
                limit=MAX_RECEIPT_BYTES,
            )
            if public_sidecar != trusted_payload:
                raise ValueError("trusted exact receipt drift detected")
            evidence = _verified_release(
                runner,
                base_url=config.live_base_url,
                expected_sha=expected_sha,
            )
        _restore_timer_states(runner, states)
    receipt = _base_receipt("resume", config, predecessor)
    receipt.update({
        "release_evidence": evidence,
        "restored_timer_states": states,
    })
    return _write_phase_receipt("resume", config, receipt)


def execute_phase(phase: str, config: LaunchConfig, runner: Any = None) -> dict[str, Any]:
    """Execute exactly one receipt-bound launch phase."""
    if phase not in PHASES:
        raise ValueError("unknown launch phase")
    config = config.normalized()
    runner = runner or SubprocessRunner()
    predecessor = _load_predecessor(phase, config)
    if phase == "preflight":
        return _preflight(config, runner)
    assert predecessor is not None
    functions = {
        "pause": _pause,
        "backup": _backup,
        "stage": _stage,
        "verify-stage": _verify_stage,
        "promote": _promote,
        "restore": _restore,
        "resume": _resume,
    }
    return functions[phase](config, runner, predecessor)


def build_plan(phase: str) -> dict[str, Any]:
    """Return a portable plan without touching paths, processes, or network."""
    if phase not in PHASES:
        raise ValueError("unknown launch phase")
    return {
        "schema": "uniswap_v3_launch_plan/v1",
        "phase": phase,
        "execute": False,
        "public_bundle": list(PUBLIC_BUNDLE_NAMES),
        "managed_units": list(MANAGED_UNITS),
        "state_changes": "none",
        "notes": [
            "Execution requires --execute and one canonical predecessor receipt.",
            "The tool never switches the application checkout or edits service files.",
        ],
    }


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--launch-dir", type=Path, required=True)
    parser.add_argument("--stage-dir", type=Path, required=True)
    parser.add_argument("--target-sha", required=True)
    parser.add_argument("--previous-app-sha", required=True)
    parser.add_argument("--live-base-url", default="http://127.0.0.1:8765")
    parser.add_argument("--stage-port", type=int, default=18765)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Optional[Iterable[str]] = None,
    *,
    runner: Any = None,
    stdout: Optional[TextIO] = None,
) -> int:
    args = parse_args(argv)
    output = stdout or sys.stdout
    if not args.execute:
        output.write(canonical_json_bytes(build_plan(args.phase)).decode("utf-8"))
        return 0
    config = LaunchConfig(
        data_dir=args.data_dir,
        launch_dir=args.launch_dir,
        stage_dir=args.stage_dir,
        target_sha=args.target_sha,
        previous_app_sha=args.previous_app_sha,
        live_base_url=args.live_base_url,
        stage_port=args.stage_port,
    )
    receipt = execute_phase(args.phase, config, runner)
    output.write(canonical_json_bytes({
        "phase": receipt["phase"],
        "receipt_sha256": _receipt_sha(receipt),
        "status": "recorded",
    }).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
