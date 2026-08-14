"""Descriptor-safe bounded evidence for the shared collection lock."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

try:
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


SHADOW_LOCK_OWNER_SCHEMA = "route_shadow_collection_lock_owner/v1"
PRIMARY_CONTENTION_SCHEMA = "route_shadow_primary_contention/v1"
PRIMARY_CONTENTION_OVERFLOW_SCHEMA = (
    "route_shadow_primary_contention_overflow/v1"
)
PRIMARY_CONTENTION_CAP_BYTES = 4 * 1024 * 1024
PRIMARY_CONTENTION_OVERFLOW_RESERVE_BYTES = 4 * 1024
PRIMARY_CONTENTION_MAX_RECEIPT_BYTES = 2 * 1024
PRIMARY_CONTENTION_DATA_BYTES = (
    PRIMARY_CONTENTION_CAP_BYTES - PRIMARY_CONTENTION_OVERFLOW_RESERVE_BYTES
)
PRIMARY_RUN_SCHEMA = "route_shadow_primary_run/v1"
PRIMARY_COLLECTION_MANIFEST_PROJECTION_SCHEMA = (
    "route_primary_collection_manifest_projection/v1"
)
PRIMARY_RUN_OVERFLOW_SCHEMA = "route_shadow_primary_run_overflow/v1"
PRIMARY_RUN_CAP_BYTES = 1024 * 1024
PRIMARY_RUN_OVERFLOW_RESERVE_BYTES = 4 * 1024
PRIMARY_RUN_MAX_RECEIPT_BYTES = 4 * 1024
PRIMARY_RUN_MAX_PROJECTION_BYTES = 2 * 1024
PRIMARY_RUN_DATA_BYTES = PRIMARY_RUN_CAP_BYTES - PRIMARY_RUN_OVERFLOW_RESERVE_BYTES

_OWNER_FIELDS = frozenset({
    "schema", "owner_kind", "run_id", "boot_id", "nonce",
})
_CONTENTION_FIELDS = frozenset({
    "schema", "attribution_status", "holder_run_id", "holder_boot_id",
    "holder_nonce", "primary_profile", "primary_invocation_id",
    "observed_at", "lock_identity",
})
_OVERFLOW_FIELDS = frozenset({
    "schema", "first_rejected_invocation_id", "observed_at", "cap_bytes",
    "observed_receipt_bytes", "reason_code",
})
PRIMARY_RUN_FIELDS = (
    "schema", "primary_profile", "primary_invocation_id", "trigger_status",
    "scheduled_for", "started_at", "intent_requested_at", "intent_acquired_at",
    "intent_released_at", "intent_wait_milliseconds", "lock_acquired_at",
    "lock_released_at", "finished_at", "status", "lock_hold_milliseconds",
    "collection_manifest_projection", "collection_manifest_projection_sha256",
    "contention_receipt_sha256", "reason_code",
)
PRIMARY_COLLECTION_MANIFEST_PROJECTION_FIELDS = (
    "schema", "primary_profile", "source_run_id", "source_manifest_sha256",
    "source_schema_version", "source_profile", "source_status",
    "source_publish_local", "source_started_at", "source_finished_at",
    "source_step_names", "source_step_statuses",
)
_PRIMARY_RUN_FIELDS = frozenset(PRIMARY_RUN_FIELDS)
_PRIMARY_PROJECTION_FIELDS = frozenset(
    PRIMARY_COLLECTION_MANIFEST_PROJECTION_FIELDS
)
_PRIMARY_RUN_OVERFLOW_FIELDS = _OVERFLOW_FIELDS
_COLLECTION_MANIFEST_FIELDS = frozenset({
    "schema_version", "run_id", "profile", "status", "publish_local",
    "started_at", "finished_at", "atomicity", "steps", "facts",
    "dependency_files",
})
_PROFILE_STEPS = {
    "daily": ("lifecycle", "daily", "tvl"),
    "depth": ("depth", "dex_price", "dex_depth"),
}
_LOWER_32 = re.compile(r"[0-9a-f]{32}\Z", flags=re.ASCII)
_LOWER_64 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII)
_PRODUCTION_RUN_ID = re.compile(
    r"\d{8}T\d{6}Z-[0-9a-f]{8}\Z", flags=re.ASCII
)
_CANONICAL_UTC = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z",
    flags=re.ASCII,
)
_CANONICAL_WHOLE_UTC = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z", flags=re.ASCII
)
_CANONICAL_SOURCE_UTC = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00\Z",
    flags=re.ASCII,
)
_LOCK_IDENTITY = re.compile(r"[1-9]\d*:[1-9]\d*\Z", flags=re.ASCII)
_PRIMARY_RUN_REASONS = {
    "succeeded": frozenset({None}),
    "failed": frozenset({"collection_failed"}),
    "skipped_locked": frozenset({"collection_lock_busy"}),
    "unexplained": frozenset({"collection_run_unexplained"}),
}


def _directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ValueError("descriptor-safe directory operations are unavailable")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _absolute_path(path: Path) -> Path:
    absolute = Path(os.path.abspath(os.path.expanduser(os.fspath(path))))
    if sys.platform == "darwin" and len(absolute.parts) > 1:
        if absolute.parts[1] in {"tmp", "var"}:
            alias = Path("/") / absolute.parts[1]
            expected = Path("/private") / absolute.parts[1]
            if alias.is_symlink() and Path(os.path.realpath(str(alias))) == expected:
                absolute = expected.joinpath(*absolute.parts[2:])
    return absolute


def open_verified_directory_chain(
    path: Path, *, create: bool = False
) -> Tuple[int, Tuple[Tuple[int, int, int, int], ...]]:
    """Open one directory ancestry without following any user-controlled link."""
    absolute = _absolute_path(path)
    flags = _directory_flags()
    descriptor = os.open(os.sep, flags)
    snapshots = []
    try:
        for component in absolute.parts[1:]:
            if create:
                try:
                    os.mkdir(component, 0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as error:
                raise ValueError(
                    "directory ancestry is missing, unsafe, or changed"
                ) from error
            metadata = os.fstat(child)
            path_metadata = os.stat(
                component, dir_fd=descriptor, follow_symlinks=False
            )
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
            ):
                os.close(child)
                raise ValueError("directory ancestry is unsafe or changed")
            snapshots.append((
                metadata.st_dev, metadata.st_ino,
                metadata.st_ctime_ns, metadata.st_mtime_ns,
            ))
            os.close(descriptor)
            descriptor = child
        return descriptor, tuple(snapshots)
    except BaseException:
        os.close(descriptor)
        raise


def verify_directory_chain(
    path: Path, expected: Tuple[Tuple[int, int, int, int], ...]
) -> None:
    descriptor, current = open_verified_directory_chain(path)
    try:
        if current != expected:
            raise ValueError("directory ancestry changed during operation")
    finally:
        os.close(descriptor)


def open_verified_regular_at(
    directory_fd: int, name: str, *, create: bool = False, mode: int = 0o600
) -> int:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError("regular member name is unsafe")
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    if create:
        flags |= os.O_CREAT
    try:
        descriptor = os.open(name, flags, mode, dir_fd=directory_fd)
    except OSError as error:
        raise ValueError("regular member is unsafe or changed") from error
    metadata = os.fstat(descriptor)
    path_metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_dev, metadata.st_ino)
        != (path_metadata.st_dev, path_metadata.st_ino)
    ):
        os.close(descriptor)
        raise ValueError("regular member is unsafe or changed")
    return descriptor


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or _CANONICAL_UTC.fullmatch(value) is None:
        raise ValueError("{} must be canonical UTC".format(label))
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError) as error:
        raise ValueError("{} must be canonical UTC".format(label)) from error
    return value


def _canonical_whole_timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or _CANONICAL_WHOLE_UTC.fullmatch(value) is None:
        raise ValueError("{} must be canonical whole-second UTC".format(label))
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError) as error:
        raise ValueError("{} must be canonical whole-second UTC".format(label)) from error
    return value


def _canonical_source_timestamp(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or _CANONICAL_SOURCE_UTC.fullmatch(value) is None
    ):
        raise ValueError("{} must be canonical UTC".format(label))
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError) as error:
        raise ValueError("{} must be canonical UTC".format(label)) from error
    return value


def _milliseconds_between(start: str, finish: str, label: str) -> int:
    start_epoch = exact_rfc3339_epoch_seconds(start)
    finish_epoch = exact_rfc3339_epoch_seconds(finish)
    delta = (finish_epoch - start_epoch) * Decimal(1000)
    if delta < 0 or delta != delta.to_integral_value():
        raise ValueError("{} timestamps do not define exact milliseconds".format(label))
    return int(delta)


def _ordered_timestamp_fields(
    value: Mapping[str, Any], fields: Sequence[str], label: str
) -> None:
    prior = None
    for field in fields:
        current = _canonical_timestamp(value.get(field), field)
        if prior is not None and exact_rfc3339_epoch_seconds(current) < prior:
            raise ValueError("{} timestamps are reversed".format(label))
        prior = exact_rfc3339_epoch_seconds(current)


def _exact_nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("{} must be a nonnegative integer".format(label))
    return value


def _canonical_scheduled_for(profile: str, value: Any) -> str:
    text = _canonical_timestamp(value, "scheduled_for")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    if parsed.second != 0 or parsed.microsecond != 0:
        raise ValueError("primary run schedule is not on the production grid")
    if profile == "daily":
        valid = parsed.hour == 0 and parsed.minute == 30
    else:
        valid = parsed.minute == 5
    if not valid:
        raise ValueError("primary run schedule is not on the production grid")
    return text


def _lock_identity(fd: int) -> str:
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValueError("collection lock file is unsafe or hard-linked")
    return "{}:{}".format(metadata.st_dev, metadata.st_ino)


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise OSError("short write while installing collection-lock evidence")
        offset += written


def _read_lock_bytes(fd: int, maximum: int = 4096) -> bytes:
    payload = os.pread(fd, maximum + 1, 0)
    if len(payload) > maximum:
        raise ValueError("collection-lock owner evidence exceeds its bound")
    return payload


def validate_shadow_lock_owner(value: Mapping[str, Any]) -> Dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _OWNER_FIELDS
        or value.get("schema") != SHADOW_LOCK_OWNER_SCHEMA
        or value.get("owner_kind") != "route_shadow"
        or not isinstance(value.get("run_id"), str)
        or _RUN_ID.fullmatch(value["run_id"]) is None
        or value["run_id"] in {".", ".."}
        or not isinstance(value.get("boot_id"), str)
        or _LOWER_32.fullmatch(value["boot_id"]) is None
        or not isinstance(value.get("nonce"), str)
        or _LOWER_32.fullmatch(value["nonce"]) is None
    ):
        raise ValueError("collection-lock owner evidence is invalid")
    return dict(value)


def read_shadow_lock_owner(lock_fd: int) -> Optional[Dict[str, Any]]:
    payload = _read_lock_bytes(lock_fd)
    if not payload:
        return None
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("collection-lock owner evidence is invalid") from error
    owner = validate_shadow_lock_owner(value)
    if _canonical_bytes(owner) != payload:
        raise ValueError("collection-lock owner evidence is not canonical")
    return owner


def write_shadow_lock_owner(
    lock_fd: int,
    *,
    run_id: str,
    boot_id: str,
    nonce: str,
) -> Dict[str, Any]:
    _lock_identity(lock_fd)
    owner = validate_shadow_lock_owner({
        "schema": SHADOW_LOCK_OWNER_SCHEMA,
        "owner_kind": "route_shadow",
        "run_id": run_id,
        "boot_id": boot_id,
        "nonce": nonce,
    })
    payload = _canonical_bytes(owner)
    if len(payload) > 4096:
        raise ValueError("collection-lock owner evidence exceeds its bound")
    os.ftruncate(lock_fd, 0)
    os.lseek(lock_fd, 0, os.SEEK_SET)
    _write_all(lock_fd, payload)
    os.fsync(lock_fd)
    if read_shadow_lock_owner(lock_fd) != owner:
        raise ValueError("collection-lock owner evidence changed after write")
    return owner


def clear_shadow_lock_owner(lock_fd: int, *, nonce: str) -> None:
    owner = read_shadow_lock_owner(lock_fd)
    if owner is None or owner["nonce"] != nonce:
        raise ValueError("collection-lock owner nonce does not match")
    os.ftruncate(lock_fd, 0)
    os.fsync(lock_fd)
    if _read_lock_bytes(lock_fd):
        raise ValueError("collection-lock owner evidence was not cleared")


def validate_primary_contention_receipt(
    value: Mapping[str, Any]
) -> Dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _CONTENTION_FIELDS
        or value.get("schema") != PRIMARY_CONTENTION_SCHEMA
        or value.get("attribution_status") not in {"shadow", "unattributed"}
        or value.get("primary_profile") not in {"daily", "depth"}
        or not isinstance(value.get("primary_invocation_id"), str)
        or _LOWER_32.fullmatch(value["primary_invocation_id"]) is None
        or not isinstance(value.get("lock_identity"), str)
        or _LOCK_IDENTITY.fullmatch(value["lock_identity"]) is None
    ):
        raise ValueError("primary contention receipt is invalid")
    _canonical_whole_timestamp(value.get("observed_at"), "contention observed_at")
    holder_values = (
        value.get("holder_run_id"),
        value.get("holder_boot_id"),
        value.get("holder_nonce"),
    )
    if value["attribution_status"] == "shadow":
        validate_shadow_lock_owner({
            "schema": SHADOW_LOCK_OWNER_SCHEMA,
            "owner_kind": "route_shadow",
            "run_id": holder_values[0],
            "boot_id": holder_values[1],
            "nonce": holder_values[2],
        })
    elif holder_values != (None, None, None):
        raise ValueError("unattributed contention holder fields must be null")
    return dict(value)


def _parse_exact_collection_manifest(source_manifest_bytes: bytes) -> Dict[str, Any]:
    if not isinstance(source_manifest_bytes, bytes):
        raise ValueError("source manifest must be exact bytes")
    if len(source_manifest_bytes) > 1024 * 1024:
        raise ValueError("source manifest exceeds its bounded read")
    try:
        decoded = source_manifest_bytes.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("source manifest is invalid JSON") from error
    if not isinstance(value, Mapping) or set(value) != _COLLECTION_MANIFEST_FIELDS:
        raise ValueError("source manifest schema is not exact")
    canonical_source = (
        json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    if canonical_source != source_manifest_bytes:
        raise ValueError("source manifest bytes are not canonical")
    return dict(value)


def validate_primary_collection_manifest_projection(
    value: Mapping[str, Any]
) -> Dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _PRIMARY_PROJECTION_FIELDS
        or value.get("schema") != PRIMARY_COLLECTION_MANIFEST_PROJECTION_SCHEMA
        or value.get("primary_profile") not in _PROFILE_STEPS
        or value.get("source_profile") != value.get("primary_profile")
        or value.get("source_schema_version") != 1
        or value.get("source_status") != "succeeded"
        or value.get("source_publish_local") is not True
        or not isinstance(value.get("source_run_id"), str)
        or _PRODUCTION_RUN_ID.fullmatch(value["source_run_id"]) is None
        or not isinstance(value.get("source_manifest_sha256"), str)
        or _LOWER_64.fullmatch(value["source_manifest_sha256"]) is None
    ):
        raise ValueError("primary collection manifest projection is invalid")
    started = _canonical_source_timestamp(
        value.get("source_started_at"), "source_started_at"
    )
    finished = _canonical_source_timestamp(
        value.get("source_finished_at"), "source_finished_at"
    )
    if exact_rfc3339_epoch_seconds(finished) < exact_rfc3339_epoch_seconds(started):
        raise ValueError("primary collection manifest timestamps are reversed")
    expected_steps = list(_PROFILE_STEPS[value["primary_profile"]])
    if value.get("source_step_names") != expected_steps:
        raise ValueError("primary collection manifest step names are invalid")
    if value.get("source_step_statuses") != ["succeeded"] * len(expected_steps):
        raise ValueError("primary collection manifest step statuses are invalid")
    cloned = dict(value)
    cloned["source_step_names"] = list(value["source_step_names"])
    cloned["source_step_statuses"] = list(value["source_step_statuses"])
    if len(_canonical_bytes(cloned)) > PRIMARY_RUN_MAX_PROJECTION_BYTES:
        raise ValueError("primary collection manifest projection exceeds its bound")
    return cloned


def build_primary_collection_manifest_projection(
    source_manifest_bytes: bytes, *, primary_profile: str
) -> Dict[str, Any]:
    if primary_profile not in _PROFILE_STEPS:
        raise ValueError("primary profile is invalid")
    manifest = _parse_exact_collection_manifest(source_manifest_bytes)
    if (
        manifest.get("schema_version") != 1
        or manifest.get("profile") != primary_profile
        or manifest.get("status") != "succeeded"
        or manifest.get("publish_local") is not True
        or not isinstance(manifest.get("run_id"), str)
        or _PRODUCTION_RUN_ID.fullmatch(manifest["run_id"]) is None
        or not isinstance(manifest.get("steps"), list)
    ):
        raise ValueError("source manifest is not a successful production manifest")
    expected_steps = list(_PROFILE_STEPS[primary_profile])
    actual_names = []
    actual_statuses = []
    for step in manifest["steps"]:
        if not isinstance(step, Mapping):
            raise ValueError("source manifest step is invalid")
        actual_names.append(step.get("name"))
        actual_statuses.append(step.get("status"))
    if actual_names != expected_steps or actual_statuses != ["succeeded"] * len(expected_steps):
        raise ValueError("source manifest steps do not match the production profile")
    projection = {
        "schema": PRIMARY_COLLECTION_MANIFEST_PROJECTION_SCHEMA,
        "primary_profile": primary_profile,
        "source_run_id": manifest["run_id"],
        "source_manifest_sha256": _sha256(source_manifest_bytes),
        "source_schema_version": manifest["schema_version"],
        "source_profile": manifest["profile"],
        "source_status": manifest["status"],
        "source_publish_local": manifest["publish_local"],
        "source_started_at": manifest["started_at"],
        "source_finished_at": manifest["finished_at"],
        "source_step_names": actual_names,
        "source_step_statuses": actual_statuses,
    }
    return validate_primary_collection_manifest_projection(projection)


def validate_primary_run_receipt(value: Mapping[str, Any]) -> Dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _PRIMARY_RUN_FIELDS
        or value.get("schema") != PRIMARY_RUN_SCHEMA
        or value.get("primary_profile") not in _PROFILE_STEPS
        or not isinstance(value.get("primary_invocation_id"), str)
        or _LOWER_32.fullmatch(value["primary_invocation_id"]) is None
        or value.get("trigger_status") not in {"scheduled", "manual", "invalid"}
        or value.get("status") not in _PRIMARY_RUN_REASONS
        or value.get("reason_code") not in _PRIMARY_RUN_REASONS.get(value.get("status"), ())
    ):
        raise ValueError("primary run receipt is invalid")
    _ordered_timestamp_fields(
        value,
        (
            "started_at", "intent_requested_at", "intent_acquired_at",
            "intent_released_at", "finished_at",
        ),
        "primary run",
    )
    wait_milliseconds = _exact_nonnegative_int(
        value.get("intent_wait_milliseconds"), "intent_wait_milliseconds"
    )
    if wait_milliseconds != _milliseconds_between(
        value["intent_requested_at"], value["intent_acquired_at"], "intent wait"
    ):
        raise ValueError("primary run intent wait milliseconds do not reproduce")
    trigger_status = value["trigger_status"]
    if trigger_status == "invalid" and value["status"] != "unexplained":
        raise ValueError("invalid primary trigger must be unexplained")
    if trigger_status == "scheduled":
        scheduled_for = _canonical_scheduled_for(
            value["primary_profile"], value.get("scheduled_for")
        )
        delay = exact_rfc3339_epoch_seconds(value["started_at"]) - exact_rfc3339_epoch_seconds(scheduled_for)
        if delay < 0 or delay > 60:
            raise ValueError("primary run is outside the schedule accuracy window")
    elif value.get("scheduled_for") is not None:
        raise ValueError("manual or invalid primary run scheduled_for must be null")

    acquired = value["status"] in {"succeeded", "failed"}
    if acquired:
        _ordered_timestamp_fields(
            value,
            ("intent_acquired_at", "lock_acquired_at", "lock_released_at", "intent_released_at"),
            "primary run lock",
        )
        hold_milliseconds = _exact_nonnegative_int(
            value.get("lock_hold_milliseconds"), "lock_hold_milliseconds"
        )
        if hold_milliseconds != _milliseconds_between(
            value["lock_acquired_at"], value["lock_released_at"], "lock hold"
        ):
            raise ValueError("primary run lock hold milliseconds do not reproduce")
    elif (
        value.get("lock_acquired_at") is not None
        or value.get("lock_released_at") is not None
        or value.get("lock_hold_milliseconds") is not None
    ):
        raise ValueError("non-acquired primary run lock facts must be null")

    projection = value.get("collection_manifest_projection")
    projection_sha = value.get("collection_manifest_projection_sha256")
    if value["status"] == "succeeded":
        validated_projection = validate_primary_collection_manifest_projection(projection)
        if validated_projection["primary_profile"] != value["primary_profile"]:
            raise ValueError("primary run projection profile does not match")
        if (
            not isinstance(projection_sha, str)
            or _LOWER_64.fullmatch(projection_sha) is None
            or _sha256(_canonical_bytes(validated_projection)) != projection_sha
        ):
            raise ValueError("primary run projection SHA does not reproduce")
    elif projection is not None or projection_sha is not None:
        raise ValueError("non-success primary run projection fields must be null")

    contention_sha = value.get("contention_receipt_sha256")
    if value["status"] == "skipped_locked":
        if not isinstance(contention_sha, str) or _LOWER_64.fullmatch(contention_sha) is None:
            raise ValueError("skipped primary run contention SHA is invalid")
    elif contention_sha is not None:
        raise ValueError("non-skipped primary run contention SHA must be null")
    cloned = dict(value)
    if projection is not None:
        cloned["collection_manifest_projection"] = validated_projection
    if len(_canonical_bytes(cloned)) > PRIMARY_RUN_MAX_RECEIPT_BYTES:
        raise ValueError("primary run receipt exceeds its bound")
    return cloned


def _ensure_safe_root(path: Path) -> int:
    fd, _snapshots = open_verified_directory_chain(path, create=True)
    return fd


def _read_regular_at(root_fd: int, name: str, maximum: int) -> Optional[bytes]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(name, flags, dir_fd=root_fd)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValueError("primary contention member is unsafe or hard-linked") from error
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError("primary contention member is unsafe or hard-linked")
        payload = os.read(fd, maximum + 1)
        if len(payload) > maximum:
            raise ValueError("primary contention member exceeds its bound")
        return payload
    finally:
        os.close(fd)


def _open_cap_lock(root_fd: int, label: str) -> int:
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        try:
            fd = os.open(".cap.lock", flags, dir_fd=root_fd)
        except FileNotFoundError:
            try:
                fd = os.open(
                    ".cap.lock", flags | os.O_CREAT | os.O_EXCL, 0o600,
                    dir_fd=root_fd,
                )
            except FileExistsError:
                fd = os.open(".cap.lock", flags, dir_fd=root_fd)
    except OSError as error:
        raise ValueError("{} cap lock is unsafe".format(label)) from error
    try:
        metadata = os.fstat(fd)
        path_metadata = os.stat(
            ".cap.lock", dir_fd=root_fd, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != 0
            or (metadata.st_dev, metadata.st_ino)
            != (path_metadata.st_dev, path_metadata.st_ino)
        ):
            raise ValueError("{} cap lock is unsafe".format(label))
        return fd
    except BaseException:
        os.close(fd)
        raise


def _scan_bounded_receipt_root(
    root_fd: int,
    *,
    label: str,
    receipt_pattern: re.Pattern,
    maximum_member_bytes: int,
    overflow_schema: str,
    cap_bytes: int,
) -> Tuple[Optional[bytes], int]:
    members = os.listdir(root_fd)
    current_bytes = 0
    overflow_payload = None
    for name in members:
        if name == ".cap.lock":
            continue
        if name == "overflow.json":
            overflow_payload = _read_regular_at(root_fd, name, 4096)
            if overflow_payload is None:
                raise ValueError("{} overflow marker disappeared".format(label))
            try:
                marker = json.loads(overflow_payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError("{} overflow marker is invalid".format(label)) from error
            if (
                not isinstance(marker, Mapping)
                or set(marker) != _OVERFLOW_FIELDS
                or marker.get("schema") != overflow_schema
                or not isinstance(marker.get("first_rejected_invocation_id"), str)
                or _LOWER_32.fullmatch(marker["first_rejected_invocation_id"]) is None
                or marker.get("cap_bytes") != cap_bytes
                or isinstance(marker.get("observed_receipt_bytes"), bool)
                or not isinstance(marker.get("observed_receipt_bytes"), int)
                or marker["observed_receipt_bytes"] < 0
                or marker.get("reason_code") != (
                    "primary_contention_receipt_capacity_exhausted"
                    if overflow_schema == PRIMARY_CONTENTION_OVERFLOW_SCHEMA
                    else "primary_run_receipt_capacity_exhausted"
                )
                or _canonical_bytes(marker) != overflow_payload
            ):
                raise ValueError("{} overflow marker is invalid".format(label))
            _canonical_timestamp(marker.get("observed_at"), "overflow observed_at")
            continue
        if receipt_pattern.fullmatch(name) is None:
            raise ValueError("{} root has an unknown member".format(label))
        member = _read_regular_at(root_fd, name, maximum_member_bytes)
        if member is None:
            raise ValueError("{} member disappeared".format(label))
        current_bytes += len(member)
    return overflow_payload, current_bytes


def _write_new_at(root_fd: int, name: str, payload: bytes) -> None:
    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    )
    fd = os.open(name, flags, 0o600, dir_fd=root_fd)
    try:
        _write_all(fd, payload)
        os.fsync(fd)
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != len(payload)
        ):
            raise ValueError("primary contention member is unsafe")
    finally:
        os.close(fd)


def write_primary_contention_receipt(
    data_dir: Path,
    *,
    lock_fd: int,
    primary_profile: str,
    primary_invocation_id: str,
    observed_at: str,
) -> Dict[str, Any]:
    if _LOWER_32.fullmatch(primary_invocation_id or "") is None:
        raise ValueError("primary invocation ID is invalid")
    lock_identity = _lock_identity(lock_fd)
    owner = None
    try:
        owner = read_shadow_lock_owner(lock_fd)
    except ValueError:
        owner = None
    receipt = validate_primary_contention_receipt({
        "schema": PRIMARY_CONTENTION_SCHEMA,
        "attribution_status": "shadow" if owner is not None else "unattributed",
        "holder_run_id": None if owner is None else owner["run_id"],
        "holder_boot_id": None if owner is None else owner["boot_id"],
        "holder_nonce": None if owner is None else owner["nonce"],
        "primary_profile": primary_profile,
        "primary_invocation_id": primary_invocation_id,
        "observed_at": observed_at,
        "lock_identity": lock_identity,
    })
    payload = _canonical_bytes(receipt)
    if len(payload) > PRIMARY_CONTENTION_MAX_RECEIPT_BYTES:
        raise ValueError("primary contention receipt exceeds its bound")
    root = Path(data_dir) / "routes/shadow/primary-contention"
    root_fd = _ensure_safe_root(root)
    cap_fd = -1
    try:
        cap_fd = _open_cap_lock(root_fd, "primary contention")
        fcntl.flock(cap_fd, fcntl.LOCK_EX)
        overflow, current_bytes = _scan_bounded_receipt_root(
            root_fd,
            label="primary contention",
            receipt_pattern=re.compile(r"[0-9a-f]{32}\.json\Z", flags=re.ASCII),
            maximum_member_bytes=PRIMARY_CONTENTION_MAX_RECEIPT_BYTES,
            overflow_schema=PRIMARY_CONTENTION_OVERFLOW_SCHEMA,
            cap_bytes=PRIMARY_CONTENTION_CAP_BYTES,
        )
        if overflow is not None:
            raise ValueError("primary contention receipt capacity is exhausted")
        if current_bytes > PRIMARY_CONTENTION_DATA_BYTES:
            # The reserved marker budget has already been consumed by receipt
            # data.  Installing any further byte would compound the corrupt
            # inventory and can cross the hard cap, so fail with zero writes.
            raise ValueError("primary contention receipt capacity is exhausted")
        existing = _read_regular_at(
            root_fd, primary_invocation_id + ".json", PRIMARY_CONTENTION_MAX_RECEIPT_BYTES
        )
        if existing is not None:
            if existing != payload:
                raise ValueError("immutable primary contention receipt conflicts")
            return receipt
        if current_bytes + len(payload) > PRIMARY_CONTENTION_DATA_BYTES:
            marker = {
                "schema": PRIMARY_CONTENTION_OVERFLOW_SCHEMA,
                "first_rejected_invocation_id": primary_invocation_id,
                "observed_at": observed_at,
                "cap_bytes": PRIMARY_CONTENTION_CAP_BYTES,
                "observed_receipt_bytes": current_bytes,
                "reason_code": "primary_contention_receipt_capacity_exhausted",
            }
            marker_payload = _canonical_bytes(marker)
            if (
                set(marker) != _OVERFLOW_FIELDS
                or len(marker_payload) > PRIMARY_CONTENTION_OVERFLOW_RESERVE_BYTES
                or current_bytes + len(marker_payload)
                > PRIMARY_CONTENTION_CAP_BYTES
            ):
                raise AssertionError("primary contention overflow schema drifted")
            _write_new_at(root_fd, "overflow.json", marker_payload)
            os.fsync(root_fd)
            raise ValueError("primary contention receipt capacity is exhausted")
        _write_new_at(root_fd, primary_invocation_id + ".json", payload)
        os.fsync(root_fd)
        installed = _read_regular_at(
            root_fd, primary_invocation_id + ".json", PRIMARY_CONTENTION_MAX_RECEIPT_BYTES
        )
        if installed != payload:
            raise ValueError("primary contention receipt changed after write")
        return receipt
    finally:
        if cap_fd >= 0:
            try:
                fcntl.flock(cap_fd, fcntl.LOCK_UN)
            finally:
                os.close(cap_fd)
        os.close(root_fd)


def write_primary_run_receipt(
    data_dir: Path, receipt: Mapping[str, Any]
) -> Dict[str, Any]:
    validated = validate_primary_run_receipt(receipt)
    invocation_id = validated["primary_invocation_id"]
    payload = _canonical_bytes(validated)
    if validated["status"] == "skipped_locked":
        contention_root = Path(data_dir) / "routes/shadow/primary-contention"
        try:
            contention_root_fd, _ancestry = open_verified_directory_chain(
                contention_root
            )
        except ValueError as error:
            raise ValueError("primary contention receipt root is unavailable") from error
        try:
            contention_payload = _read_regular_at(
                contention_root_fd,
                invocation_id + ".json",
                PRIMARY_CONTENTION_MAX_RECEIPT_BYTES,
            )
            if contention_payload is None:
                raise ValueError("primary contention receipt is missing")
            try:
                contention_value = json.loads(contention_payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError("primary contention receipt is invalid") from error
            validated_contention = validate_primary_contention_receipt(
                contention_value
            )
            if (
                validated_contention["primary_invocation_id"] != invocation_id
                or validated_contention["primary_profile"]
                != validated["primary_profile"]
                or _canonical_bytes(validated_contention) != contention_payload
                or _sha256(contention_payload)
                != validated["contention_receipt_sha256"]
            ):
                raise ValueError("primary contention receipt binding is invalid")
        finally:
            os.close(contention_root_fd)
    root = Path(data_dir) / "routes/shadow/primary-runs"
    root_fd = _ensure_safe_root(root)
    cap_fd = -1
    try:
        cap_fd = _open_cap_lock(root_fd, "primary run")
        fcntl.flock(cap_fd, fcntl.LOCK_EX)
        overflow, current_bytes = _scan_bounded_receipt_root(
            root_fd,
            label="primary run",
            receipt_pattern=re.compile(r"[0-9a-f]{32}\.json\Z", flags=re.ASCII),
            maximum_member_bytes=PRIMARY_RUN_MAX_RECEIPT_BYTES,
            overflow_schema=PRIMARY_RUN_OVERFLOW_SCHEMA,
            cap_bytes=PRIMARY_RUN_CAP_BYTES,
        )
        if overflow is not None:
            raise ValueError("primary run receipt capacity is exhausted")
        if current_bytes > PRIMARY_RUN_DATA_BYTES:
            # Preserve the permanent marker reserve and never grow a root that
            # is already beyond its admissible receipt-data inventory.
            raise ValueError("primary run receipt capacity is exhausted")
        name = invocation_id + ".json"
        existing = _read_regular_at(root_fd, name, PRIMARY_RUN_MAX_RECEIPT_BYTES)
        if existing is not None:
            if existing != payload:
                raise ValueError("immutable primary run receipt conflicts")
            return validated
        if current_bytes + len(payload) > PRIMARY_RUN_DATA_BYTES:
            marker = {
                "schema": PRIMARY_RUN_OVERFLOW_SCHEMA,
                "first_rejected_invocation_id": invocation_id,
                "observed_at": validated["finished_at"],
                "cap_bytes": PRIMARY_RUN_CAP_BYTES,
                "observed_receipt_bytes": current_bytes,
                "reason_code": "primary_run_receipt_capacity_exhausted",
            }
            marker_payload = _canonical_bytes(marker)
            if (
                set(marker) != _PRIMARY_RUN_OVERFLOW_FIELDS
                or len(marker_payload) > PRIMARY_RUN_OVERFLOW_RESERVE_BYTES
                or current_bytes + len(marker_payload) > PRIMARY_RUN_CAP_BYTES
            ):
                raise AssertionError("primary run overflow schema drifted")
            _write_new_at(root_fd, "overflow.json", marker_payload)
            os.fsync(root_fd)
            raise ValueError("primary run receipt capacity is exhausted")
        _write_new_at(root_fd, name, payload)
        os.fsync(root_fd)
        installed = _read_regular_at(root_fd, name, PRIMARY_RUN_MAX_RECEIPT_BYTES)
        if installed != payload:
            raise ValueError("primary run receipt changed after write")
        return validated
    finally:
        if cap_fd >= 0:
            try:
                fcntl.flock(cap_fd, fcntl.LOCK_UN)
            finally:
                os.close(cap_fd)
        os.close(root_fd)
