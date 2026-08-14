"""Fail-closed loader for the committed route Shadow authority.

Task 3 deliberately recognizes only feature-off authority.  The complete
transaction replay and live unit proof required for feature-on authority are
reserved for Task 6; until then, no on-disk record can produce ``enabled``.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple


ROUTE_SHADOW_AUTHORITY_VIEW_SCHEMA = "route_shadow_authority_view/v1"
ROUTE_SHADOW_ENABLED_SCHEMA = "route_shadow_enabled/v1"

_AUTHORITY_FILENAME = "enabled.json"
_AUTHORITY_FIELDS = frozenset({"schema", "enabled", "transaction_id"})
_MAX_AUTHORITY_BYTES = 3 * 1024
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_INVALID_AUTHORITY_REASON = "authority_evidence_invalid"
_UNAVAILABLE_ENABLE_REASON = "enable_contract_not_available"

_DirectoryIdentity = Tuple[int, int, Optional[Tuple[Any, ...]]]


class _AuthorityAbsent(Exception):
    pass


class _AuthorityUnsafe(Exception):
    pass


_PROBE_CAPABILITY = object()


class _AuthorityLiveProbe:
    """Sealed no-argument live-probe capability reserved for Task 6."""

    __slots__ = ("_operation",)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del cls, kwargs
        raise TypeError("AuthorityLiveProbe is sealed")

    def __init__(self, capability: object, operation: Callable[[], Mapping[str, Any]]):
        if capability is not _PROBE_CAPABILITY or not callable(operation):
            raise TypeError("invalid AuthorityLiveProbe capability")
        self._operation = operation

    def sample(self) -> Mapping[str, Any]:
        """Return the fixed Task 6 projection without accepting arguments."""

        return self._operation()


def _make_authority_live_probe_for_test(
    operation: Callable[[], Mapping[str, Any]]
) -> _AuthorityLiveProbe:
    """Create the module-private identity-capability test seam."""

    return _AuthorityLiveProbe(_PROBE_CAPABILITY, operation)


def _task6_probe_not_available() -> Mapping[str, Any]:
    raise RuntimeError("Task 6 live authority probe is not available")


def _default_live_probe() -> _AuthorityLiveProbe:
    return _AuthorityLiveProbe(_PROBE_CAPABILITY, _task6_probe_not_available)


def _authority_view(
    status: str,
    *,
    transaction_id: Optional[str] = None,
    authority_sha256: Optional[str] = None,
    reason_code: Optional[str] = None
) -> Dict[str, Any]:
    return {
        "schema": ROUTE_SHADOW_AUTHORITY_VIEW_SCHEMA,
        "status": status,
        "transaction_id": transaction_id,
        "authority_sha256": authority_sha256,
        "primary_unit_projection_sha256": None,
        "reason_code": reason_code,
    }


def _secure_directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise _AuthorityUnsafe("secure directory open is unavailable")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _absolute_path(path: Path) -> Path:
    try:
        expanded = os.path.abspath(os.path.expanduser(os.fspath(path)))
    except (TypeError, ValueError, OSError) as error:
        raise _AuthorityUnsafe("authority root path is invalid") from error
    if sys.platform == "darwin":
        if expanded == "/var" or expanded.startswith("/var/"):
            expanded = "/private" + expanded
        elif expanded == "/tmp" or expanded.startswith("/tmp/"):
            expanded = "/private" + expanded
    return Path(expanded)


def _stable_directory_metadata(metadata: os.stat_result) -> Tuple[Any, ...]:
    return (
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        getattr(metadata, "st_mtime_ns", None),
        getattr(metadata, "st_ctime_ns", None),
        getattr(metadata, "st_birthtime_ns", None),
        getattr(metadata, "st_flags", None),
    )


def _open_directory_chain(path: Path) -> Tuple[int, Tuple[_DirectoryIdentity, ...]]:
    absolute = _absolute_path(path)
    flags = _secure_directory_flags()
    try:
        descriptor = os.open(os.sep, flags)
    except OSError as error:
        raise _AuthorityUnsafe("authority root cannot be opened safely") from error
    identities: List[_DirectoryIdentity] = []
    try:
        root_metadata = os.fstat(descriptor)
        identities.append((root_metadata.st_dev, root_metadata.st_ino, None))
        stable_tail_start = max(1, len(absolute.parts) - 4)
        for index, component in enumerate(absolute.parts[1:], start=1):
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError as error:
                raise _AuthorityAbsent("authority directory is absent") from error
            except OSError as error:
                raise _AuthorityUnsafe(
                    "authority directory is missing, changed, or a symlink"
                ) from error
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise _AuthorityUnsafe("authority parent is not a directory")
            stable_metadata = (
                _stable_directory_metadata(metadata)
                if index >= stable_tail_start
                else None
            )
            identities.append(
                (metadata.st_dev, metadata.st_ino, stable_metadata)
            )
        return descriptor, tuple(identities)
    except BaseException:
        os.close(descriptor)
        raise


def _recheck_directory_chain(
    path: Path, expected: Tuple[_DirectoryIdentity, ...]
) -> None:
    try:
        descriptor, actual = _open_directory_chain(path)
    except _AuthorityAbsent as error:
        raise _AuthorityUnsafe("authority directory disappeared") from error
    try:
        if actual != expected:
            raise _AuthorityUnsafe("authority directory identity changed")
    finally:
        os.close(descriptor)


def _stable_file_metadata(
    metadata: os.stat_result,
) -> Tuple[int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        metadata.st_nlink,
        metadata.st_size,
        getattr(metadata, "st_mtime_ns", int(metadata.st_mtime * 1000000000)),
        getattr(metadata, "st_ctime_ns", int(metadata.st_ctime * 1000000000)),
    )


def _read_bounded_descriptor(descriptor: int) -> bytes:
    chunks = []
    offset = 0
    while offset <= _MAX_AUTHORITY_BYTES:
        block = os.pread(
            descriptor,
            min(64 * 1024, _MAX_AUTHORITY_BYTES + 1 - offset),
            offset,
        )
        if not block:
            break
        chunks.append(block)
        offset += len(block)
        if offset > _MAX_AUTHORITY_BYTES:
            raise _AuthorityUnsafe("authority exceeds its bounded read limit")
    return b"".join(chunks)


def _read_authority_bytes(data_dir: Path) -> Tuple[bytes, str]:
    operational = _absolute_path(data_dir) / "routes" / "shadow" / "operational"
    parent_descriptor, directory_identities = _open_directory_chain(operational)
    descriptor = -1
    try:
        try:
            path_before = os.stat(
                _AUTHORITY_FILENAME,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError as error:
            _recheck_directory_chain(operational, directory_identities)
            try:
                os.stat(
                    _AUTHORITY_FILENAME,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                _recheck_directory_chain(operational, directory_identities)
                raise _AuthorityAbsent("authority file is absent") from error
            raise _AuthorityUnsafe("authority appeared during absence check")
        except OSError as error:
            raise _AuthorityUnsafe("authority path cannot be inspected") from error

        if (
            not stat.S_ISREG(path_before.st_mode)
            or path_before.st_nlink != 1
            or path_before.st_size > _MAX_AUTHORITY_BYTES
        ):
            raise _AuthorityUnsafe(
                "authority must be a bounded single-link regular file"
            )

        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise _AuthorityUnsafe("secure authority open is unavailable")
        flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(
                _AUTHORITY_FILENAME, flags, dir_fd=parent_descriptor
            )
        except OSError as error:
            raise _AuthorityUnsafe("authority changed or is a symlink") from error

        descriptor_before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(descriptor_before.st_mode)
            or descriptor_before.st_nlink != 1
            or _stable_file_metadata(path_before)
            != _stable_file_metadata(descriptor_before)
        ):
            raise _AuthorityUnsafe(
                "authority path and descriptor identity differ"
            )
        payload = _read_bounded_descriptor(descriptor)
        descriptor_after = os.fstat(descriptor)
        if (
            len(payload) != descriptor_before.st_size
            or _stable_file_metadata(descriptor_before)
            != _stable_file_metadata(descriptor_after)
        ):
            raise _AuthorityUnsafe("authority changed while it was read")
        try:
            path_after = os.stat(
                _AUTHORITY_FILENAME,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise _AuthorityUnsafe("authority path changed after read") from error
        if _stable_file_metadata(path_after) != _stable_file_metadata(descriptor_after):
            raise _AuthorityUnsafe("authority path identity changed after read")
        _recheck_directory_chain(operational, directory_identities)
        return payload, hashlib.sha256(payload).hexdigest()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def _reject_duplicate_keys(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    value: Dict[str, Any] = {}
    for key, member in pairs:
        if key in value:
            raise _AuthorityUnsafe("authority contains duplicate JSON keys")
        value[key] = member
    return value


def _decode_authority(payload: bytes) -> Tuple[bool, Optional[str]]:
    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        _AuthorityUnsafe,
    ) as error:
        raise _AuthorityUnsafe("authority JSON is invalid") from error
    if not isinstance(value, dict) or set(value) != _AUTHORITY_FIELDS:
        raise _AuthorityUnsafe("authority schema fields are invalid")
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if payload != canonical:
        raise _AuthorityUnsafe("authority bytes are not canonical JSON")
    if value.get("schema") != ROUTE_SHADOW_ENABLED_SCHEMA:
        raise _AuthorityUnsafe("authority schema is invalid")
    enabled = value.get("enabled")
    if type(enabled) is not bool:
        raise _AuthorityUnsafe("authority enabled state is invalid")
    transaction_id = value.get("transaction_id")
    if transaction_id is not None and (
        not isinstance(transaction_id, str)
        or _SHA256_PATTERN.fullmatch(transaction_id) is None
    ):
        raise _AuthorityUnsafe("authority transaction ID is invalid")
    if enabled and transaction_id is None:
        raise _AuthorityUnsafe("enabled authority requires a transaction ID")
    return enabled, transaction_id


def _load_authority_with_probe(
    data_dir: Path, live_probe: _AuthorityLiveProbe
) -> dict:
    if type(live_probe) is not _AuthorityLiveProbe:
        raise TypeError("live_probe must be the sealed AuthorityLiveProbe")
    try:
        payload, authority_sha256 = _read_authority_bytes(data_dir)
    except _AuthorityAbsent:
        return _authority_view("disabled")
    except (OSError, ValueError, _AuthorityUnsafe):
        return _authority_view("invalid", reason_code=_INVALID_AUTHORITY_REASON)

    try:
        enabled, transaction_id = _decode_authority(payload)
    except _AuthorityUnsafe:
        return _authority_view(
            "invalid",
            authority_sha256=authority_sha256,
            reason_code=_INVALID_AUTHORITY_REASON,
        )

    if not enabled and transaction_id is None:
        return _authority_view("disabled", authority_sha256=authority_sha256)

    # Task 3 intentionally performs no probe: a true or transaction-backed
    # record cannot be replayed until Task 6 installs the complete contract.
    return _authority_view(
        "invalid",
        transaction_id=transaction_id,
        authority_sha256=authority_sha256,
        reason_code=_UNAVAILABLE_ENABLE_REASON,
    )


def load_committed_route_shadow_authority(data_dir: Path) -> dict:
    """Return the exact current fail-closed route Shadow authority view."""

    return _load_authority_with_probe(data_dir, _default_live_probe())
