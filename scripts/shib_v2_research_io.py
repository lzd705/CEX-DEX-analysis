"""Bounded filesystem boundary for SHIB V2/V2 research JSON."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys
import uuid
from typing import Sequence, Tuple

try:
    from scripts.shib_v2_research import (
        ResearchContractError,
        canonical_json_bytes,
        scan_public_payload,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from shib_v2_research import (  # type: ignore
        ResearchContractError,
        canonical_json_bytes,
        scan_public_payload,
    )


MAX_JSON_BYTES = 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_JSON_MEMBERS = 4096
MAX_JSON_STRING_TOKEN_BYTES = 64 * 1024
MAX_JSON_INTEGER_TOKEN_BYTES = 128


def _absolute_path(path: Path) -> Path:
    absolute = os.path.abspath(os.fspath(path))
    if sys.platform == "darwin":
        if absolute == "/var" or absolute.startswith("/var/"):
            absolute = "/private" + absolute
        elif absolute == "/tmp" or absolute.startswith("/tmp/"):
            absolute = "/private" + absolute
    return Path(absolute)


def _directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ResearchContractError("secure directory open is unavailable")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _open_directory_chain(path: Path) -> Tuple[int, Tuple[Tuple[int, int], ...]]:
    absolute = _absolute_path(path)
    descriptor = os.open(os.sep, _directory_flags())
    identities = []
    try:
        for component in absolute.parts[1:]:
            child = os.open(component, _directory_flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise ResearchContractError("path ancestor is not a directory")
            identities.append((metadata.st_dev, metadata.st_ino))
        return descriptor, tuple(identities)
    except OSError as error:
        os.close(descriptor)
        raise ResearchContractError("path ancestor is missing or a symlink") from error
    except BaseException:
        os.close(descriptor)
        raise


def _recheck_directory_chain(path: Path, expected: Sequence[Tuple[int, int]]) -> None:
    descriptor, actual = _open_directory_chain(path)
    try:
        if tuple(expected) != actual:
            raise ResearchContractError("path ancestor identity changed")
    finally:
        os.close(descriptor)


def _stable_metadata(metadata: os.stat_result) -> Tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        metadata.st_nlink,
        metadata.st_size,
    )


def _read_bounded(descriptor: int) -> bytes:
    parts = []
    total = 0
    while True:
        block = os.read(descriptor, min(65536, MAX_JSON_BYTES + 1 - total))
        if not block:
            break
        total += len(block)
        if total > MAX_JSON_BYTES:
            raise ResearchContractError("JSON input exceeds 1 MiB limit")
        parts.append(block)
    return b"".join(parts)


def _preflight_json_bytes(raw: bytes) -> None:
    depth = 0
    members = 0
    index = 0
    in_string = False
    escaped = False
    string_start = 0
    length = len(raw)
    while index < length:
        byte = raw[index]
        if in_string:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                if index - string_start > MAX_JSON_STRING_TOKEN_BYTES:
                    raise ResearchContractError("JSON string token is too large")
                in_string = False
            index += 1
            continue
        if byte == 34:
            in_string = True
            string_start = index
        elif byte in (91, 123):
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise ResearchContractError("JSON nesting is too deep")
        elif byte in (93, 125):
            depth -= 1
            if depth < 0:
                raise ResearchContractError("JSON nesting is invalid")
        elif byte == 58:
            members += 1
            if members > MAX_JSON_MEMBERS:
                raise ResearchContractError("JSON has too many members")
        elif byte == 45 or 48 <= byte <= 57:
            start = index
            while index < length and raw[index] not in b" \t\r\n,]}":
                index += 1
            if index - start > MAX_JSON_INTEGER_TOKEN_BYTES:
                raise ResearchContractError("JSON number token is too large")
            continue
        index += 1
    if in_string or depth != 0:
        raise ResearchContractError("JSON structure is invalid")


def _reject_duplicate_keys(pairs):
    value = {}
    for key, child in pairs:
        if key in value:
            raise ResearchContractError("duplicate JSON key")
        value[key] = child
    return value


def _bounded_int(token: str) -> int:
    if len(token) > MAX_JSON_INTEGER_TOKEN_BYTES:
        raise ResearchContractError("JSON integer token is too large")
    return int(token)


def _reject_float(_token: str) -> float:
    raise ResearchContractError("JSON binary-float and exponent tokens are forbidden")


def _reject_constant(_token: str) -> object:
    raise ResearchContractError("JSON nonfinite tokens are forbidden")


def _parse_bounded_canonical_json(raw: bytes, label: str) -> object:
    if len(raw) > MAX_JSON_BYTES:
        raise ResearchContractError("JSON input exceeds 1 MiB limit")
    _preflight_json_bytes(raw)
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_int=_bounded_int,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResearchContractError("{} is invalid JSON".format(label)) from error
    scan_public_payload(value)
    if raw != canonical_json_bytes(value) + b"\n":
        raise ResearchContractError("{} is not canonical JSON".format(label))
    return value


def load_bounded_json(path: Path, label: str) -> object:
    """Read one canonical, bounded JSON file without following symlinks."""
    if not isinstance(path, Path) or not isinstance(label, str) or not label:
        raise ResearchContractError("JSON source arguments are invalid")
    absolute = _absolute_path(path)
    if absolute.name in {"", ".", ".."}:
        raise ResearchContractError("JSON source filename is invalid")
    directory_fd, ancestors = _open_directory_chain(absolute.parent)
    descriptor = -1
    try:
        try:
            path_before = os.stat(
                absolute.name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as error:
            raise ResearchContractError("{} is not a regular file".format(label)) from error
        if not stat.S_ISREG(path_before.st_mode) or path_before.st_nlink != 1:
            raise ResearchContractError("{} is not a regular file".format(label))
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise ResearchContractError("secure file open is unavailable")
        descriptor = os.open(absolute.name, flags | nofollow, dir_fd=directory_fd)
        descriptor_before = os.fstat(descriptor)
        if _stable_metadata(path_before) != _stable_metadata(descriptor_before):
            raise ResearchContractError("{} identity changed before read".format(label))
        if descriptor_before.st_size > MAX_JSON_BYTES:
            raise ResearchContractError("JSON input exceeds 1 MiB limit")
        raw = _read_bounded(descriptor)
        descriptor_after = os.fstat(descriptor)
        path_after = os.stat(absolute.name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            _stable_metadata(descriptor_before) != _stable_metadata(descriptor_after)
            or _stable_metadata(descriptor_before) != _stable_metadata(path_after)
        ):
            raise ResearchContractError("{} identity changed during read".format(label))
    except OSError as error:
        raise ResearchContractError("{} could not be read safely".format(label)) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory_fd)
    _recheck_directory_chain(absolute.parent, ancestors)
    return _parse_bounded_canonical_json(raw, label)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("staged JSON write made no progress")
        view = view[written:]


def atomic_write_canonical_json(path: Path, payload: object) -> None:
    """Atomically replace one safe destination with canonical public JSON."""
    if not isinstance(path, Path):
        raise ResearchContractError("JSON destination is invalid")
    scan_public_payload(payload)
    rendered = canonical_json_bytes(payload) + b"\n"
    _parse_bounded_canonical_json(rendered, "JSON output")
    absolute = _absolute_path(path)
    if absolute.name in {"", ".", ".."}:
        raise ResearchContractError("JSON destination filename is invalid")
    directory_fd, ancestors = _open_directory_chain(absolute.parent)
    staged_name = ".{}.{}.stage".format(absolute.name, uuid.uuid4().hex)
    staged_fd = -1
    try:
        try:
            existing = os.stat(
                absolute.name, dir_fd=directory_fd, follow_symlinks=False
            )
        except FileNotFoundError:
            existing = None
        if existing is not None and (
            not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1
        ):
            raise ResearchContractError("JSON destination is not a regular file")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise ResearchContractError("secure file open is unavailable")
        staged_fd = os.open(staged_name, flags | nofollow, 0o600, dir_fd=directory_fd)
        staged_metadata = os.fstat(staged_fd)
        if not stat.S_ISREG(staged_metadata.st_mode) or staged_metadata.st_nlink != 1:
            raise ResearchContractError("staged JSON destination is not a regular file")
        _write_all(staged_fd, rendered)
        os.fsync(staged_fd)
        os.close(staged_fd)
        staged_fd = -1
        os.replace(
            staged_name,
            absolute.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    except OSError as error:
        raise ResearchContractError("JSON destination could not be replaced safely") from error
    finally:
        if staged_fd >= 0:
            os.close(staged_fd)
        try:
            os.unlink(staged_name, dir_fd=directory_fd)
        except OSError:
            pass
        os.close(directory_fd)
    _recheck_directory_chain(absolute.parent, ancestors)
