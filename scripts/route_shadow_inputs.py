"""Build one source-bound private route universe from captured publications.

The shadow runner must not hash one filesystem generation and parse another.
Every parser therefore consumes a private, unlinked descriptor-backed capture;
SQLite is opened through a private named staging copy of that capture and raw
text bytes are released one bounded source at a time.
"""

from __future__ import annotations

import csv
import ctypes
import errno
import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import sys
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

try:
    from scripts.cex_instrument_lifecycle import (
        validate_cex_instrument_lifecycle_review,
    )
    from scripts.execution_cost import (
        EXECUTION_COST_COLUMNS,
        validate_execution_snapshot,
    )
    from scripts.fetch_cex_depth import (
        DEPTH_COLUMNS_ALL as CEX_DEPTH_COLUMNS,
        validate_snapshot as validate_cex_depth_snapshot,
    )
    from scripts.fetch_dex_depth import (
        DEX_DEPTH_COLUMNS,
        validate_snapshot as validate_dex_depth_snapshot,
    )
    from scripts.fetch_tvl import (
        TVL_COLUMNS,
        validate_snapshot as validate_tvl_snapshot,
    )
    from scripts.route_universe import build_route_universe, route_universe_sha256
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
    from scripts.token_registry import (
        TokenRegistryError,
        normalize_contract_address,
        normalize_chain,
        validate_registry_payload,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cex_instrument_lifecycle import (  # type: ignore[no-redef]
        validate_cex_instrument_lifecycle_review,
    )
    from execution_cost import (  # type: ignore[no-redef]
        EXECUTION_COST_COLUMNS,
        validate_execution_snapshot,
    )
    from fetch_cex_depth import (  # type: ignore[no-redef]
        DEPTH_COLUMNS_ALL as CEX_DEPTH_COLUMNS,
        validate_snapshot as validate_cex_depth_snapshot,
    )
    from fetch_dex_depth import (  # type: ignore[no-redef]
        DEX_DEPTH_COLUMNS,
        validate_snapshot as validate_dex_depth_snapshot,
    )
    from fetch_tvl import (  # type: ignore[no-redef]
        TVL_COLUMNS,
        validate_snapshot as validate_tvl_snapshot,
    )
    from route_universe import (  # type: ignore[no-redef]
        build_route_universe,
        route_universe_sha256,
    )
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore
    from token_registry import (  # type: ignore[no-redef]
        TokenRegistryError,
        normalize_contract_address,
        normalize_chain,
        validate_registry_payload,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CALCULATION_VERSION = "route_shadow_inputs/v1"
BASELINE_MANIFEST_SCHEMA = "route_shadow_baseline_manifest/v1"
WINDOW_DAYS = 30
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_SQLITE_BYTES = 192 * 1024 * 1024
MAX_CONFIG_BYTES = 4 * 1024 * 1024
MAX_AGGREGATE_SOURCE_BYTES = 256 * 1024 * 1024

TYPED_SOURCE_LINEAGE_SCHEMA = "route_leg_typed_source_lineage/v1"
TYPED_SOURCE_MANIFEST_SCHEMA = "route_typed_source_manifest/v1"
TYPED_SOURCE_LINEAGE_FIELDS = frozenset({"schema", "members"})
TYPED_SOURCE_MEMBER_FIELDS = frozenset({
    "role", "status", "reason_code", "filename", "sha256", "size",
    "logical_generation", "adapter_id", "content_schema",
})
TYPED_SOURCE_MANIFEST_FIELDS = frozenset({
    "schema", "raw_evidence_run_id", "member_count", "members",
})
TYPED_SOURCE_MANIFEST_MEMBER_FIELDS = frozenset({
    "market_id", "role", "filename", "sha256", "size",
    "logical_generation", "adapter_id", "content_schema",
})
TYPED_SOURCE_UNAVAILABLE_REASONS = frozenset({
    "typed_source_missing",
    "typed_source_not_found",
    "typed_source_failed",
    "typed_source_adapter_unsupported",
    "typed_source_validation_failed",
})
MAX_TYPED_SOURCE_AGGREGATE_BYTES = 24 * 1024 * 1024

# The values are deliberately immutable and literal.  Task 4 consumes these
# contracts through the exported validator rather than reflecting over the
# collector implementation.
TYPED_SOURCE_ROLE_CONTRACTS = MappingProxyType({
    "cex_raw_book_response": MappingProxyType({
        "market_type": "cex",
        "adapter_id": "fetch_cex_depth/parse_book/v1",
        "content_schema": "route_bytes/v1",
        "max_bytes": 8 * 1024 * 1024,
    }),
    "cex_market_rules": MappingProxyType({
        "market_type": "cex",
        "adapter_id": "route_quantity_quote_for_book/v1",
        "content_schema": "route_market_rules_source/v1",
        "max_bytes": 256 * 1024,
    }),
    "quote_usd_conversion": MappingProxyType({
        "market_type": "cex",
        "adapter_id": "route_usd_conversion_source/v1",
        "content_schema": "route_usd_conversion_source/v1",
        "max_bytes": 256 * 1024,
    }),
    "dex_pool_state": MappingProxyType({
        "market_type": "dex",
        "adapter_id": "route_quantity_quote_for_v2_pool/v1",
        "content_schema": "route_v2_pool_state/v1",
        "max_bytes": 1024 * 1024,
    }),
    "dex_usd_price_context": MappingProxyType({
        "market_type": "dex",
        "adapter_id": "route_dex_usd_price_context/v1",
        "content_schema": "route_dex_usd_price_context/v1",
        "max_bytes": 1024 * 1024,
    }),
})

_TYPED_ROLES_BY_MARKET_TYPE = MappingProxyType({
    market_type: tuple(sorted(
        role for role, contract in TYPED_SOURCE_ROLE_CONTRACTS.items()
        if contract["market_type"] == market_type
    ))
    for market_type in ("cex", "dex")
})
_TYPED_BASENAME = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII
)

_DATA_INPUTS = (
    ("market_facts.sqlite3", "market_facts.sqlite3", MAX_SQLITE_BYTES),
    ("cex_instrument_lifecycle.json", "cex_instrument_lifecycle.json", MAX_SOURCE_BYTES),
    ("admin/token_registry.json", "admin/token_registry.json", MAX_SOURCE_BYTES),
    ("cex_exchange_volume_daily.csv", "cex_exchange_volume_daily.csv", MAX_SOURCE_BYTES),
    ("cex_depth_latest.csv", "cex_depth_latest.csv", MAX_SOURCE_BYTES),
    ("dex_depth_latest.csv", "dex_depth_latest.csv", MAX_SOURCE_BYTES),
    ("cex_execution_cost_latest.csv", "cex_execution_cost_latest.csv", MAX_SOURCE_BYTES),
    ("dex_execution_cost_latest.csv", "dex_execution_cost_latest.csv", MAX_SOURCE_BYTES),
    ("dex_pool_tvl_latest.csv", "dex_pool_tvl_latest.csv", MAX_SOURCE_BYTES),
)
_CONFIG_LOGICAL_PATH = "config/tokens.csv"
_CHAIN_CONFIG_LOGICAL_PATH = "config/token_chains.csv"
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_RUN_ID_PATTERN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII
)
_CEX_PART = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII)
_LOWER_PART = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z", flags=re.ASCII)


@dataclass(frozen=True)
class SourceFileIdentity:
    path: str
    size: int
    sha256: str


@dataclass
class _CapturedSource:
    identity: SourceFileIdentity
    descriptor: int

    def close(self) -> None:
        descriptor = self.descriptor
        if descriptor >= 0:
            self.descriptor = -1
            os.close(descriptor)


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def validate_typed_source_lineage(
    value: Mapping[str, Any], *, market_type: str
) -> Dict[str, Any]:
    """Validate and return a detached canonical typed-source lineage object."""
    if market_type not in _TYPED_ROLES_BY_MARKET_TYPE:
        raise ValueError("typed-source market type is invalid")
    if (
        not isinstance(value, Mapping)
        or set(value) != TYPED_SOURCE_LINEAGE_FIELDS
        or value.get("schema") != TYPED_SOURCE_LINEAGE_SCHEMA
        or not isinstance(value.get("members"), list)
    ):
        raise ValueError("typed-source lineage schema is invalid")
    expected_roles = _TYPED_ROLES_BY_MARKET_TYPE[market_type]
    raw_members = value["members"]
    if len(raw_members) > 5:
        raise ValueError("typed-source member count is invalid")
    roles = [
        member.get("role") if isinstance(member, Mapping) else None
        for member in raw_members
    ]
    if tuple(roles) != expected_roles:
        raise ValueError("typed-source roles are invalid for market type")

    aggregate_size = 0
    members: List[Dict[str, Any]] = []
    for raw in raw_members:
        if not isinstance(raw, Mapping) or set(raw) != TYPED_SOURCE_MEMBER_FIELDS:
            raise ValueError("typed-source member schema is invalid")
        role = raw.get("role")
        contract = TYPED_SOURCE_ROLE_CONTRACTS.get(role)
        if contract is None or contract["market_type"] != market_type:
            raise ValueError("typed-source role conflicts with market type")
        if (
            raw.get("adapter_id") != contract["adapter_id"]
            or raw.get("content_schema") != contract["content_schema"]
        ):
            raise ValueError("typed-source adapter contract is invalid")
        status = raw.get("status")
        if status == "observed":
            filename = raw.get("filename")
            size = raw.get("size")
            if (
                raw.get("reason_code") is not None
                or not isinstance(filename, str)
                or _TYPED_BASENAME.fullmatch(filename) is None
                or filename in {".", ".."}
                or len(filename.encode("ascii")) > 128
                or not isinstance(raw.get("sha256"), str)
                or _HASH_PATTERN.fullmatch(raw["sha256"]) is None
                or not isinstance(raw.get("logical_generation"), str)
                or _HASH_PATTERN.fullmatch(raw["logical_generation"]) is None
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size <= 0
                or size > contract["max_bytes"]
            ):
                raise ValueError("typed-source observed filename, hash, or size is invalid")
            aggregate_size += size
        elif status == "unavailable":
            if (
                raw.get("reason_code") not in TYPED_SOURCE_UNAVAILABLE_REASONS
                or any(
                    raw.get(field) is not None
                    for field in (
                        "filename", "sha256", "size", "logical_generation"
                    )
                )
            ):
                raise ValueError("typed-source unavailable null/reason matrix is invalid")
        else:
            raise ValueError("typed-source status is invalid")
        members.append({field: raw[field] for field in (
            "role", "status", "reason_code", "filename", "sha256", "size",
            "logical_generation", "adapter_id", "content_schema",
        )})
    if aggregate_size > MAX_TYPED_SOURCE_AGGREGATE_BYTES:
        raise ValueError("typed-source aggregate bytes exceed the bound")
    return {"schema": TYPED_SOURCE_LINEAGE_SCHEMA, "members": members}


def typed_source_lineage_observed_members(
    value: Mapping[str, Any], *, market_type: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Return detached observed members in the exact manifest projection."""
    if market_type is None:
        if not isinstance(value, Mapping) or not isinstance(value.get("members"), list):
            raise ValueError("typed-source lineage is invalid")
        roles = {
            member.get("role")
            for member in value["members"]
            if isinstance(member, Mapping)
        }
        candidates = [
            kind for kind, expected in _TYPED_ROLES_BY_MARKET_TYPE.items()
            if roles == set(expected)
        ]
        if len(candidates) != 1:
            raise ValueError("typed-source market type cannot be inferred")
        market_type = candidates[0]
    lineage = validate_typed_source_lineage(value, market_type=market_type)
    return [
        {field: member[field] for field in (
            "role", "filename", "sha256", "size", "logical_generation",
            "adapter_id", "content_schema",
        )}
        for member in lineage["members"]
        if member["status"] == "observed"
    ]


def selection_window(now: datetime) -> Dict[str, str]:
    """Return the rolling 30 complete UTC calendar days ending yesterday."""
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("selection clock must be timezone-aware")
    utc_day = now.astimezone(timezone.utc).date()
    end = utc_day - timedelta(days=1)
    start = end - timedelta(days=WINDOW_DAYS - 1)
    return {"start": start.isoformat(), "end": end.isoformat()}


def _identity_record(identity: SourceFileIdentity) -> Dict[str, Any]:
    if (
        not isinstance(identity.path, str)
        or not identity.path
        or identity.path.startswith("/")
        or "\\" in identity.path
        or any(part in {"", ".", ".."} for part in identity.path.split("/"))
        or isinstance(identity.size, bool)
        or not isinstance(identity.size, int)
        or identity.size < 0
        or _HASH_PATTERN.fullmatch(identity.sha256 or "") is None
    ):
        raise ValueError("source identity is invalid")
    return {
        "path": identity.path,
        "size": identity.size,
        "sha256": identity.sha256,
    }


def _candidate_source_generation(
    identities: Iterable[SourceFileIdentity],
) -> str:
    records = sorted(
        (_identity_record(identity) for identity in identities),
        key=lambda row: row["path"],
    )
    paths = [row["path"] for row in records]
    if len(paths) != len(set(paths)) or not records:
        raise ValueError("source identities must be unique and non-empty")
    payload = {
        "calculation_version": CALCULATION_VERSION,
        "inputs": records,
    }
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _secure_directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ValueError("secure source directory open is unavailable")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _absolute_path(path: Path) -> Path:
    expanded = os.path.abspath(os.path.expanduser(os.fspath(path)))
    # Darwin exposes only these two system roots through compatibility links.
    # Normalize that fixed OS boundary without resolving any user-controlled
    # descendant, which must still be rejected by O_NOFOLLOW dirfd walking.
    if sys.platform == "darwin":
        if expanded == "/var" or expanded.startswith("/var/"):
            expanded = "/private" + expanded
        elif expanded == "/tmp" or expanded.startswith("/tmp/"):
            expanded = "/private" + expanded
    return Path(expanded)


def _open_directory_chain(path: Path) -> Tuple[int, Tuple[Tuple[int, int], ...]]:
    absolute = _absolute_path(path)
    flags = _secure_directory_flags()
    descriptor = os.open(os.sep, flags)
    identities = []
    try:
        root_metadata = os.fstat(descriptor)
        identities.append((root_metadata.st_dev, root_metadata.st_ino))
        for component in absolute.parts[1:]:
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as error:
                raise ValueError(
                    "source parent is missing, changed, or a symlink"
                ) from error
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("source parent is not a regular directory")
            identities.append((metadata.st_dev, metadata.st_ino))
        return descriptor, tuple(identities)
    except BaseException:
        os.close(descriptor)
        raise


def _recheck_directory_chain(
    path: Path, expected: Sequence[Tuple[int, int]]
) -> None:
    descriptor, actual = _open_directory_chain(path)
    try:
        if tuple(expected) != actual:
            raise ValueError("source parent directory identity changed")
    finally:
        os.close(descriptor)


def _descriptor_sha256(descriptor: int, maximum_bytes: int) -> Tuple[int, str]:
    digest = hashlib.sha256()
    offset = 0
    while True:
        block = os.pread(
            descriptor,
            min(1024 * 1024, maximum_bytes + 1 - offset),
            offset,
        )
        if not block:
            break
        offset += len(block)
        if offset > maximum_bytes:
            raise ValueError("source exceeds the bounded input limit")
        digest.update(block)
    return offset, digest.hexdigest()


def _new_unlinked_capture() -> int:
    descriptor, path = tempfile.mkstemp(prefix="route-shadow-capture-")
    try:
        os.unlink(path)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _copy_source_descriptor(
    source_descriptor: int,
    capture_descriptor: int,
    maximum_bytes: int,
) -> Tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    while True:
        block = os.read(
            source_descriptor,
            min(1024 * 1024, maximum_bytes + 1 - total),
        )
        if not block:
            break
        total += len(block)
        if total > maximum_bytes:
            raise ValueError("source exceeds the bounded input limit")
        digest.update(block)
        view = memoryview(block)
        offset = 0
        while offset < len(view):
            written = os.write(capture_descriptor, view[offset:])
            if written <= 0:
                raise OSError("captured source write made no progress")
            offset += written
    os.fsync(capture_descriptor)
    return total, digest.hexdigest()


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


def _capture_entry(
    parent_descriptor: int,
    name: str,
    logical_path: str,
    maximum_bytes: int,
) -> _CapturedSource:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError("source entry is invalid")
    try:
        path_before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        raise FileNotFoundError("required source is missing: {}".format(logical_path))
    if not stat.S_ISREG(path_before.st_mode) or path_before.st_nlink != 1:
        raise ValueError(
            "required source must be a single-link regular non-symlink file"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("secure source open is unavailable")
    flags |= nofollow
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise ValueError("required source changed or is a symlink") from error
    capture_descriptor = -1
    try:
        descriptor_before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(descriptor_before.st_mode)
            or _stable_file_metadata(path_before)
            != _stable_file_metadata(descriptor_before)
        ):
            raise ValueError("required source path and descriptor identity differ")
        if descriptor_before.st_size > maximum_bytes:
            raise ValueError("source exceeds the bounded input limit")
        capture_descriptor = _new_unlinked_capture()
        captured_size, captured_sha = _copy_source_descriptor(
            descriptor,
            capture_descriptor,
            maximum_bytes,
        )
        descriptor_after = os.fstat(descriptor)
        source_size_after, source_sha_after = _descriptor_sha256(
            descriptor,
            maximum_bytes,
        )
        try:
            path_after = os.stat(
                name, dir_fd=parent_descriptor, follow_symlinks=False
            )
        except FileNotFoundError:
            raise ValueError("required source path identity changed") from None
        expected = _stable_file_metadata(descriptor_before)
        if (
            expected != _stable_file_metadata(descriptor_after)
            or expected != _stable_file_metadata(path_after)
            or captured_size != descriptor_after.st_size
            or source_size_after != captured_size
            or source_sha_after != captured_sha
        ):
            raise ValueError("required source changed while it was captured")
        capture_metadata = os.fstat(capture_descriptor)
        verified_size, verified_sha = _descriptor_sha256(
            capture_descriptor,
            maximum_bytes,
        )
        if (
            not stat.S_ISREG(capture_metadata.st_mode)
            or capture_metadata.st_nlink != 0
            or capture_metadata.st_size != captured_size
            or verified_size != captured_size
            or verified_sha != captured_sha
        ):
            raise ValueError("private source capture identity is invalid")
    except BaseException:
        if capture_descriptor >= 0:
            os.close(capture_descriptor)
        raise
    finally:
        os.close(descriptor)
    identity = SourceFileIdentity(
        path=logical_path,
        size=captured_size,
        sha256=captured_sha,
    )
    return _CapturedSource(identity=identity, descriptor=capture_descriptor)


def _verify_capture(capture: _CapturedSource) -> None:
    if capture.descriptor < 0:
        raise ValueError("private source capture is closed")
    metadata = os.fstat(capture.descriptor)
    size, digest = _descriptor_sha256(
        capture.descriptor,
        capture.identity.size,
    )
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 0
        or metadata.st_size != capture.identity.size
        or size != capture.identity.size
        or digest != capture.identity.sha256
    ):
        raise ValueError("private source capture bytes or identity changed")


def _capture_bytes(capture: _CapturedSource) -> bytes:
    _verify_capture(capture)
    duplicate = os.dup(capture.descriptor)
    os.lseek(duplicate, 0, os.SEEK_SET)
    with os.fdopen(duplicate, "rb") as handle:
        payload = handle.read(capture.identity.size + 1)
    if len(payload) != capture.identity.size:
        raise ValueError("private source capture size changed")
    if hashlib.sha256(payload).hexdigest() != capture.identity.sha256:
        raise ValueError("private source capture bytes changed")
    _verify_capture(capture)
    return payload


def _reject_sqlite_sidecars(data_descriptor: int) -> None:
    for suffix in ("-wal", "-journal", "-shm"):
        name = "market_facts.sqlite3" + suffix
        try:
            os.stat(name, dir_fd=data_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise ValueError("unbound SQLite sidecar is present: {}".format(name))


def _capture_required_sources(
    data_dir: Path, static_token_config: Path
) -> Tuple[_CapturedSource, ...]:
    data_path = _absolute_path(Path(data_dir))
    config_path = _absolute_path(Path(static_token_config))
    expected_config = _absolute_path(PROJECT_ROOT / "config/tokens.csv")
    try:
        same_config = os.path.realpath(str(config_path)) == os.path.realpath(
            str(expected_config)
        )
    except OSError as error:
        raise ValueError("static Token config is unavailable") from error
    if not same_config or config_path != expected_config:
        raise ValueError(
            "static_token_config must resolve exactly to tracked config/tokens.csv"
        )

    data_descriptor, data_chain = _open_directory_chain(data_path)
    admin_descriptor = -1
    config_descriptor = -1
    try:
        _reject_sqlite_sidecars(data_descriptor)
        try:
            admin_descriptor = os.open(
                "admin", _secure_directory_flags(), dir_fd=data_descriptor
            )
        except OSError as error:
            raise ValueError("admin source directory is missing or a symlink") from error
        config_descriptor, config_chain = _open_directory_chain(config_path.parent)
        captures = []
        aggregate_size = 0
        for logical_path, relative_path, maximum_bytes in _DATA_INPUTS:
            if "/" in relative_path:
                parent_descriptor = admin_descriptor
                name = relative_path.split("/", 1)[1]
            else:
                parent_descriptor = data_descriptor
                name = relative_path
            capture = _capture_entry(
                    parent_descriptor,
                    name,
                    logical_path,
                    maximum_bytes,
                )
            aggregate_size += capture.identity.size
            if aggregate_size > MAX_AGGREGATE_SOURCE_BYTES:
                capture.close()
                raise ValueError("aggregate source capture budget exceeded")
            captures.append(capture)
        for name, logical_path in (
            (config_path.name, _CONFIG_LOGICAL_PATH),
            ("token_chains.csv", _CHAIN_CONFIG_LOGICAL_PATH),
        ):
            capture = _capture_entry(
                config_descriptor,
                name,
                logical_path,
                MAX_CONFIG_BYTES,
            )
            aggregate_size += capture.identity.size
            if aggregate_size > MAX_AGGREGATE_SOURCE_BYTES:
                capture.close()
                raise ValueError("aggregate source capture budget exceeded")
            captures.append(capture)
        _reject_sqlite_sidecars(data_descriptor)
        data_metadata = os.fstat(data_descriptor)
        if (data_metadata.st_dev, data_metadata.st_ino) != data_chain[-1]:
            raise ValueError("source parent directory identity changed")
        admin_metadata = os.fstat(admin_descriptor)
        current_admin = os.stat(
            "admin", dir_fd=data_descriptor, follow_symlinks=False
        )
        if (
            (admin_metadata.st_dev, admin_metadata.st_ino)
            != (current_admin.st_dev, current_admin.st_ino)
            or not stat.S_ISDIR(current_admin.st_mode)
        ):
            raise ValueError("admin source directory identity changed")
        _recheck_directory_chain(data_path, data_chain)
        _recheck_directory_chain(config_path.parent, config_chain)
        return tuple(captures)
    except BaseException:
        for capture in locals().get("captures", []):
            capture.close()
        raise
    finally:
        if config_descriptor >= 0:
            os.close(config_descriptor)
        if admin_descriptor >= 0:
            os.close(admin_descriptor)
        os.close(data_descriptor)


def _decode_text(payload: bytes, label: str) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("{} is not valid UTF-8".format(label)) from error
    if "\x00" in text:
        raise ValueError("{} contains NUL bytes".format(label))
    return text


def _parse_json(payload: bytes, label: str) -> Any:
    try:
        return json.loads(_decode_text(payload, label))
    except json.JSONDecodeError as error:
        raise ValueError("{} is not valid JSON".format(label)) from error


def _parse_csv(
    payload: bytes, label: str, required_fields: Iterable[str]
) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(_decode_text(payload, label), newline=""))
    fields = list(reader.fieldnames or [])
    if len(fields) != len(set(fields)):
        raise ValueError("{} contains duplicate columns".format(label))
    missing = sorted(set(required_fields) - set(fields))
    if missing:
        raise ValueError(
            "{} is missing columns: {}".format(label, ", ".join(missing))
        )
    rows = []
    for row in reader:
        if None in row:
            raise ValueError("{} contains an over-wide row".format(label))
        rows.append({str(key): str(value or "") for key, value in row.items()})
    return rows


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("{} is missing or non-canonical".format(field))
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("{} contains control characters".format(field))
    return value


def _timestamp(value: Any, field: str) -> str:
    text = _required_text(value, field)
    try:
        exact_rfc3339_epoch_seconds(text)
    except (TypeError, ValueError) as error:
        raise ValueError("{} is not a timezone-aware timestamp".format(field)) from error
    return text


def _decimal_text(
    value: Any, field: str, *, positive: bool = False
) -> Optional[str]:
    if value is None or str(value).strip() == "":
        return None
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError("{} is not a finite Decimal".format(field)) from error
    if not number.is_finite() or number < 0 or (positive and number <= 0):
        raise ValueError("{} has an invalid sign or magnitude".format(field))
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _chain_native_token_id(value: Any, *, chain: str, field: str) -> str:
    """Return the canonical address bound to one collector Token ID.

    GeckoTerminal identifiers are serialized as ``<chain>_<address>``.  The
    address grammar is chain-specific: applying an EVM-only regular expression
    here would silently drop valid Starknet and Solana research legs, while
    accepting a 20-byte EVM address under a non-EVM prefix would create a false
    identity.  Reuse the captured Token-registry normalizer and require the
    source bytes to already be canonical.
    """
    text = _required_text(value, field)
    try:
        canonical_chain = normalize_chain(chain)
    except (TokenRegistryError, ValueError) as error:
        raise ValueError("collector Token chain is invalid") from error
    prefix, separator, address = text.partition("_")
    if not separator or prefix != canonical_chain or not address:
        raise ValueError("collector Token identity is invalid")
    try:
        canonical_address = normalize_contract_address(canonical_chain, address)
    except (TokenRegistryError, ValueError) as error:
        raise ValueError("collector Token identity is invalid") from error
    if canonical_address != address:
        raise ValueError("collector Token identity is non-canonical")
    return canonical_address


def _safe_source_endpoint(value: Any) -> str:
    text = _required_text(value, "source_endpoint")
    if len(text.encode("utf-8")) > 4096:
        raise ValueError("source_endpoint exceeds its byte limit")
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError("source_endpoint is unsafe") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("source_endpoint is unsafe")
    safe_host = "[{}]".format(hostname) if ":" in hostname else hostname
    if port is not None:
        safe_host = "{}:{}".format(safe_host, port)
    if urlunsplit((parsed.scheme, safe_host, parsed.path, "", "")) != text:
        raise ValueError("source_endpoint is non-canonical")
    return text


def _cex_market_id(row: Mapping[str, Any]) -> str:
    token = _required_text(row.get("token_symbol"), "token_symbol")
    exchange = _required_text(row.get("exchange"), "exchange")
    symbol = _required_text(row.get("cex_symbol"), "cex_symbol")
    parts = symbol.split("/")
    if (
        token != token.upper()
        or _CEX_PART.fullmatch(token) is None
        or exchange != exchange.lower()
        or _LOWER_PART.fullmatch(exchange) is None
        or len(parts) != 2
        or any(_CEX_PART.fullmatch(part) is None for part in parts)
        or parts[0] != token
        or symbol != symbol.upper()
    ):
        raise ValueError("CEX market identity is not canonical")
    return "cex:{}:{}".format(exchange, symbol)


def _dex_market_id(row: Mapping[str, Any]) -> str:
    token = _required_text(row.get("token_symbol"), "token_symbol")
    chain = _required_text(row.get("chain"), "chain")
    dex = _required_text(row.get("dex"), "dex")
    pool = _required_text(row.get("pool_address"), "pool_address")
    canonical_pool = pool.lower() if pool.startswith("0x") else pool
    if (
        token != token.upper()
        or _CEX_PART.fullmatch(token) is None
        or chain != chain.lower()
        or dex != dex.lower()
        or _LOWER_PART.fullmatch(chain) is None
        or _LOWER_PART.fullmatch(dex) is None
        or canonical_pool != pool
        or any(character in pool for character in ":/\\")
    ):
        raise ValueError("DEX market identity is not canonical")
    return "dex:{}:{}:{}:{}".format(chain, dex, pool, token)


def _parse_static_tokens(
    payload: bytes,
) -> Tuple[set, Tuple[str, ...], Dict[Tuple[str, str], str]]:
    rows = _parse_csv(
        payload,
        _CONFIG_LOGICAL_PATH,
        {"token_symbol", "cex_symbol"},
    )
    if not rows:
        raise ValueError("tracked Token config is empty")
    symbols = set()
    crypto_markets = []
    identities: Dict[Tuple[str, str], str] = {}
    for row in rows:
        token = _required_text(row.get("token_symbol"), "token_symbol")
        if token != token.upper() or _CEX_PART.fullmatch(token) is None:
            raise ValueError("tracked Token symbol is not canonical")
        if token in symbols:
            raise ValueError("tracked Token config contains duplicate symbols")
        symbols.add(token)
        chain = _required_text(row.get("chain"), "chain")
        address = _required_text(row.get("contract_address"), "contract_address")
        try:
            normalized_chain = normalize_chain(chain)
            normalized_address = normalize_contract_address(
                normalized_chain, address
            )
        except (TokenRegistryError, ValueError) as error:
            raise ValueError("tracked Token identity is invalid") from error
        identities[(token, normalized_chain)] = normalized_address
        crypto_markets.append(_cex_market_id({
            "token_symbol": token,
            "exchange": "crypto_com",
            "cex_symbol": row.get("cex_symbol"),
        }))
    return symbols, tuple(sorted(crypto_markets)), identities


def _parse_token_chain_identities(
    payload: bytes, *, configured_tokens: set
) -> Dict[Tuple[str, str], str]:
    rows = _parse_csv(
        payload,
        _CHAIN_CONFIG_LOGICAL_PATH,
        {"token_symbol", "chain", "contract_address", "notes"},
    )
    if not rows:
        raise ValueError("tracked Token-chain config is empty")
    identities: Dict[Tuple[str, str], str] = {}
    for row in rows:
        token = _required_text(row.get("token_symbol"), "token_symbol")
        if (
            token != token.upper()
            or _CEX_PART.fullmatch(token) is None
            or token not in configured_tokens
        ):
            raise ValueError("tracked Token-chain symbol is invalid")
        try:
            chain = normalize_chain(row.get("chain"))
            address = normalize_contract_address(
                chain, row.get("contract_address")
            )
        except (TokenRegistryError, ValueError) as error:
            raise ValueError("tracked Token-chain identity is invalid") from error
        key = (token, chain)
        if key in identities:
            raise ValueError(
                "tracked Token-chain config contains duplicate identities"
            )
        identities[key] = address
    return identities


def _parse_runtime_registry(
    payload: bytes,
) -> Tuple[set, Tuple[str, ...], Dict[Tuple[str, str], str]]:
    normalized = validate_registry_payload(
        _parse_json(payload, "admin/token_registry.json")
    )
    symbols = set()
    crypto_markets = []
    identities: Dict[Tuple[str, str], str] = {}
    for record in normalized["tokens"].values():
        if record.get("status") != "active":
            continue
        token = str(record["token_symbol"])
        symbols.add(token)
        identities[(token, str(record["chain"]))] = str(record["contract_address"])
        mapping = record.get("cex_mapping") or {}
        if (
            mapping.get("status") == "approved"
            and "crypto_com" in mapping.get("exchanges", [])
        ):
            crypto_markets.append(_cex_market_id({
                "token_symbol": token,
                "exchange": "crypto_com",
                "cex_symbol": mapping.get("cex_symbol"),
            }))
    return symbols, tuple(sorted(crypto_markets)), identities


def _market_ids_sha256(values: Sequence[str]) -> str:
    encoded = json.dumps(
        tuple(sorted(values)), ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _parse_lifecycle(payload: bytes, configured_ids: Sequence[str]) -> set:
    value = _parse_json(payload, "cex_instrument_lifecycle.json")
    required = {
        "schema", "generated_at_utc", "checked_at_utc", "response_sha256",
        "inventory_count", "configured_market_count",
        "configured_market_ids_sha256", "review_count", "reviews",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("CEX lifecycle manifest fields are invalid")
    if value["schema"] != "cex_instrument_lifecycle/v1":
        raise ValueError("CEX lifecycle schema is invalid")
    _timestamp(value["generated_at_utc"], "lifecycle generated_at_utc")
    _timestamp(value["checked_at_utc"], "lifecycle checked_at_utc")
    if (
        not isinstance(value["response_sha256"], str)
        or _HASH_PATTERN.fullmatch(value["response_sha256"]) is None
    ):
        raise ValueError("CEX lifecycle response SHA is invalid")
    reviews = value["reviews"]
    if not isinstance(reviews, list):
        raise ValueError("CEX lifecycle reviews are invalid")
    expected_ids = tuple(sorted(configured_ids))
    if (
        isinstance(value["inventory_count"], bool)
        or not isinstance(value["inventory_count"], int)
        or value["inventory_count"] <= 0
        or value["configured_market_count"] != len(expected_ids)
        or value["configured_market_ids_sha256"] != _market_ids_sha256(expected_ids)
        or value["review_count"] != len(reviews)
    ):
        raise ValueError("CEX lifecycle manifest is not bound to Token config")
    withheld = set()
    for raw_review in reviews:
        review = validate_cex_instrument_lifecycle_review(raw_review)
        market_id = review["market_id"]
        if market_id not in expected_ids or market_id in withheld:
            raise ValueError("CEX lifecycle review identity is invalid")
        withheld.add(market_id)
    return withheld


@contextmanager
def _stage_sqlite_capture(capture: _CapturedSource) -> Iterator[str]:
    _verify_capture(capture)
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("secure SQLite staging is unavailable")
    with tempfile.TemporaryDirectory(prefix="route-shadow-sqlite-") as directory:
        os.chmod(directory, 0o700)
        directory_metadata = os.stat(directory, follow_symlinks=False)
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or stat.S_IMODE(directory_metadata.st_mode) != 0o700
        ):
            raise ValueError("private SQLite staging directory is invalid")

        path = Path(directory) / "market_facts.sqlite3"
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | nofollow
            | getattr(os, "O_CLOEXEC", 0)
        )
        descriptor = os.open(str(path), flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            digest = hashlib.sha256()
            total = 0
            while True:
                block = os.pread(
                    capture.descriptor,
                    min(1024 * 1024, capture.identity.size + 1 - total),
                    total,
                )
                if not block:
                    break
                total += len(block)
                if total > capture.identity.size:
                    raise ValueError("private SQLite staging size changed")
                digest.update(block)
                view = memoryview(block)
                offset = 0
                while offset < len(view):
                    written = os.write(descriptor, view[offset:])
                    if written <= 0:
                        raise OSError("private SQLite staging write made no progress")
                    offset += written
            os.fsync(descriptor)
            metadata = os.fstat(descriptor)
            path_metadata = os.stat(str(path), follow_symlinks=False)
            verified_size, verified_sha = _descriptor_sha256(
                descriptor,
                capture.identity.size,
            )
            if (
                not stat.S_ISREG(metadata.st_mode)
                or not stat.S_ISREG(path_metadata.st_mode)
                or metadata.st_nlink != 1
                or path_metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or stat.S_IMODE(path_metadata.st_mode) != 0o600
                or (metadata.st_dev, metadata.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
                or metadata.st_size != capture.identity.size
                or path_metadata.st_size != capture.identity.size
                or total != capture.identity.size
                or digest.hexdigest() != capture.identity.sha256
                or verified_size != capture.identity.size
                or verified_sha != capture.identity.sha256
            ):
                raise ValueError("private SQLite staging identity is invalid")
        finally:
            os.close(descriptor)
        _verify_capture(capture)
        yield "{}?mode=ro&immutable=1".format(path.as_uri())


def _parse_sqlite(
    capture: _CapturedSource,
    cex_identity: SourceFileIdentity,
    allowed_tokens: set,
    lifecycle_withheld: set,
) -> Tuple[List[Dict[str, Any]], str]:
    _verify_capture(capture)
    with _stage_sqlite_capture(capture) as uri:
        _verify_capture(capture)
        try:
            connection = sqlite3.connect(uri, uri=True)
        except sqlite3.Error as error:
            raise ValueError("captured SQLite state is invalid") from error
        connection.row_factory = sqlite3.Row
        try:
            _verify_capture(capture)
            try:
                integrity = connection.execute("PRAGMA integrity_check").fetchall()
                if len(integrity) != 1 or integrity[0][0] != "ok":
                    raise ValueError("captured SQLite integrity check failed")
                required_tables = {
                    "schema_migrations", "dataset_snapshots", "import_runs",
                    "dataset_state", "tokens", "cex_market_daily",
                    "dex_pool_daily",
                }
                actual_tables = {
                    str(row[0]) for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if not required_tables <= actual_tables:
                    raise ValueError("captured SQLite schema is incomplete")
                user_version = connection.execute("PRAGMA user_version").fetchone()[0]
                migrations = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                if user_version != 1 or [row[0] for row in migrations] != [1]:
                    raise ValueError("captured SQLite schema version is unsupported")
                current_rows = connection.execute(
                    """
                    SELECT state.singleton_id, state.snapshot_id, state.import_run_id,
                           snapshot.cex_source_name, snapshot.cex_source_bytes,
                           snapshot.cex_sha256, snapshot.token_count,
                           snapshot.cex_row_count, snapshot.dex_row_count,
                           run.imported_at, run.status, run.snapshot_id AS run_snapshot_id
                    FROM dataset_state state
                    JOIN dataset_snapshots snapshot
                      ON snapshot.snapshot_id = state.snapshot_id
                    JOIN import_runs run
                      ON run.run_id = state.import_run_id
                    """
                ).fetchall()
                if len(current_rows) != 1:
                    raise ValueError("SQLite dataset current state is not unique")
                current = current_rows[0]
                if (
                    current["singleton_id"] != 1
                    or current["status"] != "published"
                    or current["run_snapshot_id"] != current["snapshot_id"]
                ):
                    raise ValueError("SQLite dataset current state is invalid")
                imported_at = _timestamp(
                    current["imported_at"], "SQLite import timestamp"
                )
                if (
                    current["cex_source_name"] != "cex_exchange_volume_daily.csv"
                    or current["cex_source_bytes"] != cex_identity.size
                    or current["cex_sha256"] != cex_identity.sha256
                ):
                    raise ValueError(
                        "SQLite current CEX CSV SHA/source binding is invalid"
                    )
                actual_cex_count = connection.execute(
                    "SELECT COUNT(*) FROM cex_market_daily"
                ).fetchone()[0]
                actual_dex_count = connection.execute(
                    "SELECT COUNT(*) FROM dex_pool_daily"
                ).fetchone()[0]
                actual_token_count = connection.execute(
                    "SELECT COUNT(*) FROM tokens"
                ).fetchone()[0]
                if actual_cex_count != current["cex_row_count"]:
                    raise ValueError("SQLite current CEX row count is invalid")
                if actual_dex_count != current["dex_row_count"]:
                    raise ValueError("SQLite current DEX row count is invalid")
                if actual_token_count != current["token_count"]:
                    raise ValueError("SQLite current Token row count is invalid")
                cex_rows = connection.execute(
                    "SELECT DISTINCT token_symbol, exchange, cex_symbol "
                    "FROM cex_market_daily"
                ).fetchall()
                dex_rows = connection.execute(
                    """
                    SELECT DISTINCT token_symbol, chain, dex, pool_address
                    FROM dex_pool_daily
                    """
                ).fetchall()
            except sqlite3.Error as error:
                raise ValueError("captured SQLite state is invalid") from error
        finally:
            connection.close()
    _verify_capture(capture)

    catalog = []
    seen = set()
    for row in list(cex_rows) + list(dex_rows):
        raw = dict(row)
        token = str(raw.get("token_symbol") or "")
        if token not in allowed_tokens:
            raise ValueError("SQLite catalog Token is absent from Token config")
        market_type = "cex" if "exchange" in raw else "dex"
        market_id = _cex_market_id(raw) if market_type == "cex" else _dex_market_id(raw)
        if market_id in seen:
            raise ValueError("SQLite catalog contains duplicate canonical market IDs")
        seen.add(market_id)
        withheld = market_id in lifecycle_withheld
        catalog.append({
            "market_id": market_id,
            "market_type": market_type,
            "token_symbol": token,
            "observed_at": imported_at,
            "lifecycle_status": "unavailable" if withheld else "active",
            "lifecycle_withheld": withheld,
            "execution_adapter_status": "supported",
        })
    if not catalog:
        raise ValueError("SQLite catalog contains no markets")
    catalog.sort(key=lambda row: row["market_id"])
    return catalog, imported_at


def _single_publication_snapshot_id(
    rows: Sequence[Mapping[str, Any]], label: str
) -> str:
    snapshot_ids = {
        _required_text(row.get("snapshot_id"), "{} snapshot_id".format(label))
        for row in rows
    }
    if len(snapshot_ids) != 1:
        raise ValueError("{} must contain one nonempty snapshot ID".format(label))
    return next(iter(snapshot_ids))


def _parse_cex_volume(
    payload: bytes,
    window: Mapping[str, str],
    imported_at: str,
) -> List[Dict[str, Any]]:
    rows = _parse_csv(
        payload,
        "cex_exchange_volume_daily.csv",
        {"date", "token_symbol", "exchange", "cex_symbol", "quote_volume_usd"},
    )
    totals = {}
    observed = set()
    for row in rows:
        market_id = _cex_market_id(row)
        date_text = _required_text(row.get("date"), "date")
        try:
            parsed = datetime.strptime(date_text, "%Y-%m-%d").date().isoformat()
        except ValueError as error:
            raise ValueError("CEX volume date is invalid") from error
        if parsed != date_text:
            raise ValueError("CEX volume date is non-canonical")
        key = (date_text, market_id)
        if key in observed:
            raise ValueError("CEX volume contains duplicate market-date rows")
        observed.add(key)
        if window["start"] <= date_text <= window["end"]:
            amount = _decimal_text(
                row.get("quote_volume_usd"), "quote_volume_usd"
            )
            if amount is not None:
                previous = totals.get(market_id)
                totals[market_id] = (
                    previous if previous is not None else Decimal(0)
                ) + Decimal(amount)
            elif market_id not in totals:
                totals[market_id] = None
    result = []
    for market_id in sorted(totals):
        amount = totals[market_id]
        result.append({
            "market_id": market_id,
            "selected_window_usd": (
                _decimal_text(amount, "selected_window_usd")
                if amount is not None else None
            ),
            "observed_at": imported_at,
        })
    return result


def _parse_depth(
    payload: bytes,
    *,
    market_type: str,
    expected_market_ids: set,
) -> Tuple[List[Dict[str, Any]], str]:
    if market_type == "cex":
        label = "cex_depth_latest.csv"
        builder = _cex_market_id
        required = CEX_DEPTH_COLUMNS
    else:
        label = "dex_depth_latest.csv"
        builder = _dex_market_id
        required = DEX_DEPTH_COLUMNS
    source_rows = _parse_csv(payload, label, required)
    market_ids = [builder(row) for row in source_rows]
    if len(market_ids) != len(set(market_ids)):
        raise ValueError("{} contains duplicate market rows".format(label))
    if set(market_ids) != expected_market_ids:
        raise ValueError("{} coverage does not match the catalog".format(label))
    snapshot_id = _single_publication_snapshot_id(source_rows, label)
    if market_type == "cex":
        validate_cex_depth_snapshot(
            source_rows,
            source_rows,
            allow_no_observed=True,
        )
    else:
        validate_dex_depth_snapshot(
            source_rows,
            source_rows,
            allow_no_observed=True,
        )
    projected = []
    for market_id, row in zip(market_ids, source_rows):
        status_text = _required_text(row.get("status"), "status")
        if market_type == "cex":
            state_time = _timestamp(row.get("observed_at"), "observed_at")
            directional = {
                "bid_depth_100bps_usd": _decimal_text(
                    row.get("bid_depth_100bps_usd"), "bid_depth_100bps_usd"
                ),
                "ask_depth_100bps_usd": _decimal_text(
                    row.get("ask_depth_100bps_usd"), "ask_depth_100bps_usd"
                ),
            }
        else:
            raw_block_time = row.get("block_timestamp")
            state_time = (
                _timestamp(raw_block_time, "block_timestamp")
                if raw_block_time else None
            )
            if status_text == "observed" and state_time is None:
                raise ValueError("observed DEX Depth lacks block timestamp")
            directional = {
                "buy_depth_100bps_usd": _decimal_text(
                    row.get("buy_depth_100bps_usd"), "buy_depth_100bps_usd"
                ),
                "sell_depth_100bps_usd": _decimal_text(
                    row.get("sell_depth_100bps_usd"), "sell_depth_100bps_usd"
                ),
            }
        projected.append({
            "market_id": market_id,
            "snapshot_id": snapshot_id,
            "status": status_text,
            "state_observed_at": state_time,
            "total_depth_100bps_usd": _decimal_text(
                row.get("total_depth_100bps_usd"), "total_depth_100bps_usd"
            ),
            **directional,
        })
    return projected, snapshot_id


def _parse_execution(
    payload: bytes,
    *,
    market_type: str,
    depth_snapshot_id: str,
    expected_market_ids: set,
) -> List[Dict[str, Any]]:
    label = "{}_execution_cost_latest.csv".format(market_type)
    rows = _parse_csv(
        payload,
        label,
        EXECUTION_COST_COLUMNS,
    )
    validate_execution_snapshot(
        expected_market_ids,
        rows,
        enforce_usd_price_timing=(market_type == "dex"),
    )
    _single_publication_snapshot_id(rows, label)
    source_snapshot_ids = {
        _required_text(row.get("source_snapshot_id"), "source_snapshot_id")
        for row in rows
    }
    if source_snapshot_ids != {depth_snapshot_id}:
        raise ValueError("Depth and Execution source snapshot lineage do not match")
    projected = []
    keys = set()
    for row in rows:
        expected_id = _cex_market_id(row) if market_type == "cex" else _dex_market_id(row)
        if row.get("market_type") != market_type or row.get("market_id") != expected_id:
            raise ValueError("Execution market identity is invalid")
        direction = _required_text(row.get("direction"), "direction")
        if direction not in {"buy_token", "sell_token"}:
            raise ValueError("Execution direction is invalid")
        status_text = _required_text(row.get("status"), "status")
        if status_text not in {"observed", "partial", "failed", "unsupported"}:
            raise ValueError("Execution status is invalid")
        source_snapshot = _required_text(
            row.get("source_snapshot_id"), "source_snapshot_id"
        )
        notional = _decimal_text(
            row.get("requested_notional_usd"),
            "requested_notional_usd",
            positive=True,
        )
        key = (expected_id, direction, notional)
        if key in keys:
            raise ValueError("Execution publication contains duplicate scenarios")
        keys.add(key)
        state_time = row.get("state_observed_at")
        state_time = _timestamp(state_time, "state_observed_at") if state_time else None
        if status_text in {"observed", "partial"} and state_time is None:
            raise ValueError("measured Execution row lacks state timestamp")
        projected.append({
            "market_id": expected_id,
            "direction": direction,
            "requested_notional_usd": notional,
            "status": status_text,
            "state_observed_at": state_time,
            "source_snapshot_id": source_snapshot,
        })
    return projected


def _parse_tvl_and_volume(
    payload: bytes,
    *,
    expected_market_ids: set,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    rows = _parse_csv(
        payload,
        "dex_pool_tvl_latest.csv",
        TVL_COLUMNS,
    )
    market_ids = [_dex_market_id(row) for row in rows]
    if len(market_ids) != len(set(market_ids)):
        raise ValueError("dex_pool_tvl_latest.csv contains duplicate market rows")
    if set(market_ids) != expected_market_ids:
        raise ValueError("TVL publication coverage does not match the catalog")
    _single_publication_snapshot_id(rows, "dex_pool_tvl_latest.csv")
    validate_tvl_snapshot(rows, rows, allow_no_observed=True)
    tvl_rows = []
    volume_rows = []
    collector_contexts = {}
    for market_id, row in zip(market_ids, rows):
        chain = _required_text(row.get("chain"), "chain")
        try:
            canonical_chain = normalize_chain(chain)
        except (TokenRegistryError, ValueError) as error:
            raise ValueError("collector Token chain is invalid") from error
        if canonical_chain != chain or market_id.split(":", 4)[1] != chain:
            raise ValueError("collector Token chain is non-canonical")
        request_started_at = _timestamp(
            row.get("request_started_at"), "request_started_at"
        )
        observed_at = _timestamp(row.get("observed_at"), "observed_at")
        response_received_at = _timestamp(
            row.get("response_received_at"), "response_received_at"
        )
        if not (
            exact_rfc3339_epoch_seconds(request_started_at)
            <= exact_rfc3339_epoch_seconds(observed_at)
            <= exact_rfc3339_epoch_seconds(response_received_at)
        ):
            raise ValueError("collector context timestamps are unordered")
        status_text = _required_text(row.get("status"), "status")
        tvl_value = None
        volume_value = None
        if status_text == "observed":
            tvl_value = _decimal_text(row.get("tvl_usd"), "tvl_usd")
            volume_value = _decimal_text(
                row.get("volume_24h_usd"), "volume_24h_usd"
            )
        elif str(row.get("volume_24h_usd") or "").strip():
            raise ValueError("non-observed TVL row cannot publish 24h Volume")
        binding = _required_text(row.get("snapshot_id"), "snapshot_id")
        observed = status_text == "observed"
        context_fields = (
            "base_token_id", "quote_token_id",
            "base_token_price_usd", "quote_token_price_usd",
        )
        if not observed and any(
            str(row.get(field) or "").strip() for field in context_fields
        ):
            raise ValueError(
                "non-observed collector context cannot retain Token or price fields"
            )
        base_token_id = None
        quote_token_id = None
        base_token_price = None
        quote_token_price = None
        if observed:
            base_token_id = _required_text(
                row.get("base_token_id"), "base_token_id"
            )
            quote_token_id = _required_text(
                row.get("quote_token_id"), "quote_token_id"
            )
            base_address = _chain_native_token_id(
                base_token_id, chain=chain, field="base_token_id"
            )
            quote_address = _chain_native_token_id(
                quote_token_id, chain=chain, field="quote_token_id"
            )
            if base_address == quote_address:
                raise ValueError("collector Token identities must be distinct")
            base_token_price = _decimal_text(
                row.get("base_token_price_usd"),
                "base_token_price_usd",
                positive=True,
            )
            quote_token_price = _decimal_text(
                row.get("quote_token_price_usd"),
                "quote_token_price_usd",
                positive=True,
            )
            if base_token_price is None or quote_token_price is None:
                raise ValueError("observed collector prices must be positive")
        raw_response_sha256 = _required_text(
            row.get("raw_response_sha256"), "raw_response_sha256"
        )
        if _HASH_PATTERN.fullmatch(raw_response_sha256) is None:
            raise ValueError("collector raw response SHA is invalid")
        context = {
            "schema": "route_collector_context/v1",
            "snapshot_id": binding,
            "request_started_at": request_started_at,
            "observed_at": observed_at,
            "response_received_at": response_received_at,
            "status": status_text,
            "reason_code": _required_text(row.get("reason_code"), "reason_code"),
            "pool_name": _required_text(row.get("pool_name"), "pool_name"),
            "base_token_id": base_token_id,
            "quote_token_id": quote_token_id,
            "base_token_price_usd": base_token_price,
            "quote_token_price_usd": quote_token_price,
            "tvl_method": _required_text(row.get("tvl_method"), "tvl_method"),
            "source": _required_text(row.get("source"), "source"),
            "source_endpoint": _safe_source_endpoint(
                row.get("source_endpoint")
            ),
            "raw_response_sha256": raw_response_sha256,
        }
        collector_contexts[market_id] = context
        tvl_rows.append({
            "market_id": market_id,
            "tvl_usd": tvl_value,
            "observed_at": observed_at,
            "source_snapshot_id": binding,
        })
        volume_rows.append({
            "market_id": market_id,
            "volume_24h_usd": volume_value,
            "observed_at": observed_at,
            "source_snapshot_id": binding,
        })
    return tvl_rows, volume_rows, collector_contexts


def _source_manifest(
    captures: Sequence[_CapturedSource], window: Mapping[str, str]
) -> Dict[str, Any]:
    identities = [capture.identity for capture in captures]
    generation = _candidate_source_generation(identities)
    end_exclusive = (
        datetime.strptime(window["end"], "%Y-%m-%d").date() + timedelta(days=1)
    ).isoformat()
    return {
        "schema": BASELINE_MANIFEST_SCHEMA,
        "calculation_version": CALCULATION_VERSION,
        "candidate_source_generation": generation,
        "selection_window": dict(window),
        "filters": {
            "window_days": WINDOW_DAYS,
            "calendar": "complete_utc_days",
            "cex_volume_aggregation": "sum_quote_volume_usd",
            "maximum_legs_per_token_market_type": 3,
        },
        "observation_bounds": {
            "start_inclusive": window["start"] + "T00:00:00Z",
            "end_exclusive": end_exclusive + "T00:00:00Z",
        },
        "inputs": [_identity_record(identity) for identity in identities],
    }


def build_shadow_universe(
    data_dir: Path,
    now: datetime,
    *,
    static_token_config: Path
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build one route universe and its exact immutable source manifest."""
    window = selection_window(now)
    captures = _capture_required_sources(Path(data_dir), Path(static_token_config))
    try:
        by_path = {capture.identity.path: capture for capture in captures}
        expected_paths = [item[0] for item in _DATA_INPUTS] + [
            _CONFIG_LOGICAL_PATH, _CHAIN_CONFIG_LOGICAL_PATH,
        ]
        if list(by_path) != expected_paths or len(by_path) != len(captures):
            raise ValueError("required source capture set is invalid")
        manifest = _source_manifest(captures, window)
    except BaseException:
        for capture in captures:
            capture.close()
        raise
    try:
        payload = _capture_bytes(by_path[_CONFIG_LOGICAL_PATH])
        static_tokens, static_crypto, static_identities = _parse_static_tokens(
            payload
        )
        del payload
        by_path[_CONFIG_LOGICAL_PATH].close()

        payload = _capture_bytes(by_path[_CHAIN_CONFIG_LOGICAL_PATH])
        chain_identities = _parse_token_chain_identities(
            payload, configured_tokens=static_tokens
        )
        del payload
        by_path[_CHAIN_CONFIG_LOGICAL_PATH].close()
        for key, address in chain_identities.items():
            prior = static_identities.get(key)
            if prior is not None and prior != address:
                raise ValueError("captured Token identities conflict")
            static_identities[key] = address

        payload = _capture_bytes(by_path["admin/token_registry.json"])
        runtime_tokens, runtime_crypto, runtime_identities = (
            _parse_runtime_registry(payload)
        )
        del payload
        by_path["admin/token_registry.json"].close()

        configured_crypto = tuple(sorted(set(static_crypto) | set(runtime_crypto)))
        payload = _capture_bytes(by_path["cex_instrument_lifecycle.json"])
        lifecycle_withheld = _parse_lifecycle(payload, configured_crypto)
        del payload
        by_path["cex_instrument_lifecycle.json"].close()

        catalog, imported_at = _parse_sqlite(
            by_path["market_facts.sqlite3"],
            by_path["cex_exchange_volume_daily.csv"].identity,
            static_tokens | runtime_tokens,
            lifecycle_withheld,
        )
        by_path["market_facts.sqlite3"].close()
        expected_by_type = {
            market_type: {
                row["market_id"]
                for row in catalog
                if row["market_type"] == market_type
            }
            for market_type in ("cex", "dex")
        }

        payload = _capture_bytes(by_path["cex_exchange_volume_daily.csv"])
        cex_volume_rows = _parse_cex_volume(payload, window, imported_at)
        del payload
        by_path["cex_exchange_volume_daily.csv"].close()

        payload = _capture_bytes(by_path["cex_depth_latest.csv"])
        cex_depth_rows, cex_depth_snapshot_id = _parse_depth(
            payload,
            market_type="cex",
            expected_market_ids=expected_by_type["cex"],
        )
        del payload
        by_path["cex_depth_latest.csv"].close()

        payload = _capture_bytes(by_path["dex_depth_latest.csv"])
        dex_depth_rows, dex_depth_snapshot_id = _parse_depth(
            payload,
            market_type="dex",
            expected_market_ids=expected_by_type["dex"],
        )
        del payload
        by_path["dex_depth_latest.csv"].close()

        payload = _capture_bytes(by_path["cex_execution_cost_latest.csv"])
        cex_execution_rows = _parse_execution(
            payload,
            market_type="cex",
            depth_snapshot_id=cex_depth_snapshot_id,
            expected_market_ids=expected_by_type["cex"],
        )
        del payload
        by_path["cex_execution_cost_latest.csv"].close()

        payload = _capture_bytes(by_path["dex_execution_cost_latest.csv"])
        dex_execution_rows = _parse_execution(
            payload,
            market_type="dex",
            depth_snapshot_id=dex_depth_snapshot_id,
            expected_market_ids=expected_by_type["dex"],
        )
        del payload
        by_path["dex_execution_cost_latest.csv"].close()

        payload = _capture_bytes(by_path["dex_pool_tvl_latest.csv"])
        tvl_rows, dex_volume_rows, collector_contexts = _parse_tvl_and_volume(
            payload,
            expected_market_ids=expected_by_type["dex"],
        )
        del payload
        by_path["dex_pool_tvl_latest.csv"].close()

        universe = build_route_universe(
            catalog,
            cex_depth_rows + dex_depth_rows,
            cex_execution_rows + dex_execution_rows,
            cex_volume_rows,
            dex_volume_rows,
            tvl_rows,
            selection_window=window,
            candidate_source_generation=manifest["candidate_source_generation"],
        )
        universe = dict(universe)
        token_identities = dict(static_identities)
        for key, address in runtime_identities.items():
            prior = token_identities.get(key)
            if prior is not None and prior != address:
                raise ValueError("captured Token identities conflict")
            token_identities[key] = address
        selected_legs = []
        for leg in universe["selected_legs"]:
            projected = dict(leg)
            if leg["market_type"] == "dex":
                context = collector_contexts[leg["market_id"]]
                chain = leg["market_id"].split(":", 4)[1]
                target_address = token_identities.get(
                    (leg["token_symbol"], chain)
                )
                if target_address is None:
                    raise ValueError("selected DEX target Token identity is absent")
                target_side = None
                if context["status"] == "observed":
                    target_id = "{}_{}".format(chain, target_address)
                    matches = [
                        side
                        for side in ("base", "quote")
                        if context[side + "_token_id"] == target_id
                    ]
                    if len(matches) != 1:
                        raise ValueError(
                            "selected DEX target Token side is ambiguous"
                        )
                    target_side = matches[0]
                projected.update({
                    "collector_context": context,
                    "target_token_address": target_address,
                    "target_token_side": target_side,
                })
            selected_legs.append(projected)
        universe["selected_legs"] = selected_legs
        return universe, manifest
    finally:
        for capture in captures:
            capture.close()


def current_source_generation(
    data_dir: Path, *, static_token_config: Path
) -> str:
    """Rehash the exact eleven current source inputs without parsing a universe."""
    captures = _capture_required_sources(
        Path(data_dir), Path(static_token_config)
    )
    try:
        return _candidate_source_generation(
            capture.identity for capture in captures
        )
    finally:
        for capture in captures:
            capture.close()


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run ID is invalid")
    if run_id in {".", ".."}:
        raise ValueError("run ID contains a dot segment")
    return run_id


def _read_run_binding_member(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int,
) -> Tuple[bytes, Tuple[Any, ...]]:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("secure shadow member read is unavailable")
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        raise ValueError("shadow run input member is missing or unsafe") from error
    try:
        before = os.fstat(descriptor)
        payload = _read_member_bytes(descriptor, maximum_bytes + 1)
        after = os.fstat(descriptor)
        path_metadata = os.stat(
            name, dir_fd=directory_descriptor, follow_symlinks=False
        )
        if (
            len(payload) > maximum_bytes
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or _stable_file_metadata(before) != _stable_file_metadata(after)
            or _stable_file_metadata(before)
            != _stable_file_metadata(path_metadata)
        ):
            raise ValueError("shadow run input member is changed, unsafe, or hard-linked")
        return payload, _stable_file_metadata(before)
    finally:
        os.close(descriptor)


def load_run_input_binding(shadow_root: Path, run_id: str) -> Dict[str, Any]:
    """Descriptor-reread and fully validate one immutable universe binding."""
    validated_run_id = _validate_run_id(run_id)
    runs_path = _absolute_path(Path(shadow_root)) / "runs"
    runs_descriptor, runs_chain = _open_directory_chain(runs_path)
    run_descriptor = -1
    try:
        try:
            run_descriptor = os.open(
                validated_run_id,
                _secure_directory_flags(),
                dir_fd=runs_descriptor,
            )
        except OSError as error:
            raise ValueError("shadow run input directory is missing or unsafe") from error
        run_before = os.fstat(run_descriptor)
        path_before = os.stat(
            validated_run_id,
            dir_fd=runs_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(run_before.st_mode)
            or (run_before.st_dev, run_before.st_ino)
            != (path_before.st_dev, path_before.st_ino)
        ):
            raise ValueError("shadow run input directory identity is invalid")
        universe_bytes, universe_metadata = _read_run_binding_member(
            run_descriptor,
            "route_universe.json",
            maximum_bytes=MAX_SOURCE_BYTES,
        )
        baseline_bytes, baseline_metadata = _read_run_binding_member(
            run_descriptor,
            "baseline_manifest.json",
            maximum_bytes=MAX_SOURCE_BYTES,
        )
        try:
            universe_raw = json.loads(universe_bytes.decode("utf-8"))
            baseline_raw = json.loads(baseline_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("shadow run input JSON is invalid") from error
        if (
            not isinstance(universe_raw, dict)
            or not isinstance(baseline_raw, dict)
            or _canonical_json_bytes(universe_raw) != universe_bytes
            or _canonical_json_bytes(baseline_raw) != baseline_bytes
        ):
            raise ValueError("shadow run input JSON is not canonical")
        try:
            # Task 2 owns the complete route-universe contract.  Import lazily
            # to avoid a module-import cycle while sharing its strict reader.
            from scripts.route_publication import _validate_route_universe_payload
            universe = _validate_route_universe_payload(universe_raw)
        except (ImportError, TypeError, ValueError) as error:
            raise ValueError("shadow route universe is invalid") from error
        try:
            baseline = _validated_publication_manifest(universe, baseline_raw)
        except (TypeError, ValueError) as error:
            raise ValueError("shadow baseline manifest is invalid") from error
        universe_sha256 = route_universe_sha256(universe)
        if (
            baseline != baseline_raw
            or baseline.get("route_universe_sha256") != universe_sha256
            or baseline.get("candidate_source_generation")
            != universe.get("candidate_source_generation")
        ):
            raise ValueError("shadow run input lineage is invalid")
        for name, expected_bytes, expected_metadata in (
            ("route_universe.json", universe_bytes, universe_metadata),
            ("baseline_manifest.json", baseline_bytes, baseline_metadata),
        ):
            current_bytes, current_metadata = _read_run_binding_member(
                run_descriptor, name, maximum_bytes=MAX_SOURCE_BYTES
            )
            if current_bytes != expected_bytes or current_metadata != expected_metadata:
                raise ValueError("shadow run input changed during validation")
        run_after = os.fstat(run_descriptor)
        path_after = os.stat(
            validated_run_id,
            dir_fd=runs_descriptor,
            follow_symlinks=False,
        )
        if (
            _stable_file_metadata(run_before) != _stable_file_metadata(run_after)
            or (run_after.st_dev, run_after.st_ino)
            != (path_after.st_dev, path_after.st_ino)
        ):
            raise ValueError("shadow run input directory changed during validation")
        _recheck_directory_chain(runs_path, runs_chain)
        return {
            "run_id": validated_run_id,
            "universe": universe,
            "baseline_manifest": baseline,
            "route_universe_sha256": universe_sha256,
            "baseline_manifest_sha256": hashlib.sha256(
                baseline_bytes
            ).hexdigest(),
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
        }
    finally:
        if run_descriptor >= 0:
            os.close(run_descriptor)
        os.close(runs_descriptor)


def _validated_publication_manifest(
    universe: Mapping[str, Any], source_manifest: Mapping[str, Any]
) -> Dict[str, Any]:
    manifest = dict(source_manifest)
    required_fields = {
        "schema", "calculation_version", "candidate_source_generation",
        "selection_window", "filters", "observation_bounds", "inputs",
    }
    if frozenset(manifest) not in {
        frozenset(required_fields),
        frozenset(required_fields | {"route_universe_sha256"}),
    }:
        raise ValueError("baseline source manifest fields are invalid")
    if (
        manifest.get("schema") != BASELINE_MANIFEST_SCHEMA
        or manifest.get("calculation_version") != CALCULATION_VERSION
        or manifest.get("selection_window") != universe.get("selection_window")
        or not isinstance(manifest.get("filters"), Mapping)
        or manifest["filters"].get("window_days") != WINDOW_DAYS
        or not isinstance(manifest.get("observation_bounds"), Mapping)
    ):
        raise ValueError("baseline source manifest contract is invalid")
    raw_inputs = manifest.get("inputs")
    if not isinstance(raw_inputs, list):
        raise ValueError("baseline source manifest inputs are invalid")
    identities = []
    for raw in raw_inputs:
        if not isinstance(raw, dict) or set(raw) != {"path", "size", "sha256"}:
            raise ValueError("baseline source identity is invalid")
        identity = SourceFileIdentity(
            path=raw.get("path"),
            size=raw.get("size"),
            sha256=raw.get("sha256"),
        )
        _identity_record(identity)
        identities.append(identity)
    expected_paths = [item[0] for item in _DATA_INPUTS] + [
        _CONFIG_LOGICAL_PATH, _CHAIN_CONFIG_LOGICAL_PATH,
    ]
    if [identity.path for identity in identities] != expected_paths:
        raise ValueError("baseline source manifest paths are invalid")
    generation = _candidate_source_generation(identities)
    if (
        manifest.get("candidate_source_generation") != generation
        or universe.get("candidate_source_generation") != generation
    ):
        raise ValueError("baseline source manifest generation is invalid")
    return manifest


def _ensure_directory_chain(path: Path) -> Tuple[int, Tuple[Tuple[int, int], ...]]:
    absolute = _absolute_path(path)
    flags = _secure_directory_flags()
    descriptor = os.open(os.sep, flags)
    identities = []
    try:
        root = os.fstat(descriptor)
        identities.append((root.st_dev, root.st_ino))
        for component in absolute.parts[1:]:
            try:
                os.mkdir(component, 0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as error:
                raise ValueError(
                    "shadow run directory ancestor is changed or a symlink"
                ) from error
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            identities.append((metadata.st_dev, metadata.st_ino))
        return descriptor, tuple(identities)
    except BaseException:
        os.close(descriptor)
        raise


def _write_staged_file(
    directory_descriptor: int, name: str, payload: bytes
) -> None:
    if name not in {"route_universe.json", "baseline_manifest.json"}:
        raise ValueError("shadow run member is invalid")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("secure shadow file creation is unavailable")
    descriptor = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow
        | getattr(os, "O_CLOEXEC", 0),
        0o600,
        dir_fd=directory_descriptor,
    )
    try:
        offset = 0
        view = memoryview(payload)
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise OSError("short write while publishing shadow input")
            offset += written
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != len(payload)
        ):
            raise ValueError("shadow run member is unsafe or hard-linked")
    finally:
        os.close(descriptor)


def _directory_identity(descriptor: int) -> Tuple[int, int]:
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("shadow staging descriptor is not a directory")
    return metadata.st_dev, metadata.st_ino


def _read_member_bytes(descriptor: int, maximum_bytes: int) -> bytes:
    with os.fdopen(os.dup(descriptor), "rb") as handle:
        payload = handle.read(maximum_bytes + 1)
    if len(payload) > maximum_bytes:
        raise ValueError("shadow run member exceeds expected bytes")
    return payload


def _verify_run_directory(
    directory_descriptor: int,
    expected_identity: Tuple[int, int],
    expected_payloads: Mapping[str, bytes],
) -> None:
    if _directory_identity(directory_descriptor) != expected_identity:
        raise ValueError("shadow staging directory identity changed")
    if sorted(os.listdir(directory_descriptor)) != sorted(expected_payloads):
        raise ValueError("shadow run directory has unexpected members")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("secure shadow member verification is unavailable")
    for name, expected_payload in expected_payloads.items():
        descriptor = os.open(
            name,
            os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_descriptor,
        )
        try:
            before = os.fstat(descriptor)
            payload = _read_member_bytes(descriptor, len(expected_payload))
            after = os.fstat(descriptor)
            path_metadata = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or _stable_file_metadata(before) != _stable_file_metadata(after)
                or _stable_file_metadata(before) != _stable_file_metadata(path_metadata)
                or payload != expected_payload
            ):
                raise ValueError("shadow run member bytes or identity changed")
        finally:
            os.close(descriptor)


def _verify_named_directory(
    runs_descriptor: int,
    name: str,
    expected_identity: Tuple[int, int],
) -> None:
    metadata = os.stat(name, dir_fd=runs_descriptor, follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != expected_identity
    ):
        raise ValueError("shadow staging directory path identity changed")


def _rename_directory_noreplace_at(
    directory_descriptor: int,
    source_name: str,
    destination_name: str,
    *,
    expected_source_identity: Optional[Tuple[int, int]] = None,
    expected_payloads: Optional[Mapping[str, bytes]] = None,
) -> Tuple[int, Tuple[int, int]]:
    source_descriptor = -1
    try:
        if expected_source_identity is not None:
            _verify_named_directory(
                directory_descriptor,
                source_name,
                expected_source_identity,
            )
            source_descriptor = os.open(
                source_name,
                _secure_directory_flags(),
                dir_fd=directory_descriptor,
            )
            if expected_payloads is not None:
                _verify_run_directory(
                    source_descriptor,
                    expected_source_identity,
                    expected_payloads,
                )
        library = ctypes.CDLL(None, use_errno=True)
        source = os.fsencode(source_name)
        destination = os.fsencode(destination_name)
        if sys.platform == "darwin":
            try:
                operation = library.renameatx_np
            except AttributeError as error:
                raise ValueError(
                    "atomic no-replace directory rename is unsupported"
                ) from error
            operation.argtypes = [
                ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
                ctypes.c_uint,
            ]
            operation.restype = ctypes.c_int
            arguments = (
                directory_descriptor, source, directory_descriptor,
                destination, 0x00000004,
            )
        elif sys.platform.startswith("linux"):
            try:
                operation = library.renameat2
            except AttributeError as error:
                raise ValueError(
                    "atomic no-replace directory rename is unsupported"
                ) from error
            operation.argtypes = [
                ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
                ctypes.c_uint,
            ]
            operation.restype = ctypes.c_int
            arguments = (
                directory_descriptor, source, directory_descriptor,
                destination, 1,
            )
        else:
            raise ValueError("atomic no-replace directory rename is unsupported")
        ctypes.set_errno(0)
        if operation(*arguments) != 0:
            error_number = ctypes.get_errno()
            if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
                raise ValueError("immutable shadow run already exists")
            if error_number in {errno.ENOSYS, errno.ENOTSUP}:
                raise ValueError(
                    "atomic no-replace directory rename is unsupported"
                )
            raise OSError(error_number, os.strerror(error_number))

        installed_descriptor = os.open(
            destination_name,
            _secure_directory_flags(),
            dir_fd=directory_descriptor,
        )
        try:
            installed_identity = _directory_identity(installed_descriptor)
            _verify_named_directory(
                directory_descriptor,
                destination_name,
                installed_identity,
            )
        except BaseException:
            os.close(installed_descriptor)
            raise
        return installed_descriptor, installed_identity
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)


def _named_directory_matches_descriptor(
    runs_descriptor: int,
    name: str,
    owned_descriptor: int,
    owned_identity: Tuple[int, int],
) -> bool:
    if _directory_identity(owned_descriptor) != owned_identity:
        raise ValueError("owned shadow directory descriptor identity changed")
    candidate_descriptor = -1
    try:
        candidate_descriptor = os.open(
            name,
            _secure_directory_flags(),
            dir_fd=runs_descriptor,
        )
        if _directory_identity(candidate_descriptor) != owned_identity:
            return False
        metadata = os.stat(name, dir_fd=runs_descriptor, follow_symlinks=False)
        return (
            stat.S_ISDIR(metadata.st_mode)
            and (metadata.st_dev, metadata.st_ino) == owned_identity
        )
    except (FileNotFoundError, OSError):
        return False
    finally:
        if candidate_descriptor >= 0:
            os.close(candidate_descriptor)


def _cleanup_owned_directory(
    runs_descriptor: int,
    owned_descriptor: int,
    owned_identity: Tuple[int, int],
    expected_members: Iterable[str],
) -> None:
    owned_name = None
    for candidate in os.listdir(runs_descriptor):
        try:
            metadata = os.stat(
                candidate,
                dir_fd=runs_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            continue
        if (
            stat.S_ISDIR(metadata.st_mode)
            and (metadata.st_dev, metadata.st_ino) == owned_identity
        ):
            owned_name = candidate
            break
    if owned_name is None:
        return
    if not owned_name.startswith((".stage-", ".quarantine-")):
        quarantine_name = ".quarantine-{}".format(uuid.uuid4().hex)
        if not _named_directory_matches_descriptor(
            runs_descriptor,
            owned_name,
            owned_descriptor,
            owned_identity,
        ):
            return
        os.rename(
            owned_name,
            quarantine_name,
            src_dir_fd=runs_descriptor,
            dst_dir_fd=runs_descriptor,
        )
        owned_name = quarantine_name
    if not _named_directory_matches_descriptor(
        runs_descriptor,
        owned_name,
        owned_descriptor,
        owned_identity,
    ):
        return
    for member in expected_members:
        try:
            os.unlink(member, dir_fd=owned_descriptor)
        except FileNotFoundError:
            pass
    if not _named_directory_matches_descriptor(
        runs_descriptor,
        owned_name,
        owned_descriptor,
        owned_identity,
    ):
        return
    try:
        os.rmdir(owned_name, dir_fd=runs_descriptor)
    except (FileNotFoundError, OSError):
        pass


def write_run_universe(
    shadow_root: Path,
    run_id: str,
    universe: Mapping[str, Any],
    source_manifest: Mapping[str, Any],
) -> Tuple[Path, Path]:
    """Publish universe and baseline manifest as one immutable run directory."""
    validated_run_id = _validate_run_id(run_id)
    if not isinstance(universe, Mapping) or not isinstance(source_manifest, Mapping):
        raise ValueError("shadow universe publication payload is invalid")
    universe_value = dict(universe)
    manifest_value = _validated_publication_manifest(
        universe_value, source_manifest
    )
    generation = universe_value.get("candidate_source_generation")
    if (
        not isinstance(generation, str)
        or _HASH_PATTERN.fullmatch(generation) is None
        or manifest_value.get("candidate_source_generation") != generation
    ):
        raise ValueError("universe and baseline manifest generation do not match")
    universe_sha = route_universe_sha256(universe_value)
    existing_sha = manifest_value.get("route_universe_sha256")
    if existing_sha is not None and existing_sha != universe_sha:
        raise ValueError("baseline manifest route universe hash conflicts")
    manifest_value["route_universe_sha256"] = universe_sha
    universe_payload = _canonical_json_bytes(universe_value)
    manifest_payload = _canonical_json_bytes(manifest_value)

    display_runs_path = Path(shadow_root) / "runs"
    runs_path = _absolute_path(Path(shadow_root)) / "runs"
    runs_descriptor, runs_chain = _ensure_directory_chain(runs_path)
    stage_name = ".stage-{}-{}".format(validated_run_id, uuid.uuid4().hex)
    stage_descriptor = -1
    stage_identity = None
    installed_descriptor = -1
    installed_identity = None
    committed = False
    try:
        try:
            os.mkdir(stage_name, 0o700, dir_fd=runs_descriptor)
        except FileExistsError as error:  # pragma: no cover - UUID collision
            raise ValueError("shadow staging directory already exists") from error
        stage_descriptor = os.open(
            stage_name, _secure_directory_flags(), dir_fd=runs_descriptor
        )
        stage_identity = _directory_identity(stage_descriptor)
        expected_payloads = {
            "route_universe.json": universe_payload,
            "baseline_manifest.json": manifest_payload,
        }
        _write_staged_file(
            stage_descriptor, "route_universe.json", universe_payload
        )
        _write_staged_file(
            stage_descriptor, "baseline_manifest.json", manifest_payload
        )
        _verify_run_directory(
            stage_descriptor,
            stage_identity,
            expected_payloads,
        )
        os.fsync(stage_descriptor)
        _recheck_directory_chain(runs_path, runs_chain)
        installed_descriptor, installed_identity = _rename_directory_noreplace_at(
            runs_descriptor,
            stage_name,
            validated_run_id,
            expected_source_identity=stage_identity,
            expected_payloads=expected_payloads,
        )
        if installed_identity != stage_identity:
            raise ValueError("installed shadow run identity changed")
        _verify_named_directory(
            runs_descriptor,
            validated_run_id,
            installed_identity,
        )
        _verify_run_directory(
            installed_descriptor,
            installed_identity,
            expected_payloads,
        )
        os.fsync(runs_descriptor)
        _recheck_directory_chain(runs_path, runs_chain)
        committed = True
    finally:
        if installed_descriptor >= 0:
            if not committed and installed_identity is not None:
                _cleanup_owned_directory(
                    runs_descriptor,
                    installed_descriptor,
                    installed_identity,
                    ("route_universe.json", "baseline_manifest.json"),
                )
            os.close(installed_descriptor)
        if stage_descriptor >= 0:
            if not committed and stage_identity is not None:
                _cleanup_owned_directory(
                    runs_descriptor,
                    stage_descriptor,
                    stage_identity,
                    ("route_universe.json", "baseline_manifest.json"),
                )
            os.close(stage_descriptor)
        os.close(runs_descriptor)

    final_directory = display_runs_path / validated_run_id
    return (
        final_directory / "route_universe.json",
        final_directory / "baseline_manifest.json",
    )
