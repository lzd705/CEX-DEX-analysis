"""Sealed command boundary for historical Foundry replay operations.

The module owns the canonical import/binding boundary, exact CLI grammar,
live-pointer invariant, and fail-closed scan/publication orchestration.
Audit-only verification remains a separate command path.
"""

from __future__ import annotations

import sys as _startup_sys


SAFE_HISTORICAL_REPLAY_STARTUP_FLAGS = (
    "-E",
    "-s",
    "-S",
    "-B",
    "-X",
    "pycache_prefix=/dev/null",
)
_SAFE_CPYTHON_VERSION = (3, 8, 10)
_SAFE_RUNTIME_BASENAME = "cpython-3.8.10-runtime"
_ENTRYPOINT_SUFFIX = "/scripts/run_historical_foundry_replay.py"
_UNSAFE_STARTUP_MESSAGE = "historical replay startup is unsafe"


def _trusted_launch_is_exact() -> bool:
    flags = _startup_sys.flags
    version = _startup_sys.version_info
    source_path = __file__
    if (
        not isinstance(source_path, str)
        or not source_path.startswith("/")
        or not source_path.endswith(_ENTRYPOINT_SUFFIX)
    ):
        return False
    project_root = source_path[: -len(_ENTRYPOINT_SUFFIX)]
    prefix = _startup_sys.prefix
    if (
        not isinstance(prefix, str)
        or not prefix.startswith("/")
        or prefix.endswith("/")
    ):
        return False
    expected_path = [
        project_root,
        prefix + "/lib/python38.zip",
        prefix + "/lib/python3.8",
        prefix + "/lib/python3.8/lib-dynload",
    ]
    expected_prefix = (
        project_root.rsplit("/", 1)[0] + "/" + _SAFE_RUNTIME_BASENAME
    )
    return (
        _startup_sys.implementation.name == "cpython"
        and (version.major, version.minor, version.micro)
        == _SAFE_CPYTHON_VERSION
        and version.releaselevel == "final"
        and version.serial == 0
        and _startup_sys.implementation.cache_tag == "cpython-38"
        and prefix == expected_prefix
        and prefix.rsplit("/", 1)[-1] == _SAFE_RUNTIME_BASENAME
        and _startup_sys.base_prefix == prefix
        and _startup_sys.exec_prefix == prefix
        and _startup_sys.base_exec_prefix == prefix
        and _startup_sys.executable == prefix + "/bin/python3.8"
        and _startup_sys.path == expected_path
        and _startup_sys.pycache_prefix == "/dev/null"
        and _startup_sys._xoptions == {"pycache_prefix": "/dev/null"}
        and _startup_sys.warnoptions == []
        and flags.debug == 0
        and flags.inspect == 0
        and flags.interactive == 0
        and flags.optimize == 0
        and flags.dont_write_bytecode == 1
        and flags.no_user_site == 1
        and flags.no_site == 1
        and flags.ignore_environment == 1
        and flags.verbose == 0
        and flags.bytes_warning == 0
        and flags.quiet == 0
        and flags.hash_randomization == 1
        and flags.isolated == 0
        and flags.dev_mode is False
    )


if __name__ == "__main__":
    # ``python -m`` executes a transient ``__main__`` module.  Import the
    # canonical module so the verifier sees its one authentic module-top bind.
    if not _trusted_launch_is_exact():
        _startup_sys.stderr.write(_UNSAFE_STARTUP_MESSAGE + "\n")
        raise SystemExit(1)
    import importlib as _importlib

    _canonical_entrypoint = _importlib.import_module(
        "scripts.run_historical_foundry_replay"
    )
    raise SystemExit(_canonical_entrypoint.main(_startup_sys.argv[1:]))

import argparse
import base64
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import sys
import time
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence, Tuple
import weakref


class HistoricalReplayEntrypointError(RuntimeError):
    """Stable fail-closed command-boundary error."""


def _require_safe_historical_startup() -> None:
    if not _trusted_launch_is_exact():
        raise HistoricalReplayEntrypointError(_UNSAFE_STARTUP_MESSAGE)


def _production_connected_historical_verification_engine(
    _request: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Fail closed until Task-8's connected production controller is complete."""
    _require_safe_historical_startup()
    raise HistoricalReplayEntrypointError(
        "historical production controller is unavailable"
    )


# This must remain a genuine canonical module-top call edge.  The verifier
# authenticates the loader/code/global namespace and consumes its binder once.
import scripts.historical_foundry_verifier as _historical_verifier

_connected_engine_binder = (
    _historical_verifier._bind_connected_historical_verification_engine
)
_connected_engine_binder(
    _production_connected_historical_verification_engine
)
del _connected_engine_binder
del _historical_verifier


_LIVE_POINTER_PATHS = (
    "routes/core/latest.json",
    "routes/latest.json",
)
_MAX_LIVE_POINTER_BYTES = 1024 * 1024
_PROJECT_ROOT = Path(__file__).parent.parent
_FORGE_STD_COMMIT = "620536fa5277db4e3fd46772d5cbc1ea0696fb43"
_TRACKED_SOURCE_INVENTORY = (
    ("policy", "config/historical_foundry_replay_policy.json"),
    ("authority", "config/historical_foundry_replay_authority.json"),
    ("toolchain_config", "config/historical_foundry_replay_toolchain.json"),
    ("executor", "foundry/src/TwoVenueV2Executor.sol"),
    ("unit_test", "foundry/test/TwoVenueV2Unit.t.sol"),
    ("fork_test", "foundry/test/TwoVenueV2Fork.t.sol"),
    ("foundry_toml", "foundry.toml"),
    ("foundry_lock", "foundry.lock"),
    ("gitmodules", ".gitmodules"),
)
_PRODUCTION_PYTHON_SOURCE_INVENTORY = (
    ("source:scripts_package", "scripts/__init__.py"),
    ("source:atomic_publication", "scripts/atomic_publication.py"),
    (
        "source:bootstrap_historical_foundry_toolchain",
        "scripts/bootstrap_historical_foundry_toolchain.py",
    ),
    ("source:bounded_json", "scripts/bounded_json.py"),
    ("source:bounded_snapshot_merge", "scripts/bounded_snapshot_merge.py"),
    ("source:cex_fee_facts", "scripts/cex_fee_facts.py"),
    (
        "source:cex_instrument_lifecycle",
        "scripts/cex_instrument_lifecycle.py",
    ),
    ("source:collection_deadline", "scripts/collection_deadline.py"),
    ("source:execution_cost", "scripts/execution_cost.py"),
    (
        "source:execution_cost_components",
        "scripts/execution_cost_components.py",
    ),
    ("source:fact_quality", "scripts/fact_quality.py"),
    ("source:fetch_cex", "scripts/fetch_cex.py"),
    ("source:fetch_cex_depth", "scripts/fetch_cex_depth.py"),
    ("source:fetch_dex_depth", "scripts/fetch_dex_depth.py"),
    ("source:fetch_tvl", "scripts/fetch_tvl.py"),
    (
        "source:historical_foundry_anvil",
        "scripts/historical_foundry_anvil.py",
    ),
    (
        "source:historical_foundry_contracts",
        "scripts/historical_foundry_contracts.py",
    ),
    (
        "source:historical_foundry_replay",
        "scripts/historical_foundry_replay.py",
    ),
    (
        "source:historical_foundry_rpc",
        "scripts/historical_foundry_rpc.py",
    ),
    (
        "source:historical_foundry_scan",
        "scripts/historical_foundry_scan.py",
    ),
    (
        "source:historical_foundry_storage",
        "scripts/historical_foundry_storage.py",
    ),
    (
        "source:historical_foundry_verifier",
        "scripts/historical_foundry_verifier.py",
    ),
    (
        "source:historical_route_publication",
        "scripts/historical_route_publication.py",
    ),
    (
        "source:market_lifecycle_reviews",
        "scripts/market_lifecycle_reviews.py",
    ),
    ("source:publication_gate", "scripts/publication_gate.py"),
    ("source:quality_outcomes", "scripts/quality_outcomes.py"),
    ("source:route_cohort", "scripts/route_cohort.py"),
    ("source:route_cost_evidence", "scripts/route_cost_evidence.py"),
    ("source:route_cost_topology", "scripts/route_cost_topology.py"),
    ("source:route_inventory", "scripts/route_inventory.py"),
    ("source:route_opportunity", "scripts/route_opportunity.py"),
    ("source:route_publication", "scripts/route_publication.py"),
    ("source:route_quantity", "scripts/route_quantity.py"),
    ("source:route_shadow_audit", "scripts/route_shadow_audit.py"),
    ("source:route_shadow_inputs", "scripts/route_shadow_inputs.py"),
    ("source:route_universe", "scripts/route_universe.py"),
    (
        "source:run_historical_foundry_replay",
        "scripts/run_historical_foundry_replay.py",
    ),
    ("source:timestamp_contract", "scripts/timestamp_contract.py"),
    ("source:token_registry", "scripts/token_registry.py"),
)
_PRODUCTION_PYTHON_SOURCE_PATHS = tuple(
    relative_path
    for _role, relative_path in _PRODUCTION_PYTHON_SOURCE_INVENTORY
)
_TRACKED_SOURCE_INVENTORY += _PRODUCTION_PYTHON_SOURCE_INVENTORY
_MAX_GIT_OUTPUT_BYTES = 4 * 1024 * 1024
_MAX_TRACKED_SOURCE_BYTES = 1024 * 1024
_GIT_TIMEOUT_SECONDS = 30.0
_PROCESS_CLEANUP_SECONDS = 1.0


@dataclass(frozen=True)
class LivePointerSnapshot:
    relative_path: str
    present: bool
    size: Optional[int]
    sha256: Optional[str]
    bytes_value: Optional[bytes]


def _entrypoint_error(reason: str) -> HistoricalReplayEntrypointError:
    return HistoricalReplayEntrypointError(reason)


def _close_descriptor_inventory(descriptors: Sequence[int]) -> None:
    first_error = None
    attempted = set()
    for descriptor in reversed(tuple(descriptors)):
        if descriptor in attempted:
            continue
        attempted.add(descriptor)
        try:
            os.close(descriptor)
        except BaseException as error:
            if first_error is None or (
                isinstance(first_error, Exception)
                and not isinstance(error, Exception)
            ):
                first_error = error
    if first_error is not None:
        if not isinstance(first_error, Exception):
            raise first_error
        raise _entrypoint_error("live pointer snapshot is invalid") from first_error


def _raise_after_cleanup(
    original_error: BaseException,
    cleanup_error: Optional[BaseException],
) -> None:
    if not isinstance(original_error, Exception):
        raise original_error from cleanup_error
    if cleanup_error is not None and not isinstance(cleanup_error, Exception):
        raise cleanup_error from original_error
    raise original_error


def _close_after_error(
    descriptors: Sequence[int], original_error: BaseException,
) -> None:
    cleanup_error = None
    try:
        _close_descriptor_inventory(descriptors)
    except BaseException as error:
        cleanup_error = error
    _raise_after_cleanup(original_error, cleanup_error)


def _stable_metadata(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        getattr(
            metadata,
            "st_mtime_ns",
            int(metadata.st_mtime * 1000000000),
        ),
        getattr(
            metadata,
            "st_ctime_ns",
            int(metadata.st_ctime * 1000000000),
        ),
    )


def _secure_file_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    cloexec = getattr(os, "O_CLOEXEC", None)
    if (
        type(nofollow) is not int
        or nofollow == 0
        or type(cloexec) is not int
        or cloexec == 0
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")
    return os.O_RDONLY | nofollow | cloexec


def _secure_directory_flags() -> int:
    directory = getattr(os, "O_DIRECTORY", None)
    if type(directory) is not int or directory == 0:
        raise _entrypoint_error("live pointer snapshot is invalid")
    return _secure_file_flags() | directory


def _require_descriptor_noninheritable(descriptor: int) -> None:
    try:
        inheritable = os.get_inheritable(descriptor)
    except (AttributeError, OSError) as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    if inheritable is not False:
        raise _entrypoint_error("live pointer snapshot is invalid")


def _open_descriptor(
    name: str,
    flags: int,
    *,
    parent_descriptor: Optional[int] = None,
) -> int:
    try:
        if parent_descriptor is None:
            descriptor = os.open(name, flags)
        else:
            descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    try:
        _require_descriptor_noninheritable(descriptor)
    except BaseException as error:
        _close_after_error((descriptor,), error)
        raise AssertionError("unreachable")
    return descriptor


def _read_descriptor_bounded(descriptor: int, maximum: int) -> bytes:
    chunks = []
    size = 0
    while True:
        chunk = os.read(descriptor, min(65536, maximum - size + 1))
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > maximum:
            raise _entrypoint_error("live pointer snapshot is invalid")
    return b"".join(chunks)


def _reread_descriptor_bounded(descriptor: int, maximum: int) -> bytes:
    chunks = []
    size = 0
    offset = 0
    while True:
        try:
            chunk = os.pread(
                descriptor,
                min(65536, maximum - size + 1),
                offset,
            )
        except OSError as error:
            raise _entrypoint_error("live pointer snapshot is invalid") from error
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        offset += len(chunk)
        if size > maximum:
            raise _entrypoint_error("live pointer snapshot is invalid")
    return b"".join(chunks)


def _directory_identity(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _absolute_path(path: Path) -> Path:
    if not isinstance(path, Path):
        raise _entrypoint_error("live pointer snapshot is invalid")
    try:
        expanded = os.path.abspath(os.fspath(path))
    except (TypeError, ValueError, OSError) as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    if "\x00" in expanded:
        raise _entrypoint_error("live pointer snapshot is invalid")
    if sys.platform == "darwin":
        if expanded == "/var" or expanded.startswith("/var/"):
            expanded = "/private" + expanded
        elif expanded == "/tmp" or expanded.startswith("/tmp/"):
            expanded = "/private" + expanded
    return Path(expanded)


def _open_absolute_directory_chain(
    path: Path,
) -> Tuple[Tuple[int, Optional[int], str, Tuple[int, ...]], ...]:
    absolute = _absolute_path(path)
    descriptors = []
    owned_descriptors = []
    try:
        root_descriptor = _open_descriptor(
            os.sep, _secure_directory_flags()
        )
        owned_descriptors.append(root_descriptor)
        root_metadata = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_metadata.st_mode):
            raise _entrypoint_error("live pointer snapshot is invalid")
        descriptors.append((
            root_descriptor,
            None,
            os.sep,
            _directory_identity(root_metadata),
        ))
        parent_descriptor = root_descriptor
        for component in absolute.parts[1:]:
            descriptor = _open_descriptor(
                component,
                _secure_directory_flags(),
                parent_descriptor=parent_descriptor,
            )
            owned_descriptors.append(descriptor)
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise _entrypoint_error("live pointer snapshot is invalid")
            descriptors.append((
                descriptor,
                parent_descriptor,
                component,
                _directory_identity(metadata),
            ))
            parent_descriptor = descriptor
        return tuple(descriptors)
    except BaseException as error:
        _close_after_error(owned_descriptors, error)
        raise AssertionError("unreachable")


def _require_absolute_directory_chain_stable(
    chain: Sequence[Tuple[int, Optional[int], str, Tuple[int, ...]]],
) -> None:
    if not chain:
        raise _entrypoint_error("live pointer snapshot is invalid")
    for descriptor, parent_descriptor, name, expected in chain:
        _require_descriptor_noninheritable(descriptor)
        try:
            current = os.fstat(descriptor)
            if parent_descriptor is None:
                by_path = os.stat(os.sep, follow_symlinks=False)
            else:
                by_path = os.stat(
                    name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
        except OSError as error:
            raise _entrypoint_error("live pointer snapshot is invalid") from error
        if (
            not stat.S_ISDIR(current.st_mode)
            or _directory_identity(current) != expected
            or _directory_identity(by_path) != expected
        ):
            raise _entrypoint_error("live pointer snapshot is invalid")


def _require_directory_stable(
    *,
    parent_descriptor: Optional[int],
    name: str,
    descriptor: int,
    expected: Tuple[int, ...],
) -> None:
    if parent_descriptor is None:
        by_path = os.stat(name, follow_symlinks=False)
    else:
        by_path = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    current = os.fstat(descriptor)
    _require_descriptor_noninheritable(descriptor)
    if (
        not stat.S_ISDIR(current.st_mode)
        or _stable_metadata(current) != expected
        or _stable_metadata(by_path) != expected
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")


def _open_child_directory(
    *, parent_descriptor: int, name: str,
) -> Optional[Tuple[int, Tuple[int, ...]]]:
    try:
        by_path = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    except OSError as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    if not stat.S_ISDIR(by_path.st_mode):
        raise _entrypoint_error("live pointer snapshot is invalid")
    descriptor = _open_descriptor(
        name,
        _secure_directory_flags(),
        parent_descriptor=parent_descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        expected = _stable_metadata(opened)
        if (
            not stat.S_ISDIR(by_path.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or _stable_metadata(by_path) != expected
        ):
            raise _entrypoint_error("live pointer snapshot is invalid")
        return descriptor, expected
    except BaseException as error:
        _close_after_error((descriptor,), error)
        raise AssertionError("unreachable")


def _absent_snapshot(relative_path: str) -> LivePointerSnapshot:
    return LivePointerSnapshot(relative_path, False, None, None, None)


def _open_held_pointer(
    *,
    relative_path: str,
    parent_descriptor: int,
    leaf_name: str,
) -> Mapping[str, Any]:
    try:
        by_path = os.stat(
            leaf_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return {
            "relative_path": relative_path,
            "present": False,
            "parent_descriptor": parent_descriptor,
            "leaf_name": leaf_name,
            "descriptor": None,
            "metadata": None,
            "payload": None,
        }
    except OSError as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    if (
        not stat.S_ISREG(by_path.st_mode)
        or by_path.st_nlink != 1
        or by_path.st_size > _MAX_LIVE_POINTER_BYTES
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")
    descriptor = _open_descriptor(
        leaf_name,
        _secure_file_flags(),
        parent_descriptor=parent_descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        expected = _stable_metadata(opened)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_size > _MAX_LIVE_POINTER_BYTES
            or _stable_metadata(by_path) != expected
        ):
            raise _entrypoint_error("live pointer snapshot is invalid")
        payload = _read_descriptor_bounded(
            descriptor, _MAX_LIVE_POINTER_BYTES
        )
    except BaseException as error:
        _close_after_error((descriptor,), error)
        raise AssertionError("unreachable")
    return {
        "relative_path": relative_path,
        "present": True,
        "parent_descriptor": parent_descriptor,
        "leaf_name": leaf_name,
        "descriptor": descriptor,
        "metadata": expected,
        "payload": payload,
    }


def _require_held_pointer_stable(pointer: Mapping[str, Any]) -> None:
    parent_descriptor = pointer["parent_descriptor"]
    leaf_name = pointer["leaf_name"]
    if not pointer["present"]:
        try:
            os.stat(
                leaf_name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        except OSError as error:
            raise _entrypoint_error("live pointer snapshot is invalid") from error
        raise _entrypoint_error("live pointer snapshot is invalid")
    descriptor = pointer["descriptor"]
    expected = pointer["metadata"]
    payload = pointer["payload"]
    _require_descriptor_noninheritable(descriptor)
    try:
        current = os.fstat(descriptor)
        by_path = os.stat(
            leaf_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    reread = _reread_descriptor_bounded(
        descriptor, _MAX_LIVE_POINTER_BYTES
    )
    if (
        not stat.S_ISREG(current.st_mode)
        or current.st_nlink != 1
        or _stable_metadata(current) != expected
        or _stable_metadata(by_path) != expected
        or reread != payload
        or len(reread) != current.st_size
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")


def _snapshot_from_held_pointer(
    pointer: Mapping[str, Any],
) -> LivePointerSnapshot:
    if not pointer["present"]:
        return _absent_snapshot(pointer["relative_path"])
    payload = pointer["payload"]
    return LivePointerSnapshot(
        relative_path=pointer["relative_path"],
        present=True,
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        bytes_value=payload,
    )


def capture_live_pointer_snapshots(
    *, data_dir: Path,
) -> Tuple[LivePointerSnapshot, LivePointerSnapshot]:
    chain = ()
    internal_directories = []
    held_pointers = []
    result = None
    original_error = None
    try:
        chain = _open_absolute_directory_chain(data_dir)
        data_descriptor = chain[-1][0]
        data_metadata = _stable_metadata(os.fstat(data_descriptor))
        routes = _open_child_directory(
            parent_descriptor=data_descriptor, name="routes"
        )
        if routes is None:
            held_pointers = [
                {
                    "relative_path": relative_path,
                    "present": False,
                    "parent_descriptor": data_descriptor,
                    "leaf_name": "routes",
                    "descriptor": None,
                    "metadata": None,
                    "payload": None,
                }
                for relative_path in _LIVE_POINTER_PATHS
            ]
        else:
            routes_descriptor, routes_metadata = routes
            internal_directories.append((
                data_descriptor,
                "routes",
                routes_descriptor,
                routes_metadata,
            ))
            core = _open_child_directory(
                parent_descriptor=routes_descriptor, name="core"
            )
            if core is None:
                held_pointers.append({
                    "relative_path": _LIVE_POINTER_PATHS[0],
                    "present": False,
                    "parent_descriptor": routes_descriptor,
                    "leaf_name": "core",
                    "descriptor": None,
                    "metadata": None,
                    "payload": None,
                })
            else:
                core_descriptor, core_metadata = core
                internal_directories.append((
                    routes_descriptor,
                    "core",
                    core_descriptor,
                    core_metadata,
                ))
                held_pointers.append(_open_held_pointer(
                    relative_path=_LIVE_POINTER_PATHS[0],
                    parent_descriptor=core_descriptor,
                    leaf_name="latest.json",
                ))
            held_pointers.append(_open_held_pointer(
                relative_path=_LIVE_POINTER_PATHS[1],
                parent_descriptor=routes_descriptor,
                leaf_name="latest.json",
            ))

        for pointer in held_pointers:
            _require_held_pointer_stable(pointer)
        for parent, name, descriptor, expected in reversed(
            internal_directories
        ):
            _require_directory_stable(
                parent_descriptor=parent,
                name=name,
                descriptor=descriptor,
                expected=expected,
            )
        if _stable_metadata(os.fstat(data_descriptor)) != data_metadata:
            raise _entrypoint_error("live pointer snapshot is invalid")
        _require_absolute_directory_chain_stable(chain)
        result = tuple(
            _snapshot_from_held_pointer(pointer)
            for pointer in held_pointers
        )
    except BaseException as error:
        original_error = error

    cleanup_error = None
    try:
        _close_descriptor_inventory(
            tuple(
                pointer["descriptor"]
                for pointer in held_pointers
                if pointer.get("descriptor") is not None
            )
            + tuple(row[2] for row in internal_directories)
            + tuple(row[0] for row in chain)
        )
    except BaseException as error:
        cleanup_error = error
    if original_error is not None:
        if not isinstance(original_error, Exception):
            raise original_error from cleanup_error
        if cleanup_error is not None and not isinstance(cleanup_error, Exception):
            raise cleanup_error from original_error
        raise original_error
    if cleanup_error is not None:
        raise cleanup_error
    if type(result) is not tuple or len(result) != 2:
        raise _entrypoint_error("live pointer snapshot is invalid")
    return result  # type: ignore[return-value]


def _require_snapshot_valid(snapshot: LivePointerSnapshot) -> None:
    if (
        type(snapshot) is not LivePointerSnapshot
        or type(snapshot.relative_path) is not str
        or type(snapshot.present) is not bool
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")
    if snapshot.relative_path not in _LIVE_POINTER_PATHS:
        raise _entrypoint_error("live pointer snapshot is invalid")
    if snapshot.present is False:
        if any(
            value is not None
            for value in (
                snapshot.size,
                snapshot.sha256,
                snapshot.bytes_value,
            )
        ):
            raise _entrypoint_error("live pointer snapshot is invalid")
        return
    if (
        snapshot.present is not True
        or type(snapshot.size) is not int
        or snapshot.size < 0
        or type(snapshot.bytes_value) is not bytes
        or snapshot.size != len(snapshot.bytes_value)
        or type(snapshot.sha256) is not str
        or snapshot.sha256 != hashlib.sha256(snapshot.bytes_value).hexdigest()
    ):
        raise _entrypoint_error("live pointer snapshot is invalid")


def _require_snapshot_sequence(
    snapshots: Sequence[LivePointerSnapshot],
) -> Tuple[LivePointerSnapshot, LivePointerSnapshot]:
    try:
        value = tuple(snapshots)
    except TypeError as error:
        raise _entrypoint_error("live pointer snapshot is invalid") from error
    if len(value) != len(_LIVE_POINTER_PATHS):
        raise _entrypoint_error("live pointer snapshot is invalid")
    for snapshot in value:
        _require_snapshot_valid(snapshot)
    if tuple(row.relative_path for row in value) != _LIVE_POINTER_PATHS:
        raise _entrypoint_error("live pointer snapshot is invalid")
    return value  # type: ignore[return-value]


def require_live_pointer_snapshots_unchanged(
    before: Sequence[LivePointerSnapshot],
    after: Sequence[LivePointerSnapshot],
) -> None:
    held_before = _require_snapshot_sequence(before)
    held_after = _require_snapshot_sequence(after)
    if held_before != held_after:
        raise _entrypoint_error("live pointer changed")


def project_live_pointer_snapshots(
    snapshots: Sequence[LivePointerSnapshot],
) -> Tuple[Mapping[str, Any], Mapping[str, Any]]:
    held = _require_snapshot_sequence(snapshots)
    return tuple(
        {
            "relative_path": snapshot.relative_path,
            "present": snapshot.present,
            "size": snapshot.size,
            "sha256": snapshot.sha256,
            "bytes_base64": (
                base64.b64encode(snapshot.bytes_value).decode("ascii")
                if snapshot.present
                else None
            ),
        }
        for snapshot in held
    )  # type: ignore[return-value]


class _LivePointerGuard:
    __slots__ = ("_data_dir", "_entered", "before", "after")

    def __init__(self, *, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._entered = False
        self.before = None
        self.after = None

    def __enter__(self) -> "_LivePointerGuard":
        if self._entered:
            raise _entrypoint_error("live pointer guard is invalid")
        self._entered = True
        self.before = capture_live_pointer_snapshots(data_dir=self._data_dir)
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> bool:
        guard_error = None
        try:
            self.after = capture_live_pointer_snapshots(
                data_dir=self._data_dir
            )
            require_live_pointer_snapshots_unchanged(
                self.before, self.after
            )
        except BaseException as error:
            guard_error = error
        if (
            _type is not None
            and not issubclass(_type, Exception)
            and isinstance(_value, BaseException)
        ):
            raise _value.with_traceback(_traceback) from guard_error
        if guard_error is not None:
            raise guard_error
        return False


def _absolute_bundle_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(
            "bundle must be an absolute immutable bundle path"
        )
    return path


def _parse_arguments(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_historical_foundry_replay",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    scan = commands.add_parser("scan", allow_abbrev=False)
    scan.add_argument("--data-dir", type=Path, required=True)
    mode = scan.add_mutually_exclusive_group(required=True)
    mode.add_argument("--publish", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    verify = commands.add_parser("verify", allow_abbrev=False)
    verify.add_argument("--data-dir", type=Path, required=True)
    verify.add_argument(
        "--bundle", type=_absolute_bundle_path, required=True
    )
    try:
        supplied = tuple(arguments)
    except TypeError:
        parser.error("invalid arguments")
    if (
        any(type(token) is not str for token in supplied)
        or any(token.startswith("--") and "=" in token for token in supplied)
    ):
        parser.error("invalid arguments")
    if supplied[:1] == ("scan",):
        exact_shape = (
            len(supplied) == 4
            and supplied.count("--data-dir") == 1
            and supplied.count("--publish")
            + supplied.count("--dry-run") == 1
            and supplied.count("--publish") <= 1
            and supplied.count("--dry-run") <= 1
        )
    elif supplied[:1] == ("verify",):
        exact_shape = (
            len(supplied) == 5
            and supplied.count("--data-dir") == 1
            and supplied.count("--bundle") == 1
        )
    else:
        exact_shape = False
    if not exact_shape:
        parser.error("invalid arguments")
    return parser.parse_args(supplied)


def _open_reviewed_historical_toolchain():
    # Kept lazy so canonical import and invalid CLI parsing remain zero-I/O.
    from scripts.bootstrap_historical_foundry_toolchain import (
        open_reviewed_historical_toolchain,
    )

    return open_reviewed_historical_toolchain()


def _initialize_clean_source_verifier():
    hash_pattern = re.compile(r"[0-9a-f]{64}")
    commit_pattern = re.compile(r"[0-9a-f]{40}")
    token = object()
    retired = object()
    authority_registry = {}
    git_environment = {
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }

    def fail(error: Optional[BaseException] = None) -> None:
        value = _entrypoint_error("historical source preflight failed")
        if error is None:
            raise value
        raise value from error

    def reap_process(process: Any) -> None:
        first_error = None

        def remember(error: BaseException) -> None:
            nonlocal first_error
            if first_error is None or (
                isinstance(first_error, Exception)
                and not isinstance(error, Exception)
            ):
                first_error = error

        def poll() -> Any:
            try:
                return process.poll()
            except BaseException as error:
                remember(error)
                return None

        returncode = poll()
        if returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            except BaseException as error:
                remember(error)
            try:
                process.wait(timeout=_PROCESS_CLEANUP_SECONDS)
            except subprocess.TimeoutExpired:
                pass
            except BaseException as error:
                remember(error)
            returncode = poll()
        if returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            except BaseException as error:
                remember(error)
            try:
                process.wait(timeout=_PROCESS_CLEANUP_SECONDS)
            except BaseException as error:
                remember(error)
            returncode = poll()
        if returncode is None and first_error is None:
            first_error = _entrypoint_error(
                "historical source preflight failed"
            )
        if first_error is not None:
            if not isinstance(first_error, Exception):
                raise first_error
            fail(first_error)

    def run_bounded_process(
        command: Tuple[str, ...], *, stdout_maximum: int,
    ) -> Tuple[int, bytes, bytes]:
        if (
            type(command) is not tuple
            or not command
            or any(type(argument) is not str for argument in command)
            or type(stdout_maximum) is not int
            or stdout_maximum < 0
        ):
            fail()
        process = None
        selector = None
        streams = []
        original_error = None
        result = None
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=dict(git_environment),
                close_fds=True,
            )
            if process.stdout is None or process.stderr is None:
                fail()
            streams = [process.stdout, process.stderr]
            for stream in streams:
                _require_descriptor_noninheritable(stream.fileno())
            selector = selectors.DefaultSelector()
            selector.register(
                process.stdout, selectors.EVENT_READ, ("stdout", stdout_maximum)
            )
            selector.register(
                process.stderr,
                selectors.EVENT_READ,
                ("stderr", _MAX_GIT_OUTPUT_BYTES),
            )
            buffers = {"stdout": [], "stderr": []}
            sizes = {"stdout": 0, "stderr": 0}
            deadline = time.monotonic() + _GIT_TIMEOUT_SECONDS
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    fail()
                events = selector.select(remaining)
                if not events:
                    fail()
                for key, _mask in events:
                    label, maximum = key.data
                    allowance = maximum - sizes[label] + 1
                    try:
                        chunk = os.read(
                            key.fileobj.fileno(), min(65536, allowance)
                        )
                    except OSError as error:
                        fail(error)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffers[label].append(chunk)
                    sizes[label] += len(chunk)
                    if sizes[label] > maximum:
                        fail()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fail()
            returncode = process.wait(timeout=remaining)
            result = (
                returncode,
                b"".join(buffers["stdout"]),
                b"".join(buffers["stderr"]),
            )
        except BaseException as error:
            original_error = error

        cleanup_error = None
        if selector is not None:
            try:
                selector.close()
            except BaseException as error:
                cleanup_error = error
        for stream in streams:
            try:
                stream.close()
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
        if process is not None:
            try:
                reap_process(process)
            except BaseException as error:
                if cleanup_error is None or (
                    isinstance(cleanup_error, Exception)
                    and not isinstance(error, Exception)
                ):
                    cleanup_error = error
        if original_error is not None:
            if not isinstance(original_error, Exception):
                raise original_error from cleanup_error
            if cleanup_error is not None and not isinstance(
                cleanup_error, Exception
            ):
                raise cleanup_error from original_error
            raise original_error
        if cleanup_error is not None:
            if not isinstance(cleanup_error, Exception):
                raise cleanup_error
            fail(cleanup_error)
        if result is None:
            fail()
        return result

    def run_git(
        arguments: Tuple[str, ...], *,
        stdout_maximum: int = _MAX_GIT_OUTPUT_BYTES,
    ) -> bytes:
        # Only closure-owned frozen call sites can reach this command channel.
        if (
            type(arguments) is not tuple
            or not arguments
            or any(type(argument) is not str for argument in arguments)
        ):
            fail()
        command = (
            "/usr/bin/git",
            "-C",
            str(_PROJECT_ROOT),
        ) + arguments
        returncode, stdout, _stderr = run_bounded_process(
            command, stdout_maximum=stdout_maximum
        )
        if returncode != 0:
            fail()
        return stdout

    def parse_commit(payload: bytes) -> str:
        try:
            value = payload.decode("ascii")
        except UnicodeDecodeError as error:
            fail(error)
        if not value.endswith("\n") or commit_pattern.fullmatch(value[:-1]) is None:
            fail()
        return value[:-1]

    def parse_size(payload: bytes) -> int:
        try:
            value = payload.decode("ascii")
        except UnicodeDecodeError as error:
            fail(error)
        if (
            not value.endswith("\n")
            or not value[:-1]
            or not value[:-1].isdigit()
        ):
            fail()
        size = int(value[:-1])
        if size < 0 or size > _MAX_TRACKED_SOURCE_BYTES:
            fail()
        return size

    def require_digest(value: Any) -> str:
        if type(value) is not str or hash_pattern.fullmatch(value) is None:
            fail()
        return value

    def require_version(value: Any) -> str:
        if (
            type(value) is not str
            or not value
            or len(value) > 128
            or any(character in value for character in ("/", "\\", "\n", "\r", "\x00"))
        ):
            fail()
        return value

    def safe_toolchain_projection(identity: Any) -> Tuple[Mapping[str, str], ...]:
        if (
            not isinstance(identity, Mapping)
            or identity.get("schema")
            != "historical_foundry_toolchain_candidate/v1"
        ):
            fail()
        rows = identity.get("binaries")
        solc = identity.get("solc")
        if type(rows) is not list or len(rows) != 3 or not isinstance(solc, Mapping):
            fail()
        foundry = {}
        for row in rows:
            if (
                type(row) is not dict
                or set(row) != {"name", "sha256", "version"}
                or row.get("name") not in ("forge", "cast", "anvil")
                or row["name"] in foundry
            ):
                fail()
            foundry[row["name"]] = {
                "name": row["name"],
                "sha256": require_digest(row["sha256"]),
                "version": require_version(row["version"]),
            }
        if set(foundry) != {"forge", "cast", "anvil"}:
            fail()
        projected = []
        for name in ("forge", "anvil", "cast"):
            projected.append(foundry[name])
        projected.append({
            "name": "solc",
            "sha256": require_digest(solc.get("sha256")),
            "version": require_version(solc.get("version")),
        })
        return tuple(projected)

    def frozen_projection(value: Any) -> Any:
        if type(value) is dict:
            return MappingProxyType({
                key: frozen_projection(item)
                for key, item in value.items()
            })
        if type(value) in (list, tuple):
            return tuple(frozen_projection(item) for item in value)
        return value

    def open_tracked_members():
        chain = ()
        members = []
        owned_member_descriptors = []
        try:
            chain = _open_absolute_directory_chain(_PROJECT_ROOT)
            root_descriptor = chain[-1][0]
            for name, relative_path in _TRACKED_SOURCE_INVENTORY:
                components = relative_path.split("/")
                parent_descriptor = root_descriptor
                directories = []
                for component in components[:-1]:
                    opened = _open_child_directory(
                        parent_descriptor=parent_descriptor,
                        name=component,
                    )
                    if opened is None:
                        fail()
                    descriptor, expected = opened
                    owned_member_descriptors.append(descriptor)
                    directories.append((
                        parent_descriptor,
                        component,
                        descriptor,
                        expected,
                    ))
                    parent_descriptor = descriptor
                pointer = _open_held_pointer(
                    relative_path=relative_path,
                    parent_descriptor=parent_descriptor,
                    leaf_name=components[-1],
                )
                if not pointer["present"]:
                    fail()
                owned_member_descriptors.append(pointer["descriptor"])
                members.append({
                    "name": name,
                    "relative_path": relative_path,
                    "directories": tuple(directories),
                    "pointer": pointer,
                })
            return chain, tuple(members)
        except BaseException as original_error:
            _close_after_error(
                tuple(owned_member_descriptors)
                + tuple(row[0] for row in chain),
                original_error,
            )
            raise AssertionError("unreachable")

    def require_members_unchanged(chain: Any, members: Any) -> None:
        for member in members:
            _require_held_pointer_stable(member["pointer"])
            for parent, name, descriptor, expected in reversed(
                member["directories"]
            ):
                _require_directory_stable(
                    parent_descriptor=parent,
                    name=name,
                    descriptor=descriptor,
                    expected=expected,
                )
        _require_absolute_directory_chain_stable(chain)

    def close_members(chain: Any, members: Any) -> None:
        descriptors = []
        for member in members:
            pointer_descriptor = member["pointer"].get("descriptor")
            if pointer_descriptor is not None:
                descriptors.append(pointer_descriptor)
            descriptors.extend(row[2] for row in member["directories"])
        descriptors.extend(row[0] for row in chain)
        _close_descriptor_inventory(descriptors)

    def parse_tree_member(
        payload: bytes, *, relative_path: str, expected_mode: str,
        expected_type: str,
    ) -> str:
        prefix = (expected_mode + " " + expected_type + " ").encode("ascii")
        suffix = b"\t" + relative_path.encode("ascii") + b"\x00"
        if (
            not payload.startswith(prefix)
            or not payload.endswith(suffix)
            or payload.count(b"\x00") != 1
        ):
            fail()
        oid_bytes = payload[len(prefix):-len(suffix)]
        try:
            oid = oid_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            fail(error)
        if commit_pattern.fullmatch(oid) is None:
            fail()
        return oid

    def parse_index_member(
        payload: bytes, *, relative_path: str, expected_mode: str,
    ) -> str:
        prefix = (expected_mode + " ").encode("ascii")
        suffix = b" 0\t" + relative_path.encode("ascii") + b"\x00"
        if (
            not payload.startswith(prefix)
            or not payload.endswith(suffix)
            or payload.count(b"\x00") != 1
        ):
            fail()
        oid_bytes = payload[len(prefix):-len(suffix)]
        try:
            oid = oid_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            fail(error)
        if commit_pattern.fullmatch(oid) is None:
            fail()
        return oid

    def member_payloads(members: Any) -> Mapping[str, bytes]:
        return {
            member["relative_path"]: member["pointer"]["payload"]
            for member in members
        }

    def read_git_source_state(
        *, frozen_head: str, members: Any,
    ) -> Tuple[Mapping[str, Any], ...]:
        physical = member_payloads(members)
        rows = []
        for name, relative_path in _TRACKED_SOURCE_INVENTORY:
            tree_payload = run_git((
                "ls-tree", "-z", frozen_head, "--", relative_path,
            ))
            oid = parse_tree_member(
                tree_payload,
                relative_path=relative_path,
                expected_mode="100644",
                expected_type="blob",
            )
            index_oid = parse_index_member(
                run_git((
                    "ls-files", "--stage", "-z", "--", relative_path,
                )),
                relative_path=relative_path,
                expected_mode="100644",
            )
            if index_oid != oid:
                fail()
            size = parse_size(run_git(("cat-file", "-s", oid)))
            payload = run_git(
                ("show", frozen_head + ":" + relative_path),
                stdout_maximum=size,
            )
            if len(payload) != size or payload != physical[relative_path]:
                fail()
            rows.append({
                "name": name,
                "relative_path": relative_path,
                "mode": "100644",
                "oid": oid,
                "size": size,
                "payload": payload,
            })
        return tuple(rows)

    def read_gitlink_state(*, frozen_head: str) -> Mapping[str, str]:
        head_oid = parse_tree_member(
            run_git((
                "ls-tree", "-z", frozen_head, "--", "lib/forge-std",
            )),
            relative_path="lib/forge-std",
            expected_mode="160000",
            expected_type="commit",
        )
        index_oid = parse_index_member(
            run_git((
                "ls-files", "--stage", "-z", "--", "lib/forge-std",
            )),
            relative_path="lib/forge-std",
            expected_mode="160000",
        )
        checkout_oid = parse_commit(run_git((
            "-C",
            "lib/forge-std",
            "rev-parse",
            "--verify",
            "HEAD^{commit}",
        )))
        if (
            head_oid != _FORGE_STD_COMMIT
            or index_oid != head_oid
            or checkout_oid != head_oid
        ):
            fail()
        return {
            "head_oid": head_oid,
            "index_oid": index_oid,
            "checkout_oid": checkout_oid,
        }

    def git_state_equal(left: Any, right: Any) -> bool:
        return left == right

    def require_git_and_members_unchanged(
        *, frozen: Mapping[str, Any], chain: Any, members: Any,
    ) -> None:
        if (
            parse_commit(run_git((
                "rev-parse", "--verify", "HEAD^{commit}",
            ))) != frozen["head"]
            or run_git((
                "status", "--porcelain=v1", "--untracked-files=all",
            )) != b""
        ):
            fail()
        rows = read_git_source_state(
            frozen_head=frozen["head"], members=members
        )
        gitlink = read_gitlink_state(frozen_head=frozen["head"])
        if (
            not git_state_equal(rows, frozen["rows"])
            or not git_state_equal(gitlink, frozen["gitlink"])
        ):
            fail()
        require_members_unchanged(chain, members)

    def require_project_identity(
        project_identity: Any, record_digests: Mapping[str, str],
    ) -> str:
        if (
            type(project_identity) is not dict
            or set(project_identity) != {
                "schema",
                "foundry_toml_sha256",
                "foundry_lock_sha256",
                "gitmodules_sha256",
                "forge_std_commit",
                "forge_std_tree_sha256",
            }
            or project_identity.get("schema")
            != "historical_foundry_project_input_identity/v1"
            or project_identity.get("foundry_toml_sha256")
            != record_digests["foundry_toml"]
            or project_identity.get("foundry_lock_sha256")
            != record_digests["foundry_lock"]
            or project_identity.get("gitmodules_sha256")
            != record_digests["gitmodules"]
            or project_identity.get("forge_std_commit")
            != _FORGE_STD_COMMIT
        ):
            fail()
        return require_digest(project_identity.get("forge_std_tree_sha256"))

    def close_authority_parts(
        *, chain: Any, members: Any, toolchain: Any,
        original_error: Optional[BaseException] = None,
    ) -> None:
        cleanup_error = None
        if toolchain is not None:
            try:
                toolchain.__exit__(
                    type(original_error) if original_error is not None else None,
                    original_error,
                    original_error.__traceback__
                    if original_error is not None else None,
                )
            except BaseException as error:
                cleanup_error = error
        try:
            close_members(chain, members)
        except BaseException as error:
            if cleanup_error is None or (
                isinstance(cleanup_error, Exception)
                and not isinstance(error, Exception)
            ):
                cleanup_error = error
        if original_error is not None:
            if not isinstance(original_error, Exception):
                raise original_error from cleanup_error
            if cleanup_error is not None and not isinstance(
                cleanup_error, Exception
            ):
                raise cleanup_error from original_error
            raise original_error
        if cleanup_error is not None:
            if not isinstance(cleanup_error, Exception):
                raise cleanup_error
            fail(cleanup_error)

    def close_authority_state(state: Mapping[str, Any]) -> None:
        close_authority_parts(
            chain=state["chain"],
            members=state["members"],
            toolchain=state["toolchain"],
        )

    def require_authority(instance: Any) -> Mapping[str, Any]:
        if type(instance) is not HeldCleanSource:
            fail()
        row = authority_registry.get(id(instance))
        if (
            type(row) is not tuple
            or len(row) != 2
            or row[0]() is not instance
            or row[1] is retired
        ):
            fail()
        return row[1]

    def retire_authority(instance: Any) -> Optional[Mapping[str, Any]]:
        if type(instance) is not HeldCleanSource:
            fail()
        identity = id(instance)
        row = authority_registry.get(identity)
        if (
            type(row) is not tuple
            or len(row) != 2
            or row[0]() is not instance
        ):
            fail()
        if row[1] is retired:
            return None
        authority_registry[identity] = (row[0], retired)
        return row[1]

    def register_authority(
        instance: Any, state: Mapping[str, Any],
    ) -> None:
        if type(instance) is not HeldCleanSource:
            fail()
        identity = id(instance)
        if identity in authority_registry:
            fail()

        def discard(reference: Any) -> None:
            row = authority_registry.get(identity)
            if type(row) is not tuple or row[0] is not reference:
                return
            authority_registry.pop(identity, None)
            if row[1] is retired:
                return
            try:
                close_authority_state(row[1])
            except BaseException:
                pass

        reference = weakref.ref(instance, discard)
        authority_registry[identity] = (reference, state)

    class HeldCleanSource:
        __slots__ = ("__weakref__",)

        def __new__(cls, key: Any, *args: Any, **kwargs: Any):
            if cls is not HeldCleanSource or key is not token:
                fail()
            return super().__new__(cls)

        def __init__(self, key: Any) -> None:
            if key is not token:
                fail()

        def __init_subclass__(cls, **kwargs: Any) -> None:
            raise TypeError("held clean source cannot be subclassed")

        def __repr__(self) -> str:
            return "HeldCleanHistoricalSource(<sealed>)"

        def __reduce__(self) -> Any:
            raise TypeError("held clean source is not serializable")

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError("held clean source is not serializable")

        def __copy__(self) -> Any:
            raise TypeError("held clean source is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("held clean source is not copyable")

        @property
        def identity_projection(self) -> Mapping[str, Any]:
            return require_authority(self)["identity_projection"]

        def reread_unchanged(self) -> None:
            state = require_authority(self)
            try:
                state["toolchain"]._verify_versions_and_hardfork()
                current_toolchain_identity = (
                    state["toolchain"].verified_identity
                )
                safe_toolchain_projection(current_toolchain_identity)
                if (
                    frozen_projection(current_toolchain_identity)
                    != state["toolchain_identity"]
                ):
                    fail()
                current_project_identity = (
                    state["toolchain"].verified_project_input_identity()
                )
                if current_project_identity != state["project_identity"]:
                    fail()
                require_project_identity(
                    current_project_identity, state["record_digests"]
                )
                require_git_and_members_unchanged(
                    frozen=state["frozen_git"],
                    chain=state["chain"],
                    members=state["members"],
                )
            except HistoricalReplayEntrypointError:
                raise
            except Exception as error:
                fail(error)

        def close(self) -> None:
            state = retire_authority(self)
            if state is None:
                return
            close_authority_state(state)

        def __enter__(self):
            require_authority(self)
            return self

        def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
            cleanup_error = None
            try:
                self.close()
            except BaseException as error:
                cleanup_error = error
            if (
                _type is not None
                and not issubclass(_type, Exception)
                and isinstance(_value, BaseException)
            ):
                raise _value.with_traceback(_traceback) from cleanup_error
            if cleanup_error is not None:
                raise cleanup_error
            return None

        def __del__(self) -> None:
            try:
                self.close()
            except BaseException:
                pass

    def verify() -> HeldCleanSource:
        chain = ()
        members = ()
        toolchain = None
        try:
            chain, members = open_tracked_members()
            if run_git((
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            )) != b"":
                fail()
            repository_head = parse_commit(
                run_git(("rev-parse", "--verify", "HEAD^{commit}"))
            )
            rows = read_git_source_state(
                frozen_head=repository_head, members=members
            )
            gitlink = read_gitlink_state(frozen_head=repository_head)
            frozen_git = {
                "head": repository_head,
                "rows": rows,
                "gitlink": gitlink,
            }
            record_digests = {
                row["name"]: hashlib.sha256(row["payload"]).hexdigest()
                for row in rows
            }
            records = tuple({
                "name": row["name"],
                "size": row["size"],
                "sha256": record_digests[row["name"]],
            } for row in rows)

            toolchain = _open_reviewed_historical_toolchain()
            entered = toolchain.__enter__()
            if entered is not toolchain:
                fail()
            toolchain._verify_versions_and_hardfork()
            toolchain_identity = toolchain.verified_identity
            safe_toolchain = safe_toolchain_projection(toolchain_identity)
            frozen_toolchain_identity = frozen_projection(toolchain_identity)
            project_identity = (
                toolchain.verified_project_input_identity()
            )
            forge_std_tree_sha256 = require_digest(
                require_project_identity(project_identity, record_digests)
            )
            require_git_and_members_unchanged(
                frozen=frozen_git, chain=chain, members=members
            )
            projection = frozen_projection({
                "schema": "historical_foundry_clean_source_preflight/v1",
                "repository_head": repository_head,
                "tracked_source": records,
                "forge_std": {
                    "commit": _FORGE_STD_COMMIT,
                    "tree_sha256": forge_std_tree_sha256,
                },
                "toolchain": safe_toolchain,
            })
            authority = HeldCleanSource(token)
            register_authority(
                authority,
                MappingProxyType({
                    "chain": chain,
                    "members": members,
                    "toolchain": toolchain,
                    "toolchain_identity": frozen_toolchain_identity,
                    "frozen_git": frozen_git,
                    "project_identity": project_identity,
                    "record_digests": record_digests,
                    "identity_projection": projection,
                }),
            )
            chain = ()
            members = ()
            toolchain = None
            return authority
        except BaseException as error:
            if not isinstance(error, HistoricalReplayEntrypointError) and isinstance(
                error, Exception
            ):
                error = _entrypoint_error(
                    "historical source preflight failed"
                )
            close_authority_parts(
                chain=chain,
                members=members,
                toolchain=toolchain,
                original_error=error,
            )
            raise AssertionError("unreachable")

    verify.__name__ = "verify_clean_tracked_historical_source"
    verify.__qualname__ = "verify_clean_tracked_historical_source"
    return verify


verify_clean_tracked_historical_source = _initialize_clean_source_verifier()
del _initialize_clean_source_verifier


def _drive_historical_candidate_replay(
    *, snapshot: Any, replay_context: Any,
) -> Mapping[str, Any]:
    """Consume the sealed newest-first selection protocol to one terminal."""
    import scripts.historical_foundry_anvil as anvil
    import scripts.historical_foundry_scan as scan

    replay_ledger = None
    while True:
        step = scan._advance_historical_selection_controller(
            snapshot=snapshot, replay_ledger=replay_ledger
        )
        if isinstance(step, Mapping):
            status = step.get("status")
            if status in (
                "found_publishable_profitable_block",
                "no_publishable_profitable_block",
            ):
                return step
            if status == "candidate_unresolved":
                raise _entrypoint_error(
                    "historical replay candidate is unresolved"
                )
            raise _entrypoint_error(
                "historical replay selection is invalid"
            )

        action = step
        scenario = scan._consume_historical_selection_action(
            action=action, context=replay_context
        )
        sink = anvil._open_scenario_evidence_sink(
            context=replay_context, scenario=scenario
        )
        try:
            anvil._replay_historical_scenario(
                context=replay_context, scenario=scenario, sink=sink
            )
        except anvil.HistoricalReplayError as error:
            scan._record_historical_selection_failure(
                action=action, error=error
            )
            terminal = scan._advance_historical_selection_controller(
                snapshot=snapshot, replay_ledger=replay_ledger
            )
            if (
                not isinstance(terminal, Mapping)
                or terminal.get("status") != "candidate_unresolved"
            ):
                raise _entrypoint_error(
                    "historical replay selection is invalid"
                )
            raise _entrypoint_error(
                "historical replay candidate is unresolved"
            ) from None
        replay_ledger = sink.validated_ledger()


def _close_historical_controller_resources(
    resources: Sequence[Any],
) -> Optional[BaseException]:
    first_error = None
    closed = set()
    for resource in reversed(tuple(resources)):
        if resource is None or id(resource) in closed:
            continue
        closed.add(id(resource))
        closer = getattr(resource, "close", None)
        if not callable(closer):
            exit_method = getattr(resource, "__exit__", None)
            closer = (
                (lambda method=exit_method: method(None, None, None))
                if callable(exit_method)
                else None
            )
        if not callable(closer):
            continue
        try:
            closer()
        except BaseException as error:
            if first_error is None or (
                isinstance(first_error, Exception)
                and not isinstance(error, Exception)
            ):
                first_error = error
    return first_error


def _produce_historical_raw_run(*, data_dir: Path) -> Mapping[str, Any]:
    """Execute capture through immutable raw-run finalization once."""
    import scripts.historical_foundry_anvil as anvil
    import scripts.historical_foundry_contracts as contracts
    import scripts.historical_foundry_rpc as rpc
    import scripts.historical_foundry_scan as scan
    import scripts.historical_foundry_storage as storage

    owned = []
    try:
        config = contracts.load_historical_foundry_config_set()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=data_dir
        )
        owned = [spool]
        rpc_context = rpc._open_production_archive_rpc_run()
        owned = [spool, rpc_context]
        claim = (
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=rpc_context
            )
        )
        owned = [spool, claim]
        capability = scan._capture_production_historical_window(
            claim=claim, spool=spool
        )
        owned = [capability]
        capture = scan._materialize_historical_window_staging_snapshot(
            capability=capability
        )
        owned = [capture]
        window = scan.open_validated_historical_window(
            config=config, staging=capture
        )
        rows = scan.build_historical_prefilter_grid(
            config=config, window=window
        )
        prefilter = storage._freeze_historical_prefilter_grid(
            staging=capture, rows=rows
        )
        owned = [prefilter]
        snapshot = scan.open_validated_historical_scan_snapshot(
            config=config, staging=prefilter
        )
        artifact = contracts.build_validated_executor_artifact(config)
        replay_context = anvil.open_historical_replay_context(
            config=config,
            staging=prefilter,
            window=snapshot.validated_window,
            grid=snapshot.validated_grid,
            executor_artifact=artifact,
        )
        owned = [replay_context]
        selection = _drive_historical_candidate_replay(
            snapshot=snapshot, replay_context=replay_context
        )
        finalized = scan._finalize_historical_replay_run(
            config=config, snapshot=snapshot, selection=selection
        )
        owned = [replay_context, finalized]
        run_identity = dict(finalized.identity_projection())
        replay_context.close()
        owned = [finalized]
        selection_projection = dict(selection)
        if selection_projection.get("status") == (
            "no_publishable_profitable_block"
        ):
            finalized.close()
            owned = []
            return {
                "config": config,
                "selection": selection_projection,
                "run": None,
                "run_identity": run_identity,
                "publication_lease": None,
            }
        if selection_projection.get("status") != (
            "found_publishable_profitable_block"
        ):
            raise _entrypoint_error(
                "historical replay selection is invalid"
            )
        publication_lease = (
            storage._acquire_historical_run_publication_lease(
                run_id=run_identity["run_id"],
                expected_manifest_sha256=run_identity[
                    "run_manifest_sha256"
                ],
            )
        )
        owned = [finalized, publication_lease]
        result = {
            "config": config,
            "selection": selection_projection,
            "run": finalized,
            "run_identity": run_identity,
            "publication_lease": publication_lease,
        }
        owned = []
        return result
    except BaseException as error:
        cleanup_error = _close_historical_controller_resources(owned)
        if not isinstance(error, Exception):
            raise error from cleanup_error
        if cleanup_error is not None and not isinstance(
            cleanup_error, Exception
        ):
            raise cleanup_error from error
        if isinstance(error, HistoricalReplayEntrypointError):
            raise error
        raise _entrypoint_error(
            "historical production scan failed"
        ) from error


def _prepare_historical_replay_bundle(
    *, data_dir: Path, raw_state: Mapping[str, Any], publish: bool,
) -> Mapping[str, Any]:
    """Stage the private core and complete bundle from one raw authority."""
    import scripts.historical_route_publication as publication

    if (
        type(raw_state) is not dict
        or type(publish) is not bool
        or raw_state.get("run") is None
        or raw_state.get("publication_lease") is None
    ):
        raise _entrypoint_error(
            "historical publication preparation is invalid"
        )
    finalized = raw_state["run"]
    publication_lease = raw_state["publication_lease"]
    raw_state["run"] = None
    raw_state["publication_lease"] = None
    owned = [finalized, publication_lease]
    try:
        core_stage = publication.stage_historical_replay_core(
            data_dir=data_dir,
            config=raw_state["config"],
            publication_lease=publication_lease,
        )
        owned = [finalized, core_stage]
        staged_context = (
            publication.load_validated_historical_replay_core_at(
                staged_core=core_stage
            )
        )
        owned = [finalized, core_stage, staged_context]
        staged_projection_bytes = publication._canonical_bytes(
            dict(staged_context.identity_projection())
        )
        staged_payload_bytes = publication._canonical_bytes(dict(
            publication._build_historical_complete_payload(
                context=staged_context
            )
        ))

        if publish:
            staged_context.close()
            owned = [finalized, core_stage]
            context = publication.publish_historical_replay_core(
                data_dir=data_dir, staged_core=core_stage
            )
            core_stage = None
            owned = [finalized, context]
            if publication._canonical_bytes(
                dict(context.identity_projection())
            ) != staged_projection_bytes:
                raise _entrypoint_error(
                    "historical committed core differs from staged core"
                )
        else:
            context = staged_context

        authoritative_payload_bytes = publication._canonical_bytes(dict(
            publication._build_historical_complete_payload(
                context=context
            )
        ))
        if authoritative_payload_bytes != staged_payload_bytes:
            raise _entrypoint_error(
                "historical opportunity economics changed after core staging"
            )
        bundle = publication.stage_historical_replay_bundle(
            data_dir=data_dir,
            raw_root=(data_dir / "raw" / "historical-foundry-replay"),
            context=context,
        )
        if not isinstance(bundle, Mapping):
            raise _entrypoint_error(
                "historical complete bundle is invalid"
            )
        subject = bundle.get("verification_subject")
        pointer_publication = bundle.get("pointer_publication")
        if subject is None or pointer_publication is None:
            raise _entrypoint_error(
                "historical complete bundle is invalid"
            )
        owned.append(subject)
        owned.append(pointer_publication)
        prepared = {
            "mode": "publish" if publish else "dry-run",
            "data_dir": data_dir,
            "selection": raw_state["selection"],
            "run_identity": raw_state["run_identity"],
            "run": finalized,
            "core_stage": core_stage,
            "context": context,
            "bundle": bundle,
            "verification_subject": subject,
            "pointer_publication": pointer_publication,
            "_owned_resources": owned,
        }
        owned = []
        return prepared
    except BaseException as error:
        cleanup_error = _close_historical_controller_resources(owned)
        if not isinstance(error, Exception):
            raise error from cleanup_error
        if cleanup_error is not None and not isinstance(
            cleanup_error, Exception
        ):
            raise cleanup_error from error
        if isinstance(error, HistoricalReplayEntrypointError):
            raise error
        raise _entrypoint_error(
            "historical publication preparation failed"
        ) from error


def _close_prepared_historical_bundle(
    prepared: Mapping[str, Any],
) -> None:
    if type(prepared) is not dict:
        raise _entrypoint_error(
            "historical publication cleanup is invalid"
        )
    resources = prepared.get("_owned_resources")
    if type(resources) is not list:
        if resources == ():
            return None
        raise _entrypoint_error(
            "historical publication cleanup is invalid"
        )
    prepared["_owned_resources"] = ()
    cleanup_error = _close_historical_controller_resources(resources)
    if cleanup_error is not None:
        if not isinstance(cleanup_error, Exception):
            raise cleanup_error
        raise _entrypoint_error(
            "historical publication cleanup failed"
        ) from cleanup_error
    return None


def _verify_prepared_historical_bundle(
    *, prepared: Mapping[str, Any], publish: bool,
) -> Mapping[str, Any]:
    """Run the sealed connected verifier and validate its handoff bytes."""
    import scripts.historical_foundry_verifier as verifier

    if (
        type(prepared) is not dict
        or type(publish) is not bool
        or prepared.get("mode")
        != ("publish" if publish else "dry-run")
    ):
        raise _entrypoint_error(
            "historical connected verification input is invalid"
        )
    subject = prepared.get("verification_subject")
    try:
        subject.reread_unchanged()
        result = verifier.run_connected_historical_verification(
            subject, mode="publish" if publish else "staged"
        )
        subject.reread_unchanged()
        if not isinstance(result, Mapping):
            raise _entrypoint_error(
                "historical connected verification result is invalid"
            )
        report = result.get("report")
        report_bytes = result.get("report_bytes")
        report_sha256 = result.get("report_sha256")
        pointer_core = result.get("pointer_core")
        final_pointer = result.get("final_pointer")
        final_pointer_bytes = result.get("final_pointer_bytes")
        bundle = prepared.get("bundle")
        bundle_pointer_core = (
            bundle.get("pointer_core")
            if isinstance(bundle, Mapping)
            else None
        )
        expected_mode = "publish" if publish else "staged"
        if (
            result.get("schema")
            != "historical_connected_verification_result/v1"
            or result.get("mode") != expected_mode
            or not isinstance(report, Mapping)
            or report.get("schema")
            != "route_historical_replay_verification/v1"
            or report.get("status") != "verified"
            or report.get("evidence_mode") != "production_connected"
            or type(report_bytes) is not bytes
            or type(report_sha256) is not str
            or hashlib.sha256(report_bytes).hexdigest() != report_sha256
            or not isinstance(pointer_core, Mapping)
            or not isinstance(bundle_pointer_core, Mapping)
            or dict(pointer_core) != dict(bundle_pointer_core)
            or not isinstance(final_pointer, Mapping)
            or type(final_pointer_bytes) is not bytes
            or final_pointer_bytes
            != verifier._canonical_bytes(dict(final_pointer))
            or final_pointer.get("verification_report_sha256")
            != report_sha256
            or dict(verifier.historical_replay_pointer_core(
                final_pointer
            )) != dict(pointer_core)
            or publish and result.get("install_result") is None
            or not publish and result.get("install_result") is not None
        ):
            if (
                isinstance(report, Mapping)
                and report.get("evidence_mode")
                != "production_connected"
            ):
                raise _entrypoint_error(
                    "historical production-connected evidence is required"
                )
            raise _entrypoint_error(
                "historical connected verification result is invalid"
            )
        prepared["verification"] = result
        return result
    except BaseException as error:
        if not isinstance(error, Exception):
            raise
        if isinstance(error, HistoricalReplayEntrypointError):
            raise
        raise _entrypoint_error(
            "historical connected verification failed"
        ) from error


def _publish_verified_historical_bundle(
    *, prepared: Mapping[str, Any], verification: Mapping[str, Any],
    publish: bool,
) -> Optional[Mapping[str, Any]]:
    """Hand the exact connected-verification result to the pointer CAS."""
    import scripts.historical_route_publication as publication

    expected_prepared_mode = "publish" if publish else "dry-run"
    expected_verification_mode = "publish" if publish else "staged"
    if (
        type(prepared) is not dict
        or type(publish) is not bool
        or prepared.get("mode") != expected_prepared_mode
        or verification is not prepared.get("verification")
        or not isinstance(verification, Mapping)
        or verification.get("mode") != expected_verification_mode
    ):
        raise _entrypoint_error(
            "historical complete publication handoff is invalid"
        )
    if not publish:
        return None
    subject = prepared.get("verification_subject")
    data_dir = prepared.get("data_dir")
    authority = prepared.get("pointer_publication")
    final_pointer = verification.get("final_pointer")
    final_pointer_bytes = verification.get("final_pointer_bytes")
    if (
        subject is None
        or not isinstance(data_dir, Path)
        or authority is None
        or not isinstance(final_pointer, Mapping)
        or type(final_pointer_bytes) is not bytes
    ):
        raise _entrypoint_error(
            "historical complete publication handoff is invalid"
        )
    try:
        subject.reread_unchanged()
        installed = publication.publish_historical_replay_bundle(
            data_dir=data_dir,
            pointer_publication=authority,
            final_pointer_bytes=final_pointer_bytes,
        )
        subject.reread_unchanged()
        if (
            not isinstance(installed, Mapping)
            or dict(installed) != dict(final_pointer)
        ):
            raise _entrypoint_error(
                "historical complete pointer result is invalid"
            )
        prepared["complete_pointer"] = installed
        return installed
    except BaseException as error:
        if not isinstance(error, Exception):
            raise
        if isinstance(error, HistoricalReplayEntrypointError):
            raise
        raise _entrypoint_error(
            "historical complete pointer publication failed"
        ) from error


def _audit_latest_historical_replay_bundle(
    *, data_dir: Path, bundle_path: Path,
) -> Mapping[str, Any]:
    """Audit only the bundle pinned by the current historical pointer."""
    import scripts.historical_foundry_verifier as verifier
    import scripts.historical_route_publication as publication
    import scripts.route_publication as route_publication

    if (
        not isinstance(data_dir, Path)
        or not isinstance(bundle_path, Path)
        or not bundle_path.is_absolute()
    ):
        raise _entrypoint_error("historical audit input is invalid")
    historical_fd = None
    locked = False
    subject = None
    result = None
    original_error = None
    try:
        data = route_publication._absolute_without_symlink_resolution(
            data_dir
        )
        requested_bundle = (
            route_publication._absolute_without_symlink_resolution(
                bundle_path
            )
        )
        historical_root, historical_fd, historical_details = (
            route_publication._open_verified_directory(
                data / "routes" / "historical",
                "historical audit root",
            )
        )
        fcntl.flock(historical_fd, fcntl.LOCK_SH)
        locked = True
        pointer_before = route_publication._optional_pointer_snapshot_at(
            historical_fd
        )
        if pointer_before is None:
            raise _entrypoint_error(
                "current historical pointer is missing"
            )
        pointer_bytes = pointer_before[0]
        try:
            pointer = json.loads(pointer_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise _entrypoint_error(
                "current historical pointer is invalid"
            ) from error
        if (
            type(pointer) is not dict
            or publication._canonical_bytes(pointer) != pointer_bytes
        ):
            raise _entrypoint_error(
                "current historical pointer is invalid"
            )
        pointer_core = dict(
            verifier.historical_replay_pointer_core(pointer)
        )
        expected_bundle = (
            historical_root / "bundles" / pointer_core["replay_id"]
        )
        if requested_bundle != expected_bundle:
            raise _entrypoint_error(
                "bundle is not the current historical pointer directory"
            )
        validated = publication.validate_historical_replay_bundle(
            data_dir=data,
            raw_root=(data / "raw" / "historical-foundry-replay"),
            bundle_path=requested_bundle,
            expected_pointer_core=pointer_core,
        )
        if not isinstance(validated, Mapping):
            raise _entrypoint_error(
                "historical audit bundle is invalid"
            )
        subject = validated.get("verification_subject")
        manifest = validated.get("manifest")
        if (
            subject is None
            or validated.get("path") != requested_bundle
            or validated.get("replay_id") != pointer_core["replay_id"]
            or validated.get("manifest_sha256")
            != pointer_core["manifest_sha256"]
            or not isinstance(validated.get("pointer_core"), Mapping)
            or dict(validated["pointer_core"]) != pointer_core
            or not isinstance(manifest, Mapping)
        ):
            raise _entrypoint_error(
                "historical audit bundle is invalid"
            )
        report_before = (
            publication._reread_historical_verification_report(
                data_dir=data, final_pointer=pointer
            )
        )
        if (
            type(report_before) is not bytes
            or hashlib.sha256(report_before).hexdigest()
            != pointer["verification_report_sha256"]
        ):
            raise _entrypoint_error(
                "historical retained verification report is invalid"
            )
        subject.reread_unchanged()
        verification = verifier.run_connected_historical_verification(
            subject, mode="audit"
        )
        subject.reread_unchanged()
        if not isinstance(verification, Mapping):
            raise _entrypoint_error(
                "historical audit verification result is invalid"
            )
        audit_report = verification.get("report")
        audit_report_bytes = verification.get("report_bytes")
        audit_report_sha256 = verification.get("report_sha256")
        audit_pointer = verification.get("final_pointer")
        audit_pointer_bytes = verification.get("final_pointer_bytes")
        if (
            verification.get("schema")
            != "historical_connected_verification_result/v1"
            or verification.get("mode") != "audit"
            or verification.get("install_result") is not None
            or not isinstance(audit_report, Mapping)
            or audit_report.get("status") != "verified"
            or audit_report.get("evidence_mode")
            != "production_connected"
            or type(audit_report_bytes) is not bytes
            or verifier._canonical_bytes(dict(audit_report))
            != audit_report_bytes
            or type(audit_report_sha256) is not str
            or hashlib.sha256(audit_report_bytes).hexdigest()
            != audit_report_sha256
            or not isinstance(verification.get("pointer_core"), Mapping)
            or dict(verification["pointer_core"]) != pointer_core
            or not isinstance(audit_pointer, Mapping)
            or type(audit_pointer_bytes) is not bytes
            or publication._canonical_bytes(dict(audit_pointer))
            != audit_pointer_bytes
            or audit_pointer.get("verification_report_sha256")
            != audit_report_sha256
            or dict(verifier.historical_replay_pointer_core(
                audit_pointer
            )) != pointer_core
        ):
            raise _entrypoint_error(
                "historical audit verification result is invalid"
            )
        verifier._require_historical_audit_report_parity(
            retained_report_bytes=report_before,
            audit_report=audit_report,
        )
        report_after = publication._reread_historical_verification_report(
            data_dir=data, final_pointer=pointer
        )
        pointer_after = route_publication._optional_pointer_snapshot_at(
            historical_fd
        )
        route_publication._verify_open_path_identity(
            historical_root,
            historical_details,
            "historical audit root",
        )
        if (
            report_after != report_before
            or not publication._snapshot_matches(
                pointer_after, pointer_before
            )
        ):
            raise _entrypoint_error(
                "historical audit changed during verification"
            )
        run_id = manifest.get("run_id")
        run_manifest_sha256 = manifest.get("run_manifest_sha256")
        if type(run_id) is not str or type(run_manifest_sha256) is not str:
            raise _entrypoint_error(
                "historical audit bundle is invalid"
            )
        result = MappingProxyType({
            "status": "verified",
            "run_identity": {
                "run_id": run_id,
                "run_manifest_sha256": run_manifest_sha256,
            },
            "bundle": {
                "replay_id": pointer_core["replay_id"],
                "manifest_sha256": pointer_core["manifest_sha256"],
                "pointer_core": pointer_core,
            },
            "verification": {
                "retained_report_sha256": pointer[
                    "verification_report_sha256"
                ],
                "audit_report_sha256": audit_report_sha256,
                "audit_final_pointer": dict(audit_pointer),
            },
            "published_pointer": dict(pointer),
        })
    except BaseException as error:
        original_error = error

    cleanup_error = _close_historical_controller_resources((subject,))
    if locked and historical_fd is not None:
        try:
            fcntl.flock(historical_fd, fcntl.LOCK_UN)
        except BaseException as error:
            if cleanup_error is None or (
                isinstance(cleanup_error, Exception)
                and not isinstance(error, Exception)
            ):
                cleanup_error = error
    if historical_fd is not None:
        try:
            os.close(historical_fd)
        except BaseException as error:
            if cleanup_error is None or (
                isinstance(cleanup_error, Exception)
                and not isinstance(error, Exception)
            ):
                cleanup_error = error
    if original_error is not None:
        if not isinstance(original_error, Exception):
            raise original_error from cleanup_error
        if cleanup_error is not None and not isinstance(
            cleanup_error, Exception
        ):
            raise cleanup_error from original_error
        if isinstance(original_error, HistoricalReplayEntrypointError):
            raise original_error
        raise _entrypoint_error("historical audit failed") from original_error
    if cleanup_error is not None:
        if not isinstance(cleanup_error, Exception):
            raise cleanup_error
        raise _entrypoint_error(
            "historical audit cleanup failed"
        ) from cleanup_error
    if result is None:
        raise _entrypoint_error("historical audit failed")
    return result


def _invoke_production_controller(
    arguments: argparse.Namespace, preflight: Any,
) -> Mapping[str, Any]:
    if type(arguments) is not argparse.Namespace:
        raise _entrypoint_error(
            "historical production controller is unavailable"
        )
    if arguments.command == "scan":
        valid_arguments = (
            set(vars(arguments))
            == {"command", "data_dir", "publish", "dry_run"}
            and isinstance(arguments.data_dir, Path)
            and type(arguments.publish) is bool
            and type(arguments.dry_run) is bool
            and arguments.publish != arguments.dry_run
        )
    elif arguments.command == "verify":
        valid_arguments = (
            set(vars(arguments)) == {"command", "data_dir", "bundle"}
            and isinstance(arguments.data_dir, Path)
            and isinstance(arguments.bundle, Path)
            and arguments.bundle.is_absolute()
        )
    else:
        valid_arguments = False
    if not valid_arguments:
        raise _entrypoint_error(
            "historical production controller is unavailable"
        )
    try:
        source_identity = preflight.identity_projection
    except Exception as error:
        raise _entrypoint_error(
            "historical production controller input is invalid"
        ) from error
    if not isinstance(source_identity, Mapping):
        raise _entrypoint_error(
            "historical production controller input is invalid"
        )

    if arguments.command == "verify":
        audit = _audit_latest_historical_replay_bundle(
            data_dir=arguments.data_dir,
            bundle_path=arguments.bundle,
        )
        if not isinstance(audit, Mapping):
            raise _entrypoint_error(
                "historical audit controller result is invalid"
            )
        return MappingProxyType({
            "schema": "historical_replay_command_result/v1",
            "command": "verify",
            "mode": "audit",
            "status": audit.get("status"),
            "source_identity": dict(source_identity),
            "run_identity": audit.get("run_identity"),
            "bundle": audit.get("bundle"),
            "verification": audit.get("verification"),
            "published_pointer": audit.get("published_pointer"),
        })

    publish = arguments.publish
    raw_state = None
    prepared = None
    result = None
    original_error = None
    try:
        raw_state = _produce_historical_raw_run(
            data_dir=arguments.data_dir
        )
        if type(raw_state) is not dict:
            raise _entrypoint_error(
                "historical raw controller result is invalid"
            )
        selection = raw_state.get("selection")
        run_identity = raw_state.get("run_identity")
        if (
            type(selection) is not dict
            or type(run_identity) is not dict
        ):
            raise _entrypoint_error(
                "historical raw controller result is invalid"
            )
        status = selection.get("status")
        result_base = {
            "schema": "historical_replay_command_result/v1",
            "command": "scan",
            "mode": "publish" if publish else "dry-run",
            "status": status,
            "source_identity": dict(source_identity),
            "selection": dict(selection),
            "run_identity": dict(run_identity),
        }
        if status == "no_publishable_profitable_block":
            if (
                raw_state.get("run") is not None
                or raw_state.get("publication_lease") is not None
            ):
                raise _entrypoint_error(
                    "historical raw controller result is invalid"
                )
            result = MappingProxyType({
                **result_base,
                "bundle": None,
                "verification": None,
                "published_pointer": None,
            })
        elif status == "found_publishable_profitable_block":
            if (
                raw_state.get("run") is None
                or raw_state.get("publication_lease") is None
            ):
                raise _entrypoint_error(
                    "historical raw controller result is invalid"
                )
            prepared = _prepare_historical_replay_bundle(
                data_dir=arguments.data_dir,
                raw_state=raw_state,
                publish=publish,
            )
            if type(prepared) is not dict:
                raise _entrypoint_error(
                    "historical publication preparation is invalid"
                )
            verification = _verify_prepared_historical_bundle(
                prepared=prepared, publish=publish
            )
            published_pointer = _publish_verified_historical_bundle(
                prepared=prepared,
                verification=verification,
                publish=publish,
            )
            bundle = prepared.get("bundle")
            final_pointer = verification.get("final_pointer")
            if (
                not isinstance(bundle, Mapping)
                or not isinstance(verification, Mapping)
                or not isinstance(final_pointer, Mapping)
                or type(bundle.get("replay_id")) is not str
                or type(bundle.get("manifest_sha256")) is not str
                or not isinstance(bundle.get("pointer_core"), Mapping)
                or type(verification.get("report_sha256")) is not str
                or publish
                and (
                    not isinstance(published_pointer, Mapping)
                    or dict(published_pointer) != dict(final_pointer)
                )
                or not publish and published_pointer is not None
            ):
                raise _entrypoint_error(
                    "historical production controller result is invalid"
                )
            result = MappingProxyType({
                **result_base,
                "bundle": {
                    "replay_id": bundle["replay_id"],
                    "manifest_sha256": bundle["manifest_sha256"],
                    "pointer_core": dict(bundle["pointer_core"]),
                },
                "verification": {
                    "report_sha256": verification["report_sha256"],
                    "final_pointer": dict(final_pointer),
                },
                "published_pointer": (
                    dict(published_pointer) if publish else None
                ),
            })
        else:
            raise _entrypoint_error(
                "historical replay selection is invalid"
            )
    except BaseException as error:
        original_error = error

    cleanup_error = None
    if prepared is not None:
        try:
            _close_prepared_historical_bundle(prepared)
        except BaseException as error:
            cleanup_error = error
    if type(raw_state) is dict:
        raw_resources = [
            raw_state.get("run"), raw_state.get("publication_lease")
        ]
        raw_state["run"] = None
        raw_state["publication_lease"] = None
        raw_cleanup_error = _close_historical_controller_resources(
            raw_resources
        )
        if cleanup_error is None or (
            isinstance(cleanup_error, Exception)
            and raw_cleanup_error is not None
            and not isinstance(raw_cleanup_error, Exception)
        ):
            cleanup_error = raw_cleanup_error

    if original_error is not None:
        if not isinstance(original_error, Exception):
            raise original_error from cleanup_error
        if cleanup_error is not None and not isinstance(
            cleanup_error, Exception
        ):
            raise cleanup_error from original_error
        if isinstance(original_error, HistoricalReplayEntrypointError):
            raise original_error
        raise _entrypoint_error(
            "historical production controller failed"
        ) from original_error
    if cleanup_error is not None:
        if not isinstance(cleanup_error, Exception):
            raise cleanup_error
        raise _entrypoint_error(
            "historical production controller cleanup failed"
        ) from cleanup_error
    if result is None:
        raise _entrypoint_error(
            "historical production controller is unavailable"
        )
    return result


def _execute(arguments: argparse.Namespace) -> Mapping[str, Any]:
    _require_safe_historical_startup()
    guard = _LivePointerGuard(data_dir=arguments.data_dir)
    with guard:
        preflight = verify_clean_tracked_historical_source()
        original_error = None
        result = None
        try:
            preflight.reread_unchanged()
            result = _invoke_production_controller(arguments, preflight)
        except BaseException as error:
            original_error = error
        maintenance_error = None
        try:
            preflight.reread_unchanged()
        except BaseException as error:
            maintenance_error = error
        try:
            preflight.close()
        except BaseException as error:
            if maintenance_error is None or (
                isinstance(maintenance_error, Exception)
                and not isinstance(error, Exception)
            ):
                maintenance_error = error
        if original_error is not None:
            if not isinstance(original_error, Exception):
                raise original_error from maintenance_error
            if maintenance_error is not None:
                if not isinstance(maintenance_error, Exception):
                    raise maintenance_error from original_error
                raise maintenance_error from original_error
            raise original_error
        if maintenance_error is not None:
            raise maintenance_error
        if result is None:
            raise _entrypoint_error(
                "historical production controller is unavailable"
            )
    if (
        not isinstance(result, Mapping)
        or "live_pointers_before" in result
        or "live_pointers_after" in result
        or guard.before is None
        or guard.after is None
    ):
        raise _entrypoint_error(
            "historical production controller result is invalid"
        )
    return MappingProxyType({
        **dict(result),
        "live_pointers_before": project_live_pointer_snapshots(
            guard.before
        ),
        "live_pointers_after": project_live_pointer_snapshots(
            guard.after
        ),
    })


def main(arguments: Optional[Sequence[str]] = None) -> int:
    try:
        _require_safe_historical_startup()
    except HistoricalReplayEntrypointError as error:
        sys.stderr.write(str(error) + "\n")
        return 1
    parsed = _parse_arguments(
        sys.argv[1:] if arguments is None else arguments
    )
    try:
        _execute(parsed)
    except HistoricalReplayEntrypointError as error:
        sys.stderr.write(str(error) + "\n")
        return 1
    return 0
