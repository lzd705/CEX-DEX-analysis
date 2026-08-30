from __future__ import annotations

from collections.abc import Mapping as MappingABC
import hashlib
import json
from pathlib import Path
import os
import stat
import sys
from types import MappingProxyType
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple
import weakref


_HISTORICAL_WINDOW_MODULE_GENERATION = object()


class HistoricalFoundryStorageError(RuntimeError):
    __slots__ = ()

    def __init__(self) -> None:
        RuntimeError.__init__(self, "historical foundry storage failed")
        RuntimeError.__setattr__(self, "__suppress_context__", True)

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        raise TypeError("HistoricalFoundryStorageError is sealed")

    def __repr__(self) -> str:
        return "HistoricalFoundryStorageError(<redacted>)"

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("HistoricalFoundryStorageError is immutable")

    def __copy__(self) -> Any:
        raise TypeError("HistoricalFoundryStorageError is not copyable")

    def __deepcopy__(self, _memo: Any) -> Any:
        raise TypeError("HistoricalFoundryStorageError is not copyable")

    def __reduce__(self) -> Any:
        raise TypeError("HistoricalFoundryStorageError is not serializable")

    def __reduce_ex__(self, _protocol: int) -> Any:
        raise TypeError("HistoricalFoundryStorageError is not serializable")


def _initialize_historical_foundry_storage_types():
    class _InternalFailure(Exception):
        __slots__ = ()

    constructor_provenance = object()
    test_lane = object()
    production_lane = object()

    transfer_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    pending_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    receipt_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    capability_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    consumed_view_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    binding_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    cursor_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    quota_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    active_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    sealed_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    active_tombstones: Dict[int, Tuple[weakref.ReferenceType, int]] = {}
    sealed_tombstones: Dict[int, Tuple[weakref.ReferenceType, int]] = {}
    capability_tombstones: Dict[
        int, Tuple[weakref.ReferenceType, int]
    ] = {}
    consumed_view_tombstones: Dict[
        int, Tuple[weakref.ReferenceType, int]
    ] = {}
    cursor_tombstones: Dict[
        int, Tuple[weakref.ReferenceType, int]
    ] = {}
    active_audits: Dict[
        int, Tuple[weakref.ReferenceType, int, Tuple[Any, ...]]
    ] = {}
    sealed_audits: Dict[
        int, Tuple[weakref.ReferenceType, int, Tuple[Any, ...]]
    ] = {}
    tombstone_generation = [0]

    transfer_projection_keys = (
        "exchange_index",
        "logical_batch_index",
        "attempt_index",
        "request_byte_count",
        "request_sha256",
        "request_ids",
        "wire_byte_count",
        "wire_sha256",
        "decoded_byte_count",
        "decoded_sha256",
        "response_ids",
    )
    projection_keys = (
        "state",
        "committed_physical_bytes",
        "committed_members",
        "provisional_physical_bytes",
        "provisional_members",
        "committed_receipt_count",
        "committed_eof",
        "receipt_inventory_sha256",
    )
    receipt_keys = (
        "schema",
        "exchange_index",
        "logical_batch_index",
        "attempt_index",
        "request_byte_count",
        "request_sha256",
        "request_ids",
        "wire_byte_count",
        "wire_sha256",
        "decoded_byte_count",
        "decoded_sha256",
        "response_ids",
        "spool_member_index",
        "spool_offset",
        "spool_length",
        "spool_member_sha256",
    )
    uint64_maximum = 18_446_744_073_709_551_615
    request_byte_limit = 4_194_304
    response_byte_limit = 8_388_608

    def _raise_storage_error() -> None:
        raise HistoricalFoundryStorageError() from None

    def _new_authority_base():
        authorized_class = [None]

        class _PrivateAuthorityBase:
            __slots__ = ()

            def __new__(cls, *args: Any, **kwargs: Any) -> Any:
                del args, kwargs
                _raise_storage_error()

            def __init_subclass__(cls, **_kwargs: Any) -> None:
                if authorized_class[0] is not None:
                    raise TypeError("historical foundry authority type is sealed")

            def __repr__(self) -> str:
                return "{}(<redacted>)".format(type(self).__name__)

            def __copy__(self) -> Any:
                raise TypeError("historical foundry authority is not copyable")

            def __deepcopy__(self, _memo: Any) -> Any:
                raise TypeError("historical foundry authority is not copyable")

            def __reduce__(self) -> Any:
                raise TypeError("historical foundry authority is not serializable")

            def __reduce_ex__(self, _protocol: int) -> Any:
                raise TypeError("historical foundry authority is not serializable")

        return _PrivateAuthorityBase, authorized_class

    transfer_base, transfer_authorized = _new_authority_base()
    pending_base, pending_authorized = _new_authority_base()
    receipt_base, receipt_authorized = _new_authority_base()
    capability_base, capability_authorized = _new_authority_base()
    consumed_view_base, consumed_view_authorized = _new_authority_base()
    binding_base, binding_authorized = _new_authority_base()
    cursor_base, cursor_authorized = _new_authority_base()
    quota_base, quota_authorized = _new_authority_base()
    active_base, active_authorized = _new_authority_base()
    sealed_base, sealed_authorized = _new_authority_base()

    def _prepare_handle(authority_class: type, record: Dict[str, Any]) -> Any:
        handle = object.__new__(authority_class)
        record["constructor"] = constructor_provenance
        return handle

    def _live_record(
        value: object,
        authority_class: type,
        registry: Dict[int, Tuple[object, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        if type(value) is not authority_class:
            _raise_storage_error()
        entry = registry.get(id(value))
        if (
            entry is None
            or entry[0] is not value
            or entry[1].get("constructor") is not constructor_provenance
        ):
            _raise_storage_error()
        return entry[1]

    def _metadata_snapshot(details: os.stat_result) -> Tuple[int, ...]:
        return (
            details.st_dev,
            details.st_ino,
            stat.S_IFMT(details.st_mode),
            details.st_uid,
            details.st_gid,
            stat.S_IMODE(details.st_mode),
        )

    def _file_identity(details: os.stat_result) -> Tuple[int, ...]:
        return _metadata_snapshot(details) + (details.st_nlink,)

    def _required_directory_flags() -> int:
        if not all(
            hasattr(os, name)
            for name in ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC")
        ):
            raise _InternalFailure()
        if (
            os.open not in os.supports_dir_fd
            or os.stat not in os.supports_dir_fd
            or os.stat not in os.supports_follow_symlinks
            or os.unlink not in os.supports_dir_fd
        ):
            raise _InternalFailure()
        return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC

    def _canonical_data_dir(data_dir: Path) -> Tuple[Path, Tuple[str, ...]]:
        if type(data_dir) is not type(Path(".")):
            raise _InternalFailure()
        if not data_dir.is_absolute() or ".." in data_dir.parts:
            raise _InternalFailure()
        supplied = str(data_dir)
        canonical = data_dir
        if sys.platform == "darwin":
            if supplied == "/tmp" or supplied.startswith("/tmp/"):
                canonical = Path("/private" + supplied)
            elif supplied == "/var" or supplied.startswith("/var/"):
                canonical = Path("/private" + supplied)
        parts = canonical.parts
        if not parts or parts[0] != os.sep:
            raise _InternalFailure()
        components = tuple(parts[1:])
        if len(components) > 64:
            raise _InternalFailure()
        try:
            encoded_path = os.fsencode(str(canonical))
            encoded_components = tuple(os.fsencode(item) for item in components)
        except Exception:
            raise _InternalFailure()
        if len(encoded_path) > 1024 or any(
            len(item) == 0 or len(item) > 255 for item in encoded_components
        ):
            raise _InternalFailure()
        return canonical, components

    def _open_into_slot(
        slot: Dict[str, Any], path: str, flags: int, **kwargs: Any
    ) -> None:
        slot["fd"] = os.open(path, flags, **kwargs)

    def _open_ancestry(
        canonical: Path,
        components: Tuple[str, ...],
        acquiring_fds: list,
    ) -> Tuple[Tuple[int, Optional[int], Optional[str], Tuple[int, ...]], ...]:
        flags = _required_directory_flags()
        opened = []
        root_slot = {"fd": None}
        acquiring_fds.append(root_slot)
        _open_into_slot(root_slot, os.sep, flags)
        root_fd = root_slot["fd"]
        opened.append((root_fd, None, None, None))
        root_details = os.fstat(root_fd)
        root_current = os.stat(os.sep, follow_symlinks=False)
        root_snapshot = _metadata_snapshot(root_details)
        if (
            not stat.S_ISDIR(root_details.st_mode)
            or _metadata_snapshot(root_current) != root_snapshot
        ):
            raise _InternalFailure()
        opened[-1] = (root_fd, None, None, root_snapshot)
        for component in components:
            parent_fd = opened[-1][0]
            child_slot = {"fd": None}
            acquiring_fds.append(child_slot)
            _open_into_slot(child_slot, component, flags, dir_fd=parent_fd)
            child_fd = child_slot["fd"]
            opened.append((child_fd, parent_fd, component, None))
            child_details = os.fstat(child_fd)
            entry_details = os.stat(
                component, dir_fd=parent_fd, follow_symlinks=False
            )
            snapshot = _metadata_snapshot(child_details)
            if (
                not stat.S_ISDIR(child_details.st_mode)
                or _metadata_snapshot(entry_details) != snapshot
            ):
                raise _InternalFailure()
            opened[-1] = (child_fd, parent_fd, component, snapshot)
        if str(canonical) != os.sep and opened[-1][2] is None:
            raise _InternalFailure()
        return tuple(opened)

    def _require_private_leaf(
        chain: Tuple[Tuple[int, Optional[int], Optional[str], Tuple[int, ...]], ...]
    ) -> None:
        details = os.fstat(chain[-1][0])
        if (
            not stat.S_ISDIR(details.st_mode)
            or details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) & 0o022
        ):
            raise _InternalFailure()

    def _verify_ancestry(
        chain: Tuple[Tuple[int, Optional[int], Optional[str], Tuple[int, ...]], ...]
    ) -> None:
        for index, (fd, parent_fd, component, snapshot) in enumerate(chain):
            opened = os.fstat(fd)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or _metadata_snapshot(opened) != snapshot
            ):
                raise _InternalFailure()
            if index == 0:
                current = os.stat(os.sep, follow_symlinks=False)
            else:
                if parent_fd is None or component is None:
                    raise _InternalFailure()
                current = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
            if (
                not stat.S_ISDIR(current.st_mode)
                or _metadata_snapshot(current) != snapshot
            ):
                raise _InternalFailure()
        _require_private_leaf(chain)

    def _resnapshot_leaf(
        chain: Tuple[Tuple[int, Optional[int], Optional[str], Tuple[int, ...]], ...]
    ) -> Tuple[Tuple[int, Optional[int], Optional[str], Tuple[int, ...]], ...]:
        _verify_ancestry(chain)
        leaf_fd, parent_fd, component, snapshot = chain[-1]
        opened = os.fstat(leaf_fd)
        if parent_fd is None:
            current = os.stat(os.sep, follow_symlinks=False)
        elif component is not None:
            current = os.stat(
                component, dir_fd=parent_fd, follow_symlinks=False
            )
        else:
            raise _InternalFailure()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or _metadata_snapshot(opened) != snapshot
            or _metadata_snapshot(current) != snapshot
        ):
            raise _InternalFailure()
        _require_private_leaf(chain)
        return chain

    def _require_relative_basename(value: str) -> None:
        if (
            type(value) is not str
            or not value
            or value in (".", "..")
            or os.path.basename(value) != value
            or os.sep in value
            or (os.altsep is not None and os.altsep in value)
        ):
            raise _InternalFailure()

    def _verify_file_entry(
        record: Dict[str, Any], *, expected_size: Optional[int]
    ) -> None:
        _verify_ancestry(record["chain"])
        file_fd = record["file_fd"]
        basename = record["basename"]
        leaf_fd = record["chain"][-1][0]
        if type(file_fd) is not int or type(basename) is not str:
            raise _InternalFailure()
        opened = os.fstat(file_fd)
        current = os.stat(basename, dir_fd=leaf_fd, follow_symlinks=False)
        identity = record["file_identity"]
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or _file_identity(opened) != identity
            or _file_identity(current) != identity
            or opened.st_nlink != 1
            or current.st_nlink != 1
        ):
            raise _InternalFailure()
        if expected_size is not None and (
            opened.st_size != expected_size or current.st_size != expected_size
        ):
            raise _InternalFailure()

    def _verify_created_entry_for_cleanup(record: Dict[str, Any]) -> None:
        _verify_ancestry(record["chain"])
        file_fd = record["file_fd"]
        basename = record["basename"]
        if type(file_fd) is not int or type(basename) is not str:
            raise _InternalFailure()
        opened = os.fstat(file_fd)
        current = os.stat(
            basename,
            dir_fd=record["chain"][-1][0],
            follow_symlinks=False,
        )
        opened_identity = _file_identity(opened)
        frozen_identity = record.get("file_identity")
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or opened_identity != _file_identity(current)
            or opened.st_nlink != 1
            or (
                frozen_identity is not None
                and opened_identity != frozen_identity
            )
        ):
            raise _InternalFailure()

    def _capture_failure(
        current_control: Optional[BaseException],
        current_ordinary: bool,
        error: BaseException,
    ) -> Tuple[Optional[BaseException], bool]:
        if isinstance(error, Exception):
            return current_control, True
        if current_control is None:
            return error, current_ordinary
        return current_control, current_ordinary

    def _cleanup_resources(
        record: Dict[str, Any], *, created: bool
    ) -> Tuple[Optional[BaseException], bool]:
        cleanup = record.get("_cleanup_state")
        if type(cleanup) is not dict:
            record["_cleanup_state"] = cleanup = {
                "phase": "verify",
                "created": bool(created),
                "verified": False,
                "unlink_attempted": False,
                "fsync_attempted": False,
                "attempted_fds": set(),
                "control": None,
                "ordinary": False,
            }

        while cleanup["phase"] != "done":
            phase = cleanup["phase"]
            if phase == "verify":
                if not cleanup["created"] or record.get("file_fd") is None:
                    cleanup["phase"] = "file_fd"
                    continue
                try:
                    _verify_created_entry_for_cleanup(record)
                except BaseException as error:
                    cleanup["control"], cleanup["ordinary"] = _capture_failure(
                        cleanup["control"], cleanup["ordinary"], error
                    )
                    if isinstance(error, Exception):
                        cleanup["phase"] = "file_fd"
                else:
                    cleanup["verified"] = True
                    cleanup["phase"] = "unlink"
                continue

            if phase == "unlink":
                if cleanup["verified"] and not cleanup["unlink_attempted"]:
                    try:
                        cleanup["unlink_attempted"] = True; os.unlink(record["basename"], dir_fd=record["chain"][-1][0])
                    except BaseException as error:
                        cleanup["control"], cleanup["ordinary"] = _capture_failure(
                            cleanup["control"], cleanup["ordinary"], error
                        )
                    if not cleanup["unlink_attempted"]:
                        continue
                cleanup["phase"] = "fsync"
                continue

            if phase == "fsync":
                if cleanup["unlink_attempted"] and not cleanup["fsync_attempted"]:
                    try:
                        cleanup["fsync_attempted"] = True; os.fsync(record["chain"][-1][0])
                    except BaseException as error:
                        cleanup["control"], cleanup["ordinary"] = _capture_failure(
                            cleanup["control"], cleanup["ordinary"], error
                        )
                    if not cleanup["fsync_attempted"]:
                        continue
                cleanup["phase"] = "file_fd"
                continue

            if phase == "file_fd":
                fd = record.get("file_fd")
                if type(fd) is int and fd not in cleanup["attempted_fds"]:
                    try:
                        cleanup["attempted_fds"].add(fd); record["file_fd"] = None; os.close(fd)
                    except BaseException as error:
                        cleanup["control"], cleanup["ordinary"] = _capture_failure(
                            cleanup["control"], cleanup["ordinary"], error
                        )
                    if fd not in cleanup["attempted_fds"]:
                        continue
                else:
                    record["file_fd"] = None
                cleanup["phase"] = "retiring_file_fd"
                continue

            if phase == "retiring_file_fd":
                fd = record.get("retiring_file_fd")
                if type(fd) is int and fd not in cleanup["attempted_fds"]:
                    try:
                        cleanup["attempted_fds"].add(fd); record["retiring_file_fd"] = None; os.close(fd)
                    except BaseException as error:
                        cleanup["control"], cleanup["ordinary"] = _capture_failure(
                            cleanup["control"], cleanup["ordinary"], error
                        )
                    if fd not in cleanup["attempted_fds"]:
                        continue
                else:
                    record["retiring_file_fd"] = None
                cleanup["phase"] = "chain"
                continue

            if phase == "chain":
                chain = record.get("chain", ())
                if chain:
                    fd = chain[-1][0]
                    if type(fd) is int and fd not in cleanup["attempted_fds"]:
                        try:
                            cleanup["attempted_fds"].add(fd); record["chain"] = chain[:-1]; os.close(fd)
                        except BaseException as error:
                            cleanup["control"], cleanup["ordinary"] = _capture_failure(
                                cleanup["control"], cleanup["ordinary"], error
                            )
                    else:
                        record["chain"] = chain[:-1]
                    continue
                cleanup["phase"] = "acquiring_fds"
                continue

            if phase == "acquiring_fds":
                acquiring_fds = record.get("acquiring_fds", ())
                if acquiring_fds:
                    slot = acquiring_fds[-1]
                    fd = slot.get("fd") if type(slot) is dict else None
                    if type(fd) is int and fd not in cleanup["attempted_fds"]:
                        try:
                            cleanup["attempted_fds"].add(fd); record["acquiring_fds"] = acquiring_fds[:-1]; slot["fd"] = None; os.close(fd)
                        except BaseException as error:
                            cleanup["control"], cleanup["ordinary"] = _capture_failure(
                                cleanup["control"], cleanup["ordinary"], error
                            )
                    else:
                        record["acquiring_fds"] = acquiring_fds[:-1]
                        if type(slot) is dict:
                            slot["fd"] = None
                    continue
                cleanup["phase"] = "finish"
                continue

            if phase == "finish":
                record["basename"] = None
                record["file_identity"] = None
                cleanup["phase"] = "done"
                continue
            raise _InternalFailure()
        return cleanup["control"], cleanup["ordinary"]

    def _prepare_tombstone(
        handle: object,
        tombstones: Dict[int, Tuple[weakref.ReferenceType, int]],
        audits: Dict[int, Tuple[weakref.ReferenceType, int, Tuple[Any, ...]]],
        audit: Optional[Tuple[Any, ...]],
        generation: int,
    ) -> Tuple[int, Tuple[weakref.ReferenceType, int], Dict[int, Any]]:
        handle_id = id(handle)

        def remove(reference: weakref.ReferenceType) -> None:
            current = tombstones.get(handle_id)
            if (
                current is not None
                and current[0] is reference
                and current[1] == generation
            ):
                tombstones.pop(handle_id, None)
            current_audit = audits.get(handle_id)
            if (
                current_audit is not None
                and current_audit[0] is reference
                and current_audit[1] == generation
            ):
                audits.pop(handle_id, None)

        reference = weakref.ref(handle, remove)
        audit_update = {}
        if audit is not None:
            audit_update[handle_id] = (reference, generation, audit)
        return handle_id, (reference, generation), audit_update

    def _is_exact_tombstone(
        handle: object,
        authority_class: type,
        tombstones: Dict[int, Tuple[weakref.ReferenceType, int]],
    ) -> bool:
        if type(handle) is not authority_class:
            return False
        entry = tombstones.get(id(handle))
        return entry is not None and entry[0]() is handle

    def _retire_nonowner_handle(
        handle: object,
        registry: Dict[int, Tuple[object, Dict[str, Any]]],
        tombstones: Dict[int, Tuple[weakref.ReferenceType, int]],
    ) -> None:
        handle_id = id(handle)
        tombstone_generation[0] += 1
        generation = tombstone_generation[0]

        def remove(reference: weakref.ReferenceType) -> None:
            current = tombstones.get(handle_id)
            if (
                current is not None
                and current[0] is reference
                and current[1] == generation
            ):
                tombstones.pop(handle_id, None)

        reference = weakref.ref(handle, remove)
        tombstones[handle_id] = (reference, generation); registry.pop(handle_id, None)

    def _retire_lineage(
        record: Dict[str, Any],
        preserve_handle: Optional[object] = None,
        preserve_registry: Optional[
            Dict[int, Tuple[object, Dict[str, Any]]]
        ] = None,
    ) -> None:
        lineage = record.get("lineage")
        for registry in (
            transfer_registry,
            pending_registry,
            receipt_registry,
            capability_registry,
            consumed_view_registry,
            binding_registry,
            cursor_registry,
            quota_registry,
        ):
            for handle_id, (_handle, candidate) in tuple(registry.items()):
                if candidate.get("lineage") is lineage:
                    if (
                        registry is preserve_registry
                        and _handle is preserve_handle
                        and candidate is record
                    ):
                        continue
                    if registry is transfer_registry:
                        candidate["canonical_request_bytes"] = None
                        candidate["decoded_response_bytes"] = None
                    if registry is cursor_registry:
                        candidate["state"] = "closed"
                        _retire_nonowner_handle(
                            _handle, cursor_registry, cursor_tombstones
                        )
                        continue
                    registry.pop(handle_id, None)

    def _closed_audit(
        record: Dict[str, Any], quota: Dict[str, Any]
    ) -> Tuple[Any, ...]:
        return (
            "closed",
            int(quota["committed_physical_bytes"]),
            int(quota["committed_members"]),
            int(quota["provisional_physical_bytes"]),
            int(quota["provisional_members"]),
            int(len(record["inventory"])),
            int(record["committed_eof"]),
            record["receipt_inventory_sha256"],
        )

    def _terminal_audit(record: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
        if record.get("lane") is production_lane and record.get("source_bound"):
            quota_entry = quota_registry.get(id(record.get("quota")))
            if quota_entry is None or quota_entry[0] is not record.get("quota"):
                return None
            return _closed_audit(record, quota_entry[1])
        if record.get("lane") is not test_lane or record.get("source_bound"):
            return None
        if record.get("state") in (
            "appending",
            "aborting",
            "committing",
            "quota_transition",
            "sealing",
            "closing",
        ):
            frozen = record.get("terminal_audit")
            if type(frozen) is not tuple or len(frozen) != len(projection_keys):
                return None
            return frozen
        quota_entry = quota_registry.get(id(record.get("quota")))
        if quota_entry is None or quota_entry[0] is not record.get("quota"):
            return None
        return _closed_audit(record, quota_entry[1])

    def _terminalize_active(
        handle: object, record: Dict[str, Any]
    ) -> Tuple[Optional[BaseException], bool]:
        terminal = record.get("_terminal_state")
        while True:
            try:
                if type(terminal) is not dict:
                    audit = _terminal_audit(record)
                    tombstone_generation[0] += 1; record["_terminal_state"] = terminal = {"phase": "prepare", "audit": audit, "generation": tombstone_generation[0], "prepared": None, "control": None, "ordinary": False}
                    record["terminal_audit"] = audit
                    record["state"] = "closing"
                phase = terminal["phase"]
                if phase == "prepare":
                    terminal["prepared"] = _prepare_tombstone(
                        handle,
                        active_tombstones,
                        active_audits,
                        terminal["audit"],
                        terminal["generation"],
                    )
                    terminal["phase"] = "publish"
                    continue
                if phase == "publish":
                    handle_id, tombstone, audit_update = terminal["prepared"]
                    active_tombstones[handle_id] = tombstone; active_audits.update(audit_update)
                    terminal["phase"] = "revoke"
                    continue
                if phase == "revoke":
                    _revoke_bound_source(record)
                    terminal["phase"] = "retire"
                    continue
                if phase == "retire":
                    _retire_lineage(record)
                    terminal["phase"] = "cleanup"
                    continue
                if phase == "cleanup":
                    cleanup_control, cleanup_ordinary = _cleanup_resources(
                        record, created=True
                    )
                    if terminal["control"] is None:
                        terminal["control"] = cleanup_control
                    terminal["ordinary"] = (
                        terminal["ordinary"] or cleanup_ordinary
                    )
                    terminal["phase"] = "release"
                    continue
                if phase == "release":
                    handle_id, tombstone, audit_update = terminal["prepared"]
                    active_registry.pop(id(handle), None); active_tombstones[handle_id] = tombstone; active_audits.update(audit_update)
                    terminal["phase"] = "done"
                    continue
                if phase == "done":
                    return terminal["control"], terminal["ordinary"]
                raise _InternalFailure()
            except BaseException as error:
                if type(terminal) is not dict:
                    raise
                terminal["control"], terminal["ordinary"] = _capture_failure(
                    terminal["control"], terminal["ordinary"], error
                )

    def _exact_positive_uint64(value: Any) -> bool:
        return (
            type(value) is int
            and value > 0
            and value.bit_length() <= 64
            and value <= uint64_maximum
        )

    def _exact_positive_count(value: Any, maximum: int) -> bool:
        return type(value) is int and value > 0 and value <= maximum

    def _exact_sha256(value: Any) -> bool:
        return (
            type(value) is str
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        )

    def _validate_exchange_projection(
        exchange_projection: Mapping[str, Any],
        canonical_request_bytes: bytes,
        decoded_response_bytes: bytes,
    ) -> Dict[str, Any]:
        if type(exchange_projection) is not dict:
            _raise_storage_error()
        if len(exchange_projection) != len(transfer_projection_keys):
            _raise_storage_error()
        exact_keys = []
        for key in exchange_projection:
            if type(key) is not str:
                _raise_storage_error()
            exact_keys.append(key)
        if frozenset(exact_keys) != frozenset(transfer_projection_keys):
            _raise_storage_error()
        if (
            type(canonical_request_bytes) is not bytes
            or type(decoded_response_bytes) is not bytes
            or not canonical_request_bytes
            or not decoded_response_bytes
            or len(canonical_request_bytes) > request_byte_limit
            or len(decoded_response_bytes) > response_byte_limit
        ):
            _raise_storage_error()

        for key in ("exchange_index", "logical_batch_index", "attempt_index"):
            if not _exact_positive_uint64(exchange_projection[key]):
                _raise_storage_error()
        if not _exact_positive_count(
            exchange_projection["request_byte_count"], request_byte_limit
        ):
            _raise_storage_error()
        if not _exact_positive_count(
            exchange_projection["wire_byte_count"], response_byte_limit
        ):
            _raise_storage_error()
        if not _exact_positive_count(
            exchange_projection["decoded_byte_count"], response_byte_limit
        ):
            _raise_storage_error()
        for key in ("request_sha256", "wire_sha256", "decoded_sha256"):
            if not _exact_sha256(exchange_projection[key]):
                _raise_storage_error()

        request_ids = exchange_projection["request_ids"]
        response_ids = exchange_projection["response_ids"]
        if (
            type(request_ids) is not tuple
            or type(response_ids) is not tuple
            or not 1 <= len(request_ids) <= 40
            or not 1 <= len(response_ids) <= 40
        ):
            _raise_storage_error()
        for identifier in request_ids:
            if not _exact_positive_uint64(identifier):
                _raise_storage_error()
        for identifier in response_ids:
            if not _exact_positive_uint64(identifier):
                _raise_storage_error()
        if (
            len(set(request_ids)) != len(request_ids)
            or len(set(response_ids)) != len(response_ids)
            or set(request_ids) != set(response_ids)
        ):
            _raise_storage_error()

        if (
            exchange_projection["request_byte_count"]
            != len(canonical_request_bytes)
            or exchange_projection["decoded_byte_count"]
            != len(decoded_response_bytes)
            or exchange_projection["request_sha256"]
            != hashlib.sha256(canonical_request_bytes).hexdigest()
            or exchange_projection["decoded_sha256"]
            != hashlib.sha256(decoded_response_bytes).hexdigest()
        ):
            _raise_storage_error()
        return {
            key: (
                tuple(exchange_projection[key])
                if key in ("request_ids", "response_ids")
                else exchange_projection[key]
            )
            for key in transfer_projection_keys
        }

    def _quota_record_for_owner(record: Dict[str, Any]) -> Dict[str, Any]:
        quota = record.get("quota")
        entry = quota_registry.get(id(quota))
        if (
            type(quota) is not _HistoricalWindowRunQuota
            or entry is None
            or entry[0] is not quota
            or entry[1].get("lineage") is not record.get("lineage")
        ):
            _raise_storage_error()
        return entry[1]

    def _projection_for_record(
        record: Dict[str, Any], state_value: str
    ) -> Mapping[str, Any]:
        quota = _quota_record_for_owner(record)
        values = (
            state_value,
            quota["committed_physical_bytes"],
            quota["committed_members"],
            quota["provisional_physical_bytes"],
            quota["provisional_members"],
            len(record["inventory"]),
            record["committed_eof"],
            record["receipt_inventory_sha256"],
        )
        return MappingProxyType(dict(zip(projection_keys, values)))

    def _projection_from_audit(
        value: object,
        authority_class: type,
        tombstones: Dict[int, Tuple[weakref.ReferenceType, int]],
        audits: Dict[int, Tuple[weakref.ReferenceType, int, Tuple[Any, ...]]],
    ) -> Mapping[str, Any]:
        if type(value) is not authority_class:
            _raise_storage_error()
        tombstone = tombstones.get(id(value))
        audit = audits.get(id(value))
        if (
            tombstone is None
            or audit is None
            or tombstone[0]() is not value
            or audit[0]() is not value
            or audit[0] is not tombstone[0]
            or audit[1] != tombstone[1]
        ):
            _raise_storage_error()
        return MappingProxyType(dict(zip(projection_keys, audit[2])))

    def _active_owner_for_quota(
        quota_handle: object, quota_record: Dict[str, Any]
    ) -> Tuple[object, Dict[str, Any]]:
        matches = []
        for owner_handle, owner_record in active_registry.values():
            if (
                owner_record.get("lineage") is quota_record.get("lineage")
                and owner_record.get("quota") is quota_handle
            ):
                matches.append((owner_handle, owner_record))
        if len(matches) != 1:
            _raise_storage_error()
        return matches[0]

    def _terminal_quota_failure(
        owner_handle: object,
        owner_record: Dict[str, Any],
        original_control: Optional[BaseException] = None,
    ) -> None:
        current = active_registry.get(id(owner_handle))
        if current is not None and current[0] is owner_handle:
            owner_record = current[1]
        cleanup_control, _cleanup_ordinary = _terminalize_active(
            owner_handle, owner_record
        )
        if original_control is not None:
            raise original_control
        if cleanup_control is not None:
            raise cleanup_control
        _raise_storage_error()

    def _normal_active_record(
        spool: object,
    ) -> Dict[str, Any]:
        record = _live_record(
            spool, _HistoricalWindowExchangeSpool, active_registry
        )
        if record["state"] != "active" or record["mode"] != "normal":
            _raise_storage_error()
        return record

    def _pwrite_all(fd: int, value: bytes, offset: int) -> None:
        written = 0
        while written < len(value):
            count = os.pwrite(fd, value[written:], offset + written)
            if type(count) is not int or count <= 0 or count > len(value) - written:
                raise _InternalFailure()
            written += count

    def _pread_exact(fd: int, length: int, offset: int) -> bytes:
        if type(length) is not int or length < 0 or type(offset) is not int or offset < 0:
            raise _InternalFailure()
        chunks = []
        remaining = length
        current = offset
        while remaining:
            chunk = os.pread(fd, remaining, current)
            if type(chunk) is not bytes or not chunk or len(chunk) > remaining:
                raise _InternalFailure()
            chunks.append(chunk)
            remaining -= len(chunk)
            current += len(chunk)
        return b"".join(chunks)

    def _frame_bytes(transfer_record: Dict[str, Any]) -> bytes:
        request = transfer_record["canonical_request_bytes"]
        decoded = transfer_record["decoded_response_bytes"]
        if type(request) is not bytes or type(decoded) is not bytes:
            raise _InternalFailure()
        return (
            len(request).to_bytes(8, "big")
            + request
            + len(decoded).to_bytes(8, "big")
            + decoded
        )

    def _verify_frame(
        owner_record: Dict[str, Any],
        projection: Dict[str, Any],
        *,
        spool_offset: int,
        spool_length: int,
        spool_member_sha256: str,
    ) -> Tuple[bytes, bytes]:
        if (
            type(spool_offset) is not int
            or spool_offset < 0
            or type(spool_length) is not int
            or spool_length < 16
            or spool_length > request_byte_limit + response_byte_limit + 16
            or not _exact_sha256(spool_member_sha256)
        ):
            raise _InternalFailure()
        fd = owner_record["file_fd"]
        request_prefix = _pread_exact(fd, 8, spool_offset)
        request_length = int.from_bytes(request_prefix, "big")
        if request_length != projection["request_byte_count"]:
            raise _InternalFailure()
        request = _pread_exact(fd, request_length, spool_offset + 8)
        decoded_prefix_offset = spool_offset + 8 + request_length
        decoded_prefix = _pread_exact(fd, 8, decoded_prefix_offset)
        decoded_length = int.from_bytes(decoded_prefix, "big")
        if decoded_length != projection["decoded_byte_count"]:
            raise _InternalFailure()
        expected_length = 8 + request_length + 8 + decoded_length
        if expected_length != spool_length:
            raise _InternalFailure()
        decoded = _pread_exact(fd, decoded_length, decoded_prefix_offset + 8)
        if (
            hashlib.sha256(request).hexdigest() != projection["request_sha256"]
            or hashlib.sha256(decoded).hexdigest() != projection["decoded_sha256"]
        ):
            raise _InternalFailure()
        frame = request_prefix + request + decoded_prefix + decoded
        if hashlib.sha256(frame).hexdigest() != spool_member_sha256:
            raise _InternalFailure()
        return request, decoded

    def _transfer_for_spool(
        owner_record: Dict[str, Any],
        transfer: object,
        required_state: str,
    ) -> Dict[str, Any]:
        transfer_record = _live_record(
            transfer, _ProductionArchiveRpcExchangeTransfer, transfer_registry
        )
        if (
            transfer_record["lineage"] is not owner_record["lineage"]
            or transfer_record["lane"] is not owner_record["lane"]
            or transfer_record["state"] != required_state
            or owner_record["live_transfer"] is not transfer
        ):
            _raise_storage_error()
        return transfer_record

    def _pending_for_spool(
        owner_record: Dict[str, Any],
        transfer_record: Dict[str, Any],
        pending_receipt: object,
        required_state: str = "pending",
    ) -> Dict[str, Any]:
        pending_record = _live_record(
            pending_receipt,
            _PendingHistoricalWindowSpoolReceipt,
            pending_registry,
        )
        if (
            pending_record["lineage"] is not owner_record["lineage"]
            or pending_record["lane"] is not owner_record["lane"]
            or pending_record["exchange"] is not transfer_record["exchange"]
            or pending_record["state"] != required_state
            or owner_record["pending"] is not pending_receipt
        ):
            _raise_storage_error()
        return pending_record

    def _receipt_for_spool(
        owner_record: Dict[str, Any], receipt: object
    ) -> Dict[str, Any]:
        receipt_record = _live_record(
            receipt, _HistoricalWindowSpoolReceipt, receipt_registry
        )
        projection = receipt_record["projection"]
        position = projection["spool_member_index"] - 1
        if (
            receipt_record["lineage"] is not owner_record["lineage"]
            or receipt_record["lane"] is not owner_record["lane"]
            or receipt_record["state"] not in (
                "committed", "committed_unverified", "committed_verified"
            )
            or position < 0
            or position >= len(owner_record["inventory"])
            or owner_record["inventory"][position] is not receipt
        ):
            _raise_storage_error()
        return receipt_record

    def _terminalize_operation_failure(
        owner_handle: object,
        owner_record: Dict[str, Any],
        original_control: Optional[BaseException],
    ) -> None:
        current = active_registry.get(id(owner_handle))
        if current is not None and current[0] is owner_handle:
            owner_record = current[1]
        cleanup_control, _cleanup_ordinary = _terminalize_active(
            owner_handle, owner_record
        )
        if original_control is not None:
            raise original_control
        if cleanup_control is not None:
            raise cleanup_control
        _raise_storage_error()

    def _install_test_commit_transition(
        spool: object,
        committing_owner: Dict[str, Any],
        receipt: object,
        receipt_record: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
        transfer: object,
        transfer_record: Dict[str, Any],
        pending_receipt: object,
        pending_record: Dict[str, Any],
    ) -> None:
        active_registry[id(spool)] = (spool, committing_owner)
        receipt_registry[id(receipt)] = (receipt, receipt_record)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        committing_owner["inventory"].append(receipt)
        active_registry[id(spool)] = (spool, final_owner)
        transfer_record["canonical_request_bytes"] = None
        transfer_record["decoded_response_bytes"] = None
        transfer_record["state"] = "consumed"
        pending_record["state"] = "consumed"
        transfer_registry.pop(id(transfer), None)
        pending_registry.pop(id(pending_receipt), None)

    def _install_production_commit_transition(
        spool: object,
        committing_owner: Dict[str, Any],
        receipt: object,
        receipt_record: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
        transfer: object,
        next_transfer: Dict[str, Any],
        pending_receipt: object,
        next_pending: Dict[str, Any],
    ) -> None:
        active_registry[id(spool)] = (spool, committing_owner)
        receipt_registry[id(receipt)] = (receipt, receipt_record)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        transfer_registry[id(transfer)] = (transfer, next_transfer)
        pending_registry[id(pending_receipt)] = (
            pending_receipt, next_pending
        )
        committing_owner["inventory"].append(receipt)
        active_registry[id(spool)] = (spool, final_owner)

    def _rollback_append(
        owner_record: Dict[str, Any], quota_record: Dict[str, Any], saved_eof: int
    ) -> Tuple[bool, Optional[BaseException]]:
        control = None
        succeeded = False
        try:
            os.ftruncate(owner_record["file_fd"], saved_eof)
            os.fsync(owner_record["file_fd"])
            _verify_file_entry(owner_record, expected_size=saved_eof)
            succeeded = True
        except BaseException as error:
            if not isinstance(error, Exception):
                control = error
        if succeeded:
            quota_record["provisional_physical_bytes"] = 0
            quota_record["provisional_members"] = 0
            quota_record["reservation"] = None
        return succeeded, control

    def _install_append_quota_transition(
        spool: object,
        prior_owner: Dict[str, Any],
        appending_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        reserved_owner: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(spool))
        if (
            current is None
            or current[0] is not spool
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(spool)] = (spool, appending_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        active_registry[id(spool)] = (spool, reserved_owner)

    def _install_append_transition(
        spool: object,
        prior_owner: Dict[str, Any],
        appending_owner: Dict[str, Any],
        pending: object,
        pending_record: Dict[str, Any],
        transfer: object,
        next_transfer: Dict[str, Any],
        final_owner: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(spool))
        if (
            current is None
            or current[0] is not spool
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(spool)] = (spool, appending_owner)
        pending_registry[id(pending)] = (pending, pending_record)
        transfer_registry[id(transfer)] = (transfer, next_transfer)
        active_registry[id(spool)] = (spool, final_owner)

    def _append_transfer(
        spool: object, transfer: object
    ) -> object:
        owner_record = _normal_active_record(spool)
        transfer_record = _transfer_for_spool(owner_record, transfer, "issued")
        if (
            owner_record["pending"] is not None
            or transfer_record["exchange_index"] != owner_record["next_exchange_index"]
        ):
            _raise_storage_error()
        currentness_failed = False
        currentness_control = None
        try:
            _verify_file_entry(
                owner_record, expected_size=owner_record["committed_eof"]
            )
        except BaseException as error:
            if isinstance(error, Exception):
                currentness_failed = True
            else:
                currentness_control = error
        if currentness_failed or currentness_control is not None:
            _terminalize_operation_failure(
                spool, owner_record, currentness_control
            )

        frame = _frame_bytes(transfer_record)
        frame_length = len(frame)
        quota_record = _quota_record_for_owner(owner_record)
        if quota_record["reservation"] is not None:
            _raise_storage_error()
        remaining_bytes = (
            8_589_934_592
            - quota_record["committed_physical_bytes"]
            - quota_record["provisional_physical_bytes"]
        )
        remaining_members = (
            200_000
            - quota_record["committed_members"]
            - quota_record["provisional_members"]
        )
        if frame_length > remaining_bytes or 1 > remaining_members:
            _terminalize_operation_failure(spool, owner_record, None)
        reservation = {
            "kind": "append",
            "token": object(),
            "physical_bytes": frame_length,
            "members": 1,
        }
        preappend_audit = _terminal_audit(owner_record)
        if preappend_audit is None:
            _terminalize_operation_failure(spool, owner_record, None)
        appending_owner = dict(owner_record)
        appending_owner["state"] = "appending"
        appending_owner["terminal_audit"] = preappend_audit
        next_quota = dict(quota_record)
        next_quota["reservation"] = reservation
        next_quota["provisional_physical_bytes"] = frame_length
        next_quota["provisional_members"] = 1
        reserved_owner = dict(owner_record)
        saved_eof = owner_record["committed_eof"]
        transition_failed = False
        transition_control = None
        try:
            _install_append_quota_transition(
                spool,
                owner_record,
                appending_owner,
                owner_record["quota"],
                next_quota,
                reserved_owner,
            )
        except BaseException as error:
            if isinstance(error, Exception):
                transition_failed = True
            else:
                transition_control = error
        if transition_failed or transition_control is not None:
            current = active_registry.get(id(spool))
            rollback_owner = owner_record
            if current is not None and current[0] is spool:
                rollback_owner = current[1]
            rollback_quota = _quota_record_for_owner(rollback_owner)
            rollback_succeeded, rollback_control = _rollback_append(
                rollback_owner, rollback_quota, saved_eof
            )
            if (
                rollback_succeeded
                and rollback_owner.get("state") == "appending"
            ):
                rollback_owner["terminal_audit"] = _closed_audit(
                    rollback_owner, rollback_quota
                )
            if transition_control is None:
                transition_control = rollback_control
            _terminalize_operation_failure(
                spool, rollback_owner, transition_control
            )
        owner_record = reserved_owner
        quota_record = next_quota
        candidate_end = saved_eof + frame_length
        operation_failed = False
        operation_control = None
        pending = None
        next_transfer = None
        try:
            _pwrite_all(owner_record["file_fd"], frame, saved_eof)
            os.fsync(owner_record["file_fd"])
            member_hash = hashlib.sha256(frame).hexdigest()
            _verify_frame(
                owner_record,
                transfer_record["projection"],
                spool_offset=saved_eof,
                spool_length=frame_length,
                spool_member_sha256=member_hash,
            )
            if os.pread(owner_record["file_fd"], 1, candidate_end) != b"":
                raise _InternalFailure()
            _verify_file_entry(owner_record, expected_size=candidate_end)
            source = transfer_record["projection"]
            receipt_projection = {
                "schema": "historical_foundry_exchange_spool_receipt/v1",
                "exchange_index": source["exchange_index"],
                "logical_batch_index": source["logical_batch_index"],
                "attempt_index": source["attempt_index"],
                "request_byte_count": source["request_byte_count"],
                "request_sha256": source["request_sha256"],
                "request_ids": source["request_ids"],
                "wire_byte_count": source["wire_byte_count"],
                "wire_sha256": source["wire_sha256"],
                "decoded_byte_count": source["decoded_byte_count"],
                "decoded_sha256": source["decoded_sha256"],
                "response_ids": source["response_ids"],
                "spool_member_index": owner_record["next_member_index"],
                "spool_offset": saved_eof,
                "spool_length": frame_length,
                "spool_member_sha256": member_hash,
            }
            pending_record = {
                "lineage": owner_record["lineage"],
                "lane": transfer_record["lane"],
                "exchange": transfer_record["exchange"],
                "exchange_index": transfer_record["exchange_index"],
                "state": "pending",
                "projection": receipt_projection,
            }
            pending = _prepare_handle(
                _PendingHistoricalWindowSpoolReceipt, pending_record
            )
            pending_audit = _terminal_audit(owner_record)
            if pending_audit is None:
                raise _InternalFailure()
            pending_transition = dict(owner_record)
            pending_transition["state"] = "appending"
            pending_transition["terminal_audit"] = pending_audit
            next_transfer = dict(transfer_record)
            next_transfer["state"] = "pending"
            next_transfer["pending_receipt"] = pending
            final_owner = dict(owner_record)
            final_owner["pending"] = pending
            _install_append_transition(
                spool,
                owner_record,
                pending_transition,
                pending,
                pending_record,
                transfer,
                next_transfer,
                final_owner,
            )
            transfer_record["canonical_request_bytes"] = None
            transfer_record["decoded_response_bytes"] = None
            result = pending
            return result
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            transfer_record["canonical_request_bytes"] = None
            transfer_record["decoded_response_bytes"] = None
            if next_transfer is not None:
                next_transfer["canonical_request_bytes"] = None
                next_transfer["decoded_response_bytes"] = None
            _rollback_succeeded, rollback_control = _rollback_append(
                owner_record, quota_record, saved_eof
            )
            if _rollback_succeeded:
                current = active_registry.get(id(spool))
                if (
                    current is not None
                    and current[0] is spool
                    and current[1].get("state") == "appending"
                ):
                    current[1]["terminal_audit"] = _closed_audit(
                        current[1], quota_record
                    )
            if operation_control is None:
                operation_control = rollback_control
            _terminalize_operation_failure(
                spool, owner_record, operation_control
            )
        raise _InternalFailure()

    def _commit_transfer(
        spool: object, transfer: object, pending_receipt: object
    ) -> object:
        owner_record = _normal_active_record(spool)
        production = owner_record["lane"] is production_lane
        transfer_record = _transfer_for_spool(
            owner_record,
            transfer,
            "pending_verified" if production else "pending",
        )
        pending_record = _pending_for_spool(
            owner_record,
            transfer_record,
            pending_receipt,
            "pending_verified" if production else "pending",
        )
        projection = pending_record["projection"]
        operation_failed = False
        operation_control = None
        receipt = None
        try:
            expected_end = projection["spool_offset"] + projection["spool_length"]
            _verify_file_entry(owner_record, expected_size=expected_end)
            _verify_frame(
                owner_record,
                projection,
                spool_offset=projection["spool_offset"],
                spool_length=projection["spool_length"],
                spool_member_sha256=projection["spool_member_sha256"],
            )
            if os.pread(owner_record["file_fd"], 1, expected_end) != b"":
                raise _InternalFailure()
            _verify_file_entry(owner_record, expected_size=expected_end)
            quota_record = _quota_record_for_owner(owner_record)
            reservation = quota_record["reservation"]
            if (
                type(reservation) is not dict
                or reservation.get("kind") != "append"
                or reservation["physical_bytes"] != projection["spool_length"]
                or reservation["members"] != 1
            ):
                raise _InternalFailure()
            receipt = object.__new__(_HistoricalWindowSpoolReceipt)
            receipt_record = {
                "constructor": constructor_provenance,
                "lineage": owner_record["lineage"],
                "lane": transfer_record["lane"],
                "exchange": transfer_record["exchange"],
                "exchange_index": transfer_record["exchange_index"],
                "state": (
                    "committed_unverified" if production else "committed"
                ),
                "projection": dict(projection),
            }
            precommit_audit = _terminal_audit(owner_record)
            if precommit_audit is None:
                raise _InternalFailure()
            committing_owner = dict(owner_record)
            committing_owner["state"] = "committing"
            committing_owner["terminal_audit"] = precommit_audit
            next_quota = dict(quota_record)
            next_quota.update(
                {
                    "committed_physical_bytes": quota_record[
                        "committed_physical_bytes"
                    ]
                    + reservation["physical_bytes"],
                    "committed_members": quota_record["committed_members"] + 1,
                    "provisional_physical_bytes": 0,
                    "provisional_members": 0,
                    "reservation": None,
                }
            )
            final_owner = dict(owner_record)
            final_owner.update(
                {
                    "committed_eof": expected_end,
                    "next_exchange_index": owner_record[
                        "next_exchange_index"
                    ]
                    + 1,
                    "next_member_index": owner_record["next_member_index"] + 1,
                    "pending": None,
                    "live_transfer": None,
                }
            )
            if production:
                final_owner["live_transfer"] = transfer
                next_transfer = dict(transfer_record)
                next_transfer["state"] = "committed_unverified"
                next_transfer["receipt"] = receipt
                next_pending = dict(pending_record)
                next_pending["state"] = "committed_unverified"
                _install_production_commit_transition(
                    spool,
                    committing_owner,
                    receipt,
                    receipt_record,
                    owner_record["quota"],
                    next_quota,
                    final_owner,
                    transfer,
                    next_transfer,
                    pending_receipt,
                    next_pending,
                )
            else:
                _install_test_commit_transition(
                    spool,
                    committing_owner,
                    receipt,
                    receipt_record,
                    owner_record["quota"],
                    next_quota,
                    final_owner,
                    transfer,
                    transfer_record,
                    pending_receipt,
                    pending_record,
                )
            return receipt
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            _terminalize_operation_failure(
                spool, owner_record, operation_control
            )
        raise _InternalFailure()

    def _install_abort_transition(
        spool: object,
        prior_owner: Dict[str, Any],
        aborting_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
        transfer: object,
        transfer_record: Dict[str, Any],
        pending_receipt: object,
        pending_record: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(spool))
        if (
            current is None
            or current[0] is not spool
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(spool)] = (spool, aborting_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        active_registry[id(spool)] = (spool, final_owner)
        transfer_record["canonical_request_bytes"] = None
        transfer_record["decoded_response_bytes"] = None
        transfer_record["state"] = "consumed"
        pending_record["state"] = "consumed"
        transfer_registry.pop(id(transfer), None)
        pending_registry.pop(id(pending_receipt), None)

    def _abort_transfer(
        spool: object, transfer: object, pending_receipt: object
    ) -> None:
        owner_record = _normal_active_record(spool)
        transfer_record = _transfer_for_spool(owner_record, transfer, "pending")
        pending_record = _pending_for_spool(
            owner_record, transfer_record, pending_receipt
        )
        projection = pending_record["projection"]
        if projection["spool_offset"] != owner_record["committed_eof"]:
            _raise_storage_error()
        operation_failed = False
        operation_control = None
        try:
            expected_end = projection["spool_offset"] + projection["spool_length"]
            _verify_file_entry(owner_record, expected_size=expected_end)
            os.ftruncate(owner_record["file_fd"], projection["spool_offset"])
            os.fsync(owner_record["file_fd"])
            _verify_file_entry(
                owner_record, expected_size=projection["spool_offset"]
            )
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            _terminalize_operation_failure(
                spool, owner_record, operation_control
            )
        quota_record = _quota_record_for_owner(owner_record)
        reservation = quota_record["reservation"]
        if (
            type(reservation) is not dict
            or reservation.get("kind") != "append"
            or reservation["physical_bytes"] != projection["spool_length"]
            or reservation["members"] != 1
        ):
            _terminalize_operation_failure(spool, owner_record, None)
        preabort_audit = _terminal_audit(owner_record)
        if preabort_audit is None:
            _terminalize_operation_failure(spool, owner_record, None)
        aborting_owner = dict(owner_record)
        aborting_owner["state"] = "aborting"
        aborting_owner["terminal_audit"] = preabort_audit
        next_quota = dict(quota_record)
        next_quota["provisional_physical_bytes"] = 0
        next_quota["provisional_members"] = 0
        next_quota["reservation"] = None
        final_owner = dict(owner_record)
        final_owner["pending"] = None
        final_owner["live_transfer"] = None
        transition_failed = False
        transition_control = None
        try:
            _install_abort_transition(
                spool,
                owner_record,
                aborting_owner,
                owner_record["quota"],
                next_quota,
                final_owner,
                transfer,
                transfer_record,
                pending_receipt,
                pending_record,
            )
            result = None
            return result
        except BaseException as error:
            if isinstance(error, Exception):
                transition_failed = True
            else:
                transition_control = error
        if transition_failed or transition_control is not None:
            transfer_record["canonical_request_bytes"] = None
            transfer_record["decoded_response_bytes"] = None
            _terminalize_operation_failure(
                spool, owner_record, transition_control
            )
        raise _InternalFailure()

    def _reread_exchange(
        spool: object, receipt: object
    ) -> Tuple[bytes, bytes]:
        owner_record = _normal_active_record(spool)
        receipt_record = _receipt_for_spool(owner_record, receipt)
        projection = receipt_record["projection"]
        operation_failed = False
        operation_control = None
        result = None
        try:
            _verify_file_entry(
                owner_record, expected_size=owner_record["committed_eof"]
            )
            result = _verify_frame(
                owner_record,
                projection,
                spool_offset=projection["spool_offset"],
                spool_length=projection["spool_length"],
                spool_member_sha256=projection["spool_member_sha256"],
            )
            _verify_file_entry(
                owner_record, expected_size=owner_record["committed_eof"]
            )
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            _terminalize_operation_failure(
                spool, owner_record, operation_control
            )
        return result

    def _terminalize_sealed(
        sealed_handle: object, sealed_record: Dict[str, Any]
    ) -> Tuple[Optional[BaseException], bool]:
        terminal = sealed_record.get("_terminal_state")
        while True:
            try:
                if type(terminal) is not dict:
                    audit = _terminal_audit(sealed_record)
                    moved_active = sealed_record.get("moved_active")
                    tombstone_generation[0] += 1; sealed_record["_terminal_state"] = terminal = {"phase": "prepare", "audit": audit, "generation": tombstone_generation[0], "sealed_prepared": None, "active_prepared": None, "moved_active": moved_active, "control": None, "ordinary": False}
                    sealed_record["terminal_audit"] = audit
                    sealed_record["state"] = "closing"
                phase = terminal["phase"]
                if phase == "prepare":
                    terminal["sealed_prepared"] = _prepare_tombstone(
                        sealed_handle,
                        sealed_tombstones,
                        sealed_audits,
                        terminal["audit"],
                        terminal["generation"],
                    )
                    terminal["active_prepared"] = _prepare_tombstone(
                        terminal["moved_active"],
                        active_tombstones,
                        active_audits,
                        terminal["audit"],
                        terminal["generation"],
                    )
                    terminal["phase"] = "publish"
                    continue
                if phase == "publish":
                    sealed_id, sealed_tombstone, sealed_audit = terminal["sealed_prepared"]
                    active_id, active_tombstone, active_audit = terminal["active_prepared"]
                    sealed_tombstones[sealed_id] = sealed_tombstone; sealed_audits.update(sealed_audit); active_tombstones[active_id] = active_tombstone; active_audits.update(active_audit)
                    terminal["phase"] = "revoke"
                    continue
                if phase == "revoke":
                    _revoke_bound_source(sealed_record)
                    terminal["phase"] = "retire"
                    continue
                if phase == "retire":
                    _retire_lineage(sealed_record)
                    terminal["phase"] = "cleanup"
                    continue
                if phase == "cleanup":
                    cleanup_control, cleanup_ordinary = _cleanup_resources(
                        sealed_record, created=True
                    )
                    if terminal["control"] is None:
                        terminal["control"] = cleanup_control
                    terminal["ordinary"] = (
                        terminal["ordinary"] or cleanup_ordinary
                    )
                    terminal["phase"] = "release"
                    continue
                if phase == "release":
                    sealed_id, sealed_tombstone, sealed_audit = terminal["sealed_prepared"]
                    active_id, active_tombstone, active_audit = terminal["active_prepared"]
                    sealed_registry.pop(id(sealed_handle), None); active_registry.pop(id(terminal["moved_active"]), None); sealed_tombstones[sealed_id] = sealed_tombstone; sealed_audits.update(sealed_audit); active_tombstones[active_id] = active_tombstone; active_audits.update(active_audit)
                    sealed_record["moved_active"] = None
                    terminal["phase"] = "done"
                    continue
                if phase == "done":
                    return terminal["control"], terminal["ordinary"]
                raise _InternalFailure()
            except BaseException as error:
                if type(terminal) is not dict:
                    raise
                terminal["control"], terminal["ordinary"] = _capture_failure(
                    terminal["control"], terminal["ordinary"], error
                )

    def _terminalize_sealed_failure(
        sealed_handle: object,
        sealed_record: Dict[str, Any],
        original_control: Optional[BaseException],
    ) -> None:
        cleanup_control, _cleanup_ordinary = _terminalize_sealed(
            sealed_handle, sealed_record
        )
        if original_control is not None:
            raise original_control
        if cleanup_control is not None:
            raise cleanup_control
        _raise_storage_error()

    def _install_seal_transition(
        spool: object,
        sealing_record: Dict[str, Any],
        sealed_handle: object,
        active_nonowner: Dict[str, Any],
    ) -> None:
        active_registry[id(spool)] = (spool, sealing_record)
        retiring_file_fd = sealing_record["retiring_file_fd"]
        sealing_record["retiring_file_fd"] = None; os.close(retiring_file_fd)
        sealed_registry[id(sealed_handle)] = (sealed_handle, sealing_record)
        _retire_nonowner_handle(
            spool, active_registry, active_tombstones
        )
        binding = sealing_record.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if binding_entry is not None and binding_entry[0] is binding:
            binding_entry[1]["owner_kind"] = "sealed"
            binding_entry[1]["owner_handle"] = sealed_handle
            binding_entry[1]["owner_generation"] = sealing_record[
                "owner_generation"
            ]
        sealing_record["state"] = "sealed"

    def _seal_spool(spool: object, delivery_guard: list) -> object:
        owner_record = _normal_active_record(spool)
        quota_record = _quota_record_for_owner(owner_record)
        if (
            owner_record["pending"] is not None
            or owner_record["live_transfer"] is not None
            or quota_record["reservation"] is not None
            or quota_record["provisional_physical_bytes"] != 0
            or quota_record["provisional_members"] != 0
            or (
                owner_record.get("lane") is production_lane
                and owner_record.get("prefinalization") is None
            )
        ):
            _raise_storage_error()
        operation_failed = False
        operation_control = None
        read_fd = None
        sealed_handle = None
        inventory_digest = None
        sealing_record = None
        transition_ready = False
        if owner_record.get("lane") is production_lane:
            _verify_active_bound_source_current(spool, owner_record)
        try:
            _verify_file_entry(
                owner_record, expected_size=owner_record["committed_eof"]
            )
            os.fsync(owner_record["file_fd"])
            expected_offset = 0
            digest = hashlib.sha256()
            digest.update(
                b"historical_foundry_exchange_spool_receipt_inventory/v1\0"
            )
            for position, receipt in enumerate(owner_record["inventory"], 1):
                receipt_record = _receipt_for_spool(owner_record, receipt)
                projection = receipt_record["projection"]
                if (
                    projection["exchange_index"] != position
                    or projection["spool_member_index"] != position
                    or projection["spool_offset"] != expected_offset
                ):
                    raise _InternalFailure()
                _verify_frame(
                    owner_record,
                    projection,
                    spool_offset=projection["spool_offset"],
                    spool_length=projection["spool_length"],
                    spool_member_sha256=projection["spool_member_sha256"],
                )
                expected_offset += projection["spool_length"]
                payload = json.dumps(
                    dict(projection),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                digest.update(len(payload).to_bytes(8, "big"))
                digest.update(payload)
            if (
                expected_offset != owner_record["committed_eof"]
                or len(owner_record["inventory"])
                != quota_record["committed_members"]
                or expected_offset != quota_record["committed_physical_bytes"]
            ):
                raise _InternalFailure()
            if os.pread(owner_record["file_fd"], 1, expected_offset) != b"":
                raise _InternalFailure()
            _verify_file_entry(owner_record, expected_size=expected_offset)
            inventory_digest = digest.hexdigest()
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
            read_fd = os.open(
                owner_record["basename"],
                flags,
                dir_fd=owner_record["chain"][-1][0],
            )
            opened = os.fstat(read_fd)
            current = os.stat(
                owner_record["basename"],
                dir_fd=owner_record["chain"][-1][0],
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(opened.st_mode)
                or _file_identity(opened) != owner_record["file_identity"]
                or _file_identity(current) != owner_record["file_identity"]
                or opened.st_size != expected_offset
                or current.st_size != expected_offset
            ):
                raise _InternalFailure()
            _verify_file_entry(owner_record, expected_size=expected_offset)
            reopened = os.fstat(read_fd)
            reopened_current = os.stat(
                owner_record["basename"],
                dir_fd=owner_record["chain"][-1][0],
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(reopened.st_mode)
                or not stat.S_ISREG(reopened_current.st_mode)
                or _file_identity(reopened) != owner_record["file_identity"]
                or _file_identity(reopened_current)
                != owner_record["file_identity"]
                or reopened.st_size != expected_offset
                or reopened_current.st_size != expected_offset
            ):
                raise _InternalFailure()
            preseal_audit = _terminal_audit(owner_record)
            if preseal_audit is None:
                raise _InternalFailure()
            sealed_handle = object.__new__(_SealedHistoricalWindowExchangeSpool)
            sealing_record = dict(owner_record)
            sealing_record.update(
                {
                    "state": "sealing",
                    "receipt_inventory_sha256": inventory_digest,
                    "file_fd": read_fd,
                    "retiring_file_fd": owner_record["file_fd"],
                    "moved_active": spool,
                    "terminal_audit": preseal_audit,
                    "owner_generation": owner_record["owner_generation"] + 1,
                }
            )
            active_nonowner = dict(owner_record)
            active_nonowner.update(
                {
                    "state": "sealed_nonowning",
                    "chain": (),
                    "file_fd": None,
                    "basename": None,
                    "file_identity": None,
                    "quota": None,
                    "inventory": [],
                    "committed_eof": 0,
                    "next_exchange_index": 0,
                    "next_member_index": 0,
                    "receipt_inventory_sha256": None,
                    "source_bound": False,
                    "binding": None,
                    "prefinalization": None,
                    "prefinalization_digests": None,
                    "claimed_finalization": None,
                }
            )
            transition_ready = True
            _install_seal_transition(
                spool, sealing_record, sealed_handle, active_nonowner
            )
            delivery_guard[0] = sealed_handle
            return sealed_handle
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            if transition_ready:
                sealed_entry = sealed_registry.get(id(sealed_handle))
                if sealed_entry is not None and sealed_entry[0] is sealed_handle:
                    _terminalize_sealed_failure(
                        sealed_handle, sealed_entry[1], operation_control
                    )
                current_active = active_registry.get(id(spool))
                if (
                    current_active is not None
                    and current_active[0] is spool
                    and current_active[1] is owner_record
                ):
                    active_registry[id(spool)] = (spool, sealing_record)
                _terminalize_operation_failure(
                    spool, sealing_record, operation_control
                )
            read_close_control = None
            if type(read_fd) is int:
                try:
                    os.close(read_fd)
                except BaseException as error:
                    if not isinstance(error, Exception):
                        read_close_control = error
            if operation_control is None:
                operation_control = read_close_control
            _terminalize_operation_failure(
                spool, owner_record, operation_control
            )
        raise _InternalFailure()

    def _sealed_reread_exchange(
        sealed: object, receipt: object
    ) -> Tuple[bytes, bytes]:
        sealed_record = _live_record(
            sealed, _SealedHistoricalWindowExchangeSpool, sealed_registry
        )
        if sealed_record["state"] != "sealed":
            _raise_storage_error()
        receipt_record = _receipt_for_spool(sealed_record, receipt)
        projection = receipt_record["projection"]
        operation_failed = False
        operation_control = None
        result = None
        try:
            _verify_file_entry(
                sealed_record, expected_size=sealed_record["committed_eof"]
            )
            result = _verify_frame(
                sealed_record,
                projection,
                spool_offset=projection["spool_offset"],
                spool_length=projection["spool_length"],
                spool_member_sha256=projection["spool_member_sha256"],
            )
            _verify_file_entry(
                sealed_record, expected_size=sealed_record["committed_eof"]
            )
        except BaseException as error:
            if isinstance(error, Exception):
                operation_failed = True
            else:
                operation_control = error
        if operation_failed or operation_control is not None:
            _terminalize_sealed_failure(
                sealed, sealed_record, operation_control
            )
        return result

    def _public_spool_control_failure(
        spool: object,
        operation: str,
        original_control: BaseException,
        sealed_delivery: object = None,
    ) -> None:
        sealed_match = None
        if operation == "seal" and sealed_delivery is not None:
            sealed_entry = sealed_registry.get(id(sealed_delivery))
            if (
                sealed_entry is not None
                and sealed_entry[0] is sealed_delivery
                and sealed_entry[1].get("moved_active") is spool
            ):
                sealed_match = sealed_entry
        if sealed_match is not None:
            _terminalize_sealed(sealed_match[0], sealed_match[1])
            raise original_control
        active_entry = active_registry.get(id(spool))
        if active_entry is not None and active_entry[0] is spool:
            owner_record = active_entry[1]
            if owner_record.get("state") == "sealed_nonowning":
                raise original_control
            if operation == "append" and owner_record.get("file_fd") is not None:
                quota_entry = quota_registry.get(id(owner_record.get("quota")))
                if (
                    quota_entry is not None
                    and quota_entry[0] is owner_record.get("quota")
                    and type(quota_entry[1].get("reservation")) is dict
                    and quota_entry[1]["reservation"].get("kind") == "append"
                ):
                    _rollback_append(
                        owner_record,
                        quota_entry[1],
                        owner_record["committed_eof"],
                    )
            _terminalize_active(spool, owner_record)
        raise original_control

    def _public_quota_control_failure(
        quota: object, original_control: BaseException
    ) -> None:
        owner_match = None
        for owner_handle, owner_record in active_registry.values():
            if owner_record.get("quota") is quota:
                owner_match = (owner_handle, owner_record)
                break
        if owner_match is not None:
            _terminalize_active(owner_match[0], owner_match[1])
        raise original_control

    def _public_sealed_control_failure(
        sealed: object, original_control: BaseException
    ) -> None:
        sealed_entry = sealed_registry.get(id(sealed))
        if sealed_entry is not None and sealed_entry[0] is sealed:
            _terminalize_sealed(sealed, sealed_entry[1])
        raise original_control

    class _ProductionArchiveRpcExchangeTransfer(transfer_base):
        __slots__ = ("__weakref__",)

    transfer_authorized[0] = _ProductionArchiveRpcExchangeTransfer

    class _PendingHistoricalWindowSpoolReceipt(pending_base):
        __slots__ = ("__weakref__",)

    pending_authorized[0] = _PendingHistoricalWindowSpoolReceipt

    class _HistoricalWindowSpoolReceipt(receipt_base, MappingABC):
        __slots__ = ("__weakref__",)

        def __getitem__(self, key: str) -> Any:
            record = _live_record(self, _HistoricalWindowSpoolReceipt, receipt_registry)
            if type(key) is not str or key not in receipt_keys:
                _raise_storage_error()
            return record["projection"][key]

        def __iter__(self) -> Iterator[str]:
            record = _live_record(self, _HistoricalWindowSpoolReceipt, receipt_registry)
            return iter(record["projection"])

        def __len__(self) -> int:
            record = _live_record(self, _HistoricalWindowSpoolReceipt, receipt_registry)
            return len(record["projection"])

    receipt_authorized[0] = _HistoricalWindowSpoolReceipt

    def _fail_moved_owner_delivery(
        handle: object,
        owner: Dict[str, Any],
        registry: Dict[int, Tuple[object, Dict[str, Any]]],
        closed_state: str,
        original_error: BaseException,
    ) -> None:
        cleanup_error = None
        try:
            _close_moved_owner(handle, owner, registry, closed_state)
        except BaseException as error:
            cleanup_error = error
        if not isinstance(original_error, Exception):
            raise original_error
        if cleanup_error is not None and not isinstance(
            cleanup_error, Exception
        ):
            raise cleanup_error
        _raise_storage_error()

    class _ProductionHistoricalWindowCapability(capability_base):
        __slots__ = ("__weakref__",)

        def close(self) -> None:
            if type(self) is not _ProductionHistoricalWindowCapability:
                _raise_storage_error()
            entry = capability_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    _ProductionHistoricalWindowCapability,
                    capability_tombstones,
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record.get("constructor") is not constructor_provenance:
                _raise_storage_error()
            if record["state"] in ("consumed_nonowning", "closed_nonowning"):
                return None
            if (
                record["state"] == "closing"
                and type(record.get("_moved_terminal_state")) is dict
            ):
                return _close_moved_owner(
                    self, record, capability_registry, "closed_nonowning"
                )
            if record["state"] != "capability":
                _raise_storage_error()
            return _close_moved_owner(
                self, record, capability_registry, "closed_nonowning"
            )

        def __enter__(self) -> "_ProductionHistoricalWindowCapability":
            record = _live_record(
                self,
                _ProductionHistoricalWindowCapability,
                capability_registry,
            )
            if record["state"] != "capability":
                _raise_storage_error()
            return self

        def __exit__(
            self, error_type: Any, error: Any, traceback: Any
        ) -> None:
            del error_type, error, traceback
            return self.close()

    capability_authorized[0] = _ProductionHistoricalWindowCapability

    class _ConsumedProductionHistoricalWindowCapabilityView(
        consumed_view_base
    ):
        __slots__ = ("__weakref__",)

        def close(self) -> None:
            if type(self) is not _ConsumedProductionHistoricalWindowCapabilityView:
                _raise_storage_error()
            entry = consumed_view_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    _ConsumedProductionHistoricalWindowCapabilityView,
                    consumed_view_tombstones,
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record.get("constructor") is not constructor_provenance:
                _raise_storage_error()
            if record["state"] == "closed_nonowning":
                return None
            if (
                record["state"] == "closing"
                and type(record.get("_moved_terminal_state")) is dict
            ):
                return _close_moved_owner(
                    self,
                    record,
                    consumed_view_registry,
                    "closed_nonowning",
                )
            if record["state"] != "consumed_view":
                _raise_storage_error()
            return _close_moved_owner(
                self, record, consumed_view_registry, "closed_nonowning"
            )

        def __enter__(
            self,
        ) -> "_ConsumedProductionHistoricalWindowCapabilityView":
            record = _live_record(
                self,
                _ConsumedProductionHistoricalWindowCapabilityView,
                consumed_view_registry,
            )
            if record["state"] != "consumed_view":
                _raise_storage_error()
            return self

        def __exit__(
            self, error_type: Any, error: Any, traceback: Any
        ) -> None:
            del error_type, error, traceback
            return self.close()

    consumed_view_authorized[0] = (
        _ConsumedProductionHistoricalWindowCapabilityView
    )

    class _HistoricalWindowSpoolSourceBinding(binding_base):
        __slots__ = ("__weakref__",)

    binding_authorized[0] = _HistoricalWindowSpoolSourceBinding

    def _install_quota_reserve_transition(
        owner_handle: object,
        prior_owner: Dict[str, Any],
        transition_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
        live_transfer: object,
        transfer_record: Optional[Dict[str, Any]],
    ) -> None:
        current = active_registry.get(id(owner_handle))
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        active_registry[id(owner_handle)] = (owner_handle, final_owner)
        if live_transfer is not None and transfer_record is not None:
            transfer_record["canonical_request_bytes"] = None
            transfer_record["decoded_response_bytes"] = None
            transfer_record["state"] = "consumed"
            transfer_registry.pop(id(live_transfer), None)

    def _install_quota_commit_transition(
        owner_handle: object,
        prior_owner: Dict[str, Any],
        transition_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(owner_handle))
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        active_registry[id(owner_handle)] = (owner_handle, final_owner)

    def _install_quota_abort_transition(
        owner_handle: object,
        prior_owner: Dict[str, Any],
        transition_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(owner_handle))
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        active_registry[id(owner_handle)] = (owner_handle, final_owner)

    def _reserve_quota_for_test(
        quota_handle: object, *, physical_bytes: int, members: int
    ) -> None:
        quota_record = _live_record(
            quota_handle, _HistoricalWindowRunQuota, quota_registry
        )
        owner_handle, owner_record = _active_owner_for_quota(
            quota_handle, quota_record
        )
        if (
            owner_record["state"] != "active"
            or owner_record["mode"] not in ("normal", "quota_test_only")
            or owner_record["source_bound"]
            or owner_record["lane"] is not test_lane
        ):
            _raise_storage_error()
        if (
            type(physical_bytes) is not int
            or type(members) is not int
            or physical_bytes <= 0
            or members <= 0
        ):
            _raise_storage_error()
        if quota_record["reservation"] is not None:
            _raise_storage_error()

        live_transfer = None
        transfer_record = None
        if owner_record["mode"] == "normal":
            live_transfers = []
            for candidate_handle, candidate_record in transfer_registry.values():
                if candidate_record.get("lineage") is owner_record["lineage"]:
                    live_transfers.append((candidate_handle, candidate_record))
            live_transfer = owner_record["live_transfer"]
            if (
                owner_record["committed_eof"] != 0
                or owner_record["next_exchange_index"] != 1
                or owner_record["next_member_index"] != 1
                or owner_record["inventory"]
                or owner_record["pending"] is not None
                or quota_record["committed_physical_bytes"] != 0
                or quota_record["committed_members"] != 0
                or quota_record["provisional_physical_bytes"] != 0
                or quota_record["provisional_members"] != 0
                or len(live_transfers) not in (0, 1)
            ):
                _raise_storage_error()
            if live_transfers:
                candidate_handle, transfer_record = live_transfers[0]
                if (
                    candidate_handle is not live_transfer
                    or transfer_record["state"] != "issued"
                    or transfer_record["lane"] is not test_lane
                    or transfer_record["lineage"] is not owner_record["lineage"]
                ):
                    _raise_storage_error()
            elif live_transfer is not None:
                _raise_storage_error()

        currentness_control = None
        currentness_failed = False
        try:
            _verify_file_entry(owner_record, expected_size=0)
        except BaseException as error:
            if isinstance(error, Exception):
                currentness_failed = True
            else:
                currentness_control = error
        if currentness_failed or currentness_control is not None:
            _terminal_quota_failure(
                owner_handle, owner_record, currentness_control
            )

        remaining_bytes = (
            8_589_934_592
            - quota_record["committed_physical_bytes"]
            - quota_record["provisional_physical_bytes"]
        )
        remaining_members = (
            200_000
            - quota_record["committed_members"]
            - quota_record["provisional_members"]
        )
        if physical_bytes > remaining_bytes or members > remaining_members:
            _terminal_quota_failure(owner_handle, owner_record)

        reservation = {
            "kind": "quota_test",
            "token": object(),
            "physical_bytes": physical_bytes,
            "members": members,
        }
        pretransition_audit = _terminal_audit(owner_record)
        if pretransition_audit is None:
            _terminal_quota_failure(owner_handle, owner_record)
        transition_owner = dict(owner_record)
        transition_owner["state"] = "quota_transition"
        transition_owner["terminal_audit"] = pretransition_audit
        next_quota = dict(quota_record)
        next_quota["reservation"] = reservation
        next_quota["provisional_physical_bytes"] = physical_bytes
        next_quota["provisional_members"] = members
        final_owner = dict(owner_record)
        if owner_record["mode"] == "normal":
            final_owner["live_transfer"] = None
            final_owner["mode"] = "quota_test_only"
        transition_failed = False
        transition_control = None
        try:
            _install_quota_reserve_transition(
                owner_handle,
                owner_record,
                transition_owner,
                quota_handle,
                next_quota,
                final_owner,
                live_transfer,
                transfer_record,
            )
            result = None
            return result
        except BaseException as error:
            if isinstance(error, Exception):
                transition_failed = True
            else:
                transition_control = error
        if transition_failed or transition_control is not None:
            if transfer_record is not None:
                transfer_record["canonical_request_bytes"] = None
                transfer_record["decoded_response_bytes"] = None
            _terminal_quota_failure(
                owner_handle, owner_record, transition_control
            )
        raise _InternalFailure()

    def _commit_quota_reservation_for_test(quota_handle: object) -> None:
        quota_record = _live_record(
            quota_handle, _HistoricalWindowRunQuota, quota_registry
        )
        owner_handle, owner_record = _active_owner_for_quota(
            quota_handle, quota_record
        )
        reservation = quota_record["reservation"]
        if (
            owner_record["state"] != "active"
            or owner_record["mode"] != "quota_test_only"
            or type(reservation) is not dict
            or reservation.get("kind") != "quota_test"
        ):
            _raise_storage_error()
        pretransition_audit = _terminal_audit(owner_record)
        if pretransition_audit is None:
            _terminal_quota_failure(owner_handle, owner_record)
        transition_owner = dict(owner_record)
        transition_owner["state"] = "quota_transition"
        transition_owner["terminal_audit"] = pretransition_audit
        next_quota = dict(quota_record)
        next_quota["committed_physical_bytes"] = (
            quota_record["committed_physical_bytes"]
            + reservation["physical_bytes"]
        )
        next_quota["committed_members"] = (
            quota_record["committed_members"] + reservation["members"]
        )
        next_quota["provisional_physical_bytes"] = 0
        next_quota["provisional_members"] = 0
        next_quota["reservation"] = None
        final_owner = dict(owner_record)
        transition_failed = False
        transition_control = None
        try:
            _install_quota_commit_transition(
                owner_handle,
                owner_record,
                transition_owner,
                quota_handle,
                next_quota,
                final_owner,
            )
            result = None
            return result
        except BaseException as error:
            if isinstance(error, Exception):
                transition_failed = True
            else:
                transition_control = error
        if transition_failed or transition_control is not None:
            _terminal_quota_failure(
                owner_handle, owner_record, transition_control
            )
        raise _InternalFailure()

    def _abort_quota_reservation_for_test(quota_handle: object) -> None:
        quota_record = _live_record(
            quota_handle, _HistoricalWindowRunQuota, quota_registry
        )
        owner_handle, owner_record = _active_owner_for_quota(
            quota_handle, quota_record
        )
        reservation = quota_record["reservation"]
        if (
            owner_record["state"] != "active"
            or owner_record["mode"] != "quota_test_only"
            or type(reservation) is not dict
            or reservation.get("kind") != "quota_test"
        ):
            _raise_storage_error()
        pretransition_audit = _terminal_audit(owner_record)
        if pretransition_audit is None:
            _terminal_quota_failure(owner_handle, owner_record)
        transition_owner = dict(owner_record)
        transition_owner["state"] = "quota_transition"
        transition_owner["terminal_audit"] = pretransition_audit
        next_quota = dict(quota_record)
        next_quota["provisional_physical_bytes"] = 0
        next_quota["provisional_members"] = 0
        next_quota["reservation"] = None
        final_owner = dict(owner_record)
        transition_failed = False
        transition_control = None
        try:
            _install_quota_abort_transition(
                owner_handle,
                owner_record,
                transition_owner,
                quota_handle,
                next_quota,
                final_owner,
            )
            result = None
            return result
        except BaseException as error:
            if isinstance(error, Exception):
                transition_failed = True
            else:
                transition_control = error
        if transition_failed or transition_control is not None:
            _terminal_quota_failure(
                owner_handle, owner_record, transition_control
            )
        raise _InternalFailure()

    class _HistoricalWindowRunQuota(quota_base):
        __slots__ = ("__weakref__",)

        def _reserve_for_test(self, *, physical_bytes: int, members: int) -> None:
            try:
                result = _reserve_quota_for_test(
                    self, physical_bytes=physical_bytes, members=members
                )
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_quota_control_failure(self, error)
            raise _InternalFailure()

        def _commit_reservation_for_test(self) -> None:
            try:
                result = _commit_quota_reservation_for_test(self)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_quota_control_failure(self, error)
            raise _InternalFailure()

        def _abort_reservation_for_test(self) -> None:
            try:
                result = _abort_quota_reservation_for_test(self)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_quota_control_failure(self, error)
            raise _InternalFailure()

    quota_authorized[0] = _HistoricalWindowRunQuota

    _bound_object_names = {
        "rpc": (
            "_ArchiveRpcError",
            "_ProductionHistoricalWindowRunClaim",
            "_ProductionHistoricalWindowLogicalBatchScope",
            "_ClaimedHistoricalWindowSourceCapsule",
            "_ProductionArchiveRpcFinalization",
            "_get_claimed_historical_window_config",
            "_consume_claimed_historical_window_source_capsule_for_storage",
            "_commit_claimed_historical_window_source_capsule_move",
            "_abort_claimed_historical_window_source_capsule_move",
            "_open_production_archive_rpc_historical_window_logical_batch",
            "_production_archive_rpc_historical_window_logical_batch_attempt",
            "_finalize_claimed_production_archive_rpc_run_for_historical_window",
            "_verify_claimed_historical_window_finalization",
            "_ProductionHistoricalWindowRunClaim.__enter__",
            "_ProductionHistoricalWindowRunClaim.__exit__",
            "_ProductionHistoricalWindowRunClaim.close",
        ),
        "scan": (
            "_ProductionHistoricalWindowPreFinalization",
            "_ProductionHistoricalWindowReconciliation",
            "_capture_production_historical_window",
            "_verify_production_historical_window_prefinalization",
            "_reconcile_production_historical_window",
            "_verify_production_historical_window_reconciliation",
        ),
        "storage": (
            "_HistoricalWindowExchangeSpool",
            "_SealedHistoricalWindowExchangeSpool",
            "_HistoricalWindowSpoolReconciliationCursor",
            "_ProductionHistoricalWindowCapability",
            "_ConsumedProductionHistoricalWindowCapabilityView",
            "_HistoricalWindowExchangeSpool._bind_claimed_source_authority_from_rpc",
            "_HistoricalWindowExchangeSpool._verify_bound_source_authority_for_claimed_finalization",
            "_HistoricalWindowExchangeSpool.issue_transfer_from_bound_rpc",
            "_HistoricalWindowExchangeSpool.append_transfer",
            "_HistoricalWindowExchangeSpool.verify_pending_receipt",
            "_HistoricalWindowExchangeSpool.commit_transfer",
            "_HistoricalWindowExchangeSpool.verify_committed_receipt",
            "_HistoricalWindowExchangeSpool.release_verified_transfer",
            "_HistoricalWindowExchangeSpool.abort_transfer",
            "_HistoricalWindowExchangeSpool.reread_exchange",
            "_HistoricalWindowExchangeSpool.seal",
            "_HistoricalWindowExchangeSpool.close",
            "_SealedHistoricalWindowExchangeSpool.reread_exchange",
            "_SealedHistoricalWindowExchangeSpool._open_reconciliation_cursor_from_bound_scan",
            "_SealedHistoricalWindowExchangeSpool.mint_production_historical_window_capability",
            "_SealedHistoricalWindowExchangeSpool.close",
            "_HistoricalWindowSpoolReconciliationCursor.__enter__",
            "_HistoricalWindowSpoolReconciliationCursor.__iter__",
            "_HistoricalWindowSpoolReconciliationCursor.__next__",
            "_HistoricalWindowSpoolReconciliationCursor.__exit__",
            "_HistoricalWindowSpoolReconciliationCursor.close",
            "_ProductionHistoricalWindowCapability.__enter__",
            "_ProductionHistoricalWindowCapability.__exit__",
            "_ProductionHistoricalWindowCapability.close",
            "_ConsumedProductionHistoricalWindowCapabilityView.__enter__",
            "_ConsumedProductionHistoricalWindowCapabilityView.__exit__",
            "_ConsumedProductionHistoricalWindowCapabilityView.close",
            "consume_production_historical_window_capability",
        ),
    }

    def _resolve_bound_object(module: Any, qualified_name: str) -> Any:
        value = module
        for component in qualified_name.split("."):
            value = getattr(value, component)
        return value

    def _bound_scan_module_is_current(module: Any) -> bool:
        canonical_name = "scripts.historical_foundry_scan"
        canonical = sys.modules.get(canonical_name)
        main = sys.modules.get("__main__")
        main_spec = getattr(main, "__spec__", None)
        main_is_scan = getattr(main_spec, "name", None) == canonical_name
        return (
            module is canonical
            and (not main_is_scan or main is module)
        ) or (
            module is main
            and main_is_scan
            and (canonical is None or canonical is module)
        )

    def _bound_source_file_identity(details: os.stat_result) -> Tuple[int, ...]:
        return (
            details.st_dev,
            details.st_ino,
            details.st_mode,
            details.st_uid,
            details.st_gid,
            details.st_nlink,
            details.st_size,
            getattr(
                details, "st_mtime_ns",
                int(details.st_mtime * 1_000_000_000),
            ),
            getattr(
                details, "st_ctime_ns",
                int(details.st_ctime * 1_000_000_000),
            ),
        )

    def _read_bound_source_fd(fd: int, expected_size: int) -> bytes:
        chunks = []
        offset = 0
        while offset <= expected_size:
            chunk = os.pread(
                fd, min(1024 * 1024, expected_size + 1 - offset), offset
            )
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
        return b"".join(chunks)

    def _bound_source_drift(binding_record: Dict[str, Any]) -> BaseException:
        error_class = binding_record.get("rpc_error_class")
        rows = binding_record.get("bound_module_rows")
        if (
            type(rows) is tuple
            and len(rows) == 3
            and type(rows[0]) is tuple
            and len(rows[0]) == 9
            and type(rows[0][8]) is tuple
            and rows[0][8]
            and rows[0][8][0] is error_class
        ):
            try:
                return error_class(
                    "authority_mismatch", "final_identity_drift"
                )
            except BaseException:
                pass
        return HistoricalFoundryStorageError()

    class _BoundSourceIdentityDrift(Exception):
        pass

    def _verify_bound_source_current(binding_record: Dict[str, Any]) -> None:
        drifted = False
        try:
            module_rows = binding_record["bound_module_rows"]
            ancestry_rows = binding_record["ancestry_rows"]
            source_rows = binding_record["source_rows"]
            if (
                binding_record.get("state") != "live"
                or type(module_rows) is not tuple
                or tuple(row[0] for row in module_rows)
                != ("rpc", "scan", "storage")
                or type(ancestry_rows) is not tuple
                or type(source_rows) is not tuple
                or tuple(row[0] for row in source_rows)
                != ("rpc", "scan", "storage")
            ):
                raise _BoundSourceIdentityDrift()
            for row in module_rows:
                (
                    role, canonical_name, actual_key, module, generation,
                    spec_name, origin, file_name, expected_objects,
                ) = row
                spec = getattr(module, "__spec__", None)
                try:
                    current_objects = tuple(
                        _resolve_bound_object(module, name)
                        for name in _bound_object_names[role]
                    )
                except AttributeError:
                    raise _BoundSourceIdentityDrift()
                if (
                    role not in ("rpc", "scan", "storage")
                    or type(canonical_name) is not str
                    or type(actual_key) is not str
                    or sys.modules.get(actual_key) is not module
                    or (
                        role == "scan"
                        and not _bound_scan_module_is_current(module)
                    )
                    or getattr(module, "_HISTORICAL_WINDOW_MODULE_GENERATION", None)
                    is not generation
                    or getattr(spec, "name", None) != spec_name
                    or getattr(spec, "origin", None) != origin
                    or getattr(module, "__file__", None) != file_name
                    or current_objects != expected_objects
                ):
                    raise _BoundSourceIdentityDrift()
            ancestry_fds = []
            for index, row in enumerate(ancestry_rows):
                components, fd, parent_index, name, identity = row
                if (
                    type(components) is not tuple
                    or type(fd) is not int
                    or os.get_inheritable(fd)
                    or _metadata_snapshot(os.fstat(fd)) != identity
                ):
                    raise _BoundSourceIdentityDrift()
                if parent_index is None:
                    if index != 0 or components != () or name is not None:
                        raise _BoundSourceIdentityDrift()
                else:
                    if (
                        type(parent_index) is not int
                        or not 0 <= parent_index < index
                        or type(name) is not str
                        or _metadata_snapshot(os.stat(
                            name,
                            dir_fd=ancestry_fds[parent_index],
                            follow_symlinks=False,
                        )) != identity
                    ):
                        raise _BoundSourceIdentityDrift()
                ancestry_fds.append(fd)
            for row in source_rows:
                (
                    role, fd, parent_index, name, relative, identity,
                    expected_bytes, expected_size, expected_sha256,
                ) = row
                if (
                    role not in ("rpc", "scan", "storage")
                    or type(fd) is not int
                    or type(parent_index) is not int
                    or not 0 <= parent_index < len(ancestry_fds)
                    or type(name) is not str
                    or type(relative) is not str
                    or type(expected_bytes) is not bytes
                    or type(expected_size) is not int
                    or expected_size != len(expected_bytes)
                    or type(expected_sha256) is not str
                    or os.get_inheritable(fd)
                    or _bound_source_file_identity(os.fstat(fd)) != identity
                    or _bound_source_file_identity(os.stat(
                        name,
                        dir_fd=ancestry_fds[parent_index],
                        follow_symlinks=False,
                    )) != identity
                ):
                    raise _BoundSourceIdentityDrift()
                observed = _read_bound_source_fd(fd, expected_size)
                if (
                    observed != expected_bytes
                    or hashlib.sha256(observed).hexdigest()
                    != expected_sha256
                ):
                    raise _BoundSourceIdentityDrift()
        except (_BoundSourceIdentityDrift, OSError):
            drifted = True
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            _raise_storage_error()
        if drifted:
            raise _bound_source_drift(binding_record)

    def _verify_active_bound_source_current(
        spool: object, owner: Dict[str, Any]
    ) -> None:
        binding = owner.get("binding")
        entry = binding_registry.get(id(binding))
        if entry is None or entry[0] is not binding:
            _terminalize_operation_failure(spool, owner, None)
        try:
            _verify_bound_source_current(entry[1])
        except BaseException as error:
            cleanup_control, _cleanup_ordinary = _terminalize_active(
                spool, owner
            )
            if not isinstance(error, Exception):
                raise
            if cleanup_control is not None:
                raise cleanup_control
            raise

    def _verify_sealed_bound_source_current(
        sealed: object, owner: Dict[str, Any]
    ) -> None:
        binding = owner.get("binding")
        entry = binding_registry.get(id(binding))
        if entry is None or entry[0] is not binding:
            _terminalize_sealed_failure(sealed, owner, None)
        try:
            _verify_bound_source_current(entry[1])
        except BaseException as error:
            cleanup_control, _cleanup_ordinary = _terminalize_sealed(
                sealed, owner
            )
            if not isinstance(error, Exception):
                raise
            if cleanup_control is not None:
                raise cleanup_control
            raise

    def _close_bound_source_rows(binding_record: Dict[str, Any]) -> None:
        if binding_record.get("state") == "closed":
            return
        attempted = binding_record.setdefault("attempted_fds", set())
        first_control = None
        ordinary = False
        rows = binding_record.get("ancestry_rows", ()) + binding_record.get(
            "source_rows", ()
        )
        for row in reversed(rows):
            fd = row[1]
            if type(fd) is not int or fd in attempted:
                continue
            attempted.add(fd)
            try:
                os.close(fd)
            except BaseException as error:
                if isinstance(error, Exception):
                    ordinary = True
                elif first_control is None:
                    first_control = error
        binding_record["state"] = "closed"
        if first_control is not None:
            raise first_control
        if ordinary:
            _raise_storage_error()

    def _revoke_bound_source(record: Dict[str, Any]) -> None:
        binding = record.get("binding")
        if binding is None:
            return
        entry = binding_registry.get(id(binding))
        if entry is not None and entry[0] is binding:
            _close_bound_source_rows(entry[1])

    def _bind_claimed_source_authority(
        spool: object,
        claim: Any,
        bound_rpc_module: Any,
        bound_scan_module: Any,
        bound_storage_module: Any,
        source_capsule: Any,
        delivery_guard: List[Any],
    ) -> object:
        owner = _normal_active_record(spool)
        if (
            owner["lane"] is not None
            or owner["source_bound"]
            or owner["inventory"]
            or owner["committed_eof"] != 0
            or owner["live_transfer"] is not None
            or owner["pending"] is not None
            or bound_storage_module is not sys.modules.get(__name__)
            or bound_rpc_module
            is not sys.modules.get("scripts.historical_foundry_rpc")
            or not _bound_scan_module_is_current(bound_scan_module)
        ):
            _raise_storage_error()
        consume = getattr(
            bound_rpc_module,
            "_consume_claimed_historical_window_source_capsule_for_storage",
            None,
        )
        commit = getattr(
            bound_rpc_module,
            "_commit_claimed_historical_window_source_capsule_move",
            None,
        )
        abort = getattr(
            bound_rpc_module,
            "_abort_claimed_historical_window_source_capsule_move",
            None,
        )
        if not callable(consume) or not callable(commit) or not callable(abort):
            _raise_storage_error()
        payload = None
        binding = None
        binding_record = None
        installed = False
        owner["state"] = "binding"
        try:
            payload = consume(
                capsule=source_capsule,
                expected_claim=claim,
                expected_spool=spool,
                expected_storage_module=bound_storage_module,
            )
            if (
                type(payload) is not tuple
                or len(payload) != 4
                or payload[0]
                != "historical_foundry_claimed_source_payload/v1"
                or type(payload[1]) is not tuple
                or type(payload[2]) is not tuple
                or type(payload[3]) is not tuple
                or tuple(row[0] for row in payload[2])
                != ("rpc", "scan", "storage")
                or tuple(row[0] for row in payload[3])
                != ("rpc", "scan", "storage")
                or payload[3][0][3] is not bound_rpc_module
                or payload[3][1][3] is not bound_scan_module
                or payload[3][2][3] is not bound_storage_module
            ):
                raise _InternalFailure()
            all_fds = tuple(row[1] for row in payload[1] + payload[2])
            if (
                any(type(fd) is not int or fd < 0 for fd in all_fds)
                or len(set(all_fds)) != len(all_fds)
            ):
                raise _InternalFailure()
            binding_record = {
                "lineage": owner["lineage"],
                "state": "pending",
                "claim": claim,
                "rpc_module": bound_rpc_module,
                "scan_module": bound_scan_module,
                "storage_module": bound_storage_module,
                "ancestry_rows": payload[1],
                "source_rows": payload[2],
                "bound_module_rows": payload[3],
                "attempted_fds": set(),
                "owner_kind": "active",
                "owner_handle": spool,
                "owner_generation": owner["owner_generation"],
                "prefinalization_class": payload[3][1][8][0],
                "prefinalization_verifier": payload[3][1][8][3],
                "rpc_claim_class": payload[3][0][8][1],
                "rpc_error_class": payload[3][0][8][0],
                "rpc_finalization_class": payload[3][0][8][4],
                "rpc_finalization_verifier": payload[3][0][8][12],
                "scan_reconciliation_class": payload[3][1][8][1],
                "scan_reconciliation_verifier": payload[3][1][8][5],
            }
            binding = _prepare_handle(
                _HistoricalWindowSpoolSourceBinding, binding_record
            )
            binding_registry[id(binding)] = (binding, binding_record)
            installed = True
            binding_record["state"] = "live_undelivered"
            owner["lane"] = production_lane
            owner["source_bound"] = True
            owner["binding"] = binding
            commit(
                capsule=source_capsule,
                expected_claim=claim,
                expected_spool=spool,
                binding=binding,
            )
            binding_record["state"] = "live"
            owner["state"] = "active"
            delivery_guard[0] = (
                owner, binding, binding_record, abort, source_capsule,
                claim, bound_storage_module,
            )
            return binding
        except BaseException as original_error:
            control = (
                original_error
                if not isinstance(original_error, Exception) else None
            )
            if binding_record is not None:
                binding_record["state"] = "closing"
                try:
                    _close_bound_source_rows(binding_record)
                except BaseException as close_error:
                    if (
                        not isinstance(close_error, Exception)
                        and control is None
                    ):
                        control = close_error
            if payload is not None:
                try:
                    abort(
                        capsule=source_capsule,
                        expected_claim=claim,
                        expected_spool=spool,
                    )
                except BaseException as abort_error:
                    if (
                        not isinstance(abort_error, Exception)
                        and control is None
                    ):
                        control = abort_error
            owner["state"] = "active"
            owner["lane"] = None
            owner["source_bound"] = False
            owner["binding"] = None
            if installed:
                binding_registry.pop(id(binding), None)
            if control is not None and control is not original_error:
                raise control
            raise

    def _rollback_claimed_source_binding_delivery(
        spool: object,
        guard: Tuple[Any, ...],
        original_control: BaseException,
    ) -> None:
        (
            owner, binding, binding_record, abort, source_capsule,
            claim, bound_storage_module,
        ) = guard
        control = original_control
        entry = active_registry.get(id(spool))
        if (
            entry is not None
            and entry[0] is spool
            and entry[1] is owner
            and owner.get("binding") is binding
        ):
            binding_record["state"] = "closing"
            try:
                _close_bound_source_rows(binding_record)
            except BaseException as close_error:
                if isinstance(control, Exception) and not isinstance(
                    close_error, Exception
                ):
                    control = close_error
            try:
                abort(
                    capsule=source_capsule,
                    expected_claim=claim,
                    expected_spool=spool,
                )
            except BaseException as abort_error:
                if isinstance(control, Exception) and not isinstance(
                    abort_error, Exception
                ):
                    control = abort_error
            owner["state"] = "active"
            owner["lane"] = None
            owner["source_bound"] = False
            owner["binding"] = None
            binding_registry.pop(id(binding), None)
        raise control

    def _project_bound_rpc_transfer_state(
        spool: object, claim: Any
    ) -> str:
        owner = _normal_active_record(spool)
        binding = owner.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if (
            owner.get("lane") is not production_lane
            or not owner.get("source_bound")
            or binding_entry is None
            or binding_entry[0] is not binding
            or binding_entry[1].get("state") != "live"
            or binding_entry[1].get("claim") is not claim
            or binding_entry[1].get("owner_kind") != "active"
            or binding_entry[1].get("owner_handle") is not spool
            or binding_entry[1].get("owner_generation")
            != owner.get("owner_generation")
        ):
            _raise_storage_error()
        _verify_bound_source_current(binding_entry[1])
        transfer = owner.get("live_transfer")
        pending = owner.get("pending")
        if transfer is None:
            if pending is not None:
                _raise_storage_error()
            return "clear"
        transfer_entry = transfer_registry.get(id(transfer))
        if (
            transfer_entry is None
            or transfer_entry[0] is not transfer
            or transfer_entry[1].get("lineage") is not owner.get("lineage")
            or transfer_entry[1].get("lane") is not production_lane
        ):
            _raise_storage_error()
        state = transfer_entry[1].get("state")
        if state not in (
            "issued",
            "pending",
            "pending_verified",
            "committed_unverified",
            "committed_verified",
        ):
            _raise_storage_error()
        return state

    def _issue_transfer_from_bound_rpc(
        spool: object,
        claim: Any,
        exchange_projection: Mapping[str, Any],
        canonical_request_bytes: bytes,
        decoded_response_bytes: bytes,
    ) -> object:
        record = _normal_active_record(spool)
        binding = record.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if (
            record["lane"] is not production_lane
            or not record["source_bound"]
            or binding_entry is None
            or binding_entry[0] is not binding
            or binding_entry[1]["state"] != "live"
            or binding_entry[1]["claim"] is not claim
            or record["live_transfer"] is not None
            or record["pending"] is not None
        ):
            _raise_storage_error()
        _verify_bound_source_current(binding_entry[1])
        projection = _validate_exchange_projection(
            exchange_projection,
            canonical_request_bytes,
            decoded_response_bytes,
        )
        transfer_record = {
            "lineage": record["lineage"],
            "lane": production_lane,
            "exchange": object(),
            "exchange_index": projection["exchange_index"],
            "state": "issued",
            "projection": projection,
            "canonical_request_bytes": memoryview(
                canonical_request_bytes
            ).tobytes(),
            "decoded_response_bytes": memoryview(
                decoded_response_bytes
            ).tobytes(),
        }
        transfer = _prepare_handle(
            _ProductionArchiveRpcExchangeTransfer, transfer_record
        )
        issuing_owner = dict(record)
        issuing_owner["state"] = "issuing"
        final_owner = dict(record)
        final_owner["live_transfer"] = transfer
        try:
            _install_transfer_transition(
                spool,
                record,
                issuing_owner,
                final_owner,
                transfer,
                transfer_record,
            )
            return transfer
        except BaseException:
            transfer_record["canonical_request_bytes"] = None
            transfer_record["decoded_response_bytes"] = None
            transfer_registry.pop(id(transfer), None)
            raise

    def _verify_pending_production_receipt(
        spool: object, transfer: object, pending_receipt: object
    ) -> None:
        owner = _normal_active_record(spool)
        if owner["lane"] is not production_lane:
            _raise_storage_error()
        transfer_record = _transfer_for_spool(owner, transfer, "pending")
        pending_record = _pending_for_spool(
            owner, transfer_record, pending_receipt, "pending"
        )
        projection = pending_record["projection"]
        _verify_file_entry(
            owner,
            expected_size=projection["spool_offset"] + projection["spool_length"],
        )
        _verify_frame(
            owner,
            projection,
            spool_offset=projection["spool_offset"],
            spool_length=projection["spool_length"],
            spool_member_sha256=projection["spool_member_sha256"],
        )
        next_transfer = dict(transfer_record)
        next_transfer["state"] = "pending_verified"
        next_pending = dict(pending_record)
        next_pending["state"] = "pending_verified"
        transfer_registry[id(transfer)] = (transfer, next_transfer)
        pending_registry[id(pending_receipt)] = (
            pending_receipt, next_pending
        )
        return None

    def _verify_bound_source_authority_for_claimed_finalization(
        spool: object, claim: Any, prefinalization: Any
    ) -> None:
        owner = _normal_active_record(spool)
        binding = owner.get("binding")
        entry = binding_registry.get(id(binding))
        if (
            owner.get("lane") is not production_lane
            or not owner.get("source_bound")
            or owner.get("live_transfer") is not None
            or owner.get("pending") is not None
            or owner.get("prefinalization") is not None
            or entry is None
            or entry[0] is not binding
        ):
            _raise_storage_error()
        binding_record = entry[1]
        if (
            binding_record.get("state") != "live"
            or binding_record.get("claim") is not claim
            or binding_record.get("owner_kind") != "active"
            or binding_record.get("owner_handle") is not spool
            or binding_record.get("owner_generation")
            != owner.get("owner_generation")
            or type(prefinalization)
            is not binding_record.get("prefinalization_class")
        ):
            _raise_storage_error()
        _verify_bound_source_current(binding_record)
        verifier = binding_record.get("prefinalization_verifier")
        if not callable(verifier):
            _raise_storage_error()
        verifier(
            prefinalization=prefinalization,
            expected_claim=claim,
            expected_spool=spool,
        )
        digests = getattr(prefinalization, "_digests", None)
        if (
            type(digests) is not tuple
            or len(digests) != 5
            or digests[0]
            != "historical_foundry_prefinalization_digest_binding/v1"
        ):
            _raise_storage_error()
        digest = hashlib.sha256()
        digest.update(
            b"historical_foundry_exchange_spool_receipt_inventory/v1\0"
        )
        expected_offset = 0
        for position, receipt in enumerate(owner["inventory"], 1):
            receipt_record = _receipt_for_spool(owner, receipt)
            projection = receipt_record["projection"]
            if (
                receipt_record.get("state") != "committed"
                or projection["exchange_index"] != position
                or projection["spool_member_index"] != position
                or projection["spool_offset"] != expected_offset
            ):
                _raise_storage_error()
            _verify_frame(
                owner,
                projection,
                spool_offset=projection["spool_offset"],
                spool_length=projection["spool_length"],
                spool_member_sha256=projection["spool_member_sha256"],
            )
            expected_offset += projection["spool_length"]
            payload = json.dumps(
                dict(projection),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        if expected_offset != owner["committed_eof"]:
            _raise_storage_error()
        owner["prefinalization"] = prefinalization
        owner["prefinalization_digests"] = digests
        owner["receipt_inventory_sha256"] = digest.hexdigest()
        return None

    def _verify_committed_production_receipt(
        spool: object, transfer: object, receipt: object
    ) -> None:
        owner = _normal_active_record(spool)
        if owner["lane"] is not production_lane:
            _raise_storage_error()
        transfer_record = _transfer_for_spool(
            owner, transfer, "committed_unverified"
        )
        receipt_record = _receipt_for_spool(owner, receipt)
        if (
            receipt_record["state"] != "committed_unverified"
            or transfer_record.get("receipt") is not receipt
        ):
            _raise_storage_error()
        projection = receipt_record["projection"]
        _verify_file_entry(owner, expected_size=owner["committed_eof"])
        _verify_frame(
            owner,
            projection,
            spool_offset=projection["spool_offset"],
            spool_length=projection["spool_length"],
            spool_member_sha256=projection["spool_member_sha256"],
        )
        pending = transfer_record.get("pending_receipt")
        pending_record = _live_record(
            pending, _PendingHistoricalWindowSpoolReceipt, pending_registry
        )
        next_transfer = dict(transfer_record)
        next_transfer["state"] = "committed_verified"
        next_pending = dict(pending_record)
        next_pending["state"] = "committed_verified"
        next_receipt = dict(receipt_record)
        next_receipt["state"] = "committed_verified"
        transfer_registry[id(transfer)] = (transfer, next_transfer)
        pending_registry[id(pending)] = (pending, next_pending)
        receipt_registry[id(receipt)] = (receipt, next_receipt)
        return None

    def _release_verified_production_transfer(
        spool: object, transfer: object, receipt: object
    ) -> None:
        owner = _normal_active_record(spool)
        if owner["lane"] is not production_lane:
            _raise_storage_error()
        transfer_record = _transfer_for_spool(
            owner, transfer, "committed_verified"
        )
        receipt_record = _receipt_for_spool(owner, receipt)
        if (
            receipt_record["state"] != "committed_verified"
            or transfer_record.get("receipt") is not receipt
        ):
            _raise_storage_error()
        pending = transfer_record.get("pending_receipt")
        pending_record = _live_record(
            pending, _PendingHistoricalWindowSpoolReceipt, pending_registry
        )
        if pending_record["state"] != "committed_verified":
            _raise_storage_error()
        transfer_record["canonical_request_bytes"] = None
        transfer_record["decoded_response_bytes"] = None
        transfer_record["state"] = "consumed"
        pending_record["state"] = "consumed"
        receipt_record["state"] = "committed"
        transfer_registry.pop(id(transfer), None)
        pending_registry.pop(id(pending), None)
        final_owner = dict(owner)
        final_owner["live_transfer"] = None
        active_registry[id(spool)] = (spool, final_owner)
        return None

    def _fail_production_transfer_delivery(
        spool: object, original_error: BaseException
    ) -> None:
        cleanup_control = None
        entry = active_registry.get(id(spool))
        if entry is not None and entry[0] is spool:
            try:
                cleanup_control, _cleanup_ordinary = _terminalize_active(
                    spool, entry[1]
                )
            except BaseException as cleanup_error:
                if not isinstance(cleanup_error, Exception):
                    cleanup_control = cleanup_error
        if not isinstance(original_error, Exception):
            raise original_error
        if cleanup_control is not None:
            raise cleanup_control
        raise original_error

    class _HistoricalWindowExchangeSpool(active_base):
        __slots__ = ("__weakref__",)

        def _bind_claimed_source_authority_from_rpc(
            self,
            *,
            claim: Any,
            bound_rpc_module: Any,
            bound_scan_module: Any,
            bound_storage_module: Any,
            source_capsule: Any,
        ) -> "_HistoricalWindowSpoolSourceBinding":
            delivery_guard = [None]
            try:
                result = _bind_claimed_source_authority(
                    self,
                    claim,
                    bound_rpc_module,
                    bound_scan_module,
                    bound_storage_module,
                    source_capsule,
                    delivery_guard,
                )
                return result
            except BaseException as error:
                if (
                    not isinstance(error, Exception)
                    and delivery_guard[0] is not None
                ):
                    _rollback_claimed_source_binding_delivery(
                        self, delivery_guard[0], error
                    )
                raise

        def _verify_bound_source_authority_for_claimed_finalization(
            self,
            *,
            claim: Any,
            prefinalization: Any,
        ) -> None:
            return _verify_bound_source_authority_for_claimed_finalization(
                self, claim, prefinalization
            )

        def _project_bound_rpc_transfer_state(
            self, *, claim: Any
        ) -> str:
            return _project_bound_rpc_transfer_state(self, claim)

        def issue_transfer_from_bound_rpc(
            self,
            *,
            claim: Any,
            exchange_projection: Mapping[str, Any],
            canonical_request_bytes: bytes,
            decoded_response_bytes: bytes,
        ) -> "_ProductionArchiveRpcExchangeTransfer":
            try:
                return _issue_transfer_from_bound_rpc(
                    self,
                    claim,
                    exchange_projection,
                    canonical_request_bytes,
                    decoded_response_bytes,
                )
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _fail_production_transfer_delivery(self, error)
            raise _InternalFailure()

        def verify_pending_receipt(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
            pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
        ) -> None:
            try:
                return _verify_pending_production_receipt(
                    self, transfer, pending_receipt
                )
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _fail_production_transfer_delivery(self, error)
            raise _InternalFailure()

        def verify_committed_receipt(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
            receipt: "_HistoricalWindowSpoolReceipt",
        ) -> None:
            try:
                return _verify_committed_production_receipt(
                    self, transfer, receipt
                )
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _fail_production_transfer_delivery(self, error)
            raise _InternalFailure()

        def release_verified_transfer(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
            receipt: "_HistoricalWindowSpoolReceipt",
        ) -> None:
            try:
                return _release_verified_production_transfer(
                    self, transfer, receipt
                )
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _fail_production_transfer_delivery(self, error)
            raise _InternalFailure()

        def append_transfer(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
        ) -> "_PendingHistoricalWindowSpoolReceipt":
            try:
                result = _append_transfer(self, transfer)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_spool_control_failure(self, "append", error)
            raise _InternalFailure()

        def commit_transfer(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
            pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
        ) -> "_HistoricalWindowSpoolReceipt":
            try:
                result = _commit_transfer(self, transfer, pending_receipt)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_spool_control_failure(self, "commit", error)
            raise _InternalFailure()

        def abort_transfer(
            self,
            *,
            transfer: "_ProductionArchiveRpcExchangeTransfer",
            pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
        ) -> None:
            try:
                result = _abort_transfer(self, transfer, pending_receipt)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_spool_control_failure(self, "abort", error)
            raise _InternalFailure()

        def reread_exchange(
            self,
            *,
            receipt: "_HistoricalWindowSpoolReceipt",
        ) -> Tuple[bytes, bytes]:
            try:
                result = _reread_exchange(self, receipt)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_spool_control_failure(self, "reread", error)
            raise _InternalFailure()

        def seal(self) -> "_SealedHistoricalWindowExchangeSpool":
            delivery_guard = [None]
            try:
                result = _seal_spool(self, delivery_guard)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_spool_control_failure(
                    self, "seal", error, delivery_guard[0]
                )
            raise _InternalFailure()

        def close(self) -> None:
            if type(self) is not _HistoricalWindowExchangeSpool:
                _raise_storage_error()
            entry = active_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self, _HistoricalWindowExchangeSpool, active_tombstones
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record.get("constructor") is not constructor_provenance:
                _raise_storage_error()
            if record["state"] == "sealed_nonowning":
                return None
            if record["state"] == "closing":
                if type(record.get("_terminal_state")) is dict:
                    return None
                _raise_storage_error()
            if record["state"] != "active":
                _raise_storage_error()
            try:
                control, ordinary = _terminalize_active(self, record)
                if control is not None:
                    raise control
                if ordinary:
                    _raise_storage_error()
                result = None
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                terminal = record.get("_terminal_state")
                if type(terminal) is not dict or terminal.get("phase") != "done":
                    _terminalize_active(self, record)
                raise error

    active_authorized[0] = _HistoricalWindowExchangeSpool

    def _next_reconciliation_cursor_core(
        cursor: object, delivery_guard: List[Any]
    ) -> Tuple[Mapping[str, Any], bytes, bytes]:
        record = _live_record(
            cursor,
            _HistoricalWindowSpoolReconciliationCursor,
            cursor_registry,
        )
        if record["state"] != "entered":
            _raise_storage_error()
        sealed = record["sealed"]
        entry = sealed_registry.get(id(sealed))
        if (
            entry is None
            or entry[0] is not sealed
            or entry[1] is not record["owner"]
            or entry[1]["state"] != "sealed"
        ):
            _raise_storage_error()
        owner = entry[1]; delivery_guard[0] = (sealed, owner, record)
        position = record["position"]
        if position == len(owner["inventory"]):
            _verify_sealed_bound_source_current(sealed, owner)
            record["eof"] = True
            raise StopIteration
        if position < 0 or position > len(owner["inventory"]):
            _raise_storage_error()
        receipt = owner["inventory"][position]
        receipt_record = _receipt_for_spool(owner, receipt)
        projection = receipt_record["projection"]
        try:
            _verify_file_entry(
                owner, expected_size=owner["committed_eof"]
            )
            request, decoded = _verify_frame(
                owner,
                projection,
                spool_offset=projection["spool_offset"],
                spool_length=projection["spool_length"],
                spool_member_sha256=projection["spool_member_sha256"],
            )
            _verify_file_entry(
                owner, expected_size=owner["committed_eof"]
            )
        except BaseException as error:
            _terminalize_sealed(sealed, owner)
            record["state"] = "closed"
            if not isinstance(error, Exception):
                raise
            _raise_storage_error()
        record["position"] = position + 1
        return dict(projection), request, decoded

    class _HistoricalWindowSpoolReconciliationCursor(cursor_base):
        __slots__ = ("__weakref__",)

        def __enter__(self) -> "_HistoricalWindowSpoolReconciliationCursor":
            record = _live_record(
                self,
                _HistoricalWindowSpoolReconciliationCursor,
                cursor_registry,
            )
            if record["state"] != "fresh":
                _raise_storage_error()
            record["state"] = "entered"
            return self

        def __iter__(self) -> "_HistoricalWindowSpoolReconciliationCursor":
            record = _live_record(
                self,
                _HistoricalWindowSpoolReconciliationCursor,
                cursor_registry,
            )
            if record["state"] != "entered":
                _raise_storage_error()
            return self

        def __next__(self) -> Tuple[Mapping[str, Any], bytes, bytes]:
            delivery_guard = [None]
            try:
                result = _next_reconciliation_cursor_core(
                    self, delivery_guard
                )
                return result
            except StopIteration:
                raise
            except BaseException as error:
                guarded = delivery_guard[0]
                if guarded is None:
                    raise
                sealed, owner, record = guarded
                control, _ordinary = _terminalize_sealed(sealed, owner)
                record["state"] = "closed"
                if not isinstance(error, Exception):
                    raise error
                if control is not None:
                    raise control
                _raise_storage_error()
            raise _InternalFailure()

        def __exit__(
            self, error_type: Any, error: Any, traceback: Any
        ) -> None:
            del traceback
            if type(self) is not _HistoricalWindowSpoolReconciliationCursor:
                _raise_storage_error()
            entry = cursor_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    _HistoricalWindowSpoolReconciliationCursor,
                    cursor_tombstones,
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record["state"] == "complete":
                return None
            if (
                record["state"] == "entered"
                and error_type is None
                and error is None
                and record["eof"]
            ):
                record["state"] = "complete"
                record["owner"]["reconciliation_read_complete"] = True
                return None
            sealed = record["sealed"]
            owner = record["owner"]
            record["state"] = "closed"
            control, ordinary = _terminalize_sealed(sealed, owner)
            if error is not None and not isinstance(error, Exception):
                return None
            if control is not None:
                raise control
            if error is not None:
                return None
            if ordinary:
                _raise_storage_error()
            return None

        def close(self) -> None:
            if type(self) is not _HistoricalWindowSpoolReconciliationCursor:
                _raise_storage_error()
            entry = cursor_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    _HistoricalWindowSpoolReconciliationCursor,
                    cursor_tombstones,
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record["state"] in ("complete", "closed"):
                return None
            sealed = record["sealed"]
            owner = record["owner"]
            record["state"] = "closed"
            control, ordinary = _terminalize_sealed(sealed, owner)
            if control is not None:
                raise control
            if ordinary:
                _raise_storage_error()
            return None

    cursor_authorized[0] = _HistoricalWindowSpoolReconciliationCursor

    def _open_reconciliation_cursor_core(
        sealed: object,
        *,
        claim: Any,
        finalization: Any,
        delivery_guard: List[Any],
    ) -> object:
        owner = _live_record(
            sealed, _SealedHistoricalWindowExchangeSpool, sealed_registry
        )
        delivery_guard[0] = (sealed, owner)
        binding = owner.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if (
            owner["state"] != "sealed"
            or owner.get("lane") is not production_lane
            or not owner.get("source_bound")
            or owner.get("prefinalization") is None
            or owner.get("reconciliation_cursor_opened")
            or binding_entry is None
            or binding_entry[0] is not binding
        ):
            _raise_storage_error()
        binding_record = binding_entry[1]
        if (
            binding_record.get("state") != "live"
            or binding_record.get("claim") is not claim
            or binding_record.get("owner_kind") != "sealed"
            or binding_record.get("owner_handle") is not sealed
            or binding_record.get("owner_generation")
            != owner.get("owner_generation")
            or type(claim) is not binding_record.get("rpc_claim_class")
            or type(finalization)
            is not binding_record.get("rpc_finalization_class")
        ):
            _raise_storage_error()
        _verify_sealed_bound_source_current(sealed, owner)
        verifier = binding_record.get("rpc_finalization_verifier")
        if not callable(verifier):
            _raise_storage_error()
        verifier(
            claim=claim,
            finalization=finalization,
            expected_prefinalization=owner["prefinalization"],
            expected_receipt_inventory_sha256=owner[
                "receipt_inventory_sha256"
            ],
        )
        cursor_record = {
            "lineage": owner["lineage"],
            "state": "fresh",
            "sealed": sealed,
            "owner": owner,
            "position": 0,
            "eof": False,
        }
        cursor = _prepare_handle(
            _HistoricalWindowSpoolReconciliationCursor, cursor_record
        ); delivery_guard[1] = (cursor, cursor_record, sealed, owner)
        cursor_registry[id(cursor)] = (cursor, cursor_record)
        owner["reconciliation_cursor_opened"] = True
        owner["claimed_finalization"] = finalization
        return cursor

    def _mint_production_historical_window_capability_core(
        sealed: object,
        *,
        claim: Any,
        finalization: Any,
        reconciliation: Any,
        delivery_guard: List[Any],
    ) -> object:
        owner = _live_record(
            sealed, _SealedHistoricalWindowExchangeSpool, sealed_registry
        )
        delivery_guard[0] = (sealed, owner)
        binding = owner.get("binding")
        entry = binding_registry.get(id(binding))
        if (
            owner["state"] != "sealed"
            or owner.get("lane") is not production_lane
            or not owner.get("source_bound")
            or not owner.get("reconciliation_read_complete")
            or owner.get("claimed_finalization") is not finalization
            or entry is None
            or entry[0] is not binding
        ):
            _raise_storage_error()
        binding_record = entry[1]
        if (
            binding_record.get("claim") is not claim
            or binding_record.get("owner_kind") != "sealed"
            or binding_record.get("owner_handle") is not sealed
            or binding_record.get("owner_generation")
            != owner.get("owner_generation")
            or type(reconciliation)
            is not binding_record.get("scan_reconciliation_class")
        ):
            _raise_storage_error()
        _verify_sealed_bound_source_current(sealed, owner)
        verifier = binding_record.get("rpc_finalization_verifier")
        if not callable(verifier):
            _raise_storage_error()
        verifier(
            claim=claim,
            finalization=finalization,
            expected_prefinalization=owner["prefinalization"],
            expected_receipt_inventory_sha256=owner[
                "receipt_inventory_sha256"
            ],
        )
        verifier = binding_record.get("scan_reconciliation_verifier")
        if not callable(verifier):
            _raise_storage_error()
        verifier(
            reconciliation=reconciliation,
            expected_spool_identity=sealed,
            expected_finalization_identity=finalization,
        )
        capability = _prepare_handle(
            _ProductionHistoricalWindowCapability, owner
        ); delivery_guard[1] = (capability, owner)
        next_generation = owner["owner_generation"] + 1
        nonowner = {
            "constructor": constructor_provenance,
            "state": "capability_nonowning",
        }
        owner["state"] = "capability"
        owner["owner_generation"] = next_generation
        owner["reconciliation"] = reconciliation
        capability_registry[id(capability)] = (capability, owner)
        _retire_nonowner_handle(
            sealed, sealed_registry, sealed_tombstones
        )
        binding_record["owner_kind"] = "capability"
        binding_record["owner_handle"] = capability
        binding_record["owner_generation"] = next_generation
        return capability

    class _SealedHistoricalWindowExchangeSpool(sealed_base):
        __slots__ = ("__weakref__",)

        def _open_reconciliation_cursor_from_bound_scan(
            self,
            *,
            claim: Any,
            finalization: Any,
        ) -> "_HistoricalWindowSpoolReconciliationCursor":
            delivery_guard = [None, None]
            try:
                result = _open_reconciliation_cursor_core(
                    self,
                    claim=claim,
                    finalization=finalization,
                    delivery_guard=delivery_guard,
                )
                return result
            except BaseException as error:
                guarded = delivery_guard[1]
                if guarded is not None:
                    cursor, cursor_record, sealed, owner = guarded
                    control, _ordinary = _terminalize_sealed(sealed, owner)
                    cursor_record["state"] = "closed"
                    if not isinstance(error, Exception):
                        raise error
                    if control is not None:
                        raise control
                    _raise_storage_error()
                attempted = delivery_guard[0]
                if attempted is None:
                    raise
                control, _ordinary = _terminalize_sealed(
                    attempted[0], attempted[1]
                )
                if not isinstance(error, Exception):
                    raise error
                if control is not None:
                    raise control
                raise error
            raise _InternalFailure()

        def mint_production_historical_window_capability(
            self,
            *,
            claim: Any,
            finalization: Any,
            reconciliation: Any,
        ) -> "_ProductionHistoricalWindowCapability":
            delivery_guard = [None, None]
            try:
                result = _mint_production_historical_window_capability_core(
                    self,
                    claim=claim,
                    finalization=finalization,
                    reconciliation=reconciliation,
                    delivery_guard=delivery_guard,
                )
                return result
            except BaseException as error:
                moved = delivery_guard[1]
                if moved is not None:
                    _fail_moved_owner_delivery(
                        moved[0],
                        moved[1],
                        capability_registry,
                        "closed_nonowning",
                        error,
                    )
                attempted = delivery_guard[0]
                if attempted is None:
                    raise
                cleanup_control, _cleanup_ordinary = _terminalize_sealed(
                    attempted[0], attempted[1]
                )
                if not isinstance(error, Exception):
                    raise error
                if cleanup_control is not None:
                    raise cleanup_control
                raise error
            raise _InternalFailure()

        def reread_exchange(
            self,
            *,
            receipt: "_HistoricalWindowSpoolReceipt",
        ) -> Tuple[bytes, bytes]:
            try:
                result = _sealed_reread_exchange(self, receipt)
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                _public_sealed_control_failure(self, error)
            raise _InternalFailure()

        def close(self) -> None:
            if type(self) is not _SealedHistoricalWindowExchangeSpool:
                _raise_storage_error()
            entry = sealed_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self, _SealedHistoricalWindowExchangeSpool, sealed_tombstones
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            if record.get("constructor") is not constructor_provenance:
                _raise_storage_error()
            if record["state"] == "capability_nonowning":
                return None
            if record["state"] == "closing":
                if type(record.get("_terminal_state")) is dict:
                    return None
                _raise_storage_error()
            if record["state"] != "sealed":
                _raise_storage_error()
            try:
                control, ordinary = _terminalize_sealed(self, record)
                if control is not None:
                    raise control
                if ordinary:
                    _raise_storage_error()
                result = None
                return result
            except BaseException as error:
                if isinstance(error, Exception):
                    raise
                terminal = record.get("_terminal_state")
                if type(terminal) is not dict or terminal.get("phase") != "done":
                    _terminalize_sealed(self, record)
                raise error

    sealed_authorized[0] = _SealedHistoricalWindowExchangeSpool

    def _consume_production_historical_window_capability_core(
        capability: object,
        delivery_guard: List[Any],
    ) -> object:
        owner = _live_record(
            capability,
            _ProductionHistoricalWindowCapability,
            capability_registry,
        )
        if owner["state"] != "capability":
            _raise_storage_error()
        binding = owner.get("binding")
        entry = binding_registry.get(id(binding))
        if (
            entry is None
            or entry[0] is not binding
            or entry[1].get("owner_kind") != "capability"
            or entry[1].get("owner_handle") is not capability
            or entry[1].get("owner_generation")
            != owner.get("owner_generation")
        ):
            _raise_storage_error()
        try:
            _verify_bound_source_current(entry[1])
        except BaseException as error:
            cleanup_error = None
            try:
                _close_moved_owner(
                    capability,
                    owner,
                    capability_registry,
                    "closed_nonowning",
                )
            except BaseException as observed:
                cleanup_error = observed
            if not isinstance(error, Exception):
                raise error
            if cleanup_error is not None and not isinstance(
                cleanup_error, Exception
            ):
                raise cleanup_error
            raise
        view = _prepare_handle(
            _ConsumedProductionHistoricalWindowCapabilityView, owner
        ); delivery_guard[0] = (view, owner)
        next_generation = owner["owner_generation"] + 1
        nonowner = {
            "constructor": constructor_provenance,
            "state": "consumed_nonowning",
        }
        owner["state"] = "consumed_view"
        owner["owner_generation"] = next_generation
        consumed_view_registry[id(view)] = (view, owner)
        _retire_nonowner_handle(
            capability, capability_registry, capability_tombstones
        )
        entry[1]["owner_kind"] = "consumed_view"
        entry[1]["owner_handle"] = view
        entry[1]["owner_generation"] = next_generation
        return view

    def consume_production_historical_window_capability(
        *, capability: "_ProductionHistoricalWindowCapability"
    ) -> "_ConsumedProductionHistoricalWindowCapabilityView":
        delivery_guard = [None]
        try:
            result = _consume_production_historical_window_capability_core(
                capability, delivery_guard
            )
            return result
        except BaseException as error:
            moved = delivery_guard[0]
            if moved is None:
                raise
            _fail_moved_owner_delivery(
                moved[0],
                moved[1],
                consumed_view_registry,
                "closed_nonowning",
                error,
            )
        raise _InternalFailure()

    def _close_moved_owner(
        handle: object,
        owner: Dict[str, Any],
        registry: Dict[int, Tuple[object, Dict[str, Any]]],
        closed_state: str,
    ) -> None:
        terminal = owner.get("_moved_terminal_state")
        while True:
            try:
                if type(terminal) is not dict:
                    owner["_moved_terminal_state"] = terminal = {
                        "phase": "revoke",
                        "control": None,
                        "ordinary": False,
                    }
                    owner["state"] = "closing"
                phase = terminal["phase"]
                if phase == "revoke":
                    _revoke_bound_source(owner)
                    terminal["phase"] = "retire"
                    continue
                if phase == "retire":
                    _retire_lineage(
                        owner,
                        preserve_handle=handle,
                        preserve_registry=registry,
                    )
                    terminal["phase"] = "cleanup"
                    continue
                if phase == "cleanup":
                    cleanup_control, cleanup_ordinary = _cleanup_resources(
                        owner, created=True
                    )
                    if terminal["control"] is None:
                        terminal["control"] = cleanup_control
                    terminal["ordinary"] = (
                        terminal["ordinary"] or cleanup_ordinary
                    )
                    terminal["phase"] = "release"
                    continue
                if phase == "release":
                    if registry is capability_registry:
                        tombstones = capability_tombstones
                    elif registry is consumed_view_registry:
                        tombstones = consumed_view_tombstones
                    else:
                        raise _InternalFailure()
                    _retire_nonowner_handle(handle, registry, tombstones)
                    terminal["phase"] = "done"
                    continue
                if phase == "done":
                    if terminal["control"] is not None:
                        raise terminal["control"]
                    if terminal["ordinary"]:
                        _raise_storage_error()
                    return None
                raise _InternalFailure()
            except BaseException as error:
                if type(terminal) is not dict:
                    raise
                terminal["control"], terminal["ordinary"] = _capture_failure(
                    terminal["control"], terminal["ordinary"], error
                )
                if terminal["phase"] == "done":
                    if terminal["control"] is not None:
                        raise terminal["control"]
                    if terminal["ordinary"]:
                        _raise_storage_error()

    def _install_open_handles(
        spool: object,
        owner_record: Dict[str, Any],
        quota: object,
        quota_record: Dict[str, Any],
    ) -> None:
        quota_registry[id(quota)] = (quota, quota_record)
        active_registry[id(spool)] = (spool, owner_record)

    def _discard_open_handles(
        spool: object,
        owner_record: Dict[str, Any],
        quota: object,
        quota_record: Optional[Dict[str, Any]],
    ) -> None:
        active_entry = active_registry.get(id(spool))
        if (
            active_entry is not None
            and active_entry[0] is spool
            and active_entry[1] is owner_record
        ):
            active_registry.pop(id(spool), None)
        quota_entry = quota_registry.get(id(quota))
        if (
            quota_entry is not None
            and quota_entry[0] is quota
            and quota_entry[1] is quota_record
        ):
            quota_registry.pop(id(quota), None)

    def _install_transfer_transition(
        spool: object,
        prior_owner: Dict[str, Any],
        issuing_owner: Dict[str, Any],
        final_owner: Dict[str, Any],
        transfer: object,
        transfer_record: Dict[str, Any],
    ) -> None:
        current = active_registry.get(id(spool))
        if (
            current is None
            or current[0] is not spool
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        active_registry[id(spool)] = (spool, issuing_owner)
        transfer_registry[id(transfer)] = (transfer, transfer_record)
        active_registry[id(spool)] = (spool, final_owner)

    def _open_historical_window_exchange_spool(
        *,
        data_dir: Path,
    ) -> "_HistoricalWindowExchangeSpool":
        record = {
            "chain": (),
            "acquiring_fds": [],
            "file_fd": None,
            "basename": None,
            "file_identity": None,
        }
        control = None
        ordinary = False
        spool = None
        quota = None
        quota_record = None
        try:
            canonical, components = _canonical_data_dir(data_dir)
            record["chain"] = _open_ancestry(
                canonical, components, record["acquiring_fds"]
            )
            record["acquiring_fds"] = []
            _require_private_leaf(record["chain"])
            random_bytes = os.urandom(16)
            if type(random_bytes) is not bytes or len(random_bytes) != 16:
                raise _InternalFailure()
            basename = ".historical-foundry-exchange-spool-{}.bin".format(
                random_bytes.hex()
            )
            _require_relative_basename(basename)
            record["basename"] = basename
            flags = (
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | os.O_CLOEXEC
            )
            record["file_fd"] = os.open(
                basename, flags, 0o600, dir_fd=record["chain"][-1][0]
            )
            opened = os.fstat(record["file_fd"])
            current = os.stat(
                basename,
                dir_fd=record["chain"][-1][0],
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(opened.st_mode)
                or not stat.S_ISREG(current.st_mode)
                or _file_identity(opened) != _file_identity(current)
                or opened.st_nlink != 1
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_size != 0
            ):
                raise _InternalFailure()
            record["file_identity"] = _file_identity(opened)
            os.fsync(record["chain"][-1][0])
            record["chain"] = _resnapshot_leaf(record["chain"])
            _verify_file_entry(record, expected_size=0)

            lineage = object()
            quota_record = {
                "lineage": lineage,
                "committed_physical_bytes": 0,
                "committed_members": 0,
                "provisional_physical_bytes": 0,
                "provisional_members": 0,
                "reservation": None,
            }
            quota = _prepare_handle(_HistoricalWindowRunQuota, quota_record)
            record.update(
                {
                    "state": "active",
                    "mode": "normal",
                    "lane": None,
                    "source_bound": False,
                    "lineage": lineage,
                    "quota": quota,
                    "inventory": [],
                    "committed_eof": 0,
                    "next_exchange_index": 1,
                    "next_member_index": 1,
                    "live_transfer": None,
                    "pending": None,
                    "receipt_inventory_sha256": None,
                    "owner_generation": 1,
                    "binding": None,
                    "prefinalization": None,
                    "prefinalization_digests": None,
                    "claimed_finalization": None,
                    "reconciliation_cursor_opened": False,
                    "reconciliation_read_complete": False,
                }
            )
            spool = _prepare_handle(_HistoricalWindowExchangeSpool, record)
            _install_open_handles(
                spool, record, quota, quota_record
            )
            result = spool
            return result
        except BaseException as error:
            control, ordinary = _capture_failure(control, ordinary, error)
        _discard_open_handles(spool, record, quota, quota_record)
        cleanup_control, cleanup_ordinary = _cleanup_resources(
            record, created=record.get("file_fd") is not None
        )
        if control is None:
            control = cleanup_control
        ordinary = ordinary or cleanup_ordinary
        if control is not None:
            raise control
        del ordinary
        _raise_storage_error()

    def _issue_historical_window_exchange_transfer_for_test(
        *,
        spool: "_HistoricalWindowExchangeSpool",
        exchange_projection: Mapping[str, Any],
        canonical_request_bytes: bytes,
        decoded_response_bytes: bytes,
    ) -> "_ProductionArchiveRpcExchangeTransfer":
        record = _live_record(
            spool, _HistoricalWindowExchangeSpool, active_registry
        )
        if (
            record["state"] != "active"
            or record["mode"] != "normal"
            or record["source_bound"]
            or record["lane"] not in (None, test_lane)
            or record["live_transfer"] is not None
            or record["pending"] is not None
        ):
            _raise_storage_error()
        projection = _validate_exchange_projection(
            exchange_projection,
            canonical_request_bytes,
            decoded_response_bytes,
        )
        exchange_token = object()
        transfer_record = {
            "lineage": record["lineage"],
            "lane": test_lane,
            "exchange": exchange_token,
            "exchange_index": projection["exchange_index"],
            "state": "issued",
            "projection": projection,
            "canonical_request_bytes": bytes(canonical_request_bytes),
            "decoded_response_bytes": bytes(decoded_response_bytes),
        }
        transfer = _prepare_handle(
            _ProductionArchiveRpcExchangeTransfer, transfer_record
        )
        issuing_owner = dict(record)
        issuing_owner["state"] = "issuing"
        final_owner = dict(record)
        final_owner["lane"] = test_lane
        final_owner["live_transfer"] = transfer
        control = None
        ordinary = False
        try:
            _install_transfer_transition(
                spool,
                record,
                issuing_owner,
                final_owner,
                transfer,
                transfer_record,
            )
            result = transfer
            return result
        except BaseException as error:
            control, ordinary = _capture_failure(control, ordinary, error)
        transfer_entry = transfer_registry.get(id(transfer))
        if (
            transfer_entry is not None
            and transfer_entry[0] is transfer
            and transfer_entry[1] is transfer_record
        ):
            transfer_registry.pop(id(transfer), None)
        active_entry = active_registry.get(id(spool))
        if (
            active_entry is not None
            and active_entry[0] is spool
            and (
                active_entry[1] is issuing_owner
                or active_entry[1] is final_owner
            )
        ):
            active_registry[id(spool)] = (spool, record)
        transfer_record["canonical_request_bytes"] = None
        transfer_record["decoded_response_bytes"] = None
        transfer_record["state"] = "consumed"
        if control is not None:
            raise control
        del ordinary
        _raise_storage_error()

    def _get_historical_window_run_quota_for_test(
        *,
        spool: "_HistoricalWindowExchangeSpool",
    ) -> "_HistoricalWindowRunQuota":
        record = _live_record(
            spool, _HistoricalWindowExchangeSpool, active_registry
        )
        if (
            record["state"] != "active"
            or record["mode"] != "normal"
            or record["source_bound"]
            or record["lane"] is not test_lane
        ):
            _raise_storage_error()
        _quota_record_for_owner(record)
        return record["quota"]

    def _project_historical_window_exchange_spool_for_test(
        *,
        spool_or_sealed: object,
    ) -> Mapping[str, Any]:
        if type(spool_or_sealed) is _HistoricalWindowExchangeSpool:
            entry = active_registry.get(id(spool_or_sealed))
            if entry is not None and entry[0] is spool_or_sealed:
                record = entry[1]
                if (
                    record.get("constructor") is not constructor_provenance
                    or record["state"] != "active"
                    or record["mode"] not in ("normal", "quota_test_only")
                    or record["lane"] is not test_lane
                    or record["source_bound"]
                ):
                    _raise_storage_error()
                state_value = (
                    "quota_test_only"
                    if record["mode"] == "quota_test_only"
                    else "active"
                )
                return _projection_for_record(record, state_value)
            return _projection_from_audit(
                spool_or_sealed,
                _HistoricalWindowExchangeSpool,
                active_tombstones,
                active_audits,
            )
        if type(spool_or_sealed) is _SealedHistoricalWindowExchangeSpool:
            entry = sealed_registry.get(id(spool_or_sealed))
            if entry is not None and entry[0] is spool_or_sealed:
                record = entry[1]
                if (
                    record.get("constructor") is not constructor_provenance
                    or record["state"] != "sealed"
                    or record["lane"] is not test_lane
                    or record["source_bound"]
                ):
                    _raise_storage_error()
                return _projection_for_record(record, "sealed")
            return _projection_from_audit(
                spool_or_sealed,
                _SealedHistoricalWindowExchangeSpool,
                sealed_tombstones,
                sealed_audits,
            )
        _raise_storage_error()

    return (
        _ProductionArchiveRpcExchangeTransfer,
        _PendingHistoricalWindowSpoolReceipt,
        _HistoricalWindowSpoolReceipt,
        _ProductionHistoricalWindowCapability,
        _ConsumedProductionHistoricalWindowCapabilityView,
        _HistoricalWindowSpoolSourceBinding,
        _HistoricalWindowRunQuota,
        _HistoricalWindowExchangeSpool,
        _SealedHistoricalWindowExchangeSpool,
        _HistoricalWindowSpoolReconciliationCursor,
        consume_production_historical_window_capability,
        _open_historical_window_exchange_spool,
        _issue_historical_window_exchange_transfer_for_test,
        _get_historical_window_run_quota_for_test,
        _project_historical_window_exchange_spool_for_test,
    )


(
    _ProductionArchiveRpcExchangeTransfer,
    _PendingHistoricalWindowSpoolReceipt,
    _HistoricalWindowSpoolReceipt,
    _ProductionHistoricalWindowCapability,
    _ConsumedProductionHistoricalWindowCapabilityView,
    _HistoricalWindowSpoolSourceBinding,
    _HistoricalWindowRunQuota,
    _HistoricalWindowExchangeSpool,
    _SealedHistoricalWindowExchangeSpool,
    _HistoricalWindowSpoolReconciliationCursor,
    consume_production_historical_window_capability,
    _open_historical_window_exchange_spool,
    _issue_historical_window_exchange_transfer_for_test,
    _get_historical_window_run_quota_for_test,
    _project_historical_window_exchange_spool_for_test,
) = _initialize_historical_foundry_storage_types()
del _initialize_historical_foundry_storage_types
