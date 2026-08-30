from __future__ import annotations

from collections.abc import Mapping as MappingABC
import gzip
import hashlib
import io
import json
from pathlib import Path
import os
import stat
import sys
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple
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


def _plan_historical_raw_chunk_append(
    *,
    current_chunk_byte_count: int,
    request_byte_count: int,
    decoded_byte_count: int,
) -> Tuple[str, int]:
    values = (
        current_chunk_byte_count, request_byte_count, decoded_byte_count,
    )
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError("historical raw chunk size is invalid")
    if (
        current_chunk_byte_count > 16_777_216
        or request_byte_count > 4_194_304
        or decoded_byte_count > 8_388_608
    ):
        raise ValueError("historical raw chunk size is invalid")
    frame_size = 16 + request_byte_count + decoded_byte_count
    resulting_size = current_chunk_byte_count + frame_size
    if resulting_size <= 16_777_216:
        return ("append_current", resulting_size)
    if current_chunk_byte_count == 0:
        raise ValueError("historical raw chunk size is invalid")
    return ("flush_then_append", frame_size)


def _require_historical_capture_inventory_size(*, byte_count: int) -> int:
    if type(byte_count) is not int or not 0 <= byte_count <= 16_777_216:
        raise ValueError("historical capture inventory size is invalid")
    return byte_count


def _require_historical_gzip_member_size(*, byte_count: int) -> int:
    if type(byte_count) is not int or not 0 <= byte_count <= 16_842_752:
        raise ValueError("historical gzip member size is invalid")
    return byte_count


def _plan_historical_typed_root_append(
    *,
    current_decoded_size: int,
    current_row_count: int,
    candidate_row_encoded_lengths: Tuple[int, ...],
) -> Tuple[str, int]:
    if (
        type(current_decoded_size) is not int
        or type(current_row_count) is not int
        or current_decoded_size < 0
        or current_row_count < 0
        or type(candidate_row_encoded_lengths) is not tuple
        or not candidate_row_encoded_lengths
        or any(
            type(length) is not int or length <= 0
            for length in candidate_row_encoded_lengths
        )
    ):
        raise ValueError("historical typed root size is invalid")
    candidate_size = (
        2 + sum(candidate_row_encoded_lengths)
        + len(candidate_row_encoded_lengths) - 1
    )
    if candidate_size > 16_777_216 or current_decoded_size > 16_777_216:
        raise ValueError("historical typed root size is invalid")
    if current_row_count == 0:
        if current_decoded_size != 2:
            raise ValueError("historical typed root size is invalid")
        return ("append_current", candidate_size)
    if current_decoded_size < 3:
        raise ValueError("historical typed root size is invalid")
    resulting_size = (
        current_decoded_size + sum(candidate_row_encoded_lengths)
        + len(candidate_row_encoded_lengths)
    )
    if resulting_size <= 16_777_216:
        return ("append_current", resulting_size)
    return ("flush_then_append", candidate_size)


def _initialize_historical_foundry_storage_types():
    from collections import defaultdict as captured_defaultdict
    from collections import deque as captured_deque
    from functools import partial as captured_partial
    from itertools import chain as captured_chain

    class _InternalFailure(Exception):
        __slots__ = ()

    class _Task4bReplayMismatch(Exception):
        __slots__ = ()

    captured_raw_chunk_append = _plan_historical_raw_chunk_append
    captured_capture_inventory_size = _require_historical_capture_inventory_size
    captured_gzip_member_size = _require_historical_gzip_member_size
    captured_typed_root_append = _plan_historical_typed_root_append
    captured_source_sha256 = hashlib.sha256
    constructor_provenance = object()
    atomic_open_slot_key = object()
    atomic_close_journal_key = object()
    atomic_close_callable_key = object()
    close_entered_token = object()
    close_returned_token = object()
    captured_map = map
    test_lane = object()
    production_lane = object()

    transfer_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    pending_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    receipt_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    capability_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    consumed_view_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    staging_snapshot_registry: Dict[
        int, Tuple[object, Dict[str, Any]]
    ] = {}
    replay_source_registry: Dict[
        int, Tuple[object, Dict[str, Any]]
    ] = {}
    binding_registry: Dict[int, Tuple[object, Dict[str, Any]]] = {}
    task4b_checker_registry: Dict[
        int, Tuple[object, Callable[[], None]]
    ] = {}
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
    staging_snapshot_tombstones: Dict[
        int, Tuple[weakref.ReferenceType, int]
    ] = {}
    replay_source_tombstones: Dict[
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
    task4b_provisional_join_keys = receipt_keys + (
        "segment", "segment_local_index", "leaf_index",
        "wire_hash_authority", "raw_chunk_path", "raw_chunk_offset",
    )
    task4b_final_join_keys = task4b_provisional_join_keys + (
        "typed_role", "typed_chunk_refs",
    )
    task4b_typed_roles = ("headers", "reserves", "prices", "fees")
    task4b_semantic_roles = (
        "anchor_stage", "lower_observation",
        "headers", "reserves", "prices", "fees", "final_anchor",
    )
    task4b_typed_domains = {
        "headers": b"historical_foundry_header_inventory/v1",
        "reserves": b"historical_foundry_reserve_inventory/v1",
        "prices": b"historical_foundry_price_inventory/v1",
        "fees": b"historical_foundry_fee_inventory/v1",
    }
    task5_prefilter_domain = b"historical_foundry_prefilter_grid/v1"
    task4b_post_leaf_keys = (
        "schema", "segment", "segment_local_index", "leaf_index",
        "request_ids", "request_count", "canonical_request_sha256",
        "response_ids", "exchange_index", "logical_batch_index",
        "attempt_index", "request_byte_count", "decoded_byte_count",
        "decoded_sha256", "wire_byte_count", "wire_sha256",
        "wire_hash_authority", "spool_member_index", "spool_offset",
        "spool_length", "spool_member_sha256",
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
    replay_source_base, replay_source_authorized = _new_authority_base()
    staging_snapshot_base, staging_snapshot_authorized = _new_authority_base()
    run_snapshot_base, run_snapshot_authorized = _new_authority_base()

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
            capture_ledger = record.get("_task4b_staging")
            capture_state = (
                capture_ledger.get("cleanup_state")
                if type(capture_ledger) is dict else None
            )
            if (
                type(capture_state) is dict
                and capture_state.get("phase") == "done"
            ):
                capture_control, capture_ordinary = None, False
            else:
                capture_control, capture_ordinary = (
                    _cleanup_task4b_capture_staging(record)
                )
            record["_cleanup_state"] = cleanup = {
                "phase": "verify",
                "created": bool(created),
                "verified": False,
                "unlink_attempted": False,
                "fsync_attempted": False,
                "attempted_fds": set(),
                "control": capture_control,
                "ordinary": capture_ordinary,
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

    def _drop_task4b_checker(
        binding: Any, binding_record: Dict[str, Any]
    ) -> None:
        if binding is not None:
            checker_entry = task4b_checker_registry.get(id(binding))
            if checker_entry is not None and checker_entry[0] is binding:
                task4b_checker_registry.pop(id(binding), None)
        binding_record["task4b_currentness_checker"] = None

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
            staging_snapshot_registry,
            replay_source_registry,
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
                    if registry is binding_registry:
                        _drop_task4b_checker(_handle, candidate)
                    if registry is replay_source_registry:
                        raw_builder = candidate.get("raw_builder")
                        if type(raw_builder) is bytearray:
                            raw_builder.clear()
                        for raw_name in (
                            "raw_builder_rows",
                            "raw_chunks",
                            "raw_exchange_records",
                            "exchange_joins",
                            "post_roots",
                            "root_records",
                            "typed_chunks",
                        ):
                            raw_value = candidate.get(raw_name)
                            if type(raw_value) is list:
                                raw_value.clear()
                        typed_builder = candidate.get("typed_builder")
                        if type(typed_builder) is dict:
                            typed_rows = typed_builder.get("row_bytes")
                            if type(typed_rows) is list:
                                typed_rows.clear()
                            typed_builder.clear()
                        candidate["compact_rows"] = None
                        candidate["state"] = "closed"
                        candidate["source"] = None
                        candidate["view"] = None
                        candidate["owner"] = None
                        candidate["binding"] = None
                        candidate["reconciliation"] = None
                        _retire_nonowner_handle(
                            _handle,
                            replay_source_registry,
                            replay_source_tombstones,
                        )
                        continue
                    if registry is cursor_registry:
                        candidate["state"] = "closed"
                        _retire_nonowner_handle(
                            _handle, cursor_registry, cursor_tombstones
                        )
                        continue
                    registry.pop(handle_id, None)
        record["capture_replay_source"] = None
        record.pop("_task4b_raw_chunks", None)
        record.pop("_task4b_raw_exchange_records", None)
        record.pop("_task4b_exchange_joins", None)
        record.pop("_task4b_typed_chunks", None)
        record.pop("_task4b_capture_phase", None)

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

    def _task4b_exact_directory_identity(
        details: os.stat_result,
    ) -> Tuple[int, ...]:
        identity = _metadata_snapshot(details)
        if (
            type(identity) is not tuple
            or len(identity) != 6
            or not all(type(value) is int for value in identity)
        ):
            raise _BoundSourceIdentityDrift()
        return identity

    def _task4b_verify_repository_root(
        binding_record: Dict[str, Any],
    ) -> int:
        rows = binding_record.get("ancestry_rows")
        if type(rows) is not tuple or not rows:
            raise _BoundSourceIdentityDrift()
        row = rows[0]
        if type(row) is not tuple or len(row) != 5:
            raise _BoundSourceIdentityDrift()
        components, fd, parent_index, name, identity = row
        if (
            components != ()
            or type(fd) is not int
            or parent_index is not None
            or name is not None
            or type(identity) is not tuple
            or len(identity) != 6
            or not all(type(value) is int for value in identity)
            or os.get_inheritable(fd)
        ):
            raise _BoundSourceIdentityDrift()
        try:
            opened = os.fstat(fd)
            current = os.stat(
                ".", dir_fd=fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or _task4b_exact_directory_identity(opened) != identity
            or _task4b_exact_directory_identity(current) != identity
            or opened.st_uid != os.geteuid()
            or current.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
            or stat.S_IMODE(current.st_mode) & 0o022
        ):
            raise _BoundSourceIdentityDrift()
        return fd

    def _task4b_directory_flags() -> int:
        flags = _required_directory_flags()
        if (
            os.mkdir not in os.supports_dir_fd
            or os.rmdir not in os.supports_dir_fd
        ):
            raise _InternalFailure()
        return flags

    def _task4b_file_flags(*, create: bool) -> int:
        if not all(
            hasattr(os, name) for name in ("O_NOFOLLOW", "O_CLOEXEC")
        ):
            raise _InternalFailure()
        if os.open not in os.supports_dir_fd:
            raise _InternalFailure()
        if create:
            return (
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | os.O_CLOEXEC
            )
        return os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC

    def _task4b_new_capture_ledger() -> Dict[str, Any]:
        return {
            "transient_fds": [],
            "files": [],
            "directories": [],
            "role_directories": {},
            "cleanup_state": None,
        }

    def _task4b_registered_slot_fd(slot: Dict[str, Any]) -> Optional[int]:
        if type(slot) is not dict:
            raise _InternalFailure()
        close_journal = slot.get(atomic_close_journal_key)
        if close_journal is not None:
            if type(close_journal) is not captured_deque:
                raise _InternalFailure()
            if close_journal:
                return None
        direct = slot.get("fd")
        holder = slot.get(atomic_open_slot_key)
        held = None
        if holder is not None:
            if type(holder) is not captured_defaultdict:
                raise _InternalFailure()
            factory = holder.default_factory
            if factory is not None and type(factory) is not captured_partial:
                raise _InternalFailure()
            held = holder.get("fd")
        if direct is not None and type(direct) is not int:
            raise _InternalFailure()
        if held is not None and type(held) is not int:
            raise _InternalFailure()
        if type(direct) is int and type(held) is int and direct != held:
            raise _InternalFailure()
        return direct if type(direct) is int else held

    def _task4b_open_registered_slot(
        slot: Dict[str, Any],
        path: str,
        flags: int,
        *,
        dir_fd: int,
        mode: Optional[int] = None,
    ) -> int:
        if (
            type(slot) is not dict
            or type(path) is not str
            or type(flags) is not int
            or type(dir_fd) is not int
            or (mode is not None and type(mode) is not int)
        ):
            raise _InternalFailure()
        opener = os.open
        if opener not in os.supports_dir_fd:
            raise _InternalFailure()
        if mode is None:
            factory = captured_partial(
                opener, path, flags, dir_fd=dir_fd
            )
        else:
            factory = captured_partial(
                opener, path, flags, mode, dir_fd=dir_fd
            )
        if type(factory) is not captured_partial:
            raise _InternalFailure()
        holder = captured_defaultdict(factory)
        if (
            type(holder) is not captured_defaultdict
            or holder.default_factory is not factory
        ):
            raise _InternalFailure()
        slot[atomic_open_slot_key] = holder
        fd = holder["fd"]
        holder.default_factory = None
        if type(fd) is not int:
            raise _InternalFailure()
        slot["fd"] = fd
        return fd

    def _task4b_transient_fd_slot(
        ledger: Dict[str, Any]
    ) -> Dict[str, Any]:
        slot = {
            "fd": None,
            "acquisition_state": "pending",
            "close_state": "pending",
        }
        ledger["transient_fds"].append(slot)
        return slot

    def _task4b_close_fd_slot(
        ledger: Dict[str, Any], slot: Dict[str, Any]
    ) -> None:
        if type(ledger) is not dict or type(slot) is not dict:
            raise _InternalFailure()
        holder = slot.get(atomic_open_slot_key)
        if holder is not None:
            if type(holder) is not captured_defaultdict:
                raise _InternalFailure()
            holder.default_factory = None
        close_state = slot.get("close_state")

        def release_close_authority(final_state: str) -> None:
            if final_state not in ("attempted", "unresolved"):
                raise _InternalFailure()
            slot["close_state"] = final_state
            slot["fd"] = None
            if holder is not None:
                holder["fd"] = None
            slot.pop(atomic_open_slot_key, None)
            slot.pop(atomic_close_callable_key, None)
            slot.pop(atomic_close_journal_key, None)
            return None

        if close_state in ("attempted", "unresolved"):
            release_close_authority(close_state)
            return None
        if close_state not in ("pending", "attempting"):
            raise _InternalFailure()

        journal = slot.get(atomic_close_journal_key)
        if journal is not None:
            if type(journal) is not captured_deque:
                raise _InternalFailure()
            journal_items = tuple(journal)
            if journal_items:
                if journal_items == (
                    close_entered_token,
                    None,
                    close_returned_token,
                ):
                    release_close_authority("attempted")
                    return None
                if journal_items in (
                    (close_entered_token,),
                    (close_entered_token, None),
                ):
                    release_close_authority("unresolved")
                    return None
                release_close_authority("unresolved")
                raise _InternalFailure()

        fd = _task4b_registered_slot_fd(slot)
        if type(fd) is not int:
            release_close_authority(
                "attempted" if close_state == "pending" else "unresolved"
            )
            return None

        closer = slot.get(atomic_close_callable_key)
        if closer is None:
            closer = os.close
            if not callable(closer):
                raise _InternalFailure()
            slot[atomic_close_callable_key] = closer
        elif not callable(closer):
            raise _InternalFailure()
        if journal is None:
            journal = captured_deque()
            if type(journal) is not captured_deque:
                raise _InternalFailure()
            slot[atomic_close_journal_key] = journal
        slot["close_state"] = "attempting"
        close_results = captured_map(closer, (fd,))
        if type(close_results) is not captured_map:
            raise _InternalFailure()
        close_sequence = captured_chain(
            (close_entered_token,),
            close_results,
            (close_returned_token,),
        )
        if type(close_sequence) is not captured_chain:
            raise _InternalFailure()
        journal.extend(close_sequence)
        if tuple(journal) != (
            close_entered_token,
            None,
            close_returned_token,
        ):
            release_close_authority("unresolved")
            raise _InternalFailure()
        release_close_authority("attempted")
        return None

    def _task4b_open_repository_config_directory(
        binding_record: Dict[str, Any],
        ledger: Dict[str, Any],
    ) -> Dict[str, Any]:
        root_fd = _task4b_verify_repository_root(binding_record)
        flags = _task4b_directory_flags()
        slot = _task4b_transient_fd_slot(ledger)
        try:
            before = os.stat(
                "config", dir_fd=root_fd, follow_symlinks=False
            )
            slot["acquisition_state"] = "attempting"
            fd = _task4b_open_registered_slot(
                slot, "config", flags, dir_fd=root_fd
            ); slot["acquisition_state"] = "attempted"
            opened = os.fstat(fd)
            after = os.stat(
                "config", dir_fd=root_fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        identity = _task4b_exact_directory_identity(opened)
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(after.st_mode)
            or _task4b_exact_directory_identity(before) != identity
            or _task4b_exact_directory_identity(after) != identity
            or opened.st_uid != os.geteuid()
            or before.st_uid != os.geteuid()
            or after.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
            or stat.S_IMODE(before.st_mode) & 0o022
            or stat.S_IMODE(after.st_mode) & 0o022
            or os.get_inheritable(fd)
        ):
            raise _BoundSourceIdentityDrift()
        _task4b_verify_repository_root(binding_record)
        return {
            "fd": fd,
            "root_fd": root_fd,
            "identity": identity,
            "slot": slot,
        }

    def _task4b_verify_repository_config_directory(
        binding_record: Dict[str, Any], directory: Dict[str, Any]
    ) -> None:
        root_fd = _task4b_verify_repository_root(binding_record)
        fd = directory.get("fd")
        if root_fd != directory.get("root_fd") or type(fd) is not int:
            raise _BoundSourceIdentityDrift()
        try:
            opened = os.fstat(fd)
            current = os.stat(
                "config", dir_fd=root_fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        identity = directory.get("identity")
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or _task4b_exact_directory_identity(opened) != identity
            or _task4b_exact_directory_identity(current) != identity
            or opened.st_uid != os.geteuid()
            or current.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
            or stat.S_IMODE(current.st_mode) & 0o022
            or os.get_inheritable(fd)
        ):
            raise _BoundSourceIdentityDrift()
        return None

    def _task4b_open_config_source(
        config_fd: int,
        basename: str,
        ledger: Dict[str, Any],
    ) -> Dict[str, Any]:
        _require_relative_basename(basename)
        slot = _task4b_transient_fd_slot(ledger)
        try:
            before = os.stat(
                basename, dir_fd=config_fd, follow_symlinks=False
            )
            slot["acquisition_state"] = "attempting"
            fd = _task4b_open_registered_slot(
                slot,
                basename,
                _task4b_file_flags(create=False),
                dir_fd=config_fd,
            ); slot["acquisition_state"] = "attempted"
            opened = os.fstat(fd)
            after = os.stat(
                basename, dir_fd=config_fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        identity = _bound_source_file_identity(opened)
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or _bound_source_file_identity(before) != identity
            or _bound_source_file_identity(after) != identity
            or opened.st_nlink != 1
            or before.st_nlink != 1
            or after.st_nlink != 1
            or opened.st_uid != os.geteuid()
            or before.st_uid != os.geteuid()
            or after.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
            or stat.S_IMODE(before.st_mode) & 0o022
            or stat.S_IMODE(after.st_mode) & 0o022
            or opened.st_size > 1_048_576
            or opened.st_size <= 0
            or os.get_inheritable(fd)
        ):
            raise _BoundSourceIdentityDrift()
        return {
            "basename": basename,
            "fd": fd,
            "identity": identity,
            "slot": slot,
            "size": opened.st_size,
        }

    def _task4b_read_config_source(
        config_fd: int, source: Dict[str, Any]
    ) -> bytes:
        fd = source["fd"]
        basename = source["basename"]
        identity = source["identity"]
        try:
            before_fd = os.fstat(fd)
            before_path = os.stat(
                basename, dir_fd=config_fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        if (
            _bound_source_file_identity(before_fd) != identity
            or _bound_source_file_identity(before_path) != identity
        ):
            raise _BoundSourceIdentityDrift()
        try:
            payload = _read_bound_source_fd(fd, source["size"])
        except OSError:
            raise _InternalFailure()
        try:
            after_fd = os.fstat(fd)
            after_path = os.stat(
                basename, dir_fd=config_fd, follow_symlinks=False
            )
        except OSError:
            raise _BoundSourceIdentityDrift()
        if (
            _bound_source_file_identity(after_fd) != identity
            or _bound_source_file_identity(after_path) != identity
            or type(payload) is not bytes
            or len(payload) != source["size"]
        ):
            raise _BoundSourceIdentityDrift()
        return payload

    def _task4b_decode_canonical_config(payload: bytes) -> Dict[str, Any]:
        def reject_pairs(pairs: Any) -> Dict[str, Any]:
            result = {}
            for key, value in pairs:
                if type(key) is not str or key in result:
                    raise _BoundSourceIdentityDrift()
                result[key] = value
            return result

        def reject_constant(_value: str) -> Any:
            raise _BoundSourceIdentityDrift()

        if (
            type(payload) is not bytes
            or not payload.endswith(b"\n")
            or payload.endswith(b"\n\n")
        ):
            raise _BoundSourceIdentityDrift()
        try:
            decoded = payload[:-1].decode("utf-8")
            value = json.loads(
                decoded,
                object_pairs_hook=reject_pairs,
                parse_int=int,
                parse_float=float,
                parse_constant=reject_constant,
            )
            canonical = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8") + b"\n"
        except _BoundSourceIdentityDrift:
            raise
        except Exception:
            raise _BoundSourceIdentityDrift()
        if type(value) is not dict or canonical != payload:
            raise _BoundSourceIdentityDrift()
        return value

    def _task4b_finalization_config_identity(
        owner: Dict[str, Any], binding_record: Dict[str, Any]
    ) -> Dict[str, str]:
        finalization = owner.get("claimed_finalization")
        if type(finalization) is not binding_record.get(
            "rpc_finalization_class"
        ):
            raise _BoundSourceIdentityDrift()
        try:
            identity = finalization["identity"]
            configs = identity["configs"]
        except Exception:
            raise _BoundSourceIdentityDrift()
        keys = (
            "policy_id",
            "policy_physical_sha256",
            "authority_physical_sha256",
            "toolchain_physical_sha256",
        )
        if (
            type(identity) is not dict
            or type(configs) is not dict
            or len(configs) != len(keys)
            or frozenset(configs) != frozenset(keys)
            or type(configs.get("policy_id")) is not str
            or not all(
                _exact_sha256(configs.get(key)) for key in keys[1:]
            )
        ):
            raise _BoundSourceIdentityDrift()
        return {key: configs[key] for key in keys}

    def _task4b_validate_config_set(
        rows: Tuple[Tuple[str, bytes, Dict[str, Any]], ...],
        owner: Dict[str, Any],
        binding_record: Dict[str, Any],
    ) -> Dict[str, Any]:
        if tuple(row[0] for row in rows) != (
            "policy", "authority", "toolchain"
        ):
            raise _BoundSourceIdentityDrift()
        values = {row[0]: _task4b_decode_canonical_config(row[1]) for row in rows}
        expected_schemas = {
            "policy": "historical_foundry_replay_policy/v1",
            "authority": "historical_foundry_replay_authority/v1",
            "toolchain": "historical_foundry_replay_toolchain/v1",
        }
        if any(
            values[role].get("schema") != schema
            for role, schema in expected_schemas.items()
        ):
            raise _BoundSourceIdentityDrift()
        digests = {
            role: hashlib.sha256(payload).hexdigest()
            for role, payload, _source in rows
        }
        sealed = _task4b_finalization_config_identity(
            owner, binding_record
        )
        if (
            digests["policy"] != sealed["policy_physical_sha256"]
            or digests["authority"]
            != sealed["authority_physical_sha256"]
            or digests["toolchain"]
            != sealed["toolchain_physical_sha256"]
            or values["policy"].get("authority_sha256")
            != digests["authority"]
            or values["policy"].get("toolchain_sha256")
            != digests["toolchain"]
            or sealed["policy_id"] != "policy:" + digests["policy"]
        ):
            raise _BoundSourceIdentityDrift()
        return {"values": values, "digests": digests, "sealed": sealed}

    def _task4b_open_capture_directory(
        ledger: Dict[str, Any],
        parent_fd: int,
        name: str,
        *,
        allow_existing: bool,
    ) -> Dict[str, Any]:
        _require_relative_basename(name)
        entry = {
            "parent_fd": parent_fd,
            "name": name,
            "fd": None,
            "identity": None,
            "created": False,
            "mkdir_state": "pending",
            "open_state": "pending",
            "reopen_state": "pending",
            "rmdir_state": "pending",
            "parent_fsync_state": "pending",
            "close_state": "pending",
            "cleanup_phase": "verify",
        }
        ledger["directories"].append(entry)
        entry["mkdir_state"] = "attempting"
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd); entry["created"] = True; entry["mkdir_state"] = "attempted"
        except FileExistsError:
            entry["mkdir_state"] = "attempted"
            if not allow_existing:
                raise
        if entry["created"]:
            entry["open_state"] = "attempting"
            open_error = None
            open_traceback = None
            try:
                _task4b_open_registered_slot(
                    entry,
                    name,
                    _task4b_directory_flags(),
                    dir_fd=parent_fd,
                ); entry["open_state"] = "attempted"
            except BaseException as observed_open_error:
                open_error = observed_open_error
                open_traceback = observed_open_error.__traceback__
                del observed_open_error
            if open_error is not None:
                identity_error = None
                identity_traceback = None
                try:
                    entry["identity"] = _metadata_snapshot(os.stat(
                        name, dir_fd=parent_fd, follow_symlinks=False
                    ))
                except BaseException as observed_identity_error:
                    identity_error = observed_identity_error
                    identity_traceback = observed_identity_error.__traceback__
                    del observed_identity_error
                retry_error = None
                retry_traceback = None
                if identity_error is not None:
                    try:
                        entry["identity"] = _metadata_snapshot(os.stat(
                            name, dir_fd=parent_fd, follow_symlinks=False
                        ))
                    except BaseException as observed_retry_error:
                        retry_error = observed_retry_error
                        retry_traceback = observed_retry_error.__traceback__
                        del observed_retry_error
                selected = open_error
                selected_traceback = open_traceback
                if isinstance(open_error, Exception):
                    if identity_error is not None and not isinstance(
                        identity_error, Exception
                    ):
                        selected = identity_error
                        selected_traceback = identity_traceback
                    elif retry_error is not None and not isinstance(
                        retry_error, Exception
                    ):
                        selected = retry_error
                        selected_traceback = retry_traceback
                raise selected.with_traceback(selected_traceback) from None
            before = os.stat(
                name, dir_fd=parent_fd, follow_symlinks=False
            )
            entry["identity"] = _metadata_snapshot(before)
        else:
            before = os.stat(
                name, dir_fd=parent_fd, follow_symlinks=False
            )
            entry["open_state"] = "attempting"
            _task4b_open_registered_slot(
                entry,
                name,
                _task4b_directory_flags(),
                dir_fd=parent_fd,
            ); entry["open_state"] = "attempted"
        fd = entry["fd"]
        opened = os.fstat(fd)
        after = os.stat(
            name, dir_fd=parent_fd, follow_symlinks=False
        )
        identity = _metadata_snapshot(opened)
        entry["identity"] = identity
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(after.st_mode)
            or _metadata_snapshot(before) != identity
            or _metadata_snapshot(after) != identity
            or opened.st_uid != os.geteuid()
            or before.st_uid != os.geteuid()
            or after.st_uid != os.geteuid()
            or os.get_inheritable(fd)
            or (
                entry["created"]
                and (
                    stat.S_IMODE(before.st_mode) != 0o700
                    or stat.S_IMODE(opened.st_mode) != 0o700
                    or stat.S_IMODE(after.st_mode) != 0o700
                )
            )
            or (
                not entry["created"]
                and (
                    stat.S_IMODE(before.st_mode) & 0o022
                    or stat.S_IMODE(opened.st_mode) & 0o022
                    or stat.S_IMODE(after.st_mode) & 0o022
                )
            )
        ):
            raise _InternalFailure()
        return entry

    def _task4b_write_capture_config(
        ledger: Dict[str, Any],
        staging_fd: int,
        target: str,
        payload: bytes,
        expected_value: Dict[str, Any],
        expected_digest: str,
    ) -> None:
        entry = {
            "parent_fd": staging_fd,
            "name": target,
            "fd": None,
            "identity": None,
            "acquisition_state": "pending",
            "unlink_state": "pending",
            "parent_fsync_state": "pending",
            "close_state": "pending",
            "cleanup_phase": "verify",
        }
        ledger["files"].append(entry)
        quota_token = _task4b_reserve_output_quota(
            ledger, entry, len(payload)
        )
        entry["acquisition_state"] = "attempting"
        fd = _task4b_open_registered_slot(
            entry,
            target,
            _task4b_file_flags(create=True),
            dir_fd=staging_fd,
            mode=0o600,
        ); entry["acquisition_state"] = "attempted"
        if os.get_inheritable(fd):
            raise _InternalFailure()
        _pwrite_all(fd, payload, 0)
        os.fsync(fd)
        before_fd = os.fstat(fd)
        before_path = os.stat(
            target, dir_fd=staging_fd, follow_symlinks=False
        )
        identity = _bound_source_file_identity(before_fd)
        entry["identity"] = identity
        if (
            not stat.S_ISREG(before_fd.st_mode)
            or not stat.S_ISREG(before_path.st_mode)
            or _bound_source_file_identity(before_path) != identity
            or before_fd.st_nlink != 1
            or before_path.st_nlink != 1
            or before_fd.st_uid != os.geteuid()
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_fd.st_mode) != 0o600
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_fd.st_size != len(payload)
        ):
            raise _InternalFailure()
        observed = _pread_exact(fd, len(payload), 0)
        after_fd = os.fstat(fd)
        after_path = os.stat(
            target, dir_fd=staging_fd, follow_symlinks=False
        )
        if (
            _bound_source_file_identity(after_fd) != identity
            or _bound_source_file_identity(after_path) != identity
            or observed != payload
            or hashlib.sha256(observed).hexdigest() != expected_digest
        ):
            raise _InternalFailure()
        try:
            observed_value = _task4b_decode_canonical_config(observed)
        except _BoundSourceIdentityDrift:
            raise _InternalFailure()
        if observed_value != expected_value:
            raise _InternalFailure()
        _task4b_close_fd_slot(ledger, entry)
        os.fsync(staging_fd)
        _task4b_commit_output_quota(ledger, entry, quota_token)
        return None

    def _task4b_verify_raw_chunk_payload(
        payload: bytes,
        frame_rows: Tuple[Dict[str, Any], ...],
    ) -> None:
        if type(payload) is not bytes or not payload:
            raise _InternalFailure()
        cursor = 0
        for frame_row in frame_rows:
            projection = frame_row.get("projection")
            if (
                type(frame_row) is not dict
                or type(projection) is not dict
                or tuple(projection) != receipt_keys
                or frame_row.get("raw_offset") != cursor
                or cursor + 16 > len(payload)
            ):
                raise _InternalFailure()
            request_length = int.from_bytes(
                payload[cursor:cursor + 8], "big"
            )
            request_start = cursor + 8
            request_stop = request_start + request_length
            if request_stop + 8 > len(payload):
                raise _InternalFailure()
            decoded_length = int.from_bytes(
                payload[request_stop:request_stop + 8], "big"
            )
            decoded_start = request_stop + 8
            frame_stop = decoded_start + decoded_length
            if frame_stop > len(payload):
                raise _InternalFailure()
            request = payload[request_start:request_stop]
            decoded = payload[decoded_start:frame_stop]
            frame = payload[cursor:frame_stop]
            if (
                request_length != projection["request_byte_count"]
                or decoded_length != projection["decoded_byte_count"]
                or hashlib.sha256(request).hexdigest()
                != projection["request_sha256"]
                or hashlib.sha256(decoded).hexdigest()
                != projection["decoded_sha256"]
                or len(frame) != projection["spool_length"]
                or hashlib.sha256(frame).hexdigest()
                != projection["spool_member_sha256"]
            ):
                raise _InternalFailure()
            cursor = frame_stop
        if cursor != len(payload):
            raise _InternalFailure()
        return None

    def _task4b_write_raw_chunk(
        ledger: Dict[str, Any],
        *,
        chunk_index: int,
        payload: bytes,
        frame_rows: Tuple[Dict[str, Any], ...],
    ) -> Dict[str, Any]:
        role_directories = ledger.get("role_directories")
        rpc_directory = (
            role_directories.get("rpc")
            if type(role_directories) is dict else None
        )
        if (
            type(chunk_index) is not int
            or chunk_index <= 0
            or type(payload) is not bytes
            or not payload
            or len(payload) > 16_777_216
            or type(frame_rows) is not tuple
            or not frame_rows
            or type(rpc_directory) is not dict
        ):
            raise _InternalFailure()
        _task4b_verify_capture_directory(rpc_directory)
        parent_fd = rpc_directory.get("fd")
        if type(parent_fd) is not int:
            raise _InternalFailure()
        target = "{:08d}.bin".format(chunk_index)
        _require_relative_basename(target)
        entry = {
            "parent_fd": parent_fd,
            "name": target,
            "fd": None,
            "identity": None,
            "acquisition_state": "pending",
            "unlink_state": "pending",
            "parent_fsync_state": "pending",
            "close_state": "pending",
            "cleanup_phase": "verify",
        }
        ledger["files"].append(entry)
        quota_token = _task4b_reserve_output_quota(
            ledger, entry, len(payload)
        )
        entry["acquisition_state"] = "attempting"
        fd = _task4b_open_registered_slot(
            entry,
            target,
            _task4b_file_flags(create=True),
            dir_fd=parent_fd,
            mode=0o600,
        ); entry["acquisition_state"] = "attempted"
        if os.get_inheritable(fd):
            raise _InternalFailure()
        _pwrite_all(fd, payload, 0)
        os.fsync(fd)
        before_fd = os.fstat(fd)
        before_path = os.stat(
            target, dir_fd=parent_fd, follow_symlinks=False
        )
        identity = _bound_source_file_identity(before_fd)
        entry["identity"] = identity
        if (
            not stat.S_ISREG(before_fd.st_mode)
            or not stat.S_ISREG(before_path.st_mode)
            or _bound_source_file_identity(before_path) != identity
            or before_fd.st_nlink != 1
            or before_path.st_nlink != 1
            or before_fd.st_uid != os.geteuid()
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_fd.st_mode) != 0o600
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_fd.st_size != len(payload)
            or before_path.st_size != len(payload)
        ):
            raise _InternalFailure()
        observed = _pread_exact(fd, len(payload), 0)
        if os.pread(fd, 1, len(payload)) != b"":
            raise _InternalFailure()
        after_fd = os.fstat(fd)
        after_path = os.stat(
            target, dir_fd=parent_fd, follow_symlinks=False
        )
        digest = hashlib.sha256(payload).hexdigest()
        if (
            _bound_source_file_identity(after_fd) != identity
            or _bound_source_file_identity(after_path) != identity
            or observed != payload
            or hashlib.sha256(observed).hexdigest() != digest
        ):
            raise _InternalFailure()
        _task4b_verify_raw_chunk_payload(observed, frame_rows)
        first_projection = frame_rows[0]["projection"]
        last_projection = frame_rows[-1]["projection"]
        exchange_start = first_projection["exchange_index"]
        exchange_stop = last_projection["exchange_index"]
        request_start = first_projection["request_ids"][0]
        request_stop = last_projection["request_ids"][-1]
        row = {
            "path": "rpc/" + target,
            "byte_count": len(payload),
            "sha256": digest,
            "exchange_index_start": exchange_start,
            "exchange_index_stop": exchange_stop,
            "exchange_count": len(frame_rows),
            "request_id_start": request_start,
            "request_id_stop": request_stop,
        }
        if (
            exchange_stop - exchange_start + 1 != len(frame_rows)
            or any(
                frame_row.get("raw_chunk_path") != row["path"]
                or frame_row.get("raw_physical_verified") is not False
                for frame_row in frame_rows
            )
        ):
            raise _InternalFailure()
        for frame_row in frame_rows:
            frame_row["raw_physical_verified"] = True
        _task4b_close_fd_slot(ledger, entry)
        os.fsync(parent_fd)
        _task4b_verify_capture_directory(rpc_directory)
        _task4b_commit_output_quota(ledger, entry, quota_token)
        return row

    def _task4b_canonical_json_bytes(value: Any) -> bytes:
        try:
            return json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except Exception:
            raise _InternalFailure()

    def _task4b_decode_canonical_json(
        payload: bytes, *, expected_container: type
    ) -> Any:
        def reject_pairs(pairs: Any) -> Dict[str, Any]:
            result = {}
            for key, value in pairs:
                if type(key) is not str or key in result:
                    raise _InternalFailure()
                result[key] = value
            return result

        def reject_number(_value: str) -> Any:
            raise _InternalFailure()

        if type(payload) is not bytes:
            raise _InternalFailure()
        try:
            value = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=reject_pairs,
                parse_int=int,
                parse_float=reject_number,
                parse_constant=reject_number,
            )
        except _InternalFailure:
            raise
        except Exception:
            raise _InternalFailure()
        if (
            type(value) is not expected_container
            or _task4b_canonical_json_bytes(value) != payload
        ):
            raise _InternalFailure()
        return value

    def _task4b_inventory_digest(domain: bytes, rows: Any) -> str:
        if type(domain) is not bytes or type(rows) not in (list, tuple):
            raise _InternalFailure()
        digest = hashlib.sha256()
        digest.update(domain)
        digest.update(b"\0")
        for row in rows:
            if type(row) is not dict:
                raise _InternalFailure()
            payload = _task4b_canonical_json_bytes(row)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    def _task4b_encode_gzip(decoded: bytes) -> bytes:
        if (
            type(decoded) is not bytes
            or not decoded
            or len(decoded) > 16_777_216
        ):
            raise _InternalFailure()
        buffer = io.BytesIO()
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=9,
            fileobj=buffer,
            mtime=0,
        ) as handle:
            handle.write(decoded)
        physical = buffer.getvalue()
        try:
            captured_gzip_member_size(byte_count=len(physical))
        except ValueError:
            raise _InternalFailure()
        if (
            len(physical) < 18
            or physical[:10] != bytes.fromhex("1f8b08000000000002ff")
            or int.from_bytes(physical[-4:], "little") != len(decoded)
        ):
            raise _InternalFailure()
        return physical

    def _task4b_decode_gzip(physical: bytes) -> bytes:
        if type(physical) is not bytes:
            raise _InternalFailure()
        try:
            captured_gzip_member_size(byte_count=len(physical))
        except ValueError:
            raise _InternalFailure()
        if (
            len(physical) < 18
            or physical[:10] != bytes.fromhex("1f8b08000000000002ff")
            or int.from_bytes(physical[-4:], "little") > 16_777_216
        ):
            raise _InternalFailure()
        try:
            buffer = io.BytesIO(physical)
            with gzip.GzipFile(mode="rb", fileobj=buffer) as handle:
                decoded = handle.read(16_777_217)
                extra = handle.read(1)
        except Exception:
            raise _InternalFailure()
        if (
            type(decoded) is not bytes
            or len(decoded) > 16_777_216
            or extra != b""
            or _task4b_encode_gzip(decoded) != physical
        ):
            raise _InternalFailure()
        return decoded

    def _task4b_write_capture_member(
        ledger: Dict[str, Any],
        directory: Dict[str, Any],
        target: str,
        payload: bytes,
    ) -> None:
        if type(payload) is not bytes or not payload:
            raise _InternalFailure()
        _require_relative_basename(target)
        _task4b_verify_capture_directory(directory)
        parent_fd = directory.get("fd")
        if type(parent_fd) is not int:
            raise _InternalFailure()
        entry = {
            "parent_fd": parent_fd,
            "name": target,
            "fd": None,
            "identity": None,
            "acquisition_state": "pending",
            "unlink_state": "pending",
            "parent_fsync_state": "pending",
            "close_state": "pending",
            "cleanup_phase": "verify",
        }
        ledger["files"].append(entry)
        quota_token = _task4b_reserve_output_quota(
            ledger, entry, len(payload)
        )
        entry["acquisition_state"] = "attempting"
        fd = _task4b_open_registered_slot(
            entry,
            target,
            _task4b_file_flags(create=True),
            dir_fd=parent_fd,
            mode=0o600,
        ); entry["acquisition_state"] = "attempted"
        if os.get_inheritable(fd):
            raise _InternalFailure()
        _pwrite_all(fd, payload, 0)
        os.fsync(fd)
        before_fd = os.fstat(fd)
        before_path = os.stat(
            target, dir_fd=parent_fd, follow_symlinks=False
        )
        identity = _bound_source_file_identity(before_fd)
        entry["identity"] = identity
        if (
            not stat.S_ISREG(before_fd.st_mode)
            or not stat.S_ISREG(before_path.st_mode)
            or _bound_source_file_identity(before_path) != identity
            or before_fd.st_nlink != 1
            or before_path.st_nlink != 1
            or before_fd.st_uid != os.geteuid()
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_fd.st_mode) != 0o600
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_fd.st_size != len(payload)
            or before_path.st_size != len(payload)
        ):
            raise _InternalFailure()
        observed = _pread_exact(fd, len(payload), 0)
        if os.pread(fd, 1, len(payload)) != b"":
            raise _InternalFailure()
        after_fd = os.fstat(fd)
        after_path = os.stat(
            target, dir_fd=parent_fd, follow_symlinks=False
        )
        if (
            _bound_source_file_identity(after_fd) != identity
            or _bound_source_file_identity(after_path) != identity
            or observed != payload
        ):
            raise _InternalFailure()
        _task4b_close_fd_slot(ledger, entry)
        os.fsync(parent_fd)
        _task4b_verify_capture_directory(directory)
        _task4b_commit_output_quota(ledger, entry, quota_token)
        return None

    def _task4b_validate_typed_rows(role: str, rows: Any) -> None:
        if role not in task4b_typed_roles or type(rows) is not list or not rows:
            raise _InternalFailure()
        block_key = "number" if role == "headers" else "block_number"
        blocks = []
        for row in rows:
            block = row.get(block_key) if type(row) is dict else None
            if type(block) is not int or block < 0:
                raise _InternalFailure()
            blocks.append(block)
        if blocks != sorted(blocks):
            raise _InternalFailure()
        if role == "headers":
            if any(right != left + 1 for left, right in zip(blocks, blocks[1:])):
                raise _InternalFailure()
        elif role in ("prices", "fees"):
            if len(set(blocks)) != len(blocks):
                raise _InternalFailure()
        else:
            if len(rows) % 2 != 0:
                raise _InternalFailure()
            for offset in range(0, len(rows), 2):
                pair = rows[offset:offset + 2]
                if (
                    pair[0]["block_number"] != pair[1]["block_number"]
                    or tuple(row.get("venue_id") for row in pair)
                    != ("uniswap_v2", "sushiswap_v2")
                ):
                    raise _InternalFailure()
        return None

    def _task4b_flush_typed_builder(record: Dict[str, Any]) -> None:
        builder = record.get("typed_builder")
        if type(builder) is not dict:
            raise _InternalFailure()
        role = builder.get("role")
        row_bytes = builder.get("row_bytes")
        if role is None:
            if row_bytes != [] or builder.get("row_count") != 0:
                raise _InternalFailure()
            return None
        if (
            role not in task4b_typed_roles
            or type(row_bytes) is not list
            or not row_bytes
            or builder.get("row_count") != len(row_bytes)
        ):
            raise _InternalFailure()
        decoded = b"[" + b",".join(row_bytes) + b"]"
        if len(decoded) != builder.get("decoded_size"):
            raise _InternalFailure()
        physical = _task4b_encode_gzip(decoded)
        indices = record.get("next_typed_chunk_indices")
        chunks = record.get("typed_chunks")
        ledger = record["owner"].get("_task4b_staging")
        directories = ledger.get("role_directories") if type(ledger) is dict else None
        chunk_index = indices.get(role) if type(indices) is dict else None
        directory = directories.get(role) if type(directories) is dict else None
        if type(chunks) is not list or type(chunk_index) is not int:
            raise _InternalFailure()
        target = "{:08d}.json.gz".format(chunk_index)
        _task4b_write_capture_member(ledger, directory, target, physical)
        chunks.append({
            "path": role + "/" + target,
            "role": role,
            "chunk_index": chunk_index,
            "block_start": builder["block_start"],
            "block_stop": builder["block_stop"],
            "row_count": len(row_bytes),
            "decoded_byte_count": len(decoded),
            "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
            "gzip_byte_count": len(physical),
            "gzip_sha256": hashlib.sha256(physical).hexdigest(),
        })
        indices[role] = chunk_index + 1
        builder.clear()
        builder.update({
            "role": None,
            "row_bytes": [],
            "row_count": 0,
            "decoded_size": 2,
            "block_start": None,
            "block_stop": None,
        })
        del decoded, physical, row_bytes
        return None

    def _task4b_append_typed_root(
        record: Dict[str, Any],
        *,
        role: str,
        canonical_payload: bytes,
        row_count: int,
        logical_sha256: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if (
            role not in task4b_typed_roles
            or type(row_count) is not int
            or row_count <= 0
            or not _exact_sha256(logical_sha256)
        ):
            raise _InternalFailure()
        rows = _task4b_decode_canonical_json(
            canonical_payload, expected_container=list
        )
        if len(rows) != row_count:
            raise _InternalFailure()
        _task4b_validate_typed_rows(role, rows)
        if (
            _task4b_inventory_digest(task4b_typed_domains[role], rows)
            != logical_sha256
        ):
            raise _InternalFailure()
        row_bytes = tuple(_task4b_canonical_json_bytes(row) for row in rows)
        if canonical_payload != b"[" + b",".join(row_bytes) + b"]":
            raise _InternalFailure()
        builder = record.get("typed_builder")
        role_position = task4b_typed_roles.index(role)
        if type(builder) is not dict:
            raise _InternalFailure()
        if builder.get("role") not in (None, role):
            _task4b_flush_typed_builder(record)
        last_position = record.get("typed_role_position")
        if type(last_position) is not int or role_position < last_position:
            raise _InternalFailure()
        record["typed_role_position"] = role_position
        builder = record["typed_builder"]
        if builder["role"] is None:
            builder["role"] = role
        try:
            decision, resulting_size = captured_typed_root_append(
                current_decoded_size=builder["decoded_size"],
                current_row_count=builder["row_count"],
                candidate_row_encoded_lengths=tuple(
                    len(payload) for payload in row_bytes
                ),
            )
        except ValueError:
            raise _InternalFailure()
        if decision == "flush_then_append":
            _task4b_flush_typed_builder(record)
            builder = record["typed_builder"]
            builder["role"] = role
            expected_size = 2 + sum(len(value) for value in row_bytes) + len(row_bytes) - 1
            if resulting_size != expected_size:
                raise _InternalFailure()
        elif decision != "append_current":
            raise _InternalFailure()
        first_row_index = builder["row_count"]
        block_key = "number" if role == "headers" else "block_number"
        if first_row_index == 0:
            builder["block_start"] = rows[0][block_key]
        builder["block_stop"] = rows[-1][block_key]
        builder["row_bytes"].extend(row_bytes)
        builder["row_count"] += len(row_bytes)
        builder["decoded_size"] = resulting_size
        chunk_index = record["next_typed_chunk_indices"][role]
        refs = [{
            "path": role + "/{:08d}.json.gz".format(chunk_index),
            "first_row_index": first_row_index,
            "row_count": len(rows),
        }]
        return refs, rows

    def _task4b_verify_capture_directory(entry: Dict[str, Any]) -> None:
        fd = entry.get("fd")
        parent_fd = entry.get("parent_fd")
        name = entry.get("name")
        if type(fd) is not int or type(parent_fd) is not int:
            raise _InternalFailure()
        opened = os.fstat(fd)
        current = os.stat(
            name, dir_fd=parent_fd, follow_symlinks=False
        )
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or _metadata_snapshot(opened) != entry.get("identity")
            or _metadata_snapshot(current) != entry.get("identity")
            or opened.st_uid != os.geteuid()
            or current.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
            or stat.S_IMODE(current.st_mode) & 0o022
            or os.get_inheritable(fd)
        ):
            raise _InternalFailure()
        return None

    def _task4b_prepare_config_staging(
        owner_handle: object,
        owner: Dict[str, Any],
        binding_record: Dict[str, Any],
    ) -> None:
        if owner.get("_task4b_staging") is not None:
            raise _InternalFailure()
        ledger = _task4b_new_capture_ledger()
        ledger["lineage"] = owner.get("lineage")
        ledger["private_basename"] = None
        ledger["quota_owner_handle"] = owner_handle
        owner["_task4b_staging"] = ledger
        config_source_drifted = False
        try:
            config_directory = _task4b_open_repository_config_directory(
                binding_record, ledger
            )
            config_fd = config_directory["fd"]
            source_specs = (
                (
                    "policy",
                    "historical_foundry_replay_policy.json",
                    "policy.json",
                ),
                (
                    "authority",
                    "historical_foundry_replay_authority.json",
                    "authority.json",
                ),
                (
                    "toolchain",
                    "historical_foundry_replay_toolchain.json",
                    "toolchain.json",
                ),
            )
            opened_source_rows = []
            for role, source_name, target in source_specs:
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
                source = _task4b_open_config_source(
                    config_fd, source_name, ledger
                )
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
                opened_source_rows.append((role, source, target))
            opened_sources = tuple(opened_source_rows)
            first_source_rows = []
            for role, source, _target in opened_sources:
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
                payload = _task4b_read_config_source(config_fd, source)
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
                first_source_rows.append((role, payload, source))
            first_rows = tuple(first_source_rows)
            authority = _task4b_validate_config_set(
                first_rows, owner, binding_record
            )
        except _BoundSourceIdentityDrift:
            config_source_drifted = True
        if config_source_drifted:
            raise _bound_source_drift(binding_record)

        _verify_ancestry(owner["chain"])
        data_fd = owner["chain"][-1][0]
        raw = _task4b_open_capture_directory(
            ledger, data_fd, "raw", allow_existing=True
        )
        replay = _task4b_open_capture_directory(
            ledger,
            raw["fd"],
            "historical-foundry-replay",
            allow_existing=True,
        )
        entropy = os.urandom(16)
        if type(entropy) is not bytes or len(entropy) != 16:
            raise _InternalFailure()
        staging_name = ".staging-" + entropy.hex()
        if (
            len(staging_name) != 41
            or any(character not in "0123456789abcdef" for character in staging_name[9:])
        ):
            raise _InternalFailure()
        ledger["private_basename"] = staging_name
        staging = _task4b_open_capture_directory(
            ledger,
            replay["fd"],
            staging_name,
            allow_existing=False,
        )
        roles = tuple(
            _task4b_open_capture_directory(
                ledger, staging["fd"], role, allow_existing=False
            )
            for role in (
                "rpc", "headers", "reserves", "prices", "fees", "scan"
            )
        )
        ledger["role_directories"] = {
            role["name"]: role for role in roles
        }
        ledger["capture_directories"] = {
            "raw": raw,
            "replay": replay,
            "staging": staging,
        }
        for role, source, target in opened_sources:
            config_source_drifted = False
            try:
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
                second = _task4b_read_config_source(config_fd, source)
                _task4b_verify_repository_config_directory(
                    binding_record, config_directory
                )
            except _BoundSourceIdentityDrift:
                config_source_drifted = True
            if config_source_drifted:
                raise _bound_source_drift(binding_record)
            first = next(row[1] for row in first_rows if row[0] == role)
            if (
                second != first
                or len(second) != len(first)
                or hashlib.sha256(second).hexdigest()
                != authority["digests"][role]
            ):
                raise _bound_source_drift(binding_record)
            _task4b_write_capture_config(
                ledger,
                staging["fd"],
                target,
                first,
                authority["values"][role],
                authority["digests"][role],
            )
        ledger["config_rows"] = tuple({
            "role": role,
            "path": target,
            "schema": authority["values"][role]["schema"],
            "byte_count": len(next(
                row[1] for row in first_rows if row[0] == role
            )),
            "sha256": authority["digests"][role],
            "policy_id": (
                authority["sealed"]["policy_id"]
                if role == "policy" else None
            ),
        } for role, _source, target in opened_sources)
        ledger["policy_value"] = dict(authority["values"]["policy"])
        for _role, source, _target in reversed(opened_sources):
            _task4b_close_fd_slot(ledger, source["slot"])
        _task4b_close_fd_slot(ledger, config_directory["slot"])
        expected = frozenset((
            "policy.json", "authority.json", "toolchain.json",
            "rpc", "headers", "reserves", "prices", "fees", "scan",
        ))
        observed = os.listdir(staging["fd"])
        if (
            type(observed) is not list
            or len(observed) != len(expected)
            or any(type(name) is not str for name in observed)
            or frozenset(observed) != expected
        ):
            raise _InternalFailure()
        for role in roles:
            _task4b_verify_capture_directory(role)
            role_members = os.listdir(role["fd"])
            if type(role_members) is not list or role_members:
                raise _InternalFailure()
            os.fsync(role["fd"])
        for directory in (staging, replay, raw):
            _task4b_verify_capture_directory(directory)
            os.fsync(directory["fd"])
        os.fsync(data_fd)
        _verify_ancestry(owner["chain"])
        return None

    def _task4b_post_leaf_from_join(join: Dict[str, Any]) -> Dict[str, Any]:
        if type(join) is not dict or tuple(join) != task4b_final_join_keys:
            raise _InternalFailure()
        leaf = {
            "schema": "historical_foundry_leaf_ledger/v1",
            "segment": join["segment"],
            "segment_local_index": join["segment_local_index"],
            "leaf_index": join["leaf_index"],
            "request_ids": join["request_ids"],
            "request_count": len(join["request_ids"]),
            "canonical_request_sha256": join["request_sha256"],
            "response_ids": join["response_ids"],
            "exchange_index": join["exchange_index"],
            "logical_batch_index": join["logical_batch_index"],
            "attempt_index": join["attempt_index"],
            "request_byte_count": join["request_byte_count"],
            "decoded_byte_count": join["decoded_byte_count"],
            "decoded_sha256": join["decoded_sha256"],
            "wire_byte_count": join["wire_byte_count"],
            "wire_sha256": join["wire_sha256"],
            "wire_hash_authority": join["wire_hash_authority"],
            "spool_member_index": join["spool_member_index"],
            "spool_offset": join["spool_offset"],
            "spool_length": join["spool_length"],
            "spool_member_sha256": join["spool_member_sha256"],
        }
        if tuple(leaf) != task4b_post_leaf_keys:
            raise _InternalFailure()
        return leaf

    def _task4b_capture_source_identity(owner: Dict[str, Any]) -> Dict[str, Any]:
        finalization = owner.get("claimed_finalization")
        try:
            identity = finalization["identity"]
        except Exception:
            raise _InternalFailure()
        if type(identity) is not dict or type(identity.get("configs")) is not dict:
            raise _InternalFailure()
        result = dict(identity)
        result.pop("configs")
        result["schema"] = "historical_foundry_capture_source_identity/v1"
        # Normalize tuples to JSON arrays without accepting a noncanonical or
        # nonfinite value from the claimed identity.
        return _task4b_decode_canonical_json(
            _task4b_canonical_json_bytes(result), expected_container=dict
        )

    def _task4b_validate_digest_bindings(
        prefinalization: Any, reconciliation: Any
    ) -> None:
        if (
            type(prefinalization) is not tuple
            or len(prefinalization) != 5
            or prefinalization[0]
            != "historical_foundry_prefinalization_digest_binding/v1"
            or not all(_exact_sha256(value) for value in prefinalization[1:])
            or type(reconciliation) is not tuple
            or len(reconciliation) != 6
            or reconciliation[0]
            != "historical_foundry_reconciliation_digest_binding/v1"
            or type(reconciliation[1]) is not int
            or reconciliation[1] <= 0
            or not _exact_sha256(reconciliation[2])
            or type(reconciliation[3]) is not int
            or reconciliation[3] <= 0
            or not _exact_sha256(reconciliation[4])
            or not _exact_sha256(reconciliation[5])
            or prefinalization[3] != reconciliation[5]
        ):
            raise _InternalFailure()
        return None

    def _task4b_build_capture_inventory(
        record: Dict[str, Any],
        *,
        config_rows: Any,
        raw_chunks: Any,
        typed_chunks: Any,
        range_row: Dict[str, Any],
    ) -> Dict[str, Any]:
        owner = record.get("owner")
        joins = record.get("exchange_joins")
        post_roots = record.get("post_roots")
        finish = record.get("finish_payload")
        if (
            type(owner) is not dict
            or type(joins) is not list
            or not joins
            or type(post_roots) is not list
            or not post_roots
            or type(finish) is not tuple
            or len(finish) != 5
            or type(config_rows) not in (list, tuple)
            or type(raw_chunks) not in (list, tuple)
            or type(typed_chunks) not in (list, tuple)
            or type(range_row) is not dict
        ):
            raise _InternalFailure()
        prefinalization, reconciliation = finish[3], finish[4]
        _task4b_validate_digest_bindings(prefinalization, reconciliation)
        if finish[1] != len(joins):
            raise _InternalFailure()
        receipts = []
        leaves = []
        request_ids = []
        for expected_index, join in enumerate(joins, 1):
            if (
                type(join) is not dict
                or tuple(join) != task4b_final_join_keys
                or join.get("exchange_index") != expected_index
                or type(join.get("request_ids")) is not tuple
                or not join["request_ids"]
            ):
                raise _InternalFailure()
            receipt = {key: join[key] for key in receipt_keys}
            receipt["schema"] = "historical_foundry_exchange_spool_receipt/v1"
            receipts.append(receipt)
            leaves.append(_task4b_post_leaf_from_join(join))
            request_ids.extend(join["request_ids"])
        receipt_digest = _task4b_inventory_digest(
            b"historical_foundry_exchange_spool_receipt_inventory/v1",
            receipts,
        )
        if receipt_digest != owner.get("receipt_inventory_sha256"):
            raise _InternalFailure()
        roots_by_index = {}
        leaves_by_root = {}
        for leaf in leaves:
            leaves_by_root.setdefault(leaf["logical_batch_index"], []).append(leaf)
        for expected_index, root in enumerate(post_roots, 1):
            logical_index = root.get("logical_batch_index") if type(root) is dict else None
            root_leaves = leaves_by_root.get(logical_index)
            if (
                type(logical_index) is not int
                or logical_index != expected_index
                or logical_index in roots_by_index
                or type(root_leaves) is not list
                or tuple(leaf["exchange_index"] for leaf in root_leaves)
                != root.get("success_exchange_indices")
                or root.get("leaf_count") != len(root_leaves)
                or root.get("leaf_ledger_sha256")
                != _task4b_inventory_digest(
                    b"historical_foundry_leaf_ledger/v1", root_leaves
                )
            ):
                raise _InternalFailure()
            roots_by_index[logical_index] = root
        rebuilt_reconciliation = (
            "historical_foundry_reconciliation_digest_binding/v1",
            len(post_roots),
            _task4b_inventory_digest(
                b"historical_foundry_reconciliation_post_root_ledger/v1",
                post_roots,
            ),
            len(leaves),
            _task4b_inventory_digest(
                b"historical_foundry_reconciliation_post_leaf_ledger/v1",
                leaves,
            ),
            prefinalization[3],
        )
        if rebuilt_reconciliation != reconciliation:
            raise _InternalFailure()
        if request_ids != list(range(1, len(request_ids) + 1)):
            raise _InternalFailure()
        source_identity = _task4b_capture_source_identity(owner)
        collection = {
            "logical_batch_count": len(post_roots),
            "successful_exchange_count": len(joins),
            "request_count": sum(len(row["request_ids"]) for row in joins),
            "response_count": sum(len(row["response_ids"]) for row in joins),
            "wire_byte_count": sum(row["wire_byte_count"] for row in joins),
            "decoded_byte_count": sum(row["decoded_byte_count"] for row in joins),
        }
        if source_identity.get("collection") != collection:
            raise _InternalFailure()
        return {
            "schema": "historical_foundry_capture_inventory/v1",
            "source_identity": source_identity,
            "receipt_inventory_sha256": receipt_digest,
            "prefinalization_digests": prefinalization,
            "reconciliation_digests": reconciliation,
            "range": range_row,
            "request_range": {
                "first_request_id": 1,
                "last_request_id": len(request_ids),
                "request_count": len(request_ids),
            },
            "configs": list(config_rows),
            "raw_chunks": list(raw_chunks),
            "typed_chunks": list(typed_chunks),
            "post_roots": post_roots,
            "exchanges": joins,
        }

    def _task4b_capture_directory_for_path(
        ledger: Dict[str, Any], relative_path: str
    ) -> Tuple[Dict[str, Any], str]:
        if type(relative_path) is not str or not relative_path:
            raise _InternalFailure()
        if relative_path.startswith("scan/prefilter/"):
            basename = relative_path[len("scan/prefilter/"):]
            directory = ledger["role_directories"].get("prefilter")
        elif "/" in relative_path:
            role, basename = relative_path.split("/", 1)
            directory = ledger["role_directories"].get(role)
        else:
            basename = relative_path
            directory = ledger["capture_directories"].get("staging")
        _require_relative_basename(basename)
        if type(directory) is not dict:
            raise _InternalFailure()
        return directory, basename

    def _task4b_reread_capture_member(
        ledger: Dict[str, Any],
        *,
        relative_path: str,
        expected_size: int,
        maximum_size: int,
        size_kind: str,
    ) -> bytes:
        if (
            type(expected_size) is not int
            or expected_size <= 0
            or type(maximum_size) is not int
            or expected_size > maximum_size
        ):
            raise _InternalFailure()
        if size_kind == "gzip":
            try:
                captured_gzip_member_size(byte_count=expected_size)
            except ValueError:
                raise _InternalFailure()
        elif size_kind == "inventory":
            try:
                captured_capture_inventory_size(byte_count=expected_size)
            except ValueError:
                raise _InternalFailure()
        directory, basename = _task4b_capture_directory_for_path(
            ledger, relative_path
        )
        _task4b_verify_capture_directory(directory)
        parent_fd = directory["fd"]
        file_entry = next((
            entry for entry in ledger["files"]
            if entry.get("parent_fd") == parent_fd
            and entry.get("name") == basename
        ), None)
        if type(file_entry) is not dict:
            raise _InternalFailure()
        slot = _task4b_transient_fd_slot(ledger)
        slot["acquisition_state"] = "attempting"
        try:
            fd = _task4b_open_registered_slot(
                slot,
                basename,
                _task4b_file_flags(create=False),
                dir_fd=parent_fd,
            ); slot["acquisition_state"] = "attempted"
            before_fd = os.fstat(fd)
            before_path = os.stat(
                basename, dir_fd=parent_fd, follow_symlinks=False
            )
            identity = _bound_source_file_identity(before_fd)
            frozen_identity = file_entry.get("identity")
            if (
                not stat.S_ISREG(before_fd.st_mode)
                or not stat.S_ISREG(before_path.st_mode)
                or _bound_source_file_identity(before_path) != identity
                or type(frozen_identity) is not tuple
                or len(frozen_identity) != 9
                or identity[:6] != frozen_identity[:6]
                or before_fd.st_nlink != 1
                or before_fd.st_uid != os.geteuid()
                or stat.S_IMODE(before_fd.st_mode) != 0o600
                or os.get_inheritable(fd)
            ):
                raise _InternalFailure()
            # The member is inside the private no-replace staging tree and the
            # descriptor/path still name the exact same inode.  Refresh only
            # cleanup's frozen metadata so a detected in-place corruption is
            # still terminally removable; an inode/path transplant never gets
            # this authority.
            file_entry["identity"] = identity
            if before_fd.st_size != expected_size:
                raise _InternalFailure()
            payload = _pread_exact(fd, expected_size, 0)
            if os.pread(fd, 1, expected_size) != b"":
                raise _InternalFailure()
            after_fd = os.fstat(fd)
            after_path = os.stat(
                basename, dir_fd=parent_fd, follow_symlinks=False
            )
            if (
                _bound_source_file_identity(after_fd) != identity
                or _bound_source_file_identity(after_path) != identity
                or len(payload) != expected_size
            ):
                raise _InternalFailure()
            return payload
        finally:
            _task4b_close_fd_slot(ledger, slot)

    def _task4b_rebuild_raw_chunks(
        record: Dict[str, Any], ledger: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        joins = record["exchange_joins"]
        rebuilt = []
        for expected in record["raw_chunks"]:
            path = expected["path"]
            payload = _task4b_reread_capture_member(
                ledger,
                relative_path=path,
                expected_size=expected["byte_count"],
                maximum_size=16_777_216,
                size_kind="raw",
            )
            chunk_joins = [row for row in joins if row["raw_chunk_path"] == path]
            frame_rows = tuple({
                "projection": {key: row[key] for key in receipt_keys},
                "raw_offset": row["raw_chunk_offset"],
            } for row in chunk_joins)
            _task4b_verify_raw_chunk_payload(payload, frame_rows)
            request_ids = [
                request_id for row in chunk_joins
                for request_id in row["request_ids"]
            ]
            if not chunk_joins or not request_ids:
                raise _InternalFailure()
            rebuilt.append({
                "path": path,
                "byte_count": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "exchange_index_start": chunk_joins[0]["exchange_index"],
                "exchange_index_stop": chunk_joins[-1]["exchange_index"],
                "exchange_count": len(chunk_joins),
                "request_id_start": request_ids[0],
                "request_id_stop": request_ids[-1],
            })
            del payload, chunk_joins, frame_rows, request_ids
        if rebuilt != record["raw_chunks"]:
            raise _InternalFailure()
        return rebuilt

    def _task4b_rebuild_typed_chunks(
        record: Dict[str, Any], ledger: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        rebuilt = []
        observed_counts = {role: 0 for role in task4b_typed_roles}
        header_range = {
            "lower_bound_number": None,
            "anchor_number": None,
            "anchor_timestamp": None,
            "count": 0,
        }
        for root_record in record["root_records"]:
            role = root_record["role"]
            refs = root_record["refs"]
            root_joins = [
                record["exchange_joins"][index - 1]
                for index in root_record["success_exchange_indices"]
            ]
            if any(
                join.get("typed_role") != role
                or join.get("typed_chunk_refs") != refs
                for join in root_joins
            ):
                raise _InternalFailure()
            if role not in task4b_typed_roles:
                if refs != []:
                    raise _InternalFailure()
            elif type(refs) is not list or len(refs) != 1:
                raise _InternalFailure()
        for expected in record["typed_chunks"]:
            path = expected["path"]
            physical = _task4b_reread_capture_member(
                ledger,
                relative_path=path,
                expected_size=expected["gzip_byte_count"],
                maximum_size=16_842_752,
                size_kind="gzip",
            )
            decoded = _task4b_decode_gzip(physical)
            rows = _task4b_decode_canonical_json(decoded, expected_container=list)
            role = expected["role"]
            _task4b_validate_typed_rows(role, rows)
            block_key = "number" if role == "headers" else "block_number"
            row = {
                "path": path,
                "role": role,
                "chunk_index": expected["chunk_index"],
                "block_start": rows[0][block_key],
                "block_stop": rows[-1][block_key],
                "row_count": len(rows),
                "decoded_byte_count": len(decoded),
                "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                "gzip_byte_count": len(physical),
                "gzip_sha256": hashlib.sha256(physical).hexdigest(),
            }
            rebuilt.append(row)
            intervals = []
            for root_record in record["root_records"]:
                refs = root_record["refs"]
                if (
                    root_record["role"] not in task4b_typed_roles
                    or refs[0]["path"] != path
                ):
                    continue
                ref = refs[0]
                if (
                    type(ref) is not dict
                    or tuple(ref) != ("path", "first_row_index", "row_count")
                    or type(ref["first_row_index"]) is not int
                    or type(ref["row_count"]) is not int
                    or ref["first_row_index"] < 0
                    or ref["row_count"] <= 0
                ):
                    raise _InternalFailure()
                stop = ref["first_row_index"] + ref["row_count"]
                root_rows = rows[ref["first_row_index"]:stop]
                root_role = root_record["role"]
                if (
                    len(root_rows) != root_record["row_count"]
                    or _task4b_inventory_digest(
                        task4b_typed_domains[root_role], root_rows
                    ) != root_record["logical_sha256"]
                ):
                    raise _InternalFailure()
                intervals.append((ref["first_row_index"], stop))
                observed_counts[root_role] += len(root_rows)
            cursor = 0
            for start, stop in sorted(intervals):
                if start != cursor or stop <= start:
                    raise _InternalFailure()
                cursor = stop
            if cursor != len(rows):
                raise _InternalFailure()
            if expected["role"] == "headers":
                if header_range["count"] == 0:
                    header_range["lower_bound_number"] = rows[0]["number"]
                elif rows[0]["number"] != header_range["anchor_number"] + 1:
                    raise _InternalFailure()
                header_range["anchor_number"] = rows[-1]["number"]
                header_range["anchor_timestamp"] = rows[-1]["timestamp"]
                header_range["count"] += len(rows)
            del physical, decoded, rows, intervals
        if rebuilt != record["typed_chunks"]:
            raise _InternalFailure()
        expected_counts = dict(record["finish_payload"][2])
        if observed_counts != expected_counts:
            raise _InternalFailure()
        return rebuilt, header_range

    def _task4b_freeze_audit(record: Dict[str, Any]) -> None:
        owner = record["owner"]
        ledger = owner.get("_task4b_staging")
        roles = ledger.get("role_directories") if type(ledger) is dict else None
        captures = ledger.get("capture_directories") if type(ledger) is dict else None
        if type(roles) is not dict or type(captures) is not dict:
            raise _InternalFailure()
        # Freeze step 1: tiered fsync, inside out.
        for role in ("rpc", "headers", "reserves", "prices", "fees", "scan"):
            _task4b_verify_capture_directory(roles[role])
            os.fsync(roles[role]["fd"])
        for name in ("staging", "replay", "raw"):
            _task4b_verify_capture_directory(captures[name])
            os.fsync(captures[name]["fd"])
        _verify_ancestry(owner["chain"])
        for ancestry_row in reversed(owner["chain"]):
            os.fsync(ancestry_row[0])
        _verify_ancestry(owner["chain"])

        # Freeze step 2: exact tree enumeration.
        expected_role_members = {
            role: set() for role in ("rpc", "headers", "reserves", "prices", "fees", "scan")
        }
        for row in record["raw_chunks"]:
            role, basename = row["path"].split("/", 1)
            expected_role_members[role].add(basename)
        for row in record["typed_chunks"]:
            role, basename = row["path"].split("/", 1)
            expected_role_members[role].add(basename)
        expected_role_members["scan"].add("capture_inventory.json")
        if owner.get("capture_generation") == 2:
            expected_role_members["scan"].update((
                "prefilter", "prefilter_inventory.json",
            ))
        staging_expected = {
            "policy.json", "authority.json", "toolchain.json",
            "rpc", "headers", "reserves", "prices", "fees", "scan",
        }
        if set(os.listdir(captures["staging"]["fd"])) != staging_expected:
            raise _InternalFailure()
        for role, expected in expected_role_members.items():
            if set(os.listdir(roles[role]["fd"])) != expected:
                raise _InternalFailure()

        # Freeze steps 3-4: complete descriptor sessions and immediate
        # reconstruction.  Only one bounded member is live at a time.
        config_rows = []
        policy_value = None
        claimed_configs = owner["claimed_finalization"]["identity"]["configs"]
        for role, path in (
            ("policy", "policy.json"),
            ("authority", "authority.json"),
            ("toolchain", "toolchain.json"),
        ):
            expected = next(
                row for row in ledger["config_rows"] if row["path"] == path
            )
            payload = _task4b_reread_capture_member(
                ledger,
                relative_path=path,
                expected_size=expected["byte_count"],
                maximum_size=1_048_576,
                size_kind="config",
            )
            try:
                value = _task4b_decode_canonical_config(payload)
            except _BoundSourceIdentityDrift:
                raise _InternalFailure()
            digest = hashlib.sha256(payload).hexdigest()
            hash_key = role + "_physical_sha256"
            if digest != claimed_configs[hash_key]:
                raise _InternalFailure()
            config_rows.append({
                "role": role,
                "path": path,
                "schema": value["schema"],
                "byte_count": len(payload),
                "sha256": digest,
                "policy_id": claimed_configs["policy_id"] if role == "policy" else None,
            })
            if role == "policy":
                policy_value = value
            del payload, value
        raw_chunks = _task4b_rebuild_raw_chunks(record, ledger)
        typed_chunks, header_range = _task4b_rebuild_typed_chunks(
            record, ledger
        )
        lookback = policy_value.get("lookback_seconds") if type(policy_value) is dict else None
        if (
            header_range.get("count", 0) <= 0
            or type(lookback) is not int
            or lookback <= 0
        ):
            raise _InternalFailure()
        range_row = {
            "lower_bound_number": header_range["lower_bound_number"],
            "anchor_number": header_range["anchor_number"],
            "cutoff_timestamp": header_range["anchor_timestamp"] - lookback,
            "block_count": header_range["count"],
        }
        inventory_bytes = _task4b_reread_capture_member(
            ledger,
            relative_path="scan/capture_inventory.json",
            expected_size=record["inventory_byte_count"],
            maximum_size=16_777_216,
            size_kind="inventory",
        )
        # Freeze step 5: canonical inventory byte equality.
        rebuilt = _task4b_build_capture_inventory(
            record,
            config_rows=config_rows,
            raw_chunks=raw_chunks,
            typed_chunks=typed_chunks,
            range_row=range_row,
        )
        _task4b_decode_canonical_json(
            inventory_bytes, expected_container=dict
        )
        if _task4b_canonical_json_bytes(rebuilt) != inventory_bytes:
            raise _InternalFailure()
        return None

    def _task4b_capture_file_cleanup_safe(
        entry: Dict[str, Any]
    ) -> bool:
        fd = _task4b_registered_slot_fd(entry)
        current = os.stat(
            entry["name"],
            dir_fd=entry["parent_fd"],
            follow_symlinks=False,
        )
        if type(fd) is int:
            opened = os.fstat(fd)
            identity = _bound_source_file_identity(opened)
            frozen = entry.get("identity")
            return (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(current.st_mode)
                and _bound_source_file_identity(current) == identity
                and (frozen is None or frozen == identity)
                and opened.st_uid == os.geteuid()
                and current.st_uid == os.geteuid()
                and opened.st_nlink == 1
                and current.st_nlink == 1
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(current.st_mode) == 0o600
            )
        frozen = entry.get("identity")
        return (
            type(frozen) is tuple
            and len(frozen) == 9
            and stat.S_ISREG(current.st_mode)
            and _bound_source_file_identity(current) == frozen
            and current.st_uid == os.geteuid()
            and current.st_nlink == 1
            and stat.S_IMODE(current.st_mode) == 0o600
        )

    def _task4b_capture_directory_cleanup_safe(
        entry: Dict[str, Any]
    ) -> bool:
        fd = _task4b_registered_slot_fd(entry)
        frozen = entry.get("identity")
        if type(fd) is not int or (
            frozen is not None
            and (type(frozen) is not tuple or len(frozen) != 6)
        ):
            return False
        opened = os.fstat(fd)
        current = os.stat(
            entry["name"],
            dir_fd=entry["parent_fd"],
            follow_symlinks=False,
        )
        opened_identity = _metadata_snapshot(opened)
        current_identity = _metadata_snapshot(current)
        return (
            stat.S_ISDIR(opened.st_mode)
            and stat.S_ISDIR(current.st_mode)
            and opened_identity == current_identity
            and (frozen is None or opened_identity == frozen)
            and opened.st_uid == os.geteuid()
            and current.st_uid == os.geteuid()
            and stat.S_IMODE(opened.st_mode) == 0o700
            and stat.S_IMODE(current.st_mode) == 0o700
            and not os.get_inheritable(fd)
        )

    def _task4b_capture_cleanup_failure(
        state: Dict[str, Any], error: BaseException
    ) -> None:
        state["control"], state["ordinary"] = _capture_failure(
            state["control"], state["ordinary"], error
        )
        if not isinstance(error, Exception):
            raise error

    def _cleanup_task4b_capture_staging(
        record: Dict[str, Any]
    ) -> Tuple[Optional[BaseException], bool]:
        ledger = record.get("_task4b_staging")
        if type(ledger) is not dict:
            return None, False
        state = ledger.get("cleanup_state")
        if type(state) is not dict:
            state = {
                "phase": "transient",
                "current": None,
                "control": None,
                "ordinary": False,
            }
            ledger["cleanup_state"] = state

        while state["phase"] != "done":
            phase = state["phase"]
            if phase == "transient":
                entries = ledger["transient_fds"]
                if state["current"] is None:
                    if not entries:
                        state["phase"] = "files"
                        continue
                    state["current"] = entries[-1]
                slot = state["current"]
                try:
                    _task4b_close_fd_slot(ledger, slot)
                except BaseException as error:
                    _task4b_capture_cleanup_failure(state, error)
                    continue
                state["current"] = None
                if entries and entries[-1] is slot:
                    entries.pop()
                continue

            if phase == "files":
                entries = ledger["files"]
                if state["current"] is None:
                    if not entries:
                        state["phase"] = "directories"
                        continue
                    state["current"] = entries[-1]
                entry = state["current"]
                entry_phase = entry.get("cleanup_phase")
                if entry_phase == "verify":
                    try:
                        if not _task4b_capture_file_cleanup_safe(entry):
                            raise _InternalFailure()
                    except BaseException as error:
                        _task4b_capture_cleanup_failure(state, error)
                        entry["cleanup_phase"] = "close"
                    else:
                        entry["cleanup_phase"] = "unlink"
                    continue
                if entry_phase == "unlink":
                    unlink_state = entry.get("unlink_state")
                    if unlink_state == "pending":
                        try:
                            entry["unlink_state"] = "attempting"; os.unlink(entry["name"], dir_fd=entry["parent_fd"]); entry["unlink_state"] = "attempted"
                        except BaseException as error:
                            if isinstance(error, Exception):
                                _task4b_capture_cleanup_failure(state, error)
                                entry["unlink_error"] = True
                            else:
                                _task4b_capture_cleanup_failure(state, error)
                        else:
                            entry["cleanup_phase"] = "parent_fsync"
                        continue
                    if unlink_state == "attempting":
                        try:
                            still_safe = _task4b_capture_file_cleanup_safe(
                                entry
                            )
                        except FileNotFoundError:
                            entry.pop("unlink_error", None)
                            entry["unlink_state"] = "attempted"
                            entry["cleanup_phase"] = "parent_fsync"
                        except BaseException as error:
                            _task4b_capture_cleanup_failure(state, error)
                            entry.pop("unlink_error", None)
                            entry["unlink_state"] = "attempted"
                            entry["cleanup_phase"] = "close"
                        else:
                            if still_safe:
                                if entry.pop("unlink_error", False):
                                    entry["unlink_state"] = "attempted"
                                    entry["cleanup_phase"] = "close"
                                else:
                                    entry["unlink_state"] = "pending"
                            else:
                                _task4b_capture_cleanup_failure(
                                    state, _InternalFailure()
                                )
                                entry.pop("unlink_error", None)
                                entry["unlink_state"] = "attempted"
                                entry["cleanup_phase"] = "close"
                        continue
                    if unlink_state == "attempted":
                        entry["cleanup_phase"] = "parent_fsync"
                        continue
                    raise _InternalFailure()
                if entry_phase == "parent_fsync":
                    fsync_state = entry.get("parent_fsync_state")
                    if fsync_state == "pending":
                        try:
                            entry["parent_fsync_state"] = "attempting"; os.fsync(entry["parent_fd"]); entry["parent_fsync_state"] = "attempted"
                        except BaseException as error:
                            if isinstance(error, Exception):
                                _task4b_capture_cleanup_failure(state, error)
                                entry["parent_fsync_state"] = "attempted"
                            else:
                                _task4b_capture_cleanup_failure(state, error)
                        else:
                            entry["cleanup_phase"] = "close"
                        continue
                    if fsync_state == "attempting":
                        entry["parent_fsync_state"] = "pending"
                        continue
                    if fsync_state == "attempted":
                        entry["cleanup_phase"] = "close"
                        continue
                    raise _InternalFailure()
                if entry_phase == "close":
                    try:
                        _task4b_close_fd_slot(ledger, entry)
                    except BaseException as error:
                        _task4b_capture_cleanup_failure(state, error)
                        continue
                    entry["cleanup_phase"] = "done"
                    continue
                if entry_phase == "done":
                    quota_state = entry.get("quota_state")
                    if quota_state in (
                        "reserving", "reserved", "committing", "aborting"
                    ):
                        try:
                            quota_state = _task4b_reconcile_output_quota_entry(
                                ledger, entry
                            )
                            entry["quota_state"] = quota_state
                        except BaseException as error:
                            _task4b_capture_cleanup_failure(state, error)
                            continue
                    if quota_state == "reserved":
                        if (
                            entry.get("unlink_state") != "attempted"
                            or entry.get("parent_fsync_state") != "attempted"
                            or entry.get("close_state")
                            not in ("attempted", "unresolved")
                        ):
                            _task4b_capture_cleanup_failure(
                                state, _InternalFailure()
                            )
                        entry["quota_state"] = "aborting"
                        try:
                            _task4b_abort_output_quota(
                                ledger, entry, entry.get("quota_token")
                            )
                        except BaseException as error:
                            _task4b_capture_cleanup_failure(state, error)
                            continue
                        entry["quota_state"] = "aborted"
                    state["current"] = None
                    if entries and entries[-1] is entry:
                        entries.pop()
                    continue
                raise _InternalFailure()

            if phase == "directories":
                entries = ledger["directories"]
                if state["current"] is None:
                    if not entries:
                        state["phase"] = "done"
                        continue
                    state["current"] = entries[-1]
                entry = state["current"]
                entry_phase = entry.get("cleanup_phase")
                if entry_phase == "verify":
                    if not entry.get("created"):
                        if entry.get("mkdir_state") == "attempting":
                            try:
                                os.stat(
                                    entry["name"],
                                    dir_fd=entry["parent_fd"],
                                    follow_symlinks=False,
                                )
                            except FileNotFoundError:
                                entry["mkdir_state"] = "unresolved"
                            except BaseException as error:
                                if isinstance(error, Exception):
                                    _task4b_capture_cleanup_failure(
                                        state, error
                                    )
                                    entry["mkdir_state"] = "unresolved"
                                else:
                                    _task4b_capture_cleanup_failure(
                                        state, error
                                    )
                            else:
                                entry["mkdir_state"] = "unresolved"
                            if entry.get("mkdir_state") != "unresolved":
                                continue
                        entry["cleanup_phase"] = "close"
                        continue
                    try:
                        if type(_task4b_registered_slot_fd(entry)) is not int:
                            frozen = entry.get("identity")
                            current = os.stat(
                                entry["name"],
                                dir_fd=entry["parent_fd"],
                                follow_symlinks=False,
                            )
                            if (
                                type(frozen) is not tuple
                                or len(frozen) != 6
                                or not stat.S_ISDIR(current.st_mode)
                                or _metadata_snapshot(current) != frozen
                                or current.st_uid != os.geteuid()
                                or stat.S_IMODE(current.st_mode) != 0o700
                            ):
                                raise _InternalFailure()
                            entry["reopen_state"] = "attempting"
                            _task4b_open_registered_slot(
                                entry,
                                entry["name"],
                                _task4b_directory_flags(),
                                dir_fd=entry["parent_fd"],
                            ); entry["reopen_state"] = "attempted"
                        if not _task4b_capture_directory_cleanup_safe(entry):
                            raise _InternalFailure()
                    except BaseException as error:
                        _task4b_capture_cleanup_failure(state, error)
                        entry["cleanup_phase"] = "close"
                    else:
                        entry["cleanup_phase"] = "rmdir"
                    continue
                if entry_phase == "rmdir":
                    rmdir_state = entry.get("rmdir_state")
                    if rmdir_state == "pending":
                        try:
                            entry["rmdir_state"] = "attempting"; os.rmdir(entry["name"], dir_fd=entry["parent_fd"]); entry["rmdir_state"] = "attempted"
                        except BaseException as error:
                            if isinstance(error, Exception):
                                _task4b_capture_cleanup_failure(state, error)
                                entry["rmdir_error"] = True
                            else:
                                _task4b_capture_cleanup_failure(state, error)
                        else:
                            entry["cleanup_phase"] = "parent_fsync"
                        continue
                    if rmdir_state == "attempting":
                        try:
                            still_safe = (
                                _task4b_capture_directory_cleanup_safe(entry)
                            )
                        except FileNotFoundError:
                            entry.pop("rmdir_error", None)
                            entry["rmdir_state"] = "attempted"
                            entry["cleanup_phase"] = "parent_fsync"
                        except BaseException as error:
                            _task4b_capture_cleanup_failure(state, error)
                            entry.pop("rmdir_error", None)
                            entry["rmdir_state"] = "attempted"
                            entry["cleanup_phase"] = "close"
                        else:
                            if still_safe:
                                if entry.pop("rmdir_error", False):
                                    entry["rmdir_state"] = "attempted"
                                    entry["cleanup_phase"] = "close"
                                else:
                                    entry["rmdir_state"] = "pending"
                            else:
                                _task4b_capture_cleanup_failure(
                                    state, _InternalFailure()
                                )
                                entry.pop("rmdir_error", None)
                                entry["rmdir_state"] = "attempted"
                                entry["cleanup_phase"] = "close"
                        continue
                    if rmdir_state == "attempted":
                        entry["cleanup_phase"] = "parent_fsync"
                        continue
                    raise _InternalFailure()
                if entry_phase == "parent_fsync":
                    fsync_state = entry.get("parent_fsync_state")
                    if fsync_state == "pending":
                        try:
                            entry["parent_fsync_state"] = "attempting"; os.fsync(entry["parent_fd"]); entry["parent_fsync_state"] = "attempted"
                        except BaseException as error:
                            if isinstance(error, Exception):
                                _task4b_capture_cleanup_failure(state, error)
                                entry["parent_fsync_state"] = "attempted"
                            else:
                                _task4b_capture_cleanup_failure(state, error)
                        else:
                            entry["cleanup_phase"] = "close"
                        continue
                    if fsync_state == "attempting":
                        entry["parent_fsync_state"] = "pending"
                        continue
                    if fsync_state == "attempted":
                        entry["cleanup_phase"] = "close"
                        continue
                    raise _InternalFailure()
                if entry_phase == "close":
                    try:
                        _task4b_close_fd_slot(ledger, entry)
                    except BaseException as error:
                        _task4b_capture_cleanup_failure(state, error)
                        continue
                    entry["cleanup_phase"] = "done"
                    continue
                if entry_phase == "done":
                    state["current"] = None
                    if entries and entries[-1] is entry:
                        entries.pop()
                    continue
                raise _InternalFailure()
            raise _InternalFailure()
        return state["control"], state["ordinary"]

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

    def _task4b_root_role(root: Dict[str, Any]) -> str:
        segment = root.get("segment") if type(root) is dict else None
        if segment == "anchor_stage":
            return "anchor_stage"
        if segment == "lower_observation":
            return "lower_observation"
        if segment != "window_root":
            raise _InternalFailure()
        kinds = {
            "header": "headers",
            "reserve": "reserves",
            "price": "prices",
            "fee_history": "fees",
            "final_anchor": "final_anchor",
        }
        role = kinds.get(root.get("kind"))
        if role is None:
            raise _InternalFailure()
        return role

    def _task4b_consume_root_payload(
        record: Dict[str, Any], payload: Tuple[Any, ...]
    ) -> None:
        if type(payload) is not tuple or len(payload) != 6 or payload[0] != "root":
            raise _Task4bReplayMismatch()
        root, role = payload[1], payload[2]
        canonical_payload, row_count, logical_sha256 = payload[3:]
        post_roots = record.get("post_roots")
        joins = record.get("exchange_joins")
        root_records = record.get("root_records")
        if (
            type(root) is not dict
            or type(role) is not str
            or role not in task4b_semantic_roles
            or _task4b_root_role(root) != role
            or type(post_roots) is not list
            or type(root_records) is not list
            or type(joins) is not list
            or root.get("logical_batch_index") != len(post_roots) + 1
        ):
            raise _Task4bReplayMismatch()
        success_indices = root.get("success_exchange_indices")
        if (
            type(success_indices) is not tuple
            or not success_indices
            or not all(type(index) is int and index > 0 for index in success_indices)
            or root.get("leaf_count") != len(success_indices)
        ):
            raise _Task4bReplayMismatch()
        root_joins = []
        for exchange_index in success_indices:
            join = joins[exchange_index - 1] if exchange_index <= len(joins) else None
            if (
                type(join) is not dict
                or tuple(join) != task4b_provisional_join_keys
                or join.get("exchange_index") != exchange_index
                or join.get("logical_batch_index")
                != root["logical_batch_index"]
            ):
                raise _Task4bReplayMismatch()
            root_joins.append(join)
        rows = None
        if role in task4b_typed_roles:
            if (
                type(canonical_payload) is not bytes
                or type(row_count) is not int
                or row_count <= 0
                or not _exact_sha256(logical_sha256)
                or root.get("typed_role") != role
                or root.get("typed_row_count") != row_count
                or root.get("typed_logical_sha256") != logical_sha256
            ):
                raise _Task4bReplayMismatch()
            try:
                refs, rows = _task4b_append_typed_root(
                    record,
                    role=role,
                    canonical_payload=canonical_payload,
                    row_count=row_count,
                    logical_sha256=logical_sha256,
                )
            except _InternalFailure:
                raise _Task4bReplayMismatch()
            record["typed_counts"][role] += row_count
            block_key = "number" if role == "headers" else "block_number"
            if (
                root.get("block_start") != rows[0][block_key]
                or root.get("block_stop") != rows[-1][block_key]
            ):
                raise _Task4bReplayMismatch()
            if role == "headers":
                header_range = record["header_range"]
                if header_range["count"] == 0:
                    header_range["lower_bound_number"] = rows[0]["number"]
                elif rows[0]["number"] != header_range["anchor_number"] + 1:
                    raise _Task4bReplayMismatch()
                header_range["anchor_number"] = rows[-1]["number"]
                header_range["anchor_timestamp"] = rows[-1]["timestamp"]
                header_range["count"] += len(rows)
        else:
            if (
                canonical_payload is not None
                or type(row_count) is not int
                or row_count != 0
                or logical_sha256 is not None
            ):
                raise _Task4bReplayMismatch()
            refs = []
        for join in root_joins:
            join["typed_role"] = role
            join["typed_chunk_refs"] = refs
            if tuple(join) != task4b_final_join_keys:
                raise _Task4bReplayMismatch()
        leaves = [_task4b_post_leaf_from_join(join) for join in root_joins]
        if root.get("leaf_ledger_sha256") != _task4b_inventory_digest(
            b"historical_foundry_leaf_ledger/v1", leaves
        ):
            raise _Task4bReplayMismatch()
        post_roots.append(root)
        root_records.append({
            "logical_batch_index": root["logical_batch_index"],
            "role": role,
            "row_count": row_count,
            "logical_sha256": logical_sha256,
            "refs": refs,
            "success_exchange_indices": success_indices,
        })
        del rows, root_joins, leaves
        return None

    def _task4b_consume_finish_payload(
        record: Dict[str, Any], payload: Tuple[Any, ...]
    ) -> None:
        if (
            type(payload) is not tuple
            or len(payload) != 5
            or payload[0] != "finish"
            or record.get("finish_payload") is not None
        ):
            raise _Task4bReplayMismatch()
        exchange_count, counts, prefinalization, reconciliation = payload[1:]
        if (
            type(exchange_count) is not int
            or exchange_count <= 0
            or type(counts) is not tuple
            or len(counts) != 4
            or tuple(pair[0] if type(pair) is tuple and len(pair) == 2 else None for pair in counts)
            != task4b_typed_roles
            or any(
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not str
                or type(pair[1]) is not int
                or pair[1] <= 0
                for pair in counts
            )
        ):
            raise _Task4bReplayMismatch()
        observed = record.get("typed_counts")
        if (
            type(observed) is not dict
            or tuple((role, observed.get(role)) for role in task4b_typed_roles)
            != counts
            or counts[1][1] != 2 * counts[0][1]
            or counts[2][1] != counts[0][1]
            or counts[3][1] != counts[0][1]
            or not 1 <= counts[0][1] <= 50_401
            or exchange_count != len(record.get("exchange_joins", ()))
            or any(
                type(join) is not dict or tuple(join) != task4b_final_join_keys
                for join in record["exchange_joins"]
            )
        ):
            raise _Task4bReplayMismatch()
        try:
            _task4b_validate_digest_bindings(prefinalization, reconciliation)
            _task4b_flush_typed_builder(record)
        except _InternalFailure:
            raise _Task4bReplayMismatch()
        if (
            reconciliation[1] != len(record["post_roots"])
            or reconciliation[3] != exchange_count
        ):
            raise _Task4bReplayMismatch()
        record["finish_payload"] = payload
        return None

    def _task4b_move_source_descriptor_slot(
        row: Tuple[Any, ...], *, row_kind: str, index: int
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        if (
            type(row) is not tuple
            or row_kind not in ("ancestry", "source")
            or type(index) is not int
            or index < 0
            or len(row) not in (5, 9)
            or type(row[1]) is not int
        ):
            raise _InternalFailure()
        slot = {
            "fd": row[1],
            "acquisition_state": "attempted",
            "close_state": "pending",
            "move_state": "pending",
            "row_kind": row_kind,
            "index": index,
            "metadata": row[:1] + row[2:],
        }
        emptied = list(row)
        emptied[1] = None
        return tuple(emptied), slot

    def _task4b_move_bound_source_authority(
        owner: Dict[str, Any], binding: object,
        binding_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        checker = binding_record.get("task4b_currentness_checker")
        if not callable(checker):
            raise _InternalFailure()
        checker()
        authority = {
            "checker": checker,
            "ancestry_slots": [],
            "source_slots": [],
            "close_state": "live",
        }
        owner["_task4b_snapshot_source_authority"] = authority
        for key, kind in (
            ("ancestry_rows", "ancestry"),
            ("source_rows", "source"),
        ):
            rows = binding_record.get(key)
            if type(rows) is not tuple:
                raise _InternalFailure()
            emptied_rows = list(rows)
            slots = authority[kind + "_slots"]
            for index, row in enumerate(rows):
                emptied, slot = _task4b_move_source_descriptor_slot(
                    row, row_kind=kind, index=index
                )
                slot["binding_record"] = binding_record
                slot["binding_key"] = key
                slots.append(slot)
                slot["move_state"] = "attempting"
                emptied_rows[index] = emptied
                binding_record[key] = tuple(emptied_rows)
                slot["move_state"] = "attempted"
                slot.pop("binding_record", None)
                slot.pop("binding_key", None)
            authority[kind + "_slots"] = tuple(slots)
        _close_bound_source_rows(binding, binding_record)
        binding_registry.pop(id(binding), None)
        owner["binding"] = None
        return authority

    def _task4b_verify_snapshot_source_authority(
        authority: Dict[str, Any]
    ) -> None:
        if (
            type(authority) is not dict
            or authority.get("close_state") != "live"
            or not callable(authority.get("checker"))
        ):
            raise _BoundSourceIdentityDrift()
        authority["checker"]()
        ancestry_fds = []
        for slot in authority.get("ancestry_slots", ()):
            metadata = slot.get("metadata") if type(slot) is dict else None
            fd = slot.get("fd") if type(slot) is dict else None
            if type(metadata) is not tuple or len(metadata) != 4:
                raise _BoundSourceIdentityDrift()
            components, parent_index, name, identity = metadata
            if (
                type(fd) is not int
                or os.get_inheritable(fd)
                or _metadata_snapshot(os.fstat(fd)) != identity
            ):
                raise _BoundSourceIdentityDrift()
            if parent_index is None:
                if slot.get("index") != 0 or components != () or name is not None:
                    raise _BoundSourceIdentityDrift()
            elif (
                type(parent_index) is not int
                or not 0 <= parent_index < len(ancestry_fds)
                or type(name) is not str
                or _metadata_snapshot(os.stat(
                    name,
                    dir_fd=ancestry_fds[parent_index],
                    follow_symlinks=False,
                )) != identity
            ):
                raise _BoundSourceIdentityDrift()
            ancestry_fds.append(fd)
        for slot in authority.get("source_slots", ()):
            metadata = slot.get("metadata") if type(slot) is dict else None
            fd = slot.get("fd") if type(slot) is dict else None
            if type(metadata) is not tuple or len(metadata) != 8:
                raise _BoundSourceIdentityDrift()
            (
                role, parent_index, name, relative, identity,
                expected_bytes, expected_size, expected_sha256,
            ) = metadata
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
                or hashlib.sha256(observed).hexdigest() != expected_sha256
            ):
                raise _BoundSourceIdentityDrift()
        return None

    def _task4b_close_snapshot_source_authority(
        owner: Dict[str, Any]
    ) -> Tuple[Optional[BaseException], bool]:
        authority = owner.get("_task4b_snapshot_source_authority")
        if type(authority) is not dict or authority.get("close_state") == "done":
            return None, False
        ledger = owner.get("_task4b_staging")
        first_control = None
        ordinary = False
        for slot in reversed(
            tuple(authority.get("source_slots", ()))
            + tuple(authority.get("ancestry_slots", ()))
        ):
            try:
                move_state = slot.get("move_state")
                if move_state == "attempting":
                    binding_record = slot.get("binding_record")
                    binding_key = slot.get("binding_key")
                    index = slot.get("index")
                    rows = (
                        binding_record.get(binding_key)
                        if type(binding_record) is dict else None
                    )
                    if (
                        type(rows) is tuple
                        and type(index) is int
                        and 0 <= index < len(rows)
                        and rows[index][1] == slot.get("fd")
                    ):
                        continue
                    if (
                        type(rows) is not tuple
                        or type(index) is not int
                        or not 0 <= index < len(rows)
                        or rows[index][1] is not None
                    ):
                        raise _InternalFailure()
                elif move_state != "attempted":
                    continue
                _task4b_close_fd_slot(ledger, slot)
            except BaseException as error:
                if isinstance(error, Exception):
                    ordinary = True
                elif first_control is None:
                    first_control = error
        authority["checker"] = None
        authority["close_state"] = "done"
        return first_control, ordinary

    def _task4b_snapshot_members(
        owner: Dict[str, Any], source_record: Dict[str, Any]
    ) -> Tuple[Dict[str, Dict[str, Any]], str, int]:
        ledger = owner["_task4b_staging"]
        rows = {}
        for row in ledger["config_rows"]:
            rows[row["path"]] = {
                "size": row["byte_count"], "sha256": row["sha256"],
                "cap": 1_048_576, "kind": "config",
            }
        for row in source_record["raw_chunks"]:
            rows[row["path"]] = {
                "size": row["byte_count"], "sha256": row["sha256"],
                "cap": 16_777_216, "kind": "raw",
            }
        for row in source_record["typed_chunks"]:
            rows[row["path"]] = {
                "size": row["gzip_byte_count"],
                "sha256": row["gzip_sha256"],
                "cap": 16_842_752, "kind": "gzip",
            }
        inventory = _task4b_reread_capture_member(
            ledger,
            relative_path="scan/capture_inventory.json",
            expected_size=source_record["inventory_byte_count"],
            maximum_size=16_777_216,
            size_kind="inventory",
        )
        inventory_sha256 = hashlib.sha256(inventory).hexdigest()
        rows["scan/capture_inventory.json"] = {
            "size": len(inventory), "sha256": inventory_sha256,
            "cap": 16_777_216, "kind": "inventory",
        }
        return rows, inventory_sha256, sum(
            row["size"] for row in rows.values()
        )

    def _task4b_retire_committed_spool(
        owner: Dict[str, Any], source_record: Dict[str, Any]
    ) -> None:
        journal = owner.get("_task4b_spool_retirement")
        if journal is None:
            basename = owner.get("basename")
            identity = owner.get("file_identity")
            chain = owner.get("chain")
            prefix = ".historical-foundry-exchange-spool-"
            suffix = ".bin"
            token = (
                basename[len(prefix):-len(suffix)]
                if type(basename) is str
                and basename.startswith(prefix)
                and basename.endswith(suffix)
                else None
            )
            if (
                type(owner) is not dict
                or type(source_record) is not dict
                or type(basename) is not str
                or type(token) is not str
                or len(token) != 32
                or any(character not in "0123456789abcdef" for character in token)
                or type(identity) is not tuple
                or type(chain) is not tuple
                or not chain
                or type(chain[-1]) is not tuple
                or type(chain[-1][0]) is not int
                or type(owner.get("file_fd")) is not int
            ):
                raise _InternalFailure()
            _require_relative_basename(basename)
            journal = {
                "phase": "verify",
                "basename": basename,
                "identity": identity,
                "parent_fd": chain[-1][0],
                "close_slot": None,
                "control": None,
                "ordinary": False,
            }
            owner["_task4b_spool_retirement"] = journal
        if type(journal) is not dict:
            raise _InternalFailure()

        while journal.get("phase") != "done":
            phase = journal.get("phase")
            if phase == "verify":
                raw_records = source_record.get("raw_exchange_records")
                retained_records = owner.get("_task4b_raw_exchange_records")
                inventory = owner.get("inventory")
                committed_eof = owner.get("committed_eof")
                if (
                    type(raw_records) is not list
                    or type(retained_records) is not tuple
                    or type(inventory) is not list
                    or not raw_records
                    or len(raw_records) != len(retained_records)
                    or len(raw_records) != len(inventory)
                    or any(
                        current is not retained
                        for current, retained in zip(
                            raw_records, retained_records
                        )
                    )
                    or type(committed_eof) is not int
                    or committed_eof <= 0
                    or source_record.get("next_spool_offset")
                    != committed_eof
                    or owner.get("basename") != journal.get("basename")
                    or owner.get("file_identity") != journal.get("identity")
                    or owner.get("chain")[-1][0]
                    != journal.get("parent_fd")
                ):
                    raise _InternalFailure()
                expected_offset = 0
                for position, (raw_row, receipt) in enumerate(
                    zip(raw_records, inventory), 1
                ):
                    receipt_record = _receipt_for_spool(owner, receipt)
                    projection = receipt_record.get("projection")
                    compact = (
                        raw_row.get("projection")
                        if type(raw_row) is dict else None
                    )
                    if (
                        type(raw_row) is not dict
                        or tuple(raw_row) != (
                            "projection", "raw_chunk_path", "raw_offset",
                            "raw_physical_verified",
                        )
                        or raw_row.get("raw_physical_verified") is not True
                        or type(raw_row.get("raw_chunk_path")) is not str
                        or not raw_row["raw_chunk_path"].startswith("rpc/")
                        or type(raw_row.get("raw_offset")) is not int
                        or raw_row["raw_offset"] < 0
                        or receipt_record.get("state") != "committed"
                        or type(projection) is not dict
                        or type(compact) is not dict
                        or tuple(projection) != receipt_keys
                        or tuple(compact) != receipt_keys
                        or compact.get("schema")
                        != "historical_foundry_archive_rpc_spooled_success_exchange/v1"
                        or projection.get("exchange_index") != position
                        or projection.get("spool_member_index") != position
                        or projection.get("spool_offset") != expected_offset
                        or any(
                            compact[key] != projection[key]
                            for key in receipt_keys[1:]
                        )
                        or type(projection.get("spool_length")) is not int
                        or projection["spool_length"] <= 0
                    ):
                        raise _InternalFailure()
                    expected_offset += projection["spool_length"]
                if expected_offset != committed_eof:
                    raise _InternalFailure()
                _verify_file_entry(owner, expected_size=committed_eof)
                opened = os.fstat(owner["file_fd"])
                current = os.stat(
                    journal["basename"],
                    dir_fd=journal["parent_fd"],
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or not stat.S_ISREG(current.st_mode)
                    or _file_identity(opened) != journal["identity"]
                    or _file_identity(current) != journal["identity"]
                    or opened.st_nlink != 1
                    or current.st_nlink != 1
                    or opened.st_uid != os.geteuid()
                    or current.st_uid != os.geteuid()
                    or stat.S_IMODE(opened.st_mode) != 0o600
                    or stat.S_IMODE(current.st_mode) != 0o600
                    or opened.st_size != committed_eof
                    or current.st_size != committed_eof
                ):
                    raise _InternalFailure()
                journal["phase"] = "unlink"
                continue

            if phase == "unlink":
                controlled = False
                try:
                    os.unlink(
                        journal["basename"],
                        dir_fd=journal["parent_fd"],
                    )
                except BaseException as error:
                    if isinstance(error, Exception):
                        raise
                    if journal.get("control") is None:
                        journal["control"] = error
                    controlled = True
                    del error
                if controlled:
                    try:
                        current = os.stat(
                            journal["basename"],
                            dir_fd=journal["parent_fd"],
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        journal["phase"] = "parent_fsync"
                        continue
                    if _file_identity(current) != journal["identity"]:
                        raise _InternalFailure()
                    continue
                journal["phase"] = "parent_fsync"
                continue

            if phase == "parent_fsync":
                controlled = False
                try:
                    os.fsync(journal["parent_fd"])
                except BaseException as error:
                    if isinstance(error, Exception):
                        raise
                    if journal.get("control") is None:
                        journal["control"] = error
                    controlled = True
                    del error
                if controlled:
                    continue
                journal["phase"] = "close"
                continue

            if phase == "close":
                controlled = False
                ordinary = False
                try:
                    if journal.get("close_slot") is None:
                        journal["close_slot"] = {
                            "fd": owner.get("file_fd"),
                            "acquisition_state": "attempted",
                            "close_state": "pending",
                        }; owner["file_fd"] = None
                    _task4b_close_fd_slot(
                        owner["_task4b_staging"],
                        journal["close_slot"],
                    )
                except BaseException as error:
                    if isinstance(error, Exception):
                        ordinary = True
                    elif journal.get("control") is None:
                        journal["control"] = error
                    controlled = not isinstance(error, Exception)
                    del error
                if controlled or ordinary:
                    try:
                        _task4b_close_fd_slot(
                            owner["_task4b_staging"],
                            journal["close_slot"],
                        )
                    except BaseException as error:
                        if isinstance(error, Exception):
                            ordinary = True
                        elif journal.get("control") is None:
                            journal["control"] = error
                        del error
                close_state = journal["close_slot"].get("close_state")
                if close_state not in ("attempted", "unresolved"):
                    continue
                journal["ordinary"] = journal.get("ordinary", False) or ordinary
                journal["phase"] = "done"
                continue
            raise _InternalFailure()

        first_control = journal.get("control")
        ordinary = journal.get("ordinary") is True
        owner["file_fd"] = None
        owner["basename"] = None
        owner["file_identity"] = None
        journal.clear()
        journal["phase"] = "done"
        if first_control is not None:
            raise first_control
        if ordinary:
            raise _InternalFailure()
        return None

    def _task4b_install_capture_snapshot(
        view: object,
        owner: Dict[str, Any],
        binding: object,
        binding_record: Dict[str, Any],
        source: object,
        source_record: Dict[str, Any],
        delivery_guard: List[Any],
    ) -> object:
        _verify_bound_source_current(binding, binding_record)
        members, inventory_sha256, frozen_bytes = _task4b_snapshot_members(
            owner, source_record
        )
        frozen_record = {
            "owner": owner,
            "exchange_joins": [
                dict(row) for row in source_record["exchange_joins"]
            ],
            "raw_chunks": [dict(row) for row in source_record["raw_chunks"]],
            "typed_chunks": [
                dict(row) for row in source_record["typed_chunks"]
            ],
            "post_roots": [dict(row) for row in source_record["post_roots"]],
            "root_records": [
                dict(row) for row in source_record["root_records"]
            ],
            "finish_payload": source_record["finish_payload"],
            "inventory_byte_count": source_record["inventory_byte_count"],
        }
        error_class = binding_record.get("rpc_error_class")
        _task4b_retire_committed_spool(owner, source_record)
        source_authority = _task4b_move_bound_source_authority(
            owner, binding, binding_record
        )
        owner["_task4b_snapshot_source_authority"] = source_authority
        owner["_task4b_frozen_record"] = frozen_record
        owner["_task4b_snapshot_members"] = members
        owner["_task4b_rpc_error_class"] = error_class
        ledger = owner["_task4b_staging"]
        ledger.pop("quota_owner_handle", None)
        quota_record = _quota_record_for_owner(owner)
        owner_generation = owner["owner_generation"] + 1
        owner["owner_generation"] = owner_generation
        owner["capture_generation"] = 1
        owner["state"] = "capture_frozen"
        owner["binding"] = None
        owner["reconciliation"] = None
        owner["capture_replay_source"] = None
        owner["_task4b_snapshot_projection"] = {
            "schema": "historical_foundry_staging_snapshot_identity/v1",
            "stage": "capture_frozen",
            "generation": 1,
            "capture_inventory_sha256": inventory_sha256,
            "frozen_member_count": len(members),
            "frozen_physical_byte_count": frozen_bytes,
            "quota_committed_physical_bytes": quota_record[
                "committed_physical_bytes"
            ],
            "quota_committed_member_count": quota_record["committed_members"],
        }
        for consumerless_key in (
            "_task4b_raw_exchange_records",
            "_task4b_exchange_joins",
            "_task4b_raw_chunks",
            "_task4b_typed_chunks",
            "_task4b_capture_phase",
        ):
            owner.pop(consumerless_key, None)
        snapshot = _prepare_handle(HistoricalRunStagingSnapshot, owner)
        owner["_task4b_snapshot_handle"] = snapshot
        owner["_task4b_snapshot_owner_generation"] = owner_generation
        staging_snapshot_registry[id(snapshot)] = (snapshot, owner)
        delivery_guard[0] = (snapshot, owner, staging_snapshot_registry)
        owner["_task4b_delivery_view_ref"] = weakref.ref(view)
        owner["_task4b_delivery_guard_phase"] = "armed"
        source_record["state"] = "closed"
        source_record["source"] = None
        source_record["view"] = None
        source_record["owner"] = None
        source_record["binding"] = None
        source_record["reconciliation"] = None
        source_record["compact_rows"] = None
        source_record["raw_builder"].clear()
        source_record["raw_builder_rows"].clear()
        source_record["typed_builder"].clear()
        _retire_nonowner_handle(
            source, replay_source_registry, replay_source_tombstones
        )
        return snapshot

    def _materialize_task4b_capture_core(
        view: object, delivery_guard: List[Any]
    ) -> object:
        entry = consumed_view_registry.get(id(view))
        if (
            type(view)
            is not _ConsumedProductionHistoricalWindowCapabilityView
            or entry is None
            or entry[0] is not view
        ):
            _raise_storage_error()
        owner = entry[1]
        binding = owner.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if (
            owner.get("constructor") is not constructor_provenance
            or owner.get("state") != "consumed_view"
            or binding_entry is None
            or binding_entry[0] is not binding
        ):
            _raise_storage_error()
        binding_record = binding_entry[1]
        if (
            binding_record.get("state") != "live"
            or binding_record.get("owner_kind") != "consumed_view"
            or binding_record.get("owner_handle") is not view
            or binding_record.get("owner_generation")
            != owner.get("owner_generation")
            or binding_record.get("lineage") is not owner.get("lineage")
        ):
            _raise_task4b_capability_invalid(view)
        _verify_bound_source_current(binding, binding_record)
        next_generation = owner["owner_generation"] + 1
        owner["state"] = "capture_materializing"
        owner["owner_generation"] = next_generation
        owner["capture_generation"] = 0
        binding_record["owner_kind"] = "capture_materializing"
        binding_record["owner_handle"] = view
        binding_record["owner_generation"] = next_generation
        _verify_bound_source_current(binding, binding_record)
        task4b_rows = binding_record.get("task4b_bound_objects")
        if (
            type(task4b_rows) is not tuple
            or len(task4b_rows) != 3
            or type(task4b_rows[0]) is not tuple
            or len(task4b_rows[0]) != 5
            or type(task4b_rows[2]) is not tuple
            or len(task4b_rows[2]) != 12
        ):
            raise _bound_source_drift(binding_record)
        _task4b_prepare_config_staging(view, owner, binding_record)
        _verify_bound_source_current(binding, binding_record)
        binder = task4b_rows[2][1]
        if (
            binder is not task4b_rows[0][2]
            or not callable(binder)
            or owner.get("reconciliation") is None
            or owner.get("capture_replay_source") is not None
        ):
            _raise_task4b_capability_invalid(view)
        finalization = owner.get("claimed_finalization")
        invalid_finalization = False
        try:
            compact_rows = finalization["successful_exchanges"]
        except (KeyError, TypeError, ValueError):
            invalid_finalization = True
        if invalid_finalization:
            raise _task4b_bound_error(
                binding_record,
                "historical_window_reconciliation_mismatch",
            ) from None
        inventory = owner.get("inventory")
        if (
            type(compact_rows) is not tuple
            or type(inventory) is not list
            or len(compact_rows) != len(inventory)
            or not compact_rows
            or any(
                type(row) is not dict
                or tuple(row) != receipt_keys
                or row.get("schema")
                != "historical_foundry_archive_rpc_spooled_success_exchange/v1"
                or row.get("exchange_index") != expected_index
                for expected_index, row in enumerate(compact_rows, 1)
            )
        ):
            raise _task4b_bound_error(
                binding_record,
                "historical_window_reconciliation_mismatch",
            )
        source_record = {
            "constructor": constructor_provenance,
            "lineage": owner["lineage"],
            "state": "fresh",
            "source": None,
            "view": view,
            "owner": owner,
            "binding": binding,
            "reconciliation": owner["reconciliation"],
            "owner_generation": next_generation,
            "capture_generation": 0,
            "position": 0,
            "iterated": False,
            "eof": False,
            "next_request_id": 1,
            "next_spool_offset": 0,
            "next_raw_chunk_index": 1,
            "compact_rows": compact_rows,
            "raw_builder": bytearray(),
            "raw_builder_rows": [],
            "raw_chunks": [],
            "raw_exchange_records": [],
            "exchange_joins": [],
            "post_roots": [],
            "root_records": [],
            "finish_payload": None,
            "typed_builder": {
                "role": None,
                "row_bytes": [],
                "row_count": 0,
                "decoded_size": 2,
                "block_start": None,
                "block_stop": None,
            },
            "typed_chunks": [],
            "next_typed_chunk_indices": {
                role: 1 for role in task4b_typed_roles
            },
            "typed_role_position": -1,
            "typed_counts": {role: 0 for role in task4b_typed_roles},
            "header_range": {
                "lower_bound_number": None,
                "anchor_number": None,
                "anchor_timestamp": None,
                "count": 0,
            },
        }
        source = _prepare_handle(
            _HistoricalWindowCaptureReplaySource, source_record
        )
        source_record["source"] = source
        owner["capture_replay_source"] = source
        replay_source_registry[id(source)] = (source, source_record)
        binder(
            reconciliation=owner["reconciliation"],
            source=source,
        )
        if source_record.get("state") != "bound_verified":
            _raise_task4b_capability_invalid(view)
        replay = task4b_rows[0][3]
        consumer = task4b_rows[0][4]
        if (
            replay is not task4b_rows[2][2]
            or consumer is not task4b_rows[2][3]
            or not callable(replay)
            or not callable(consumer)
        ):
            _raise_task4b_capability_invalid(view)
        event_stream = replay(source=source)
        event_index = 0
        exchange_count = 0
        try:
            for event in event_stream:
                payload = consumer(
                    event=event,
                    expected_source=source,
                    expected_event_index=event_index,
                )
                if source_record.get("finish_payload") is not None:
                    raise _Task4bReplayMismatch()
                if type(payload) is not tuple or not payload:
                    raise _Task4bReplayMismatch()
                if payload[0] == "exchange":
                    raw_exchanges = source_record.get(
                        "raw_exchange_records"
                    )
                    if (
                        len(payload) != 3
                        or type(payload[1]) is not dict
                        or tuple(payload[1]) != receipt_keys
                        or type(payload[2]) is not dict
                        or tuple(payload[2]) != task4b_post_leaf_keys
                        or type(raw_exchanges) is not list
                        or len(raw_exchanges) != exchange_count + 1
                    ):
                        raise _Task4bReplayMismatch()
                    compact = payload[1]
                    post_leaf = payload[2]
                    raw_row = raw_exchanges[exchange_count]
                    projection = raw_row.get("projection")
                    if (
                        type(projection) is not dict
                        or compact != projection
                        or compact.get("exchange_index") != exchange_count + 1
                        or post_leaf.get("exchange_index") != exchange_count + 1
                        or post_leaf.get("logical_batch_index")
                        != compact.get("logical_batch_index")
                        or type(raw_row.get("raw_chunk_path")) is not str
                        or type(raw_row.get("raw_offset")) is not int
                        or raw_row["raw_offset"] < 0
                    ):
                        raise _Task4bReplayMismatch()
                    join = dict(compact)
                    for key in (
                        "segment",
                        "segment_local_index",
                        "leaf_index",
                        "wire_hash_authority",
                    ):
                        join[key] = post_leaf[key]
                    join["raw_chunk_path"] = raw_row["raw_chunk_path"]
                    join["raw_chunk_offset"] = raw_row["raw_offset"]
                    if tuple(join) != task4b_provisional_join_keys:
                        raise _Task4bReplayMismatch()
                    source_record["exchange_joins"].append(join)
                    exchange_count += 1
                    del compact, post_leaf, raw_row, projection, raw_exchanges
                elif payload[0] == "root":
                    _task4b_consume_root_payload(source_record, payload)
                elif payload[0] == "finish":
                    _task4b_consume_finish_payload(source_record, payload)
                else:
                    raise _Task4bReplayMismatch()
                event_index += 1
                del event, payload
        finally:
            event_stream.close()
        raw_exchanges = source_record.get("raw_exchange_records")
        raw_chunks = source_record.get("raw_chunks")
        exchange_joins = source_record.get("exchange_joins")
        post_roots = source_record.get("post_roots")
        typed_chunks = source_record.get("typed_chunks")
        finish = source_record.get("finish_payload")
        if (
            source_record.get("state") != "complete"
            or source_record.get("eof") is not True
            or type(raw_exchanges) is not list
            or type(raw_chunks) is not list
            or type(exchange_joins) is not list
            or type(post_roots) is not list
            or type(typed_chunks) is not list
            or type(finish) is not tuple
            or event_index != exchange_count + len(post_roots) + 1
            or exchange_count != len(owner.get("inventory", ()))
            or len(raw_exchanges) != exchange_count
            or len(exchange_joins) != exchange_count
            or not raw_chunks
            or not typed_chunks
            or any(
                type(row) is not dict
                or row.get("raw_physical_verified") is not True
                for row in raw_exchanges
            )
        ):
            raise _Task4bReplayMismatch()
        header_range = source_record.get("header_range")
        ledger = owner.get("_task4b_staging")
        policy = ledger.get("policy_value") if type(ledger) is dict else None
        lookback = policy.get("lookback_seconds") if type(policy) is dict else None
        if (
            type(header_range) is not dict
            or type(lookback) is not int
            or lookback <= 0
            or header_range.get("count") != finish[2][0][1]
            or type(header_range.get("lower_bound_number")) is not int
            or type(header_range.get("anchor_number")) is not int
            or type(header_range.get("anchor_timestamp")) is not int
            or header_range["anchor_number"]
            - header_range["lower_bound_number"] + 1
            != header_range["count"]
        ):
            raise _Task4bReplayMismatch()
        range_row = {
            "lower_bound_number": header_range["lower_bound_number"],
            "anchor_number": header_range["anchor_number"],
            "cutoff_timestamp": header_range["anchor_timestamp"] - lookback,
            "block_count": header_range["count"],
        }
        source_record["raw_chunks"] = raw_chunks
        source_record["typed_chunks"] = typed_chunks
        owner["_task4b_exchange_joins"] = tuple(exchange_joins)
        owner["_task4b_raw_chunks"] = tuple(raw_chunks)
        owner["_task4b_typed_chunks"] = tuple(typed_chunks)
        try:
            capture_inventory = _task4b_build_capture_inventory(
                source_record,
                config_rows=ledger["config_rows"],
                raw_chunks=raw_chunks,
                typed_chunks=typed_chunks,
                range_row=range_row,
            )
            inventory_bytes = _task4b_canonical_json_bytes(capture_inventory)
            captured_capture_inventory_size(byte_count=len(inventory_bytes))
            _task4b_write_capture_member(
                ledger,
                ledger["role_directories"]["scan"],
                "capture_inventory.json",
                inventory_bytes,
            )
            source_record["inventory_byte_count"] = len(inventory_bytes)
            del capture_inventory, inventory_bytes
            _task4b_freeze_audit(source_record)
        except (KeyError, TypeError, ValueError, _InternalFailure):
            raise _Task4bReplayMismatch()
        owner["_task4b_capture_phase"] = "audit_complete"
        return _task4b_install_capture_snapshot(
            view,
            owner,
            binding,
            binding_record,
            source,
            source_record,
            delivery_guard,
        )

    def _materialize_task4b_capture(view: object) -> object:
        binding_record = _task4b_binding_record_for_view(view)
        delivery_guard = [None]
        body_error = None
        body_traceback = None
        try:
            return _materialize_task4b_capture_core(view, delivery_guard)
        except BaseException as observed_body_error:
            body_error = observed_body_error
            body_traceback = observed_body_error.__traceback__
            del observed_body_error
        cleanup_control = None
        try:
            moved = delivery_guard[0]
            if moved is not None:
                _close_moved_owner(
                    moved[0], moved[1], moved[2], "closed_nonowning"
                )
            else:
                entry = consumed_view_registry.get(id(view))
            if moved is None and entry is not None and entry[0] is view:
                _close_moved_owner(
                    view,
                    entry[1],
                    consumed_view_registry,
                    "closed_nonowning",
                )
        except BaseException as observed_cleanup_error:
            if not isinstance(observed_cleanup_error, Exception):
                cleanup_control = observed_cleanup_error
            del observed_cleanup_error
        if not isinstance(body_error, Exception):
            raise body_error.with_traceback(body_traceback)
        if cleanup_control is not None:
            raise cleanup_control from None
        error_class = (
            binding_record.get("rpc_error_class")
            if type(binding_record) is dict else None
        )
        if type(body_error) is error_class:
            raise body_error.with_traceback(body_traceback) from None
        if type(binding_record) is dict:
            raise _task4b_bound_error(
                binding_record,
                "historical_window_spool_handoff_failed",
            ) from None
        _raise_storage_error()

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
            if (
                record.get("state") == "capture_frozen"
                and record.get("_task4b_delivery_guard_phase") == "armed"
            ):
                snapshot = record.get("_task4b_snapshot_handle")
                if type(snapshot) is not HistoricalRunStagingSnapshot:
                    _raise_storage_error()
                record["_task4b_delivery_guard_phase"] = "closing"
                _retire_nonowner_handle(
                    self,
                    consumed_view_registry,
                    consumed_view_tombstones,
                )
                return snapshot.close()
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
            if record["state"] not in (
                "consumed_view", "capture_materializing"
            ):
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

        def _materialize_staging_snapshot_from_bound_scan(
            self,
        ) -> "HistoricalRunStagingSnapshot":
            return _materialize_task4b_capture(self)

    consumed_view_authorized[0] = (
        _ConsumedProductionHistoricalWindowCapabilityView
    )

    def _reject_task4b_source_protocol(source: object) -> None:
        entry = replay_source_registry.get(id(source))
        view = None
        if entry is not None and entry[0] is source:
            record = entry[1]
            view = record.get("view")
            if record.get("state") not in ("closed", "misuse_failed"):
                record["state"] = "misuse_failed"
        _raise_task4b_capability_invalid(view)

    def _task4b_current_replay_source_record(
        source: object, expected_states: Tuple[str, ...]
    ) -> Dict[str, Any]:
        entry = replay_source_registry.get(id(source))
        if (
            type(expected_states) is not tuple
            or not expected_states
            or entry is None
            or entry[0] is not source
        ):
            _reject_task4b_source_protocol(source)
        record = entry[1]
        owner = record.get("owner")
        view = record.get("view")
        binding = record.get("binding")
        view_entry = consumed_view_registry.get(id(view))
        binding_entry = binding_registry.get(id(binding))
        if (
            record.get("constructor") is not constructor_provenance
            or record.get("source") is not source
            or record.get("state") not in expected_states
            or type(owner) is not dict
            or view_entry is None
            or view_entry[0] is not view
            or view_entry[1] is not owner
            or binding_entry is None
            or binding_entry[0] is not binding
            or owner.get("state") != "capture_materializing"
            or owner.get("owner_generation")
            != record.get("owner_generation")
            or owner.get("capture_generation") != 0
            or record.get("capture_generation") != 0
            or binding_entry[1].get("owner_kind")
            != "capture_materializing"
            or binding_entry[1].get("owner_handle") is not view
            or binding_entry[1].get("owner_generation")
            != owner.get("owner_generation")
            or type(record.get("compact_rows")) is not tuple
        ):
            _reject_task4b_source_protocol(source)
        source_currentness_drifted = False
        try:
            _verify_task4b_bound_source_current(
                binding, binding_entry[1]
            )
        except _BoundSourceIdentityDrift:
            source_currentness_drifted = True
        if source_currentness_drifted:
            raise _bound_source_drift(binding_entry[1])
        _verify_file_entry(
            owner, expected_size=owner.get("committed_eof")
        )
        return record

    def _task4b_flush_raw_builder(record: Dict[str, Any]) -> None:
        builder = record.get("raw_builder")
        frame_rows = record.get("raw_builder_rows")
        if (
            type(builder) is not bytearray
            or type(frame_rows) is not list
        ):
            raise _InternalFailure()
        if not frame_rows:
            if builder:
                raise _InternalFailure()
            return None
        payload = bytes(builder)
        chunk_index = record.get("next_raw_chunk_index")
        write_failed = False
        failed_binding_record = None
        try:
            row = _task4b_write_raw_chunk(
                record["owner"]["_task4b_staging"],
                chunk_index=chunk_index,
                payload=payload,
                frame_rows=tuple(frame_rows),
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            write_failed = True
            binding = record.get("binding")
            binding_entry = binding_registry.get(id(binding))
            if (
                binding_entry is not None
                and binding_entry[0] is binding
            ):
                failed_binding_record = binding_entry[1]
        if write_failed:
            if failed_binding_record is not None:
                raise _task4b_bound_error(
                    failed_binding_record,
                    "historical_window_spool_handoff_failed",
                )
            _raise_storage_error()
        raw_chunks = record.get("raw_chunks")
        if type(raw_chunks) is not list:
            raise _InternalFailure()
        raw_chunks.append(row)
        record["next_raw_chunk_index"] = chunk_index + 1
        builder.clear()
        frame_rows.clear()
        del payload
        return None

    def _task4b_next_replay_source_frame(
        source: object,
    ) -> Tuple[Mapping[str, Any], bytes, bytes]:
        record = _task4b_current_replay_source_record(
            source, ("iterating",)
        )
        owner = record["owner"]
        position = record.get("position")
        inventory = owner.get("inventory")
        if (
            type(position) is not int
            or position < 0
            or type(inventory) is not list
            or position > len(inventory)
        ):
            raise _Task4bReplayMismatch()
        if position == len(inventory):
            _task4b_flush_raw_builder(record)
            _task4b_current_replay_source_record(
                source, ("iterating",)
            )
            raw_exchanges = record.get("raw_exchange_records")
            raw_chunks = record.get("raw_chunks")
            if (
                type(raw_exchanges) is not list
                or len(raw_exchanges) != len(inventory)
                or not all(
                    type(row) is dict
                    and row.get("raw_physical_verified") is True
                    for row in raw_exchanges
                )
                or type(raw_chunks) is not list
                or not raw_chunks
                or record.get("next_spool_offset")
                != owner.get("committed_eof")
            ):
                raise _Task4bReplayMismatch()
            owner["_task4b_raw_chunks"] = tuple(
                dict(row) for row in raw_chunks
            )
            owner["_task4b_raw_exchange_records"] = tuple(
                raw_exchanges
            )
            record["compact_rows"] = None
            record["eof"] = True
            record["state"] = "exhausted"
            raise StopIteration
        compact_rows = record.get("compact_rows")
        if (
            type(compact_rows) is not tuple
            or len(compact_rows) != len(inventory)
        ):
            raise _Task4bReplayMismatch()
        receipt = inventory[position]
        receipt_record = _receipt_for_spool(owner, receipt)
        projection = receipt_record.get("projection")
        compact = compact_rows[position]
        if (
            receipt_record.get("state") != "committed"
            or type(projection) is not dict
            or type(compact) is not dict
            or tuple(projection) != receipt_keys
            or tuple(compact) != receipt_keys
            or projection["schema"]
            != "historical_foundry_exchange_spool_receipt/v1"
            or compact["schema"]
            != "historical_foundry_archive_rpc_spooled_success_exchange/v1"
            or any(
                projection[key] != compact[key]
                for key in receipt_keys[1:]
            )
            or compact["exchange_index"] != position + 1
            or compact["spool_member_index"] != position + 1
            or compact["spool_offset"]
            != record.get("next_spool_offset")
        ):
            raise _Task4bReplayMismatch()
        request_ids = compact["request_ids"]
        response_ids = compact["response_ids"]
        next_request_id = record.get("next_request_id")
        if (
            type(request_ids) is not tuple
            or not request_ids
            or not all(type(value) is int for value in request_ids)
            or request_ids
            != tuple(range(next_request_id, next_request_id + len(request_ids)))
            or type(response_ids) is not tuple
            or len(response_ids) != len(request_ids)
            or set(response_ids) != set(request_ids)
        ):
            raise _Task4bReplayMismatch()
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
        frame = (
            len(request).to_bytes(8, "big")
            + request
            + len(decoded).to_bytes(8, "big")
            + decoded
        )
        if (
            len(frame) != compact["spool_length"]
            or hashlib.sha256(frame).hexdigest()
            != compact["spool_member_sha256"]
        ):
            raise _Task4bReplayMismatch()
        builder = record.get("raw_builder")
        builder_rows = record.get("raw_builder_rows")
        if type(builder) is not bytearray or type(builder_rows) is not list:
            raise _InternalFailure()
        try:
            decision, resulting_size = captured_raw_chunk_append(
                current_chunk_byte_count=len(builder),
                request_byte_count=len(request),
                decoded_byte_count=len(decoded),
            )
        except ValueError:
            raise _Task4bReplayMismatch()
        if decision == "flush_then_append":
            _task4b_flush_raw_builder(record)
            builder = record["raw_builder"]
            builder_rows = record["raw_builder_rows"]
            if resulting_size != len(frame):
                raise _Task4bReplayMismatch()
        elif (
            decision != "append_current"
            or resulting_size != len(builder) + len(frame)
        ):
            raise _Task4bReplayMismatch()
        raw_chunk_path = "rpc/{:08d}.bin".format(
            record["next_raw_chunk_index"]
        )
        raw_offset = len(builder)
        raw_row = {
            "projection": dict(compact),
            "raw_chunk_path": raw_chunk_path,
            "raw_offset": raw_offset,
            "raw_physical_verified": False,
        }
        builder.extend(frame)
        builder_rows.append(raw_row)
        record["raw_exchange_records"].append(raw_row)
        record["position"] = position + 1
        record["next_request_id"] = request_ids[-1] + 1
        record["next_spool_offset"] = (
            compact["spool_offset"] + compact["spool_length"]
        )
        _task4b_current_replay_source_record(
            source, ("iterating",)
        )
        return dict(compact), request, decoded

    class _HistoricalWindowCaptureReplaySource(replay_source_base):
        __slots__ = ("__weakref__",)

        def __enter__(self) -> "_HistoricalWindowCaptureReplaySource":
            record = _task4b_current_replay_source_record(
                self, ("bound_verified",)
            )
            record["state"] = "entered"
            return self

        def _bind_reconciliation_from_bound_scan(
            self,
            *,
            expected_view: Any,
            expected_reconciliation: Any,
        ) -> None:
            entry = replay_source_registry.get(id(self))
            if entry is None or entry[0] is not self:
                _raise_task4b_capability_invalid(expected_view)
            record = entry[1]
            if record.get("state") != "fresh":
                _raise_task4b_capability_invalid(expected_view)
            record["state"] = "bind_attempted"
            try:
                owner = record.get("owner")
                binding = record.get("binding")
                binding_entry = binding_registry.get(id(binding))
                view_entry = consumed_view_registry.get(id(expected_view))
                if (
                    type(owner) is not dict
                    or binding_entry is None
                    or binding_entry[0] is not binding
                    or view_entry is None
                    or view_entry[0] is not expected_view
                    or view_entry[1] is not owner
                ):
                    _raise_task4b_capability_invalid(expected_view)
                binding_record = binding_entry[1]
                source_currentness_drifted = False
                try:
                    _verify_task4b_bound_source_current(
                        binding, binding_record
                    )
                except _BoundSourceIdentityDrift:
                    source_currentness_drifted = True
                if source_currentness_drifted:
                    raise _bound_source_drift(binding_record)
                if (
                    record.get("source") is not self
                    or record.get("view") is not expected_view
                    or record.get("reconciliation")
                    is not expected_reconciliation
                    or owner.get("state") != "capture_materializing"
                    or owner.get("reconciliation")
                    is not expected_reconciliation
                    or owner.get("owner_generation")
                    != record.get("owner_generation")
                    or owner.get("capture_generation") != 0
                    or record.get("capture_generation") != 0
                    or binding_record.get("state") != "live"
                    or binding_record.get("owner_kind")
                    != "capture_materializing"
                    or binding_record.get("owner_handle")
                    is not expected_view
                    or binding_record.get("owner_generation")
                    != owner.get("owner_generation")
                ):
                    _raise_task4b_capability_invalid(expected_view)
                record["state"] = "bound_verified"
                return None
            except BaseException:
                if record.get("state") != "bound_verified":
                    record["state"] = "bind_failed"
                raise

        def __iter__(self) -> "_HistoricalWindowCaptureReplaySource":
            record = _task4b_current_replay_source_record(
                self, ("entered",)
            )
            if record.get("iterated") is not False:
                _reject_task4b_source_protocol(self)
            record["iterated"] = True
            record["state"] = "iterating"
            return self

        def __next__(
            self,
        ) -> Tuple[Mapping[str, Any], bytes, bytes]:
            replay_mismatch = False
            failed_binding_record = None
            try:
                return _task4b_next_replay_source_frame(self)
            except StopIteration:
                raise
            except _Task4bReplayMismatch:
                replay_mismatch = True
                record = replay_source_registry.get(id(self))
                view = (
                    record[1].get("view")
                    if record is not None and record[0] is self else None
                )
                binding_record = _task4b_binding_record_for_view(view)
                if type(binding_record) is dict:
                    failed_binding_record = binding_record
            if replay_mismatch:
                if failed_binding_record is not None:
                    raise _task4b_bound_error(
                        failed_binding_record,
                        "historical_window_reconciliation_mismatch",
                    )
                _raise_storage_error()

        def __exit__(
            self,
            error_type: Any,
            error: Any,
            traceback: Any,
        ) -> None:
            del traceback
            entry = replay_source_registry.get(id(self))
            if entry is None or entry[0] is not self:
                _raise_storage_error()
            record = entry[1]
            if error_type is None and error is None:
                if (
                    record.get("state") != "exhausted"
                    or record.get("eof") is not True
                ):
                    _reject_task4b_source_protocol(self)
                record["state"] = "complete"
                return None
            if record.get("state") not in (
                "closed", "misuse_failed", "complete"
            ):
                record["state"] = "failed"
            return None

        def close(self) -> None:
            if type(self) is not _HistoricalWindowCaptureReplaySource:
                _raise_storage_error()
            entry = replay_source_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    _HistoricalWindowCaptureReplaySource,
                    replay_source_tombstones,
                ):
                    return None
                _raise_storage_error()
            record = entry[1]
            view = record.get("view")
            view_entry = consumed_view_registry.get(id(view))
            if view_entry is None or view_entry[0] is not view:
                _raise_storage_error()
            return _close_moved_owner(
                view,
                view_entry[1],
                consumed_view_registry,
                "closed_nonowning",
            )

    replay_source_authorized[0] = _HistoricalWindowCaptureReplaySource

    def _task4b_snapshot_error(
        owner: Dict[str, Any], failure_kind: str
    ) -> BaseException:
        error_class = owner.get("_task4b_rpc_error_class")
        if type(failure_kind) is str and type(error_class) is type:
            try:
                return error_class("authority_mismatch", failure_kind)
            except BaseException as construction_error:
                if not isinstance(construction_error, Exception):
                    return construction_error
        return HistoricalFoundryStorageError()

    def _task4b_terminalize_snapshot_failure(
        snapshot: object,
        owner: Dict[str, Any],
        failure_kind: str,
        original_control: Optional[BaseException] = None,
    ) -> None:
        public_error = _task4b_snapshot_error(owner, failure_kind)
        construction_control = (
            public_error if not isinstance(public_error, Exception) else None
        )
        cleanup_control = None
        try:
            _close_moved_owner(
                snapshot,
                owner,
                staging_snapshot_registry,
                "closed_nonowning",
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                cleanup_control = error
        if original_control is not None:
            raise original_control
        if construction_control is not None:
            raise construction_control
        if cleanup_control is not None:
            raise cleanup_control
        raise public_error from None

    def _task4b_current_snapshot_owner(
        snapshot: object,
    ) -> Dict[str, Any]:
        entry = staging_snapshot_registry.get(id(snapshot))
        if (
            type(snapshot) is not HistoricalRunStagingSnapshot
            or entry is None
            or entry[0] is not snapshot
        ):
            _raise_storage_error()
        owner = entry[1]
        drifted = False
        control = None
        try:
            generation = owner.get("capture_generation")
            expected_state = {
                1: "capture_frozen",
                2: "prefilter_frozen",
            }.get(generation)
            if (
                owner.get("constructor") is not constructor_provenance
                or expected_state is None
                or owner.get("state") != expected_state
                or owner.get("_task4b_snapshot_handle") is not snapshot
                or owner.get("_task4b_snapshot_owner_generation")
                != owner.get("owner_generation")
            ):
                raise _BoundSourceIdentityDrift()
            _task4b_verify_snapshot_source_authority(
                owner.get("_task4b_snapshot_source_authority")
            )
            _verify_ancestry(owner["chain"])
            ledger = owner.get("_task4b_staging")
            captures = (
                ledger.get("capture_directories")
                if type(ledger) is dict else None
            )
            if type(captures) is not dict:
                raise _BoundSourceIdentityDrift()
            for name in ("raw", "replay", "staging"):
                _task4b_verify_capture_directory(captures[name])
        except BaseException as error:
            if isinstance(error, Exception):
                drifted = True
            else:
                control = error
        if drifted or control is not None:
            _task4b_terminalize_snapshot_failure(
                snapshot, owner, "final_identity_drift", control
            )
        return owner

    def _task4b_acknowledge_snapshot_delivery(
        snapshot: object, owner: Dict[str, Any]
    ) -> None:
        view_reference = owner.pop("_task4b_delivery_view_ref", None)
        if view_reference is None:
            return None
        view = view_reference()
        entry = (
            consumed_view_registry.get(id(view))
            if view is not None else None
        )
        if (
            view is None
            or entry is None
            or entry[0] is not view
            or entry[1] is not owner
            or owner.get("_task4b_delivery_guard_phase") != "armed"
        ):
            raise _BoundSourceIdentityDrift()
        owner["_task4b_delivery_guard_phase"] = "acknowledged"
        _retire_nonowner_handle(
            view, consumed_view_registry, consumed_view_tombstones
        )
        return None

    def _task5_detach_json(value: Any) -> Any:
        if isinstance(value, MappingABC):
            return {
                key: _task5_detach_json(nested)
                for key, nested in value.items()
            }
        if type(value) in (list, tuple):
            return [_task5_detach_json(nested) for nested in value]
        if value is None or type(value) in (bool, int, str):
            return value
        raise _InternalFailure()

    def _task5_grid_digest(rows: Any) -> str:
        if type(rows) not in (list, tuple) or not rows:
            raise _InternalFailure()
        digest = hashlib.sha256()
        digest.update(task5_prefilter_domain)
        digest.update(b"\0")
        for row in rows:
            payload = _task4b_canonical_json_bytes(row)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    def _task5_rebuild_prefilter(
        owner: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        ledger = owner.get("_task4b_staging")
        chunks = owner.get("_task5_prefilter_chunks")
        inventory_size = owner.get("_task5_inventory_byte_count")
        if (
            type(ledger) is not dict
            or type(chunks) is not tuple
            or not chunks
            or type(inventory_size) is not int
            or inventory_size <= 0
        ):
            raise _InternalFailure()
        rebuilt_chunks = []
        rows = []
        for expected_index, chunk in enumerate(chunks, 1):
            if (
                type(chunk) is not dict
                or chunk.get("chunk_index") != expected_index
                or chunk.get("path")
                != "scan/prefilter/{:08d}.json.gz".format(expected_index)
            ):
                raise _InternalFailure()
            physical = _task4b_reread_capture_member(
                ledger,
                relative_path=chunk["path"],
                expected_size=chunk["gzip_byte_count"],
                maximum_size=16_842_752,
                size_kind="gzip",
            )
            if hashlib.sha256(physical).hexdigest() != chunk["gzip_sha256"]:
                raise _InternalFailure()
            decoded = _task4b_decode_gzip(physical)
            decoded_rows = _task4b_decode_canonical_json(
                decoded, expected_container=list
            )
            if (
                not decoded_rows
                or len(decoded_rows) != chunk["row_count"]
                or len(decoded) != chunk["decoded_byte_count"]
                or hashlib.sha256(decoded).hexdigest()
                != chunk["decoded_sha256"]
            ):
                raise _InternalFailure()
            rebuilt = {
                "path": chunk["path"],
                "chunk_index": expected_index,
                "row_count": len(decoded_rows),
                "decoded_byte_count": len(decoded),
                "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                "gzip_byte_count": len(physical),
                "gzip_sha256": hashlib.sha256(physical).hexdigest(),
            }
            rebuilt_chunks.append(rebuilt)
            rows.extend(decoded_rows)
        inventory_bytes = _task4b_reread_capture_member(
            ledger,
            relative_path="scan/prefilter_inventory.json",
            expected_size=inventory_size,
            maximum_size=16_777_216,
            size_kind="inventory",
        )
        inventory = _task4b_decode_canonical_json(
            inventory_bytes, expected_container=dict
        )
        if (
            inventory.get("schema")
            != "historical_foundry_scan_inventory/v1"
            or inventory.get("capture_inventory_sha256")
            != owner["_task4b_snapshot_projection"][
                "capture_inventory_sha256"
            ]
            or inventory.get("prefilter_chunks") != rebuilt_chunks
            or inventory.get("row_count") != len(rows)
            or inventory.get("grid_digest") != _task5_grid_digest(rows)
            or inventory.get("safe_excluded_count")
            != sum(row.get("decision") == "safe_excluded" for row in rows)
            or inventory.get("replay_required_count")
            != sum(row.get("decision") == "replay_required" for row in rows)
        ):
            raise _InternalFailure()
        return inventory, rows

    def _task5_freeze_audit(owner: Dict[str, Any]) -> None:
        ledger = owner.get("_task4b_staging")
        roles = ledger.get("role_directories") if type(ledger) is dict else None
        prefilter = roles.get("prefilter") if type(roles) is dict else None
        chunks = owner.get("_task5_prefilter_chunks")
        if type(prefilter) is not dict or type(chunks) is not tuple:
            raise _InternalFailure()
        _task4b_verify_capture_directory(prefilter)
        os.fsync(prefilter["fd"])
        _task4b_verify_capture_directory(roles["scan"])
        os.fsync(roles["scan"]["fd"])
        expected = {
            row["path"].rsplit("/", 1)[1] for row in chunks
        }
        if set(os.listdir(prefilter["fd"])) != expected:
            raise _InternalFailure()
        inventory, rows = _task5_rebuild_prefilter(owner)
        inventory_bytes = _task4b_reread_capture_member(
            ledger,
            relative_path="scan/prefilter_inventory.json",
            expected_size=owner["_task5_inventory_byte_count"],
            maximum_size=16_777_216,
            size_kind="inventory",
        )
        if (
            _task4b_canonical_json_bytes(inventory) != inventory_bytes
            or hashlib.sha256(inventory_bytes).hexdigest()
            != owner["_task4b_snapshot_projection"][
                "scan_inventory_sha256"
            ]
            or len(rows)
            != owner["_task4b_snapshot_projection"]["prefilter_row_count"]
            or inventory["grid_digest"]
            != owner["_task4b_snapshot_projection"]["prefilter_grid_digest"]
        ):
            raise _InternalFailure()
        return None

    def _freeze_historical_prefilter_grid(
        *,
        staging: "HistoricalRunStagingSnapshot",
        rows: Tuple[Mapping[str, Any], ...],
    ) -> "HistoricalRunStagingSnapshot":
        owner = _task4b_current_snapshot_owner(staging)
        if (
            owner.get("capture_generation") != 1
            or owner.get("state") != "capture_frozen"
            or type(rows) is not tuple
            or not rows
        ):
            _raise_storage_error()
        owner["state"] = "prefilter_materializing"
        owner["_task4b_staging"]["quota_owner_handle"] = staging
        detached = tuple(_task5_detach_json(row) for row in rows)
        row_payloads = tuple(
            _task4b_canonical_json_bytes(row) for row in detached
        )
        if any(len(payload) + 2 > 16_777_216 for payload in row_payloads):
            _raise_storage_error()
        ledger = owner["_task4b_staging"]
        scan_directory = ledger["role_directories"]["scan"]
        prefilter_directory = _task4b_open_capture_directory(
            ledger,
            scan_directory["fd"],
            "prefilter",
            allow_existing=False,
        )
        ledger["role_directories"]["prefilter"] = prefilter_directory
        chunks = []
        pending = []
        pending_size = 2

        def flush_pending() -> None:
            nonlocal pending, pending_size
            if not pending:
                return None
            decoded = b"[" + b",".join(pending) + b"]"
            if len(decoded) != pending_size:
                raise _InternalFailure()
            physical = _task4b_encode_gzip(decoded)
            chunk_index = len(chunks) + 1
            target = "{:08d}.json.gz".format(chunk_index)
            _task4b_write_capture_member(
                ledger, prefilter_directory, target, physical
            )
            chunks.append({
                "path": "scan/prefilter/" + target,
                "chunk_index": chunk_index,
                "row_count": len(pending),
                "decoded_byte_count": len(decoded),
                "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                "gzip_byte_count": len(physical),
                "gzip_sha256": hashlib.sha256(physical).hexdigest(),
            })
            pending = []
            pending_size = 2
            return None

        for payload in row_payloads:
            candidate_size = pending_size + len(payload) + (1 if pending else 0)
            if candidate_size > 16_777_216:
                flush_pending()
            pending.append(payload)
            pending_size += len(payload) + (1 if len(pending) > 1 else 0)
        flush_pending()
        decision_counts = {
            decision: sum(
                row.get("decision") == decision for row in detached
            )
            for decision in ("safe_excluded", "replay_required")
        }
        first_window = detached[0].get("window")
        inventory = {
            "schema": "historical_foundry_scan_inventory/v1",
            "capture_inventory_sha256": owner[
                "_task4b_snapshot_projection"
            ]["capture_inventory_sha256"],
            "range": first_window,
            "scenario_denominator": len(detached),
            "row_count": len(detached),
            "safe_excluded_count": decision_counts["safe_excluded"],
            "replay_required_count": decision_counts["replay_required"],
            "grid_digest": _task5_grid_digest(detached),
            "prefilter_chunks": chunks,
        }
        inventory_bytes = _task4b_canonical_json_bytes(inventory)
        try:
            captured_capture_inventory_size(byte_count=len(inventory_bytes))
        except ValueError:
            _raise_storage_error()
        _task4b_write_capture_member(
            ledger,
            scan_directory,
            "prefilter_inventory.json",
            inventory_bytes,
        )
        owner["_task5_prefilter_chunks"] = tuple(chunks)
        owner["_task5_inventory_byte_count"] = len(inventory_bytes)
        members = dict(owner["_task4b_snapshot_members"])
        for row in chunks:
            members[row["path"]] = {
                "size": row["gzip_byte_count"],
                "sha256": row["gzip_sha256"],
                "cap": 16_842_752,
                "kind": "gzip",
            }
        scan_inventory_sha256 = hashlib.sha256(inventory_bytes).hexdigest()
        members["scan/prefilter_inventory.json"] = {
            "size": len(inventory_bytes),
            "sha256": scan_inventory_sha256,
            "cap": 16_777_216,
            "kind": "inventory",
        }
        owner["_task4b_snapshot_members"] = members
        owner["capture_generation"] = 2
        owner["state"] = "prefilter_frozen"
        quota = _quota_record_for_owner(owner)
        owner["_task4b_snapshot_projection"] = {
            "schema": "historical_foundry_staging_snapshot_identity/v1",
            "stage": "prefilter_frozen",
            "generation": 2,
            "capture_inventory_sha256": inventory[
                "capture_inventory_sha256"
            ],
            "scan_inventory_sha256": scan_inventory_sha256,
            "frozen_member_count": len(members),
            "frozen_physical_byte_count": sum(
                row["size"] for row in members.values()
            ),
            "quota_committed_physical_bytes": quota[
                "committed_physical_bytes"
            ],
            "quota_committed_member_count": quota["committed_members"],
            "prefilter_chunk_count": len(chunks),
            "prefilter_row_count": len(detached),
            "prefilter_grid_digest": inventory["grid_digest"],
        }
        _task4b_freeze_audit(owner["_task4b_frozen_record"])
        _task5_freeze_audit(owner)
        old_snapshot = staging
        owner_generation = owner["owner_generation"] + 1
        owner["owner_generation"] = owner_generation
        next_snapshot = _prepare_handle(HistoricalRunStagingSnapshot, owner)
        owner["_task4b_snapshot_handle"] = next_snapshot
        owner["_task4b_snapshot_owner_generation"] = owner_generation
        staging_snapshot_registry[id(next_snapshot)] = (next_snapshot, owner)
        _retire_nonowner_handle(
            old_snapshot,
            staging_snapshot_registry,
            staging_snapshot_tombstones,
        )
        return next_snapshot

    class HistoricalRunStagingSnapshot(staging_snapshot_base):
        __slots__ = ("__weakref__",)

        def read_frozen_member(
            self,
            relative_path: str,
            *,
            expected_sha256: str,
            max_bytes: int,
        ) -> bytes:
            owner = _task4b_current_snapshot_owner(self)
            members = owner.get("_task4b_snapshot_members")
            row = (
                members.get(relative_path)
                if type(relative_path) is str and type(members) is dict
                else None
            )
            if (
                type(relative_path) is not str
                or not relative_path
                or type(row) is not dict
                or type(expected_sha256) is not str
                or expected_sha256 != row.get("sha256")
                or type(max_bytes) is not int
                or max_bytes < row.get("size", -1)
                or max_bytes > row.get("cap", -1)
            ):
                invalid_error = _task4b_snapshot_error(
                    owner, "historical_window_capability_invalid"
                )
                if not isinstance(invalid_error, Exception):
                    _task4b_terminalize_snapshot_failure(
                        self,
                        owner,
                        "historical_window_capability_invalid",
                        invalid_error,
                    )
                raise invalid_error from None
            try:
                payload = _task4b_reread_capture_member(
                    owner["_task4b_staging"],
                    relative_path=relative_path,
                    expected_size=row["size"],
                    maximum_size=row["cap"],
                    size_kind=row["kind"],
                )
                if (
                    hashlib.sha256(payload).hexdigest()
                    != expected_sha256
                ):
                    raise _InternalFailure()
                if row["kind"] == "gzip":
                    _task4b_decode_gzip(payload)
                _task4b_current_snapshot_owner(self)
                return bytes(payload)
            except BaseException as error:
                original_control = (
                    error if not isinstance(error, Exception) else None
                )
                del error
            _task4b_terminalize_snapshot_failure(
                self,
                owner,
                "historical_window_spool_handoff_failed",
                original_control,
            )

        def frozen_identity_projection(self) -> Mapping[str, Any]:
            owner = _task4b_current_snapshot_owner(self)
            projection = owner.get("_task4b_snapshot_projection")
            expected_size = 8 if owner.get("capture_generation") == 1 else 12
            if type(projection) is not dict or len(projection) != expected_size:
                _task4b_terminalize_snapshot_failure(
                    self, owner, "final_identity_drift"
                )
            try:
                _task4b_acknowledge_snapshot_delivery(self, owner)
                return dict(projection)
            except BaseException as error:
                original_control = (
                    error if not isinstance(error, Exception) else None
                )
                del error
            _task4b_terminalize_snapshot_failure(
                self, owner, "final_identity_drift", original_control
            )

        def reread_frozen_members_unchanged(self) -> None:
            owner = _task4b_current_snapshot_owner(self)
            try:
                _task4b_freeze_audit(owner["_task4b_frozen_record"])
                if owner.get("capture_generation") == 2:
                    _task5_freeze_audit(owner)
                _task4b_current_snapshot_owner(self)
                return None
            except BaseException as error:
                original_control = (
                    error if not isinstance(error, Exception) else None
                )
                del error
            _task4b_terminalize_snapshot_failure(
                self,
                owner,
                "historical_window_spool_handoff_failed",
                original_control,
            )

        def close(self) -> None:
            if type(self) is not HistoricalRunStagingSnapshot:
                _raise_storage_error()
            entry = staging_snapshot_registry.get(id(self))
            if entry is None or entry[0] is not self:
                if _is_exact_tombstone(
                    self,
                    HistoricalRunStagingSnapshot,
                    staging_snapshot_tombstones,
                ):
                    return None
                _raise_storage_error()
            return _close_moved_owner(
                self,
                entry[1],
                staging_snapshot_registry,
                "closed_nonowning",
            )

        def __enter__(self) -> "HistoricalRunStagingSnapshot":
            _task4b_current_snapshot_owner(self)
            return self

        def __exit__(
            self,
            error_type: Any,
            error: Any,
            traceback: Any,
        ) -> None:
            del error_type, traceback
            try:
                return self.close()
            except BaseException as cleanup_error:
                if error is not None and not isinstance(error, Exception):
                    raise error
                raise cleanup_error

    staging_snapshot_authorized[0] = HistoricalRunStagingSnapshot

    class HistoricalRunSnapshot(run_snapshot_base):
        __slots__ = ("__weakref__",)

        def read_member(
            self,
            relative_path: str,
            *,
            expected_sha256: str,
            max_bytes: int,
        ) -> bytes:
            del relative_path, expected_sha256, max_bytes
            _raise_storage_error()

        def identity_projection(self) -> Mapping[str, Any]:
            _raise_storage_error()

        def reread_unchanged(self) -> None:
            _raise_storage_error()

        def close(self) -> None:
            _raise_storage_error()

    run_snapshot_authorized[0] = HistoricalRunSnapshot

    def open_validated_run(
        *,
        data_dir: Path,
        run_id: str,
        expected_manifest_sha256: str,
    ) -> "HistoricalRunSnapshot":
        _raise_storage_error()

    class _HistoricalWindowSpoolSourceBinding(binding_base):
        __slots__ = ("__weakref__",)

    binding_authorized[0] = _HistoricalWindowSpoolSourceBinding

    def _task4b_owner_registry_for_handle(
        owner_handle: object,
    ) -> Optional[Dict[int, Tuple[object, Dict[str, Any]]]]:
        matches = []
        for registry in (
            active_registry,
            consumed_view_registry,
            staging_snapshot_registry,
        ):
            current = registry.get(id(owner_handle))
            if current is not None and current[0] is owner_handle:
                matches.append(registry)
        if len(matches) != 1:
            return None
        return matches[0]

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
        owner_registry = _task4b_owner_registry_for_handle(owner_handle)
        current = (
            owner_registry.get(id(owner_handle))
            if owner_registry is not None else None
        )
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        owner_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        owner_registry[id(owner_handle)] = (owner_handle, final_owner)
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
        owner_registry = _task4b_owner_registry_for_handle(owner_handle)
        current = (
            owner_registry.get(id(owner_handle))
            if owner_registry is not None else None
        )
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        owner_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        owner_registry[id(owner_handle)] = (owner_handle, final_owner)

    def _install_quota_abort_transition(
        owner_handle: object,
        prior_owner: Dict[str, Any],
        transition_owner: Dict[str, Any],
        quota_handle: object,
        next_quota: Dict[str, Any],
        final_owner: Dict[str, Any],
    ) -> None:
        owner_registry = _task4b_owner_registry_for_handle(owner_handle)
        current = (
            owner_registry.get(id(owner_handle))
            if owner_registry is not None else None
        )
        if (
            current is None
            or current[0] is not owner_handle
            or current[1] is not prior_owner
        ):
            raise _InternalFailure()
        owner_registry[id(owner_handle)] = (owner_handle, transition_owner)
        quota_registry[id(quota_handle)] = (quota_handle, next_quota)
        owner_registry[id(owner_handle)] = (owner_handle, final_owner)

    def _task4b_output_quota_context(
        ledger: Dict[str, Any], expected_state: str
    ) -> Tuple[object, Dict[str, Any], object, Dict[str, Any]]:
        owner_handle = ledger.get("quota_owner_handle")
        owner_registry = _task4b_owner_registry_for_handle(owner_handle)
        current = (
            owner_registry.get(id(owner_handle))
            if owner_registry is not None else None
        )
        accepted_states = (
            ("capture_materializing", "prefilter_materializing")
            if expected_state == "capture_materializing"
            else (expected_state,)
        )
        if (
            current is None
            or current[0] is not owner_handle
            or current[1].get("state") not in accepted_states
            or current[1].get("lineage") is not ledger.get("lineage")
        ):
            raise _InternalFailure()
        owner = current[1]
        quota = owner.get("quota")
        quota_entry = quota_registry.get(id(quota))
        if (
            type(quota) is not _HistoricalWindowRunQuota
            or quota_entry is None
            or quota_entry[0] is not quota
            or quota_entry[1].get("lineage") is not owner.get("lineage")
        ):
            raise _InternalFailure()
        return owner_handle, owner, quota, quota_entry[1]

    def _task4b_reserve_output_quota(
        ledger: Dict[str, Any], entry: Dict[str, Any], physical_bytes: int
    ) -> object:
        if (
            type(entry) is not dict
            or type(physical_bytes) is not int
            or physical_bytes <= 0
        ):
            raise _InternalFailure()
        owner_handle, owner, quota, quota_record = (
            _task4b_output_quota_context(ledger, "capture_materializing")
        )
        if quota_record.get("reservation") is not None:
            raise _InternalFailure()
        if (
            physical_bytes
            > 8_589_934_592
            - quota_record["committed_physical_bytes"]
            - quota_record["provisional_physical_bytes"]
            or 1
            > 200_000
            - quota_record["committed_members"]
            - quota_record["provisional_members"]
        ):
            raise _InternalFailure()
        token = object()
        reservation = {
            "kind": "task4b_output",
            "token": token,
            "physical_bytes": physical_bytes,
            "members": 1,
        }
        next_quota = dict(quota_record)
        next_quota["reservation"] = reservation
        next_quota["provisional_physical_bytes"] = physical_bytes
        next_quota["provisional_members"] = 1
        entry["quota_token"] = token
        entry["quota_physical_bytes"] = physical_bytes
        entry["quota_committed_before"] = (
            quota_record["committed_physical_bytes"],
            quota_record["committed_members"],
        )
        entry["quota_state"] = "reserving"
        try:
            _install_quota_reserve_transition(
                owner_handle,
                owner,
                owner,
                quota,
                next_quota,
                owner,
                None,
                None,
            )
        except BaseException:
            observed = quota_registry.get(id(quota))
            if (
                observed is None
                or observed[0] is not quota
                or observed[1].get("reservation") is not reservation
            ):
                raise
            raise
        entry["quota_state"] = "reserved"
        return token

    def _task4b_commit_output_quota(
        ledger: Dict[str, Any], entry: Dict[str, Any], token: object
    ) -> None:
        owner_handle, owner, quota, quota_record = (
            _task4b_output_quota_context(ledger, "capture_materializing")
        )
        reservation = quota_record.get("reservation")
        if (
            type(reservation) is not dict
            or reservation.get("kind") != "task4b_output"
            or reservation.get("token") is not token
        ):
            raise _InternalFailure()
        next_quota = dict(quota_record)
        next_quota["committed_physical_bytes"] += reservation[
            "physical_bytes"
        ]
        next_quota["committed_members"] += 1
        next_quota["provisional_physical_bytes"] = 0
        next_quota["provisional_members"] = 0
        next_quota["reservation"] = None
        entry["quota_state"] = "committing"
        _install_quota_commit_transition(
            owner_handle,
            owner,
            owner,
            quota,
            next_quota,
            owner,
        )
        entry["quota_state"] = "committed"

    def _task4b_abort_output_quota(
        ledger: Dict[str, Any], entry: Dict[str, Any], token: object
    ) -> None:
        owner_handle, owner, quota, quota_record = (
            _task4b_output_quota_context(ledger, "closing")
        )
        reservation = quota_record.get("reservation")
        if (
            type(reservation) is not dict
            or reservation.get("kind") != "task4b_output"
            or reservation.get("token") is not token
        ):
            raise _InternalFailure()
        next_quota = dict(quota_record)
        next_quota["provisional_physical_bytes"] = 0
        next_quota["provisional_members"] = 0
        next_quota["reservation"] = None
        entry["quota_state"] = "aborting"
        _install_quota_abort_transition(
            owner_handle,
            owner,
            owner,
            quota,
            next_quota,
            owner,
        )
        entry["quota_state"] = "aborted"

    def _task4b_reconcile_output_quota_entry(
        ledger: Dict[str, Any], entry: Dict[str, Any]
    ) -> str:
        _owner_handle, _owner, _quota, quota_record = (
            _task4b_output_quota_context(ledger, "closing")
        )
        token = entry.get("quota_token")
        before = entry.get("quota_committed_before")
        physical_bytes = entry.get("quota_physical_bytes")
        reservation = quota_record.get("reservation")
        if (
            type(before) is not tuple
            or len(before) != 2
            or any(type(value) is not int for value in before)
            or type(physical_bytes) is not int
            or physical_bytes <= 0
        ):
            raise _InternalFailure()
        if (
            type(reservation) is dict
            and reservation.get("kind") == "task4b_output"
            and reservation.get("token") is token
            and reservation.get("physical_bytes") == physical_bytes
            and reservation.get("members") == 1
        ):
            return "reserved"
        if reservation is not None:
            raise _InternalFailure()
        committed = (
            quota_record.get("committed_physical_bytes"),
            quota_record.get("committed_members"),
        )
        if committed == before:
            return "aborted"
        if committed == (before[0] + physical_bytes, before[1] + 1):
            return "committed"
        raise _InternalFailure()

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
    task4b_bound_object_names = (
        ("scan", "_ProductionHistoricalWindowCaptureReplayEvent"),
        ("scan", "_bind_production_historical_window_capture_replay_source_from_bound_storage"),
        ("scan", "_replay_production_historical_window_capture_from_bound_storage"),
        ("scan", "_consume_production_historical_window_capture_replay_event_for_storage"),
        ("storage", "_HistoricalWindowCaptureReplaySource"),
        ("storage", "_HistoricalWindowCaptureReplaySource.__enter__"),
        ("storage", "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan"),
        ("storage", "_HistoricalWindowCaptureReplaySource.__iter__"),
        ("storage", "_HistoricalWindowCaptureReplaySource.__next__"),
        ("storage", "_HistoricalWindowCaptureReplaySource.__exit__"),
        ("storage", "_HistoricalWindowCaptureReplaySource.close"),
        ("storage", "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan"),
    )
    task4b_scan_local_names = (
        "_materialize_historical_window_staging_snapshot",
        "_ProductionHistoricalWindowCaptureReplayEvent",
        "_bind_production_historical_window_capture_replay_source_from_bound_storage",
        "_replay_production_historical_window_capture_from_bound_storage",
        "_consume_production_historical_window_capture_replay_event_for_storage",
    )
    task4b_storage_local_names = (
        "_HistoricalWindowCaptureReplaySource",
        "_HistoricalWindowCaptureReplaySource.__enter__",
        "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan",
        "_HistoricalWindowCaptureReplaySource.__iter__",
        "_HistoricalWindowCaptureReplaySource.__next__",
        "_HistoricalWindowCaptureReplaySource.__exit__",
        "_HistoricalWindowCaptureReplaySource.close",
        "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan",
        "HistoricalRunStagingSnapshot",
        "HistoricalRunStagingSnapshot.read_frozen_member",
        "HistoricalRunStagingSnapshot.frozen_identity_projection",
        "HistoricalRunStagingSnapshot.reread_frozen_members_unchanged",
        "HistoricalRunStagingSnapshot.close",
        "HistoricalRunStagingSnapshot.__enter__",
        "HistoricalRunStagingSnapshot.__exit__",
        "open_validated_run",
        "HistoricalRunSnapshot",
        "HistoricalRunSnapshot.read_member",
        "HistoricalRunSnapshot.identity_projection",
        "HistoricalRunSnapshot.reread_unchanged",
        "HistoricalRunSnapshot.close",
    )
    task4b_storage_local_objects = (
        _HistoricalWindowCaptureReplaySource,
        _HistoricalWindowCaptureReplaySource.__enter__,
        _HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan,
        _HistoricalWindowCaptureReplaySource.__iter__,
        _HistoricalWindowCaptureReplaySource.__next__,
        _HistoricalWindowCaptureReplaySource.__exit__,
        _HistoricalWindowCaptureReplaySource.close,
        _ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan,
        HistoricalRunStagingSnapshot,
        HistoricalRunStagingSnapshot.read_frozen_member,
        HistoricalRunStagingSnapshot.frozen_identity_projection,
        HistoricalRunStagingSnapshot.reread_frozen_members_unchanged,
        HistoricalRunStagingSnapshot.close,
        HistoricalRunStagingSnapshot.__enter__,
        HistoricalRunStagingSnapshot.__exit__,
        open_validated_run,
        HistoricalRunSnapshot,
        HistoricalRunSnapshot.read_member,
        HistoricalRunSnapshot.identity_projection,
        HistoricalRunSnapshot.reread_unchanged,
        HistoricalRunSnapshot.close,
    )
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
            except BaseException as construction_error:
                if not isinstance(construction_error, Exception):
                    return construction_error
        return HistoricalFoundryStorageError()

    def _task4b_bound_error(
        binding_record: Dict[str, Any], failure_kind: str
    ) -> BaseException:
        error_class = binding_record.get("rpc_error_class")
        rows = binding_record.get("bound_module_rows")
        if (
            type(failure_kind) is str
            and type(rows) is tuple
            and len(rows) == 3
            and type(rows[0]) is tuple
            and len(rows[0]) == 9
            and type(rows[0][8]) is tuple
            and rows[0][8]
            and rows[0][8][0] is error_class
        ):
            try:
                return error_class("authority_mismatch", failure_kind)
            except BaseException as construction_error:
                if not isinstance(construction_error, Exception):
                    return construction_error
        return HistoricalFoundryStorageError()

    def _task4b_binding_record_for_view(view: Any) -> Optional[Dict[str, Any]]:
        entry = consumed_view_registry.get(id(view))
        if entry is None or entry[0] is not view:
            return None
        owner = entry[1]
        binding = owner.get("binding")
        binding_entry = binding_registry.get(id(binding))
        if (
            binding_entry is None
            or binding_entry[0] is not binding
            or binding_entry[1].get("lineage") is not owner.get("lineage")
        ):
            return None
        return binding_entry[1]

    def _raise_task4b_capability_invalid(view: Any) -> None:
        binding_record = _task4b_binding_record_for_view(view)
        if binding_record is None:
            _raise_storage_error()
        raise _task4b_bound_error(
            binding_record, "historical_window_capability_invalid"
        )

    class _BoundSourceIdentityDrift(Exception):
        pass

    def _task4b_current_rows(
        binding_record: Dict[str, Any]
    ) -> Tuple[Tuple[Any, ...], Tuple[Any, ...], Tuple[Any, ...]]:
        scan_module = binding_record["scan_module"]
        storage_module = binding_record["storage_module"]
        current_scan = tuple(
            _resolve_bound_object(scan_module, name)
            for name in task4b_scan_local_names
        )
        current_storage = tuple(
            _resolve_bound_object(storage_module, name)
            for name in task4b_storage_local_names
        )
        current_cross = tuple(
            _resolve_bound_object(
                scan_module if role == "scan" else storage_module, name,
            )
            for role, name in task4b_bound_object_names
        )
        return current_scan, current_storage, current_cross

    def _verify_task4b_provisional_bound_source_current(
        binding_record: Dict[str, Any]
    ) -> None:
        row = binding_record.get("task4b_bound_objects")
        if type(row) is not tuple or len(row) != 3:
            raise _BoundSourceIdentityDrift()
        scan_objects, storage_objects, cross_objects = row
        try:
            scan_module = binding_record["scan_module"]
            storage_module = binding_record["storage_module"]
            current_scan, current_storage, current_cross = (
                _task4b_current_rows(binding_record)
            )
            exported_scan = getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS"
            )
            exported_storage = getattr(
                storage_module, "_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS"
            )
        except (AttributeError, TypeError):
            raise _BoundSourceIdentityDrift()
        if (
            type(scan_objects) is not tuple
            or type(storage_objects) is not tuple
            or type(cross_objects) is not tuple
            or len(scan_objects) != len(task4b_scan_local_names)
            or len(storage_objects) != len(task4b_storage_local_objects)
            or len(cross_objects) != len(task4b_bound_object_names)
            or type(exported_scan) is not tuple
            or len(exported_scan) != len(task4b_scan_local_names)
            or type(exported_storage) is not tuple
            or len(exported_storage) != len(task4b_storage_local_objects)
            or not all(
                current is exported is recorded
                for current, exported, recorded in zip(
                    current_scan, exported_scan, scan_objects
                )
            )
            or not all(
                current is original is exported is recorded
                for current, original, exported, recorded in zip(
                    current_storage,
                    task4b_storage_local_objects,
                    exported_storage,
                    storage_objects,
                )
            )
            or not all(
                current is expected is recorded
                for current, expected, recorded in zip(
                    current_cross,
                    current_scan[1:] + task4b_storage_local_objects[:8],
                    cross_objects,
                )
            )
            or getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_NAMES", None
            ) != task4b_scan_local_names
            or getattr(
                storage_module, "_TASK4B_STORAGE_LOCAL_SURFACE_NAMES", None
            ) != task4b_storage_local_names
            or not all(
                exported is original
                for exported, original in zip(
                    exported_storage,
                    task4b_storage_local_objects,
                )
            )
        ):
            raise _BoundSourceIdentityDrift()

    def _make_task4b_binding_currentness_checker(
        binding_record: Dict[str, Any], direct_attestation: Any
    ) -> Callable[[], None]:
        row = binding_record.get("task4b_bound_objects")
        if (
            type(direct_attestation) is not tuple
            or len(direct_attestation) != 2
            or type(direct_attestation[0]) is not object
            or type(direct_attestation[1]) is not tuple
            or len(direct_attestation[1]) != len(task4b_scan_local_names)
            or type(row) is not tuple
            or len(row) != 3
        ):
            raise _BoundSourceIdentityDrift()
        attested_scan_originals = direct_attestation[1]
        recorded_scan, recorded_storage, recorded_cross = row
        scan_module = binding_record["scan_module"]
        storage_module = binding_record["storage_module"]
        scan_generation = getattr(
            scan_module, "_HISTORICAL_WINDOW_MODULE_GENERATION", None
        )
        storage_generation = getattr(
            storage_module, "_HISTORICAL_WINDOW_MODULE_GENERATION", None
        )

        def checker() -> None:
            attestation_anchor = direct_attestation
            if (
                type(attestation_anchor) is not tuple
                or len(attestation_anchor) != 2
                or attestation_anchor[1] is not attested_scan_originals
                or sys.modules.get("scripts.historical_foundry_scan")
                is not scan_module
                or sys.modules.get(__name__) is not storage_module
                or getattr(
                    scan_module, "_HISTORICAL_WINDOW_MODULE_GENERATION", None
                ) is not scan_generation
                or getattr(
                    storage_module,
                    "_HISTORICAL_WINDOW_MODULE_GENERATION",
                    None,
                ) is not storage_generation
            ):
                raise _BoundSourceIdentityDrift()
            try:
                current_scan, current_storage, current_cross = (
                    _task4b_current_rows(binding_record)
                )
                exported_scan = getattr(
                    scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS"
                )
                exported_storage = getattr(
                    storage_module, "_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS"
                )
            except (AttributeError, TypeError):
                raise _BoundSourceIdentityDrift()
            expected_cross = (
                attested_scan_originals[1:]
                + task4b_storage_local_objects[:8]
            )
            if (
                type(recorded_scan) is not tuple
                or len(recorded_scan) != len(task4b_scan_local_names)
                or type(recorded_storage) is not tuple
                or len(recorded_storage) != len(task4b_storage_local_objects)
                or type(recorded_cross) is not tuple
                or len(recorded_cross) != len(task4b_bound_object_names)
                or type(exported_scan) is not tuple
                or len(exported_scan) != len(task4b_scan_local_names)
                or type(exported_storage) is not tuple
                or len(exported_storage) != len(task4b_storage_local_objects)
                or not all(
                    current is original is exported is recorded
                    for current, original, exported, recorded in zip(
                        current_scan,
                        attested_scan_originals,
                        exported_scan,
                        recorded_scan,
                    )
                )
                or not all(
                    current is original is exported is recorded
                    for current, original, exported, recorded in zip(
                        current_storage,
                        task4b_storage_local_objects,
                        exported_storage,
                        recorded_storage,
                    )
                )
                or not all(
                    current is expected is recorded
                    for current, expected, recorded in zip(
                        current_cross, expected_cross, recorded_cross
                    )
                )
                or getattr(
                    scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_NAMES", None
                ) != task4b_scan_local_names
                or getattr(
                    storage_module,
                    "_TASK4B_STORAGE_LOCAL_SURFACE_NAMES",
                    None,
                ) != task4b_storage_local_names
            ):
                raise _BoundSourceIdentityDrift()

        checker_drifted = False
        try:
            checker()
        except _BoundSourceIdentityDrift:
            checker_drifted = True
        if checker_drifted:
            raise _bound_source_drift(binding_record)
        return checker

    def _verify_task4b_bound_source_current(
        binding: object, binding_record: Dict[str, Any]
    ) -> None:
        checker = binding_record.get("task4b_currentness_checker")
        checker_entry = task4b_checker_registry.get(id(binding))
        if checker_entry is None:
            if checker is not None:
                raise _BoundSourceIdentityDrift()
            _verify_task4b_provisional_bound_source_current(binding_record)
            return None
        if (
            checker_entry[0] is not binding
            or checker_entry[1] is not checker
            or not callable(checker_entry[1])
        ):
            raise _BoundSourceIdentityDrift()
        checker_entry[1]()

    def _verify_bound_source_current(
        binding: object, binding_record: Dict[str, Any]
    ) -> None:
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
            _verify_task4b_bound_source_current(binding, binding_record)
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
                    or captured_source_sha256(observed).hexdigest()
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
            _verify_bound_source_current(binding, entry[1])
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
            _verify_bound_source_current(binding, entry[1])
        except BaseException as error:
            cleanup_control, _cleanup_ordinary = _terminalize_sealed(
                sealed, owner
            )
            if not isinstance(error, Exception):
                raise
            if cleanup_control is not None:
                raise cleanup_control
            raise

    def _close_bound_source_rows(
        binding: Any, binding_record: Dict[str, Any]
    ) -> None:
        _drop_task4b_checker(binding, binding_record)
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
        binding_record["task4b_bound_objects"] = None
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
            _close_bound_source_rows(binding, entry[1])

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
            try:
                task4b_scan_objects = tuple(
                    _resolve_bound_object(bound_scan_module, name)
                    for name in task4b_scan_local_names
                )
                task4b_current_storage_objects = tuple(
                    _resolve_bound_object(bound_storage_module, name)
                    for name in task4b_storage_local_names
                )
                task4b_cross_objects = tuple(
                    _resolve_bound_object(
                        bound_scan_module if role == "scan" else bound_storage_module,
                        name,
                    )
                    for role, name in task4b_bound_object_names
                )
            except (AttributeError, TypeError):
                raise _InternalFailure()
            scan_exported_objects = getattr(
                bound_scan_module,
                "_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS",
                None,
            )
            storage_exported_objects = getattr(
                bound_storage_module,
                "_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS",
                None,
            )
            if (
                getattr(bound_scan_module,
                        "_TASK4B_SCAN_LOCAL_SURFACE_NAMES", None)
                != task4b_scan_local_names
                or getattr(bound_storage_module,
                           "_TASK4B_STORAGE_LOCAL_SURFACE_NAMES", None)
                != task4b_storage_local_names
                or type(scan_exported_objects) is not tuple
                or len(scan_exported_objects) != len(task4b_scan_local_names)
                or type(storage_exported_objects) is not tuple
                or len(storage_exported_objects)
                != len(task4b_storage_local_objects)
                or not all(
                    current is exported
                    for current, exported in zip(
                        task4b_scan_objects, scan_exported_objects
                    )
                )
                or not all(
                    current is original is exported
                    for current, original, exported in zip(
                        task4b_current_storage_objects,
                        task4b_storage_local_objects,
                        storage_exported_objects,
                    )
                )
                or not all(
                    current is expected
                    for current, expected in zip(
                        task4b_cross_objects,
                        task4b_scan_objects[1:]
                        + task4b_storage_local_objects[:8],
                    )
                )
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
                "task4b_bound_objects": (
                    task4b_scan_objects,
                    task4b_storage_local_objects,
                    task4b_cross_objects,
                ),
                "task4b_currentness_checker": None,
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
                    _close_bound_source_rows(binding, binding_record)
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
                _close_bound_source_rows(binding, binding_record)
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
        _verify_bound_source_current(binding, binding_entry[1])
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
        _verify_bound_source_current(binding, binding_entry[1])
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
        _verify_bound_source_current(binding, binding_record)
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
        task4b_attestation = verifier(
            reconciliation=reconciliation,
            expected_spool_identity=sealed,
            expected_finalization_identity=finalization,
        )
        task4b_checker = _make_task4b_binding_currentness_checker(
            binding_record, task4b_attestation
        )
        if (
            binding_record.get("task4b_currentness_checker") is not None
            or task4b_checker_registry.get(id(binding)) is not None
        ):
            _raise_storage_error()
        binding_record["task4b_currentness_checker"] = task4b_checker
        task4b_checker_registry[id(binding)] = (binding, task4b_checker)
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
            _verify_bound_source_current(binding, entry[1])
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
                        "phase": "capture_cleanup",
                        "control": None,
                        "ordinary": False,
                    }
                    owner["state"] = "closing"
                phase = terminal["phase"]
                if phase == "capture_cleanup":
                    cleanup_control, cleanup_ordinary = (
                        _cleanup_task4b_capture_staging(owner)
                    )
                    if terminal["control"] is None:
                        terminal["control"] = cleanup_control
                    terminal["ordinary"] = (
                        terminal["ordinary"] or cleanup_ordinary
                    )
                    terminal["phase"] = "source_cleanup"
                    continue
                if phase == "source_cleanup":
                    cleanup_control, cleanup_ordinary = (
                        _task4b_close_snapshot_source_authority(owner)
                    )
                    if terminal["control"] is None:
                        terminal["control"] = cleanup_control
                    terminal["ordinary"] = (
                        terminal["ordinary"] or cleanup_ordinary
                    )
                    terminal["phase"] = "revoke"
                    continue
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
                    elif registry is staging_snapshot_registry:
                        tombstones = staging_snapshot_tombstones
                    else:
                        raise _InternalFailure()
                    _retire_nonowner_handle(handle, registry, tombstones)
                    terminal["phase"] = "done"
                    continue
                if phase == "done":
                    if terminal["control"] is not None:
                        raise terminal["control"]
                    if terminal["ordinary"]:
                        break
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
                        break
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
        _HistoricalWindowCaptureReplaySource,
        HistoricalRunStagingSnapshot,
        HistoricalRunSnapshot,
        consume_production_historical_window_capability,
        open_validated_run,
        _open_historical_window_exchange_spool,
        _issue_historical_window_exchange_transfer_for_test,
        _get_historical_window_run_quota_for_test,
        _project_historical_window_exchange_spool_for_test,
        _freeze_historical_prefilter_grid,
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
    _HistoricalWindowCaptureReplaySource,
    HistoricalRunStagingSnapshot,
    HistoricalRunSnapshot,
    consume_production_historical_window_capability,
    open_validated_run,
    _open_historical_window_exchange_spool,
    _issue_historical_window_exchange_transfer_for_test,
    _get_historical_window_run_quota_for_test,
    _project_historical_window_exchange_spool_for_test,
    _freeze_historical_prefilter_grid,
) = _initialize_historical_foundry_storage_types()
del _initialize_historical_foundry_storage_types


_TASK4B_BOUND_OBJECT_NAMES = (
    ("scan", "_ProductionHistoricalWindowCaptureReplayEvent"),
    ("scan", "_bind_production_historical_window_capture_replay_source_from_bound_storage"),
    ("scan", "_replay_production_historical_window_capture_from_bound_storage"),
    ("scan", "_consume_production_historical_window_capture_replay_event_for_storage"),
    ("storage", "_HistoricalWindowCaptureReplaySource"),
    ("storage", "_HistoricalWindowCaptureReplaySource.__enter__"),
    ("storage", "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan"),
    ("storage", "_HistoricalWindowCaptureReplaySource.__iter__"),
    ("storage", "_HistoricalWindowCaptureReplaySource.__next__"),
    ("storage", "_HistoricalWindowCaptureReplaySource.__exit__"),
    ("storage", "_HistoricalWindowCaptureReplaySource.close"),
    ("storage", "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan"),
)

_TASK4B_STORAGE_LOCAL_SURFACE_NAMES = (
    "_HistoricalWindowCaptureReplaySource",
    "_HistoricalWindowCaptureReplaySource.__enter__",
    "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan",
    "_HistoricalWindowCaptureReplaySource.__iter__",
    "_HistoricalWindowCaptureReplaySource.__next__",
    "_HistoricalWindowCaptureReplaySource.__exit__",
    "_HistoricalWindowCaptureReplaySource.close",
    "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan",
    "HistoricalRunStagingSnapshot",
    "HistoricalRunStagingSnapshot.read_frozen_member",
    "HistoricalRunStagingSnapshot.frozen_identity_projection",
    "HistoricalRunStagingSnapshot.reread_frozen_members_unchanged",
    "HistoricalRunStagingSnapshot.close",
    "HistoricalRunStagingSnapshot.__enter__",
    "HistoricalRunStagingSnapshot.__exit__",
    "open_validated_run",
    "HistoricalRunSnapshot",
    "HistoricalRunSnapshot.read_member",
    "HistoricalRunSnapshot.identity_projection",
    "HistoricalRunSnapshot.reread_unchanged",
    "HistoricalRunSnapshot.close",
)
_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
    _HistoricalWindowCaptureReplaySource,
    _HistoricalWindowCaptureReplaySource.__enter__,
    _HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan,
    _HistoricalWindowCaptureReplaySource.__iter__,
    _HistoricalWindowCaptureReplaySource.__next__,
    _HistoricalWindowCaptureReplaySource.__exit__,
    _HistoricalWindowCaptureReplaySource.close,
    _ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan,
    HistoricalRunStagingSnapshot,
    HistoricalRunStagingSnapshot.read_frozen_member,
    HistoricalRunStagingSnapshot.frozen_identity_projection,
    HistoricalRunStagingSnapshot.reread_frozen_members_unchanged,
    HistoricalRunStagingSnapshot.close,
    HistoricalRunStagingSnapshot.__enter__,
    HistoricalRunStagingSnapshot.__exit__,
    open_validated_run,
    HistoricalRunSnapshot,
    HistoricalRunSnapshot.read_member,
    HistoricalRunSnapshot.identity_projection,
    HistoricalRunSnapshot.reread_unchanged,
    HistoricalRunSnapshot.close,
)
