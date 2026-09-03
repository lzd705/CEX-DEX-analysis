"""Bounded, route-scoped collection of synchronized market legs."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
import argparse
import base64
import ctypes
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import errno
import fcntl
import hashlib
import inspect
import json
import math
import multiprocessing
from multiprocessing.connection import wait as wait_for_connections
import os
from pathlib import Path, PurePath
from queue import Queue
import re
import stat
import sys
from threading import Lock, Thread, current_thread, enumerate as enumerate_threads
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit
import uuid

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.route_cohort import canonical_route_id, classify_route_timing
    from scripts.route_publication import (
        publish_complete_route_bundle,
        publish_route_cohort_bundle,
    )
    from scripts.fetch_cex_depth import (
        STRICT_CEX_TYPED_RULE_VENUES,
        cex_market_id,
        collect_cex_market_observation,
        load_cataloged_markets,
    )
    from scripts.fetch_dex_depth import (
        RpcClient,
        block_timestamp_text,
        canonical_route_fixed_block_header,
        collect_dex_pool_observation,
        freeze_v2_pool_state,
        is_canonical_rpc_quantity,
        protocol_model,
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )
    from scripts.execution_cost import USD_PRICE_SKEW_MAX_SECONDS
    from scripts.route_shadow_inputs import (
        TYPED_SOURCE_LINEAGE_SCHEMA,
        TYPED_SOURCE_LINEAGE_SCHEMA_V2,
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from scripts.route_quantity import (
        MAX_DEX_QUANTITY_STATE_AGE_SECONDS,
        MarketRules,
    )
except ModuleNotFoundError:
    from collection_deadline import CollectionDeadline, CollectionDeadlineExceeded
    from route_cohort import canonical_route_id, classify_route_timing
    from route_publication import (
        publish_complete_route_bundle,
        publish_route_cohort_bundle,
    )
    from fetch_cex_depth import (
        STRICT_CEX_TYPED_RULE_VENUES,
        cex_market_id,
        collect_cex_market_observation,
        load_cataloged_markets,
    )
    from fetch_dex_depth import (
        RpcClient,
        block_timestamp_text,
        canonical_route_fixed_block_header,
        collect_dex_pool_observation,
        freeze_v2_pool_state,
        is_canonical_rpc_quantity,
        protocol_model,
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )
    from execution_cost import USD_PRICE_SKEW_MAX_SECONDS
    from route_shadow_inputs import (  # type: ignore[no-redef]
        TYPED_SOURCE_LINEAGE_SCHEMA,
        TYPED_SOURCE_LINEAGE_SCHEMA_V2,
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from route_quantity import MAX_DEX_QUANTITY_STATE_AGE_SECONDS, MarketRules


class _DaemonFutureExecutor:
    """Small Python-3.8-compatible executor whose blocked workers cannot hold exit."""

    def __init__(self, max_workers: int) -> None:
        self._queue = Queue()
        self._closed = False
        self._lock = Lock()
        self._threads = []
        for index in range(max_workers):
            worker = Thread(
                target=self._worker,
                name="route-cohort-{}".format(index + 1),
                daemon=True,
            )
            worker.start()
            self._threads.append(worker)

    def _worker(self) -> None:
        while True:
            work = self._queue.get()
            if work is None:
                return
            future, function, args = work
            if not future.set_running_or_notify_cancel():
                continue
            try:
                result = function(*args)
            except BaseException as error:
                future.set_exception(error)
            else:
                future.set_result(result)

    def submit(self, function: Callable[..., Any], *args: Any) -> Future:
        with self._lock:
            if self._closed:
                raise RuntimeError("executor is shut down")
            future = Future()
            self._queue.put((future, function, args))
            return future

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                for _thread in self._threads:
                    self._queue.put(None)
        if wait:
            for thread in self._threads:
                thread.join()


class _RouteCollectionResult(dict):
    """JSON cohort plus a transport copy checked against disk authority."""

    __slots__ = ("_typed_source_payloads",)

    def __init__(
        self,
        value: Mapping[str, Any],
        typed_source_payloads: Mapping[str, Tuple[Mapping[str, Any], ...]],
    ) -> None:
        super().__init__(value)
        self._typed_source_payloads = {
            market_id: tuple(dict(item) for item in members)
            for market_id, members in typed_source_payloads.items()
        }


_CANONICAL_CHAIN_IDS = {
    "eth": "0x1",
    "optimism": "0xa",
    "bsc": "0x38",
    "zksync": "0x144",
    "base": "0x2105",
    "arbitrum": "0xa4b1",
}


def _run_process_call(
    connection: Any,
    function: Callable[..., Any],
    args: Tuple[Any, ...],
    child_close_fds: Tuple[int, ...],
) -> None:
    """Run one inherited callable and return only a value or a generic failure."""
    try:
        for descriptor in child_close_fds:
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            payload = ("result", function(*args))
        except BaseException:
            payload = ("error", "route collection worker failed")
        try:
            connection.send(payload)
        except BaseException:
            try:
                connection.send(("error", "route collection worker failed"))
            except BaseException:
                pass
    finally:
        connection.close()


def _require_single_threaded_fork() -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        raise RuntimeError("fork process isolation is unavailable")
    caller = current_thread()
    if any(
        thread is not caller and thread.is_alive()
        for thread in enumerate_threads()
    ):
        raise RuntimeError(
            "fork process isolation requires a single-threaded caller"
        )


class _ForkProcessExecutor:
    """Killable Unix-process executor for deadline-bound collector calls.

    The collector functions are intentionally inherited through ``fork``.  This
    keeps the production boundary killable without imposing a picklability
    contract on the existing collector closures.  Tests that need shared
    in-process state inject ``ThreadPoolExecutor`` explicitly.
    """

    def __init__(
        self,
        max_workers: int,
        *,
        child_close_fds: Iterable[int] = (),
    ) -> None:
        _require_single_threaded_fork()
        if type(max_workers) is not int or max_workers < 1:
            raise ValueError("max_workers must be positive")
        try:
            close_fds = tuple(child_close_fds)
        except TypeError as error:
            raise ValueError("child_close_fds must be an iterable of descriptors") from error
        if (
            any(type(descriptor) is not int or descriptor < 0 for descriptor in close_fds)
            or len(close_fds) != len(set(close_fds))
        ):
            raise ValueError("child_close_fds must contain unique nonnegative integers")
        self._context = multiprocessing.get_context("fork")
        self._max_workers = max_workers
        self._child_close_fds = close_fds
        self._closed = False
        self._lock = Lock()
        self._records: Dict[Future, Dict[str, Any]] = {}
        self._sequence = 0
        self._started_count = 0
        self._reaped_count = 0
        self._orphan_count = 0

    def submit(self, function: Callable[..., Any], *args: Any) -> Future:
        with self._lock:
            if self._closed:
                raise RuntimeError("executor is shut down")
            _require_single_threaded_fork()
            live = sum(
                1 for record in self._records.values()
                if record["process"].is_alive()
            )
            if live >= self._max_workers:
                raise RuntimeError("executor worker limit exceeded")
            future = Future()
            if not future.set_running_or_notify_cancel():  # pragma: no cover
                return future
            receive, send = self._context.Pipe(duplex=False)
            process = self._context.Process(
                target=_run_process_call,
                args=(send, function, tuple(args), self._child_close_fds),
                name="route-cohort-process-{}".format(self._sequence + 1),
            )
            process.daemon = True
            try:
                process.start()
            except BaseException:
                receive.close()
                send.close()
                future.set_exception(
                    RuntimeError("route collection process could not start")
                )
                return future
            send.close()
            self._sequence += 1
            self._started_count += 1
            record: Dict[str, Any] = {
                "process": process,
                "connection": receive,
                "reaped": False,
            }
            self._records[future] = record
            return future

    @staticmethod
    def _reap_process(process: Any) -> None:
        process.join(timeout=0.1)
        if process.is_alive():
            process.terminate()
            process.join(timeout=0.25)
        if process.is_alive():
            process.kill()
            process.join(timeout=0.25)

    def _finish(self, future: Future, record: Mapping[str, Any]) -> None:
        process = record["process"]
        connection = record["connection"]
        try:
            try:
                kind, value = connection.recv()
            except (EOFError, OSError, ValueError):
                kind, value = "error", "route collection worker terminated"
            self._reap_process(process)
            self._mark_reaped(record)
            if future.done():
                return
            if kind == "result":
                future.set_result(value)
            else:
                future.set_exception(RuntimeError(str(value)))
        finally:
            try:
                connection.close()
            except OSError:
                pass

    def _mark_reaped(self, record: Mapping[str, Any]) -> None:
        if record["process"].is_alive():
            return
        if not record.get("reaped"):
            record["reaped"] = True
            self._reaped_count += 1

    def process_evidence(self) -> Dict[str, int]:
        """Return observed child lifecycle counts without inferring success."""
        with self._lock:
            return {
                "collector_process_started_count": self._started_count,
                "collector_process_reaped_count": self._reaped_count,
                "orphan_process_count": self._orphan_count,
            }

    def wait_for_any(
        self,
        futures: Iterable[Future],
        timeout: Optional[float],
    ) -> Set[Future]:
        """Poll result pipes in the calling thread and complete ready futures."""
        targets = list(futures)
        done = {future for future in targets if future.done()}
        if done:
            return done
        with self._lock:
            by_connection = {
                record["connection"]: (future, record)
                for future, record in self._records.items()
                if future in targets and not future.done()
            }
        if not by_connection:
            return set()
        ready = wait_for_connections(list(by_connection), timeout=timeout)
        for connection in ready:
            future, record = by_connection[connection]
            self._finish(future, record)
            done.add(future)
        return done

    def shutdown(self, wait: bool = True) -> None:
        """Terminate, reap, and close every child even when ``wait`` is false."""
        del wait  # cleanup is mandatory for the production isolation boundary
        with self._lock:
            self._closed = True
            records = list(self._records.values())
        for record in records:
            process = record["process"]
            if process.is_alive():
                process.terminate()
        for record in records:
            record["process"].join(timeout=0.25)
            self._mark_reaped(record)
        for record in records:
            process = record["process"]
            if process.is_alive():
                process.kill()
        for record in records:
            record["process"].join(timeout=0.25)
            self._mark_reaped(record)
        for record in records:
            try:
                record["connection"].close()
            except OSError:
                pass
        with self._lock:
            pending_futures = [
                future for future in self._records if not future.done()
            ]
        for future in pending_futures:
            future.set_exception(
                RuntimeError("route collection worker terminated")
            )
        survivors = [
            record["process"].pid
            for record in records
            if record["process"].is_alive()
        ]
        self._orphan_count = len(survivors)
        if survivors:
            raise RuntimeError("route collection process cleanup failed")
        with self._lock:
            self._records.clear()


def collect_unique_route_legs(
    routes: Iterable[Mapping[str, Any]],
) -> List[str]:
    """Return each route-market identity once in canonical lexical order."""
    market_ids = set()
    for route in routes:
        if not isinstance(route, Mapping):
            raise ValueError("route candidate is invalid")
        buy = route.get("buy_market_id")
        sell = route.get("sell_market_id")
        if not isinstance(buy, str) or not isinstance(sell, str) or not buy or not sell:
            raise ValueError("route candidate is invalid")
        market_ids.add(buy)
        market_ids.add(sell)
    return sorted(market_ids)


def _validated_routes(
    routes: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    normalized = []
    route_ids = set()
    for route in routes:
        if not isinstance(route, Mapping):
            raise ValueError("route candidate is invalid")
        try:
            route_id = canonical_route_id(route)
        except ValueError as error:
            raise ValueError("route candidate is invalid") from error
        if route.get("route_id") != route_id:
            raise ValueError("route_id must be canonical")
        if route_id in route_ids:
            raise ValueError("duplicate route candidate")
        route_ids.add(route_id)
        normalized.append(dict(route))
    return normalized


def _canonical_reference_volume(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("route volume lineage is invalid")
    try:
        amount = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError("route volume lineage is invalid") from None
    if not amount.is_finite() or amount <= 0:
        raise ValueError("route volume lineage is invalid")
    text = format(amount, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _validate_route_volume_lineage(
    routes: Iterable[Mapping[str, Any]],
    selected_by_id: Mapping[str, Mapping[str, Any]],
) -> None:
    expected_by_market: Dict[str, Optional[str]] = {}
    for market_id, leg in selected_by_id.items():
        inputs = leg.get("selection_inputs")
        if not isinstance(inputs, Mapping):
            raise ValueError("route volume lineage is invalid")
        market_type = _market_type(leg)
        source_field = (
            "cex_selected_window_usd"
            if market_type == "cex"
            else "dex_24h_usd"
        )
        if source_field not in inputs:
            raise ValueError("route volume lineage is invalid")
        expected_by_market[market_id] = _canonical_reference_volume(
            inputs.get(source_field)
        )

    for route in routes:
        buy_id = str(route.get("buy_market_id") or "")
        sell_id = str(route.get("sell_market_id") or "")
        if buy_id not in expected_by_market or sell_id not in expected_by_market:
            raise ValueError("route volume lineage is invalid")
        buy_volume = expected_by_market[buy_id]
        sell_volume = expected_by_market[sell_id]
        expected_route_volume = None
        if buy_volume is not None and sell_volume is not None:
            expected_route_volume = _canonical_reference_volume(
                str(min(Decimal(buy_volume), Decimal(sell_volume)))
            )
        if (
            route.get("buy_reference_volume_usd") != buy_volume
            or route.get("sell_reference_volume_usd") != sell_volume
            or route.get("route_volume_usd") != expected_route_volume
            or route.get("route_volume_basis")
            != "minimum_leg_source_horizon_usd"
        ):
            raise ValueError("route volume lineage is invalid")


def _has_route_volume_lineage(
    routes: Iterable[Mapping[str, Any]],
    selected_by_id: Mapping[str, Mapping[str, Any]],
) -> bool:
    route_fields = {
        "buy_reference_volume_usd",
        "sell_reference_volume_usd",
        "route_volume_usd",
        "route_volume_basis",
    }
    return any(
        bool(route_fields & set(route)) for route in routes
    ) or any("selection_inputs" in leg for leg in selected_by_id.values())


def materialize_route_leg_rows(
    market_ids: Iterable[str],
    collected_rows: Mapping[str, Mapping[str, Any]],
    *,
    deadline_exceeded: Optional[Set[str]] = None,
    terminal_reasons: Optional[Mapping[str, str]] = None,
    fixed_blocks_by_market: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Normalize collected facts, retaining every requested terminal leg."""
    expired = deadline_exceeded or set()
    reasons = terminal_reasons or {}
    lineage = fixed_blocks_by_market or {}
    rows = []
    for market_id in sorted(set(market_ids)):
        if market_id in expired or market_id in reasons:
            row = {
                    "leg_id": market_id,
                    "market_id": market_id,
                    "status": (
                        "deadline_exceeded"
                        if market_id in expired
                        or reasons.get(market_id) == "route_deadline_exceeded"
                        else "failed"
                    ),
                    "available": False,
                    "reason_code": reasons.get(
                        market_id, "route_deadline_exceeded"
                    ),
                }
            if market_id in lineage:
                row.update(lineage[market_id])
            rows.append(row)
            continue
        row = dict(collected_rows.get(market_id, {}))
        row["leg_id"] = market_id
        row["market_id"] = market_id
        if market_id in lineage:
            row.update(lineage[market_id])
        rows.append(row)
    return rows


def _canonical_fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _open_typed_publication_lock(
    root_descriptor: int,
) -> Tuple[int, Tuple[int, int]]:
    name = ".typed-publication.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=root_descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        opened = os.fstat(descriptor)
        named = os.stat(
            name, dir_fd=root_descriptor, follow_symlinks=False
        )
        identity = (opened.st_dev, opened.st_ino)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or _attachment_file_fingerprint(opened)
            != _attachment_file_fingerprint(named)
        ):
            raise _UnsafeRawEvidence(
                "typed-source publication lock changed"
            )
        return descriptor, identity
    except BaseException:
        if "descriptor" in locals():
            os.close(descriptor)
        raise


def _require_typed_publication_lock(
    root_descriptor: int,
    lock_descriptor: int,
    expected_identity: Tuple[int, int],
) -> None:
    opened = os.fstat(lock_descriptor)
    named = os.stat(
        ".typed-publication.lock",
        dir_fd=root_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or (opened.st_dev, opened.st_ino) != expected_identity
        or _attachment_file_fingerprint(opened)
        != _attachment_file_fingerprint(named)
    ):
        raise _UnsafeRawEvidence("typed-source publication lock changed")


def _require_typed_publication_root(
    root: Path,
    root_descriptor: int,
    root_identity: Tuple[int, int],
) -> None:
    """Bind returned filesystem paths to the directory held during commit."""
    _reject_symlink_ancestry(root)
    _require_directory_identity(root, root_identity)
    _descriptor_directory_identity(root_descriptor, root_identity)
    _reject_symlink_ancestry(root)
    _require_directory_identity(root, root_identity)


def _typed_publication_file_evidence(
    descriptor: int,
    payload: bytes,
) -> Dict[str, Any]:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size != len(payload)
    ):
        raise _UnsafeRawEvidence("typed-source publication file is unsafe")
    return {
        "fingerprint": _attachment_file_fingerprint(metadata),
        "payload": payload,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _require_typed_publication_file(
    parent_descriptor: int,
    name: str,
    descriptor: int,
    evidence: Mapping[str, Any],
    *,
    changed_message: str,
) -> None:
    expected_payload = evidence.get("payload")
    expected_fingerprint = evidence.get("fingerprint")
    expected_sha256 = evidence.get("sha256")
    if (
        not isinstance(expected_payload, bytes)
        or not isinstance(expected_fingerprint, tuple)
        or len(expected_fingerprint) != 7
        or not isinstance(expected_sha256, str)
        or hashlib.sha256(expected_payload).hexdigest() != expected_sha256
    ):
        raise _UnsafeRawEvidence(changed_message)
    try:
        named_before = os.stat(
            name, dir_fd=parent_descriptor, follow_symlinks=False
        )
        opened_before = os.fstat(descriptor)
        payload = _read_open_attachment_file(
            descriptor, max_bytes=len(expected_payload)
        )
        opened_after = os.fstat(descriptor)
        named_after = os.stat(
            name, dir_fd=parent_descriptor, follow_symlinks=False
        )
    except (OSError, _UnsafeRawEvidence) as error:
        raise _UnsafeRawEvidence(changed_message) from error
    fingerprints = (
        _attachment_file_fingerprint(named_before),
        _attachment_file_fingerprint(opened_before),
        _attachment_file_fingerprint(opened_after),
        _attachment_file_fingerprint(named_after),
    )
    if (
        any(value != expected_fingerprint for value in fingerprints)
        or payload != expected_payload
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise _UnsafeRawEvidence(changed_message)


def _open_typed_publication_file(
    parent_descriptor: int,
    name: str,
    evidence: Mapping[str, Any],
    *,
    changed_message: str,
) -> int:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise _UnsafeRawEvidence(changed_message)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise _UnsafeRawEvidence(changed_message) from error
    try:
        _require_typed_publication_file(
            parent_descriptor,
            name,
            descriptor,
            evidence,
            changed_message=changed_message,
        )
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_typed_publication_inventory(
    parent_descriptor: int,
    name: str,
    directory_identity: Tuple[int, int],
    member_evidence: Mapping[str, Mapping[str, Any]],
    *,
    changed_message: str,
) -> Tuple[int, Dict[str, int]]:
    directory_descriptor = _open_directory_entry(
        parent_descriptor, name, directory_identity
    )
    member_descriptors: Dict[str, int] = {}
    try:
        expected_names = sorted(member_evidence)
        if sorted(os.listdir(directory_descriptor)) != expected_names:
            raise _UnsafeRawEvidence(changed_message)
        for member_name in expected_names:
            member_descriptors[member_name] = _open_typed_publication_file(
                directory_descriptor,
                member_name,
                member_evidence[member_name],
                changed_message=changed_message,
            )
        if sorted(os.listdir(directory_descriptor)) != expected_names:
            raise _UnsafeRawEvidence(changed_message)
        return directory_descriptor, member_descriptors
    except BaseException:
        for descriptor in member_descriptors.values():
            os.close(descriptor)
        os.close(directory_descriptor)
        raise


def _capture_promoted_typed_file(
    parent_descriptor: int,
    name: str,
    prior_evidence: Mapping[str, Any],
) -> Dict[str, Any]:
    """Refresh rename-mutated ctime while preserving inode and exact bytes."""
    payload = prior_evidence["payload"]
    prior_fingerprint = prior_evidence["fingerprint"]
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise _UnsafeRawEvidence(
            "typed-source manifest changed during publication"
        ) from error
    try:
        metadata = os.fstat(descriptor)
        if (
            (metadata.st_dev, metadata.st_ino)
            != tuple(prior_fingerprint[:2])
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != len(payload)
            or _read_open_attachment_file(
                descriptor, max_bytes=len(payload)
            ) != payload
        ):
            raise _UnsafeRawEvidence(
                "typed-source manifest changed during publication"
            )
        evidence = _typed_publication_file_evidence(descriptor, payload)
        _require_typed_publication_file(
            parent_descriptor,
            name,
            descriptor,
            evidence,
            changed_message="typed-source manifest changed during publication",
        )
        return evidence
    finally:
        os.close(descriptor)


def _detach_typed_publication_entry(
    root_descriptor: int,
    *,
    canonical_name: str,
    quarantine_name: str,
    expected_identity: Tuple[int, int],
    expected_kind: str,
    restore_foreign: bool,
) -> None:
    """Move one public name aside without ever deleting its current target."""
    _rename_directory_entry(
        canonical_name,
        quarantine_name,
        source_directory_fd=root_descriptor,
        destination_directory_fd=root_descriptor,
    )
    moved = _directory_entry_metadata(root_descriptor, quarantine_name)
    moved_identity = (
        None if moved is None else (moved.st_dev, moved.st_ino)
    )
    moved_kind_matches = (
        moved is not None
        and (
            (expected_kind == "file" and stat.S_ISREG(moved.st_mode))
            or (
                expected_kind == "directory"
                and stat.S_ISDIR(moved.st_mode)
            )
        )
    )
    if moved_identity == expected_identity and moved_kind_matches:
        if _directory_entry_metadata(root_descriptor, canonical_name) is not None:
            raise _UnsafeRawEvidence(
                "typed-source canonical name changed during quarantine"
            )
        return

    failure = _UnsafeRawEvidence(
        "typed-source quarantine detached a foreign entry"
    )
    if not restore_foreign:
        raise failure

    restore_error = None
    if (
        moved is not None
        and _directory_entry_metadata(root_descriptor, canonical_name) is None
    ):
        try:
            _rename_directory_entry(
                quarantine_name,
                canonical_name,
                source_directory_fd=root_descriptor,
                destination_directory_fd=root_descriptor,
            )
            restored = _directory_entry_metadata(
                root_descriptor, canonical_name
            )
            if (
                restored is None
                or (restored.st_dev, restored.st_ino) != moved_identity
            ):
                restore_error = _UnsafeRawEvidence(
                    "typed-source foreign quarantine restore changed"
                )
        except BaseException as error:
            restore_error = error
    else:
        restore_error = _UnsafeRawEvidence(
            "typed-source foreign quarantine could not be restored"
        )
    if restore_error is not None:
        raise failure from restore_error
    raise failure


def _quarantine_typed_source_publication(
    root: Path,
    *,
    root_descriptor: int,
    root_identity: Tuple[int, int],
    lock_descriptor: int,
    lock_identity: Tuple[int, int],
    typed_identity: Optional[Tuple[int, int]],
    manifest_evidence: Optional[Mapping[str, Any]],
) -> None:
    """Invalidate a failed publication by atomic no-replace detachment."""
    token = uuid.uuid4().hex
    failures: List[BaseException] = []
    _descriptor_directory_identity(root_descriptor, root_identity)
    try:
        _require_typed_publication_root(
            root, root_descriptor, root_identity
        )
    except BaseException as error:
        failures.append(error)
    try:
        _require_typed_publication_lock(
            root_descriptor, lock_descriptor, lock_identity
        )
    except BaseException as error:
        failures.append(error)

    # The manifest is the commit marker, so detach it before the data tree.
    if manifest_evidence is not None:
        try:
            _detach_typed_publication_entry(
                root_descriptor,
                canonical_name="typed-manifest.json",
                quarantine_name=(
                    ".typed-quarantine-{}-typed-manifest.json".format(token)
                ),
                expected_identity=tuple(
                    manifest_evidence["fingerprint"][:2]
                ),
                expected_kind="file",
                restore_foreign=False,
            )
        except BaseException as error:
            failures.append(error)
    if typed_identity is not None:
        try:
            _detach_typed_publication_entry(
                root_descriptor,
                canonical_name="typed",
                quarantine_name=(
                    ".typed-quarantine-{}-typed".format(token)
                ),
                expected_identity=typed_identity,
                expected_kind="directory",
                restore_foreign=True,
            )
        except BaseException as error:
            failures.append(error)
    os.fsync(root_descriptor)

    manifest_now = _directory_entry_metadata(
        root_descriptor, "typed-manifest.json"
    )
    typed_now = _directory_entry_metadata(root_descriptor, "typed")
    if (
        manifest_evidence is not None
        and manifest_now is not None
        and (manifest_now.st_dev, manifest_now.st_ino)
        == tuple(manifest_evidence["fingerprint"][:2])
    ):
        failures.append(_UnsafeRawEvidence(
            "typed-source canonical manifest remains after quarantine"
        ))
    if (
        typed_identity is not None
        and typed_now is not None
        and (typed_now.st_dev, typed_now.st_ino) == typed_identity
    ):
        failures.append(_UnsafeRawEvidence(
            "typed-source canonical directory remains after quarantine"
        ))
    if not failures and (manifest_now is not None or typed_now is not None):
        failures.append(_UnsafeRawEvidence(
            "typed-source canonical publication changed during quarantine"
        ))
    try:
        _require_typed_publication_root(
            root, root_descriptor, root_identity
        )
    except BaseException as error:
        failures.append(error)
    try:
        _require_typed_publication_lock(
            root_descriptor, lock_descriptor, lock_identity
        )
    except BaseException as error:
        failures.append(error)
    if failures:
        raise _UnsafeRawEvidence(
            "typed-source publication quarantine failed"
        ) from failures[0]


def publish_typed_source_manifest(
    raw_run_root: Path,
    *,
    raw_evidence_run_id: str,
    members: Iterable[Mapping[str, Any]],
    source_validator: Optional[Callable[[], None]] = None,
) -> Dict[str, Any]:
    """Atomically retain exact observed typed members and their manifest."""
    if not isinstance(raw_evidence_run_id, str) or not _SNAPSHOT_ID.fullmatch(
        raw_evidence_run_id
    ):
        raise ValueError("typed-source raw run ID is invalid")
    if source_validator is not None and not callable(source_validator):
        raise ValueError("typed-source source validator is invalid")
    root = _canonical_raw_path(Path(raw_run_root))
    _reject_symlink_ancestry(root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root_identity = _directory_identity(root)
    stage_name = ".typed-stage-{}".format(uuid.uuid4().hex)
    root_descriptor = os.open(str(root), _directory_open_flags())
    lock_descriptor = None
    lock_identity = None
    stage_descriptor = None
    typed_stage_descriptor = None
    stage_identity = None
    typed_identity = None
    member_evidence: Dict[str, Mapping[str, Any]] = {}
    manifest_evidence: Optional[Mapping[str, Any]] = None
    installed_typed = False
    installed_manifest = False
    expected_root_inventory: Tuple[str, ...] = ()
    records = []
    seen = set()
    try:
        _descriptor_directory_identity(root_descriptor, root_identity)
        lock_descriptor, lock_identity = _open_typed_publication_lock(
            root_descriptor
        )
        os.mkdir(stage_name, mode=0o700, dir_fd=root_descriptor)
        stage_identity = _directory_entry_identity(
            root_descriptor, stage_name
        )
        stage_descriptor = _open_directory_entry(
            root_descriptor, stage_name, stage_identity
        )
        os.mkdir("typed", mode=0o700, dir_fd=stage_descriptor)
        typed_identity = _directory_entry_identity(
            stage_descriptor, "typed"
        )
        typed_stage_descriptor = _open_directory_entry(
            stage_descriptor, "typed", typed_identity
        )

        normalized = []
        for raw in members:
            if not isinstance(raw, Mapping) or set(raw) != {
                "market_id", "role", "payload", "logical_generation",
                "adapter_id", "content_schema",
            }:
                raise ValueError("typed-source producer member schema is invalid")
            market_id = raw.get("market_id")
            role = raw.get("role")
            contract = TYPED_SOURCE_ROLE_CONTRACTS.get(role)
            payload = raw.get("payload")
            key = (market_id, role)
            if key in seen:
                raise ValueError("typed-source market/role must be unique")
            seen.add(key)
            if (
                not isinstance(market_id, str)
                or not market_id.startswith(("cex:", "dex:"))
                or contract is None
                or raw.get("adapter_id") != contract["adapter_id"]
                or raw.get("content_schema") != contract["content_schema"]
                or not isinstance(payload, bytes)
                or not 0 < len(payload) <= contract["max_bytes"]
                or not isinstance(raw.get("logical_generation"), str)
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    raw["logical_generation"],
                    flags=re.ASCII,
                ) is None
            ):
                raise ValueError("typed-source producer member is invalid")
            normalized.append(dict(raw))
        normalized.sort(key=lambda row: (row["market_id"], row["role"]))
        for index, raw in enumerate(normalized):
            filename = "{:04d}-{}.json".format(index, raw["role"])
            payload = raw["payload"]
            descriptor = os.open(
                filename,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=typed_stage_descriptor,
            )
            try:
                offset = 0
                while offset < len(payload):
                    written = os.write(descriptor, payload[offset:])
                    if written <= 0:
                        raise OSError("short typed-source member write")
                    offset += written
                os.fsync(descriptor)
                evidence = _typed_publication_file_evidence(
                    descriptor, payload
                )
            finally:
                os.close(descriptor)
            member_evidence[filename] = evidence
            record = {
                "market_id": raw["market_id"],
                "role": raw["role"],
                "filename": filename,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
                "logical_generation": raw["logical_generation"],
                "adapter_id": raw["adapter_id"],
                "content_schema": raw["content_schema"],
            }
            if set(record) != TYPED_SOURCE_MANIFEST_MEMBER_FIELDS:
                raise AssertionError("typed-source manifest member schema drifted")
            records.append(record)
        os.fsync(typed_stage_descriptor)
        manifest = {
            "schema": TYPED_SOURCE_MANIFEST_SCHEMA,
            "raw_evidence_run_id": raw_evidence_run_id,
            "member_count": len(records),
            "members": records,
        }
        if set(manifest) != TYPED_SOURCE_MANIFEST_FIELDS:
            raise AssertionError("typed-source manifest schema drifted")
        manifest_bytes = json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        manifest_descriptor = os.open(
            "typed-manifest.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=stage_descriptor,
        )
        try:
            offset = 0
            while offset < len(manifest_bytes):
                written = os.write(
                    manifest_descriptor, manifest_bytes[offset:]
                )
                if written <= 0:
                    raise OSError("short typed-source manifest write")
                offset += written
            os.fsync(manifest_descriptor)
            manifest_evidence = _typed_publication_file_evidence(
                manifest_descriptor, manifest_bytes
            )
        finally:
            os.close(manifest_descriptor)
        os.fsync(stage_descriptor)
        final_typed = root / "typed"
        final_manifest = root / "typed-manifest.json"
        if (
            _directory_entry_metadata(root_descriptor, "typed") is not None
            or _directory_entry_metadata(
                root_descriptor, "typed-manifest.json"
            ) is not None
        ):
            raise ValueError("immutable typed-source inventory already exists")
        root_inventory_before_commit = tuple(
            sorted(os.listdir(root_descriptor))
        )
        try:
            if source_validator is not None:
                source_validator()
            if (
                _directory_entry_identity(root_descriptor, stage_name)
                != stage_identity
                or sorted(os.listdir(stage_descriptor))
                != ["typed", "typed-manifest.json"]
            ):
                raise _UnsafeRawEvidence(
                    "typed-source stage changed before publication"
                )
            _rename_directory_entry(
                "typed",
                "typed",
                source_directory_fd=stage_descriptor,
                destination_directory_fd=root_descriptor,
            )
            installed_typed = True
            expected_root_inventory = tuple(sorted(
                root_inventory_before_commit + ("typed",)
            ))
            if sorted(os.listdir(root_descriptor)) != list(
                expected_root_inventory
            ):
                raise _UnsafeRawEvidence(
                    "typed-source publication root inventory changed"
                )
            _rename_directory_entry(
                "typed-manifest.json",
                "typed-manifest.json",
                source_directory_fd=stage_descriptor,
                destination_directory_fd=root_descriptor,
            )
            installed_manifest = True
            expected_root_inventory = tuple(sorted(
                root_inventory_before_commit
                + ("typed", "typed-manifest.json")
            ))
            manifest_evidence = _capture_promoted_typed_file(
                root_descriptor,
                "typed-manifest.json",
                manifest_evidence,
            )
            os.fsync(root_descriptor)
            if source_validator is not None:
                source_validator()
            if sorted(os.listdir(root_descriptor)) != list(
                expected_root_inventory
            ):
                raise _UnsafeRawEvidence(
                    "typed-source publication root inventory changed"
                )
            verification_typed = None
            verification_members: Dict[str, int] = {}
            verification_manifest = None
            try:
                verification_typed, verification_members = (
                    _open_typed_publication_inventory(
                        root_descriptor,
                        "typed",
                        typed_identity,
                        member_evidence,
                        changed_message=(
                            "typed-source member changed after publication"
                        ),
                    )
                )
                verification_manifest = _open_typed_publication_file(
                    root_descriptor,
                    "typed-manifest.json",
                    manifest_evidence,
                    changed_message=(
                        "typed-source manifest changed after publication"
                    ),
                )
            finally:
                if verification_manifest is not None:
                    os.close(verification_manifest)
                for descriptor in verification_members.values():
                    os.close(descriptor)
                if verification_typed is not None:
                    os.close(verification_typed)
            _require_typed_publication_lock(
                root_descriptor, lock_descriptor, lock_identity
            )
            _require_typed_publication_root(
                root, root_descriptor, root_identity
            )
        except BaseException:
            if installed_typed or installed_manifest:
                _quarantine_typed_source_publication(
                    root,
                    root_descriptor=root_descriptor,
                    root_identity=root_identity,
                    lock_descriptor=lock_descriptor,
                    lock_identity=lock_identity,
                    typed_identity=(typed_identity if installed_typed else None),
                    manifest_evidence=(
                        manifest_evidence if installed_manifest else None
                    ),
                )
            raise
        return {
            "manifest": manifest,
            "typed_source_manifest_sha256": hashlib.sha256(
                manifest_bytes
            ).hexdigest(),
            "typed_root": str(final_typed),
            "manifest_path": str(final_manifest),
        }
    finally:
        if typed_stage_descriptor is not None:
            os.close(typed_stage_descriptor)
        if stage_descriptor is not None:
            os.close(stage_descriptor)
        if lock_descriptor is not None:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            os.close(lock_descriptor)
        os.close(root_descriptor)


def _typed_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _typed_unavailable(role: str, reason: str) -> Dict[str, Any]:
    contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
    return {
        "role": role,
        "status": "unavailable",
        "reason_code": reason,
        "filename": None,
        "sha256": None,
        "size": None,
        "logical_generation": None,
        "adapter_id": contract["adapter_id"],
        "content_schema": contract["content_schema"],
    }


def _typed_member_spec(
    market_id: str, role: str, payload: bytes, logical_generation: str
) -> Dict[str, Any]:
    contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
    return {
        "market_id": market_id,
        "role": role,
        "payload": payload,
        "logical_generation": logical_generation,
        "adapter_id": contract["adapter_id"],
        "content_schema": contract["content_schema"],
    }


def _attachment_authority_bytes(
    *,
    market_id: str,
    trusted_leg: Mapping[str, Any],
    collector_row: Mapping[str, Any],
    accepted_raw_sha256: str,
    collection_input_generation: str,
    validated_specs: Iterable[Mapping[str, Any]],
) -> bytes:
    """Seal only safe, parent-validated facts needed by later attachment."""
    identity = _canonical_leg_identity(trusted_leg)
    market_type = identity["market_type"]
    if trusted_leg.get("market_id") != market_id:
        raise ValueError("attachment authority identity is invalid")
    generation_projection = _safe_leg_projection({
        "collection_input_generation": collection_input_generation,
    })
    if generation_projection.get(
        "collection_input_generation"
    ) != collection_input_generation:
        raise ValueError("attachment authority generation is invalid")
    trusted_input = _safe_leg_projection({
        **dict(trusted_leg),
        **identity,
    })
    canonical_collector_row = _safe_leg_projection({
        **dict(collector_row),
        "market_id": market_id,
        "market_type": market_type,
    })
    if not trusted_input or not canonical_collector_row:
        raise ValueError("attachment authority projection is invalid")
    final_leg = _final_route_leg_projection(
        trusted_input,
        canonical_collector_row,
        market_id=market_id,
    )
    raw_values = []
    expected_specs = []
    for spec in validated_specs:
        if not isinstance(spec, Mapping) or set(spec) != {
            "market_id", "role", "payload", "logical_generation",
            "adapter_id", "content_schema",
        }:
            raise ValueError("attachment authority typed member is invalid")
        raw_values.append({
            "role": spec.get("role"),
            "payload": spec.get("payload"),
        })
        expected_specs.append(dict(spec))
    canonical_specs = _validated_typed_payload_inventory(
        trusted_leg=trusted_input,
        collector_row=canonical_collector_row,
        accepted_raw_sha256=accepted_raw_sha256,
        values=raw_values,
    )
    if tuple(expected_specs) != canonical_specs:
        raise ValueError("attachment authority typed inventory is invalid")
    typed_members = []
    for spec in canonical_specs:
        payload = spec["payload"]
        typed_members.append({
            "market_id": market_id,
            "role": spec["role"],
            "logical_generation": spec["logical_generation"],
            "adapter_id": spec["adapter_id"],
            "content_schema": spec["content_schema"],
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "payload_base64": base64.b64encode(payload).decode("ascii"),
        })
    authority = {
        "schema": _ATTACHMENT_AUTHORITY_SCHEMA,
        "market_id": market_id,
        "market_type": market_type,
        "collection_input_generation": collection_input_generation,
        "accepted_raw_response_sha256": accepted_raw_sha256,
        "trusted_input": trusted_input,
        "collector_row": canonical_collector_row,
        "final_leg": final_leg,
        "typed_members": typed_members,
    }
    payload = _typed_json_bytes(authority)
    if not 0 < len(payload) <= _ATTACHMENT_AUTHORITY_MAX_BYTES:
        raise ValueError("attachment authority is too large")
    return payload


def _validated_attachment_authority(
    payload: bytes,
    *,
    market_id: str,
    market_type: str,
    accepted_raw_sha256: str,
    collection_input_generation: Any,
) -> Tuple[Dict[str, Any], Tuple[Mapping[str, Any], ...]]:
    """Rebuild attachment facts solely from one canonical disk authority."""
    if not 0 < len(payload) <= _ATTACHMENT_AUTHORITY_MAX_BYTES:
        raise ValueError("attachment authority is invalid")
    try:
        authority = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("attachment authority is invalid") from error
    if (
        not isinstance(authority, Mapping)
        or set(authority) != {
            "schema", "market_id", "market_type",
            "collection_input_generation",
            "accepted_raw_response_sha256", "trusted_input",
            "collector_row", "final_leg", "typed_members",
        }
        or payload != _typed_json_bytes(authority)
        or authority.get("schema") != _ATTACHMENT_AUTHORITY_SCHEMA
        or authority.get("market_id") != market_id
        or authority.get("market_type") != market_type
        or authority.get("accepted_raw_response_sha256")
        != accepted_raw_sha256
        or authority.get("collection_input_generation")
        != collection_input_generation
    ):
        raise ValueError("attachment authority is invalid")
    trusted_input = authority.get("trusted_input")
    collector_row = authority.get("collector_row")
    final_leg = authority.get("final_leg")
    typed_members = authority.get("typed_members")
    if (
        not isinstance(trusted_input, Mapping)
        or not isinstance(collector_row, Mapping)
        or not isinstance(final_leg, Mapping)
        or not isinstance(typed_members, list)
        or _safe_leg_projection(trusted_input) != dict(trusted_input)
        or _safe_leg_projection(collector_row) != dict(collector_row)
        or _safe_leg_projection(final_leg) != dict(final_leg)
    ):
        raise ValueError("attachment authority is invalid")
    if (
        _market_type(trusted_input) != market_type
        or trusted_input.get("market_id") != market_id
        or collector_row.get("market_id") != market_id
        or collector_row.get("market_type") != market_type
        or collector_row.get("status") not in {"observed", "partial"}
        or collector_row.get("raw_response_sha256")
        != accepted_raw_sha256
    ):
        raise ValueError("attachment authority is invalid")
    rebuilt_final_leg = _final_route_leg_projection(
        trusted_input,
        collector_row,
        market_id=market_id,
    )
    if dict(final_leg) != rebuilt_final_leg:
        raise ValueError("attachment authority final leg is invalid")
    values = []
    expected_specs = []
    seen_roles = set()
    for member in typed_members:
        if (
            not isinstance(member, Mapping)
            or set(member) != {
                "market_id", "role", "logical_generation", "adapter_id",
                "content_schema", "size", "sha256", "payload_base64",
            }
            or member.get("market_id") != market_id
            or member.get("role") in seen_roles
            or type(member.get("size")) is not int
            or member["size"] <= 0
            or _TYPED_SHA256.fullmatch(str(member.get("sha256") or ""))
            is None
            or not isinstance(member.get("payload_base64"), str)
        ):
            raise ValueError("attachment authority typed member is invalid")
        try:
            member_payload = base64.b64decode(
                member["payload_base64"].encode("ascii"), validate=True
            )
        except (UnicodeEncodeError, ValueError) as error:
            raise ValueError(
                "attachment authority typed member is invalid"
            ) from error
        if (
            len(member_payload) != member["size"]
            or hashlib.sha256(member_payload).hexdigest()
            != member["sha256"]
        ):
            raise ValueError("attachment authority typed member is invalid")
        seen_roles.add(member["role"])
        values.append({"role": member["role"], "payload": member_payload})
        expected_specs.append({
            "market_id": market_id,
            "role": member["role"],
            "payload": member_payload,
            "logical_generation": member.get("logical_generation"),
            "adapter_id": member.get("adapter_id"),
            "content_schema": member.get("content_schema"),
        })
    canonical_specs = _validated_typed_payload_inventory(
        trusted_leg=trusted_input,
        collector_row=collector_row,
        accepted_raw_sha256=accepted_raw_sha256,
        values=values,
    )
    if tuple(expected_specs) != canonical_specs:
        raise ValueError("attachment authority typed inventory is invalid")
    return dict(final_leg), canonical_specs


def _attachment_file_fingerprint(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_open_attachment_file(
    descriptor: int,
    *,
    max_bytes: Optional[int],
) -> bytes:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (
            max_bytes is not None
            and (
                type(max_bytes) is not int
                or max_bytes <= 0
                or metadata.st_size > max_bytes
            )
        )
    ):
        raise _UnsafeRawEvidence("accepted evidence file is unsafe")
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    total = 0
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if max_bytes is not None and total > max_bytes:
            raise _UnsafeRawEvidence("accepted evidence file is too large")
        chunks.append(chunk)
    return b"".join(chunks)


def _open_held_attachment_file(
    directory_descriptor: int,
    name: str,
    *,
    max_bytes: Optional[int] = None,
) -> Tuple[int, bytes, Tuple[int, ...]]:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise _UnsafeRawEvidence("accepted evidence filename is invalid")
    try:
        before = os.stat(
            name, dir_fd=directory_descriptor, follow_symlinks=False
        )
    except OSError as error:
        raise _UnsafeRawEvidence(
            "accepted evidence file is unavailable"
        ) from error
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=directory_descriptor)
    except OSError as error:
        raise _UnsafeRawEvidence(
            "accepted evidence file could not be opened safely"
        ) from error
    try:
        payload = _read_open_attachment_file(
            descriptor, max_bytes=max_bytes
        )
        opened = os.fstat(descriptor)
        after = os.stat(
            name, dir_fd=directory_descriptor, follow_symlinks=False
        )
        fingerprint = _attachment_file_fingerprint(opened)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or _attachment_file_fingerprint(before) != fingerprint
            or _attachment_file_fingerprint(after) != fingerprint
            or len(payload) != opened.st_size
        ):
            raise _UnsafeRawEvidence(
                "accepted evidence file identity changed"
            )
        return descriptor, payload, fingerprint
    except BaseException:
        os.close(descriptor)
        raise


class _HeldAttachmentEvidence:
    """Accepted evidence whose exact open files stay pinned to publication."""

    __slots__ = (
        "accepted_root", "accepted_identity", "accepted_descriptor",
        "entry_name", "entry_identity", "entry_descriptor",
        "response_descriptor", "response_payload", "response_fingerprint",
        "authority_descriptor", "authority_payload", "authority_fingerprint",
        "closed",
    )

    def __init__(
        self,
        *,
        accepted_root: Path,
        accepted_identity: Tuple[int, int],
        accepted_descriptor: int,
        entry_name: str,
        entry_identity: Tuple[int, int],
        entry_descriptor: int,
        response_descriptor: int,
        response_payload: bytes,
        response_fingerprint: Tuple[int, ...],
        authority_descriptor: int,
        authority_payload: bytes,
        authority_fingerprint: Tuple[int, ...],
    ) -> None:
        self.accepted_root = accepted_root
        self.accepted_identity = accepted_identity
        self.accepted_descriptor = accepted_descriptor
        self.entry_name = entry_name
        self.entry_identity = entry_identity
        self.entry_descriptor = entry_descriptor
        self.response_descriptor = response_descriptor
        self.response_payload = response_payload
        self.response_fingerprint = response_fingerprint
        self.authority_descriptor = authority_descriptor
        self.authority_payload = authority_payload
        self.authority_fingerprint = authority_fingerprint
        self.closed = False

    @property
    def response_sha256(self) -> str:
        return hashlib.sha256(self.response_payload).hexdigest()

    def _validate_file(
        self,
        *,
        name: str,
        descriptor: int,
        expected_payload: bytes,
        expected_fingerprint: Tuple[int, ...],
        max_bytes: Optional[int],
    ) -> None:
        before_descriptor = os.fstat(descriptor)
        before_name = os.stat(
            name,
            dir_fd=self.entry_descriptor,
            follow_symlinks=False,
        )
        payload = _read_open_attachment_file(
            descriptor, max_bytes=max_bytes
        )
        after_descriptor = os.fstat(descriptor)
        after_name = os.stat(
            name,
            dir_fd=self.entry_descriptor,
            follow_symlinks=False,
        )
        if (
            _attachment_file_fingerprint(before_descriptor)
            != expected_fingerprint
            or _attachment_file_fingerprint(before_name)
            != expected_fingerprint
            or _attachment_file_fingerprint(after_descriptor)
            != expected_fingerprint
            or _attachment_file_fingerprint(after_name)
            != expected_fingerprint
            or payload != expected_payload
        ):
            raise _UnsafeRawEvidence(
                "accepted evidence file changed before publication"
            )

    def validate(self) -> None:
        if self.closed:
            raise _UnsafeRawEvidence("accepted evidence handle is closed")
        _descriptor_directory_identity(
            self.accepted_descriptor, self.accepted_identity
        )
        _descriptor_directory_identity(
            self.entry_descriptor, self.entry_identity
        )
        if _directory_entry_identity(
            self.accepted_descriptor, self.entry_name
        ) != self.entry_identity:
            raise _UnsafeRawEvidence(
                "accepted evidence directory identity changed"
            )
        if sorted(os.listdir(self.entry_descriptor)) != [
            _ATTACHMENT_AUTHORITY_FILENAME,
            "response.json",
        ]:
            raise _UnsafeRawEvidence(
                "accepted evidence file inventory changed"
            )
        self._validate_file(
            name="response.json",
            descriptor=self.response_descriptor,
            expected_payload=self.response_payload,
            expected_fingerprint=self.response_fingerprint,
            max_bytes=None,
        )
        self._validate_file(
            name=_ATTACHMENT_AUTHORITY_FILENAME,
            descriptor=self.authority_descriptor,
            expected_payload=self.authority_payload,
            expected_fingerprint=self.authority_fingerprint,
            max_bytes=_ATTACHMENT_AUTHORITY_MAX_BYTES,
        )
        _require_directory_identity(
            self.accepted_root, self.accepted_identity
        )
        _reject_symlink_ancestry(self.accepted_root)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for descriptor in (
            self.authority_descriptor,
            self.response_descriptor,
            self.entry_descriptor,
            self.accepted_descriptor,
        ):
            os.close(descriptor)


def _accepted_evidence_for_attachment(
    raw_run_root: Path,
    market_id: str,
) -> _HeldAttachmentEvidence:
    """Open one accepted response and authority and retain every handle."""
    accepted_root = raw_run_root / "accepted"
    _reject_symlink_ancestry(accepted_root)
    accepted_identity = _directory_identity(accepted_root)
    try:
        accepted_descriptor = os.open(
            str(accepted_root), _directory_open_flags()
        )
    except OSError as error:
        raise _UnsafeRawEvidence(
            "accepted raw evidence could not be opened safely"
        ) from error
    entry_descriptor = None
    response_descriptor = None
    authority_descriptor = None
    try:
        _descriptor_directory_identity(
            accepted_descriptor, accepted_identity
        )
        entry_name = hashlib.sha256(market_id.encode("utf-8")).hexdigest()
        entry_identity = _directory_entry_identity(
            accepted_descriptor, entry_name
        )
        entry_descriptor = _open_directory_entry(
            accepted_descriptor, entry_name, entry_identity
        )
        if sorted(os.listdir(entry_descriptor)) != [
            _ATTACHMENT_AUTHORITY_FILENAME,
            "response.json",
        ]:
            raise _UnsafeRawEvidence(
                "accepted evidence file inventory is invalid"
            )
        (
            response_descriptor,
            response_payload,
            response_fingerprint,
        ) = _open_held_attachment_file(
            entry_descriptor,
            "response.json",
        )
        (
            authority_descriptor,
            authority_payload,
            authority_fingerprint,
        ) = _open_held_attachment_file(
            entry_descriptor,
            _ATTACHMENT_AUTHORITY_FILENAME,
            max_bytes=_ATTACHMENT_AUTHORITY_MAX_BYTES,
        )
        if _directory_entry_identity(
            accepted_descriptor, entry_name
        ) != entry_identity:
            raise _UnsafeRawEvidence(
                "accepted evidence directory identity changed"
            )
        _descriptor_directory_identity(
            accepted_descriptor, accepted_identity
        )
        _require_directory_identity(accepted_root, accepted_identity)
        _reject_symlink_ancestry(accepted_root)
        evidence = _HeldAttachmentEvidence(
            accepted_root=accepted_root,
            accepted_identity=accepted_identity,
            accepted_descriptor=accepted_descriptor,
            entry_name=entry_name,
            entry_identity=entry_identity,
            entry_descriptor=entry_descriptor,
            response_descriptor=response_descriptor,
            response_payload=response_payload,
            response_fingerprint=response_fingerprint,
            authority_descriptor=authority_descriptor,
            authority_payload=authority_payload,
            authority_fingerprint=authority_fingerprint,
        )
        evidence.validate()
        return evidence
    except BaseException:
        for descriptor in (
            authority_descriptor,
            response_descriptor,
            entry_descriptor,
            accepted_descriptor,
        ):
            if descriptor is not None:
                os.close(descriptor)
        raise


def _attach_typed_source_lineage_with_evidence(
    cohort: Mapping[str, Any],
    *,
    raw_root: Path,
    accepted_evidence: Mapping[str, _HeldAttachmentEvidence],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Install retained typed members and bind every leg to the manifest."""
    if not isinstance(cohort, Mapping):
        raise ValueError("typed-source cohort is invalid")
    run_id = cohort.get("raw_evidence_run_id")
    if not isinstance(run_id, str) or _SNAPSHOT_ID.fullmatch(run_id) is None:
        raise ValueError("typed-source raw run ID is invalid")
    raw_run_root = _canonical_raw_path(Path(raw_root)) / run_id
    legs = cohort.get("legs")
    if not isinstance(legs, list):
        raise ValueError("typed-source cohort legs are invalid")
    specs: List[Dict[str, Any]] = []
    has_sealed_capability = hasattr(cohort, "_typed_source_payloads")
    sealed_payloads = getattr(cohort, "_typed_source_payloads", {})
    if not isinstance(sealed_payloads, Mapping):
        raise ValueError("typed-source payload capability is invalid")
    eligible_market_ids = {
        leg.get("market_id")
        for leg in legs
        if isinstance(leg, Mapping)
        and leg.get("status") in {"observed", "partial"}
    }
    if (
        any(not isinstance(value, str) for value in eligible_market_ids)
        or any(not isinstance(value, str) for value in sealed_payloads)
        or set(sealed_payloads) - eligible_market_ids
        or (
            has_sealed_capability
            and set(sealed_payloads) != eligible_market_ids
        )
    ):
        raise ValueError("typed-source payload capability is invalid")
    pending: Dict[str, Dict[str, Dict[str, Any]]] = {}
    authoritative_legs: Dict[str, Dict[str, Any]] = {}
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise ValueError("typed-source leg is invalid")
        market_id = leg.get("market_id")
        market_type = leg.get("market_type")
        if not isinstance(market_id, str) or market_type not in {"cex", "dex"}:
            raise ValueError("typed-source leg identity is invalid")
        contracts = {
            role: contract
            for role, contract in TYPED_SOURCE_ROLE_CONTRACTS.items()
            if contract["market_type"] == market_type
        }
        members = {
            role: _typed_unavailable(role, "typed_source_missing")
            for role in contracts
        }
        available = leg.get("status") in {"observed", "partial"}
        response = None
        actual_raw_sha256 = None
        authority_specs: Tuple[Mapping[str, Any], ...] = ()
        if available:
            try:
                evidence = accepted_evidence[market_id]
                evidence.validate()
                response = evidence.response_payload
                actual_raw_sha256 = evidence.response_sha256
                authority_payload = evidence.authority_payload
                authoritative_leg, authority_specs = (
                    _validated_attachment_authority(
                        authority_payload,
                        market_id=market_id,
                        market_type=market_type,
                        accepted_raw_sha256=actual_raw_sha256,
                        collection_input_generation=cohort.get(
                            "collection_input_generation"
                        ),
                    )
                )
            except (
                KeyError, FileNotFoundError, OSError, TypeError, ValueError,
            ) as error:
                raise ValueError(
                    "typed-source accepted raw evidence is invalid"
                ) from error
            if dict(leg) != authoritative_leg:
                raise ValueError(
                    "typed-source attachment authority differs from cohort leg"
                )
            authoritative_legs[market_id] = authoritative_leg
        if market_type == "cex":
            venue = market_id.split(":", 2)[1]
            if venue not in STRICT_CEX_TYPED_RULE_VENUES:
                members["cex_market_rules"] = _typed_unavailable(
                    "cex_market_rules", "typed_source_adapter_unsupported"
                )
                members["quote_usd_conversion"] = _typed_unavailable(
                    "quote_usd_conversion", "typed_source_adapter_unsupported"
                )
        if market_type == "cex" and available:
            if venue in STRICT_CEX_TYPED_RULE_VENUES:
                members["cex_market_rules"] = _typed_unavailable(
                    "cex_market_rules", "typed_source_failed"
                )
                members["quote_usd_conversion"] = _typed_unavailable(
                    "quote_usd_conversion", "typed_source_failed"
                )
            assert response is not None
            assert actual_raw_sha256 is not None
            specs.append(_typed_member_spec(
                market_id,
                "cex_raw_book_response",
                response,
                actual_raw_sha256,
            ))
        elif market_type == "dex":
            if available:
                for role in ("dex_market_rules", "dex_usd_conversion"):
                    members[role] = _typed_unavailable(
                        role, "typed_source_failed"
                    )
            context = (
                authoritative_legs[market_id].get("collector_context")
                if available
                else None
            )
            if available and isinstance(context, Mapping):
                context_payload = _typed_json_bytes(context)
                specs.append(_typed_member_spec(
                    market_id, "dex_usd_price_context", context_payload,
                    hashlib.sha256(context_payload).hexdigest(),
                ))
        raw_sealed_members = sealed_payloads.get(market_id, ())
        if not isinstance(raw_sealed_members, (list, tuple)):
            raise ValueError("typed-source payload capability is invalid")
        sealed_by_role = {}
        for sealed in raw_sealed_members:
            if (
                not isinstance(sealed, Mapping)
                or set(sealed) != {
                    "market_id", "role", "payload", "logical_generation",
                    "adapter_id", "content_schema",
                }
                or sealed.get("market_id") != market_id
            ):
                raise ValueError("typed-source payload capability is invalid")
            role = sealed.get("role")
            if role in sealed_by_role:
                raise ValueError("typed-source payload capability is invalid")
            sealed_by_role[role] = dict(sealed)
        if available:
            if (
                set(sealed_by_role)
                != {item["role"] for item in authority_specs}
                or any(
                    sealed_by_role[item["role"]] != dict(item)
                    for item in authority_specs
                )
            ):
                raise ValueError("typed-source payload capability is invalid")
            specs.extend(dict(item) for item in authority_specs)
        elif sealed_by_role:
            raise ValueError("typed-source payload capability is invalid")
        pending[market_id] = members

    def validate_accepted_sources() -> None:
        if set(accepted_evidence) != eligible_market_ids:
            raise _UnsafeRawEvidence(
                "accepted evidence inventory differs from eligible legs"
            )
        for market_id in sorted(accepted_evidence):
            accepted_evidence[market_id].validate()

    publication = publish_typed_source_manifest(
        raw_run_root,
        raw_evidence_run_id=run_id,
        members=specs,
        source_validator=validate_accepted_sources,
    )
    for record in publication["manifest"]["members"]:
        member = {
            "role": record["role"],
            "status": "observed",
            "reason_code": None,
            "filename": record["filename"],
            "sha256": record["sha256"],
            "size": record["size"],
            "logical_generation": record["logical_generation"],
            "adapter_id": record["adapter_id"],
            "content_schema": record["content_schema"],
        }
        pending[record["market_id"]][record["role"]] = member
    normalized_legs = []
    observed_inventory = []
    for untrusted_leg in legs:
        market_id = untrusted_leg["market_id"]
        leg = authoritative_legs.get(market_id, dict(untrusted_leg))
        market_type = leg["market_type"]
        lineage = validate_typed_source_lineage({
            "schema": (
                TYPED_SOURCE_LINEAGE_SCHEMA_V2
                if market_type == "dex"
                else TYPED_SOURCE_LINEAGE_SCHEMA
            ),
            "members": sorted(
                pending[market_id].values(), key=lambda row: row["role"]
            ),
        }, market_type=market_type)
        normalized_legs.append({**dict(leg), "typed_source_lineage": lineage})
        for item in typed_source_lineage_observed_members(
            lineage, market_type=market_type
        ):
            observed_inventory.append({"market_id": market_id, **item})
    if observed_inventory != publication["manifest"]["members"]:
        raise ValueError("typed-source manifest/core inventory differs")
    normalized = dict(cohort)
    normalized["legs"] = normalized_legs
    normalized.pop("route_cohort_id", None)
    normalized.pop("fingerprint", None)
    without_hashes = dict(normalized)
    normalized["route_cohort_id"] = "cohort:" + _canonical_fingerprint(
        without_hashes
    )
    normalized["fingerprint"] = _canonical_fingerprint(normalized)
    return normalized, publication


def attach_typed_source_lineage(
    cohort: Mapping[str, Any], *, raw_root: Path
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Hold accepted evidence descriptors through the publication boundary."""
    if not isinstance(cohort, Mapping):
        raise ValueError("typed-source cohort is invalid")
    run_id = cohort.get("raw_evidence_run_id")
    legs = cohort.get("legs")
    if (
        not isinstance(run_id, str)
        or _SNAPSHOT_ID.fullmatch(run_id) is None
        or not isinstance(legs, list)
    ):
        raise ValueError("typed-source cohort is invalid")
    sealed_payloads = getattr(cohort, "_typed_source_payloads", {})
    eligible_market_ids = {
        leg.get("market_id")
        for leg in legs
        if isinstance(leg, Mapping)
        and leg.get("status") in {"observed", "partial"}
    }
    if (
        not isinstance(sealed_payloads, Mapping)
        or any(not isinstance(value, str) for value in eligible_market_ids)
        or any(not isinstance(value, str) for value in sealed_payloads)
        or set(sealed_payloads) - eligible_market_ids
        or (
            hasattr(cohort, "_typed_source_payloads")
            and set(sealed_payloads) != eligible_market_ids
        )
    ):
        raise ValueError("typed-source payload capability is invalid")
    raw_run_root = _canonical_raw_path(Path(raw_root)) / run_id
    accepted_evidence: Dict[str, _HeldAttachmentEvidence] = {}
    try:
        for leg in legs:
            if (
                not isinstance(leg, Mapping)
                or leg.get("status") not in {"observed", "partial"}
            ):
                continue
            market_id = leg.get("market_id")
            market_type = leg.get("market_type")
            if (
                not isinstance(market_id, str)
                or market_type not in {"cex", "dex"}
                or market_id in accepted_evidence
            ):
                raise ValueError("typed-source leg identity is invalid")
            try:
                accepted_evidence[market_id] = (
                    _accepted_evidence_for_attachment(
                        raw_run_root, market_id
                    )
                )
            except (
                FileNotFoundError, OSError, TypeError, ValueError,
            ) as error:
                raise ValueError(
                    "typed-source accepted raw evidence is invalid"
                ) from error
        return _attach_typed_source_lineage_with_evidence(
            cohort,
            raw_root=raw_root,
            accepted_evidence=accepted_evidence,
        )
    finally:
        for evidence in accepted_evidence.values():
            evidence.close()


_CEX_MARKET_ID = re.compile(
    r"cex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})/"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)
_DEX_MARKET_ID = re.compile(
    r"dex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([a-z0-9][a-z0-9._-]{0,127}):"
    r"([A-Za-z0-9][A-Za-z0-9._-]{0,255}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)


def _selected_identity_field_matches(
    leg: Mapping[str, Any],
    key: str,
    expected: str,
    *,
    normalization: str,
) -> bool:
    if key not in leg:
        return True
    supplied = leg.get(key)
    if (
        not isinstance(supplied, str)
        or not supplied
        or supplied != supplied.strip()
    ):
        return False
    if normalization == "lower":
        return supplied.strip().lower() == expected
    if normalization == "upper":
        return supplied.strip().upper() == expected
    return supplied.strip() == expected


def _canonical_leg_identity(leg: Mapping[str, Any]) -> Dict[str, str]:
    declared = leg.get("market_type")
    market_id = leg.get("market_id")
    if not isinstance(market_id, str):
        raise ValueError("route leg identity is invalid")
    cex_match = _CEX_MARKET_ID.fullmatch(market_id)
    dex_match = _DEX_MARKET_ID.fullmatch(market_id)
    if cex_match is not None:
        inferred = "cex"
        exchange, base, quote = cex_match.groups()
        identity = {
            "market_type": inferred,
            "exchange": exchange,
            "cex_symbol": "{}/{}".format(base, quote),
            "token_symbol": base,
        }
        comparisons = (
            ("exchange", exchange, "lower"),
            ("cex_symbol", identity["cex_symbol"], "upper"),
            ("token_symbol", base, "upper"),
        )
    elif dex_match is not None:
        inferred = "dex"
        chain, dex, pool, token = dex_match.groups()
        if pool.startswith("0x") and pool != pool.lower():
            raise ValueError("route leg identity is invalid")
        identity = {
            "market_type": inferred,
            "chain": chain,
            "dex": dex,
            "pool_address": pool,
            "token_symbol": token,
        }
        comparisons = (
            ("chain", chain, "lower"),
            ("dex", dex, "lower"),
            ("pool_address", pool, "exact"),
            ("token_symbol", token, "upper"),
        )
    elif market_id.startswith(("cex:", "dex:")):
        raise ValueError("route leg identity is invalid")
    else:
        raise ValueError("route leg market type is invalid")
    if declared not in (None, "", "cex", "dex"):
        raise ValueError("route leg market type is invalid")
    if declared not in (None, "") and declared != inferred:
        raise ValueError("route leg market type does not match market_id")
    if any(
        not _selected_identity_field_matches(
            leg,
            key,
            expected,
            normalization=normalization,
        )
        for key, expected, normalization in comparisons
    ):
        raise ValueError("route leg identity is invalid")
    return identity


def _market_type(leg: Mapping[str, Any]) -> str:
    return _canonical_leg_identity(leg)["market_type"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_utc(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("{} is invalid".format(field)) from error
    else:
        raise ValueError("{} is invalid".format(field))
    if parsed.tzinfo is None:
        raise ValueError("{} is invalid".format(field))
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_datetime(value: Any, *, field: str) -> datetime:
    return datetime.fromisoformat(
        _canonical_utc(value, field=field).replace("Z", "+00:00")
    )


def _source_key(leg: Mapping[str, Any]) -> Tuple[str, str]:
    identity = _canonical_leg_identity(leg)
    if identity["market_type"] == "cex":
        return "cex", identity["exchange"]
    return "dex", identity["chain"]


def _terminal_for_chain(
    legs_by_market: Mapping[str, Mapping[str, Any]],
    market_ids: Iterable[str],
    chain: str,
    reason: str,
    terminal_reasons: Dict[str, str],
) -> None:
    for market_id in market_ids:
        leg = legs_by_market[market_id]
        if _market_type(leg) == "dex" and _source_key(leg)[1] == chain:
            terminal_reasons[market_id] = reason


def _row_from_collector(value: Any) -> Mapping[str, Any]:
    if isinstance(value, tuple) and len(value) == 2:
        value = value[0]
    if not isinstance(value, Mapping):
        raise ValueError("route leg collector returned an invalid row")
    return value


def _collector_accepts_typed_sink(collector: Callable[..., Any]) -> bool:
    """Do not pass a new keyword to legacy/custom collectors by accident."""
    try:
        parameters = inspect.signature(collector).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        item.name == "typed_source_payload_sink"
        for item in parameters
    )


def _collector_accepts_degraded_usd_context(
    collector: Callable[..., Any],
) -> bool:
    """Opt in only collectors that explicitly declare the route-only flag."""
    try:
        parameters = inspect.signature(collector).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        item.name == "allow_degraded_usd_context"
        for item in parameters
    )


_TYPED_ASSET = re.compile(
    r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII
)
_TYPED_EVM_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", flags=re.ASCII)
_TYPED_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_ZERO_EVM_ADDRESS = "0x" + "0" * 40
_ATTACHMENT_AUTHORITY_SCHEMA = "route_attachment_authority/v1"
_ATTACHMENT_AUTHORITY_FILENAME = "attachment-authority.json"
_ATTACHMENT_AUTHORITY_MAX_BYTES = 40 * 1024 * 1024
_DEX_COLLECTOR_CONTEXT_FIELDS = frozenset({
    "schema", "snapshot_id", "request_started_at", "observed_at",
    "response_received_at", "status", "reason_code", "pool_name",
    "base_token_id", "quote_token_id", "base_token_price_usd",
    "quote_token_price_usd", "tvl_method", "source", "source_endpoint",
    "raw_response_sha256",
})
_DEX_MARKET_RULES_SOURCE_FIELDS = frozenset({
    "schema", "market_id", "base_asset", "quote_asset",
    "base_token_address", "quote_token_address",
    "base_unit_decimals", "quote_unit_decimals",
    "base_increment", "quote_increment",
    "min_base_quantity", "min_quote_notional",
    "increment_source", "minimum_source",
    "observed_at", "valid_until", "raw_response_sha256",
})
_DEX_USD_CONVERSION_SOURCE_FIELDS = frozenset({
    "schema", "market_id", "target_asset", "target_token_address",
    "quote_asset", "quote_token_address", "usd_per_quote", "value_status",
    "observed_at", "valid_until", "state_observed_at", "source",
    "source_snapshot_id", "source_raw_response_sha256",
    "state_raw_response_sha256",
})


def _typed_nonzero_evm_address(value: Any, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or _TYPED_EVM_ADDRESS.fullmatch(value) is None
        or value == _ZERO_EVM_ADDRESS
    ):
        raise ValueError("{} is not a canonical nonzero EVM address".format(field))
    return value


def _typed_positive_decimal_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("{} is not a canonical positive Decimal".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(
            "{} is not a canonical positive Decimal".format(field)
        ) from error
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if not number.is_finite() or number <= 0 or canonical != value:
        raise ValueError("{} is not a canonical positive Decimal".format(field))
    return value


def _dex_context_token_address(value: Any, *, chain: str, field: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("{} is invalid".format(field))
    prefix = chain + "_"
    if not value.startswith(prefix):
        raise ValueError("{} is invalid".format(field))
    address = value[len(prefix):]
    return _typed_nonzero_evm_address(address, field=field)


def _strict_dex_collector_input(leg: Mapping[str, Any]) -> Dict[str, Any]:
    """Project a captured collector context without inventing pool facts."""
    identity = _canonical_leg_identity(leg)
    if identity["market_type"] != "dex":
        raise ValueError("DEX collector leg is invalid")
    context = leg.get("collector_context")
    if not isinstance(context, Mapping):
        return {**dict(leg), **identity}
    context_status = context.get("status")
    if (
        set(context) != _DEX_COLLECTOR_CONTEXT_FIELDS
        or context.get("schema") != "route_collector_context/v1"
        or context_status not in {
            "observed", "missing", "not_found", "source_no_observation",
            "failed", "error", "collection_failed", "unavailable",
            "not_cataloged_in_snapshot",
        }
    ):
        raise ValueError("DEX collector context is invalid")
    pool_address = _typed_nonzero_evm_address(
        identity["pool_address"], field="pool_address"
    )
    if protocol_model(identity["dex"], identity["chain"], pool_address)[0] == "unsupported":
        raise ValueError("DEX collector protocol is unsupported")
    target_address = _typed_nonzero_evm_address(
        leg.get("target_token_address"), field="target_token_address"
    )
    target_side = leg.get("target_token_side")
    if context_status == "observed":
        if target_side not in {"base", "quote"}:
            raise ValueError("DEX target Token side is invalid")
        base_address = _dex_context_token_address(
            context.get("base_token_id"),
            chain=identity["chain"],
            field="base_token_id",
        )
        quote_address = _dex_context_token_address(
            context.get("quote_token_id"),
            chain=identity["chain"],
            field="quote_token_id",
        )
        if base_address == quote_address or (
            target_address != (
                base_address if target_side == "base" else quote_address
            )
        ):
            raise ValueError("DEX collector Token identity is invalid")
        _typed_positive_decimal_text(
            context.get("base_token_price_usd"), field="base_token_price_usd"
        )
        _typed_positive_decimal_text(
            context.get("quote_token_price_usd"), field="quote_token_price_usd"
        )
    elif target_side is not None or any(
        context.get(field) is not None
        for field in (
            "base_token_id", "quote_token_id",
            "base_token_price_usd", "quote_token_price_usd",
        )
    ):
        raise ValueError("non-observed DEX collector context is invalid")
    request_started = _utc_datetime(
        context.get("request_started_at"), field="request_started_at"
    )
    observed = _utc_datetime(context.get("observed_at"), field="observed_at")
    response_received = _utc_datetime(
        context.get("response_received_at"), field="response_received_at"
    )
    if not request_started <= observed <= response_received:
        raise ValueError("DEX collector context timestamps are invalid")
    for field in (
        "snapshot_id", "reason_code", "pool_name", "tvl_method", "source",
        "source_endpoint",
    ):
        value = context.get(field)
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError("DEX collector context is invalid")
    if _TYPED_SHA256.fullmatch(
        str(context.get("raw_response_sha256") or "")
    ) is None:
        raise ValueError("DEX collector context is invalid")
    projected = {**dict(leg), **identity}
    for field, expected in context.items():
        if field == "schema":
            continue
        supplied = leg.get(field)
        if supplied not in (None, "") and supplied != expected:
            raise ValueError("DEX collector context conflicts with route leg")
        projected[field] = expected
    return projected


def _trusted_dex_collector_projection(
    leg: Mapping[str, Any],
) -> Dict[str, Any]:
    """Revalidate immutable DEX inputs without treating result fields as inputs."""
    _canonical_leg_identity(leg)
    return _strict_dex_collector_input({
        key: leg[key]
        for key in (
            "market_id", "market_type", "token_symbol",
            "target_token_address", "target_token_side", "collector_context",
        )
        if key in leg
    })


def _typed_unit_increment(decimals: int) -> str:
    if type(decimals) is not int or not 0 <= decimals <= 255:
        raise ValueError("typed asset decimals are invalid")
    return "1" if decimals == 0 else "0." + "0" * (decimals - 1) + "1"


def _typed_exact_window(
    observed_at: Any,
    valid_until: Any,
    *,
    duration_seconds: int,
) -> Tuple[datetime, datetime]:
    observed = _utc_datetime(observed_at, field="typed observed_at")
    valid = _utc_datetime(valid_until, field="typed valid_until")
    if valid - observed != timedelta(seconds=duration_seconds):
        raise ValueError("typed validity window is invalid")
    return observed, valid


def _typed_row_decimals(value: Any, *, field: str) -> int:
    if (
        not isinstance(value, str)
        or not value
        or not value.isdigit()
        or str(int(value)) != value
    ):
        raise ValueError("{} is invalid".format(field))
    decimals = int(value)
    if not 0 <= decimals <= 255:
        raise ValueError("{} is invalid".format(field))
    return decimals


def _trusted_dex_directional_binding(
    trusted_leg: Mapping[str, Any],
    collector_row: Mapping[str, Any],
    accepted_raw_sha256: str,
    *,
    require_usd_lineage: bool,
) -> Dict[str, Any]:
    identity = _canonical_leg_identity(trusted_leg)
    if identity["market_type"] != "dex":
        raise ValueError("DEX typed-source identity is invalid")
    pool_address = _typed_nonzero_evm_address(
        identity["pool_address"], field="pool_address"
    )
    if protocol_model(
        identity["dex"], identity["chain"], pool_address
    )[0] == "unsupported":
        raise ValueError("DEX typed-source protocol is unsupported")
    projected = _trusted_dex_collector_projection(trusted_leg)
    context = trusted_leg.get("collector_context")
    if require_usd_lineage and (
        not isinstance(context, Mapping)
        or context.get("status") != "observed"
    ):
        raise ValueError("DEX typed-source collector context is unavailable")
    if (
        collector_row.get("status") not in {"observed", "partial"}
        or collector_row.get("token_symbol") != identity["token_symbol"]
        or collector_row.get("chain") != identity["chain"]
        or collector_row.get("dex") != identity["dex"]
        or collector_row.get("pool_address") != identity["pool_address"]
        or collector_row.get("raw_response_sha256") != accepted_raw_sha256
    ):
        raise ValueError("DEX typed-source collector row is invalid")
    if require_usd_lineage:
        assert isinstance(context, Mapping)
        for field in (
            "usd_price_source_snapshot_id", "usd_price_observed_at",
            "usd_price_source", "usd_price_source_endpoint",
            "usd_price_raw_response_sha256",
        ):
            context_field = {
                "usd_price_source_snapshot_id": "snapshot_id",
                "usd_price_observed_at": "observed_at",
                "usd_price_source": "source",
                "usd_price_source_endpoint": "source_endpoint",
                "usd_price_raw_response_sha256": "raw_response_sha256",
            }[field]
            if collector_row.get(field) != context.get(context_field):
                raise ValueError("DEX typed-source price lineage is invalid")

    token_addresses = (
        _typed_nonzero_evm_address(
            collector_row.get("token0_address"), field="token0_address"
        ),
        _typed_nonzero_evm_address(
            collector_row.get("token1_address"), field="token1_address"
        ),
    )
    if token_addresses[0] == token_addresses[1]:
        raise ValueError("DEX typed-source token identities are invalid")
    if isinstance(context, Mapping) and context.get("status") == "observed":
        context_addresses = {
            _dex_context_token_address(
                context["base_token_id"],
                chain=identity["chain"],
                field="base_token_id",
            ),
            _dex_context_token_address(
                context["quote_token_id"],
                chain=identity["chain"],
                field="quote_token_id",
            ),
        }
        if set(token_addresses) != context_addresses:
            raise ValueError("DEX typed-source token identities are invalid")
    position = collector_row.get("target_token_position")
    if position not in {"token0", "token1"}:
        raise ValueError("DEX typed-source target position is invalid")
    target_index = int(position[-1])
    quote_index = 1 - target_index
    target_address = _typed_nonzero_evm_address(
        trusted_leg.get("target_token_address"), field="target_token_address"
    )
    if (
        token_addresses[target_index] != target_address
        or collector_row.get("target_token_address") != target_address
        or projected.get("target_token_address") != target_address
    ):
        raise ValueError("DEX typed-source target identity is invalid")
    symbols = (
        collector_row.get("token0_symbol"),
        collector_row.get("token1_symbol"),
    )
    if any(
        not isinstance(symbol, str)
        or _TYPED_ASSET.fullmatch(symbol) is None
        for symbol in symbols
    ) or (
        symbols[target_index] != identity["token_symbol"]
        or symbols[quote_index] == identity["token_symbol"]
    ):
        raise ValueError("DEX typed-source assets are invalid")
    decimals = (
        _typed_row_decimals(
            collector_row.get("token0_decimals"), field="token0_decimals"
        ),
        _typed_row_decimals(
            collector_row.get("token1_decimals"), field="token1_decimals"
        ),
    )
    state_observed_at = collector_row.get("block_timestamp")
    _utc_datetime(state_observed_at, field="typed state observed_at")
    binding = {
        "market_id": trusted_leg["market_id"],
        "target_asset": symbols[target_index],
        "quote_asset": symbols[quote_index],
        "target_token_address": target_address,
        "quote_token_address": token_addresses[quote_index],
        "target_unit_decimals": decimals[target_index],
        "quote_unit_decimals": decimals[quote_index],
        "state_observed_at": state_observed_at,
        "state_raw_response_sha256": accepted_raw_sha256,
        "token0_address": token_addresses[0],
        "token1_address": token_addresses[1],
        "token0_decimals": decimals[0],
        "token1_decimals": decimals[1],
    }
    if require_usd_lineage:
        assert isinstance(context, Mapping)
        context_prices = {
            _dex_context_token_address(
                context["base_token_id"],
                chain=identity["chain"],
                field="base_token_id",
            ): _typed_positive_decimal_text(
                context["base_token_price_usd"],
                field="base_token_price_usd",
            ),
            _dex_context_token_address(
                context["quote_token_id"],
                chain=identity["chain"],
                field="quote_token_id",
            ): _typed_positive_decimal_text(
                context["quote_token_price_usd"],
                field="quote_token_price_usd",
            ),
        }
        prices = (
            _typed_positive_decimal_text(
                collector_row.get("token0_price_usd"), field="token0_price_usd"
            ),
            _typed_positive_decimal_text(
                collector_row.get("token1_price_usd"), field="token1_price_usd"
            ),
        )
        if any(
            prices[index] != context_prices[token_addresses[index]]
            for index in (0, 1)
        ):
            raise ValueError("DEX typed-source observed prices are invalid")
        binding.update({
            "usd_per_quote": prices[quote_index],
            "source": context["source"],
            "source_snapshot_id": context["snapshot_id"],
            "source_raw_response_sha256": context["raw_response_sha256"],
            "conversion_observed_at": context["observed_at"],
        })
    return binding


def _validate_dex_pool_state_binding(
    pool_state: Any,
    trusted_leg: Mapping[str, Any],
    collector_row: Mapping[str, Any],
    accepted_raw_sha256: str,
) -> None:
    identity = _canonical_leg_identity(trusted_leg)
    target_address = _typed_nonzero_evm_address(
        trusted_leg.get("target_token_address"), field="target_token_address"
    )
    row_token0 = _typed_nonzero_evm_address(
        collector_row.get("token0_address"), field="token0_address"
    )
    row_token1 = _typed_nonzero_evm_address(
        collector_row.get("token1_address"), field="token1_address"
    )
    if (
        pool_state.chain != identity["chain"]
        or pool_state.dex != identity["dex"]
        or pool_state.pool_address != identity["pool_address"]
        or collector_row.get("chain") != identity["chain"]
        or collector_row.get("dex") != identity["dex"]
        or collector_row.get("pool_address") != identity["pool_address"]
        or pool_state.token0_address != row_token0
        or pool_state.token1_address != row_token1
        or pool_state.token0_decimals != _typed_row_decimals(
            collector_row.get("token0_decimals"), field="token0_decimals"
        )
        or pool_state.token1_decimals != _typed_row_decimals(
            collector_row.get("token1_decimals"), field="token1_decimals"
        )
        or target_address not in {row_token0, row_token1}
        or _utc_datetime(
            pool_state.observed_at, field="typed pool observed_at"
        ) != _utc_datetime(
            collector_row.get("block_timestamp"),
            field="typed collector block_timestamp",
        )
        or pool_state.raw_response_sha256 != accepted_raw_sha256
        or collector_row.get("raw_response_sha256") != accepted_raw_sha256
    ):
        raise ValueError("DEX pool-state binding is invalid")
    context = trusted_leg.get("collector_context")
    if isinstance(context, Mapping) and context.get("status") == "observed":
        context_addresses = {
            _dex_context_token_address(
                context.get("base_token_id"),
                chain=identity["chain"],
                field="base_token_id",
            ),
            _dex_context_token_address(
                context.get("quote_token_id"),
                chain=identity["chain"],
                field="quote_token_id",
            ),
        }
        if context_addresses != {row_token0, row_token1}:
            raise ValueError("DEX pool-state context binding is invalid")


def _validated_typed_payload_inventory(
    trusted_leg: Mapping[str, Any],
    collector_row: Mapping[str, Any],
    accepted_raw_sha256: str,
    values: Iterable[Any],
) -> Tuple[Mapping[str, Any], ...]:
    if not isinstance(trusted_leg, Mapping) or not isinstance(
        collector_row, Mapping
    ):
        raise ValueError("typed-source worker inventory is invalid")
    try:
        identity = _canonical_leg_identity(trusted_leg)
        market_id = trusted_leg["market_id"]
        market_type = identity["market_type"]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("typed-source worker inventory is invalid") from error
    if (
        _TYPED_SHA256.fullmatch(str(accepted_raw_sha256 or "")) is None
        or not _collector_identity_matches(
            market_id, market_type, trusted_leg, collector_row
        )
        or collector_row.get("raw_response_sha256")
        not in (None, "", accepted_raw_sha256)
    ):
        raise ValueError("typed-source worker inventory is invalid")
    members = []
    seen = set()
    aggregate = 0
    parsed_by_role: Dict[str, Any] = {}
    for raw in values:
        if not isinstance(raw, Mapping) or set(raw) != {"role", "payload"}:
            raise ValueError("typed-source worker inventory is invalid")
        role = raw.get("role")
        payload = raw.get("payload")
        contract = TYPED_SOURCE_ROLE_CONTRACTS.get(role)
        if (
            role in seen
            or contract is None
            or contract["market_type"] != market_type
            or not isinstance(payload, bytes)
            or not 0 < len(payload) <= contract["max_bytes"]
        ):
            raise ValueError("typed-source worker inventory is invalid")
        if market_type == "cex" and role in {
            "cex_market_rules", "quote_usd_conversion"
        }:
            try:
                prefix, venue, _instrument = market_id.split(":", 2)
            except ValueError as error:
                raise ValueError(
                    "typed-source worker inventory is invalid"
                ) from error
            if prefix != "cex" or venue not in STRICT_CEX_TYPED_RULE_VENUES:
                raise ValueError("typed-source worker inventory is invalid")
        seen.add(role)
        aggregate += len(payload)
        if aggregate > 24 * 1024 * 1024:
            raise ValueError("typed-source worker inventory is invalid")
        if role == "dex_pool_state":
            try:
                parsed = json.loads(payload.decode("utf-8"))
                frozen_state = freeze_v2_pool_state({
                    **dict(parsed),
                    **{
                        field: int(parsed[field])
                        for field in (
                            "chain_id", "token0_decimals", "token1_decimals",
                            "reserve0_raw", "reserve1_raw",
                            "reserve_timestamp_last_raw", "fee_bps",
                            "fee_numerator", "fee_denominator", "block_number",
                        )
                    },
                })
                if (
                    not isinstance(parsed, Mapping)
                    or parsed.get("schema") != contract["content_schema"]
                    or parsed.get("state_id") != frozen_state.state_id
                    or payload != _typed_json_bytes(parsed)
                ):
                    raise ValueError("invalid state")
                logical_generation = parsed["state_id"].split(":", 1)[1]
                parsed_by_role[role] = frozen_state
            except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError("typed-source worker inventory is invalid") from error
        elif role in {"cex_market_rules", "quote_usd_conversion"}:
            try:
                parsed = json.loads(payload.decode("utf-8"))
                if not isinstance(parsed, Mapping) or payload != _typed_json_bytes(parsed):
                    raise ValueError("invalid typed JSON")
                if role == "cex_market_rules":
                    expected_fields = {
                        "schema", "market_id", "base_asset", "quote_asset",
                        "base_unit_decimals", "quote_unit_decimals",
                        "base_increment", "quote_increment",
                        "min_base_quantity", "min_quote_notional",
                        "observed_at", "valid_until",
                    }
                    _prefix, _venue, instrument = market_id.split(":", 2)
                    base_asset, quote_asset = instrument.split("/", 1)
                    decimal_fields = (
                        "base_increment", "quote_increment",
                        "min_base_quantity", "min_quote_notional",
                    )
                    if (
                        set(parsed) != expected_fields
                        or parsed.get("schema") != contract["content_schema"]
                        or parsed.get("market_id") != market_id
                        or parsed.get("base_asset") != base_asset
                        or parsed.get("quote_asset") != quote_asset
                        or any(
                            type(parsed.get(field)) is not int
                            or not 0 <= parsed[field] <= 255
                            for field in (
                                "base_unit_decimals", "quote_unit_decimals"
                            )
                        )
                    ):
                        raise ValueError("invalid CEX market rules")
                else:
                    expected_fields = {
                        "schema", "quote_asset", "usd_per_quote",
                        "observed_at", "valid_until", "source",
                    }
                    decimal_fields = ("usd_per_quote",)
                    _prefix, _venue, instrument = market_id.split(":", 2)
                    quote_asset = instrument.split("/", 1)[1]
                    if (
                        set(parsed) != expected_fields
                        or parsed.get("schema") != contract["content_schema"]
                        or parsed.get("quote_asset") != quote_asset
                        or not isinstance(parsed.get("source"), str)
                        or not parsed["source"]
                        or parsed["source"] != parsed["source"].strip()
                    ):
                        raise ValueError("invalid CEX USD conversion")
                for field in decimal_fields:
                    value = parsed.get(field)
                    if not isinstance(value, str) or not value or value != value.strip():
                        raise ValueError("invalid typed decimal")
                    number = Decimal(value)
                    canonical = format(number, "f")
                    if "." in canonical:
                        canonical = canonical.rstrip("0").rstrip(".")
                    if (
                        not number.is_finite()
                        or number < 0
                        or (number.is_zero() and number.is_signed())
                        or (
                            number == 0
                            and field not in {
                                "min_base_quantity", "min_quote_notional"
                            }
                        )
                        or canonical != value
                    ):
                        raise ValueError("invalid typed decimal")
                observed = _utc_datetime(
                    parsed.get("observed_at"), field="typed observed_at"
                )
                valid_until = _utc_datetime(
                    parsed.get("valid_until"), field="typed valid_until"
                )
                if valid_until <= observed:
                    raise ValueError("invalid typed validity window")
                if role == "cex_market_rules":
                    MarketRules(
                        market_id=parsed["market_id"],
                        base_asset=parsed["base_asset"],
                        quote_asset=parsed["quote_asset"],
                        base_unit_decimals=parsed["base_unit_decimals"],
                        quote_unit_decimals=parsed["quote_unit_decimals"],
                        base_increment=Decimal(parsed["base_increment"]),
                        quote_increment=Decimal(parsed["quote_increment"]),
                        min_base_quantity=Decimal(parsed["min_base_quantity"]),
                        min_quote_notional=Decimal(parsed["min_quote_notional"]),
                        observed_at=parsed["observed_at"],
                        valid_until=parsed["valid_until"],
                        source_record_sha256=hashlib.sha256(payload).hexdigest(),
                    )
                logical_generation = hashlib.sha256(payload).hexdigest()
            except (
                KeyError, TypeError, ValueError, InvalidOperation,
                UnicodeDecodeError, json.JSONDecodeError,
            ) as error:
                raise ValueError("typed-source worker inventory is invalid") from error
        elif role in {"dex_market_rules", "dex_usd_conversion"}:
            try:
                parsed = json.loads(payload.decode("utf-8"))
                if (
                    not isinstance(parsed, Mapping)
                    or payload != _typed_json_bytes(parsed)
                ):
                    raise ValueError("invalid typed JSON")
                identity = _canonical_leg_identity({
                    "market_id": market_id,
                    "market_type": "dex",
                })
                target_asset = identity["token_symbol"]
                if role == "dex_market_rules":
                    if (
                        set(parsed) != _DEX_MARKET_RULES_SOURCE_FIELDS
                        or parsed.get("schema") != contract["content_schema"]
                        or parsed.get("market_id") != market_id
                        or parsed.get("base_asset") != target_asset
                        or _TYPED_ASSET.fullmatch(
                            str(parsed.get("quote_asset") or "")
                        ) is None
                        or parsed.get("quote_asset") == target_asset
                        or _TYPED_EVM_ADDRESS.fullmatch(
                            str(parsed.get("base_token_address") or "")
                        ) is None
                        or parsed.get("base_token_address")
                        == _ZERO_EVM_ADDRESS
                        or _TYPED_EVM_ADDRESS.fullmatch(
                            str(parsed.get("quote_token_address") or "")
                        ) is None
                        or parsed.get("quote_token_address")
                        == _ZERO_EVM_ADDRESS
                        or parsed.get("base_token_address")
                        == parsed.get("quote_token_address")
                        or parsed.get("increment_source")
                        != "fixed_block_token_decimals"
                        or parsed.get("minimum_source")
                        != "dex_protocol_no_additional_order_minimum"
                        or parsed.get("min_base_quantity") != "0"
                        or parsed.get("min_quote_notional") != "0"
                        or _TYPED_SHA256.fullmatch(
                            str(parsed.get("raw_response_sha256") or "")
                        ) is None
                    ):
                        raise ValueError("invalid DEX market rules")
                    base_decimals = parsed.get("base_unit_decimals")
                    quote_decimals = parsed.get("quote_unit_decimals")
                    if (
                        parsed.get("base_increment")
                        != _typed_unit_increment(base_decimals)
                        or parsed.get("quote_increment")
                        != _typed_unit_increment(quote_decimals)
                    ):
                        raise ValueError("invalid DEX market increments")
                    _typed_exact_window(
                        parsed.get("observed_at"),
                        parsed.get("valid_until"),
                        duration_seconds=int(
                            MAX_DEX_QUANTITY_STATE_AGE_SECONDS
                        ),
                    )
                    MarketRules(
                        market_id=market_id,
                        base_asset=parsed["base_asset"],
                        quote_asset=parsed["quote_asset"],
                        base_unit_decimals=base_decimals,
                        quote_unit_decimals=quote_decimals,
                        base_increment=Decimal(parsed["base_increment"]),
                        quote_increment=Decimal(parsed["quote_increment"]),
                        min_base_quantity=Decimal(0),
                        min_quote_notional=Decimal(0),
                        observed_at=parsed["observed_at"],
                        valid_until=parsed["valid_until"],
                        source_record_sha256=hashlib.sha256(payload).hexdigest(),
                    )
                else:
                    if (
                        set(parsed) != _DEX_USD_CONVERSION_SOURCE_FIELDS
                        or parsed.get("schema") != contract["content_schema"]
                        or parsed.get("market_id") != market_id
                        or parsed.get("target_asset") != target_asset
                        or _TYPED_ASSET.fullmatch(
                            str(parsed.get("quote_asset") or "")
                        ) is None
                        or parsed.get("quote_asset") == target_asset
                        or _TYPED_EVM_ADDRESS.fullmatch(
                            str(parsed.get("target_token_address") or "")
                        ) is None
                        or parsed.get("target_token_address")
                        == _ZERO_EVM_ADDRESS
                        or _TYPED_EVM_ADDRESS.fullmatch(
                            str(parsed.get("quote_token_address") or "")
                        ) is None
                        or parsed.get("quote_token_address")
                        == _ZERO_EVM_ADDRESS
                        or parsed.get("target_token_address")
                        == parsed.get("quote_token_address")
                        or parsed.get("value_status") != "measured"
                        or not isinstance(parsed.get("source"), str)
                        or not parsed["source"]
                        or parsed["source"] != parsed["source"].strip()
                        or not isinstance(parsed.get("source_snapshot_id"), str)
                        or not parsed["source_snapshot_id"]
                        or parsed["source_snapshot_id"]
                        != parsed["source_snapshot_id"].strip()
                        or _TYPED_SHA256.fullmatch(str(
                            parsed.get("source_raw_response_sha256") or ""
                        )) is None
                        or _TYPED_SHA256.fullmatch(str(
                            parsed.get("state_raw_response_sha256") or ""
                        )) is None
                    ):
                        raise ValueError("invalid DEX USD conversion")
                    rate = parsed.get("usd_per_quote")
                    if not isinstance(rate, str) or not rate or rate != rate.strip():
                        raise ValueError("invalid DEX USD conversion")
                    number = Decimal(rate)
                    canonical = format(number, "f")
                    if "." in canonical:
                        canonical = canonical.rstrip("0").rstrip(".")
                    if not number.is_finite() or number <= 0 or canonical != rate:
                        raise ValueError("invalid DEX USD conversion")
                    observed, _valid = _typed_exact_window(
                        parsed.get("observed_at"),
                        parsed.get("valid_until"),
                        duration_seconds=USD_PRICE_SKEW_MAX_SECONDS,
                    )
                    state_observed = _utc_datetime(
                        parsed.get("state_observed_at"),
                        field="typed state_observed_at",
                    )
                    if abs(observed - state_observed) > timedelta(
                        seconds=USD_PRICE_SKEW_MAX_SECONDS
                    ):
                        raise ValueError("invalid DEX USD conversion timing")
                logical_generation = hashlib.sha256(payload).hexdigest()
                parsed_by_role[role] = parsed
            except (
                KeyError, TypeError, ValueError, InvalidOperation,
                UnicodeDecodeError, json.JSONDecodeError,
            ) as error:
                raise ValueError("typed-source worker inventory is invalid") from error
        else:
            logical_generation = hashlib.sha256(payload).hexdigest()
        members.append({
            "market_id": market_id,
            "role": role,
            "payload": payload,
            "logical_generation": logical_generation,
            "adapter_id": contract["adapter_id"],
            "content_schema": contract["content_schema"],
        })
    rules = parsed_by_role.get("dex_market_rules")
    conversion = parsed_by_role.get("dex_usd_conversion")
    pool_state = parsed_by_role.get("dex_pool_state")
    try:
        if pool_state is not None:
            _validate_dex_pool_state_binding(
                pool_state,
                trusted_leg,
                collector_row,
                accepted_raw_sha256,
            )
        rules_binding = (
            _trusted_dex_directional_binding(
                trusted_leg,
                collector_row,
                accepted_raw_sha256,
                require_usd_lineage=False,
            )
            if rules is not None
            else None
        )
        conversion_binding = (
            _trusted_dex_directional_binding(
                trusted_leg,
                collector_row,
                accepted_raw_sha256,
                require_usd_lineage=True,
            )
            if conversion is not None
            else None
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("typed-source worker inventory is invalid") from error
    if rules is not None and rules_binding is not None:
        if any(
            rules[field] != expected
            for field, expected in (
                ("market_id", rules_binding["market_id"]),
                ("base_asset", rules_binding["target_asset"]),
                ("quote_asset", rules_binding["quote_asset"]),
                (
                    "base_token_address",
                    rules_binding["target_token_address"],
                ),
                (
                    "quote_token_address",
                    rules_binding["quote_token_address"],
                ),
                (
                    "base_unit_decimals",
                    rules_binding["target_unit_decimals"],
                ),
                (
                    "quote_unit_decimals",
                    rules_binding["quote_unit_decimals"],
                ),
                (
                    "raw_response_sha256",
                    rules_binding["state_raw_response_sha256"],
                ),
            )
        ) or _utc_datetime(
            rules["observed_at"], field="typed rules observed_at"
        ) != _utc_datetime(
            rules_binding["state_observed_at"],
            field="typed collector state_observed_at",
        ):
            raise ValueError("typed-source worker inventory is invalid")
    if conversion is not None and conversion_binding is not None:
        if any(
            conversion[field] != expected
            for field, expected in (
                ("market_id", conversion_binding["market_id"]),
                ("target_asset", conversion_binding["target_asset"]),
                ("quote_asset", conversion_binding["quote_asset"]),
                (
                    "target_token_address",
                    conversion_binding["target_token_address"],
                ),
                (
                    "quote_token_address",
                    conversion_binding["quote_token_address"],
                ),
                ("usd_per_quote", conversion_binding["usd_per_quote"]),
                ("source", conversion_binding["source"]),
                (
                    "source_snapshot_id",
                    conversion_binding["source_snapshot_id"],
                ),
                (
                    "source_raw_response_sha256",
                    conversion_binding["source_raw_response_sha256"],
                ),
                (
                    "state_raw_response_sha256",
                    conversion_binding["state_raw_response_sha256"],
                ),
            )
        ) or _utc_datetime(
            conversion["observed_at"], field="typed conversion observed_at"
        ) != _utc_datetime(
            conversion_binding["conversion_observed_at"],
            field="typed collector conversion_observed_at",
        ) or _utc_datetime(
            conversion["state_observed_at"],
            field="typed conversion state_observed_at",
        ) != _utc_datetime(
            conversion_binding["state_observed_at"],
            field="typed collector state_observed_at",
        ):
            raise ValueError("typed-source worker inventory is invalid")
    if pool_state is not None:
        identity = _canonical_leg_identity({
            "market_id": market_id,
            "market_type": "dex",
        })
        if any(
            actual != identity[field]
            for field, actual in (
                ("chain", pool_state.chain),
                ("dex", pool_state.dex),
                ("pool_address", pool_state.pool_address),
            )
        ):
            raise ValueError("typed-source worker inventory is invalid")

        def pool_direction(
            target_address: str,
            quote_address: str,
        ) -> Optional[Tuple[int, int]]:
            if (
                target_address == pool_state.token0_address
                and quote_address == pool_state.token1_address
            ):
                return pool_state.token0_decimals, pool_state.token1_decimals
            if (
                target_address == pool_state.token1_address
                and quote_address == pool_state.token0_address
            ):
                return pool_state.token1_decimals, pool_state.token0_decimals
            return None

        if rules is not None:
            rule_decimals = pool_direction(
                rules["base_token_address"], rules["quote_token_address"]
            )
            if (
                rule_decimals is None
                or rule_decimals != (
                    rules["base_unit_decimals"], rules["quote_unit_decimals"]
                )
                or _utc_datetime(
                    rules["observed_at"], field="typed observed_at"
                ) != _utc_datetime(
                    pool_state.observed_at, field="typed pool observed_at"
                )
                or rules["raw_response_sha256"]
                != pool_state.raw_response_sha256
            ):
                raise ValueError("typed-source worker inventory is invalid")
        if conversion is not None:
            if (
                pool_direction(
                    conversion["target_token_address"],
                    conversion["quote_token_address"],
                ) is None
                or _utc_datetime(
                    conversion["state_observed_at"],
                    field="typed state_observed_at",
                ) != _utc_datetime(
                    pool_state.observed_at, field="typed pool observed_at"
                )
                or conversion["state_raw_response_sha256"]
                != pool_state.raw_response_sha256
            ):
                raise ValueError("typed-source worker inventory is invalid")
    if rules is not None and conversion is not None:
        if any(
            rules[left] != conversion[right]
            for left, right in (
                ("market_id", "market_id"),
                ("base_asset", "target_asset"),
                ("base_token_address", "target_token_address"),
                ("quote_asset", "quote_asset"),
                ("quote_token_address", "quote_token_address"),
                ("observed_at", "state_observed_at"),
                ("raw_response_sha256", "state_raw_response_sha256"),
            )
        ):
            raise ValueError("typed-source worker inventory is invalid")
    members.sort(key=lambda item: item["role"])
    return tuple(members)


def _collector_identity_matches(
    requested_market_id: str,
    market_type: str,
    requested_leg: Mapping[str, Any],
    row: Mapping[str, Any],
) -> bool:
    supplied = row.get("market_id")
    if supplied not in (None, "", requested_market_id):
        return False
    if market_type == "cex":
        _prefix, expected_exchange, expected_symbol = requested_market_id.split(":", 2)
        expected = {
            "market_type": "cex",
            "exchange": expected_exchange,
            "cex_symbol": expected_symbol,
            "token_symbol": expected_symbol.split("/", 1)[0],
        }
        identity_fields = ("market_type", "exchange", "cex_symbol", "token_symbol")
    else:
        _prefix, expected_chain, expected_dex, expected_pool, expected_token = (
            requested_market_id.split(":", 4)
        )
        expected = {
            "market_type": "dex",
            "chain": expected_chain,
            "dex": expected_dex,
            "pool_address": expected_pool,
            "token_symbol": expected_token,
        }
        identity_fields = (
            "market_type", "chain", "dex", "pool_address", "token_symbol"
        )
    for key in identity_fields:
        if requested_leg.get(key) not in (None, ""):
            expected[key] = requested_leg[key]
        supplied_value = row.get(key)
        if supplied_value not in (None, ""):
            if key == "pool_address":
                if (
                    not isinstance(supplied_value, str)
                    or supplied_value != expected[key]
                ):
                    return False
            elif _normal_identity_value(
                market_type, key, supplied_value
            ) != _normal_identity_value(market_type, key, expected[key]):
                return False
    try:
        if market_type == "cex" and all(
            row.get(key) not in (None, "") for key in ("exchange", "cex_symbol")
        ):
            return cex_market_id(dict(row)) == requested_market_id
        if market_type == "dex" and all(
            row.get(key) not in (None, "")
            for key in ("chain", "dex", "pool_address", "token_symbol")
        ):
            return dex_market_id(dict(row)) == requested_market_id
    except (KeyError, TypeError, ValueError):
        return False
    return True


_DROP_PROJECTION = object()


def _sensitive_projection_key(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", value.lower())
    if normalized == "path" or normalized.endswith("path"):
        return True
    if normalized in {
        "auth",
        "authorization",
        "bearer",
        "credential",
        "error",
        "exception",
        "password",
        "privatekey",
        "rawpath",
        "secret",
        "signature",
        "token",
        "traceback",
    }:
        return True
    return any(
        marker in normalized
        for marker in (
            "accesstoken",
            "apikey",
            "authorization",
            "credential",
            "password",
            "refreshtoken",
            "secret",
            "signature",
        )
    )


_WINDOWS_DRIVE_PATH = re.compile(r"^[A-Za-z]:[\\/]", flags=re.ASCII)


def _unsafe_projection_string(value: str) -> bool:
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return True
    if value == "~" or re.match(r"^~[^/\\]*[/\\]", value):
        return True
    if value.startswith(("./", "../", ".\\", "..\\")):
        return True
    if any(segment in {".", ".."} for segment in re.split(r"[/\\]", value)):
        return True
    return (
        os.path.isabs(value)
        or value.startswith("\\")
        or _WINDOWS_DRIVE_PATH.match(value) is not None
        or value.lower().startswith("file:")
    )


def _safe_url_projection(value: str) -> Any:
    try:
        value.encode("utf-8")
        parsed = urlsplit(value)
    except (UnicodeEncodeError, ValueError):
        return _DROP_PROJECTION
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        if not parsed.netloc:
            return _DROP_PROJECTION
    elif parsed.scheme and (
        parsed.netloc
        or "://" in value
        or "@" in parsed.path
        or bool(parsed.query)
        or bool(parsed.fragment)
    ):
        return _DROP_PROJECTION
    if scheme not in {"http", "https"}:
        return value
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return _DROP_PROJECTION
    if not hostname:
        return _DROP_PROJECTION
    safe_host = "[{}]".format(hostname) if ":" in hostname else hostname
    if port is not None:
        safe_host = "{}:{}".format(safe_host, port)
    return urlunsplit(
        (scheme, safe_host, parsed.path, "", "")
    )


def _safe_source_endpoint_projection(value: Any) -> Any:
    """Retain only a canonical public origin for endpoint lineage."""
    if not isinstance(value, str):
        return _DROP_PROJECTION
    try:
        value.encode("utf-8")
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except (UnicodeEncodeError, ValueError):
        return _DROP_PROJECTION
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not hostname:
        return _DROP_PROJECTION
    safe_host = "[{}]".format(hostname) if ":" in hostname else hostname
    if port is not None:
        safe_host = "{}:{}".format(safe_host, port)
    return urlunsplit((scheme, safe_host, "", "", ""))


def _safe_projection_value(
    value: Any,
    *,
    seen: Set[int],
    depth: int,
) -> Any:
    if depth > 32:
        return _DROP_PROJECTION
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _DROP_PROJECTION
    if isinstance(value, str):
        if _unsafe_projection_string(value):
            return _DROP_PROJECTION
        return _safe_url_projection(value)
    if isinstance(value, (PurePath, BaseException, bytes, bytearray)):
        return _DROP_PROJECTION
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in seen:
            return _DROP_PROJECTION
        seen.add(identity)
        projected = {}
        try:
            items = list(value.items())
        except Exception:
            seen.remove(identity)
            return _DROP_PROJECTION
        for key, nested in items:
            if not isinstance(key, str) or _sensitive_projection_key(key):
                continue
            safe_key = _safe_url_projection(key)
            if safe_key is _DROP_PROJECTION or safe_key != key:
                continue
            safe_nested = (
                _safe_source_endpoint_projection(nested)
                if key.endswith("source_endpoint")
                else _safe_projection_value(
                    nested,
                    seen=seen,
                    depth=depth + 1,
                )
            )
            if safe_nested is not _DROP_PROJECTION:
                projected[key] = safe_nested
        seen.remove(identity)
        return projected
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in seen:
            return _DROP_PROJECTION
        seen.add(identity)
        projected_items = []
        for nested in value:
            safe_nested = _safe_projection_value(
                nested,
                seen=seen,
                depth=depth + 1,
            )
            if safe_nested is not _DROP_PROJECTION:
                projected_items.append(safe_nested)
        seen.remove(identity)
        return projected_items
    return _DROP_PROJECTION


def _projection_contains_unsafe_evidence(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int)):
        return False
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, str):
        if _unsafe_projection_string(value):
            return True
        sanitized = _safe_url_projection(value)
        return sanitized is _DROP_PROJECTION or sanitized != value
    if isinstance(value, list):
        return any(_projection_contains_unsafe_evidence(item) for item in value)
    if isinstance(value, Mapping):
        return any(
            not isinstance(key, str)
            or _sensitive_projection_key(key)
            or _projection_contains_unsafe_evidence(nested)
            for key, nested in value.items()
        )
    return True


def _safe_leg_projection(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Recursively retain only JSON-safe facts with no secret-bearing fields."""
    projected = _safe_projection_value(row, seen=set(), depth=0)
    if not isinstance(projected, dict):
        return {}
    try:
        json.dumps(
            projected,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, UnicodeEncodeError):
        return {}
    if _projection_contains_unsafe_evidence(projected):
        return {}
    return projected


def _final_route_leg_projection(
    requested_leg: Mapping[str, Any],
    collector_row: Mapping[str, Any],
    *,
    market_id: str,
) -> Dict[str, Any]:
    """Build the one deterministic public leg used by authority and cohort."""
    market_type = _market_type(requested_leg)
    row = {
        **dict(collector_row),
        "leg_id": market_id,
        "market_id": market_id,
        "market_type": market_type,
        **{
            key: requested_leg[key]
            for key in (
                "execution_adapter_supported", "execution_adapter_status",
                "target_token_address", "target_token_side",
            )
            if key in requested_leg
        },
    }
    context = requested_leg.get("collector_context")
    if market_type == "dex" and isinstance(context, Mapping):
        row.update({
            "collector_context": dict(context),
            "usd_price_source_snapshot_id": context["snapshot_id"],
            "usd_price_observed_at": context["observed_at"],
            "usd_price_source": context["source"],
            "usd_price_source_endpoint": context["source_endpoint"],
            "usd_price_raw_response_sha256": context[
                "raw_response_sha256"
            ],
        })
        if (
            context.get("status") != "observed"
            or collector_row.get("available") is False
        ):
            row.update({
                "available": False,
                "token0_price_usd": None,
                "token1_price_usd": None,
            })
    projected = _safe_leg_projection(row)
    if (
        not projected
        or projected.get("market_id") != market_id
        or projected.get("market_type") != market_type
    ):
        raise ValueError("final route leg projection is invalid")
    return projected


_SNAPSHOT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def _run_id(value: Optional[str], wall_time: datetime) -> str:
    if value is not None:
        if not _SNAPSHOT_ID.fullmatch(value):
            raise ValueError("snapshot_id is invalid")
        return value
    prefix = wall_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return "{}-{}".format(prefix, uuid.uuid4().hex)


class _UnsafeRawEvidence(ValueError):
    pass


def _canonical_raw_path(value: Path) -> Path:
    absolute = Path(os.path.abspath(os.fspath(value)))
    parts = absolute.parts
    if len(parts) > 1 and parts[1] in {"tmp", "var"}:
        alias = Path("/") / parts[1]
        expected = Path("/private") / parts[1]
        if alias.is_symlink() and Path(os.path.realpath(str(alias))) == expected:
            return expected.joinpath(*parts[2:])
    return absolute


def _reject_symlink_ancestry(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            metadata = os.lstat(str(current))
        except FileNotFoundError:
            break
        except OSError as error:
            raise ValueError("raw_root ancestry is invalid") from error
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError("raw_root ancestry must not contain a symlink")
        if current != path and not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("raw_root ancestry is invalid")


def _directory_identity(path: Path) -> Tuple[int, int]:
    try:
        metadata = os.lstat(str(path))
    except OSError as error:
        raise _UnsafeRawEvidence("raw evidence directory is unavailable") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise _UnsafeRawEvidence("raw evidence path is not a real directory")
    return metadata.st_dev, metadata.st_ino


def _require_directory_identity(
    path: Path, expected: Tuple[int, int]
) -> None:
    if _directory_identity(path) != expected:
        raise _UnsafeRawEvidence("raw evidence directory identity changed")


def _directory_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


def _descriptor_directory_identity(
    descriptor: int,
    expected: Tuple[int, int],
) -> None:
    try:
        metadata = os.fstat(descriptor)
    except OSError as error:
        raise _UnsafeRawEvidence(
            "raw evidence directory descriptor is unavailable"
        ) from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != expected
    ):
        raise _UnsafeRawEvidence(
            "raw evidence directory descriptor identity changed"
        )


def _directory_entry_metadata(
    parent_descriptor: int,
    name: str,
) -> Any:
    try:
        return os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    except OSError as error:
        raise _UnsafeRawEvidence(
            "raw evidence directory entry is unavailable"
        ) from error


def _directory_entry_identity(
    parent_descriptor: int,
    name: str,
) -> Tuple[int, int]:
    metadata = _directory_entry_metadata(parent_descriptor, name)
    if metadata is None:
        raise _UnsafeRawEvidence(
            "raw evidence directory entry is unavailable"
        )
    if not stat.S_ISDIR(metadata.st_mode):
        raise _UnsafeRawEvidence(
            "raw evidence directory entry is not a real directory"
        )
    return metadata.st_dev, metadata.st_ino


def _open_directory_entry(
    parent_descriptor: int,
    name: str,
    expected: Tuple[int, int],
) -> int:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise _UnsafeRawEvidence("raw evidence directory entry is invalid")
    if _directory_entry_identity(parent_descriptor, name) != expected:
        raise _UnsafeRawEvidence(
            "raw evidence directory entry identity changed"
        )
    try:
        descriptor = os.open(
            name,
            _directory_open_flags(),
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise _UnsafeRawEvidence(
            "raw evidence directory entry could not be opened safely"
        ) from error
    try:
        _descriptor_directory_identity(descriptor, expected)
        if _directory_entry_identity(parent_descriptor, name) != expected:
            raise _UnsafeRawEvidence(
                "raw evidence directory entry identity changed"
            )
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_raw_run_descriptors(
    staging: Path,
    accepted: Path,
    guards: Mapping[str, Tuple[int, int]],
) -> Dict[str, int]:
    _require_raw_run_guards(staging, accepted, guards)
    run_dir = staging.parent
    try:
        run_descriptor = os.open(str(run_dir), _directory_open_flags())
    except OSError as error:
        raise _UnsafeRawEvidence(
            "raw evidence run directory could not be opened safely"
        ) from error
    descriptors = {"run": run_descriptor}
    try:
        _descriptor_directory_identity(run_descriptor, guards["run"])
        descriptors["staging"] = _open_directory_entry(
            run_descriptor,
            "staging",
            guards["staging"],
        )
        descriptors["accepted"] = _open_directory_entry(
            run_descriptor,
            "accepted",
            guards["accepted"],
        )
        _require_raw_run_guards(staging, accepted, guards)
        return descriptors
    except BaseException:
        for descriptor in reversed(list(descriptors.values())):
            os.close(descriptor)
        raise


def _close_raw_run_descriptors(descriptors: Mapping[str, int]) -> None:
    for key in ("accepted", "staging", "run"):
        descriptor = descriptors.get(key)
        if descriptor is not None:
            os.close(descriptor)


def _atomic_directory_entry_operation(
    source_name: str,
    destination_name: str,
    *,
    source_directory_fd: int,
    destination_directory_fd: int,
    darwin_flags: int,
    linux_flags: int,
    unsupported_message: str,
    destination_exists_message: Optional[str] = None,
) -> None:
    for name in (source_name, destination_name):
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise _UnsafeRawEvidence("raw evidence directory entry is invalid")
    for descriptor in (source_directory_fd, destination_directory_fd):
        try:
            metadata = os.fstat(descriptor)
        except OSError as error:
            raise _UnsafeRawEvidence(
                "raw evidence directory descriptor is unavailable"
            ) from error
        if not stat.S_ISDIR(metadata.st_mode):
            raise _UnsafeRawEvidence(
                "raw evidence directory descriptor is invalid"
            )

    library = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source_name)
    destination_bytes = os.fsencode(destination_name)
    if sys.platform == "darwin":
        try:
            operation = library.renameatx_np
        except AttributeError as error:
            raise _UnsafeRawEvidence(unsupported_message) from error
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        operation.restype = ctypes.c_int
        arguments = (
            source_directory_fd,
            source_bytes,
            destination_directory_fd,
            destination_bytes,
            darwin_flags,
        )
    elif sys.platform.startswith("linux"):
        try:
            operation = library.renameat2
        except AttributeError as error:
            raise _UnsafeRawEvidence(unsupported_message) from error
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        operation.restype = ctypes.c_int
        arguments = (
            source_directory_fd,
            source_bytes,
            destination_directory_fd,
            destination_bytes,
            linux_flags,
        )
    else:
        raise _UnsafeRawEvidence(unsupported_message)

    ctypes.set_errno(0)
    if operation(*arguments) == 0:
        return
    error_number = ctypes.get_errno()
    if (
        destination_exists_message is not None
        and error_number in {errno.EEXIST, errno.ENOTEMPTY}
    ):
        raise _UnsafeRawEvidence(destination_exists_message)
    if error_number in {errno.ENOSYS, errno.ENOTSUP}:
        raise _UnsafeRawEvidence(unsupported_message)
    raise OSError(error_number, os.strerror(error_number))


def _rename_directory_entry(
    source_name: str,
    destination_name: str,
    *,
    source_directory_fd: int,
    destination_directory_fd: int,
) -> None:
    """Atomically rename a directory entry without replacing a destination."""
    _atomic_directory_entry_operation(
        source_name,
        destination_name,
        source_directory_fd=source_directory_fd,
        destination_directory_fd=destination_directory_fd,
        darwin_flags=0x00000004,  # Darwin RENAME_EXCL
        linux_flags=1,  # Linux RENAME_NOREPLACE
        unsupported_message="atomic raw evidence promotion is unsupported",
        destination_exists_message="raw evidence destination already exists",
    )


def _exchange_directory_entries(
    source_name: str,
    destination_name: str,
    *,
    source_directory_fd: int,
    destination_directory_fd: int,
) -> None:
    """Atomically exchange two entries without resolving either parent path."""
    _atomic_directory_entry_operation(
        source_name,
        destination_name,
        source_directory_fd=source_directory_fd,
        destination_directory_fd=destination_directory_fd,
        darwin_flags=0x00000002,  # Darwin RENAME_SWAP
        linux_flags=2,  # Linux RENAME_EXCHANGE
        unsupported_message="atomic raw evidence rollback is unsupported",
    )


def _raw_run_directory(
    raw_root: Path, run_id: str
) -> Tuple[Path, Path, Dict[str, Tuple[int, int]]]:
    raw_root = _canonical_raw_path(raw_root)
    _reject_symlink_ancestry(raw_root)
    raw_root.mkdir(parents=True, exist_ok=True)
    _reject_symlink_ancestry(raw_root)
    root_identity = _directory_identity(raw_root)
    run_dir = raw_root / run_id
    run_dir.mkdir(exist_ok=False)
    staging = run_dir / "staging"
    accepted = run_dir / "accepted"
    staging.mkdir()
    accepted.mkdir()
    guards = {
        "root": root_identity,
        "run": _directory_identity(run_dir),
        "staging": _directory_identity(staging),
        "accepted": _directory_identity(accepted),
    }
    return staging, accepted, guards


def _require_raw_run_guards(
    staging: Path,
    accepted: Path,
    guards: Mapping[str, Tuple[int, int]],
) -> None:
    root = staging.parent.parent
    run_dir = staging.parent
    if accepted.parent != run_dir or accepted.name != "accepted":
        raise _UnsafeRawEvidence("raw evidence accepted path is invalid")
    if staging.name != "staging":
        raise _UnsafeRawEvidence("raw evidence staging path is invalid")
    try:
        _reject_symlink_ancestry(root)
    except ValueError as error:
        raise _UnsafeRawEvidence("raw evidence ancestry changed") from error
    _require_directory_identity(root, guards["root"])
    _require_directory_identity(run_dir, guards["run"])
    _require_directory_identity(staging, guards["staging"])
    _require_directory_identity(accepted, guards["accepted"])


def _read_regular_file(path: Path) -> Tuple[bytes, Tuple[int, int]]:
    try:
        before = os.lstat(str(path))
    except FileNotFoundError as error:
        raise FileNotFoundError("raw evidence is missing") from error
    except OSError as error:
        raise _UnsafeRawEvidence("raw evidence path is unavailable") from error
    if not stat.S_ISREG(before.st_mode):
        raise _UnsafeRawEvidence("raw evidence is not a real regular file")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(str(path), flags)
    except OSError as error:
        raise _UnsafeRawEvidence("raw evidence could not be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != identity
        ):
            raise _UnsafeRawEvidence("raw evidence identity changed")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read()
        after = os.lstat(str(path))
        if (
            not stat.S_ISREG(after.st_mode)
            or (after.st_dev, after.st_ino) != identity
        ):
            raise _UnsafeRawEvidence("raw evidence identity changed")
        return payload, identity
    finally:
        os.close(descriptor)


def _read_regular_file_at(
    directory_descriptor: int,
    name: str,
    *,
    max_bytes: Optional[int] = None,
) -> Tuple[bytes, Tuple[int, int]]:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise _UnsafeRawEvidence("raw evidence filename is invalid")
    try:
        before = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError as error:
        raise FileNotFoundError("raw evidence is missing") from error
    except OSError as error:
        raise _UnsafeRawEvidence("raw evidence path is unavailable") from error
    if (
        not stat.S_ISREG(before.st_mode)
        or (
            max_bytes is not None
            and (
                type(max_bytes) is not int
                or max_bytes <= 0
                or before.st_size > max_bytes
                or before.st_nlink != 1
            )
        )
    ):
        raise _UnsafeRawEvidence("raw evidence is not a real regular file")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(
            name,
            flags,
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        raise _UnsafeRawEvidence(
            "raw evidence could not be opened safely"
        ) from error
    try:
        opened = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != identity
        ):
            raise _UnsafeRawEvidence("raw evidence identity changed")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read()
        if max_bytes is not None and len(payload) > max_bytes:
            raise _UnsafeRawEvidence("raw evidence is too large")
        after = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(after.st_mode)
            or (after.st_dev, after.st_ino) != identity
        ):
            raise _UnsafeRawEvidence("raw evidence identity changed")
        return payload, identity
    except FileNotFoundError as error:
        raise _UnsafeRawEvidence("raw evidence identity changed") from error
    finally:
        os.close(descriptor)


def _quarantine_failed_regular_file_at(
    directory_descriptor: int,
    name: str,
    expected_identity: Tuple[int, int],
) -> None:
    """Detach a failed write without deleting whichever inode owns its name."""
    metadata = _directory_entry_metadata(directory_descriptor, name)
    if metadata is None:
        return
    quarantine_name = ".raw-write-quarantine-{}-{}".format(
        uuid.uuid4().hex, name
    )
    _rename_directory_entry(
        name,
        quarantine_name,
        source_directory_fd=directory_descriptor,
        destination_directory_fd=directory_descriptor,
    )
    moved = _directory_entry_metadata(
        directory_descriptor, quarantine_name
    )
    if moved is None:
        raise _UnsafeRawEvidence(
            "raw evidence write quarantine is unavailable"
        )
    moved_identity = (moved.st_dev, moved.st_ino)
    if _directory_entry_metadata(directory_descriptor, name) is not None:
        raise _UnsafeRawEvidence(
            "raw evidence write canonical name changed during quarantine"
        )
    os.fsync(directory_descriptor)
    if moved_identity != expected_identity:
        raise _UnsafeRawEvidence(
            "raw evidence write quarantine preserved a foreign file"
        )


def _write_regular_file_at(
    directory_descriptor: int,
    name: str,
    payload: bytes,
    *,
    max_bytes: int,
) -> Tuple[Tuple[int, int], str]:
    """Create, fsync, and re-read one bounded no-follow regular file."""
    if (
        not name
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or not isinstance(payload, bytes)
        or type(max_bytes) is not int
        or max_bytes <= 0
        or not 0 < len(payload) <= max_bytes
    ):
        raise _UnsafeRawEvidence("raw evidence file write is invalid")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    descriptor = None
    created = False
    identity = None
    try:
        descriptor = os.open(
            name,
            flags,
            0o600,
            dir_fd=directory_descriptor,
        )
        created = True
        opened = os.fstat(descriptor)
        identity = (opened.st_dev, opened.st_ino)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
            raise _UnsafeRawEvidence(
                "raw evidence file write is not regular"
            )
        view = memoryview(payload)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise _UnsafeRawEvidence("raw evidence file write failed")
            written += count
        os.fsync(descriptor)
        written_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(written_metadata.st_mode)
            or written_metadata.st_nlink != 1
            or written_metadata.st_size != len(payload)
            or (written_metadata.st_dev, written_metadata.st_ino)
            != identity
        ):
            raise _UnsafeRawEvidence("raw evidence file write changed")
    except (OSError, _UnsafeRawEvidence):
        if descriptor is not None:
            os.close(descriptor)
            descriptor = None
        if created and identity is not None:
            _quarantine_failed_regular_file_at(
                directory_descriptor, name, identity
            )
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
    try:
        verified, verified_identity = _read_regular_file_at(
            directory_descriptor,
            name,
            max_bytes=max_bytes,
        )
        if verified_identity != identity or verified != payload:
            raise _UnsafeRawEvidence("raw evidence file write changed")
        os.fsync(directory_descriptor)
    except (OSError, FileNotFoundError, _UnsafeRawEvidence):
        _quarantine_failed_regular_file_at(
            directory_descriptor, name, identity
        )
        raise
    return identity, hashlib.sha256(payload).hexdigest()


def _validated_raw_evidence(
    row: Mapping[str, Any],
    stage_dir: Path,
    stage_identity: Tuple[int, int],
    staging_root: Path,
    accepted_root: Path,
    guards: Mapping[str, Tuple[int, int]],
) -> Tuple[Optional[str], Optional[str], Optional[Tuple[int, int]]]:
    claimed = row.get("raw_response_sha256")
    requires_raw = str(row.get("status") or "") in {"observed", "partial"}
    requires_raw = requires_raw or claimed not in (None, "")
    try:
        _require_raw_run_guards(staging_root, accepted_root, guards)
        if stage_dir.parent != staging_root:
            raise _UnsafeRawEvidence("raw evidence stage escaped staging")
        _require_directory_identity(stage_dir, stage_identity)
    except _UnsafeRawEvidence:
        return "raw_evidence_path_unsafe", None, None
    if not requires_raw:
        return None, None, None
    raw_path = stage_dir / "response.json"
    try:
        payload, response_identity = _read_regular_file(raw_path)
    except FileNotFoundError:
        return "raw_evidence_missing", None, None
    except _UnsafeRawEvidence:
        return "raw_evidence_path_unsafe", None, None
    actual = hashlib.sha256(payload).hexdigest()
    if claimed not in (None, "") and (
        not isinstance(claimed, str)
        or not re.fullmatch(r"[0-9a-f]{64}", claimed)
        or claimed != actual
    ):
        return "raw_evidence_hash_mismatch", None, None
    return None, actual, response_identity


def _post_promotion_failure(
    entry_name: str,
    entry_descriptor: int,
    stage_identity: Tuple[int, int],
    response_identity: Tuple[int, int],
    expected_sha256: str,
    authority_identity: Tuple[int, int],
    authority_sha256: str,
    staging_root: Path,
    accepted_root: Path,
    guards: Mapping[str, Tuple[int, int]],
    descriptors: Mapping[str, int],
) -> Optional[str]:
    try:
        _descriptor_directory_identity(descriptors["run"], guards["run"])
        _descriptor_directory_identity(
            descriptors["staging"], guards["staging"]
        )
        _descriptor_directory_identity(
            descriptors["accepted"], guards["accepted"]
        )
        _descriptor_directory_identity(entry_descriptor, stage_identity)
        if _directory_entry_identity(
            descriptors["accepted"], entry_name
        ) != stage_identity:
            raise _UnsafeRawEvidence("accepted evidence identity changed")
        if sorted(os.listdir(entry_descriptor)) != [
            _ATTACHMENT_AUTHORITY_FILENAME,
            "response.json",
        ]:
            raise _UnsafeRawEvidence(
                "accepted evidence file inventory changed"
            )
        payload, actual_response_identity = _read_regular_file_at(
            entry_descriptor,
            "response.json",
        )
        if actual_response_identity != response_identity:
            raise _UnsafeRawEvidence("accepted response identity changed")
        authority_payload, actual_authority_identity = _read_regular_file_at(
            entry_descriptor,
            _ATTACHMENT_AUTHORITY_FILENAME,
            max_bytes=_ATTACHMENT_AUTHORITY_MAX_BYTES,
        )
        if (
            actual_authority_identity != authority_identity
            or hashlib.sha256(authority_payload).hexdigest()
            != authority_sha256
        ):
            raise _UnsafeRawEvidence(
                "accepted attachment authority changed"
            )
        _require_raw_run_guards(staging_root, accepted_root, guards)
    except (FileNotFoundError, _UnsafeRawEvidence):
        return "raw_evidence_path_unsafe"
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        return "raw_evidence_hash_mismatch"
    return None


def _rollback_promoted_evidence(
    entry_name: str,
    stage_identity: Tuple[int, int],
    descriptors: Mapping[str, int],
) -> bool:
    accepted_metadata = _directory_entry_metadata(
        descriptors["accepted"], entry_name
    )
    staging_metadata = _directory_entry_metadata(
        descriptors["staging"], entry_name
    )

    def is_stage(metadata: Any) -> bool:
        return bool(
            metadata is not None
            and stat.S_ISDIR(metadata.st_mode)
            and (metadata.st_dev, metadata.st_ino) == stage_identity
        )

    if is_stage(staging_metadata):
        if is_stage(accepted_metadata):
            raise _UnsafeRawEvidence(
                "raw evidence rollback has duplicate stage identity"
            )
    elif staging_metadata is None:
        if not is_stage(accepted_metadata):
            raise _UnsafeRawEvidence(
                "accepted evidence is unavailable for rollback"
            )
        _rename_directory_entry(
            entry_name,
            entry_name,
            source_directory_fd=descriptors["accepted"],
            destination_directory_fd=descriptors["staging"],
        )
    else:
        if not is_stage(accepted_metadata):
            raise _UnsafeRawEvidence(
                "accepted evidence is unavailable for rollback"
            )
        _exchange_directory_entries(
            entry_name,
            entry_name,
            source_directory_fd=descriptors["accepted"],
            destination_directory_fd=descriptors["staging"],
        )

    displaced = _directory_entry_metadata(
        descriptors["accepted"], entry_name
    )
    if displaced is not None:
        quarantine_name = ".rejected-{}-{}".format(
            entry_name,
            uuid.uuid4().hex,
        )
        _rename_directory_entry(
            entry_name,
            quarantine_name,
            source_directory_fd=descriptors["accepted"],
            destination_directory_fd=descriptors["staging"],
        )

    return _rollback_state_is_safe(
        entry_name,
        stage_identity,
        descriptors,
    )


def _rollback_state_is_safe(
    entry_name: str,
    stage_identity: Tuple[int, int],
    descriptors: Mapping[str, int],
) -> bool:
    staging_metadata = _directory_entry_metadata(
        descriptors["staging"], entry_name
    )
    accepted_metadata = _directory_entry_metadata(
        descriptors["accepted"], entry_name
    )
    return bool(
        staging_metadata is not None
        and stat.S_ISDIR(staging_metadata.st_mode)
        and (staging_metadata.st_dev, staging_metadata.st_ino) == stage_identity
        and accepted_metadata is None
    )


def _clear_unsafe_accepted_alias(
    descriptors: Mapping[str, int],
    expected_identity: Tuple[int, int],
) -> None:
    """Remove only a swapped symlink at run/accepted; never follow it."""
    try:
        metadata = os.stat(
            "accepted",
            dir_fd=descriptors["run"],
            follow_symlinks=False,
        )
    except OSError:
        return
    if (
        stat.S_ISDIR(metadata.st_mode)
        and (metadata.st_dev, metadata.st_ino) == expected_identity
    ):
        return
    if not stat.S_ISLNK(metadata.st_mode):
        return
    try:
        os.unlink("accepted", dir_fd=descriptors["run"])
    except OSError:
        return


def _validated_fixed_block(
    resolved: Any,
    *,
    chain: str,
    latest_allowed: datetime,
) -> Dict[str, Any]:
    if not isinstance(resolved, Mapping):
        raise ValueError("invalid fixed block")
    number = resolved.get("block_number")
    timestamp = resolved.get("block_timestamp")
    if type(number) is not int or number <= 0:
        raise ValueError("invalid fixed block")
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("invalid fixed block")
    normalized = _canonical_utc(timestamp, field="fixed block timestamp")
    parsed = _utc_datetime(normalized, field="fixed block timestamp")
    if parsed > latest_allowed.astimezone(timezone.utc):
        raise ValueError("invalid fixed block")
    chain_id = resolved.get("chain_id")
    if chain_id is not None:
        expected_chain_id = _CANONICAL_CHAIN_IDS.get(chain)
        if (
            not is_canonical_rpc_quantity(chain_id)
            or expected_chain_id is None
            or chain_id != expected_chain_id
        ):
            raise ValueError("invalid fixed block chain ID")
    header = None
    raw_header = resolved.get("block_header")
    if raw_header is not None:
        try:
            candidate = canonical_route_fixed_block_header(raw_header)
            if (
                int(candidate["number"], 16) == number
                and _utc_datetime(
                    block_timestamp_text(candidate),
                    field="fixed block header timestamp",
                ) == parsed
            ):
                header = candidate
        except (TypeError, ValueError):
            header = None
    return {
        "block_number": number,
        "block_timestamp": normalized,
        "chain_id": chain_id,
        "block_header": header,
    }


def collect_route_cohort(
    universe: Mapping[str, Any],
    *,
    cex_collector: Callable[..., Any] = collect_cex_market_observation,
    dex_collector: Callable[..., Any] = collect_dex_pool_observation,
    deadline_seconds: float = 60,
    max_workers: int = 24,
    cex_workers_per_venue: int = 2,
    dex_workers_per_chain: int = 4,
    target_observed_at: Optional[str] = None,
    deadline: Optional[CollectionDeadline] = None,
    dex_block_resolver: Optional[Callable[..., Mapping[str, Any]]] = None,
    source_generation_reader: Optional[Callable[[], str]] = None,
    expected_source_generation: Optional[str] = None,
    raw_root: Optional[Path] = None,
    snapshot_id: Optional[str] = None,
    executor_factory: Callable[..., Any] = _ForkProcessExecutor,
    child_close_fds: Iterable[int] = (),
    process_evidence_sink: Optional[Dict[str, int]] = None,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> Dict[str, Any]:
    """Collect one fair, bounded cohort; late or incomplete legs stay terminal."""
    if not isinstance(universe, Mapping):
        raise ValueError("route universe is invalid")
    generation = universe.get("candidate_source_generation")
    selected = universe.get("selected_legs")
    routes = universe.get("routes")
    if (not isinstance(generation, str) or not generation or not isinstance(selected, list)
            or not isinstance(routes, list)):
        raise ValueError("route universe is invalid")
    routes = _validated_routes(routes)
    try:
        duration = float(deadline_seconds)
    except (TypeError, ValueError) as error:
        raise ValueError("deadline_seconds must be finite and positive") from error
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("deadline_seconds must be finite and positive")
    if min(max_workers, cex_workers_per_venue, dex_workers_per_chain) < 1:
        raise ValueError("worker limits must be positive")
    legs_by_market: Dict[str, Mapping[str, Any]] = {}
    for leg in selected:
        if not isinstance(leg, Mapping) or not isinstance(leg.get("market_id"), str):
            raise ValueError("route leg identity is invalid")
        market_id = leg["market_id"]
        if market_id in legs_by_market:
            raise ValueError("duplicate route leg")
        _market_type(leg)
        legs_by_market[market_id] = leg
    market_ids = collect_unique_route_legs(routes)
    if set(market_ids) - set(legs_by_market):
        raise ValueError("route references an unselected leg")
    if _has_route_volume_lineage(routes, legs_by_market):
        _validate_route_volume_lineage(routes, legs_by_market)
    has_dex = any(
        _market_type(legs_by_market[item]) == "dex"
        for item in market_ids
    )
    if has_dex and dex_block_resolver is None:
        raise ValueError("DEX fixed block resolver is required")
    if raw_root is None:
        raise ValueError("raw_root is required")
    root = _canonical_raw_path(Path(raw_root))
    _reject_symlink_ancestry(root)
    if root.exists() and not root.is_dir():
        raise ValueError("raw_root must be a directory")
    if executor_factory is _ForkProcessExecutor:
        _require_single_threaded_fork()
    if source_generation_reader is None or not isinstance(
        expected_source_generation, str
    ) or not expected_source_generation:
        raise ValueError(
            "collection input generation reader is required, and expected value is required"
        )
    if source_generation_reader() != expected_source_generation:
        raise ValueError("collection input generation changed")

    wall_start = wall_clock()
    collection_started_at = _canonical_utc(wall_start, field="collection_started_at")
    wall_start_utc = _utc_datetime(
        collection_started_at, field="collection_started_at"
    )
    target = _canonical_utc(
        target_observed_at if target_observed_at is not None else wall_start_utc,
        field="target_observed_at",
    )
    if deadline is None:
        active_deadline = CollectionDeadline.for_duration(duration)
        # The declared duration is part of the logical cohort input.  Do not
        # bind scheduler call overhead into the immutable wall-clock metadata.
        remaining_at_start = duration
    else:
        active_deadline = deadline
        remaining_at_start = active_deadline.remaining_seconds()
    collection_deadline_at = _canonical_utc(
        wall_start_utc + timedelta(seconds=remaining_at_start),
        field="collection_deadline_at",
    )
    wall_deadline_utc = _utc_datetime(
        collection_deadline_at, field="collection_deadline_at"
    )
    run_id = _run_id(snapshot_id, wall_start_utc)
    staging_root, accepted_root, raw_run_guards = _raw_run_directory(
        root, run_id
    )
    terminal_reasons: Dict[str, str] = {}
    expired: Set[str] = set()
    fixed_blocks: Dict[str, Mapping[str, Any]] = {}
    dex_by_chain: Dict[str, List[str]] = {}
    for market_id in market_ids:
        leg = legs_by_market[market_id]
        if _market_type(leg) == "dex":
            dex_by_chain.setdefault(_source_key(leg)[1], []).append(market_id)
    pending_by_source: Dict[Tuple[str, str], List[str]] = {}
    for market_id in market_ids:
        if _market_type(legs_by_market[market_id]) == "cex":
            pending_by_source.setdefault(_source_key(legs_by_market[market_id]), []).append(market_id)
    for chain in sorted(dex_by_chain):
        pending_by_source[("resolver", chain)] = [chain]
    for items in pending_by_source.values():
        items.sort()
    def fair_source_order(
        keys: Iterable[Tuple[str, str]],
    ) -> List[Tuple[str, str]]:
        buckets = {
            kind: sorted(key for key in keys if key[0] == kind)
            for kind in ("cex", "resolver", "dex")
        }
        ordered = []
        while any(buckets.values()):
            for kind in ("cex", "resolver", "dex"):
                if buckets[kind]:
                    ordered.append(buckets[kind].pop(0))
        return ordered

    source_order = fair_source_order(pending_by_source)
    source_index = 0
    active_by_source: Dict[Tuple[str, str], int] = {}
    completed: Dict[str, Mapping[str, Any]] = {}
    completed_typed_payloads: Dict[
        str, Tuple[Mapping[str, Any], ...]
    ] = {}
    completed_stage_dirs: Dict[
        str, Tuple[Path, Tuple[int, int]]
    ] = {}
    futures: Dict[Any, Tuple[str, Tuple[str, str]]] = {}

    def raw_paths(
        market_id: str,
    ) -> Tuple[Path, Path, Tuple[int, int]]:
        _require_raw_run_guards(
            staging_root, accepted_root, raw_run_guards
        )
        name = hashlib.sha256(market_id.encode("utf-8")).hexdigest()
        stage_dir = staging_root / name
        stage_dir.mkdir(exist_ok=False)
        stage_identity = _directory_identity(stage_dir)
        _require_raw_run_guards(
            staging_root, accepted_root, raw_run_guards
        )
        if stage_dir.parent != staging_root:
            raise _UnsafeRawEvidence("raw evidence stage escaped staging")
        return stage_dir / "response.json", stage_dir, stage_identity

    def collect_one(
        market_id: str,
    ) -> Tuple[
        str,
        Optional[Mapping[str, Any]],
        Optional[str],
        Optional[Path],
        Optional[Tuple[int, int]],
        Tuple[Mapping[str, Any], ...],
    ]:
        leg = legs_by_market[market_id]
        kind, source = _source_key(leg)
        stage_dir = None
        stage_identity = None
        try:
            raw_path, stage_dir, stage_identity = raw_paths(market_id)
            active_deadline.require_remaining()
            typed_payloads: List[Mapping[str, Any]] = []
            typed_keyword = (
                {"typed_source_payload_sink": typed_payloads.append}
                if _collector_accepts_typed_sink(
                    cex_collector if kind == "cex" else dex_collector
                )
                else {}
            )
            degraded_usd_keyword = (
                {"allow_degraded_usd_context": True}
                if kind == "dex"
                and _collector_accepts_degraded_usd_context(dex_collector)
                else {}
            )
            if kind == "cex":
                row = _row_from_collector(cex_collector(
                    dict(leg), snapshot_id=run_id, raw_path=raw_path,
                    deadline=active_deadline,
                    **typed_keyword,
                ))
            else:
                block = fixed_blocks[source]
                collector_leg = _strict_dex_collector_input(leg)
                row = _row_from_collector(dex_collector(
                    collector_leg, snapshot_id=run_id, raw_path=raw_path,
                    fixed_block_number=block["block_number"],
                    fixed_block_timestamp=block.get("block_timestamp", ""),
                    fixed_chain_id=block.get("chain_id"),
                    fixed_block_header=block.get("block_header"),
                    deadline=active_deadline,
                    **typed_keyword,
                    **degraded_usd_keyword,
                ))
                if (str(row.get("block_number")) != str(block["block_number"])
                        or str(row.get("block_timestamp") or "") != str(block.get("block_timestamp") or "")):
                    return (
                        market_id,
                        None,
                        "fixed_block_lineage_mismatch",
                        stage_dir,
                        stage_identity,
                        (),
                    )
                row = {
                    **dict(row),
                    "fixed_block_number": str(block["block_number"]),
                    "fixed_block_timestamp": str(block.get("block_timestamp") or ""),
                }
            if not _collector_identity_matches(market_id, kind, leg, row):
                return (
                    market_id,
                    None,
                    "collector_identity_mismatch",
                    stage_dir,
                    stage_identity,
                    (),
                )
            active_deadline.require_remaining()
            if typed_payloads:
                _validated_typed_payload_inventory(
                    trusted_leg=leg,
                    collector_row=row,
                    accepted_raw_sha256=row.get("raw_response_sha256"),
                    values=typed_payloads,
                )
            inventory = tuple(
                {"role": item["role"], "payload": item["payload"]}
                for item in typed_payloads
            )
            return (
                market_id, row, None, stage_dir, stage_identity, inventory
            )
        except _UnsafeRawEvidence:
            return (
                market_id,
                None,
                "raw_evidence_path_unsafe",
                stage_dir,
                stage_identity,
                (),
            )
        except CollectionDeadlineExceeded:
            return (
                market_id,
                None,
                "route_deadline_exceeded",
                stage_dir,
                stage_identity,
                (),
            )
        except Exception:
            return (
                market_id,
                None,
                "collection_failed",
                stage_dir,
                stage_identity,
                (),
            )

    def resolve_one(
        chain: str,
    ) -> Tuple[str, Optional[Mapping[str, Any]], Optional[str]]:
        try:
            active_deadline.require_remaining()
            resolved = dex_block_resolver(chain, deadline=active_deadline)
            normalized = _validated_fixed_block(
                resolved,
                chain=chain,
                latest_allowed=wall_deadline_utc,
            )
            active_deadline.require_remaining()
            return chain, normalized, None
        except CollectionDeadlineExceeded:
            return chain, None, "route_deadline_exceeded"
        except Exception:
            return chain, None, "fixed_block_unavailable"

    close_fds = tuple(child_close_fds)
    if (
        any(type(descriptor) is not int or descriptor < 0 for descriptor in close_fds)
        or len(close_fds) != len(set(close_fds))
    ):
        raise ValueError("child_close_fds must contain unique nonnegative integers")
    if executor_factory is _ForkProcessExecutor:
        executor = executor_factory(
            max_workers=max_workers, child_close_fds=close_fds
        )
    else:
        if close_fds:
            raise ValueError(
                "child_close_fds are supported only by the process executor"
            )
        executor = executor_factory(max_workers=max_workers)

    def source_limit(key: Tuple[str, str]) -> int:
        if key[0] == "cex":
            return cex_workers_per_venue
        if key[0] == "dex":
            return dex_workers_per_chain
        return 1

    def cex_work_outstanding() -> bool:
        return any(
            key[0] == "cex" and (items or active_by_source.get(key, 0))
            for key, items in pending_by_source.items()
        )

    def non_cex_capacity_available() -> bool:
        active_non_cex = sum(
            count
            for key, count in active_by_source.items()
            if key[0] != "cex"
        )
        capacity = max_workers
        if cex_work_outstanding():
            capacity -= 1
        return active_non_cex < capacity

    def submit_fairly() -> None:
        nonlocal source_index
        progressed = True
        while len(futures) < max_workers and progressed:
            progressed = False
            for _unused in range(len(source_order)):
                key = source_order[source_index % len(source_order)]
                source_index += 1
                limit = source_limit(key)
                if (
                    pending_by_source[key]
                    and active_by_source.get(key, 0) < limit
                    and (key[0] == "cex" or non_cex_capacity_available())
                ):
                    item = pending_by_source[key].pop(0)
                    future = executor.submit(
                        resolve_one if key[0] == "resolver" else collect_one,
                        item,
                    )
                    futures[future] = (item, key)
                    active_by_source[key] = active_by_source.get(key, 0) + 1
                    progressed = True
                    break
    try:
        submit_fairly()
        while futures:
            remaining = active_deadline.remaining_seconds()
            if remaining <= 0:
                break
            process_wait = getattr(executor, "wait_for_any", None)
            if callable(process_wait):
                done = process_wait(list(futures), remaining)
            else:
                done, _not_done = wait(
                    list(futures),
                    timeout=remaining,
                    return_when=FIRST_COMPLETED,
                )
            if not done:
                break
            for future in done:
                item, key = futures.pop(future)
                active_by_source[key] -= 1
                if active_deadline.remaining_seconds() <= 0:
                    if key[0] == "resolver":
                        expired.update(dex_by_chain[item])
                    else:
                        expired.add(item)
                    continue
                if key[0] == "resolver":
                    chain, block, reason = future.result()
                    if reason == "route_deadline_exceeded":
                        expired.update(dex_by_chain[chain])
                    elif reason:
                        _terminal_for_chain(
                            legs_by_market, market_ids, chain, reason,
                            terminal_reasons,
                        )
                    elif block is not None:
                        fixed_blocks[chain] = block
                        dex_key = ("dex", chain)
                        pending_by_source[dex_key] = sorted(dex_by_chain[chain])
                        if dex_key not in source_order:
                            source_order.append(dex_key)
                else:
                    (
                        returned_id,
                        row,
                        reason,
                        stage_dir,
                        stage_identity,
                        typed_payloads,
                    ) = future.result()
                    if reason == "route_deadline_exceeded":
                        expired.add(returned_id)
                    elif reason:
                        terminal_reasons[returned_id] = reason
                    elif row is not None:
                        completed[returned_id] = row
                        completed_typed_payloads[returned_id] = typed_payloads
                        if stage_dir is not None and stage_identity is not None:
                            completed_stage_dirs[returned_id] = (
                                stage_dir,
                                stage_identity,
                            )
            submit_fairly()
    finally:
        for future, (item, key) in futures.items():
            future.cancel()
            if key[0] == "resolver":
                expired.update(dex_by_chain[item])
            else:
                expired.add(item)
        for key, items in pending_by_source.items():
            if key[0] == "resolver":
                for chain in items:
                    expired.update(dex_by_chain[chain])
            else:
                expired.update(items)
        executor.shutdown(wait=False)
        evidence = getattr(executor, "process_evidence", None)
        observed_processes = (
            evidence() if callable(evidence) else {
                "collector_process_started_count": 0,
                "collector_process_reaped_count": 0,
                "orphan_process_count": 0,
            }
        )
        if process_evidence_sink is not None:
            process_evidence_sink.clear()
            process_evidence_sink.update(observed_processes)

    if source_generation_reader() != expected_source_generation:
        raise ValueError("collection input generation changed")
    collection_completed_at = _canonical_utc(
        wall_clock(), field="collection_completed_at"
    )
    completed_wall_utc = _utc_datetime(
        collection_completed_at, field="collection_completed_at"
    )
    if completed_wall_utc < wall_start_utc:
        raise ValueError("collection_completed_at is before collection_started_at")
    for chain, block in list(fixed_blocks.items()):
        block_time = _utc_datetime(
            block["block_timestamp"], field="fixed block timestamp"
        )
        if block_time > completed_wall_utc:
            _terminal_for_chain(
                legs_by_market,
                market_ids,
                chain,
                "fixed_block_unavailable",
                terminal_reasons,
            )
            for market_id in dex_by_chain[chain]:
                completed.pop(market_id, None)
                completed_typed_payloads.pop(market_id, None)
                completed_stage_dirs.pop(market_id, None)
            fixed_blocks.pop(chain, None)
    try:
        raw_run_descriptors = _open_raw_run_descriptors(
            staging_root,
            accepted_root,
            raw_run_guards,
        )
    except _UnsafeRawEvidence:
        raw_run_descriptors = None
        for market_id in completed_stage_dirs:
            terminal_reasons[market_id] = "raw_evidence_path_unsafe"
            completed.pop(market_id, None)
            completed_typed_payloads.pop(market_id, None)
    if raw_run_descriptors is not None:
        try:
            for market_id in sorted(completed_stage_dirs):
                stage_dir, stage_identity = completed_stage_dirs[market_id]
                row = completed[market_id]
                (
                    raw_failure,
                    actual_sha256,
                    response_identity,
                ) = _validated_raw_evidence(
                    row,
                    stage_dir,
                    stage_identity,
                    staging_root,
                    accepted_root,
                    raw_run_guards,
                )
                if raw_failure is not None:
                    if raw_failure == "raw_evidence_path_unsafe":
                        _clear_unsafe_accepted_alias(
                            raw_run_descriptors,
                            raw_run_guards["accepted"],
                        )
                    terminal_reasons[market_id] = raw_failure
                    completed.pop(market_id, None)
                    completed_typed_payloads.pop(market_id, None)
                    continue
                if actual_sha256 is None or response_identity is None:
                    continue
                if row.get("raw_response_sha256") in (None, ""):
                    completed[market_id] = {
                        **dict(row),
                        "raw_response_sha256": actual_sha256,
                    }
                    row = completed[market_id]
                try:
                    completed_typed_payloads[market_id] = (
                        _validated_typed_payload_inventory(
                            trusted_leg=legs_by_market[market_id],
                            collector_row=row,
                            accepted_raw_sha256=actual_sha256,
                            values=completed_typed_payloads.get(market_id, ()),
                        )
                    )
                except (TypeError, ValueError):
                    terminal_reasons[market_id] = "collection_failed"
                    completed.pop(market_id, None)
                    completed_typed_payloads.pop(market_id, None)
                    continue
                try:
                    authority_payload = _attachment_authority_bytes(
                        market_id=market_id,
                        trusted_leg=legs_by_market[market_id],
                        collector_row=row,
                        accepted_raw_sha256=actual_sha256,
                        collection_input_generation=expected_source_generation,
                        validated_specs=completed_typed_payloads[market_id],
                    )
                except (TypeError, ValueError):
                    terminal_reasons[market_id] = "collection_failed"
                    completed.pop(market_id, None)
                    completed_typed_payloads.pop(market_id, None)
                    continue
                entry_name = stage_dir.name
                entry_descriptor = None
                try:
                    _require_raw_run_guards(
                        staging_root, accepted_root, raw_run_guards
                    )
                    entry_descriptor = _open_directory_entry(
                        raw_run_descriptors["staging"],
                        entry_name,
                        stage_identity,
                    )
                    (
                        authority_identity,
                        authority_sha256,
                    ) = _write_regular_file_at(
                        entry_descriptor,
                        _ATTACHMENT_AUTHORITY_FILENAME,
                        authority_payload,
                        max_bytes=_ATTACHMENT_AUTHORITY_MAX_BYTES,
                    )
                    _rename_directory_entry(
                        entry_name,
                        entry_name,
                        source_directory_fd=raw_run_descriptors["staging"],
                        destination_directory_fd=raw_run_descriptors["accepted"],
                    )
                except (OSError, _UnsafeRawEvidence):
                    _clear_unsafe_accepted_alias(
                        raw_run_descriptors,
                        raw_run_guards["accepted"],
                    )
                    terminal_reasons[market_id] = "raw_evidence_path_unsafe"
                    completed.pop(market_id, None)
                    completed_typed_payloads.pop(market_id, None)
                    if entry_descriptor is not None:
                        os.close(entry_descriptor)
                    continue
                try:
                    post_failure = _post_promotion_failure(
                        entry_name,
                        entry_descriptor,
                        stage_identity,
                        response_identity,
                        actual_sha256,
                        authority_identity,
                        authority_sha256,
                        staging_root,
                        accepted_root,
                        raw_run_guards,
                        raw_run_descriptors,
                    )
                    if post_failure is not None:
                        rollback_error = None
                        try:
                            rollback_succeeded = _rollback_promoted_evidence(
                                entry_name,
                                stage_identity,
                                raw_run_descriptors,
                            )
                            _descriptor_directory_identity(
                                entry_descriptor,
                                stage_identity,
                            )
                            if (
                                not rollback_succeeded
                                or not _rollback_state_is_safe(
                                    entry_name,
                                    stage_identity,
                                    raw_run_descriptors,
                                )
                            ):
                                raise _UnsafeRawEvidence(
                                    "raw evidence rollback could not be verified"
                                )
                        except (OSError, _UnsafeRawEvidence) as error:
                            rollback_error = error
                        finally:
                            _clear_unsafe_accepted_alias(
                                raw_run_descriptors,
                                raw_run_guards["accepted"],
                            )
                        if rollback_error is not None:
                            raise _UnsafeRawEvidence(
                                "raw evidence rollback could not be verified"
                            ) from rollback_error
                        terminal_reasons[market_id] = post_failure
                        completed.pop(market_id, None)
                        completed_typed_payloads.pop(market_id, None)
                finally:
                    os.close(entry_descriptor)
        finally:
            _close_raw_run_descriptors(raw_run_descriptors)
    fixed_blocks_by_market = {}
    for market_id in market_ids:
        leg = legs_by_market[market_id]
        if _market_type(leg) != "dex":
            continue
        chain = _source_key(leg)[1]
        if chain in fixed_blocks:
            block = fixed_blocks[chain]
            fixed_blocks_by_market[market_id] = {
                "fixed_block_number": str(block["block_number"]),
                "fixed_block_timestamp": str(block["block_timestamp"]),
            }
    legs = materialize_route_leg_rows(
        market_ids, completed, deadline_exceeded=expired,
        terminal_reasons=terminal_reasons,
        fixed_blocks_by_market=fixed_blocks_by_market,
    )
    legs = [
        _final_route_leg_projection(
            legs_by_market[row["market_id"]],
            row,
            market_id=row["market_id"],
        )
        for row in legs
    ]
    eligible_market_ids = {
        row["market_id"]
        for row in legs
        if row.get("status") in {"observed", "partial"}
    }
    for market_id in list(completed_typed_payloads):
        if market_id not in eligible_market_ids:
            completed_typed_payloads.pop(market_id, None)
    if set(completed_typed_payloads) != eligible_market_ids:
        raise ValueError("typed-source payload capability differs from final legs")
    rows_by_market = {row["market_id"]: row for row in legs}
    route_rows = []
    for route in routes:
        candidate = {
            **dict(route),
            "validated_at": collection_completed_at,
        }
        buy_market_id = candidate.get("buy_market_id")
        sell_market_id = candidate.get("sell_market_id")
        if buy_market_id not in rows_by_market or sell_market_id not in rows_by_market:
            raise ValueError("route references an unselected leg")
        route_rows.append(
            {**candidate, **classify_route_timing(
                candidate,
                rows_by_market[buy_market_id],
                rows_by_market[sell_market_id],
            )}
        )
    route_rows.sort(key=lambda row: row["route_id"])
    result: Dict[str, Any] = {
        "schema": "route_cohort_collection/v1",
        "candidate_source_generation": generation,
        "collection_input_generation": expected_source_generation,
        "source_state": {
            "candidate_source_generation": generation,
            "collection_input_generation": expected_source_generation,
        },
        "raw_evidence_run_id": run_id,
        "target_observed_at": target,
        "collection_started_at": collection_started_at,
        "collection_completed_at": collection_completed_at,
        "collection_deadline_at": collection_deadline_at,
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": dict(universe.get("selection_window") or {}),
        "requested_notionals_usd": list(universe.get("requested_notionals_usd") or []),
        "legs": legs,
        "routes": sorted(
            (dict(route) for route in routes),
            key=lambda row: str(row.get("route_id") or ""),
        ),
        "route_rows": route_rows,
    }
    result["route_cohort_id"] = "cohort:" + _canonical_fingerprint(result)
    result["fingerprint"] = _canonical_fingerprint(result)
    return _RouteCollectionResult(result, completed_typed_payloads)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the deliberately narrow Task 4 collection CLI."""
    parser = argparse.ArgumentParser(
        description="Collect a bounded synchronized route-leg cohort"
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--deadline-seconds", type=float, default=60)
    parser.add_argument("--max-workers", type=int, default=24)
    parser.add_argument("--cex-workers-per-venue", type=int, default=2)
    parser.add_argument("--dex-workers-per-chain", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--publish", action="store_true")
    return parser.parse_args(argv)


def _load_universe_for_cli(data_dir: Path) -> Mapping[str, Any]:
    path = data_dir / "route_universe.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError("route_universe.json does not exist in --data-dir")
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("route_universe.json is invalid") from error
    if not isinstance(value, Mapping):
        raise ValueError("route universe is invalid")
    return value


def _cli_tokens(value: Optional[str]) -> Optional[Set[str]]:
    if value is None:
        return None
    tokens = {item.strip().upper() for item in value.split(",") if item.strip()}
    if not tokens:
        raise ValueError("--tokens is invalid")
    return tokens


def _validate_cli_values(args: argparse.Namespace) -> Optional[Set[str]]:
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
        end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else None
    except ValueError as error:
        raise ValueError("--start/--end must be ISO dates") from error
    if start and end and start > end:
        raise ValueError("--start must not be after --end")
    if (not math.isfinite(args.deadline_seconds) or args.deadline_seconds <= 0
            or args.max_workers < 1
            or args.cex_workers_per_venue < 1 or args.dex_workers_per_chain < 1):
        raise ValueError("deadline and worker limits must be positive")
    return _cli_tokens(args.tokens)


def _validated_universe(
    universe: Mapping[str, Any],
    tokens: Optional[Set[str]],
    *,
    requested_start: Optional[str] = None,
    requested_end: Optional[str] = None,
) -> Dict[str, Any]:
    generation = universe.get("candidate_source_generation")
    selected = universe.get("selected_legs")
    routes = universe.get("routes")
    if (not isinstance(generation, str) or not generation or not isinstance(selected, list)
            or not isinstance(routes, list) or not selected or not routes):
        raise ValueError("route universe must contain selected legs and routes")
    selected_by_id = {}
    for leg in selected:
        if not isinstance(leg, Mapping) or not isinstance(leg.get("market_id"), str):
            raise ValueError("route leg identity is invalid")
        if leg["market_id"] in selected_by_id:
            raise ValueError("duplicate route leg")
        selected_by_id[leg["market_id"]] = dict(leg)
    window = universe.get("selection_window")
    if not isinstance(window, Mapping):
        if requested_start is not None or requested_end is not None:
            raise ValueError("requested date range does not match universe selection_window")
    else:
        if requested_start is not None and window.get("start") != requested_start:
            raise ValueError("requested date range does not match universe selection_window")
        if requested_end is not None and window.get("end") != requested_end:
            raise ValueError("requested date range does not match universe selection_window")
    normalized_routes = _validated_routes(routes)
    _validate_route_volume_lineage(normalized_routes, selected_by_id)
    filtered_routes = [
        route for route in normalized_routes
        if tokens is None or route.get("token_symbol") in tokens
    ]
    if not filtered_routes:
        raise ValueError("requested tokens have no routes")
    ids = set(collect_unique_route_legs(filtered_routes))
    if not ids <= set(selected_by_id):
        raise ValueError("route references an unselected leg")
    filtered_legs = [selected_by_id[item] for item in sorted(ids)]
    return {**dict(universe), "selected_legs": filtered_legs, "routes": filtered_routes}


def _normal_identity_value(kind: str, key: str, value: Any) -> str:
    text = str(value).strip()
    if key in {"token_symbol", "cex_symbol"}:
        return text.upper()
    if key in {"exchange", "chain", "dex", "pool_address", "market_type"}:
        return text.lower()
    return text


def _resolve_inventory_legs(
    universe: Mapping[str, Any], data_dir: Path
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    selected = universe["selected_legs"]
    cex_needed = {row["market_id"] for row in selected if _market_type(row) == "cex"}
    dex_needed = {row["market_id"] for row in selected if _market_type(row) == "dex"}
    resolved: Dict[str, Mapping[str, Any]] = {}
    if cex_needed:
        cex_rows = load_cataloged_markets(
            data_dir / "market_facts.sqlite3", data_dir / "cex_exchange_volume_daily.csv"
        )
        for row in cex_rows:
            identity = cex_market_id(row)
            if identity in resolved:
                raise ValueError("duplicate canonical inventory market ID")
            resolved[identity] = row
    if dex_needed:
        for row in load_pool_inventory(data_dir / "dex_pool_tvl_latest.csv"):
            identity = dex_market_id(row)
            if identity in resolved:
                raise ValueError("duplicate canonical inventory market ID")
            resolved[identity] = row
    if (cex_needed | dex_needed) - set(resolved):
        raise ValueError("selected route leg is absent from authoritative inventory")
    legs = []
    authoritative_rows = []
    for selected_leg in selected:
        identity = selected_leg["market_id"]
        authoritative = dict(resolved[identity])
        kind = _market_type(selected_leg)
        identity_fields = (
            ("token_symbol", "exchange", "cex_symbol")
            if kind == "cex"
            else ("token_symbol", "chain", "dex", "pool_address")
        )
        for key in identity_fields:
            if key in selected_leg and selected_leg[key] not in (None, ""):
                if key not in authoritative or _normal_identity_value(
                    kind, key, selected_leg[key]
                ) != _normal_identity_value(kind, key, authoritative[key]):
                    raise ValueError(
                        "universe identity conflicts with authoritative inventory"
                    )
        bound = {
            **dict(selected_leg),
            **authoritative,
            "market_id": identity,
            "market_type": kind,
        }
        legs.append(bound)
        authoritative_rows.append({
            **authoritative,
            "market_id": identity,
            "market_type": kind,
        })
    authoritative_rows.sort(key=lambda row: row["market_id"])
    return {**dict(universe), "selected_legs": legs}, authoritative_rows


def _load_cli_collection_inputs(
    data_dir: Path,
    tokens: Optional[Set[str]],
    *,
    requested_start: Optional[str],
    requested_end: Optional[str],
) -> Tuple[Dict[str, Any], str]:
    full = _validated_universe(
        _load_universe_for_cli(data_dir),
        None,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    filtered = _validated_universe(
        full,
        tokens,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    resolved, authoritative_rows = _resolve_inventory_legs(filtered, data_dir)
    generation = _canonical_fingerprint({
        "route_universe": full,
        "requested_tokens": sorted(tokens) if tokens is not None else None,
        "authoritative_inventory_rows": authoritative_rows,
    })
    return resolved, generation


def _default_dex_block_resolver(chain: str, *, deadline: CollectionDeadline) -> Mapping[str, Any]:
    url = rpc_url_for_chain(chain)
    if not url:
        raise ValueError("missing RPC endpoint")
    client = RpcClient(chain, url, deadline=deadline)
    chain_id = client.chain_id()
    expected_chain_id = _CANONICAL_CHAIN_IDS.get(chain)
    if expected_chain_id is None or chain_id != expected_chain_id:
        raise ValueError("fixed block chain ID is invalid")
    number = client.block_number()
    block = client.block(hex(number))
    return {
        "block_number": number,
        "block_timestamp": block_timestamp_text(block),
        "chain_id": chain_id,
        "block_header": canonical_route_fixed_block_header(block),
    }


def finalize_route_opportunity_bundle(
    *,
    data_dir: Path,
    opportunity_inputs: Iterable[Mapping[str, Any]],
    source_root: Optional[Path] = None,
    fee_profile_path: Optional[Path] = None,
    fee_profile_id: Optional[str] = None,
    inventory_profile_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Finalize the already-published core without invoking collection again."""
    root = Path(data_dir)
    return publish_complete_route_bundle(
        core_root=root / "routes/core",
        routes_root=root / "routes",
        raw_root=root / "raw/route-cohort",
        opportunity_inputs=opportunity_inputs,
        source_root=source_root,
        fee_profile_path=fee_profile_path,
        fee_profile_id=fee_profile_id,
        inventory_profile_path=inventory_profile_path,
    )


def main(
    argv: Optional[List[str]] = None,
    *,
    cex_collector: Callable[..., Any] = collect_cex_market_observation,
    dex_collector: Callable[..., Any] = collect_dex_pool_observation,
    dex_block_resolver: Callable[..., Mapping[str, Any]] = _default_dex_block_resolver,
    executor_factory: Callable[..., Any] = _ForkProcessExecutor,
) -> Dict[str, Any]:
    """Run bounded collection and optionally publish through the Task 5 boundary."""
    args = parse_args(argv)
    tokens = _validate_cli_values(args)
    resolved, expected_generation = _load_cli_collection_inputs(
        args.data_dir,
        tokens,
        requested_start=args.start,
        requested_end=args.end,
    )

    def read_generation() -> str:
        _resolved, current_generation = _load_cli_collection_inputs(
            args.data_dir,
            tokens,
            requested_start=args.start,
            requested_end=args.end,
        )
        return current_generation

    if args.dry_run:
        if read_generation() != expected_generation:
            raise ValueError("collection input generation changed")
        return {
            "dry_run": True,
            "candidate_source_generation": resolved[
                "candidate_source_generation"
            ],
            "collection_input_generation": expected_generation,
            "selected_leg_count": len(resolved["selected_legs"]),
            "route_count": len(resolved["routes"]),
        }

    result = collect_route_cohort(
        resolved,
        cex_collector=cex_collector,
        dex_collector=dex_collector,
        dex_block_resolver=dex_block_resolver,
        source_generation_reader=read_generation,
        expected_source_generation=expected_generation,
        raw_root=args.data_dir / "raw" / "route-cohort",
        deadline_seconds=args.deadline_seconds,
        max_workers=args.max_workers,
        cex_workers_per_venue=args.cex_workers_per_venue,
        dex_workers_per_chain=args.dex_workers_per_chain,
        executor_factory=executor_factory,
    )
    if args.publish:
        publish_route_cohort_bundle(
            result,
            core_root=args.data_dir / "routes/core",
        )
    return result


if __name__ == "__main__":  # pragma: no cover - command line wrapper
    print(json.dumps(main(), ensure_ascii=False, sort_keys=True))
