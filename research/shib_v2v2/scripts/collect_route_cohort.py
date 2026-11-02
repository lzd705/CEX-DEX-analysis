"""Bounded, route-scoped collection of synchronized market legs."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
import argparse
import ctypes
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import errno
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
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )
    from scripts.route_shadow_inputs import (
        TYPED_SOURCE_LINEAGE_SCHEMA,
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from scripts.route_quantity import MarketRules
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
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )
    from route_shadow_inputs import (  # type: ignore[no-redef]
        TYPED_SOURCE_LINEAGE_SCHEMA,
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from route_quantity import MarketRules


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
    """A normal JSON mapping plus one process-local sealed typed capability."""

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


def publish_typed_source_manifest(
    raw_run_root: Path,
    *,
    raw_evidence_run_id: str,
    members: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Atomically retain exact observed typed members and their manifest."""
    if not isinstance(raw_evidence_run_id, str) or not _SNAPSHOT_ID.fullmatch(
        raw_evidence_run_id
    ):
        raise ValueError("typed-source raw run ID is invalid")
    root = _canonical_raw_path(Path(raw_run_root))
    _reject_symlink_ancestry(root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    stage = root / ".typed-stage-{}".format(uuid.uuid4().hex)
    stage.mkdir(mode=0o700)
    typed_stage = stage / "typed"
    typed_stage.mkdir(mode=0o700)
    records = []
    seen = set()
    try:
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
            target = typed_stage / filename
            descriptor = os.open(
                str(target),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                offset = 0
                while offset < len(payload):
                    written = os.write(descriptor, payload[offset:])
                    if written <= 0:
                        raise OSError("short typed-source member write")
                    offset += written
                os.fsync(descriptor)
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                    raise ValueError("typed-source member is unsafe")
            finally:
                os.close(descriptor)
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
        manifest_path = stage / "typed-manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        with manifest_path.open("rb") as handle:
            os.fsync(handle.fileno())
        stage_fd = os.open(str(stage), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(stage_fd)
        finally:
            os.close(stage_fd)
        final_typed = root / "typed"
        final_manifest = root / "typed-manifest.json"
        if final_typed.exists() or final_manifest.exists():
            raise ValueError("immutable typed-source inventory already exists")
        os.rename(typed_stage, final_typed)
        os.rename(manifest_path, final_manifest)
        root_fd = os.open(str(root), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
        for record in records:
            payload = (final_typed / record["filename"]).read_bytes()
            if len(payload) != record["size"] or hashlib.sha256(payload).hexdigest() != record["sha256"]:
                raise ValueError("typed-source member changed after publication")
        if final_manifest.read_bytes() != manifest_bytes:
            raise ValueError("typed-source manifest changed after publication")
        return {
            "manifest": manifest,
            "typed_source_manifest_sha256": hashlib.sha256(
                manifest_bytes
            ).hexdigest(),
            "typed_root": str(final_typed),
            "manifest_path": str(final_manifest),
        }
    finally:
        if stage.exists():
            if typed_stage.exists():
                for path in typed_stage.iterdir():
                    path.unlink()
                typed_stage.rmdir()
            manifest_path = stage / "typed-manifest.json"
            if manifest_path.exists():
                manifest_path.unlink()
            stage.rmdir()


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


def attach_typed_source_lineage(
    cohort: Mapping[str, Any], *, raw_root: Path
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
            response_path = (
                raw_run_root / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
                / "response.json"
            )
            response, _identity = _read_regular_file(response_path)
            raw_sha = hashlib.sha256(response).hexdigest()
            if raw_sha != leg.get("raw_response_sha256"):
                raise ValueError("typed-source CEX raw response lineage differs")
            specs.append(_typed_member_spec(
                market_id, "cex_raw_book_response", response, raw_sha
            ))
        elif market_type == "dex":
            context = leg.get("collector_context")
            if isinstance(context, Mapping):
                context_payload = _typed_json_bytes(context)
                specs.append(_typed_member_spec(
                    market_id, "dex_usd_price_context", context_payload,
                    hashlib.sha256(context_payload).hexdigest(),
                ))
        for sealed in sealed_payloads.get(market_id, ()):
            if (
                not isinstance(sealed, Mapping)
                or set(sealed) != {
                    "market_id", "role", "payload", "logical_generation",
                    "adapter_id", "content_schema",
                }
                or sealed.get("market_id") != market_id
            ):
                raise ValueError("typed-source payload capability is invalid")
            specs.append(dict(sealed))
        pending[market_id] = members

    publication = publish_typed_source_manifest(
        raw_run_root, raw_evidence_run_id=run_id, members=specs
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
    for leg in legs:
        market_id = leg["market_id"]
        market_type = leg["market_type"]
        lineage = validate_typed_source_lineage({
            "schema": TYPED_SOURCE_LINEAGE_SCHEMA,
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


def _validated_typed_payload_inventory(
    market_id: str,
    market_type: str,
    values: Iterable[Any],
) -> Tuple[Mapping[str, Any], ...]:
    members = []
    seen = set()
    aggregate = 0
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
                if (
                    not isinstance(parsed, Mapping)
                    or parsed.get("schema") != contract["content_schema"]
                    or parsed.get("state_id")
                    != freeze_v2_pool_state({
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
                    }).state_id
                    or payload != _typed_json_bytes(parsed)
                ):
                    raise ValueError("invalid state")
                logical_generation = parsed["state_id"].split(":", 1)[1]
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
            safe_nested = _safe_projection_value(
                nested,
                seen=seen,
                depth=depth + 1,
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
    if not stat.S_ISREG(before.st_mode):
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
        payload, actual_response_identity = _read_regular_file_at(
            entry_descriptor,
            "response.json",
        )
        if actual_response_identity != response_identity:
            raise _UnsafeRawEvidence("accepted response identity changed")
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
        and (
            not isinstance(legs_by_market[item].get("collector_context"), Mapping)
            or legs_by_market[item]["collector_context"].get("status") == "observed"
        )
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
            context = leg.get("collector_context")
            if isinstance(context, Mapping) and context.get("status") != "observed":
                terminal_reasons[market_id] = "usd_price_context_{}".format(
                    context.get("status")
                )
            else:
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
            if kind == "cex":
                row = _row_from_collector(cex_collector(
                    dict(leg), snapshot_id=run_id, raw_path=raw_path,
                    deadline=active_deadline,
                    **typed_keyword,
                ))
            else:
                block = fixed_blocks[source]
                row = _row_from_collector(dex_collector(
                    dict(leg), snapshot_id=run_id, raw_path=raw_path,
                    fixed_block_number=block["block_number"],
                    fixed_block_timestamp=block.get("block_timestamp", ""),
                    fixed_chain_id=block.get("chain_id"),
                    fixed_block_header=block.get("block_header"),
                    deadline=active_deadline,
                    **typed_keyword,
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
            inventory = _validated_typed_payload_inventory(
                market_id, kind, typed_payloads
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
        _safe_leg_projection({
            **row,
            "market_type": _market_type(legs_by_market[row["market_id"]]),
            **{
                key: legs_by_market[row["market_id"]][key]
                for key in ("execution_adapter_supported", "execution_adapter_status")
                if key in legs_by_market[row["market_id"]]
            },
            **(
                {
                    "collector_context": legs_by_market[row["market_id"]]["collector_context"],
                    "usd_price_source_snapshot_id": legs_by_market[row["market_id"]]["collector_context"]["snapshot_id"],
                    "usd_price_observed_at": legs_by_market[row["market_id"]]["collector_context"]["observed_at"],
                    "usd_price_source": legs_by_market[row["market_id"]]["collector_context"]["source"],
                    "usd_price_source_endpoint": legs_by_market[row["market_id"]]["collector_context"]["source_endpoint"],
                    "usd_price_raw_response_sha256": legs_by_market[row["market_id"]]["collector_context"]["raw_response_sha256"],
                    **(
                        {}
                        if legs_by_market[row["market_id"]]["collector_context"].get("status") == "observed"
                        else {
                            "available": False,
                            "token0_price_usd": None,
                            "token1_price_usd": None,
                        }
                    ),
                }
                if _market_type(legs_by_market[row["market_id"]]) == "dex"
                and isinstance(legs_by_market[row["market_id"]].get("collector_context"), Mapping)
                else {}
            ),
        })
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
