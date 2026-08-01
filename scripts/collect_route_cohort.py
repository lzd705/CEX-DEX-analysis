"""Bounded, route-scoped collection of synchronized market legs."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
from queue import Queue
import re
from threading import Lock, Thread
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit
import uuid

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.route_cohort import canonical_route_id, classify_route_timing
    from scripts.fetch_cex_depth import (
        cex_market_id,
        collect_cex_market_observation,
        load_cataloged_markets,
    )
    from scripts.fetch_dex_depth import (
        RpcClient,
        block_timestamp_text,
        collect_dex_pool_observation,
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )
except ModuleNotFoundError:
    from collection_deadline import CollectionDeadline, CollectionDeadlineExceeded
    from route_cohort import canonical_route_id, classify_route_timing
    from fetch_cex_depth import (
        cex_market_id,
        collect_cex_market_observation,
        load_cataloged_markets,
    )
    from fetch_dex_depth import (
        RpcClient,
        block_timestamp_text,
        collect_dex_pool_observation,
        rpc_url_for_chain,
        dex_market_id,
        load_pool_inventory,
    )


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
        if route.get("route_id") not in (None, "", route_id):
            raise ValueError("route candidate is invalid")
        if route_id in route_ids:
            raise ValueError("duplicate route candidate")
        route_ids.add(route_id)
        normalized.append(dict(route))
    return normalized


def materialize_route_leg_rows(
    market_ids: Iterable[str],
    collected_rows: Mapping[str, Mapping[str, Any]],
    *,
    deadline_exceeded: Optional[Set[str]] = None,
    terminal_reasons: Optional[Mapping[str, str]] = None,
) -> List[dict[str, Any]]:
    """Normalize collected facts, retaining every requested terminal leg."""
    expired = deadline_exceeded or set()
    reasons = terminal_reasons or {}
    rows = []
    for market_id in sorted(set(market_ids)):
        if market_id in expired or market_id in reasons:
            rows.append(
                {
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
            )
            continue
        row = dict(collected_rows.get(market_id, {}))
        row["leg_id"] = market_id
        row["market_id"] = market_id
        rows.append(row)
    return rows


def _canonical_fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _market_type(leg: Mapping[str, Any]) -> str:
    declared = leg.get("market_type")
    market_id = leg.get("market_id")
    if declared in ("cex", "dex"):
        return str(declared)
    if isinstance(market_id, str) and market_id.startswith("cex:"):
        return "cex"
    if isinstance(market_id, str) and market_id.startswith("dex:"):
        return "dex"
    raise ValueError("route leg market type is invalid")


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


def _source_key(leg: Mapping[str, Any]) -> Tuple[str, str]:
    market_id = str(leg["market_id"])
    if _market_type(leg) == "cex":
        return "cex", str(leg.get("exchange") or market_id.split(":", 2)[1]).lower()
    return "dex", str(leg.get("chain") or market_id.split(":", 3)[1]).lower()


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
        if row.get(key) not in (None, "") and _normal_identity_value(
            market_type, key, row[key]
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


def _safe_leg_projection(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep fact fields while excluding path, secret, and exception payloads."""
    forbidden = {"raw_path", "error", "exception", "traceback", "password", "api_key", "authorization", "token", "secret", "credential", "access_token", "access-token", "bearer", "signature"}
    projected = {}
    for key, value in row.items():
        lowered = key.lower()
        if lowered in forbidden or any(
            marker in lowered for marker in ("password", "api_key", "authorization", "secret", "credential", "access_token", "bearer", "signature")
        ):
            continue
        if lowered in {"source_endpoint", "endpoint"} and isinstance(value, str):
            parsed = urlsplit(value)
            value = urlunsplit((parsed.scheme, parsed.hostname or "", parsed.path, "", ""))
        projected[key] = value
    return projected


_SNAPSHOT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def _run_id(value: Optional[str], wall_time: datetime) -> str:
    if value is not None:
        if not _SNAPSHOT_ID.fullmatch(value):
            raise ValueError("snapshot_id is invalid")
        return value
    prefix = wall_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return "{}-{}".format(prefix, uuid.uuid4().hex)


def _raw_run_directory(raw_root: Path, run_id: str) -> Tuple[Path, Path]:
    raw_root.mkdir(parents=True, exist_ok=True)
    run_dir = raw_root / run_id
    run_dir.mkdir(exist_ok=False)
    staging = run_dir / "staging"
    accepted = run_dir / "accepted"
    staging.mkdir()
    accepted.mkdir()
    return staging, accepted


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
    executor_factory: Callable[..., Any] = _DaemonFutureExecutor,
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
    if source_generation_reader is None or not isinstance(expected_source_generation, str) or not expected_source_generation:
        raise ValueError(
            "collection input generation reader is required, and expected value is required"
        )
    if source_generation_reader() != expected_source_generation:
        raise ValueError("collection input generation changed")
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
    has_dex = any(_market_type(legs_by_market[item]) == "dex" for item in market_ids)
    if has_dex and dex_block_resolver is None:
        raise ValueError("DEX fixed block resolver is required")

    wall_start = wall_clock()
    collection_started_at = _canonical_utc(wall_start, field="collection_started_at")
    target = _canonical_utc(
        target_observed_at if target_observed_at is not None else wall_start,
        field="target_observed_at",
    )
    active_deadline = deadline or CollectionDeadline.for_duration(duration)
    remaining_at_start = active_deadline.remaining_seconds()
    collection_deadline_at = _canonical_utc(
        wall_start + timedelta(seconds=remaining_at_start),
        field="collection_deadline_at",
    )
    root = raw_root or Path("data/raw/route-cohort")
    run_id = _run_id(snapshot_id, wall_start)
    staging_root, accepted_root = _raw_run_directory(root, run_id)
    terminal_reasons: Dict[str, str] = {}
    expired: Set[str] = set()
    fixed_blocks: Dict[str, Mapping[str, Any]] = {}
    dex_by_chain: Dict[str, List[str]] = {}
    for market_id in market_ids:
        if _market_type(legs_by_market[market_id]) == "dex":
            dex_by_chain.setdefault(_source_key(legs_by_market[market_id])[1], []).append(market_id)
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
    completed_stage_dirs: Dict[str, Path] = {}
    futures: Dict[Any, Tuple[str, Tuple[str, str]]] = {}

    def raw_paths(market_id: str) -> Tuple[Path, Path]:
        name = hashlib.sha256(market_id.encode("utf-8")).hexdigest()
        stage_dir = staging_root / name
        stage_dir.mkdir(exist_ok=False)
        return stage_dir / "response.json", stage_dir

    def collect_one(
        market_id: str,
    ) -> Tuple[str, Optional[Mapping[str, Any]], Optional[str], Optional[Path]]:
        leg = legs_by_market[market_id]
        kind, source = _source_key(leg)
        raw_path, stage_dir = raw_paths(market_id)
        try:
            active_deadline.require_remaining()
            if kind == "cex":
                row = _row_from_collector(cex_collector(
                    dict(leg), snapshot_id=run_id, raw_path=raw_path,
                    deadline=active_deadline,
                ))
            else:
                block = fixed_blocks[source]
                row = _row_from_collector(dex_collector(
                    dict(leg), snapshot_id=run_id, raw_path=raw_path,
                    fixed_block_number=block["block_number"],
                    fixed_block_timestamp=block.get("block_timestamp", ""),
                    deadline=active_deadline,
                ))
                if (str(row.get("block_number")) != str(block["block_number"])
                        or str(row.get("block_timestamp") or "") != str(block.get("block_timestamp") or "")):
                    return market_id, None, "fixed_block_lineage_mismatch", stage_dir
                row = {
                    **dict(row),
                    "fixed_block_number": str(block["block_number"]),
                    "fixed_block_timestamp": str(block.get("block_timestamp") or ""),
                }
            if not _collector_identity_matches(market_id, kind, leg, row):
                return market_id, None, "collector_identity_mismatch", stage_dir
            active_deadline.require_remaining()
            return market_id, row, None, stage_dir
        except CollectionDeadlineExceeded:
            return market_id, None, "route_deadline_exceeded", stage_dir
        except Exception:
            return market_id, None, "collection_failed", stage_dir

    def resolve_one(
        chain: str,
    ) -> Tuple[str, Optional[Mapping[str, Any]], Optional[str]]:
        try:
            active_deadline.require_remaining()
            resolved = dex_block_resolver(chain, deadline=active_deadline)
            if not isinstance(resolved, Mapping) or not isinstance(
                resolved.get("block_number"), int
            ):
                raise ValueError("invalid fixed block")
            active_deadline.require_remaining()
            return chain, dict(resolved), None
        except CollectionDeadlineExceeded:
            return chain, None, "route_deadline_exceeded"
        except Exception:
            return chain, None, "fixed_block_unavailable"

    executor = executor_factory(max_workers=max_workers)

    def source_limit(key: Tuple[str, str]) -> int:
        if key[0] == "cex":
            return cex_workers_per_venue
        if key[0] == "dex":
            return dex_workers_per_chain
        return 1

    def submit_fairly() -> None:
        nonlocal source_index
        progressed = True
        while len(futures) < max_workers and progressed:
            progressed = False
            for _unused in range(len(source_order)):
                key = source_order[source_index % len(source_order)]
                source_index += 1
                limit = source_limit(key)
                if pending_by_source[key] and active_by_source.get(key, 0) < limit:
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
            done, _not_done = wait(list(futures), timeout=remaining, return_when=FIRST_COMPLETED)
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
                    returned_id, row, reason, stage_dir = future.result()
                    if reason == "route_deadline_exceeded":
                        expired.add(returned_id)
                    elif reason:
                        terminal_reasons[returned_id] = reason
                    elif row is not None:
                        completed[returned_id] = row
                        if stage_dir is not None:
                            completed_stage_dirs[returned_id] = stage_dir
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

    if source_generation_reader() != expected_source_generation:
        raise ValueError("collection input generation changed")
    for market_id in sorted(completed_stage_dirs):
        stage_dir = completed_stage_dirs[market_id]
        if stage_dir.exists():
            stage_dir.replace(accepted_root / stage_dir.name)
    legs = materialize_route_leg_rows(
        market_ids, completed, deadline_exceeded=expired,
        terminal_reasons=terminal_reasons,
    )
    legs = [
        _safe_leg_projection({
            **row,
            **{
                key: legs_by_market[row["market_id"]][key]
                for key in ("execution_adapter_supported", "execution_adapter_status")
                if key in legs_by_market[row["market_id"]]
            },
        })
        for row in legs
    ]
    rows_by_market = {row["market_id"]: row for row in legs}
    route_rows = []
    for route in routes:
        candidate = dict(route)
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
    return result


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
    number = client.block_number()
    return {
        "block_number": number,
        "block_timestamp": block_timestamp_text(client.block(hex(number))),
    }


def main(
    argv: Optional[List[str]] = None,
    *,
    cex_collector: Callable[..., Any] = collect_cex_market_observation,
    dex_collector: Callable[..., Any] = collect_dex_pool_observation,
    dex_block_resolver: Callable[..., Mapping[str, Any]] = _default_dex_block_resolver,
) -> Dict[str, Any]:
    """Run live bounded collection; Task 5 remains the only publisher."""
    args = parse_args(argv)
    tokens = _validate_cli_values(args)
    if args.publish:
        raise RuntimeError("--publish is unavailable until Task 5 immutable publication")
    universe_path = args.data_dir / "route_universe.json"
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
    )
    return {**result, "dry_run": False, "universe_path": str(universe_path)}


if __name__ == "__main__":  # pragma: no cover - command line wrapper
    print(json.dumps(main(), ensure_ascii=False, sort_keys=True))
