"""Bounded, route-scoped collection of synchronized market legs."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from threading import Semaphore
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.route_cohort import classify_route_timing
except ModuleNotFoundError:
    from collection_deadline import CollectionDeadline, CollectionDeadlineExceeded
    from route_cohort import classify_route_timing


def collect_unique_route_legs(
    routes: Iterable[Mapping[str, Any]],
) -> List[str]:
    """Return each route-market identity once in canonical lexical order."""
    market_ids = set()
    for route in routes:
        market_ids.add(str(route["buy_market_id"]))
        market_ids.add(str(route["sell_market_id"]))
    return sorted(market_ids)


def materialize_route_leg_rows(
    market_ids: Iterable[str],
    collected_rows: Mapping[str, Mapping[str, Any]],
    *,
    deadline_exceeded: Optional[Set[str]] = None,
) -> List[dict[str, Any]]:
    """Normalize collected facts, retaining every requested terminal leg."""
    expired = deadline_exceeded or set()
    rows = []
    for market_id in sorted(set(market_ids)):
        if market_id in expired:
            rows.append(
                {
                    "leg_id": market_id,
                    "market_id": market_id,
                    "status": "deadline_exceeded",
                    "available": False,
                    "reason_code": "route_deadline_exceeded",
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


def _target_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def collect_route_cohort(
    universe: Mapping[str, Any],
    *,
    cex_collector: Callable[..., Mapping[str, Any]],
    dex_collector: Callable[..., Mapping[str, Any]],
    deadline_seconds: float = 60,
    max_workers: int = 24,
    cex_workers_per_venue: int = 2,
    dex_workers_per_chain: int = 4,
    target_observed_at: Optional[str] = None,
    deadline: Optional[CollectionDeadline] = None,
    dex_block_resolver: Optional[Callable[..., Mapping[str, Any]]] = None,
    source_generation_reader: Optional[Callable[[], str]] = None,
    executor_factory: Callable[..., Any] = ThreadPoolExecutor,
) -> Dict[str, Any]:
    """Collect each selected market once under global and source-local limits."""
    if not isinstance(universe, Mapping):
        raise ValueError("route universe is invalid")
    generation = universe.get("candidate_source_generation")
    selected = universe.get("selected_legs")
    if not isinstance(generation, str) or not generation or not isinstance(selected, list):
        raise ValueError("route universe is invalid")
    if min(max_workers, cex_workers_per_venue, dex_workers_per_chain) < 1:
        raise ValueError("worker limits must be positive")
    if source_generation_reader is not None and source_generation_reader() != generation:
        raise ValueError("candidate source generation changed")

    legs_by_market: Dict[str, Mapping[str, Any]] = {}
    for leg in selected:
        if not isinstance(leg, Mapping) or not isinstance(leg.get("market_id"), str):
            raise ValueError("route leg identity is invalid")
        market_id = leg["market_id"]
        if market_id in legs_by_market:
            raise ValueError("duplicate route leg")
        _market_type(leg)
        legs_by_market[market_id] = leg
    market_ids = collect_unique_route_legs(universe.get("routes", []))
    if set(market_ids) - set(legs_by_market):
        raise ValueError("route references an unselected leg")
    if dex_block_resolver is None and any(
        _market_type(legs_by_market[market_id]) == "dex"
        for market_id in market_ids
    ):
        raise ValueError("DEX fixed block resolver is required")

    active_deadline = deadline or CollectionDeadline.for_duration(deadline_seconds)
    target = target_observed_at or _target_timestamp()
    cex_limits: Dict[str, Semaphore] = {}
    dex_limits: Dict[str, Semaphore] = {}
    completed: Dict[str, Mapping[str, Any]] = {}
    expired: Set[str] = set()
    fixed_blocks: Dict[str, Mapping[str, Any]] = {}
    if dex_block_resolver is not None:
        chains = sorted(
            {
                str(leg.get("chain") or market_id.split(":", 3)[1]).lower()
                for market_id, leg in legs_by_market.items()
                if _market_type(leg) == "dex" and market_id in market_ids
            }
        )
        for chain in chains:
            active_deadline.require_remaining()
            resolved = dex_block_resolver(
                chain, deadline=active_deadline, target_observed_at=target
            )
            if not isinstance(resolved, Mapping) or not isinstance(
                resolved.get("block_number"), int
            ):
                raise ValueError("DEX fixed block is invalid")
            fixed_blocks[chain] = dict(resolved)

    def collect_one(
        market_id: str,
    ) -> Tuple[str, Optional[Mapping[str, Any]], bool]:
        leg = legs_by_market[market_id]
        kind = _market_type(leg)
        if kind == "cex":
            key = str(leg.get("exchange") or market_id.split(":", 2)[1]).lower()
            semaphore = cex_limits.setdefault(key, Semaphore(cex_workers_per_venue))
            collector = cex_collector
        else:
            key = str(leg.get("chain") or market_id.split(":", 3)[1]).lower()
            semaphore = dex_limits.setdefault(key, Semaphore(dex_workers_per_chain))
            collector = dex_collector
        with semaphore:
            try:
                active_deadline.require_remaining()
                kwargs: Dict[str, Any] = {
                    "deadline": active_deadline,
                    "target_observed_at": target,
                }
                if kind == "dex" and key in fixed_blocks:
                    kwargs.update(
                        {
                            "fixed_block_number": fixed_blocks[key]["block_number"],
                            "fixed_block_timestamp": fixed_blocks[key].get(
                                "block_timestamp", ""
                            ),
                        }
                    )
                return market_id, collector(
                    leg, **kwargs
                ), False
            except CollectionDeadlineExceeded:
                return market_id, None, True

    executor = executor_factory(max_workers=max_workers)
    try:
        futures = [executor.submit(collect_one, market_id) for market_id in market_ids]
        for future in as_completed(futures):
            market_id, row, did_expire = future.result()
            if did_expire:
                expired.add(market_id)
            elif row is not None:
                completed[market_id] = row
    finally:
        executor.shutdown(wait=True)

    if source_generation_reader is not None and source_generation_reader() != generation:
        raise ValueError("candidate source generation changed")

    legs = materialize_route_leg_rows(market_ids, completed, deadline_exceeded=expired)
    rows_by_market = {row["market_id"]: row for row in legs}
    route_rows = []
    for route in universe.get("routes", []):
        if not isinstance(route, Mapping):
            raise ValueError("route candidate is invalid")
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
        "target_observed_at": target,
        "legs": legs,
        "routes": sorted(
            (dict(route) for route in universe.get("routes", [])),
            key=lambda row: str(row.get("route_id") or ""),
        ),
        "route_rows": route_rows,
    }
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


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    """Validate a route-universe artifact; Task 5 owns publication."""
    args = parse_args(argv)
    if args.publish:
        raise RuntimeError("--publish is unavailable until Task 5 immutable publication")
    universe = _load_universe_for_cli(args.data_dir)
    generation = universe.get("candidate_source_generation")
    selected = universe.get("selected_legs")
    routes = universe.get("routes")
    if not isinstance(generation, str) or not generation or not isinstance(selected, list) or not isinstance(routes, list):
        raise ValueError("route universe is invalid")
    collect_unique_route_legs(routes)
    if args.dry_run:
        return {
            "dry_run": True,
            "candidate_source_generation": generation,
            "selected_leg_count": len(selected),
            "route_count": len(routes),
        }
    raise RuntimeError(
        "live CLI collection requires explicit collector wiring; use collect_route_cohort()"
    )


if __name__ == "__main__":  # pragma: no cover - command line wrapper
    print(json.dumps(main(), ensure_ascii=False, sort_keys=True))
