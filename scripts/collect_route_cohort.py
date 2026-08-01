"""Bounded, route-scoped collection of synchronized market legs."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.route_cohort import classify_route_timing
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
    from route_cohort import classify_route_timing
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


def _target_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _deadline_timestamp(target: str, deadline_seconds: float) -> str:
    try:
        base = datetime.fromisoformat(target.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("target_observed_at is invalid") from error
    if base.tzinfo is None:
        raise ValueError("target_observed_at is invalid")
    return (base.astimezone(timezone.utc) + timedelta(seconds=deadline_seconds)).isoformat().replace("+00:00", "Z")


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
    raw_root: Optional[Path] = None,
    snapshot_id: Optional[str] = None,
    executor_factory: Callable[..., Any] = ThreadPoolExecutor,
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
    if source_generation_reader is None:
        raise ValueError("candidate source generation reader is required")
    if source_generation_reader() != generation:
        raise ValueError("candidate source generation changed")
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

    active_deadline = deadline or CollectionDeadline.for_duration(deadline_seconds)
    target = target_observed_at or _target_timestamp()
    collection_started_at = target
    collection_deadline_at = _deadline_timestamp(target, max(0.0, deadline_seconds))
    root = raw_root or Path("data/raw/route-cohort")
    run_id = snapshot_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root.mkdir(parents=True, exist_ok=True)
    terminal_reasons: Dict[str, str] = {}
    expired: Set[str] = set()
    fixed_blocks: Dict[str, Mapping[str, Any]] = {}
    for chain in sorted({_source_key(legs_by_market[item])[1] for item in market_ids if _market_type(legs_by_market[item]) == "dex"}):
        try:
            active_deadline.require_remaining()
            resolved = dex_block_resolver(chain, deadline=active_deadline)
            if not isinstance(resolved, Mapping) or not isinstance(resolved.get("block_number"), int):
                raise ValueError("invalid fixed block")
            fixed_blocks[chain] = dict(resolved)
        except CollectionDeadlineExceeded:
            _terminal_for_chain(legs_by_market, market_ids, chain, "route_deadline_exceeded", terminal_reasons)
        except Exception:
            _terminal_for_chain(legs_by_market, market_ids, chain, "fixed_block_unavailable", terminal_reasons)

    pending_by_source: Dict[Tuple[str, str], List[str]] = {}
    for market_id in market_ids:
        if market_id not in terminal_reasons:
            pending_by_source.setdefault(_source_key(legs_by_market[market_id]), []).append(market_id)
    for items in pending_by_source.values():
        items.sort()
    source_order = sorted(pending_by_source)
    source_index = 0
    active_by_source: Dict[Tuple[str, str], int] = {}
    completed: Dict[str, Mapping[str, Any]] = {}
    futures: Dict[Any, Tuple[str, Tuple[str, str]]] = {}
    lock = Lock()

    def collect_one(market_id: str) -> Tuple[str, Optional[Mapping[str, Any]], Optional[str]]:
        leg = legs_by_market[market_id]
        kind, source = _source_key(leg)
        raw_path = root / (run_id + "-" + market_id.replace(":", "-").replace("/", "-") + ".json")
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
                    return market_id, None, "fixed_block_lineage_mismatch"
                row = {
                    **dict(row),
                    "fixed_block_number": str(block["block_number"]),
                    "fixed_block_timestamp": str(block.get("block_timestamp") or ""),
                }
            active_deadline.require_remaining()
            return market_id, row, None
        except CollectionDeadlineExceeded:
            return market_id, None, "route_deadline_exceeded"
        except Exception:
            return market_id, None, "collection_failed"

    executor = executor_factory(max_workers=max_workers)
    def submit_fairly() -> None:
        nonlocal source_index
        progressed = True
        while len(futures) < max_workers and progressed:
            progressed = False
            for _unused in range(len(source_order)):
                key = source_order[source_index % len(source_order)]
                source_index += 1
                limit = cex_workers_per_venue if key[0] == "cex" else dex_workers_per_chain
                if pending_by_source[key] and active_by_source.get(key, 0) < limit:
                    market_id = pending_by_source[key].pop(0)
                    future = executor.submit(collect_one, market_id)
                    futures[future] = (market_id, key)
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
                market_id, key = futures.pop(future)
                active_by_source[key] -= 1
                if active_deadline.remaining_seconds() <= 0:
                    expired.add(market_id)
                    continue
                returned_id, row, reason = future.result()
                if reason == "route_deadline_exceeded":
                    expired.add(returned_id)
                elif reason:
                    terminal_reasons[returned_id] = reason
                elif row is not None:
                    completed[returned_id] = row
            submit_fairly()
    finally:
        for future, (market_id, _key) in futures.items():
            future.cancel()
            expired.add(market_id)
        for items in pending_by_source.values():
            expired.update(items)
        executor.shutdown(wait=False)

    if source_generation_reader() != generation:
        raise ValueError("candidate source generation changed")
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
        "collection_started_at": collection_started_at,
        "collection_deadline_at": collection_deadline_at,
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": dict(universe.get("selection_window") or {}),
        "requested_notionals_usd": list(universe.get("requested_notionals_usd") or []),
        "legs": legs,
        "routes": sorted(
            (dict(route) for route in universe.get("routes", [])),
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
    if (args.deadline_seconds <= 0 or args.max_workers < 1
            or args.cex_workers_per_venue < 1 or args.dex_workers_per_chain < 1):
        raise ValueError("deadline and worker limits must be positive")
    return _cli_tokens(args.tokens)


def _validated_universe(universe: Mapping[str, Any], tokens: Optional[Set[str]]) -> Dict[str, Any]:
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
    filtered_routes = [dict(route) for route in routes if isinstance(route, Mapping) and (
        tokens is None or route.get("token_symbol") in tokens
    )]
    if not filtered_routes:
        raise ValueError("requested tokens have no routes")
    ids = set(collect_unique_route_legs(filtered_routes))
    if not ids <= set(selected_by_id):
        raise ValueError("route references an unselected leg")
    filtered_legs = [selected_by_id[item] for item in sorted(ids)]
    return {**dict(universe), "selected_legs": filtered_legs, "routes": filtered_routes}


def _resolve_inventory_legs(universe: Mapping[str, Any], data_dir: Path) -> Dict[str, Any]:
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
    for selected_leg in selected:
        identity = selected_leg["market_id"]
        legs.append({**dict(resolved[identity]), **dict(selected_leg)})
    return {**dict(universe), "selected_legs": legs}


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
    universe = _validated_universe(_load_universe_for_cli(args.data_dir), tokens)
    if args.dry_run:
        return {
            "dry_run": True,
            "candidate_source_generation": universe["candidate_source_generation"],
            "selected_leg_count": len(universe["selected_legs"]),
            "route_count": len(universe["routes"]),
        }
    resolved = _resolve_inventory_legs(universe, args.data_dir)
    result = collect_route_cohort(
        resolved,
        cex_collector=cex_collector,
        dex_collector=dex_collector,
        dex_block_resolver=dex_block_resolver,
        source_generation_reader=lambda: _load_universe_for_cli(args.data_dir).get(
            "candidate_source_generation"
        ),
        raw_root=args.data_dir / "raw" / "route-cohort",
        deadline_seconds=args.deadline_seconds,
        max_workers=args.max_workers,
        cex_workers_per_venue=args.cex_workers_per_venue,
        dex_workers_per_chain=args.dex_workers_per_chain,
    )
    return {**result, "dry_run": False, "universe_path": str(universe_path)}


if __name__ == "__main__":  # pragma: no cover - command line wrapper
    print(json.dumps(main(), ensure_ascii=False, sort_keys=True))
