"""Build a deterministic, bounded, evidence-only route candidate universe."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    from scripts.route_cohort import canonical_route_id
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from route_cohort import canonical_route_id  # type: ignore[no-redef]
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore[no-redef]


ROUTE_UNIVERSE_SCHEMA = "route_universe/v1"
REQUESTED_NOTIONALS_USD = (1000, 5000, 10000, 50000, 100000)
_MARKET_TYPES = ("cex", "dex")
_CAPABILITY_ORDER = {"unsupported": 0, "supported": 1, "proved": 2}
_ROUTE_VOLUME_BASIS = "minimum_leg_source_horizon_usd"


def _valid_timestamp(value: Any) -> bool:
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError):
        return False
    return True


def _positive_decimal(value: Any) -> Optional[Decimal]:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite() or result <= 0:
        return None
    return result


def _non_negative_decimal(value: Any) -> Optional[Decimal]:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite() or result < 0:
        return None
    return result


def _decimal_text(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if value == 0:
        return "0"
    return format(value, "f")


def _canonical_decimal_text(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _market_id(row: Mapping[str, Any]) -> Optional[str]:
    value = row.get("market_id")
    if not isinstance(value, str) or not value:
        return None
    return value


def execution_capability_by_market(
    execution_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Optional[str]]]:
    """Return per-market supported/proved capability from current execution facts.

    A proved capacity is the lower of the largest current observed buy and sell
    scenario.  Rows with invalid state timestamps do not prove capability.
    """
    rows_by_market = defaultdict(list)
    for row in execution_rows:
        if isinstance(row, Mapping):
            market_id = _market_id(row)
            if market_id is not None:
                rows_by_market[market_id].append(row)

    results = {}
    for market_id in sorted(rows_by_market):
        current_rows = [
            row for row in rows_by_market[market_id]
            if _valid_timestamp(row.get("state_observed_at"))
        ]
        observed_by_direction = {"buy_token": [], "sell_token": []}
        has_supported_adapter_evidence = False
        has_unsupported_evidence = False
        for row in current_rows:
            status = str(row.get("status") or "")
            if status == "unsupported" or row.get("execution_adapter_supported") is False:
                has_unsupported_evidence = True
                continue
            if status in ("observed", "partial", "failed"):
                has_supported_adapter_evidence = True
            if status == "observed":
                direction = row.get("direction")
                amount = _positive_decimal(row.get("requested_notional_usd"))
                if direction in observed_by_direction and amount is not None:
                    observed_by_direction[direction].append(amount)

        if has_unsupported_evidence:
            result = {
                "execution_capability": "unsupported",
                "proved_execution_capacity_usd": None,
            }
        elif observed_by_direction["buy_token"] and observed_by_direction["sell_token"]:
            capacity = min(
                max(observed_by_direction["buy_token"]),
                max(observed_by_direction["sell_token"]),
            )
            result = {
                "execution_capability": "proved",
                "proved_execution_capacity_usd": _decimal_text(capacity),
            }
        elif has_supported_adapter_evidence:
            result = {
                "execution_capability": "supported",
                "proved_execution_capacity_usd": None,
            }
        else:
            result = {
                "execution_capability": "unsupported",
                "proved_execution_capacity_usd": None,
            }
        results[market_id] = result
    return results


def _latest_current_numeric(
    rows: Iterable[Mapping[str, Any]], field: str
) -> Dict[str, Decimal]:
    """Return one deterministic current numeric value per market.

    Duplicate rows are resolved by their timestamp text then value text.  This
    keeps selection order independent of source iterable order without making a
    freshness claim beyond timestamp validity.
    """
    candidates = defaultdict(list)
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        market_id = _market_id(row)
        amount = _non_negative_decimal(row.get(field))
        timestamp = row.get("observed_at")
        if market_id is None or amount is None or not _valid_timestamp(timestamp):
            continue
        candidates[market_id].append((str(timestamp), amount))
    result = {}
    for market_id, values in candidates.items():
        _timestamp, amount = max(values, key=lambda item: (item[0], item[1]))
        result[market_id] = amount
    return result


def _current_depth_by_market(
    depth_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Decimal]:
    candidates = defaultdict(list)
    for row in depth_rows:
        if not isinstance(row, Mapping):
            continue
        market_id = _market_id(row)
        amount = _positive_decimal(row.get("total_depth_100bps_usd"))
        timestamp = row.get("state_observed_at")
        if market_id is not None and market_id.startswith("cex:"):
            directional_amounts = (
                _positive_decimal(row.get("bid_depth_100bps_usd")),
                _positive_decimal(row.get("ask_depth_100bps_usd")),
            )
        elif market_id is not None and market_id.startswith("dex:"):
            directional_amounts = (
                _positive_decimal(row.get("buy_depth_100bps_usd")),
                _positive_decimal(row.get("sell_depth_100bps_usd")),
            )
        else:
            directional_amounts = (None, None)
        if (
            market_id is None
            or str(row.get("status") or "") != "observed"
            or amount is None
            or any(value is None for value in directional_amounts)
            or not _valid_timestamp(timestamp)
        ):
            continue
        candidates[market_id].append((str(timestamp), amount))
    result = {}
    for market_id, values in candidates.items():
        _timestamp, amount = max(values, key=lambda item: (item[0], item[1]))
        result[market_id] = amount
    return result


def _candidate_is_usable(row: Mapping[str, Any]) -> bool:
    market_id = _market_id(row)
    market_type = row.get("market_type")
    if market_id is None or market_type not in _MARKET_TYPES:
        return False
    if not market_id.startswith(str(market_type) + ":"):
        return False
    if not isinstance(row.get("token_symbol"), str) or not row["token_symbol"]:
        return False
    if not _valid_timestamp(row.get("observed_at")):
        return False
    if row.get("lifecycle_withheld") is True:
        return False
    if str(row.get("lifecycle_status") or "active") in {
        "withheld", "inactive", "unavailable", "delisted"
    }:
        return False
    if row.get("execution_adapter_supported") is False:
        return False
    if str(row.get("execution_adapter_status") or "supported") == "unsupported":
        return False
    return True


def _selection_key(row: Mapping[str, Any]) -> Tuple[int, Decimal, Decimal, Decimal, Decimal, str]:
    inputs = row["selection_inputs"]
    capability = str(inputs["execution_capability"])
    capacity = _positive_decimal(inputs["proved_execution_capacity_usd"]) or Decimal(0)
    depth = _positive_decimal(inputs["observed_100bps_depth_usd"]) or Decimal(0)
    cex_volume = _non_negative_decimal(inputs["cex_selected_window_usd"])
    dex_volume = _non_negative_decimal(inputs["dex_24h_usd"])
    dex_tvl = _non_negative_decimal(inputs["dex_tvl_usd"])
    liquidity = capacity if capacity > 0 else depth
    return (
        -_CAPABILITY_ORDER[capability], -liquidity,
        -(cex_volume or Decimal(0)),
        -(dex_volume or Decimal(0)), -(dex_tvl or Decimal(0)),
        str(row["market_id"]),
    )


def _reference_volume_usd(leg: Mapping[str, Any]) -> Optional[Decimal]:
    inputs = leg["selection_inputs"]
    if leg["market_type"] == "cex":
        return _non_negative_decimal(inputs["cex_selected_window_usd"])
    return _non_negative_decimal(inputs["dex_24h_usd"])


def select_route_legs(
    catalog: Iterable[Mapping[str, Any]],
    depth_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    cex_volume_rows: Iterable[Mapping[str, Any]],
    dex_volume_rows: Iterable[Mapping[str, Any]],
    tvl_rows: Iterable[Mapping[str, Any]],
    *,
    selection_window: Mapping[str, Any],
    candidate_source_generation: str,
) -> List[Dict[str, Any]]:
    """Select at most three usable canonical legs of each type for every Token."""
    if not isinstance(candidate_source_generation, str) or not candidate_source_generation:
        raise ValueError("candidate source generation is invalid")
    if not isinstance(selection_window, Mapping):
        raise ValueError("selection window is invalid")

    capabilities = execution_capability_by_market(execution_rows)
    depths = _current_depth_by_market(depth_rows)
    cex_volumes = _latest_current_numeric(cex_volume_rows, "selected_window_usd")
    dex_volumes = _latest_current_numeric(dex_volume_rows, "volume_24h_usd")
    dex_tvls = _latest_current_numeric(tvl_rows, "tvl_usd")
    candidates = {}
    catalog_market_ids = set()
    for row in catalog:
        if isinstance(row, Mapping):
            market_id = _market_id(row)
            if market_id is not None:
                if market_id in catalog_market_ids:
                    raise ValueError("duplicate canonical market ID")
                catalog_market_ids.add(market_id)
        if not isinstance(row, Mapping) or not _candidate_is_usable(row):
            continue
        market_id = _market_id(row)
        assert market_id is not None
        if market_id not in depths:
            continue
        capability = capabilities.get(market_id, {
            "execution_capability": "unsupported",
            "proved_execution_capacity_usd": None,
        })
        if capability["execution_capability"] == "unsupported":
            continue
        candidates[market_id] = {
            "market_id": market_id,
            "market_type": row["market_type"],
            "token_symbol": row["token_symbol"],
            "candidate_source_generation": candidate_source_generation,
            "selection_window": dict(selection_window),
            "selection_inputs": {
                "execution_capability": capability["execution_capability"],
                "proved_execution_capacity_usd": capability[
                    "proved_execution_capacity_usd"
                ],
                "observed_100bps_depth_usd": _decimal_text(depths[market_id]),
                "cex_selected_window_usd": _decimal_text(cex_volumes.get(market_id)),
                "dex_24h_usd": _decimal_text(dex_volumes.get(market_id)),
                "dex_tvl_usd": _decimal_text(dex_tvls.get(market_id)),
            },
        }

    grouped = defaultdict(list)
    for row in candidates.values():
        grouped[(row["token_symbol"], row["market_type"])].append(row)
    selected = []
    for token_symbol, market_type in sorted(grouped):
        ranked = sorted(grouped[(token_symbol, market_type)], key=_selection_key)
        for rank, row in enumerate(ranked[:3], start=1):
            selected.append({**row, "selection_rank": rank})
    return selected


def _route_mode(buy_leg: Mapping[str, Any], sell_leg: Mapping[str, Any]) -> Tuple[str, str, Optional[str]]:
    if buy_leg["market_type"] == "dex" and sell_leg["market_type"] == "dex":
        buy_chain = str(buy_leg["market_id"]).split(":", 3)[1]
        sell_chain = str(sell_leg["market_id"]).split(":", 3)[1]
        if buy_chain != sell_chain:
            return "research_only", "research_only", "unsupported_cross_chain_settlement"
        return "atomic_onchain", "candidate", None
    return "prepositioned_inventory", "candidate", None


def strict_cost_route_classification(
    leg: Mapping[str, Any],
    *,
    selected_market_ids: Iterable[str],
    structurally_supported_market_ids: Iterable[str],
) -> Tuple[str, Optional[str]]:
    """Classify a DEX leg from frozen structural scope, never live outcomes."""
    if not isinstance(leg, Mapping):
        raise ValueError("strict-cost leg is invalid")
    market_id = leg.get("market_id")
    if not isinstance(market_id, str) or leg.get("market_type") != "dex":
        raise ValueError("strict-cost DEX leg identity is invalid")
    selected = set(selected_market_ids)
    supported = set(structurally_supported_market_ids)
    if market_id in selected and market_id in supported:
        return "candidate", None
    if market_id in supported:
        return "research_only", "cost_adapter_cohort_capacity"
    return "research_only", "strict_cost_adapter_unsupported"


def build_route_universe(
    catalog: Iterable[Mapping[str, Any]],
    depth_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    cex_volume_rows: Iterable[Mapping[str, Any]],
    dex_volume_rows: Iterable[Mapping[str, Any]],
    tvl_rows: Iterable[Mapping[str, Any]],
    *,
    selection_window: Mapping[str, Any],
    candidate_source_generation: str,
) -> Dict[str, Any]:
    """Build all directional routes from the deterministic bounded leg universe."""
    selected_legs = select_route_legs(
        catalog, depth_rows, execution_rows, cex_volume_rows, dex_volume_rows,
        tvl_rows, selection_window=selection_window,
        candidate_source_generation=candidate_source_generation,
    )
    ordered_legs = sorted(
        selected_legs,
        key=lambda row: (row["token_symbol"], row["market_type"], row["selection_rank"], row["market_id"]),
    )
    routes = []
    by_token = defaultdict(list)
    for leg in ordered_legs:
        by_token[leg["token_symbol"]].append(leg)
    for token_symbol in sorted(by_token):
        token_legs = sorted(by_token[token_symbol], key=lambda row: row["market_id"])
        for buy_leg in token_legs:
            for sell_leg in token_legs:
                if buy_leg["market_id"] == sell_leg["market_id"]:
                    continue
                route_mode, route_class, settlement_reason = _route_mode(buy_leg, sell_leg)
                identity = {
                    "token_symbol": token_symbol,
                    "buy_market_id": buy_leg["market_id"],
                    "sell_market_id": sell_leg["market_id"],
                    "route_mode": route_mode,
                }
                buy_reference_volume = _reference_volume_usd(buy_leg)
                sell_reference_volume = _reference_volume_usd(sell_leg)
                route_volume = (
                    min(buy_reference_volume, sell_reference_volume)
                    if buy_reference_volume is not None
                    and sell_reference_volume is not None
                    else None
                )
                routes.append({
                    **identity,
                    "route_id": canonical_route_id(identity),
                    "route_class": route_class,
                    "settlement_reason": settlement_reason,
                    "requested_notionals_usd": list(REQUESTED_NOTIONALS_USD),
                    "candidate_source_generation": candidate_source_generation,
                    "buy_reference_volume_usd": _canonical_decimal_text(
                        buy_reference_volume
                    ),
                    "sell_reference_volume_usd": _canonical_decimal_text(
                        sell_reference_volume
                    ),
                    "route_volume_usd": _canonical_decimal_text(route_volume),
                    "route_volume_basis": _ROUTE_VOLUME_BASIS,
                })
    routes.sort(key=lambda row: row["route_id"])
    return {
        "schema": ROUTE_UNIVERSE_SCHEMA,
        "candidate_source_generation": candidate_source_generation,
        "selection_window": dict(selection_window),
        "requested_notionals_usd": list(REQUESTED_NOTIONALS_USD),
        "selected_legs": ordered_legs,
        "routes": routes,
    }


def route_universe_sha256(universe: Mapping[str, Any]) -> str:
    """Return the SHA-256 of canonical UTF-8 route-universe JSON bytes."""
    if not isinstance(universe, Mapping):
        raise ValueError("route universe is invalid")
    return hashlib.sha256(_canonical_json_bytes(universe)).hexdigest()
