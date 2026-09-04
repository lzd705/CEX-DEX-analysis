"""Fixed, pure inputs for the public UNI/USDT CEX research workflow."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping

try:
    from scripts.execution_cost_components import validate_cost_components
    from scripts.route_quantity import FeeSemantics, MarketRules
    from scripts.route_cohort import canonical_route_id
    from scripts.route_universe import (
        REQUESTED_NOTIONALS_USD,
        ROUTE_UNIVERSE_SCHEMA,
    )
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from execution_cost_components import validate_cost_components  # type: ignore
    from route_quantity import FeeSemantics, MarketRules  # type: ignore
    from route_cohort import canonical_route_id  # type: ignore[no-redef]
    from route_universe import (  # type: ignore[no-redef]
        REQUESTED_NOTIONALS_USD,
        ROUTE_UNIVERSE_SCHEMA,
    )
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


_SELECTION_WINDOW = {"start": "2026-09-04", "end": "2026-09-04"}
_CEX_MARKETS = (
    ("binance", "cex:binance:UNI/USDT"),
    ("bybit", "cex:bybit:UNI/USDT"),
)
_ROUTE_VOLUME_BASIS = "minimum_leg_source_horizon_usd"


def _canonical_json_bytes(value: Dict[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def public_fee_semantics(
    component: Mapping[str, Any],
    *,
    direction: str,
    rules: MarketRules,
    now: str,
) -> FeeSemantics:
    """Bind a public fee component to conservative quote mechanics."""
    if not isinstance(component, Mapping):
        raise ValueError("public fee component must be a mapping")
    validate_cost_components([component])
    if direction not in {"buy", "sell"}:
        raise ValueError("public fee direction must be buy or sell")
    if not isinstance(rules, MarketRules):
        raise ValueError("public fee market rules are invalid")
    if (
        component.get("leg") != direction
        or component.get("direction")
        != ("buy_token" if direction == "buy" else "sell_token")
        or component.get("market_id") != rules.market_id
        or component.get("component_type") != "venue_taker_fee"
        or component.get("strict_eligible") is not False
        or component.get("embedded_in_leg_quote") is not False
    ):
        raise ValueError("public fee component identity is invalid")

    try:
        evaluated = exact_rfc3339_epoch_seconds(now)
        rules_observed = exact_rfc3339_epoch_seconds(rules.observed_at)
        rules_valid = exact_rfc3339_epoch_seconds(rules.valid_until)
    except (TypeError, ValueError) as error:
        raise ValueError("public fee evaluation time is invalid") from error
    if not rules_observed <= evaluated < rules_valid:
        raise ValueError("public fee market rules do not cover evaluation time")

    status = component.get("value_status")
    if status == "bounded_estimate":
        try:
            rate = Decimal(component["rate_bps"])
            observed_at = component["observed_at"]
            valid_until = component["valid_until"]
            source_hash = component["source_record_sha256"]
            if not (
                exact_rfc3339_epoch_seconds(observed_at)
                <= evaluated
                < exact_rfc3339_epoch_seconds(valid_until)
            ):
                raise ValueError("public fee bound does not cover evaluation time")
        except (InvalidOperation, KeyError, TypeError, ValueError) as error:
            raise ValueError("public fee bound is invalid") from error
    elif status == "unavailable":
        rate = Decimal(0)
        observed_at = rules.observed_at
        valid_until = rules.valid_until
        source_hash = hashlib.sha256(
            _canonical_json_bytes(dict(component))
        ).hexdigest()
    else:
        raise ValueError("public fee component must be bounded or unavailable")

    return FeeSemantics(
        rate_bps=rate,
        fee_asset=(rules.base_asset if direction == "buy" else rules.quote_asset),
        charge_basis=(
            "received_base" if direction == "buy" else "received_quote"
        ),
        fee_increment=(
            rules.base_increment if direction == "buy" else rules.quote_increment
        ),
        rounding_mode="ceiling",
        third_asset_quote_price=None,
        observed_at=observed_at,
        valid_until=valid_until,
        source_record_sha256=source_hash,
        conversion_source_record_sha256=None,
    )


def _fixed_universe_contract() -> Dict[str, Any]:
    selected_legs = []
    for rank, (exchange, market_id) in enumerate(_CEX_MARKETS, start=1):
        selected_legs.append({
            "market_id": market_id,
            "market_type": "cex",
            "exchange": exchange,
            "cex_symbol": "UNI/USDT",
            "token_symbol": "UNI",
            "selection_inputs": {
                "execution_capability": "supported",
                "proved_execution_capacity_usd": None,
                "observed_100bps_depth_usd": None,
                "cex_selected_window_usd": None,
                "dex_24h_usd": None,
                "dex_tvl_usd": None,
            },
            "selection_rank": rank,
            "execution_adapter_supported": True,
            "execution_adapter_status": "supported",
        })

    routes = []
    for _buy_exchange, buy_market_id in _CEX_MARKETS:
        for _sell_exchange, sell_market_id in _CEX_MARKETS:
            if buy_market_id == sell_market_id:
                continue
            identity = {
                "token_symbol": "UNI",
                "buy_market_id": buy_market_id,
                "sell_market_id": sell_market_id,
                "route_mode": "prepositioned_inventory",
            }
            routes.append({
                **identity,
                "route_id": canonical_route_id(identity),
                "route_class": "candidate",
                "settlement_reason": None,
                "requested_notionals_usd": list(REQUESTED_NOTIONALS_USD),
                "buy_reference_volume_usd": None,
                "sell_reference_volume_usd": None,
                "route_volume_usd": None,
                "route_volume_basis": _ROUTE_VOLUME_BASIS,
            })
    routes.sort(key=lambda route: route["route_id"])
    return {
        "schema": ROUTE_UNIVERSE_SCHEMA,
        "selection_window": dict(_SELECTION_WINDOW),
        "requested_notionals_usd": list(REQUESTED_NOTIONALS_USD),
        "selected_legs": selected_legs,
        "routes": routes,
    }


def live_cex_research_generation() -> str:
    """Return the canonical generation for the fixed public research inputs."""
    return hashlib.sha256(
        _canonical_json_bytes(_fixed_universe_contract())
    ).hexdigest()


def build_live_cex_research_universe() -> Dict[str, Any]:
    """Build the fixed Binance/Bybit UNI/USDT point-in-time universe."""
    generation = live_cex_research_generation()
    universe = _fixed_universe_contract()
    universe["candidate_source_generation"] = generation
    for leg in universe["selected_legs"]:
        leg["candidate_source_generation"] = generation
        leg["selection_window"] = dict(universe["selection_window"])
    for route in universe["routes"]:
        route["candidate_source_generation"] = generation
    return universe
