"""Fixed, pure inputs for the public UNI/USDT CEX research workflow."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

try:
    from scripts.route_cohort import canonical_route_id
    from scripts.route_universe import (
        REQUESTED_NOTIONALS_USD,
        ROUTE_UNIVERSE_SCHEMA,
    )
except ModuleNotFoundError:
    from route_cohort import canonical_route_id  # type: ignore[no-redef]
    from route_universe import (  # type: ignore[no-redef]
        REQUESTED_NOTIONALS_USD,
        ROUTE_UNIVERSE_SCHEMA,
    )


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
