"""Pure route-cohort identity and timing helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable, Mapping

try:
    from scripts.timestamp_contract import (
        exact_rfc3339_epoch_seconds,
        exact_timestamp_skew_seconds,
    )
except ModuleNotFoundError:
    from timestamp_contract import (  # type: ignore[no-redef]
        exact_rfc3339_epoch_seconds,
        exact_timestamp_skew_seconds,
    )


_TOKEN_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9._-]*\Z")
_ROUTE_MODE = re.compile(r"[a-z][a-z0-9_]*\Z")


def canonical_route_id(candidate: Mapping[str, Any]) -> str:
    """Return a direction-preserving route identifier."""
    try:
        token_symbol = candidate["token_symbol"]
        buy_market_id = candidate["buy_market_id"]
        sell_market_id = candidate["sell_market_id"]
        route_mode = candidate["route_mode"]
    except (KeyError, TypeError) as error:
        raise ValueError("route candidate identity is invalid") from error
    if not all(
        isinstance(value, str)
        for value in (token_symbol, buy_market_id, sell_market_id, route_mode)
    ):
        raise ValueError("route candidate identity is invalid")
    if (
        not _TOKEN_SYMBOL.fullmatch(token_symbol)
        or token_symbol != token_symbol.strip()
        or any(
            not market_id
            or market_id != market_id.strip()
            or not market_id.startswith(("cex:", "dex:"))
            for market_id in (buy_market_id, sell_market_id)
        )
        or not _ROUTE_MODE.fullmatch(route_mode)
        or route_mode != route_mode.strip()
    ):
        raise ValueError("route candidate identity is invalid")
    if buy_market_id == sell_market_id:
        raise ValueError("route candidate legs must be directional")
    return "route:{}:{}->{}:{}".format(
        token_symbol, buy_market_id, sell_market_id, route_mode
    )


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _unavailable_result(route_id: str, reason_code: str) -> Mapping[str, Any]:
    return {
        "route_id": route_id,
        "skew_seconds": None,
        "timing_status": "unavailable",
        "reason_code": reason_code,
    }


def _has_status(row: Mapping[str, Any], *statuses: str) -> bool:
    return str(row.get("status") or row.get("collection_status") or "") in statuses


def _adapter_is_unsupported(row: Mapping[str, Any]) -> bool:
    return (
        row.get("execution_adapter_supported") is False
        or str(row.get("execution_adapter_status") or "") == "unsupported"
    )


def _leg_is_unavailable(leg: Mapping[str, Any]) -> bool:
    return leg.get("available") is False or _has_status(
        leg, "unavailable", "failed", "timeout", "deadline_exceeded"
    )


def _route_mode_is_unavailable(candidate: Mapping[str, Any]) -> bool:
    return candidate.get("route_mode_not_executable") is True or str(
        candidate.get("route_mode") or ""
    ) in {"research_only", "unsupported", "not_executable"}


def validate_route_cohort_rows(
    candidates: Iterable[Mapping[str, Any]], legs: Iterable[Mapping[str, Any]]
) -> None:
    """Reject duplicate or internally inconsistent route-cohort identities."""
    route_ids = set()
    candidate_ids = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise ValueError("route candidate must be a mapping")
        route_id = canonical_route_id(candidate)
        if candidate.get("route_id") not in (None, "", route_id):
            raise ValueError("route candidate ID is not canonical")
        candidate_id = candidate.get("candidate_id")
        if candidate_id is None or candidate_id == "":
            candidate_id = route_id
        elif (
            not isinstance(candidate_id, str)
            or candidate_id != candidate_id.strip()
        ):
            raise ValueError("route candidate ID is invalid")
        if route_id in route_ids or candidate_id in candidate_ids:
            raise ValueError("duplicate route candidate")
        route_ids.add(route_id)
        candidate_ids.add(candidate_id)

    leg_ids = set()
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise ValueError("route leg must be a mapping")
        leg_id = leg.get("leg_id")
        market_id = leg.get("market_id")
        if not isinstance(leg_id, str) or not leg_id:
            raise ValueError("route leg identity is incomplete")
        if not isinstance(market_id, str) or not market_id:
            raise ValueError("route leg identity is incomplete")
        if leg_id in leg_ids:
            raise ValueError("duplicate route leg")
        leg_ids.add(leg_id)


def classify_route_timing(
    candidate: Mapping[str, Any],
    buy_leg: Mapping[str, Any],
    sell_leg: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Classify one route from the exact state timestamps of its two legs."""
    try:
        skew_limit = Decimal(str(candidate.get("skew_sla_seconds", "60")))
    except (AttributeError, InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("route skew SLA is invalid") from error
    if not skew_limit.is_finite() or skew_limit < 0:
        raise ValueError("route skew SLA is invalid")

    route_id = canonical_route_id(candidate)
    if candidate.get("route_deadline_exceeded") is True or _has_status(
        candidate, "deadline_exceeded"
    ) or _has_status(buy_leg, "deadline_exceeded") or _has_status(
        sell_leg, "deadline_exceeded"
    ):
        return _unavailable_result(route_id, "route_deadline_exceeded")
    if any(
        _adapter_is_unsupported(row) for row in (candidate, buy_leg, sell_leg)
    ):
        return _unavailable_result(route_id, "execution_adapter_unsupported")
    if _leg_is_unavailable(buy_leg):
        return _unavailable_result(route_id, "buy_leg_unavailable")
    if _leg_is_unavailable(sell_leg):
        return _unavailable_result(route_id, "sell_leg_unavailable")
    try:
        buy_state = buy_leg["state_observed_at"]
        sell_state = sell_leg["state_observed_at"]
        buy_epoch = exact_rfc3339_epoch_seconds(buy_state)
        sell_epoch = exact_rfc3339_epoch_seconds(sell_state)
        validated_at = candidate.get("validated_at")
        if validated_at is not None:
            validation_epoch = exact_rfc3339_epoch_seconds(validated_at)
            if buy_epoch > validation_epoch or sell_epoch > validation_epoch:
                return _unavailable_result(route_id, "invalid_state_timestamp")
        skew_seconds = exact_timestamp_skew_seconds(buy_state, sell_state)
    except (KeyError, TypeError, ValueError):
        return _unavailable_result(route_id, "invalid_state_timestamp")
    if skew_seconds > skew_limit:
        return {
            "route_id": route_id,
            "skew_seconds": _decimal_text(skew_seconds),
            "timing_status": "outside_sla",
            "reason_code": "snapshot_skew_exceeded",
        }
    if _route_mode_is_unavailable(candidate):
        return _unavailable_result(route_id, "route_mode_not_executable")
    return {
        "route_id": route_id,
        "skew_seconds": _decimal_text(skew_seconds),
        "timing_status": "within_sla",
        "reason_code": None,
    }
