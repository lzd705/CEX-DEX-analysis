#!/usr/bin/env python3
"""Fail closed when a deployed dashboard violates its public fact contract."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from scripts.cex_instrument_lifecycle import (
        configured_market_ids_sha256,
    )
    from scripts.token_registry import (
        canonical_cex_market_ids,
        cex_market_ids_sha256,
    )
    from scripts.quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
    )
    from scripts.route_publication import (
        DEFAULT_ROUTE_ROOT,
        RoutePublicationError,
        load_latest_complete_route_bundle,
    )
    from scripts.execution_cost_components import validate_cost_components
    from scripts.route_opportunity import (
        MAX_ROUTE_AGE_SECONDS,
        MAX_ROUTE_SKEW_SECONDS,
    )
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
    from scripts.event_facts import effective_datetime_interval
    from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_FILENAMES
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cex_instrument_lifecycle import configured_market_ids_sha256
    from token_registry import canonical_cex_market_ids, cex_market_ids_sha256
    from quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
    )
    from route_publication import (  # type: ignore[no-redef]
        DEFAULT_ROUTE_ROOT,
        RoutePublicationError,
        load_latest_complete_route_bundle,
    )
    from execution_cost_components import (  # type: ignore[no-redef]
        validate_cost_components,
    )
    from route_opportunity import (  # type: ignore[no-redef]
        MAX_ROUTE_AGE_SECONDS,
        MAX_ROUTE_SKEW_SECONDS,
    )
    from timestamp_contract import (  # type: ignore[no-redef]
        exact_rfc3339_epoch_seconds,
    )
    from event_facts import effective_datetime_interval
    from static_asset_contract import PUBLIC_STATIC_ASSET_FILENAMES


@dataclass(frozen=True)
class ResponseMetrics:
    path: str
    elapsed_ms: float
    wire_bytes: int
    raw_bytes: int
    compressed: bool


class ReleaseCheckError(RuntimeError):
    """One release contract or request failed."""


STATIC_ASSET_FILENAMES = PUBLIC_STATIC_ASSET_FILENAMES
MAX_STATIC_ASSET_BYTES = 4 * 1024 * 1024


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReleaseCheckError(message)


_ROUTE_STRICT_VALUE_STATUSES = frozenset(
    {"measured", "authenticated", "quoted", "not_applicable"}
)
_ROUTE_SCENARIO_VALUE_STATUSES = frozenset(
    {"bounded_estimate", "assumed"}
)
_ROUTE_TERMINAL_VALUE_STATUSES = frozenset(
    {"unavailable", "unsupported", "failed", "stale"}
)


def _route_decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(
        value, (str, int, Decimal)
    ):
        raise ReleaseCheckError("{} is not exact decimal evidence".format(field))
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "{} is not exact decimal evidence".format(field)
        ) from error
    if not result.is_finite() or (positive and result <= 0):
        raise ReleaseCheckError("{} is not valid decimal evidence".format(field))
    return result


def _route_timestamp(value: Any, field: str) -> Decimal:
    if not isinstance(value, str) or not value:
        raise ReleaseCheckError("{} timestamp is unavailable".format(field))
    try:
        return exact_rfc3339_epoch_seconds(value)
    except (OverflowError, ValueError) as error:
        raise ReleaseCheckError("{} timestamp is invalid".format(field)) from error


def _route_integer(value: Any, field: str, *, positive: bool = False) -> int:
    number = _route_decimal(value, field, positive=positive)
    if number != number.to_integral_value():
        raise ReleaseCheckError("{} is not exact integer evidence".format(field))
    return int(number)


def _route_canonical_sha256(value: Any) -> str:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError("route generation evidence is invalid") from error
    return hashlib.sha256(payload).hexdigest()


def _route_publication_sha256(value: Any) -> str:
    """Reproduce Task 7 input-generation and cost-set canonical bytes."""
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError("route publication evidence is invalid") from error
    return hashlib.sha256(payload).hexdigest()


def _route_rounded_ratio(value: Fraction, places: int = 8) -> str:
    sign = -1 if value < 0 else 1
    absolute = abs(value)
    scale = 10**places
    quotient, remainder = divmod(
        absolute.numerator * scale,
        absolute.denominator,
    )
    if remainder * 2 > absolute.denominator or (
        remainder * 2 == absolute.denominator and quotient % 2
    ):
        quotient += 1
    quotient *= sign
    if quotient == 0:
        return "0"
    digits = str(abs(quotient)).rjust(places + 1, "0")
    text = digits[:-places] + "." + digits[-places:]
    text = text.rstrip("0").rstrip(".")
    return ("-" if quotient < 0 else "") + text


def _route_ratio_fields(edge: Fraction, buy_cost: Fraction) -> tuple[str, str, str]:
    ratio = edge * 10_000 / buy_cost
    return (
        _route_rounded_ratio(ratio),
        str(ratio.numerator),
        str(ratio.denominator),
    )


def _route_expected_component_keys(
    route: Mapping[str, Any],
) -> set[tuple[str, str]]:
    expected = {("route", "rebalancing_or_transfer")}
    for leg in ("buy", "sell"):
        market_id = str(route.get(leg + "_market_id", ""))
        if market_id.startswith("cex:"):
            expected.add((leg, "venue_taker_fee"))
        elif market_id.startswith("dex:"):
            expected.update({
                (leg, "pool_swap_fee"),
                (leg, "network_gas"),
                (leg, "router_or_integrator_fee"),
                (leg, "token_transfer_tax"),
            })
        else:
            raise ReleaseCheckError("route market type is unsupported")
    return expected


def _route_cost_is_current(
    row: Mapping[str, Any], now_epoch: Decimal
) -> bool:
    if row.get("value_status") == "not_applicable":
        return row.get("observed_at") is None and row.get("valid_until") is None
    observed_at = row.get("observed_at")
    if observed_at is None:
        return row.get("value_status") in _ROUTE_SCENARIO_VALUE_STATUSES
    observed_epoch = _route_timestamp(observed_at, "cost observed_at")
    if observed_epoch > now_epoch:
        return False
    valid_until = row.get("valid_until")
    if valid_until is not None:
        return now_epoch < _route_timestamp(valid_until, "cost valid_until")
    return Fraction(now_epoch - observed_epoch) <= MAX_ROUTE_AGE_SECONDS


def _route_not_applicable_is_proved(
    row: Mapping[str, Any], route: Mapping[str, Any]
) -> bool:
    component_type = row.get("component_type")
    if component_type == "rebalancing_or_transfer":
        return (
            row.get("leg") == "route"
            and route.get("route_mode") in {
                "prepositioned_inventory", "atomic_onchain"
            }
            and row.get("source") == "validated route topology"
        )
    if component_type in {"router_or_integrator_fee", "token_transfer_tax"}:
        return (
            row.get("leg") in {"buy", "sell"}
            and row.get("source") == "validated route adapter contract"
            and isinstance(row.get("source_record_sha256"), str)
        )
    return False


def _route_cost_inventory(
    bundle: Mapping[str, Any],
    *,
    now_epoch: Decimal,
) -> dict[str, list[Mapping[str, Any]]]:
    rows = bundle.get("cost_components")
    opportunities = bundle.get("opportunities")
    routes = bundle.get("routes")
    if not isinstance(rows, list) or not isinstance(opportunities, list):
        raise ReleaseCheckError("route cost inventory is unavailable")
    if not isinstance(routes, list):
        raise ReleaseCheckError("route inventory is unavailable")
    try:
        validate_cost_components(rows)
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError(str(error)) from error

    opportunity_ids = {
        str(row.get("opportunity_id")) for row in opportunities
        if isinstance(row, Mapping)
    }
    by_opportunity: dict[str, list[Mapping[str, Any]]] = {}
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        opportunity_id = str(row.get("opportunity_id"))
        if opportunity_id not in opportunity_ids:
            raise ReleaseCheckError("orphan cost component is not publishable")
        key = (
            opportunity_id,
            str(row.get("leg")),
            str(row.get("component_type")),
        )
        if key in seen:
            raise ReleaseCheckError("duplicate cost component is not publishable")
        seen.add(key)
        if (
            row.get("component_type") == "network_gas"
            and row.get("value_status") in _ROUTE_STRICT_VALUE_STATUSES
            and row.get("amount_usd") is not None
            and _route_decimal(row["amount_usd"], "network gas amount") == 0
        ):
            raise ReleaseCheckError("fake zero gas cost is not publishable")
        if row.get("value_status") in _ROUTE_TERMINAL_VALUE_STATUSES and (
            row.get("amount_usd") is not None or row.get("rate_bps") is not None
        ):
            raise ReleaseCheckError(
                "missing or unsupported cost must remain null"
            )
        by_opportunity.setdefault(opportunity_id, []).append(row)

    routes_by_id = {
        str(route.get("route_id")): route
        for route in routes
        if isinstance(route, Mapping)
    }
    for opportunity in opportunities:
        opportunity_id = str(opportunity.get("opportunity_id"))
        route = routes_by_id.get(str(opportunity.get("route_id")))
        if route is None:
            raise ReleaseCheckError("route opportunity has no exact route")
        component_rows = by_opportunity.get(opportunity_id, [])
        actual = {
            (str(row.get("leg")), str(row.get("component_type")))
            for row in component_rows
            if row.get("component_type") != "mev_buffer"
        }
        expected = _route_expected_component_keys(route)
        if expected - actual:
            raise ReleaseCheckError("missing cost component is not publishable")
        if actual - expected:
            raise ReleaseCheckError("unexpected cost component is not publishable")
        for row in component_rows:
            if row.get("requested_notional_usd") != opportunity.get(
                "requested_notional_usd"
            ):
                raise ReleaseCheckError("cost component notional conflicts")
            if row.get("target_token_quantity") != opportunity.get(
                "target_token_quantity"
            ):
                raise ReleaseCheckError("cost component quantity conflicts")
            if row.get("value_status") == "not_applicable" and not (
                _route_not_applicable_is_proved(row, route)
            ):
                raise ReleaseCheckError(
                    "not-applicable cost lacks route proof"
                )
            if (
                opportunity.get("strict_eligible") is True
                and row.get("strict_eligible") is True
                and not _route_cost_is_current(row, now_epoch)
            ):
                raise ReleaseCheckError("stale cost evidence is not strict")
    return by_opportunity


def _route_now_epoch(now: Any) -> tuple[Decimal, str]:
    if now is None:
        current = datetime.now(timezone.utc)
        text = current.isoformat().replace("+00:00", "Z")
        return _route_timestamp(text, "route validation"), text
    if isinstance(now, datetime):
        if now.tzinfo is None or now.utcoffset() is None:
            raise ReleaseCheckError("route validation timestamp is naive")
        text = now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return _route_timestamp(text, "route validation"), text
    if isinstance(now, str):
        return _route_timestamp(now, "route validation"), now
    raise ReleaseCheckError("route validation timestamp is invalid")


def _route_assert_ratio(
    row: Mapping[str, Any],
    *,
    prefix: str,
    edge: Fraction,
    buy_cost: Fraction,
) -> None:
    expected = _route_ratio_fields(edge, buy_cost)
    actual = (
        row.get(prefix + "_bps"),
        row.get(prefix + "_bps_numerator"),
        row.get(prefix + "_bps_denominator"),
    )
    if actual != expected:
        raise ReleaseCheckError("{} bps arithmetic does not reproduce".format(prefix))


def _validate_route_opportunity_row(
    row: Mapping[str, Any],
    *,
    route: Mapping[str, Any],
    legs_by_market: Mapping[str, Mapping[str, Any]],
    components: Iterable[Mapping[str, Any]],
    core_manifest_sha256: str,
    now_epoch: Decimal,
) -> None:
    provided = dict(row)
    evidence_binding = provided.pop("evidence_binding_sha256", None)
    if evidence_binding != _route_canonical_sha256(provided):
        raise ReleaseCheckError("opportunity evidence binding does not reproduce")
    if any(
        row.get(field) != route.get(field)
        for field in (
            "route_id", "token_symbol", "buy_market_id", "sell_market_id",
            "route_mode",
        )
    ):
        raise ReleaseCheckError("route opportunity lineage does not reproduce")
    if (
        row.get("buy_core_manifest_sha256") != core_manifest_sha256
        or row.get("sell_core_manifest_sha256") != core_manifest_sha256
    ):
        raise ReleaseCheckError("route opportunity core binding conflicts")
    strict = row.get("strict_eligible") is True
    ready = row.get("strict_ready_for_publication") is True
    opportunity_class = row.get("opportunity_class")
    if ready and not strict:
        raise ReleaseCheckError(
            "prepublication strict-ready opportunity is not public"
        )
    if (opportunity_class == "executable_candidate") != (ready and strict):
        raise ReleaseCheckError(
            "executable candidate requires strict-ready and strict-eligible"
        )
    buy_market_id = str(route.get("buy_market_id", ""))
    sell_market_id = str(route.get("sell_market_id", ""))
    strict_cex_identity = (
        route.get("route_mode") == "prepositioned_inventory"
        and buy_market_id.startswith("cex:")
        and sell_market_id.startswith("cex:")
        and len(buy_market_id.split(":", 2)) == 3
        and len(sell_market_id.split(":", 2)) == 3
        and buy_market_id.split(":", 2)[1] != sell_market_id.split(":", 2)[1]
    )
    if strict and not strict_cex_identity:
        raise ReleaseCheckError("strict route identity is unsupported")
    if strict and (
        str(route.get("buy_market_id", "")).startswith("cex:upbit:")
        or str(route.get("sell_market_id", "")).startswith("cex:upbit:")
    ):
        raise ReleaseCheckError("Upbit route must never be strict")

    target = Fraction(_route_decimal(
        row.get("target_token_quantity"), "target quantity", positive=True
    ))
    raw_quantity = _route_integer(
        row.get("target_base_raw"), "target raw quantity", positive=True
    )
    unit_decimals = row.get("target_base_unit_decimals")
    lattice_raw = _route_integer(
        row.get("target_lattice_raw"), "target lattice", positive=True
    )
    if type(unit_decimals) is not int or not 0 <= unit_decimals <= 36:
        raise ReleaseCheckError("quantity unit decimals are invalid")
    if raw_quantity % lattice_raw or target != Fraction(
        raw_quantity, 10**unit_decimals
    ):
        raise ReleaseCheckError("quantity lattice does not reproduce target")

    state_epochs: dict[str, Decimal | None] = {}
    for side in ("buy", "sell"):
        market_id = str(row.get(side + "_market_id"))
        leg = legs_by_market.get(market_id)
        if leg is None:
            raise ReleaseCheckError("state lineage has no exact published leg")
        observed_at = row.get(side + "_state_observed_at")
        if observed_at != leg.get("state_observed_at"):
            raise ReleaseCheckError("state lineage timestamp conflicts with leg")
        if observed_at is None:
            if strict:
                raise ReleaseCheckError("strict state timestamp is unavailable")
            state_epochs[side] = None
            continue
        if leg.get("available") is not True or leg.get("status") != "observed":
            if strict:
                raise ReleaseCheckError("strict state leg is unavailable")
        state_epochs[side] = _route_timestamp(
            observed_at, side + " state"
        )
    available_epochs = [
        epoch for epoch in state_epochs.values() if epoch is not None
    ]
    if any(epoch > now_epoch for epoch in available_epochs):
        raise ReleaseCheckError("state timestamp is in the future")
    if len(available_epochs) != 2:
        if row.get("skew_seconds") is not None or row.get("route_age_seconds") is not None:
            raise ReleaseCheckError("unavailable state has timing residue")
    else:
        skew = abs(available_epochs[0] - available_epochs[1])
        stored_skew = Fraction(_route_decimal(row.get("skew_seconds"), "skew"))
        if stored_skew != Fraction(skew):
            raise ReleaseCheckError("skew does not reproduce state timestamps")
        if strict and stored_skew > MAX_ROUTE_SKEW_SECONDS:
            raise ReleaseCheckError("skew exceeds strict route SLA")
        current_age = Fraction(now_epoch - max(available_epochs))
        stored_age = Fraction(_route_decimal(
            row.get("route_age_seconds"), "route age"
        ))
        if stored_age < 0:
            raise ReleaseCheckError("route age is invalid")
        if strict and (
            stored_age > MAX_ROUTE_AGE_SECONDS
            or current_age > MAX_ROUTE_AGE_SECONDS
        ):
            raise ReleaseCheckError(
                "stale inventory and route evidence is not strict"
            )

    if strict:
        if row.get("mode_evidence_eligible") is not True:
            raise ReleaseCheckError("strict route mode evidence is unavailable")
        profile_hash = row.get("inventory_profile_hash")
        if not isinstance(profile_hash, str) or re.fullmatch(
            r"[0-9a-f]{64}", profile_hash
        ) is None:
            raise ReleaseCheckError("strict inventory profile is unavailable")
        capacity = Fraction(_route_decimal(
            row.get("maximum_proved_capacity_quantity"),
            "inventory capacity",
            positive=True,
        ))
        if capacity < target:
            raise ReleaseCheckError("strict inventory capacity is insufficient")
        expected_mode_hash = _route_canonical_sha256({
            "route_id": row.get("route_id"),
            "route_mode": row.get("route_mode"),
            "classification": "mode_evidence_eligible",
            "mode_evidence_eligible": True,
            "reason_code": None,
            "reason_codes": [],
            "inventory_profile_hash": profile_hash,
            "maximum_proved_capacity_quantity": row.get(
                "maximum_proved_capacity_quantity"
            ),
        })
        if row.get("mode_evidence_sha256") != expected_mode_hash:
            raise ReleaseCheckError("strict mode evidence hash does not reproduce")

    component_rows = list(components)
    canonical_components = sorted(
        component_rows,
        key=lambda item: (
            str(item.get("opportunity_id")),
            str(item.get("leg")),
            str(item.get("component_type")),
        ),
    )
    expected_cost_set_hash = _route_publication_sha256(canonical_components)
    if row.get("cost_component_set_sha256") != expected_cost_set_hash:
        raise ReleaseCheckError("opportunity cost-set binding does not reproduce")
    if strict:
        expected_attestation = _route_canonical_sha256({
            "cohort_id": row.get("cohort_id"),
            "opportunity_id": row.get("opportunity_id"),
            "route_id": row.get("route_id"),
            "target_token_quantity": row.get("target_token_quantity"),
            "buy_state_id": row.get("buy_state_id"),
            "sell_state_id": row.get("sell_state_id"),
            "buy_usd_projection_sha256": row.get("buy_usd_projection_sha256"),
            "sell_usd_projection_sha256": row.get("sell_usd_projection_sha256"),
            "cost_component_set_sha256": row.get("cost_component_set_sha256"),
            "mode_evidence_sha256": row.get("mode_evidence_sha256"),
            "core_manifest_sha256": core_manifest_sha256,
        })
        if row.get("publication_attestation_sha256") != expected_attestation:
            raise ReleaseCheckError("publication attestation does not reproduce")
    reflected_values = row.get("reflected_or_embedded_component_keys")
    if not isinstance(reflected_values, list) or any(
        not isinstance(value, str) for value in reflected_values
    ) or reflected_values != sorted(set(reflected_values)):
        raise ReleaseCheckError("reflected cost inventory is invalid")
    reflected = set(reflected_values)
    required_keys = _route_expected_component_keys(route)
    by_key = {
        (str(item.get("leg")), str(item.get("component_type"))): item
        for item in component_rows
        if item.get("component_type") != "mev_buffer"
    }
    strict_complete = True
    scenario_complete = True
    strict_total = Fraction(0)
    bounded_total = Fraction(0)
    assumed_total = Fraction(0)
    estimated = False
    for key in required_keys:
        component = by_key[key]
        status = str(component.get("value_status"))
        current = _route_cost_is_current(component, now_epoch)
        strict_component = (
            status in _ROUTE_STRICT_VALUE_STATUSES
            and component.get("strict_eligible") is True
            and current
        )
        scenario_component = (
            strict_component
            or (status in _ROUTE_SCENARIO_VALUE_STATUSES and current)
        )
        strict_complete = strict_complete and strict_component
        scenario_complete = scenario_complete and scenario_component
        if status in _ROUTE_SCENARIO_VALUE_STATUSES:
            estimated = True
        key_text = "{}:{}".format(*key)
        if component.get("embedded_in_leg_quote") is True and key_text not in reflected:
            raise ReleaseCheckError("reflected embedded cost is missing")
        if (
            component.get("component_type") == "pool_swap_fee"
            and component.get("amount_usd") is not None
            and key_text not in reflected
        ):
            raise ReleaseCheckError("reflected pool fee would be double counted")
        amount_value = component.get("amount_usd")
        if amount_value is None or key_text in reflected:
            continue
        amount = Fraction(_route_decimal(amount_value, "cost amount"))
        if strict_component:
            strict_total += amount
        elif status == "bounded_estimate":
            bounded_total += amount
        elif status == "assumed":
            assumed_total += amount

    if any(
        str(item.get("component_type")) == "mev_buffer"
        for item in component_rows
    ):
        for component in component_rows:
            if component.get("component_type") != "mev_buffer":
                continue
            status = str(component.get("value_status"))
            current = _route_cost_is_current(component, now_epoch)
            if status not in _ROUTE_SCENARIO_VALUE_STATUSES or not current:
                scenario_complete = False
            elif component.get("amount_usd") is not None:
                estimated = True
                if status == "bounded_estimate":
                    bounded_total += Fraction(_route_decimal(
                        component["amount_usd"], "MEV bounded cost"
                    ))
                else:
                    assumed_total += Fraction(_route_decimal(
                        component["amount_usd"], "MEV assumed cost"
                    ))
    elif any(
        str(route.get(side + "_market_id", "")).startswith("dex:")
        for side in ("buy", "sell")
    ):
        strict_complete = False
        scenario_complete = False

    if strict and (not strict_complete or estimated):
        raise ReleaseCheckError("strict opportunity uses incomplete or estimated cost")
    expected_cost_completeness = "complete" if strict_complete else "incomplete"
    expected_scenario_completeness = (
        "complete" if scenario_complete else "incomplete"
    )
    if row.get("cost_completeness") != expected_cost_completeness:
        raise ReleaseCheckError("strict cost completeness does not reproduce")
    if row.get("scenario_cost_completeness") != expected_scenario_completeness:
        raise ReleaseCheckError("scenario cost completeness does not reproduce")

    gross_buy_value = row.get("gross_buy_cost_usd")
    gross_sell_value = row.get("gross_sell_proceeds_usd")
    if gross_buy_value is None or gross_sell_value is None:
        if strict or opportunity_class == "executable_candidate":
            raise ReleaseCheckError("strict opportunity cashflow is unavailable")
        numeric_fields = (
            "gross_edge_usd", "strict_net_edge_usd", "research_net_edge_usd",
            "gross_edge_bps", "strict_net_edge_bps", "research_net_edge_bps",
        )
        if any(row.get(field) is not None for field in numeric_fields):
            raise ReleaseCheckError("unavailable cost was coerced into a number")
        return

    gross_buy = Fraction(_route_decimal(
        gross_buy_value, "gross buy cost", positive=True
    ))
    gross_sell = Fraction(_route_decimal(
        gross_sell_value, "gross sell proceeds", positive=True
    ))
    gross_edge = gross_sell - gross_buy
    if Fraction(_route_decimal(row.get("gross_edge_usd"), "gross edge")) != gross_edge:
        raise ReleaseCheckError("gross edge arithmetic does not reproduce")
    _route_assert_ratio(
        row, prefix="gross_edge", edge=gross_edge, buy_cost=gross_buy
    )
    if Fraction(_route_decimal(
        row.get("strict_nonembedded_cost_usd"), "strict cost"
    )) != strict_total:
        raise ReleaseCheckError("strict cost arithmetic does not reproduce")
    if Fraction(_route_decimal(
        row.get("research_bounded_cost_usd"), "bounded cost"
    )) != bounded_total:
        raise ReleaseCheckError("bounded cost arithmetic does not reproduce")
    if Fraction(_route_decimal(
        row.get("research_assumed_cost_usd"), "assumed cost"
    )) != assumed_total:
        raise ReleaseCheckError("assumed cost arithmetic does not reproduce")

    strict_net = gross_edge - strict_total
    if Fraction(_route_decimal(
        row.get("strict_net_edge_usd"), "strict net edge"
    )) != strict_net:
        raise ReleaseCheckError("strict net edge arithmetic does not reproduce")
    _route_assert_ratio(
        row, prefix="strict_net_edge", edge=strict_net, buy_cost=gross_buy
    )
    if scenario_complete:
        research_net = strict_net - bounded_total - assumed_total
        if Fraction(_route_decimal(
            row.get("research_net_edge_usd"), "research net edge"
        )) != research_net:
            raise ReleaseCheckError("research net edge arithmetic does not reproduce")
        _route_assert_ratio(
            row, prefix="research_net_edge", edge=research_net,
            buy_cost=gross_buy,
        )
    elif any(row.get(field) is not None for field in (
        "research_net_edge_usd", "research_net_edge_bps",
        "research_net_edge_bps_numerator", "research_net_edge_bps_denominator",
    )):
        raise ReleaseCheckError("missing scenario cost was coerced into zero")
    if strict and strict_net <= 0:
        raise ReleaseCheckError("strict opportunity has no positive net edge")


def _validate_loaded_route_opportunity_release(
    loaded: Mapping[str, Any],
    *,
    now: Any = None,
) -> dict[str, Any]:
    bundle = loaded.get("bundle")
    manifest = loaded.get("manifest")
    pointer = loaded.get("pointer")
    if not all(isinstance(value, Mapping) for value in (
        bundle, manifest, pointer
    )):
        raise ReleaseCheckError("complete route opportunity envelope is invalid")
    if (
        manifest.get("bundle_stage") != "route_opportunity/v1"
        or bundle.get("schema") != "route_opportunity/v1"
        or pointer.get("bundle_stage") != "route_opportunity/v1"
    ):
        raise ReleaseCheckError("core-only or partial route bundle is not public")
    now_epoch, now_text = _route_now_epoch(now)
    routes = bundle.get("routes")
    legs = bundle.get("legs")
    opportunities = bundle.get("opportunities")
    costs = bundle.get("cost_components")
    if not all(isinstance(value, list) for value in (
        routes, legs, opportunities, costs
    )):
        raise ReleaseCheckError("complete route inventories are invalid")
    routes_by_id = {
        str(route.get("route_id")): route
        for route in routes if isinstance(route, Mapping)
    }
    legs_by_market = {
        str(leg.get("market_id")): leg
        for leg in legs if isinstance(leg, Mapping)
    }
    if len(routes_by_id) != len(routes) or len(legs_by_market) != len(legs):
        raise ReleaseCheckError("complete route inventories contain duplicates")
    components_by_opportunity = _route_cost_inventory(
        bundle, now_epoch=now_epoch
    )
    for row in opportunities:
        if not isinstance(row, Mapping):
            raise ReleaseCheckError("route opportunity row is invalid")
        route = routes_by_id.get(str(row.get("route_id")))
        if route is None:
            raise ReleaseCheckError("route opportunity lineage is missing")
        _validate_route_opportunity_row(
            row,
            route=route,
            legs_by_market=legs_by_market,
            components=components_by_opportunity.get(
                str(row.get("opportunity_id")), []
            ),
            core_manifest_sha256=str(bundle.get("core_manifest_sha256")),
            now_epoch=now_epoch,
        )

    generations = bundle.get("input_generations")
    if not isinstance(generations, Mapping):
        raise ReleaseCheckError("route input generations are unavailable")
    expected_cost_generation = _route_publication_sha256(costs)
    if generations.get("cost_component_generation") != expected_cost_generation:
        raise ReleaseCheckError("cost component generation does not reproduce")
    raw_generation_members = [
        {
            "market_id": str(leg.get("market_id")),
            "sha256": str(leg.get("raw_response_sha256")),
        }
        for leg in sorted(legs, key=lambda item: str(item.get("market_id")))
    ]
    if generations.get("raw_evidence_generation") != _route_publication_sha256(
        raw_generation_members
    ):
        raise ReleaseCheckError("raw evidence generation does not reproduce")
    if manifest.get("input_generations") != generations:
        raise ReleaseCheckError("manifest input generations diverge")
    if (
        pointer.get("core_manifest_sha256")
        != bundle.get("core_manifest_sha256")
        or manifest.get("core_manifest_sha256")
        != bundle.get("core_manifest_sha256")
        or pointer.get("core_pointer_sha256")
        != bundle.get("core_pointer_sha256")
        or manifest.get("core_pointer_sha256")
        != bundle.get("core_pointer_sha256")
    ):
        raise ReleaseCheckError("complete bundle core lineage diverges")
    context = bundle.get("core_context")
    if not isinstance(context, Mapping):
        raise ReleaseCheckError("complete bundle core context is invalid")
    completed = _route_timestamp(
        context.get("collection_completed_at"), "core completion"
    )
    deadline = _route_timestamp(
        context.get("collection_deadline_at"), "core deadline"
    )
    if completed > deadline:
        raise ReleaseCheckError("core collection exceeded its deadline")

    for inventory_name, bundle_name in (
        ("legs", "legs"),
        ("cost_components", "cost_components"),
        ("opportunities", "opportunities"),
    ):
        if loaded.get(inventory_name) != bundle.get(bundle_name):
            raise ReleaseCheckError("CSV and SQLite route inventories diverge")
    strict_rows = [
        row for row in opportunities if row.get("strict_eligible") is True
    ]
    empty_generation = _route_canonical_sha256([])
    if strict_rows and any(
        generations.get(field) == empty_generation
        for field in (
            "fee_profile_generation",
            "inventory_profile_generation",
            "typed_source_generation",
        )
    ):
        raise ReleaseCheckError("strict route source generation is empty")
    research_count = sum(
        row.get("opportunity_class") == "research_estimate"
        for row in opportunities
    )
    unavailable_count = sum(
        row.get("opportunity_class") == "unavailable"
        for row in opportunities
    )
    return {
        "status": "validated",
        "reason": None,
        "bundle_stage": "route_opportunity/v1",
        "route_cohort_id": bundle["route_cohort_id"],
        "manifest_sha256": loaded.get("manifest_sha256"),
        "core_manifest_sha256": bundle["core_manifest_sha256"],
        "strict_opportunity_count": len(strict_rows),
        "research_opportunity_count": research_count,
        "unavailable_opportunity_count": unavailable_count,
        "cost_component_count": len(costs),
        "validated_at": now_text,
    }


def validate_route_opportunity_release(
    routes_root: Path,
    *,
    required: bool,
    now: Any = None,
) -> dict[str, Any]:
    """Validate only the complete public opportunity pointer and its five files."""
    pointer_path = Path(routes_root) / "latest.json"
    try:
        pointer_path.lstat()
    except FileNotFoundError:
        if required:
            raise ReleaseCheckError(
                "required complete route opportunity pointer is unavailable"
            )
        return {"status": "unavailable", "reason": "complete_pointer_absent"}
    except OSError as error:
        raise ReleaseCheckError(
            "complete route opportunity pointer cannot be inspected"
        ) from error
    try:
        loaded = load_latest_complete_route_bundle(Path(routes_root))
    except OSError as error:
        raise ReleaseCheckError(
            "complete route opportunity validation failed: I/O unavailable"
        ) from error
    except (TypeError, ValueError, RoutePublicationError) as error:
        raise ReleaseCheckError(
            "complete route opportunity validation failed: {}".format(error)
        ) from error
    try:
        return _validate_loaded_route_opportunity_release(loaded, now=now)
    except ReleaseCheckError:
        raise
    except (ArithmeticError, KeyError, TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "complete route opportunity semantic validation failed: {}".format(
                error
            )
        ) from error


def configured_route_root() -> Path:
    """Return the complete public opportunity root used by release checks."""
    configured_data_root = os.environ.get("MARKET_DATA_DIR")
    if configured_data_root:
        return Path(configured_data_root).expanduser() / "routes"
    return DEFAULT_ROUTE_ROOT


def validate_release_health(
    health: dict[str, Any],
    *,
    expected_application_sha: str | None = None,
    expected_asset_sha: str | None = None,
) -> tuple[str, str, str]:
    application_sha = health.get("application_sha")
    asset_sha = health.get("asset_sha")
    asset_version = health.get("asset_version")
    require(
        isinstance(application_sha, str)
        and re.fullmatch(r"[0-9a-f]{40,64}", application_sha) is not None,
        "Health application SHA is missing or invalid",
    )
    require(
        isinstance(asset_sha, str)
        and re.fullmatch(r"[0-9a-f]{64}", asset_sha) is not None,
        "Health asset SHA is missing or invalid",
    )
    if expected_application_sha is not None:
        expected = str(expected_application_sha).strip().lower()
        require(
            re.fullmatch(r"[0-9a-f]{40,64}", expected) is not None,
            "Expected application SHA is invalid",
        )
        require(
            application_sha == expected,
            "Deployed application SHA does not match the expected application SHA",
        )
    if expected_asset_sha is not None:
        expected_asset = str(expected_asset_sha).strip().lower()
        require(
            re.fullmatch(r"[0-9a-f]{64}", expected_asset) is not None,
            "Expected asset SHA is invalid",
        )
        require(
            asset_sha == expected_asset,
            "Deployed asset SHA does not match the expected asset SHA",
        )
    expected_version = f"{application_sha[:12]}-{asset_sha[:12]}"
    require(
        asset_version == expected_version,
        "Health asset version does not match application and asset SHA evidence",
    )
    require(
        health.get("data_status") == "current",
        "Health freshness status is not current",
    )
    freshness_checked_at = validate_source_freshness(
        health.get("freshness"),
        label="Health",
    )
    validate_lifecycle_freshness(
        health.get("cex_instrument_lifecycle"),
        freshness_checked_at=freshness_checked_at,
    )
    return application_sha, asset_sha, asset_version


COLLECTED_NOTIONALS = frozenset({1_000, 5_000, 10_000, 50_000, 100_000})
EXECUTION_DIRECTIONS = frozenset({"sell_token", "buy_token"})
EXECUTION_STATUSES = frozenset({"observed", "partial", "unsupported", "failed"})
EVENT_LIFECYCLES = frozenset(
    {"scheduled", "occurred", "postponed", "cancelled", "superseded"}
)
EVENT_EVIDENCE_STATUSES = frozenset(
    {"primary_confirmed", "cross_checked", "onchain_observed"}
)
EVENT_CLOCK_STATES = frozenset({"past", "future", "current_window"})
UTC_SECOND_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
EXPECTED_SUMMARY_VERSION = 3
EXPECTED_QUALITY_CONTRACT_VERSION = 4
SCREENING_QUALITY_STATUSES = frozenset({"ok", "info", "warning", "critical"})
SCREENING_QUALITY_SEVERITIES = frozenset({"info", "warning", "critical"})
SCREENING_QUALITY_CATEGORIES = frozenset(
    {
        "data_health",
        "availability",
        "capability",
        "measurement_limit",
        "market_condition",
    }
)
SCREENING_QUALITY_FLAG_FIELDS = frozenset(
    {
        "code",
        "severity",
        "category",
        "message",
        "observed_value",
        "threshold",
    }
)
SCREENING_QUALITY_MARKET_FIELDS = frozenset(
    {
        "screening_quality_status",
        "screening_quality_flags",
        "screening_quality_scope",
        "screening_quality_window",
    }
)
SELECTED_QUALITY_MARKET_FIELDS = frozenset(
    {
        "quality_status",
        "quality_flags",
        "screening_quality_status",
        "screening_quality_flags",
        "screening_quality_scope",
        "screening_quality_window",
    }
)
QUALITY_FACT_NAMES = frozenset({"daily", "tvl", "depth", "execution"})
DAILY_QUALITY_STATUS_PRIORITY = {
    "collection_failed": 0,
    "needs_review": 1,
    "backfill_pending": 2,
    "source_no_observation": 3,
    "unsupported": 4,
}
DAILY_FACT_EVIDENCE_FIELDS = frozenset(
    {
        "daily_evidence_mode",
        "issue_status_counts",
        "issue_outcome_counts",
        "reason_code_counts",
        "affected_date_count",
        "affected_dates",
    }
)
DAILY_MATCHED_NO_ISSUE_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        (
            "not_applicable",
            "selected_window_before_first_market_observation",
        ),
        ("needs_review", "daily_quality_outcome_invalid"),
    }
)
CEX_DAILY_LIFECYCLE_NO_REPORT_ISSUE_FLAGS = {
    (
        "source_no_observation",
        "instrument_absent_from_current_catalog",
    ): "inactive_cex_instrument",
    (
        "needs_review",
        "official_catalog_evidence_stale",
    ): "stale_cex_lifecycle_evidence",
}
DAILY_FALLBACK_OUTCOMES = frozenset(
    set(DAILY_MATCHED_NO_ISSUE_OUTCOMES)
    | {
        ("backfill_pending", "missing_unexplained"),
        (
            "backfill_pending",
            "missing_daily_observations_inside_observed_market_lifecycle",
        ),
        (
            "backfill_pending",
            "missing_daily_observations_in_selected_window",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_after_latest_observed_market_date",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_in_selected_window",
        ),
    }
)
SELECTED_QUALITY_CATEGORIES = frozenset(
    set(SCREENING_QUALITY_CATEGORIES) | {"source_outcome"}
)
SELECTED_QUALITY_FLAG_FIELDS = frozenset(
    {"code", "severity", "category", "message", "observed_value", "threshold"}
)
SCREENING_QUALITY_CODE_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z",
    flags=re.ASCII,
)
RAW_URL_PATTERN = re.compile(
    r"\b[a-z][a-z0-9+.-]*://|\bwww\.",
    flags=re.ASCII | re.IGNORECASE,
)
ABSOLUTE_POSIX_PATH_PATTERN = re.compile(
    r"(?:^|[^a-z0-9])/"
    r"(?:[a-z0-9._~][a-z0-9._~-]{0,239}/){0,32}"
    r"[a-z0-9._~][a-z0-9._~-]{0,239}",
    flags=re.ASCII | re.IGNORECASE,
)
CANONICAL_DATE_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
FORBIDDEN_EVENT_RESULT_FIELDS = frozenset(
    {
        "impact",
        "market_impact",
        "return",
        "returns",
        "future_return",
        "causality",
        "causal_result",
    }
)
CANONICAL_CEX_TVL_OUTCOME = (
    "not_applicable",
    "cex_markets_do_not_have_pool_tvl",
)
EXACT_CEX_QUOTE_ASSETS = {
    "coinbase": "USD",
    "kraken": "USD",
}


def validate_configured_cex_identity_metadata(
    metadata: Any,
) -> frozenset[str]:
    """Validate the server-published authority for exact Upbit identities."""
    root = (
        metadata.get("configured_cex_market_identities")
        if isinstance(metadata, dict)
        else None
    )
    require(
        isinstance(root, dict)
        and root.get("schema") == "configured_cex_market_identities/v1"
        and set(root) == {"schema", "upbit"},
        "Configured Upbit market identity metadata is missing or invalid",
    )
    upbit = root.get("upbit")
    require(
        isinstance(upbit, dict)
        and set(upbit)
        == {"market_count", "market_ids", "market_ids_sha256"},
        "Configured Upbit market identity metadata is missing or invalid",
    )
    market_ids = upbit.get("market_ids")
    require(
        isinstance(market_ids, list),
        "Configured Upbit market identity inventory is invalid",
    )
    try:
        canonical = canonical_cex_market_ids(
            market_ids,
            exchange="upbit",
        )
        expected_hash = cex_market_ids_sha256(
            canonical,
            exchange="upbit",
        )
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "Configured Upbit market identity inventory is invalid"
        ) from error
    require(
        list(canonical) == market_ids
        and type(upbit.get("market_count")) is int
        and upbit["market_count"] == len(canonical)
        and upbit.get("market_ids_sha256") == expected_hash,
        "Configured Upbit market identity count or hash is invalid",
    )
    return frozenset(canonical)


def validate_exact_cex_market_identity(
    market_id: Any,
    token_symbol: Any,
    *,
    configured_upbit_market_ids: Any,
) -> None:
    """Reject known legacy quote aliases at the public release boundary."""
    if not isinstance(market_id, str) or not market_id.startswith("cex:"):
        return
    match = re.fullmatch(
        r"cex:([a-z0-9_]{2,32}):([A-Z0-9._-]{1,32})/"
        r"([A-Z0-9._-]{1,32})",
        market_id,
        flags=re.ASCII,
    )
    require(
        match is not None
        and isinstance(token_symbol, str)
        and match.group(2) == token_symbol,
        "Full catalog exact CEX identity is invalid",
    )
    exchange = match.group(1)
    if exchange == "upbit":
        try:
            configured_upbit = frozenset(
                canonical_cex_market_ids(
                    configured_upbit_market_ids,
                    exchange="upbit",
                )
            )
        except (TypeError, ValueError) as error:
            raise ReleaseCheckError(
                "Configured Upbit market identity inventory is invalid"
            ) from error
        require(
            market_id in configured_upbit,
            "Full catalog market is not a configured Upbit exact identity",
        )
        return
    expected_quote = EXACT_CEX_QUOTE_ASSETS.get(exchange)
    require(
        expected_quote is None or match.group(3) == expected_quote,
        "Full catalog exact CEX identity uses a legacy quote alias",
    )


def _release_quality_fact_rule(
    market_type: Any,
    fact_name: Any,
    status: Any,
    reason_code: Any,
) -> Any:
    """Apply release-only family constraints on top of producer rules."""
    family = str(market_type or "").strip().lower()
    fact = str(fact_name or "").strip().lower()
    pair = (
        str(status or "").strip().lower(),
        str(reason_code or "").strip().lower(),
    )
    if (
        family == "cex"
        and fact == "tvl"
        and pair != CANONICAL_CEX_TVL_OUTCOME
    ):
        return None
    return canonical_quality_fact_rule(
        family,
        fact,
        pair[0],
        pair[1],
    )


def _release_quality_fact_action(
    market_type: Any,
    fact_name: Any,
    status: Any,
    reason_code: Any,
    retryable: Any,
    **kwargs: Any,
) -> str | None:
    """Derive one action without broadening an outcome to another fact."""
    rule = _release_quality_fact_rule(
        market_type,
        fact_name,
        status,
        reason_code,
    )
    if (
        rule is None
        or type(retryable) is not bool
        or retryable is not rule.retryable
    ):
        raise ValueError("quality fact outcome is not canonical")
    return canonical_quality_fact_action(
        market_type,
        fact_name,
        status,
        reason_code,
        retryable,
        **kwargs,
    )


def _validate_endpoint_generation(
    metadata: dict[str, Any],
    *,
    field: str,
    expected: str | None,
    label: str,
) -> None:
    """Enable endpoint-specific generation checks when producers publish one."""
    if expected is None:
        return
    require(
        isinstance(expected, str)
        and bool(expected)
        and expected == expected.strip(),
        f"Expected {label} generation is invalid",
    )
    require(
        metadata.get(field) == expected,
        f"{label} generation differs from the expected generation",
    )


def fetch_json(
    base_url: str,
    path: str,
    *,
    timeout: float,
) -> tuple[dict[str, Any], ResponseMetrics]:
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
    )
    started = time.perf_counter()
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            encoding = response.headers.get("Content-Encoding", "").lower()
            status = response.status
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:500]
        raise ReleaseCheckError(
            f"{path} returned HTTP {error.code}: {detail}"
        ) from error
    except URLError as error:
        raise ReleaseCheckError(f"{path} request failed: {error.reason}") from error
    elapsed_ms = (time.perf_counter() - started) * 1000
    require(status == 200, f"{path} returned HTTP {status}")
    compressed = encoding == "gzip"
    try:
        raw = gzip.decompress(body) if compressed else body
    except gzip.BadGzipFile as error:
        raise ReleaseCheckError(f"{path} declared invalid gzip") from error
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseCheckError(f"{path} did not return valid JSON") from error
    require(isinstance(payload, dict), f"{path} JSON root must be an object")
    return payload, ResponseMetrics(
        path=path,
        elapsed_ms=elapsed_ms,
        wire_bytes=len(body),
        raw_bytes=len(raw),
        compressed=compressed,
    )


def fetch_static_asset_bundle(
    base_url: str,
    asset_version: str,
    *,
    timeout: float,
) -> tuple[str, list[ResponseMetrics]]:
    """Fetch the versioned first-party assets and recompute their exact hash."""
    require(
        isinstance(asset_version, str)
        and re.fullmatch(r"[0-9a-f]{12}-[0-9a-f]{12}", asset_version)
        is not None,
        "Static asset version is invalid",
    )
    digest = hashlib.sha256()
    metrics = []
    for filename in STATIC_ASSET_FILENAMES:
        path = "/{}?v={}".format(filename, asset_version)
        request = Request(
            "{}{}".format(base_url.rstrip("/"), path),
            headers={"Accept-Encoding": "gzip"},
        )
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_STATIC_ASSET_BYTES + 1)
                encoding = response.headers.get(
                    "Content-Encoding", ""
                ).lower()
                status = response.status
        except HTTPError as error:
            raise ReleaseCheckError(
                "{} returned HTTP {}".format(path, error.code)
            ) from error
        except URLError as error:
            raise ReleaseCheckError(
                "{} request failed: {}".format(path, error.reason)
            ) from error
        elapsed_ms = (time.perf_counter() - started) * 1000
        require(status == 200, "{} returned HTTP {}".format(path, status))
        compressed = encoding == "gzip"
        try:
            raw = gzip.decompress(body) if compressed else body
        except gzip.BadGzipFile as error:
            raise ReleaseCheckError(
                "{} declared invalid gzip".format(path)
            ) from error
        require(
            len(raw) <= MAX_STATIC_ASSET_BYTES,
            "{} exceeds the static asset size limit".format(path),
        )
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update(raw)
        digest.update(b"\0")
        metrics.append(
            ResponseMetrics(
                path=path,
                elapsed_ms=elapsed_ms,
                wire_bytes=len(body),
                raw_bytes=len(raw),
                compressed=compressed,
            )
        )
    return digest.hexdigest(), metrics


def validate_summary(
    payload: dict[str, Any],
    metrics: ResponseMetrics,
    *,
    raw_max: int,
    gzip_max: int,
) -> tuple[str, str, str, str]:
    metadata = payload.get("metadata") or {}
    validate_configured_cex_identity_metadata(metadata)
    freshness_checked_at = validate_source_freshness(
        metadata.get("freshness"),
        label="Summary",
    )
    validate_lifecycle_freshness(
        metadata.get("cex_instrument_lifecycle"),
        freshness_checked_at=freshness_checked_at,
    )
    tokens = payload.get("tokens")
    require(
        metadata.get("response_scope") == "screener_summary",
        "Summary response_scope is not screener_summary",
    )
    require(
        metadata.get("summary_version") == EXPECTED_SUMMARY_VERSION,
        f"Summary version is not {EXPECTED_SUMMARY_VERSION}",
    )
    require(isinstance(tokens, list) and tokens, "Summary has no Token rows")
    token_symbols: list[str] = []
    for row in tokens:
        require(isinstance(row, dict), "Summary Token row is not an object")
        token_symbol = row.get("token_symbol")
        require(
            isinstance(token_symbol, str)
            and bool(token_symbol)
            and token_symbol == token_symbol.strip().upper(),
            "Summary Token identity is invalid",
        )
        token_symbols.append(token_symbol)
        market_count = row.get("market_count")
        status_counts = row.get("quality_status_counts")
        alert_counts = row.get("quality_alert_counts")
        require(
            type(market_count) is int and market_count > 0,
            "Summary Token market_count is invalid",
        )
        require(
            isinstance(status_counts, dict)
            and all(
                isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
                for count in status_counts.values()
            )
            and sum(status_counts.values()) == market_count,
            "Summary quality status counts do not match market_count",
        )
        require(
            isinstance(alert_counts, dict)
            and all(
                severity in {"info", "warning", "critical"}
                and isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
                for severity, count in alert_counts.items()
            ),
            "Summary quality alert counts are invalid",
        )
        comparable_days = row.get("spread_comparable_days")
        spread_values = {
            field: row.get(field)
            for field in (
                "absolute_price_gap",
                "maximum_absolute_price_spread",
                "mean_absolute_price_spread",
                "median_absolute_price_spread",
            )
        }
        require(
            type(comparable_days) is int
            and comparable_days >= 0
            and row.get("price_spread_method")
            == "directional_dex_over_cex_minus_one"
            and row.get("absolute_price_gap_method")
            == "symmetric_midpoint_relative_gap"
            and all(
                value is None
                or (
                    type(value) in {int, float}
                    and math.isfinite(value)
                    and value >= 0
                )
                for value in spread_values.values()
            )
            and (
                all(value is None for value in spread_values.values())
                if comparable_days == 0
                else all(value is not None for value in spread_values.values())
            )
            and (
                row.get("price_spread") is None
                if comparable_days == 0
                else type(row.get("price_spread")) in {int, float}
                and math.isfinite(row["price_spread"])
            ),
            "Summary spread contract is invalid",
        )
        if comparable_days:
            maximum_gap = spread_values["maximum_absolute_price_spread"]
            require(
                maximum_gap >= spread_values["absolute_price_gap"]
                and maximum_gap >= spread_values["mean_absolute_price_spread"]
                and maximum_gap >= spread_values["median_absolute_price_spread"],
                "Summary spread contract is internally inconsistent",
            )
        for market_type in ("cex", "dex"):
            market = row.get(f"primary_{market_type}")
            if market is None:
                continue
            require(
                isinstance(market, dict),
                "Summary primary market is not an object",
            )
            refresh_id = market.get("refresh_market_id")
            market_token = market.get("token_symbol")
            venue = market.get("venue")
            instrument = market.get("instrument")
            pool_address = market.get("pool_address")
            expected_refresh_id = None
            if market_type == "cex" and (
                isinstance(venue, str)
                and venue
                and isinstance(instrument, str)
                and "/" in instrument
                and instrument.split("/", 1)[0] == token_symbol
                and market_token == token_symbol
            ):
                expected_refresh_id = "cex:{}:{}".format(
                    venue,
                    instrument,
                )
            elif market_type == "dex" and (
                isinstance(venue, str)
                and venue.count(" / ") == 1
                and isinstance(pool_address, str)
                and bool(pool_address)
                and market_token == token_symbol
            ):
                chain, dex = venue.split(" / ", 1)
                if chain and dex:
                    expected_refresh_id = "dex:{}:{}:{}:{}".format(
                        chain,
                        dex,
                        pool_address,
                        token_symbol,
                    )
            require(
                isinstance(refresh_id, str)
                and refresh_id == expected_refresh_id,
                "Summary primary market refresh identity is invalid",
            )
            require(
                isinstance(market.get("depth_retryable"), bool)
                and isinstance(market.get("tvl_retryable"), bool),
                "Summary primary market retryability is invalid",
            )
            for fact_name in ("tvl", "depth"):
                status = market.get(f"{fact_name}_status")
                reason_field = f"{fact_name}_na_reason"
                reason = market.get(reason_field)
                retryable = market.get(f"{fact_name}_retryable")
                rule = _release_quality_fact_rule(
                    market_type,
                    fact_name,
                    status,
                    reason,
                )
                require(
                    reason_field in market
                    and isinstance(status, str)
                    and rule is not None
                    and retryable is rule.retryable,
                    "Summary primary market N/A outcome is not canonical",
                )
    for forbidden in ("markets", "cex_markets", "dex_pools", "price_points"):
        require(forbidden not in payload, f"Summary leaked heavy root field: {forbidden}")
    require(metrics.compressed, "Summary response was not gzip compressed")
    require(
        metrics.raw_bytes <= raw_max,
        f"Summary raw payload {metrics.raw_bytes} exceeds {raw_max}",
    )
    require(
        metrics.wire_bytes <= gzip_max,
        f"Summary gzip payload {metrics.wire_bytes} exceeds {gzip_max}",
    )
    generation = metadata.get("data_generation")
    start = metadata.get("start_date")
    end = metadata.get("end_date")
    token = metadata.get("default_workspace_token")
    require(
        len(set(token_symbols)) == len(token_symbols),
        "Summary Token identities are not unique",
    )
    require(
        type(metadata.get("token_count")) is int
        and metadata["token_count"] == len(tokens),
        "Summary token_count does not match Token rows",
    )
    require(
        type(metadata.get("catalog_market_count")) is int
        and metadata["catalog_market_count"]
        == sum(row["market_count"] for row in tokens),
        "Summary catalog_market_count does not match Token market counts",
    )
    require(isinstance(generation, str) and generation, "Summary generation is missing")
    require(isinstance(start, str) and start, "Summary start_date is missing")
    require(isinstance(end, str) and end, "Summary end_date is missing")
    require(token in set(token_symbols), "Default workspace Token is absent from summary")
    return token, start, end, generation


def validate_token_catalog(
    payload: dict[str, Any],
    metrics: ResponseMetrics,
    *,
    token: str,
    start: str,
    end: str,
    generation: str,
    raw_max: int,
    gzip_max: int,
) -> list[dict[str, Any]]:
    metadata = payload.get("metadata") or {}
    configured_upbit_market_ids = (
        validate_configured_cex_identity_metadata(metadata)
    )
    markets = payload.get("markets")
    require(payload.get("token_symbol") == token, "Token catalog returned wrong Token")
    require(isinstance(markets, list) and markets, "Token catalog has no markets")
    require(
        all(row.get("token_symbol") == token for row in markets),
        "Token catalog leaked another Token",
    )
    market_ids = [
        row.get("market_id") if isinstance(row, dict) else None
        for row in markets
    ]
    require(
        all(
            isinstance(market_id, str)
            and bool(market_id)
            and market_id == market_id.strip()
            for market_id in market_ids
        )
        and len(set(market_ids)) == len(market_ids),
        "Token catalog market IDs are invalid or duplicated",
    )
    for row, market_id in zip(markets, market_ids):
        validate_exact_cex_market_identity(
            market_id,
            row.get("token_symbol") if isinstance(row, dict) else None,
            configured_upbit_market_ids=configured_upbit_market_ids,
        )
    require(metadata.get("window_start") == start, "Token catalog start window differs")
    require(metadata.get("window_end") == end, "Token catalog end window differs")
    require(
        metadata.get("data_generation") == generation,
        "Summary and Token catalog generations differ",
    )
    require(metrics.compressed, "Token catalog response was not gzip compressed")
    require(
        metrics.raw_bytes <= raw_max,
        f"Token catalog raw payload {metrics.raw_bytes} exceeds {raw_max}",
    )
    require(
        metrics.wire_bytes <= gzip_max,
        f"Token catalog gzip payload {metrics.wire_bytes} exceeds {gzip_max}",
    )
    return markets


def validate_comparison(
    payload: dict[str, Any],
    *,
    token: str,
    market_a: str,
    market_b: str,
    start: str,
    end: str,
    expected_generation: str,
    expected_comparison_generation: str | None = None,
) -> None:
    metadata = payload.get("metadata") or {}
    observations = payload.get("observations")
    require(payload.get("token_symbol") == token, "Compare returned wrong Token")
    require(
        (payload.get("market_a") or {}).get("market_id") == market_a,
        "Compare returned wrong Market A",
    )
    require(
        (payload.get("market_b") or {}).get("market_id") == market_b,
        "Compare returned wrong Market B",
    )
    require(metadata.get("start_date") == start, "Compare returned wrong start window")
    require(metadata.get("end_date") == end, "Compare returned wrong end window")
    require(
        metadata.get("data_generation") == expected_generation,
        "Summary and Compare generations differ",
    )
    _validate_endpoint_generation(
        metadata,
        field="comparison_generation",
        expected=expected_comparison_generation,
        label="Comparison",
    )
    require(
        isinstance(observations, list) and observations,
        "Compare returned no daily observations",
    )
    require(
        all(
            isinstance(row, dict)
            and isinstance(row.get("date"), str)
            and start <= row["date"] <= end
            for row in observations
        ),
        "Compare returned an invalid or out-of-window observation",
    )
    require(
        isinstance(metadata.get("comparison_days"), int)
        and metadata["comparison_days"] > 0,
        "Compare returned no comparable days",
    )
    require(
        isinstance(payload.get("latest_comparable_observation"), dict),
        "Compare returned no latest comparable observation",
    )


def _normalized_daily_count_map(
    value: Any,
    *,
    label: str,
    allowed_keys: frozenset[str] | None = None,
    require_positive_entries: bool = False,
) -> dict[str, int]:
    require(isinstance(value, dict), label)
    normalized: dict[str, int] = {}
    for key, count in value.items():
        require(
            isinstance(key, str)
            and bool(key)
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(key) is not None
            and (allowed_keys is None or key in allowed_keys)
            and type(count) is int
            and count >= 0
            and (not require_positive_entries or count > 0),
            label,
        )
        if count:
            normalized[key] = count
    return dict(sorted(normalized.items()))


def _normalized_daily_outcome_counts(
    value: Any,
    *,
    label: str,
    market_type: str | None = None,
) -> dict[tuple[str, str], int]:
    require(isinstance(value, list), label)
    normalized: dict[tuple[str, str], int] = {}
    for item in value:
        require(
            isinstance(item, dict)
            and set(item) == {"status", "reason_code", "count"}
            and isinstance(item.get("status"), str)
            and item["status"] in DAILY_QUALITY_STATUS_PRIORITY
            and isinstance(item.get("reason_code"), str)
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(
                item["reason_code"]
            )
            is not None
            and type(item.get("count")) is int
            and item["count"] > 0,
            label,
        )
        pair = (item["status"], item["reason_code"])
        require(pair not in normalized, label)
        families = (market_type,) if market_type else ("cex", "dex")
        require(
            any(
                _release_quality_fact_rule(
                    family,
                    "daily",
                    pair[0],
                    pair[1],
                )
                is not None
                for family in families
            ),
            label,
        )
        normalized[pair] = item["count"]
    return dict(sorted(normalized.items()))


def _validate_daily_quality_report(
    report: Any,
    *,
    expected_market_ids: set[str],
) -> dict[str, Any]:
    require(
        isinstance(report, dict)
        and report.get("status")
        in {
            "matched",
            "unavailable",
            "ignored_invalid",
            "ignored_identity_unavailable",
            "ignored_identity_mismatch",
        },
        "Quality daily-audit status is invalid",
    )
    status = report["status"]
    expected_evidence_mode = (
        "published_daily_audit"
        if status == "matched"
        else "catalog_window_inference"
    )
    require(
        report.get("evidence_mode") == expected_evidence_mode,
        "Quality daily-audit evidence mode is invalid",
    )
    if status == "matched":
        require(
            report.get("schema") == "fact_quality_report/v1"
            and report.get("identity_status") == "matched_current_import",
            "Quality daily audit lacks a verified current publication identity",
        )
    else:
        require(
            report.get("identity_status")
            in {"not_verified", "unavailable", "mismatch"},
            "Quality fallback has an invalid publication identity status",
        )

    issue_count = report.get("selected_window_issue_count")
    require(
        type(issue_count) is int and issue_count >= 0,
        "Quality daily-audit reason/status counts are inconsistent",
    )
    reason_counts = _normalized_daily_count_map(
        report.get("reason_code_counts"),
        label="Quality daily-audit reason/status counts are inconsistent",
    )
    status_counts = _normalized_daily_count_map(
        report.get("status_counts"),
        label="Quality daily-audit reason/status counts are inconsistent",
        allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
    )
    outcome_counts = (
        _normalized_daily_outcome_counts(
            report.get("issue_outcome_counts"),
            label="Quality daily-audit outcome counts are invalid",
        )
        if status == "matched"
        else {}
    )
    outcome_status_counts: Counter[str] = Counter()
    outcome_reason_counts: Counter[str] = Counter()
    for (outcome_status, outcome_reason), count in outcome_counts.items():
        outcome_status_counts[outcome_status] += count
        outcome_reason_counts[outcome_reason] += count
    require(
        sum(reason_counts.values()) == issue_count
        and sum(status_counts.values()) == issue_count,
        "Quality daily-audit reason/status counts are inconsistent",
    )
    if status == "matched":
        require(
            dict(sorted(outcome_status_counts.items())) == status_counts
            and dict(sorted(outcome_reason_counts.items()))
            == reason_counts,
            "Quality daily-audit outcome counts do not match its marginals",
        )
    affected_dates = report.get("affected_dates")
    require(
        isinstance(affected_dates, list)
        and all(_is_canonical_date(value) for value in affected_dates)
        and affected_dates == sorted(set(affected_dates))
        and type(report.get("affected_date_count")) is int
        and report["affected_date_count"] == len(affected_dates)
        and len(affected_dates) <= issue_count,
        "Quality daily-audit affected dates are inconsistent",
    )
    if status != "matched":
        require(
            issue_count == 0
            and not reason_counts
            and not status_counts
            and not affected_dates,
            "Quality fallback cannot claim published daily-audit issues",
        )
        require(
            "market_issue_rollups" not in report,
            "Quality fallback cannot claim market-bound daily evidence",
        )
        require(
            "issue_outcome_counts" not in report,
            "Quality fallback cannot claim published outcome counts",
        )
        market_rollups: dict[str, dict[str, Any]] = {}
    else:
        raw_rollups = report.get("market_issue_rollups")
        require(
            isinstance(raw_rollups, list)
            and len(raw_rollups) == len(expected_market_ids),
            "Quality daily-audit market rollups are incomplete",
        )
        market_rollups = {}
        rollup_status_counts: Counter[str] = Counter()
        rollup_reason_counts: Counter[str] = Counter()
        rollup_affected_dates: set[str] = set()
        rollup_outcome_counts: Counter[tuple[str, str]] = Counter()
        rollup_issue_count = 0
        for rollup in raw_rollups:
            require(
                isinstance(rollup, dict)
                and isinstance(rollup.get("market_id"), str)
                and rollup["market_id"] in expected_market_ids
                and rollup["market_id"] not in market_rollups,
                "Quality daily-audit market rollups are invalid",
            )
            rollup_count = rollup.get("issue_count")
            rollup_reasons = _normalized_daily_count_map(
                rollup.get("reason_code_counts"),
                label="Quality daily-audit market rollups are invalid",
            )
            rollup_statuses = _normalized_daily_count_map(
                rollup.get("status_counts"),
                label="Quality daily-audit market rollups are invalid",
                allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
            )
            rollup_outcomes = _normalized_daily_outcome_counts(
                rollup.get("issue_outcome_counts"),
                label="Quality daily-audit market rollup outcome counts are invalid",
            )
            rollup_outcome_statuses: Counter[str] = Counter()
            rollup_outcome_reasons: Counter[str] = Counter()
            for (outcome_status, outcome_reason), count in (
                rollup_outcomes.items()
            ):
                rollup_outcome_statuses[outcome_status] += count
                rollup_outcome_reasons[outcome_reason] += count
            rollup_dates = rollup.get("affected_dates")
            fact_outcome = rollup.get("fact_outcome")
            require(
                type(rollup_count) is int
                and rollup_count >= 0
                and sum(rollup_reasons.values()) == rollup_count
                and sum(rollup_statuses.values()) == rollup_count
                and dict(sorted(rollup_outcome_statuses.items()))
                == rollup_statuses
                and dict(sorted(rollup_outcome_reasons.items()))
                == rollup_reasons
                and isinstance(rollup_dates, list)
                and all(_is_canonical_date(value) for value in rollup_dates)
                and rollup_dates == sorted(set(rollup_dates))
                and type(rollup.get("affected_date_count")) is int
                and rollup["affected_date_count"] == len(rollup_dates)
                and len(rollup_dates) <= rollup_count,
                "Quality daily-audit market rollups are invalid",
            )
            require(
                rollup.get("evidence_mode")
                in {
                    "published_daily_audit",
                    "catalog_report_reconciliation",
                }
                and isinstance(fact_outcome, dict)
                and set(fact_outcome)
                == {"status", "reason_code", "retryable", "action"}
                and isinstance(fact_outcome.get("status"), str)
                and isinstance(fact_outcome.get("reason_code"), str)
                and type(fact_outcome.get("retryable")) is bool
                and (
                    fact_outcome.get("action") is None
                    or isinstance(fact_outcome.get("action"), str)
                ),
                "Quality daily-audit market rollup fact outcome is invalid",
            )
            normalized_rollup = {
                "mode": rollup["evidence_mode"],
                "issue_count": rollup_count,
                "outcome_counts": rollup_outcomes,
                "reason_counts": rollup_reasons,
                "status_counts": rollup_statuses,
                "affected_dates": rollup_dates,
                "fact_outcome": fact_outcome,
            }
            market_rollups[rollup["market_id"]] = normalized_rollup
            rollup_issue_count += rollup_count
            rollup_reason_counts.update(rollup_reasons)
            rollup_status_counts.update(rollup_statuses)
            rollup_affected_dates.update(rollup_dates)
            rollup_outcome_counts.update(rollup_outcomes)
        require(
            set(market_rollups) == expected_market_ids
            and rollup_issue_count == issue_count
            and dict(sorted(rollup_reason_counts.items())) == reason_counts
            and dict(sorted(rollup_status_counts.items())) == status_counts
            and sorted(rollup_affected_dates) == affected_dates,
            "Quality daily-audit market rollups do not match the report",
        )
        require(
            dict(sorted(rollup_outcome_counts.items())) == outcome_counts,
            "Quality daily-audit market rollups do not match the report",
        )
    return {
        "status": status,
        "issue_count": issue_count,
        "reason_counts": reason_counts,
        "status_counts": status_counts,
        "outcome_counts": outcome_counts,
        "affected_dates": affected_dates,
        "market_rollups": market_rollups,
    }


def _validate_daily_fact_evidence(
    fact: dict[str, Any],
    *,
    market_type: str,
    report_status: str,
) -> dict[str, Any]:
    status = fact["status"]
    reason_code = fact["reason_code"]
    retryable = fact["retryable"]
    mode = fact.get("daily_evidence_mode")
    present_evidence_fields = {
        field for field in DAILY_FACT_EVIDENCE_FIELDS if field in fact
    }
    lifecycle_pair = (status, reason_code)
    expected_lifecycle_flag = (
        CEX_DAILY_LIFECYCLE_NO_REPORT_ISSUE_FLAGS.get(lifecycle_pair)
        if market_type == "cex"
        else None
    )
    has_cex_lifecycle_evidence = bool(
        expected_lifecycle_flag
        and any(
            isinstance(flag, dict)
            and flag.get("code") == expected_lifecycle_flag
            for flag in fact.get("quality_flags", [])
        )
    )

    if report_status == "matched":
        require(
            present_evidence_fields == DAILY_FACT_EVIDENCE_FIELDS,
            "Quality daily fact evidence/action is incomplete",
        )
    else:
        require(
            mode is None and not present_evidence_fields,
            "Quality daily fact evidence/action mode is invalid",
        )

    if mode == "published_daily_audit":
        require(
            report_status == "matched",
            "Quality daily fact evidence/action is incomplete",
        )
        status_counts = _normalized_daily_count_map(
            fact.get("issue_status_counts"),
            label="Quality daily fact evidence/action counts are invalid",
            allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
            require_positive_entries=True,
        )
        reason_counts = _normalized_daily_count_map(
            fact.get("reason_code_counts"),
            label="Quality daily fact evidence/action counts are invalid",
            require_positive_entries=True,
        )
        outcome_counts = _normalized_daily_outcome_counts(
            fact.get("issue_outcome_counts"),
            label="Quality daily fact evidence/action outcome counts are invalid",
            market_type=market_type,
        )
        outcome_status_counts: Counter[str] = Counter()
        outcome_reason_counts: Counter[str] = Counter()
        for (outcome_status, outcome_reason), count in (
            outcome_counts.items()
        ):
            outcome_status_counts[outcome_status] += count
            outcome_reason_counts[outcome_reason] += count
        issue_count = sum(status_counts.values())
        require(
            sum(reason_counts.values()) == issue_count
            and dict(sorted(outcome_status_counts.items())) == status_counts
            and dict(sorted(outcome_reason_counts.items())) == reason_counts,
            "Quality daily fact evidence/action counts are inconsistent",
        )
        affected_dates = fact.get("affected_dates")
        require(
            isinstance(affected_dates, list)
            and all(_is_canonical_date(value) for value in affected_dates)
            and affected_dates == sorted(set(affected_dates))
            and type(fact.get("affected_date_count")) is int
            and fact["affected_date_count"] == len(affected_dates)
            and len(affected_dates) <= issue_count,
            "Quality daily fact evidence/action dates are inconsistent",
        )
        if issue_count == 0:
            require(
                not status_counts
                and not reason_counts
                and not affected_dates
                and (
                    lifecycle_pair in DAILY_MATCHED_NO_ISSUE_OUTCOMES
                    or has_cex_lifecycle_evidence
                ),
                "Quality daily fact zero evidence/action is invalid",
            )
            try:
                expected_action = _release_quality_fact_action(
                    market_type,
                    "daily",
                    status,
                    reason_code,
                    retryable,
                    manual_review_present=False,
                )
            except ValueError as error:
                raise ReleaseCheckError(
                    "Quality daily fact zero evidence/action is invalid"
                ) from error
            require(
                fact.get("action") == expected_action,
                "Quality daily fact zero evidence/action is invalid",
            )
            return {
                "mode": mode,
                "issue_count": 0,
                "status_counts": {},
                "outcome_counts": {},
                "reason_counts": {},
                "affected_dates": [],
                "fact_outcome": {
                    "status": status,
                    "reason_code": reason_code,
                    "retryable": retryable,
                    "action": fact.get("action"),
                },
            }
        try:
            expected_status = aggregate_daily_quality_status(status_counts)
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality daily fact status aggregation is invalid"
            ) from error
        expected_reason = (
            next(iter(reason_counts))
            if len(reason_counts) == 1
            else "multiple_daily_quality_reasons"
        )
        expected_retryable = any(
            issue_status in {"collection_failed", "backfill_pending"}
            for issue_status in status_counts
        )
        manual_review_present = bool(status_counts.get("needs_review"))
        try:
            expected_action = _release_quality_fact_action(
                market_type,
                "daily",
                expected_status,
                expected_reason,
                expected_retryable,
                manual_review_present=manual_review_present,
            )
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality daily fact evidence/action outcome is invalid"
            ) from error
        require(
            status == expected_status
            and reason_code == expected_reason
            and retryable is expected_retryable
            and fact.get("action") == expected_action,
            "Quality daily fact evidence/action does not match its issues",
        )
        return {
            "mode": mode,
            "issue_count": issue_count,
            "status_counts": status_counts,
            "outcome_counts": outcome_counts,
            "reason_counts": reason_counts,
            "affected_dates": affected_dates,
            "fact_outcome": {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": fact.get("action"),
            },
        }

    if mode == "catalog_report_reconciliation":
        require(
            report_status == "matched"
            and present_evidence_fields == DAILY_FACT_EVIDENCE_FIELDS
            and fact.get("issue_status_counts") == {}
            and fact.get("issue_outcome_counts") == []
            and fact.get("reason_code_counts")
            == {"daily_audit_no_matching_issue": 1}
            and fact.get("affected_date_count") == 0
            and fact.get("affected_dates") == []
            and status == "needs_review"
            and reason_code == "daily_audit_no_matching_issue"
            and retryable is False
            and fact.get("action") == "operator_manual_review",
            "Quality daily fact reconciliation evidence/action is invalid",
        )
        return {
            "mode": mode,
            "issue_count": 0,
            "status_counts": {},
            "outcome_counts": {},
            "reason_counts": {},
            "affected_dates": [],
            "fact_outcome": {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": fact.get("action"),
            },
        }

    require(report_status != "matched", "Quality daily fact evidence/action is incomplete")
    pair = (status, reason_code)
    allowed = (
        DAILY_MATCHED_NO_ISSUE_OUTCOMES
        if report_status == "matched"
        else DAILY_FALLBACK_OUTCOMES
    )
    require(
        pair in allowed or has_cex_lifecycle_evidence,
        "Quality daily fact lacks required published evidence/action",
    )
    try:
        expected_action = _release_quality_fact_action(
            market_type,
            "daily",
            status,
            reason_code,
            retryable,
            manual_review_present=False,
        )
    except ValueError as error:
        raise ReleaseCheckError(
            "Quality daily fact action outcome is invalid"
        ) from error
    require(
        fact.get("action") == expected_action,
        "Quality daily fact action is not canonical",
    )
    return {
        "mode": None,
        "issue_count": 0,
        "status_counts": {},
        "outcome_counts": {},
        "reason_counts": {},
        "affected_dates": [],
        "fact_outcome": {
            "status": status,
            "reason_code": reason_code,
            "retryable": retryable,
            "action": fact.get("action"),
        },
    }


def validate_quality(
    payload: dict[str, Any],
    *,
    token: str,
    market_a: str,
    market_b: str,
    expected_generation: str | None = None,
) -> None:
    metadata = payload.get("metadata") or {}
    markets = payload.get("markets")
    expected_ids = {market_a, market_b}
    daily_report = _validate_daily_quality_report(
        metadata.get("daily_quality_report"),
        expected_market_ids=expected_ids,
    )
    daily_evidence_rows: list[dict[str, Any]] = []
    require(payload.get("token_symbol") == token, "Quality returned wrong Token")
    require(metadata.get("scope") == "selected", "Quality did not honor selected scope")
    require(
        type(metadata.get("contract_version")) is int
        and metadata["contract_version"] == EXPECTED_QUALITY_CONTRACT_VERSION,
        "Quality contract is not v4",
    )
    if expected_generation is not None:
        require(
            metadata.get("data_generation") == expected_generation,
            "Summary and selected Quality generations differ",
        )
    selected_market_ids = metadata.get("selected_market_ids")
    require(
        isinstance(selected_market_ids, list)
        and len(selected_market_ids) == 2
        and all(isinstance(market_id, str) for market_id in selected_market_ids)
        and len(set(selected_market_ids)) == 2
        and set(selected_market_ids) == expected_ids,
        "Quality metadata returned the wrong selected markets",
    )
    require(
        isinstance(markets, list) and len(markets) == 2,
        "Quality did not return both selected markets",
    )
    require(
        {row.get("market_id") for row in markets if isinstance(row, dict)}
        == expected_ids,
        "Quality returned the wrong market identities",
    )
    for row in markets:
        require(
            isinstance(row, dict) and row.get("token_symbol") == token,
            "Quality returned an empty or wrong-Token fact set",
        )
        quality_fields = {
            field for field in SELECTED_QUALITY_MARKET_FIELDS if field in row
        }
        require(
            quality_fields == SELECTED_QUALITY_MARKET_FIELDS,
            "Quality selected quality contract has missing projection fields",
        )
        _validate_screening_context(row)
        selected_status = row["quality_status"]
        selected_flags = row["quality_flags"]
        screening_status = row["screening_quality_status"]
        screening_flags = row["screening_quality_flags"]
        require(
            selected_status in SCREENING_QUALITY_STATUSES
            and isinstance(selected_flags, list)
            and (selected_status == "ok" or bool(selected_flags)),
            "Quality selected quality contract has an invalid selected projection",
        )
        require(
            screening_status in SCREENING_QUALITY_STATUSES
            and isinstance(screening_flags, list)
            and (screening_status == "ok" or bool(screening_flags)),
            "Quality selected quality contract has an invalid screening projection",
        )
        normalized_selected_flags = [
            _validate_selected_quality_flag(flag)
            for flag in selected_flags
        ]
        normalized_screening_flags = [
            _validate_screening_flag(flag)
            for flag in screening_flags
        ]
        require(
            selected_status == _quality_status_from_flags(
                normalized_selected_flags
            )
            and screening_status == _quality_status_from_flags(
                normalized_screening_flags
            ),
            "Quality status does not match its data-health flags",
        )

        facts = row.get("facts")
        market_type = row.get("market_type")
        require(
            market_type in {"cex", "dex"},
            "Quality selected market type is invalid",
        )
        require(
            isinstance(row.get("market_id"), str)
            and row["market_id"].startswith("{}:".format(market_type)),
            "Quality market identity/type is inconsistent",
        )
        require(
            isinstance(facts, dict) and set(facts) == QUALITY_FACT_NAMES,
            "Quality selected quality contract has missing or unknown fact families",
        )
        fact_flags_by_code: dict[str, dict[str, Any]] = {}
        for fact_name in QUALITY_FACT_NAMES:
            fact = facts[fact_name]
            status = fact.get("status") if isinstance(fact, dict) else None
            reason_code = fact.get("reason_code") if isinstance(fact, dict) else None
            action = fact.get("action") if isinstance(fact, dict) else None
            fact_flags = fact.get("quality_flags") if isinstance(fact, dict) else None
            require(
                isinstance(fact, dict)
                and isinstance(status, str)
                and 0 < len(status) <= 64
                and SCREENING_QUALITY_CODE_PATTERN.fullmatch(status) is not None
                and "reason_code" in fact
                and (
                    reason_code is None
                    or (
                        isinstance(reason_code, str)
                        and 0 < len(reason_code) <= 64
                        and SCREENING_QUALITY_CODE_PATTERN.fullmatch(reason_code)
                        is not None
                    )
                )
                and type(fact.get("retryable")) is bool
                and "action" in fact
                and (action is None or isinstance(action, str))
                and isinstance(fact_flags, list),
                "Quality selected quality contract has an invalid fact projection",
            )
            for flag in fact_flags:
                normalized_flag = _validate_selected_quality_flag(flag)
                prior = fact_flags_by_code.get(normalized_flag["code"])
                require(
                    prior is None or prior == normalized_flag,
                    "Quality facts contain conflicting flag projections",
                )
                fact_flags_by_code[normalized_flag["code"]] = normalized_flag
            rule = _release_quality_fact_rule(
                market_type,
                fact_name,
                status,
                reason_code,
            )
            require(
                rule is not None
                and fact["retryable"] is rule.retryable,
                "Quality fact does not use a canonical outcome/action tuple",
            )
            if fact_name == "daily":
                daily_evidence = _validate_daily_fact_evidence(
                    fact,
                    market_type=market_type,
                    report_status=daily_report["status"],
                )
                if daily_report["status"] == "matched":
                    require(
                        {
                            key: daily_evidence[key]
                            for key in (
                                "mode",
                                "issue_count",
                                "outcome_counts",
                                "status_counts",
                                "reason_counts",
                                "affected_dates",
                                "fact_outcome",
                            )
                        }
                        == daily_report["market_rollups"][row["market_id"]],
                        "Quality daily fact evidence/action does not match its market rollup",
                    )
                daily_evidence_rows.append(daily_evidence)
                expected_action = fact.get("action")
            else:
                expected_action = _release_quality_fact_action(
                    market_type,
                    fact_name,
                    status,
                    reason_code,
                    fact["retryable"],
                )
            require(
                action == expected_action,
                "Quality fact does not use a canonical outcome/action tuple",
            )
        selected_flags_by_code: dict[str, dict[str, Any]] = {}
        for normalized_flag in normalized_selected_flags:
            code = normalized_flag["code"]
            require(
                code not in selected_flags_by_code,
                "Quality selected flags contain duplicate codes",
            )
            selected_flags_by_code[code] = normalized_flag
        require(
            selected_flags_by_code == fact_flags_by_code,
            "Quality selected flags differ from the fact flag projection",
        )
    published_status_counts: Counter[str] = Counter()
    published_reason_counts: Counter[str] = Counter()
    published_outcome_counts: Counter[tuple[str, str]] = Counter()
    published_affected_dates: set[str] = set()
    published_issue_count = 0
    for evidence in daily_evidence_rows:
        if evidence["mode"] != "published_daily_audit":
            continue
        published_issue_count += evidence["issue_count"]
        published_status_counts.update(evidence["status_counts"])
        published_reason_counts.update(evidence["reason_counts"])
        published_outcome_counts.update(evidence["outcome_counts"])
        published_affected_dates.update(evidence["affected_dates"])
    require(
        published_issue_count == daily_report["issue_count"]
        and dict(sorted(published_status_counts.items()))
        == daily_report["status_counts"]
        and dict(sorted(published_reason_counts.items()))
        == daily_report["reason_counts"]
        and dict(sorted(published_outcome_counts.items()))
        == daily_report["outcome_counts"]
        and sorted(published_affected_dates)
        == daily_report["affected_dates"],
        "Quality daily fact evidence/action does not reconcile to the report",
    )


def _normalized_summary_counts(
    value: Any,
    *,
    allowed_keys: frozenset[str],
    label: str,
) -> dict[str, int]:
    require(isinstance(value, dict), f"Summary {label} counts are invalid")
    normalized: dict[str, int] = {}
    for key, count in value.items():
        require(
            key in allowed_keys
            and type(count) is int
            and count >= 0,
            f"Summary {label} counts are invalid",
        )
        if count:
            normalized[key] = count
    return dict(sorted(normalized.items()))


def _is_canonical_date(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or CANONICAL_DATE_PATTERN.fullmatch(value) is None
    ):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%d") == value


def _quality_status_from_flags(flags: list[dict[str, Any]]) -> str:
    data_health = [
        flag for flag in flags if flag.get("category") == "data_health"
    ]
    if any(flag.get("severity") == "critical" for flag in data_health):
        return "critical"
    if any(flag.get("severity") == "warning" for flag in data_health):
        return "warning"
    if data_health:
        return "info"
    return "ok"


def _validate_screening_flag(flag: Any) -> dict[str, str]:
    require(isinstance(flag, dict), "Quality screening flag is not an object")
    require(
        set(flag) == SCREENING_QUALITY_FLAG_FIELDS,
        "Quality screening flag has missing or unknown fields",
    )
    code = flag["code"]
    require(
        isinstance(code, str)
        and len(code) <= 64
        and SCREENING_QUALITY_CODE_PATTERN.fullmatch(code) is not None,
        "Quality screening flag code is invalid",
    )
    severity = flag["severity"]
    require(
        isinstance(severity, str)
        and severity in SCREENING_QUALITY_SEVERITIES,
        "Quality screening flag severity is invalid",
    )
    category = flag["category"]
    require(
        isinstance(category, str)
        and category in SCREENING_QUALITY_CATEGORIES,
        "Quality screening flag category is invalid",
    )
    message = flag["message"]
    require(
        isinstance(message, str)
        and message == message.strip()
        and 0 < len(message) <= 240,
        "Quality screening flag message is invalid",
    )
    require(
        not any(
            unicodedata.category(character) in {"Cc", "Cf"}
            for character in message
        ),
        "Quality screening flag message contains a control marker",
    )
    require(
        RAW_URL_PATTERN.search(message) is None,
        "Quality screening flag message contains a raw URL",
    )
    require(
        ABSOLUTE_POSIX_PATH_PATTERN.search(message) is None
        and "\\" not in message,
        "Quality screening flag message contains a protected path",
    )
    for field in ("observed_value", "threshold"):
        try:
            encoded = json.dumps(
                flag[field],
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as error:
            raise ReleaseCheckError(
                "Quality screening measurement is not bounded JSON"
            ) from error
        require(
            len(encoded) <= 1024
            and RAW_URL_PATTERN.search(encoded) is None
            and ABSOLUTE_POSIX_PATH_PATTERN.search(encoded) is None
            and "\\" not in encoded,
            "Quality screening measurement exposes unbounded or protected data",
        )
    return dict(flag)


def _validate_screening_context(market: dict[str, Any]) -> None:
    require(
        market.get("screening_quality_scope") == "catalog",
        "Quality screening scope is invalid",
    )
    window = market.get("screening_quality_window")
    require(
        isinstance(window, dict)
        and set(window) == {"start", "end", "method"},
        "Quality screening evaluation window is invalid",
    )
    start = window["start"]
    end = window["end"]
    method = window["method"]
    require(
        (start is None or _is_canonical_date(start))
        and (end is None or _is_canonical_date(end))
        and isinstance(method, (str, type(None)))
        and (method is None or (method == method.strip() and len(method) <= 96)),
        "Quality screening evaluation window is not bounded",
    )
    require(
        start is None or end is None or start <= end,
        "Quality screening evaluation window is reversed",
    )


def _validate_selected_quality_flag(flag: Any) -> dict[str, Any]:
    """Validate the richer selected-window flag without trusting public text."""
    require(isinstance(flag, dict), "Quality selected quality contract flag is invalid")
    require(
        set(flag) == SELECTED_QUALITY_FLAG_FIELDS,
        "Quality selected quality contract flag has missing or unknown fields",
    )
    # The producer always emits both measurement keys. An explicit JSON null
    # is canonical when a flag does not have a numeric measurement.
    code = flag["code"]
    severity = flag["severity"]
    category = flag["category"]
    message = flag["message"]
    require(
        isinstance(code, str)
        and len(code) <= 64
        and SCREENING_QUALITY_CODE_PATTERN.fullmatch(code) is not None
        and severity in SCREENING_QUALITY_SEVERITIES
        and category in SELECTED_QUALITY_CATEGORIES
        and isinstance(message, str)
        and message == message.strip()
        and 0 < len(message) <= 240,
        "Quality selected quality contract flag is invalid",
    )
    require(
        not any(
            unicodedata.category(character) in {"Cc", "Cf"}
            for character in message
        )
        and RAW_URL_PATTERN.search(message) is None
        and ABSOLUTE_POSIX_PATH_PATTERN.search(message) is None
        and "\\" not in message,
        "Quality selected quality contract flag exposes protected text",
    )
    return flag


def _validate_all_scope_market_fact_contract(
    market: dict[str, Any],
    *,
    token: str,
    daily_report: dict[str, Any],
) -> dict[str, Any]:
    """Validate every fact family for one scope=all Quality market.

    Screening parity covers the complete catalog, so it must not validate only
    the screening badges while leaving non-primary market facts unchecked.
    """
    market_type = market.get("market_type")
    market_id = market.get("market_id")
    require(
        market_type in {"cex", "dex"}
        and isinstance(market_id, str)
        and market_id.startswith("{}:".format(market_type))
        and market.get("token_symbol") == token,
        "Quality all-scope market identity/type is inconsistent",
    )
    facts = market.get("facts")
    require(
        isinstance(facts, dict) and set(facts) == QUALITY_FACT_NAMES,
        "Quality all-scope market has missing or unknown fact families",
    )
    selected_status = market.get("quality_status")
    selected_flags = market.get("quality_flags")
    require(
        selected_status in SCREENING_QUALITY_STATUSES
        and isinstance(selected_flags, list)
        and (selected_status == "ok" or bool(selected_flags)),
        "Quality all-scope selected quality projection is invalid",
    )
    selected_flags_by_code: dict[str, dict[str, Any]] = {}
    for raw_flag in selected_flags:
        flag = _validate_selected_quality_flag(raw_flag)
        require(
            flag["code"] not in selected_flags_by_code,
            "Quality all-scope selected quality flags are duplicated",
        )
        selected_flags_by_code[flag["code"]] = flag
    require(
        selected_status == _quality_status_from_flags(
            list(selected_flags_by_code.values())
        ),
        "Quality all-scope selected quality status differs from its flags",
    )
    fact_flags_by_code: dict[str, dict[str, Any]] = {}
    daily_evidence: dict[str, Any] | None = None
    for fact_name in QUALITY_FACT_NAMES:
        fact = facts[fact_name]
        status = fact.get("status") if isinstance(fact, dict) else None
        reason_code = fact.get("reason_code") if isinstance(fact, dict) else None
        retryable = fact.get("retryable") if isinstance(fact, dict) else None
        action = fact.get("action") if isinstance(fact, dict) else None
        flags = fact.get("quality_flags") if isinstance(fact, dict) else None
        require(
            isinstance(fact, dict)
            and isinstance(status, str)
            and 0 < len(status) <= 64
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(status) is not None
            and isinstance(reason_code, str)
            and 0 < len(reason_code) <= 64
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(reason_code) is not None
            and type(retryable) is bool
            and "action" in fact
            and (action is None or isinstance(action, str))
            and isinstance(flags, list),
            "Quality all-scope market has an invalid fact projection",
        )
        for raw_flag in flags:
            flag = _validate_selected_quality_flag(raw_flag)
            prior = fact_flags_by_code.get(flag["code"])
            require(
                prior is None or prior == flag,
                "Quality all-scope facts contain conflicting flag projections",
            )
            fact_flags_by_code[flag["code"]] = flag
        rule = _release_quality_fact_rule(
            market_type,
            fact_name,
            status,
            reason_code,
        )
        require(
            rule is not None and retryable is rule.retryable,
            "Quality all-scope market does not use a canonical fact outcome",
        )
        if fact_name == "daily":
            daily_evidence = _validate_daily_fact_evidence(
                fact,
                market_type=market_type,
                report_status=daily_report["status"],
            )
            require(
                daily_evidence
                == daily_report["market_rollups"].get(market_id),
                "Quality all-scope daily fact does not match its report rollup",
            )
            continue
        try:
            expected_action = _release_quality_fact_action(
                market_type,
                fact_name,
                status,
                reason_code,
                retryable,
            )
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality all-scope market does not use a canonical fact action"
            ) from error
        require(
            action == expected_action,
            "Quality all-scope market does not use a canonical fact action",
        )
    require(
        selected_flags_by_code == fact_flags_by_code,
        "Quality all-scope selected quality flags differ from fact flags",
    )
    assert daily_evidence is not None
    return daily_evidence


def validate_screening_quality_parity(
    summary_row: dict[str, Any],
    quality_payload: dict[str, Any],
    expected_generation: str,
) -> dict[str, Any]:
    """Reproduce one Summary row from its same-generation Quality projection."""
    require(isinstance(quality_payload, dict), "Quality payload is not an object")
    metadata = quality_payload.get("metadata")
    require(isinstance(metadata, dict), "Quality metadata is not an object")
    require(
        type(metadata.get("contract_version")) is int
        and metadata["contract_version"] == EXPECTED_QUALITY_CONTRACT_VERSION,
        "Quality contract v4 is required for screening parity",
    )
    require(
        isinstance(expected_generation, str)
        and bool(expected_generation)
        and expected_generation == expected_generation.strip()
        and metadata.get("data_generation") == expected_generation,
        "Summary and screening Quality generation differ",
    )
    require(
        metadata.get("scope") == "all",
        "Screening Quality did not honor all scope",
    )

    require(isinstance(summary_row, dict), "Summary Token row is not an object")
    token = summary_row.get("token_symbol")
    require(
        isinstance(token, str) and bool(token) and token == token.strip(),
        "Summary Token is invalid",
    )
    require(
        quality_payload.get("token_symbol") == token,
        "Quality returned the wrong Token for screening parity",
    )
    expected_market_count = summary_row.get("market_count")
    require(
        type(expected_market_count) is int and expected_market_count > 0,
        "Summary market count is invalid",
    )
    markets = quality_payload.get("markets")
    require(isinstance(markets, list), "Quality markets is not an array")
    require(
        len(markets) == expected_market_count,
        "Quality market count does not match Summary",
    )

    declared_market_ids = {
        market.get("market_id")
        for market in markets
        if isinstance(market, dict)
        and isinstance(market.get("market_id"), str)
    }
    require(
        all(
            isinstance(market, dict)
            and isinstance(market.get("market_id"), str)
            and bool(market["market_id"])
            and market["market_id"] == market["market_id"].strip()
            for market in markets
        )
        and len(declared_market_ids) == len(markets),
        "Quality market IDs are invalid or duplicated",
    )
    daily_report = _validate_daily_quality_report(
        metadata.get("daily_quality_report"),
        expected_market_ids=declared_market_ids,
    )
    require(
        daily_report["status"] == "matched",
        "Screening Quality daily audit is not matched to the current import",
    )

    market_ids: set[str] = set()
    status_counts: Counter[str] = Counter()
    alert_counts: Counter[str] = Counter()
    daily_evidence_rows: list[dict[str, Any]] = []
    for market in markets:
        require(isinstance(market, dict), "Quality market is not an object")
        market_id = market.get("market_id")
        require(
            isinstance(market_id, str)
            and bool(market_id)
            and market_id == market_id.strip(),
            "Quality market ID is invalid",
        )
        require(market_id not in market_ids, "Quality market IDs are not unique")
        market_ids.add(market_id)
        require(
            market.get("token_symbol") == token,
            "Quality market Token does not match Summary",
        )
        daily_evidence_rows.append(
            _validate_all_scope_market_fact_contract(
                market,
                token=token,
                daily_report=daily_report,
            )
        )
        screening_fields = {
            key
            for key in market
            if isinstance(key, str) and key.startswith("screening_quality_")
        }
        require(
            screening_fields == SCREENING_QUALITY_MARKET_FIELDS,
            "Quality market has missing or unknown screening quality fields",
        )
        _validate_screening_context(market)
        status = market["screening_quality_status"]
        require(
            isinstance(status, str)
            and status in SCREENING_QUALITY_STATUSES,
            "Quality screening status is invalid",
        )
        flags = market["screening_quality_flags"]
        require(isinstance(flags, list), "Quality screening flags is not an array")
        require(
            status == "ok" or bool(flags),
            "Quality non-OK status has no fallback alert",
        )
        normalized_screening_flags = []
        for raw_flag in flags:
            flag = _validate_screening_flag(raw_flag)
            normalized_screening_flags.append(flag)
            alert_counts[flag["severity"]] += 1
        require(
            status == _quality_status_from_flags(normalized_screening_flags),
            "Quality screening status differs from its flags",
        )
        status_counts[status] += 1

    expected_status_counts = _normalized_summary_counts(
        summary_row.get("quality_status_counts"),
        allowed_keys=SCREENING_QUALITY_STATUSES,
        label="quality status",
    )
    expected_alert_counts = _normalized_summary_counts(
        summary_row.get("quality_alert_counts"),
        allowed_keys=SCREENING_QUALITY_SEVERITIES,
        label="quality alert",
    )
    actual_status_counts = dict(sorted(status_counts.items()))
    actual_alert_counts = dict(sorted(alert_counts.items()))
    require(
        actual_status_counts == expected_status_counts,
        "Summary screening quality status counts do not match Quality",
    )
    require(
        actual_alert_counts == expected_alert_counts,
        "Summary screening quality alert counts do not match Quality",
    )
    published_issue_count = sum(
        evidence["issue_count"]
        for evidence in daily_evidence_rows
        if evidence["mode"] == "published_daily_audit"
    )
    published_status_counts: Counter[str] = Counter()
    published_reason_counts: Counter[str] = Counter()
    published_outcome_counts: Counter[tuple[str, str]] = Counter()
    published_dates: set[str] = set()
    for evidence in daily_evidence_rows:
        if evidence["mode"] != "published_daily_audit":
            continue
        published_status_counts.update(evidence["status_counts"])
        published_reason_counts.update(evidence["reason_counts"])
        published_outcome_counts.update(evidence["outcome_counts"])
        published_dates.update(evidence["affected_dates"])
    require(
        published_issue_count == daily_report["issue_count"]
        and dict(sorted(published_status_counts.items()))
        == daily_report["status_counts"]
        and dict(sorted(published_reason_counts.items()))
        == daily_report["reason_counts"]
        and dict(sorted(published_outcome_counts.items()))
        == daily_report["outcome_counts"]
        and sorted(published_dates) == daily_report["affected_dates"],
        "Screening Quality daily facts do not reconcile to the report",
    )
    return {
        "token_symbol": token,
        "market_count": len(markets),
        "market_ids": sorted(market_ids),
        "status_counts": actual_status_counts,
        "alert_counts": actual_alert_counts,
    }


def _execution_scenario_key(row: dict[str, Any]) -> tuple[str, int] | None:
    direction = row.get("direction")
    try:
        notional = float(row.get("requested_notional_usd"))
    except (TypeError, ValueError):
        return None
    if (
        direction not in EXECUTION_DIRECTIONS
        or not math.isfinite(notional)
        or notional <= 0
        or notional != int(notional)
    ):
        return None
    return direction, int(notional)


def _canonical_cohort_timestamp(
    raw: Any,
    label: str,
    field: str,
) -> datetime:
    """Normalize one release timestamp inside a controlled error boundary."""
    require(
        isinstance(raw, str)
        and bool(raw)
        and raw == raw.strip(),
        f"{label} {field} is invalid",
    )
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ReleaseCheckError(
                f"{label} {field} is not timezone-aware"
            )
        normalized = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise ReleaseCheckError(
            f"{label} {field} is invalid"
        ) from error
    require(
        normalized.isoformat() == raw,
        f"{label} {field} is not canonical UTC",
    )
    return normalized


def validate_source_freshness(
    freshness: Any,
    *,
    label: str,
) -> datetime:
    """Require every release-critical source to be current and self-consistent."""
    require(isinstance(freshness, dict), f"{label} freshness is missing")
    required = {
        "checked_at",
        "overall_status",
        "common_comparable_end",
        "cex_daily",
        "dex_daily",
        "dex_tvl",
        "cex_depth",
        "dex_depth",
        "cex_execution",
        "dex_execution",
    }
    require(
        required.issubset(freshness),
        f"{label} freshness is incomplete",
    )
    checked_at = _canonical_cohort_timestamp(
        freshness.get("checked_at"),
        label,
        "freshness.checked_at",
    )
    require(
        freshness.get("overall_status") == "current",
        f"{label} freshness overall status is not current",
    )

    daily_ends: list[str] = []
    latest_completed = (checked_at.date() - timedelta(days=1)).isoformat()
    for source_name in ("cex_daily", "dex_daily"):
        item = freshness.get(source_name)
        require(
            isinstance(item, dict)
            and item.get("source") == source_name
            and item.get("status") == "current",
            f"{label} freshness {source_name} is not current",
        )
        available_start = item.get("available_start")
        available_end = item.get("available_end")
        completed = item.get("latest_completed_utc_day")
        require(
            _is_canonical_date(available_start)
            and _is_canonical_date(available_end)
            and available_start <= available_end
            and completed == latest_completed,
            f"{label} freshness {source_name} date bounds are invalid",
        )
        lag_days = item.get("lag_days")
        max_lag_days = item.get("max_lag_days")
        expected_lag = max(
            0,
            (
                datetime.strptime(latest_completed, "%Y-%m-%d").date()
                - datetime.strptime(available_end, "%Y-%m-%d").date()
            ).days,
        )
        require(
            type(lag_days) is int
            and type(max_lag_days) is int
            and max_lag_days >= 0
            and lag_days == expected_lag
            and lag_days <= max_lag_days,
            f"{label} freshness {source_name} lag is stale or inconsistent",
        )
        daily_ends.append(available_end)

    require(
        freshness.get("common_comparable_end") == min(daily_ends),
        f"{label} freshness common comparable end is inconsistent",
    )
    for source_name in (
        "dex_tvl",
        "cex_depth",
        "dex_depth",
        "cex_execution",
        "dex_execution",
    ):
        item = freshness.get(source_name)
        require(
            isinstance(item, dict)
            and item.get("source") == source_name
            and item.get("status") == "current",
            f"{label} freshness {source_name} is not current",
        )
        observed_at = _canonical_cohort_timestamp(
            item.get("observed_at"),
            label,
            "freshness.{}.observed_at".format(source_name),
        )
        age_hours = item.get("age_hours")
        max_age_hours = item.get("max_age_hours")
        expected_age = round(
            max(0.0, (checked_at - observed_at).total_seconds() / 3600),
            3,
        )
        require(
            type(age_hours) in {int, float}
            and not isinstance(age_hours, bool)
            and math.isfinite(age_hours)
            and type(max_age_hours) in {int, float}
            and not isinstance(max_age_hours, bool)
            and math.isfinite(max_age_hours)
            and max_age_hours > 0
            and abs(float(age_hours) - expected_age) <= 0.001
            and 0 <= float(age_hours) <= float(max_age_hours)
            and observed_at <= checked_at + timedelta(minutes=5),
            f"{label} freshness {source_name} age is stale or inconsistent",
        )
    return checked_at


def validate_lifecycle_freshness(
    lifecycle: Any,
    *,
    freshness_checked_at: datetime,
) -> None:
    """Reject releases backed by expired CEX catalog-membership evidence."""
    require(
        isinstance(lifecycle, dict)
        and lifecycle.get("schema") == "cex_instrument_lifecycle/v1",
        "Summary CEX lifecycle freshness is missing",
    )
    for field in (
        "reviewed_market_count",
        "absence_market_count",
        "applied_market_count",
        "withheld_payload_market_count",
        "stale_evidence_market_count",
    ):
        require(
            type(lifecycle.get(field)) is int and lifecycle[field] >= 0,
            "Summary CEX lifecycle counts are invalid",
        )
    require(
        lifecycle["absence_market_count"]
        <= lifecycle["reviewed_market_count"]
        and lifecycle["applied_market_count"]
        == lifecycle["absence_market_count"]
        and lifecycle["withheld_payload_market_count"]
        <= lifecycle["applied_market_count"]
        and lifecycle["stale_evidence_market_count"] == 0,
        "Summary CEX lifecycle evidence is stale",
    )
    official_inventory_count = lifecycle.get("official_inventory_count")
    response_sha256 = lifecycle.get("response_sha256")
    configured_market_hash = lifecycle.get("configured_market_ids_sha256")
    require(
        type(official_inventory_count) is int
        and official_inventory_count > 0
        and isinstance(response_sha256, str)
        and re.fullmatch(r"[0-9a-f]{64}", response_sha256) is not None,
        "Summary CEX lifecycle root evidence is invalid",
    )
    require(
        isinstance(configured_market_hash, str)
        and re.fullmatch(r"[0-9a-f]{64}", configured_market_hash) is not None,
        "Summary CEX lifecycle configured-market evidence is invalid",
    )
    max_age = lifecycle.get("freshness_max_age_seconds")
    require(
        type(max_age) is int and max_age > 0,
        "Summary CEX lifecycle freshness threshold is invalid",
    )
    checked_min = _canonical_cohort_timestamp(
        lifecycle.get("checked_at_min"),
        "Summary CEX lifecycle",
        "checked_at_min",
    )
    checked_max = _canonical_cohort_timestamp(
        lifecycle.get("checked_at_max"),
        "Summary CEX lifecycle",
        "checked_at_max",
    )
    oldest_age = (freshness_checked_at - checked_min).total_seconds()
    require(
        checked_min <= checked_max
        and -300 <= oldest_age <= max_age,
        "Summary CEX lifecycle evidence exceeds its freshness threshold",
    )


def _validate_cohort_observation_metadata(
    value: dict[str, Any],
    label: str,
) -> tuple[datetime | None, datetime | None]:
    """Independently verify canonical cohort bounds and their exact span."""
    required = {
        "observed_at",
        "observed_at_min",
        "observed_at_max",
        "observation_span_seconds",
    }
    require(
        required.issubset(value),
        f"{label} observation metadata is incomplete",
    )
    observed_at = value.get("observed_at")
    observed_at_min = value.get("observed_at_min")
    observed_at_max = value.get("observed_at_max")
    span = value.get("observation_span_seconds")
    if observed_at is None and observed_at_min is None and observed_at_max is None:
        require(span is None, f"{label} span has no observation bounds")
        return None, None
    require(
        observed_at is not None
        and observed_at_min is not None
        and observed_at_max is not None,
        f"{label} observation bounds are incomplete",
    )

    first = _canonical_cohort_timestamp(observed_at, label, "observed_at")
    lower = _canonical_cohort_timestamp(
        observed_at_min,
        label,
        "observed_at_min",
    )
    upper = _canonical_cohort_timestamp(
        observed_at_max,
        label,
        "observed_at_max",
    )
    require(first == lower, f"{label} observed_at is not the lower bound")
    expected_span = (upper - lower).total_seconds()
    require(expected_span >= 0, f"{label} observation bounds are reversed")
    require(
        type(span) in {int, float}
        and math.isfinite(span)
        and span >= 0
        and span == expected_span,
        f"{label} observation span differs from its bounds",
    )
    return lower, upper


def validate_execution(
    payload: dict[str, Any],
    *,
    token: str,
    market_a: str,
    market_b: str,
    expected_generation: str,
    catalog_metadata: dict[str, Any],
    expected_execution_generation: str | None = None,
) -> None:
    metadata = payload.get("metadata") or {}
    require(
        metadata.get("data_generation") == expected_generation,
        "Summary and Execution generations differ",
    )
    _validate_endpoint_generation(
        metadata,
        field="execution_generation",
        expected=expected_execution_generation,
        label="Execution",
    )
    require(
        metadata.get("cohort_observation_model")
        == "bounded_sequential_observations",
        "Execution cohort observation model is invalid",
    )
    require(payload.get("token_symbol") == token, "Execution returned wrong Token")
    expected_scenarios = {
        (direction, notional)
        for direction in EXECUTION_DIRECTIONS
        for notional in COLLECTED_NOTIONALS
    }
    selected_market_types: dict[str, list[dict[str, Any]]] = {}
    has_measured_rows = False
    for label, expected_market in (
        ("market_a", market_a),
        ("market_b", market_b),
    ):
        leg = payload.get(label)
        require(isinstance(leg, dict), f"Execution omitted {label}")
        require(leg.get("status") == "available", f"Execution {label} is unavailable")
        require(
            (leg.get("market") or {}).get("market_id") == expected_market,
            f"Execution {label} returned the wrong market",
        )
        rows = leg.get("rows")
        require(
            isinstance(rows, list) and len(rows) == len(expected_scenarios),
            f"Execution {label} does not have the complete 10-row scenario grid",
        )
        require(
            all(
                isinstance(row, dict)
                and row.get("market_id") == expected_market
                and row.get("token_symbol") == token
                and row.get("status") in EXECUTION_STATUSES
                for row in rows
            ),
            f"Execution {label} has invalid identity or status rows",
        )
        require(
            {_execution_scenario_key(row) for row in rows} == expected_scenarios,
            f"Execution {label} has duplicate or missing direction/notional scenarios",
        )
        has_measured_rows = has_measured_rows or any(
            row.get("status") in {"observed", "partial"} for row in rows
        )
        market_type = expected_market.split(":", 1)[0]
        require(
            market_type in {"cex", "dex"},
            f"Execution {label} market type is invalid",
        )
        selected_market_types.setdefault(market_type, []).extend(rows)

    snapshots = metadata.get("snapshots")
    cohort_lineage = metadata.get("cohort_lineage")
    require(
        isinstance(snapshots, dict)
        and isinstance(cohort_lineage, dict),
        "Execution cohort metadata is missing",
    )
    expected_market_types = set(selected_market_types)
    require(
        set(snapshots) == expected_market_types
        and set(cohort_lineage) == expected_market_types,
        "Execution cohort metadata is not bounded to selected market types",
    )
    lineage_fields = {
        "market_type",
        "depth_snapshot_id",
        "execution_snapshot_id",
        "execution_source_snapshot_id",
        "depth_market_count",
        "execution_market_count",
    }
    for market_type, rows in selected_market_types.items():
        depth = catalog_metadata.get(f"{market_type}_depth_snapshot")
        snapshot = snapshots.get(market_type)
        lineage = cohort_lineage.get(market_type)
        require(
            isinstance(depth, dict)
            and isinstance(snapshot, dict)
            and isinstance(lineage, dict),
            f"Execution {market_type.upper()} cohort metadata is missing",
        )
        depth_lower, depth_upper = _validate_cohort_observation_metadata(
            depth,
            f"Execution {market_type.upper()} depth cohort",
        )
        execution_lower, execution_upper = (
            _validate_cohort_observation_metadata(
                snapshot,
                f"Execution {market_type.upper()} execution cohort",
            )
        )

        def one_id(value: Any, label: str) -> str:
            require(
                isinstance(value, list)
                and len(value) == 1
                and isinstance(value[0], str)
                and bool(value[0])
                and value[0] == value[0].strip(),
                label,
            )
            return value[0]

        depth_snapshot_id = one_id(
            depth.get("snapshot_ids"),
            f"Execution {market_type.upper()} depth lineage is invalid",
        )
        execution_snapshot_id = one_id(
            snapshot.get("snapshot_ids"),
            f"Execution {market_type.upper()} snapshot lineage is invalid",
        )
        execution_source_snapshot_id = one_id(
            snapshot.get("source_snapshot_ids"),
            f"Execution {market_type.upper()} source lineage is invalid",
        )
        row_snapshot_values = [row.get("snapshot_id") for row in rows]
        row_source_snapshot_values = [
            row.get("source_snapshot_id") for row in rows
        ]
        require(
            all(
                isinstance(value, str)
                and bool(value)
                and value == value.strip()
                for value in (
                    row_snapshot_values + row_source_snapshot_values
                )
            ),
            f"Execution {market_type.upper()} row lineage is invalid",
        )
        row_snapshot_ids = set(row_snapshot_values)
        row_source_snapshot_ids = set(row_source_snapshot_values)
        require(
            {
                depth_snapshot_id,
                execution_snapshot_id,
                execution_source_snapshot_id,
            }
            == {depth_snapshot_id}
            and row_snapshot_ids == {depth_snapshot_id}
            and row_source_snapshot_ids == {depth_snapshot_id},
            f"Execution {market_type.upper()} cohort snapshot IDs differ",
        )
        depth_count_field = (
            "market_rows" if market_type == "cex" else "pool_rows"
        )
        depth_market_count = depth.get(depth_count_field)
        execution_market_count = snapshot.get("market_count")
        require(
            type(depth_market_count) is int
            and depth_market_count > 0
            and type(execution_market_count) is int
            and execution_market_count == depth_market_count,
            f"Execution {market_type.upper()} cohort market counts differ",
        )
        require(
            depth_lower is not None
            and depth_upper is not None
            and execution_lower is not None
            and execution_upper is not None,
            f"Execution {market_type.upper()} positive cohort inventory "
            "lacks observation bounds",
        )
        for index, row in enumerate(rows):
            row_observed_at = _canonical_cohort_timestamp(
                row.get("observed_at"),
                f"Execution {market_type.upper()} selected row {index + 1}",
                "observed_at",
            )
            require(
                execution_lower <= row_observed_at <= execution_upper,
                f"Execution {market_type.upper()} selected row observed_at "
                "is outside declared full-inventory bounds",
            )
        expected_lineage = {
            "market_type": market_type,
            "depth_snapshot_id": depth_snapshot_id,
            "execution_snapshot_id": execution_snapshot_id,
            "execution_source_snapshot_id": execution_source_snapshot_id,
            "depth_market_count": depth_market_count,
            "execution_market_count": execution_market_count,
        }
        require(
            set(lineage) == lineage_fields,
            f"Execution {market_type.upper()} cohort lineage has unknown fields",
        )
        require(
            lineage.get("market_type") == market_type
            and all(
                isinstance(lineage.get(field), str)
                and bool(lineage[field])
                and lineage[field] == lineage[field].strip()
                for field in (
                    "depth_snapshot_id",
                    "execution_snapshot_id",
                    "execution_source_snapshot_id",
                )
            )
            and all(
                type(lineage.get(field)) is int and lineage[field] > 0
                for field in (
                    "depth_market_count",
                    "execution_market_count",
                )
            ),
            f"Execution {market_type.upper()} cohort lineage types are invalid",
        )
        require(
            lineage == expected_lineage,
            f"Execution {market_type.upper()} cohort lineage differs from catalog",
        )
    require(
        has_measured_rows,
        "Execution returned no observed or partial scenario for either market",
    )


def _nested_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        keys = {str(key).lower() for key in value}
        for child in value.values():
            keys.update(_nested_keys(child))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for child in value:
            keys.update(_nested_keys(child))
        return keys
    return set()


def _event_study_result_fields(value: Any) -> set[str]:
    prohibited_terms = {"impact", "return", "returns", "causal", "causality"}
    return {
        key
        for key in _nested_keys(value)
        if key in FORBIDDEN_EVENT_RESULT_FIELDS
        or prohibited_terms.intersection(
            part for part in key.replace("-", "_").split("_") if part
        )
    }


def validate_events(
    payload: dict[str, Any],
    *,
    token: str | None = None,
    start: str | None = None,
    end: str | None = None,
    lifecycle: str | None = None,
    clock_state: str | None = None,
    require_events: bool = True,
) -> list[dict[str, Any]]:
    availability = payload.get("availability") or {}
    require(
        availability.get("status") == "available",
        "Event Fact publication is unavailable",
    )
    require(availability.get("reason") is None, "Available Event feed has a reason")
    require(payload.get("schema") == "event_facts_api/v2", "Wrong Event API schema")
    require(payload.get("fact_schema") == "event_facts/v1", "Wrong Event fact schema")
    boundary = payload.get("fact_boundary")
    require(
        isinstance(boundary, str) and "Source-backed event facts only" in boundary,
        "Event fact boundary is missing",
    )
    require(
        isinstance(payload.get("bundle_id"), str)
        and len(payload["bundle_id"]) == 24
        and all(
            character in "0123456789abcdef"
            for character in payload["bundle_id"]
        ),
        "Event bundle identity is missing",
    )
    require(
        isinstance(payload.get("built_at_utc"), str)
        and payload["built_at_utc"],
        "Event build timestamp is missing",
    )
    clock_as_of_utc = payload.get("clock_as_of_utc")
    require(
        isinstance(clock_as_of_utc, str)
        and UTC_SECOND_RE.fullmatch(clock_as_of_utc) is not None,
        "Event clock_as_of_utc is invalid",
    )
    clock_as_of = datetime.fromisoformat(clock_as_of_utc[:-1] + "+00:00")

    query = payload.get("query") or {}
    require(query.get("token") == token, "Event token scope was not honored")
    require(query.get("start") == start, "Event start scope was not honored")
    require(query.get("end") == end, "Event end scope was not honored")
    require(
        query.get("lifecycle") == lifecycle,
        "Event lifecycle scope was not honored",
    )
    require(
        query.get("clock_state") == clock_state,
        "Event clock-state scope was not honored",
    )
    coverage = payload.get("coverage") or {}
    configured_token_count = coverage.get("configured_token_count")
    covered_token_count = coverage.get("covered_token_count")
    covered_tokens = coverage.get("covered_tokens")
    uncovered_tokens = coverage.get("uncovered_tokens")
    require(
        isinstance(configured_token_count, int)
        and configured_token_count > 0,
        "Event configured-Token count is invalid",
    )
    require(
        isinstance(covered_token_count, int)
        and covered_token_count > 0,
        "Event covered-Token count is invalid",
    )
    require(
        isinstance(covered_tokens, list)
        and all(isinstance(item, str) and item for item in covered_tokens)
        and covered_tokens == sorted(set(covered_tokens)),
        "Event covered-Token inventory is invalid",
    )
    require(
        isinstance(uncovered_tokens, list)
        and all(isinstance(item, str) and item for item in uncovered_tokens)
        and uncovered_tokens == sorted(set(uncovered_tokens)),
        "Event uncovered-Token inventory is invalid",
    )
    require(
        covered_token_count == len(covered_tokens)
        and configured_token_count
        == len(covered_tokens) + len(uncovered_tokens)
        and not set(covered_tokens).intersection(uncovered_tokens),
        "Event Token coverage counts are inconsistent",
    )
    expected_query_coverage = token in set(covered_tokens) if token else None
    require(
        coverage.get("query_token_has_published_fact")
        is expected_query_coverage,
        "Event query-Token coverage flag is inconsistent",
    )

    events = payload.get("events")
    require(isinstance(events, list), "Event response has no events array")
    require(
        payload.get("event_count") == len(events),
        "Event count does not match returned rows",
    )
    for counts_field in (
        "event_type_counts",
        "lifecycle_counts",
        "evidence_status_counts",
    ):
        counts = payload.get(counts_field)
        require(
            isinstance(counts, dict)
            and all(
                isinstance(key, str)
                and isinstance(value, int)
                and value > 0
                for key, value in counts.items()
            )
            and sum(counts.values()) == len(events),
            f"{counts_field} does not match returned Event rows",
        )
    clock_counts = payload.get("clock_state_counts")
    require(
        isinstance(clock_counts, dict)
        and all(
            key in EVENT_CLOCK_STATES
            and isinstance(value, int)
            and value > 0
            for key, value in clock_counts.items()
        ),
        "clock_state_counts is invalid",
    )
    require(
        sum(clock_counts.values()) == len(events),
        "clock_state_counts does not match returned Event rows",
    )
    if require_events:
        require(bool(events), "Event response has no verified records")

    forbidden = _event_study_result_fields(events)
    require(
        not forbidden,
        "Event facts leaked event-study result fields: " + ", ".join(sorted(forbidden)),
    )
    for event in events:
        require(isinstance(event, dict), "Event row is not an object")
        require(
            isinstance(event.get("event_id"), str) and event["event_id"],
            "Event identity is missing",
        )
        require(
            event.get("event_type") in {"unlock", "airdrop", "cex_listing"},
            "Event type is invalid",
        )
        require(
            isinstance(event.get("event_subtype"), str)
            and event["event_subtype"],
            "Event subtype is missing",
        )
        require(
            isinstance(event.get("event_name"), str) and event["event_name"],
            "Event name is missing",
        )
        require(
            isinstance(event.get("revision"), int) and event["revision"] > 0,
            "Event revision is invalid",
        )
        require(
            isinstance(event.get("token_symbol"), str)
            and event["token_symbol"],
            "Event token identity is missing",
        )
        if token is not None:
            require(event["token_symbol"] == token, "Event leaked another Token")
        require(
            event.get("lifecycle") in EVENT_LIFECYCLES,
            "Event lifecycle is invalid",
        )
        if lifecycle is not None:
            require(
                event["lifecycle"] == lifecycle,
                "Event leaked another lifecycle",
            )
        timing = event.get("time") or {}
        clock = event.get("clock") or {}
        current_clock_state = clock.get("state")
        require(
            current_clock_state in EVENT_CLOCK_STATES,
            "Event clock state is invalid",
        )
        if clock_state is not None:
            require(
                current_clock_state == clock_state,
                "Event leaked another clock state",
            )
        require(
            clock.get("as_of_utc") == clock_as_of_utc,
            "Event clock does not use the shared response clock",
        )
        try:
            start_at, end_exclusive, expected_basis = (
                effective_datetime_interval(
                    str(timing.get("effective_at") or ""),
                    str(timing.get("effective_at_precision") or ""),
                )
            )
        except (TypeError, ValueError) as error:
            raise ReleaseCheckError(
                "Event effective time cannot support its clock projection"
            ) from error
        if expected_basis == "exact_instant":
            expected_clock_state = (
                "future"
                if clock_as_of < start_at
                else "past"
                if clock_as_of > start_at
                else "current_window"
            )
        else:
            require(
                end_exclusive is not None,
                "Event effective interval has no exclusive end",
            )
            expected_clock_state = (
                "future"
                if clock_as_of < start_at
                else "past"
                if clock_as_of >= end_exclusive
                else "current_window"
            )
        require(
            current_clock_state == expected_clock_state
            and clock.get("basis") == expected_basis,
            "Event clock projection disagrees with effective time",
        )
        require(
            event.get("evidence_status") in EVENT_EVIDENCE_STATUSES,
            "Event evidence status is invalid",
        )

        effective_start = timing.get("effective_date_start")
        effective_end = timing.get("effective_date_end")
        require(
            isinstance(effective_start, str)
            and isinstance(effective_end, str)
            and effective_start <= effective_end,
            "Event effective date interval is invalid",
        )
        if start is not None:
            require(effective_end >= start, "Event is before requested window")
        if end is not None:
            require(effective_start <= end, "Event is after requested window")

        source = event.get("source") or {}
        require(
            isinstance(source.get("kind"), str) and source["kind"],
            "Event source kind is missing",
        )
        require(
            isinstance(source.get("url"), str)
            and source["url"].startswith("https://"),
            "Event source URL is not HTTPS",
        )
        require(
            isinstance(source.get("checked_at_utc"), str)
            and source["checked_at_utc"],
            "Event source check timestamp is missing",
        )
        require(
            isinstance(source.get("record_sha256"), str)
            and len(source["record_sha256"]) == 64
            and all(
                character in "0123456789abcdef"
                for character in source["record_sha256"]
            ),
            "Event source-record checksum is invalid",
        )
        require(
            isinstance(source.get("record_locator"), str)
            and source["record_locator"],
            "Event source-record locator is missing",
        )

        lineage = event.get("revision_lineage") or {}
        require(
            isinstance(lineage.get("recorded_at_utc"), str)
            and lineage["recorded_at_utc"],
            "Event revision timestamp is missing",
        )
        require(
            isinstance(lineage.get("reason"), str) and lineage["reason"],
            "Event revision reason is missing",
        )
    expected_counts = {
        "event_type_counts": dict(
            sorted(Counter(event["event_type"] for event in events).items())
        ),
        "lifecycle_counts": dict(
            sorted(Counter(event["lifecycle"] for event in events).items())
        ),
        "evidence_status_counts": dict(
            sorted(Counter(event["evidence_status"] for event in events).items())
        ),
    }
    expected_counts["clock_state_counts"] = dict(
        sorted(Counter(event["clock"]["state"] for event in events).items())
    )
    for counts_field, expected in expected_counts.items():
        require(
            payload.get(counts_field) == expected,
            f"{counts_field} does not match returned Event rows",
        )
    return events


def release_check(args: argparse.Namespace) -> dict[str, Any]:
    route_opportunity_validation = validate_route_opportunity_release(
        configured_route_root(),
        required=getattr(args, "require_route_cohort", False),
    )
    metrics: list[ResponseMetrics] = []
    health, health_metrics = fetch_json(
        args.base_url,
        "/health",
        timeout=args.timeout,
    )
    metrics.append(health_metrics)
    require(health.get("status") == "ok", "Health status is not ok")
    require(health.get("data_ready") is True, "Health reports data_ready=false")
    application_sha, asset_sha, asset_version = validate_release_health(
        health,
        expected_application_sha=getattr(args, "expected_application_sha", None),
        expected_asset_sha=getattr(args, "expected_asset_sha", None),
    )
    served_asset_sha, asset_metrics = fetch_static_asset_bundle(
        args.base_url,
        asset_version,
        timeout=args.timeout,
    )
    metrics.extend(asset_metrics)
    require(
        served_asset_sha == asset_sha,
        "Versioned served assets do not match the deployed asset SHA",
    )

    summary, summary_metrics = fetch_json(
        args.base_url,
        "/api/markets/summary",
        timeout=args.timeout,
    )
    metrics.append(summary_metrics)
    token, start, end, generation = validate_summary(
        summary,
        summary_metrics,
        raw_max=args.summary_raw_max,
        gzip_max=args.summary_gzip_max,
    )

    screening_quality_parity_count = 0
    screening_quality_market_count = 0
    audited_market_pairs: set[tuple[str, str]] = set()
    audited_market_ids: set[str] = set()
    for summary_row in summary["tokens"]:
        quality_token = summary_row.get("token_symbol")
        require(
            isinstance(quality_token, str) and bool(quality_token),
            "Summary Token is invalid for screening parity",
        )
        screening_quality_path = "/api/markets/quality?" + urlencode(
            {"token": quality_token, "scope": "all"}
        )
        screening_quality, screening_quality_metrics = fetch_json(
            args.base_url,
            screening_quality_path,
            timeout=args.timeout,
        )
        metrics.append(screening_quality_metrics)
        parity = validate_screening_quality_parity(
            summary_row,
            screening_quality,
            expected_generation=generation,
        )
        screening_quality_parity_count += 1
        screening_quality_market_count += parity["market_count"]
        for market_id in parity["market_ids"]:
            market_pair = (quality_token, market_id)
            require(
                market_id not in audited_market_ids,
                "Screening Quality market ID is reused across Tokens",
            )
            require(
                market_pair not in audited_market_pairs,
                "Screening Quality market identity is duplicated",
            )
            audited_market_ids.add(market_id)
            audited_market_pairs.add(market_pair)

    summary_metadata = summary.get("metadata")
    require(isinstance(summary_metadata, dict), "Summary metadata is invalid")
    declared_token_count = summary_metadata.get("token_count")
    declared_market_count = summary_metadata.get("catalog_market_count")
    require(
        type(declared_token_count) is int
        and screening_quality_parity_count == declared_token_count,
        "Screening parity Token count does not match Summary token_count",
    )
    require(
        type(declared_market_count) is int
        and screening_quality_market_count == declared_market_count,
        "Screening parity market count does not match Summary catalog_market_count",
    )
    summary_token_set = {
        row["token_symbol"]
        for row in summary["tokens"]
        if isinstance(row, dict) and isinstance(row.get("token_symbol"), str)
    }
    summary_primary_market_pairs = {
        (row["token_symbol"], primary["refresh_market_id"])
        for row in summary["tokens"]
        if isinstance(row, dict) and isinstance(row.get("token_symbol"), str)
        for primary in (row.get("primary_cex"), row.get("primary_dex"))
        if isinstance(primary, dict)
        and isinstance(primary.get("refresh_market_id"), str)
    }

    catalog_path = "/api/markets/catalog?" + urlencode(
        {"token": token, "start": start, "end": end}
    )
    token_catalog, token_metrics = fetch_json(
        args.base_url,
        catalog_path,
        timeout=args.timeout,
    )
    metrics.append(token_metrics)
    markets = validate_token_catalog(
        token_catalog,
        token_metrics,
        token=token,
        start=start,
        end=end,
        generation=generation,
        raw_max=args.token_raw_max,
        gzip_max=args.token_gzip_max,
    )
    require(
        token_catalog.get("metadata", {}).get(
            "configured_cex_market_identities"
        )
        == summary_metadata.get("configured_cex_market_identities"),
        "Summary and Token catalog configured Upbit identities differ",
    )
    token_catalog_market_ids = {
        str(market["market_id"])
        for market in markets
        if isinstance(market, dict)
    }

    full_catalog, full_metrics = fetch_json(
        args.base_url,
        "/api/markets/catalog",
        timeout=args.timeout,
    )
    metrics.append(full_metrics)
    full_catalog_metadata = full_catalog.get("metadata")
    require(
        isinstance(full_catalog_metadata, dict)
        and full_catalog_metadata.get("data_generation") == generation,
        "Summary and full catalog generation differ",
    )
    configured_upbit_market_ids = (
        validate_configured_cex_identity_metadata(full_catalog_metadata)
    )
    require(
        full_catalog_metadata.get("configured_cex_market_identities")
        == summary_metadata.get("configured_cex_market_identities"),
        "Summary and full catalog configured Upbit identities differ",
    )
    full_markets = full_catalog.get("markets")
    require(isinstance(full_markets, list), "Full audit catalog has no markets array")
    full_catalog_tokens: set[str] = set()
    full_market_ids: set[str] = set()
    full_market_pairs: set[tuple[str, str]] = set()
    for market in full_markets:
        require(isinstance(market, dict), "Full audit catalog market is not an object")
        market_token = market.get("token_symbol")
        require(
            isinstance(market_token, str)
            and bool(market_token)
            and market_token == market_token.strip().upper()
            and market_token in summary_token_set,
            "Full audit catalog market Token identity is invalid",
        )
        market_id = market.get("market_id")
        require(
            isinstance(market_id, str)
            and bool(market_id)
            and market_id == market_id.strip(),
            "Full audit catalog market ID is invalid",
        )
        require(
            market_id not in full_market_ids,
            "Full audit catalog market IDs are not unique",
        )
        full_market_ids.add(market_id)
        full_market_pairs.add((market_token, market_id))
        full_catalog_tokens.add(market_token)
        validate_exact_cex_market_identity(
            market_id,
            market_token,
            configured_upbit_market_ids=configured_upbit_market_ids,
        )
    require(
        full_catalog_tokens == summary_token_set,
        "Full audit catalog Token inventory differs from Summary",
    )
    require(
        len(full_markets) == declared_market_count,
        "Summary catalog count differs from the full audit catalog",
    )
    require(
        screening_quality_market_count == len(full_markets),
        "Screening parity market count differs from the full audit catalog",
    )
    require(
        audited_market_pairs == full_market_pairs,
        "Screening Quality exact market inventory differs from the full catalog",
    )
    lifecycle = summary_metadata.get("cex_instrument_lifecycle")
    require(isinstance(lifecycle, dict), "Summary lifecycle catalog is missing")
    crypto_com_market_ids = {
        market_id
        for market_id in full_market_ids
        if market_id.startswith("cex:crypto_com:")
    }
    try:
        crypto_com_market_hash = configured_market_ids_sha256(
            crypto_com_market_ids
        )
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "Full lifecycle catalog identity inventory is invalid"
        ) from error
    require(
        len(crypto_com_market_ids) == lifecycle["reviewed_market_count"]
        and crypto_com_market_hash
        == lifecycle["configured_market_ids_sha256"],
        "Summary lifecycle catalog does not match the full catalog",
    )
    require(
        summary_primary_market_pairs <= full_market_pairs,
        "Summary primary market refresh identity is absent from the full catalog",
    )
    require(
        token_catalog_market_ids
        == {
            market_id
            for market_token, market_id in full_market_pairs
            if market_token == token
        },
        "Token catalog inventory differs from the full audit catalog",
    )

    all_events, events_metrics = fetch_json(
        args.base_url,
        "/api/markets/events",
        timeout=args.timeout,
    )
    metrics.append(events_metrics)
    event_rows = validate_events(all_events)
    event_coverage = all_events["coverage"]
    summary_tokens = sorted(
        row["token_symbol"]
        for row in summary["tokens"]
        if isinstance(row, dict) and row.get("token_symbol")
    )
    require(
        event_coverage["covered_tokens"] == summary_tokens,
        "Event coverage does not match the current Token catalog",
    )
    require(
        event_coverage["uncovered_tokens"] == [],
        "Event publication leaves configured Tokens uncovered",
    )
    for covered_token in event_coverage["covered_tokens"]:
        token_events_path = "/api/markets/events?" + urlencode(
            {"token": covered_token}
        )
        token_events, token_events_metrics = fetch_json(
            args.base_url,
            token_events_path,
            timeout=args.timeout,
        )
        metrics.append(token_events_metrics)
        validate_events(token_events, token=covered_token)
    seed_event = event_rows[0]
    event_token = seed_event["token_symbol"]
    event_start = seed_event["time"]["effective_date_start"]
    event_end = seed_event["time"]["effective_date_end"]
    event_lifecycle = seed_event["lifecycle"]
    event_clock_state = seed_event["clock"]["state"]
    scoped_events_path = "/api/markets/events?" + urlencode(
        {
            "token": event_token,
            "start": event_start,
            "end": event_end,
            "lifecycle": event_lifecycle,
            "clock_state": event_clock_state,
        }
    )
    scoped_events, scoped_events_metrics = fetch_json(
        args.base_url,
        scoped_events_path,
        timeout=args.timeout,
    )
    metrics.append(scoped_events_metrics)
    validate_events(
        scoped_events,
        token=event_token,
        start=event_start,
        end=event_end,
        lifecycle=event_lifecycle,
        clock_state=event_clock_state,
    )

    token_summary = token_catalog.get("token_summary") or {}
    market_ids = [row.get("market_id") for row in markets if row.get("market_id")]
    market_a = next(
        (
            row.get("market_id")
            for row in markets
            if row.get("market_type") == "cex"
            and f"{row.get('venue')}|{row.get('instrument')}"
            == token_summary.get("primary_cex_id")
        ),
        None,
    )
    market_b = next(
        (
            row.get("market_id")
            for row in markets
            if row.get("market_type") == "dex"
            and row.get("pool_address") == token_summary.get("primary_dex_id")
        ),
        None,
    )
    if market_a not in market_ids:
        market_a = next(
            (
                row.get("market_id")
                for row in markets
                if row.get("market_type") == "cex" and row.get("market_id")
            ),
            market_ids[0] if market_ids else None,
        )
    if market_b not in market_ids or market_b == market_a:
        market_b = next(
            (
                row.get("market_id")
                for row in markets
                if row.get("market_type") == "dex"
                and row.get("market_id") != market_a
            ),
            next(
                (market_id for market_id in market_ids if market_id != market_a),
                None,
            ),
        )
    require(market_a is not None and market_b is not None, "No distinct smoke-test pair")
    common_query = {
        "token": token,
        "market_a": market_a,
        "market_b": market_b,
    }
    comparison_path = "/api/markets/compare?" + urlencode(
        {**common_query, "start": start, "end": end}
    )
    quality_path = "/api/markets/quality?" + urlencode(
        {**common_query, "scope": "selected"}
    )
    execution_path = "/api/markets/execution-cost?" + urlencode(common_query)
    comparison, comparison_metrics = fetch_json(
        args.base_url,
        comparison_path,
        timeout=args.timeout,
    )
    metrics.append(comparison_metrics)
    validate_comparison(
        comparison,
        token=token,
        market_a=market_a,
        market_b=market_b,
        start=start,
        end=end,
        expected_generation=generation,
        expected_comparison_generation=generation,
    )

    quality, quality_metrics = fetch_json(
        args.base_url,
        quality_path,
        timeout=args.timeout,
    )
    metrics.append(quality_metrics)
    validate_quality(
        quality,
        token=token,
        market_a=market_a,
        market_b=market_b,
        expected_generation=generation,
    )

    execution, execution_metrics = fetch_json(
        args.base_url,
        execution_path,
        timeout=args.timeout,
    )
    metrics.append(execution_metrics)
    validate_execution(
        execution,
        token=token,
        market_a=market_a,
        market_b=market_b,
        expected_generation=generation,
        expected_execution_generation=generation,
        catalog_metadata=full_catalog.get("metadata") or {},
    )

    final_health, final_health_metrics = fetch_json(
        args.base_url,
        "/health",
        timeout=args.timeout,
    )
    metrics.append(final_health_metrics)
    require(final_health.get("status") == "ok", "Final Health status is not ok")
    require(
        final_health.get("data_ready") is True,
        "Final Health reports data_ready=false",
    )
    final_application_sha, final_asset_sha, final_asset_version = (
        validate_release_health(
            final_health,
            expected_application_sha=application_sha,
            expected_asset_sha=asset_sha,
        )
    )
    require(
        (final_application_sha, final_asset_sha, final_asset_version)
        == (application_sha, asset_sha, asset_version),
        "Application or frontend assets changed during release validation",
    )
    final_served_asset_sha, final_asset_metrics = fetch_static_asset_bundle(
        args.base_url,
        final_asset_version,
        timeout=args.timeout,
    )
    metrics.extend(final_asset_metrics)
    require(
        final_served_asset_sha == final_asset_sha,
        "Final versioned served assets do not match the deployed asset SHA",
    )

    final_summary, final_summary_metrics = fetch_json(
        args.base_url,
        "/api/markets/summary",
        timeout=args.timeout,
    )
    metrics.append(final_summary_metrics)
    final_summary_metadata = final_summary.get("metadata") or {}
    require(
        final_summary_metadata.get("data_generation") == generation,
        "Published data generation changed during release validation",
    )
    final_freshness_checked_at = validate_source_freshness(
        final_summary_metadata.get("freshness"),
        label="Final Summary",
    )
    validate_lifecycle_freshness(
        final_summary_metadata.get("cex_instrument_lifecycle"),
        freshness_checked_at=final_freshness_checked_at,
    )

    return {
        "status": "ok",
        "base_url": args.base_url,
        "token": token,
        "window": {"start": start, "end": end},
        "data_generation": generation,
        "application_sha": application_sha,
        "asset_sha": asset_sha,
        "asset_version": asset_version,
        "token_count": len(summary["tokens"]),
        "screening_quality_parity_count": screening_quality_parity_count,
        "screening_quality_market_count": screening_quality_market_count,
        "catalog_market_count": len(full_markets),
        "event_count": len(event_rows),
        "event_covered_token_count": event_coverage["covered_token_count"],
        "event_bundle_id": all_events["bundle_id"],
        "route_opportunities": route_opportunity_validation,
        "requests": [
            {
                "path": item.path,
                "elapsed_ms": round(item.elapsed_ms, 2),
                "wire_bytes": item.wire_bytes,
                "raw_bytes": item.raw_bytes,
                "gzip": item.compressed,
            }
            for item in metrics
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8765")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--summary-raw-max", type=int, default=100_000)
    parser.add_argument("--summary-gzip-max", type=int, default=25_000)
    parser.add_argument("--token-raw-max", type=int, default=250_000)
    parser.add_argument("--token-gzip-max", type=int, default=50_000)
    parser.add_argument(
        "--expected-application-sha",
        help="Require /health to report this exact deployed Git SHA",
    )
    parser.add_argument(
        "--expected-asset-sha",
        help="Require /health to report this exact deployed frontend asset SHA",
    )
    parser.add_argument(
        "--require-route-cohort",
        "--require-route-opportunities",
        dest="require_route_cohort",
        action="store_true",
        help=(
            "Fail unless the complete public route-opportunity pointer and "
            "all-in cost bundle are available and valid"
        ),
    )
    return parser.parse_args()


def main() -> int:
    try:
        result = release_check(parse_args())
    except ReleaseCheckError as error:
        print(json.dumps({"status": "failed", "error": str(error)}))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
