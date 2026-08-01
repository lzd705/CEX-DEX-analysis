"""Normalized route execution-cost component fact contract.

The contract deliberately separates numeric evidence from terminal states and
keeps embedded leg costs out of additive route totals.  Every stored Decimal is
canonical base-10 text; binary floating-point values are rejected at the API
boundary.
"""

from __future__ import annotations

import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

try:
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


COST_COMPONENT_CONTRACT_VERSION = "1"

COMPONENT_TYPES = {
    "venue_taker_fee",
    "pool_swap_fee",
    "network_gas",
    "router_or_integrator_fee",
    "token_transfer_tax",
    "rebalancing_or_transfer",
    "mev_buffer",
}

VALUE_STATUSES = {
    "measured",
    "authenticated",
    "quoted",
    "bounded_estimate",
    "assumed",
    "not_applicable",
    "unavailable",
    "unsupported",
    "failed",
    "stale",
}

REQUIRED_STRICT_COMPONENT_TYPES = COMPONENT_TYPES - {"mev_buffer"}

NUMERIC_VALUE_STATUSES = {
    "measured",
    "authenticated",
    "quoted",
    "bounded_estimate",
    "assumed",
}
STRICT_VALUE_STATUSES = {
    "measured",
    "authenticated",
    "quoted",
    "not_applicable",
}
SCENARIO_VALUE_STATUSES = {"bounded_estimate", "assumed"}
TERMINAL_VALUE_STATUSES = {"unavailable", "unsupported", "failed", "stale"}
LINEAGED_VALUE_STATUSES = {"measured", "authenticated", "quoted"}
EXPIRING_VALUE_STATUSES = {"authenticated", "quoted"}
MEV_BUFFER_VALUE_STATUSES = SCENARIO_VALUE_STATUSES | TERMINAL_VALUE_STATUSES

COST_COMPONENT_COLUMNS = (
    "contract_version",
    "cohort_id",
    "opportunity_id",
    "leg",
    "market_id",
    "direction",
    "requested_notional_usd",
    "target_token_quantity",
    "component_type",
    "value_status",
    "amount_usd",
    "rate_bps",
    "basis",
    "strict_eligible",
    "embedded_in_leg_quote",
    "observed_at",
    "valid_until",
    "source",
    "source_record_sha256",
    "reason_code",
)

_LEGS = {"buy", "sell", "route"}
_DIRECTIONS_BY_LEG = {
    "buy": "buy_token",
    "sell": "sell_token",
    "route": "route",
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON_CODE = re.compile(r"[a-z][a-z0-9_]*\Z")


def _required_text(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("{} must be canonical text".format(field))
    if not allow_empty and not value:
        raise ValueError("{} must be non-empty".format(field))
    return value


def _optional_text(value: Any, field: str) -> Optional[str]:
    if value in (None, ""):
        return None
    return _required_text(value, field)


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)):
        raise ValueError("{} must be an exact Decimal value".format(field))
    if not isinstance(value, (Decimal, int, str)):
        raise ValueError("{} must be an exact Decimal value".format(field))
    if isinstance(value, str) and (not value or value != value.strip()):
        raise ValueError("{} must be canonical decimal text".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("{} must be a finite Decimal value".format(field)) from error
    if not number.is_finite():
        raise ValueError("{} must be a finite Decimal value".format(field))
    if positive and number <= 0:
        raise ValueError("{} must be positive".format(field))
    if not positive and number < 0:
        raise ValueError("{} must be non-negative".format(field))
    return number


def _decimal_text(number: Decimal) -> str:
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _decimal_coefficient_and_exponent(number: Decimal) -> Tuple[int, int]:
    """Return the exact ``coefficient * 10 ** exponent`` representation."""
    decimal_tuple = number.as_tuple()
    coefficient = 0
    for digit in decimal_tuple.digits:
        coefficient = coefficient * 10 + digit
    if decimal_tuple.sign:
        coefficient = -coefficient
    return coefficient, int(decimal_tuple.exponent)


def _scaled_integers_are_equal(
    left: Tuple[int, int], right: Tuple[int, int]
) -> bool:
    """Compare two scaled integers without using the Decimal context."""
    left_coefficient, left_exponent = left
    right_coefficient, right_exponent = right
    common_exponent = min(left_exponent, right_exponent)
    return (
        left_coefficient * (10 ** (left_exponent - common_exponent))
        == right_coefficient * (10 ** (right_exponent - common_exponent))
    )


def _amount_and_rate_recompute(
    amount: Decimal, rate: Decimal, requested_notional: Decimal
) -> bool:
    amount_coefficient, amount_exponent = _decimal_coefficient_and_exponent(
        amount
    )
    rate_coefficient, rate_exponent = _decimal_coefficient_and_exponent(rate)
    notional_coefficient, notional_exponent = (
        _decimal_coefficient_and_exponent(requested_notional)
    )
    return _scaled_integers_are_equal(
        (amount_coefficient * 10_000, amount_exponent),
        (
            rate_coefficient * notional_coefficient,
            rate_exponent + notional_exponent,
        ),
    )


def _exact_decimal_sum(numbers: Iterable[Decimal]) -> Decimal:
    """Sum finite Decimals exactly using arbitrary-precision integers."""
    parts = [_decimal_coefficient_and_exponent(number) for number in numbers]
    if not parts:
        return Decimal(0)
    common_exponent = min(exponent for _coefficient, exponent in parts)
    coefficient = sum(
        value * (10 ** (exponent - common_exponent))
        for value, exponent in parts
    )
    if coefficient == 0:
        return Decimal(0)
    sign = 1 if coefficient < 0 else 0
    digits = tuple(int(character) for character in str(abs(coefficient)))
    return Decimal((sign, digits, common_exponent))


def _normalized_decimal(value: Any, field: str, *, positive: bool = False) -> str:
    return _decimal_text(_decimal(value, field, positive=positive))


def _optional_normalized_decimal(value: Any, field: str) -> Optional[str]:
    if value in (None, ""):
        return None
    return _normalized_decimal(value, field)


def _stored_decimal(row: Mapping[str, Any], field: str, *, positive: bool) -> Decimal:
    value = row.get(field)
    if not isinstance(value, str):
        raise ValueError("{} must be stored as exact decimal text".format(field))
    number = _decimal(value, field, positive=positive)
    if value != _decimal_text(number):
        raise ValueError("{} must use canonical decimal text".format(field))
    return number


def _validate_timestamp(value: Optional[str], field: str) -> Optional[Decimal]:
    if value is None:
        return None
    _required_text(value, field)
    try:
        return exact_rfc3339_epoch_seconds(value)
    except ValueError as error:
        raise ValueError("{} must be timezone-aware RFC 3339 text".format(field)) from error


def _validate_row(row: Mapping[str, Any]) -> None:
    if any(not isinstance(key, str) for key in row):
        raise ValueError("cost component schema keys must be strings")
    missing = [column for column in COST_COMPONENT_COLUMNS if column not in row]
    extra = sorted(set(row) - set(COST_COMPONENT_COLUMNS))
    if missing or extra:
        details = []
        if missing:
            details.append("missing {}".format(", ".join(missing)))
        if extra:
            details.append("unknown {}".format(", ".join(extra)))
        raise ValueError("cost component schema is invalid: " + "; ".join(details))

    if row.get("contract_version") != COST_COMPONENT_CONTRACT_VERSION:
        raise ValueError("cost component contract_version is invalid")
    _required_text(row.get("cohort_id"), "cohort_id")
    _required_text(row.get("opportunity_id"), "opportunity_id")
    leg = _required_text(row.get("leg"), "leg")
    if leg not in _LEGS:
        raise ValueError("leg is invalid")
    market_id = _required_text(row.get("market_id"), "market_id", allow_empty=True)
    direction = _required_text(row.get("direction"), "direction")
    if direction != _DIRECTIONS_BY_LEG[leg]:
        raise ValueError("direction does not match leg")
    if leg == "route" and market_id:
        raise ValueError("route-level component market_id must be blank")
    if leg != "route" and not market_id:
        raise ValueError("leg-level component market_id must be non-empty")

    requested_notional = _stored_decimal(
        row, "requested_notional_usd", positive=True
    )
    _stored_decimal(row, "target_token_quantity", positive=True)

    component_type = _required_text(row.get("component_type"), "component_type")
    if component_type not in COMPONENT_TYPES:
        raise ValueError("component_type is invalid")
    value_status = _required_text(row.get("value_status"), "value_status")
    if value_status not in VALUE_STATUSES:
        raise ValueError("value_status is invalid")

    strict_eligible = row.get("strict_eligible")
    embedded = row.get("embedded_in_leg_quote")
    if type(strict_eligible) is not bool:  # bool is the storage contract, not 0/1.
        raise ValueError("strict_eligible must be boolean")
    if type(embedded) is not bool:
        raise ValueError("embedded_in_leg_quote must be boolean")
    if (
        component_type == "mev_buffer"
        and (
            value_status not in MEV_BUFFER_VALUE_STATUSES
            or strict_eligible
        )
    ):
        raise ValueError(
            "{} mev_buffer is scenario-only and cannot be strict".format(
                value_status
            )
        )
    if strict_eligible and value_status not in STRICT_VALUE_STATUSES:
        raise ValueError(
            "{} component cannot be strict eligible".format(value_status)
        )
    if value_status in SCENARIO_VALUE_STATUSES and strict_eligible:
        raise ValueError(
            "{} component cannot be strict eligible".format(value_status)
        )
    if value_status in TERMINAL_VALUE_STATUSES and strict_eligible:
        raise ValueError(
            "{} component cannot be strict eligible".format(value_status)
        )
    if value_status == "not_applicable" and not strict_eligible:
        raise ValueError("not_applicable requires strict route-contract proof")

    basis = _required_text(row.get("basis"), "basis", allow_empty=True)
    _required_text(row.get("source"), "source")
    amount_value = row.get("amount_usd")
    rate_value = row.get("rate_bps")
    if value_status in NUMERIC_VALUE_STATUSES:
        amount = _stored_decimal(row, "amount_usd", positive=False)
        rate = _stored_decimal(row, "rate_bps", positive=False)
        if not _amount_and_rate_recompute(amount, rate, requested_notional):
            raise ValueError(
                "amount_usd and rate_bps do not recompute from requested_notional_usd"
            )
        if not basis:
            raise ValueError("numeric component requires basis")
    elif value_status == "not_applicable":
        if amount_value is not None or rate_value is not None:
            raise ValueError("not_applicable component cannot contain numeric values")
        if not basis:
            raise ValueError("not_applicable component requires proof basis")
    else:
        if amount_value is not None or rate_value is not None:
            raise ValueError(
                "{} component cannot contain numeric values".format(value_status)
            )
        if not basis:
            raise ValueError("{} component requires basis".format(value_status))

    if embedded and component_type != "pool_swap_fee":
        raise ValueError("embedded cost must be a pool_swap_fee")
    if (
        component_type == "pool_swap_fee"
        and value_status in NUMERIC_VALUE_STATUSES
        and not embedded
    ):
        raise ValueError("pool_swap_fee must be embedded in the leg quote")
    if embedded and value_status not in NUMERIC_VALUE_STATUSES:
        raise ValueError("embedded pool_swap_fee must contain numeric evidence")

    observed_at = _optional_text(row.get("observed_at"), "observed_at")
    valid_until = _optional_text(row.get("valid_until"), "valid_until")
    observed_epoch = _validate_timestamp(observed_at, "observed_at")
    valid_epoch = _validate_timestamp(valid_until, "valid_until")
    source_hash = _optional_text(
        row.get("source_record_sha256"), "source_record_sha256"
    )
    if source_hash is not None and _SHA256.fullmatch(source_hash) is None:
        raise ValueError("source_record_sha256 must be lowercase 64-hex text")
    if value_status in LINEAGED_VALUE_STATUSES:
        if observed_at is None:
            raise ValueError("{} requires observed_at lineage".format(value_status))
        if source_hash is None:
            raise ValueError(
                "{} requires source_record_sha256 lineage".format(value_status)
            )
    if value_status in EXPIRING_VALUE_STATUSES and valid_until is None:
        raise ValueError("{} requires valid_until lineage".format(value_status))
    if valid_epoch is not None:
        if observed_epoch is None:
            raise ValueError("valid_until requires observed_at")
        if valid_epoch <= observed_epoch:
            raise ValueError("valid_until must be after observed_at")

    reason_code = _optional_text(row.get("reason_code"), "reason_code")
    if reason_code is not None and _REASON_CODE.fullmatch(reason_code) is None:
        raise ValueError("reason_code must be canonical lowercase identifier text")
    if value_status in TERMINAL_VALUE_STATUSES and reason_code is None:
        raise ValueError("{} component requires reason_code".format(value_status))


def cost_component_row(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    direction: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    component_type: str,
    value_status: str,
    amount_usd: Any,
    rate_bps: Any,
    basis: str,
    strict_eligible: bool,
    observed_at: Optional[str],
    valid_until: Optional[str],
    source: str,
    source_record_sha256: Optional[str],
    embedded_in_leg_quote: bool = False,
    reason_code: Optional[str] = None,
) -> Dict[str, Any]:
    """Build and validate one canonical component row."""
    row: Dict[str, Any] = {
        "contract_version": COST_COMPONENT_CONTRACT_VERSION,
        "cohort_id": cohort_id,
        "opportunity_id": opportunity_id,
        "leg": leg,
        "market_id": market_id,
        "direction": direction,
        "requested_notional_usd": _normalized_decimal(
            requested_notional_usd, "requested_notional_usd", positive=True
        ),
        "target_token_quantity": _normalized_decimal(
            target_token_quantity, "target_token_quantity", positive=True
        ),
        "component_type": component_type,
        "value_status": value_status,
        "amount_usd": _optional_normalized_decimal(amount_usd, "amount_usd"),
        "rate_bps": _optional_normalized_decimal(rate_bps, "rate_bps"),
        "basis": basis,
        "strict_eligible": strict_eligible,
        "embedded_in_leg_quote": embedded_in_leg_quote,
        "observed_at": _optional_text(observed_at, "observed_at"),
        "valid_until": _optional_text(valid_until, "valid_until"),
        "source": source,
        "source_record_sha256": _optional_text(
            source_record_sha256, "source_record_sha256"
        ),
        "reason_code": _optional_text(reason_code, "reason_code"),
    }
    _validate_row(row)
    return row


def validate_cost_components(rows: Iterable[Mapping[str, Any]]) -> None:
    """Validate schema, fixed grain, and scenario-wide quantity identity."""
    if isinstance(rows, (str, bytes, Mapping)):
        raise ValueError("cost component rows must be an iterable of mappings")
    inventory = list(rows)
    keys: Set[Tuple[str, str, str, str]] = set()
    scenario_values: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)
    leg_values: Dict[Tuple[str, str, str], Set[Tuple[str, str]]] = defaultdict(set)
    for row in inventory:
        if not isinstance(row, Mapping):
            raise ValueError("cost component row must be a mapping")
        _validate_row(row)
        key = (
            str(row["cohort_id"]),
            str(row["opportunity_id"]),
            str(row["leg"]),
            str(row["component_type"]),
        )
        if key in keys:
            raise ValueError("duplicate cost component at fixed contract grain")
        keys.add(key)
        scenario_key = (str(row["cohort_id"]), str(row["opportunity_id"]))
        scenario_values[scenario_key].add(
            (
                str(row["requested_notional_usd"]),
                str(row["target_token_quantity"]),
            )
        )
        leg_values[(scenario_key[0], scenario_key[1], str(row["leg"]))].add(
            (str(row["market_id"]), str(row["direction"]))
        )
    if any(len(values) != 1 for values in scenario_values.values()):
        raise ValueError(
            "one opportunity must retain one requested_notional_usd and "
            "target_token_quantity"
        )
    if any(len(values) != 1 for values in leg_values.values()):
        raise ValueError("one opportunity leg must retain one market_id and direction")


def _strict_row_is_complete(row: Mapping[str, Any]) -> bool:
    return (
        row["component_type"] != "mev_buffer"
        and bool(row["strict_eligible"])
        and row["value_status"] in STRICT_VALUE_STATUSES
    )


def _scenario_row_is_complete(
    row: Mapping[str, Any], include_assumptions: bool
) -> bool:
    if _strict_row_is_complete(row):
        return True
    return include_assumptions and row["value_status"] in SCENARIO_VALUE_STATUSES


def _sum_nonembedded(
    rows: Iterable[Mapping[str, Any]], predicate: Any
) -> Decimal:
    amounts = []
    for row in rows:
        if not predicate(row) or row["embedded_in_leg_quote"]:
            continue
        amount = row["amount_usd"]
        if amount is not None:
            amounts.append(Decimal(amount))
    return _exact_decimal_sum(amounts)


def aggregate_cost_components(
    rows: Iterable[Mapping[str, Any]], include_assumptions: bool
) -> Dict[str, Any]:
    """Aggregate one opportunity without turning missing evidence into zero."""
    if type(include_assumptions) is not bool:
        raise ValueError("include_assumptions must be boolean")
    inventory = list(rows)
    validate_cost_components(inventory)
    scenario_ids = {
        (str(row["cohort_id"]), str(row["opportunity_id"]))
        for row in inventory
    }
    if len(scenario_ids) > 1:
        raise ValueError("aggregate_cost_components accepts one opportunity")

    by_type: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in inventory:
        by_type[str(row["component_type"])].append(row)

    missing_strict = sorted(
        component_type
        for component_type in REQUIRED_STRICT_COMPONENT_TYPES
        if not by_type.get(component_type)
        or any(
            not _strict_row_is_complete(row)
            for row in by_type[component_type]
        )
    )
    missing_scenario = sorted(
        component_type
        for component_type in REQUIRED_STRICT_COMPONENT_TYPES
        if not by_type.get(component_type)
        or any(
            not _scenario_row_is_complete(row, include_assumptions)
            for row in by_type[component_type]
        )
    )

    strict_total = _sum_nonembedded(inventory, _strict_row_is_complete)
    scenario_total = _sum_nonembedded(
        inventory,
        lambda row: _scenario_row_is_complete(row, include_assumptions),
    )
    return {
        "strict_amount_usd": (
            None if missing_strict else _decimal_text(strict_total)
        ),
        "scenario_amount_usd": (
            None if missing_scenario else _decimal_text(scenario_total)
        ),
        "missing_required_kinds": missing_strict,
        "scenario_missing_required_kinds": missing_scenario,
        "completeness": "incomplete" if missing_strict else "complete",
        "scenario_completeness": (
            "incomplete" if missing_scenario else "complete"
        ),
    }
