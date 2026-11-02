"""Closed live and historical Opportunity cost-component topologies."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from fractions import Fraction
import json
import re
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Sequence, Tuple

try:
    from scripts.execution_cost_components import (
        COST_COMPONENT_COLUMNS,
        COST_COMPONENT_CONTRACT_VERSION,
        cost_component_row,
    )
except ModuleNotFoundError:
    from execution_cost_components import (  # type: ignore[no-redef]
        COST_COMPONENT_COLUMNS,
        COST_COMPONENT_CONTRACT_VERSION,
        cost_component_row,
    )


HistoricalAtomicComponentShape = Tuple[str, str, str, bool]

HISTORICAL_ATOMIC_COMPONENT_MATRIX: Tuple[
    HistoricalAtomicComponentShape, ...
] = (
    ("buy", "pool_swap_fee", "bounded_estimate", True),
    ("buy", "router_or_integrator_fee", "bounded_estimate", False),
    ("buy", "token_transfer_tax", "bounded_estimate", False),
    ("sell", "pool_swap_fee", "bounded_estimate", True),
    ("sell", "router_or_integrator_fee", "bounded_estimate", False),
    ("sell", "token_transfer_tax", "bounded_estimate", False),
    ("route", "network_gas", "assumed", False),
    ("route", "rebalancing_or_transfer", "not_applicable", False),
    ("route", "mev_buffer", "assumed", False),
)

_HISTORICAL_ATOMIC_KEYS = tuple(
    (leg, component_type)
    for leg, component_type, _value_status, _embedded
    in HISTORICAL_ATOMIC_COMPONENT_MATRIX
)
_HISTORICAL_PROOF_ROLES = (
    "receipt", "receipt", "receipt", "receipt", "receipt", "receipt",
    "receipt", "trace", "policy",
)
_HISTORICAL_ZERO_FEE_KEYS = frozenset({
    ("buy", "router_or_integrator_fee"),
    ("buy", "token_transfer_tax"),
    ("sell", "router_or_integrator_fee"),
    ("sell", "token_transfer_tax"),
})
_HISTORICAL_POOL_FEE_KEYS = frozenset({
    ("buy", "pool_swap_fee"),
    ("sell", "pool_swap_fee"),
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)


def _market_kind(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("route market ID is invalid")
    if value.startswith("cex:"):
        return "cex"
    if value.startswith("dex:"):
        return "dex"
    raise ValueError("route market type is invalid")


def live_complete_cost_component_keys(
    route: Mapping[str, Any],
) -> FrozenSet[Tuple[str, str]]:
    """Return the existing complete live cost inventory for one route."""
    if not isinstance(route, Mapping):
        raise TypeError("route must be a mapping")
    expected = {("route", "rebalancing_or_transfer")}
    has_dex = False
    for leg in ("buy", "sell"):
        market_kind = _market_kind(route[leg + "_market_id"])
        if market_kind == "cex":
            expected.add((leg, "venue_taker_fee"))
        else:
            has_dex = True
            expected.update({
                (leg, "pool_swap_fee"),
                (leg, "network_gas"),
                (leg, "router_or_integrator_fee"),
                (leg, "token_transfer_tax"),
            })
    if has_dex:
        expected.add(("route", "mev_buffer"))
    return frozenset(expected)


def build_terminal_cex_cost_components(
    *,
    cohort_id: str,
    opportunity_id: str,
    route: Mapping[str, Any],
    requested_notional_usd: Any,
    reason_code: str,
) -> List[Dict[str, Any]]:
    """Derive the sole source-less three-row CEX terminal cost inventory."""
    if not isinstance(route, Mapping) or any(
        _market_kind(route.get(leg + "_market_id")) != "cex"
        for leg in ("buy", "sell")
    ):
        raise ValueError("terminal cost route must be CEX to CEX")
    rows: List[Dict[str, Any]] = []
    for leg, component_type in sorted(
        live_complete_cost_component_keys(route)
    ):
        rows.append(cost_component_row(
            cohort_id=cohort_id,
            opportunity_id=opportunity_id,
            leg=leg,
            market_id=("" if leg == "route" else route[leg + "_market_id"]),
            direction=("route" if leg == "route" else leg + "_token"),
            requested_notional_usd=requested_notional_usd,
            target_token_quantity=None,
            component_type=component_type,
            value_status="unavailable",
            amount_usd=None,
            rate_bps=None,
            basis="retained route timing proves route unavailable",
            strict_eligible=False,
            embedded_in_leg_quote=False,
            observed_at=None,
            valid_until=None,
            source="retained route timing",
            source_record_sha256=None,
            reason_code=reason_code,
        ))
    return rows


def validate_terminal_cex_cost_components(
    rows: Iterable[Mapping[str, Any]],
    *,
    cohort_id: str,
    opportunity_id: str,
    route: Mapping[str, Any],
    requested_notional_usd: Any,
    reason_code: str,
) -> List[Dict[str, Any]]:
    """Return canonical terminal rows only after exact byte-level comparison."""
    if isinstance(rows, (str, bytes, Mapping)):
        raise ValueError("terminal CEX cost component inventory is invalid")
    expected = build_terminal_cex_cost_components(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        route=route,
        requested_notional_usd=requested_notional_usd,
        reason_code=reason_code,
    )
    try:
        supplied = sorted(
            (dict(row) for row in rows),
            key=lambda row: (row["leg"], row["component_type"]),
        )
        supplied_bytes = json.dumps(
            supplied,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        expected_bytes = json.dumps(
            expected,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "terminal CEX cost component inventory is invalid"
        ) from error
    if supplied_bytes != expected_bytes:
        raise ValueError("terminal CEX cost component inventory is inconsistent")
    return expected


def _canonical_nonnegative_decimal(value: Any, label: str) -> Tuple[str, Fraction]:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(label + " must be canonical decimal text")
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(label + " must be canonical decimal text") from error
    if not number.is_finite() or number < 0:
        raise ValueError(label + " must be a nonnegative finite decimal")
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if number == 0:
        canonical = "0"
    if value != canonical:
        raise ValueError(label + " must be canonical decimal text")
    return value, Fraction(number)


def _positive_decimal(value: Any, label: str) -> Tuple[str, Fraction]:
    text, number = _canonical_nonnegative_decimal(value, label)
    if number <= 0:
        raise ValueError(label + " must be positive")
    return text, number


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(label + " must be lowercase SHA-256 text")
    return value


def _expected_pair_mapping(
    value: Mapping[str, str], label: str
) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"buy", "sell"}:
        raise ValueError(label + " must contain exact buy and sell keys")
    return value


def _expected_zero_mapping(
    value: Mapping[Tuple[str, str], str],
) -> Mapping[Tuple[str, str], str]:
    if not isinstance(value, Mapping) or set(value) != _HISTORICAL_ZERO_FEE_KEYS:
        raise ValueError("zero-fee proof mapping is not exact")
    return value


def _validate_historical_atomic_cost_component_matrix(
    route: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_cohort_id: str,
    expected_opportunity_id: str,
    expected_pool_fee_source_sha256_by_leg: Mapping[str, str],
    expected_pool_fee_amount_usd_by_leg: Mapping[str, str],
    expected_zero_fee_proof_sha256_by_key: Mapping[Tuple[str, str], str],
    expected_gas_amount_usd: str,
    expected_gas_source_sha256: str,
    expected_transfer_source_sha256: str,
    expected_mev_amount_usd: str,
    expected_policy_sha256: str,
) -> None:
    """Validate the exact context-free nine-row historical atomic matrix.

    Tasks 5-6 authenticate the expected hashes before calling this binder.
    """
    if not isinstance(route, Mapping) or route.get("route_mode") != "atomic_onchain":
        raise ValueError("historical route must be atomic_onchain")
    buy_market_id = route.get("buy_market_id")
    sell_market_id = route.get("sell_market_id")
    if _market_kind(buy_market_id) != "dex" or _market_kind(sell_market_id) != "dex":
        raise ValueError("historical atomic route must contain two DEX legs")
    if isinstance(rows, (str, bytes, Mapping)) or not isinstance(rows, Sequence):
        raise TypeError("historical cost rows must be a sequence")
    if len(rows) != len(HISTORICAL_ATOMIC_COMPONENT_MATRIX):
        raise ValueError("historical atomic cost inventory is not exact")

    pool_sources = _expected_pair_mapping(
        expected_pool_fee_source_sha256_by_leg, "pool-fee source mapping"
    )
    pool_amounts = _expected_pair_mapping(
        expected_pool_fee_amount_usd_by_leg, "pool-fee amount mapping"
    )
    zero_sources = _expected_zero_mapping(
        expected_zero_fee_proof_sha256_by_key
    )
    for leg in ("buy", "sell"):
        _sha256(pool_sources[leg], leg + " pool-fee source")
        _positive_decimal(pool_amounts[leg], leg + " pool-fee amount")
    for key, value in zero_sources.items():
        _sha256(value, "{} {} proof".format(*key))
    _canonical_nonnegative_decimal(expected_gas_amount_usd, "expected gas amount")
    _sha256(expected_gas_source_sha256, "expected gas source")
    _sha256(expected_transfer_source_sha256, "expected transfer source")
    _canonical_nonnegative_decimal(expected_mev_amount_usd, "expected MEV amount")
    _sha256(expected_policy_sha256, "expected policy source")
    if not isinstance(expected_cohort_id, str) or not expected_cohort_id:
        raise ValueError("expected cohort ID is invalid")
    if not isinstance(expected_opportunity_id, str) or not expected_opportunity_id:
        raise ValueError("expected opportunity ID is invalid")

    requested_notional = None
    target_quantity = None
    observed_keys = []
    for row, expected_shape, expected_role in zip(
        rows,
        HISTORICAL_ATOMIC_COMPONENT_MATRIX,
        _HISTORICAL_PROOF_ROLES,
    ):
        if not isinstance(row, Mapping) or set(row) != set(COST_COMPONENT_COLUMNS):
            raise ValueError("historical cost row schema is invalid")
        leg, component_type, value_status, embedded = expected_shape
        if (
            row.get("contract_version") != COST_COMPONENT_CONTRACT_VERSION
            or row.get("leg") != leg
            or row.get("component_type") != component_type
            or row.get("value_status") != value_status
            or row.get("embedded_in_leg_quote") is not embedded
            or row.get("cohort_id") != expected_cohort_id
            or row.get("opportunity_id") != expected_opportunity_id
            or row.get("strict_eligible") is not False
            or row.get("reason_code") is not None
            or row.get("observed_at") is not None
            or row.get("valid_until") is not None
            or row.get("source") != expected_role
        ):
            raise ValueError("historical atomic cost row contract is invalid")
        observed_keys.append((leg, component_type))
        if leg == "buy":
            expected_market_id = buy_market_id
            expected_direction = "buy_token"
        elif leg == "sell":
            expected_market_id = sell_market_id
            expected_direction = "sell_token"
        else:
            expected_market_id = ""
            expected_direction = "route"
        if (
            row.get("market_id") != expected_market_id
            or row.get("direction") != expected_direction
            or not isinstance(row.get("basis"), str)
            or not row["basis"]
        ):
            raise ValueError("historical atomic cost row lineage is invalid")
        row_notional_text, row_notional = _positive_decimal(
            row.get("requested_notional_usd"), "requested notional"
        )
        row_target_text, _row_target = _positive_decimal(
            row.get("target_token_quantity"), "target quantity"
        )
        if requested_notional is None:
            requested_notional = (row_notional_text, row_notional)
            target_quantity = row_target_text
        elif (
            row_notional_text != requested_notional[0]
            or row_target_text != target_quantity
        ):
            raise ValueError("historical cost scenario quantities conflict")

        key = (leg, component_type)
        if key in _HISTORICAL_POOL_FEE_KEYS:
            if (
                row.get("amount_usd") != pool_amounts[leg]
                or row.get("rate_bps") != "30"
                or row.get("source_record_sha256") != pool_sources[leg]
            ):
                raise ValueError("historical pool-fee proof is invalid")
        elif key in _HISTORICAL_ZERO_FEE_KEYS:
            if (
                row.get("amount_usd") != "0"
                or row.get("rate_bps") != "0"
                or row.get("source_record_sha256") != zero_sources[key]
            ):
                raise ValueError("historical zero-fee proof is invalid")
        elif key == ("route", "network_gas"):
            if (
                row.get("amount_usd") != expected_gas_amount_usd
                or row.get("rate_bps") is not None
                or row.get("source_record_sha256") != expected_gas_source_sha256
            ):
                raise ValueError("historical gas proof is invalid")
        elif key == ("route", "rebalancing_or_transfer"):
            if (
                row.get("amount_usd") is not None
                or row.get("rate_bps") is not None
                or row.get("source") != "trace"
                or row.get("source_record_sha256")
                != expected_transfer_source_sha256
            ):
                raise ValueError("historical transfer topology proof is invalid")
        elif key == ("route", "mev_buffer"):
            if (
                row.get("amount_usd") != expected_mev_amount_usd
                or row.get("rate_bps") != "10"
                or row.get("source_record_sha256") != expected_policy_sha256
            ):
                raise ValueError("historical MEV proof is invalid")

    if tuple(observed_keys) != _HISTORICAL_ATOMIC_KEYS:
        raise ValueError("historical atomic cost inventory is not canonical")
    if requested_notional is None:
        raise ValueError("historical atomic cost inventory is empty")
    _expected_mev_text, expected_mev = _canonical_nonnegative_decimal(
        expected_mev_amount_usd, "expected MEV amount"
    )
    if expected_mev != requested_notional[1] * Fraction(10, 10_000):
        raise ValueError("historical MEV amount does not reproduce ten bps")
