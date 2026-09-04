"""Exact route-opportunity math with evidence-aware, fail-closed gates.

The module deliberately consumes actual quote-asset cash flows plus an
independent USD conversion record.  It never assumes that USDT, USDC, or any
other quote asset equals one USD.  Costs already reflected in a leg cash flow
remain completeness evidence but are excluded from additive totals exactly
once.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
import re
import weakref
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from scripts.execution_cost_components import (
        SCENARIO_VALUE_STATUSES,
        STRICT_VALUE_STATUSES,
        TERMINAL_VALUE_STATUSES,
        validate_cost_components,
    )
    from scripts.fetch_cex_depth import route_quantity_quote_for_book
    from scripts.route_cohort import canonical_route_id, classify_route_timing
    from scripts.route_cost_topology import (
        live_complete_cost_component_keys,
        validate_terminal_cex_cost_components,
    )
    from scripts.route_quantity import (
        CommonTarget,
        FeeSemantics,
        MarketRules,
        QuantityQuote,
        V2PoolState,
        common_net_target_quantity,
        validate_quantity_quote,
        validate_v2_quantity_quote_against_state,
    )
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from execution_cost_components import (  # type: ignore
        SCENARIO_VALUE_STATUSES,
        STRICT_VALUE_STATUSES,
        TERMINAL_VALUE_STATUSES,
        validate_cost_components,
    )
    from fetch_cex_depth import route_quantity_quote_for_book  # type: ignore
    from route_cohort import (  # type: ignore
        canonical_route_id,
        classify_route_timing,
    )
    from route_cost_topology import (  # type: ignore
        live_complete_cost_component_keys,
        validate_terminal_cex_cost_components,
    )
    from route_quantity import (  # type: ignore
        CommonTarget,
        FeeSemantics,
        MarketRules,
        QuantityQuote,
        V2PoolState,
        common_net_target_quantity,
        validate_quantity_quote,
        validate_v2_quantity_quote_against_state,
    )
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


ROUTE_OPPORTUNITY_CONTRACT_VERSION = "1"
MAX_ROUTE_SKEW_SECONDS = Fraction(60)
MAX_ROUTE_AGE_SECONDS = Fraction(120)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}\Z", flags=re.ASCII)
_ASSET = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII)

_USD_PROJECTION_FIELDS = frozenset(
    {
        "contract_version",
        "market_id",
        "state_id",
        "direction",
        "quote_asset",
        "quote_cash_quantity",
        "usd_per_quote",
        "usd_amount",
        "value_status",
        "observed_at",
        "valid_until",
        "source",
        "source_record_sha256",
        "core_manifest_sha256",
        "evidence_binding_sha256",
    }
)
_USD_PROJECTION_STATUSES = frozenset(
    {"measured", "authenticated", "quoted", "bounded_estimate", "assumed"}
)
_STRICT_USD_PROJECTION_STATUSES = frozenset(
    {"measured", "authenticated", "quoted"}
)
_ASSURANCE_STATUSES = frozenset(
    {"integrity_only", "route_bundle_validated", "authenticated_fixed_block"}
)
_ROUTE_MODES = frozenset(
    {
        "prepositioned_inventory",
        "atomic_onchain",
        "rebalance_required",
        "research_only",
    }
)
ROUTE_OPPORTUNITY_MODES = _ROUTE_MODES
_MODE_REASON_CODES_BY_MODE = {
    "prepositioned_inventory": frozenset(
        {
            "mode_expected_request_unavailable",
            "inventory_unavailable",
            "inventory_request_mismatch",
            "inventory_insufficient",
            "dex_buy_quantity_quote_unavailable",
            "dex_buy_authoritative_upstream_unavailable",
        }
    ),
    "atomic_onchain": frozenset(
        {
            "mode_expected_request_unavailable",
            "atomic_route_simulation_unavailable",
            "unsupported_cross_chain_settlement",
        }
    ),
    "rebalance_required": frozenset(
        {
            "mode_expected_request_unavailable",
            "inventory_unavailable",
            "inventory_request_mismatch",
            "inventory_insufficient",
            "dex_buy_quantity_quote_unavailable",
            "dex_buy_authoritative_upstream_unavailable",
            "rebalance_transfer_evidence_unavailable",
        }
    ),
    "research_only": frozenset({"unsupported_cross_chain_settlement"}),
}

_PUBLICATION_ATTESTATION_SEAL = object()


class _PublicationAttestation:
    """Sealed capability issued only after final-bundle verification."""

    __slots__ = ("_binding_sha256", "__weakref__")

    def __init__(self, seal: object, binding_sha256: str) -> None:
        if seal is not _PUBLICATION_ATTESTATION_SEAL:
            raise ValueError("publication attestation cannot be constructed directly")
        object.__setattr__(self, "_binding_sha256", binding_sha256)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("publication attestation is immutable")


_ISSUED_PUBLICATION_ATTESTATIONS = weakref.WeakSet()

_PRIMARY_REASON_PRIORITY = (
    "route_deadline_exceeded",
    "execution_adapter_unsupported",
    "buy_leg_unavailable",
    "sell_leg_unavailable",
    "leg_not_completely_filled",
    "invalid_state_timestamp",
    "snapshot_skew_exceeded",
    "common_quantity_unavailable",
    "quantity_quote_evidence_mismatch",
    "usd_conversion_unavailable",
    "cohort_stale",
    "unsupported_cross_chain_settlement",
    "atomic_route_simulation_unavailable",
    "inventory_unavailable",
    "inventory_insufficient",
    "rebalance_transfer_evidence_unavailable",
    "quantity_quote_evidence_not_strict",
    "usd_conversion_not_strict",
    "cost_components_incomplete",
    "cost_component_stale",
    "cost_component_estimated",
    "non_positive_net_edge",
    "publication_evidence_unverified",
)
ROUTE_OPPORTUNITY_REASON_CODES = (
    frozenset(_PRIMARY_REASON_PRIORITY)
    | frozenset().union(*_MODE_REASON_CODES_BY_MODE.values())
    | frozenset({"positive_strict_net_edge"})
)
_REASON_RANK = {
    reason: index for index, reason in enumerate(_PRIMARY_REASON_PRIORITY)
}

OPPORTUNITY_FIELDS = frozenset(
    {
        "contract_version",
        "cohort_id",
        "route_id",
        "opportunity_id",
        "token_symbol",
        "buy_market_id",
        "sell_market_id",
        "route_mode",
        "requested_notional_usd",
        "target_token_quantity",
        "target_base_raw",
        "target_base_unit_decimals",
        "target_lattice_raw",
        "buy_state_id",
        "sell_state_id",
        "buy_state_observed_at",
        "sell_state_observed_at",
        "skew_seconds",
        "route_age_seconds",
        "gross_buy_cost_usd",
        "gross_sell_proceeds_usd",
        "gross_edge_usd",
        "gross_edge_bps",
        "gross_edge_bps_numerator",
        "gross_edge_bps_denominator",
        "strict_nonembedded_cost_usd",
        "research_bounded_cost_usd",
        "research_assumed_cost_usd",
        "strict_net_edge_usd",
        "strict_net_edge_bps",
        "strict_net_edge_bps_numerator",
        "strict_net_edge_bps_denominator",
        "research_net_edge_usd",
        "research_net_edge_bps",
        "research_net_edge_bps_numerator",
        "research_net_edge_bps_denominator",
        "edge_bps_denominator_basis",
        "cost_completeness",
        "scenario_cost_completeness",
        "reflected_or_embedded_component_keys",
        "component_reasons",
        "mode_evidence_eligible",
        "inventory_profile_hash",
        "maximum_proved_capacity_quantity",
        "opportunity_class",
        "primary_reason",
        "reason_codes",
        "strict_eligible",
        "strict_ready_for_publication",
        "publication_attestation_sha256",
        "buy_usd_projection_sha256",
        "sell_usd_projection_sha256",
        "cost_component_set_sha256",
        "mode_evidence_sha256",
        "buy_core_manifest_sha256",
        "sell_core_manifest_sha256",
        "evidence_binding_sha256",
    }
)


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("{} must be canonical text".format(field))
    return value


def _hash(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if _SHA256.fullmatch(text) is None:
        raise ValueError("{} must be lowercase SHA-256 text".format(field))
    return text


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(
        value, (Decimal, int, str)
    ):
        raise ValueError("{} must be an exact Decimal".format(field))
    if isinstance(value, str) and (not value or value != value.strip()):
        raise ValueError("{} must be canonical Decimal text".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("{} must be an exact Decimal".format(field)) from error
    if not number.is_finite() or (positive and number <= 0):
        raise ValueError("{} must be a finite exact Decimal".format(field))
    return number


def _fraction(value: Any, field: str, *, positive: bool = False) -> Fraction:
    number = _decimal(value, field, positive=positive)
    parts = number.as_tuple()
    coefficient = 0
    for digit in parts.digits:
        coefficient = coefficient * 10 + digit
    if parts.sign:
        coefficient = -coefficient
    exponent = int(parts.exponent)
    if exponent >= 0:
        return Fraction(coefficient * (10**exponent), 1)
    return Fraction(coefficient, 10 ** (-exponent))


def _decimal_from_fraction(value: Fraction, field: str) -> Decimal:
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ValueError("{} is not an exact finite Decimal".format(field))
    scale = max(twos, fives)
    coefficient = (
        value.numerator
        * (2 ** (scale - twos))
        * (5 ** (scale - fives))
    )
    if coefficient == 0:
        return Decimal(0)
    sign = 1 if coefficient < 0 else 0
    digits = tuple(int(character) for character in str(abs(coefficient)))
    return Decimal((sign, digits, -scale))


def _decimal_text(value: Any, field: str = "decimal") -> str:
    number = value if isinstance(value, Decimal) else _decimal_from_fraction(value, field)
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _timestamp(value: Any, field: str) -> Tuple[str, Fraction]:
    text = _required_text(value, field)
    try:
        epoch = exact_rfc3339_epoch_seconds(text)
    except (OverflowError, ValueError) as error:
        raise ValueError("{} must be RFC 3339 text".format(field)) from error
    return text, Fraction(epoch)


def classify_terminal_route_timing(
    route: Mapping[str, Any],
    buy_leg: Mapping[str, Any],
    sell_leg: Mapping[str, Any],
    *,
    validated_at: Any,
) -> Dict[str, Any]:
    """Replay terminal timing against the immutable cohort evaluation time."""
    validated_text, _epoch = _timestamp(
        validated_at,
        "terminal timing validated_at",
    )
    return classify_route_timing(
        {
            **dict(route),
            "validated_at": validated_text,
            "skew_sla_seconds": _decimal_text(MAX_ROUTE_SKEW_SECONDS),
        },
        buy_leg,
        sell_leg,
    )


def _canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _publication_binding_sha256(
    *,
    cohort_id: Any,
    opportunity_id: Any,
    route_id: Any,
    target_token_quantity: Any,
    buy_state_id: Any,
    sell_state_id: Any,
    buy_usd_projection_sha256: Any,
    sell_usd_projection_sha256: Any,
    cost_component_set_sha256: Any,
    mode_evidence_sha256: Any,
    core_manifest_sha256: Any,
) -> str:
    cohort = _required_text(cohort_id, "cohort_id")
    if _COHORT_ID.fullmatch(cohort) is None:
        raise ValueError("cohort_id must be canonical")
    binding = {
        "cohort_id": cohort,
        "opportunity_id": _required_text(opportunity_id, "opportunity_id"),
        "route_id": _required_text(route_id, "route_id"),
        "target_token_quantity": _decimal_text(
            _decimal(target_token_quantity, "target_token_quantity", positive=True)
        ),
        "buy_state_id": _required_text(buy_state_id, "buy_state_id"),
        "sell_state_id": _required_text(sell_state_id, "sell_state_id"),
        "buy_usd_projection_sha256": _hash(
            buy_usd_projection_sha256, "buy_usd_projection_sha256"
        ),
        "sell_usd_projection_sha256": _hash(
            sell_usd_projection_sha256, "sell_usd_projection_sha256"
        ),
        "cost_component_set_sha256": _hash(
            cost_component_set_sha256, "cost_component_set_sha256"
        ),
        "mode_evidence_sha256": _hash(
            mode_evidence_sha256, "mode_evidence_sha256"
        ),
        "core_manifest_sha256": _hash(
            core_manifest_sha256, "core_manifest_sha256"
        ),
    }
    return _canonical_json_sha256(binding)


def _issue_publication_attestation(**binding: Any) -> _PublicationAttestation:
    """Issue the private capability Task 7 may call after bundle replay."""
    attestation = _PublicationAttestation(
        _PUBLICATION_ATTESTATION_SEAL,
        _publication_binding_sha256(**binding),
    )
    _ISSUED_PUBLICATION_ATTESTATIONS.add(attestation)
    return attestation


def _validated_publication_attestation(
    attestation: Any,
    **binding: Any,
) -> Optional[str]:
    if attestation is None:
        return None
    if not isinstance(attestation, _PublicationAttestation):
        raise ValueError("publication attestation must be the sealed internal type")
    try:
        provided = _hash(
            attestation._binding_sha256,
            "publication_attestation_sha256",
        )
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("publication attestation is malformed") from error
    try:
        expected = _publication_binding_sha256(**binding)
    except (TypeError, ValueError) as error:
        raise ValueError("publication attestation cannot bind incomplete evidence") from error
    if provided != expected:
        raise ValueError("publication attestation binding mismatch")
    if attestation not in _ISSUED_PUBLICATION_ATTESTATIONS:
        raise ValueError("publication attestation was not issued by private issuer")
    return provided


def _rounded_ratio_text(value: Fraction, places: int = 8) -> str:
    """Round one rational half-even without ambient Decimal context."""
    if places < 0:
        raise ValueError("places must be non-negative")
    sign = -1 if value < 0 else 1
    absolute = abs(value)
    scale = 10**places
    quotient, remainder = divmod(absolute.numerator * scale, absolute.denominator)
    doubled = remainder * 2
    if doubled > absolute.denominator or (
        doubled == absolute.denominator and quotient % 2
    ):
        quotient += 1
    quotient *= sign
    if quotient == 0:
        return "0"
    negative = quotient < 0
    digits = str(abs(quotient))
    if places:
        digits = digits.rjust(places + 1, "0")
        text = digits[:-places] + "." + digits[-places:]
        text = text.rstrip("0").rstrip(".")
    else:
        text = digits
    return ("-" if negative else "") + text


def _reason_order(reasons: Iterable[str]) -> List[str]:
    unique = set(reasons)
    return sorted(unique, key=lambda item: (_REASON_RANK.get(item, 10_000), item))


def common_target_quantity(
    *,
    requested_notional_usd: Any,
    buy_reference_price_usd: Any,
    sell_reference_price_usd: Any,
    buy_market_rules: MarketRules,
    sell_market_rules: MarketRules,
) -> CommonTarget:
    """Return a common quantity whose reference exposure is bounded on both legs."""
    buy_price = _decimal(
        buy_reference_price_usd,
        "buy_reference_price_usd",
        positive=True,
    )
    sell_price = _decimal(
        sell_reference_price_usd,
        "sell_reference_price_usd",
        positive=True,
    )
    return common_net_target_quantity(
        requested_notional_usd=requested_notional_usd,
        buy_reference_price_usd=max(buy_price, sell_price),
        buy_market_rules=buy_market_rules,
        sell_market_rules=sell_market_rules,
    )


def route_opportunity_id(route_id: Any, requested_notional_usd: Any) -> str:
    route = _required_text(route_id, "route_id")
    notional = _decimal(requested_notional_usd, "requested_notional_usd", positive=True)
    return "{}:{}".format(route, _decimal_text(notional))


def usd_projection_evidence(
    *,
    market_id: Any,
    state_id: Any,
    direction: Any,
    quote_asset: Any,
    quote_cash_quantity: Any,
    usd_per_quote: Any,
    value_status: Any,
    observed_at: Any,
    valid_until: Any,
    source: Any,
    source_record_sha256: Any,
    core_manifest_sha256: Any,
) -> Dict[str, Any]:
    """Build one exact quote-cashflow-to-USD evidence record."""
    market = _required_text(market_id, "market_id")
    state = _required_text(state_id, "state_id")
    direction_text = _required_text(direction, "direction")
    if direction_text not in {"buy", "sell"}:
        raise ValueError("direction must be buy or sell")
    asset = _required_text(quote_asset, "quote_asset")
    if _ASSET.fullmatch(asset) is None:
        raise ValueError("quote_asset must be canonical")
    cash = _fraction(quote_cash_quantity, "quote_cash_quantity", positive=True)
    rate = _fraction(usd_per_quote, "usd_per_quote", positive=True)
    amount = cash * rate
    status = _required_text(value_status, "value_status")
    if status not in _USD_PROJECTION_STATUSES:
        raise ValueError("USD projection value_status is unsupported")
    observed, observed_epoch = _timestamp(observed_at, "observed_at")
    valid, valid_epoch = _timestamp(valid_until, "valid_until")
    if valid_epoch <= observed_epoch:
        raise ValueError("USD projection valid_until must follow observed_at")
    source_text = _required_text(source, "source")
    source_hash = None
    if source_record_sha256 not in (None, ""):
        source_hash = _hash(source_record_sha256, "source_record_sha256")
    if status in _STRICT_USD_PROJECTION_STATUSES and source_hash is None:
        raise ValueError("strict USD projection requires source_record_sha256")
    core_hash = None
    if core_manifest_sha256 not in (None, ""):
        core_hash = _hash(core_manifest_sha256, "core_manifest_sha256")
    if status in _STRICT_USD_PROJECTION_STATUSES and core_hash is None:
        raise ValueError("strict USD projection requires core manifest lineage")
    row: Dict[str, Any] = {
        "contract_version": "1",
        "market_id": market,
        "state_id": state,
        "direction": direction_text,
        "quote_asset": asset,
        "quote_cash_quantity": _decimal_text(cash, "quote_cash_quantity"),
        "usd_per_quote": _decimal_text(rate, "usd_per_quote"),
        "usd_amount": _decimal_text(amount, "usd_amount"),
        "value_status": status,
        "observed_at": observed,
        "valid_until": valid,
        "source": source_text,
        "source_record_sha256": source_hash,
        "core_manifest_sha256": core_hash,
    }
    row["evidence_binding_sha256"] = _canonical_json_sha256(row)
    return row


def _validated_usd_projection(
    projection: Any,
    *,
    quote: QuantityQuote,
    direction: str,
    now_epoch: Fraction,
) -> Tuple[
    Optional[Fraction],
    bool,
    Optional[str],
    Optional[str],
    Optional[str],
]:
    if projection is None:
        return None, False, "usd_conversion_unavailable", None, None
    if not isinstance(projection, Mapping) or set(projection) != _USD_PROJECTION_FIELDS:
        raise ValueError("USD projection schema is invalid")
    row = dict(projection)
    binding = _hash(row.pop("evidence_binding_sha256"), "evidence_binding_sha256")
    if binding != _canonical_json_sha256(row):
        raise ValueError("USD projection evidence binding mismatch")
    if row.get("contract_version") != "1":
        raise ValueError("USD projection contract_version is invalid")
    if row.get("market_id") != quote.market_id or row.get("state_id") != quote.state_id:
        raise ValueError("USD projection quote lineage mismatch")
    if row.get("direction") != direction:
        raise ValueError("USD projection direction mismatch")
    expected_asset = (
        quote.quote_debit_asset if direction == "buy" else quote.quote_received_asset
    )
    expected_quantity = (
        quote.quote_debit_quantity
        if direction == "buy"
        else quote.quote_received_quantity
    )
    if expected_asset is None or expected_quantity is None:
        raise ValueError("USD projection quote cash flow is unavailable")
    if row.get("quote_asset") != expected_asset:
        raise ValueError("USD projection quote asset mismatch")
    cash = _fraction(row.get("quote_cash_quantity"), "quote_cash_quantity", positive=True)
    if cash != _fraction(expected_quantity, "quote cash quantity", positive=True):
        raise ValueError("USD projection quote quantity mismatch")
    rate = _fraction(row.get("usd_per_quote"), "usd_per_quote", positive=True)
    amount = _fraction(row.get("usd_amount"), "usd_amount", positive=True)
    if amount != cash * rate:
        raise ValueError("USD projection amount does not recompute")
    status = _required_text(row.get("value_status"), "value_status")
    if status not in _USD_PROJECTION_STATUSES:
        raise ValueError("USD projection value_status is unsupported")
    _observed, observed_epoch = _timestamp(row.get("observed_at"), "observed_at")
    _valid, valid_epoch = _timestamp(row.get("valid_until"), "valid_until")
    if valid_epoch <= observed_epoch:
        raise ValueError("USD projection validity window is invalid")
    if observed_epoch > now_epoch or now_epoch >= valid_epoch:
        return None, False, "usd_conversion_unavailable", binding, None
    source_hash = row.get("source_record_sha256")
    if source_hash is not None:
        _hash(source_hash, "source_record_sha256")
    core_hash = row.get("core_manifest_sha256")
    if core_hash is not None:
        core_hash = _hash(core_hash, "core_manifest_sha256")
    strict = (
        status in _STRICT_USD_PROJECTION_STATUSES
        and source_hash is not None
        and core_hash is not None
    )
    return amount, strict, None, binding, core_hash


def _validated_route(route: Any) -> Dict[str, Any]:
    if not isinstance(route, Mapping):
        raise ValueError("route must be a mapping")
    required = {
        "token_symbol",
        "buy_market_id",
        "sell_market_id",
        "route_mode",
        "route_id",
        "route_class",
        "settlement_reason",
    }
    if not required.issubset(route):
        raise ValueError("route identity is incomplete")
    identity = {
        "token_symbol": _required_text(route.get("token_symbol"), "token_symbol"),
        "buy_market_id": _required_text(route.get("buy_market_id"), "buy_market_id"),
        "sell_market_id": _required_text(route.get("sell_market_id"), "sell_market_id"),
        "route_mode": _required_text(route.get("route_mode"), "route_mode"),
    }
    if identity["route_mode"] not in _ROUTE_MODES:
        raise ValueError("route_mode is unsupported")
    expected_id = canonical_route_id(identity)
    if route.get("route_id") != expected_id:
        raise ValueError("route_id is not canonical")
    route_class = route.get("route_class")
    settlement_reason = route.get("settlement_reason")
    if identity["route_mode"] == "research_only":
        if (
            route_class != "research_only"
            or settlement_reason != "unsupported_cross_chain_settlement"
        ):
            raise ValueError("research-only route lineage is invalid")
    elif route_class != "candidate" or settlement_reason not in (None, ""):
        raise ValueError("candidate route lineage is invalid")
    return {**identity, "route_id": expected_id, "route_class": route_class}


def _market_type_and_chain(market_id: str) -> Tuple[str, Optional[str]]:
    if market_id.startswith("cex:"):
        return "cex", None
    if market_id.startswith("dex:"):
        parts = market_id.split(":", 3)
        if len(parts) != 4:
            raise ValueError("DEX market_id is invalid")
        return "dex", parts[1]
    raise ValueError("market_id is invalid")


def _validate_route_topology(route: Mapping[str, Any]) -> None:
    buy_type, buy_chain = _market_type_and_chain(route["buy_market_id"])
    sell_type, sell_chain = _market_type_and_chain(route["sell_market_id"])
    mode = route["route_mode"]
    if (
        buy_type == "dex"
        and sell_type == "dex"
        and buy_chain != sell_chain
        and mode != "research_only"
    ):
        raise ValueError("cross-chain DEX route must be research_only")
    if mode == "prepositioned_inventory":
        if buy_type != "cex" and sell_type != "cex":
            raise ValueError("prepositioned route requires a CEX leg")
    elif mode == "atomic_onchain":
        if (
            buy_type != "dex"
            or sell_type != "dex"
            or buy_chain != sell_chain
        ):
            raise ValueError("atomic route must use same-chain DEX legs")
    elif mode == "research_only":
        if (
            buy_type != "dex"
            or sell_type != "dex"
            or buy_chain == sell_chain
        ):
            raise ValueError("research_only route must be cross-chain DEX")


def _leg_reason(
    leg: Any,
    *,
    quote: QuantityQuote,
    expected_market_id: str,
    side: str,
) -> Optional[str]:
    if not isinstance(leg, Mapping):
        return side + "_leg_unavailable"
    if leg.get("market_id") != expected_market_id:
        return "quantity_quote_evidence_mismatch"
    status = str(leg.get("status") or "")
    reason = str(leg.get("reason_code") or "")
    if status == "deadline_exceeded" or reason == "route_deadline_exceeded":
        return "route_deadline_exceeded"
    if status == "unsupported" or reason == "unsupported_source":
        return "execution_adapter_unsupported"
    if status == "partial":
        return "leg_not_completely_filled"
    if status != "observed" or leg.get("available") is not True:
        return side + "_leg_unavailable"
    if (
        leg.get("state_observed_at") != quote.state_observed_at
        or leg.get("raw_response_sha256") != quote.raw_response_sha256
        or leg.get("state_id") != quote.state_id
        or leg.get("snapshot_id") != quote.snapshot_id
    ):
        return "quantity_quote_evidence_mismatch"
    return None


def _quote_matches_common_target(quote: QuantityQuote, target: CommonTarget) -> bool:
    return (
        quote.target_base_asset == target.asset
        and quote.target_base_raw == target.raw_quantity
        and quote.target_base_unit_decimals == target.unit_decimals
        and quote.target_lattice_raw == target.lattice_raw
        and quote.target_base_quantity == target.quantity
    )


def _validated_quote_evidence(
    quote: QuantityQuote,
    evidence: Any,
    *,
    target: CommonTarget,
    direction: str,
) -> Tuple[
    bool,
    bool,
    Optional[FeeSemantics],
    Optional[Tuple[str, Fraction]],
    Optional[str],
]:
    """Return replay validity, assurance, and adapter-owned fee lineage."""
    if not isinstance(evidence, Mapping):
        return False, False, None, None, None
    assurance = evidence.get("assurance_status")
    if assurance not in _ASSURANCE_STATUSES:
        return False, False, None, None, None
    core_hash = evidence.get("core_manifest_sha256")
    if assurance != "integrity_only":
        try:
            core_hash = _hash(core_hash, "core_manifest_sha256")
        except ValueError:
            return False, False, None, None, None
    elif core_hash not in (None, ""):
        try:
            core_hash = _hash(core_hash, "core_manifest_sha256")
        except ValueError:
            return False, False, None, None, None
    kind = evidence.get("kind")
    try:
        if kind == "cex_book":
            rules = evidence.get("market_rules")
            fee = evidence.get("fee_semantics")
            if not isinstance(rules, MarketRules) or not isinstance(fee, FeeSemantics):
                return False, False, None, None, core_hash
            expected = route_quantity_quote_for_book(
                evidence.get("market"),
                evidence.get("book"),
                direction=direction,
                target_token_quantity=target,
                market_rules=rules,
                fee_semantics=fee,
                snapshot_id=evidence.get("snapshot_id"),
                observed_at=evidence.get("observed_at"),
                cohort_now=evidence.get("cohort_now"),
                expected_state_id=evidence.get("expected_state_id"),
            )
            if expected != quote:
                return False, False, fee, None, core_hash
            return (
                True,
                assurance == "route_bundle_validated",
                fee,
                None,
                core_hash,
            )
        if kind == "dex_v2":
            state = evidence.get("pool_state")
            rules = evidence.get("market_rules")
            if not isinstance(state, V2PoolState) or not isinstance(rules, MarketRules):
                return False, False, None, None, core_hash
            pool_fee = (state.fee_proof_sha256, Fraction(state.fee_bps))
            try:
                validate_v2_quantity_quote_against_state(
                    quote,
                    state,
                    target,
                    rules,
                    direction=direction,
                    target_token_address=evidence.get("target_token_address"),
                    quote_token_address=evidence.get("quote_token_address"),
                    cohort_now=evidence.get("cohort_now"),
                    snapshot_id=evidence.get("snapshot_id"),
                )
            except (TypeError, ValueError):
                return False, False, None, pool_fee, core_hash
            return (
                True,
                assurance == "authenticated_fixed_block",
                None,
                pool_fee,
                core_hash,
            )
    except (AttributeError, TypeError, ValueError):
        return False, False, None, None, core_hash
    return False, False, None, None, core_hash


def _component_current(row: Mapping[str, Any], now_epoch: Fraction) -> bool:
    if (
        row.get("value_status") == "not_applicable"
        and row.get("observed_at") in (None, "")
        and row.get("valid_until") in (None, "")
    ):
        return True
    observed = row.get("observed_at")
    valid = row.get("valid_until")
    if observed in (None, ""):
        return row.get("value_status") in SCENARIO_VALUE_STATUSES
    try:
        _observed, observed_epoch = _timestamp(observed, "component observed_at")
    except ValueError:
        return False
    if observed_epoch > now_epoch:
        return False
    if valid not in (None, ""):
        try:
            _valid, valid_epoch = _timestamp(valid, "component valid_until")
        except ValueError:
            return False
        return now_epoch < valid_epoch
    return now_epoch - observed_epoch <= MAX_ROUTE_AGE_SECONDS


def _not_applicable_is_proved(
    row: Mapping[str, Any], route: Mapping[str, Any]
) -> bool:
    component_type = row["component_type"]
    if component_type == "rebalancing_or_transfer":
        return (
            route["route_mode"] in {"prepositioned_inventory", "atomic_onchain"}
            and row["leg"] == "route"
            and row["source"] == "validated route topology"
        )
    if component_type in {"router_or_integrator_fee", "token_transfer_tax"}:
        return (
            row["leg"] in {"buy", "sell"}
            and row["source"] == "validated route adapter contract"
            and row.get("source_record_sha256") is not None
        )
    return False


def _fee_is_reflected_in_cashflow(
    *, direction: str, quote: QuantityQuote, fee: Optional[FeeSemantics]
) -> bool:
    if fee is None:
        return False
    if direction == "buy":
        return fee.fee_asset in {
            quote.target_base_asset,
            quote.quote_debit_asset,
        }
    return fee.fee_asset == quote.quote_received_asset


def _analyze_cost_components(
    rows: Iterable[Mapping[str, Any]],
    *,
    cohort_id: str,
    opportunity_id: str,
    route: Mapping[str, Any],
    target: CommonTarget,
    requested_notional: Decimal,
    buy_quote: QuantityQuote,
    sell_quote: QuantityQuote,
    buy_fee: Optional[FeeSemantics],
    sell_fee: Optional[FeeSemantics],
    buy_pool_fee: Optional[Tuple[str, Fraction]],
    sell_pool_fee: Optional[Tuple[str, Fraction]],
    now_epoch: Fraction,
) -> Dict[str, Any]:
    inventory = list(rows)
    validate_cost_components(inventory)
    route_has_dex = any(
        _market_type_and_chain(market_id)[0] == "dex"
        for market_id in (route["buy_market_id"], route["sell_market_id"])
    )
    expected_keys = set(live_complete_cost_component_keys(route))
    expected_keys.discard(("route", "mev_buffer"))
    rows_by_key: Dict[Tuple[str, str], Mapping[str, Any]] = {}
    component_reasons: List[str] = []
    for row in inventory:
        if row["cohort_id"] != cohort_id or row["opportunity_id"] != opportunity_id:
            raise ValueError("cost component route lineage mismatch")
        if _fraction(row["requested_notional_usd"], "component notional", positive=True) != _fraction(
            requested_notional, "requested_notional_usd", positive=True
        ):
            raise ValueError("cost component notional mismatch")
        if _fraction(row["target_token_quantity"], "component target", positive=True) != _fraction(
            target.quantity, "target quantity", positive=True
        ):
            raise ValueError("cost component target quantity mismatch")
        leg = str(row["leg"])
        component_type = str(row["component_type"])
        key = (leg, component_type)
        if key in rows_by_key:
            raise ValueError("duplicate cost component topology key")
        if component_type != "mev_buffer" and key not in expected_keys:
            raise ValueError("cost component is incompatible with route topology")
        if component_type == "mev_buffer" and leg != "route":
            raise ValueError("MEV policy must be route-level")
        if leg == "buy":
            expected_market = route["buy_market_id"]
            expected_direction = "buy_token"
        elif leg == "sell":
            expected_market = route["sell_market_id"]
            expected_direction = "sell_token"
        else:
            expected_market = ""
            expected_direction = "route"
        if row["market_id"] != expected_market or row["direction"] != expected_direction:
            raise ValueError("cost component leg identity mismatch")
        rows_by_key[key] = row

    missing = sorted(expected_keys - set(rows_by_key))
    strict_missing: List[str] = []
    scenario_missing: List[str] = []
    strict_total = Fraction(0)
    bounded_total = Fraction(0)
    assumed_total = Fraction(0)
    reflected: List[str] = []
    estimated_required = False
    optional_scenario_complete = True
    mev_rows = [
        row for row in rows_by_key.values() if row["component_type"] == "mev_buffer"
    ]
    if route_has_dex:
        strict_missing.append("route:mev_protection_policy")
        if not mev_rows:
            optional_scenario_complete = False
            component_reasons.append("mev_protection_unavailable:route")

    for key in sorted(expected_keys):
        key_text = "{}:{}".format(*key)
        row = rows_by_key.get(key)
        if row is None:
            strict_missing.append(key_text)
            scenario_missing.append(key_text)
            continue
        status = str(row["value_status"])
        is_reflected = bool(row["embedded_in_leg_quote"])
        if row["component_type"] == "venue_taker_fee" and status in (
            STRICT_VALUE_STATUSES | SCENARIO_VALUE_STATUSES
        ):
            quote = buy_quote if row["leg"] == "buy" else sell_quote
            fee = buy_fee if row["leg"] == "buy" else sell_fee
            if fee is None:
                raise ValueError("venue fee quantity-quote lineage is unavailable")
            if row.get("source_record_sha256") != fee.source_record_sha256:
                raise ValueError("venue fee source does not match quantity quote")
            if _fraction(row["rate_bps"], "component rate") != _fraction(
                fee.rate_bps, "fee rate"
            ):
                raise ValueError("venue fee rate does not match quantity quote")
            is_reflected = _fee_is_reflected_in_cashflow(
                direction=row["leg"], quote=quote, fee=fee
            )
        elif row["component_type"] == "pool_swap_fee" and status in (
            STRICT_VALUE_STATUSES | SCENARIO_VALUE_STATUSES
        ):
            pool_fee = buy_pool_fee if row["leg"] == "buy" else sell_pool_fee
            if pool_fee is None:
                raise ValueError("pool fee quantity-quote lineage is unavailable")
            source_hash, rate_bps = pool_fee
            if row.get("source_record_sha256") != source_hash:
                raise ValueError("pool fee source does not match quantity quote")
            if _fraction(row["rate_bps"], "component rate") != rate_bps:
                raise ValueError("pool fee rate does not match quantity quote")
            is_reflected = True
        if status in TERMINAL_VALUE_STATUSES:
            strict_missing.append(key_text)
            scenario_missing.append(key_text)
            if status == "stale":
                component_reasons.append("cost_component_stale:" + key_text)
            continue
        current = _component_current(row, now_epoch)
        if not current:
            strict_missing.append(key_text)
            if is_reflected:
                reflected.append(key_text)
            else:
                scenario_missing.append(key_text)
            component_reasons.append("cost_component_stale:" + key_text)
            continue
        not_applicable = status == "not_applicable"
        if not_applicable and not _not_applicable_is_proved(row, route):
            strict_missing.append(key_text)
            scenario_missing.append(key_text)
            component_reasons.append("cost_not_applicable_unproved:" + key_text)
            continue
        if (
            row["component_type"] == "venue_taker_fee"
            and status in STRICT_VALUE_STATUSES
            and not is_reflected
        ):
            strict_missing.append(key_text)
            scenario_missing.append(key_text)
            component_reasons.append("fee_debit_conversion_unproved:" + key_text)
            continue
        strict_complete = (
            bool(row["strict_eligible"])
            and status in STRICT_VALUE_STATUSES
            and (not not_applicable or _not_applicable_is_proved(row, route))
        )
        scenario_complete = strict_complete or status in SCENARIO_VALUE_STATUSES
        if not strict_complete:
            strict_missing.append(key_text)
        if not scenario_complete:
            scenario_missing.append(key_text)
        if status in SCENARIO_VALUE_STATUSES:
            estimated_required = True
        if not scenario_complete or row["amount_usd"] is None:
            continue
        amount = _fraction(row["amount_usd"], "component amount")
        if is_reflected:
            reflected.append(key_text)
            continue
        if strict_complete:
            strict_total += amount
        elif status == "bounded_estimate":
            bounded_total += amount
        elif status == "assumed":
            assumed_total += amount

    for key, row in rows_by_key.items():
        if row["component_type"] != "mev_buffer":
            continue
        if row["value_status"] in TERMINAL_VALUE_STATUSES:
            optional_scenario_complete = False
            component_reasons.append(
                "mev_scenario_unavailable:{}:{}:{}".format(
                    *key,
                    row.get("reason_code") or row["value_status"],
                )
            )
            continue
        if not _component_current(row, now_epoch):
            component_reasons.append("cost_component_stale:{}:{}".format(*key))
            optional_scenario_complete = False
            continue
        if row["amount_usd"] is not None and row["value_status"] == "assumed":
            assumed_total += _fraction(row["amount_usd"], "MEV amount")
            if route_has_dex:
                estimated_required = True
        elif row["amount_usd"] is not None and row["value_status"] == "bounded_estimate":
            bounded_total += _fraction(row["amount_usd"], "MEV amount")
            if route_has_dex:
                estimated_required = True
        else:
            optional_scenario_complete = False
            component_reasons.append("mev_scenario_unavailable:{}:{}".format(*key))

    canonical_rows = sorted(
        (dict(row) for row in inventory),
        key=lambda row: (row["leg"], row["component_type"]),
    )
    return {
        "strict_complete": not strict_missing,
        "required_scenario_complete": not scenario_missing,
        "scenario_complete": not scenario_missing and optional_scenario_complete,
        "strict_total": strict_total,
        "bounded_total": bounded_total,
        "assumed_total": assumed_total,
        "strict_missing": strict_missing,
        "scenario_missing": scenario_missing,
        "component_reasons": sorted(component_reasons),
        "reflected": sorted(reflected),
        "estimated_required": estimated_required,
        "set_sha256": _canonical_json_sha256(canonical_rows),
    }


def _validated_mode_evidence(
    route: Mapping[str, Any], evidence: Any, target: CommonTarget
) -> Tuple[bool, List[str], Optional[str], Optional[str], Optional[str]]:
    if route["route_mode"] == "research_only":
        if isinstance(evidence, Mapping):
            raw_reasons = evidence.get("reason_codes")
            if (
                evidence.get("route_id") != route["route_id"]
                or evidence.get("route_mode") != "research_only"
                or evidence.get("classification") != "research_estimate"
                or evidence.get("mode_evidence_eligible") is not False
                or raw_reasons != ["unsupported_cross_chain_settlement"]
                or evidence.get("reason_code")
                != "unsupported_cross_chain_settlement"
            ):
                raise ValueError(
                    "mode evidence reason is unknown or inconsistent with mode"
                )
        return (
            False,
            ["unsupported_cross_chain_settlement"],
            None,
            None,
            None,
        )
    if not isinstance(evidence, Mapping):
        reason = (
            "atomic_route_simulation_unavailable"
            if route["route_mode"] == "atomic_onchain"
            else "inventory_unavailable"
        )
        return False, [reason], None, None, None
    if (
        evidence.get("route_id") != route["route_id"]
        or evidence.get("route_mode") != route["route_mode"]
        or type(evidence.get("mode_evidence_eligible")) is not bool
    ):
        raise ValueError("mode evidence route identity mismatch")
    raw_reasons = evidence.get("reason_codes")
    if not isinstance(raw_reasons, list) or any(
        not isinstance(item, str) or not item for item in raw_reasons
    ):
        raise ValueError("mode evidence reason_codes are invalid")
    allowed_reasons = _MODE_REASON_CODES_BY_MODE[route["route_mode"]]
    if (
        len(set(raw_reasons)) != len(raw_reasons)
        or any(reason not in allowed_reasons for reason in raw_reasons)
    ):
        raise ValueError("mode evidence reason is unknown or inconsistent with mode")
    eligible = evidence["mode_evidence_eligible"]
    expected_classification = (
        "mode_evidence_eligible" if eligible else "research_estimate"
    )
    if evidence.get("classification") != expected_classification:
        raise ValueError("mode evidence classification is inconsistent")
    if eligible and (evidence.get("reason_code") is not None or raw_reasons):
        raise ValueError("eligible mode evidence cannot contain failure reasons")
    if not eligible and (
        not raw_reasons or evidence.get("reason_code") != raw_reasons[0]
    ):
        raise ValueError("ineligible mode evidence reason projection is inconsistent")
    reasons = [] if eligible else list(raw_reasons)
    if not eligible and not reasons:
        reason = evidence.get("reason_code")
        reasons = [str(reason or "inventory_unavailable")]
    profile_hash = evidence.get("inventory_profile_hash")
    if profile_hash not in (None, ""):
        profile_hash = _hash(profile_hash, "inventory_profile_hash")
    if route["route_mode"] == "prepositioned_inventory" and eligible and profile_hash is None:
        return False, ["inventory_unavailable"], None, None, None
    capacity = evidence.get("maximum_proved_capacity_quantity")
    if capacity not in (None, ""):
        capacity = _decimal_text(
            _decimal(capacity, "maximum_proved_capacity_quantity", positive=True)
        )
    if eligible and route["route_mode"] in {
        "prepositioned_inventory",
        "rebalance_required",
    }:
        if profile_hash is None or capacity is None:
            eligible = False
            reasons = ["inventory_unavailable"]
        elif _fraction(capacity, "maximum proved capacity") < _fraction(
            target.quantity, "target quantity", positive=True
        ):
            eligible = False
            reasons = ["inventory_insufficient"]
    elif eligible and route["route_mode"] == "atomic_onchain" and capacity is None:
        eligible = False
        reasons = ["atomic_route_simulation_unavailable"]
    safe_projection = {
        "route_id": evidence.get("route_id"),
        "route_mode": evidence.get("route_mode"),
        "classification": evidence.get("classification"),
        "mode_evidence_eligible": eligible,
        "reason_code": evidence.get("reason_code"),
        "reason_codes": list(raw_reasons),
        "inventory_profile_hash": profile_hash,
        "maximum_proved_capacity_quantity": capacity,
    }
    return eligible, reasons, profile_hash, capacity, _canonical_json_sha256(safe_projection)


def _ratio_fields(edge: Optional[Fraction], buy_cost: Optional[Fraction]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if edge is None or buy_cost is None or buy_cost <= 0:
        return None, None, None
    ratio = edge * 10_000 / buy_cost
    return (
        _rounded_ratio_text(ratio),
        str(ratio.numerator),
        str(ratio.denominator),
    )


def _validated_terminal_mode_evidence(
    route: Mapping[str, Any], evidence: Any
) -> str:
    fields = {
        "route_id",
        "route_mode",
        "classification",
        "mode_evidence_eligible",
        "reason_code",
        "reason_codes",
        "inventory_profile_hash",
        "maximum_proved_capacity_quantity",
    }
    if not isinstance(evidence, Mapping) or set(evidence) != fields:
        raise ValueError("terminal mode evidence schema is invalid")
    reasons = evidence.get("reason_codes")
    allowed_reasons = _MODE_REASON_CODES_BY_MODE[route["route_mode"]]
    if (
        evidence.get("route_id") != route["route_id"]
        or evidence.get("route_mode") != route["route_mode"]
        or evidence.get("classification") != "research_estimate"
        or evidence.get("mode_evidence_eligible") is not False
        or not isinstance(reasons, list)
        or not reasons
        or len(reasons) != len(set(reasons))
        or any(reason not in allowed_reasons for reason in reasons)
        or evidence.get("reason_code") != reasons[0]
        or evidence.get("inventory_profile_hash") is not None
        or evidence.get("maximum_proved_capacity_quantity") is not None
    ):
        raise ValueError("terminal mode evidence is inconsistent")
    return _canonical_json_sha256(dict(evidence))


def _terminal_cost_component_hash(
    rows: Iterable[Mapping[str, Any]],
    *,
    cohort_id: str,
    opportunity_id: str,
    route: Mapping[str, Any],
    requested_notional: Decimal,
    reason_code: str,
) -> str:
    canonical_rows = validate_terminal_cex_cost_components(
        rows,
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        route=route,
        requested_notional_usd=_decimal_text(requested_notional),
        reason_code=reason_code,
    )
    return _canonical_json_sha256(canonical_rows)


def build_terminal_route_opportunity(
    *,
    cohort_id: Any,
    route: Mapping[str, Any],
    requested_notional_usd: Any,
    buy_leg: Mapping[str, Any],
    sell_leg: Mapping[str, Any],
    route_timing: Mapping[str, Any],
    cost_components: Iterable[Mapping[str, Any]],
    mode_evidence: Any,
    now: Any,
    core_manifest_sha256: Any,
) -> Dict[str, Any]:
    """Build one source-less opportunity only from retained terminal lineage."""
    cohort = _required_text(cohort_id, "cohort_id")
    if _COHORT_ID.fullmatch(cohort) is None:
        raise ValueError("cohort_id must be canonical")
    normalized_route = _validated_route(route)
    _validate_route_topology(normalized_route)
    if not all(
        normalized_route[field].startswith("cex:")
        for field in ("buy_market_id", "sell_market_id")
    ):
        raise ValueError("terminal route must use the exact CEX topology")
    for direction, leg in (("buy", buy_leg), ("sell", sell_leg)):
        if (
            not isinstance(leg, Mapping)
            or leg.get("market_id")
            != normalized_route[direction + "_market_id"]
        ):
            raise ValueError("terminal route leg identity is invalid")
    if (
        not isinstance(route_timing, Mapping)
        or set(route_timing)
        != {"route_id", "skew_seconds", "timing_status", "reason_code"}
    ):
        raise ValueError("terminal route timing schema is invalid")
    now_text, _now_epoch = _timestamp(now, "now")
    expected_timing = classify_terminal_route_timing(
        normalized_route,
        buy_leg,
        sell_leg,
        validated_at=now_text,
    )
    if dict(route_timing) != dict(expected_timing):
        raise ValueError("terminal route timing does not recompute")
    reason = route_timing.get("reason_code")
    if (
        route_timing.get("timing_status") == "within_sla"
        or not isinstance(reason, str)
        or reason not in ROUTE_OPPORTUNITY_REASON_CODES
    ):
        raise ValueError("terminal route timing must be outside the SLA")
    requested_notional = _decimal(
        requested_notional_usd,
        "requested_notional_usd",
        positive=True,
    )
    core_hash = _hash(core_manifest_sha256, "core_manifest_sha256")
    opportunity_id = route_opportunity_id(
        normalized_route["route_id"],
        requested_notional,
    )
    cost_hash = _terminal_cost_component_hash(
        cost_components,
        cohort_id=cohort,
        opportunity_id=opportunity_id,
        route=normalized_route,
        requested_notional=requested_notional,
        reason_code=reason,
    )
    mode_hash = _validated_terminal_mode_evidence(
        normalized_route,
        mode_evidence,
    )
    row: Dict[str, Any] = {
        "contract_version": ROUTE_OPPORTUNITY_CONTRACT_VERSION,
        "cohort_id": cohort,
        "route_id": normalized_route["route_id"],
        "opportunity_id": opportunity_id,
        "token_symbol": normalized_route["token_symbol"],
        "buy_market_id": normalized_route["buy_market_id"],
        "sell_market_id": normalized_route["sell_market_id"],
        "route_mode": normalized_route["route_mode"],
        "requested_notional_usd": _decimal_text(requested_notional),
        "target_token_quantity": None,
        "target_base_raw": None,
        "target_base_unit_decimals": None,
        "target_lattice_raw": None,
        "buy_state_id": None,
        "sell_state_id": None,
        "buy_state_observed_at": None,
        "sell_state_observed_at": None,
        "skew_seconds": route_timing["skew_seconds"],
        "route_age_seconds": None,
        "gross_buy_cost_usd": None,
        "gross_sell_proceeds_usd": None,
        "gross_edge_usd": None,
        "gross_edge_bps": None,
        "gross_edge_bps_numerator": None,
        "gross_edge_bps_denominator": None,
        "strict_nonembedded_cost_usd": None,
        "research_bounded_cost_usd": None,
        "research_assumed_cost_usd": None,
        "strict_net_edge_usd": None,
        "strict_net_edge_bps": None,
        "strict_net_edge_bps_numerator": None,
        "strict_net_edge_bps_denominator": None,
        "research_net_edge_usd": None,
        "research_net_edge_bps": None,
        "research_net_edge_bps_numerator": None,
        "research_net_edge_bps_denominator": None,
        "edge_bps_denominator_basis": None,
        "cost_completeness": "unavailable",
        "scenario_cost_completeness": "unavailable",
        "reflected_or_embedded_component_keys": [],
        "component_reasons": [],
        "mode_evidence_eligible": False,
        "inventory_profile_hash": None,
        "maximum_proved_capacity_quantity": None,
        "opportunity_class": "unavailable",
        "primary_reason": reason,
        "reason_codes": [reason],
        "strict_eligible": False,
        "strict_ready_for_publication": False,
        "publication_attestation_sha256": None,
        "buy_usd_projection_sha256": None,
        "sell_usd_projection_sha256": None,
        "cost_component_set_sha256": cost_hash,
        "mode_evidence_sha256": mode_hash,
        "buy_core_manifest_sha256": core_hash,
        "sell_core_manifest_sha256": core_hash,
    }
    row["evidence_binding_sha256"] = _canonical_json_sha256(row)
    return row


def _empty_opportunity(
    *,
    cohort_id: str,
    route: Mapping[str, Any],
    opportunity_id: str,
    requested_notional: Decimal,
    target: CommonTarget,
    buy_quote: QuantityQuote,
    sell_quote: QuantityQuote,
    skew: Optional[Fraction],
    age: Optional[Fraction],
    reasons: Sequence[str],
    buy_projection_hash: Optional[str],
    sell_projection_hash: Optional[str],
    component_set_hash: Optional[str],
    mode_hash: Optional[str],
    buy_core_hash: Optional[str],
    sell_core_hash: Optional[str],
    publication_attestation_hash: Optional[str] = None,
) -> Dict[str, Any]:
    ordered_reasons = _reason_order(reasons)
    row: Dict[str, Any] = {
        "contract_version": ROUTE_OPPORTUNITY_CONTRACT_VERSION,
        "cohort_id": cohort_id,
        "route_id": route["route_id"],
        "opportunity_id": opportunity_id,
        "token_symbol": route["token_symbol"],
        "buy_market_id": route["buy_market_id"],
        "sell_market_id": route["sell_market_id"],
        "route_mode": route["route_mode"],
        "requested_notional_usd": _decimal_text(requested_notional),
        "target_token_quantity": _decimal_text(target.quantity),
        "target_base_raw": str(target.raw_quantity),
        "target_base_unit_decimals": target.unit_decimals,
        "target_lattice_raw": str(target.lattice_raw),
        "buy_state_id": buy_quote.state_id,
        "sell_state_id": sell_quote.state_id,
        "buy_state_observed_at": buy_quote.state_observed_at,
        "sell_state_observed_at": sell_quote.state_observed_at,
        "skew_seconds": _decimal_text(skew, "skew") if skew is not None else None,
        "route_age_seconds": _decimal_text(age, "route age") if age is not None else None,
        "gross_buy_cost_usd": None,
        "gross_sell_proceeds_usd": None,
        "gross_edge_usd": None,
        "gross_edge_bps": None,
        "gross_edge_bps_numerator": None,
        "gross_edge_bps_denominator": None,
        "strict_nonembedded_cost_usd": None,
        "research_bounded_cost_usd": None,
        "research_assumed_cost_usd": None,
        "strict_net_edge_usd": None,
        "strict_net_edge_bps": None,
        "strict_net_edge_bps_numerator": None,
        "strict_net_edge_bps_denominator": None,
        "research_net_edge_usd": None,
        "research_net_edge_bps": None,
        "research_net_edge_bps_numerator": None,
        "research_net_edge_bps_denominator": None,
        "edge_bps_denominator_basis": "gross_buy_cost_usd",
        "cost_completeness": "unavailable",
        "scenario_cost_completeness": "unavailable",
        "reflected_or_embedded_component_keys": [],
        "component_reasons": [],
        "mode_evidence_eligible": False,
        "inventory_profile_hash": None,
        "maximum_proved_capacity_quantity": None,
        "opportunity_class": "unavailable",
        "primary_reason": ordered_reasons[0] if ordered_reasons else "buy_leg_unavailable",
        "reason_codes": ordered_reasons,
        "strict_eligible": False,
        "strict_ready_for_publication": False,
        "publication_attestation_sha256": publication_attestation_hash,
        "buy_usd_projection_sha256": buy_projection_hash,
        "sell_usd_projection_sha256": sell_projection_hash,
        "cost_component_set_sha256": component_set_hash,
        "mode_evidence_sha256": mode_hash,
        "buy_core_manifest_sha256": buy_core_hash,
        "sell_core_manifest_sha256": sell_core_hash,
    }
    row["evidence_binding_sha256"] = _canonical_json_sha256(row)
    return row


def build_route_opportunity(
    *,
    cohort_id: Any,
    route: Mapping[str, Any],
    requested_notional_usd: Any,
    common_target: CommonTarget,
    buy_leg: Mapping[str, Any],
    sell_leg: Mapping[str, Any],
    buy_quote: QuantityQuote,
    sell_quote: QuantityQuote,
    buy_quote_evidence: Any,
    sell_quote_evidence: Any,
    buy_usd_projection: Any,
    sell_usd_projection: Any,
    cost_components: Iterable[Mapping[str, Any]],
    mode_evidence: Any,
    now: Any,
    publication_attestation: Any = None,
) -> Dict[str, Any]:
    """Build one route/notional opportunity from independently bound evidence."""
    cohort = _required_text(cohort_id, "cohort_id")
    if publication_attestation is not None and not isinstance(
        publication_attestation, _PublicationAttestation
    ):
        raise ValueError("publication attestation must be the sealed internal type")
    if _COHORT_ID.fullmatch(cohort) is None:
        raise ValueError("cohort_id must be canonical")
    normalized_route = _validated_route(route)
    _validate_route_topology(normalized_route)
    if not isinstance(common_target, CommonTarget):
        raise ValueError("common_target must be CommonTarget")
    if not isinstance(buy_quote, QuantityQuote) or not isinstance(
        sell_quote, QuantityQuote
    ):
        raise ValueError("buy_quote and sell_quote must be QuantityQuote")
    validate_quantity_quote(buy_quote)
    validate_quantity_quote(sell_quote)
    if buy_quote.direction != "buy" or sell_quote.direction != "sell":
        raise ValueError("route quote directions are invalid")
    if (
        buy_quote.market_id != normalized_route["buy_market_id"]
        or sell_quote.market_id != normalized_route["sell_market_id"]
    ):
        raise ValueError("route quote market identity mismatch")
    if common_target.asset != normalized_route["token_symbol"]:
        raise ValueError("common target Token does not match route")
    requested_notional = _decimal(
        requested_notional_usd, "requested_notional_usd", positive=True
    )
    opportunity_id = route_opportunity_id(
        normalized_route["route_id"], requested_notional
    )
    _now, now_epoch = _timestamp(now, "now")

    reasons: List[str] = []
    for leg, quote, market_id, side in (
        (buy_leg, buy_quote, normalized_route["buy_market_id"], "buy"),
        (sell_leg, sell_quote, normalized_route["sell_market_id"], "sell"),
    ):
        reason = _leg_reason(
            leg, quote=quote, expected_market_id=market_id, side=side
        )
        if reason is not None:
            reasons.append(reason)
    if not buy_quote.calculation_complete or not sell_quote.calculation_complete:
        reasons.append("leg_not_completely_filled")
    if not _quote_matches_common_target(
        buy_quote, common_target
    ) or not _quote_matches_common_target(sell_quote, common_target):
        reasons.append("common_quantity_unavailable")

    (
        buy_quote_replayed,
        buy_quote_strict,
        buy_fee,
        buy_pool_fee,
        buy_core_hash,
    ) = _validated_quote_evidence(
        buy_quote,
        buy_quote_evidence,
        target=common_target,
        direction="buy",
    )
    (
        sell_quote_replayed,
        sell_quote_strict,
        sell_fee,
        sell_pool_fee,
        sell_core_hash,
    ) = _validated_quote_evidence(
        sell_quote,
        sell_quote_evidence,
        target=common_target,
        direction="sell",
    )
    if not buy_quote_replayed or not sell_quote_replayed:
        reasons.append("quantity_quote_evidence_mismatch")
    elif buy_core_hash != sell_core_hash:
        reasons.append("quantity_quote_evidence_mismatch")

    skew: Optional[Fraction] = None
    age: Optional[Fraction] = None
    try:
        _buy_state, buy_epoch = _timestamp(
            buy_quote.state_observed_at, "buy state_observed_at"
        )
        _sell_state, sell_epoch = _timestamp(
            sell_quote.state_observed_at, "sell state_observed_at"
        )
        if buy_epoch > now_epoch or sell_epoch > now_epoch:
            reasons.append("invalid_state_timestamp")
        else:
            skew = abs(buy_epoch - sell_epoch)
            age = now_epoch - max(buy_epoch, sell_epoch)
            if skew > MAX_ROUTE_SKEW_SECONDS:
                reasons.append("snapshot_skew_exceeded")
    except ValueError:
        reasons.append("invalid_state_timestamp")

    buy_amount: Optional[Fraction] = None
    sell_amount: Optional[Fraction] = None
    buy_projection_strict = False
    sell_projection_strict = False
    buy_projection_hash = None
    sell_projection_hash = None
    buy_projection_core_hash = None
    sell_projection_core_hash = None
    try:
        (
            buy_amount,
            buy_projection_strict,
            buy_projection_reason,
            buy_projection_hash,
            buy_projection_core_hash,
        ) = _validated_usd_projection(
            buy_usd_projection,
            quote=buy_quote,
            direction="buy",
            now_epoch=now_epoch,
        )
        (
            sell_amount,
            sell_projection_strict,
            sell_projection_reason,
            sell_projection_hash,
            sell_projection_core_hash,
        ) = _validated_usd_projection(
            sell_usd_projection,
            quote=sell_quote,
            direction="sell",
            now_epoch=now_epoch,
        )
        if buy_projection_reason is not None or sell_projection_reason is not None:
            reasons.append("usd_conversion_unavailable")
        if (
            buy_projection_core_hash is not None
            and buy_projection_core_hash != buy_core_hash
        ) or (
            sell_projection_core_hash is not None
            and sell_projection_core_hash != sell_core_hash
        ):
            reasons.append("usd_conversion_unavailable")
    except ValueError:
        raise

    mode_eligible, mode_reasons, profile_hash, capacity, mode_hash = (
        _validated_mode_evidence(normalized_route, mode_evidence, common_target)
    )

    cost_analysis: Optional[Dict[str, Any]] = None
    try:
        cost_analysis = _analyze_cost_components(
            cost_components,
            cohort_id=cohort,
            opportunity_id=opportunity_id,
            route=normalized_route,
            target=common_target,
            requested_notional=requested_notional,
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            buy_fee=buy_fee,
            sell_fee=sell_fee,
            buy_pool_fee=buy_pool_fee,
            sell_pool_fee=sell_pool_fee,
            now_epoch=now_epoch,
        )
    except ValueError:
        raise

    publication_attestation_hash = _validated_publication_attestation(
        publication_attestation,
        cohort_id=cohort,
        opportunity_id=opportunity_id,
        route_id=normalized_route["route_id"],
        target_token_quantity=common_target.quantity,
        buy_state_id=buy_quote.state_id,
        sell_state_id=sell_quote.state_id,
        buy_usd_projection_sha256=buy_projection_hash,
        sell_usd_projection_sha256=sell_projection_hash,
        cost_component_set_sha256=cost_analysis["set_sha256"],
        mode_evidence_sha256=mode_hash,
        core_manifest_sha256=(
            buy_core_hash if buy_core_hash == sell_core_hash else None
        ),
    )

    hard_reasons = {
        "route_deadline_exceeded",
        "execution_adapter_unsupported",
        "buy_leg_unavailable",
        "sell_leg_unavailable",
        "leg_not_completely_filled",
        "invalid_state_timestamp",
        "snapshot_skew_exceeded",
        "common_quantity_unavailable",
        "quantity_quote_evidence_mismatch",
        "usd_conversion_unavailable",
    }
    if cost_analysis is not None and not cost_analysis["required_scenario_complete"]:
        reasons.append("cost_components_incomplete")
        hard_reasons.add("cost_components_incomplete")
    if any(reason in hard_reasons for reason in reasons):
        unavailable = _empty_opportunity(
            cohort_id=cohort,
            route=normalized_route,
            opportunity_id=opportunity_id,
            requested_notional=requested_notional,
            target=common_target,
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            skew=skew,
            age=age,
            reasons=reasons,
            buy_projection_hash=buy_projection_hash,
            sell_projection_hash=sell_projection_hash,
            component_set_hash=(
                cost_analysis["set_sha256"] if cost_analysis is not None else None
            ),
            mode_hash=mode_hash,
            buy_core_hash=buy_core_hash,
            sell_core_hash=sell_core_hash,
            publication_attestation_hash=publication_attestation_hash,
        )
        if cost_analysis is not None:
            unavailable.update(
                {
                    "cost_completeness": (
                        "complete"
                        if cost_analysis["strict_complete"]
                        else "incomplete"
                    ),
                    "scenario_cost_completeness": (
                        "complete"
                        if cost_analysis["scenario_complete"]
                        else "incomplete"
                    ),
                    "reflected_or_embedded_component_keys": cost_analysis[
                        "reflected"
                    ],
                    "component_reasons": cost_analysis["component_reasons"],
                }
            )
            unavailable["evidence_binding_sha256"] = _canonical_json_sha256(
                {
                    key: value
                    for key, value in unavailable.items()
                    if key != "evidence_binding_sha256"
                }
            )
        return unavailable

    assert buy_amount is not None and sell_amount is not None
    assert cost_analysis is not None
    gross_edge = sell_amount - buy_amount
    strict_cost = cost_analysis["strict_total"]
    bounded_cost = cost_analysis["bounded_total"]
    assumed_cost = cost_analysis["assumed_total"]
    strict_net = gross_edge - strict_cost
    research_net = (
        gross_edge - strict_cost - bounded_cost - assumed_cost
        if cost_analysis["scenario_complete"]
        else None
    )

    soft_reasons: List[str] = []
    if age is not None and age > MAX_ROUTE_AGE_SECONDS:
        soft_reasons.append("cohort_stale")
    if not buy_quote_strict or not sell_quote_strict:
        soft_reasons.append("quantity_quote_evidence_not_strict")
    if not buy_projection_strict or not sell_projection_strict:
        soft_reasons.append("usd_conversion_not_strict")
    soft_reasons.extend(mode_reasons)
    if not cost_analysis["strict_complete"]:
        soft_reasons.append("cost_components_incomplete")
    if cost_analysis["estimated_required"]:
        soft_reasons.append("cost_component_estimated")
    if strict_net <= 0:
        soft_reasons.append("non_positive_net_edge")

    strict_ready_for_publication = (
        not soft_reasons and mode_eligible and strict_net > 0
    )
    strict_eligible = (
        strict_ready_for_publication
        and publication_attestation_hash is not None
    )
    if strict_eligible:
        opportunity_class = "executable_candidate"
        primary_reason = "positive_strict_net_edge"
        ordered_reasons: List[str] = []
    else:
        opportunity_class = "research_estimate"
        if strict_ready_for_publication:
            soft_reasons.append("publication_evidence_unverified")
        ordered_reasons = _reason_order(soft_reasons)
        primary_reason = ordered_reasons[0] if ordered_reasons else "non_positive_net_edge"

    gross_bps, gross_bps_num, gross_bps_den = _ratio_fields(gross_edge, buy_amount)
    strict_bps, strict_bps_num, strict_bps_den = _ratio_fields(strict_net, buy_amount)
    research_bps, research_bps_num, research_bps_den = _ratio_fields(
        research_net, buy_amount
    )
    row = _empty_opportunity(
        cohort_id=cohort,
        route=normalized_route,
        opportunity_id=opportunity_id,
        requested_notional=requested_notional,
        target=common_target,
        buy_quote=buy_quote,
        sell_quote=sell_quote,
        skew=skew,
        age=age,
        reasons=[primary_reason],
        buy_projection_hash=buy_projection_hash,
        sell_projection_hash=sell_projection_hash,
        component_set_hash=cost_analysis["set_sha256"],
        mode_hash=mode_hash,
        buy_core_hash=buy_core_hash,
        sell_core_hash=sell_core_hash,
        publication_attestation_hash=publication_attestation_hash,
    )
    row.update(
        {
            "gross_buy_cost_usd": _decimal_text(buy_amount, "buy USD"),
            "gross_sell_proceeds_usd": _decimal_text(sell_amount, "sell USD"),
            "gross_edge_usd": _decimal_text(gross_edge, "gross edge"),
            "gross_edge_bps": gross_bps,
            "gross_edge_bps_numerator": gross_bps_num,
            "gross_edge_bps_denominator": gross_bps_den,
            "strict_nonembedded_cost_usd": _decimal_text(
                strict_cost, "strict cost"
            ),
            "research_bounded_cost_usd": _decimal_text(
                bounded_cost, "bounded cost"
            ),
            "research_assumed_cost_usd": _decimal_text(
                assumed_cost, "assumed cost"
            ),
            "strict_net_edge_usd": _decimal_text(strict_net, "strict net edge"),
            "strict_net_edge_bps": strict_bps,
            "strict_net_edge_bps_numerator": strict_bps_num,
            "strict_net_edge_bps_denominator": strict_bps_den,
            "research_net_edge_usd": (
                _decimal_text(research_net, "research net edge")
                if research_net is not None
                else None
            ),
            "research_net_edge_bps": research_bps,
            "research_net_edge_bps_numerator": research_bps_num,
            "research_net_edge_bps_denominator": research_bps_den,
            "cost_completeness": (
                "complete" if cost_analysis["strict_complete"] else "incomplete"
            ),
            "scenario_cost_completeness": (
                "complete" if cost_analysis["scenario_complete"] else "incomplete"
            ),
            "reflected_or_embedded_component_keys": cost_analysis["reflected"],
            "component_reasons": cost_analysis["component_reasons"],
            "mode_evidence_eligible": mode_eligible,
            "inventory_profile_hash": profile_hash,
            "maximum_proved_capacity_quantity": capacity,
            "opportunity_class": opportunity_class,
            "primary_reason": primary_reason,
            "reason_codes": ordered_reasons,
            "strict_eligible": strict_eligible,
            "strict_ready_for_publication": strict_ready_for_publication,
            "publication_attestation_sha256": publication_attestation_hash,
        }
    )
    row["evidence_binding_sha256"] = _canonical_json_sha256(
        {key: value for key, value in row.items() if key != "evidence_binding_sha256"}
    )
    return row


def validate_route_opportunity(
    opportunity: Any,
    **build_inputs: Any,
) -> Mapping[str, Any]:
    """Rebuild one opportunity from source evidence and compare every field."""
    if not isinstance(opportunity, Mapping) or set(opportunity) != OPPORTUNITY_FIELDS:
        raise ValueError("route opportunity schema is invalid")
    provided = dict(opportunity)
    binding = _hash(
        provided.pop("evidence_binding_sha256"),
        "evidence_binding_sha256",
    )
    if binding != _canonical_json_sha256(provided):
        raise ValueError("route opportunity evidence binding mismatch")
    expected = (
        build_terminal_route_opportunity(**build_inputs)
        if "route_timing" in build_inputs
        else build_route_opportunity(**build_inputs)
    )
    if dict(opportunity) != expected:
        raise ValueError("route opportunity evidence does not reproduce row")
    return opportunity
