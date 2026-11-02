"""Validated, compact projections of immutable route opportunity bundles."""

from __future__ import annotations

import os
import re
import stat
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlsplit

try:
    from dashboard.freshness import (
        ROUTE_OPPORTUNITY_MAX_AGE_SECONDS,
        ROUTE_OPPORTUNITY_MAX_SKEW_SECONDS,
        route_opportunity_freshness,
    )
except ModuleNotFoundError:  # pragma: no cover - direct package execution
    from freshness import (  # type: ignore[no-redef]
        ROUTE_OPPORTUNITY_MAX_AGE_SECONDS,
        ROUTE_OPPORTUNITY_MAX_SKEW_SECONDS,
        route_opportunity_freshness,
    )
from scripts.route_publication import (
    REQUESTED_NOTIONALS_USD,
    RoutePublicationError,
    load_latest_complete_route_bundle,
)
from scripts.route_opportunity import (
    ROUTE_OPPORTUNITY_MODES,
    ROUTE_OPPORTUNITY_REASON_CODES,
)
from scripts.timestamp_contract import parse_rfc3339_utc


OPPORTUNITY_SUMMARY_CONTRACT = "opportunity_summary/v1"
COMPLETE_POINTER_ABSENT = "complete_pointer_absent"
OPPORTUNITY_BUNDLE_VALIDATION_FAILED = "opportunity_bundle_validation_failed"
MAX_ROUTE_AGE_SECONDS = Decimal(str(ROUTE_OPPORTUNITY_MAX_AGE_SECONDS))
MAX_ROUTE_SKEW_SECONDS = Decimal(str(ROUTE_OPPORTUNITY_MAX_SKEW_SECONDS))
ROUTE_VOLUME_BASIS = "minimum_leg_source_horizon_usd"

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TOKEN = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII)
_VENUE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z", flags=re.ASCII)
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_COLLECTED_NOTIONALS = frozenset(
    str(value) for value in REQUESTED_NOTIONALS_USD
)
_COLLECTED_NOTIONAL_VALUES = {
    Decimal(value): value for value in _COLLECTED_NOTIONALS
}
_MAX_DECIMAL_TEXT_LENGTH = 128
_MAX_DECIMAL_DIGITS = 128
_MAX_DECIMAL_EXPONENT = 128
APPROVED_OPPORTUNITY_SOURCE_HOSTS = frozenset({
    "api.binance.com",
    "data-api.binance.vision",
    "www.okx.com",
    "api.bybit.com",
    "api.kucoin.com",
    "api.gateio.ws",
    "api.bitget.com",
    "api.mexc.com",
    "api.huobi.pro",
    "api.exchange.coinbase.com",
    "api.kraken.com",
    "api.crypto.com",
    "api.upbit.com",
    "ethereum-rpc.publicnode.com",
    "arb1.arbitrum.io",
    "mainnet.optimism.io",
    "base-rpc.publicnode.com",
    "bsc-dataseed.bnbchain.org",
    "mainnet.era.zksync.io",
})

_OPPORTUNITY_CLASSES = frozenset(
    {"executable_candidate", "research_estimate", "unavailable"}
)
_CLASS_FILTERS = frozenset({"strict", "estimate", "all"})
_CLASS_ALIASES = {
    "strict": "executable_candidate",
    "estimate": "research_estimate",
}
_ROUTE_TYPES = frozenset({"cex_cex", "cex_dex", "dex_dex", "all"})
_AVAILABILITIES = frozenset({"available", "unavailable", "all"})
_DIRECTIONS = frozenset({"asc", "desc"})
_SORT_FIELDS = frozenset(
    {
        "net_edge_usd",
        "net_edge_bps",
        "capacity_quantity",
        "skew_seconds",
        "route_age_seconds",
        "volume",
        "requested_notional_usd",
        "token_symbol",
        "route_id",
    }
)
_NUMERIC_SORT_FIELDS = _SORT_FIELDS - {"token_symbol", "route_id"}
_ASCENDING_DEFAULT_SORT_FIELDS = frozenset({
    "route_age_seconds",
    "skew_seconds",
})
_STRICT_COST_STATUSES = frozenset(
    {"measured", "authenticated", "quoted", "not_applicable"}
)
_KNOWN_COST_STATUSES = _STRICT_COST_STATUSES | {
    "bounded_estimate",
    "assumed",
    "unavailable",
    "unsupported",
    "failed",
    "stale",
}
_SCENARIO_COST_STATUSES = frozenset({"bounded_estimate", "assumed"})
_DYNAMIC_COST_STATUSES = (
    (_STRICT_COST_STATUSES - {"not_applicable"})
    | _SCENARIO_COST_STATUSES
)


class OpportunityBundleUnavailable(FileNotFoundError):
    """A complete public pointer has not been published yet."""

    def __init__(self, reason: str = COMPLETE_POINTER_ABSENT) -> None:
        self.reason = reason
        super().__init__(reason)


class OpportunityBundleInvalid(RuntimeError):
    """A publication exists but cannot be trusted or projected."""

    def __init__(
        self, reason: str = OPPORTUNITY_BUNDLE_VALIDATION_FAILED
    ) -> None:
        self.reason = reason
        super().__init__(reason)


class OpportunityQueryError(ValueError):
    """A bounded opportunity query cannot be normalized."""


def resolve_opportunity_bundle(routes_root: Optional[Path] = None) -> Path:
    """Resolve the configured complete-route root without following its pointer."""

    if routes_root is not None:
        return Path(routes_root)
    explicit = os.environ.get("MARKET_ROUTE_DATA_DIR", "").strip()
    if explicit:
        return Path(explicit)
    data_root = os.environ.get("MARKET_DATA_DIR", "").strip()
    if data_root:
        return Path(data_root) / "routes"
    return _PROJECT_ROOT / "data" / "local" / "routes"


def _pointer_state(root: Path) -> str:
    try:
        root_details = os.lstat(str(root))
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "invalid"
    if stat.S_ISLNK(root_details.st_mode) or not stat.S_ISDIR(root_details.st_mode):
        return "invalid"
    try:
        pointer_details = os.lstat(str(root / "latest.json"))
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "invalid"
    if stat.S_ISLNK(pointer_details.st_mode) or not stat.S_ISREG(
        pointer_details.st_mode
    ):
        return "invalid"
    return "present"


def load_latest_opportunities(
    routes_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Fully validate the public five-file bundle before returning any row."""

    root = resolve_opportunity_bundle(routes_root)
    pointer_state = _pointer_state(root)
    if pointer_state == "missing":
        raise OpportunityBundleUnavailable()
    if pointer_state != "present":
        raise OpportunityBundleInvalid()
    try:
        return load_latest_complete_route_bundle(
            root,
            core_root=root / "core",
        )
    except (OSError, RoutePublicationError, TypeError, ValueError):
        raise OpportunityBundleInvalid() from None


def _canonical_decimal_text(value: Any, label: str) -> str:
    try:
        text_value = str(value).strip()
    except (TypeError, ValueError):
        raise OpportunityQueryError("{} is invalid".format(label)) from None
    if not text_value or len(text_value) > _MAX_DECIMAL_TEXT_LENGTH:
        raise OpportunityQueryError("{} is invalid".format(label))
    try:
        decimal = Decimal(text_value)
    except (InvalidOperation, TypeError, ValueError):
        raise OpportunityQueryError("{} is invalid".format(label)) from None
    if not decimal.is_finite() or decimal <= 0:
        raise OpportunityQueryError("{} must be a positive finite number".format(label))
    decimal_tuple = decimal.as_tuple()
    if (
        len(decimal_tuple.digits) > _MAX_DECIMAL_DIGITS
        or not isinstance(decimal_tuple.exponent, int)
        or abs(decimal_tuple.exponent) > _MAX_DECIMAL_EXPONENT
    ):
        raise OpportunityQueryError("{} is invalid".format(label))
    normalized = format(decimal, "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def _collected_notional_text(value: Any) -> str:
    text = _canonical_decimal_text(value, "notional")
    try:
        decimal = Decimal(text)
    except InvalidOperation:
        raise OpportunityQueryError("notional is invalid") from None
    canonical = _COLLECTED_NOTIONAL_VALUES.get(decimal)
    if canonical is None:
        raise OpportunityQueryError(
            "notional must be one of the collected opportunity notionals"
        )
    return canonical


def _normalize_filters(
    *,
    token: Optional[str] = None,
    venue: Optional[str] = None,
    notional_usd: Any = None,
    opportunity_class: Optional[str] = None,
    route_type: Optional[str] = None,
    availability: Optional[str] = None,
    sort: Optional[str] = None,
    direction: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    normalized_token = None
    if token is not None:
        normalized_token = str(token).strip().upper()
        if _TOKEN.fullmatch(normalized_token) is None:
            raise OpportunityQueryError("token is invalid")
    normalized_venue = None
    if venue is not None:
        normalized_venue = str(venue).strip().lower()
        if (
            normalized_venue == "all"
            or _VENUE.fullmatch(normalized_venue) is None
        ):
            raise OpportunityQueryError("venue is invalid")
    normalized_notional = (
        _collected_notional_text(notional_usd)
        if notional_usd not in (None, "")
        else None
    )
    normalized_class = str(opportunity_class or "all").strip().lower()
    normalized_route_type = str(route_type or "all").strip().lower()
    normalized_availability = str(availability or "all").strip().lower()
    normalized_sort = str(sort or "net_edge_usd").strip().lower()
    normalized_direction = str(
        direction
        or (
            "asc"
            if normalized_sort in _ASCENDING_DEFAULT_SORT_FIELDS
            else "desc"
        )
    ).strip().lower()
    if normalized_class not in _CLASS_FILTERS:
        raise OpportunityQueryError("class must be strict, estimate, or all")
    if normalized_route_type not in _ROUTE_TYPES:
        raise OpportunityQueryError(
            "route_type must be cex_cex, cex_dex, dex_dex, or all"
        )
    if normalized_availability not in _AVAILABILITIES:
        raise OpportunityQueryError(
            "availability must be available, unavailable, or all"
        )
    if normalized_sort not in _SORT_FIELDS:
        raise OpportunityQueryError("sort is unsupported")
    if normalized_direction not in _DIRECTIONS:
        raise OpportunityQueryError("dir must be asc or desc")
    return {
        "token": normalized_token,
        "venue": normalized_venue,
        "notional_usd": normalized_notional,
        "opportunity_class": normalized_class,
        "route_type": normalized_route_type,
        "availability": normalized_availability,
        "sort": normalized_sort,
        "direction": normalized_direction,
    }


def normalize_opportunity_filters(
    *,
    token: Optional[str] = None,
    venue: Optional[str] = None,
    notional_usd: Any = None,
    opportunity_class: Optional[str] = None,
    route_type: Optional[str] = None,
    availability: Optional[str] = None,
    sort: Optional[str] = None,
    direction: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Public query normalizer shared by the server and payload builder."""

    return _normalize_filters(
        token=token,
        venue=venue,
        notional_usd=notional_usd,
        opportunity_class=opportunity_class,
        route_type=route_type,
        availability=availability,
        sort=sort,
        direction=direction,
    )


def _route_type(row: Mapping[str, Any]) -> str:
    buy = str(row.get("buy_market_id") or "")
    sell = str(row.get("sell_market_id") or "")
    types = {
        "cex" if market.startswith("cex:") else
        "dex" if market.startswith("dex:") else ""
        for market in (buy, sell)
    }
    if types == {"cex"}:
        return "cex_cex"
    if types == {"dex"}:
        return "dex_dex"
    if types == {"cex", "dex"}:
        return "cex_dex"
    raise OpportunityBundleInvalid()


def _canonical_leg_venue(market_id: Any) -> str:
    if not isinstance(market_id, str):
        raise OpportunityBundleInvalid()
    parts = market_id.split(":")
    if len(parts) >= 3 and parts[0] == "cex":
        venue = parts[1]
    elif len(parts) >= 5 and parts[0] == "dex":
        venue = parts[2]
    else:
        raise OpportunityBundleInvalid()
    if venue == "all" or _VENUE.fullmatch(venue) is None:
        raise OpportunityBundleInvalid()
    return venue


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise OpportunityBundleInvalid()
    try:
        return parse_rfc3339_utc(value)
    except (TypeError, ValueError):
        raise OpportunityBundleInvalid() from None


def _timing(row: Mapping[str, Any], now: datetime) -> Dict[str, Any]:
    try:
        freshness = route_opportunity_freshness(
            row.get("buy_state_observed_at"),
            row.get("sell_state_observed_at"),
            now=now,
        )
    except (TypeError, ValueError):
        raise OpportunityBundleInvalid() from None
    return {
        "skew_seconds": freshness["skew_seconds"],
        "route_age_seconds": freshness["age_seconds"],
        "reason": freshness["reason"],
    }


def _safe_origin(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    normalized_host = hostname.lower().rstrip(".")
    if (
        normalized_host not in APPROVED_OPPORTUNITY_SOURCE_HOSTS
        or port not in (None, 443)
    ):
        return None
    return "https://{}".format(normalized_host)


def _source_links(
    row: Mapping[str, Any], legs_by_market: Mapping[str, Mapping[str, Any]]
) -> List[Dict[str, Any]]:
    result = []
    observed = set()
    for field in ("buy_market_id", "sell_market_id"):
        market_id = str(row.get(field) or "")
        if market_id in observed:
            continue
        observed.add(market_id)
        leg = legs_by_market.get(market_id, {})
        origin = _safe_origin(leg.get("source_endpoint"))
        result.append({"market_id": market_id, "url": origin})
    return result


def _bundle_fraction(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Fraction:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > _MAX_DECIMAL_TEXT_LENGTH
    ):
        raise OpportunityBundleInvalid()
    try:
        decimal = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise OpportunityBundleInvalid() from None
    decimal_tuple = decimal.as_tuple()
    if (
        not decimal.is_finite()
        or len(decimal_tuple.digits) > _MAX_DECIMAL_DIGITS
        or not isinstance(decimal_tuple.exponent, int)
        or abs(decimal_tuple.exponent) > _MAX_DECIMAL_EXPONENT
    ):
        raise OpportunityBundleInvalid()
    result = Fraction(decimal)
    if positive and result <= 0:
        raise OpportunityBundleInvalid()
    if nonnegative and result < 0:
        raise OpportunityBundleInvalid()
    return result


def _rounded_ratio(value: Fraction, places: int = 8) -> Fraction:
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
    return Fraction(quotient * sign, scale)


def _expected_component_keys(row: Mapping[str, Any]) -> set:
    expected = {("route", "rebalancing_or_transfer")}
    has_dex = False
    for leg in ("buy", "sell"):
        market_id = row.get(leg + "_market_id")
        if not isinstance(market_id, str):
            raise OpportunityBundleInvalid()
        if market_id.startswith("cex:"):
            expected.add((leg, "venue_taker_fee"))
        elif market_id.startswith("dex:"):
            has_dex = True
            expected.update({
                (leg, "pool_swap_fee"),
                (leg, "network_gas"),
                (leg, "router_or_integrator_fee"),
                (leg, "token_transfer_tax"),
            })
        else:
            raise OpportunityBundleInvalid()
    if has_dex:
        expected.add(("route", "mev_buffer"))
    return expected


def _validate_component_inventory(
    row: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
) -> None:
    expected = _expected_component_keys(row)
    observed = set()
    row_notional = _bundle_fraction(
        row.get("requested_notional_usd"),
        "route notional",
        positive=True,
    )
    row_target = _bundle_fraction(
        row.get("target_token_quantity"),
        "target quantity",
        positive=True,
    )
    for component in component_rows:
        leg = component.get("leg")
        component_type = component.get("component_type")
        if not isinstance(leg, str) or not isinstance(component_type, str):
            raise OpportunityBundleInvalid()
        key = (leg, component_type)
        if key in observed:
            raise OpportunityBundleInvalid()
        observed.add(key)
        expected_market = (
            "" if leg == "route" else row.get(leg + "_market_id")
        )
        if (
            component.get("market_id") != expected_market
            or component.get("cohort_id") != row.get("cohort_id")
            or type(component.get("strict_eligible")) is not bool
            or type(component.get("embedded_in_leg_quote")) is not bool
            or _bundle_fraction(
                component.get("requested_notional_usd"),
                "component notional",
                positive=True,
            )
            != row_notional
            or _bundle_fraction(
                component.get("target_token_quantity"),
                "component target",
                positive=True,
            )
            != row_target
        ):
            raise OpportunityBundleInvalid()
    if observed != expected:
        raise OpportunityBundleInvalid()


def _validate_ratio(
    row: Mapping[str, Any],
    prefix: str,
    edge: Fraction,
    gross_buy: Fraction,
) -> None:
    exact = edge * 10_000 / gross_buy
    if (
        _bundle_fraction(row.get(prefix + "_bps"), prefix + " bps")
        != _rounded_ratio(exact)
        or row.get(prefix + "_bps_numerator") != str(exact.numerator)
        or row.get(prefix + "_bps_denominator") != str(exact.denominator)
    ):
        raise OpportunityBundleInvalid()


def _validate_strict_economics(
    row: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
) -> None:
    if row.get("opportunity_class") != "executable_candidate":
        return
    if (
        row.get("cost_completeness") != "complete"
        or row.get("scenario_cost_completeness") != "complete"
    ):
        raise OpportunityBundleInvalid()
    target = _bundle_fraction(
        row.get("target_token_quantity"),
        "target quantity",
        positive=True,
    )
    capacity = _bundle_fraction(
        row.get("maximum_proved_capacity_quantity"),
        "proved capacity",
        positive=True,
    )
    if capacity < target:
        raise OpportunityBundleInvalid()
    gross_buy = _bundle_fraction(
        row.get("gross_buy_cost_usd"),
        "gross buy cost",
        positive=True,
    )
    gross_sell = _bundle_fraction(
        row.get("gross_sell_proceeds_usd"),
        "gross sell proceeds",
        positive=True,
    )
    gross_edge = _bundle_fraction(
        row.get("gross_edge_usd"),
        "gross edge",
    )
    strict_cost = _bundle_fraction(
        row.get("strict_nonembedded_cost_usd"),
        "strict cost",
        nonnegative=True,
    )
    strict_net = _bundle_fraction(
        row.get("strict_net_edge_usd"),
        "strict net edge",
        positive=True,
    )
    if (
        gross_edge != gross_sell - gross_buy
        or strict_net != gross_edge - strict_cost
    ):
        raise OpportunityBundleInvalid()

    reflected_values = row.get("reflected_or_embedded_component_keys")
    if (
        not isinstance(reflected_values, list)
        or any(not isinstance(value, str) for value in reflected_values)
        or reflected_values != sorted(set(reflected_values))
    ):
        raise OpportunityBundleInvalid()
    reflected = set(reflected_values)
    component_total = Fraction(0)
    requested_notional = _bundle_fraction(
        row.get("requested_notional_usd"),
        "route notional",
        positive=True,
    )
    for component in component_rows:
        status_value = component.get("value_status")
        if (
            status_value not in _STRICT_COST_STATUSES
            or component.get("strict_eligible") is not True
        ):
            raise OpportunityBundleInvalid()
        if status_value == "not_applicable":
            if (
                component.get("amount_usd") is not None
                or component.get("rate_bps") is not None
            ):
                raise OpportunityBundleInvalid()
            continue
        amount = _bundle_fraction(
            component.get("amount_usd"),
            "component amount",
            nonnegative=True,
        )
        rate = _bundle_fraction(
            component.get("rate_bps"),
            "component rate",
            nonnegative=True,
        )
        if amount != requested_notional * rate / 10_000:
            raise OpportunityBundleInvalid()
        key_text = "{}:{}".format(
            component.get("leg"),
            component.get("component_type"),
        )
        if (
            component.get("embedded_in_leg_quote") is not True
            and key_text not in reflected
        ):
            component_total += amount
    if component_total != strict_cost:
        raise OpportunityBundleInvalid()
    _validate_ratio(row, "gross_edge", gross_edge, gross_buy)
    _validate_ratio(row, "strict_net_edge", strict_net, gross_buy)


def _validate_research_economics(
    row: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
) -> None:
    if row.get("opportunity_class") != "research_estimate":
        return
    if row.get("research_net_edge_usd") is None:
        return
    capacity_value = row.get("maximum_proved_capacity_quantity")
    if capacity_value is not None:
        _bundle_fraction(
            capacity_value,
            "proved capacity",
            positive=True,
        )
    gross_buy = _bundle_fraction(
        row.get("gross_buy_cost_usd"),
        "gross buy cost",
        positive=True,
    )
    gross_sell = _bundle_fraction(
        row.get("gross_sell_proceeds_usd"),
        "gross sell proceeds",
        positive=True,
    )
    gross_edge = _bundle_fraction(row.get("gross_edge_usd"), "gross edge")
    strict_cost = _bundle_fraction(
        row.get("strict_nonembedded_cost_usd"),
        "strict cost",
        nonnegative=True,
    )
    bounded_cost = _bundle_fraction(
        row.get("research_bounded_cost_usd"),
        "bounded cost",
        nonnegative=True,
    )
    assumed_cost = _bundle_fraction(
        row.get("research_assumed_cost_usd"),
        "assumed cost",
        nonnegative=True,
    )
    strict_net = _bundle_fraction(
        row.get("strict_net_edge_usd"),
        "strict net edge",
    )
    research_net = _bundle_fraction(
        row.get("research_net_edge_usd"),
        "research net edge",
    )
    if (
        gross_edge != gross_sell - gross_buy
        or strict_net != gross_edge - strict_cost
        or research_net
        != gross_edge - strict_cost - bounded_cost - assumed_cost
    ):
        raise OpportunityBundleInvalid()

    reflected_values = row.get("reflected_or_embedded_component_keys")
    if (
        not isinstance(reflected_values, list)
        or any(not isinstance(value, str) for value in reflected_values)
        or reflected_values != sorted(set(reflected_values))
    ):
        raise OpportunityBundleInvalid()
    reflected = set(reflected_values)
    totals = {
        "strict": Fraction(0),
        "bounded_estimate": Fraction(0),
        "assumed": Fraction(0),
    }
    strict_complete = True
    scenario_complete = True
    requested_notional = _bundle_fraction(
        row.get("requested_notional_usd"),
        "route notional",
        positive=True,
    )
    for component in component_rows:
        status_value = component.get("value_status")
        strict_component = (
            status_value in _STRICT_COST_STATUSES
            and component.get("strict_eligible") is True
        )
        scenario_component = strict_component or status_value in {
            "bounded_estimate", "assumed"
        }
        strict_complete = strict_complete and strict_component
        scenario_complete = scenario_complete and scenario_component
        if status_value == "not_applicable":
            if (
                component.get("amount_usd") is not None
                or component.get("rate_bps") is not None
            ):
                raise OpportunityBundleInvalid()
            continue
        if status_value not in {
            "measured", "authenticated", "quoted",
            "bounded_estimate", "assumed",
        }:
            raise OpportunityBundleInvalid()
        amount = _bundle_fraction(
            component.get("amount_usd"),
            "component amount",
            nonnegative=True,
        )
        rate = _bundle_fraction(
            component.get("rate_bps"),
            "component rate",
            nonnegative=True,
        )
        if amount != requested_notional * rate / 10_000:
            raise OpportunityBundleInvalid()
        key_text = "{}:{}".format(
            component.get("leg"),
            component.get("component_type"),
        )
        if (
            component.get("embedded_in_leg_quote") is True
            or key_text in reflected
        ):
            continue
        if strict_component:
            totals["strict"] += amount
        elif status_value in {"bounded_estimate", "assumed"}:
            totals[str(status_value)] += amount
        else:
            raise OpportunityBundleInvalid()
    if (
        totals["strict"] != strict_cost
        or totals["bounded_estimate"] != bounded_cost
        or totals["assumed"] != assumed_cost
        or row.get("cost_completeness")
        != ("complete" if strict_complete else "incomplete")
        or row.get("scenario_cost_completeness")
        != ("complete" if scenario_complete else "incomplete")
        or not scenario_complete
    ):
        raise OpportunityBundleInvalid()
    _validate_ratio(row, "gross_edge", gross_edge, gross_buy)
    _validate_ratio(row, "strict_net_edge", strict_net, gross_buy)
    _validate_ratio(row, "research_net_edge", research_net, gross_buy)


def _validate_inventory(
    rows: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    costs: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Mapping[str, Any]]]:
    cohort_id = manifest.get("route_cohort_id")
    if not isinstance(cohort_id, str):
        raise OpportunityBundleInvalid()
    requested = manifest.get("requested_notionals_usd")
    if not isinstance(requested, list):
        raise OpportunityBundleInvalid()
    try:
        allowed_notionals = {
            _canonical_decimal_text(value, "manifest notional") for value in requested
        }
    except OpportunityQueryError:
        raise OpportunityBundleInvalid() from None

    opportunity_ids = set()
    route_ids = set()
    scenarios = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise OpportunityBundleInvalid()
        opportunity_id = row.get("opportunity_id")
        route_id = row.get("route_id")
        row_class = row.get("opportunity_class")
        route_mode = row.get("route_mode")
        primary_reason = row.get("primary_reason")
        reason_codes = row.get("reason_codes")
        if (
            not isinstance(opportunity_id, str)
            or not opportunity_id
            or not isinstance(route_id, str)
            or not route_id
            or row.get("cohort_id") != cohort_id
            or row_class not in _OPPORTUNITY_CLASSES
            or route_mode not in ROUTE_OPPORTUNITY_MODES
            or primary_reason not in ROUTE_OPPORTUNITY_REASON_CODES
            or not isinstance(reason_codes, list)
            or any(
                not isinstance(reason, str)
                or reason not in ROUTE_OPPORTUNITY_REASON_CODES
                for reason in reason_codes
            )
            or len(reason_codes) != len(set(reason_codes))
            or row.get("cost_completeness") not in {"complete", "incomplete"}
            or row.get("scenario_cost_completeness")
            not in {"complete", "incomplete"}
        ):
            raise OpportunityBundleInvalid()
        if row_class == "executable_candidate":
            if (
                primary_reason != "positive_strict_net_edge"
                or reason_codes != []
            ):
                raise OpportunityBundleInvalid()
        elif (
            primary_reason == "positive_strict_net_edge"
            or not reason_codes
            or reason_codes[0] != primary_reason
        ):
            raise OpportunityBundleInvalid()
        try:
            notional = _canonical_decimal_text(
                row.get("requested_notional_usd"), "route notional"
            )
        except OpportunityQueryError:
            raise OpportunityBundleInvalid() from None
        scenario = (route_id, notional)
        if (
            opportunity_id in opportunity_ids
            or scenario in scenarios
            or notional not in allowed_notionals
        ):
            raise OpportunityBundleInvalid()
        opportunity_ids.add(opportunity_id)
        route_ids.add(route_id)
        scenarios.add(scenario)

        strict_eligible = row.get("strict_eligible")
        strict_ready = row.get("strict_ready_for_publication")
        attestation = row.get("publication_attestation_sha256")
        if type(strict_eligible) is not bool or type(strict_ready) is not bool:
            raise OpportunityBundleInvalid()
        if row_class == "executable_candidate" and (
            not strict_eligible
            or not strict_ready
            or not isinstance(attestation, str)
            or _HEX_SHA256.fullmatch(attestation) is None
        ):
            raise OpportunityBundleInvalid()
        if row_class != "executable_candidate" and (
            strict_eligible or attestation is not None
        ):
            raise OpportunityBundleInvalid()

    counts = manifest.get("counts")
    classification = (
        counts.get("classification")
        if isinstance(counts, Mapping)
        else None
    )
    expected_classification = {
        "strict": sum(row.get("strict_eligible") is True for row in rows),
        "research": sum(
            row.get("opportunity_class") == "research_estimate"
            for row in rows
        ),
        "unavailable": sum(
            row.get("opportunity_class") == "unavailable"
            for row in rows
        ),
    }
    if (
        not isinstance(counts, Mapping)
        or type(counts.get("routes")) is not int
        or type(counts.get("opportunities")) is not int
        or counts.get("routes") != len(route_ids)
        or counts.get("opportunities") != len(rows)
        or not isinstance(classification, Mapping)
        or set(classification) != set(expected_classification)
        or any(type(classification.get(key)) is not int for key in classification)
        or dict(classification) != expected_classification
    ):
        raise OpportunityBundleInvalid()

    costs_by_opportunity: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for cost in costs:
        if not isinstance(cost, Mapping):
            raise OpportunityBundleInvalid()
        opportunity_id = cost.get("opportunity_id")
        status_value = cost.get("value_status")
        if (
            opportunity_id not in opportunity_ids
            or status_value not in _KNOWN_COST_STATUSES
        ):
            raise OpportunityBundleInvalid()
        costs_by_opportunity[str(opportunity_id)].append(cost)
    if set(costs_by_opportunity) != opportunity_ids:
        raise OpportunityBundleInvalid()

    rows_by_id = {str(row["opportunity_id"]): row for row in rows}
    for opportunity_id, component_rows in costs_by_opportunity.items():
        opportunity = rows_by_id[opportunity_id]
        _validate_component_inventory(opportunity, component_rows)
        if opportunity["opportunity_class"] == "research_estimate":
            _validate_research_economics(opportunity, component_rows)
            continue
        if opportunity["opportunity_class"] != "executable_candidate":
            continue
        if any(
            row.get("value_status") not in _STRICT_COST_STATUSES
            or row.get("strict_eligible") is False
            for row in component_rows
        ):
            raise OpportunityBundleInvalid()
        _validate_strict_economics(opportunity, component_rows)
    return costs_by_opportunity


def _component_projection(
    row: Mapping[str, Any],
    *,
    include_economics: bool,
    reflected_component_keys: set,
    now: datetime,
    route_reason: Optional[str],
) -> Dict[str, Any]:
    component_key = "{}:{}".format(
        row.get("leg"),
        row.get("component_type"),
    )
    embedded_in_leg_quote = row.get("embedded_in_leg_quote")
    component_is_current = _cost_component_deadline(row, now)[0]
    dynamically_stale = (
        row.get("value_status") in _DYNAMIC_COST_STATUSES
        and not component_is_current
    )
    return {
        "leg": row.get("leg"),
        "market_id": row.get("market_id"),
        "component_type": row.get("component_type"),
        "value_status": "stale" if dynamically_stale else row.get("value_status"),
        "strict_eligible": (
            False if dynamically_stale else row.get("strict_eligible")
        ),
        "embedded_in_leg_quote": embedded_in_leg_quote,
        "reflected_or_embedded": (
            embedded_in_leg_quote is True
            or component_key in reflected_component_keys
        ),
        "amount_usd": row.get("amount_usd") if include_economics else None,
        "rate_bps": row.get("rate_bps") if include_economics else None,
        "reason_code": (
            "cost_component_stale"
            if dynamically_stale
            else row.get("reason_code")
        ),
    }


def _cost_component_deadline(
    row: Mapping[str, Any],
    now: datetime,
) -> tuple[bool, Optional[datetime], bool]:
    """Return current state, next deadline, and exclusive-boundary mode."""
    status = row.get("value_status")
    if status == "not_applicable":
        return (
            row.get("observed_at") is None and row.get("valid_until") is None,
            None,
            False,
        )
    observed_value = row.get("observed_at")
    if observed_value is None:
        return status in _SCENARIO_COST_STATUSES, None, False
    try:
        observed_at = _timestamp(observed_value)
    except (TypeError, ValueError):
        raise OpportunityBundleInvalid() from None
    if observed_at > now:
        return False, None, False
    valid_until_value = row.get("valid_until")
    if valid_until_value is not None:
        try:
            valid_until = _timestamp(valid_until_value)
        except (TypeError, ValueError):
            raise OpportunityBundleInvalid() from None
        return now < valid_until, valid_until if now < valid_until else None, True
    deadline = observed_at + timedelta(seconds=float(MAX_ROUTE_AGE_SECONDS))
    return now <= deadline, deadline if now <= deadline else None, False


def _cost_components_are_current(
    rows: Sequence[Mapping[str, Any]],
    now: datetime,
) -> bool:
    return all(_cost_component_deadline(row, now)[0] for row in rows)


def _compact_row(
    row: Mapping[str, Any],
    *,
    component_rows: Sequence[Mapping[str, Any]],
    legs_by_market: Mapping[str, Mapping[str, Any]],
    route_volume: Mapping[str, Any],
    now: datetime,
) -> Dict[str, Any]:
    timing = _timing(row, now)
    stored_class = str(row["opportunity_class"])
    dynamic_reason = timing["reason"]
    if stored_class == "unavailable":
        dynamic_reason = str(row.get("primary_reason") or "route_unavailable")
    elif dynamic_reason is None and not _cost_components_are_current(
        component_rows, now
    ):
        dynamic_reason = "cost_component_stale"
    status = "unavailable" if dynamic_reason is not None else "available"
    if stored_class == "executable_candidate":
        net_usd = row.get("strict_net_edge_usd")
        net_bps = row.get("strict_net_edge_bps")
    elif stored_class == "research_estimate":
        net_usd = row.get("research_net_edge_usd")
        net_bps = row.get("research_net_edge_bps")
        if net_usd is None and dynamic_reason is None:
            status = "unavailable"
            dynamic_reason = str(row.get("primary_reason") or "route_unavailable")
    else:
        net_usd = None
        net_bps = None
    if status == "unavailable":
        net_usd = None
        net_bps = None
    include_economics = status == "available"
    reflected_component_keys = set(
        row.get("reflected_or_embedded_component_keys") or []
    )
    leg_venues = {
        "buy": _canonical_leg_venue(row.get("buy_market_id")),
        "sell": _canonical_leg_venue(row.get("sell_market_id")),
    }
    return {
        "route_id": row["route_id"],
        "opportunity_id": row["opportunity_id"],
        "token_symbol": row["token_symbol"],
        "buy_market_id": row["buy_market_id"],
        "sell_market_id": row["sell_market_id"],
        "leg_venues": leg_venues,
        "route_type": _route_type(row),
        "route_mode": row["route_mode"],
        "requested_notional_usd": row["requested_notional_usd"],
        "target_token_quantity": (
            row.get("target_token_quantity") if include_economics else None
        ),
        "opportunity_class": stored_class,
        "availability": {"status": status, "reason": dynamic_reason},
        "gross_edge_usd": (
            row.get("gross_edge_usd") if include_economics else None
        ),
        "gross_edge_bps": (
            row.get("gross_edge_bps") if include_economics else None
        ),
        "net_edge_usd": net_usd,
        "net_edge_bps": net_bps,
        "cost_breakdown": {
            "strict_nonembedded_usd": (
                row.get("strict_nonembedded_cost_usd")
                if include_economics else None
            ),
            "research_bounded_usd": (
                row.get("research_bounded_cost_usd")
                if include_economics else None
            ),
            "research_assumed_usd": (
                row.get("research_assumed_cost_usd")
                if include_economics else None
            ),
        },
        "cost_components": [
            _component_projection(
                item,
                include_economics=include_economics,
                reflected_component_keys=reflected_component_keys,
                now=now,
                route_reason=dynamic_reason,
            )
            for item in sorted(
                component_rows,
                key=lambda item: (
                    str(item.get("leg") or ""),
                    str(item.get("component_type") or ""),
                    str(item.get("market_id") or ""),
                ),
            )
        ],
        "cost_completeness": row.get("cost_completeness"),
        "scenario_cost_completeness": row.get(
            "scenario_cost_completeness"
        ),
        "leg_timestamps": {
            "buy": row["buy_state_observed_at"],
            "sell": row["sell_state_observed_at"],
        },
        "skew_seconds": timing["skew_seconds"],
        "route_age_seconds": timing["route_age_seconds"],
        "route_volume_usd": route_volume["route_volume_usd"],
        "route_volume_basis": route_volume["route_volume_basis"],
        "capacity_quantity": (
            row.get("maximum_proved_capacity_quantity")
            if include_economics else None
        ),
        "primary_reason": row.get("primary_reason"),
        "reason_codes": list(row.get("reason_codes") or []),
        "source_links": _source_links(row, legs_by_market),
    }


def _matches_filters(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    requested_class = filters["opportunity_class"]
    canonical_class = _CLASS_ALIASES.get(str(requested_class))
    if canonical_class is not None and row["opportunity_class"] != canonical_class:
        return False
    if filters["token"] is not None and row["token_symbol"] != filters["token"]:
        return False
    if (
        filters["venue"] is not None
        and filters["venue"] not in row["leg_venues"].values()
    ):
        return False
    if (
        filters["notional_usd"] is not None
        and _canonical_decimal_text(
            row["requested_notional_usd"], "route notional"
        )
        != filters["notional_usd"]
    ):
        return False
    if filters["route_type"] != "all" and row["route_type"] != filters["route_type"]:
        return False
    if (
        filters["availability"] != "all"
        and row["availability"]["status"] != filters["availability"]
    ):
        return False
    return True


def _sort_routes(
    rows: Sequence[Dict[str, Any]], sort_field: str, direction: str
) -> List[Dict[str, Any]]:
    row_field = "route_volume_usd" if sort_field == "volume" else sort_field
    present = [row for row in rows if row.get(row_field) is not None]
    missing = [row for row in rows if row.get(row_field) is None]
    present.sort(key=lambda row: (
        str(row["route_id"]), str(row["opportunity_id"])
    ))
    if sort_field in _NUMERIC_SORT_FIELDS:
        try:
            present.sort(
                key=lambda row: Decimal(str(row[row_field])),
                reverse=direction == "desc",
            )
        except (InvalidOperation, TypeError, ValueError):
            raise OpportunityBundleInvalid() from None
    else:
        present.sort(
            key=lambda row: str(row[row_field]),
            reverse=direction == "desc",
        )
    missing.sort(key=lambda row: (
        str(row["route_id"]), str(row["opportunity_id"])
    ))
    return present + missing


def _next_freshness_deadline_at(
    rows: Sequence[Mapping[str, Any]],
    current: datetime,
    component_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[Optional[str], Optional[bool]]:
    """Expose the earliest deadline across the unfiltered route inventory.

    Availability is a response-time projection.  The deadline must therefore
    cover rows removed by an availability filter as well as rows returned to
    the client; otherwise a route can cross the age SLA while the response is
    being encoded and be omitted from both sides of the filter boundary.
    """

    deadlines: list[tuple[datetime, bool]] = []
    for row in rows:
        try:
            freshness = route_opportunity_freshness(
                row.get("leg_timestamps", {}).get("buy"),
                row.get("leg_timestamps", {}).get("sell"),
                now=current,
            )
        except (AttributeError, TypeError, ValueError):
            raise OpportunityBundleInvalid() from None
        if freshness.get("status") != "current":
            continue
        observed_at = _timestamp(freshness.get("observed_at"))
        deadlines.append((
            observed_at + timedelta(seconds=float(MAX_ROUTE_AGE_SECONDS)),
            False,
        ))
        opportunity_id = str(row.get("opportunity_id") or "")
        for component in component_rows.get(opportunity_id, ()):
            is_current, deadline, exclusive = _cost_component_deadline(
                component, current
            )
            if is_current and deadline is not None:
                deadlines.append((deadline, exclusive))
    if not deadlines:
        return None, None
    earliest = min(deadline for deadline, _ in deadlines)
    exclusive = any(
        deadline == earliest and boundary_is_exclusive
        for deadline, boundary_is_exclusive in deadlines
    )
    return earliest.isoformat(), exclusive


def build_opportunity_payload(
    rows: Iterable[Mapping[str, Any]],
    *,
    manifest: Mapping[str, Any],
    legs: Optional[Iterable[Mapping[str, Any]]] = None,
    cost_components: Optional[Iterable[Mapping[str, Any]]] = None,
    route_candidates: Optional[Iterable[Mapping[str, Any]]] = None,
    token: Optional[str] = None,
    venue: Optional[str] = None,
    notional_usd: Any = None,
    opportunity_class: Optional[str] = None,
    route_type: Optional[str] = None,
    availability: Optional[str] = None,
    sort: Optional[str] = None,
    direction: Optional[str] = None,
    now: Optional[datetime] = None,
    manifest_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    """Build one compact, filtered view from an already validated generation."""

    filters = _normalize_filters(
        token=token,
        venue=venue,
        notional_usd=notional_usd,
        opportunity_class=opportunity_class,
        route_type=route_type,
        availability=availability,
        sort=sort,
        direction=direction,
    )
    row_inventory = list(rows)
    cost_inventory = list(cost_components or [])
    component_rows = _validate_inventory(row_inventory, manifest, cost_inventory)
    allowed_notionals = {
        _canonical_decimal_text(item, "manifest notional")
        for item in manifest.get("requested_notionals_usd", [])
    }
    if (
        filters["notional_usd"] is not None
        and filters["notional_usd"] not in allowed_notionals
    ):
        raise OpportunityQueryError(
            "notional must be one of the collected opportunity notionals"
        )
    legs_by_market: Dict[str, Mapping[str, Any]] = {}
    for leg in list(legs or []):
        if not isinstance(leg, Mapping) or not isinstance(leg.get("market_id"), str):
            raise OpportunityBundleInvalid()
        market_id = str(leg["market_id"])
        if market_id in legs_by_market:
            raise OpportunityBundleInvalid()
        legs_by_market[market_id] = leg
    route_ids = {str(row["route_id"]) for row in row_inventory}
    route_volumes: Dict[str, Dict[str, Any]] = {
        route_id: {
            "route_volume_usd": None,
            "route_volume_basis": ROUTE_VOLUME_BASIS,
        }
        for route_id in route_ids
    }
    if route_candidates is not None:
        supplied: Dict[str, Dict[str, Any]] = {}
        for candidate in route_candidates:
            if not isinstance(candidate, Mapping):
                raise OpportunityBundleInvalid()
            route_id = candidate.get("route_id")
            if not isinstance(route_id, str) or route_id in supplied:
                raise OpportunityBundleInvalid()
            if candidate.get("route_volume_basis") != ROUTE_VOLUME_BASIS:
                raise OpportunityBundleInvalid()
            volume = candidate.get("route_volume_usd")
            if volume is not None:
                volume = _canonical_decimal_text(volume, "route volume")
                try:
                    if Decimal(volume) <= 0:
                        raise OpportunityBundleInvalid()
                except InvalidOperation:
                    raise OpportunityBundleInvalid() from None
            supplied[route_id] = {
                "route_volume_usd": volume,
                "route_volume_basis": ROUTE_VOLUME_BASIS,
            }
        if set(supplied) != route_ids:
            raise OpportunityBundleInvalid()
        route_volumes = supplied
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    projected = [
        _compact_row(
            row,
            component_rows=component_rows[str(row["opportunity_id"])],
            legs_by_market=legs_by_market,
            route_volume=route_volumes[str(row["route_id"])],
            now=current,
        )
        for row in row_inventory
    ]
    filtered = [row for row in projected if _matches_filters(row, filters)]
    filtered = _sort_routes(
        filtered,
        str(filters["sort"]),
        str(filters["direction"]),
    )
    status_counts = Counter(
        row["availability"]["status"] for row in projected
    )
    class_counts = Counter(row["opportunity_class"] for row in projected)
    (
        next_freshness_deadline_at,
        next_freshness_deadline_exclusive,
    ) = _next_freshness_deadline_at(
        projected,
        current,
        component_rows,
    )
    available_notionals = [
        _canonical_decimal_text(item, "manifest notional")
        for item in manifest.get("requested_notionals_usd", [])
    ]
    available_venues = sorted({
        venue
        for row in projected
        for venue in row["leg_venues"].values()
    })
    return {
        "availability": {"status": "available", "reason": None},
        "metadata": {
            "contract_version": OPPORTUNITY_SUMMARY_CONTRACT,
            "route_cohort_id": manifest.get("route_cohort_id"),
            "manifest_sha256": manifest_sha256,
            "publication_status": "available",
            "checked_at": current.isoformat(),
            "next_freshness_deadline_at": next_freshness_deadline_at,
            "next_freshness_deadline_exclusive": (
                next_freshness_deadline_exclusive
            ),
            "max_route_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
            "max_route_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
            "available_notionals_usd": available_notionals,
            "available_venues": available_venues,
            "coverage": {
                "route_count": len({row["route_id"] for row in projected}),
                "scenario_count": len(projected),
                "returned_count": len(filtered),
                "class_counts": dict(sorted(class_counts.items())),
                "availability_counts": dict(sorted(status_counts.items())),
            },
        },
        "filters": filters,
        "routes": filtered,
    }


def build_unavailable_opportunity_payload(
    *,
    reason: str = COMPLETE_POINTER_ABSENT,
    token: Optional[str] = None,
    venue: Optional[str] = None,
    notional_usd: Any = None,
    opportunity_class: Optional[str] = None,
    route_type: Optional[str] = None,
    availability: Optional[str] = None,
    sort: Optional[str] = None,
    direction: Optional[str] = None,
) -> Dict[str, Any]:
    """Return the only public payload used when no complete pointer exists."""

    if reason != COMPLETE_POINTER_ABSENT:
        raise OpportunityBundleInvalid()
    filters = _normalize_filters(
        token=token,
        venue=venue,
        notional_usd=notional_usd,
        opportunity_class=opportunity_class,
        route_type=route_type,
        availability=availability,
        sort=sort,
        direction=direction,
    )
    return {
        "availability": {"status": "unavailable", "reason": reason},
        "metadata": {
            "contract_version": OPPORTUNITY_SUMMARY_CONTRACT,
            "route_cohort_id": None,
            "manifest_sha256": None,
            "publication_status": "missing",
            "checked_at": None,
            "next_freshness_deadline_at": None,
            "next_freshness_deadline_exclusive": None,
            "max_route_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
            "max_route_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
            "available_notionals_usd": [],
            "available_venues": [],
            "coverage": {
                "route_count": 0,
                "scenario_count": 0,
                "returned_count": 0,
                "class_counts": {},
                "availability_counts": {},
            },
        },
        "filters": filters,
        "routes": [],
    }


def opportunity_publication_health(
    routes_root: Optional[Path] = None,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Describe optional route publication health without exposing internals."""

    try:
        loaded = load_latest_opportunities(routes_root)
    except OpportunityBundleUnavailable as error:
        return {
            "status": "missing",
            "reason": error.reason,
            "route_cohort_id": None,
            "manifest_sha256": None,
            "observed_at": None,
            "age_seconds": None,
            "max_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
            "max_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
        }
    except OpportunityBundleInvalid as error:
        return {
            "status": "invalid",
            "reason": error.reason,
            "route_cohort_id": None,
            "manifest_sha256": None,
            "observed_at": None,
            "age_seconds": None,
            "max_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
            "max_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
        }
    try:
        rows = loaded["opportunities"]
        if not isinstance(rows, list) or not rows:
            raise OpportunityBundleInvalid()
        component_rows = _validate_inventory(
            rows,
            loaded["manifest"],
            loaded["cost_components"],
        )
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        evaluations = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise OpportunityBundleInvalid()
            evaluation = route_opportunity_freshness(
                row.get("buy_state_observed_at"),
                row.get("sell_state_observed_at"),
                now=current,
            )
            if evaluation.get("status") not in {
                "current", "stale", "unavailable"
            }:
                raise OpportunityBundleInvalid()
            if (
                evaluation.get("status") == "current"
                and not _cost_components_are_current(
                    component_rows[str(row["opportunity_id"])],
                    current,
                )
            ):
                evaluation = {
                    **evaluation,
                    "status": "stale",
                    "reason": "cost_component_stale",
                }
            evaluations.append(evaluation)
        priority = {"current": 0, "stale": 1, "unavailable": 2}
        freshness = max(
            evaluations,
            key=lambda item: (
                priority[str(item["status"])],
                float(item["age_seconds"] or 0),
                float(item["skew_seconds"] or 0),
            ),
        )
    except (KeyError, TypeError, ValueError, OpportunityBundleInvalid):
        return {
            "status": "invalid",
            "reason": OPPORTUNITY_BUNDLE_VALIDATION_FAILED,
            "route_cohort_id": None,
            "manifest_sha256": None,
            "observed_at": None,
            "age_seconds": None,
            "max_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
            "max_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
        }
    return {
        "status": freshness["status"],
        "reason": freshness["reason"],
        "route_cohort_id": loaded["manifest"].get("route_cohort_id"),
        "manifest_sha256": loaded.get("manifest_sha256"),
        "observed_at": freshness["observed_at"],
        "age_seconds": freshness["age_seconds"],
        "max_age_seconds": int(MAX_ROUTE_AGE_SECONDS),
        "max_skew_seconds": int(MAX_ROUTE_SKEW_SECONDS),
        "scenario_count": len(rows),
    }
