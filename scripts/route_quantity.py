"""Exact common-quantity types and CEX level-walk quotes.

This module is deliberately separate from the fixed-USD execution-cost v1
contract. Strict route quantities use integer lattices and exact rational
arithmetic; they never inherit the process Decimal context or infer execution
from depth bands.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
from math import gcd
import re
from typing import Any, Iterable, Optional, Tuple

try:
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


ROUTE_QUANTITY_CONTRACT_VERSION = "1"
MAX_CEX_QUANTITY_STATE_AGE_SECONDS = Decimal("60")

_ASSET = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_CEX_MARKET = re.compile(
    r"cex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})/"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)
_DEX_MARKET = re.compile(
    r"dex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([a-z0-9][a-z0-9._-]{0,127}):"
    r"(0x[0-9a-f]{40}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)
_FEE_BASES = frozenset(
    {
        "received_base",
        "spent_quote",
        "received_quote",
        "sold_base",
        "third_asset_quote_value",
        "embedded",
    }
)
_ROUNDING_MODES = frozenset({"ceiling", "floor", "exact", "not_applicable"})
_UNAVAILABLE_REASONS = frozenset(
    {
        "base_fee_target_not_exactly_representable",
        "book_state_not_current",
        "book_state_observed_at_unavailable",
        "embedded_fee_not_supported_for_cex",
        "fee_asset_semantics_mismatch",
        "fee_semantics_binding_mismatch",
        "fee_semantics_not_current",
        "market_rules_binding_mismatch",
        "market_rules_not_current",
        "minimum_base_quantity_not_met",
        "minimum_notional_not_met",
        "source_quote_asset_mismatch",
        "target_asset_mismatch",
        "target_base_unit_misaligned",
        "target_lot_misaligned",
        "third_asset_conversion_unavailable",
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


def _asset(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if _ASSET.fullmatch(text) is None:
        raise ValueError("{} must be a canonical asset".format(field))
    return text


def _hash(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if _SHA256.fullmatch(text) is None:
        raise ValueError("{} must be a lowercase SHA-256".format(field))
    return text


def _timestamp(value: Any, field: str) -> Tuple[str, Fraction]:
    text = _required_text(value, field)
    try:
        epoch = exact_rfc3339_epoch_seconds(text)
    except ValueError as error:
        raise ValueError("{} must be RFC 3339 text".format(field)) from error
    return text, Fraction(epoch)


def _window(observed_at: Any, valid_until: Any) -> Tuple[str, str]:
    observed, observed_epoch = _timestamp(observed_at, "observed_at")
    valid, valid_epoch = _timestamp(valid_until, "valid_until")
    if valid_epoch <= observed_epoch:
        raise ValueError("valid_until must be after observed_at")
    return observed, valid


def _decimal(
    value: Any,
    field: str,
    *,
    positive: bool = False,
) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(
        value,
        (Decimal, int, str),
    ):
        raise ValueError("{} must be an exact Decimal".format(field))
    if isinstance(value, str) and (not value or value != value.strip()):
        raise ValueError("{} must be an exact Decimal".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("{} must be an exact Decimal".format(field)) from error
    if not number.is_finite() or number < 0 or (positive and number <= 0):
        raise ValueError("{} must be an exact finite Decimal".format(field))
    return number


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError("{} must be an integer".format(field))
    return value


def _fraction(value: Decimal) -> Fraction:
    parts = value.as_tuple()
    coefficient = 0
    for digit in parts.digits:
        coefficient = coefficient * 10 + digit
    if parts.sign:
        coefficient = -coefficient
    exponent = int(parts.exponent)
    if exponent >= 0:
        return Fraction(coefficient * (10**exponent), 1)
    return Fraction(coefficient, 10 ** (-exponent))


def _fraction_decimal(value: Fraction, field: str) -> Decimal:
    numerator = value.numerator
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
        raise ValueError("{} is not a finite Decimal".format(field))
    scale = max(twos, fives)
    coefficient = numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    if coefficient == 0:
        return Decimal(0)
    sign = 1 if coefficient < 0 else 0
    digits = tuple(int(character) for character in str(abs(coefficient)))
    return Decimal((sign, digits, -scale))


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _record_binding(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _ceil_fraction(value: Fraction) -> int:
    return -(-value.numerator // value.denominator)


def _floor_fraction(value: Fraction) -> int:
    return value.numerator // value.denominator


def _lcm(left: int, right: int) -> int:
    if left <= 0 or right <= 0:
        raise ValueError("quantity lattice increments must be positive")
    return abs(left // gcd(left, right) * right)


def _round_fraction(
    value: Fraction,
    increment: Decimal,
    mode: str,
) -> Fraction:
    step = _fraction(increment)
    quotient = value / step
    floor_steps = quotient.numerator // quotient.denominator
    has_remainder = quotient.denominator != 1
    if mode == "ceiling":
        steps = floor_steps + (1 if has_remainder else 0)
    elif mode == "floor":
        steps = floor_steps
    elif mode == "exact":
        if has_remainder:
            raise ValueError("value is not aligned to its exact increment")
        steps = floor_steps
    elif mode == "not_applicable":
        if value != 0:
            raise ValueError("nonzero value cannot use not_applicable rounding")
        steps = 0
    else:  # pragma: no cover - constructor validation owns this boundary
        raise ValueError("rounding mode is unsupported")
    return steps * step


def _raw_exact(value: Decimal, decimals: int, field: str) -> int:
    raw = _fraction(value) * (10**decimals)
    if raw.denominator != 1:
        raise ValueError("{} is not aligned to base units".format(field))
    return raw.numerator


def _fraction_raw_exact(value: Fraction, decimals: int, field: str) -> int:
    raw = value * (10**decimals)
    if raw.denominator != 1:
        raise ValueError("{} is not aligned to base units".format(field))
    return raw.numerator


def _parse_market(
    market_id: Any,
    *,
    base_asset: str,
    quote_asset: str,
) -> str:
    text = _required_text(market_id, "market_id")
    cex_match = _CEX_MARKET.fullmatch(text)
    if cex_match is not None:
        _venue, base, quote = cex_match.groups()
        if base != base_asset or quote != quote_asset or base == quote:
            raise ValueError("market identity does not match its assets")
        return text
    dex_match = _DEX_MARKET.fullmatch(text)
    if dex_match is not None:
        _chain, _dex, _pool, token = dex_match.groups()
        if token != base_asset or base_asset == quote_asset:
            raise ValueError("market identity does not match its assets")
        return text
    raise ValueError("market_id must be canonical")


@dataclass(frozen=True)
class MarketRules:
    market_id: str
    base_asset: str
    quote_asset: str
    base_unit_decimals: int
    quote_unit_decimals: int
    base_increment: Decimal
    quote_increment: Decimal
    min_base_quantity: Decimal
    min_quote_notional: Decimal
    observed_at: str
    valid_until: str
    source_record_sha256: str
    record_binding_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        base = _asset(self.base_asset, "base_asset")
        quote = _asset(self.quote_asset, "quote_asset")
        market = _parse_market(
            self.market_id,
            base_asset=base,
            quote_asset=quote,
        )
        base_decimals = _integer(
            self.base_unit_decimals,
            "base_unit_decimals",
        )
        quote_decimals = _integer(
            self.quote_unit_decimals,
            "quote_unit_decimals",
        )
        if base_decimals > 255 or quote_decimals > 255:
            raise ValueError("unit decimals must be within [0, 255]")
        base_increment = _decimal(
            self.base_increment,
            "base_increment",
            positive=True,
        )
        quote_increment = _decimal(
            self.quote_increment,
            "quote_increment",
            positive=True,
        )
        minimum_base = _decimal(self.min_base_quantity, "min_base_quantity")
        minimum_quote = _decimal(
            self.min_quote_notional,
            "min_quote_notional",
        )
        _raw_exact(base_increment, base_decimals, "base_increment")
        _raw_exact(quote_increment, quote_decimals, "quote_increment")
        _raw_exact(minimum_base, base_decimals, "min_base_quantity")
        _raw_exact(minimum_quote, quote_decimals, "min_quote_notional")
        observed, valid = _window(self.observed_at, self.valid_until)
        source_hash = _hash(self.source_record_sha256, "source_record_sha256")
        object.__setattr__(self, "market_id", market)
        object.__setattr__(self, "base_asset", base)
        object.__setattr__(self, "quote_asset", quote)
        object.__setattr__(self, "base_unit_decimals", base_decimals)
        object.__setattr__(self, "quote_unit_decimals", quote_decimals)
        object.__setattr__(self, "base_increment", base_increment)
        object.__setattr__(self, "quote_increment", quote_increment)
        object.__setattr__(self, "min_base_quantity", minimum_base)
        object.__setattr__(self, "min_quote_notional", minimum_quote)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "source_record_sha256", source_hash)
        object.__setattr__(
            self,
            "record_binding_sha256",
            market_rules_record_binding_sha256(self),
        )

    @property
    def base_increment_raw(self) -> int:
        return _raw_exact(
            self.base_increment,
            self.base_unit_decimals,
            "base_increment",
        )


@dataclass(frozen=True)
class FeeSemantics:
    rate_bps: Decimal
    fee_asset: str
    charge_basis: str
    fee_increment: Decimal
    rounding_mode: str
    third_asset_quote_price: Optional[Decimal]
    observed_at: str
    valid_until: str
    source_record_sha256: str
    conversion_source_record_sha256: Optional[str]
    record_binding_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        rate = _decimal(self.rate_bps, "rate_bps")
        if rate >= Decimal(10_000):
            raise ValueError("rate_bps must be below 10000")
        fee_asset = _asset(self.fee_asset, "fee_asset")
        basis = _required_text(self.charge_basis, "charge_basis")
        if basis not in _FEE_BASES:
            raise ValueError("charge_basis is unsupported")
        increment = _decimal(
            self.fee_increment,
            "fee_increment",
            positive=True,
        )
        rounding = _required_text(self.rounding_mode, "rounding_mode")
        if rounding not in _ROUNDING_MODES:
            raise ValueError("rounding_mode is unsupported")
        third_price = None
        if self.third_asset_quote_price is not None:
            third_price = _decimal(
                self.third_asset_quote_price,
                "third_asset_quote_price",
                positive=True,
            )
        conversion_hash = self.conversion_source_record_sha256
        if conversion_hash is not None:
            conversion_hash = _hash(
                conversion_hash,
                "conversion_source_record_sha256",
            )
        if (third_price is None) != (conversion_hash is None):
            raise ValueError(
                "third-asset conversion price and lineage must be supplied together"
            )
        observed, valid = _window(self.observed_at, self.valid_until)
        source_hash = _hash(self.source_record_sha256, "source_record_sha256")
        object.__setattr__(self, "rate_bps", rate)
        object.__setattr__(self, "fee_asset", fee_asset)
        object.__setattr__(self, "charge_basis", basis)
        object.__setattr__(self, "fee_increment", increment)
        object.__setattr__(self, "rounding_mode", rounding)
        object.__setattr__(self, "third_asset_quote_price", third_price)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "source_record_sha256", source_hash)
        object.__setattr__(
            self,
            "conversion_source_record_sha256",
            conversion_hash,
        )
        object.__setattr__(
            self,
            "record_binding_sha256",
            fee_semantics_record_binding_sha256(self),
        )


def market_rules_record_binding_sha256(rules: MarketRules) -> str:
    """Hash every normalized MarketRules claim, not merely its source hash."""
    if not isinstance(rules, MarketRules):
        raise ValueError("rules must be MarketRules")
    return _record_binding(
        {
            "contract": "market_rules/v1",
            "market_id": rules.market_id,
            "base_asset": rules.base_asset,
            "quote_asset": rules.quote_asset,
            "base_unit_decimals": rules.base_unit_decimals,
            "quote_unit_decimals": rules.quote_unit_decimals,
            "base_increment": _decimal_text(rules.base_increment),
            "quote_increment": _decimal_text(rules.quote_increment),
            "min_base_quantity": _decimal_text(rules.min_base_quantity),
            "min_quote_notional": _decimal_text(rules.min_quote_notional),
            "observed_at": rules.observed_at,
            "valid_until": rules.valid_until,
            "source_record_sha256": rules.source_record_sha256,
        }
    )


def fee_semantics_record_binding_sha256(fee: FeeSemantics) -> str:
    """Hash every normalized fee and third-asset conversion claim."""
    if not isinstance(fee, FeeSemantics):
        raise ValueError("fee must be FeeSemantics")
    return _record_binding(
        {
            "contract": "fee_semantics/v1",
            "rate_bps": _decimal_text(fee.rate_bps),
            "fee_asset": fee.fee_asset,
            "charge_basis": fee.charge_basis,
            "fee_increment": _decimal_text(fee.fee_increment),
            "rounding_mode": fee.rounding_mode,
            "third_asset_quote_price": (
                _decimal_text(fee.third_asset_quote_price)
                if fee.third_asset_quote_price is not None
                else None
            ),
            "observed_at": fee.observed_at,
            "valid_until": fee.valid_until,
            "source_record_sha256": fee.source_record_sha256,
            "conversion_source_record_sha256": (
                fee.conversion_source_record_sha256
            ),
        }
    )


@dataclass(frozen=True)
class CommonTarget:
    asset: str
    unit_decimals: int
    raw_quantity: int
    lattice_raw: int

    def __post_init__(self) -> None:
        asset = _asset(self.asset, "asset")
        decimals = _integer(self.unit_decimals, "unit_decimals")
        if decimals > 255:
            raise ValueError("unit_decimals must be within [0, 255]")
        raw = _integer(self.raw_quantity, "raw_quantity", minimum=1)
        lattice = _integer(self.lattice_raw, "lattice_raw", minimum=1)
        if raw % lattice:
            raise ValueError("raw_quantity must align to the common lattice")
        object.__setattr__(self, "asset", asset)
        object.__setattr__(self, "unit_decimals", decimals)
        object.__setattr__(self, "raw_quantity", raw)
        object.__setattr__(self, "lattice_raw", lattice)

    @property
    def quantity(self) -> Decimal:
        return _fraction_decimal(
            Fraction(self.raw_quantity, 10**self.unit_decimals),
            "common target",
        )

    @property
    def canonical_text(self) -> str:
        return _decimal_text(self.quantity)


@dataclass(frozen=True)
class QuantityQuote:
    contract_version: str
    market_id: str
    direction: str
    status: str
    reason_code: Optional[str]
    complete: bool
    calculation_complete: bool
    strict_eligible: bool
    target_base_asset: str
    target_base_raw: int
    target_base_unit_decimals: int
    target_lattice_raw: int
    target_base_quantity: Decimal
    market_base_unit_decimals: int
    order_base_quantity: Optional[Decimal]
    order_base_raw: Optional[int]
    filled_gross_base_quantity: Optional[Decimal]
    filled_gross_base_raw: Optional[int]
    gross_base_received_quantity: Optional[Decimal]
    gross_base_received_raw: Optional[int]
    net_base_received_quantity: Optional[Decimal]
    net_base_received_raw: Optional[int]
    base_debit_quantity: Optional[Decimal]
    base_debit_raw: Optional[int]
    gross_quote_quantity: Optional[Decimal]
    net_quote_quantity: Optional[Decimal]
    quote_debit_asset: Optional[str]
    quote_debit_quantity: Optional[Decimal]
    quote_received_asset: Optional[str]
    quote_received_quantity: Optional[Decimal]
    fee_debit_asset: Optional[str]
    fee_debit_quantity: Optional[Decimal]
    levels_or_ticks_consumed: int
    ending_price: Optional[Decimal]
    vwap_quote_per_base: Optional[Decimal]
    vwap_quote_numerator: Optional[int]
    vwap_quote_denominator: Optional[int]
    state_id: str
    snapshot_id: str
    state_observed_at: str
    cohort_now: str
    raw_response_sha256: str
    levels_binding_sha256: str
    market_rules_sha256: str
    fee_source_sha256: str
    market_rules_binding_sha256: str
    fee_binding_sha256: str

    def __post_init__(self) -> None:
        validate_quantity_quote(self)


def _validate_quote_decimal(
    value: Any,
    field_name: str,
    *,
    positive: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal):
        raise ValueError("{} must be Decimal".format(field_name))
    return _decimal(value, field_name, positive=positive)


def _validate_raw_decimal_pair(
    *,
    value: Optional[Decimal],
    raw: Optional[int],
    decimals: int,
    field_name: str,
) -> None:
    if (value is None) != (raw is None):
        raise ValueError(
            "{} Decimal and raw fields must be present together".format(
                field_name
            )
        )
    if value is None:
        return
    number = _validate_quote_decimal(value, field_name)
    integer = _integer(raw, field_name + "_raw")
    if _fraction(number) * (10**decimals) != integer:
        raise ValueError(
            "{} raw value does not match its Decimal".format(field_name)
        )


def validate_quantity_quote(quote: QuantityQuote) -> QuantityQuote:
    """Validate the closed v1 calculation-quote output contract."""
    if not isinstance(quote, QuantityQuote):
        raise ValueError("quote must be QuantityQuote")
    if quote.contract_version != ROUTE_QUANTITY_CONTRACT_VERSION:
        raise ValueError("QuantityQuote contract_version must be v1")
    if quote.strict_eligible is not False:
        raise ValueError(
            "QuantityQuote v1 strict_eligible must remain false"
        )
    if type(quote.complete) is not bool or type(quote.calculation_complete) is not bool:
        raise ValueError("QuantityQuote completion flags must be boolean")
    if quote.direction not in {"buy", "sell"}:
        raise ValueError("QuantityQuote direction must be buy or sell")
    market_id = _required_text(quote.market_id, "market_id")
    cex_match = _CEX_MARKET.fullmatch(market_id)
    dex_match = _DEX_MARKET.fullmatch(market_id)
    if cex_match is None and dex_match is None:
        raise ValueError("QuantityQuote market_id must be canonical")
    target_asset = _asset(quote.target_base_asset, "target_base_asset")
    target_mismatch_is_explicit_unavailable = (
        quote.status == "unavailable"
        and quote.reason_code == "target_asset_mismatch"
    )
    if cex_match is not None:
        _venue, market_base, market_quote = cex_match.groups()
        if market_base == market_quote:
            raise ValueError("QuantityQuote CEX base and quote assets must differ")
        if (
            target_asset != market_base
            and not target_mismatch_is_explicit_unavailable
        ):
            raise ValueError("QuantityQuote target asset does not match Market")
    else:
        _chain, _dex, _pool, market_base = dex_match.groups()
        market_quote = None
        if (
            target_asset != market_base
            and not target_mismatch_is_explicit_unavailable
        ):
            raise ValueError("QuantityQuote target asset does not match Market")
    if target_asset == market_base and target_mismatch_is_explicit_unavailable:
        raise ValueError("target_asset_mismatch reason does not match target")

    status = _required_text(quote.status, "status")
    if status not in {"unavailable", "partial", "calculation_complete"}:
        raise ValueError("QuantityQuote status is unsupported")
    reason = _required_text(quote.reason_code, "reason_code")
    if status == "calculation_complete":
        if not quote.complete or not quote.calculation_complete:
            raise ValueError("calculation_complete flags are inconsistent")
        if reason != "authenticated_upstream_unavailable":
            raise ValueError("calculation_complete reason is inconsistent")
    elif status == "partial":
        if quote.complete or quote.calculation_complete:
            raise ValueError("partial completion flags are inconsistent")
        if reason not in {
            "source_level_limit",
            "full_book_insufficient_liquidity",
        }:
            raise ValueError("partial reason is inconsistent")
    else:
        if quote.complete or quote.calculation_complete:
            raise ValueError("unavailable completion flags are inconsistent")
        if reason not in _UNAVAILABLE_REASONS:
            raise ValueError("unavailable reason is inconsistent")

    target_decimals = _integer(
        quote.target_base_unit_decimals,
        "target_base_unit_decimals",
    )
    market_decimals = _integer(
        quote.market_base_unit_decimals,
        "market_base_unit_decimals",
    )
    if target_decimals > 255 or market_decimals > 255:
        raise ValueError("QuantityQuote unit decimals exceed 255")
    target_raw = _integer(quote.target_base_raw, "target_base_raw", minimum=1)
    lattice_raw = _integer(
        quote.target_lattice_raw,
        "target_lattice_raw",
        minimum=1,
    )
    if target_raw % lattice_raw:
        raise ValueError("target_base_raw does not align to target lattice")
    target_quantity = _validate_quote_decimal(
        quote.target_base_quantity,
        "target_base_quantity",
        positive=True,
    )
    if _fraction(target_quantity) * (10**target_decimals) != target_raw:
        raise ValueError("target raw value does not match its Decimal")

    raw_pairs = (
        (quote.order_base_quantity, quote.order_base_raw, "order_base_quantity"),
        (
            quote.filled_gross_base_quantity,
            quote.filled_gross_base_raw,
            "filled_gross_base_quantity",
        ),
        (
            quote.gross_base_received_quantity,
            quote.gross_base_received_raw,
            "gross_base_received_quantity",
        ),
        (
            quote.net_base_received_quantity,
            quote.net_base_received_raw,
            "net_base_received_quantity",
        ),
        (quote.base_debit_quantity, quote.base_debit_raw, "base_debit_quantity"),
    )
    for decimal_value, raw_value, field_name in raw_pairs:
        _validate_raw_decimal_pair(
            value=decimal_value,
            raw=raw_value,
            decimals=market_decimals,
            field_name=field_name,
        )

    optional_decimals = (
        (quote.gross_quote_quantity, "gross_quote_quantity"),
        (quote.net_quote_quantity, "net_quote_quantity"),
        (quote.quote_debit_quantity, "quote_debit_quantity"),
        (quote.quote_received_quantity, "quote_received_quantity"),
        (quote.fee_debit_quantity, "fee_debit_quantity"),
        (quote.ending_price, "ending_price"),
        (quote.vwap_quote_per_base, "vwap_quote_per_base"),
    )
    for decimal_value, field_name in optional_decimals:
        if decimal_value is not None:
            _validate_quote_decimal(
                decimal_value,
                field_name,
                positive=(field_name == "ending_price"),
            )

    consumed = _integer(
        quote.levels_or_ticks_consumed,
        "levels_or_ticks_consumed",
    )
    required_calculation_fields = (
        quote.order_base_quantity,
        quote.filled_gross_base_quantity,
        quote.gross_quote_quantity,
        quote.fee_debit_quantity,
        quote.ending_price,
    )
    if status == "unavailable":
        unavailable_calculation_fields = (
            quote.order_base_quantity,
            quote.order_base_raw,
            quote.filled_gross_base_quantity,
            quote.filled_gross_base_raw,
            quote.gross_base_received_quantity,
            quote.gross_base_received_raw,
            quote.net_base_received_quantity,
            quote.net_base_received_raw,
            quote.base_debit_quantity,
            quote.base_debit_raw,
            quote.gross_quote_quantity,
            quote.net_quote_quantity,
            quote.quote_debit_asset,
            quote.quote_debit_quantity,
            quote.quote_received_asset,
            quote.quote_received_quantity,
            quote.fee_debit_asset,
            quote.fee_debit_quantity,
            quote.ending_price,
            quote.vwap_quote_per_base,
            quote.vwap_quote_numerator,
            quote.vwap_quote_denominator,
        )
        if (
            any(value is not None for value in unavailable_calculation_fields)
            or consumed != 0
        ):
            raise ValueError("unavailable quote cannot retain calculation values")
    else:
        if (
            any(value is None for value in required_calculation_fields)
            or consumed < 1
        ):
            raise ValueError("calculated quote is missing required values")
        order_quantity = _fraction(quote.order_base_quantity)
        filled_quantity = _fraction(quote.filled_gross_base_quantity)
        gross_quote_quantity = _fraction(quote.gross_quote_quantity)
        fee_quantity = _fraction(quote.fee_debit_quantity)
        if status == "calculation_complete" and filled_quantity != order_quantity:
            raise ValueError("complete fill does not match order quantity")
        if status == "partial" and not Fraction(0) < filled_quantity < order_quantity:
            raise ValueError("partial fill is not below order quantity")
        if quote.fee_debit_asset is None:
            raise ValueError("calculated quote fee asset is missing")
        _asset(quote.fee_debit_asset, "fee_debit_asset")
        if quote.vwap_quote_numerator is None or quote.vwap_quote_denominator is None:
            raise ValueError("calculated quote exact VWAP is missing")
        numerator = _integer(
            quote.vwap_quote_numerator,
            "vwap_quote_numerator",
        )
        denominator = _integer(
            quote.vwap_quote_denominator,
            "vwap_quote_denominator",
            minimum=1,
        )
        expected_vwap = _fraction(quote.gross_quote_quantity) / _fraction(
            quote.filled_gross_base_quantity
        )
        if Fraction(numerator, denominator) != expected_vwap:
            raise ValueError("exact VWAP does not match quote and fill")
        if (
            quote.vwap_quote_per_base is not None
            and _fraction(quote.vwap_quote_per_base) != expected_vwap
        ):
            raise ValueError("Decimal VWAP does not match exact VWAP")

        if quote.direction == "buy":
            if (
                quote.gross_base_received_quantity is None
                or quote.net_base_received_quantity is None
                or quote.quote_debit_quantity is None
                or quote.quote_debit_asset is None
                or quote.base_debit_quantity is not None
                or quote.quote_received_quantity is not None
                or quote.quote_received_asset is not None
                or quote.net_quote_quantity != quote.quote_debit_quantity
            ):
                raise ValueError("buy QuantityQuote asset flows are inconsistent")
            gross_received = _fraction(quote.gross_base_received_quantity)
            net_received = _fraction(quote.net_base_received_quantity)
            quote_debit = _fraction(quote.quote_debit_quantity)
            quote_debit_asset = _asset(
                quote.quote_debit_asset,
                "quote_debit_asset",
            )
            if quote_debit_asset == target_asset:
                raise ValueError("buy quote asset cannot equal target base asset")
            if cex_match is not None and quote_debit_asset != market_quote:
                raise ValueError("CEX buy quote asset does not match Market")
            if (
                gross_received != filled_quantity
                or net_received > gross_received
                or quote_debit < gross_quote_quantity
            ):
                raise ValueError("buy QuantityQuote flow quantities are inconsistent")
            if cex_match is not None:
                if gross_quote_quantity <= 0:
                    raise ValueError("CEX buy gross quote must be positive")
                if order_quantity < _fraction(target_quantity):
                    raise ValueError("CEX buy order is below its net target")
                if (
                    quote.fee_debit_asset != market_base
                    and order_quantity != _fraction(target_quantity)
                ):
                    raise ValueError(
                        "CEX buy order does not match its non-base-fee target"
                    )
                if (
                    status == "calculation_complete"
                    and net_received != _fraction(target_quantity)
                ):
                    raise ValueError(
                        "complete CEX buy net receipt does not match target"
                    )
                if quote.fee_debit_asset == market_base:
                    if (
                        gross_received - net_received != fee_quantity
                        or quote_debit != gross_quote_quantity
                        or (
                            order_quantity - _fraction(target_quantity)
                            < fee_quantity
                        )
                    ):
                        raise ValueError("CEX buy base fee flows are inconsistent")
                elif quote.fee_debit_asset == market_quote:
                    if (
                        quote_debit - gross_quote_quantity != fee_quantity
                        or net_received != gross_received
                    ):
                        raise ValueError("CEX buy quote fee flows are inconsistent")
                elif (
                    net_received != gross_received
                    or quote_debit != gross_quote_quantity
                ):
                    raise ValueError(
                        "CEX buy third-asset fee flows are inconsistent"
                    )
        else:
            if (
                quote.base_debit_quantity is None
                or quote.quote_received_quantity is None
                or quote.quote_received_asset is None
                or quote.quote_debit_quantity is not None
                or quote.quote_debit_asset is not None
                or quote.gross_base_received_quantity is not None
                or quote.net_base_received_quantity is not None
                or quote.net_quote_quantity != quote.quote_received_quantity
            ):
                raise ValueError("sell QuantityQuote asset flows are inconsistent")
            base_debit = _fraction(quote.base_debit_quantity)
            quote_received = _fraction(quote.quote_received_quantity)
            quote_received_asset = _asset(
                quote.quote_received_asset,
                "quote_received_asset",
            )
            if quote_received_asset == target_asset:
                raise ValueError("sell quote asset cannot equal target base asset")
            if cex_match is not None and quote_received_asset != market_quote:
                raise ValueError("CEX sell quote asset does not match Market")
            if (
                base_debit < filled_quantity
                or quote_received > gross_quote_quantity
            ):
                raise ValueError("sell QuantityQuote flow quantities are inconsistent")
            if cex_match is not None:
                if order_quantity != _fraction(target_quantity):
                    raise ValueError("CEX sell order does not match target")
                if quote.fee_debit_asset == market_base:
                    if (
                        base_debit - filled_quantity != fee_quantity
                        or quote_received != gross_quote_quantity
                    ):
                        raise ValueError("CEX sell base fee flows are inconsistent")
                elif quote.fee_debit_asset == market_quote:
                    if (
                        gross_quote_quantity - quote_received != fee_quantity
                        or base_debit != filled_quantity
                    ):
                        raise ValueError("CEX sell quote fee flows are inconsistent")
                elif (
                    base_debit != filled_quantity
                    or quote_received != gross_quote_quantity
                ):
                    raise ValueError(
                        "CEX sell third-asset fee flows are inconsistent"
                    )

    required_hashes = (
        (quote.market_rules_sha256, "market_rules_sha256"),
        (quote.fee_source_sha256, "fee_source_sha256"),
        (quote.market_rules_binding_sha256, "market_rules_binding_sha256"),
        (quote.fee_binding_sha256, "fee_binding_sha256"),
    )
    for value, field_name in required_hashes:
        _hash(value, field_name)
    if bool(quote.raw_response_sha256) != bool(quote.levels_binding_sha256):
        raise ValueError("raw and levels bindings must be present together")
    if quote.raw_response_sha256:
        _hash(quote.raw_response_sha256, "raw_response_sha256")
        _hash(quote.levels_binding_sha256, "levels_binding_sha256")

    collector_fields = (
        quote.snapshot_id,
        quote.state_observed_at,
        quote.cohort_now,
        quote.raw_response_sha256,
        quote.levels_binding_sha256,
    )
    if any(collector_fields):
        if not all(collector_fields):
            raise ValueError("collector bindings must be complete")
        if cex_match is None:
            raise ValueError("cex-quantity collector bindings require a CEX Market")
        _required_text(quote.snapshot_id, "snapshot_id")
        _state_text, state_epoch = _timestamp(
            quote.state_observed_at,
            "state_observed_at",
        )
        _cohort_text, cohort_epoch = _timestamp(
            quote.cohort_now,
            "cohort_now",
        )
        state_age = cohort_epoch - state_epoch
        state_is_current = (
            Fraction(0)
            <= state_age
            <= _fraction(MAX_CEX_QUANTITY_STATE_AGE_SECONDS)
        )
        if status != "unavailable" and not state_is_current:
            raise ValueError("calculated collector quote state is not current")
        if (
            status == "unavailable"
            and reason == "book_state_not_current"
            and state_is_current
        ):
            raise ValueError("book_state_not_current reason is inconsistent")
        if re.fullmatch(r"cex-quantity:[0-9a-f]{64}", quote.state_id) is None:
            raise ValueError("collector state_id must be a bound SHA-256")
    else:
        _required_text(quote.state_id, "state_id")
    return quote


def common_net_target_quantity(
    *,
    requested_notional_usd: Any,
    buy_reference_price_usd: Any,
    buy_market_rules: MarketRules,
    sell_market_rules: MarketRules,
) -> CommonTarget:
    """Floor one budget-derived target onto both markets' exact base lattice."""
    if not isinstance(buy_market_rules, MarketRules) or not isinstance(
        sell_market_rules,
        MarketRules,
    ):
        raise ValueError("market rules are required")
    if buy_market_rules.base_asset != sell_market_rules.base_asset:
        raise ValueError("route base assets do not match")
    notional = _decimal(
        requested_notional_usd,
        "requested_notional_usd",
        positive=True,
    )
    price = _decimal(
        buy_reference_price_usd,
        "buy_reference_price_usd",
        positive=True,
    )
    decimals = max(
        buy_market_rules.base_unit_decimals,
        sell_market_rules.base_unit_decimals,
    )
    scale = 10**decimals
    buy_increment = _fraction(buy_market_rules.base_increment) * scale
    sell_increment = _fraction(sell_market_rules.base_increment) * scale
    if buy_increment.denominator != 1 or sell_increment.denominator != 1:
        raise ValueError("market increments do not share exact base units")
    lattice = _lcm(buy_increment.numerator, sell_increment.numerator)
    theoretical_raw = _fraction(notional) / _fraction(price) * scale
    raw = (theoretical_raw.numerator // theoretical_raw.denominator)
    raw = raw // lattice * lattice
    if raw <= 0:
        raise ValueError("common target is below one common quantity unit")
    minimums = []
    for current in (buy_market_rules, sell_market_rules):
        minimum = _fraction(current.min_base_quantity) * scale
        if minimum.denominator != 1:
            raise ValueError("minimum base quantity is not exactly representable")
        minimums.append(minimum.numerator)
    if raw < max(minimums):
        raise ValueError("common target is below a market minimum")
    return CommonTarget(
        asset=buy_market_rules.base_asset,
        unit_decimals=decimals,
        raw_quantity=raw,
        lattice_raw=lattice,
    )


def _unavailable_quote(
    *,
    rules: MarketRules,
    fee: FeeSemantics,
    target: CommonTarget,
    direction: str,
    state_id: str,
    reason_code: str,
    snapshot_id: str = "",
    state_observed_at: str = "",
    cohort_now: str = "",
    raw_response_sha256: str = "",
    levels_binding_sha256: str = "",
) -> QuantityQuote:
    return QuantityQuote(
        contract_version=ROUTE_QUANTITY_CONTRACT_VERSION,
        market_id=rules.market_id,
        direction=direction,
        status="unavailable",
        reason_code=reason_code,
        complete=False,
        calculation_complete=False,
        strict_eligible=False,
        target_base_asset=target.asset,
        target_base_raw=target.raw_quantity,
        target_base_unit_decimals=target.unit_decimals,
        target_lattice_raw=target.lattice_raw,
        target_base_quantity=target.quantity,
        market_base_unit_decimals=rules.base_unit_decimals,
        order_base_quantity=None,
        order_base_raw=None,
        filled_gross_base_quantity=None,
        filled_gross_base_raw=None,
        gross_base_received_quantity=None,
        gross_base_received_raw=None,
        net_base_received_quantity=None,
        net_base_received_raw=None,
        base_debit_quantity=None,
        base_debit_raw=None,
        gross_quote_quantity=None,
        net_quote_quantity=None,
        quote_debit_asset=None,
        quote_debit_quantity=None,
        quote_received_asset=None,
        quote_received_quantity=None,
        fee_debit_asset=None,
        fee_debit_quantity=None,
        levels_or_ticks_consumed=0,
        ending_price=None,
        vwap_quote_per_base=None,
        vwap_quote_numerator=None,
        vwap_quote_denominator=None,
        state_id=state_id,
        snapshot_id=snapshot_id,
        state_observed_at=state_observed_at,
        cohort_now=cohort_now,
        raw_response_sha256=raw_response_sha256,
        levels_binding_sha256=levels_binding_sha256,
        market_rules_sha256=rules.source_record_sha256,
        fee_source_sha256=fee.source_record_sha256,
        market_rules_binding_sha256=market_rules_record_binding_sha256(rules),
        fee_binding_sha256=fee_semantics_record_binding_sha256(fee),
    )


def _record_is_current(
    *,
    observed_at: str,
    valid_until: str,
    cohort_epoch: Fraction,
) -> bool:
    _observed, observed_epoch = _timestamp(observed_at, "observed_at")
    _valid, valid_epoch = _timestamp(valid_until, "valid_until")
    return observed_epoch <= cohort_epoch < valid_epoch


def _fee_contract_reason(
    direction: str,
    rules: MarketRules,
    fee: FeeSemantics,
) -> Optional[str]:
    expected = {
        ("buy", "received_base"): rules.base_asset,
        ("buy", "spent_quote"): rules.quote_asset,
        ("sell", "received_quote"): rules.quote_asset,
        ("sell", "sold_base"): rules.base_asset,
    }
    if fee.charge_basis == "third_asset_quote_value":
        if fee.fee_asset in {rules.base_asset, rules.quote_asset}:
            return "fee_asset_semantics_mismatch"
        if (
            fee.third_asset_quote_price is None
            or fee.conversion_source_record_sha256 is None
        ):
            return "third_asset_conversion_unavailable"
        return None
    if fee.charge_basis == "embedded":
        return "embedded_fee_not_supported_for_cex"
    expected_asset = expected.get((direction, fee.charge_basis))
    if expected_asset is None or fee.fee_asset != expected_asset:
        return "fee_asset_semantics_mismatch"
    return None


def _fee_amount(basis: Fraction, fee: FeeSemantics) -> Fraction:
    raw = basis * _fraction(fee.rate_bps) / 10_000
    return _round_fraction(raw, fee.fee_increment, fee.rounding_mode)


def _gross_for_exact_net_base(
    target: Fraction,
    rules: MarketRules,
    fee: FeeSemantics,
) -> Optional[Tuple[Fraction, Fraction]]:
    lot = _fraction(rules.base_increment)
    fee_increment = _fraction(fee.fee_increment)
    rate = _fraction(fee.rate_bps) / 10_000
    if rate == 0:
        amount = _fee_amount(target, fee)
        if amount != 0 or (target / lot).denominator != 1:
            return None
        return target, amount
    if fee.rounding_mode == "not_applicable":
        return None
    remaining_rate = 1 - rate
    center = rate * target / (fee_increment * remaining_rate)
    if fee.rounding_mode == "ceiling":
        lower_multiple = max(0, _ceil_fraction(center))
        strict_upper = (
            rate * target + fee_increment
        ) / (fee_increment * remaining_rate)
        upper_multiple = _ceil_fraction(strict_upper) - 1
    elif fee.rounding_mode == "floor":
        strict_lower = (
            rate * target - fee_increment
        ) / (fee_increment * remaining_rate)
        lower_multiple = max(0, _floor_fraction(strict_lower) + 1)
        upper_multiple = _floor_fraction(center)
    elif fee.rounding_mode == "exact":
        if center.denominator != 1:
            return None
        lower_multiple = center.numerator
        upper_multiple = center.numerator
    else:  # pragma: no cover - FeeSemantics validates the vocabulary
        return None
    if upper_multiple < lower_multiple:
        return None

    common_denominator = _lcm(
        _lcm(target.denominator, fee_increment.denominator),
        lot.denominator,
    )
    target_units = (target * common_denominator).numerator
    fee_units = (fee_increment * common_denominator).numerator
    lot_units = (lot * common_denominator).numerator
    divisor = gcd(fee_units, lot_units)
    if (-target_units) % divisor:
        return None
    modulus = lot_units // divisor
    if modulus == 1:
        residue = 0
    else:
        reduced_fee = fee_units // divisor
        reduced_target = -target_units // divisor
        residue = (
            reduced_target * pow(reduced_fee, -1, modulus)
        ) % modulus
    if residue < lower_multiple:
        residue += _ceil_fraction(
            Fraction(lower_multiple - residue, modulus)
        ) * modulus
    if residue > upper_multiple:
        return None

    amount = residue * fee_increment
    gross = target + amount
    try:
        rounded_amount = _fee_amount(gross, fee)
    except ValueError:
        return None
    if (
        rounded_amount != amount
        or gross - rounded_amount != target
        or (gross / lot).denominator != 1
    ):
        return None
    return gross, amount


def _walk_levels(
    levels: Iterable[Tuple[Any, Any]],
    *,
    direction: str,
    order_quantity: Fraction,
    rules: MarketRules,
) -> Tuple[Fraction, Fraction, int, Optional[Decimal], bool]:
    filled = Fraction(0)
    quote = Fraction(0)
    consumed = 0
    ending = None
    previous = None
    for raw_level in levels:
        if not isinstance(raw_level, (tuple, list)) or len(raw_level) != 2:
            raise ValueError("CEX level must contain exact price and quantity")
        price_decimal = _decimal(raw_level[0], "level_price", positive=True)
        quantity_decimal = _decimal(raw_level[1], "level_quantity", positive=True)
        _raw_exact(
            quantity_decimal,
            rules.base_unit_decimals,
            "level_quantity",
        )
        price = _fraction(price_decimal)
        quantity = _fraction(quantity_decimal)
        if previous is not None:
            if direction == "buy" and price < previous:
                raise ValueError("buy levels must be ascending")
            if direction == "sell" and price > previous:
                raise ValueError("sell levels must be descending")
        previous = price
        remaining = order_quantity - filled
        if remaining <= 0:
            break
        take = min(quantity, remaining)
        if take <= 0:  # pragma: no cover - positive validation owns this
            continue
        filled += take
        quote += price * take
        consumed += 1
        ending = price_decimal
        if filled == order_quantity:
            break
    return filled, quote, consumed, ending, filled == order_quantity


def _optional_finite_ratio(numerator: Fraction, denominator: Fraction) -> Optional[Decimal]:
    if denominator <= 0:
        return None
    try:
        return _fraction_decimal(numerator / denominator, "VWAP")
    except ValueError:
        return None


def quote_cex_book_quantity(
    levels: Iterable[Tuple[Any, Any]],
    target_token_quantity: CommonTarget,
    market_rules: MarketRules,
    fee_semantics: FeeSemantics,
    *,
    direction: str,
    source_quote_asset: str,
    full_book_reported: bool,
    state_id: str,
    snapshot_id: str = "",
    state_observed_at: str = "",
    cohort_now: str = "",
    raw_response_sha256: str = "",
    levels_binding_sha256: str = "",
) -> QuantityQuote:
    """Calculate one exact common base quantity from normalized CEX levels.

    This public arithmetic helper never authenticates its caller or upstream
    records. A complete fill is therefore ``calculation_complete`` but never
    final strict route evidence. The collector adapter supplies and verifies
    immutable snapshot bindings and current-time evidence separately.
    """
    if not isinstance(target_token_quantity, CommonTarget):
        raise ValueError("target_token_quantity must be a CommonTarget")
    if not isinstance(market_rules, MarketRules):
        raise ValueError("market_rules must be MarketRules")
    if not isinstance(fee_semantics, FeeSemantics):
        raise ValueError("fee_semantics must be FeeSemantics")
    direction_text = _required_text(direction, "direction")
    if direction_text not in {"buy", "sell"}:
        raise ValueError("direction must be buy or sell")
    if type(full_book_reported) is not bool:
        raise ValueError("full_book_reported must be boolean")
    state = _required_text(state_id, "state_id")
    snapshot = ""
    if snapshot_id:
        snapshot = _required_text(snapshot_id, "snapshot_id")
    state_observed = ""
    cohort = ""
    if state_observed_at:
        state_observed, _state_epoch = _timestamp(
            state_observed_at,
            "state_observed_at",
        )

    def unavailable(reason_code: str) -> QuantityQuote:
        return _unavailable_quote(
            rules=market_rules,
            fee=fee_semantics,
            target=target_token_quantity,
            direction=direction_text,
            state_id=state,
            reason_code=reason_code,
            snapshot_id=snapshot,
            state_observed_at=state_observed,
            cohort_now=cohort,
            raw_response_sha256=raw_response_sha256,
            levels_binding_sha256=levels_binding_sha256,
        )

    if cohort_now:
        cohort, cohort_epoch = _timestamp(cohort_now, "cohort_now")
        if (
            market_rules.record_binding_sha256
            != market_rules_record_binding_sha256(market_rules)
        ):
            return unavailable("market_rules_binding_mismatch")
        if (
            fee_semantics.record_binding_sha256
            != fee_semantics_record_binding_sha256(fee_semantics)
        ):
            return unavailable("fee_semantics_binding_mismatch")
        if not _record_is_current(
            observed_at=market_rules.observed_at,
            valid_until=market_rules.valid_until,
            cohort_epoch=cohort_epoch,
        ):
            return unavailable("market_rules_not_current")
        if not _record_is_current(
            observed_at=fee_semantics.observed_at,
            valid_until=fee_semantics.valid_until,
            cohort_epoch=cohort_epoch,
        ):
            return unavailable("fee_semantics_not_current")
        if not state_observed:
            return unavailable("book_state_observed_at_unavailable")
        _state_text, state_epoch = _timestamp(
            state_observed,
            "state_observed_at",
        )
        state_age = cohort_epoch - state_epoch
        if (
            state_age < 0
            or state_age > _fraction(MAX_CEX_QUANTITY_STATE_AGE_SECONDS)
        ):
            return unavailable("book_state_not_current")
    source_quote = _asset(source_quote_asset, "source_quote_asset")
    if source_quote != market_rules.quote_asset:
        return unavailable("source_quote_asset_mismatch")
    if target_token_quantity.asset != market_rules.base_asset:
        return unavailable("target_asset_mismatch")
    target = _fraction(target_token_quantity.quantity)
    target_rule_raw = target * (10**market_rules.base_unit_decimals)
    if target_rule_raw.denominator != 1:
        return unavailable("target_base_unit_misaligned")
    if target_rule_raw.numerator % market_rules.base_increment_raw:
        return unavailable("target_lot_misaligned")
    fee_reason = _fee_contract_reason(
        direction_text,
        market_rules,
        fee_semantics,
    )
    if fee_reason is not None:
        return unavailable(fee_reason)

    order_quantity = target
    if direction_text == "buy" and fee_semantics.charge_basis == "received_base":
        solution = _gross_for_exact_net_base(
            target,
            market_rules,
            fee_semantics,
        )
        if solution is None:
            return unavailable("base_fee_target_not_exactly_representable")
        order_quantity = solution[0]

    if order_quantity < _fraction(market_rules.min_base_quantity):
        return unavailable("minimum_base_quantity_not_met")

    filled, raw_quote, consumed, ending, complete = _walk_levels(
        levels,
        direction=direction_text,
        order_quantity=order_quantity,
        rules=market_rules,
    )
    settled_quote = _round_fraction(
        raw_quote,
        market_rules.quote_increment,
        "ceiling" if direction_text == "buy" else "floor",
    )

    actual_fee = Fraction(0)
    gross_base_received = None
    net_base_received = None
    base_debit = None
    quote_debit = None
    quote_received = None
    fee_asset = fee_semantics.fee_asset

    if direction_text == "buy":
        gross_base_received = filled
        if fee_semantics.charge_basis == "received_base":
            actual_fee = _fee_amount(filled, fee_semantics)
            net_base_received = filled - actual_fee
        elif fee_semantics.charge_basis == "spent_quote":
            actual_fee = _fee_amount(settled_quote, fee_semantics)
            net_base_received = filled
        else:
            assert fee_semantics.charge_basis == "third_asset_quote_value"
            assert fee_semantics.third_asset_quote_price is not None
            actual_fee = _round_fraction(
                settled_quote
                * _fraction(fee_semantics.rate_bps)
                / 10_000
                / _fraction(fee_semantics.third_asset_quote_price),
                fee_semantics.fee_increment,
                fee_semantics.rounding_mode,
            )
            net_base_received = filled
        quote_debit = (
            settled_quote + actual_fee
            if fee_semantics.charge_basis == "spent_quote"
            else settled_quote
        )
    else:
        base_debit = filled
        if fee_semantics.charge_basis == "received_quote":
            actual_fee = _fee_amount(settled_quote, fee_semantics)
            quote_received = settled_quote - actual_fee
        elif fee_semantics.charge_basis == "sold_base":
            actual_fee = _fee_amount(filled, fee_semantics)
            base_debit = filled + actual_fee
            quote_received = settled_quote
        else:
            assert fee_semantics.charge_basis == "third_asset_quote_value"
            assert fee_semantics.third_asset_quote_price is not None
            actual_fee = _round_fraction(
                settled_quote
                * _fraction(fee_semantics.rate_bps)
                / 10_000
                / _fraction(fee_semantics.third_asset_quote_price),
                fee_semantics.fee_increment,
                fee_semantics.rounding_mode,
            )
            quote_received = settled_quote

    status = "calculation_complete" if complete else "partial"
    reason = "authenticated_upstream_unavailable" if complete else (
        "full_book_insufficient_liquidity"
        if full_book_reported
        else "source_level_limit"
    )
    if complete and settled_quote < _fraction(market_rules.min_quote_notional):
        return unavailable("minimum_notional_not_met")

    filled_decimal = _fraction_decimal(filled, "filled base quantity")
    settled_decimal = _fraction_decimal(settled_quote, "gross quote quantity")
    order_decimal = _fraction_decimal(order_quantity, "order base quantity")
    fee_decimal = _fraction_decimal(actual_fee, "fee debit quantity")
    gross_base_decimal = (
        _fraction_decimal(gross_base_received, "gross base received")
        if gross_base_received is not None
        else None
    )
    net_base_decimal = (
        _fraction_decimal(net_base_received, "net base received")
        if net_base_received is not None
        else None
    )
    base_debit_decimal = (
        _fraction_decimal(base_debit, "base debit")
        if base_debit is not None
        else None
    )
    quote_debit_decimal = (
        _fraction_decimal(quote_debit, "quote debit")
        if quote_debit is not None
        else None
    )
    quote_received_decimal = (
        _fraction_decimal(quote_received, "quote received")
        if quote_received is not None
        else None
    )
    vwap_fraction = settled_quote / filled if filled > 0 else None
    vwap = (
        _optional_finite_ratio(settled_quote, filled)
        if filled > 0
        else None
    )
    return QuantityQuote(
        contract_version=ROUTE_QUANTITY_CONTRACT_VERSION,
        market_id=market_rules.market_id,
        direction=direction_text,
        status=status,
        reason_code=reason,
        complete=complete,
        calculation_complete=(status == "calculation_complete"),
        strict_eligible=False,
        target_base_asset=target_token_quantity.asset,
        target_base_raw=target_token_quantity.raw_quantity,
        target_base_unit_decimals=target_token_quantity.unit_decimals,
        target_lattice_raw=target_token_quantity.lattice_raw,
        target_base_quantity=target_token_quantity.quantity,
        market_base_unit_decimals=market_rules.base_unit_decimals,
        order_base_quantity=order_decimal,
        order_base_raw=_fraction_raw_exact(
            order_quantity,
            market_rules.base_unit_decimals,
            "order base quantity",
        ),
        filled_gross_base_quantity=filled_decimal,
        filled_gross_base_raw=_fraction_raw_exact(
            filled,
            market_rules.base_unit_decimals,
            "filled gross base quantity",
        ),
        gross_base_received_quantity=gross_base_decimal,
        gross_base_received_raw=(
            _fraction_raw_exact(
                gross_base_received,
                market_rules.base_unit_decimals,
                "gross base received",
            )
            if gross_base_received is not None
            else None
        ),
        net_base_received_quantity=net_base_decimal,
        net_base_received_raw=(
            _fraction_raw_exact(
                net_base_received,
                market_rules.base_unit_decimals,
                "net base received",
            )
            if net_base_received is not None
            else None
        ),
        base_debit_quantity=base_debit_decimal,
        base_debit_raw=(
            _fraction_raw_exact(
                base_debit,
                market_rules.base_unit_decimals,
                "base debit",
            )
            if base_debit is not None
            else None
        ),
        gross_quote_quantity=settled_decimal,
        net_quote_quantity=(
            quote_received_decimal
            if direction_text == "sell"
            else quote_debit_decimal
        ),
        quote_debit_asset=(
            market_rules.quote_asset if direction_text == "buy" else None
        ),
        quote_debit_quantity=quote_debit_decimal,
        quote_received_asset=(
            market_rules.quote_asset if direction_text == "sell" else None
        ),
        quote_received_quantity=quote_received_decimal,
        fee_debit_asset=fee_asset,
        fee_debit_quantity=fee_decimal,
        levels_or_ticks_consumed=consumed,
        ending_price=ending,
        vwap_quote_per_base=vwap,
        vwap_quote_numerator=(
            vwap_fraction.numerator if vwap_fraction is not None else None
        ),
        vwap_quote_denominator=(
            vwap_fraction.denominator if vwap_fraction is not None else None
        ),
        state_id=state,
        snapshot_id=snapshot,
        state_observed_at=state_observed,
        cohort_now=cohort,
        raw_response_sha256=raw_response_sha256,
        levels_binding_sha256=levels_binding_sha256,
        market_rules_sha256=market_rules.source_record_sha256,
        fee_source_sha256=fee_semantics.source_record_sha256,
        market_rules_binding_sha256=market_rules_record_binding_sha256(
            market_rules
        ),
        fee_binding_sha256=fee_semantics_record_binding_sha256(
            fee_semantics
        ),
    )
