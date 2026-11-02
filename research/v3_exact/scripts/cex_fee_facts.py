"""Account-specific and bounded public CEX taker-fee facts.

Authenticated responses are normalized at a narrow boundary that retains only
fee evidence.  Credentials, account identifiers, arbitrary response fields,
and private file paths are neither returned nor logged.  Public schedules are
scenario bounds only and can never become strict fee facts.
"""

from __future__ import annotations

import csv
import fnmatch
import hashlib
import json
import os
import re
import stat
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
from urllib.parse import urlparse

try:
    from scripts.execution_cost_components import cost_component_row
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from execution_cost_components import cost_component_row  # type: ignore
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


PRIVATE_FEE_PROFILE_COLUMNS = (
    "profile_id",
    "venue",
    "instrument",
    "side",
    "taker_fee_bps",
    "fee_asset",
    "basis",
    "observed_at",
    "valid_until",
    "source_record_sha256",
)

PUBLIC_FEE_SCHEDULE_COLUMNS = (
    "venue",
    "instrument_pattern",
    "side",
    "min_taker_fee_bps",
    "max_taker_fee_bps",
    "fee_asset",
    "basis",
    "checked_at",
    "valid_until",
    "source_url",
)

OFFICIAL_AUTHENTICATED_FEE_ENDPOINTS = {
    "binance": "GET /api/v3/account/commission",
    "bybit": "GET /v5/account/fee-rate",
    "okx": "GET /api/v5/account/trade-fee",
}

_PROFILE_ID = re.compile(r"[0-9a-f]{64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_VENUE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_INSTRUMENT = re.compile(r"[A-Z0-9][A-Z0-9_./-]{0,127}\Z")
_ASSET = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,31}\Z")
_PUBLIC_ASSET = re.compile(r"(?:[A-Z0-9][A-Z0-9._-]{0,31}|received_asset)\Z")
_PUBLIC_PATTERN = re.compile(r"[A-Z0-9*][A-Z0-9_./*-]{0,127}\Z")
_CANONICAL_PAIR = re.compile(
    r"([A-Z0-9][A-Z0-9._-]{0,31})/([A-Z0-9][A-Z0-9._-]{0,31})\Z"
)
_CEX_MARKET_ID = re.compile(
    r"cex:([a-z][a-z0-9_]{0,63}):"
    r"([A-Z0-9][A-Z0-9._-]{0,31})/"
    r"([A-Z0-9][A-Z0-9._-]{0,31})\Z"
)

_PRIVATE_BASIS = {
    "authenticated_taker_fee": (
        "validated authenticated taker fee on requested notional"
    ),
}
_PUBLIC_BASIS = {
    "official_spot_taker_fee_range": "official public spot taker-fee range",
}


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("{} must be non-empty canonical text".format(field))
    return value


def _validated_text(value: Any, field: str, pattern: re.Pattern) -> str:
    text = _required_text(value, field)
    if pattern.fullmatch(text) is None:
        raise ValueError("{} is invalid".format(field))
    return text


def _profile_id(value: Any) -> str:
    try:
        return _validated_text(value, "profile_id", _PROFILE_ID)
    except ValueError as error:
        raise ValueError("profile_id must be an opaque lowercase SHA-256 id") from error


def _venue(value: Any) -> str:
    return _validated_text(value, "venue", _VENUE)


def _instrument(value: Any) -> str:
    return _validated_text(value, "instrument", _INSTRUMENT)


def _side(value: Any, *, allow_both: bool = False) -> str:
    text = _required_text(value, "side")
    allowed = {"buy", "sell"}
    if allow_both:
        allowed.add("both")
    if text not in allowed:
        raise ValueError("side is invalid")
    return text


def _fee_asset(value: Any, *, public: bool = False) -> str:
    pattern = _PUBLIC_ASSET if public else _ASSET
    return _validated_text(value, "fee_asset", pattern)


def _basis_from_code(
    value: Any,
    *,
    templates: Mapping[str, str],
) -> str:
    code = _required_text(value, "basis")
    try:
        return templates[code]
    except KeyError:
        raise ValueError("basis code is unsupported") from None


def _canonical_pair(value: Any) -> Tuple[str, str, str]:
    instrument = _instrument(value)
    match = _CANONICAL_PAIR.fullmatch(instrument)
    if match is None:
        raise ValueError("instrument must be canonical BASE/QUOTE")
    base, quote = match.groups()
    if base == quote:
        raise ValueError("instrument base and quote must differ")
    return base, quote, instrument


def _parse_cex_market_id(value: Any) -> Tuple[str, str, str, str]:
    market_id = _required_text(value, "market_id")
    match = _CEX_MARKET_ID.fullmatch(market_id)
    if match is None:
        raise ValueError("market_id must be canonical cex:venue:BASE/QUOTE")
    venue, base, quote = match.groups()
    if base == quote:
        raise ValueError("market_id base and quote must differ")
    return venue, base, quote, "{}/{}".format(base, quote)


def _native_instrument(venue: str, base: str, quote: str) -> str:
    if venue in ("binance", "bybit"):
        return base + quote
    if venue == "okx":
        return "{}-{}".format(base, quote)
    if venue == "crypto_com":
        return "{}_{}".format(base, quote)
    return "{}/{}".format(base, quote)


def _received_asset(base: str, quote: str, side: str) -> str:
    return base if side == "buy" else quote


def _decimal(value: Any, field: str, *, signed: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(
        value, (Decimal, int, str)
    ):
        raise ValueError("{} must be an exact Decimal value".format(field))
    if isinstance(value, str) and (not value or value != value.strip()):
        raise ValueError("{} must be canonical Decimal text".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(
            "{} must be an exact finite Decimal value".format(field)
        ) from error
    if not number.is_finite() or (not signed and number < 0):
        raise ValueError("{} must be a non-negative finite Decimal value".format(field))
    return number


def _decimal_text(number: Decimal) -> str:
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _parts(number: Decimal) -> Tuple[int, int]:
    value = number.as_tuple()
    coefficient = 0
    for digit in value.digits:
        coefficient = coefficient * 10 + digit
    if value.sign:
        coefficient = -coefficient
    return coefficient, int(value.exponent)


def _from_parts(coefficient: int, exponent: int) -> Decimal:
    if coefficient == 0:
        return Decimal(0)
    sign = 1 if coefficient < 0 else 0
    digits = tuple(int(character) for character in str(abs(coefficient)))
    return Decimal((sign, digits, exponent))


def _exact_sum(values: Iterable[Decimal]) -> Decimal:
    parts = [_parts(value) for value in values]
    if not parts:
        return Decimal(0)
    common_exponent = min(exponent for _coefficient, exponent in parts)
    coefficient = sum(
        value * (10 ** (exponent - common_exponent))
        for value, exponent in parts
    )
    return _from_parts(coefficient, common_exponent)


def _exact_product(*values: Decimal) -> Decimal:
    coefficient = 1
    exponent = 0
    for value in values:
        part_coefficient, part_exponent = _parts(value)
        coefficient *= part_coefficient
        exponent += part_exponent
    return _from_parts(coefficient, exponent)


def _validate_timestamp(value: Any, field: str) -> str:
    text = _required_text(value, field)
    try:
        exact_rfc3339_epoch_seconds(text)
    except ValueError as error:
        raise ValueError(
            "{} must be timezone-aware RFC 3339 text".format(field)
        ) from error
    return text


def _validate_window(observed_at: Any, valid_until: Any) -> Tuple[str, str]:
    observed = _validate_timestamp(observed_at, "observed_at")
    valid = _validate_timestamp(valid_until, "valid_until")
    if exact_rfc3339_epoch_seconds(valid) <= exact_rfc3339_epoch_seconds(observed):
        raise ValueError("valid_until must be after observed_at")
    return observed, valid


def _timestamp_from_milliseconds(value: Any, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("{} must be exact integer milliseconds".format(field))
    text = str(value)
    if not text or not text.isdigit():
        raise ValueError("{} must be exact integer milliseconds".format(field))
    milliseconds = int(text)
    seconds, remainder = divmod(milliseconds, 1000)
    try:
        instant = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            seconds=seconds, milliseconds=remainder
        )
    except (OverflowError, ValueError) as error:
        raise ValueError(
            "{} is outside the supported timestamp range".format(field)
        ) from error
    if remainder:
        return instant.strftime("%Y-%m-%dT%H:%M:%S.") + "{:03d}Z".format(
            remainder
        )
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("{} must be an object".format(field))
    return value


def _sequence(value: Any, field: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("{} must be an array".format(field))
    return value


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence(
    *,
    profile_id: Any,
    venue: Any,
    instrument: Any,
    side: Any,
    taker_fee_bps: Decimal,
    fee_asset: Any,
    basis: Any,
    observed_at: Any,
    valid_until: Any,
    source_record: Mapping[str, Any],
) -> Dict[str, str]:
    observed, valid = _validate_window(observed_at, valid_until)
    row = {
        "profile_id": _profile_id(profile_id),
        "venue": _venue(venue),
        "instrument": _instrument(instrument),
        "side": _side(side),
        "taker_fee_bps": _decimal_text(
            _decimal(taker_fee_bps, "taker_fee_bps")
        ),
        "fee_asset": _fee_asset(fee_asset),
        "basis": _required_text(basis, "basis"),
        "observed_at": observed,
        "valid_until": valid,
        "source_record_sha256": _canonical_sha256(source_record),
    }
    return row


def _commission_side_rate(
    response: Mapping[str, Any], field: str, side: str
) -> Tuple[Decimal, Decimal]:
    rates = _mapping(response.get(field), field)
    taker = _decimal(rates.get("taker"), "{}.taker".format(field))
    side_rate = _decimal(rates.get("buyer" if side == "buy" else "seller"),
                         "{}.{}".format(field, "buyer" if side == "buy" else "seller"))
    return taker, side_rate


def normalize_binance_taker_fee(
    response: Mapping[str, Any],
    *,
    side: str,
    profile_id: str,
    observed_at: str,
    valid_until: str,
    discount_asset_funded: Optional[bool],
    received_asset: Optional[str] = None,
) -> Dict[str, str]:
    """Normalize a redacted Binance account-commission response.

    Binance's discount flags do not prove that the account will have enough of
    the discount Token at execution.  The caller must therefore bind that state
    explicitly; an unknown funding state is rejected instead of guessed.
    """
    payload = _mapping(response, "Binance response")
    trade_side = _side(side)
    instrument = _instrument(payload.get("symbol"))
    standard_taker, standard_side = _commission_side_rate(
        payload, "standardCommission", trade_side
    )
    special_taker, special_side = _commission_side_rate(
        payload, "specialCommission", trade_side
    )
    tax_taker, tax_side = _commission_side_rate(
        payload, "taxCommission", trade_side
    )
    discount = _mapping(payload.get("discount"), "discount")
    enabled_for_account = discount.get("enabledForAccount")
    enabled_for_symbol = discount.get("enabledForSymbol")
    if type(enabled_for_account) is not bool or type(enabled_for_symbol) is not bool:
        raise ValueError("Binance discount flags must be booleans")
    discount_enabled = enabled_for_account and enabled_for_symbol
    discount_rate = _decimal(discount.get("discount"), "discount.discount")
    if discount_rate > 1:
        raise ValueError("discount.discount must be between zero and one")

    standard_rate = _exact_sum((standard_taker, standard_side))
    fee_asset: str
    if discount_enabled:
        if type(discount_asset_funded) is not bool:
            raise ValueError(
                "discount_asset_funded must explicitly bind the discount funding state"
            )
        if discount_asset_funded:
            standard_rate = _exact_product(standard_rate, discount_rate)
            fee_asset = _fee_asset(discount.get("discountAsset"))
            if fee_asset != "BNB":
                raise ValueError(
                    "funded Binance discount evidence must identify BNB"
                )
        else:
            fee_asset = _fee_asset(received_asset)
    else:
        if discount_asset_funded not in (None, False):
            raise ValueError("discount_asset_funded conflicts with disabled discount")
        fee_asset = _fee_asset(received_asset)

    special_rate = _exact_sum((special_taker, special_side))
    tax_rate = _exact_sum((tax_taker, tax_side))
    total_rate = _exact_sum((standard_rate, special_rate, tax_rate))
    rate_bps = _exact_product(total_rate, Decimal("10000"))
    basis = (
        "authenticated Binance spot taker commission; side={}; standard={}"
        "; special={}; tax={}; discount={}; fee_asset={}"
    ).format(
        trade_side,
        _decimal_text(standard_rate),
        _decimal_text(special_rate),
        _decimal_text(tax_rate),
        "funded" if discount_enabled and discount_asset_funded else "not_applied",
        fee_asset,
    )
    source_record = {
        "venue": "binance",
        "instrument": instrument,
        "side": trade_side,
        "standard_taker": _decimal_text(standard_taker),
        "standard_side": _decimal_text(standard_side),
        "special_taker": _decimal_text(special_taker),
        "special_side": _decimal_text(special_side),
        "tax_taker": _decimal_text(tax_taker),
        "tax_side": _decimal_text(tax_side),
        "discount_enabled": discount_enabled,
        "discount_funded": discount_asset_funded,
        "discount_rate": _decimal_text(discount_rate),
        "fee_asset": fee_asset,
    }
    return _evidence(
        profile_id=profile_id,
        venue="binance",
        instrument=instrument,
        side=trade_side,
        taker_fee_bps=rate_bps,
        fee_asset=fee_asset,
        basis=basis,
        observed_at=observed_at,
        valid_until=valid_until,
        source_record=source_record,
    )


def normalize_bybit_taker_fee(
    response: Mapping[str, Any],
    *,
    instrument: str,
    side: str,
    fee_asset: str,
    profile_id: str,
    valid_until: str,
) -> Dict[str, str]:
    """Normalize one authenticated Bybit V5 spot fee-rate response."""
    payload = _mapping(response, "Bybit response")
    if payload.get("retCode") not in (0, "0"):
        raise ValueError("Bybit fee response must report success")
    bound_instrument = _instrument(instrument)
    result = _mapping(payload.get("result"), "Bybit result")
    category = result.get("category")
    if category != "spot":
        raise ValueError("Bybit fee response category must be exactly spot")
    matches = []
    for item in _sequence(result.get("list"), "Bybit result.list"):
        row = _mapping(item, "Bybit fee row")
        if row.get("symbol") == bound_instrument:
            matches.append(row)
    if len(matches) != 1:
        raise ValueError("Bybit response must contain exactly one bound instrument")
    row = matches[0]
    taker_rate = _decimal(row.get("takerFeeRate"), "takerFeeRate")
    maker_rate = _decimal(row.get("makerFeeRate"), "makerFeeRate")
    observed_at = _timestamp_from_milliseconds(payload.get("time"), "Bybit time")
    trade_side = _side(side)
    asset = _fee_asset(fee_asset)
    return _evidence(
        profile_id=profile_id,
        venue="bybit",
        instrument=bound_instrument,
        side=trade_side,
        taker_fee_bps=_exact_product(taker_rate, Decimal("10000")),
        fee_asset=asset,
        basis=(
            "authenticated spot takerFeeRate; side={}; fee_asset={}"
        ).format(trade_side, asset),
        observed_at=observed_at,
        valid_until=valid_until,
        source_record={
            "venue": "bybit",
            "instrument": bound_instrument,
            "side": trade_side,
            "taker_fee_rate": _decimal_text(taker_rate),
            "maker_fee_rate": _decimal_text(maker_rate),
            "fee_asset": asset,
            "observed_at": observed_at,
        },
    )


def normalize_okx_taker_fee(
    response: Mapping[str, Any],
    *,
    instrument: str,
    side: str,
    fee_asset: str,
    profile_id: str,
    valid_until: str,
) -> Dict[str, str]:
    """Normalize one authenticated OKX V5 spot fee-rate response."""
    payload = _mapping(response, "OKX response")
    if payload.get("code") not in (0, "0"):
        raise ValueError("OKX fee response must report success")
    spot_rows = []
    for item in _sequence(payload.get("data"), "OKX data"):
        row = _mapping(item, "OKX fee row")
        if row.get("instType") == "SPOT":
            spot_rows.append(row)
    if len(spot_rows) != 1:
        raise ValueError("OKX response must contain exactly one SPOT fee row")
    row = spot_rows[0]
    taker_encoded = _decimal(row.get("taker"), "taker", signed=True)
    maker_encoded = _decimal(row.get("maker"), "maker", signed=True)
    if taker_encoded > 0:
        raise ValueError("OKX taker rebate cannot be represented as a nonnegative cost")
    taker_cost_rate = -taker_encoded
    observed_at = _timestamp_from_milliseconds(row.get("ts"), "OKX ts")
    bound_instrument = _instrument(instrument)
    trade_side = _side(side)
    asset = _fee_asset(fee_asset)
    return _evidence(
        profile_id=profile_id,
        venue="okx",
        instrument=bound_instrument,
        side=trade_side,
        taker_fee_bps=_exact_product(taker_cost_rate, Decimal("10000")),
        fee_asset=asset,
        basis=(
            "authenticated OKX spot taker rate; negative commission encoding"
            "; side={}; fee_asset={}"
        ).format(trade_side, asset),
        observed_at=observed_at,
        valid_until=valid_until,
        source_record={
            "venue": "okx",
            "instrument": bound_instrument,
            "side": trade_side,
            "taker_encoded_rate": _decimal_text(taker_encoded),
            "maker_encoded_rate": _decimal_text(maker_encoded),
            "fee_asset": asset,
            "observed_at": observed_at,
        },
    )


def _validate_private_metadata(metadata: os.stat_result) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("private fee profile must be a regular owner-only file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ValueError("private fee profile must be owner-only")
    getuid = getattr(os, "geteuid", None)
    if getuid is not None and metadata.st_uid != getuid():
        raise ValueError("private fee profile must be owned by the running user")


def _read_private_rows(path: Path) -> List[Dict[str, str]]:
    """Read an owner-only profile from the same validated, no-follow FD."""
    try:
        path_metadata = os.lstat(str(path))
    except OSError:
        raise ValueError("private fee profile is unavailable") from None
    if stat.S_ISLNK(path_metadata.st_mode):
        raise ValueError("private fee profile must be a regular owner-only file")
    _validate_private_metadata(path_metadata)

    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("private fee profile secure open is unavailable")
    flags = os.O_RDONLY | nofollow
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        file_descriptor = os.open(str(path), flags)
    except OSError:
        raise ValueError("private fee profile is unavailable or changed") from None

    try:
        opened_metadata = os.fstat(file_descriptor)
        _validate_private_metadata(opened_metadata)
        if (
            opened_metadata.st_dev != path_metadata.st_dev
            or opened_metadata.st_ino != path_metadata.st_ino
        ):
            raise ValueError("private fee profile changed during open")
        try:
            with os.fdopen(
                file_descriptor,
                "r",
                encoding="utf-8",
                newline="",
            ) as handle:
                file_descriptor = -1
                reader = csv.DictReader(handle)
                if tuple(reader.fieldnames or ()) != PRIVATE_FEE_PROFILE_COLUMNS:
                    raise ValueError("private fee profile columns are invalid")
                return list(reader)
        except (OSError, UnicodeError, csv.Error):
            raise ValueError("private fee profile could not be read") from None
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)


def load_validated_fee_profile(
    path: os.PathLike,
    *,
    now: str,
) -> List[Dict[str, str]]:
    """Load one owner-only, fresh generic authenticated fee profile."""
    profile_path = Path(path)
    now_text = _validate_timestamp(now, "now")
    now_epoch = exact_rfc3339_epoch_seconds(now_text)
    raw_rows = _read_private_rows(profile_path)
    if not raw_rows:
        raise ValueError("private fee profile must contain at least one row")

    inventory: List[Dict[str, str]] = []
    keys = set()
    for raw in raw_rows:
        observed, valid = _validate_window(
            raw.get("observed_at"), raw.get("valid_until")
        )
        if exact_rfc3339_epoch_seconds(observed) > now_epoch:
            raise ValueError("private fee profile observation is in the future")
        if exact_rfc3339_epoch_seconds(valid) <= now_epoch:
            raise ValueError("private fee profile contains a stale record")
        source_hash = _validated_text(
            raw.get("source_record_sha256"), "source_record_sha256", _SHA256
        )
        rate = _decimal(raw.get("taker_fee_bps"), "taker_fee_bps")
        venue = _venue(raw.get("venue"))
        base, quote, instrument = _canonical_pair(raw.get("instrument"))
        trade_side = _side(raw.get("side"))
        expected_fee_asset = _received_asset(base, quote, trade_side)
        fee_asset = _fee_asset(raw.get("fee_asset"))
        if fee_asset != expected_fee_asset:
            raise ValueError("private fee profile has wrong received fee_asset")
        row = {
            "profile_id": _profile_id(raw.get("profile_id")),
            "venue": venue,
            "instrument": instrument,
            "side": trade_side,
            "taker_fee_bps": _decimal_text(rate),
            "fee_asset": fee_asset,
            "basis": _basis_from_code(
                raw.get("basis"),
                templates=_PRIVATE_BASIS,
            ),
            "observed_at": observed,
            "valid_until": valid,
            "source_record_sha256": source_hash,
        }
        key = (
            row["profile_id"],
            row["venue"],
            row["instrument"],
            row["side"],
        )
        if key in keys:
            raise ValueError("private fee profile contains a duplicate fee key")
        keys.add(key)
        inventory.append(row)
    return inventory


def _load_public_fee_schedules(
    path: os.PathLike,
    *,
    now: str,
) -> List[Dict[str, str]]:
    schedule_path = Path(path)
    now_text = _validate_timestamp(now, "now")
    now_epoch = exact_rfc3339_epoch_seconds(now_text)
    try:
        with schedule_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != PUBLIC_FEE_SCHEDULE_COLUMNS:
                raise ValueError("public fee schedule columns are invalid")
            raw_rows = list(reader)
    except (OSError, UnicodeError, csv.Error):
        raise ValueError("public fee schedule could not be read") from None
    inventory: List[Dict[str, str]] = []
    keys = set()
    for raw in raw_rows:
        checked, valid = _validate_window(raw.get("checked_at"), raw.get("valid_until"))
        if exact_rfc3339_epoch_seconds(checked) > now_epoch:
            raise ValueError("public fee schedule check is in the future")
        if exact_rfc3339_epoch_seconds(valid) <= now_epoch:
            raise ValueError("public fee schedule contains a stale bound")
        lower = _decimal(raw.get("min_taker_fee_bps"), "min_taker_fee_bps")
        upper = _decimal(raw.get("max_taker_fee_bps"), "max_taker_fee_bps")
        if upper < lower:
            raise ValueError("public fee schedule bounds are reversed")
        pattern = _validated_text(
            raw.get("instrument_pattern"), "instrument_pattern", _PUBLIC_PATTERN
        )
        source_url = _required_text(raw.get("source_url"), "source_url")
        parsed = urlparse(source_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "public fee schedule source_url must be a public HTTPS URL"
            )
        fee_asset = _fee_asset(raw.get("fee_asset"), public=True)
        if fee_asset != "received_asset":
            raise ValueError(
                "public fee schedule fee_asset must be received_asset"
            )
        row = {
            "venue": _venue(raw.get("venue")),
            "instrument_pattern": pattern,
            "side": _side(raw.get("side"), allow_both=True),
            "min_taker_fee_bps": _decimal_text(lower),
            "max_taker_fee_bps": _decimal_text(upper),
            "fee_asset": fee_asset,
            "basis": _basis_from_code(
                raw.get("basis"),
                templates=_PUBLIC_BASIS,
            ),
            "checked_at": checked,
            "valid_until": valid,
            "source_url": source_url,
        }
        key = (row["venue"], pattern, row["side"])
        if key in keys:
            raise ValueError("public fee schedule contains a duplicate bound")
        keys.add(key)
        inventory.append(row)
    return inventory


def _terminal_component(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    value_status: str,
    reason_code: str,
    basis: str,
) -> Dict[str, Any]:
    return cost_component_row(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        direction="buy_token" if leg == "buy" else "sell_token",
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
        component_type="venue_taker_fee",
        value_status=value_status,
        amount_usd=None,
        rate_bps=None,
        basis=basis,
        strict_eligible=False,
        observed_at=None,
        valid_until=None,
        source="CEX fee evidence collector",
        source_record_sha256=None,
        reason_code=reason_code,
    )


def _safe_log(logger: Optional[Callable[[str], None]], message: str) -> None:
    if logger is not None:
        logger(message)


def _evidence_freshness(
    evidence: Mapping[str, str],
    *,
    now: str,
) -> Optional[Tuple[str, str, str]]:
    now_epoch = exact_rfc3339_epoch_seconds(_validate_timestamp(now, "now"))
    observed_epoch = exact_rfc3339_epoch_seconds(
        _validate_timestamp(evidence.get("observed_at"), "observed_at")
    )
    valid_epoch = exact_rfc3339_epoch_seconds(
        _validate_timestamp(evidence.get("valid_until"), "valid_until")
    )
    if observed_epoch > now_epoch:
        return (
            "failed",
            "cex_fee_observation_in_future",
            "authenticated CEX fee observation is after the requested snapshot",
        )
    if now_epoch >= valid_epoch:
        return (
            "stale",
            "cex_fee_evidence_expired",
            "authenticated CEX fee evidence is no longer valid",
        )
    return None


def _component_from_evidence(
    evidence: Mapping[str, str],
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    source: str,
) -> Dict[str, Any]:
    rate = _decimal(evidence.get("taker_fee_bps"), "taker_fee_bps")
    notional = _decimal(requested_notional_usd, "requested_notional_usd")
    amount = _exact_product(notional, rate, Decimal("0.0001"))
    return cost_component_row(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        direction="buy_token" if leg == "buy" else "sell_token",
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
        component_type="venue_taker_fee",
        value_status="authenticated",
        amount_usd=amount,
        rate_bps=rate,
        basis=evidence["basis"] + "; fee_asset=" + evidence["fee_asset"],
        strict_eligible=True,
        observed_at=evidence["observed_at"],
        valid_until=evidence["valid_until"],
        source=source,
        source_record_sha256=evidence["source_record_sha256"],
    )


def collect_cex_fee_snapshot(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    venue: str,
    instrument: str,
    side: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    now: str,
    client: Optional[Any] = None,
    private_profile_path: Optional[os.PathLike] = None,
    profile_id: Optional[str] = None,
    observed_at: Optional[str] = None,
    valid_until: Optional[str] = None,
    fee_asset: Optional[str] = None,
    discount_asset_funded: Optional[bool] = None,
    allow_public_estimate: bool = False,
    public_schedule_path: Optional[os.PathLike] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Collect one CEX fee component without accepting credential material.

    ``client`` is an already configured read-only wrapper exposing
    ``fetch_authenticated_fee(venue=..., instrument=...)``.  The collector
    never accepts, inspects, serializes, or logs client credentials.
    """
    canonical_venue = _venue(venue)
    market_venue, base, quote, canonical_instrument = _parse_cex_market_id(
        market_id
    )
    if market_venue != canonical_venue:
        raise ValueError("market_id venue must equal venue")
    supplied_base, supplied_quote, supplied_instrument = _canonical_pair(
        instrument
    )
    if (
        supplied_base != base
        or supplied_quote != quote
        or supplied_instrument != canonical_instrument
    ):
        raise ValueError("instrument must equal market_id BASE/QUOTE")
    trade_side = _side(side)
    now_text = _validate_timestamp(now, "now")
    received_asset = _received_asset(base, quote, trade_side)
    requested_fee_asset = None
    if fee_asset is not None:
        requested_fee_asset = _fee_asset(fee_asset)
        allowed_assets = {received_asset}
        if canonical_venue == "binance" and client is not None:
            allowed_assets.add("BNB")
        if requested_fee_asset not in allowed_assets:
            raise ValueError(
                "fee_asset must equal the received asset or proven Binance BNB"
            )
    native_instrument = _native_instrument(canonical_venue, base, quote)
    if leg not in ("buy", "sell") or leg != trade_side:
        raise ValueError("CEX fee leg must equal buy/sell side")
    if type(allow_public_estimate) is not bool:
        raise ValueError("allow_public_estimate must be boolean")
    if client is not None and private_profile_path is not None:
        raise ValueError("select exactly one authenticated fee source")

    if client is not None:
        try:
            response = client.fetch_authenticated_fee(
                venue=canonical_venue,
                instrument=native_instrument,
            )
            if canonical_venue == "binance":
                evidence = normalize_binance_taker_fee(
                    response,
                    side=trade_side,
                    profile_id=profile_id,
                    observed_at=observed_at or now_text,
                    valid_until=valid_until,
                    discount_asset_funded=discount_asset_funded,
                    received_asset=received_asset,
                )
            elif canonical_venue == "bybit":
                evidence = normalize_bybit_taker_fee(
                    response,
                    instrument=native_instrument,
                    side=trade_side,
                    fee_asset=received_asset,
                    profile_id=profile_id,
                    valid_until=valid_until,
                )
            elif canonical_venue == "okx":
                evidence = normalize_okx_taker_fee(
                    response,
                    instrument=native_instrument,
                    side=trade_side,
                    fee_asset=received_asset,
                    profile_id=profile_id,
                    valid_until=valid_until,
                )
            else:
                raise ValueError("authenticated venue adapter is unsupported")
        except Exception:
            row = _terminal_component(
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                value_status="failed",
                reason_code="cex_fee_authenticated_fetch_failed",
                basis="authenticated CEX fee evidence could not be normalized",
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status=failed reason={}"
                .format(canonical_venue, row["reason_code"]),
            )
            return row

        if (
            evidence["venue"] != canonical_venue
            or evidence["instrument"] != native_instrument
            or evidence["side"] != trade_side
        ):
            row = _terminal_component(
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                value_status="failed",
                reason_code="cex_fee_evidence_identity_mismatch",
                basis="authenticated CEX fee evidence identity does not match request",
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status=failed reason={}".format(
                    canonical_venue, row["reason_code"]
                ),
            )
            return row
        allowed_evidence_assets = {received_asset}
        if canonical_venue == "binance":
            allowed_evidence_assets.add("BNB")
        if (
            evidence["fee_asset"] not in allowed_evidence_assets
            or (
                requested_fee_asset is not None
                and evidence["fee_asset"] != requested_fee_asset
            )
        ):
            row = _terminal_component(
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                value_status="failed",
                reason_code="cex_fee_asset_evidence_mismatch",
                basis="authenticated fee asset evidence does not match the trade",
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status=failed reason={}".format(
                    canonical_venue, row["reason_code"]
                ),
            )
            return row
        freshness = _evidence_freshness(evidence, now=now_text)
        if freshness is not None:
            status, reason_code, basis = freshness
            row = _terminal_component(
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                value_status=status,
                reason_code=reason_code,
                basis=basis,
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status={} reason={}".format(
                    canonical_venue, status, reason_code
                ),
            )
            return row
        row = _component_from_evidence(
            evidence,
            cohort_id=cohort_id,
            opportunity_id=opportunity_id,
            leg=leg,
            market_id=market_id,
            requested_notional_usd=requested_notional_usd,
            target_token_quantity=target_token_quantity,
            source="redacted authenticated {} fee response".format(
                canonical_venue
            ),
        )
        _safe_log(
            logger,
            "cex_fee_snapshot venue={} status=authenticated".format(
                canonical_venue
            ),
        )
        return row

    if private_profile_path is not None:
        opaque_id = _profile_id(profile_id)
        rows = load_validated_fee_profile(private_profile_path, now=now)
        matches = [
            row
            for row in rows
            if row["profile_id"] == opaque_id
            and row["venue"] == canonical_venue
            and row["instrument"] == canonical_instrument
            and row["side"] == trade_side
        ]
        if len(matches) == 1:
            freshness = _evidence_freshness(matches[0], now=now_text)
            if freshness is not None:
                status, reason_code, basis = freshness
                return _terminal_component(
                    cohort_id=cohort_id,
                    opportunity_id=opportunity_id,
                    leg=leg,
                    market_id=market_id,
                    requested_notional_usd=requested_notional_usd,
                    target_token_quantity=target_token_quantity,
                    value_status=status,
                    reason_code=reason_code,
                    basis=basis,
                )
            result = _component_from_evidence(
                matches[0],
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                source="validated private CEX fee profile (redacted)",
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status=authenticated".format(
                    canonical_venue
                ),
            )
            return result
        row = _terminal_component(
            cohort_id=cohort_id,
            opportunity_id=opportunity_id,
            leg=leg,
            market_id=market_id,
            requested_notional_usd=requested_notional_usd,
            target_token_quantity=target_token_quantity,
            value_status="unavailable",
            reason_code="cex_fee_profile_record_missing",
            basis="validated private fee profile has no exact market-side record",
        )
        _safe_log(
            logger,
            "cex_fee_snapshot venue={} status=unavailable reason={}"
            .format(canonical_venue, row["reason_code"]),
        )
        return row

    if allow_public_estimate:
        schedule = public_schedule_path
        if schedule is None:
            schedule = (
                Path(__file__).resolve().parents[1]
                / "config"
                / "cex_public_fee_schedules.csv"
            )
        schedules = _load_public_fee_schedules(schedule, now=now)
        matches = [
            row
            for row in schedules
            if row["venue"] == canonical_venue
            and row["side"] in (trade_side, "both")
            and fnmatch.fnmatchcase(
                canonical_instrument, row["instrument_pattern"]
            )
        ]
        if len(matches) > 1:
            raise ValueError("public fee schedule has ambiguous matching bounds")
        if len(matches) == 1:
            bound = matches[0]
            rate = _decimal(bound["max_taker_fee_bps"], "max_taker_fee_bps")
            notional = _decimal(requested_notional_usd, "requested_notional_usd")
            amount = _exact_product(notional, rate, Decimal("0.0001"))
            range_basis = (
                "{}; public interval [{},{}] bps; conservative upper bound"
                " projected; fee_asset={}"
            ).format(
                bound["basis"],
                bound["min_taker_fee_bps"],
                bound["max_taker_fee_bps"],
                received_asset,
            )
            source_hash = _canonical_sha256(bound)
            row = cost_component_row(
                cohort_id=cohort_id,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                direction="buy_token" if leg == "buy" else "sell_token",
                requested_notional_usd=requested_notional_usd,
                target_token_quantity=target_token_quantity,
                component_type="venue_taker_fee",
                value_status="bounded_estimate",
                amount_usd=amount,
                rate_bps=rate,
                basis=range_basis,
                strict_eligible=False,
                observed_at=bound["checked_at"],
                valid_until=bound["valid_until"],
                source=bound["source_url"],
                source_record_sha256=source_hash,
            )
            _safe_log(
                logger,
                "cex_fee_snapshot venue={} status=bounded_estimate".format(
                    canonical_venue
                ),
            )
            return row
        row = _terminal_component(
            cohort_id=cohort_id,
            opportunity_id=opportunity_id,
            leg=leg,
            market_id=market_id,
            requested_notional_usd=requested_notional_usd,
            target_token_quantity=target_token_quantity,
            value_status="unavailable",
            reason_code="cex_fee_public_bound_unavailable",
            basis="no current public fee bound matches the exact venue and instrument",
        )
        _safe_log(
            logger,
            "cex_fee_snapshot venue={} status=unavailable reason={}"
            .format(canonical_venue, row["reason_code"]),
        )
        return row

    row = _terminal_component(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
        value_status="unavailable",
        reason_code="cex_fee_authentication_missing",
        basis="no authenticated CEX fee source was configured",
    )
    _safe_log(
        logger,
        "cex_fee_snapshot venue={} status=unavailable reason={}"
        .format(canonical_venue, row["reason_code"]),
    )
    return row
