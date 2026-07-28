"""Shared long-form fixed-notional execution-cost fact contract.

One row represents one market, one direction, and one requested USD notional.
The requested notional defines a target Token quantity at the snapshot's
pre-trade reference price.  ``sell_token`` is exact-input Token and
``buy_token`` is exact-output Token, so both directions compare the same Token
quantity.

Only a fully filled request publishes VWAP and quoted execution cost.  Partial
rows retain observed fill and quote facts but never interpolate a full-trade
cost from the 10/25/50/100 bps depth bands.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from math import ceil
import re
from typing import Any, Iterable


EXECUTION_COST_CONTRACT_VERSION = "1"
EXECUTION_NOTIONALS_USD = (
    Decimal("1000"),
    Decimal("5000"),
    Decimal("10000"),
    Decimal("50000"),
    Decimal("100000"),
)
EXECUTION_DIRECTIONS = ("sell_token", "buy_token")
EXECUTION_STATUSES = {"observed", "partial", "unsupported", "failed"}
USD_PRICE_SKEW_WARNING_SECONDS = 15 * 60
USD_PRICE_SKEW_MAX_SECONDS = 2 * 60 * 60
NOTIONAL_DEFINITION = (
    "target Token quantity valued at the snapshot pre-trade reference price"
)
MEASURED_PROVENANCE_COLUMNS = (
    "state_observed_at",
    "reference_price_method",
    "fee_status",
    "usd_conversion_status",
    "excluded_costs",
    "source",
    "source_endpoint",
    "raw_response_sha256",
)
DEX_MEASURED_PROVENANCE_COLUMNS = (
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "target_token_decimals",
    "quote_token_address",
    "quote_token_decimals",
    "fee_rate_bps",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
)

EXECUTION_COST_COLUMNS = (
    "snapshot_id",
    "source_snapshot_id",
    "contract_version",
    "calculation_method",
    "observed_at",
    "state_observed_at",
    "request_started_at",
    "response_received_at",
    "market_id",
    "market_type",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "chain",
    "dex",
    "pool_address",
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "target_token_decimals",
    "quote_token_address",
    "quote_token_decimals",
    "direction",
    "requested_notional_usd",
    "notional_definition",
    "reference_price_method",
    "reference_price_quote_per_token",
    "quote_to_usd",
    "reference_price_usd_per_token",
    "reference_notional_usd",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "target_token_quantity",
    "filled_token_quantity",
    "fill_ratio",
    "quote_amount",
    "quote_amount_usd",
    "filled_vwap_quote_per_token",
    "filled_vwap_usd_per_token",
    "quoted_execution_cost_usd",
    "quoted_execution_cost_bps",
    "levels_or_ticks_consumed",
    "ending_marginal_price_quote_per_token",
    "fee_status",
    "fee_rate_bps",
    "fee_amount_usd",
    "usd_conversion_status",
    "excluded_costs",
    "status",
    "status_reason",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
    "error",
)

RESULT_NUMERIC_COLUMNS = (
    "reference_price_quote_per_token",
    "quote_to_usd",
    "reference_price_usd_per_token",
    "reference_notional_usd",
    "target_token_quantity",
    "filled_token_quantity",
    "fill_ratio",
    "quote_amount",
    "quote_amount_usd",
    "filled_vwap_quote_per_token",
    "filled_vwap_usd_per_token",
    "quoted_execution_cost_usd",
    "quoted_execution_cost_bps",
    "levels_or_ticks_consumed",
    "ending_marginal_price_quote_per_token",
    "fee_rate_bps",
    "fee_amount_usd",
)

MARKET_LINEAGE_COLUMNS = (
    "snapshot_id",
    "source_snapshot_id",
    "calculation_method",
    "observed_at",
    "state_observed_at",
    "request_started_at",
    "response_received_at",
    "market_id",
    "market_type",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "chain",
    "dex",
    "pool_address",
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "target_token_decimals",
    "quote_token_address",
    "quote_token_decimals",
    "reference_price_method",
    "reference_price_quote_per_token",
    "quote_to_usd",
    "reference_price_usd_per_token",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "fee_status",
    "fee_rate_bps",
    "usd_conversion_status",
    "excluded_costs",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
)

REQUIRED_IDENTITY_COLUMNS = (
    "snapshot_id",
    "source_snapshot_id",
    "calculation_method",
    "observed_at",
    "market_id",
    "market_type",
    "token_symbol",
)


def finite_decimal(value: Any, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError(f"Invalid execution-cost numeric value: {value}") from error
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ValueError(f"Invalid execution-cost numeric value: {value}")
    return number


def optional_decimal(value: Any) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    return finite_decimal(value)


def decimal_text(value: Decimal | int | str | None) -> str:
    if value is None:
        return ""
    number = finite_decimal(value)
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def usd_price_timing(
    state_observed_at: str | None,
    usd_price_observed_at: str | None,
    *,
    warning_seconds: int = USD_PRICE_SKEW_WARNING_SECONDS,
    max_seconds: int = USD_PRICE_SKEW_MAX_SECONDS,
) -> dict[str, Any]:
    """Classify the observable skew between market state and a USD-price response.

    GeckoTerminal exposes the time at which this project received the pool
    response, not the provider's internal price-event time.  The result is
    therefore an observation-skew contract, not a claim about tick-level price
    age.
    """
    result: dict[str, Any] = {
        "state_observed_at": state_observed_at or None,
        "usd_price_observed_at": usd_price_observed_at or None,
        "skew_seconds": None,
        "status": "unavailable",
        "usable": False,
        "warning_seconds": warning_seconds,
        "max_seconds": max_seconds,
        "reason": "usd_price_timing_unavailable",
    }
    if (
        warning_seconds < 0
        or max_seconds <= 0
        or warning_seconds > max_seconds
    ):
        raise ValueError("USD price timing thresholds are invalid")
    if not state_observed_at or not usd_price_observed_at:
        return result

    def parse_timestamp(value: str) -> Decimal:
        matched = re.fullmatch(
            r"(?P<prefix>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
            r"(?:\.(?P<fraction>\d+))?"
            r"(?P<offset>Z|[+-]\d{2}:\d{2})",
            value.strip(),
        )
        if matched is None:
            raise ValueError("timestamp must be RFC 3339 with a timezone")
        offset = (
            "+00:00"
            if matched.group("offset") == "Z"
            else matched.group("offset")
        )
        parsed = datetime.fromisoformat(f"{matched.group('prefix')}{offset}")
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        utc_value = parsed.astimezone(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = utc_value - epoch
        whole_seconds = Decimal(delta.days * 86400 + delta.seconds)
        fraction = matched.group("fraction")
        if fraction:
            whole_seconds += Decimal(f"0.{fraction}")
        return whole_seconds

    try:
        state_time = parse_timestamp(state_observed_at)
        price_time = parse_timestamp(usd_price_observed_at)
    except (TypeError, ValueError):
        result["reason"] = "usd_price_timestamp_invalid"
        return result

    skew_seconds = int(ceil(abs(state_time - price_time)))
    result["skew_seconds"] = skew_seconds
    if skew_seconds <= warning_seconds:
        result.update(
            {
                "status": "current",
                "usable": True,
                "reason": None,
            }
        )
    elif skew_seconds <= max_seconds:
        result.update(
            {
                "status": "warning",
                "usable": True,
                "reason": "usd_price_observation_skew_warning",
            }
        )
    else:
        result.update(
            {
                "status": "stale",
                "reason": "usd_price_observation_skew_exceeds_maximum",
            }
        )
    return result


def blank_execution_row() -> dict[str, str]:
    return {column: "" for column in EXECUTION_COST_COLUMNS}


def execution_fact_row(
    *,
    common: dict[str, Any],
    direction: str,
    requested_notional_usd: Decimal | int | str,
    status: str,
    status_reason: str,
    reference_price_quote_per_token: Decimal | int | str | None = None,
    quote_to_usd: Decimal | int | str | None = None,
    target_token_quantity: Decimal | int | str | None = None,
    filled_token_quantity: Decimal | int | str | None = None,
    quote_amount: Decimal | int | str | None = None,
    levels_or_ticks_consumed: int | str | None = None,
    ending_marginal_price_quote_per_token: Decimal | int | str | None = None,
    fee_amount_usd: Decimal | int | str | None = None,
    error: str = "",
) -> dict[str, str]:
    """Build and calculate one normalized execution-cost fact row."""
    normalized_direction = direction.strip().lower()
    if normalized_direction not in EXECUTION_DIRECTIONS:
        raise ValueError(f"Unknown execution direction: {direction}")
    normalized_status = status.strip().lower()
    if normalized_status not in EXECUTION_STATUSES:
        raise ValueError(f"Unknown execution status: {status}")
    if not status_reason:
        raise ValueError("Execution status_reason is required")
    requested = finite_decimal(requested_notional_usd, positive=True)
    if requested not in EXECUTION_NOTIONALS_USD:
        raise ValueError(f"Unsupported execution notional: {requested}")

    row = blank_execution_row()
    row.update(
        {
            key: str(value)
            for key, value in common.items()
            if key in row and value is not None
        }
    )
    row.update(
        {
            "contract_version": EXECUTION_COST_CONTRACT_VERSION,
            "direction": normalized_direction,
            "requested_notional_usd": decimal_text(requested),
            "notional_definition": NOTIONAL_DEFINITION,
            "status": normalized_status,
            "status_reason": status_reason,
            "error": error,
        }
    )
    if normalized_status in {"unsupported", "failed"}:
        return row

    reference_quote = finite_decimal(
        reference_price_quote_per_token,
        positive=True,
    )
    conversion = finite_decimal(quote_to_usd, positive=True)
    target = finite_decimal(target_token_quantity, positive=True)
    filled = (
        finite_decimal(filled_token_quantity)
        if filled_token_quantity is not None
        else None
    )
    quote = finite_decimal(quote_amount) if quote_amount is not None else None
    if filled is not None and filled > target:
        tolerance = max(Decimal("1e-24"), target * Decimal("1e-18"))
        if filled - target > tolerance:
            raise ValueError("Filled Token quantity exceeds requested quantity")
        filled = target
    if normalized_status == "observed":
        if filled is None or quote is None:
            raise ValueError("Observed execution requires fill and quote facts")
        if quote <= 0:
            raise ValueError("Observed execution requires a positive quote amount")
        tolerance = max(Decimal("1e-24"), target * Decimal("1e-18"))
        if abs(filled - target) > tolerance:
            raise ValueError("Observed execution did not fill requested quantity")
        filled = target
    else:
        if (filled is None) != (quote is None):
            raise ValueError(
                "Partial execution fill and quote facts must be present together"
            )
        if filled is not None and filled >= target:
            raise ValueError("Partial execution must have fill_ratio below one")

    with localcontext() as context:
        context.prec = 100
        reference_usd = reference_quote * conversion
        reference_notional = target * reference_usd
        row.update(
            {
                "reference_price_quote_per_token": decimal_text(reference_quote),
                "quote_to_usd": decimal_text(conversion),
                "reference_price_usd_per_token": decimal_text(reference_usd),
                "reference_notional_usd": decimal_text(reference_notional),
                "target_token_quantity": decimal_text(target),
            }
        )
        if filled is not None:
            row["filled_token_quantity"] = decimal_text(filled)
            row["fill_ratio"] = decimal_text(filled / target)
        if quote is not None:
            row["quote_amount"] = decimal_text(quote)
            row["quote_amount_usd"] = decimal_text(quote * conversion)
        if levels_or_ticks_consumed is not None:
            consumed = finite_decimal(levels_or_ticks_consumed)
            if consumed != consumed.to_integral_value():
                raise ValueError("levels_or_ticks_consumed must be an integer")
            row["levels_or_ticks_consumed"] = str(int(consumed))
        if ending_marginal_price_quote_per_token is not None:
            row["ending_marginal_price_quote_per_token"] = decimal_text(
                finite_decimal(
                    ending_marginal_price_quote_per_token,
                    positive=True,
                )
            )
        if fee_amount_usd is not None:
            row["fee_amount_usd"] = decimal_text(
                finite_decimal(fee_amount_usd)
            )

        if normalized_status == "observed":
            assert quote is not None
            quote_usd = quote * conversion
            vwap_quote = quote / target
            cost = (
                reference_notional - quote_usd
                if normalized_direction == "sell_token"
                else quote_usd - reference_notional
            )
            tolerance = max(
                Decimal("1e-18"),
                reference_notional * Decimal("1e-18"),
            )
            if cost < -tolerance:
                raise ValueError(
                    "Execution improves beyond the pre-trade reference price"
                )
            if cost < 0:
                cost = Decimal(0)
            row.update(
                {
                    "fill_ratio": "1",
                    "filled_vwap_quote_per_token": decimal_text(vwap_quote),
                    "filled_vwap_usd_per_token": decimal_text(
                        vwap_quote * conversion
                    ),
                    "quoted_execution_cost_usd": decimal_text(cost),
                    "quoted_execution_cost_bps": decimal_text(
                        cost / reference_notional * Decimal(10_000)
                    ),
                }
            )
    return row


def status_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    values = list(rows)
    return {
        status: sum(row.get("status") == status for row in values)
        for status in ("observed", "partial", "unsupported", "failed")
    }


def _assert_close(
    actual: Decimal | None,
    expected: Decimal,
    *,
    label: str,
) -> None:
    if actual is None:
        raise ValueError(f"{label} is missing")
    tolerance = max(Decimal("1e-18"), abs(expected) * Decimal("1e-16"))
    if abs(actual - expected) > tolerance:
        raise ValueError(f"{label} does not match its source formula")


def _base_unit_decimals(value: Any, *, label: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    decimals = int(text)
    if not 0 <= decimals <= 255:
        raise ValueError(f"{label} is outside [0, 255]")
    return decimals


def _assert_base_unit_aligned(
    value: Decimal | None,
    decimals: int | None,
    *,
    label: str,
) -> None:
    if value is None or decimals is None:
        return
    raw_value = Fraction(value) * (10**decimals)
    if raw_value.denominator != 1:
        raise ValueError(
            f"{label} is not an integer number of base units"
        )


def _expected_market_id(row: dict[str, Any]) -> str:
    market_type = str(row.get("market_type") or "").strip().lower()
    token = str(row.get("token_symbol") or "").strip().upper()
    if market_type == "cex":
        exchange = str(row.get("exchange") or "").strip().lower()
        symbol = str(row.get("cex_symbol") or "").strip().upper()
        if not exchange or not symbol:
            raise ValueError("CEX execution row lacks exchange identity")
        symbol_parts = symbol.split("/", 1)
        base_asset = str(row.get("base_asset") or "").strip().upper()
        if (
            len(symbol_parts) != 2
            or not symbol_parts[0]
            or token != symbol_parts[0]
            or base_asset != symbol_parts[0]
        ):
            raise ValueError(
                "CEX execution Token identity does not match cex_symbol/base_asset"
            )
        return f"cex:{exchange}:{symbol}"
    if market_type == "dex":
        chain = str(row.get("chain") or "").strip().lower()
        dex = str(row.get("dex") or "").strip().lower()
        address = str(row.get("pool_address") or "").strip()
        if address.startswith("0x"):
            address = address.lower()
        if not chain or not dex or not address:
            raise ValueError("DEX execution row lacks pool identity")
        return f"dex:{chain}:{dex}:{address}:{token}"
    raise ValueError(f"Execution row has invalid market_type: {market_type}")


def validate_execution_snapshot(
    expected_market_ids: Iterable[str],
    rows: list[dict[str, Any]],
    *,
    enforce_usd_price_timing: bool = False,
) -> None:
    """Apply the hard gates under a precision independent of process defaults."""
    with localcontext() as context:
        context.prec = 100
        _validate_execution_snapshot(
            expected_market_ids,
            rows,
            enforce_usd_price_timing=enforce_usd_price_timing,
        )


def _validate_execution_snapshot(
    expected_market_ids: Iterable[str],
    rows: list[dict[str, Any]],
    *,
    enforce_usd_price_timing: bool = False,
) -> None:
    """Implementation for coverage, formula, state, and monotonicity gates."""
    expected = set(expected_market_ids)
    if not expected:
        raise ValueError("Execution inventory is empty")
    keys = [
        (
            row.get("snapshot_id"),
            row.get("market_id"),
            row.get("direction"),
            row.get("requested_notional_usd"),
        )
        for row in rows
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("Execution snapshot contains duplicate scenario rows")
    actual_markets = {str(row.get("market_id") or "") for row in rows}
    if actual_markets != expected:
        raise ValueError("Execution snapshot coverage does not match inventory")

    expected_count = len(expected) * len(EXECUTION_DIRECTIONS) * len(
        EXECUTION_NOTIONALS_USD
    )
    if len(rows) != expected_count:
        raise ValueError(
            f"Execution snapshot has {len(rows)} rows; expected {expected_count}"
        )
    snapshot_ids = {str(row.get("snapshot_id") or "") for row in rows}
    source_snapshot_ids = {
        str(row.get("source_snapshot_id") or "") for row in rows
    }
    if "" in snapshot_ids or len(snapshot_ids) != 1:
        raise ValueError("Execution file must contain one non-empty snapshot_id")
    if "" in source_snapshot_ids or len(source_snapshot_ids) != 1:
        raise ValueError(
            "Execution file must contain one non-empty source_snapshot_id"
        )

    by_market_direction: dict[
        tuple[str, str],
        list[dict[str, Any]],
    ] = defaultdict(list)
    for row in rows:
        missing_identity = [
            column
            for column in REQUIRED_IDENTITY_COLUMNS
            if not str(row.get(column) or "").strip()
        ]
        if missing_identity:
            raise ValueError(
                "Execution row lacks required identity fields: "
                + ", ".join(missing_identity)
            )
        expected_row_market_id = _expected_market_id(row)
        if str(row.get("market_id") or "") != expected_row_market_id:
            raise ValueError(
                "Execution row market_id does not match its retained identity"
            )
        status = str(row.get("status") or "")
        if status not in EXECUTION_STATUSES:
            raise ValueError(f"Execution row has invalid status: {status or 'missing'}")
        direction = str(row.get("direction") or "")
        if direction not in EXECUTION_DIRECTIONS:
            raise ValueError(f"Execution row has invalid direction: {direction}")
        if str(row.get("contract_version") or "") != EXECUTION_COST_CONTRACT_VERSION:
            raise ValueError("Execution row has wrong contract version")
        if str(row.get("notional_definition") or "") != NOTIONAL_DEFINITION:
            raise ValueError("Execution row has wrong notional definition")
        if not row.get("status_reason"):
            raise ValueError("Execution row lacks status_reason")
        if status in {"observed", "partial"}:
            missing_provenance = [
                column
                for column in MEASURED_PROVENANCE_COLUMNS
                if not str(row.get(column) or "").strip()
            ]
            if missing_provenance:
                raise ValueError(
                    "Measured execution row lacks provenance fields: "
                    + ", ".join(missing_provenance)
                )
            raw_hash = str(row.get("raw_response_sha256") or "")
            if (
                len(raw_hash) != 64
                or any(character not in "0123456789abcdef" for character in raw_hash)
            ):
                raise ValueError(
                    "Measured execution row has invalid raw_response_sha256"
                )
            if str(row.get("market_type") or "") == "dex":
                missing_dex_provenance = [
                    column
                    for column in DEX_MEASURED_PROVENANCE_COLUMNS
                    if not str(row.get(column) or "").strip()
                ]
                if missing_dex_provenance:
                    raise ValueError(
                        "Measured DEX execution row lacks fixed-block provenance: "
                        + ", ".join(missing_dex_provenance)
                    )
                block_number = str(row.get("block_number") or "")
                if not block_number.isdigit() or int(block_number) <= 0:
                    raise ValueError(
                        "Measured DEX execution row has invalid block_number"
                    )
                if row.get("state_observed_at") != row.get("block_timestamp"):
                    raise ValueError(
                        "Measured DEX state_observed_at does not match block_timestamp"
                    )
                if str(row.get("source_sequence") or "") != block_number:
                    raise ValueError(
                        "Measured DEX source_sequence does not match block_number"
                    )
                if enforce_usd_price_timing:
                    timing = usd_price_timing(
                        str(row.get("state_observed_at") or ""),
                        str(row.get("usd_price_observed_at") or ""),
                    )
                    if not timing["usable"]:
                        raise ValueError(
                            "Measured DEX execution uses an unavailable or stale "
                            f"USD price observation: {timing['reason']}"
                        )
                fee_rate = optional_decimal(row.get("fee_rate_bps"))
                if (
                    fee_rate is None
                    or fee_rate != fee_rate.to_integral_value()
                    or fee_rate >= Decimal(10_000)
                ):
                    raise ValueError(
                        "Measured DEX execution row has invalid fee_rate_bps"
                    )
        by_market_direction[(str(row["market_id"]), direction)].append(row)

    for market_id in expected:
        market_rows = [row for row in rows if row.get("market_id") == market_id]
        lineage_values = {
            tuple(row.get(column) for column in MARKET_LINEAGE_COLUMNS)
            for row in market_rows
        }
        if len(lineage_values) != 1:
            raise ValueError(f"{market_id} scenarios mix source lineage")

    for (market_id, direction), scenarios in by_market_direction.items():
        scenarios.sort(
            key=lambda row: finite_decimal(
                row["requested_notional_usd"],
                positive=True,
            )
        )
        notionals = tuple(
            finite_decimal(row["requested_notional_usd"], positive=True)
            for row in scenarios
        )
        if notionals != EXECUTION_NOTIONALS_USD:
            raise ValueError(f"{market_id} {direction} has wrong notional set")
        statuses = {str(row["status"]) for row in scenarios}
        if statuses & {"unsupported", "failed"}:
            if len(statuses) != 1:
                raise ValueError(
                    f"{market_id} {direction} mixes terminal and measured statuses"
                )
            for row in scenarios:
                if any(row.get(field) not in (None, "") for field in RESULT_NUMERIC_COLUMNS):
                    raise ValueError(
                        f"{market_id} {direction} terminal row contains numeric facts"
                    )
            continue

        partial_seen = False
        previous_fill_ratio: Decimal | None = None
        previous_cost_bps: Decimal | None = None
        previous_vwap: Decimal | None = None
        previous_reference_notional: Decimal | None = None
        previous_target: Decimal | None = None
        for row in scenarios:
            status = str(row["status"])
            if status not in {"observed", "partial"}:
                raise ValueError(f"{market_id} {direction} has mixed status family")
            reference_quote = optional_decimal(
                row.get("reference_price_quote_per_token")
            )
            conversion = optional_decimal(row.get("quote_to_usd"))
            reference_usd = optional_decimal(
                row.get("reference_price_usd_per_token")
            )
            reference_notional = optional_decimal(
                row.get("reference_notional_usd")
            )
            target = optional_decimal(row.get("target_token_quantity"))
            filled = optional_decimal(row.get("filled_token_quantity"))
            fill_ratio = optional_decimal(row.get("fill_ratio"))
            quote = optional_decimal(row.get("quote_amount"))
            quote_usd = optional_decimal(row.get("quote_amount_usd"))
            if (
                reference_quote is None
                or conversion is None
                or target is None
            ):
                raise ValueError(f"{market_id} {direction} lacks reference facts")
            requested = finite_decimal(
                row["requested_notional_usd"],
                positive=True,
            )
            theoretical_target = requested / (reference_quote * conversion)
            target_decimals = _base_unit_decimals(
                row.get("target_token_decimals"),
                label="target_token_decimals",
            )
            quote_decimals = _base_unit_decimals(
                row.get("quote_token_decimals"),
                label="quote_token_decimals",
            )
            if target_decimals is not None:
                _assert_base_unit_aligned(
                    target,
                    target_decimals,
                    label="target_token_quantity",
                )
                target_fraction = Fraction(target)
                theoretical_fraction = Fraction(requested) / (
                    Fraction(reference_quote) * Fraction(conversion)
                )
                unit_fraction = Fraction(1, 10**target_decimals)
                if not (
                    target_fraction
                    <= theoretical_fraction
                    < target_fraction + unit_fraction
                ):
                    raise ValueError(
                        "Quantized target Token quantity is not a one-unit floor"
                    )
            else:
                _assert_close(
                    target,
                    theoretical_target,
                    label="target_token_quantity",
                )
            _assert_base_unit_aligned(
                filled,
                target_decimals,
                label="filled_token_quantity",
            )
            _assert_base_unit_aligned(
                quote,
                quote_decimals,
                label="quote_amount",
            )
            _assert_close(
                reference_usd,
                reference_quote * conversion,
                label="reference_price_usd_per_token",
            )
            _assert_close(
                reference_notional,
                target * reference_quote * conversion,
                label="reference_notional_usd",
            )
            if filled is not None:
                _assert_close(
                    fill_ratio,
                    filled / target,
                    label="fill_ratio",
                )
                if fill_ratio is not None and not Decimal(0) <= fill_ratio <= Decimal(1):
                    raise ValueError("fill_ratio is outside [0, 1]")
            if quote is not None:
                _assert_close(
                    quote_usd,
                    quote * conversion,
                    label="quote_amount_usd",
                )
            if (filled is None) != (quote is None):
                raise ValueError(
                    "Partial execution fill and quote facts must be present together"
                )
            if filled is None and (
                fill_ratio is not None or quote_usd is not None
            ):
                raise ValueError(
                    "Execution row has dependent fill facts without fill and quote"
                )

            if status == "partial":
                partial_seen = True
                if fill_ratio is not None and fill_ratio >= 1:
                    raise ValueError("Partial execution has fill_ratio >= 1")
                for field in (
                    "filled_vwap_quote_per_token",
                    "filled_vwap_usd_per_token",
                    "quoted_execution_cost_usd",
                    "quoted_execution_cost_bps",
                ):
                    if row.get(field) not in (None, ""):
                        raise ValueError(
                            f"Partial execution contains complete field {field}"
                        )
            else:
                if partial_seen:
                    raise ValueError(
                        f"{market_id} {direction} becomes observed after partial"
                    )
                if fill_ratio != Decimal(1) or filled is None or quote is None:
                    raise ValueError("Observed execution is not fully filled")
                if quote <= 0:
                    raise ValueError(
                        "Observed execution requires a positive quote amount"
                    )
                vwap_quote = optional_decimal(
                    row.get("filled_vwap_quote_per_token")
                )
                vwap_usd = optional_decimal(
                    row.get("filled_vwap_usd_per_token")
                )
                cost_usd = optional_decimal(
                    row.get("quoted_execution_cost_usd")
                )
                cost_bps = optional_decimal(
                    row.get("quoted_execution_cost_bps")
                )
                _assert_close(vwap_quote, quote / target, label="filled_vwap")
                assert vwap_quote is not None
                _assert_close(
                    vwap_usd,
                    vwap_quote * conversion,
                    label="filled_vwap_usd",
                )
                assert reference_notional is not None
                assert quote_usd is not None
                expected_cost = (
                    reference_notional - quote_usd
                    if direction == "sell_token"
                    else quote_usd - reference_notional
                )
                expected_cost_tolerance = max(
                    Decimal("1e-18"),
                    reference_notional * Decimal("1e-18"),
                )
                if expected_cost < -expected_cost_tolerance:
                    raise ValueError(
                        "Execution improves beyond the pre-trade reference price"
                    )
                if expected_cost < 0:
                    expected_cost = Decimal(0)
                _assert_close(
                    cost_usd,
                    expected_cost,
                    label="quoted_execution_cost_usd",
                )
                assert cost_usd is not None
                _assert_close(
                    cost_bps,
                    cost_usd / reference_notional * Decimal(10_000),
                    label="quoted_execution_cost_bps",
                )
                cost_rounding_tolerance = Decimal("1e-16")
                vwap_rounding_tolerance = Decimal("1e-16")
                if (
                    str(row.get("market_type") or "") == "dex"
                    and quote_decimals is not None
                    and previous_reference_notional is not None
                    and previous_target is not None
                ):
                    quote_unit = Decimal(10) ** -quote_decimals
                    # DEX sell outputs are floored and buy inputs are ceiled, so
                    # reported cost is biased upward by less than one quote
                    # base unit. Only the previous scenario's upward bias can
                    # make a larger notional appear to improve.
                    cost_rounding_tolerance = max(
                        cost_rounding_tolerance,
                        quote_unit
                        * conversion
                        * Decimal(10_000)
                        / previous_reference_notional,
                    )
                    vwap_rounding_tolerance = max(
                        vwap_rounding_tolerance,
                        quote_unit / previous_target,
                    )
                if (
                    previous_cost_bps is not None
                    and cost_bps is not None
                    and cost_bps + cost_rounding_tolerance < previous_cost_bps
                ):
                    raise ValueError("Observed execution cost decreases with notional")
                if previous_vwap is not None and vwap_quote is not None:
                    if (
                        direction == "sell_token"
                        and vwap_quote
                        > previous_vwap + vwap_rounding_tolerance
                    ):
                        raise ValueError("Sell VWAP improves with larger notional")
                    if (
                        direction == "buy_token"
                        and vwap_quote + vwap_rounding_tolerance < previous_vwap
                    ):
                        raise ValueError("Buy VWAP improves with larger notional")
                previous_cost_bps = cost_bps
                previous_vwap = vwap_quote
                previous_reference_notional = reference_notional
                previous_target = target

            if (
                previous_fill_ratio is not None
                and fill_ratio is not None
                and fill_ratio > previous_fill_ratio + Decimal("1e-16")
            ):
                raise ValueError("Fill ratio increases with notional")
            if fill_ratio is not None:
                previous_fill_ratio = fill_ratio


def execution_api_rows(
    rows: Iterable[dict[str, Any]],
    *,
    number_parser,
) -> list[dict[str, Any]]:
    """Normalize rows without losing exact decimal facts through JSON floats.

    The configured requested notionals are safe JSON numbers.  Every measured
    Decimal remains a base-10 string so a client can reconstruct Token base
    units and recompute formulas without IEEE-754 precision loss.
    """
    result = []
    for row in rows:
        item = {}
        for column in EXECUTION_COST_COLUMNS:
            value = row.get(column)
            if column == "requested_notional_usd":
                item[column] = number_parser(value)
            elif column in RESULT_NUMERIC_COLUMNS:
                item[column] = None if value in (None, "") else str(value)
            else:
                item[column] = value or None
        result.append(item)
    return result
