"""Collect auditable point-in-time CEX order-book depth snapshots.

Depth is computed from public spot order books, never inferred from daily
volume.  For each published CEX market the collector stores:

- best bid/ask, midpoint, and quoted spread;
- bid and ask quote notional inside 10/25/50/100 bps from midpoint;
- conservative completeness flags for every band;
- source instrument, quote conversion, endpoint, timestamps, and raw hashes;
- an explicit observed/partial/failed status for every cataloged market.

Runtime data is intentionally separate from the historical OHLCV database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.atomic_publication import atomic_replace_bundle, csv_payload
    from scripts.bounded_snapshot_merge import (
        merge_exact_market_snapshot,
        require_aligned_depth_execution_lineage,
        validate_exact_publication_scope,
    )
    from scripts.fetch_cex import (
        canonical_collected_instrument,
        make_binance_symbol,
        make_bitget_symbol,
        make_bybit_symbol,
        make_coinbase_product_id,
        make_crypto_com_instrument,
        make_gate_currency_pair,
        make_htx_symbol,
        make_kraken_pair,
        make_kucoin_symbol,
        make_mexc_symbol,
        make_okx_inst_id,
        make_upbit_market_candidates,
    )
    from scripts.execution_cost import (
        EXECUTION_COST_COLUMNS,
        EXECUTION_DIRECTIONS,
        EXECUTION_NOTIONALS_USD,
        execution_fact_row,
        status_counts as execution_status_counts,
        validate_execution_snapshot,
    )
    from scripts.publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
        enforce_publication_coverage_bundle,
        validate_passing_coverage_report,
    )
    from scripts.quality_outcomes import (
        normalize_cex_source_outcome,
        quality_outcome_resolution_state,
    )
    from scripts.route_quantity import (
        CommonTarget,
        FeeSemantics,
        MarketRules,
        QuantityQuote,
        fee_semantics_record_binding_sha256,
        market_rules_record_binding_sha256,
        quote_cex_book_quantity,
    )
    from scripts.timestamp_contract import (
        canonical_rfc3339_utc,
        exact_rfc3339_epoch_seconds,
        validate_observation_bounds,
    )
except ModuleNotFoundError:
    from collection_deadline import CollectionDeadline, CollectionDeadlineExceeded
    from atomic_publication import atomic_replace_bundle, csv_payload
    from bounded_snapshot_merge import (
        merge_exact_market_snapshot,
        require_aligned_depth_execution_lineage,
        validate_exact_publication_scope,
    )
    from fetch_cex import (
        canonical_collected_instrument,
        make_binance_symbol,
        make_bitget_symbol,
        make_bybit_symbol,
        make_coinbase_product_id,
        make_crypto_com_instrument,
        make_gate_currency_pair,
        make_htx_symbol,
        make_kraken_pair,
        make_kucoin_symbol,
        make_mexc_symbol,
        make_okx_inst_id,
        make_upbit_market_candidates,
    )
    from execution_cost import (
        EXECUTION_COST_COLUMNS,
        EXECUTION_DIRECTIONS,
        EXECUTION_NOTIONALS_USD,
        execution_fact_row,
        status_counts as execution_status_counts,
        validate_execution_snapshot,
    )
    from publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
        enforce_publication_coverage_bundle,
        validate_passing_coverage_report,
    )
    from quality_outcomes import (
        normalize_cex_source_outcome,
        quality_outcome_resolution_state,
    )
    from route_quantity import (  # type: ignore
        CommonTarget,
        FeeSemantics,
        MarketRules,
        QuantityQuote,
        fee_semantics_record_binding_sha256,
        market_rules_record_binding_sha256,
        quote_cex_book_quantity,
    )
    from timestamp_contract import (
        canonical_rfc3339_utc,
        exact_rfc3339_epoch_seconds,
        validate_observation_bounds,
    )


# Exchange decimal fields are fixed-point text, not arbitrary Decimal input.
# These caps are checked lexically before Decimal can allocate or format an
# attacker-controlled exponent.
MAX_RULE_DECIMAL_TOKEN_BYTES = 512
MAX_RULE_DECIMAL_PLACES = 255
_RULE_DECIMAL_TEXT = re.compile(
    r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z", flags=re.ASCII
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "data/local/market_facts.sqlite3"
DEFAULT_CEX_CSV = PROJECT_ROOT / "data/local/cex_exchange_volume_daily.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/cex-depth"
TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

CURRENT_FILENAME = "cex_depth_snapshot.csv"
LATEST_FILENAME = "cex_depth_latest.csv"
HISTORY_FILENAME = "cex_depth_history.csv"
EXECUTION_CURRENT_FILENAME = "cex_execution_cost_snapshot.csv"
EXECUTION_LATEST_FILENAME = "cex_execution_cost_latest.csv"
MINIMUM_PUBLISHABLE_COVERAGE_BPS = 9000
MINIMUM_BASELINE_RETENTION_BPS = 9500
COVERAGE_POLICY = {
    "thresholds": {
        "allow_no_eligible_candidate": False,
        "minimum_candidate_usable_bps": MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        "minimum_baseline_retention_bps": MINIMUM_BASELINE_RETENTION_BPS,
        "minimum_cohort_baseline_count": 5,
        "minimum_cohort_lost_count": 2,
        "minimum_cohort_retention_bps": 5000,
    },
    "usable_statuses": ["observed", "partial"],
    "excluded_statuses": [],
    "valid_statuses": ["failed", "observed", "partial"],
}
EXACT_COVERAGE_POLICY = {
    **COVERAGE_POLICY,
    "thresholds": {
        **COVERAGE_POLICY["thresholds"],
        "minimum_candidate_usable_bps": 0,
        "minimum_baseline_retention_bps": 10_000,
    },
}
DEPTH_BANDS_BPS = (10, 25, 50, 100)
REQUEST_SLEEP_SECONDS = 0.15
MAX_RETRIES = 3
STRICT_CEX_TYPED_RULE_VENUES = frozenset({"binance", "bybit"})
STRICT_CEX_TYPED_VALIDITY_SECONDS = 60
MAX_CEX_TYPED_RULE_RESPONSE_BYTES = 256 * 1024

# Public REST limits differ by venue.  A completeness flag prevents a limited
# response from being represented as the venue's complete depth.
REQUESTED_LEVELS = {
    "binance": 100,
    "okx": 400,
    "bybit": 1000,
    "kucoin": 100,
    "gate": 100,
    "bitget": 150,
    "mexc": 100,
    "htx": 150,
    "coinbase": 0,  # level=2 is the full aggregated book
    "kraken": 500,
    "crypto_com": 50,
    "upbit": 30,
}

BASE_COLUMNS = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "quote_to_usd",
    "quote_conversion_method",
    "quote_conversion_endpoint",
    "quote_conversion_response_sha256",
    "best_bid",
    "best_ask",
    "midpoint",
    "spread_quote",
    "spread_bps",
    "bid_levels_returned",
    "ask_levels_returned",
    "requested_level_limit",
    "full_book_reported",
]
DEPTH_COLUMNS = [
    field
    for band in DEPTH_BANDS_BPS
    for field in (
        f"bid_depth_{band}bps_usd",
        f"ask_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
]
AUDIT_COLUMNS = [
    "depth_method",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
]
DEPTH_COLUMNS_ALL = BASE_COLUMNS + DEPTH_COLUMNS + AUDIT_COLUMNS

CEX_DEPTH_REASON_CODES = {
    "observed",
    "source_level_limit",
    "source_no_two_sided_book",
    "source_no_order_book",
    "source_invalid_order_book",
    "not_listed",
    "rate_limit",
    "source_unavailable",
    "source_rejected_request",
    "network",
    "parse",
    "unsupported_source",
    "collection_failed",
}
NON_RETRYABLE_DEPTH_REASON_CODES = {
    "source_no_two_sided_book",
    "source_no_order_book",
    "source_invalid_order_book",
    "not_listed",
    "source_rejected_request",
    "unsupported_source",
}


class SourceBookError(RuntimeError):
    """Carry a source response that could not be normalized as a valid book."""

    def __init__(
        self,
        message: str,
        *,
        raw: bytes = b"",
        endpoint: str = "",
        source_instrument: str = "",
        source_quote_asset: str = "",
        reason_code: str = "",
    ) -> None:
        super().__init__(message)
        self.raw = raw
        self.endpoint = endpoint
        self.source_instrument = source_instrument
        self.source_quote_asset = source_quote_asset
        self.reason_code = reason_code


def depth_failure_reason_code(error: BaseException) -> str:
    """Map collector failures onto a stable, public quality vocabulary."""
    if (
        isinstance(error, SourceBookError)
        and error.reason_code in CEX_DEPTH_REASON_CODES
    ):
        return error.reason_code
    if isinstance(error, urllib.error.HTTPError):
        if error.code == 404:
            return "not_listed"
        if error.code == 429:
            return "rate_limit"
        if 500 <= error.code < 600:
            return "source_unavailable"
        return "source_rejected_request"
    if isinstance(error, (urllib.error.URLError, TimeoutError)):
        return "network"
    if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
        return "parse"

    message = str(error).lower()
    if "empty order-book side" in message:
        return "source_no_two_sided_book"
    if "crossed or locked" in message or "invalid numeric order-book" in message:
        return "source_invalid_order_book"
    if "source timestamp" in message:
        return "parse"
    if "returned no order book" in message:
        return "source_no_order_book"
    if "unsupported exchange" in message or "unsupported source" in message:
        return "unsupported_source"
    return "collection_failed"


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite_decimal(value: Any, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError(f"Invalid numeric order-book value: {value}") from error
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ValueError(f"Invalid order-book value: {value}")
    return number


def decimal_text(value: Decimal | str | int | float | None) -> str:
    if value is None:
        return ""
    number = finite_decimal(value)
    return format(number, "f")


def timestamp_text(value: Any) -> str:
    """Normalize milliseconds, seconds, or an ISO timestamp to UTC text."""
    if value is None or str(value).strip() == "":
        return ""
    text = str(value).strip()
    try:
        numeric = float(text)
    except ValueError:
        try:
            return canonical_rfc3339_utc(text)
        except (OverflowError, ValueError) as error:
            raise ValueError("Invalid source timestamp") from error
    seconds = numeric / 1000 if numeric > 10_000_000_000 else numeric
    try:
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError("Invalid source timestamp") from error


def load_markets_from_database(database_path: Path) -> list[dict[str, str]]:
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT token_symbol, exchange, cex_symbol
            FROM cex_market_daily
            WHERE rowid IN (
                SELECT MAX(rowid)
                FROM cex_market_daily
                GROUP BY token_symbol, exchange
            )
            ORDER BY exchange, token_symbol, cex_symbol
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def load_markets_from_csv(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"token_symbol", "exchange", "cex_symbol"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{csv_path.name} is missing columns: {', '.join(missing)}")
        unique = {}
        for row in reader:
            if not (
                row.get("token_symbol")
                and row.get("exchange")
                and row.get("cex_symbol")
            ):
                continue
            key = (
                row["token_symbol"].upper(),
                row["exchange"].lower(),
            )
            unique[key] = {
                "token_symbol": row["token_symbol"].upper(),
                "exchange": row["exchange"].lower(),
                "cex_symbol": row["cex_symbol"].upper(),
            }
    return [unique[key] for key in sorted(unique, key=lambda item: (item[1], item[0]))]


def load_cataloged_markets(
    database_path: Path = DEFAULT_DATABASE,
    csv_path: Path = DEFAULT_CEX_CSV,
) -> list[dict[str, str]]:
    if database_path.exists():
        rows = load_markets_from_database(database_path)
    elif csv_path.exists():
        rows = load_markets_from_csv(csv_path)
    else:
        raise FileNotFoundError(
            f"No published CEX market inventory found at {database_path} or {csv_path}"
        )
    if not rows:
        raise ValueError("Published CEX market inventory contains no markets")
    for row in rows:
        published_instrument = str(row.get("cex_symbol") or "").strip().upper()
        exact_instrument = canonical_collected_instrument(
            str(row.get("exchange") or ""),
            published_instrument,
        )
        if published_instrument != exact_instrument:
            raise ValueError(
                "Published CEX catalog does not preserve the exact source "
                "instrument identity for {}: expected {}, found {}".format(
                    row.get("exchange"),
                    exact_instrument,
                    published_instrument,
                )
            )
    return rows


def query_url(base: str, params: dict[str, Any]) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def _canonical_typed_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _bounded_rule_decimal_text(
    value: Any, field: str, *, permit_internal_decimal: bool = False
) -> str:
    if permit_internal_decimal and isinstance(value, Decimal):
        value = str(value)
    if not isinstance(value, str):
        raise ValueError("{} is not exact fixed-point text".format(field))
    try:
        width = len(value.encode("ascii"))
    except UnicodeEncodeError as error:
        raise ValueError("{} is not exact fixed-point text".format(field)) from error
    if (
        not value
        or width > MAX_RULE_DECIMAL_TOKEN_BYTES
        or _RULE_DECIMAL_TEXT.fullmatch(value) is None
    ):
        raise ValueError("{} is not exact fixed-point text".format(field))
    places = len(value.rsplit(".", 1)[1]) if "." in value else 0
    if places > MAX_RULE_DECIMAL_PLACES:
        raise ValueError("{} has unsupported precision".format(field))
    return value


def _rule_decimal(
    value: Any,
    field: str,
    *,
    allow_zero: bool,
    permit_internal_decimal: bool = False,
) -> Decimal:
    text = _bounded_rule_decimal_text(
        value, field, permit_internal_decimal=permit_internal_decimal
    )
    try:
        number = Decimal(text)
    except InvalidOperation as error:  # pragma: no cover - grammar is stricter
        raise ValueError("{} is not exact fixed-point text".format(field)) from error
    if number < 0 or (not allow_zero and number == 0):
        qualification = "nonnegative" if allow_zero else "positive"
        raise ValueError("{} is not an exact {} decimal".format(field, qualification))
    return number


def _positive_rule_decimal(value: Any, field: str) -> Decimal:
    return _rule_decimal(value, field, allow_zero=False)


def _nonnegative_rule_decimal(value: Any, field: str) -> Decimal:
    return _rule_decimal(value, field, allow_zero=True)


def _canonical_rule_decimal(
    value: Any,
    field: str,
    *,
    allow_zero: bool = False,
    permit_internal_decimal: bool = False,
) -> str:
    number = _rule_decimal(
        value,
        field,
        allow_zero=allow_zero,
        permit_internal_decimal=permit_internal_decimal,
    )
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _decimal_unit_places(value: Any, field: str) -> int:
    number = _positive_rule_decimal(value, field).normalize()
    exponent = int(number.as_tuple().exponent)
    places = max(0, -exponent)
    if places > 255:
        raise ValueError("{} has unsupported precision".format(field))
    return places


def _unit_increment(decimals: int) -> str:
    if type(decimals) is not int or not 0 <= decimals <= 255:
        raise ValueError("asset precision is invalid")
    if decimals == 0:
        return "1"
    return "0." + "0" * (decimals - 1) + "1"


def _typed_source_window(observed_at: str) -> tuple[str, str]:
    canonical = canonical_rfc3339_utc(observed_at)
    observed = datetime.fromisoformat(canonical)
    valid_until = observed + timedelta(seconds=STRICT_CEX_TYPED_VALIDITY_SECONDS)
    return canonical, valid_until.isoformat()


def binance_market_rules_projection(
    payload: Any,
    *,
    base_asset: str,
    quote_asset: str,
    source_instrument: str,
) -> dict[str, str]:
    """Replay Binance's exact trading-rule fields without I/O or timestamps."""
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise ValueError("Binance market-rules response is invalid")
    matches = [
        item for item in payload["symbols"]
        if isinstance(item, dict) and item.get("symbol") == source_instrument
    ]
    if len(matches) != 1:
        raise ValueError("Binance market-rules instrument is not unique")
    record = matches[0]
    if (
        record.get("status") != "TRADING"
        or record.get("baseAsset") != base_asset
        or record.get("quoteAsset") != quote_asset
        or type(record.get("baseAssetPrecision")) is not int
        or type(record.get("quoteAssetPrecision")) is not int
        or not 0 <= record["baseAssetPrecision"] <= 255
        or not 0 <= record["quoteAssetPrecision"] <= 255
        or not isinstance(record.get("filters"), list)
    ):
        raise ValueError("Binance market-rules identity is invalid")
    filters: dict[str, dict[str, Any]] = {}
    for item in record["filters"]:
        if not isinstance(item, dict) or not isinstance(item.get("filterType"), str):
            raise ValueError("Binance market-rules filter is invalid")
        filter_type = item["filterType"]
        if filter_type in filters:
            raise ValueError("Binance market-rules filter is duplicated")
        filters[filter_type] = item
    try:
        price_filter = filters["PRICE_FILTER"]
        lot_filter = filters["LOT_SIZE"]
        notional_filters = [
            filters[name] for name in ("NOTIONAL", "MIN_NOTIONAL")
            if name in filters
        ]
        if len(notional_filters) != 1:
            raise ValueError(
                "Binance notional filter must contain exactly one unambiguous rule"
            )
        notional_filter = notional_filters[0]
        return {
            "price_tick": _canonical_rule_decimal(
                price_filter["tickSize"], "Binance PRICE_FILTER.tickSize"
            ),
            "quantity_step": _canonical_rule_decimal(
                lot_filter["stepSize"], "Binance LOT_SIZE.stepSize"
            ),
            "min_quantity": _canonical_rule_decimal(
                lot_filter["minQty"],
                "Binance LOT_SIZE.minQty",
                allow_zero=True,
            ),
            "min_notional": _canonical_rule_decimal(
                notional_filter["minNotional"],
                "Binance notional minimum",
                allow_zero=True,
            ),
        }
    except (KeyError, TypeError) as error:
        raise ValueError("Binance market-rules fields are incomplete") from error


def _binance_market_rules_source(
    payload: Any,
    *,
    market: dict[str, str],
    source_instrument: str,
    observed_at: str,
) -> dict[str, Any]:
    base_asset, quote_asset = market["cex_symbol"].upper().split("/", 1)
    projection = binance_market_rules_projection(
        payload,
        base_asset=base_asset,
        quote_asset=quote_asset,
        source_instrument=source_instrument,
    )
    record = next(
        item for item in payload["symbols"]
        if isinstance(item, dict) and item.get("symbol") == source_instrument
    )
    exact_observed_at, valid_until = _typed_source_window(observed_at)
    return {
        "schema": "route_market_rules_source/v1",
        "market_id": cex_market_id(market),
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "base_unit_decimals": record["baseAssetPrecision"],
        "quote_unit_decimals": record["quoteAssetPrecision"],
        "base_increment": projection["quantity_step"],
        "quote_increment": _unit_increment(record["quoteAssetPrecision"]),
        "min_base_quantity": projection["min_quantity"],
        "min_quote_notional": projection["min_notional"],
        "observed_at": exact_observed_at,
        "valid_until": valid_until,
    }


def _bybit_market_rules_source(
    payload: Any,
    *,
    market: dict[str, str],
    source_instrument: str,
    observed_at: str,
) -> dict[str, Any]:
    if (
        not isinstance(payload, dict)
        or payload.get("retCode") != 0
        or not isinstance(payload.get("result"), dict)
        or payload["result"].get("category") != "spot"
        or not isinstance(payload["result"].get("list"), list)
    ):
        raise ValueError("Bybit market-rules response is invalid")
    matches = [
        item for item in payload["result"]["list"]
        if isinstance(item, dict) and item.get("symbol") == source_instrument
    ]
    if len(matches) != 1:
        raise ValueError("Bybit market-rules instrument is not unique")
    record = matches[0]
    base_asset, quote_asset = market["cex_symbol"].upper().split("/", 1)
    lot = record.get("lotSizeFilter")
    price = record.get("priceFilter")
    if (
        record.get("status") != "Trading"
        or record.get("baseCoin") != base_asset
        or record.get("quoteCoin") != quote_asset
        or not isinstance(lot, dict)
        or not isinstance(price, dict)
    ):
        raise ValueError("Bybit market-rules identity is invalid")
    try:
        base_increment = _canonical_rule_decimal(
            lot["basePrecision"], "Bybit basePrecision"
        )
        quote_increment = _canonical_rule_decimal(
            lot["quotePrecision"], "Bybit quotePrecision"
        )
        # Bybit's spot instrument contract marks minOrderQty deprecated and
        # says it is no longer enforced; minOrderAmt is the active minimum.
        # A stale positive minOrderQty therefore cannot become a strict rule.
        min_base_quantity = "0"
        min_quote_notional = _canonical_rule_decimal(
            lot["minOrderAmt"], "Bybit minOrderAmt"
        )
        _canonical_rule_decimal(
            price["tickSize"], "Bybit tickSize"
        )
    except (KeyError, TypeError) as error:
        raise ValueError("Bybit market-rules fields are incomplete") from error
    exact_observed_at, valid_until = _typed_source_window(observed_at)
    return {
        "schema": "route_market_rules_source/v1",
        "market_id": cex_market_id(market),
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "base_unit_decimals": _decimal_unit_places(
            lot["basePrecision"], "Bybit basePrecision"
        ),
        "quote_unit_decimals": _decimal_unit_places(
            lot["quotePrecision"], "Bybit quotePrecision"
        ),
        "base_increment": base_increment,
        "quote_increment": quote_increment,
        "min_base_quantity": min_base_quantity,
        "min_quote_notional": min_quote_notional,
        "observed_at": exact_observed_at,
        "valid_until": valid_until,
    }


def _validate_typed_market_rules(value: dict[str, Any]) -> dict[str, Any]:
    payload = _canonical_typed_json(value)
    MarketRules(
        market_id=value["market_id"],
        base_asset=value["base_asset"],
        quote_asset=value["quote_asset"],
        base_unit_decimals=value["base_unit_decimals"],
        quote_unit_decimals=value["quote_unit_decimals"],
        base_increment=Decimal(value["base_increment"]),
        quote_increment=Decimal(value["quote_increment"]),
        min_base_quantity=Decimal(value["min_base_quantity"]),
        min_quote_notional=Decimal(value["min_quote_notional"]),
        observed_at=value["observed_at"],
        valid_until=value["valid_until"],
        source_record_sha256=hashlib.sha256(payload).hexdigest(),
    )
    return value


def _typed_market_rules_request(exchange: str, source_instrument: str) -> str:
    if exchange == "binance":
        return query_url(
            "https://api.binance.com/api/v3/exchangeInfo",
            {"symbol": source_instrument},
        )
    if exchange == "bybit":
        return query_url(
            "https://api.bybit.com/v5/market/instruments-info",
            {"category": "spot", "symbol": source_instrument},
        )
    raise ValueError("CEX typed market-rules adapter is unsupported")


def _typed_cex_source_payloads(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    rules_response: Any,
    rules_raw: bytes,
    rules_observed_at: str,
    conversion_observed_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    exchange = market["exchange"].lower()
    if exchange not in STRICT_CEX_TYPED_RULE_VENUES:
        raise ValueError("CEX typed market-rules adapter is unsupported")
    if not isinstance(rules_raw, bytes) or not rules_raw:
        raise ValueError("CEX market-rules raw response is invalid")
    if len(rules_raw) > MAX_CEX_TYPED_RULE_RESPONSE_BYTES:
        raise ValueError("CEX market-rules response exceeds its byte limit")
    if exchange == "binance":
        rules = _validate_typed_market_rules(_binance_market_rules_source(
            rules_response,
            market=market,
            source_instrument=book["source_instrument"],
            observed_at=rules_observed_at,
        ))
    else:
        rules = _validate_typed_market_rules(_bybit_market_rules_source(
            rules_response,
            market=market,
            source_instrument=book["source_instrument"],
            observed_at=rules_observed_at,
        ))
    quote_asset = str(book["source_quote_asset"]).upper()
    conversion_method = str(book.get("quote_conversion_method") or "")
    conversion_value = _canonical_rule_decimal(
        book.get("quote_to_usd"),
        "CEX quote-to-USD conversion",
        permit_internal_decimal=True,
    )
    if quote_asset == "USD":
        if conversion_value != "1" or conversion_method != "USD=USD identity":
            raise ValueError("CEX USD identity conversion is invalid")
    elif quote_asset == "USDT":
        if conversion_value != "1" or conversion_method != "USDT=USD proxy":
            raise ValueError("CEX USDT proxy conversion is invalid")
    else:
        raise ValueError("CEX typed USD conversion adapter is unsupported")
    conversion_observed, conversion_valid_until = _typed_source_window(
        conversion_observed_at
    )
    conversion = {
        "schema": "route_usd_conversion_source/v1",
        "quote_asset": quote_asset,
        "usd_per_quote": conversion_value,
        "observed_at": conversion_observed,
        "valid_until": conversion_valid_until,
        "source": conversion_method,
    }
    return rules, conversion


def request_json(
    url: str,
    *,
    deadline: CollectionDeadline | None = None,
    timeout_seconds: float = 30,
    max_retries: int = MAX_RETRIES,
    max_bytes: int | None = None,
) -> tuple[Any, bytes]:
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")
    if max_bytes is not None and (
        type(max_bytes) is not int or max_bytes < 1
    ):
        raise ValueError("max_bytes must be a positive integer")
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "CEX-DEX-Market-Monitor/1.0",
        },
    )
    for attempt in range(max_retries):
        try:
            timeout = (
                deadline.request_timeout(timeout_seconds)
                if deadline is not None
                else timeout_seconds
            )
            with open_public_json_request(request, timeout=timeout) as response:
                raw = (
                    response.read()
                    if max_bytes is None
                    else response.read(max_bytes + 1)
                )
                if max_bytes is not None and len(raw) > max_bytes:
                    raise ValueError("HTTP response exceeds its byte limit")
                return json.loads(raw.decode("utf-8")), raw
        except urllib.error.HTTPError as error:
            if deadline is not None:
                deadline.require_remaining()
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt + 1 >= max_retries:
                raise
            retry_after = error.headers.get("Retry-After")
            delay = max(2.0, float(retry_after or 0), 2 ** attempt)
            if deadline is not None:
                deadline.sleep_before_retry(delay)
            else:
                time.sleep(delay)
        except urllib.error.URLError:
            if deadline is not None:
                deadline.require_remaining()
            if attempt + 1 >= max_retries:
                raise
            delay = max(2.0, 2 ** attempt)
            if deadline is not None:
                deadline.sleep_before_retry(delay)
            else:
                time.sleep(delay)
    raise RuntimeError(f"Failed after retries: {url}")


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def open_public_json_request(
    request: urllib.request.Request, *, timeout: float
) -> Any:
    """Open one public JSON request without widening its endpoint by redirect."""
    opener = urllib.request.build_opener(
        _NoRedirectHandler(),
        urllib.request.HTTPSHandler(context=TLS_CONTEXT),
    )
    return opener.open(request, timeout=timeout)


def source_request(exchange: str, cex_symbol: str) -> tuple[str, str, str, bool]:
    """Return endpoint, source instrument, quote asset, and full-book flag."""
    if exchange == "binance":
        instrument = make_binance_symbol(cex_symbol)
        return (
            query_url(
                "https://data-api.binance.vision/api/v3/depth",
                {"symbol": instrument, "limit": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "okx":
        instrument = make_okx_inst_id(cex_symbol)
        return (
            query_url(
                "https://www.okx.com/api/v5/market/books",
                {"instId": instrument, "sz": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "bybit":
        instrument = make_bybit_symbol(cex_symbol)
        return (
            query_url(
                "https://api.bybit.com/v5/market/orderbook",
                {
                    "category": "spot",
                    "symbol": instrument,
                    "limit": REQUESTED_LEVELS[exchange],
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "kucoin":
        instrument = make_kucoin_symbol(cex_symbol)
        return (
            query_url(
                "https://api.kucoin.com/api/v1/market/orderbook/level2_100",
                {"symbol": instrument},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "gate":
        instrument = make_gate_currency_pair(cex_symbol)
        return (
            query_url(
                "https://api.gateio.ws/api/v4/spot/order_book",
                {
                    "currency_pair": instrument,
                    "limit": REQUESTED_LEVELS[exchange],
                    "with_id": "true",
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "bitget":
        instrument = make_bitget_symbol(cex_symbol)
        return (
            query_url(
                "https://api.bitget.com/api/v2/spot/market/orderbook",
                {
                    "symbol": instrument,
                    "type": "step0",
                    "limit": REQUESTED_LEVELS[exchange],
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "mexc":
        instrument = make_mexc_symbol(cex_symbol)
        return (
            query_url(
                "https://api.mexc.com/api/v3/depth",
                {"symbol": instrument, "limit": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "htx":
        instrument = make_htx_symbol(cex_symbol)
        return (
            query_url(
                "https://api.huobi.pro/market/depth",
                {"symbol": instrument, "type": "step0"},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "coinbase":
        instrument = make_coinbase_product_id(cex_symbol)
        return (
            query_url(
                f"https://api.exchange.coinbase.com/products/{instrument}/book",
                {"level": 2},
            ),
            instrument,
            "USD",
            True,
        )
    if exchange == "kraken":
        instrument = make_kraken_pair(cex_symbol)
        return (
            query_url(
                "https://api.kraken.com/0/public/Depth",
                {"pair": instrument, "count": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USD",
            False,
        )
    if exchange == "crypto_com":
        instrument = make_crypto_com_instrument(cex_symbol)
        return (
            query_url(
                "https://api.crypto.com/exchange/v1/public/get-book",
                {"instrument_name": instrument, "depth": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    raise ValueError(f"Unsupported exchange: {exchange}")


def parse_book(
    exchange: str,
    payload: Any,
    *,
    requested_instrument: str,
) -> dict[str, Any]:
    """Normalize a successful venue response to price/quantity levels."""
    instrument = requested_instrument
    sequence = ""
    observed_at = ""
    if exchange in {"binance", "mexc"}:
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"{exchange} returned no order book")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("lastUpdateId") or "")
    elif exchange == "okx":
        if payload.get("code") != "0" or not payload.get("data"):
            raise ValueError(f"OKX returned no order book: {payload}")
        book = payload["data"][0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("seqId") or "")
        observed_at = timestamp_text(book.get("ts"))
    elif exchange == "bybit":
        if payload.get("retCode") != 0 or not payload.get("result"):
            raise ValueError(f"Bybit returned no order book: {payload}")
        book = payload["result"]
        bids, asks = book.get("b", []), book.get("a", [])
        returned_instrument = str(book.get("s") or "")
        if returned_instrument and returned_instrument != instrument:
            raise ValueError("Bybit returned a different order-book instrument")
        sequence = str(book.get("u") or book.get("seq") or "")
        observed_at = timestamp_text(book.get("cts") or book.get("ts"))
    elif exchange == "kucoin":
        if payload.get("code") != "200000" or not payload.get("data"):
            raise ValueError(f"KuCoin returned no order book: {payload}")
        book = payload["data"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("sequence") or "")
        observed_at = timestamp_text(book.get("time"))
    elif exchange == "gate":
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"Gate returned no order book: {payload}")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("id") or payload.get("order_book_id") or "")
        observed_at = timestamp_text(payload.get("update") or payload.get("current"))
    elif exchange == "bitget":
        if payload.get("code") != "00000" or not payload.get("data"):
            raise ValueError(f"Bitget returned no order book: {payload}")
        book = payload["data"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        observed_at = timestamp_text(book.get("ts"))
    elif exchange == "htx":
        if payload.get("status") != "ok" or not payload.get("tick"):
            raise ValueError(f"HTX returned no order book: {payload}")
        book = payload["tick"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("version") or "")
        observed_at = timestamp_text(book.get("ts") or payload.get("ts"))
    elif exchange == "coinbase":
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"Coinbase returned no order book: {payload}")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("sequence") or "")
        observed_at = timestamp_text(payload.get("time"))
    elif exchange == "kraken":
        if payload.get("error"):
            raise ValueError(f"Kraken returned an error: {payload['error']}")
        result = payload.get("result") or {}
        books = [(key, value) for key, value in result.items() if isinstance(value, dict)]
        if not books:
            raise ValueError(f"Kraken returned no order book: {payload}")
        instrument, book = books[0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        timestamps = [
            level[2]
            for level in list(bids) + list(asks)
            if isinstance(level, (list, tuple)) and len(level) > 2
        ]
        observed_at = timestamp_text(max(timestamps)) if timestamps else ""
    elif exchange == "crypto_com":
        if payload.get("code") != 0 or not payload.get("result"):
            raise ValueError(f"Crypto.com returned no order book: {payload}")
        result = payload["result"]
        data = result.get("data") or []
        if not data:
            raise ValueError(f"Crypto.com returned no order book: {payload}")
        book = data[0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        instrument = str(result.get("instrument_name") or instrument)
        sequence = str(book.get("u") or "")
        observed_at = timestamp_text(book.get("tt") or book.get("t"))
    elif exchange == "upbit":
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Upbit returned no order book: {payload}")
        book = payload[0]
        units = book.get("orderbook_units") or []
        bids = [[unit.get("bid_price"), unit.get("bid_size")] for unit in units]
        asks = [[unit.get("ask_price"), unit.get("ask_size")] for unit in units]
        instrument = str(book.get("market") or instrument)
        observed_at = timestamp_text(book.get("timestamp"))
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

    def normalized_levels(values: Iterable[Any], *, reverse: bool) -> list[tuple[Decimal, Decimal]]:
        levels: dict[Decimal, Decimal] = {}
        for level in values:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = finite_decimal(level[0])
            quantity = finite_decimal(level[1])
            # Some venue snapshots retain a placeholder on one side with a
            # zero price or size. It is not resting liquidity and must be
            # ignored rather than counted or allowed to invalidate valid levels.
            if price == 0 or quantity == 0:
                continue
            levels[price] = levels.get(price, Decimal(0)) + quantity
        return sorted(levels.items(), key=lambda item: item[0], reverse=reverse)

    normalized_bids = normalized_levels(bids, reverse=True)
    normalized_asks = normalized_levels(asks, reverse=False)
    if not normalized_bids or not normalized_asks:
        raise ValueError(f"{exchange} returned an empty order-book side")
    if normalized_bids[0][0] >= normalized_asks[0][0]:
        raise ValueError(f"{exchange} returned a crossed or locked order book")
    return {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "source_instrument": instrument,
        "source_sequence": sequence,
        "source_observed_at": observed_at,
    }


def upbit_book(
    cex_symbol: str,
    request: Callable[[str], tuple[Any, bytes]],
) -> dict[str, Any]:
    candidate_errors: list[SourceBookError] = []
    for market in make_upbit_market_candidates(cex_symbol):
        raw = b""
        url = query_url(
            "https://api.upbit.com/v1/orderbook",
            {"markets": market, "count": REQUESTED_LEVELS["upbit"]},
        )
        try:
            payload, raw = request(url)
            parsed = parse_book("upbit", payload, requested_instrument=market)
        except CollectionDeadlineExceeded:
            raise
        except Exception as error:
            candidate_error = (
                error
                if isinstance(error, SourceBookError)
                else SourceBookError(
                    str(error),
                    raw=raw,
                    endpoint=url,
                    source_instrument=market,
                    source_quote_asset=market.split("-", 1)[0].upper(),
                    reason_code=depth_failure_reason_code(error),
                )
            )
            candidate_errors.append(candidate_error)
            continue

        quote_asset = market.split("-", 1)[0].upper()
        result = {
            **parsed,
            "source_endpoint": url,
            "raw": raw,
            "source_quote_asset": quote_asset,
            "quote_to_usd": Decimal(1),
            "quote_conversion_method": (
                "USDT=USD proxy" if quote_asset == "USDT" else "Upbit KRW-USDT midpoint"
            ),
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
        }
        if quote_asset == "USDT":
            return result
        if quote_asset != "KRW":
            candidate_errors.append(
                SourceBookError(
                    f"Unsupported Upbit quote asset: {quote_asset}",
                    raw=raw,
                    endpoint=url,
                    source_instrument=market,
                    source_quote_asset=quote_asset,
                    reason_code="unsupported_source",
                )
            )
            continue

        fx_url = query_url(
            "https://api.upbit.com/v1/orderbook",
            {"markets": "KRW-USDT", "count": 1},
        )
        fx_raw = b""
        try:
            fx_payload, fx_raw = request(fx_url)
            fx_book = parse_book("upbit", fx_payload, requested_instrument="KRW-USDT")
            krw_per_usdt = (fx_book["bids"][0][0] + fx_book["asks"][0][0]) / Decimal(2)
            result["quote_to_usd"] = Decimal(1) / krw_per_usdt
            result["quote_conversion_endpoint"] = fx_url
            result["quote_conversion_response_sha256"] = hashlib.sha256(fx_raw).hexdigest()
            result["quote_conversion_observed_at"] = (
                fx_book.get("source_observed_at") or ""
            )
            result["quote_conversion_raw"] = fx_raw
            return result
        except CollectionDeadlineExceeded:
            raise
        except Exception as error:
            candidate_errors.append(SourceBookError(
                f"Failed KRW quote conversion: {error}",
                raw=fx_raw,
                endpoint=fx_url,
                source_instrument="KRW-USDT",
                source_quote_asset=quote_asset,
                reason_code=depth_failure_reason_code(error),
            ))
            continue
    if candidate_errors:
        # A candidate that answered but failed for another explicit reason
        # carries stronger evidence than a later fallback candidate's 404.
        # Only report not_listed when every configured quote candidate is absent.
        selected_error = next(
            (
                error
                for error in candidate_errors
                if error.reason_code != "not_listed"
            ),
            candidate_errors[-1],
        )
        raise selected_error
    raise RuntimeError(f"Failed to fetch Upbit order book for {cex_symbol}")


def fetch_source_book(
    exchange: str,
    cex_symbol: str,
    *,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
) -> dict[str, Any]:
    if exchange == "upbit":
        return upbit_book(cex_symbol, request)
    url, instrument, quote_asset, full_book = source_request(exchange, cex_symbol)
    payload, raw = request(url)
    try:
        parsed = parse_book(exchange, payload, requested_instrument=instrument)
    except Exception as error:
        raise SourceBookError(
            str(error),
            raw=raw,
            endpoint=url,
            source_instrument=instrument,
            source_quote_asset=quote_asset,
            reason_code=depth_failure_reason_code(error),
        ) from error
    return {
        **parsed,
        "source_endpoint": url,
        "raw": raw,
        "source_quote_asset": quote_asset,
        "quote_to_usd": Decimal(1),
        "quote_conversion_method": f"{quote_asset}=USD proxy",
        "quote_conversion_endpoint": "",
        "quote_conversion_response_sha256": "",
        "full_book_reported": full_book,
    }


def depth_metrics(
    bids: list[tuple[Decimal, Decimal]],
    asks: list[tuple[Decimal, Decimal]],
    *,
    quote_to_usd: Decimal,
    requested_limit: int,
    full_book_reported: bool,
) -> dict[str, str]:
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    midpoint = (best_bid + best_ask) / Decimal(2)
    spread_quote = best_ask - best_bid
    result = {
        "best_bid": decimal_text(best_bid),
        "best_ask": decimal_text(best_ask),
        "midpoint": decimal_text(midpoint),
        "spread_quote": decimal_text(spread_quote),
        "spread_bps": decimal_text(spread_quote / midpoint * Decimal(10_000)),
        "bid_levels_returned": str(len(bids)),
        "ask_levels_returned": str(len(asks)),
        "requested_level_limit": str(requested_limit),
        "full_book_reported": "1" if full_book_reported else "0",
    }
    for band in DEPTH_BANDS_BPS:
        fraction = Decimal(band) / Decimal(10_000)
        bid_boundary = midpoint * (Decimal(1) - fraction)
        ask_boundary = midpoint * (Decimal(1) + fraction)
        bid_depth = sum(
            price * quantity * quote_to_usd
            for price, quantity in bids
            if price >= bid_boundary
        )
        ask_depth = sum(
            price * quantity * quote_to_usd
            for price, quantity in asks
            if price <= ask_boundary
        )
        bid_complete = full_book_reported or bids[-1][0] <= bid_boundary
        ask_complete = full_book_reported or asks[-1][0] >= ask_boundary
        result[f"bid_depth_{band}bps_usd"] = decimal_text(bid_depth)
        result[f"ask_depth_{band}bps_usd"] = decimal_text(ask_depth)
        result[f"total_depth_{band}bps_usd"] = decimal_text(bid_depth + ask_depth)
        result[f"depth_{band}bps_complete"] = "1" if bid_complete and ask_complete else "0"
    return result


def _walk_book_for_base_quantity(
    levels: list[tuple[Decimal, Decimal]],
    target_base_quantity: Decimal,
) -> tuple[Decimal, Decimal, int, Decimal | None, bool]:
    """Walk normalized levels and partially consume only the final price level."""
    target = finite_decimal(target_base_quantity, positive=True)
    with localcontext() as context:
        context.prec = 100
        filled = Decimal(0)
        quote_amount = Decimal(0)
        remaining = target
        levels_consumed = 0
        ending_price: Decimal | None = None
        for price_value, quantity_value in levels:
            price = finite_decimal(price_value, positive=True)
            quantity = finite_decimal(quantity_value, positive=True)
            take = min(quantity, remaining)
            filled += take
            quote_amount += price * take
            remaining -= take
            levels_consumed += 1
            ending_price = price
            if remaining == 0:
                break
    return filled, quote_amount, levels_consumed, ending_price, remaining == 0


def cex_market_id(market: dict[str, str]) -> str:
    return (
        f"cex:{market['exchange'].lower()}:"
        f"{market['cex_symbol'].upper()}"
    )


def _canonical_quantity_level(value: Any, field: str) -> str:
    if not isinstance(value, Decimal):
        raise ValueError(f"{field} must be an exact normalized Decimal")
    number = finite_decimal(value, positive=True)
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


@dataclass(frozen=True)
class _CexQuantityBookSnapshot:
    market_id: str
    exchange: str
    cex_symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    raw: bytes
    source_instrument: str
    source_quote_asset: str
    source_sequence: str
    source_observed_at: str
    source_endpoint: str
    full_book_reported: bool


def _expected_cex_source_identity(
    exchange: str,
    cex_symbol: str,
) -> tuple[str, str]:
    quote_asset = cex_symbol.split("/", 1)[1].upper()
    makers = {
        "binance": make_binance_symbol,
        "okx": make_okx_inst_id,
        "bybit": make_bybit_symbol,
        "kucoin": make_kucoin_symbol,
        "gate": make_gate_currency_pair,
        "bitget": make_bitget_symbol,
        "mexc": make_mexc_symbol,
        "htx": make_htx_symbol,
        "coinbase": make_coinbase_product_id,
        "kraken": make_kraken_pair,
        "crypto_com": make_crypto_com_instrument,
    }
    if exchange == "upbit":
        candidates = make_upbit_market_candidates(cex_symbol)
        if len(candidates) != 1:
            raise ValueError("Upbit source_instrument mapping is ambiguous")
        instrument = candidates[0]
        return instrument, instrument.split("-", 1)[0].upper()
    maker = makers.get(exchange)
    if maker is None:
        raise ValueError("unsupported CEX source instrument adapter")
    instrument = maker(cex_symbol)
    if exchange in {"coinbase", "kraken"}:
        quote_asset = "USD"
    return instrument, quote_asset


def _freeze_quantity_levels(
    levels: Any,
    side: str,
) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(levels, (tuple, list)) or not levels:
        raise ValueError(f"normalized CEX {side} are required")
    frozen = []
    for index, level in enumerate(tuple(levels)):
        if not isinstance(level, (tuple, list)) or len(level) != 2:
            raise ValueError(f"normalized CEX {side} level is invalid")
        price, quantity = level
        _canonical_quantity_level(price, f"{side}[{index}].price")
        _canonical_quantity_level(quantity, f"{side}[{index}].quantity")
        frozen.append((price, quantity))
    return tuple(frozen)


def _freeze_cex_quantity_book(
    market: dict[str, str],
    book: dict[str, Any],
    market_rules: MarketRules,
) -> _CexQuantityBookSnapshot:
    exchange_value = market.get("exchange")
    symbol_value = market.get("cex_symbol")
    if not isinstance(exchange_value, str) or not isinstance(symbol_value, str):
        raise ValueError("CEX collector Market identity is incomplete")
    exchange = exchange_value.lower()
    cex_symbol = symbol_value.upper()
    market_id = "cex:{}:{}".format(exchange, cex_symbol)
    if market_rules.market_id != market_id:
        raise ValueError("Market ID does not match the CEX collector market")
    expected_instrument, expected_quote = _expected_cex_source_identity(
        exchange,
        cex_symbol,
    )
    raw = book.get("raw")
    frozen_bids = _freeze_quantity_levels(book.get("bids"), "bids")
    frozen_asks = _freeze_quantity_levels(book.get("asks"), "asks")
    source_instrument = book.get("source_instrument")
    source_quote_asset = book.get("source_quote_asset")
    source_sequence = book.get("source_sequence", "")
    source_observed_at = book.get("source_observed_at", "")
    source_endpoint = book.get("source_endpoint", "")
    full_book_reported = book.get("full_book_reported", False)
    if type(raw) is not bytes or not raw:
        raise ValueError("exact raw response bytes are required")
    if source_instrument != expected_instrument:
        raise ValueError(
            "source_instrument does not match the exact venue adapter"
        )
    if (
        source_quote_asset != expected_quote
        or source_quote_asset != market_rules.quote_asset
    ):
        raise ValueError(
            "source_quote_asset does not match Market and venue adapter"
        )
    if not isinstance(source_sequence, str):
        raise ValueError("source_sequence must be text")
    if not isinstance(source_observed_at, str):
        raise ValueError("source_observed_at must be text")
    if not isinstance(source_endpoint, str):
        raise ValueError("source_endpoint must be text")
    if type(full_book_reported) is not bool:
        raise ValueError("full_book_reported must be boolean")
    return _CexQuantityBookSnapshot(
        market_id=market_id,
        exchange=exchange,
        cex_symbol=cex_symbol,
        bids=frozen_bids,
        asks=frozen_asks,
        raw=raw,
        source_instrument=source_instrument,
        source_quote_asset=source_quote_asset,
        source_sequence=source_sequence,
        source_observed_at=source_observed_at,
        source_endpoint=source_endpoint,
        full_book_reported=full_book_reported,
    )


def _quantity_levels_binding_sha256(
    snapshot: _CexQuantityBookSnapshot,
) -> str:
    canonical_sides: dict[str, list[list[str]]] = {}
    for side in ("bids", "asks"):
        levels = getattr(snapshot, side)
        canonical_levels = []
        for index, level in enumerate(levels):
            canonical_levels.append(
                [
                    _canonical_quantity_level(
                        level[0],
                        f"{side}[{index}].price",
                    ),
                    _canonical_quantity_level(
                        level[1],
                        f"{side}[{index}].quantity",
                    ),
                ]
            )
        canonical_sides[side] = canonical_levels
    encoded = json.dumps(
        canonical_sides,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_exact_epoch_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _cex_quantity_state_id_from_snapshot(
    snapshot: _CexQuantityBookSnapshot,
    *,
    snapshot_id: str,
    observed_at: str,
    cohort_now: str,
    market_rules: MarketRules,
    fee_semantics: FeeSemantics,
) -> str:
    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or snapshot_id != snapshot_id.strip()
    ):
        raise ValueError("snapshot_id must be canonical text")
    try:
        exact_observed_at = exact_rfc3339_epoch_seconds(observed_at)
        exact_cohort_now = exact_rfc3339_epoch_seconds(cohort_now)
    except (OverflowError, ValueError) as error:
        raise ValueError(
            "observed_at and cohort_now must be RFC 3339 text"
        ) from error
    current_rules_binding = market_rules_record_binding_sha256(market_rules)
    if market_rules.record_binding_sha256 != current_rules_binding:
        raise ValueError("MarketRules full-record binding mismatch")
    current_fee_binding = fee_semantics_record_binding_sha256(fee_semantics)
    if fee_semantics.record_binding_sha256 != current_fee_binding:
        raise ValueError("FeeSemantics full-record binding mismatch")
    levels_binding = _quantity_levels_binding_sha256(snapshot)
    payload = {
        "contract": "cex_quantity_state/v1",
        "market_id": snapshot.market_id,
        "snapshot_id": snapshot_id,
        "observed_at_epoch_seconds": _canonical_exact_epoch_text(
            exact_observed_at
        ),
        "cohort_now_epoch_seconds": _canonical_exact_epoch_text(
            exact_cohort_now
        ),
        "source_instrument": snapshot.source_instrument,
        "source_sequence": snapshot.source_sequence,
        "source_observed_at": snapshot.source_observed_at,
        "source_endpoint": snapshot.source_endpoint,
        "source_quote_asset": snapshot.source_quote_asset,
        "full_book_reported": snapshot.full_book_reported,
        "raw_response_sha256": hashlib.sha256(snapshot.raw).hexdigest(),
        "levels_binding_sha256": levels_binding,
        "market_rules_binding_sha256": current_rules_binding,
        "fee_binding_sha256": current_fee_binding,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return "cex-quantity:" + hashlib.sha256(encoded).hexdigest()


def cex_quantity_state_id(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    snapshot_id: str,
    observed_at: str,
    cohort_now: str,
    market_rules: MarketRules,
    fee_semantics: FeeSemantics,
) -> str:
    """Bind exact book, Market, snapshot, rules, and fees for calculation.

    This is an integrity checksum, not authentication. Public callers can
    construct their own records, so matching this ID alone never upgrades a
    quantity calculation to strict route evidence.
    """
    frozen = _freeze_cex_quantity_book(market, book, market_rules)
    return _cex_quantity_state_id_from_snapshot(
        frozen,
        snapshot_id=snapshot_id,
        observed_at=observed_at,
        cohort_now=cohort_now,
        market_rules=market_rules,
        fee_semantics=fee_semantics,
    )


def route_quantity_quote_for_book(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    direction: str,
    target_token_quantity: CommonTarget,
    market_rules: MarketRules,
    fee_semantics: FeeSemantics,
    snapshot_id: str,
    observed_at: str,
    cohort_now: str,
    expected_state_id: str,
) -> QuantityQuote:
    """Bind a calculation quote to one immutable raw CEX book snapshot.

    The fixed-notional v1 publication path intentionally does not call this
    adapter. Route evaluation opts in with independently sourced market rules
    and fee semantics, while the adapter selects the original bid/ask levels
    and derives its immutable state identity from the exact raw response.
    """
    frozen = _freeze_cex_quantity_book(market, book, market_rules)
    calculated_state_id = _cex_quantity_state_id_from_snapshot(
        frozen,
        snapshot_id=snapshot_id,
        observed_at=observed_at,
        cohort_now=cohort_now,
        market_rules=market_rules,
        fee_semantics=fee_semantics,
    )
    if expected_state_id != calculated_state_id:
        raise ValueError("CEX quantity state binding does not match")
    if direction == "buy":
        levels = frozen.asks
    elif direction == "sell":
        levels = frozen.bids
    else:
        raise ValueError("direction must be buy or sell")
    levels_binding = _quantity_levels_binding_sha256(frozen)
    return quote_cex_book_quantity(
        levels,
        target_token_quantity,
        market_rules,
        fee_semantics,
        direction=direction,
        source_quote_asset=frozen.source_quote_asset,
        full_book_reported=frozen.full_book_reported,
        state_id=calculated_state_id,
        snapshot_id=snapshot_id,
        state_observed_at=observed_at,
        cohort_now=cohort_now,
        raw_response_sha256=hashlib.sha256(frozen.raw).hexdigest(),
        levels_binding_sha256=levels_binding,
    )


def usd_conversion_status(book: dict[str, Any]) -> str:
    quote_asset = str(book["source_quote_asset"]).upper()
    if quote_asset == "USD":
        return "identity_usd"
    if quote_asset == "USDT":
        return "proxy_usdt_equals_usd"
    if quote_asset == "KRW":
        return "observed_krw_usdt_midpoint_with_usdt_usd_proxy"
    return "observed_quote_conversion"


def execution_rows_for_book(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> list[dict[str, str]]:
    """Build ten long-form CEX execution facts from the same raw depth book."""
    conversion = finite_decimal(book["quote_to_usd"], positive=True)
    with localcontext() as context:
        context.prec = 100
        midpoint = (
            book["bids"][0][0] + book["asks"][0][0]
        ) / Decimal(2)
    state_observed_at = book.get("source_observed_at") or response_received_at
    common = {
        "snapshot_id": snapshot_id,
        "source_snapshot_id": snapshot_id,
        "calculation_method": "normalized_order_book_level_walk",
        "observed_at": state_observed_at,
        "state_observed_at": state_observed_at,
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "market_id": cex_market_id(market),
        "market_type": "cex",
        "token_symbol": market["token_symbol"].upper(),
        "exchange": market["exchange"].lower(),
        "cex_symbol": market["cex_symbol"].upper(),
        "source_instrument": book["source_instrument"],
        "base_asset": market["cex_symbol"].split("/", 1)[0].upper(),
        "source_quote_asset": book["source_quote_asset"],
        "reference_price_method": "order_book_midpoint",
        "usd_price_source_snapshot_id": (
            snapshot_id if book.get("quote_conversion_response_sha256") else ""
        ),
        "usd_price_observed_at": book.get("quote_conversion_observed_at", ""),
        "fee_status": "excluded_unknown_account_tier",
        "usd_conversion_status": usd_conversion_status(book),
        "excluded_costs": "taker_fee,lot_size,latency",
        "source": f"{market['exchange']} public spot order-book API",
        "source_endpoint": book["source_endpoint"],
        "source_sequence": book.get("source_sequence", ""),
        "raw_response_sha256": hashlib.sha256(book["raw"]).hexdigest(),
    }
    full_book_reported = bool(book.get("full_book_reported"))
    rows: list[dict[str, str]] = []
    with localcontext() as context:
        context.prec = 100
        midpoint_usd = midpoint * conversion
        for notional in EXECUTION_NOTIONALS_USD:
            target_base_quantity = notional / midpoint_usd
            for direction, levels in (
                ("sell_token", book["bids"]),
                ("buy_token", book["asks"]),
            ):
                (
                    filled,
                    quote_amount,
                    levels_consumed,
                    ending_price,
                    complete,
                ) = _walk_book_for_base_quantity(
                    levels,
                    target_base_quantity,
                )
                if complete:
                    status = "observed"
                    status_reason = "target_filled"
                elif full_book_reported:
                    status = "partial"
                    status_reason = "full_book_insufficient_liquidity"
                else:
                    status = "partial"
                    status_reason = "source_level_limit"
                rows.append(
                    execution_fact_row(
                        common=common,
                        direction=direction,
                        requested_notional_usd=notional,
                        status=status,
                        status_reason=status_reason,
                        reference_price_quote_per_token=midpoint,
                        quote_to_usd=conversion,
                        target_token_quantity=target_base_quantity,
                        filled_token_quantity=filled,
                        quote_amount=quote_amount,
                        levels_or_ticks_consumed=levels_consumed,
                        ending_marginal_price_quote_per_token=ending_price,
                    )
                )
    return rows


def failed_execution_rows(
    market: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    error: Exception,
    source_endpoint: str = "",
    source_instrument: str = "",
    source_quote_asset: str = "",
    source_sequence: str = "",
    raw_response_sha256: str = "",
    status_reason: str = "order_book_fetch_or_normalization_failed",
) -> list[dict[str, str]]:
    common = {
        "snapshot_id": snapshot_id,
        "source_snapshot_id": snapshot_id,
        "calculation_method": "normalized_order_book_level_walk",
        "observed_at": response_received_at,
        "state_observed_at": "",
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "market_id": cex_market_id(market),
        "market_type": "cex",
        "token_symbol": market["token_symbol"].upper(),
        "exchange": market["exchange"].lower(),
        "cex_symbol": market["cex_symbol"].upper(),
        "source_instrument": source_instrument,
        "base_asset": market["cex_symbol"].split("/", 1)[0].upper(),
        "source_quote_asset": source_quote_asset,
        "fee_status": "excluded_unknown_account_tier",
        "excluded_costs": "taker_fee,lot_size,latency",
        "source": f"{market['exchange']} public spot order-book API",
        "source_endpoint": source_endpoint,
        "source_sequence": source_sequence,
        "raw_response_sha256": raw_response_sha256,
    }
    error_text = f"{type(error).__name__}: {error}"
    return [
        execution_fact_row(
            common=common,
            direction=direction,
            requested_notional_usd=notional,
            status="failed",
            status_reason=status_reason,
            error=error_text,
        )
        for notional in EXECUTION_NOTIONALS_USD
        for direction in EXECUTION_DIRECTIONS
    ]


def base_row(
    market: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = {column: "" for column in DEPTH_COLUMNS_ALL}
    row.update(
        {
            "snapshot_id": snapshot_id,
            "observed_at": response_received_at,
            "request_started_at": request_started_at,
            "response_received_at": response_received_at,
            "token_symbol": market["token_symbol"].upper(),
            "exchange": market["exchange"].lower(),
            "cex_symbol": market["cex_symbol"].upper(),
            "base_asset": market["cex_symbol"].split("/", 1)[0].upper(),
            "depth_method": "midpoint_symmetric_quote_notional",
            "source": f"{market['exchange']} public spot order-book API",
        }
    )
    return row


def observed_row(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = base_row(
        market,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    requested_limit = REQUESTED_LEVELS[market["exchange"]]
    full_book_reported = bool(book.get("full_book_reported"))
    row.update(
        depth_metrics(
            book["bids"],
            book["asks"],
            quote_to_usd=book["quote_to_usd"],
            requested_limit=requested_limit,
            full_book_reported=full_book_reported,
        )
    )
    row.update(
        {
            "observed_at": book.get("source_observed_at") or response_received_at,
            "source_instrument": book["source_instrument"],
            "source_quote_asset": book["source_quote_asset"],
            "quote_to_usd": decimal_text(book["quote_to_usd"]),
            "quote_conversion_method": book["quote_conversion_method"],
            "quote_conversion_endpoint": book.get("quote_conversion_endpoint", ""),
            "quote_conversion_response_sha256": book.get(
                "quote_conversion_response_sha256",
                "",
            ),
            "source_endpoint": book["source_endpoint"],
            "source_sequence": book.get("source_sequence", ""),
            "raw_response_sha256": hashlib.sha256(book["raw"]).hexdigest(),
        }
    )
    complete = all(row[f"depth_{band}bps_complete"] == "1" for band in DEPTH_BANDS_BPS)
    row["status"] = "observed" if complete else "partial"
    row["reason_code"] = "observed" if complete else "source_level_limit"
    if not complete:
        incomplete = [
            str(band)
            for band in DEPTH_BANDS_BPS
            if row[f"depth_{band}bps_complete"] != "1"
        ]
        row["error"] = (
            "Returned levels do not prove complete coverage for "
            + "/".join(incomplete)
            + " bps; reported depth is an observed lower bound"
        )
    return row


def failure_row(
    market: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    error: Exception,
    source_endpoint: str = "",
    source_instrument: str = "",
    source_quote_asset: str = "",
    raw_response_sha256: str = "",
    reason_code: str = "",
) -> dict[str, str]:
    row = base_row(
        market,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    row["source_endpoint"] = source_endpoint
    row["source_instrument"] = source_instrument
    row["source_quote_asset"] = source_quote_asset
    row["raw_response_sha256"] = raw_response_sha256
    row["requested_level_limit"] = str(REQUESTED_LEVELS[market["exchange"]])
    row["status"] = "failed"
    row["reason_code"] = reason_code or depth_failure_reason_code(error)
    row["error"] = f"{type(error).__name__}: {error}"
    return row


def safe_component(value: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in value).strip("-")


def collect_cex_market_observation(
    market: dict[str, str],
    *,
    snapshot_id: str,
    raw_path: Path,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
    deadline: CollectionDeadline | None = None,
    typed_source_payload_sink: Callable[[dict[str, Any]], Any] | None = None,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Collect one CEX market without owning orchestration or publication."""
    if deadline is not None:
        deadline.require_remaining()
    request_started_at = utc_now_text()
    effective_request = request
    if deadline is not None:
        effective_request = lambda url: request(url, deadline=deadline)
    try:
        book = fetch_source_book(
            market["exchange"],
            market["cex_symbol"],
            request=effective_request,
        )
        response_received_at = utc_now_text()
        raw_path.write_bytes(book["raw"])
        if book.get("quote_conversion_raw"):
            raw_path.with_name(raw_path.stem + "-quote-conversion.json").write_bytes(
                book["quote_conversion_raw"]
            )
        row = observed_row(
            market,
            book,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
        )
        if (
            typed_source_payload_sink is not None
            and market["exchange"].lower() in STRICT_CEX_TYPED_RULE_VENUES
        ):
            try:
                if deadline is not None:
                    deadline.require_remaining()
                conversion_observed_at = response_received_at

                def rules_request(url: str) -> tuple[Any, bytes]:
                    if request is request_json:
                        return request_json(
                            url,
                            deadline=deadline,
                            max_bytes=MAX_CEX_TYPED_RULE_RESPONSE_BYTES,
                        )
                    return effective_request(url)

                rules_response, rules_raw = rules_request(
                    _typed_market_rules_request(
                        market["exchange"].lower(), book["source_instrument"]
                    )
                )
                if deadline is not None:
                    deadline.require_remaining()
                rules_observed_at = utc_now_text()
                rules, conversion = _typed_cex_source_payloads(
                    market,
                    book,
                    rules_response=rules_response,
                    rules_raw=rules_raw,
                    rules_observed_at=rules_observed_at,
                    conversion_observed_at=conversion_observed_at,
                )
            except CollectionDeadlineExceeded:
                raise
            except Exception:
                pass
            else:
                typed_source_payload_sink({
                    "role": "cex_market_rules",
                    "payload": _canonical_typed_json(rules),
                })
                typed_source_payload_sink({
                    "role": "quote_usd_conversion",
                    "payload": _canonical_typed_json(conversion),
                })
        try:
            market_execution_rows = execution_rows_for_book(
                market,
                book,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
            )
        except Exception as execution_error:
            market_execution_rows = failed_execution_rows(
                market,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                error=execution_error,
                source_endpoint=book["source_endpoint"],
                source_instrument=book["source_instrument"],
                source_quote_asset=book["source_quote_asset"],
                source_sequence=book.get("source_sequence", ""),
                raw_response_sha256=hashlib.sha256(book["raw"]).hexdigest(),
                status_reason="execution_calculation_failed",
            )
    except CollectionDeadlineExceeded:
        raise
    except Exception as error:
        response_received_at = utc_now_text()
        reason_code = depth_failure_reason_code(error)
        source_endpoint = ""
        source_instrument = ""
        source_quote_asset = ""
        source_raw = b""
        if isinstance(error, SourceBookError):
            source_endpoint = error.endpoint
            source_instrument = error.source_instrument
            source_quote_asset = error.source_quote_asset
            source_raw = error.raw
        elif market["exchange"] != "upbit":
            try:
                (
                    source_endpoint,
                    source_instrument,
                    source_quote_asset,
                    _full_book,
                ) = source_request(market["exchange"], market["cex_symbol"])
            except ValueError:
                pass
        if source_raw:
            raw_path.write_bytes(source_raw)
            raw_response_sha256 = hashlib.sha256(source_raw).hexdigest()
        else:
            error_payload = json.dumps(
                {
                    "market": market,
                    "source_endpoint": source_endpoint,
                    "request_started_at": request_started_at,
                    "response_received_at": response_received_at,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
            raw_path.write_bytes(error_payload)
            raw_response_sha256 = ""
        row = failure_row(
            market,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            error=error,
            source_endpoint=source_endpoint,
            source_instrument=source_instrument,
            source_quote_asset=source_quote_asset,
            raw_response_sha256=raw_response_sha256,
            reason_code=reason_code,
        )
        market_execution_rows = failed_execution_rows(
            market,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            error=error,
            source_endpoint=source_endpoint,
            source_instrument=source_instrument,
            source_quote_asset=source_quote_asset,
            raw_response_sha256=raw_response_sha256,
            status_reason=reason_code,
        )
    return row, market_execution_rows


def collect_depth_with_execution(
    markets: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    allow_terminal_only: bool = False,
) -> tuple[str, list[dict[str, str]], list[dict[str, str]]]:
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    snapshot_raw_dir = raw_root / snapshot_id
    snapshot_raw_dir.mkdir(parents=True, exist_ok=False)
    rows: list[dict[str, str]] = []
    execution_rows: list[dict[str, str]] = []

    for index, market in enumerate(markets, start=1):
        raw_path = snapshot_raw_dir / (
            f"{index:03d}-{safe_component(market['exchange'])}-"
            f"{safe_component(market['token_symbol'])}.json"
        )
        row, market_execution_rows = collect_cex_market_observation(
            market,
            snapshot_id=snapshot_id,
            raw_path=raw_path,
            request=request,
        )
        rows.append(row)
        execution_rows.extend(market_execution_rows)
        print(
            f"[{index}/{len(markets)}] {market['token_symbol']} "
            f"{market['exchange']}: {row['status']}",
            flush=True,
        )
        if index < len(markets) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now_text(),
        "market_count": len(rows),
        "token_count": len({row["token_symbol"] for row in rows}),
        "exchange_count": len({row["exchange"] for row in rows}),
        "depth_bands_bps": list(DEPTH_BANDS_BPS),
        "execution_notionals_usd": [int(value) for value in EXECUTION_NOTIONALS_USD],
        "execution_cost_row_count": len(execution_rows),
        "execution_cost_status_counts": execution_status_counts(execution_rows),
        "execution_cost_fee_status": "excluded_unknown_account_tier",
        "status_counts": {
            status: sum(row["status"] == status for row in rows)
            for status in ("observed", "partial", "failed")
        },
        "reason_code_counts": {
            reason: sum(row["reason_code"] == reason for row in rows)
            for reason in sorted({row["reason_code"] for row in rows})
        },
        "raw_files": sorted(path.name for path in snapshot_raw_dir.glob("*.json")),
    }
    (snapshot_raw_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validate_snapshot(
        markets,
        rows,
        allow_terminal_only=allow_terminal_only,
    )
    validate_execution_snapshot(
        [cex_market_id(market) for market in markets],
        execution_rows,
    )
    return snapshot_id, rows, execution_rows


def collect_depth(
    markets: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    allow_terminal_only: bool = False,
) -> tuple[str, list[dict[str, str]]]:
    """Preserve the original depth-only return contract for existing callers."""
    snapshot_id, rows, _execution_rows = collect_depth_with_execution(
        markets,
        raw_root=raw_root,
        request=request,
        sleep_seconds=sleep_seconds,
        allow_terminal_only=allow_terminal_only,
    )
    return snapshot_id, rows


def validate_snapshot(
    inventory: list[dict[str, str]],
    rows: list[dict[str, str]],
    *,
    allow_terminal_only: bool = False,
    allow_no_observed: bool = False,
) -> None:
    expected = {
        (row["token_symbol"].upper(), row["exchange"].lower(), row["cex_symbol"].upper())
        for row in inventory
    }
    actual = {
        (row["token_symbol"].upper(), row["exchange"].lower(), row["cex_symbol"].upper())
        for row in rows
    }
    if len(rows) != len(actual):
        raise ValueError("CEX depth snapshot contains duplicate market rows")
    if expected != actual:
        raise ValueError("CEX depth snapshot coverage does not match the published inventory")
    validate_observation_bounds(rows)
    if any(row["status"] not in {"observed", "partial", "failed"} for row in rows):
        raise ValueError("CEX depth snapshot contains an invalid status")
    if any(row.get("reason_code") not in CEX_DEPTH_REASON_CODES for row in rows):
        raise ValueError("CEX depth snapshot contains an invalid reason code")
    if allow_terminal_only and any(
        quality_outcome_resolution_state(
            *normalize_cex_source_outcome(
                row.get("status"),
                row.get("reason_code"),
                row.get("error"),
            )
        ) not in {"observed", "confirmed_terminal_absence"}
        for row in rows
    ):
        raise ValueError(
            "CEX refresh is not a terminal non-retryable or resolved exact candidate"
        )
    if (
        not allow_no_observed
        and not any(row["status"] in {"observed", "partial"} for row in rows)
    ):
        if not allow_terminal_only:
            raise ValueError("CEX depth snapshot contains no observed order books")
        if any(
            quality_outcome_resolution_state(
                *normalize_cex_source_outcome(
                    row.get("status"),
                    row.get("reason_code"),
                    row.get("error"),
                )
            ) != "confirmed_terminal_absence"
            for row in rows
        ):
            raise ValueError(
                "CEX exact candidate is not a terminal non-retryable outcome"
            )
    for row in rows:
        if row["status"] not in {"observed", "partial"}:
            continue
        if float(row["best_bid"]) >= float(row["best_ask"]):
            raise ValueError("CEX depth snapshot contains a crossed or locked book")
        for band in DEPTH_BANDS_BPS:
            for side in ("bid", "ask", "total"):
                value = float(row[f"{side}_depth_{band}bps_usd"])
                if not math.isfinite(value) or value < 0:
                    raise ValueError("CEX depth snapshot contains invalid depth")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def merge_exact_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    target_market_id: str,
    publish_dir: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Merge one collected CEX market into the validated full publication."""
    baseline_depth = read_csv_rows(publish_dir / LATEST_FILENAME)
    baseline_execution = read_csv_rows(
        publish_dir / EXECUTION_LATEST_FILENAME
    )
    require_aligned_depth_execution_lineage(
        baseline_depth,
        baseline_execution,
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    merged_depth = merge_exact_market_snapshot(
        baseline_depth,
        depth_rows,
        target_market_id=target_market_id,
        market_id_for_row=cex_market_id,
        row_identity=cex_market_id,
    )
    merged_execution = merge_exact_market_snapshot(
        baseline_execution,
        execution_rows,
        target_market_id=target_market_id,
        market_id_for_row=lambda row: str(row.get("market_id") or ""),
        row_identity=lambda row: (
            str(row.get("market_id") or ""),
            str(row.get("direction") or ""),
            str(row.get("requested_notional_usd") or ""),
        ),
        rebind_source_snapshot_id=True,
    )
    depth_snapshot_ids = {row["snapshot_id"] for row in merged_depth}
    execution_snapshot_ids = {
        row["snapshot_id"] for row in merged_execution
    }
    if depth_snapshot_ids != execution_snapshot_ids:
        raise ValueError(
            "bounded CEX depth and execution publications are not coherent"
        )
    validate_snapshot(
        baseline_depth,
        merged_depth,
        allow_no_observed=True,
    )
    expected_market_ids = {cex_market_id(row) for row in baseline_depth}
    validate_execution_snapshot(expected_market_ids, merged_execution)
    return merged_depth, merged_execution


def depth_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path) if latest_path.exists() else None
    report = enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="cex_depth",
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            row.get("exchange", "").strip().lower(),
            row.get("cex_symbol", "").strip().upper(),
        ),
        cohort=lambda row: row.get("exchange", "").strip().lower(),
        usable_statuses={"observed", "partial"},
        valid_statuses={"observed", "partial", "failed"},
        minimum_candidate_usable_bps=MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        minimum_baseline_retention_bps=MINIMUM_BASELINE_RETENTION_BPS,
    )
    return bind_passing_coverage_report(
        report,
        fact_family="cex_depth",
        baseline_path=latest_path,
    )


def execution_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path) if latest_path.exists() else None
    report = enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="cex_execution_cost",
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        cohort=lambda row: row.get("exchange", "").strip().lower(),
        usable_statuses={"observed", "partial"},
        valid_statuses={"observed", "partial", "failed"},
        minimum_candidate_usable_bps=MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        minimum_baseline_retention_bps=MINIMUM_BASELINE_RETENTION_BPS,
    )
    return bind_passing_coverage_report(
        report,
        fact_family="cex_execution_cost",
        baseline_path=latest_path,
    )


def exact_depth_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str,
) -> dict[str, Any]:
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path)
    scope = validate_exact_publication_scope(
        baseline_rows,
        rows,
        target_market_id=target_market_id,
        market_id_for_row=cex_market_id,
        row_identity=cex_market_id,
    )
    report = enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="cex_depth",
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            row.get("exchange", "").strip().lower(),
            row.get("cex_symbol", "").strip().upper(),
        ),
        usable_statuses={"observed", "partial"},
        valid_statuses={"observed", "partial", "failed"},
        minimum_candidate_usable_bps=0,
        minimum_baseline_retention_bps=10_000,
    )
    target_rows = [
        row for row in rows if cex_market_id(row) == target_market_id
    ]
    resolutions = {
        quality_outcome_resolution_state(
            *normalize_cex_source_outcome(
                row.get("status"),
                row.get("reason_code"),
                row.get("error"),
            )
        )
        for row in target_rows
    }
    report.update(
        {
            "mode": "exact_target_recovery/v1",
            "exact_target": {**scope, "resolutions": sorted(resolutions)},
        }
    )
    if not target_rows or not resolutions.issubset(
        {"observed", "confirmed_terminal_absence"}
    ):
        report["status"] = "rejected"
        report["passed"] = False
        report["reasons"] = list(
            dict.fromkeys(
                list(report.get("reasons") or [])
                + ["exact_target_unresolved"]
            )
        )
        raise CoverageRegressionError(report)
    return bind_passing_coverage_report(
        report,
        fact_family="cex_depth",
        baseline_path=latest_path,
    )


def exact_execution_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str,
) -> dict[str, Any]:
    latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path)
    scope = validate_exact_publication_scope(
        baseline_rows,
        rows,
        target_market_id=target_market_id,
        market_id_for_row=lambda row: str(row.get("market_id") or ""),
        row_identity=lambda row: (
            str(row.get("market_id") or ""),
            str(row.get("direction") or ""),
            str(row.get("requested_notional_usd") or ""),
        ),
        rebound_fields=("snapshot_id", "source_snapshot_id"),
    )
    report = enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="cex_execution_cost",
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        usable_statuses={"observed", "partial"},
        valid_statuses={"observed", "partial", "failed"},
        minimum_candidate_usable_bps=0,
        minimum_baseline_retention_bps=10_000,
    )
    report.update(
        {
            "mode": "exact_target_recovery/v1",
            "exact_target": scope,
        }
    )
    return bind_passing_coverage_report(
        report,
        fact_family="cex_execution_cost",
        baseline_path=latest_path,
    )


def preflight_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Reject either coverage regression before writing either latest view."""
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    if target_market_id is not None:
        target = str(target_market_id).strip()
        if not target:
            raise ValueError("exact target market identity is empty")
        return enforce_publication_coverage_bundle(
            (
                (
                    "cex_depth",
                    lambda: exact_depth_publication_coverage_gate(
                        depth_rows,
                        publish_dir,
                        target_market_id=target,
                    ),
                ),
                (
                    "cex_execution_cost",
                    lambda: exact_execution_publication_coverage_gate(
                        execution_rows,
                        publish_dir,
                        target_market_id=target,
                    ),
                ),
            ),
            bundle="cex_depth_execution_exact",
        )
    return enforce_publication_coverage_bundle(
        (
            (
                "cex_depth",
                lambda: depth_publication_coverage_gate(
                    depth_rows,
                    publish_dir,
                ),
            ),
            (
                "cex_execution_cost",
                lambda: execution_publication_coverage_gate(
                    execution_rows,
                    publish_dir,
                ),
            ),
        ),
        bundle="cex_depth_execution",
    )


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DEPTH_COLUMNS_ALL, lineterminator="\n")
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in DEPTH_COLUMNS_ALL}
                for row in rows
            )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_execution_csv(
    path: Path,
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=EXECUTION_COST_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in EXECUTION_COST_COLUMNS}
                for row in rows
            )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_snapshot(
    rows: list[dict[str, str]],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
    preflight_report: dict[str, Any] | None = None,
    history_rows_to_append: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)
    result: dict[str, Any] = {"current_path": str(current_path), "row_count": len(rows)}
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    publication_gate = (
        validate_passing_coverage_report(
            preflight_report,
            fact_family="cex_depth",
            candidate_rows=rows,
            identity=lambda row: (
                row.get("token_symbol", "").strip().upper(),
                row.get("exchange", "").strip().lower(),
                row.get("cex_symbol", "").strip().upper(),
            ),
            baseline_path=publish_dir / LATEST_FILENAME,
            expected_policy=COVERAGE_POLICY,
        )
        if preflight_report is not None
        else depth_publication_coverage_gate(rows, publish_dir)
    )
    history_path = publish_dir / HISTORY_FILENAME
    existing_history = read_csv_rows(history_path)
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ): row
        for row in existing_history
    }
    for row in (
        rows if history_rows_to_append is None else history_rows_to_append
    ):
        merged[
            (
                row["snapshot_id"],
                row["token_symbol"],
                row["exchange"],
                row["cex_symbol"],
            )
        ] = row
    history_rows = sorted(
        merged.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ),
    )
    atomic_write_csv(history_path, history_rows)
    atomic_write_csv(publish_dir / LATEST_FILENAME, rows)
    atomic_write_csv(publish_dir / CURRENT_FILENAME, rows)
    result.update(
        {
            "latest_path": str(publish_dir / LATEST_FILENAME),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": publication_gate,
        }
    )
    return result


def publish_execution_snapshot(
    rows: list[dict[str, str]],
    *,
    expected_market_ids: Iterable[str],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
    preflight_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validate_execution_snapshot(expected_market_ids, rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / EXECUTION_CURRENT_FILENAME
    atomic_write_execution_csv(current_path, rows)
    result: dict[str, Any] = {
        "current_path": str(current_path),
        "row_count": len(rows),
        "status_counts": execution_status_counts(rows),
    }
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    publication_gate = (
        validate_passing_coverage_report(
            preflight_report,
            fact_family="cex_execution_cost",
            candidate_rows=rows,
            identity=lambda row: (
                row.get("market_id", "").strip(),
                row.get("direction", "").strip(),
                row.get("requested_notional_usd", "").strip(),
            ),
            baseline_path=publish_dir / EXECUTION_LATEST_FILENAME,
            expected_policy=COVERAGE_POLICY,
        )
        if preflight_report is not None
        else execution_publication_coverage_gate(rows, publish_dir)
    )
    atomic_write_execution_csv(
        publish_dir / EXECUTION_LATEST_FILENAME,
        rows,
    )
    result.update(
        {
            "latest_path": str(publish_dir / EXECUTION_LATEST_FILENAME),
            "publication_gate": publication_gate,
        }
    )
    return result


def _require_disjoint_publication_destinations(
    private_destinations: Iterable[Path],
    public_destinations: Iterable[Path],
) -> None:
    resolved_private = {
        path.resolve(strict=False) for path in private_destinations
    }
    resolved_public = {
        path.resolve(strict=False) for path in public_destinations
    }
    if resolved_private & resolved_public:
        raise ValueError(
            "private and public publication destinations overlap"
        )


def publish_full_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    output_dir: Path,
    publish_dir: Path,
    preflight_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Failure-atomically publish one full CEX depth/execution cohort."""
    current_path = output_dir / CURRENT_FILENAME
    execution_current_path = output_dir / EXECUTION_CURRENT_FILENAME
    history_path = publish_dir / HISTORY_FILENAME
    latest_path = publish_dir / LATEST_FILENAME
    public_current_path = publish_dir / CURRENT_FILENAME
    execution_latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    _require_disjoint_publication_destinations(
        (current_path, execution_current_path),
        (
            history_path,
            latest_path,
            public_current_path,
            execution_latest_path,
        ),
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    expected_market_ids = {cex_market_id(row) for row in depth_rows}
    validate_execution_snapshot(expected_market_ids, execution_rows)
    depth_gate = validate_passing_coverage_report(
        preflight_reports.get("cex_depth"),
        fact_family="cex_depth",
        candidate_rows=depth_rows,
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            row.get("exchange", "").strip().lower(),
            row.get("cex_symbol", "").strip().upper(),
        ),
        baseline_path=latest_path,
        expected_policy=COVERAGE_POLICY,
    )
    execution_gate = validate_passing_coverage_report(
        preflight_reports.get("cex_execution_cost"),
        fact_family="cex_execution_cost",
        candidate_rows=execution_rows,
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        baseline_path=execution_latest_path,
        expected_policy=COVERAGE_POLICY,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(current_path, depth_rows)
    atomic_write_execution_csv(execution_current_path, execution_rows)

    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ): row
        for row in read_csv_rows(history_path)
    }
    for row in depth_rows:
        merged_history[
            (
                row["snapshot_id"],
                row["token_symbol"],
                row["exchange"],
                row["cex_symbol"],
            )
        ] = row
    history_rows = sorted(
        merged_history.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ),
    )
    atomic_replace_bundle(
        (
            (history_path, csv_payload(DEPTH_COLUMNS_ALL, history_rows)),
            (
                latest_path,
                csv_payload(DEPTH_COLUMNS_ALL, depth_rows),
            ),
            (
                public_current_path,
                csv_payload(DEPTH_COLUMNS_ALL, depth_rows),
            ),
            (
                execution_latest_path,
                csv_payload(EXECUTION_COST_COLUMNS, execution_rows),
            ),
        )
    )
    return (
        {
            "current_path": str(current_path),
            "row_count": len(depth_rows),
            "latest_path": str(latest_path),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": depth_gate,
        },
        {
            "current_path": str(execution_current_path),
            "row_count": len(execution_rows),
            "status_counts": execution_status_counts(execution_rows),
            "latest_path": str(execution_latest_path),
            "publication_gate": execution_gate,
        },
    )


def publish_exact_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    target_market_id: str,
    history_rows_to_append: list[dict[str, str]],
    output_dir: Path,
    publish_dir: Path,
    preflight_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Failure-atomically publish one bounded CEX depth/execution merge."""
    current_path = output_dir / CURRENT_FILENAME
    execution_current_path = output_dir / EXECUTION_CURRENT_FILENAME
    history_path = publish_dir / HISTORY_FILENAME
    latest_path = publish_dir / LATEST_FILENAME
    public_current_path = publish_dir / CURRENT_FILENAME
    execution_latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    _require_disjoint_publication_destinations(
        (current_path, execution_current_path),
        (
            history_path,
            latest_path,
            public_current_path,
            execution_latest_path,
        ),
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    expected_market_ids = {
        str(row.get("market_id") or "") for row in execution_rows
    }
    validate_execution_snapshot(expected_market_ids, execution_rows)
    depth_gate = validate_passing_coverage_report(
        preflight_reports.get("cex_depth"),
        fact_family="cex_depth",
        candidate_rows=depth_rows,
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            row.get("exchange", "").strip().lower(),
            row.get("cex_symbol", "").strip().upper(),
        ),
        baseline_path=latest_path,
        expected_policy=EXACT_COVERAGE_POLICY,
    )
    execution_gate = validate_passing_coverage_report(
        preflight_reports.get("cex_execution_cost"),
        fact_family="cex_execution_cost",
        candidate_rows=execution_rows,
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        baseline_path=execution_latest_path,
        expected_policy=EXACT_COVERAGE_POLICY,
    )
    target = str(target_market_id or "").strip()
    for report in (depth_gate, execution_gate):
        exact_target = report.get("exact_target")
        if (
            report.get("mode") != "exact_target_recovery/v1"
            or not isinstance(exact_target, dict)
            or exact_target.get("market_id") != target
        ):
            raise ValueError("exact preflight report does not match target")
    if (
        depth_gate["exact_target"]["candidate_snapshot_id"]
        != execution_gate["exact_target"]["candidate_snapshot_id"]
    ):
        raise ValueError("exact preflight reports do not share one generation")
    published_target_rows = [
        row for row in depth_rows if cex_market_id(row) == target
    ]
    if (
        len(history_rows_to_append) != 1
        or len(published_target_rows) != 1
        or cex_market_id(history_rows_to_append[0]) != target
        or str(history_rows_to_append[0].get("snapshot_id") or "")
        != depth_gate["exact_target"]["candidate_snapshot_id"]
        or history_rows_to_append[0] != published_target_rows[0]
    ):
        raise ValueError("exact history append does not match target publication")

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(current_path, depth_rows)
    atomic_write_execution_csv(execution_current_path, execution_rows)

    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ): row
        for row in read_csv_rows(history_path)
    }
    for row in history_rows_to_append:
        merged_history[
            (
                row["snapshot_id"],
                row["token_symbol"],
                row["exchange"],
                row["cex_symbol"],
            )
        ] = row
    history_rows = sorted(
        merged_history.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ),
    )
    atomic_replace_bundle(
        (
            (history_path, csv_payload(DEPTH_COLUMNS_ALL, history_rows)),
            (
                latest_path,
                csv_payload(DEPTH_COLUMNS_ALL, depth_rows),
            ),
            (
                public_current_path,
                csv_payload(DEPTH_COLUMNS_ALL, depth_rows),
            ),
            (
                execution_latest_path,
                csv_payload(EXECUTION_COST_COLUMNS, execution_rows),
            ),
        )
    )
    return (
        {
            "current_path": str(current_path),
            "row_count": len(depth_rows),
            "latest_path": str(latest_path),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": depth_gate,
        },
        {
            "current_path": str(execution_current_path),
            "row_count": len(execution_rows),
            "status_counts": execution_status_counts(execution_rows),
            "latest_path": str(execution_latest_path),
            "publication_gate": execution_gate,
        },
    )


def parse_list(value: str | None, *, upper: bool) -> list[str] | None:
    if not value:
        return None
    transform = str.upper if upper else str.lower
    values = [transform(item.strip()) for item in value.split(",") if item.strip()]
    return values or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect point-in-time depth for all published CEX markets"
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--cex-csv", type=Path, default=DEFAULT_CEX_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--exchanges", help="Comma-separated exchange names")
    parser.add_argument(
        "--market-id",
        help="One canonical CEX market identity for bounded collection",
    )
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument(
        "--publish-dir",
        type=Path,
        help="Explicit runtime directory for an atomic publication",
    )
    parser.add_argument(
        "--merge-publish",
        action="store_true",
        help="Merge one exact market into an existing full publication",
    )
    parser.add_argument("--sleep-seconds", type=float, default=REQUEST_SLEEP_SECONDS)
    return parser.parse_args()


def ensure_full_publish_scope(
    publish_local: bool,
    *filters: set[str],
) -> None:
    if publish_local and any(filters):
        raise ValueError(
            "--publish-local cannot be combined with token or exchange filters"
        )


def main() -> None:
    args = parse_args()
    markets = load_cataloged_markets(args.database, args.cex_csv)
    tokens = set(parse_list(args.tokens, upper=True) or [])
    exchanges = set(parse_list(args.exchanges, upper=False) or [])
    market_id = str(args.market_id or "").strip()
    publish_dir = (
        args.publish_dir
        if args.publish_dir is not None
        else DEFAULT_PUBLISH_DIR if args.publish_local else None
    )
    if market_id and (tokens or exchanges):
        raise ValueError("--market-id cannot be combined with other filters")
    if args.merge_publish and (publish_dir is None or not market_id):
        raise ValueError(
            "--merge-publish requires --publish-dir and --market-id"
        )
    if publish_dir is not None and market_id and not args.merge_publish:
        raise ValueError(
            "filtered publication requires explicit --merge-publish"
        )
    if not args.merge_publish:
        ensure_full_publish_scope(publish_dir is not None, tokens, exchanges)
    if tokens:
        markets = [row for row in markets if row["token_symbol"] in tokens]
    if exchanges:
        markets = [row for row in markets if row["exchange"] in exchanges]
    if market_id:
        markets = [row for row in markets if cex_market_id(row) == market_id]
    if not markets:
        raise ValueError("No cataloged CEX markets match the requested filters")

    snapshot_id, rows, execution_rows = collect_depth_with_execution(
        markets,
        raw_root=args.raw_root,
        sleep_seconds=max(0.0, args.sleep_seconds),
        allow_terminal_only=args.merge_publish,
    )
    collected_rows = rows
    collected_execution_rows = execution_rows
    if args.merge_publish:
        assert publish_dir is not None
        rows, execution_rows = merge_exact_publication_bundle(
            rows,
            execution_rows,
            target_market_id=market_id,
            publish_dir=publish_dir,
        )
    publication_gates = (
        preflight_publication_bundle(
            rows,
            execution_rows,
            publish_dir,
            target_market_id=market_id if args.merge_publish else None,
        )
        if publish_dir is not None
        else {}
    )
    if args.merge_publish:
        assert publish_dir is not None
        result, execution_result = publish_exact_publication_bundle(
            rows,
            execution_rows,
            target_market_id=market_id,
            history_rows_to_append=collected_rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
            preflight_reports=publication_gates,
        )
    elif publish_dir is not None:
        result, execution_result = publish_full_publication_bundle(
            rows,
            execution_rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
            preflight_reports=publication_gates,
        )
    else:
        result = publish_snapshot(
            rows,
            output_dir=args.output_dir,
        )
        execution_result = publish_execution_snapshot(
            execution_rows,
            output_dir=args.output_dir,
            expected_market_ids={
                str(row.get("market_id") or "") for row in execution_rows
            },
        )
    depth_gate = result.pop("publication_gate", None)
    execution_gate = execution_result.pop("publication_gate", None)
    result["execution_cost"] = execution_result
    result.update(
        {
            "snapshot_id": snapshot_id,
            "token_count": len({row["token_symbol"] for row in rows}),
            "exchange_count": len({row["exchange"] for row in rows}),
            "market_count": len(rows),
            "observed_count": sum(row["status"] == "observed" for row in rows),
            "partial_count": sum(row["status"] == "partial" for row in rows),
            "failed_count": sum(row["status"] == "failed" for row in rows),
            "execution_cost_row_count": len(execution_rows),
            "collected_market_count": len(collected_rows),
            "collected_execution_cost_row_count": len(
                collected_execution_rows
            ),
            "execution_cost_status_counts": execution_status_counts(
                execution_rows
            ),
        }
    )
    result_publication_gates = {
        name: gate
        for name, gate in (
            ("cex_depth", depth_gate),
            ("cex_execution_cost", execution_gate),
        )
        if gate is not None
    }
    if result_publication_gates:
        result["publication_gates"] = result_publication_gates
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
