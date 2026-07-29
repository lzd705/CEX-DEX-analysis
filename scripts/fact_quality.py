#!/usr/bin/env python3
"""Audit daily CEX/DEX facts without modifying the published data.

The checker deliberately separates three kinds of evidence:

* hard-invalid rows, which require human review and are never auto-retried;
* historical holes between a market's first and last observed dates;
* an active market missing the latest completed UTC day (D-1).

The daily CSVs contain successful observations, not collection-attempt records.
Consequently, an absent date is labelled ``backfill_pending`` rather than
``collection_failed``.  A collector may only use the latter when it has a
structured failed-attempt record.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import quote, urlencode


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CEX_CSV = PROJECT_ROOT / "data/local/cex_exchange_volume_daily.csv"
DEFAULT_DEX_CSV = PROJECT_ROOT / "data/local/dex_pool_volume_daily.csv"
REPORT_SCHEMA = "fact_quality_report/v1"
ATTEMPT_SCHEMA = "daily_collection_attempts/v1"
ACTIVE_LOOKBACK_DAYS = 7
ACTIVE_MIN_OBSERVATIONS = 3
MAX_RETRY_WINDOW_DAYS = 180
ATTEMPT_STATUSES = {
    "succeeded",
    "partial",
    "no_data",
    "failed",
    "unsupported",
}
ATTEMPT_REASON_CODES = {
    "observed",
    "network",
    "rate_limit",
    "source_unavailable",
    "not_listed",
    "no_candles",
    "parse",
    "validation",
    "source_range_unavailable",
}
ATTEMPT_OUTCOMES = {
    "observed",
    "partial_observation",
    "no_candles",
    "request_failed",
    "range_unavailable",
}
RETRYABLE_ATTEMPT_REASONS = {
    "network",
    "rate_limit",
    "source_unavailable",
    "parse",
    "validation",
}

CEX_REQUIRED_COLUMNS = {
    "date",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
}
DEX_REQUIRED_COLUMNS = {
    "date",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "open",
    "high",
    "low",
    "close",
    "dex_volume_usd",
    "pool_tvl_usd",
}
OHLC_FIELDS = ("open", "high", "low", "close")
CATEGORY_ORDER = {
    "hard_invalid": 0,
    "d1_active_gap": 1,
    "stale_market_unknown": 2,
    "historical_gap": 3,
}


class FactQualityInputError(ValueError):
    """Raised when a source CSV cannot satisfy the audit input contract."""


def normalize_collection_attempts(
    raw_attempts: Sequence[Mapping[str, Any]],
    *,
    market_type: str,
) -> List[Dict[str, Any]]:
    """Validate and reduce attempt records to the bounded public contract.

    The returned dictionaries contain no collector-specific exception objects,
    request URLs, or filesystem paths. Callers that carry evidence between
    append publications must use this validator instead of copying raw JSON.
    """

    if market_type not in {"cex", "dex"}:
        raise ValueError("unknown collection-attempt market type")
    normalized: List[Dict[str, Any]] = []
    for raw in raw_attempts:
        if not isinstance(raw, dict):
            raise ValueError("attempt is not an object")
        status = str(raw.get("status") or "")
        reason_code = str(raw.get("reason_code") or "")
        outcome = str(raw.get("outcome") or "")
        if status not in ATTEMPT_STATUSES:
            raise ValueError("unknown attempt status")
        if reason_code not in ATTEMPT_REASON_CODES:
            raise ValueError("unknown attempt reason")
        if outcome not in ATTEMPT_OUTCOMES:
            raise ValueError("unknown attempt outcome")
        valid_status_reasons = (
            status == "succeeded"
            and reason_code == "observed"
            and outcome == "observed"
        ) or (
            status in {"partial", "no_data"}
            and reason_code == "no_candles"
            and outcome in {"partial_observation", "no_candles"}
        ) or (
            status == "unsupported"
            and reason_code == "source_range_unavailable"
            and outcome == "range_unavailable"
        ) or (
            status == "failed"
            and reason_code
            in {
                "network",
                "rate_limit",
                "source_unavailable",
                "not_listed",
                "parse",
                "validation",
            }
            and outcome == "request_failed"
        )
        if not valid_status_reasons:
            raise ValueError("attempt status and reason are inconsistent")
        if raw.get("market_type") != market_type:
            raise ValueError("attempt market type does not match collector")
        token = str(raw.get("token_symbol") or "").strip().upper()
        if not token:
            raise ValueError("attempt Token identity is missing")
        start_text = raw.get("requested_start_date")
        end_text = raw.get("requested_end_date")
        if bool(start_text) != bool(end_text):
            raise ValueError("attempt window is incomplete")
        if start_text and end_text:
            start_day = date.fromisoformat(str(start_text))
            end_day = date.fromisoformat(str(end_text))
            day_count = (end_day - start_day).days + 1
            if day_count < 1 or day_count > MAX_RETRY_WINDOW_DAYS:
                raise ValueError("attempt window is outside the supported range")
        observed_dates = raw.get("observed_dates")
        if not isinstance(observed_dates, list):
            raise ValueError("observed_dates is not a list")
        normalized_dates = sorted(
            {
                date.fromisoformat(str(item)).isoformat()
                for item in observed_dates
            }
        )
        if raw.get("observed_day_count") != len(normalized_dates):
            raise ValueError("observed_day_count does not match observed_dates")
        error_text = raw.get("error")
        if error_text is not None:
            error_text = str(error_text).strip()
            if (
                not error_text
                or len(error_text) > 240
                or "://" in error_text
                or any(
                    marker in error_text
                    for marker in (
                        "/Users/",
                        "/home/",
                        "/private/",
                        "\\\\",
                    )
                )
            ):
                raise ValueError("attempt error is not safely bounded")
        if reason_code == "observed" and error_text is not None:
            raise ValueError("successful attempt cannot contain an error")
        if reason_code != "observed" and error_text is None:
            raise ValueError("non-success attempt must contain a bounded error")
        http_status = raw.get("http_status")
        if http_status is not None and (
            not isinstance(http_status, int)
            or isinstance(http_status, bool)
            or not 100 <= http_status <= 599
        ):
            raise ValueError("attempt HTTP status is invalid")
        pool_address = str(raw.get("pool_address") or "").strip() or None
        if pool_address and pool_address.startswith("0x"):
            pool_address = pool_address.lower()
        normalized.append(
            {
                "attempt_id": str(raw.get("attempt_id") or "")[:64],
                "market_type": market_type,
                "token_symbol": token,
                "exchange": (
                    str(raw.get("exchange") or "").strip().lower() or None
                ),
                "instrument": (
                    str(raw.get("instrument") or "").strip().upper() or None
                ),
                "chain": str(raw.get("chain") or "").strip().lower() or None,
                "dex": str(raw.get("dex") or "").strip().lower() or None,
                "pool_address": pool_address,
                "requested_start_date": str(start_text) if start_text else None,
                "requested_end_date": str(end_text) if end_text else None,
                "observed_dates": normalized_dates,
                "observed_day_count": len(normalized_dates),
                "status": status,
                "outcome": outcome,
                "reason_code": reason_code,
                "http_status": http_status,
                "error": error_text,
                "finished_at_utc": (
                    str(raw.get("finished_at_utc") or "")[:64] or None
                ),
            }
        )
    return normalized


def _attempt_source(
    *,
    path: Optional[Path],
    market_type: str,
    source_csv_sha256: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load only lineage-matched, bounded attempt evidence.

    Attempt evidence is optional. Invalid or stale ledgers are ignored rather
    than used to invent a collection-failure cause for a missing fact.
    """

    metadata: Dict[str, Any] = {
        "market_type": market_type,
        "status": "absent",
        "attempt_count": 0,
        "sha256": None,
        "reason": None,
    }
    if path is None or not path.exists():
        return [], metadata
    metadata["sha256"] = sha256_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("ledger root is not an object")
        if payload.get("schema") != ATTEMPT_SCHEMA:
            raise ValueError("unsupported ledger schema")
        if payload.get("collector") != market_type:
            raise ValueError("collector does not match market type")
        if payload.get("source_csv_sha256") != source_csv_sha256:
            metadata["status"] = "ignored_stale"
            metadata["reason"] = "source_csv_sha256_mismatch"
            return [], metadata
        raw_attempts = payload.get("attempts")
        if (
            not isinstance(raw_attempts, list)
            or payload.get("attempt_count") != len(raw_attempts)
        ):
            raise ValueError("attempt_count does not match attempts")

        normalized = normalize_collection_attempts(
            raw_attempts,
            market_type=market_type,
        )
        metadata["status"] = "accepted"
        metadata["attempt_count"] = len(normalized)
        metadata["reason_counts"] = dict(
            sorted(Counter(item["reason_code"] for item in normalized).items())
        )
        return normalized, metadata
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        metadata["status"] = "ignored_invalid"
        metadata["reason"] = "invalid_attempt_ledger"
        return [], metadata


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize_address(value: str) -> str:
    address = value.strip()
    return address.lower() if address.startswith("0x") else address


def cex_market(row: Mapping[str, str]) -> Dict[str, Any]:
    token = (row.get("token_symbol") or "").strip().upper()
    exchange = (row.get("exchange") or "").strip().lower()
    instrument = (row.get("cex_symbol") or "").strip().upper()
    return {
        "market_id": (
            "cex:{}:{}".format(exchange, instrument)
            if exchange and instrument
            else None
        ),
        "market_type": "cex",
        "token_symbol": token or None,
        "exchange": exchange or None,
        "instrument": instrument or None,
        "chain": None,
        "dex": None,
        "pool_address": None,
    }


def dex_market(row: Mapping[str, str]) -> Dict[str, Any]:
    token = (row.get("token_symbol") or "").strip().upper()
    chain = (row.get("chain") or "").strip().lower()
    dex = (row.get("dex") or "").strip().lower()
    address = normalize_address(row.get("pool_address") or "")
    return {
        "market_id": (
            "dex:{}:{}:{}:{}".format(chain, dex, address, token)
            if chain and dex and address and token
            else None
        ),
        "market_type": "dex",
        "token_symbol": token or None,
        "exchange": None,
        "instrument": (row.get("pool_name") or "").strip() or None,
        "chain": chain or None,
        "dex": dex or None,
        "pool_address": address or None,
    }


def source_url_hints(market: Mapping[str, Any]) -> List[str]:
    """Return source endpoints suitable for a human spot check.

    These are hints, not evidence that a request was made.  The review record
    must still retain the actual response and its hash.
    """

    if market.get("market_type") == "dex":
        chain = market.get("chain")
        address = market.get("pool_address")
        if not chain or not address:
            return []
        return [
            (
                "https://api.geckoterminal.com/api/v2/networks/{}/pools/{}/"
                "ohlcv/day?{}"
            ).format(
                quote(str(chain), safe=""),
                quote(str(address), safe=""),
                urlencode({"aggregate": "1", "currency": "usd", "limit": "30"}),
            )
        ]

    exchange = str(market.get("exchange") or "").lower()
    instrument = str(market.get("instrument") or "").upper()
    if not exchange or not instrument:
        return []
    base, _, quote_asset = instrument.partition("/")
    compact = "{}{}".format(base, quote_asset)
    dashed = "{}-{}".format(base, quote_asset)
    underscored = "{}_{}".format(base, quote_asset)
    urls = {
        "binance": "https://api.binance.com/api/v3/klines?{}".format(
            urlencode({"symbol": compact, "interval": "1d", "limit": "30"})
        ),
        "okx": "https://www.okx.com/api/v5/market/candles?{}".format(
            urlencode({"instId": dashed, "bar": "1Dutc", "limit": "30"})
        ),
        "bybit": "https://api.bybit.com/v5/market/kline?{}".format(
            urlencode(
                {
                    "category": "spot",
                    "symbol": compact,
                    "interval": "D",
                    "limit": "30",
                }
            )
        ),
        "kucoin": "https://api.kucoin.com/api/v1/market/candles?{}".format(
            urlencode({"symbol": dashed, "type": "1day"})
        ),
        "gate": "https://api.gateio.ws/api/v4/spot/candlesticks?{}".format(
            urlencode({"currency_pair": underscored, "interval": "1d", "limit": "30"})
        ),
        "bitget": "https://api.bitget.com/api/v2/spot/market/candles?{}".format(
            urlencode({"symbol": compact, "granularity": "1day", "limit": "30"})
        ),
        "mexc": "https://api.mexc.com/api/v3/klines?{}".format(
            urlencode({"symbol": compact, "interval": "1d", "limit": "30"})
        ),
        "htx": "https://api.huobi.pro/market/history/kline?{}".format(
            urlencode({"symbol": compact.lower(), "period": "1day", "size": "30"})
        ),
        "coinbase": (
            "https://api.exchange.coinbase.com/products/{}/candles?{}"
        ).format(
            quote("{}-USD".format(base), safe="-"),
            urlencode({"granularity": "86400"}),
        ),
        "kraken": "https://api.kraken.com/0/public/OHLC?{}".format(
            urlencode({"pair": "{}USD".format(base), "interval": "1440"})
        ),
        "crypto_com": (
            "https://api.crypto.com/exchange/v1/public/get-candlestick?{}"
        ).format(
            urlencode({"instrument_name": underscored, "timeframe": "1D"})
        ),
        "upbit": "https://api.upbit.com/v1/candles/days?{}".format(
            urlencode({"market": "KRW-{}".format(base), "count": "30"})
        ),
    }
    return [urls[exchange]] if exchange in urls else []


def parse_iso_day(value: str) -> Optional[date]:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None or str(value).strip() == "":
        return None
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def issue_id(
    category: str,
    reason_code: str,
    market_id: Optional[str],
    day_text: Optional[str],
    details: Mapping[str, Any],
) -> str:
    material = json.dumps(
        {
            "category": category,
            "reason_code": reason_code,
            "market_id": market_id,
            "date": day_text,
            "details": details,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]


def make_issue(
    *,
    category: str,
    status: str,
    reason_code: str,
    retryable: bool,
    market: Mapping[str, Any],
    day_text: Optional[str],
    message: str,
    details: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    detail_payload = dict(details or {})
    return {
        "issue_id": issue_id(
            category,
            reason_code,
            market.get("market_id"),
            day_text,
            detail_payload,
        ),
        "category": category,
        "status": status,
        "reason_code": reason_code,
        "retryable": retryable,
        "market": dict(market),
        "date": day_text,
        "message": message,
        "details": detail_payload,
        "source_url_hints": source_url_hints(market),
    }


def validate_header(
    path: Path,
    fieldnames: Optional[Sequence[str]],
    required: Set[str],
) -> None:
    missing = sorted(required - set(fieldnames or []))
    if missing:
        raise FactQualityInputError(
            "{} is missing required columns: {}".format(path, ", ".join(missing))
        )


def validate_row_values(
    row: Mapping[str, str],
    *,
    market: Mapping[str, Any],
    day_text: Optional[str],
    row_number: int,
) -> Tuple[List[Dict[str, Any]], bool]:
    issues: List[Dict[str, Any]] = []
    parsed_ohlc = {field: parse_decimal(row.get(field)) for field in OHLC_FIELDS}
    invalid_ohlc = sorted(
        field
        for field, value in parsed_ohlc.items()
        if value is None or value <= 0
    )
    if invalid_ohlc:
        issues.append(
            make_issue(
                category="hard_invalid",
                status="invalid",
                reason_code="invalid_positive_ohlc",
                retryable=False,
                market=market,
                day_text=day_text,
                message="OHLC values must all be finite and strictly positive.",
                details={
                    "fields": invalid_ohlc,
                    "row_number": row_number,
                    "observed_values": {
                        field: row.get(field) for field in invalid_ohlc
                    },
                },
            )
        )
    else:
        open_value = parsed_ohlc["open"]
        high_value = parsed_ohlc["high"]
        low_value = parsed_ohlc["low"]
        close_value = parsed_ohlc["close"]
        assert open_value is not None
        assert high_value is not None
        assert low_value is not None
        assert close_value is not None
        bounds_valid = (
            high_value >= low_value
            and high_value >= max(open_value, close_value)
            and low_value <= min(open_value, close_value)
        )
        if not bounds_valid:
            issues.append(
                make_issue(
                    category="hard_invalid",
                    status="invalid",
                    reason_code="inconsistent_ohlc_bounds",
                    retryable=False,
                    market=market,
                    day_text=day_text,
                    message=(
                        "Daily high/low do not contain open and close in a "
                        "consistent price interval."
                    ),
                    details={
                        "row_number": row_number,
                        "observed_values": {
                            field: row.get(field) for field in OHLC_FIELDS
                        },
                    },
                )
            )

    volume_fields = (
        ("base_volume", "quote_volume_usd")
        if market.get("market_type") == "cex"
        else ("dex_volume_usd",)
    )
    parsed_volumes = {
        field: parse_decimal(row.get(field)) for field in volume_fields
    }
    invalid_volumes = sorted(
        field
        for field, value in parsed_volumes.items()
        if value is None or value < 0
    )
    if invalid_volumes:
        issues.append(
            make_issue(
                category="hard_invalid",
                status="invalid",
                reason_code="invalid_non_negative_volume",
                retryable=False,
                market=market,
                day_text=day_text,
                message="Volume values must be finite and non-negative.",
                details={
                    "fields": invalid_volumes,
                    "row_number": row_number,
                    "observed_values": {
                        field: row.get(field) for field in invalid_volumes
                    },
                },
            )
        )

    if market.get("market_type") == "dex":
        raw_pool_tvl = row.get("pool_tvl_usd")
        if raw_pool_tvl is not None and str(raw_pool_tvl).strip():
            pool_tvl = parse_decimal(raw_pool_tvl)
            if pool_tvl is None or pool_tvl < 0:
                issues.append(
                    make_issue(
                        category="hard_invalid",
                        status="invalid",
                        reason_code="invalid_non_negative_pool_tvl",
                        retryable=False,
                        market=market,
                        day_text=day_text,
                        message=(
                            "Pool TVL must be finite and non-negative when "
                            "the source supplies it."
                        ),
                        details={
                            "fields": ["pool_tvl_usd"],
                            "row_number": row_number,
                            "observed_values": {
                                "pool_tvl_usd": raw_pool_tvl,
                            },
                        },
                    )
                )
    return issues, not issues


def read_daily_source(
    path: Path,
    market_type: str,
    *,
    latest_completed_day: Optional[date] = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError("Daily fact CSV does not exist: {}".format(path))
    required = CEX_REQUIRED_COLUMNS if market_type == "cex" else DEX_REQUIRED_COLUMNS
    market_builder = cex_market if market_type == "cex" else dex_market
    series: Dict[str, Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []
    primary_key_rows: Dict[Tuple[str, ...], List[int]] = defaultdict(list)
    row_count = 0

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        validate_header(path, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            row_count += 1
            market = market_builder(row)
            raw_day = (row.get("date") or "").strip()
            parsed_day = parse_iso_day(raw_day)
            identity_fields = (
                ("token_symbol", "exchange", "cex_symbol")
                if market_type == "cex"
                else ("token_symbol", "chain", "dex", "pool_address")
            )
            missing_identity = [
                field for field in identity_fields if not (row.get(field) or "").strip()
            ]
            if missing_identity:
                issues.append(
                    make_issue(
                        category="hard_invalid",
                        status="invalid",
                        reason_code="missing_market_identity",
                        retryable=False,
                        market=market,
                        day_text=raw_day or None,
                        message="The row does not contain a complete market identity.",
                        details={
                            "fields": missing_identity,
                            "row_number": row_number,
                        },
                    )
                )
            if parsed_day is None:
                issues.append(
                    make_issue(
                        category="hard_invalid",
                        status="invalid",
                        reason_code="invalid_date",
                        retryable=False,
                        market=market,
                        day_text=raw_day or None,
                        message="The daily fact date is not a valid ISO calendar day.",
                        details={"row_number": row_number},
                    )
                )
            elif (
                latest_completed_day is not None
                and parsed_day > latest_completed_day
            ):
                issues.append(
                    make_issue(
                        category="hard_invalid",
                        status="invalid",
                        reason_code="incomplete_or_future_date",
                        retryable=False,
                        market=market,
                        day_text=parsed_day.isoformat(),
                        message=(
                            "Daily facts may not use the current incomplete "
                            "UTC day or a future date."
                        ),
                        details={
                            "row_number": row_number,
                            "latest_completed_utc_day": (
                                latest_completed_day.isoformat()
                            ),
                            "observed_date": parsed_day.isoformat(),
                        },
                    )
                )

            market_id = market.get("market_id")
            date_is_publishable = (
                parsed_day is not None
                and (
                    latest_completed_day is None
                    or parsed_day <= latest_completed_day
                )
            )
            if market_id and date_is_publishable:
                assert parsed_day is not None
                normalized_day = parsed_day.isoformat()
                if market_type == "cex":
                    primary_key = (
                        normalized_day,
                        str(market["token_symbol"]),
                        str(market["exchange"]),
                        str(market["instrument"]),
                    )
                else:
                    primary_key = (
                        normalized_day,
                        str(market["token_symbol"]),
                        str(market["chain"]),
                        str(market["pool_address"]),
                    )
                primary_key_rows[primary_key].append(row_number)
                state = series.setdefault(
                    str(market_id),
                    {
                        "market": market,
                        "dates": set(),
                        "valid_dates": set(),
                    },
                )
                state["dates"].add(parsed_day)

            value_issues, values_valid = validate_row_values(
                row,
                market=market,
                day_text=parsed_day.isoformat() if parsed_day else raw_day or None,
                row_number=row_number,
            )
            issues.extend(value_issues)
            if (
                market_id
                and date_is_publishable
                and values_valid
                and not missing_identity
            ):
                assert parsed_day is not None
                series[str(market_id)]["valid_dates"].add(parsed_day)

    for key, row_numbers in sorted(primary_key_rows.items()):
        if len(row_numbers) < 2:
            continue
        market_id = None
        market = None
        for candidate_id, state in series.items():
            candidate = state["market"]
            if market_type == "cex":
                matches = (
                    candidate.get("token_symbol") == key[1]
                    and candidate.get("exchange") == key[2]
                    and candidate.get("instrument") == key[3]
                )
            else:
                matches = (
                    candidate.get("token_symbol") == key[1]
                    and candidate.get("chain") == key[2]
                    and candidate.get("pool_address") == key[3]
                )
            if matches:
                market_id = candidate_id
                market = candidate
                break
        assert market_id is not None and market is not None
        issues.append(
            make_issue(
                category="hard_invalid",
                status="invalid",
                reason_code="duplicate_primary_key",
                retryable=False,
                market=market,
                day_text=key[0],
                message="More than one daily fact row has the same primary key.",
                details={
                    "duplicate_count": len(row_numbers),
                    "row_numbers": row_numbers,
                },
            )
        )

    source = {
        "market_type": market_type,
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "row_count": row_count,
        "market_count": len(series),
    }
    return series, issues, source


def day_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _attempt_matches_market(
    attempt: Mapping[str, Any],
    market: Mapping[str, Any],
) -> bool:
    if (
        attempt.get("market_type") != market.get("market_type")
        or attempt.get("token_symbol") != market.get("token_symbol")
    ):
        return False
    if market.get("market_type") == "cex":
        # Token+exchange is the stable adapter identity. Upbit may resolve the
        # configured USDT symbol to an observed KRW instrument.
        return attempt.get("exchange") == market.get("exchange")
    return (
        attempt.get("chain") == market.get("chain")
        and attempt.get("pool_address") == market.get("pool_address")
    )


def attempt_for_gap(
    attempts: Sequence[Mapping[str, Any]],
    market: Mapping[str, Any],
    missing_day: date,
) -> Optional[Mapping[str, Any]]:
    day_text = missing_day.isoformat()
    matches = []
    for attempt in attempts:
        if not _attempt_matches_market(attempt, market):
            continue
        start = attempt.get("requested_start_date")
        end = attempt.get("requested_end_date")
        if not start or not end or not start <= day_text <= end:
            continue
        if day_text in set(attempt.get("observed_dates") or []):
            continue
        if (
            attempt.get("status") == "succeeded"
            or attempt.get("reason_code") == "observed"
        ):
            continue
        matches.append(attempt)
    if not matches:
        return None
    return sorted(
        matches,
        key=lambda item: (
            str(item.get("finished_at_utc") or ""),
            str(item.get("attempt_id") or ""),
        ),
    )[-1]


def gap_evidence(
    *,
    attempts: Sequence[Mapping[str, Any]],
    market: Mapping[str, Any],
    missing_day: date,
    default_message: str,
) -> Dict[str, Any]:
    attempt = attempt_for_gap(attempts, market, missing_day)
    if attempt is None:
        return {
            "status": "backfill_pending",
            "reason_code": "missing_unexplained",
            "retryable": True,
            "message": default_message,
            "attempt": None,
        }
    reason = str(attempt["reason_code"])
    if reason in {"not_listed", "source_range_unavailable"}:
        status = "needs_review"
        retryable = False
    elif reason == "no_candles":
        status = "source_no_observation"
        retryable = False
    else:
        status = "collection_failed"
        retryable = reason in RETRYABLE_ATTEMPT_REASONS
    return {
        "status": status,
        "reason_code": reason,
        "retryable": retryable,
        "message": (
            "The requested daily fact is absent and the matching collection "
            "attempt reported {}.".format(reason.replace("_", " "))
        ),
        "attempt": {
            "attempt_id": attempt.get("attempt_id"),
            "status": attempt.get("status"),
            "outcome": attempt.get("outcome"),
            "reason_code": reason,
            "http_status": attempt.get("http_status"),
            "error": attempt.get("error"),
            "requested_start_date": attempt.get("requested_start_date"),
            "requested_end_date": attempt.get("requested_end_date"),
            "finished_at_utc": attempt.get("finished_at_utc"),
        },
    }


def gap_issues(
    series: Mapping[str, Mapping[str, Any]],
    *,
    today: date,
    active_lookback_days: int = ACTIVE_LOOKBACK_DAYS,
    active_min_observations: int = ACTIVE_MIN_OBSERVATIONS,
    attempts: Sequence[Mapping[str, Any]] = (),
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    if active_lookback_days < 1:
        raise ValueError("active_lookback_days must be positive")
    if active_min_observations < 1 or active_min_observations > active_lookback_days:
        raise ValueError(
            "active_min_observations must be within active_lookback_days"
        )
    issues: List[Dict[str, Any]] = []
    summaries: Dict[str, Dict[str, Any]] = {}
    target_day = today - timedelta(days=1)

    for market_id, state in sorted(series.items()):
        market = state["market"]
        dates = set(state["dates"])
        valid_dates = set(state["valid_dates"])
        historical_missing: List[date] = []
        if dates:
            first_day = min(dates)
            last_day = max(dates)
            historical_missing = [
                day for day in day_range(first_day, last_day) if day not in dates
            ]
            for missing_day in historical_missing:
                evidence = gap_evidence(
                    attempts=attempts,
                    market=market,
                    missing_day=missing_day,
                    default_message=(
                        "No daily row exists between this market's first "
                        "and last observed dates, and no matching failed "
                        "collection attempt explains the absence."
                    ),
                )
                details = {
                    "first_observed_date": first_day.isoformat(),
                    "last_observed_date": last_day.isoformat(),
                }
                if evidence["attempt"] is not None:
                    details["collection_attempt"] = evidence["attempt"]
                issues.append(
                    make_issue(
                        category="historical_gap",
                        status=evidence["status"],
                        reason_code=evidence["reason_code"],
                        retryable=evidence["retryable"],
                        market=market,
                        day_text=missing_day.isoformat(),
                        message=evidence["message"],
                        details=details,
                    )
                )
        else:
            first_day = None
            last_day = None

        active_start = target_day - timedelta(days=active_lookback_days - 1)
        active_reference_end = last_day
        active_reference_start = (
            active_reference_end - timedelta(days=active_lookback_days - 1)
            if active_reference_end is not None
            else None
        )
        last_active_count = (
            sum(
                active_reference_start <= observed_day <= active_reference_end
                for observed_day in valid_dates
            )
            if active_reference_start is not None
            and active_reference_end is not None
            else 0
        )
        recent_valid_count = sum(
            active_start <= observed_day <= target_day
            for observed_day in valid_dates
        )
        d1_gap = bool(
            last_day is not None
            and last_day < target_day
            and recent_valid_count >= active_min_observations
        )
        stale_market_unknown = bool(
            last_day is not None
            and last_day < target_day
            and not d1_gap
            and last_active_count >= active_min_observations
        )
        trailing_missing: List[date] = []
        if d1_gap:
            assert last_day is not None
            retry_floor = target_day - timedelta(days=MAX_RETRY_WINDOW_DAYS - 1)
            trailing_start = max(last_day + timedelta(days=1), retry_floor)
            trailing_missing = list(day_range(trailing_start, target_day))
            for missing_day in trailing_missing:
                is_latest_completed_day = missing_day == target_day
                evidence = gap_evidence(
                    attempts=attempts,
                    market=market,
                    missing_day=missing_day,
                    default_message=(
                        "A previously active market has no daily row "
                        + (
                            "for the latest completed UTC day, and no matching "
                            "failed collection attempt explains the absence."
                            if is_latest_completed_day
                            else "after its last observed UTC day, and no "
                            "matching failed collection attempt explains the "
                            "absence."
                        )
                    ),
                )
                details = {
                    "active_reference_window_start": (
                        active_reference_start.isoformat()
                        if active_reference_start is not None
                        else None
                    ),
                    "active_reference_window_end": (
                        active_reference_end.isoformat()
                        if active_reference_end is not None
                        else None
                    ),
                    "active_observation_count": recent_valid_count,
                    "active_min_observations": active_min_observations,
                    "last_observed_date": last_day.isoformat(),
                    "explicit_inactive_metadata": False,
                }
                if evidence["attempt"] is not None:
                    details["collection_attempt"] = evidence["attempt"]
                issues.append(
                    make_issue(
                        category="d1_active_gap",
                        status=evidence["status"],
                        reason_code=evidence["reason_code"],
                        retryable=evidence["retryable"],
                        market=market,
                        day_text=missing_day.isoformat(),
                        message=evidence["message"],
                        details=details,
                    )
                )
        elif stale_market_unknown:
            assert last_day is not None
            issues.append(
                make_issue(
                    category="stale_market_unknown",
                    status="needs_review",
                    reason_code="stale_market_lifecycle_unknown",
                    retryable=False,
                    market=market,
                    day_text=target_day.isoformat(),
                    message=(
                        "This previously active market has stopped producing "
                        "daily rows, but no explicit inactive or delisted "
                        "metadata is available."
                    ),
                    details={
                        "last_observed_date": last_day.isoformat(),
                        "missing_since": (
                            last_day + timedelta(days=1)
                        ).isoformat(),
                        "last_active_reference_window_start": (
                            active_reference_start.isoformat()
                            if active_reference_start is not None
                            else None
                        ),
                        "last_active_reference_window_end": (
                            active_reference_end.isoformat()
                            if active_reference_end is not None
                            else None
                        ),
                        "last_active_observation_count": last_active_count,
                        "explicit_inactive_metadata": False,
                    },
                )
            )

        summaries[market_id] = {
            "market": dict(market),
            "first_observed_date": first_day.isoformat() if first_day else None,
            "last_observed_date": last_day.isoformat() if last_day else None,
            "observation_day_count": len(dates),
            "valid_observation_day_count": len(valid_dates),
            "historical_gap_count": len(historical_missing),
            "d1_active_gap": d1_gap,
            "stale_market_unknown": stale_market_unknown,
            "trailing_active_gap_count": len(trailing_missing),
            "trailing_active_gap_start": (
                trailing_missing[0].isoformat() if trailing_missing else None
            ),
            "d1_active_observation_count": recent_valid_count,
        }
    return issues, summaries


def split_consecutive_days(
    days: Iterable[date],
    *,
    maximum_days: int = MAX_RETRY_WINDOW_DAYS,
) -> List[List[date]]:
    if maximum_days < 1 or maximum_days > MAX_RETRY_WINDOW_DAYS:
        raise ValueError(
            "maximum_days must be between 1 and {}".format(
                MAX_RETRY_WINDOW_DAYS
            )
        )
    ordered = sorted(set(days))
    if not ordered:
        return []
    windows: List[List[date]] = []
    current: List[date] = []
    for observed_day in ordered:
        if (
            not current
            or (
                observed_day == current[-1] + timedelta(days=1)
                and len(current) < maximum_days
            )
        ):
            current.append(observed_day)
            continue
        windows.append(current)
        current = [observed_day]
    if current:
        windows.append(current)
    return windows


def build_retry_windows(
    issues: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    by_token_date: Dict[str, Dict[date, Dict[str, Set[str]]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "market_ids": set(),
                "reason_codes": set(),
                "issue_ids": set(),
            }
        )
    )
    for issue in issues:
        if issue.get("retryable") is not True:
            continue
        parsed_day = parse_iso_day(str(issue.get("date") or ""))
        market = issue.get("market") or {}
        token = str(market.get("token_symbol") or "").upper()
        if parsed_day is None or not token:
            continue
        entry = by_token_date[token][parsed_day]
        if market.get("market_id"):
            entry["market_ids"].add(str(market["market_id"]))
        if issue.get("reason_code"):
            entry["reason_codes"].add(str(issue["reason_code"]))
        if issue.get("issue_id"):
            entry["issue_ids"].add(str(issue["issue_id"]))

    result: Dict[str, List[Dict[str, Any]]] = {}
    for token, date_metadata in sorted(by_token_date.items()):
        token_windows = []
        for window_days in split_consecutive_days(date_metadata):
            metadata = [date_metadata[day] for day in window_days]
            token_windows.append(
                {
                    "token_symbol": token,
                    "start_date": window_days[0].isoformat(),
                    "end_date": window_days[-1].isoformat(),
                    "day_count": len(window_days),
                    "market_ids": sorted(
                        set().union(
                            *(entry["market_ids"] for entry in metadata)
                        )
                    ),
                    "reason_codes": sorted(
                        set().union(
                            *(entry["reason_codes"] for entry in metadata)
                        )
                    ),
                    "issue_ids": sorted(
                        set().union(
                            *(entry["issue_ids"] for entry in metadata)
                        )
                    ),
                }
            )
        result[token] = token_windows
    return result


def manual_review_queue(
    issues: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    queue = []
    for issue in issues:
        if issue.get("category") not in {
            "hard_invalid",
            "stale_market_unknown",
        } and issue.get("status") != "needs_review":
            continue
        market = issue.get("market") or {}
        queue.append(
            {
                "review_id": "review-{}".format(issue["issue_id"]),
                "review_status": "pending",
                "issue_id": issue["issue_id"],
                "token_symbol": market.get("token_symbol"),
                "market_id": market.get("market_id"),
                "date": issue.get("date"),
                "reason_code": issue.get("reason_code"),
                "category": issue.get("category"),
                "source_url_hints": list(issue.get("source_url_hints") or []),
                "required_evidence": [
                    "declared_source_response_or_primary_cross_check",
                    "checked_at_utc",
                    "reviewer_disposition",
                ],
            }
        )
    return queue


def issue_sort_key(issue: Mapping[str, Any]) -> Tuple[Any, ...]:
    market = issue.get("market") or {}
    return (
        CATEGORY_ORDER.get(str(issue.get("category")), 99),
        str(market.get("token_symbol") or ""),
        str(market.get("market_id") or ""),
        str(issue.get("date") or ""),
        str(issue.get("reason_code") or ""),
        str(issue.get("issue_id") or ""),
    )


def build_report(
    cex_csv: Path,
    dex_csv: Path,
    *,
    cex_attempts: Optional[Path] = None,
    dex_attempts: Optional[Path] = None,
    today: Optional[date] = None,
    active_lookback_days: int = ACTIVE_LOOKBACK_DAYS,
    active_min_observations: int = ACTIVE_MIN_OBSERVATIONS,
) -> Dict[str, Any]:
    audit_day = today or utc_today()
    latest_completed_day = audit_day - timedelta(days=1)
    cex_series, cex_issues, cex_source = read_daily_source(
        cex_csv,
        "cex",
        latest_completed_day=latest_completed_day,
    )
    dex_series, dex_issues, dex_source = read_daily_source(
        dex_csv,
        "dex",
        latest_completed_day=latest_completed_day,
    )
    cex_attempt_rows, cex_attempt_source = _attempt_source(
        path=cex_attempts,
        market_type="cex",
        source_csv_sha256=cex_source["sha256"],
    )
    dex_attempt_rows, dex_attempt_source = _attempt_source(
        path=dex_attempts,
        market_type="dex",
        source_csv_sha256=dex_source["sha256"],
    )
    collection_attempts = [*cex_attempt_rows, *dex_attempt_rows]
    all_series = dict(cex_series)
    overlapping_ids = sorted(set(all_series).intersection(dex_series))
    if overlapping_ids:
        raise FactQualityInputError(
            "CEX and DEX market IDs overlap: {}".format(", ".join(overlapping_ids))
        )
    all_series.update(dex_series)
    gaps, market_summaries = gap_issues(
        all_series,
        today=audit_day,
        active_lookback_days=active_lookback_days,
        active_min_observations=active_min_observations,
        attempts=collection_attempts,
    )
    issues = sorted([*cex_issues, *dex_issues, *gaps], key=issue_sort_key)
    category_counts = Counter(str(issue["category"]) for issue in issues)
    status_counts = Counter(str(issue["status"]) for issue in issues)
    reason_counts = Counter(str(issue["reason_code"]) for issue in issues)
    hard_count = category_counts.get("hard_invalid", 0)
    d1_count = category_counts.get("d1_active_gap", 0)
    stale_count = category_counts.get("stale_market_unknown", 0)
    historical_count = category_counts.get("historical_gap", 0)
    retryable_count = sum(issue.get("retryable") is True for issue in issues)
    reviews = manual_review_queue(issues)
    if hard_count or d1_count:
        report_status = "failed"
    elif historical_count or stale_count:
        report_status = "warning"
    else:
        report_status = "ok"

    return {
        "schema": REPORT_SCHEMA,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "audit_date": audit_day.isoformat(),
        "latest_completed_utc_day": latest_completed_day.isoformat(),
        "status": report_status,
        "policy": {
            "historical_gap_range": "first_observed_date_through_last_observed_date",
            "pre_listing_rule": (
                "Dates before a market's first observed row are not classified "
                "as historical gaps."
            ),
            "d1_active_market_rule": {
                "lookback_days": active_lookback_days,
                "minimum_valid_observation_days": active_min_observations,
                "stale_lifecycle_rule": (
                    "A previously active market that ages out of the retry "
                    "window becomes non-retryable needs_review until explicit "
                    "inactive or delisted metadata exists."
                ),
            },
            "missing_row_status": "backfill_pending",
            "missing_row_reason": (
                "A missing successful row is missing_unexplained unless a "
                "lineage-matched structured attempt record proves a more "
                "specific collection outcome."
            ),
            "maximum_retry_window_days": MAX_RETRY_WINDOW_DAYS,
        },
        "sources": [cex_source, dex_source],
        "attempt_sources": [cex_attempt_source, dex_attempt_source],
        "collection_attempt_summary": {
            "attempt_count": len(collection_attempts),
            "status_counts": dict(
                sorted(
                    Counter(
                        item["status"] for item in collection_attempts
                    ).items()
                )
            ),
            "reason_code_counts": dict(
                sorted(
                    Counter(
                        item["reason_code"] for item in collection_attempts
                    ).items()
                )
            ),
        },
        "collection_attempts": collection_attempts,
        "summary": {
            "source_row_count": cex_source["row_count"] + dex_source["row_count"],
            "market_count": len(all_series),
            "issue_count": len(issues),
            "hard_invalid_count": hard_count,
            "historical_gap_count": historical_count,
            "d1_active_gap_count": d1_count,
            "stale_market_unknown_count": stale_count,
            "retryable_issue_count": retryable_count,
            "manual_review_count": len(reviews),
        },
        "category_counts": {
            category: category_counts.get(category, 0)
            for category in (
                "hard_invalid",
                "historical_gap",
                "d1_active_gap",
                "stale_market_unknown",
            )
        },
        "status_counts": dict(sorted(status_counts.items())),
        "reason_code_counts": dict(sorted(reason_counts.items())),
        "issues": issues,
        "retry_windows_by_token": build_retry_windows(issues),
        "manual_review_queue": reviews,
        "markets": [
            market_summaries[market_id]
            for market_id in sorted(market_summaries)
        ],
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit CEX/DEX daily facts and emit a structured JSON report"
    )
    parser.add_argument("--cex-csv", type=Path, default=DEFAULT_CEX_CSV)
    parser.add_argument("--dex-csv", type=Path, default=DEFAULT_DEX_CSV)
    parser.add_argument("--cex-attempts", type=Path)
    parser.add_argument("--dex-attempts", type=Path)
    parser.add_argument(
        "--today",
        type=date.fromisoformat,
        help="UTC audit date override in YYYY-MM-DD form",
    )
    parser.add_argument("--output", type=Path, help="Write JSON to this path")
    parser.add_argument(
        "--fail-on-hard",
        action="store_true",
        help="Exit non-zero when hard-invalid rows require manual review",
    )
    parser.add_argument(
        "--fail-on-d1",
        action="store_true",
        help="Exit non-zero when an active market is missing D-1",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        report = build_report(
            args.cex_csv,
            args.dex_csv,
            cex_attempts=args.cex_attempts,
            dex_attempts=args.dex_attempts,
            today=args.today,
        )
    except (FactQualityInputError, FileNotFoundError, OSError, ValueError) as error:
        print(
            json.dumps(
                {
                    "schema": REPORT_SCHEMA,
                    "status": "input_error",
                    "error": "{}: {}".format(type(error).__name__, error),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(".{}.tmp".format(args.output.name))
        try:
            temporary.write_text(encoded, encoding="utf-8")
            temporary.replace(args.output)
        finally:
            temporary.unlink(missing_ok=True)
    else:
        print(encoded, end="")

    hard_failed = (
        args.fail_on_hard and report["summary"]["hard_invalid_count"] > 0
    )
    d1_failed = (
        args.fail_on_d1 and report["summary"]["d1_active_gap_count"] > 0
    )
    return 1 if hard_failed or d1_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
