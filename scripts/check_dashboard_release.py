#!/usr/bin/env python3
"""Fail closed when a deployed dashboard violates its public fact contract."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import re
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ResponseMetrics:
    path: str
    elapsed_ms: float
    wire_bytes: int
    raw_bytes: int
    compressed: bool


class ReleaseCheckError(RuntimeError):
    """One release contract or request failed."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReleaseCheckError(message)


COLLECTED_NOTIONALS = frozenset({1_000, 5_000, 10_000, 50_000, 100_000})
EXECUTION_DIRECTIONS = frozenset({"sell_token", "buy_token"})
EXECUTION_STATUSES = frozenset({"observed", "partial", "unsupported", "failed"})
EVENT_LIFECYCLES = frozenset(
    {"scheduled", "occurred", "postponed", "cancelled", "superseded"}
)
EVENT_EVIDENCE_STATUSES = frozenset(
    {"primary_confirmed", "cross_checked", "onchain_observed"}
)
EXPECTED_SUMMARY_VERSION = 2
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
    {"code", "severity", "category", "message"}
)
SCREENING_QUALITY_MARKET_FIELDS = frozenset(
    {"screening_quality_status", "screening_quality_flags"}
)
SCREENING_QUALITY_CODE_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z",
    flags=re.ASCII,
)
RAW_URL_PATTERN = re.compile(
    r"\b[a-z][a-z0-9+.-]*://|\bwww\.",
    flags=re.ASCII | re.IGNORECASE,
)
PROTECTED_PATH_PATTERN = re.compile(
    r"(?:^|[\s(\"'])/(?:[a-z0-9._-]+(?:/|\Z))"
    r"|\b[a-z]:\\",
    flags=re.ASCII | re.IGNORECASE,
)
PROTECTED_POSIX_PREFIX_PATTERN = re.compile(
    r"/(?:home|private|users)/",
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


def validate_summary(
    payload: dict[str, Any],
    metrics: ResponseMetrics,
    *,
    raw_max: int,
    gzip_max: int,
) -> tuple[str, str, str, str]:
    metadata = payload.get("metadata") or {}
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
        require(
            type(row.get("spread_comparable_days")) is int
            and row["spread_comparable_days"] >= 0,
            "Summary spread comparable-day count is invalid",
        )
        for market_type in ("cex", "dex"):
            market = row.get(f"primary_{market_type}")
            if market is None:
                continue
            refresh_id = market.get("refresh_market_id")
            require(
                isinstance(refresh_id, str)
                and refresh_id.startswith(f"{market_type}:"),
                "Summary primary market refresh identity is invalid",
            )
            require(
                isinstance(market.get("depth_retryable"), bool)
                and isinstance(market.get("tvl_retryable"), bool),
                "Summary primary market retryability is invalid",
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
    markets = payload.get("markets")
    require(payload.get("token_symbol") == token, "Token catalog returned wrong Token")
    require(isinstance(markets, list) and markets, "Token catalog has no markets")
    require(
        all(row.get("token_symbol") == token for row in markets),
        "Token catalog leaked another Token",
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
    require(
        all(
            row.get("token_symbol") == token
            and isinstance(row.get("facts"), dict)
            and row["facts"]
            for row in markets
        ),
        "Quality returned an empty or wrong-Token fact set",
    )
    report = metadata.get("daily_quality_report")
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
    require(
        report.get("evidence_mode")
        in {"published_daily_audit", "catalog_window_inference"},
        "Quality daily-audit evidence mode is invalid",
    )
    if report.get("status") == "matched":
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
    reason_counts = report.get("reason_code_counts")
    status_counts = report.get("status_counts")
    affected_dates = report.get("affected_dates")
    require(
        type(issue_count) is int
        and issue_count >= 0
        and isinstance(reason_counts, dict)
        and all(
            isinstance(key, str)
            and key
            and isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
            for key, value in reason_counts.items()
        )
        and sum(reason_counts.values()) == issue_count
        and isinstance(status_counts, dict)
        and all(
            isinstance(key, str)
            and key
            and isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
            for key, value in status_counts.items()
        )
        and sum(status_counts.values()) == issue_count,
        "Quality daily-audit reason/status counts are inconsistent",
    )
    require(
        isinstance(affected_dates, list)
        and all(_is_canonical_date(value) for value in affected_dates)
        and affected_dates == sorted(set(affected_dates))
        and type(report.get("affected_date_count")) is int
        and report["affected_date_count"] == len(affected_dates),
        "Quality daily-audit affected dates are inconsistent",
    )
    for market in markets:
        daily = market["facts"].get("daily") or {}
        if daily.get("retryable"):
            require(
                daily.get("action")
                in {
                    "operator_review_retry_queue",
                    "operator_review_retry_and_manual_queues",
                },
                "Public quality retryable daily fact lacks an operator-only action",
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
        PROTECTED_PATH_PATTERN.search(message) is None,
        "Quality screening flag message contains a protected path",
    )
    require(
        PROTECTED_POSIX_PREFIX_PATTERN.search(message) is None
        and "\\" not in message,
        "Quality screening flag message contains a protected path",
    )
    return {
        "code": code,
        "severity": severity,
        "category": category,
        "message": message,
    }


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

    market_ids: set[str] = set()
    status_counts: Counter[str] = Counter()
    alert_counts: Counter[str] = Counter()
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
        screening_fields = {
            key
            for key in market
            if isinstance(key, str) and key.startswith("screening_quality_")
        }
        require(
            screening_fields == SCREENING_QUALITY_MARKET_FIELDS,
            "Quality market has missing or unknown screening quality fields",
        )
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
        status_counts[status] += 1
        for raw_flag in flags:
            flag = _validate_screening_flag(raw_flag)
            alert_counts[flag["severity"]] += 1

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
    return {
        "token_symbol": token,
        "market_count": len(markets),
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


def validate_execution(
    payload: dict[str, Any],
    *,
    token: str,
    market_a: str,
    market_b: str,
) -> None:
    require(payload.get("token_symbol") == token, "Execution returned wrong Token")
    expected_scenarios = {
        (direction, notional)
        for direction in EXECUTION_DIRECTIONS
        for notional in COLLECTED_NOTIONALS
    }
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
    require_events: bool = True,
) -> list[dict[str, Any]]:
    availability = payload.get("availability") or {}
    require(
        availability.get("status") == "available",
        "Event Fact publication is unavailable",
    )
    require(availability.get("reason") is None, "Available Event feed has a reason")
    require(payload.get("schema") == "event_facts_api/v1", "Wrong Event API schema")
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

    query = payload.get("query") or {}
    require(query.get("token") == token, "Event token scope was not honored")
    require(query.get("start") == start, "Event start scope was not honored")
    require(query.get("end") == end, "Event end scope was not honored")
    require(
        query.get("lifecycle") == lifecycle,
        "Event lifecycle scope was not honored",
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
        require(
            event.get("evidence_status") in EVENT_EVIDENCE_STATUSES,
            "Event evidence status is invalid",
        )

        timing = event.get("time") or {}
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
    for counts_field, expected in expected_counts.items():
        require(
            payload.get(counts_field) == expected,
            f"{counts_field} does not match returned Event rows",
        )
    return events


def release_check(args: argparse.Namespace) -> dict[str, Any]:
    metrics: list[ResponseMetrics] = []
    health, health_metrics = fetch_json(
        args.base_url,
        "/health",
        timeout=args.timeout,
    )
    metrics.append(health_metrics)
    require(health.get("status") == "ok", "Health status is not ok")
    require(health.get("data_ready") is True, "Health reports data_ready=false")

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

    full_catalog, full_metrics = fetch_json(
        args.base_url,
        "/api/markets/catalog",
        timeout=args.timeout,
    )
    metrics.append(full_metrics)
    full_markets = full_catalog.get("markets")
    require(isinstance(full_markets, list), "Full audit catalog has no markets array")
    full_catalog_tokens: set[str] = set()
    full_market_ids: set[str] = set()
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
        full_catalog_tokens.add(market_token)
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
    scoped_events_path = "/api/markets/events?" + urlencode(
        {
            "token": event_token,
            "start": event_start,
            "end": event_end,
            "lifecycle": event_lifecycle,
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
    )

    return {
        "status": "ok",
        "base_url": args.base_url,
        "token": token,
        "window": {"start": start, "end": end},
        "data_generation": generation,
        "token_count": len(summary["tokens"]),
        "screening_quality_parity_count": screening_quality_parity_count,
        "screening_quality_market_count": screening_quality_market_count,
        "catalog_market_count": len(full_markets),
        "event_count": len(event_rows),
        "event_covered_token_count": event_coverage["covered_token_count"],
        "event_bundle_id": all_events["bundle_id"],
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
