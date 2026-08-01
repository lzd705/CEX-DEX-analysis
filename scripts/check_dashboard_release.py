#!/usr/bin/env python3
"""Fail closed when a deployed dashboard violates its public fact contract."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import re
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from scripts.cex_instrument_lifecycle import (
        configured_market_ids_sha256,
    )
    from scripts.token_registry import (
        canonical_cex_market_ids,
        cex_market_ids_sha256,
    )
    from scripts.quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
    )
    from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_FILENAMES
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cex_instrument_lifecycle import configured_market_ids_sha256
    from token_registry import canonical_cex_market_ids, cex_market_ids_sha256
    from quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
    )
    from static_asset_contract import PUBLIC_STATIC_ASSET_FILENAMES


@dataclass(frozen=True)
class ResponseMetrics:
    path: str
    elapsed_ms: float
    wire_bytes: int
    raw_bytes: int
    compressed: bool


class ReleaseCheckError(RuntimeError):
    """One release contract or request failed."""


STATIC_ASSET_FILENAMES = PUBLIC_STATIC_ASSET_FILENAMES
MAX_STATIC_ASSET_BYTES = 4 * 1024 * 1024


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReleaseCheckError(message)


def validate_release_health(
    health: dict[str, Any],
    *,
    expected_application_sha: str | None = None,
    expected_asset_sha: str | None = None,
) -> tuple[str, str, str]:
    application_sha = health.get("application_sha")
    asset_sha = health.get("asset_sha")
    asset_version = health.get("asset_version")
    require(
        isinstance(application_sha, str)
        and re.fullmatch(r"[0-9a-f]{40,64}", application_sha) is not None,
        "Health application SHA is missing or invalid",
    )
    require(
        isinstance(asset_sha, str)
        and re.fullmatch(r"[0-9a-f]{64}", asset_sha) is not None,
        "Health asset SHA is missing or invalid",
    )
    if expected_application_sha is not None:
        expected = str(expected_application_sha).strip().lower()
        require(
            re.fullmatch(r"[0-9a-f]{40,64}", expected) is not None,
            "Expected application SHA is invalid",
        )
        require(
            application_sha == expected,
            "Deployed application SHA does not match the expected application SHA",
        )
    if expected_asset_sha is not None:
        expected_asset = str(expected_asset_sha).strip().lower()
        require(
            re.fullmatch(r"[0-9a-f]{64}", expected_asset) is not None,
            "Expected asset SHA is invalid",
        )
        require(
            asset_sha == expected_asset,
            "Deployed asset SHA does not match the expected asset SHA",
        )
    expected_version = f"{application_sha[:12]}-{asset_sha[:12]}"
    require(
        asset_version == expected_version,
        "Health asset version does not match application and asset SHA evidence",
    )
    require(
        health.get("data_status") == "current",
        "Health freshness status is not current",
    )
    freshness_checked_at = validate_source_freshness(
        health.get("freshness"),
        label="Health",
    )
    validate_lifecycle_freshness(
        health.get("cex_instrument_lifecycle"),
        freshness_checked_at=freshness_checked_at,
    )
    return application_sha, asset_sha, asset_version


COLLECTED_NOTIONALS = frozenset({1_000, 5_000, 10_000, 50_000, 100_000})
EXECUTION_DIRECTIONS = frozenset({"sell_token", "buy_token"})
EXECUTION_STATUSES = frozenset({"observed", "partial", "unsupported", "failed"})
EVENT_LIFECYCLES = frozenset(
    {"scheduled", "occurred", "postponed", "cancelled", "superseded"}
)
EVENT_EVIDENCE_STATUSES = frozenset(
    {"primary_confirmed", "cross_checked", "onchain_observed"}
)
EXPECTED_SUMMARY_VERSION = 3
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
    {
        "code",
        "severity",
        "category",
        "message",
        "observed_value",
        "threshold",
    }
)
SCREENING_QUALITY_MARKET_FIELDS = frozenset(
    {
        "screening_quality_status",
        "screening_quality_flags",
        "screening_quality_scope",
        "screening_quality_window",
    }
)
SELECTED_QUALITY_MARKET_FIELDS = frozenset(
    {
        "quality_status",
        "quality_flags",
        "screening_quality_status",
        "screening_quality_flags",
        "screening_quality_scope",
        "screening_quality_window",
    }
)
QUALITY_FACT_NAMES = frozenset({"daily", "tvl", "depth", "execution"})
DAILY_QUALITY_STATUS_PRIORITY = {
    "collection_failed": 0,
    "needs_review": 1,
    "backfill_pending": 2,
    "source_no_observation": 3,
    "unsupported": 4,
}
DAILY_FACT_EVIDENCE_FIELDS = frozenset(
    {
        "daily_evidence_mode",
        "issue_status_counts",
        "issue_outcome_counts",
        "reason_code_counts",
        "affected_date_count",
        "affected_dates",
    }
)
DAILY_MATCHED_NO_ISSUE_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        (
            "not_applicable",
            "selected_window_before_first_market_observation",
        ),
        ("needs_review", "daily_quality_outcome_invalid"),
    }
)
DAILY_FALLBACK_OUTCOMES = frozenset(
    set(DAILY_MATCHED_NO_ISSUE_OUTCOMES)
    | {
        ("backfill_pending", "missing_unexplained"),
        (
            "backfill_pending",
            "missing_daily_observations_inside_observed_market_lifecycle",
        ),
        (
            "backfill_pending",
            "missing_daily_observations_in_selected_window",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_after_latest_observed_market_date",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_in_selected_window",
        ),
    }
)
SELECTED_QUALITY_CATEGORIES = frozenset(
    set(SCREENING_QUALITY_CATEGORIES) | {"source_outcome"}
)
SELECTED_QUALITY_FLAG_FIELDS = frozenset(
    {"code", "severity", "category", "message", "observed_value", "threshold"}
)
SCREENING_QUALITY_CODE_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z",
    flags=re.ASCII,
)
RAW_URL_PATTERN = re.compile(
    r"\b[a-z][a-z0-9+.-]*://|\bwww\.",
    flags=re.ASCII | re.IGNORECASE,
)
ABSOLUTE_POSIX_PATH_PATTERN = re.compile(
    r"(?:^|[^a-z0-9])/"
    r"(?:[a-z0-9._~][a-z0-9._~-]{0,239}/){0,32}"
    r"[a-z0-9._~][a-z0-9._~-]{0,239}",
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
CANONICAL_CEX_TVL_OUTCOME = (
    "not_applicable",
    "cex_markets_do_not_have_pool_tvl",
)
EXACT_CEX_QUOTE_ASSETS = {
    "coinbase": "USD",
    "kraken": "USD",
}


def validate_configured_cex_identity_metadata(
    metadata: Any,
) -> frozenset[str]:
    """Validate the server-published authority for exact Upbit identities."""
    root = (
        metadata.get("configured_cex_market_identities")
        if isinstance(metadata, dict)
        else None
    )
    require(
        isinstance(root, dict)
        and root.get("schema") == "configured_cex_market_identities/v1"
        and set(root) == {"schema", "upbit"},
        "Configured Upbit market identity metadata is missing or invalid",
    )
    upbit = root.get("upbit")
    require(
        isinstance(upbit, dict)
        and set(upbit)
        == {"market_count", "market_ids", "market_ids_sha256"},
        "Configured Upbit market identity metadata is missing or invalid",
    )
    market_ids = upbit.get("market_ids")
    require(
        isinstance(market_ids, list),
        "Configured Upbit market identity inventory is invalid",
    )
    try:
        canonical = canonical_cex_market_ids(
            market_ids,
            exchange="upbit",
        )
        expected_hash = cex_market_ids_sha256(
            canonical,
            exchange="upbit",
        )
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "Configured Upbit market identity inventory is invalid"
        ) from error
    require(
        list(canonical) == market_ids
        and type(upbit.get("market_count")) is int
        and upbit["market_count"] == len(canonical)
        and upbit.get("market_ids_sha256") == expected_hash,
        "Configured Upbit market identity count or hash is invalid",
    )
    return frozenset(canonical)


def validate_exact_cex_market_identity(
    market_id: Any,
    token_symbol: Any,
    *,
    configured_upbit_market_ids: Any,
) -> None:
    """Reject known legacy quote aliases at the public release boundary."""
    if not isinstance(market_id, str) or not market_id.startswith("cex:"):
        return
    match = re.fullmatch(
        r"cex:([a-z0-9_]{2,32}):([A-Z0-9._-]{1,32})/"
        r"([A-Z0-9._-]{1,32})",
        market_id,
        flags=re.ASCII,
    )
    require(
        match is not None
        and isinstance(token_symbol, str)
        and match.group(2) == token_symbol,
        "Full catalog exact CEX identity is invalid",
    )
    exchange = match.group(1)
    if exchange == "upbit":
        try:
            configured_upbit = frozenset(
                canonical_cex_market_ids(
                    configured_upbit_market_ids,
                    exchange="upbit",
                )
            )
        except (TypeError, ValueError) as error:
            raise ReleaseCheckError(
                "Configured Upbit market identity inventory is invalid"
            ) from error
        require(
            market_id in configured_upbit,
            "Full catalog market is not a configured Upbit exact identity",
        )
        return
    expected_quote = EXACT_CEX_QUOTE_ASSETS.get(exchange)
    require(
        expected_quote is None or match.group(3) == expected_quote,
        "Full catalog exact CEX identity uses a legacy quote alias",
    )


def _release_quality_fact_rule(
    market_type: Any,
    fact_name: Any,
    status: Any,
    reason_code: Any,
) -> Any:
    """Apply release-only family constraints on top of producer rules."""
    family = str(market_type or "").strip().lower()
    fact = str(fact_name or "").strip().lower()
    pair = (
        str(status or "").strip().lower(),
        str(reason_code or "").strip().lower(),
    )
    if (
        family == "cex"
        and fact == "tvl"
        and pair != CANONICAL_CEX_TVL_OUTCOME
    ):
        return None
    return canonical_quality_fact_rule(
        family,
        fact,
        pair[0],
        pair[1],
    )


def _release_quality_fact_action(
    market_type: Any,
    fact_name: Any,
    status: Any,
    reason_code: Any,
    retryable: Any,
    **kwargs: Any,
) -> str | None:
    """Derive one action without broadening an outcome to another fact."""
    rule = _release_quality_fact_rule(
        market_type,
        fact_name,
        status,
        reason_code,
    )
    if (
        rule is None
        or type(retryable) is not bool
        or retryable is not rule.retryable
    ):
        raise ValueError("quality fact outcome is not canonical")
    return canonical_quality_fact_action(
        market_type,
        fact_name,
        status,
        reason_code,
        retryable,
        **kwargs,
    )


def _validate_endpoint_generation(
    metadata: dict[str, Any],
    *,
    field: str,
    expected: str | None,
    label: str,
) -> None:
    """Enable endpoint-specific generation checks when producers publish one."""
    if expected is None:
        return
    require(
        isinstance(expected, str)
        and bool(expected)
        and expected == expected.strip(),
        f"Expected {label} generation is invalid",
    )
    require(
        metadata.get(field) == expected,
        f"{label} generation differs from the expected generation",
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


def fetch_static_asset_bundle(
    base_url: str,
    asset_version: str,
    *,
    timeout: float,
) -> tuple[str, list[ResponseMetrics]]:
    """Fetch the versioned first-party assets and recompute their exact hash."""
    require(
        isinstance(asset_version, str)
        and re.fullmatch(r"[0-9a-f]{12}-[0-9a-f]{12}", asset_version)
        is not None,
        "Static asset version is invalid",
    )
    digest = hashlib.sha256()
    metrics = []
    for filename in STATIC_ASSET_FILENAMES:
        path = "/{}?v={}".format(filename, asset_version)
        request = Request(
            "{}{}".format(base_url.rstrip("/"), path),
            headers={"Accept-Encoding": "gzip"},
        )
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_STATIC_ASSET_BYTES + 1)
                encoding = response.headers.get(
                    "Content-Encoding", ""
                ).lower()
                status = response.status
        except HTTPError as error:
            raise ReleaseCheckError(
                "{} returned HTTP {}".format(path, error.code)
            ) from error
        except URLError as error:
            raise ReleaseCheckError(
                "{} request failed: {}".format(path, error.reason)
            ) from error
        elapsed_ms = (time.perf_counter() - started) * 1000
        require(status == 200, "{} returned HTTP {}".format(path, status))
        compressed = encoding == "gzip"
        try:
            raw = gzip.decompress(body) if compressed else body
        except gzip.BadGzipFile as error:
            raise ReleaseCheckError(
                "{} declared invalid gzip".format(path)
            ) from error
        require(
            len(raw) <= MAX_STATIC_ASSET_BYTES,
            "{} exceeds the static asset size limit".format(path),
        )
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update(raw)
        digest.update(b"\0")
        metrics.append(
            ResponseMetrics(
                path=path,
                elapsed_ms=elapsed_ms,
                wire_bytes=len(body),
                raw_bytes=len(raw),
                compressed=compressed,
            )
        )
    return digest.hexdigest(), metrics


def validate_summary(
    payload: dict[str, Any],
    metrics: ResponseMetrics,
    *,
    raw_max: int,
    gzip_max: int,
) -> tuple[str, str, str, str]:
    metadata = payload.get("metadata") or {}
    validate_configured_cex_identity_metadata(metadata)
    freshness_checked_at = validate_source_freshness(
        metadata.get("freshness"),
        label="Summary",
    )
    validate_lifecycle_freshness(
        metadata.get("cex_instrument_lifecycle"),
        freshness_checked_at=freshness_checked_at,
    )
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
        comparable_days = row.get("spread_comparable_days")
        spread_values = {
            field: row.get(field)
            for field in (
                "absolute_price_gap",
                "maximum_absolute_price_spread",
                "mean_absolute_price_spread",
                "median_absolute_price_spread",
            )
        }
        require(
            type(comparable_days) is int
            and comparable_days >= 0
            and row.get("price_spread_method")
            == "directional_dex_over_cex_minus_one"
            and row.get("absolute_price_gap_method")
            == "symmetric_midpoint_relative_gap"
            and all(
                value is None
                or (
                    type(value) in {int, float}
                    and math.isfinite(value)
                    and value >= 0
                )
                for value in spread_values.values()
            )
            and (
                all(value is None for value in spread_values.values())
                if comparable_days == 0
                else all(value is not None for value in spread_values.values())
            )
            and (
                row.get("price_spread") is None
                if comparable_days == 0
                else type(row.get("price_spread")) in {int, float}
                and math.isfinite(row["price_spread"])
            ),
            "Summary spread contract is invalid",
        )
        if comparable_days:
            maximum_gap = spread_values["maximum_absolute_price_spread"]
            require(
                maximum_gap >= spread_values["absolute_price_gap"]
                and maximum_gap >= spread_values["mean_absolute_price_spread"]
                and maximum_gap >= spread_values["median_absolute_price_spread"],
                "Summary spread contract is internally inconsistent",
            )
        for market_type in ("cex", "dex"):
            market = row.get(f"primary_{market_type}")
            if market is None:
                continue
            require(
                isinstance(market, dict),
                "Summary primary market is not an object",
            )
            refresh_id = market.get("refresh_market_id")
            market_token = market.get("token_symbol")
            venue = market.get("venue")
            instrument = market.get("instrument")
            pool_address = market.get("pool_address")
            expected_refresh_id = None
            if market_type == "cex" and (
                isinstance(venue, str)
                and venue
                and isinstance(instrument, str)
                and "/" in instrument
                and instrument.split("/", 1)[0] == token_symbol
                and market_token == token_symbol
            ):
                expected_refresh_id = "cex:{}:{}".format(
                    venue,
                    instrument,
                )
            elif market_type == "dex" and (
                isinstance(venue, str)
                and venue.count(" / ") == 1
                and isinstance(pool_address, str)
                and bool(pool_address)
                and market_token == token_symbol
            ):
                chain, dex = venue.split(" / ", 1)
                if chain and dex:
                    expected_refresh_id = "dex:{}:{}:{}:{}".format(
                        chain,
                        dex,
                        pool_address,
                        token_symbol,
                    )
            require(
                isinstance(refresh_id, str)
                and refresh_id == expected_refresh_id,
                "Summary primary market refresh identity is invalid",
            )
            require(
                isinstance(market.get("depth_retryable"), bool)
                and isinstance(market.get("tvl_retryable"), bool),
                "Summary primary market retryability is invalid",
            )
            for fact_name in ("tvl", "depth"):
                status = market.get(f"{fact_name}_status")
                reason_field = f"{fact_name}_na_reason"
                reason = market.get(reason_field)
                retryable = market.get(f"{fact_name}_retryable")
                rule = _release_quality_fact_rule(
                    market_type,
                    fact_name,
                    status,
                    reason,
                )
                require(
                    reason_field in market
                    and isinstance(status, str)
                    and rule is not None
                    and retryable is rule.retryable,
                    "Summary primary market N/A outcome is not canonical",
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
    configured_upbit_market_ids = (
        validate_configured_cex_identity_metadata(metadata)
    )
    markets = payload.get("markets")
    require(payload.get("token_symbol") == token, "Token catalog returned wrong Token")
    require(isinstance(markets, list) and markets, "Token catalog has no markets")
    require(
        all(row.get("token_symbol") == token for row in markets),
        "Token catalog leaked another Token",
    )
    market_ids = [
        row.get("market_id") if isinstance(row, dict) else None
        for row in markets
    ]
    require(
        all(
            isinstance(market_id, str)
            and bool(market_id)
            and market_id == market_id.strip()
            for market_id in market_ids
        )
        and len(set(market_ids)) == len(market_ids),
        "Token catalog market IDs are invalid or duplicated",
    )
    for row, market_id in zip(markets, market_ids):
        validate_exact_cex_market_identity(
            market_id,
            row.get("token_symbol") if isinstance(row, dict) else None,
            configured_upbit_market_ids=configured_upbit_market_ids,
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
    expected_generation: str,
    expected_comparison_generation: str | None = None,
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
        metadata.get("data_generation") == expected_generation,
        "Summary and Compare generations differ",
    )
    _validate_endpoint_generation(
        metadata,
        field="comparison_generation",
        expected=expected_comparison_generation,
        label="Comparison",
    )
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


def _normalized_daily_count_map(
    value: Any,
    *,
    label: str,
    allowed_keys: frozenset[str] | None = None,
    require_positive_entries: bool = False,
) -> dict[str, int]:
    require(isinstance(value, dict), label)
    normalized: dict[str, int] = {}
    for key, count in value.items():
        require(
            isinstance(key, str)
            and bool(key)
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(key) is not None
            and (allowed_keys is None or key in allowed_keys)
            and type(count) is int
            and count >= 0
            and (not require_positive_entries or count > 0),
            label,
        )
        if count:
            normalized[key] = count
    return dict(sorted(normalized.items()))


def _normalized_daily_outcome_counts(
    value: Any,
    *,
    label: str,
    market_type: str | None = None,
) -> dict[tuple[str, str], int]:
    require(isinstance(value, list), label)
    normalized: dict[tuple[str, str], int] = {}
    for item in value:
        require(
            isinstance(item, dict)
            and set(item) == {"status", "reason_code", "count"}
            and isinstance(item.get("status"), str)
            and item["status"] in DAILY_QUALITY_STATUS_PRIORITY
            and isinstance(item.get("reason_code"), str)
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(
                item["reason_code"]
            )
            is not None
            and type(item.get("count")) is int
            and item["count"] > 0,
            label,
        )
        pair = (item["status"], item["reason_code"])
        require(pair not in normalized, label)
        families = (market_type,) if market_type else ("cex", "dex")
        require(
            any(
                _release_quality_fact_rule(
                    family,
                    "daily",
                    pair[0],
                    pair[1],
                )
                is not None
                for family in families
            ),
            label,
        )
        normalized[pair] = item["count"]
    return dict(sorted(normalized.items()))


def _validate_daily_quality_report(
    report: Any,
    *,
    expected_market_ids: set[str],
) -> dict[str, Any]:
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
    status = report["status"]
    expected_evidence_mode = (
        "published_daily_audit"
        if status == "matched"
        else "catalog_window_inference"
    )
    require(
        report.get("evidence_mode") == expected_evidence_mode,
        "Quality daily-audit evidence mode is invalid",
    )
    if status == "matched":
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
    require(
        type(issue_count) is int and issue_count >= 0,
        "Quality daily-audit reason/status counts are inconsistent",
    )
    reason_counts = _normalized_daily_count_map(
        report.get("reason_code_counts"),
        label="Quality daily-audit reason/status counts are inconsistent",
    )
    status_counts = _normalized_daily_count_map(
        report.get("status_counts"),
        label="Quality daily-audit reason/status counts are inconsistent",
        allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
    )
    outcome_counts = (
        _normalized_daily_outcome_counts(
            report.get("issue_outcome_counts"),
            label="Quality daily-audit outcome counts are invalid",
        )
        if status == "matched"
        else {}
    )
    outcome_status_counts: Counter[str] = Counter()
    outcome_reason_counts: Counter[str] = Counter()
    for (outcome_status, outcome_reason), count in outcome_counts.items():
        outcome_status_counts[outcome_status] += count
        outcome_reason_counts[outcome_reason] += count
    require(
        sum(reason_counts.values()) == issue_count
        and sum(status_counts.values()) == issue_count,
        "Quality daily-audit reason/status counts are inconsistent",
    )
    if status == "matched":
        require(
            dict(sorted(outcome_status_counts.items())) == status_counts
            and dict(sorted(outcome_reason_counts.items()))
            == reason_counts,
            "Quality daily-audit outcome counts do not match its marginals",
        )
    affected_dates = report.get("affected_dates")
    require(
        isinstance(affected_dates, list)
        and all(_is_canonical_date(value) for value in affected_dates)
        and affected_dates == sorted(set(affected_dates))
        and type(report.get("affected_date_count")) is int
        and report["affected_date_count"] == len(affected_dates)
        and len(affected_dates) <= issue_count,
        "Quality daily-audit affected dates are inconsistent",
    )
    if status != "matched":
        require(
            issue_count == 0
            and not reason_counts
            and not status_counts
            and not affected_dates,
            "Quality fallback cannot claim published daily-audit issues",
        )
        require(
            "market_issue_rollups" not in report,
            "Quality fallback cannot claim market-bound daily evidence",
        )
        require(
            "issue_outcome_counts" not in report,
            "Quality fallback cannot claim published outcome counts",
        )
        market_rollups: dict[str, dict[str, Any]] = {}
    else:
        raw_rollups = report.get("market_issue_rollups")
        require(
            isinstance(raw_rollups, list)
            and len(raw_rollups) == len(expected_market_ids),
            "Quality daily-audit market rollups are incomplete",
        )
        market_rollups = {}
        rollup_status_counts: Counter[str] = Counter()
        rollup_reason_counts: Counter[str] = Counter()
        rollup_affected_dates: set[str] = set()
        rollup_outcome_counts: Counter[tuple[str, str]] = Counter()
        rollup_issue_count = 0
        for rollup in raw_rollups:
            require(
                isinstance(rollup, dict)
                and isinstance(rollup.get("market_id"), str)
                and rollup["market_id"] in expected_market_ids
                and rollup["market_id"] not in market_rollups,
                "Quality daily-audit market rollups are invalid",
            )
            rollup_count = rollup.get("issue_count")
            rollup_reasons = _normalized_daily_count_map(
                rollup.get("reason_code_counts"),
                label="Quality daily-audit market rollups are invalid",
            )
            rollup_statuses = _normalized_daily_count_map(
                rollup.get("status_counts"),
                label="Quality daily-audit market rollups are invalid",
                allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
            )
            rollup_outcomes = _normalized_daily_outcome_counts(
                rollup.get("issue_outcome_counts"),
                label="Quality daily-audit market rollup outcome counts are invalid",
            )
            rollup_outcome_statuses: Counter[str] = Counter()
            rollup_outcome_reasons: Counter[str] = Counter()
            for (outcome_status, outcome_reason), count in (
                rollup_outcomes.items()
            ):
                rollup_outcome_statuses[outcome_status] += count
                rollup_outcome_reasons[outcome_reason] += count
            rollup_dates = rollup.get("affected_dates")
            fact_outcome = rollup.get("fact_outcome")
            require(
                type(rollup_count) is int
                and rollup_count >= 0
                and sum(rollup_reasons.values()) == rollup_count
                and sum(rollup_statuses.values()) == rollup_count
                and dict(sorted(rollup_outcome_statuses.items()))
                == rollup_statuses
                and dict(sorted(rollup_outcome_reasons.items()))
                == rollup_reasons
                and isinstance(rollup_dates, list)
                and all(_is_canonical_date(value) for value in rollup_dates)
                and rollup_dates == sorted(set(rollup_dates))
                and type(rollup.get("affected_date_count")) is int
                and rollup["affected_date_count"] == len(rollup_dates)
                and len(rollup_dates) <= rollup_count,
                "Quality daily-audit market rollups are invalid",
            )
            require(
                rollup.get("evidence_mode")
                in {
                    "published_daily_audit",
                    "catalog_report_reconciliation",
                }
                and isinstance(fact_outcome, dict)
                and set(fact_outcome)
                == {"status", "reason_code", "retryable", "action"}
                and isinstance(fact_outcome.get("status"), str)
                and isinstance(fact_outcome.get("reason_code"), str)
                and type(fact_outcome.get("retryable")) is bool
                and (
                    fact_outcome.get("action") is None
                    or isinstance(fact_outcome.get("action"), str)
                ),
                "Quality daily-audit market rollup fact outcome is invalid",
            )
            normalized_rollup = {
                "mode": rollup["evidence_mode"],
                "issue_count": rollup_count,
                "outcome_counts": rollup_outcomes,
                "reason_counts": rollup_reasons,
                "status_counts": rollup_statuses,
                "affected_dates": rollup_dates,
                "fact_outcome": fact_outcome,
            }
            market_rollups[rollup["market_id"]] = normalized_rollup
            rollup_issue_count += rollup_count
            rollup_reason_counts.update(rollup_reasons)
            rollup_status_counts.update(rollup_statuses)
            rollup_affected_dates.update(rollup_dates)
            rollup_outcome_counts.update(rollup_outcomes)
        require(
            set(market_rollups) == expected_market_ids
            and rollup_issue_count == issue_count
            and dict(sorted(rollup_reason_counts.items())) == reason_counts
            and dict(sorted(rollup_status_counts.items())) == status_counts
            and sorted(rollup_affected_dates) == affected_dates,
            "Quality daily-audit market rollups do not match the report",
        )
        require(
            dict(sorted(rollup_outcome_counts.items())) == outcome_counts,
            "Quality daily-audit market rollups do not match the report",
        )
    return {
        "status": status,
        "issue_count": issue_count,
        "reason_counts": reason_counts,
        "status_counts": status_counts,
        "outcome_counts": outcome_counts,
        "affected_dates": affected_dates,
        "market_rollups": market_rollups,
    }


def _validate_daily_fact_evidence(
    fact: dict[str, Any],
    *,
    market_type: str,
    report_status: str,
) -> dict[str, Any]:
    status = fact["status"]
    reason_code = fact["reason_code"]
    retryable = fact["retryable"]
    mode = fact.get("daily_evidence_mode")
    present_evidence_fields = {
        field for field in DAILY_FACT_EVIDENCE_FIELDS if field in fact
    }

    if report_status == "matched":
        require(
            present_evidence_fields == DAILY_FACT_EVIDENCE_FIELDS,
            "Quality daily fact evidence/action is incomplete",
        )
    else:
        require(
            mode is None and not present_evidence_fields,
            "Quality daily fact evidence/action mode is invalid",
        )

    if mode == "published_daily_audit":
        require(
            report_status == "matched",
            "Quality daily fact evidence/action is incomplete",
        )
        status_counts = _normalized_daily_count_map(
            fact.get("issue_status_counts"),
            label="Quality daily fact evidence/action counts are invalid",
            allowed_keys=frozenset(DAILY_QUALITY_STATUS_PRIORITY),
            require_positive_entries=True,
        )
        reason_counts = _normalized_daily_count_map(
            fact.get("reason_code_counts"),
            label="Quality daily fact evidence/action counts are invalid",
            require_positive_entries=True,
        )
        outcome_counts = _normalized_daily_outcome_counts(
            fact.get("issue_outcome_counts"),
            label="Quality daily fact evidence/action outcome counts are invalid",
            market_type=market_type,
        )
        outcome_status_counts: Counter[str] = Counter()
        outcome_reason_counts: Counter[str] = Counter()
        for (outcome_status, outcome_reason), count in (
            outcome_counts.items()
        ):
            outcome_status_counts[outcome_status] += count
            outcome_reason_counts[outcome_reason] += count
        issue_count = sum(status_counts.values())
        require(
            sum(reason_counts.values()) == issue_count
            and dict(sorted(outcome_status_counts.items())) == status_counts
            and dict(sorted(outcome_reason_counts.items())) == reason_counts,
            "Quality daily fact evidence/action counts are inconsistent",
        )
        affected_dates = fact.get("affected_dates")
        require(
            isinstance(affected_dates, list)
            and all(_is_canonical_date(value) for value in affected_dates)
            and affected_dates == sorted(set(affected_dates))
            and type(fact.get("affected_date_count")) is int
            and fact["affected_date_count"] == len(affected_dates)
            and len(affected_dates) <= issue_count,
            "Quality daily fact evidence/action dates are inconsistent",
        )
        if issue_count == 0:
            require(
                not status_counts
                and not reason_counts
                and not affected_dates
                and (status, reason_code) in DAILY_MATCHED_NO_ISSUE_OUTCOMES,
                "Quality daily fact zero evidence/action is invalid",
            )
            try:
                expected_action = _release_quality_fact_action(
                    market_type,
                    "daily",
                    status,
                    reason_code,
                    retryable,
                    manual_review_present=False,
                )
            except ValueError as error:
                raise ReleaseCheckError(
                    "Quality daily fact zero evidence/action is invalid"
                ) from error
            require(
                fact.get("action") == expected_action,
                "Quality daily fact zero evidence/action is invalid",
            )
            return {
                "mode": mode,
                "issue_count": 0,
                "status_counts": {},
                "outcome_counts": {},
                "reason_counts": {},
                "affected_dates": [],
                "fact_outcome": {
                    "status": status,
                    "reason_code": reason_code,
                    "retryable": retryable,
                    "action": fact.get("action"),
                },
            }
        try:
            expected_status = aggregate_daily_quality_status(status_counts)
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality daily fact status aggregation is invalid"
            ) from error
        expected_reason = (
            next(iter(reason_counts))
            if len(reason_counts) == 1
            else "multiple_daily_quality_reasons"
        )
        expected_retryable = any(
            issue_status in {"collection_failed", "backfill_pending"}
            for issue_status in status_counts
        )
        manual_review_present = bool(status_counts.get("needs_review"))
        try:
            expected_action = _release_quality_fact_action(
                market_type,
                "daily",
                expected_status,
                expected_reason,
                expected_retryable,
                manual_review_present=manual_review_present,
            )
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality daily fact evidence/action outcome is invalid"
            ) from error
        require(
            status == expected_status
            and reason_code == expected_reason
            and retryable is expected_retryable
            and fact.get("action") == expected_action,
            "Quality daily fact evidence/action does not match its issues",
        )
        return {
            "mode": mode,
            "issue_count": issue_count,
            "status_counts": status_counts,
            "outcome_counts": outcome_counts,
            "reason_counts": reason_counts,
            "affected_dates": affected_dates,
            "fact_outcome": {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": fact.get("action"),
            },
        }

    if mode == "catalog_report_reconciliation":
        require(
            report_status == "matched"
            and present_evidence_fields == DAILY_FACT_EVIDENCE_FIELDS
            and fact.get("issue_status_counts") == {}
            and fact.get("issue_outcome_counts") == []
            and fact.get("reason_code_counts")
            == {"daily_audit_no_matching_issue": 1}
            and fact.get("affected_date_count") == 0
            and fact.get("affected_dates") == []
            and status == "needs_review"
            and reason_code == "daily_audit_no_matching_issue"
            and retryable is False
            and fact.get("action") == "operator_manual_review",
            "Quality daily fact reconciliation evidence/action is invalid",
        )
        return {
            "mode": mode,
            "issue_count": 0,
            "status_counts": {},
            "outcome_counts": {},
            "reason_counts": {},
            "affected_dates": [],
            "fact_outcome": {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": fact.get("action"),
            },
        }

    require(report_status != "matched", "Quality daily fact evidence/action is incomplete")
    pair = (status, reason_code)
    allowed = (
        DAILY_MATCHED_NO_ISSUE_OUTCOMES
        if report_status == "matched"
        else DAILY_FALLBACK_OUTCOMES
    )
    require(
        pair in allowed,
        "Quality daily fact lacks required published evidence/action",
    )
    try:
        expected_action = _release_quality_fact_action(
            market_type,
            "daily",
            status,
            reason_code,
            retryable,
            manual_review_present=False,
        )
    except ValueError as error:
        raise ReleaseCheckError(
            "Quality daily fact action outcome is invalid"
        ) from error
    require(
        fact.get("action") == expected_action,
        "Quality daily fact action is not canonical",
    )
    return {
        "mode": None,
        "issue_count": 0,
        "status_counts": {},
        "outcome_counts": {},
        "reason_counts": {},
        "affected_dates": [],
        "fact_outcome": {
            "status": status,
            "reason_code": reason_code,
            "retryable": retryable,
            "action": fact.get("action"),
        },
    }


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
    daily_report = _validate_daily_quality_report(
        metadata.get("daily_quality_report"),
        expected_market_ids=expected_ids,
    )
    daily_evidence_rows: list[dict[str, Any]] = []
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
    for row in markets:
        require(
            isinstance(row, dict) and row.get("token_symbol") == token,
            "Quality returned an empty or wrong-Token fact set",
        )
        quality_fields = {
            field for field in SELECTED_QUALITY_MARKET_FIELDS if field in row
        }
        require(
            quality_fields == SELECTED_QUALITY_MARKET_FIELDS,
            "Quality selected quality contract has missing projection fields",
        )
        _validate_screening_context(row)
        selected_status = row["quality_status"]
        selected_flags = row["quality_flags"]
        screening_status = row["screening_quality_status"]
        screening_flags = row["screening_quality_flags"]
        require(
            selected_status in SCREENING_QUALITY_STATUSES
            and isinstance(selected_flags, list)
            and (selected_status == "ok" or bool(selected_flags)),
            "Quality selected quality contract has an invalid selected projection",
        )
        require(
            screening_status in SCREENING_QUALITY_STATUSES
            and isinstance(screening_flags, list)
            and (screening_status == "ok" or bool(screening_flags)),
            "Quality selected quality contract has an invalid screening projection",
        )
        normalized_selected_flags = [
            _validate_selected_quality_flag(flag)
            for flag in selected_flags
        ]
        normalized_screening_flags = [
            _validate_screening_flag(flag)
            for flag in screening_flags
        ]
        require(
            selected_status == _quality_status_from_flags(
                normalized_selected_flags
            )
            and screening_status == _quality_status_from_flags(
                normalized_screening_flags
            ),
            "Quality status does not match its data-health flags",
        )

        facts = row.get("facts")
        market_type = row.get("market_type")
        require(
            market_type in {"cex", "dex"},
            "Quality selected market type is invalid",
        )
        require(
            isinstance(row.get("market_id"), str)
            and row["market_id"].startswith("{}:".format(market_type)),
            "Quality market identity/type is inconsistent",
        )
        require(
            isinstance(facts, dict) and set(facts) == QUALITY_FACT_NAMES,
            "Quality selected quality contract has missing or unknown fact families",
        )
        fact_flags_by_code: dict[str, dict[str, Any]] = {}
        for fact_name in QUALITY_FACT_NAMES:
            fact = facts[fact_name]
            status = fact.get("status") if isinstance(fact, dict) else None
            reason_code = fact.get("reason_code") if isinstance(fact, dict) else None
            action = fact.get("action") if isinstance(fact, dict) else None
            fact_flags = fact.get("quality_flags") if isinstance(fact, dict) else None
            require(
                isinstance(fact, dict)
                and isinstance(status, str)
                and 0 < len(status) <= 64
                and SCREENING_QUALITY_CODE_PATTERN.fullmatch(status) is not None
                and "reason_code" in fact
                and (
                    reason_code is None
                    or (
                        isinstance(reason_code, str)
                        and 0 < len(reason_code) <= 64
                        and SCREENING_QUALITY_CODE_PATTERN.fullmatch(reason_code)
                        is not None
                    )
                )
                and type(fact.get("retryable")) is bool
                and "action" in fact
                and (action is None or isinstance(action, str))
                and isinstance(fact_flags, list),
                "Quality selected quality contract has an invalid fact projection",
            )
            for flag in fact_flags:
                normalized_flag = _validate_selected_quality_flag(flag)
                prior = fact_flags_by_code.get(normalized_flag["code"])
                require(
                    prior is None or prior == normalized_flag,
                    "Quality facts contain conflicting flag projections",
                )
                fact_flags_by_code[normalized_flag["code"]] = normalized_flag
            rule = _release_quality_fact_rule(
                market_type,
                fact_name,
                status,
                reason_code,
            )
            require(
                rule is not None
                and fact["retryable"] is rule.retryable,
                "Quality fact does not use a canonical outcome/action tuple",
            )
            if fact_name == "daily":
                daily_evidence = _validate_daily_fact_evidence(
                    fact,
                    market_type=market_type,
                    report_status=daily_report["status"],
                )
                if daily_report["status"] == "matched":
                    require(
                        {
                            key: daily_evidence[key]
                            for key in (
                                "mode",
                                "issue_count",
                                "outcome_counts",
                                "status_counts",
                                "reason_counts",
                                "affected_dates",
                                "fact_outcome",
                            )
                        }
                        == daily_report["market_rollups"][row["market_id"]],
                        "Quality daily fact evidence/action does not match its market rollup",
                    )
                daily_evidence_rows.append(daily_evidence)
                expected_action = fact.get("action")
            else:
                expected_action = _release_quality_fact_action(
                    market_type,
                    fact_name,
                    status,
                    reason_code,
                    fact["retryable"],
                )
            require(
                action == expected_action,
                "Quality fact does not use a canonical outcome/action tuple",
            )
        selected_flags_by_code: dict[str, dict[str, Any]] = {}
        for normalized_flag in normalized_selected_flags:
            code = normalized_flag["code"]
            require(
                code not in selected_flags_by_code,
                "Quality selected flags contain duplicate codes",
            )
            selected_flags_by_code[code] = normalized_flag
        require(
            selected_flags_by_code == fact_flags_by_code,
            "Quality selected flags differ from the fact flag projection",
        )
    published_status_counts: Counter[str] = Counter()
    published_reason_counts: Counter[str] = Counter()
    published_outcome_counts: Counter[tuple[str, str]] = Counter()
    published_affected_dates: set[str] = set()
    published_issue_count = 0
    for evidence in daily_evidence_rows:
        if evidence["mode"] != "published_daily_audit":
            continue
        published_issue_count += evidence["issue_count"]
        published_status_counts.update(evidence["status_counts"])
        published_reason_counts.update(evidence["reason_counts"])
        published_outcome_counts.update(evidence["outcome_counts"])
        published_affected_dates.update(evidence["affected_dates"])
    require(
        published_issue_count == daily_report["issue_count"]
        and dict(sorted(published_status_counts.items()))
        == daily_report["status_counts"]
        and dict(sorted(published_reason_counts.items()))
        == daily_report["reason_counts"]
        and dict(sorted(published_outcome_counts.items()))
        == daily_report["outcome_counts"]
        and sorted(published_affected_dates)
        == daily_report["affected_dates"],
        "Quality daily fact evidence/action does not reconcile to the report",
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


def _quality_status_from_flags(flags: list[dict[str, Any]]) -> str:
    data_health = [
        flag for flag in flags if flag.get("category") == "data_health"
    ]
    if any(flag.get("severity") == "critical" for flag in data_health):
        return "critical"
    if any(flag.get("severity") == "warning" for flag in data_health):
        return "warning"
    if data_health:
        return "info"
    return "ok"


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
        ABSOLUTE_POSIX_PATH_PATTERN.search(message) is None
        and "\\" not in message,
        "Quality screening flag message contains a protected path",
    )
    for field in ("observed_value", "threshold"):
        try:
            encoded = json.dumps(
                flag[field],
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as error:
            raise ReleaseCheckError(
                "Quality screening measurement is not bounded JSON"
            ) from error
        require(
            len(encoded) <= 1024
            and RAW_URL_PATTERN.search(encoded) is None
            and ABSOLUTE_POSIX_PATH_PATTERN.search(encoded) is None
            and "\\" not in encoded,
            "Quality screening measurement exposes unbounded or protected data",
        )
    return dict(flag)


def _validate_screening_context(market: dict[str, Any]) -> None:
    require(
        market.get("screening_quality_scope") == "catalog",
        "Quality screening scope is invalid",
    )
    window = market.get("screening_quality_window")
    require(
        isinstance(window, dict)
        and set(window) == {"start", "end", "method"},
        "Quality screening evaluation window is invalid",
    )
    start = window["start"]
    end = window["end"]
    method = window["method"]
    require(
        (start is None or _is_canonical_date(start))
        and (end is None or _is_canonical_date(end))
        and isinstance(method, (str, type(None)))
        and (method is None or (method == method.strip() and len(method) <= 96)),
        "Quality screening evaluation window is not bounded",
    )
    require(
        start is None or end is None or start <= end,
        "Quality screening evaluation window is reversed",
    )


def _validate_selected_quality_flag(flag: Any) -> dict[str, Any]:
    """Validate the richer selected-window flag without trusting public text."""
    require(isinstance(flag, dict), "Quality selected quality contract flag is invalid")
    require(
        set(flag) == SELECTED_QUALITY_FLAG_FIELDS,
        "Quality selected quality contract flag has missing or unknown fields",
    )
    # The producer always emits both measurement keys. An explicit JSON null
    # is canonical when a flag does not have a numeric measurement.
    code = flag["code"]
    severity = flag["severity"]
    category = flag["category"]
    message = flag["message"]
    require(
        isinstance(code, str)
        and len(code) <= 64
        and SCREENING_QUALITY_CODE_PATTERN.fullmatch(code) is not None
        and severity in SCREENING_QUALITY_SEVERITIES
        and category in SELECTED_QUALITY_CATEGORIES
        and isinstance(message, str)
        and message == message.strip()
        and 0 < len(message) <= 240,
        "Quality selected quality contract flag is invalid",
    )
    require(
        not any(
            unicodedata.category(character) in {"Cc", "Cf"}
            for character in message
        )
        and RAW_URL_PATTERN.search(message) is None
        and ABSOLUTE_POSIX_PATH_PATTERN.search(message) is None
        and "\\" not in message,
        "Quality selected quality contract flag exposes protected text",
    )
    return flag


def _validate_all_scope_market_fact_contract(
    market: dict[str, Any],
    *,
    token: str,
    daily_report: dict[str, Any],
) -> dict[str, Any]:
    """Validate every fact family for one scope=all Quality market.

    Screening parity covers the complete catalog, so it must not validate only
    the screening badges while leaving non-primary market facts unchecked.
    """
    market_type = market.get("market_type")
    market_id = market.get("market_id")
    require(
        market_type in {"cex", "dex"}
        and isinstance(market_id, str)
        and market_id.startswith("{}:".format(market_type))
        and market.get("token_symbol") == token,
        "Quality all-scope market identity/type is inconsistent",
    )
    facts = market.get("facts")
    require(
        isinstance(facts, dict) and set(facts) == QUALITY_FACT_NAMES,
        "Quality all-scope market has missing or unknown fact families",
    )
    selected_status = market.get("quality_status")
    selected_flags = market.get("quality_flags")
    require(
        selected_status in SCREENING_QUALITY_STATUSES
        and isinstance(selected_flags, list)
        and (selected_status == "ok" or bool(selected_flags)),
        "Quality all-scope selected quality projection is invalid",
    )
    selected_flags_by_code: dict[str, dict[str, Any]] = {}
    for raw_flag in selected_flags:
        flag = _validate_selected_quality_flag(raw_flag)
        require(
            flag["code"] not in selected_flags_by_code,
            "Quality all-scope selected quality flags are duplicated",
        )
        selected_flags_by_code[flag["code"]] = flag
    require(
        selected_status == _quality_status_from_flags(
            list(selected_flags_by_code.values())
        ),
        "Quality all-scope selected quality status differs from its flags",
    )
    fact_flags_by_code: dict[str, dict[str, Any]] = {}
    daily_evidence: dict[str, Any] | None = None
    for fact_name in QUALITY_FACT_NAMES:
        fact = facts[fact_name]
        status = fact.get("status") if isinstance(fact, dict) else None
        reason_code = fact.get("reason_code") if isinstance(fact, dict) else None
        retryable = fact.get("retryable") if isinstance(fact, dict) else None
        action = fact.get("action") if isinstance(fact, dict) else None
        flags = fact.get("quality_flags") if isinstance(fact, dict) else None
        require(
            isinstance(fact, dict)
            and isinstance(status, str)
            and 0 < len(status) <= 64
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(status) is not None
            and isinstance(reason_code, str)
            and 0 < len(reason_code) <= 64
            and SCREENING_QUALITY_CODE_PATTERN.fullmatch(reason_code) is not None
            and type(retryable) is bool
            and "action" in fact
            and (action is None or isinstance(action, str))
            and isinstance(flags, list),
            "Quality all-scope market has an invalid fact projection",
        )
        for raw_flag in flags:
            flag = _validate_selected_quality_flag(raw_flag)
            prior = fact_flags_by_code.get(flag["code"])
            require(
                prior is None or prior == flag,
                "Quality all-scope facts contain conflicting flag projections",
            )
            fact_flags_by_code[flag["code"]] = flag
        rule = _release_quality_fact_rule(
            market_type,
            fact_name,
            status,
            reason_code,
        )
        require(
            rule is not None and retryable is rule.retryable,
            "Quality all-scope market does not use a canonical fact outcome",
        )
        if fact_name == "daily":
            daily_evidence = _validate_daily_fact_evidence(
                fact,
                market_type=market_type,
                report_status=daily_report["status"],
            )
            require(
                daily_evidence
                == daily_report["market_rollups"].get(market_id),
                "Quality all-scope daily fact does not match its report rollup",
            )
            continue
        try:
            expected_action = _release_quality_fact_action(
                market_type,
                fact_name,
                status,
                reason_code,
                retryable,
            )
        except ValueError as error:
            raise ReleaseCheckError(
                "Quality all-scope market does not use a canonical fact action"
            ) from error
        require(
            action == expected_action,
            "Quality all-scope market does not use a canonical fact action",
        )
    require(
        selected_flags_by_code == fact_flags_by_code,
        "Quality all-scope selected quality flags differ from fact flags",
    )
    assert daily_evidence is not None
    return daily_evidence


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

    declared_market_ids = {
        market.get("market_id")
        for market in markets
        if isinstance(market, dict)
        and isinstance(market.get("market_id"), str)
    }
    require(
        all(
            isinstance(market, dict)
            and isinstance(market.get("market_id"), str)
            and bool(market["market_id"])
            and market["market_id"] == market["market_id"].strip()
            for market in markets
        )
        and len(declared_market_ids) == len(markets),
        "Quality market IDs are invalid or duplicated",
    )
    daily_report = _validate_daily_quality_report(
        metadata.get("daily_quality_report"),
        expected_market_ids=declared_market_ids,
    )
    require(
        daily_report["status"] == "matched",
        "Screening Quality daily audit is not matched to the current import",
    )

    market_ids: set[str] = set()
    status_counts: Counter[str] = Counter()
    alert_counts: Counter[str] = Counter()
    daily_evidence_rows: list[dict[str, Any]] = []
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
        daily_evidence_rows.append(
            _validate_all_scope_market_fact_contract(
                market,
                token=token,
                daily_report=daily_report,
            )
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
        _validate_screening_context(market)
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
        normalized_screening_flags = []
        for raw_flag in flags:
            flag = _validate_screening_flag(raw_flag)
            normalized_screening_flags.append(flag)
            alert_counts[flag["severity"]] += 1
        require(
            status == _quality_status_from_flags(normalized_screening_flags),
            "Quality screening status differs from its flags",
        )
        status_counts[status] += 1

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
    published_issue_count = sum(
        evidence["issue_count"]
        for evidence in daily_evidence_rows
        if evidence["mode"] == "published_daily_audit"
    )
    published_status_counts: Counter[str] = Counter()
    published_reason_counts: Counter[str] = Counter()
    published_outcome_counts: Counter[tuple[str, str]] = Counter()
    published_dates: set[str] = set()
    for evidence in daily_evidence_rows:
        if evidence["mode"] != "published_daily_audit":
            continue
        published_status_counts.update(evidence["status_counts"])
        published_reason_counts.update(evidence["reason_counts"])
        published_outcome_counts.update(evidence["outcome_counts"])
        published_dates.update(evidence["affected_dates"])
    require(
        published_issue_count == daily_report["issue_count"]
        and dict(sorted(published_status_counts.items()))
        == daily_report["status_counts"]
        and dict(sorted(published_reason_counts.items()))
        == daily_report["reason_counts"]
        and dict(sorted(published_outcome_counts.items()))
        == daily_report["outcome_counts"]
        and sorted(published_dates) == daily_report["affected_dates"],
        "Screening Quality daily facts do not reconcile to the report",
    )
    return {
        "token_symbol": token,
        "market_count": len(markets),
        "market_ids": sorted(market_ids),
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


def _canonical_cohort_timestamp(
    raw: Any,
    label: str,
    field: str,
) -> datetime:
    """Normalize one release timestamp inside a controlled error boundary."""
    require(
        isinstance(raw, str)
        and bool(raw)
        and raw == raw.strip(),
        f"{label} {field} is invalid",
    )
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ReleaseCheckError(
                f"{label} {field} is not timezone-aware"
            )
        normalized = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise ReleaseCheckError(
            f"{label} {field} is invalid"
        ) from error
    require(
        normalized.isoformat() == raw,
        f"{label} {field} is not canonical UTC",
    )
    return normalized


def validate_source_freshness(
    freshness: Any,
    *,
    label: str,
) -> datetime:
    """Require every release-critical source to be current and self-consistent."""
    require(isinstance(freshness, dict), f"{label} freshness is missing")
    required = {
        "checked_at",
        "overall_status",
        "common_comparable_end",
        "cex_daily",
        "dex_daily",
        "dex_tvl",
        "cex_depth",
        "dex_depth",
        "cex_execution",
        "dex_execution",
    }
    require(
        required.issubset(freshness),
        f"{label} freshness is incomplete",
    )
    checked_at = _canonical_cohort_timestamp(
        freshness.get("checked_at"),
        label,
        "freshness.checked_at",
    )
    require(
        freshness.get("overall_status") == "current",
        f"{label} freshness overall status is not current",
    )

    daily_ends: list[str] = []
    latest_completed = (checked_at.date() - timedelta(days=1)).isoformat()
    for source_name in ("cex_daily", "dex_daily"):
        item = freshness.get(source_name)
        require(
            isinstance(item, dict)
            and item.get("source") == source_name
            and item.get("status") == "current",
            f"{label} freshness {source_name} is not current",
        )
        available_start = item.get("available_start")
        available_end = item.get("available_end")
        completed = item.get("latest_completed_utc_day")
        require(
            _is_canonical_date(available_start)
            and _is_canonical_date(available_end)
            and available_start <= available_end
            and completed == latest_completed,
            f"{label} freshness {source_name} date bounds are invalid",
        )
        lag_days = item.get("lag_days")
        max_lag_days = item.get("max_lag_days")
        expected_lag = max(
            0,
            (
                datetime.strptime(latest_completed, "%Y-%m-%d").date()
                - datetime.strptime(available_end, "%Y-%m-%d").date()
            ).days,
        )
        require(
            type(lag_days) is int
            and type(max_lag_days) is int
            and max_lag_days >= 0
            and lag_days == expected_lag
            and lag_days <= max_lag_days,
            f"{label} freshness {source_name} lag is stale or inconsistent",
        )
        daily_ends.append(available_end)

    require(
        freshness.get("common_comparable_end") == min(daily_ends),
        f"{label} freshness common comparable end is inconsistent",
    )
    for source_name in (
        "dex_tvl",
        "cex_depth",
        "dex_depth",
        "cex_execution",
        "dex_execution",
    ):
        item = freshness.get(source_name)
        require(
            isinstance(item, dict)
            and item.get("source") == source_name
            and item.get("status") == "current",
            f"{label} freshness {source_name} is not current",
        )
        observed_at = _canonical_cohort_timestamp(
            item.get("observed_at"),
            label,
            "freshness.{}.observed_at".format(source_name),
        )
        age_hours = item.get("age_hours")
        max_age_hours = item.get("max_age_hours")
        expected_age = round(
            max(0.0, (checked_at - observed_at).total_seconds() / 3600),
            3,
        )
        require(
            type(age_hours) in {int, float}
            and not isinstance(age_hours, bool)
            and math.isfinite(age_hours)
            and type(max_age_hours) in {int, float}
            and not isinstance(max_age_hours, bool)
            and math.isfinite(max_age_hours)
            and max_age_hours > 0
            and abs(float(age_hours) - expected_age) <= 0.001
            and 0 <= float(age_hours) <= float(max_age_hours)
            and observed_at <= checked_at + timedelta(minutes=5),
            f"{label} freshness {source_name} age is stale or inconsistent",
        )
    return checked_at


def validate_lifecycle_freshness(
    lifecycle: Any,
    *,
    freshness_checked_at: datetime,
) -> None:
    """Reject releases backed by expired CEX catalog-membership evidence."""
    require(
        isinstance(lifecycle, dict)
        and lifecycle.get("schema") == "cex_instrument_lifecycle/v1",
        "Summary CEX lifecycle freshness is missing",
    )
    for field in (
        "reviewed_market_count",
        "absence_market_count",
        "applied_market_count",
        "withheld_payload_market_count",
        "stale_evidence_market_count",
    ):
        require(
            type(lifecycle.get(field)) is int and lifecycle[field] >= 0,
            "Summary CEX lifecycle counts are invalid",
        )
    require(
        lifecycle["absence_market_count"]
        <= lifecycle["reviewed_market_count"]
        and lifecycle["applied_market_count"]
        == lifecycle["absence_market_count"]
        and lifecycle["withheld_payload_market_count"]
        <= lifecycle["applied_market_count"]
        and lifecycle["stale_evidence_market_count"] == 0,
        "Summary CEX lifecycle evidence is stale",
    )
    official_inventory_count = lifecycle.get("official_inventory_count")
    response_sha256 = lifecycle.get("response_sha256")
    configured_market_hash = lifecycle.get("configured_market_ids_sha256")
    require(
        type(official_inventory_count) is int
        and official_inventory_count > 0
        and isinstance(response_sha256, str)
        and re.fullmatch(r"[0-9a-f]{64}", response_sha256) is not None,
        "Summary CEX lifecycle root evidence is invalid",
    )
    require(
        isinstance(configured_market_hash, str)
        and re.fullmatch(r"[0-9a-f]{64}", configured_market_hash) is not None,
        "Summary CEX lifecycle configured-market evidence is invalid",
    )
    max_age = lifecycle.get("freshness_max_age_seconds")
    require(
        type(max_age) is int and max_age > 0,
        "Summary CEX lifecycle freshness threshold is invalid",
    )
    checked_min = _canonical_cohort_timestamp(
        lifecycle.get("checked_at_min"),
        "Summary CEX lifecycle",
        "checked_at_min",
    )
    checked_max = _canonical_cohort_timestamp(
        lifecycle.get("checked_at_max"),
        "Summary CEX lifecycle",
        "checked_at_max",
    )
    oldest_age = (freshness_checked_at - checked_min).total_seconds()
    require(
        checked_min <= checked_max
        and -300 <= oldest_age <= max_age,
        "Summary CEX lifecycle evidence exceeds its freshness threshold",
    )


def _validate_cohort_observation_metadata(
    value: dict[str, Any],
    label: str,
) -> tuple[datetime | None, datetime | None]:
    """Independently verify canonical cohort bounds and their exact span."""
    required = {
        "observed_at",
        "observed_at_min",
        "observed_at_max",
        "observation_span_seconds",
    }
    require(
        required.issubset(value),
        f"{label} observation metadata is incomplete",
    )
    observed_at = value.get("observed_at")
    observed_at_min = value.get("observed_at_min")
    observed_at_max = value.get("observed_at_max")
    span = value.get("observation_span_seconds")
    if observed_at is None and observed_at_min is None and observed_at_max is None:
        require(span is None, f"{label} span has no observation bounds")
        return None, None
    require(
        observed_at is not None
        and observed_at_min is not None
        and observed_at_max is not None,
        f"{label} observation bounds are incomplete",
    )

    first = _canonical_cohort_timestamp(observed_at, label, "observed_at")
    lower = _canonical_cohort_timestamp(
        observed_at_min,
        label,
        "observed_at_min",
    )
    upper = _canonical_cohort_timestamp(
        observed_at_max,
        label,
        "observed_at_max",
    )
    require(first == lower, f"{label} observed_at is not the lower bound")
    expected_span = (upper - lower).total_seconds()
    require(expected_span >= 0, f"{label} observation bounds are reversed")
    require(
        type(span) in {int, float}
        and math.isfinite(span)
        and span >= 0
        and span == expected_span,
        f"{label} observation span differs from its bounds",
    )
    return lower, upper


def validate_execution(
    payload: dict[str, Any],
    *,
    token: str,
    market_a: str,
    market_b: str,
    expected_generation: str,
    catalog_metadata: dict[str, Any],
    expected_execution_generation: str | None = None,
) -> None:
    metadata = payload.get("metadata") or {}
    require(
        metadata.get("data_generation") == expected_generation,
        "Summary and Execution generations differ",
    )
    _validate_endpoint_generation(
        metadata,
        field="execution_generation",
        expected=expected_execution_generation,
        label="Execution",
    )
    require(
        metadata.get("cohort_observation_model")
        == "bounded_sequential_observations",
        "Execution cohort observation model is invalid",
    )
    require(payload.get("token_symbol") == token, "Execution returned wrong Token")
    expected_scenarios = {
        (direction, notional)
        for direction in EXECUTION_DIRECTIONS
        for notional in COLLECTED_NOTIONALS
    }
    selected_market_types: dict[str, list[dict[str, Any]]] = {}
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
        market_type = expected_market.split(":", 1)[0]
        require(
            market_type in {"cex", "dex"},
            f"Execution {label} market type is invalid",
        )
        selected_market_types.setdefault(market_type, []).extend(rows)

    snapshots = metadata.get("snapshots")
    cohort_lineage = metadata.get("cohort_lineage")
    require(
        isinstance(snapshots, dict)
        and isinstance(cohort_lineage, dict),
        "Execution cohort metadata is missing",
    )
    expected_market_types = set(selected_market_types)
    require(
        set(snapshots) == expected_market_types
        and set(cohort_lineage) == expected_market_types,
        "Execution cohort metadata is not bounded to selected market types",
    )
    lineage_fields = {
        "market_type",
        "depth_snapshot_id",
        "execution_snapshot_id",
        "execution_source_snapshot_id",
        "depth_market_count",
        "execution_market_count",
    }
    for market_type, rows in selected_market_types.items():
        depth = catalog_metadata.get(f"{market_type}_depth_snapshot")
        snapshot = snapshots.get(market_type)
        lineage = cohort_lineage.get(market_type)
        require(
            isinstance(depth, dict)
            and isinstance(snapshot, dict)
            and isinstance(lineage, dict),
            f"Execution {market_type.upper()} cohort metadata is missing",
        )
        depth_lower, depth_upper = _validate_cohort_observation_metadata(
            depth,
            f"Execution {market_type.upper()} depth cohort",
        )
        execution_lower, execution_upper = (
            _validate_cohort_observation_metadata(
                snapshot,
                f"Execution {market_type.upper()} execution cohort",
            )
        )

        def one_id(value: Any, label: str) -> str:
            require(
                isinstance(value, list)
                and len(value) == 1
                and isinstance(value[0], str)
                and bool(value[0])
                and value[0] == value[0].strip(),
                label,
            )
            return value[0]

        depth_snapshot_id = one_id(
            depth.get("snapshot_ids"),
            f"Execution {market_type.upper()} depth lineage is invalid",
        )
        execution_snapshot_id = one_id(
            snapshot.get("snapshot_ids"),
            f"Execution {market_type.upper()} snapshot lineage is invalid",
        )
        execution_source_snapshot_id = one_id(
            snapshot.get("source_snapshot_ids"),
            f"Execution {market_type.upper()} source lineage is invalid",
        )
        row_snapshot_values = [row.get("snapshot_id") for row in rows]
        row_source_snapshot_values = [
            row.get("source_snapshot_id") for row in rows
        ]
        require(
            all(
                isinstance(value, str)
                and bool(value)
                and value == value.strip()
                for value in (
                    row_snapshot_values + row_source_snapshot_values
                )
            ),
            f"Execution {market_type.upper()} row lineage is invalid",
        )
        row_snapshot_ids = set(row_snapshot_values)
        row_source_snapshot_ids = set(row_source_snapshot_values)
        require(
            {
                depth_snapshot_id,
                execution_snapshot_id,
                execution_source_snapshot_id,
            }
            == {depth_snapshot_id}
            and row_snapshot_ids == {depth_snapshot_id}
            and row_source_snapshot_ids == {depth_snapshot_id},
            f"Execution {market_type.upper()} cohort snapshot IDs differ",
        )
        depth_count_field = (
            "market_rows" if market_type == "cex" else "pool_rows"
        )
        depth_market_count = depth.get(depth_count_field)
        execution_market_count = snapshot.get("market_count")
        require(
            type(depth_market_count) is int
            and depth_market_count > 0
            and type(execution_market_count) is int
            and execution_market_count == depth_market_count,
            f"Execution {market_type.upper()} cohort market counts differ",
        )
        require(
            depth_lower is not None
            and depth_upper is not None
            and execution_lower is not None
            and execution_upper is not None,
            f"Execution {market_type.upper()} positive cohort inventory "
            "lacks observation bounds",
        )
        for index, row in enumerate(rows):
            row_observed_at = _canonical_cohort_timestamp(
                row.get("observed_at"),
                f"Execution {market_type.upper()} selected row {index + 1}",
                "observed_at",
            )
            require(
                execution_lower <= row_observed_at <= execution_upper,
                f"Execution {market_type.upper()} selected row observed_at "
                "is outside declared full-inventory bounds",
            )
        expected_lineage = {
            "market_type": market_type,
            "depth_snapshot_id": depth_snapshot_id,
            "execution_snapshot_id": execution_snapshot_id,
            "execution_source_snapshot_id": execution_source_snapshot_id,
            "depth_market_count": depth_market_count,
            "execution_market_count": execution_market_count,
        }
        require(
            set(lineage) == lineage_fields,
            f"Execution {market_type.upper()} cohort lineage has unknown fields",
        )
        require(
            lineage.get("market_type") == market_type
            and all(
                isinstance(lineage.get(field), str)
                and bool(lineage[field])
                and lineage[field] == lineage[field].strip()
                for field in (
                    "depth_snapshot_id",
                    "execution_snapshot_id",
                    "execution_source_snapshot_id",
                )
            )
            and all(
                type(lineage.get(field)) is int and lineage[field] > 0
                for field in (
                    "depth_market_count",
                    "execution_market_count",
                )
            ),
            f"Execution {market_type.upper()} cohort lineage types are invalid",
        )
        require(
            lineage == expected_lineage,
            f"Execution {market_type.upper()} cohort lineage differs from catalog",
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
    application_sha, asset_sha, asset_version = validate_release_health(
        health,
        expected_application_sha=getattr(args, "expected_application_sha", None),
        expected_asset_sha=getattr(args, "expected_asset_sha", None),
    )
    served_asset_sha, asset_metrics = fetch_static_asset_bundle(
        args.base_url,
        asset_version,
        timeout=args.timeout,
    )
    metrics.extend(asset_metrics)
    require(
        served_asset_sha == asset_sha,
        "Versioned served assets do not match the deployed asset SHA",
    )

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
    audited_market_pairs: set[tuple[str, str]] = set()
    audited_market_ids: set[str] = set()
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
        for market_id in parity["market_ids"]:
            market_pair = (quality_token, market_id)
            require(
                market_id not in audited_market_ids,
                "Screening Quality market ID is reused across Tokens",
            )
            require(
                market_pair not in audited_market_pairs,
                "Screening Quality market identity is duplicated",
            )
            audited_market_ids.add(market_id)
            audited_market_pairs.add(market_pair)

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
    summary_primary_market_pairs = {
        (row["token_symbol"], primary["refresh_market_id"])
        for row in summary["tokens"]
        if isinstance(row, dict) and isinstance(row.get("token_symbol"), str)
        for primary in (row.get("primary_cex"), row.get("primary_dex"))
        if isinstance(primary, dict)
        and isinstance(primary.get("refresh_market_id"), str)
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
    require(
        token_catalog.get("metadata", {}).get(
            "configured_cex_market_identities"
        )
        == summary_metadata.get("configured_cex_market_identities"),
        "Summary and Token catalog configured Upbit identities differ",
    )
    token_catalog_market_ids = {
        str(market["market_id"])
        for market in markets
        if isinstance(market, dict)
    }

    full_catalog, full_metrics = fetch_json(
        args.base_url,
        "/api/markets/catalog",
        timeout=args.timeout,
    )
    metrics.append(full_metrics)
    full_catalog_metadata = full_catalog.get("metadata")
    require(
        isinstance(full_catalog_metadata, dict)
        and full_catalog_metadata.get("data_generation") == generation,
        "Summary and full catalog generation differ",
    )
    configured_upbit_market_ids = (
        validate_configured_cex_identity_metadata(full_catalog_metadata)
    )
    require(
        full_catalog_metadata.get("configured_cex_market_identities")
        == summary_metadata.get("configured_cex_market_identities"),
        "Summary and full catalog configured Upbit identities differ",
    )
    full_markets = full_catalog.get("markets")
    require(isinstance(full_markets, list), "Full audit catalog has no markets array")
    full_catalog_tokens: set[str] = set()
    full_market_ids: set[str] = set()
    full_market_pairs: set[tuple[str, str]] = set()
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
        full_market_pairs.add((market_token, market_id))
        full_catalog_tokens.add(market_token)
        validate_exact_cex_market_identity(
            market_id,
            market_token,
            configured_upbit_market_ids=configured_upbit_market_ids,
        )
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
    require(
        audited_market_pairs == full_market_pairs,
        "Screening Quality exact market inventory differs from the full catalog",
    )
    lifecycle = summary_metadata.get("cex_instrument_lifecycle")
    require(isinstance(lifecycle, dict), "Summary lifecycle catalog is missing")
    crypto_com_market_ids = {
        market_id
        for market_id in full_market_ids
        if market_id.startswith("cex:crypto_com:")
    }
    try:
        crypto_com_market_hash = configured_market_ids_sha256(
            crypto_com_market_ids
        )
    except (TypeError, ValueError) as error:
        raise ReleaseCheckError(
            "Full lifecycle catalog identity inventory is invalid"
        ) from error
    require(
        len(crypto_com_market_ids) == lifecycle["reviewed_market_count"]
        and crypto_com_market_hash
        == lifecycle["configured_market_ids_sha256"],
        "Summary lifecycle catalog does not match the full catalog",
    )
    require(
        summary_primary_market_pairs <= full_market_pairs,
        "Summary primary market refresh identity is absent from the full catalog",
    )
    require(
        token_catalog_market_ids
        == {
            market_id
            for market_token, market_id in full_market_pairs
            if market_token == token
        },
        "Token catalog inventory differs from the full audit catalog",
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
        expected_generation=generation,
        expected_comparison_generation=generation,
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
        expected_generation=generation,
        expected_execution_generation=generation,
        catalog_metadata=full_catalog.get("metadata") or {},
    )

    final_health, final_health_metrics = fetch_json(
        args.base_url,
        "/health",
        timeout=args.timeout,
    )
    metrics.append(final_health_metrics)
    require(final_health.get("status") == "ok", "Final Health status is not ok")
    require(
        final_health.get("data_ready") is True,
        "Final Health reports data_ready=false",
    )
    final_application_sha, final_asset_sha, final_asset_version = (
        validate_release_health(
            final_health,
            expected_application_sha=application_sha,
            expected_asset_sha=asset_sha,
        )
    )
    require(
        (final_application_sha, final_asset_sha, final_asset_version)
        == (application_sha, asset_sha, asset_version),
        "Application or frontend assets changed during release validation",
    )
    final_served_asset_sha, final_asset_metrics = fetch_static_asset_bundle(
        args.base_url,
        final_asset_version,
        timeout=args.timeout,
    )
    metrics.extend(final_asset_metrics)
    require(
        final_served_asset_sha == final_asset_sha,
        "Final versioned served assets do not match the deployed asset SHA",
    )

    final_summary, final_summary_metrics = fetch_json(
        args.base_url,
        "/api/markets/summary",
        timeout=args.timeout,
    )
    metrics.append(final_summary_metrics)
    final_summary_metadata = final_summary.get("metadata") or {}
    require(
        final_summary_metadata.get("data_generation") == generation,
        "Published data generation changed during release validation",
    )
    final_freshness_checked_at = validate_source_freshness(
        final_summary_metadata.get("freshness"),
        label="Final Summary",
    )
    validate_lifecycle_freshness(
        final_summary_metadata.get("cex_instrument_lifecycle"),
        freshness_checked_at=final_freshness_checked_at,
    )

    return {
        "status": "ok",
        "base_url": args.base_url,
        "token": token,
        "window": {"start": start, "end": end},
        "data_generation": generation,
        "application_sha": application_sha,
        "asset_sha": asset_sha,
        "asset_version": asset_version,
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
    parser.add_argument(
        "--expected-application-sha",
        help="Require /health to report this exact deployed Git SHA",
    )
    parser.add_argument(
        "--expected-asset-sha",
        help="Require /health to report this exact deployed frontend asset SHA",
    )
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
