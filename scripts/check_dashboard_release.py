#!/usr/bin/env python3
"""Fail closed when a deployed dashboard violates its public fact contract."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import time
from collections import Counter
from dataclasses import dataclass
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
    require(metadata.get("summary_version") == 1, "Summary version is not 1")
    require(isinstance(tokens, list) and tokens, "Summary has no Token rows")
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
    token_symbols = {
        row.get("token_symbol") for row in tokens if isinstance(row, dict)
    }
    require(isinstance(generation, str) and generation, "Summary generation is missing")
    require(isinstance(start, str) and start, "Summary start_date is missing")
    require(isinstance(end, str) and end, "Summary end_date is missing")
    require(token in token_symbols, "Default workspace Token is absent from summary")
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
) -> None:
    metadata = payload.get("metadata") or {}
    markets = payload.get("markets")
    expected_ids = {market_a, market_b}
    require(payload.get("token_symbol") == token, "Quality returned wrong Token")
    require(metadata.get("scope") == "selected", "Quality did not honor selected scope")
    require(
        set(metadata.get("selected_market_ids") or []) == expected_ids,
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
    require(
        len(full_markets) == summary["metadata"].get("catalog_market_count"),
        "Summary catalog count differs from the full audit catalog",
    )

    all_events, events_metrics = fetch_json(
        args.base_url,
        "/api/markets/events",
        timeout=args.timeout,
    )
    metrics.append(events_metrics)
    event_rows = validate_events(all_events)
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
        "catalog_market_count": len(full_markets),
        "event_count": len(event_rows),
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
