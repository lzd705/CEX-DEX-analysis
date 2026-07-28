#!/usr/bin/env python3
"""Fail closed when a deployed dashboard violates its public fact contract."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import time
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
