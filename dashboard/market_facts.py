"""Pure market-fact contracts shared by the API and known-answer tests."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any


CATALOG_VERSION = 2
DAILY_GRAIN = "1 day, UTC"
PRICE_QUOTE_ASSET = "USD"
MISSING_VALUE_RULE = (
    "Preserve missing source values as null; do not forward-fill or replace them "
    "with zero. Compare prices only when both selected markets have a finite close "
    "on the same UTC date."
)
SERIES_STATISTICS_METHOD = "adjacent_utc_daily_log_returns_only_v1"
PRIMARY_SELECTION_METHOD = "quality_weighted_primary_v1"
MARKET_QUALITY_THRESHOLDS = {
    # These are explicit product-quality heuristics, not exchange or protocol
    # guarantees. They are returned in API metadata so consumers can audit them.
    "tiny_pool_tvl_usd": 100_000.0,
    "off_market_price_deviation_bps": 100.0,
    "critical_off_market_price_deviation_bps": 500.0,
    "wide_cex_quoted_spread_bps": 100.0,
    "minimum_primary_coverage_ratio": 0.80,
}
PRIMARY_SELECTION_WEIGHTS = {
    "window_volume_share": 40.0,
    "coverage_ratio": 25.0,
    "quote_quality": 20.0,
    "depth_support": 15.0,
}


def finite_number(value: Any) -> float | None:
    """Return one finite float while preserving invalid or missing values as null."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _iso_day(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def market_series_statistics(
    rows: list[dict[str, Any]],
    *,
    date_field: str = "date",
    price_field: str = "close",
    requested_start: str | None = None,
    requested_end: str | None = None,
) -> dict[str, Any]:
    """Calculate auditable price-series statistics without filling missing days.

    Window return remains the simple first-to-last return and is explicitly
    described as such. Daily volatility uses only log returns between adjacent
    UTC calendar dates. A gap therefore never masquerades as a one-day return.
    """
    by_day: dict[str, float] = {}
    for row in rows:
        day_text = row.get(date_field)
        day = _iso_day(day_text)
        price = finite_number(row.get(price_field))
        if day is None or price is None or price <= 0:
            continue
        by_day[day.isoformat()] = price
    observations = sorted(by_day.items())

    requested_start_day = _iso_day(requested_start)
    requested_end_day = _iso_day(requested_end)
    requested_window_days = (
        (requested_end_day - requested_start_day).days + 1
        if requested_start_day is not None
        and requested_end_day is not None
        and requested_start_day <= requested_end_day
        else None
    )
    if not observations:
        return {
            "price_usd": None,
            "window_return": None,
            "daily_volatility": None,
            "first_observed_date": None,
            "latest_observed_date": None,
            "calendar_span_days": 0,
            "requested_window_days": requested_window_days,
            "observation_count": 0,
            "coverage_ratio": 0.0 if requested_window_days else None,
            "missing_calendar_days": requested_window_days,
            "return_interval_count": 0,
            "skipped_gap_interval_count": 0,
            "max_gap_days": None,
            "window_return_method": "first_to_last_observed_close",
            "daily_volatility_method": SERIES_STATISTICS_METHOD,
        }

    first_date = _iso_day(observations[0][0])
    latest_date = _iso_day(observations[-1][0])
    assert first_date is not None and latest_date is not None
    calendar_span_days = (latest_date - first_date).days + 1
    coverage_denominator = requested_window_days or calendar_span_days
    observation_count = len(observations)

    adjacent_log_returns: list[float] = []
    skipped_gap_intervals = 0
    max_gap_days = 0
    for previous, current in zip(observations, observations[1:]):
        previous_date = _iso_day(previous[0])
        current_date = _iso_day(current[0])
        assert previous_date is not None and current_date is not None
        interval_days = (current_date - previous_date).days
        max_gap_days = max(max_gap_days, interval_days - 1)
        if interval_days != 1:
            skipped_gap_intervals += 1
            continue
        adjacent_log_returns.append(math.log(current[1] / previous[1]))

    first_price = observations[0][1]
    latest_price = observations[-1][1]
    window_return = (
        latest_price / first_price - 1 if observation_count >= 2 else None
    )
    daily_volatility = (
        statistics.stdev(adjacent_log_returns)
        if len(adjacent_log_returns) >= 2
        else None
    )
    return {
        "price_usd": latest_price,
        "window_return": window_return,
        "daily_volatility": daily_volatility,
        "first_observed_date": first_date.isoformat(),
        "latest_observed_date": latest_date.isoformat(),
        "calendar_span_days": calendar_span_days,
        "requested_window_days": requested_window_days,
        "observation_count": observation_count,
        "coverage_ratio": (
            observation_count / coverage_denominator
            if coverage_denominator
            else None
        ),
        "missing_calendar_days": max(coverage_denominator - observation_count, 0),
        "return_interval_count": len(adjacent_log_returns),
        "skipped_gap_interval_count": skipped_gap_intervals,
        "max_gap_days": max_gap_days,
        "window_return_method": "first_to_last_observed_close",
        "daily_volatility_method": SERIES_STATISTICS_METHOD,
    }


def _quality_flag(
    code: str,
    severity: str,
    message: str,
    *,
    observed_value: Any = None,
    threshold: Any = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "observed_value": observed_value,
        "threshold": threshold,
    }


def market_quality_assessment(row: dict[str, Any]) -> dict[str, Any]:
    """Return machine-readable and human-auditable market quality flags."""
    flags: list[dict[str, Any]] = []
    market_type = row.get("market") or row.get("market_type")
    depth_status = row.get(
        "depth_status" if market_type == "cex" else "dex_depth_status",
        row.get("depth_status"),
    )
    normalized_status = str(depth_status or "unavailable").lower()
    if normalized_status in {
        "unsupported",
        "unsupported_protocol",
        "unsupported_chain",
    }:
        flags.append(
            _quality_flag(
                "depth_unsupported",
                "warning",
                "Executable depth is unsupported for this market.",
                observed_value=depth_status,
            )
        )
    elif normalized_status == "partial":
        flags.append(
            _quality_flag(
                "depth_partial",
                "warning",
                "Depth is a measured lower bound because one or more bands are incomplete.",
                observed_value=depth_status,
            )
        )
    elif normalized_status in {"failed", "error"}:
        flags.append(
            _quality_flag(
                "depth_failed",
                "critical",
                "The most recent executable-depth collection failed.",
                observed_value=depth_status,
            )
        )
    elif normalized_status not in {"observed", "complete"}:
        flags.append(
            _quality_flag(
                "depth_unavailable",
                "info",
                "No executable-depth observation is available.",
                observed_value=depth_status,
            )
        )

    total_depth_10bps = finite_number(row.get("total_depth_10bps_usd"))
    if normalized_status in {"observed", "partial", "complete"} and total_depth_10bps == 0:
        spread_bps = finite_number(
            row.get("spread_bps", row.get("quoted_spread_bps"))
        )
        flags.append(
            _quality_flag(
                "zero_depth_10bps",
                "warning",
                (
                    "No executable notional was observed inside the ±10 bps band; "
                    "the band may lie inside the quoted spread."
                ),
                observed_value=total_depth_10bps,
                threshold={"band_bps": 10, "quoted_spread_bps": spread_bps},
            )
        )

    if market_type == "dex":
        tvl = finite_number(row.get("tvl_usd"))
        tiny_threshold = MARKET_QUALITY_THRESHOLDS["tiny_pool_tvl_usd"]
        if tvl is not None and tvl < tiny_threshold:
            flags.append(
                _quality_flag(
                    "tiny_pool",
                    "warning",
                    "The point-in-time pool TVL is below the declared quality threshold.",
                    observed_value=tvl,
                    threshold=tiny_threshold,
                )
            )
        difference_bps = finite_number(row.get("price_difference_bps"))
        if difference_bps is None:
            pool_state_price = finite_number(row.get("pool_state_price_usd"))
            source_target_price = finite_number(row.get("source_target_price_usd"))
            if (
                pool_state_price is not None
                and source_target_price is not None
                and pool_state_price > 0
                and source_target_price > 0
            ):
                difference_bps = (
                    (pool_state_price / source_target_price) - 1
                ) * 10_000
        warning_threshold = MARKET_QUALITY_THRESHOLDS[
            "off_market_price_deviation_bps"
        ]
        critical_threshold = MARKET_QUALITY_THRESHOLDS[
            "critical_off_market_price_deviation_bps"
        ]
        if (
            difference_bps is not None
            and abs(difference_bps) > warning_threshold
        ):
            severity = (
                "critical"
                if abs(difference_bps) > critical_threshold
                else "warning"
            )
            flags.append(
                _quality_flag(
                    "off_market_pool_state_price",
                    severity,
                    "Pool-state price deviates materially from the source target price.",
                    observed_value=difference_bps,
                    threshold=(
                        critical_threshold
                        if severity == "critical"
                        else warning_threshold
                    ),
                )
            )
    else:
        spread_bps = finite_number(
            row.get("spread_bps", row.get("quoted_spread_bps"))
        )
        spread_threshold = MARKET_QUALITY_THRESHOLDS[
            "wide_cex_quoted_spread_bps"
        ]
        if spread_bps is not None and spread_bps > spread_threshold:
            flags.append(
                _quality_flag(
                    "wide_quoted_spread",
                    "warning",
                    "Quoted CEX spread exceeds the declared quality threshold.",
                    observed_value=spread_bps,
                    threshold=spread_threshold,
                )
            )

    coverage_ratio = finite_number(row.get("coverage_ratio"))
    coverage_threshold = MARKET_QUALITY_THRESHOLDS[
        "minimum_primary_coverage_ratio"
    ]
    if coverage_ratio is not None and coverage_ratio < coverage_threshold:
        flags.append(
            _quality_flag(
                "low_daily_coverage",
                "warning",
                "Daily close coverage is below the declared primary-market threshold.",
                observed_value=coverage_ratio,
                threshold=coverage_threshold,
            )
        )

    severity_rank = {"info": 1, "warning": 2, "critical": 3}
    worst_rank = max((severity_rank[flag["severity"]] for flag in flags), default=0)
    quality_status = {0: "ok", 1: "info", 2: "warning", 3: "critical"}[worst_rank]
    return {
        "quality_status": quality_status,
        "quality_flags": [flag["code"] for flag in flags],
        "quality_flag_details": flags,
        "quality_thresholds_version": "market_quality_v1",
    }


def enrich_market_quality(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, **market_quality_assessment(row)}


def market_identity(row: dict[str, Any]) -> str | None:
    market_type = row.get("market") or row.get("market_type")
    if market_type == "cex":
        venue = row.get("venue")
        instrument = row.get("instrument")
        return f"{venue}|{instrument}" if venue and instrument else None
    return row.get("pool_address") or row.get("market_id")


def _depth_support_score(row: dict[str, Any]) -> float:
    market_type = row.get("market") or row.get("market_type")
    status = row.get(
        "depth_status" if market_type == "cex" else "dex_depth_status",
        row.get("depth_status"),
    )
    normalized = str(status or "").lower()
    if normalized in {"observed", "complete"}:
        return 1.0
    if normalized == "partial":
        return 0.5
    return 0.0


def _quote_quality_score(row: dict[str, Any]) -> float:
    price = finite_number(row.get("price_usd"))
    if price is None or price <= 0:
        return 0.0
    assessment = market_quality_assessment(row)
    critical = {
        detail["code"]
        for detail in assessment["quality_flag_details"]
        if detail["severity"] == "critical"
    }
    if critical:
        return 0.0
    warning_codes = set(assessment["quality_flags"])
    if warning_codes & {
        "off_market_pool_state_price",
        "wide_quoted_spread",
        "zero_depth_10bps",
        "tiny_pool",
    }:
        return 0.5
    return 1.0


def select_primary_market(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Choose one primary market with an auditable, deterministic score."""
    if not rows:
        return None, {
            "method": PRIMARY_SELECTION_METHOD,
            "candidate_count": 0,
            "selected_market_id": None,
            "score": None,
            "components": None,
        }
    positive_volumes = [
        max(finite_number(row.get("volume_usd")) or 0.0, 0.0)
        for row in rows
    ]
    max_volume = max(positive_volumes, default=0.0)
    candidates = []
    for row, volume in zip(rows, positive_volumes):
        volume_share = volume / max_volume if max_volume else 0.0
        coverage = finite_number(row.get("coverage_ratio"))
        if coverage is None:
            observation_count = finite_number(row.get("observation_count"))
            requested_days = finite_number(row.get("requested_window_days"))
            coverage = (
                observation_count / requested_days
                if observation_count is not None
                and requested_days is not None
                and requested_days > 0
                else 0.0
            )
        coverage = min(max(coverage, 0.0), 1.0)
        quote_quality = _quote_quality_score(row)
        depth_support = _depth_support_score(row)
        components = {
            "window_volume_share": volume_share
            * PRIMARY_SELECTION_WEIGHTS["window_volume_share"],
            "coverage_ratio": coverage
            * PRIMARY_SELECTION_WEIGHTS["coverage_ratio"],
            "quote_quality": quote_quality
            * PRIMARY_SELECTION_WEIGHTS["quote_quality"],
            "depth_support": depth_support
            * PRIMARY_SELECTION_WEIGHTS["depth_support"],
        }
        inputs = {
            "window_volume_usd": volume,
            "volume_share_of_highest_volume_candidate": volume_share,
            "coverage_ratio": coverage,
            "quote_quality_score": quote_quality,
            "depth_support_score": depth_support,
        }
        score = sum(components.values())
        candidates.append(
            (
                score,
                volume,
                coverage,
                str(market_identity(row) or ""),
                row,
                components,
                inputs,
            )
        )
    score, _, coverage, _, selected, components, inputs = max(
        candidates,
        key=lambda candidate: (
            candidate[0],
            candidate[1],
            candidate[2],
            candidate[3],
        ),
    )
    reason = {
        "method": PRIMARY_SELECTION_METHOD,
        "candidate_count": len(rows),
        "selected_market_id": market_identity(selected),
        "score": score,
        "components": components,
        "inputs": inputs,
        "weights": dict(PRIMARY_SELECTION_WEIGHTS),
        "coverage_threshold": MARKET_QUALITY_THRESHOLDS[
            "minimum_primary_coverage_ratio"
        ],
    }
    return selected, reason


def _common_price_comparison(
    cex: dict[str, Any] | None,
    dex: dict[str, Any] | None,
) -> tuple[str | None, float | None]:
    if not cex or not dex:
        return None, None
    cex_prices = {
        point["date"]: finite_number(point.get("price_usd"))
        for point in cex.get("price_points", [])
    }
    dex_prices = {
        point["date"]: finite_number(point.get("price_usd"))
        for point in dex.get("price_points", [])
    }
    common_dates = sorted(
        day
        for day in set(cex_prices) & set(dex_prices)
        if cex_prices[day] is not None
        and dex_prices[day] is not None
        and cex_prices[day] > 0
    )
    if not common_dates:
        return None, None
    comparison_date = common_dates[-1]
    spread = dex_prices[comparison_date] / cex_prices[comparison_date] - 1
    return comparison_date, spread


def build_token_summaries(
    cex_markets: list[dict[str, Any]],
    dex_markets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate all venue volume and select quality-weighted primary markets."""
    by_token_cex: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_token_dex: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cex_markets:
        by_token_cex[row["token_symbol"]].append(row)
    for row in dex_markets:
        by_token_dex[row["token_symbol"]].append(row)

    summaries = []
    for token in sorted(set(by_token_cex) | set(by_token_dex)):
        cex_rows = by_token_cex[token]
        dex_rows = by_token_dex[token]
        primary_cex, cex_reason = select_primary_market(cex_rows)
        primary_dex, dex_reason = select_primary_market(dex_rows)
        cex_volumes = [
            max(value, 0.0)
            for row in cex_rows
            if (value := finite_number(row.get("volume_usd"))) is not None
        ]
        dex_volumes = [
            max(value, 0.0)
            for row in dex_rows
            if (value := finite_number(row.get("volume_usd"))) is not None
        ]
        aggregate_cex = sum(cex_volumes) if cex_volumes else None
        aggregate_dex = sum(dex_volumes) if dex_volumes else None
        observed_aggregates = [
            value
            for value in (aggregate_cex, aggregate_dex)
            if value is not None
        ]
        aggregate_volume = (
            sum(observed_aggregates) if observed_aggregates else None
        )
        comparison_date, spread = _common_price_comparison(
            primary_cex,
            primary_dex,
        )
        selected_cex_volume = (
            finite_number(primary_cex.get("volume_usd"))
            if primary_cex
            else None
        )
        selected_dex_volume = (
            finite_number(primary_dex.get("volume_usd"))
            if primary_dex
            else None
        )
        summary = {
            "token_symbol": token,
            "aggregate_cex_volume_usd": aggregate_cex,
            "aggregate_dex_volume_usd": aggregate_dex,
            "aggregate_volume_usd": aggregate_volume,
            "aggregate_dex_volume_share": (
                aggregate_dex / aggregate_volume
                if aggregate_cex is not None
                and aggregate_dex is not None
                and aggregate_volume
                else None
            ),
            "volume_aggregation_method": (
                "sum_of_all_cataloged_market_series_in_selected_window"
            ),
            "selected_cex_volume_usd": selected_cex_volume,
            "selected_dex_volume_usd": selected_dex_volume,
            "selected_pair_volume_usd": (
                (selected_cex_volume or 0.0) + (selected_dex_volume or 0.0)
                if selected_cex_volume is not None
                or selected_dex_volume is not None
                else None
            ),
            "price_spread": spread,
            "spread_date": comparison_date,
            "primary_cex_id": market_identity(primary_cex) if primary_cex else None,
            "primary_dex_id": market_identity(primary_dex) if primary_dex else None,
            "primary_cex_selection_reason": cex_reason,
            "primary_dex_selection_reason": dex_reason,
            # Backward-compatible aliases. Their aggregate meaning is now
            # explicitly documented and new consumers should use aggregate_*.
            "cex_volume_usd": aggregate_cex,
            "dex_volume_usd": aggregate_dex,
            "total_volume_usd": aggregate_volume,
            "observed_dex_share": (
                aggregate_dex / aggregate_volume
                if aggregate_cex is not None
                and aggregate_dex is not None
                and aggregate_volume
                else None
            ),
        }
        summaries.append(summary)
    return summaries


def dex_pool_counts(dex_markets: list[dict[str, Any]]) -> dict[str, int]:
    """Distinguish token-perspective market series from physical DEX pools."""
    pool_ids = set()
    for row in dex_markets:
        venue = row.get("venue")
        address = row.get("pool_address")
        if not venue or " / " not in venue or not address:
            continue
        chain, dex = venue.split(" / ", 1)
        pool_ids.add(dex_pool_id(chain, dex, address))
    return {
        "market_series_rows": len(dex_markets),
        "unique_pool_count": len(pool_ids),
    }


def attach_explicit_dex_counts(
    metadata: dict[str, Any],
    dex_markets: list[dict[str, Any]],
) -> dict[str, Any]:
    """Add unambiguous DEX counts while keeping legacy pool_rows readable."""
    counts = dex_pool_counts(dex_markets)
    result = dict(metadata)
    result["dex_market_series_rows"] = counts["market_series_rows"]
    result["dex_unique_pool_count"] = counts["unique_pool_count"]
    for key in ("tvl_snapshot", "dex_depth_snapshot"):
        snapshot = result.get(key)
        if not isinstance(snapshot, dict):
            continue
        explicit_snapshot = dict(snapshot)
        explicit_snapshot.update(counts)
        if "pool_rows" in explicit_snapshot:
            explicit_snapshot["pool_rows_deprecated"] = (
                "Use market_series_rows for token-perspective series and "
                "unique_pool_count for physical pools."
            )
        result[key] = explicit_snapshot
    return result


def decimal_adjust(raw_amount: str | int, decimals: int) -> Decimal:
    """Convert an integer base-unit amount using an explicit decimals value."""
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 255:
        raise ValueError("decimals must be an integer between 0 and 255")
    try:
        amount = Decimal(str(raw_amount))
    except InvalidOperation as error:
        raise ValueError("raw_amount must be numeric") from error
    if amount != amount.to_integral_value():
        raise ValueError("raw_amount must be an integer base-unit value")
    return amount.scaleb(-decimals)


def absolute_price_spread(
    price_a: float | int | str | None,
    price_b: float | int | str | None,
) -> tuple[float | None, float | None]:
    """Return absolute USD spread and midpoint-relative basis points."""
    if price_a is None or price_b is None:
        return None, None
    try:
        a = Decimal(str(price_a))
        b = Decimal(str(price_b))
    except InvalidOperation:
        return None, None
    if not a.is_finite() or not b.is_finite() or a <= 0 or b <= 0:
        return None, None
    spread = abs(a - b)
    midpoint = (a + b) / Decimal(2)
    return float(spread), float(spread / midpoint * Decimal(10_000))


def cex_market_id(exchange: str, instrument: str) -> str:
    return f"cex:{exchange}:{instrument}"


def dex_pool_id(chain: str, dex: str, pool_address: str) -> str:
    """Return the stable identity of one on-chain liquidity pool."""
    address = pool_address.strip()
    if address.startswith("0x"):
        address = address.lower()
    return (
        f"dex:{chain.strip().lower()}:{dex.strip().lower()}:"
        f"{address}"
    )


def dex_market_id(
    chain: str,
    dex: str,
    pool_address: str,
    token_symbol: str,
) -> str:
    """Identify one token-price series observed from a DEX pool.

    A pool can appear from both token perspectives, so the pool address alone is
    not a globally unique identifier for the cataloged price series.
    """
    return f"{dex_pool_id(chain, dex, pool_address)}:{token_symbol.upper()}"


def source_quote_asset(instrument: str) -> str | None:
    """Return the displayed source quote label without claiming raw venue parity."""
    if "/" not in instrument:
        return None
    value = instrument.rsplit("/", 1)[1].strip()
    return value or None


def catalog_contract() -> dict[str, Any]:
    return {
        "catalog_version": CATALOG_VERSION,
        "time_grain": DAILY_GRAIN,
        "price_quote_asset": PRICE_QUOTE_ASSET,
        "volume_quote_asset": "USD",
        "price_field": "daily close",
        "missing_value_rule": MISSING_VALUE_RULE,
        "comparison_formula": {
            "absolute_spread_usd": "abs(price_a_usd - price_b_usd)",
            "spread_bps": (
                "abs(price_a_usd - price_b_usd) / "
                "((price_a_usd + price_b_usd) / 2) * 10000"
            ),
        },
        "market_id_semantics": (
            "CEX IDs identify one venue instrument. DEX market IDs identify one "
            "token-price series within a stable pool_id, because one pool can be "
            "observed from either token perspective."
        ),
        "semantic_boundary": (
            "Daily OHLCV fields are not order-book depth, quoted bid/ask spread, "
            "executable price, or measured slippage. CEX order-book depth and DEX "
            "pool-state depth appear only when separate point-in-time snapshots exist."
        ),
        "series_statistics": {
            "window_return": "first-to-last observed close in the selected window",
            "daily_volatility": SERIES_STATISTICS_METHOD,
            "coverage_ratio": (
                "finite positive close observations / requested UTC calendar days"
            ),
            "no_fill": True,
        },
        "primary_selection": {
            "method": PRIMARY_SELECTION_METHOD,
            "weights": dict(PRIMARY_SELECTION_WEIGHTS),
        },
        "market_quality_thresholds": dict(MARKET_QUALITY_THRESHOLDS),
    }


def catalog_from_market_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a stable, source-described catalog from a full-window market payload."""
    markets: list[dict[str, Any]] = []
    for row in payload["cex_markets"]:
        markets.append(
            {
                "market_id": cex_market_id(row["venue"], row["instrument"]),
                "token_symbol": row["token_symbol"],
                "market_type": "cex",
                "venue": row["venue"],
                "instrument": row["instrument"],
                "exchange": row["venue"],
                "chain": None,
                "pool_address": None,
                "price_field": "close",
                "volume_field": "quote_volume_usd",
                "price_quote_asset": PRICE_QUOTE_ASSET,
                "source_quote_asset_label": source_quote_asset(row["instrument"]),
                "source": f"{row['venue']} public daily OHLCV API",
                "depth_status": row.get("depth_status"),
                "depth_observed_at": row.get("depth_observed_at"),
                "depth_method": row.get("depth_method"),
                "depth_snapshot_id": row.get("depth_snapshot_id"),
                "depth_source": row.get("depth_source"),
                "depth_source_endpoint": row.get("depth_source_endpoint"),
                "depth_raw_response_sha256": row.get(
                    "depth_raw_response_sha256"
                ),
                "depth_error": row.get("depth_error"),
                "depth_source_instrument": row.get("depth_source_instrument"),
                "depth_source_quote_asset": row.get("depth_source_quote_asset"),
                "depth_quote_conversion_method": row.get(
                    "depth_quote_conversion_method"
                ),
                "best_bid": row.get("best_bid"),
                "best_ask": row.get("best_ask"),
                "quoted_spread": row.get("spread_quote"),
                "quoted_spread_bps": row.get("spread_bps"),
                "bid_depth_10bps_usd": row.get("bid_depth_10bps_usd"),
                "ask_depth_10bps_usd": row.get("ask_depth_10bps_usd"),
                "total_depth_10bps_usd": row.get("total_depth_10bps_usd"),
                "bid_depth_25bps_usd": row.get("bid_depth_25bps_usd"),
                "ask_depth_25bps_usd": row.get("ask_depth_25bps_usd"),
                "total_depth_25bps_usd": row.get("total_depth_25bps_usd"),
                "bid_depth_50bps_usd": row.get("bid_depth_50bps_usd"),
                "ask_depth_50bps_usd": row.get("ask_depth_50bps_usd"),
                "total_depth_50bps_usd": row.get("total_depth_50bps_usd"),
                "bid_depth_100bps_usd": row.get("bid_depth_100bps_usd"),
                "ask_depth_100bps_usd": row.get("ask_depth_100bps_usd"),
                "total_depth_100bps_usd": row.get("total_depth_100bps_usd"),
                "depth_10bps_complete": row.get("depth_10bps_complete", False),
                "depth_25bps_complete": row.get("depth_25bps_complete", False),
                "depth_50bps_complete": row.get("depth_50bps_complete", False),
                "depth_100bps_complete": row.get("depth_100bps_complete", False),
                "observed_start": row.get(
                    "first_observed_date",
                    row["price_points"][0]["date"] if row["price_points"] else None,
                ),
                "observed_end": row.get(
                    "latest_observed_date",
                    row.get("latest_date"),
                ),
                "observation_days": row.get(
                    "observation_count",
                    row.get("observation_days"),
                ),
                "calendar_span_days": row.get("calendar_span_days"),
                "requested_window_days": row.get("requested_window_days"),
                "coverage_ratio": row.get("coverage_ratio"),
                "missing_calendar_days": row.get("missing_calendar_days"),
                "return_interval_count": row.get("return_interval_count"),
                "skipped_gap_interval_count": row.get(
                    "skipped_gap_interval_count"
                ),
                "max_gap_days": row.get("max_gap_days"),
                "window_return_method": row.get("window_return_method"),
                "daily_volatility_method": row.get(
                    "daily_volatility_method"
                ),
                **market_quality_assessment(row),
            }
        )
    for row in payload["dex_pools"]:
        markets.append(
            {
                "market_id": dex_market_id(
                    row["venue"].split(" / ", 1)[0],
                    row["venue"].split(" / ", 1)[1],
                    row["pool_address"],
                    row["token_symbol"],
                ),
                "pool_id": dex_pool_id(
                    row["venue"].split(" / ", 1)[0],
                    row["venue"].split(" / ", 1)[1],
                    row["pool_address"],
                ),
                "token_symbol": row["token_symbol"],
                "market_type": "dex",
                "venue": row["venue"],
                "instrument": row["instrument"],
                "exchange": None,
                "chain": row["venue"].split(" / ", 1)[0],
                "pool_address": row["pool_address"],
                "price_field": "close",
                "volume_field": "dex_volume_usd",
                "price_quote_asset": PRICE_QUOTE_ASSET,
                "source_quote_asset_label": "USD (GeckoTerminal currency=usd)",
                "source": "GeckoTerminal API v2 daily pool OHLCV",
                "tvl_usd": row.get("tvl_usd"),
                "tvl_status": row.get("tvl_status"),
                "tvl_observed_at": row.get("tvl_observed_at"),
                "tvl_method": row.get("tvl_method"),
                "tvl_snapshot_id": row.get("tvl_snapshot_id"),
                "tvl_source": row.get("tvl_source"),
                "tvl_source_endpoint": row.get("tvl_source_endpoint"),
                "tvl_raw_response_sha256": row.get(
                    "tvl_raw_response_sha256"
                ),
                "tvl_error": row.get("tvl_error"),
                "depth_status": row.get("dex_depth_status"),
                "depth_observed_at": row.get("dex_depth_observed_at"),
                "depth_method": row.get("dex_depth_method"),
                "depth_snapshot_id": row.get("dex_depth_snapshot_id"),
                "depth_source": row.get("dex_depth_source"),
                "depth_source_endpoint": row.get(
                    "dex_depth_source_endpoint"
                ),
                "depth_raw_response_sha256": row.get(
                    "dex_depth_raw_response_sha256"
                ),
                "depth_error": row.get("dex_depth_error"),
                "depth_protocol_model": row.get("dex_depth_protocol_model"),
                "depth_block_number": row.get("dex_depth_block_number"),
                "depth_block_timestamp": row.get(
                    "dex_depth_block_timestamp"
                ),
                "depth_source_status": row.get("dex_depth_source_status"),
                "depth_usd_price_source_snapshot_id": row.get(
                    "dex_depth_usd_price_source_snapshot_id"
                ),
                "depth_usd_price_observed_at": row.get(
                    "dex_depth_usd_price_observed_at"
                ),
                "depth_usd_price_skew_seconds": row.get(
                    "dex_depth_usd_price_skew_seconds"
                ),
                "depth_usd_price_freshness_status": row.get(
                    "dex_depth_usd_price_freshness_status"
                ),
                "fee_bps": row.get("fee_bps"),
                "pool_state_price_usd": row.get("pool_state_price_usd"),
                "source_target_price_usd": row.get("source_target_price_usd"),
                "price_difference_bps": row.get("price_difference_bps"),
                "sell_depth_10bps_usd": row.get("sell_depth_10bps_usd"),
                "buy_depth_10bps_usd": row.get("buy_depth_10bps_usd"),
                "total_depth_10bps_usd": row.get("total_depth_10bps_usd"),
                "sell_depth_25bps_usd": row.get("sell_depth_25bps_usd"),
                "buy_depth_25bps_usd": row.get("buy_depth_25bps_usd"),
                "total_depth_25bps_usd": row.get("total_depth_25bps_usd"),
                "sell_depth_50bps_usd": row.get("sell_depth_50bps_usd"),
                "buy_depth_50bps_usd": row.get("buy_depth_50bps_usd"),
                "total_depth_50bps_usd": row.get("total_depth_50bps_usd"),
                "sell_depth_100bps_usd": row.get("sell_depth_100bps_usd"),
                "buy_depth_100bps_usd": row.get("buy_depth_100bps_usd"),
                "total_depth_100bps_usd": row.get("total_depth_100bps_usd"),
                "depth_10bps_complete": row.get("depth_10bps_complete", False),
                "depth_25bps_complete": row.get("depth_25bps_complete", False),
                "depth_50bps_complete": row.get("depth_50bps_complete", False),
                "depth_100bps_complete": row.get("depth_100bps_complete", False),
                "observed_start": row.get(
                    "first_observed_date",
                    row["price_points"][0]["date"] if row["price_points"] else None,
                ),
                "observed_end": row.get(
                    "latest_observed_date",
                    row.get("latest_date"),
                ),
                "observation_days": row.get(
                    "observation_count",
                    row.get("observation_days"),
                ),
                "calendar_span_days": row.get("calendar_span_days"),
                "requested_window_days": row.get("requested_window_days"),
                "coverage_ratio": row.get("coverage_ratio"),
                "missing_calendar_days": row.get("missing_calendar_days"),
                "return_interval_count": row.get("return_interval_count"),
                "skipped_gap_interval_count": row.get(
                    "skipped_gap_interval_count"
                ),
                "max_gap_days": row.get("max_gap_days"),
                "window_return_method": row.get("window_return_method"),
                "daily_volatility_method": row.get(
                    "daily_volatility_method"
                ),
                **market_quality_assessment(row),
            }
        )
    market_id_counts: dict[str, int] = {}
    for market in markets:
        market_id = market["market_id"]
        market_id_counts[market_id] = market_id_counts.get(market_id, 0) + 1
    duplicate_ids = sorted(
        market_id
        for market_id, count in market_id_counts.items()
        if count > 1
    )
    if duplicate_ids:
        raise ValueError(
            "Catalog market_id values must be globally unique: "
            + ", ".join(duplicate_ids)
        )

    metadata = {
        **catalog_contract(),
        "available_start": payload["metadata"]["available_start"],
        "available_end": payload["metadata"]["available_end"],
        "source_date_ranges": payload["metadata"].get("source_date_ranges", {}),
        "freshness": payload["metadata"].get("freshness"),
        "sources": payload["metadata"]["sources"],
        "storage": payload["metadata"]["storage"],
        "tvl_snapshot": payload["metadata"].get("tvl_snapshot"),
        "cex_depth_snapshot": payload["metadata"].get("cex_depth_snapshot"),
        "dex_depth_snapshot": payload["metadata"].get("dex_depth_snapshot"),
        "cex_normalization_note": (
            "The displayed instrument is the configured canonical pair label. "
            "Adapters normalize source prices and volume to USD; USDT pairs use a "
            "1 USDT = 1 USD proxy, and venue-native raw pair labels may differ."
        ),
        "cex_depth_note": payload["metadata"].get("cex_depth_note"),
        "dex_depth_note": payload["metadata"].get("dex_depth_note"),
    }
    metadata = attach_explicit_dex_counts(metadata, payload["dex_pools"])
    return {
        "metadata": metadata,
        "tokens": sorted({market["token_symbol"] for market in markets}),
        "markets": sorted(
            markets,
            key=lambda market: (
                market["token_symbol"],
                market["market_type"],
                market["venue"],
                market["instrument"],
            ),
        ),
    }


def compare_daily_rows(
    rows_a: list[dict[str, Any]],
    rows_b: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Align two raw daily fact series without filling missing observations."""
    by_date_a = {row["date"]: row for row in rows_a}
    by_date_b = {row["date"]: row for row in rows_b}
    observations = []
    for day in sorted(set(by_date_a) | set(by_date_b)):
        row_a = by_date_a.get(day)
        row_b = by_date_b.get(day)
        price_a = row_a.get("price_usd") if row_a else None
        price_b = row_b.get("price_usd") if row_b else None
        absolute_spread, spread_bps = absolute_price_spread(price_a, price_b)
        if row_a is None and row_b is None:
            missing_reason = "both_missing"
        elif row_a is None:
            missing_reason = "market_a_missing"
        elif row_b is None:
            missing_reason = "market_b_missing"
        elif absolute_spread is None:
            missing_reason = "non_comparable_price"
        else:
            missing_reason = None
        observations.append(
            {
                "date": day,
                "market_a": {
                    "price_usd": price_a,
                    "volume_usd": row_a.get("volume_usd") if row_a else None,
                },
                "market_b": {
                    "price_usd": price_b,
                    "volume_usd": row_b.get("volume_usd") if row_b else None,
                },
                "absolute_spread_usd": absolute_spread,
                "spread_bps": spread_bps,
                "missing_reason": missing_reason,
            }
        )
    return observations
