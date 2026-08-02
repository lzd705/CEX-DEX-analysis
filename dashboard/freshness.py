"""Pure source-freshness calculations shared by the API and collection runner."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

try:
    from scripts.timestamp_contract import parse_rfc3339_utc
except ModuleNotFoundError:  # pragma: no cover - direct package execution
    from timestamp_contract import parse_rfc3339_utc


DAILY_MAX_LAG_DAYS = 1
TVL_MAX_AGE_HOURS = 26.0
CEX_DEPTH_MAX_AGE_HOURS = 2.0
DEX_DEPTH_MAX_AGE_HOURS = 2.0
CEX_EXECUTION_MAX_AGE_HOURS = 2.0
DEX_EXECUTION_MAX_AGE_HOURS = 2.0
MAX_FUTURE_CLOCK_SKEW_MINUTES = 5
ROUTE_OPPORTUNITY_MAX_AGE_SECONDS = 120.0
ROUTE_OPPORTUNITY_MAX_SKEW_SECONDS = 60.0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return parse_rfc3339_utc(value.strip())


def daily_freshness(
    source: str,
    bounds: dict[str, str] | None,
    *,
    now: datetime | None = None,
    max_lag_days: int = DAILY_MAX_LAG_DAYS,
) -> dict[str, Any]:
    checked_at = (now or utc_now()).astimezone(timezone.utc)
    if not bounds or not bounds.get("available_end"):
        return {
            "source": source,
            "status": "unavailable",
            "available_start": bounds.get("available_start") if bounds else None,
            "available_end": None,
            "latest_completed_utc_day": (
                checked_at.date() - timedelta(days=1)
            ).isoformat(),
            "lag_days": None,
            "max_lag_days": max_lag_days,
        }
    available_end = date.fromisoformat(bounds["available_end"])
    latest_completed = checked_at.date() - timedelta(days=1)
    lag_days = max(0, (latest_completed - available_end).days)
    return {
        "source": source,
        "status": "stale" if lag_days > max_lag_days else "current",
        "available_start": bounds.get("available_start"),
        "available_end": available_end.isoformat(),
        "latest_completed_utc_day": latest_completed.isoformat(),
        "lag_days": lag_days,
        "max_lag_days": max_lag_days,
    }


def snapshot_freshness(
    source: str,
    observed_at: str | None,
    *,
    now: datetime | None = None,
    max_age_hours: float,
) -> dict[str, Any]:
    checked_at = (now or utc_now()).astimezone(timezone.utc)
    observed = parse_utc_timestamp(observed_at)
    if observed is None:
        return {
            "source": source,
            "status": "unavailable",
            "observed_at": None,
            "age_hours": None,
            "max_age_hours": max_age_hours,
        }
    if observed > checked_at + timedelta(
        minutes=MAX_FUTURE_CLOCK_SKEW_MINUTES
    ):
        raise ValueError(
            "{} observed_at exceeds the allowed future clock skew".format(
                source
            )
        )
    age_hours = max(0.0, (checked_at - observed).total_seconds() / 3600)
    return {
        "source": source,
        "status": "stale" if age_hours > max_age_hours else "current",
        "observed_at": observed.isoformat(),
        "age_hours": round(age_hours, 3),
        "max_age_hours": max_age_hours,
    }


def route_opportunity_freshness(
    buy_observed_at: str | None,
    sell_observed_at: str | None,
    *,
    now: datetime | None = None,
    max_age_seconds: float = ROUTE_OPPORTUNITY_MAX_AGE_SECONDS,
    max_skew_seconds: float = ROUTE_OPPORTUNITY_MAX_SKEW_SECONDS,
) -> dict[str, Any]:
    """Evaluate synchronized route evidence with second-level strict gates."""

    checked_at = (now or utc_now()).astimezone(timezone.utc)
    buy = parse_utc_timestamp(buy_observed_at)
    sell = parse_utc_timestamp(sell_observed_at)
    base = {
        "source": "route_opportunities",
        "checked_at": checked_at.isoformat(),
        "buy_observed_at": buy.isoformat() if buy is not None else None,
        "sell_observed_at": sell.isoformat() if sell is not None else None,
        "observed_at": None,
        "age_seconds": None,
        "skew_seconds": None,
        "max_age_seconds": max_age_seconds,
        "max_skew_seconds": max_skew_seconds,
    }
    if buy is None or sell is None:
        return {
            **base,
            "status": "unavailable",
            "reason": "route_timestamp_absent",
        }
    if buy > checked_at or sell > checked_at:
        raise ValueError("route opportunity timestamp is in the future")
    latest = max(buy, sell)
    age_seconds = (checked_at - latest).total_seconds()
    skew_seconds = abs((buy - sell).total_seconds())
    result = {
        **base,
        "observed_at": latest.isoformat(),
        "age_seconds": round(age_seconds, 6),
        "skew_seconds": round(skew_seconds, 6),
    }
    if skew_seconds > max_skew_seconds:
        return {
            **result,
            "status": "unavailable",
            "reason": "snapshot_skew_exceeded",
        }
    if age_seconds > max_age_seconds:
        return {
            **result,
            "status": "stale",
            "reason": "cohort_stale",
        }
    return {**result, "status": "current", "reason": None}


def build_source_freshness(
    source_date_ranges: dict[str, dict[str, str] | None],
    *,
    tvl_observed_at: str | None = None,
    depth_observed_at: str | None = None,
    dex_depth_observed_at: str | None = None,
    cex_execution_observed_at: str | None = None,
    dex_execution_observed_at: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    checked_at = (now or utc_now()).astimezone(timezone.utc)
    cex_daily = daily_freshness(
        "cex_daily",
        source_date_ranges.get("cex_daily"),
        now=checked_at,
    )
    dex_daily = daily_freshness(
        "dex_daily",
        source_date_ranges.get("dex_daily"),
        now=checked_at,
    )
    tvl = snapshot_freshness(
        "dex_tvl",
        tvl_observed_at,
        now=checked_at,
        max_age_hours=TVL_MAX_AGE_HOURS,
    )
    depth = snapshot_freshness(
        "cex_depth",
        depth_observed_at,
        now=checked_at,
        max_age_hours=CEX_DEPTH_MAX_AGE_HOURS,
    )
    dex_depth = snapshot_freshness(
        "dex_depth",
        dex_depth_observed_at,
        now=checked_at,
        max_age_hours=DEX_DEPTH_MAX_AGE_HOURS,
    )
    cex_execution = snapshot_freshness(
        "cex_execution",
        cex_execution_observed_at,
        now=checked_at,
        max_age_hours=CEX_EXECUTION_MAX_AGE_HOURS,
    )
    dex_execution = snapshot_freshness(
        "dex_execution",
        dex_execution_observed_at,
        now=checked_at,
        max_age_hours=DEX_EXECUTION_MAX_AGE_HOURS,
    )
    daily_ends = [
        item["available_end"]
        for item in (cex_daily, dex_daily)
        if item["available_end"]
    ]
    statuses = [
        item["status"]
        for item in (
            cex_daily,
            dex_daily,
            tvl,
            depth,
            dex_depth,
            cex_execution,
            dex_execution,
        )
    ]
    if "stale" in statuses:
        overall_status = "stale"
    elif "unavailable" in statuses:
        overall_status = "partial"
    else:
        overall_status = "current"
    return {
        "checked_at": checked_at.isoformat(),
        "overall_status": overall_status,
        "common_comparable_end": min(daily_ends) if len(daily_ends) == 2 else None,
        "cex_daily": cex_daily,
        "dex_daily": dex_daily,
        "dex_tvl": tvl,
        "cex_depth": depth,
        "dex_depth": dex_depth,
        "cex_execution": cex_execution,
        "dex_execution": dex_execution,
    }
