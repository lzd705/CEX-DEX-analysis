"""Pure source-freshness calculations shared by the API and collection runner."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any


DAILY_MAX_LAG_DAYS = 1
TVL_MAX_AGE_HOURS = 26.0
CEX_DEPTH_MAX_AGE_HOURS = 2.0
DEX_DEPTH_MAX_AGE_HOURS = 2.0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
    age_hours = max(0.0, (checked_at - observed).total_seconds() / 3600)
    return {
        "source": source,
        "status": "stale" if age_hours > max_age_hours else "current",
        "observed_at": observed.isoformat(),
        "age_hours": round(age_hours, 3),
        "max_age_hours": max_age_hours,
    }


def build_source_freshness(
    source_date_ranges: dict[str, dict[str, str] | None],
    *,
    tvl_observed_at: str | None = None,
    depth_observed_at: str | None = None,
    dex_depth_observed_at: str | None = None,
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
    daily_ends = [
        item["available_end"]
        for item in (cex_daily, dex_daily)
        if item["available_end"]
    ]
    statuses = [
        item["status"]
        for item in (cex_daily, dex_daily, tvl, depth, dex_depth)
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
    }
