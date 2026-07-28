"""Serve the fact-only CEX/DEX Market Monitor."""

from __future__ import annotations

import argparse
import copy
import csv
import gzip
import hashlib
import hmac
import ipaddress
import json
import math
import os
import posixpath
import re
import sqlite3
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, unquote, urlparse

try:
    from dashboard.admin import AdminService
    from dashboard.freshness import build_source_freshness
    from dashboard.market_facts import (
        attach_explicit_dex_counts,
        build_token_summaries as build_fact_token_summaries,
        catalog_contract,
        catalog_from_market_payload,
        compare_daily_rows,
        enrich_market_quality,
        market_series_statistics,
    )
except ModuleNotFoundError:
    from admin import AdminService
    from freshness import build_source_freshness
    from market_facts import (
        attach_explicit_dex_counts,
        build_token_summaries as build_fact_token_summaries,
        catalog_contract,
        catalog_from_market_payload,
        compare_daily_rows,
        enrich_market_quality,
        market_series_statistics,
    )

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.execution_cost import (
    EXECUTION_COST_COLUMNS,
    EXECUTION_COST_CONTRACT_VERSION,
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    NOTIONAL_DEFINITION,
    execution_api_rows,
    validate_execution_snapshot,
)

DASHBOARD_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = DASHBOARD_ROOT / "static"
DEFAULT_DATA_DIRS = [
    PROJECT_ROOT / "data/local",
    PROJECT_ROOT / "data/processed",
    PROJECT_ROOT / "data/public/facts",
]
CEX_FILENAME = "cex_exchange_volume_daily.csv"
DEX_FILENAME = "dex_pool_volume_daily.csv"
DATABASE_FILENAME = "market_facts.sqlite3"
TVL_FILENAME = "dex_pool_tvl_latest.csv"
CEX_DEPTH_FILENAME = "cex_depth_latest.csv"
DEX_DEPTH_FILENAME = "dex_depth_latest.csv"
CEX_EXECUTION_COST_FILENAME = "cex_execution_cost_latest.csv"
DEX_EXECUTION_COST_FILENAME = "dex_execution_cost_latest.csv"
VENDOR_FILES = {
    "/vendor/lucide.js": STATIC_ROOT / "vendor/lucide.min.js",
}
API_FRESHNESS_CACHE_SECONDS = 60
LARGE_PAYLOAD_CACHE_SIZE = 8
SERIALIZED_RESPONSE_CACHE_SIZE = 32
PUBLIC_API_CACHE_LOCK = threading.RLock()
SOURCE_CACHE_GENERATION_LOCK = threading.RLock()
_SOURCE_CACHE_GENERATION: tuple[tuple[str, int, int], ...] | None = None
_PUBLIC_RESPONSE_CACHE_GENERATION: (
    tuple[tuple[tuple[str, int, int], ...], int] | None
) = None
PUBLIC_API_QUERY_FIELDS = {
    "catalog": (),
    "market": ("start", "end"),
    "compare": ("token", "market_a", "market_b", "start", "end"),
    "execution_cost": ("token", "market_a", "market_b"),
    "quality": ("token", "scope", "market_a", "market_b"),
}
ADMIN_STATIC_PATHS = {"/admin.html", "/admin.js"}
SPA_TOKEN_PAGES = {"markets", "compare", "liquidity", "quality"}
SPA_TOKEN_ROUTE = re.compile(
    r"/tokens/[A-Za-z0-9][A-Za-z0-9._-]*/"
    r"(?:markets|compare|liquidity|quality)/?"
)
SPA_METHODOLOGY_ROUTE = re.compile(
    r"/methodology/[a-z0-9]+(?:-[a-z0-9]+)*/?"
)


def load_local_environment(path: Path) -> None:
    """Load simple KEY=VALUE entries without executing the local env file."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_local_environment(PROJECT_ROOT / ".env")
ADMIN_SERVICE = AdminService()


def parse_number(value: str | float | int | None) -> float | None:
    """Parse a finite number while preserving missing values as null."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_iso_date(value: str | None) -> str | None:
    """Validate an optional ISO date and return its normalized form."""
    if not value:
        return None
    return date.fromisoformat(value).isoformat()


def resolve_data_paths() -> tuple[Path, Path]:
    """Resolve detailed CEX and DEX facts without relying on the current cwd."""
    explicit_cex = os.environ.get("MARKET_CEX_DATA")
    explicit_dex = os.environ.get("MARKET_DEX_DATA")
    if explicit_cex or explicit_dex:
        if not explicit_cex or not explicit_dex:
            raise FileNotFoundError("MARKET_CEX_DATA and MARKET_DEX_DATA must be set together")
        cex_path = Path(explicit_cex).expanduser().resolve()
        dex_path = Path(explicit_dex).expanduser().resolve()
        if not cex_path.exists() or not dex_path.exists():
            raise FileNotFoundError("Configured CEX or DEX data file does not exist")
        return cex_path, dex_path

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        cex_path = data_dir / CEX_FILENAME
        dex_path = data_dir / DEX_FILENAME
        if cex_path.exists() and dex_path.exists():
            return cex_path, dex_path
    checked = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"No detailed market snapshot found. Checked: {checked}")


def resolve_database_path() -> Path | None:
    """Prefer the published SQLite snapshot unless explicit CSV files are configured."""
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None

    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        database_path = Path(explicit_database).expanduser().resolve()
        if not database_path.exists():
            raise FileNotFoundError(f"Configured market database does not exist: {database_path}")
        return database_path

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        database_path = data_dir / DATABASE_FILENAME
        if database_path.exists():
            return database_path
    return None


def resolve_tvl_path() -> Path | None:
    """Resolve an optional point-in-time TVL snapshot independently of OHLCV."""
    explicit_tvl = os.environ.get("MARKET_TVL_DATA")
    if explicit_tvl:
        tvl_path = Path(explicit_tvl).expanduser().resolve()
        if not tvl_path.exists():
            raise FileNotFoundError(f"Configured TVL snapshot does not exist: {tvl_path}")
        return tvl_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / TVL_FILENAME
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        tvl_path = data_dir / TVL_FILENAME
        if tvl_path.exists():
            return tvl_path
    return None


def resolve_cex_depth_path() -> Path | None:
    """Resolve an optional point-in-time CEX depth snapshot."""
    explicit_depth = os.environ.get("MARKET_CEX_DEPTH_DATA")
    if explicit_depth:
        depth_path = Path(explicit_depth).expanduser().resolve()
        if not depth_path.exists():
            raise FileNotFoundError(f"Configured CEX depth snapshot does not exist: {depth_path}")
        return depth_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / CEX_DEPTH_FILENAME
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        depth_path = data_dir / CEX_DEPTH_FILENAME
        if depth_path.exists():
            return depth_path
    return None


def resolve_dex_depth_path() -> Path | None:
    """Resolve an optional point-in-time DEX pool-state depth snapshot."""
    explicit_depth = os.environ.get("MARKET_DEX_DEPTH_DATA")
    if explicit_depth:
        depth_path = Path(explicit_depth).expanduser().resolve()
        if not depth_path.exists():
            raise FileNotFoundError(
                f"Configured DEX depth snapshot does not exist: {depth_path}"
            )
        return depth_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = (
            Path(explicit_database).expanduser().resolve().parent
            / DEX_DEPTH_FILENAME
        )
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = (
        [Path(configured_dir).expanduser().resolve()]
        if configured_dir
        else DEFAULT_DATA_DIRS
    )
    for data_dir in candidates:
        depth_path = data_dir / DEX_DEPTH_FILENAME
        if depth_path.exists():
            return depth_path
    return None


def resolve_execution_cost_path(
    filename: str,
    environment_key: str,
) -> Path | None:
    """Resolve an optional long-form execution-cost snapshot."""
    explicit = os.environ.get(environment_key)
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(
                f"Configured execution-cost snapshot does not exist: {path}"
            )
        return path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / filename
        return sibling if sibling.exists() else None
    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = (
        [Path(configured_dir).expanduser().resolve()]
        if configured_dir
        else DEFAULT_DATA_DIRS
    )
    for data_dir in candidates:
        path = data_dir / filename
        if path.exists():
            return path
    return None


def resolve_cex_execution_cost_path() -> Path | None:
    return resolve_execution_cost_path(
        CEX_EXECUTION_COST_FILENAME,
        "MARKET_CEX_EXECUTION_COST_DATA",
    )


def resolve_dex_execution_cost_path() -> Path | None:
    return resolve_execution_cost_path(
        DEX_EXECUTION_COST_FILENAME,
        "MARKET_DEX_EXECUTION_COST_DATA",
    )


def connect_database(database_path: Path) -> sqlite3.Connection:
    """Open the published database read-only so web requests cannot mutate facts."""
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def iter_csv(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def dataset_bounds(paths: Iterable[Path]) -> tuple[str, str]:
    dates: list[str] = []
    for path in paths:
        dates.extend(row["date"] for row in iter_csv(path) if row.get("date"))
    if not dates:
        raise ValueError("Market data files contain no dated rows")
    return min(dates), max(dates)


def csv_date_bounds(path: Path) -> dict[str, str]:
    dates = [row["date"] for row in iter_csv(path) if row.get("date")]
    if not dates:
        raise ValueError(f"{path.name} contains no dated rows")
    return {
        "available_start": min(dates),
        "available_end": max(dates),
    }


def rows_in_window(path: Path, start_date: str, end_date: str) -> list[dict[str, str]]:
    return [
        row
        for row in iter_csv(path)
        if row.get("date") and start_date <= row["date"] <= end_date
    ]


def latest_non_null(rows: list[dict[str, Any]], field: str) -> float | None:
    for row in sorted(rows, key=lambda item: item["date"], reverse=True):
        value = parse_number(row.get(field))
        if value is not None:
            return value
    return None


def price_observations(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """Return sorted finite, positive daily closes."""
    observations = [
        (row["date"], parse_number(row.get("close")))
        for row in rows
        if (
            parse_number(row.get("close")) is not None
            and parse_number(row.get("close")) > 0
        )
    ]
    return sorted(observations)


def price_statistics(rows: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    """Backward-compatible tuple backed by the auditable series contract."""
    result = market_series_statistics(rows)
    return (
        result["price_usd"],
        result["window_return"],
        result["daily_volatility"],
    )


def summarize_cex(
    rows: list[dict[str, str]],
    requested_start: str | None = None,
    requested_end: str | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["token_symbol"], row["exchange"], row["cex_symbol"])].append(row)

    summaries = []
    for (token, exchange, symbol), market_rows in groups.items():
        statistics_payload = market_series_statistics(
            market_rows,
            requested_start=requested_start,
            requested_end=requested_end,
        )
        volumes = [parse_number(row.get("quote_volume_usd")) for row in market_rows]
        summaries.append(
            {
                "token_symbol": token,
                "market": "cex",
                "venue": exchange,
                "instrument": symbol,
                **statistics_payload,
                "volume_usd": sum(value for value in volumes if value is not None),
                "tvl_usd": None,
                "observation_days": statistics_payload["observation_count"],
                "latest_date": statistics_payload["latest_observed_date"],
                "price_points": [{"date": day, "price_usd": value} for day, value in price_observations(market_rows)],
            }
        )
    return sorted(summaries, key=lambda row: (row["token_symbol"], -row["volume_usd"], row["venue"]))


def summarize_dex(
    rows: list[dict[str, str]],
    requested_start: str | None = None,
    requested_end: str | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[
            (
                row["token_symbol"],
                row["chain"],
                row["dex"],
                row["pool_address"],
            )
        ].append(row)

    summaries = []
    for (token, chain, dex, address), pool_rows in groups.items():
        latest_row = max(
            pool_rows,
            key=lambda row: (row["date"], row.get("pool_name", "")),
        )
        pool_name = latest_row.get("pool_name") or address
        statistics_payload = market_series_statistics(
            pool_rows,
            requested_start=requested_start,
            requested_end=requested_end,
        )
        volumes = [parse_number(row.get("dex_volume_usd")) for row in pool_rows]
        summaries.append(
            {
                "token_symbol": token,
                "market": "dex",
                "venue": f"{chain} / {dex}",
                "instrument": pool_name,
                "pool_address": address,
                **statistics_payload,
                "volume_usd": sum(value for value in volumes if value is not None),
                "tvl_usd": latest_non_null(pool_rows, "pool_tvl_usd"),
                "observation_days": statistics_payload["observation_count"],
                "latest_date": statistics_payload["latest_observed_date"],
                "price_points": [{"date": day, "price_usd": value} for day, value in price_observations(pool_rows)],
            }
        )
    return sorted(summaries, key=lambda row: (row["token_symbol"], -row["volume_usd"], row["venue"]))


def select_primary(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        token = row["token_symbol"]
        if token not in selected or row["volume_usd"] > selected[token]["volume_usd"]:
            selected[token] = row
    return selected


def common_price_comparison(
    cex: dict[str, Any] | None,
    dex: dict[str, Any] | None,
) -> tuple[str | None, float | None, float | None, float | None]:
    """Compare prices only on the latest date observed by both selected markets."""
    if not cex or not dex:
        return None, None, None, None
    cex_prices = {point["date"]: point["price_usd"] for point in cex["price_points"]}
    dex_prices = {point["date"]: point["price_usd"] for point in dex["price_points"]}
    common_dates = sorted(set(cex_prices) & set(dex_prices))
    if not common_dates:
        return None, None, None, None
    comparison_date = common_dates[-1]
    cex_price = cex_prices[comparison_date]
    dex_price = dex_prices[comparison_date]
    spread = dex_price / cex_price - 1 if cex_price else None
    return comparison_date, cex_price, dex_price, spread


def build_token_summaries(
    cex_markets: list[dict[str, Any]],
    dex_pools: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return build_fact_token_summaries(cex_markets, dex_pools)


def file_metadata(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    stat = path.stat()
    return {
        "name": path.name,
        "bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "sha256": digest.hexdigest()[:16],
    }


def data_signature(paths: Iterable[Path]) -> tuple[tuple[str, int, int], ...]:
    """Return a cache key that changes whenever a published data file changes."""
    signature = []
    for path in paths:
        stat = path.stat()
        signature.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


@lru_cache(maxsize=8)
def _load_tvl_snapshot_cached(
    path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "token_symbol",
            "chain",
            "pool_address",
            "tvl_usd",
            "tvl_method",
            "source",
            "source_endpoint",
            "raw_response_sha256",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing TVL columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no TVL rows")

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            row["chain"].lower(),
            row["pool_address"].lower()
            if row["pool_address"].startswith("0x")
            else row["pool_address"],
        )
        if key not in latest or row["observed_at"] > latest[key]["observed_at"]:
            latest[key] = row
    snapshot_ids = sorted({row["snapshot_id"] for row in rows if row.get("snapshot_id")})
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": snapshot_ids,
        "observed_at": max(row["observed_at"] for row in rows),
        "status_counts": {
            status: sum(row.get("status") == status for row in rows)
            for status in ("observed", "missing", "not_found", "failed")
        },
    }


def overlay_tvl_snapshot(payload: dict[str, Any], tvl_path: Path | None) -> dict[str, Any]:
    """Overlay current source-reported TVL without rewriting historical OHLCV."""
    result = copy.deepcopy(payload)
    if tvl_path is None:
        for pool in result["dex_pools"]:
            pool["tvl_status"] = "legacy_ohlcv_snapshot" if pool.get("tvl_usd") is not None else "unavailable"
            pool["tvl_observed_at"] = None
            pool["tvl_method"] = "legacy_geckoterminal_reserve_in_usd"
            pool["tvl_snapshot_id"] = None
            pool["tvl_source"] = None
            pool["tvl_source_endpoint"] = None
            pool["tvl_raw_response_sha256"] = None
            pool["tvl_error"] = None
        return result

    snapshot = _load_tvl_snapshot_cached(
        str(tvl_path),
        data_signature([tvl_path]),
    )
    matched = 0
    for pool in result["dex_pools"]:
        chain, _ = pool["venue"].split(" / ", 1)
        address = pool["pool_address"]
        key = (
            pool["token_symbol"].upper(),
            chain.lower(),
            address.lower() if address.startswith("0x") else address,
        )
        tvl_row = snapshot["rows"].get(key)
        if tvl_row is None:
            pool["tvl_usd"] = None
            pool["tvl_status"] = "not_cataloged_in_snapshot"
            pool["tvl_observed_at"] = None
            pool["tvl_method"] = None
            pool["tvl_snapshot_id"] = None
            pool["tvl_source"] = None
            pool["tvl_source_endpoint"] = None
            pool["tvl_raw_response_sha256"] = None
            pool["tvl_error"] = None
            continue
        matched += 1
        pool["tvl_usd"] = (
            parse_number(tvl_row.get("tvl_usd"))
            if tvl_row.get("status") == "observed"
            else None
        )
        pool["tvl_status"] = tvl_row.get("status")
        pool["tvl_observed_at"] = tvl_row.get("observed_at") or None
        pool["tvl_method"] = tvl_row.get("tvl_method") or None
        pool["tvl_snapshot_id"] = tvl_row.get("snapshot_id") or None
        pool["tvl_source"] = tvl_row.get("source") or None
        pool["tvl_source_endpoint"] = tvl_row.get("source_endpoint") or None
        pool["tvl_raw_response_sha256"] = tvl_row.get("raw_response_sha256") or None
        pool["tvl_error"] = tvl_row.get("error") or None

    metadata = result["metadata"]
    metadata["tvl_note"] = (
        "Pool TVL is a separate point-in-time GeckoTerminal reserve_in_usd "
        "snapshot. It is not historical daily TVL and it is not market depth."
    )
    metadata["tvl_snapshot"] = {
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "pool_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "source": file_metadata(tvl_path),
        "method": "geckoterminal_reserve_in_usd",
    }
    metadata["sources"].append(file_metadata(tvl_path))
    return result


@lru_cache(maxsize=8)
def _load_cex_depth_snapshot_cached(
    path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "response_received_at",
            "token_symbol",
            "exchange",
            "cex_symbol",
            "source_instrument",
            "source_quote_asset",
            "quote_conversion_method",
            "best_bid",
            "best_ask",
            "midpoint",
            "spread_quote",
            "spread_bps",
            "bid_depth_10bps_usd",
            "ask_depth_10bps_usd",
            "total_depth_10bps_usd",
            "bid_depth_25bps_usd",
            "ask_depth_25bps_usd",
            "total_depth_25bps_usd",
            "bid_depth_50bps_usd",
            "ask_depth_50bps_usd",
            "total_depth_50bps_usd",
            "bid_depth_100bps_usd",
            "ask_depth_100bps_usd",
            "total_depth_100bps_usd",
            "depth_10bps_complete",
            "depth_25bps_complete",
            "depth_50bps_complete",
            "depth_100bps_complete",
            "depth_method",
            "source_endpoint",
            "raw_response_sha256",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing CEX depth columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no CEX depth rows")

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            row["exchange"].lower(),
            row["cex_symbol"].upper(),
        )
        if key not in latest or row["response_received_at"] > latest[key]["response_received_at"]:
            latest[key] = row
    snapshot_ids = sorted({row["snapshot_id"] for row in rows if row.get("snapshot_id")})
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": snapshot_ids,
        "observed_at": max(row["observed_at"] for row in rows),
        "status_counts": {
            status: sum(row.get("status") == status for row in rows)
            for status in ("observed", "partial", "failed")
        },
    }


def overlay_cex_depth_snapshot(
    payload: dict[str, Any],
    depth_path: Path | None,
) -> dict[str, Any]:
    """Overlay current real order-book depth without rewriting daily OHLCV."""
    result = copy.deepcopy(payload)
    if depth_path is None:
        for market in result["cex_markets"]:
            market["depth_status"] = "unavailable"
            market["depth_observed_at"] = None
            market["depth_method"] = None
            market["depth_snapshot_id"] = None
            market["depth_source"] = None
            market["depth_source_endpoint"] = None
            market["depth_raw_response_sha256"] = None
            market["depth_error"] = None
        result["metadata"]["cex_depth_note"] = (
            "CEX depth snapshot is unavailable. Daily volume is not used as a depth proxy."
        )
        return result

    snapshot = _load_cex_depth_snapshot_cached(
        str(depth_path),
        data_signature([depth_path]),
    )
    matched = 0
    numeric_fields = (
        "best_bid",
        "best_ask",
        "midpoint",
        "spread_quote",
        "spread_bps",
        "bid_depth_10bps_usd",
        "ask_depth_10bps_usd",
        "total_depth_10bps_usd",
        "bid_depth_25bps_usd",
        "ask_depth_25bps_usd",
        "total_depth_25bps_usd",
        "bid_depth_50bps_usd",
        "ask_depth_50bps_usd",
        "total_depth_50bps_usd",
        "bid_depth_100bps_usd",
        "ask_depth_100bps_usd",
        "total_depth_100bps_usd",
    )
    completeness_fields = (
        "depth_10bps_complete",
        "depth_25bps_complete",
        "depth_50bps_complete",
        "depth_100bps_complete",
    )
    for market in result["cex_markets"]:
        key = (
            market["token_symbol"].upper(),
            market["venue"].lower(),
            market["instrument"].upper(),
        )
        depth_row = snapshot["rows"].get(key)
        if depth_row is None:
            market["depth_status"] = "not_cataloged_in_snapshot"
            market["depth_observed_at"] = None
            market["depth_method"] = None
            market["depth_snapshot_id"] = None
            market["depth_source"] = None
            market["depth_source_endpoint"] = None
            market["depth_raw_response_sha256"] = None
            market["depth_error"] = None
            for field in numeric_fields:
                market[field] = None
            for field in completeness_fields:
                market[field] = False
            continue

        matched += 1
        market["depth_status"] = depth_row.get("status")
        market["depth_observed_at"] = depth_row.get("observed_at") or None
        market["depth_method"] = depth_row.get("depth_method") or None
        market["depth_snapshot_id"] = depth_row.get("snapshot_id") or None
        market["depth_source"] = depth_row.get("source") or None
        market["depth_source_instrument"] = depth_row.get("source_instrument") or None
        market["depth_source_quote_asset"] = depth_row.get("source_quote_asset") or None
        market["depth_quote_conversion_method"] = (
            depth_row.get("quote_conversion_method") or None
        )
        market["depth_source_endpoint"] = depth_row.get("source_endpoint") or None
        market["depth_raw_response_sha256"] = (
            depth_row.get("raw_response_sha256") or None
        )
        market["depth_error"] = depth_row.get("error") or None
        observed = depth_row.get("status") in {"observed", "partial"}
        for field in numeric_fields:
            market[field] = parse_number(depth_row.get(field)) if observed else None
        for field in completeness_fields:
            market[field] = observed and depth_row.get(field) == "1"

    metadata = result["metadata"]
    metadata["cex_depth_note"] = (
        "CEX depth is a separate point-in-time public spot order-book snapshot. "
        "USD values are quote notional inside symmetric midpoint bands. Partial "
        "bands are observed lower bounds, not complete depth."
    )
    metadata["cex_depth_snapshot"] = {
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "market_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "bands_bps": [10, 25, 50, 100],
        "source": file_metadata(depth_path),
        "method": "midpoint_symmetric_quote_notional",
    }
    metadata["sources"].append(file_metadata(depth_path))
    return result


@lru_cache(maxsize=8)
def _load_dex_depth_snapshot_cached(
    path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "response_received_at",
            "token_symbol",
            "chain",
            "dex",
            "pool_address",
            "protocol_model",
            "block_number",
            "fee_bps",
            "pool_state_price_usd",
            "source_target_price_usd",
            "price_difference_bps",
            "sell_depth_10bps_usd",
            "buy_depth_10bps_usd",
            "total_depth_10bps_usd",
            "sell_depth_25bps_usd",
            "buy_depth_25bps_usd",
            "total_depth_25bps_usd",
            "sell_depth_50bps_usd",
            "buy_depth_50bps_usd",
            "total_depth_50bps_usd",
            "sell_depth_100bps_usd",
            "buy_depth_100bps_usd",
            "total_depth_100bps_usd",
            "depth_10bps_complete",
            "depth_25bps_complete",
            "depth_50bps_complete",
            "depth_100bps_complete",
            "depth_method",
            "source_endpoint",
            "raw_response_sha256",
            "status",
            "error",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                f"{path.name} is missing DEX depth columns: {', '.join(missing)}"
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no DEX depth rows")

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        address = row["pool_address"]
        key = (
            row["token_symbol"].upper(),
            row["chain"].lower(),
            address.lower() if address.startswith("0x") else address,
        )
        if (
            key not in latest
            or row["response_received_at"] > latest[key]["response_received_at"]
        ):
            latest[key] = row
    snapshot_ids = sorted(
        {row["snapshot_id"] for row in rows if row.get("snapshot_id")}
    )
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": snapshot_ids,
        "observed_at": max(row["observed_at"] for row in rows),
        "status_counts": dict(
            Counter(row.get("status") or "missing_status" for row in rows)
        ),
    }


def overlay_dex_depth_snapshot(
    payload: dict[str, Any],
    depth_path: Path | None,
) -> dict[str, Any]:
    """Overlay fixed-block DEX pool-state depth without using TVL as a proxy."""
    result = copy.deepcopy(payload)
    if depth_path is None:
        for pool in result["dex_pools"]:
            pool["dex_depth_status"] = "unavailable"
            pool["dex_depth_observed_at"] = None
            pool["dex_depth_method"] = None
            pool["dex_depth_snapshot_id"] = None
            pool["dex_depth_source"] = None
            pool["dex_depth_source_endpoint"] = None
            pool["dex_depth_raw_response_sha256"] = None
            pool["dex_depth_error"] = None
        result["metadata"]["dex_depth_note"] = (
            "DEX pool-state depth snapshot is unavailable. TVL and daily volume "
            "are not used as depth proxies."
        )
        return result

    snapshot = _load_dex_depth_snapshot_cached(
        str(depth_path),
        data_signature([depth_path]),
    )
    matched = 0
    numeric_fields = [
        "fee_bps",
        "pool_state_price_usd",
        "source_target_price_usd",
        "price_difference_bps",
    ] + [
        f"{side}_depth_{band}bps_usd"
        for band in (10, 25, 50, 100)
        for side in ("sell", "buy", "total")
    ]
    completeness_fields = [
        f"depth_{band}bps_complete"
        for band in (10, 25, 50, 100)
    ]
    for pool in result["dex_pools"]:
        chain, _ = pool["venue"].split(" / ", 1)
        address = pool["pool_address"]
        key = (
            pool["token_symbol"].upper(),
            chain.lower(),
            address.lower() if address.startswith("0x") else address,
        )
        depth_row = snapshot["rows"].get(key)
        if depth_row is None:
            pool["dex_depth_status"] = "not_cataloged_in_snapshot"
            pool["dex_depth_observed_at"] = None
            pool["dex_depth_method"] = None
            pool["dex_depth_snapshot_id"] = None
            pool["dex_depth_source"] = None
            pool["dex_depth_source_endpoint"] = None
            pool["dex_depth_raw_response_sha256"] = None
            pool["dex_depth_error"] = None
            for field in numeric_fields:
                pool[field] = None
            for field in completeness_fields:
                pool[field] = False
            continue

        matched += 1
        pool["dex_depth_status"] = depth_row.get("status")
        pool["dex_depth_observed_at"] = depth_row.get("observed_at") or None
        pool["dex_depth_method"] = depth_row.get("depth_method") or None
        pool["dex_depth_snapshot_id"] = depth_row.get("snapshot_id") or None
        pool["dex_depth_source"] = depth_row.get("source") or None
        pool["dex_depth_protocol_model"] = (
            depth_row.get("protocol_model") or None
        )
        pool["dex_depth_block_number"] = (
            int(depth_row["block_number"])
            if depth_row.get("block_number")
            else None
        )
        pool["dex_depth_source_endpoint"] = (
            depth_row.get("source_endpoint") or None
        )
        pool["dex_depth_raw_response_sha256"] = (
            depth_row.get("raw_response_sha256") or None
        )
        pool["dex_depth_error"] = depth_row.get("error") or None
        measured = depth_row.get("status") in {"observed", "partial"}
        for field in numeric_fields:
            pool[field] = (
                parse_number(depth_row.get(field)) if measured else None
            )
        for field in completeness_fields:
            pool[field] = measured and depth_row.get(field) == "1"

    metadata = result["metadata"]
    metadata["dex_depth_note"] = (
        "DEX depth is measured from one fixed EVM block by integrating each "
        "supported pool's actual invariant and active tick liquidity to marginal "
        "price bands. Unsupported protocols remain null; TVL is never substituted."
    )
    metadata["dex_depth_snapshot"] = {
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "pool_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "bands_bps": [10, 25, 50, 100],
        "source": file_metadata(depth_path),
        "method": "fixed_block_pool_state_marginal_price_band",
    }
    metadata["sources"].append(file_metadata(depth_path))
    return result


def finalize_fact_contract(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach quality, aggregation, and count semantics after all snapshots."""
    cex_markets = [enrich_market_quality(row) for row in payload["cex_markets"]]
    dex_pools = [enrich_market_quality(row) for row in payload["dex_pools"]]
    metadata = {
        **catalog_contract(),
        **payload["metadata"],
    }
    metadata = attach_explicit_dex_counts(metadata, dex_pools)
    return {
        **payload,
        "metadata": metadata,
        "tokens": build_fact_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_market_payload_cached(
    start: str | None,
    end: str | None,
    cex_path_text: str,
    dex_path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    cex_path = Path(cex_path_text)
    dex_path = Path(dex_path_text)
    cex_bounds = csv_date_bounds(cex_path)
    dex_bounds = csv_date_bounds(dex_path)
    available_start = min(
        cex_bounds["available_start"],
        dex_bounds["available_start"],
    )
    available_end = max(
        cex_bounds["available_end"],
        dex_bounds["available_end"],
    )
    effective_end = parse_iso_date(end) or available_end
    default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
    effective_start = parse_iso_date(start) or max(available_start, default_start)
    if effective_start > effective_end:
        raise ValueError("start date must not be after end date")
    if effective_start < available_start or effective_end > available_end:
        raise ValueError(f"date window must be within {available_start} and {available_end}")

    cex_markets = summarize_cex(
        rows_in_window(cex_path, effective_start, effective_end),
        effective_start,
        effective_end,
    )
    dex_pools = summarize_dex(
        rows_in_window(dex_path, effective_start, effective_end),
        effective_start,
        effective_end,
    )
    if not cex_markets and not dex_pools:
        raise ValueError("No market observations exist in the selected time window")

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
            "source_date_ranges": {
                "cex_daily": cex_bounds,
                "dex_daily": dex_bounds,
            },
            "start_date": effective_start,
            "end_date": effective_end,
            "token_count": len({row["token_symbol"] for row in cex_markets + dex_pools}),
            "grain": "venue/pool summary over selected daily observations",
            "sources": [file_metadata(cex_path), file_metadata(dex_path)],
            "storage": {"engine": "csv"},
            "tvl_note": "Pool TVL is a latest-fetch snapshot, not a historical daily series.",
        },
        "tokens": build_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_database_payload_cached(
    start: str | None,
    end: str | None,
    database_path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    database_path = Path(database_path_text)
    connection = connect_database(database_path)
    try:
        state = connection.execute(
            """
            SELECT s.*, r.run_id, r.imported_at
            FROM dataset_state state
            JOIN dataset_snapshots s ON s.snapshot_id = state.snapshot_id
            JOIN import_runs r ON r.run_id = state.import_run_id
            WHERE state.singleton_id = 1
            """
        ).fetchone()
        if state is None:
            raise ValueError("Market database does not contain a published dataset state")

        available_start = state["available_start"]
        available_end = state["available_end"]
        effective_end = parse_iso_date(end) or available_end
        default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
        effective_start = parse_iso_date(start) or max(available_start, default_start)
        if effective_start > effective_end:
            raise ValueError("start date must not be after end date")
        if effective_start < available_start or effective_end > available_end:
            raise ValueError(f"date window must be within {available_start} and {available_end}")

        cex_bounds_row = connection.execute(
            "SELECT MIN(date) AS available_start, MAX(date) AS available_end "
            "FROM cex_market_daily"
        ).fetchone()
        dex_bounds_row = connection.execute(
            "SELECT MIN(date) AS available_start, MAX(date) AS available_end "
            "FROM dex_pool_daily"
        ).fetchone()
        cex_bounds = {
            "available_start": cex_bounds_row["available_start"],
            "available_end": cex_bounds_row["available_end"],
        }
        dex_bounds = {
            "available_start": dex_bounds_row["available_start"],
            "available_end": dex_bounds_row["available_end"],
        }
        cex_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT date, token_symbol, exchange, cex_symbol, open, high, low,
                       close, base_volume, quote_volume_usd
                FROM cex_market_daily
                WHERE date BETWEEN ? AND ?
                ORDER BY date, token_symbol, exchange, cex_symbol
                """,
                (effective_start, effective_end),
            )
        ]
        dex_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT date, token_symbol, chain, dex, pool_address, pool_name,
                       open, high, low, close, dex_volume_usd, pool_tvl_usd
                FROM dex_pool_daily
                WHERE date BETWEEN ? AND ?
                ORDER BY date, token_symbol, chain, dex, pool_address
                """,
                (effective_start, effective_end),
            )
        ]
    finally:
        connection.close()

    cex_markets = summarize_cex(cex_rows, effective_start, effective_end)
    dex_pools = summarize_dex(dex_rows, effective_start, effective_end)
    if not cex_markets and not dex_pools:
        raise ValueError("No market observations exist in the selected time window")

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
            "source_date_ranges": {
                "cex_daily": cex_bounds,
                "dex_daily": dex_bounds,
            },
            "start_date": effective_start,
            "end_date": effective_end,
            "token_count": len({row["token_symbol"] for row in cex_markets + dex_pools}),
            "grain": "venue/pool summary over selected daily observations",
            "sources": [
                {
                    "name": state["cex_source_name"],
                    "bytes": state["cex_source_bytes"],
                    "modified_at": state["imported_at"],
                    "sha256": state["cex_sha256"][:16],
                },
                {
                    "name": state["dex_source_name"],
                    "bytes": state["dex_source_bytes"],
                    "modified_at": state["imported_at"],
                    "sha256": state["dex_sha256"][:16],
                },
            ],
            "storage": {
                "engine": "sqlite",
                "schema_version": 1,
                "snapshot_id": state["snapshot_id"],
                "import_run_id": state["run_id"],
            },
            "tvl_note": "Pool TVL is a latest-fetch snapshot, not a historical daily series.",
        },
        "tokens": build_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


def execution_freshness_observed_at(path: Path | None) -> str | None:
    """Read the execution fact's own state time without borrowing depth time."""
    if path is None:
        return None
    try:
        snapshot = load_execution_cost_snapshot(path)
    except (OSError, ValueError):
        return None
    return snapshot.get("state_observed_at") if snapshot else None


def attach_freshness_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach dynamic freshness without mutating the cached fact payload."""
    result = {
        **payload,
        "metadata": {**payload["metadata"]},
    }
    metadata = result["metadata"]
    cex_execution_path = resolve_cex_execution_cost_path()
    dex_execution_path = resolve_dex_execution_cost_path()
    metadata["freshness"] = build_source_freshness(
        metadata.get("source_date_ranges", {}),
        tvl_observed_at=(
            metadata.get("tvl_snapshot", {}).get("observed_at")
            if metadata.get("tvl_snapshot")
            else None
        ),
        depth_observed_at=(
            metadata.get("cex_depth_snapshot", {}).get("observed_at")
            if metadata.get("cex_depth_snapshot")
            else None
        ),
        dex_depth_observed_at=(
            metadata.get("dex_depth_snapshot", {}).get("observed_at")
            if metadata.get("dex_depth_snapshot")
            else None
        ),
        cex_execution_observed_at=execution_freshness_observed_at(
            cex_execution_path
        ),
        dex_execution_observed_at=execution_freshness_observed_at(
            dex_execution_path
        ),
    )
    return result


def _optional_signature(path: Path | None) -> tuple[tuple[str, int, int], ...]:
    return data_signature([path]) if path is not None else ()


def market_payload_cache_key(
    start: str | None,
    end: str | None,
) -> tuple[Any, ...]:
    """Resolve every fact source into a hashable, invalidation-aware cache key."""
    tvl_path = resolve_tvl_path()
    depth_path = resolve_cex_depth_path()
    dex_depth_path = resolve_dex_depth_path()
    database_path = resolve_database_path()
    if database_path is not None:
        cex_path = None
        dex_path = None
        daily_signature = data_signature([database_path])
    else:
        cex_path, dex_path = resolve_data_paths()
        daily_signature = data_signature([cex_path, dex_path])
    return (
        start,
        end,
        str(database_path) if database_path is not None else "",
        str(cex_path) if cex_path is not None else "",
        str(dex_path) if dex_path is not None else "",
        daily_signature,
        str(tvl_path) if tvl_path is not None else "",
        _optional_signature(tvl_path),
        str(depth_path) if depth_path is not None else "",
        _optional_signature(depth_path),
        str(dex_depth_path) if dex_depth_path is not None else "",
        _optional_signature(dex_depth_path),
    )


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_enriched_payload_cached(cache_key: tuple[Any, ...]) -> dict[str, Any]:
    (
        start,
        end,
        database_path_text,
        cex_path_text,
        dex_path_text,
        daily_signature,
        tvl_path_text,
        _tvl_signature,
        depth_path_text,
        _depth_signature,
        dex_depth_path_text,
        _dex_depth_signature,
    ) = cache_key
    if database_path_text:
        payload = _build_database_payload_cached(
            start,
            end,
            database_path_text,
            daily_signature,
        )
    else:
        payload = _build_market_payload_cached(
            start,
            end,
            cex_path_text,
            dex_path_text,
            daily_signature,
        )
    payload = overlay_tvl_snapshot(payload, Path(tvl_path_text) if tvl_path_text else None)
    payload = overlay_cex_depth_snapshot(
        payload,
        Path(depth_path_text) if depth_path_text else None,
    )
    payload = overlay_dex_depth_snapshot(
        payload,
        Path(dex_depth_path_text) if dex_depth_path_text else None,
    )
    return finalize_fact_contract(payload)


def build_market_payload(
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    # Keep generation validation, assembly, and the lru_cache write-back in one
    # critical section. Otherwise a concurrent cache clear can finish while an
    # old miss is still computing, allowing that old key to be inserted again.
    with SOURCE_CACHE_GENERATION_LOCK:
        ensure_source_cache_generation(api_source_signature())
        cache_key = market_payload_cache_key(start, end)
        return attach_freshness_metadata(_build_enriched_payload_cached(cache_key))


@lru_cache(maxsize=8)
def _build_market_catalog_cached(cache_key: tuple[Any, ...]) -> dict[str, Any]:
    return catalog_from_market_payload(_build_enriched_payload_cached(cache_key))


def build_market_catalog() -> dict[str, Any]:
    """Return every observed market plus the versioned fact contract."""
    with SOURCE_CACHE_GENERATION_LOCK:
        default_payload = build_market_payload()
        metadata = default_payload["metadata"]
        cache_key = market_payload_cache_key(
            metadata["available_start"],
            metadata["available_end"],
        )
        catalog = _build_market_catalog_cached(cache_key)
        return {
            **catalog,
            "metadata": {
                **catalog["metadata"],
                "freshness": metadata.get("freshness"),
            },
        }


def validate_fact_window(
    start: str | None,
    end: str | None,
    available_start: str,
    available_end: str,
) -> tuple[str, str]:
    effective_end = parse_iso_date(end) or available_end
    default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
    effective_start = parse_iso_date(start) or max(available_start, default_start)
    if effective_start > effective_end:
        raise ValueError("start date must not be after end date")
    if effective_start < available_start or effective_end > available_end:
        raise ValueError(f"date window must be within {available_start} and {available_end}")
    return effective_start, effective_end


def database_market_rows(
    database_path: Path,
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    connection = connect_database(database_path)
    try:
        if market["market_type"] == "cex":
            rows = connection.execute(
                """
                SELECT date, close AS price_usd, quote_volume_usd AS volume_usd
                FROM cex_market_daily
                WHERE token_symbol = ? AND exchange = ? AND cex_symbol = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                (
                    market["token_symbol"],
                    market["exchange"],
                    market["instrument"],
                    start,
                    end,
                ),
            )
        else:
            rows = connection.execute(
                """
                SELECT date, close AS price_usd, dex_volume_usd AS volume_usd
                FROM dex_pool_daily
                WHERE token_symbol = ? AND chain = ? AND pool_address = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                (
                    market["token_symbol"],
                    market["chain"],
                    market["pool_address"],
                    start,
                    end,
                ),
            )
        return [
            {
                "date": row["date"],
                "price_usd": parse_number(row["price_usd"]),
                "volume_usd": parse_number(row["volume_usd"]),
            }
            for row in rows
        ]
    finally:
        connection.close()


def csv_market_rows(
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    cex_path, dex_path = resolve_data_paths()
    path = cex_path if market["market_type"] == "cex" else dex_path
    result = []
    for row in rows_in_window(path, start, end):
        if row.get("token_symbol") != market["token_symbol"]:
            continue
        if market["market_type"] == "cex":
            if row.get("exchange") != market["exchange"] or row.get("cex_symbol") != market["instrument"]:
                continue
            volume = row.get("quote_volume_usd")
        else:
            if row.get("chain") != market["chain"] or row.get("pool_address") != market["pool_address"]:
                continue
            volume = row.get("dex_volume_usd")
        result.append(
            {
                "date": row["date"],
                "price_usd": parse_number(row.get("close")),
                "volume_usd": parse_number(volume),
            }
        )
    return sorted(result, key=lambda row: row["date"])


def selected_market_rows(
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    database_path = resolve_database_path()
    if database_path is not None:
        return database_market_rows(database_path, market, start, end)
    return csv_market_rows(market, start, end)


def build_market_comparison(
    token_symbol: str | None,
    market_a_id: str | None,
    market_b_id: str | None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Return aligned raw daily facts for two selected markets."""
    if not token_symbol or not market_a_id or not market_b_id:
        raise ValueError("token, market_a, and market_b are required")
    token = token_symbol.upper()
    if market_a_id == market_b_id:
        raise ValueError("market_a and market_b must be different")

    catalog = build_market_catalog()
    metadata = catalog["metadata"]
    effective_start, effective_end = validate_fact_window(
        start,
        end,
        metadata["available_start"],
        metadata["available_end"],
    )
    markets = {
        market["market_id"]: market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    }
    market_a = markets.get(market_a_id)
    market_b = markets.get(market_b_id)
    if market_a is None or market_b is None:
        raise ValueError("Selected market is not cataloged for the requested token")

    rows_a = selected_market_rows(market_a, effective_start, effective_end)
    rows_b = selected_market_rows(market_b, effective_start, effective_end)
    statistics_a = market_series_statistics(
        rows_a,
        price_field="price_usd",
        requested_start=effective_start,
        requested_end=effective_end,
    )
    statistics_b = market_series_statistics(
        rows_b,
        price_field="price_usd",
        requested_start=effective_start,
        requested_end=effective_end,
    )
    observations = compare_daily_rows(rows_a, rows_b)
    comparable = [row for row in observations if row["spread_bps"] is not None]
    return {
        "metadata": {
            **catalog_contract(),
            "available_start": metadata["available_start"],
            "available_end": metadata["available_end"],
            "source_date_ranges": metadata.get("source_date_ranges", {}),
            "freshness": metadata.get("freshness"),
            "start_date": effective_start,
            "end_date": effective_end,
            "sources": metadata["sources"],
            "storage": metadata["storage"],
            "comparison_days": len(comparable),
            "union_observation_days": len(observations),
        },
        "token_symbol": token,
        "market_a": market_a,
        "market_b": market_b,
        "market_a_statistics": statistics_a,
        "market_b_statistics": statistics_b,
        "latest_comparable_observation": comparable[-1] if comparable else None,
        "observations": observations,
    }


@lru_cache(maxsize=8)
def _load_execution_cost_snapshot_cached(
    path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(EXECUTION_COST_COLUMNS) - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                f"{path.name} is missing execution-cost columns: "
                + ", ".join(missing)
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no execution-cost rows")
    market_ids = {row["market_id"] for row in rows if row.get("market_id")}
    validate_execution_snapshot(market_ids, rows)
    by_market: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_market[row["market_id"]].append(row)
    for market_rows in by_market.values():
        market_rows.sort(
            key=lambda row: (
                float(row["requested_notional_usd"]),
                EXECUTION_DIRECTIONS.index(row["direction"]),
            )
        )
    state_times = [
        row["state_observed_at"]
        for row in rows
        if row.get("state_observed_at")
    ]
    return {
        "path": path,
        "rows": rows,
        "by_market": by_market,
        "snapshot_ids": sorted(
            {row["snapshot_id"] for row in rows if row.get("snapshot_id")}
        ),
        "source_snapshot_ids": sorted(
            {
                row["source_snapshot_id"]
                for row in rows
                if row.get("source_snapshot_id")
            }
        ),
        "observed_at": max(
            (row["observed_at"] for row in rows if row.get("observed_at")),
            default=None,
        ),
        "state_observed_at": max(state_times, default=None),
        "market_count": len(by_market),
        "row_count": len(rows),
        "status_counts": dict(
            Counter(row.get("status") or "missing_status" for row in rows)
        ),
    }


def load_execution_cost_snapshot(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return _load_execution_cost_snapshot_cached(
        str(path),
        data_signature([path]),
    )


def _timestamp_seconds(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _execution_snapshot_metadata(
    snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "snapshot_ids": snapshot["snapshot_ids"],
        "source_snapshot_ids": snapshot["source_snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "state_observed_at": snapshot["state_observed_at"],
        "market_count": snapshot["market_count"],
        "row_count": snapshot["row_count"],
        "status_counts": snapshot["status_counts"],
        "source": file_metadata(snapshot["path"]),
    }


def build_execution_cost_comparison(
    token_symbol: str | None,
    market_a_id: str | None,
    market_b_id: str | None,
) -> dict[str, Any]:
    """Return source-backed fixed-notional facts for two exact catalog markets."""
    if not token_symbol or not market_a_id or not market_b_id:
        raise ValueError("token, market_a, and market_b are required")
    if market_a_id == market_b_id:
        raise ValueError("market_a and market_b must be different")
    token = token_symbol.upper()
    catalog = build_market_catalog()
    markets = {
        market["market_id"]: market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    }
    market_a = markets.get(market_a_id)
    market_b = markets.get(market_b_id)
    if market_a is None or market_b is None:
        raise ValueError("Selected market is not cataloged for the requested token")

    selected_market_types = {
        market_a["market_type"],
        market_b["market_type"],
    }
    snapshot_paths = {
        "cex": resolve_cex_execution_cost_path,
        "dex": resolve_dex_execution_cost_path,
    }
    snapshots = {
        market_type: load_execution_cost_snapshot(
            snapshot_paths[market_type]()
        )
        for market_type in selected_market_types
    }

    def market_result(market: dict[str, Any]) -> dict[str, Any]:
        snapshot = snapshots.get(market["market_type"])
        if snapshot is None:
            return {
                "market": market,
                "status": "unavailable",
                "rows": [],
            }
        rows = snapshot["by_market"].get(market["market_id"])
        if rows is None:
            return {
                "market": market,
                "status": "not_cataloged_in_snapshot",
                "rows": [],
            }
        return {
            "market": market,
            "status": "available",
            "rows": execution_api_rows(rows, number_parser=parse_number),
        }

    result_a = market_result(market_a)
    result_b = market_result(market_b)
    times = []
    for result in (result_a, result_b):
        if not result["rows"]:
            times.append(None)
            continue
        values = {
            row.get("state_observed_at")
            for row in result["rows"]
            if row.get("state_observed_at")
        }
        times.append(_timestamp_seconds(max(values)) if values else None)
    skew = (
        abs(times[0] - times[1])
        if times[0] is not None and times[1] is not None
        else None
    )
    return {
        "metadata": {
            "contract_version": EXECUTION_COST_CONTRACT_VERSION,
            "notionals_usd": [int(value) for value in EXECUTION_NOTIONALS_USD],
            "directions": list(EXECUTION_DIRECTIONS),
            "notional_definition": NOTIONAL_DEFINITION,
            "reference_prices": {
                "cex": "same-snapshot best bid/ask midpoint",
                "dex": "same-block pre-trade pre-fee marginal pool price",
            },
            "formula": {
                "sell_token": (
                    "(reference_notional_usd - quote_amount_usd) / "
                    "reference_notional_usd * 10000"
                ),
                "buy_token": (
                    "(quote_amount_usd - reference_notional_usd) / "
                    "reference_notional_usd * 10000"
                ),
            },
            "numeric_encoding": {
                "requested_notional_usd": "JSON number",
                "measured_decimal_fields": (
                    "exact base-10 JSON strings; null when unavailable"
                ),
            },
            "cost_scope": (
                "Source-mechanics quoted cost, not realized or all-in cost. "
                "CEX account taker fees are excluded. Supported DEX V2 quotes "
                "include protocol pool fees while gas, router fees, transfer "
                "taxes, and MEV are excluded. DEX V3 execution is explicitly "
                "unsupported in this release."
            ),
            "missing_value_rule": (
                "Partial, unsupported, failed, unavailable, and not-cataloged "
                "full-request cost fields remain null; they are never zero-filled "
                "or interpolated from depth bands."
            ),
            "snapshot_skew_seconds": skew,
            "snapshots": {
                market_type: _execution_snapshot_metadata(snapshot)
                for market_type, snapshot in snapshots.items()
            },
        },
        "token_symbol": token,
        "market_a": result_a,
        "market_b": result_b,
    }


QUALITY_CONTRACT_VERSION = 1
QUALITY_STATUS_SEMANTICS = {
    "observed": "A source-backed fact is present.",
    "partial": "Only part of the requested execution or depth is proved.",
    "unsupported": "No audited adapter exists for this market model.",
    "failed": "A supported collection or calculation failed.",
    "unavailable": "No current snapshot is configured or published.",
    "not_cataloged_in_snapshot": (
        "A current snapshot exists, but it contains no row for this market."
    ),
    "not_applicable": "This fact is not defined for this market type.",
}


def _quality_lineage(
    *,
    status: str,
    observed_at: str | None = None,
    source: str | None = None,
    source_endpoint: str | None = None,
    method: str | None = None,
    reason: str | None = None,
    snapshot_id: str | None = None,
    dataset_sha256: str | None = None,
    raw_response_sha256: str | None = None,
) -> dict[str, Any]:
    """Return one stable set of fields shared by every quality fact."""
    return {
        "status": status,
        "observed_at": observed_at,
        "source": source,
        "source_endpoint": source_endpoint,
        "method": method,
        "reason": reason,
        "snapshot_id": snapshot_id,
        "dataset_sha256": dataset_sha256,
        "raw_response_sha256": raw_response_sha256,
    }


def _dataset_source_for_market(
    metadata: dict[str, Any],
    market_type: str,
) -> dict[str, Any] | None:
    expected_name = CEX_FILENAME if market_type == "cex" else DEX_FILENAME
    sources = metadata.get("sources") or []
    for source in sources:
        if source.get("name") == expected_name:
            return source
    daily_sources = sources[:2]
    fallback_index = 0 if market_type == "cex" else 1
    return (
        daily_sources[fallback_index]
        if len(daily_sources) > fallback_index
        else None
    )


def _quality_flags_for_fact(
    market: dict[str, Any],
    fact: str,
) -> list[dict[str, Any]]:
    details = market.get("quality_flag_details") or []
    if fact == "daily":
        codes = {"low_daily_coverage"}
    elif fact == "tvl":
        codes = {"tiny_pool"}
    elif fact == "depth":
        codes = {
            "depth_unavailable",
            "depth_unsupported",
            "unsupported_depth",
            "depth_partial",
            "partial_depth",
            "depth_failed",
            "failed_depth",
            "depth_not_cataloged",
            "zero_depth_10bps",
            "zero_depth_inside_spread",
            "off_market_pool_state_price",
            "off_market_price",
            "wide_quoted_spread",
        }
    else:
        codes = set()
    return [
        detail
        for detail in details
        if detail.get("code") in codes
    ]


def _daily_quality_fact(
    market: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    observation_days = market.get("observation_days")
    observed = (
        isinstance(observation_days, (int, float))
        and not isinstance(observation_days, bool)
        and observation_days > 0
    )
    dataset_source = _dataset_source_for_market(
        metadata,
        market["market_type"],
    )
    return {
        **_quality_lineage(
            status="observed" if observed else "unavailable",
            observed_at=market.get("observed_end"),
            source=market.get("source"),
            method="daily_close_no_fill",
            reason=None if observed else "no_daily_observations",
            dataset_sha256=(
                dataset_source.get("sha256") if dataset_source else None
            ),
        ),
        "observed_start": market.get("observed_start"),
        "observed_end": market.get("observed_end"),
        "observation_days": observation_days,
        "requested_window_days": market.get("requested_window_days"),
        "coverage_ratio": market.get("coverage_ratio"),
        "quality_flags": _quality_flags_for_fact(market, "daily"),
    }


def _tvl_quality_fact(market: dict[str, Any]) -> dict[str, Any]:
    if market["market_type"] == "cex":
        return {
            **_quality_lineage(
                status="not_applicable",
                reason="cex_markets_do_not_have_pool_tvl",
            ),
            "value_usd": None,
            "quality_flags": [],
        }
    status = market.get("tvl_status") or "unavailable"
    return {
        **_quality_lineage(
            status=status,
            observed_at=market.get("tvl_observed_at"),
            source=market.get("tvl_source"),
            source_endpoint=market.get("tvl_source_endpoint"),
            method=market.get("tvl_method"),
            reason=market.get("tvl_error"),
            snapshot_id=market.get("tvl_snapshot_id"),
            raw_response_sha256=market.get("tvl_raw_response_sha256"),
        ),
        # An observed zero is a real value.  Do not use truthiness here.
        "value_usd": market.get("tvl_usd"),
        "quality_flags": _quality_flags_for_fact(market, "tvl"),
    }


def _depth_quality_fact(market: dict[str, Any]) -> dict[str, Any]:
    market_type = market["market_type"]
    status = market.get("depth_status") or "unavailable"
    bands = {}
    for band in (10, 25, 50, 100):
        sell_prefix = "bid" if market_type == "cex" else "sell"
        buy_prefix = "ask" if market_type == "cex" else "buy"
        bands[str(band)] = {
            "sell_token_usd": market.get(
                f"{sell_prefix}_depth_{band}bps_usd"
            ),
            "buy_token_usd": market.get(
                f"{buy_prefix}_depth_{band}bps_usd"
            ),
            "total_usd": market.get(f"total_depth_{band}bps_usd"),
            "complete": bool(market.get(f"depth_{band}bps_complete")),
        }
    return {
        **_quality_lineage(
            status=status,
            observed_at=market.get("depth_observed_at"),
            source=market.get("depth_source"),
            source_endpoint=market.get("depth_source_endpoint"),
            method=market.get("depth_method"),
            reason=market.get("depth_error"),
            snapshot_id=market.get("depth_snapshot_id"),
            raw_response_sha256=market.get(
                "depth_raw_response_sha256"
            ),
        ),
        "block_number": market.get("depth_block_number"),
        "protocol_model": market.get("depth_protocol_model"),
        "bands_bps": bands,
        "quality_flags": _quality_flags_for_fact(market, "depth"),
    }


def _execution_quality_source(
    resolver,
) -> dict[str, Any]:
    try:
        path = resolver()
    except FileNotFoundError as error:
        return {"snapshot": None, "error": str(error)}
    if path is None:
        return {"snapshot": None, "error": None}
    try:
        return {"snapshot": load_execution_cost_snapshot(path), "error": None}
    except (OSError, ValueError) as error:
        return {"snapshot": None, "error": str(error)}


def _one_execution_value(
    rows: list[dict[str, str]],
    field: str,
) -> str | None:
    values = sorted(
        {
            str(row.get(field))
            for row in rows
            if row.get(field) not in (None, "")
        }
    )
    return values[0] if len(values) == 1 else None


def _execution_quality_fact(
    market: dict[str, Any],
    source_state: dict[str, Any],
) -> dict[str, Any]:
    snapshot = source_state["snapshot"]
    load_error = source_state["error"]
    if load_error is not None:
        return {
            **_quality_lineage(
                status="failed",
                reason=f"execution_snapshot_invalid: {load_error}",
            ),
            "published_at": None,
            "status_counts": {"failed": 1},
            "status_reason_counts": {
                "execution_snapshot_invalid": 1,
            },
            "scenario_count": 0,
        }
    if snapshot is None:
        return {
            **_quality_lineage(
                status="unavailable",
                reason="execution_snapshot_unavailable",
            ),
            "published_at": None,
            "status_counts": {},
            "status_reason_counts": {},
            "scenario_count": 0,
        }
    rows = snapshot["by_market"].get(market["market_id"])
    if rows is None:
        return {
            **_quality_lineage(
                status="not_cataloged_in_snapshot",
                reason="execution_market_not_cataloged_in_snapshot",
            ),
            "published_at": snapshot.get("observed_at"),
            "status_counts": {},
            "status_reason_counts": {},
            "scenario_count": 0,
        }

    status_counts = Counter(
        row.get("status") or "failed"
        for row in rows
    )
    status_priority = ("failed", "partial", "unsupported", "observed")
    status = next(
        candidate
        for candidate in status_priority
        if status_counts.get(candidate)
    )
    reason_counts = Counter(
        row.get("status_reason") or "missing_status_reason"
        for row in rows
    )
    state_times = sorted(
        {
            row["state_observed_at"]
            for row in rows
            if row.get("state_observed_at")
        }
    )
    published_times = sorted(
        {
            row["observed_at"]
            for row in rows
            if row.get("observed_at")
        }
    )
    errors = sorted(
        {
            row["error"]
            for row in rows
            if row.get("error")
        }
    )
    return {
        **_quality_lineage(
            status=status,
            observed_at=state_times[-1] if state_times else None,
            source=_one_execution_value(rows, "source"),
            source_endpoint=_one_execution_value(
                rows,
                "source_endpoint",
            ),
            method=_one_execution_value(rows, "calculation_method"),
            reason=(
                sorted(reason_counts)[0]
                if len(reason_counts) == 1
                else "mixed_execution_status_reasons"
            ),
            snapshot_id=_one_execution_value(rows, "snapshot_id"),
            raw_response_sha256=_one_execution_value(
                rows,
                "raw_response_sha256",
            ),
        ),
        "source_snapshot_id": _one_execution_value(
            rows,
            "source_snapshot_id",
        ),
        "published_at": published_times[-1] if published_times else None,
        "status_counts": dict(sorted(status_counts.items())),
        "status_reason_counts": dict(sorted(reason_counts.items())),
        "errors": errors,
        "scenario_count": len(rows),
        "directions": sorted(
            {row["direction"] for row in rows if row.get("direction")}
        ),
        "notionals_usd": sorted(
            {
                int(Decimal(row["requested_notional_usd"]))
                for row in rows
                if row.get("requested_notional_usd")
            }
        ),
    }


def build_market_quality(
    token_symbol: str | None,
    scope: str | None = None,
    market_a_id: str | None = None,
    market_b_id: str | None = None,
) -> dict[str, Any]:
    """Return a fact-by-market quality inventory for one exact Token."""
    if not token_symbol:
        raise ValueError("token is required")
    token = token_symbol.strip().upper()
    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in {"all", "selected"}:
        raise ValueError("scope must be all or selected")

    catalog = build_market_catalog()
    token_markets = [
        market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    ]
    if not token_markets:
        raise ValueError("Token is not cataloged")
    by_id = {market["market_id"]: market for market in token_markets}
    selected_ids: list[str] = []
    if normalized_scope == "selected":
        if not market_a_id or not market_b_id:
            raise ValueError(
                "market_a and market_b are required for selected scope"
            )
        if market_a_id == market_b_id:
            raise ValueError("market_a and market_b must be different")
        if market_a_id not in by_id or market_b_id not in by_id:
            raise ValueError(
                "Selected market is not cataloged for the requested token"
            )
        selected_ids = [market_a_id, market_b_id]
        token_markets = [by_id[market_id] for market_id in selected_ids]

    execution_sources = {
        "cex": _execution_quality_source(
            resolve_cex_execution_cost_path
        ),
        "dex": _execution_quality_source(
            resolve_dex_execution_cost_path
        ),
    }
    quality_markets = []
    for market in token_markets:
        quality_markets.append(
            {
                "market_id": market["market_id"],
                "token_symbol": market["token_symbol"],
                "market_type": market["market_type"],
                "venue": market["venue"],
                "instrument": market["instrument"],
                "chain": market.get("chain"),
                "pool_address": market.get("pool_address"),
                "quality_status": market.get("quality_status"),
                "quality_flags": market.get("quality_flag_details") or [],
                "facts": {
                    "daily": _daily_quality_fact(
                        market,
                        catalog["metadata"],
                    ),
                    "tvl": _tvl_quality_fact(market),
                    "depth": _depth_quality_fact(market),
                    "execution": _execution_quality_fact(
                        market,
                        execution_sources[market["market_type"]],
                    ),
                },
            }
        )
    return {
        "metadata": {
            "contract_version": QUALITY_CONTRACT_VERSION,
            "scope": normalized_scope,
            "selected_market_ids": selected_ids,
            "facts": ["daily", "tvl", "depth", "execution"],
            "status_semantics": QUALITY_STATUS_SEMANTICS,
            "freshness": catalog["metadata"].get("freshness"),
            "sources": catalog["metadata"].get("sources", []),
            "missing_value_rule": (
                "Measured zero remains zero. Missing, unavailable, failed, "
                "unsupported, not-cataloged, and not-applicable facts remain "
                "distinct and are never zero-filled."
            ),
        },
        "token_symbol": token,
        "markets": quality_markets,
    }


def encode_json_payload(payload: Any, accept_encoding: str = "") -> tuple[bytes, bool]:
    """Serialize JSON and compress substantial responses when the client supports gzip."""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) >= 1024 and "gzip" in accept_encoding.lower():
        return gzip.compress(raw, compresslevel=5), True
    return raw, False


def api_source_signature() -> tuple[tuple[str, int, int], ...]:
    """Return one signature covering every source that can change public facts."""
    database_path = resolve_database_path()
    paths: list[Path] = []
    if database_path is not None:
        paths.append(database_path)
    else:
        paths.extend(resolve_data_paths())
    for optional_path in (
        resolve_tvl_path(),
        resolve_cex_depth_path(),
        resolve_dex_depth_path(),
        resolve_cex_execution_cost_path(),
        resolve_dex_execution_cost_path(),
    ):
        if optional_path is not None:
            paths.append(optional_path)
    return data_signature(paths)


def api_freshness_bucket() -> int:
    """Refresh wall-clock freshness while retaining short-lived response reuse."""
    return int(time.time() // API_FRESHNESS_CACHE_SECONDS)


def _build_public_api_payload(
    route: str,
    query_items: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    query = dict(query_items)
    if route == "catalog":
        return build_market_catalog()
    if route == "market":
        return build_market_payload(query.get("start"), query.get("end"))
    if route == "compare":
        return build_market_comparison(
            token_symbol=query.get("token"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
            start=query.get("start"),
            end=query.get("end"),
        )
    if route == "execution_cost":
        return build_execution_cost_comparison(
            token_symbol=query.get("token"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
        )
    if route == "quality":
        return build_market_quality(
            token_symbol=query.get("token"),
            scope=query.get("scope"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
        )
    raise ValueError(f"Unknown public API route: {route}")


@lru_cache(maxsize=SERIALIZED_RESPONSE_CACHE_SIZE)
def _build_public_api_response_cached(
    route: str,
    query_items: tuple[tuple[str, str], ...],
    _source_signature: tuple[tuple[str, int, int], ...],
    _freshness_bucket: int,
) -> tuple[bytes, bool]:
    return encode_json_payload(
        _build_public_api_payload(route, query_items),
        "gzip",
    )


def clear_runtime_caches() -> None:
    """Drop every payload derived from a previous published source generation."""
    global _SOURCE_CACHE_GENERATION, _PUBLIC_RESPONSE_CACHE_GENERATION
    with SOURCE_CACHE_GENERATION_LOCK:
        for cached_builder in (
            _load_tvl_snapshot_cached,
            _load_cex_depth_snapshot_cached,
            _load_dex_depth_snapshot_cached,
            _load_execution_cost_snapshot_cached,
            _build_market_payload_cached,
            _build_database_payload_cached,
            _build_enriched_payload_cached,
            _build_market_catalog_cached,
            _build_public_api_response_cached,
        ):
            cached_builder.cache_clear()
        _SOURCE_CACHE_GENERATION = None
        _PUBLIC_RESPONSE_CACHE_GENERATION = None


def ensure_source_cache_generation(
    source_signature: tuple[tuple[str, int, int], ...],
) -> None:
    """Retain only one complete source generation instead of 32 large copies."""
    global _SOURCE_CACHE_GENERATION, _PUBLIC_RESPONSE_CACHE_GENERATION
    with SOURCE_CACHE_GENERATION_LOCK:
        if _SOURCE_CACHE_GENERATION is None:
            _SOURCE_CACHE_GENERATION = source_signature
            return
        if _SOURCE_CACHE_GENERATION == source_signature:
            return
        for cached_builder in (
            _load_tvl_snapshot_cached,
            _load_cex_depth_snapshot_cached,
            _load_dex_depth_snapshot_cached,
            _load_execution_cost_snapshot_cached,
            _build_market_payload_cached,
            _build_database_payload_cached,
            _build_enriched_payload_cached,
            _build_market_catalog_cached,
            _build_public_api_response_cached,
        ):
            cached_builder.cache_clear()
        _SOURCE_CACHE_GENERATION = source_signature
        _PUBLIC_RESPONSE_CACHE_GENERATION = None


def ensure_public_response_cache_generation(
    source_signature: tuple[tuple[str, int, int], ...],
    freshness_bucket: int,
) -> None:
    """Bound serialized responses to the active source and freshness minute."""
    global _PUBLIC_RESPONSE_CACHE_GENERATION
    generation = (source_signature, freshness_bucket)
    with SOURCE_CACHE_GENERATION_LOCK:
        if _PUBLIC_RESPONSE_CACHE_GENERATION == generation:
            return
        _build_public_api_response_cached.cache_clear()
        _PUBLIC_RESPONSE_CACHE_GENERATION = generation


def build_public_api_response(
    route: str,
    query_items: tuple[tuple[str, str], ...],
    accepts_gzip: bool,
) -> tuple[bytes, bool]:
    """Use one cold-cache builder so concurrent misses do not duplicate work."""
    with PUBLIC_API_CACHE_LOCK:
        with SOURCE_CACHE_GENERATION_LOCK:
            if not accepts_gzip:
                return encode_json_payload(
                    _build_public_api_payload(route, query_items),
                    "",
                )
            source_signature = api_source_signature()
            freshness_bucket = api_freshness_bucket()
            ensure_source_cache_generation(source_signature)
            ensure_public_response_cache_generation(source_signature, freshness_bucket)
            return _build_public_api_response_cached(
                route,
                query_items,
                source_signature,
                freshness_bucket,
            )


def public_api_query_items(
    route: str,
    query: dict[str, list[str]],
) -> tuple[tuple[str, str], ...]:
    """Normalize only supported fields so irrelevant query keys cannot fill the cache."""
    fields = PUBLIC_API_QUERY_FIELDS.get(route)
    if fields is None:
        raise ValueError(f"Unknown public API route: {route}")
    return tuple(
        (name, query[name][0])
        for name in fields
        if query.get(name) and query[name][0] is not None
    )


def is_loopback_host(host: str) -> bool:
    """Accept only an explicit loopback bind for the write-capable admin surface."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def is_spa_shell_path(path: str) -> bool:
    """Return true only for the dashboard's declared client-side routes."""
    decoded = unquote(urlparse(path).path)
    if decoded in {"/screener", "/screener/", "/methodology", "/methodology/"}:
        return True
    token_match = SPA_TOKEN_ROUTE.fullmatch(decoded)
    if token_match:
        page = decoded.rstrip("/").rsplit("/", 1)[-1]
        return page in SPA_TOKEN_PAGES
    return SPA_METHODOLOGY_ROUTE.fullmatch(decoded) is not None


def is_admin_surface_path(path: str) -> bool:
    normalized = (
        "/" + posixpath.normpath(unquote(path)).lstrip("/")
    ).casefold()
    return (
        normalized in ADMIN_STATIC_PATHS
        or normalized == "/admin"
        or normalized == "/api/admin"
        or normalized.startswith("/api/admin/")
    )


class MarketMonitorHandler(SimpleHTTPRequestHandler):
    server_version = "CexDexMarketMonitor"
    sys_version = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def send_json(
        self,
        payload: Any,
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body, compressed = encode_json_payload(
            payload,
            self.headers.get("Accept-Encoding", ""),
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def send_encoded_json(self, body: bytes, compressed: bool) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        self.end_headers()
        self.wfile.write(body)

    def send_public_api(self, route: str, query: dict[str, list[str]]) -> None:
        query_items = public_api_query_items(route, query)
        body, compressed = build_public_api_response(
            route,
            query_items,
            "gzip" in self.headers.get("Accept-Encoding", "").lower(),
        )
        self.send_encoded_json(body, compressed)

    def read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length < 0 or content_length > 16_384:
            raise ValueError("Request body is too large")
        payload = json.loads(self.rfile.read(content_length) or b"{}")
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def admin_session_token(self) -> str | None:
        cookie = SimpleCookie()
        cookie.load(self.headers.get("Cookie", ""))
        morsel = cookie.get("admin_session")
        return morsel.value if morsel else None

    def admin_surface_available(self) -> bool:
        bound_host = str(self.server.server_address[0])
        return ADMIN_SERVICE.available and is_loopback_host(bound_host)

    def require_admin(self, *, csrf: bool = False) -> tuple[str, dict[str, Any]] | None:
        if not self.admin_surface_available():
            self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return None
        if ADMIN_SERVICE.open_mode:
            return "", {
                "username": "open-admin",
                "csrf_token": "",
            }
        session_token = self.admin_session_token()
        session = ADMIN_SERVICE.get_session(session_token)
        if not session:
            self.send_json({"error": "Administrator authentication required"}, HTTPStatus.UNAUTHORIZED)
            return None
        if csrf and not hmac.compare_digest(
            self.headers.get("X-CSRF-Token", ""),
            session["csrf_token"],
        ):
            self.send_json({"error": "Invalid CSRF token"}, HTTPStatus.FORBIDDEN)
            return None
        return session_token or "", session

    def admin_cookie(self, session_token: str, *, clear: bool = False) -> str:
        secure = os.environ.get("ADMIN_COOKIE_SECURE", "").lower() in {"1", "true", "yes"}
        parts = [
            f"admin_session={'' if clear else session_token}",
            "Path=/api/admin",
            "HttpOnly",
            "SameSite=Strict",
            f"Max-Age={0 if clear else 8 * 60 * 60}",
        ]
        if secure:
            parts.append("Secure")
        return "; ".join(parts)

    def end_headers(self) -> None:
        request_path = urlparse(self.path).path
        if not request_path.startswith("/api/") and request_path != "/health":
            self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        super().end_headers()

    def list_directory(self, path: str) -> None:
        self.send_error(HTTPStatus.NOT_FOUND)
        return None

    def translate_path(self, path: str) -> str:
        request_path = urlparse(path).path
        if request_path in VENDOR_FILES:
            return str(VENDOR_FILES[request_path])
        if is_spa_shell_path(request_path):
            return str(STATIC_ROOT / "index.html")
        return super().translate_path(path)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if is_admin_surface_path(parsed.path) and not self.admin_surface_available():
            if parsed.path.startswith("/api/"):
                self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
            return
        if parsed.path == "/api/markets/catalog":
            try:
                self.send_public_api("catalog", {})
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/markets/compare":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("compare", query)
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/markets/execution-cost":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("execution_cost", query)
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/markets/quality":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("quality", query)
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/market":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("market", query)
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/admin/session":
            session = ADMIN_SERVICE.get_session(self.admin_session_token())
            self.send_json(ADMIN_SERVICE.public_session(session))
            return
        if parsed.path == "/api/admin/tokens":
            authenticated = self.require_admin()
            if authenticated:
                self.send_json({"tokens": ADMIN_SERVICE.configured_tokens()})
            return
        if parsed.path == "/api/admin/jobs":
            authenticated = self.require_admin()
            if authenticated:
                self.send_json({"jobs": ADMIN_SERVICE.list_jobs()})
            return
        if parsed.path == "/health":
            try:
                payload = build_market_payload()
                metadata = payload["metadata"]
                self.send_json(
                    {
                        "status": "ok",
                        "data_ready": True,
                        "storage": metadata["storage"]["engine"],
                        "data_status": metadata["freshness"]["overall_status"],
                        "freshness": metadata["freshness"],
                    }
                )
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"status": "degraded", "data_ready": False, "error": str(error)}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if is_admin_surface_path(path) and not self.admin_surface_available():
            self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            payload = self.read_json()
        except (ValueError, json.JSONDecodeError) as error:
            self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/admin/login":
            if ADMIN_SERVICE.open_mode:
                self.send_json({"error": "Administrator login is disabled"}, HTTPStatus.NOT_FOUND)
                return
            try:
                session_token, session = ADMIN_SERVICE.login(
                    self.client_address[0],
                    str(payload.get("username", ""))[:80],
                    str(payload.get("password", ""))[:512],
                )
            except RuntimeError as error:
                self.send_json({"error": str(error)}, HTTPStatus.SERVICE_UNAVAILABLE)
                return
            except PermissionError as error:
                self.send_json({"error": str(error)}, HTTPStatus.TOO_MANY_REQUESTS)
                return
            except ValueError as error:
                self.send_json({"error": str(error)}, HTTPStatus.UNAUTHORIZED)
                return
            self.send_json(session, extra_headers={"Set-Cookie": self.admin_cookie(session_token)})
            return

        if path == "/api/admin/logout":
            if ADMIN_SERVICE.open_mode:
                self.send_json(ADMIN_SERVICE.public_session(None))
                return
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            session_token, _ = authenticated
            ADMIN_SERVICE.logout(session_token)
            self.send_json(
                {"authenticated": False},
                extra_headers={"Set-Cookie": self.admin_cookie(session_token, clear=True)},
            )
            return

        if path == "/api/admin/jobs":
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            _, session = authenticated
            try:
                job = ADMIN_SERVICE.create_job(payload, session["username"])
            except RuntimeError as error:
                self.send_json({"error": str(error)}, HTTPStatus.CONFLICT)
                return
            except (ValueError, OSError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
                return
            self.send_json(job, HTTPStatus.ACCEPTED)
            return

        self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fact-only CEX/DEX Market Monitor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data-dir", help="Directory containing detailed CEX and DEX CSV files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.data_dir:
        os.environ["MARKET_DATA_DIR"] = str(Path(args.data_dir).expanduser().resolve())
    if ADMIN_SERVICE.available and not is_loopback_host(args.host):
        raise SystemExit(
            "Administrator surface requires a loopback bind. "
            "Run behind an HTTPS reverse proxy or disable ADMIN_ENABLED."
        )
    server = ThreadingHTTPServer((args.host, args.port), MarketMonitorHandler)
    print(f"Market Monitor running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
