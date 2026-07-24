"""Serve the fact-only CEX/DEX Market Monitor."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

try:
    from dashboard.admin import AdminService
    from dashboard.market_facts import (
        catalog_contract,
        catalog_from_market_payload,
        compare_daily_rows,
    )
except ModuleNotFoundError:
    from admin import AdminService
    from market_facts import (
        catalog_contract,
        catalog_from_market_payload,
        compare_daily_rows,
    )

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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
VENDOR_FILES = {
    "/vendor/lucide.js": DASHBOARD_ROOT / "node_modules/lucide/dist/umd/lucide.min.js",
}


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
    """Return sorted finite daily closes."""
    observations = [
        (row["date"], parse_number(row.get("close")))
        for row in rows
        if parse_number(row.get("close")) is not None
    ]
    return sorted(observations)


def price_statistics(rows: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    """Return latest price, window return, and daily log-return volatility."""
    observations = price_observations(rows)
    if not observations:
        return None, None, None

    latest_price = observations[-1][1]
    first_price = observations[0][1]
    window_return = (
        latest_price / first_price - 1
        if len(observations) >= 2 and first_price and latest_price is not None
        else None
    )
    log_returns = [
        math.log(current[1] / previous[1])
        for previous, current in zip(observations, observations[1:])
        if previous[1] and current[1] and previous[1] > 0 and current[1] > 0
    ]
    daily_volatility = statistics.stdev(log_returns) if len(log_returns) >= 2 else None
    return latest_price, window_return, daily_volatility


def summarize_cex(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["token_symbol"], row["exchange"], row["cex_symbol"])].append(row)

    summaries = []
    for (token, exchange, symbol), market_rows in groups.items():
        price, window_return, volatility = price_statistics(market_rows)
        volumes = [parse_number(row.get("quote_volume_usd")) for row in market_rows]
        summaries.append(
            {
                "token_symbol": token,
                "market": "cex",
                "venue": exchange,
                "instrument": symbol,
                "price_usd": price,
                "window_return": window_return,
                "daily_volatility": volatility,
                "volume_usd": sum(value for value in volumes if value is not None),
                "tvl_usd": None,
                "observation_days": len({row["date"] for row in market_rows}),
                "latest_date": max(row["date"] for row in market_rows),
                "price_points": [{"date": day, "price_usd": value} for day, value in price_observations(market_rows)],
            }
        )
    return sorted(summaries, key=lambda row: (row["token_symbol"], -row["volume_usd"], row["venue"]))


def summarize_dex(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[
            (
                row["token_symbol"],
                row["chain"],
                row["dex"],
                row["pool_address"],
                row["pool_name"],
            )
        ].append(row)

    summaries = []
    for (token, chain, dex, address, pool_name), pool_rows in groups.items():
        price, window_return, volatility = price_statistics(pool_rows)
        volumes = [parse_number(row.get("dex_volume_usd")) for row in pool_rows]
        summaries.append(
            {
                "token_symbol": token,
                "market": "dex",
                "venue": f"{chain} / {dex}",
                "instrument": pool_name,
                "pool_address": address,
                "price_usd": price,
                "window_return": window_return,
                "daily_volatility": volatility,
                "volume_usd": sum(value for value in volumes if value is not None),
                "tvl_usd": latest_non_null(pool_rows, "pool_tvl_usd"),
                "observation_days": len({row["date"] for row in pool_rows}),
                "latest_date": max(row["date"] for row in pool_rows),
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
    primary_cex = select_primary(cex_markets)
    primary_dex = select_primary(dex_pools)
    cex_volume: dict[str, float] = defaultdict(float)
    dex_volume: dict[str, float] = defaultdict(float)
    for row in cex_markets:
        cex_volume[row["token_symbol"]] += row["volume_usd"]
    for row in dex_pools:
        dex_volume[row["token_symbol"]] += row["volume_usd"]

    tokens = sorted(set(primary_cex) | set(primary_dex))
    summaries = []
    for token in tokens:
        cex = primary_cex.get(token)
        dex = primary_dex.get(token)
        comparison_date, _, _, spread = common_price_comparison(cex, dex)
        total_volume = cex_volume[token] + dex_volume[token]
        summaries.append(
            {
                "token_symbol": token,
                "cex_volume_usd": cex_volume[token],
                "dex_volume_usd": dex_volume[token],
                "total_volume_usd": total_volume,
                "observed_dex_share": dex_volume[token] / total_volume if total_volume else None,
                "price_spread": spread,
                "spread_date": comparison_date,
                "primary_cex_id": f"{cex['venue']}|{cex['instrument']}" if cex else None,
                "primary_dex_id": dex.get("pool_address") if dex else None,
            }
        )
    return summaries


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


@lru_cache(maxsize=32)
def _build_market_payload_cached(
    start: str | None,
    end: str | None,
    cex_path_text: str,
    dex_path_text: str,
    _signature: tuple[tuple[str, int, int], ...],
) -> dict[str, Any]:
    cex_path = Path(cex_path_text)
    dex_path = Path(dex_path_text)
    available_start, available_end = dataset_bounds([cex_path, dex_path])
    effective_end = parse_iso_date(end) or available_end
    default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
    effective_start = parse_iso_date(start) or max(available_start, default_start)
    if effective_start > effective_end:
        raise ValueError("start date must not be after end date")
    if effective_start < available_start or effective_end > available_end:
        raise ValueError(f"date window must be within {available_start} and {available_end}")

    cex_markets = summarize_cex(rows_in_window(cex_path, effective_start, effective_end))
    dex_pools = summarize_dex(rows_in_window(dex_path, effective_start, effective_end))
    if not cex_markets and not dex_pools:
        raise ValueError("No market observations exist in the selected time window")

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
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


@lru_cache(maxsize=32)
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

    cex_markets = summarize_cex(cex_rows)
    dex_pools = summarize_dex(dex_rows)
    if not cex_markets and not dex_pools:
        raise ValueError("No market observations exist in the selected time window")

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
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


def build_market_payload(start: str | None = None, end: str | None = None) -> dict[str, Any]:
    database_path = resolve_database_path()
    if database_path is not None:
        signature = data_signature([database_path])
        return _build_database_payload_cached(start, end, str(database_path), signature)

    cex_path, dex_path = resolve_data_paths()
    signature = data_signature([cex_path, dex_path])
    return _build_market_payload_cached(start, end, str(cex_path), str(dex_path), signature)


def build_market_catalog() -> dict[str, Any]:
    """Return every observed market plus the versioned fact contract."""
    default_payload = build_market_payload()
    metadata = default_payload["metadata"]
    full_payload = build_market_payload(
        metadata["available_start"],
        metadata["available_end"],
    )
    return catalog_from_market_payload(full_payload)


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
    observations = compare_daily_rows(rows_a, rows_b)
    comparable = [row for row in observations if row["spread_bps"] is not None]
    return {
        "metadata": {
            **catalog_contract(),
            "available_start": metadata["available_start"],
            "available_end": metadata["available_end"],
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
        "latest_comparable_observation": comparable[-1] if comparable else None,
        "observations": observations,
    }


def encode_json_payload(payload: Any, accept_encoding: str = "") -> tuple[bytes, bool]:
    """Serialize JSON and compress substantial responses when the client supports gzip."""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) >= 1024 and "gzip" in accept_encoding.lower():
        return gzip.compress(raw, compresslevel=5), True
    return raw, False


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

    def require_admin(self, *, csrf: bool = False) -> tuple[str, dict[str, Any]] | None:
        if not ADMIN_SERVICE.login_required:
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
        return super().translate_path(path)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/markets/catalog":
            try:
                self.send_json(build_market_catalog())
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/markets/compare":
            query = parse_qs(parsed.query)
            try:
                self.send_json(
                    build_market_comparison(
                        token_symbol=query.get("token", [None])[0],
                        market_a_id=query.get("market_a", [None])[0],
                        market_b_id=query.get("market_b", [None])[0],
                        start=query.get("start", [None])[0],
                        end=query.get("end", [None])[0],
                    )
                )
            except (FileNotFoundError, ValueError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/market":
            query = parse_qs(parsed.query)
            try:
                self.send_json(
                    build_market_payload(
                        start=query.get("start", [None])[0],
                        end=query.get("end", [None])[0],
                    )
                )
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
                database_path = resolve_database_path()
                if database_path is not None:
                    self.send_json({"status": "ok", "data_ready": True, "storage": "sqlite"})
                    return
                cex_path, dex_path = resolve_data_paths()
                self.send_json(
                    {
                        "status": "ok",
                        "data_ready": cex_path.exists() and dex_path.exists(),
                        "storage": "csv",
                    }
                )
            except FileNotFoundError as error:
                self.send_json({"status": "degraded", "data_ready": False, "error": str(error)}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            payload = self.read_json()
        except (ValueError, json.JSONDecodeError) as error:
            self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/admin/login":
            if not ADMIN_SERVICE.login_required:
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
            if not ADMIN_SERVICE.login_required:
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
