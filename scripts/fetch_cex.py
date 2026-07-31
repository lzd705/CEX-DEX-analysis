"""Fetch daily CEX OHLCV and volume data.

This version is intentionally simple:
    - Read token symbols from config/tokens.csv
    - Fetch daily spot klines from supported exchanges
    - Write exchange-level rows to data/processed/cex_exchange_volume_daily.csv
    - Write aggregated token-date rows to data/processed/cex_volume_daily.csv
"""

import csv
import argparse
import hashlib
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

try:
    from scripts.fact_quality import (
        normalize_cex_instrument,
        normalize_collection_attempts,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from fact_quality import normalize_cex_instrument, normalize_collection_attempts


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_CONFIG_PATH = PROJECT_ROOT / "config/tokens.csv"
EXCHANGE_OUTPUT_PATH = PROJECT_ROOT / "data/processed/cex_exchange_volume_daily.csv"
COVERAGE_OUTPUT_PATH = PROJECT_ROOT / "data/processed/cex_exchange_coverage.csv"
OUTPUT_PATH = PROJECT_ROOT / "data/processed/cex_volume_daily.csv"
ATTEMPT_OUTPUT_PATH = (
    PROJECT_ROOT / "data/processed/cex_daily_collection_attempts.json"
)
ATTEMPT_SCHEMA = "daily_collection_attempts/v1"
LIMIT_DAYS = 180
MAX_REFRESH_WINDOW_DAYS = 180
HTX_RECENT_BAR_CAP = 2000
KRAKEN_RESPONSE_CAP = 720
MIN_HISTORY_DAYS = 120
MIN_EXCHANGE_COUNT = 3
PRICE_EXCHANGE = "binance"
TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

BINANCE_BASE_URLS = [
    "https://api.binance.com",
    "https://data-api.binance.vision",
]

EXCHANGES = [
    "binance",
    "okx",
    "bybit",
    "kucoin",
    "gate",
    "bitget",
    "mexc",
    "htx",
    "coinbase",
    "kraken",
    "crypto_com",
    "upbit",
]

ATTEMPT_ERROR_MESSAGES = {
    "network": "The source request could not reach the remote service.",
    "rate_limit": "The source rejected the request because its rate limit was reached.",
    "source_unavailable": "The remote source was temporarily unavailable.",
    "not_listed": "The source reported that the requested market was unavailable.",
    "parse": "The source response could not be decoded into the expected format.",
    "validation": "The source response did not satisfy the collector contract.",
    "source_range_unavailable": "The source endpoint cannot reach the requested date window.",
}


class SourceRangeUnavailable(RuntimeError):
    """The source endpoint cannot position its response over the requested range."""


def get_request_window(limit_days: int, start_date=None, end_date=None):
    """Return UTC [start, end) datetimes for a daily request."""
    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date must be provided together")
        start_time = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_time = datetime.strptime(end_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        ) + timedelta(days=1)
        window_days = (end_time - start_time).days
        if window_days < 1 or window_days > MAX_REFRESH_WINDOW_DAYS:
            raise ValueError("Refresh window must contain between 1 and 180 days")
        return start_time, end_time

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=limit_days + 5)
    return start_time, end_time


def get_recent_bar_count(limit_days: int, start_date, cap: int):
    """Size a recent-only endpoint so a historical start can still be reached."""
    if start_date is None:
        return min(limit_days, cap)
    start_time, _ = get_request_window(1, start_date, start_date)
    distance_days = (datetime.now(timezone.utc).date() - start_time.date()).days
    return min(cap, max(limit_days, distance_days + 3))


def require_recent_response_covers_end(
    rows,
    *,
    end_date,
    timestamp_getter,
    source,
    cap,
):
    """Reject a recent-only response whose oldest bar is newer than target end."""
    if end_date is None or not rows:
        return
    target_start, _ = get_request_window(1, end_date, end_date)
    oldest_timestamp = min(int(timestamp_getter(row)) for row in rows)
    if oldest_timestamp > int(target_start.timestamp()):
        raise SourceRangeUnavailable(
            "source_range_unavailable: %s recent-bar cap %s does not reach %s"
            % (source, cap, end_date)
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _exception_chain(error):
    current = error
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def classify_attempt_error(error):
    """Return a bounded reason without persisting raw URLs, payloads, or paths."""
    chain = list(_exception_chain(error))
    http_status = next(
        (
            int(candidate.code)
            for candidate in chain
            if isinstance(getattr(candidate, "code", None), int)
            and 100 <= int(candidate.code) <= 599
        ),
        None,
    )
    lowered = " ".join(str(candidate).lower() for candidate in chain)
    if "source_range_unavailable" in lowered:
        reason = "source_range_unavailable"
    elif http_status == 429 or "rate limit" in lowered or "too many requests" in lowered:
        reason = "rate_limit"
    elif http_status is not None and http_status >= 500:
        reason = "source_unavailable"
    elif http_status == 404 or any(
        marker in lowered
        for marker in (
            "invalid symbol",
            "unknown symbol",
            "not listed",
            "market not found",
            "instrument not found",
            "does not exist",
        )
    ):
        reason = "not_listed"
    elif any(
        isinstance(candidate, (urllib.error.URLError, TimeoutError))
        for candidate in chain
    ):
        reason = "network"
    elif any(
        isinstance(candidate, (json.JSONDecodeError, UnicodeDecodeError))
        for candidate in chain
    ):
        reason = "parse"
    elif http_status in {400, 409, 422} or any(
        isinstance(candidate, (KeyError, IndexError, TypeError, ValueError))
        for candidate in chain
    ):
        reason = "validation"
    else:
        reason = "source_unavailable"
    return {
        "reason_code": reason,
        "http_status": http_status,
        "error": ATTEMPT_ERROR_MESSAGES[reason],
    }


def _window_dates(start_date, end_date):
    if not start_date or not end_date:
        return None
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not precede start_date")
    return {
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    }


def cex_attempt_record(
    token_symbol,
    exchange,
    instrument,
    *,
    rows=None,
    error=None,
    start_date=None,
    end_date=None,
):
    observed_dates = sorted(
        {
            str(row.get("date") or "")
            for row in (rows or [])
            if row.get("date")
            and (start_date is None or row["date"] >= start_date)
            and (end_date is None or row["date"] <= end_date)
        }
    )
    expected_dates = _window_dates(start_date, end_date)
    if error is not None:
        classified = classify_attempt_error(error)
        if classified["reason_code"] == "source_range_unavailable":
            status = "unsupported"
            outcome = "range_unavailable"
        else:
            status = "failed"
            outcome = "request_failed"
    elif not observed_dates:
        classified = {
            "reason_code": "no_candles",
            "http_status": None,
            "error": (
                "The source returned no daily candles inside the requested window."
            ),
        }
        status = "no_data"
        outcome = "no_candles"
    elif expected_dates is not None and not expected_dates.issubset(
        set(observed_dates)
    ):
        classified = {
            "reason_code": "no_candles",
            "http_status": None,
            "error": (
                "The source returned only part of the requested daily-candle window."
            ),
        }
        status = "partial"
        outcome = "partial_observation"
    else:
        classified = {
            "reason_code": "observed",
            "http_status": None,
            "error": None,
        }
        status = "succeeded"
        outcome = "observed"
    source_instruments = set()
    for row in rows or []:
        has_explicit_source = "source_instrument" in row
        raw_source = (
            row.get("source_instrument")
            if has_explicit_source
            else row.get("cex_symbol")
        )
        if has_explicit_source or raw_source not in (None, ""):
            source_instruments.add(
                normalize_cex_instrument(
                    raw_source,
                    field_name="CEX source instrument",
                )
            )
    if len(source_instruments) > 1:
        raise ValueError("CEX attempt returned multiple source instruments")
    canonical_instrument = normalize_cex_instrument(
        instrument,
        field_name="CEX canonical instrument",
    )
    source_instrument = next(iter(source_instruments), None)
    source_alias_validated = False
    if source_instrument and source_instrument != canonical_instrument:
        canonical_base, _, canonical_quote = canonical_instrument.partition("/")
        source_base, _, source_quote = source_instrument.partition("/")
        if not (
            str(exchange).strip().lower() == "upbit"
            and canonical_quote == "USDT"
            and source_quote == "KRW"
            and canonical_base == source_base
        ):
            raise ValueError("CEX source instrument is not an approved alias")
        source_alias_validated = True
    identity = {
        "market_type": "cex",
        "token_symbol": str(token_symbol).strip().upper(),
        "exchange": str(exchange).strip().lower(),
        "instrument": canonical_instrument,
        "chain": None,
        "dex": None,
        "pool_address": None,
    }
    finished_at_utc = datetime.now(timezone.utc).isoformat()
    id_material = {
        **identity,
        "source_instrument": source_instrument,
        "source_instrument_alias_validated": source_alias_validated,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "status": status,
        "reason_code": classified["reason_code"],
        "finished_at_utc": finished_at_utc,
    }
    return {
        "attempt_id": hashlib.sha256(
            json.dumps(
                id_material,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:20],
        **identity,
        "source_instrument": source_instrument,
        "source_instrument_alias_validated": source_alias_validated,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "observed_dates": observed_dates,
        "observed_day_count": len(observed_dates),
        "status": status,
        "outcome": outcome,
        **classified,
        "finished_at_utc": finished_at_utc,
    }


def write_attempt_ledger(
    path: Path,
    attempts,
    *,
    source_csv: Path,
    start_date=None,
    end_date=None,
):
    validated_attempts = list(attempts)
    attempt_ids = set()
    for attempt in validated_attempts:
        if not isinstance(attempt, dict):
            raise ValueError("attempt must be an object")
        attempt_id = attempt.get("attempt_id")
        if (
            not isinstance(attempt_id, str)
            or not attempt_id.strip()
            or attempt_id != attempt_id.strip()
            or len(attempt_id) > 64
        ):
            raise ValueError("attempt ID is missing or outside the supported range")
        if attempt_id in attempt_ids:
            raise ValueError("attempt IDs must be unique")
        attempt_ids.add(attempt_id)
        if not all(str(attempt.get(key) or "").strip() for key in ("token_symbol", "exchange", "instrument")):
            raise ValueError("CEX attempt identity is incomplete")
    normalized_attempts = normalize_collection_attempts(
        validated_attempts,
        market_type="cex",
    )
    payload = {
        "schema": ATTEMPT_SCHEMA,
        "collector": "cex",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_window": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "source_csv": source_csv.name,
        "source_csv_sha256": sha256_file(source_csv),
        "attempt_count": len(normalized_attempts),
        "attempts": sorted(
            normalized_attempts,
            key=lambda item: (
                item["token_symbol"],
                item["exchange"],
                item["instrument"],
                item["attempt_id"],
            ),
        ),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".{}.tmp".format(path.name))
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        os.replace(str(temporary), str(path))
    finally:
        temporary.unlink(missing_ok=True)
    return payload


def make_binance_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNIUSDT for Binance REST API."""
    return cex_symbol.replace("/", "").upper()


def make_okx_inst_id(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNI-USDT for OKX REST API."""
    return cex_symbol.replace("/", "-").upper()


def make_bybit_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNIUSDT for Bybit REST API."""
    return cex_symbol.replace("/", "").upper()


def make_kucoin_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNI-USDT for KuCoin REST API."""
    return cex_symbol.replace("/", "-").upper()


def make_gate_currency_pair(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNI_USDT for Gate REST API."""
    return cex_symbol.replace("/", "_").upper()


def make_bitget_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNIUSDT for Bitget REST API."""
    return cex_symbol.replace("/", "").upper()


def make_mexc_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNIUSDT for MEXC REST API."""
    return cex_symbol.replace("/", "").upper()


def make_htx_symbol(cex_symbol: str) -> str:
    """Convert UNI/USDT to uniusdt for HTX REST API."""
    return cex_symbol.replace("/", "").lower()


def make_coinbase_product_id(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNI-USD for Coinbase REST API."""
    base_asset = cex_symbol.split("/")[0].upper()
    return base_asset + "-USD"


def make_kraken_pair(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNIUSD for Kraken REST API."""
    base_asset = cex_symbol.split("/")[0].upper()
    return base_asset + "USD"


def make_crypto_com_instrument(cex_symbol: str) -> str:
    """Convert UNI/USDT to UNI_USDT for Crypto.com."""
    return cex_symbol.replace("/", "_").upper()


def make_upbit_market_candidates(cex_symbol: str):
    """Return Upbit markets in preferred quote-currency order."""
    base_asset = cex_symbol.split("/")[0].upper()
    return ["KRW-" + base_asset, "USDT-" + base_asset]


def convert_binance_kline(kline, token_symbol: str, cex_symbol: str, exchange: str):
    """Convert one Binance kline list into one output CSV row."""
    open_time_ms = int(kline[0])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": exchange,
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[7]),
    }

    return row


def convert_okx_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one OKX kline list into one output CSV row."""
    open_time_ms = int(kline[0])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "okx",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[7]),
    }

    return row


def convert_bybit_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one Bybit kline list into one output CSV row."""
    open_time_ms = int(kline[0])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "bybit",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[6]),
    }

    return row


def convert_kucoin_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one KuCoin kline list into one output CSV row."""
    open_time = int(kline[0])
    date = datetime.fromtimestamp(open_time, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "kucoin",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[3]),
        "low": float(kline[4]),
        "close": float(kline[2]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[6]),
    }

    return row


def convert_gate_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one Gate kline list into one output CSV row."""
    open_time = int(kline[0])
    date = datetime.fromtimestamp(open_time, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "gate",
        "cex_symbol": cex_symbol,
        "open": float(kline[5]),
        "high": float(kline[3]),
        "low": float(kline[4]),
        "close": float(kline[2]),
        "base_volume": float(kline[6]),
        "quote_volume_usd": float(kline[1]),
    }

    return row


def convert_bitget_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one Bitget kline list into one output CSV row."""
    open_time_ms = int(kline[0])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "bitget",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[6]),
    }

    return row


def convert_mexc_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one MEXC kline list into one output CSV row."""
    open_time_ms = int(kline[0])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "mexc",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "base_volume": float(kline[5]),
        "quote_volume_usd": float(kline[7]),
    }

    return row


def convert_htx_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one HTX kline dictionary into one output CSV row."""
    open_time = int(kline["id"])
    date = datetime.fromtimestamp(open_time, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "htx",
        "cex_symbol": cex_symbol,
        "open": float(kline["open"]),
        "high": float(kline["high"]),
        "low": float(kline["low"]),
        "close": float(kline["close"]),
        "base_volume": float(kline["amount"]),
        "quote_volume_usd": float(kline["vol"]),
    }

    return row


def convert_coinbase_candle(candle, token_symbol: str, cex_symbol: str):
    """Convert one Coinbase candle into one output CSV row.

    Coinbase returns base volume only, so quote volume is approximated as
    close price times base volume.
    """
    open_time = int(candle[0])
    date = datetime.fromtimestamp(open_time, timezone.utc).strftime("%Y-%m-%d")
    close = float(candle[4])
    base_volume = float(candle[5])

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "coinbase",
        "cex_symbol": cex_symbol,
        "open": float(candle[3]),
        "high": float(candle[2]),
        "low": float(candle[1]),
        "close": close,
        "base_volume": base_volume,
        "quote_volume_usd": close * base_volume,
    }

    return row


def convert_kraken_kline(kline, token_symbol: str, cex_symbol: str):
    """Convert one Kraken kline into one output CSV row.

    Kraken returns base volume only, so quote volume is approximated as
    close price times base volume.
    """
    open_time = int(kline[0])
    date = datetime.fromtimestamp(open_time, timezone.utc).strftime("%Y-%m-%d")
    close = float(kline[4])
    base_volume = float(kline[6])

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "kraken",
        "cex_symbol": cex_symbol,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": close,
        "base_volume": base_volume,
        "quote_volume_usd": close * base_volume,
    }

    return row


def convert_crypto_com_candle(candle, token_symbol: str, cex_symbol: str):
    """Convert one Crypto.com candle into one output CSV row.

    Crypto.com candles provide base volume, so quote volume is approximated as
    close price times base volume.
    """
    open_time_ms = int(candle["t"])
    date = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
    close = float(candle["c"])
    base_volume = float(candle["v"])

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "crypto_com",
        "cex_symbol": cex_symbol,
        "open": float(candle["o"]),
        "high": float(candle["h"]),
        "low": float(candle["l"]),
        "close": close,
        "base_volume": base_volume,
        "quote_volume_usd": close * base_volume,
    }

    return row


def convert_upbit_candle(candle, token_symbol: str, quote_to_usd: float):
    """Convert one Upbit candle and its quote currency into USD."""
    market = candle["market"]
    quote_asset, base_asset = market.split("-", 1)
    date = candle["candle_date_time_utc"][:10]

    row = {
        "date": date,
        "token_symbol": token_symbol,
        "exchange": "upbit",
        "cex_symbol": base_asset + "/" + quote_asset,
        "open": float(candle["opening_price"]) * quote_to_usd,
        "high": float(candle["high_price"]) * quote_to_usd,
        "low": float(candle["low_price"]) * quote_to_usd,
        "close": float(candle["trade_price"]) * quote_to_usd,
        "base_volume": float(candle["candle_acc_trade_volume"]),
        "quote_volume_usd": float(candle["candle_acc_trade_price"]) * quote_to_usd,
    }

    return row


def read_token_config(path: Path):
    """Read token config rows."""
    with path.open("r", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    return rows


def read_exchange_rows(path: Path):
    """Read existing exchange facts with numeric fields restored."""
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    numeric_fields = ["open", "high", "low", "close", "base_volume", "quote_volume_usd"]
    for row in rows:
        for field in numeric_fields:
            value = row.get(field)
            row[field] = float(value) if value not in (None, "") else None
    return rows


def merge_exchange_rows(existing_rows, new_rows):
    """Upsert exchange facts by token, exchange, symbol, and date."""
    merged = {
        (row["token_symbol"], row["exchange"], row["cex_symbol"], row["date"]): row
        for row in existing_rows
    }
    for row in new_rows:
        merged[(row["token_symbol"], row["exchange"], row["cex_symbol"], row["date"])] = row
    return list(merged.values())


def filter_token_rows(rows, token_symbols):
    """Keep only explicitly requested token symbols."""
    if token_symbols is None:
        return rows
    requested = {token_symbol.upper() for token_symbol in token_symbols}
    return [row for row in rows if row["token_symbol"].upper() in requested]


def fetch_binance_klines(
    binance_symbol: str,
    limit_days: int,
    start_date=None,
    end_date=None,
):
    """Fetch daily klines from Binance."""
    query = {
        "symbol": binance_symbol,
        "interval": "1d",
        "limit": str(min(limit_days, 1000)),
    }
    if start_date is not None:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["startTime"] = str(int(start_time.timestamp() * 1000))
        query["endTime"] = str(int(end_time.timestamp() * 1000) - 1)

    encoded_query = urllib.parse.urlencode(query)
    last_error = None

    for base_url in BINANCE_BASE_URLS:
        url = base_url + "/api/v3/klines?" + encoded_query

        try:
            with urllib.request.urlopen(url, timeout=30, context=TLS_CONTEXT) as response:
                text = response.read().decode("utf-8")
                data = json.loads(text)
                return data
        except Exception as error:
            last_error = error

    raise RuntimeError("Failed to fetch %s" % binance_symbol) from last_error


def request_json(url: str):
    """Request JSON with a basic User-Agent."""
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

    with urllib.request.urlopen(request, timeout=30, context=TLS_CONTEXT) as response:
        text = response.read().decode("utf-8")
        data = json.loads(text)

    return data


def get_time_window(limit_days: int):
    """Return UTC start and end times for daily candle requests."""
    return get_request_window(limit_days)


def fetch_okx_klines(inst_id: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from OKX."""
    query = {
        "instId": inst_id,
        "bar": "1Dutc",
        "limit": str(min(limit_days, 300)),
    }
    endpoint = "/api/v5/market/candles"
    if start_date is not None:
        _, end_time = get_request_window(limit_days, start_date, end_date)
        query["after"] = str(int(end_time.timestamp() * 1000))
        endpoint = "/api/v5/market/history-candles"

    encoded_query = urllib.parse.urlencode(query)
    url = "https://www.okx.com" + endpoint + "?" + encoded_query

    data = request_json(url)

    if data.get("code") != "0":
        raise RuntimeError("OKX error for %s: %s" % (inst_id, data))

    return data.get("data", [])


def fetch_bybit_klines(symbol: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from Bybit."""
    query = {
        "category": "spot",
        "symbol": symbol,
        "interval": "D",
        "limit": str(min(limit_days, 1000)),
    }
    if start_date is not None:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["start"] = str(int(start_time.timestamp() * 1000))
        query["end"] = str(int(end_time.timestamp() * 1000) - 1)

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.bybit.com/v5/market/kline?" + encoded_query

    data = request_json(url)

    if data.get("retCode") != 0:
        raise RuntimeError("Bybit error for %s: %s" % (symbol, data))

    result = data.get("result", {})
    return result.get("list", [])


def fetch_kucoin_klines(symbol: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from KuCoin."""
    start_time, end_time = get_request_window(limit_days, start_date, end_date)
    query = {
        "type": "1day",
        "symbol": symbol,
        "startAt": str(int(start_time.timestamp())),
        "endAt": str(int(end_time.timestamp())),
    }

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.kucoin.com/api/v1/market/candles?" + encoded_query

    data = request_json(url)

    if data.get("code") != "200000":
        raise RuntimeError("KuCoin error for %s: %s" % (symbol, data))

    return data.get("data", [])[:limit_days]


def fetch_gate_klines(
    currency_pair: str,
    limit_days: int,
    start_date=None,
    end_date=None,
):
    """Fetch daily klines from Gate."""
    query = {
        "currency_pair": currency_pair,
        "interval": "1d",
    }
    if start_date is None:
        query["limit"] = str(min(limit_days, 1000))
    else:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["from"] = str(int(start_time.timestamp()))
        query["to"] = str(int(end_time.timestamp()) - 1)

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.gateio.ws/api/v4/spot/candlesticks?" + encoded_query

    return request_json(url)


def fetch_bitget_klines(symbol: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from Bitget."""
    query = {
        "symbol": symbol,
        "granularity": "1day",
        "limit": str(min(limit_days, 1000)),
    }
    if start_date is not None:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["startTime"] = str(int(start_time.timestamp() * 1000))
        query["endTime"] = str(int(end_time.timestamp() * 1000) - 1)

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.bitget.com/api/v2/spot/market/candles?" + encoded_query

    data = request_json(url)

    if data.get("code") != "00000":
        raise RuntimeError("Bitget error for %s: %s" % (symbol, data))

    return data.get("data", [])


def fetch_mexc_klines(symbol: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from MEXC."""
    query = {
        "symbol": symbol,
        "interval": "1d",
        "limit": str(min(limit_days, 1000)),
    }
    if start_date is not None:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["startTime"] = str(int(start_time.timestamp() * 1000))
        query["endTime"] = str(int(end_time.timestamp() * 1000) - 1)

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.mexc.com/api/v3/klines?" + encoded_query

    return request_json(url)


def fetch_htx_klines(symbol: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from HTX."""
    query = {
        "symbol": symbol,
        "period": "1day",
        "size": str(
            get_recent_bar_count(limit_days, start_date, HTX_RECENT_BAR_CAP)
        ),
    }

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.huobi.pro/market/history/kline?" + encoded_query

    data = request_json(url)

    if data.get("status") != "ok":
        raise RuntimeError("HTX error for %s: %s" % (symbol, data))

    rows = data.get("data", [])
    require_recent_response_covers_end(
        rows,
        end_date=end_date,
        timestamp_getter=lambda row: row["id"],
        source="HTX",
        cap=HTX_RECENT_BAR_CAP,
    )
    return rows


def fetch_coinbase_candles(
    product_id: str,
    limit_days: int,
    start_date=None,
    end_date=None,
):
    """Fetch daily candles from Coinbase."""
    start_time, end_time = get_request_window(limit_days, start_date, end_date)
    query = {
        "granularity": "86400",
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
    }

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.exchange.coinbase.com/products/%s/candles?%s" % (
        product_id,
        encoded_query,
    )

    return request_json(url)[:limit_days]


def fetch_kraken_klines(pair: str, limit_days: int, start_date=None, end_date=None):
    """Fetch daily klines from Kraken."""
    start_time, _ = get_request_window(limit_days, start_date, end_date)
    query = {
        "pair": pair,
        "interval": "1440",
        "since": str(int(start_time.timestamp())),
    }

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.kraken.com/0/public/OHLC?" + encoded_query

    data = request_json(url)

    if data.get("error"):
        raise RuntimeError("Kraken error for %s: %s" % (pair, data))

    result = data.get("result", {})
    rows = []

    for key, value in result.items():
        if key == "last":
            continue
        rows = value
        break

    if start_date is not None:
        require_recent_response_covers_end(
            rows,
            end_date=end_date,
            timestamp_getter=lambda row: row[0],
            source="Kraken",
            cap=KRAKEN_RESPONSE_CAP,
        )
        return rows
    return rows[-limit_days:]


def fetch_crypto_com_candles(
    instrument_name: str,
    limit_days: int,
    start_date=None,
    end_date=None,
):
    """Fetch daily candles from Crypto.com."""
    query = {
        "instrument_name": instrument_name,
        "timeframe": "1D",
        "count": str(limit_days),
    }
    if start_date is not None:
        start_time, end_time = get_request_window(
            limit_days, start_date, end_date
        )
        query["start_ts"] = str(int(start_time.timestamp() * 1000))
        query["end_ts"] = str(int(end_time.timestamp() * 1000) - 1)

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.crypto.com/exchange/v1/public/get-candlestick?" + encoded_query

    data = request_json(url)

    if data.get("code") != 0:
        raise RuntimeError("Crypto.com error for %s: %s" % (instrument_name, data))

    result = data.get("result", {})
    return result.get("data", [])


def fetch_upbit_candles(market: str, limit_days: int, end_date=None):
    """Fetch daily candles from Upbit Korea."""
    query = {
        "market": market,
        "count": str(min(limit_days, 200)),
    }
    if end_date is not None:
        _, end_time = get_request_window(1, end_date, end_date)
        query["to"] = end_time.isoformat().replace("+00:00", "Z")

    encoded_query = urllib.parse.urlencode(query)
    url = "https://api.upbit.com/v1/candles/days?" + encoded_query
    data = request_json(url)

    if not isinstance(data, list):
        raise RuntimeError("Upbit error for %s: %s" % (market, data))

    return data[:limit_days]


def build_upbit_rows(
    token_symbol: str,
    cex_symbol: str,
    limit_days: int,
    start_date=None,
    end_date=None,
):
    """Fetch the preferred available Upbit market and convert volume to USD."""
    last_error = None

    for market in make_upbit_market_candidates(cex_symbol):
        try:
            candles = fetch_upbit_candles(market, limit_days, end_date=end_date)
        except Exception as error:
            last_error = error
            continue

        if not candles:
            continue

        quote_asset = market.split("-", 1)[0]
        quote_to_usd_by_date = {}

        if quote_asset == "USDT":
            for candle in candles:
                date = candle["candle_date_time_utc"][:10]
                quote_to_usd_by_date[date] = 1.0
        elif quote_asset == "KRW":
            reference_candles = fetch_upbit_candles(
                "KRW-USDT", limit_days, end_date=end_date
            )

            for reference in reference_candles:
                date = reference["candle_date_time_utc"][:10]
                krw_per_usdt = float(reference["trade_price"])

                if krw_per_usdt > 0:
                    quote_to_usd_by_date[date] = 1.0 / krw_per_usdt
        else:
            continue

        rows = []

        for candle in candles:
            date = candle["candle_date_time_utc"][:10]
            quote_to_usd = quote_to_usd_by_date.get(date)

            if quote_to_usd is None:
                continue

            row = convert_upbit_candle(candle, token_symbol, quote_to_usd)
            row["source_instrument"] = row["cex_symbol"]
            row["cex_symbol"] = cex_symbol.upper()
            rows.append(row)

        if rows:
            return rows

    raise RuntimeError("Failed to fetch %s on Upbit" % token_symbol) from last_error


def fetch_exchange_rows(
    token_symbol: str,
    cex_symbol: str,
    exchange: str,
    start_date=None,
    end_date=None,
):
    """Fetch rows for one token on one exchange."""
    if exchange == "binance":
        binance_symbol = make_binance_symbol(cex_symbol)
        klines = fetch_binance_klines(
            binance_symbol, LIMIT_DAYS, start_date, end_date
        )
        rows = []

        for kline in klines:
            row = convert_binance_kline(kline, token_symbol, cex_symbol, "binance")
            rows.append(row)

        return rows

    if exchange == "okx":
        inst_id = make_okx_inst_id(cex_symbol)
        klines = fetch_okx_klines(inst_id, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_okx_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "bybit":
        symbol = make_bybit_symbol(cex_symbol)
        klines = fetch_bybit_klines(symbol, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_bybit_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "kucoin":
        symbol = make_kucoin_symbol(cex_symbol)
        klines = fetch_kucoin_klines(symbol, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_kucoin_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "gate":
        currency_pair = make_gate_currency_pair(cex_symbol)
        klines = fetch_gate_klines(
            currency_pair, LIMIT_DAYS, start_date, end_date
        )
        rows = []

        for kline in klines:
            row = convert_gate_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "bitget":
        symbol = make_bitget_symbol(cex_symbol)
        klines = fetch_bitget_klines(symbol, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_bitget_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "mexc":
        symbol = make_mexc_symbol(cex_symbol)
        klines = fetch_mexc_klines(symbol, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_mexc_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "htx":
        symbol = make_htx_symbol(cex_symbol)
        klines = fetch_htx_klines(symbol, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_htx_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "coinbase":
        product_id = make_coinbase_product_id(cex_symbol)
        candles = fetch_coinbase_candles(
            product_id, LIMIT_DAYS, start_date, end_date
        )
        rows = []

        for candle in candles:
            row = convert_coinbase_candle(candle, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "kraken":
        pair = make_kraken_pair(cex_symbol)
        klines = fetch_kraken_klines(pair, LIMIT_DAYS, start_date, end_date)
        rows = []

        for kline in klines:
            row = convert_kraken_kline(kline, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "crypto_com":
        instrument_name = make_crypto_com_instrument(cex_symbol)
        candles = fetch_crypto_com_candles(
            instrument_name, LIMIT_DAYS, start_date, end_date
        )
        rows = []

        for candle in candles:
            row = convert_crypto_com_candle(candle, token_symbol, cex_symbol)
            rows.append(row)

        return rows

    if exchange == "upbit":
        return build_upbit_rows(
            token_symbol,
            cex_symbol,
            LIMIT_DAYS,
            start_date,
            end_date,
        )

    raise ValueError("Unsupported exchange: %s" % exchange)


def build_rows(
    token_rows,
    exchanges=None,
    *,
    attempt_records=None,
    start_date=None,
    end_date=None,
):
    """Fetch all CEX rows for configured tokens."""
    all_rows = []
    selected_exchanges = exchanges or EXCHANGES

    for token in token_rows:
        token_symbol = token["token_symbol"]
        cex_symbol = token["cex_symbol"]

        for exchange in selected_exchanges:
            try:
                rows = fetch_exchange_rows(
                    token_symbol,
                    cex_symbol,
                    exchange,
                    start_date,
                    end_date,
                )
            except Exception as error:
                print("Failed %s on %s: %s" % (token_symbol, exchange, error))
                if attempt_records is not None:
                    attempt_records.append(
                        cex_attempt_record(
                            token_symbol,
                            exchange,
                            cex_symbol,
                            error=error,
                            start_date=start_date,
                            end_date=end_date,
                        )
                    )
                continue

            all_rows.extend(rows)
            if attempt_records is not None:
                attempt_records.append(
                    cex_attempt_record(
                        token_symbol,
                        exchange,
                        cex_symbol,
                        rows=rows,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

            print("Fetched %s on %s: %s rows" % (token_symbol, exchange, len(rows)))
            time.sleep(0.2)

    return all_rows


def select_stable_exchanges(
    rows,
    minimum_history_days=MIN_HISTORY_DAYS,
    minimum_exchange_count=MIN_EXCHANGE_COUNT,
    price_exchange=PRICE_EXCHANGE,
):
    """Select one fixed exchange set for each token."""
    dates_by_token_exchange = {}

    for row in rows:
        key = (row["token_symbol"], row["exchange"])

        if key not in dates_by_token_exchange:
            dates_by_token_exchange[key] = set()

        dates_by_token_exchange[key].add(row["date"])

    stable_by_token = {}

    for key in sorted(dates_by_token_exchange.keys()):
        token_symbol, exchange = key
        observation_days = len(dates_by_token_exchange[key])

        if observation_days < minimum_history_days:
            continue

        if token_symbol not in stable_by_token:
            stable_by_token[token_symbol] = []

        stable_by_token[token_symbol].append(exchange)

    selected_by_token = {}

    for token_symbol in sorted(stable_by_token.keys()):
        exchanges = sorted(stable_by_token[token_symbol])

        if len(exchanges) < minimum_exchange_count:
            continue
        if price_exchange not in exchanges:
            continue

        selected_by_token[token_symbol] = exchanges

    return selected_by_token


def build_coverage_rows(
    rows,
    stable_exchanges_by_token,
    token_symbols=None,
    exchanges=None,
):
    """Summarize raw observation coverage for each token and exchange."""
    dates_by_token_exchange = {}

    for row in rows:
        key = (row["token_symbol"], row["exchange"])

        if key not in dates_by_token_exchange:
            dates_by_token_exchange[key] = set()

        dates_by_token_exchange[key].add(row["date"])

    coverage_keys = set(dates_by_token_exchange.keys())

    if token_symbols is not None and exchanges is not None:
        for token_symbol in token_symbols:
            for exchange in exchanges:
                coverage_keys.add((token_symbol, exchange))

    coverage_rows = []

    for key in sorted(coverage_keys):
        token_symbol, exchange = key
        dates = sorted(dates_by_token_exchange.get(key, set()))
        selected_exchanges = stable_exchanges_by_token.get(token_symbol, [])

        first_date = ""
        last_date = ""

        if dates:
            first_date = dates[0]
            last_date = dates[-1]

        coverage_rows.append(
            {
                "token_symbol": token_symbol,
                "exchange": exchange,
                "observation_days": len(dates),
                "first_date": first_date,
                "last_date": last_date,
                "is_selected": 1 if exchange in selected_exchanges else 0,
            }
        )

    return coverage_rows


def aggregate_cex_rows(
    rows,
    required_exchange_count=None,
    stable_exchanges_by_token=None,
):
    """Aggregate exchange-level rows into token-date CEX volume rows."""
    grouped_rows = {}

    for row in rows:
        token_symbol = row["token_symbol"]

        if stable_exchanges_by_token is not None:
            selected_exchanges = stable_exchanges_by_token.get(token_symbol, [])

            if row["exchange"] not in selected_exchanges:
                continue

        key = (row["date"], row["token_symbol"])

        if key not in grouped_rows:
            grouped_rows[key] = {
                "date": row["date"],
                "token_symbol": row["token_symbol"],
                "close": None,
                "cex_volume_usd": 0.0,
                "exchanges": set(),
            }

        grouped = grouped_rows[key]
        grouped["cex_volume_usd"] = grouped["cex_volume_usd"] + row["quote_volume_usd"]
        grouped["exchanges"].add(row["exchange"])

        if row["exchange"] == "binance":
            grouped["close"] = row["close"]

    output_rows = []

    for key in sorted(grouped_rows.keys()):
        grouped = grouped_rows[key]
        exchanges = sorted(grouped["exchanges"])

        if stable_exchanges_by_token is not None:
            selected_exchanges = stable_exchanges_by_token.get(
                grouped["token_symbol"],
                [],
            )

            if exchanges != selected_exchanges:
                continue

        if required_exchange_count is not None:
            if len(exchanges) < required_exchange_count:
                continue

        output_row = {
            "date": grouped["date"],
            "token_symbol": grouped["token_symbol"],
            "close": grouped["close"],
            "cex_volume_usd": grouped["cex_volume_usd"],
            "exchange_count": len(exchanges),
            "included_exchanges": ";".join(exchanges),
        }

        output_rows.append(output_row)

    return output_rows


def write_exchange_rows(rows, output_path: Path):
    """Write exchange-level output CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
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
    ]

    rows = sorted(rows, key=lambda row: (row["token_symbol"], row["exchange"], row["date"]))

    serializable_rows = []
    for row in rows:
        serializable = dict(row)
        serializable.pop("source_instrument", None)
        serializable_rows.append(serializable)
    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(serializable_rows)


def write_aggregated_rows(rows, output_path: Path):
    """Write token-date aggregated CEX output CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "date",
        "token_symbol",
        "close",
        "cex_volume_usd",
        "exchange_count",
        "included_exchanges",
    ]

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_coverage_rows(rows, output_path: Path):
    """Write token-exchange coverage information."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "token_symbol",
        "exchange",
        "observation_days",
        "first_date",
        "last_date",
        "is_selected",
    ]

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main(
    token_symbols=None,
    exchanges=None,
    append=False,
    start_date=None,
    end_date=None,
    limit_days=LIMIT_DAYS,
    output_dir=None,
) -> None:
    """Fetch CEX data into processed CSV files."""
    global LIMIT_DAYS
    LIMIT_DAYS = limit_days
    resolved_output_dir = (
        Path(output_dir)
        if output_dir is not None
        else EXCHANGE_OUTPUT_PATH.parent
    )
    exchange_output_path = resolved_output_dir / EXCHANGE_OUTPUT_PATH.name
    coverage_output_path = resolved_output_dir / COVERAGE_OUTPUT_PATH.name
    output_path = resolved_output_dir / OUTPUT_PATH.name
    attempt_output_path = resolved_output_dir / ATTEMPT_OUTPUT_PATH.name
    token_rows = filter_token_rows(read_token_config(TOKEN_CONFIG_PATH), token_symbols)
    selected_exchanges = exchanges or EXCHANGES
    unknown_exchanges = sorted(set(selected_exchanges) - set(EXCHANGES))
    if unknown_exchanges:
        raise ValueError("Unsupported exchanges: %s" % ", ".join(unknown_exchanges))
    get_request_window(limit_days, start_date, end_date)
    publish_attempts = start_date is not None and end_date is not None
    attempt_records = [] if publish_attempts else None
    rows = build_rows(
        token_rows,
        selected_exchanges,
        attempt_records=attempt_records,
        start_date=start_date,
        end_date=end_date,
    )
    rows = [
        row
        for row in rows
        if (start_date is None or row["date"] >= start_date)
        and (end_date is None or row["date"] <= end_date)
    ]
    if append:
        if token_symbols is None:
            raise ValueError("--append requires --tokens")
        rows = merge_exchange_rows(read_exchange_rows(exchange_output_path), rows)
    stable_exchanges = select_stable_exchanges(
        rows,
        minimum_exchange_count=min(MIN_EXCHANGE_COUNT, len(selected_exchanges)),
    )
    token_symbols = []

    for token in token_rows:
        token_symbols.append(token["token_symbol"])

    coverage_rows = build_coverage_rows(
        rows,
        stable_exchanges,
        token_symbols=token_symbols,
        exchanges=selected_exchanges,
    )
    aggregated_rows = aggregate_cex_rows(
        rows,
        stable_exchanges_by_token=stable_exchanges,
    )

    write_exchange_rows(rows, exchange_output_path)
    write_coverage_rows(coverage_rows, coverage_output_path)
    write_aggregated_rows(aggregated_rows, output_path)
    if publish_attempts:
        write_attempt_ledger(
            attempt_output_path,
            attempt_records,
            source_csv=exchange_output_path,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        attempt_output_path.unlink(missing_ok=True)

    print("Wrote %s rows to %s" % (len(rows), exchange_output_path))
    print("Wrote %s rows to %s" % (len(coverage_rows), coverage_output_path))
    print("Wrote %s rows to %s" % (len(aggregated_rows), output_path))
    print(
        "Wrote %s collection attempts to %s"
        % (len(attempt_records or []), attempt_output_path)
    )


def parse_args():
    """Parse optional token and exchange subsets for reproducible refreshes."""
    parser = argparse.ArgumentParser(description="Fetch daily CEX market facts")
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--exchanges", help="Comma-separated exchange names")
    parser.add_argument("--append", action="store_true", help="Upsert selected Token rows")
    parser.add_argument("--start", help="Inclusive UTC date")
    parser.add_argument("--end", help="Inclusive UTC date")
    parser.add_argument("--limit-days", type=int, default=LIMIT_DAYS)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    tokens = [item.strip().upper() for item in (args.tokens or "").split(",") if item.strip()] or None
    exchanges = [item.strip().lower() for item in (args.exchanges or "").split(",") if item.strip()] or None
    return (
        tokens,
        exchanges,
        args.append,
        args.start,
        args.end,
        args.limit_days,
        args.output_dir,
    )


if __name__ == "__main__":
    (
        selected_tokens,
        selected_exchanges,
        append_rows,
        start_date,
        end_date,
        limit_days,
        selected_output_dir,
    ) = parse_args()
    main(
        selected_tokens,
        selected_exchanges,
        append_rows,
        start_date,
        end_date,
        limit_days,
        selected_output_dir,
    )
