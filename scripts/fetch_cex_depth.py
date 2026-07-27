"""Collect auditable point-in-time CEX order-book depth snapshots.

Depth is computed from public spot order books, never inferred from daily
volume.  For each published CEX market the collector stores:

- best bid/ask, midpoint, and quoted spread;
- bid and ask quote notional inside 10/25/50/100 bps from midpoint;
- conservative completeness flags for every band;
- source instrument, quote conversion, endpoint, timestamps, and raw hashes;
- an explicit observed/partial/failed status for every cataloged market.

Runtime data is intentionally separate from the historical OHLCV database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

try:
    from scripts.fetch_cex import (
        make_binance_symbol,
        make_bitget_symbol,
        make_bybit_symbol,
        make_coinbase_product_id,
        make_crypto_com_instrument,
        make_gate_currency_pair,
        make_htx_symbol,
        make_kraken_pair,
        make_kucoin_symbol,
        make_mexc_symbol,
        make_okx_inst_id,
        make_upbit_market_candidates,
    )
except ModuleNotFoundError:
    from fetch_cex import (
        make_binance_symbol,
        make_bitget_symbol,
        make_bybit_symbol,
        make_coinbase_product_id,
        make_crypto_com_instrument,
        make_gate_currency_pair,
        make_htx_symbol,
        make_kraken_pair,
        make_kucoin_symbol,
        make_mexc_symbol,
        make_okx_inst_id,
        make_upbit_market_candidates,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "data/local/market_facts.sqlite3"
DEFAULT_CEX_CSV = PROJECT_ROOT / "data/local/cex_exchange_volume_daily.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/cex-depth"
TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

CURRENT_FILENAME = "cex_depth_snapshot.csv"
LATEST_FILENAME = "cex_depth_latest.csv"
HISTORY_FILENAME = "cex_depth_history.csv"
DEPTH_BANDS_BPS = (10, 25, 50, 100)
REQUEST_SLEEP_SECONDS = 0.15
MAX_RETRIES = 3

# Public REST limits differ by venue.  A completeness flag prevents a limited
# response from being represented as the venue's complete depth.
REQUESTED_LEVELS = {
    "binance": 100,
    "okx": 400,
    "bybit": 1000,
    "kucoin": 100,
    "gate": 100,
    "bitget": 150,
    "mexc": 100,
    "htx": 150,
    "coinbase": 0,  # level=2 is the full aggregated book
    "kraken": 500,
    "crypto_com": 50,
    "upbit": 30,
}

BASE_COLUMNS = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "quote_to_usd",
    "quote_conversion_method",
    "quote_conversion_endpoint",
    "quote_conversion_response_sha256",
    "best_bid",
    "best_ask",
    "midpoint",
    "spread_quote",
    "spread_bps",
    "bid_levels_returned",
    "ask_levels_returned",
    "requested_level_limit",
    "full_book_reported",
]
DEPTH_COLUMNS = [
    field
    for band in DEPTH_BANDS_BPS
    for field in (
        f"bid_depth_{band}bps_usd",
        f"ask_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
]
AUDIT_COLUMNS = [
    "depth_method",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
    "status",
    "error",
]
DEPTH_COLUMNS_ALL = BASE_COLUMNS + DEPTH_COLUMNS + AUDIT_COLUMNS


class SourceBookError(RuntimeError):
    """Carry a source response that could not be normalized as a valid book."""

    def __init__(
        self,
        message: str,
        *,
        raw: bytes = b"",
        endpoint: str = "",
        source_instrument: str = "",
        source_quote_asset: str = "",
    ) -> None:
        super().__init__(message)
        self.raw = raw
        self.endpoint = endpoint
        self.source_instrument = source_instrument
        self.source_quote_asset = source_quote_asset


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite_decimal(value: Any, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError(f"Invalid numeric order-book value: {value}") from error
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ValueError(f"Invalid order-book value: {value}")
    return number


def decimal_text(value: Decimal | str | int | float | None) -> str:
    if value is None:
        return ""
    number = finite_decimal(value)
    return format(number, "f")


def timestamp_text(value: Any) -> str:
    """Normalize milliseconds, seconds, or an ISO timestamp to UTC text."""
    if value is None or str(value).strip() == "":
        return ""
    text = str(value).strip()
    try:
        numeric = float(text)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    seconds = numeric / 1000 if numeric > 10_000_000_000 else numeric
    try:
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return text


def load_markets_from_database(database_path: Path) -> list[dict[str, str]]:
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT token_symbol, exchange, cex_symbol
            FROM cex_market_daily
            WHERE rowid IN (
                SELECT MAX(rowid)
                FROM cex_market_daily
                GROUP BY token_symbol, exchange
            )
            ORDER BY exchange, token_symbol, cex_symbol
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def load_markets_from_csv(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"token_symbol", "exchange", "cex_symbol"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{csv_path.name} is missing columns: {', '.join(missing)}")
        unique = {}
        for row in reader:
            if not (
                row.get("token_symbol")
                and row.get("exchange")
                and row.get("cex_symbol")
            ):
                continue
            key = (
                row["token_symbol"].upper(),
                row["exchange"].lower(),
            )
            unique[key] = {
                "token_symbol": row["token_symbol"].upper(),
                "exchange": row["exchange"].lower(),
                "cex_symbol": row["cex_symbol"].upper(),
            }
    return [unique[key] for key in sorted(unique, key=lambda item: (item[1], item[0]))]


def load_cataloged_markets(
    database_path: Path = DEFAULT_DATABASE,
    csv_path: Path = DEFAULT_CEX_CSV,
) -> list[dict[str, str]]:
    if database_path.exists():
        rows = load_markets_from_database(database_path)
    elif csv_path.exists():
        rows = load_markets_from_csv(csv_path)
    else:
        raise FileNotFoundError(
            f"No published CEX market inventory found at {database_path} or {csv_path}"
        )
    if not rows:
        raise ValueError("Published CEX market inventory contains no markets")
    return rows


def query_url(base: str, params: dict[str, Any]) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def request_json(url: str) -> tuple[Any, bytes]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "CEX-DEX-Market-Monitor/1.0",
        },
    )
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=30, context=TLS_CONTEXT) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8")), raw
        except urllib.error.HTTPError as error:
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt + 1 >= MAX_RETRIES:
                raise
            retry_after = error.headers.get("Retry-After")
            time.sleep(max(2.0, float(retry_after or 0), 2 ** attempt))
        except urllib.error.URLError:
            if attempt + 1 >= MAX_RETRIES:
                raise
            time.sleep(max(2.0, 2 ** attempt))
    raise RuntimeError(f"Failed after retries: {url}")


def source_request(exchange: str, cex_symbol: str) -> tuple[str, str, str, bool]:
    """Return endpoint, source instrument, quote asset, and full-book flag."""
    if exchange == "binance":
        instrument = make_binance_symbol(cex_symbol)
        return (
            query_url(
                "https://data-api.binance.vision/api/v3/depth",
                {"symbol": instrument, "limit": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "okx":
        instrument = make_okx_inst_id(cex_symbol)
        return (
            query_url(
                "https://www.okx.com/api/v5/market/books",
                {"instId": instrument, "sz": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "bybit":
        instrument = make_bybit_symbol(cex_symbol)
        return (
            query_url(
                "https://api.bybit.com/v5/market/orderbook",
                {
                    "category": "spot",
                    "symbol": instrument,
                    "limit": REQUESTED_LEVELS[exchange],
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "kucoin":
        instrument = make_kucoin_symbol(cex_symbol)
        return (
            query_url(
                "https://api.kucoin.com/api/v1/market/orderbook/level2_100",
                {"symbol": instrument},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "gate":
        instrument = make_gate_currency_pair(cex_symbol)
        return (
            query_url(
                "https://api.gateio.ws/api/v4/spot/order_book",
                {
                    "currency_pair": instrument,
                    "limit": REQUESTED_LEVELS[exchange],
                    "with_id": "true",
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "bitget":
        instrument = make_bitget_symbol(cex_symbol)
        return (
            query_url(
                "https://api.bitget.com/api/v2/spot/market/orderbook",
                {
                    "symbol": instrument,
                    "type": "step0",
                    "limit": REQUESTED_LEVELS[exchange],
                },
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "mexc":
        instrument = make_mexc_symbol(cex_symbol)
        return (
            query_url(
                "https://api.mexc.com/api/v3/depth",
                {"symbol": instrument, "limit": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "htx":
        instrument = make_htx_symbol(cex_symbol)
        return (
            query_url(
                "https://api.huobi.pro/market/depth",
                {"symbol": instrument, "type": "step0"},
            ),
            instrument,
            "USDT",
            False,
        )
    if exchange == "coinbase":
        instrument = make_coinbase_product_id(cex_symbol)
        return (
            query_url(
                f"https://api.exchange.coinbase.com/products/{instrument}/book",
                {"level": 2},
            ),
            instrument,
            "USD",
            True,
        )
    if exchange == "kraken":
        instrument = make_kraken_pair(cex_symbol)
        return (
            query_url(
                "https://api.kraken.com/0/public/Depth",
                {"pair": instrument, "count": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USD",
            False,
        )
    if exchange == "crypto_com":
        instrument = make_crypto_com_instrument(cex_symbol)
        return (
            query_url(
                "https://api.crypto.com/exchange/v1/public/get-book",
                {"instrument_name": instrument, "depth": REQUESTED_LEVELS[exchange]},
            ),
            instrument,
            "USDT",
            False,
        )
    raise ValueError(f"Unsupported exchange: {exchange}")


def parse_book(
    exchange: str,
    payload: Any,
    *,
    requested_instrument: str,
) -> dict[str, Any]:
    """Normalize a successful venue response to price/quantity levels."""
    instrument = requested_instrument
    sequence = ""
    observed_at = ""
    if exchange in {"binance", "mexc"}:
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"{exchange} returned no order book")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("lastUpdateId") or "")
    elif exchange == "okx":
        if payload.get("code") != "0" or not payload.get("data"):
            raise ValueError(f"OKX returned no order book: {payload}")
        book = payload["data"][0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("seqId") or "")
        observed_at = timestamp_text(book.get("ts"))
    elif exchange == "bybit":
        if payload.get("retCode") != 0 or not payload.get("result"):
            raise ValueError(f"Bybit returned no order book: {payload}")
        book = payload["result"]
        bids, asks = book.get("b", []), book.get("a", [])
        instrument = str(book.get("s") or instrument)
        sequence = str(book.get("u") or book.get("seq") or "")
        observed_at = timestamp_text(book.get("cts") or book.get("ts"))
    elif exchange == "kucoin":
        if payload.get("code") != "200000" or not payload.get("data"):
            raise ValueError(f"KuCoin returned no order book: {payload}")
        book = payload["data"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("sequence") or "")
        observed_at = timestamp_text(book.get("time"))
    elif exchange == "gate":
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"Gate returned no order book: {payload}")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("id") or payload.get("order_book_id") or "")
        observed_at = timestamp_text(payload.get("update") or payload.get("current"))
    elif exchange == "bitget":
        if payload.get("code") != "00000" or not payload.get("data"):
            raise ValueError(f"Bitget returned no order book: {payload}")
        book = payload["data"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        observed_at = timestamp_text(book.get("ts"))
    elif exchange == "htx":
        if payload.get("status") != "ok" or not payload.get("tick"):
            raise ValueError(f"HTX returned no order book: {payload}")
        book = payload["tick"]
        bids, asks = book.get("bids", []), book.get("asks", [])
        sequence = str(book.get("version") or "")
        observed_at = timestamp_text(book.get("ts") or payload.get("ts"))
    elif exchange == "coinbase":
        if not isinstance(payload, dict) or "bids" not in payload or "asks" not in payload:
            raise ValueError(f"Coinbase returned no order book: {payload}")
        bids, asks = payload["bids"], payload["asks"]
        sequence = str(payload.get("sequence") or "")
        observed_at = timestamp_text(payload.get("time"))
    elif exchange == "kraken":
        if payload.get("error"):
            raise ValueError(f"Kraken returned an error: {payload['error']}")
        result = payload.get("result") or {}
        books = [(key, value) for key, value in result.items() if isinstance(value, dict)]
        if not books:
            raise ValueError(f"Kraken returned no order book: {payload}")
        instrument, book = books[0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        timestamps = [
            level[2]
            for level in list(bids) + list(asks)
            if isinstance(level, (list, tuple)) and len(level) > 2
        ]
        observed_at = timestamp_text(max(timestamps)) if timestamps else ""
    elif exchange == "crypto_com":
        if payload.get("code") != 0 or not payload.get("result"):
            raise ValueError(f"Crypto.com returned no order book: {payload}")
        result = payload["result"]
        data = result.get("data") or []
        if not data:
            raise ValueError(f"Crypto.com returned no order book: {payload}")
        book = data[0]
        bids, asks = book.get("bids", []), book.get("asks", [])
        instrument = str(result.get("instrument_name") or instrument)
        sequence = str(book.get("u") or "")
        observed_at = timestamp_text(book.get("tt") or book.get("t"))
    elif exchange == "upbit":
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Upbit returned no order book: {payload}")
        book = payload[0]
        units = book.get("orderbook_units") or []
        bids = [[unit.get("bid_price"), unit.get("bid_size")] for unit in units]
        asks = [[unit.get("ask_price"), unit.get("ask_size")] for unit in units]
        instrument = str(book.get("market") or instrument)
        observed_at = timestamp_text(book.get("timestamp"))
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

    def normalized_levels(values: Iterable[Any], *, reverse: bool) -> list[tuple[Decimal, Decimal]]:
        levels: dict[Decimal, Decimal] = {}
        for level in values:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = finite_decimal(level[0])
            quantity = finite_decimal(level[1])
            # Some venue snapshots retain a placeholder on one side with a
            # zero price or size. It is not resting liquidity and must be
            # ignored rather than counted or allowed to invalidate valid levels.
            if price == 0 or quantity == 0:
                continue
            levels[price] = levels.get(price, Decimal(0)) + quantity
        return sorted(levels.items(), key=lambda item: item[0], reverse=reverse)

    normalized_bids = normalized_levels(bids, reverse=True)
    normalized_asks = normalized_levels(asks, reverse=False)
    if not normalized_bids or not normalized_asks:
        raise ValueError(f"{exchange} returned an empty order-book side")
    if normalized_bids[0][0] >= normalized_asks[0][0]:
        raise ValueError(f"{exchange} returned a crossed or locked order book")
    return {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "source_instrument": instrument,
        "source_sequence": sequence,
        "source_observed_at": observed_at,
    }


def upbit_book(
    cex_symbol: str,
    request: Callable[[str], tuple[Any, bytes]],
) -> dict[str, Any]:
    last_error: Exception | None = None
    for market in make_upbit_market_candidates(cex_symbol):
        raw = b""
        url = query_url(
            "https://api.upbit.com/v1/orderbook",
            {"markets": market, "count": REQUESTED_LEVELS["upbit"]},
        )
        try:
            payload, raw = request(url)
            parsed = parse_book("upbit", payload, requested_instrument=market)
        except Exception as error:
            last_error = (
                error
                if isinstance(error, SourceBookError)
                else SourceBookError(
                    str(error),
                    raw=raw,
                    endpoint=url,
                    source_instrument=market,
                    source_quote_asset=market.split("-", 1)[0].upper(),
                )
            )
            continue

        quote_asset = market.split("-", 1)[0].upper()
        result = {
            **parsed,
            "source_endpoint": url,
            "raw": raw,
            "source_quote_asset": quote_asset,
            "quote_to_usd": Decimal(1),
            "quote_conversion_method": (
                "USDT=USD proxy" if quote_asset == "USDT" else "Upbit KRW-USDT midpoint"
            ),
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
        }
        if quote_asset == "USDT":
            return result
        if quote_asset != "KRW":
            last_error = ValueError(f"Unsupported Upbit quote asset: {quote_asset}")
            continue

        fx_url = query_url(
            "https://api.upbit.com/v1/orderbook",
            {"markets": "KRW-USDT", "count": 1},
        )
        try:
            fx_payload, fx_raw = request(fx_url)
            fx_book = parse_book("upbit", fx_payload, requested_instrument="KRW-USDT")
            krw_per_usdt = (fx_book["bids"][0][0] + fx_book["asks"][0][0]) / Decimal(2)
            result["quote_to_usd"] = Decimal(1) / krw_per_usdt
            result["quote_conversion_endpoint"] = fx_url
            result["quote_conversion_response_sha256"] = hashlib.sha256(fx_raw).hexdigest()
            result["quote_conversion_raw"] = fx_raw
            return result
        except Exception as error:
            last_error = SourceBookError(
                f"Failed KRW quote conversion: {error}",
                raw=raw,
                endpoint=url,
                source_instrument=market,
                source_quote_asset=quote_asset,
            )
            continue
    if isinstance(last_error, SourceBookError):
        raise last_error
    raise RuntimeError(f"Failed to fetch Upbit order book for {cex_symbol}: {last_error}")


def fetch_source_book(
    exchange: str,
    cex_symbol: str,
    *,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
) -> dict[str, Any]:
    if exchange == "upbit":
        return upbit_book(cex_symbol, request)
    url, instrument, quote_asset, full_book = source_request(exchange, cex_symbol)
    payload, raw = request(url)
    try:
        parsed = parse_book(exchange, payload, requested_instrument=instrument)
    except Exception as error:
        raise SourceBookError(
            str(error),
            raw=raw,
            endpoint=url,
            source_instrument=instrument,
            source_quote_asset=quote_asset,
        ) from error
    return {
        **parsed,
        "source_endpoint": url,
        "raw": raw,
        "source_quote_asset": quote_asset,
        "quote_to_usd": Decimal(1),
        "quote_conversion_method": f"{quote_asset}=USD proxy",
        "quote_conversion_endpoint": "",
        "quote_conversion_response_sha256": "",
        "full_book_reported": full_book,
    }


def depth_metrics(
    bids: list[tuple[Decimal, Decimal]],
    asks: list[tuple[Decimal, Decimal]],
    *,
    quote_to_usd: Decimal,
    requested_limit: int,
    full_book_reported: bool,
) -> dict[str, str]:
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    midpoint = (best_bid + best_ask) / Decimal(2)
    spread_quote = best_ask - best_bid
    result = {
        "best_bid": decimal_text(best_bid),
        "best_ask": decimal_text(best_ask),
        "midpoint": decimal_text(midpoint),
        "spread_quote": decimal_text(spread_quote),
        "spread_bps": decimal_text(spread_quote / midpoint * Decimal(10_000)),
        "bid_levels_returned": str(len(bids)),
        "ask_levels_returned": str(len(asks)),
        "requested_level_limit": str(requested_limit),
        "full_book_reported": "1" if full_book_reported else "0",
    }
    for band in DEPTH_BANDS_BPS:
        fraction = Decimal(band) / Decimal(10_000)
        bid_boundary = midpoint * (Decimal(1) - fraction)
        ask_boundary = midpoint * (Decimal(1) + fraction)
        bid_depth = sum(
            price * quantity * quote_to_usd
            for price, quantity in bids
            if price >= bid_boundary
        )
        ask_depth = sum(
            price * quantity * quote_to_usd
            for price, quantity in asks
            if price <= ask_boundary
        )
        bid_complete = full_book_reported or bids[-1][0] <= bid_boundary
        ask_complete = full_book_reported or asks[-1][0] >= ask_boundary
        result[f"bid_depth_{band}bps_usd"] = decimal_text(bid_depth)
        result[f"ask_depth_{band}bps_usd"] = decimal_text(ask_depth)
        result[f"total_depth_{band}bps_usd"] = decimal_text(bid_depth + ask_depth)
        result[f"depth_{band}bps_complete"] = "1" if bid_complete and ask_complete else "0"
    return result


def base_row(
    market: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = {column: "" for column in DEPTH_COLUMNS_ALL}
    row.update(
        {
            "snapshot_id": snapshot_id,
            "observed_at": response_received_at,
            "request_started_at": request_started_at,
            "response_received_at": response_received_at,
            "token_symbol": market["token_symbol"].upper(),
            "exchange": market["exchange"].lower(),
            "cex_symbol": market["cex_symbol"].upper(),
            "base_asset": market["cex_symbol"].split("/", 1)[0].upper(),
            "depth_method": "midpoint_symmetric_quote_notional",
            "source": f"{market['exchange']} public spot order-book API",
        }
    )
    return row


def observed_row(
    market: dict[str, str],
    book: dict[str, Any],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = base_row(
        market,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    requested_limit = REQUESTED_LEVELS[market["exchange"]]
    full_book_reported = bool(book.get("full_book_reported"))
    row.update(
        depth_metrics(
            book["bids"],
            book["asks"],
            quote_to_usd=book["quote_to_usd"],
            requested_limit=requested_limit,
            full_book_reported=full_book_reported,
        )
    )
    row.update(
        {
            "observed_at": book.get("source_observed_at") or response_received_at,
            "source_instrument": book["source_instrument"],
            "source_quote_asset": book["source_quote_asset"],
            "quote_to_usd": decimal_text(book["quote_to_usd"]),
            "quote_conversion_method": book["quote_conversion_method"],
            "quote_conversion_endpoint": book.get("quote_conversion_endpoint", ""),
            "quote_conversion_response_sha256": book.get(
                "quote_conversion_response_sha256",
                "",
            ),
            "source_endpoint": book["source_endpoint"],
            "source_sequence": book.get("source_sequence", ""),
            "raw_response_sha256": hashlib.sha256(book["raw"]).hexdigest(),
        }
    )
    complete = all(row[f"depth_{band}bps_complete"] == "1" for band in DEPTH_BANDS_BPS)
    row["status"] = "observed" if complete else "partial"
    if not complete:
        incomplete = [
            str(band)
            for band in DEPTH_BANDS_BPS
            if row[f"depth_{band}bps_complete"] != "1"
        ]
        row["error"] = (
            "Returned levels do not prove complete coverage for "
            + "/".join(incomplete)
            + " bps; reported depth is an observed lower bound"
        )
    return row


def failure_row(
    market: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    error: Exception,
    source_endpoint: str = "",
    source_instrument: str = "",
    source_quote_asset: str = "",
    raw_response_sha256: str = "",
) -> dict[str, str]:
    row = base_row(
        market,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    row["source_endpoint"] = source_endpoint
    row["source_instrument"] = source_instrument
    row["source_quote_asset"] = source_quote_asset
    row["raw_response_sha256"] = raw_response_sha256
    row["requested_level_limit"] = str(REQUESTED_LEVELS[market["exchange"]])
    row["status"] = "failed"
    row["error"] = f"{type(error).__name__}: {error}"
    return row


def safe_component(value: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in value).strip("-")


def collect_depth(
    markets: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    request: Callable[[str], tuple[Any, bytes]] = request_json,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
) -> tuple[str, list[dict[str, str]]]:
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    snapshot_raw_dir = raw_root / snapshot_id
    snapshot_raw_dir.mkdir(parents=True, exist_ok=False)
    rows: list[dict[str, str]] = []

    for index, market in enumerate(markets, start=1):
        request_started_at = utc_now_text()
        raw_path = snapshot_raw_dir / (
            f"{index:03d}-{safe_component(market['exchange'])}-"
            f"{safe_component(market['token_symbol'])}.json"
        )
        try:
            book = fetch_source_book(
                market["exchange"],
                market["cex_symbol"],
                request=request,
            )
            response_received_at = utc_now_text()
            raw_path.write_bytes(book["raw"])
            if book.get("quote_conversion_raw"):
                raw_path.with_name(raw_path.stem + "-quote-conversion.json").write_bytes(
                    book["quote_conversion_raw"]
                )
            row = observed_row(
                market,
                book,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
            )
        except Exception as error:
            response_received_at = utc_now_text()
            source_endpoint = ""
            source_instrument = ""
            source_quote_asset = ""
            source_raw = b""
            if isinstance(error, SourceBookError):
                source_endpoint = error.endpoint
                source_instrument = error.source_instrument
                source_quote_asset = error.source_quote_asset
                source_raw = error.raw
            elif market["exchange"] != "upbit":
                try:
                    (
                        source_endpoint,
                        source_instrument,
                        source_quote_asset,
                        _full_book,
                    ) = source_request(market["exchange"], market["cex_symbol"])
                except ValueError:
                    pass
            if source_raw:
                raw_path.write_bytes(source_raw)
                raw_response_sha256 = hashlib.sha256(source_raw).hexdigest()
            else:
                error_payload = json.dumps(
                    {
                        "market": market,
                        "source_endpoint": source_endpoint,
                        "request_started_at": request_started_at,
                        "response_received_at": response_received_at,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                    ensure_ascii=False,
                    indent=2,
                ).encode("utf-8")
                raw_path.write_bytes(error_payload)
                raw_response_sha256 = ""
            row = failure_row(
                market,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                error=error,
                source_endpoint=source_endpoint,
                source_instrument=source_instrument,
                source_quote_asset=source_quote_asset,
                raw_response_sha256=raw_response_sha256,
            )
        rows.append(row)
        print(
            f"[{index}/{len(markets)}] {market['token_symbol']} "
            f"{market['exchange']}: {row['status']}",
            flush=True,
        )
        if index < len(markets) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now_text(),
        "market_count": len(rows),
        "token_count": len({row["token_symbol"] for row in rows}),
        "exchange_count": len({row["exchange"] for row in rows}),
        "depth_bands_bps": list(DEPTH_BANDS_BPS),
        "status_counts": {
            status: sum(row["status"] == status for row in rows)
            for status in ("observed", "partial", "failed")
        },
        "raw_files": sorted(path.name for path in snapshot_raw_dir.glob("*.json")),
    }
    (snapshot_raw_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validate_snapshot(markets, rows)
    return snapshot_id, rows


def validate_snapshot(
    inventory: list[dict[str, str]],
    rows: list[dict[str, str]],
) -> None:
    expected = {
        (row["token_symbol"].upper(), row["exchange"].lower(), row["cex_symbol"].upper())
        for row in inventory
    }
    actual = {
        (row["token_symbol"].upper(), row["exchange"].lower(), row["cex_symbol"].upper())
        for row in rows
    }
    if len(rows) != len(actual):
        raise ValueError("CEX depth snapshot contains duplicate market rows")
    if expected != actual:
        raise ValueError("CEX depth snapshot coverage does not match the published inventory")
    if any(row["status"] not in {"observed", "partial", "failed"} for row in rows):
        raise ValueError("CEX depth snapshot contains an invalid status")
    if not any(row["status"] in {"observed", "partial"} for row in rows):
        raise ValueError("CEX depth snapshot contains no observed order books")
    for row in rows:
        if row["status"] not in {"observed", "partial"}:
            continue
        if float(row["best_bid"]) >= float(row["best_ask"]):
            raise ValueError("CEX depth snapshot contains a crossed or locked book")
        for band in DEPTH_BANDS_BPS:
            for side in ("bid", "ask", "total"):
                value = float(row[f"{side}_depth_{band}bps_usd"])
                if not math.isfinite(value) or value < 0:
                    raise ValueError("CEX depth snapshot contains invalid depth")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DEPTH_COLUMNS_ALL, lineterminator="\n")
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in DEPTH_COLUMNS_ALL}
                for row in rows
            )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_snapshot(
    rows: list[dict[str, str]],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)
    result: dict[str, Any] = {"current_path": str(current_path), "row_count": len(rows)}
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    history_path = publish_dir / HISTORY_FILENAME
    existing_history = read_csv_rows(history_path)
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ): row
        for row in existing_history
    }
    for row in rows:
        merged[
            (
                row["snapshot_id"],
                row["token_symbol"],
                row["exchange"],
                row["cex_symbol"],
            )
        ] = row
    history_rows = sorted(
        merged.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("exchange", ""),
            row.get("cex_symbol", ""),
        ),
    )
    atomic_write_csv(history_path, history_rows)
    atomic_write_csv(publish_dir / LATEST_FILENAME, rows)
    shutil.copyfile(current_path, publish_dir / CURRENT_FILENAME)
    result.update(
        {
            "latest_path": str(publish_dir / LATEST_FILENAME),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
        }
    )
    return result


def parse_list(value: str | None, *, upper: bool) -> list[str] | None:
    if not value:
        return None
    transform = str.upper if upper else str.lower
    values = [transform(item.strip()) for item in value.split(",") if item.strip()]
    return values or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect point-in-time depth for all published CEX markets"
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--cex-csv", type=Path, default=DEFAULT_CEX_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--exchanges", help="Comma-separated exchange names")
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=REQUEST_SLEEP_SECONDS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    markets = load_cataloged_markets(args.database, args.cex_csv)
    tokens = set(parse_list(args.tokens, upper=True) or [])
    exchanges = set(parse_list(args.exchanges, upper=False) or [])
    if tokens:
        markets = [row for row in markets if row["token_symbol"] in tokens]
    if exchanges:
        markets = [row for row in markets if row["exchange"] in exchanges]
    if not markets:
        raise ValueError("No cataloged CEX markets match the requested filters")

    snapshot_id, rows = collect_depth(
        markets,
        raw_root=args.raw_root,
        sleep_seconds=max(0.0, args.sleep_seconds),
    )
    result = publish_snapshot(
        rows,
        output_dir=args.output_dir,
        publish_dir=DEFAULT_PUBLISH_DIR if args.publish_local else None,
    )
    result.update(
        {
            "snapshot_id": snapshot_id,
            "token_count": len({row["token_symbol"] for row in rows}),
            "exchange_count": len({row["exchange"] for row in rows}),
            "market_count": len(rows),
            "observed_count": sum(row["status"] == "observed" for row in rows),
            "partial_count": sum(row["status"] == "partial" for row in rows),
            "failed_count": sum(row["status"] == "failed" for row in rows),
        }
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
