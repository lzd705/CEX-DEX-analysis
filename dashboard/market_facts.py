"""Pure market-fact contracts shared by the API and known-answer tests."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


CATALOG_VERSION = 1
DAILY_GRAIN = "1 day, UTC"
PRICE_QUOTE_ASSET = "USD"
MISSING_VALUE_RULE = (
    "Preserve missing source values as null; do not forward-fill or replace them "
    "with zero. Compare prices only when both selected markets have a finite close "
    "on the same UTC date."
)


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


def dex_market_id(chain: str, dex: str, pool_address: str) -> str:
    return f"dex:{chain}:{dex}:{pool_address}"


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
        "semantic_boundary": (
            "These are daily OHLCV facts. They are not order-book depth, quoted "
            "bid/ask spread, executable price, or measured slippage."
        ),
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
                "observed_start": row["price_points"][0]["date"] if row["price_points"] else None,
                "observed_end": row["latest_date"],
                "observation_days": row["observation_days"],
            }
        )
    for row in payload["dex_pools"]:
        markets.append(
            {
                "market_id": dex_market_id(
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
                "observed_start": row["price_points"][0]["date"] if row["price_points"] else None,
                "observed_end": row["latest_date"],
                "observation_days": row["observation_days"],
            }
        )
    metadata = {
        **catalog_contract(),
        "available_start": payload["metadata"]["available_start"],
        "available_end": payload["metadata"]["available_end"],
        "sources": payload["metadata"]["sources"],
        "storage": payload["metadata"]["storage"],
        "cex_normalization_note": (
            "The displayed instrument is the configured canonical pair label. "
            "Adapters normalize source prices and volume to USD; USDT pairs use a "
            "1 USDT = 1 USD proxy, and venue-native raw pair labels may differ."
        ),
    }
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
