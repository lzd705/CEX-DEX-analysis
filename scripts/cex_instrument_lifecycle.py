"""Validated current CEX instrument lifecycle evidence.

The manifest records only a bounded current-catalog observation. It does not
infer or publish an exact delisting date from catalog absence.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

try:
    from scripts.timestamp_contract import parse_rfc3339_utc
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from timestamp_contract import parse_rfc3339_utc


SCHEMA = "cex_instrument_lifecycle/v1"
MAX_MANIFEST_BYTES = 256 * 1024
MAX_REVIEWS = 1_000
ROOT_FIELDS = {
    "schema",
    "generated_at_utc",
    "checked_at_utc",
    "response_sha256",
    "inventory_count",
    "configured_market_count",
    "configured_market_ids_sha256",
    "review_count",
    "reviews",
}
REVIEW_FIELDS = {
    "market_id",
    "market_type",
    "token_symbol",
    "exchange",
    "instrument",
    "current_listing_status",
    "reason_code",
    "checked_at_utc",
    "source_url",
    "http_status",
    "response_sha256",
    "inventory_count",
    "instrument_present",
}
CURRENT_ABSENCE_STATUS = "absent_from_official_current_catalog"
CURRENT_ABSENCE_REASON = "instrument_absent_from_current_catalog"
OFFICIAL_CATALOG_ENDPOINTS = {
    "crypto_com": (
        "api.crypto.com",
        "/exchange/v1/public/get-instruments",
    ),
}
INSTRUMENT_PART = re.compile(r"^[A-Z0-9._-]{1,32}$")


def canonical_configured_market_ids(market_ids):
    """Return one exact sorted Crypto.com configured-market identity set."""
    if isinstance(market_ids, (str, bytes)):
        raise ValueError("configured market IDs must be an inventory")
    values = list(market_ids)
    if not values or len(values) > MAX_REVIEWS:
        raise ValueError("configured market ID inventory is invalid")
    canonical = []
    for market_id in values:
        if not isinstance(market_id, str) or market_id != market_id.strip():
            raise ValueError("configured market ID is invalid")
        prefix = "cex:crypto_com:"
        instrument = market_id[len(prefix):] if market_id.startswith(prefix) else ""
        parts = instrument.split("/")
        if (
            len(parts) != 2
            or not all(INSTRUMENT_PART.fullmatch(part) for part in parts)
        ):
            raise ValueError("configured market ID is invalid")
        canonical.append(market_id)
    if len(canonical) != len(set(canonical)):
        raise ValueError("configured market IDs must be unique")
    return tuple(sorted(canonical))


def configured_market_ids_sha256(market_ids):
    """Hash the exact canonical market-ID set using stable JSON bytes."""
    canonical = canonical_configured_market_ids(market_ids)
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_utc(value, field):
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("{} must be a canonical UTC timestamp".format(field))
    parsed = parse_rfc3339_utc(value)
    if parsed.isoformat() != value:
        raise ValueError("{} must use canonical UTC representation".format(field))
    return value


def _validate_source_url(exchange, value):
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("lifecycle source URL is invalid")
    parsed = urlsplit(value)
    expected = OFFICIAL_CATALOG_ENDPOINTS.get(exchange)
    if (
        expected is None
        or parsed.scheme != "https"
        or parsed.hostname != expected[0]
        or parsed.path != expected[1]
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
    ):
        raise ValueError("lifecycle source is not an approved official catalog")
    return value


def _validate_review(review):
    if not isinstance(review, dict) or set(review) != REVIEW_FIELDS:
        raise ValueError("CEX lifecycle review has missing or unknown fields")
    exchange = review["exchange"]
    token = review["token_symbol"]
    instrument = review["instrument"]
    instrument_parts = (
        instrument.split("/") if isinstance(instrument, str) else []
    )
    if (
        review["market_type"] != "cex"
        or not isinstance(exchange, str)
        or not re.fullmatch(r"[a-z0-9_]{2,32}", exchange)
        or not isinstance(token, str)
        or not INSTRUMENT_PART.fullmatch(token)
        or len(instrument_parts) != 2
        or not all(INSTRUMENT_PART.fullmatch(part) for part in instrument_parts)
        or instrument_parts[0] != token
        or review["market_id"] != "cex:{}:{}".format(exchange, instrument)
    ):
        raise ValueError("CEX lifecycle market identity is invalid")
    if (
        review["current_listing_status"] != CURRENT_ABSENCE_STATUS
        or review["reason_code"] != CURRENT_ABSENCE_REASON
        or review["instrument_present"] is not False
    ):
        raise ValueError("CEX lifecycle outcome is unsupported")
    _canonical_utc(review["checked_at_utc"], "checked_at_utc")
    _validate_source_url(exchange, review["source_url"])
    if review["http_status"] != 200:
        raise ValueError("CEX lifecycle evidence must be a successful response")
    digest = review["response_sha256"]
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("CEX lifecycle response hash is invalid")
    count = review["inventory_count"]
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("CEX lifecycle inventory count is invalid")
    return dict(review)


def _required_catalog_text(row, field):
    value = row.get(field)
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or value != value.upper()
    ):
        raise ValueError(
            "Crypto.com instrument catalog has invalid {}".format(field)
        )
    return value


def parse_crypto_com_inventory(raw):
    """Return exact tradable Crypto.com spot symbols and total catalog rows.

    ``raw`` may be the decoded response object used by the daily collector or
    the exact response bytes retained by the lifecycle collector. Derivative
    rows never establish spot-market presence. Every spot row must bind its
    symbol, display name, base, quote, type, and current tradability exactly.
    """
    if isinstance(raw, bytes):
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                "Crypto.com instrument catalog is not valid JSON"
            ) from error
    elif isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(
                "Crypto.com instrument catalog is not valid JSON"
            ) from error
    else:
        payload = raw
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError(
            "Crypto.com instrument catalog response was unsuccessful"
        )
    result = payload.get("result")
    rows = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError("Crypto.com instrument catalog is empty")

    spot_instruments = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Crypto.com instrument catalog row is invalid")
        symbol = _required_catalog_text(row, "symbol")
        instrument_type = _required_catalog_text(row, "inst_type")
        if instrument_type != "CCY_PAIR":
            continue
        base = _required_catalog_text(row, "base_ccy")
        quote = _required_catalog_text(row, "quote_ccy")
        display_name = _required_catalog_text(row, "display_name")
        if (
            not INSTRUMENT_PART.fullmatch(base)
            or not INSTRUMENT_PART.fullmatch(quote)
        ):
            raise ValueError(
                "Crypto.com instrument catalog asset identity is invalid"
            )
        tradable = row.get("tradable")
        if not isinstance(tradable, bool):
            raise ValueError(
                "Crypto.com spot instrument tradability is invalid"
            )
        if (
            symbol != base + "_" + quote
            or display_name != base + "/" + quote
            or tradable is not True
        ):
            # Namespaced venue variants and non-tradable rows are not the
            # exact canonical spot identity queried by this application.
            # Excluding them is fail-closed for target-market presence; an
            # inventory containing no canonical tradable spot remains invalid.
            continue
        if symbol in spot_instruments:
            raise ValueError(
                "Crypto.com spot instrument identity is duplicated"
            )
        spot_instruments.add(symbol)
    if not spot_instruments:
        raise ValueError("Crypto.com instrument catalog has no tradable spot markets")
    return spot_instruments, len(rows)


def load_cex_instrument_lifecycle_manifest(path):
    """Load and validate the complete root evidence and exact review list."""
    manifest_path = Path(path)
    if not manifest_path.is_file() or manifest_path.stat().st_size > MAX_MANIFEST_BYTES:
        raise ValueError("CEX lifecycle manifest is not a bounded regular file")
    raw = manifest_path.read_bytes()
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("CEX lifecycle manifest exceeds the size limit")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("CEX lifecycle manifest is not valid JSON") from error
    if not isinstance(payload, dict) or set(payload) != ROOT_FIELDS:
        raise ValueError("CEX lifecycle manifest root is invalid")
    if payload["schema"] != SCHEMA:
        raise ValueError("CEX lifecycle manifest schema is unsupported")
    generated_at = _canonical_utc(
        payload["generated_at_utc"], "generated_at_utc"
    )
    checked_at = _canonical_utc(payload["checked_at_utc"], "checked_at_utc")
    if generated_at != checked_at:
        raise ValueError("CEX lifecycle root timestamps are inconsistent")
    response_sha256 = payload["response_sha256"]
    if (
        not isinstance(response_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", response_sha256)
    ):
        raise ValueError("CEX lifecycle root response hash is invalid")
    inventory_count = payload["inventory_count"]
    configured_market_count = payload["configured_market_count"]
    configured_market_hash = payload["configured_market_ids_sha256"]
    if (
        isinstance(inventory_count, bool)
        or not isinstance(inventory_count, int)
        or inventory_count <= 0
    ):
        raise ValueError("CEX lifecycle root inventory count is invalid")
    if (
        isinstance(configured_market_count, bool)
        or not isinstance(configured_market_count, int)
        or configured_market_count <= 0
        or configured_market_count > MAX_REVIEWS
    ):
        raise ValueError("CEX lifecycle configured market count is invalid")
    if (
        not isinstance(configured_market_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", configured_market_hash)
    ):
        raise ValueError("CEX lifecycle configured market hash is invalid")
    reviews = payload["reviews"]
    if not isinstance(reviews, list) or len(reviews) > MAX_REVIEWS:
        raise ValueError("CEX lifecycle review inventory is invalid")
    if payload["review_count"] != len(reviews):
        raise ValueError("CEX lifecycle review count is inconsistent")
    if len(reviews) > configured_market_count:
        raise ValueError("CEX lifecycle reviews exceed configured inventory")
    by_market = {}
    for raw_review in reviews:
        review = _validate_review(raw_review)
        if (
            review["checked_at_utc"] != checked_at
            or review["response_sha256"] != response_sha256
            or review["inventory_count"] != inventory_count
        ):
            raise ValueError("CEX lifecycle review evidence differs from root")
        market_id = review["market_id"]
        if market_id in by_market:
            raise ValueError("CEX lifecycle market IDs must be unique")
        by_market[market_id] = review
    return {
        **payload,
        "reviews": [dict(item) for item in reviews],
    }


def load_cex_instrument_lifecycle(path):
    """Load an exact current-instrument review map keyed by canonical market ID."""
    payload = load_cex_instrument_lifecycle_manifest(path)
    by_market = {}
    for review in payload["reviews"]:
        by_market[review["market_id"]] = dict(review)
    return by_market
