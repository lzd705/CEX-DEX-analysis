"""Collect current Crypto.com spot-instrument lifecycle evidence.

The collector publishes only exact configured markets absent from the official
current spot catalog.  It never infers a delisting date.  Every HTTP response
is retained byte-for-byte under its SHA-256 before parsing, while network,
HTTP, parse, catalog, or manifest-validation failures leave the previously
published manifest untouched.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import ssl
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

try:
    import certifi
except ImportError:  # pragma: no cover - system CA remains the safe fallback
    certifi = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cex_instrument_lifecycle import (
    CURRENT_ABSENCE_REASON,
    CURRENT_ABSENCE_STATUS,
    INSTRUMENT_PART,
    SCHEMA,
    configured_market_ids_sha256,
    load_cex_instrument_lifecycle,
    parse_crypto_com_inventory,
)
from scripts.token_registry import TokenRegistry


SOURCE_URL = "https://api.crypto.com/exchange/v1/public/get-instruments"
DEFAULT_TOKENS_CSV = PROJECT_ROOT / "config/tokens.csv"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data/local"
DEFAULT_MANIFEST_PATH = DEFAULT_DATA_DIR / "cex_instrument_lifecycle.json"
DEFAULT_RAW_ROOT = DEFAULT_DATA_DIR / "raw/cex-instrument-lifecycle"
DEFAULT_RUNTIME_REGISTRY = DEFAULT_DATA_DIR / "admin/token_registry.json"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024


def utc_text(value: Optional[datetime] = None) -> str:
    moment = value or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        raise ValueError("lifecycle collection time must be timezone-aware")
    return moment.astimezone(timezone.utc).isoformat()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_bytes(path: Path, value: bytes, *, mode: int = 0o640) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".{}.".format(path.name),
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary_path), str(path))
        _fsync_directory(path.parent)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def retain_raw_response(raw_root: Path, raw: bytes) -> Tuple[Path, str]:
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("Crypto.com instrument response is empty or too large")
    digest = sha256_bytes(raw)
    path = Path(raw_root) / (digest + ".json")
    if path.exists():
        if not path.is_file() or path.read_bytes() != raw:
            raise ValueError("raw lifecycle evidence hash collision")
        return path, digest
    _atomic_write_bytes(path, raw)
    return path, digest


def build_tls_context() -> ssl.SSLContext:
    return (
        ssl.create_default_context(cafile=certifi.where())
        if certifi is not None
        else ssl.create_default_context()
    )


def fetch_official_response(
    url: str = SOURCE_URL,
    *,
    timeout_seconds: float = 30.0,
) -> Tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "cex-dex-market-monitor-lifecycle/1.0",
        },
        method="GET",
    )
    context = build_tls_context()
    try:
        response = urllib.request.urlopen(
            request,
            timeout=timeout_seconds,
            context=context,
        )
    except urllib.error.HTTPError as error:
        raw = error.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("Crypto.com instrument response exceeds size limit")
        return int(error.code), raw
    with response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("Crypto.com instrument response exceeds size limit")
        return int(response.getcode()), raw


def _canonical_market(token_symbol: Any, cex_symbol: Any) -> Dict[str, str]:
    token = str(token_symbol or "").strip().upper()
    instrument = str(cex_symbol or "").strip().upper()
    parts = instrument.split("/")
    if (
        not INSTRUMENT_PART.fullmatch(token)
        or len(parts) != 2
        or not all(INSTRUMENT_PART.fullmatch(part) for part in parts)
        or parts[0] != token
    ):
        raise ValueError("configured Crypto.com market identity is invalid")
    return {
        "market_id": "cex:crypto_com:" + instrument,
        "token_symbol": token,
        "instrument": instrument,
        "source_instrument": parts[0] + "_" + parts[1],
    }


def load_configured_crypto_com_markets(
    tokens_csv: Path,
    runtime_registry: Path,
) -> List[Dict[str, str]]:
    configured: Dict[str, Dict[str, str]] = {}
    with Path(tokens_csv).open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            market = _canonical_market(
                row.get("token_symbol"),
                row.get("cex_symbol"),
            )
            if market["market_id"] in configured:
                raise ValueError("configured Crypto.com markets must be unique")
            configured[market["market_id"]] = market

    registry = TokenRegistry(Path(runtime_registry))
    for record in registry.list_records(statuses={"active"}):
        mapping = record.get("cex_mapping") or {}
        if (
            mapping.get("status") != "approved"
            or "crypto_com" not in mapping.get("exchanges", [])
        ):
            continue
        market = _canonical_market(
            record.get("token_symbol"),
            mapping.get("cex_symbol"),
        )
        existing = configured.get(market["market_id"])
        if existing is not None and existing != market:
            raise ValueError("runtime Crypto.com market conflicts with static catalog")
        configured[market["market_id"]] = market

    if not configured:
        raise ValueError("Crypto.com lifecycle catalog is empty")
    return [configured[market_id] for market_id in sorted(configured)]


def build_lifecycle_manifest(
    markets: Iterable[Dict[str, str]],
    current_spot_instruments: Set[str],
    *,
    checked_at_utc: str,
    response_sha256: str,
    http_status: int,
    inventory_count: int,
) -> Dict[str, Any]:
    market_list = sorted(list(markets), key=lambda item: item["market_id"])
    configured_market_hash = configured_market_ids_sha256(
        market["market_id"] for market in market_list
    )
    reviews = []
    for market in market_list:
        if market["source_instrument"] in current_spot_instruments:
            continue
        reviews.append(
            {
                "market_id": market["market_id"],
                "market_type": "cex",
                "token_symbol": market["token_symbol"],
                "exchange": "crypto_com",
                "instrument": market["instrument"],
                "current_listing_status": CURRENT_ABSENCE_STATUS,
                "reason_code": CURRENT_ABSENCE_REASON,
                "checked_at_utc": checked_at_utc,
                "source_url": SOURCE_URL,
                "http_status": http_status,
                "response_sha256": response_sha256,
                "inventory_count": inventory_count,
                "instrument_present": False,
            }
        )
    return {
        "schema": SCHEMA,
        "generated_at_utc": checked_at_utc,
        "checked_at_utc": checked_at_utc,
        "response_sha256": response_sha256,
        "inventory_count": inventory_count,
        "configured_market_count": len(market_list),
        "configured_market_ids_sha256": configured_market_hash,
        "review_count": len(reviews),
        "reviews": reviews,
    }


def publish_manifest(path: Path, payload: Dict[str, Any]) -> None:
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".{}.".format(path.name),
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o640)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        load_cex_instrument_lifecycle(temporary_path)
        os.replace(str(temporary_path), str(path))
        _fsync_directory(path.parent)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def collect_crypto_com_lifecycle(
    *,
    tokens_csv: Path = DEFAULT_TOKENS_CSV,
    runtime_registry: Path = DEFAULT_RUNTIME_REGISTRY,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    raw_root: Path = DEFAULT_RAW_ROOT,
    now: Optional[datetime] = None,
    fetcher: Callable[[str], Tuple[int, bytes]] = fetch_official_response,
) -> Dict[str, Any]:
    checked_at_utc = utc_text(now)
    http_status, raw = fetcher(SOURCE_URL)
    raw_path, response_sha256 = retain_raw_response(Path(raw_root), raw)
    if http_status != 200:
        raise RuntimeError(
            "Crypto.com instrument catalog returned HTTP {}".format(http_status)
        )
    current_spot_instruments, inventory_count = parse_crypto_com_inventory(raw)
    markets = load_configured_crypto_com_markets(
        Path(tokens_csv),
        Path(runtime_registry),
    )
    manifest = build_lifecycle_manifest(
        markets,
        current_spot_instruments,
        checked_at_utc=checked_at_utc,
        response_sha256=response_sha256,
        http_status=http_status,
        inventory_count=inventory_count,
    )
    publish_manifest(Path(manifest_path), manifest)
    return {
        "schema": SCHEMA,
        "status": "published",
        "checked_at_utc": checked_at_utc,
        "configured_market_count": len(markets),
        "configured_market_ids_sha256": manifest[
            "configured_market_ids_sha256"
        ],
        "official_spot_instrument_count": len(current_spot_instruments),
        "inventory_count": inventory_count,
        "review_count": manifest["review_count"],
        "response_sha256": response_sha256,
        "raw_path": str(raw_path),
        "manifest_path": str(Path(manifest_path)),
    }


def parse_args() -> argparse.Namespace:
    configured_manifest = os.environ.get("MARKET_CEX_INSTRUMENT_LIFECYCLE")
    parser = argparse.ArgumentParser(
        description="Publish exact current Crypto.com spot lifecycle evidence"
    )
    parser.add_argument("--tokens-csv", type=Path, default=DEFAULT_TOKENS_CSV)
    parser.add_argument(
        "--runtime-registry",
        type=Path,
        default=DEFAULT_RUNTIME_REGISTRY,
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=(
            Path(configured_manifest).expanduser()
            if configured_manifest
            else DEFAULT_MANIFEST_PATH
        ),
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = collect_crypto_com_lifecycle(
        tokens_csv=args.tokens_csv,
        runtime_registry=args.runtime_registry,
        manifest_path=args.manifest,
        raw_root=args.raw_root,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
