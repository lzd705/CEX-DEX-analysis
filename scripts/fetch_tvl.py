"""Collect point-in-time TVL facts for every currently cataloged DEX pool.

The collector intentionally keeps TVL separate from daily OHLCV:

- pool identities come from the published SQLite/CSV market snapshot;
- GeckoTerminal ``reserve_in_usd`` is stored as a source-reported snapshot;
- requests are batched by chain through the multi-pool endpoint;
- every pool receives an explicit observed/missing/not_found/failed status;
- raw responses and their SHA-256 hashes are retained for audit;
- publishing appends history and atomically replaces the latest snapshot.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

try:
    from scripts.atomic_publication import atomic_replace_bundle, csv_payload
    from scripts.bounded_snapshot_merge import merge_exact_market_snapshot
    from scripts.publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
    )
    from scripts.quality_outcomes import (
        normalize_tvl_source_outcome,
        quality_outcome_resolution_state,
    )
    from scripts.timestamp_contract import validate_observation_bounds
except ModuleNotFoundError:
    from atomic_publication import atomic_replace_bundle, csv_payload
    from bounded_snapshot_merge import merge_exact_market_snapshot
    from publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
    )
    from quality_outcomes import (
        normalize_tvl_source_outcome,
        quality_outcome_resolution_state,
    )
    from timestamp_contract import validate_observation_bounds


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "data/local/market_facts.sqlite3"
DEFAULT_DEX_CSV = PROJECT_ROOT / "data/local/dex_pool_volume_daily.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/tvl"
GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"
MAX_POOLS_PER_REQUEST = 30
# Multi-pool requests appear to consume more than one unit of the public quota.
# Keep the default below five batches per minute even though the documented
# account-level headline limit is 30 requests per minute.
REQUEST_SLEEP_SECONDS = 12.5
MAX_RETRIES = 3
TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

CURRENT_FILENAME = "dex_pool_tvl_snapshot.csv"
LATEST_FILENAME = "dex_pool_tvl_latest.csv"
HISTORY_FILENAME = "dex_pool_tvl_history.csv"
MINIMUM_PUBLISHABLE_COVERAGE_BPS = 8000
MINIMUM_BASELINE_RETENTION_BPS = 9500

TVL_COLUMNS = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "source_dex",
    "source_pool_name",
    "base_token_id",
    "quote_token_id",
    "tvl_usd",
    "base_token_price_usd",
    "quote_token_price_usd",
    "volume_24h_usd",
    "pool_created_at",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "error",
]


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def pool_key(chain: str, pool_address: str) -> tuple[str, str]:
    """Return a chain-aware pool key without damaging case-sensitive addresses."""
    address = pool_address.strip()
    if address.startswith("0x"):
        address = address.lower()
    return chain.strip().lower(), address


def dex_market_id(pool: dict[str, str]) -> str:
    chain, address = pool_key(pool["chain"], pool["pool_address"])
    return (
        f"dex:{chain}:{pool.get('dex', '').strip().lower()}:"
        f"{address}:{pool['token_symbol'].strip().upper()}"
    )


def optional_decimal_text(value: Any) -> str:
    """Normalize a finite, non-negative source number without inventing zero."""
    if value is None or str(value).strip() == "":
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid numeric source value: {value}") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"Invalid non-negative source value: {value}")
    return str(value).strip()


def chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    if size < 1:
        raise ValueError("batch size must be positive")
    for start in range(0, len(values), size):
        yield values[start:start + size]


def load_pools_from_database(database_path: Path) -> list[dict[str, str]]:
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT token_symbol, chain, dex, pool_address, pool_name
            FROM dex_pool_daily
            WHERE rowid IN (
                SELECT MAX(rowid)
                FROM dex_pool_daily
                GROUP BY token_symbol, chain, pool_address
            )
            ORDER BY chain, token_symbol, dex, pool_address
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def load_pools_from_csv(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"token_symbol", "chain", "dex", "pool_address", "pool_name"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{csv_path.name} is missing columns: {', '.join(missing)}")
        unique: dict[tuple[str, str, str], dict[str, str]] = {}
        for row in reader:
            if not row.get("token_symbol") or not row.get("chain") or not row.get("pool_address"):
                continue
            key = (
                row["token_symbol"].upper(),
                *pool_key(row["chain"], row["pool_address"]),
            )
            unique[key] = {
                "token_symbol": row["token_symbol"].upper(),
                "chain": row["chain"].lower(),
                "dex": row.get("dex", ""),
                "pool_address": row["pool_address"],
                "pool_name": row.get("pool_name", ""),
            }
    return sorted(
        unique.values(),
        key=lambda row: (row["chain"], row["token_symbol"], row["dex"], row["pool_address"]),
    )


def load_cataloged_pools(
    database_path: Path = DEFAULT_DATABASE,
    csv_path: Path = DEFAULT_DEX_CSV,
) -> list[dict[str, str]]:
    if database_path.exists():
        rows = load_pools_from_database(database_path)
    elif csv_path.exists():
        rows = load_pools_from_csv(csv_path)
    else:
        raise FileNotFoundError(
            f"No published DEX pool inventory found at {database_path} or {csv_path}"
        )
    if not rows:
        raise ValueError("Published DEX pool inventory contains no pools")
    return rows


def multi_pool_url(chain: str, pool_addresses: list[str]) -> str:
    encoded = urllib.parse.quote(",".join(pool_addresses), safe=",")
    return f"{GECKOTERMINAL_BASE_URL}/networks/{chain}/pools/multi/{encoded}"


def request_json(url: str) -> tuple[dict[str, Any], bytes]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json;version=20230302",
            "User-Agent": "CEX-DEX-Market-Monitor/1.0",
        },
    )
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=30, context=TLS_CONTEXT) as response:
                raw = response.read()
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("GeckoTerminal response must be a JSON object")
                return payload, raw
        except urllib.error.HTTPError as error:
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt + 1 >= MAX_RETRIES:
                raise
            retry_after = error.headers.get("Retry-After")
            if error.code == 429:
                wait_seconds = max(65.0, float(retry_after or 0))
            else:
                wait_seconds = max(5.0, float(retry_after or 0), 2 ** attempt)
            time.sleep(wait_seconds)
        except urllib.error.URLError:
            if attempt + 1 >= MAX_RETRIES:
                raise
            time.sleep(max(2.0, 2 ** attempt))
    raise RuntimeError(f"Failed after retries: {url}")


def payload_pools(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    data = payload.get("data", [])
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError("GeckoTerminal pool response data must be a list")
    for item in data:
        if not isinstance(item, dict):
            continue
        attributes = item.get("attributes") or {}
        address = attributes.get("address")
        item_id = str(item.get("id") or "")
        if not address and "_" in item_id:
            _, address = item_id.split("_", 1)
        chain = item_id.split("_", 1)[0] if "_" in item_id else ""
        if address and chain:
            result[pool_key(chain, str(address))] = item
    return result


def source_pool_fields(item: dict[str, Any]) -> dict[str, str]:
    attributes = item.get("attributes") or {}
    relationships = item.get("relationships") or {}
    volume = attributes.get("volume_usd") or {}
    return {
        "source_dex": str(
            ((relationships.get("dex") or {}).get("data") or {}).get("id") or ""
        ),
        "source_pool_name": str(attributes.get("name") or ""),
        "base_token_id": str(
            ((relationships.get("base_token") or {}).get("data") or {}).get("id") or ""
        ),
        "quote_token_id": str(
            ((relationships.get("quote_token") or {}).get("data") or {}).get("id") or ""
        ),
        "tvl_usd": optional_decimal_text(attributes.get("reserve_in_usd")),
        "base_token_price_usd": optional_decimal_text(attributes.get("base_token_price_usd")),
        "quote_token_price_usd": optional_decimal_text(attributes.get("quote_token_price_usd")),
        "volume_24h_usd": optional_decimal_text(volume.get("h24")),
        "pool_created_at": str(attributes.get("pool_created_at") or ""),
    }


def base_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    source_endpoint: str,
) -> dict[str, str]:
    return {
        "snapshot_id": snapshot_id,
        "observed_at": response_received_at,
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "token_symbol": pool["token_symbol"].upper(),
        "chain": pool["chain"].lower(),
        "dex": pool.get("dex", ""),
        "pool_address": pool["pool_address"],
        "pool_name": pool.get("pool_name", ""),
        "source_dex": "",
        "source_pool_name": "",
        "base_token_id": "",
        "quote_token_id": "",
        "tvl_usd": "",
        "base_token_price_usd": "",
        "quote_token_price_usd": "",
        "volume_24h_usd": "",
        "pool_created_at": "",
        "tvl_method": "geckoterminal_reserve_in_usd",
        "source": "GeckoTerminal API v2",
        "source_endpoint": source_endpoint,
        "raw_response_sha256": "",
        "status": "",
        "error": "",
    }


def rows_from_payload(
    pools: list[dict[str, str]],
    payload: dict[str, Any],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    source_endpoint: str,
    raw_sha256: str,
) -> list[dict[str, str]]:
    by_pool = payload_pools(payload)
    rows = []
    for pool in pools:
        row = base_row(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            source_endpoint=source_endpoint,
        )
        row["raw_response_sha256"] = raw_sha256
        item = by_pool.get(pool_key(pool["chain"], pool["pool_address"]))
        if item is None:
            row["status"] = "not_found"
            row["error"] = "Pool was not returned by the GeckoTerminal multi-pool endpoint"
        else:
            try:
                row.update(source_pool_fields(item))
                row["status"] = "observed" if row["tvl_usd"] else "missing"
                if row["status"] == "missing":
                    row["error"] = "Source returned no reserve_in_usd value"
            except ValueError as error:
                row["status"] = "failed"
                row["error"] = str(error)
        rows.append(row)
    return rows


def failure_rows(
    pools: list[dict[str, str]],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    source_endpoint: str,
    error: Exception,
) -> list[dict[str, str]]:
    rows = []
    for pool in pools:
        row = base_row(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            source_endpoint=source_endpoint,
        )
        row["status"] = "failed"
        row["error"] = f"{type(error).__name__}: {error}"
        rows.append(row)
    return rows


def collect_tvl(
    pools: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    request: Callable[[str], tuple[dict[str, Any], bytes]] = request_json,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    allow_terminal_only: bool = False,
) -> tuple[str, list[dict[str, str]]]:
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    snapshot_raw_dir = raw_root / snapshot_id
    snapshot_raw_dir.mkdir(parents=True, exist_ok=False)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for pool in pools:
        grouped[pool["chain"].lower()].append(pool)

    batches: list[tuple[str, list[dict[str, str]]]] = []
    for chain in sorted(grouped):
        chain_pools = sorted(
            grouped[chain],
            key=lambda row: (row["token_symbol"], row["dex"], row["pool_address"]),
        )
        batches.extend((chain, batch) for batch in chunks(chain_pools, MAX_POOLS_PER_REQUEST))

    rows = []
    for batch_index, (chain, batch) in enumerate(batches, start=1):
        url = multi_pool_url(chain, [pool["pool_address"] for pool in batch])
        request_started_at = utc_now_text()
        raw_path = snapshot_raw_dir / f"{batch_index:03d}-{chain}.json"
        try:
            payload, raw = request(url)
            response_received_at = utc_now_text()
            raw_path.write_bytes(raw)
            raw_sha256 = hashlib.sha256(raw).hexdigest()
            rows.extend(
                rows_from_payload(
                    batch,
                    payload,
                    snapshot_id=snapshot_id,
                    request_started_at=request_started_at,
                    response_received_at=response_received_at,
                    source_endpoint=url,
                    raw_sha256=raw_sha256,
                )
            )
        except Exception as error:
            response_received_at = utc_now_text()
            error_payload = json.dumps(
                {
                    "source_endpoint": url,
                    "request_started_at": request_started_at,
                    "response_received_at": response_received_at,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
            raw_path.write_bytes(error_payload)
            rows.extend(
                failure_rows(
                    batch,
                    snapshot_id=snapshot_id,
                    request_started_at=request_started_at,
                    response_received_at=response_received_at,
                    source_endpoint=url,
                    error=error,
                )
            )
        if batch_index < len(batches) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now_text(),
        "pool_count": len(rows),
        "token_count": len({row["token_symbol"] for row in rows}),
        "chain_count": len({row["chain"] for row in rows}),
        "status_counts": {
            status: sum(row["status"] == status for row in rows)
            for status in ("observed", "missing", "not_found", "failed")
        },
        "raw_files": sorted(path.name for path in snapshot_raw_dir.glob("*.json")),
    }
    (snapshot_raw_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validate_snapshot(
        pools,
        rows,
        allow_terminal_only=allow_terminal_only,
    )
    return snapshot_id, rows


def validate_snapshot(
    inventory: list[dict[str, str]],
    rows: list[dict[str, str]],
    *,
    allow_terminal_only: bool = False,
    allow_no_observed: bool = False,
) -> None:
    validate_observation_bounds(rows)
    validate_tvl_fact_rows(rows)
    expected = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in inventory
    }
    actual = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in rows
    }
    if len(rows) != len(actual):
        raise ValueError("TVL snapshot contains duplicate Token/pool rows")
    if expected != actual:
        raise ValueError("TVL snapshot pool coverage does not match the published inventory")
    accepted_statuses = {"observed", "missing", "not_found", "failed"}
    if any(row["status"] not in accepted_statuses for row in rows):
        raise ValueError("TVL snapshot contains an invalid status")
    if (
        not allow_no_observed
        and not any(row["status"] == "observed" for row in rows)
    ):
        if not allow_terminal_only:
            raise ValueError("TVL snapshot contains no observed TVL facts")
        if any(
            quality_outcome_resolution_state(
                *normalize_tvl_source_outcome(
                    row.get("status"),
                    error=row.get("error"),
                )
            ) != "confirmed_terminal_absence"
            for row in rows
        ):
            raise ValueError(
                "TVL exact candidate is not a terminal non-retryable outcome"
            )


def validate_tvl_fact_rows(rows: Iterable[dict[str, str]]) -> None:
    """Bind every TVL value to an explicit source-observation status."""
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        raw_value = row.get("tvl_usd")
        has_value = raw_value not in (None, "")
        if status == "observed":
            if not has_value:
                raise ValueError("TVL snapshot observed TVL must have a value")
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "TVL snapshot observed TVL must be finite and non-negative"
                ) from error
            if not math.isfinite(value) or value < 0:
                raise ValueError(
                    "TVL snapshot observed TVL must be finite and non-negative"
                )
        elif has_value:
            raise ValueError("TVL snapshot non-observed TVL must be null")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def merge_exact_publication(
    rows: list[dict[str, str]],
    *,
    target_market_id: str,
    publish_dir: Path,
) -> list[dict[str, str]]:
    """Merge one freshly collected pool into the full TVL latest view."""
    baseline = read_csv_rows(publish_dir / LATEST_FILENAME)
    baseline_market_ids = {dex_market_id(row) for row in baseline}
    merged = merge_exact_market_snapshot(
        baseline,
        rows,
        target_market_id=target_market_id,
        market_id_for_row=dex_market_id,
        row_identity=dex_market_id,
        allow_target_insert=True,
    )
    expected_inventory = (
        baseline
        if target_market_id in baseline_market_ids
        else baseline + rows
    )
    validate_snapshot(
        expected_inventory,
        merged,
        allow_no_observed=True,
    )
    return merged


def publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path) if latest_path.exists() else None
    return enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="dex_tvl",
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
        cohort=lambda row: row.get("chain", "").strip().lower(),
        usable_statuses={"observed"},
        valid_statuses={"observed", "missing", "not_found", "failed"},
        minimum_candidate_usable_bps=MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        minimum_baseline_retention_bps=MINIMUM_BASELINE_RETENTION_BPS,
    )


def exact_publication_coverage_gate(
    rows: list[dict[str, str]],
    *,
    target_market_id: str,
    history_rows_to_append: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    """Prove one TVL recovery without applying the full-refresh threshold.

    A bounded refresh is allowed to repair a low-coverage publication one
    target at a time.  It must not, however, change any non-target fact or
    reduce an already observed target to a terminal absence.  The ordinary
    full-publication gate remains unchanged and continues to require 80%
    observed coverage.
    """
    target = str(target_market_id or "").strip()
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path)
    report = enforce_publication_coverage(
        rows,
        baseline_rows,
        fact_family="dex_tvl",
        identity=lambda row: dex_market_id(dict(row)),
        usable_statuses={"observed"},
        valid_statuses={"observed", "missing", "not_found", "failed"},
        minimum_candidate_usable_bps=0,
        minimum_baseline_retention_bps=10_000,
    )

    reasons: list[str] = []
    if not target:
        reasons.append("exact_target_missing")
    if not baseline_rows:
        reasons.append("exact_baseline_missing")
    if len(history_rows_to_append) != 1:
        reasons.append("exact_history_target_count_invalid")

    def index_by_market_id(
        source_rows: list[dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        indexed: dict[str, dict[str, str]] = {}
        for row in source_rows:
            market_id = dex_market_id(row)
            if market_id in indexed:
                reasons.append("exact_market_identity_duplicate")
            indexed[market_id] = row
        return indexed

    baseline_by_market = index_by_market_id(baseline_rows)
    candidate_by_market = index_by_market_id(rows)
    history_by_market = index_by_market_id(history_rows_to_append)
    expected_market_ids = set(baseline_by_market)
    expected_market_ids.add(target)
    if set(candidate_by_market) != expected_market_ids:
        reasons.append("exact_inventory_changed")
    if set(history_by_market) != {target}:
        reasons.append("exact_history_target_mismatch")

    snapshot_ids = {
        str(row.get("snapshot_id") or "").strip() for row in rows
    }
    baseline_snapshot_ids = {
        str(row.get("snapshot_id") or "").strip() for row in baseline_rows
    }
    history_snapshot_ids = {
        str(row.get("snapshot_id") or "").strip()
        for row in history_rows_to_append
    }
    if len(snapshot_ids) != 1 or not next(iter(snapshot_ids), ""):
        reasons.append("exact_publication_identity_invalid")
    if len(baseline_snapshot_ids) != 1 or not next(
        iter(baseline_snapshot_ids), ""
    ):
        reasons.append("exact_baseline_identity_invalid")
    if (
        snapshot_ids == baseline_snapshot_ids
        or history_snapshot_ids != snapshot_ids
    ):
        reasons.append("exact_publication_identity_unchanged_or_unbound")

    rebound_fields = {"snapshot_id"}
    for market_id, baseline_row in baseline_by_market.items():
        if market_id == target or market_id not in candidate_by_market:
            continue
        candidate_row = candidate_by_market[market_id]
        if {
            key: value
            for key, value in baseline_row.items()
            if key not in rebound_fields
        } != {
            key: value
            for key, value in candidate_row.items()
            if key not in rebound_fields
        }:
            reasons.append("exact_non_target_fact_changed")
            break

    target_row = candidate_by_market.get(target)
    history_target = history_by_market.get(target)
    resolution = "unresolved"
    if target_row is None:
        reasons.append("exact_target_missing_from_publication")
    else:
        resolution = quality_outcome_resolution_state(
            *normalize_tvl_source_outcome(
                target_row.get("status"),
                error=target_row.get("error"),
            )
        )
        if resolution not in {"observed", "confirmed_terminal_absence"}:
            reasons.append("exact_target_unresolved")
        raw_sha256 = str(target_row.get("raw_response_sha256") or "")
        if (
            len(raw_sha256) != 64
            or any(character not in "0123456789abcdef" for character in raw_sha256)
        ):
            reasons.append("exact_target_source_hash_invalid")
    if target_row is not None and history_target != target_row:
        reasons.append("exact_history_target_fact_mismatch")

    baseline_target = baseline_by_market.get(target)
    if (
        baseline_target is not None
        and baseline_target.get("status") == "observed"
        and resolution != "observed"
    ):
        reasons.append("exact_target_observed_fact_lost")

    report.update(
        {
            "mode": "exact_target_recovery/v1",
            "exact_target": {
                "market_id": target,
                "baseline_present": baseline_target is not None,
                "resolution": resolution,
                "history_row_count": len(history_rows_to_append),
            },
        }
    )
    if reasons:
        report["status"] = "rejected"
        report["passed"] = False
        report["reasons"] = list(
            dict.fromkeys(list(report.get("reasons") or []) + reasons)
        )
        raise CoverageRegressionError(report)
    return bind_passing_coverage_report(
        report,
        fact_family="dex_tvl",
        baseline_path=latest_path,
    )


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=TVL_COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows({field: row.get(field, "") for field in TVL_COLUMNS} for row in rows)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_snapshot(
    rows: list[dict[str, str]],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
    history_rows_to_append: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)
    result: dict[str, Any] = {
        "current_path": str(current_path),
        "row_count": len(rows),
    }
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    publication_gate = publication_coverage_gate(rows, publish_dir)
    existing_history = read_csv_rows(publish_dir / HISTORY_FILENAME)
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in existing_history
    }
    for row in (
        rows if history_rows_to_append is None else history_rows_to_append
    ):
        merged[
            (
                row["snapshot_id"],
                row["token_symbol"],
                *pool_key(row["chain"], row["pool_address"]),
            )
        ] = row
    history_rows = sorted(
        merged.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("chain", ""),
            row.get("pool_address", ""),
        ),
    )
    atomic_write_csv(publish_dir / HISTORY_FILENAME, history_rows)
    atomic_write_csv(publish_dir / LATEST_FILENAME, rows)
    atomic_write_csv(publish_dir / CURRENT_FILENAME, rows)
    result.update(
        {
            "latest_path": str(publish_dir / LATEST_FILENAME),
            "history_path": str(publish_dir / HISTORY_FILENAME),
            "history_row_count": len(history_rows),
            "publication_gate": publication_gate,
        }
    )
    return result


def publish_exact_snapshot(
    rows: list[dict[str, str]],
    *,
    target_market_id: str,
    history_rows_to_append: list[dict[str, str]],
    output_dir: Path,
    publish_dir: Path,
) -> dict[str, Any]:
    """Failure-atomically publish one bounded TVL merge."""
    publication_gate = exact_publication_coverage_gate(
        rows,
        target_market_id=target_market_id,
        history_rows_to_append=history_rows_to_append,
        publish_dir=publish_dir,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)

    history_path = publish_dir / HISTORY_FILENAME
    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in read_csv_rows(history_path)
    }
    for row in history_rows_to_append:
        merged_history[
            (
                row["snapshot_id"],
                row["token_symbol"],
                *pool_key(row["chain"], row["pool_address"]),
            )
        ] = row
    history_rows = sorted(
        merged_history.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("chain", ""),
            row.get("pool_address", ""),
        ),
    )
    atomic_replace_bundle(
        (
            (history_path, csv_payload(TVL_COLUMNS, history_rows)),
            (publish_dir / LATEST_FILENAME, csv_payload(TVL_COLUMNS, rows)),
            (publish_dir / CURRENT_FILENAME, csv_payload(TVL_COLUMNS, rows)),
        )
    )
    return {
        "current_path": str(current_path),
        "row_count": len(rows),
        "latest_path": str(publish_dir / LATEST_FILENAME),
        "history_path": str(history_path),
        "history_row_count": len(history_rows),
        "publication_gate": publication_gate,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect TVL for all published DEX pools")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--dex-csv", type=Path, default=DEFAULT_DEX_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--market-id",
        help="One canonical DEX market identity for bounded collection",
    )
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument(
        "--publish-dir",
        type=Path,
        help="Explicit runtime directory for an atomic publication",
    )
    parser.add_argument(
        "--merge-publish",
        action="store_true",
        help="Merge one exact pool into an existing full publication",
    )
    parser.add_argument("--sleep-seconds", type=float, default=REQUEST_SLEEP_SECONDS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pools = load_cataloged_pools(args.database, args.dex_csv)
    market_id = str(args.market_id or "").strip()
    publish_dir = (
        args.publish_dir
        if args.publish_dir is not None
        else DEFAULT_PUBLISH_DIR if args.publish_local else None
    )
    if args.merge_publish and (publish_dir is None or not market_id):
        raise ValueError(
            "--merge-publish requires --publish-dir and --market-id"
        )
    if publish_dir is not None and market_id and not args.merge_publish:
        raise ValueError(
            "filtered publication requires explicit --merge-publish"
        )
    if market_id:
        pools = [pool for pool in pools if dex_market_id(pool) == market_id]
    if not pools:
        raise ValueError("No cataloged DEX pool matches --market-id")
    snapshot_id, rows = collect_tvl(
        pools,
        raw_root=args.raw_root,
        sleep_seconds=max(0.0, args.sleep_seconds),
        allow_terminal_only=args.merge_publish,
    )
    collected_rows = rows
    if args.merge_publish:
        assert publish_dir is not None
        rows = merge_exact_publication(
            rows,
            target_market_id=market_id,
            publish_dir=publish_dir,
        )
    result = (
        publish_exact_snapshot(
            rows,
            target_market_id=market_id,
            history_rows_to_append=collected_rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
        )
        if args.merge_publish and publish_dir is not None
        else publish_snapshot(
            rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
        )
    )
    result.update(
        {
            "snapshot_id": snapshot_id,
            "token_count": len({row["token_symbol"] for row in rows}),
            "pool_count": len(rows),
            "collected_pool_count": len(collected_rows),
            "observed_count": sum(row["status"] == "observed" for row in rows),
            "missing_count": sum(row["status"] == "missing" for row in rows),
            "not_found_count": sum(row["status"] == "not_found" for row in rows),
            "failed_count": sum(row["status"] == "failed" for row in rows),
        }
    )
    publication_gate = result.pop("publication_gate", None)
    if publication_gate is not None:
        result["publication_gates"] = {"dex_tvl": publication_gate}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
