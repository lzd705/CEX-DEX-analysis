"""Fetch daily DEX volume data from GeckoTerminal.

This first version is intentionally simple:
    - Read token config from config/tokens.csv
    - Read token-chain config from config/token_chains.csv
    - Find the global top DEX pools across configured chains for each token
    - Fetch daily OHLCV for those pools
    - Write data/processed/dex_pools.csv
    - Write data/processed/dex_pool_volume_daily.csv
    - Write data/processed/dex_volume_daily.csv
"""

import argparse
import csv
import hashlib
import json
import ssl
import time
import urllib.parse
import urllib.request
import urllib.error
import os
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
    from scripts.token_registry import (
        DEFAULT_REGISTRY_PATH,
        TokenRegistry,
        TokenRegistryError,
        normalize_chain,
        normalize_contract_address,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from token_registry import (
        DEFAULT_REGISTRY_PATH,
        TokenRegistry,
        TokenRegistryError,
        normalize_chain,
        normalize_contract_address,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_CONFIG_PATH = PROJECT_ROOT / "config/tokens.csv"
TOKEN_CHAIN_CONFIG_PATH = PROJECT_ROOT / "config/token_chains.csv"
DEX_POOLS_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_pools.csv"
DEX_POOL_VOLUME_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_pool_volume_daily.csv"
DEX_VOLUME_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_volume_daily.csv"
ATTEMPT_OUTPUT_PATH = (
    PROJECT_ROOT / "data/processed/dex_daily_collection_attempts.json"
)
ATTEMPT_SCHEMA = "daily_collection_attempts/v1"
TVL_LATEST_PATH = PROJECT_ROOT / "data/local/dex_pool_tvl_latest.csv"

GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"
LIMIT_DAYS = 180
MAX_REFRESH_WINDOW_DAYS = 180
REQUEST_SLEEP_SECONDS = 15.0
INCREMENTAL_POOL_SLEEP_SECONDS = 13.0
MAX_RETRIES = 3
MIN_HISTORY_DAYS = 120
MAX_POOL_CANDIDATES = 8
TOP_POOL_COUNT = 5
TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

ATTEMPT_ERROR_MESSAGES = {
    "network": "The source request could not reach the remote service.",
    "rate_limit": "The source rejected the request because its rate limit was reached.",
    "source_unavailable": "The remote source was temporarily unavailable.",
    "source_range_unavailable": (
        "The public OHLCV endpoint does not permit the requested historical "
        "date window."
    ),
    "not_listed": "The source reported that the requested Token or pool was unavailable.",
    "parse": "The source response could not be decoded into the expected format.",
    "validation": "The source response did not satisfy the collector contract.",
}


class SourceRangeUnavailable(ValueError):
    """The keyless source cannot serve the requested historical window."""


def _is_geckoterminal_ohlcv_url(value):
    try:
        parsed = urllib.parse.urlsplit(str(value))
    except (TypeError, ValueError):
        return False
    return (
        parsed.scheme == "https"
        and parsed.hostname == "api.geckoterminal.com"
        and "/ohlcv/" in parsed.path
    )


def is_public_history_limit_response(error, request_url):
    """Recognize only GeckoTerminal's explicit bounded range response."""
    if (
        not isinstance(error, urllib.error.HTTPError)
        or error.code != 401
        or not _is_geckoterminal_ohlcv_url(request_url)
        or not _is_geckoterminal_ohlcv_url(error.geturl())
    ):
        return False
    try:
        payload = error.read(4097)
    except (AttributeError, OSError, TypeError, ValueError):
        return False
    if not isinstance(payload, bytes) or len(payload) > 4096:
        return False
    lowered = payload.decode("utf-8", errors="replace").lower()
    return "past 180 days" in lowered and "public api" in lowered


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
    """Return a bounded reason without retaining URLs, payloads, or local paths."""
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
    if (
        "source_range_unavailable" in lowered
        or any(isinstance(candidate, SourceRangeUnavailable) for candidate in chain)
    ):
        reason = "source_range_unavailable"
    elif http_status == 429 or "rate limit" in lowered or "too many requests" in lowered:
        reason = "rate_limit"
    elif http_status is not None and http_status >= 500:
        reason = "source_unavailable"
    elif http_status == 404 or any(
        marker in lowered
        for marker in (
            "not found",
            "invalid token",
            "invalid pool",
            "does not exist",
        )
    ):
        reason = "not_listed"
    elif any(
        (
            isinstance(candidate, TimeoutError)
            or (
                isinstance(candidate, urllib.error.URLError)
                and not isinstance(candidate, urllib.error.HTTPError)
            )
        )
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


def dex_attempt_record(
    token_symbol,
    chain,
    dex,
    pool_address,
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
    address = str(pool_address or "").strip()
    if address.startswith("0x"):
        address = address.lower()
    identity = {
        "market_type": "dex",
        "token_symbol": str(token_symbol).strip().upper(),
        "exchange": None,
        "instrument": None,
        "chain": str(chain or "").strip().lower() or None,
        "dex": str(dex or "").strip().lower() or None,
        "pool_address": address or None,
    }
    finished_at_utc = datetime.now(timezone.utc).isoformat()
    id_material = {
        **identity,
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
        if not all(str(attempt.get(key) or "").strip() for key in ("token_symbol", "chain", "dex", "pool_address")):
            raise ValueError("DEX attempt identity is incomplete")
    payload = {
        "schema": ATTEMPT_SCHEMA,
        "collector": "dex",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_window": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "source_csv": source_csv.name,
        "source_csv_sha256": sha256_file(source_csv),
        "attempt_count": len(validated_attempts),
        "attempts": sorted(
            validated_attempts,
            key=lambda item: (
                item["token_symbol"],
                item.get("chain") or "",
                item.get("pool_address") or "",
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


def safe_float(value) -> float:
    """Convert a value to float for internal ranking, treating missing as zero."""
    if value is None:
        return 0.0
    if value == "":
        return 0.0
    return float(value)


def optional_float(value):
    """Convert a source fact to float without inventing a missing zero."""
    if value is None or value == "":
        return None
    return float(value)


def get_retry_wait_seconds(status_code, retry_after) -> int:
    """Return wait seconds for rate-limited requests."""
    if status_code != 429:
        return 0

    if retry_after is None:
        return 65

    try:
        wait_seconds = int(retry_after)
        return wait_seconds if wait_seconds > 0 else 65
    except ValueError:
        return 65


def get_status_code(error):
    """Extract an HTTP status code from urllib errors or error text."""
    status_code = getattr(error, "code", None)

    if status_code is not None:
        return status_code

    if "HTTP Error 429" in str(error):
        return 429

    return None


def normalized_address_identity(chain: str, contract_address: str):
    """Return a chain-aware address identity used for exact comparisons."""
    normalized_chain = normalize_chain(chain)
    return (
        normalized_chain,
        normalize_contract_address(normalized_chain, contract_address),
    )


def token_id_identity(token_id):
    """Parse and normalize a GeckoTerminal Token id for safe comparison."""
    source_chain, separator, source_address = str(token_id or "").partition("_")
    if not separator:
        return None
    try:
        return normalized_address_identity(source_chain, source_address)
    except TokenRegistryError:
        return None


def get_token_side(pool, chain: str, contract_address: str) -> str:
    """Return the exact base/quote side for one chain-aware Token identity."""
    target_identity = normalized_address_identity(chain, contract_address)

    if token_id_identity(pool.get("base_token_id")) == target_identity:
        return "base"

    if token_id_identity(pool.get("quote_token_id")) == target_identity:
        return "quote"

    raise ValueError(
        "pool_token_mismatch: target %s is neither base_token_id nor quote_token_id"
        % ("%s_%s" % target_identity)
    )


def sort_pools_by_volume(pools):
    """Sort GeckoTerminal pools by 24h volume, highest first."""
    def get_volume(pool):
        attributes = pool.get("attributes", {})
        volume_usd = attributes.get("volume_usd", {})
        return safe_float(volume_usd.get("h24"))

    return sorted(pools, key=get_volume, reverse=True)


def request_json(url: str):
    """Request JSON from GeckoTerminal."""
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            with urllib.request.urlopen(request, timeout=30, context=TLS_CONTEXT) as response:
                text = response.read().decode("utf-8")
                data = json.loads(text)
                return data
        except Exception as error:
            if is_public_history_limit_response(error, url):
                raise SourceRangeUnavailable(
                    "source_range_unavailable: the public OHLCV endpoint "
                    "rejected the requested historical window"
                ) from error
            status_code = get_status_code(error)
            retry_after = None

            headers = getattr(error, "headers", None)
            if headers is not None:
                retry_after = headers.get("Retry-After")

            wait_seconds = get_retry_wait_seconds(status_code, retry_after)

            if wait_seconds <= 0:
                raise

            wait_seconds = wait_seconds * (2 ** attempt)
            attempt = attempt + 1
            print("Rate limited. Waiting %s seconds before retry." % wait_seconds)
            time.sleep(wait_seconds)

    raise RuntimeError("Failed after retries: %s" % url)


def read_token_config(path: Path):
    """Read token config rows."""
    with path.open("r", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    return rows


def runtime_registry_path() -> Path:
    """Return the runtime registry path without modifying reviewed CSV config."""
    configured = os.environ.get("TOKEN_REGISTRY_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    data_dir = os.environ.get("MARKET_DATA_DIR")
    if data_dir:
        return (
            Path(data_dir).expanduser().resolve()
            / "admin/token_registry.json"
        )
    return DEFAULT_REGISTRY_PATH


def merge_runtime_token_config(token_rows, chain_rows, token_symbols=None):
    """Append active runtime identities plus one explicitly authorized pending job."""
    onboarding_job_id = os.environ.get("TOKEN_ONBOARDING_JOB_ID", "").strip()
    requested_symbol_list = [
        str(token_symbol or "").strip().upper()
        for token_symbol in (token_symbols or [])
        if str(token_symbol or "").strip()
    ]
    requested_symbols = set(requested_symbol_list)
    if onboarding_job_id and len(requested_symbol_list) != 1:
        raise ValueError(
            "TOKEN_ONBOARDING_JOB_ID requires exactly one requested Token"
        )

    statuses = {"active", "pending"} if onboarding_job_id else {"active"}
    records = TokenRegistry(runtime_registry_path()).list_records(statuses=statuses)
    if onboarding_job_id:
        pending_matches = [
            record
            for record in records
            if record.get("status") == "pending"
            and record.get("token_symbol") in requested_symbols
            and record.get("last_job_id") == onboarding_job_id
        ]
        if len(pending_matches) != 1:
            raise ValueError(
                "TOKEN_ONBOARDING_JOB_ID does not match the requested pending Token"
            )
        records = [
            record
            for record in records
            if record.get("status") == "active"
        ] + pending_matches
    else:
        records = [
            record
            for record in records
            if record.get("status") == "active"
        ]

    merged_tokens = list(token_rows)
    merged_chains = list(chain_rows)
    existing_symbols = {
        row.get("token_symbol", "").strip().upper()
        for row in token_rows
    }
    existing_identities = {
        (
            row.get("token_symbol", "").strip().upper(),
            *normalized_address_identity(
                row.get("chain", ""),
                row.get("contract_address", ""),
            ),
        )
        for row in chain_rows
    }
    for record in records:
        symbol = record["token_symbol"]
        if symbol not in existing_symbols:
            merged_tokens.append(
                {
                    "token_symbol": symbol,
                    "coingecko_id": record.get("coingecko_id") or "",
                    "chain": record["chain"],
                    "contract_address": record["contract_address"],
                    "cex_symbol": "",
                    "primary_cex": "",
                    "secondary_cex": "",
                    "dex_source": "geckoterminal",
                    "primary_dex": "",
                    "pool_address": "",
                    "notes": "runtime registry DEX-only Token",
                }
            )
            existing_symbols.add(symbol)
        identity = (
            symbol,
            *normalized_address_identity(
                record["chain"],
                record["contract_address"],
            ),
        )
        if identity not in existing_identities:
            merged_chains.append(
                {
                    "token_symbol": symbol,
                    "chain": record["chain"],
                    "contract_address": record["contract_address"],
                    "notes": "runtime registry canonical identity",
                }
            )
            existing_identities.add(identity)
    return merged_tokens, merged_chains


def filter_token_rows(rows, token_symbols):
    """Keep configured tokens requested by the caller."""
    if token_symbols is None:
        return rows

    requested = set()
    for token_symbol in token_symbols:
        requested.add(token_symbol.upper())

    result = []
    for row in rows:
        if row["token_symbol"].upper() in requested:
            result.append(row)

    return result


def read_csv_rows(path):
    """Read an existing pipeline CSV if it exists."""
    if not path.exists():
        return []

    with path.open("r", newline="") as file:
        return list(csv.DictReader(file))


def replace_token_rows(existing_rows, new_rows, token_symbols):
    """Replace selected-token rows while preserving other tokens."""
    selected = set()
    for token_symbol in token_symbols:
        selected.add(token_symbol.upper())

    result = []
    for row in existing_rows:
        if row["token_symbol"].upper() not in selected:
            result.append(row)

    result.extend(new_rows)
    return result


def merge_pool_volume_rows(existing_rows, new_rows):
    """Upsert pool facts by token, chain, pool, and date."""
    merged = {
        (row["token_symbol"], row["chain"], row["pool_address"], row["date"]): row
        for row in existing_rows
    }
    for row in new_rows:
        merged[(row["token_symbol"], row["chain"], row["pool_address"], row["date"])] = row
    return list(merged.values())


def deduplicate_pool_volume_rows(rows):
    """Keep one row for each token-pool-date combination."""
    seen = set()
    result = []

    for row in rows:
        key = (
            row["date"],
            row["token_symbol"],
            row["chain"],
            row["pool_address"],
        )

        if key in seen:
            continue

        seen.add(key)
        result.append(row)

    return result


def group_chain_rows_by_token(rows):
    """Group token-chain config rows by token symbol."""
    grouped = {}

    for row in rows:
        token_symbol = row["token_symbol"]

        if token_symbol not in grouped:
            grouped[token_symbol] = []

        grouped[token_symbol].append(row)

    return grouped


def load_existing_pool_inventory(
    pool_volume_path,
    tvl_latest_path,
    chain_rows_by_token,
    token_symbols,
):
    """Build an exact OHLCV inventory from published pool and TVL facts.

    The daily pool file is authoritative for the tracked token-pool identities.
    The latest TVL snapshot supplies base/quote token ids so the incremental
    request cannot silently fetch the wrong side of a pool. Missing metadata
    sends that token through normal discovery. Pools whose advertised
    base/quote pair excludes the target token are explicitly rejected.
    """
    requested = {token_symbol.upper() for token_symbol in token_symbols}
    latest_by_pool = {}
    for row in read_csv_rows(pool_volume_path):
        token_symbol = row.get("token_symbol", "").upper()
        if token_symbol not in requested:
            continue
        key = (token_symbol, row.get("chain", ""), row.get("pool_address", ""))
        current = latest_by_pool.get(key)
        if current is None or row.get("date", "") > current.get("date", ""):
            latest_by_pool[key] = row

    tvl_by_pool = {}
    for row in read_csv_rows(tvl_latest_path):
        key = (
            row.get("token_symbol", "").upper(),
            row.get("chain", ""),
            row.get("pool_address", ""),
        )
        tvl_by_pool[key] = row

    contracts = {}
    for token_symbol, rows in chain_rows_by_token.items():
        for row in rows:
            contracts[(token_symbol.upper(), row["chain"])] = row["contract_address"]

    pools_by_token = {}
    unresolved_tokens = set()
    invalid_pool_keys = set()
    for key, daily in latest_by_pool.items():
        token_symbol, chain, pool_address = key
        tvl = tvl_by_pool.get(key)
        contract_address = contracts.get((token_symbol, chain))
        if tvl is None or not contract_address:
            unresolved_tokens.add(token_symbol)
            continue

        base_token_id = tvl.get("base_token_id", "")
        quote_token_id = tvl.get("quote_token_id", "")
        try:
            ohlcv_token = get_token_side(
                {
                    "base_token_id": base_token_id,
                    "quote_token_id": quote_token_id,
                },
                chain,
                contract_address,
            )
        except ValueError:
            invalid_pool_keys.add(key)
            continue

        pool = {
            "token_symbol": token_symbol,
            "chain": chain,
            "contract_address": contract_address,
            "pool_rank": 0,
            "dex": tvl.get("source_dex") or daily.get("dex", ""),
            "pool_address": pool_address,
            "pool_name": tvl.get("source_pool_name") or daily.get("pool_name", ""),
            "pool_tvl_usd": optional_float(tvl.get("tvl_usd")),
            "volume_24h_usd": optional_float(tvl.get("volume_24h_usd")),
            "ohlcv_token": ohlcv_token,
            "base_token_id": base_token_id,
            "quote_token_id": quote_token_id,
        }
        pools_by_token.setdefault(token_symbol, []).append(pool)

    resolved_pools = []
    resolved_tokens = set()
    for token_symbol in sorted(requested):
        token_pools = pools_by_token.get(token_symbol, [])
        if token_symbol in unresolved_tokens or not token_pools:
            unresolved_tokens.add(token_symbol)
            continue
        token_pools.sort(
            key=lambda pool: (
                -safe_float(pool.get("volume_24h_usd")),
                pool["chain"],
                pool["pool_address"],
            )
        )
        for pool_rank, pool in enumerate(token_pools, start=1):
            pool["pool_rank"] = pool_rank
            resolved_pools.append(pool)
        resolved_tokens.add(token_symbol)

    return (
        resolved_pools,
        sorted(unresolved_tokens),
        sorted(resolved_tokens),
        sorted(invalid_pool_keys),
    )


def remove_pool_rows(rows, pool_keys):
    """Remove pool histories whose target token cannot be represented by OHLCV."""
    invalid = set(pool_keys)
    return [
        row
        for row in rows
        if (
            row.get("token_symbol", "").upper(),
            row.get("chain", ""),
            row.get("pool_address", ""),
        )
        not in invalid
    ]


def read_token_chain_config(path: Path, token_rows):
    """Read token-chain config, falling back to config/tokens.csv chains."""
    if path.exists():
        with path.open("r", newline="") as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        return rows

    rows = []
    for token in token_rows:
        rows.append(
            {
                "token_symbol": token["token_symbol"],
                "chain": token["chain"],
                "contract_address": token["contract_address"],
                "notes": "fallback from tokens.csv",
            }
        )

    return rows


def build_pool_result(pool_data):
    """Convert GeckoTerminal pool data into our pool row."""
    attributes = pool_data.get("attributes", {})
    relationships = pool_data.get("relationships", {})
    dex_data = relationships.get("dex", {}).get("data", {})
    base_token_data = relationships.get("base_token", {}).get("data", {})
    quote_token_data = relationships.get("quote_token", {}).get("data", {})
    volume_usd = attributes.get("volume_usd", {})

    result = {
        "pool_address": attributes.get("address", ""),
        "dex": dex_data.get("id", ""),
        "pool_name": attributes.get("name", ""),
        "pool_tvl_usd": optional_float(attributes.get("reserve_in_usd")),
        "volume_24h_usd": optional_float(volume_usd.get("h24")),
        "base_token_id": base_token_data.get("id", ""),
        "quote_token_id": quote_token_data.get("id", ""),
    }

    return result


def choose_main_pool(pools):
    """Choose the highest-volume pool from GeckoTerminal pool data."""
    sorted_pools = sort_pools_by_volume(pools)

    if len(sorted_pools) == 0:
        return None

    return build_pool_result(sorted_pools[0])


def choose_top_pools(pools, pool_count):
    """Choose top pools by 24h volume from GeckoTerminal pool data."""
    sorted_pools = sort_pools_by_volume(pools)
    selected_pool_data = sorted_pools[:pool_count]

    selected_pools = []
    for pool_data in selected_pool_data:
        selected_pools.append(build_pool_result(pool_data))

    return selected_pools


def add_token_fields(pool, token, pool_rank):
    """Add token metadata to a selected pool row."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    pool["token_symbol"] = token["token_symbol"]
    pool["chain"] = chain
    pool["contract_address"] = contract_address
    pool["pool_rank"] = pool_rank
    pool["ohlcv_token"] = get_token_side(pool, chain, contract_address)

    return pool


def fetch_pool_candidates_for_chain(chain_row):
    """Fetch candidate pools for one token on one chain."""
    chain = chain_row["chain"]
    contract_address = chain_row["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    sorted_pools = sort_pools_by_volume(pools)
    candidates = sorted_pools[:MAX_POOL_CANDIDATES]

    results = []
    for pool_data in candidates:
        pool = build_pool_result(pool_data)
        add_token_fields(pool, chain_row, 0)
        results.append(pool)

    return results


def find_main_pool(token):
    """Find one main pool for a token."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    pool = choose_main_pool(pools)

    if pool is None:
        return None

    add_token_fields(pool, token, 1)

    return pool


def find_pool_with_ohlcv(token, start_date=None, end_date=None):
    """Find a pool with enough daily OHLCV history."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    sorted_pools = sort_pools_by_volume(pools)
    candidates = sorted_pools[:MAX_POOL_CANDIDATES]

    fallback_pool = None
    fallback_ohlcv_list = []

    time.sleep(REQUEST_SLEEP_SECONDS)

    for pool_data in candidates:
        pool = build_pool_result(pool_data)
        pool["token_symbol"] = token["token_symbol"]
        pool["chain"] = chain
        pool["contract_address"] = contract_address
        pool["ohlcv_token"] = get_token_side(pool, chain, contract_address)

        try:
            ohlcv_list = fetch_pool_ohlcv(pool, start_date, end_date)
        except Exception as error:
            print("Candidate failed %s: %s" % (pool["pool_name"], error))
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        row_count = len(ohlcv_list)

        if row_count > len(fallback_ohlcv_list):
            fallback_pool = pool
            fallback_ohlcv_list = ohlcv_list

        if row_count >= MIN_HISTORY_DAYS:
            time.sleep(REQUEST_SLEEP_SECONDS)
            return pool, ohlcv_list

        print(
            "Candidate has short history for %s: %s rows (%s)"
            % (token["token_symbol"], row_count, pool["pool_name"])
        )
        time.sleep(REQUEST_SLEEP_SECONDS)

    if fallback_pool is None:
        return None, []

    return fallback_pool, fallback_ohlcv_list


def find_top_pools_with_ohlcv(
    token,
    chain_rows,
    attempt_errors=None,
    start_date=None,
    end_date=None,
):
    """Find global top pools with enough daily OHLCV history for one token."""
    candidates = []

    for chain_row in chain_rows:
        try:
            chain_candidates = fetch_pool_candidates_for_chain(chain_row)
        except Exception as error:
            if attempt_errors is not None:
                attempt_errors.append(error)
            print(
                "Failed candidate list for %s on %s: %s"
                % (token["token_symbol"], chain_row["chain"], error)
            )
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        candidates.extend(chain_candidates)
        time.sleep(REQUEST_SLEEP_SECONDS)

    candidates = sorted(candidates, key=lambda pool: pool["volume_24h_usd"], reverse=True)

    selected = []
    fallback = []

    for pool in candidates:
        pool_rank = len(selected) + 1
        pool["pool_rank"] = pool_rank

        try:
            ohlcv_list = fetch_pool_ohlcv(pool, start_date, end_date)
        except Exception as error:
            if attempt_errors is not None:
                attempt_errors.append(error)
            print("Candidate failed %s: %s" % (pool["pool_name"], error))
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        row_count = len(ohlcv_list)

        if row_count > 0:
            fallback.append((pool, ohlcv_list))

        if row_count >= MIN_HISTORY_DAYS:
            selected.append((pool, ohlcv_list))
            print(
                "Selected %s pool %s: %s"
                % (token["token_symbol"], len(selected), pool["pool_name"])
            )

        if len(selected) >= TOP_POOL_COUNT:
            time.sleep(REQUEST_SLEEP_SECONDS)
            return selected

        if row_count < MIN_HISTORY_DAYS:
            print(
                "Candidate has short history for %s: %s rows (%s)"
                % (token["token_symbol"], row_count, pool["pool_name"])
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    if len(selected) > 0:
        return selected

    return fallback[:TOP_POOL_COUNT]


def convert_ohlcv_row(ohlcv, pool, include_tvl_snapshot=True):
    """Convert one GeckoTerminal OHLCV list into one output CSV row."""
    timestamp = int(ohlcv[0])
    date = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": pool["token_symbol"],
        "chain": pool["chain"],
        "dex": pool["dex"],
        "pool_address": pool["pool_address"],
        "pool_name": pool["pool_name"],
        "open": float(ohlcv[1]),
        "high": float(ohlcv[2]),
        "low": float(ohlcv[3]),
        "close": float(ohlcv[4]),
        "dex_volume_usd": float(ohlcv[5]),
        "pool_tvl_usd": pool["pool_tvl_usd"] if include_tvl_snapshot else None,
    }

    return row


def aggregate_dex_pool_rows(rows):
    """Aggregate pool-level DEX volume rows into token-date rows."""
    groups = {}

    for row in rows:
        key = (row["date"], row["token_symbol"])

        if key not in groups:
            groups[key] = {
                "date": row["date"],
                "token_symbol": row["token_symbol"],
                "dex_volume_usd": 0.0,
                "pool_addresses": set(),
                "dexes": set(),
                "chains": set(),
            }

        groups[key]["dex_volume_usd"] += float(row["dex_volume_usd"])
        groups[key]["pool_addresses"].add(row["pool_address"])
        groups[key]["dexes"].add(row["dex"])
        groups[key]["chains"].add(row["chain"])

    result = []
    for item in groups.values():
        pool_addresses = sorted(item["pool_addresses"])
        dexes = sorted(item["dexes"])
        chains = sorted(item["chains"])
        selected_chains = ";".join(chains)

        result.append(
            {
                "date": item["date"],
                "token_symbol": item["token_symbol"],
                "chain": selected_chains,
                "selected_chains": selected_chains,
                "dex_volume_usd": item["dex_volume_usd"],
                "pool_count": len(pool_addresses),
                "included_dexes": ";".join(dexes),
                "included_pool_addresses": ";".join(pool_addresses),
            }
        )

    return sorted(result, key=lambda row: (row["token_symbol"], row["date"]))


def filter_complete_dates(rows, expected_token_count):
    """Keep only dates that have all expected tokens."""
    tokens_by_date = {}

    for row in rows:
        date = row["date"]
        token_symbol = row["token_symbol"]

        if date not in tokens_by_date:
            tokens_by_date[date] = set()

        tokens_by_date[date].add(token_symbol)

    complete_dates = set()
    for date, token_symbols in tokens_by_date.items():
        if len(token_symbols) == expected_token_count:
            complete_dates.add(date)

    result = []
    for row in rows:
        if row["date"] in complete_dates:
            result.append(row)

    return sorted(result, key=lambda row: (row["token_symbol"], row["date"]))


def get_ohlcv_before_timestamp(start_date=None, end_date=None):
    """Return GeckoTerminal's exclusive historical cursor for an inclusive date."""
    if start_date is None and end_date is None:
        return None
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
    return int(end_time.timestamp())


def fetch_pool_ohlcv(pool, start_date=None, end_date=None):
    """Fetch daily OHLCV for one pool."""
    chain = pool["chain"]
    pool_address = pool["pool_address"]

    query = {
        "aggregate": "1",
        "limit": str(min(LIMIT_DAYS, 1000)),
        "currency": "usd",
        "token": pool.get("ohlcv_token", "base"),
    }
    before_timestamp = get_ohlcv_before_timestamp(start_date, end_date)
    if before_timestamp is not None:
        query["before_timestamp"] = str(before_timestamp)

    encoded_query = urllib.parse.urlencode(query)
    path = "/networks/%s/pools/%s/ohlcv/day" % (chain, pool_address)
    url = GECKOTERMINAL_BASE_URL + path + "?" + encoded_query

    data = request_json(url)
    attributes = data.get("data", {}).get("attributes", {})
    ohlcv_list = attributes.get("ohlcv_list", [])

    return ohlcv_list


def write_pool_rows(pools, output_path: Path):
    """Write selected pools to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "token_symbol",
        "chain",
        "contract_address",
        "pool_rank",
        "dex",
        "pool_address",
        "pool_name",
        "pool_tvl_usd",
        "volume_24h_usd",
        "ohlcv_token",
        "base_token_id",
        "quote_token_id",
    ]

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(pools)


def write_pool_volume_rows(rows, output_path: Path):
    """Write pool-level DEX volume rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "date",
        "token_symbol",
        "chain",
        "dex",
        "pool_address",
        "pool_name",
        "open",
        "high",
        "low",
        "close",
        "dex_volume_usd",
        "pool_tvl_usd",
    ]

    rows = sorted(rows, key=lambda row: (row["token_symbol"], row["date"]))

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_volume_rows(rows, output_path: Path):
    """Write aggregated DEX volume rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "date",
        "token_symbol",
        "chain",
        "selected_chains",
        "dex_volume_usd",
        "pool_count",
        "included_dexes",
        "included_pool_addresses",
    ]

    rows = sorted(rows, key=lambda row: (row["token_symbol"], row["date"]))

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def fetch_selected_tokens(
    token_rows,
    chain_rows_by_token,
    *,
    attempt_records=None,
    start_date=None,
    end_date=None,
):
    """Fetch selected pools and pool-level rows for configured tokens."""
    selected_pools = []
    pool_volume_rows = []

    for token in token_rows:
        token_symbol = token["token_symbol"]
        token_chain_rows = chain_rows_by_token.get(token_symbol, [])

        if len(token_chain_rows) == 0:
            print("No token-chain config found for %s" % token_symbol)
            continue

        discovery_errors = []
        try:
            pool_results = find_top_pools_with_ohlcv(
                token,
                token_chain_rows,
                discovery_errors,
                start_date,
                end_date,
            )
        except Exception as error:
            # Discovery is token-level only until a pool has been resolved, so
            # retain a bounded diagnostic but never publish it as market evidence.
            print(
                "DEX discovery failed for %s: %s"
                % (token_symbol, classify_attempt_error(error)["error"])
            )
            continue

        if len(pool_results) == 0:
            print("No usable pool found for %s" % token_symbol)
            continue

        for pool_result in pool_results:
            pool = pool_result[0]
            ohlcv_list = pool_result[1]
            selected_pools.append(pool)

            print(
                "Using %s pool: %s (%s)"
                % (token_symbol, pool["pool_name"], pool["pool_address"])
            )

            latest_timestamp = max(int(ohlcv[0]) for ohlcv in ohlcv_list)
            for ohlcv in ohlcv_list:
                row = convert_ohlcv_row(
                    ohlcv,
                    pool,
                    include_tvl_snapshot=int(ohlcv[0]) == latest_timestamp,
                )
                pool_volume_rows.append(row)
            if attempt_records is not None:
                attempt_records.append(
                    dex_attempt_record(
                        token_symbol,
                        pool.get("chain"),
                        pool.get("dex"),
                        pool.get("pool_address"),
                        rows=[
                            convert_ohlcv_row(
                                ohlcv,
                                pool,
                                include_tvl_snapshot=False,
                            )
                            for ohlcv in ohlcv_list
                        ],
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

        print("Fetched %s DEX pools: %s" % (token_symbol, len(pool_results)))
        time.sleep(REQUEST_SLEEP_SECONDS)

    return selected_pools, pool_volume_rows


def fetch_existing_pools(
    pools,
    *,
    attempt_records=None,
    start_date=None,
    end_date=None,
    fail_on_incomplete=True,
):
    """Fetch OHLCV directly for a validated published pool inventory."""
    pool_volume_rows = []
    failed_pools = []
    pool_count = len(pools)
    range_error = None

    for index, pool in enumerate(pools, start=1):
        try:
            if range_error is not None:
                raise range_error
            ohlcv_list = fetch_pool_ohlcv(pool, start_date, end_date)
        except Exception as error:
            if isinstance(error, SourceRangeUnavailable):
                range_error = error
            print(
                "Existing pool failed %s/%s %s %s: %s"
                % (
                    index,
                    pool_count,
                    pool["token_symbol"],
                    pool["pool_address"],
                    error,
                )
            )
            ohlcv_list = []
            if attempt_records is not None:
                attempt_records.append(
                    dex_attempt_record(
                        pool.get("token_symbol"),
                        pool.get("chain"),
                        pool.get("dex"),
                        pool.get("pool_address"),
                        error=error,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )
        if not ohlcv_list:
            failed_pools.append(
                "%s:%s:%s"
                % (pool["token_symbol"], pool["chain"], pool["pool_address"])
            )
            if attempt_records is not None and not any(
                item["token_symbol"] == pool.get("token_symbol")
                and item.get("chain") == pool.get("chain")
                and item.get("pool_address")
                == (
                    pool.get("pool_address", "").lower()
                    if str(pool.get("pool_address", "")).startswith("0x")
                    else pool.get("pool_address")
                )
                for item in attempt_records
            ):
                attempt_records.append(
                    dex_attempt_record(
                        pool.get("token_symbol"),
                        pool.get("chain"),
                        pool.get("dex"),
                        pool.get("pool_address"),
                        rows=[],
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

        converted_rows = []
        for ohlcv in ohlcv_list:
            converted = convert_ohlcv_row(
                ohlcv,
                pool,
                include_tvl_snapshot=False,
            )
            converted_rows.append(converted)
            pool_volume_rows.append(converted)
        if ohlcv_list and attempt_records is not None:
            attempt_records.append(
                dex_attempt_record(
                    pool.get("token_symbol"),
                    pool.get("chain"),
                    pool.get("dex"),
                    pool.get("pool_address"),
                    rows=converted_rows,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

        print(
            "Fetched existing pool %s/%s: %s %s (%s rows)"
            % (
                index,
                pool_count,
                pool["token_symbol"],
                pool["pool_address"],
                len(ohlcv_list),
            )
        )
        if index < pool_count and range_error is None:
            time.sleep(INCREMENTAL_POOL_SLEEP_SECONDS)

    if failed_pools and fail_on_incomplete:
        raise RuntimeError(
            "Incremental DEX refresh is incomplete for %s pools: %s"
            % (len(failed_pools), ",".join(failed_pools))
        )

    return pool_volume_rows


def main(
    token_symbols=None,
    append=False,
    start_date=None,
    end_date=None,
    limit_days=LIMIT_DAYS,
    output_dir=None,
    local_dir=None,
) -> None:
    """Fetch DEX data into processed CSV files."""
    global LIMIT_DAYS
    LIMIT_DAYS = limit_days
    get_ohlcv_before_timestamp(start_date, end_date)
    resolved_output_dir = (
        Path(output_dir)
        if output_dir is not None
        else DEX_POOL_VOLUME_OUTPUT_PATH.parent
    )
    resolved_local_dir = (
        Path(local_dir)
        if local_dir is not None
        else TVL_LATEST_PATH.parent
    )
    dex_pools_output_path = resolved_output_dir / DEX_POOLS_OUTPUT_PATH.name
    dex_pool_volume_output_path = (
        resolved_output_dir / DEX_POOL_VOLUME_OUTPUT_PATH.name
    )
    dex_volume_output_path = resolved_output_dir / DEX_VOLUME_OUTPUT_PATH.name
    attempt_output_path = resolved_output_dir / ATTEMPT_OUTPUT_PATH.name
    tvl_latest_path = resolved_local_dir / TVL_LATEST_PATH.name
    all_token_rows = read_token_config(TOKEN_CONFIG_PATH)
    static_chain_rows = read_token_chain_config(
        TOKEN_CHAIN_CONFIG_PATH,
        all_token_rows,
    )
    all_token_rows, chain_rows = merge_runtime_token_config(
        all_token_rows,
        static_chain_rows,
        token_symbols=token_symbols,
    )
    token_rows = filter_token_rows(all_token_rows, token_symbols)
    if token_symbols is not None:
        configured_symbols = {
            row["token_symbol"].upper()
            for row in token_rows
        }
        missing_tokens = sorted(
            {
                token_symbol.upper()
                for token_symbol in token_symbols
            }
            - configured_symbols
        )
        if missing_tokens:
            raise ValueError(
                "Requested Tokens are not configured: %s"
                % ",".join(missing_tokens)
            )
    chain_rows_by_token = group_chain_rows_by_token(chain_rows)

    selected_pools = []
    pool_volume_rows = []
    attempt_records = []
    discovery_token_rows = token_rows
    invalid_pool_keys = []
    if append:
        if token_symbols is None:
            raise ValueError("--append requires --tokens")
        (
            selected_pools,
            unresolved_tokens,
            resolved_tokens,
            invalid_pool_keys,
        ) = load_existing_pool_inventory(
            dex_pool_volume_output_path,
            tvl_latest_path,
            chain_rows_by_token,
            token_symbols,
        )
        if invalid_pool_keys:
            print(
                "Dropping %s pools whose OHLCV base/quote excludes the target token"
                % len(invalid_pool_keys)
            )
        if selected_pools:
            print(
                "Reusing %s published pools for %s tokens"
                % (len(selected_pools), len(resolved_tokens))
            )
            try:
                pool_volume_rows.extend(
                    fetch_existing_pools(
                        selected_pools,
                        attempt_records=attempt_records,
                        start_date=start_date,
                        end_date=end_date,
                        fail_on_incomplete=False,
                    )
                )
            except Exception:
                if dex_pool_volume_output_path.exists():
                    write_attempt_ledger(
                        attempt_output_path,
                        attempt_records,
                        source_csv=dex_pool_volume_output_path,
                        start_date=start_date,
                        end_date=end_date,
                    )
                raise
        discovery_token_rows = filter_token_rows(token_rows, unresolved_tokens)
        if unresolved_tokens:
            print(
                "Running pool discovery for unresolved tokens: %s"
                % ",".join(unresolved_tokens)
            )

    if discovery_token_rows:
        discovered_pools, discovered_rows = fetch_selected_tokens(
            discovery_token_rows,
            chain_rows_by_token,
            attempt_records=attempt_records,
            start_date=start_date,
            end_date=end_date,
        )
        selected_pools.extend(discovered_pools)
        pool_volume_rows.extend(discovered_rows)
    pool_volume_rows = [
        row
        for row in pool_volume_rows
        if (start_date is None or row["date"] >= start_date)
        and (end_date is None or row["date"] <= end_date)
    ]
    if token_symbols is not None and not append:
        observed_tokens = {
            row["token_symbol"].upper()
            for row in pool_volume_rows
        }
        missing_observations = sorted(
            {
                token_symbol.upper()
                for token_symbol in token_symbols
            }
            - observed_tokens
        )
        if missing_observations:
            raise RuntimeError(
                "No DEX daily rows were collected for requested Tokens: %s"
                % ",".join(missing_observations)
            )

    expected_token_count = len(token_rows)

    if append:
        existing_pools = read_csv_rows(dex_pools_output_path)
        existing_pool_volume_rows = read_csv_rows(dex_pool_volume_output_path)
        existing_pool_volume_rows = remove_pool_rows(
            existing_pool_volume_rows,
            invalid_pool_keys,
        )
        selected_pools = replace_token_rows(
            existing_pools,
            selected_pools,
            token_symbols,
        )
        pool_volume_rows = merge_pool_volume_rows(existing_pool_volume_rows, pool_volume_rows)
        expected_token_count = len(all_token_rows)
        if token_symbols is not None:
            observed_tokens = {
                row["token_symbol"].upper()
                for row in pool_volume_rows
            }
            missing_observations = sorted(
                {
                    token_symbol.upper()
                    for token_symbol in token_symbols
                }
                - observed_tokens
            )
            if missing_observations:
                raise RuntimeError(
                    "No DEX daily rows were collected or previously published "
                    "for requested Tokens: %s"
                    % ",".join(missing_observations)
                )

    pool_volume_rows = deduplicate_pool_volume_rows(pool_volume_rows)
    volume_rows = aggregate_dex_pool_rows(pool_volume_rows)
    volume_rows = filter_complete_dates(volume_rows, expected_token_count)

    write_pool_rows(selected_pools, dex_pools_output_path)
    write_pool_volume_rows(pool_volume_rows, dex_pool_volume_output_path)
    write_volume_rows(volume_rows, dex_volume_output_path)
    write_attempt_ledger(
        attempt_output_path,
        attempt_records,
        source_csv=dex_pool_volume_output_path,
        start_date=start_date,
        end_date=end_date,
    )

    print("Wrote %s pools to %s" % (len(selected_pools), dex_pools_output_path))
    print(
        "Wrote %s pool rows to %s"
        % (len(pool_volume_rows), dex_pool_volume_output_path)
    )
    print("Wrote %s rows to %s" % (len(volume_rows), dex_volume_output_path))
    print(
        "Wrote %s collection attempts to %s"
        % (len(attempt_records), attempt_output_path)
    )


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Fetch GeckoTerminal DEX data")
    parser.add_argument(
        "--tokens",
        help="Comma-separated token symbols to refresh",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Replace selected tokens and preserve existing token rows",
    )
    parser.add_argument("--start", help="Inclusive UTC date")
    parser.add_argument("--end", help="Inclusive UTC date")
    parser.add_argument("--limit-days", type=int, default=LIMIT_DAYS)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--local-dir", type=Path)
    args = parser.parse_args()

    token_symbols = None
    if args.tokens:
        token_symbols = []
        for token_symbol in args.tokens.split(","):
            cleaned = token_symbol.strip().upper()
            if cleaned:
                token_symbols.append(cleaned)

    return (
        token_symbols,
        args.append,
        args.start,
        args.end,
        args.limit_days,
        args.output_dir,
        args.local_dir,
    )


if __name__ == "__main__":
    (
        selected_tokens,
        append_rows,
        start_date,
        end_date,
        limit_days,
        selected_output_dir,
        selected_local_dir,
    ) = parse_args()
    main(
        selected_tokens,
        append_rows,
        start_date,
        end_date,
        limit_days,
        selected_output_dir,
        selected_local_dir,
    )
